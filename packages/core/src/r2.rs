//! Encrypted R2 blob storage client.
//!
//! All content is encrypted with AES-256-GCM before being written to R2,
//! and decrypted transparently on read.  Plaintext never touches the wire.
//!
//! # Wire format
//! Objects are stored as JSON-serialized [`EncryptedBlob`] values with
//! `Content-Type: application/json`.  The object key follows the pattern
//! `{graph_id}/{atom_id}` for consumer/personal content, or
//! `{org_id}/{graph_id}/{atom_id}` for org-tenant content (org-tenancy
//! PR 2.4) — the org prefix scopes every org's blobs under its own key
//! namespace so per-org lifecycle (offboarding prefix-delete) is clean and a
//! bucket-policy misconfiguration can't cross orgs. The **stored `blob_key`
//! column is authoritative per atom**: reads always pass the stored key
//! verbatim (see [`R2Client::get_atom_blob`]) and never reconstruct a key
//! from parts, so legacy 2-segment keys keep resolving forever with no
//! object backfill.
//!
//! # Credentials
//! Credentials are passed explicitly to [`R2Client::new`] — the client never
//! reads from the environment or IAM, so the caller controls the source.

use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{Builder as S3ConfigBuilder, Region, SharedAsyncSleep},
    error::SdkError,
    operation::get_object::GetObjectError,
    Client as S3Client,
};
use aws_smithy_async::rt::sleep::TokioSleep;
use thiserror::Error;
use uuid::Uuid;

use crate::crypto::{decrypt, encrypt, CryptoError, EncryptedBlob};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can arise from R2 blob operations.
#[derive(Debug, Error)]
pub enum R2Error {
    #[error("S3 error: {0}")]
    S3(String),

    #[error("encryption error: {0}")]
    Crypto(#[from] CryptoError),

    #[error("blob not found: {0}")]
    NotFound(String),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("invalid blob key")]
    InvalidKey,
}

impl R2Error {
    /// The object store itself refused or could not be reached: the same
    /// operation can succeed once the store is back. A missing object, a bad
    /// key or an encryption failure is not a store outage. The one place
    /// this rule lives; every caller that classifies a storage failure
    /// (ingest's 503 `storage_unavailable`) reads it here.
    pub fn is_store_unavailable(&self) -> bool {
        matches!(self, Self::S3(_))
    }
}

// Map AWS SDK errors to R2Error.
impl<E: std::fmt::Debug, R: std::fmt::Debug> From<SdkError<E, R>> for R2Error {
    fn from(_e: SdkError<E, R>) -> Self {
        R2Error::S3("storage operation failed".to_string())
    }
}

// ---------------------------------------------------------------------------
// Blob keys (org-tenancy PR 2.4)
// ---------------------------------------------------------------------------

/// Maximum accepted length of the org segment in a 3-segment blob key.
///
/// Clerk org ids (`org_…`) are ~31 chars today; 64 leaves headroom without
/// admitting pathological keys.
const MAX_ORG_SEGMENT_LEN: usize = 64;

/// The org-segment rule: a `org_`-prefixed Clerk org id, bounded length, and
/// only `[A-Za-z0-9_-]` — which structurally excludes `/` (segment separator),
/// `.` (no `.`/`..` path traversal), whitespace, and every other
/// S3-key-significant character.
///
/// The mandatory `org_` prefix is not cosmetic: it makes the org key namespace
/// provably DISJOINT from personal keys, whose first segment is always a UUID
/// (`{graph_id}/{atom_id}`) and can therefore never begin with `org_`. Without
/// it, a UUID-shaped org segment could nest personal keys inside an org prefix,
/// and PR 3.2's offboarding prefix-delete (`{org_id}/*`) could then delete a
/// personal graph's blobs. Clerk org ids (`org_2Abc…`) always satisfy this.
fn valid_org_segment(org_id: &str) -> bool {
    org_id.starts_with("org_")
        && org_id.len() <= MAX_ORG_SEGMENT_LEN
        && org_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The consumer/personal blob key: `{graph_id}/{atom_id}` (2-segment,
/// byte-identical to the pre-tenancy scheme). Infallible — UUIDs always
/// render to a valid key.
pub fn personal_blob_key(graph_id: Uuid, atom_id: Uuid) -> String {
    format!("{graph_id}/{atom_id}")
}

/// Build the blob key for a NEW atom, org-aware.
///
/// * `org_id = None` (consumer/personal plane) → `{graph_id}/{atom_id}` —
///   byte-identical to the legacy scheme.
/// * `org_id = Some(clerk_org_id)` (org tenant) → `{org_id}/{graph_id}/{atom_id}`.
///
/// `org_id` must come from a server-side, verified source (the request's
/// `TenantContext` / the routing row) — never from client input. A malformed
/// org segment fails closed with [`R2Error::InvalidKey`] rather than ever
/// falling back to an unprefixed key.
///
/// This is the single construction point for atom blob keys: ingest, workers,
/// and PR 2.5's promotion re-upload all derive keys here, and the result is
/// what gets persisted as the atom's authoritative `blob_key`.
pub fn blob_key_for(
    org_id: Option<&str>,
    graph_id: Uuid,
    atom_id: Uuid,
) -> Result<String, R2Error> {
    match org_id {
        None => Ok(personal_blob_key(graph_id, atom_id)),
        Some(org) if valid_org_segment(org) => Ok(format!("{org}/{graph_id}/{atom_id}")),
        Some(_) => Err(R2Error::InvalidKey),
    }
}

/// Validate a blob key before it is used against the bucket.
///
/// Accepts BOTH on-disk forms — the stored `blob_key` column is authoritative
/// and pre-tenancy atoms keep their 2-segment keys forever (no backfill):
///
/// * legacy/personal 2-segment: `{graph_uuid}/{atom_uuid}`
/// * org-prefixed 3-segment: `{org_segment}/{graph_uuid}/{atom_uuid}`
///
/// Everything else (wrong segment count, empty segments, non-UUID
/// graph/atom parts, path traversal, disallowed org charset) is rejected.
pub fn validate_blob_key(key: &str) -> Result<(), R2Error> {
    let parts: Vec<&str> = key.split('/').collect();
    let ok = match parts.as_slice() {
        [graph, atom] => Uuid::parse_str(graph).is_ok() && Uuid::parse_str(atom).is_ok(),
        [org, graph, atom] => {
            valid_org_segment(org)
                && Uuid::parse_str(graph).is_ok()
                && Uuid::parse_str(atom).is_ok()
        }
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(R2Error::InvalidKey)
    }
}

/// The org segment a (shape-valid) blob key carries, or `None` for a legacy
/// 2-segment personal key. Does NOT itself validate shape — callers assert
/// [`validate_blob_key`] first (as every read/write call site already does).
fn org_segment_of(key: &str) -> Option<&str> {
    match key.split('/').collect::<Vec<&str>>().as_slice() {
        [org, _graph, _atom] => Some(org),
        _ => None,
    }
}

/// Org-tenancy PR 4.1 deferred assert (gap-review item 7, deferred from PR
/// 2.4/2.5): before a blob is served (or overwritten) for a request bound to
/// `expected_org`, assert the stored `blob_key`'s own org segment is that
/// EXACT org — never merely "shape-valid". `validate_blob_key` alone only
/// proves the key isn't malformed; it says nothing about which org it
/// belongs to, so a bug or data-corruption elsewhere (e.g. an atom row whose
/// `content_blob_key` was copy-pasted, mis-migrated, or forged) could still
/// carry a perfectly well-formed key for a DIFFERENT org. This assert is the
/// last line of defense at the point of use — call it AFTER
/// `validate_blob_key` (shape) and BEFORE the object is fetched/overwritten
/// (scope).
///
/// * `expected_org = Some(org_id)` (an ORG request) — the key MUST be a
///   3-segment key whose org segment equals `org_id` exactly. A legacy
///   2-segment key (no org segment at all) or any other org's segment is
///   rejected — an org request must never resolve a personal-plane or
///   foreign-org object, even one that is otherwise well-formed.
/// * `expected_org = None` (a PERSONAL/consumer request) — unrestricted.
///   Content on the shared consumer plane is scoped by `account_id`/graph
///   ownership upstream (reachability), never by blob-key shape, and no org
///   atom is ever reachable from a personal request in the first place (org
///   tenant databases are physically separate, not filtered rows) — so there
///   is no cross-tenant reach this arm could catch that the DB boundary
///   doesn't already provide.
pub fn assert_blob_key_org_scope(blob_key: &str, expected_org: Option<&str>) -> Result<(), R2Error> {
    match expected_org {
        None => Ok(()),
        Some(expected) => match org_segment_of(blob_key) {
            Some(actual) if actual == expected => Ok(()),
            _ => Err(R2Error::InvalidKey),
        },
    }
}

// ---------------------------------------------------------------------------
// R2Client
// ---------------------------------------------------------------------------

/// An S3-compatible client pointed at Cloudflare R2, with transparent
/// AES-256-GCM encryption on every write.
pub struct R2Client {
    inner: S3Client,
    bucket: String,
}

impl R2Client {
    /// Build an [`R2Client`] from explicit credentials.
    ///
    /// This constructor does **not** make any network calls; it only configures
    /// the underlying SDK client.
    ///
    /// # Parameters
    /// - `account_id` — Cloudflare account ID (used to build the endpoint URL)
    /// - `access_key_id` — R2 API token access key
    /// - `secret_access_key` — R2 API token secret key
    /// - `bucket` — R2 bucket name
    ///
    /// # No endpoint override, on purpose
    /// The endpoint is ALWAYS `https://{account_id}.r2.cloudflarestorage.com`
    /// — this constructor reads no environment variable and accepts no
    /// override parameter, so there is no way for a stray env var or config
    /// value to silently redirect production blob traffic elsewhere. Tests
    /// that need a local S3-compatible fake (MinIO, s3mock) use
    /// [`R2Client::new_with_endpoint_override`] instead, which exists only
    /// behind the `test-utils` Cargo feature and is never compiled into a
    /// production binary (see that function's doc comment).
    pub async fn new(
        account_id: &str,
        access_key_id: &str,
        secret_access_key: &str,
        bucket: &str,
    ) -> Self {
        let endpoint = format!("https://{account_id}.r2.cloudflarestorage.com");
        Self::build(&endpoint, access_key_id, secret_access_key, bucket, false)
    }

    /// Test-only sibling of [`R2Client::new`] that points at an explicit
    /// `endpoint_override` instead of the hardcoded Cloudflare R2 URL, so a
    /// local S3-compatible fake (MinIO, s3mock, …) can serve requests. Also
    /// enables S3 path-style addressing (`http://host/bucket/key` rather than
    /// `http://bucket.host/key`), which MinIO/s3mock require for arbitrary
    /// bucket names without wildcard DNS.
    ///
    /// # Why this is not a parameter on `new`, and not env-var-driven
    /// Before this fix, `R2Client`'s endpoint had no override at all, which
    /// meant no test anywhere could exercise a *successful* R2 GET/PUT — every
    /// R2 consumer's success path was implemented but unproven (see this
    /// crate's hardening notes). The natural fix is an override, but an
    /// override reachable from a production build is itself a footgun: an
    /// accidentally-set env var (e.g. a leftover `R2_ENDPOINT_OVERRIDE` in a
    /// shared `.env` or CI secret) would silently redirect real blob traffic
    /// to whatever it points at, with no error and no log line pointing at
    /// the cause. Gating this constructor behind the `test-utils` Cargo
    /// feature — enabled ONLY as a `dev-dependency` feature of consuming
    /// crates' test builds (see `backend/Cargo.toml`) — means the override
    /// path does not exist as compiled code in any production artifact at
    /// all: there is no runtime flag to leave set, because there is no
    /// runtime code path to trigger. Misuse (calling this from
    /// non-test/non-dev code) is a `cargo build` **compile error**, not a
    /// silent runtime redirect — as loud as a misconfiguration can get.
    #[cfg(feature = "test-utils")]
    pub async fn new_with_endpoint_override(
        account_id: &str,
        access_key_id: &str,
        secret_access_key: &str,
        bucket: &str,
        endpoint_override: &str,
    ) -> Self {
        let _ = account_id; // kept for signature parity with `new`; R2 doesn't need it once the endpoint is explicit
        Self::build(
            endpoint_override,
            access_key_id,
            secret_access_key,
            bucket,
            true,
        )
    }

    /// Shared S3 client construction — see [`R2Client::new`] and
    /// [`R2Client::new_with_endpoint_override`] for the two (and only two)
    /// callers.
    fn build(
        endpoint: &str,
        access_key_id: &str,
        secret_access_key: &str,
        bucket: &str,
        force_path_style: bool,
    ) -> Self {
        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            None,  // session token — not used by R2
            None,  // expiry
            "coralswarm-r2",
        );

        let s3_config = S3ConfigBuilder::new()
            .endpoint_url(endpoint)
            .region(Region::new("auto"))
            .credentials_provider(credentials)
            .sleep_impl(SharedAsyncSleep::new(TokioSleep::new()))
            .force_path_style(force_path_style)
            .behavior_version_latest()
            .build();

        R2Client {
            inner: S3Client::from_conf(s3_config),
            bucket: bucket.to_owned(),
        }
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// Encrypt `content` with `data_key` and upload to R2.
    ///
    /// The object key is derived by [`blob_key_for`]: `"{graph_id}/{atom_id}"`
    /// on the consumer/personal plane (`org_id = None`, byte-identical to the
    /// legacy scheme), `"{org_id}/{graph_id}/{atom_id}"` for org tenants
    /// (org-tenancy PR 2.4). The stored object is a JSON-serialized
    /// [`EncryptedBlob`] with `Content-Type: application/json`.
    ///
    /// Returns the blob key on success — persist it verbatim as the atom's
    /// authoritative `blob_key`; reads must use the stored key, never
    /// re-derive one.
    ///
    /// # Privacy guarantee
    /// `content` is encrypted unconditionally before any network call.
    /// Plaintext never leaves the process.
    pub async fn put_atom_blob(
        &self,
        org_id: Option<&str>,
        graph_id: Uuid,
        atom_id: Uuid,
        content: &[u8],
        data_key: &[u8; 32],
    ) -> Result<String, R2Error> {
        let blob_key = blob_key_for(org_id, graph_id, atom_id)?;
        self.put_atom_blob_at(&blob_key, content, data_key).await?;
        Ok(blob_key)
    }

    /// Encrypt `content` with `data_key` and upload to R2 at an
    /// already-derived `blob_key` (validated by [`validate_blob_key`] —
    /// legacy 2-segment and org-prefixed 3-segment both accepted).
    ///
    /// For call sites that must write back at an atom's **stored,
    /// authoritative** key rather than re-deriving one (e.g. the reembed
    /// worker's in-place content overwrite, PR 2.5's promotion re-upload) —
    /// reconstructing a key there would silently assume a scheme and write an
    /// org atom's content to the wrong (unprefixed) location.
    ///
    /// # Privacy guarantee
    /// `content` is encrypted unconditionally before any network call.
    /// Plaintext never leaves the process.
    pub async fn put_atom_blob_at(
        &self,
        blob_key: &str,
        content: &[u8],
        data_key: &[u8; 32],
    ) -> Result<(), R2Error> {
        validate_blob_key(blob_key)?;

        // Encrypt first — plaintext never reaches the wire.
        let blob: EncryptedBlob = encrypt(content, data_key)?;
        let body_bytes = serde_json::to_vec(&blob)?;

        self.inner
            .put_object()
            .bucket(&self.bucket)
            .key(blob_key)
            .body(body_bytes.into())
            .content_type("application/json")
            .send()
            .await
            .map_err(|_e| R2Error::S3("storage operation failed".to_string()))?;

        Ok(())
    }

    /// Download from R2 and decrypt with `data_key`.
    ///
    /// Returns the original plaintext on success.
    ///
    /// # Error behaviour
    /// - [`R2Error::NotFound`] if the object does not exist.
    /// - [`R2Error::Crypto`] wrapping [`CryptoError::TagMismatch`] if the
    ///   authentication tag does not verify — corrupted plaintext is **never**
    ///   returned.
    pub async fn get_atom_blob(
        &self,
        blob_key: &str,
        data_key: &[u8; 32],
    ) -> Result<Vec<u8>, R2Error> {
        validate_blob_key(blob_key)?;

        let resp = self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(blob_key)
            .send()
            .await
            .map_err(|e| {
                // Distinguish 404 from other S3 errors.
                if let SdkError::ServiceError(ref svc) = e {
                    if matches!(svc.err(), GetObjectError::NoSuchKey(_)) {
                        return R2Error::NotFound(blob_key.to_owned());
                    }
                }
                R2Error::S3("storage operation failed".to_string())
            })?;

        let body_bytes = resp
            .body
            .collect()
            .await
            .map_err(|_e| R2Error::S3("storage operation failed".to_string()))?
            .into_bytes();

        let blob: EncryptedBlob = serde_json::from_slice(&body_bytes)?;

        // Decrypt — returns CryptoError::TagMismatch on authentication failure.
        let plaintext = decrypt(&blob, data_key)?;
        Ok(plaintext)
    }

    /// Delete the object at `blob_key`.
    ///
    /// Succeeds even if the object does not exist (S3 DELETE is idempotent).
    pub async fn delete_atom_blob(&self, blob_key: &str) -> Result<(), R2Error> {
        validate_blob_key(blob_key)?;

        self.inner
            .delete_object()
            .bucket(&self.bucket)
            .key(blob_key)
            .send()
            .await
            .map_err(|_e| R2Error::S3("storage operation failed".to_string()))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{decrypt, encrypt};

    fn test_key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    // -----------------------------------------------------------------------
    // validate_blob_key
    // -----------------------------------------------------------------------

    #[test]
    fn valid_blob_key_accepted() {
        let key = "00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        assert!(validate_blob_key(key).is_ok());
    }

    #[test]
    fn path_traversal_key_rejected() {
        assert!(matches!(
            validate_blob_key("../../other-account/secret"),
            Err(R2Error::InvalidKey)
        ));
    }

    #[test]
    fn non_uuid_key_rejected() {
        assert!(matches!(
            validate_blob_key("notauuid/alsonotauuid"),
            Err(R2Error::InvalidKey)
        ));
    }

    #[test]
    fn single_segment_key_rejected() {
        assert!(matches!(
            validate_blob_key("00000000-0000-0000-0000-000000000001"),
            Err(R2Error::InvalidKey)
        ));
    }

    #[test]
    fn three_segment_org_key_accepted() {
        // Org-prefixed keys (org-tenancy PR 2.4): {org_id}/{graph_id}/{atom_id}.
        let key = "org_2AbcDEF123/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        assert!(validate_blob_key(key).is_ok());
    }

    #[test]
    fn three_segment_key_with_non_uuid_tail_rejected() {
        assert!(matches!(
            validate_blob_key(
                "00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002/extra"
            ),
            Err(R2Error::InvalidKey)
        ));
        assert!(matches!(
            validate_blob_key("org_2Abc/00000000-0000-0000-0000-000000000001/notauuid"),
            Err(R2Error::InvalidKey)
        ));
    }

    #[test]
    fn four_segment_key_rejected() {
        assert!(matches!(
            validate_blob_key(
                "org_2Abc/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002/00000000-0000-0000-0000-000000000003"
            ),
            Err(R2Error::InvalidKey)
        ));
    }

    #[test]
    fn empty_segments_rejected() {
        // Leading, trailing, and interior empty segments are all malformed.
        assert!(validate_blob_key("/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002").is_err());
        assert!(validate_blob_key("org_2Abc//00000000-0000-0000-0000-000000000002").is_err());
        assert!(validate_blob_key("00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002/").is_err());
        assert!(validate_blob_key("").is_err());
    }

    #[test]
    fn org_segment_path_traversal_rejected() {
        // `.` is outside the org-segment charset, so `..` can never validate.
        assert!(matches!(
            validate_blob_key(
                "../00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
            ),
            Err(R2Error::InvalidKey)
        ));
    }

    #[test]
    fn org_segment_bad_charset_rejected() {
        for org in ["org 2Abc", "org.2Abc", "org$2Abc", "örg_2Abc"] {
            let key = format!(
                "{org}/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
            );
            assert!(
                validate_blob_key(&key).is_err(),
                "org segment {org:?} should be rejected"
            );
        }
    }

    #[test]
    fn org_segment_over_max_length_rejected() {
        // org_-prefixed AND over-length, so ONLY the length rule can reject it
        // (a non-org_ string would fail the prefix check first, leaving length
        // untested).
        let org = format!("org_{}", "a".repeat(MAX_ORG_SEGMENT_LEN));
        assert!(org.len() > MAX_ORG_SEGMENT_LEN && org.starts_with("org_"));
        let key = format!(
            "{org}/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
        );
        assert!(validate_blob_key(&key).is_err());
        // Exactly at the bound is fine (org_-prefixed, total length == max).
        let org = format!("org_{}", "a".repeat(MAX_ORG_SEGMENT_LEN - 4));
        assert_eq!(org.len(), MAX_ORG_SEGMENT_LEN);
        let key = format!(
            "{org}/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
        );
        assert!(validate_blob_key(&key).is_ok());
    }

    #[test]
    fn org_namespace_is_disjoint_from_personal_keys() {
        // The mandatory `org_` prefix keeps the org namespace provably disjoint
        // from personal keys (whose first segment is a UUID). A UUID-shaped or
        // otherwise non-`org_` first segment in a 3-segment key must be
        // rejected — otherwise PR 3.2's `{org_id}/*` prefix-delete could reach
        // personal blobs. Both the validator and the builder enforce it.
        let uuid_org = "00000000-0000-0000-0000-000000000009";
        let key = format!(
            "{uuid_org}/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
        );
        assert!(validate_blob_key(&key).is_err(), "UUID-shaped org segment must be rejected");
        for not_org in ["tenant_2Abc", "2AbcDEF", "acme", uuid_org] {
            assert!(
                matches!(
                    blob_key_for(Some(not_org), Uuid::new_v4(), Uuid::new_v4()),
                    Err(R2Error::InvalidKey)
                ),
                "non-org_ segment {not_org:?} must fail closed"
            );
        }
    }

    // -----------------------------------------------------------------------
    // assert_blob_key_org_scope (org-tenancy PR 4.1 deferred assert)
    // -----------------------------------------------------------------------

    #[test]
    fn org_scope_assert_accepts_matching_org() {
        let key = "org_a/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        assert!(assert_blob_key_org_scope(key, Some("org_a")).is_ok());
    }

    #[test]
    fn org_scope_assert_rejects_foreign_org_prefix() {
        // Shape-valid (would pass validate_blob_key), but the org segment is
        // org_b's, not org_a's — the exact cross-org forgery this assert
        // exists to catch.
        let key = "org_b/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        assert!(matches!(
            assert_blob_key_org_scope(key, Some("org_a")),
            Err(R2Error::InvalidKey)
        ));
    }

    #[test]
    fn org_scope_assert_rejects_legacy_personal_key_under_an_org_request() {
        // An org-context request resolving a 2-segment (no org prefix at all)
        // key must fail closed too — never silently treat it as "this org's".
        let key = "00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        assert!(matches!(
            assert_blob_key_org_scope(key, Some("org_a")),
            Err(R2Error::InvalidKey)
        ));
    }

    #[test]
    fn org_scope_assert_unrestricted_for_personal_requests() {
        // Personal/consumer requests (expected_org = None) are unrestricted —
        // the DB boundary, not blob-key shape, is what scopes them.
        let personal_key =
            "00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        let org_key = "org_a/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        assert!(assert_blob_key_org_scope(personal_key, None).is_ok());
        assert!(assert_blob_key_org_scope(org_key, None).is_ok());
    }

    #[test]
    fn org_scope_assert_case_and_substring_org_ids_never_match() {
        // Case-sensitive, exact-match only — "org_a2" must never satisfy
        // "org_a", and "ORG_A" must never satisfy "org_a".
        let key_a2 = "org_a2/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        assert!(matches!(
            assert_blob_key_org_scope(key_a2, Some("org_a")),
            Err(R2Error::InvalidKey)
        ));
        let key_upper = "ORG_A/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002";
        assert!(matches!(
            assert_blob_key_org_scope(key_upper, Some("org_a")),
            Err(R2Error::InvalidKey)
        ));
    }

    // -----------------------------------------------------------------------
    // Blob key builders (org-tenancy PR 2.4)
    // -----------------------------------------------------------------------

    #[test]
    fn personal_key_is_two_segment_and_legacy_identical() {
        let graph_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let atom_id = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

        let key = personal_blob_key(graph_id, atom_id);
        // Byte-identical to the pre-tenancy scheme.
        assert_eq!(
            key,
            "00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
        );
        // blob_key_for(None, ..) is the same key.
        assert_eq!(blob_key_for(None, graph_id, atom_id).unwrap(), key);
        // And it round-trips through the validator.
        assert!(validate_blob_key(&key).is_ok());
    }

    #[test]
    fn org_key_is_three_segment_with_org_prefix() {
        let graph_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let atom_id = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

        let key = blob_key_for(Some("org_2AbcDEF123"), graph_id, atom_id).unwrap();
        assert_eq!(
            key,
            "org_2AbcDEF123/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
        );
        assert!(validate_blob_key(&key).is_ok());
    }

    #[test]
    fn malformed_org_id_fails_closed_never_unprefixed() {
        let graph_id = Uuid::new_v4();
        let atom_id = Uuid::new_v4();
        // Empty, traversal, separator, whitespace, over-length — every one
        // must be an error, never a silent fallback to a 2-segment key.
        for bad in ["", "..", "a/b", "org 2Abc", "org.2Abc"] {
            assert!(
                matches!(
                    blob_key_for(Some(bad), graph_id, atom_id),
                    Err(R2Error::InvalidKey)
                ),
                "org id {bad:?} should fail closed"
            );
        }
        // org_-prefixed so the length rule (not the prefix rule) is what fails.
        let too_long = format!("org_{}", "a".repeat(MAX_ORG_SEGMENT_LEN));
        assert!(matches!(
            blob_key_for(Some(&too_long), graph_id, atom_id),
            Err(R2Error::InvalidKey)
        ));
    }

    // -----------------------------------------------------------------------
    // Encrypt → serialize → deserialize → decrypt roundtrip (no S3)
    // -----------------------------------------------------------------------

    #[test]
    fn encrypted_blob_roundtrip_via_json() {
        let data_key = test_key();
        let plaintext = b"hello, encrypted R2 world!";

        // Encrypt (mirrors what put_atom_blob does).
        let blob = encrypt(plaintext, &data_key).expect("encrypt should succeed");

        // Serialize to JSON (wire format).
        let json_bytes = serde_json::to_vec(&blob).expect("serialize should succeed");

        // Deserialize back (mirrors what get_atom_blob does after download).
        let recovered_blob: EncryptedBlob =
            serde_json::from_slice(&json_bytes).expect("deserialize should succeed");

        // Decrypt.
        let recovered = decrypt(&recovered_blob, &data_key).expect("decrypt should succeed");
        assert_eq!(recovered, plaintext);
    }

    #[test]
    fn wrong_key_after_roundtrip_returns_tag_mismatch() {
        let data_key = test_key();
        let mut bad_key = test_key();
        bad_key[0] ^= 0xFF;

        let blob = encrypt(b"secret", &data_key).unwrap();
        let json_bytes = serde_json::to_vec(&blob).unwrap();
        let recovered_blob: EncryptedBlob = serde_json::from_slice(&json_bytes).unwrap();

        let err = decrypt(&recovered_blob, &bad_key).unwrap_err();
        assert!(
            matches!(err, CryptoError::TagMismatch),
            "expected TagMismatch, got {err:?}"
        );
    }

    #[test]
    fn corrupted_ciphertext_after_roundtrip_returns_tag_mismatch() {
        let data_key = test_key();
        let blob = encrypt(b"important data", &data_key).unwrap();
        let json_bytes = serde_json::to_vec(&blob).unwrap();

        // Tamper with the JSON by corrupting a ciphertext byte via the struct.
        let mut tampered: EncryptedBlob = serde_json::from_slice(&json_bytes).unwrap();
        tampered.ciphertext[0] ^= 0xFF;

        let err = decrypt(&tampered, &data_key).unwrap_err();
        assert!(
            matches!(err, CryptoError::TagMismatch),
            "expected TagMismatch, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // R2Client construction (no network calls)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn r2client_constructs_without_panic() {
        // Verifies the constructor completes without panicking.
        // No network call is made in new().
        let _client = R2Client::new(
            "test-account-id",
            "test-access-key",
            "test-secret-key",
            "test-bucket",
        )
        .await;
    }

    // -----------------------------------------------------------------------
    // Local-fake integration test (graph-temporal-memory hardening, fix 2)
    // -----------------------------------------------------------------------
    //
    // Before this fix, the two `#[ignore]`d tests below (which require real,
    // live Cloudflare R2 credentials) were the ONLY tests anywhere in the repo
    // that could exercise `get_atom_blob` against an actual network round
    // trip — meaning a successful R2 GET was never actually proven in normal
    // `cargo test` runs, only in whatever environment happens to carry
    // `R2_*` secrets. `new_with_endpoint_override` (behind the `test-utils`
    // feature — see its doc comment for why production code can never reach
    // it) lets this same shape of test run against a LOCAL S3-compatible fake
    // (MinIO, s3mock) instead, with no live cloud credentials required.
    //
    // Mirrors `integration_put_get_delete`/`integration_get_nonexistent_
    // returns_not_found` below exactly, just against the local fake. Run
    // with, e.g.:
    //   docker run -d -p 19000:9000 -e MINIO_ROOT_USER=minioadmin \
    //     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
    //   docker run --rm --network host --entrypoint sh minio/mc -c \
    //     "mc alias set local http://localhost:19000 minioadmin minioadmin && \
    //      mc mb local/cs-test-bucket --ignore-existing"
    //   R2_TEST_ENDPOINT_OVERRIDE=http://localhost:19000 \
    //   R2_TEST_ACCESS_KEY=minioadmin R2_TEST_SECRET_KEY=minioadmin \
    //   R2_TEST_BUCKET=cs-test-bucket \
    //     cargo test -p coralswarm-core --features test-utils -- --ignored r2_local_fake
    #[cfg(feature = "test-utils")]
    #[tokio::test]
    #[ignore]
    async fn r2_local_fake_put_get_delete_round_trips_a_real_network_call() {
        let endpoint = std::env::var("R2_TEST_ENDPOINT_OVERRIDE")
            .expect("R2_TEST_ENDPOINT_OVERRIDE (e.g. http://localhost:19000 for a local MinIO)");
        let access_key = std::env::var("R2_TEST_ACCESS_KEY").expect("R2_TEST_ACCESS_KEY");
        let secret_key = std::env::var("R2_TEST_SECRET_KEY").expect("R2_TEST_SECRET_KEY");
        let bucket = std::env::var("R2_TEST_BUCKET").expect("R2_TEST_BUCKET");

        let client = R2Client::new_with_endpoint_override(
            "unused-account-id",
            &access_key,
            &secret_key,
            &bucket,
            &endpoint,
        )
        .await;

        let graph_id = Uuid::new_v4();
        let atom_id = Uuid::new_v4();
        let data_key = test_key();
        let plaintext = b"local-fake R2 integration test payload";

        let blob_key = client
            .put_atom_blob(None, graph_id, atom_id, plaintext, &data_key)
            .await
            .expect("put should succeed against the local fake");
        assert_eq!(blob_key, format!("{graph_id}/{atom_id}"));

        let recovered = client
            .get_atom_blob(&blob_key, &data_key)
            .await
            .expect("get should succeed — a REAL network round trip, not a stub");
        assert_eq!(recovered, plaintext);

        client
            .delete_atom_blob(&blob_key)
            .await
            .expect("delete should succeed");

        let err = client
            .get_atom_blob(&blob_key, &data_key)
            .await
            .expect_err("should be NotFound after delete");
        assert!(
            matches!(err, R2Error::NotFound(_)),
            "expected NotFound, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Integration tests (require live R2 credentials — skipped in CI)
    // -----------------------------------------------------------------------

    // requires R2 credentials in env: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
    #[tokio::test]
    #[ignore]
    async fn integration_put_get_delete() {
        let account_id = std::env::var("R2_ACCOUNT_ID").expect("R2_ACCOUNT_ID");
        let access_key = std::env::var("R2_ACCESS_KEY_ID").expect("R2_ACCESS_KEY_ID");
        let secret_key = std::env::var("R2_SECRET_ACCESS_KEY").expect("R2_SECRET_ACCESS_KEY");
        let bucket = std::env::var("R2_BUCKET_NAME").expect("R2_BUCKET_NAME");

        let client = R2Client::new(&account_id, &access_key, &secret_key, &bucket).await;

        let graph_id = Uuid::new_v4();
        let atom_id = Uuid::new_v4();
        let data_key = test_key();
        let plaintext = b"integration test payload";

        // Put (personal/consumer scheme — no org prefix).
        let blob_key = client
            .put_atom_blob(None, graph_id, atom_id, plaintext, &data_key)
            .await
            .expect("put should succeed");

        assert_eq!(blob_key, format!("{graph_id}/{atom_id}"));

        // Get.
        let recovered = client
            .get_atom_blob(&blob_key, &data_key)
            .await
            .expect("get should succeed");
        assert_eq!(recovered, plaintext);

        // Delete.
        client
            .delete_atom_blob(&blob_key)
            .await
            .expect("delete should succeed");

        // Get after delete should return NotFound.
        let err = client
            .get_atom_blob(&blob_key, &data_key)
            .await
            .expect_err("should be NotFound after delete");
        assert!(
            matches!(err, R2Error::NotFound(_)),
            "expected NotFound, got {err:?}"
        );
    }

    // requires R2 credentials in env: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
    #[tokio::test]
    #[ignore]
    async fn integration_get_nonexistent_returns_not_found() {
        let account_id = std::env::var("R2_ACCOUNT_ID").expect("R2_ACCOUNT_ID");
        let access_key = std::env::var("R2_ACCESS_KEY_ID").expect("R2_ACCESS_KEY_ID");
        let secret_key = std::env::var("R2_SECRET_ACCESS_KEY").expect("R2_SECRET_ACCESS_KEY");
        let bucket = std::env::var("R2_BUCKET_NAME").expect("R2_BUCKET_NAME");

        let client = R2Client::new(&account_id, &access_key, &secret_key, &bucket).await;
        let data_key = test_key();

        let nonexistent_key = format!(
            "{}/{}",
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        let err = client
            .get_atom_blob(&nonexistent_key, &data_key)
            .await
            .expect_err("should be NotFound");
        assert!(
            matches!(err, R2Error::NotFound(_)),
            "expected NotFound, got {err:?}"
        );
    }
}
