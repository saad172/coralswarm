//! Per-purpose key derivation from a shared server secret.
//!
//! # Why
//!
//! A raw secret used as the HMAC key for two different jobs turns either job
//! into a signing oracle for the other. Concretely: `OAUTH_STATE_SECRET` used
//! to key both the HS256 OAuth `state` JWTs (`oauth::state`) and the
//! `query_audit_log.question_digest` HMAC (`services::query_audit`), and an
//! HS256 signature IS `HMAC-SHA256(key, "<header>.<payload>")`. Anyone who
//! could ask a question and read the audit log could therefore ask the text
//! `"<header>.<payload>"` and receive a valid state-token signature for it.
//!
//! The rule this module enforces: **never key an HMAC or a JWT with a raw
//! shared secret.** Every use derives its own key with [`derive_key`] and a
//! purpose label from [`purpose`], so an output produced under one purpose is
//! useless under every other.
//!
//! # How
//!
//! Full HKDF-SHA256 (RFC 5869) — extract, then expand — with no salt and
//! `info = "<purpose>:v1"`. The extract step matters: it computes
//! `PRK = HMAC(zero_salt, secret)`, so the secret is the HMAC *message*, not
//! the key. An expand-only construction would make the derived key
//! `HMAC(secret, info || 0x01)`, which any remaining raw-key HMAC oracle over
//! attacker-chosen text could compute directly. The `kdf` tests pin this.
//!
//! # Adding a purpose
//!
//! Add a constant to [`purpose`] (lowercase, `-`-separated, never reused, never
//! renamed — the label is part of the key) and call
//! `derive_key(secret.as_bytes(), purpose::YOUR_PURPOSE)`. To rotate one
//! purpose's key without touching the secret, switch that call site to
//! [`derive_key_versioned`] with `version` 2 (and so on).

use hkdf::Hkdf;
use sha2::Sha256;

/// One purpose's key derived from `MASTER_KEK_HEX`, for the HMAC uses that
/// key from the master KEK rather than from `OAUTH_STATE_SECRET`
/// (`routes::orgs_create::cap_bucket_key`, `routes::google_push::
/// gmail_mailbox_key`). One place loads the KEK, derives and logs, so no
/// caller repeats the pattern.
///
/// # Caching, and logging once
///
/// The outcome, key or no key, is kept in a `OnceLock` after the first
/// call. The KEK comes from the process environment, which cannot change
/// while the process runs: a rotation (`coralswarm_core::kek`'s
/// `MASTER_KEK_HEX_NEXT` procedure) flips the variables and restarts the
/// process, and a KEK that is unset or malformed at boot stays so until the
/// next boot. `load_kek_next` is for unwrapping DEKs stored under the old
/// key; nothing derived here outlives the rotation window (a dedupe key
/// lives while its job is pending, a cap bucket 24 hours), so a derived key
/// only ever comes from the CURRENT KEK, and the process-wide cache is
/// exact. A failure is cached for the same reason a success is: no KEK can
/// appear later in this process, so re-reading the environment on every
/// Gmail push or org creation would only repeat the same answer, and the
/// same log line. The failure is therefore logged exactly once per process
/// per purpose, when it is first seen. Unit tests set different KEKs in one
/// process, so the cache is off under `cfg(test)` (and so is the log-once
/// guard, which lives in the same cell).
pub struct DerivedKey {
    purpose: &'static str,
    /// Whether an unset `MASTER_KEK_HEX` is worth a warning. A malformed
    /// one always logs at error with `alert`: that is a misconfiguration.
    /// An unset one is a bare dev environment, which some callers (the
    /// self-serve cap buckets, which have always fallen back quietly) do
    /// not want a log line for, and others (the Gmail dedupe key, whose
    /// absence means pushes stop coalescing) do.
    warn_when_unset: bool,
    #[cfg(not(test))]
    cell: std::sync::OnceLock<Option<[u8; DERIVED_KEY_LEN]>>,
}

impl DerivedKey {
    /// A key for `purpose` that stays quiet when `MASTER_KEK_HEX` is unset.
    pub const fn new(purpose: &'static str) -> Self {
        DerivedKey {
            purpose,
            warn_when_unset: false,
            #[cfg(not(test))]
            cell: std::sync::OnceLock::new(),
        }
    }

    /// A key for `purpose` that logs once at warn when `MASTER_KEK_HEX` is
    /// unset.
    pub const fn warning_when_unset(purpose: &'static str) -> Self {
        DerivedKey {
            purpose,
            warn_when_unset: true,
            #[cfg(not(test))]
            cell: std::sync::OnceLock::new(),
        }
    }

    /// The derived key, or `None` when `MASTER_KEK_HEX` is unset (a bare
    /// dev environment, logged once at warn only for a key built with
    /// [`Self::warning_when_unset`]) or set but malformed (a
    /// misconfiguration, logged once at error with `alert`). The log never
    /// carries the key or the decode error (`hex::decode`'s error names the
    /// offending character); callers decide what their weaker fallback is.
    pub fn get(&self) -> Option<[u8; DERIVED_KEY_LEN]> {
        #[cfg(not(test))]
        {
            *self.cell.get_or_init(|| self.load())
        }
        #[cfg(test)]
        {
            self.load()
        }
    }

    /// One read of the environment, logging the failure it finds.
    fn load(&self) -> Option<[u8; DERIVED_KEY_LEN]> {
        match coralswarm_core::kek::load_kek() {
            Ok(kek) => Some(derive_key(&kek, self.purpose)),
            Err(_) => {
                let set = std::env::var_os("MASTER_KEK_HEX").is_some_and(|v| !v.is_empty());
                if set {
                    tracing::error!(
                        alert = true,
                        purpose = self.purpose,
                        "MASTER_KEK_HEX is set but invalid (expected 64 hex chars); no derived key for this purpose \
                         for the rest of this process"
                    );
                } else if self.warn_when_unset {
                    tracing::warn!(
                        purpose = self.purpose,
                        "MASTER_KEK_HEX is unset; no derived key for this purpose for the rest of this process"
                    );
                }
                None
            }
        }
    }
}

/// Length of every derived key, in bytes (the HMAC-SHA256 block-friendly size).
pub const DERIVED_KEY_LEN: usize = 32;

/// Purpose labels. Each label is part of the derived key: renaming one
/// silently changes every key derived for it, so treat them as wire values.
pub mod purpose {
    /// HS256 OAuth `state` tokens (`oauth::state`).
    pub const OAUTH_STATE: &str = "oauth-state";
    /// `query_audit_log.question_digest` HMAC (`services::query_audit`).
    pub const QUERY_AUDIT_DIGEST: &str = "query-audit-digest";
    /// HS256 Teams member-onboarding tickets (`routes::teams_onboarding`).
    pub const TEAMS_MEMBER_ONBOARDING: &str = "teams-member-onboarding";
    /// HS256 Teams meeting-capture intents (`services::teams_meeting_capture`).
    pub const TEAMS_MEETING_CAPTURE: &str = "teams-meeting-capture";
    /// Account-deletion cancel-link signing (`services::account_deletion`).
    pub const ACCOUNT_DELETION_CANCEL: &str = "account-deletion-cancel";
    /// Free-mail cap buckets of self-serve org creation
    /// (`routes::orgs_create::cap_bucket_for`), keyed from `MASTER_KEK_HEX`.
    pub const SELFSERVE_CAP_BUCKET: &str = "selfserve-cap-bucket";
    /// Gmail push receive-job dedupe key, an HMAC of the mailbox address
    /// (`routes::google_push::gmail_mailbox_key`), keyed from `MASTER_KEK_HEX`.
    pub const GMAIL_PUSH_MAILBOX: &str = "gmail-push-mailbox";
}

/// Derive the `v1` key for `purpose` from `secret`.
///
/// `info = "<purpose>:v1"`. Use a constant from [`purpose`], never a runtime
/// string.
pub fn derive_key(secret: &[u8], purpose: &str) -> [u8; DERIVED_KEY_LEN] {
    derive_key_versioned(secret, purpose, 1)
}

/// Derive the key for `purpose` at an explicit `version`
/// (`info = "<purpose>:v<version>"`). For rotating one purpose's key without
/// rotating the underlying secret.
pub fn derive_key_versioned(secret: &[u8], purpose: &str, version: u32) -> [u8; DERIVED_KEY_LEN] {
    let hk = Hkdf::<Sha256>::new(None, secret);
    let info = format!("{purpose}:v{version}");
    let mut okm = [0u8; DERIVED_KEY_LEN];
    hk.expand(info.as_bytes(), &mut okm)
        .expect("32 bytes is far below HKDF-SHA256's 255*32 byte output limit");
    okm
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::{Hmac, Mac};

    const SECRET: &[u8] = b"kdf-test-secret-0123456789abcdef";

    fn raw_hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(msg);
        mac.finalize().into_bytes().to_vec()
    }

    #[test]
    fn matches_rfc5869_hkdf_sha256_with_purpose_v1_info() {
        // Independent recomputation of extract+expand with plain HMAC, so the
        // construction (no salt, info = "<purpose>:v1", one block) is pinned
        // rather than whatever the hkdf crate happens to default to.
        let prk = raw_hmac(&[0u8; 32], SECRET);
        let t1 = raw_hmac(&prk, b"oauth-state:v1\x01");
        assert_eq!(derive_key(SECRET, purpose::OAUTH_STATE).to_vec(), t1);
    }

    #[test]
    fn deterministic() {
        assert_eq!(
            derive_key(SECRET, purpose::QUERY_AUDIT_DIGEST),
            derive_key(SECRET, purpose::QUERY_AUDIT_DIGEST)
        );
    }

    #[test]
    fn every_purpose_gets_a_distinct_key_and_none_equals_the_raw_secret() {
        let all = [
            purpose::OAUTH_STATE,
            purpose::QUERY_AUDIT_DIGEST,
            purpose::TEAMS_MEMBER_ONBOARDING,
            purpose::TEAMS_MEETING_CAPTURE,
            purpose::ACCOUNT_DELETION_CANCEL,
            purpose::SELFSERVE_CAP_BUCKET,
            purpose::GMAIL_PUSH_MAILBOX,
        ];
        let keys: Vec<_> = all.iter().map(|p| derive_key(SECRET, p)).collect();
        for (i, a) in keys.iter().enumerate() {
            assert_ne!(&a[..], SECRET);
            for b in &keys[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn version_and_secret_change_the_key() {
        assert_ne!(
            derive_key(SECRET, purpose::OAUTH_STATE),
            derive_key_versioned(SECRET, purpose::OAUTH_STATE, 2)
        );
        assert_ne!(
            derive_key(SECRET, purpose::OAUTH_STATE),
            derive_key(b"another-secret-0123456789abcdef", purpose::OAUTH_STATE)
        );
    }

    #[test]
    fn derived_key_is_not_computable_from_a_raw_secret_hmac_oracle() {
        // An expand-only KDF would give HMAC(secret, info || 0x01) — exactly
        // what a raw-secret HMAC over attacker-chosen text returns. The
        // extract step must keep the derived key out of that oracle's reach.
        for p in [purpose::OAUTH_STATE, purpose::QUERY_AUDIT_DIGEST] {
            let mut msg = format!("{p}:v1").into_bytes();
            msg.push(0x01);
            assert_ne!(derive_key(SECRET, p).to_vec(), raw_hmac(SECRET, &msg));
        }
    }
}
