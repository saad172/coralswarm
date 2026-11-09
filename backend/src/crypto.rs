//! Cryptographic helpers shared across the backend.
//!
//! Centralises DEK (Data Encryption Key) resolution so that the embed worker
//! and the query handler use a single, consistent implementation.
//!
//! # Tenant split (org-tenancy PR 1.3)
//! A wrapped DEK lives in one of two places depending on [`TenantId`]:
//! personal accounts keep it on the shared control plane
//! (`accounts.dek_wrapped`); org accounts have it relocated to their own
//! tenant database (`account_keys.dek_wrapped`), reached only via that
//! tenant's [`TenantContext::pool`]. [`resolve_dek`]/[`resolve_dek_cached`]
//! are unchanged (always `Consumer`) so every existing personal-plane call
//! site is byte-identical; [`resolve_dek_for_context`] is the org-aware
//! entry point new tenant-scoped call sites use — it asserts the requested
//! `account_id` matches the context's own `account_id` (the DEK's tenant
//! must match the pool's tenant) and never falls back to the other plane on
//! a mismatch.
//!
//! [`DekPlane`] carries the `(tenant, pool)` pair as ONE value, for the call
//! sites that must RESOLVE which plane an account's DEK lives on before they
//! can read it and cannot get there through any of the three entry points
//! above — chiefly `routes::oauth::callback`, an unauthenticated browser
//! redirect the binding gate never sees (so there is no `TenancyDecision` or
//! `TenantContext` to hand it a pool), which nonetheless has to encrypt
//! `oauth_tokens` under the org's TENANT DEK. Before it existed, that handler
//! called [`resolve_dek`] against the control pool and 503'd
//! ("encryption key not configured") on every org connector install, because
//! an org account has no `accounts.dek_wrapped` at all.
//!
//! [`TenantId`]: crate::tenancy::TenantId
//! [`TenantContext::pool`]: crate::tenancy::runtime::TenantContext

/// Per-purpose HKDF key derivation from shared server secrets — see the
/// module doc for why no HMAC/JWT key may be a raw shared secret.
pub mod kdf;

use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_core::kek::{load_kek, load_kek_next, unwrap_dek_with_rotation};

use crate::dek_cache::DekCache;
use crate::tenancy::runtime::TenantContext;
use crate::tenancy::TenantId;

// ── Error type ────────────────────────────────────────────────────────────────

/// Typed error for DEK / KEK cryptographic operations.
///
/// Callers can pattern-match to distinguish configuration errors (→ 503) from
/// data-level failures (→ 500) without parsing string messages.
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    /// `MASTER_KEK_HEX` environment variable is absent or empty.
    #[error("master KEK not configured: {0}")]
    MasterKekNotSet(String),
    /// `MASTER_KEK_HEX` (or `MASTER_KEK_HEX_NEXT`) is present but not valid hex
    /// / wrong length.
    #[error("master KEK invalid: {0}")]
    MasterKekInvalid(String),
    /// A sqlx database error occurred.
    #[error("database error: {0}")]
    DbError(#[from] sqlx::Error),
    /// The envelope bytes could not be deserialised.
    #[error("envelope deserialization failed: {0}")]
    DeserializationFailed(String),
    /// AES-KWP unwrap of the wrapped DEK failed.
    #[error("DEK unwrap failed: {0}")]
    UnwrapFailed(String),
    /// No account row exists for the given UUID.
    #[error("account not found: {0}")]
    AccountNotFound(Uuid),
    /// The load-bearing tenant/pool assert: the caller asked for a DEK on
    /// behalf of a `TenantContext` scoped to a *different* account than the
    /// one requested. Wrong-key encryption is unrecoverable, so this fails
    /// closed rather than ever falling back to the other plane.
    #[error("tenant mismatch resolving DEK: {0}")]
    TenantMismatch(String),
}

fn classify_kek_error(e: coralswarm_core::kek::KekError) -> CryptoError {
    let msg = e.to_string();
    // Only a genuinely-absent env var (message: "<VAR> not set: ...") is
    // NotSet. A *present but malformed* value — empty, wrong length, bad hex —
    // is a real misconfiguration and must classify as Invalid, not be masked as
    // "not set" by an incidental "missing"/"empty" substring. (Both variants
    // currently 503 at the caller via the same match arm, so this only affects
    // the operator-facing label — but the label must be honest.)
    if msg.to_lowercase().contains("not set") {
        CryptoError::MasterKekNotSet(msg)
    } else {
        CryptoError::MasterKekInvalid(msg)
    }
    // NOTE: `KekError::EnvVar`'s Display template is
    // "KEK env var missing or invalid: {0}" — so it ALWAYS contains "missing".
    // Classification must key off the *inner* distinguisher ("not set", emitted
    // only by the absent-var path in kek.rs), never "missing"/"empty", or the
    // Invalid branch becomes unreachable. Covered by
    // `classify_kek_error_distinguishes_not_set_from_invalid`.
}

/// Load the current (+ in-rotation "next", if configured) master KEK(s).
/// Malformed `MASTER_KEK_HEX_NEXT` is a configuration error like a malformed
/// `MASTER_KEK_HEX` — it must fail loudly (503 at the caller), never silently
/// degrade to single-key operation.
fn load_master_keks() -> Result<([u8; 32], Option<[u8; 32]>), CryptoError> {
    let current = load_kek().map_err(classify_kek_error)?;
    let next = load_kek_next().map_err(classify_kek_error)?;
    Ok((current, next))
}

// ── Legacy oauth-token DEK (CS-031, pre-cutover — Foundation PR 1.2) ──────────

/// The pre-cutover per-account DEK derivation `routes::oauth::
/// derive_account_dek` used to encrypt every `oauth_tokens` row before
/// Foundation PR 1.2: `dek[i] = master_kek[i] ^ account_id_bytes[i % 16]`,
/// where `master_kek` comes from the (now-legacy) `ACCOUNT_MASTER_KEK` env
/// var and `account_id_bytes` are the UUID's raw 16 bytes, repeated. This
/// gave each account a unique DEK without a DB column, at the cost of never
/// being able to rotate the KEK independently per account and reusing no
/// machinery with content encryption.
///
/// **Retained ONLY for the wrapped-DEK cutover backfill** —
/// [`crate::oauth_dek_cutover::reencrypt_oauth_tokens`] (driven by
/// `bin/backfill_oauth_tokens_wrapped_dek`) calls this to decrypt rows still
/// in the legacy format before re-encrypting them under the wrapped DEK
/// ([`resolve_dek`]) every other oauth_tokens read/write path uses as of
/// that PR. No request-path caller may use this function — see that
/// binary's own doc comment for the full cutover runbook, and
/// `routes::oauth`/`routes::connectors`/`workers::huddle`'s doc comments for
/// confirmation the live paths no longer call it.
///
/// # Errors
/// Returns `None` if `ACCOUNT_MASTER_KEK` is absent or not a valid 64-char
/// hex string.
pub fn derive_legacy_oauth_account_dek(account_id: Uuid) -> Option<[u8; 32]> {
    let hex_kek = std::env::var("ACCOUNT_MASTER_KEK").ok()?;
    if hex_kek.len() != 64 {
        return None;
    }

    let kek_bytes = hex::decode(&hex_kek).ok()?;
    let mut kek = [0u8; 32];
    kek.copy_from_slice(&kek_bytes);

    let id_bytes = account_id.as_bytes(); // &[u8; 16]
    let mut dek = [0u8; 32];
    for (i, b) in kek.iter().enumerate() {
        dek[i] = b ^ id_bytes[i % 16];
    }

    Some(dek)
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Fetch the RAW wrapped-DEK bytes for `account_id` on `tenant`, without
/// unwrapping. `TenantId::Consumer` reads the control plane's
/// `accounts.dek_wrapped`; `TenantId::Org(_)` reads the tenant plane's
/// `account_keys.dek_wrapped` on `pool`.
///
/// `pub(crate)` — [`resolve_dek_for_tenant`] uses it internally, and
/// `routes::admin::rotate_dek` uses it directly to distinguish first-provision
/// (no existing row) from rewrap/rotate (existing row) *before* deciding
/// whether it is safe to mint a brand-new DEK (see that handler's doc comment
/// — minting a new DEK over an existing one orphans all content encrypted
/// under the old one).
pub(crate) async fn fetch_wrapped_dek(
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    match tenant {
        TenantId::Consumer => {
            let row: Option<(Option<Vec<u8>>,)> =
                sqlx::query_as("SELECT dek_wrapped FROM accounts WHERE id = $1")
                    .bind(account_id)
                    .fetch_optional(crate::tenancy::scope::request_read(pool))
                    .await?;
            Ok(row.and_then(|(wrapped,)| wrapped))
        }
        TenantId::Org(_) => {
            sqlx::query_scalar("SELECT dek_wrapped FROM account_keys WHERE account_id = $1")
                .bind(account_id)
                .fetch_optional(crate::tenancy::scope::request_read(pool))
                .await
        }
    }
}

/// Resolve the Data Encryption Key for `account_id` on tenant `tenant`.
///
/// `TenantId::Consumer` reads the control plane's `accounts.dek_wrapped`;
/// `TenantId::Org(_)` reads the tenant plane's `account_keys.dek_wrapped` on
/// `pool` — the caller is responsible for `pool` actually being that org's
/// tenant pool (see [`resolve_dek_for_context`], which derives both from the
/// same [`TenantContext`] so they can never be mismatched by construction).
///
/// Returns `Ok(Some(key))` when the DEK is found and successfully unwrapped.
/// Returns `Ok(None)` when no DEK row exists for the account (provisioning
/// incomplete). Returns `Err(CryptoError)` on configuration or database
/// failures — never a partial/fallback result.
pub async fn resolve_dek_for_tenant(
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Option<[u8; 32]>, CryptoError> {
    let (current_kek, next_kek) = load_master_keks()?;

    let Some(wrapped_bytes) = fetch_wrapped_dek(tenant, pool, account_id).await? else {
        return Ok(None);
    };

    let dek = unwrap_dek_with_rotation(&current_kek, next_kek.as_ref(), &wrapped_bytes)
        .map_err(|e| CryptoError::UnwrapFailed(e.to_string()))?;

    Ok(Some(dek))
}

/// Resolve the Data Encryption Key for the given account on the consumer
/// (control) plane. Unchanged behavior — every existing personal-plane call
/// site keeps calling this and stays byte-identical; see
/// [`resolve_dek_for_tenant`] for the tenant-aware entry point.
pub async fn resolve_dek(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Option<[u8; 32]>, CryptoError> {
    resolve_dek_for_tenant(&TenantId::Consumer, pool, account_id).await
}

/// Resolve the Data Encryption Key for `account_id` on the org tenant `ctx`
/// belongs to.
///
/// **Load-bearing assert:** `account_id` must equal `ctx.account_id` — a
/// `TenantContext` is scoped to exactly one org account, so asking it for a
/// different account's DEK is always a bug (a mixed-up caller, never a
/// legitimate cross-account read) and fails closed with
/// [`CryptoError::TenantMismatch`] rather than silently resolving against the
/// wrong plane. `ctx.pool` and `ctx.org_id` are read together off the same
/// struct so they can never independently drift out of sync.
pub async fn resolve_dek_for_context(
    ctx: &TenantContext,
    account_id: Uuid,
) -> Result<Option<[u8; 32]>, CryptoError> {
    if account_id != ctx.account_id {
        return Err(CryptoError::TenantMismatch(format!(
            "requested DEK for account {account_id} but tenant context \
             (org {}) is scoped to account {}",
            ctx.org_id, ctx.account_id
        )));
    }
    resolve_dek_for_tenant(&ctx.tenant_id(), &ctx.pool, account_id).await
}

// ── Cached variant ────────────────────────────────────────────────────────────

/// Resolve the DEK for `account_id` on the consumer plane, consulting `cache`
/// before hitting the DB.
///
/// On a cache miss the DEK is resolved via `resolve_dek` and stored in the
/// cache for future calls.  The embed worker, which lacks `AppState` access,
/// should continue to call `resolve_dek` directly.
pub async fn resolve_dek_cached(
    pool: &PgPool,
    account_id: Uuid,
    cache: &DekCache,
) -> Result<Option<[u8; 32]>, CryptoError> {
    if let Some(dek) = cache.get(&TenantId::Consumer, account_id) {
        return Ok(Some(dek));
    }

    let dek = resolve_dek(pool, account_id).await?;
    if let Some(d) = dek {
        cache.insert(&TenantId::Consumer, account_id, d);
    }
    Ok(dek)
}

/// Resolve the DEK for `account_id` on `tenant`, consulting `cache` before
/// hitting the DB — the cached counterpart to [`resolve_dek_for_tenant`], for
/// callers that already hold a bare [`TenantId`] (no [`TenantContext`]/
/// [`super::tenancy::runtime::TenancyDecision`] on hand, e.g. a
/// `workers::tenant_harness` tick, whose [`super::workers::tenant_harness::
/// WorkerTenant`] carries exactly `(tenant, pool, ..)`). Behaviorally
/// identical to [`resolve_dek_cached`] on `TenantId::Consumer` and to
/// [`resolve_dek_cached_for_context`] on `TenantId::Org(_)` (same DEK table
/// per tenant, same cache key shape) — this is the missing third entry point
/// for a caller that has the tenant/pool pair directly rather than through
/// either of those two existing wrappers' own preconditions.
pub async fn resolve_dek_cached_for_tenant(
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    cache: &DekCache,
) -> Result<Option<[u8; 32]>, CryptoError> {
    if let Some(dek) = cache.get(tenant, account_id) {
        return Ok(Some(dek));
    }

    let dek = resolve_dek_for_tenant(tenant, pool, account_id).await?;
    if let Some(d) = dek {
        cache.insert(tenant, account_id, d);
    }
    Ok(dek)
}

/// Resolve the DEK for `account_id` on the org tenant `ctx` belongs to,
/// consulting `cache` before hitting the tenant DB. Same load-bearing assert
/// as [`resolve_dek_for_context`].
pub async fn resolve_dek_cached_for_context(
    ctx: &TenantContext,
    account_id: Uuid,
    cache: &DekCache,
) -> Result<Option<[u8; 32]>, CryptoError> {
    let tenant = ctx.tenant_id();
    if let Some(dek) = cache.get(&tenant, account_id) {
        return Ok(Some(dek));
    }

    let dek = resolve_dek_for_context(ctx, account_id).await?;
    if let Some(d) = dek {
        cache.insert(&tenant, account_id, d);
    }
    Ok(dek)
}

/// Resolve the DEK for `account_id`, dispatching on the request's
/// [`crate::tenancy::runtime::TenancyDecision`] — the one place org-tenancy PR
/// 1.4's route migration needs to pick between [`resolve_dek_cached`] (the
/// unchanged consumer-plane path) and [`resolve_dek_cached_for_context`] (the
/// tenant-plane path), so each route call site doesn't hand-roll the same
/// match. Mirrors the `None | Some(TenancyDecision::Personal)` convention
/// [`crate::tenancy::reachability::ReachabilityResolver`] already
/// established: absent decision (tenancy runtime off, or a call site not yet
/// wired to the binding gate) behaves exactly like `Personal` — always safe,
/// always byte-identical to today.
///
/// `pool` is only used on the `Personal`/absent arm; the `Org` arm reads
/// `ctx.pool` off the context itself (so the DEK's tenant and the pool it was
/// read from can never independently drift — same reasoning as
/// [`resolve_dek_for_context`]'s own doc comment).
pub async fn resolve_dek_cached_dispatch(
    decision: Option<&crate::tenancy::runtime::TenancyDecision>,
    pool: &PgPool,
    account_id: Uuid,
    cache: &DekCache,
) -> Result<Option<[u8; 32]>, CryptoError> {
    use crate::tenancy::runtime::TenancyDecision;
    match decision {
        None | Some(TenancyDecision::Personal) => resolve_dek_cached(pool, account_id, cache).await,
        Some(TenancyDecision::Org(ctx)) => {
            resolve_dek_cached_for_context(ctx, account_id, cache).await
        }
    }
}


// ── The (tenant, pool) pair, as one value ─────────────────────────────────────

/// Where an account's wrapped DEK physically lives — the tenant dimension AND
/// the pool it must be read from, carried together as ONE value so a caller
/// cannot pass a tenant from one place and a pool from another.
///
/// This exists because the three entry points above each assume the caller
/// already knows the answer: [`resolve_dek`] hardcodes `Consumer`,
/// [`resolve_dek_for_tenant`] takes the pair as two independent arguments, and
/// [`resolve_dek_for_context`] can only be built from a request-scoped
/// [`TenantContext`]. The OAuth callback (`routes::oauth::callback`) has NONE
/// of those: it is an unauthenticated browser redirect, so the binding gate
/// never ran and there is no `TenancyDecision`/`TenantContext` for it — yet it
/// must still write `oauth_tokens` under the RIGHT plane's DEK. It resolves
/// the plane itself (routing row → `TenantRegistry`) and hands the result down
/// as one of these.
///
/// **Never construct an `Org` plane from a pool that is not that org's own
/// tenant pool.** `expect_account` is the structural guard that a mixed-up
/// caller trips instead of silently encrypting under the wrong key: it is the
/// account this plane was RESOLVED for, and [`DekPlane::resolve`] refuses any
/// other account with [`CryptoError::TenantMismatch`] — the same load-bearing
/// assert [`resolve_dek_for_context`] makes against `ctx.account_id`.
#[derive(Clone)]
pub struct DekPlane {
    tenant: TenantId,
    pool: PgPool,
    /// The account this plane was resolved for, when known. `None` only for
    /// the consumer plane built from a bare pool, where there is no
    /// cross-account hazard to guard (one shared plane, one `accounts` row per
    /// account, keyed by the very id being requested).
    expect_account: Option<Uuid>,
}

impl std::fmt::Debug for DekPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never renders the pool (its DSN carries credentials).
        f.debug_struct("DekPlane")
            .field("tenant", &self.tenant)
            .field("expect_account", &self.expect_account)
            .finish_non_exhaustive()
    }
}

impl DekPlane {
    /// The consumer/control plane: `accounts.dek_wrapped` on `pool`.
    /// Byte-identical to calling [`resolve_dek`] with the same pool.
    pub fn consumer(pool: PgPool) -> Self {
        DekPlane {
            tenant: TenantId::Consumer,
            pool,
            expect_account: None,
        }
    }

    /// `org_id`'s own tenant plane: `account_keys.dek_wrapped` on `pool`,
    /// which MUST be that org's fingerprint-verified tenant pool (i.e. one
    /// obtained from [`crate::tenancy::registry::TenantRegistry::get_or_build`],
    /// which asserts the tenant DB's own `tenant_meta` row before returning
    /// it). `account_id` is the account the caller resolved this plane for;
    /// [`Self::resolve`] refuses to serve any other one.
    pub fn org(org_id: String, pool: PgPool, account_id: Uuid) -> Self {
        DekPlane {
            tenant: TenantId::Org(org_id),
            pool,
            expect_account: Some(account_id),
        }
    }

    /// The plane implied by a request's own binding-gate decision — the
    /// counterpart of [`resolve_dek_cached_dispatch`] for call sites that
    /// need the plane as a value (to pass down, log, or assert on) rather
    /// than as a one-shot resolution. Same convention: absent decision
    /// (tenancy runtime off, or a call site not behind the gate) behaves
    /// exactly like `Personal`.
    ///
    /// `control_pool` is used only on the `Personal`/absent arm; the `Org`
    /// arm takes `ctx.pool` and `ctx.account_id` off the context itself, so
    /// the tenant, the pool, and the account can never independently drift.
    pub fn from_decision(
        decision: Option<&crate::tenancy::runtime::TenancyDecision>,
        control_pool: &PgPool,
    ) -> Self {
        use crate::tenancy::runtime::TenancyDecision;
        match decision {
            None | Some(TenancyDecision::Personal) => DekPlane::consumer(control_pool.clone()),
            Some(TenancyDecision::Org(ctx)) => {
                DekPlane::org(ctx.org_id.clone(), ctx.pool.clone(), ctx.account_id)
            }
        }
    }

    /// The plane implied by an ALREADY-RESOLVED `(TenantId, pool)` pair —
    /// the value-level twin of [`resolve_dek_for_tenant`]'s two loose
    /// arguments, for the many Slack/worker call sites that were handed both
    /// by whoever resolved the plane upstream (the binding gate, the tenant
    /// harness's `WorkerTenant`, or `slack_events::resolve_org_plane`).
    ///
    /// This is NOT a second copy of the decision table
    /// ([`Self::resolve_for_org`] is the only one): it is the RESULT of that
    /// decision, already made once by the caller for its CONTENT pool, being
    /// reused for the key so the two cannot disagree. `pool` must be the pool
    /// that pairs with `tenant` — the tenant DB for `Org`, the control pool
    /// for `Consumer` — which is exactly the invariant every `WorkerTenant`
    /// and every `(tenant, pool)` parameter pair in this codebase already
    /// carries.
    pub fn from_tenant(tenant: &TenantId, pool: PgPool, account_id: Uuid) -> Self {
        DekPlane {
            tenant: tenant.clone(),
            pool,
            expect_account: Some(account_id),
        }
    }

    /// The tenant dimension this plane reads from. Also the `DekCache` key
    /// dimension, so a personal and an org DEK for the same `account_id`
    /// never collide.
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// `true` for a genuine org tenant plane. Callers use this to refuse a
    /// cross-plane operation loudly rather than proceed on the wrong key.
    pub fn is_org(&self) -> bool {
        matches!(self.tenant, TenantId::Org(_))
    }

    /// The CONTENT pool this plane resolved to — the org's own tenant pool
    /// for `Org` (built via `TenantRegistry::get_or_build`, fingerprint-
    /// asserted), the control/consumer pool for `Consumer`. Not just a DEK
    /// detail: [`Self::resolve_for_org`]/[`Self::resolve_for_account`] are
    /// already THE decision table for "which pool does this account's data
    /// live on", so a caller with an already-resolved plane and a NON-key
    /// write to make (e.g. `routes::google_push::handle_chat_added_to_space`
    /// handing `connectors::google_chat::record_added_to_space` the tenant
    /// pool for an ordinary org, CS-432/D17 point 4) should reuse this
    /// value rather than resolving the same tenancy a second time via a
    /// parallel helper. Mirrors `TenantContext::pool` being a plain public
    /// field for the same reason on the request-scoped side.
    pub(crate) fn content_pool(&self) -> &PgPool {
        &self.pool
    }

    fn check_account(&self, account_id: Uuid) -> Result<(), CryptoError> {
        match self.expect_account {
            Some(expected) if expected != account_id => Err(CryptoError::TenantMismatch(format!(
                "requested DEK for account {account_id} but this DEK plane ({:?}) was \
                 resolved for account {expected}",
                self.tenant
            ))),
            _ => Ok(()),
        }
    }

    /// Resolve `account_id`'s DEK on this plane. Fails closed on an
    /// account mismatch; never falls back to the other plane.
    pub async fn resolve(&self, account_id: Uuid) -> Result<Option<[u8; 32]>, CryptoError> {
        self.check_account(account_id)?;
        resolve_dek_for_tenant(&self.tenant, &self.pool, account_id).await
    }

    /// [`Self::resolve`], consulting `cache` first. Same fail-closed assert.
    pub async fn resolve_cached(
        &self,
        account_id: Uuid,
        cache: &DekCache,
    ) -> Result<Option<[u8; 32]>, CryptoError> {
        self.check_account(account_id)?;
        resolve_dek_cached_for_tenant(&self.tenant, &self.pool, account_id, cache).await
    }

    /// **THE decision table** — the single definition of "which plane holds
    /// this org's wrapped DEK", for callers that hold neither a
    /// `TenancyDecision` nor an already-resolved `(TenantId, pool)` pair.
    ///
    /// Every such caller is the same shape: a SIGNATURE-authenticated webhook
    /// or an unauthenticated provider redirect that bootstrapped `account_id`
    /// from a provider identifier, with no JWT, no session, and therefore no
    /// binding-gate decision to read a plane off — `routes::oauth::callback`
    /// (the OAuth redirect) and the Slack event/interactivity webhooks. They
    /// must all agree, because a disagreement is a wrong-key write, so there
    /// is exactly one copy of this and it lives here rather than once per
    /// route file.
    ///
    /// The table, and why each arm is what it is:
    ///
    /// * `clerk_org_id` is `None` (personal account) ⇒ consumer plane.
    /// * The tenancy runtime is absent or `enabled == false` ⇒ consumer
    ///   plane. In that mode `clerk_provision::ensure_org_account` provisions
    ///   an org's DEK on the control plane itself (its shadow-provisioning
    ///   fence only skips that under `MULTITENANT_ENABLED`), so control IS
    ///   the org's only plane. This is the deployment mode, not a fallback.
    /// * No `tenant_databases` row for the org ⇒ consumer plane: never
    ///   onboarded onto a tenant database, so whatever DEK it has is on
    ///   control. Same arm `routes::graph_webhook::
    ///   resolve_org_tenant_pool_for_graph_ingest` takes for content. If the
    ///   DEK is missing there too, the caller still gets `Ok(None)` from
    ///   [`Self::resolve`] — no silent wrong-key write is possible either way.
    /// * A routing row exists but is `suspended`/`offboarded`/unrecognised ⇒
    ///   [`DekPlaneError::TenantNotWritable`]. Mid-teardown or write-frozen;
    ///   guessing a plane here is the split-brain this fails closed on.
    ///   `provisioning` IS allowed, matching `routes::admin::rotate_dek`'s own
    ///   status gate — that endpoint provisions the tenant DEK at onboarding
    ///   step 8, while the row is still `provisioning`.
    /// * The routing row's `account_id` disagrees with the requested account
    ///   ⇒ [`DekPlaneError::AccountMismatch`]. Same cross-check `rotate_dek`
    ///   makes before writing a DEK into a tenant's `account_keys`.
    /// * Otherwise ⇒ the org's own tenant plane, via
    ///   `TenantRegistry::get_or_build`, which asserts the tenant DB's
    ///   `tenant_meta` fingerprint before handing back the pool (the
    ///   swapped-DSN defence) — so an `Org` plane can never be built on some
    ///   other org's database.
    ///
    /// **There is no arm that resolves an org WITH a servable tenant database
    /// to the consumer plane.** Every failure is an `Err`, never a quiet
    /// downgrade: callers must be able to tell "this account has no DEK"
    /// (`Ok(plane)` then `resolve() == Ok(None)`) apart from "we could not
    /// determine the plane" (`Err`), because only the first is a fact about
    /// the account and only the second should be retried.
    pub async fn resolve_for_org(
        state: &crate::AppState,
        control_pool: &PgPool,
        account_id: Uuid,
        clerk_org_id: Option<&str>,
    ) -> Result<Self, DekPlaneError> {
        use crate::tenancy::routing::TenantStatus;

        let Some(org_id) = clerk_org_id else {
            return Ok(DekPlane::consumer(control_pool.clone()));
        };
        let Some(runtime) = state.tenant_runtime.as_ref().filter(|rt| rt.enabled) else {
            return Ok(DekPlane::consumer(control_pool.clone()));
        };

        let row = crate::tenancy::routing::lookup_by_org(control_pool, org_id)
            .await
            .map_err(|e| DekPlaneError::RoutingLookup(e.to_string()))?;
        let Some(row) = row else {
            tracing::debug!(
                account_id = %account_id,
                org_id = %org_id,
                "DEK plane: org has no tenant_databases row — its DEK, if any, is on the \
                 control plane (never tenant-onboarded)"
            );
            return Ok(DekPlane::consumer(control_pool.clone()));
        };

        if matches!(
            row.status(),
            Some(TenantStatus::Suspended) | Some(TenantStatus::Offboarded) | None
        ) {
            return Err(DekPlaneError::TenantNotWritable {
                org_id: org_id.to_string(),
                status: row.status.clone(),
            });
        }
        if row.account_id != account_id {
            return Err(DekPlaneError::AccountMismatch {
                org_id: org_id.to_string(),
                row_account: row.account_id,
                requested: account_id,
            });
        }

        let pool = runtime
            .registry
            .get_or_build(org_id, row.account_id, &row.db_secret_ref)
            .await
            .map_err(|e| DekPlaneError::PoolBuild(e.to_string()))?;
        Ok(DekPlane::org(org_id.to_string(), pool, account_id))
    }

    /// [`Self::resolve_for_org`] for a caller that does not already know
    /// whether `account_id` is an org account — one indexed `accounts` lookup
    /// by primary key on the control plane, then the identical table.
    ///
    /// A missing `accounts` row is [`DekPlaneError::AccountNotFound`], never
    /// a silent consumer-plane default: this function's callers are webhooks
    /// that bootstrapped `account_id` from a provider identifier, and "that
    /// account no longer exists" must not be answered with a key.
    pub async fn resolve_for_account(
        state: &crate::AppState,
        control_pool: &PgPool,
        account_id: Uuid,
    ) -> Result<Self, DekPlaneError> {
        let row: Option<Option<String>> =
            sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
                .bind(account_id)
                .fetch_optional(crate::tenancy::scope::request_read(control_pool))
                .await
                .map_err(|e| DekPlaneError::AccountLookup(e.to_string()))?;
        let clerk_org_id = row.ok_or(DekPlaneError::AccountNotFound(account_id))?;
        DekPlane::resolve_for_org(state, control_pool, account_id, clerk_org_id.as_deref()).await
    }
}

/// Why [`DekPlane::resolve_for_org`] could not name a plane.
///
/// Every variant is an INFRASTRUCTURE or wiring condition — none of them
/// means "this account has no key" (that is `Ok(plane)` followed by
/// `resolve() == Ok(None)`). Callers must keep the two apart: #741 and
/// `slack_token_rls_pg::slack_bot_token_lookup_succeeds_under_rls` pin
/// exactly that distinction for the Slack bot-token path, where collapsing
/// them once already turned "this account has no Slack install" into a
/// swallowed error.
#[derive(Debug, thiserror::Error)]
pub enum DekPlaneError {
    /// The control-plane `accounts` lookup failed.
    #[error("account lookup failed: {0}")]
    AccountLookup(String),
    /// No `accounts` row for the requested id.
    #[error("account not found: {0}")]
    AccountNotFound(Uuid),
    /// The `tenant_databases` routing lookup failed.
    #[error("tenant routing lookup failed: {0}")]
    RoutingLookup(String),
    /// The org's tenant is suspended/offboarded/unrecognised — write-frozen,
    /// so no plane may be guessed.
    #[error("tenant for org {org_id} is not writable (status {status})")]
    TenantNotWritable { org_id: String, status: String },
    /// The routing row names a different account than the one requested.
    #[error(
        "routing row for org {org_id} names account {row_account}, not the requested {requested}"
    )]
    AccountMismatch {
        org_id: String,
        row_account: Uuid,
        requested: Uuid,
    },
    /// The tenant pool could not be built (or failed its fingerprint check).
    #[error("tenant pool build failed: {0}")]
    PoolBuild(String),
}

/// An account-INDEPENDENT `(tenant, pool)` pair a background loop resolved
/// ONCE, from which a per-account [`DekPlane`] is minted with
/// [`Self::plane_for`].
///
/// Exists for exactly one caller shape: a `workers::tenant_harness` tick.
/// Those chains (`workers::graph_subscriptions`, `workers::
/// google_watch_renewal`) run several call frames deep, hold **no
/// `AppState`** — so [`DekPlane::resolve_for_account`] is unavailable to them
/// — and touch several accounts per tick, so a single `DekPlane` (which is
/// bound to one account by `expect_account`) cannot be threaded either. This
/// carries the plane decision the harness ALREADY made down to the frame that
/// needs it, without re-deciding it and without an extra `accounts` round
/// trip per account.
///
/// **Not a second copy of the decision table.** [`DekPlane::resolve_for_org`]
/// remains the only one. This is the same "reuse the caller's already-made
/// decision" contract [`DekPlane::from_tenant`] documents — a `WorkerTenant`
/// is handed its `(tenant, pool)` pair by the harness, which resolved it from
/// the routing table through the same `TenantRegistry`; all this adds is
/// picking WHICH of the tick's two pools that tenant's keys live in.
#[derive(Clone)]
pub struct DekPlaneSource {
    tenant: TenantId,
    /// The pool that pairs with `tenant` — the org's own tenant DB for
    /// `Org` (its `account_keys`), the control pool for `Consumer` (its
    /// `accounts.dek_wrapped`).
    pool: PgPool,
}

impl std::fmt::Debug for DekPlaneSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never renders the pool (its DSN carries credentials) — same rule
        // as `DekPlane`'s own Debug.
        f.debug_struct("DekPlaneSource")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl DekPlaneSource {
    /// The consumer/control plane, for a loop that is not tenant-aware.
    pub fn consumer(control_pool: &PgPool) -> Self {
        DekPlaneSource {
            tenant: TenantId::Consumer,
            pool: control_pool.clone(),
        }
    }

    /// The plane whose key store serves a harness tick's `WorkerTenant`.
    ///
    /// `tenant_pool` is the tick's own content pool (`WorkerTenant::pool`),
    /// `control_pool` the shared control/consumer pool the harness also hands
    /// every tick. The `Consumer` arm deliberately takes `control_pool`
    /// rather than `tenant_pool` even though the harness makes them the same
    /// physical pool today: `accounts.dek_wrapped` is a CONTROL table, and
    /// naming it explicitly is what keeps this correct if that identity ever
    /// stops holding (it is only a fact about the current deployment, not an
    /// invariant of the type).
    pub fn for_worker_tenant(
        tenant: &TenantId,
        tenant_pool: &PgPool,
        control_pool: &PgPool,
    ) -> Self {
        let pool = match tenant {
            TenantId::Consumer => control_pool.clone(),
            TenantId::Org(_) => tenant_pool.clone(),
        };
        DekPlaneSource {
            tenant: tenant.clone(),
            pool,
        }
    }

    /// The tenant dimension these planes read from.
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// This source's plane, bound to `account_id` (so a mixed-up caller trips
    /// [`DekPlane`]'s `expect_account` assert instead of resolving some other
    /// account's key).
    pub fn plane_for(&self, account_id: Uuid) -> DekPlane {
        DekPlane::from_tenant(&self.tenant, self.pool.clone(), account_id)
    }
}

impl DekPlane {
    /// Widen an already-resolved plane back into a [`DekPlaneSource`], for a
    /// caller that resolved ONE account's plane (typically via
    /// [`Self::resolve_for_account`]) and must hand it to a helper shared with
    /// the worker loops, which take the account-independent form.
    ///
    /// Sound only because that helper is itself single-account: re-binding via
    /// [`DekPlaneSource::plane_for`] with the SAME `account_id` reproduces this
    /// exact plane. Calling it with a different account would produce a plane
    /// this one's `expect_account` would have refused, so the guarantee is
    /// "the caller must keep using the same account", which is the same
    /// contract [`Self::from_tenant`] already carries.
    pub fn as_source(&self) -> DekPlaneSource {
        DekPlaneSource {
            tenant: self.tenant.clone(),
            pool: self.pool.clone(),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── derive_legacy_oauth_account_dek: formula tests (moved from
    //    routes::oauth alongside the function itself, Foundation PR 1.2) ──────
    //
    // Instead of going through the env-var path (which is not parallel-test
    // safe), these test the underlying XOR derivation formula inline — same
    // idiom the original `routes::oauth` tests used.

    #[test]
    fn derive_legacy_dek_formula_is_deterministic() {
        let kek = [0xAAu8; 32];
        let account_id = Uuid::nil(); // all-zero bytes
        let id_bytes = account_id.as_bytes();

        let mut dek1 = [0u8; 32];
        let mut dek2 = [0u8; 32];
        for (i, b) in kek.iter().enumerate() {
            let xored = b ^ id_bytes[i % 16];
            dek1[i] = xored;
            dek2[i] = xored;
        }
        assert_eq!(dek1, dek2, "derivation must be deterministic");
    }

    #[test]
    fn derive_legacy_dek_different_account_ids_produce_different_deks() {
        let kek = [0x55u8; 32];

        let id_a = Uuid::from_bytes([1u8; 16]);
        let id_b = Uuid::from_bytes([2u8; 16]);

        let mut dek_a = [0u8; 32];
        let mut dek_b = [0u8; 32];
        let id_a_bytes = id_a.as_bytes();
        let id_b_bytes = id_b.as_bytes();
        for (i, b) in kek.iter().enumerate() {
            dek_a[i] = b ^ id_a_bytes[i % 16];
            dek_b[i] = b ^ id_b_bytes[i % 16];
        }

        assert_ne!(dek_a, dek_b, "different accounts must get different DEKs");
    }

    /// Serializes every test in THIS module that touches `ACCOUNT_MASTER_KEK`
    /// — `cargo test` runs `#[test]`s concurrently within one process, so an
    /// unguarded `set_var`/`remove_var` could race a concurrent test's read
    /// of the same process-global env var. Same pattern as
    /// `workers::huddle`'s and `routes::slack_events`'s identically-purposed
    /// (but privately-scoped) locks.
    static LEGACY_KEK_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn derive_legacy_dek_end_to_end_via_env_var() {
        let _guard = LEGACY_KEK_ENV_LOCK.lock().unwrap();
        let prior = std::env::var("ACCOUNT_MASTER_KEK").ok();
        // SAFETY: guarded by `LEGACY_KEK_ENV_LOCK` for this test's whole body.
        unsafe {
            std::env::set_var("ACCOUNT_MASTER_KEK", "cd".repeat(32));
        }

        let account_id = Uuid::new_v4();
        let dek = derive_legacy_oauth_account_dek(account_id)
            .expect("a valid 64-hex-char ACCOUNT_MASTER_KEK must derive a DEK");
        // Deterministic: calling again with the same env/account reproduces
        // the identical bytes (needed so decrypt-after-encrypt round-trips).
        let dek2 = derive_legacy_oauth_account_dek(account_id).expect("must derive again");
        assert_eq!(dek, dek2);

        match prior {
            Some(v) => unsafe { std::env::set_var("ACCOUNT_MASTER_KEK", v) },
            None => unsafe { std::env::remove_var("ACCOUNT_MASTER_KEK") },
        }
    }

    #[test]
    fn derive_legacy_dek_missing_env_returns_none() {
        let _guard = LEGACY_KEK_ENV_LOCK.lock().unwrap();
        let prior = std::env::var("ACCOUNT_MASTER_KEK").ok();
        // SAFETY: guarded by `LEGACY_KEK_ENV_LOCK` for this test's whole body.
        unsafe {
            std::env::remove_var("ACCOUNT_MASTER_KEK");
        }

        assert!(derive_legacy_oauth_account_dek(Uuid::new_v4()).is_none());

        match prior {
            Some(v) => unsafe { std::env::set_var("ACCOUNT_MASTER_KEK", v) },
            None => unsafe { std::env::remove_var("ACCOUNT_MASTER_KEK") },
        }
    }

    #[test]
    fn derive_legacy_dek_malformed_hex_returns_none() {
        let _guard = LEGACY_KEK_ENV_LOCK.lock().unwrap();
        let prior = std::env::var("ACCOUNT_MASTER_KEK").ok();
        // SAFETY: guarded by `LEGACY_KEK_ENV_LOCK` for this test's whole body.
        unsafe {
            std::env::set_var("ACCOUNT_MASTER_KEK", "too-short");
        }

        assert!(derive_legacy_oauth_account_dek(Uuid::new_v4()).is_none());

        match prior {
            Some(v) => unsafe { std::env::set_var("ACCOUNT_MASTER_KEK", v) },
            None => unsafe { std::env::remove_var("ACCOUNT_MASTER_KEK") },
        }
    }

    /// Verify that CryptoError variants produce the expected Display strings.
    #[test]
    fn crypto_error_display_strings() {
        let id = Uuid::nil();

        assert!(
            CryptoError::MasterKekNotSet("env var missing".to_string())
                .to_string()
                .contains("master KEK not configured")
        );
        assert!(
            CryptoError::MasterKekInvalid("bad hex".to_string())
                .to_string()
                .contains("master KEK invalid")
        );
        assert!(
            CryptoError::UnwrapFailed("bad padding".to_string())
                .to_string()
                .contains("DEK unwrap failed")
        );
        assert!(
            CryptoError::AccountNotFound(id)
                .to_string()
                .contains("account not found")
        );
        assert!(
            CryptoError::DeserializationFailed("bad bytes".to_string())
                .to_string()
                .contains("envelope deserialization failed")
        );
    }

    /// Verify CryptoError::DbError is constructible from sqlx::Error.
    #[test]
    fn crypto_error_from_sqlx() {
        let sqlx_err = sqlx::Error::RowNotFound;
        let err: CryptoError = sqlx_err.into();
        assert!(err.to_string().contains("database error"));
    }

    /// HTTP status code mapping contract: callers must map errors as follows:
    ///   MasterKekNotSet | MasterKekInvalid | DbError → 503 Service Unavailable
    ///   UnwrapFailed | DeserializationFailed | AccountNotFound → 500 Internal Server Error
    #[test]
    fn crypto_error_http_status_mapping() {
        fn expected_503(e: &CryptoError) -> bool {
            matches!(
                e,
                CryptoError::MasterKekNotSet(_)
                    | CryptoError::MasterKekInvalid(_)
                    | CryptoError::DbError(_)
            )
        }
        fn expected_500(e: &CryptoError) -> bool {
            matches!(
                e,
                CryptoError::UnwrapFailed(_)
                    | CryptoError::DeserializationFailed(_)
                    | CryptoError::AccountNotFound(_)
            )
        }

        assert!(expected_503(&CryptoError::MasterKekNotSet("x".into())));
        assert!(expected_503(&CryptoError::MasterKekInvalid("x".into())));
        assert!(expected_503(&CryptoError::DbError(sqlx::Error::RowNotFound)));
        assert!(expected_500(&CryptoError::UnwrapFailed("x".into())));
        assert!(expected_500(&CryptoError::DeserializationFailed("x".into())));
        assert!(expected_500(&CryptoError::AccountNotFound(Uuid::nil())));
        // TenantMismatch is neither of the above buckets in this helper's
        // classification (see its own dedicated mapping-contract test below).
        assert!(!expected_503(&CryptoError::TenantMismatch("x".into())));
        assert!(!expected_500(&CryptoError::TenantMismatch("x".into())));
    }

    /// `TenantMismatch` is a caller/wiring bug (never a client-triggerable
    /// condition), so it maps to 500 — asserted as its own contract test
    /// rather than folded into `expected_500` above, since a caller-bug class
    /// is conceptually distinct from the data-level 500s enumerated there.
    #[test]
    fn tenant_mismatch_maps_to_500() {
        fn expected_500(e: &CryptoError) -> bool {
            matches!(e, CryptoError::TenantMismatch(_))
        }
        assert!(expected_500(&CryptoError::TenantMismatch("x".into())));
    }

    /// `TenantMismatch`'s Display carries only the non-secret identifiers its
    /// single construction site (`resolve_dek_for_context`) interpolates — two
    /// account UUIDs + the Clerk org id — and never key material. Built through
    /// the real constructor so the guarantee is asserted where the string is
    /// actually formatted, not on a hand-made variant that could drift from it.
    /// (A generic `.contains("tenant mismatch")` alone would pass even if the
    /// whole detail — or a DEK — leaked; the hex-run check is the real guard.)
    #[tokio::test]
    async fn tenant_mismatch_display_has_no_key_material() {
        let ctx = TenantContext {
            org_id: "org_sentinel".to_string(),
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects"),
            page_pool: None,
        };
        let other = Uuid::new_v4();
        let display = resolve_dek_for_context(&ctx, other)
            .await
            .expect_err("mismatched account_id must be rejected")
            .to_string();

        assert!(display.contains("tenant mismatch"));
        // Informative: names the ids so an operator can debug the wiring bug.
        assert!(display.contains(&other.to_string()));
        assert!(display.contains(&ctx.account_id.to_string()));
        assert!(display.contains("org_sentinel"));
        // The load-bearing guarantee: no 32-byte (64-hex-char) key-material run
        // is ever present. UUIDs are hex but max 12 contiguous hex chars, so a
        // 64-run can only come from a leaked KEK/DEK. Fails loud if one ever is.
        let longest_hex_run = display
            .split(|c: char| !c.is_ascii_hexdigit())
            .map(str::len)
            .max()
            .unwrap_or(0);
        assert!(
            longest_hex_run < 64,
            "TenantMismatch Display must never embed key material (hex run {longest_hex_run}): {display}"
        );
    }

    /// The load-bearing assert: a `TenantContext` scoped to one account must
    /// refuse to resolve a DEK for a *different* account — fails closed with
    /// `TenantMismatch`, never falling back to another plane. A lazily-built
    /// pool never actually connects unless a query executes against it, so
    /// this exercises the assert (which short-circuits before any DB access)
    /// with no live Postgres required.
    #[tokio::test]
    async fn resolve_dek_for_context_rejects_account_id_mismatch() {
        let ctx = TenantContext {
            org_id: "org_a".to_string(),
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects"),
            page_pool: None,
        };
        let other_account = Uuid::new_v4();

        let err = resolve_dek_for_context(&ctx, other_account)
            .await
            .expect_err("mismatched account_id must be rejected before any DB access");
        assert!(matches!(err, CryptoError::TenantMismatch(_)));
    }

    /// `resolve_dek_cached_dispatch`'s `Org` arm must delegate to
    /// [`resolve_dek_cached_for_context`] — not silently fall back to the
    /// consumer plane — for org-tenancy PR 1.4's route call sites
    /// (`atoms::get_atom`, `queries::fetch_atom_content`,
    /// `ingest::submit`). Proven the same way
    /// `resolve_dek_for_context_rejects_account_id_mismatch` proves the
    /// non-dispatched entry point: the account-id-mismatch fail-closed assert
    /// short-circuits before any DB access, so this needs neither a live
    /// Postgres pool nor `MASTER_KEK_HEX` configured in the test process.
    #[tokio::test]
    async fn resolve_dek_cached_dispatch_org_arm_delegates_to_context_resolution() {
        let ctx = TenantContext {
            org_id: "org_a".to_string(),
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects"),
            page_pool: None,
        };
        let other_account = Uuid::new_v4();
        let cache = DekCache::new();
        // Ignored by the `Org` arm (it reads `ctx.pool` off the context
        // instead) — a second lazy pool, never connected, stands in for "the
        // caller's ambient pool" a real Personal-arm call site would pass.
        let unused_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool construction never connects");
        let decision = crate::tenancy::runtime::TenancyDecision::Org(ctx);

        let err = resolve_dek_cached_dispatch(Some(&decision), &unused_pool, other_account, &cache)
            .await
            .expect_err("mismatched account_id must be rejected before any DB access");
        assert!(
            matches!(err, CryptoError::TenantMismatch(_)),
            "dispatch's Org arm must reach the SAME load-bearing assert as \
             resolve_dek_cached_for_context, not a generic/consumer-path error"
        );
    }

    /// A `DekPlane` resolved for one account must refuse a DIFFERENT one —
    /// the same fail-closed assert [`resolve_dek_for_context`] makes, moved
    /// onto the value the OAuth callback carries down to
    /// `store_token_bundle_tx`. Short-circuits before any DB access, so a
    /// never-connected lazy pool is enough.
    #[tokio::test]
    async fn dek_plane_org_refuses_a_different_account() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool construction never connects");
        let account = Uuid::new_v4();
        let plane = DekPlane::org("org_a".to_string(), pool, account);
        assert!(plane.is_org());
        assert!(matches!(plane.tenant(), TenantId::Org(o) if o == "org_a"));

        let err = plane
            .resolve(Uuid::new_v4())
            .await
            .expect_err("a plane resolved for one account must never serve another");
        assert!(matches!(err, CryptoError::TenantMismatch(_)));

        let cache = DekCache::new();
        let cached_err = plane
            .resolve_cached(Uuid::new_v4(), &cache)
            .await
            .expect_err("the cached path must reach the SAME assert, not skip it");
        assert!(matches!(cached_err, CryptoError::TenantMismatch(_)));
    }

    /// `DekPlane::from_decision` mirrors [`resolve_dek_cached_dispatch`]'s
    /// convention exactly: absent decision and `Personal` both mean the
    /// consumer plane, `Org` means that org's tenant plane — and the `Org`
    /// arm takes its account from the context, so the assert above is armed
    /// automatically rather than by caller discipline.
    #[tokio::test]
    async fn dek_plane_from_decision_matches_the_dispatch_convention() {
        let control = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool construction never connects");
        let ctx = TenantContext {
            org_id: "org_a".to_string(),
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects"),
            page_pool: None,
        };

        assert!(!DekPlane::from_decision(None, &control).is_org());
        assert!(
            !DekPlane::from_decision(
                Some(&crate::tenancy::runtime::TenancyDecision::Personal),
                &control
            )
            .is_org()
        );

        let expected_account = ctx.account_id;
        let decision = crate::tenancy::runtime::TenancyDecision::Org(ctx);
        let plane = DekPlane::from_decision(Some(&decision), &control);
        assert!(plane.is_org());
        // Armed with the context's own account: any other id fails closed.
        assert!(matches!(
            plane.resolve(Uuid::new_v4()).await,
            Err(CryptoError::TenantMismatch(_))
        ));
        // And its Debug never renders the pool (whose DSN carries credentials).
        let rendered = format!("{plane:?}");
        assert!(rendered.contains("org_a"));
        assert!(rendered.contains(&expected_account.to_string()));
        assert!(!rendered.contains("postgres://"));
    }

    /// Guards the reachability of BOTH classify branches. `KekError::EnvVar`'s
    /// Display always contains "missing"/"invalid" (its template), so an absent
    /// var and a malformed value must be distinguished by the *inner* text, not
    /// the template. A regression that keys off "missing"/"empty" would make
    /// `MasterKekInvalid` unreachable — this test fails if that happens.
    #[test]
    fn classify_kek_error_distinguishes_not_set_from_invalid() {
        use coralswarm_core::kek::KekError;
        let not_set = classify_kek_error(KekError::EnvVar(
            "MASTER_KEK_HEX not set: environment variable not found".into(),
        ));
        assert!(matches!(not_set, CryptoError::MasterKekNotSet(_)));

        for bad in [
            "MASTER_KEK_HEX: expected 64 hex chars (32 bytes), got 3 chars",
            "MASTER_KEK_HEX: invalid hex: odd length",
            "MASTER_KEK_HEX: decoded 31 bytes, expected 32",
        ] {
            assert!(
                matches!(
                    classify_kek_error(KekError::EnvVar(bad.into())),
                    CryptoError::MasterKekInvalid(_)
                ),
                "present-but-malformed KEK must classify Invalid: {bad}"
            );
        }
    }
}

/// Unwrap the exact key selected by a worker's member READ ONLY snapshot.
/// Pure cryptography: opens no pool and never returns key material in a payload.
pub(crate) fn unwrap_snapshot_dek(wrapped: &[u8]) -> Result<[u8;32],CryptoError> {
    let (current,next)=load_master_keks()?;
    unwrap_dek_with_rotation(&current,next.as_ref(),wrapped).map_err(|e|CryptoError::UnwrapFailed(e.to_string()))
}
