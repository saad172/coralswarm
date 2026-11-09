//! Revocable per-coding-session share links (coding-session capture Phase 5,
//! audit amendment A7) — a faithful clone of `routes::shares`
//! (cs-uxp3b-library-search), retargeted at `coding_sessions` (migration
//! 0076) instead of `meeting_transcripts`.
//!
//! - `POST /v1/sessions/shares` — mint a new share link for a coding session
//!   the caller owns (graph-reachability guarded, same as `routes::shares`).
//! - `POST /v1/sessions/shares/{id}/revoke` — permanently revoke a share.
//! - `GET  /v1/shared/sessions/{token}` — PUBLIC (no auth) viewer, `'link'`
//!   shares only. Mounted in the `open` router in `lib::app`, rate-limited
//!   per token. Returns the session's metadata plus its corals, oldest first.
//!
//! Deliberately narrower than `routes::shares`: no `GET /v1/sessions/shares`
//! (list) route and no authed `.../view/{token}` route — the store methods
//! backing them exist (`Store::list_session_shares`,
//! `Store::get_active_session_share_by_token`) for parity/testability, but
//! only the three routes above are wired up per the coding-session capture
//! Phase 5 scope (no web UI wiring in this phase either — see migration
//! 0076's header and the audit's build-order doc).
//!
//! # Security model
//! * The bearer secret is the `token` itself — see `routes::shares::
//!   generate_share_token`'s doc comment (reused verbatim here, not
//!   duplicated).
//! * Revoked and unknown tokens are indistinguishable to every caller: both
//!   resolve to the identical generic 404 body.
//! * The public route is rate-limited per token (not per caller), same as
//!   `routes::shares::public_share`.
//!
//! # Content scope + redaction (documented per the task's explicit ask)
//! * **Scope**: the public payload's `corals` list is built from
//!   `Store::list_shared_session_corals`, which matches ONLY via
//!   `atoms.source_id LIKE 'cc:{session_id}:%'` scoped to the share's own
//!   `graph_id` (`session_source_id_prefix` — no new LIKE logic; the exact
//!   same escaping/prefix convention `list_coding_sessions`'s `coral_count`
//!   subquery and the `search_atoms`/`ask_ocean` session filter already use).
//!   A share can therefore never surface another session's corals, another
//!   graph's data, or anything beyond this one session's content.
//! * **Redaction**: `hostname` and `repo_path` are OMITTED from the public
//!   payload. The meeting-share public payload's own judgment call
//!   (`routes::shares::build_shared_payload`) is to return only
//!   content-equivalent fields (`title`, `occurred_at`, `duration_ms`,
//!   `summary`, `transcript`) and never any auxiliary identity/environment
//!   metadata — notably, it does NOT surface `created_by` even though the
//!   `MeetingShare` row carries it. Applying that same judgment here:
//!   `hostname` (a local machine name) and `repo_path` (a local filesystem
//!   path, which on most setups embeds the OS username, e.g.
//!   `/home/dev/...`) are auxiliary environment metadata, not part of
//!   the "content" a session share exists to convey, and are not required
//!   for someone to read/resume the work. They are redacted. `project_key`,
//!   `title`, and `remote_sanitized` (already credential-stripped by A5) ARE
//!   returned — they're either already-sanitized or non-identifying, and are
//!   exactly the "which project, what was this session" context D7 ("people
//!   pick up work") calls for.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.5b)
//!
//! `coding_session_shares`, `coding_sessions` and `atoms` all carry 0083
//! policies, so every read/write below runs inside a
//! [`crate::tenancy::scope::begin_account_scoped`] transaction. This is the
//! faithful clone of `routes::shares`' B.3b conversion, and it inherits the
//! same two structural facts (which is why migration
//! `0087_session_share_bootstrap_functions` exists, mirroring 0086's
//! meeting-share pair):
//!
//! 1. **The scoping account is the share OWNER's, never the caller's.** Shares
//!    are reachability-gated (ADR-0007 Model B), so a legitimate viewer may sit
//!    in a DIFFERENT account from the ocean's owner — scoping to
//!    `user.account_id` would 404 exactly the cross-account readers the feature
//!    exists for. Resolved from the graph via `Store::graph_account_id`
//!    (0084's `resolve_graph_account_id`).
//! 2. **The public viewer authenticates by CAPABILITY TOKEN and has no account
//!    at all** — it is an ANONYMOUS route on the `open` router, so there is not
//!    even a caller account to fall back on. `resolve_session_share_account_id`
//!    (0087) is that first hop: bare `account_id`, ACTIVE shares only, so a
//!    revoked or unknown token resolves to NULL and the route returns the same
//!    generic 404 it always did. `revoke_session_share` does the same via
//!    `resolve_session_share_graph_id`, which matches ANY share (active or
//!    revoked) so a double-revoke still no-ops rather than 404ing.
//!
//! `build_shared_session_payload` scopes PER STATEMENT rather than sharing one
//! transaction across the whole payload build: it interleaves DB reads with R2
//! blob fetches + AES decrypts (one per coral), and pinning a pooled connection
//! across that network I/O is exactly what PR #291's `JobScope` rationale rules
//! out. Every read there is independent and already best-effort, so the failure
//! semantics are unchanged.
//!
//! A pool-less `Tenant` keeps the plain Store path throughout, exactly as the
//! B.1/B.3b routes do.
//!
//! **Known gap, deliberately not papered over:**
//! `Store::list_shared_session_corals` has NO `_scoped` twin on the `Store`
//! trait yet, so the public payload's coral list still runs ambient.
//! Post-OPS-4 it will hard-error into its existing
//! `Err => tracing::warn!; return (Vec::new(), false)` arm — the share still
//! resolves and still returns its session metadata, only the coral list comes
//! back empty. Adding the twin is a `Store`-layer change, out of this lane's
//! scope (which must not touch `store/mod.rs`); it is called out here and in
//! the PR body rather than left to be discovered during the OPS-4 soak.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::store::Store;
use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver, AppState,
};

// Content-plane store, resolved by the `Tenant` extractor (org-tenancy PR
// 1.6; was ambient `state.store`) — `coding_session_shares`/
// `coding_sessions`/`atoms` are tenant-tree tables. The PUBLIC route below
// passes `Tenant::legacy(&state)` (see its own doc comment) rather than the
// axum extractor, so this macro serves both.
macro_rules! require_store {
    ($tenant:expr) => {
        match &$tenant.store {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
            }
        }
    };
}

fn internal_error() -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "internal error"})),
    )
}

/// The share owner's account for `graph_id`, or a ready-to-return error signal
/// (org-tenancy OPS-4 phase B.5b — see the module doc's RLS section).
/// `Ok(None)` means "no such graph"; callers map it to their own generic 404 so
/// a deleted ocean stays indistinguishable from an unreachable one. Verbatim
/// counterpart of `routes::shares::owner_account`.
async fn owner_account(store: &dyn Store, graph_id: Uuid) -> Result<Option<Uuid>, ()> {
    match store.graph_account_id(graph_id).await {
        Ok(a) => Ok(a),
        Err(e) => {
            tracing::error!(error = %e, "session_shares: graph account lookup failed");
            Err(())
        }
    }
}

// ── POST /v1/sessions/shares ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateSessionShareRequest {
    pub graph_id: Uuid,
    pub session_id: String,
    /// `"link"` | `"team"`. Defaults to `"link"` when omitted.
    #[serde(default)]
    pub visibility: Option<String>,
}

pub async fn create_session_share(
    State(_state): State<Arc<AppState>>,
    _tenant: Tenant,
    Extension(_user): Extension<UserContext>,
    Json(_body): Json<CreateSessionShareRequest>,
) -> (StatusCode, Json<Value>) {
    return (StatusCode::CONFLICT,Json(json!({"error":"Whole-original links require selected evidence consent","code":"selected_evidence_consent_required"})));
}

// ── POST /v1/sessions/shares/{id}/revoke ─────────────────────────────────────

pub async fn revoke_session_share(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // RLS bootstrap (org-tenancy OPS-4 phase B.5b): this handler receives a
    // bare share id and its IDOR guard is graph REACHABILITY, so the scoping
    // account can only come from the share's OWNING graph — which we cannot
    // read under a scope we don't have yet. `resolve_session_share_graph_id`
    // (0087) breaks that cycle: bare `graph_id` only, ANY share active or
    // revoked (so a double-revoke behaves exactly as it did unscoped).
    let graph_lookup = match tenant.pool.as_ref() {
        // A scalar function call always returns exactly one row, with NULL for
        // "no such share" — hence `fetch_one` into `Option<Uuid>`, not
        // `fetch_optional` (same idiom as `routes::shares::revoke_share`).
        Some(pool) => {
            sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_session_share_graph_id($1)")
                .bind(id)
                .fetch_one(pool)
                .await
                .map_err(crate::store::StoreError::from)
        }
        None => store.session_share_graph_id(id).await,
    };
    let graph_id = match graph_lookup {
        Ok(Some(g)) => g,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "share not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "revoke_session_share lookup error");
            return internal_error();
        }
    };

    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "share not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "revoke_session_share reachability error");
            return internal_error();
        }
    }

    // The revoke itself, scoped to the share's OWNING account (module doc).
    if let Some(_pool) = tenant.pool.as_ref() {
        let owner = match owner_account(store.as_ref(), graph_id).await {
            Ok(Some(a)) => a,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "share not found or access denied"})),
                )
            }
            Err(()) => return internal_error(),
        };
        let mut tx = match tenant.begin_member_scoped(owner).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, "revoke_session_share: begin_account_scoped failed");
                return internal_error();
            }
        };
        if let Err(e) = store.revoke_session_share_scoped(&mut tx, id).await {
            tracing::error!(error = %e, "revoke_session_share update error");
            return internal_error();
        }
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "revoke_session_share: scoped commit failed");
            return internal_error();
        }
    } else if let Err(e) = store.revoke_session_share(id).await {
        tracing::error!(error = %e, "revoke_session_share update error");
        return internal_error();
    }
    (StatusCode::OK, Json(json!({"ok": true})))
}

// ── GET /v1/shared/sessions/{token} (PUBLIC — no auth) ────────────────────────

pub async fn public_session_share(
    State(_state): State<Arc<AppState>>,
    Path(_token): Path<String>,
) -> (StatusCode, Json<Value>) {
    // This PUBLIC route lives on the `open` router (no auth layer), so it
    // takes no `Tenant` extractor: that would trip its
    // mounted-outside-the-gate 500 tripwire once the runtime is enabled.
    return (StatusCode::CONFLICT,Json(json!({"error":"Whole-original links require selected evidence consent","code":"selected_evidence_consent_required"})));
}
