//! Revocable per-meeting share links (cs-uxp3b-library-search).
//!
//! - `POST /v1/meetings/shares` — mint a new share link for an INGESTED
//!   meeting (one with a stored transcript row). IDOR-guarded by graph
//!   reachability (ADR-0007 D9), same as `routes::meetings`.
//! - `GET  /v1/meetings/shares?graph_id=&source_id=` — list every share link
//!   (active + revoked) for one meeting, so the UI can show history and
//!   re-copy an active link.
//! - `POST /v1/meetings/shares/{id}/revoke` — permanently revoke a share.
//! - `GET  /v1/meetings/shares/view/{token}` — AUTHED viewer for both `link`
//!   and `team` shares.
//! - `GET  /v1/shared/{token}` — PUBLIC (no auth) viewer, `link` shares only.
//!   Mounted in the `open` router in `lib::app`, rate-limited per token.
//!
//! # Security model
//! * The bearer secret is the `token` itself: 32 bytes of CSPRNG randomness,
//!   base64url (no padding) encoded (43 chars) — see [`generate_share_token`].
//!   It is never derived from `id`/`graph_id`/`source_id` and is never logged.
//! * Revoked and unknown tokens are indistinguishable to every caller: both
//!   resolve to the identical generic 404 body. A `'team'` share resolved
//!   through the PUBLIC route also 404s (never leaks that a team-only share
//!   exists) — only the AUTHED viewer honors `'team'` visibility, and only for
//!   callers who can reach the ocean.
//! * The public route is rate-limited per token (not per caller — there is no
//!   caller identity) so a leaked/guessed token can't be hammered.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::store::{MeetingShare, Store};
use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver, AppState,
};

// Content-plane store, resolved by the `Tenant` extractor (org-tenancy PR
// 1.6; was ambient `state.store`) — `meeting_shares`/`meeting_transcripts`/
// `graphs` are tenant-tree tables. The PUBLIC route below passes
// `Tenant::legacy(&state)` (see its own doc comment) rather than the axum
// extractor, so this macro serves both.
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

// ── RLS scoping (org-tenancy B.3) ─────────────────────────────────────────────
//
// `meeting_shares` and `meeting_transcripts` both carry 0083 policies, so every
// read/write below runs inside ONE `begin_account_scoped` transaction per
// request. Two things make this route family unusual, and both are why
// migration `0086_webhook_share_bootstrap_functions` exists:
//
// 1. **The scoping account is the share OWNER's, never the caller's.** Shares
//    are reachability-gated (ADR-0007 Model B), so a legitimate viewer may sit
//    in a DIFFERENT account from the ocean's owner — scoping to
//    `user.account_id` would 404 exactly the cross-account readers the feature
//    exists for. The owner is resolved from the graph via
//    `Store::graph_account_id` (0084's `resolve_graph_account_id`), the same
//    choice `routes/graphs.rs::get_ocean_stats` made in B.1.
//
// 2. **The public/authed viewers authenticate by CAPABILITY TOKEN, not by
//    account** — they arrive holding nothing but an opaque token, so the owning
//    account has to be DISCOVERED before a scope can exist at all. That first
//    hop goes through `resolve_meeting_share_account_id` (0086), which returns
//    a bare `account_id` for ACTIVE shares only: a revoked or unknown token
//    resolves to NULL and the caller 404s, indistinguishable from before.
//    `revoke_share` does the same via `resolve_meeting_share_graph_id`, which
//    matches ANY share (active or revoked) so revoking twice keeps working.
//
// 3. **The payload BUILDER scopes per statement, not per request.** The two
//    handlers above open one transaction each because their statements are
//    contiguous; [`build_shared_payload`] cannot, because an R2 blob fetch +
//    AES decrypt sits between its `meeting_transcripts` read and its `atoms`
//    read. It therefore uses `services::transcripts::transcript_scope` — one
//    short scoped transaction per Store call — for the same reason PR #291's
//    `JobScope` doc gives. See that function's own doc comment.
//
// A pool-less `Tenant` keeps the plain Store path throughout, exactly as
// B.1's routes do.

/// The share owner's account for `graph_id`, or a ready-to-return error
/// response. `Ok(None)` means "no such graph" — callers map it to their own
/// generic 404 so a deleted ocean stays indistinguishable from an unreachable
/// one.
async fn owner_account(store: &dyn Store, graph_id: Uuid) -> Result<Option<Uuid>, ()> {
    match store.graph_account_id(graph_id).await {
        Ok(a) => Ok(a),
        Err(e) => {
            tracing::error!(error = %e, "shares: graph account lookup failed");
            Err(())
        }
    }
}

// ── Token generation ──────────────────────────────────────────────────────────

/// Build the frontend share URL for a token.
///
/// **Deliberately ALWAYS the PERSONAL web base — never the org host, even
/// for an org-owned share** (org-slack step 5 review). `public_share`
/// below resolves a token exclusively on the CONSUMER plane by design (see
/// its own doc comment): a share minted in an org's tenant DB simply does
/// not resolve there, on ANY host, today. Pointing an org share's link at
/// an org-branded host (org subdomains are retired; `org_web_base_url` now
/// returns the personal base anyway) would not make it openable — the lookup
/// still 404s — it would only add a misleading org-branded link that is
/// broken regardless of where it's served from. So this always calls
/// [`AppState::personal_web_base_url`] (never `web_base_url`/
/// `org_web_base_url`), a deliberate exception to the "derive from the
/// account's plane" rule the OAuth-install redirect follows
/// (`routes::oauth::callback`) — not an oversight.
fn build_share_url(state: &AppState, token: &str) -> String {
    build_share_url_from_base(&state.personal_web_base_url(), token)
}

/// Pure URL-shaping half of [`build_share_url`], split out so the path/query
/// construction can be unit-tested with an explicit base — no process-global
/// env mutation (which isn't safe under parallel test execution).
fn build_share_url_from_base(frontend_url: &str, token: &str) -> String {
    format!("{frontend_url}/shared?t={token}")
}

// ── POST /v1/meetings/shares ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateShareRequest {
    pub graph_id: Uuid,
    pub source_id: String,
    /// `"link"` | `"team"`. Defaults to `"link"` when omitted.
    #[serde(default)]
    pub visibility: Option<String>,
}

pub async fn create_share(
    State(_state): State<Arc<AppState>>,
    _tenant: Tenant,
    Extension(_user): Extension<UserContext>,
    Json(_body): Json<CreateShareRequest>,
) -> (StatusCode, Json<Value>) {
    return (StatusCode::CONFLICT,Json(json!({"error":"Whole-original links require selected evidence consent","code":"selected_evidence_consent_required"})));
}

// ── GET /v1/meetings/shares ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ListSharesQuery {
    pub graph_id: Uuid,
    pub source_id: String,
}

pub async fn list_shares(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<ListSharesQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        q.graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "meeting not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_shares reachability error");
            return internal_error();
        }
    }

    // One scoped read on the ocean OWNER's account (see this module's RLS note).
    let shares = {
        let owner = match owner_account(store.as_ref(), q.graph_id).await {
            Ok(Some(a)) => a,
            // Reachability passed above, so a vanished ocean is a delete race —
            // same generic 404 as unreachable.
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "meeting not found or access denied"})),
                )
            }
            Err(()) => return internal_error(),
        };
        let mut tx = match tenant.begin_member_scoped(owner).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, "list_shares: begin_account_scoped failed");
                return internal_error();
            }
        };
        let shares = match store
            .list_meeting_shares_scoped(&mut tx, q.graph_id, &q.source_id)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "list_shares load error");
                return internal_error();
            }
        };
        tx.rollback().await.ok();
        shares
    };

    let shares_json: Vec<Value> = shares
        .iter()
        .map(|s: &MeetingShare| {
            json!({
                "id":         s.id,
                "visibility": s.visibility,
                "share_url":  build_share_url(&state, &s.token),
                "created_at": s.created_at,
                "revoked_at": s.revoked_at,
                "active":     s.revoked_at.is_none(),
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({ "shares": shares_json })))
}

// ── POST /v1/meetings/shares/{id}/revoke ───────────────────────────────────────

pub async fn revoke_share(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // RLS bootstrap: this handler receives a bare share id and its IDOR guard
    // is graph REACHABILITY, so the scoping account can only come from the
    // share's OWNING graph — which we cannot read under a scope we don't have
    // yet. `resolve_meeting_share_graph_id` (0086) breaks that cycle: bare
    // `graph_id` only, ANY share active or revoked (so a double-revoke behaves
    // exactly as it did unscoped).
    let graph_lookup = match tenant.pool.as_ref() {
        // A scalar function call always returns exactly one row, with NULL for
        // "no such share" — hence `fetch_one` into `Option<Uuid>`, not
        // `fetch_optional`.
        Some(pool) => sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT resolve_meeting_share_graph_id($1)",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .map_err(crate::store::StoreError::from),
        None => store.meeting_share_graph_id(id).await,
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
            tracing::error!(error = %e, "revoke_share lookup error");
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
            tracing::error!(error = %e, "revoke_share reachability error");
            return internal_error();
        }
    }

    {
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
                tracing::error!(error = %e, "revoke_share: begin_account_scoped failed");
                return internal_error();
            }
        };
        if let Err(e) = store.revoke_meeting_share_scoped(&mut tx, id).await {
            tracing::error!(error = %e, "revoke_share update error");
            return internal_error();
        }
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "revoke_share: scoped commit failed");
            return internal_error();
        }
        (StatusCode::OK, Json(json!({"ok": true})))
    }
}

// ── GET /v1/meetings/shares/view/{token} (AUTHED) ──────────────────────────────

pub async fn view_share(
    State(_state): State<Arc<AppState>>,
    _tenant: Tenant,
    Extension(_user): Extension<UserContext>,
    Path(_token): Path<String>,
) -> (StatusCode, Json<Value>) {
    return (StatusCode::CONFLICT,Json(json!({"error":"Whole-original links require selected evidence consent","code":"selected_evidence_consent_required"})));
}

// ── GET /v1/shared/{token} (PUBLIC — no auth) ──────────────────────────────────

pub async fn public_share(
    State(_state): State<Arc<AppState>>,
    Path(_token): Path<String>,
) -> (StatusCode, Json<Value>) {
    // This PUBLIC route lives on the `open` router (no auth layer), so it
    // takes no `Tenant` extractor: that would trip its
    // mounted-outside-the-gate 500 tripwire once the runtime is enabled.
    return (StatusCode::CONFLICT,Json(json!({"error":"Whole-original links require selected evidence consent","code":"selected_evidence_consent_required"})));
}

/// Test-only access to the actual scoped payload builder. This mounts no route
/// and grants no share capability; Postgres HTTP guards remain unconditional.
#[cfg(feature = "test-support")]
pub async fn test_scoped_shared_payload(
    state: &Arc<AppState>,
    tenant: &Tenant,
    graph_id: Uuid,
    source_id: &str,
    visibility: &str,
) -> Value {
    let store = tenant.store.as_ref().expect("scoped payload test store");
    build_shared_payload(state, tenant, store.as_ref(), graph_id, source_id, visibility).await
}

// ── Shared payload builder ──────────────────────────────────────────────────────

/// Build the payload returned by both the public and authed-view routes:
/// `{ title, occurred_at, duration_ms, summary, transcript, visibility }`.
/// Every field except `visibility` is best-effort/nullable — a resolution
/// failure on any one of them never fails the whole payload (the same
/// degrade-gracefully contract `session_shares::build_shared_session_payload`
/// keeps, and what the `/shared` web view renders against).
///
/// # RLS scoping (org-tenancy OPS-4 — the F3 fix)
///
/// Both reads below hit tables carrying `0083_consumer_rls` policies
/// (`meeting_transcripts`, and `atoms` behind `meeting_occurred_at`), and both
/// used to run AMBIENT. Post-OPS-4 that meant every share link — public and
/// authed alike — returned `HTTP 200` with an all-`null` body: three unscoped
/// reads each hard-erroring on the strict `current_setting('app.account_id')`
/// predicate, each swallowed into its own `warn!` arm. A loud DB failure
/// rendered as a quiet empty page.
///
/// Scoping is PER STATEMENT via [`crate::services::transcripts::transcript_scope`],
/// the dispatch this module's sibling already owns, NOT one transaction across
/// the whole builder: `resolve_raw_text` sits between the two reads and does an
/// R2 blob fetch + AES decrypt, and pinning a pooled connection across that
/// network round trip is exactly what PR #291's `JobScope` rationale rules out.
/// Neither read depends on the other's uncommitted state.
///
/// The scoping account is the OCEAN OWNER's, never a caller's — the public
/// route is ANONYMOUS (there is no caller account at all) and the authed route
/// is reachability-gated, so a legitimate viewer may sit in a different
/// account. `transcript_scope` resolves it from `graph_id` through migration
/// 0084's audited `resolve_graph_account_id` bootstrap, and returns `None` only
/// for a pool-less `Tenant`, where the plain unscoped Store
/// methods are still correct.
///
/// The ORG plane is scoped too, and by the acting member. This route family
/// reaches it: `view_share` is the AUTHED viewer, extracts a real `Tenant`, and
/// therefore carries an `Org` decision on an org host. (`public_share` is the
/// anonymous one; it builds `Tenant::legacy`, whose decision is always `None`,
/// so it never reaches the org plane at all.) Before this was scoped, an
/// authed org viewer reading a member-private or team meeting got an all-null
/// body: `migrations_tenant/0009_tenant_rls`'s `member_ocean_isolation` hides
/// those oceans from a transaction that set no `app.user_id`, and a hidden row
/// arrives as `Ok(None)` — the same shape as a meeting nobody ingested.
///
/// ## The all-null body that remains, and who still gets it
///
/// Scoping the org plane narrowed that to a deliberate residue. `0009`'s
/// policy has no share-capability arm — it knows ownership and team
/// membership, not that a token was minted — and `view_share` runs NO
/// reachability check for a `'link'` share. So two viewers still read an
/// all-null body here, and they are two, not one:
///
/// * a `'link'` share of a MEMBER-PRIVATE ocean, viewed by an authed org
///   member who does not own it;
/// * a `'link'` share of a TEAM ocean, viewed by an authed org member who is
///   not on that team.
///
/// Both are reachable — `create_share` mints a `'link'` share for any ocean
/// its minter can reach — and both degrade identically. See
/// `services::transcripts::begin_transcript_scoped` for why that is the
/// answer and not a gap, and `transcript_scope_org_rls_pg.rs`'s
/// `the_org_dispatch_is_what_makes_the_read_scoped`, which asserts both.
#[cfg(feature = "test-support")]
async fn build_shared_payload(
    state: &Arc<AppState>,
    tenant: &Tenant,
    store: &dyn Store,
    graph_id: Uuid,
    source_id: &str,
    visibility: &str,
) -> Value {
    // A scope this path required and could not open is a refusal, never an
    // unscoped read. `transcript_scope` has already logged the reason; the
    // payload degrades to the same all-null body every other failure here
    // produces, so a viewer learns nothing new from it.
    let scope = match crate::services::transcripts::transcript_scope(
        store,
        tenant.pool.as_ref(),
        tenant.decision.as_ref(),
        graph_id,
    )
    .await
    {
        Ok(s) => s,
        Err(_) => {
            tracing::error!(%graph_id, "build_shared_payload: no RLS scope; returning nulls");
            return json!({
                "title":       Value::Null,
                "occurred_at": Value::Null,
                "duration_ms": Value::Null,
                "summary":     Value::Null,
                "transcript":  Value::Null,
                "visibility":  visibility,
            });
        }
    };

    // `summary` now rides along on the transcript row rather than being
    // re-read by its own runtime-typed query: `MeetingTranscript` has carried
    // `summary`/`summary_status` since migration 0071 landed (which is also
    // what retires the old "columns may not exist yet" probe and its
    // `42703`/`42P01` classifier — a classifier that, post-OPS-4, could never
    // have matched a scope failure's `42704`/`22P02` anyway). One fewer
    // round trip, and one fewer place for a scope failure to hide.
    let (title, duration_ms, transcript, summary) =
        match crate::services::transcripts::scoped_get_meeting_transcript(
            store, scope, graph_id, source_id,
        )
        .await
        {
            Ok(Some(t)) => {
                let duration_ms = t.duration_ms;
                // Only surface a summary once generation is done AND non-null:
                // gating on `summary_status = 'done'` avoids leaking a
                // half-written/failed summary (verbatim the gate the removed
                // query carried in its WHERE clause).
                let summary = if t.summary_status == "done" {
                    t.summary.clone()
                } else {
                    None
                };
                let raw_text = crate::services::transcripts::resolve_raw_text(
                    state, tenant, store, graph_id, &t,
                )
                .await;
                let title = raw_text
                    .as_deref()
                    .and_then(crate::routes::meetings::meeting_title_from_raw);
                (title, duration_ms, raw_text, summary)
            }
            // The read SUCCEEDED and returned nothing, and this arm used to
            // say nothing about it. Every sibling failure arm here logs; this
            // one did not, so "a viewer got an empty share body because the
            // member-ocean policy hid the meeting" was operationally silent
            // and indistinguishable from a genuinely empty meeting. That is
            // the exact failure mode this series removes.
            //
            // `warn!`, not `error!`: nothing failed. When the cause is the
            // policy (see the limit on this function's doc), the policy did
            // its job, and paging on a correct refusal is how an alert gets
            // ignored. `warn!`, not `info!`/`debug!`: silence at production
            // log levels IS the hazard, and a share token was minted for this
            // meeting — somebody asserted it exists — so an empty body is
            // anomalous even when it is correct.
            //
            // Ids and kinds only: the ocean id, the scope kind and the share
            // kind. Nothing that identifies the viewer, and no content.
            Ok(None) => {
                tracing::warn!(
                    %graph_id,
                    scope = crate::services::transcripts::scope_kind(scope),
                    visibility,
                    "build_shared_payload: the scoped read returned no transcript row; \
                     returning nulls. Under scope=org-member this is EITHER a meeting nobody \
                     ingested OR one the member-ocean policy hid from this viewer"
                );
                (None, None, None, None)
            }
            // `error!`, not `warn!`: with the read scoped there is no longer a
            // benign cause for this arm, and the whole point of F3 is that a
            // real DB/scope failure must not be indistinguishable from an
            // un-ingested meeting in the logs.
            Err(e) => {
                tracing::error!(error = %e, %graph_id, "build_shared_payload transcript load failed; returning nulls");
                (None, None, None, None)
            }
        };

    let occurred_at = scoped_meeting_occurred_at(store, scope, graph_id, source_id)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, %graph_id, "build_shared_payload occurred_at load failed; returning null");
            None
        });

    json!({
        "title":       title,
        "occurred_at": occurred_at,
        "duration_ms": duration_ms,
        "summary":     summary,
        "transcript":  transcript,
        "visibility":  visibility,
    })
}

/// [`Store::meeting_occurred_at`] under `scope` — it reads `atoms` (an
/// RLS-covered, graph-mediated table), so it needs the same treatment as the
/// transcript read above. Shaped exactly like
/// `services::transcripts::scoped_get_meeting_transcript`; it lives here rather
/// than there because it is the only `atoms`-backed read on the share path.
#[cfg(feature = "test-support")]
async fn scoped_meeting_occurred_at(
    store: &dyn Store,
    scope: crate::services::transcripts::TranscriptScope<'_>,
    graph_id: Uuid,
    source_id: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, crate::store::StoreError> {
    match scope.target() {
        Some(target) => {
            let mut tx = crate::services::transcripts::begin_transcript_scoped(target).await?;
            let occurred_at = store
                .meeting_occurred_at_scoped(&mut tx, graph_id, source_id)
                .await;
            // Read-only: rollback is as durable as commit and returns the
            // connection to the pool a statement sooner.
            tx.rollback().await.ok();
            occurred_at
        }
        None => store.meeting_occurred_at(graph_id, source_id).await,
    }
}

// The old `best_effort_summary` + `is_undefined_column_or_table` pair lived
// here. Both are gone (org-tenancy OPS-4 — the F3 fix):
//
//   * The query was a runtime-typed re-read of `meeting_transcripts.summary`
//     that `build_shared_payload` had ALREADY loaded — `MeetingTranscript`
//     has carried `summary`/`summary_status` since migration 0071 — so the
//     summary is now read off that (already scoped) row instead. That deletes
//     a whole unscoped statement rather than converting it.
//   * Its error classifier only ever recognised `42703`/`42P01` (undefined
//     column/table, the pre-0071 race it was written for). An RLS scope
//     failure raises `42704` (`unrecognized configuration parameter`) or
//     `22P02` (`invalid input syntax for type uuid: ""`), so it correctly
//     took the loud `warn!` branch — which is the only reason this bug was
//     ever visible. Nothing here reclassifies a scope failure as benign now
//     either: the single remaining transcript read logs `error!`.

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn share_url_uses_frontend_url_env_or_localhost_default() {
        // Exercise the pure URL-shaping helper with explicit bases instead of
        // mutating the process-global FRONTEND_URL env var — the latter isn't
        // safe under `cargo test`'s parallel, same-process execution.
        assert_eq!(
            build_share_url_from_base("http://localhost:3000", "abc123"),
            "http://localhost:3000/shared?t=abc123",
            "localhost default base",
        );
        assert_eq!(
            build_share_url_from_base("https://app.example.com", "xyz789"),
            "https://app.example.com/shared?t=xyz789",
            "configured FRONTEND_URL base",
        );
    }
}
