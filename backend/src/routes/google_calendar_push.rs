//! Google Calendar `events.watch` push webhook (gsuite Train G3 PR 1,
//! `.scratch/gsuite-enterprise/plan.md`).
//!
//! **Separate route from `routes::google_push`.** That route backs Gmail
//! `users.watch` and Chat's Workspace Events subscriptions, both delivered
//! via Cloud Pub/Sub with a Google-signed OIDC bearer token. Calendar's
//! `events.watch` channels are a completely different delivery mechanism —
//! Google POSTs directly to the channel's registered `address` (this route)
//! with `X-Goog-Channel-*`/`X-Goog-Resource-*` headers and NO OIDC bearer at
//! all — so this route has its own, independent trust boundary: the
//! `X-Goog-Channel-Token` header, constant-time-compared against the secret
//! `connectors::google_calendar::ensure_watch` minted and stored at channel-
//! creation time. This route never touches, weakens, or shares any state
//! with `google_push`'s OIDC verification.
//!
//! # Authentication
//! 1. `X-Goog-Channel-ID` names the channel. Since the notification carries
//!    no `account_id`, [`connectors::google_calendar::resolve_channel_for_push`]
//!    discovers it via a narrow `SECURITY DEFINER` bootstrap function
//!    (migration `0183`) — same bootstrap-then-scope shape
//!    `routes::graph_webhook` uses for Graph's `clientState`.
//! 2. `X-Goog-Channel-Token` is compared, constant-time, against the stored
//!    secret for that channel (`subtle::ConstantTimeEq`, same primitive
//!    `routes::graph_webhook`/`routes::clerk_webhook`/`routes::slack_events`
//!    already use for their own shared-secret checks).
//!
//! An unknown channel id is `404`; a channel-token mismatch is `403`
//! (identified as *some* channel, just not authorized) — the same
//! distinction `routes::google_push::PushAuthError::IdentityMismatch`
//! draws for the Pub/Sub path.
//!
//! # No payload — a trigger, not data
//! Calendar push notifications carry NO event content, only headers. A
//! verified notification with `X-Goog-Resource-State: sync` is Google's
//! one-time bootstrap confirmation sent right after channel creation — ACK
//! and do nothing. Any other verified state (`exists`, or an unrecognized
//! value — treated the same, since re-running an idempotent sync on an
//! unexpected state is harmless) runs an incremental
//! `connectors::google_calendar::sync` pass for that channel's account.
//!
//! # Org members (Google Calendar org lift)
//! A channel row carrying an `owner_user_id` is ONE org member's own channel
//! (`connectors::google_calendar::CalendarTarget`): the push syncs THAT
//! member's calendar into THEIR member-private ocean on the org's tenant pool
//! via `workers::google_calendar_sync::sync_org_member`, gated on the org's
//! `google_calendar` flag. Channel routing is unchanged — the channel id
//! resolves through the control-only `SECURITY DEFINER` bootstrap (migration
//! `0183`) because org-member watch rows live on the CONTROL plane (see
//! `connectors::google_calendar`'s module doc).
//!
//! # Personal-plane synchronous convention
//! The personal (account-wide) channel has no org branch to route to a
//! durable job for. Its pass is `workers::google_calendar_sync::
//! sync_personal_account` — the same one the 30-minute backstop sweep runs
//! for every personal connection — which mirrors `routes::graph_webhook::
//! ingest_message_with`'s `GraphIngestTarget::Sync` arm exactly:
//! `Tenant::legacy(state)` + a synthetic `UserContext` built from the
//! account's `owner_user_id`, then straight through
//! `routes::ingest::ingest_atom`. The actual sync/ingest work is handed to a
//! spawned task so the webhook response returns immediately (same reasoning
//! `routes::google_push::google_push`'s `dispatch_stub` spawn already
//! documents) — Google does not guarantee aggressive redelivery the way
//! Pub/Sub does, but there is no reason to hold the HTTP response open for a
//! Calendar API round trip either.
//!
//! # Bounded, not one task per notification
//! The spawned pass runs under `crate::calendar_pass_gate` ([`dispatch_push_pass`]):
//! a pass pins a pooled connection on its lock transaction for its whole
//! duration, so one unbounded task per push let a burst across many
//! channels pin the shared pool and time out every pass and every request
//! behind them. A pass that cannot get a permit within
//! `calendar_pass_gate::PUSH_WAIT` is skipped: the push carried no data,
//! the cursor is untouched, and the next push for the channel or the
//! 30-minute sweep runs the same pass. An org member's channel resolves its
//! org plane first and also takes a slot of that org's own bound (its pass
//! ingests through the org's small tenant request pool).

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::{
    calendar_pass_gate::{CalendarPassGate, PUSH_WAIT},
    connectors::{google_calendar, ConnectorError},
    middleware::auth::UserContext,
    ingest::atom::IngestAtomRequest,
    tenancy::extractor::Tenant,
    workers::google_calendar_sync::PassOutcome,
    AppState,
};

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// Constant-time comparison of the notification's `X-Goog-Channel-Token`
/// against the secret stored at channel-creation time. Mirrors
/// `routes::graph_webhook::client_state_matches` exactly.
fn channel_token_matches(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1
}

/// `POST /v1/webhooks/google/calendar` — Calendar `events.watch` channel
/// notifications. See module doc for the full trust/dispatch story.
pub async fn google_calendar_push(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    // CodeRabbit review finding on PR #677 (Major, security): a fresh
    // `google_watch_channels` row created by a sync-only write (before
    // `ensure_watch` has ever succeeded — see that function's own doc)
    // defaults `channel_id`/`channel_token` to `''` (the column defaults,
    // `migrations_control/0180_google_watch_channels.sql`). An inbound
    // request carrying an EMPTY `X-Goog-Channel-ID`/`X-Goog-Channel-Token`
    // header must never be allowed to match such a placeholder row — that
    // would let anyone with no knowledge of any real secret trigger a sync
    // for whichever account happens to have a placeholder row. Reject empty
    // (or missing) headers outright, before ever touching the DB — belt,
    // not just suspenders: `resolve_channel_for_push`'s own queries also
    // exclude empty rows (defense in depth, see that function's doc).
    let Some(channel_id) = header_str(&headers, "x-goog-channel-id").filter(|v| !v.trim().is_empty())
    else {
        tracing::warn!("google calendar push missing or empty X-Goog-Channel-ID");
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(provided_token) =
        header_str(&headers, "x-goog-channel-token").filter(|v| !v.trim().is_empty())
    else {
        tracing::warn!(%channel_id, "google calendar push missing or empty X-Goog-Channel-Token");
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let resource_state = header_str(&headers, "x-goog-resource-state").unwrap_or("");

    let Some(pool) = state.db.clone() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let (account_id, channel_owner, expected_token) =
        match google_calendar::resolve_channel_for_push(&pool, channel_id).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                tracing::warn!(%channel_id, "google calendar push: unknown channel");
                return StatusCode::NOT_FOUND.into_response();
            }
            Err(e) => {
                tracing::warn!(error = %e, %channel_id, "google calendar push: channel resolution failed");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };

    if !channel_token_matches(provided_token, &expected_token) {
        tracing::warn!(%channel_id, %account_id, "google calendar push: channel token mismatch — rejecting");
        return StatusCode::FORBIDDEN.into_response();
    }

    // Bootstrap confirmation sent once, right after channel creation — no
    // change to react to.
    if resource_state == "sync" {
        return StatusCode::OK.into_response();
    }

    // Verified. Run the sync pass off the request path — see module doc.
    //
    // Bare `tokio::spawn`, deliberately not `state.task_tracker.spawn(..)`
    // (CodeRabbit review finding on PR #677, Major, verified and NOT
    // applied — see `AppState::task_tracker`'s own doc comment): that
    // tracker exists for a specific, named set of post-response flows
    // (Slack webhook message ingest, member-joined prompts, the huddle-probe
    // persist), not a blanket rule for every spawned webhook task. This
    // spawn matches the identical, already-reviewed convention BOTH of this
    // route's own precedents already use — `routes::google_push::
    // google_push`'s `dispatch_stub` spawn (the Pub/Sub sibling this route
    // was explicitly modeled on) and `routes::graph_webhook`'s per-
    // notification spawn — neither tracked, neither flagged in their own
    // reviews. Diverging to `task_tracker` here alone, while its closest
    // siblings stay untracked, would be inconsistent rather than safer; if
    // this class of task is ever brought under the tracker it should happen
    // for all three together, not piecemeal on this PR.
    let bg_state = state.clone();
    let pool_for_bg = pool.clone();
    tokio::spawn(async move {
        dispatch_push_pass(CalendarPassGate::shared(), PUSH_WAIT, &bg_state, &pool_for_bg, account_id, channel_owner).await;
    });

    StatusCode::OK.into_response()
}

/// The spawned half of [`google_calendar_push`]: one gated pass for the
/// channel's connection (module doc, "Bounded"). `gate` and `wait` are the
/// process-wide `CalendarPassGate::shared()` and `PUSH_WAIT` in production;
/// a test hands in a gate of a known size and a short wait.
///
/// Google Calendar org lift: a channel with an `owner_user_id` is ONE org
/// member's own channel — sync THAT member's calendar into THEIR
/// member-private ocean on the org plane. No owner = the personal
/// account-wide channel. Both arms are the SAME per-connection pass the
/// 30-minute `workers::google_calendar_sync` backstop runs — a push only
/// decides WHEN it happens.
pub(crate) async fn dispatch_push_pass(
    gate: &CalendarPassGate,
    wait: std::time::Duration,
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    channel_owner: Option<Uuid>,
) -> Option<Result<PassOutcome, ConnectorError>> {
    let owner = channel_owner.map(crate::connectors::TokenOwner::Member)
        .unwrap_or(crate::connectors::TokenOwner::AccountWide);
    let credential_live = || async {
        crate::connectors::credential_health::is_credential_live(pool, account_id, owner, "google").await
            .map_err(|error| ConnectorError::Internal(format!("push credential health: {error}")))
    };
    match credential_live().await {
        Ok(Some(true)) => {}
        Ok(_) => return Some(Ok(PassOutcome::Skipped("credential unavailable or revoked"))),
        Err(error) => return Some(Err(error)),
    }
    // An org member's channel resolves its plane BEFORE taking a permit (a
    // few control-plane reads, no lock): the permit then carries a slot of
    // that org's own bound too — its pass ingests through the org's small
    // tenant request pool (`calendar_pass_gate`, "Two bounds").
    let org_target = match channel_owner {
        Some(owner_user_id) => match resolve_org_member_channel(state, pool, account_id, owner_user_id).await {
            Ok(Ok(target)) => Some((owner_user_id, target)),
            Ok(Err(skipped)) => {
                tracing::info!(%account_id, reason = ?skipped, "google calendar push: pass skipped");
                return Some(Ok(skipped));
            }
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "google calendar push: sync failed");
                return Some(Err(e));
            }
        },
        None => None,
    };
    let org_plane = org_target
        .as_ref()
        .map(|(_, (org_id, content_pool))| crate::calendar_pass_gate::OrgSlot::google(org_id.as_str(), content_pool));
    let Some(_permit) = gate.acquire_scoped(org_plane, wait).await else {
        tracing::info!(
            %account_id, in_flight = gate.in_flight(), bound = gate.size(),
            "google calendar push: pass gate full — skipping; the next push or the sweep runs this pass"
        );
        return None;
    };
    match credential_live().await {
        Ok(Some(true)) => {}
        Ok(_) => return Some(Ok(PassOutcome::Skipped("credential unavailable or revoked"))),
        Err(error) => return Some(Err(error)),
    }
    let result = match &org_target {
        Some((owner_user_id, (org_id, content_pool))) => {
            crate::workers::google_calendar_sync::sync_org_member(
                state,
                pool,
                content_pool,
                account_id,
                org_id,
                *owner_user_id,
            )
            .await
        }
        None => crate::workers::google_calendar_sync::sync_personal_account(state, pool, account_id).await,
    };
    match &result {
        Ok(PassOutcome::Synced { ingested }) => {
            tracing::info!(%account_id, ingested, "google calendar push: sync complete")
        }
        Ok(PassOutcome::Busy) => {
            tracing::info!(%account_id, "google calendar push: a pass is already in flight for this connection — skipping")
        }
        Ok(PassOutcome::Skipped(reason)) => tracing::info!(%account_id, reason, "google calendar push: pass skipped"),
        Err(e) => tracing::warn!(error = %e, %account_id, "google calendar push: sync failed"),
    }
    Some(result)
}

/// Where a push for ONE org member's channel syncs to (Google Calendar org
/// lift): `(clerk_org_id, the org's content pool)`. Resolves the org plane
/// exactly like `routes::oauth::callback`'s own post-connect work does —
/// `clerk_org_id` off the account, then the org's content pool through THE
/// decision table (`routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest`)
/// — for `workers::google_calendar_sync::sync_org_member`, the same
/// per-member pass the 30-minute tenant-loop backstop runs.
///
/// An unresolvable plane (revoked/mid-migration org) is a logged
/// `Ok(Err(Skipped))`, never a guess: the next push or backstop tick
/// retries, and the cursor has not moved.
async fn resolve_org_member_channel(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Result<(String, sqlx::PgPool), PassOutcome>, ConnectorError> {
    let org_id: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("push org lookup: {e}")))?
        .flatten();
    let Some(org_id) = org_id else {
        // A member-owned channel on a non-org account is not a shape any
        // writer produces; refuse rather than sync into a guessed ocean.
        tracing::warn!(%account_id, %owner_user_id, "google calendar push: member channel on a non-org account — ignoring");
        return Ok(Err(PassOutcome::Skipped("member channel on a non-org account")));
    };
    // Flag-off orgs stop being synced even if a channel from before the flag
    // was turned off is still live.
    if !crate::routes::oauth::org_google_calendar_enabled_for(state, &org_id).await {
        tracing::info!(%account_id, %org_id, "google calendar push: org flag is off — not syncing");
        return Ok(Err(PassOutcome::Skipped("org calendar flag is off")));
    }
    let Some(content_pool) =
        crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(state, control_pool, &org_id)
            .await
    else {
        tracing::warn!(%account_id, %org_id, "google calendar push: org content pool unresolved — skipping");
        return Ok(Err(PassOutcome::Skipped("org content pool unresolved")));
    };
    Ok(Ok((org_id, content_pool)))
}

/// Ingest every item in `pending.items`, then commit `pending.commit`
/// (advance the sync cursor) ONLY IF every one of them succeeded — see
/// `connectors::google_calendar::fetch_pending`'s doc, "commit-after-ingest"
/// (CodeRabbit review finding on PR #677, Major). Split out from the
/// per-connection passes so a test can drive it directly against a
/// hand-built `PendingSync`, without needing a live Google API round trip
/// (there is no mock-HTTP infrastructure in this crate to fake
/// `fetch_pending`'s own network calls — see `backend/tests/fixtures/
/// google_calendar/README.md`). Returns how many items were ingested.
pub(crate) async fn ingest_and_commit(
    state: &Arc<AppState>,
    target: &google_calendar::CalendarTarget<'_>,
    graph_id: Uuid,
    owner_user_id: Uuid,
    tenant: Tenant,
    pending: google_calendar::PendingSync,
) -> Result<usize, ConnectorError> {
    let account_id = target.account_id;
    let blobs = state.r2.as_deref().map(|r| r as &dyn crate::services::blob_store::PromotionBlobStore);
    let mut ingested = 0usize;
    let mut all_succeeded = true;
    for item in pending.items {
        let user_ctx = UserContext {
            user_id: owner_user_id,
            account_id,
            role: "member".into(),
            device_id: String::new(),
        };
        // Captured before `item.source_id` moves into `req`, purely for the
        // warn! below.
        let source_id_for_log = item.source_id.clone();
        // Event EDITS (verified in prod 2026-10-06: a renamed event came
        // back as `skipped:1`, coral unchanged): `ingest_atom`'s insert is
        // `ON CONFLICT ... DO NOTHING` on the event's source id, so an
        // already-ingested event that Google reports CHANGED must have its
        // stale coral retracted first — confined to THIS graph (one member's
        // own ocean under org; never another member's copy of the same
        // event id) — so the ingest below lands the new title/time/
        // attendees as a fresh coral. An unchanged re-fetch (a retried page)
        // is left alone and the insert stays the no-op it always was. See
        // `services::source_reingest`'s module doc. A failure here is
        // treated exactly like a failed ingest: this item is not ingested
        // and the cursor does not advance, so the next pass retries it.
        let incoming = crate::services::source_reingest::Incoming {
            content: &item.content,
            timestamp: item.timestamp,
            participants: &item.participants,
        };
        match crate::services::source_reingest::retract_if_changed(
            target.content,
            blobs,
            account_id,
            graph_id,
            &item.source_type,
            &item.source_id,
            &incoming,
        )
        .await
        {
            Ok(crate::services::source_reingest::ReingestOutcome::Replaced { atoms_deleted, reefs_deleted }) => {
                tracing::info!(
                    %account_id, source_id = %source_id_for_log, atoms_deleted, reefs_deleted,
                    "google calendar push: event changed upstream — stale coral retracted, re-ingesting"
                );
            }
            Ok(_) => {}
            Err(e) => {
                all_succeeded = false;
                tracing::warn!(
                    error = %e, %account_id, source_id = %source_id_for_log,
                    "google calendar push: could not check/retract the stored coral for a changed event — \
                     skipping it this pass, cursor will NOT advance"
                );
                continue;
            }
        }
        let req = IngestAtomRequest {
            graph_id,
            source_type: item.source_type,
            source_id: item.source_id,
            content: item.content,
            participants: item.participants,
            timestamp: item.timestamp,
            scope: item.scope,
            fts_language: None,
            channel_id: item.channel_id,
            channel_name: None,
            author_id: item.author_id,
            author_name: item.author_name,
            project: None,
            mcp_client_name: None,
            raw_content: None,
            duration_ms: None,
            segments: None,
            session_id: None,
            session_kind: None,
            session_title: None,
            repo_path: None,
            remote: None,
            hostname: None,
            harness_version: None,
            model: None,
            branch: None,
            agent_name: None,
            platform: None,
            task: None,
            parent_session_id: None,
            trusted_user_agent: None,
            client_info: None,
            code_path: None,
        };
        // `tenant` is `Tenant::legacy` on the personal plane — see module
        // doc's "Personal-plane synchronous convention" (mirrors
        // `routes::graph_webhook::ingest_message_with`'s `Sync` arm) — and a
        // real `TenancyDecision::Org` for an org member's pass (Google
        // Calendar org lift, built by `workers::google_calendar_sync`).
        let source_namespace = format!("google_calendar:connection:{account_id}:{owner_user_id}");
        let refused = crate::ingest::submit(crate::ingest::IngestRequest::private(crate::ingest::Member {
            provider_namespace: Some(&source_namespace),
            ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
        })).await.err();
        match refused {
            None => {
                ingested += 1;
            }
            Some(refused) if refused.is_invalid() => {
                // Deterministic validation rejection (CodeRabbit review finding
                // on PR #677, round 2 — the poison-pill case): `ingest_atom`
                // refused this SPECIFIC event's content (e.g. an upstream
                // event `shape_calendar_item` couldn't fully validate). Retrying
                // the identical item will never turn a 422 into success, so —
                // unlike a transient failure — this must NOT block the cursor:
                // treat it as "consumed" (permanently skipped, logged) rather
                // than as the kind of failure `all_succeeded` exists to guard
                // against. `shape_calendar_item`'s own empty-id guard already
                // closes the specific case CodeRabbit found; this is the
                // second, independent layer — any OTHER 422 `ingest_atom` might
                // ever raise for a calendar event gets the same treatment.
                tracing::warn!(
                    %account_id, source_id = %source_id_for_log, error = %refused,
                    "google calendar push: the ingest door rejected this event as invalid — \
                     skipping permanently, not blocking the sync cursor"
                );
            }
            Some(refused) => {
                all_succeeded = false;
                tracing::warn!(
                    error = %refused, %account_id, source_id = %source_id_for_log,
                    "google calendar push: ingest_atom failed for one event — cursor will NOT advance this pass"
                );
            }
        }
    }

    if all_succeeded {
        google_calendar::commit_pending(target, pending.commit).await?;
    } else {
        tracing::warn!(
            %account_id, ingested, skipped_committing = true,
            "google calendar push: one or more ingests failed — sync cursor left unchanged, \
             next pass re-fetches this page"
        );
    }
    Ok(ingested)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_token_matches_exact_only() {
        assert!(channel_token_matches("secret-abc", "secret-abc"));
        assert!(!channel_token_matches("secret-abc", "secret-abd"));
        assert!(!channel_token_matches("secret-ab", "secret-abc"), "length mismatch must fail closed");
        assert!(!channel_token_matches("", "secret-abc"));
    }

    #[test]
    fn header_str_reads_and_rejects_missing() {
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-channel-id", "chan-1".parse().unwrap());
        assert_eq!(header_str(&headers, "x-goog-channel-id"), Some("chan-1"));
        assert_eq!(header_str(&headers, "x-goog-channel-token"), None);
    }

    #[tokio::test]
    async fn unknown_channel_is_404() {
        let state = crate::test_app_state();
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-channel-id", "does-not-exist".parse().unwrap());
        headers.insert("x-goog-channel-token", "whatever".parse().unwrap());
        let resp = google_calendar_push(State(state), headers).await;
        // No consumer pool configured in the test AppState → 503 before the
        // lookup even runs; either way this must never be a 2xx.
        assert!(!resp.status().is_success());
    }

    // ── Fixture-driven (backend/tests/fixtures/google_calendar/
    // watch_notification_headers.json — see that directory's README for
    // provenance) ───────────────────────────────────────────────────────

    const NOTIFICATION_HEADERS: &str =
        include_str!("../../tests/fixtures/google_calendar/watch_notification_headers.json");

    fn fixture_header(set: &str, name: &str) -> String {
        let v: serde_json::Value = serde_json::from_str(NOTIFICATION_HEADERS).unwrap();
        v[set]["headers"][name].as_str().unwrap().to_string()
    }

    #[test]
    fn fixture_sync_bootstrap_state_is_recognized() {
        assert_eq!(fixture_header("sync_bootstrap", "X-Goog-Resource-State"), "sync");
    }

    #[test]
    fn fixture_change_notification_carries_the_matching_channel_token() {
        let expected = fixture_header("sync_bootstrap", "X-Goog-Channel-Token");
        let provided = fixture_header("change_notification", "X-Goog-Channel-Token");
        assert_eq!(
            fixture_header("sync_bootstrap", "X-Goog-Channel-ID"),
            fixture_header("change_notification", "X-Goog-Channel-ID"),
            "both notifications target the same channel"
        );
        assert!(channel_token_matches(&provided, &expected));
    }

    #[test]
    fn fixture_wrong_token_notification_is_rejected() {
        let expected = fixture_header("sync_bootstrap", "X-Goog-Channel-Token");
        let forged = fixture_header("wrong_token", "X-Goog-Channel-Token");
        assert_ne!(forged, expected);
        assert!(
            !channel_token_matches(&forged, &expected),
            "a channel-id match alone must never authenticate — the token must match too"
        );
    }

    #[tokio::test]
    async fn missing_headers_are_401() {
        let state = crate::test_app_state();
        let resp = google_calendar_push(State(state.clone()), HeaderMap::new()).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let mut only_id = HeaderMap::new();
        only_id.insert("x-goog-channel-id", "chan-1".parse().unwrap());
        let resp2 = google_calendar_push(State(state), only_id).await;
        assert_eq!(resp2.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Empty (present but blank) headers must be rejected exactly like
    // missing ones (CodeRabbit review finding on PR #677, Major/security,
    // fixed before founder review) — this is the Rust-side half of the
    // empty-placeholder-row fix; `connectors::google_calendar`'s own pg
    // tests cover the DB-level guards. ───────────────────────────────────

    #[tokio::test]
    async fn empty_channel_id_header_is_401_not_treated_as_absent() {
        let state = crate::test_app_state();
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-channel-id", "".parse().unwrap());
        headers.insert("x-goog-channel-token", "some-token".parse().unwrap());
        let resp = google_calendar_push(State(state), headers).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn empty_channel_token_header_is_401() {
        let state = crate::test_app_state();
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-channel-id", "chan-1".parse().unwrap());
        headers.insert("x-goog-channel-token", "".parse().unwrap());
        let resp = google_calendar_push(State(state), headers).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn whitespace_only_channel_id_header_is_401() {
        // `.trim()` is what the route actually checks — a header that is
        // technically non-empty but blank must be rejected the same way.
        let state = crate::test_app_state();
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-channel-id", "   ".parse().unwrap());
        headers.insert("x-goog-channel-token", "some-token".parse().unwrap());
        let resp = google_calendar_push(State(state), headers).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── commit-after-ingest: a failed ingest must leave the sync cursor
    // unchanged (CodeRabbit review finding on PR #677, Major, fixed before
    // founder review) ───────────────────────────────────────────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //     cargo test -p coralswarm-backend --lib routes::google_calendar_push::tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }

    /// Build a real `AppState` wired to `pool` (CodeRabbit review finding on
    /// PR #677, round 2): `crate::test_app_state()`'s default `db: None`/
    /// `store: None` make `Tenant::legacy(state)` carry `pool: None` too, so
    /// `ingest::submit` returns 503 before ever touching the seeded
    /// Postgres `pool` these tests assert against.
    /// Wiring both fields with a real `PostgresStore` (the same pattern
    /// `workers::google_watch_renewal`'s and `routes::ingest`'s own pg
    /// tests already use) makes `ingest_atom` exercise the REAL cloud path
    /// against `pool`.
    fn wire_real_store(pool: &sqlx::PgPool) -> Arc<AppState> {
        let mut owned = crate::test_app_state_owned(None);
        owned.db = Some(pool.clone());
        owned.store = Some(Arc::new(crate::store::PostgresStore::new(pool.clone())));
        Arc::new(owned)
    }

    /// A note on what these two tests deliberately do NOT attempt: a fully
    /// successful atom write additionally needs the target account's
    /// `accounts.dek_wrapped` (content-encryption key) provisioned, which
    /// neither of these tests seeds — reverse-engineering that setup
    /// correctly, with no way to run it against a live Postgres from this
    /// environment to confirm, would trade a verifiable test for an
    /// unverifiable guess. Both scenarios below are instead built from
    /// `ingest::submit` branches that return BEFORE DEK resolution ever
    /// runs (`routes::ingest.rs`: the graph-authorization 403 and the
    /// basic-input-validation 422 both precede it) — real code paths,
    /// deterministic outcomes, and exactly the two failure classes
    /// CodeRabbit's finding asked to be tested (422 = consumed/non-
    /// blocking; everything else = blocking).

    #[tokio::test]
    async fn ingest_and_commit_leaves_cursor_unchanged_on_a_blocking_failure_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping ingest_and_commit_leaves_cursor_unchanged_on_a_blocking_failure_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g31_push_commit_gate";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();

        // Seed an EXISTING sync cursor — the "before" state the test asserts
        // is unchanged after the blocking failure.
        sqlx::query(
            "INSERT INTO google_watch_channels (account_id, surface, resource_id, expires_at, sync_token, backfill_complete) \
             VALUES ($1, 'calendar', 'primary', now() + interval '7 days', 'sync-tok-before', true)",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .unwrap();

        // Deliberately a graph id that does NOT exist — `store.
        // graph_account_id` (routes::ingest.rs) returns `Ok(None)` and
        // `ingest::submit` answers 403 FORBIDDEN, well before any DEK
        // resolution. A real, non-422 failure: exactly the class the
        // commit gate must treat as blocking.
        let missing_graph_id = Uuid::new_v4();
        let item = crate::connectors::NormalizedItem {
            source_type: "google_calendar".into(),
            source_id: "evt-1".into(),
            content: "Some Event".into(),
            participants: vec![],
            timestamp: chrono::Utc::now(),
            scope: None,
            thread_id: None,
            channel_id: None,
            author_id: None,
            author_name: None,
        };

        let state = wire_real_store(&pool);
        let pending = crate::connectors::google_calendar::PendingSync {
            items: vec![item],
            commit: crate::connectors::google_calendar::PendingCommit::Complete("sync-tok-after".into()),
        };

        let ingested = ingest_and_commit(&state, &google_calendar::CalendarTarget::personal(&pool, account_id), missing_graph_id, owner, Tenant::legacy(&state), pending)
            .await
            .unwrap();
        assert_eq!(ingested, 0, "the graph doesn't exist — nothing can ingest");

        let sync_token_after: String =
            sqlx::query_scalar("SELECT sync_token FROM google_watch_channels WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            sync_token_after, "sync-tok-before",
            "a blocking (non-422) ingest failure must NOT advance the sync cursor — \
             it must still read the pre-pass value, not the pending commit"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn ingest_and_commit_advances_cursor_when_only_a_422_is_hit_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping ingest_and_commit_advances_cursor_when_only_a_422_is_hit_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g31_push_commit_success";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
        // Matches `routes::slack::resolve_primary_graph`'s own auto-create
        // shape for the Personal-plane primary ocean (see
        // `connectors::google_calendar`'s identical fixture fix — found via
        // a real pg run, CodeRabbit-directed, 2026-08-23 — for the full
        // rationale on `name` being NOT NULL and `owner_user_id` needing to
        // stay NULL for a non-'member' `kind`).
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO google_watch_channels (account_id, surface, resource_id, expires_at, sync_token, backfill_complete) \
             VALUES ($1, 'calendar', 'primary', now() + interval '7 days', 'sync-tok-before', true)",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .unwrap();

        // `source_type: ""` is rejected by `ingest::submit`
        // (`req.source_type.is_empty() && req.session_id.is_none()`
        // → 422) BEFORE the graph-authorization/DEK section runs, so this is
        // reachable with a valid, existing `graph_id` and no DEK setup. The
        // graph DOES exist (unlike the blocking-failure test), isolating
        // the ONE variable this test cares about: does a 422 alone block
        // the commit?
        let item = crate::connectors::NormalizedItem {
            source_type: "".into(),
            source_id: "evt-1".into(),
            content: "Some Event".into(),
            participants: vec![],
            timestamp: chrono::Utc::now(),
            scope: None,
            thread_id: None,
            channel_id: None,
            author_id: None,
            author_name: None,
        };
        let state = wire_real_store(&pool);
        let pending = crate::connectors::google_calendar::PendingSync {
            items: vec![item],
            commit: crate::connectors::google_calendar::PendingCommit::Complete("sync-tok-after".into()),
        };

        let ingested = ingest_and_commit(&state, &google_calendar::CalendarTarget::personal(&pool, account_id), graph_id, owner, Tenant::legacy(&state), pending)
            .await
            .unwrap();
        assert_eq!(ingested, 0, "the 422'd item was never actually ingested");

        let sync_token_after: String =
            sqlx::query_scalar("SELECT sync_token FROM google_watch_channels WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            sync_token_after, "sync-tok-after",
            "a 422 (deterministic validation rejection) must NOT block the cursor — \
             the pass still commits"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
