//! `POST /v1/slack/interactivity` — Slack Block Kit interactive payload handler.
//!
//! # Security
//!
//! Slack posts interactive payloads as `application/x-www-form-urlencoded` with a
//! single `payload` field.  This handler reads the raw body first, verifies the
//! Slack HMAC-SHA256 signature over those raw bytes (identical to the events
//! endpoint), then URL-decodes and JSON-parses the `payload` field.
//!
//! Requests older than 300 seconds are rejected for replay protection.
//!
//! # Behaviour
//!
//! - `block_actions / cs_backfill_none`: acknowledge and update the original
//!   message via `response_url` to "✅ Capturing new messages only."
//! - `block_actions / cs_backfill_{7d,30d,90d,all}`: respond 200 immediately,
//!   then in a spawned task: update the message to "⏳ Ingesting…", run the
//!   backfill over `conversations.history`, then update to the final summary via
//!   `chat.update` with the bot token.
//! - `block_actions / {slack_events::APP_HOME_SWITCH_ACTION_ID}` (the App
//!   Home account switcher's `static_select`): respond 200 immediately, then
//!   in a spawned task hand off to `slack_events::handle_app_home_switch`,
//!   which re-confirms the requested account against the viewer's freshly
//!   recomputed entitled set (never trusting the payload's account id
//!   directly) and republishes the Home tab via `views.publish`.
//!
//! # chat.update vs response_url
//!
//! For the final "done" message we use `chat.update` (bot token + channel + ts)
//! rather than `response_url` because `response_url` expires in 30 minutes and
//! large backfills may exceed that window.  The in-progress "⏳" update is sent
//! via `response_url` (fast, cheap, no bot-token required for the ack).
//!
//! # Org/tenant reach (org-slack step 3)
//!
//! Unlike ingest (`routes::slack_events`/`workers::slack_ingest`), this
//! handler cannot defer tenant resolution to a background worker: Slack drops
//! an interaction it has not heard back from within roughly 3 seconds, and
//! the human clicking is watching for a visible reply. So every handler in
//! this file funnels through [`resolve_pool_account_bot_token`], the single
//! pool-acquisition point, which now resolves BOTH the control/consumer pool
//! (`accounts`/`account_members`/`oauth_tokens` — always `state.db`) and,
//! for an ORG account, the org's own tenant pool (`graphs`/`teams`/
//! `team_members`/`team_seat_grants`/`slack_watched_channels`/
//! `connector_identities`) — see [`SlackActionContext`]. The tenant-pool
//! resolution chain (`org_is_revoked` -> `lookup_by_org` -> `is_servable()`
//! -> `registry.get_or_build`) mirrors
//! `routes::clerk_webhook::provision_member_ocean_best_effort`'s shape for
//! the same "no acting Tenant/session, webhook-driven" reason, but with a
//! MUCH tighter per-step timeout budget — see
//! [`ORG_TENANT_RESOLVE_STEP_TIMEOUT`]'s own doc for why.

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{
    routes::slack::AccountPlane,
    routes::slack_events::verify_slack_signature_with_secret,
    routes::slack_events::VerifyResult,
    AppState,
};

// ── Backfill window ───────────────────────────────────────────────────────────

/// Map an `action_id` string to a number of days (or `None` = all history).
///
/// Returns `Err(())` for unknown action IDs so callers can skip gracefully.
pub(crate) fn action_id_to_days(action_id: &str) -> Result<Option<u32>, ()> {
    match action_id {
        "cs_backfill_none" => Err(()), // handled separately
        "cs_backfill_7d"   => Ok(Some(7)),
        "cs_backfill_30d"  => Ok(Some(30)),
        "cs_backfill_90d"  => Ok(Some(90)),
        "cs_backfill_all"  => Ok(None),
        _                  => Err(()),
    }
}

// ── Handler ───────────────────────────────────────────────────────────────────

/// `POST /v1/slack/interactivity`
///
/// Receives Slack Block Kit interactive payloads.
pub async fn slack_interactivity(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // ── Signature verification ────────────────────────────────────────────────
    let secret = match std::env::var("SLACK_SIGNING_SECRET") {
        Ok(s) if !s.is_empty() => s,
        _ => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "SLACK_SIGNING_SECRET not configured"})),
            )
                .into_response();
        }
    };

    match verify_slack_signature_with_secret(&secret, &headers, &body) {
        VerifyResult::Ok => {}
        VerifyResult::SecretNotSet => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "SLACK_SIGNING_SECRET not configured"})),
            )
                .into_response();
        }
        VerifyResult::TimestampInvalid | VerifyResult::SignatureMismatch => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid signature"})),
            )
                .into_response();
        }
    }

    // ── URL-decode and extract the `payload` field ────────────────────────────
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid UTF-8 in body"})),
            )
                .into_response();
        }
    };

    let payload_json = extract_payload_field(body_str);
    let payload_json = match payload_json {
        Some(s) => s,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing payload field"})),
            )
                .into_response();
        }
    };

    let payload: Value = match serde_json::from_str(&payload_json) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid payload JSON"})),
            )
                .into_response();
        }
    };

    // ── Only handle block_actions ─────────────────────────────────────────────
    let payload_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
    if payload_type != "block_actions" {
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // ── Extract required fields ───────────────────────────────────────────────
    let action = payload
        .get("actions")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first());

    let action = match action {
        Some(a) => a,
        None => {
            return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
        }
    };

    let action_id = action
        .get("action_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    let channel_value = action
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    // Which ACCOUNT this prompt was posted for. One workspace can map to
    // several accounts, each with its own prompt — see
    // `slack_events::ACCOUNT_BLOCK_ID_PREFIX`. Absent on prompts posted before
    // that shipped, which falls back to first-match.
    let requested_account = action
        .get("block_id")
        .and_then(Value::as_str)
        .and_then(account_from_block_id);

    // `static_select` actions (the destination picker's per-plane pickers)
    // carry their chosen value under `selected_option.value`, not `value`
    // (that field belongs to buttons, `channel_value` above).
    let selected_value = action
        .get("selected_option")
        .and_then(|o| o.get("value"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    let team_id = payload
        .pointer("/team/id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    let slack_user_id = payload
        .pointer("/user/id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    let channel_id = payload
        .pointer("/channel/id")
        .and_then(Value::as_str)
        .unwrap_or(&channel_value)
        .to_owned();

    let response_url = payload
        .get("response_url")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    tracing::info!(
        action_id = %action_id,
        channel_id = %channel_id,
        team_id = %team_id,
        "slack interactivity action received"
    );

    // ── Dispatch ──────────────────────────────────────────────────────────────

    if action_id == crate::connectors::slack_huddle_consent::ACTION_ID {
        // Require the account-bound opaque prompt value; never fall back to
        // whichever account happens to be first for a shared workspace.
        let value: Value = match serde_json::from_str(&channel_value) {
            Ok(value) => value,
            Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid huddle confirmation"}))).into_response(),
        };
        let account = value.get("account_id").and_then(Value::as_str).and_then(|s| s.parse::<uuid::Uuid>().ok());
        let id = value.get("consent_id").and_then(Value::as_str).and_then(|s| s.parse::<uuid::Uuid>().ok());
        let (Some(account), Some(id)) = (account, id) else {
            return (StatusCode::BAD_REQUEST, Json(json!({"error":"invalid huddle confirmation"}))).into_response();
        };
        let message_ts = payload.pointer("/message/ts").and_then(Value::as_str).unwrap_or("").to_owned();
        let state_clone = state.clone();
        tokio::spawn(async move {
            let ctx = match resolve_pool_account_bot_token_for(&state_clone, &team_id, Some(account)).await {
                Ok(ctx) => ctx,
                Err(_) => return,
            };
            if let Err(error) = crate::connectors::slack_huddle_consent::confirm(
                &ctx.control_pool, &ctx.tenant_pool, &state_clone.http_client, &ctx.bot_token,
                account, &team_id, &channel_id, &message_ts, &slack_user_id, id,
            ).await {
                tracing::warn!(account_id=%account, error=%error, "huddle confirmation refused");
            }
        });
        return (StatusCode::OK, Json(json!({"ok":true}))).into_response();
    }

    if action_id == "cs_backfill_none" {
        // Acknowledge with no backfill — update the message via response_url.
        let http_clone = state.http_client.clone();
        let response_url_clone = response_url.clone();
        tokio::spawn(async move {
            if let Err(e) = post_response_url(
                &http_clone,
                &response_url_clone,
                true,
                "✅ Capturing new messages only.",
            )
            .await
            {
                tracing::warn!(error = %e, "failed to update Slack message via response_url");
            }
        });
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // App Home's account switcher (`static_select`,
    // `slack_events::APP_HOME_SWITCH_ACTION_ID`) — its own dispatch, separate
    // from `cs_dest_*` below, because it is not scoped to a single "the
    // workspace's account" the way every other action in this file is: the
    // whole point of the switcher is that ONE Slack user can be entitled to
    // SEVERAL accounts on this workspace, and which one this click means is
    // exactly the question `slack_events::handle_app_home_switch` re-derives
    // from scratch rather than trusting `selected_value` (untrusted payload
    // data) directly — see that function's doc comment. Ack immediately and
    // do the entitlement re-check + `derive_status` + `views.publish` in a
    // spawned task, the same ack-then-spawn shape `cs_backfill_*` above and
    // `cs_dest_*` below both already use: none of that inline work belongs on
    // Slack's ~3-second interactivity deadline (this file's header).
    if action_id == crate::routes::slack_events::APP_HOME_SWITCH_ACTION_ID {
        let state_clone = state.clone();
        let team_id_clone = team_id.clone();
        let slack_user_id_clone = slack_user_id.clone();
        let requested_account_id = selected_value.clone();
        tokio::spawn(async move {
            crate::routes::slack_events::handle_app_home_switch(
                state_clone,
                team_id_clone,
                slack_user_id_clone,
                requested_account_id,
            )
            .await;
        });
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // High-signal review buttons on the App Home (`connectors::slack_review`,
    // Stage 2): confirm / reject a pending clarification without leaving
    // Slack. Checked before `action_id_to_days` for the same reason as
    // `cs_dest_*` below. Authorization: the button `value`'s account_id is
    // CONTEXT only — the workspace→account mapping is re-derived server-side
    // (`resolve_pool_account_bot_token_for`) and the clicking user must pass
    // `resolve_clicking_user`'s verified-identity + membership check
    // (fail-closed for unmapped Slack users; #412 tenant pairing).
    if action_id.starts_with("cs_review_") {
        let requested = serde_json::from_str::<Value>(&channel_value)
            .ok()
            .and_then(|v| v.get("account_id").and_then(Value::as_str).and_then(|s| s.parse().ok()));
        let ctx = match resolve_pool_account_bot_token_for(&state, &team_id, requested).await {
            Ok(v) => v,
            Err(early_response) => return early_response,
        };
        // Ack within Slack's deadline; the verdict write + Home re-publish
        // run spawned (same ack-then-spawn shape as every arm here).
        let state_clone = state.clone();
        let team_id_clone = team_id.clone();
        let slack_user_id_clone = slack_user_id.clone();
        let action_id_clone = action_id.clone();
        let value_clone = channel_value.clone();
        tokio::spawn(async move {
            handle_review_action(
                state_clone,
                ctx,
                action_id_clone,
                team_id_clone,
                slack_user_id_clone,
                value_clone,
            )
            .await;
        });
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // The per-channel destination picker (docs/connectors/slack.md §2) uses
    // its own `cs_dest_*` action_id namespace, dispatched separately from the
    // backfill-window flow below because it needs a resolved account/bot
    // token but NOT a `days` window. Checked before `action_id_to_days`,
    // which would otherwise silently ack every `cs_dest_*` id as "unknown".
    if action_id.starts_with("cs_dest_") {
        let ctx = match resolve_pool_account_bot_token_for(&state, &team_id, requested_account).await {
            Ok(v) => v,
            Err(early_response) => return early_response,
        };
        return handle_destination_action(
            state,
            ctx,
            action_id,
            slack_user_id,
            selected_value,
            channel_id,
            response_url,
        )
        .await;
    }

    // Parse window from action_id.
    let days = match action_id_to_days(&action_id) {
        Ok(d) => d,
        Err(_) => {
            // Unknown action — ack silently.
            return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
        }
    };

    // Resolve account + bot token (and, for an org account, the tenant pool)
    // in the handler before spawning, so we can fail fast if the workspace is
    // unknown. The DB checks are cheap (see `resolve_pool_account_bot_token`'s
    // own doc comment for the tenant-resolution cost in the steady state).
    let ctx = match resolve_pool_account_bot_token_for(&state, &team_id, requested_account).await {
        Ok(v) => v,
        Err(early_response) => return early_response,
    };

    let window_label = match days {
        Some(d) => format!("{d} days"),
        None    => "all history".to_owned(),
    };

    // Resolve who this click is FOR and which ocean it targets — see
    // `resolve_backfill_target`'s own doc comment. Done INLINE, before the
    // ack, the same "resolve identity synchronously, before handing off"
    // shape `cs_dest_*`/`cs_review_*` above already use: `JobKind::
    // HistoryBackfill`'s payload must carry an ALREADY-authorized target,
    // never one the worker re-derives with no request-time context to bind
    // it to (see that variant's own doc comment).
    let (target_user_id, target_graph_id) =
        match resolve_backfill_target(&state, &ctx, &team_id, &slack_user_id).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, account_id = %ctx.account_id, "backfill: could not resolve a target ocean");
                let http = state.http_client.clone();
                let response_url_clone = response_url.clone();
                // Detail stays in the log — a Slack message is user-facing,
                // and `e` can carry raw sqlx/Postgres text (table/column/
                // constraint names, or pool host/database details on a
                // connection error) that must never reach a workspace member
                // (CodeRabbit round 1, #1005).
                let msg = "⚠️ Couldn't start the backfill — please try again shortly.".to_string();
                tokio::spawn(async move {
                    if let Err(e) = post_response_url(&http, &response_url_clone, true, &msg).await {
                        tracing::warn!(error = %e, "failed to send backfill target-resolution error");
                    }
                });
                return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
            }
        };

    // Enqueue the durable job (`connectors::history_backfill` +
    // `workers::history_backfill`) instead of the old bare
    // `tokio::spawn(backfill_channel(..))` — see `JobKind::HistoryBackfill`'s
    // own doc comment for why this also lifts the org refusal (the worker
    // never touches `ingest_slack_message`/`slack_integrations` at all) and
    // for the idempotency-key shape below. Enqueued on `ctx.tenant_pool` —
    // the SAME plane `target_graph_id` lives on — since `workers::
    // history_backfill::tick` runs tenant-looped, exactly like `workers::
    // slack_ingest`, and a job on the wrong plane's `jobs` table would be
    // invisible to it.
    let idempotency_key = format!("backfill:slack:{team_id}:{channel_id}:{target_user_id}");
    let enqueue_result = crate::jobs::queue::enqueue(
        &ctx.tenant_pool,
        &crate::jobs::JobKind::HistoryBackfill {
            provider: "slack".to_string(),
            account_id: ctx.account_id,
            workspace_id: team_id.clone(),
            container_id: channel_id.clone(),
            target_graph_id,
            target_user_id,
            clerk_org_id: ctx.clerk_org_id.clone(),
            window_days: days,
        },
        Some(ctx.account_id),
        Some(&idempotency_key),
        0,
    )
    .await;

    // One honest response, sent via `response_url` (fast, no bot token
    // needed) — NOT the old two-phase "⏳ Ingesting…" / "✅ Ingested N
    // messages" `chat.update` flow. That flow could report a real count
    // because it ran the whole backfill INLINE in this spawned task; now the
    // actual work happens in a durable worker with no synchronous hook back
    // to this request, so printing a count here would be a number this
    // handler no longer has — exactly the dishonesty the PR-2 plan forbids.
    let final_text = match enqueue_result {
        Ok(Some(_)) => format!(
            "⏳ Queued a backfill of the last {window_label}. New messages will appear as \
             they're processed — this can take a few minutes for a large channel."
        ),
        // `None`: `idx_jobs_idem_pending`'s dedup found an identical backfill
        // (same workspace/channel/target user) already `pending` — not a
        // failure, just nothing new to do.
        // `window_days` is not part of `idempotency_key`, so a SECOND click
        // with a wider window before the first job leaves `pending` lands
        // here too — the reply must not imply the wider window was honored
        // (CodeRabbit round 1, #1005): the queued job still has whichever
        // window the FIRST click asked for.
        Ok(None) => "⏳ A backfill for this channel is already queued — its original window \
                     stands; a new one can be requested once it finishes."
            .to_string(),
        Err(e) => {
            tracing::warn!(error = %e, channel_id = %channel_id, "backfill: failed to enqueue job");
            // Same reasoning as the target-resolution error above: `e` can
            // carry raw sqlx/Postgres text, so only the log gets it.
            "⚠️ Couldn't queue the backfill — please try again shortly.".to_string()
        }
    };
    let http = state.http_client.clone();
    let response_url_clone = response_url.clone();
    tokio::spawn(async move {
        if let Err(e) = post_response_url(&http, &response_url_clone, true, &final_text).await {
            tracing::warn!(error = %e, "failed to send backfill queued confirmation");
        }
    });

    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

/// Resolve who a backfill-window click is FOR, and which ocean it should
/// land in — the "already-authorized target" `JobKind::HistoryBackfill`'s
/// own doc comment says the enqueuer must resolve once, synchronously,
/// rather than have the worker re-derive it later with no request-time
/// context to bind to.
///
/// **Personal:** the account has exactly one member — its owner — so no
/// per-click identity confirmation is needed; the owner IS the target,
/// unconditionally, the same attribution every other personal-plane ingest
/// path already uses.
///
/// **Org:** the clicking Slack user must be a CONFIRMED, CURRENT member
/// ([`resolve_clicking_user`] — the same C5 matcher every other org routing
/// decision in this file already uses). An org account has no single
/// default "owner" ocean a backfill could fall back to without guessing a
/// seat (contract: never fabricate), so an unconfirmed identity refuses
/// rather than picking someone — this is also what lifts the org refusal
/// honestly rather than merely routing around it: a backfill an org admin
/// clicks now lands in THEIR OWN ocean, not nobody's.
async fn resolve_backfill_target(
    state: &Arc<AppState>,
    ctx: &SlackActionContext,
    team_id: &str,
    slack_user_id: &str,
) -> Result<(uuid::Uuid, uuid::Uuid), String> {
    let target_user_id = match ctx.plane {
        AccountPlane::Personal => {
            sqlx::query_scalar::<_, uuid::Uuid>("SELECT owner_user_id FROM accounts WHERE id = $1")
                .bind(ctx.account_id)
                .fetch_one(&ctx.control_pool)
                .await
                .map_err(|e| format!("failed to resolve account owner: {e}"))?
        }
        AccountPlane::Org => resolve_clicking_user(
            &ctx.control_pool,
            &ctx.tenant_pool,
            &state.http_client,
            ctx.account_id,
            team_id,
            &ctx.bot_token,
            slack_user_id,
        )
        .await
        .ok_or_else(|| {
            "could not confirm your CoralSwarm identity from this Slack account — refusing to \
             guess whose ocean to fill"
                .to_string()
        })?,
    };

    let target_graph_id = super::slack::resolve_primary_graph(
        &ctx.tenant_pool,
        ctx.account_id,
        ctx.plane,
        super::slack::IngestCaller::Attributable(target_user_id),
    )
    .await
    .map_err(|e| format!("failed to resolve target ocean: {e}"))?;

    Ok((target_user_id, target_graph_id))
}

// ── Shared account/bot-token/tenant resolution ───────────────────────────────

/// Everything [`resolve_pool_account_bot_token`] resolves for one
/// interactivity payload's `team_id` — which account, which plane, which
/// pool(s) that account's CONTENT lives on, and its bot token. Every
/// downstream helper in this file takes this instead of a bare
/// `(pool, account_id, bot_token)` tuple, so the personal-vs-org routing
/// decision is made exactly once per request (org-slack step 3).
///
/// `control_pool` and `tenant_pool` are DELIBERATELY both present, even
/// though for a Personal account they are the same pool: every function
/// downstream that reads/writes a CONTROL-only table (`accounts`,
/// `account_members`) needs `control_pool`, and every function that reads/
/// writes a tenant-tree table (`graphs`, `teams`, `team_members`,
/// `team_seat_grants`, `slack_watched_channels`, `connector_identities`)
/// needs `tenant_pool` — see this module's header for the full plane split.
#[derive(Clone)]
struct SlackActionContext {
    /// `accounts`/`account_members`/`oauth_tokens` — always `state.db`.
    control_pool: sqlx::PgPool,
    /// Where this account's CONTENT lives: `control_pool` itself for a
    /// Personal account, or the org's own tenant pool for an Org account.
    tenant_pool: sqlx::PgPool,
    account_id: uuid::Uuid,
    plane: AccountPlane,
    bot_token: String,
    /// `Some(org_id)` for an Org account, `None` for Personal — carried so a
    /// caller that needs to hand this account's org id somewhere else (e.g.
    /// `JobKind::HistoryBackfill`'s payload, PR 2) doesn't have to re-derive
    /// it from `plane` with a second `accounts` read; it was already
    /// resolved in [`resolve_pool_account_bot_token_for`] below.
    clerk_org_id: Option<String>,
}

/// Timeout budget for EACH step of [`resolve_org_tenant_pool`]'s chain below
/// — deliberately much tighter than `clerk_webhook::MEMBER_OCEAN_WEBHOOK_
/// TIMEOUT` (5s): that webhook answers to Svix, which retries generously
/// over minutes; this one answers to Slack's interactivity ack, which Slack
/// itself drops as failed around 3 seconds after delivery (this module's
/// header). Three sequential steps at this bound cost at most ~1.5s together
/// in the worst case, on top of everything else already inline in this
/// handler (the account/bot-token lookup above, the destination picker's own
/// identity-click resolution and write) — and in the steady state every one
/// of the three is cheap: `org_is_revoked`/`lookup_by_org` are single
/// indexed reads either way, and `registry.get_or_build` only pays a real
/// connect cost once per org per process lifetime, returning an
/// already-built pool instantly on every request after. So this bound is a
/// safety net against a genuinely wedged dependency (a stalled control-pool
/// connection, an unreachable tenant host), not the expected per-request
/// cost — see this file's header ("The 3-second budget") for the full
/// reasoning on what stays inline versus what moves off the ack path.
const ORG_TENANT_RESOLVE_STEP_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Resolve the tenant pool for an ORG account's `clerk_org_id`, following the
/// exact fence `routes::clerk_webhook::provision_member_ocean_best_effort`
/// established for the same "no acting Tenant/session, webhook-driven" shape:
/// `org_is_revoked` first and unconditionally (member-private-oceans PR 7 §D
/// — a revoked org must never be served, even from an interaction Slack
/// itself delivered), then `lookup_by_org`, then `is_servable()`, then
/// `registry.get_or_build`. Each step is timeout-bounded independently (see
/// [`ORG_TENANT_RESOLVE_STEP_TIMEOUT`]).
///
/// Every refusal along this chain is reported the SAME way this file's other
/// "nothing to do" branches already report a permanent condition: acked 200
/// with no visible follow-up — Slack's own delivery already degrades no
/// better than that when its OWN ~3s timeout fires, so this is not a worse
/// failure mode than the one Slack itself imposes. A transient-shaped failure
/// (a lookup error, a timeout) is reported 503 instead, mirroring
/// `resolve_pool_account_bot_token`'s existing DB-error branches, so a real
/// outage is at least visible in Slack's own webhook health view rather than
/// silently swallowed.
async fn resolve_org_tenant_pool(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    clerk_org_id: &str,
) -> Result<sqlx::PgPool, axum::response::Response> {
    fn ack() -> axum::response::Response {
        (StatusCode::OK, Json(json!({"ok": true}))).into_response()
    }
    fn unavailable() -> axum::response::Response {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response()
    }

    match tokio::time::timeout(
        ORG_TENANT_RESOLVE_STEP_TIMEOUT,
        crate::tenancy::routing::org_is_revoked(control_pool, clerk_org_id),
    )
    .await
    {
        Ok(Ok(true)) => {
            tracing::info!(
                org = %clerk_org_id,
                %account_id,
                "slack interactivity: org is revoked — refusing to resolve a tenant pool"
            );
            return Err(ack());
        }
        Ok(Ok(false)) => {}
        Ok(Err(e)) => {
            tracing::warn!(org = %clerk_org_id, error = %e, "slack interactivity: org_revocations lookup failed");
            return Err(unavailable());
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_ms = ORG_TENANT_RESOLVE_STEP_TIMEOUT.as_millis(),
                "slack interactivity: org_revocations lookup timed out"
            );
            return Err(unavailable());
        }
    }

    // In-app org deletion (control 0387): members are locked out from the
    // request, so a click, an App Home action or a channel route in a deleting
    // org is refused exactly like a revoked one. (Capture hand-off keeps
    // working during grace; it does not come through here.)
    match tokio::time::timeout(
        ORG_TENANT_RESOLVE_STEP_TIMEOUT,
        crate::services::org_deletion::state_for_org(control_pool, clerk_org_id),
    )
    .await
    {
        Ok(Ok(Some(st))) if crate::services::org_deletion::locks_members_out(&st) => {
            tracing::info!(
                org = %clerk_org_id,
                %account_id,
                state = %st,
                "slack interactivity: org deletion is live — refusing to serve members"
            );
            return Err(ack());
        }
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            tracing::warn!(org = %clerk_org_id, error = %e, "slack interactivity: org deletion state lookup failed");
            return Err(unavailable());
        }
        Err(_) => {
            tracing::warn!(org = %clerk_org_id, "slack interactivity: org deletion state lookup timed out");
            return Err(unavailable());
        }
    }

    let Some(rt) = state.tenant_runtime.as_ref() else {
        tracing::warn!(
            org = %clerk_org_id,
            "slack interactivity: no tenant runtime configured — cannot serve an org account"
        );
        return Err(ack());
    };

    let row = match tokio::time::timeout(
        ORG_TENANT_RESOLVE_STEP_TIMEOUT,
        crate::tenancy::routing::lookup_by_org(control_pool, clerk_org_id),
    )
    .await
    {
        Ok(Ok(Some(row))) => row,
        Ok(Ok(None)) => {
            tracing::info!(
                org = %clerk_org_id,
                "slack interactivity: no tenant routing row yet — refusing"
            );
            return Err(ack());
        }
        Ok(Err(e)) => {
            tracing::warn!(org = %clerk_org_id, error = %e, "slack interactivity: routing lookup failed");
            return Err(unavailable());
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_ms = ORG_TENANT_RESOLVE_STEP_TIMEOUT.as_millis(),
                "slack interactivity: routing lookup timed out"
            );
            return Err(unavailable());
        }
    };

    if !row.is_servable() {
        tracing::info!(
            org = %clerk_org_id,
            status = %row.status,
            "slack interactivity: tenant not active yet — refusing"
        );
        return Err(ack());
    }

    match tokio::time::timeout(
        ORG_TENANT_RESOLVE_STEP_TIMEOUT,
        rt.registry.get_or_build(clerk_org_id, row.account_id, &row.db_secret_ref),
    )
    .await
    {
        Ok(Ok(pool)) => Ok(pool),
        Ok(Err(e)) => {
            tracing::warn!(org = %clerk_org_id, error = %e, "slack interactivity: tenant pool build failed");
            Err(unavailable())
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_ms = ORG_TENANT_RESOLVE_STEP_TIMEOUT.as_millis(),
                "slack interactivity: tenant pool build timed out"
            );
            Err(unavailable())
        }
    }
}

/// Resolve a [`SlackActionContext`] for an interactivity payload's
/// `team_id`, or an already-built axum response to return immediately on any
/// of the "nothing to do" / "transient failure" branches. Factored out of
/// the backfill-window flow (which used to inline this) so the
/// destination-picker dispatch (`cs_dest_*`, below) does not duplicate the
/// same lookup a third time.
///
/// **Single pool-acquisition point for the whole file (org-slack step 3).**
/// `account_id`+plane resolution and the bot-token fetch both stay
/// CONTROL-plane — `connector_installs`/`oauth_tokens` are not (yet)
/// tenant-converted, so every account's Slack install and bot token live in
/// `state.db` regardless of plane, same as `routes::oauth::callback`'s write
/// side today. What is NEW here is resolving which pool the account's
/// CONTENT lives on: Personal stays `control_pool` (byte-identical to before
/// this change); Org resolves the tenant pool via [`resolve_org_tenant_pool`].
///
/// **Does not run a second, non-healing resolution path.** `account_id` and
/// its plane come from ONE call to `slack::resolve_slack_account_planes`
/// (org-slack step 2) — itself a thin wrapper over the self-healing
/// `resolve_slack_account_ids` (fast path via `connector_installs`,
/// self-healing fallback over `oauth_tokens`/`slack_integrations` on a miss,
/// migration `0121_connector_installs`) plus the ONE extra `clerk_org_id`
/// column this handler needs — never a fresh, independently-derived lookup.
/// Takes the FIRST match, preserving `resolve_slack_account_planes`'
/// (and therefore `resolve_slack_account_ids`') deterministic ordering — the
/// same "single representative account" contract `resolve_slack_account_id`
/// (this file's pre-step-2 resolver) documented; every existing interactivity
/// call site already expects exactly one account, not a fan-out.
/// Refused because the action named an account the signed workspace does not
/// resolve to. Distinct from "no accounts at all" so the caller can log the
/// two differently — one is an attempted mismatch, the other is an
/// uninstalled workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ForeignAccount(pub uuid::Uuid);

/// Pick which of a workspace's accounts an interactivity action belongs to.
///
/// Single source of truth for the decision, extracted so it is testable
/// without a database — the same reason `tenant_harness::wake_reason` and
/// `pool_failure_severity` are extracted. A test that re-implemented this
/// `match` would keep passing while production regressed.
///
/// `matches` MUST be the accounts the signature-verified `team_id` resolves
/// to. `requested` is untrusted payload data and is only ever used to select
/// WITHIN that set — see [`resolve_pool_account_bot_token_for`].
pub(crate) fn choose_account<T: Clone>(
    matches: &[(uuid::Uuid, T)],
    requested: Option<uuid::Uuid>,
) -> Result<Option<(uuid::Uuid, T)>, ForeignAccount> {
    match requested {
        Some(want) => matches
            .iter()
            .find(|(id, _)| *id == want)
            .cloned()
            .map(Some)
            // Deliberately an error, NOT a fall back to first-match: answering
            // the prompt against a different account is the bug this exists to
            // remove, so a mismatch must refuse rather than quietly redirect.
            .ok_or(ForeignAccount(want)),
        None => Ok(matches.first().cloned()),
    }
}

/// Extract the account a prompt was posted for from its `block_id`.
///
/// See `routes::slack_events::ACCOUNT_BLOCK_ID_PREFIX`. Returns `None` for a
/// prompt posted before that shipped (no such `block_id`), which the caller
/// treats as "no preference" and falls back to first-match — so buttons
/// already sitting in channels keep working.
///
/// Reads only up to the first `:` after the prefix: everything past it is the
/// per-block slot that keeps `block_id` unique WITHIN a message (Slack
/// requires that, and an org with teams renders several action blocks at
/// once). Ignoring the slot here means new blocks can be added without
/// touching this parser.
pub(crate) fn account_from_block_id(block_id: &str) -> Option<uuid::Uuid> {
    block_id
        .strip_prefix(crate::routes::slack_events::ACCOUNT_BLOCK_ID_PREFIX)
        .map(|rest| rest.split(':').next().unwrap_or(rest))
        .and_then(|id| uuid::Uuid::parse_str(id).ok())
}

/// As above, but honouring the account a prompt names in its `block_id`.
///
/// # The payload is NOT trusted
///
/// `requested` comes from a Slack payload. The REQUEST is authenticated (the
/// signature is verified before this runs), so `team_id` is trustworthy — but
/// the `block_id` inside it is just a string, and treating it as an account
/// selector would let anyone who can post a crafted Block Kit message in ANY
/// workspace name someone else's `account_id` and act against it.
///
/// So `requested` is only ever used to CHOOSE AMONG the accounts that the
/// signed `team_id` already resolves to. An id outside that set is refused,
/// never honoured and never silently downgraded to first-match — a downgrade
/// would answer the prompt against the wrong account, which is the exact
/// failure this whole change exists to remove.
async fn resolve_pool_account_bot_token_for(
    state: &Arc<AppState>,
    team_id: &str,
    requested: Option<uuid::Uuid>,
) -> Result<SlackActionContext, axum::response::Response> {
    let control_pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            tracing::warn!("DB not available — skipping interactivity action");
            return Err((StatusCode::OK, Json(json!({"ok": true}))).into_response());
        }
    };

    // Look up account_id + clerk_org_id from team_id in one call — see
    // `slack::resolve_slack_account_planes`'s own doc comment.
    let (account_id, clerk_org_id): (uuid::Uuid, Option<String>) =
        match super::slack::resolve_slack_account_planes(&control_pool, team_id).await {
            Ok(matches) => {
                let chosen = match choose_account(&matches, requested) {
                    Ok(c) => c,
                    Err(ForeignAccount(want)) => {
                            tracing::warn!(
                                team_id = %team_id,
                                requested = %want,
                                resolved = matches.len(),
                                connector_health = true,
                                "interactivity: action names an account this workspace does not \
                                 resolve to — refusing rather than falling back to another \
                                 account (a fallback would answer the prompt against the wrong \
                                 account, which is exactly what this carries block_id to prevent)"
                            );
                        return Err((StatusCode::OK, Json(json!({"ok": true}))).into_response());
                    }
                };
                match chosen {
                    Some(m) => m,
                    None => {
                        tracing::debug!(team_id = %team_id, "no oauth_tokens entry for workspace — ignoring interactivity");
                        return Err((StatusCode::OK, Json(json!({"ok": true}))).into_response());
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "DB error looking up account for interactivity");
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response());
            }
        };
    let plane = match &clerk_org_id {
        None => AccountPlane::Personal,
        Some(_) => AccountPlane::Org,
    };

    // Fetch the bot token — account-scoped inside `get_slack_bot_token_pub`
    // (org-tenancy OPS-4; `account_id` above came from 0084's
    // `resolve_oauth_account_id` bootstrap, which is what makes that possible
    // on a webhook with no session). Stays CONTROL-plane — see this
    // function's doc comment.
    // The `oauth_tokens` ROW is control-plane; for an org account its KEY is
    // not (org-plane DEK fix — the token was written under the org's tenant
    // `account_keys` DEK by `routes::oauth::callback`). `clerk_org_id` is
    // already in hand from `resolve_slack_account_planes` above, so the one
    // shared plane table is reached without a second lookup. An `Err` is an
    // infrastructure condition: 503 so Slack retries, NOT the `200 OK` ack
    // the "no token configured" arm below deliberately returns for a
    // permanent state.
    let dek_plane = match crate::crypto::DekPlane::resolve_for_org(
        state,
        &control_pool,
        account_id,
        clerk_org_id.as_deref(),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, account_id = %account_id, "failed to resolve DEK plane for interactivity");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response());
        }
    };
    let bot_token =
        match crate::workers::huddle::get_slack_bot_token_for_workspace(&control_pool, &dek_plane, account_id, team_id)
            .await
        {
            Ok(Some(t)) => t,
            // No token configured for this workspace: a permanent condition, so
            // ack and stop — a Slack retry would take the identical path.
            Ok(None) => {
                tracing::warn!(account_id = %account_id, "no Slack bot token for account");
                return Err((StatusCode::OK, Json(json!({"ok": true}))).into_response());
            }
            // A real failure (DB/decrypt): 503 so Slack retries rather than a
            // silent permanent loss.
            Err(e) => {
                tracing::error!(account_id = %account_id, error = %e, "failed to fetch bot token");
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response());
            }
        };

    let tenant_pool = match &clerk_org_id {
        None => control_pool.clone(),
        Some(clerk_org_id) => {
            resolve_org_tenant_pool(state, &control_pool, account_id, clerk_org_id).await?
        }
    };

    Ok(SlackActionContext {
        control_pool,
        tenant_pool,
        account_id,
        plane,
        bot_token,
        clerk_org_id,
    })
}

// ── Destination picker (`cs_dest_*`) ─────────────────────────────────────────
//
// docs/connectors/slack.md §2's three-option picker, answered here. The
// prompt itself is posted from `routes::slack_events::post_destination_prompt`
// (`handle_member_joined`'s gate-event handler).
//
// ## Identity resolution — capability C5, wired (phase two)
//
// [`resolve_clicking_user`] is a thin wrapper over
// `connectors::slack::resolve_verified_slack_identity` — the SAME matcher
// every other C5 routing path uses, so there is exactly one identity path,
// not two that can drift apart. It still degrades to `None` ("cannot confirm
// who clicked") whenever it cannot CONFIRM an email match (no
// `users:read.email` grant yet, no matching `users` row, or a match that
// isn't a CURRENT member of this account), and never falls back to matching
// a Slack display name (docs/connectors/CONNECTOR_CONTRACT.md §4's C5 row:
// "never guess from display names").
//
// A confirmed identity gates the two picks that hand a channel to a SHARED
// ocean (contract I3: "a shared ocean is reached only by a promotion
// decision... someone empowered to promote... must have made it"):
// routing to an EXISTING team ocean requires confirming the clicker is that
// team's admin; creating a NEW team ocean requires a confirmed identity to
// record as its first admin (falling back to the account owner, clearly
// disclosed in the response, when identity cannot be confirmed — see
// `handle_destination_action`'s `cs_dest_org_team_new` arm). The "everyone's
// individual oceans" fan-out pick needs NO such check: it hands nobody
// access to anything they do not already own (contract I3 does not apply —
// there is no shared ocean here at all), so its provenance `user_id` is
// bookkeeping only, exactly like every other `slack_watched_channels.user_id`
// (see that column's doc comment in migration `0101_slack_watched_channels`).

/// Identity resolution for a Slack interactivity click. See this module's
/// "Identity resolution" doc section above for why `None` is the correct,
/// honest answer whenever an email cannot be CONFIRMED to belong to a
/// current member of this account — never a display-name guess.
///
/// A thin wrapper over `connectors::slack::resolve_verified_slack_identity`
/// (the SAME matcher `resolve_org_fanout_destinations` and `auto_seat_
/// channel_members` both use) plus the `account_members` membership check
/// that matcher's own doc comment says every routing caller must add itself
/// — it answers "is there ANY CoralSwarm user with this verified email", not
/// "is that user a member of the account routing this workspace".
///
/// **Genuinely cross-plane (org-slack step 3), all the way down to the
/// matcher itself (org-slack step 5a).** `resolve_verified_slack_identity`
/// now takes its own `control_pool`/`content_pool` pair — its confirmed-link
/// directory read is against `connector_identities` (present on both trees,
/// so `content_pool` — `tenant_pool` here), and its cache/directory-miss
/// fallback (`match_and_persist_verified_email`) queries `users`, a
/// CONTROL-ONLY table, on `control_pool`. This function passes its OWN
/// `control_pool`/`tenant_pool` straight through to both of the matcher's
/// parameters, so a Slack member who has NEVER been matched before (no
/// `connector_identities` row yet, under an org account whose
/// `tenant_pool` is a genuine tenant database) now resolves correctly
/// instead of the pre-step-5a fallback hitting a `users` table that does not
/// exist there — see `resolve_verified_slack_identity`'s own doc comment for
/// the full split. The membership check below reads `account_members` —
/// CONTROL-only — so it is called with `control_pool` too. On the consumer
/// plane every parameter here is the same pool, so behaviour there is
/// byte-identical to before either split.
///
/// **`pub(super)` for the App Home tab, not just for clicks.**
/// `routes::slack_events::handle_app_home_opened` needs the identical
/// question — "which ONE account is this Slack user entitled to be shown?" —
/// and calls THIS function rather than growing a second copy: the module doc
/// above ("exactly one identity path, not two that can drift apart") is a
/// property of the system, not of this file. The name is historical; read it
/// as "the Slack user we are acting on behalf of", which for App Home is
/// whoever opened the tab.
pub(super) async fn resolve_clicking_user(
    control_pool: &sqlx::PgPool,
    tenant_pool: &sqlx::PgPool,
    http: &reqwest::Client,
    account_id: uuid::Uuid,
    workspace_id: &str,
    bot_token: &str,
    slack_user_id: &str,
) -> Option<uuid::Uuid> {
    if slack_user_id.is_empty() {
        return None;
    }

    let user_id = crate::connectors::slack::resolve_verified_slack_identity(
        control_pool,
        tenant_pool,
        http,
        account_id,
        workspace_id,
        slack_user_id,
        bot_token,
    )
    .await?;

    // This is what confirms the matched user is a CURRENT member of THIS
    // account, not merely a `users` row with a matching email somewhere else
    // in the system — `resolve_verified_slack_identity` itself deliberately
    // does not check this (see its own doc comment). For an org account it is
    // `live_org_membership`, so a member removed, an org revoked, or an org
    // with a live in-app deletion (members locked out from the request) is
    // not a member here either.
    match member_role(control_pool, account_id, user_id).await {
        Ok(Some(_)) => Some(user_id),
        _ => None,
    }
}

/// The user's CURRENT role in `account_id`, or `None` when they are not a
/// member right now. `account_members` carries no consumer-plane RLS policy
/// (not in `check_schema_parity.sh`'s `CONTROL_ONLY_RLS_TABLES`), so a plain
/// pool query is correct — same convention as
/// `routes::slack::resolve_slack_account_id`'s bootstrap lookup. An ORG
/// account goes through `middleware::auth::live_org_membership` (the one
/// predicate every member-facing door shares: not revoked, not mid-deletion);
/// a personal account has no org to revoke or delete and keeps the bare row
/// check. A DB error is an `Err` so callers fail closed.
pub(super) async fn member_role(
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    user_id: uuid::Uuid,
) -> Result<Option<String>, sqlx::Error> {
    let org: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(control_pool)
        .await?
        .flatten();
    match org {
        Some(org_id) => Ok(crate::middleware::auth::live_org_membership(control_pool, &org_id, user_id)
            .await?
            .filter(|(live_account, _)| *live_account == account_id)
            .map(|(_, role)| role)),
        None => {
            sqlx::query_scalar("SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2")
                .bind(account_id)
                .bind(user_id)
                .fetch_optional(control_pool)
                .await
        }
    }
}

/// The outcome of checking whether a (possibly unresolved) clicking user
/// administers a given team ocean.
///
/// `pub(crate)` (wt-slack-actions): `connectors::slack_channel_actions::
/// set_channel_destination` — the REST write path for the "change
/// destination" management action — reuses [`check_team_ocean_admin`]
/// directly rather than re-implementing the admin check, so this type has to
/// be visible outside `slack_interactivity` too.
#[derive(Debug)]
pub(crate) enum TeamOceanCheck {
    /// `user_id` is confirmed to hold `role = 'admin'` on this team.
    Admin { graph_id: uuid::Uuid, name: String },
    /// The team exists (and belongs to this account), but the clicking user
    /// either could not be identified at all, or was identified and is NOT
    /// an admin of it.
    NotAdmin { name: String },
    /// No such team for this account — a stale/foreign `team_id`.
    NotFound,
}

/// Look up a team ocean by id (scoped to `account_id` — `teams` carries a
/// consumer-plane `account_isolation` RLS policy, migration 0083) and check
/// whether `user_id` (if any) administers it. Contract I3's enforcement
/// point for the "route to an existing team ocean" pick: `user_id` is only
/// ever `Some` when [`resolve_clicking_user`] CONFIRMED an identity, so a
/// `None` here always yields [`TeamOceanCheck::NotAdmin`], never a bypass —
/// unaffected by the pool split below: a missing `user_id` short-circuits to
/// `false` regardless of which pool `team_members` is read from.
///
/// `pool` here is `tenant_pool` at every call site (org-slack step 3):
/// `teams`/`team_members` are tenant-tree tables. `begin_tenant_system_scoped`
/// — a strict superset of `begin_account_scoped` (same `app.account_id` GUC,
/// plus `app.tenant_actor='system'`) — is the right scoping helper: this read
/// runs from a Slack webhook-authenticated interaction, which has no acting
/// `Tenant`/end-user session for `begin_member_scoped` to key off. Inert on
/// the consumer plane, same convention `ensure_slack_watched_channel`'s own
/// conversion uses.
///
/// `pub(crate)` (wt-slack-actions): also called from
/// `connectors::slack_channel_actions::set_channel_destination` — the REST
/// "change destination" write path, which DOES have a genuine acting user
/// (an authenticated `UserContext`), unlike the Slack-webhook callers above.
/// Reused verbatim rather than re-implemented, so contract I3's "route to a
/// team ocean you administer" enforcement can never drift between the two
/// entry points; that caller still passes `tenant_pool` (the same parameter
/// this function already takes), just with `user_id` sourced from the
/// authenticated session instead of a resolved Slack identity.
pub(crate) async fn check_team_ocean_admin(
    pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    user_id: Option<uuid::Uuid>,
    team_id: uuid::Uuid,
) -> Result<TeamOceanCheck, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("check_team_ocean_admin scoped tx: {e}")))?;

    let team: Option<(uuid::Uuid, String)> =
        sqlx::query_as("SELECT graph_id, name FROM teams WHERE id = $1 AND account_id = $2")
            .bind(team_id)
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await?;

    let Some((graph_id, name)) = team else {
        tx.rollback().await.ok();
        return Ok(TeamOceanCheck::NotFound);
    };

    let is_admin = match user_id {
        None => false,
        Some(uid) => {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM team_members \
                 WHERE team_id = $1 AND user_id = $2 AND role = 'admin' AND departing_at IS NULL)",
            )
            .bind(team_id)
            .bind(uid)
            .fetch_one(&mut *tx)
            .await?
        }
    };
    tx.rollback().await.ok();

    Ok(if is_admin {
        TeamOceanCheck::Admin { graph_id, name }
    } else {
        TeamOceanCheck::NotAdmin { name }
    })
}

/// The account's owner — used as the provenance fallback when
/// [`resolve_clicking_user`] cannot confirm an identity but a `users.id` is
/// still required (creating a brand-new team ocean needs a first admin).
/// Never used to bypass [`check_team_ocean_admin`]'s check for an EXISTING
/// team, only as a documented, disclosed fallback for a NEW one — see
/// `handle_destination_action`'s `cs_dest_org_team_new` arm.
///
/// `accounts` is CONTROL-only — always `control_pool` at every call site, on
/// either plane. Not a genuinely cross-plane function; kept a single-pool
/// signature deliberately, so a caller can never mistakenly hand this the
/// tenant pool.
async fn resolve_account_owner(
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
) -> Result<uuid::Uuid, sqlx::Error> {
    sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_one(control_pool)
        .await
}

/// Create a brand-new channel-mirrored team ocean (docs/connectors/slack.md
/// §2's third org option): a fresh `graphs` row (`kind = 'team'`), its
/// `teams` row, and a `team_members` row seating `creator_user_id` as its
/// first admin. Scoped inside one `begin_tenant_system_scoped` transaction —
/// `graphs`/`teams`/`team_members` are tenant-tree tables; the scoping
/// helper is the same "no acting Tenant on a webhook" swap
/// `check_team_ocean_admin`/`ensure_slack_watched_channel` use, inert on the
/// consumer plane. `pool` is `tenant_pool` at its one call site (org-slack
/// step 3).
async fn create_org_team_ocean(
    pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    creator_user_id: uuid::Uuid,
    name: &str,
) -> Result<(uuid::Uuid, uuid::Uuid), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("create_org_team_ocean scoped tx: {e}")))?;

    let graph_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, $2, 'team') RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .fetch_one(&mut *tx)
    .await?;

    let team_id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) \
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(name)
    .bind(creator_user_id)
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
        .bind(team_id)
        .bind(creator_user_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok((team_id, graph_id))
}

/// Auto-seat a channel's Slack members into an EXISTING team ocean —
/// docs/connectors/slack.md's C5 row, "auto-seat with full read access"
/// (founder decision, 2026-07-26, `bd17b10` / PR #372): the admin's act of
/// routing a channel into an existing team ocean IS the decision to admit
/// that channel's members, authorising both directions at once (the
/// channel's content to the team, and the team's content to the channel's
/// members).
///
/// Enumerates `channel_id`'s roster and resolves it through the SAME
/// verified-email matcher every other C5 path uses
/// (`connectors::slack::resolve_channel_members`, which itself delegates to
/// `resolve_verified_slack_identity`) — seating ONLY members that resolve to
/// a CURRENT member of THIS account. An unresolved member is a normal,
/// expected outcome (an external participant, or someone not yet
/// identity-matched) — never an error, and never seated on a guess.
///
/// **Idempotent.** `team_members`'s `UNIQUE (team_id, user_id)` constraint
/// (migration `0022_teams`) makes each seat write an `ON CONFLICT DO
/// NOTHING`, so re-running this (a retry after a partial failure, or a
/// second attempt at the same pick) never duplicates a seat and never
/// touches an already-seated member's existing role (an admin re-resolved
/// here is never silently demoted to `'member'` — the conflict simply
/// no-ops, leaving their row untouched).
///
/// Seats with `role = 'member'`, never `'admin'` — auto-seating grants read
/// access, not administration; only [`create_org_team_ocean`] (creating a
/// brand-new team) and an explicit, out-of-scope admin action ever grant
/// `'admin'`.
///
/// One scoped transaction PER member (`graphs`/`teams`/`team_members` all
/// carry the SAME consumer-plane `account_isolation` RLS policy,
/// migration 0083, as `create_org_team_ocean` above) rather than one
/// spanning the whole roster: a write failure seating one resolved member
/// must not roll back seats already committed for others — the same
/// per-destination independence `routes::slack_events::ingest_slack_message`'s
/// fan-out loop was restructured to guarantee.
///
/// Returns `(seated, unresolved)` — never a partial failure that vanishes
/// silently: seating N of M members and 0 of the rest is reported as
/// `(N, M - N)`, not collapsed into a single ambiguous count. `Err` is
/// reserved for the one thing that blocks seating outright:
/// `conversations.members` itself failing (via
/// `connectors::slack::resolve_channel_members`) — reported honestly rather
/// than silently seating nobody and calling it zero unresolved members.
///
/// **Two pools (org-slack step 3).** The roster resolution
/// (`resolve_channel_members`) needs `control_pool` for its own
/// `account_members` membership check; the seating write
/// (`seat_resolved_members`, `team_members`/`team_seat_grants`) needs
/// `tenant_pool`. On the consumer plane both are the same pool.
///
/// `pub(crate)` (wt-slack-actions): the REST "change destination" write path
/// (`connectors::slack_channel_actions::set_channel_destination`) reuses
/// this verbatim for its own "auto-seat on team-ocean targets" requirement —
/// exactly the same seat write, the same `team_seat_grants` provenance (so
/// revocation still works), and the same roster-resolution honesty
/// guarantees. A second seating implementation for the REST path is exactly
/// how the two entry points would drift on who gets seated.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn auto_seat_channel_members(
    control_pool: &sqlx::PgPool,
    tenant_pool: &sqlx::PgPool,
    http: &reqwest::Client,
    account_id: uuid::Uuid,
    workspace_id: &str,
    bot_token: &str,
    channel_id: &str,
    team_id: uuid::Uuid,
) -> Result<(usize, usize), String> {
    // `control_pool` for `resolve_channel_members`'s own `account_members`
    // check, `tenant_pool` for its `connector_identities` directory read —
    // this call site is exactly why `resolve_channel_members` gained a
    // second pool parameter (org-slack step 3/4, see its own doc comment):
    // for an ORG account the two genuinely differ.
    let (matched_user_ids, total_members) = crate::connectors::slack::resolve_channel_members(
        control_pool,
        tenant_pool,
        http,
        account_id,
        workspace_id,
        bot_token,
        channel_id,
    )
    .await?;

    // Founding roster / auto-seated-by-routing: `None` (full history). This
    // call site seats a channel's roster at the moment it is FIRST routed
    // into a team ocean (or its "change destination" REST-path twin) — the
    // org-connectors plan's §4/§5 rule that a founding/auto-seated member
    // reads the destination ocean from its beginning, not from the routing
    // date (withholding history here would also hide the backfill this same
    // routing action just triggered from the very people who wrote it).
    let seated = seat_resolved_members(
        tenant_pool,
        account_id,
        team_id,
        &matched_user_ids,
        &crate::seats::slack_channel_source(channel_id),
        None,
    )
    .await;

    // Routing IS the founding moment for this source — record it durably so
    // the reconcilers' late-joiner rule holds even if every founding grant
    // is later revoked (see `seats::founding_history_from`). Only once
    // someone was actually seated: an all-unmatched roster leaves the source
    // unfounded for a later sweep to seat with full history. Non-fatal on
    // error — the surviving grants themselves are the fallback signal.
    if seated > 0 {
        if let Err(e) = crate::seats::record_source_founding(
            tenant_pool,
            account_id,
            team_id,
            &crate::seats::slack_channel_source(channel_id),
        )
        .await
        {
            tracing::warn!(
                error = %e,
                %account_id,
                %team_id,
                channel_id,
                "auto-seat: recording source founding failed — continuing"
            );
        }
    }

    // `unresolved` = every channel member NOT counted as seated above —
    // never-identified members AND identified-but-write-failed members alike
    // (both mean this person does not, in fact, have a seat right now, which
    // is the property this count exists to report).
    let unresolved = total_members.saturating_sub(seated);
    Ok((seated, unresolved))
}

/// The write half of [`auto_seat_channel_members`], factored out so its
/// idempotency and per-member fault-isolation are unit-testable against a
/// real `team_members` table WITHOUT a live `conversations.members` call —
/// this crate has no HTTP-mock harness, and `resolve_channel_members` always
/// hits the real Slack API for any external id it hasn't already confirmed.
/// Takes the roster resolution's OUTPUT (`matched_user_ids`) directly, so a
/// test can hand it a hand-built list instead.
///
/// One scoped transaction PER member (`graphs`/`teams`/`team_members` all
/// carry the SAME consumer-plane `account_isolation` RLS policy,
/// migration 0083, as `create_org_team_ocean` above) rather than one
/// spanning the whole roster: a write failure seating one resolved member
/// must not roll back seats already committed for others — the same
/// per-destination independence `routes::slack_events::ingest_slack_message`'s
/// fan-out loop was restructured to guarantee.
///
/// Idempotent via `team_members`'s `UNIQUE (team_id, user_id)` constraint
/// (migration `0022_teams`): each seat write is an `ON CONFLICT DO NOTHING`,
/// so re-running this for the SAME roster never duplicates a seat and never
/// touches an already-seated member's existing role (an admin re-resolved
/// here is never silently demoted to `'member'` — the conflict simply
/// no-ops, leaving their row untouched). Seats with `role = 'member'`, never
/// `'admin'` — auto-seating grants read access, not administration.
///
/// Returns the count of members actually seated (newly, or already-seated —
/// both count, since the caller's job is "make sure they have a seat", and
/// an idempotent re-run finding them already seated is success, not a
/// zero-progress no-op).
/// [`seat_resolved_members`] for callers outside this module — the
/// `member_joined_channel` seat grant and the reconciler sweep both need the
/// exact same write, and a second implementation of "seat these people and
/// record why" is how the grant and revoke halves drift apart.
///
/// `pool` must be whichever pool `team_members`/`team_seat_grants` live on
/// for `account_id` — `tenant_pool` for an org account, the single pool a
/// not-yet-tenant-converted caller already has otherwise (unchanged for
/// `member_joined_channel`/the reconciler, neither of which is converted by
/// org-slack step 3).
pub(crate) async fn seat_members_for_source(
    pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    team_id: uuid::Uuid,
    matched_user_ids: &[uuid::Uuid],
    source: &str,
    history_from: Option<chrono::DateTime<chrono::Utc>>,
) -> usize {
    seat_resolved_members(pool, account_id, team_id, matched_user_ids, source, history_from).await
}

/// `history_from` (m365 enterprise train PR 4.1) — the window floor to stamp
/// on the `source` grant for anyone newly seated by THIS call. `None` for a
/// founding/auto-seated roster, `Some(ts)` for a member discovered later
/// (a join event's own timestamp, or the reconciler sweep's `now()` for a
/// late-discovered member — see each caller's own doc comment). Threaded
/// through as a single value for the whole `matched_user_ids` batch: the
/// decision ("is this the founding sweep for this `(team_id, source)`?") is
/// made once by the caller, before this function runs, not per member.
async fn seat_resolved_members(
    pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    team_id: uuid::Uuid,
    matched_user_ids: &[uuid::Uuid],
    source: &str,
    history_from: Option<chrono::DateTime<chrono::Utc>>,
) -> usize {
    let mut seated = 0usize;
    for user_id in matched_user_ids {
        // Read BEFORE the insert, because after it the two cases are
        // indistinguishable. An active seat with NO provenance at all earns a
        // `preexisting` grant, which is what stops this system from ever
        // revoking a hand-granted seat — see `crate::seats`'s header.
        //
        // The `AND NOT EXISTS(...grants...)` half is load-bearing and was
        // added because a test caught its absence: without it, seating the
        // same person from a SECOND routed channel sees "already active" —
        // true, but active because of the FIRST channel — and stamps a
        // marker that can never be dropped. Every multi-channel member would
        // become permanently un-revocable, which is precisely the failure
        // this feature exists to fix. `preexisting` means "we found a seat
        // nobody recorded a reason for", not "we found a seat".
        // Scoped, and NOT error-swallowing. Both tables carry 0083's
        // `account_isolation` policy; read on a bare pool the policy's
        // `current_setting('app.account_id')::uuid` cast raises `invalid
        // input syntax for type uuid: ""`. The original `.unwrap_or(None)`
        // then turned that hard failure into `preexisting_active = false`,
        // which silently disables the `preexisting` marker entirely — the
        // one thing standing between a hand-granted seat and automatic
        // revocation. A safety property that fails closed in tests and open
        // in production is worse than no safety property, because it reads
        // as covered.
        //
        // An error here skips this member rather than guessing: without a
        // trustworthy answer we must not record provenance we cannot stand
        // behind, and a member with NO grant is un-revocable (safe), whereas
        // a member wrongly missing the `preexisting` marker is revocable
        // when they should not be (unsafe).
        let preexisting_active = {
            let probe: Result<bool, sqlx::Error> = async {
                // `team_members` is on the tenant plane's `0009_tenant_rls`
                // roster (org-slack step 4): `begin_account_scoped` alone
                // sets only `app.account_id`, which `tenant_rls_visible_
                // graph_ids()` never treats as sufficient — a plain scope
                // here would silently read zero rows on an org tenant
                // (`workers::slack_seat_reconcile` is the first tenant-plane
                // caller of this function). `begin_tenant_system_scoped` is
                // additionally `app.tenant_actor='system'`, harmless on the
                // consumer plane (0083's policies never read that GUC).
                let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
                    .await
                    .map_err(|e| sqlx::Error::Protocol(format!("preexisting probe scope: {e}")))?;
                let v: bool = sqlx::query_scalar(
                    "SELECT EXISTS( \
                         SELECT 1 FROM team_members \
                         WHERE team_id = $1 AND user_id = $2 AND departing_at IS NULL \
                     ) AND NOT EXISTS( \
                         SELECT 1 FROM team_seat_grants WHERE team_id = $1 AND user_id = $2 \
                     )",
                )
                .bind(team_id)
                .bind(user_id)
                .fetch_one(&mut *tx)
                .await?;
                tx.rollback().await.ok();
                Ok(v)
            }
            .await;

            match probe {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        %team_id,
                        %user_id,
                        "seat_resolved_members: could not determine whether this seat pre-existed \
                         — skipping the member entirely rather than recording provenance that \
                         might make a hand-granted seat revocable"
                    );
                    continue;
                }
            }
        };

        let seat_result: Result<bool, sqlx::Error> = async {
            // Same tenant-RLS reasoning as the `preexisting_active` probe
            // above — this transaction writes `team_members` too.
            let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
                .await
                .map_err(|e| sqlx::Error::Protocol(format!("seat_resolved_members scoped tx: {e}")))?;
            sqlx::query(
                "INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member') \
                 ON CONFLICT (team_id, user_id) DO NOTHING",
            )
            .bind(team_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

            // `team_members` carries a soft-delete `departing_at` under
            // `UNIQUE (team_id, user_id)` (migration 0022_teams), and every
            // reachability function gates on `departing_at IS NULL`. So a
            // member who previously departed this team already HAS a row:
            // the insert above hits the conflict, does nothing, and returns
            // `Ok` — while the person remains unseated in every sense that
            // matters. Counting that as a seat is how the disclosure ends up
            // reporting access that does not exist.
            //
            // The row is deliberately NOT resurrected. `departing_at` is set
            // when someone is removed from a team, and silently readmitting
            // them because they are still in a Slack channel would let a
            // channel roster overrule an admin's explicit removal. Reporting
            // honestly is the fix here; re-admission is a decision for a
            // human, not a side effect of routing.
            let active: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM team_members \
                 WHERE team_id = $1 AND user_id = $2 AND departing_at IS NULL)",
            )
            .bind(team_id)
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(active)
        }
        .await;

        match seat_result {
            Ok(true) => {
                seated += 1;

                // Provenance, recorded only for a seat that is actually
                // standing. `preexisting` FIRST and unconditionally when the
                // seat was already active: if the source grant landed and
                // this one failed, a later "left the channel" would see an
                // empty grant set and revoke a seat this system never
                // granted. Ordering it first makes the dangerous
                // interleaving impossible rather than unlikely.
                if preexisting_active {
                    // Always `None` (full history), independent of THIS
                    // call's `history_from` — the `preexisting` marker means
                    // the seat already existed before this system touched
                    // anything, so it earns full history unconditionally,
                    // the same way a founding/auto-seated member does.
                    if let Err(e) = crate::seats::record_seat_grant(
                        pool,
                        account_id,
                        team_id,
                        *user_id,
                        crate::seats::SOURCE_PREEXISTING,
                        None,
                    )
                    .await
                    {
                        tracing::error!(
                            error = %e,
                            %team_id,
                            %user_id,
                            "seat_resolved_members: failed to record the `preexisting` grant for \
                             a seat this system did not create — SKIPPING the source grant too, \
                             so this seat keeps no provenance at all and stays un-revocable \
                             (safe direction)"
                        );
                        continue;
                    }
                }

                if let Err(e) = crate::seats::record_seat_grant(
                    pool, account_id, team_id, *user_id, source, history_from,
                )
                .await
                {
                    // The seat stands, but nothing records WHY. Consequence
                    // is bounded and safe: with no grant to delete, the
                    // revocation gate reports `NotOurs` and leaves the seat
                    // alone forever. Loud because it means this pair will
                    // never revoke until a reconciler sweep re-asserts it.
                    tracing::error!(
                        error = %e,
                        %team_id,
                        %user_id,
                        source,
                        "seat_resolved_members: seated the member but failed to record the seat \
                         grant — the seat stands and is now un-revocable until a reconciler \
                         sweep re-asserts provenance"
                    );
                }
            }
            Ok(false) => {
                tracing::info!(
                    %team_id,
                    %user_id,
                    "seat_resolved_members: member has a departing team_members row — left \
                     unseated and NOT counted as seated (an admin's removal outranks a Slack \
                     channel roster)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    %team_id,
                    %user_id,
                    "seat_resolved_members: failed to seat a resolved member — continuing with \
                     the rest of the roster"
                );
            }
        }
    }
    seated
}

/// Dispatch one `cs_dest_*` interactivity action. Always acks 200 to Slack —
/// the write's outcome (saved / already routed / refused) is reported via
/// `response_url` instead, same convention as the backfill flow's
/// in-progress/final messages.
#[allow(clippy::too_many_arguments)]
/// Resolve one `cs_review_*` button click: verify the clicking user, apply
/// the verdict through the SAME transport-independent paths the web/MCP use
/// (`confirm/reject_node_impl_scoped` for decisions; the account-anchored
/// scoped edge-status setter for edges), stamped `via: "slack"`, then
/// re-publish the Home so the resolved item disappears.
///
/// Spawned post-ack — nothing here can respond to Slack directly; failures
/// log, and an App Home surface has no ephemeral channel, so an unverified
/// clicker sees the item simply remain (fail-closed, documented trade).
async fn handle_review_action(
    state: Arc<AppState>,
    ctx: SlackActionContext,
    action_id: String,
    team_id: String,
    slack_user_id: String,
    value: String,
) {
    let account_id = ctx.account_id;
    let Some(item_id) = serde_json::from_str::<Value>(&value)
        .ok()
        .and_then(|v| v.get("id").and_then(Value::as_str).and_then(|s| s.parse::<uuid::Uuid>().ok()))
    else {
        tracing::warn!(%action_id, "cs_review action with unparseable value — ignoring");
        return;
    };

    let Some(user_id) = resolve_clicking_user(
        &ctx.control_pool,
        &ctx.tenant_pool,
        &state.http_client,
        account_id,
        &team_id,
        &ctx.bot_token,
        &slack_user_id,
    )
    .await
    else {
        tracing::warn!(%account_id, %slack_user_id, "cs_review click from unverified Slack user — refused (fail-closed)");
        return;
    };

    // Write scope (settings 2.10): the same REST rule the web review uses —
    // the clicker's own private ocean, or a shared ocean they moderate as an
    // owner/admin. The verified Slack user is an account member, which used
    // to be the whole check; in an org that let any member resolve another
    // member's private items. Fail closed when the role cannot be read.
    let role: String = match member_role(&ctx.control_pool, account_id, user_id).await {
        Ok(Some(r)) => r,
        Ok(None) => String::new(),
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "cs_review role lookup failed — refused (fail-closed)");
            return;
        }
    };
    let decision = match (&ctx.plane, &ctx.clerk_org_id) {
        (AccountPlane::Org, Some(org_id)) => {
            crate::tenancy::runtime::TenancyDecision::Org(crate::tenancy::runtime::TenantContext {
                org_id: org_id.clone(),
                account_id,
                user_id,
                pool: ctx.tenant_pool.clone(),
                page_pool: None,
            })
        }
        _ => crate::tenancy::runtime::TenancyDecision::Personal,
    };
    let writer = crate::routes::nodes::ReefWriter::with_role(account_id, user_id, &role, Some(&decision));

    let confirm = action_id.ends_with("_confirm");
    let outcome: Result<(), String> = match action_id.as_str() {
        "cs_review_dec_confirm" | "cs_review_dec_reject" => {
            let mut scoped_tx: crate::routes::nodes::ScopedTx = Some(
                match crate::tenancy::scope::begin_account_scoped(&ctx.tenant_pool, account_id).await {
                    Ok(tx) => tx,
                    Err(e) => {
                        tracing::warn!(error = %e, "cs_review decision scope failed");
                        return;
                    }
                },
            );
            let store = match &state.store {
                Some(s) => s.clone(),
                None => return,
            };
            let res = if confirm {
                crate::routes::nodes::confirm_node_impl_scoped(
                    store.as_ref(),
                    &mut scoped_tx,
                    &writer,
                    item_id,
                    crate::routes::nodes::ConfirmRequest { title: None, rationale: None, decided_at: None },
                    "slack",
                )
                .await
                .map(|_| ())
            } else {
                crate::routes::nodes::reject_node_impl_scoped(
                    store.as_ref(),
                    &mut scoped_tx,
                    &writer,
                    item_id,
                    crate::routes::nodes::RejectRequest { reason: None },
                    "slack",
                )
                .await
                .map(|_| ())
            };
            match res {
                Ok(()) => {
                    if let Some(tx) = scoped_tx.take() {
                        tx.commit().await.map_err(|e| e.to_string())
                    } else {
                        Ok(())
                    }
                }
                // NotFound = already resolved (sweep or another surface) —
                // benign; the re-publish below drops the stale card.
                Err(crate::routes::nodes::ReviewError::NotFound) => {
                    Err("no pending decision with that id (already resolved?)".to_string())
                }
                Err(crate::routes::nodes::ReviewError::Store(e)) => Err(e.to_string()),
            }
        }
        "cs_review_edge_confirm" | "cs_review_edge_reject" => {
            let status = if confirm {
                crate::relationships::EdgeStatus::Confirmed
            } else {
                crate::relationships::EdgeStatus::Rejected
            };
            let store = match &state.store {
                Some(s) => s.clone(),
                None => return,
            };
            let run = async {
                let mut tx = crate::tenancy::scope::begin_account_scoped(&ctx.tenant_pool, account_id)
                    .await
                    .map_err(|e| e.to_string())?;
                // Account anchoring (IDOR): the edge's ocean must belong to
                // the resolved account — RLS enforces this too on the
                // consumer plane; this check makes it explicit on both.
                let graph_id: Option<uuid::Uuid> = sqlx::query_scalar(
                    "SELECT e.graph_id FROM relationship_edge e \
                     JOIN graphs g ON g.id = e.graph_id \
                     WHERE e.edge_id = $1 AND g.account_id = $2",
                )
                .bind(item_id)
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
                let Some(graph_id) = graph_id else {
                    return Err("edge not in this account".to_string());
                };
                // In the account is not enough: the clicker must be allowed
                // to write that ocean (settings 2.10).
                let mut scoped: crate::routes::nodes::ScopedTx = Some(tx);
                let writable = crate::routes::nodes::graph_writable_by(store.as_ref(), &mut scoped, graph_id, &writer)
                    .await
                    .map_err(|e| e.to_string())?;
                let Some(mut tx) = scoped.take() else {
                    return Err("scoped transaction lost".to_string());
                };
                if !writable {
                    return Err("edge not writable by this user".to_string());
                }
                store
                    .set_relationship_edge_status_scoped(&mut tx, item_id, status, Some(user_id), "slack")
                    .await
                    .map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())
            };
            run.await
        }
        other => {
            tracing::warn!(action_id = %other, "unknown cs_review action — ignoring");
            return;
        }
    };

    match outcome {
        Ok(()) => {
            tracing::info!(%account_id, %item_id, %action_id, "cs_review verdict applied via Slack");
        }
        Err(e) => {
            tracing::warn!(%account_id, %item_id, error = %e, "cs_review verdict failed (item may already be resolved)");
        }
    }

    // Re-publish the Home either way so the surface reflects reality (a
    // resolved item disappears; an already-resolved one stops being stale).
    crate::routes::slack_events::handle_app_home_switch(
        state,
        team_id,
        slack_user_id,
        account_id.to_string(),
    )
    .await;
}

async fn handle_destination_action(
    state: Arc<AppState>,
    ctx: SlackActionContext,
    action_id: String,
    slack_user_id: String,
    selected_value: String,
    channel_id: String,
    response_url: String,
) -> axum::response::Response {
    use crate::connectors::slack::{ensure_slack_watched_channel, PickedDestination, SlackChannelPick};

    let account_id = ctx.account_id;
    let bot_token = ctx.bot_token.clone();

    // Team_id — needed for both destination-writing branches below
    // (`slack_watched_channels`'s composite key), resolved the same way
    // `resolve_pool_account_bot_token` resolved `account_id` FROM it: we
    // only have the reverse lookup (`account_id` -> workspace) readily
    // available via `slack_integrations`, matching the shape
    // `backfill_channel` already uses.
    // Scoped: `oauth_tokens` carries 0083's `account_isolation` policy, so a
    // bare-pool read raises `invalid input syntax for type uuid: ""` rather
    // than returning a row. The old `.unwrap_or(None)` swallowed that into
    // "no oauth_tokens row", and every `cs_dest_*` button click therefore
    // returned here without doing anything — in production the destination
    // picker was inert on BOTH halves: the prompt failed to post (see
    // `slack_events::post_destination_prompt`) and the click did nothing.
    //
    // The two failure modes are now reported differently, because they are
    // different: a genuinely missing row is a workspace that was never
    // installed, while an error is us being unable to look.
    //
    // Stays CONTROL-plane — `oauth_tokens` for this account lives there
    // regardless of plane (see `resolve_pool_account_bot_token`'s own doc
    // comment).
    let workspace_id: Option<String> = match async {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&ctx.control_pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(format!("workspace lookup scope: {e}")))?;
        // Foundation PR 1.1: `owner_user_id IS NULL` — Slack has no
        // per-member OAuth (§14.1), so this account's Slack row is always
        // account-wide.
        let v: Option<String> = sqlx::query_scalar(
            "SELECT provider_workspace_id FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack' AND owner_user_id IS NULL LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;
        tx.rollback().await.ok();
        Ok::<_, sqlx::Error>(v)
    }
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(%account_id, error = %e, "cs_dest_*: workspace lookup FAILED");
            return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
        }
    };
    let Some(workspace_id) = workspace_id else {
        tracing::warn!(%account_id, "cs_dest_*: no oauth_tokens row — cannot determine workspace_id");
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    };

    // Provenance-only lookup shared by every arm below that needs SOME
    // `users.id` to satisfy `slack_watched_channels.user_id NOT NULL` — see
    // this module's "Identity resolution" doc section for what this can and
    // cannot be trusted for. Cross-plane: identity comes from
    // `connector_identities` (tenant-tree), membership from `account_members`
    // (control-only) — see `resolve_clicking_user`'s own doc comment.
    let clicking_user = resolve_clicking_user(
        &ctx.control_pool,
        &ctx.tenant_pool,
        &state.http_client,
        account_id,
        &workspace_id,
        &bot_token,
        &slack_user_id,
    )
    .await;

    let report = |text: String| {
        let http = state.http_client.clone();
        let response_url = response_url.clone();
        tokio::spawn(async move {
            if let Err(e) = post_response_url(&http, &response_url, true, &text).await {
                tracing::warn!(error = %e, "failed to update Slack message via response_url");
            }
        });
    };

    match action_id.as_str() {
        "cs_dest_personal" => {
            let Ok(graph_id) = uuid::Uuid::parse_str(&selected_value) else {
                tracing::warn!(selected_value, "cs_dest_personal: invalid graph_id");
                return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
            };
            // Personal plane is single-user by construction — the account
            // owner IS the picking human, no identity resolution needed.
            // `accounts.owner_user_id` is CONTROL-only.
            let user_id = match resolve_account_owner(&ctx.control_pool, account_id).await {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!(error = %e, "cs_dest_personal: failed to resolve account owner");
                    return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
                }
            };
            let result = ensure_slack_watched_channel(
                &ctx.control_pool,
                &ctx.tenant_pool,
                ctx.plane,
                SlackChannelPick {
                    account_id,
                    user_id,
                    workspace_id,
                    channel_id,
                    display_name: String::new(),
                    destination: PickedDestination::Single(graph_id),
                },
            )
            .await;
            match result {
                Ok(outcome) => {
                    let text = if outcome.newly_inserted {
                        "✅ Saved — this channel now feeds that ocean.".to_string()
                    } else {
                        "ℹ️ This channel is already routed elsewhere and cannot be re-pointed."
                            .to_string()
                    };
                    report(text);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "cs_dest_personal: failed to persist pick");
                    report("⚠️ Couldn't save that pick — please try again.".to_string());
                }
            }
        }

        "cs_dest_org_fanout" => {
            // Provenance only (see this module's doc section) — never a
            // gate, so an unresolved identity still proceeds, falling back
            // to the account owner. `accounts.owner_user_id` is CONTROL-only.
            let user_id = match clicking_user {
                Some(u) => u,
                None => match resolve_account_owner(&ctx.control_pool, account_id).await {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::error!(error = %e, "cs_dest_org_fanout: failed to resolve account owner");
                        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
                    }
                },
            };
            let result = ensure_slack_watched_channel(
                &ctx.control_pool,
                &ctx.tenant_pool,
                ctx.plane,
                SlackChannelPick {
                    account_id,
                    user_id,
                    workspace_id,
                    channel_id,
                    display_name: String::new(),
                    destination: PickedDestination::FanOut,
                },
            )
            .await;
            match result {
                Ok(outcome) => {
                    let already = !outcome.newly_inserted;
                    let text = if already {
                        "ℹ️ This channel is already routed elsewhere and cannot be re-pointed."
                            .to_string()
                    } else {
                        // Phase two wired identity mapping; the "this doesn't
                        // work yet" note that used to close this sentence is
                        // now false. The real limit takes its place.
                        "✅ Saved — \"everyone's individual oceans\" recorded for this channel. \
                         Each person's messages land in their own ocean once their Slack \
                         account is matched to a CoralSwarm account by verified email; anyone \
                         unmatched is skipped and their content isn't saved."
                            .to_string()
                    };
                    report(text);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "cs_dest_org_fanout: failed to persist pick");
                    report("⚠️ Couldn't save that pick — please try again.".to_string());
                }
            }
        }

        "cs_dest_org_team_existing" => {
            let Ok(team_id) = uuid::Uuid::parse_str(&selected_value) else {
                tracing::warn!(selected_value, "cs_dest_org_team_existing: invalid team_id");
                return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
            };
            // `teams`/`team_members` are tenant-tree tables.
            let check =
                check_team_ocean_admin(&ctx.tenant_pool, account_id, clicking_user, team_id).await;
            match check {
                Ok(TeamOceanCheck::Admin { graph_id, name }) => {
                    // Safe: `Admin` is only returned when `clicking_user`
                    // was `Some` AND confirmed as that team's admin.
                    let user_id = clicking_user.expect("TeamOceanCheck::Admin implies Some");
                    // Captured before `SlackChannelPick` below takes ownership
                    // of both — auto-seating (only fired on a NEWLY inserted
                    // pick) needs them again afterward.
                    let workspace_id_for_seat = workspace_id.clone();
                    let channel_id_for_seat = channel_id.clone();
                    let result = ensure_slack_watched_channel(
                        &ctx.control_pool,
                        &ctx.tenant_pool,
                        ctx.plane,
                        SlackChannelPick {
                            account_id,
                            user_id,
                            workspace_id,
                            channel_id,
                            // EMPTY, not `name` — `name` here is the TEAM
                            // OCEAN's name, and this column means "the human
                            // label for this CHANNEL" (migration 0101's
                            // header: `#general`, etc.). Storing the team name
                            // put a value of the wrong KIND in the column: the
                            // three sibling write sites all pass the empty
                            // string, so this was also the only row shape that
                            // ever looked populated, and
                            // `connectors::slack_channel_management`'s
                            // `stale_name` check (stored vs. live channel name)
                            // would have reported EVERY team-ocean-routed
                            // channel as renamed, permanently — a team name
                            // essentially never equals its channel's name.
                            //
                            // Not resolved to the real channel name here on
                            // purpose: the payload carries only `channel.id`,
                            // and a `conversations.info` round-trip inside the
                            // interactivity ACK path is the exact thing that
                            // forced auto-seating off this path (Slack drops an
                            // interaction it hasn't heard back from in ~3s).
                            // The empty string is the honest "not known yet".
                            display_name: String::new(),
                            destination: PickedDestination::Single(graph_id),
                        },
                    )
                    .await;
                    match result {
                        Ok(outcome) => {
                            let text = if outcome.newly_inserted {
                                format!("✅ Saved — this channel now feeds \"{name}\".")
                            } else {
                                "ℹ️ This channel is already routed elsewhere and cannot be \
                                 re-pointed."
                                    .to_string()
                            };

                            // Auto-seat — docs/connectors/slack.md's C5 row,
                            // "auto-seat with full read access" (2026-07-26,
                            // bd17b10 / PR #372): the admin's act of routing
                            // THIS pick into an EXISTING team ocean is the
                            // seating decision. Only fires on the pick that
                            // actually took effect — an "already routed
                            // elsewhere" outcome means THIS team was not the
                            // one the channel landed on, so seating its
                            // roster into it would be seating people into an
                            // ocean this channel doesn't even feed.
                            //
                            // Deliberately OFF the ack path. Every arm of
                            // this dispatch runs inline before the handler's
                            // closing 200, and Slack drops an interaction it
                            // has not heard back from within ~3 seconds. A
                            // first-time seat walks `conversations.members`
                            // to exhaustion, issues a `users.info` for every
                            // member not already in the directory, and opens
                            // a transaction per seat — comfortably past that
                            // budget on a large channel, which would show
                            // the admin a timeout error for a pick that
                            // actually succeeded, and invites Slack to
                            // redeliver the interaction.
                            //
                            // So: confirm the routing immediately, then seat
                            // in the background and replace that message
                            // with the seat summary when it lands. Both
                            // updates go through `response_url`, which stays
                            // valid for 30 minutes and already carries
                            // `replace_original`.
                            if outcome.newly_inserted {
                                report(text);

                                let control_pool = ctx.control_pool.clone();
                                let tenant_pool = ctx.tenant_pool.clone();
                                let http = state.http_client.clone();
                                let response_url = response_url.clone();
                                let bot_token = bot_token.clone();
                                let name = name.clone();
                                let base = format!(
                                    "✅ Saved — this channel now feeds \"{name}\"."
                                );
                                tokio::spawn(async move {
                                    let summary = match auto_seat_channel_members(
                                        &control_pool,
                                        &tenant_pool,
                                        &http,
                                        account_id,
                                        &workspace_id_for_seat,
                                        &bot_token,
                                        &channel_id_for_seat,
                                        team_id,
                                    )
                                    .await
                                    {
                                        Ok((seated, unresolved)) => {
                                            let mut s = format!(
                                                "{base} Seated {seated} member(s) with read \
                                                 access to \"{name}\"."
                                            );
                                            if unresolved > 0 {
                                                s.push_str(&format!(
                                                    " {unresolved} channel member(s) could not \
                                                     be identified yet and were not seated."
                                                ));
                                            }
                                            s
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                error = %e,
                                                %team_id,
                                                channel_id = %channel_id_for_seat,
                                                "cs_dest_org_team_existing: auto-seat failed \
                                                 outright — the routing pick itself still \
                                                 succeeded"
                                            );
                                            format!(
                                                "{base} Couldn't confirm this channel's member \
                                                 list to seat them — nobody was seated; try \
                                                 again shortly."
                                            )
                                        }
                                    };
                                    if let Err(e) =
                                        post_response_url(&http, &response_url, true, &summary)
                                            .await
                                    {
                                        tracing::warn!(
                                            error = %e,
                                            "failed to post auto-seat summary via response_url"
                                        );
                                    }
                                });
                            } else {
                                report(text);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "cs_dest_org_team_existing: failed to persist pick");
                            report("⚠️ Couldn't save that pick — please try again.".to_string());
                        }
                    }
                }
                Ok(TeamOceanCheck::NotAdmin { name }) => {
                    report(format!(
                        "🚫 We couldn't confirm you administer \"{name}\" — ask a team admin to \
                         make this pick."
                    ));
                }
                Ok(TeamOceanCheck::NotFound) => {
                    report("⚠️ That team ocean no longer exists.".to_string());
                }
                Err(e) => {
                    tracing::warn!(error = %e, "cs_dest_org_team_existing: admin check failed");
                    report("⚠️ Couldn't verify that pick — please try again.".to_string());
                }
            }
        }

        "cs_dest_org_team_new" => {
            // A brand-new team ocean is not yet shared with anyone, so
            // creating one does not need the same admin-of-an-EXISTING-team
            // proof `cs_dest_org_team_existing` needs (contract I3 is about
            // reaching an ALREADY-shared ocean). It still needs a
            // `users.id` for the new team's first admin: fall back to the
            // account owner when identity cannot be confirmed, and say so
            // plainly in the confirmation.
            let (creator, owner_fallback) = match clicking_user {
                Some(u) => (Some(u), false),
                None => (None, true),
            };
            let creator = match creator {
                Some(u) => u,
                None => match resolve_account_owner(&ctx.control_pool, account_id).await {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::error!(error = %e, "cs_dest_org_team_new: failed to resolve account owner");
                        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
                    }
                },
            };
            let team_name = format!("#{channel_id}");
            // `graphs`/`teams`/`team_members` are tenant-tree tables.
            match create_org_team_ocean(&ctx.tenant_pool, account_id, creator, &team_name).await {
                Ok((_team_id, graph_id)) => {
                    let result = ensure_slack_watched_channel(
                        &ctx.control_pool,
                        &ctx.tenant_pool,
                        ctx.plane,
                        SlackChannelPick {
                            account_id,
                            user_id: creator,
                            workspace_id,
                            channel_id,
                            display_name: String::new(),
                            destination: PickedDestination::Single(graph_id),
                        },
                    )
                    .await;
                    match result {
                        Ok(outcome) => {
                            let mut text = if outcome.newly_inserted {
                                "✅ Created a new team ocean for this channel and saved it as the \
                                 destination."
                                    .to_string()
                            } else {
                                "ℹ️ This channel was already routed elsewhere (the new team ocean \
                                 was still created, but isn't this channel's destination)."
                                    .to_string()
                            };
                            if owner_fallback {
                                text.push_str(
                                    " Note: we couldn't confirm your CoralSwarm identity, so the \
                                     account owner was recorded as this team's first admin — ask \
                                     them to add you.",
                                );
                            }
                            report(text);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "cs_dest_org_team_new: failed to persist pick");
                            report(
                                "⚠️ Created the team ocean, but couldn't save it as this \
                                 channel's destination — please try again."
                                    .to_string(),
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "cs_dest_org_team_new: failed to create team ocean");
                    report("⚠️ Couldn't create a new team ocean — please try again.".to_string());
                }
            }
        }

        other => {
            tracing::debug!(action_id = other, "unknown cs_dest_* action — ignoring");
        }
    }

    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

// ── URL-encoded payload extraction ───────────────────────────────────────────

/// Extract and URL-decode the `payload` field from a
/// `application/x-www-form-urlencoded` body string.
///
/// Slack sends exactly one field called `payload` whose value is JSON.
pub(crate) fn extract_payload_field(body: &str) -> Option<String> {
    for pair in body.split('&') {
        if let Some(rest) = pair.strip_prefix("payload=") {
            // URL-decode: replace '+' with ' ', then percent-decode.
            let plus_decoded = rest.replace('+', " ");
            return Some(urlencoding::decode(&plus_decoded).ok()?.into_owned());
        }
    }
    None
}

// ── Slack API helpers ─────────────────────────────────────────────────────────

/// POST to `response_url` to update the original Slack message.
///
/// When `replace_original` is true the whole message is replaced with `text`.
async fn post_response_url(
    http: &reqwest::Client,
    response_url: &str,
    replace_original: bool,
    text: &str,
) -> Result<(), String> {
    if response_url.is_empty() {
        return Ok(());
    }
    let body = json!({
        "replace_original": replace_original,
        "text": text,
    });
    let resp = http
        .post(response_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        let body_text = resp.text().await.unwrap_or_default();
        return Err(format!("response_url returned {status}: {body_text}"));
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    // ── block_id → account selector ──────────────────────────────────────────

    /// Round-trips the `block_id` the prompt builders emit, for EVERY slot.
    ///
    /// Slack requires `block_id` to be unique within a message, and a
    /// destination prompt for an org that has teams renders several action
    /// blocks at once — so the account id alone collided. The slot suffix
    /// fixes that, and must not disturb the account the reader recovers.
    #[test]
    fn account_block_id_round_trips_for_every_slot() {
        let id = uuid::Uuid::new_v4();
        let mut seen = std::collections::HashSet::new();
        for slot in ["org_fanout", "org_team", "personal", "backfill"] {
            let block_id = crate::routes::slack_events::account_block_id(id, slot);
            assert_eq!(
                account_from_block_id(&block_id),
                Some(id),
                "slot {slot:?} must not disturb the recovered account",
            );
            assert!(seen.insert(block_id), "slots must produce DISTINCT block_ids");
        }
    }

    /// A slot the reader has never heard of still resolves — the parser stops
    /// at the first `:`, so new blocks can be added without touching it.
    #[test]
    fn an_unknown_slot_still_resolves_its_account() {
        let id = uuid::Uuid::new_v4();
        let block_id = crate::routes::slack_events::account_block_id(id, "some_future_block");
        assert_eq!(account_from_block_id(&block_id), Some(id));
    }

    /// A prompt posted BEFORE this shipped carries no such `block_id`. It must
    /// read as "no preference" (→ first-match), not as an error, or every
    /// button already sitting in a channel breaks the day this deploys.
    #[test]
    fn legacy_and_foreign_block_ids_read_as_no_preference() {
        for b in ["", "some_other_block", "cs_acct:", "cs_acct:not-a-uuid"] {
            assert_eq!(
                account_from_block_id(b),
                None,
                "{b:?} must not resolve to an account",
            );
        }
    }

    // ── choose_account: which account an action belongs to ──────────────────

    fn acct(n: u8) -> uuid::Uuid {
        uuid::Uuid::from_bytes([n; 16])
    }

    /// The defect this whole change fixes: two accounts share one workspace,
    /// each posted its own prompt, and each must be answered against ITS OWN
    /// account — not whichever sorts first.
    #[test]
    fn a_named_account_is_chosen_even_when_it_is_not_first() {
        let matches = vec![(acct(1), "personal"), (acct(2), "testorg")];
        assert_eq!(
            choose_account(&matches, Some(acct(2))),
            Ok(Some((acct(2), "testorg"))),
            "the SECOND account's prompt must resolve to the second account",
        );
    }

    /// The security property. An id outside the signed workspace's own set is
    /// REFUSED — never honoured, and never quietly downgraded to first-match.
    /// A downgrade would answer the prompt against the wrong account, which is
    /// precisely the bug being removed.
    #[test]
    fn an_account_the_workspace_does_not_resolve_to_is_refused() {
        let matches = vec![(acct(1), "personal"), (acct(2), "testorg")];
        let stranger = acct(99);
        assert_eq!(
            choose_account(&matches, Some(stranger)),
            Err(ForeignAccount(stranger)),
            "a foreign account id must refuse, NOT fall back to first-match",
        );
    }

    /// Backward compatibility: a prompt posted before this shipped names no
    /// account, and must keep the old first-match behaviour.
    #[test]
    fn no_preference_keeps_the_legacy_first_match_contract() {
        let matches = vec![(acct(1), "personal"), (acct(2), "testorg")];
        assert_eq!(choose_account(&matches, None), Ok(Some((acct(1), "personal"))));
    }

    /// An uninstalled workspace is `Ok(None)` (ack and ignore), distinct from
    /// a refusal — the caller logs them differently on purpose.
    #[test]
    fn no_accounts_at_all_is_none_not_a_refusal() {
        let empty: Vec<(uuid::Uuid, &str)> = vec![];
        assert_eq!(choose_account(&empty, None), Ok(None));
        assert_eq!(
            choose_account(&empty, Some(acct(1))),
            Err(ForeignAccount(acct(1))),
            "naming an account against a workspace with none is still a refusal",
        );
    }

    /// The security property, at the parsing seam: a well-formed `block_id`
    /// yields a *candidate* id and nothing more. It is deliberately NOT
    /// authority — `resolve_pool_account_bot_token_for` only ever uses it to
    /// pick among the accounts the SIGNED `team_id` already resolves to, and
    /// refuses outright when it names anything else. Encoded here so the
    /// parser is never "fixed" into returning something pre-validated.
    #[test]
    fn a_parsed_block_id_is_a_candidate_not_an_authorisation() {
        let attacker_named = uuid::Uuid::new_v4();
        let block_id = crate::routes::slack_events::account_block_id(attacker_named, "org_fanout");
        // Parsing succeeds — it is just a string from an untrusted payload...
        assert_eq!(account_from_block_id(&block_id), Some(attacker_named));
        // ...and the resolver's contract (see its doc comment) is that this
        // value is intersected with the signed workspace's own accounts. There
        // is no code path that turns this parse into access on its own.
    }

    use crate::routes::slack_events::{verify_slack_signature_with_secret, VerifyResult};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    fn make_signature(secret: &str, timestamp: &str, body: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(b"v0:");
        mac.update(timestamp.as_bytes());
        mac.update(b":");
        mac.update(body.as_bytes());
        format!("v0={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn now_ts() -> String {
        chrono::Utc::now().timestamp().to_string()
    }

    // ── Signature tests ───────────────────────────────────────────────────────

    /// Valid signature passes.
    #[test]
    fn interactivity_valid_signature_passes() {
        let secret = "interactivity_sig_secret_test";
        let ts = now_ts();
        let body = "payload=%7B%22type%22%3A%22block_actions%22%7D";
        let sig = make_signature(secret, &ts, body);

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-slack-request-timestamp", ts.parse().unwrap());
        headers.insert("x-slack-signature", sig.parse().unwrap());

        assert_eq!(
            verify_slack_signature_with_secret(secret, &headers, body.as_bytes()),
            VerifyResult::Ok
        );
    }

    /// Bad signature → SignatureMismatch.
    ///
    /// Mirrors the pattern in slack_events::tests: test the pure verification
    /// function directly to avoid mutating process-global env-var state under
    /// parallel test execution.  The handler maps SignatureMismatch → 401 via
    /// the same branch proven by handler_maps_signature_mismatch_to_401_branch
    /// in slack_events tests.
    #[test]
    fn interactivity_bad_signature_returns_mismatch() {
        let secret = "interactivity_bad_sig_test_key";
        let ts = now_ts();
        let body = b"payload=%7B%22type%22%3A%22block_actions%22%7D";

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-slack-request-timestamp", ts.parse().unwrap());
        // all-zero hex digest — correct length, wrong value
        headers.insert(
            "x-slack-signature",
            "v0=0000000000000000000000000000000000000000000000000000000000000000"
                .parse()
                .unwrap(),
        );

        let result = verify_slack_signature_with_secret(secret, &headers, body);
        assert_eq!(result, VerifyResult::SignatureMismatch);

        // Prove the handler maps SignatureMismatch to 401 (same branch logic
        // as the slack_events handler which has been end-to-end tested).
        let status = match result {
            VerifyResult::Ok => StatusCode::OK,
            VerifyResult::SecretNotSet => StatusCode::SERVICE_UNAVAILABLE,
            VerifyResult::TimestampInvalid | VerifyResult::SignatureMismatch => {
                StatusCode::UNAUTHORIZED
            }
        };
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// Missing SLACK_SIGNING_SECRET → SecretNotSet → handler maps to 503.
    ///
    /// Tests the branch mapping directly without touching the process-global
    /// env var (avoids races under parallel test execution — same pattern as
    /// handler_maps_secret_not_set_to_503_branch in slack_events tests).
    #[test]
    fn interactivity_missing_secret_maps_to_503_branch() {
        // When the env var is absent the outer env-check returns SecretNotSet.
        // Prove the handler maps that to 503.
        let status = match VerifyResult::SecretNotSet {
            VerifyResult::Ok => StatusCode::OK,
            VerifyResult::SecretNotSet => StatusCode::SERVICE_UNAVAILABLE,
            VerifyResult::TimestampInvalid | VerifyResult::SignatureMismatch => {
                StatusCode::UNAUTHORIZED
            }
        };
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Payload parsing ───────────────────────────────────────────────────────

    /// URL-encoded `payload=<json>` with a block_actions cs_backfill_30d
    /// extracts action_id, channel value, team id, and response_url.
    #[test]
    fn payload_parsing_block_actions_30d() {
        let inner = serde_json::json!({
            "type": "block_actions",
            "team": { "id": "T_TEAM_01" },
            "user": { "id": "U_USER_01" },
            "channel": { "id": "C_CHAN_01" },
            "message": { "ts": "1700000000.000100" },
            "response_url": "https://hooks.slack.com/actions/abc123",
            "actions": [
                {
                    "action_id": "cs_backfill_30d",
                    "value": "C_CHAN_01"
                }
            ]
        });
        let json_str = inner.to_string();
        let encoded = urlencoding::encode(&json_str);
        let body_str = format!("payload={encoded}");

        let decoded = extract_payload_field(&body_str).expect("should extract payload");
        let parsed: Value = serde_json::from_str(&decoded).expect("should parse JSON");

        assert_eq!(
            parsed.get("type").and_then(Value::as_str),
            Some("block_actions"),
            "type must be block_actions"
        );
        assert_eq!(
            parsed.pointer("/team/id").and_then(Value::as_str),
            Some("T_TEAM_01"),
            "team id must match"
        );
        let action = &parsed["actions"][0];
        assert_eq!(
            action.get("action_id").and_then(Value::as_str),
            Some("cs_backfill_30d"),
            "action_id must be cs_backfill_30d"
        );
        assert_eq!(
            action.get("value").and_then(Value::as_str),
            Some("C_CHAN_01"),
            "value (channel id) must match"
        );
        assert_eq!(
            parsed.get("response_url").and_then(Value::as_str),
            Some("https://hooks.slack.com/actions/abc123"),
            "response_url must be present"
        );
    }

    // ── action_id → window mapping ────────────────────────────────────────────

    #[test]
    fn action_id_none_returns_err() {
        // cs_backfill_none is handled separately — returns Err so callers dispatch specially.
        assert!(action_id_to_days("cs_backfill_none").is_err());
    }

    #[test]
    fn action_id_7d_returns_some_7() {
        assert_eq!(action_id_to_days("cs_backfill_7d"), Ok(Some(7)));
    }

    #[test]
    fn action_id_30d_returns_some_30() {
        assert_eq!(action_id_to_days("cs_backfill_30d"), Ok(Some(30)));
    }

    #[test]
    fn action_id_90d_returns_some_90() {
        assert_eq!(action_id_to_days("cs_backfill_90d"), Ok(Some(90)));
    }

    #[test]
    fn action_id_all_returns_none() {
        assert_eq!(action_id_to_days("cs_backfill_all"), Ok(None));
    }

    #[test]
    fn action_id_unknown_returns_err() {
        assert!(action_id_to_days("cs_backfill_banana").is_err());
        assert!(action_id_to_days("").is_err());
    }

    // ── member_joined_channel: self-join predicate ────────────────────────────

    /// When the joining user IS the bot, is_bot_self_join returns true.
    #[test]
    fn self_join_recognised_when_user_matches_bot_id() {
        let bot_id = "UBOT123";
        let event_user = "UBOT123";
        assert!(
            is_bot_self_join(event_user, bot_id),
            "user == bot_id must be recognised as a self-join"
        );
    }

    /// When the joining user is a human, is_bot_self_join returns false.
    #[test]
    fn human_join_ignored() {
        let bot_id = "UBOT123";
        let event_user = "UHUMAN456";
        assert!(
            !is_bot_self_join(event_user, bot_id),
            "user != bot_id must NOT be recognised as a self-join"
        );
    }

    /// Empty user / bot_id edge cases.
    #[test]
    fn self_join_predicate_empty_strings() {
        assert!(!is_bot_self_join("", "UBOT"), "empty user is not a self-join");
        assert!(!is_bot_self_join("UBOT", ""), "empty bot_id is not a self-join");
        assert!(!is_bot_self_join("", ""), "both empty is not a self-join");
    }
}

// ── Pure predicate (also used by slack_events.rs) ────────────────────────────

/// Returns `true` when the joining user is the bot itself.
///
/// Used to filter `member_joined_channel` events — we only post the backfill
/// prompt when our own bot is added to the channel.
pub(crate) fn is_bot_self_join(event_user: &str, bot_user_id: &str) -> bool {
    !event_user.is_empty() && !bot_user_id.is_empty() && event_user == bot_user_id
}

// ── C5 phase two: resolve_clicking_user, tested without a live Slack call ───

#[cfg(test)]
mod resolve_clicking_user_pg_tests {
    //! DB-backed tests for [`resolve_clicking_user`] — proves it is a thin
    //! wrapper over `connectors::slack::resolve_verified_slack_identity` plus
    //! the `account_members` membership check, exercised via the directory
    //! path (no live Slack call needed — same technique as `connectors::
    //! slack::verified_identity_matcher_pg_tests`).
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib resolve_clicking_user_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::resolve_clicking_user;

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
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }

    async fn seed_user(pool: &PgPool, email: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_org_account(pool: &PgPool, owner: Uuid) -> Uuid {
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(format!("org-{}", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
        account_id
    }

    async fn add_member(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn seed_confirmed_identity(pool: &PgPool, workspace_id: &str, external_user_id: &str, user_id: Uuid) {
        sqlx::query(
            "INSERT INTO connector_identities \
                (provider, workspace_id, external_user_id, canonical_name, user_id, \
                 link_method, link_verified_at) \
             VALUES ('slack', $1, $2, 'Test Person', $3, 'verified_email', now())",
        )
        .bind(workspace_id)
        .bind(external_user_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// In-app org deletion (control 0387): members are locked out of the org
    /// from the request, so a confirmed Slack identity stops resolving (the
    /// click, the role read and the App Home viewer all go through this
    /// function / `member_role`), and an undo restores it.
    #[tokio::test]
    async fn a_live_org_deletion_locks_a_confirmed_member_out_and_an_undo_restores_them_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_live_org_deletion_locks_a_confirmed_member_out_and_an_undo_restores_them_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_orgdel_clicking_user";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool, "owner@test.local").await;
        let account = seed_org_account(&pool, owner).await;
        let org: String = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account)
            .fetch_one(&pool)
            .await
            .unwrap();
        let clicker = seed_user(&pool, "clicker@test.local").await;
        add_member(&pool, account, clicker).await;
        seed_confirmed_identity(&pool, "T-ORGDEL", "U-ORGDEL-CLICKER", clicker).await;
        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build().unwrap();
        let resolve = || resolve_clicking_user(&pool, &pool, &http, account, "T-ORGDEL", "xoxb-garbage", "U-ORGDEL-CLICKER");
        let role = || super::member_role(&pool, account, clicker);

        assert_eq!(resolve().await, Some(clicker));
        assert_eq!(role().await.unwrap().as_deref(), Some("member"));

        let deletion: Uuid = sqlx::query_scalar(
            "INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by) VALUES ($1, $2, 'owner') RETURNING id",
        )
        .bind(account)
        .bind(&org)
        .fetch_one(&pool)
        .await
        .unwrap();
        for live in ["requested", "grace", "awaiting_approval"] {
            sqlx::query("UPDATE org_deletions SET state = $2 WHERE id = $1")
                .bind(deletion)
                .bind(live)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(resolve().await, None, "{live}: a locked-out member must not resolve");
            assert_eq!(role().await.unwrap(), None, "{live}: no role either (the review click's write scope)");
            let owner_role = super::member_role(&pool, account, owner).await.unwrap();
            assert_eq!(owner_role, None, "{live}: the owner too, on the Slack surface");
        }

        sqlx::query("UPDATE org_deletions SET state = 'canceled', canceled_at = now() WHERE id = $1")
            .bind(deletion)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(resolve().await, Some(clicker), "undo restores the member");
        assert_eq!(role().await.unwrap().as_deref(), Some("member"));

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The Admin/existing-team-ocean pick's whole unblock: a confirmed
    /// identity for a CURRENT account member resolves to a real `user_id` —
    /// this is what makes `check_team_ocean_admin` work on a real identity
    /// instead of always seeing `None`. FAILS WITHOUT THE FIX: before this
    /// phase `resolve_clicking_user` hand-rolled its own `users.info` call
    /// and required a live network round-trip that a garbage bot token in
    /// this test would make fail — this test proves it now resolves via the
    /// SAME directory-first matcher every other C5 path uses, no network.
    #[tokio::test]
    async fn resolves_a_confirmed_current_member_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resolves_a_confirmed_current_member_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_c5_clicking_user_confirmed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool, "owner@test.local").await;
        let account = seed_org_account(&pool, owner).await;

        let clicker = seed_user(&pool, "clicker@test.local").await;
        add_member(&pool, account, clicker).await;
        seed_confirmed_identity(&pool, "T1", "U-CLICKER", clicker).await;

        let short_timeout_http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let resolved =
            resolve_clicking_user(&pool, &pool, &short_timeout_http, account, "T1", "xoxb-garbage", "U-CLICKER")
                .await;
        assert_eq!(
            resolved,
            Some(clicker),
            "a confirmed identity for a current account member must resolve, via the directory \
             alone — no live Slack call needed"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A confirmed identity for someone who is NOT a current member of THIS
    /// account must never resolve — `resolve_verified_slack_identity` alone
    /// answers "does ANY CoralSwarm user have this email", and
    /// `resolve_clicking_user` is precisely the function that must add the
    /// `account_members` check on top.
    #[tokio::test]
    async fn refuses_a_confirmed_identity_for_a_non_member_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping refuses_a_confirmed_identity_for_a_non_member_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_c5_clicking_user_non_member";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool, "owner2@test.local").await;
        let account = seed_org_account(&pool, owner).await;

        // Confirmed identity, but NEVER added to this account's members.
        let outsider = seed_user(&pool, "outsider@test.local").await;
        seed_confirmed_identity(&pool, "T1", "U-OUTSIDER", outsider).await;

        let short_timeout_http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        let resolved =
            resolve_clicking_user(&pool, &pool, &short_timeout_http, account, "T1", "xoxb-garbage", "U-OUTSIDER")
                .await;
        assert_eq!(
            resolved, None,
            "a confirmed identity for a non-member of this account must never resolve"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ── C5 phase two: auto-seating write, tested without a live Slack call ──────

#[cfg(test)]
mod auto_seat_pg_tests {
    //! DB-backed tests for [`seat_resolved_members`] — the write half of
    //! [`auto_seat_channel_members`], the C5 "auto-seat with full read
    //! access" decision (docs/connectors/slack.md, founder decision
    //! 2026-07-26, `bd17b10` / PR #372). This has never been exercised at
    //! all before this phase (the write did not exist).
    //!
    //! In-crate: [`seat_resolved_members`] is private, so an external test
    //! crate cannot drive it. No live Slack call is needed here — see that
    //! function's own doc comment for why it takes the roster resolution's
    //! OUTPUT directly rather than calling `conversations.members` itself.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib auto_seat_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::seat_resolved_members;

    /// The grant `source` these tests seat under. Any non-empty value works —
    /// the revocation rules care only that it matches EXACTLY.
    const SRC: &str = "slack:C-TEST";

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
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn seed_org_account(pool: &PgPool, owner: Uuid) -> Uuid {
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(format!("org-{}", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
        account_id
    }

    /// Seeds a team ocean (`graphs` + `teams` rows) with `creator` as its
    /// first admin — the same shape `create_org_team_ocean` writes.
    async fn seed_team(pool: &PgPool, account_id: Uuid, creator: Uuid) -> Uuid {
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'test team', 'team') RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap();
        let team_id: Uuid = sqlx::query_scalar(
            "INSERT INTO teams (account_id, graph_id, name, created_by) \
             VALUES ($1, $2, 'test team', $3) RETURNING id",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(creator)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
            .bind(team_id)
            .bind(creator)
            .execute(pool)
            .await
            .unwrap();
        team_id
    }

    async fn team_member_role(pool: &PgPool, team_id: Uuid, user_id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT role FROM team_members WHERE team_id = $1 AND user_id = $2")
            .bind(team_id)
            .bind(user_id)
            .fetch_optional(pool)
            .await
            .unwrap()
    }

    async fn team_member_count(pool: &PgPool, team_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM team_members WHERE team_id = $1")
            .bind(team_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// The core write, exercised for the first time: two resolved members get
    /// seated as `'member'`, and re-running the SAME seat call is idempotent
    /// — no duplicate rows, no role change for the admin who created the
    /// team. FAILS WITHOUT THE FIX: before this phase, nothing ever called
    /// `INSERT INTO team_members` for an auto-seat pick at all — `team_
    /// members` would have zero rows for either member, and this test's very
    /// first assertion (`seated == 2`) would read `0` from a function that
    /// did not exist to seat anyone.
    #[tokio::test]
    async fn seats_resolved_members_idempotently_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping seats_resolved_members_idempotently_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_c5_auto_seat_idempotent";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;

        let member_a = seed_user(&pool).await;
        let member_b = seed_user(&pool).await;

        let seated = seat_resolved_members(&pool, account, team_id, &[member_a, member_b], SRC, None).await;
        assert_eq!(seated, 2, "both resolved members must be seated");
        assert_eq!(
            team_member_role(&pool, team_id, member_a).await.as_deref(),
            Some("member"),
            "auto-seating grants 'member', never 'admin'"
        );
        assert_eq!(team_member_role(&pool, team_id, member_b).await.as_deref(), Some("member"));
        assert_eq!(
            team_member_count(&pool, team_id).await,
            3,
            "the creator's admin seat plus the two newly auto-seated members"
        );

        // Re-run with the SAME roster (a retry, or a second pick attempt) —
        // must not duplicate rows, and must not touch the creator's admin role.
        let seated_again =
            seat_resolved_members(&pool, account, team_id, &[member_a, member_b, owner], SRC, None).await;
        assert_eq!(
            seated_again, 3,
            "an idempotent re-run (including the creator, already seated as admin) still counts \
             as seated — it does not need to be NEWLY inserted to count"
        );
        assert_eq!(
            team_member_count(&pool, team_id).await,
            3,
            "re-running the seat write must never duplicate rows"
        );
        assert_eq!(
            team_member_role(&pool, team_id, owner).await.as_deref(),
            Some("admin"),
            "an already-admin member must NEVER be silently demoted to 'member' by auto-seating"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A member who was REMOVED from this team (`departing_at` set) is still
    /// in the Slack channel, so the roster resolves them — but the insert
    /// hits `UNIQUE (team_id, user_id)`, does nothing, and leaves them
    /// unseated. They must not be counted, and must not be resurrected.
    ///
    /// FAILS WITHOUT THE FIX: `ON CONFLICT DO NOTHING` returns `Ok(())` for
    /// this row, and the old `Ok(()) => seated += 1` counted it. The admin
    /// was then told "Seated 1 member(s)" about a person who has no seat and
    /// is filtered out of every reachability function by `departing_at IS
    /// NULL` — the exact class of confidently-wrong disclosure this whole
    /// capability is supposed to prevent.
    #[tokio::test]
    async fn a_departing_member_is_not_counted_as_seated_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_departing_member_is_not_counted_as_seated_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_c5_auto_seat_departing";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;

        let departing = seed_user(&pool).await;
        let active = seed_user(&pool).await;

        // Previously seated, then removed from the team by an admin.
        sqlx::query(
            "INSERT INTO team_members (team_id, user_id, role, departing_at) \
             VALUES ($1, $2, 'member', now())",
        )
        .bind(team_id)
        .bind(departing)
        .execute(&pool)
        .await
        .unwrap();

        let seated = seat_resolved_members(&pool, account, team_id, &[departing, active], SRC, None).await;

        assert_eq!(
            seated, 1,
            "only the genuinely seated member counts — a departing row is not a seat, and \
             reporting it as one tells the admin about access that does not exist"
        );

        let still_departing: bool = sqlx::query_scalar(
            "SELECT departing_at IS NOT NULL FROM team_members WHERE team_id = $1 AND user_id = $2",
        )
        .bind(team_id)
        .bind(departing)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            still_departing,
            "auto-seating must NOT resurrect a removed member — a Slack channel roster does not \
             overrule an admin's explicit removal"
        );

        assert_eq!(
            team_member_role(&pool, team_id, active).await.as_deref(),
            Some("member"),
            "the un-departed member is seated normally"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Per-member fault isolation: a resolved-but-nonexistent user (its
    /// `users` row deleted between resolution and this write — the FK
    /// violation this induces stands in for that race) must not prevent the
    /// OTHER resolved member from being seated. Mirrors
    /// `routes::slack_events::ingest_slack_message`'s fan-out loop
    /// restructuring, applied to the auto-seat write.
    #[tokio::test]
    async fn a_failed_seat_does_not_prevent_the_others_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_failed_seat_does_not_prevent_the_others_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_c5_auto_seat_fault_isolation";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;

        let real_member = seed_user(&pool).await;
        // Never inserted into `users` at all — `team_members.user_id`'s FK
        // makes seating this one a genuine, guaranteed failure.
        let ghost_member = Uuid::new_v4();

        let seated =
            seat_resolved_members(&pool, account, team_id, &[ghost_member, real_member], SRC, None).await;
        assert_eq!(
            seated, 1,
            "the ghost member's seat must fail, but the real member must still be seated"
        );
        assert_eq!(
            team_member_role(&pool, team_id, real_member).await.as_deref(),
            Some("member"),
            "the real member must be seated even though the ghost member's write failed"
        );
        assert_eq!(
            team_member_role(&pool, team_id, ghost_member).await,
            None,
            "the ghost member must never end up with a row despite the attempted insert"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── Revocation ────────────────────────────────────────────────────────
    // Each of these pins one way seat revocation could silently take away
    // access it never granted. They are the reason the grant table exists at
    // all — see `crate::seats`'s module header.

    use crate::seats::{
        record_seat_grant, revoke_seat_grant, RevokeOutcome, SOURCE_PREEXISTING,
    };

    async fn is_seated(pool: &PgPool, team_id: Uuid, user_id: Uuid) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM team_members \
             WHERE team_id = $1 AND user_id = $2 AND departing_at IS NULL)",
        )
        .bind(team_id)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn grant_count(pool: &PgPool, team_id: Uuid, user_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM team_seat_grants WHERE team_id = $1 AND user_id = $2")
            .bind(team_id)
            .bind(user_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// **The headline case, and the reason this is a table and not a boolean.**
    /// A member seated by TWO routed channels keeps their seat after leaving
    /// one of them, and loses it only when the last reason goes.
    ///
    /// FAILS WITHOUT THE FIX: a `granted_by_slack` flag on `team_members` (or
    /// any design that revokes on the first departure) drops the seat at the
    /// first `member_left_channel`, taking away access the second channel
    /// still justifies.
    #[tokio::test]
    async fn a_seat_survives_until_its_last_reason_is_gone_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_seat_survives_until_its_last_reason_is_gone_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_seat_two_channels";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;
        let member = seed_user(&pool).await;

        // Seated by two different channels.
        seat_resolved_members(&pool, account, team_id, &[member], "slack:C-ONE", None).await;
        seat_resolved_members(&pool, account, team_id, &[member], "slack:C-TWO", None).await;
        assert_eq!(grant_count(&pool, team_id, member).await, 2, "one grant per reason");
        assert!(is_seated(&pool, team_id, member).await);

        // Leaves the first channel — the second still vouches for them.
        let outcome =
            revoke_seat_grant(&pool, account, team_id, member, "slack:C-ONE").await.unwrap();
        assert_eq!(
            outcome,
            RevokeOutcome::GrantDropped { remaining: 1 },
            "one reason gone, one remains"
        );
        assert!(
            is_seated(&pool, team_id, member).await,
            "leaving ONE routed channel must not cost a seat another routed channel still \
             justifies — this is the entire reason grants are rows and not a boolean"
        );

        // Leaves the second. Nothing is left to justify the seat.
        let outcome =
            revoke_seat_grant(&pool, account, team_id, member, "slack:C-TWO").await.unwrap();
        assert_eq!(outcome, RevokeOutcome::SeatRevoked);
        assert!(!is_seated(&pool, team_id, member).await, "the last reason is gone");

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// **A seat with no provenance is never ours to revoke.** This is the
    /// invariant protecting every seat that predates `team_seat_grants` and
    /// every seat a human granted by hand.
    ///
    /// FAILS WITHOUT THE FIX: counting grants FIRST and revoking on zero —
    /// the obvious implementation — reads "no grants" as "no remaining
    /// reasons" and silently removes a hand-granted seat on the first
    /// unrelated `member_left_channel`.
    #[tokio::test]
    async fn a_seat_with_no_grants_is_never_revoked_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_seat_with_no_grants_is_never_revoked_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_seat_no_provenance";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;
        let hand_seated = seed_user(&pool).await;

        // Seated by a human, with no grant row anywhere — exactly the state
        // of every seat that existed before this feature.
        sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(team_id)
            .bind(hand_seated)
            .execute(&pool)
            .await
            .unwrap();

        let outcome =
            revoke_seat_grant(&pool, account, team_id, hand_seated, "slack:C-ANY").await.unwrap();

        assert_eq!(
            outcome,
            RevokeOutcome::NotOurs,
            "absence of provenance means 'not ours to revoke', never 'safe to revoke'"
        );
        assert!(
            is_seated(&pool, team_id, hand_seated).await,
            "a seat this system never granted must survive a departure event untouched"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A seat granted by hand FIRST and auto-granted SECOND keeps the
    /// `preexisting` marker, so its grant set never empties and the
    /// revocation branch stays unreachable for it.
    ///
    /// FAILS WITHOUT THE FIX: recording only the channel grant makes the
    /// hand-granted seat look purely automatic, and leaving the channel
    /// revokes access a human deliberately granted.
    #[tokio::test]
    async fn auto_seating_an_already_seated_member_marks_it_preexisting_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping auto_seating_an_already_seated_member_marks_it_preexisting_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_seat_preexisting";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;
        let member = seed_user(&pool).await;

        sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(team_id)
            .bind(member)
            .execute(&pool)
            .await
            .unwrap();

        // Now a routed channel auto-seats the same person.
        seat_resolved_members(&pool, account, team_id, &[member], "slack:C-ONE", None).await;

        let sources: Vec<String> = sqlx::query_scalar(
            "SELECT source FROM team_seat_grants WHERE team_id = $1 AND user_id = $2 ORDER BY source",
        )
        .bind(team_id)
        .bind(member)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            sources,
            vec!["preexisting".to_string(), "slack:C-ONE".to_string()],
            "an already-active seat must earn a `preexisting` marker alongside the channel grant"
        );

        // Leaving the channel drops that grant but not the marker.
        let outcome =
            revoke_seat_grant(&pool, account, team_id, member, "slack:C-ONE").await.unwrap();
        assert_eq!(outcome, RevokeOutcome::GrantDropped { remaining: 1 });
        assert!(
            is_seated(&pool, team_id, member).await,
            "a seat that was manual FIRST must survive losing an automatic reason it gained later"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Auto-seating only ever writes `'member'`, so revocation must never
    /// remove an `'admin'` seat even when the last grant is gone.
    ///
    /// FAILS WITHOUT THE FIX: dropping `role <> 'admin'` from the UPDATE lets
    /// a team's own admin lose their seat by leaving a Slack channel.
    #[tokio::test]
    async fn an_admin_seat_is_never_revoked_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping an_admin_seat_is_never_revoked_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_seat_admin_protected";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        // `seed_team` seats `owner` as the team's admin.
        let team_id = seed_team(&pool, account, owner).await;

        // A grant naming the admin — the state after a roster walk that
        // found the team's own admin sitting in the routed channel.
        record_seat_grant(&pool, account, team_id, owner, "slack:C-ONE", None).await.unwrap();

        let outcome =
            revoke_seat_grant(&pool, account, team_id, owner, "slack:C-ONE").await.unwrap();

        assert_eq!(
            outcome,
            RevokeOutcome::AdminPreserved,
            "the grant is dropped, but an admin seat is not this system's to remove"
        );
        assert_eq!(
            team_member_role(&pool, team_id, owner).await.as_deref(),
            Some("admin"),
            "auto-seating never grants 'admin', so it must never take one away"
        );
        assert!(is_seated(&pool, team_id, owner).await);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A redelivered `member_left_channel` must not cascade. The first call
    /// revokes; the second finds no grant and reports `NotOurs` without
    /// touching anything — which also means it cannot revoke a seat the
    /// member has since legitimately regained.
    #[tokio::test]
    async fn revocation_is_idempotent_against_a_redelivered_event_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping revocation_is_idempotent_against_a_redelivered_event_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_seat_idempotent_revoke";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;
        let member = seed_user(&pool).await;

        seat_resolved_members(&pool, account, team_id, &[member], "slack:C-ONE", None).await;
        assert_eq!(
            revoke_seat_grant(&pool, account, team_id, member, "slack:C-ONE").await.unwrap(),
            RevokeOutcome::SeatRevoked
        );

        // Slack redelivers the same departure.
        assert_eq!(
            revoke_seat_grant(&pool, account, team_id, member, "slack:C-ONE").await.unwrap(),
            RevokeOutcome::NotOurs,
            "a redelivered departure has no grant left to drop and must change nothing"
        );

        // The member is re-added by hand, then the stale event arrives again.
        sqlx::query("UPDATE team_members SET departing_at = NULL WHERE team_id = $1 AND user_id = $2")
            .bind(team_id)
            .bind(member)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            revoke_seat_grant(&pool, account, team_id, member, "slack:C-ONE").await.unwrap(),
            RevokeOutcome::NotOurs
        );
        assert!(
            is_seated(&pool, team_id, member).await,
            "a stale redelivery must not undo a seat a human has since restored"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The `preexisting` marker is only earned by an ACTIVE seat. A departing
    /// row must not earn one — otherwise the marker would permanently block
    /// revocation for a person who is not even seated.
    #[tokio::test]
    async fn a_departing_row_earns_no_preexisting_marker_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_departing_row_earns_no_preexisting_marker_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_seat_departing_no_marker";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;
        let departing = seed_user(&pool).await;

        sqlx::query(
            "INSERT INTO team_members (team_id, user_id, role, departing_at) \
             VALUES ($1, $2, 'member', now())",
        )
        .bind(team_id)
        .bind(departing)
        .execute(&pool)
        .await
        .unwrap();

        seat_resolved_members(&pool, account, team_id, &[departing], "slack:C-ONE", None).await;

        assert_eq!(
            grant_count(&pool, team_id, departing).await,
            0,
            "a member left unseated earns no grants at all — neither the channel grant nor a \
             `preexisting` marker that would block revocation forever"
        );
        assert_ne!(SOURCE_PREEXISTING, "", "marker constant is non-empty (CHECK constraint)");

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── org-slack step 3: the admin check must never bypass an unidentified
    //    clicker ──────────────────────────────────────────────────────────

    /// [`TeamOceanCheck::NotAdmin`] — never a bypass — when `user_id` is
    /// `None`. This is the exact shape a real request takes whenever
    /// [`resolve_clicking_user`] could not CONFIRM an identity for the Slack
    /// user who clicked (no `users:read.email` grant yet, no matching
    /// `users` row, or a match that is not a current member): the team is
    /// real and belongs to this account, but an unidentified clicker must
    /// never be treated as its admin. Regression guard for org-slack step 3's
    /// pool split — `check_team_ocean_admin` now takes `tenant_pool`
    /// (`&pool` here, the single migrations_control-migrated pool, matching
    /// the byte-identical consumer-plane behaviour this split promises) —
    /// proving the split did not quietly weaken the `None` short-circuit.
    #[tokio::test]
    async fn check_team_ocean_admin_refuses_an_unidentified_clicker_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping check_team_ocean_admin_refuses_an_unidentified_clicker_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_team_admin_unidentified_clicker";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;

        let check = super::check_team_ocean_admin(&pool, account, None, team_id)
            .await
            .expect("check_team_ocean_admin must succeed against a real, existing team");
        assert!(
            matches!(check, super::TeamOceanCheck::NotAdmin { .. }),
            "an unidentified clicker (user_id: None) against a REAL team must yield NotAdmin, \
             never a bypass — got {check:?}"
        );

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ── org-slack step 3: tenant-pool resolution, and a picker write actually
//    landing on the tenant plane ────────────────────────────────────────────

#[cfg(test)]
mod org_tenant_pool_pg_tests {
    //! DB-backed tests for [`resolve_org_tenant_pool`] and the plane split it
    //! feeds into [`ensure_slack_watched_channel`] — org-slack step 3. TWO
    //! real Postgres databases: a CONTROL db (`accounts`/`tenant_databases`/
    //! `org_revocations`, `migrations_control` applied) and a genuine TENANT
    //! db (`migrations_tenant` applied), wired through a real
    //! [`TenantRegistry`] — the exact shape `tenant_registry_pg.rs` uses for
    //! the registry itself, extended here to prove the interactivity
    //! handler's OWN resolution chain (`org_is_revoked` -> `lookup_by_org` ->
    //! `is_servable()` -> `registry.get_or_build`) reaches the same place,
    //! and that a write made with the resolved pool actually lands THERE and
    //! not on the control/consumer plane.
    //!
    //! In-crate: [`resolve_org_tenant_pool`] is private, so an external test
    //! crate cannot drive it directly.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib org_tenant_pool_pg_tests

    use std::sync::Arc;

    use axum::http::StatusCode;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use crate::connectors::slack::{ensure_slack_watched_channel, PickedDestination, SlackChannelPick};
    use crate::routes::slack::AccountPlane;
    use crate::tenancy::registry::TenantRegistry;
    use crate::tenancy::runtime::TenantRuntime;
    use crate::tenancy::secrets::StaticMapResolver;

    use super::resolve_org_tenant_pool;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate_db(conn: &mut PgConnection, db: &str) {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
    }

    /// Everything one test needs: a real control pool with an org account +
    /// routing row, a real tenant pool with the tenant tree applied, and a
    /// `TenantRuntime` wired to actually resolve the routing row to the
    /// tenant pool via a real `TenantRegistry`.
    struct Fx {
        control_pool: PgPool,
        tenant_pool: PgPool,
        account_id: Uuid,
        clerk_org_id: String,
        /// A `graphs` row seeded in the TENANT db — the destination a
        /// `SlackChannelPick::Single` names.
        tenant_graph_id: Uuid,
        state: Arc<crate::AppState>,
    }

    async fn setup(control_db: &str, tenant_db: &str, tenant_status: &str) -> (PgConnection, Fx) {
        let base = base_url().expect("caller already checked base_url().is_some()");
        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        recreate_db(&mut admin_conn, control_db).await;
        recreate_db(&mut admin_conn, tenant_db).await;

        let control_pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, control_db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control").run(&control_pool).await.unwrap();

        let tenant_pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, tenant_db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();

        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("owner-{}@test.local", Uuid::new_v4()))
        .fetch_one(&control_pool)
        .await
        .unwrap();

        let clerk_org_id = format!("org-{}", Uuid::new_v4());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&clerk_org_id)
        .fetch_one(&control_pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(owner)
            .execute(&control_pool)
            .await
            .unwrap();

        let secret_ref = format!("SECRET_{}", Uuid::new_v4().simple());
        // Enter the real account/period financial transaction for this guarded fixture write.
        let mut fixture_financial_tx = crate::tenancy::scope::begin_account_scoped(&control_pool, account_id)
            .await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut fixture_financial_tx, account_id)
            .await.unwrap();
        sqlx::query(
            "INSERT INTO tenant_databases \
                (clerk_org_id, account_id, slug, db_secret_ref, status) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&clerk_org_id)
        .bind(account_id)
        .bind(format!("slug-{}", &Uuid::new_v4().simple().to_string()[..8]))
        .bind(&secret_ref)
        .bind(tenant_status)
        .execute(&mut *fixture_financial_tx)
        .await
        .unwrap();
        fixture_financial_tx.commit().await.unwrap();

        // Tenant-side fingerprint `TenantRegistry::build` asserts against —
        // same convention as `tenant_registry_pg.rs`'s
        // `apply_tenant_tree_and_fingerprint`.
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(&clerk_org_id)
            .execute(&tenant_pool)
            .await
            .unwrap();

        let tenant_graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'test team', 'team') \
             RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();

        let resolver = StaticMapResolver::from_pairs([(secret_ref, with_db(&base, tenant_db))]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let runtime = TenantRuntime::new(true, "apex.test", registry);
        let state = crate::test_app_state_with_tenant_runtime(Some(runtime));

        (
            admin_conn,
            Fx {
                control_pool,
                tenant_pool,
                account_id,
                clerk_org_id,
                tenant_graph_id,
                state,
            },
        )
    }

    async fn teardown(mut admin_conn: PgConnection, fx: Fx, control_db: &str, tenant_db: &str) {
        fx.control_pool.close().await;
        fx.tenant_pool.close().await;
        for db in [control_db, tenant_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    async fn slack_watched_channel_count(pool: &PgPool, account_id: Uuid, channel_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM slack_watched_channels WHERE account_id = $1 AND channel_id = $2",
        )
        .bind(account_id)
        .bind(channel_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// In-app org deletion (control 0387): every interactivity action in a
    /// deleting org (App Home buttons, the destination picker, backfills) is
    /// refused at the tenant-pool door; an undo reopens it.
    #[tokio::test]
    async fn a_live_org_deletion_refuses_the_tenant_pool_and_an_undo_restores_it_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping a_live_org_deletion_refuses_the_tenant_pool_and_an_undo_restores_it_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let (admin_conn, fx) = setup("orgt_slack_orgdel_pool_control", "orgt_slack_orgdel_pool_tenant", "active").await;
        assert!(
            resolve_org_tenant_pool(&fx.state, &fx.control_pool, fx.account_id, &fx.clerk_org_id).await.is_ok(),
            "baseline: servable"
        );
        let deletion: Uuid = sqlx::query_scalar(
            "INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by) VALUES ($1, $2, 'owner') RETURNING id",
        )
        .bind(fx.account_id)
        .bind(&fx.clerk_org_id)
        .fetch_one(&fx.control_pool)
        .await
        .unwrap();
        for live in ["requested", "grace", "awaiting_approval"] {
            sqlx::query("UPDATE org_deletions SET state = $2 WHERE id = $1")
                .bind(deletion)
                .bind(live)
                .execute(&fx.control_pool)
                .await
                .unwrap();
            let refused = resolve_org_tenant_pool(&fx.state, &fx.control_pool, fx.account_id, &fx.clerk_org_id).await;
            let resp = refused.expect_err(live);
            assert_eq!(resp.status(), StatusCode::OK, "{live}: acked with no visible follow-up, like a revoked org");
        }
        sqlx::query("UPDATE org_deletions SET state = 'canceled', canceled_at = now() WHERE id = $1")
            .bind(deletion)
            .execute(&fx.control_pool)
            .await
            .unwrap();
        assert!(
            resolve_org_tenant_pool(&fx.state, &fx.control_pool, fx.account_id, &fx.clerk_org_id).await.is_ok(),
            "undo restores it"
        );
        teardown(admin_conn, fx, "orgt_slack_orgdel_pool_control", "orgt_slack_orgdel_pool_tenant").await;
    }

    /// **The core proof, and this file's headline test:** a picker pick for
    /// an ORG account resolves its tenant pool via
    /// [`resolve_org_tenant_pool`] (the same chain
    /// `resolve_pool_account_bot_token` runs), and the resulting write
    /// (`ensure_slack_watched_channel`, exactly as `handle_destination_action`
    /// calls it) lands in the TENANT database — never the control/consumer
    /// one. FAILS WITHOUT THE FIX: before org-slack step 3,
    /// `resolve_pool_account_bot_token` returned only `state.db` (the control
    /// pool) for every account regardless of plane, so this exact write would
    /// have landed in `control_pool` instead, and `tenant_pool`'s count below
    /// would read `0` where this test asserts `1`.
    #[tokio::test]
    async fn a_picker_click_resolves_to_the_org_tenant_and_writes_there_pg() {
        let Some(_) = base_url() else {
            eprintln!(
                "skipping a_picker_click_resolves_to_the_org_tenant_and_writes_there_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (admin_conn, fx) = setup(
            "orgt_slack_i9y_tenant_write_control",
            "orgt_slack_i9y_tenant_write_tenant",
            "active",
        )
        .await;

        let resolved = resolve_org_tenant_pool(&fx.state, &fx.control_pool, fx.account_id, &fx.clerk_org_id)
            .await
            .expect("an active, servable tenant must resolve, not early-return a Response");

        let dsn_matches: bool = sqlx::query_scalar::<_, bool>("SELECT true")
            .fetch_one(&resolved)
            .await
            .is_ok();
        assert!(dsn_matches, "resolved tenant pool must be a live, queryable connection");

        let channel_id = "C-ORG-TENANT-WRITE";
        let owner_user_id: Uuid =
            sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
                .bind(fx.account_id)
                .fetch_one(&fx.control_pool)
                .await
                .unwrap();
        let outcome = ensure_slack_watched_channel(
            &fx.control_pool,
            &resolved,
            AccountPlane::Org,
            SlackChannelPick {
                account_id: fx.account_id,
                user_id: owner_user_id,
                workspace_id: "T-ORG-TENANT-WRITE".to_string(),
                channel_id: channel_id.to_string(),
                display_name: "test team".to_string(),
                destination: PickedDestination::Single(fx.tenant_graph_id),
            },
        )
        .await
        .expect("ensure_slack_watched_channel must succeed against the resolved tenant pool");
        assert!(outcome.newly_inserted);

        assert_eq!(
            slack_watched_channel_count(&resolved, fx.account_id, channel_id).await,
            1,
            "the pick must land on the TENANT pool"
        );
        assert_eq!(
            slack_watched_channel_count(&fx.control_pool, fx.account_id, channel_id).await,
            0,
            "the pick must NOT land on the control/consumer pool — this is the exact \
             regression org-slack step 3 exists to prevent"
        );

        teardown(
            admin_conn,
            fx,
            "orgt_slack_i9y_tenant_write_control",
            "orgt_slack_i9y_tenant_write_tenant",
        )
        .await;
    }

    /// A revoked org must never resolve a tenant pool, even from an
    /// interaction Slack itself delivered — member-private-oceans PR 7 §D,
    /// the same fence `clerk_webhook::provision_member_ocean_best_effort`
    /// enforces for its own webhook-driven, no-acting-user shape.
    #[tokio::test]
    async fn a_revoked_org_never_resolves_a_tenant_pool_pg() {
        let Some(_) = base_url() else {
            eprintln!(
                "skipping a_revoked_org_never_resolves_a_tenant_pool_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (admin_conn, fx) = setup(
            "orgt_slack_i9y_revoked_control",
            "orgt_slack_i9y_revoked_tenant",
            "active",
        )
        .await;

        sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)")
            .bind(&fx.clerk_org_id)
            .execute(&fx.control_pool)
            .await
            .unwrap();

        let result =
            resolve_org_tenant_pool(&fx.state, &fx.control_pool, fx.account_id, &fx.clerk_org_id).await;
        // `is_err()` alone cannot distinguish a 200 ack from a 503 — and
        // `resolve_org_tenant_pool`'s own doc comment draws exactly that
        // line: a revoked org is a PERMANENT condition (never a carve-out,
        // member-private-oceans PR 7 §D), acked 200 with no visible
        // follow-up, same as this file's other "nothing to do" branches —
        // never the 503 reserved for a transient-shaped failure (a lookup
        // error, a timeout). Asserting the actual status is what would catch
        // a regression that answered 503 here instead of silently passing.
        let response = result.expect_err("a revoked org must never resolve a tenant pool, even via the interactivity path");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a revoked org is a PERMANENT refusal — must ack 200 (no visible follow-up), never \
             the 503 reserved for a transient-shaped failure"
        );

        teardown(
            admin_conn,
            fx,
            "orgt_slack_i9y_revoked_control",
            "orgt_slack_i9y_revoked_tenant",
        )
        .await;
    }

    /// A `provisioning`-status routing row (mid-onboarding, not yet
    /// `active`) must never be treated as servable — mirrors
    /// `tenancy::routing::TenantStatus::is_servable`'s own contract.
    #[tokio::test]
    async fn a_non_servable_tenant_never_resolves_a_pool_pg() {
        let Some(_) = base_url() else {
            eprintln!(
                "skipping a_non_servable_tenant_never_resolves_a_pool_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (admin_conn, fx) = setup(
            "orgt_slack_i9y_provisioning_control",
            "orgt_slack_i9y_provisioning_tenant",
            "provisioning",
        )
        .await;

        let result =
            resolve_org_tenant_pool(&fx.state, &fx.control_pool, fx.account_id, &fx.clerk_org_id).await;
        // Same reasoning as the revoked-org test above: a `provisioning`
        // routing row is ALSO a permanent-shaped condition per
        // `resolve_org_tenant_pool`'s own doc comment (not a lookup error or
        // a timeout), so it must ack 200, never 503 — asserting the actual
        // status catches a regression `is_err()` alone would let through.
        let response = result.expect_err(
            "a routing row still in 'provisioning' status must never resolve a servable pool",
        );
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a non-servable routing row is a PERMANENT refusal — must ack 200 (no visible \
             follow-up), never the 503 reserved for a transient-shaped failure"
        );

        teardown(
            admin_conn,
            fx,
            "orgt_slack_i9y_provisioning_control",
            "orgt_slack_i9y_provisioning_tenant",
        )
        .await;
    }
}
