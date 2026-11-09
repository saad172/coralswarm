//! Slack integration routes.
//!
//! # OAuth flow
//! 1. Frontend calls `GET /v1/integrations/slack/oauth/start` → gets a redirect URL.
//! 2. User authorises CoralSwarm in Slack.
//! 3. Slack redirects to `GET /v1/integrations/slack/oauth/callback?code=...&state=...`.
//! 4. Backend exchanges code for token and stores in `slack_integrations`.
//!
//! Slack's Event API payloads arrive at `POST /v1/slack/events`
//! (`routes::slack_events`).
//!
//! # Required env vars (all optional — handlers return 501 when absent)
//! - `SLACK_CLIENT_ID`
//! - `SLACK_CLIENT_SECRET`

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

use crate::{middleware::auth::UserContext, tenancy::extractor::Tenant, AppState};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Return a 501 response when a required env var is missing.
pub(crate) fn not_configured(var: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({"error": format!("{var} not configured")})),
    )
}

/// Return a 503 response when the DB pool is absent.
fn no_db() -> (StatusCode, Json<Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
}

/// Fail-closed gate for the JWT-authed Slack management routes under an org
/// `TenancyDecision` (org-tenancy PR 1.6) — same policy + rationale as
/// `routes::connectors::reject_org_connector_access`: connectors (and their
/// workspace connections) are personal-workspace-only in the v1 org model,
/// and the unauthenticated Slack webhooks resolve state on the CONSUMER
/// plane, so an org-context connect/list/disconnect here would create or
/// manage rows the webhook path could never see. Refuse cleanly instead of
/// silently touching the wrong plane. Personal / flag-off requests are
/// byte-identical to before.
///
/// **org-slack step 5 — lifted under [`crate::routes::oauth::
/// org_slack_oauth_enabled`].** Unlike `routes::oauth`'s generic
/// `reject_org_oauth_management`, this gate needs no separate per-provider
/// check: every route in THIS file is Slack, always — there is no
/// `provider` path parameter to get wrong. Once lifted, `oauth_callback`/
/// `list_integrations`/`delete_integration` resolve their pool from
/// `state.control_db()`, not `tenant.pool` — see each handler's own doc
/// comment for why (the token-plane decision: `oauth_tokens`/
/// `slack_integrations`/`connector_installs` are CONTROL-plane tables, and
/// `tenant.pool` stops being the control pool the moment an org account can
/// reach this file). Shares the ONE flag predicate with
/// `routes::oauth::reject_org_oauth_management` so the two Slack-OAuth
/// surfaces (this legacy path and the generic connector path) can never
/// disagree about "on".
async fn reject_org_slack_management(
    state: &AppState,
    tenant: &Tenant,
) -> Option<(StatusCode, Json<Value>)> {
    let ctx = tenant.org_context()?;
    if crate::routes::oauth::org_slack_oauth_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "slack management route refused under org context (personal-workspace-only in v1)"
    );
    Some((
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "Slack integrations are available in your personal workspace only; \
                      switch out of the organization workspace to manage them"
        })),
    ))
}

// ── OAuth: start ──────────────────────────────────────────────────────────────

/// `GET /v1/integrations/slack/oauth/start`
///
/// Returns a Slack OAuth URL that the frontend should redirect the user to.
/// Requires `SLACK_CLIENT_ID` to be set.
///
/// No `reject_org_slack_management` gate here — see that function's own doc
/// comment ("no DB access, no write"), unchanged by this PR. It DOES now
/// carry the org governance admin gate (2026-08-04, piece 1):
/// [`oauth_callback`] below (this route's own callback — NOT the generic
/// `routes::oauth::callback`) is itself authenticated
/// (`Extension<UserContext>`, same as `delete_integration`) and now carries
/// the SAME admin gate this function does, so a non-admin's authorize URL is
/// a genuine dead end all the way through — not merely "some other gate
/// downstream will still catch it". Gating the URL-mint here too closes the
/// window one step earlier and keeps the two ends of one flow agreeing on
/// who may even start it. `tenant`/`user` are threaded through purely for
/// this check; neither is otherwise used here.
pub async fn oauth_start(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = crate::routes::oauth::reject_org_slack_non_admin(&tenant, &user.role) {
        return reject.into_response();
    }

    // PR 1 (Slack OAuth scope reconciliation, 2026-09-19): this handler used
    // to mint its OWN authorize URL with a hardcoded, comma-separated,
    // never-URL-encoded scope list that never carried
    // `users:read`/`users:read.email`, no `redirect_uri` at all, and a
    // `state` its own callback never verified (`OAuthCallbackQuery::state`
    // is `#[allow(dead_code)]` right below). A workspace installed through
    // that URL could never pass `resolve_verified_slack_identity`'s C5
    // identity match — its bot token would never carry
    // `users:read.email` — so it could never fan out into any linked
    // member's private ocean, which defeats the entire reason this route
    // exists. Slack does not retroactively grant a newly-added scope to an
    // already-installed workspace, so no code change here can repair a
    // workspace that already installed through the old URL; it needs a
    // human to reinstall (see `SlackConnector::authorize_url`'s own doc
    // comment, and `slack_scopes()` above it, for the general version of
    // this limitation).
    //
    // This route has ZERO live callers in `web/`/`marketing-web/`
    // (verified 2026-09-19) — the product already exclusively drives Slack
    // connect through `GET /v1/oauth/slack/authorize`
    // (`web/lib/api.ts`'s `authorizeUrl`). Rather than 410-ing a route that
    // might still be curled by an operator or hit from an old bookmark, this
    // now DELEGATES to the exact same connector the modern flow uses —
    // `SlackConnector::authorize_url`, via `connectors::lookup("slack")` —
    // with the modern `redirect_uri` and a real, single-use, HMAC-signed
    // state token (`oauth::state::sign_state`) bound to THIS authenticated
    // caller's own account, the same mechanism `routes::oauth::mint_state`
    // uses for every other connect flow. Any surviving caller of this route
    // is upgraded onto the fully-scoped, CSRF-verified modern flow instead
    // of being broken outright. Its own callback below
    // (`GET /v1/integrations/slack/oauth/callback`) is consequently never
    // reached by a URL minted from here anymore — the URL this now returns
    // redirects Slack to the MODERN callback (`GET /v1/oauth/slack/
    // callback`) instead. That old callback route is deliberately left in
    // place (removing it needs `member_ocean_probe_routes.manifest` /
    // `route_migration_org_pg.rs` churn, out of scope for this PR) but is
    // now reachable only by a state token minted the pre-PR way, which
    // nothing in this codebase mints anymore.
    let connector = match crate::connectors::lookup("slack") {
        Some(c) => c,
        // Slack is always a registered connector in this binary; this arm
        // exists only so a future refactor that ever de-registers it fails
        // loudly here instead of panicking on `.unwrap()`.
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "slack connector not registered"})),
            )
                .into_response()
        }
    };

    let secret = match crate::oauth::state::state_secret() {
        Some(s) => s,
        None => {
            tracing::error!("oauth_start: OAUTH_STATE_SECRET missing or empty");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "oauth state signing not configured"})),
            )
                .into_response();
        }
    };
    let nonce = Uuid::new_v4().to_string();
    let state_param = match crate::oauth::state::sign_state(
        &secret,
        user.account_id,
        Utc::now().timestamp(),
        &nonce,
        Some(user.user_id),
        None,
    ) {
        Ok(token) => token,
        Err(e) => {
            tracing::error!(error = %e, "oauth_start: failed to sign oauth state");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    let base_url = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());
    match connector.authorize_url(&base_url, &state_param) {
        Ok(url) => (StatusCode::OK, Json(json!({"url": url}))).into_response(),
        // `SlackConnector::authorize_url`'s only failure mode is a missing
        // `SLACK_CLIENT_ID` (`ConnectorError::NotConfigured`) — mapped to
        // `501`, matching this file's own `not_configured` convention (see
        // this module's header: "handlers return 501 when absent"), not the
        // `503` `routes::oauth::connector_error_response` would use for the
        // same variant. Kept as an explicit match (not a call into that
        // private function, which is not visible from this module) so the
        // status code stays byte-identical to every other missing-env-var
        // response in this file.
        Err(crate::connectors::ConnectorError::NotConfigured(msg)) => {
            (StatusCode::NOT_IMPLEMENTED, Json(json!({"error": msg}))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "oauth_start: failed to build Slack authorize URL");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

// ── OAuth: callback ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct OAuthCallbackQuery {
    code: String,
    #[allow(dead_code)]
    state: Option<String>,
}

/// `GET /v1/integrations/slack/oauth/callback`
///
/// Exchanges the OAuth code for a bot token and stores it.
/// Requires `SLACK_CLIENT_ID` and `SLACK_CLIENT_SECRET`.
pub async fn oauth_callback(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<OAuthCallbackQuery>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }
    // Org governance decision (2026-08-04), piece 1. CRITICAL: without this,
    // any org member — not just an admin — could complete Slack consent on a
    // workspace they personally control and `curl` this callback with their
    // ORG bearer token, writing `slack_integrations`/`connector_installs` for
    // the org account (this route is authenticated with `Extension<UserContext>`
    // same as `delete_integration`; `reject_org_slack_management` above only
    // gates the `ORG_SLACK_OAUTH_ENABLED` flag, not who is calling). Combined
    // with piece 2's one-workspace-per-org refusal, a member could even use
    // this to occupy the org's only workspace slot and lock the real admin's
    // legitimate connect out with a 409.
    if let Some(reject) = crate::routes::oauth::reject_org_slack_non_admin(&tenant, &user.role) {
        return reject.into_response();
    }
    let client_id = match std::env::var("SLACK_CLIENT_ID") {
        Ok(v) => v,
        Err(_) => return not_configured("SLACK_CLIENT_ID").into_response(),
    };
    let client_secret = match std::env::var("SLACK_CLIENT_SECRET") {
        Ok(v) => v,
        Err(_) => return not_configured("SLACK_CLIENT_SECRET").into_response(),
    };

    // `slack_integrations`/`connector_installs` are CONTROL-plane tables
    // (org-slack step 5, the token-plane decision), so this reaches for
    // `state.control_db()`, never `tenant.pool` — the webhook ingest path
    // (`resolve_slack_account_id` / `resolve_slack_account_planes`) always
    // resolves these tables against the control pool too, and `tenant.pool`
    // stops being the SAME handle as control the moment an org account can
    // reach this route (`ORG_SLACK_OAUTH_ENABLED`): under
    // `TenancyDecision::Org` it is the org's TENANT pool instead, and
    // writing there would be invisible to every webhook that later tries to
    // resolve this workspace. Personal / flag-off requests are unaffected —
    // this was always the same physical pool `tenant.pool` resolved to for
    // them.
    let pool = match state.control_db() {
        Some(p) => p.clone(),
        None => return no_db().into_response(),
    };

    // Exchange code for token via Slack API.
    let http = reqwest::Client::new();
    let resp = match http
        .post(format!("{}/oauth.v2.access", crate::connectors::slack::slack_api_base()))
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("code", params.code.as_str()),
        ])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "Slack oauth.v2.access HTTP error");
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "failed to reach Slack API"})),
            )
                .into_response();
        }
    };

    let body: Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "Slack oauth.v2.access JSON parse error");
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "invalid response from Slack API"})),
            )
                .into_response();
        }
    };

    if body.get("ok").and_then(Value::as_bool) != Some(true) {
        let err = body.get("error").and_then(Value::as_str).unwrap_or("unknown");
        tracing::warn!(slack_error = %err, "Slack oauth.v2.access returned ok=false");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("Slack error: {err}")})),
        )
            .into_response();
    }

    // Extract workspace info and bot token.
    let workspace_id = match body
        .pointer("/team/id")
        .and_then(Value::as_str)
    {
        Some(v) => v.to_owned(),
        None => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "missing team.id in Slack response"})),
            )
                .into_response();
        }
    };

    let workspace_name = body
        .pointer("/team/name")
        .and_then(Value::as_str)
        .unwrap_or("Unknown workspace")
        .to_owned();

    let bot_token = match body
        .pointer("/access_token")
        .and_then(Value::as_str)
    {
        Some(v) => v.to_owned(),
        None => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "missing access_token in Slack response"})),
            )
                .into_response();
        }
    };

    // The Slack user who completed this install, when present — best-effort,
    // used only as `connector_installs.external_user_id` (migration
    // 0121_connector_installs) below; nothing in this legacy flow reads it
    // back.
    let authed_user_id = body
        .pointer("/authed_user/id")
        .and_then(Value::as_str)
        .map(str::to_owned);

    // Foundation PR 1.5: dual-write `oauth_tokens` (below, alongside
    // `slack_integrations`) so every read site migrated off
    // `slack_integrations.bot_token` in this PR can resolve a bot token for
    // a workspace connected through THIS legacy path — before this PR, this
    // callback wrote `slack_integrations`/`connector_installs` only, and
    // never `oauth_tokens` at all (the generic `routes::oauth::callback`
    // path is the only one that did). `scope`/`authed_user.access_token`
    // mirror exactly what `connectors::slack::SlackConnector::exchange_code`
    // extracts from the identical Slack API response shape (`oauth/slack.rs::
    // exchange_code`), so the TokenBundle built below is byte-equivalent to
    // what the generic connector path would have stored for the same Slack
    // response.
    let scope = body.pointer("/scope").and_then(Value::as_str).unwrap_or("").to_owned();
    let user_token = body
        .pointer("/authed_user/access_token")
        .and_then(Value::as_str)
        .map(str::to_owned);

    // Org governance decision (2026-08-04), piece 2: one workspace per org.
    // This legacy path is the SECOND of the two writers `routes::oauth::
    // refuse_second_org_slack_workspace`'s own doc comment names — reached
    // whenever an org account is past `reject_org_slack_management` above
    // (i.e. `ORG_SLACK_OAUTH_ENABLED` is on). Checked here too, not only in
    // the generic path's `callback`, so an org can never end up with two
    // workspaces regardless of which of the two callbacks a request lands
    // on. Personal accounts are unaffected — `tenant.org_context()` is
    // `None` for them, same "org accounts only" scope the decision itself
    // draws. This is a cheap early exit for the ordinary (non-racing) case
    // ONLY — see `refuse_second_org_slack_workspace`'s own doc comment for
    // why it alone does not close a genuinely concurrent connect; the
    // definitive check is `refuse_second_org_slack_workspace_locked`, run
    // again below inside the write transaction.
    if tenant.org_context().is_some() {
        if let Err(resp) = crate::routes::oauth::refuse_second_org_slack_workspace(
            &pool,
            user.account_id,
            &workspace_id,
        )
        .await
        {
            return resp;
        }
    }

    // Upsert `slack_integrations` AND the control-plane routing index
    // (`connector_installs`, migration 0121_connector_installs) as ONE
    // atomic transaction — inside a `begin_account_scoped` transaction
    // (org-tenancy RLS conversion; `slack_integrations` carries an
    // `account_id` policy per migration 0083; `connector_installs` carries
    // none, see that migration's header, but sharing the transaction costs
    // nothing and is what makes the atomicity below work). This legacy flow
    // never writes `oauth_tokens`, so it is one of the two places (alongside
    // `routes/oauth.rs::callback`) that must record the install itself — and
    // NOT on a best-effort basis: see `routes::oauth::record_connector_install`'s
    // own doc comment for why an install that half-lands (this table
    // committed, `connector_installs` not) is now a silent permanent
    // message-drop, not a degraded-router nicety.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(&pool, user.account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, "failed to open scoped tx for slack_integrations upsert");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "database error"})),
            )
                .into_response();
        }
    };

    // Org governance decision (2026-08-04), piece 2 — the definitive,
    // race-closing recheck (see `refuse_second_org_slack_workspace_locked`'s
    // own doc comment): same condition as the pre-check above, run again
    // inside THIS transaction after taking a per-account advisory lock.
    if tenant.org_context().is_some() {
        if let Err(resp) = crate::routes::oauth::refuse_second_org_slack_workspace_locked(
            &mut tx,
            user.account_id,
            &workspace_id,
        )
        .await
        {
            tx.rollback().await.ok();
            return resp;
        }
    }

    let integration_upsert = sqlx::query(
        r#"
        INSERT INTO slack_integrations
            (account_id, workspace_id, workspace_name, bot_token, created_at)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (account_id, workspace_id)
        DO UPDATE SET
            workspace_name = EXCLUDED.workspace_name,
            bot_token      = EXCLUDED.bot_token
        "#,
    )
    .bind(user.account_id)
    .bind(&workspace_id)
    .bind(&workspace_name)
    .bind(&bot_token)
    .execute(&mut *tx)
    .await;
    if let Err(e) = integration_upsert {
        tracing::warn!(error = %e, "failed to upsert slack_integrations");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "database error"})),
        )
            .into_response();
    }

    // Foundation PR 1.5 (dual-write — see the `scope`/`user_token` extraction
    // above for why): same atomic transaction as `slack_integrations`, same
    // `TokenOwner::AccountWide` every other Slack call site uses (Slack has
    // no per-member OAuth — see `connectors::TokenOwner`'s own doc comment).
    // Failing the whole install on an encryption/DB error here (rather than
    // logging and continuing) matches `record_connector_install`'s own
    // "half-landed install is now a silent permanent gap" reasoning just
    // below: an install that leaves `slack_integrations` committed but
    // `oauth_tokens` missing is exactly the legacy gap this PR's backfill
    // binary (`bin/backfill_slack_oauth_tokens`) exists to repair for
    // installs that happened BEFORE this dual-write shipped — a fresh
    // install must not manufacture a new instance of that same gap.
    let bundle = crate::connectors::TokenBundle {
        access_token: bot_token.clone(),
        refresh_token: None,
        user_token,
        scope,
        // Slack v2 bot tokens do not carry an expiry in this flow (matches
        // `connectors::slack::SlackConnector::exchange_code`'s identical note).
        expires_at: None,
        provider_workspace_id: workspace_id.clone(),
        provider_workspace_name: Some(workspace_name.clone()),
        provider_user_id: authed_user_id.clone().unwrap_or_default(),
    };
    // The DEK plane is a SEPARATE question from `pool` (org-plane DEK fix):
    // `pool` is `state.control_db()` because the `oauth_tokens`/
    // `slack_integrations` ROWS are control-plane-only, but an ORG account's
    // wrapped DEK is not — it lives in that org's tenant DB's `account_keys`.
    // This route runs behind the binding gate (it takes a `Tenant`), so the
    // request's own decision already carries the fingerprint-verified tenant
    // pool; no routing lookup needed, unlike `routes::oauth::callback`'s
    // unauthenticated redirect. Personal/flag-off installs resolve to the
    // consumer plane exactly as before.
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), &pool);
    if let Err(resp) = crate::routes::oauth::store_token_bundle_tx(
        &mut tx,
        &dek_plane,
        user.account_id,
        "slack",
        crate::connectors::TokenOwner::AccountWide,
        &bundle,
    )
    .await
    {
        return resp;
    }

    if let Err(e) = crate::routes::oauth::record_connector_install(
        &mut tx,
        user.account_id,
        "slack",
        &workspace_id,
        authed_user_id.as_deref(),
        // Slack's bot token is workspace-wide by design — account-wide,
        // byte-identical to before this parameter existed (see that fn's
        // own doc comment).
        None,
    )
    .await
    {
        tracing::warn!(
            error = %e,
            account_id = %user.account_id,
            workspace_id = %workspace_id,
            "failed to upsert connector_installs — failing the install rather than leaving it \
             unroutable (see routes::oauth::record_connector_install's doc comment)"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "database error"})),
        )
            .into_response();
    }

    if let Err(e) = tx.commit().await {
        tracing::warn!(error = %e, "failed to commit slack_integrations/connector_installs transaction");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "database error"})),
        )
            .into_response();
    }

    tracing::info!(
        account_id = %user.account_id,
        workspace_id = %workspace_id,
        workspace_name = %workspace_name,
        "Slack workspace connected"
    );

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "workspace": workspace_name,
            "workspace_id": workspace_id,
        })),
    )
        .into_response()
}

// ── List integrations ─────────────────────────────────────────────────────────

/// `GET /v1/integrations/slack`
///
/// Lists all Slack workspaces connected to the authenticated account.
pub async fn list_integrations(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }
    // CONTROL pool, not `tenant.pool` — see `oauth_callback`'s doc comment.
    let pool = match state.control_db() {
        Some(p) => p.clone(),
        None => return no_db().into_response(),
    };

    // One `begin_account_scoped` transaction per request (org-tenancy RLS
    // conversion) — same rationale as `oauth_callback` above.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(&pool, user.account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, "failed to open scoped tx for slack_integrations list");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "database error"})),
            )
                .into_response();
        }
    };
    let rows = sqlx::query(
        "SELECT id, workspace_id, workspace_name, created_at \
         FROM slack_integrations \
         WHERE account_id = $1 \
         ORDER BY created_at DESC",
    )
    .bind(user.account_id)
    .fetch_all(&mut *tx)
    .await;
    tx.rollback().await.ok();

    let rows = match rows {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "failed to list slack_integrations");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "database error"})),
            )
                .into_response();
        }
    };

    let workspaces: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "id": r.get::<Uuid, _>("id"),
                "workspace_id": r.get::<String, _>("workspace_id"),
                "workspace_name": r.get::<String, _>("workspace_name"),
                "created_at": r.get::<chrono::DateTime<Utc>, _>("created_at"),
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({"workspaces": workspaces}))).into_response()
}

// ── Delete integration ────────────────────────────────────────────────────────

/// `DELETE /v1/integrations/slack/:workspace_id`
///
/// Disconnects a Slack workspace from the authenticated account. Also
/// removes the matching `connector_installs` routing row (migration
/// `0121_connector_installs`) — UNLESS an `oauth_tokens` row still backs the
/// same `(account_id, workspace)` via the modern connector path
/// (`routes/oauth.rs::callback`), which owns that row and is untouched by
/// this route (mirror image of `routes/oauth.rs::delete_connection`'s own
/// guard — see that function's doc comment).
pub async fn delete_integration(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(workspace_id): Path<String>,
) -> impl IntoResponse {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }
    // Org governance decision (2026-08-04), piece 1.
    if let Some(reject) = crate::routes::oauth::reject_org_slack_non_admin(&tenant, &user.role) {
        return reject.into_response();
    }
    // CONTROL pool, not `tenant.pool` — see `oauth_callback`'s doc comment.
    let pool = match state.control_db() {
        Some(p) => p.clone(),
        None => return no_db().into_response(),
    };

    // Org governance decision (2026-08-04), piece 3: capture the bot token
    // BEFORE the delete below removes it — the shared purge routine's
    // best-effort `conversations.leave` step (run after a successful delete,
    // see below) needs it, and once `slack_integrations` is gone this is the
    // only place it lived. Best-effort itself: a failed lookup here only
    // means the purge's `conversations.leave` step is skipped, never that
    // the disconnect itself is refused.
    // ROW on control (`pool`), KEY on the request's own plane (org-plane DEK
    // fix) — this route is behind the binding gate, so the decision already
    // carries the org's fingerprint-verified tenant pool.
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), &pool);
    let bot_token_before = crate::connectors::slack_disconnect::fetch_slack_bot_token(
        &pool,
        &dek_plane,
        user.account_id,
        &workspace_id,
    )
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(
            error = %e,
            account_id = %user.account_id,
            workspace_id = %workspace_id,
            "slack disconnect: failed to read bot token before delete — conversations.leave \
             will be skipped for this disconnect's purge"
        );
        None
    });

    // Same one-scoped-tx-per-request pattern as `list_integrations`.
    let result = match crate::tenancy::scope::begin_account_scoped(&pool, user.account_id).await {
        Ok(mut tx) => {
            let r = sqlx::query(
                "DELETE FROM slack_integrations \
                 WHERE account_id = $1 AND workspace_id = $2",
            )
            .bind(user.account_id)
            .bind(&workspace_id)
            .execute(&mut *tx)
            .await;
            let r = match r {
                Ok(r) if r.rows_affected() > 0 => {
                    // Foundation PR 1.1: `owner_user_id IS NULL` — Slack has no
                    // per-member OAuth (a workspace bot token is always
                    // account-wide; see `docs/plans/org-connectors-and-channel-
                    // ingest.md` §14.1), so this guard only ever needs to see
                    // the account-wide row.
                    let install_delete = sqlx::query(
                        "DELETE FROM connector_installs \
                         WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id = $2 \
                           AND NOT EXISTS ( \
                                 SELECT 1 FROM oauth_tokens ot \
                                 WHERE ot.account_id = $1 AND ot.provider = 'slack' \
                                   AND ot.provider_workspace_id = $2 \
                                   AND ot.owner_user_id IS NULL \
                               )",
                    )
                    .bind(user.account_id)
                    .bind(&workspace_id)
                    .execute(&mut *tx)
                    .await;
                    // Same correction as `routes::oauth::delete_connection`'s
                    // sibling cleanup, for the same reason: this is NOT
                    // "non-fatal". A failed statement aborts the enclosing
                    // Postgres transaction, and `COMMIT` on an aborted
                    // transaction runs as ROLLBACK and RETURNS SUCCESS — so
                    // logging a warning and committing would discard the
                    // `slack_integrations` delete too and still answer
                    // `200 OK`. Propagating the error takes the `Err` arm
                    // below, which skips the commit and 500s.
                    match install_delete {
                        Ok(_) => Ok(r),
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                account_id = %user.account_id,
                                workspace_id = %workspace_id,
                                "failed to delete connector_installs row on slack_integration \
                                 disconnect — failing the whole disconnect (the transaction is \
                                 already poisoned, so committing would silently revert the \
                                 slack_integrations delete and still report success)"
                            );
                            Err(e)
                        }
                    }
                }
                other => other,
            };
            match r {
                Ok(r) => tx.commit().await.map(|_| r),
                Err(e) => Err(e),
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to open scoped tx for slack_integration delete");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "database error"})),
            )
                .into_response();
        }
    };

    match result {
        Err(e) => {
            tracing::warn!(error = %e, "failed to delete slack_integration");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "database error"})),
            )
                .into_response()
        }
        Ok(res) if res.rows_affected() == 0 => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "workspace not found"})),
        )
            .into_response(),
        Ok(_) => {
            tracing::info!(
                account_id = %user.account_id,
                workspace_id = %workspace_id,
                "Slack workspace disconnected"
            );

            // ── Shared disconnect purge (org governance decision 2026-08-04,
            // piece 3) — see `connectors::slack_disconnect`'s own header for
            // the full contract. Runs on `tenant.pool` (the content plane,
            // NOT the `pool`/CONTROL handle this function otherwise uses),
            // matching `resolve_connected_slack_workspace`'s own pool split.
            match &tenant.pool {
                Some(tenant_pool) => {
                    let outcome = crate::connectors::slack_disconnect::purge_slack_workspace(
                        &state.http_client,
                        &pool,
                        tenant_pool,
                        user.account_id,
                        &workspace_id,
                        bot_token_before.as_deref(),
                    )
                    .await;
                    if outcome.purge_had_a_failure() {
                        // See `PurgeOutcome::purge_had_a_failure`'s own doc
                        // comment — the integration row is already gone
                        // above, so a failed purge here must never read as
                        // "complete".
                        tracing::warn!(
                            account_id = %user.account_id,
                            workspace_id = %workspace_id,
                            ?outcome,
                            connector_health = true,
                            "slack disconnect: watched-channel/seat purge FAILED (partially) — \
                             the integration row is already gone; stale destinations may \
                             resurrect on reconnect"
                        );
                    } else {
                        tracing::info!(
                            account_id = %user.account_id,
                            workspace_id = %workspace_id,
                            ?outcome,
                            "slack disconnect: watched-channel/seat purge complete"
                        );
                    }
                }
                None => tracing::warn!(
                    account_id = %user.account_id,
                    workspace_id = %workspace_id,
                    "slack disconnect: no tenant pool available — skipping watched-channel/seat \
                     purge (the integration row is already gone above)"
                ),
            }

            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
        }
    }
}

// ── Onboarding status ────────────────────────────────────────────────────────

/// The `slack_integrations` row [`resolve_connected_slack_workspace`] hands
/// back — the single connected workspace both authed Slack read endpoints
/// derive everything else from.
struct ConnectedWorkspaceRow {
    workspace_id: String,
    workspace_name: String,
    bot_token: String,
}

/// Everything [`onboarding_status`] and [`channel_management`] both need
/// before they can derive anything: the two pools, the plane, and the
/// account's most-recently-connected `slack_integrations` row (if any).
struct ConnectedSlackWorkspace {
    /// CONTROL pool — `slack_integrations`, `slack_huddle_captures`,
    /// `oauth_tokens`.
    control_pool: sqlx::PgPool,
    /// Content-plane pool — `slack_watched_channels`/`graphs`/`atoms`/
    /// `connector_identities`. Byte-identical to `control_pool` on Personal.
    tenant_pool: sqlx::PgPool,
    plane: AccountPlane,
    /// `None` = this account has no Slack workspace connected at all. The two
    /// callers deliberately disagree about what that MEANS (a soft 200
    /// checklist vs. a 404 detail page), so this helper reports the fact and
    /// refuses to pick — see each handler's own `let Some(row) = …` branch.
    row: Option<ConnectedWorkspaceRow>,
}

/// Resolve the pools, the plane, and the one connected Slack workspace for
/// `account_id` — the shared prologue of [`onboarding_status`] and
/// [`channel_management`].
///
/// **Why this is a function and not two copies.** These ~55 lines were
/// duplicated byte-for-byte between the two handlers, right down to the
/// `ORDER BY created_at DESC LIMIT 1` and the `begin_account_scoped` wrapper.
/// That is the specific duplication worth killing: this is the lookup that
/// resolves a **bot token** under account scoping, so a scoping change
/// (the `coralswarm_app`/`BYPASSRLS` cutover — OPS-4, which has since run —
/// was exactly such a change, and a future tightening of
/// the `account_id` predicate is another) applied to one copy and not the other would
/// leave a live hole in whichever handler was missed, with nothing in review
/// to flag it — the two bodies had already drifted apart in their log
/// strings, so a reviewer diffing them would not see them as "the same code"
/// at a glance.
///
/// Returns `Err(Response)` for the two conditions that are the SAME answer at
/// both call sites — no database at all (503) and a failed lookup (500, with
/// the real cause logged, never echoed to the client). `context` only labels
/// the log line so the two handlers stay distinguishable in traces.
///
/// Which workspace? An account can have more than one `slack_integrations`
/// row (`UNIQUE (account_id, workspace_id)`, not `UNIQUE (account_id)`); both
/// endpoints' contracts name exactly one, so the most-recently-connected row
/// wins — the same "one workspace" assumption `oauth_callback`'s redirect and
/// this file's other single-workspace-shaped UI already make. A genuine
/// multi-workspace account is a pre-existing edge case neither endpoint
/// attempts to widen.
///
/// Scoped: `slack_integrations` carries 0083's `account_isolation` policy.
/// The explicit `account_id = $1` predicate is the first layer, and — since
/// OPS-4 cut the app over to the non-`BYPASSRLS` `coralswarm_app` role — the
/// scope wrapper's `app.account_id` makes the RLS policy a live second layer,
/// not a placeholder.
async fn resolve_connected_slack_workspace(
    state: &Arc<AppState>,
    tenant: &Tenant,
    account_id: Uuid,
    context: &'static str,
) -> Result<ConnectedSlackWorkspace, axum::response::Response> {
    // CONTROL pool, not `tenant.pool` — `slack_integrations` is a
    // CONTROL-plane table (see `oauth_callback`'s doc comment above).
    let Some(control_pool) = state.control_db().cloned() else {
        return Err(no_db().into_response());
    };
    // Content-plane pool — the consumer pool for Personal (byte-identical to
    // `control_pool` there), or the org's own tenant pool once the org gate
    // is lifted and cutover has run. Same resolution
    // `routes::connectors::finish_ingest` relies on post-gate.
    let Some(tenant_pool) = tenant.pool.clone() else {
        return Err(no_db().into_response());
    };

    let plane = if tenant.org_context().is_some() {
        AccountPlane::Org
    } else {
        AccountPlane::Personal
    };

    let mut tx = match crate::tenancy::scope::begin_account_scoped(&control_pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                context = context,
                "failed to open scoped tx for slack_integrations lookup"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response());
        }
    };
    let row = sqlx::query(
        "SELECT workspace_id, workspace_name FROM slack_integrations \
         WHERE account_id = $1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await;
    tx.rollback().await.ok();

    let row = match row {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                context = context,
                "failed to look up slack_integrations"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response());
        }
    };

    let Some(row) = row else {
        return Ok(ConnectedSlackWorkspace { control_pool, tenant_pool, plane, row: None });
    };
    let workspace_id: String = row.get("workspace_id");
    let workspace_name: String = row.get("workspace_name");

    // Foundation PR 1.5: the bot token itself now comes from the encrypted
    // `oauth_tokens` copy, not `slack_integrations.bot_token` — the workspace
    // id/name above are non-sensitive and still read from `slack_integrations`
    // (the only table that carries them). `Ok(None)` (no oauth_tokens row
    // yet — e.g. a legacy connection not yet backfilled by
    // `bin/backfill_slack_oauth_tokens`) and a hard `Err` are both treated as
    // "this workspace is connected but not yet capturing" rather than
    // refusing the whole page — `derive_status`'s callers already render a
    // degraded-but-connected state for other soft Slack API failures.
    // ROW on control, KEY on the request's own plane (org-plane DEK fix).
    // This route runs behind the binding gate, so the decision already
    // carries the fingerprint-verified tenant pool — no routing lookup, and
    // no chance of disagreeing with the plane `tenant_pool` above resolved.
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), &control_pool);
    let bot_token = match crate::workers::huddle::get_slack_bot_token_for_workspace(
        &control_pool,
        &dek_plane,
        account_id,
        &workspace_id,
    )
    .await
    {
        Ok(Some(token)) => token,
        Ok(None) => {
            tracing::warn!(
                account_id = %account_id,
                workspace_id = %workspace_id,
                context = context,
                connector_health = true,
                "slack_integrations row has no matching oauth_tokens bot token yet — treating as \
                 not-yet-capturing rather than failing the request"
            );
            String::new()
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                workspace_id = %workspace_id,
                context = context,
                "failed to resolve Slack bot token from oauth_tokens"
            );
            String::new()
        }
    };

    Ok(ConnectedSlackWorkspace {
        control_pool,
        tenant_pool,
        plane,
        row: Some(ConnectedWorkspaceRow { workspace_id, workspace_name, bot_token }),
    })
}

/// Body for [`request_backfill`]: the containers this member picked, plus an
/// optional window.
#[derive(Deserialize)]
pub struct BackfillRequestBody {
    /// Slack channel ids the caller is offering to backfill. ONE request
    /// carries all of them, deliberately — a per-row request would be an
    /// O(rows) fan-out, which `scripts/check_no_unbounded_fanout.sh` forbids
    /// and which someone in forty channels would feel immediately.
    pub channel_ids: Vec<String>,
    /// Days of history to ask the platform for. Absent means "as much as the
    /// platform still keeps". Either way `connectors::history_backfill` caps
    /// it at `HISTORY_BACKFILL_MAX_DAYS` (90) on our side, for cost — see
    /// plan §4.1 on stating the window as the PLATFORM's rather than ours.
    #[serde(default)]
    pub window_days: Option<u32>,
}

/// At most this many containers per request. Someone in forty channels who
/// picks "all" is forty durable jobs; the engine paces the work, but the
/// REQUEST still has to be bounded or one click can queue an unbounded amount
/// of Slack API traffic in the caller's name.
const MAX_BACKFILL_CONTAINERS_PER_REQUEST: usize = 25;

/// `POST /v1/connectors/slack/backfill`
///
/// The backfill offer's accept path (chat-per-user plan §4.2). Queues one
/// durable `JobKind::HistoryBackfill` per chosen container, each writing into
/// **exactly one ocean: the caller's own**.
///
/// **Why the caller's identity comes from Clerk and not from Slack.** The
/// target user is `user.user_id` off the authenticated session — this route
/// sits behind `require_auth`, so the person is *proved*, not vouched for.
/// That is the same distinction contract capability C16 draws and invariant
/// I11 rests on, and it is precisely why this offer lives in the web app
/// rather than as a Slack button: a Slack interaction would only ever carry
/// Slack's word about who clicked.
///
/// **Slack membership is still checked, and the load-bearing half is checked
/// FRESH.** Clerk proves who is asking; it says nothing about which Slack
/// channels they are in. Pulling history from a channel the caller is not a
/// member of would be a read they were never entitled to, so every requested
/// container must appear in BOTH lists — and the two are not equally
/// sensitive:
///
/// * **The CALLER's own `users.conversations` list is the authorization
///   basis**, so it passes `force_fresh: true`. A 45-second-stale answer is
///   fine for rendering a count and is not a basis for granting a read
///   (CWE-613; see `connectors::slack_onboarding::cached_member_channels`'
///   own doc comment). Someone removed from a channel seconds ago must not be
///   able to pull its history.
/// * **The BOT's `conversations.list` is not an entitlement check** — it
///   answers "is there anything here to read at all", since the agent must be
///   in the channel for us to have a token that can read it. Stale by up to
///   45 seconds is harmless there: the worst case is queueing a job that
///   finds nothing, and Slack refuses the read anyway if the bot really did
///   leave. Hence no `force_fresh` parameter on that one.
///
/// Partial success is a real outcome and is reported as one: a container the
/// caller cannot back up is skipped WITH ITS REASON rather than failing the
/// whole request, because the most common cause is a stale page listing a
/// channel the person has since left.
pub async fn request_backfill(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<BackfillRequestBody>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }

    if body.channel_ids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "pick at least one channel to bring in"})),
        )
            .into_response();
    }
    if body.channel_ids.len() > MAX_BACKFILL_CONTAINERS_PER_REQUEST {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!(
                    "too many channels in one request ({} > {}) — bring them in a few at a time",
                    body.channel_ids.len(),
                    MAX_BACKFILL_CONTAINERS_PER_REQUEST
                )
            })),
        )
            .into_response();
    }

    let resolved = match resolve_connected_slack_workspace(
        &state,
        &tenant,
        user.account_id,
        "slack backfill request",
    )
    .await
    {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let Some(row) = resolved.row else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no Slack workspace is connected"})),
        )
            .into_response();
    };

    // The caller's CONFIRMED Slack identity. Without one we cannot ask Slack
    // which channels they are in, and we will not guess — an unlinked member
    // gets a clear 409 rather than a silent empty result that would read as
    // "none of your channels qualify".
    let link = match crate::connectors::slack_onboarding::fetch_member_link(
        &resolved.tenant_pool,
        &row.workspace_id,
        user.user_id,
    )
    .await
    {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user.account_id, "backfill: member link lookup failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    let Some(slack_user_id) = link.slack_user_id.filter(|_| link.linked) else {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "we haven't matched your CoralSwarm account to a Slack member yet, so we \
                          can't confirm which channels are yours to bring in"
            })),
        )
            .into_response();
    };

    // Both memberships, both fresh. An `Err` on either is a refusal, not an
    // empty list: "we could not check" must never render as "you are in none
    // of them" — the same unavailable-is-not-zero rule the onboarding
    // payload's three-way state exists for.
    let mine = match crate::connectors::slack_onboarding::cached_member_channels(
        &state.http_client,
        user.account_id,
        &row.workspace_id,
        &row.bot_token,
        &slack_user_id,
        true,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "backfill: users.conversations lookup failed");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "couldn't check your Slack channels just now — try again shortly"})),
            )
                .into_response();
        }
    };
    let bot = match crate::connectors::slack_onboarding::cached_bot_channels(
        &state.http_client,
        &resolved.control_pool,
        user.account_id,
        &row.workspace_id,
        &row.bot_token,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "backfill: bot conversations.list lookup failed");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "couldn't check which channels the bot is in just now — try again shortly"})),
            )
                .into_response();
        }
    };
    let mine_set: std::collections::HashSet<&str> = mine.iter().map(String::as_str).collect();
    let bot_set: std::collections::HashSet<&str> = bot.iter().map(|(id, _)| id.as_str()).collect();

    // The ONE ocean every queued job writes into. Resolved here, once, from
    // the authenticated caller — never re-derived by the worker, which has no
    // request context to bind it to.
    let target_graph_id = match resolve_primary_graph(
        &resolved.tenant_pool,
        user.account_id,
        resolved.plane,
        IngestCaller::Attributable(user.user_id),
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user.account_id, "backfill: no target ocean");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    let clerk_org_id: Option<String> = match resolved.plane {
        AccountPlane::Personal => None,
        AccountPlane::Org => sqlx::query_scalar::<_, Option<String>>(
            "SELECT clerk_org_id FROM accounts WHERE id = $1",
        )
        .bind(user.account_id)
        .fetch_optional(&resolved.control_pool)
        .await
        .ok()
        .flatten()
        .flatten(),
    };
    if resolved.plane == AccountPlane::Org && clerk_org_id.is_none() {
        // Refuse rather than fall through to a personal-plane ingest. I8:
        // "silently writing an org's data onto the shared plane is the worst
        // outcome in the system".
        tracing::error!(account_id = %user.account_id, connector_health = true,
            "backfill: org account with no clerk_org_id — refusing to queue");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response();
    }

    let mut queued: Vec<Value> = Vec::new();
    let mut skipped: Vec<Value> = Vec::new();
    for channel_id in body.channel_ids {
        if !mine_set.contains(channel_id.as_str()) {
            skipped.push(json!({"channel_id": channel_id, "reason": "you're not in this channel"}));
            continue;
        }
        if !bot_set.contains(channel_id.as_str()) {
            skipped
                .push(json!({"channel_id": channel_id, "reason": "the bot isn't in this channel"}));
            continue;
        }
        let idempotency_key = format!(
            "backfill:slack:{}:{}:{}",
            row.workspace_id, channel_id, user.user_id
        );
        let job = crate::jobs::JobKind::HistoryBackfill {
            provider: "slack".to_string(),
            account_id: user.account_id,
            workspace_id: row.workspace_id.clone(),
            container_id: channel_id.clone(),
            target_graph_id,
            target_user_id: user.user_id,
            clerk_org_id: clerk_org_id.clone(),
            window_days: body.window_days,
        };
        match crate::jobs::queue::enqueue(
            &resolved.tenant_pool,
            &job,
            Some(user.account_id),
            Some(&idempotency_key),
            0,
        )
        .await
        {
            // `Ok(None)` = an identical job is already pending. That is a
            // success from the caller's point of view (their history IS on
            // its way), so it is reported as queued rather than as an error
            // they would reasonably retry into a third duplicate.
            Ok(_) => queued.push(json!({"channel_id": channel_id})),
            Err(e) => {
                tracing::warn!(error = %e, channel_id = %channel_id, "backfill: enqueue failed");
                skipped.push(json!({"channel_id": channel_id, "reason": "couldn't queue this one"}));
            }
        }
    }

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "queued": queued,
            "skipped": skipped,
            // Said once, here, so the UI never has to invent it: the window
            // belongs to Slack, not to us.
            "window": "everything this workspace still keeps, up to 90 days",
        })),
    )
        .into_response()
}

/// `GET /v1/connectors/slack/onboarding`
///
/// The ONE server-derived Slack onboarding status payload —
/// docs/connectors/slack-onboarding.md §3. Retracts the false "CoralSwarm
/// will start ingesting from your workspace" banner (§1): `connected` and
/// "actually capturing" are kept visibly distinct all the way to this JSON.
///
/// Rendered here as JSON for the in-app `/connectors` checklist, and by
/// `routes::slack_events`' `app_home_opened` handler as Block Kit for the
/// Slack App Home tab — both call
/// `connectors::slack_onboarding::derive_status`, so the two surfaces can
/// never independently drift on "is this set up" (§3's whole reason for
/// being one contract instead of two computed checklists).
///
/// Same gate as every other route in this file
/// (`reject_org_slack_management`): connectors are personal-workspace-only in
/// v1 unless `ORG_SLACK_OAUTH_ENABLED` is lifted.
pub async fn onboarding_status(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }

    // Pools, plane and the one connected workspace — shared verbatim with
    // `channel_management` (see `resolve_connected_slack_workspace`'s doc
    // comment for why the two copies of this had to become one).
    let resolved =
        match resolve_connected_slack_workspace(&state, &tenant, user.account_id, "onboarding status")
            .await
        {
            Ok(r) => r,
            Err(resp) => return resp,
        };
    let ConnectedSlackWorkspace { control_pool, tenant_pool, plane, row } = resolved;

    // Who is asking. `UserContext::role` is already live-re-checked against
    // `account_members` by `require_auth` on every request — see
    // `channel_management`'s doc comment for the full argument against
    // querying it again here. A personal account's role is always "owner",
    // so `Viewer::Member` is unreachable off the personal plane.
    let viewer = if plane == AccountPlane::Org && user.role == "member" {
        crate::connectors::slack_onboarding::Viewer::Member { user_id: user.user_id }
    } else {
        crate::connectors::slack_onboarding::Viewer::Account { role: &user.role }
    };

    let Some(row) = row else {
        // Not connected — nothing downstream to check. This is the
        // retraction §1 exists for: no workspace, so no "capturing" claim of
        // any kind, live or cached. (The other caller of the shared lookup,
        // `channel_management`, answers 404 for the same `None` — which is
        // exactly why the helper returns the `Option` instead of deciding.)
        //
        // `viewer` still goes in: this is the payload the connectors page
        // reads to decide between a Connect button and "an org admin needs
        // to connect this", and that decision only exists before anything is
        // connected.
        return (
            StatusCode::OK,
            Json(crate::connectors::slack_onboarding::SlackOnboardingStatus::not_connected(
                plane.into(),
                viewer,
            )),
        )
            .into_response();
    };

    let ConnectedWorkspaceRow { workspace_id, workspace_name, bot_token } = row;

    match crate::connectors::slack_onboarding::derive_status(
        &control_pool,
        &tenant_pool,
        &state.http_client,
        user.account_id,
        plane,
        viewer,
        &workspace_id,
        &workspace_name,
        &bot_token,
    )
    .await
    {
        Ok(status) => (StatusCode::OK, Json(status)).into_response(),
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %user.account_id,
                "failed to derive Slack onboarding status"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

// ── Per-channel management ───────────────────────────────────────────────────

/// `GET /v1/connectors/slack/channels`
///
/// The read-only per-channel Slack management payload behind a connector
/// detail page — "what is CoralSwarm actually reading, where does each
/// channel land, and are huddles being saved there"
/// (`connectors::slack_channel_management::derive_channel_management`).
/// Shares `onboarding_status`'s "unavailable is not empty" rule and its
/// live-`conversations.list`-not-`slack_watched_channels` row-set choice —
/// see that module's header for the full rationale, not re-argued here.
///
/// Same gate as every other route in this file
/// (`reject_org_slack_management`): connectors are personal-workspace-only in
/// v1 unless `ORG_SLACK_OAUTH_ENABLED` is lifted.
///
/// **`viewer_role` comes from `UserContext::role`, not a fresh
/// `account_members` query.** `require_auth`'s org branch already resolves
/// this with a LIVE re-check against `account_members`
/// (`live_org_membership`, in `middleware::auth`) on every request — a
/// demotion or removal is effective on the very next call, not merely on
/// token refresh — and defaults to `"owner"` for a personal account (no org,
/// nothing to be demoted from). Querying `account_members` again here would
/// only risk disagreeing with the value `require_auth` already gated the
/// request on, for no fresher an answer.
pub async fn channel_management(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }

    // Same shared lookup `onboarding_status` runs — pools, plane, and the
    // most-recently-connected workspace. See
    // `resolve_connected_slack_workspace`'s doc comment.
    let resolved = match resolve_connected_slack_workspace(
        &state,
        &tenant,
        user.account_id,
        "channel management",
    )
    .await
    {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let ConnectedSlackWorkspace { control_pool, tenant_pool, plane, row } = resolved;

    // Unlike `onboarding_status`'s soft "not connected" 200 (a checklist that
    // has something to say even with nothing connected), this is a detail
    // page FOR a connected workspace — there is nothing to manage without
    // one, so this answers plainly rather than shipping an empty-shaped 200.
    // The shared lookup deliberately does not choose between the two.
    let Some(row) = row else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no Slack workspace connected"})),
        )
            .into_response();
    };

    let ConnectedWorkspaceRow { workspace_id, workspace_name, bot_token } = row;

    match crate::connectors::slack_channel_management::derive_channel_management(
        &control_pool,
        &tenant_pool,
        &state.http_client,
        user.account_id,
        plane,
        &user.role,
        user.user_id,
        &workspace_id,
        &workspace_name,
        &bot_token,
    )
    .await
    {
        Ok(payload) => (StatusCode::OK, Json(payload)).into_response(),
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %user.account_id,
                "failed to derive Slack channel management payload"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

// ── Per-channel management: write half (wt-slack-actions) ───────────────────
//
// `PUT .../destination`, `DELETE .../channels/{id}`, `GET .../impact` — the
// mutating siblings of `channel_management` above. Business logic lives in
// `connectors::slack_channel_actions`; see that module's header for the
// contract-I2 contradiction ACTION 1 deliberately commits, the ordering
// invariant ACTION 2 enforces, and why the admin check / auto-seat write are
// reused from `routes::slack_interactivity` rather than re-implemented here.

/// A destination pick submitted to [`set_channel_destination`]. Internally
/// tagged on `kind` so the wire shape matches the brief exactly:
/// `{"kind":"single","graph_id":"…"}` / `{"kind":"fanout"}` /
/// `{"kind":"default"}`.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DestinationPickBody {
    Single { graph_id: Uuid },
    Fanout,
    Default,
}

impl From<DestinationPickBody> for crate::connectors::slack_channel_actions::DestinationPick {
    fn from(body: DestinationPickBody) -> Self {
        use crate::connectors::slack_channel_actions::DestinationPick;
        match body {
            DestinationPickBody::Single { graph_id } => DestinationPick::Single(graph_id),
            DestinationPickBody::Fanout => DestinationPick::Fanout,
            DestinationPickBody::Default => DestinationPick::Default,
        }
    }
}

/// Render a [`crate::connectors::slack::ChannelDestination`] for the JSON
/// response — shared by [`set_channel_destination`] and [`channel_impact`]
/// so the two endpoints can never describe the same destination shape
/// differently.
async fn destination_json(
    tenant_pool: &sqlx::PgPool,
    account_id: Uuid,
    destination: &crate::connectors::slack::ChannelDestination,
) -> Value {
    use crate::connectors::slack::ChannelDestination;
    match destination {
        ChannelDestination::Single(graph_id) => {
            let ocean_name = crate::connectors::slack_channel_actions::graph_name(tenant_pool, account_id, *graph_id).await;
            json!({"kind": "single", "graph_id": graph_id, "ocean_name": ocean_name})
        }
        ChannelDestination::FanOut { graphs, unmapped_members, .. } => {
            json!({
                "kind": "fanout",
                "seated_oceans": graphs.len(),
                "unmapped_members": unmapped_members,
            })
        }
    }
}

fn set_destination_error_response(e: crate::connectors::slack_channel_actions::SetDestinationError) -> axum::response::Response {
    use crate::connectors::slack_channel_actions::SetDestinationError as E;
    match e {
        E::GraphNotOwned => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "that ocean does not exist or does not belong to your account"})),
        )
            .into_response(),
        E::OrgOcean => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "the org ocean is never a valid Slack destination"})),
        )
            .into_response(),
        E::NotYourOcean => (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "you can only route a channel directly into your own ocean"})),
        )
            .into_response(),
        E::TeamNotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "that team ocean no longer exists"})),
        )
            .into_response(),
        E::NotTeamAdmin { name } => (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": format!(
                    "we couldn't confirm you administer \"{name}\" — ask a team admin to make this pick"
                )
            })),
        )
            .into_response(),
        E::FanoutRequiresOrgPlane => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "\"everyone's individual oceans\" is only available in an organization workspace"
            })),
        )
            .into_response(),
        E::Db(msg) => {
            tracing::warn!(error = %msg, "set_channel_destination failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

/// `PUT /v1/connectors/slack/channels/{channel_id}/destination`
///
/// Change (or clear) a Slack channel's destination for FUTURE content —
/// wt-slack-actions ACTION 1. **Never moves, re-tags, or re-embeds content
/// already captured** — see
/// `connectors::slack_channel_actions::upsert_channel_destination`'s doc
/// comment for the founder decision this deliberately-I2-breaking write
/// records.
///
/// Body picks exactly one of:
/// * `{"kind":"single","graph_id":"…"}` — one specific ocean. `404` if
///   `graph_id` does not belong to this account; `400` if it is the org
///   ocean (never a valid Slack destination — docs/connectors/slack.md §2);
///   `403` if it is another member's private ocean, or if it is a team
///   ocean the caller does not administer (the SAME admin check the Slack
///   Block-Kit picker uses — `check_team_ocean_admin`, never
///   re-implemented). Routing into an EXISTING team ocean auto-seats that
///   channel's current members exactly as the picker does; a seat-write
///   failure is reported in `seat_result` and does NOT undo the routing
///   change.
/// * `{"kind":"fanout"}` — "everyone's individual oceans". `400` outside an
///   organization workspace.
/// * `{"kind":"default"}` — delete the routing row; the channel falls back
///   to the account-level default.
///
/// Same gate as every other route in this file
/// (`reject_org_slack_management`).
/// The write half of the 2026-08-04 role-scoping decision: org admins act on
/// any bot channel; a member acts only on channels their CONFIRMED Slack
/// identity is in. Returns `Some(response)` when the caller must be refused.
///
/// Resolution goes through the SAME
/// `slack_channel_management::resolve_member_channel_scope` the read side
/// uses, so a member can act on exactly the rows their page shows — never a
/// row they cannot see, never a hidden 403 on a row they can. Fail-closed on
/// both non-answers: unlinked → 403 naming the fix, lookup failure → 503
/// (retryable), never "couldn't check, allow it".
#[allow(clippy::too_many_arguments)]
async fn refuse_out_of_scope_member(
    http: &reqwest::Client,
    tenant_pool: &sqlx::PgPool,
    account_id: Uuid,
    plane: AccountPlane,
    viewer_role: &str,
    caller_user_id: Uuid,
    workspace_id: &str,
    bot_token: &str,
    channel_id: &str,
) -> Option<axum::response::Response> {
    use crate::connectors::slack_channel_management::MemberChannelScope;
    let scope = match crate::connectors::slack_channel_management::resolve_member_channel_scope(
        http,
        tenant_pool,
        account_id,
        plane,
        viewer_role,
        caller_user_id,
        workspace_id,
        bot_token,
        // Fresh, never cached: write authorization must reflect a membership
        // revocation immediately, not after the display cache's TTL.
        true,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "member channel scope resolution failed");
            return Some(
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response(),
            );
        }
    };
    match scope {
        MemberChannelScope::Unrestricted => None,
        MemberChannelScope::MemberChannels(mine) if mine.contains(channel_id) => None,
        MemberChannelScope::MemberChannels(_) => Some(
            (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "you can manage only channels you're in — an org admin can manage any channel"
                })),
            )
                .into_response(),
        ),
        MemberChannelScope::Unlinked => Some(
            (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "link your Slack identity first — once linked, you can manage the channels you're in"
                })),
            )
                .into_response(),
        ),
        MemberChannelScope::Unavailable(e) => {
            tracing::warn!(error = %e, %account_id, "member channel lookup unavailable during write gate");
            Some(
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "error": "we couldn't confirm which channels you're in — try again in a moment"
                    })),
                )
                    .into_response(),
            )
        }
    }
}

pub async fn set_channel_destination(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(channel_id): Path<String>,
    Json(body): Json<DestinationPickBody>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }

    let resolved = match resolve_connected_slack_workspace(
        &state,
        &tenant,
        user.account_id,
        "set channel destination",
    )
    .await
    {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let ConnectedSlackWorkspace { control_pool, tenant_pool, plane, row } = resolved;
    let Some(row) = row else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no Slack workspace connected"})),
        )
            .into_response();
    };
    let ConnectedWorkspaceRow { workspace_id, bot_token, .. } = row;

    if let Some(refuse) = refuse_out_of_scope_member(
        &state.http_client,
        &tenant_pool,
        user.account_id,
        plane,
        &user.role,
        user.user_id,
        &workspace_id,
        &bot_token,
        &channel_id,
    )
    .await
    {
        return refuse;
    }

    let result = crate::connectors::slack_channel_actions::set_channel_destination(
        &control_pool,
        &tenant_pool,
        &state.http_client,
        user.account_id,
        plane,
        user.user_id,
        &workspace_id,
        &channel_id,
        &bot_token,
        body.into(),
    )
    .await;

    match result {
        Ok(outcome) => {
            let destination = destination_json(&tenant_pool, user.account_id, &outcome.destination).await;
            let seat_result = outcome.seat_result.map(|r| match r {
                Ok((seated, unresolved)) => json!({"seated": seated, "unresolved": unresolved}),
                Err(e) => json!({"error": e}),
            });
            (
                StatusCode::OK,
                Json(json!({"destination": destination, "seat_result": seat_result})),
            )
                .into_response()
        }
        Err(e) => set_destination_error_response(e),
    }
}

/// `DELETE /v1/connectors/slack/channels/{channel_id}`
///
/// Stop watching a channel — wt-slack-actions ACTION 2. The bot actually
/// LEAVES the channel (`conversations.leave`), and the routing row is
/// deleted only after that succeeds (or the bot was already out of the
/// channel) — see
/// `connectors::slack_channel_actions::stop_watching_channel_after_leave`'s
/// doc comment for why that ordering is load-bearing: a row deleted while
/// the bot is still in the channel is worse than doing nothing.
///
/// `missing_scope` (a workspace installed before `channels:leave` was added
/// to `SLACK_SCOPES`) answers `409` naming the fix — a reinstall — rather
/// than a generic failure; any other Slack-side failure leaves the row
/// intact and answers `502`.
///
/// Same gate as every other route in this file
/// (`reject_org_slack_management`).
pub async fn stop_watching_channel(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(channel_id): Path<String>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }

    let resolved = match resolve_connected_slack_workspace(
        &state,
        &tenant,
        user.account_id,
        "stop watching channel",
    )
    .await
    {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let ConnectedSlackWorkspace { tenant_pool, plane, row, .. } = resolved;
    let Some(row) = row else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no Slack workspace connected"})),
        )
            .into_response();
    };
    let ConnectedWorkspaceRow { workspace_id, bot_token, .. } = row;

    if let Some(refuse) = refuse_out_of_scope_member(
        &state.http_client,
        &tenant_pool,
        user.account_id,
        plane,
        &user.role,
        user.user_id,
        &workspace_id,
        &bot_token,
        &channel_id,
    )
    .await
    {
        return refuse;
    }

    match crate::connectors::slack_channel_actions::stop_watching_channel(
        &state.http_client,
        &bot_token,
        &tenant_pool,
        user.account_id,
        &workspace_id,
        &channel_id,
    )
    .await
    {
        Ok(outcome) => {
            let already_left = outcome.left == crate::connectors::slack::LeaveOutcome::AlreadyLeft;
            (
                StatusCode::OK,
                Json(json!({
                    "left": true,
                    "already_left": already_left,
                    "watch_row_deleted": outcome.watch_row_deleted,
                })),
            )
                .into_response()
        }
        Err(e) => {
            use crate::connectors::slack_channel_actions::StopWatchingError as E;
            match e {
                E::MissingScope => (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "error": "this workspace was installed before CoralSwarm could leave a \
                                  channel on request — reconnect Slack (an admin needs to reinstall \
                                  the app) before \"stop watching\" will work here"
                    })),
                )
                    .into_response(),
                E::Slack(msg) => {
                    tracing::warn!(error = %msg, %channel_id, "conversations.leave Slack error");
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(json!({
                            "error": "Slack couldn't remove the bot from that channel — nothing was \
                                      changed here; try again shortly"
                        })),
                    )
                        .into_response()
                }
                E::Transport(msg) => {
                    tracing::warn!(error = %msg, %channel_id, "conversations.leave transport error");
                    (StatusCode::BAD_GATEWAY, Json(json!({"error": msg}))).into_response()
                }
                E::Db(msg) => {
                    tracing::warn!(error = %msg, %channel_id, "stop_watching_channel db error");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    )
                        .into_response()
                }
            }
        }
    }
}

#[derive(Deserialize)]
pub struct ChannelImpactQuery {
    target_graph_id: Option<Uuid>,
}

/// `GET /v1/connectors/slack/channels/{channel_id}/impact`
///
/// Real numbers for the confirmation dialog in front of EITHER action —
/// always the channel's CURRENT destination and how many atoms already live
/// there ("the N corals already captured stay in `<ocean>`" — never moved by
/// either action). Optionally, `?target_graph_id=<uuid>` previews a
/// PROSPECTIVE team-ocean pick as `prospective_team_seats: {"identified": N,
/// "total": M}`, computed via the exact same roster matcher the seating
/// write itself uses
/// (`connectors::slack::resolve_channel_members`) — so this number and the
/// number a subsequent `PUT .../destination` call would actually seat can
/// never drift apart. Omitted (not zeroed) when `target_graph_id` does not
/// resolve to a `kind = 'team'` graph in this account.
///
/// Same gate as every other route in this file
/// (`reject_org_slack_management`).
pub async fn channel_impact(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(channel_id): Path<String>,
    Query(query): Query<ChannelImpactQuery>,
) -> impl IntoResponse {
    if let Some(reject) = reject_org_slack_management(&state, &tenant).await {
        return reject.into_response();
    }

    let resolved =
        match resolve_connected_slack_workspace(&state, &tenant, user.account_id, "channel impact").await {
            Ok(r) => r,
            Err(resp) => return resp,
        };
    let ConnectedSlackWorkspace { control_pool, tenant_pool, plane, row } = resolved;
    let Some(row) = row else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no Slack workspace connected"})),
        )
            .into_response();
    };
    let ConnectedWorkspaceRow { workspace_id, bot_token, .. } = row;

    match crate::connectors::slack_channel_actions::channel_impact(
        &control_pool,
        &tenant_pool,
        &state.http_client,
        user.account_id,
        plane,
        &workspace_id,
        &channel_id,
        &bot_token,
        query.target_graph_id,
    )
    .await
    {
        Ok(impact) => {
            let current_destination = destination_json(&tenant_pool, user.account_id, &impact.current_destination).await;
            let prospective_team_seats = impact.prospective_team_seats.map(|r| match r {
                Ok((identified, total)) => json!({"identified": identified, "total": total}),
                Err(e) => json!({"error": e}),
            });
            (
                StatusCode::OK,
                Json(json!({
                    "atom_count": impact.atom_count,
                    "current_destination": current_destination,
                    "prospective_team_seats": prospective_team_seats,
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, %channel_id, "failed to derive Slack channel impact");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

// ── Internal ingest helper ────────────────────────────────────────────────────

/// Whether a caller of [`resolve_primary_graph`] can be attributed to one
/// specific, currently-authenticated org member (member-private-oceans PR 5,
/// plan §3 "Connector ingest, Stage 1").
///
/// Deliberately a distinct type from any `user_id: Uuid` a caller might
/// already be carrying for OTHER reasons — e.g. `teams_ingest::install_watch`'s
/// bookkeeping `user_id` column, which for the `teams_bot.rs` webhook path is
/// `account_members ORDER BY joined_at LIMIT 1`: an ARBITRARY member, picked
/// only because that signature-authenticated webhook has no session at all.
/// A bare `Uuid` cannot distinguish "this IS the acting user, Clerk-JWT-
/// verified" from "this is some member we found nearby" — and the latter, fed
/// into the owner-scoped member-ocean query below, would silently land a
/// webhook's content in an arbitrary member's private ocean instead of
/// refusing. Every call site must pick a variant deliberately, not default
/// one from whatever `Uuid` happens to be in scope.
/// Which plane an account is on — personal (consumer) or org (tenant) —
/// as [`resolve_primary_graph`] needs it to pick a resolution strategy.
///
/// Org-tenancy plane split, step 0: `resolve_primary_graph` used to derive
/// this itself with `SELECT clerk_org_id IS NOT NULL FROM accounts WHERE id
/// = $1` — a query against `accounts`, a CONTROL-ONLY table that does not
/// exist on an org tenant pool (`relation "accounts" does not exist` the
/// moment this function is ever handed one). Every caller already knows
/// (or can cheaply determine) which plane it is on — most have already run
/// their own `reject_org_*_access` gate, or already hold an `is_org` bool
/// from a check they run anyway — so the function is now plane-EXPLICIT:
/// the caller passes this in, and `resolve_primary_graph` never touches
/// `accounts` at all. See [`account_plane`] for the shared lookup helper a
/// caller that does not already know its plane can use to derive one,
/// byte-identical to what this function used to do internally.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AccountPlane {
    Personal,
    Org,
}

impl AccountPlane {
    /// The plane a worker already knows from its tenant: an org tenant's
    /// accounts are org accounts. A consumer-plane caller that may be
    /// serving a not-yet-migrated org account uses [`account_plane`].
    pub(crate) fn for_tenant(tenant: &crate::tenancy::TenantId) -> Self {
        match tenant {
            crate::tenancy::TenantId::Consumer => Self::Personal,
            crate::tenancy::TenantId::Org(_) => Self::Org,
        }
    }
}

/// The `graphs` predicates that name a primary ocean, written once. Every
/// resolver and the ingest door's grant re-check (`ingest::grant`) bind the
/// same fragment, so a widening lands on every surface at once or not at
/// all: a kind-only widening of [`primary_ocean::MEMBER`] is the
/// cross-member leak [`resolve_org_member_primary_graph`] warns about.
pub(crate) mod primary_ocean {
    /// A personal account's primary ocean (`resolve_primary_graph`'s
    /// personal arm, ADR-0006 / migration 0026). The caller binds
    /// `account_id`.
    pub(crate) const PERSONAL: &str = "is_primary = true";
    /// A member's own primary ocean under an org account
    /// (`resolve_org_member_primary_graph`). The caller binds `account_id`
    /// and `owner_user_id`.
    pub(crate) const MEMBER: &str = "kind = 'member' AND archived_at IS NULL AND is_primary = true";
}

/// Current membership of an account, written once. `account_members` is a
/// control-plane table; the caller picks the executor (a scoped control
/// transaction, the control pool, or on the consumer plane the capture's own
/// transaction) and so decides which snapshot the answer comes from.
pub(crate) mod membership {
    use uuid::Uuid;

    /// Is `user_id` a current member of `account_id`? Binds `account_id`,
    /// then `user_id`.
    pub(crate) const IS_CURRENT_MEMBER: &str =
        "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)";

    /// [`IS_CURRENT_MEMBER`] on `exec`. A lookup error is returned, never
    /// folded into either answer.
    pub(crate) async fn is_current_member<'e>(
        exec: impl sqlx::PgExecutor<'e>,
        account_id: Uuid,
        user_id: Uuid,
    ) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar(IS_CURRENT_MEMBER).bind(account_id).bind(user_id).fetch_one(exec).await
    }
}

/// Look up which plane `account_id` is on (`accounts.clerk_org_id IS NOT
/// NULL`) — the exact query [`resolve_primary_graph`] used to run
/// internally before it became plane-explicit (see [`AccountPlane`]'s doc
/// comment). For callers that do not already know their plane from a
/// request-time gate or an existing check.
///
/// `pool` must be a CONTROL/consumer-plane pool — `accounts` does not exist
/// on an org tenant pool, so a caller that already knows it is running
/// against a tenant pool must not call this; it must already know its
/// plane by construction instead.
///
/// A missing `accounts` row is an `Err`, deliberately never collapsed into
/// `AccountPlane::Personal` via `unwrap_or(false)` — that would conflate
/// "this is a consumer account" with "this account does not exist", the
/// same invariant `resolve_primary_graph`'s own doc comment insists on.
pub(crate) async fn account_plane(
    pool: &sqlx::PgPool,
    account_id: Uuid,
) -> Result<AccountPlane, sqlx::Error> {
    let is_org: bool =
        sqlx::query_scalar("SELECT clerk_org_id IS NOT NULL FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(crate::tenancy::scope::request_read(pool))
            .await?
            .ok_or_else(|| {
                sqlx::Error::Protocol(format!(
                    "account_plane: no accounts row for {account_id}; refusing to guess \
                     personal-vs-org"
                ))
            })?;
    Ok(if is_org {
        AccountPlane::Org
    } else {
        AccountPlane::Personal
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum IngestCaller {
    /// A request carrying a verified `user_ctx.user_id` — Clerk JWT
    /// authenticated, with current org membership already re-confirmed by
    /// the binding gate before this code runs. Under an org account, ingest
    /// resolves to THIS user's own private member ocean.
    Attributable(Uuid),
    /// A workspace/webhook-resolved account with no bound user identity: the
    /// Slack Events API funnel (`slack_events.rs`), Microsoft Graph change
    /// notifications (`graph_webhook.rs`), the Teams bot presence webhook
    /// (`teams_bot.rs`), and huddle-transcript ingest (`workers/huddle.rs`)
    /// all discover only an `account_id` — never a caller. Under an org
    /// account there is no single member this content can be attributed to
    /// (Stage 2 shared-channel fan-out — plan §3/§4 — is not implemented: no
    /// channel-membership snapshot exists anywhere in this codebase), so this
    /// refuses rather than guessing one.
    NonAttributable,
}

/// Resolve the account's PRIMARY private ocean — the ocean model's designated
/// default destination for ingest (`graphs.is_primary`, migration 0026 /
/// ADR-0006) — **on whichever plane the caller is on** (member-private-oceans
/// PR 5, plan §3/§7 item 5):
///
/// * **Personal account:** byte-identical to pre-PR-5 behavior. Creates the
///   personal primary on the rare path where the account has none yet (the
///   partial unique index `uq_graphs_one_primary_account_wide`, migration
///   0090/0005, keeps it singular; on a race we fall back to re-selecting
///   the winner).
/// * **Org account:** resolves the CALLER's OWN `kind='member'` primary ocean
///   — see [`resolve_org_member_primary_graph`]. Never auto-creates one here:
///   provisioning is the reconciler/first-sight triggers' job
///   (member-private-oceans PR 3/4), not ingest's.
///
/// Shared by Slack message ingest, huddle-transcript ingest, the connector
/// routes, and workflows, so all land in the same place under the same rules.
///
/// `plane` is caller-supplied (org-tenancy plane split, step 0 — see
/// [`AccountPlane`]'s doc comment): this function never queries `accounts`
/// itself, so it works unmodified once `pool` is a genuine org TENANT pool
/// (where `accounts` does not exist at all).
pub(crate) async fn resolve_primary_graph(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    plane: AccountPlane,
    caller: IngestCaller,
) -> Result<Uuid, sqlx::Error> {
    // RLS scoping (org-tenancy conversion; tightened in m365 Train 2 PR 2.5).
    // `graphs` carries a CONSUMER-plane `account_id` policy (migration 0083)
    // AND, on the TENANT plane, migration 0009's OWN `member_ocean_isolation`
    // policy (`id IN (SELECT * FROM tenant_rls_visible_graph_ids())`, whose
    // predicate needs `app.tenant_actor='system'` OR `owner_user_id =
    // tenant_rls_actor_user_id()` — i.e. `app.user_id` — NEITHER of which
    // plain `begin_account_scoped` sets). This function's own doc comment
    // used to call that "harmless" on a genuine tenant pool because "no 0083
    // policy exists there" — true, but incomplete: it never addressed 0009's
    // SEPARATE policy on the SAME table, which DOES exist on the tenant
    // plane and DOES gate this function's own `graphs` read/write. That gap
    // was invisible as long as every caller's own `reject_org_*` gate kept
    // `pool` provably consumer-only for every `AccountPlane::Org` caller
    // (`docs/tenant-rls-cutover-runbook.md` §2.6's whole "exempt" argument
    // for this function) — m365 Train 2 PR 2.5 is exactly the "future
    // change" that document's own §2.6 already named as invalidating that
    // exemption ("must convert the Attributable arm to begin_tenant_scoped
    // and the NonAttributable arm to begin_tenant_system_scoped at that
    // time"), by lifting `routes::teams_ingest.rs`'s org gate and making 3 of
    // this function's 8 call sites (`install_resource`/`ingest_resource`/
    // `sync_transcripts`) genuinely reachable with a real tenant pool.
    //
    // So: Org uses the tenant-aware primitive appropriate to `caller` —
    // `begin_tenant_scoped` (threads the real acting user) for `Attributable`,
    // `begin_tenant_system_scoped` for `NonAttributable` (which
    // `resolve_org_member_primary_graph` refuses outright before touching
    // `graphs` at all anyway — see its own match arm — so the extra GUC is
    // never even consulted there, but setting it uniformly keeps this
    // dispatch a plain match on `plane`/`caller`, not a second copy of that
    // refusal logic living here too). Personal keeps plain
    // `begin_account_scoped`, byte-identical to before — the extra tenant
    // GUCs the Org arm sets are inert on the consumer plane regardless (0083's
    // policy never reads them), so this split is about using the RIGHT
    // primitive for Org, not about Personal needing one.
    let open_tx = async {
        if plane == AccountPlane::Org {
            match caller {
                IngestCaller::Attributable(user_id) => {
                    crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await
                }
                IngestCaller::NonAttributable => {
                    crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await
                }
            }
        } else {
            crate::tenancy::scope::begin_account_scoped(pool, account_id).await
        }
    };
    let mut tx = open_tx
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("resolve_primary_graph scoped tx: {e}")))?;

    // ── Plane check FIRST, before any generic "does a primary already exist"
    // lookup (member-private-oceans PR 5). This ordering is a REQUIRED
    // correctness fix, not a style choice: PR 0's version of this function
    // ran `WHERE account_id = $1 AND is_primary = true LIMIT 1` unconditionally
    // as its very first statement, only consulting the plane inside the
    // "nothing found" fallback. That was safe under PR 0 because an
    // org account could hold AT MOST one `is_primary = true` row account-wide.
    // It stops being safe the moment `kind='member'` rows exist: migration
    // 0090/0005 (plan §2.1) makes `is_primary` a PER-OWNER property, so one org
    // account can legitimately hold MANY `is_primary = true` rows at once — one
    // org-wide (`uq_graphs_one_primary_account_wide`, `owner_user_id IS NULL`,
    // e.g. the org commons ocean) plus one PER MEMBER
    // (`uq_graphs_one_primary_per_owner`). Left in its old position, that first
    // query would return WHICHEVER of those rows Postgres happens to pick —
    // with no ownership filter at all — the moment any org account has more
    // than one primary. That is not merely "the kind check alone is not
    // sufficient" (plan §3's own warning); a kind check would never even run,
    // because this function returned before reaching it. So the org/personal
    // split has to happen before that query is issued, not inside its
    // "nothing found" branch.
    //
    // Org-tenancy plane split, step 0: `plane` is now caller-supplied (see
    // [`AccountPlane`]'s doc comment) instead of derived here via a
    // `SELECT clerk_org_id IS NOT NULL FROM accounts WHERE id = $1` lookup —
    // `accounts` is a CONTROL-ONLY table and does not exist on an org tenant
    // pool. Every caller either already knows its plane from a request-time
    // `reject_org_*_access` gate, or derives one itself via [`account_plane`]
    // (byte-identical to what this function used to do internally, including
    // the "no such `accounts` row is an `Err`, never `unwrap_or(false)`"
    // refusal) — so this function no longer needs its own `tx`-scoped lookup
    // or its own "unknown account" refusal branch; an unknown `account_id`
    // now fails at the caller's `account_plane` call, before this function is
    // ever reached at all.
    if plane == AccountPlane::Org {
        return resolve_org_member_primary_graph(tx, account_id, caller).await;
    }

    // ── Personal-account path — byte-identical to pre-PR-5 behavior ────────
    if let Some(r) = sqlx::query(&format!(
        "SELECT id FROM graphs WHERE account_id = $1 AND {} LIMIT 1",
        primary_ocean::PERSONAL
    ))
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?
    {
        tx.rollback().await.ok();
        return Ok(r.get("id"));
    }

    let new_id = Uuid::new_v4();
    let inserted = sqlx::query(
        "INSERT INTO graphs (id, account_id, name, kind, is_primary, created_at) \
         VALUES ($1, $2, 'Primary', 'personal', true, now()) \
         ON CONFLICT DO NOTHING \
         RETURNING id",
    )
    .bind(new_id)
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(r) = inserted {
        let id = r.get("id");
        tx.commit().await?;
        return Ok(id);
    }

    // Lost the race to create the primary — re-select the existing one.
    let r = sqlx::query(&format!(
        "SELECT id FROM graphs WHERE account_id = $1 AND {} LIMIT 1",
        primary_ocean::PERSONAL
    ))
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(r.get("id"))
}

/// Org-account branch of [`resolve_primary_graph`] (member-private-oceans
/// PR 5, plan §3/§7 item 5, replacing PR 0's unconditional org hard-fail).
/// `tx` is the SAME account-scoped transaction the caller already opened —
/// this function only ever reads and always ends the transaction itself with
/// a `rollback` (there is nothing to write here: creating a member ocean is
/// the reconciler/first-sight provisioning triggers' job, PR 3/4, and racing
/// them from a read path would risk two different creation policies both
/// racing `uq_graphs_one_primary_per_owner`).
///
/// **The kind check alone is NOT sufficient** — called out explicitly in the
/// plan's §3: the query below constrains ALL of `account_id`,
/// `owner_user_id = <the attributed caller>`, `kind = 'member'`, and
/// `archived_at IS NULL`, plus the `is_primary` pick. A kind-only widening
/// would return ANY member ocean in the account, not necessarily the
/// caller's own — exactly the cross-member leak this feature exists to
/// prevent.
async fn resolve_org_member_primary_graph(
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    caller: IngestCaller,
) -> Result<Uuid, sqlx::Error> {
    let user_id = match caller {
        IngestCaller::Attributable(user_id) => user_id,
        IngestCaller::NonAttributable => {
            tx.rollback().await.ok();
            // Logged as a connector-health warning, not a plain error: under
            // Stage 1 this is the EXPECTED steady state for any org that has
            // Slack (or Outlook/Teams) installed, until Stage 2 shared-channel
            // fan-out ships (plan §8 residual risk — a genuine functional
            // regression, the correct fail-closed choice, not a bug). An
            // operator watching connector health should be able to see this
            // firing, not mistake silence for "it's working."
            tracing::warn!(
                %account_id,
                connector_health = true,
                "resolve_primary_graph: refusing non-attributable workspace ingest under an \
                 org account — no channel-membership snapshot exists yet (member-private-oceans \
                 plan §3 Stage 2 fan-out is not implemented), so this content cannot be \
                 attributed to one member; not landing anywhere"
            );
            return Err(sqlx::Error::Protocol(format!(
                "resolve_primary_graph: account {account_id} is an organization account and this \
                 caller carries no attributable user identity; refusing rather than guessing a \
                 destination (shared-channel fan-out is not yet implemented)"
            )));
        }
    };

    let row = sqlx::query(&format!(
        "SELECT id FROM graphs WHERE account_id = $1 AND owner_user_id = $2 AND {} LIMIT 1",
        primary_ocean::MEMBER
    ))
    .bind(account_id)
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();

    match row {
        Some(r) => Ok(r.get("id")),
        None => {
            // Fires when `MEMBER_OCEANS_ENABLED` is off (no `kind='member'`
            // row is ever created, so this predicate is observably inert
            // until the flag flips — same convention as the reachability
            // predicate in PR 2), or when the flag is on but provisioning
            // (PR 3/4) has not yet caught up for this member. Either way:
            // refuse; this is a read path, not a provisioning trigger.
            tracing::warn!(
                %account_id,
                %user_id,
                connector_health = true,
                "resolve_primary_graph: no primary member ocean found for this member under an \
                 org account (MEMBER_OCEANS_ENABLED off, or provisioning has not caught up yet) \
                 — refusing rather than auto-creating here"
            );
            // A NAMED refusal, not `RowNotFound`. The sibling refusal paths in
            // this function already use `Protocol`, and collapsing this one to
            // the generic "no rows returned" makes the graph_webhook /
            // connectors / workflows logs unreadable at exactly the moment
            // they matter: when provisioning is lagging or the flag is off,
            // an operator sees a bare RowNotFound and cannot tell it apart
            // from any other empty query.
            Err(sqlx::Error::Protocol(format!(
                "resolve_primary_graph: no primary member ocean for user {user_id} under org \
                 account {account_id}; refusing to auto-create (MEMBER_OCEANS_ENABLED off, or \
                 provisioning has not caught up)"
            )))
        }
    }
}

/// Resolve EVERY account that has installed a given Slack workspace
/// (`provider_workspace_id` = the Slack `team_id`), via the
/// `resolve_connector_install_account_ids` SECURITY DEFINER bootstrap
/// function (migration `0121_connector_installs`) — this webhook lookup has
/// only a Slack team_id and must discover the owning account(s) before any
/// scoped connection could be opened.
///
/// Unlike the single-account [`resolve_slack_account_id`] below, this
/// returns EVERY match: two different CoralSwarm accounts can legitimately
/// install the same Slack workspace (two separate customers, or a customer
/// plus someone's personal account), and `docs/plans/org-connectors-and-
/// channel-ingest.md` §9c's inbound router is meant to fan out to each one
/// rather than pick one arbitrarily. Wiring an actual caller to consume the
/// fan-out (rather than the single-account callers below) is the next step,
/// not this migration — this function exists now so that step has something
/// to call.
///
/// Reads `connector_installs`, which (per that migration's backfill) is
/// derived from BOTH `oauth_tokens` and `slack_integrations`, so it sees
/// every install either the modern or the legacy OAuth path recorded — a
/// strict superset of what the OLD `resolve_oauth_account_id` (0084) or
/// `resolve_slack_integration_account_id` (0086) could see alone (0086's own
/// header notes the first would silently orphan a legacy-only install).
/// Ordered by `created_at, account_id` for full determinism, unlike either
/// old function's bare `LIMIT 1` with no `ORDER BY`.
///
/// ── Self-healing fallback (founder decision, org-slack step 1 review) ──
/// `connector_installs` is populated by the transactional install-time write
/// (`routes::oauth::record_connector_install`, called from both OAuth
/// callbacks) and this migration's one-time backfill — see the migration's
/// own "OPERATOR-FACING" header note for the investigation into whether
/// anything else can write `oauth_tokens`/`slack_integrations` (nothing in
/// this codebase does, today). But six independent test fixtures proved how
/// easy it is to create one of those rows by a route that bypasses
/// `record_connector_install`, so this function does not simply trust the
/// index: on a miss, it falls back to the AUTHORITATIVE tables themselves.
///
/// **Correctness comes from the fallback query, not from the repair
/// write that follows it** — every account resolved via the fallback is
/// returned to the caller regardless of whether the repair below succeeds.
/// This deliberately inverts the discipline `record_connector_install`
/// documents for the INSTALL-time write (best-effort there was wrong,
/// because the index was the sole source): here the fallback tables are the
/// source of truth and `connector_installs` is a cache repairing itself, so
/// a failed repair costs a little latency on the NEXT resolve for this
/// workspace and nothing else.
///
/// The fallback itself is multi-row (`resolve_oauth_token_account_ids` /
/// `resolve_slack_integration_account_ids`, both `SETOF` — NOT the existing
/// `LIMIT 1` `resolve_oauth_account_id`/`resolve_slack_integration_account_id`),
/// for the same reason `resolve_connector_install_account_ids` above is:
/// falling back through a `LIMIT 1` resolver would silently reintroduce the
/// arbitrary-pick bug this whole migration exists to kill, only on the one
/// code path where nobody would think to look for it.
///
/// A negative-result cache ([`negative_resolve_cache`]) bounds the fallback
/// query rate for a workspace that genuinely has no installer anywhere (its
/// only way to reach this function is a workspace where CoralSwarm's Slack
/// app is still present — and therefore still delivering signed webhooks —
/// after every local row was deleted, e.g. an incomplete disconnect): every
/// message from that workspace would otherwise re-run both fallback queries
/// forever, since there is nothing to write and therefore nothing that ever
/// lands in `connector_installs` to short-circuit the fast path.
pub(crate) async fn resolve_slack_account_ids(
    pool: &sqlx::PgPool,
    team_id: &str,
) -> Result<Vec<Uuid>, sqlx::Error> {
    let indexed: Vec<Uuid> = sqlx::query_scalar("SELECT resolve_connector_install_account_ids($1, $2)")
        .bind("slack")
        .bind(team_id)
        .fetch_all(pool)
        .await?;
    if !indexed.is_empty() {
        return Ok(indexed);
    }

    if negative_resolve_cache_hit("slack", team_id) {
        // Recently confirmed nowhere — this exact miss was already logged
        // once (below, the first time it was observed); a suppressed
        // repeat is not a new event and does not need its own log line.
        // See this function's own doc comment for why this cache exists.
        return Ok(Vec::new());
    }

    // ── Fallback: query the AUTHORITATIVE tables directly, not the index ──
    // Foundation PR 1.1 (migration 0175, amended): `resolve_oauth_token_
    // account_ids` is pinned to `owner_user_id IS NULL` rows (`connectors::
    // TokenOwner::AccountWide`) — a no-op for Slack, which has no per-member
    // OAuth (§14.1 of the org-connectors plan) and so never has any other
    // kind of row to miss. See that migration's own comment.
    let via_oauth_tokens: Vec<Uuid> =
        sqlx::query_scalar("SELECT resolve_oauth_token_account_ids($1, $2)")
            .bind("slack")
            .bind(team_id)
            .fetch_all(pool)
            .await?;
    let via_slack_integrations: Vec<Uuid> =
        sqlx::query_scalar("SELECT resolve_slack_integration_account_ids($1)")
            .bind(team_id)
            .fetch_all(pool)
            .await?;

    let mut found: Vec<Uuid> = via_oauth_tokens;
    for id in via_slack_integrations {
        if !found.contains(&id) {
            found.push(id);
        }
    }

    if found.is_empty() {
        cache_negative_resolve("slack", team_id);
        tracing::warn!(
            team_id = %team_id,
            connector_health = true,
            "no account found for Slack workspace anywhere — checked connector_installs AND \
             the oauth_tokens/slack_integrations fallback; team_id did not resolve to any \
             account (never installed, fully uninstalled, or the Slack-side app is still \
             present with no matching local row at all)"
        );
        return Ok(found);
    }

    // ── Repair, best-effort — see this function's own doc comment for why a
    // failure here does not affect what is returned above. Distinct log
    // (and distinct wording) from the "found nothing" case: a healed miss
    // and a genuine miss are different events for an operator, per the
    // founder's own framing of this requirement.
    tracing::warn!(
        team_id = %team_id,
        account_ids = ?found,
        connector_health = true,
        "connector_installs self-healed for Slack workspace — found via the oauth_tokens/\
         slack_integrations fallback with no connector_installs row; a route bypassing \
         record_connector_install must have created this install (or this predates migration \
         0120's backfill)"
    );
    for &account_id in &found {
        repair_connector_install_best_effort(pool, account_id, "slack", team_id).await;
    }

    Ok(found)
}

/// Resolve a SINGLE account for a Slack workspace — the shape every existing
/// call site still expects (`slack_events.rs` ×3 plus its own
/// `ingest_slack_message` — the message-ingest funnel, repointed here from
/// `resolve_slack_integration_account_id` in this same PR, since it is the
/// highest-traffic site of all — and `slack_interactivity.rs` ×1; all five
/// kept compiling unchanged by this migration). Backed by
/// [`resolve_slack_account_ids`] above (the genuine multi-row, self-healing
/// resolver), taking the EARLIEST installer when more than one account has
/// connected the same workspace — deterministic (unlike the arbitrary
/// `LIMIT 1` this replaces), but still a single, non-fan-out choice, because
/// none of today's call sites are written to handle more than one account;
/// making them fan out is the next step
/// (`docs/plans/org-connectors-and-channel-ingest.md` §9c), not this one.
///
/// All `connector_health` observability (fast-path hit, self-healed, or
/// genuinely nowhere) is emitted by [`resolve_slack_account_ids`] itself —
/// nothing further to log here.
pub(crate) async fn resolve_slack_account_id(
    pool: &sqlx::PgPool,
    team_id: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    let ids = resolve_slack_account_ids(pool, team_id).await?;
    Ok(ids.into_iter().next())
}

// ── Self-healing fallback support: negative-result cache + repair write ───────

/// Per-process cache of `(provider, workspace_id)` pairs recently confirmed
/// to have NO installer anywhere (checked in `resolve_slack_account_ids`'s
/// fallback and found empty even there) — bounds how often a workspace with
/// no local row at all re-triggers the fallback queries. Same
/// `OnceLock<Mutex<HashMap<..>>>` shape as [`channel_name_cache`] /
/// [`user_name_cache`] below, but with a TTL (a negative result can become
/// stale the moment a real install happens — see the field's own doc comment
/// on [`resolve_slack_account_ids`] for why that staleness window is
/// harmless: the fast `connector_installs` path always runs first and is
/// unaffected by this cache).
fn negative_resolve_cache() -> &'static std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>>
{
    static C: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>>,
    > = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// How long a confirmed-nowhere result suppresses re-querying the fallback
/// tables. Long enough to meaningfully cut query volume during a burst of
/// messages from a dead/uninstalled workspace; short enough that a genuinely
/// new (if unusual) direct-DB install becomes visible again within minutes
/// rather than requiring a process restart.
const NEGATIVE_RESOLVE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

fn negative_resolve_cache_hit(provider: &str, workspace_id: &str) -> bool {
    let key = (provider.to_string(), workspace_id.to_string());
    let cache = negative_resolve_cache().lock().unwrap();
    matches!(cache.get(&key), Some(t) if t.elapsed() < NEGATIVE_RESOLVE_TTL)
}

fn cache_negative_resolve(provider: &str, workspace_id: &str) {
    let key = (provider.to_string(), workspace_id.to_string());
    negative_resolve_cache()
        .lock()
        .unwrap()
        .insert(key, std::time::Instant::now());
}

/// Best-effort repair write for the self-healing fallback path in
/// [`resolve_slack_account_ids`] — NOT the transactional install-time path
/// (`routes::oauth::record_connector_install`'s own doc comment, and
/// `resolve_slack_account_ids`'s, cover why the two have opposite
/// failure-handling disciplines on purpose). Opens its own transaction,
/// calls `record_connector_install`, and commits; any failure at any step is
/// logged and swallowed — the caller already has its answer regardless.
///
/// `external_user_id` is always `None` here: the fallback resolves a set of
/// `account_id`s merged from two tables without tracking which table (or
/// which row) each came from, and `external_user_id` is documented as a
/// purely informational enrichment column (`record_connector_install`'s own
/// doc comment) — not worth a second query per account to populate on a
/// path that is, by construction, rare.
async fn repair_connector_install_best_effort(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    provider: &str,
    external_workspace_id: &str,
) {
    let mut tx = match crate::tenancy::scope::begin_account_scoped(pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                provider = %provider,
                workspace = %external_workspace_id,
                connector_health = true,
                "self-heal: failed to open tx for connector_installs repair (non-fatal — will \
                 retry on the next resolve for this workspace)"
            );
            return;
        }
    };
    if let Err(e) = crate::routes::oauth::record_connector_install(
        &mut tx,
        account_id,
        provider,
        external_workspace_id,
        None,
        // Slack self-heal repair — always account-wide, byte-identical to
        // before this parameter existed.
        None,
    )
    .await
    {
        tracing::warn!(
            error = %e,
            account_id = %account_id,
            provider = %provider,
            workspace = %external_workspace_id,
            connector_health = true,
            "self-heal: failed to repair connector_installs (non-fatal — will retry on the \
             next resolve for this workspace)"
        );
        return;
    }
    if let Err(e) = tx.commit().await {
        tracing::warn!(
            error = %e,
            account_id = %account_id,
            provider = %provider,
            workspace = %external_workspace_id,
            connector_health = true,
            "self-heal: failed to commit connector_installs repair (non-fatal — will retry on \
             the next resolve for this workspace)"
        );
    }
}

/// [`resolve_slack_account_ids`] plus the ONE extra column the durable
/// tenant hand-off (org-slack step 2) needs to route each match's ingest
/// work: `clerk_org_id` — `NULL` = the account is a personal/consumer
/// account, `Some` = an org account (see [`AccountPlane`]'s doc comment for
/// why "org account" and "has an active tenant database" are two
/// independent things — the caller still has to resolve `tenant_databases`
/// for a `Some` row; see `routes::slack_events::enqueue_slack_message_
/// ingest`'s doc comment for that decision table).
///
/// **Builds on the healed result, does not re-derive it.**
/// [`resolve_slack_account_ids`] is already self-healing (fast path via
/// `connector_installs`, falling back to the authoritative `oauth_tokens`/
/// `slack_integrations` tables and best-effort repairing the index on a
/// miss — see that function's own doc comment) and already emits every
/// `connector_health` observability line this fan-out needs (fast-path,
/// self-healed, or genuinely-nowhere) — this function adds nothing on top
/// of that except the one extra column, and deliberately does NOT run a
/// second, non-healing resolution path of its own.
///
/// A plain, unscoped read of `accounts.clerk_org_id` — `accounts` carries no
/// RLS policy (`0083_consumer_rls`'s roster; see `tenancy::scope`'s module
/// doc), and every `account_id` here already came out of
/// [`resolve_slack_account_ids`] (backed by `resolve_connector_install_
/// account_ids`, migration `0121_connector_installs`, or its own fallback
/// resolvers over the authoritative tables), the SAME bootstrap-grade
/// "before any scope exists" resolution every sibling function in this file
/// goes through — so this is not a WIDER unscoped read than the caller
/// already trusts, only one extra column off the SAME row.
///
/// Preserves [`resolve_slack_account_ids`]'s deterministic ordering rather
/// than whatever order `= ANY($1)` happens to return. A resolved account
/// whose `accounts` row has since vanished (a hard-deleted account racing
/// this read) is silently dropped rather than erroring the whole batch —
/// every OTHER match still gets its plane resolved and its message
/// ingested; losing one to a rare concurrent-delete race is a smaller
/// failure than losing all of them.
pub(crate) async fn resolve_slack_account_planes(
    pool: &sqlx::PgPool,
    team_id: &str,
) -> Result<Vec<(Uuid, Option<String>)>, sqlx::Error> {
    let account_ids = resolve_slack_account_ids(pool, team_id).await?;
    if account_ids.is_empty() {
        // `resolve_slack_account_ids` already raised the `connector_health`
        // warning for this miss (self-healed vs. genuinely nowhere — see
        // that function's own doc comment) — nothing further to log here.
        return Ok(Vec::new());
    }

    let rows: Vec<(Uuid, Option<String>)> =
        sqlx::query_as("SELECT id, clerk_org_id FROM accounts WHERE id = ANY($1)")
            .bind(&account_ids)
            .fetch_all(pool)
            .await?;
    let mut by_id: std::collections::HashMap<Uuid, Option<String>> = rows.into_iter().collect();

    let resolved: Vec<(Uuid, Option<String>)> = account_ids
        .iter()
        .filter_map(|id| by_id.remove(id).map(|org| (*id, org)))
        .collect();
    if resolved.len() != account_ids.len() {
        tracing::warn!(
            team_id = %team_id,
            connector_installs_matches = account_ids.len(),
            resolved_matches = resolved.len(),
            connector_health = true,
            "resolve_slack_account_planes: some connector_installs account(s) had no accounts \
             row on re-read (likely a concurrent account deletion) — those matches are skipped, \
             not guessed"
        );
    }
    Ok(resolved)
}

// ── Slack name resolution (channel id → name, user id → display name) ─────────
// Per-process caches keyed by (account, id). Slack ids (C…/U…) are stable, so a
// successful lookup is cached for the process lifetime; failures are NOT cached
// (so a transient API error retries next time). Used to give ingested messages
// readable who/where context instead of opaque ids.

fn channel_name_cache() -> &'static std::sync::Mutex<std::collections::HashMap<(Uuid, String), String>>
{
    static C: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<(Uuid, String), String>>> =
        std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

fn user_name_cache() -> &'static std::sync::Mutex<std::collections::HashMap<(Uuid, String), String>> {
    static C: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<(Uuid, String), String>>> =
        std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Resolve a Slack channel id (e.g. `C0123`) to its name (e.g. `general`) via
/// `conversations.info`. Returns the raw id on any error (missing scope, rate
/// limit, network) so ingest is never blocked.
pub(crate) async fn resolve_channel_name(
    http: &reqwest::Client,
    account_id: Uuid,
    channel_id: &str,
    bot_token: &str,
) -> String {
    let key = (account_id, channel_id.to_string());
    if let Some(name) = channel_name_cache().lock().unwrap().get(&key).cloned() {
        return name;
    }
    let fetched = async {
        let resp = http
            .get(format!("{}/conversations.info", crate::connectors::slack::slack_api_base()))
            .query(&[("channel", channel_id)])
            .bearer_auth(bot_token)
            .send()
            .await
            .ok()?
            .json::<serde_json::Value>()
            .await
            .ok()?;
        resp.get("channel")?
            .get("name")?
            .as_str()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    }
    .await;
    match fetched {
        Some(name) => {
            channel_name_cache().lock().unwrap().insert(key, name.clone());
            name
        }
        None => channel_id.to_string(),
    }
}

/// Resolve a Slack user id (e.g. `U0123`) to a display name via `users.info`
/// (prefers `display_name`, then `real_name`). Returns the raw id on any error.
pub(crate) async fn resolve_user_name(
    http: &reqwest::Client,
    account_id: Uuid,
    user_id: &str,
    bot_token: &str,
) -> String {
    let key = (account_id, user_id.to_string());
    if let Some(name) = user_name_cache().lock().unwrap().get(&key).cloned() {
        return name;
    }
    let fetched = async {
        let resp = http
            .get(format!("{}/users.info", crate::connectors::slack::slack_api_base()))
            .query(&[("user", user_id)])
            .bearer_auth(bot_token)
            .send()
            .await
            .ok()?
            .json::<serde_json::Value>()
            .await
            .ok()?;
        let profile = resp.get("user")?.get("profile")?;
        let pick = |k: &str| {
            profile
                .get(k)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        };
        pick("display_name").or_else(|| pick("real_name"))
    }
    .await;
    match fetched {
        Some(name) => {
            user_name_cache().lock().unwrap().insert(key, name.clone());
            name
        }
        None => user_id.to_string(),
    }
}

fn workspace_url_cache() -> &'static std::sync::Mutex<std::collections::HashMap<(Uuid, String), String>> {
    static C: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<(Uuid, String), String>>> =
        std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// The workspace's web origin (`https://acme.slack.com/`), from `auth.test`
/// on any token of that workspace (`auth.test` needs no scope). Cached per
/// (account, workspace) for the process lifetime, so a capture calls Slack
/// once per workspace, not once per message. `None` on any error, when the
/// token belongs to another workspace, or when Slack answers with something
/// that is not an `https://….slack.com/` origin.
pub(crate) async fn resolve_workspace_url(
    http: &reqwest::Client,
    account_id: Uuid,
    team_id: &str,
    token: &str,
) -> Option<String> {
    let key = (account_id, team_id.to_string());
    if let Some(url) = workspace_url_cache().lock().unwrap().get(&key).cloned() {
        return Some(url);
    }
    let resp = http
        .get(format!("{}/auth.test", crate::connectors::slack::slack_api_base()))
        .bearer_auth(token)
        .send()
        .await
        .ok()?
        .json::<serde_json::Value>()
        .await
        .ok()?;
    if resp.get("ok").and_then(|v| v.as_bool()) != Some(true)
        || resp.get("team_id").and_then(|v| v.as_str()) != Some(team_id)
    {
        return None;
    }
    let url = workspace_origin(resp.get("url")?.as_str()?)?;
    workspace_url_cache().lock().unwrap().insert(key, url.clone());
    Some(url)
}

/// `https://<host>.slack.com/` with a trailing slash, or `None` for anything
/// else (another scheme or host, a path, a query).
fn workspace_origin(raw: &str) -> Option<String> {
    let url = reqwest::Url::parse(raw.trim()).ok()?;
    let host = url.host_str()?;
    let slack_host = host.ends_with(".slack.com") && host.len() > ".slack.com".len();
    (url.scheme() == "https" && slack_host && url.path() == "/" && url.query().is_none() && url.port().is_none())
        .then(|| format!("https://{host}/"))
}

/// Slack's message permalink: `<origin>archives/<channel>/p<ts without the
/// dot>`, plus `?thread_ts=<parent>&cid=<channel>` for a thread reply.
/// Built from ids already in hand, so no `chat.getPermalink` call per message.
pub(crate) fn message_permalink(workspace_url: &str, channel_id: &str, ts: &str, thread_ts: Option<&str>) -> Option<String> {
    let id_ok = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric());
    let ts_ok = |s: &str| s.split_once('.').is_some_and(|(a, b)| !a.is_empty() && !b.is_empty() && a.chars().chain(b.chars()).all(|c| c.is_ascii_digit()));
    if !id_ok(channel_id) || !ts_ok(ts) || !workspace_url.ends_with('/') {
        return None;
    }
    let mut link = format!("{workspace_url}archives/{channel_id}/p{}", ts.replace('.', ""));
    if let Some(parent) = thread_ts.filter(|p| *p != ts && ts_ok(p)) {
        link.push_str(&format!("?thread_ts={parent}&cid={channel_id}"));
    }
    Some(link)
}

#[cfg(test)]
mod permalink_tests {
    use super::*;

    #[test]
    fn permalinks_follow_slacks_archive_shape() {
        let origin = workspace_origin("https://acme.slack.com/").unwrap();
        assert_eq!(message_permalink(&origin, "C0123", "1729000000.000100", None).as_deref(),
            Some("https://acme.slack.com/archives/C0123/p1729000000000100"));
        assert_eq!(message_permalink(&origin, "C0123", "1729000001.000200", Some("1729000000.000100")).as_deref(),
            Some("https://acme.slack.com/archives/C0123/p1729000001000200?thread_ts=1729000000.000100&cid=C0123"));
        // A thread parent is its own thread_ts: no query.
        assert_eq!(message_permalink(&origin, "C0123", "1729000000.000100", Some("1729000000.000100")).as_deref(),
            Some("https://acme.slack.com/archives/C0123/p1729000000000100"));
        assert_eq!(message_permalink(&origin, "C0/../x", "1.2", None), None);
        assert_eq!(message_permalink(&origin, "C0123", "not-a-ts", None), None);
    }

    #[test]
    fn only_a_slack_workspace_origin_is_accepted() {
        assert_eq!(workspace_origin("https://acme.enterprise.slack.com/").as_deref(), Some("https://acme.enterprise.slack.com/"));
        for bad in ["http://acme.slack.com/", "https://evil.com/", "https://slack.com/", "https://acme.slack.com.evil.com/",
            "https://acme.slack.com/x", "https://acme.slack.com/?a=1", "javascript:alert(1)", ""] {
            assert_eq!(workspace_origin(bad), None, "{bad}");
        }
    }
}

// `ingest_slack_message` (the shared Slack ingest funnel), its atomic
// `persist_identity_and_enqueue_backfill` helper, and the pure
// `canonical_author_name` core were promoted to `routes::slack_events`
// (org-tenancy PR 1.6): they are CONSUMER-plane webhook machinery — invoked
// only from the signature-authenticated endpoints (`/v1/slack/events`,
// `/v1/slack/interactivity`), never from a
// Clerk-JWT-authed route — so they live in the allowlisted webhook module
// where ambient `state.db`/`state.store` access is legitimate, leaving this
// file's remaining handlers fully `Tenant`-resolved (and grep-guard-clean).

#[cfg(test)]
mod resolve_primary_graph_org_gate_tests {
    //! Item 8 of the Group-B security fix (PR 0 of the member-private-oceans
    //! plan, `docs/plans/member-private-oceans.md` §1.4(a)/(c)) proved
    //! `resolve_primary_graph` hard-fails on an org account rather than
    //! auto-creating a `kind='personal'` ocean. **PR 5** (plan §3/§7 item 5)
    //! splits that hard-fail by attributability: [`super::IngestCaller::
    //! NonAttributable`] still refuses unconditionally (the org-gate test
    //! below), while [`super::IngestCaller::Attributable`] now resolves the
    //! caller's own `kind='member'` ocean — proved by the three tests that
    //! follow it, including the one asserting a kind-only widening would have
    //! been wrong: a caller must never receive another member's ocean.
    //!
    //! In-crate (not `backend/tests/*_pg.rs`) because `resolve_primary_graph`
    //! is `pub(crate)` — every route that reaches it over HTTP under an org
    //! `TenancyDecision` already 403s before calling it (each carries its own
    //! `reject_org_*` gate; PR 5 left those gates in place — see this file's
    //! module-level PR notes), so an external test crate has no way to drive
    //! this function under an org account through the router at all. These
    //! are the only tests that exercise the function directly.
    //!
    //! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every
    //! `backend/tests/*_pg.rs` file, migrating the CONTROL tree only: both
    //! `accounts` (control-only) and `graphs` (a shared table present in both
    //! trees) live there, and every real caller of `resolve_primary_graph`
    //! that can reach an org `account_id` at all does so against this same
    //! physical consumer/control pool (`state.db`), never a real org tenant
    //! pool — see this module's callers audit in PR 0's description. Wired
    //! into CI in the `cross-tenant-probe` job (`.github/workflows/ci.yml`,
    //! step "resolve_primary_graph org gate (in-crate)") — the workspace
    //! `cargo test` job has no Postgres service and silently skips every test
    //! in this module.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib resolve_primary_graph_org_gate_tests

    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use std::sync::Arc;
    use uuid::Uuid;

    use super::{oauth_callback, AccountPlane, IngestCaller, OAuthCallbackQuery};

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

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn seed_account(pool: &PgPool, clerk_org_id: Option<&str>) -> Uuid {
        let owner = seed_user(pool).await;
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Seeds a `kind='member'` ocean directly (bypassing PR 3/4's
    /// reconciler/first-sight provisioning, which this module does not touch)
    /// so these tests can exercise `resolve_primary_graph`'s READ path in
    /// isolation from provisioning timing.
    async fn seed_member_ocean(
        pool: &PgPool,
        account_id: Uuid,
        owner_user_id: Uuid,
        is_primary: bool,
        archived: bool,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary, archived_at) \
             VALUES ($1, 'My Ocean', 'member', $2, $3, CASE WHEN $4 THEN now() ELSE NULL END) \
             RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .bind(is_primary)
        .bind(archived)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn resolve_primary_graph_refuses_to_auto_create_under_an_org_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_primary_graph_refuses_to_auto_create_under_an_org_account_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_resolve_primary_graph_org_gate";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let org_account = seed_account(&pool, Some("org_slack_gate_test")).await;

        // A NON-attributable caller (the Slack Events / huddle / Teams-bot
        // webhook shape) must refuse under an org account — Stage 1's
        // "fail closed" requirement, plan §3.
        let err = super::resolve_primary_graph(
            &pool,
            org_account,
            AccountPlane::Org,
            IngestCaller::NonAttributable,
        )
        .await
        .expect_err("resolve_primary_graph must refuse a non-attributable caller under an org account");
        assert!(
            err.to_string().contains("organization account"),
            "the refusal error should name the reason, got: {err}"
        );

        let created: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1")
            .bind(org_account)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            created, 0,
            "no graph — personal or otherwise — may be auto-created for an org account"
        );

        // Positive control: a genuinely personal account still gets its
        // primary ocean auto-created, unaffected by this fix — the caller
        // classification is ignored entirely on the personal path.
        let personal_account = seed_account(&pool, None).await;
        let graph_id = super::resolve_primary_graph(
            &pool,
            personal_account,
            AccountPlane::Personal,
            IngestCaller::NonAttributable,
        )
        .await
        .expect("resolve_primary_graph must still auto-create for a personal account");
        let row: (String, bool) =
            sqlx::query_as("SELECT kind, is_primary FROM graphs WHERE id = $1")
                .bind(graph_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(row, ("personal".to_string(), true));

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn resolve_primary_graph_attributable_resolves_own_member_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_primary_graph_attributable_resolves_own_member_ocean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_resolve_primary_graph_attributable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let org_account = seed_account(&pool, Some("org_attributable_test")).await;
        let member_a = seed_user(&pool).await;
        let ocean_a = seed_member_ocean(&pool, org_account, member_a, true, false).await;

        let resolved = super::resolve_primary_graph(
            &pool,
            org_account,
            AccountPlane::Org,
            IngestCaller::Attributable(member_a),
        )
        .await
        .expect("an attributable caller with an existing primary member ocean must resolve it");
        assert_eq!(
            resolved, ocean_a,
            "must resolve to the CALLER's own primary member ocean"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// The full-predicate test (plan §3: "the kind check alone is NOT
    /// sufficient"). Two members of the SAME org account each have their own
    /// primary member ocean; member B must resolve to B's own ocean, never
    /// A's, even though a kind-only (or kind + account_id-only) query would
    /// return whichever row Postgres happened to pick.
    #[tokio::test]
    async fn resolve_primary_graph_attributable_never_returns_another_members_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_primary_graph_attributable_never_returns_another_members_ocean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_resolve_primary_graph_cross_member";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let org_account = seed_account(&pool, Some("org_cross_member_test")).await;
        let member_a = seed_user(&pool).await;
        let member_b = seed_user(&pool).await;
        let ocean_a = seed_member_ocean(&pool, org_account, member_a, true, false).await;
        let ocean_b = seed_member_ocean(&pool, org_account, member_b, true, false).await;
        assert_ne!(ocean_a, ocean_b);

        let resolved_for_b = super::resolve_primary_graph(
            &pool,
            org_account,
            AccountPlane::Org,
            IngestCaller::Attributable(member_b),
        )
        .await
        .expect("member B has their own primary member ocean");
        assert_eq!(
            resolved_for_b, ocean_b,
            "member B must resolve to B's own ocean"
        );
        assert_ne!(
            resolved_for_b, ocean_a,
            "member B must NEVER receive member A's private ocean"
        );

        let resolved_for_a = super::resolve_primary_graph(
            &pool,
            org_account,
            AccountPlane::Org,
            IngestCaller::Attributable(member_a),
        )
        .await
        .expect("member A has their own primary member ocean");
        assert_eq!(resolved_for_a, ocean_a);

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// An attributable caller with NO member ocean yet (provisioning has not
    /// caught up, or `MEMBER_OCEANS_ENABLED` is off) must fail closed — this
    /// function is a read path, not a provisioning trigger, and must not
    /// auto-create one (that would race PR 3/4's reconciler/first-sight
    /// triggers and their own `ON CONFLICT` semantics). Also proves an
    /// archived member ocean is treated as absent.
    #[tokio::test]
    async fn resolve_primary_graph_attributable_refuses_when_no_member_ocean_exists_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_primary_graph_attributable_refuses_when_no_member_ocean_exists_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_resolve_primary_graph_no_ocean_yet";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let org_account = seed_account(&pool, Some("org_no_ocean_yet_test")).await;
        let member_a = seed_user(&pool).await;

        // No member ocean at all yet.
        super::resolve_primary_graph(
            &pool,
            org_account,
            AccountPlane::Org,
            IngestCaller::Attributable(member_a),
        )
        .await
        .expect_err("must refuse — no member ocean exists yet, and this function must not create one");

        // Only an ARCHIVED member ocean — must be treated identically to
        // "none exists" (plan §6 P0-5: archived ⇒ unreachable everywhere).
        seed_member_ocean(&pool, org_account, member_a, true, true).await;
        super::resolve_primary_graph(
            &pool,
            org_account,
            AccountPlane::Org,
            IngestCaller::Attributable(member_a),
        )
        .await
        .expect_err("must refuse — the only member ocean for this caller is archived");

        // A NON-PRIMARY, ACTIVE member ocean must ALSO be refused. Without
        // this case every assertion above passes `is_primary = true`, so
        // deleting `AND is_primary = true` from the resolver's predicate
        // would not fail a single test in this module — the test could not
        // detect the removal of the very clause it exists to pin.
        //
        // This pairing is representable precisely because
        // `uq_graphs_one_primary_per_owner` also excludes archived rows: the
        // archived primary seeded above and this active secondary can coexist
        // for the same owner without violating it. That is by design
        // (secondary member oceans are supported), which is exactly why the
        // resolver must pick on `is_primary` rather than "any member ocean".
        seed_member_ocean(&pool, org_account, member_a, false, false).await;
        super::resolve_primary_graph(
            &pool,
            org_account,
            AccountPlane::Org,
            IngestCaller::Attributable(member_a),
        )
        .await
        .expect_err(
            "must refuse — the caller's only ACTIVE member ocean is non-primary, and ingest \
             resolves the PRIMARY one",
        );

        let created: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1")
            .bind(org_account)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            created, 2,
            "resolve_primary_graph must not have created a graph on any of the three calls \
             (only the two seeded by the test should exist)"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Org-tenancy plane split, step 0: [`account_plane`] must derive the
    /// SAME plane [`resolve_primary_graph`] used to derive internally before
    /// this split, for both personal and org accounts, and must `Err`
    /// (never silently default to `Personal`) for an unknown `account_id` —
    /// the exact invariant the removed inline lookup used to enforce.
    #[tokio::test]
    async fn account_plane_matches_the_old_inline_lookup_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping account_plane_matches_the_old_inline_lookup_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_account_plane";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let personal_account = seed_account(&pool, None).await;
        assert_eq!(
            super::account_plane(&pool, personal_account).await.unwrap(),
            AccountPlane::Personal
        );

        let org_account = seed_account(&pool, Some("org_account_plane_test")).await;
        assert_eq!(
            super::account_plane(&pool, org_account).await.unwrap(),
            AccountPlane::Org
        );

        let unknown_account = Uuid::new_v4();
        super::account_plane(&pool, unknown_account)
            .await
            .expect_err(
                "account_plane must refuse (never guess Personal) for an unknown account_id",
            );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Consumer-plane no-behaviour-change case: a caller that derives its
    /// plane via [`account_plane`] (the pattern every caller that does not
    /// already have a plane in hand now uses) and feeds it into
    /// `resolve_primary_graph` must observe EXACTLY the pre-split behavior —
    /// auto-create-and-return for a personal account, and the org
    /// non-attributable hard refusal — end to end, not just at the
    /// `account_plane` lookup in isolation.
    #[tokio::test]
    async fn resolve_primary_graph_with_derived_plane_matches_pre_split_behavior_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_primary_graph_with_derived_plane_matches_pre_split_behavior_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_resolve_primary_graph_derived_plane";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Personal: the caller derives its own plane, exactly as the removed
        // inline lookup used to, and the auto-create-primary behavior is
        // unaffected.
        let personal_account = seed_account(&pool, None).await;
        let plane = super::account_plane(&pool, personal_account).await.unwrap();
        let graph_id = super::resolve_primary_graph(
            &pool,
            personal_account,
            plane,
            IngestCaller::NonAttributable,
        )
        .await
        .expect("personal account must still auto-create its primary via a derived plane");
        let row: (String, bool) =
            sqlx::query_as("SELECT kind, is_primary FROM graphs WHERE id = $1")
                .bind(graph_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(row, ("personal".to_string(), true));

        // Org, non-attributable: the caller derives its own plane and the
        // hard refusal still fires — unchanged by where the plane came from.
        let org_account = seed_account(&pool, Some("org_derived_plane_test")).await;
        let plane = super::account_plane(&pool, org_account).await.unwrap();
        let err = super::resolve_primary_graph(
            &pool,
            org_account,
            plane,
            IngestCaller::NonAttributable,
        )
        .await
        .expect_err(
            "org account via a derived plane must still refuse a non-attributable caller",
        );
        assert!(err.to_string().contains("organization account"));

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── Org governance decision (2026-08-04), piece 2: one workspace per org ──
    //
    // `refuse_second_org_slack_workspace` lives in `routes::oauth` (`pub(crate)`,
    // control-plane only — `slack_integrations`/`connector_installs`, both in
    // `migrations_control`), so it fits this module's existing lightweight
    // control-only harness (`recreate`/`seed_account`/`seed_user`) exactly —
    // no tenant tree needed, same reason this whole module is control-only.

    #[tokio::test]
    async fn refuse_second_org_slack_workspace_blocks_a_different_workspace_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping refuse_second_org_slack_workspace_blocks_a_different_workspace_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_refuse_second_org_slack_workspace";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let org_account = seed_account(&pool, Some("org_second_workspace_test")).await;

        // Seed the account's ALREADY-connected workspace, exactly the row
        // `routes::slack::oauth_callback`'s upsert would leave behind.
        sqlx::query(
            "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, 'T_FIRST', 'First Workspace', 'xoxb-first')",
        )
        .bind(org_account)
        .execute(&pool)
        .await
        .unwrap();

        // Reconnecting the SAME workspace must be allowed — this is a
        // reconnect/refresh, not a second workspace.
        crate::routes::oauth::refuse_second_org_slack_workspace(&pool, org_account, "T_FIRST")
            .await
            .expect("reconnecting the SAME workspace must not be refused");

        // A DIFFERENT workspace must be refused, with the existing
        // workspace's name in the error, and — this is the actual safety
        // property, not just the returned error — NOTHING written as a side
        // effect of the check itself.
        let resp = crate::routes::oauth::refuse_second_org_slack_workspace(
            &pool,
            org_account,
            "T_SECOND",
        )
        .await
        .expect_err("a second, different workspace must be refused");
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        let still_only_first: Vec<String> =
            sqlx::query_scalar("SELECT workspace_id FROM slack_integrations WHERE account_id = $1")
                .bind(org_account)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            still_only_first,
            vec!["T_FIRST".to_string()],
            "the refusal must not have written anything for the second workspace"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// THE race-closing property (independent-verification MAJOR finding):
    /// `refuse_second_org_slack_workspace` alone is check-then-write across
    /// separate connections/transactions, so two genuinely CONCURRENT
    /// connects for two different workspaces could both pass it before
    /// either commits. This proves `refuse_second_org_slack_workspace_locked`
    /// — the per-account `pg_advisory_xact_lock` recheck run a second time
    /// inside the write transaction — actually closes that window under
    /// real concurrency, not just sequential calls.
    ///
    /// Task A acquires the lock, passes its check, writes `T_A`, and signals
    /// task B only AFTER acquiring the lock but BEFORE committing — so task
    /// B's own lock acquisition is GUARANTEED to block on task A's still-open
    /// transaction (this is Postgres's own blocking contract for
    /// `pg_advisory_xact_lock`, not a Tokio-scheduling race this test hopes
    /// to win). Once task A commits and releases the lock, task B unblocks,
    /// re-runs the check, now sees `T_A` already committed, and must be
    /// refused.
    #[tokio::test]
    async fn refuse_second_org_slack_workspace_locked_serializes_concurrent_connects_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping refuse_second_org_slack_workspace_locked_serializes_concurrent_connects_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_refuse_second_workspace_concurrent";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account = seed_account(&pool, Some("org_concurrent_connect_test")).await;

        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
        let pool_a = pool.clone();
        let task_a = tokio::spawn(async move {
            let mut tx = pool_a.begin().await.unwrap();
            let result =
                crate::routes::oauth::refuse_second_org_slack_workspace_locked(&mut tx, account, "T_A")
                    .await;
            assert!(result.is_ok(), "task A: nothing else connected yet, must succeed");
            sqlx::query(
                "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
                 VALUES ($1, 'T_A', 'Workspace A', 'xoxb-a')",
            )
            .bind(account)
            .execute(&mut *tx)
            .await
            .unwrap();
            acquired_tx.send(()).expect("test harness receiver must still be alive");
            // Hold the transaction (and therefore the advisory lock) open —
            // simulates the real write work (token encryption,
            // connector_installs upsert) that happens between this check and
            // commit in production. Task B's lock acquisition below is
            // forced to wait out this entire window.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            tx.commit().await.unwrap();
        });

        acquired_rx.await.expect("task A must signal after acquiring its lock");

        let pool_b = pool.clone();
        let task_b = tokio::spawn(async move {
            let mut tx = pool_b.begin().await.unwrap();
            let result =
                crate::routes::oauth::refuse_second_org_slack_workspace_locked(&mut tx, account, "T_B")
                    .await;
            tx.rollback().await.ok();
            result
        });

        task_a.await.unwrap();
        let b_result = task_b.await.unwrap();

        let b_err = b_result.expect_err(
            "task B must block on the advisory lock until task A commits, then see task A's \
             now-committed workspace and be refused — this is the genuinely-concurrent \
             race-closing property, not just the single-request check",
        );
        assert_eq!(b_err.status(), StatusCode::CONFLICT);

        let workspace_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM slack_integrations WHERE account_id = $1")
                .bind(account)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            workspace_count, 1,
            "exactly one workspace must have landed, even though both requests ran genuinely \
             concurrently"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A personal account is explicitly OUT of scope for this decision (see
    /// `refuse_second_org_slack_workspace`'s own doc comment) — connecting a
    /// second workspace on the personal plane must be unaffected.
    #[tokio::test]
    async fn refuse_second_org_slack_workspace_is_not_called_for_personal_accounts_by_construction_pg(
    ) {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping refuse_second_org_slack_workspace_is_not_called_for_personal_accounts_by_construction_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_refuse_second_workspace_personal";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let personal_account = seed_account(&pool, None).await;
        sqlx::query(
            "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, 'T_FIRST', 'First Workspace', 'xoxb-first')",
        )
        .bind(personal_account)
        .execute(&pool)
        .await
        .unwrap();

        // Directly proves the underlying check itself is workspace-scoped
        // logic only, with no account-plane awareness of its own — it is the
        // CALLERS (`routes::oauth::callback`, `routes::slack::oauth_callback`)
        // that gate this behind `clerk_org_id.is_some()`/`tenant.org_context()
        // .is_some()`, never invoking it at all for a personal account. This
        // test documents that contract rather than re-deriving it, so a
        // future caller cannot silently start calling this for Personal too
        // without a reviewer noticing the assumption change.
        let resp = crate::routes::oauth::refuse_second_org_slack_workspace(
            &pool,
            personal_account,
            "T_SECOND",
        )
        .await;
        assert!(
            resp.is_err(),
            "the bare function has no plane awareness — it is the callers' job to skip it for \
             Personal, not this function's"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── Org governance decision (2026-08-04), piece 1: the LEGACY callback ──
    // (independent-verification CRITICAL finding). `reject_org_slack_management`
    // alone only gates the `ORG_SLACK_OAUTH_ENABLED` flag — it says nothing
    // about WHO is calling — so before the admin gate below existed, a plain
    // org member with a valid org bearer token could `curl` this callback
    // directly (no Slack consent screen required to have actually run first;
    // this route trusts `?code=` from the query string) and write
    // `slack_integrations`/`connector_installs` for the org account.
    //
    // `ORG_SLACK_OAUTH_ENABLED=1` is set for the duration of this test only
    // (saved/restored) so the request actually reaches the admin gate rather
    // than being turned away earlier by the flag gate — this test is about
    // the SECOND gate, not the first, and both currently answer 403, so
    // leaving the flag off would let this test "pass" via the wrong gate and
    // never actually exercise `reject_org_slack_non_admin`.
    #[tokio::test]
    async fn oauth_callback_refuses_a_plain_org_member_and_writes_nothing_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping oauth_callback_refuses_a_plain_org_member_and_writes_nothing_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_gb_oauth_callback_member_gate";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let org_account = seed_account(&pool, Some("org_oauth_callback_gate_test")).await;
        let member_user = seed_user(&pool).await;

        let prior_flag = std::env::var("ORG_SLACK_OAUTH_ENABLED").ok();
        // SAFETY: this whole suite runs `--test-threads=1` (its own doc
        // comment's invocation line), and no other test in this crate reads
        // or writes `ORG_SLACK_OAUTH_ENABLED`.
        unsafe {
            std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "1");
        }

        // Built through the lib.rs helper rather than assigning `state.db`
        // here — see `test_app_state_owned_with_db`'s doc comment (the
        // ambient-pool guard greps this file line-by-line, tests included).
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        // A lazy pool never connects unless a query executes against it —
        // `oauth_callback`'s own control-plane work always goes through
        // `state.control_db()`, never `tenant.pool`, so this is never
        // touched by a request this gate refuses. Same pattern
        // `tenancy::reachability`'s own `lazy_ctx` test helper uses.
        let tenant = crate::tenancy::extractor::Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_oauth_callback_gate_test".to_string(),
                    account_id: org_account,
                    user_id: member_user,
                    pool: sqlx::postgres::PgPoolOptions::new()
                        .connect_lazy("postgres://localhost/unused")
                        .expect("lazy pool construction never connects"),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        };
        let user_ctx = crate::middleware::auth::UserContext {
            user_id: member_user,
            account_id: org_account,
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };

        let resp = oauth_callback(
            axum::extract::State(state),
            tenant,
            axum::Extension(user_ctx),
            axum::extract::Query(OAuthCallbackQuery {
                code: "irrelevant-a-plain-member-must-never-reach-the-slack-exchange".to_string(),
                state: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "a plain org member must be refused before any Slack exchange or DB write is attempted"
        );

        let slack_integrations_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM slack_integrations WHERE account_id = $1")
                .bind(org_account)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            slack_integrations_rows, 0,
            "the refused request must have written zero slack_integrations rows"
        );

        let connector_installs_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(org_account)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            connector_installs_rows, 0,
            "the refused request must have written zero connector_installs rows"
        );

        // SAFETY: still under the same-thread, no-other-writer guarantee
        // this test's own header note documents.
        unsafe {
            match &prior_flag {
                Some(v) => std::env::set_var("ORG_SLACK_OAUTH_ENABLED", v),
                None => std::env::remove_var("ORG_SLACK_OAUTH_ENABLED"),
            }
        }

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── member channel write gate (2026-08-04 role-scoping decision) ─────

    /// The unlinked branch END-TO-END: an org member with no confirmed
    /// `connector_identities` row is refused with 403 naming the fix, and an
    /// admin passes without the gate ever touching Slack (the garbage
    /// `bot_token` below would make any HTTP attempt surface as a 503, so the
    /// admin's `None` doubly proves the short-circuit).
    ///
    /// NEGATIVE CONTROL (performed by hand, documented in the PR): with the
    /// `plane != Org || viewer_role != "member"` early-return in
    /// `resolve_member_channel_scope` inverted to always return
    /// `Unrestricted`, the first assertion fails (member gets `None`);
    /// restored, green.
    #[tokio::test]
    async fn member_write_gate_refuses_unlinked_member_and_passes_admin_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_write_gate_refuses_unlinked_member_and_passes_admin_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_member_write_gate_unlinked";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account = seed_account(&pool, Some("org_member_write_gate")).await;
        let member = seed_user(&pool).await;
        let http = reqwest::Client::new();

        let refused = super::refuse_out_of_scope_member(
            &http,
            &pool,
            account,
            AccountPlane::Org,
            "member",
            member,
            "T-GATE",
            "xoxb-not-a-real-token",
            "C-ANY",
        )
        .await
        .expect("an unlinked member must be refused");
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(refused.into_body(), 4096).await.unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("link your Slack identity"),
            "the refusal must name the fix"
        );

        let admin_pass = super::refuse_out_of_scope_member(
            &http,
            &pool,
            account,
            AccountPlane::Org,
            "admin",
            member,
            "T-GATE",
            "xoxb-not-a-real-token",
            "C-ANY",
        )
        .await;
        assert!(admin_pass.is_none(), "admins are never scoped, and no Slack call happens");

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// The unavailable branch END-TO-END: a LINKED member whose
    /// `users.conversations` lookup fails (garbage token against real Slack)
    /// gets a retryable 503 — never a 403 ("you're in no channels") and
    /// never a pass. A failed lookup must not change what a member may
    /// touch in either direction.
    #[tokio::test]
    async fn member_write_gate_fails_closed_when_the_lookup_is_unavailable_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_write_gate_fails_closed_when_the_lookup_is_unavailable_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_member_write_gate_unavailable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account = seed_account(&pool, Some("org_member_write_gate_unavail")).await;
        let member = seed_user(&pool).await;
        // Confirmed link: migration 0104's provenance CHECK makes
        // user_id/link_method/link_verified_at all-or-nothing.
        sqlx::query(
            "INSERT INTO connector_identities \
             (provider, workspace_id, external_user_id, canonical_name, user_id, link_method, link_verified_at) \
             VALUES ('slack', 'T-GATE', 'U-LINKED', 'Linked Member', $1, 'verified_email', now())",
        )
        .bind(member)
        .execute(&pool)
        .await
        .unwrap();

        let refused = super::refuse_out_of_scope_member(
            &reqwest::Client::new(),
            &pool,
            account,
            AccountPlane::Org,
            "member",
            member,
            "T-GATE",
            "xoxb-not-a-real-token",
            "C-ANY",
        )
        .await
        .expect("a failed lookup must refuse, not pass");
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// The negative-result cache backing `resolve_slack_account_ids`'s
/// self-healing fallback ([`negative_resolve_cache_hit`] / [`cache_negative_resolve`]).
/// Plain, fast, no DB — operates on process-global state only, so this is a
/// bare `#[test]`, not `#[tokio::test]`, and not gated on `ORGT_TEST_BASE_URL`.
#[cfg(test)]
mod negative_resolve_cache_tests {
    use super::{cache_negative_resolve, negative_resolve_cache_hit};

    #[test]
    fn a_cached_negative_is_hit_immediately_and_does_not_leak_to_other_keys() {
        // A random-ish key per test run so this can never collide with
        // another test in the same process (the cache is a process-global
        // singleton, shared across every test in the binary).
        let provider = "test-provider";
        let workspace = format!("T-NEG-CACHE-{}", uuid::Uuid::new_v4());

        assert!(
            !negative_resolve_cache_hit(provider, &workspace),
            "an unseen key must not be a cache hit"
        );

        cache_negative_resolve(provider, &workspace);
        assert!(
            negative_resolve_cache_hit(provider, &workspace),
            "a cached negative must be hit immediately after caching"
        );

        // A DIFFERENT workspace under the same provider must not be affected.
        let other_workspace = format!("T-NEG-CACHE-OTHER-{}", uuid::Uuid::new_v4());
        assert!(
            !negative_resolve_cache_hit(provider, &other_workspace),
            "caching one workspace must not leak a hit onto a different one"
        );
    }
}

/// Self-healing fallback (`resolve_slack_account_ids`, migration
/// `0121_connector_installs`, founder decision — org-slack step 1 review).
///
/// Six independent pre-existing test fixtures across this file and
/// `slack_events.rs` proved a fixture author reaching straight for
/// `oauth_tokens`/`slack_integrations` (without also writing
/// `connector_installs`) is a natural, easy mistake — see those tests'
/// own comments (`routes::slack_events::tests::huddle_trigger_
/// message_changed_reaches_trigger_ahead_of_subtype_filter_pg` and
/// `routes::slack_events::slack_watched_channel_destination_tests`) for the
/// regression they now (correctly) prove passes via THIS fallback, not via
/// a taught-fixture incantation. This module instead tests the fallback
/// mechanism directly: the exact properties those six tests could only
/// prove incidentally.
///
/// In-crate (not `backend/tests/*_pg.rs`) because `resolve_slack_account_ids`
/// is `pub(crate)`. Gated on `ORGT_TEST_BASE_URL`, same scratch-DB
/// convention as every sibling module in this file. Wired into CI in the
/// `cross-tenant-probe` job (`.github/workflows/ci.yml`).
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib connector_install_self_heal_tests
#[cfg(test)]
mod connector_install_self_heal_tests {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

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

    async fn seed_account(pool: &PgPool, email: &str) -> Uuid {
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
                .bind(email)
                .fetch_one(pool)
                .await
                .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_oauth_tokens_only(pool: &PgPool, account_id: Uuid, workspace_id: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, access_token, \
             provider_workspace_id) VALUES ($1, 'slack', 'test', $2, $3)",
        )
        .bind(account_id)
        .bind(b"stub-ciphertext".to_vec())
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_slack_integrations_only(pool: &PgPool, account_id: Uuid, workspace_id: &str) {
        sqlx::query(
            "INSERT INTO slack_integrations(account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, $2, 'Test Workspace', 'xoxb-test')",
        )
        .bind(account_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn connector_installs_count(pool: &PgPool, account_id: Uuid, workspace_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM connector_installs \
             WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id = $2",
        )
        .bind(account_id)
        .bind(workspace_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn fallback_resolves_and_repairs_an_oauth_tokens_only_install_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fallback_resolves_and_repairs_an_oauth_tokens_only_install_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_connector_install_self_heal_oauth_tokens";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account = seed_account(&pool, "self-heal-oauth@test.local").await;
        seed_oauth_tokens_only(&pool, account, "T-SELF-HEAL-OAUTH").await;

        assert_eq!(
            connector_installs_count(&pool, account, "T-SELF-HEAL-OAUTH").await,
            0,
            "sanity: no connector_installs row exists before the first resolve"
        );

        let resolved = super::resolve_slack_account_ids(&pool, "T-SELF-HEAL-OAUTH")
            .await
            .expect("resolve_slack_account_ids must succeed");
        assert_eq!(
            resolved,
            vec![account],
            "an oauth_tokens-only install (no connector_installs row) must still resolve via \
             the fallback"
        );

        assert_eq!(
            connector_installs_count(&pool, account, "T-SELF-HEAL-OAUTH").await,
            1,
            "the fallback must have REPAIRED connector_installs with the resolved account"
        );

        // Second resolve now hits the fast path — same answer, and it does
        // not depend on oauth_tokens/slack_integrations at all any more
        // (deleting them and re-resolving proves the fast path alone now
        // carries the answer).
        sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1")
            .bind(account)
            .execute(&pool)
            .await
            .unwrap();
        let resolved_again = super::resolve_slack_account_ids(&pool, "T-SELF-HEAL-OAUTH")
            .await
            .expect("resolve_slack_account_ids must succeed on the repaired fast path");
        assert_eq!(
            resolved_again,
            vec![account],
            "after repair, the fast connector_installs path alone must resolve this workspace"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn fallback_resolves_a_slack_integrations_only_legacy_install_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fallback_resolves_a_slack_integrations_only_legacy_install_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_connector_install_self_heal_slack_integrations";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account = seed_account(&pool, "self-heal-legacy@test.local").await;
        seed_slack_integrations_only(&pool, account, "T-SELF-HEAL-LEGACY").await;

        let resolved = super::resolve_slack_account_ids(&pool, "T-SELF-HEAL-LEGACY")
            .await
            .expect("resolve_slack_account_ids must succeed");
        assert_eq!(
            resolved,
            vec![account],
            "a slack_integrations-only (legacy) install must still resolve via the fallback"
        );
        assert_eq!(
            connector_installs_count(&pool, account, "T-SELF-HEAL-LEGACY").await,
            1,
            "the fallback must have repaired connector_installs for the legacy install too"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// The property the founder was most explicit about protecting: falling
    /// back through the OLD `LIMIT 1` resolvers would have picked ONE of
    /// these two accounts arbitrarily. Account A is installed via
    /// `oauth_tokens` only, account B via `slack_integrations` only — same
    /// workspace, two different source tables — and the fallback must
    /// return BOTH.
    #[tokio::test]
    async fn fallback_returns_every_account_across_both_source_tables_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fallback_returns_every_account_across_both_source_tables_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_connector_install_self_heal_multi_row";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_a = seed_account(&pool, "self-heal-multi-a@test.local").await;
        let account_b = seed_account(&pool, "self-heal-multi-b@test.local").await;
        seed_oauth_tokens_only(&pool, account_a, "T-SELF-HEAL-SHARED").await;
        seed_slack_integrations_only(&pool, account_b, "T-SELF-HEAL-SHARED").await;

        let mut resolved = super::resolve_slack_account_ids(&pool, "T-SELF-HEAL-SHARED")
            .await
            .expect("resolve_slack_account_ids must succeed");
        resolved.sort();
        let mut expected = vec![account_a, account_b];
        expected.sort();
        assert_eq!(
            resolved, expected,
            "BOTH accounts sharing this workspace across the two source tables must come back \
             — an arbitrary LIMIT-1 pick here would silently orphan whichever account it does \
             not choose, exactly the bug this migration exists to kill"
        );

        assert_eq!(
            connector_installs_count(&pool, account_a, "T-SELF-HEAL-SHARED").await,
            1,
            "account A's repair must have landed"
        );
        assert_eq!(
            connector_installs_count(&pool, account_b, "T-SELF-HEAL-SHARED").await,
            1,
            "account B's repair must have landed too — the fan-out is not just returned to the \
             caller, both are actually persisted"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn fallback_finds_nothing_returns_empty_and_writes_nothing_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fallback_finds_nothing_returns_empty_and_writes_nothing_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_connector_install_self_heal_nothing";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let resolved = super::resolve_slack_account_ids(&pool, "T-NEVER-INSTALLED")
            .await
            .expect("resolve_slack_account_ids must succeed even when nothing resolves");
        assert!(
            resolved.is_empty(),
            "a workspace with no install anywhere must resolve to an empty set, not error"
        );

        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_installs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(total, 0, "nothing found anywhere must write nothing");

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
