//! Calendar read routes (cs-uxp4a-calendar).
//!
//! One integration (Google Calendar today; structured for Microsoft) that feeds
//! four UX gaps: meeting titles, attendee rosters for speaker naming, the
//! pre-meeting record nudge, and detection confidence. These endpoints are the
//! read side — the OAuth connect/disconnect flow is the generic connector path
//! (`routes/oauth.rs`, provider `"google"`).
//!
//! # Routes (all bearer-authed, account-scoped)
//!
//! | Method | Path                     | Description                                   |
//! |--------|--------------------------|-----------------------------------------------|
//! | GET    | /v1/calendar/upcoming    | Next ~24h events (title, times, attendees, URL)|
//! | GET    | /v1/calendar/status      | connected? account email? reminders on?        |
//! | PUT    | /v1/calendar/reminders   | Toggle the pre-meeting reminder preference     |
//!
//! # Security
//! - The connected calendar is resolved from the *authenticated* account
//!   (`UserContext.account_id`), never a client parameter.
//! - Stored tokens are decrypted only in-process to call Google; the access
//!   token is refreshed (and re-encrypted/persisted) when expired. Tokens and
//!   event contents are never logged or echoed in error bodies.
//! - Degrades cleanly: `404` when no calendar is connected (the client polls
//!   nothing), so a disconnected user gets zero calendar traffic.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{Duration, Utc};
use serde::Deserialize;
use serde_json::json;

use crate::{
    connectors::{self, ConnectorError, GoogleConnector, TokenOwner},
    middleware::auth::UserContext,
    routes::{connectors::load_creds_bg, oauth::store_token_bundle},
    tenancy::extractor::Tenant,
    AppState,
};

/// Fail-closed gate for the calendar routes under an org `TenancyDecision`
/// (org-tenancy PR 1.6) — same policy + rationale as
/// `routes::connectors::reject_org_connector_access`. **Lifted for an org
/// whose `google_calendar` flag is on** (Google Calendar org lift,
/// `routes::oauth::org_google_calendar_enabled_for`,
/// `ORG_GOOGLE_CALENDAR_ENABLED`, default OFF): the connection is then the
/// caller's OWN per-member `oauth_tokens` row (`calendar_owner` below), read
/// on the CONTROL pool. Flag off, it still refuses cleanly under Org —
/// including the read-only `status`, so an org-context Settings page
/// reports "unavailable here" rather than a misleading answer. Personal
/// requests are byte-identical to before (`org_context()` is `None`).
async fn reject_org_calendar_access(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if crate::routes::oauth::org_google_calendar_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "calendar route refused under org context (ORG_GOOGLE_CALENDAR_ENABLED/\
         org_connector_flags resolved off for this org)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "calendar is not enabled for this organization workspace yet; \
                          switch to your personal workspace to use it"
            })),
        )
            .into_response(),
    )
}

/// Whose `oauth_tokens` row the calendar routes read: the caller's OWN
/// per-member row under an org, the account-wide row on the personal plane.
/// Same resolution as the write path (`Surface::calendar_token_owner`).
fn calendar_owner(tenant: &Tenant, user: &UserContext) -> TokenOwner {
    connectors::google::Surface::calendar_token_owner(tenant.org_context().is_some(), user.user_id)
}

/// Provider id for the Google connector (matches `oauth_tokens.provider`).
const GOOGLE: &str = "google";

/// How far ahead `upcoming` looks. One day covers "the rest of today + tonight"
/// for the pre-meeting nudge without pulling a whole week of noise.
const HORIZON_HOURS: i64 = 24;

// ---------------------------------------------------------------------------
// GET /v1/calendar/upcoming
// ---------------------------------------------------------------------------

/// Return the account's calendar events over the next [`HORIZON_HOURS`], shaped
/// for the meeting nudge + title/roster attachment. `404` when no calendar is
/// connected.
pub async fn upcoming(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    if let Some(reject) = reject_org_calendar_access(&state, &tenant).await {
        return reject;
    }
    // `oauth_tokens` is CONTROL-plane-only: read/persist it on the control
    // pool. Past the org gate this is Personal (where `tenant.pool` IS that
    // same consumer pool) or an org with the calendar flag on (where
    // `tenant.pool` would be the org's TENANT pool and must never be asked
    // for a token row). `tenant.pool` stays the proof the extractor resolved
    // a plane at all.
    if tenant.pool.is_none() {
        return unavailable();
    }
    let Some(pool) = state.control_db().cloned() else {
        return unavailable();
    };
    let account_id = user.account_id;
    let owner = calendar_owner(&tenant, &user);

    // The DEK plane comes off the request's OWN binding-gate decision rather
    // than being pinned to `consumer` — `reject_org_calendar_access` above
    // makes this Personal/flag-off today (so this is `DekPlane::consumer`
    // byte-for-byte), but taking it from `decision` means lifting that gate
    // cannot silently reintroduce a wrong-plane key read: an `Org` decision
    // yields that org's tenant plane, which is where its `account_keys` DEK
    // actually lives.
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), &pool);

    // Load + decrypt the stored Google token (None → not connected):
    // account-wide on the personal plane, the caller's own per-member row
    // under an org (`calendar_owner`).
    let mut creds = match load_creds_bg(
        &pool,
        &dek_plane,
        account_id,
        GOOGLE,
        owner,
    )
    .await
    {
        Ok(Some(c)) => c,
        Ok(None) => return not_connected(),
        Err(e) => return connector_error_response(e),
    };

    let google = GoogleConnector;

    // Refresh the access token if it is expired, and persist the rotation.
    if let Err(e) = google
        .refresh_if_needed(&state.http_client, &mut creds)
        .await
    {
        return connector_error_response(e);
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(resp) = store_token_bundle(
            &pool,
            &dek_plane,
            account_id,
            GOOGLE,
            owner,
            refreshed,
        )
        .await
        {
            // Non-fatal: we still hold a valid in-memory access token for this
            // request; a persist failure only means we refresh again next time.
            tracing::warn!(
                account_id = %account_id,
                status = ?resp.status(),
                "failed to persist refreshed google token (non-fatal)"
            );
        }
    }

    let now = Utc::now();
    let events = match google
        .fetch_upcoming(
            &state.http_client,
            &creds.access_token,
            now,
            now + Duration::hours(HORIZON_HOURS),
        )
        .await
    {
        Ok(events) => events,
        Err(e) => return connector_error_response(e),
    };

    tracing::info!(
        account_id = %account_id,
        count = events.len(),
        "calendar upcoming served"
    );

    (
        StatusCode::OK,
        Json(json!({
            "events": events,
            "horizon_hours": HORIZON_HOURS,
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// GET /v1/calendar/status
// ---------------------------------------------------------------------------

/// Report whether a calendar is connected, the account email (for the Settings
/// "connected as …" line), and whether pre-meeting reminders are enabled.
pub async fn status(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    if let Some(reject) = reject_org_calendar_access(&state, &tenant).await {
        return reject;
    }
    // `oauth_tokens` AND `calendar_prefs` are both CONTROL-tree tables, read
    // through the named control accessor. (Before the Google Calendar org
    // lift the `oauth_tokens` read went through `tenant.pool`, which was the
    // same physical pool on the only plane that reached here; under an org
    // `tenant.pool` is the TENANT pool, which holds no token row.)
    if tenant.pool.is_none() {
        return unavailable();
    }
    let Some(control_pool) = state.control_db().cloned() else {
        return unavailable();
    };
    let account_id = user.account_id;
    let owner = calendar_owner(&tenant, &user);

    let (connected, email) = match connection_info(&control_pool, account_id, owner).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %account_id, "calendar status query failed");
            return internal_error();
        }
    };

    // `calendar_prefs` has one row PER ACCOUNT. Under an org that would be one
    // preference shared by every member (and any member flipping it would
    // flip it for everyone), so an org member's status reports the default
    // (reminders on) and `set_reminders` is refused under org — a per-member
    // preference needs an owner column on `calendar_prefs`, a follow-up.
    let reminders = if tenant.org_context().is_some() {
        true
    } else {
        match reminders_pref(&control_pool, account_id).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, account_id = %account_id, "calendar reminders pref query failed");
                return internal_error();
            }
        }
    };

    (StatusCode::OK, Json(status_body(connected, email, reminders))).into_response()
}

// ---------------------------------------------------------------------------
// PUT /v1/calendar/reminders
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct RemindersBody {
    /// Whether to fire pre-meeting notifications.
    pub enabled: bool,
}

/// Set the pre-meeting reminder preference and return the updated status.
pub async fn set_reminders(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<RemindersBody>,
) -> Response {
    if let Some(reject) = reject_org_calendar_access(&state, &tenant).await {
        return reject;
    }
    // See `status`: `calendar_prefs` is one row per account, so under an org
    // the toggle would silently apply to every member. Refuse it there rather
    // than let one member change another's reminders.
    if tenant.org_context().is_some() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "pre-meeting reminders are a personal-workspace preference for now; \
                          switch to your personal workspace to change them"
            })),
        )
            .into_response();
    }
    // Same plane split as `status` (see its comment): both the prefs upsert
    // and the connection lookup below are CONTROL-plane.
    if tenant.pool.is_none() {
        return unavailable();
    }
    let Some(control_pool) = state.control_db().cloned() else {
        return unavailable();
    };
    let account_id = user.account_id;
    let owner = calendar_owner(&tenant, &user);

    let res = sqlx::query(
        r#"
        INSERT INTO calendar_prefs (account_id, premeeting_reminders, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (account_id)
        DO UPDATE SET premeeting_reminders = EXCLUDED.premeeting_reminders,
                      updated_at = now()
        "#,
    )
    .bind(account_id)
    .bind(body.enabled)
    .execute(&control_pool)
    .await;

    if let Err(e) = res {
        tracing::warn!(error = %e, account_id = %account_id, "failed to upsert calendar reminders pref");
        return internal_error();
    }

    // Reflect the connected state back so the client can re-render in one round-trip.
    // A lookup failure here must surface (500 + log), NOT masquerade as
    // "disconnected" — a transient pool error right after a successful upsert would
    // otherwise misreport a connected account.
    let (connected, email) = match connection_info(&control_pool, account_id, owner).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %account_id, "calendar connection lookup failed after reminders upsert");
            return internal_error();
        }
    };

    (
        StatusCode::OK,
        Json(status_body(connected, email, body.enabled)),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Whether a Google calendar is connected for this account, and the associated
/// account email. Shared by `status` and `set_reminders` so both surface the same
/// connection state and propagate query errors identically (never silently report
/// "disconnected" on a transient failure).
async fn connection_info(
    pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    owner: TokenOwner,
) -> Result<(bool, Option<String>), sqlx::Error> {
    // RLS scoping (org-tenancy conversion): `oauth_tokens` carries an
    // `account_id` policy (migration 0083) — read inside a
    // `begin_account_scoped` transaction (inert pre-OPS-4). The scope
    // failure maps onto this function's existing sqlx::Error surface.
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("connection_info scoped tx: {e}")))?;
    // Foundation PR 1.1: an EXACT owner match, never `IS NULL OR`. Personal:
    // `owner_user_id IS NULL` (the account-wide row, byte-identical to
    // before). Org (Google Calendar org lift): the caller's OWN row — another
    // member's connection never reads as "connected" for this caller.
    let row: Option<Option<String>> = sqlx::query_scalar(
        r#"SELECT provider_workspace_id FROM oauth_tokens
           WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NOT DISTINCT FROM $3
           ORDER BY updated_at DESC LIMIT 1"#,
    )
    .bind(account_id)
    .bind(GOOGLE)
    .bind(owner.owner_user_id())
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    let connected = row.is_some();
    let email = row
        .flatten()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    Ok((connected, email))
}

/// Read the pre-meeting reminder preference, defaulting to `true` when no row
/// exists (reminders are on by default once a calendar is connected).
async fn reminders_pref(pool: &sqlx::PgPool, account_id: uuid::Uuid) -> Result<bool, sqlx::Error> {
    let row: Option<bool> = sqlx::query_scalar(
        "SELECT premeeting_reminders FROM calendar_prefs WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or(true))
}

fn status_body(connected: bool, email: Option<String>, reminders: bool) -> serde_json::Value {
    json!({
        "connected": connected,
        "email": email,
        "premeeting_reminders": reminders,
    })
}

fn not_connected() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "google calendar is not connected"})),
    )
        .into_response()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
        .into_response()
}

fn internal_error() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "internal error"})),
    )
        .into_response()
}

/// Map a [`ConnectorError`] to the HTTP status the connector routes use.
fn connector_error_response(e: ConnectorError) -> Response {
    match e {
        ConnectorError::NotConfigured(msg) => {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": msg}))).into_response()
        }
        ConnectorError::NotImplemented(msg) => {
            (StatusCode::NOT_IMPLEMENTED, Json(json!({"error": msg}))).into_response()
        }
        ConnectorError::Exchange(msg) | ConnectorError::UpstreamStatus { message: msg, .. } => {
            tracing::warn!(error = %msg, "calendar upstream failure");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "failed to reach calendar provider"})),
            )
                .into_response()
        }
        ConnectorError::Internal(msg) => {
            tracing::error!(error = %msg, "calendar internal failure");
            internal_error()
        }
        ConnectorError::CredentialRevoked { provider, reason } => {
            // CS-453: same `409` mapping as `routes::connectors`'s own
            // `connector_error_response` — this route shares `web/lib/api.ts`
            // `req()` 401-refresh handling. Google Calendar's connector
            // never raises this today (only Microsoft/Slack classify a
            // revoked credential yet), so this arm exists purely to keep
            // the match exhaustive and future-proof.
            tracing::warn!(%provider, reason = reason.code(), "calendar credential revoked");
            (
                StatusCode::CONFLICT,
                Json(json!({"error": "credential revoked", "reason": reason.code()})),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — routing/auth guards (event shaping is unit-tested in connectors::google)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware,
        routing::{get, put},
        Router,
    };
    use tower::ServiceExt;

    fn router() -> Router {
        let state = crate::test_app_state();
        Router::new()
            .route("/v1/calendar/upcoming", get(upcoming))
            .route("/v1/calendar/status", get(status))
            .route("/v1/calendar/reminders", put(set_reminders))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn upcoming_requires_auth() {
        let resp = router()
            .oneshot(
                Request::builder()
                    .uri("/v1/calendar/upcoming")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn status_requires_auth() {
        let resp = router()
            .oneshot(
                Request::builder()
                    .uri("/v1/calendar/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn upcoming_with_bearer_no_db_returns_503() {
        // require_auth hits the missing pool and 503s before the handler.
        let resp = router()
            .oneshot(
                Request::builder()
                    .uri("/v1/calendar/upcoming")
                    .header("Authorization", "Bearer sometoken")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn reminders_requires_auth() {
        let resp = router()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/calendar/reminders")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"enabled":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
