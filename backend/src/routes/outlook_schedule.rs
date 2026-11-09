//! Outlook compose capture v2 routes (`/v1/outlook-capture-schedule/*`).
//!
//! Contract: docs/plans/outlook-compose-capture-v2.md. Authentication is the ordinary
//! `/v1` `require_auth` path (Clerk session or the Microsoft SSO `csms_` credential);
//! there is no separate broker. Org workspace only: the intent, the capture it
//! governs and the transcripts live in the caller's own tenant database / member ocean.
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::{
    connectors::outlook_schedule as graph_events,
    middleware::auth::UserContext,
    routes::slack::{resolve_primary_graph, AccountPlane, IngestCaller},
    services::outlook_schedule::{self as svc, CreateError, Intent},
    tenancy::extractor::Tenant,
    AppState,
};

fn err(status: StatusCode, code: &str, msg: &str) -> Response {
    (status, Json(json!({"code": code, "error": msg}))).into_response()
}
fn unavailable() -> Response {
    err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable")
}
fn internal() -> Response {
    err(StatusCode::INTERNAL_SERVER_ERROR, "internal", "internal error")
}
fn not_found() -> Response {
    err(StatusCode::NOT_FOUND, "intent_not_found", "No such meeting capture")
}

/// Org workspace + Teams flag + the caller's own Microsoft identity.
async fn gate(
    state: &Arc<AppState>,
    tenant: &Tenant,
    ctx: &UserContext,
) -> Result<(sqlx::PgPool, String, String), Response> {
    if tenant.org_context().is_none() {
        return Err(err(
            StatusCode::FORBIDDEN,
            "organization_required",
            "Select your organization workspace for private meeting capture",
        ));
    }
    if let Some(refused) = crate::routes::teams_ingest::reject_org_teams_access(state, tenant).await {
        return Err(refused);
    }
    let Some(pool) = tenant.pool.clone() else {
        return Err(unavailable());
    };
    let Some(control) = state.control_db() else {
        return Err(unavailable());
    };
    match crate::routes::teams_onboarding::identity(control, ctx.account_id, ctx.user_id).await {
        Ok(Some((tid, aad))) => Ok((pool, tid, aad)),
        Ok(None) => Err(err(
            StatusCode::FORBIDDEN,
            "connect_required",
            "Connect your own Microsoft account first",
        )),
        Err(_) => Err(internal()),
    }
}

/// `POST /v1/outlook-capture-schedule/intents`
pub async fn create_intent(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
) -> Response {
    let (pool, tid, aad) = match gate(&state, &tenant, &ctx).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let graph = match resolve_primary_graph(
        &pool,
        ctx.account_id,
        AccountPlane::Org,
        IngestCaller::Attributable(ctx.user_id),
    )
    .await
    {
        Ok(g) => g,
        // The expected refusal: no private ocean provisioned for this member yet.
        Err(sqlx::Error::Protocol(msg)) if msg.contains("no primary member ocean") => {
            tracing::warn!(error = %msg, "outlook schedule: no member ocean yet");
            return err(
                StatusCode::CONFLICT,
                "member_ocean_unavailable",
                "Your private ocean is not ready yet; try again shortly",
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "outlook schedule: member ocean resolution failed");
            return internal();
        }
    };
    match svc::create(&pool, ctx.account_id, ctx.user_id, &tid, &aad, graph).await {
        Ok(c) => {
            // Wake the tenant worker: it polls the saved event on its own clock.
            if let Some(org) = tenant.org_context() {
                state.worker_activity.note_enqueue(&org.org_id);
            }
            (
                StatusCode::CREATED,
                Json(json!({"intent_id": c.id, "nonce": c.nonce, "expires_at": c.expires_at})),
            )
                .into_response()
        }
        Err(CreateError::TooManyPending) => err(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_pending",
            "Save or discard your other meetings before capturing another",
        ),
        Err(CreateError::TooManyOpen) => err(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_active",
            "Too many meetings are already being captured",
        ),
        Err(CreateError::RateLimited) => err(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Too many capture requests; try again later",
        ),
        Err(CreateError::Db(e)) => {
            tracing::error!(error = %e, "outlook schedule: create failed");
            internal()
        }
    }
}

fn view(intent: &Intent, with_id: bool) -> Response {
    let mut body = intent.view();
    if with_id {
        body["intent_id"] = json!(intent.id);
    }
    Json(body).into_response()
}

/// `GET /v1/outlook-capture-schedule/intents/{intent_id}`
pub async fn get_intent(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let (pool, _, _) = match gate(&state, &tenant, &ctx).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match svc::get(&pool, ctx.account_id, ctx.user_id, id).await {
        Ok(Some(i)) => view(&i, false),
        Ok(None) => not_found(),
        Err(_) => internal(),
    }
}

/// `DELETE /v1/outlook-capture-schedule/intents/{intent_id}`
pub async fn delete_intent(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    // Negative authority: a Stop needs only the org workspace, not a live Microsoft link.
    if tenant.org_context().is_none() {
        return err(StatusCode::FORBIDDEN, "organization_required", "Select your organization workspace");
    }
    let Some(pool) = tenant.pool.clone() else {
        return unavailable();
    };
    match svc::stop_by_user(&pool, ctx.account_id, ctx.user_id, id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => not_found(),
        Err(e) => {
            tracing::error!(error = %e, "outlook schedule: stop failed");
            internal()
        }
    }
}

/// `GET /v1/outlook-capture-schedule/by-nonce/{nonce}`
pub async fn by_nonce(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Path(nonce): Path<Uuid>,
) -> Response {
    let (pool, _, _) = match gate(&state, &tenant, &ctx).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match svc::by_nonce(&pool, ctx.account_id, ctx.user_id, nonce).await {
        Ok(Some(i)) => view(&i, true),
        Ok(None) => not_found(),
        Err(_) => internal(),
    }
}

#[derive(Deserialize)]
pub struct ByEventQuery {
    event_id: String,
}

/// `GET /v1/outlook-capture-schedule/by-event?event_id=...` (REST or immutable id).
pub async fn by_event(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Query(q): Query<ByEventQuery>,
) -> Response {
    let (pool, _, _) = match gate(&state, &tenant, &ctx).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    // Cheap path: the id is already the stored immutable one.
    match svc::by_event(&pool, ctx.account_id, ctx.user_id, &q.event_id).await {
        Ok(Some(i)) => return view(&i, true),
        Ok(None) => {}
        Err(_) => return internal(),
    }
    let (token, _) = match crate::routes::teams_ingest::microsoft_token(&state, ctx.account_id, ctx.user_id).await {
        Ok(t) => t,
        Err(r) => return r,
    };
    let immutable = match graph_events::immutable_id(
        &state.http_client,
        &token,
        crate::connectors::teams_targeted_transcripts::GRAPH,
        &q.event_id,
    )
    .await
    {
        Ok(Some(id)) => id,
        Ok(None) => return not_found(),
        Err(_) => return err(StatusCode::BAD_GATEWAY, "provider_unavailable", "Microsoft could not be reached"),
    };
    match svc::by_event(&pool, ctx.account_id, ctx.user_id, &immutable).await {
        Ok(Some(i)) => view(&i, true),
        Ok(None) => not_found(),
        Err(_) => internal(),
    }
}
