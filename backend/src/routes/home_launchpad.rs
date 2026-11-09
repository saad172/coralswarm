//! `GET /v1/home/continue`, `GET /v1/home/whats-new`, `GET /v1/home/attention`:
//! the Home launchpad's three reads. Thin wrappers over
//! [`crate::services::home_launchpad`]; the contract is
//! `docs/implementation/home-launchpad-api.md`.
//!
//! Mounted through `page_read::plain`: one member-scoped READ ONLY transaction
//! on the page lane with the 1 s statement budget. Not stored read models, so
//! never withheld on a fence and never answered with a placeholder. Answers
//! 404 while `HOME_DASHBOARD_ENABLED` is unset (read per request), 400 for
//! any query parameter, 503 when the database cannot answer.
//!
//! A request holds at most one pooled connection at a time. Attention also
//! reads the control plane, which on the personal plane is the same pool as
//! content, so it reads and releases the control facts before it opens the
//! content transaction.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use sqlx::{Postgres, Transaction};
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    read_models as rm,
    services::home_launchpad as home,
    tenancy::{extractor::Tenant, runtime::TenancyDecision},
    AppState,
};

fn failure(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

fn unavailable(error: impl std::fmt::Display) -> Response {
    tracing::warn!(error = %error, "home launchpad read unavailable");
    failure(StatusCode::SERVICE_UNAVAILABLE, "home unavailable")
}

/// Canonical scoped-SQL accounting records a PageRead timeout exactly once.
fn sql_unavailable(error: sqlx::Error) -> Response {
    unavailable(crate::store::StoreError::from(error))
}

fn service_unavailable(error: home::Error) -> Response {
    match error {
        home::Error::Db(e) => sql_unavailable(e),
        other => unavailable(other),
    }
}

/// Flag, parameters and actor: decided before any connection is taken.
fn gate(
    tenant: &Tenant,
    user: &UserContext,
    query: &BTreeMap<String, String>,
) -> Result<(), Response> {
    if !crate::services::home_metrics::enabled() {
        return Err(failure(StatusCode::NOT_FOUND, "not found"));
    }
    if !query.is_empty() {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "this read takes no query parameters",
        ));
    }
    if let Some(TenancyDecision::Org(ctx)) = &tenant.decision {
        if ctx.user_id != user.user_id || ctx.account_id != user.account_id {
            return Err(failure(StatusCode::FORBIDDEN, "actor mismatch"));
        }
    }
    Ok(())
}

/// The member-scoped content transaction and the viewer's visible oceans in
/// the active account (the graph set every Home read model is keyed by). The
/// only connection a request holds while it reads content.
async fn open_content(
    tenant: &Tenant,
    user: &UserContext,
) -> Result<(Transaction<'static, Postgres>, Vec<Uuid>), Response> {
    let mut tx = tenant
        .begin_member_scoped(user.account_id)
        .await
        .map_err(unavailable)?;
    crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id)
        .await
        .map_err(unavailable)?;
    let viewer = rm::Viewer::load(&mut tx, user.account_id, user.user_id)
        .await
        .map_err(unavailable)?;
    Ok((tx, viewer.visible_graphs()))
}

async fn open(
    tenant: &Tenant,
    user: &UserContext,
    query: &BTreeMap<String, String>,
) -> Result<(Transaction<'static, Postgres>, Vec<Uuid>), Response> {
    gate(tenant, user, query)?;
    open_content(tenant, user).await
}

/// The connector facts on the control plane, on a transaction that is rolled
/// back (its connection released) before this returns. A failed read is the
/// error returned, even when the rollback after it also fails (that one is
/// only logged), so a broken read is never reported as a rollback failure.
async fn control_facts(
    state: &AppState,
    user: &UserContext,
) -> Result<home::ControlFacts, Response> {
    let Some(control) = state.control_db() else {
        return Err(unavailable("control plane unavailable"));
    };
    let mut control_tx =
        crate::tenancy::scope::begin_request_account_read(control, user.account_id)
            .await
            .map_err(unavailable)?;
    crate::tenancy::scope::set_tenant_user(&mut control_tx, user.user_id)
        .await
        .map_err(unavailable)?;
    match home::attention_control_facts(&mut control_tx, user.account_id, user.user_id).await {
        Ok(facts) => {
            control_tx.rollback().await.map_err(sql_unavailable)?;
            Ok(facts)
        }
        Err(e) => {
            if let Err(rollback) = control_tx.rollback().await {
                tracing::warn!(error = %rollback, "home attention: control rollback after a failed read");
            }
            Err(service_unavailable(e))
        }
    }
}

async fn finish<T: serde::Serialize>(tx: Transaction<'static, Postgres>, body: T) -> Response {
    if let Err(e) = tx.rollback().await {
        return sql_unavailable(e);
    }
    Json(body).into_response()
}

/// The viewer's own recent items: chats, coding/agent sessions (same repo and
/// branch collapsed), meetings and workspaces. At most 6 rows, 3 per kind.
#[utoipa_path(
    get,
    path = "/v1/home/continue",
    tag = "home",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Continue rows", body = serde_json::Value),
        (status = 400, description = "Any query parameter"),
        (status = 404, description = "HOME_DASHBOARD_ENABLED is off"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_continue(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let (mut tx, graphs) = match open(&tenant, &user, &query).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match home::continue_items(&mut tx, &graphs, user.user_id, chrono::Utc::now()).await {
        Ok(body) => finish(tx, body).await,
        Err(e) => sql_unavailable(e),
    }
}

/// Up to 5 reefs formed in the last 7 days across the viewer's oceans:
/// decisions and threads first, then entities, newest first within each.
#[utoipa_path(
    get,
    path = "/v1/home/whats-new",
    tag = "home",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Recently formed reefs", body = serde_json::Value),
        (status = 400, description = "Any query parameter"),
        (status = 404, description = "HOME_DASHBOARD_ENABLED is off"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_whats_new(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let (mut tx, graphs) = match open(&tenant, &user, &query).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match home::whats_new(&mut tx, &graphs, chrono::Utc::now()).await {
        Ok(body) => finish(tx, body).await,
        Err(e) => sql_unavailable(e),
    }
}

/// Whether the viewer can see any content (setup checklist vs. Home), the
/// checklist's steps (`has_source`, `has_mcp`, `has_asked`), and the first
/// broken connector, if any. Reads the control plane first and releases it
/// before it opens the content transaction.
#[utoipa_path(
    get,
    path = "/v1/home/attention",
    tag = "home",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "has_content, has_source, has_mcp, has_asked and the first broken connector (or null)", body = serde_json::Value),
        (status = 400, description = "Any query parameter"),
        (status = 404, description = "HOME_DASHBOARD_ENABLED is off"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_attention(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    if let Err(r) = gate(&tenant, &user, &query) {
        return r;
    }
    // Control first, released, then content: never one pooled connection held
    // while waiting for another (on the personal plane both planes are the
    // same pool, so N concurrent requests holding content and waiting for
    // control could exhaust it).
    let facts = match control_facts(&state, &user).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    let (mut tx, graphs) = match open_content(&tenant, &user).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    match home::attention(
        &mut tx,
        facts,
        user.account_id,
        user.user_id,
        &graphs,
        chrono::Utc::now(),
    )
    .await
    {
        Ok(body) => finish(tx, body).await,
        Err(e) => service_unavailable(e),
    }
}
