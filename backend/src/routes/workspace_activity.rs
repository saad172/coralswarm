//! `GET /v1/home/workspace-activity?limit=6&since_days=7`: the Home
//! dashboard's "From your workspaces" card. A thin wrapper over
//! [`crate::services::workspace_activity`]; the contract is
//! `docs/implementation/workspace-activity-api.md`.
//!
//! Answers 404 while `HOME_DASHBOARD_ENABLED` is unset (read per request).
//! `Cache-Control: private, no-store`: the body is per-caller.

use axum::{
    extract::Query,
    response::{IntoResponse, Response},
    Extension,
};
use serde::Deserialize;
use utoipa::path as utoipa_path;

use crate::{middleware::auth::UserContext, tenancy::extractor::Tenant};

#[derive(Debug, Deserialize)]
pub struct WorkspaceActivityQuery {
    /// Parsed as signed integers so a negative or huge value is a 400 with the
    /// contract's message, not an extractor rejection.
    pub limit: Option<i64>,
    pub since_days: Option<i64>,
}

/// Recent activity in the shared workspaces the caller can read in this tenant.
#[utoipa_path(
    get,
    path = "/v1/home/workspace-activity",
    tag = "home",
    security(("bearer_token" = [])),
    params(
        ("limit" = Option<i64>, Query, description = "Max rows, 1..20 (default 6)"),
        ("since_days" = Option<i64>, Query, description = "Window length in days, 1..30 (default 7)"),
    ),
    responses(
        (status = 200, description = "Workspace activity", body = serde_json::Value),
        (status = 400, description = "Invalid limit or since_days"),
        (status = 404, description = "HOME_DASHBOARD_ENABLED is off"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_workspace_activity(
    state: axum::extract::State<std::sync::Arc<crate::AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    query: Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    crate::routes::page_read::serve::<crate::read_models::home::WorkspaceActivity>(
        state,
        tenant,
        user,
        original,
        axum::extract::Path(std::collections::BTreeMap::new()),
        query,
    )
    .await
    .into_response()
}
