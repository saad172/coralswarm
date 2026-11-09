//! `GET /v1/home/metrics?days=30&tz=Area/City` — the Home dashboard's data.
//! A thin wrapper over [`crate::services::home_metrics`]; the contract is
//! `docs/implementation/home-metrics-api.md`.
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
pub struct HomeMetricsQuery {
    /// Parsed as a signed integer so a negative or huge value is a 400 with
    /// the contract's message, not an extractor rejection.
    pub days: Option<i64>,
    pub tz: Option<String>,
}

/// Home dashboard metrics for the caller's readable oceans in this tenant.
#[utoipa_path(
    get,
    path = "/v1/home/metrics",
    tag = "home",
    security(("bearer_token" = [])),
    params(
        ("days" = Option<i64>, Query, description = "Window length in local days, 7..60 (default 30)"),
        ("tz" = Option<String>, Query, description = "IANA time zone for day buckets (default UTC)"),
    ),
    responses(
        (status = 200, description = "Home metrics", body = serde_json::Value),
        (status = 400, description = "Invalid days or tz"),
        (status = 404, description = "HOME_DASHBOARD_ENABLED is off"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_home_metrics(
    state: axum::extract::State<std::sync::Arc<crate::AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    query: Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    crate::routes::page_read::serve::<crate::read_models::home::HomeMetrics>(
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
