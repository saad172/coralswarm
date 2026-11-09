//! `GET /v1/connectors/status`: the HTTP edge of the Home dashboard's
//! connector health. Everything but authorization and mapping errors lives in
//! `services::connector_status` (state rule, thresholds, sources).
//!
//! 404 while `HOME_DASHBOARD_ENABLED` is unset. Any authenticated member may
//! call it; the rows are the caller's own view: account-wide connections plus
//! their own member connections (the visibility of `GET /v1/oauth/connections`,
//! including its per-org provider flags), and 30-day counts over the oceans the
//! caller can read. Never returns token material.
//!
//! `200 { connectors: [...], summary: { total, healthy, stale, failing } }`,
//! `Cache-Control: no-store`. 503 when the control plane is unavailable.

use crate::{middleware::auth::UserContext, tenancy::extractor::Tenant, AppState};
use axum::{
    response::{IntoResponse, Response},
    Extension,
};
use std::sync::Arc;

pub async fn status(
    state: axum::extract::State<Arc<AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    query: axum::extract::Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    crate::routes::page_read::serve::<crate::read_models::connectors::ConnectorStatus>(
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
