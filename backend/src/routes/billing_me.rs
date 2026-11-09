//! `GET /v1/billing/me`: the caller's own billing position. Any signed-in
//! member; no admin gate, because it returns only the caller's own figures
//! and never money. The contract and the reasoning are in
//! [`crate::billing::me`].

use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use utoipa::path as utoipa_path;

use crate::billing::me::{build, BillingMe};
use crate::middleware::auth::UserContext;
use crate::AppState;

#[utoipa_path(
    get,
    path = "/v1/billing/me",
    tag = "billing",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "The caller's plan credits, extra usage against their cap, and whether they are blocked (credits and percentages only, no dollars)", body = BillingMe),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_billing_me(State(state): State<Arc<AppState>>, Extension(user): Extension<UserContext>) -> Response {
    let Some(control) = state.control_db() else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "database unavailable"}))).into_response();
    };
    match build(control, user.account_id, user.user_id).await {
        Ok(me) => Json(me).into_response(),
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "billing.me: read failed");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
        }
    }
}
