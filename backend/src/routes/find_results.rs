//! `GET /v1/find/titles`: the hidden Cmd-K title-only search over sources. The
//! read is `services::find_results` (module doc: kinds, access, ranking); this
//! file only authorizes and maps answers. The chat tiles themselves ride on the
//! scoped answer (`source_results`), attached by `routes::queries`.
//!
//! 404 while `CHAT_FIND_RESULTS_ENABLED` is unset.
//!
//! # Contract
//!
//! `GET /v1/find/titles?q=&limit=10` -> 200 `{ query, results: [SourceResult] }`
//! (same tile shape as the chat tiles; `best` is always false; `quotes` empty).
//! `q` is required (1..=200 characters), `limit` is clamped to 1..=25.
//! 400 on a missing or over-long `q`.
//!
//! Full shapes: `docs/implementation/find-results-api.md`.

use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;
use utoipa::path as utoipa_path;

use crate::{
    middleware::auth::UserContext, services::find_results as fr, tenancy::extractor::Tenant,
};

fn reply(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(body),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct TitleParams {
    pub q: Option<String>,
    pub limit: Option<i64>,
}

/// `GET /v1/find/titles`
#[utoipa_path(
    get,
    path = "/v1/find/titles",
    tag = "find",
    security(("bearer_token" = [])),
    params(
        ("q" = String, Query, description = "Title text to look for (case-insensitive substring), 1-200 characters"),
        ("limit" = Option<i64>, Query, description = "Most results (default 10, max 25)"),
    ),
    responses(
        (status = 200, description = "Matching sources as result tiles, exact and prefix matches first, then newest", body = serde_json::Value),
        (status = 400, description = "Missing or over-long q"),
        (status = 404, description = "CHAT_FIND_RESULTS_ENABLED is off"),
        (status = 500, description = "Internal error"),
    )
)]
pub async fn title_search(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<TitleParams>,
) -> Response {
    if !fr::enabled() {
        return reply(StatusCode::NOT_FOUND, json!({"error": "not found"}));
    }
    let q = match fr::parse_query(params.q.as_deref()) {
        Ok(q) => q,
        Err(m) => return reply(StatusCode::BAD_REQUEST, json!({"error": m})),
    };
    let limit = fr::parse_limit(params.limit);
    match fr::search_titles(&tenant, &user, &q, limit).await {
        Ok(results) => reply(StatusCode::OK, json!({"query": q, "results": results})),
        Err(e) => {
            tracing::error!(error = %e, "find titles failed");
            reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error": "internal error"}),
            )
        }
    }
}
