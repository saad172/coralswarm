//! `POST /v1/ingest/atom`: the HTTP door to [`crate::ingest::submit`].

use axum::{extract::State, response::{IntoResponse, Response}, Extension, Json};
use std::sync::Arc;
use utoipa::path as utoipa_path;

use crate::{
    ingest::atom::{IngestAtomRequest, IngestAtomResponse, PROVENANCE_LABEL_CAP},
    middleware::auth::UserContext,
    tenancy::extractor::Tenant,
    AppState,
};

// ── Handler ───────────────────────────────────────────────────────────────────

/// Ingest a single atom into the knowledge graph.
///
/// Chunks the content, stores encrypted blobs in R2, inserts atom rows (idempotent),
/// and enqueues a background embedding job, all in one transaction. If R2 refuses a
/// body nothing is stored and the answer is a retryable 503.
#[utoipa_path(
    post,
    path = "/v1/ingest/atom",
    tag = "ingest",
    security(("bearer_token" = [])),
    request_body = IngestAtomRequest,
    responses(
        (status = 200, description = "Atoms ingested successfully", body = IngestAtomResponse),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden — graph not owned by caller's account"),
        (status = 422, description = "Unprocessable entity — missing or invalid fields"),
        (status = 503, description = "Database not available, or the object store refused a body: nothing was stored, and the body carries `\"code\": \"storage_unavailable\"`; retry the same request"),
    )
)]
pub async fn ingest_atom(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    headers: axum::http::HeaderMap,
    Json(mut req): Json<IngestAtomRequest>,
) -> Response {
    // Platform detection reads the transport's `User-Agent`, never the body
    // (see `IngestAtomRequest::trusted_user_agent`). Same trim + cap as the
    // MCP auth layer's `extract_client_name`.
    req.trusted_user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(PROVENANCE_LABEL_CAP).collect());
    // The HTTP path never anchors: anchoring is an in-process-only concern, so
    // external clients cannot drive reef linking.
    crate::ingest::submit(crate::ingest::IngestRequest::private(crate::ingest::Member::new(&state, &tenant, &user_ctx, req))).await.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::post,
        Router,
    };
    use tower::ServiceExt;
    use uuid::Uuid;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    // Helper to build a minimal router with require_auth wired in.
    fn ingest_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/ingest/atom", post(ingest_atom))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    // ── No Authorization header → 401 ─────────────────────────────────────────

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let router = ingest_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/ingest/atom")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "graph_id": Uuid::new_v4(),
                    "source_type": "slack",
                    "source_id": "msg_001",
                    "content": "hello world",
                    "participants": [],
                    "timestamp": "2024-01-01T00:00:00Z"
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Bearer token with no DB → 503 ────────────────────────────────────────

    #[tokio::test]
    async fn bearer_no_db_returns_503() {
        let router = ingest_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/ingest/atom")
            .header("Authorization", "Bearer sometoken")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "graph_id": Uuid::new_v4(),
                    "source_type": "slack",
                    "source_id": "msg_001",
                    "content": "hello world",
                    "participants": [],
                    "timestamp": "2024-01-01T00:00:00Z"
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // require_auth middleware returns 503 when db=None.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Missing auth → 401 regardless of body validity ───────────────────────
    //
    // The `require_auth` middleware returns 401 before the Json extractor even
    // runs, so a request with a completely malformed body still yields 401 when
    // no Authorization header is present.

    #[tokio::test]
    async fn no_auth_with_missing_fields_returns_401() {
        let router = ingest_router(test_state());

        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/ingest/atom")
            .header("Content-Type", "application/json")
            // missing all required fields AND no auth header
            .body(Body::from("{}"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // Auth middleware fires first → 401 before Json deserialization.
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Non-JSON body without auth → 401 ─────────────────────────────────────
    //
    // Verifies auth gate fires before any body parsing.

    #[tokio::test]
    async fn non_json_body_without_auth_returns_401() {
        let router = ingest_router(test_state());

        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/ingest/atom")
            // No Content-Type, no auth
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── fts_language integration tests (no DB — middleware returns before handler) ──

    #[tokio::test]
    async fn invalid_fts_language_without_auth_returns_401() {
        // Auth middleware fires before the handler, so an invalid language
        // with no bearer token still returns 401 (not 400).
        let router = ingest_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/ingest/atom")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "graph_id": Uuid::new_v4(),
                    "source_type": "slack",
                    "source_id": "msg_001",
                    "content": "hello world",
                    "participants": [],
                    "timestamp": "2024-01-01T00:00:00Z",
                    "fts_language": "klingon"
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn valid_fts_language_without_auth_returns_401() {
        // A valid fts_language with no auth token still yields 401.
        let router = ingest_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/ingest/atom")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_string(&serde_json::json!({
                    "graph_id": Uuid::new_v4(),
                    "source_type": "slack",
                    "source_id": "msg_001",
                    "content": "bonjour monde",
                    "participants": [],
                    "timestamp": "2024-01-01T00:00:00Z",
                    "fts_language": "french"
                }))
                .unwrap(),
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
