//! `POST /v1/embed` — embed caller-supplied texts with the server cloud model.
//!
//! The browser private engine sends content here so local cosine search shares
//! the OpenRouter `openai/text-embedding-3-small@384` space. The API key stays
//! on the server. Local-only egress forbids this (ADR-0007 D11).

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{path as utoipa_path, ToSchema};

use crate::middleware::auth::UserContext;
use crate::AppState;

const MAX_BATCH: usize = 8;
const MAX_CHARS: usize = 32_000;

#[derive(Debug, Deserialize, ToSchema)]
pub struct EmbedRequest {
    pub texts: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct EmbedResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub model: String,
}

#[utoipa_path(
    post,
    path = "/v1/embed",
    tag = "embed",
    security(("bearer_token" = [])),
    request_body = EmbedRequest,
    responses(
        (status = 200, description = "Cloud embeddings", body = EmbedResponse),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Cloud egress forbidden"),
        (status = 503, description = "Embedder unavailable"),
    )
)]
pub async fn embed_texts(
    State(state): State<Arc<AppState>>,
    tenant: crate::tenancy::extractor::Tenant,
    Extension(user): Extension<UserContext>,
    Json(req): Json<EmbedRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    if req.texts.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "texts required"})),
        );
    }
    if req.texts.len() > MAX_BATCH {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("at most {MAX_BATCH} texts")})),
        );
    }
    if req.texts.iter().any(|t| t.chars().count() > MAX_CHARS) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("each text must be <= {MAX_CHARS} characters")})),
        );
    }
    let graph_id = if tenant.org_context().is_some() {
        let Some(pool) = tenant.pool.as_ref() else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "embedding context unavailable"})),
            );
        };
        match crate::routes::slack::resolve_primary_graph(
            pool,
            user.account_id,
            crate::routes::slack::AccountPlane::Org,
            crate::routes::slack::IngestCaller::Attributable(user.user_id),
        )
        .await
        {
            Ok(graph_id) => Some(graph_id),
            Err(error) => {
                tracing::warn!(%error, account_id = %user.account_id, "POST /v1/embed context unavailable");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error": "embedding context unavailable"})),
                );
            }
        }
    } else {
        None
    };
    let refs: Vec<&str> = req.texts.iter().map(String::as_str).collect();
    let usage = tenant.pool.clone().map(|pool| {
        crate::llm::usage::UsageContext::new(pool, Some(user.account_id), graph_id)
            .with_user(user.user_id)
            .with_attributed_user(user.user_id)
            .with_source_query(uuid::Uuid::new_v4())
    });
    let purpose = crate::embedding::Purpose::Metered { usage: usage.as_ref(), task: "desktop_embed" };
    match crate::embedding::submit(state.embedder.as_ref(), purpose, &refs).await {
        Ok(embeddings)
            if embeddings.len() == req.texts.len()
                && embeddings
                    .iter()
                    .all(|v| v.iter().any(|n| *n != 0.0)) =>
        {
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "embeddings": embeddings,
                    "model": coralswarm_core::embed::EMBED_MODEL_CLOUD,
                })),
            )
        }
        Ok(embeddings) if embeddings.len() == req.texts.len() => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "embedding failed"})),
        ),
        Ok(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "embedding failed"})),
        ),
        Err(e) => {
            tracing::error!(error = %e, "POST /v1/embed failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "embedding failed"})),
            )
        }
    }
}
