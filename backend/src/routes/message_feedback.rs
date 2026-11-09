use axum::{
    extract::{rejection::JsonRejection, Path},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde_json::json;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    services::message_feedback::{self as mf, Feedback, FeedbackBody, FeedbackError},
    tenancy::extractor::Tenant,
};

fn err(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"error": msg}))).into_response()
}

fn not_found() -> Response {
    err(StatusCode::NOT_FOUND, "not found")
}

fn error_response(e: FeedbackError) -> Response {
    match e {
        FeedbackError::Invalid(m) => err(StatusCode::UNPROCESSABLE_ENTITY, &m),
        FeedbackError::NotFound => not_found(),
        FeedbackError::NotAnswer => err(
            StatusCode::UNPROCESSABLE_ENTITY,
            "feedback applies to an answer, not a question",
        ),
        FeedbackError::Unavailable => err(StatusCode::SERVICE_UNAVAILABLE, "service unavailable"),
        FeedbackError::Internal(e) => {
            tracing::error!(error = %e, "message feedback failed");
            err(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

/// Store the caller's vote on one answer. Idempotent.
#[utoipa_path(
    put,
    path = "/v1/conversations/{id}/messages/{message_id}/feedback",
    tag = "conversations",
    security(("bearer_token" = [])),
    params(
        ("id" = Uuid, Path, description = "Conversation id"),
        ("message_id" = Uuid, Path, description = "Assistant message id"),
    ),
    request_body(content = serde_json::Value, description = "{\"vote\":\"up\"|\"down\",\"reasons\":[\"wrong\"|\"missing_source\"|\"outdated\"|\"other\"],\"note\":string|null}"),
    responses(
        (status = 200, description = "The stored feedback {vote, reasons, note}", body = serde_json::Value),
        (status = 404, description = "MESSAGE_FEEDBACK_ENABLED is off, or no answer the caller can read"),
        (status = 422, description = "Invalid body, or the message is not an answer"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn put_feedback(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((conversation_id, message_id)): Path<(Uuid, Uuid)>,
    body: Result<Json<FeedbackBody>, JsonRejection>,
) -> Response {
    if !mf::enabled() {
        return not_found();
    }
    let feedback = match body.map_err(|r| FeedbackError::Invalid(r.body_text())).and_then(|Json(b)| Feedback::try_from(b)) {
        Ok(f) => f,
        Err(e) => return error_response(e),
    };
    match mf::upsert(&tenant, &user, conversation_id, message_id, &feedback).await {
        Ok(stored) => (StatusCode::OK, Json(stored)).into_response(),
        Err(e) => error_response(e),
    }
}

/// Clear the caller's vote on one answer. Idempotent.
#[utoipa_path(
    delete,
    path = "/v1/conversations/{id}/messages/{message_id}/feedback",
    tag = "conversations",
    security(("bearer_token" = [])),
    params(
        ("id" = Uuid, Path, description = "Conversation id"),
        ("message_id" = Uuid, Path, description = "Assistant message id"),
    ),
    responses(
        (status = 204, description = "No vote is stored"),
        (status = 404, description = "MESSAGE_FEEDBACK_ENABLED is off, or no answer the caller can read"),
        (status = 422, description = "The message is not an answer"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn delete_feedback(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((conversation_id, message_id)): Path<(Uuid, Uuid)>,
) -> Response {
    if !mf::enabled() {
        return not_found();
    }
    match mf::clear(&tenant, &user, conversation_id, message_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error_response(e),
    }
}
