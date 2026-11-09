//! `GET/PUT/DELETE /v1/me/private-key` — the caller's passphrase-wrapped
//! private-ocean key blob (ADR-0007 D5, cross-device enrollment). Opaque to the
//! server (it's wrapped under the user's passphrase on-device); stored so a new
//! device can fetch it and unlock with the passphrase. DELETE supports a
//! forgotten-passphrase reset (the client then generates a fresh key).

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::middleware::auth::UserContext;
use crate::AppState;

fn unavailable() -> (StatusCode, Json<Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
}

pub async fn get_private_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    let Some(pool) = &state.db else { return unavailable() };
    match sqlx::query_as::<_, (String,)>("SELECT blob FROM user_private_keys WHERE user_id = $1")
        .bind(user.user_id)
        .fetch_optional(pool)
        .await
    {
        Ok(Some((blob,))) => (StatusCode::OK, Json(json!({ "blob": blob }))),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({"error": "no private key enrolled"}))),
        Err(e) => {
            tracing::error!(error = %e, "get_private_key DB error");
            unavailable()
        }
    }
}

#[derive(Deserialize)]
pub struct PrivateKeyBody {
    pub blob: String,
}

pub async fn put_private_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PrivateKeyBody>,
) -> (StatusCode, Json<Value>) {
    if body.blob.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "blob required"})));
    }
    let Some(pool) = &state.db else { return unavailable() };
    match sqlx::query(
        "INSERT INTO user_private_keys (user_id, blob) VALUES ($1, $2) \
         ON CONFLICT (user_id) DO UPDATE SET blob = EXCLUDED.blob, updated_at = now()",
    )
    .bind(user.user_id)
    .bind(&body.blob)
    .execute(pool)
    .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => {
            tracing::error!(error = %e, "put_private_key DB error");
            unavailable()
        }
    }
}

pub async fn delete_private_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    let Some(pool) = &state.db else { return unavailable() };
    match sqlx::query("DELETE FROM user_private_keys WHERE user_id = $1")
        .bind(user.user_id)
        .execute(pool)
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => {
            tracing::error!(error = %e, "delete_private_key DB error");
            unavailable()
        }
    }
}
