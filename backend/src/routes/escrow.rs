//! Org-escrow recovery storage (ADR-0007 D6), asymmetric per-user model. The
//! server stores only opaque, client-encrypted blobs + the recovery PUBLIC key,
//! and stays blind. The recovery private key is Shamir-split and held by admins
//! out-of-band — it (and the shares) NEVER touch the server, so it cannot
//! reconstruct anything.
//! - `PUT /v1/escrow/recovery-pubkey` — an admin publishes the org recovery
//!   public key + ceremony metadata (threshold/admin_count).
//! - `GET /v1/escrow/recovery-pubkey` — a member fetches it to wrap their key.
//! - `PUT /v1/escrow/wrapped-key` — a member stores their private-ocean key
//!   wrapped to the recovery public key.
//! - `GET /v1/escrow/wrapped-key?user_id=` — an admin fetches a member's wrapped
//!   key during recovery.
//! - `PUT /v1/escrow/recovered-key` — an admin re-delivers a recovered, re-wrapped
//!   key to a member's device-trust slot (membership-gated).
//! - `GET/POST /v1/escrow/recovery-log` — list / record recovery events (audit).
//!
//! Blobs are transported base64 and stored as BYTEA. Admin-scoped routes require
//! the active org role to be admin/owner.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::AppState;

fn unavailable() -> (StatusCode, Json<Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
}
fn bad(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })))
}
fn forbidden() -> (StatusCode, Json<Value>) {
    (StatusCode::FORBIDDEN, Json(json!({"error": "admin only"})))
}
/// Exact match, same as `routes::org_invites::is_admin`. `account_members.role`
/// is CHECK-constrained to `owner`/`admin`/`member` (migration 0001), so this
/// accepts exactly what the old substring match accepted for every stored
/// role, and refuses an unmapped Clerk role like `org:billing_admin`.
fn is_admin(role: &str) -> bool {
    // Settings launch PR 2.8: no admin/owner authority for a non-interactive
    // credential — see `middleware::auth::authority_allowed`.
    (role == "admin" || role == "owner") && crate::middleware::auth::authority_allowed()
}

#[derive(Deserialize)]
pub struct WrappedKeyBody {
    pub wrapped_key: String,
}

pub async fn put_wrapped_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<WrappedKeyBody>,
) -> (StatusCode, Json<Value>) {
    let Some(pool) = &state.db else { return unavailable() };
    let Ok(bytes) = STANDARD.decode(&body.wrapped_key) else {
        return bad("wrapped_key must be base64");
    };
    match sqlx::query(
        "INSERT INTO escrow_wrapped_keys (account_id, user_id, wrapped_key) VALUES ($1, $2, $3) \
         ON CONFLICT (account_id, user_id) DO UPDATE SET wrapped_key = EXCLUDED.wrapped_key, updated_at = now()",
    )
    .bind(user.account_id)
    .bind(user.user_id)
    .bind(&bytes)
    .execute(pool)
    .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => {
            tracing::error!(error = %e, "put_wrapped_key DB error");
            unavailable()
        }
    }
}

#[derive(Deserialize)]
pub struct UserQuery {
    pub user_id: Uuid,
}

pub async fn get_wrapped_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Query(q): Query<UserQuery>,
) -> (StatusCode, Json<Value>) {
    if !is_admin(&user.role) {
        return forbidden();
    }
    let Some(pool) = &state.db else { return unavailable() };
    match sqlx::query_as::<_, (Vec<u8>,)>(
        "SELECT wrapped_key FROM escrow_wrapped_keys WHERE account_id = $1 AND user_id = $2",
    )
    .bind(user.account_id)
    .bind(q.user_id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some((b,))) => (StatusCode::OK, Json(json!({ "wrapped_key": STANDARD.encode(b) }))),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))),
        Err(e) => {
            tracing::error!(error = %e, "get_wrapped_key DB error");
            unavailable()
        }
    }
}

// NOTE: Shamir shares of the recovery private key are NEVER sent to or stored by
// the server. They are distributed to admins out-of-band at the setup ceremony
// and pasted back at recovery, so the server cannot reconstruct the recovery key
// and stays blind (ADR-0007 D6). Hence there is no share-storage endpoint.

#[derive(Deserialize)]
pub struct RecoveryPubkeyBody {
    pub pubkey: String,
    pub threshold: i32,
    pub admin_count: i32,
}

/// Publish the org recovery PUBLIC key + ceremony metadata (admin only). Members
/// fetch it to wrap their own private-ocean key at enrollment.
pub async fn put_recovery_pubkey(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<RecoveryPubkeyBody>,
) -> (StatusCode, Json<Value>) {
    if !is_admin(&user.role) {
        return forbidden();
    }
    if body.threshold < 2 || body.admin_count < body.threshold {
        return bad("require 2 <= threshold <= admin_count");
    }
    let Some(pool) = &state.db else { return unavailable() };
    let Ok(bytes) = STANDARD.decode(&body.pubkey) else {
        return bad("pubkey must be base64");
    };
    match sqlx::query(
        "INSERT INTO escrow_recovery_pubkey (account_id, pubkey, threshold, admin_count) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (account_id) DO UPDATE SET pubkey = EXCLUDED.pubkey, \
             threshold = EXCLUDED.threshold, admin_count = EXCLUDED.admin_count, updated_at = now()",
    )
    .bind(user.account_id)
    .bind(&bytes)
    .bind(body.threshold)
    .bind(body.admin_count)
    .execute(pool)
    .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => {
            tracing::error!(error = %e, "put_recovery_pubkey DB error");
            unavailable()
        }
    }
}

/// Fetch the org recovery public key + ceremony metadata. Any authenticated
/// member of the active account may read it (it is public by design). 404 when
/// recovery has not been set up for this account.
pub async fn get_recovery_pubkey(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    let Some(pool) = &state.db else { return unavailable() };
    match sqlx::query_as::<_, (Vec<u8>, i32, i32)>(
        "SELECT pubkey, threshold, admin_count FROM escrow_recovery_pubkey WHERE account_id = $1",
    )
    .bind(user.account_id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some((b, threshold, admin_count))) => (
            StatusCode::OK,
            Json(json!({
                "pubkey": STANDARD.encode(b),
                "threshold": threshold,
                "admin_count": admin_count,
            })),
        ),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({"error": "recovery not set up"}))),
        Err(e) => {
            tracing::error!(error = %e, "get_recovery_pubkey DB error");
            unavailable()
        }
    }
}

#[derive(Deserialize)]
pub struct RecoveryLogBody {
    pub recovered_user_id: Uuid,
}

pub async fn post_recovery_log(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<RecoveryLogBody>,
) -> (StatusCode, Json<Value>) {
    if !is_admin(&user.role) {
        return forbidden();
    }
    let Some(pool) = &state.db else { return unavailable() };

    // The target must belong to the admin's active account — same IDOR guard
    // as `put_recovered_key` below. Without this, a malicious/compromised
    // admin could log a recovery against a foreign-tenant user id.
    match sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2",
    )
    .bind(user.account_id)
    .bind(body.recovered_user_id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(_)) => {}
        Ok(None) => return forbidden(),
        Err(e) => {
            tracing::error!(error = %e, "post_recovery_log membership check error");
            return unavailable();
        }
    }

    // Snapshot identity AT RECOVERY TIME so a departed member can still be
    // named in the audit trail later, rather than resolving against the
    // (possibly stale) org roster on every read. Scoped to the caller's
    // account via the account_members join so a foreign user id can never
    // resolve to an email here (defense in depth alongside the membership
    // check above). A real DB error is logged and treated the same as a
    // legitimate miss — the column is nullable and the frontend already
    // falls back gracefully.
    let recovered_user_email: Option<String> = match sqlx::query_scalar::<_, String>(
        "SELECT u.email FROM users u \
         JOIN account_members am ON am.user_id = u.id \
         WHERE u.id = $1 AND am.account_id = $2",
    )
    .bind(body.recovered_user_id)
    .bind(user.account_id)
    .fetch_optional(pool)
    .await
    {
        Ok(email) => email,
        Err(e) => {
            tracing::warn!(error = %e, "post_recovery_log recovered_user_email lookup error");
            None
        }
    };
    // The initiator is the authenticated caller, whose account membership is
    // already established by `require_auth` — no cross-tenant risk here.
    let initiated_by_email: Option<String> =
        match sqlx::query_scalar::<_, String>("SELECT email FROM users WHERE id = $1")
            .bind(user.user_id)
            .fetch_optional(pool)
            .await
        {
            Ok(email) => email,
            Err(e) => {
                tracing::warn!(error = %e, "post_recovery_log initiated_by_email lookup error");
                None
            }
        };

    match sqlx::query(
        "INSERT INTO escrow_recovery_log \
             (account_id, recovered_user_id, initiated_by, recovered_user_email, initiated_by_email) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(user.account_id)
    .bind(body.recovered_user_id)
    .bind(user.user_id)
    .bind(&recovered_user_email)
    .bind(&initiated_by_email)
    .execute(pool)
    .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => {
            tracing::error!(error = %e, "post_recovery_log DB error");
            unavailable()
        }
    }
}

#[derive(Deserialize)]
pub struct RecoveredKeyBody {
    pub user_id: Uuid,
    /// The recovered ocean key re-wrapped under a fresh recovery passphrase
    /// (opaque device-trust blob, same shape as `/v1/me/private-key`).
    pub blob: String,
}

/// Re-deliver a recovered private-ocean key to a member (admin only). The admin
/// reconstructs the member's ocean key client-side (M-of-N), re-wraps it under a
/// fresh recovery passphrase, and writes the opaque blob to the member's
/// device-trust slot so they can re-enroll with that passphrase. The target must
/// be a member of the admin's active account (IDOR guard); the server never sees
/// plaintext.
pub async fn put_recovered_key(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<RecoveredKeyBody>,
) -> (StatusCode, Json<Value>) {
    if !is_admin(&user.role) {
        return forbidden();
    }
    if body.blob.is_empty() {
        return bad("blob required");
    }
    let Some(pool) = &state.db else { return unavailable() };

    // The target must belong to the admin's active account.
    match sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2",
    )
    .bind(user.account_id)
    .bind(body.user_id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(_)) => {}
        Ok(None) => return forbidden(),
        Err(e) => {
            tracing::error!(error = %e, "put_recovered_key membership check error");
            return unavailable();
        }
    }

    match sqlx::query(
        "INSERT INTO user_private_keys (user_id, blob) VALUES ($1, $2) \
         ON CONFLICT (user_id) DO UPDATE SET blob = EXCLUDED.blob, updated_at = now()",
    )
    .bind(body.user_id)
    .bind(&body.blob)
    .execute(pool)
    .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(e) => {
            tracing::error!(error = %e, "put_recovered_key DB error");
            unavailable()
        }
    }
}

/// List recent recovery events for the active account (admin only) — the audit
/// trail surfaced in the recovery UI. Newest first, capped.
pub async fn list_recovery_log(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    if !is_admin(&user.role) {
        return forbidden();
    }
    let Some(pool) = &state.db else { return unavailable() };
    match sqlx::query_as::<
        _,
        (
            Uuid,
            Uuid,
            Option<Uuid>,
            Option<String>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
        ),
    >(
        "SELECT id, recovered_user_id, initiated_by, recovered_user_email, initiated_by_email, created_at \
         FROM escrow_recovery_log WHERE account_id = $1 ORDER BY created_at DESC LIMIT 100",
    )
    .bind(user.account_id)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            let events: Vec<Value> = rows
                .into_iter()
                .map(
                    |(
                        id,
                        recovered_user_id,
                        initiated_by,
                        recovered_user_email,
                        initiated_by_email,
                        created_at,
                    )| {
                        json!({
                            "id": id,
                            "recovered_user_id": recovered_user_id,
                            "initiated_by": initiated_by,
                            // Snapshotted at recovery time (may be null for rows
                            // written before this identity-enrichment migration).
                            "recovered_user_email": recovered_user_email,
                            "initiated_by_email": initiated_by_email,
                            "created_at": created_at.to_rfc3339(),
                        })
                    },
                )
                .collect();
            (StatusCode::OK, Json(json!({ "events": events })))
        }
        Err(e) => {
            tracing::error!(error = %e, "list_recovery_log DB error");
            unavailable()
        }
    }
}
