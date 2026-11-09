//! Active-member management for organization accounts (settings launch PR 2.2):
//! - `DELETE /v1/orgs/members/{user_id}` — remove a member from the org.
//! - `PATCH  /v1/orgs/members/{user_id}` — change a member's role
//!   (`admin` <-> `member`).
//!
//! `/v1/account/members/{user_id}` 403s for org accounts by design
//! (`routes::teams::reject_org_account_membership_management`), and
//! `DELETE /v1/orgs/invites/{user_id}` only handles members who have never
//! signed in. This module is the admin path for everyone else.
//!
//! Clerk is the source of truth for membership: both endpoints call Clerk's
//! organization-membership API first. The `organizationMembership.deleted` /
//! `.updated` webhooks would eventually sync the local `account_members` row,
//! but both endpoints ALSO write the local state synchronously, for two
//! reasons that are about correctness rather than convenience:
//!
//! * **Remove.** Webhook delivery is asynchronous and unordered. Until it
//!   lands, the removed member's still-valid Clerk JWT (which carries the
//!   org claim) could keep them in the org: `account_members` is the row every
//!   request re-checks (`middleware::auth::live_org_membership`), and the
//!   per-org tombstone is what stops the claims refresher re-creating that row
//!   from the stale JWT. So we run the exact offboarding routine the webhook
//!   runs ([`crate::clerk_provision::remove_org_membership`]: tombstone, row
//!   delete, seat release, write-grant clear, member-ocean archive intent,
//!   connector-health handoff) before returning. It is idempotent, so the
//!   webhook that follows is a no-op.
//! * **Role change.** Every admin-gated endpoint reads the live
//!   `account_members.role`. A demoted admin keeps admin powers until the
//!   webhook lands unless we write the role now.
//!
//! Guards (each a distinct `error` code, see the response helpers): not
//! yourself, not the org owner (`accounts.owner_user_id`), and never leave the
//! org with zero admins.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::json;
use utoipa::path as utoipa_path;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::clerk_admin::{ClerkAdminClient, ClerkAdminError};
use crate::middleware::auth::UserContext;
use crate::routes::org_invites::{map_clerk_error, map_role, require_org_admin};
use crate::tenancy::extractor::Tenant;
use crate::AppState;

fn err(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({"error": code, "message": message}))).into_response()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
        .into_response()
}

fn internal(step: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step, "org_members: failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "internal error"})),
    )
        .into_response()
}

/// What the guards need to know about the target member.
struct TargetMember {
    clerk_user_id: Option<String>,
    role: String,
    is_owner: bool,
}

async fn load_target(
    control: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<Option<TargetMember>, sqlx::Error> {
    // `is_owner` reads `accounts.owner_user_id`, the org's real ownership
    // record (same as `org_invites::revoke_invite`); a stray `'owner'` role on
    // the membership row is treated as owner too.
    let row: Option<(Option<String>, String, bool)> = sqlx::query_as(
        "SELECT u.clerk_user_id, am.role, \
                (COALESCE(a.owner_user_id = am.user_id, false) OR am.role = 'owner') AS is_owner \
         FROM account_members am \
         JOIN users u ON u.id = am.user_id \
         JOIN accounts a ON a.id = am.account_id \
         WHERE am.account_id = $1 AND am.user_id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_optional(control)
    .await?;
    Ok(row.map(|(clerk_user_id, role, is_owner)| TargetMember {
        clerk_user_id,
        role,
        is_owner,
    }))
}

/// Would taking admin rights away from `target` (removal or demotion) leave
/// the org without any other admin? Counts the OTHER `admin`/`owner` rows
/// straight from the database rather than trusting the caller's request
/// context, so a caller demoted between authentication and this check no
/// longer counts as the surviving admin.
async fn leaves_no_admin(
    conn: &mut sqlx::PgConnection,
    account_id: Uuid,
    target: Uuid,
) -> Result<bool, sqlx::Error> {
    let others: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM account_members \
         WHERE account_id = $1 AND user_id <> $2 AND role IN ('admin', 'owner')",
    )
    .bind(account_id)
    .bind(target)
    .fetch_one(conn)
    .await?;
    Ok(others == 0)
}

fn target_is_admin(role: &str) -> bool {
    crate::routes::org_invites::is_admin(role)
}

fn clerk_404(e: &ClerkAdminError) -> bool {
    matches!(e, ClerkAdminError::Api { status: 404, .. })
}

/// Clerk refuses to drop the last member holding the minimum required
/// permissions (`organization_minimum_permissions_needed`). Surface it as our
/// own `last_admin` rather than a pass-through Clerk status.
fn clerk_last_admin(e: &ClerkAdminError) -> bool {
    matches!(e, ClerkAdminError::Api { code: Some(c), .. } if c.contains("minimum_permissions"))
}

fn last_admin_error() -> Response {
    err(
        StatusCode::CONFLICT,
        "last_admin",
        "The organization must keep at least one admin.",
    )
}

/// Serialize admin-affecting changes per org. The lock is transaction-scoped
/// (`pg_advisory_xact_lock`, never the session-level variant — the Neon
/// pooler multiplexes sessions) and is held across the admin count AND the
/// Clerk call, so two admins demoting each other cannot both pass the guard.
async fn begin_org_lock(
    control: &sqlx::PgPool,
    account_id: Uuid,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    let mut tx = control.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('org_members:' || $1::text, 0))")
        .bind(account_id)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

/// A previous (partially failed) removal already tombstoned this member.
async fn already_removed(
    control: &sqlx::PgPool,
    org_id: &str,
    user_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM org_membership_tombstones \
                         WHERE clerk_org_id = $1 AND user_id = $2)",
    )
    .bind(org_id)
    .bind(user_id)
    .fetch_one(control)
    .await
}

// ── DELETE /v1/orgs/members/{user_id} ───────────────────────────────────────

#[utoipa_path(
    delete,
    path = "/v1/orgs/members/{user_id}",
    tag = "teams",
    security(("bearer_token" = [])),
    params(("user_id" = Uuid, Path, description = "target member's control-plane user id (from GET /v1/orgs/members)")),
    responses(
        (status = 200, description = "Removed", body = serde_json::Value),
        (status = 400, description = "error = cannot_remove_self | cannot_remove_owner"),
        (status = 403, description = "Not an org context, or caller is not an admin"),
        (status = 404, description = "No such member of this organization"),
        (status = 409, description = "error = last_admin"),
        (status = 502, description = "Clerk API error"),
        (status = 503, description = "Database, or Clerk API key, unavailable"),
    )
)]
pub async fn remove_org_member(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let org_id = ctx.org_id.clone();
    let account_id = ctx.account_id;
    debug_assert_eq!(account_id, user.account_id);
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let Some(clerk) = ClerkAdminClient::from_env() else {
        return unavailable();
    };

    if target_user_id == user.user_id {
        return err(
            StatusCode::BAD_REQUEST,
            "cannot_remove_self",
            "You cannot remove yourself from the organization.",
        );
    }

    // Everything from the admin count to the end of local offboarding runs
    // under the per-org advisory lock (see `begin_org_lock`).
    let mut lock = match begin_org_lock(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal("org lock", e),
    };

    let target = match load_target(control, account_id, target_user_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            // A retry after Clerk succeeded but local cleanup failed leaves a
            // tombstone and no row: that is "already removed", not a 404.
            // A user who was never a member of THIS org has no tombstone here.
            return match already_removed(control, &org_id, target_user_id).await {
                Ok(true) => (
                    StatusCode::OK,
                    Json(json!({"removed": true, "user_id": target_user_id})),
                )
                    .into_response(),
                Ok(false) => err(
                    StatusCode::NOT_FOUND,
                    "member_not_found",
                    "No such member of this organization.",
                ),
                Err(e) => internal("tombstone lookup", e),
            };
        }
        Err(e) => return internal("member lookup", e),
    };
    let Some(clerk_user_id) = target.clerk_user_id else {
        return err(
            StatusCode::NOT_FOUND,
            "member_not_found",
            "Member has no linked identity.",
        );
    };
    if target.is_owner {
        return err(
            StatusCode::BAD_REQUEST,
            "cannot_remove_owner",
            "The organization owner cannot be removed.",
        );
    }
    if target_is_admin(&target.role) {
        match leaves_no_admin(&mut lock, account_id, target_user_id).await {
            Ok(true) => return last_admin_error(),
            Ok(false) => {}
            Err(e) => return internal("admin count", e),
        }
    }

    // Clerk first (source of truth). A 404 means the membership is already
    // gone upstream (a retry after a partial failure) — carry on to the local
    // cleanup instead of wedging the member in place.
    if let Err(e) = clerk.delete_org_membership(&org_id, &clerk_user_id).await {
        if clerk_last_admin(&e) {
            return last_admin_error();
        }
        if !clerk_404(&e) {
            return map_clerk_error("delete_org_membership", e);
        }
    }

    // Synchronous local revocation — see the module doc for why we do not
    // wait for the webhook.
    if let Err(e) =
        crate::clerk_provision::remove_org_membership(&state, control, &org_id, &clerk_user_id)
            .await
    {
        tracing::error!(error = %e, %org_id, "org_members: local offboarding failed after Clerk delete");
        return err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "local_cleanup_failed",
            "The member was removed from the identity provider but local cleanup failed; retry the request.",
        );
    }
    if let Err(e) = lock.commit().await {
        tracing::warn!(error = %e, "org_members: releasing the org lock failed");
    }

    (
        StatusCode::OK,
        Json(json!({"removed": true, "user_id": target_user_id})),
    )
        .into_response()
}

// ── PATCH /v1/orgs/members/{user_id} ────────────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct ChangeRoleRequest {
    /// `"admin"` or `"member"`.
    pub role: String,
}

#[utoipa_path(
    patch,
    path = "/v1/orgs/members/{user_id}",
    tag = "teams",
    security(("bearer_token" = [])),
    params(("user_id" = Uuid, Path, description = "target member's control-plane user id (from GET /v1/orgs/members)")),
    request_body = ChangeRoleRequest,
    responses(
        (status = 200, description = "Role set; `changed` is false when it already matched", body = serde_json::Value),
        (status = 400, description = "error = invalid_role | cannot_change_own_role | cannot_change_owner_role"),
        (status = 403, description = "Not an org context, or caller is not an admin"),
        (status = 404, description = "No such member of this organization"),
        (status = 409, description = "error = last_admin"),
        (status = 502, description = "Clerk API error"),
        (status = 503, description = "Database, or Clerk API key, unavailable"),
    )
)]
pub async fn change_org_member_role(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
    Json(body): Json<ChangeRoleRequest>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let org_id = ctx.org_id.clone();
    let account_id = ctx.account_id;
    debug_assert_eq!(account_id, user.account_id);
    let Some((new_role, clerk_role)) = map_role(&body.role) else {
        return err(
            StatusCode::BAD_REQUEST,
            "invalid_role",
            "role must be \"admin\" or \"member\".",
        );
    };
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let Some(clerk) = ClerkAdminClient::from_env() else {
        return unavailable();
    };

    if target_user_id == user.user_id {
        return err(
            StatusCode::BAD_REQUEST,
            "cannot_change_own_role",
            "You cannot change your own role.",
        );
    }

    let mut lock = match begin_org_lock(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal("org lock", e),
    };

    let target = match load_target(control, account_id, target_user_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return err(
                StatusCode::NOT_FOUND,
                "member_not_found",
                "No such member of this organization.",
            )
        }
        Err(e) => return internal("member lookup", e),
    };
    let Some(clerk_user_id) = target.clerk_user_id else {
        return err(
            StatusCode::NOT_FOUND,
            "member_not_found",
            "Member has no linked identity.",
        );
    };
    if target.is_owner {
        return err(
            StatusCode::BAD_REQUEST,
            "cannot_change_owner_role",
            "The organization owner's role cannot be changed.",
        );
    }
    if target.role == new_role {
        return (
            StatusCode::OK,
            Json(json!({"role": new_role, "changed": false})),
        )
            .into_response();
    }
    if new_role == "member" && target_is_admin(&target.role) {
        match leaves_no_admin(&mut lock, account_id, target_user_id).await {
            Ok(true) => return last_admin_error(),
            Ok(false) => {}
            Err(e) => return internal("admin count", e),
        }
    }

    if let Err(e) = clerk
        .update_org_membership_role(&org_id, &clerk_user_id, clerk_role)
        .await
    {
        if clerk_last_admin(&e) {
            return last_admin_error();
        }
        return map_clerk_error("update_org_membership_role", e);
    }

    // Synchronous local write — a demoted admin must lose admin powers now,
    // not when the webhook lands (see module doc). The webhook that follows
    // takes the role from Clerk's current state, so it cannot undo this.
    if let Err(e) = sqlx::query(
        "UPDATE account_members SET role = $3 WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(target_user_id)
    .bind(new_role)
    .execute(&mut *lock)
    .await
    {
        tracing::error!(error = %e, %org_id, "org_members: local role write failed after Clerk update");
        return err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "local_cleanup_failed",
            "The role was changed in the identity provider but the local update failed; retry the request.",
        );
    }
    if let Err(e) = lock.commit().await {
        tracing::error!(error = %e, %org_id, "org_members: committing the role write failed");
        return err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "local_cleanup_failed",
            "The role was changed in the identity provider but the local update failed; retry the request.",
        );
    }

    (
        StatusCode::OK,
        Json(json!({"role": new_role, "changed": true})),
    )
        .into_response()
}

#[cfg(test)]
mod pg_tests {
    mod retained_database {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/retained_database.rs"
        ));
    }
    use super::*;
    use crate::tenancy::runtime::{TenancyDecision, TenantContext};
    use axum::extract::{Path as AxumPath, State as AxumState};
    use axum::routing::delete as axum_delete;
    use serde_json::Value;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, PgConnection, PgPool};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex as AsyncMutex;

    /// Serializes tests that point `CLERK_API_BASE_URL` at their own mock.
    /// Also takes the process-wide lock `org_invites`' pg tests take, because
    /// they mutate the same process-global Clerk env vars under a lock of
    /// their own.
    static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        pool
    }

    #[allow(dead_code)]
    struct EnvGuard {
        _resend: std::sync::MutexGuard<'static, ()>,
        _env: tokio::sync::MutexGuard<'static, ()>,
    }
    impl EnvGuard {
        async fn acquire(clerk_base: &str) -> Self {
            let resend = crate::services::unmatched_recipient_email::RESEND_BASE_URL_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let guard = ENV_LOCK.lock().await;
            // SAFETY: serialized by both guards, held for this struct's lifetime.
            unsafe {
                std::env::set_var("CLERK_API_BASE_URL", clerk_base);
                std::env::set_var("CLERK_SECRET_KEY", "sk_test_mock_org_members");
            }
            Self {
                _resend: resend,
                _env: guard,
            }
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: still serialized by the guards.
            unsafe {
                std::env::remove_var("CLERK_API_BASE_URL");
                std::env::remove_var("CLERK_SECRET_KEY");
            }
        }
    }

    async fn body_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    // ── Clerk Backend API mock (membership delete + role patch only) ────────

    #[derive(Default)]
    struct ClerkMock {
        deleted_memberships: Vec<(String, String)>,
        patched_roles: Vec<(String, String, String)>,
        /// When set, both calls answer with this status instead of 200.
        fail_status: Option<u16>,
        /// Clerk error `code` used with `fail_status`.
        fail_code: Option<String>,
        /// Delay applied to the role PATCH, to force overlapping requests.
        patch_delay_ms: u64,
        /// Clerk's current memberships: `(org_id, clerk_user_id, role)`.
        memberships: Vec<(String, String, String)>,
        /// When set, the user-memberships listing answers with this status.
        list_fail_status: Option<u16>,
        /// One-shot pause for the user-memberships listing: the handler reads
        /// the memberships, signals `.0`, then waits on `.1` before replying.
        /// Lets a test act between "Clerk answered" and "the webhook writes".
        list_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    }
    type SharedClerkMock = std::sync::Arc<StdMutex<ClerkMock>>;

    async fn spawn_clerk_mock(initial: ClerkMock) -> (String, SharedClerkMock) {
        let state: SharedClerkMock = std::sync::Arc::new(StdMutex::new(initial));
        let app = axum::Router::new()
            .route(
                "/v1/organizations/{org_id}/memberships/{user_id}",
                axum_delete(clerk_delete_membership).patch(clerk_patch_membership),
            )
            .route(
                "/v1/users/{user_id}/organization_memberships",
                axum::routing::get(clerk_list_user_memberships),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind clerk mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), state)
    }

    fn mock_failure(status: u16, code: Option<&str>) -> Response {
        (
            StatusCode::from_u16(status).unwrap(),
            Json(json!({"errors": [{"code": code.unwrap_or("mock_error"), "message": "m", "long_message": "m"}]})),
        )
            .into_response()
    }

    async fn clerk_list_user_memberships(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath(user_id): AxumPath<String>,
    ) -> Response {
        let (data, gate) = {
            let mut s = state.lock().unwrap();
            if let Some(st) = s.list_fail_status {
                return mock_failure(st, None);
            }
            let data: Vec<Value> = s
                .memberships
                .iter()
                .filter(|(_, u, _)| *u == user_id)
                .map(|(o, _, r)| json!({"organization": {"id": o}, "role": r}))
                .collect();
            (data, s.list_gate.take())
        };
        if let Some((listed, release)) = gate {
            listed.notify_one();
            release.notified().await;
        }
        let total = data.len();
        (StatusCode::OK, Json(json!({"data": data, "total_count": total}))).into_response()
    }

    async fn clerk_delete_membership(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath((org_id, user_id)): AxumPath<(String, String)>,
    ) -> Response {
        let mut s = state.lock().unwrap();
        s.deleted_memberships.push((org_id.clone(), user_id.clone()));
        match s.fail_status {
            Some(st) => mock_failure(st, s.fail_code.as_deref()),
            None => {
                s.memberships.retain(|(o, u, _)| !(*o == org_id && *u == user_id));
                (StatusCode::OK, Json(json!({}))).into_response()
            }
        }
    }

    async fn clerk_patch_membership(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath((org_id, user_id)): AxumPath<(String, String)>,
        Json(body): Json<Value>,
    ) -> Response {
        let delay = state.lock().unwrap().patch_delay_ms;
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        let mut s = state.lock().unwrap();
        let role = body["role"].as_str().unwrap_or("").to_string();
        s.patched_roles
            .push((org_id.clone(), user_id.clone(), role.clone()));
        match s.fail_status {
            Some(st) => mock_failure(st, s.fail_code.as_deref()),
            None => {
                for m in s.memberships.iter_mut() {
                    if m.0 == org_id && m.1 == user_id {
                        m.2 = role.clone();
                    }
                }
                (StatusCode::OK, Json(json!({}))).into_response()
            }
        }
    }

    // ── Fixtures ────────────────────────────────────────────────────────────

    struct Org {
        org_id: String,
        account_id: Uuid,
        /// `accounts.owner_user_id`, also an `admin` member.
        owner: Uuid,
        /// A second, non-owner admin (the usual caller).
        admin: Uuid,
        admin_clerk: String,
        owner_clerk: String,
    }

    async fn seed_user(control: &PgPool, clerk_user_id: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{}@org-members-pg.test", Uuid::new_v4()))
            .bind(clerk_user_id)
            .fetch_one(control)
            .await
            .expect("insert user")
    }

    async fn add_member(control: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(role)
        .execute(control)
        .await
        .expect("insert membership");
    }

    async fn seed_org(control: &PgPool) -> Org {
        let org_id = format!("org_om_{}", Uuid::new_v4());
        let owner_clerk = format!("user_owner_{}", Uuid::new_v4());
        let admin_clerk = format!("user_admin_{}", Uuid::new_v4());
        let owner = seed_user(control, &owner_clerk).await;
        let admin = seed_user(control, &admin_clerk).await;
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&org_id)
        .fetch_one(control)
        .await
        .expect("insert org account");
        add_member(control, account_id, owner, "admin").await;
        add_member(control, account_id, admin, "admin").await;
        Org {
            org_id,
            account_id,
            owner,
            admin,
            admin_clerk,
            owner_clerk,
        }
    }

    async fn seed_plain_member(control: &PgPool, org: &Org, role: &str) -> (Uuid, String) {
        let clerk_id = format!("user_target_{}", Uuid::new_v4());
        let id = seed_user(control, &clerk_id).await;
        add_member(control, org.account_id, id, role).await;
        (id, clerk_id)
    }

    fn user_ctx(org: &Org, user_id: Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id: org.account_id,
            role: role.to_string(),
            device_id: "org-members-pg-test".to_string(),
        }
    }

    fn org_tenant(control: &PgPool, org: &Org, user_id: Uuid) -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: org.org_id.clone(),
                account_id: org.account_id,
                user_id,
                pool: control.clone(),
                page_pool: None,
            })),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn app_state(control: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(control, None))
    }

    async fn role_of(control: &PgPool, account_id: Uuid, user_id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .fetch_optional(control)
            .await
            .unwrap()
    }

    /// Per-test scratch DB + Clerk mock + env.
    struct Fixture {
        conn: PgConnection,
        db: String,
        control: PgPool,
        org: Org,
        clerk: SharedClerkMock,
        _env: EnvGuard,
    }

    async fn fixture(db: &str, mock: ClerkMock) -> Option<Fixture> {
        let Some(base) = base_url() else {
            eprintln!("skipping {db}: set ORGT_TEST_BASE_URL");
            return None;
        };
        let mut conn = PgConnection::connect(&base).await.expect("connect admin");
        retained_database::recreate(&mut conn, db).await.unwrap();
        let control = control_pool(&base, db).await;
        let org = seed_org(&control).await;
        let (clerk_base, clerk) = spawn_clerk_mock(mock).await;
        let env = EnvGuard::acquire(&clerk_base).await;
        Some(Fixture {
            conn,
            db: db.to_string(),
            control,
            org,
            clerk,
            _env: env,
        })
    }

    impl Fixture {
        /// Tell the Clerk mock this user currently holds `role` in the org.
        fn clerk_has(&self, clerk_user_id: &str, role: &str) {
            self.clerk.lock().unwrap().memberships.push((
                self.org.org_id.clone(),
                clerk_user_id.to_string(),
                role.to_string(),
            ));
        }
        fn payload(&self, clerk_user_id: &str, role: &str) -> Value {
            json!({
                "organization": {"id": self.org.org_id, "name": "Acme"},
                "public_user_data": {"user_id": clerk_user_id, "identifier": "x@org-members-pg.test"},
                "role": role,
            })
        }
        async fn replay(&self, clerk_user_id: &str, payload_role: &str, created: bool) -> Result<(), crate::clerk_provision::ProvisionError> {
            crate::routes::clerk_webhook::handle_membership_upsert(
                &app_state(self.control.clone()),
                &self.control,
                &self.payload(clerk_user_id, payload_role),
                created,
            )
            .await
        }
        async fn remove(&self, caller: Uuid, caller_role: &str, target: Uuid) -> Response {
            remove_org_member(
                State(app_state(self.control.clone())),
                org_tenant(&self.control, &self.org, caller),
                Extension(user_ctx(&self.org, caller, caller_role)),
                Path(target),
            )
            .await
        }
        async fn patch(
            &self,
            caller: Uuid,
            caller_role: &str,
            target: Uuid,
            role: &str,
        ) -> Response {
            change_org_member_role(
                State(app_state(self.control.clone())),
                org_tenant(&self.control, &self.org, caller),
                Extension(user_ctx(&self.org, caller, caller_role)),
                Path(target),
                Json(ChangeRoleRequest {
                    role: role.to_string(),
                }),
            )
            .await
        }
        fn no_clerk_calls(&self) -> bool {
            let m = self.clerk.lock().unwrap();
            m.deleted_memberships.is_empty() && m.patched_roles.is_empty()
        }
        async fn finish(mut self) {
            self.control.close().await;
            retained_database::finish(&mut self.conn, &self.db, true)
                .await
                .ok();
        }
    }

    async fn assert_error(resp: Response, status: StatusCode, code: &str) {
        assert_eq!(resp.status(), status);
        let body = body_json(resp).await;
        assert_eq!(body["error"], json!(code), "body: {body}");
    }

    // ── DELETE ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn remove_member_calls_clerk_and_revokes_access_immediately() {
        let Some(f) = fixture("orgt_om_remove_ok", ClerkMock::default()).await else {
            return;
        };
        let (target, target_clerk) = seed_plain_member(&f.control, &f.org, "member").await;
        // Precondition: the same live check every request goes through.
        assert!(
            crate::middleware::auth::live_org_membership(&f.control, &f.org.org_id, target)
                .await
                .unwrap()
                .is_some()
        );

        let resp = f.remove(f.org.admin, "admin", target).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_json(resp).await["removed"], json!(true));

        assert_eq!(
            f.clerk.lock().unwrap().deleted_memberships,
            vec![(f.org.org_id.clone(), target_clerk)]
        );
        // Access is gone without any webhook having run.
        assert!(
            crate::middleware::auth::live_org_membership(&f.control, &f.org.org_id, target)
                .await
                .unwrap()
                .is_none()
        );
        // The tombstone that stops a still-valid JWT re-creating the row.
        let tombstones: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM org_membership_tombstones WHERE clerk_org_id = $1 AND user_id = $2",
        )
        .bind(&f.org.org_id)
        .bind(target)
        .fetch_one(&f.control)
        .await
        .unwrap();
        assert_eq!(tombstones, 1);
        // Member-ocean archive intent recorded for the reconciler.
        let archived: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM member_ocean_desired_state \
             WHERE clerk_org_id = $1 AND user_id = $2 AND state = 'archived'",
        )
        .bind(&f.org.org_id)
        .bind(target)
        .fetch_one(&f.control)
        .await
        .unwrap();
        assert_eq!(archived, 1);
        f.finish().await;
    }

    #[tokio::test]
    async fn remove_member_is_retryable_when_clerk_already_dropped_the_membership() {
        let mock = ClerkMock {
            fail_status: Some(404),
            ..Default::default()
        };
        let Some(f) = fixture("orgt_om_remove_404", mock).await else {
            return;
        };
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        let resp = f.remove(f.org.admin, "admin", target).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(role_of(&f.control, f.org.account_id, target).await.is_none());
        f.finish().await;
    }

    #[tokio::test]
    async fn remove_member_clerk_failure_leaves_local_state_untouched() {
        let mock = ClerkMock {
            fail_status: Some(500),
            ..Default::default()
        };
        let Some(f) = fixture("orgt_om_remove_5xx", mock).await else {
            return;
        };
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        assert_error(
            f.remove(f.org.admin, "admin", target).await,
            StatusCode::BAD_GATEWAY,
            "clerk_error",
        )
        .await;
        assert_eq!(
            role_of(&f.control, f.org.account_id, target).await.as_deref(),
            Some("member")
        );
        f.finish().await;
    }

    #[tokio::test]
    async fn remove_member_refuses_non_admin_caller() {
        let Some(f) = fixture("orgt_om_remove_nonadmin", ClerkMock::default()).await else {
            return;
        };
        let (caller, _) = seed_plain_member(&f.control, &f.org, "member").await;
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        assert_error(
            f.remove(caller, "member", target).await,
            StatusCode::FORBIDDEN,
            "admin only",
        )
        .await;
        assert!(f.no_clerk_calls());
        f.finish().await;
    }

    #[tokio::test]
    async fn remove_member_refuses_self() {
        let Some(f) = fixture("orgt_om_remove_self", ClerkMock::default()).await else {
            return;
        };
        assert_error(
            f.remove(f.org.admin, "admin", f.org.admin).await,
            StatusCode::BAD_REQUEST,
            "cannot_remove_self",
        )
        .await;
        assert!(f.no_clerk_calls());
        f.finish().await;
    }

    #[tokio::test]
    async fn remove_member_refuses_owner() {
        let Some(f) = fixture("orgt_om_remove_owner", ClerkMock::default()).await else {
            return;
        };
        assert_error(
            f.remove(f.org.admin, "admin", f.org.owner).await,
            StatusCode::BAD_REQUEST,
            "cannot_remove_owner",
        )
        .await;
        assert!(f.no_clerk_calls());
        assert!(role_of(&f.control, f.org.account_id, f.org.owner).await.is_some());
        f.finish().await;
    }

    #[tokio::test]
    async fn remove_member_unknown_target_is_404() {
        let Some(f) = fixture("orgt_om_remove_404u", ClerkMock::default()).await else {
            return;
        };
        assert_error(
            f.remove(f.org.admin, "admin", Uuid::new_v4()).await,
            StatusCode::NOT_FOUND,
            "member_not_found",
        )
        .await;
        f.finish().await;
    }

    /// The caller's request context says admin, but their row was demoted
    /// (concurrently, by another admin) after authentication. They must not
    /// count as the surviving admin, so removing the only OTHER admin is
    /// refused.
    #[tokio::test]
    async fn remove_member_refuses_to_remove_the_last_admin() {
        let Some(f) = fixture("orgt_om_remove_last", ClerkMock::default()).await else {
            return;
        };
        // The owner is the only DB admin left once the caller is demoted.
        // Make the owner a non-owner-protected target by clearing ownership
        // onto the (demoted) caller so the owner guard does not fire first.
        sqlx::query("UPDATE accounts SET owner_user_id = $2 WHERE id = $1")
            .bind(f.org.account_id)
            .bind(f.org.admin)
            .execute(&f.control)
            .await
            .unwrap();
        sqlx::query("UPDATE account_members SET role = 'member' WHERE account_id = $1 AND user_id = $2")
            .bind(f.org.account_id)
            .bind(f.org.admin)
            .execute(&f.control)
            .await
            .unwrap();
        assert_error(
            f.remove(f.org.admin, "admin", f.org.owner).await,
            StatusCode::CONFLICT,
            "last_admin",
        )
        .await;
        assert!(f.no_clerk_calls());
        f.finish().await;
    }

    // ── PATCH ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn change_role_promotes_and_demotes_via_clerk_and_locally() {
        let Some(f) = fixture("orgt_om_role_ok", ClerkMock::default()).await else {
            return;
        };
        let (target, target_clerk) = seed_plain_member(&f.control, &f.org, "member").await;

        let resp = f.patch(f.org.admin, "admin", target, "admin").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body, json!({"role": "admin", "changed": true}));
        assert_eq!(
            role_of(&f.control, f.org.account_id, target).await.as_deref(),
            Some("admin")
        );

        let resp = f.patch(f.org.admin, "admin", target, "member").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            role_of(&f.control, f.org.account_id, target).await.as_deref(),
            Some("member")
        );
        assert_eq!(
            f.clerk.lock().unwrap().patched_roles,
            vec![
                (f.org.org_id.clone(), target_clerk.clone(), "org:admin".to_string()),
                (f.org.org_id.clone(), target_clerk, "org:member".to_string()),
            ]
        );
        f.finish().await;
    }

    #[tokio::test]
    async fn change_role_to_the_current_role_is_a_noop() {
        let Some(f) = fixture("orgt_om_role_noop", ClerkMock::default()).await else {
            return;
        };
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        let resp = f.patch(f.org.admin, "admin", target, "member").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_json(resp).await["changed"], json!(false));
        assert!(f.no_clerk_calls());
        f.finish().await;
    }

    #[tokio::test]
    async fn change_role_clerk_failure_leaves_local_role_untouched() {
        let mock = ClerkMock {
            fail_status: Some(422),
            ..Default::default()
        };
        let Some(f) = fixture("orgt_om_role_clerkfail", mock).await else {
            return;
        };
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        let resp = f.patch(f.org.admin, "admin", target, "admin").await;
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            role_of(&f.control, f.org.account_id, target).await.as_deref(),
            Some("member")
        );
        f.finish().await;
    }

    #[tokio::test]
    async fn change_role_refuses_non_admin_caller() {
        let Some(f) = fixture("orgt_om_role_nonadmin", ClerkMock::default()).await else {
            return;
        };
        let (caller, _) = seed_plain_member(&f.control, &f.org, "member").await;
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        assert_error(
            f.patch(caller, "member", target, "admin").await,
            StatusCode::FORBIDDEN,
            "admin only",
        )
        .await;
        assert!(f.no_clerk_calls());
        f.finish().await;
    }

    #[tokio::test]
    async fn change_role_refuses_invalid_role() {
        let Some(f) = fixture("orgt_om_role_invalid", ClerkMock::default()).await else {
            return;
        };
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        for bad in ["owner", "org:admin", ""] {
            assert_error(
                f.patch(f.org.admin, "admin", target, bad).await,
                StatusCode::BAD_REQUEST,
                "invalid_role",
            )
            .await;
        }
        assert!(f.no_clerk_calls());
        f.finish().await;
    }

    #[tokio::test]
    async fn change_role_refuses_self() {
        let Some(f) = fixture("orgt_om_role_self", ClerkMock::default()).await else {
            return;
        };
        assert_error(
            f.patch(f.org.admin, "admin", f.org.admin, "member").await,
            StatusCode::BAD_REQUEST,
            "cannot_change_own_role",
        )
        .await;
        assert!(f.no_clerk_calls());
        f.finish().await;
    }

    #[tokio::test]
    async fn change_role_refuses_owner() {
        let Some(f) = fixture("orgt_om_role_owner", ClerkMock::default()).await else {
            return;
        };
        assert_error(
            f.patch(f.org.admin, "admin", f.org.owner, "member").await,
            StatusCode::BAD_REQUEST,
            "cannot_change_owner_role",
        )
        .await;
        assert!(f.no_clerk_calls());
        assert_eq!(
            role_of(&f.control, f.org.account_id, f.org.owner).await.as_deref(),
            Some("admin")
        );
        f.finish().await;
    }

    #[tokio::test]
    async fn change_role_unknown_target_is_404() {
        let Some(f) = fixture("orgt_om_role_404", ClerkMock::default()).await else {
            return;
        };
        assert_error(
            f.patch(f.org.admin, "admin", Uuid::new_v4(), "admin").await,
            StatusCode::NOT_FOUND,
            "member_not_found",
        )
        .await;
        f.finish().await;
    }

    /// Same concurrent-demotion shape as the remove test: the caller's row is
    /// no longer admin, so demoting the only other admin must be refused.
    #[tokio::test]
    async fn change_role_refuses_to_demote_the_last_admin() {
        let Some(f) = fixture("orgt_om_role_last", ClerkMock::default()).await else {
            return;
        };
        sqlx::query("UPDATE accounts SET owner_user_id = $2 WHERE id = $1")
            .bind(f.org.account_id)
            .bind(f.org.admin)
            .execute(&f.control)
            .await
            .unwrap();
        sqlx::query("UPDATE account_members SET role = 'member' WHERE account_id = $1 AND user_id = $2")
            .bind(f.org.account_id)
            .bind(f.org.admin)
            .execute(&f.control)
            .await
            .unwrap();
        assert_error(
            f.patch(f.org.admin, "admin", f.org.owner, "member").await,
            StatusCode::CONFLICT,
            "last_admin",
        )
        .await;
        assert!(f.no_clerk_calls());
        assert_eq!(
            role_of(&f.control, f.org.account_id, f.org.owner).await.as_deref(),
            Some("admin")
        );
        // Silence unused-field warnings for fixture fields only some tests read.
        let _ = (&f.org.admin_clerk, &f.org.owner_clerk);
        f.finish().await;
    }

    // ── Webhook ordering (security review) ──────────────────────────────────

    /// A late `.updated` / `.created` for a member who was just removed must
    /// not resurrect them (and must not clear the removal tombstone).
    #[tokio::test]
    async fn stale_membership_webhooks_cannot_resurrect_a_removed_member() {
        let Some(f) = fixture("orgt_om_stale_resurrect", ClerkMock::default()).await else {
            return;
        };
        let (target, target_clerk) = seed_plain_member(&f.control, &f.org, "admin").await;
        f.clerk_has(&target_clerk, "org:admin");
        assert_eq!(f.remove(f.org.admin, "admin", target).await.status(), StatusCode::OK);
        assert!(role_of(&f.control, f.org.account_id, target).await.is_none());

        for created in [false, true] {
            f.replay(&target_clerk, "org:admin", created).await.unwrap();
            assert!(
                crate::middleware::auth::live_org_membership(&f.control, &f.org.org_id, target)
                    .await
                    .unwrap()
                    .is_none(),
                "a stale membership webhook (created={created}) resurrected the removed member"
            );
        }
        let tombstones: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM org_membership_tombstones WHERE clerk_org_id = $1 AND user_id = $2",
        )
        .bind(&f.org.org_id)
        .bind(target)
        .fetch_one(&f.control)
        .await
        .unwrap();
        assert_eq!(tombstones, 1, "the tombstone must survive a stale delivery");
        f.finish().await;
    }

    /// A stale `.updated` carrying the OLD role must not undo a role change:
    /// the webhook takes the role from Clerk's current membership.
    #[tokio::test]
    async fn stale_updated_webhook_cannot_undo_a_role_change() {
        let Some(f) = fixture("orgt_om_stale_role", ClerkMock::default()).await else {
            return;
        };
        let (target, target_clerk) = seed_plain_member(&f.control, &f.org, "member").await;
        f.clerk_has(&target_clerk, "org:member");
        assert_eq!(
            f.patch(f.org.admin, "admin", target, "admin").await.status(),
            StatusCode::OK
        );
        // Late delivery of the earlier "member" event.
        f.replay(&target_clerk, "org:member", false).await.unwrap();
        assert_eq!(
            role_of(&f.control, f.org.account_id, target).await.as_deref(),
            Some("admin")
        );
        // And the reverse: a stale "admin" payload after a demotion.
        assert_eq!(
            f.patch(f.org.admin, "admin", target, "member").await.status(),
            StatusCode::OK
        );
        f.replay(&target_clerk, "org:admin", false).await.unwrap();
        assert_eq!(
            role_of(&f.control, f.org.account_id, target).await.as_deref(),
            Some("member")
        );
        f.finish().await;
    }

    /// If Clerk cannot be asked, the webhook fails (5xx, Svix retries) rather
    /// than guessing from the payload.
    #[tokio::test]
    async fn membership_webhook_fails_closed_when_clerk_lookup_fails() {
        let mock = ClerkMock {
            list_fail_status: Some(500),
            ..Default::default()
        };
        let Some(f) = fixture("orgt_om_webhook_failclosed", mock).await else {
            return;
        };
        let (target, target_clerk) = seed_plain_member(&f.control, &f.org, "member").await;
        sqlx::query("DELETE FROM account_members WHERE user_id = $1")
            .bind(target)
            .execute(&f.control)
            .await
            .unwrap();
        assert!(f.replay(&target_clerk, "org:admin", false).await.is_err());
        assert!(role_of(&f.control, f.org.account_id, target).await.is_none());
        f.finish().await;
    }

    async fn tombstone_count(control: &PgPool, org_id: &str, user_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM org_membership_tombstones WHERE clerk_org_id = $1 AND user_id = $2",
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_one(control)
        .await
        .unwrap()
    }

    /// Settings PR 2.9, residual race: the webhook reads "member" from Clerk,
    /// THEN an admin removes the member, THEN the webhook writes. Without the
    /// removal fence the webhook clears the fresh tombstone and re-inserts the
    /// row, and nothing removes them again. With it the webhook fails
    /// (retryable), and the retry re-reads Clerk and skips.
    #[tokio::test]
    async fn removal_between_clerk_lookup_and_write_does_not_resurrect() {
        let listed = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let mock = ClerkMock {
            list_gate: Some((listed.clone(), release.clone())),
            ..Default::default()
        };
        let Some(f) = fixture("orgt_om_removal_race", mock).await else {
            return;
        };
        let (target, target_clerk) = seed_plain_member(&f.control, &f.org, "member").await;
        f.clerk_has(&target_clerk, "org:member");

        let (webhook, removed) = tokio::join!(
            f.replay(&target_clerk, "org:member", false),
            async {
                // Clerk has answered "member" to the webhook; remove now.
                listed.notified().await;
                let status = f.remove(f.org.admin, "admin", target).await.status();
                release.notify_one();
                status
            }
        );
        assert_eq!(removed, StatusCode::OK);
        assert!(
            matches!(webhook, Err(crate::clerk_provision::ProvisionError::RemovedSinceObserved)),
            "the webhook must refuse (retryably), got {webhook:?}"
        );
        assert!(
            role_of(&f.control, f.org.account_id, target).await.is_none(),
            "the in-flight webhook resurrected the removed member"
        );
        assert_eq!(
            tombstone_count(&f.control, &f.org.org_id, target).await,
            1,
            "the in-flight webhook cleared the removal tombstone"
        );

        // Svix's redelivery re-reads Clerk (no membership now) and skips.
        f.replay(&target_clerk, "org:member", false).await.unwrap();
        assert!(role_of(&f.control, f.org.account_id, target).await.is_none());
        assert_eq!(tombstone_count(&f.control, &f.org.org_id, target).await, 1);
        f.finish().await;
    }

    /// End to end: remove, then a genuine re-invite through Clerk restores
    /// the member and clears the (older) tombstone.
    #[tokio::test]
    async fn reinvite_after_removal_restores_the_member() {
        let Some(f) = fixture("orgt_om_reinvite", ClerkMock::default()).await else {
            return;
        };
        let (target, target_clerk) = seed_plain_member(&f.control, &f.org, "member").await;
        f.clerk_has(&target_clerk, "org:member");
        assert_eq!(f.remove(f.org.admin, "admin", target).await.status(), StatusCode::OK);
        assert_eq!(tombstone_count(&f.control, &f.org.org_id, target).await, 1);

        f.clerk_has(&target_clerk, "org:admin");
        f.replay(&target_clerk, "org:member", true).await.unwrap();
        assert_eq!(
            role_of(&f.control, f.org.account_id, target).await.as_deref(),
            Some("admin"),
            "re-invite restores the member with Clerk's role"
        );
        assert_eq!(tombstone_count(&f.control, &f.org.org_id, target).await, 0);
        f.finish().await;
    }

    /// No Clerk key: outside `APP_ENV=development` the webhook refuses
    /// (5xx, Svix retries) instead of trusting the payload role; in
    /// development the payload fallback still works.
    #[tokio::test]
    async fn membership_webhook_without_clerk_key_refuses_outside_dev() {
        let Some(f) = fixture("orgt_om_no_clerk_key", ClerkMock::default()).await else {
            return;
        };
        let (target, target_clerk) = seed_plain_member(&f.control, &f.org, "member").await;
        sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(f.org.account_id)
            .bind(target)
            .execute(&f.control)
            .await
            .unwrap();
        // SAFETY: the fixture holds the process-wide env locks until `finish`.
        unsafe { std::env::remove_var("CLERK_SECRET_KEY") };

        let prod_like = crate::test_app_state_owned_with_db_and_store(f.control.clone(), None);
        assert!(!prod_like.env.is_dev);
        let r = crate::routes::clerk_webhook::handle_membership_upsert(
            &prod_like,
            &f.control,
            &f.payload(&target_clerk, "org:admin"),
            true,
        )
        .await;
        assert!(r.is_err(), "no Clerk key outside dev must be a retryable error");
        assert!(role_of(&f.control, f.org.account_id, target).await.is_none());

        let mut dev = crate::test_app_state_owned_with_db_and_store(f.control.clone(), None);
        dev.env.is_dev = true;
        crate::routes::clerk_webhook::handle_membership_upsert(
            &dev,
            &f.control,
            &f.payload(&target_clerk, "org:admin"),
            true,
        )
        .await
        .unwrap();
        assert_eq!(role_of(&f.control, f.org.account_id, target).await.as_deref(), Some("admin"));
        f.finish().await;
    }

    // ── Hardening ───────────────────────────────────────────────────────────

    /// A second DELETE after a successful removal is a success, with no
    /// second Clerk call.
    #[tokio::test]
    async fn remove_member_retry_after_success_is_idempotent() {
        let Some(f) = fixture("orgt_om_remove_idem", ClerkMock::default()).await else {
            return;
        };
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        assert_eq!(f.remove(f.org.admin, "admin", target).await.status(), StatusCode::OK);
        let resp = f.remove(f.org.admin, "admin", target).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_json(resp).await["removed"], json!(true));
        assert_eq!(f.clerk.lock().unwrap().deleted_memberships.len(), 1);
        f.finish().await;
    }

    /// A user id from ANOTHER org is a 404 with no Clerk call and no
    /// "already removed" success.
    #[tokio::test]
    async fn cross_org_target_is_404_without_touching_clerk() {
        let Some(f) = fixture("orgt_om_cross_org", ClerkMock::default()).await else {
            return;
        };
        let other = seed_org(&f.control).await;
        let (foreign, _) = seed_plain_member(&f.control, &other, "member").await;
        assert_error(
            f.remove(f.org.admin, "admin", foreign).await,
            StatusCode::NOT_FOUND,
            "member_not_found",
        )
        .await;
        assert_error(
            f.patch(f.org.admin, "admin", foreign, "admin").await,
            StatusCode::NOT_FOUND,
            "member_not_found",
        )
        .await;
        assert!(f.no_clerk_calls());
        assert_eq!(
            role_of(&f.control, other.account_id, foreign).await.as_deref(),
            Some("member")
        );
        f.finish().await;
    }

    /// Personal / absent tenancy never reaches Clerk.
    #[tokio::test]
    async fn non_org_tenancy_is_403() {
        let Some(f) = fixture("orgt_om_non_org", ClerkMock::default()).await else {
            return;
        };
        let (target, _) = seed_plain_member(&f.control, &f.org, "member").await;
        for decision in [None, Some(TenancyDecision::Personal)] {
            let tenant = || Tenant {
                store: None,
                pool: None,
                decision: decision.clone(),
                class: crate::route_class::RouteClass::Other,
            };
            let resp = remove_org_member(
                State(app_state(f.control.clone())),
                tenant(),
                Extension(user_ctx(&f.org, f.org.admin, "admin")),
                Path(target),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::FORBIDDEN);
            let resp = change_org_member_role(
                State(app_state(f.control.clone())),
                tenant(),
                Extension(user_ctx(&f.org, f.org.admin, "admin")),
                Path(target),
                Json(ChangeRoleRequest { role: "admin".into() }),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        }
        assert!(f.no_clerk_calls());
        f.finish().await;
    }

    /// Clerk's own last-admin refusal maps to our 409.
    #[tokio::test]
    async fn clerk_minimum_permissions_error_maps_to_last_admin() {
        let mock = ClerkMock {
            fail_status: Some(403),
            fail_code: Some("organization_minimum_permissions_needed".into()),
            ..Default::default()
        };
        let Some(f) = fixture("orgt_om_clerk_lastadmin", mock).await else {
            return;
        };
        let (target, _) = seed_plain_member(&f.control, &f.org, "admin").await;
        assert_error(
            f.remove(f.org.admin, "admin", target).await,
            StatusCode::CONFLICT,
            "last_admin",
        )
        .await;
        assert_error(
            f.patch(f.org.admin, "admin", target, "member").await,
            StatusCode::CONFLICT,
            "last_admin",
        )
        .await;
        assert_eq!(
            role_of(&f.control, f.org.account_id, target).await.as_deref(),
            Some("admin")
        );
        f.finish().await;
    }

    /// Two admins demoting each other at once: the advisory lock serializes
    /// them, so exactly one wins and the org keeps an admin.
    #[tokio::test]
    async fn concurrent_mutual_demotion_leaves_one_admin() {
        let mock = ClerkMock {
            patch_delay_ms: 300,
            ..Default::default()
        };
        let Some(f) = fixture("orgt_om_race", mock).await else {
            return;
        };
        // Make the (protected) owner a plain member so only A and B are admins.
        sqlx::query("UPDATE account_members SET role = 'member' WHERE account_id = $1 AND user_id = $2")
            .bind(f.org.account_id)
            .bind(f.org.owner)
            .execute(&f.control)
            .await
            .unwrap();
        let (b, _) = seed_plain_member(&f.control, &f.org, "admin").await;
        let a = f.org.admin;
        let (r1, r2) = tokio::join!(
            f.patch(a, "admin", b, "member"),
            f.patch(b, "admin", a, "member"),
        );
        let mut statuses = [r1.status(), r2.status()];
        statuses.sort();
        assert_eq!(statuses, [StatusCode::OK, StatusCode::CONFLICT]);
        let admins: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM account_members WHERE account_id = $1 AND role IN ('admin','owner')",
        )
        .bind(f.org.account_id)
        .fetch_one(&f.control)
        .await
        .unwrap();
        assert_eq!(admins, 1);
        f.finish().await;
    }

    #[test]
    fn org_role_mapping_is_exact() {
        use crate::clerk_provision::map_org_role;
        assert_eq!(map_org_role(Some("org:admin")), "admin");
        assert_eq!(map_org_role(Some("admin")), "admin");
        assert_eq!(map_org_role(Some("org:member")), "member");
        assert_eq!(map_org_role(Some("org:billing_admin")), "member");
        assert_eq!(map_org_role(None), "member");
        assert!(crate::routes::org_invites::is_admin("admin"));
        assert!(crate::routes::org_invites::is_admin("owner"));
        assert!(!crate::routes::org_invites::is_admin("billing_admin"));
    }

    /// `GET /v1/orgs/members` marks the owner's row so clients can hide the
    /// remove / change-role controls the endpoints above would refuse.
    #[tokio::test]
    async fn roster_marks_the_owner_row() {
        let Some(f) = fixture("orgt_om_roster_owner", ClerkMock::default()).await else {
            return;
        };
        let (plain, _) = seed_plain_member(&f.control, &f.org, "member").await;
        // Teams routes are flag-gated; flip the flags for this test only
        // (serialized by the fixture's env guards).
        // SAFETY: `fixture` holds the process-wide env locks until `finish`.
        unsafe {
            std::env::set_var("ORG_TEAMS_ENABLED", "true");
            std::env::set_var("MEMBER_OCEANS_ENABLED", "true");
        }
        let (status, Json(body)) = crate::routes::teams::list_org_members(
            State(app_state(f.control.clone())),
            org_tenant(&f.control, &f.org, f.org.admin),
            Extension(user_ctx(&f.org, f.org.admin, "admin")),
        )
        .await;
        unsafe {
            std::env::remove_var("ORG_TEAMS_ENABLED");
            std::env::remove_var("MEMBER_OCEANS_ENABLED");
        }
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body.as_array().expect("array");
        let owner_flag = |id: Uuid| {
            rows.iter()
                .find(|r| r["user_id"] == json!(id))
                .unwrap_or_else(|| panic!("row for {id}"))["is_owner"]
                .clone()
        };
        assert_eq!(owner_flag(f.org.owner), json!(true));
        assert_eq!(owner_flag(f.org.admin), json!(false));
        assert_eq!(owner_flag(plain), json!(false));
        // Existing fields are untouched.
        assert_eq!(
            rows.iter().find(|r| r["user_id"] == json!(f.org.owner)).unwrap()["role"],
            json!("admin")
        );
        f.finish().await;
    }

    /// A pool that runs as prod's `coralswarm_app` (NOSUPERUSER, NOBYPASSRLS), so
    /// RLS applies exactly as it does in production. The fixture's own pool is
    /// the test superuser and silently bypasses every policy, which is how the
    /// roster's unscoped reads shipped (#1500). One connection, pre-used by a
    /// scoped transaction so `app.account_id` is left as `''` the way a
    /// recycled prod connection leaves it.
    async fn app_role_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET ROLE coralswarm_app").execute(conn).await?;
                    Ok(())
                })
            })
            .connect(&with_db(base, db))
            .await
            .unwrap();
        let tx = crate::tenancy::scope::begin_account_scoped(&pool, Uuid::new_v4())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        pool
    }

    async fn roster_as_app_role(f: &Fixture, caller: Uuid, role: &str) -> (StatusCode, Value) {
        let app = app_role_pool(&base_url().unwrap(), &f.db).await;
        // SAFETY: `fixture` holds the process-wide env locks until `finish`.
        unsafe {
            std::env::set_var("ORG_TEAMS_ENABLED", "true");
            std::env::set_var("MEMBER_OCEANS_ENABLED", "true");
        }
        let (status, Json(body)) = crate::routes::teams::list_org_members(
            State(app_state(app.clone())),
            org_tenant(&app, &f.org, caller),
            Extension(user_ctx(&f.org, caller, role)),
        )
        .await;
        unsafe {
            std::env::remove_var("ORG_TEAMS_ENABLED");
            std::env::remove_var("MEMBER_OCEANS_ENABLED");
        }
        app.close().await;
        (status, body)
    }

    /// #1500: `GET /v1/orgs/members` as an org admin on the RLS-enforced app
    /// role must serve the roster, not 500 with `invalid input syntax for type
    /// uuid: ""`.
    #[tokio::test]
    async fn roster_serves_an_admin_on_the_rls_enforced_app_role() {
        let Some(f) = fixture("orgt_om_roster_app_admin", ClerkMock::default()).await else {
            return;
        };
        let (plain, _) = seed_plain_member(&f.control, &f.org, "member").await;
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token) \
             VALUES ($1, $2, 'google', 'x', 'x'::bytea)",
        )
        .bind(f.org.account_id)
        .bind(plain)
        .execute(&f.control)
        .await
        .expect("seed oauth token");
        let (status, body) = roster_as_app_role(&f, f.org.admin, "admin").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body.as_array().expect("array");
        assert_eq!(rows.len(), 3);
        let row = |id: Uuid| rows.iter().find(|r| r["user_id"] == json!(id)).unwrap();
        assert_eq!(row(f.org.owner)["is_owner"], json!(true));
        assert_eq!(row(f.org.admin)["is_owner"], json!(false));
        assert_eq!(row(plain)["connected_providers"], json!(["google"]));
        assert_eq!(row(plain)["connect_nudged_at"], Value::Null);
        f.finish().await;
    }

    /// Same read for a plain member: the legacy four-key shape, unchanged.
    #[tokio::test]
    async fn roster_serves_a_member_on_the_rls_enforced_app_role() {
        let Some(f) = fixture("orgt_om_roster_app_member", ClerkMock::default()).await else {
            return;
        };
        let (plain, _) = seed_plain_member(&f.control, &f.org, "member").await;
        let (status, body) = roster_as_app_role(&f, plain, "member").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body.as_array().expect("array");
        assert_eq!(rows.len(), 3);
        for r in rows {
            let mut keys: Vec<_> = r.as_object().unwrap().keys().cloned().collect();
            keys.sort();
            assert_eq!(keys, ["email", "name", "role", "user_id"]);
        }
        f.finish().await;
    }
}
