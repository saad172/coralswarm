//! `GET /v1/query-audit` — the query-audit disclosure log's admin read (m365
//! enterprise train PR 4.3, plan decision #11 part 2).
//!
//! Org-admin-only: gated by [`crate::services::deletion::is_admin_role`]
//! against the caller's OWN `account_members.role`
//! ([`crate::middleware::auth::UserContext::role`], already resolved by the
//! auth middleware — the same field [`crate::routes::escrow::is_admin`] and
//! [`crate::services::deletion::is_admin_role`]'s own doc comment cite as
//! the existing admin-gating convention). This is deliberately the SAME
//! check `routes::escrow`/`services::deletion` already use, not
//! `routes::teams::is_org_admin`'s heavier `OrgAdminProof`/governance-GUC
//! machinery — that machinery exists to let a non-member org admin read
//! MEMBER-OCEAN-anchored tables (`teams`/`team_members`/`membership_events`)
//! through `tenant_rls_visible_graph_ids()`'s member-only predicate. This
//! table has no ocean anchor at all (`graphs_searched` is a bounded array,
//! not a single `graph_id`), so its tenant-plane SELECT policy is
//! `tenant_rls_is_system()`-only (see `migrations_tenant/
//! 0069_query_audit_log.sql`'s header) — there is no member-ocean bypass to
//! grant here, only a route-layer role check followed by a system-scoped
//! read. See `services::query_audit`'s module doc for the full "SELECT/RLS
//! split, stated honestly" reasoning this route is the other half of.
//!
//! Disclosure copy (plan decision #11: "disclosed to users") rides every
//! response body's `disclosure` field — see
//! [`crate::services::query_audit::DISCLOSURE_TEXT`]'s own doc comment for
//! why that, plus `docs/query-audit-disclosure.md`, is what ships in this PR,
//! and why a first-class in-product surface for ORDINARY members (not just
//! the admins who can reach this route) is flagged as follow-up rather than
//! built here.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Extension, Json,
};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::middleware::auth::UserContext;
use crate::services::deletion::is_admin_role;
use crate::services::query_audit::{self, DISCLOSURE_TEXT};
use crate::tenancy::extractor::Tenant;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct QueryAuditQueryParams {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
}

fn default_page() -> u32 {
    1
}

fn default_page_size() -> u32 {
    20
}

/// Resolve `GET /v1/query-audit`'s pagination into the `(LIMIT, OFFSET)`
/// pair the store read takes. Same clamp shape as
/// `routes::teams::events_page_bounds` (page size 1..=50, page floor 1) — a
/// free function, not inlined, for the identical reason that doc comment
/// gives: it is testable without a database, and it is what makes an
/// unbounded `?page_size=` query param safe to expose at all.
fn query_audit_page_bounds(params: &QueryAuditQueryParams) -> (i64, i64) {
    let page_size = params.page_size.clamp(1, 50) as i64;
    let page = params.page.max(1) as i64;
    (page_size, (page - 1) * page_size)
}

/// Paginated `query_audit_log` history for the caller's account, newest
/// first. Org-admin (or account owner, on a personal account) only.
#[utoipa::path(
    get,
    path = "/v1/query-audit",
    tag = "query_audit",
    params(
        ("page" = Option<u32>, Query, description = "Page number (default 1)"),
        ("page_size" = Option<u32>, Query, description = "Page size (default 20, max 50)"),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Query-audit rows, newest first, plus disclosure copy", body = serde_json::Value),
        (status = 403, description = "Caller is not an org admin / account owner"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn list_query_audit(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<QueryAuditQueryParams>,
) -> impl IntoResponse {
    if !is_admin_role(&user.role) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "must be an org admin (or account owner) to view the query-audit log"})),
        )
            .into_response();
    }

    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };

    let (page_size, offset) = query_audit_page_bounds(&params);

    // System-scoped on an org tenant pool (this table's tenant-plane SELECT
    // policy admits `tenant_rls_is_system()` only — see the migration
    // header); account-scoped on the consumer/control pool otherwise. Either
    // way, the `is_admin_role` gate above has ALREADY run — this is the
    // read, not the authorization.
    let is_org = tenant.org_context().is_some();
    let opened = if is_org {
        crate::tenancy::scope::begin_tenant_system_scoped(&pool, user.account_id).await
    } else {
        crate::tenancy::scope::begin_account_scoped(&pool, user.account_id).await
    };

    let mut tx = match opened {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "list_query_audit: failed to open scoped transaction");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    };

    let rows = match query_audit::list_page(&mut tx, user.account_id, page_size, offset).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "list_query_audit: read failed");
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    // Read-only — rolled back, not committed (same convention as
    // `routes::queries`'s `scoped_get_conversation`/`scoped_intake_summary`).
    tx.rollback().await.ok();

    (
        StatusCode::OK,
        Json(json!({
            "rows": rows,
            "page": params.page.max(1),
            "page_size": page_size,
            "disclosure": DISCLOSURE_TEXT,
        })),
    )
        .into_response()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_audit_page_bounds_defaults_to_page_1_size_20() {
        let params = QueryAuditQueryParams { page: 1, page_size: 20 };
        assert_eq!(query_audit_page_bounds(&params), (20, 0));
    }

    #[test]
    fn query_audit_page_bounds_clamps_page_size_to_fifty() {
        let params = QueryAuditQueryParams {
            page: 1,
            page_size: 100_000,
        };
        let (page_size, _) = query_audit_page_bounds(&params);
        assert_eq!(page_size, 50, "an unbounded LIMIT must never reach the store");
    }

    #[test]
    fn query_audit_page_bounds_clamps_zero_page_size_up_to_one() {
        let params = QueryAuditQueryParams { page: 1, page_size: 0 };
        let (page_size, _) = query_audit_page_bounds(&params);
        assert_eq!(page_size, 1);
    }

    #[test]
    fn query_audit_page_bounds_clamps_zero_page_up_to_one() {
        let params = QueryAuditQueryParams { page: 0, page_size: 20 };
        let (_, offset) = query_audit_page_bounds(&params);
        assert_eq!(offset, 0, "page=0 must not produce a negative offset");
    }

    #[test]
    fn query_audit_page_bounds_computes_offset_for_later_pages() {
        let params = QueryAuditQueryParams { page: 3, page_size: 20 };
        assert_eq!(query_audit_page_bounds(&params), (20, 40));
    }

    #[tokio::test]
    async fn list_query_audit_rejects_a_non_admin_role_before_touching_the_database() {
        let state = crate::test_app_state();
        let user = UserContext {
            user_id: uuid::Uuid::new_v4(),
            account_id: uuid::Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "dev-1".to_string(),
        };
        let resp = list_query_audit(
            State(state.clone()),
            Tenant::legacy(&state),
            Extension(user),
            Query(QueryAuditQueryParams { page: 1, page_size: 20 }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn list_query_audit_admin_role_with_no_db_returns_503() {
        let state = crate::test_app_state();
        let user = UserContext {
            user_id: uuid::Uuid::new_v4(),
            account_id: uuid::Uuid::new_v4(),
            role: "admin".to_string(),
            device_id: "dev-1".to_string(),
        };
        let resp = list_query_audit(
            State(state.clone()),
            Tenant::legacy(&state),
            Extension(user),
            Query(QueryAuditQueryParams { page: 1, page_size: 20 }),
        )
        .await
        .into_response();
        // `test_app_state()` carries no Postgres pool — the admin gate must
        // pass (role="admin") and THEN the missing-pool branch must answer,
        // proving the role check runs before the pool check, not after.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn owner_role_is_treated_as_admin() {
        assert!(is_admin_role("owner"));
        assert!(is_admin_role("admin"));
        assert!(!is_admin_role("member"));
    }
}

// ── Live-Postgres tests: the admin route's real read + the write path it
// depends on (m365 enterprise train PR 4.3) ─────────────────────────────────
//
// In-crate, not `backend/tests/*_pg.rs`: `UserContext` (`middleware::auth`)
// is not `pub` outside the crate — `mod middleware;` in `lib.rs` is
// crate-private — so an EXTERNAL test file has no way to hand-construct one
// and call `list_query_audit` directly (the only alternative would be a
// full HTTP request through real Clerk JWT verification, which
// `team_by_id_graph_compat_pg.rs` shows is a much heavier rig than this
// route's own logic warrants). Mirrors `routes::ingest::
// anchored_correction_pg_tests`'s established pattern for exercising a
// crate-private-dependent handler against a real Postgres. Gated on
// `ORGT_TEST_BASE_URL`, same scratch-DB convention as every
// `backend/tests/*_pg.rs` file.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:55987/postgres \
//     cargo test -p coralswarm-backend --lib routes::query_audit::query_audit_pg_tests -- --nocapture
#[cfg(test)]
mod query_audit_pg_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn admin_conn(base: &str) -> PgConnection {
        PgConnection::connect(base).await.expect("connect admin")
    }
    async fn recreate(conn: &mut PgConnection, db: &str) {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
    }
    async fn control_pool(base: &str, db: &str) -> sqlx::PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&with_db(base, db))
            .await
            .expect("connect control");
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }

    /// Seed one (user, account) pair, owner-connected (bypasses RLS — this
    /// suite proves the ROUTE's own logic and `services::query_audit`'s SQL,
    /// not the RLS backstop itself, which `tests/consumer_rls_pg.rs`'s
    /// `query_audit_log_rls_isolates_and_is_append_only` and
    /// `tests/tenant_rls_pg.rs`'s roster fixture already cover).
    async fn seed_account(pool: &sqlx::PgPool, email: &str) -> (uuid::Uuid, uuid::Uuid) {
        let user_id: uuid::Uuid =
            sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
                .bind(email)
                .fetch_one(pool)
                .await
                .unwrap();
        let account_id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, plan_tier) VALUES ($1, 'free') RETURNING id",
        )
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap();
        (user_id, account_id)
    }

    fn user_ctx(account_id: uuid::Uuid, user_id: uuid::Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: role.to_string(),
            device_id: "pg-test".to_string(),
        }
    }

    fn legacy_tenant(pool: sqlx::PgPool) -> Tenant {
        // `decision: None` is the Personal/legacy shape (see `Tenant::legacy`'s
        // doc comment) — exercises `list_query_audit`'s `begin_account_scoped`
        // arm, not the org `begin_tenant_system_scoped` one (that arm's real
        // DB behavior — the `tenant_rls_is_system()`-only SELECT policy — is
        // proven directly by `tests/tenant_rls_pg.rs`'s query_audit_log block).
        Tenant {
            store: None,
            pool: Some(pool),
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    #[tokio::test]
    async fn record_writes_the_expected_row_and_never_the_raw_question_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping record_writes_the_expected_row_and_never_the_raw_question_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "query_audit_record_writes_row";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let (user_id, account_id) = seed_account(&pool, "qad-record@test.local").await;
        let g1 = uuid::Uuid::new_v4();
        let g2 = uuid::Uuid::new_v4();
        let atom1 = uuid::Uuid::new_v4();
        let question = "what did the founder decide about pricing?";

        query_audit::record(&pool, account_id, user_id, question, &[g1, g2], &[atom1]).await;

        let row: (uuid::Uuid, uuid::Uuid, String, Vec<uuid::Uuid>, Vec<uuid::Uuid>) = sqlx::query_as(
            "SELECT account_id, asked_by_user_id, question_digest, graphs_searched, atom_ids_cited \
             FROM query_audit_log WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("record() must have written exactly one row");

        assert_eq!(row.0, account_id);
        assert_eq!(row.1, user_id);
        assert_eq!(row.2, query_audit::question_digest(question), "the stored digest must match the real question's digest");
        assert_ne!(row.2, question, "the raw question text must never be stored");
        assert!(!row.2.contains("pricing"), "no fragment of the raw question may leak into the stored digest");
        let mut graphs = row.3.clone();
        graphs.sort();
        let mut expected_graphs = vec![g1, g2];
        expected_graphs.sort();
        assert_eq!(graphs, expected_graphs);
        assert_eq!(row.4, vec![atom1]);

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn list_query_audit_admin_sees_only_their_own_account_rows_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping list_query_audit_admin_sees_only_their_own_account_rows_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "query_audit_admin_no_cross_account_leak";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let (admin_user, account_a) = seed_account(&pool, "qad-admin-a@test.local").await;
        let (other_user, account_b) = seed_account(&pool, "qad-other-b@test.local").await;

        // Two questions for A, one for B — enough to prove both "admin sees
        // their own account's rows" and "never account B's".
        query_audit::record(&pool, account_a, admin_user, "question a1", &[], &[]).await;
        query_audit::record(&pool, account_a, admin_user, "question a2", &[], &[]).await;
        query_audit::record(&pool, account_b, other_user, "question b1", &[], &[]).await;

        let state = crate::test_app_state();

        // ── Admin, scoped to account A: sees exactly A's 2 rows ────────────
        let admin_resp = list_query_audit(
            State(state.clone()),
            legacy_tenant(pool.clone()),
            Extension(user_ctx(account_a, admin_user, "admin")),
            Query(QueryAuditQueryParams { page: 1, page_size: 20 }),
        )
        .await
        .into_response();
        assert_eq!(admin_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(admin_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let rows = json["rows"].as_array().expect("rows must be an array");
        assert_eq!(rows.len(), 2, "account A's admin must see exactly A's 2 query-audit rows, got {rows:?}");
        assert!(
            json["disclosure"].as_str().unwrap_or_default().contains("logs who asks"),
            "the response must carry the disclosure copy — got {json:?}"
        );

        // ── Non-admin (member) role on account A: 403, never reaches the DB ─
        let member_resp = list_query_audit(
            State(state.clone()),
            legacy_tenant(pool.clone()),
            Extension(user_ctx(account_a, admin_user, "member")),
            Query(QueryAuditQueryParams { page: 1, page_size: 20 }),
        )
        .await
        .into_response();
        assert_eq!(member_resp.status(), StatusCode::FORBIDDEN);

        // ── Admin scoped to account B: sees exactly B's 1 row, never A's ───
        let b_admin_resp = list_query_audit(
            State(state),
            legacy_tenant(pool.clone()),
            Extension(user_ctx(account_b, other_user, "owner")),
            Query(QueryAuditQueryParams { page: 1, page_size: 20 }),
        )
        .await
        .into_response();
        assert_eq!(b_admin_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(b_admin_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let rows = json["rows"].as_array().expect("rows must be an array");
        assert_eq!(rows.len(), 1, "account B's admin must see exactly B's 1 query-audit row, never A's, got {rows:?}");

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
