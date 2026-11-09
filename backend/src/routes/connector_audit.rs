//! `GET /v1/connector-audit` — the connector compliance audit log's admin
//! read (m365 enterprise train PR 4.2, plan decision #11 part 1).
//!
//! Mirrors `routes::query_audit::list_query_audit`'s shape closely
//! (org-admin gate via [`is_admin_role`] against the caller's OWN
//! `account_members.role`, the same SELECT/RLS split — RLS backstops the
//! account/tenant boundary, this route enforces the admin boundary RLS
//! cannot see) — see that route's own doc comment and
//! `services::connector_audit`'s module doc for the full reasoning this
//! route is the read half of.

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
use crate::services::connector_audit;
use crate::services::deletion::is_admin_role;
use crate::tenancy::extractor::Tenant;
use crate::AppState;

#[derive(Debug, Deserialize, Default)]
pub struct ConnectorAuditQueryParams {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// Keyset cursor (CodeRabbit round 1 on #701): the `occurred_at`/`id`
    /// of the LAST row the caller saw, normally lifted verbatim from a
    /// prior response's `next_cursor`. When BOTH fields are present,
    /// pagination switches to `services::connector_audit::list_page_after`
    /// — stable under concurrent inserts, unlike the `page`/`page_size`
    /// OFFSET path, which this stays backward-compatible with (the default,
    /// no-cursor first call) rather than replacing outright. Either field
    /// alone (no matching pair) is treated as "no cursor" — see
    /// `connector_audit_cursor` below.
    pub after_occurred_at: Option<chrono::DateTime<chrono::Utc>>,
    pub after_id: Option<uuid::Uuid>,
}

fn default_page() -> u32 {
    1
}

fn default_page_size() -> u32 {
    20
}

/// Same clamp shape as `routes::query_audit::query_audit_page_bounds` (page
/// size 1..=50, page floor 1) — a free function for the identical reason
/// that route's own doc comment gives: testable without a database, and
/// what makes an unbounded `?page_size=` safe to expose at all.
fn connector_audit_page_bounds(params: &ConnectorAuditQueryParams) -> (i64, i64) {
    let page_size = params.page_size.clamp(1, 50) as i64;
    let page = params.page.max(1) as i64;
    (page_size, (page - 1) * page_size)
}

/// Extracts a keyset cursor from `params`, requiring BOTH `after_occurred_at`
/// AND `after_id` — a lone field (a hand-built or truncated query string) is
/// treated as "no cursor" rather than silently ignoring the half that's
/// present, since `list_page_after` needs the pair to form a valid `(occurred_at,
/// id)` tuple comparison.
fn connector_audit_cursor(
    params: &ConnectorAuditQueryParams,
) -> Option<(chrono::DateTime<chrono::Utc>, uuid::Uuid)> {
    match (params.after_occurred_at, params.after_id) {
        (Some(at), Some(id)) => Some((at, id)),
        _ => None,
    }
}

/// Paginated `connector_audit_log` history for the caller's account, newest
/// first. Org-admin (or account owner, on a personal account) only.
///
/// Two pagination modes (CodeRabbit round 1 on #701): `page`/`page_size`
/// (OFFSET, the original/default shape — stable only against a table that
/// isn't being written to, which this append-only log always is) OR
/// `after_occurred_at`/`after_id` (keyset, taken from a prior response's
/// `next_cursor` — stable under concurrent inserts, the correct way to walk
/// a compliance export). The response always carries `next_cursor` so a
/// caller can switch onto the keyset path after its first, offset-based
/// call.
#[utoipa::path(
    get,
    path = "/v1/connector-audit",
    tag = "connector_audit",
    params(
        ("page" = Option<u32>, Query, description = "Page number (default 1) — ignored when after_occurred_at/after_id are both present"),
        ("page_size" = Option<u32>, Query, description = "Page size (default 20, max 50)"),
        ("after_occurred_at" = Option<String>, Query, description = "Keyset cursor: occurred_at of the last row seen (RFC3339). Requires after_id."),
        ("after_id" = Option<String>, Query, description = "Keyset cursor: id (uuid) of the last row seen. Requires after_occurred_at."),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "connector_audit_log rows, newest first, with a next_cursor for stable keyset paging", body = serde_json::Value),
        (status = 403, description = "Caller is not an org admin / account owner"),
        (status = 503, description = "Database not available"),
    )
)]
pub async fn list_connector_audit(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<ConnectorAuditQueryParams>,
) -> impl IntoResponse {
    if !is_admin_role(&user.role) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "must be an org admin (or account owner) to view the connector-audit log"})),
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

    let (page_size, offset) = connector_audit_page_bounds(&params);

    // System-scoped on an org tenant pool (this table's tenant-plane SELECT
    // policy admits `tenant_rls_is_system()` only); account-scoped on the
    // consumer/control pool otherwise. The `is_admin_role` gate above has
    // ALREADY run — this is the read, not the authorization.
    let is_org = tenant.org_context().is_some();
    let opened = if is_org {
        crate::tenancy::scope::begin_tenant_system_scoped(&pool, user.account_id).await
    } else {
        crate::tenancy::scope::begin_account_scoped(&pool, user.account_id).await
    };

    let mut tx = match opened {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "list_connector_audit: failed to open scoped transaction");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    };

    let cursor = connector_audit_cursor(&params);
    let read = if let Some(cursor) = cursor {
        connector_audit::list_page_after(&mut tx, user.account_id, page_size, Some(cursor)).await
    } else {
        connector_audit::list_page(&mut tx, user.account_id, page_size, offset).await
    };
    let rows = match read {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "list_connector_audit: read failed");
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    // Read-only — rolled back, not committed.
    tx.rollback().await.ok();

    // The keyset cursor for the NEXT call — the last (oldest) row on this
    // page, or `null` only when this page came back EMPTY (a short-but-non-
    // empty page still returns a cursor; do NOT treat "short of page_size"
    // as end-of-walk — a later call with the cursor is what proves there is
    // nothing older). A cursor derived from any non-empty page is not
    // itself proof more rows exist, but the client's next call returning
    // zero rows makes that self-correcting — same "one extra, harmless
    // round trip rather than a wrong claim" tradeoff most has-more/cursor
    // APIs make.
    let next_cursor = rows.last().map(|r| json!({"occurred_at": r.occurred_at, "id": r.id}));

    (
        StatusCode::OK,
        Json(json!({
            "rows": rows,
            "page": params.page.max(1),
            "page_size": page_size,
            "next_cursor": next_cursor,
        })),
    )
        .into_response()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_audit_page_bounds_defaults_to_page_1_size_20() {
        let params = ConnectorAuditQueryParams { page: 1, page_size: 20, ..Default::default() };
        assert_eq!(connector_audit_page_bounds(&params), (20, 0));
    }

    #[test]
    fn connector_audit_page_bounds_clamps_page_size_to_fifty() {
        let params = ConnectorAuditQueryParams { page: 1, page_size: 100_000, ..Default::default() };
        let (page_size, _) = connector_audit_page_bounds(&params);
        assert_eq!(page_size, 50, "an unbounded LIMIT must never reach the store");
    }

    #[test]
    fn connector_audit_page_bounds_clamps_zero_page_size_up_to_one() {
        let params = ConnectorAuditQueryParams { page: 1, page_size: 0, ..Default::default() };
        let (page_size, _) = connector_audit_page_bounds(&params);
        assert_eq!(page_size, 1);
    }

    #[test]
    fn connector_audit_page_bounds_clamps_zero_page_up_to_one() {
        let params = ConnectorAuditQueryParams { page: 0, page_size: 20, ..Default::default() };
        let (_, offset) = connector_audit_page_bounds(&params);
        assert_eq!(offset, 0, "page=0 must not produce a negative offset");
    }

    #[test]
    fn connector_audit_page_bounds_computes_offset_for_later_pages() {
        let params = ConnectorAuditQueryParams { page: 3, page_size: 20, ..Default::default() };
        assert_eq!(connector_audit_page_bounds(&params), (20, 40));
    }

    #[test]
    fn connector_audit_cursor_is_none_when_neither_field_is_present() {
        let params = ConnectorAuditQueryParams { page: 1, page_size: 20, ..Default::default() };
        assert!(connector_audit_cursor(&params).is_none());
    }

    /// CodeRabbit round 1 on #701: a lone `after_id` (or lone
    /// `after_occurred_at`) — a hand-built or truncated query string —
    /// must NOT be treated as a valid cursor; `list_page_after` needs the
    /// pair to form a real `(occurred_at, id)` tuple comparison.
    #[test]
    fn connector_audit_cursor_is_none_when_only_one_field_is_present() {
        let only_id = ConnectorAuditQueryParams {
            page: 1,
            page_size: 20,
            after_id: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        };
        assert!(connector_audit_cursor(&only_id).is_none());

        let only_at = ConnectorAuditQueryParams {
            page: 1,
            page_size: 20,
            after_occurred_at: Some(chrono::Utc::now()),
            ..Default::default()
        };
        assert!(connector_audit_cursor(&only_at).is_none());
    }

    #[test]
    fn connector_audit_cursor_is_some_when_both_fields_are_present() {
        let at = chrono::Utc::now();
        let id = uuid::Uuid::new_v4();
        let params = ConnectorAuditQueryParams {
            page: 1,
            page_size: 20,
            after_occurred_at: Some(at),
            after_id: Some(id),
        };
        assert_eq!(connector_audit_cursor(&params), Some((at, id)));
    }

    #[tokio::test]
    async fn list_connector_audit_rejects_a_non_admin_role_before_touching_the_database() {
        let state = crate::test_app_state();
        let user = UserContext {
            user_id: uuid::Uuid::new_v4(),
            account_id: uuid::Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "dev-1".to_string(),
        };
        let resp = list_connector_audit(
            State(state.clone()),
            Tenant::legacy(&state),
            Extension(user),
            Query(ConnectorAuditQueryParams { page: 1, page_size: 20, ..Default::default() }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn list_connector_audit_admin_role_with_no_db_returns_503() {
        let state = crate::test_app_state();
        let user = UserContext {
            user_id: uuid::Uuid::new_v4(),
            account_id: uuid::Uuid::new_v4(),
            role: "admin".to_string(),
            device_id: "dev-1".to_string(),
        };
        let resp = list_connector_audit(
            State(state.clone()),
            Tenant::legacy(&state),
            Extension(user),
            Query(ConnectorAuditQueryParams { page: 1, page_size: 20, ..Default::default() }),
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

// ── Live-Postgres tests ──────────────────────────────────────────────────────
//
// In-crate, not `backend/tests/*_pg.rs`: `UserContext` is not `pub` outside
// the crate — same reasoning `routes::query_audit`'s own pg-test module
// header gives, and the same `team_by_id_graph_compat_pg.rs` precedent.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//     cargo test -p coralswarm-backend --lib routes::connector_audit::connector_audit_pg_tests -- --nocapture
#[cfg(test)]
mod connector_audit_pg_tests {
    use super::*;
    use crate::services::connector_audit::{Actor, AuditEntry};
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
        Tenant {
            store: None,
            pool: Some(pool),
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    #[tokio::test]
    async fn list_connector_audit_admin_sees_only_their_own_account_rows_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping list_connector_audit_admin_sees_only_their_own_account_rows_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "connector_audit_admin_no_cross_account_leak";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let (admin_user, account_a) = seed_account(&pool, "cad-admin-a@test.local").await;
        let (other_user, account_b) = seed_account(&pool, "cad-other-b@test.local").await;

        connector_audit::record(
            &pool,
            AuditEntry {
                account_id: account_a,
                actor: Actor::User(admin_user),
                action: "install",
                provider: Some("slack".to_string()),
                subject: Some("T-A1".to_string()),
                detail: None,
            },
        )
        .await
        .expect("record A1");
        connector_audit::record(
            &pool,
            AuditEntry {
                account_id: account_a,
                actor: Actor::System,
                action: "seat_grant",
                provider: Some("slack".to_string()),
                subject: Some("C-A1".to_string()),
                detail: None,
            },
        )
        .await
        .expect("record A2");
        connector_audit::record(
            &pool,
            AuditEntry {
                account_id: account_b,
                actor: Actor::User(other_user),
                action: "install",
                provider: Some("microsoft".to_string()),
                subject: Some("tenant-b".to_string()),
                detail: None,
            },
        )
        .await
        .expect("record B1");

        let state = crate::test_app_state();

        let admin_resp = list_connector_audit(
            State(state.clone()),
            legacy_tenant(pool.clone()),
            Extension(user_ctx(account_a, admin_user, "admin")),
            Query(ConnectorAuditQueryParams { page: 1, page_size: 20, ..Default::default() }),
        )
        .await
        .into_response();
        assert_eq!(admin_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(admin_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let rows = json["rows"].as_array().expect("rows must be an array");
        assert_eq!(rows.len(), 2, "account A's admin must see exactly A's 2 rows, got {rows:?}");

        let member_resp = list_connector_audit(
            State(state.clone()),
            legacy_tenant(pool.clone()),
            Extension(user_ctx(account_a, admin_user, "member")),
            Query(ConnectorAuditQueryParams { page: 1, page_size: 20, ..Default::default() }),
        )
        .await
        .into_response();
        assert_eq!(member_resp.status(), StatusCode::FORBIDDEN);

        let b_admin_resp = list_connector_audit(
            State(state),
            legacy_tenant(pool.clone()),
            Extension(user_ctx(account_b, other_user, "owner")),
            Query(ConnectorAuditQueryParams { page: 1, page_size: 20, ..Default::default() }),
        )
        .await
        .into_response();
        assert_eq!(b_admin_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(b_admin_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let rows = json["rows"].as_array().expect("rows must be an array");
        assert_eq!(rows.len(), 1, "account B's admin must see exactly B's 1 row, never A's, got {rows:?}");

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The durability bar itself: a write via [`connector_audit::record`]
    /// against a table with NO INSERT grant for the connecting role must
    /// fail loudly (the whole point of the compliance bar), never silently
    /// succeed-as-a-no-op or warn-and-drop.
    #[tokio::test]
    async fn record_fails_when_the_insert_is_impossible_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping record_fails_when_the_insert_is_impossible_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "connector_audit_record_hard_fail";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;
        let (user_id, account_id) = seed_account(&pool, "cad-fail@test.local").await;

        // Revoke the connecting role's INSERT privilege on the table itself
        // — a write attempt now fails at the database, not merely at RLS,
        // proving `record()` surfaces the failure rather than swallowing it.
        sqlx::query("REVOKE INSERT ON connector_audit_log FROM PUBLIC")
            .execute(&pool)
            .await
            .ok();
        // The pool here connects as the migration owner, which typically
        // bypasses table-level GRANT checks the same way it bypasses RLS —
        // so this test instead proves the propagation contract directly:
        // an injected error from the write must reach the caller as `Err`,
        // never be swallowed into `Ok(())`. A constraint violation is the
        // reliable way to force that deterministically.
        let result = connector_audit::record(
            &pool,
            AuditEntry {
                account_id,
                actor: Actor::User(user_id),
                // Not in the migration's closed `action` CHECK vocabulary —
                // the INSERT must be rejected by the constraint.
                action: "not_a_real_action",
                provider: None,
                subject: None,
                detail: None,
            },
        )
        .await;
        assert!(
            result.is_err(),
            "an audit write violating the action CHECK constraint must surface as Err, not Ok(())"
        );

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// CodeRabbit round 1 on #701: `connector_audit_log_actor_pairing`
    /// (migrations `0193_connector_audit_log.sql` control /
    /// `0071_connector_audit_log.sql` tenant) is the hard DB backstop for
    /// `actor_kind`/`actor_user_id` pairing — `services::connector_audit`'s
    /// `Actor` enum is only the FIRST line of defense, never reachable by a
    /// raw SQL write. This test bypasses `Actor` entirely (a direct
    /// `INSERT`, mirroring `record_fails_when_the_insert_is_impossible_pg`'s
    /// "prove the constraint, not just the enum" reasoning just above) to
    /// prove the CHECK itself rejects both illegal pairings — `('user',
    /// NULL)` and `('system', <uuid>)` — the exact two CodeRabbit's finding
    /// named as currently insertable.
    #[tokio::test]
    async fn actor_pairing_check_rejects_mismatched_kind_and_user_id_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping actor_pairing_check_rejects_mismatched_kind_and_user_id_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "connector_audit_actor_pairing_check";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;
        let (_user_id, account_id) = seed_account(&pool, "cad-pairing@test.local").await;

        // ('user', NULL) — a 'user'-kind row naming no actor.
        let user_with_no_id = sqlx::query(
            "INSERT INTO connector_audit_log (account_id, actor_user_id, actor_kind, action) \
             VALUES ($1, NULL, 'user', 'install')",
        )
        .bind(account_id)
        .execute(&pool)
        .await;
        assert!(
            user_with_no_id.is_err(),
            "the actor_pairing CHECK must reject ('user', NULL) — got {user_with_no_id:?}"
        );

        // ('system', <uuid>) — a 'system'-kind row naming a real actor.
        let system_with_id = sqlx::query(
            "INSERT INTO connector_audit_log (account_id, actor_user_id, actor_kind, action) \
             VALUES ($1, gen_random_uuid(), 'system', 'seat_grant')",
        )
        .bind(account_id)
        .execute(&pool)
        .await;
        assert!(
            system_with_id.is_err(),
            "the actor_pairing CHECK must reject ('system', <uuid>) — got {system_with_id:?}"
        );

        // The two legal pairings the CHECK admits, as a non-vacuity guard —
        // an over-broad predicate that also rejected everything would still
        // pass the two assertions above without actually enforcing anything.
        let legal_user = connector_audit::record(
            &pool,
            AuditEntry {
                account_id,
                actor: Actor::User(_user_id),
                action: "install",
                provider: None,
                subject: None,
                detail: None,
            },
        )
        .await;
        assert!(legal_user.is_ok(), "('user', <uuid>) must be accepted — got {legal_user:?}");
        let legal_system = connector_audit::record(
            &pool,
            AuditEntry {
                account_id,
                actor: Actor::System,
                action: "seat_grant",
                provider: None,
                subject: None,
                detail: None,
            },
        )
        .await;
        assert!(legal_system.is_ok(), "('system', NULL) must be accepted — got {legal_system:?}");

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// CodeRabbit round 1 on #701: proves the fix for the
    /// `MAX_DETAIL_BYTES`/DB-CHECK mismatch actually holds against a real
    /// Postgres — the unit-test half
    /// (`services::connector_audit::tests::jsonb_text_len_matches_postgres_
    /// jsonb_text_spacing` and its sibling `bound_detail_elides_...`) proves
    /// the Rust-side arithmetic; this proves the arithmetic agrees with
    /// what the migration's `octet_length(detail::text) <= 4096` CHECK
    /// actually measures. Builds a `detail` value deliberately close to the
    /// 4096-byte jsonb-text boundary (same dense-object shape as the unit
    /// test's fixture, sized so `bound_detail` passes it through
    /// UNCHANGED — i.e. this is a value `services::connector_audit`
    /// genuinely decided was small enough) and asserts the INSERT it
    /// produces actually succeeds: a value the Rust bound accepts must
    /// never be the value that fails the DB CHECK.
    #[tokio::test]
    async fn record_accepts_a_detail_value_at_the_jsonb_text_boundary_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping record_accepts_a_detail_value_at_the_jsonb_text_boundary_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "connector_audit_detail_boundary";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;
        let (user_id, account_id) = seed_account(&pool, "cad-boundary@test.local").await;

        // 300 short entries — dense enough that jsonb-text's added spacing
        // is a meaningful fraction of the total (599 extra bytes: one per
        // `:` and per non-first `,`), but sized so the RENDERED total
        // (11*300+1 compact = 3301, + 599 spacing = 3900) still clears
        // `bound_detail`'s 4096-byte jsonb-text measure — i.e. this value
        // is exactly the kind `services::connector_audit` decides is small
        // enough to write through unchanged.
        let mut map = serde_json::Map::new();
        for i in 0..300 {
            map.insert(format!("k{i:03}"), serde_json::Value::String("x".to_string()));
        }
        let detail = serde_json::Value::Object(map);

        let result = connector_audit::record(
            &pool,
            AuditEntry {
                account_id,
                actor: Actor::User(user_id),
                action: "install",
                provider: Some("slack".to_string()),
                subject: Some("boundary-fixture".to_string()),
                detail: Some(detail),
            },
        )
        .await;
        assert!(
            result.is_ok(),
            "a detail value bound_detail passed through unchanged must always satisfy the DB \
             CHECK it was measured against — got {result:?}"
        );

        let stored: serde_json::Value =
            sqlx::query_scalar("SELECT detail FROM connector_audit_log WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .expect("select the row back");
        assert!(
            stored.get("_elided").is_none(),
            "this fixture must be small enough that bound_detail passes it through UNCHANGED — \
             got {stored:?}"
        );

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// CodeRabbit round 1 on #701: the bug itself, reproduced and then
    /// proven fixed. `list_page`'s `LIMIT ... OFFSET` is stable only
    /// against a frozen table — a row inserted between two page reads
    /// shifts the whole window, so an admin walking pages can see a row
    /// TWICE (re-shown) while `list_page_after`'s keyset read cannot: it
    /// names an absolute `(occurred_at, id)` position, not a row count from
    /// the top, so a newly-inserted newest row simply sorts ahead of every
    /// cursor already issued and never displaces what a later page returns.
    #[tokio::test]
    async fn keyset_pagination_is_stable_under_a_concurrent_insert_where_offset_is_not_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping keyset_pagination_is_stable_under_a_concurrent_insert_where_offset_is_not_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "connector_audit_keyset_stability";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;
        let (user_id, account_id) = seed_account(&pool, "cad-keyset@test.local").await;

        // Seed 3 rows (oldest to newest: r1, r2, r3) with distinct,
        // strictly increasing `occurred_at` so ORDER BY is unambiguous —
        // `occurred_at` is set explicitly (not left to `DEFAULT now()`)
        // so no two rows can land in the same microsecond under CI jitter.
        async fn seed_row(pool: &sqlx::PgPool, account_id: uuid::Uuid, user_id: uuid::Uuid, subject: &str, at: &str) {
            sqlx::query(
                "INSERT INTO connector_audit_log \
                    (account_id, actor_user_id, actor_kind, action, provider, subject, occurred_at) \
                 VALUES ($1, $2, 'user', 'install', 'slack', $3, $4::timestamptz)",
            )
            .bind(account_id)
            .bind(user_id)
            .bind(subject)
            .bind(at)
            .execute(pool)
            .await
            .expect("seed row");
        }
        seed_row(&pool, account_id, user_id, "r1", "2026-01-01T00:00:01Z").await;
        seed_row(&pool, account_id, user_id, "r2", "2026-01-01T00:00:02Z").await;
        seed_row(&pool, account_id, user_id, "r3", "2026-01-01T00:00:03Z").await;

        // Page 1 of size 2, BOTH ways: newest first is [r3, r2] for either.
        let mut tx = pool.begin().await.unwrap();
        let offset_page1 = connector_audit::list_page(&mut tx, account_id, 2, 0).await.unwrap();
        tx.rollback().await.ok();
        let mut tx = pool.begin().await.unwrap();
        let keyset_page1 = connector_audit::list_page_after(&mut tx, account_id, 2, None).await.unwrap();
        tx.rollback().await.ok();
        assert_eq!(
            offset_page1.iter().map(|r| r.subject.clone()).collect::<Vec<_>>(),
            vec![Some("r3".to_string()), Some("r2".to_string())]
        );
        assert_eq!(
            keyset_page1.iter().map(|r| r.subject.clone()).collect::<Vec<_>>(),
            offset_page1.iter().map(|r| r.subject.clone()).collect::<Vec<_>>(),
            "page 1 must agree between the two pagination modes when nothing has changed yet"
        );

        // The write that lands BETWEEN page 1 and page 2 — a new NEWEST
        // row, r4, exactly the "admin is mid-walk when a fresh connector
        // event comes in" scenario this bug describes.
        seed_row(&pool, account_id, user_id, "r4", "2026-01-01T00:00:04Z").await;

        // Offset page 2 (OFFSET 2, LIMIT 2): the newest-first order is now
        // [r4, r3, r2, r1] — offset 2 lands on r2, which page 1 ALREADY
        // returned. That is the bug: a duplicate, not a fresh page.
        let mut tx = pool.begin().await.unwrap();
        let offset_page2 = connector_audit::list_page(&mut tx, account_id, 2, 2).await.unwrap();
        tx.rollback().await.ok();
        assert_eq!(
            offset_page2.iter().map(|r| r.subject.clone()).collect::<Vec<_>>(),
            vec![Some("r2".to_string()), Some("r1".to_string())],
            "demonstrates the bug: OFFSET drifts under a concurrent insert and RE-RETURNS r2, \
             already seen on offset page 1"
        );

        // Keyset page 2, cursor = the last row of keyset page 1 (r2): must
        // continue exactly where page 1 left off — r1, and ONLY r1 — never
        // re-showing r2 and never silently absorbing r4 (r4 sorts AHEAD of
        // the cursor, so it correctly never appears on this page at all).
        let last = keyset_page1.last().expect("page 1 non-empty");
        let mut tx = pool.begin().await.unwrap();
        let keyset_page2 = connector_audit::list_page_after(
            &mut tx,
            account_id,
            2,
            Some((last.occurred_at, last.id)),
        )
        .await
        .unwrap();
        tx.rollback().await.ok();
        assert_eq!(
            keyset_page2.iter().map(|r| r.subject.clone()).collect::<Vec<_>>(),
            vec![Some("r1".to_string())],
            "the fix: a keyset cursor is immune to the concurrent insert — page 2 continues \
             exactly where page 1 ended, no duplicate and no skip"
        );

        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
