//! Integration test for the admin atom-metadata backfill's loop-per-account
//! RLS conversion (org-tenancy OPS-4 Phase B.4, `routes/admin.rs`).
//!
//! `POST /internal/backfill-atom-metadata` used to run ONE cross-account
//! `UPDATE atoms … WHERE … IS NULL` sweep with no account filter — a query
//! shape that hard-errors under migration `0083_consumer_rls`'s fail-LOUD
//! strict predicate once OPS-4 switches the app onto the non-`BYPASSRLS`
//! `coralswarm_app` role. The conversion enumerates `accounts` (a control
//! table outside the RLS roster) and runs one `begin_account_scoped`
//! transaction pair per account. This test proves the load-bearing claims,
//! **executing the handler with `state.db` connected AS `coralswarm_app`**
//! (real login, not `SET ROLE`) so RLS is genuinely enforced:
//!
//!   1. Negative control: an ordinary unscoped `atoms` read on the app-role
//!      pool errors — RLS is active here, not merely untested.
//!   2. The real `/internal` HTTP router completes the backfill and BOTH
//!      accounts' slack atoms get `channel_id`/`author_name`/`channel_name`
//!      — i.e. the loop reaches every account despite each transaction being
//!      confined to one.
//!   3. Mid-loop isolation: inside one account's scoped transaction, the
//!      other account's atoms are invisible, and even a deliberately
//!      UNANCHORED sweep (the old query shape, no account filter) touches
//!      only the scoped account's rows — the RLS backstop working as
//!      designed.
//!   4. Idempotency: a second run reports zero further updates.
//!
//! Gated on `ORGT_TEST_BASE_URL`; same scratch-DB pattern as
//! `bootstrap_scope_pg.rs` / `rotate_dek_pg.rs`.
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test admin_backfill_rls_pg

use std::str::FromStr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use coralswarm_backend::AppState;
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

const APP_ROLE_TEST_PASSWORD: &str = "orgt_admin_backfill_test_only_pw";
const INTERNAL_TOKEN: &str = "orgt-admin-backfill-test-token";

/// Serializes tests in this file against the process-global `INTERNAL_TOKEN`
/// env var — same rationale as `rotate_dek_pg.rs`'s own `ENV_LOCK` (separate
/// compilation units can't share `#[cfg(test)]`-only guards).
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

struct TestEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}
impl TestEnv {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        unsafe {
            std::env::set_var("INTERNAL_TOKEN", INTERNAL_TOKEN);
        }
        Self { _guard: guard }
    }
}
impl Drop for TestEnv {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("INTERNAL_TOKEN");
        }
    }
}

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
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

/// A pool connected AS `coralswarm_app` (real login, not `SET ROLE`).
async fn app_role_pool(db_url: &str) -> PgPool {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_app")
        .password(APP_ROLE_TEST_PASSWORD);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .expect("connect AS coralswarm_app")
}

async fn call_backfill(app: &axum::Router, token: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/internal/backfill-atom-metadata");
    if let Some(tok) = token {
        builder = builder.header("x-internal-token", tok);
    }
    let req = builder.body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

struct Seeded {
    account_id: Uuid,
    graph_id: Uuid,
}

/// One (user, account, graph) fixture, seeded via the owner/BYPASSRLS pool.
async fn seed_account(owner: &PgPool, email: &str) -> Seeded {
    let account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(email) VALUES ($1) RETURNING id) \
         INSERT INTO accounts(owner_user_id) SELECT id FROM u RETURNING id",
    )
    .bind(email)
    .fetch_one(owner)
    .await
    .expect("insert user+account");
    let graph_id: Uuid =
        sqlx::query_scalar("INSERT INTO graphs(account_id, name) VALUES ($1, 'home') RETURNING id")
            .bind(account_id)
            .fetch_one(owner)
            .await
            .expect("insert graph");
    Seeded {
        account_id,
        graph_id,
    }
}

/// Insert a slack atom with NULL channel_id/author_name/channel_name — the
/// backfill's target shape. `header_len > 0` marks `content_inline` as
/// carrying a contextual chunk-header prefix of that byte length.
async fn seed_slack_atom(
    owner: &PgPool,
    graph_id: Uuid,
    source_id: &str,
    participant: &str,
    content_inline: &str,
    header_len: i32,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \
                           participants, timestamp, content_inline, header_len) \
         VALUES ($1, 'slack', $2, 'blob-key', ARRAY[$3], now(), $4, $5) \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(participant)
    .bind(content_inline)
    .bind(header_len)
    .fetch_one(owner)
    .await
    .expect("insert slack atom")
}

#[tokio::test]
async fn backfill_covers_all_accounts_as_app_role_with_rls_enforced() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping backfill_covers_all_accounts_as_app_role_with_rls_enforced: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = TestEnv::enable().await;

    let db = "orgt_admin_backfill";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083");

    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    // ── Fixtures: two accounts, both with slack atoms missing metadata ───────
    let a = seed_account(&owner, "backfill-a@x.co").await;
    let b = seed_account(&owner, "backfill-b@x.co").await;

    // A: one plain atom, one whose inline content carries a chunk-header
    // prefix (header_len > 0) that must be stripped before parsing.
    let a_atom_plain = seed_slack_atom(
        &owner,
        a.graph_id,
        "CALPHA/1719268112.000100",
        "Alice",
        "Alice in #alpha-channel · Jun 24\nhello from alpha",
        0,
    )
    .await;
    let chunk_header = "ctx: coralswarm demo\n";
    let a_atom_chunked = seed_slack_atom(
        &owner,
        a.graph_id,
        "CALPHA/1719268112.000200",
        "Bob",
        &format!("{chunk_header}Bob in #alpha-channel · Jun 25\nsecond message"),
        chunk_header.len() as i32,
    )
    .await;
    // A: a NON-slack atom that every pass must leave untouched.
    let a_atom_other: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', 'probe-1', 'blob-key', now()) RETURNING atom_id",
    )
    .bind(a.graph_id)
    .fetch_one(&owner)
    .await
    .expect("insert non-slack atom");

    // B: one atom missing everything, plus one already fully backfilled that
    // must not be re-touched.
    let b_atom = seed_slack_atom(
        &owner,
        b.graph_id,
        "CBETA/1719268112.000300",
        "Carol",
        "Carol in #beta-channel · Jun 26\ngreetings from beta",
        0,
    )
    .await;
    sqlx::query(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, participants, \
                           timestamp, content_inline, channel_id, channel_name, author_name) \
         VALUES ($1, 'slack', 'CBETA/1719268112.000400', 'blob-key', ARRAY['Dave'], now(), \
                 'Dave in #beta-channel · Jun 27\nalready done', 'CBETA', 'preset-channel', 'Dave')",
    )
    .bind(b.graph_id)
    .execute(&owner)
    .await
    .expect("insert already-backfilled atom");

    // ── The handler's pool connects AS coralswarm_app (non-BYPASSRLS) ────────
    let app_pool = app_role_pool(&db_url).await;

    // Negative control: an ordinary unscoped RLS-covered read on this pool
    // must hard-error — proving RLS is genuinely enforced for everything the
    // handler does below.
    {
        let unscoped: Result<Vec<Uuid>, sqlx::Error> =
            sqlx::query_scalar("SELECT atom_id FROM atoms")
                .fetch_all(&app_pool)
                .await;
        assert!(
            unscoped.is_err(),
            "sanity check: unscoped atoms read as coralswarm_app must error — got {unscoped:?}"
        );
    }

    // ── Drive the REAL /internal router with state.db = the app-role pool ────
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(app_pool.clone());
    let state = Arc::new(state);
    let app = coralswarm_backend::test_support_internal_app(state);

    // No token → 401, before anything else.
    let (status, _) = call_backfill(&app, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // First run: both accounts fully backfilled in one call.
    let (status, body) = call_backfill(&app, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(body["accounts_processed"], 2, "body: {body:?}");
    // 3 atoms were missing channel_id/author_name (A×2 + B×1); the preset and
    // non-slack atoms match no pass.
    assert_eq!(body["channel_id_author_rows"], 3, "body: {body:?}");
    assert_eq!(body["channel_name_updated"], 3, "body: {body:?}");
    assert_eq!(body["channel_name_remaining"], 0, "body: {body:?}");

    // ── Verify row-level outcomes via the owner pool (sees everything) ───────
    let fetch = |atom_id: Uuid| {
        let owner = owner.clone();
        async move {
            let row: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(
                "SELECT channel_id, author_name, channel_name FROM atoms WHERE atom_id = $1",
            )
            .bind(atom_id)
            .fetch_one(&owner)
            .await
            .expect("fetch atom row");
            row
        }
    };
    assert_eq!(
        fetch(a_atom_plain).await,
        (
            Some("CALPHA".into()),
            Some("Alice".into()),
            Some("alpha-channel".into())
        ),
        "account A's plain atom must be fully backfilled"
    );
    assert_eq!(
        fetch(a_atom_chunked).await,
        (
            Some("CALPHA".into()),
            Some("Bob".into()),
            Some("alpha-channel".into())
        ),
        "the chunk-header prefix must be stripped before parsing channel_name"
    );
    assert_eq!(
        fetch(b_atom).await,
        (
            Some("CBETA".into()),
            Some("Carol".into()),
            Some("beta-channel".into())
        ),
        "account B must be reached by the per-account loop too"
    );
    assert_eq!(
        fetch(a_atom_other).await,
        (None, None, None),
        "non-slack atoms must stay untouched"
    );

    // ── Mid-loop isolation: one account's scoped transaction cannot see (or
    // sweep) the other account's rows ─────────────────────────────────────────
    {
        let mut tx = begin_account_scoped(&app_pool, a.account_id)
            .await
            .expect("open scoped tx for account A");
        let visible: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM atoms")
            .fetch_all(&mut *tx)
            .await
            .expect("scoped atoms read must succeed");
        assert!(visible.contains(&a_atom_plain) && visible.contains(&a_atom_chunked));
        assert!(
            !visible.contains(&b_atom),
            "account A's scoped transaction must NOT see account B's atoms"
        );

        // The old query shape — an UNANCHORED cross-account sweep — now only
        // reaches the scoped account's rows (3 = A's two slack atoms + none
        // of B's): the RLS backstop, independent of the explicit WHERE anchor
        // the handler also carries.
        let swept =
            sqlx::query("UPDATE atoms SET channel_name = channel_name WHERE source_type = 'slack'")
                .execute(&mut *tx)
                .await
                .expect("scoped unanchored sweep must succeed")
                .rows_affected();
        assert_eq!(
            swept, 2,
            "an unanchored sweep inside A's scope must touch exactly A's slack atoms"
        );
        // Roll back — this probe must not perturb the idempotency check below.
        tx.rollback().await.expect("rollback probe tx");
    }

    // ── Idempotency: a second run finds nothing left to do ───────────────────
    let (status, body) = call_backfill(&app, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(body["channel_id_author_rows"], 0, "body: {body:?}");
    assert_eq!(body["channel_name_updated"], 0, "body: {body:?}");
    assert_eq!(body["channel_name_remaining"], 0, "body: {body:?}");

    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}
