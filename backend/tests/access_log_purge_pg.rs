//! `user_access_log` schema + purge worker — live-Postgres proof (platform-
//! admin-metrics PR 1 of 7, migration `0095_platform_metrics`).
//!
//! Scratch-DB pattern shared by every `*_pg.rs` file in this directory
//! (`member_ocean_reconciler_pg.rs` is the closest sibling: same
//! `admin`/`recreate`/`with_db`/`control_pool` helpers, minus the tenant-pool
//! half — this suite needs only the control tree, since `user_access_log`
//! has no tenant-tree counterpart at all, see the migration's header).
//!
//! Proves three properties, against the database rather than in-process:
//!   1. **Idempotent login recording** — the `UNIQUE (user_id,
//!      clerk_session_id)` constraint makes an `ON CONFLICT DO NOTHING`
//!      insert of the same session a no-op, so "one row per login" holds
//!      even if the (future) middleware's insert runs on every request of a
//!      session, not just its first.
//!   2. **Retention purge** — `workers::access_log_purge::tick` deletes rows
//!      older than `ACCESS_LOG_RETENTION_DAYS` and leaves everything inside
//!      the window untouched, including a row exactly one second inside the
//!      cutoff.
//!   3. **Cascade on user deletion** — `user_id ... REFERENCES users(id) ON
//!      DELETE CASCADE` means a deleted user's access-log rows disappear
//!      with them rather than becoming orphaned FK violations.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same convention as every other `*_pg.rs` file here.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test access_log_purge_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::workers::access_log_purge::{tick, ACCESS_LOG_RETENTION_DAYS};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

// ── Scratch-DB plumbing (pattern shared by every `*_pg.rs` file) ─────────────

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
    conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
}
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&with_db(base, db))
        .await
        .expect("connect control");
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

async fn mint_user(pool: &PgPool, email: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(pool)
        .await
        .expect("insert user")
}

async fn insert_login(pool: &PgPool, user_id: Uuid, session_id: &str, client: Option<&str>) {
    sqlx::query(
        "INSERT INTO user_access_log (user_id, clerk_session_id, client) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (user_id, clerk_session_id) DO NOTHING",
    )
    .bind(user_id)
    .bind(session_id)
    .bind(client)
    .execute(pool)
    .await
    .expect("insert access log row");
}

async fn access_log_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM user_access_log")
        .fetch_one(pool)
        .await
        .expect("count user_access_log")
}

async fn backdate(pool: &PgPool, user_id: Uuid, session_id: &str, created_at: chrono::DateTime<Utc>) {
    sqlx::query("UPDATE user_access_log SET created_at = $3 WHERE user_id = $1 AND clerk_session_id = $2")
        .bind(user_id)
        .bind(session_id)
        .bind(created_at)
        .execute(pool)
        .await
        .expect("backdate access log row");
}

#[tokio::test]
async fn one_row_per_login_via_on_conflict_do_nothing() {
    let Some(base) = base_url() else {
        eprintln!("skipping one_row_per_login_via_on_conflict_do_nothing: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_alp1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let user_id = mint_user(&pool, &format!("{}@test.local", Uuid::new_v4())).await;

    // Same session, three "requests" — only the first should actually insert.
    insert_login(&pool, user_id, "sess-abc", Some("desktop")).await;
    insert_login(&pool, user_id, "sess-abc", Some("desktop")).await;
    insert_login(&pool, user_id, "sess-abc", Some("desktop")).await;
    assert_eq!(
        access_log_count(&pool).await,
        1,
        "UNIQUE (user_id, clerk_session_id) + ON CONFLICT DO NOTHING must dedupe repeat requests \
         on the same session down to exactly one row"
    );

    // A genuinely different session for the same user IS a second row.
    insert_login(&pool, user_id, "sess-def", None).await;
    assert_eq!(access_log_count(&pool).await, 2, "a different session id is a new login");

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

#[tokio::test]
async fn purge_deletes_only_rows_past_the_retention_window() {
    let Some(base) = base_url() else {
        eprintln!("skipping purge_deletes_only_rows_past_the_retention_window: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_alp2_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let user_id = mint_user(&pool, &format!("{}@test.local", Uuid::new_v4())).await;

    // Fresh row: created "now", well inside the window.
    insert_login(&pool, user_id, "sess-fresh", Some("web")).await;

    // Just inside the window: retention days minus one hour.
    let just_inside = Utc::now() - chrono::Duration::days(ACCESS_LOG_RETENTION_DAYS) + chrono::Duration::hours(1);
    insert_login(&pool, user_id, "sess-just-inside", Some("web")).await;
    backdate(&pool, user_id, "sess-just-inside", just_inside).await;

    // Just past the window: retention days plus one hour.
    let just_past = Utc::now() - chrono::Duration::days(ACCESS_LOG_RETENTION_DAYS) - chrono::Duration::hours(1);
    insert_login(&pool, user_id, "sess-just-past", Some("web")).await;
    backdate(&pool, user_id, "sess-just-past", just_past).await;

    // Way past the window.
    let ancient = Utc::now() - chrono::Duration::days(ACCESS_LOG_RETENTION_DAYS * 3);
    insert_login(&pool, user_id, "sess-ancient", Some("web")).await;
    backdate(&pool, user_id, "sess-ancient", ancient).await;

    assert_eq!(access_log_count(&pool).await, 4, "all four rows present before the purge runs");

    let purged = tick(&pool).await.expect("tick ok");
    assert_eq!(purged, 2, "exactly the two expired rows (just-past, ancient) are purged");
    assert_eq!(access_log_count(&pool).await, 2, "the two in-window rows survive");

    let remaining: Vec<String> = sqlx::query_scalar("SELECT clerk_session_id FROM user_access_log ORDER BY clerk_session_id")
        .fetch_all(&pool)
        .await
        .expect("remaining sessions");
    assert_eq!(remaining, vec!["sess-fresh".to_string(), "sess-just-inside".to_string()]);

    // A second tick is idempotent: nothing left to purge.
    let purged_again = tick(&pool).await.expect("tick ok (second pass)");
    assert_eq!(purged_again, 0, "second tick finds nothing left expired");

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

#[tokio::test]
async fn deleting_the_user_cascades_to_their_access_log_rows() {
    let Some(base) = base_url() else {
        eprintln!("skipping deleting_the_user_cascades_to_their_access_log_rows: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_alp3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let user_id = mint_user(&pool, &format!("{}@test.local", Uuid::new_v4())).await;
    insert_login(&pool, user_id, "sess-to-be-orphaned", Some("desktop")).await;
    assert_eq!(access_log_count(&pool).await, 1);

    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("delete user");

    assert_eq!(
        access_log_count(&pool).await,
        0,
        "ON DELETE CASCADE must remove the deleted user's access-log rows, not orphan them"
    );

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}
