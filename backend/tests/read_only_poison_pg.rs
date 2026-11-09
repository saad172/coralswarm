//! COR-82: a pooled connection left in `default_transaction_read_only = on`
//! made every org write fail with SQLSTATE 25006 for 27 minutes, until
//! PgBouncer retired the server connection on its own.
//!
//! The reproduction is the incident's own trigger in miniature: a one
//! connection pool, a bare session-level `SET default_transaction_read_only =
//! on` on it, and a worker tick that writes through the pool. Without the
//! heal every tick fails forever, because the harness retries on the same
//! poisoned connection. With it, the first 25006 makes the harness discard the
//! backend and the next tick's write lands.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as
//! `worker_harness_pg.rs`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test read_only_poison_pg
//!
//! The second test runs the same trigger through a real PgBouncer in
//! transaction mode, the topology of the incident, where the poisoned state
//! lives on a server connection the app's pool never owns. CI has no
//! PgBouncer, so it runs only when `ORGT_TEST_PGBOUNCER_URL` points at one
//! (`pool_mode = transaction`, `max_prepared_statements > 0`):
//!
//!   ORGT_TEST_PGBOUNCER_URL=postgres://postgres@localhost:6482/postgres \
//!     cargo test -p coralswarm-backend --test read_only_poison_pg

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coralswarm_backend::shutdown::Shutdown;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::workers::tenant_harness::{
    run_multi_tenant, HarnessConfig, TenantActivity, Tick, WorkerTenant,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

async fn recreate(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}

fn fast_cfg() -> HarnessConfig {
    HarnessConfig {
        name: "cor82",
        base_poll: Duration::from_millis(20),
        error_sleep: Duration::from_millis(50),
        hot_loop_on_work: false,
        wake_on_enqueue: false,
        max_org_idle_backoff: Duration::from_millis(200),
        tenant_refresh: Duration::from_millis(100),
        pool_build_timeout: Duration::from_secs(5),
        include_revoked_orgs: false,
    }
}

/// What the writing tick saw, recorded without touching the database.
#[derive(Default)]
struct TickOutcomes {
    read_only_rejections: AtomicUsize,
    landed: AtomicUsize,
}

type TickFn = dyn Fn(WorkerTenant) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Tick, String>> + Send>>
    + Send
    + Sync;

fn writing_tick() -> Box<TickFn> {
    writing_tick_into("cor82_writes", Arc::new(TickOutcomes::default()))
}

fn writing_tick_into(table: &'static str, outcomes: Arc<TickOutcomes>) -> Box<TickFn> {
    Box::new(move |t: WorkerTenant| {
        let outcomes = outcomes.clone();
        Box::pin(async move {
            let result = sqlx::query(&format!("INSERT INTO {table} (n) VALUES (1)"))
                .execute(&t.pool)
                .await;
            match result {
                Ok(_) => {
                    outcomes.landed.fetch_add(1, Ordering::SeqCst);
                    Ok(Tick::worked(1))
                }
                Err(e) => {
                    if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("25006") {
                        outcomes.read_only_rejections.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(format!("write failed: {e}"))
                }
            }
        })
    })
}

async fn writes_landed(pool: &PgPool) -> i64 {
    count_rows(pool, "cor82_writes").await
}

async fn count_rows(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if cond().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn harness_recovers_from_a_read_only_poisoned_connection() {
    let Some(base) = base_url() else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "ORGT_TEST_BASE_URL is required"
        );
        eprintln!("skipping harness_recovers_from_a_read_only_poisoned_connection: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_cor82_readonly";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = PgConnection::connect(&base).await.expect("connect admin");
    recreate(&mut admin, db).await;

    // One connection, so the poisoned connection is the only one the harness
    // can ever draw: the same shape as PgBouncer handing the app its most
    // recently released server connection every time.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&with_db(&base, db))
        .await
        .expect("connect scratch db");
    sqlx::query("CREATE TABLE cor82_writes (id serial PRIMARY KEY, n int NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    // The incident's trigger: a bare session SET on a pooled connection.
    sqlx::query("SET default_transaction_read_only = on")
        .execute(&pool)
        .await
        .unwrap();
    let poisoned_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let rejected = sqlx::query("INSERT INTO cor82_writes (n) VALUES (0)")
        .execute(&pool)
        .await
        .expect_err("a poisoned connection rejects writes");
    assert_eq!(
        rejected.as_database_error().and_then(|d| d.code()).as_deref(),
        Some("25006"),
        "the rejection is SQLSTATE 25006 read_only_sql_transaction"
    );

    let activity = TenantActivity::new();
    let shutdown = Shutdown::new();
    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
    let handle = tokio::spawn(run_multi_tenant(
        pool.clone(),
        store,
        None,
        activity.clone(),
        fast_cfg(),
        shutdown.clone(),
        writing_tick(),
    ));

    wait_until(
        "a write to land after the harness discards the poisoned connection",
        Duration::from_secs(10),
        || {
            let pool = pool.clone();
            async move { writes_landed(&pool).await >= 1 }
        },
    )
    .await;

    let (pid_now, read_only_now): (i32, bool) = sqlx::query_as(
        "SELECT pg_backend_pid(), current_setting('default_transaction_read_only')::bool",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_ne!(pid_now, poisoned_pid, "the poisoned backend was discarded, not reused");
    assert!(!read_only_now, "the replacement connection is read-write");

    let ledger = activity.read_only.snapshot();
    assert_eq!(ledger.len(), 1, "one tenant recorded: {ledger:?}");
    let (tenant, record) = &ledger[0];
    assert_eq!(tenant, "consumer");
    assert!(record.rejections >= 1, "the 25006 was counted: {record:?}");
    assert_eq!(
        record.discarded_backends, 1,
        "exactly the one poisoned backend was terminated: {record:?}"
    );
    assert!(record.last_discard_at.is_some());

    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("harness stops on shutdown")
        .expect("harness task did not panic");

    pool.close().await;
    admin
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

#[tokio::test]
async fn harness_recovers_from_a_read_only_server_behind_pgbouncer() {
    let Ok(url) = std::env::var("ORGT_TEST_PGBOUNCER_URL") else {
        eprintln!(
            "skipping harness_recovers_from_a_read_only_server_behind_pgbouncer: set ORGT_TEST_PGBOUNCER_URL"
        );
        return;
    };
    let table = "cor82_pgbouncer_writes";

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect through pgbouncer");
    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE TABLE {table} (id serial PRIMARY KEY, n int NOT NULL)"))
        .execute(&pool)
        .await
        .unwrap();

    // Several live server connections, so the poisoned one is not the only
    // server PgBouncer can hand out.
    let mut held = Vec::new();
    for _ in 0..3 {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT 1").execute(&mut *tx).await.unwrap();
        held.push(tx);
    }
    for tx in held {
        tx.rollback().await.unwrap();
    }

    // The incident's trigger, verbatim: an outside client's bare session SET
    // through the pooler. The client leaves; the server keeps the setting.
    let mut outsider = PgConnection::connect(&url).await.expect("outside client");
    outsider
        .execute("SET default_transaction_read_only = on")
        .await
        .unwrap();
    let poisoned_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut outsider)
        .await
        .unwrap();
    outsider.close().await.unwrap();

    // Nothing but the harness touches the pool until a write lands. Any other
    // query here would race the harness for the poisoned server and could
    // leave it idle and untouched, which proves nothing either way.
    let outcomes = Arc::new(TickOutcomes::default());
    let activity = TenantActivity::new();
    let shutdown = Shutdown::new();
    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
    let handle = tokio::spawn(run_multi_tenant(
        pool.clone(),
        store,
        None,
        activity.clone(),
        fast_cfg(),
        shutdown.clone(),
        writing_tick_into(table, outcomes.clone()),
    ));

    wait_until(
        "a harness write to land after the poisoned server rejected one",
        Duration::from_secs(10),
        || {
            let outcomes = outcomes.clone();
            async move { outcomes.landed.load(Ordering::SeqCst) >= 1 }
        },
    )
    .await;
    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("harness stops on shutdown")
        .expect("harness task did not panic");

    assert!(
        outcomes.read_only_rejections.load(Ordering::SeqCst) >= 1,
        "PgBouncer handed the harness the poisoned server and the write was rejected with 25006"
    );
    let alive: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid = $1")
        .bind(poisoned_pid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(alive, 0, "the poisoned server backend {poisoned_pid} was terminated");

    // Hold as many transactions as the pool allows so each sample is a
    // distinct server.
    let mut held = Vec::new();
    for _ in 0..4 {
        let mut tx = pool.begin().await.unwrap();
        let read_only: bool =
            sqlx::query_scalar("SELECT current_setting('default_transaction_read_only')::bool")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert!(!read_only, "no server behind PgBouncer is still read-only");
        held.push(tx);
    }
    drop(held);

    let ledger = activity.read_only.snapshot();
    assert_eq!(ledger.len(), 1, "one tenant recorded: {ledger:?}");
    let record = &ledger[0].1;
    assert!(record.rejections >= 1, "the 25006 was counted: {record:?}");
    assert_eq!(
        record.discarded_backends, 1,
        "the heal terminated exactly the poisoned server: {record:?}"
    );

    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(&pool)
        .await
        .ok();
    pool.close().await;
}
