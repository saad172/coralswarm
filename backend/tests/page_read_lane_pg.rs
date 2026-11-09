//! Page-load standard L1 (`docs/plans/page-load-standard/design.md` §4.3,
//! §10): the transaction a `RouteClass::PageRead` request opens, and the
//! two-lane tenant pool it runs on, against a real Postgres.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a local Postgres with CREATE DATABASE
//! rights). The tenant database is a migrated template clone
//! (`test_db::migrated_database`), so the registry's fingerprint and
//! org-ocean checks run exactly as in production.
//!
//! The last test is the L1 measurement: with every work-lane connection held
//! by a long statement, a page read through `Tenant::begin_member_scoped`
//! still answers in milliseconds, where the same read on one shared pool
//! waits for a holder to finish. Run it alone with `--nocapture` to see the
//! numbers:
//!
//! ```text
//! ORGT_TEST_BASE_URL=postgres://localhost:5432/postgres \
//!   cargo test -p coralswarm-backend --test page_read_lane_pg -- --nocapture
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coralswarm_backend::route_class::RouteClass;
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::tenancy::registry::{
    lane_split, TenantLanes, TenantRegistry, DEFAULT_MAX_CONNECTIONS,
};
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext};
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::test_db::{migrated_database, Plane, TestDatabase};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

const ORG: &str = "org_page_lane";

fn base() -> Option<PgConnectOptions> {
    let url = std::env::var("ORGT_TEST_BASE_URL").ok()?;
    Some(PgConnectOptions::from_str(&url).expect("ORGT_TEST_BASE_URL is a Postgres URL"))
}

fn url_of(base: &PgConnectOptions, db: &TestDatabase) -> String {
    let raw = std::env::var("ORGT_TEST_BASE_URL").unwrap();
    let (prefix, rest) = raw.split_at(raw.rfind('/').expect("url path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    let _ = base;
    format!("{prefix}/{}{params}", db.name())
}

struct Fixture {
    _db: TestDatabase,
    registry: Arc<TenantRegistry>,
    account_id: Uuid,
    user_id: Uuid,
    lanes: TenantLanes,
    url: String,
}

/// A migrated tenant database with its fingerprint row, and the registry's
/// own two-lane request pool built against it the way the gate builds it.
async fn fixture(base: &PgConnectOptions) -> Fixture {
    let db = migrated_database(base, Plane::Tenant, "page_lane").await;
    let url = url_of(base, &db);
    let setup = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1)")
        .bind(ORG)
        .execute(&setup)
        .await
        .unwrap();
    setup.close().await;
    let account_id = Uuid::new_v4();
    let registry = Arc::new(TenantRegistry::new(Arc::new(
        StaticMapResolver::from_pairs([("SECRET_LANE", url.clone())]),
    )));
    let lanes = registry
        .get_or_build_lanes(ORG, account_id, "SECRET_LANE")
        .await
        .unwrap();
    Fixture {
        _db: db,
        registry,
        account_id,
        user_id: Uuid::new_v4(),
        lanes,
        url,
    }
}

impl Fixture {
    /// The `Tenant` the extractor builds for a request of `class` on this
    /// org: the page lane as `pool` for `PageRead`, the work lane otherwise
    /// (`tenancy::extractor`).
    fn tenant(&self, class: RouteClass) -> Tenant {
        let pool = match class {
            RouteClass::PageRead => self.lanes.page.clone(),
            RouteClass::Stream | RouteClass::Other => self.lanes.work.clone(),
        };
        Tenant {
            store: Some(Arc::new(PostgresStore::new(pool.clone()))),
            pool: Some(pool.clone()),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: ORG.to_string(),
                account_id: self.account_id,
                user_id: self.user_id,
                pool,
                page_pool: Some(self.lanes.page.clone()),
            })),
            class,
        }
    }
}

async fn show(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, guc: &str) -> String {
    sqlx::query_scalar(&format!("SHOW {guc}"))
        .fetch_one(&mut **tx)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_page_read_transaction_is_read_only_with_a_one_second_statement_timeout() {
    let Some(base) = base() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let f = fixture(&base).await;

    let page = f.tenant(RouteClass::PageRead);
    let mut tx = page.begin_member_scoped(f.account_id).await.unwrap();
    assert_eq!(show(&mut tx, "transaction_read_only").await, "on");
    assert_eq!(show(&mut tx, "statement_timeout").await, "1s");
    assert_eq!(
        show(&mut tx, "app.user_id").await,
        f.user_id.to_string(),
        "member scope still applies"
    );
    let write = sqlx::query("CREATE TABLE page_read_probe(x int)")
        .execute(&mut *tx)
        .await;
    let code = match write {
        Err(sqlx::Error::Database(e)) => e.code().map(|c| c.to_string()),
        other => panic!("a write inside a page read must fail at the database, got {other:?}"),
    };
    assert_eq!(code.as_deref(), Some("25006"), "read_only_sql_transaction");
    tx.rollback().await.unwrap();

    let other = f.tenant(RouteClass::Other);
    let mut tx = other.begin_member_scoped(f.account_id).await.unwrap();
    assert_eq!(show(&mut tx, "transaction_read_only").await, "off");
    assert_eq!(
        show(&mut tx, "statement_timeout").await,
        "10s",
        "today's W2 request timeout, unchanged"
    );
    tx.rollback().await.unwrap();

    // The lanes are the registry's: page 2, work 3 at the default budget.
    let (page_n, work_n) = lane_split(DEFAULT_MAX_CONNECTIONS, 2);
    assert_eq!(f.lanes.page.options().get_max_connections(), page_n);
    assert_eq!(f.lanes.work.options().get_max_connections(), work_n);
    assert_eq!(f.registry.live_pool_count(), 1);
    let stats = f.registry.live_lane_stats();
    assert_eq!(
        stats.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        vec![format!("org:{ORG}:work"), format!("org:{ORG}:page")]
    );
}

#[tokio::test]
async fn a_consumer_plane_page_read_is_read_only_and_bounded_too() {
    let Some(base) = base() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let f = fixture(&base).await;
    // The consumer plane: no decision, one pool, class from the route.
    let consumer = |class| Tenant {
        store: None,
        pool: Some(f.lanes.work.clone()),
        decision: None,
        class,
    };
    let mut tx = consumer(RouteClass::PageRead)
        .begin_member_scoped(f.account_id)
        .await
        .unwrap();
    assert_eq!(show(&mut tx, "transaction_read_only").await, "on");
    assert_eq!(show(&mut tx, "statement_timeout").await, "1s");
    assert_eq!(
        show(&mut tx, "app.account_id").await,
        f.account_id.to_string()
    );
    tx.rollback().await.unwrap();

    let mut tx = consumer(RouteClass::Other)
        .begin_member_scoped(f.account_id)
        .await
        .unwrap();
    assert_eq!(show(&mut tx, "transaction_read_only").await, "off");
    assert_eq!(
        show(&mut tx, "statement_timeout").await,
        "0",
        "the consumer plane sets none for Other"
    );
    tx.rollback().await.unwrap();
}

/// Hold every connection of `pool` inside a `pg_sleep(hold)` statement, plus
/// `extra` more callers queued on acquire, and keep them there until the
/// returned handles are awaited.
fn saturate(pool: &PgPool, extra: usize, hold: Duration) -> Vec<tokio::task::JoinHandle<()>> {
    let n = pool.options().get_max_connections() as usize + extra;
    (0..n)
        .map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut conn = pool
                    .acquire()
                    .await
                    .expect("acquire within the 5 s timeout");
                sqlx::query("SELECT pg_sleep($1)")
                    .bind(hold.as_secs_f64())
                    .execute(&mut *conn)
                    .await
                    .unwrap();
            })
        })
        .collect()
}

/// One page read through the real request-path opener: `begin_member_scoped`
/// (acquire, `BEGIN READ ONLY`, the scope GUCs), one indexed statement,
/// rollback.
async fn page_read(tenant: &Tenant, account_id: Uuid) -> Duration {
    let started = Instant::now();
    let mut tx = tenant.begin_member_scoped(account_id).await.unwrap();
    let _: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    started.elapsed()
}

/// The L1 measurement. Work lane saturated by long statements, page lane
/// idle: a page read must not queue behind the holders. The baseline is the
/// pre-L1 shape, one shared five-connection pool, where the same read waits
/// for a holder.
#[tokio::test]
async fn page_reads_do_not_queue_behind_a_saturated_work_lane() {
    let Some(base) = base() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let f = fixture(&base).await;
    let hold = Duration::from_secs(2);
    let rounds = 3;

    // Warm both lanes so the first timed read is not a connection handshake.
    let page = f.tenant(RouteClass::PageRead);
    for _ in 0..3 {
        page_read(&page, f.account_id).await;
    }

    let mut laned = Vec::new();
    for _ in 0..rounds {
        let holders = saturate(&f.lanes.work, 3, hold);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let d = page_read(&page, f.account_id).await;
        laned.push(d);
        for h in holders {
            h.await.unwrap();
        }
    }

    // Baseline: the pre-L1 shape. One pool of DEFAULT_MAX_CONNECTIONS against
    // the same database, every connection held, and the read goes through
    // the same opener as a plain `Other` request would have.
    let shared = PgPoolOptions::new()
        .max_connections(DEFAULT_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&f.url)
        .await
        .unwrap();
    let shared_tenant = Tenant {
        store: None,
        pool: Some(shared.clone()),
        decision: Some(TenancyDecision::Org(TenantContext {
            org_id: ORG.to_string(),
            account_id: f.account_id,
            user_id: f.user_id,
            pool: shared.clone(),
            page_pool: None,
        })),
        class: RouteClass::PageRead,
    };
    for _ in 0..3 {
        page_read(&shared_tenant, f.account_id).await;
    }
    let mut queued = Vec::new();
    for _ in 0..rounds {
        let holders = saturate(&shared, 1, hold);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let d = page_read(&shared_tenant, f.account_id).await;
        queued.push(d);
        for h in holders {
            h.await.unwrap();
        }
    }
    shared.close().await;

    let ms = |d: &Duration| d.as_secs_f64() * 1000.0;
    println!(
        "page read behind a saturated WORK lane (two lanes, page idle): {:?}",
        laned.iter().map(ms).collect::<Vec<_>>()
    );
    println!(
        "page read on ONE shared {DEFAULT_MAX_CONNECTIONS}-connection pool, all held: {:?}",
        queued.iter().map(ms).collect::<Vec<_>>()
    );

    for d in &laned {
        assert!(
            *d < Duration::from_millis(250),
            "page read waited {d:?} behind the work lane"
        );
    }
    for d in &queued {
        // The holders sleep `hold`; a queued read waits for the first to
        // finish, about `hold - 300 ms`, and never less than a second.
        assert!(
            *d > Duration::from_secs(1),
            "the shared-pool baseline did not queue ({d:?}); the holders were not holding"
        );
    }
}

/// A failed begin is converted once, not recorded at acquire and again on
/// StoreError conversion. Also exercise the real one-second cancellation.
#[tokio::test]
async fn page_read_database_timeouts_are_recorded_once() {
    let Some(base) = base() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let f = fixture(&base).await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(50))
        .connect(&f.url)
        .await
        .unwrap();
    let tenant = Tenant {
        store: None,
        pool: Some(pool.clone()),
        decision: None,
        class: RouteClass::PageRead,
    };
    let held = pool.acquire().await.unwrap();
    let (result, timing) = coralswarm_backend::request_timing::scope(
        Instant::now(),
        RouteClass::PageRead,
        tenant.begin_member_scoped(f.account_id),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(timing.page_read_signals().acquire_timeouts, 1);
    drop(held);
    let ((), timing) =
        coralswarm_backend::request_timing::scope(Instant::now(), RouteClass::PageRead, async {
            let mut tx = tenant.begin_member_scoped(f.account_id).await.unwrap();
            let error = sqlx::query("SELECT pg_sleep(2)")
                .execute(&mut *tx)
                .await
                .unwrap_err();
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("57014")
            );
            let _: coralswarm_backend::store::StoreError = error.into();
            tx.rollback().await.unwrap();
        })
        .await;
    assert_eq!(timing.page_read_signals().statement_timeouts, 1);
    assert_eq!(timing.page_read_signals().acquire_timeouts, 0);
    pool.close().await;
}

#[tokio::test]
async fn raw_read_failures_count_once_with_or_without_store_conversion() {
    use coralswarm_backend::{request_timing, tenancy::scope};
    let Some(base) = base() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let f = fixture(&base).await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(50))
        .connect(&f.url)
        .await
        .unwrap();
    let ((), timing) = request_timing::scope(Instant::now(), RouteClass::PageRead, async {
        // A fallback/string consumer never converts through StoreError.
        let swallowed = sqlx::query("SELECT pg_sleep(2)")
            .fetch_optional(scope::request_read(&pool))
            .await
            .unwrap_err();
        assert_eq!(
            swallowed.as_database_error().unwrap().code().as_deref(),
            Some("57014")
        );
        assert!(swallowed
            .as_database_error()
            .unwrap()
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            .is_some());
        let _fallback = swallowed.to_string();
        // fetch_all uses the streaming Executor path; conversion must not
        // count this second cancellation a second time.
        let forwarded = sqlx::query("SELECT pg_sleep(2)")
            .fetch_all(scope::request_read(&pool))
            .await
            .unwrap_err();
        let stored = coralswarm_backend::store::StoreError::from(forwarded);
        let coralswarm_backend::store::StoreError::Db(inner) = stored else {
            panic!("expected SQL error")
        };
        let original = inner.downcast_ref::<sqlx::Error>().unwrap();
        assert!(original
            .as_database_error()
            .unwrap()
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            .is_some());
    })
    .await;
    assert_eq!(timing.page_read_signals().statement_timeouts, 2);
    assert_eq!(timing.page_read_signals().acquire_timeouts, 0);

    let held = pool.acquire().await.unwrap();
    let ((), timing) = request_timing::scope(Instant::now(), RouteClass::PageRead, async {
        let raw = sqlx::query("SELECT 1")
            .fetch_optional(scope::request_read(&pool))
            .await
            .unwrap_err();
        assert_eq!(
            request_timing::PageReadFailure::of(&raw),
            Some(request_timing::PageReadFailure::AcquireTimeout)
        );
        let _fallback = raw.to_string();
        let forwarded = sqlx::query("SELECT 1")
            .fetch_all(scope::request_read(&pool))
            .await
            .unwrap_err();
        let stored = coralswarm_backend::store::StoreError::from(forwarded);
        let coralswarm_backend::store::StoreError::Db(inner) = stored else {
            panic!("expected SQL error")
        };
        assert!(matches!(
            inner.downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::PoolTimedOut)
        ));
    })
    .await;
    assert_eq!(timing.page_read_signals().statement_timeouts, 0);
    assert_eq!(timing.page_read_signals().acquire_timeouts, 2);
    drop(held);
    pool.close().await;
}

#[tokio::test]
async fn shared_control_read_helpers_preserve_scope_and_reset_the_connection() {
    use coralswarm_backend::{request_timing, tenancy::scope};
    let Some(base) = base() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let f = fixture(&base).await;
    // One connection makes reset/leakage assertions meaningful.
    let pool = PgPoolOptions::new().max_connections(1).connect(&f.url).await.unwrap();
    request_timing::scope(Instant::now(), RouteClass::PageRead, async {
        let settings: (String, String) = sqlx::query_as(
            "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout')",
        ).fetch_one(scope::request_read(&pool)).await.unwrap();
        assert_eq!(settings, ("on".into(), "1s".into()));
        let mut tx = scope::begin_request_tenant_read(&pool, f.account_id, f.user_id).await.unwrap();
        assert_eq!(show(&mut tx, "app.account_id").await, f.account_id.to_string());
        assert_eq!(show(&mut tx, "app.user_id").await, f.user_id.to_string());
        assert_eq!(show(&mut tx, "transaction_read_only").await, "on");
        assert_eq!(show(&mut tx, "statement_timeout").await, "1s");
        tx.rollback().await.unwrap();
        let mut tx = scope::begin_request_system_read(&pool, f.account_id).await.unwrap();
        assert_eq!(show(&mut tx, "app.tenant_actor").await, "system");
        assert_eq!(show(&mut tx, "statement_timeout").await, "1s");
        tx.rollback().await.unwrap();
        let write = sqlx::query("CREATE TABLE l1_read_executor_write(x int)")
            .execute(scope::request_read(&pool)).await.unwrap_err();
        assert!(matches!(write, sqlx::Error::Database(ref e) if e.code().as_deref() == Some("25006")));
    }).await;
    let settings: (String, String) = sqlx::query_as(
        "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout')",
    ).fetch_one(scope::request_read(&pool)).await.unwrap();
    assert_eq!(settings, ("off".into(), "0".into()), "local settings must not leak to Other");
    pool.close().await;
}
