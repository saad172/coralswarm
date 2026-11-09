//! Integration tests for the multi-tenant worker harness (org-tenancy PR 2.1):
//!
//! 1. Jobs enqueued in tenant A and tenant B are each processed on their OWN
//!    pool (and the consumer plane keeps processing too); killing tenant B's
//!    database mid-run leaves A (and the consumer) unaffected — per-tenant
//!    failure isolation.
//! 2. The schema-version gate: a tenant whose routing row's `schema_version`
//!    lags the binary's embedded tenant tree is never ticked — its planted job
//!    stays untouched (no queries run against it).
//! 3. Flag off (`enabled == false`): the tenant list is `[consumer]` — org
//!    jobs are not touched even when routing rows exist.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights). Follows the same scratch-DB pattern as
//! `tenant_registry_pg.rs` / `binding_gate_pg.rs` / `dek_relocation_pg.rs`.

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use coralswarm_backend::shutdown::Shutdown;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::migrate;
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::tenant_harness::{
    run_multi_tenant, HarnessConfig, TenantActivity, Tick, WorkerTenant,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

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

async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(base, db))
        .await
        .expect("connect control");
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

/// Tenant DB: tenant tree + fingerprint.
async fn tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(base, db))
        .await
        .expect("connect tenant");
    sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&pool)
        .await
        .unwrap();
    pool
}

/// Mint an org's control-plane account row (the routing table FKs it).
async fn mint_account(control: &PgPool, org_id: &str) -> Uuid {
    sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(), $1 || '@wh.test') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, $1 FROM u RETURNING id",
    )
    .bind(org_id)
    .fetch_one(control)
    .await
    .unwrap()
}

async fn insert_routing_row(
    control: &PgPool,
    org_id: &str,
    account_id: Uuid,
    slug: &str,
    secret_ref: &str,
    schema_version: i64,
) {
    financial_fixture::execute(
        control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases \
         (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
         VALUES ($1, $2, $3, $4, 'neon', $5, 'active')",
        )
        .bind(org_id)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref)
        .bind(schema_version),
    )
    .await
    .unwrap();
}

async fn enqueue_test_jobs(pool: &PgPool, n: i64) {
    sqlx::query(
        "INSERT INTO jobs (kind, payload) \
         SELECT 'HarnessTest', '{}'::jsonb FROM generate_series(1, $1)",
    )
    .bind(n)
    .execute(pool)
    .await
    .unwrap();
}

async fn pending_test_jobs(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'HarnessTest' AND status = 'pending'")
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The test worker tick: claim up to 2 pending `HarnessTest` jobs on THIS
/// tenant's pool, mark them done, and record which tenant they ran under.
fn recording_tick(
    seen: Arc<Mutex<Vec<TenantId>>>,
) -> impl Fn(WorkerTenant) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Tick, String>> + Send>>
       + Send
       + Sync
       + 'static {
    move |t: WorkerTenant| {
        let seen = seen.clone();
        Box::pin(async move {
            let done: Vec<Uuid> = sqlx::query_scalar(
                "WITH c AS ( \
                   SELECT id FROM jobs \
                   WHERE status = 'pending' AND kind = 'HarnessTest' \
                   ORDER BY id LIMIT 2 \
                   FOR UPDATE SKIP LOCKED \
                 ) \
                 UPDATE jobs j SET status = 'done', finished_at = now() \
                 FROM c WHERE j.id = c.id RETURNING j.id",
            )
            .fetch_all(&t.pool)
            .await
            .map_err(|e| format!("claim failed: {e}"))?;

            if done.is_empty() {
                return Ok(Tick::idle());
            }
            let mut guard = seen.lock().unwrap();
            for _ in &done {
                guard.push(t.tenant.clone());
            }
            Ok(Tick::worked(done.len()))
        })
    }
}

fn fast_cfg(name: &'static str) -> HarnessConfig {
    HarnessConfig {
        name,
        base_poll: Duration::from_millis(20),
        error_sleep: Duration::from_millis(50),
        hot_loop_on_work: true,
        wake_on_enqueue: true,
        max_org_idle_backoff: Duration::from_millis(200),
        tenant_refresh: Duration::from_millis(100),
        pool_build_timeout: Duration::from_secs(5),
        include_revoked_orgs: false,
    }
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
async fn jobs_process_per_tenant_and_poisoned_tenant_is_isolated() {
    let Some(base) = base_url() else {
        eprintln!("skipping jobs_process_per_tenant_and_poisoned_tenant_is_isolated: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, db_a, db_b) = ("orgt_wh1_ctl", "orgt_wh1_a", "orgt_wh1_b");
    let mut conn = admin(&base).await;
    for db in [ctl, db_a, db_b] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    let control = control_pool(&base, ctl).await;
    let pool_a = tenant_pool(&base, db_a, "org_wh1_a").await;
    let pool_b = tenant_pool(&base, db_b, "org_wh1_b").await;

    let head = migrate::head_version();
    let acct_a = mint_account(&control, "org_wh1_a").await;
    let acct_b = mint_account(&control, "org_wh1_b").await;
    insert_routing_row(&control, "org_wh1_a", acct_a, "wh1-org-a", "WH1_SECRET_A", head).await;
    insert_routing_row(&control, "org_wh1_b", acct_b, "wh1-org-b", "WH1_SECRET_B", head).await;

    // Jobs on every plane before the harness starts.
    enqueue_test_jobs(&control, 2).await;
    enqueue_test_jobs(&pool_a, 3).await;
    enqueue_test_jobs(&pool_b, 3).await;

    let resolver = StaticMapResolver::from_pairs([
        ("WH1_SECRET_A", with_db(&base, db_a)),
        ("WH1_SECRET_B", with_db(&base, db_b)),
    ]);
    let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
    let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
    let seen: Arc<Mutex<Vec<TenantId>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = Shutdown::new();

    let consumer_store: Arc<dyn Store> = Arc::new(PostgresStore::new(control.clone()));
    let handle = tokio::spawn(run_multi_tenant(
        control.clone(),
        consumer_store,
        Some(runtime),
        TenantActivity::new(),
        fast_cfg("wh1"),
        shutdown.clone(),
        recording_tick(seen.clone()),
    ));

    // Every plane's jobs drain — each on its own pool (the tick only ever
    // sees the pool the harness handed it, so a completed job in DB X proves
    // it was processed on X's pool).
    wait_until("all planes drained", Duration::from_secs(20), || {
        let (c, a, b) = (control.clone(), pool_a.clone(), pool_b.clone());
        async move {
            pending_test_jobs(&c).await == 0
                && pending_test_jobs(&a).await == 0
                && pending_test_jobs(&b).await == 0
        }
    })
    .await;
    {
        let guard = seen.lock().unwrap();
        assert!(guard.contains(&TenantId::Consumer), "consumer plane processed");
        assert!(guard.contains(&TenantId::Org("org_wh1_a".into())), "tenant A processed");
        assert!(guard.contains(&TenantId::Org("org_wh1_b".into())), "tenant B processed");
    }

    // ── Poison tenant B mid-run: hard-drop its database ──────────────────────
    pool_b.close().await;
    conn.execute(format!("DROP DATABASE {db_b} WITH (FORCE)").as_str())
        .await
        .expect("force-drop tenant B");

    // A (and the consumer) must keep processing while B's ticks fail.
    enqueue_test_jobs(&pool_a, 2).await;
    enqueue_test_jobs(&control, 1).await;
    wait_until("A + consumer drain after B poisoned", Duration::from_secs(20), || {
        let (c, a) = (control.clone(), pool_a.clone());
        async move { pending_test_jobs(&c).await == 0 && pending_test_jobs(&a).await == 0 }
    })
    .await;

    // Clean shutdown: the harness task returns.
    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("harness stops on shutdown")
        .expect("harness task did not panic");

    for db in [db_a, ctl] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[tokio::test]
async fn schema_gate_skips_lagging_tenant_without_any_queries() {
    let Some(base) = base_url() else {
        eprintln!("skipping schema_gate_skips_lagging_tenant_without_any_queries: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, db_ok, db_lag) = ("orgt_wh2_ctl", "orgt_wh2_ok", "orgt_wh2_lag");
    let mut conn = admin(&base).await;
    for db in [ctl, db_ok, db_lag] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    let control = control_pool(&base, ctl).await;
    let pool_ok = tenant_pool(&base, db_ok, "org_wh2_ok").await;
    let pool_lag = tenant_pool(&base, db_lag, "org_wh2_lag").await;

    let head = migrate::head_version();
    let acct_ok = mint_account(&control, "org_wh2_ok").await;
    let acct_lag = mint_account(&control, "org_wh2_lag").await;
    insert_routing_row(&control, "org_wh2_ok", acct_ok, "wh2-org-ok", "WH2_SECRET_OK", head).await;
    // The lagging tenant's recorded schema_version is BEHIND the binary's tree.
    insert_routing_row(&control, "org_wh2_lag", acct_lag, "wh2-org-lag", "WH2_SECRET_LAG", head - 1)
        .await;

    enqueue_test_jobs(&pool_ok, 2).await;
    enqueue_test_jobs(&pool_lag, 2).await; // must stay untouched

    let resolver = StaticMapResolver::from_pairs([
        ("WH2_SECRET_OK", with_db(&base, db_ok)),
        ("WH2_SECRET_LAG", with_db(&base, db_lag)),
    ]);
    let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
    let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
    let seen: Arc<Mutex<Vec<TenantId>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = Shutdown::new();

    let consumer_store: Arc<dyn Store> = Arc::new(PostgresStore::new(control.clone()));
    let handle = tokio::spawn(run_multi_tenant(
        control.clone(),
        consumer_store,
        Some(runtime),
        TenantActivity::new(),
        fast_cfg("wh2"),
        shutdown.clone(),
        recording_tick(seen.clone()),
    ));

    // The healthy tenant drains…
    wait_until("healthy tenant drains", Duration::from_secs(20), || {
        let ok = pool_ok.clone();
        async move { pending_test_jobs(&ok).await == 0 }
    })
    .await;
    // …then give the harness a few more refresh cycles to (incorrectly) pick
    // up the lagging tenant if the gate were broken.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        pending_test_jobs(&pool_lag).await,
        2,
        "lagging-schema tenant's jobs must be untouched (no queries run against it)"
    );
    {
        let guard = seen.lock().unwrap();
        assert!(guard.contains(&TenantId::Org("org_wh2_ok".into())));
        assert!(
            !guard.iter().any(|t| *t == TenantId::Org("org_wh2_lag".into())),
            "lagging tenant must never be ticked"
        );
    }

    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("harness stops on shutdown")
        .expect("harness task did not panic");

    for db in [db_ok, db_lag, ctl] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// P1 regression test: a tenant DB that HANGS (SYN blackhole via a
/// non-routable address) instead of refusing must not head-of-line-block the
/// consumer plane. Each stalled build is bounded by `pool_build_timeout`
/// (1s here), so consumer jobs still drain promptly while the tenant hangs.
#[tokio::test]
async fn hanging_tenant_db_does_not_starve_the_consumer_plane() {
    let Some(base) = base_url() else {
        eprintln!("skipping hanging_tenant_db_does_not_starve_the_consumer_plane: set ORGT_TEST_BASE_URL");
        return;
    };

    let ctl = "orgt_wh4_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control = control_pool(&base, ctl).await;

    let head = migrate::head_version();
    let acct = mint_account(&control, "org_wh4_hang").await;
    // Routing row is active + schema-current, but its secret resolves to a
    // non-routable address (TEST-NET-ish blackhole): connects hang, not fail.
    insert_routing_row(&control, "org_wh4_hang", acct, "wh4-org-hang", "WH4_SECRET_HANG", head)
        .await;

    enqueue_test_jobs(&control, 4).await;

    let resolver = StaticMapResolver::from_pairs([(
        "WH4_SECRET_HANG",
        "postgres://cs:cs@10.255.255.1:5432/hang".to_string(),
    )]);
    let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
    let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
    let seen: Arc<Mutex<Vec<TenantId>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = Shutdown::new();

    let mut cfg = fast_cfg("wh4");
    cfg.pool_build_timeout = Duration::from_secs(1);
    // Let the hung tenant's error backoff escalate to seconds quickly so the
    // scheduler isn't dominated by 1s stalls for the whole test.
    cfg.max_org_idle_backoff = Duration::from_secs(5);

    let consumer_store: Arc<dyn Store> = Arc::new(PostgresStore::new(control.clone()));
    let started = Instant::now();
    let handle = tokio::spawn(run_multi_tenant(
        control.clone(),
        consumer_store,
        Some(runtime),
        TenantActivity::new(),
        cfg,
        shutdown.clone(),
        recording_tick(seen.clone()),
    ));

    // Consumer drain latency stays bounded while the tenant hangs: even in
    // the worst interleaving (a 1s tenant stall between every consumer tick),
    // 4 jobs at 2/tick need 2 ticks — well under this deadline. Without the
    // build timeout each stall would be sqlx-default 30s and this would blow
    // past it.
    wait_until("consumer drains despite hung tenant", Duration::from_secs(15), || {
        let c = control.clone();
        async move { pending_test_jobs(&c).await == 0 }
    })
    .await;
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(15),
        "consumer drain took {elapsed:?} — hung tenant is head-of-line-blocking the scheduler"
    );
    assert!(
        seen.lock().unwrap().iter().all(|t| *t == TenantId::Consumer),
        "the hung tenant must never produce a successful tick"
    );

    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("harness stops on shutdown (bounded even mid-hang)")
        .expect("harness task did not panic");

    conn.execute(format!("DROP DATABASE IF EXISTS {ctl} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// A tick that PANICS for one tenant is isolated: the harness survives, the
/// consumer plane keeps draining, and the panicking tenant's jobs are simply
/// left alone.
#[tokio::test]
async fn panicking_tenant_tick_is_isolated() {
    let Some(base) = base_url() else {
        eprintln!("skipping panicking_tenant_tick_is_isolated: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, db_p) = ("orgt_wh5_ctl", "orgt_wh5_p");
    let mut conn = admin(&base).await;
    for db in [ctl, db_p] {
        let _database_cleanup_3 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let pool_p = tenant_pool(&base, db_p, "org_wh5_p").await;

    let head = migrate::head_version();
    let acct = mint_account(&control, "org_wh5_p").await;
    insert_routing_row(&control, "org_wh5_p", acct, "wh5-org-p", "WH5_SECRET_P", head).await;

    enqueue_test_jobs(&control, 3).await;
    enqueue_test_jobs(&pool_p, 2).await;

    let resolver = StaticMapResolver::from_pairs([("WH5_SECRET_P", with_db(&base, db_p))]);
    let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
    let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
    let seen: Arc<Mutex<Vec<TenantId>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = Shutdown::new();

    let consumer_store: Arc<dyn Store> = Arc::new(PostgresStore::new(control.clone()));
    let base_tick = recording_tick(seen.clone());
    let handle = tokio::spawn(run_multi_tenant(
        control.clone(),
        consumer_store,
        Some(runtime),
        TenantActivity::new(),
        fast_cfg("wh5"),
        shutdown.clone(),
        move |t: WorkerTenant| {
            if matches!(&t.tenant, TenantId::Org(org) if org == "org_wh5_p") {
                panic!("tick panic for org_wh5_p (must be isolated)");
            }
            base_tick(t)
        },
    ));

    wait_until("consumer drains despite panicking tenant", Duration::from_secs(20), || {
        let c = control.clone();
        async move { pending_test_jobs(&c).await == 0 }
    })
    .await;
    // Give the harness a few more passes to prove it keeps running (and keeps
    // catching the panics) rather than having died on the first one.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!handle.is_finished(), "harness must survive tick panics");
    assert_eq!(pending_test_jobs(&pool_p).await, 2, "panicking tenant's jobs untouched");
    assert!(seen.lock().unwrap().iter().all(|t| *t == TenantId::Consumer));

    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("harness stops on shutdown")
        .expect("harness task did not panic");

    for db in [db_p, ctl] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[tokio::test]
async fn flag_off_means_consumer_only() {
    let Some(base) = base_url() else {
        eprintln!("skipping flag_off_means_consumer_only: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, db_t) = ("orgt_wh3_ctl", "orgt_wh3_t");
    let mut conn = admin(&base).await;
    for db in [ctl, db_t] {
        let _database_cleanup_4 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    let control = control_pool(&base, ctl).await;
    let pool_t = tenant_pool(&base, db_t, "org_wh3_t").await;

    let head = migrate::head_version();
    let acct = mint_account(&control, "org_wh3_t").await;
    insert_routing_row(&control, "org_wh3_t", acct, "wh3-org-t", "WH3_SECRET_T", head).await;

    enqueue_test_jobs(&control, 2).await;
    enqueue_test_jobs(&pool_t, 2).await; // must stay untouched with the flag off

    let resolver = StaticMapResolver::from_pairs([("WH3_SECRET_T", with_db(&base, db_t))]);
    let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
    // enabled == false → the harness must never consult tenant_databases.
    let runtime = TenantRuntime::new(false, "coralswarm.test", registry);
    let seen: Arc<Mutex<Vec<TenantId>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = Shutdown::new();

    let consumer_store: Arc<dyn Store> = Arc::new(PostgresStore::new(control.clone()));
    let handle = tokio::spawn(run_multi_tenant(
        control.clone(),
        consumer_store,
        Some(runtime),
        TenantActivity::new(),
        fast_cfg("wh3"),
        shutdown.clone(),
        recording_tick(seen.clone()),
    ));

    wait_until("consumer drains", Duration::from_secs(20), || {
        let c = control.clone();
        async move { pending_test_jobs(&c).await == 0 }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        pending_test_jobs(&pool_t).await,
        2,
        "flag off: tenant jobs must be untouched"
    );
    {
        let guard = seen.lock().unwrap();
        assert!(guard.iter().all(|t| *t == TenantId::Consumer), "only the consumer plane ticked");
    }

    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("harness stops on shutdown")
        .expect("harness task did not panic");

    for db in [db_t, ctl] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
