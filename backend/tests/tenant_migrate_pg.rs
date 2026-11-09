//! Integration test for the migrate_tenants orchestrator (org-tenancy PR 3.1).
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE DATABASE
//! rights). Builds a control DB + two tenant DBs and drives the orchestrator
//! through: applying the tenant tree to fresh tenants, recording schema_version,
//! fingerprint-mismatch failure isolation, and `--check` drift reporting.

#[path = "support/retained_database.rs"]
mod retained_database;

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coralswarm_backend::tenancy::migrate::{self, DdlLockPolicy, MigrateOpts, Mode, TenantResult};
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use sqlx::migrate::{Migration, MigrationType};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}
async fn recreate(conn: &mut PgConnection, db: &str) {
    retained_database::recreate(conn, db).await.unwrap();
}

async fn result_for<'a>(report: &'a migrate::MigrateReport, org: &str) -> &'a TenantResult {
    &report
        .outcomes
        .iter()
        .find(|o| o.clerk_org_id == org)
        .unwrap_or_else(|| panic!("no outcome for {org}"))
        .result
}

#[tokio::test]
async fn orchestrator_migrates_records_and_isolates_failures() {
    let Some(base) = base_url() else {
        eprintln!("skipping orchestrator test: set ORGT_TEST_BASE_URL to run");
        return;
    };
    let (ctl_db, ta, tb) = ("orgt_mig_ctl", "orgt_mig_a", "orgt_mig_b");
    let mut conn = admin(&base).await;
    for db in [ctl_db, ta, tb] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // Control DB: apply the control tree, seed an account for the FK.
    let control: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(&base, ctl_db))
        .await
        .expect("connect control");
    sqlx::migrate!("./migrations_control")
        .run(&control)
        .await
        .expect("control tree");
    async fn account(control: &PgPool, org: &str) -> uuid::Uuid {
        sqlx::query_scalar(
            "WITH u AS (INSERT INTO users(email) VALUES($1) RETURNING id) \
             INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, $2 FROM u RETURNING id",
        )
        .bind(format!("{org}@tenant-migrate.test"))
        .bind(org)
        .fetch_one(control).await.unwrap()
    }

    for (org, slug, secret) in [
        ("org_a", "tenant-a", "SECRET_A"),
        ("org_b", "tenant-b", "SECRET_B"),
    ] {
        let account_id = account(&control, org).await;
        financial_fixture::execute(
            &control,
            account_id,
            sqlx::query(
                "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
                 VALUES($1,$2,$3,$4,'active')",
            )
            .bind(org)
            .bind(account_id)
            .bind(slug)
            .bind(secret),
        )
        .await
        .expect("insert routing row");
    }

    let resolver = Arc::new(StaticMapResolver::from_pairs([
        ("SECRET_A", with_db(&base, ta)),
        ("SECRET_B", with_db(&base, tb)),
        // A distinct ref (UNIQUE) that ALSO points at tenant A's DB — the
        // swapped-DSN scenario for the Phase 2 fingerprint-mismatch check.
        ("SECRET_X", with_db(&base, ta)),
    ]));

    // Phase 1: migrate all. Both fresh tenants apply the tenant tree.
    let report = migrate::migrate_all(
        &control,
        resolver.clone(),
        &MigrateOpts {
            jobs: 2,
            ..Default::default()
        },
    )
    .await
    .expect("migrate_all");
    assert_eq!(report.outcomes.len(), 2);
    for org in ["org_a", "org_b"] {
        assert!(
            matches!(result_for(&report, org).await, TenantResult::Migrated { .. }),
            "{org} should have migrated: {:?}",
            result_for(&report, org).await
        );
    }
    // schema_version recorded on the control routing rows.
    let ver: Option<i64> =
        sqlx::query_scalar("SELECT schema_version FROM tenant_databases WHERE clerk_org_id='org_a'")
            .fetch_one(&control)
            .await
            .unwrap();
    assert!(ver.unwrap_or(0) >= 2, "schema_version should be set (>=2)");
    // tenant DB actually has the tenant schema.
    let a_pool = PgPoolOptions::new()
        .connect(&with_db(&base, ta))
        .await
        .unwrap();
    let has_graphs: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='graphs')",
    )
    .fetch_one(&a_pool)
    .await
    .unwrap();
    assert!(has_graphs, "tenant A should have the graphs table");

    // Phase 2: fingerprint tenant A as org_a, then add a MISLABELED routing row
    // (org_x pointing at tenant A). Migrating org_x must FAIL on the fingerprint
    // mismatch, while org_a still succeeds — failure isolation.
    sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_a')")
        .execute(&a_pool)
        .await
        .unwrap();
    let account_x = account(&control, "org_x").await;
    financial_fixture::execute(
        &control,
        account_x,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
         VALUES('org_x',$1,'tenant-x','SECRET_X','active')",
        )
        .bind(account_x),
    )
    .await
    .unwrap();
    // Target org_x specifically (deterministic — it is the only row touching
    // tenant A, so it takes the lock and reaches the fingerprint check).
    let report2 = migrate::migrate_all(
        &control,
        resolver.clone(),
        &MigrateOpts {
            only: Some("org_x".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(result_for(&report2, "org_x").await, TenantResult::Failed { .. }),
        "org_x must fail the fingerprint check: {:?}",
        result_for(&report2, "org_x").await
    );
    assert!(report2.any_failed());
    // Remove the mislabeled row so later all-tenant runs don't contend on tenant A.
    financial_fixture::execute(
        &control,
        account_x,
        sqlx::query("DELETE FROM tenant_databases WHERE clerk_org_id='org_x'"),
    )
    .await
    .unwrap();

    // Phase 3: failure isolation. A tenant whose secret ref does not resolve must
    // FAIL without aborting the run — the healthy tenants still process.
    let account_c = account(&control, "org_c").await;
    financial_fixture::execute(
        &control,
        account_c,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
         VALUES('org_c',$1,'tenant-c','SECRET_MISSING','active')",
        )
        .bind(account_c),
    )
    .await
    .unwrap();
    let report_iso = migrate::migrate_all(&control, resolver.clone(), &MigrateOpts::default())
        .await
        .unwrap();
    assert!(
        matches!(result_for(&report_iso, "org_c").await, TenantResult::Failed { .. }),
        "org_c (unresolvable secret) must fail"
    );
    assert!(
        !matches!(result_for(&report_iso, "org_b").await, TenantResult::Failed { .. }),
        "org_b must remain healthy despite org_c's failure"
    );
    assert!(report_iso.any_failed());
    financial_fixture::execute(
        &control,
        account_c,
        sqlx::query("DELETE FROM tenant_databases WHERE clerk_org_id='org_c'"),
    )
    .await
    .unwrap();

    // Phase 4: --check for org_a is up-to-date.
    let report3 = migrate::migrate_all(
        &control,
        resolver.clone(),
        &MigrateOpts {
            mode: Mode::Check,
            only: Some("org_a".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(result_for(&report3, "org_a").await, TenantResult::UpToDate { .. }),
        "org_a should be up-to-date under --check: {:?}",
        result_for(&report3, "org_a").await
    );
    assert!(!report3.any_failed());
    assert!(!report3.any_skipped());

    // Phase 5: advisory-lock contention — the case the deploy path cares about.
    // A leftover manual `migrate_tenants` (or a second release machine) already
    // holds tenant B's lock. The orchestrator must SKIP that tenant rather than
    // migrate it concurrently, and the skip must be reportable as distinct from
    // both success and failure: `any_skipped()` true, `any_failed()` false. The
    // binary turns that into exit 3 (see bin/migrate_tenants.rs's exit table),
    // so a deploy cannot bank a green release it did not earn.
    const ADVISORY_LOCK_KEY: i64 = 0x636F_7261_6C6D_6967; // "coralmig"
    let mut holder = PgConnection::connect(&with_db(&base, tb))
        .await
        .expect("connect lock holder");
    let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .fetch_one(&mut holder)
        .await
        .unwrap();
    assert!(held, "test fixture must acquire tenant B's advisory lock");

    let report4 = migrate::migrate_all(
        &control,
        resolver.clone(),
        &MigrateOpts {
            only: Some("org_b".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(result_for(&report4, "org_b").await, TenantResult::Skipped { .. }),
        "org_b must be skipped while another runner holds its lock: {:?}",
        result_for(&report4, "org_b").await
    );
    assert!(report4.any_skipped(), "a skip must be visible to the caller");
    assert!(
        !report4.any_failed(),
        "lock contention is not a hard failure — the two must stay distinguishable"
    );
    let c4 = report4.counts();
    assert_eq!(
        (c4.total(), c4.skipped, c4.migrated, c4.failed),
        (1, 1, 0, 0),
        "tally must attribute the outcome to `skipped`: {c4:?}"
    );

    // Releasing the lock lets the very same run succeed — i.e. the remedy for
    // exit 3 really is "re-run once the other orchestrator finishes", and the
    // re-run is idempotent (tenant B is already at head, so UpToDate).
    holder.close().await.ok();
    let report5 = migrate::migrate_all(
        &control,
        resolver.clone(),
        &MigrateOpts {
            mode: Mode::Check,
            only: Some("org_b".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(result_for(&report5, "org_b").await, TenantResult::UpToDate { .. }),
        "org_b must be up-to-date once the lock is released: {:?}",
        result_for(&report5, "org_b").await
    );
    assert!(!report5.any_skipped() && !report5.any_failed());

    // Phase 6: a normal run must leave the advisory lock FREE.
    //
    // Phase 5 proves the migrator yields to a lock someone else holds. This
    // proves the migrator does not become that someone else.
    //
    // HONEST SCOPE — this is a POSTCONDITION assertion, not a reproduction of
    // the bug that motivated it. That bug was: `pg_try_advisory_lock` is
    // session-scoped, and the lock, the migration and the unlock were three
    // separate calls against a 2-connection `PgPool` with nothing pinning them
    // to one connection, so the unlock could land on the other connection
    // where it is a no-op returning false — silently, because the result was
    // discarded with `let _ =`. Whether that happens depends on which
    // connection the pool hands out, and with sequential queries it reuses the
    // same one; this test therefore PASSES against the unfixed code in a quiet
    // environment (verified). It would catch the leak only when scheduling
    // happens to split the pair.
    //
    // It is kept anyway because the postcondition is the thing that actually
    // matters operationally — a leaked lock makes the NEXT deploy skip this
    // tenant and exit 3 — and because a future refactor that reintroduces
    // pool-based unlocking has at least one place that can notice.
    //
    // Probed from a FRESH connection: a session can always re-take a lock it
    // already holds, so checking on the migrator's own connection would pass
    // regardless.
    let report6 = migrate::migrate_all(
        &control,
        resolver.clone(),
        &MigrateOpts {
            only: Some("org_b".to_string()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        !report6.any_failed() && !report6.any_skipped(),
        "phase 6 precondition: the run itself must be clean: {:?}",
        report6.counts()
    );

    let mut after = PgConnection::connect(&with_db(&base, tb))
        .await
        .expect("connect post-run lock prober");
    let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .fetch_one(&mut after)
        .await
        .unwrap();
    assert!(
        free,
        "the migrator leaked tenant B's advisory lock: a clean run must leave it \
         free, or the NEXT deploy skips this tenant and aborts on exit 3"
    );
    after.close().await.ok();

    a_pool.close().await;
    control.close().await;
    for db in [ctl_db, ta, tb] {
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }
}

/// True while some session is queued (not granted) for a lock on `table`.
async fn lock_queued(conn: &mut PgConnection, table: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation=to_regclass($1) AND NOT granted)")
        .bind(table)
        .fetch_one(conn)
        .await
        .unwrap()
}

/// Migration DDL on a hot table (`CREATE TRIGGER` on `source_items`, the lock
/// class the listing migration takes nine times) must never queue behind a
/// long write transaction and take every later writer down with it.
///
/// Phase 1, the synthetic tree: with one ordinary write transaction holding
/// `source_items`, the migrator queues for its lock, a plain writer queues
/// behind the migrator, and WITHOUT `lock_timeout` that writer waits for the
/// whole long transaction. Under the policy the migrator gives the lock up at
/// `lock_timeout`, the writer proceeds, the run retries, and past its budget
/// the run fails with the lock error rather than blocking anyone. A later run
/// is idempotent. Phase 2 drives the real `migrate_all` path through the
/// same policy, independent of which migration is head.
#[tokio::test]
async fn ddl_lock_waits_are_bounded_retried_and_never_block_writers() {
    let Some(base) = base_url() else {
        eprintln!("skipping lock-timeout test: set ORGT_TEST_BASE_URL to run");
        return;
    };
    let (ctl_db, ta, tb) = ("orgt_lock_ctl", "orgt_lock_a", "orgt_lock_b");
    let mut conn = admin(&base).await;
    for db in [ctl_db, ta, tb] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Phase 1: a synthetic tree over a migrated tenant, through apply_tree ──
    let a_url = with_db(&base, ta);
    {
        let mut c = PgConnection::connect(&a_url).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&mut c).await.unwrap();
        c.close().await.ok();
    }
    let probe_sql = "CREATE FUNCTION orgt_lock_probe() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; \
        CREATE TRIGGER orgt_lock_probe AFTER INSERT ON source_items FOR EACH STATEMENT EXECUTE FUNCTION orgt_lock_probe();";
    // A version no real tree will reach; the row it leaves is confined to this
    // disposable database.
    let probe = Migration::new(9_000_000_000, Cow::Borrowed("orgt lock probe"), MigrationType::Simple, Cow::Borrowed(probe_sql), false);
    let mut tree = sqlx::migrate!("./migrations_tenant");
    tree.migrations = Cow::Owned(vec![probe]);
    tree.set_ignore_missing(true);
    let tree = Arc::new(tree);
    let policy = DdlLockPolicy {
        lock_timeout: Duration::from_millis(400),
        attempts: 3,
        retry_backoff: Duration::from_millis(300),
    };
    async fn probe_installed(url: &str) -> bool {
        let mut c = PgConnection::connect(url).await.unwrap();
        let installed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='orgt_lock_probe')")
            .fetch_one(&mut c).await.unwrap();
        c.close().await.ok();
        installed
    }

    // An ordinary long write transaction: ROW EXCLUSIVE on source_items, as
    // any INSERT/UPDATE holds until it commits.
    let mut holder = PgConnection::connect(&a_url).await.unwrap();
    holder.execute("BEGIN; LOCK TABLE source_items IN ROW EXCLUSIVE MODE").await.unwrap();
    let migrator = {
        let (url, tree) = (a_url.clone(), tree.clone());
        tokio::spawn(async move {
            let mut c = PgConnection::connect(&url).await.unwrap();
            let result = migrate::apply_tree(&tree, &mut c, policy, "orgt_lock_a").await;
            c.close().await.ok();
            result
        })
    };
    // Wait until the migrator is actually queued for the table lock.
    let mut prober = PgConnection::connect(&a_url).await.unwrap();
    let queued_at = Instant::now();
    while !lock_queued(&mut prober, "source_items").await {
        assert!(queued_at.elapsed() < Duration::from_secs(10), "migrator never queued for source_items");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // A plain writer arriving behind the queued DDL. Its wait is the whole
    // point: bounded by lock_timeout, not by the holder's transaction.
    let mut writer = PgConnection::connect(&a_url).await.unwrap();
    let started = Instant::now();
    writer.execute("UPDATE source_items SET source_id=source_id WHERE false").await.unwrap();
    let waited = started.elapsed();
    assert!(
        waited < Duration::from_millis(1500),
        "a writer queued behind the migration waited {waited:?}; the migration must give up its lock wait at lock_timeout (400ms)"
    );
    // Hold on past the whole retry budget (3 x (400 + 300) ms): the run must
    // report the lock error, never wait for this transaction.
    let budget = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(20), migrator).await.expect("migrator must not block on the holder").unwrap();
    let error = outcome.expect_err("every attempt waited lock_timeout; the run must fail, not block");
    assert!(
        error.to_string().contains("lock timeout"),
        "the failure must name the lock wait: {error}"
    );
    assert!(budget.elapsed() < Duration::from_secs(10), "the retry budget bounds the run");
    assert!(!probe_installed(&a_url).await, "a given-up migration leaves nothing behind");
    // The next writer is still unblocked.
    let started = Instant::now();
    writer.execute("UPDATE source_items SET source_id=source_id WHERE false").await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(1500));

    // Retry success: the holder finishes during the retry budget.
    let migrator = {
        let (url, tree) = (a_url.clone(), tree.clone());
        tokio::spawn(async move {
            let mut c = PgConnection::connect(&url).await.unwrap();
            let result = migrate::apply_tree(&tree, &mut c, policy, "orgt_lock_a").await;
            c.close().await.ok();
            result
        })
    };
    let queued_at = Instant::now();
    while !lock_queued(&mut prober, "source_items").await {
        assert!(queued_at.elapsed() < Duration::from_secs(10), "migrator never queued for source_items");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Past the first attempt, inside the budget.
    tokio::time::sleep(Duration::from_millis(500)).await;
    holder.execute("COMMIT").await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(20), migrator).await.unwrap().unwrap();
    outcome.expect("a retry after the holder commits applies the migration");
    assert!(probe_installed(&a_url).await, "the retried migration is applied");
    // Idempotent: nothing pending, nothing re-applied.
    let mut c = PgConnection::connect(&a_url).await.unwrap();
    migrate::apply_tree(&tree, &mut c, policy, "orgt_lock_a").await.expect("a clean re-run is a no-op");
    let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE version=9000000000")
        .fetch_one(&mut c).await.unwrap();
    assert_eq!(applied, 1);
    c.close().await.ok();
    writer.close().await.ok();
    prober.close().await.ok();
    holder.close().await.ok();

    // ── Phase 2: the real orchestrator path carries the policy ──
    let control: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(&base, ctl_db))
        .await
        .expect("connect control");
    sqlx::migrate!("./migrations_control").run(&control).await.expect("control tree");
    let account_id: uuid::Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(email) VALUES('org_lock@tenant-migrate.test') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, 'org_lock' FROM u RETURNING id",
    )
    .fetch_one(&control).await.unwrap();
    financial_fixture::execute(
        &control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
             VALUES('org_lock',$1,'tenant-lock','SECRET_LOCK','active')",
        )
        .bind(account_id),
    )
    .await
    .unwrap();
    let resolver = Arc::new(StaticMapResolver::from_pairs([("SECRET_LOCK", with_db(&base, tb))]));
    let report = migrate::migrate_all(&control, resolver.clone(), &MigrateOpts::default()).await.unwrap();
    assert!(matches!(result_for(&report, "org_lock").await, TenantResult::Migrated { .. }), "{:?}", result_for(&report, "org_lock").await);

    // Every run reads the ledger, so an exclusive lock on it contends with
    // any tree whatever its head. One attempt, no retry: the lock error is
    // the tenant's failure, reported, not a blocked deploy.
    let b_url = with_db(&base, tb);
    let mut holder = PgConnection::connect(&b_url).await.unwrap();
    holder.execute("BEGIN; LOCK TABLE _sqlx_migrations IN ACCESS EXCLUSIVE MODE").await.unwrap();
    let tight = MigrateOpts {
        only: Some("org_lock".to_string()),
        ddl_lock: DdlLockPolicy { lock_timeout: Duration::from_millis(300), attempts: 1, retry_backoff: Duration::from_millis(100) },
        ..Default::default()
    };
    let started = Instant::now();
    let report = tokio::time::timeout(Duration::from_secs(20), migrate::migrate_all(&control, resolver.clone(), &tight)).await.unwrap().unwrap();
    match result_for(&report, "org_lock").await {
        TenantResult::Failed { error } => assert!(error.contains("lock timeout"), "{error}"),
        other => panic!("a lock wait past the budget must fail the tenant: {other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(report.any_failed());

    // With retries, the run outlives the holder and succeeds.
    let patient = MigrateOpts {
        only: Some("org_lock".to_string()),
        ddl_lock: DdlLockPolicy { lock_timeout: Duration::from_millis(300), attempts: 4, retry_backoff: Duration::from_millis(300) },
        ..Default::default()
    };
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(700)).await;
        holder.execute("COMMIT").await.unwrap();
        holder.close().await.ok();
    });
    let report = tokio::time::timeout(Duration::from_secs(30), migrate::migrate_all(&control, resolver.clone(), &patient)).await.unwrap().unwrap();
    release.await.unwrap();
    assert!(matches!(result_for(&report, "org_lock").await, TenantResult::Migrated { .. }), "a retry after the holder commits must succeed: {:?}", result_for(&report, "org_lock").await);
    let report = migrate::migrate_all(&control, resolver, &MigrateOpts { mode: Mode::Check, only: Some("org_lock".to_string()), ..Default::default() }).await.unwrap();
    assert!(matches!(result_for(&report, "org_lock").await, TenantResult::UpToDate { .. }), "{:?}", result_for(&report, "org_lock").await);

    control.close().await;
    for db in [ctl_db, ta, tb] {
        retained_database::finish(&mut conn, db, true).await.ok();
    }
}
