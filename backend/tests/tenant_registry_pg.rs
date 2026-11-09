//! Integration tests for the tenant pool registry: the fingerprint gate — the
//! swapped-DSN / two-orgs-one-DB defense (org-tenancy PR 1.1) — and the org
//! commons-ocean self-heal (org-tenancy OPS-4 follow-up:
//! `TenantRegistry::ensure_org_ocean`, called from `build()`).
//!
//! Gated on `ORGT_TEST_BASE_URL`: a base Postgres connection with CREATE DATABASE
//! rights (e.g. `postgres://postgres:postgres@localhost:5432/postgres`). Skips
//! cleanly when unset. It creates its own throwaway tenant DBs, applies the
//! tenant migration tree, and seeds each `tenant_meta.org_id`.
//!
//! Wired into CI (`.github/workflows/ci.yml`'s `tenant-registry` job, which
//! sets `ORGT_TEST_BASE_URL` and runs `cargo test --test tenant_registry_pg`
//! on every backend-touching PR and on `workflow_dispatch`) — unlike several
//! sibling `*_pg.rs` suites elsewhere in this tree (e.g. `route_migration_org_pg.rs`,
//! `org_creation_gate_pg.rs`) that have no CI job wired for them at all today
//! and only ever run via a developer's local `cargo test --test <name>` with
//! `ORGT_TEST_BASE_URL` set.

use std::sync::Arc;

use coralswarm_backend::tenancy::registry::{TenantPoolError, TenantRegistry};
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

/// Swap the database name on a base URL.
fn with_db(base: &str, db: &str) -> String {
    // base like scheme://user[:pw]@host:port/<db>[?params]
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}

async fn recreate_db(conn: &mut PgConnection, db: &str) {
    // CREATE/DROP DATABASE cannot run in a transaction; PgConnection autocommits.
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .expect("drop db");
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .expect("create db");
}

async fn apply_tenant_tree_and_fingerprint(base: &str, db: &str, org_id: Option<&str>) {
    let url = with_db(base, db);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect scratch");
    sqlx::migrate!("./migrations_tenant")
        .run(&pool)
        .await
        .expect("apply tenant tree");
    if let Some(org) = org_id {
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org)
            .execute(&pool)
            .await
            .expect("seed tenant_meta");
    }
    pool.close().await;
}

/// Every `kind = 'org'` row in `graphs` for `account_id`, as `(name, is_primary)`.
async fn org_oceans(pool: &PgPool, account_id: Uuid) -> Vec<(String, bool)> {
    sqlx::query_as(
        "SELECT name, is_primary FROM graphs WHERE account_id = $1 AND kind = 'org' \
         ORDER BY created_at",
    )
    .bind(account_id)
    .fetch_all(pool)
    .await
    .expect("query org oceans")
}

#[tokio::test]
async fn registry_fingerprint_gate() {
    let Some(base) = base_url() else {
        eprintln!("skipping registry_fingerprint_gate: set ORGT_TEST_BASE_URL to run");
        return;
    };

    let (db_a, db_b, db_c) = ("orgt_reg_a", "orgt_reg_b", "orgt_reg_c");
    let mut conn = admin(&base).await;
    for db in [db_a, db_b, db_c] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate_db(&mut conn, db).await;
    }
    apply_tenant_tree_and_fingerprint(&base, db_a, Some("org_a")).await;
    apply_tenant_tree_and_fingerprint(&base, db_b, Some("org_b")).await;
    // db_c: tenant tree applied but NO tenant_meta row (missing fingerprint).
    apply_tenant_tree_and_fingerprint(&base, db_c, None).await;

    let resolver = StaticMapResolver::from_pairs([
        ("SECRET_A", with_db(&base, db_a)),
        ("SECRET_B", with_db(&base, db_b)),
        ("SECRET_C", with_db(&base, db_c)),
    ]);
    let reg = TenantRegistry::new(Arc::new(resolver));
    let account_a = Uuid::new_v4();
    let account_b = Uuid::new_v4();

    // Correct mapping: builds, verifies, publishes.
    let pool_a = reg
        .get_or_build("org_a", account_a, "SECRET_A")
        .await
        .expect("org_a builds");
    let seen: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta")
        .fetch_one(&pool_a)
        .await
        .unwrap();
    assert_eq!(seen, "org_a");
    assert_eq!(reg.live_pool_count(), 1);

    // Second org: independent pool.
    reg.get_or_build("org_b", account_b, "SECRET_B")
        .await
        .expect("org_b builds");
    assert_eq!(reg.live_pool_count(), 2);

    // Idempotent: same org+secret returns the cached pool, no new entry.
    reg.get_or_build("org_a", account_a, "SECRET_A")
        .await
        .expect("org_a cached");
    assert_eq!(reg.live_pool_count(), 2);

    // SWAPPED DSN: routed as org_a but the secret points at org_b's DB. The
    // fingerprint gate MUST reject and MUST NOT cache a pool.
    let reg2 = TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_SWAP",
        with_db(&base, db_b),
    )])));
    match reg2.get_or_build("org_a", account_a, "SECRET_SWAP").await {
        Err(TenantPoolError::FingerprintMismatch { expected, found }) => {
            assert_eq!(expected, "org_a");
            assert_eq!(found, "org_b");
        }
        other => panic!("expected FingerprintMismatch, got {other:?}"),
    }
    assert_eq!(reg2.live_pool_count(), 0, "failed build must not be cached");

    // Missing fingerprint row: also rejects.
    match reg2
        .get_or_build("org_c", Uuid::new_v4(), "SECRET_C_MISSING")
        .await
    {
        Err(TenantPoolError::Secret(_)) => {} // unknown secret ref -> resolver error
        other => panic!("expected Secret error for unknown ref, got {other:?}"),
    }
    let reg3 = TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_C",
        with_db(&base, db_c),
    )])));
    match reg3.get_or_build("org_c", Uuid::new_v4(), "SECRET_C").await {
        Err(TenantPoolError::MissingFingerprint(org)) => assert_eq!(org, "org_c"),
        other => panic!("expected MissingFingerprint, got {other:?}"),
    }

    // Cleanup.
    for db in [db_a, db_b, db_c] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

// ── Org commons-ocean self-heal (`TenantRegistry::ensure_org_ocean`) ─────────
//
// These reproduce the actual prod bug (`testorg`: an `active`, fully-migrated,
// fingerprinted tenant with ZERO oceans) and its fix, entirely at the
// registry/tenant-DB layer — deliberately without a control-plane `accounts`
// row anywhere in these tests. That absence is itself the point: the original
// bug was that ocean-creation lived ONLY in `clerk_provision::
// ensure_org_account`, gated on the control-plane `accounts` INSERT and
// whatever pool happened to be reachable at that exact moment (see that
// function's doc comment for the full "provisioning-order trap"). The fix
// moves ocean-creation to `TenantRegistry::build` instead, which never
// consults `accounts`/`tenant_databases` timing at all — only "does this
// tenant database exist and pass its fingerprint check" — so proving it works
// with no control DB in the picture is exactly what demonstrates
// order-independence, not a gap in coverage.

#[tokio::test]
async fn build_creates_org_ocean_when_missing() {
    let Some(base) = base_url() else {
        eprintln!("skipping build_creates_org_ocean_when_missing: set ORGT_TEST_BASE_URL to run");
        return;
    };

    let db = "orgt_reg_ocean_missing";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate_db(&mut conn, db).await;
    // Simulates a tenant provisioned AFTER `ensure_org_account` already ran
    // and skipped the ocean (the real bug's exact shape): tenant tree + a
    // fingerprint, but no `graphs` row of any kind.
    apply_tenant_tree_and_fingerprint(&base, db, Some("org_missing")).await;

    let account_id = Uuid::new_v4();
    let resolver = StaticMapResolver::from_pairs([("SECRET_MISSING", with_db(&base, db))]);
    let reg = TenantRegistry::new(Arc::new(resolver));

    let pool = reg
        .get_or_build("org_missing", account_id, "SECRET_MISSING")
        .await
        .expect("build must succeed and self-heal the missing ocean");

    let oceans = org_oceans(&pool, account_id).await;
    assert_eq!(
        oceans.len(),
        1,
        "exactly one org ocean must exist after the first build, got {oceans:?}"
    );
    let (name, is_primary) = &oceans[0];
    assert_eq!(name, "Organization", "self-healed ocean uses the generic fallback name");
    assert!(!is_primary, "an org ocean is never is_primary");

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

#[tokio::test]
async fn ensure_org_ocean_is_idempotent_across_separate_cold_builds() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping ensure_org_ocean_is_idempotent_across_separate_cold_builds: \
             set ORGT_TEST_BASE_URL to run"
        );
        return;
    };

    let db = "orgt_reg_ocean_idem";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate_db(&mut conn, db).await;
    apply_tenant_tree_and_fingerprint(&base, db, Some("org_idem")).await;

    let account_id = Uuid::new_v4();
    let url = with_db(&base, db);

    // Two INDEPENDENT registries (and therefore two independent `OnceCell`s,
    // i.e. two genuinely separate cold `build()` calls, not one cached hit) —
    // simulates the ocean-creating step running twice across process
    // restarts/redeploys, or across the request path and the tenant-harness
    // worker both reaching a cold build for the same org.
    let reg1 = TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_IDEM",
        url.clone(),
    )])));
    let pool1 = reg1
        .get_or_build("org_idem", account_id, "SECRET_IDEM")
        .await
        .expect("first cold build creates the ocean");

    let reg2 = TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_IDEM",
        url,
    )])));
    reg2.get_or_build("org_idem", account_id, "SECRET_IDEM")
        .await
        .expect("second cold build is a no-op, not an error");

    let oceans = org_oceans(&pool1, account_id).await;
    assert_eq!(
        oceans.len(),
        1,
        "running the ensure-ocean insert across two separate cold builds must not duplicate \
         the ocean, got {oceans:?}"
    );

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

#[tokio::test]
async fn build_never_overwrites_a_pre_existing_org_ocean() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping build_never_overwrites_a_pre_existing_org_ocean: set ORGT_TEST_BASE_URL to run"
        );
        return;
    };

    let db = "orgt_reg_ocean_preexisting";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate_db(&mut conn, db).await;
    apply_tenant_tree_and_fingerprint(&base, db, Some("org_preexisting")).await;

    let account_id = Uuid::new_v4();
    let url = with_db(&base, db);

    // Simulates the ORDER-CORRECT case: `clerk_provision::ensure_org_account`
    // already created the ocean (with the real Clerk org name) before the
    // tenant pool is ever built here.
    let seed_pool: PgPool = PgPoolOptions::new().connect(&url).await.unwrap();
    sqlx::query("INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Acme Corp', 'org')")
        .bind(account_id)
        .execute(&seed_pool)
        .await
        .unwrap();
    seed_pool.close().await;

    let reg = TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_PRE",
        url,
    )])));
    let pool = reg
        .get_or_build("org_preexisting", account_id, "SECRET_PRE")
        .await
        .expect("build succeeds against an already-correctly-provisioned tenant");

    let oceans = org_oceans(&pool, account_id).await;
    assert_eq!(oceans.len(), 1, "still exactly one org ocean, got {oceans:?}");
    assert_eq!(
        oceans[0].0, "Acme Corp",
        "ON CONFLICT DO NOTHING must never rename/clobber an ocean that already exists"
    );

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

#[tokio::test]
async fn ensure_org_ocean_is_race_safe_under_concurrent_first_builds() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping ensure_org_ocean_is_race_safe_under_concurrent_first_builds: \
             set ORGT_TEST_BASE_URL to run"
        );
        return;
    };

    let db = "orgt_reg_ocean_race";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate_db(&mut conn, db).await;
    apply_tenant_tree_and_fingerprint(&base, db, Some("org_race")).await;

    let account_id = Uuid::new_v4();
    let url = with_db(&base, db);

    // Two SEPARATE registries (separate `OnceCell`s) racing their first cold
    // `build()` for the SAME org/account concurrently — simulates a webhook
    // and a member's first JWT (or two members' first requests) landing on
    // different process instances at the same moment. Only the database's own
    // `uq_graphs_one_org_per_account` unique index (not any in-process lock)
    // can make this safe.
    let reg_a = TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_RACE",
        url.clone(),
    )])));
    let reg_b = TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_RACE",
        url,
    )])));

    let (r1, r2) = tokio::join!(
        reg_a.get_or_build("org_race", account_id, "SECRET_RACE"),
        reg_b.get_or_build("org_race", account_id, "SECRET_RACE"),
    );
    let pool = r1.expect("first racer's build must succeed");
    r2.expect("second racer's build must also succeed (ON CONFLICT DO NOTHING, never an error)");

    let oceans = org_oceans(&pool, account_id).await;
    assert_eq!(
        oceans.len(),
        1,
        "a race between two concurrent first builds must still leave exactly one org ocean, \
         got {oceans:?}"
    );

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}
