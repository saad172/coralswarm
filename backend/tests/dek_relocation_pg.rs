//! Integration test for DEK relocation (org-tenancy PR 1.3): personal DEKs
//! resolve from the control plane's `accounts.dek_wrapped`, org DEKs resolve
//! from the tenant plane's `account_keys.dek_wrapped`, and the load-bearing
//! tenant/pool assert rejects a swapped-pool / mismatched-account attempt.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights). Follows the same scratch-DB pattern as
//! `tenant_registry_pg.rs` / `binding_gate_pg.rs`.

use coralswarm_backend::crypto::{resolve_dek, resolve_dek_for_context, resolve_dek_for_tenant};
use coralswarm_backend::tenancy::runtime::TenantContext;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
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

/// A fixed test KEK so wrap/unwrap round-trips deterministically without
/// touching the real `MASTER_KEK_HEX` (set once per test process; safe since
/// each test that reads it runs under `#[tokio::test]`'s single-threaded
/// current-thread runtime per test binary invocation with `--test-threads=1`
/// not required here because every test in this file sets the SAME value).
fn set_test_kek() {
    unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) };
}

#[tokio::test]
async fn dek_relocation_splits_by_tenant_and_asserts_reject_mismatch() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping dek_relocation_splits_by_tenant_and_asserts_reject_mismatch: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    set_test_kek();
    let kek = load_kek().expect("test KEK loads");

    let (ctl, tenant_db) = ("orgt_dek_ctl", "orgt_dek_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + a personal account with a wrapped DEK ──────
    let control: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, ctl))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let personal_dek = generate_dek();
    let personal_wrapped = wrap_dek(&kek, &personal_dek).unwrap();
    let personal_account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'personal@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, dek_wrapped) SELECT id, $1 FROM u RETURNING id",
    )
    .bind(&personal_wrapped)
    .fetch_one(&control)
    .await
    .unwrap();

    // An org account row (control side always mints the account_id identity),
    // but deliberately NO accounts.dek_wrapped for it — under database-per-org
    // its DEK lives only in the tenant DB (the shadow-provisioning fence).
    let org_account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'org-owner@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, 'org_a' FROM u RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();

    // ── Tenant DB: tenant tree + fingerprint + the org's account_keys row ─────
    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_a')")
        .execute(&tenant_pool)
        .await
        .unwrap();

    let org_dek = generate_dek();
    let org_wrapped = wrap_dek(&kek, &org_dek).unwrap();
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(org_account_id)
        .bind(&org_wrapped)
        .execute(&tenant_pool)
        .await
        .unwrap();

    // ── 1. Personal DEK resolves from the CONTROL plane via resolve_dek ───────
    let resolved_personal = resolve_dek(&control, personal_account_id)
        .await
        .expect("resolve_dek succeeds")
        .expect("personal DEK present");
    assert_eq!(resolved_personal, personal_dek);

    // Sanity: the org account has no control-plane DEK at all (the fence held).
    let no_control_dek = resolve_dek(&control, org_account_id).await.unwrap();
    assert_eq!(
        no_control_dek, None,
        "org account must have no accounts.dek_wrapped row on the control plane"
    );

    // ── 2. Org DEK resolves from the TENANT plane via resolve_dek_for_tenant ──
    let resolved_org = resolve_dek_for_tenant(&TenantId::Org("org_a".into()), &tenant_pool, org_account_id)
        .await
        .expect("resolve_dek_for_tenant succeeds")
        .expect("org DEK present in tenant DB");
    assert_eq!(resolved_org, org_dek);

    // ── 3. resolve_dek_for_context: the happy path (ctx scoped to its own
    //    account) reaches the same tenant-DB DEK. ─────────────────────────────
    let ctx = TenantContext {
        org_id: "org_a".to_string(),
        account_id: org_account_id,
        // DEK resolution is account-scoped, not user-scoped, so a synthetic
        // id is fine here — `user_id` only matters to `ReachabilityResolver`
        // (org-tenancy PR 1.7's security fix), not `resolve_dek_for_context`.
        user_id: Uuid::new_v4(),
        pool: tenant_pool.clone(),
        page_pool: None,
    };
    let via_context = resolve_dek_for_context(&ctx, org_account_id)
        .await
        .expect("resolve_dek_for_context succeeds")
        .expect("org DEK present via context");
    assert_eq!(via_context, org_dek);

    // ── 4. Load-bearing assert: a context scoped to org_account_id must
    //    refuse to resolve a DIFFERENT account's DEK — fails closed, never
    //    silently substituting or falling back to another plane. ─────────────
    let mismatch_err = resolve_dek_for_context(&ctx, personal_account_id)
        .await
        .expect_err("mismatched account_id must be rejected");
    assert!(matches!(
        mismatch_err,
        coralswarm_backend::crypto::CryptoError::TenantMismatch(_)
    ));

    // ── 5. The swapped-pool case: calling the Org-tenant fetch path against
    //    the WRONG (control) pool must never silently succeed or return
    //    personal data — `account_keys` doesn't exist there at all, so it
    //    surfaces as a DB error, not a cross-plane read. ─────────────────────
    let swapped_pool_result =
        resolve_dek_for_tenant(&TenantId::Org("org_a".into()), &control, org_account_id).await;
    assert!(
        matches!(
            swapped_pool_result,
            Err(coralswarm_backend::crypto::CryptoError::DbError(_))
        ),
        "org-tenant fetch against the control pool must fail closed, got {swapped_pool_result:?}"
    );

    // Cleanup.
    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// The DekCache itself never mixes up personal vs. org entries for the same
/// `account_id` — belt-and-suspenders on top of the DB-level split above,
/// exercised here against the real `resolve_dek`/`resolve_dek_for_context`
/// cached variants rather than the pure unit tests in `dek_cache.rs`.
#[tokio::test]
async fn dek_cache_keeps_tenant_planes_independent_for_same_account_id() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping dek_cache_keeps_tenant_planes_independent_for_same_account_id: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    set_test_kek();
    let kek = load_kek().expect("test KEK loads");

    let (ctl, tenant_db) = ("orgt_dekcache_ctl", "orgt_dekcache_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_b')")
        .execute(&tenant_pool)
        .await
        .unwrap();

    // Deliberately use the SAME uuid for a control account_id and a tenant
    // account_keys.account_id, to prove the cache (and the resolvers) never
    // conflate the two merely because the UUID matches.
    let shared_id = Uuid::new_v4();

    let personal_dek = generate_dek();
    sqlx::query(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'p2@x.co') RETURNING id) \
         INSERT INTO accounts(id, owner_user_id, dek_wrapped) SELECT $1, id, $2 FROM u",
    )
    .bind(shared_id)
    .bind(wrap_dek(&kek, &personal_dek).unwrap())
    .execute(&control)
    .await
    .unwrap();

    let org_dek = generate_dek();
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(shared_id)
        .bind(wrap_dek(&kek, &org_dek).unwrap())
        .execute(&tenant_pool)
        .await
        .unwrap();

    let cache = coralswarm_backend::dek_cache::DekCache::new();

    let personal = coralswarm_backend::crypto::resolve_dek_cached(&control, shared_id, &cache)
        .await
        .unwrap()
        .unwrap();
    let ctx = TenantContext {
        org_id: "org_b".to_string(),
        account_id: shared_id,
        user_id: Uuid::new_v4(),
        pool: tenant_pool.clone(),
        page_pool: None,
    };
    let org = coralswarm_backend::crypto::resolve_dek_cached_for_context(&ctx, shared_id, &cache)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(personal, personal_dek);
    assert_eq!(org, org_dek);
    assert_ne!(
        personal, org,
        "same account_id, two tenants — must never collide in the cache"
    );

    // Second reads must be served from cache (still correct per tenant).
    assert_eq!(
        coralswarm_backend::crypto::resolve_dek_cached(&control, shared_id, &cache)
            .await
            .unwrap()
            .unwrap(),
        personal_dek
    );
    assert_eq!(
        coralswarm_backend::crypto::resolve_dek_cached_for_context(&ctx, shared_id, &cache)
            .await
            .unwrap()
            .unwrap(),
        org_dek
    );

    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
