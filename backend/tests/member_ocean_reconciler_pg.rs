//! Member-ocean reconciler — provisioning-arm proof (member-private-oceans
//! PR 3, `docs/plans/member-private-oceans.md` §2.4 trigger 3).
//!
//! Calls `workers::member_ocean_reconciler::tick` directly against a real
//! control-plane scratch DB and a real tenant-plane scratch DB (no HTTP
//! stack, no `run_multi_tenant` harness loop — this is a unit-level proof of
//! the tick's own SQL, the same scope `reaper::tick` gets in isolation).
//! Three properties:
//!
//! 1. **Idempotence** — running the tick twice against an unchanged
//!    `account_members` roster yields exactly one member ocean per member,
//!    not two.
//! 2. **Race-safety** — two concurrent ticks provisioning the SAME member
//!    (simulating two backend processes, or this reconciler racing the
//!    PR-4 first-sight path) yield exactly one ocean for that member, via
//!    `uq_graphs_one_primary_per_owner`'s `ON CONFLICT ... DO NOTHING`.
//! 3. **Skip-if-present** — a member who already has a non-archived primary
//!    member ocean (planted directly, as PR 4's first-sight trigger would
//!    have created it) is left alone: the tick provisions nothing for them
//!    and their existing `graphs.id` is unchanged.
//!
//! Also covers: the flag-off no-op, and the consumer-plane no-op.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same scratch-DB pattern as every other `*_pg.rs` file
//! in this directory (`worker_harness_pg.rs` is the closest sibling: it
//! exercises `tenant_harness::run_multi_tenant` directly rather than routes).
//!
//! ## Honest CI coverage
//!
//! This suite is **NOT** currently wired into any `.github/workflows/ci.yml`
//! job — the same gap `worker_harness_pg.rs`, `reachability_pg.rs`,
//! `mcp_tenant_pg.rs`, `dek_relocation_pg.rs`, and a number of other
//! `ORGT_TEST_BASE_URL`-gated suites already have (verified: none of those
//! names appear anywhere in `ci.yml` either). The workspace `cargo test`
//! job has no Postgres service and no `ORGT_TEST_BASE_URL`, so every test in
//! this file returns `ok` having done nothing the moment `base_url()` is
//! `None` — indistinguishable in CI logs from a real pass. This is a
//! pre-existing, repo-wide gap this PR does not attempt to fix wholesale (see
//! the PR description); flagged here rather than silently relying on green
//! CI to mean "this ran."
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test member_ocean_reconciler_pg -- --nocapture

#[path = "support/retained_database.rs"]
mod retained_database;

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::member_ocean_reconciler::tick;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

/// Serializes this file's process-global `MEMBER_OCEANS_ENABLED` mutation
/// against sibling tests IN THIS BINARY — same pattern as every other
/// `*_pg.rs` file's `ENV_LOCK` (e.g. `route_migration_org_pg.rs`).
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

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
    retained_database::recreate(conn, db).await.unwrap();
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

async fn tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(8)
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

/// Mint an org's control-plane account row + routing row (`tenant_databases`)
/// — the reconciler resolves `account_id` from the routing row, not from
/// `account_keys` on the tenant side (see `tick`'s doc comment).
async fn mint_org(control: &PgPool, org_id: &str, slug: &str) -> Uuid {
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("{org_id}-owner@wh.test"))
        .fetch_one(control)
        .await
        .unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(org_id)
    .fetch_one(control)
    .await
    .unwrap();
    financial_fixture::execute(
        control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases \
         (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
         VALUES ($1, $2, $3, $4, 'neon', 1, 'active')",
        )
        .bind(org_id)
        .bind(account_id)
        .bind(slug)
        .bind(format!("{slug}-secret")),
    )
    .await
    .unwrap();
    account_id
}

/// Add a member to the org (control-plane `account_members`), optionally with
/// a display name on their `users` row.
async fn add_member(control: &PgPool, account_id: Uuid, email: &str, name: Option<&str>) -> Uuid {
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users(email, name) VALUES ($1, $2) RETURNING id")
            .bind(email)
            .bind(name)
            .fetch_one(control)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
        .bind(account_id)
        .bind(user_id)
        .execute(control)
        .await
        .unwrap();
    user_id
}

/// Primary, non-archived `kind='member'` oceans owned by `user_id` on the
/// tenant pool — the invariant `uq_graphs_one_primary_per_owner` enforces at
/// most one of.
async fn primary_member_oceans(tenant: &PgPool, user_id: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM graphs \
         WHERE kind = 'member' AND owner_user_id = $1 AND is_primary AND archived_at IS NULL",
    )
    .bind(user_id)
    .fetch_all(tenant)
    .await
    .unwrap()
}

fn worker_tenant(org_id: &str, pool: PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: TenantId::Org(org_id.to_string()),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
        pool,
    }
}

/// SAFETY: env mutation is serialized process-wide by the caller holding
/// `ENV_LOCK` for the duration of the test body.
unsafe fn set_member_oceans_enabled(on: bool) {
    if on {
        std::env::set_var("MEMBER_OCEANS_ENABLED", "1");
    } else {
        std::env::remove_var("MEMBER_OCEANS_ENABLED");
    }
}

#[tokio::test]
async fn idempotent_race_safe_and_skips_existing_ocean() {
    let Some(base) = base_url() else {
        eprintln!("skipping idempotent_race_safe_and_skips_existing_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    unsafe { set_member_oceans_enabled(true) };

    let (ctl, tnt) = ("orgt_mor1_ctl", "orgt_mor1_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        recreate(&mut conn, db).await;
    }

    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_mor1").await;

    let account_id = mint_org(&control, "org_mor1", "mor1-org").await;
    let alice = add_member(&control, account_id, "alice@wh.test", Some("Alice Smith")).await;
    let bob = add_member(&control, account_id, "bob@wh.test", None).await;

    // Bob already has a primary member ocean, planted directly (as if PR 4's
    // first-sight trigger created it earlier) — the reconciler must skip him.
    let bobs_existing_ocean: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
         VALUES ($1, 'Bob''s pre-existing ocean', 'member', $2, true) RETURNING id",
    )
    .bind(account_id)
    .bind(bob)
    .fetch_one(&tenant)
    .await
    .unwrap();

    let wt = worker_tenant("org_mor1", tenant.clone());

    // ── First tick: provisions Alice, skips Bob ──────────────────────────────
    let result = tick(&wt, &control, None).await.expect("tick 1 ok");
    assert_eq!(result.processed, 1, "exactly one new ocean (Alice's)");

    let alice_oceans = primary_member_oceans(&tenant, alice).await;
    assert_eq!(alice_oceans.len(), 1, "Alice has exactly one primary member ocean");
    let alice_ocean_name: String =
        sqlx::query_scalar("SELECT name FROM graphs WHERE id = $1")
            .bind(alice_oceans[0])
            .fetch_one(&tenant)
            .await
            .unwrap();
    assert_eq!(alice_ocean_name, "Alice Smith", "named from users.name");

    let bob_oceans = primary_member_oceans(&tenant, bob).await;
    assert_eq!(bob_oceans, vec![bobs_existing_ocean], "Bob's ocean untouched, no duplicate");

    // ── Idempotence: a second tick changes nothing ───────────────────────────
    let result2 = tick(&wt, &control, None).await.expect("tick 2 ok");
    assert_eq!(result2.processed, 0, "second tick finds nothing to provision");
    assert_eq!(
        primary_member_oceans(&tenant, alice).await,
        alice_oceans,
        "still exactly Alice's one ocean, same id"
    );
    assert_eq!(
        primary_member_oceans(&tenant, bob).await,
        vec![bobs_existing_ocean],
        "Bob still has exactly his pre-existing ocean"
    );

    // ── Race-safety: two concurrent ticks provisioning a NEW third member ───
    let carol = add_member(&control, account_id, "carol@wh.test", Some("Carol Jones")).await;
    let wt2 = worker_tenant("org_mor1", tenant.clone());
    let (r1, r2) = tokio::join!(tick(&wt, &control, None), tick(&wt2, &control, None));
    let (r1, r2) = (r1.expect("race tick A ok"), r2.expect("race tick B ok"));
    assert_eq!(
        r1.processed + r2.processed,
        1,
        "exactly one of the two concurrent ticks wins Carol's insert"
    );
    let carol_oceans = primary_member_oceans(&tenant, carol).await;
    assert_eq!(carol_oceans.len(), 1, "exactly one primary member ocean for Carol, never two");

    unsafe { set_member_oceans_enabled(false) };
    tenant.close().await;
    control.close().await;
    for db in [ctl, tnt] {
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }
}

#[tokio::test]
async fn flag_off_is_a_pure_noop() {
    let Some(base) = base_url() else {
        eprintln!("skipping flag_off_is_a_pure_noop: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    unsafe { set_member_oceans_enabled(false) };

    let (ctl, tnt) = ("orgt_mor2_ctl", "orgt_mor2_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_mor2").await;
    let account_id = mint_org(&control, "org_mor2", "mor2-org").await;
    let alice = add_member(&control, account_id, "alice2@wh.test", Some("Alice")).await;

    let wt = worker_tenant("org_mor2", tenant.clone());
    let result = tick(&wt, &control, None).await.expect("tick ok");
    assert_eq!(result.processed, 0, "flag off: nothing provisioned");
    assert!(
        primary_member_oceans(&tenant, alice).await.is_empty(),
        "flag off: no member ocean created for anyone"
    );

    tenant.close().await;
    control.close().await;
    for db in [ctl, tnt] {
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }
}

#[tokio::test]
async fn consumer_plane_is_a_pure_noop() {
    let Some(base) = base_url() else {
        eprintln!("skipping consumer_plane_is_a_pure_noop: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    unsafe { set_member_oceans_enabled(true) };

    let ctl = "orgt_mor3_ctl";
    let mut conn = admin(&base).await;
    recreate(&mut conn, ctl).await;
    let control = control_pool(&base, ctl).await;

    let wt = WorkerTenant {
        tenant: TenantId::Consumer,
        store: Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>,
        pool: control.clone(),
    };
    let result = tick(&wt, &control, None).await.expect("tick ok");
    assert_eq!(result.processed, 0, "consumer plane: member oceans are org-tenant-only, always a no-op");

    unsafe { set_member_oceans_enabled(false) };
    control.close().await;
    retained_database::finish(&mut conn, ctl, true)
        .await
        .ok();
}
