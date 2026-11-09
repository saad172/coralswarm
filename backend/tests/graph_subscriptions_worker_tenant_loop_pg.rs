//! m365 Train 2 PR 2.2: proves `workers::graph_subscriptions`'s renewal tick
//! actually resolves an org tenant's own singleton account from the
//! control-plane `tenant_databases` routing row, and reads/writes
//! `graph_subscriptions` on that tenant's OWN content pool — a code path
//! that never had a genuine second database to run against before this PR
//! (the worker was consumer-only; see `workers::tenant_harness`'s decision
//! table). Mirrors `slack_worker_tenant_loop_pg.rs`'s "the reconciler
//! sweeping a real tenant" section, which is this suite's closest precedent.
//!
//! Every scenario here seeds NO `oauth_tokens` row, so `renew_one`'s
//! `load_creds_bg` always returns `Ok(None)` — the same "processing itself
//! fails closed, no oauth token seeded, but dequeue/discovery visibility is
//! what this test proves" posture `slack_worker_tenant_loop_pg.rs`'s huddle
//! test documents. That keeps every test here network-free while still
//! exercising the full `resolve_tenant_account` → `expiring_subscriptions_
//! for_account` → `renew_one` → `delete_subscription_row_for_account` chain
//! against a REAL tenant Postgres database.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every other
//! `*_pg.rs` suite in this tree:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test graph_subscriptions_worker_tenant_loop_pg

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::graph_subscriptions;
use coralswarm_backend::workers::tenant_harness::{Tick, WorkerTenant};
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

async fn drop_db(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

fn worker_tenant(tenant: TenantId, pool: &PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
    }
}

/// Control + two independent org tenant databases — org A carries the
/// seeded subscription, org B is the sanity check that a tenant with no rows
/// of its own sees nothing.
struct ThreeDbFixture {
    _databases: Vec<coralswarm_backend::test_db::TestDatabase>,
    admin_conn: PgConnection,
    control: PgPool,
    tenant_a: PgPool,
    tenant_b: PgPool,
}

async fn setup_three_db(prefix: &str) -> ThreeDbFixture {
    let base = base_url().expect("caller already checked base_url().is_some()");
    let (ctl, ten_a, ten_b) = (
        format!("{prefix}_ctl"),
        format!("{prefix}_ten_a"),
        format!("{prefix}_ten_b"),
    );
    let databases = [&ctl, &ten_a, &ten_b].iter().map(|name| coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, name)).collect();
    let mut admin_conn = admin(&base).await;
    for db in [&ctl, &ten_a, &ten_b] {
        recreate(&mut admin_conn, db).await;
    }

    let control: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, &ctl))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control")
        .run(&control)
        .await
        .unwrap();

    let tenant_a: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, &ten_a))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant")
        .run(&tenant_a)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_a')")
        .execute(&tenant_a)
        .await
        .unwrap();

    let tenant_b: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, &ten_b))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant")
        .run(&tenant_b)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_b')")
        .execute(&tenant_b)
        .await
        .unwrap();

    ThreeDbFixture {
        _databases: databases,
        admin_conn,
        control,
        tenant_a,
        tenant_b,
    }
}

impl ThreeDbFixture {
    /// Seed a real org account + its `tenant_databases` routing row on the
    /// control DB, pointing at `clerk_org_id`. Returns the account id.
    async fn seed_org_account(&self, clerk_org_id: &str, slug: &str) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("{clerk_org_id}-{}@test.local", Uuid::new_v4()))
        .fetch_one(&self.control)
        .await
        .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(clerk_org_id)
        .fetch_one(&self.control)
        .await
        .unwrap();
        // `db_secret_ref` carries its own UNIQUE constraint
        // (`tenant_databases_secret_ref_key`) — derived from `slug` (already
        // unique per call site in this suite) rather than a shared literal,
        // which collided the moment a single test seeded two accounts.
        financial_fixture::execute(
            &self.control,
            account_id,
            sqlx::query(
                "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
                 VALUES ($1, $2, $3, $3, 'active')",
            )
            .bind(clerk_org_id)
            .bind(account_id)
            .bind(slug),
        )
        .await
        .unwrap();
        account_id
    }

    async fn teardown(mut self, prefix: &str) {
        self.control.close().await;
        self.tenant_a.close().await;
        self.tenant_b.close().await;
        for suffix in ["_ctl", "_ten_a", "_ten_b"] {
            drop_db(&mut self.admin_conn, &format!("{prefix}{suffix}")).await;
        }
    }
}

/// Seed a `graph_subscriptions` row on `pool` for `account_id`, due `hours`
/// from now (negative = already past).
async fn seed_subscription(pool: &PgPool, account_id: Uuid, subscription_id: &str, hours: i64) {
    sqlx::query(
        "INSERT INTO graph_subscriptions \
             (account_id, subscription_id, resource, client_state, expires_at, tenant_id) \
         VALUES ($1, $2, 'me/mailFolders/Inbox/messages', 'test-client-state', $3, '')",
    )
    .bind(account_id)
    .bind(subscription_id)
    .bind(Utc::now() + ChronoDuration::hours(hours))
    .execute(pool)
    .await
    .unwrap();
}

async fn subscription_row_exists(pool: &PgPool, subscription_id: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM graph_subscriptions WHERE subscription_id = $1)",
    )
    .bind(subscription_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ── org_tick_renew resolves the real org account and acts on its own row ───

/// A `graph_subscriptions` row already due for renewal, seeded on tenant A's
/// own content pool with NO `oauth_tokens` row for its account, must be
/// discovered by `tick_renew` scoped to org A (proving `resolve_tenant_
/// account` → `expiring_subscriptions_for_account` against a REAL tenant
/// database) and then dropped by `renew_one` (proving `delete_subscription_
/// row_for_account` — the account-scoped write path, not the control-only
/// `resolve_graph_subscription_account_id` re-derivation the plain `delete_
/// subscription_row` uses).
#[tokio::test]
async fn org_tenant_renew_tick_finds_and_drops_its_own_due_subscription_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping org_tenant_renew_tick_finds_and_drops_its_own_due_subscription_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_t22_renew").await;
    let account_a = fx.seed_org_account("org_a", "orgt-t22-renew-a").await;
    let _account_b = fx.seed_org_account("org_b", "orgt-t22-renew-b").await;

    seed_subscription(&fx.tenant_a, account_a, "sub-a-due", -1).await;

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = graph_subscriptions::tick_renew(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant A must not error");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the due subscription must be discovered against org A's OWN tenant database via its \
         resolved singleton account — this discovery path never ran against a genuine second \
         database before this PR (the worker was consumer-only)"
    );
    assert!(
        !subscription_row_exists(&fx.tenant_a, "sub-a-due").await,
        "with no oauth_tokens row for the account, renew_one must drop the stale subscription \
         row via the account-scoped delete path (delete_subscription_row_for_account)"
    );

    fx.teardown("orgt_t22_renew").await;
}

/// The SAME fixture shape, but ticking tenant B (a different org, no
/// subscriptions of its own) must find nothing — proving `resolve_tenant_
/// account` resolves EACH tenant's own account (never leaking org A's row or
/// crashing on org B's empty table) and that `expiring_subscriptions_for_
/// account`'s `account_id = $1` predicate is doing real work, not vacuously
/// matching.
#[tokio::test]
async fn org_tenant_renew_tick_on_a_different_tenant_finds_nothing_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping org_tenant_renew_tick_on_a_different_tenant_finds_nothing_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_t22_crosstenant").await;
    let account_a = fx.seed_org_account("org_a", "orgt-t22-cross-a").await;
    fx.seed_org_account("org_b", "orgt-t22-cross-b").await;

    seed_subscription(&fx.tenant_a, account_a, "sub-a-only", -1).await;

    let wt_b = worker_tenant(TenantId::Org("org_b".into()), &fx.tenant_b);
    let tick_b = graph_subscriptions::tick_renew(&wt_b, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant B must not error");
    assert_eq!(
        tick_b,
        Tick::idle(),
        "org A's subscription must be invisible to a tick scoped to org B's own (empty) tenant \
         database"
    );
    assert!(
        subscription_row_exists(&fx.tenant_a, "sub-a-only").await,
        "org B's tick must not have touched org A's row"
    );

    fx.teardown("orgt_t22_crosstenant").await;
}

/// A subscription that is NOT yet due (`expires_at` well past the renewal
/// window) must be left alone — proving `expiring_subscriptions_for_account`'s
/// cutoff filter, not merely "does it find rows at all".
#[tokio::test]
async fn org_tenant_renew_tick_leaves_a_not_yet_due_subscription_alone_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping org_tenant_renew_tick_leaves_a_not_yet_due_subscription_alone_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_t22_notdue").await;
    let account_a = fx.seed_org_account("org_a", "orgt-t22-notdue-a").await;

    // Comfortably beyond RENEW_WINDOW_HOURS (6h).
    seed_subscription(&fx.tenant_a, account_a, "sub-a-not-due", 48).await;

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = graph_subscriptions::tick_renew(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant A must not error");
    assert_eq!(
        tick_a,
        Tick::idle(),
        "a subscription expiring 48h out is well outside the renewal window and must not be \
         touched"
    );
    assert!(
        subscription_row_exists(&fx.tenant_a, "sub-a-not-due").await,
        "the not-yet-due row must still exist, untouched"
    );

    fx.teardown("orgt_t22_notdue").await;
}
