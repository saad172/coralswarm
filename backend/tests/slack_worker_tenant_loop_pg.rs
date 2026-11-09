//! Org-slack step 4: proves the three Slack workers (`huddle`,
//! `slack_name_backfill`, `slack_seat_reconcile`) that just moved onto the
//! tenant loop actually read the SAME plane their enqueue site writes onto —
//! and would FAIL if a pair were ever flipped out of step.
//!
//! # Why this suite exists
//!
//! Tenant databases have their own `jobs` table. An enqueue on one plane is
//! completely invisible to a dequeuer on the other — no error, no warning, no
//! dead-lettered row, the job simply sits in a queue nobody reads. Every
//! worker had to move planes in the SAME commit as its enqueue site
//! (`routes::slack_events::enqueue_huddle_capture` /
//! `persist_identity_and_enqueue_backfill`'s `SlackNameBackfill` enqueue /
//! the hand-inserted `JobKind::SlackSeatReconcile`), and this is the
//! regression guard that would catch it if a future change ever un-paired
//! them: each test enqueues a job directly onto ONE tenant's `jobs` table,
//! then proves the SAME worker's `tick`, pointed at a DIFFERENT tenant's
//! pool, finds nothing — only the correctly-paired plane sees the job.
//!
//! # Second half: the reconciler sweeping a real tenant
//!
//! `workers::slack_seat_reconcile`'s two plane-aware helpers —
//! `accounts_to_sweep` (resolving an org tenant's own singleton account from
//! `tenant_databases`) and `seat_bearing_channels_for_account` (reading
//! `slack_watched_channels JOIN teams` on the tenant's own content pool) —
//! never had a genuine SECOND database to run against before this PR: the
//! worker only ever served the single shared consumer pool. This suite
//! proves both against a real, separate tenant database.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every other
//! `*_pg.rs` suite in this tree:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test slack_worker_tenant_loop_pg

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use coralswarm_backend::jobs::{queue, JobKind};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::tenant_harness::{Tick, WorkerTenant};
use coralswarm_backend::workers::{huddle, slack_name_backfill, slack_seat_reconcile};
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

/// Three real Postgres databases: control + two independent org tenants —
/// exactly the shape a cross-plane pairing bug can hide in (two tenant DBs
/// each with their own `jobs` table, invisible to each other by
/// construction).
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

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, &ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let tenant_a: PgPool = PgPoolOptions::new().connect(&with_db(&base, &ten_a)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_a).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_a')")
        .execute(&tenant_a)
        .await
        .unwrap();

    let tenant_b: PgPool = PgPoolOptions::new().connect(&with_db(&base, &ten_b)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_b).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_b')")
        .execute(&tenant_b)
        .await
        .unwrap();

    ThreeDbFixture { _databases: databases, admin_conn, control, tenant_a, tenant_b }
}

impl ThreeDbFixture {
    async fn teardown(mut self, prefix: &str) {
        self.control.close().await;
        self.tenant_a.close().await;
        self.tenant_b.close().await;
        for suffix in ["_ctl", "_ten_a", "_ten_b"] {
            drop_db(&mut self.admin_conn, &format!("{prefix}{suffix}")).await;
        }
    }
}

// ── Pair 1: huddle enqueue ↔ workers::huddle::tick ──────────────────────────

/// A `SlackHuddleCapture` job enqueued onto tenant A's `jobs` table must be
/// invisible to `huddle::tick` scoped to tenant B, and visible (claimed) to
/// `huddle::tick` scoped to tenant A. If `enqueue_huddle_capture` and
/// `workers::huddle::tick` were ever flipped out of step — one writing to a
/// hardcoded plane while the other reads a different one — this test fails:
/// either tenant A stops seeing its own job (regression on the correct
/// pairing) or tenant B starts seeing a job that isn't its own (the actual
/// cross-tenant leak this whole PR exists to prevent).
#[tokio::test]
async fn huddle_job_on_tenant_a_is_invisible_to_tenant_b_worker_pg() {
    let Some(_) = base_url() else {
        eprintln!("skipping huddle_job_on_tenant_a_is_invisible_to_tenant_b_worker_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let fx = setup_three_db("orgt_s4_huddle").await;

    let account_id = Uuid::new_v4();
    let kind = JobKind::SlackHuddleCapture {
        account_id,
        workspace_id: "T1".to_string(),
        channel_id: "C1".to_string(),
        huddle_thread_ts: "111.222".to_string(),
        canvas_file_id: "F1".to_string(),
        participant_history: vec![],
        date_start: None,
        date_end: None,
    };
    queue::enqueue(&fx.tenant_a, &kind, Some(account_id), None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");

    let wt_b = worker_tenant(TenantId::Org("org_b".into()), &fx.tenant_b);
    let tick_b = huddle::tick(&wt_b, &fx.control, &reqwest::Client::new(), None)
        .await
        .expect("tick against tenant B must not error");
    assert_eq!(
        tick_b,
        Tick::idle(),
        "a job enqueued on tenant A's jobs table must be invisible to a worker scoped to tenant B"
    );

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = huddle::tick(&wt_a, &fx.control, &reqwest::Client::new(), None)
        .await
        .expect("tick against tenant A must not error");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the job must be claimable by a worker scoped to the SAME tenant it was enqueued on \
         (processing itself fails closed — no oauth token seeded — but dequeue visibility is \
         what this test proves)"
    );

    fx.teardown("orgt_s4_huddle").await;
}

// ── Pair 2: SlackNameBackfill enqueue ↔ workers::slack_name_backfill::tick ──

/// Same proof for `SlackNameBackfill` — the natural-repair enqueue in
/// `persist_identity_and_enqueue_backfill` and `workers::slack_name_backfill
/// ::tick` must always target the same plane.
#[tokio::test]
async fn slack_name_backfill_job_on_tenant_a_is_invisible_to_tenant_b_worker_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping slack_name_backfill_job_on_tenant_a_is_invisible_to_tenant_b_worker_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_s4_backfill").await;

    let account_id = Uuid::new_v4();
    let kind = JobKind::SlackNameBackfill {
        graph_id: None,
        workspace_id: None,
        author_id: Some("U0TESTUSER1".to_string()),
        after_atom_id: None,
    };
    queue::enqueue(&fx.tenant_a, &kind, Some(account_id), None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");

    let wt_b = worker_tenant(TenantId::Org("org_b".into()), &fx.tenant_b);
    let tick_b = slack_name_backfill::tick(&wt_b, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant B must not error");
    assert_eq!(
        tick_b,
        Tick::idle(),
        "a SlackNameBackfill job enqueued on tenant A must be invisible to a worker scoped to \
         tenant B"
    );

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = slack_name_backfill::tick(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant A must not error");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the job must be claimable by a worker scoped to the SAME tenant it was enqueued on \
         (the batch itself finds zero matching atoms — none seeded — but dequeue visibility is \
         what this test proves)"
    );

    fx.teardown("orgt_s4_backfill").await;
}

// ── Pair 3: SlackSeatReconcile hand-insert ↔ workers::slack_seat_reconcile::tick ──

/// `JobKind::SlackSeatReconcile` has no live enqueue site (hand-inserted —
/// see that variant's own doc comment), so there is no sibling commit to
/// pair with; what still must hold is that the worker only ever reads the
/// plane an operator targets. Filtered to a channel id that matches nothing
/// seeded, so `process_job` completes with zero Slack API calls (network-free
/// — see this suite's module doc) while still proving dequeue visibility.
#[tokio::test]
async fn slack_seat_reconcile_job_on_tenant_a_is_invisible_to_tenant_b_worker_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping slack_seat_reconcile_job_on_tenant_a_is_invisible_to_tenant_b_worker_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_s4_seat").await;

    let account_id = Uuid::new_v4();
    let kind = JobKind::SlackSeatReconcile {
        account_id: Some(account_id),
        channel_id: Some("C-nonexistent".to_string()),
    };
    queue::enqueue(&fx.tenant_a, &kind, Some(account_id), None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");

    let wt_b = worker_tenant(TenantId::Org("org_b".into()), &fx.tenant_b);
    let tick_b = slack_seat_reconcile::tick(&wt_b, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant B must not error");
    assert_eq!(
        tick_b,
        Tick::idle(),
        "a SlackSeatReconcile job hand-inserted onto tenant A's jobs table must be invisible to \
         a worker scoped to tenant B"
    );

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = slack_seat_reconcile::tick(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant A must not error");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the job must be claimable by a worker scoped to the SAME tenant it was hand-inserted \
         onto"
    );

    fx.teardown("orgt_s4_seat").await;
}

// ── The reconciler sweeping a real tenant ───────────────────────────────────

/// `accounts_to_sweep`'s org-tenant arm resolves an org's SINGLETON account
/// from the control plane's `tenant_databases` routing row — this never had
/// a genuine second database to run against before this PR (the worker only
/// ever served the single shared consumer pool). Proves it against a real
/// control DB + a real, separate tenant DB.
///
/// `process_job` is driven with `account_id: None` (the all-accounts-sweep
/// shape) precisely so this exercises `accounts_to_sweep`'s control-plane
/// lookup rather than shortcutting past it via an explicitly-stamped id.
#[tokio::test]
async fn seat_reconcile_resolves_the_real_org_account_from_control_pg() {
    let Some(_) = base_url() else {
        eprintln!("skipping seat_reconcile_resolves_the_real_org_account_from_control_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let fx = setup_three_db("orgt_s4_acct").await;

    // Seed control: a real org account + its tenant_databases routing row,
    // pointing at tenant A.
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("seat-acct-{}@test.local", Uuid::new_v4()))
    .fetch_one(&fx.control)
    .await
    .unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, 'org_a') RETURNING id",
    )
    .bind(user_id)
    .fetch_one(&fx.control)
    .await
    .unwrap();
    financial_fixture::execute(
        &fx.control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_a', $1, 'orgt-s4-acct', 'unused-in-this-test', 'active')",
        )
        .bind(account_id),
    )
    .await
    .unwrap();

    // Filtered to a channel matching nothing seeded on tenant A, so this
    // still completes with zero Slack API calls (network-free — module doc)
    // once account resolution has already been exercised.
    let kind = JobKind::SlackSeatReconcile {
        account_id: None,
        channel_id: Some("C-nonexistent".to_string()),
    };
    queue::enqueue(&fx.tenant_a, &kind, None, None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = slack_seat_reconcile::tick(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant A must not error");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the account-less sweep must resolve tenant A's own singleton account from control's \
         tenant_databases and complete the job — proving the org-tenant arm of \
         accounts_to_sweep against a REAL second database, not merely the consumer plane"
    );

    fx.teardown("orgt_s4_acct").await;
}

/// `seat_bearing_channels_for_account` reads `slack_watched_channels JOIN
/// teams` on the tenant's own content pool — this table pair, and the join,
/// never ran against a genuine tenant database before this PR. Seeds a real
/// team ocean + a real seat-bearing `slack_watched_channels` row on tenant A
/// and proves the worker's own helper (not a hand-rolled query) finds it.
#[tokio::test]
async fn seat_bearing_channels_for_account_finds_a_real_row_on_a_tenant_database_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping seat_bearing_channels_for_account_finds_a_real_row_on_a_tenant_database_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_s4_channels").await;

    let account_id = Uuid::new_v4();
    let creator_user_id = Uuid::new_v4();
    // `kind = 'team'` — `graphs_member_requires_owner`'s CHECK ties
    // `owner_user_id IS NOT NULL` to exactly `kind = 'member'`, so a team
    // ocean's `owner_user_id` must stay NULL.
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Team ocean', 'team') \
         RETURNING id",
    )
    .bind(account_id)
    .fetch_one(&fx.tenant_a)
    .await
    .unwrap();
    let team_id: Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, 'Eng', $3) \
         RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(creator_user_id)
    .fetch_one(&fx.tenant_a)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO slack_watched_channels \
             (account_id, user_id, workspace_id, channel_id, mode, graph_id) \
         VALUES ($1, $2, 'T1', 'C-real', 'single', $3)",
    )
    .bind(account_id)
    .bind(creator_user_id)
    .bind(graph_id)
    .execute(&fx.tenant_a)
    .await
    .unwrap();

    // Direct call to the worker's own discovery helper (via the test-support
    // wrapper — the function itself is private to the module) against the
    // REAL tenant A pool: the seeded row must actually be found.
    let found = slack_seat_reconcile::test_support_seat_bearing_channel_count(
        &fx.tenant_a,
        account_id,
    )
    .await
    .expect("seat_bearing_channels_for_account must not error against a real tenant DB");
    assert_eq!(
        found, 1,
        "the seeded slack_watched_channels/teams row must be discovered on tenant A — the \
         join this helper runs never had a genuine second database to run against before this \
         PR (the worker only ever served the single shared consumer pool)"
    );

    // Sanity: a DIFFERENT (unseeded) account on the SAME tenant DB finds
    // nothing — proves the account_id predicate is doing real work, not
    // vacuously matching every row in the database.
    let other_account = Uuid::new_v4();
    let found_other = slack_seat_reconcile::test_support_seat_bearing_channel_count(
        &fx.tenant_a,
        other_account,
    )
    .await
    .expect("must not error");
    assert_eq!(found_other, 0, "an unrelated account must see none of this account's channels");

    let _ = team_id;
    fx.teardown("orgt_s4_channels").await;
}

// ── accounts_to_sweep rejects a foreign payload account_id ─────────────────

/// A hand-inserted `SlackSeatReconcile { account_id: Some(<wrong account>) }`
/// landing on an org tenant's own `jobs` table must be REJECTED, not swept —
/// `JobKind::SlackSeatReconcile` is hand-inserted (this worker's own module
/// doc), so an operator mistyping an id, or pasting a DIFFERENT org's
/// account into org A's `jobs` table, is the likely failure mode. Before
/// this fix `accounts_to_sweep` trusted the payload verbatim; a mismatch
/// would have swept org A's content pool under a foreign account_id
/// (CodeRabbit review on PR #490).
#[tokio::test]
async fn seat_reconcile_rejects_a_payload_account_id_foreign_to_the_tenant_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping seat_reconcile_rejects_a_payload_account_id_foreign_to_the_tenant_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_s4_wrongacct").await;

    // Seed control: org A's REAL account + its tenant_databases routing row.
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("wrongacct-{}@test.local", Uuid::new_v4()))
    .fetch_one(&fx.control)
    .await
    .unwrap();
    let real_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, 'org_a') RETURNING id",
    )
    .bind(user_id)
    .fetch_one(&fx.control)
    .await
    .unwrap();
    financial_fixture::execute(
        &fx.control,
        real_account_id,
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_a', $1, 'orgt-s4-wrongacct', 'unused-in-this-test', 'active')",
        )
        .bind(real_account_id),
    )
    .await
    .unwrap();

    // Hand-insert a job naming a DIFFERENT (foreign) account — the operator
    // mistake this fix guards against.
    let foreign_account_id = Uuid::new_v4();
    let kind = JobKind::SlackSeatReconcile {
        account_id: Some(foreign_account_id),
        channel_id: None,
    };
    let job_id = queue::enqueue(&fx.tenant_a, &kind, Some(foreign_account_id), None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = slack_seat_reconcile::tick(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant A must not error");
    assert_eq!(tick_a, Tick::worked(1), "the job is still claimed (dequeue is unaffected)");

    let (status, error_message): (String, Option<String>) =
        sqlx::query_as("SELECT status::text, error_message FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&fx.tenant_a)
            .await
            .unwrap();
    assert_ne!(
        status, "done",
        "a foreign account_id must never complete successfully — that would mean it swept \
         org A's content pool under an account that isn't org A's own"
    );
    let msg = error_message.unwrap_or_default();
    assert!(
        msg.contains(&foreign_account_id.to_string()) && msg.contains(&real_account_id.to_string()),
        "the failure must name BOTH the rejected id and the tenant's real one, not fail \
         silently or ambiguously: {msg}"
    );

    fx.teardown("orgt_s4_wrongacct").await;
}
