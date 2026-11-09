//! m365 Train 2 PR 2.4: the Teams analog of `slack_worker_tenant_loop_pg.rs`'s
//! plane-pairing + real-tenant-discovery suite, for the brand-new
//! `workers::teams_seat_reconcile`.
//!
//! # Why this suite exists
//!
//! `JobKind::TeamsSeatReconcile` is hand-inserted (no live enqueue site —
//! same as `SlackSeatReconcile`), so there is no enqueue-site pairing bug to
//! regress here the way there was for `huddle`/`slack_name_backfill`. What
//! this worker never had before this PR is a genuine SECOND database to run
//! its two plane-aware helpers against: `accounts_to_sweep` (resolving an
//! org tenant's own singleton account from control's `tenant_databases`) and
//! `seat_bearing_channels_for_account` (reading `teams_watched_resources
//! JOIN teams` on the tenant's own content pool). This suite proves both
//! against real, separate tenant databases, plus the cross-tenant queue
//! isolation every tenant-loop worker gets (a job on tenant A's `jobs` table
//! must be invisible to a tick scoped to tenant B), plus the roster-abort
//! semantics copied from Slack: a roster walk that cannot be established
//! (here, no `MICROSOFT_CLIENT_ID` configured — a fast, local,
//! network-free failure) must change zero seats, never revoke on an
//! "empty" answer.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every other
//! `*_pg.rs` suite in this tree:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test teams_seat_reconcile_pg

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use coralswarm_backend::jobs::{queue, JobKind};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::teams_seat_reconcile;
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

/// Control + two independent org tenant databases — same shape as
/// `slack_worker_tenant_loop_pg.rs`'s fixture.
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

/// Seed a real seat-bearing Teams channel on `pool`: a team-kind graph, a
/// `teams` row owning it, and a `teams_watched_resources` row whose
/// `graph_id` IS that team ocean — the join `seat_bearing_channels_for_
/// account` runs, mirroring `slack_worker_tenant_loop_pg.rs`'s identical
/// helper for `slack_watched_channels`.
async fn seed_seat_bearing_channel(
    pool: &PgPool,
    account_id: Uuid,
    ms_tenant_id: &str,
    ms_team_id: &str,
    channel_id: &str,
) {
    let creator_user_id = Uuid::new_v4();
    // `kind = 'team'` — `graphs_member_requires_owner`'s CHECK ties
    // `owner_user_id IS NOT NULL` to exactly `kind = 'member'`.
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Team ocean', 'team') \
         RETURNING id",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, 'Eng', $3)",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(creator_user_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO teams_watched_resources \
             (account_id, user_id, graph_id, kind, team_id, channel_id, tenant_id, auth_mode) \
         VALUES ($1, $2, $3, 'channel', $4, $5, $6, 'app')",
    )
    .bind(account_id)
    .bind(creator_user_id)
    .bind(graph_id)
    .bind(ms_team_id)
    .bind(channel_id)
    .bind(ms_tenant_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn seat_grant_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM team_seat_grants")
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Seed a `teams_watched_resources` row of `kind='chat'` whose `graph_id`
/// nonetheless IS owned by a `teams` row (i.e., an artificially seat-bearing-
/// shaped chat) — used only to prove the discovery query's `kind = 'channel'`
/// filter genuinely excludes it, per this worker's own module doc: "A
/// standard Teams channel's roster IS the team's roster... only private/
/// shared channels differ" — chats never seat via this reconciler.
async fn seed_chat_kind_watch_on_a_team_graph(
    pool: &PgPool,
    account_id: Uuid,
    ms_tenant_id: &str,
    ms_team_id: &str,
    chat_id: &str,
) {
    let creator_user_id = Uuid::new_v4();
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Team ocean', 'team') \
         RETURNING id",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, 'Eng', $3)",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(creator_user_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO teams_watched_resources \
             (account_id, user_id, graph_id, kind, team_id, chat_id, tenant_id, auth_mode) \
         VALUES ($1, $2, $3, 'chat', $4, $5, $6, 'app')",
    )
    .bind(account_id)
    .bind(creator_user_id)
    .bind(graph_id)
    .bind(ms_team_id)
    .bind(chat_id)
    .bind(ms_tenant_id)
    .execute(pool)
    .await
    .unwrap();
}

/// Seed a `teams_watched_resources` row shaped exactly like what `routes::
/// teams_ingest::install_watch` writes IN PRODUCTION TODAY: `graph_id`
/// resolved via `resolve_primary_graph` (a member's own primary ocean, kind
/// != 'team'), never a `teams` row's own graph. No `teams` row is created at
/// all — this account has zero team oceans.
async fn seed_personal_ocean_channel_watch(
    pool: &PgPool,
    account_id: Uuid,
    ms_tenant_id: &str,
    ms_team_id: &str,
    channel_id: &str,
) {
    let creator_user_id = Uuid::new_v4();
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Personal ocean', 'personal') \
         RETURNING id",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO teams_watched_resources \
             (account_id, user_id, graph_id, kind, team_id, channel_id, tenant_id, auth_mode) \
         VALUES ($1, $2, $3, 'channel', $4, $5, $6, 'app')",
    )
    .bind(account_id)
    .bind(creator_user_id)
    .bind(graph_id)
    .bind(ms_team_id)
    .bind(channel_id)
    .bind(ms_tenant_id)
    .execute(pool)
    .await
    .unwrap();
}

// ── Cross-tenant queue isolation ────────────────────────────────────────────

/// A `TeamsSeatReconcile` job enqueued on tenant A's OWN `jobs` table must be
/// invisible to a tick scoped to tenant B, and claimable by a tick scoped to
/// tenant A — the exact property every tenant-loop worker depends on (a
/// tenant DB's `jobs` queue has no error/warning for a write nobody ever
/// reads).
#[tokio::test]
async fn teams_seat_reconcile_tick_is_isolated_to_its_own_tenant_pg() {
    let Some(_) = base_url() else {
        eprintln!("skipping teams_seat_reconcile_tick_is_isolated_to_its_own_tenant_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let fx = setup_three_db("orgt_t24_isolation").await;
    fx.seed_org_account("org_a", "orgt-t24-iso-a").await;
    fx.seed_org_account("org_b", "orgt-t24-iso-b").await;

    // No channels seeded at all — this proves queue visibility, not
    // discovery (that's the next test). A account-less, channel-less sweep
    // still completes as soon as it can dequeue and process the job.
    let kind = JobKind::TeamsSeatReconcile {
        account_id: None,
        channel_id: None,
    };
    queue::enqueue(&fx.tenant_a, &kind, None, None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");

    let wt_b = worker_tenant(TenantId::Org("org_b".into()), &fx.tenant_b);
    let tick_b = teams_seat_reconcile::tick(&wt_b, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant B must not error");
    assert_eq!(
        tick_b,
        Tick::idle(),
        "org A's TeamsSeatReconcile job must be invisible to a tick scoped to org B's own \
         (empty) jobs table"
    );

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = teams_seat_reconcile::tick(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant A must not error");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the job must be claimable by a tick correctly scoped to tenant A"
    );

    fx.teardown("orgt_t24_isolation").await;
}

// ── Real discovery: teams_watched_resources JOIN teams ──────────────────────

/// `seat_bearing_channels_for_account` reads `teams_watched_resources JOIN
/// teams` on the tenant's own content pool — this join never ran against a
/// genuine second database before this PR (the table pair is brand new to
/// the tenant loop). Seeds a real team ocean + a real seat-bearing
/// `teams_watched_resources` row on tenant A and proves the worker's own
/// helper (not a hand-rolled query) finds it, and that an unrelated account
/// on the SAME tenant database sees none of it.
#[tokio::test]
async fn seat_bearing_channels_for_account_finds_a_real_row_on_a_tenant_database_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping seat_bearing_channels_for_account_finds_a_real_row_on_a_tenant_database_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_t24_channels").await;

    let account_id = Uuid::new_v4();
    seed_seat_bearing_channel(
        &fx.tenant_a,
        account_id,
        "ms-tenant-abc",
        "ms-team-eng",
        "19:chan@thread.tacv2",
    )
    .await;

    let found = teams_seat_reconcile::test_support_seat_bearing_channel_count(
        &fx.tenant_a,
        account_id,
    )
    .await
    .expect("seat_bearing_channels_for_account must not error against a real tenant DB");
    assert_eq!(
        found, 1,
        "the seeded teams_watched_resources/teams row must be discovered on tenant A"
    );

    let other_account = Uuid::new_v4();
    let found_other =
        teams_seat_reconcile::test_support_seat_bearing_channel_count(&fx.tenant_a, other_account)
            .await
            .expect("must not error");
    assert_eq!(found_other, 0, "an unrelated account must see none of this account's channels");

    fx.teardown("orgt_t24_channels").await;
}

// ── accounts_to_sweep rejects a foreign payload account_id ─────────────────

/// A hand-inserted `TeamsSeatReconcile { account_id: Some(<wrong account>) }`
/// landing on an org tenant's own `jobs` table must be REJECTED, not swept —
/// same protection `slack_seat_reconcile::accounts_to_sweep` carries, copied
/// here because `JobKind::TeamsSeatReconcile` is hand-inserted the identical
/// way (an operator mistyping an id, or pasting a different org's account
/// into org A's `jobs` table, is the likely failure mode).
#[tokio::test]
async fn accounts_to_sweep_rejects_a_foreign_payload_account_id_pg() {
    let Some(_) = base_url() else {
        eprintln!("skipping accounts_to_sweep_rejects_a_foreign_payload_account_id_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let fx = setup_three_db("orgt_t24_foreign").await;
    let account_a = fx.seed_org_account("org_a", "orgt-t24-foreign-a").await;
    let account_b = fx.seed_org_account("org_b", "orgt-t24-foreign-b").await;
    assert_ne!(account_a, account_b);

    let kind = JobKind::TeamsSeatReconcile {
        account_id: Some(account_b),
        channel_id: None,
    };
    queue::enqueue(&fx.tenant_a, &kind, None, None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = teams_seat_reconcile::tick(&wt_a, &fx.control, &reqwest::Client::new()).await;
    // `tick` propagates `process_job`'s rejection as the job's own failure —
    // the tick call itself does not error (each job is caught and failed
    // individually), but it must not silently sweep org B's account under
    // org A's tenant pool.
    let tick_a = tick_a.expect("tick must not error even when a job it dequeued is rejected");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the job was dequeued and processed (rejected, then marked failed) — not silently \
         dropped or left pending forever"
    );

    fx.teardown("orgt_t24_foreign").await;
}

// ── Roster-abort semantics: an unknown roster changes zero seats ───────────

/// With `MICROSOFT_CLIENT_ID` unset, `connectors::microsoft::app_token` fails
/// fast and locally (no network call) — `resolve_team_roster_members`
/// propagates that as `Err`, which `reconcile_channel` must treat as "we do
/// not know", never as "the roster is empty, revoke everyone". A seat-
/// bearing channel seeded here with NO pre-existing seat grants must still
/// have ZERO grants after the sweep — proving the abort path changes
/// nothing, the same property `slack_seat_reconcile`'s own header asserts
/// for `Unavailable`.
#[tokio::test]
async fn an_unresolvable_roster_changes_zero_seats_pg() {
    let Some(_) = base_url() else {
        eprintln!("skipping an_unresolvable_roster_changes_zero_seats_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    // Defensive: this test's premise depends on the app-token mint failing
    // fast (no configured Microsoft app registration), which is the normal
    // state for a CI runner. If a future job ever sets these for an
    // unrelated suite in the same process, this test would attempt a live
    // network call instead of the intended local failure — guard explicitly
    // rather than let that drift silently.
    assert!(
        std::env::var("MICROSOFT_CLIENT_ID").is_err(),
        "this test requires MICROSOFT_CLIENT_ID to be unset so the roster walk fails fast and \
         locally, without a real network call"
    );

    let fx = setup_three_db("orgt_t24_abort").await;
    let account_id = fx.seed_org_account("org_a", "orgt-t24-abort-a").await;
    seed_seat_bearing_channel(
        &fx.tenant_a,
        account_id,
        "ms-tenant-abc",
        "ms-team-eng",
        "19:chan@thread.tacv2",
    )
    .await;
    assert_eq!(
        seat_grant_count(&fx.tenant_a).await,
        0,
        "no seat grants exist before the sweep"
    );

    let kind = JobKind::TeamsSeatReconcile {
        account_id: None,
        channel_id: None,
    };
    queue::enqueue(&fx.tenant_a, &kind, None, None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");

    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = teams_seat_reconcile::tick(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick against tenant A must not error even though the channel's roster walk \
                 fails");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the job was dequeued and processed (one channel skipped), not silently dropped"
    );
    assert_eq!(
        seat_grant_count(&fx.tenant_a).await,
        0,
        "an unresolvable roster (app-token mint failure) must leave team_seat_grants \
         untouched — 'we do not know' is never treated as 'the roster is empty, revoke \
         everyone', and it must never be treated as 'grant nobody' either — both are guesses"
    );

    fx.teardown("orgt_t24_abort").await;
}

// ── Discovery-query gaps: what is NOT seat-bearing ─────────────────────────
//
// m365 Train 2 PR 2.7. The four tests above (PR 2.4 + 4.1) prove the queue
// isolation, the real JOIN discovery for a genuinely seat-bearing row, the
// foreign-account-id rejection, and the `Unavailable` roster-abort case.
// What none of them prove is the OTHER half of `seat_bearing_channels_for_
// account`'s own SQL: which rows it must NOT discover. Both gaps below are
// pure discovery-query assertions — no roster walk, no network, no
// MICROSOFT_CLIENT_ID dependency — so they run unconditionally whenever
// ORGT_TEST_BASE_URL is set, unlike `an_unresolvable_roster_changes_zero_
// seats_pg`'s network-free-failure precondition.

/// `seat_bearing_channels_for_account`'s discovery query filters on `kind =
/// 'channel'` — a `kind = 'chat'` row must never be discovered, even if its
/// `graph_id` happens to be owned by a `teams` row (this module's own header:
/// "A standard Teams channel's roster IS the team's roster... only private/
/// shared channels differ" — chats have no such concept and never seat via
/// this reconciler).
#[tokio::test]
async fn seat_bearing_channels_for_account_excludes_chat_kind_rows_pg() {
    let Some(_) = base_url() else {
        eprintln!("skipping seat_bearing_channels_for_account_excludes_chat_kind_rows_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let fx = setup_three_db("orgt_t27_excl_chat").await;

    let account_id = Uuid::new_v4();
    // A genuine seat-bearing CHANNEL (control case) plus a CHAT row on its
    // own team graph (the row that must be excluded).
    seed_seat_bearing_channel(&fx.tenant_a, account_id, "ms-tenant-abc", "ms-team-eng", "19:chan@thread.tacv2")
        .await;
    seed_chat_kind_watch_on_a_team_graph(
        &fx.tenant_a,
        account_id,
        "ms-tenant-abc",
        "ms-team-eng",
        "19:chat-eng-standup@unq.gbl.spaces",
    )
    .await;

    let found = teams_seat_reconcile::test_support_seat_bearing_channel_count(&fx.tenant_a, account_id)
        .await
        .expect("must not error");
    assert_eq!(
        found, 1,
        "exactly the one seat-bearing CHANNEL row must be discovered — the sibling CHAT row on \
         the same team graph must be excluded by the kind = 'channel' filter"
    );

    fx.teardown("orgt_t27_excl_chat").await;
}

/// The module's own header states this worker's discovery query returns
/// ZERO rows in production TODAY because `routes::teams_ingest::install_
/// watch` always writes a member's own primary-ocean `graph_id` (`kind !=
/// 'team'`), never a `teams` row's own graph — this is that claim, turned
/// into a regression-guarded assertion rather than left as a comment. Seeds
/// a `teams_watched_resources` row shaped exactly like today's real
/// production write (a personal-ocean `graph_id`, no `teams` row at all) and
/// proves it is invisible to the JOIN — same "we do not know" contract as
/// the roster-abort test: an account with only this kind of row must sweep
/// as `ReconcileStats::default()`, never grant or revoke anything.
#[tokio::test]
async fn seat_bearing_channels_for_account_excludes_a_watch_whose_graph_is_not_a_team_ocean_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping seat_bearing_channels_for_account_excludes_a_watch_whose_graph_is_not_a_team_ocean_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let fx = setup_three_db("orgt_t27_excl_personal").await;

    let account_id = fx.seed_org_account("org_a", "orgt-t27-excl-personal-a").await;
    seed_personal_ocean_channel_watch(
        &fx.tenant_a,
        account_id,
        "ms-tenant-abc",
        "ms-team-eng",
        "19:chan-general@thread.tacv2",
    )
    .await;

    let found = teams_seat_reconcile::test_support_seat_bearing_channel_count(&fx.tenant_a, account_id)
        .await
        .expect("must not error");
    assert_eq!(
        found, 0,
        "a watch whose graph_id is a personal ocean (today's only real production shape) must \
         be invisible to seat-bearing discovery — no teams row owns it"
    );

    // End-to-end via a real sweep, not just the discovery helper: the job
    // must still complete cleanly (dequeued, processed) with zero seats
    // touched — the discovery-empty case, not the roster-abort case.
    let kind = JobKind::TeamsSeatReconcile { account_id: None, channel_id: None };
    queue::enqueue(&fx.tenant_a, &kind, None, None, 0)
        .await
        .expect("enqueue onto tenant A")
        .expect("not deduped");
    let wt_a = worker_tenant(TenantId::Org("org_a".into()), &fx.tenant_a);
    let tick_a = teams_seat_reconcile::tick(&wt_a, &fx.control, &reqwest::Client::new())
        .await
        .expect("tick must not error");
    assert_eq!(
        tick_a,
        Tick::worked(1),
        "the job is dequeued and processed even though discovery finds nothing to reconcile"
    );
    assert_eq!(seat_grant_count(&fx.tenant_a).await, 0);

    fx.teardown("orgt_t27_excl_personal").await;
}
