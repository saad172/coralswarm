//! `workers::platform_rollup` — live-Postgres proof (platform-admin-metrics
//! PR 5 of 7, migration `0095_platform_metrics` + this PR's rollup worker).
//!
//! Scratch-DB pattern shared by every `*_pg.rs` file in this directory
//! (`worker_harness_pg.rs` is the closest sibling for the tenant-pool half;
//! `access_log_purge_pg.rs` for the control-tree-only half).
//!
//! Proves, against real databases rather than in-process:
//!   1. **Consumer plane** — `tick` sums atom/reef/session counts and
//!      `llm_usage` tokens ACROSS every account into the single
//!      `tenant = '__consumer__'` row, correctly excludes rows outside the
//!      rollup day, and deduplicates a user active in TWO accounts down to
//!      one `platform_active_users` row per source (the whole point of that
//!      table's PK grain — see migration `0095`'s header).
//!   2. **Org tenant plane** — `tick` computes the same shape of row directly
//!      against a tenant DB (no per-account loop — whole-database
//!      isolation), and never writes an `access`-source row there (no
//!      `user_access_log` on that plane).
//!   3. **Idempotency** — a second same-day tick upserts `computed_at`
//!      without duplicating active-user rows or double-counting totals.
//!   4. **Resilience** — a tick against an unreachable pool returns `Err`
//!      (never panics), for both the consumer and org branches.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same convention as every other `*_pg.rs` file here.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test platform_rollup_pg -- --test-threads=1 --nocapture

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::platform_rollup::tick;
use coralswarm_backend::workers::tenant_harness::{Tick, WorkerTenant};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

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
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
}
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new().max_connections(8).connect(&with_db(base, db)).await.expect("connect control");
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}
async fn tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
    let pool = PgPoolOptions::new().max_connections(8).connect(&with_db(base, db)).await.expect("connect tenant");
    sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&pool)
        .await
        .unwrap();
    pool
}

// ── Seed helpers ──────────────────────────────────────────────────────────────

async fn mint_user(pool: &PgPool, email: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(pool)
        .await
        .expect("insert user")
}
async fn mint_account(pool: &PgPool, owner: Uuid) -> Uuid {
    sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
        .bind(owner)
        .fetch_one(pool)
        .await
        .expect("insert account")
}
/// A SECOND account for the SAME owner — `accounts` only allows one PERSONAL
/// account per owner (`uq_accounts_one_personal_per_owner`, migration
/// `0028_accounts_owner_partial_unique`, `WHERE clerk_org_id IS NULL`), so
/// this mints an org-shaped account (`clerk_org_id` set) to test a user
/// active across two accounts without tripping that constraint.
async fn mint_second_account_for_owner(pool: &PgPool, owner: Uuid, clerk_org_id: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
        .bind(owner)
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .expect("insert account")
}
async fn mint_graph(pool: &PgPool, account_id: Uuid, name: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, $2) RETURNING id")
        .bind(account_id)
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("insert graph")
}
async fn backdate_created_at(pool: &PgPool, table: &str, id_col: &str, id: Uuid, when: chrono::DateTime<Utc>) {
    sqlx::query(&format!("UPDATE {table} SET created_at = $2 WHERE {id_col} = $1"))
        .bind(id)
        .bind(when)
        .execute(pool)
        .await
        .expect("backdate row");
}

async fn mint_atom(pool: &PgPool, graph_id: Uuid, source_id: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \"timestamp\") \
         VALUES ($1, 'test', $2, 'blob-key', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(pool)
    .await
    .expect("insert atom")
}
async fn mint_conversation(pool: &PgPool, account_id: Uuid, graph_id: Uuid, user_id: Uuid) -> Uuid {
    sqlx::query_scalar("INSERT INTO conversations (account_id, graph_id, user_id) VALUES ($1, $2, $3) RETURNING id")
        .bind(account_id)
        .bind(graph_id)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .expect("insert conversation")
}
async fn mint_llm_usage(pool: &PgPool, account_id: Uuid, input_tokens: i32, output_tokens: i32) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO llm_usage (account_id, task, model, input_tokens, output_tokens) \
         VALUES ($1, 'test', 'test-model', $2, $3) RETURNING id",
    )
    .bind(account_id)
    .bind(input_tokens)
    .bind(output_tokens)
    .fetch_one(pool)
    .await
    .expect("insert llm_usage")
}
/// [`mint_llm_usage`] plus explicit Anthropic prompt-cache token columns
/// (migration `0073_llm_usage_cache_tokens.sql`) — a separate helper rather
/// than widening `mint_llm_usage`'s signature so every existing call site
/// above (which doesn't care about cache tokens) stays untouched.
async fn mint_llm_usage_with_cache(
    pool: &PgPool,
    account_id: Uuid,
    input_tokens: i32,
    output_tokens: i32,
    cache_creation_tokens: i64,
    cache_read_tokens: i64,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO llm_usage \
           (account_id, task, model, input_tokens, output_tokens, \
            cache_creation_input_tokens, cache_read_input_tokens) \
         VALUES ($1, 'test', 'test-model', $2, $3, $4, $5) RETURNING id",
    )
    .bind(account_id)
    .bind(input_tokens)
    .bind(output_tokens)
    .bind(cache_creation_tokens)
    .bind(cache_read_tokens)
    .fetch_one(pool)
    .await
    .expect("insert llm_usage with cache tokens")
}
async fn mint_activity_event(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO activity_events (account_id, user_id, event_type) VALUES ($1, $2, 'atom_created') RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(pool)
    .await
    .expect("insert activity_event")
}
async fn mint_query(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Uuid {
    sqlx::query_scalar("INSERT INTO queries (account_id, user_id, text) VALUES ($1, $2, 'hello') RETURNING id")
        .bind(account_id)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .expect("insert query")
}
async fn mint_mcp_call(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO mcp_tool_calls (account_id, user_id, tool, outcome) VALUES ($1, $2, 'ask', 'ok') RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(pool)
    .await
    .expect("insert mcp_tool_calls")
}
async fn insert_access_log(pool: &PgPool, user_id: Uuid, session_id: &str) {
    sqlx::query(
        "INSERT INTO user_access_log (user_id, clerk_session_id) VALUES ($1, $2) \
         ON CONFLICT (user_id, clerk_session_id) DO NOTHING",
    )
    .bind(user_id)
    .bind(session_id)
    .execute(pool)
    .await
    .expect("insert user_access_log");
}

// ── Assertion helpers ────────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
struct MetricsRow {
    atom_count: Option<i64>,
    reef_count: Option<i64>,
    session_count: Option<i64>,
    llm_input_tokens: Option<i64>,
    llm_output_tokens: Option<i64>,
    // Migration `0123_platform_metrics_daily_cache_tokens.sql` — nullable,
    // no default (NULL = "not (re)computed since this migration shipped",
    // distinct from a real computed 0). `Option<i64>` here lets tests assert
    // on that distinction directly rather than only ever seeing an
    // already-coalesced value.
    llm_cache_creation_tokens: Option<i64>,
    llm_cache_read_tokens: Option<i64>,
}
async fn fetch_metrics_row(pool: &PgPool, day: chrono::NaiveDate, tenant: &str) -> Option<MetricsRow> {
    sqlx::query_as::<_, (Option<i64>, Option<i64>, Option<i64>, Option<i64>, Option<i64>, Option<i64>, Option<i64>)>(
        "SELECT atom_count, reef_count, session_count, llm_input_tokens, llm_output_tokens, \
                llm_cache_creation_tokens, llm_cache_read_tokens \
         FROM platform_metrics_daily WHERE day = $1 AND tenant = $2",
    )
    .bind(day)
    .bind(tenant)
    .fetch_optional(pool)
    .await
    .expect("query platform_metrics_daily")
    .map(
        |(
            atom_count,
            reef_count,
            session_count,
            llm_input_tokens,
            llm_output_tokens,
            llm_cache_creation_tokens,
            llm_cache_read_tokens,
        )| MetricsRow {
            atom_count,
            reef_count,
            session_count,
            llm_input_tokens,
            llm_output_tokens,
            llm_cache_creation_tokens,
            llm_cache_read_tokens,
        },
    )
}
async fn fetch_computed_at(pool: &PgPool, day: chrono::NaiveDate, tenant: &str) -> chrono::DateTime<Utc> {
    sqlx::query_scalar("SELECT computed_at FROM platform_metrics_daily WHERE day = $1 AND tenant = $2")
        .bind(day)
        .bind(tenant)
        .fetch_one(pool)
        .await
        .expect("fetch computed_at")
}
async fn fetch_active_users(pool: &PgPool, day: chrono::NaiveDate, tenant: &str, source: &str) -> Vec<Uuid> {
    let mut rows: Vec<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM platform_active_users WHERE day = $1 AND tenant = $2 AND source = $3 ORDER BY user_id",
    )
    .bind(day)
    .bind(tenant)
    .bind(source)
    .fetch_all(pool)
    .await
    .expect("query platform_active_users");
    rows.sort();
    rows
}

fn sorted(mut v: Vec<Uuid>) -> Vec<Uuid> {
    v.sort();
    v
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn consumer_rollup_sums_across_accounts_excludes_other_days_and_dedupes_users() {
    let Some(base) = base_url() else {
        eprintln!("skipping consumer_rollup_sums_across_accounts_excludes_other_days_and_dedupes_users: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_pr1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let today = Utc::now();
    let yesterday = today - ChronoDuration::days(1);

    // ── Account A: user_a owns it, plus a second account (C) also owned by
    // user_a — proves cross-account dedup of the SAME user.
    let user_a = mint_user(&control, &format!("{}@a.test", Uuid::new_v4())).await;
    let user_b = mint_user(&control, &format!("{}@b.test", Uuid::new_v4())).await;
    let acct_a = mint_account(&control, user_a).await;
    let acct_b = mint_account(&control, user_b).await;
    let acct_c = mint_second_account_for_owner(&control, user_a, "org_pr1_c").await; // same owner as A

    let graph_a = mint_graph(&control, acct_a, "graph-a").await;
    let graph_b = mint_graph(&control, acct_b, "graph-b").await;
    // A second graph for account A, backdated — must NOT count toward
    // today's reef_count.
    let graph_a_old = mint_graph(&control, acct_a, "graph-a-old").await;
    backdate_created_at(&control, "graphs", "id", graph_a_old, yesterday).await;

    // Atoms: 2 in graph_a today, 1 in graph_b today, 1 in graph_a backdated
    // (must be excluded from today's atom_count).
    mint_atom(&control, graph_a, "atom-1").await;
    mint_atom(&control, graph_a, "atom-2").await;
    mint_atom(&control, graph_b, "atom-3").await;
    let old_atom = mint_atom(&control, graph_a, "atom-old").await;
    backdate_created_at(&control, "atoms", "atom_id", old_atom, yesterday).await;

    // Conversations ("sessions"): one per account today, one backdated for A.
    mint_conversation(&control, acct_a, graph_a, user_a).await;
    mint_conversation(&control, acct_b, graph_b, user_b).await;
    let old_convo = mint_conversation(&control, acct_a, graph_a, user_a).await;
    backdate_created_at(&control, "conversations", "id", old_convo, yesterday).await;

    // llm_usage: sums today, one backdated row excluded.
    mint_llm_usage(&control, acct_a, 100, 50).await;
    mint_llm_usage(&control, acct_b, 30, 10).await;
    let old_usage = mint_llm_usage(&control, acct_a, 999, 999).await;
    backdate_created_at(&control, "llm_usage", "id", old_usage, yesterday).await;

    // Activity / query / mcp: user_a (via A) and user_b (via B) active today;
    // user_a ALSO active via account C today — must dedupe to one row.
    mint_activity_event(&control, acct_a, user_a).await;
    mint_activity_event(&control, acct_b, user_b).await;
    mint_activity_event(&control, acct_c, user_a).await;
    mint_query(&control, acct_a, user_a).await;
    mint_query(&control, acct_b, user_b).await;
    mint_mcp_call(&control, acct_a, user_a).await;
    mint_mcp_call(&control, acct_b, user_b).await;

    // Login (access source): user_a only.
    insert_access_log(&control, user_a, "sess-1").await;

    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(control.clone()));
    let wt = WorkerTenant { tenant: TenantId::Consumer, pool: control.clone(), store };
    let outcome = tick(&wt, &control).await.expect("consumer tick succeeds");
    assert_ne!(outcome, Tick::idle(), "a day with real activity must not report idle");

    let day = today.date_naive();
    let row = fetch_metrics_row(&control, day, "__consumer__").await.expect("consumer row written");
    assert_eq!(row.atom_count, Some(3), "2 in graph_a + 1 in graph_b, excluding the backdated atom");
    assert_eq!(row.reef_count, Some(2), "graph_a + graph_b, excluding the backdated graph_a_old");
    assert_eq!(row.session_count, Some(2), "one conversation per account today, excluding the backdated one");
    assert_eq!(row.llm_input_tokens, Some(130), "100 (A) + 30 (B), excluding the backdated 999");
    assert_eq!(row.llm_output_tokens, Some(60), "50 (A) + 10 (B), excluding the backdated 999");

    let activity_users = fetch_active_users(&control, day, "__consumer__", "activity").await;
    assert_eq!(activity_users, sorted(vec![user_a, user_b]), "user_a deduped across accounts A and C");

    let query_users = fetch_active_users(&control, day, "__consumer__", "query").await;
    assert_eq!(query_users, sorted(vec![user_a, user_b]));

    let mcp_users = fetch_active_users(&control, day, "__consumer__", "mcp").await;
    assert_eq!(mcp_users, sorted(vec![user_a, user_b]));

    let access_users = fetch_active_users(&control, day, "__consumer__", "access").await;
    assert_eq!(access_users, vec![user_a], "only user_a logged in");

    // ── Idempotency: a second same-day tick upserts computed_at, doesn't
    // change totals, and doesn't duplicate active-user rows.
    let computed_at_1 = fetch_computed_at(&control, day, "__consumer__").await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let outcome2 = tick(&wt, &control).await.expect("second consumer tick succeeds");
    assert_ne!(outcome2, Tick::idle());
    let computed_at_2 = fetch_computed_at(&control, day, "__consumer__").await;
    assert!(computed_at_2 > computed_at_1, "computed_at must advance on a re-run");

    let row_again = fetch_metrics_row(&control, day, "__consumer__").await.expect("row still present");
    assert_eq!(row_again.atom_count, row.atom_count, "totals unchanged by a same-day re-run");
    let activity_users_again = fetch_active_users(&control, day, "__consumer__", "activity").await;
    assert_eq!(activity_users_again, activity_users, "no duplicate active-user rows on a re-run");

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

#[tokio::test]
async fn org_tenant_rollup_reads_the_whole_tenant_db_with_no_account_loop() {
    let Some(base) = base_url() else {
        eprintln!("skipping org_tenant_rollup_reads_the_whole_tenant_db_with_no_account_loop: set ORGT_TEST_BASE_URL");
        return;
    };
    let (ctl, db_t) = ("orgt_pr2_ctl", "orgt_pr2_t");
    let mut conn = admin(&base).await;
    for db in [ctl, db_t] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    // A control-plane DB IS needed here, even though `org_tick` COLLECTS
    // stats only from the tenant pool: `platform_metrics_daily`/
    // `platform_active_users` are control-only tables with no tenant-tree
    // counterpart at all, so the rollup ROW this tick writes lands on the
    // control pool, not the tenant one — see `workers::platform_rollup::tick`'s
    // doc comment ("Where reads happen vs. where writes land").
    let control = control_pool(&base, ctl).await;
    let org_id = "org_pr2_t";
    let tenant = tenant_pool(&base, db_t, org_id).await;

    let today = Utc::now();
    let yesterday = today - ChronoDuration::days(1);

    // A tenant DB has NO `accounts` table concept to loop over — every
    // content table is queried directly. Use a fixed account_id/graph_id
    // pair the tenant tree's own tables carry (whole-database isolation).
    let account_id = Uuid::new_v4();
    let user_1 = Uuid::new_v4();
    let user_2 = Uuid::new_v4();

    let graph_id: Uuid =
        sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, 'org-graph') RETURNING id")
            .bind(account_id)
            .fetch_one(&tenant)
            .await
            .unwrap();

    mint_atom(&tenant, graph_id, "org-atom-1").await;
    mint_atom(&tenant, graph_id, "org-atom-2").await;
    let old_atom = mint_atom(&tenant, graph_id, "org-atom-old").await;
    backdate_created_at(&tenant, "atoms", "atom_id", old_atom, yesterday).await;

    mint_conversation(&tenant, account_id, graph_id, user_1).await;
    mint_conversation(&tenant, account_id, graph_id, user_2).await;

    mint_llm_usage(&tenant, account_id, 40, 20).await;
    mint_llm_usage(&tenant, account_id, 10, 5).await;

    mint_activity_event(&tenant, account_id, user_1).await;
    mint_activity_event(&tenant, account_id, user_2).await;
    mint_query(&tenant, account_id, user_1).await;
    mint_mcp_call(&tenant, account_id, user_2).await;

    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(tenant.clone()));
    let wt = WorkerTenant { tenant: TenantId::Org(org_id.to_string()), pool: tenant.clone(), store };
    let outcome = tick(&wt, &control).await.expect("org tenant tick succeeds");
    assert_ne!(outcome, Tick::idle());

    let day = today.date_naive();
    // The rollup ROW lives on the CONTROL pool (see this test's setup
    // comment), even though every input above was seeded on `tenant`.
    let row = fetch_metrics_row(&control, day, org_id).await.expect("org tenant row written to the control pool");
    assert_eq!(row.atom_count, Some(2), "2 today, excluding the backdated atom");
    assert_eq!(row.reef_count, Some(1));
    assert_eq!(row.session_count, Some(2));
    assert_eq!(row.llm_input_tokens, Some(50));
    assert_eq!(row.llm_output_tokens, Some(25));

    let activity_users = fetch_active_users(&control, day, org_id, "activity").await;
    assert_eq!(activity_users, sorted(vec![user_1, user_2]));
    let query_users = fetch_active_users(&control, day, org_id, "query").await;
    assert_eq!(query_users, vec![user_1]);
    let mcp_users = fetch_active_users(&control, day, org_id, "mcp").await;
    assert_eq!(mcp_users, vec![user_2]);

    // No `access` source on the org tenant plane at all (no
    // `user_access_log` there — see the module doc).
    let access_users = fetch_active_users(&control, day, org_id, "access").await;
    assert!(access_users.is_empty(), "org tenant plane never writes an access-source row");

    for db in [db_t, ctl] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

/// Cache-token rollup (migration `0123_platform_metrics_daily_cache_tokens.
/// sql`), for both planes, against real Postgres — the gap PR that added
/// `workers::platform_rollup`'s cache-summing queries left untested (the two
/// tests above never seeded or asserted on cache tokens at all, so they
/// couldn't have caught a broken `SUM(bigint)`-without-`::bigint` decode
/// failure the way PR #509's own `admin_spend` fix originally shipped one).
/// Proves three things:
///   1. Consumer plane sums `cache_creation_input_tokens`/
///      `cache_read_input_tokens` across accounts into the rollup row,
///      same as it already does for plain input/output tokens.
///   2. Org tenant plane does the same from its own tenant DB.
///   3. NULL vs. a real computed 0 is the distinction the migration's doc
///      comment promises: a `(day, tenant)` row this worker has never
///      (re)written has NULL cache columns even though `llm_input_tokens`/
///      `llm_output_tokens` are populated (simulating a row computed by a
///      pre-migration build of this worker) — and a `tick` run against that
///      SAME day turns those NULLs into real, non-null sums.
#[tokio::test]
async fn rollup_sums_cache_tokens_and_null_means_not_yet_recomputed() {
    let Some(base) = base_url() else {
        eprintln!("skipping rollup_sums_cache_tokens_and_null_means_not_yet_recomputed: set ORGT_TEST_BASE_URL");
        return;
    };
    let (ctl, db_t) = ("orgt_pr4_ctl", "orgt_pr4_t");
    let mut conn = admin(&base).await;
    for db in [ctl, db_t] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = "org_pr4_t";
    let tenant = tenant_pool(&base, db_t, org_id).await;

    let today = Utc::now();
    let day = today.date_naive();

    // ── Consumer plane: two accounts, cache tokens on both. ─────────────────
    let user_a = mint_user(&control, &format!("{}@a.test", Uuid::new_v4())).await;
    let user_b = mint_user(&control, &format!("{}@b.test", Uuid::new_v4())).await;
    let acct_a = mint_account(&control, user_a).await;
    let acct_b = mint_account(&control, user_b).await;
    mint_llm_usage_with_cache(&control, acct_a, 100, 50, 40, 10).await;
    mint_llm_usage_with_cache(&control, acct_b, 30, 10, 5, 15).await;

    // A pre-existing row for TODAY that simulates output from a
    // pre-migration-0123 build of this worker: plain tokens populated,
    // cache columns never written (raw SQL, bypassing `upsert_metrics_daily`
    // entirely, since that function ALWAYS writes real cache sums now — the
    // whole point of this row is to model code that predates it).
    sqlx::query(
        "INSERT INTO platform_metrics_daily \
           (day, tenant, atom_count, reef_count, session_count, llm_input_tokens, llm_output_tokens) \
         VALUES ($1, '__consumer__', 0, 0, 0, 999, 999)",
    )
    .bind(day)
    .execute(&control)
    .await
    .expect("seed pre-cache-aware consumer row");
    let pre_row = fetch_metrics_row(&control, day, "__consumer__").await.expect("pre-tick row present");
    assert_eq!(pre_row.llm_cache_creation_tokens, None, "never written by this worker yet");
    assert_eq!(pre_row.llm_cache_read_tokens, None);

    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(control.clone()));
    let wt = WorkerTenant { tenant: TenantId::Consumer, pool: control.clone(), store };
    tick(&wt, &control).await.expect("consumer tick succeeds");

    let row = fetch_metrics_row(&control, day, "__consumer__").await.expect("consumer row written");
    assert_eq!(row.llm_input_tokens, Some(130), "100 (A) + 30 (B) — this tick's own real recompute, not the seeded 999");
    assert_eq!(row.llm_cache_creation_tokens, Some(45), "40 (A) + 5 (B)");
    assert_eq!(row.llm_cache_read_tokens, Some(25), "10 (A) + 15 (B)");

    // ── Org tenant plane: same shape, seeded on the tenant DB. ──────────────
    let account_id = Uuid::new_v4();
    mint_llm_usage_with_cache(&tenant, account_id, 40, 20, 8, 2).await;
    mint_llm_usage_with_cache(&tenant, account_id, 10, 5, 3, 7).await;

    let org_store: Arc<dyn Store> = Arc::new(PostgresStore::new(tenant.clone()));
    let org_wt = WorkerTenant { tenant: TenantId::Org(org_id.to_string()), pool: tenant.clone(), store: org_store };
    tick(&org_wt, &control).await.expect("org tenant tick succeeds");

    let org_row = fetch_metrics_row(&control, day, org_id).await.expect("org tenant row written to the control pool");
    assert_eq!(org_row.llm_input_tokens, Some(50));
    assert_eq!(org_row.llm_cache_creation_tokens, Some(11), "8 + 3");
    assert_eq!(org_row.llm_cache_read_tokens, Some(9), "2 + 7");

    for db in [db_t, ctl] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

/// Resilience: a tick against an unreachable pool returns `Err`, never
/// panics — the shape `workers::tenant_harness::run_multi_tenant` relies on
/// to isolate one bad tenant from the rest of the run (see the module doc's
/// "Resilience" section; the harness's OWN per-tenant isolation is already
/// proven generically in `worker_harness_pg.rs` for any tick fn — this test
/// only proves THIS worker's tick surfaces a proper `Err` rather than
/// panicking when its pool is unusable).
#[tokio::test]
async fn tick_returns_err_not_panic_when_pool_is_unavailable() {
    let Some(base) = base_url() else {
        eprintln!("skipping tick_returns_err_not_panic_when_pool_is_unavailable: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_pr3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;
    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(control.clone()));

    control.close().await;

    let consumer_wt = WorkerTenant { tenant: TenantId::Consumer, pool: control.clone(), store: store.clone() };
    let consumer_result = tick(&consumer_wt, &control).await;
    assert!(consumer_result.is_err(), "consumer tick on a closed pool must error, not panic");

    let org_wt = WorkerTenant { tenant: TenantId::Org("org_pr3_closed".to_string()), pool: control.clone(), store };
    let org_result = tick(&org_wt, &control).await;
    assert!(org_result.is_err(), "org tick on a closed pool must error, not panic");

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}
