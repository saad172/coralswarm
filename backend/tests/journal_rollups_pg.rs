//! Integration tests for the `journal_rollups` store layer (Today-journal
//! PR A; migration `0113_journal_rollups`/`0024_journal_rollups`).
//!
//! Covers:
//!   1. `upsert_journal_rollup` — insert, then a second call on the same
//!      natural key (`user_id, graph_id, hour_start`) refreshes the row and
//!      increments `recompile_count` (never resets it).
//!   2. `journal_hour_inputs` — gathers exactly the hour's atoms (authored by
//!      the given user, `[hour_start, hour_start + 1h)`) plus that hour's
//!      `activity_events` AND `query_history` rows ("asks", hourly-
//!      completeness audit gaps #4/#5 — NOT `activity_events`' `query_run`
//!      type), with the provenance header stripped and the excerpt
//!      truncated.
//!   3. `journal_dirty_hours` — finds hours with no rollup yet, finds hours
//!      whose stored fingerprint no longer matches their current inputs
//!      (both a count change and a max-created-at change), and skips hours
//!      that are already up to date. The fingerprint is derived over the
//!      UNION of `atoms`, `activity_events`, AND `query_history`: an hour
//!      with rows in only ONE of the three is still a candidate, and a
//!      change confined to any one of them in an already-compiled hour
//!      (including a NEW ask with no new/deleted atom or event) marks it
//!      dirty again.
//!   4. `list_journal_rollups` — respects `[from, to)` and the `graph_ids`
//!      filter, AND is genuinely RLS-isolated: run under a REAL
//!      `coralswarm_app` login (not `SET ROLE` in the owner's session) with
//!      `app.account_id` scoped to account A, a rollup that indisputably
//!      exists on account B (seeded via the BYPASSRLS owner pool, so its
//!      presence in the table is proven) must never come back — this is
//!      the actual isolation proof, not a vacuous "we didn't seed anything
//!      for B" check.
//!   5. `journal_range_aggregate` (Today-journal PR C) — groups atoms into
//!      blocks by (source_type, natural grouping key), resolves a coding-
//!      session block's title from `coding_sessions.title` and falls back to
//!      a grouped atom's own `atoms.title` for everything else, counts
//!      `query_history` rows as `asks`, respects `[from, to)`/`graph_ids`,
//!      and is RLS-isolated across accounts under the same real
//!      `coralswarm_app` login proof as (4). See `journal_endpoint_pg.rs` for
//!      the full HTTP-route-level coverage (span validation, per-MEMBER — not
//!      merely per-account — isolation, unreachable-graph filtering).
//!   6. `workers::journal_sweep::tick` / `sweep_pass` switches: the sweep runs
//!      on `managed_enrichment` (source-agnostic plan PR 6) and is idle,
//!      touching no connection and calling no model, while
//!      `ENRICHMENT_PROCESSING_ENABLED` is not `true` or `JOURNAL_SWEEP_ENABLED`
//!      is `false`. The managed compile itself (receipts, stale inputs, cap,
//!      daily bound, single billing) is proven in
//!      `managed::gate::pg_tests::enrichment_consumers`.
//!
//! Follows the `consumer_rls_pg.rs` / `store_scoped_atoms_nodes_graphs_pg.rs`
//! scratch-DB pattern. Gated on `ORGT_TEST_BASE_URL`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test journal_rollups_pg

mod support;

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use coralswarm_backend::llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent};
use coralswarm_backend::store::{journal_input_fingerprint, PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::journal_sweep::{journal_epoch, sweep_pass, tick};
use coralswarm_backend::workers::tenant_harness::{Tick, WorkerTenant};
use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use support::app_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
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

/// A pool connected AS `coralswarm_app` (real login, not `SET ROLE`).
async fn app_role_pool(db_url: &str) -> PgPool {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_app")
        .password(APP_ROLE_TEST_PASSWORD);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .expect("connect AS coralswarm_app")
}

struct Seeded {
    user_id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
}

async fn seed_account(owner: &PgPool, email: &str) -> Seeded {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(owner)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .expect("insert account");
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'home') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert graph");
    Seeded { user_id, account_id, graph_id }
}

/// Set one seeded user's `users.timezone` (migration `0118_users_timezone`)
/// — the per-user tier-boundary source `workers::journal_sweep::
/// resolve_user_tz` now reads FIRST, ahead of `JOURNAL_TZ`/UTC. Tier tests
/// that assert specific local-day/week boundaries call this so the assertion
/// is proven against the REAL per-user lookup path, not an env/default
/// fallback that happens to agree.
async fn set_user_timezone(owner: &PgPool, user_id: Uuid, tz: &str) {
    sqlx::query("UPDATE users SET timezone = $2 WHERE id = $1")
        .bind(user_id)
        .bind(tz)
        .execute(owner)
        .await
        .expect("set user timezone");
}

/// Pins `JOURNAL_EPOCH` to [`coralswarm_backend::workers::journal_sweep::
/// DEFAULT_JOURNAL_EPOCH`] for the duration of the guard, restoring whatever
/// was there before on drop (CodeRabbit round 1: the `sweep_pass` admission
/// test below uses hardcoded Aug 2026 fixture dates that are safely after the
/// DEFAULT epoch, but nothing pinned that against a real environment's
/// `JOURNAL_EPOCH` override happening to be set later than those fixtures at
/// test-run time — which would silently exclude every fixture hour from
/// `sweep_pass`'s own epoch floor and make its control assertion fail for a
/// reason that has nothing to do with the code under test).
/// `America/Chicago`/`UTC` (the tz side, `journal_tz`'s own fallback) is NOT
/// a similarly unpinned dependency here — that test sets its user's
/// `users.timezone` explicitly via [`set_user_timezone`], so it never falls
/// through to `journal_tz`'s env/UTC fallback chain at all.
struct JournalEpochGuard {
    prev: Option<String>,
}

impl JournalEpochGuard {
    fn pin_default() -> Self {
        let prev = std::env::var("JOURNAL_EPOCH").ok();
        std::env::set_var("JOURNAL_EPOCH", coralswarm_backend::workers::journal_sweep::DEFAULT_JOURNAL_EPOCH);
        JournalEpochGuard { prev }
    }
}

impl Drop for JournalEpochGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var("JOURNAL_EPOCH", v),
            None => std::env::remove_var("JOURNAL_EPOCH"),
        }
    }
}

/// Insert one atom, returning its id. `header_len`/`content_inline` let
/// callers exercise the provenance-header-stripping path in
/// `journal_hour_inputs`.
#[allow(clippy::too_many_arguments)]
async fn insert_atom(
    owner: &PgPool,
    graph_id: Uuid,
    ingested_by: Uuid,
    source_type: &str,
    source_id: &str,
    timestamp: DateTime<Utc>,
    content_inline: Option<&str>,
    header_len: Option<i32>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \
                            ingested_by, \"timestamp\", content_inline, header_len) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_type)
    .bind(source_id)
    .bind(format!("blob-{source_id}"))
    .bind(ingested_by)
    .bind(timestamp)
    .bind(content_inline)
    .bind(header_len)
    .fetch_one(owner)
    .await
    .expect("insert atom")
}

async fn insert_activity_event(
    owner: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    event_type: &str,
    created_at: DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO activity_events(account_id, user_id, graph_id, event_type, payload, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(graph_id)
    .bind(event_type)
    .bind(json!({}))
    .bind(created_at)
    .execute(owner)
    .await
    .expect("insert activity event");
}

/// Insert one `query_history` row, returning its id — the journal's asks
/// input (hourly-completeness audit gaps #4/#5), used both by
/// `journal_hour_inputs`/`journal_dirty_hours` (`Store::journal_hour_inputs`'s
/// `asks` field, `Store::journal_dirty_hours`'s `ask_inputs` CTE) and,
/// separately, by `journal_range_aggregate`'s live "asks" count.
async fn insert_query_history(
    owner: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    user_id: Uuid,
    query_text: &str,
    citation_count: i32,
    created_at: DateTime<Utc>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO query_history(account_id, graph_id, user_id, query_text, citation_count, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(user_id)
    .bind(query_text)
    .bind(citation_count)
    .bind(created_at)
    .fetch_one(owner)
    .await
    .expect("insert query_history")
}

fn hour(y: i32, mo: u32, d: u32, h: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, 0, 0).unwrap()
}

fn dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

/// 1. `upsert_journal_rollup`: insert, then a second call on the same
/// natural key refreshes the row and increments `recompile_count`.
#[tokio::test]
async fn upsert_inserts_then_increments_recompile_count_on_conflict() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping upsert_inserts_then_increments_recompile_count_on_conflict: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "jr_upsert";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-upsert-a@x.co").await;
    let store = PostgresStore::new(owner.clone());
    let hour_start = hour(2026, 7, 15, 9);

    let first = store
        .upsert_journal_rollup(
            a.account_id,
            a.user_id,
            a.graph_id,
            hour_start,
            "Worked on the ingest pipeline.",
            json!({"atom_count": 3}),
            "fp-v1",
            "claude-haiku-4-5",
        )
        .await
        .expect("first upsert must succeed");
    assert_eq!(first.recompile_count, 0, "first compile starts at recompile_count 0");
    assert_eq!(first.summary, "Worked on the ingest pipeline.");
    assert_eq!(first.input_fingerprint, "fp-v1");

    let second = store
        .upsert_journal_rollup(
            a.account_id,
            a.user_id,
            a.graph_id,
            hour_start,
            "Worked on the ingest pipeline, then reviewed a PR.",
            json!({"atom_count": 5}),
            "fp-v2",
            "claude-haiku-4-5",
        )
        .await
        .expect("second upsert (same natural key) must succeed");
    assert_eq!(second.id, first.id, "same (user_id, graph_id, hour_start) must update the same row");
    assert_eq!(second.recompile_count, 1, "a recompile must increment recompile_count");
    assert_eq!(second.summary, "Worked on the ingest pipeline, then reviewed a PR.");
    assert_eq!(second.input_fingerprint, "fp-v2");
    assert_eq!(second.stats, json!({"atom_count": 5}));

    // A third recompile must increment again, not reset.
    let third = store
        .upsert_journal_rollup(
            a.account_id,
            a.user_id,
            a.graph_id,
            hour_start,
            "Third pass.",
            json!({}),
            "fp-v3",
            "claude-haiku-4-5",
        )
        .await
        .expect("third upsert must succeed");
    assert_eq!(third.recompile_count, 2);

    let row_count: i64 = sqlx::query_scalar("SELECT count(*) FROM journal_rollups")
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(row_count, 1, "upserts on the same natural key must never duplicate the row");

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// 2. `journal_hour_inputs`: gathers exactly the hour's atoms authored by the
/// given user plus that hour's activity events AND `query_history` rows
/// ("asks", audit gaps #4/#5), strips the provenance header, and truncates
/// the excerpt.
#[tokio::test]
async fn journal_hour_inputs_gathers_the_hours_atoms_and_activity() {
    let Some(base) = base_url() else {
        eprintln!("skipping journal_hour_inputs_gathers_the_hours_atoms_and_activity: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "jr_hour_inputs";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-hour-a@x.co").await;
    let other_user: Uuid =
        sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind("jr-hour-other@x.co")
            .fetch_one(&owner)
            .await
            .unwrap();
    let store = PostgresStore::new(owner.clone());

    let h = hour(2026, 7, 15, 9);

    // In-window, authored by the target user: must be included.
    let in_window = insert_atom(
        &owner,
        a.graph_id,
        a.user_id,
        "coding_session",
        "cc:sess-1:chk-1",
        h + Duration::minutes(10),
        Some("Session: sess-1. Repo: coralswarm.\n\nActual body content here."),
        Some("Session: sess-1. Repo: coralswarm.\n\n".len() as i32),
    )
    .await;

    // Same hour but authored by a DIFFERENT user: must be excluded.
    insert_atom(
        &owner,
        a.graph_id,
        other_user,
        "note",
        "other-note",
        h + Duration::minutes(15),
        Some("Someone else's note."),
        None,
    )
    .await;

    // The hour BEFORE: must be excluded ([hour_start, hour_start+1h) is
    // half-open).
    insert_atom(
        &owner,
        a.graph_id,
        a.user_id,
        "note",
        "before",
        h - Duration::minutes(1),
        Some("Too early."),
        None,
    )
    .await;

    // Exactly the hour AFTER (the open end of the window): must be excluded.
    insert_atom(
        &owner,
        a.graph_id,
        a.user_id,
        "note",
        "after",
        h + Duration::hours(1),
        Some("Too late."),
        None,
    )
    .await;

    // An excerpt long enough to require truncation.
    let long_body = "x".repeat(400);
    let long_atom = insert_atom(
        &owner,
        a.graph_id,
        a.user_id,
        "note",
        "long-note",
        h + Duration::minutes(20),
        Some(&long_body),
        None,
    )
    .await;

    insert_activity_event(&owner, a.account_id, a.user_id, a.graph_id, "atom_created", h + Duration::minutes(5)).await;
    // Different graph: must be excluded.
    let other_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'other') RETURNING id",
    )
    .bind(a.account_id)
    .fetch_one(&owner)
    .await
    .unwrap();
    insert_activity_event(&owner, a.account_id, a.user_id, other_graph, "atom_created", h + Duration::minutes(6)).await;

    // Asks (`query_history`, audit gaps #4/#5): in-window + authored by the
    // target user must be included; a different user, a different graph, and
    // outside the half-open window must all be excluded — the exact same
    // four exclusion cases the atoms fixtures above exercise.
    let in_window_ask =
        insert_query_history(&owner, a.account_id, a.graph_id, a.user_id, "what shipped this hour?", 3, h + Duration::minutes(12))
            .await;
    insert_query_history(
        &owner, a.account_id, a.graph_id, other_user, "someone else's question", 1, h + Duration::minutes(13),
    )
    .await;
    insert_query_history(
        &owner, a.account_id, other_graph, a.user_id, "wrong graph question", 0, h + Duration::minutes(14),
    )
    .await;
    insert_query_history(&owner, a.account_id, a.graph_id, a.user_id, "too early question", 0, h - Duration::minutes(1))
        .await;
    insert_query_history(&owner, a.account_id, a.graph_id, a.user_id, "too late question", 0, h + Duration::hours(1))
        .await;

    let inputs = store
        .journal_hour_inputs(a.user_id, a.graph_id, h)
        .await
        .expect("journal_hour_inputs must succeed");

    let atom_ids: Vec<Uuid> = inputs.atoms.iter().map(|a| a.atom_id).collect();
    assert_eq!(
        atom_ids.len(),
        2,
        "must return exactly the two in-window atoms authored by the user, got {atom_ids:?}"
    );
    assert!(atom_ids.contains(&in_window));
    assert!(atom_ids.contains(&long_atom));

    let with_header = inputs.atoms.iter().find(|a| a.atom_id == in_window).unwrap();
    assert_eq!(
        with_header.excerpt.as_deref(),
        Some("Actual body content here."),
        "the provenance header must be stripped before the excerpt is taken"
    );

    let truncated = inputs.atoms.iter().find(|a| a.atom_id == long_atom).unwrap();
    let excerpt = truncated.excerpt.as_deref().expect("long body must produce an excerpt");
    assert_eq!(excerpt.chars().count(), 281, "280 kept chars + one ellipsis char");
    assert!(excerpt.ends_with('\u{2026}'));

    assert_eq!(inputs.activity.len(), 1, "must return exactly this graph's activity event");
    assert_eq!(inputs.activity[0].event_type, "atom_created");

    assert_eq!(
        inputs.asks.len(),
        1,
        "must return exactly the one in-window ask authored by the user, got {:?}",
        inputs.asks
    );
    let ask = &inputs.asks[0];
    assert_eq!(ask.id, in_window_ask);
    assert_eq!(ask.query_text, "what shipped this hour?");
    assert_eq!(ask.citation_count, 3);

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Regression test for the today-zoomable-journal audit's gap 9: cloud
/// ingest used to populate a journal excerpt ONLY via `content_inline`,
/// which the R2-on ingest branch never writes — an atom with a real
/// `content_blob_key` and no `content_inline` would compile to
/// `excerpt = None` forever, even though its full text was available (and
/// derivable) at insert time. `Store::insert_atoms_scoped` now takes a
/// pre-derived `excerpts` array (`store::derive_journal_excerpt`, run by
/// `routes::ingest` against the in-memory chunk text BEFORE it decides
/// where the full text is stored) and persists it to `atoms.excerpt` in the
/// SAME insert as everything else.
///
/// This simulates the R2-on shape directly — a real blob key, NO
/// `content_inline` ever set — and asserts the resulting excerpt is NOT
/// `None`. On the pre-fix code (`journal_hour_inputs` reading only
/// `content_inline`) this test fails.
#[tokio::test]
async fn journal_hour_inputs_excerpt_survives_blob_only_atoms_without_content_inline() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping journal_hour_inputs_excerpt_survives_blob_only_atoms_without_content_inline: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "jr_blob_only_excerpt";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-blob-only@x.co").await;
    let store = PostgresStore::new(owner.clone());

    let h = hour(2026, 7, 20, 14);
    let atom_id = Uuid::new_v4();

    // Simulate the R2-on write path: `insert_atoms_scoped` gets a real blob
    // key (this atom's content "lives in R2", never `content_inline`) plus a
    // pre-derived excerpt — exactly what `routes::ingest` computes from the
    // in-memory chunk text before the R2-vs-inline storage decision.
    let mut tx = owner.begin().await.expect("begin tx");
    let inserted = store
        .insert_atoms_scoped(
            &mut tx,
            &[atom_id],
            a.graph_id,
            "note",
            &["blob-only-note".to_string()],
            &["real-r2-blob-key".to_string()],
            &[],
            h + Duration::minutes(5),
            a.user_id,
            "private",
            "en",
            &[Some("Persisted at ingest time, independent of R2.".to_string())],
            &[None],
            &[None],
            &[None],
            None,
        )
        .await
        .expect("insert_atoms_scoped must succeed");
    assert_eq!(inserted, vec![atom_id]);
    tx.commit().await.expect("commit");

    // Test-setup sanity: `content_inline` was NEVER written for this atom
    // (no `set_atoms_inline_content_scoped` call) — confirms this really is
    // the R2-on shape, not an accidental inline write.
    let content_inline: Option<String> =
        sqlx::query_scalar("SELECT content_inline FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&owner)
            .await
            .expect("read back content_inline");
    assert!(
        content_inline.is_none(),
        "test setup: this atom must have NO content_inline (simulating R2-on)"
    );

    let inputs = store
        .journal_hour_inputs(a.user_id, a.graph_id, h)
        .await
        .expect("journal_hour_inputs must succeed");

    let got = inputs.atoms.iter().find(|at| at.atom_id == atom_id);
    assert!(got.is_some(), "the blob-only atom must still land in the hour's inputs");
    assert_eq!(
        got.unwrap().excerpt.as_deref(),
        Some("Persisted at ingest time, independent of R2."),
        "an atom with a blob key and no content_inline must NOT yield excerpt=None — \
         atoms.excerpt must be persisted at ingest time regardless of storage backend (gap 9)"
    );

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// 3. `journal_dirty_hours`: finds a never-compiled hour, finds a hour whose
/// stored fingerprint has gone stale (both a count change and a
/// max-created-at change), and skips a hour that is already up to date.
#[tokio::test]
async fn journal_dirty_hours_finds_new_and_changed_and_skips_unchanged() {
    let Some(base) = base_url() else {
        eprintln!("skipping journal_dirty_hours_finds_new_and_changed_and_skips_unchanged: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "jr_dirty_hours";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-dirty-a@x.co").await;
    let store = PostgresStore::new(owner.clone());

    let lookback_from = hour(2026, 7, 15, 0);
    let now = hour(2026, 7, 16, 0);

    let hour_new = hour(2026, 7, 15, 9); // never compiled
    let hour_stable = hour(2026, 7, 15, 10); // compiled and unchanged
    let hour_changed = hour(2026, 7, 15, 11); // compiled, then a new atom lands

    insert_atom(&owner, a.graph_id, a.user_id, "note", "n1", hour_new + Duration::minutes(1), None, None).await;
    insert_atom(&owner, a.graph_id, a.user_id, "note", "s1", hour_stable + Duration::minutes(1), None, None).await;
    insert_atom(&owner, a.graph_id, a.user_id, "note", "c1", hour_changed + Duration::minutes(1), None, None).await;

    // First pass: both hour_stable and hour_changed have no rollup yet, so
    // all three candidate hours must come back dirty.
    let first_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let first_pass_hours: Vec<DateTime<Utc>> = first_pass.iter().map(|d| d.hour_start).collect();
    assert!(first_pass_hours.contains(&hour_new));
    assert!(first_pass_hours.contains(&hour_stable));
    assert!(first_pass_hours.contains(&hour_changed));
    assert_eq!(first_pass.len(), 3, "exactly the three hours with atoms, got {first_pass_hours:?}");

    // "Compile" hour_stable and hour_changed using the fingerprint the sweep
    // itself returned — exactly the contract PR B is expected to follow.
    for dirty in first_pass.iter().filter(|d| d.hour_start == hour_stable || d.hour_start == hour_changed) {
        store
            .upsert_journal_rollup(
                dirty.account_id,
                dirty.user_id,
                dirty.graph_id,
                dirty.hour_start,
                "compiled",
                json!({}),
                &dirty.current_fingerprint,
                "test-model",
            )
            .await
            .unwrap();
    }

    // Second pass: hour_stable's inputs haven't changed, so it must drop out.
    // hour_new is still uncompiled. hour_changed is (for now) also stable.
    let second_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let second_pass_hours: Vec<DateTime<Utc>> = second_pass.iter().map(|d| d.hour_start).collect();
    assert!(second_pass_hours.contains(&hour_new), "an uncompiled hour must stay dirty");
    assert!(
        !second_pass_hours.contains(&hour_stable),
        "a compiled hour with unchanged inputs must NOT be reported dirty, got {second_pass_hours:?}"
    );
    assert!(!second_pass_hours.contains(&hour_changed), "not yet changed since its compile");

    // A new atom lands in hour_changed: its input_count and max(created_at)/
    // max(id) all move, so the stored fingerprint goes stale.
    insert_atom(&owner, a.graph_id, a.user_id, "note", "c2", hour_changed + Duration::minutes(2), None, None).await;

    let third_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let third_pass_hours: Vec<DateTime<Utc>> = third_pass.iter().map(|d| d.hour_start).collect();
    assert!(
        third_pass_hours.contains(&hour_changed),
        "a hour whose inputs grew since its last compile must be reported dirty again, got {third_pass_hours:?}"
    );
    assert!(!third_pass_hours.contains(&hour_stable), "still stable, still skipped");

    // Sanity: the fingerprint really did change (proves the detection isn't
    // a fluke of some unrelated field).
    let stale_row: (String,) = sqlx::query_as(
        "SELECT input_fingerprint FROM journal_rollups WHERE hour_start = $1",
    )
    .bind(hour_changed)
    .fetch_one(&owner)
    .await
    .unwrap();
    let recomputed = third_pass.iter().find(|d| d.hour_start == hour_changed).unwrap();
    assert_ne!(stale_row.0, recomputed.current_fingerprint);

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// 3b. `journal_dirty_hours`: an hour with ONLY `activity_events` rows and
/// zero atoms must still surface as a candidate, and an activity-events-only
/// change to an already-compiled hour (no new/deleted atom in that hour)
/// must mark it dirty again — the exact gap CodeRabbit's round-one review
/// found in the atoms-only fingerprint (PR #454 finding 1).
#[tokio::test]
async fn journal_dirty_hours_detects_events_only_hours_and_changes() {
    let Some(base) = base_url() else {
        eprintln!("skipping journal_dirty_hours_detects_events_only_hours_and_changes: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "jr_dirty_events_only";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-dirty-events-a@x.co").await;
    let store = PostgresStore::new(owner.clone());

    let lookback_from = hour(2026, 7, 15, 0);
    let now = hour(2026, 7, 16, 0);

    // `hour_events_only` gets NO atoms at all, only an activity_events row —
    // must still surface as a dirty candidate.
    let hour_events_only = hour(2026, 7, 15, 9);
    insert_activity_event(
        &owner,
        a.account_id,
        a.user_id,
        a.graph_id,
        "atom_created",
        hour_events_only + Duration::minutes(1),
    )
    .await;

    // `hour_mixed` gets one atom (so it's already compiled below), then a
    // LATER activity_events-only change with no new/deleted atom — must go
    // dirty again purely from the event.
    let hour_mixed = hour(2026, 7, 15, 10);
    insert_atom(&owner, a.graph_id, a.user_id, "note", "m1", hour_mixed + Duration::minutes(1), None, None).await;
    insert_activity_event(
        &owner,
        a.account_id,
        a.user_id,
        a.graph_id,
        "atom_created",
        hour_mixed + Duration::minutes(2),
    )
    .await;

    let first_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let first_pass_hours: Vec<DateTime<Utc>> = first_pass.iter().map(|d| d.hour_start).collect();
    assert!(
        first_pass_hours.contains(&hour_events_only),
        "an hour with ONLY activity_events (zero atoms) must be a candidate, got {first_pass_hours:?}"
    );
    assert!(first_pass_hours.contains(&hour_mixed));

    // "Compile" both hours using the fingerprints the sweep returned —
    // exactly the contract PR B follows.
    for dirty in
        first_pass.iter().filter(|d| d.hour_start == hour_events_only || d.hour_start == hour_mixed)
    {
        store
            .upsert_journal_rollup(
                dirty.account_id,
                dirty.user_id,
                dirty.graph_id,
                dirty.hour_start,
                "compiled",
                json!({}),
                &dirty.current_fingerprint,
                "test-model",
            )
            .await
            .unwrap();
    }

    let second_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let second_pass_hours: Vec<DateTime<Utc>> = second_pass.iter().map(|d| d.hour_start).collect();
    assert!(
        !second_pass_hours.contains(&hour_events_only),
        "a compiled events-only hour with unchanged inputs must NOT be reported dirty, got {second_pass_hours:?}"
    );
    assert!(!second_pass_hours.contains(&hour_mixed), "not yet changed since its compile");

    // A SECOND activity_events row lands in `hour_mixed` — no new atom at
    // all — this alone must mark the hour dirty again.
    insert_activity_event(
        &owner,
        a.account_id,
        a.user_id,
        a.graph_id,
        "query_run",
        hour_mixed + Duration::minutes(3),
    )
    .await;

    let third_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let third_pass_hours: Vec<DateTime<Utc>> = third_pass.iter().map(|d| d.hour_start).collect();
    assert!(
        third_pass_hours.contains(&hour_mixed),
        "an activity_events-only change (no new/deleted atom) must mark an already-compiled hour \
         dirty again, got {third_pass_hours:?}"
    );
    assert!(
        !third_pass_hours.contains(&hour_events_only),
        "hour_events_only had no further change, must stay clean"
    );

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// 3c. `journal_dirty_hours`: the SAME two cases as
/// `journal_dirty_hours_detects_events_only_hours_and_changes`, this time
/// proving `query_history` (audit gaps #4/#5) — an hour with ONLY a
/// `query_history` row and zero atoms/`activity_events` must still surface
/// as a candidate, and a NEW ask landing in an already-compiled hour (no new
/// atom, no new `activity_events` row) must mark it dirty again. This is the
/// end-to-end proof that the streaming web-chat/MCP `ask_ocean` paths —
/// which write `query_history` only, never `query_run` — actually re-dirty
/// their hour, which the old atoms+`activity_events`-only union could not
/// detect at all.
#[tokio::test]
async fn journal_dirty_hours_detects_asks_only_hours_and_changes() {
    let Some(base) = base_url() else {
        eprintln!("skipping journal_dirty_hours_detects_asks_only_hours_and_changes: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "jr_dirty_asks_only";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-dirty-asks-a@x.co").await;
    let store = PostgresStore::new(owner.clone());

    let lookback_from = hour(2026, 7, 15, 0);
    let now = hour(2026, 7, 16, 0);

    // `hour_asks_only` gets NO atoms and NO activity_events at all, only a
    // `query_history` row — must still surface as a dirty candidate (a
    // streamed-chat/MCP ask with no legacy `query_run` event at all).
    let hour_asks_only = hour(2026, 7, 15, 9);
    insert_query_history(
        &owner, a.account_id, a.graph_id, a.user_id, "what happened this hour?", 2,
        hour_asks_only + Duration::minutes(1),
    )
    .await;

    // `hour_mixed` gets one atom (so it's already compiled below), then a
    // LATER ask with no new/deleted atom and no activity_events row at all
    // — must go dirty again purely from the ask.
    let hour_mixed = hour(2026, 7, 15, 10);
    insert_atom(&owner, a.graph_id, a.user_id, "note", "m1", hour_mixed + Duration::minutes(1), None, None).await;

    let first_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let first_pass_hours: Vec<DateTime<Utc>> = first_pass.iter().map(|d| d.hour_start).collect();
    assert!(
        first_pass_hours.contains(&hour_asks_only),
        "an hour with ONLY query_history (zero atoms/activity_events) must be a candidate, got {first_pass_hours:?}"
    );
    assert!(first_pass_hours.contains(&hour_mixed));

    // "Compile" both hours using the fingerprints the sweep returned.
    for dirty in first_pass.iter().filter(|d| d.hour_start == hour_asks_only || d.hour_start == hour_mixed) {
        store
            .upsert_journal_rollup(
                dirty.account_id,
                dirty.user_id,
                dirty.graph_id,
                dirty.hour_start,
                "compiled",
                json!({}),
                &dirty.current_fingerprint,
                "test-model",
            )
            .await
            .unwrap();
    }

    let second_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let second_pass_hours: Vec<DateTime<Utc>> = second_pass.iter().map(|d| d.hour_start).collect();
    assert!(
        !second_pass_hours.contains(&hour_asks_only),
        "a compiled asks-only hour with unchanged inputs must NOT be reported dirty, got {second_pass_hours:?}"
    );
    assert!(!second_pass_hours.contains(&hour_mixed), "not yet changed since its compile");

    // A NEW ask lands in `hour_mixed` — no new atom, no activity_events row
    // at all — this alone must mark the hour dirty again.
    insert_query_history(
        &owner, a.account_id, a.graph_id, a.user_id, "a late-arriving question", 0, hour_mixed + Duration::minutes(2),
    )
    .await;

    let third_pass = store.journal_dirty_hours(lookback_from, now).await.unwrap();
    let third_pass_hours: Vec<DateTime<Utc>> = third_pass.iter().map(|d| d.hour_start).collect();
    assert!(
        third_pass_hours.contains(&hour_mixed),
        "a query_history-only change (no new/deleted atom or event) must mark an already-compiled hour \
         dirty again, got {third_pass_hours:?}"
    );
    assert!(!third_pass_hours.contains(&hour_asks_only), "hour_asks_only had no further change, must stay clean");

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// 4. `list_journal_rollups`: respects `[from, to)` and the `graph_ids`
/// filter, AND is genuinely RLS-isolated across accounts under a real
/// `coralswarm_app` login — the negative probe seeds a real row on account B
/// (via the BYPASSRLS owner pool, so it indisputably exists) and proves it
/// never comes back while scoped to account A, even when A's query
/// explicitly asks for B's graph_id.
#[tokio::test]
async fn list_journal_rollups_respects_range_and_graph_filter_and_isolates_across_accounts() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping list_journal_rollups_respects_range_and_graph_filter_and_isolates_across_accounts: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "jr_list_rls";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083_consumer_rls");
    set_app_role_password_with_retry(&owner).await;

    let a = seed_account(&owner, "jr-list-a@x.co").await;
    let b = seed_account(&owner, "jr-list-b@x.co").await;
    // A second ocean under account A, to exercise the graph_ids filter.
    let a_graph2: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'second') RETURNING id",
    )
    .bind(a.account_id)
    .fetch_one(&owner)
    .await
    .unwrap();

    let store_owner = PostgresStore::new(owner.clone());

    let h0 = hour(2026, 7, 15, 8);
    let h1 = hour(2026, 7, 15, 9);
    let h2 = hour(2026, 7, 15, 10);
    let h_out_of_range = hour(2026, 7, 15, 23);

    // A's graph 1: three rollups, one deliberately outside the query range.
    for h in [h0, h1, h_out_of_range] {
        store_owner
            .upsert_journal_rollup(a.account_id, a.user_id, a.graph_id, h, "s", json!({}), "fp", "m")
            .await
            .unwrap();
    }
    // A's graph 2: one rollup inside the range, to prove the graph_ids
    // filter (not just a bare user_id filter) actually narrows results.
    store_owner
        .upsert_journal_rollup(a.account_id, a.user_id, a_graph2, h1, "s2", json!({}), "fp", "m")
        .await
        .unwrap();
    // B's graph: one rollup, indisputably present in the table — the
    // negative probe's target.
    store_owner
        .upsert_journal_rollup(b.account_id, b.user_id, b.graph_id, h1, "b-secret", json!({}), "fp", "m")
        .await
        .unwrap();

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());

    // ── Positive: scoped to A, querying only A's graph 1, [h0, h2) ─────────
    let mut tx = begin_account_scoped(&app_pool, a.account_id)
        .await
        .expect("begin_account_scoped for account A");
    let rows = store
        .list_journal_rollups(&mut tx, a.user_id, &[a.graph_id], "hour", h0, h2)
        .await
        .expect("list_journal_rollups must succeed when scoped to A");
    let hours: Vec<DateTime<Utc>> = rows.iter().map(|r| r.hour_start).collect();
    assert_eq!(hours, vec![h0, h1], "must respect [from, to) and return hour_start ASC");
    assert!(
        rows.iter().all(|r| r.graph_id == a.graph_id),
        "must not include A's OTHER graph's rollup when it isn't in graph_ids"
    );

    // ── graph_ids filter widened to include A's second graph ───────────────
    let rows_both_graphs = store
        .list_journal_rollups(&mut tx, a.user_id, &[a.graph_id, a_graph2], "hour", h0, h2)
        .await
        .unwrap();
    assert_eq!(rows_both_graphs.len(), 3, "h0 + h1(graph1) + h1(graph2)");

    // ── Negative (the real isolation proof): still scoped to A, explicitly
    // ask for B's graph_id. B's row indisputably exists (seeded via the
    // BYPASSRLS owner pool above) — if RLS were not enforced, or if this
    // method silently trusted the caller-supplied graph_ids instead of the
    // scoped transaction's account boundary, `b-secret` would leak here.
    let cross_account = store
        .list_journal_rollups(&mut tx, b.user_id, &[b.graph_id], "hour", h0, h2)
        .await
        .expect("must not ERROR — RLS filters to empty, it does not reject the query");
    assert!(
        cross_account.is_empty(),
        "scoped to account A, list_journal_rollups for B's user/graph must return ZERO rows — got {cross_account:?}"
    );
    let cross_account_mixed_graph_ids = store
        .list_journal_rollups(&mut tx, a.user_id, &[a.graph_id, b.graph_id], "hour", h0, h2)
        .await
        .expect("must not ERROR on a mixed graph_ids list");
    assert!(
        cross_account_mixed_graph_ids.iter().all(|r| r.graph_id != b.graph_id),
        "B's graph_id in the filter list must never surface B's rows while scoped to A"
    );

    tx.rollback().await.ok();

    // ── Sanity: the owner (BYPASSRLS) pool really does see B's row — proves
    // the negative result above is RLS enforcement, not an empty table.
    let b_row_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM journal_rollups WHERE summary = 'b-secret')",
    )
    .fetch_one(&owner)
    .await
    .unwrap();
    assert!(b_row_exists, "B's rollup must genuinely exist in the table");

    app_pool.close().await;
    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Sanity cross-check between the SQL-side detection in
/// `journal_dirty_hours` and the pure Rust helper it (and every future
/// upsert caller) shares — proves the two really do compute the identical
/// value for the identical inputs, not merely "both call a function named
/// `journal_input_fingerprint`". Covers all THREE input tables at once
/// (atoms, `activity_events`, `query_history`), not just atoms, so the
/// three-way union (audit gaps #4/#5's `ask_inputs` CTE) is proven bit-for-
/// bit identical to the Rust side, not merely the pre-existing two-way one.
#[tokio::test]
async fn journal_dirty_hours_fingerprint_matches_the_pure_helper() {
    let Some(base) = base_url() else {
        eprintln!("skipping journal_dirty_hours_fingerprint_matches_the_pure_helper: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "jr_fp_cross_check";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-fp-a@x.co").await;
    let store = PostgresStore::new(owner.clone());
    let h = hour(2026, 7, 15, 9);

    let atom1 = insert_atom(&owner, a.graph_id, a.user_id, "note", "f1", h + Duration::minutes(1), None, None).await;
    let atom2 = insert_atom(&owner, a.graph_id, a.user_id, "note", "f2", h + Duration::minutes(2), None, None).await;
    insert_activity_event(&owner, a.account_id, a.user_id, a.graph_id, "atom_created", h + Duration::minutes(3)).await;
    let ask_id =
        insert_query_history(&owner, a.account_id, a.graph_id, a.user_id, "fingerprint check ask", 0, h + Duration::minutes(4))
            .await;

    let (max_atom_created_at,): (DateTime<Utc>,) = sqlx::query_as(
        "SELECT max(created_at) FROM atoms WHERE atom_id = ANY($1)",
    )
    .bind([atom1, atom2])
    .fetch_one(&owner)
    .await
    .unwrap();
    let (event_id, event_created_at): (Uuid, DateTime<Utc>) = sqlx::query_as(
        "SELECT id, created_at FROM activity_events WHERE graph_id = $1",
    )
    .bind(a.graph_id)
    .fetch_one(&owner)
    .await
    .unwrap();
    let (ask_created_at,): (DateTime<Utc>,) =
        sqlx::query_as("SELECT created_at FROM query_history WHERE id = $1").bind(ask_id).fetch_one(&owner).await.unwrap();

    let max_created_at = [max_atom_created_at, event_created_at, ask_created_at].into_iter().max().unwrap();
    let mut max_id = std::cmp::max(atom1, atom2);
    for candidate in [event_id, ask_id] {
        if candidate.to_string() > max_id.to_string() {
            max_id = candidate;
        }
    }
    let expected = journal_input_fingerprint(4, max_created_at, max_id);

    let dirty = store
        .journal_dirty_hours(hour(2026, 7, 15, 0), hour(2026, 7, 16, 0))
        .await
        .unwrap();
    let found = dirty.iter().find(|d| d.hour_start == h).expect("hour must be reported dirty");
    assert_eq!(
        found.current_fingerprint, expected,
        "the SQL-computed fingerprint must match the pure Rust helper bit-for-bit"
    );

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// 5a. `list_journal_rollups`/`journal_range_aggregate` (Today-journal PR C):
/// the per-MEMBER boundary is independent of the `account_id` RLS backstop —
/// TWO users under the SAME account/ocean, scoped to that one account under a
/// real `coralswarm_app` login, must each see only their OWN rollup and their
/// OWN live aggregate, never the other's, even though RLS alone (which only
/// keys on `account_id`) cannot tell them apart. Also re-proves cross-ACCOUNT
/// isolation for the new `journal_range_aggregate` method specifically (the
/// route-level suite `journal_endpoint_pg.rs` covers this too, over real
/// HTTP; this is the tight, fast, store-level version of the same proof).
#[tokio::test]
async fn journal_range_aggregate_and_list_rollups_isolate_by_member_within_one_account_and_across_accounts() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping journal_range_aggregate_and_list_rollups_isolate_by_member_within_one_account_and_across_accounts: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "jr_aggregate_rls";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083_consumer_rls");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    let a = seed_account(&owner, "jr-agg-a@x.co").await;
    // A second MEMBER of A's own account/ocean — no `account_members` row is
    // needed here (these store methods predicate on the `user_id` PARAMETER
    // directly, never on membership), only a `users` row to satisfy the FK.
    let a2_user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind("jr-agg-a2@x.co")
        .fetch_one(&owner)
        .await
        .unwrap();
    let b = seed_account(&owner, "jr-agg-b@x.co").await; // a fully separate account

    let store_owner = PostgresStore::new(owner.clone());
    let h = hour(2026, 7, 20, 9);
    let from = hour(2026, 7, 20, 0);
    let to = hour(2026, 7, 21, 0);

    // Rollups: A's own, A2's (same account/ocean), B's (separate account).
    store_owner
        .upsert_journal_rollup(a.account_id, a.user_id, a.graph_id, h, "A's own prose.", json!({}), "fp", "m")
        .await
        .unwrap();
    store_owner
        .upsert_journal_rollup(
            a.account_id,
            a2_user_id,
            a.graph_id,
            h,
            "A2's prose — same account/ocean as A, must stay A2's alone.",
            json!({}),
            "fp",
            "m",
        )
        .await
        .unwrap();
    store_owner
        .upsert_journal_rollup(b.account_id, b.user_id, b.graph_id, h, "B's prose.", json!({}), "fp", "m")
        .await
        .unwrap();

    // Live inputs: A gets 1 atom + 1 ask, A2 gets 2 atoms + 1 ask (SAME
    // graph as A), B gets 1 atom + 1 ask (separate account/graph).
    insert_atom(&owner, a.graph_id, a.user_id, "note", "a-n1", h + Duration::minutes(1), None, None).await;
    insert_atom(&owner, a.graph_id, a2_user_id, "note", "a2-n1", h + Duration::minutes(2), None, None).await;
    insert_atom(&owner, a.graph_id, a2_user_id, "note", "a2-n2", h + Duration::minutes(3), None, None).await;
    insert_atom(&owner, b.graph_id, b.user_id, "note", "b-n1", h + Duration::minutes(1), None, None).await;
    // NOTE: no `insert_activity_event` fixture here — `journal_range_aggregate`
    // (the only aggregate method this test exercises) aggregates `atoms` and
    // `query_history` only, never `activity_events` (unlike `journal_dirty_hours`,
    // covered separately in `journal_dirty_hours_fingerprint_matches_the_pure_helper`
    // and the dirty-hours tests above); a seeded-but-unasserted event here would
    // misleadingly imply a dependency this method doesn't have.
    for (account_id, graph_id, user_id, text) in [
        (a.account_id, a.graph_id, a.user_id, "a's ask"),
        (a.account_id, a.graph_id, a2_user_id, "a2's ask"),
        (b.account_id, b.graph_id, b.user_id, "b's ask"),
    ] {
        sqlx::query(
            "INSERT INTO query_history(account_id, graph_id, user_id, query_text, created_at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(user_id)
        .bind(text)
        .bind(h + Duration::minutes(4))
        .execute(&owner)
        .await
        .expect("insert query_history");
    }

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());
    let mut tx = begin_account_scoped(&app_pool, a.account_id)
        .await
        .expect("begin_account_scoped for account A");

    // ── A, scoped to A's account: exactly A's rollup and A's aggregate.
    let a_rollups = store.list_journal_rollups(&mut tx, a.user_id, &[a.graph_id], "hour", from, to).await.unwrap();
    assert_eq!(a_rollups.len(), 1, "A must see exactly one rollup: {a_rollups:?}");
    assert_eq!(a_rollups[0].summary, "A's own prose.");
    let a_agg = store.journal_range_aggregate(&mut tx, a.user_id, &[a.graph_id], from, to).await.unwrap();
    assert_eq!(a_agg.len(), 1);
    assert_eq!(a_agg[0].atom_count, 1, "A's aggregate must count only A's own atom, not A2's two: {a_agg:?}");
    assert_eq!(a_agg[0].asks, 1, "A's aggregate must count only A's own ask, not A2's: {a_agg:?}");

    // ── A2, SAME account/ocean, different user_id: exactly A2's rollup and
    // A2's aggregate — the per-MEMBER boundary RLS alone cannot enforce.
    let a2_rollups = store.list_journal_rollups(&mut tx, a2_user_id, &[a.graph_id], "hour", from, to).await.unwrap();
    assert_eq!(a2_rollups.len(), 1, "A2 must see exactly one rollup: {a2_rollups:?}");
    assert_eq!(a2_rollups[0].summary, "A2's prose — same account/ocean as A, must stay A2's alone.");
    assert_ne!(
        a2_rollups[0].summary, a_rollups[0].summary,
        "A2's rollup must never equal A's — proves this is a real per-user split, not a shared row"
    );
    let a2_agg = store.journal_range_aggregate(&mut tx, a2_user_id, &[a.graph_id], from, to).await.unwrap();
    assert_eq!(a2_agg.len(), 1);
    assert_eq!(a2_agg[0].atom_count, 2, "A2's aggregate must count only A2's own two atoms, not A's one: {a2_agg:?}");
    assert_eq!(a2_agg[0].asks, 1);

    // ── Cross-account (still scoped to A): B's user/graph must yield ZERO
    // rollups and ZERO aggregate rows — never an error, never B's data.
    let cross_rollups = store.list_journal_rollups(&mut tx, b.user_id, &[b.graph_id], "hour", from, to).await.unwrap();
    assert!(cross_rollups.is_empty(), "scoped to A, B's rollups must be empty: {cross_rollups:?}");
    let cross_agg = store.journal_range_aggregate(&mut tx, b.user_id, &[b.graph_id], from, to).await.unwrap();
    assert!(cross_agg.is_empty(), "scoped to A, B's live aggregate must be empty: {cross_agg:?}");

    tx.rollback().await.ok();

    // Sanity: all three rollups genuinely exist (proves the negative results
    // above are real scoping, not an empty table).
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM journal_rollups")
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(total, 3, "all three seeded rollups (A, A2, B) must genuinely exist");

    app_pool.close().await;
    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// 5b. `journal_range_aggregate`'s block grouping/title resolution: a
/// coding-session atom groups by `session_id` (extracted from
/// `"cc:{session_id}:{checkpoint_id}"`) regardless of a trailing
/// `:chunk:{n}` suffix, titled from `coding_sessions.title`; a non-coding-
/// session atom groups by its `source_id` with any trailing `:chunk:{n}`
/// stripped, titled from a grouped atom's own `atoms.title` when present, and
/// falls back to a generic `source_type` label when neither title source
/// exists.
#[tokio::test]
async fn journal_range_aggregate_groups_blocks_by_session_and_resolves_titles() {
    let Some(base) = base_url() else {
        eprintln!("skipping journal_range_aggregate_groups_blocks_by_session_and_resolves_titles: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "jr_aggregate_blocks";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-blocks-a@x.co").await;
    let store = PostgresStore::new(owner.clone());
    let h = hour(2026, 7, 21, 9);

    sqlx::query("INSERT INTO coding_sessions(graph_id, session_id, title) VALUES ($1, $2, $3)")
        .bind(a.graph_id)
        .bind("sess-titled")
        .bind("A Real Session Title")
        .execute(&owner)
        .await
        .unwrap();

    // Two checkpoints of the SAME titled session, one with a trailing
    // `:chunk:N` suffix — both must fold into ONE block.
    insert_atom(
        &owner, a.graph_id, a.user_id, "coding_session", "cc:sess-titled:ckpt-1",
        h + Duration::minutes(1), None, None,
    )
    .await;
    insert_atom(
        &owner, a.graph_id, a.user_id, "coding_session", "cc:sess-titled:ckpt-2:chunk:0",
        h + Duration::minutes(2), None, None,
    )
    .await;

    // A coding-session atom whose session has NO `coding_sessions.title`
    // (never titled) but does have its own `atoms.title` — falls back to it.
    // `insert_atom`'s `content_inline` param is NOT `atoms.title` (a
    // separate column — migration `0097_atoms_title`), so `title` is set via
    // a follow-up UPDATE.
    let untitled_session_atom = insert_atom(
        &owner, a.graph_id, a.user_id, "coding_session", "cc:sess-untitled:ckpt-1",
        h + Duration::minutes(3), None, None,
    )
    .await;
    sqlx::query("UPDATE atoms SET title = $1 WHERE atom_id = $2")
        .bind("headline for untitled session")
        .bind(untitled_session_atom)
        .execute(&owner)
        .await
        .unwrap();

    // A non-coding-session atom with a chunked source_id — two chunks of the
    // SAME meeting must fold into ONE block, titled from its own atoms.title.
    let meeting_atom = insert_atom(
        &owner, a.graph_id, a.user_id, "transcript", "meeting-1:chunk:0",
        h + Duration::minutes(4), None, None,
    )
    .await;
    sqlx::query("UPDATE atoms SET title = $1 WHERE atom_id = $2")
        .bind("Weekly sync headline")
        .bind(meeting_atom)
        .execute(&owner)
        .await
        .unwrap();
    insert_atom(
        &owner, a.graph_id, a.user_id, "transcript", "meeting-1:chunk:1",
        h + Duration::minutes(5), None, None,
    )
    .await;

    // A totally untitled, untitleable atom — falls back to the generic
    // source_type label.
    insert_atom(&owner, a.graph_id, a.user_id, "slack", "slack-thread-1", h + Duration::minutes(6), None, None)
        .await;

    let mut tx = owner.begin().await.expect("begin plain tx on owner pool");
    // `journal_range_aggregate` requires SOME transaction to satisfy its
    // signature (its RLS contract is proven separately in test 5a above and
    // in test (4)); this test is only about grouping/title logic, so a plain
    // (BYPASSRLS, unscoped) transaction on the owner pool is fine here.
    let agg = store
        .journal_range_aggregate(&mut tx, a.user_id, &[a.graph_id], hour(2026, 7, 21, 0), hour(2026, 7, 22, 0))
        .await
        .expect("journal_range_aggregate must succeed");
    tx.rollback().await.ok();

    assert_eq!(agg.len(), 1, "all atoms land in the same hour: {agg:?}");
    let blocks = &agg[0].blocks;
    assert_eq!(agg[0].atom_count, 6, "must count every seeded atom: {agg:?}");

    assert_eq!(blocks.len(), 4, "4 distinct blocks: {blocks:?}");
    let titled_session = blocks
        .iter()
        .find(|b| b.title == "A Real Session Title")
        .unwrap_or_else(|| panic!("titled coding-session block missing: {blocks:?}"));
    assert_eq!(titled_session.source_type, "coding_session");
    assert_eq!(titled_session.atom_count, 2, "the chunk suffix must not split the session's checkpoints: {blocks:?}");

    let untitled_session = blocks
        .iter()
        .find(|b| b.title == "headline for untitled session")
        .unwrap_or_else(|| panic!("untitled-session-falls-back-to-atom-title block missing: {blocks:?}"));
    assert_eq!(untitled_session.source_type, "coding_session");
    assert_eq!(untitled_session.atom_count, 1);

    let meeting = blocks
        .iter()
        .find(|b| b.title == "Weekly sync headline")
        .unwrap_or_else(|| panic!("meeting block missing: {blocks:?}"));
    assert_eq!(meeting.source_type, "transcript");
    assert_eq!(meeting.atom_count, 2, "the chunk suffix must not split the meeting's two chunks: {blocks:?}");

    let slack = blocks
        .iter()
        .find(|b| b.source_type == "slack")
        .unwrap_or_else(|| panic!("slack block missing: {blocks:?}"));
    assert_eq!(slack.title, "Slack", "an untitleable atom falls back to the generic source_type label: {blocks:?}");
    assert_eq!(slack.atom_count, 1);

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// An hour that satisfies EVERY bound `sweep_pass` applies to a compile
/// candidate, on any day this suite is ever run.
///
/// # Why this replaced `journal_epoch()`
///
/// These tests used to anchor `hour_start` at `journal_epoch()` itself,
/// reasoning that the first instant the no-backfill floor allows is "guaranteed
/// closed ... with no dependency on wall-clock arithmetic". The closed-hour part
/// was right. The problem is that `sweep_pass` applies a SECOND bound the
/// reasoning never accounted for:
///
/// ```text
/// lookback_from(now) = max(now - LOOKBACK_DAYS(14), journal_epoch())
/// ```
///
/// That is a sliding floor. While `now < epoch + 14 days` the `max` returns the
/// epoch, an hour seeded AT the epoch sits exactly on the boundary, and the
/// tests pass. Once wall-clock time passes `epoch + 14 days`, the `max` starts
/// returning `now - 14 days` instead, and the seeded hour falls permanently
/// outside the window — no candidate, `Tick { processed: 0 }`, forever.
///
/// `DEFAULT_JOURNAL_EPOCH` is `2026-08-02T00:00:00Z`, so this detonated on
/// 2026-08-16 and took 7 tests with it. It was not caused by any code change,
/// which is what made it confusing: `main` simply went red overnight and
/// blocked every push from every branch via the pre-push gate.
///
/// # What this returns instead
///
/// Three hours before now, truncated to the hour boundary. That is inside the
/// 14-day window by a wide margin, is closed well past `CLOSE_GRACE_MINUTES`
/// (5) and `TIER_CLOSE_GRACE_MINUTES` (15), and stays after the epoch for any
/// run at least three hours past it. Nothing here drifts toward a boundary as
/// time passes, which is the property the original anchor lacked.
fn seedable_closed_hour() -> DateTime<Utc> {
    let h = Utc::now() - Duration::hours(3);
    let hour = Utc
        .with_ymd_and_hms(h.year(), h.month(), h.day(), h.hour(), 0, 0)
        .single()
        .expect("a UTC hour boundary is never ambiguous");
    assert!(
        hour >= journal_epoch(),
        "the seeded hour must still be at or after the no-backfill floor — run this suite at \
         least three hours after JOURNAL_EPOCH ({}), got {hour}",
        journal_epoch()
    );
    hour
}

async fn scratch_consumer_tenant(base: &str, db: &str) -> (PgPool, WorkerTenant) {
    let mut admin_conn = admin(base).await;
    recreate(&mut admin_conn, db).await;
    let pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&pool).await.expect("apply control tree");
    let t = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())),
    };
    (pool, t)
}

/// 5c. Regression (CodeRabbit round 1, Major): a `session_id` containing
/// colons — `add_context`'s `session_id`/`checkpoint_id` validation enforces
/// only a length cap, never a charset restriction (`mcp/tools/write.rs`) —
/// must group ALL its checkpoints into ONE block and resolve
/// `coding_sessions.title`, exactly like a colon-free `session_id` does in
/// the test above. Proves `journal_range_aggregate` no longer truncates
/// `session_id` at the FIRST colon via `split_part(source_id, ':', 2)`:
/// `"cc:team:alpha:ckpt-1"` used to fold to grouping key `"team"`, splitting
/// one session across multiple blocks and missing the `coding_sessions`
/// title join. Also proves a DIFFERENT session whose id happens to equal
/// that truncated prefix (`"team"`) lands in its own block rather than
/// silently merging with the colon-bearing one.
#[tokio::test]
async fn journal_range_aggregate_groups_a_colon_bearing_session_id_into_one_block() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping journal_range_aggregate_groups_a_colon_bearing_session_id_into_one_block: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "jr_aggregate_colon_session";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-colon-a@x.co").await;
    let store = PostgresStore::new(owner.clone());
    let h = hour(2026, 7, 22, 9);

    let session_id = "team:alpha";
    sqlx::query("INSERT INTO coding_sessions(graph_id, session_id, title) VALUES ($1, $2, $3)")
        .bind(a.graph_id)
        .bind(session_id)
        .bind("Team Alpha's Session")
        .execute(&owner)
        .await
        .unwrap();

    // Two checkpoints of the SAME colon-bearing session — must fold into ONE
    // block, exactly like the colon-free case in the test above.
    insert_atom(
        &owner, a.graph_id, a.user_id, "coding_session", "cc:team:alpha:ckpt-1",
        h + Duration::minutes(1), None, None,
    )
    .await;
    insert_atom(
        &owner, a.graph_id, a.user_id, "coding_session", "cc:team:alpha:ckpt-2",
        h + Duration::minutes(2), None, None,
    )
    .await;

    // A totally unrelated SECOND session in the same hour — must land in its
    // own, separate block, not merge with the colon-bearing one above (the
    // old `split_part(..., ':', 2)` bug would have folded "team:alpha" to
    // grouping key "team", which happens to share no characters with this
    // session's id, so this alone would NOT have caught the regression —
    // the assertions on `team_alpha` below are what actually pin it).
    sqlx::query("INSERT INTO coding_sessions(graph_id, session_id, title) VALUES ($1, $2, $3)")
        .bind(a.graph_id)
        .bind("gamma")
        .bind("Just Gamma")
        .execute(&owner)
        .await
        .unwrap();
    insert_atom(
        &owner, a.graph_id, a.user_id, "coding_session", "cc:gamma:ckpt-1",
        h + Duration::minutes(3), None, None,
    )
    .await;

    let mut tx = owner.begin().await.expect("begin plain tx on owner pool");
    // `journal_range_aggregate` requires SOME transaction to satisfy its
    // signature (its RLS contract is proven separately in test 5a above); a
    // plain (BYPASSRLS, unscoped) transaction on the owner pool is fine here.
    let agg = store
        .journal_range_aggregate(&mut tx, a.user_id, &[a.graph_id], hour(2026, 7, 22, 0), hour(2026, 7, 23, 0))
        .await
        .expect("journal_range_aggregate must succeed");
    tx.rollback().await.ok();

    assert_eq!(agg.len(), 1, "all atoms land in the same hour: {agg:?}");
    let blocks = &agg[0].blocks;
    assert_eq!(agg[0].atom_count, 3, "must count every seeded atom: {agg:?}");
    assert_eq!(
        blocks.len(), 2,
        "the colon-bearing session and the \"gamma\" session must be DISTINCT blocks: {blocks:?}"
    );

    let team_alpha = blocks
        .iter()
        .find(|b| b.title == "Team Alpha's Session")
        .unwrap_or_else(|| panic!("colon-bearing session's titled block missing: {blocks:?}"));
    assert_eq!(team_alpha.source_type, "coding_session");
    assert_eq!(
        team_alpha.atom_count, 2,
        "BOTH checkpoints of the colon-bearing session must fold into ONE block, not split at the embedded colon: {blocks:?}"
    );

    let gamma = blocks
        .iter()
        .find(|b| b.title == "Just Gamma")
        .unwrap_or_else(|| panic!("the unrelated second session's own block missing: {blocks:?}"));
    assert_eq!(
        gamma.atom_count, 1,
        "the unrelated second session must not absorb the colon-bearing session's checkpoints: {blocks:?}"
    );

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// 5d. Regression (CodeRabbit round 1, Major, on top of 5c): `UNIQUE
/// (graph_id, session_id)` does NOT prevent one session id from being a
/// literal PREFIX of another — "team" and "team:alpha" can coexist as two
/// separate `coding_sessions` rows. A checkpoint atom under the LONGER
/// session ("cc:team:alpha:ckpt-1") satisfies the escaped-LIKE
/// `"cc:{session_id}:%"` pattern for BOTH rows ("cc:team:alpha:ckpt-1" LIKE
/// "cc:team:%" is also true), so a plain `LEFT JOIN` — with no tiebreak —
/// matches (and fans the atom out across) both rows: the atom would be
/// double-counted and duplicated in the compile prompt. Both
/// `journal_hour_inputs` (PR B's sweep input) and `journal_range_aggregate`
/// (PR C's live aggregate) share the same escaped-LIKE join, so both must
/// resolve the atom to its MOST SPECIFIC (longest) matching session ---
/// "team:alpha", never "team" --- and each must return it exactly once.
#[tokio::test]
async fn journal_hour_inputs_and_range_aggregate_resolve_the_longest_matching_session_when_ids_overlap_as_prefixes() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping journal_hour_inputs_and_range_aggregate_resolve_the_longest_matching_session_when_ids_overlap_as_prefixes: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "jr_overlap_session_prefix";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner: PgPool =
        PgPoolOptions::new().connect(&with_db(&base, db)).await.expect("connect owner");
    sqlx::migrate!("./migrations_control").run(&owner).await.expect("apply control tree");

    let a = seed_account(&owner, "jr-overlap-a@x.co").await;
    let store = PostgresStore::new(owner.clone());
    let h = hour(2026, 7, 24, 9);

    // Two sessions where one id is a literal prefix of the other.
    sqlx::query("INSERT INTO coding_sessions(graph_id, session_id, title) VALUES ($1, $2, $3)")
        .bind(a.graph_id)
        .bind("team")
        .bind("Just Team")
        .execute(&owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO coding_sessions(graph_id, session_id, title) VALUES ($1, $2, $3)")
        .bind(a.graph_id)
        .bind("team:alpha")
        .bind("Team Alpha's Session")
        .execute(&owner)
        .await
        .unwrap();

    // A single checkpoint under the LONGER session — its source_id matches
    // BOTH sessions' escaped-LIKE pattern.
    let atom_id = insert_atom(
        &owner, a.graph_id, a.user_id, "coding_session", "cc:team:alpha:ckpt-1",
        h + Duration::minutes(1), None, None,
    )
    .await;

    // `journal_hour_inputs`: the atom must appear exactly once, resolved to
    // the longer session.
    let inputs = store
        .journal_hour_inputs(a.user_id, a.graph_id, h)
        .await
        .expect("journal_hour_inputs must succeed");
    assert_eq!(
        inputs.atoms.len(), 1,
        "the checkpoint must not be duplicated across the two overlapping sessions: {:?}",
        inputs.atoms
    );
    assert_eq!(inputs.atoms[0].atom_id, atom_id);
    assert_eq!(
        inputs.atoms[0].coding_session_id.as_deref(), Some("team:alpha"),
        "must resolve to the MOST SPECIFIC session, not the shorter prefix \"team\": {:?}",
        inputs.atoms[0]
    );

    // `journal_range_aggregate`: same atom, same requirement — one block,
    // titled from the longer session, never split or duplicated across both.
    let mut tx = owner.begin().await.expect("begin plain tx on owner pool");
    let agg = store
        .journal_range_aggregate(&mut tx, a.user_id, &[a.graph_id], hour(2026, 7, 24, 0), hour(2026, 7, 25, 0))
        .await
        .expect("journal_range_aggregate must succeed");
    tx.rollback().await.ok();

    assert_eq!(agg.len(), 1, "all atoms land in the same hour: {agg:?}");
    assert_eq!(agg[0].atom_count, 1, "the checkpoint must not be double-counted: {agg:?}");
    assert_eq!(
        agg[0].blocks.len(), 1,
        "the checkpoint must land in exactly ONE block, not one per overlapping session: {:?}",
        agg[0].blocks
    );
    assert_eq!(
        agg[0].blocks[0].title, "Team Alpha's Session",
        "must resolve to the MOST SPECIFIC session's title, not \"Just Team\": {:?}",
        agg[0].blocks[0]
    );
    assert_eq!(agg[0].blocks[0].atom_count, 1);

    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

// ── `workers::journal_sweep` entry points: the managed switches ────────────
//
// Source-agnostic plan PR 6 moved the sweep onto `managed_enrichment`. While
// the shared switch (`ENRICHMENT_PROCESSING_ENABLED`) is not exactly `true`,
// or the journal's own switch (`JOURNAL_SWEEP_ENABLED`) is `false`, `tick`
// and `sweep_pass` return `Tick::idle()` before the advisory lock, the dirty
// scan, the compile call, or any write. The two tests below pin both switches
// (production runs with the shared switch on, so each is the rollback
// lever). The managed compile is proven in
// `managed::gate::pg_tests::enrichment_consumers`.

/// The two switch states that must keep the sweep idle, restored on drop.
struct SwitchEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl SwitchEnv {
    fn set(enrichment: Option<&str>, journal: Option<&str>) -> Self {
        let saved = ["ENRICHMENT_PROCESSING_ENABLED", "JOURNAL_SWEEP_ENABLED"].map(|k| (k, std::env::var_os(k)));
        for (key, value) in [("ENRICHMENT_PROCESSING_ENABLED", enrichment), ("JOURNAL_SWEEP_ENABLED", journal)] {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
        Self(saved.into())
    }
}
impl Drop for SwitchEnv {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }
}
/// (shared switch, journal switch) states that must leave the sweep idle.
const OFF_STATES: [(Option<&str>, Option<&str>); 3] = [(None, None), (Some("false"), None), (Some("true"), Some("false"))];

/// Fails the test on first contact: a switched-off sweep must never reach a
/// model call.
struct PanicLlm;

#[async_trait::async_trait]
impl LlmProvider for PanicLlm {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        panic!("a switched-off JournalSweep must never reach the model")
    }

    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError> {
        panic!("a switched-off JournalSweep must never reach the model")
    }
}

/// A consumer-plane tenant over a pool that is already closed. Any query the
/// worker issues fails with a pool error instead of succeeding quietly, so a
/// `Tick::idle()` from it proves the worker touched no connection at all.
/// That subsumes the retired production-incident fixture (a pooled
/// connection left with the `app.account_id` placeholder): a connection the
/// worker never acquires cannot be poisoned.
async fn closed_consumer_tenant(base: &str, db: &str) -> WorkerTenant {
    let pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.expect("connect closed-pool tenant");
    pool.close().await;
    WorkerTenant { tenant: TenantId::Consumer, pool: pool.clone(), store: Arc::new(PostgresStore::new(pool)) }
}

async fn journal_rollup_count(owner: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM journal_rollups").fetch_one(owner).await.expect("count journal_rollups")
}

/// `tick` on a genuinely dirty hour (the owner-pool store reports it as a
/// candidate, so idle is attributable to the admission block and not to an
/// empty fixture), run twice concurrently the way the advisory-lock race did:
/// both calls return idle, nothing is written, and the model is never called.
#[tokio::test]
async fn tick_is_idle_while_switched_off() {
    let Some(base) = base_url() else {
        eprintln!("skipping tick_is_idle_while_switched_off: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "jr_sweep_admission_tick";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let (owner, _) = scratch_consumer_tenant(&base, db).await;

    let a = seed_account(&owner, "jr-sweep-admission-tick@x.co").await;
    let hour_start = seedable_closed_hour();
    insert_atom(
        &owner,
        a.graph_id,
        a.user_id,
        "note",
        "n1",
        hour_start + Duration::minutes(5),
        Some("Must never reach journal_rollups."),
        None,
    )
    .await;
    let dirty = PostgresStore::new(owner.clone())
        .journal_dirty_hours(hour_start, hour_start + Duration::hours(2))
        .await
        .expect("dirty scan");
    assert!(
        dirty.iter().any(|d| d.hour_start == hour_start),
        "the fixture hour must be a real compile candidate, or idle proves nothing: {dirty:?}"
    );

    let t = closed_consumer_tenant(&base, db).await;
    for (enrichment, journal) in OFF_STATES {
        let _env = SwitchEnv::set(enrichment, journal);
        let (r1, r2) = tokio::join!(tick(&t, &t.pool, &PanicLlm), tick(&t, &t.pool, &PanicLlm));
        assert_eq!(r1.expect("tick must not error"), Tick::idle(), "{enrichment:?}/{journal:?}: switched off");
        assert_eq!(r2.expect("tick must not error"), Tick::idle(), "{enrichment:?}/{journal:?}: switched off");
    }
    {
        // Red control: switched on, the same tick reaches the (closed) plane.
        let _env = SwitchEnv::set(Some("true"), None);
        assert!(tick(&t, &t.pool, &PanicLlm).await.is_err(), "switched on, the sweep touches the plane");
    }
    assert_eq!(journal_rollup_count(&owner).await, 0, "a switched-off sweep writes nothing");

    owner.close().await;
    admin(&base).await.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

/// `sweep_pass` with an explicit `now` past a closed local day whose two hours
/// are real compile candidates (the day/week tier fixture from the retired
/// tier tests): idle, nothing written, model never called.
#[tokio::test]
async fn sweep_pass_is_idle_while_switched_off() {
    let Some(base) = base_url() else {
        eprintln!("skipping sweep_pass_is_idle_while_switched_off: set ORGT_TEST_BASE_URL");
        return;
    };
    let _epoch_guard = JournalEpochGuard::pin_default();
    let db = "jr_sweep_admission_sweep_pass";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let (owner, _) = scratch_consumer_tenant(&base, db).await;

    let a = seed_account(&owner, "jr-sweep-admission-pass@x.co").await;
    set_user_timezone(&owner, a.user_id, "America/Chicago").await;
    let hour1 = dt("2026-08-03T14:00:00Z");
    let hour2 = dt("2026-08-03T20:00:00Z");
    insert_atom(&owner, a.graph_id, a.user_id, "note", "n1", hour1 + Duration::minutes(5), Some("Wrote the day tier."), None)
        .await;
    insert_atom(&owner, a.graph_id, a.user_id, "note", "n2", hour2 + Duration::minutes(5), Some("Reviewed the churn guard."), None)
        .await;
    let dirty = PostgresStore::new(owner.clone())
        .journal_dirty_hours(hour1, hour2 + Duration::hours(2))
        .await
        .expect("dirty scan");
    assert_eq!(dirty.len(), 2, "both fixture hours must be real compile candidates: {dirty:?}");

    let t = closed_consumer_tenant(&base, db).await;
    let now = dt("2026-08-10T00:00:00Z");
    for (enrichment, journal) in OFF_STATES {
        let _env = SwitchEnv::set(enrichment, journal);
        let outcome = sweep_pass(&t, &t.pool, &PanicLlm, now).await.expect("sweep_pass must not error");
        assert_eq!(outcome, Tick::idle(), "{enrichment:?}/{journal:?}: switched off");
    }
    assert_eq!(journal_rollup_count(&owner).await, 0, "a switched-off sweep writes nothing");

    owner.close().await;
    admin(&base).await.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}
