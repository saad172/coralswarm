//! Live Postgres tests for the coding-session capture Phase 3 READ surface
//! (`docs plan`: audit D6 stage 1/2) — the `project`/`session_id`
//! `RetrievalFilter` fields threaded through `PostgresStore::vector_search`/
//! `keyword_search`/`timestamp_window_atoms`, and `PostgresStore::
//! list_coding_sessions` (the `list_sessions` MCP tool's backing query).
//! Also home to the `source_types` bind tests (GitHub connector eval,
//! `docs/connectors/github.md` §5) since they are the same "pg-side bind of
//! a `RetrievalFilter` field" shape.
//!
//! Deliberately drives `PostgresStore` DIRECTLY (not through the MCP/HTTP
//! layer — see `mcp_pg_e2e.rs` for the full `add_context` -> `list_sessions`/
//! `search_atoms` round trip over real HTTP) so these are tight, fast
//! "pg-side bind" tests of the actual SQL: real `%`/`_`/`\` escaping, real
//! keyset-cursor pagination, real correlated `coral_count` subquery — nothing
//! here is mocked or re-implemented in Rust.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied); a normal `cargo test` with no DB configured skips every test in
//! this file. `REQUIRE_PG_SUITE=1` makes a missing URL fail instead.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     REQUIRE_PG_SUITE=1 cargo test -p coralswarm-backend --test coding_sessions_read_pg -- --test-threads=1 --show-output

use chrono::Utc;
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;
use coralswarm_backend::store::{
    FacetCursor, FacetDimension, FacetPageRequest, FacetValue, SessionIdentity, SessionListFilter,
};
use serde_json::{json, Value};

macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                assert!(
                    std::env::var_os("REQUIRE_PG_SUITE").is_none(),
                    "RELINFER_TEST_DATABASE_URL required"
                );
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run coding_sessions_read_pg");
                return;
            }
        }
    }};
}

/// Seed a fresh `(user, account, graph)` tenancy chain — pattern-copied from
/// `store_provenance_pg.rs`.
async fn seed_tenancy(pool: &sqlx::PgPool) -> (Uuid, Uuid, Uuid) {
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &format!("coding-sessions-read-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");
    (user_id, account_id, graph.id)
}

async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid, user_id: Uuid) {
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&mut *financial)
        .await
        .ok();
    financial.commit().await.unwrap();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
}

/// Insert one atom with inline content (FTS-searchable via a direct
/// `content_tsv` backfill — mirrors `mcp_pg_e2e.rs`'s `mark_atoms_searchable`)
/// plus an optional `project` stamp, at a given `source_id`.
#[allow(clippy::too_many_arguments)]
async fn seed_atom(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    user_id: Uuid,
    source_id: &str,
    content: &str,
    project: Option<&str>,
) -> Uuid {
    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph_id,
            "mcp",
            &[source_id.to_string()],
            &[format!("blob-{atom_id}")],
            &[],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert_atoms");
    sqlx::query("UPDATE atoms SET content_inline = $1, content_tsv = to_tsvector('simple', $1) WHERE atom_id = $2")
        .bind(content)
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("backfill content + content_tsv");
    if let Some(p) = project {
        store
            .stamp_atom_provenance(&[atom_id], None, None, None, None, Some(p), None)
            .await
            .expect("stamp project");
    }
    atom_id
}

// ── `project`/`session_id` RetrievalFilter bind tests ───────────────────────

#[tokio::test]
async fn keyword_search_project_filter_bind_narrows_to_the_matching_project() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let a = seed_atom(&store, &pool, graph_id, user_id, "s-proj-a", "zzqbindtest77 alpha", Some("proj-a")).await;
    let b = seed_atom(&store, &pool, graph_id, user_id, "s-proj-b", "zzqbindtest77 beta", Some("proj-b")).await;

    // Unfiltered: both hit.
    let unfiltered = store
        .keyword_search(graph_id, "zzqbindtest77", 10, &RetrievalFilter::default())
        .await
        .expect("keyword_search unfiltered");
    let ids: Vec<Uuid> = unfiltered.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&a) && ids.contains(&b), "unfiltered search must find both atoms: {ids:?}");

    // project=proj-a: only `a`.
    let filter = RetrievalFilter {
        project: Some("proj-a".to_string()),
        ..Default::default()
    };
    let filtered = store
        .keyword_search(graph_id, "zzqbindtest77", 10, &filter)
        .await
        .expect("keyword_search filtered");
    assert_eq!(filtered.len(), 1, "project filter must narrow to exactly one atom: {filtered:?}");
    assert_eq!(filtered[0].0, a);

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn timestamp_window_atoms_project_filter_bind_narrows_to_the_matching_project() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let a = seed_atom(&store, &pool, graph_id, user_id, "s-window-a", "window a", Some("proj-a")).await;
    let b = seed_atom(&store, &pool, graph_id, user_id, "s-window-b", "window b", Some("proj-b")).await;

    let since = Utc::now() - chrono::Duration::hours(1);
    let until = Utc::now() + chrono::Duration::hours(1);

    let filter = RetrievalFilter {
        project: Some("proj-a".to_string()),
        ..Default::default()
    };
    let filtered = store
        .timestamp_window_atoms(graph_id, since, until, Some(50), &filter)
        .await
        .expect("timestamp_window_atoms filtered");
    let ids: Vec<Uuid> = filtered.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&a));
    assert!(!ids.contains(&b), "out-of-project atom must not appear: {ids:?}");

    cleanup(&pool, account_id, user_id).await;
}

// ── `source_types` RetrievalFilter bind tests (GitHub connector eval §5) ───

/// Seed one FTS-searchable atom of a given `source_type` — the
/// `seed_atom` helper above hardcodes `"mcp"`, and this suite's new tests
/// are specifically about the `source_type` column.
async fn seed_typed_atom(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    user_id: Uuid,
    source_type: &str,
    source_id: &str,
    content: &str,
) -> Uuid {
    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph_id,
            source_type,
            &[source_id.to_string()],
            &[format!("blob-{atom_id}")],
            &[],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert_atoms");
    sqlx::query("UPDATE atoms SET content_inline = $1, content_tsv = to_tsvector('simple', $1) WHERE atom_id = $2")
        .bind(content)
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("backfill content + content_tsv");
    atom_id
}

/// The real `($n::text[] IS NULL OR source_type = ANY($n))` bind, driven
/// against Postgres: a two-element `source_types` list matches both listed
/// types and nothing else; a NULL array (no filter at all) is still "no
/// restriction"; the legacy single `source_type` still narrows to exactly
/// one type through the same parameter.
#[tokio::test]
async fn keyword_search_source_types_bind_matches_any_listed_type() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let code = seed_typed_atom(&store, &pool, graph_id, user_id, "github_code", "1:blob:a.rs", "zzqtypesbind31 code").await;
    let summary = seed_typed_atom(&store, &pool, graph_id, user_id, "github_code_summary", "1:summary:.", "zzqtypesbind31 summary").await;
    let pr = seed_typed_atom(&store, &pool, graph_id, user_id, "github_pr", "1:pr:9", "zzqtypesbind31 pr").await;

    // NULL array: everything.
    let all = store
        .keyword_search(graph_id, "zzqtypesbind31", 10, &RetrievalFilter::default())
        .await
        .expect("keyword_search unfiltered");
    let ids: Vec<Uuid> = all.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&code) && ids.contains(&summary) && ids.contains(&pr), "unfiltered: {ids:?}");

    // Two-element list: both tiers, not the PR.
    let filter = RetrievalFilter {
        source_types: Some(vec!["github_code".to_string(), "github_code_summary".to_string()]),
        ..Default::default()
    };
    let both = store
        .keyword_search(graph_id, "zzqtypesbind31", 10, &filter)
        .await
        .expect("keyword_search source_types");
    let mut ids: Vec<Uuid> = both.iter().map(|(id, _)| *id).collect();
    ids.sort();
    let mut want = vec![code, summary];
    want.sort();
    assert_eq!(ids, want, "source_types must match exactly the listed types");

    // Legacy single value through the same parameter: one type.
    let filter = RetrievalFilter {
        source_type: Some("github_pr".to_string()),
        ..Default::default()
    };
    let single = store
        .keyword_search(graph_id, "zzqtypesbind31", 10, &filter)
        .await
        .expect("keyword_search source_type");
    let ids: Vec<Uuid> = single.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![pr], "single source_type must still narrow to one type");

    cleanup(&pool, account_id, user_id).await;
}

/// Precedence, proven on the SQL side: when both fields are set, the list
/// wins outright (the single value is NOT unioned in), and an empty list
/// defers to the single value rather than binding an empty array that
/// would match nothing.
#[tokio::test]
async fn keyword_search_source_types_wins_over_source_type_and_empty_list_defers() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let summary = seed_typed_atom(&store, &pool, graph_id, user_id, "github_code_summary", "2:summary:.", "zzqtypesprec58 summary").await;
    let pr = seed_typed_atom(&store, &pool, graph_id, user_id, "github_pr", "2:pr:9", "zzqtypesprec58 pr").await;

    let filter = RetrievalFilter {
        source_type: Some("github_pr".to_string()),
        source_types: Some(vec!["github_code_summary".to_string()]),
        ..Default::default()
    };
    let list_wins = store
        .keyword_search(graph_id, "zzqtypesprec58", 10, &filter)
        .await
        .expect("keyword_search list wins");
    let ids: Vec<Uuid> = list_wins.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![summary], "non-empty source_types must override source_type");

    let filter = RetrievalFilter {
        source_type: Some("github_pr".to_string()),
        source_types: Some(Vec::new()),
        ..Default::default()
    };
    let empty_defers = store
        .keyword_search(graph_id, "zzqtypesprec58", 10, &filter)
        .await
        .expect("keyword_search empty list");
    let ids: Vec<Uuid> = empty_defers.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![pr], "an empty source_types must fall back to source_type");

    // The same parameter on the timestamp-window path (event-window scans).
    let since = Utc::now() - chrono::Duration::hours(1);
    let until = Utc::now() + chrono::Duration::hours(1);
    let filter = RetrievalFilter {
        source_types: Some(vec!["github_code_summary".to_string(), "github_pr".to_string()]),
        ..Default::default()
    };
    let window = store
        .timestamp_window_atoms(graph_id, since, until, Some(50), &filter)
        .await
        .expect("timestamp_window_atoms source_types");
    let mut ids: Vec<Uuid> = window.iter().map(|(id, _)| *id).collect();
    ids.sort();
    let mut want = vec![summary, pr];
    want.sort();
    assert_eq!(ids, want, "timestamp_window_atoms must honour the same list");

    cleanup(&pool, account_id, user_id).await;
}

/// The session_id filter's `%`/`_`/`\` escaping (`session_source_id_prefix`)
/// AND the `:` separator that stops one session id from prefix-matching a
/// DIFFERENT session whose id happens to start with the same text — both
/// proven against a REAL Postgres `LIKE` evaluation, not a Rust re-
/// implementation.
#[tokio::test]
async fn keyword_search_session_id_filter_escapes_metacharacters_and_never_widens() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // "sess-a" vs "sess-ax" — must NOT cross-match.
    let a = seed_atom(&store, &pool, graph_id, user_id, "cc:sess-a:ckpt-1", "zzqsessbind42 a", None).await;
    let widener =
        seed_atom(&store, &pool, graph_id, user_id, "cc:sess-ax:ckpt-1", "zzqsessbind42 widened", None).await;

    let filter = RetrievalFilter {
        session_id: Some("sess-a".to_string()),
        ..Default::default()
    };
    let results = store
        .keyword_search(graph_id, "zzqsessbind42", 10, &filter)
        .await
        .expect("keyword_search session filtered");
    let ids: Vec<Uuid> = results.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&a), "must find the exact session's atom: {ids:?}");
    assert!(!ids.contains(&widener), "must NOT prefix-match \"sess-ax\": {ids:?}");

    // A session id containing literal `%`/`_` — must match only its own atom.
    let literal = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "cc:a%b_c:ckpt-1",
        "zzqsessliteral99 literal",
        None,
    )
    .await;
    let decoy = seed_atom(
        &store,
        &pool,
        graph_id,
        user_id,
        "cc:aXbYc:ckpt-1",
        "zzqsessliteral99 decoy",
        None,
    )
    .await;
    let filter = RetrievalFilter {
        session_id: Some("a%b_c".to_string()),
        ..Default::default()
    };
    let results = store
        .keyword_search(graph_id, "zzqsessliteral99", 10, &filter)
        .await
        .expect("keyword_search literal-metacharacter session filtered");
    let ids: Vec<Uuid> = results.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![literal], "literal %/_ must not act as a wildcard: {ids:?} (decoy={decoy})");

    cleanup(&pool, account_id, user_id).await;
}

/// Best-effort `EXPLAIN` check that the session_id prefix-LIKE query CAN be
/// served by `idx_atoms_graph_source_id_pattern` (migration 0074): seeds
/// enough decoy atoms to make the index attractive to the planner, runs
/// `ANALYZE`, then EXPLAINs the exact query shape `keyword_search`/
/// `vector_search`/`timestamp_window_atoms` all use for the session filter
/// (`graph_id = $1 AND source_id LIKE $2`). This is inherently sensitive to
/// planner heuristics on a small/freshly-created test table, so a plan that
/// does NOT mention the index is logged loudly rather than failing the
/// build — the query FORM (bind shape, escaping) is what's contractually
/// guaranteed and is covered by the hard-asserting tests above; this test is
/// additional, best-effort visibility into whether the index is actually
/// reachable for this predicate shape.
#[tokio::test]
async fn timestamp_window_atoms_session_prefix_query_can_use_the_source_id_pattern_index() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Seed a target atom plus enough decoys to bias the planner toward the
    // index over a sequential scan.
    let _target = seed_atom(&store, &pool, graph_id, user_id, "cc:explain-target:ckpt-1", "explain target", None)
        .await;
    for i in 0..200 {
        seed_atom(
            &store,
            &pool,
            graph_id,
            user_id,
            &format!("cc:explain-decoy-{i}:ckpt-1"),
            "explain decoy",
            None,
        )
        .await;
    }
    sqlx::query("ANALYZE atoms").execute(&pool).await.expect("ANALYZE atoms");

    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT atom_id, timestamp FROM atoms \
         WHERE graph_id = $1 AND source_id LIKE $2",
    )
    .bind(graph_id)
    .bind("cc:explain-target:%")
    .fetch_all(&pool)
    .await
    .expect("EXPLAIN query");
    let plan_text = plan.join("\n");
    println!("EXPLAIN plan for session-prefix query:\n{plan_text}");
    if plan_text.contains("idx_atoms_graph_source_id_pattern") {
        println!("index confirmed reachable for this predicate shape");
    } else {
        eprintln!(
            "NOTE: idx_atoms_graph_source_id_pattern did not appear in the plan — likely planner \
             heuristics on this small test table (Seq Scan can still win under `graph_id = $1` \
             alone on a table this size). Query FORM correctness is covered by the hard-asserting \
             escaping/narrowing tests in this file; this check is informational only.\nplan:\n{plan_text}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
}

// ── `list_coding_sessions` (the `list_sessions` MCP tool's backing query) ──

#[tokio::test]
async fn list_coding_sessions_orders_by_recent_activity_and_computes_coral_count_in_one_query() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    store
        .upsert_coding_session(graph_id, "sess-1", "coding", Some("proj-a"), Some("Session One"), None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .expect("upsert sess-1");
    let version1 = capture_session(&pool, graph_id, "sess-1", json!({
        "session_kind": "coding", "project_key": "proj-a", "session_title": "Session One"
    }), "permitted").await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    store
        .upsert_coding_session(graph_id, "sess-2", "coding", Some("proj-a"), Some("Session Two"), None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .expect("upsert sess-2");
    let version2 = capture_session(&pool, graph_id, "sess-2", json!({
        "session_kind": "coding", "project_key": "proj-a", "session_title": "Session Two"
    }), "permitted").await;

    let atom1 = seed_atom(&store, &pool, graph_id, user_id, "cc:sess-1:ckpt-1", "checkpoint one", None).await;
    let atom2 = seed_atom(&store, &pool, graph_id, user_id, "cc:sess-1:ckpt-2", "checkpoint two", None).await;
    let atom3 = seed_atom(&store, &pool, graph_id, user_id, "cc:sess-2:ckpt-1", "checkpoint three", None).await;
    let mut tx = pool.begin().await.unwrap();
    coralswarm_backend::source_evidence::link_verified_chunks_tx(&mut tx, graph_id, version1, &[atom1, atom2]).await.unwrap();
    coralswarm_backend::source_evidence::link_verified_chunks_tx(&mut tx, graph_id, version2, &[atom3]).await.unwrap();
    tx.commit().await.unwrap();

    let rows = store
        .list_coding_sessions(graph_id, None, None, None, None, 10, &SessionListFilter::default())
        .await
        .expect("list_coding_sessions");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].session_id, "sess-2");
    assert_eq!(rows[1].session_id, "sess-1");
    let sess1 = rows.iter().find(|r| r.session_id == "sess-1").unwrap();
    let sess2 = rows.iter().find(|r| r.session_id == "sess-2").unwrap();
    assert_eq!(sess1.coral_count, 2, "sess-1 has 2 corals: {sess1:?}");
    assert_eq!(sess2.coral_count, 1, "sess-2 has 1 coral: {sess2:?}");

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn list_coding_sessions_project_filter_and_empty_project_case() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    store
        .upsert_coding_session(graph_id, "sess-a", "coding", Some("proj-a"), None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .unwrap();
    capture_session(&pool, graph_id, "sess-a", json!({
        "session_kind": "coding", "project_key": "proj-a"
    }), "permitted").await;
    store
        .upsert_coding_session(graph_id, "sess-b", "coding", Some("proj-b"), None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .unwrap();
    capture_session(&pool, graph_id, "sess-b", json!({
        "session_kind": "coding", "project_key": "proj-b"
    }), "permitted").await;

    let proj_a = store
        .list_coding_sessions(graph_id, Some("proj-a"), None, None, None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    assert_eq!(proj_a.len(), 1);
    assert_eq!(proj_a[0].session_id, "sess-a");

    // Empty-project case: a project value with zero matching sessions is a
    // normal empty result, not an error.
    let empty = store
        .list_coding_sessions(graph_id, Some("proj-does-not-exist"), None, None, None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    assert!(empty.is_empty(), "an unmatched project filter must return an empty list: {empty:?}");

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn list_coding_sessions_requires_permitted_capture_and_uses_captured_context() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    for sid in ["descriptor-only", "restricted-capture", "permitted-capture"] {
        store.upsert_coding_session(
            graph_id, sid, "coding", Some("initial-project"), Some("Initial title"),
            None, None, None, None, Some("initial-model"), None, &SessionIdentity::default(),
        ).await.unwrap();
    }
    capture_session(&pool, graph_id, "restricted-capture", json!({
        "session_kind": "coding", "project_key": "restricted-project",
        "session_title": "Restricted title", "model": "restricted-model"
    }), "restricted").await;
    capture_session(&pool, graph_id, "permitted-capture", json!({
        "session_kind": "coding", "project_key": "captured-project",
        "session_title": "Captured title", "model": "captured-model"
    }), "permitted").await;
    store.upsert_coding_session(
        graph_id, "permitted-capture", "coding", Some("mutable-project"), Some("Mutable title"),
        None, None, None, None, Some("mutable-model"), None, &SessionIdentity::default(),
    ).await.unwrap();

    let rows = store.list_coding_sessions(
        graph_id, None, None, None, None, 10, &SessionListFilter::default(),
    ).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.session_id.as_str()).collect::<Vec<_>>(), vec!["permitted-capture"]);
    assert_eq!(rows[0].session_kind, "coding");
    assert_eq!(rows[0].project_key.as_deref(), Some("captured-project"));
    assert_eq!(rows[0].title.as_deref(), Some("Captured title"));
    assert_eq!(rows[0].model.as_deref(), Some("captured-model"));

    for (project, expected) in [("captured-project", vec!["permitted-capture"]), ("mutable-project", vec![])] {
        let rows = store.list_coding_sessions(
            graph_id, Some(project), None, None, None, 10, &SessionListFilter::default(),
        ).await.unwrap();
        assert_eq!(rows.iter().map(|row| row.session_id.as_str()).collect::<Vec<_>>(), expected);
    }
    for (model, expected) in [("captured-model", vec!["permitted-capture"]), ("mutable-model", vec![])] {
        let rows = store.list_coding_sessions(
            graph_id, None, None, None, None, 10,
            &SessionListFilter { model: Some(model.into()), ..Default::default() },
        ).await.unwrap();
        assert_eq!(rows.iter().map(|row| row.session_id.as_str()).collect::<Vec<_>>(), expected);
    }

    cleanup(&pool, account_id, user_id).await;
}

/// The `session_kind` filter, and the reason it lives in SQL rather than in
/// the browser (capture unit 07 item 5).
///
/// `coding_sessions` has held all three kinds since migration
/// `0185_coding_sessions_session_kind`, so "list the coding sessions" and
/// "list the agent sessions" are the SAME table with different predicates.
///
/// The pagination half is the point. Every `agent` session here is OLDER than
/// every `coding` session, so an unfiltered first page of size 2 contains
/// only coding sessions. A client that fetched that page and dropped the
/// wrong kinds in memory would render an EMPTY agent-sessions facet while two
/// agent sessions sat in the ocean — an empty list that is not evidence of an
/// empty ocean, which is the `absence-renders-as-success` failure the facet
/// was specified to close. The assertion below is that the SQL filter does
/// NOT behave that way.
#[tokio::test]
async fn list_coding_sessions_kind_filter_is_applied_before_the_page_limit() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Oldest first, so `last_activity_at DESC` puts the two coding sessions
    // at the head of the unfiltered list and the two agent sessions behind
    // them.
    for (sid, kind) in [
        ("agent-1", "agent"),
        ("agent-2", "agent"),
        ("code-1", "coding"),
        ("code-2", "coding"),
    ] {
        store
            .upsert_coding_session(graph_id, sid, kind, None, None, None, None, None, None, None, None, &SessionIdentity::default())
            .await
            .unwrap_or_else(|e| panic!("upsert {sid}: {e}"));
        capture_session(&pool, graph_id, sid, json!({"session_kind": kind}), "permitted").await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Control: unfiltered, page size 2 — only the coding sessions come back.
    // This is what a client-side filter would have had to work from.
    let unfiltered_first_page = store
        .list_coding_sessions(graph_id, None, None, None, None, 2, &SessionListFilter::default())
        .await
        .unwrap();
    assert_eq!(unfiltered_first_page.len(), 2);
    assert!(
        unfiltered_first_page.iter().all(|r| r.session_kind == "coding"),
        "precondition: the unfiltered first page must hold no agent sessions, else this test proves nothing: {unfiltered_first_page:?}"
    );

    // The filter, at the same page size: both agent sessions, and nothing else.
    let agent = store
        .list_coding_sessions(graph_id, None, Some("agent"), None, None, 2, &SessionListFilter::default())
        .await
        .unwrap();
    let mut agent_ids: Vec<_> = agent.iter().map(|r| r.session_id.clone()).collect();
    agent_ids.sort();
    assert_eq!(
        agent_ids,
        vec!["agent-1".to_string(), "agent-2".to_string()],
        "the kind filter must be applied in SQL, before LIMIT: {agent:?}"
    );
    assert!(agent.iter().all(|r| r.session_kind == "agent"));

    let coding = store
        .list_coding_sessions(graph_id, None, Some("coding"), None, None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    let mut coding_ids: Vec<_> = coding.iter().map(|r| r.session_id.clone()).collect();
    coding_ids.sort();
    assert_eq!(coding_ids, vec!["code-1".to_string(), "code-2".to_string()]);

    // A kind nobody has written yet is a normal empty list, not an error.
    let meeting = store
        .list_coding_sessions(graph_id, None, Some("meeting"), None, None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    assert!(meeting.is_empty(), "unmatched kind must return empty: {meeting:?}");

    // `None` keeps the pre-existing every-kind behaviour byte-for-byte.
    let every = store
        .list_coding_sessions(graph_id, None, None, None, None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    assert_eq!(every.len(), 4, "an absent kind filter must still return every kind: {every:?}");

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn list_coding_sessions_zero_sessions_case_on_a_fresh_ocean() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let rows = store.list_coding_sessions(graph_id, None, None, None, None, 10, &SessionListFilter::default()).await.unwrap();
    assert!(rows.is_empty(), "a fresh ocean with zero sessions must return an empty list, not an error");

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn list_coding_sessions_since_filters_on_last_activity_at() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    store
        .upsert_coding_session(graph_id, "sess-old", "coding", None, None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .unwrap();
    capture_session(&pool, graph_id, "sess-old", json!({"session_kind": "coding"}), "permitted").await;
    let cutoff = Utc::now() + chrono::Duration::milliseconds(50);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    store
        .upsert_coding_session(graph_id, "sess-new", "coding", None, None, None, None, None, None, None, None, &SessionIdentity::default())
        .await
        .unwrap();
    capture_session(&pool, graph_id, "sess-new", json!({"session_kind": "coding"}), "permitted").await;

    let rows = store
        .list_coding_sessions(graph_id, None, None, Some(cutoff), None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].session_id, "sess-new");

    cleanup(&pool, account_id, user_id).await;
}

/// Keyset-cursor pagination: `limit=1` across 3 sessions must produce 3
/// distinct, non-overlapping, correctly-ordered pages — the exact scenario
/// offset-based `page`/`page_size` would get wrong if `last_activity_at` kept
/// moving mid-pagination (it can't in this test, but the CURSOR SHAPE itself
/// — `(last_activity_at, id) < (cursor_ts, cursor_id)` — is what's under test
/// here, independent of live churn).
#[tokio::test]
async fn list_coding_sessions_cursor_pagination_covers_every_row_exactly_once() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    for i in 0..3 {
        store
            .upsert_coding_session(
                graph_id,
                &format!("sess-page-{i}"),
                "coding",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                &SessionIdentity::default(),
            )
            .await
            .unwrap();
        capture_session(&pool, graph_id, &format!("sess-page-{i}"), json!({"session_kind": "coding"}), "permitted").await;
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<(chrono::DateTime<Utc>, Uuid)> = None;
    for _ in 0..5 {
        // bounded loop — guards against an infinite loop on a pagination bug
        let page = store.list_coding_sessions(graph_id, None, None, None, cursor, 1, &SessionListFilter::default()).await.unwrap();
        if page.is_empty() {
            break;
        }
        assert_eq!(page.len(), 1);
        seen.push(page[0].session_id.clone());
        cursor = Some((page[0].last_activity_at, page[0].id));
    }

    assert_eq!(
        seen,
        vec!["sess-page-2", "sess-page-1", "sess-page-0"],
        "cursor pagination must visit every row exactly once, most-recent-activity first"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// Three coding sessions that differ on project / harness / model / repo /
/// branch: each new filter (migration 0220) returns the matching one, `repo`
/// matches either `repo_path` or `remote_sanitized`, and combining
/// `session_kind=coding` with a project still works when platform is unset.
#[tokio::test]
async fn list_coding_sessions_filters_by_harness_model_repo_and_branch() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    store
        .upsert_coding_session(
            graph_id,
            "sess-alpha",
            "coding",
            Some("alpha"),
            None,
            Some("/src/alpha"),
            Some("github.com/org/alpha"),
            None,
            Some("claude-code/1"),
            Some("opus"),
            Some("main"),
            &SessionIdentity::default(),
        )
        .await
        .unwrap();
    store
        .upsert_coding_session(
            graph_id,
            "sess-beta",
            "coding",
            Some("beta"),
            None,
            Some("/src/beta"),
            Some("github.com/org/beta"),
            None,
            Some("cursor/2"),
            Some("sonnet"),
            Some("feat/b"),
            &SessionIdentity::default(),
        )
        .await
        .unwrap();
    store
        .upsert_coding_session(
            graph_id,
            "sess-gamma",
            "coding",
            Some("gamma"),
            None,
            None,
            Some("github.com/org/gamma"),
            None,
            Some("codex/3"),
            Some("gpt"),
            Some("feat/c"),
            &SessionIdentity::default(),
        )
        .await
        .unwrap();

    for (sid, context) in [
        ("sess-alpha", json!({"session_kind": "coding", "project_key": "alpha", "repo_path": "/src/alpha", "remote_sanitized": "github.com/org/alpha", "harness_version": "claude-code/1", "model": "opus", "branch": "main"})),
        ("sess-beta", json!({"session_kind": "coding", "project_key": "beta", "repo_path": "/src/beta", "remote_sanitized": "github.com/org/beta", "harness_version": "cursor/2", "model": "sonnet", "branch": "feat/b"})),
        ("sess-gamma", json!({"session_kind": "coding", "project_key": "gamma", "remote_sanitized": "github.com/org/gamma", "harness_version": "codex/3", "model": "gpt", "branch": "feat/c"})),
    ] {
        capture_session(&pool, graph_id, sid, context, "permitted").await;
    }

    let ids = |filter: SessionListFilter| {
        let store = &store;
        async move {
            let rows = store
                .list_coding_sessions(graph_id, None, Some("coding"), None, None, 10, &filter)
                .await
                .unwrap();
            rows.into_iter().map(|r| r.session_id).collect::<Vec<_>>()
        }
    };

    assert_eq!(
        ids(SessionListFilter {
            harness: Some("claude-code/1".into()),
            ..Default::default()
        })
        .await,
        vec!["sess-alpha"]
    );
    assert_eq!(
        ids(SessionListFilter {
            model: Some("sonnet".into()),
            ..Default::default()
        })
        .await,
        vec!["sess-beta"]
    );
    assert_eq!(
        ids(SessionListFilter {
            branch: Some("feat/c".into()),
            ..Default::default()
        })
        .await,
        vec!["sess-gamma"]
    );

    // `repo` matches a local path OR the sanitized remote.
    assert_eq!(
        ids(SessionListFilter {
            repo: Some("/src/alpha".into()),
            ..Default::default()
        })
        .await,
        vec!["sess-alpha"],
        "repo filter must match repo_path"
    );
    assert_eq!(
        ids(SessionListFilter {
            repo: Some("github.com/org/alpha".into()),
            ..Default::default()
        })
        .await,
        vec!["sess-alpha"],
        "repo filter must match remote_sanitized"
    );
    assert_eq!(
        ids(SessionListFilter {
            repo: Some("github.com/org/gamma".into()),
            ..Default::default()
        })
        .await,
        vec!["sess-gamma"],
        "a session with only remote_sanitized still matches repo="
    );

    let by_project = store
        .list_coding_sessions(
            graph_id,
            Some("alpha"),
            Some("coding"),
            None,
            None,
            10,
            &SessionListFilter {
                platform: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        by_project.iter().map(|r| r.session_id.as_str()).collect::<Vec<_>>(),
        vec!["sess-alpha"],
        "kind=coding + empty platform + project still narrows"
    );
    assert_eq!(by_project[0].branch.as_deref(), Some("main"));
    assert_eq!(by_project[0].remote_sanitized.as_deref(), Some("github.com/org/alpha"));

    cleanup(&pool, account_id, user_id).await;
}

/// `branch` is last-reported: a later checkpoint that sends a branch
/// overwrites; one that omits it keeps the stored value.
#[tokio::test]
async fn upsert_coding_session_branch_is_last_reported() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    store
        .upsert_coding_session(
            graph_id,
            "sess-branch",
            "coding",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("main"),
            &SessionIdentity::default(),
        )
        .await
        .unwrap();
    let first = store.get_coding_session(graph_id, "sess-branch").await.unwrap().unwrap();
    assert_eq!(first.branch.as_deref(), Some("main"));

    store
        .upsert_coding_session(
            graph_id,
            "sess-branch",
            "coding",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("feat/filters"),
            &SessionIdentity::default(),
        )
        .await
        .unwrap();
    let second = store.get_coding_session(graph_id, "sess-branch").await.unwrap().unwrap();
    assert_eq!(second.branch.as_deref(), Some("feat/filters"), "a later reported branch wins");

    store
        .upsert_coding_session(
            graph_id,
            "sess-branch",
            "coding",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &SessionIdentity::default(),
        )
        .await
        .unwrap();
    let third = store.get_coding_session(graph_id, "sess-branch").await.unwrap().unwrap();
    assert_eq!(
        third.branch.as_deref(),
        Some("feat/filters"),
        "omitting branch must not blank the last reported value"
    );

    cleanup(&pool, account_id, user_id).await;
}

// ── `session_facets` (`GET /v1/sessions/facets`'s backing query) ───────────

/// Same cap `routes::sessions::SESSIONS_FACETS_CAP` uses — duplicated here
/// (the route constant is private to that module) rather than imported, same
/// convention as this file's cursor/limit constants elsewhere in the suite.
const FACETS_TEST_CAP: i64 = 200;

async fn capture_session(pool: &sqlx::PgPool, graph_id: Uuid, session_id: &str, ctx: Value, access_state: &str) -> Uuid {
    let mut context = ctx;
    context["revision"] = json!("1");
    context["session_id"] = json!(session_id);
    let namespace = format!("facets:{session_id}");
    let origin = json!({"service_id":"capture","transport":"native","content_kind":"message","connection":{"kind":"oauth_grant","id":"facets-grant"},"container":{"kind":"channel","id":"C1","provider_namespace":namespace,"label":"Facets"}});
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'agent_session',$2,$3,$4,$5) RETURNING source_item_id")
        .bind(graph_id).bind(&namespace).bind(session_id).bind(&origin).bind(access_state)
        .fetch_one(pool).await.expect("insert source item");
    sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,capture_context,occurred_at,captured_at) VALUES($1,$2,$3,'agent_session',$4,$5,'fixture',$6,$7,now(),now()) RETURNING version_id")
        .bind(graph_id).bind(item).bind(session_id).bind(&namespace).bind(&origin)
        .bind(json!({"session_id": session_id, "capture_context": context}))
        .bind(&context)
        .fetch_one(pool).await.expect("insert source version")
}

/// Flatten a `Vec<FacetValue>` to `(value, count)` pairs for terse
/// `assert_eq!`s against an expected order/count list.
fn facet_pairs(values: &[FacetValue]) -> Vec<(String, i64)> {
    values.iter().map(|v| (v.value.clone(), v.count)).collect()
}

/// Seven sessions exercising every facet-endpoint contract in one seed:
///
///   sess-1  coding  proj-a  grokbot   claude-code  harness-x  opus    main     repo_path=repoA remote_sanitized=repoA
///   sess-2  coding  proj-a  grokbot   claude-code  harness-x  sonnet  feat/b   repo_path=NULL   remote_sanitized=repoA
///   sess-3  coding  proj-b  otherbot  cursor       harness-y  opus    main     repo_path=repoC  remote_sanitized=NULL
///   sess-4  coding  proj-b  otherbot  cursor       harness-y  sonnet  feat/b   repo_path=NULL   remote_sanitized=repoB
///   sess-5  agent   proj-a  thirdbot  claude-code  harness-z  opus    main     repo_path=NULL   remote_sanitized=repoD
///   sess-6  coding  NULL    NULL      NULL         NULL       NULL    NULL     repo_path=NULL   remote_sanitized=NULL
///   sess-7  coding  ""      ""        ""           ""         ""      ""       repo_path=""     remote_sanitized=""
///
/// sess-1's repo_path and remote_sanitized are the SAME value ("repoA") —
/// proves a session with both columns set to one value is counted ONCE for
/// that value, not twice. sess-2 has ONLY remote_sanitized="repoA" — proves
/// the union/merge: "repoA" ends up with count 2 (sess-1 + sess-2), a
/// DISTINCT-session count, not a sum of column matches. sess-6/sess-7 exist
/// purely to prove NULL and empty-string are excluded from every dimension —
/// every assertion below is unaffected by their presence, because neither
/// contributes a row to any facet.
async fn seed_facets_fixture(store: &PostgresStore, pool: &sqlx::PgPool, graph_id: Uuid) {
    let session = |id: &str, kind: &str, project: Option<&str>, agent_name: Option<&str>,
                   platform: Option<&str>, harness: Option<&str>, model: Option<&str>,
                   branch: Option<&str>, repo_path: Option<&str>, remote_sanitized: Option<&str>| {
        let identity = SessionIdentity {
            agent_name: agent_name.map(String::from),
            platform: platform.map(String::from),
            ..Default::default()
        };
        (
            id.to_string(),
            kind.to_string(),
            project.map(String::from),
            repo_path.map(String::from),
            remote_sanitized.map(String::from),
            harness.map(String::from),
            model.map(String::from),
            branch.map(String::from),
            identity,
        )
    };
    let rows = vec![
        session("sess-1", "coding", Some("proj-a"), Some("grokbot"), Some("claude-code"), Some("harness-x"), Some("opus"), Some("main"), Some("repoA"), Some("repoA")),
        session("sess-2", "coding", Some("proj-a"), Some("grokbot"), Some("claude-code"), Some("harness-x"), Some("sonnet"), Some("feat/b"), None, Some("repoA")),
        session("sess-3", "coding", Some("proj-b"), Some("otherbot"), Some("cursor"), Some("harness-y"), Some("opus"), Some("main"), Some("repoC"), None),
        session("sess-4", "coding", Some("proj-b"), Some("otherbot"), Some("cursor"), Some("harness-y"), Some("sonnet"), Some("feat/b"), None, Some("repoB")),
        session("sess-5", "agent", Some("proj-a"), Some("thirdbot"), Some("claude-code"), Some("harness-z"), Some("opus"), Some("main"), None, Some("repoD")),
        session("sess-6", "coding", None, None, None, None, None, None, None, None),
        session("sess-7", "coding", Some(""), Some(""), Some(""), Some(""), Some(""), Some(""), Some(""), Some("")),
    ];
    for (id, kind, project, repo_path, remote_sanitized, harness, model, branch, identity) in rows {
        store
            .upsert_coding_session(
                graph_id,
                &id,
                &kind,
                project.as_deref(),
                None,
                repo_path.as_deref(),
                remote_sanitized.as_deref(),
                None,
                harness.as_deref(),
                model.as_deref(),
                branch.as_deref(),
                &identity,
            )
            .await
            .unwrap_or_else(|e| panic!("upsert {id}: {e}"));
        let ctx = json!({
            "session_kind": kind, "project_key": project, "repo_path": repo_path,
            "remote_sanitized": remote_sanitized, "harness_version": harness, "model": model,
            "branch": branch, "agent_name": identity.agent_name, "platform": identity.platform,
        });
        capture_session(pool, graph_id, &id, ctx, "permitted").await;
    }
}

#[tokio::test]
async fn session_facets_reports_every_dimension_with_count_desc_value_asc_order() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    seed_facets_fixture(&store, &pool, graph_id).await;

    // kind=coding excludes sess-5 (the only "agent" session) — proves (vi)
    // "kind narrows" by comparison against the no-kind-filter call below,
    // and gives every OTHER dimension a clean, symmetric 2-vs-2 tie except
    // `repo`, which is asserted below to prove real count-DESC ordering
    // (not just alphabetical, since not every dimension here has a tie).
    let facets = store
        .session_facets(graph_id, None, Some("coding"), &SessionListFilter::default(), None, FACETS_TEST_CAP)
        .await
        .expect("session_facets");

    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::AgentName)),
        vec![("grokbot".to_string(), 2), ("otherbot".to_string(), 2)],
        "agent_name: sess-6/sess-7's NULL/empty agent_name must not appear as a value: {:?}",
        facets.values(FacetDimension::AgentName)
    );
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Platform)),
        vec![("claude-code".to_string(), 2), ("cursor".to_string(), 2)]
    );
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Project)),
        vec![("proj-a".to_string(), 2), ("proj-b".to_string(), 2)]
    );
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Harness)),
        vec![("harness-x".to_string(), 2), ("harness-y".to_string(), 2)]
    );
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Model)),
        vec![("opus".to_string(), 2), ("sonnet".to_string(), 2)]
    );
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Branch)),
        vec![("feat/b".to_string(), 2), ("main".to_string(), 2)]
    );
    // (ii) repo union+merge: sess-1 (repo_path=remote_sanitized="repoA") is
    // ONE distinct session, sess-2 (remote_sanitized="repoA" only) is
    // another — "repoA" is the OR-merged count of BOTH, i.e. 2, not 3 (which
    // would mean sess-1 got double-counted for matching both its own
    // columns) and not 1 (which would mean the merge missed sess-2's
    // column-only match). "repoB"/"repoC" are single-session values.
    // (iv) count DESC then value ASC: repoA(2) sorts first even though
    // "repoB"/"repoC" would come first alphabetically — proves the ORDER is
    // driven by count, not just the value tiebreak the ties above exercise.
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Repo)),
        vec![("repoA".to_string(), 2), ("repoB".to_string(), 1), ("repoC".to_string(), 1)],
        "repo union/merge + count-desc-then-value-asc order: {:?}",
        facets.values(FacetDimension::Repo)
    );

    // (vi) kind narrows: with NO kind filter, sess-5 ("agent") is back in —
    // proj-a gains a third session and a fourth agent_name/platform/repo
    // value appear that were entirely absent from the kind=coding facets
    // above.
    let unfiltered = store
        .session_facets(graph_id, None, None, &SessionListFilter::default(), None, FACETS_TEST_CAP)
        .await
        .expect("session_facets unfiltered");
    assert_eq!(
        facet_pairs(&unfiltered.values(FacetDimension::Project)),
        vec![("proj-a".to_string(), 3), ("proj-b".to_string(), 2)],
        "omitting kind must include sess-5, raising proj-a's count 2 -> 3: {:?}",
        unfiltered.values(FacetDimension::Project)
    );
    assert!(
        facet_pairs(&unfiltered.values(FacetDimension::AgentName)).contains(&("thirdbot".to_string(), 1)),
        "sess-5's agent_name must appear once kind is not filtered: {:?}",
        unfiltered.values(FacetDimension::AgentName)
    );
    assert!(
        !facet_pairs(&facets.values(FacetDimension::AgentName)).contains(&("thirdbot".to_string(), 1)),
        "kind=coding must exclude sess-5's agent_name: {:?}",
        facets.values(FacetDimension::AgentName)
    );

    cleanup(&pool, account_id, user_id).await;
}

/// (iii) faceted-search semantics: `project_key=proj-a` narrows every OTHER
/// dimension to sessions inside project A, but the `project` facet ITSELF
/// stays unnarrowed (still lists every project) — that is the entire point
/// of excluding a dimension's own predicate from its own count. Also proves
/// (v) NULL/empty exclusion again under a narrower filter (sess-6/sess-7
/// still contribute nothing) and re-confirms (ii)'s repo merge under a
/// narrower filter (only sess-1/sess-2 remain, both mapping to "repoA").
#[tokio::test]
async fn session_facets_excludes_its_own_dimensions_filter_but_narrows_every_other_dimension() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    seed_facets_fixture(&store, &pool, graph_id).await;

    let facets = store
        .session_facets(graph_id, Some("proj-a"), Some("coding"), &SessionListFilter::default(), None, FACETS_TEST_CAP)
        .await
        .expect("session_facets project-filtered");

    // NOT narrowed: `project` shows proj-a AND proj-b even though the
    // request itself is scoped to proj-a.
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Project)),
        vec![("proj-a".to_string(), 2), ("proj-b".to_string(), 2)],
        "the project dimension must not be narrowed by its own project filter: {:?}",
        facets.values(FacetDimension::Project)
    );

    // Narrowed: every other dimension drops to only proj-a's coding sessions
    // (sess-1, sess-2) — branch, agent_name, platform, harness, model and
    // repo all shrink from their unfiltered-by-project values above.
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Branch)),
        vec![("feat/b".to_string(), 1), ("main".to_string(), 1)],
        "branch must narrow to proj-a's sessions only: {:?}",
        facets.values(FacetDimension::Branch)
    );
    assert_eq!(facet_pairs(&facets.values(FacetDimension::AgentName)), vec![("grokbot".to_string(), 2)]);
    assert_eq!(facet_pairs(&facets.values(FacetDimension::Platform)), vec![("claude-code".to_string(), 2)]);
    assert_eq!(facet_pairs(&facets.values(FacetDimension::Harness)), vec![("harness-x".to_string(), 2)]);
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Model)),
        vec![("opus".to_string(), 1), ("sonnet".to_string(), 1)]
    );
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Repo)),
        vec![("repoA".to_string(), 2)],
        "repoB/repoC belong to proj-b's sessions and must be narrowed away: {:?}",
        facets.values(FacetDimension::Repo)
    );

    cleanup(&pool, account_id, user_id).await;
}

/// `cap` truncates each dimension independently — a dimension with more
/// distinct values than the cap is cut to the cap's highest `count DESC,
/// value ASC` entries, and a LOW-cardinality dimension is never shortened by
/// a high-cardinality neighbor eating a shared budget (there is no shared
/// budget: each dimension gets its own `LIMIT`).
#[tokio::test]
async fn session_facets_caps_each_dimension_independently() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    seed_facets_fixture(&store, &pool, graph_id).await;

    let facets = store
        .session_facets(graph_id, None, Some("coding"), &SessionListFilter::default(), None, 1)
        .await
        .expect("session_facets cap=1");

    assert_eq!(facets.values(FacetDimension::AgentName).len(), 1, "cap=1 must truncate a 2-value dimension to 1: {:?}", facets.values(FacetDimension::AgentName));
    assert_eq!(
        facets.values(FacetDimension::AgentName)[0],
        FacetValue { value: "grokbot".to_string(), count: 2 },
        "the SURVIVING entry must still be the highest count/lowest value one: {:?}",
        facets.values(FacetDimension::AgentName)
    );
    // `repo`'s 3 distinct values also truncate to 1 under the same cap, and
    // the survivor is "repoA" (count 2) even though "repoB"/"repoC" would
    // sort first if this were truncating by value alone.
    assert_eq!(facets.values(FacetDimension::Repo).len(), 1);
    assert_eq!(facets.values(FacetDimension::Repo)[0].value, "repoA");
    assert_eq!(
        facets.pages[&FacetDimension::Repo].next,
        Some(FacetCursor { count: 2, value: "repoA".to_string() }),
        "a truncated dimension carries a cursor at its last value"
    );

    cleanup(&pool, account_id, user_id).await;
}

/// Walk one dimension page by page from its first page, asserting each page
/// holds at most `cap` values and ends with a cursor exactly when more follow.
async fn walk_facet(
    store: &PostgresStore,
    graph_id: Uuid,
    filter: &SessionListFilter,
    dimension: FacetDimension,
    cap: i64,
) -> Vec<(String, i64)> {
    let mut seen = Vec::new();
    let mut after: Option<FacetCursor> = None;
    for _ in 0..1000 {
        let request = FacetPageRequest { dimension, after: after.clone() };
        let facets = store
            .session_facets(graph_id, None, None, filter, Some(&request), cap)
            .await
            .expect("session_facets page");
        assert_eq!(facets.pages.keys().copied().collect::<Vec<_>>(), vec![dimension], "a paged request returns only its dimension");
        let page = &facets.pages[&dimension];
        assert!(page.values.len() as i64 <= cap, "page larger than cap: {}", page.values.len());
        seen.extend(facet_pairs(&page.values));
        match &page.next {
            Some(next) => {
                assert_eq!(page.values.len() as i64, cap, "a short page must be the last one");
                let last = page.values.last().expect("a page with a cursor has values");
                assert_eq!((next.count, next.value.as_str()), (last.count, last.value.as_str()));
                after = Some(next.clone());
            }
            None => return seen,
        }
    }
    panic!("facet paging never terminated");
}

/// `count DESC, value ASC` with `value` compared bytewise, the order paging
/// promises.
fn expected_order(mut pairs: Vec<(String, i64)>) -> Vec<(String, i64)> {
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.as_bytes().cmp(b.0.as_bytes())));
    pairs
}

/// More distinct branches and tasks than the route's 200-value cap. Paging
/// must reach every value exactly once in order and then stop. Mixed-case
/// and punctuation branch values put bytewise order and a locale collation
/// at odds, and ties at count 1 and 2 put page boundaries inside a tie.
#[tokio::test]
async fn session_facets_paging_walks_every_value_past_the_cap_exactly_once() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let mut branches: Vec<String> = (0..202).map(|i| format!("b-{i:03}")).collect();
    branches.extend(["Zeta", "alpha", "_under"].map(String::from));
    let mut sessions: Vec<(String, String)> = branches.iter().map(|b| (b.clone(), b.clone())).collect();
    // A second session on every twentieth branch gives it count 2.
    sessions.extend(branches.iter().step_by(20).map(|b| (format!("{b}-again"), b.clone())));
    for (i, (id, branch)) in sessions.iter().enumerate() {
        store
            .upsert_coding_session(graph_id, id, "coding", None, None, None, None, None, None, None, Some(branch.as_ref()), &SessionIdentity::default())
            .await
            .unwrap_or_else(|e| panic!("upsert {id}: {e}"));
        let ctx = json!({"session_kind": "coding", "branch": branch, "task": format!("Task {i:03}")});
        capture_session(&pool, graph_id, id, ctx, "permitted").await;
    }
    let mut branch_counts = std::collections::BTreeMap::<String, i64>::new();
    for (_, b) in &sessions {
        *branch_counts.entry(b.clone()).or_default() += 1;
    }
    let expected_branches = expected_order(branch_counts.into_iter().collect());
    assert_eq!(expected_branches.len(), 205);
    let expected_tasks = expected_order((0..sessions.len()).map(|i| (format!("Task {i:03}"), 1)).collect());

    let first = store
        .session_facets(graph_id, None, None, &SessionListFilter::default(), None, FACETS_TEST_CAP)
        .await
        .expect("first pages");
    assert_eq!(first.pages.keys().copied().collect::<Vec<_>>(), FacetDimension::ALL.to_vec(), "a first-page request returns every dimension");
    let branch_page = &first.pages[&FacetDimension::Branch];
    assert_eq!(facet_pairs(&branch_page.values), expected_branches[..200].to_vec());
    assert!(branch_page.next.is_some(), "205 branches must not fit one 200-value page");
    assert!(first.pages[&FacetDimension::Model].values.is_empty());
    assert_eq!(first.pages[&FacetDimension::Model].next, None);

    for cap in [FACETS_TEST_CAP, 7] {
        let branches = walk_facet(&store, graph_id, &SessionListFilter::default(), FacetDimension::Branch, cap).await;
        assert_eq!(branches, expected_branches, "branch walk at cap {cap}");
        let tasks = walk_facet(&store, graph_id, &SessionListFilter::default(), FacetDimension::Task, cap).await;
        assert_eq!(tasks, expected_tasks, "task walk at cap {cap}");
    }

    cleanup(&pool, account_id, user_id).await;
}

/// `task` is its own dimension: a task filter narrows every other facet but
/// not the task facet, and every other filter narrows the task facet.
#[tokio::test]
async fn session_facets_task_filter_narrows_other_dimensions_but_not_its_own() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    for (id, task, branch) in [("t-1", "Alpha one", "main"), ("t-2", "Beta", "dev"), ("t-3", "alpha two", "dev")] {
        store
            .upsert_coding_session(graph_id, id, "coding", None, None, None, None, None, None, None, Some(branch.as_ref()), &SessionIdentity::default())
            .await
            .unwrap();
        capture_session(&pool, graph_id, id, json!({"session_kind": "coding", "task": task, "branch": branch}), "permitted").await;
    }

    let by_task = SessionListFilter { task: Some("alpha".into()), ..Default::default() };
    let facets = store.session_facets(graph_id, None, None, &by_task, None, FACETS_TEST_CAP).await.unwrap();
    assert_eq!(
        facet_pairs(facets.values(FacetDimension::Branch)),
        vec![("dev".to_string(), 1), ("main".to_string(), 1)],
        "the task filter (case-insensitive substring) narrows branch to t-1 and t-3"
    );
    assert_eq!(
        facet_pairs(facets.values(FacetDimension::Task)),
        vec![("Alpha one".to_string(), 1), ("Beta".to_string(), 1), ("alpha two".to_string(), 1)],
        "the task facet is not narrowed by its own filter"
    );

    let by_branch = SessionListFilter { branch: Some("dev".into()), ..by_task };
    let facets = store.session_facets(graph_id, None, None, &by_branch, None, FACETS_TEST_CAP).await.unwrap();
    assert_eq!(
        facet_pairs(facets.values(FacetDimension::Task)),
        vec![("Beta".to_string(), 1), ("alpha two".to_string(), 1)],
        "the branch filter narrows the task facet"
    );
    assert_eq!(facet_pairs(facets.values(FacetDimension::Branch)), vec![("dev".to_string(), 1), ("main".to_string(), 1)]);

    cleanup(&pool, account_id, user_id).await;
}

/// A session whose only capture is not permitted contributes no value and no
/// count to any dimension, on the first page or while paging.
#[tokio::test]
async fn session_facets_never_count_a_session_the_caller_cannot_read() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    let fields = |branch: &str, task: &str, model: &str| {
        json!({"session_kind": "coding", "branch": branch, "task": task, "model": model, "agent_name": model,
               "platform": model, "project_key": model, "harness_version": model, "repo_path": model})
    };
    for (id, ctx, access) in [
        ("open-1", fields("main", "Open task", "shared"), "permitted"),
        ("open-2", fields("dev", "Other task", "shared"), "permitted"),
        ("hidden-1", fields("main", "Hidden task", "shared"), "restricted"),
        ("hidden-2", fields("secret-branch", "Secret task", "secret"), "restricted"),
    ] {
        store
            .upsert_coding_session(graph_id, id, "coding", None, None, None, None, None, None, None, None, &SessionIdentity::default())
            .await
            .unwrap();
        capture_session(&pool, graph_id, id, ctx, access).await;
    }

    let facets = store
        .session_facets(graph_id, None, None, &SessionListFilter::default(), None, FACETS_TEST_CAP)
        .await
        .unwrap();
    for dim in FacetDimension::ALL {
        let page = &facets.pages[&dim];
        assert_eq!(page.next, None);
        assert!(
            page.values.iter().all(|v| !v.value.contains("secret") && !v.value.contains("Hidden") && !v.value.contains("Secret")),
            "{dim:?} leaks a restricted session's value: {:?}",
            page.values
        );
        let total: i64 = page.values.iter().map(|v| v.count).sum();
        assert_eq!(total, 2, "{dim:?} counts must cover only the two readable sessions: {:?}", page.values);
    }
    assert_eq!(
        facet_pairs(facets.values(FacetDimension::Branch)),
        vec![("dev".to_string(), 1), ("main".to_string(), 1)],
        "main stays at 1: hidden-1 shares it but cannot be read"
    );
    assert_eq!(
        walk_facet(&store, graph_id, &SessionListFilter::default(), FacetDimension::Branch, 1).await,
        vec![("dev".to_string(), 1), ("main".to_string(), 1)]
    );
    assert_eq!(
        walk_facet(&store, graph_id, &SessionListFilter::default(), FacetDimension::Task, 1).await,
        vec![("Open task".to_string(), 1), ("Other task".to_string(), 1)]
    );

    cleanup(&pool, account_id, user_id).await;
}

// ── Effective-platform fallback (declared ?? detected) ──────────────────────
//
// Founder bug report: a Cursor coding session lands with `platform` NULL and
// `platform_detected = "cursor"` (`routes::ingest`'s write policy never
// back-fills a coding session's `platform` from detection — only `agent`
// sessions get that fallback at WRITE time). The READ side must still treat
// it as a `cursor` session: `coding_sessions_list_pg`'s filter and
// `coding_sessions_facets_pg`'s `platform` dimension both match against
// `COALESCE(cs.platform, cs.platform_detected)`, mirroring
// `SessionIdentity::effective_platform`.
async fn seed_effective_platform_fixture(store: &PostgresStore, pool: &sqlx::PgPool, graph_id: Uuid) {
    // sess-cursor: declared no platform, detected as cursor — the exact
    // shape of a real Cursor coding session today.
    store
        .upsert_coding_session(
            graph_id,
            "sess-cursor",
            "coding",
            None,
            None,
            None,
            None,
            None,
            Some("cursor-harness/1"),
            Some("grok-4.6"),
            None,
            &SessionIdentity {
                platform: None,
                platform_detected: Some("cursor".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // sess-claude: declared AND detected agree — the Claude Code case, which
    // must be entirely unaffected by the fallback.
    store
        .upsert_coding_session(
            graph_id,
            "sess-claude",
            "coding",
            None,
            None,
            None,
            None,
            None,
            Some("claude-harness/1"),
            Some("sonnet-5"),
            None,
            &SessionIdentity {
                platform: Some("claude-code".into()),
                platform_detected: Some("claude-code".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // sess-neither: BOTH null — must never appear as a platform facet value.
    store
        .upsert_coding_session(
            graph_id,
            "sess-neither",
            "coding",
            None,
            None,
            None,
            None,
            None,
            Some("mystery-harness/1"),
            Some("mystery-model"),
            None,
            &SessionIdentity::default(),
        )
        .await
        .unwrap();
    for (id, harness, model, platform, detected) in [
        ("sess-cursor", "cursor-harness/1", "grok-4.6", None, Some("cursor")),
        ("sess-claude", "claude-harness/1", "sonnet-5", Some("claude-code"), Some("claude-code")),
        ("sess-neither", "mystery-harness/1", "mystery-model", None, None),
    ] {
        let ctx = json!({"session_kind": "coding", "harness_version": harness, "model": model,
                         "platform": platform, "platform_detected": detected});
        capture_session(pool, graph_id, id, ctx, "permitted").await;
    }
}

/// (a) `SessionListFilter { platform: Some("cursor") }` matches the session
/// that declared no platform but was detected as cursor, and does NOT match
/// the session with both null.
#[tokio::test]
async fn list_coding_sessions_platform_filter_falls_back_to_detected_platform() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    seed_effective_platform_fixture(&store, &pool, graph_id).await;

    let rows = store
        .list_coding_sessions(
            graph_id,
            None,
            Some("coding"),
            None,
            None,
            10,
            &SessionListFilter {
                platform: Some("cursor".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|r| r.session_id.as_str()).collect::<Vec<_>>(),
        vec!["sess-cursor"],
        "platform=cursor must match the undeclared-but-detected session and nothing else: {:?}",
        rows.iter().map(|r| &r.session_id).collect::<Vec<_>>()
    );
    // Declared-and-agreeing sessions are unaffected by the fallback.
    let claude_rows = store
        .list_coding_sessions(
            graph_id,
            None,
            Some("coding"),
            None,
            None,
            10,
            &SessionListFilter {
                platform: Some("claude-code".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        claude_rows.iter().map(|r| r.session_id.as_str()).collect::<Vec<_>>(),
        vec!["sess-claude"]
    );

    cleanup(&pool, account_id, user_id).await;
}

/// (b) the `platform` facet lists `cursor` (declared-null, detected) and
/// `claude-code` (declared, agreeing) each with count 1; (c) applying
/// `platform=cursor` cross-narrows the OTHER dimensions (harness/model) to
/// just that one session, proving the cross-narrowing predicate uses the
/// same COALESCE as the list filter; (d) `sess-neither` (both null) never
/// appears in the platform facet.
#[tokio::test]
async fn session_facets_platform_dimension_falls_back_to_detected_and_cross_narrows() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    seed_effective_platform_fixture(&store, &pool, graph_id).await;

    let facets = store
        .session_facets(graph_id, None, Some("coding"), &SessionListFilter::default(), None, FACETS_TEST_CAP)
        .await
        .expect("session_facets");

    // (b)
    assert_eq!(
        facet_pairs(&facets.values(FacetDimension::Platform)),
        vec![("claude-code".to_string(), 1), ("cursor".to_string(), 1)],
        "platform facet must list the detected-only value alongside the declared one: {:?}",
        facets.values(FacetDimension::Platform)
    );
    // (d) sess-neither (both platform and platform_detected null) never
    // contributes a facet value — the count above is exactly 1 + 1, not 1 + 2.
    let total: i64 = facets.values(FacetDimension::Platform).iter().map(|v| v.count).sum();
    assert_eq!(total, 2, "sess-neither must not appear in the platform facet: {:?}", facets.values(FacetDimension::Platform));

    // (c) narrow by the detected-only platform and confirm harness/model
    // shrink to sess-cursor's values alone.
    let narrowed = store
        .session_facets(
            graph_id,
            None,
            Some("coding"),
            &SessionListFilter { platform: Some("cursor".into()), ..Default::default() },
            None,
            FACETS_TEST_CAP,
        )
        .await
        .expect("session_facets platform=cursor");
    assert_eq!(
        facet_pairs(&narrowed.values(FacetDimension::Harness)),
        vec![("cursor-harness/1".to_string(), 1)],
        "harness facet must narrow to the cursor-detected session only: {:?}",
        narrowed.values(FacetDimension::Harness)
    );
    assert_eq!(
        facet_pairs(&narrowed.values(FacetDimension::Model)),
        vec![("grok-4.6".to_string(), 1)],
        "model facet must narrow to the cursor-detected session only: {:?}",
        narrowed.values(FacetDimension::Model)
    );
    // The platform dimension itself stays unnarrowed by its own filter (same
    // faceted-search contract as every other dimension) — both values still
    // show up even though the request itself filtered to "cursor".
    assert_eq!(
        facet_pairs(&narrowed.values(FacetDimension::Platform)),
        vec![("claude-code".to_string(), 1), ("cursor".to_string(), 1)],
        "platform dimension must not be narrowed by its own filter: {:?}",
        narrowed.values(FacetDimension::Platform)
    );

    cleanup(&pool, account_id, user_id).await;
}

// ── GET /v1/sessions timeout regression (2026-10-27) ────────────────────────

/// Every session and every plain version of the fixture below shares ONE
/// `provider_namespace`, as on prod (the org ocean's 4,883 versions all sit
/// in one history namespace). The legacy statement's per-row version scan
/// was keyed on `provider_namespace = cs.provider_namespace`, so a fixture
/// with one namespace per session (`capture_session`'s `facets:{id}`) would
/// let it emit one version per page row and hide the re-resolution.
const SHARED_NAMESPACE: &str = "history:A";

/// `capture_session` under [`SHARED_NAMESPACE`].
async fn capture_shared_session(pool: &sqlx::PgPool, graph_id: Uuid, session_id: &str, ctx: Value) -> Uuid {
    let mut context = ctx;
    context["revision"] = json!("1");
    context["session_id"] = json!(session_id);
    let origin = json!({"service_id":"capture","transport":"native","content_kind":"message","connection":{"kind":"oauth_grant","id":"shared-grant"},"container":{"kind":"channel","id":"C1","provider_namespace":SHARED_NAMESPACE,"label":"Shared"}});
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'agent_session',$2,$3,$4,'permitted') RETURNING source_item_id")
        .bind(graph_id).bind(SHARED_NAMESPACE).bind(session_id).bind(&origin)
        .fetch_one(pool).await.expect("insert shared source item");
    sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,capture_context,occurred_at,captured_at) VALUES($1,$2,$3,'agent_session',$4,$5,'fixture',$6,$7,now(),now()) RETURNING version_id")
        .bind(graph_id).bind(item).bind(session_id).bind(SHARED_NAMESPACE).bind(&origin)
        .bind(json!({"session_id": session_id, "capture_context": context}))
        .bind(&context)
        .fetch_one(pool).await.expect("insert shared source version")
}

/// One permitted source version that reports NO session id (neither
/// `capture_context` nor `source_metadata`) and whose single chunk atom is not
/// a `cc:` capture. `library_session_versions`' chunk-derived fallback runs for
/// it and resolves nothing. Prod oceans are mostly this shape (5,231 of 5,709
/// versions on the org tenant), which is why the fallback's cost, and how many
/// times the list statement pays it, decided whether `GET /v1/sessions`
/// finished inside the 10 s statement timeout.
async fn seed_unattributed_version(store: &PostgresStore, pool: &sqlx::PgPool, graph_id: Uuid, user_id: Uuid, i: usize) {
    let namespace = SHARED_NAMESPACE;
    let source_id = format!("plain-{i}");
    let origin = json!({"service_id":"capture","transport":"native","content_kind":"message","connection":{"kind":"oauth_grant","id":"plain-grant"},"container":{"kind":"channel","id":"C1","provider_namespace":namespace,"label":"Plain"}});
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'agent_session',$2,$3,$4,'permitted') RETURNING source_item_id")
        .bind(graph_id).bind(namespace).bind(&source_id).bind(&origin)
        .fetch_one(pool).await.expect("insert unattributed source item");
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) VALUES($1,$2,$3,'agent_session',$4,$5,'fixture','{}'::jsonb,now(),now()) RETURNING version_id")
        .bind(graph_id).bind(item).bind(&source_id).bind(namespace).bind(&origin)
        .fetch_one(pool).await.expect("insert unattributed source version");
    let atom = seed_atom(store, pool, graph_id, user_id, &format!("slack:W1:C1:{i}"), "a plain message", None).await;
    let mut tx = pool.begin().await.unwrap();
    coralswarm_backend::source_evidence::link_verified_chunks_tx(&mut tx, graph_id, version, &[atom]).await.unwrap();
    tx.commit().await.unwrap();
}

/// How many times the plan probed `source_versions`: for every node that
/// scans it, `Actual Loops x max(Actual Rows, 1)`. Each loop of a scan is at
/// least one probe even when it emits nothing, and `Actual Rows` is a
/// per-loop average that PG16/17 round to an integer (40 rows over 200 loops
/// prints as 0), so plain `rows x loops` would read 0 there. CTE, InitPlan
/// and SubPlan nodes all hang off `Plans`, so one walk sees them all.
fn source_versions_probes(node: &Value) -> f64 {
    let mut total = 0.0;
    if node.get("Relation Name").and_then(Value::as_str) == Some("source_versions") {
        let rows = node["Actual Rows"].as_f64().unwrap_or(0.0);
        let loops = node["Actual Loops"].as_f64().unwrap_or(0.0);
        eprintln!(
            "  source_versions node: {} via {} rows={rows} loops={loops}",
            node["Node Type"].as_str().unwrap_or("?"),
            node["Index Name"].as_str().unwrap_or("heap")
        );
        total += loops * rows.max(1.0);
    }
    for child in node.get("Plans").and_then(Value::as_array).into_iter().flatten() {
        total += source_versions_probes(child);
    }
    total
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` of a list statement with the handler's
/// binds for `GET /v1/sessions?kind=coding` (roots only, first page of 21).
async fn explain_list(pool: &sqlx::PgPool, graph_id: Uuid, sql: &str) -> Value {
    let mut tx = pool.begin().await.unwrap();
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}"))
        .bind(graph_id)
        .bind(None::<String>)
        .bind(Some("coding"))
        .bind(None::<chrono::DateTime<Utc>>)
        .bind(None::<chrono::DateTime<Utc>>)
        .bind(None::<Uuid>)
        .bind(21_i64)
        .bind(true)
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(None::<String>)
        .fetch_one(&mut *tx)
        .await
        .expect("explain list statement");
    tx.rollback().await.unwrap();
    plan[0]["Plan"].clone()
}

/// The statement `GET /v1/sessions` ran until 2026-10-27, kept here only as
/// the negative control for the plan assertion below: its per-row
/// `coral_count` subquery calls `library_sessions($1)` and
/// `library_session_versions($1)` inline, so Postgres re-resolves EVERY
/// version of the graph once per page row.
const LEGACY_PER_ROW_LIST_SQL: &str = r#"
        SELECT cs.id, cs.session_id, cs.session_kind, cs.project_key, cs.title, cs.repo_path,
               cs.remote_sanitized, cs.hostname, cs.harness_version, cs.model, cs.branch,
               cs.started_at, cs.last_activity_at,
               (SELECT COUNT(DISTINCT c.atom_id)::int8
                FROM library_sessions($1) t
                JOIN library_session_versions($1) sv ON sv.session_id=t.session_id AND sv.provider_namespace=t.provider_namespace
                JOIN source_version_chunks c ON c.graph_id=t.graph_id AND c.source_version_id=sv.version_id
                WHERE t.graph_id=cs.graph_id AND t.provider_namespace=cs.provider_namespace
                  AND (t.session_id=cs.session_id OR ($8::bool AND t.root_session_id=cs.session_id))
                  AND library_atom_permitted(t.graph_id,c.atom_id)
               ) AS coral_count,
               (CASE WHEN $8::bool THEN
                   (SELECT COUNT(*)::int8
                    FROM   library_sessions($1) d
                    WHERE  d.graph_id = cs.graph_id
                      AND  d.provider_namespace = cs.provider_namespace
                      AND  d.root_session_id = cs.session_id
                      AND  d.session_id <> cs.session_id)
                ELSE 0::int8 END) AS subagent_count,
               cs.agent_name, cs.platform, cs.platform_detected, cs.task,
               cs.parent_session_id, cs.root_session_id, cs.subagent_name
        FROM   library_sessions($1) cs
        WHERE  cs.graph_id = $1
          AND  ($2::text IS NULL OR cs.project_key = $2)
          AND  ($3::text IS NULL OR cs.session_kind = $3)
          AND  ($4::timestamptz IS NULL OR cs.last_activity_at >= $4)
          AND  ($5::timestamptz IS NULL OR (cs.last_activity_at, cs.id) < ($5::timestamptz, $6::uuid))
          AND  ($8::bool IS FALSE OR cs.parent_session_id IS NULL)
          AND  ($9::text IS NULL OR cs.parent_session_id = $9)
          AND  ($10::text IS NULL OR cs.agent_name = $10)
          AND  ($11::text IS NULL OR COALESCE(cs.platform, cs.platform_detected) = $11)
          AND  ($12::text IS NULL OR cs.task ILIKE $12)
          AND  ($13::text IS NULL OR cs.harness_version = $13)
          AND  ($14::text IS NULL OR cs.model = $14)
          AND  ($15::text IS NULL OR cs.repo_path = $15 OR cs.remote_sanitized = $15)
          AND  ($16::text IS NULL OR cs.branch = $16)
        ORDER  BY cs.last_activity_at DESC, cs.id DESC
        LIMIT  $7
        "#;

/// `GET /v1/sessions?kind=coding` 500'd on the org tenant (chain health paged
/// `http_5xx`, 2026-10-27): the list statement took 18.6 s against a 10 s
/// statement timeout. Two causes, one invariant.
///
/// The statement called `library_sessions($1)` / `library_session_versions($1)`
/// inside its per-row `coral_count` subquery, so Postgres re-resolved the
/// whole graph's versions once per page row (21 x 4,883 versions), and each
/// resolution ran the chunk-derived session fallback with `coding_sessions`
/// on the outer side of a LIKE join (765 sessions x every chunk atom, per
/// version). `CODING_SESSIONS_LIST_SQL` now resolves once in `MATERIALIZED`
/// CTEs and pages first; tenant 0200 / control 0379 drive the fallback from
/// the atom. Measured read-only on prod: 18,636 ms -> 512 ms.
///
/// The assertion is on the plan, not the clock: over a fixture with more
/// sessions than a page and far more unattributed versions than sessions
/// (the prod shape), the statement must read the graph's versions a
/// CONSTANT number of times. `LEGACY_PER_ROW_LIST_SQL` is the negative
/// control proving the metric sees the per-row re-resolution. The listed
/// rows themselves are checked too: every root on the page with its own
/// coral count, and the keyset walk covering every session exactly once.
#[tokio::test]
async fn list_coding_sessions_reads_each_source_version_a_bounded_number_of_times() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());
    const SESSIONS: usize = 40; // more than one page of 21
    const UNATTRIBUTED: usize = 160; // the fallback's input, 4x the sessions
    const PAGE: i64 = 21;

    for i in 0..SESSIONS {
        let sid = format!("sess-{i:02}");
        let title = format!("Session {i:02}");
        store
            .upsert_coding_session(graph_id, &sid, "coding", Some("proj"), Some(&title), None, None, None, None, None, None, &SessionIdentity::default())
            .await
            .expect("upsert session");
        let version = capture_shared_session(&pool, graph_id, &sid, json!({
            "session_kind": "coding", "project_key": "proj", "session_title": title
        })).await;
        let atom = seed_atom(&store, &pool, graph_id, user_id, &format!("cc:{sid}:ckpt-1"), "checkpoint", None).await;
        let mut tx = pool.begin().await.unwrap();
        coralswarm_backend::source_evidence::link_verified_chunks_tx(&mut tx, graph_id, version, &[atom]).await.unwrap();
        tx.commit().await.unwrap();
    }
    for i in 0..UNATTRIBUTED {
        seed_unattributed_version(&store, &pool, graph_id, user_id, i).await;
    }
    let versions: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(versions, (SESSIONS + UNATTRIBUTED) as i64);

    // The rows: the handler's exact call for `?kind=coding` (limit + 1 for
    // `has_more`), then the keyset walk.
    let filter = SessionListFilter { roots_only: true, ..Default::default() };
    let first = store
        .list_coding_sessions(graph_id, None, Some("coding"), None, None, PAGE + 1, &filter)
        .await
        .expect("first page");
    assert_eq!(first.len() as i64, PAGE + 1, "limit + 1 rows prove has_more");
    for row in &first {
        assert_eq!(row.coral_count, 1, "every session has exactly its own coral: {row:?}");
        assert_eq!(row.subagent_count, 0, "no subagents in this fixture: {row:?}");
        assert_eq!(row.session_kind, "coding");
    }
    let mut seen: Vec<String> = first.iter().take(PAGE as usize).map(|r| r.session_id.clone()).collect();
    let last = &first[PAGE as usize - 1];
    let second = store
        .list_coding_sessions(graph_id, None, Some("coding"), None, Some((last.last_activity_at, last.id)), PAGE + 1, &filter)
        .await
        .expect("second page");
    assert_eq!(second.len(), SESSIONS - PAGE as usize, "the rest fits on page two");
    seen.extend(second.iter().map(|r| r.session_id.clone()));
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), SESSIONS, "the keyset walk lists every session exactly once");

    // The plan: a constant number of passes over the graph's versions.
    eprintln!("CODING_SESSIONS_LIST_SQL plan:");
    let fixed = source_versions_probes(&explain_list(&pool, graph_id, coralswarm_backend::store::CODING_SESSIONS_LIST_SQL).await);
    eprintln!("LEGACY_PER_ROW_LIST_SQL plan:");
    let legacy = source_versions_probes(&explain_list(&pool, graph_id, LEGACY_PER_ROW_LIST_SQL).await);
    let bound = (5 * versions) as f64;
    eprintln!("source_versions probes: fixed={fixed} legacy={legacy} bound={bound} (versions={versions}, page={PAGE})");
    assert!(
        fixed <= bound,
        "the list statement re-resolves the graph's versions per page row again: {fixed} source_versions probes for {versions} versions (bound {bound})"
    );
    assert!(
        legacy > bound,
        "control: the legacy per-row statement must exceed the bound or the assertion above proves nothing ({legacy} probes, bound {bound})"
    );

    cleanup(&pool, account_id, user_id).await;
}
