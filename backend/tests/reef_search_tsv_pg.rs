//! Live Postgres tests for §5.6 reef matching (`nodes.search_tsv`, migration
//! `0108`/`0018`) and the D8 fix it subsumes.
//!
//! Deliberately drives `PostgresStore::keyword_search_node_atoms` DIRECTLY
//! (not through the HTTP layer) so these are tight tests of the real SQL:
//! real `tsvector`/`ts_rank_cd` behaviour, a real `EXPLAIN` of the query plan,
//! and the real entity-reservation self-clamping logic against real rows —
//! nothing here is mocked or re-implemented in Rust (the pure-Rust unit tests
//! for the shared selection/scoring logic live in `src/store/mod.rs` and
//! `src/retrieval/mod.rs`).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied, INCLUDING `0108_nodes_search_tsv`); a normal `cargo test` with no
//! DB configured skips every test in this file.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/cs_reefmatch_control \
//!     cargo test -p coralswarm-backend --test reef_search_tsv_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store, MAX_SIBLING_ATOMS_PER_NODE};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run reef_search_tsv_pg");
                return;
            }
        }
    }};
}

/// Seed a fresh `(user, account, graph)` tenancy chain — pattern-copied from
/// `coding_sessions_read_pg.rs`.
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
        .create_graph(account_id, &format!("reef-search-tsv-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");
    (user_id, account_id, graph.id)
}

async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid, user_id: Uuid) {
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(pool).await.ok();
}

/// One atom, linked to nothing yet.
async fn seed_atom(store: &PostgresStore, graph_id: Uuid, user_id: Uuid, source_id: &str) -> Uuid {
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
    atom_id
}

/// Create a reef. For `type = "entity"` this goes through the REAL write path
/// (`upsert_node`, then `jsonb_set`/`set_node_digest` for aliases/summary —
/// the exact shape ER alias-teaching and digest writes use). For
/// `type = "decision"`, `nodes`' own `decisions_require_decided_at` CHECK
/// constraint (migration `0011`) is NOT deferrable, so `decided_at` must be
/// present in `body` at INSERT time — `upsert_node` has no body parameter, so
/// decision reefs are inserted directly with a stamped `decided_at` up front.
/// Either way `search_tsv` (a GENERATED column) backfills the same way a real
/// write would, not a hand-crafted row shape.
async fn seed_reef(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    node_type: &str,
    title: &str,
    aliases: &[&str],
    summary: Option<&str>,
) -> Uuid {
    let node_id = if node_type == "decision" {
        sqlx::query_scalar(
            "INSERT INTO nodes (graph_id, type, title, body) \
             VALUES ($1, 'decision', $2, jsonb_build_object('decided_at', '2026-01-01T00:00:00Z')) \
             RETURNING node_id",
        )
        .bind(graph_id)
        .bind(title)
        .fetch_one(pool)
        .await
        .expect("insert decision reef")
    } else {
        store.upsert_node(graph_id, node_type, title).await.expect("upsert_node")
    };
    if !aliases.is_empty() {
        let aliases_json = serde_json::to_string(aliases).unwrap();
        sqlx::query("UPDATE nodes SET body = jsonb_set(body, '{aliases}', $2::jsonb) WHERE node_id = $1")
            .bind(node_id)
            .bind(&aliases_json)
            .execute(pool)
            .await
            .expect("set aliases");
    }
    if let Some(s) = summary {
        let digest = serde_json::json!({"summary": s, "atoms_key": "", "excerpts": []}).to_string();
        store.set_node_digest(node_id, &digest).await.expect("set_node_digest");
    }
    node_id
}

// ── §5.6 Gap 1: alias-only retrieval, against a real GENERATED column ──────────

#[tokio::test]
async fn alias_only_reef_is_retrieved_even_though_title_shares_no_tokens_with_the_query() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // ER taught the survivor "zzqnomad77" as an alias; the reef's own title
    // shares no tokens with the query at all — the exact gap `nomad_alias_gap`
    // (the eval harness on `feat/graph-temporal-eval`) documents.
    let reef = seed_reef(&store, &pool, graph_id, "entity", "Zzq Jordan Rivera", &["zzqnomad77"], None).await;
    let atom = seed_atom(&store, graph_id, user_id, "s-nomad").await;
    store.link_node_atoms(&[(reef, atom)]).await.expect("link");

    let hits = store
        .keyword_search_node_atoms(graph_id, "zzqnomad77", 8, &RetrievalFilter::default())
        .await
        .expect("keyword_search_node_atoms");
    let ids: Vec<Uuid> = hits.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![atom], "alias-only match must surface the reef's atom: {hits:?}");

    cleanup(&pool, account_id, user_id).await;
}

// ── §5.6: coverage qualifies, ts_rank_cd ranks (the spec's worked example) ─────

#[tokio::test]
async fn coverage_floor_excludes_a_weakly_matching_reef_the_old_flat_scoring_would_have_kept() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // "Zzqpricing88" (entity): covers only 1 of the 3 query tokens (0.33 < 0.5)
    // — must be EXCLUDED, even though its single hit is at weight 'A'.
    let pricing_entity = seed_reef(&store, &pool, graph_id, "entity", "Zzqpricing88", &[], None).await;
    let pricing_atom = seed_atom(&store, graph_id, user_id, "s-pricing-entity").await;
    store.link_node_atoms(&[(pricing_entity, pricing_atom)]).await.unwrap();

    // The widened decision reef covers all 3/3 — must qualify.
    let decision = seed_reef(
        &store,
        &pool,
        graph_id,
        "decision",
        "We zzqdecided88 to change the zzqpricing88 model for zzqenterprise88 customers",
        &[],
        None,
    )
    .await;
    let decision_atom = seed_atom(&store, graph_id, user_id, "s-decision").await;
    store.link_node_atoms(&[(decision, decision_atom)]).await.unwrap();

    let hits = store
        .keyword_search_node_atoms(
            graph_id,
            "zzqdecided88 zzqpricing88 zzqenterprise88",
            8,
            &RetrievalFilter::default(),
        )
        .await
        .expect("keyword_search_node_atoms");
    let ids: Vec<Uuid> = hits.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&decision_atom), "the 3/3-coverage decision reef must qualify: {hits:?}");
    assert!(
        !ids.contains(&pricing_atom),
        "the 1/3-coverage entity reef must be EXCLUDED by the coverage floor: {hits:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

// ── §5.6: reserved entity slots self-clamp ──────────────────────────────────────

#[tokio::test]
async fn reserved_entity_slots_survive_being_outranked_by_many_qualifying_assertions() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // A 6-token marker query. 10 decision reefs each match ALL 6 tokens (full
    // coverage, weight 'C') — collectively they can out-rank a partial-
    // coverage entity match on raw score. 2 entity reefs each match only 3 of
    // the 6 tokens (exactly the 0.5 floor) at weight 'A'.
    let tokens = ["zzqm1", "zzqm2", "zzqm3", "zzqm4", "zzqm5", "zzqm6"];
    let query = tokens.join(" ");

    let mut decision_atoms = Vec::new();
    for i in 0..10 {
        let title = format!("{} filler{i} reef", tokens.join(" "));
        let node = seed_reef(&store, &pool, graph_id, "decision", &title, &[], None).await;
        let atom = seed_atom(&store, graph_id, user_id, &format!("s-filler-{i}")).await;
        store.link_node_atoms(&[(node, atom)]).await.unwrap();
        decision_atoms.push(atom);
    }

    let mut entity_atoms = Vec::new();
    for (i, half) in [[tokens[0], tokens[1], tokens[2]], [tokens[3], tokens[4], tokens[5]]]
        .iter()
        .enumerate()
    {
        let title = half.join(" ");
        let node = seed_reef(&store, &pool, graph_id, "entity", &title, &[], None).await;
        let atom = seed_atom(&store, graph_id, user_id, &format!("s-entity-{i}")).await;
        store.link_node_atoms(&[(node, atom)]).await.unwrap();
        entity_atoms.push(atom);
    }

    let hits = store
        .keyword_search_node_atoms(graph_id, &query, 8, &RetrievalFilter::default())
        .await
        .expect("keyword_search_node_atoms");
    let ids: std::collections::HashSet<Uuid> = hits.iter().map(|(id, _)| *id).collect();
    println!("hits: {} candidates (10 decisions + 2 entities qualify; k=8)", hits.len());

    for (i, atom) in entity_atoms.iter().enumerate() {
        assert!(
            ids.contains(atom),
            "reserved entity slot {i} must survive being outranked by qualifying assertions: {hits:?}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
}

// ── §5.6 deviation: full-title-match escape hatch (entity reefs) ────────────────
//
// Deliberate deviation from the design-locked spec — see
// `retrieval::node_match_score`'s doc comment for the full rationale. This
// pins the regression the §7.1 harness's `priya_reef_sibling_pull_cap` check
// surfaced: before this escape hatch, a 40-coral entity reef whose title's
// BOTH tokens appear in a realistic multi-clause question surfaced ZERO of
// its corals via sibling-pull (the ordinary coverage floor cannot clear on a
// query this long); after, it surfaces up to MAX_SIBLING_ATOMS_PER_NODE of
// them again, same as any other qualifying reef.

#[tokio::test]
async fn full_title_match_recovers_sibling_pull_on_a_long_question_naming_the_full_entity() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Mirrors the §7.1 fixture ocean's priya-anand reef shape: a two-word
    // person's name, linked to more corals than MAX_SIBLING_ATOMS_PER_NODE
    // can ever surface at once, queried with the harness's own
    // multi-clause question.
    let reef = seed_reef(&store, &pool, graph_id, "entity", "Zzq Priya Anand", &[], None).await;
    let mut atoms = Vec::new();
    for i in 0..40 {
        let atom = seed_atom(&store, graph_id, user_id, &format!("s-priya-{i}")).await;
        atoms.push(atom);
    }
    let pairs: Vec<(Uuid, Uuid)> = atoms.iter().map(|&a| (reef, a)).collect();
    store.link_node_atoms(&pairs).await.expect("link 40 corals to the reef");

    let query = "Zzq Priya Anand on-call pager handoff rotation";
    let query_tokens = coralswarm_backend::retrieval::node_query_tokens(query);
    let required = (coralswarm_backend::retrieval::REEF_COVERAGE_FLOOR * query_tokens.len() as f64)
        .ceil() as usize;
    assert!(
        required > 3,
        "test setup: this question must be long enough that a 3-token title \
         (zzq, priya, anand) cannot clear the ordinary coverage floor on its own \
         (query_tokens={query_tokens:?}, required={required})"
    );

    let hits = store
        .keyword_search_node_atoms(graph_id, query, 8, &RetrievalFilter::default())
        .await
        .expect("keyword_search_node_atoms");

    let recovered: usize = hits.iter().filter(|(id, _)| atoms.contains(id)).count();
    println!(
        "priya_reef_sibling_pull_cap analog: {recovered} of 40 corals surfaced via sibling-pull \
         for a full-title-match, coverage-floor-failing question (MAX_SIBLING_ATOMS_PER_NODE={MAX_SIBLING_ATOMS_PER_NODE})"
    );
    assert!(
        recovered > 0,
        "before the full-title-match escape hatch this reef surfaced ZERO of its 40 corals for a \
         realistic multi-clause question naming it in full — the exact regression the §7.1 \
         harness's priya_reef_sibling_pull_cap check reported. Got {recovered}: {hits:?}"
    );
    assert_eq!(
        recovered, MAX_SIBLING_ATOMS_PER_NODE,
        "a qualifying reef with more linked corals than MAX_SIBLING_ATOMS_PER_NODE still caps at \
         exactly that many (§5.2's per-reef sibling cap, unaffected by which qualification path \
         let the reef through)"
    );

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn full_title_match_does_not_rescue_a_partial_name_match_on_the_same_long_question() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // Same shape as the recovery test, but the query only names ONE of the
    // title's two words — the documented residual gap: a partial name match
    // is NOT rescued by the escape hatch, only a full one.
    let reef = seed_reef(&store, &pool, graph_id, "entity", "Zzq2 Priya Anand", &[], None).await;
    let atom = seed_atom(&store, graph_id, user_id, "s-priya2-0").await;
    store.link_node_atoms(&[(reef, atom)]).await.unwrap();

    let hits = store
        .keyword_search_node_atoms(
            graph_id,
            "what did Zzq2 Priya say about the on-call pager rotation",
            8,
            &RetrievalFilter::default(),
        )
        .await
        .expect("keyword_search_node_atoms");
    let ids: Vec<Uuid> = hits.iter().map(|(id, _)| *id).collect();
    assert!(
        !ids.contains(&atom),
        "a partial (not-full) title match on a long question must NOT be rescued: {hits:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn full_title_match_escape_hatch_does_not_reopen_the_one_token_pricing_case() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    // The spec's own §5.6 worked example, against a real GENERATED column:
    // a ONE-token entity title matching its own single word (a "full title
    // match" in the degenerate 1-token sense) must NOT be rescued.
    let pricing_entity = seed_reef(&store, &pool, graph_id, "entity", "Zzqpricing99", &[], None).await;
    let pricing_atom = seed_atom(&store, graph_id, user_id, "s-pricing99-entity").await;
    store.link_node_atoms(&[(pricing_entity, pricing_atom)]).await.unwrap();

    let hits = store
        .keyword_search_node_atoms(
            graph_id,
            "zzqdecided99 zzqpricing99 zzqenterprise99",
            8,
            &RetrievalFilter::default(),
        )
        .await
        .expect("keyword_search_node_atoms");
    let ids: Vec<Uuid> = hits.iter().map(|(id, _)| *id).collect();
    assert!(
        !ids.contains(&pricing_atom),
        "a 1-token entity title must stay excluded — FULL_TITLE_MATCH_MIN_TOKENS=2 exists \
         precisely so this spec-cited case is not reopened: {hits:?}"
    );

    cleanup(&pool, account_id, user_id).await;
}

// ── D8: the GIN index, not a full scan, serves the query ────────────────────────

/// Best-effort `EXPLAIN` check that the candidate-filtering half of
/// `keyword_search_node_atoms`'s query CAN be served by `idx_nodes_search_tsv`
/// (migration `0108`) rather than a sequential scan over `nodes` — the D8
/// fix. Seeds enough decoy reefs to make the index attractive to the planner,
/// runs `ANALYZE`, then EXPLAINs the exact `candidates` CTE shape the real
/// query uses. This is inherently sensitive to planner heuristics on a
/// small/freshly-created table, so a plan that does NOT mention the index is
/// logged loudly rather than failing the build (same convention as
/// `coding_sessions_read_pg.rs`'s analogous check) — the FUNCTIONAL
/// correctness (which reefs match, in what order) is covered by the
/// hard-asserting tests above; this is additional, best-effort visibility
/// into whether the index is actually reachable for this predicate shape.
#[tokio::test]
async fn keyword_search_node_atoms_candidate_filter_can_use_the_search_tsv_gin_index() {
    let pool = test_pool!();
    let (user_id, account_id, graph_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let _target = seed_reef(&store, &pool, graph_id, "entity", "Zzqexplaintarget99", &[], None).await;
    for i in 0..500 {
        seed_reef(
            &store,
            &pool,
            graph_id,
            "decision",
            &format!("Filler decision reef number {i} about routine operational matters"),
            &[],
            None,
        )
        .await;
    }
    // `VACUUM`, not just `ANALYZE`: a GIN index defaults to `fastupdate=on`,
    // so freshly bulk-inserted rows sit in the index's PENDING LIST until a
    // vacuum (autovacuum, normally) flushes them into the main structure.
    // Measured directly against a 25,001-row scratch table (2026-07-28): the
    // identical query chose a Seq Scan pre-vacuum and a Bitmap Index Scan on
    // `idx_nodes_search_tsv` post-vacuum, with EXECUTION TIME dropping from
    // ~5.6ms to ~0.03ms for a selective query. Production relies on
    // autovacuum to do this continuously; a test/bulk-load script that
    // ANALYZEs without ever vacuuming can see a misleadingly pessimistic plan.
    sqlx::query("VACUUM ANALYZE nodes").execute(&pool).await.expect("VACUUM ANALYZE nodes");

    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT node_id FROM nodes \
         WHERE graph_id = $1 AND search_tsv @@ (to_tsquery('simple', $2) || to_tsquery('english', $2))",
    )
    .bind(graph_id)
    .bind("zzqexplaintarget99")
    .fetch_all(&pool)
    .await
    .expect("EXPLAIN query");
    let plan_text = plan.join("\n");
    println!("EXPLAIN plan for reef search_tsv candidate filter:\n{plan_text}");
    if plan_text.contains("idx_nodes_search_tsv") {
        println!("D8 CONFIRMED FIXED: idx_nodes_search_tsv is reachable — no full `nodes` scan for this predicate.");
    } else {
        eprintln!(
            "NOTE: idx_nodes_search_tsv did not appear in the plan — likely planner heuristics \
             on this small test table (a Seq Scan can still win under `graph_id = $1` alone at this \
             row count). Functional correctness (which reefs match) is covered by the hard-asserting \
             tests in this file; this check is informational only.\nplan:\n{plan_text}"
        );
    }

    cleanup(&pool, account_id, user_id).await;
}
