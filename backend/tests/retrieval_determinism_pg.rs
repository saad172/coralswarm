//! Live regression suite proving `PostgresStore::vector_search` /
//! `PostgresStore::keyword_search` are deterministic (graph-temporal-memory
//! hardening, fix 1).
//!
//! Before this fix, both queries were plain `ORDER BY <score> LIMIT $k` with
//! no secondary sort key. When scores tie — which happens routinely, and is
//! deliberately engineered here — a tied cohort straddling the retrieval
//! candidate cap (`RETRIEVAL_CANDIDATE_K = 50` in production) was cut
//! arbitrarily and could differ between runs, so *which* atoms reach the
//! reranker was nondeterministic. Both queries now carry `atom_id` as a
//! deterministic secondary sort key.
//!
//! This file seeds a corpus where:
//!   - 45 atoms have a strictly-decreasing (vector) / strictly-higher-coverage
//!     (keyword) score, so they always make the cut regardless of tiebreak;
//!   - 20 MORE atoms are given an EXACT tie with each other, deliberately
//!     placed just past the top 45 — so with `k = 50`, the tied cohort of 20
//!     straddles the cap and only its 5 lowest-`atom_id` members should ever
//!     be selected.
//!
//! Each query runs 3 times; the assertion is that all 3 runs return the exact
//! same atoms in the exact same order, AND that the 5 selected from the tied
//! cohort are precisely its 5 lowest-`atom_id` members (proving the cut is
//! governed by the `atom_id` tiebreak, not an accident of scan/plan order).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied — same convention as `expand_d1_pg.rs` / `retrieval_eval_pg.rs`); a
//! normal `cargo test` with no DB configured SKIPS LOUDLY.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/graph_hardening_wt \
//!     cargo test -p coralswarm-backend --test retrieval_determinism_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::eval::RETRIEVAL_CANDIDATE_K;
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const SKIP_REASON: &str =
    "skipping: set RELINFER_TEST_DATABASE_URL to run the live retrieval-determinism regression \
     suite (e.g. RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/graph_hardening_wt)";

/// Number of atoms with a strictly-distinct score — always inside the cap
/// regardless of tiebreak. Chosen so `WINNERS + 5 == RETRIEVAL_CANDIDATE_K`,
/// i.e. exactly 5 of the 20 tied atoms below should survive the cut.
const WINNERS: usize = RETRIEVAL_CANDIDATE_K - 5;
/// Size of the exactly-tied cohort straddling the cap boundary.
const TIED_COHORT: usize = 20;

/// Mirrors `expand_d1_pg.rs`'s setup exactly.
async fn setup(url: &str) -> (PostgresStore, sqlx::PgPool, Uuid, Uuid) {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(url)
        .await
        .expect("connect to test Postgres");

    let email = format!("retrieval-determinism-{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("insert account");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(
            account_id,
            &format!("retrieval-determinism-{}", Uuid::new_v4()),
            "personal",
        )
        .await
        .expect("create graph");
    (store, pool, user_id, graph.id)
}

/// Bulk-insert `n` bare atoms with distinct `source_id`s and no content/embedding
/// yet (both are stamped separately by the two tests below).
async fn insert_atoms(store: &PostgresStore, graph_id: Uuid, user_id: Uuid, n: usize) -> Vec<Uuid> {
    let ids: Vec<Uuid> = (0..n).map(|_| Uuid::new_v4()).collect();
    let source_ids: Vec<String> = (0..n)
        .map(|i| format!("determinism-src-{i}-{}", Uuid::new_v4()))
        .collect();
    let blob_keys: Vec<String> = ids.iter().map(|id| format!("blob-{id}")).collect();

    let inserted = store
        .insert_atoms(
            &ids,
            graph_id,
            "note",
            &source_ids,
            &blob_keys,
            &["alice".to_string()],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("bulk insert atoms");
    assert_eq!(inserted.len(), n, "every atom should insert cleanly");
    ids
}

// ── Fix 1: vector_search ─────────────────────────────────────────────────────

/// Stamp an atom's embedding + `embed_model` directly (mirrors
/// `expand_d1_pg.rs`'s `stamp_embedding` helper).
async fn stamp_embedding(pool: &sqlx::PgPool, atom_id: Uuid, embedding: &[f32]) {
    sqlx::query("UPDATE atoms SET embedding = $1, embed_model = $2, embedding_next = $1, embed_model_next = $3 WHERE atom_id = $4")
        .bind(pgvector::Vector::from(embedding.to_vec()))
        .bind(coralswarm_core::embed::EMBED_MODEL_NAME)
        .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("stamp embedding");
}

/// A 384-dim embedding whose cosine distance to the query `[1, 0, 0, ...]`
/// increases monotonically with `i` — always strictly ordered, never tied.
fn winner_embedding(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 384];
    v[0] = 1.0;
    // Larger i -> larger second-component -> larger angle from the query ->
    // larger cosine distance, but always < the fully-orthogonal tied cohort's
    // distance (which uses component 0 = 0.0 entirely).
    v[1] = 0.001 * (i as f32 + 1.0);
    v
}

/// The exactly-tied embedding: fully orthogonal to the query, so every atom
/// stamped with this exact vector has an IDENTICAL cosine distance to the
/// query (1.0) — a genuine SQL-level score tie, not merely "close".
fn tied_embedding() -> Vec<f32> {
    let mut v = vec![0.0f32; 384];
    v[2] = 1.0;
    v
}

#[tokio::test]
async fn vector_search_tied_cohort_straddling_cap_is_deterministic_across_runs() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };
    let (store, pool, user_id, graph_id) = setup(&url).await;

    // 45 atoms with a strictly distinct (always-better) distance.
    let winner_ids = insert_atoms(&store, graph_id, user_id, WINNERS).await;
    for (i, &id) in winner_ids.iter().enumerate() {
        stamp_embedding(&pool, id, &winner_embedding(i)).await;
    }

    // 20 atoms with an EXACT tied distance, all strictly worse than every
    // winner above (fully orthogonal to the query vs. winners' near-parallel
    // vectors) — this is the tied cohort straddling the k=50 cap.
    let mut tied_ids = insert_atoms(&store, graph_id, user_id, TIED_COHORT).await;
    for &id in &tied_ids {
        stamp_embedding(&pool, id, &tied_embedding()).await;
    }
    tied_ids.sort();
    let expected_from_tied: Vec<Uuid> = tied_ids.iter().take(5).copied().collect();

    let query_embedding = {
        let mut v = vec![0.0f32; 384];
        v[0] = 1.0;
        v
    };

    // Run 3 times; every run must return byte-identical output.
    let mut runs = Vec::new();
    for _ in 0..3 {
        let res = store
            .vector_search(
                graph_id,
                &query_embedding,
                RETRIEVAL_CANDIDATE_K,
                &RetrievalFilter::default(),
            )
            .await
            .expect("vector_search");
        runs.push(res);
    }

    for (i, run) in runs.iter().enumerate() {
        assert_eq!(
            run.len(),
            RETRIEVAL_CANDIDATE_K,
            "run {i}: expected exactly k={RETRIEVAL_CANDIDATE_K} candidates"
        );
    }
    assert_eq!(runs[0], runs[1], "run 0 and run 1 must be byte-identical");
    assert_eq!(runs[1], runs[2], "run 1 and run 2 must be byte-identical");

    // All 45 winners must be present (they always beat the tied cohort).
    let run0_ids: Vec<Uuid> = runs[0].iter().map(|(id, _)| *id).collect();
    for &w in &winner_ids {
        assert!(run0_ids.contains(&w), "every winner must survive the cap");
    }

    // Exactly the 5 lowest-atom_id members of the tied cohort must be the
    // ones that made the cut — proving the cut is governed by the atom_id
    // tiebreak, not scan-order happenstance.
    let selected_from_tied: Vec<Uuid> = run0_ids
        .iter()
        .filter(|id| tied_ids.contains(id))
        .copied()
        .collect();
    assert_eq!(
        selected_from_tied.len(),
        5,
        "exactly 5 of the 20-way tied cohort should straddle the k={RETRIEVAL_CANDIDATE_K} cap"
    );
    let mut selected_sorted = selected_from_tied.clone();
    selected_sorted.sort();
    assert_eq!(
        selected_sorted, expected_from_tied,
        "the tied cohort must be cut at its 5 lowest atom_ids, deterministically"
    );
}

// ── Fix 1: keyword_search ─────────────────────────────────────────────────────

/// Stamp an atom's `title` + `content_tsv` the way production does, so its
/// keyword score against [`KEYWORD_QUERY`] is controlled by how many DISTINCT
/// query tokens it carries.
///
/// **This fixture used to work the other way**: it repeated ONE query term
/// `weight` times, because `keyword_search` then ranked on a bare
/// `ts_rank_cd` and more repetitions meant a strictly higher score. That axis
/// is gone on purpose. `keyword_search` now ranks by term COVERAGE with
/// `ts_rank_cd(..., 2|32)` — length-normalised — as the tie-break, and
/// length-normalisation makes repetition-only documents score IDENTICALLY
/// (measured: `to_tsvector('simple', repeat('coral ', n))` ranks `0.0909` for
/// every `n`, because dividing by the document's word count cancels the
/// frequency exactly). That is the whole point of the change — a long file
/// that repeats one common word is not more relevant than a short one that
/// answers the question — so this fixture is rebuilt on the score axis that
/// now exists, NOT loosened to accommodate the old one. The contract under
/// test is unchanged: a genuinely tied cohort straddling the candidate cap is
/// cut at its lowest `atom_id`s, identically on every run.
async fn stamp_title_and_content(pool: &sqlx::PgPool, atom_id: Uuid, both_tokens: bool) {
    // Both cohorts are reachable by the keyword leg's title predicate (an OR
    // over the query's tokens); they differ only in how many DISTINCT query
    // tokens they cover — 2 for a winner, 1 for the tied cohort.
    let (title, content) = if both_tokens {
        ("coral reef study", "coral reef")
    } else {
        ("coral study", "coral")
    };
    sqlx::query(
        "UPDATE atoms SET title = $1, title_kind = 'llm', \
                          content_tsv = to_tsvector('simple', $2) \
         WHERE atom_id = $3",
    )
    .bind(title)
    .bind(content)
    .bind(atom_id)
    .execute(pool)
    .await
    .expect("stamp title + content_tsv");
}

/// Two tokens, so coverage has two distinct levels to separate the cohorts.
const KEYWORD_QUERY: &str = "coral reef";

#[tokio::test]
async fn keyword_search_tied_cohort_straddling_cap_is_deterministic_across_runs() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };
    let (store, pool, user_id, graph_id) = setup(&url).await;

    // 45 atoms covering BOTH query tokens — a strictly better keyword score
    // than the tied cohort below, so every one of them always makes the cut.
    let winner_ids = insert_atoms(&store, graph_id, user_id, WINNERS).await;
    for &id in winner_ids.iter() {
        stamp_title_and_content(&pool, id, true).await;
    }

    // 20 atoms covering ONE token, with identical text → an exact tie in both
    // the coverage sort and the `ts_rank_cd` tie-break, all strictly worse
    // than every winner above. This is the cohort straddling the k=50 cap.
    let mut tied_ids = insert_atoms(&store, graph_id, user_id, TIED_COHORT).await;
    for &id in &tied_ids {
        stamp_title_and_content(&pool, id, false).await;
    }
    tied_ids.sort();
    let expected_from_tied: Vec<Uuid> = tied_ids.iter().take(5).copied().collect();

    let mut runs = Vec::new();
    for _ in 0..3 {
        let res = store
            .keyword_search(
                graph_id,
                KEYWORD_QUERY,
                RETRIEVAL_CANDIDATE_K,
                &RetrievalFilter::default(),
            )
            .await
            .expect("keyword_search");
        runs.push(res);
    }

    for (i, run) in runs.iter().enumerate() {
        assert_eq!(
            run.len(),
            RETRIEVAL_CANDIDATE_K,
            "run {i}: expected exactly k={RETRIEVAL_CANDIDATE_K} candidates"
        );
    }
    assert_eq!(runs[0], runs[1], "run 0 and run 1 must be byte-identical");
    assert_eq!(runs[1], runs[2], "run 1 and run 2 must be byte-identical");

    let run0_ids: Vec<Uuid> = runs[0].iter().map(|(id, _)| *id).collect();
    for &w in &winner_ids {
        assert!(run0_ids.contains(&w), "every winner must survive the cap");
    }

    let selected_from_tied: Vec<Uuid> = run0_ids
        .iter()
        .filter(|id| tied_ids.contains(id))
        .copied()
        .collect();
    assert_eq!(
        selected_from_tied.len(),
        5,
        "exactly 5 of the 20-way tied cohort should straddle the k={RETRIEVAL_CANDIDATE_K} cap"
    );
    let mut selected_sorted = selected_from_tied.clone();
    selected_sorted.sort();
    assert_eq!(
        selected_sorted, expected_from_tied,
        "the tied cohort must be cut at its 5 lowest atom_ids, deterministically"
    );
}
