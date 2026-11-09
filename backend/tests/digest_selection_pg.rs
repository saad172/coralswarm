//! Live-Postgres proof of §2.4's coral selection against the EXACT failure
//! mode the graph-temporal-memory plan measured: "the first 50 corals
//! returned for the 'Chris Marchetti' reef (298 total) are four coding-session
//! chunks followed by 46 consecutive chunks of a single meeting."
//!
//! Builds a reef with real pgvector rows shaped the same way: a handful of
//! genuinely on-topic corals (diverse embeddings, clustered near the reef
//! title's own embedding) plus a 46-atom run from ONE meeting whose chunks
//! are near-duplicates of each other. Drives the REAL
//! `Store::node_digest_coral_candidates` (pgvector `<=>` ranking) into the
//! REAL `workers::digest::select_digest_corals`, and asserts the meeting run
//! is capped at `DIGEST_PER_SOURCE_CAP` rather than dominating the selection.
//!
//! Scratch-DB pattern shared by every `*_pg.rs` file in this directory (see
//! `digest_trigger_pg.rs` / `geo_backfill_pg.rs`). Gated on `ORGT_TEST_BASE_URL`.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:55987/postgres \
//!     cargo test -p coralswarm-backend --test digest_selection_pg -- --nocapture

use std::collections::HashMap;

use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::workers::digest::{select_digest_corals, DIGEST_PER_SOURCE_CAP};
use pgvector::Vector;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

const DIM: usize = 384;

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
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&with_db(base, db))
        .await
        .expect("connect control");
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .unwrap();
    pool
}

/// The reef-title direction: a pure one-hot vector on dimension 0.
fn title_direction() -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[0] = 1.0;
    v
}

/// An "on-topic" coral: mostly aligned with the title direction plus a tiny,
/// `variant`-seeded perturbation — cosine similarity to the title is close
/// to 1.0 (the best-scoring cluster), so these anchor `best_score` for the
/// relative-floor check.
fn on_topic_vec(variant: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[0] = 1.0;
    v[1 + (variant % 10)] = 0.02 * ((variant % 5) as f32 + 1.0);
    normalize(v)
}

/// A "same meeting, different moment" coral: an EQUAL blend of the title
/// direction and a moment-specific direction unique to `variant`. Cosine to
/// the title works out to `0.7 / sqrt(0.98) ≈ 0.707` — comfortably above a
/// 0.6-of-best floor when `on_topic_vec` scores near 1.0, so these clear the
/// relevance floor on their own merits (this reef genuinely discusses this
/// meeting) — the SAME shape as the plan's own observation that heap order,
/// not irrelevance, was the bug. Pairwise cosine between two DIFFERENT
/// meeting-moment vectors is `0.49 / 0.98 = 0.5` (their `0.7` title-aligned
/// components dot; their moment-specific components are orthogonal to each
/// other) — well under the 0.92 novelty threshold, so novelty does NOT
/// explain why they get trimmed; only the per-source cap does, isolating
/// exactly the mechanism this test is proving.
fn same_meeting_vec(variant: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[0] = 0.7;
    v[100 + variant] = 0.7;
    normalize(v)
}

fn normalize(v: Vec<f32>) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.iter().map(|x| x / norm).collect()
}

async fn seed_graph(pool: &PgPool) -> Uuid {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("sel-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, plan_tier) VALUES ($1, 'free') RETURNING id",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, 'test') RETURNING id")
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn insert_atom_with_embedding(
    pool: &PgPool,
    graph_id: Uuid,
    source_id: &str,
    embedding: &[f32],
) -> Uuid {
    sqlx::query_scalar(
        // Selection ranks `embedding_next` (migration 0231/0093); write the
        // same vector to both columns.
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp, embedding, embedding_next) \
         VALUES ($1, 'slack', $2, $3, now(), $4, $4) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(format!("blob-{source_id}"))
    .bind(Vector::from(embedding.to_vec()))
    .fetch_one(pool)
    .await
    .expect("insert atom with embedding")
}

#[tokio::test]
async fn selection_prevents_one_meeting_from_dominating_a_large_reef() {
    let Some(base) = base_url() else {
        eprintln!("skipping selection_prevents_one_meeting_from_dominating_a_large_reef: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_digestsel1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;
    let store = PostgresStore::new(pool.clone());

    let graph_id = seed_graph(&pool).await;
    let node_id: Uuid =
        sqlx::query_scalar("INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', 'Chris Marchetti') RETURNING node_id")
            .bind(graph_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    let title_vec = title_direction();
    let mut source_of: HashMap<Uuid, String> = HashMap::new();

    // 4 on-topic coding-session corals (distinct sources, matching the
    // plan's own "four coding-session chunks" observation) — these anchor
    // `best_score` near 1.0.
    for i in 0..4 {
        let source = format!("coding-session-{i}");
        let atom_id = insert_atom_with_embedding(
            &pool,
            graph_id,
            &format!("{source}:chunk:0"),
            &on_topic_vec(i),
        )
        .await;
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(&pool)
            .await
            .unwrap();
        source_of.insert(atom_id, source);
    }

    // 46 chunks of ONE meeting — the exact "46 consecutive chunks of a
    // single meeting" the plan measured. Each clears the relevance floor on
    // its own merits (cosine ≈0.707 to the title, well above 0.6×best) and
    // is NOT a near-duplicate of its meeting siblings (pairwise cosine
    // ≈0.5, well under the 0.92 novelty threshold) — so the per-source cap,
    // not the floor or novelty, is what has to do the trimming here. All 46
    // share ONE base source_id (`base_source_id` strips ":chunk:<n>"), so
    // the cap applies to all of them as a single source.
    for i in 0..46 {
        let atom_id = insert_atom_with_embedding(
            &pool,
            graph_id,
            &format!("weekly-standup-2026-01-05:chunk:{i}"),
            &same_meeting_vec(i),
        )
        .await;
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(&pool)
            .await
            .unwrap();
        source_of.insert(atom_id, "weekly-standup-2026-01-05".to_string());
    }

    // Rank by cosine similarity to the reef TITLE's embedding (§2.4) —
    // exactly what the background worker does.
    let candidates = store
        .node_digest_coral_candidates(node_id, &title_vec, 200)
        .await
        .expect("fetch candidates");
    assert_eq!(
        candidates.len(),
        50,
        "all 50 linked corals must be embedded candidates"
    );

    let selected = select_digest_corals(&candidates);

    // The meeting run must be CAPPED, not dominant.
    let meeting_selected = selected
        .iter()
        .filter(|id| {
            source_of
                .get(id)
                .map(|s| s == "weekly-standup-2026-01-05")
                .unwrap_or(false)
        })
        .count();
    assert!(
        meeting_selected <= DIGEST_PER_SOURCE_CAP,
        "the 46-chunk meeting must be capped at {DIGEST_PER_SOURCE_CAP} (got {meeting_selected})"
    );

    // And the reef must not be reduced to JUST that one source: at least one
    // of the four on-topic coding-session corals must survive selection —
    // the entire point of ranking by title similarity plus the per-source
    // cap, rather than heap order.
    let coding_selected = selected
        .iter()
        .filter(|id| {
            source_of
                .get(id)
                .map(|s| s.starts_with("coding-session"))
                .unwrap_or(false)
        })
        .count();
    assert!(
        coding_selected >= 1,
        "at least one on-topic coding-session coral must survive selection (got {coding_selected})"
    );

    eprintln!(
        "selected {} corals total: {} from the coding-session sources, {} from the 46-chunk meeting (capped from 46)",
        selected.len(),
        coding_selected,
        meeting_selected
    );
}
