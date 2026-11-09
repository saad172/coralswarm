//! Live smoke test for the retrieval eval harness against a REAL Postgres.
//!
//! Ingests the committed fixture corpus through the real chunker + header + FTS
//! + embedding path (deterministic hash embedder — no model download), runs the
//! committed golden set through the mirrored retrieval path, and asserts the
//! aggregate metrics are sane. This is the `cargo test` entry point for the
//! harness; the CI gate itself runs the `retrieval_eval` binary (which compares
//! against `backend/eval/baseline.json`).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied); a normal `cargo test` with no DB configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55433/coralswarm \
//!     cargo test -p coralswarm-backend --test retrieval_eval_pg -- --nocapture

use std::path::PathBuf;

use coralswarm_backend::eval::{self, FixtureDoc, GoldenQuery, HashEmbedder, ReefFixture};
use coralswarm_backend::retrieval::rerank::RerankParams;
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

fn eval_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("eval")
}

#[tokio::test(flavor = "multi_thread")]
async fn eval_harness_runs_end_to_end_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the retrieval eval smoke");
            return;
        }
    };

    let dir = eval_dir();
    let corpus: Vec<FixtureDoc> =
        eval::load_jsonl(&dir.join("fixtures/corpus.jsonl")).expect("load corpus");
    let golden: Vec<GoldenQuery> =
        eval::load_jsonl(&dir.join("golden/golden.jsonl")).expect("load golden");
    let reef_fixtures: Vec<ReefFixture> =
        eval::load_jsonl(&dir.join("fixtures/reef.jsonl")).expect("load reef fixtures");
    assert!(!corpus.is_empty() && !golden.is_empty());

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect test Postgres");

    let email = format!("eval-{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("seed user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("seed account");

    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
        .bind(account_id).bind(user_id).execute(&pool).await.expect("seed owner membership");
    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &eval::unique_graph_name(), "personal")
        .await
        .expect("create graph");

    let embedder = HashEmbedder;
    let doc_atoms = eval::ingest_corpus(
        &store,
        &pool,
        &embedder,
        graph.id,
        user_id,
        &corpus,
    )
    .await
    .expect("ingest corpus");
    assert_eq!(doc_atoms.len(), corpus.len(), "every doc produced atoms");

    let reef_nodes = eval::ingest_reef_fixtures(&store, graph.id, &doc_atoms, &reef_fixtures)
        .await
        .expect("ingest reef fixtures");
    let supports = eval::single_member_fixture_supports(&doc_atoms, &reef_fixtures).unwrap();
    eval::curate_reef_fixtures(&store, &pool, graph.id, account_id, user_id,
        &supports, &reef_fixtures, &reef_nodes).await.expect("curate reef identities");

    let report = eval::run_eval(
        &store,
        &embedder,
        graph.id,
        &doc_atoms,
        &golden,
        "hash",
        eval::DEFAULT_TOP_K,
        &RerankParams::default(),
    )
    .await
    .expect("run eval");

    println!(
        "eval: Recall@20={:.4} nDCG@10={:.4} abstention-acc={:.4}",
        report.overall.recall_at_20, report.overall.ndcg_at_10, report.overall.abstention_accuracy
    );

    // Sanity floors (well below the committed baseline; this test proves the
    // pipeline works end-to-end, not the exact numbers — the binary gates those).
    assert!(
        report.overall.recall_at_20 > 0.75,
        "Recall@20 unexpectedly low: {}",
        report.overall.recall_at_20
    );
    assert!(
        report.overall.ndcg_at_10 > 0.6,
        "nDCG@10 unexpectedly low: {}",
        report.overall.ndcg_at_10
    );
    // Answerable queries must (almost) never abstain — proves retrieval fired.
    let direct = &report.by_category["direct"];
    assert!(
        direct.abstention_accuracy > 0.95,
        "direct queries should not abstain: {}",
        direct.abstention_accuracy
    );
}
