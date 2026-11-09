//! Live test for the judgement-graph resolver slice, PR5 — the one-shot
//! `backfill_reef_judgement_from_history` (issue #585's PR list).
//!
//! Drives the REAL core (`coralswarm_backend::judgement_backfill::
//! backfill_database`) against real Postgres, plus the REAL compiled binary
//! (via `CARGO_BIN_EXE_…`) end to end — never a re-implementation of either.
//! Modeled on `judgement_graph_pg.rs`: one already-migrated database shared
//! by every test, gated on `RELINFER_TEST_DATABASE_URL`, panic-safe teardown
//! through [`with_cleanup`].
//!
//! **Every test narrows to its OWN ocean with `only_graph`** (the one
//! exception, [`whole_database_run_covers_every_ocean`], asserts only about
//! its own graphs) — the suite shares a database with `judgement_graph_pg`/
//! `judgement_graph_worker_pg`, and a whole-database backfill that asserted
//! global tallies would be a flake generator, not a test.
//!
//! Covers:
//!   * a `not_same` dismissal becomes a NEGATIVE with the EXACT evidence
//!     migration 0139's header documents, `source='backfill'`, and NULL
//!     `created_by`/`expires_at`/`confidence`;
//!   * **the gate-4 clobber control** — a live `llm` POSITIVE, a live `llm`
//!     UNSURE and a live `human` NEGATIVE are each left completely untouched
//!     (this is the mutation-control target: deleting the active-judgement
//!     pre-check makes `write_judgement` annul-and-supersede all three);
//!   * an undone merge, a post-flag merge (`judgement_id` set) and a
//!     dead-endpoint pair are each skipped, ATTRIBUTED to the right gate;
//!   * the POSITIVE half end-to-end (synthetic live-endpoint merge) incl.
//!     the `node_merges.judgement_id` back-link, and its crash-window
//!     repair on a re-run;
//!   * a re-run writes nothing at all;
//!   * a dry run writes nothing AND its counters equal the subsequent
//!     execute run's, field for field;
//!   * an expired-but-unannulled UNSURE row is SKIPPED and left completely
//!     intact — never annulled, never superseded — and counted as such;
//!   * the RLS-bypass preflight REFUSES a role that cannot bypass (its own
//!     negative control — otherwise the preflight could be vacuous);
//!   * the compiled binary: `--control-only` dry run writes nothing,
//!     `--execute` writes, and `--execute --dry-run` is a usage error.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://…/judgement_graph_ci \
//!     cargo test -p coralswarm-backend --test judgement_backfill_pg -- --test-threads=1

#[cfg(not(feature = "binaries"))]
compile_error!("This suite runs utilities. Enable defaults or --features binaries.");

use coralswarm_backend::judgement_backfill::{
    assert_policy_bypass, backfill_database, dismissal_evidence, merge_evidence, ordered_pair,
    BackfillError, BackfillOpts,
};
use coralswarm_backend::store::{
    JudgementSource, JudgementVerdict, NewJudgement, PostgresStore, Store,
};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// `None` (with a skip line) unless `RELINFER_TEST_DATABASE_URL` is set —
/// same convention as `judgement_graph_pg.rs`/`merge_unmerge_pg.rs`.
async fn pool_or_skip(test_name: &str) -> Option<PgPool> {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run the live \
                 judgement-backfill suite"
            );
            return None;
        }
    };
    Some(
        PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .expect("connect to test Postgres"),
    )
}

async fn seed_graph(pool: &PgPool, label: &str) -> (Uuid, Uuid, Uuid) {
    let email = format!("{}-{}@test.local", label, Uuid::new_v4());
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
    let graph_id = store
        .create_graph(account_id, label, "personal")
        .await
        .expect("create graph")
        .id;
    (user_id, account_id, graph_id)
}

async fn cleanup(pool: &PgPool, user_id: Uuid, account_id: Uuid) {
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
}

/// Runs `body` (spawned as its own task), then UNCONDITIONALLY cleans up,
/// then re-panics with `body`'s original payload — the shared-database
/// teardown discipline `judgement_graph_pg.rs` established.
async fn with_cleanup<Fut>(pool: PgPool, accounts: Vec<(Uuid, Uuid)>, body: Fut)
where
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let result = tokio::spawn(body).await;
    for (user_id, account_id) in accounts {
        cleanup(&pool, user_id, account_id).await;
    }
    if let Err(e) = result {
        std::panic::resume_unwind(e.into_panic());
    }
}

/// Two real nodes in canonical `(lo, hi)` order.
async fn seed_pair(store: &PostgresStore, graph_id: Uuid, label: &str) -> (Uuid, Uuid) {
    let n1 = store
        .upsert_node(graph_id, "entity", &format!("{label}-1"))
        .await
        .expect("seed node 1");
    let n2 = store
        .upsert_node(graph_id, "entity", &format!("{label}-2"))
        .await
        .expect("seed node 2");
    ordered_pair(n1, n2)
}

/// Insert a `node_merges` row DIRECTLY.
///
/// **This is a synthetic shape and that is the point.** A real merge
/// hard-deletes `dup_id` (`store::merge_nodes`), so a genuine standing merge
/// can NEVER have two live endpoints — which is precisely why the POSITIVE
/// half of this backfill is vacuous on real data. Constructing the row by
/// hand is the only way to exercise gates 3-6 of that half at all; every
/// test that does so is testing the CODE PATH, not claiming the shape occurs
/// in production. The dead-endpoint case ([`dead_endpoint_candidates_are_
/// skipped`]) reproduces the real shape instead, by deleting the dup.
async fn insert_merge(
    pool: &PgPool,
    graph_id: Uuid,
    keep_id: Uuid,
    dup_id: Uuid,
    undone: bool,
    judgement_id: Option<Uuid>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO node_merges (graph_id, keep_id, dup_id, dup_snapshot, undone_at, judgement_id) \
         VALUES ($1, $2, $3, '{}'::jsonb, CASE WHEN $4 THEN now() ELSE NULL END, $5) \
         RETURNING merge_id",
    )
    .bind(graph_id)
    .bind(keep_id)
    .bind(dup_id)
    .bind(undone)
    .bind(judgement_id)
    .fetch_one(pool)
    .await
    .expect("insert node_merges")
}

/// Every `reef_judgement` row for a graph, newest first — including annulled
/// ones, because "what happened to the row that was already there" is what
/// most of these tests are actually about.
async fn judgements(pool: &PgPool, graph_id: Uuid) -> Vec<sqlx::postgres::PgRow> {
    sqlx::query(
        "SELECT judgement_id, node_a_id, node_b_id, verdict, source, confidence, model_id, \
                prompt_version, evidence, created_by, expires_at, annulled_at, annulled_by_kind, \
                annul_reason, superseded_by \
         FROM reef_judgement WHERE graph_id = $1 ORDER BY created_at DESC, judgement_id DESC",
    )
    .bind(graph_id)
    .fetch_all(pool)
    .await
    .expect("read reef_judgement")
}

async fn judgement_count(pool: &PgPool, graph_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM reef_judgement WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(pool)
        .await
        .expect("count reef_judgement")
}

async fn merge_backlink(pool: &PgPool, merge_id: Uuid) -> Option<Uuid> {
    sqlx::query_scalar("SELECT judgement_id FROM node_merges WHERE merge_id = $1")
        .bind(merge_id)
        .fetch_one(pool)
        .await
        .expect("read node_merges.judgement_id")
}

fn dry(graph_id: Uuid) -> BackfillOpts {
    BackfillOpts {
        execute: false,
        only_graph: Some(graph_id),
    }
}
fn exec(graph_id: Uuid) -> BackfillOpts {
    BackfillOpts {
        execute: true,
        only_graph: Some(graph_id),
    }
}

/// Write a live judgement of some other source, so a test can prove the
/// backfill leaves it alone.
#[allow(clippy::too_many_arguments)]
async fn write_foreign(
    store: &PostgresStore,
    graph_id: Uuid,
    a: Uuid,
    b: Uuid,
    verdict: JudgementVerdict,
    source: JudgementSource,
    created_by: Option<Uuid>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Uuid {
    store
        .write_judgement(NewJudgement {
            graph_id,
            node_a_id: a,
            node_b_id: b,
            verdict,
            source,
            confidence: Some(0.83),
            model_id: Some("claude-haiku-4-5".to_string()),
            prompt_version: Some("resolve_entities_v1".to_string()),
            evidence: serde_json::json!({"llm_verdict": "same", "reason": "pre-existing"}),
            created_by,
            expires_at,
        })
        .await
        .expect("seed a pre-existing judgement")
}

// ── 1. The payload half: not_same -> NEGATIVE ───────────────────────────────

#[tokio::test]
async fn not_same_pair_becomes_a_backfilled_negative() {
    let Some(pool) = pool_or_skip("not_same_pair_becomes_a_backfilled_negative").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-negative").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-neg").await;
        store
            .insert_merge_dismissal(graph_id, hi, lo) // deliberately reversed
            .await
            .expect("dismiss");

        let c = backfill_database(&body_pool, &exec(graph_id))
            .await
            .expect("backfill");
        assert_eq!(c.dismissals_scanned, 1);
        assert_eq!(c.negatives_written, 1);
        assert_eq!(c.merges_scanned, 0, "no node_merges rows exist here");
        assert_eq!(c.errors, 0);
        assert_eq!(c.skipped_expired_unsure, 0);
        assert_eq!(c.dismissals_accounted(), c.dismissals_scanned);

        let rows = judgements(&body_pool, graph_id).await;
        assert_eq!(rows.len(), 1, "exactly one judgement row");
        let r = &rows[0];
        assert_eq!(r.get::<String, _>("verdict"), "negative");
        assert_eq!(r.get::<String, _>("source"), "backfill");
        // The pair is CANONICAL even though the dismissal was passed
        // reversed — gate 1.
        assert_eq!(r.get::<Uuid, _>("node_a_id"), lo);
        assert_eq!(r.get::<Uuid, _>("node_b_id"), hi);
        // Evidence is byte-identical to migration 0139's documented shape;
        // drift here silently turns every re-run into a re-adjudication.
        // Asserted against the LITERAL, not only against
        // `dismissal_evidence()` — comparing the helper to itself would pass
        // even if the shape drifted (confirmed: that exact mutation control
        // did NOT fail until this literal was added).
        assert_eq!(
            r.get::<Value, _>("evidence"),
            serde_json::json!({"backfilled_from": "not_same"})
        );
        assert_eq!(r.get::<Value, _>("evidence"), dismissal_evidence());
        // A dismissal records no actor and no date — see the module doc.
        assert!(r.get::<Option<Uuid>, _>("created_by").is_none());
        assert!(r
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("expires_at")
            .is_none());
        assert!(r.get::<Option<f64>, _>("confidence").is_none());
        assert!(r.get::<Option<String>, _>("model_id").is_none());
        assert!(r.get::<Option<String>, _>("prompt_version").is_none());
        assert!(r
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("annulled_at")
            .is_none());
    })
    .await;
}

// ── 2. THE GATE-4 CLOBBER CONTROL ───────────────────────────────────────────

/// **Mutation-control target.** Deleting gate 4 (the `pair_state` pre-check)
/// from `backfill_merges_for_graph`/`backfill_dismissals_for_graph` makes
/// `write_judgement` annul-and-supersede the `llm` rows below and refuse the
/// `human` one — so the row count, the `annulled_at` assertions and the
/// counter assertions all fail. That is the demonstration that this gate,
/// not `write_judgement`, is what protects a live verdict from a legacy one.
#[tokio::test]
async fn live_judgements_are_never_clobbered() {
    let Some(pool) = pool_or_skip("live_judgements_are_never_clobbered").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-noclobber").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());

        // (a) a live llm POSITIVE — "merge pending materialization".
        let (p_lo, p_hi) = seed_pair(&store, graph_id, "bf-nc-pos").await;
        let pos_id = write_foreign(
            &store,
            graph_id,
            p_lo,
            p_hi,
            JudgementVerdict::Positive,
            JudgementSource::Llm,
            None,
            None,
        )
        .await;
        // (b) a live llm UNSURE — literally a review-queue card.
        let (u_lo, u_hi) = seed_pair(&store, graph_id, "bf-nc-uns").await;
        let uns_id = write_foreign(
            &store,
            graph_id,
            u_lo,
            u_hi,
            JudgementVerdict::Unsure,
            JudgementSource::Llm,
            None,
            Some(chrono::Utc::now() + chrono::Duration::days(30)),
        )
        .await;
        // (c) a live HUMAN NEGATIVE.
        let (h_lo, h_hi) = seed_pair(&store, graph_id, "bf-nc-hum").await;
        let hum_id = write_foreign(
            &store,
            graph_id,
            h_lo,
            h_hi,
            JudgementVerdict::Negative,
            JudgementSource::Human,
            Some(user_id),
            None,
        )
        .await;

        for (a, b) in [(p_lo, p_hi), (u_lo, u_hi), (h_lo, h_hi)] {
            store.insert_merge_dismissal(graph_id, a, b).await.expect("dismiss");
        }

        let c = backfill_database(&body_pool, &exec(graph_id))
            .await
            .expect("backfill");
        assert_eq!(c.dismissals_scanned, 3);
        assert_eq!(
            c.dismissals_skipped_live_judgement, 3,
            "all three live rows must be skipped at gate 4"
        );
        assert_eq!(c.negatives_written, 0, "nothing may be written over a live verdict");
        assert_eq!(c.raced_human_judgement, 0, "gate 4 must skip BEFORE write_judgement refuses");
        assert_eq!(c.errors, 0);

        let rows = judgements(&body_pool, graph_id).await;
        assert_eq!(rows.len(), 3, "no new rows, no superseding rows");
        for r in &rows {
            let id = r.get::<Uuid, _>("judgement_id");
            assert!(
                [pos_id, uns_id, hum_id].contains(&id),
                "an unexpected judgement row was written: {id}"
            );
            assert!(
                r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("annulled_at").is_none(),
                "row {id} was annulled — the backfill clobbered a live verdict"
            );
            assert!(
                r.get::<Option<Uuid>, _>("superseded_by").is_none(),
                "row {id} was superseded — the backfill clobbered a live verdict"
            );
            assert_ne!(r.get::<String, _>("source"), "backfill");
        }
    })
    .await;
}

// ── 3-5. The merge half's skip gates ────────────────────────────────────────

#[tokio::test]
async fn undone_merge_is_skipped() {
    let Some(pool) = pool_or_skip("undone_merge_is_skipped").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-undone").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        // An undone merge has BOTH endpoints live again (undo recreates the
        // dup), so gate 3 would pass — gate 2 is the only thing standing
        // between it and an active POSITIVE the ER worker would re-merge.
        let (lo, hi) = seed_pair(&store, graph_id, "bf-undone").await;
        insert_merge(&body_pool, graph_id, lo, hi, true, None).await;

        let c = backfill_database(&body_pool, &exec(graph_id))
            .await
            .expect("backfill");
        assert_eq!(c.merges_scanned, 1);
        assert_eq!(c.merges_skipped_undone, 1);
        assert_eq!(c.positives_written, 0);
        assert_eq!(c.merges_accounted(), c.merges_scanned);
        assert_eq!(judgement_count(&body_pool, graph_id).await, 0);
    })
    .await;
}

#[tokio::test]
async fn post_flag_merge_is_skipped() {
    let Some(pool) = pool_or_skip("post_flag_merge_is_skipped").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-postflag").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-postflag").await;
        // A merge PR2 already materialized carries its judgement back-link.
        let judgement_id = write_foreign(
            &store,
            graph_id,
            lo,
            hi,
            JudgementVerdict::Positive,
            JudgementSource::Llm,
            None,
            None,
        )
        .await;
        insert_merge(&body_pool, graph_id, lo, hi, false, Some(judgement_id)).await;

        let c = backfill_database(&body_pool, &exec(graph_id))
            .await
            .expect("backfill");
        assert_eq!(c.merges_scanned, 1);
        assert_eq!(c.merges_skipped_already_judged, 1);
        assert_eq!(c.positives_written, 0);
        assert_eq!(c.merges_accounted(), c.merges_scanned);

        let rows = judgements(&body_pool, graph_id).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<Uuid, _>("judgement_id"), judgement_id);
        assert_eq!(rows[0].get::<String, _>("source"), "llm");
    })
    .await;
}

#[tokio::test]
async fn dead_endpoint_candidates_are_skipped() {
    let Some(pool) = pool_or_skip("dead_endpoint_candidates_are_skipped").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-dead").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());

        // (a) THE REAL SHAPE of a standing merge: the dup reef is gone.
        let (m_lo, m_hi) = seed_pair(&store, graph_id, "bf-dead-merge").await;
        insert_merge(&body_pool, graph_id, m_lo, m_hi, false, None).await;
        sqlx::query("DELETE FROM nodes WHERE node_id = $1")
            .bind(m_hi)
            .execute(&body_pool)
            .await
            .expect("delete the merged-away dup");

        // (b) a dismissal whose HIGHER node was later deleted. (A dismissal
        // on a deleted LOWER node is invisible — the array died with the
        // row; see the module doc's loss-asymmetry note.)
        let (d_lo, d_hi) = seed_pair(&store, graph_id, "bf-dead-dismiss").await;
        store.insert_merge_dismissal(graph_id, d_lo, d_hi).await.expect("dismiss");
        sqlx::query("DELETE FROM nodes WHERE node_id = $1")
            .bind(d_hi)
            .execute(&body_pool)
            .await
            .expect("delete the dismissal partner");

        let c = backfill_database(&body_pool, &exec(graph_id))
            .await
            .expect("backfill");
        assert_eq!(c.merges_scanned, 1);
        assert_eq!(c.merges_skipped_dead_endpoint, 1);
        assert_eq!(c.dismissals_scanned, 1);
        assert_eq!(c.dismissals_skipped_dead_endpoint, 1);
        assert_eq!(c.writes(), 0);
        assert_eq!(c.errors, 0, "a dead endpoint is an OUTCOME, never an error");
        assert_eq!(judgement_count(&body_pool, graph_id).await, 0);
    })
    .await;
}

// ── 6. The merge half end-to-end (synthetic — see `insert_merge`'s doc) ─────

#[tokio::test]
async fn live_endpoint_merge_writes_positive_and_backlink() {
    let Some(pool) = pool_or_skip("live_endpoint_merge_writes_positive_and_backlink").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-positive").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-pos").await;
        // keep/dup passed in NON-canonical order on purpose (gate 1).
        let merge_id = insert_merge(&body_pool, graph_id, hi, lo, false, None).await;

        let c = backfill_database(&body_pool, &exec(graph_id))
            .await
            .expect("backfill");
        assert_eq!(c.merges_scanned, 1);
        assert_eq!(c.positives_written, 1);
        assert_eq!(c.backlinks_written, 1);
        assert_eq!(c.backlinks_repaired, 0);
        assert_eq!(c.errors, 0);
        assert_eq!(c.merges_accounted(), c.merges_scanned);

        let rows = judgements(&body_pool, graph_id).await;
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.get::<String, _>("verdict"), "positive");
        assert_eq!(r.get::<String, _>("source"), "backfill");
        assert_eq!(r.get::<Uuid, _>("node_a_id"), lo);
        assert_eq!(r.get::<Uuid, _>("node_b_id"), hi);
        // The LITERAL shape from migration 0139's header (see the note in
        // `not_same_pair_becomes_a_backfilled_negative` for why the helper
        // alone is not enough), then the helper as well.
        assert_eq!(
            r.get::<Value, _>("evidence"),
            serde_json::json!({"backfilled_from": "node_merges", "merge_id": merge_id.to_string()})
        );
        assert_eq!(r.get::<Value, _>("evidence"), merge_evidence(merge_id));
        assert!(r.get::<Option<Uuid>, _>("created_by").is_none());

        // Gate 6: the back-link is what keeps `unmerge_nodes`' annul-linkage
        // (PR3 F1) working for a backfilled merge.
        assert_eq!(
            merge_backlink(&body_pool, merge_id).await,
            Some(r.get::<Uuid, _>("judgement_id"))
        );
    })
    .await;
}

// ── 7-8. Idempotency, and the crash-window repair ───────────────────────────

#[tokio::test]
async fn rerun_writes_nothing() {
    let Some(pool) = pool_or_skip("rerun_writes_nothing").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-rerun").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (d_lo, d_hi) = seed_pair(&store, graph_id, "bf-rerun-dismiss").await;
        store.insert_merge_dismissal(graph_id, d_lo, d_hi).await.expect("dismiss");
        let (m_lo, m_hi) = seed_pair(&store, graph_id, "bf-rerun-merge").await;
        let merge_id = insert_merge(&body_pool, graph_id, m_lo, m_hi, false, None).await;

        let first = backfill_database(&body_pool, &exec(graph_id)).await.expect("run 1");
        assert_eq!(first.writes(), 2);
        let after_first = judgement_count(&body_pool, graph_id).await;
        assert_eq!(after_first, 2);
        let link = merge_backlink(&body_pool, merge_id).await;
        assert!(link.is_some());

        let second = backfill_database(&body_pool, &exec(graph_id)).await.expect("run 2");
        assert_eq!(second.writes(), 0, "a re-run must write nothing");
        assert_eq!(second.dismissals_already_backfilled, 1);
        // The merge's back-link is set, so gate 2 catches it first — see the
        // module doc's note on which counter a clean re-run lands in.
        assert_eq!(second.merges_skipped_already_judged, 1);
        assert_eq!(second.merges_already_backfilled, 0);
        assert_eq!(second.errors, 0);

        assert_eq!(judgement_count(&body_pool, graph_id).await, after_first);
        assert_eq!(merge_backlink(&body_pool, merge_id).await, link);
        for r in judgements(&body_pool, graph_id).await {
            assert!(
                r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("annulled_at").is_none(),
                "a re-run annulled its own row — evidence/verdict drift, not idempotency"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn missing_backlink_is_repaired_on_rerun() {
    let Some(pool) = pool_or_skip("missing_backlink_is_repaired_on_rerun").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-repair").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-repair").await;
        let merge_id = insert_merge(&body_pool, graph_id, lo, hi, false, None).await;
        backfill_database(&body_pool, &exec(graph_id)).await.expect("run 1");
        let judgement_id = merge_backlink(&body_pool, merge_id).await.expect("back-link");

        // Simulate a process killed between gate 5 and gate 6.
        sqlx::query("UPDATE node_merges SET judgement_id = NULL WHERE merge_id = $1")
            .bind(merge_id)
            .execute(&body_pool)
            .await
            .expect("clear the back-link");

        let c = backfill_database(&body_pool, &exec(graph_id)).await.expect("run 2");
        assert_eq!(c.merges_already_backfilled, 1);
        assert_eq!(c.backlinks_repaired, 1);
        assert_eq!(c.positives_written, 0, "the repair must not write a second judgement");
        assert_eq!(c.errors, 0);
        assert_eq!(judgement_count(&body_pool, graph_id).await, 1);
        assert_eq!(
            merge_backlink(&body_pool, merge_id).await,
            Some(judgement_id),
            "the back-link must be restored to the SAME judgement"
        );
    })
    .await;
}

/// A DIFFERENT merge over the same pair must never adopt the first merge's
/// judgement as its back-link — that is what matching on the row's own
/// `evidence->>'merge_id'` buys, and removing it makes this fail.
#[tokio::test]
async fn a_second_merge_over_the_same_pair_does_not_adopt_the_first_judgement() {
    let Some(pool) =
        pool_or_skip("a_second_merge_over_the_same_pair_does_not_adopt_the_first_judgement").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-twomerges").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-two").await;
        let first = insert_merge(&body_pool, graph_id, lo, hi, false, None).await;
        backfill_database(&body_pool, &exec(graph_id)).await.expect("run 1");
        assert!(merge_backlink(&body_pool, first).await.is_some());

        // A second, distinct merge row over the same pair.
        let second = insert_merge(&body_pool, graph_id, lo, hi, false, None).await;
        let c = backfill_database(&body_pool, &exec(graph_id)).await.expect("run 2");
        assert_eq!(c.merges_scanned, 2);
        assert_eq!(c.merges_skipped_already_judged, 1, "the first merge is linked");
        assert_eq!(
            c.merges_skipped_live_judgement, 1,
            "the second merge sees a live POSITIVE that is NOT its own"
        );
        assert_eq!(c.merges_already_backfilled, 0);
        assert_eq!(c.backlinks_repaired, 0);
        assert_eq!(c.positives_written, 0);
        assert_eq!(
            merge_backlink(&body_pool, second).await,
            None,
            "the second merge must NOT adopt the first merge's judgement"
        );
        assert_eq!(judgement_count(&body_pool, graph_id).await, 1);
    })
    .await;
}

/// Two standing merges over the SAME pair inside ONE run. This should not
/// occur (the first merge deletes the dup), but it is the one shape where a
/// dry run has to simulate its own earlier decision — and where getting that
/// simulation wrong would make the rehearsal disagree with the real run in a
/// way no other test would catch.
#[tokio::test]
async fn dry_run_matches_execute_for_two_merges_over_one_pair() {
    let Some(pool) = pool_or_skip("dry_run_matches_execute_for_two_merges_over_one_pair").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-dupmerge").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-dup").await;
        insert_merge(&body_pool, graph_id, lo, hi, false, None).await;
        insert_merge(&body_pool, graph_id, lo, hi, false, None).await;

        let forecast = backfill_database(&body_pool, &dry(graph_id)).await.expect("dry run");
        assert_eq!(judgement_count(&body_pool, graph_id).await, 0);
        let real = backfill_database(&body_pool, &exec(graph_id)).await.expect("execute");
        assert_eq!(
            forecast, real,
            "the dry run's simulation of its own earlier write diverged from the execute run"
        );
        assert_eq!(real.merges_scanned, 2);
        assert_eq!(real.positives_written, 1);
        assert_eq!(real.merges_skipped_live_judgement, 1);
        assert_eq!(judgement_count(&body_pool, graph_id).await, 1);
    })
    .await;
}

// ── 9. Dry run ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn dry_run_writes_nothing_and_predicts_the_execute_run() {
    let Some(pool) = pool_or_skip("dry_run_writes_nothing_and_predicts_the_execute_run").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-dryrun").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        // One of each interesting shape, so the prediction has something to
        // be wrong about.
        let (d_lo, d_hi) = seed_pair(&store, graph_id, "bf-dry-dismiss").await;
        store.insert_merge_dismissal(graph_id, d_lo, d_hi).await.expect("dismiss");
        let (m_lo, m_hi) = seed_pair(&store, graph_id, "bf-dry-merge").await;
        let merge_id = insert_merge(&body_pool, graph_id, m_lo, m_hi, false, None).await;
        let (u_lo, u_hi) = seed_pair(&store, graph_id, "bf-dry-undone").await;
        insert_merge(&body_pool, graph_id, u_lo, u_hi, true, None).await;
        let (l_lo, l_hi) = seed_pair(&store, graph_id, "bf-dry-live").await;
        write_foreign(
            &store,
            graph_id,
            l_lo,
            l_hi,
            JudgementVerdict::Positive,
            JudgementSource::Llm,
            None,
            None,
        )
        .await;
        store.insert_merge_dismissal(graph_id, l_lo, l_hi).await.expect("dismiss");

        let before = judgement_count(&body_pool, graph_id).await;
        let dry_counts = backfill_database(&body_pool, &dry(graph_id)).await.expect("dry run");

        // Nothing at all changed.
        assert_eq!(
            judgement_count(&body_pool, graph_id).await,
            before,
            "a dry run wrote a reef_judgement row"
        );
        assert_eq!(
            merge_backlink(&body_pool, merge_id).await,
            None,
            "a dry run wrote a node_merges back-link"
        );
        assert!(dry_counts.writes() > 0, "this fixture must have something to write");

        // …and the forecast is exact, field for field.
        let real_counts = backfill_database(&body_pool, &exec(graph_id)).await.expect("execute");
        assert_eq!(
            dry_counts, real_counts,
            "the dry run's tally must equal what the execute run actually did"
        );
        assert_eq!(judgement_count(&body_pool, graph_id).await, before + 2);
        assert!(merge_backlink(&body_pool, merge_id).await.is_some());
    })
    .await;
}

// ── 10. Expired UNSURE rows are skipped, never superseded ───────────────────

/// The pair is dismissed AND holds an expired, un-annulled UNSURE row — the
/// one shape where this backfill could mutate a pre-existing row. It must not:
/// `not_same` cannot tell a human's ruling from a machine's auto-dismissal, an
/// expired UNSURE is the evidence that a high-impact question went unanswered,
/// and `gate_pair` suppresses the pair on the dismissal alone regardless. See
/// the `judgement_backfill` module doc.
#[tokio::test]
async fn expired_unsure_is_skipped_and_left_intact() {
    let Some(pool) = pool_or_skip("expired_unsure_is_skipped_and_left_intact").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-expired").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-expired").await;
        let stale = write_foreign(
            &store,
            graph_id,
            lo,
            hi,
            JudgementVerdict::Unsure,
            JudgementSource::Llm,
            None,
            Some(chrono::Utc::now() - chrono::Duration::days(1)),
        )
        .await;
        store.insert_merge_dismissal(graph_id, lo, hi).await.expect("dismiss");

        // The dry run must PREDICT the skip, not discover it.
        let forecast = backfill_database(&body_pool, &dry(graph_id)).await.expect("dry run");
        assert_eq!(forecast.skipped_expired_unsure, 1);
        assert_eq!(forecast.negatives_written, 0, "an occupied slot is never written");

        let c = backfill_database(&body_pool, &exec(graph_id)).await.expect("backfill");
        assert_eq!(c, forecast, "dry run and execute must agree");
        assert_eq!(
            c.dismissals_skipped_live_judgement, 0,
            "an expired row is not LIVE — it must land in the expired bucket, not this one"
        );

        // The expired row is untouched: still un-annulled, still the only row.
        // Superseding it would have annulled it 'expired' and inserted a
        // second, backfilled NEGATIVE.
        let rows = judgements(&body_pool, graph_id).await;
        assert_eq!(rows.len(), 1, "no backfilled NEGATIVE was inserted");
        let only = &rows[0];
        assert_eq!(only.get::<Uuid, _>("judgement_id"), stale);
        assert_eq!(only.get::<String, _>("verdict"), "unsure");
        assert_eq!(only.get::<String, _>("source"), "llm");
        assert!(
            only.get::<Option<chrono::DateTime<chrono::Utc>>, _>("annulled_at")
                .is_none(),
            "the expired UNSURE must remain un-annulled"
        );
        assert!(only.get::<Option<Uuid>, _>("superseded_by").is_none());
    })
    .await;
}

// ── 11. Multi-ocean walk ────────────────────────────────────────────────────

#[tokio::test]
async fn whole_database_run_covers_every_ocean() {
    let Some(pool) = pool_or_skip("whole_database_run_covers_every_ocean").await else {
        return;
    };
    let (u1, a1, g1) = seed_graph(&pool, "bf-multi-1").await;
    let (u2, a2, g2) = seed_graph(&pool, "bf-multi-2").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(u1, a1), (u2, a2)], async move {
        let store = PostgresStore::new(body_pool.clone());
        for g in [g1, g2] {
            let (lo, hi) = seed_pair(&store, g, "bf-multi").await;
            store.insert_merge_dismissal(g, lo, hi).await.expect("dismiss");
        }

        // No `only_graph`: the real invocation shape. Assertions stay LOCAL
        // to these two oceans — this database is shared with other suites.
        let c = backfill_database(
            &body_pool,
            &BackfillOpts {
                execute: true,
                only_graph: None,
            },
        )
        .await
        .expect("backfill");
        assert!(c.graphs_scanned >= 2);
        assert_eq!(c.errors, 0);

        for g in [g1, g2] {
            let rows = judgements(&body_pool, g).await;
            assert_eq!(rows.len(), 1, "each ocean gets its own NEGATIVE");
            assert_eq!(rows[0].get::<String, _>("source"), "backfill");
            assert_eq!(rows[0].get::<String, _>("verdict"), "negative");
        }
    })
    .await;
}

// ── 12. The RLS-bypass preflight, and its own negative control ──────────────

/// The preflight must PASS for the elevated role every other test here uses…
#[tokio::test]
async fn preflight_passes_for_the_elevated_role() {
    let Some(pool) = pool_or_skip("preflight_passes_for_the_elevated_role").await else {
        return;
    };
    assert_policy_bypass(&pool)
        .await
        .expect("the test DSN's role must be able to write reef_judgement unscoped");
}

/// …and REFUSE for one that cannot bypass RLS. Without this control the
/// preflight could be a function that always returns `Ok` and no test would
/// notice. Uses a real, throwaway `NOBYPASSRLS` role and `SET ROLE` on every
/// pooled connection (`after_connect`) rather than a single ad-hoc `SET`,
/// because a pool hands out whichever connection it likes.
#[tokio::test]
async fn preflight_refuses_a_role_that_cannot_bypass_rls() {
    let Some(pool) = pool_or_skip("preflight_refuses_a_role_that_cannot_bypass_rls").await else {
        return;
    };
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pool_or_skip");
    // Roles are CLUSTER-global: unique per run, and dropped afterwards.
    let role = coralswarm_backend::test_db::TestRole::from_url(&url, "bf_norls");
    sqlx::query(&format!(
        "CREATE ROLE \"{role}\" NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE"
    ))
    .execute(&pool)
    .await
    .expect("create the throwaway role (needs a superuser/CREATEROLE test DSN)");

    let role_for_connect = role.to_string();
    let restricted = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |conn, _meta| {
            let role = role_for_connect.clone();
            Box::pin(async move {
                sqlx::query(&format!("SET ROLE \"{role}\""))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("connect as the restricted role");

    let outcome = assert_policy_bypass(&restricted).await;
    restricted.close().await;
    sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
        .execute(&pool)
        .await
        .expect("drop the throwaway role");

    match outcome {
        Err(BackfillError::Preflight(msg)) => {
            assert!(
                msg.contains("does not bypass row-level security"),
                "unexpected refusal message: {msg}"
            );
        }
        other => panic!(
            "the preflight accepted a role that cannot bypass RLS — it is vacuous: {other:?}"
        ),
    }
}

// ── 13. Canonical-ordering agreement with the Store ─────────────────────────

/// `judgement_backfill::ordered_pair` is a deliberate local copy of the
/// `store`-private `PostgresStore::ordered_pair`. This is what pins them
/// together: it drives a REAL `write_judgement` with the pair reversed and
/// asserts the database row agrees with this module's own canonicalization.
#[tokio::test]
async fn ordered_pair_matches_store_convention() {
    let Some(pool) = pool_or_skip("ordered_pair_matches_store_convention").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-ordering").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-order").await;
        assert_eq!(ordered_pair(hi, lo), (lo, hi));
        write_foreign(
            &store,
            graph_id,
            hi,
            lo,
            JudgementVerdict::Negative,
            JudgementSource::Llm,
            None,
            None,
        )
        .await;
        let rows = judgements(&body_pool, graph_id).await;
        assert_eq!(rows[0].get::<Uuid, _>("node_a_id"), lo);
        assert_eq!(rows[0].get::<Uuid, _>("node_b_id"), hi);
    })
    .await;
}

// ── 14. The compiled binary, end to end ─────────────────────────────────────

/// The `--control-only` DRY RUN over the WHOLE database (no `--only-graph`),
/// through the real compiled binary — argument parsing, DSN resolution,
/// preflight, report and greppable summary included. This is the shape an
/// operator runs first, and it must touch nothing.
#[tokio::test]
async fn bin_control_only_dry_run_writes_nothing() {
    let Some(pool) = pool_or_skip("bin_control_only_dry_run_writes_nothing").await else {
        return;
    };
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pool_or_skip");
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-bin-dry").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-bin-dry").await;
        store.insert_merge_dismissal(graph_id, lo, hi).await.expect("dismiss");

        let out = std::process::Command::new(env!(
            "CARGO_BIN_EXE_backfill_reef_judgement_from_history"
        ))
        .arg("--control-only")
        .env("MIGRATOR_DATABASE_URL", &url)
        .env_remove("DATABASE_URL")
        .output()
        .expect("run the backfill binary");
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        assert!(out.status.success(), "binary failed:\n{stdout}\n{stderr}");
        assert!(
            stdout.contains("ONE-SHOT, RUN ONCE PER DATABASE (mode=dry-run)"),
            "missing the one-shot banner:\n{stdout}"
        );
        assert!(
            stdout.contains("DRY RUN -- nothing was written"),
            "missing the dry-run disclaimer:\n{stdout}"
        );
        assert!(
            stderr.contains("summary: backfill_reef_judgement db=control mode=dry-run"),
            "missing the greppable per-database summary:\n{stderr}"
        );
        assert!(
            stderr.contains("summary: backfill_reef_judgement db=TOTAL mode=dry-run"),
            "missing the greppable grand total:\n{stderr}"
        );
        assert!(stderr.contains("errors=0"), "summary reported errors:\n{stderr}");

        assert_eq!(
            judgement_count(&body_pool, graph_id).await,
            0,
            "the binary's dry run wrote a judgement"
        );
    })
    .await;
}

/// The same binary with `--execute` actually writes — the end-to-end proof
/// that the wrapper is wired to the core it documents.
#[tokio::test]
async fn bin_execute_writes_the_backfilled_negative() {
    let Some(pool) = pool_or_skip("bin_execute_writes_the_backfilled_negative").await else {
        return;
    };
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pool_or_skip");
    let (user_id, account_id, graph_id) = seed_graph(&pool, "bf-bin-exec").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (lo, hi) = seed_pair(&store, graph_id, "bf-bin-exec").await;
        store.insert_merge_dismissal(graph_id, lo, hi).await.expect("dismiss");

        let out = std::process::Command::new(env!(
            "CARGO_BIN_EXE_backfill_reef_judgement_from_history"
        ))
        .args(["--execute", "--control-only", "--only-graph"])
        .arg(graph_id.to_string())
        .env("MIGRATOR_DATABASE_URL", &url)
        .env_remove("DATABASE_URL")
        .output()
        .expect("run the backfill binary");
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        assert!(out.status.success(), "binary failed:\n{stdout}\n{stderr}");
        assert!(stderr.contains("mode=execute"), "not an execute run:\n{stderr}");
        assert!(stderr.contains("negatives_written=1"), "nothing written:\n{stderr}");
        assert!(!stdout.contains("DRY RUN"), "execute run claimed to be a dry run:\n{stdout}");

        let rows = judgements(&body_pool, graph_id).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<String, _>("source"), "backfill");
        assert_eq!(rows[0].get::<String, _>("verdict"), "negative");
        assert_eq!(
            rows[0].get::<Value, _>("evidence"),
            serde_json::json!({"backfilled_from": "not_same"})
        );
        assert_eq!(rows[0].get::<Value, _>("evidence"), dismissal_evidence());
    })
    .await;
}

/// `--execute --dry-run` is refused rather than resolved by precedence, and
/// the refusal happens before any database is touched (exit 2, the usage
/// code — never 0, never a partial run).
#[tokio::test]
async fn bin_refuses_contradictory_mode_flags() {
    let Some(pool) = pool_or_skip("bin_refuses_contradictory_mode_flags").await else {
        return;
    };
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pool_or_skip");
    drop(pool);
    let out = std::process::Command::new(env!(
        "CARGO_BIN_EXE_backfill_reef_judgement_from_history"
    ))
    .args(["--execute", "--dry-run"])
    .env("MIGRATOR_DATABASE_URL", &url)
    .env_remove("DATABASE_URL")
    .output()
    .expect("run the backfill binary");
    assert_eq!(out.status.code(), Some(2), "expected the usage exit code");
    assert!(String::from_utf8_lossy(&out.stderr).contains("mutually exclusive"));
}
