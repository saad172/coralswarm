//! Live test for the judgement-graph resolver slice, PR1 (schema + `Store`
//! plumbing for `reef_judgement`, migrations 0139/0046 — design doc §2/§5.1).
//!
//! THIS PR IS DELIBERATELY INERT: nothing in `workers`/`routes` calls
//! `write_judgement`/`annul_judgement` yet, so there is no end-to-end
//! behavior to exercise — this suite proves the TABLE and the `Store`
//! methods themselves are correct in isolation, modeled on
//! `backend/tests/merge_unmerge_pg.rs`'s shape (a real `PostgresStore`
//! against an already-migrated Postgres, gated on `RELINFER_TEST_DATABASE_URL`,
//! no scratch-DB machinery).
//!
//! Covers (design doc §9 PR1 test list, plus review round 1 + CodeRabbit
//! round 2 fixes):
//!   * insert/read each verdict (positive/negative/unsure) round-trips
//!     through `write_judgement`/`get_active_judgement`.
//!   * NEGATIVE CONTROLS on every CHECK/unique constraint the migration
//!     ships: `reef_judgement_ordered_pair`, `reef_judgement_expiry_shape`,
//!     `reef_judgement_annul_kind_shape` (both directions), and
//!     `uq_reef_judgement_active_pair` (a second active insert for the same
//!     pair fails; annul-then-retry succeeds).
//!   * `write_judgement` re-adjudication (UNSURE -> NEGATIVE): exactly one
//!     active row survives, the old row is annulled with `superseded_by`
//!     pointing at the new one; a SEPARATE test proves the no-op path is
//!     narrow enough to still re-adjudicate when only metadata changes
//!     (review round 1 finding 1 — this is that finding's own
//!     revert-negative-control target).
//!   * two connections racing `write_judgement` on the identical pair:
//!     exactly one active row survives, the loser resolves via the
//!     documented no-op path (`Ok(existing_id)`), never an unhandled error.
//!   * `HumanJudgementExists`: an active human POSITIVE blocks a subsequent
//!     `llm`-sourced write for the same pair; nothing is written.
//!   * expiry live-ness (review round 1 finding 2 — this is that finding's
//!     own revert-negative-control target): an expired-but-unannulled row
//!     is invisible to both reads, and — the critical interaction —
//!     unconditionally supersedable by `write_judgement` even when its own
//!     `source` is `human`.
//!   * `annul_judgement`'s full contract (review round 1 finding 3):
//!     annuls + idempotent, reports `superseded_by` when a row was already
//!     annulled by a re-adjudication (not by this call), and a clean no-op
//!     for an unknown id.
//!   * each `_scoped` variant exercised at least once inside a manually
//!     managed transaction.
//!   * `write_judgement` endpoint validation (CodeRabbit round 2 Major): a
//!     cross-graph pair, and a pair naming a node that was never created,
//!     are both refused with nothing written.
//!   * RLS cross-account invisibility on the new `graph_id`-anchored
//!     `account_isolation` policy, with the same non-vacuous-denial negative
//!     control (`DISABLE`/`ENABLE ROW LEVEL SECURITY`) `consumer_rls_pg.rs`/
//!     `tenant_rls_pg.rs` use — see
//!     [`judgement_graph_rls_denies_cross_account_reads`]'s own doc for why
//!     this test switches role via `SET LOCAL ROLE` rather than a second
//!     real login.
//!
//! **Panic-safe teardown (review round 1 finding 5 / CodeRabbit's
//! overlapping note):** unlike `consumer_rls_pg.rs`/`tenant_rls_pg.rs`, each
//! of which gets a FRESH scratch database per test, this suite (mirroring
//! `merge_unmerge_pg.rs`) reuses ONE already-migrated database across every
//! test in a run. Every test therefore routes its body through
//! [`with_cleanup`], which spawns the body as its own task and
//! unconditionally runs `cleanup` afterward whether or not the body
//! panicked, then re-panics with the original payload so the test still
//! reports failed. The RLS test additionally wraps its
//! `DISABLE ROW LEVEL SECURITY` window the same way, so a panicking
//! assertion inside that window can never strand the shared table with RLS
//! disabled for every other test/run sharing this database.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test judgement_graph_pg -- --nocapture

use coralswarm_backend::store::{
    AnnulKind, AnnulOutcome, JudgementSource, JudgementVerdict, NewJudgement, PostgresStore,
    Store, StoreError,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// `None` (with a skip line) unless `RELINFER_TEST_DATABASE_URL` is set —
/// same convention as `merge_unmerge_pg.rs`.
async fn pool_or_skip(test_name: &str) -> Option<PgPool> {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run the live judgement-graph suite");
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

/// Seed a fresh `(user, account, graph)` chain and return the graph id —
/// same shape as `merge_unmerge_pg.rs`'s inline seeding, factored out since
/// every test in this file needs one.
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
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(pool).await.ok();
}

/// Runs `body` (spawned as its own task), then UNCONDITIONALLY runs
/// `cleanup` for every `(user_id, account_id)` pair in `accounts` afterward
/// — whether or not `body` panicked — then re-panics with `body`'s original
/// payload if it did, so the test still reports failed. See this file's own
/// module doc for why every test routes through this.
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

/// Postgres error helpers — match by SQLSTATE/constraint name, never by
/// message text (`slack_token_rls_pg.rs`/`provisioning_and_grants_rls_pg.rs`'s
/// own convention in this repo).
fn pg_code(e: &sqlx::Error) -> Option<String> {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string())
}
fn pg_constraint(e: &sqlx::Error) -> Option<String> {
    e.as_database_error().and_then(|d| d.constraint()).map(|c| c.to_string())
}

fn ordered_pair(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}

/// Create two real nodes in `graph_id` and return their ids in canonical
/// (ordered) `write_judgement` pair order. `write_judgement` validates
/// (CodeRabbit round 2 Major "endpoint validation") that both endpoints
/// currently resolve to live nodes in the judgement's own `graph_id`, so
/// every test that calls `write_judgement`/`write_judgement_scoped` needs
/// REAL nodes rather than bare `Uuid::new_v4()` pairs — unlike the raw-SQL
/// CHECK/uniqueness negative-control tests below, which never go through
/// `write_judgement` at all and so are unaffected.
async fn seed_pair(store: &PostgresStore, graph_id: Uuid, label: &str) -> (Uuid, Uuid) {
    let n1 = store.upsert_node(graph_id, "entity", &format!("{label}-1")).await.expect("seed node 1");
    let n2 = store.upsert_node(graph_id, "entity", &format!("{label}-2")).await.expect("seed node 2");
    ordered_pair(n1, n2)
}

// ── 1. Insert/read each verdict ─────────────────────────────────────────────

#[tokio::test]
async fn write_judgement_round_trips_each_verdict() {
    let Some(pool) = pool_or_skip("write_judgement_round_trips_each_verdict").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-verdicts").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());

        // Three DISTINCT pairs, one per verdict, so the active-pair uniqueness
        // constraint never interferes with this test's own writes.
        let (pos_a, pos_b) = seed_pair(&store, graph_id, "verdicts-pos").await;
        let (neg_a, neg_b) = seed_pair(&store, graph_id, "verdicts-neg").await;
        let (uns_a, uns_b) = seed_pair(&store, graph_id, "verdicts-uns").await;
        let expires = chrono::Utc::now() + chrono::Duration::days(30);

        let pos_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: pos_a,
                node_b_id: pos_b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::IdentifierAuto,
                confidence: Some(0.97),
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({"identifier": {"kind": "email", "shared": true}}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("write POSITIVE");

        let neg_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: neg_a,
                node_b_id: neg_b,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Llm,
                confidence: Some(0.2),
                model_id: Some("claude-haiku-4-5".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "different"}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("write NEGATIVE");

        let unsure_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: uns_a,
                node_b_id: uns_b,
                verdict: JudgementVerdict::Unsure,
                source: JudgementSource::Llm,
                confidence: Some(0.6),
                model_id: Some("claude-haiku-4-5".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "unsure"}),
                created_by: None,
                expires_at: Some(expires),
            })
            .await
            .expect("write UNSURE");

        let pos = store
            .get_active_judgement(graph_id, pos_b, pos_a) // reversed order on purpose
            .await
            .unwrap()
            .expect("positive judgement must round-trip");
        assert_eq!(pos.judgement_id, pos_id);
        assert_eq!(pos.verdict, "positive");
        assert_eq!(pos.source, "identifier_auto");
        assert_eq!(pos.confidence, Some(0.97));
        assert!(pos.annulled_at.is_none());

        let neg = store
            .get_active_judgement(graph_id, neg_a, neg_b)
            .await
            .unwrap()
            .expect("negative round-trips");
        assert_eq!(neg.judgement_id, neg_id);
        assert_eq!(neg.verdict, "negative");
        assert_eq!(neg.model_id.as_deref(), Some("claude-haiku-4-5"));

        let unsure = store
            .get_active_judgement(graph_id, uns_a, uns_b)
            .await
            .unwrap()
            .expect("unsure round-trips");
        assert_eq!(unsure.judgement_id, unsure_id);
        assert_eq!(unsure.verdict, "unsure");
        assert!(unsure.expires_at.is_some(), "UNSURE must carry the expiry it was written with");

        // list_active_judgements_for_node finds the POSITIVE row from EITHER endpoint.
        let from_a = store.list_active_judgements_for_node(graph_id, pos_a).await.unwrap();
        let from_b = store.list_active_judgements_for_node(graph_id, pos_b).await.unwrap();
        assert!(from_a.iter().any(|j| j.judgement_id == pos_id));
        assert!(from_b.iter().any(|j| j.judgement_id == pos_id));
    })
    .await;
}

// ── 2. Negative controls: CHECK constraints ─────────────────────────────────
// (raw SQL throughout — these never go through `write_judgement`, so its
// endpoint-liveness check does not apply; bare `Uuid::new_v4()` pairs are
// fine.)

#[tokio::test]
async fn ordered_pair_check_rejects_an_unordered_insert() {
    let Some(pool) = pool_or_skip("ordered_pair_check_rejects_an_unordered_insert").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-orderedpair").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let (lo, hi) = ordered_pair(Uuid::new_v4(), Uuid::new_v4());
        // Deliberately backwards: node_a_id = hi, node_b_id = lo.
        let err = sqlx::query(
            "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source) \
             VALUES ($1, $2, $3, 'positive', 'test')",
        )
        .bind(graph_id)
        .bind(hi)
        .bind(lo)
        .execute(&body_pool)
        .await
        .expect_err("an unordered pair must be rejected by reef_judgement_ordered_pair");
        assert_eq!(pg_code(&err).as_deref(), Some("23514"), "must be a CHECK violation, not some other error");
        assert_eq!(pg_constraint(&err).as_deref(), Some("reef_judgement_ordered_pair"));
    })
    .await;
}

#[tokio::test]
async fn expiry_shape_check_rejects_a_positive_row_with_an_expiry() {
    let Some(pool) = pool_or_skip("expiry_shape_check_rejects_a_positive_row_with_an_expiry").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-expiryshape").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let (a, b) = ordered_pair(Uuid::new_v4(), Uuid::new_v4());
        let err = sqlx::query(
            "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source, expires_at) \
             VALUES ($1, $2, $3, 'positive', 'test', now() + interval '30 days')",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .execute(&body_pool)
        .await
        .expect_err("a POSITIVE row with expires_at must be rejected by reef_judgement_expiry_shape");
        assert_eq!(pg_code(&err).as_deref(), Some("23514"));
        assert_eq!(pg_constraint(&err).as_deref(), Some("reef_judgement_expiry_shape"));
    })
    .await;
}

#[tokio::test]
async fn annul_kind_shape_check_rejects_either_half_set_alone() {
    let Some(pool) = pool_or_skip("annul_kind_shape_check_rejects_either_half_set_alone").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-annulkindshape").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Direction 1: annulled_at set, annulled_by_kind NULL.
        let (a1, b1) = ordered_pair(Uuid::new_v4(), Uuid::new_v4());
        let err1 = sqlx::query(
            "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source, annulled_at) \
             VALUES ($1, $2, $3, 'positive', 'test', now())",
        )
        .bind(graph_id)
        .bind(a1)
        .bind(b1)
        .execute(&body_pool)
        .await
        .expect_err("annulled_at with no annulled_by_kind must be rejected");
        assert_eq!(pg_code(&err1).as_deref(), Some("23514"));
        assert_eq!(pg_constraint(&err1).as_deref(), Some("reef_judgement_annul_kind_shape"));

        // Direction 2: annulled_by_kind set, annulled_at NULL.
        let (a2, b2) = ordered_pair(Uuid::new_v4(), Uuid::new_v4());
        let err2 = sqlx::query(
            "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source, annulled_by_kind) \
             VALUES ($1, $2, $3, 'positive', 'test', 'system')",
        )
        .bind(graph_id)
        .bind(a2)
        .bind(b2)
        .execute(&body_pool)
        .await
        .expect_err("annulled_by_kind with no annulled_at must be rejected");
        assert_eq!(pg_code(&err2).as_deref(), Some("23514"));
        assert_eq!(pg_constraint(&err2).as_deref(), Some("reef_judgement_annul_kind_shape"));

        // Sanity: BOTH set together is fine (this is what a real annul looks like).
        sqlx::query(
            "INSERT INTO reef_judgement \
                 (graph_id, node_a_id, node_b_id, verdict, source, annulled_at, annulled_by_kind) \
             VALUES ($1, $2, $3, 'positive', 'test', now(), 'system')",
        )
        .bind(graph_id)
        .bind(a2)
        .bind(b2)
        .execute(&body_pool)
        .await
        .expect("annulled_at + annulled_by_kind together must be accepted");
    })
    .await;
}

#[tokio::test]
async fn active_pair_uniqueness_rejects_a_second_active_row_then_allows_after_annul() {
    let Some(pool) =
        pool_or_skip("active_pair_uniqueness_rejects_a_second_active_row_then_allows_after_annul").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-uniquepair").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let (a, b) = ordered_pair(Uuid::new_v4(), Uuid::new_v4());

        let first_id: Uuid = sqlx::query_scalar(
            "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source) \
             VALUES ($1, $2, $3, 'positive', 'test') RETURNING judgement_id",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .expect("first active insert must succeed");

        let err = sqlx::query(
            "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source) \
             VALUES ($1, $2, $3, 'negative', 'test')",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .execute(&body_pool)
        .await
        .expect_err("a second active row for the same pair must be rejected");
        assert_eq!(pg_code(&err).as_deref(), Some("23505"), "must be a unique violation");
        assert_eq!(pg_constraint(&err).as_deref(), Some("uq_reef_judgement_active_pair"));

        // Annul the first row, then retry: must now succeed.
        sqlx::query(
            "UPDATE reef_judgement SET annulled_at = now(), annulled_by_kind = 'system' \
             WHERE judgement_id = $1",
        )
        .bind(first_id)
        .execute(&body_pool)
        .await
        .expect("annul the first row");

        let second_id: Uuid = sqlx::query_scalar(
            "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source) \
             VALUES ($1, $2, $3, 'negative', 'test') RETURNING judgement_id",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .expect("insert after annul must succeed");
        assert_ne!(second_id, first_id);

        let active_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND node_a_id = $2 \
             AND node_b_id = $3 AND annulled_at IS NULL",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(active_count, 1, "exactly one active row must survive");
    })
    .await;
}

// ── 3. write_judgement re-adjudication ──────────────────────────────────────

#[tokio::test]
async fn write_judgement_readjudication_unsure_to_negative_annuls_and_supersedes() {
    let Some(pool) = pool_or_skip("write_judgement_readjudication_unsure_to_negative_annuls_and_supersedes").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-readjudicate").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "readjudicate").await;

        let first_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Unsure,
                source: JudgementSource::Llm,
                confidence: Some(0.55),
                model_id: Some("claude-haiku-4-5".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "unsure"}),
                created_by: None,
                expires_at: Some(chrono::Utc::now() + chrono::Duration::days(30)),
            })
            .await
            .expect("initial UNSURE write");

        let second_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Llm,
                confidence: Some(0.1),
                model_id: Some("claude-haiku-4-5".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "different"}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("re-adjudication write to NEGATIVE");

        assert_ne!(first_id, second_id, "re-adjudication must write a NEW row, not mutate the old one");

        // Exactly one active row, and it is the new one.
        let active =
            store.get_active_judgement(graph_id, a, b).await.unwrap().expect("an active row must exist");
        assert_eq!(active.judgement_id, second_id);
        assert_eq!(active.verdict, "negative");

        let active_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND node_a_id = $2 \
             AND node_b_id = $3 AND annulled_at IS NULL",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(active_count, 1, "re-adjudication must leave exactly one active row");

        // The old row is annulled, system-kind (source was 'llm', not 'human'),
        // and points forward at the new row via superseded_by.
        let (annulled_at, annulled_by_kind, superseded_by): (
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
            Option<Uuid>,
        ) = sqlx::query_as(
            "SELECT annulled_at, annulled_by_kind, superseded_by FROM reef_judgement WHERE judgement_id = $1",
        )
        .bind(first_id)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert!(annulled_at.is_some(), "the old UNSURE row must be annulled");
        assert_eq!(annulled_by_kind.as_deref(), Some("system"));
        assert_eq!(superseded_by, Some(second_id), "old row must point forward at the new one");
    })
    .await;
}

/// Review round 1 finding 1's own revert-negative-control target: this test
/// FAILS on the pre-fix code, which treated "same verdict AND same source"
/// alone as a no-op — silently dropping the refreshed `prompt_version`/
/// `model_id`/`evidence` below instead of recording them.
#[tokio::test]
async fn write_judgement_readjudication_same_verdict_and_source_refreshes_metadata() {
    let Some(pool) =
        pool_or_skip("write_judgement_readjudication_same_verdict_and_source_refreshes_metadata").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-readjudicate-metadata").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "readjudicate-metadata").await;

        let v1_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Llm,
                confidence: Some(0.15),
                model_id: Some("claude-haiku-4-4".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "different", "reason": "v1"}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("initial NEGATIVE(llm, v1) write");

        let v2_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Negative, // SAME verdict as v1
                source: JudgementSource::Llm,        // SAME source as v1
                confidence: Some(0.12),
                model_id: Some("claude-haiku-4-5".to_string()), // different
                prompt_version: Some("resolve_entities_v2".to_string()), // different
                evidence: serde_json::json!({"llm_verdict": "different", "reason": "v2"}), // different
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("re-adjudication NEGATIVE(llm, v2) write");

        assert_ne!(
            v1_id, v2_id,
            "same verdict/source with DIFFERENT metadata must NOT resolve as an idempotent \
             no-op — it must write a new row"
        );

        let active = store.get_active_judgement(graph_id, a, b).await.unwrap().expect("active row must exist");
        assert_eq!(active.judgement_id, v2_id);
        assert_eq!(active.prompt_version.as_deref(), Some("resolve_entities_v2"));
        assert_eq!(active.model_id.as_deref(), Some("claude-haiku-4-5"));
        assert_eq!(active.evidence, serde_json::json!({"llm_verdict": "different", "reason": "v2"}));

        let active_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND node_a_id = $2 \
             AND node_b_id = $3 AND annulled_at IS NULL",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(active_count, 1, "exactly one active row must survive the re-adjudication");

        let (annulled_at, annul_reason, superseded_by): (
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
            Option<Uuid>,
        ) = sqlx::query_as(
            "SELECT annulled_at, annul_reason, superseded_by FROM reef_judgement WHERE judgement_id = $1",
        )
        .bind(v1_id)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert!(annulled_at.is_some(), "v1 must be annulled by the re-adjudication");
        assert_eq!(annul_reason.as_deref(), Some("re-adjudication"));
        assert_eq!(superseded_by, Some(v2_id), "v1 must point forward at v2");
    })
    .await;
}

// ── 4. Concurrency race ─────────────────────────────────────────────────────

/// Two connections racing `write_judgement` for the IDENTICAL pair with the
/// IDENTICAL outcome (same verdict, same source, same metadata) — the
/// scenario design doc §5.1 describes as two concurrent `EntityResolution`
/// jobs independently reaching the same conclusion. The advisory lock
/// (statement 1 of `write_judgement`) serializes them at the database level
/// regardless of how the two Rust futures interleave; the LOSER must
/// resolve through the documented no-op path (`Ok(existing_id)`), never an
/// unhandled error, and exactly one active row must survive.
#[tokio::test]
async fn concurrent_write_judgement_on_the_same_pair_leaves_exactly_one_active_row() {
    let Some(pool) =
        pool_or_skip("concurrent_write_judgement_on_the_same_pair_leaves_exactly_one_active_row").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-race").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "race").await;

        let build = move || NewJudgement {
            graph_id,
            node_a_id: a,
            node_b_id: b,
            verdict: JudgementVerdict::Positive,
            source: JudgementSource::Llm,
            confidence: Some(0.9),
            model_id: Some("claude-haiku-4-5".to_string()),
            prompt_version: Some("resolve_entities_v1".to_string()),
            evidence: serde_json::json!({"llm_verdict": "same"}),
            created_by: None,
            expires_at: None,
        };

        let store_1 = PostgresStore::new(body_pool.clone());
        let store_2 = PostgresStore::new(body_pool.clone());
        let task_1 = tokio::spawn(async move { store_1.write_judgement(build()).await });
        let task_2 = tokio::spawn(async move { store_2.write_judgement(build()).await });

        let (r1, r2) = tokio::join!(task_1, task_2);
        let r1 = r1.expect("task 1 panicked");
        let r2 = r2.expect("task 2 panicked");

        assert!(r1.is_ok(), "writer 1 must not surface an unhandled error: {r1:?}");
        assert!(r2.is_ok(), "writer 2 must not surface an unhandled error: {r2:?}");
        assert_eq!(
            r1.unwrap(),
            r2.unwrap(),
            "both writers wrote the identical outcome, so the loser must resolve to the SAME \
             judgement_id as the winner (the documented no-op path), not mint a second row"
        );

        let active_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND node_a_id = $2 \
             AND node_b_id = $3 AND annulled_at IS NULL",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(active_count, 1, "exactly one active row must survive the race");

        let total_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND node_a_id = $2 AND node_b_id = $3",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(total_count, 1, "the no-op path must not have inserted a second (even annulled) row");
    })
    .await;
}

// ── 5. HumanJudgementExists ──────────────────────────────────────────────────

#[tokio::test]
async fn human_judgement_blocks_a_subsequent_non_human_write() {
    let Some(pool) = pool_or_skip("human_judgement_blocks_a_subsequent_non_human_write").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-humanblock").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "humanblock").await;

        let human_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Human,
                confidence: None,
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({"card_id": "test-card"}),
                created_by: Some(user_id),
                expires_at: None,
            })
            .await
            .expect("human POSITIVE write");

        let attempt = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Llm,
                confidence: Some(0.3),
                model_id: Some("claude-haiku-4-5".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "different"}),
                created_by: None,
                expires_at: None,
            })
            .await;

        match attempt {
            Err(StoreError::HumanJudgementExists { judgement_id }) => {
                assert_eq!(judgement_id, human_id, "the error must name the active human row");
            }
            other => panic!("expected HumanJudgementExists, got {other:?}"),
        }

        // Nothing was written: still exactly one row total for this pair, still
        // the original human one, still active.
        let total_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND node_a_id = $2 AND node_b_id = $3",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(total_count, 1, "a refused write must not leave ANY row behind, annulled or otherwise");

        let active =
            store.get_active_judgement(graph_id, a, b).await.unwrap().expect("human row still active");
        assert_eq!(active.judgement_id, human_id);
        assert_eq!(active.source, "human");
        assert!(active.annulled_at.is_none(), "the human row must be untouched by the refused write");
    })
    .await;
}

// ── 6. Expiry live-ness ──────────────────────────────────────────────────────

/// Review round 1 finding 2's own revert-negative-control target: this test
/// FAILS on the pre-fix code, which had no expiry filter on reads at all —
/// an already-expired UNSURE row would still come back from
/// `get_active_judgement`/`list_active_judgements_for_node`.
#[tokio::test]
async fn expired_active_row_is_invisible_to_reads() {
    let Some(pool) = pool_or_skip("expired_active_row_is_invisible_to_reads").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-expired-reads").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "expired-reads").await;

        // write_judgement itself does not reject a past expires_at — only
        // verdict='unsure' is required (reef_judgement_expiry_shape). This
        // simulates a row whose TTL has simply lapsed since it was written.
        let past = chrono::Utc::now() - chrono::Duration::days(1);
        store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Unsure,
                source: JudgementSource::Llm,
                confidence: Some(0.5),
                model_id: Some("claude-haiku-4-5".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "unsure"}),
                created_by: None,
                expires_at: Some(past),
            })
            .await
            .expect("write an already-expired UNSURE");

        // The row genuinely still exists, un-annulled, in the database — this
        // proves the assertions below exercise the READ-side "live" filter,
        // not an absence of any row at all.
        let raw_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND node_a_id = $2 \
             AND node_b_id = $3 AND annulled_at IS NULL",
        )
        .bind(graph_id)
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(raw_count, 1, "sanity: the expired row must still be un-annulled at the SQL level");

        assert!(
            store.get_active_judgement(graph_id, a, b).await.unwrap().is_none(),
            "get_active_judgement must treat an expired-but-unannulled row as NOT live"
        );
        let listed = store.list_active_judgements_for_node(graph_id, a).await.unwrap();
        assert!(
            listed.is_empty(),
            "list_active_judgements_for_node must treat an expired-but-unannulled row as NOT \
             live either — got {listed:?}"
        );
    })
    .await;
}

/// The critical interaction review round 1 finding 2 called out: an
/// expired-but-unannulled row must be UNCONDITIONALLY supersedable by
/// `write_judgement`, regardless of its own `source` — INCLUDING
/// `source=human`. A LIVE (non-expired) human row would refuse a non-human
/// write via `HumanJudgementExists`; an EXPIRED one must not.
#[tokio::test]
async fn expired_human_unsure_does_not_block_and_gets_superseded() {
    let Some(pool) = pool_or_skip("expired_human_unsure_does_not_block_and_gets_superseded").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-expired-human").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "expired-human").await;

        let past = chrono::Utc::now() - chrono::Duration::days(1);
        let human_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Unsure,
                source: JudgementSource::Human,
                confidence: None,
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({"card_id": "expired-human-card"}),
                created_by: Some(user_id),
                expires_at: Some(past),
            })
            .await
            .expect("write an already-expired human UNSURE");

        // A non-human write for the same pair must SUCCEED — the expired
        // human row must NEVER trigger HumanJudgementExists.
        let auto_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::IdentifierAuto,
                confidence: Some(0.99),
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({"identifier": {"kind": "email", "shared": true}}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect(
                "an identifier_auto write must succeed against an EXPIRED human UNSURE — an \
                 expired row must never block via HumanJudgementExists",
            );
        assert_ne!(human_id, auto_id);

        let active = store.get_active_judgement(graph_id, a, b).await.unwrap().expect("new active row");
        assert_eq!(active.judgement_id, auto_id);
        assert_eq!(active.verdict, "positive");

        let (annulled_at, annulled_by_kind, annul_reason, superseded_by): (
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
            Option<String>,
            Option<Uuid>,
        ) = sqlx::query_as(
            "SELECT annulled_at, annulled_by_kind, annul_reason, superseded_by FROM reef_judgement \
             WHERE judgement_id = $1",
        )
        .bind(human_id)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert!(annulled_at.is_some(), "the expired human row must be annulled");
        assert_eq!(annulled_by_kind.as_deref(), Some("system"), "expiry-driven annuls are always system-kind");
        assert_eq!(annul_reason.as_deref(), Some("expired"));
        assert_eq!(superseded_by, Some(auto_id));
    })
    .await;
}

// ── 7. annul_judgement's full contract ──────────────────────────────────────

#[tokio::test]
async fn annul_judgement_annuls_and_is_idempotent() {
    let Some(pool) = pool_or_skip("annul_judgement_annuls_and_is_idempotent").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-annul-idempotent").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "annul-idempotent").await;

        let id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Human,
                confidence: None,
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({}),
                created_by: Some(user_id),
                expires_at: None,
            })
            .await
            .expect("seed a human POSITIVE");

        let outcome = store
            .annul_judgement(id, Some(user_id), AnnulKind::Human, "undo test")
            .await
            .expect("annul_judgement");
        assert_eq!(outcome, AnnulOutcome::Annulled);

        assert!(
            store.get_active_judgement(graph_id, a, b).await.unwrap().is_none(),
            "the row must no longer be active after annul"
        );

        // Idempotent: a second call is a clean no-op naming nothing to
        // supersede it (this row was rejected, not re-adjudicated).
        let outcome2 = store
            .annul_judgement(id, Some(user_id), AnnulKind::Human, "undo test again")
            .await
            .expect("second annul_judgement must not error");
        assert_eq!(outcome2, AnnulOutcome::AlreadyAnnulled { superseded_by: None });

        // The second call must not have clobbered the first annul's reason.
        let annul_reason: Option<String> =
            sqlx::query_scalar("SELECT annul_reason FROM reef_judgement WHERE judgement_id = $1")
                .bind(id)
                .fetch_one(&body_pool)
                .await
                .unwrap();
        assert_eq!(
            annul_reason.as_deref(),
            Some("undo test"),
            "the SECOND call's reason must never overwrite the first annul's"
        );
    })
    .await;
}

#[tokio::test]
async fn annul_judgement_reports_superseded_by_when_already_annulled_via_readjudication() {
    let Some(pool) = pool_or_skip(
        "annul_judgement_reports_superseded_by_when_already_annulled_via_readjudication",
    )
    .await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-annul-superseded").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "annul-superseded").await;

        let first_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Unsure,
                source: JudgementSource::Llm,
                confidence: Some(0.5),
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({}),
                created_by: None,
                expires_at: Some(chrono::Utc::now() + chrono::Duration::days(30)),
            })
            .await
            .expect("initial UNSURE");

        let second_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Llm,
                confidence: Some(0.9),
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("re-adjudication annuls the first row via write_judgement itself");

        // first_id is ALREADY annulled — by write_judgement's own
        // supersession, not by an explicit annul_judgement call — so calling
        // annul_judgement on it now must be an idempotent no-op that still
        // reports what superseded it.
        let outcome = store
            .annul_judgement(first_id, None, AnnulKind::System, "should not apply")
            .await
            .expect("annul_judgement on an already-annulled row");
        assert_eq!(outcome, AnnulOutcome::AlreadyAnnulled { superseded_by: Some(second_id) });
    })
    .await;
}

#[tokio::test]
async fn annul_judgement_unknown_id_is_a_clean_no_op() {
    let Some(pool) = pool_or_skip("annul_judgement_unknown_id_is_a_clean_no_op").await else {
        return;
    };
    let store = PostgresStore::new(pool);
    let outcome = store
        .annul_judgement(Uuid::new_v4(), None, AnnulKind::System, "never existed")
        .await
        .expect("annul_judgement on an unknown id must not error");
    assert_eq!(outcome, AnnulOutcome::AlreadyAnnulled { superseded_by: None });
}

// ── 8. `_scoped` variants ────────────────────────────────────────────────────

#[tokio::test]
async fn scoped_variants_round_trip_write_get_list_annul() {
    let Some(pool) = pool_or_skip("scoped_variants_round_trip_write_get_list_annul").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-scoped").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (a, b) = seed_pair(&store, graph_id, "scoped-pair").await;

        let mut tx = body_pool.begin().await.expect("begin");

        let id = store
            .write_judgement_scoped(
                &mut tx,
                NewJudgement {
                    graph_id,
                    node_a_id: a,
                    node_b_id: b,
                    verdict: JudgementVerdict::Positive,
                    source: JudgementSource::IdentifierAuto,
                    confidence: Some(0.95),
                    model_id: None,
                    prompt_version: None,
                    evidence: serde_json::json!({}),
                    created_by: None,
                    expires_at: None,
                },
            )
            .await
            .expect("write_judgement_scoped");

        let active = store
            .get_active_judgement_scoped(&mut tx, graph_id, a, b)
            .await
            .unwrap()
            .expect("get_active_judgement_scoped must find the row written in the SAME transaction");
        assert_eq!(active.judgement_id, id);

        let listed = store.list_active_judgements_for_node_scoped(&mut tx, graph_id, a).await.unwrap();
        assert!(listed.iter().any(|j| j.judgement_id == id));

        let outcome = store
            .annul_judgement_scoped(&mut tx, id, None, AnnulKind::System, "scoped test annul")
            .await
            .expect("annul_judgement_scoped");
        assert_eq!(outcome, AnnulOutcome::Annulled);

        let outcome2 = store
            .annul_judgement_scoped(&mut tx, id, None, AnnulKind::System, "second")
            .await
            .expect("second annul_judgement_scoped must not error");
        assert_eq!(outcome2, AnnulOutcome::AlreadyAnnulled { superseded_by: None });

        tx.commit().await.expect("commit");

        // After commit, the plain (unscoped) read confirms the annul stuck.
        assert!(store.get_active_judgement(graph_id, a, b).await.unwrap().is_none());
    })
    .await;
}

// ── 9. Endpoint validation (CodeRabbit round 2 Major) ───────────────────────

#[tokio::test]
async fn write_judgement_rejects_a_cross_graph_pair() {
    let Some(pool) = pool_or_skip("write_judgement_rejects_a_cross_graph_pair").await else {
        return;
    };
    let (user_1, account_1, graph_1) = seed_graph(&pool, "judgement-crossgraph-1").await;
    let (user_2, account_2, graph_2) = seed_graph(&pool, "judgement-crossgraph-2").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_1, account_1), (user_2, account_2)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let node_in_graph_1 =
            store.upsert_node(graph_1, "entity", "in-graph-1").await.expect("seed node in graph 1");
        let node_in_graph_2 =
            store.upsert_node(graph_2, "entity", "in-graph-2").await.expect("seed node in graph 2");
        let (a, b) = ordered_pair(node_in_graph_1, node_in_graph_2);

        let attempt = store
            .write_judgement(NewJudgement {
                graph_id: graph_1, // claims graph_1, but one endpoint actually lives in graph_2
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::IdentifierAuto,
                confidence: Some(0.9),
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({}),
                created_by: None,
                expires_at: None,
            })
            .await;
        match attempt {
            Err(StoreError::JudgementEndpointMissing { graph_id }) => assert_eq!(graph_id, graph_1),
            other => panic!("expected JudgementEndpointMissing, got {other:?}"),
        }

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE node_a_id = $1 AND node_b_id = $2",
        )
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "a cross-graph pair must write NOTHING");
    })
    .await;
}

#[tokio::test]
async fn write_judgement_rejects_a_missing_endpoint() {
    let Some(pool) = pool_or_skip("write_judgement_rejects_a_missing_endpoint").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "judgement-missingendpoint").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let real_node = store.upsert_node(graph_id, "entity", "real-node").await.expect("seed real node");
        let never_created = Uuid::new_v4();
        let (a, b) = ordered_pair(real_node, never_created);

        let attempt = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::IdentifierAuto,
                confidence: Some(0.9),
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({}),
                created_by: None,
                expires_at: None,
            })
            .await;
        match attempt {
            Err(StoreError::JudgementEndpointMissing { graph_id: g }) => assert_eq!(g, graph_id),
            other => panic!("expected JudgementEndpointMissing, got {other:?}"),
        }

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE node_a_id = $1 AND node_b_id = $2",
        )
        .bind(a)
        .bind(b)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "a pair with a missing endpoint must write NOTHING");
    })
    .await;
}

// ── 10. RLS cross-account invisibility ──────────────────────────────────────

/// This suite (unlike `consumer_rls_pg.rs`) is not trying to prove the LIVE
/// connection path works — it reuses whatever privileged role
/// `RELINFER_TEST_DATABASE_URL` connects as, then switches into
/// `coralswarm_app` with `SET LOCAL ROLE` (transaction-scoped; reverts
/// automatically at COMMIT/ROLLBACK, so it never leaks onto the shared pool)
/// rather than a second real login. That is a materially weaker claim about
/// the CONNECTION (it assumes the calling role can `SET ROLE` into
/// `coralswarm_app` — true for a superuser/local-dev owner role, and is
/// exactly `consumer_rls_pg.rs`'s own job to prove for the real deployed
/// connection) but an equally strong claim about the POLICY PREDICATE
/// itself: once inside `coralswarm_app` (which is `NOBYPASSRLS` and not a
/// table owner — migration 0083 §"NOLOGIN would be safer..."), RLS applies
/// identically regardless of how the session got into that role. Same
/// non-vacuous-denial discipline as `consumer_rls_pg.rs`/`tenant_rls_pg.rs`:
/// assert the denial, then DISABLE ROW LEVEL SECURITY and watch the same
/// query go positive, then re-enable and assert the denial is back — with
/// the DISABLE window itself wrapped in a spawned task (review round 1
/// finding 5 / CodeRabbit's overlapping note) so a panicking assertion
/// inside it can never strand this SHARED database with RLS disabled.
#[tokio::test]
async fn judgement_graph_rls_denies_cross_account_reads() {
    let Some(pool) = pool_or_skip("judgement_graph_rls_denies_cross_account_reads").await else {
        return;
    };
    let (user_a, account_a, graph_a) = seed_graph(&pool, "judgement-rls-a").await;
    let (user_b, account_b, _graph_b) = seed_graph(&pool, "judgement-rls-b").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_a, account_a), (user_b, account_b)], async move {
        // A judgement row that exists only in A's ocean. Bare Uuid::new_v4()
        // node ids are fine here — this is a raw SQL insert that bypasses
        // write_judgement's endpoint-liveness check entirely, which has no
        // bearing on what this RLS test is proving.
        let (node_1, node_2) = ordered_pair(Uuid::new_v4(), Uuid::new_v4());
        sqlx::query(
            "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source) \
             VALUES ($1, $2, $3, 'positive', 'test')",
        )
        .bind(graph_a)
        .bind(node_1)
        .bind(node_2)
        .execute(&body_pool)
        .await
        .expect("seed A's judgement row");

        async fn count_as_account(pool: &PgPool, account_id: Uuid, graph_id: Uuid) -> i64 {
            let mut tx = pool.begin().await.expect("begin");
            sqlx::query("SET LOCAL ROLE coralswarm_app")
                .execute(&mut *tx)
                .await
                .expect(
                    "SET LOCAL ROLE coralswarm_app failed — this test needs a connecting role \
                     that can switch into coralswarm_app (superuser, or explicit role \
                     membership); see this test's own doc comment",
                );
            sqlx::query("SELECT set_config('app.account_id', $1, true)")
                .bind(account_id.to_string())
                .execute(&mut *tx)
                .await
                .expect("set app.account_id");
            let row = sqlx::query("SELECT count(*) AS n FROM reef_judgement WHERE graph_id = $1")
                .bind(graph_id)
                .fetch_one(&mut *tx)
                .await
                .expect("probe query");
            let n: i64 = row.get("n");
            tx.rollback().await.ok();
            n
        }

        // Scoped correctly to A: the row IS visible (proves this isn't just an
        // empty table / totally-deny-everything misconfiguration).
        let as_a = count_as_account(&body_pool, account_a, graph_a).await;
        assert_eq!(as_a, 1, "account A must see its own reef_judgement row");

        // Scoped to B: must be invisible.
        let as_b = count_as_account(&body_pool, account_b, graph_a).await;
        assert_eq!(as_b, 0, "account B must not see account A's reef_judgement row — RLS boundary leaked");

        // ── Non-vacuous-denial negative control, made panic-safe ───────────
        let probe_pool = body_pool.clone();
        let probe = tokio::spawn(async move {
            sqlx::query("ALTER TABLE reef_judgement DISABLE ROW LEVEL SECURITY")
                .execute(&probe_pool)
                .await
                .expect("disable RLS for the negative control");
            count_as_account(&probe_pool, account_b, graph_a).await
        });
        let probe_result = probe.await;

        // ALWAYS re-enable, whether the probe task panicked or not — ENABLE
        // ROW LEVEL SECURITY is idempotent, so this is safe even if DISABLE
        // (above) never actually ran.
        sqlx::query("ALTER TABLE reef_judgement ENABLE ROW LEVEL SECURITY")
            .execute(&body_pool)
            .await
            .expect("re-enable RLS");

        let leaked = match probe_result {
            Ok(n) => n,
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        };
        assert!(
            leaked > 0,
            "NEGATIVE CONTROL FAILED: with row-level security disabled, B's probe still returned \
             0 rows — the `as_b == 0` assertion above is therefore vacuous (the fixture row is \
             missing, or this connection cannot reach the table at all), and this test is \
             certifying nothing"
        );

        let denied_again = count_as_account(&body_pool, account_b, graph_a).await;
        assert_eq!(denied_again, 0, "must deny again once RLS is re-enabled");
    })
    .await;
}
