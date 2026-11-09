//! Current judgement-graph contract: EntityResolution is admitted onto the
//! `managed_enrichment` purpose (source-agnostic plan PR 5) and its worker
//! claims nothing while `ENRICHMENT_PROCESSING_ENABLED` is off, the default.
//! Plus reachable manual merge/dismiss/undo/review and store proofs.
//! The historical automatic rewire/materialization/read-gate cases keep their
//! distinct input states and now assert real queue ADMISSION (a pending job,
//! no longer dead-lettered), idle worker ticks with the switch off, no
//! provider entry and unchanged retained/accounting state. The switched-on
//! managed behaviour (judged merges with receipts, "different" keeps reefs
//! apart, stale inputs publish nothing, the shared cap holds, identifier
//! auto-merge stays free, no double billing) is proven against funded,
//! two-plane fixtures in `managed::gate::pg_tests::entity_resolution`.
//!
//! Uses the existing RELINFER_TEST_DATABASE_URL shared migrated database.
//! Most fixtures use its owner/BYPASSRLS pool; the three late SET ROLE cases
//! retain their existing explicit application-role coverage. Failed fixture
//! accounts are preserved; only successful fixtures are cleaned.
//!
//! `merge_node`/`dismiss_merge_suggestion` are invoked as PLAIN FUNCTIONS
//! with a hand-built `Tenant`/`UserContext` (re-exported at the crate root —
//! see `lib.rs`'s doc comment on that `pub use`), the same pattern
//! `journal_endpoint_pg.rs` established for `journal_hour_items`: no
//! existing `*_pg.rs` file called a `routes/nodes.rs` handler before this
//! suite, so a narrow re-export was the minimum change needed rather than
//! standing up the full HTTP+JWKS router harness.
//!
//! `JUDGEMENT_GRAPH_WRITES` is process-global env, mutated per test — every
//! test sets it at the start and restores the prior value at the end, and
//! this file MUST run with `--test-threads=1` (the pg-suite convention,
//! doubly load-bearing here since parallel tests would race the flag).
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test judgement_graph_worker_pg -- --test-threads=1 --nocapture

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::http::StatusCode;
use axum::Json;
use coralswarm_backend::jobs::{queue, JobKind};
use coralswarm_backend::llm::{
    CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent,
};
use coralswarm_backend::store::{
    AnnulKind, JudgementSource, JudgementVerdict, NewJudgement, PostgresStore, Store, StoreError,
};
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::entity_resolution;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::{
    admin_chain_health, confirm_merge_review, dedup_entities, dismiss_merge_suggestion,
    list_merge_review, merge_node, review_count, undo_merge, ConfirmMergeReviewRequest,
    DismissMergeRequest, ListMergeReviewQuery, MergeRequest, ReviewCountQuery,
};
use coralswarm_backend::UserContext;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

// ── Fixture helpers (mirrors `judgement_graph_pg.rs`'s own — this is a
//    separate test binary/crate, so helpers are duplicated locally rather
//    than shared, matching this repo's existing `*_pg.rs` convention) ──────

async fn pool_or_skip(test_name: &str) -> Option<PgPool> {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run the live \
                 judgement-graph worker suite"
            );
            return None;
        }
    };
    Some(PgPoolOptions::new().max_connections(8).connect(&url).await.expect("connect to test Postgres"))
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
    // `account_members` (NOT just `accounts.owner_user_id`) is what
    // `resolve_graph_reachable_by_user`/`ReachabilityResolver` actually
    // reads (routes/nodes.rs's ADR-0007 D9 reachability gate) — PR1's own
    // `seed_graph` never needed this (its tests only exercise `write_judgement`'s
    // endpoint-liveness check + RLS, neither of which reads `account_members`),
    // but this suite's route-coverage tests go through the real handler's
    // reachability check, so a member row is load-bearing here.
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("insert account_members");
    let store = PostgresStore::new(pool.clone());
    let graph_id = store.create_graph(account_id, label, "personal").await.expect("create graph").id;
    (user_id, account_id, graph_id)
}

async fn cleanup(pool: &PgPool, user_id: Uuid, account_id: Uuid) {
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(pool).await.ok();
}

async fn with_cleanup<Fut>(pool: PgPool, accounts: Vec<(Uuid, Uuid)>, body: Fut)
where
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let result = tokio::spawn(body).await;
    match result {
        Ok(()) => {
            for (user_id, account_id) in accounts {
                cleanup(&pool, user_id, account_id).await;
            }
        }
        Err(e) => {
            eprintln!("preserving failed judgement fixture accounts: {accounts:?}");
            std::panic::resume_unwind(e.into_panic());
        }
    }
}

fn ordered_pair(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}

async fn seed_node(store: &PostgresStore, graph_id: Uuid, title: &str) -> Uuid {
    store.upsert_node(graph_id, "entity", title).await.expect("seed node")
}

/// Give `node_id` `n` corals (`node_atoms` rows) so `nodes.coral_count`
/// crosses `review_policy::IMPACT_ATOMS` (3) — the cheapest way to make a
/// node "high-impact" for the §5.3 UNSURE band's impact gate. Bypasses the
/// normal atom/ingest pipeline entirely (raw SQL): the trigger
/// `trg_node_atoms_coral_count` maintains `nodes.coral_count` on
/// `node_atoms` INSERT regardless of how the atom row itself was created.
async fn seed_atoms_for_impact(pool: &PgPool, graph_id: Uuid, node_id: Uuid, n: usize) {
    for i in 0..n {
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'note', $2, 'unused', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("impact-{node_id}-{i}"))
        .fetch_one(pool)
        .await
        .expect("seed atom");
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(pool)
            .await
            .expect("link atom");
    }
}

/// Read the (at most one) active `reef_judgement` row for a canonical pair —
/// raw SQL, not `Store::get_active_judgement`, so these assertions don't
/// depend on the very method the tests are trying to prove correct.
async fn active_judgement_row(pool: &PgPool, graph_id: Uuid, a: Uuid, b: Uuid) -> Option<ReefJudgementRow> {
    let (a, b) = ordered_pair(a, b);
    sqlx::query_as::<_, ReefJudgementRow>(
        "SELECT judgement_id, verdict, source, confidence, model_id, prompt_version, evidence, \
                created_by, expires_at, annulled_at, annulled_by_kind, annul_reason \
         FROM reef_judgement \
         WHERE graph_id = $1 AND node_a_id = $2 AND node_b_id = $3 AND annulled_at IS NULL",
    )
    .bind(graph_id)
    .bind(a)
    .bind(b)
    .fetch_optional(pool)
    .await
    .expect("query reef_judgement")
}

async fn any_judgement_row(pool: &PgPool, graph_id: Uuid, a: Uuid, b: Uuid) -> Option<ReefJudgementRow> {
    let (a, b) = ordered_pair(a, b);
    sqlx::query_as::<_, ReefJudgementRow>(
        "SELECT judgement_id, verdict, source, confidence, model_id, prompt_version, evidence, \
                created_by, expires_at, annulled_at, annulled_by_kind, annul_reason \
         FROM reef_judgement \
         WHERE graph_id = $1 AND node_a_id = $2 AND node_b_id = $3 \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(graph_id)
    .bind(a)
    .bind(b)
    .fetch_optional(pool)
    .await
    .expect("query reef_judgement")
}

#[derive(sqlx::FromRow, Debug)]
struct ReefJudgementRow {
    judgement_id: Uuid,
    verdict: String,
    source: String,
    confidence: Option<f64>,
    model_id: Option<String>,
    prompt_version: Option<String>,
    evidence: serde_json::Value,
    created_by: Option<Uuid>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    annulled_at: Option<chrono::DateTime<chrono::Utc>>,
    annulled_by_kind: Option<String>,
    annul_reason: Option<String>,
}

async fn reef_judgement_count(pool: &PgPool, graph_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM reef_judgement WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(pool)
        .await
        .expect("count reef_judgement")
}

async fn node_exists(pool: &PgPool, node_id: Uuid) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)")
        .bind(node_id)
        .fetch_one(pool)
        .await
        .expect("check node existence")
}

/// The judgement's four annul columns as one comparable snapshot —
/// `annulled_by` included deliberately: it's the column that actually
/// differs between a human annul (`Some(user_id)`) and the undo path's
/// system annul (`None`), so metadata-preservation assertions that omit it
/// would miss a clobber that swaps who annulled.
async fn annul_columns(
    pool: &PgPool,
    judgement_id: Uuid,
) -> (Option<chrono::DateTime<chrono::Utc>>, Option<Uuid>, Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT annulled_at, annulled_by, annulled_by_kind, annul_reason \
         FROM reef_judgement WHERE judgement_id = $1",
    )
    .bind(judgement_id)
    .fetch_one(pool)
    .await
    .expect("judgement row must still exist (append-only)")
}

async fn latest_merge_id(pool: &PgPool, keep_id: Uuid, dup_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "SELECT merge_id FROM node_merges WHERE keep_id = $1 AND dup_id = $2 \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(keep_id)
    .bind(dup_id)
    .fetch_one(pool)
    .await
    .expect("find the merge_id to undo")
}

async fn undone_at(pool: &PgPool, merge_id: Uuid) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar("SELECT undone_at FROM node_merges WHERE merge_id = $1")
        .bind(merge_id)
        .fetch_one(pool)
        .await
        .expect("merge row must exist")
}

async fn node_merges_judgement_id(pool: &PgPool, keep_id: Uuid, dup_id: Uuid) -> Option<Option<Uuid>> {
    sqlx::query(
        "SELECT judgement_id FROM node_merges WHERE keep_id = $1 AND dup_id = $2 \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(keep_id)
    .bind(dup_id)
    .fetch_optional(pool)
    .await
    .expect("query node_merges")
    .map(|row| row.get::<Option<Uuid>, _>("judgement_id"))
}

async fn not_same_contains(pool: &PgPool, a: Uuid, b: Uuid) -> bool {
    let (lo, hi) = ordered_pair(a, b);
    // COALESCE, not a bare `body->'not_same'`: when the lower-id node has
    // never had a dismissal recorded at all, that expression is SQL NULL
    // (not JSON `null`), which a non-`Option` `serde_json::Value` decode
    // target rejects (F5's new negative-control test is the first case in
    // this file that legitimately reaches this helper on a node with no
    // `not_same` key yet — the review round 2 fix pass's whole point is that
    // NO dismissal must be written there).
    let arr: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT COALESCE(body->'not_same', '[]'::jsonb) FROM nodes WHERE node_id = $1",
    )
    .bind(lo)
    .fetch_optional(pool)
    .await
    .expect("query not_same");
    arr.and_then(|v| v.as_array().map(|a| a.iter().any(|e| e.as_str() == Some(&hi.to_string()))))
        .unwrap_or(false)
}

/// RAII-ish save/set/restore for `JUDGEMENT_GRAPH_WRITES` — every test in
/// this file must run with `--test-threads=1` (module doc), so this is safe
/// without a mutex.
struct FlagGuard(Option<String>);
impl FlagGuard {
    fn set(value: &str) -> Self {
        let prev = std::env::var("JUDGEMENT_GRAPH_WRITES").ok();
        std::env::set_var("JUDGEMENT_GRAPH_WRITES", value);
        FlagGuard(prev)
    }
    fn unset() -> Self {
        let prev = std::env::var("JUDGEMENT_GRAPH_WRITES").ok();
        std::env::remove_var("JUDGEMENT_GRAPH_WRITES");
        FlagGuard(prev)
    }
}
impl Drop for FlagGuard {
    fn drop(&mut self) {
        match &self.0 {
            Some(v) => std::env::set_var("JUDGEMENT_GRAPH_WRITES", v),
            None => std::env::remove_var("JUDGEMENT_GRAPH_WRITES"),
        }
    }
}

// ── LLM stub: counts calls, returns one fixed canned verdict ──────────────

struct CountingLlm {
    body: String,
    calls: AtomicUsize,
}
impl CountingLlm {
    fn new(body: &str) -> Self {
        Self { body: body.to_string(), calls: AtomicUsize::new(0) }
    }
    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}
#[async_trait::async_trait]
impl LlmProvider for CountingLlm {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CompletionResponse {
            content: self.body.clone(),
            input_tokens: 5,
            output_tokens: 5,
            model: "stub-model".to_string(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: None,
            cost_usd: None,
        })
    }
    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        unimplemented!("not exercised by these tests")
    }
}

/// Historical concurrent-human-write provider, retained as a side-effect
/// trap. The switched-off test requires it never to be entered; it no longer
/// claims to exercise the dormant worker's mid-adjudication race handling.
struct HumanRaceLlm {
    pool: PgPool,
    graph_id: Uuid,
    a: Uuid,
    b: Uuid,
    body: String,
    injected: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl LlmProvider for HumanRaceLlm {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        if !self.injected.swap(true, Ordering::SeqCst) {
            let store = PostgresStore::new(self.pool.clone());
            store
                .write_judgement(NewJudgement {
                    graph_id: self.graph_id,
                    node_a_id: self.a,
                    node_b_id: self.b,
                    verdict: JudgementVerdict::Positive,
                    source: JudgementSource::Human,
                    confidence: None,
                    model_id: None,
                    prompt_version: None,
                    evidence: serde_json::json!({"race": "concurrent human merge mid-adjudication"}),
                    created_by: None,
                    expires_at: None,
                })
                .await
                .expect("inject the concurrent human judgement");
        }
        Ok(CompletionResponse {
            content: self.body.clone(),
            input_tokens: 5,
            output_tokens: 5,
            model: "stub-model".to_string(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: None,
            cost_usd: None,
        })
    }
    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        unimplemented!("not exercised by these tests")
    }
}

/// Snapshot the named retained graph rows and account-side effects. This is
/// fixture instrumentation, not another permission/admission implementation.
async fn sweep_state(pool: &PgPool, graph_id: Uuid, account_id: Uuid, excluded_job: Option<Uuid>) -> serde_json::Value {
    sqlx::query_scalar(r#"SELECT jsonb_build_object(
      'nodes',(SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id) FROM nodes n WHERE graph_id=$1),
      'atoms',(SELECT jsonb_agg(to_jsonb(a) ORDER BY atom_id) FROM atoms a WHERE graph_id=$1),
      'links',(SELECT jsonb_agg(to_jsonb(l) ORDER BY node_id,atom_id) FROM node_atoms l WHERE node_id IN(SELECT node_id FROM nodes WHERE graph_id=$1)),
      'edges',(SELECT jsonb_agg(to_jsonb(e) ORDER BY edge_id) FROM relationship_edge e WHERE graph_id=$1),
      'identifiers',(SELECT jsonb_agg(to_jsonb(i) ORDER BY kind,value_norm) FROM reef_identifiers i WHERE graph_id=$1),
      'judgements',(SELECT jsonb_agg(to_jsonb(j) ORDER BY judgement_id) FROM reef_judgement j WHERE graph_id=$1),
      'merges',(SELECT jsonb_agg(to_jsonb(m) ORDER BY merge_id) FROM node_merges m WHERE graph_id=$1),
      'usage',(SELECT jsonb_agg(to_jsonb(u) ORDER BY id) FROM llm_usage u WHERE account_id=$2),
      'pending_usage',(SELECT jsonb_agg(to_jsonb(u) ORDER BY id) FROM llm_usage_pending u WHERE account_id=$2),
      'budgets',(SELECT jsonb_agg(to_jsonb(b) ORDER BY purpose,period_start) FROM managed_budget_periods b WHERE account_id=$2),
      'attempts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY receipt_id) FROM managed_attempt_receipts r WHERE account_id=$2),
      'reservations',(SELECT jsonb_agg(to_jsonb(r) ORDER BY receipt_id) FROM managed_credit_reservations r WHERE account_id=$2),
      'balances',(SELECT jsonb_agg(to_jsonb(b) ORDER BY account_id) FROM credit_balances b WHERE account_id=$2),
      'ledger',(SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM credit_ledger l WHERE account_id=$2),
      'other_jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM jobs j WHERE account_id=$2 AND ($3::uuid IS NULL OR id<>$3))
    )"#).bind(graph_id).bind(account_id).bind(excluded_job)
        .fetch_one(pool).await.expect("snapshot sweep fixture state")
}

/// Real typed queue admission now ADMITS the job (it is managed enrichment,
/// no longer dead-lettered), and the actual worker, asked to sweep with
/// `ENRICHMENT_PROCESSING_ENABLED` off, claims nothing. No private worker
/// implementation or flag bypass.
async fn run_sweep(
    pool: &PgPool,
    store: Arc<dyn Store>,
    graph_id: Uuid,
    account_id: Uuid,
    llm: &dyn LlmProvider,
) -> coralswarm_backend::workers::tenant_harness::Tick {
    let _switch = EnrichmentSwitchOff::new();
    let before = sweep_state(pool, graph_id, account_id, None).await;
    let job_id = queue::enqueue(pool, &JobKind::EntityResolution { graph_id }, Some(account_id), None, 0)
        .await.expect("enqueue entity-resolution job").expect("job id");
    let receipt: (String, String, Option<String>, i32, bool, Option<String>, Option<String>, serde_json::Value, Option<Uuid>) = sqlx::query_as(
        "SELECT kind,status::text,error_message,attempts,finished_at IS NOT NULL,failure_class,outcome,payload,account_id FROM jobs WHERE id=$1")
        .bind(job_id).fetch_one(pool).await.expect("read admission receipt");
    assert_eq!(receipt, ("EntityResolution".into(), "pending".into(), None, 0, false, None, None,
        serde_json::json!({"graph_id":graph_id}), Some(account_id)), "managed enrichment admits the job");
    let admitted: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(j) FROM jobs j WHERE id=$1")
        .bind(job_id).fetch_one(pool).await.expect("snapshot admission receipt");
    let wt = WorkerTenant { tenant: TenantId::Org("judgement-pr2-test-org".to_string()), pool: pool.clone(), store };
    let tick = entity_resolution::tick(&wt, pool, llm).await.expect("entity_resolution tick ok");
    assert_eq!(tick.processed, 0, "with enrichment switched off the worker claims nothing");
    let after: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(j) FROM jobs j WHERE id=$1")
        .bind(job_id).fetch_one(pool).await.expect("read unchanged pending job");
    assert_eq!(after, admitted, "an idle sweep must not claim, retry or complete the admitted job");
    assert_eq!(sweep_state(pool, graph_id, account_id, Some(job_id)).await, before,
        "switched-off enrichment must preserve retained inputs, judgements, merges, other jobs and named accounting sinks");
    sqlx::query("DELETE FROM jobs WHERE id=$1").bind(job_id).execute(pool).await.expect("drop the fixture job");
    tick
}

/// Pins `ENRICHMENT_PROCESSING_ENABLED` unset for one sweep (the default
/// production state until the switch is flipped); restored on drop.
struct EnrichmentSwitchOff(Option<String>);
impl EnrichmentSwitchOff {
    fn new() -> Self {
        let prev = std::env::var("ENRICHMENT_PROCESSING_ENABLED").ok();
        std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
        Self(prev)
    }
}
impl Drop for EnrichmentSwitchOff {
    fn drop(&mut self) {
        if let Some(v) = &self.0 {
            std::env::set_var("ENRICHMENT_PROCESSING_ENABLED", v);
        }
    }
}

fn tenant_for(pool: &PgPool) -> Tenant {
    Tenant {
        store: Some(Arc::new(PostgresStore::new(pool.clone()))),
        pool: Some(pool.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    }
}

fn user_ctx(user_id: Uuid, account_id: Uuid) -> UserContext {
    UserContext { user_id, account_id, role: "owner".to_string(), device_id: "test-device".to_string() }
}

// ── 1. Flag-on per-branch shapes ────────────────────────────────────────────
//
// Automatic identifier/LLM decisions and materialization run only with
// ENRICHMENT_PROCESSING_ENABLED on. Historical fixture shapes below cover
// admission with the switch off; reachable manual/store proofs remain
// separate, and the switched-on behaviour lives in the managed gate suite.

#[tokio::test]
async fn switched_off_er_flag_on_same_verdict_does_not_adjudicate_or_merge() {
    let Some(pool) = pool_or_skip("switched_off_er_flag_on_same_verdict_does_not_adjudicate_or_merge").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-llm-same").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Bob Baxter").await;
        let b = seed_node(&store, graph_id, "Bob Barnes").await;

        let llm = CountingLlm::new(r#"{"verdict":"same","confidence":0.95,"reason":"clearly the same person"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(any_judgement_row(&body_pool, graph_id, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, b, a).await.is_none());
        assert!(!not_same_contains(&body_pool, a, b).await);
    })
    .await;
}

#[tokio::test]
async fn switched_off_er_flag_on_different_verdict_does_not_write_or_dismiss() {
    let Some(pool) = pool_or_skip("switched_off_er_flag_on_different_verdict_does_not_write_or_dismiss").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-llm-diff").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Carol Carter").await;
        let b = seed_node(&store, graph_id, "Carol Chan").await;

        let llm = CountingLlm::new(r#"{"verdict":"different","confidence":0.9,"reason":"different people"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(any_judgement_row(&body_pool, graph_id, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, b, a).await.is_none());
        assert!(!not_same_contains(&body_pool, a, b).await);
    })
    .await;
}

#[tokio::test]
async fn switched_off_er_high_impact_unsure_does_not_write_a_judgement() {
    let Some(pool) = pool_or_skip("switched_off_er_high_impact_unsure_does_not_write_a_judgement").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-unsure-hi").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Dave Dolan").await;
        let b = seed_node(&store, graph_id, "Dave Dunbar").await;
        seed_atoms_for_impact(&body_pool, graph_id, a, 3).await;

        let llm = CountingLlm::new(r#"{"verdict":"unsure","confidence":0.6,"reason":"not sure"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(any_judgement_row(&body_pool, graph_id, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, b, a).await.is_none());
        assert!(!not_same_contains(&body_pool, a, b).await);
    })
    .await;
}

#[tokio::test]
async fn switched_off_er_low_confidence_fixture_stays_unchanged() {
    let Some(pool) = pool_or_skip("switched_off_er_low_confidence_fixture_stays_unchanged").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-unsure-lo").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Erin Ellis").await;
        let b = seed_node(&store, graph_id, "Erin Ewing").await;
        seed_atoms_for_impact(&body_pool, graph_id, a, 3).await;

        let llm = CountingLlm::new(r#"{"verdict":"unsure","confidence":0.4,"reason":"very unsure"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(any_judgement_row(&body_pool, graph_id, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, b, a).await.is_none());
        assert!(!not_same_contains(&body_pool, a, b).await);
    })
    .await;
}

// ── 2. Flag-off negative control ────────────────────────────────────────────

#[tokio::test]
async fn switched_off_er_flag_off_does_not_call_provider_or_merge() {
    let Some(pool) = pool_or_skip("switched_off_er_flag_off_does_not_call_provider_or_merge").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-flag-off").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::unset();
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Frank Foster").await;
        let b = seed_node(&store, graph_id, "Frank Fenton").await;

        let llm = CountingLlm::new(r#"{"verdict":"same","confidence":0.95,"reason":"clearly the same person"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(any_judgement_row(&body_pool, graph_id, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, b, a).await.is_none());
        assert!(!not_same_contains(&body_pool, a, b).await);
    })
    .await;
}

// ── 3. Switched-off enrichment with an existing human NEGATIVE ─────────────

#[tokio::test]
async fn switched_off_er_two_sweeps_preserve_existing_human_negative() {
    let Some(pool) = pool_or_skip("switched_off_er_two_sweeps_preserve_existing_human_negative").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-c3-human-neg").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Gina Gable").await;
        let b = seed_node(&store, graph_id, "Gina Grant").await;

        let human_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Human,
                confidence: None,
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({"card_id": "test"}),
                created_by: Some(user_id),
                expires_at: None,
            })
            .await
            .expect("write human negative");

        let llm = CountingLlm::new(r#"{"verdict":"same","confidence":0.95,"reason":"must never be called"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        let row = active_judgement_row(&body_pool, graph_id, a, b).await.expect("human row remains active");
        assert_eq!(row.judgement_id, human_id);
        assert_eq!(row.source, "human");
        assert_eq!(row.verdict, "negative");
    })
    .await;
}

// ── 4. Switched-off enrichment with a pending-positive fixture ──────────────

#[tokio::test]
async fn switched_off_er_preserves_pending_positive_without_materializing() {
    let Some(pool) = pool_or_skip("switched_off_er_preserves_pending_positive_without_materializing").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-crash-recover").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Zephyr Kowalski").await;
        let b = seed_node(&store, graph_id, "Marmot Higgins").await;

        let judgement_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Llm,
                confidence: Some(0.9),
                model_id: Some("stub-model".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "same", "reason": "pre-crash write"}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("write positive judgement directly");

        let llm = CountingLlm::new(r#"{"verdict":"unsure","confidence":0.01,"reason":"must never be called"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        let row = active_judgement_row(&body_pool, graph_id, a, b).await.expect("pending positive remains");
        assert_eq!(row.judgement_id, judgement_id);
        assert_eq!(row.verdict, "positive");
        assert!(node_merges_judgement_id(&body_pool, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, b, a).await.is_none());
    })
    .await;
}

// ── 5. Switched-off enrichment preserves stale-POSITIVE audit ────────────────

#[tokio::test]
async fn switched_off_er_preserves_stale_positive_audit_without_sweeping() {
    let Some(pool) = pool_or_skip("switched_off_er_preserves_stale_positive_audit_without_sweeping").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-stale-annul").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Yonder Blaze").await;
        let b = seed_node(&store, graph_id, "Rutabaga Finch").await;

        let judgement_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Llm,
                confidence: Some(0.9),
                model_id: Some("stub-model".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "same"}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("write positive judgement directly");

        sqlx::query("DELETE FROM nodes WHERE node_id = $1").bind(b).execute(&body_pool).await.expect("delete b");

        let llm = CountingLlm::new(r#"{"verdict":"unsure","confidence":0.01,"reason":"must never be called"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes the audit sweep");
        let (annulled_at, _, kind, reason) = annul_columns(&body_pool, judgement_id).await;
        assert!(annulled_at.is_none());
        assert!(kind.is_none());
        assert!(reason.is_none());
        assert!(node_exists(&body_pool, a).await);
        assert!(!node_exists(&body_pool, b).await);
    })
    .await;
}

// ── 6. Switched-off enrichment with existing UNSURE rows ───────────────────────

#[tokio::test]
async fn switched_off_er_preserves_twenty_five_unsure_rows_without_adjudicating() {
    let Some(pool) = pool_or_skip("switched_off_er_preserves_twenty_five_unsure_rows_without_adjudicating").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-unsure-cap").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());

        for i in 0..25u32 {
            let (x, y) = ordered_pair(
                seed_node(&store, graph_id, &format!("Unrelatedfillera{i}")).await,
                seed_node(&store, graph_id, &format!("Unrelatedfillerb{i}")).await,
            );
            sqlx::query(
                "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source, \
                    confidence, evidence) \
                 VALUES ($1, $2, $3, 'unsure', 'llm', 0.6, '{}'::jsonb)",
            )
            .bind(graph_id)
            .bind(x)
            .bind(y)
            .execute(&body_pool)
            .await
            .expect("seed unsure row");
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND verdict = 'unsure' \
                 AND annulled_at IS NULL"
            )
            .bind(graph_id)
            .fetch_one(&body_pool)
            .await
            .unwrap(),
            25
        );

        let a = seed_node(&store, graph_id, "Hank Holloway").await;
        let b = seed_node(&store, graph_id, "Hank Hutton").await;
        seed_atoms_for_impact(&body_pool, graph_id, a, 3).await;

        let llm = CountingLlm::new(r#"{"verdict":"unsure","confidence":0.6,"reason":"not sure"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(any_judgement_row(&body_pool, graph_id, a, b).await.is_none());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM reef_judgement WHERE graph_id=$1 AND verdict='unsure' AND annulled_at IS NULL")
            .bind(graph_id).fetch_one(&body_pool).await.expect("read preserved unsure count");
        assert_eq!(count, 25, "switched-off enrichment preserves the existing rows, not a cap-policy proof");
    })
    .await;
}

// ── 7. Back-link validation ──────────────────────────────────────────────────

#[tokio::test]
async fn back_link_validation_rejects_a_judgement_for_a_different_pair() {
    let Some(pool) = pool_or_skip("back_link_validation_rejects_a_judgement_for_a_different_pair").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-backlink-mismatch").await;
    // F14 (review round 2): seed the second account BEFORE `with_cleanup` and
    // register it in the accounts list, so ANY panic between here and the
    // final assertions (not just the ones after the old inline `cleanup()`
    // call used to sit) still reclaims user2/account2/graph2 — the old
    // pattern leaked all three into the shared test database on the exact
    // failure this test exists to catch (a regression in the wrong-graph
    // assertion below).
    let (user2, account2, graph2) = seed_graph(&pool, "pr2-backlink-mismatch-graph2").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id), (user2, account2)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let (x, y) = (seed_node(&store, graph_id, "Ivy Irwin").await, seed_node(&store, graph_id, "Ivy Ingram").await);
        let (keep, dup) = (seed_node(&store, graph_id, "Jack Jansen").await, seed_node(&store, graph_id, "Jack Judd").await);

        let judgement_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: x,
                node_b_id: y,
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
            .expect("write judgement for (x, y)");

        // Wrong pair, same graph.
        let err = store
            .merge_nodes_with_judgement(keep, dup, judgement_id)
            .await
            .expect_err("a judgement for a DIFFERENT pair must be refused");
        assert!(
            matches!(err, StoreError::JudgementMergeMismatch { judgement_id: jid } if jid == judgement_id),
            "expected JudgementMergeMismatch, got: {err}"
        );
        assert!(node_exists(&body_pool, keep).await, "no merge must have happened");
        assert!(node_exists(&body_pool, dup).await);
        assert!(
            node_merges_judgement_id(&body_pool, keep, dup).await.is_none(),
            "no node_merges row must have been written"
        );

        // Wrong graph entirely (graph2 seeded above, before `with_cleanup`).
        let store2 = PostgresStore::new(body_pool.clone());
        let (keep2, dup2) = (
            seed_node(&store2, graph2, "Karl Keegan").await,
            seed_node(&store2, graph2, "Karl Kinsey").await,
        );
        let err2 = store
            .merge_nodes_with_judgement(keep2, dup2, judgement_id)
            .await
            .expect_err("a judgement from a DIFFERENT graph must be refused");
        assert!(matches!(err2, StoreError::JudgementMergeMismatch { .. }), "expected JudgementMergeMismatch, got: {err2}");
        assert!(node_exists(&body_pool, keep2).await);
        assert!(node_exists(&body_pool, dup2).await);
    })
    .await;
}

// ── 8. Route coverage: merge_node / dismiss_merge_suggestion ───────────────

#[tokio::test]
async fn route_merge_node_flag_on_writes_human_positive_judgement() {
    let Some(pool) = pool_or_skip("route_merge_node_flag_on_writes_human_positive_judgement").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-route-merge-on").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let keep = seed_node(&store, graph_id, "Laura Lindqvist").await;
        let dup = seed_node(&store, graph_id, "Marco Moreau").await; // dissimilar on purpose — route bypasses blocking entirely

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) =
            merge_node(tenant, Extension(ctx.clone()), Path(keep), Json(MergeRequest { dup_id: dup })).await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);
        assert_eq!(response.0["ok"], serde_json::json!(true));

        let row = active_judgement_row(&body_pool, graph_id, keep, dup).await.expect("expected an active row");
        assert_eq!(row.verdict, "positive");
        assert_eq!(row.source, "human");
        assert_eq!(row.created_by, Some(user_id));
        assert_eq!(row.evidence["route"], serde_json::json!("manual_merge"));

        assert!(node_exists(&body_pool, keep).await);
        assert!(!node_exists(&body_pool, dup).await, "dup must have been merged away");
        let backlink = node_merges_judgement_id(&body_pool, keep, dup).await.expect("node_merges row must exist");
        assert_eq!(backlink, Some(row.judgement_id));
    })
    .await;
}

#[tokio::test]
async fn route_merge_node_flag_off_no_judgement_row_unchanged_response() {
    let Some(pool) = pool_or_skip("route_merge_node_flag_off_no_judgement_row_unchanged_response").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-route-merge-off").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::unset();
        let store = PostgresStore::new(body_pool.clone());
        let keep = seed_node(&store, graph_id, "Nadia Novak").await;
        let dup = seed_node(&store, graph_id, "Oscar Ostrander").await;

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) =
            merge_node(tenant, Extension(ctx), Path(keep), Json(MergeRequest { dup_id: dup })).await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);
        assert_eq!(response.0["ok"], serde_json::json!(true), "response shape must be unchanged");

        assert_eq!(reef_judgement_count(&body_pool, graph_id).await, 0);
        assert!(node_exists(&body_pool, keep).await);
        assert!(!node_exists(&body_pool, dup).await, "merge must still fire, unchanged");
        let backlink = node_merges_judgement_id(&body_pool, keep, dup).await.expect("node_merges row must exist");
        assert_eq!(backlink, None);
    })
    .await;
}

#[tokio::test]
async fn route_dismiss_merge_suggestion_flag_on_writes_human_negative_judgement() {
    let Some(pool) = pool_or_skip("route_dismiss_merge_suggestion_flag_on_writes_human_negative_judgement").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-route-dismiss-on").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Petra Palermo").await;
        let b = seed_node(&store, graph_id, "Quentin Quill").await;

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) = dismiss_merge_suggestion(
            tenant,
            Extension(ctx.clone()),
            Path(graph_id),
            Json(DismissMergeRequest { a, b }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);
        assert_eq!(response.0["ok"], serde_json::json!(true));

        let row = active_judgement_row(&body_pool, graph_id, a, b).await.expect("expected an active row");
        assert_eq!(row.verdict, "negative");
        assert_eq!(row.source, "human");
        assert_eq!(row.created_by, Some(user_id));
        assert_eq!(row.evidence["route"], serde_json::json!("dismiss_merge_suggestion"));

        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(not_same_contains(&body_pool, a, b).await, "legacy not_same dual-write must still fire");
    })
    .await;
}

#[tokio::test]
async fn route_dismiss_merge_suggestion_flag_off_no_judgement_row_unchanged_response() {
    let Some(pool) = pool_or_skip("route_dismiss_merge_suggestion_flag_off_no_judgement_row_unchanged_response").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-route-dismiss-off").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::unset();
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Rosa Reyes").await;
        let b = seed_node(&store, graph_id, "Stefan Sokolov").await;

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) =
            dismiss_merge_suggestion(tenant, Extension(ctx), Path(graph_id), Json(DismissMergeRequest { a, b })).await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);
        assert_eq!(response.0["ok"], serde_json::json!(true), "response shape must be unchanged");

        assert_eq!(reef_judgement_count(&body_pool, graph_id).await, 0);
        assert!(not_same_contains(&body_pool, a, b).await, "legacy dismissal must still fire, unchanged");
    })
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Review round 2 fix pass — F1/F2/F3/F4/F5/F7/F8/F12/F14.
// ═══════════════════════════════════════════════════════════════════════════

// ── F1: undo must annul the linked judgement (pulled forward from PR3) ────

#[tokio::test]
async fn undo_merge_annuls_linked_judgement_and_switched_off_sweep_preserves_it() {
    let Some(pool) =
        pool_or_skip("undo_merge_annuls_linked_judgement_and_switched_off_sweep_preserves_it").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f1-undo-annul").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        // Retain the historical dissimilar-title fixture. The real manual
        // merge/undo and linked-judgement annul checks remain active; the
        // later sweep proves switched-off admission, not pending-positive logic.
        let keep = seed_node(&store, graph_id, "Zeppelin Orchid").await;
        let dup = seed_node(&store, graph_id, "Gumdrop Falcon").await;

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) =
            merge_node(tenant, Extension(ctx.clone()), Path(keep), Json(MergeRequest { dup_id: dup })).await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);
        assert!(!node_exists(&body_pool, dup).await, "dup must have been merged away");

        let judgement_row =
            active_judgement_row(&body_pool, graph_id, keep, dup).await.expect("expected an active row post-merge");
        let judgement_id = judgement_row.judgement_id;
        assert_eq!(judgement_row.verdict, "positive");

        let merge_id = latest_merge_id(&body_pool, keep, dup).await;

        let (undo_status, undo_response) =
            undo_merge(tenant_for(&body_pool), Extension(ctx.clone()), Path(merge_id)).await;
        assert_eq!(undo_status, StatusCode::OK, "body: {:?}", undo_response.0);
        assert!(node_exists(&body_pool, dup).await, "undo must restore dup");
        assert!(node_exists(&body_pool, keep).await);

        // F1: the linked judgement must now be annulled, system/"unmerge".
        let annulled: ReefJudgementRow = sqlx::query_as(
            "SELECT judgement_id, verdict, source, confidence, model_id, prompt_version, evidence, \
                    created_by, expires_at, annulled_at, annulled_by_kind, annul_reason \
             FROM reef_judgement WHERE judgement_id = $1",
        )
        .bind(judgement_id)
        .fetch_one(&body_pool)
        .await
        .expect("judgement row must still exist (append-only)");
        assert!(annulled.annulled_at.is_some(), "F1: undo must annul the linked judgement");
        assert_eq!(annulled.annulled_by_kind.as_deref(), Some("system"));
        assert_eq!(annulled.annul_reason.as_deref(), Some("unmerge"));

        // The real undo/annul checks above remain reachable. A subsequent
        // switched-off sweep now proves an idle worker and unchanged state,
        // not pending-positive read-gate suppression.
        let llm = CountingLlm::new(r#"{"verdict":"different","confidence":0.9,"reason":"dissimilar titles never reach the LLM either way"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes lexical blocking");
        assert!(node_exists(&body_pool, keep).await);
        assert!(
            node_exists(&body_pool, dup).await,
            "F1: undo must remain effective — the pair must NOT have been silently re-merged"
        );
    })
    .await;
}

// ── PR3 (§5.4 scraps): undo-annul idempotency — the implementation shipped
//    in PR2's fix round (F1 above); these two tests pin the no-op contracts
//    it leans on. ──────────────────────────────────────────────────────────

/// A repeated undo of the SAME merge is a clean no-op through BOTH twins:
/// undo #2 calls the plain `unmerge_nodes` DIRECTLY (the route always runs
/// the `_scoped` twin via `begin_scoped_tx!` — `tenant_for` sets `pool:
/// Some(..)` — so only a direct call executes the unscoped fetch guard and
/// its annul path), and undo #3 repeats through the route for the scoped
/// twin. In both, the `AND undone_at IS NULL FOR UPDATE` fetch is the guard:
/// a missing/undone row commits and returns `Ok` before any restore or annul
/// work, so the judgement's annul columns, the merge's `undone_at` audit
/// timestamp, and the judgement-row count all stay byte-identical.
#[tokio::test]
async fn second_undo_of_same_merge_is_a_noop_and_does_not_clobber_annul_metadata() {
    let Some(pool) =
        pool_or_skip("second_undo_of_same_merge_is_a_noop_and_does_not_clobber_annul_metadata").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr3-double-undo").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let keep = seed_node(&store, graph_id, "Quixotic Lantern").await;
        let dup = seed_node(&store, graph_id, "Barnaby Thistle").await;

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) =
            merge_node(tenant, Extension(ctx.clone()), Path(keep), Json(MergeRequest { dup_id: dup })).await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);
        assert!(!node_exists(&body_pool, dup).await, "dup must have been merged away");

        let judgement_id = active_judgement_row(&body_pool, graph_id, keep, dup)
            .await
            .expect("expected an active row post-merge")
            .judgement_id;
        let merge_id = latest_merge_id(&body_pool, keep, dup).await;
        let rows_after_merge = reef_judgement_count(&body_pool, graph_id).await;

        // Undo #1 (route → scoped twin): does the real restore + annul.
        let (undo_status, undo_response) =
            undo_merge(tenant_for(&body_pool), Extension(ctx.clone()), Path(merge_id)).await;
        assert_eq!(undo_status, StatusCode::OK, "body: {:?}", undo_response.0);
        assert!(node_exists(&body_pool, dup).await, "first undo must restore dup");

        // Snapshot what the first undo stamped (system/"unmerge", no user).
        let first = annul_columns(&body_pool, judgement_id).await;
        assert!(first.0.is_some(), "first undo must have annulled the judgement");
        assert_eq!(first.1, None, "the undo's system annul carries no annulled_by user");
        assert_eq!(first.2.as_deref(), Some("system"));
        assert_eq!(first.3.as_deref(), Some("unmerge"));

        // `undone_at` is the audit timestamp of when the undo happened; the
        // `AND undone_at IS NULL` fetch guard is what keeps a repeated undo
        // from re-running the restore and re-stamping it.
        let first_undone_at = undone_at(&body_pool, merge_id).await;
        assert!(first_undone_at.is_some(), "first undo must stamp undone_at");

        // Undo #2: the PLAIN `unmerge_nodes`, called directly — the unscoped
        // twin's guard + annul path (owner/BYPASSRLS pool, so no RLS in play).
        store
            .unmerge_nodes(merge_id)
            .await
            .expect("second (plain unscoped) undo must be a clean no-op");
        assert!(node_exists(&body_pool, dup).await, "dup must still be restored");
        assert!(node_exists(&body_pool, keep).await);
        assert_eq!(
            annul_columns(&body_pool, judgement_id).await,
            first,
            "the plain no-op undo must not touch the annul columns"
        );
        assert_eq!(
            undone_at(&body_pool, merge_id).await,
            first_undone_at,
            "the plain no-op undo must not re-stamp undone_at (its fetch guard must short-circuit)"
        );

        // Undo #3: the route again — the same no-op contract through the
        // scoped twin.
        let (again_status, again_response) =
            undo_merge(tenant_for(&body_pool), Extension(ctx), Path(merge_id)).await;
        assert_eq!(
            again_status,
            StatusCode::OK,
            "repeat route undo must be a clean no-op; body: {:?}",
            again_response.0
        );
        assert!(node_exists(&body_pool, dup).await, "dup must still be restored");
        assert_eq!(
            annul_columns(&body_pool, judgement_id).await,
            first,
            "the scoped no-op undo must not touch the annul columns"
        );
        assert_eq!(
            undone_at(&body_pool, merge_id).await,
            first_undone_at,
            "the scoped no-op undo must not re-stamp undone_at"
        );

        assert_eq!(
            reef_judgement_count(&body_pool, graph_id).await,
            rows_after_merge,
            "no undo may add or remove judgement rows (annul mutates in place, append-only table)"
        );
    })
    .await;
}

/// Undoing a merge whose linked judgement was ALREADY annulled by something
/// else (here: a direct human annul) still restores fully, and the original
/// annul metadata survives — `annul_judgement_tx`'s already-annulled branch
/// returns `AlreadyAnnulled` BEFORE the UPDATE, so the unmerge's
/// system/"unmerge" stamp never overwrites the earlier human/"test-preannul".
#[tokio::test]
async fn undo_merge_with_pre_annulled_judgement_restores_without_clobbering_annul_metadata() {
    let Some(pool) = pool_or_skip(
        "undo_merge_with_pre_annulled_judgement_restores_without_clobbering_annul_metadata",
    )
    .await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr3-preannul-undo").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let keep = seed_node(&store, graph_id, "Meridian Cobble").await;
        let dup = seed_node(&store, graph_id, "Halcyon Verge").await;

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) =
            merge_node(tenant, Extension(ctx.clone()), Path(keep), Json(MergeRequest { dup_id: dup })).await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);

        assert!(!node_exists(&body_pool, dup).await, "dup must have been merged away");

        let judgement_id = active_judgement_row(&body_pool, graph_id, keep, dup)
            .await
            .expect("expected an active row post-merge")
            .judgement_id;
        let merge_id = latest_merge_id(&body_pool, keep, dup).await;
        let rows_after_merge = reef_judgement_count(&body_pool, graph_id).await;

        // Annul the judgement directly FIRST, with deliberately different
        // by/kind/reason than the undo path writes — `annulled_by =
        // Some(user_id)` is the column the undo's system annul (`None`)
        // would visibly clobber.
        let outcome = store
            .annul_judgement(judgement_id, Some(user_id), AnnulKind::Human, "test-preannul")
            .await
            .expect("direct annul");
        assert_eq!(outcome, coralswarm_backend::store::AnnulOutcome::Annulled);

        let pre = annul_columns(&body_pool, judgement_id).await;
        assert!(pre.0.is_some(), "direct annul must stamp annulled_at");
        assert_eq!(pre.1, Some(user_id), "human annul must record who annulled");
        assert_eq!(pre.2.as_deref(), Some("human"));
        assert_eq!(pre.3.as_deref(), Some("test-preannul"));

        // Undo must still fully restore, and must NOT clobber the earlier annul.
        let (undo_status, undo_response) =
            undo_merge(tenant_for(&body_pool), Extension(ctx), Path(merge_id)).await;
        assert_eq!(undo_status, StatusCode::OK, "body: {:?}", undo_response.0);
        assert!(node_exists(&body_pool, dup).await, "undo must restore dup despite the pre-annul");
        assert!(node_exists(&body_pool, keep).await);
        assert!(
            undone_at(&body_pool, merge_id).await.is_some(),
            "the undo must stamp undone_at — the restore really ran, pre-annul notwithstanding"
        );

        assert_eq!(
            annul_columns(&body_pool, judgement_id).await,
            pre,
            "undo must not overwrite the pre-existing annul (AlreadyAnnulled early-return)"
        );
        assert_eq!(
            reef_judgement_count(&body_pool, graph_id).await,
            rows_after_merge,
            "the undo may not add or remove judgement rows"
        );
    })
    .await;
}

// ── F2: dedup sweep must not merge past an active NEGATIVE ────────────────

#[tokio::test]
async fn dedup_entities_skips_a_pair_with_an_active_negative_judgement() {
    let Some(pool) = pool_or_skip("dedup_entities_skips_a_pair_with_an_active_negative_judgement").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f2-dedup-negative").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        // `dedup_entities`' own clustering (`same_entity`: exact match OR
        // edit-distance <= 1 on names >= 6 chars) puts these in the same
        // cluster. NOT byte-identical titles: `upsert_node` upserts on
        // `(graph_id, type, lower(title))`, so two `seed_node` calls with
        // the IDENTICAL title would silently collapse to the SAME node,
        // not two distinct ones.
        let a = seed_node(&store, graph_id, "Fennimore Wisp").await;
        let b = seed_node(&store, graph_id, "Fennimore Wisk").await;
        seed_review_title_support(&body_pool, graph_id, a).await;
        seed_review_title_support(&body_pool, graph_id, b).await;

        store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Llm,
                confidence: Some(0.9),
                model_id: Some("stub-model".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "different"}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("write an active NEGATIVE for this pair (any source counts, F2)");

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) = dedup_entities(tenant, Extension(ctx), Path(graph_id)).await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);
        assert_eq!(
            response.0["reefs_removed"],
            serde_json::json!(0),
            "the pair must be skipped, not merged, because of the active NEGATIVE"
        );
        assert_eq!(response.0["merged_groups"], serde_json::json!(1),
            "the supported manual route must see the real candidate cluster before suppressing its merge");
        assert!(node_exists(&body_pool, a).await, "F2: dedup must not merge past an active NEGATIVE");
        assert!(node_exists(&body_pool, b).await);
    })
    .await;
}

// ── F3: back-link validation refuses an ANNULLED or NEGATIVE judgement ────

#[tokio::test]
async fn back_link_validation_rejects_an_annulled_or_negative_judgement() {
    let Some(pool) = pool_or_skip("back_link_validation_rejects_an_annulled_or_negative_judgement").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f3-backlink-inactive").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());

        // Case 1: an ANNULLED positive judgement must be refused — Materialize's
        // whole authority is an ACTIVE POSITIVE, and time passes between a
        // gate read and a merge call (the exact race F3 closes).
        let (a, b) = (seed_node(&store, graph_id, "Uma Ulloa").await, seed_node(&store, graph_id, "Uma Uribe").await);
        let jid1 = store
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
            .expect("write positive judgement for (a, b)");
        store
            .annul_judgement(jid1, Some(user_id), AnnulKind::Human, "test-dismiss")
            .await
            .expect("annul it (simulating a dismissal landing before materialization)");

        let err1 = store
            .merge_nodes_with_judgement(a, b, jid1)
            .await
            .expect_err("an ANNULLED judgement must be refused");
        assert!(
            matches!(err1, StoreError::JudgementMergeMismatch { judgement_id } if judgement_id == jid1),
            "expected JudgementMergeMismatch, got: {err1}"
        );
        assert!(node_exists(&body_pool, a).await, "no merge must have happened");
        assert!(node_exists(&body_pool, b).await);
        assert!(node_merges_judgement_id(&body_pool, a, b).await.is_none());

        // Case 2: a NEGATIVE (never-annulled, fully "active" by annulled_at
        // alone) judgement must ALSO be refused.
        let (c, d) =
            (seed_node(&store, graph_id, "Victor Voss").await, seed_node(&store, graph_id, "Victor Vance").await);
        let jid2 = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: c,
                node_b_id: d,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Human,
                confidence: None,
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({}),
                created_by: Some(user_id),
                expires_at: None,
            })
            .await
            .expect("write negative judgement for (c, d)");
        let err2 = store
            .merge_nodes_with_judgement(c, d, jid2)
            .await
            .expect_err("a NEGATIVE judgement must be refused (not just an annulled one)");
        assert!(
            matches!(err2, StoreError::JudgementMergeMismatch { judgement_id } if judgement_id == jid2),
            "expected JudgementMergeMismatch, got: {err2}"
        );
        assert!(node_exists(&body_pool, c).await);
        assert!(node_exists(&body_pool, d).await);
    })
    .await;
}

// ── F4: stale-pair dismiss must not 500 or roll back the legacy dismissal ──

#[tokio::test]
async fn route_dismiss_merge_suggestion_stale_pair_returns_200_keeps_legacy_dismissal() {
    let Some(pool) =
        pool_or_skip("route_dismiss_merge_suggestion_stale_pair_returns_200_keeps_legacy_dismissal").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f4-stale-dismiss").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Wendell Yates").await;
        let b = seed_node(&store, graph_id, "Xiomara Zettel").await;
        // Ensure the SURVIVING node is the lower uuid, so the legacy
        // `not_same` write (which lands on the lower-id node's `body`) can
        // actually be observed after the endpoint goes stale.
        let (lo, hi) = ordered_pair(a, b);
        store.merge_nodes(lo, hi).await.expect("pre-merge one endpoint away, simulating a worker merge \
             that ran after the suggestion was listed but before the user clicked dismiss");
        assert!(node_exists(&body_pool, lo).await);
        assert!(!node_exists(&body_pool, hi).await, "the suggestion is now stale");

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) =
            dismiss_merge_suggestion(tenant, Extension(ctx), Path(graph_id), Json(DismissMergeRequest { a, b })).await;
        assert_eq!(status, StatusCode::OK, "F4: a stale pair must return 200, not 500 — body: {:?}", response.0);
        assert_eq!(response.0["ok"], serde_json::json!(true));

        assert!(
            not_same_contains(&body_pool, a, b).await,
            "F4: the legacy not_same dismissal must survive the judgement dual-write being skipped"
        );
        assert_eq!(
            reef_judgement_count(&body_pool, graph_id).await,
            0,
            "no judgement row should exist for a pair whose endpoint check refused"
        );
    })
    .await;
}

#[tokio::test]
async fn route_dismiss_merge_suggestion_a_equals_b_does_not_500() {
    let Some(pool) = pool_or_skip("route_dismiss_merge_suggestion_a_equals_b_does_not_500").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f4-a-eq-b").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let x = seed_node(&store, graph_id, "Yusuf Yang").await;

        let tenant = tenant_for(&body_pool);
        let ctx = user_ctx(user_id, account_id);
        let (status, response) =
            dismiss_merge_suggestion(tenant, Extension(ctx), Path(graph_id), Json(DismissMergeRequest { a: x, b: x })).await;
        assert_eq!(status, StatusCode::OK, "F4: a==b must not 500 — body: {:?}", response.0);
        assert_eq!(response.0["ok"], serde_json::json!(true));
        assert_eq!(reef_judgement_count(&body_pool, graph_id).await, 0);
    })
    .await;
}

// ── Historical F5 race fixture: provider entry is now refused ──────────────

#[tokio::test]
async fn switched_off_er_does_not_enter_the_human_race_provider() {
    let Some(pool) = pool_or_skip("switched_off_er_does_not_enter_the_human_race_provider").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f5-human-race").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Alfredo Ackerman").await;
        let b = seed_node(&store, graph_id, "Alfredo Abernathy").await;

        let llm = HumanRaceLlm {
            pool: body_pool.clone(),
            graph_id,
            a,
            b,
            body: r#"{"verdict":"different","confidence":0.9,"reason":"looks different by name alone"}"#.to_string(),
            injected: std::sync::atomic::AtomicBool::new(false),
        };
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert!(!llm.injected.load(Ordering::SeqCst), "switched-off enrichment must never enter the race provider");
        assert!(!not_same_contains(&body_pool, a, b).await);
        assert!(any_judgement_row(&body_pool, graph_id, a, b).await.is_none());
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
    })
    .await;
}

// ── Historical F7 fixtures: no automatic annul or supersession ──────────────

#[tokio::test]
async fn switched_off_er_preserves_dead_endpoint_unsure_without_sweeping() {
    let Some(pool) = pool_or_skip("switched_off_er_preserves_dead_endpoint_unsure_without_sweeping").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f7-unsure-dead-endpoint").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Beatrix Bellweather").await;
        let b = seed_node(&store, graph_id, "Corwin Castellan").await;

        let judgement_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Unsure,
                source: JudgementSource::Llm,
                confidence: Some(0.6),
                model_id: Some("stub-model".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "unsure"}),
                created_by: None,
                expires_at: Some(chrono::Utc::now() + chrono::Duration::days(30)),
            })
            .await
            .expect("write active unsure judgement");

        sqlx::query("DELETE FROM nodes WHERE node_id = $1").bind(b).execute(&body_pool).await.expect("delete b");

        let llm = CountingLlm::new(r#"{"verdict":"unsure","confidence":0.01,"reason":"must never be called"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes the audit sweep");
        let (annulled_at, _, kind, reason) = annul_columns(&body_pool, judgement_id).await;
        assert!(annulled_at.is_none());
        assert!(kind.is_none());
        assert!(reason.is_none());
        assert!(node_exists(&body_pool, a).await);
        assert!(!node_exists(&body_pool, b).await);
    })
    .await;
}

#[tokio::test]
async fn switched_off_er_preserves_expired_own_row_at_cap() {
    let Some(pool) = pool_or_skip("switched_off_er_preserves_expired_own_row_at_cap").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f7-cap-neutral").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Ignatius Larkspur").await;
        let b = seed_node(&store, graph_id, "Ignatius Larsen").await;
        seed_atoms_for_impact(&body_pool, graph_id, a, 3).await;

        for i in 0..24u32 {
            let (x, y) = ordered_pair(
                seed_node(&store, graph_id, &format!("Unrelatedfillerc{i}")).await,
                seed_node(&store, graph_id, &format!("Unrelatedfillerd{i}")).await,
            );
            sqlx::query(
                "INSERT INTO reef_judgement (graph_id, node_a_id, node_b_id, verdict, source, \
                    confidence, evidence) \
                 VALUES ($1, $2, $3, 'unsure', 'llm', 0.6, '{}'::jsonb)",
            )
            .bind(graph_id)
            .bind(x)
            .bind(y)
            .execute(&body_pool)
            .await
            .expect("seed unsure row");
        }
        let own_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Unsure,
                source: JudgementSource::Llm,
                confidence: Some(0.55),
                model_id: Some("stub-model".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "unsure"}),
                created_by: None,
                expires_at: Some(chrono::Utc::now() - chrono::Duration::days(1)),
            })
            .await
            .expect("write the pair's own expired-but-active unsure row");

        let count_before: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_judgement WHERE graph_id = $1 AND verdict = 'unsure' \
             AND annulled_at IS NULL",
        )
        .bind(graph_id)
        .fetch_one(&body_pool)
        .await
        .unwrap();
        assert_eq!(count_before, 25, "must be exactly at cap before the sweep");

        let llm = CountingLlm::new(r#"{"verdict":"unsure","confidence":0.65,"reason":"still unsure, re-adjudicated"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        let (annulled_at, _, _, _) = annul_columns(&body_pool, own_id).await;
        assert!(annulled_at.is_none(), "switched-off enrichment must not supersede the expired row");
        let count_after: i64 = sqlx::query_scalar("SELECT count(*) FROM reef_judgement WHERE graph_id=$1 AND verdict='unsure' AND annulled_at IS NULL")
            .bind(graph_id).fetch_one(&body_pool).await.expect("read preserved count");
        assert_eq!(count_after, count_before);
    })
    .await;
}

// ── Historical F8 fixture: switched-off enrichment preserves the conflict ───────

#[tokio::test]
async fn switched_off_er_preserves_dismissed_pending_positive_without_sweeping() {
    let Some(pool) = pool_or_skip("switched_off_er_preserves_dismissed_pending_positive_without_sweeping").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f8-dismissed-pending").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Marzipan Quokka").await;
        let b = seed_node(&store, graph_id, "Vellichor Ibis").await;

        let judgement_id = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: a,
                node_b_id: b,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Llm,
                confidence: Some(0.9),
                model_id: Some("stub-model".to_string()),
                prompt_version: Some("resolve_entities_v1".to_string()),
                evidence: serde_json::json!({"llm_verdict": "same"}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("write a pending positive judgement (simulating a crash-window write)");

        store.insert_merge_dismissal(graph_id, a, b).await.expect("seed a legacy dismissal for the same pair");
        assert!(not_same_contains(&body_pool, a, b).await, "sanity: the legacy dismissal must be recorded");

        let llm = CountingLlm::new(r#"{"verdict":"unsure","confidence":0.01,"reason":"must never be called"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes the audit sweep");
        let (annulled_at, _, kind, reason) = annul_columns(&body_pool, judgement_id).await;
        assert!(annulled_at.is_none());
        assert!(kind.is_none());
        assert!(reason.is_none());
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(not_same_contains(&body_pool, a, b).await);
    })
    .await;
}

// ── Log mode cannot override Switched-off enrichment ────────────────────────

#[tokio::test]
async fn switched_off_er_log_mode_does_not_call_provider_or_merge() {
    let Some(pool) = pool_or_skip("switched_off_er_log_mode_does_not_call_provider_or_merge").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr2-f12-log-mode").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Preserve this historical fixture while testing the current switched-off boundary.
        let _flag = FlagGuard::set("log");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Ulric Underhill").await;
        let b = seed_node(&store, graph_id, "Ulric Uzumaki").await;

        let llm = CountingLlm::new(r#"{"verdict":"same","confidence":0.95,"reason":"clearly the same person"}"#);
        run_sweep(&body_pool, Arc::new(PostgresStore::new(body_pool.clone())), graph_id, account_id, &llm).await;
        assert_eq!(llm.call_count(), 0, "switched-off enrichment precedes adjudication");
        assert!(node_exists(&body_pool, a).await);
        assert!(node_exists(&body_pool, b).await);
        assert!(any_judgement_row(&body_pool, graph_id, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, a, b).await.is_none());
        assert!(node_merges_judgement_id(&body_pool, b, a).await.is_none());
        assert!(!not_same_contains(&body_pool, a, b).await);
    })
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// PR4 — the /review merge-review surface (design doc §6).
//
// The UNSURE rows PR2 taught the worker to write are only worth writing if a
// human can resolve them. These tests drive the REAL `list_merge_review` and
// `review_count` handlers plus the two EXISTING resolution routes, against a
// real `reef_judgement` table.
//
// Everything here depends on the process-global `JUDGEMENT_GRAPH_WRITES`, so
// it obeys this file's `--test-threads=1` + `FlagGuard` convention.
// ═══════════════════════════════════════════════════════════════════════════

/// Literal synthetic evidence for current Library title admission. Existing
/// nonempty impact-link counts stay exact; bare endpoints receive one real
/// unmapped legacy coral. No semantic identity or captured origin is invented.
async fn seed_review_title_support(pool: &PgPool, graph_id: Uuid, node_id: Uuid) {
    let title: String = sqlx::query_scalar("SELECT title FROM nodes WHERE graph_id=$1 AND node_id=$2")
        .bind(graph_id).bind(node_id).fetch_one(pool).await.expect("read fixture title");
    let count_before: i64 = sqlx::query_scalar("SELECT count(*) FROM node_atoms WHERE node_id=$1")
        .bind(node_id).fetch_one(pool).await.expect("read original link count");
    let existing: Option<Uuid> = sqlx::query_scalar("SELECT a.atom_id FROM node_atoms l JOIN atoms a ON a.atom_id=l.atom_id AND a.graph_id=$1 WHERE l.node_id=$2 ORDER BY a.atom_id LIMIT 1")
        .bind(graph_id).bind(node_id).fetch_optional(pool).await.expect("find actual fixture coral");
    let literal = format!("Synthetic merge-review fixture evidence names {title}.");
    if let Some(atom_id) = existing {
        let changed = sqlx::query("UPDATE atoms SET content_inline=$3 WHERE graph_id=$1 AND atom_id=$2 AND (content_inline IS NULL OR content_inline=$3) AND NOT source_evidence_bound")
            .bind(graph_id).bind(atom_id).bind(&literal).execute(pool).await.expect("attach literal fixture context");
        assert_eq!(changed.rows_affected(), 1, "only an actual empty or identical unmapped fixture coral may be refined");
    } else {
        let atom_id: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_inline,content_blob_key,timestamp) VALUES($1,'note',$2,$3,'unused',now()) RETURNING atom_id")
            .bind(graph_id).bind(format!("merge-review-literal-{node_id}")).bind(&literal)
            .fetch_one(pool).await.expect("insert literal fixture evidence");
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(node_id).bind(atom_id).execute(pool).await.expect("link literal fixture evidence");
    }
    let admitted: Option<String> = sqlx::query_scalar("SELECT library_node_title($1,$2)")
        .bind(graph_id).bind(node_id).fetch_one(pool).await.expect("verify real title admission");
    assert_eq!(admitted.as_deref(), Some(title.as_str()));
    let count_after: i64 = sqlx::query_scalar("SELECT count(*) FROM node_atoms WHERE node_id=$1")
        .bind(node_id).fetch_one(pool).await.expect("read supported link count");
    assert_eq!(count_after, count_before.max(1));
}

/// One LIVE UNSURE row for a pair, shaped like the ER worker's §5.3 band
/// (source `llm`, mid-band confidence, a TTL). Returns the judgement id.
async fn seed_unsure(
    store: &PostgresStore,
    pool: &PgPool,
    graph_id: Uuid,
    a: Uuid,
    b: Uuid,
    confidence: f64,
    ttl_days: i64,
) -> Uuid {
    seed_review_title_support(pool, graph_id, a).await;
    seed_review_title_support(pool, graph_id, b).await;
    store
        .write_judgement(NewJudgement {
            graph_id,
            node_a_id: a,
            node_b_id: b,
            verdict: JudgementVerdict::Unsure,
            source: JudgementSource::Llm,
            confidence: Some(confidence),
            model_id: Some("stub-model".to_string()),
            prompt_version: Some("v1".to_string()),
            evidence: serde_json::json!({"seed": "pr4"}),
            created_by: None,
            expires_at: Some(chrono::Utc::now() + chrono::Duration::days(ttl_days)),
        })
        .await
        .expect("seed UNSURE judgement")
}

/// Pin a judgement's `created_at` so queue ORDER is deterministic rather than
/// dependent on how fast two consecutive transactions run.
async fn backdate_judgement(pool: &PgPool, judgement_id: Uuid, seconds_ago: i64) {
    sqlx::query(
        "UPDATE reef_judgement SET created_at = now() - make_interval(secs => $2::double precision) \
         WHERE judgement_id = $1",
    )
    .bind(judgement_id)
    .bind(seconds_ago as f64)
    .execute(pool)
    .await
    .expect("backdate judgement");
}

/// Force a row's `expires_at` into the past — the "expired but nothing has
/// annulled it yet" state `write_judgement` only cleans up on the NEXT write
/// to that pair, and which the review queue must therefore filter at read
/// time.
async fn expire_judgement(pool: &PgPool, judgement_id: Uuid) {
    sqlx::query("UPDATE reef_judgement SET expires_at = now() - interval '1 hour' WHERE judgement_id = $1")
        .bind(judgement_id)
        .execute(pool)
        .await
        .expect("expire judgement");
}

async fn superseded_by(pool: &PgPool, judgement_id: Uuid) -> Option<Uuid> {
    sqlx::query_scalar("SELECT superseded_by FROM reef_judgement WHERE judgement_id = $1")
        .bind(judgement_id)
        .fetch_one(pool)
        .await
        .expect("judgement row must still exist (append-only)")
}

/// `list_merge_review` through the real handler, returning the JSON body.
async fn merge_review_page(
    pool: &PgPool,
    user_id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
    limit: Option<i64>,
    cursor: Option<String>,
) -> (StatusCode, serde_json::Value) {
    let (status, body) = list_merge_review(
        tenant_for(pool),
        Extension(user_ctx(user_id, account_id)),
        Path(graph_id),
        Query(ListMergeReviewQuery { limit, cursor }),
    )
    .await;
    (status, body.0)
}

fn item_judgement_ids(body: &serde_json::Value) -> Vec<Uuid> {
    body["items"]
        .as_array()
        .expect("items must be an array")
        .iter()
        .map(|i| Uuid::parse_str(i["judgement_id"].as_str().expect("judgement_id string")).unwrap())
        .collect()
}

// ── a) live-only, queue order, keyset pagination ──────────────────────────

#[tokio::test]
async fn merge_review_lists_only_live_unsure_oldest_first_and_paginates() {
    let Some(pool) = pool_or_skip("merge_review_lists_only_live_unsure_oldest_first_and_paginates").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-list").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());

        // Three live UNSURE pairs, explicitly aged oldest → newest.
        let mut live: Vec<Uuid> = Vec::new();
        for (i, (x, y)) in [("Ada Aster", "Ada Astor"), ("Bo Brandt", "Bo Brandtt"), ("Cy Cortez", "Cy Cortes")]
            .into_iter()
            .enumerate()
        {
            let a = seed_node(&store, graph_id, x).await;
            let b = seed_node(&store, graph_id, y).await;
            seed_atoms_for_impact(&body_pool, graph_id, a, 4 + i).await;
            seed_atoms_for_impact(&body_pool, graph_id, b, 1).await;
            let j = seed_unsure(&store, &body_pool, graph_id, a, b, 0.7, 30).await;
            // 300s, 200s, 100s ago → index 0 is the oldest.
            backdate_judgement(&body_pool, j, 300 - 100 * i as i64).await;
            live.push(j);
        }

        // An ANNULLED UNSURE row.
        let da = seed_node(&store, graph_id, "Dev Dunn").await;
        let db = seed_node(&store, graph_id, "Dev Dunne").await;
        let annulled = seed_unsure(&store, &body_pool, graph_id, da, db, 0.7, 30).await;
        store
            .annul_judgement(annulled, None, AnnulKind::System, "pr4-test")
            .await
            .expect("annul");

        // An EXPIRED-but-unannulled UNSURE row.
        let ea = seed_node(&store, graph_id, "Eli Ebert").await;
        let eb = seed_node(&store, graph_id, "Eli Ebbert").await;
        let expired = seed_unsure(&store, &body_pool, graph_id, ea, eb, 0.7, 30).await;
        expire_judgement(&body_pool, expired).await;

        // A LIVE UNSURE row whose reef has since been deleted. `reef_judgement`
        // carries no endpoint FK by design, so the row survives its own
        // subject; a half-rendered card is worse than no card. The current
        // A switched-off worker does not perform an automatic annul sweep.
        // NOTE: the second title used to be "Ken Kruse " (trailing space) —
        // the identity-resolution door now folds whitespace/case variants
        // into the same reef at mint time (design doc
        // identity-resolution-at-the-door.md §4.1, step 3), so that pair
        // would come back as ONE node and `seed_unsure` would refuse it.
        // A typo-distance variant stays a separate reef (the door leaves
        // spelling to ER), which is all this fixture needs: two live nodes,
        // one of which is deleted below.
        let ka = seed_node(&store, graph_id, "Ken Kruse").await;
        let kb = seed_node(&store, graph_id, "Ken Kruze").await;
        let dead_endpoint = seed_unsure(&store, &body_pool, graph_id, ka, kb, 0.7, 30).await;
        // Newest of the five, so it sorts onto the LAST page — where dropping
        // it makes that page come back short, which is the shape the skip is
        // supposed to produce (the cursor tracks rows fetched, not rows
        // rendered, so the next page can never skip a live row).
        backdate_judgement(&body_pool, dead_endpoint, 50).await;
        sqlx::query("DELETE FROM nodes WHERE node_id = $1")
            .bind(kb)
            .execute(&body_pool)
            .await
            .expect("delete one endpoint");

        // A POSITIVE and a NEGATIVE — other verdicts are not review items.
        let pa = seed_node(&store, graph_id, "Fay Fisk").await;
        let pb = seed_node(&store, graph_id, "Fay Fiske").await;
        let positive = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: pa,
                node_b_id: pb,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Llm,
                confidence: Some(0.97),
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("seed positive");
        let na = seed_node(&store, graph_id, "Gus Grant").await;
        let nb = seed_node(&store, graph_id, "Gus Grantt").await;
        let negative = store
            .write_judgement(NewJudgement {
                graph_id,
                node_a_id: na,
                node_b_id: nb,
                verdict: JudgementVerdict::Negative,
                source: JudgementSource::Llm,
                confidence: Some(0.96),
                model_id: None,
                prompt_version: None,
                evidence: serde_json::json!({}),
                created_by: None,
                expires_at: None,
            })
            .await
            .expect("seed negative");

        // ── Page 1 of 2 ──────────────────────────────────────────────────
        let (status, page1) = merge_review_page(&body_pool, user_id, account_id, graph_id, Some(2), None).await;
        assert_eq!(status, StatusCode::OK, "body: {page1:?}");
        assert_eq!(
            item_judgement_ids(&page1),
            vec![live[0], live[1]],
            "the queue is OLDEST FIRST and excludes annulled/expired/positive/negative"
        );
        assert_eq!(page1["has_more"], serde_json::json!(true));
        let cursor = page1["next_cursor"].as_str().expect("next_cursor on a full page").to_string();

        // Card shape, on the oldest row.
        let card = &page1["items"][0];
        let a_id = Uuid::parse_str(card["a"]["node_id"].as_str().unwrap()).unwrap();
        let b_id = Uuid::parse_str(card["b"]["node_id"].as_str().unwrap()).unwrap();
        assert!(a_id < b_id, "endpoints come back in the table's canonical order");
        assert!(card["a"]["title"].as_str().is_some_and(|t| t.starts_with("Ada")));
        assert!(card["b"]["title"].as_str().is_some_and(|t| t.starts_with("Ada")));
        let counts = [card["a"]["atom_count"].as_i64().unwrap(), card["b"]["atom_count"].as_i64().unwrap()];
        assert!(counts.contains(&4) && counts.contains(&1), "coral counts ride the card: {counts:?}");
        assert_eq!(card["confidence"], serde_json::json!(0.7));
        assert!(card["created_at"].as_str().is_some());
        assert!(card["expires_at"].as_str().is_some(), "an UNSURE row always carries its TTL");

        // ── Page 2, via the cursor ───────────────────────────────────────
        let (status, page2) =
            merge_review_page(&body_pool, user_id, account_id, graph_id, Some(2), Some(cursor)).await;
        assert_eq!(status, StatusCode::OK, "body: {page2:?}");
        assert_eq!(
            item_judgement_ids(&page2),
            vec![live[2]],
            "the cursor resumes after the first page, and the dead-endpoint row is dropped \
             rather than rendered as half a card — so this page is legitimately short"
        );
        assert_eq!(page2["has_more"], serde_json::json!(false));
        assert_eq!(page2["next_cursor"], serde_json::Value::Null);

        // Nothing excluded ever appears, on any page.
        let (_, all) = merge_review_page(&body_pool, user_id, account_id, graph_id, Some(100), None).await;
        let ids = item_judgement_ids(&all);
        assert_eq!(ids, live, "one uncapped page = exactly the three live UNSURE rows, in queue order");
        for excluded in [annulled, expired, positive, negative, dead_endpoint] {
            assert!(!ids.contains(&excluded), "excluded row {excluded} leaked into the queue");
        }
    })
    .await;
}

// ── b) the flag gate darkens the whole surface ────────────────────────────

#[tokio::test]
async fn merge_review_flag_off_and_log_return_an_empty_page_and_zero_count() {
    let Some(pool) = pool_or_skip("merge_review_flag_off_and_log_return_an_empty_page_and_zero_count").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-flag-gate").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Hana Holt").await;
        let b = seed_node(&store, graph_id, "Hana Holte").await;
        {
            // Seed the row WITH the flag on — it is real data that exists in
            // the table; the gate is about whether the surface shows it.
            let _flag = FlagGuard::set("on");
            seed_unsure(&store, &body_pool, graph_id, a, b, 0.7, 30).await;
        }

        for mode in ["off", "log"] {
            let _flag = if mode == "off" { FlagGuard::unset() } else { FlagGuard::set("log") };

            let (status, body) = merge_review_page(&body_pool, user_id, account_id, graph_id, None, None).await;
            assert_eq!(status, StatusCode::OK, "mode {mode}: body {body:?}");
            assert_eq!(body["items"], serde_json::json!([]), "mode {mode}");
            assert_eq!(body["has_more"], serde_json::json!(false), "mode {mode}");
            assert_eq!(body["next_cursor"], serde_json::Value::Null, "mode {mode}");

            let (status, count) = review_count(
                tenant_for(&body_pool),
                Extension(user_ctx(user_id, account_id)),
                Query(ReviewCountQuery { graph_id }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "mode {mode}");
            assert_eq!(
                count.0["merge_reviews"],
                serde_json::json!(0),
                "mode {mode}: the badge must not count a queue nothing can drain"
            );
        }

        // Control: the row IS there, and flag-on proves the empties above
        // came from the gate and not from an empty table.
        let _flag = FlagGuard::set("on");
        let (_, body) = merge_review_page(&body_pool, user_id, account_id, graph_id, None, None).await;
        assert_eq!(body["items"].as_array().unwrap().len(), 1, "flag-on control");
    })
    .await;
}

// ── c) Confirm = the existing merge route, superseding the UNSURE row ─────

#[tokio::test]
async fn merge_review_confirm_supersedes_the_unsure_row_and_leaves_the_queue() {
    let Some(pool) = pool_or_skip("merge_review_confirm_supersedes_the_unsure_row_and_leaves_the_queue").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-confirm").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let keep = seed_node(&store, graph_id, "Iris Ipsen").await;
        let dup = seed_node(&store, graph_id, "Iris Ipson").await;
        seed_atoms_for_impact(&body_pool, graph_id, keep, 4).await;
        let unsure = seed_unsure(&store, &body_pool, graph_id, keep, dup, 0.7, 30).await;

        let (_, before) = merge_review_page(&body_pool, user_id, account_id, graph_id, None, None).await;
        assert_eq!(item_judgement_ids(&before), vec![unsure], "the card is in the queue to begin with");

        let (status, response) = merge_node(
            tenant_for(&body_pool),
            Extension(user_ctx(user_id, account_id)),
            Path(keep),
            Json(MergeRequest { dup_id: dup }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);

        // The UNSURE row is annulled and points forward at the human POSITIVE.
        let human = active_judgement_row(&body_pool, graph_id, keep, dup)
            .await
            .expect("the human POSITIVE is now the pair's active row");
        assert_eq!(human.verdict, "positive");
        assert_eq!(human.source, "human");
        let (annulled_at, _, kind, _) = annul_columns(&body_pool, unsure).await;
        assert!(annulled_at.is_some(), "confirming must retire the UNSURE row");
        assert_eq!(kind.as_deref(), Some("human"));
        assert_eq!(
            superseded_by(&body_pool, unsure).await,
            Some(human.judgement_id),
            "the retired row must point at the ruling that replaced it"
        );

        // Materialized, back-linked, and out of the queue.
        assert!(!node_exists(&body_pool, dup).await);
        assert_eq!(
            node_merges_judgement_id(&body_pool, keep, dup).await.expect("node_merges row"),
            Some(human.judgement_id)
        );
        let (_, after) = merge_review_page(&body_pool, user_id, account_id, graph_id, None, None).await;
        assert_eq!(after["items"], serde_json::json!([]), "the resolved pair leaves the queue");
    })
    .await;
}

// ── d) Reject = the existing dismiss route ────────────────────────────────

#[tokio::test]
async fn merge_review_reject_supersedes_the_unsure_row_and_records_not_same() {
    let Some(pool) = pool_or_skip("merge_review_reject_supersedes_the_unsure_row_and_records_not_same").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-reject").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let a = seed_node(&store, graph_id, "Jax Jarvis").await;
        let b = seed_node(&store, graph_id, "Jax Jarvish").await;
        let unsure = seed_unsure(&store, &body_pool, graph_id, a, b, 0.7, 30).await;

        let (status, response) = dismiss_merge_suggestion(
            tenant_for(&body_pool),
            Extension(user_ctx(user_id, account_id)),
            Path(graph_id),
            Json(DismissMergeRequest { a, b }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);

        let human = active_judgement_row(&body_pool, graph_id, a, b)
            .await
            .expect("the human NEGATIVE is now the pair's active row");
        assert_eq!(human.verdict, "negative");
        assert_eq!(human.source, "human");
        let (annulled_at, _, kind, _) = annul_columns(&body_pool, unsure).await;
        assert!(annulled_at.is_some(), "rejecting must retire the UNSURE row");
        assert_eq!(kind.as_deref(), Some("human"));
        assert_eq!(superseded_by(&body_pool, unsure).await, Some(human.judgement_id));

        assert!(not_same_contains(&body_pool, a, b).await, "the legacy dismissal still dual-writes");
        assert!(node_exists(&body_pool, a).await, "reject keeps both reefs");
        assert!(node_exists(&body_pool, b).await);

        let (_, after) = merge_review_page(&body_pool, user_id, account_id, graph_id, None, None).await;
        assert_eq!(after["items"], serde_json::json!([]), "the resolved pair leaves the queue");
    })
    .await;
}

// ── e) a stale card's Confirm is a 404, not a 500 ─────────────────────────

/// The NEW error-mapping arm (PR4). `write_judgement` refuses any pair whose
/// endpoints are not BOTH live in the judgement's own graph, and a
/// cross-ocean pair is the deterministic way to reach that refusal through
/// `merge_node`: the handler's earlier `node_graph_id` lookups both succeed
/// (each reef exists, each ocean is reachable) and only the judgement write
/// discovers the pair isn't one. Before PR4 this was a `500 "merge failed"`.
#[tokio::test]
async fn merge_node_cross_ocean_pair_returns_404_not_500_and_writes_nothing() {
    let Some(pool) = pool_or_skip("merge_node_cross_ocean_pair_returns_404_not_500_and_writes_nothing").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-stale-a").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let other_graph = store
            .create_graph(account_id, "pr4-stale-b", "personal")
            .await
            .expect("second ocean")
            .id;
        let keep = seed_node(&store, graph_id, "Kai Kessler").await;
        let dup = seed_node(&store, other_graph, "Kai Kesler").await;

        let (status, response) = merge_node(
            tenant_for(&body_pool),
            Extension(user_ctx(user_id, account_id)),
            Path(keep),
            Json(MergeRequest { dup_id: dup }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "body: {:?}", response.0);
        assert_eq!(
            response.0["error"],
            serde_json::json!("reef pair no longer exists"),
            "the body must say the PAIR is gone, not report an outage"
        );

        // Nothing partial: no judgement in either ocean, both reefs intact.
        assert_eq!(reef_judgement_count(&body_pool, graph_id).await, 0);
        assert_eq!(reef_judgement_count(&body_pool, other_graph).await, 0);
        assert!(node_exists(&body_pool, keep).await);
        assert!(node_exists(&body_pool, dup).await);
        assert!(
            node_merges_judgement_id(&body_pool, keep, dup).await.is_none(),
            "no merge may have been recorded"
        );
    })
    .await;
}

/// The OTHER stale shape, for completeness: the pair already merged away, so
/// one endpoint's row is gone entirely. This 404 comes from `merge_node`'s
/// PRE-EXISTING `node_graph_id` lookup, not from the PR4 arm — which is
/// exactly why it is asserted separately and on a different message. A test
/// that only covered this shape would pass on `main` and prove nothing about
/// the new mapping.
#[tokio::test]
async fn merge_node_deleted_endpoint_still_404s_through_the_pre_existing_arm() {
    let Some(pool) = pool_or_skip("merge_node_deleted_endpoint_still_404s_through_the_pre_existing_arm").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-stale-deleted").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let keep = seed_node(&store, graph_id, "Lena Lund").await;
        let dup = seed_node(&store, graph_id, "Lena Lunde").await;
        seed_unsure(&store, &body_pool, graph_id, keep, dup, 0.7, 30).await;
        sqlx::query("DELETE FROM nodes WHERE node_id = $1")
            .bind(dup)
            .execute(&body_pool)
            .await
            .expect("delete the dup out from under the card");

        let (status, response) = merge_node(
            tenant_for(&body_pool),
            Extension(user_ctx(user_id, account_id)),
            Path(keep),
            Json(MergeRequest { dup_id: dup }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "body: {:?}", response.0);
        assert_eq!(response.0["error"], serde_json::json!("reef not found or access denied"));
        assert!(
            node_merges_judgement_id(&body_pool, keep, dup).await.is_none(),
            "no merge may have been recorded"
        );
    })
    .await;
}

// ── f) the sidebar badge's third total ────────────────────────────────────

#[tokio::test]
async fn review_count_reports_live_merge_reviews_and_leaves_the_other_totals_alone() {
    let Some(pool) =
        pool_or_skip("review_count_reports_live_merge_reviews_and_leaves_the_other_totals_alone").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-count").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());

        let count = |pool: PgPool| async move {
            let (status, body) = review_count(
                tenant_for(&pool),
                Extension(user_ctx(user_id, account_id)),
                Query(ReviewCountQuery { graph_id }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "body: {:?}", body.0);
            body.0
        };

        let empty = count(body_pool.clone()).await;
        assert_eq!(empty["merge_reviews"], serde_json::json!(0), "no rows yet");
        assert_eq!(empty["decisions"], serde_json::json!(0));
        assert_eq!(empty["suggested_edges"], serde_json::json!(0));

        // Two live rows, one annulled, one expired — the count must match the
        // LISTING's predicate, not the worker's expiry-blind cap.
        for (x, y) in [("Mia Marek", "Mia Marec"), ("Nils Nyman", "Nils Niman")] {
            let a = seed_node(&store, graph_id, x).await;
            let b = seed_node(&store, graph_id, y).await;
            seed_unsure(&store, &body_pool, graph_id, a, b, 0.7, 30).await;
        }
        let oa = seed_node(&store, graph_id, "Ola Olsen").await;
        let ob = seed_node(&store, graph_id, "Ola Olssen").await;
        let annulled = seed_unsure(&store, &body_pool, graph_id, oa, ob, 0.7, 30).await;
        store.annul_judgement(annulled, None, AnnulKind::System, "pr4-test").await.expect("annul");
        let pa = seed_node(&store, graph_id, "Pia Prinz").await;
        let pb = seed_node(&store, graph_id, "Pia Prins").await;
        let expired = seed_unsure(&store, &body_pool, graph_id, pa, pb, 0.7, 30).await;
        expire_judgement(&body_pool, expired).await;

        let totals = count(body_pool.clone()).await;
        assert_eq!(
            totals["merge_reviews"],
            serde_json::json!(2),
            "annulled and expired rows are not review work"
        );
        assert_eq!(totals["decisions"], serde_json::json!(0), "unchanged by PR4");
        assert_eq!(totals["suggested_edges"], serde_json::json!(0), "unchanged by PR4");

        // The badge and the list agree — the whole point of the two sharing
        // one predicate.
        let (_, page) = merge_review_page(&body_pool, user_id, account_id, graph_id, None, None).await;
        assert_eq!(page["items"].as_array().unwrap().len() as i64, totals["merge_reviews"].as_i64().unwrap());
    })
    .await;
}

// ── g) the kill trigger's window query, live ──────────────────────────────

/// Inserts a `reef_judgement` row directly (no `write_judgement`): this test
/// is about the chain-health WINDOW QUERY, and raw inserts let it build a
/// 50+-row history with explicit timestamps without also standing up 100
/// reef nodes. Endpoints are bare, un-FK'd uuids by design (migration 0139's
/// header), so they need not exist.
#[allow(clippy::too_many_arguments)]
async fn insert_raw_judgement(
    pool: &PgPool,
    graph_id: Uuid,
    verdict: &str,
    source: &str,
    seconds_ago: i64,
    annulled: bool,
) -> Uuid {
    let (a, b) = ordered_pair(Uuid::new_v4(), Uuid::new_v4());
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO reef_judgement \
             (graph_id, node_a_id, node_b_id, verdict, source, created_at, annulled_at, annulled_by_kind) \
         VALUES ($1, $2, $3, $4, $5, now() - make_interval(secs => $6::double precision), \
                 CASE WHEN $7 THEN now() ELSE NULL END, \
                 CASE WHEN $7 THEN 'system' ELSE NULL END) \
         RETURNING judgement_id",
    )
    .bind(graph_id)
    .bind(a)
    .bind(b)
    .bind(verdict)
    .bind(source)
    .bind(seconds_ago as f64)
    .bind(annulled)
    .fetch_one(pool)
    .await
    .expect("insert raw judgement");
    id
}

/// Runs the REAL `admin_chain_health` window SQL. The predicate over its
/// output is unit-tested in `routes::admin`
/// (`unsure_rate_boundary_is_strictly_above_forty_percent` etc.); what can
/// only be proven live is that this runtime-checked query is valid against
/// the real schema and selects the rows the invariant claims — a typo'd
/// column here would compile fine and only surface as a `500` from the
/// health endpoint itself, i.e. a detector that has quietly gone dark.
async fn llm_adjudication_window(pool: &PgPool, account_id: Uuid, limit: i64) -> Vec<(chrono::DateTime<chrono::Utc>, Uuid, String)> {
    sqlx::query_as(coralswarm_backend::CHAIN_HEALTH_LLM_ADJUDICATION_WINDOW_SQL)
        .bind(account_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .expect("chain-health window query must be valid against the live schema")
}

#[tokio::test]
async fn chain_health_llm_window_counts_the_oldest_fifty_including_annulled() {
    let Some(pool) = pool_or_skip("chain_health_llm_window_counts_the_oldest_fifty_including_annulled").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-kill-trigger").await;
    // Seeded OUT here, and handed to `with_cleanup` alongside the first
    // account, so a failed assertion below cannot leak it. `with_cleanup`
    // spawns the body, joins, cleans up, THEN resumes the unwind — cleanup
    // called from inside the body does not survive a panic.
    let (other_user, other_account, other_graph) =
        seed_graph(&pool, "pr4-kill-trigger-other").await;
    let body_pool = pool.clone();
    with_cleanup(
        pool,
        vec![(user_id, account_id), (other_user, other_account)],
        async move {
        // 49 LLM adjudications: 30 UNSURE (well past 40%), 19 positive. A
        // partial window renders NO verdict, whatever the ratio.
        for i in 0..49i64 {
            let verdict = if i < 30 { "unsure" } else { "positive" };
            // Newest first by index → oldest is i=48. Annul a third of them:
            // annulment is lifecycle, the adjudication still happened.
            insert_raw_judgement(&body_pool, graph_id, verdict, "llm", 10_000 - i * 10, i % 3 == 0).await;
        }
        // Noise that must NOT be in the window: human- and backfill-sourced
        // rows, and rows from another account entirely.
        insert_raw_judgement(&body_pool, graph_id, "unsure", "human", 20_000, false).await;
        insert_raw_judgement(&body_pool, graph_id, "unsure", "backfill", 20_000, false).await;
        for i in 0..10i64 {
            insert_raw_judgement(&body_pool, other_graph, "unsure", "llm", 30_000 - i, false).await;
        }

        let rows = llm_adjudication_window(&body_pool, account_id, 50).await;
        assert_eq!(rows.len(), 49, "only this account's LLM rows, and there are only 49");
        assert_eq!(
            rows.iter().filter(|(_, _, v)| v == "unsure").count(),
            30,
            "annulled rows are still adjudications and stay in the window"
        );
        assert!(
            rows.windows(2).all(|w| (w[0].0, w[0].1) <= (w[1].0, w[1].1)),
            "the window must come back oldest-first, ties broken on judgement_id"
        );
        // 49 rows is a partial window, so `unsure_adjudication_rate_violated`
        // reports healthy no matter how bad the ratio is (unit-tested).

        // One more row makes it a FULL window of 50.
        insert_raw_judgement(&body_pool, graph_id, "unsure", "llm", 10_000 - 49 * 10, false).await;
        let rows = llm_adjudication_window(&body_pool, account_id, 50).await;
        assert_eq!(rows.len(), 50);
        assert_eq!(rows.iter().filter(|(_, _, v)| v == "unsure").count(), 31);

        // The LIMIT genuinely bounds the window: 20 newer rows must not
        // displace the oldest 50.
        for i in 0..20i64 {
            insert_raw_judgement(&body_pool, graph_id, "positive", "llm", 100 - i, false).await;
        }
        let rows = llm_adjudication_window(&body_pool, account_id, 50).await;
        assert_eq!(rows.len(), 50, "LIMIT bounds the window");
        assert_eq!(
            rows.iter().filter(|(_, _, v)| v == "unsure").count(),
            31,
            "later adjudications cannot change the FIRST fifty"
        );

        },
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════════
// PR4 review round 1 — the two findings that are only visible against a real
// database.
// ═══════════════════════════════════════════════════════════════════════════

/// The test database URL, for the places that need to build a SECOND pool
/// with different connection semantics (see [`app_role_pool`]).
fn test_database_url(test_name: &str) -> Option<String> {
    match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!(
                "skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run the live \
                 judgement-graph worker suite"
            );
            None
        }
    }
}

/// A pool whose every connection runs as `coralswarm_app` — the
/// non-`BYPASSRLS` role OPS-4 points the app at — so migration 0083's `nodes`
/// policy is genuinely ENFORCED rather than inert.
///
/// The rest of this suite deliberately runs on the owner/BYPASSRLS pool (see
/// the module doc): RLS is PR1's concern, not PR2's. Two PR4 review findings
/// are the exception, because both are invisible without it — a policy that
/// hides another account's rows is exactly the mechanism at issue.
///
/// `SET ROLE` on connect, not a separate login (the shape
/// `consumer_rls_pg.rs` uses). The RLS semantics are identical — policies key
/// on the CURRENT role, and `coralswarm_app` is neither the table owner nor
/// `BYPASSRLS` — but it needs no `ALTER ROLE ... PASSWORD`, which is a
/// CLUSTER-wide mutation of a role every other suite on the same cluster
/// shares. Verified against a live database: with `app.account_id` set to a
/// foreign account, a node is invisible; set to its own graph's account, it
/// is visible.
async fn app_role_pool(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::Executor::execute(&mut *conn, "SET ROLE coralswarm_app").await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .expect("connect an RLS-enforcing (coralswarm_app) pool")
}

/// Enrol `user_id` in `account_id` as a plain member — the shape that makes
/// an ocean REACHABLE (`resolve_graph_reachable_by_user` admits any account
/// the caller is a member of) without making it the caller's ACTIVE account.
async fn add_account_member(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("enrol the caller in the other account");
}

// ── Finding 1: Confirm must work on a reachable ocean ANOTHER account owns ──

/// The queue was undrainable by its own primary action on any ocean the
/// caller's ACTIVE account does not own.
///
/// `list_merge_review` and Reject are GRAPH-anchored (scope = the ocean's
/// owning account, via `graph_account_id`); Confirm went through
/// `merge_node`, which is NODE-anchored (scope = `user.account_id`). Under
/// live RLS the `nodes` policy then hides the pair from its own Confirm, so
/// the cards listed, Reject worked, and every Confirm 404'd.
///
/// This test asserts BOTH halves against the RLS-enforcing pool: the old
/// path still 404s (that is the bug, reproduced, and it is why a
/// BYPASSRLS-pool test could never have caught this), and the new
/// graph-anchored route succeeds and supersedes the card.
#[tokio::test]
async fn merge_review_confirm_works_on_a_reachable_ocean_another_account_owns() {
    let name = "merge_review_confirm_works_on_a_reachable_ocean_another_account_owns";
    let Some(url) = test_database_url(name) else { return };
    let Some(pool) = pool_or_skip(name).await else { return };

    // Caller's own account (A) and the account that OWNS the ocean (B).
    let (user_a, account_a, _graph_a) = seed_graph(&pool, "pr4-xacct-caller").await;
    let (user_b, account_b, graph_b) = seed_graph(&pool, "pr4-xacct-owner").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_a, account_a), (user_b, account_b)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        add_account_member(&body_pool, account_b, user_a).await;

        let keep = seed_node(&store, graph_b, "Rhea Roth").await;
        let dup = seed_node(&store, graph_b, "Rhea Roht").await;
        seed_atoms_for_impact(&body_pool, graph_b, keep, 4).await;
        let unsure = seed_unsure(&store, &body_pool, graph_b, keep, dup, 0.7, 30).await;

        // Everything below runs through an RLS-ENFORCING pool. On the
        // BYPASSRLS pool the rest of this suite uses, both paths would pass
        // and this test would prove nothing.
        let rls_pool = app_role_pool(&url).await;
        let ctx = user_ctx(user_a, account_a);

        // The ocean IS listable — proving reachability and the graph-anchored
        // scope work for this caller.
        let (status, page) = list_merge_review(
            tenant_for(&rls_pool),
            Extension(ctx.clone()),
            Path(graph_b),
            Query(ListMergeReviewQuery { limit: None, cursor: None }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", page.0);
        assert_eq!(
            item_judgement_ids(&page.0),
            vec![unsure],
            "a reachable ocean another account owns must list its review queue"
        );

        // The PRE-PR4 Confirm path, on the same ocean, same caller: 404.
        // `merge_node` scopes by the caller's ACTIVE account, so the `nodes`
        // policy hides both endpoints from it.
        let (status, response) = merge_node(
            tenant_for(&rls_pool),
            Extension(ctx.clone()),
            Path(keep),
            Json(MergeRequest { dup_id: dup }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "the node-anchored merge route is expected to still fail here — that IS the bug \
             this route split exists to route around; body: {:?}",
            response.0
        );

        // The PR4 Confirm route, same ocean, same caller: succeeds.
        let (status, response) = confirm_merge_review(
            tenant_for(&rls_pool),
            Extension(ctx),
            Path(graph_b),
            Json(ConfirmMergeReviewRequest {
                judgement_id: unsure,
                keep_id: keep,
                dup_id: dup,
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "body: {:?}", response.0);
        assert_eq!(response.0["ok"], serde_json::json!(true));

        // …and it superseded the card, exactly like the merge route would.
        let human = active_judgement_row(&body_pool, graph_b, keep, dup)
            .await
            .expect("the human POSITIVE is now the pair's active row");
        assert_eq!(human.verdict, "positive");
        assert_eq!(human.source, "human");
        assert_eq!(human.created_by, Some(user_a));
        assert_eq!(human.evidence["route"], serde_json::json!("merge_review_confirm"));
        let (annulled_at, _, kind, _) = annul_columns(&body_pool, unsure).await;
        assert!(annulled_at.is_some(), "confirming must retire the UNSURE row");
        assert_eq!(kind.as_deref(), Some("human"));
        assert_eq!(superseded_by(&body_pool, unsure).await, Some(human.judgement_id));
        assert!(!node_exists(&body_pool, dup).await, "the merge must have materialized");
        assert_eq!(
            node_merges_judgement_id(&body_pool, keep, dup).await.expect("node_merges row"),
            Some(human.judgement_id)
        );
    })
    .await;
}

/// The one-404-rule: a card that is no longer open must not merge anything.
/// Same RLS-enforcing pool, because the fixture is the same cross-account
/// shape — a stale card on a shared ocean is the likeliest stale card there
/// is (two people working the same queue).
#[tokio::test]
async fn merge_review_confirm_refuses_a_stale_card_without_merging() {
    let name = "merge_review_confirm_refuses_a_stale_card_without_merging";
    let Some(url) = test_database_url(name) else { return };
    let Some(pool) = pool_or_skip(name).await else { return };

    let (user_id, account_id, graph_id) = seed_graph(&pool, "pr4-stale-card").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        let _flag = FlagGuard::set("on");
        let store = PostgresStore::new(body_pool.clone());
        let keep = seed_node(&store, graph_id, "Sven Salo").await;
        let dup = seed_node(&store, graph_id, "Sven Salu").await;
        let unsure = seed_unsure(&store, &body_pool, graph_id, keep, dup, 0.7, 30).await;

        let rls_pool = app_role_pool(&url).await;
        let ctx = user_ctx(user_id, account_id);

        // Someone else resolved it first: the card's judgement is annulled.
        store
            .annul_judgement(unsure, None, AnnulKind::System, "pr4-test")
            .await
            .expect("annul");

        let (status, response) = confirm_merge_review(
            tenant_for(&rls_pool),
            Extension(ctx.clone()),
            Path(graph_id),
            Json(ConfirmMergeReviewRequest {
                judgement_id: unsure,
                keep_id: keep,
                dup_id: dup,
            }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "body: {:?}", response.0);
        assert!(node_exists(&body_pool, dup).await, "a stale card must merge NOTHING");
        assert!(node_merges_judgement_id(&body_pool, keep, dup).await.is_none());

        // A LIVE card whose id the caller got wrong is equally refused — the
        // route resolves the named card, it does not merge an arbitrary pair.
        let live = seed_unsure(&store, &body_pool, graph_id, keep, dup, 0.7, 30).await;
        let (status, response) = confirm_merge_review(
            tenant_for(&rls_pool),
            Extension(ctx),
            Path(graph_id),
            Json(ConfirmMergeReviewRequest {
                judgement_id: Uuid::new_v4(),
                keep_id: keep,
                dup_id: dup,
            }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "body: {:?}", response.0);
        assert!(node_exists(&body_pool, dup).await, "an unknown card id must merge NOTHING");
        let (still_open, _, _, _) = annul_columns(&body_pool, live).await;
        assert!(still_open.is_none(), "…and must leave the real card open");
    })
    .await;
}

// ── Finding 2: an unqueryable reef_judgement must not blind chain-health ────

/// The kill trigger going dark must not take the incident detectors with it.
///
/// `admin_chain_health` renders four invariants. Three of them detect real,
/// already-happened production incidents; the fourth (PR4's UNSURE-rate kill
/// trigger) reads `reef_judgement`, a table the other three do not need.
/// Wiring it with [`fail_loud!`] like its neighbours meant a single
/// unqueryable-`reef_judgement` context 500'd the WHOLE endpoint — and the
/// hourly poller `curl -fsS`s it, so a real incident's specific violation
/// would have been replaced by an opaque transport failure.
#[tokio::test]
async fn chain_health_survives_an_unqueryable_reef_judgement() {
    let name = "chain_health_survives_an_unqueryable_reef_judgement";
    let Some(url) = test_database_url(name) else { return };
    let Some(pool) = pool_or_skip(name).await else { return };

    let (user_id, account_id, _graph_id) = seed_graph(&pool, "pr4-chain-health").await;
    let body_pool = pool.clone();
    with_cleanup(pool, vec![(user_id, account_id)], async move {
        // Internal endpoints reject without this; process-global env, same
        // `--test-threads=1` convention as `FlagGuard`.
        let prev_token = std::env::var("INTERNAL_TOKEN").ok();
        std::env::set_var("INTERNAL_TOKEN", "pr4-chain-health-test-token");
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-internal-token", "pr4-chain-health-test-token".parse().unwrap());

        // Run the endpoint against an RLS-enforcing pool so the revoke below
        // actually bites (the owner/superuser pool ignores table grants).
        let rls_pool = app_role_pool(&url).await;
        let state = std::sync::Arc::new(coralswarm_backend::test_app_state_owned_with_db(
            rls_pool.clone(),
            None,
        ));

        // Baseline: all four invariants render.
        let (status, body) =
            admin_chain_health(axum::extract::State(state.clone()), headers.clone()).await;
        assert_eq!(status, StatusCode::OK, "baseline: {:?}", body.0);
        assert_eq!(
            body.0["unsure_adjudication_rate"]["available"],
            serde_json::json!(true),
            "baseline: the kill trigger should be evaluable"
        );

        // Simulate "this one table is unqueryable in this context" (a
        // pre-0139 restore, a half-stamped database) as narrowly as possible:
        // ONE table, ONE role, in this database only.
        //
        // The response is captured and the grant restored BEFORE any
        // assertion runs. A `Drop` guard would be the tidier shape and is
        // wrong here: `Drop` cannot await, and blocking on the pool from
        // inside `#[tokio::test]`'s current-thread runtime deadlocks — which
        // is exactly what the first draft of this test did, hanging the whole
        // suite until it was killed.
        sqlx::query("REVOKE SELECT ON reef_judgement FROM coralswarm_app")
            .execute(&body_pool)
            .await
            .expect("revoke");
        // A fresh pool: the one above may hold connections whose plan cache
        // predates the revoke.
        let revoked_pool = app_role_pool(&url).await;
        let revoked_state = std::sync::Arc::new(
            coralswarm_backend::test_app_state_owned_with_db(revoked_pool.clone(), None),
        );
        let (revoked_status, revoked_body) =
            admin_chain_health(axum::extract::State(revoked_state), headers.clone()).await;
        revoked_pool.close().await;
        sqlx::query("GRANT SELECT ON reef_judgement TO coralswarm_app")
            .execute(&body_pool)
            .await
            .expect("restore coralswarm_app's SELECT grant");

        // THE ASSERTIONS. Pre-fix this was a 500 with `{"error": ...}` and no
        // verdicts at all.
        let body = revoked_body.0;
        assert_eq!(
            revoked_status,
            StatusCode::OK,
            "an unqueryable reef_judgement must not 500 the endpoint: {body:?}"
        );
        assert_eq!(
            body["unsure_adjudication_rate"]["available"],
            serde_json::json!(false),
            "…and must SAY the kill trigger went dark, not silently pass"
        );
        assert_eq!(
            body["unsure_adjudication_rate"]["violated"],
            serde_json::json!(false),
            "an unevaluated invariant is never a violation"
        );
        assert_eq!(body["unsure_adjudication_rate"]["adjudications_seen"], serde_json::Value::Null);

        // The three incident detectors still rendered and still decide.
        assert!(body["conservation"]["violated"].is_boolean(), "{body:?}");
        assert!(body["conservation"]["atoms_created"].is_i64(), "{body:?}");
        assert!(body["outcome_distribution"]["violations"].is_array(), "{body:?}");
        assert!(body["structural_dead_letters"]["violations"].is_array(), "{body:?}");
        assert!(body["healthy"].is_boolean(), "{body:?}");

        // The grant is back, so the trigger is evaluable again.
        let restored_pool = app_role_pool(&url).await;
        let restored_state = std::sync::Arc::new(
            coralswarm_backend::test_app_state_owned_with_db(restored_pool, None),
        );
        let (status, body) =
            admin_chain_health(axum::extract::State(restored_state), headers).await;
        assert_eq!(status, StatusCode::OK, "{:?}", body.0);
        assert_eq!(
            body.0["unsure_adjudication_rate"]["available"],
            serde_json::json!(true),
            "the degradation must be transient, not sticky"
        );

        match prev_token {
            Some(v) => std::env::set_var("INTERNAL_TOKEN", v),
            None => std::env::remove_var("INTERNAL_TOKEN"),
        }
    })
    .await;
}
