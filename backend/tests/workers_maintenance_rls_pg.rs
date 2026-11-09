//! W.2 group (b) — RLS-scope conversion proofs for the maintenance/enrichment
//! workers (`inference_closure`, `summary`, `tombstone_purge`, `huddle`,
//! `reembed`, `content_tsv_backfill`).
//!
//! Companion to `workers_ingest_rls_pg.rs` (group (a)). Every test drives the
//! REAL worker entry point against the P.1 [`RlsProbe`] harness — a pool
//! authenticated AS the non-BYPASSRLS `coralswarm_app` role with every
//! `0083_consumer_rls` policy live — and proves one of:
//!
//!   1. **Scoped processing**: a job stamped for account A processes A's own
//!      rows successfully under A's scope.
//!   2. **Cross-account isolation** (the actual security property): a job
//!      stamped for account A whose PAYLOAD names account B's ids cannot
//!      touch B's rows. The payload-vs-stamp ownership check (0084's
//!      `resolve_graph_account_id`) refuses the job outright, which holds
//!      even PRE-OPS-4 where the owner role's BYPASSRLS would otherwise serve
//!      B's rows happily; RLS is the post-cutover backstop.
//!   3. **The `None`-`account_id` defensive failure**: an RLS-relevant job
//!      that somehow reaches processing without a stamp is failed on the
//!      queue with `UNSCOPED_JOB_ERR`, never processed unscoped.
//!   4. **Loop-per-account sweeps**: the account-less sweeps converted to
//!      per-account passes actually visit BOTH accounts (not just the first).
//!
//! Gated on `ORGT_TEST_BASE_URL`, one scratch DB per test (the P.1 harness
//! convention):
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test workers_maintenance_rls_pg

mod support;

use std::sync::Arc;

use chrono::Utc;
use coralswarm_backend::jobs::{queue, JobKind};
use coralswarm_backend::llm::{
    CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent,
};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::workers::{content_tsv_backfill, huddle, inference_closure, reembed, summary, tombstone_purge};
use sqlx::PgPool;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

// ── Stub LLM providers ────────────────────────────────────────────────────────

/// LLM returning a fixed JSON body (the workers parse `resp.content` as JSON).
struct FixedLlm(&'static str);
#[async_trait::async_trait]
impl LlmProvider for FixedLlm {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        Ok(CompletionResponse {
            content: self.0.to_string(),
            input_tokens: 1,
            output_tokens: 1,
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

/// LLM that fails the test if it is ever reached — proves a cross-account
/// payload is refused before any model call (and therefore before any
/// transcript text could egress).
struct PanicLlm;
#[async_trait::async_trait]
impl LlmProvider for PanicLlm {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        panic!("LLM must not be called for a cross-account payload");
    }
    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        panic!("LLM must not be called for a cross-account payload");
    }
}

// ── Fixture helpers ───────────────────────────────────────────────────────────

fn consumer_tenant(pool: &PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: TenantId::Consumer,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
    }
}

async fn job_state(owner: &PgPool, job_id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status::text, error_message FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(owner)
        .await
        .expect("read job state")
}

/// Seed a node via the owner (BYPASSRLS) pool.
async fn seed_node(owner: &PgPool, graph_id: Uuid, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', $2) RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .fetch_one(owner)
    .await
    .expect("insert node")
}

/// Seed a CONFIRMED relationship edge via the owner pool.
async fn seed_edge(owner: &PgPool, graph_id: Uuid, src: Uuid, predicate: &str, dst: Uuid) {
    sqlx::query(
        "INSERT INTO relationship_edge (graph_id, src_node_id, predicate, dst_node_id, origin, \
                                        status, confidence) \
         VALUES ($1, $2, $3, $4, 'stated', 'confirmed', 1.0)",
    )
    .bind(graph_id)
    .bind(src)
    .bind(predicate)
    .bind(dst)
    .execute(owner)
    .await
    .expect("insert edge");
}

async fn count_inferred_edges(owner: &PgPool, graph_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM relationship_edge WHERE graph_id = $1 AND origin = 'inferred'",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("count inferred edges")
}

/// Seed a `grandparent_of`-producible chain (`a parent_of b`, `b parent_of c`)
/// so the deterministic composition pass has something to infer.
async fn seed_inferable_chain(owner: &PgPool, graph_id: Uuid) {
    let a = seed_node(owner, graph_id, "Ann").await;
    let b = seed_node(owner, graph_id, "Bob").await;
    let c = seed_node(owner, graph_id, "Cy").await;
    seed_edge(owner, graph_id, a, "parent_of", b).await;
    seed_edge(owner, graph_id, b, "parent_of", c).await;
}

/// Seed a meeting transcript with inline raw text via the owner pool.
async fn seed_transcript(owner: &PgPool, graph_id: Uuid, source_id: &str, raw: &str) {
    sqlx::query(
        "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline) VALUES ($1, $2, $3)",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(raw)
    .execute(owner)
    .await
    .expect("insert transcript");
}

async fn summary_status(owner: &PgPool, graph_id: Uuid, source_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT summary_status FROM meeting_transcripts WHERE graph_id = $1 AND source_id = $2",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(owner)
    .await
    .expect("read summary_status")
}

/// A map+reduce-shaped body the summary worker can parse for both passes.
const SUMMARY_LLM_BODY: &str = r#"{"overview":"A short overview.","decisions":[],"action_items":[]}"#;

// ── inference_closure ─────────────────────────────────────────────────────────

#[tokio::test]
async fn inference_closure_scoped_processing_and_cross_account_isolation() {
    let Some(probe) = RlsProbe::setup("orgt_w2b_inference_closure").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    seed_inferable_chain(&probe.owner, probe.a.graph_id).await;
    seed_inferable_chain(&probe.owner, probe.b.graph_id).await;

    let wt = consumer_tenant(&probe.app_pool);

    // ── 1. Scoped processing: A's job composes A's edges under A's scope ─────
    let job_a = queue::enqueue(
        &probe.app_pool,
        &JobKind::InferRelationships { graph_id: probe.a.graph_id },
        Some(probe.a.account_id),
        None,
        0,
    )
    .await
    .expect("enqueue A job")
    .expect("job id");
    // InferRelationships makes no model call, so it needs no managed purpose
    // and runs (reclassified NotPaid, 2026-10-18; tenant 0160 / control 0327).
    let tick = inference_closure::tick(&wt).await.expect("tick ok");
    assert_eq!(tick.processed, 1);
    let (status, err) = job_state(&probe.owner, job_a).await;
    assert_eq!(status, "done", "A's job must complete (error: {err:?})");
    assert!(
        count_inferred_edges(&probe.owner, probe.a.graph_id).await > 0,
        "the composition pass must have written A's inferred edges under A's scope"
    );

    // ── 2. Cross-account isolation: A-stamped job naming B's graph REFUSED ───
    let b_before = count_inferred_edges(&probe.owner, probe.b.graph_id).await;
    let job_forged = queue::enqueue(
        &probe.app_pool,
        &JobKind::InferRelationships { graph_id: probe.b.graph_id },
        Some(probe.a.account_id),
        None,
        0,
    )
    .await
    .expect("enqueue forged job")
    .expect("job id");
    let tick2 = inference_closure::tick(&wt).await.expect("tick ok");
    assert_eq!(tick2.processed, 1);
    let (status2, err2) = job_state(&probe.owner, job_forged).await;
    assert_ne!(status2, "done", "a cross-account payload must never complete");
    assert!(
        err2.unwrap_or_default().contains("refusing cross-account processing"),
        "failure must be the payload-vs-stamp ownership check"
    );
    assert_eq!(
        count_inferred_edges(&probe.owner, probe.b.graph_id).await,
        b_before,
        "B's graph must be untouched by A's forged job"
    );

    // ── 3. Defensive: an unstamped job is failed, never processed unscoped ───
    let store = PostgresStore::new(probe.app_pool.clone());
    let unstamped = unstamped_job(&probe.owner, "InferRelationships", probe.a.graph_id).await;
    let unstamped_id = unstamped.id;
    inference_closure::process_job(&TenantId::Consumer, &probe.app_pool, &store, unstamped).await;
    let (status3, err3) = job_state(&probe.owner, unstamped_id).await;
    assert_ne!(status3, "done", "an unscoped job must never complete");
    assert!(
        err3.unwrap_or_default().contains("no account_id"),
        "failure must be UNSCOPED_JOB_ERR"
    );

    // ── 4. Fail-closed sanity: the worker's table hard-errors unscoped ───────
    probe
        .assert_unscoped_hard_errors(
            "SELECT edge_id FROM relationship_edge WHERE graph_id = $1",
            &[probe.a.graph_id],
        )
        .await;

    probe.teardown().await;
}

/// Build a dequeued-shaped [`queue::JobRow`] with `account_id: None` — the
/// state W.1's dequeue makes unreachable through the tick path, so it can only
/// be produced by hand.
async fn unstamped_job(
    owner: &PgPool,
    kind: &str,
    graph_id: Uuid,
) -> coralswarm_backend::jobs::queue::JobRow {
    let payload = serde_json::json!({ "graph_id": graph_id.to_string() });
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO jobs (kind, payload, status, account_id) \
         VALUES ($1, $2, 'running', NULL) RETURNING id",
    )
    .bind(kind)
    .bind(&payload)
    .fetch_one(owner)
    .await
    .expect("insert unstamped job");
    coralswarm_backend::jobs::queue::JobRow {
        id,
        kind: kind.to_string(),
        payload,
        attempts: 0,
        account_id: None,
    }
}

// ── summary ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn summary_worker_scoped_processing_and_cross_account_isolation() {
    let Some(probe) = RlsProbe::setup("orgt_w2b_summary").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    seed_transcript(&probe.owner, probe.a.graph_id, "meet-a", "Ann: we ship on Friday.").await;
    seed_transcript(&probe.owner, probe.b.graph_id, "meet-b", "Bob: secret plans.").await;

    let wt = consumer_tenant(&probe.app_pool);

    // ── 1. Scoped processing: A's job summarizes A's transcript ──────────────
    let job_a = queue::enqueue(
        &probe.app_pool,
        &JobKind::GenerateMeetingSummary {
            graph_id: probe.a.graph_id,
            source_id: "meet-a".to_string(),
            allow_cloud_override: true,
        },
        Some(probe.a.account_id),
        None,
        0,
    )
    .await
    .expect("enqueue A job")
    .expect("job id");
    let llm = FixedLlm(SUMMARY_LLM_BODY);
    // Managed enrichment (source-agnostic PR 10): the kind is admitted as
    // pending, and the worker claims nothing while
    // ENRICHMENT_PROCESSING_ENABLED is unset (the default), so it makes no
    // model call or graph write. Metered processing is covered by
    // managed::gate::pg_tests::meeting_summary.
    let tick = summary::tick(&wt, &probe.app_pool, &llm, None).await.expect("tick ok");
    assert_eq!(tick.processed, 0, "switched off: nothing claimed");
    assert_eq!(
        job_state(&probe.owner, job_a).await,
        ("pending".to_string(), None),
        "a managed_enrichment kind waits pending, never dead-lettered"
    );
    assert_ne!(summary_status(&probe.owner, probe.a.graph_id, "meet-a").await, "done");
    probe.teardown().await;
}

// ── tombstone_purge (loop-per-account sweep) ──────────────────────────────────

#[tokio::test]
async fn tombstone_purge_sweeps_every_account_under_its_own_scope() {
    let Some(probe) = RlsProbe::setup("orgt_w2b_tombstone_purge").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    // One expired delete-tombstone per account, plus one fresh op per account
    // that must SURVIVE (proves the sweep is age-filtered, not indiscriminate).
    for graph_id in [probe.a.graph_id, probe.b.graph_id] {
        seed_op(&probe.owner, graph_id, "delete", Utc::now() - chrono::Duration::days(90)).await;
        seed_op(&probe.owner, graph_id, "upsert", Utc::now() - chrono::Duration::days(90)).await;
        seed_op(&probe.owner, graph_id, "delete", Utc::now()).await;
    }

    let wt = consumer_tenant(&probe.app_pool);
    let tick = tombstone_purge::tick(&wt).await.expect("tick ok");
    assert_eq!(
        tick.processed, 2,
        "the loop-per-account sweep must purge BOTH accounts' expired tombstones, not just the first"
    );

    for graph_id in [probe.a.graph_id, probe.b.graph_id] {
        assert_eq!(
            count_ops(&probe.owner, graph_id, "delete").await,
            1,
            "only the fresh tombstone survives in each account"
        );
        assert_eq!(
            count_ops(&probe.owner, graph_id, "upsert").await,
            1,
            "non-tombstone ops are never purged"
        );
    }

    probe.teardown().await;
}

async fn seed_op(
    owner: &PgPool,
    graph_id: Uuid,
    op_type: &str,
    updated_at: chrono::DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO op_log (op_id, entity, entity_id, op_type, updated_at, device_id, graph_id) \
         VALUES ($1, 'atom', $2, $3, $4, 'test-device', $5)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4().to_string())
    .bind(op_type)
    .bind(updated_at)
    .bind(graph_id)
    .execute(owner)
    .await
    .expect("insert op_log row");
}

async fn count_ops(owner: &PgPool, graph_id: Uuid, op_type: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM op_log WHERE graph_id = $1 AND op_type = $2")
        .bind(graph_id)
        .bind(op_type)
        .fetch_one(owner)
        .await
        .expect("count op_log rows")
}

// ── huddle capture (job-queue dispatch, per-job account scope) ────────────────
//
// The huddle-capture rewrite replaced the polled `slack_huddle_captures`
// QUEUE with the generic `jobs` table (`JobKind::SlackHuddleCapture`,
// `workers::huddle::process_job`) — `slack_huddle_captures` is now a
// terminal-outcome LEDGER (migration `0102_slack_huddle_captures_ledger` /
// tenant `0014`), written only on a decided outcome (ingested, or skipped
// with a reason). There is no more cross-account claim to loop over per
// account: `jobs::queue::dequeue` already returns rows spanning every
// account in one statement (exactly like every other job-queue worker), and
// each job carries its OWN stamped `account_id`. This test proves the
// property that matters — every account's job is processed under ITS OWN
// scope, never another's — against the job-based entry point.

#[tokio::test]
async fn huddle_capture_jobs_process_under_each_accounts_own_scope() {
    let Some(probe) = RlsProbe::setup("orgt_w2b_huddle").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    // Foundation PR 1.2 (CS-031): `get_slack_bot_token` now resolves the
    // account DEK via `crate::crypto::resolve_dek` (`MASTER_KEK_HEX` +
    // `accounts.dek_wrapped`), not the retired `ACCOUNT_MASTER_KEK` XOR
    // derivation — so each account needs a REAL wrapped DEK, not just the env
    // var set, before the token lookup can reach its scoped `oauth_tokens`
    // SELECT (rather than short-circuiting on "no DEK configured"). Same
    // idiom as `workers_ingest_rls_pg.rs`'s `set_test_kek`/`give_account_dek`.
    unsafe { std::env::set_var("MASTER_KEK_HEX", "cd".repeat(32)) };
    let kek = coralswarm_core::kek::load_kek().expect("test KEK loads");
    for account_id in [probe.a.account_id, probe.b.account_id] {
        let wrapped =
            coralswarm_core::kek::wrap_dek(&kek, &coralswarm_core::kek::generate_dek()).unwrap();
        sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
            .bind(&wrapped)
            .bind(account_id)
            .execute(&probe.owner)
            .await
            .expect("set account dek_wrapped");
    }

    // One SlackHuddleCapture job per account, each already stamped with that
    // account's own id (exactly as `routes::slack_events::
    // enqueue_huddle_capture` does at enqueue time). Neither account has a
    // Slack oauth_token row, so each job terminates right after its OWN
    // scoped `oauth_tokens` lookup returns no row — no network I/O — which is
    // exactly what makes this a pure claim/scoping proof.
    let mut job_ids = Vec::new();
    for account_id in [probe.a.account_id, probe.b.account_id] {
        let kind = JobKind::SlackHuddleCapture {
            account_id,
            workspace_id: "T1".to_string(),
            channel_id: "C123".to_string(),
            huddle_thread_ts: format!("{}.000", Uuid::new_v4().as_u128() % 1_000_000),
            canvas_file_id: "F000TEST".to_string(),
            participant_history: vec![],
            date_start: None,
            date_end: None,
        };
        let id = queue::enqueue(&probe.owner, &kind, Some(account_id), None, 0)
            .await
            .expect("enqueue huddle capture job")
            .expect("not deduped — no idempotency key set");
        job_ids.push((account_id, id));
    }

    let jobs = queue::dequeue(&probe.app_pool, &["SlackHuddleCapture"], 10)
        .await
        .expect("dequeue huddle capture jobs");
    assert_eq!(jobs.len(), 2, "both accounts' jobs must be claimable in one dequeue pass");

    for job in jobs {
        huddle::process_job(
            &probe.app_pool,
            &TenantId::Consumer,
            &probe.app_pool,
            &reqwest::Client::new(),
            None,
            job,
        )
        .await;
    }

    // A missing `oauth_tokens` row is a PERMANENT condition (an uninstalled
    // or revoked Slack app never heals on retry) — `workers::huddle`'s
    // `CaptureError::Terminal` classification routes this through
    // `finish_as_terminal_failure`. Track B reclassified it: this worker
    // COULD NOT capture the huddle, so it is a `FailureClass::Structural`
    // `could_not_do` — the job dead-letters on the FIRST attempt (never
    // requeued to `pending`, so it still never burns the retry backoff
    // schedule, which was the original BLOCKER 3 intent) rather than
    // reporting a misleadingly green `done`. The terminal ledger row is
    // written exactly as before, per account, under ITS OWN scope.
    for (account_id, job_id) in &job_ids {
        let (status, error) = job_state(&probe.owner, *job_id).await;
        assert_eq!(
            status, "dead",
            "a missing-oauth-token failure is a TERMINAL condition — it must dead-letter \
             immediately (never requeue to pending, never report done) for account {account_id}"
        );
        assert!(
            error.as_deref().is_some_and(|e| e.contains("oauth token")),
            "a Structural could_not_do dead-letters via queue::dead_letter, which records the \
             reason in error_message, for account {account_id}: {error:?}"
        );

        let skip_reason: Option<String> = sqlx::query_scalar(
            "SELECT skip_reason FROM slack_huddle_captures WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&probe.owner)
        .await
        .expect("a Terminal outcome must write a ledger row");
        assert!(
            skip_reason.unwrap_or_default().contains("oauth token"),
            "each account must have failed at ITS OWN account's token lookup, not another's: \
             {account_id}"
        );
    }

    probe
        .assert_unscoped_hard_errors(
            "SELECT id FROM slack_huddle_captures WHERE account_id = $1",
            &[probe.a.account_id],
        )
        .await;

    probe.teardown().await;
}

// ── reembed / content_tsv_backfill (seed → per-account fan-out) ───────────────

#[tokio::test]
async fn account_less_sweeps_fan_out_one_child_per_account() {
    let Some(probe) = RlsProbe::setup("orgt_w2b_sweep_fanout").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let store = PostgresStore::new(probe.app_pool.clone());

    // ── content_tsv_backfill: the operator-inserted account-less job is a SEED
    let seed_id: Uuid = sqlx::query_scalar(
        "INSERT INTO jobs (kind, payload, status, account_id) \
         VALUES ('ContentTsvRegconfigRepair', '{}'::jsonb, 'running', NULL) RETURNING id",
    )
    .fetch_one(&probe.owner)
    .await
    .expect("insert seed job");
    content_tsv_backfill::process_job(
        &probe.app_pool,
        coralswarm_backend::jobs::queue::JobRow {
            id: seed_id,
            kind: "ContentTsvRegconfigRepair".to_string(),
            payload: serde_json::json!({}),
            attempts: 0,
            account_id: None,
        },
    )
    .await;
    let (seed_status, seed_err) = job_state(&probe.owner, seed_id).await;
    assert_eq!(seed_status, "done", "the seed must complete (error: {seed_err:?})");
    assert_children_per_account(&probe, "ContentTsvRegconfigRepair", seed_id).await;

    // ── reembed: same seed → per-account fan-out shape ───────────────────────
    let reembed_seed: Uuid = sqlx::query_scalar(
        "INSERT INTO jobs (kind, payload, status, account_id) \
         VALUES ('ReembedOutdatedChunks', '{}'::jsonb, 'running', NULL) RETURNING id",
    )
    .fetch_one(&probe.owner)
    .await
    .expect("insert reembed seed job");
    reembed::process_job(
        &coralswarm_backend::tenancy::TenantId::Consumer,
        &probe.app_pool,
        &store,
        &StubEmbedder,
        None,
        None,
        coralswarm_backend::jobs::queue::JobRow {
            id: reembed_seed,
            kind: "ReembedOutdatedChunks".to_string(),
            payload: serde_json::json!({}),
            attempts: 0,
            account_id: None,
        },
    )
    .await;
    // Cloud re-embedding has no managed purpose: the seed is terminal at
    // admission and the worker refuses it before any per-account fan-out.
    let (rs_status, rs_err) = job_state(&probe.owner, reembed_seed).await;
    assert_eq!(rs_status, "dead");
    assert_eq!(rs_err.as_deref(), Some("managed_purpose_unsupported"));
    let children: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE kind = 'ReembedOutdatedChunks' AND id <> $1")
        .bind(reembed_seed)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(children, 0, "no reembed fan-out without managed admission");

    probe.teardown().await;
}

/// Assert the seed produced exactly one STAMPED child per account — the
/// loop-per-account property for the account-less sweeps.
async fn assert_children_per_account(probe: &RlsProbe, kind: &str, seed_id: Uuid) {
    let stamped: Vec<Uuid> = sqlx::query_scalar(
        "SELECT account_id FROM jobs WHERE kind = $1 AND id <> $2 AND account_id IS NOT NULL \
         ORDER BY account_id",
    )
    .bind(kind)
    .bind(seed_id)
    .fetch_all(&probe.owner)
    .await
    .expect("read child jobs");
    let mut expected = vec![probe.a.account_id, probe.b.account_id];
    expected.sort();
    assert_eq!(
        stamped, expected,
        "{kind} seed must fan out exactly one account-stamped child per account"
    );
    let unstamped: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE kind = $1 AND id <> $2 AND account_id IS NULL",
    )
    .bind(kind)
    .bind(seed_id)
    .fetch_one(&probe.owner)
    .await
    .expect("count unstamped children");
    assert_eq!(unstamped, 0, "{kind} must never re-enqueue an account-less continuation");
}

/// 384-dim constant-vector embedder (matches `atoms.embedding vector(384)`).
struct StubEmbedder;
impl coralswarm_core::embed::EmbedProvider for StubEmbedder {
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> coralswarm_core::embed::EmbedBatchFuture<'a> {
        let n = texts.len();
        Box::pin(async move { Ok(vec![vec![0.1_f32; 384]; n]) })
    }
}
