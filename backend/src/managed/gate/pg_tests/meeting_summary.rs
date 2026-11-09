//! Meeting summaries on `managed_enrichment` (source-agnostic plan PR 10,
//! COR-35): the real `workers::summary` worker against funded two-plane
//! fixtures. Map and reduce calls each write receipts under
//! `managed_enrichment`; the cap, the daily bound and every switch hold the
//! meeting as `pending` without a call and without dead-lettering it; the
//! summary binds the transcript, its version and every chunk atom; and
//! deleting or retracting any bound atom retracts the published summary. The
//! org plane also runs as a Neon-shaped table-owner runtime.
use super::*;
use crate::jobs::{queue, JobKind};
use crate::llm::{CompletionResponse, LlmError, LlmProvider, StreamEvent};
use crate::store::PostgresStore;
use crate::workers::summary;
use crate::workers::tenant_harness::WorkerTenant;
use sqlx::types::Decimal;
use std::ffi::OsString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Process env for one test; restored on drop (`--test-threads=1`).
struct Env(Vec<(&'static str, Option<OsString>)>);

impl Env {
    fn set(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let saved = pairs.iter().map(|(key, _)| (*key, std::env::var_os(key))).collect();
        for (key, value) in pairs {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        Self(saved)
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

/// Production's switches: enrichment on, the summaries' own switch, bound and
/// cap at their defaults.
const ON: &[(&str, Option<&str>)] = &[
    ("ENRICHMENT_PROCESSING_ENABLED", Some("true")),
    ("MANAGED_PROCESSING_DISABLED", None),
    ("MANAGED_DAILY_CAP_USD_ENRICHMENT", None),
    ("MEETING_SUMMARIES_ENABLED", None),
    ("MEETING_SUMMARY_DAILY_CALLS_PER_GRAPH", None),
];

const SOURCE: &str = "teams-standup-1";

/// Scripted provider: answers by tool, counts calls, can run one SQL
/// statement mid-call (an edit racing the call).
struct Script {
    calls: AtomicUsize,
    tasks: std::sync::Mutex<Vec<String>>,
    prompts: std::sync::Mutex<Vec<String>>,
    during: tokio::sync::Mutex<Option<(PgPool, String)>>,
}

impl Script {
    fn new() -> Self {
        Self { calls: AtomicUsize::new(0), tasks: Default::default(), prompts: Default::default(), during: tokio::sync::Mutex::new(None) }
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    async fn during(&self, pool: &PgPool, sql: String) {
        *self.during.lock().await = Some((pool.clone(), sql));
    }
}

#[async_trait::async_trait]
impl LlmProvider for Script {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let tool = req.tool_schema.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        let content = if tool == "emit_meeting_summary" {
            if let Some((pool, sql)) = self.during.lock().await.take() {
                sqlx::raw_sql(&sql).execute(&pool).await.expect("mid-call edit");
            }
            serde_json::json!({
                "overview": "The team moved meeting summaries onto managed enrichment.",
                "decisions": [{"text": "Meter every summary call", "ts_ms": null}],
                "action_items": [{"text": "Ship the managed summary worker", "owner": "Alice", "ts_ms": null}],
            })
        } else {
            serde_json::json!({
                "decisions": [{"text": "Meter every summary call", "ts_ms": null}],
                "action_items": [{"text": "Ship the managed summary worker", "owner": "Alice", "ts_ms": null}],
            })
        };
        self.tasks.lock().unwrap().push(tool);
        self.prompts.lock().unwrap().push(req.messages[0].content.clone());
        Ok(CompletionResponse {
            content: content.to_string(),
            input_tokens: 900,
            output_tokens: 40,
            model: req.model.clone(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: None,
            cost_usd: Some(0.00011),
        })
    }
    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError> {
        unimplemented!("summaries never stream")
    }
}

async fn funded(org: bool) -> Fixture {
    let f = Fixture::new(org).await;
    f.balance(Decimal::from(100)).await;
    f
}

fn worker(f: &Fixture, pool: &PgPool) -> WorkerTenant {
    WorkerTenant { tenant: f.tenant.clone(), pool: pool.clone(), store: Arc::new(PostgresStore::new(pool.clone())) }
}

/// A captured server-side transcript (one retained version) with two chunk
/// atoms bound to that version, as `routes::ingest` leaves it. Returns the
/// chunk atoms.
async fn meeting(f: &Fixture) -> Vec<Uuid> {
    let raw = "Alice: We should meter every summary call.\nBob: Agreed, ship the managed worker this week.";
    let mut tx = match f.tenant {
        TenantId::Org(_) => crate::tenancy::scope::begin_tenant_system_scoped(f.app(), f.account).await.unwrap(),
        TenantId::Consumer => begin_account_scoped(f.app(), f.account).await.unwrap(),
    };
    let source = crate::source_evidence::CaptureSource {
        source_type: "transcript",
        provider_namespace: "legacy:meeting",
        source_id: SOURCE,
        normalized_text: raw,
        representation: "normalized_text",
        metadata: &serde_json::json!({}),
        occurred_at: None,
        participants: &[],
        ingested_by: Some(f.payer),
        scope: "private",
        duration_ms: Some(60_000),
        segments: None,
    };
    let saved = crate::source_evidence::capture_source_version_tx(&mut tx, None, f.graph, f.account, &source, None, &[43; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut atoms = Vec::new();
    for (n, line) in raw.lines().enumerate() {
        let atom: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by) \
             VALUES($1,'transcript',$2,'fixture-only',$3,now(),$4) RETURNING atom_id",
        )
        .bind(f.graph)
        .bind(format!("{SOURCE}:chunk:{n}"))
        .bind(line)
        .bind(f.payer)
        .fetch_one(f.owner())
        .await
        .unwrap();
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
            .bind(f.graph)
            .bind(saved.version_id)
            .bind(atom)
            .execute(f.owner())
            .await
            .unwrap();
        atoms.push(atom);
    }
    atoms
}

/// Queue the summary the way ingest does. A managed_enrichment kind is
/// admitted as `pending`, never dead-lettered.
async fn enqueue(f: &Fixture, pool: &PgPool) -> Uuid {
    let id = queue::enqueue(
        pool,
        &JobKind::GenerateMeetingSummary { graph_id: f.graph, source_id: SOURCE.into(), allow_cloud_override: false },
        Some(f.account),
        Some(&format!("summary:{}:{SOURCE}", f.graph)),
        0,
    )
    .await
    .unwrap()
    .expect("a fresh summary job");
    assert_eq!(jobs(f).await, vec![("pending".to_string(), None)], "admitted, not managed_purpose_unsupported");
    id
}

async fn tick(f: &Fixture, pool: &PgPool, llm: &Script) -> usize {
    summary::tick(&worker(f, pool), &f.control.app, &crate::llm::testing::GatedScript(llm), None).await.unwrap().processed
}

async fn jobs(f: &Fixture) -> Vec<(String, Option<String>)> {
    sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE kind='GenerateMeetingSummary' ORDER BY created_at")
        .fetch_all(f.owner())
        .await
        .unwrap()
}

/// (summary_status, summary, summary_error)
async fn transcript(f: &Fixture) -> (String, Option<serde_json::Value>, Option<String>) {
    sqlx::query_as("SELECT summary_status,summary,summary_error FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2")
        .bind(f.graph)
        .bind(SOURCE)
        .fetch_one(f.owner())
        .await
        .unwrap()
}

/// (receipt, purpose, state, job)
async fn receipts(f: &Fixture) -> Vec<(Uuid, String, String, Option<Uuid>)> {
    sqlx::query_as("SELECT receipt_id,purpose,state,job_id FROM managed_attempt_receipts WHERE account_id=$1 ORDER BY created_at")
        .bind(f.account)
        .fetch_all(f.owner())
        .await
        .unwrap()
}

async fn inputs_of(f: &Fixture, receipt: Uuid) -> Vec<(String, String)> {
    sqlx::query_as("SELECT input_kind,outcome FROM managed_enrichment_inputs WHERE receipt_id=$1 ORDER BY ordinal")
        .bind(receipt)
        .fetch_all(f.owner())
        .await
        .unwrap()
}

/// The meeting is back to `pending`, no job is dead, and exactly one pending
/// pass waits until at least `min_delay_secs` from now.
async fn assert_held(f: &Fixture, what: &str, min_delay_secs: i64) {
    let rows = jobs(f).await;
    assert!(rows.iter().all(|(s, _)| s != "dead"), "{what}: never dead-lettered: {rows:?}");
    let later: Vec<bool> = sqlx::query_scalar(
        "SELECT run_after > now() + make_interval(secs => $1) FROM jobs WHERE kind='GenerateMeetingSummary' AND status='pending'",
    )
    .bind(min_delay_secs as f64)
    .fetch_all(f.owner())
    .await
    .unwrap();
    assert_eq!(later, vec![true], "{what}: one pending pass, rescheduled for later");
    assert_eq!(transcript(f).await.0, "pending", "{what}: the meeting stays pending");
}

async fn summary_publishes_with_receipts(org: bool, owner: bool) {
    let _env = Env::set(ON);
    let f = funded(org).await;
    let runtime = if owner { super::entity_resolution::owner_runtime(&f).await } else { f.app().clone() };
    let atoms = meeting(&f).await;
    let job = enqueue(&f, &runtime).await;
    let llm = Script::new();
    assert_eq!(tick(&f, &runtime, &llm).await, 1);
    assert_eq!(llm.calls(), 2, "one map chunk and the reduce");
    assert_eq!(*llm.tasks.lock().unwrap(), ["emit_meeting_extraction", "emit_meeting_summary"]);

    let (status, published, error) = transcript(&f).await;
    assert_eq!((status.as_str(), error), ("done", None));
    assert_eq!(published.unwrap()["overview"], "The team moved meeting summaries onto managed enrichment.");
    assert_eq!(jobs(&f).await, vec![("done".to_string(), None)]);

    let receipts = receipts(&f).await;
    assert_eq!(receipts.len(), 2, "one receipt per provider attempt, one attempt per call");
    for (_, purpose, state, job_id) in &receipts {
        assert_eq!((purpose.as_str(), state.as_str(), *job_id), ("managed_enrichment", "billed", Some(job)));
    }
    let map = inputs_of(&f, receipts[0].0).await;
    let reduce = inputs_of(&f, receipts[1].0).await;
    let kinds = |rows: &[(String, String)]| rows.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    assert_eq!(kinds(&map), ["meeting_transcript", "source_version", "atom", "atom", "summary_map_prompt"]);
    assert_eq!(kinds(&reduce), ["meeting_transcript", "source_version", "atom", "atom", "summary_reduce_prompt"]);
    assert!(reduce.iter().all(|(_, o)| o == "written"), "the published call is marked written: {reduce:?}");
    let bound: Vec<Uuid> = sqlx::query_scalar(
        "SELECT input_id FROM managed_enrichment_inputs WHERE receipt_id=$1 AND input_kind='atom' ORDER BY input_id",
    )
    .bind(receipts[1].0)
    .fetch_all(f.owner())
    .await
    .unwrap();
    let mut expected = atoms.clone();
    expected.sort();
    assert_eq!(bound, expected, "every chunk atom of the version is bound");

    let usage: (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE source_job_id=$1) FROM llm_usage WHERE task IN ('summary_map','summary_reduce')",
    )
    .bind(job)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(usage, (2, 2), "usage rows on the content plane, against the job");
    for _ in 0..2 {
        crate::workers::billing_derivation::tick(&worker(&f, f.app()), &f.control.app).await.unwrap();
    }
    let debits: Vec<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT entry_kind,source_receipt_id FROM credit_ledger WHERE account_id=$1 AND entry_kind LIKE 'usage_%' ORDER BY source_receipt_id",
    )
    .bind(f.account)
    .fetch_all(&f.control.owner)
    .await
    .unwrap();
    let mut want: Vec<(String, Option<Uuid>)> = receipts.iter().map(|r| ("usage_managed".to_string(), Some(r.0))).collect();
    want.sort_by_key(|d| d.1);
    assert_eq!(debits, want, "one managed debit per receipt, nothing on the job axis");
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn meeting_summary_publishes_with_receipts_consumer() {
    summary_publishes_with_receipts(false, false).await;
}
#[tokio::test]
async fn meeting_summary_publishes_with_receipts_tenant_app_role() {
    summary_publishes_with_receipts(true, false).await;
}
#[tokio::test]
async fn meeting_summary_publishes_with_receipts_tenant_owner_runtime() {
    summary_publishes_with_receipts(true, true).await;
}

/// The shared cap and the per-graph daily bound hold before any send: no
/// call, no receipt, the meeting back to `pending`, the job rescheduled for
/// the next UTC day and never dead-lettered. It then runs once the hold
/// lifts.
#[tokio::test]
async fn meeting_summary_cap_and_bound_hold_without_dead_letter_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        meeting(&f).await;
        enqueue(&f, f.app()).await;
        let llm = Script::new();
        for (var, value) in [("MANAGED_DAILY_CAP_USD_ENRICHMENT", "0.000001"), ("MEETING_SUMMARY_DAILY_CALLS_PER_GRAPH", "1")] {
            let _held = Env::set(&[(var, Some(value))]);
            assert_eq!(tick(&f, f.app(), &llm).await, 1);
            assert_held(&f, var, 30).await;
            sqlx::query("UPDATE jobs SET run_after=now() WHERE kind='GenerateMeetingSummary' AND status='pending'")
                .execute(f.owner())
                .await
                .unwrap();
        }
        assert_eq!((llm.calls(), receipts(&f).await.len()), (0, 0), "held: nothing sent, no receipt");
        assert_eq!(tick(&f, f.app(), &llm).await, 1);
        assert_eq!(llm.calls(), 2, "the held meeting runs once the hold lifts");
        assert_eq!(transcript(&f).await.0, "done");
        f.finish().await;
    }
}

/// With the shared switch off, the global kill switch on, or the summaries'
/// own switch off, nothing is claimed or called and the work stays pending.
/// A switch flipped after the claim hands the job back as pending.
#[tokio::test]
async fn meeting_summary_switches_off_call_nothing_and_stay_pending_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        meeting(&f).await;
        enqueue(&f, f.app()).await;
        let llm = Script::new();
        for (var, value) in [
            ("ENRICHMENT_PROCESSING_ENABLED", None),
            ("MANAGED_PROCESSING_DISABLED", Some("true")),
            ("MEETING_SUMMARIES_ENABLED", Some("false")),
        ] {
            let _off = Env::set(&[(var, value)]);
            assert_eq!(tick(&f, f.app(), &llm).await, 0, "{var}: nothing claimed");
            assert_eq!(jobs(&f).await, vec![("pending".to_string(), None)], "{var}");
            assert_eq!(transcript(&f).await.0, "pending", "{var}");
        }
        // Claimed while on, switched off before processing.
        let claimed = queue::dequeue(f.app(), &["GenerateMeetingSummary"], 1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        {
            let _off = Env::set(&[("ENRICHMENT_PROCESSING_ENABLED", None)]);
            let store = PostgresStore::new(f.app().clone());
            let gated = crate::llm::testing::GatedScript(&llm);
            for job in claimed {
                summary::process_job(&f.tenant, f.app(), &f.control.app, &store, &gated, None, job).await;
            }
        }
        assert_held(&f, "switched off after claim", 60).await;
        assert_eq!((llm.calls(), receipts(&f).await.len()), (0, 0));
        f.finish().await;
    }
}

async fn release_pending(f: &Fixture) {
    sqlx::query("UPDATE jobs SET run_after=now() WHERE kind='GenerateMeetingSummary' AND status='pending'")
        .execute(f.owner())
        .await
        .unwrap();
}

/// Every input is bound, so deleting or retracting any chunk atom retracts
/// the published summary and queues a regeneration built only from the
/// evidence that is left (no raw shortcut: the gone chunk's text never
/// reaches the model). With no chunk left the retry is refused without a
/// call. An edit racing the reduce call publishes nothing and holds.
#[tokio::test]
async fn meeting_summary_input_deletion_retracts_the_summary_both_planes() {
    let _env = Env::set(ON);
    for (org, retract) in [(false, false), (true, false), (false, true), (true, true)] {
        let what = format!("org={org} retract={retract}");
        let f = funded(org).await;
        let atoms = meeting(&f).await;
        let gone = |sql: &'static str| if retract { "UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1" } else { sql };

        // An edit racing the reduce call: billed, nothing published, held.
        enqueue(&f, f.app()).await;
        let llm = Script::new();
        llm.during(f.owner(), format!("UPDATE atoms SET content_inline='Alice: We should meter every summary call, edited.' WHERE atom_id='{}'", atoms[0])).await;
        tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 2);
        let reduce = receipts(&f).await[1].0;
        assert!(inputs_of(&f, reduce).await.iter().all(|(_, o)| o == "discarded_stale"), "{what}");
        assert_held(&f, "edited during the call", 60).await;
        assert!(transcript(&f).await.1.is_none(), "{what}: nothing published from an edited chunk");

        // The held pass runs again and publishes.
        release_pending(&f).await;
        tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 4);
        assert_eq!(transcript(&f).await.0, "done", "{what}");

        // Deleting (or retracting) one bound chunk retracts the summary and
        // queues one regeneration.
        sqlx::query(gone("DELETE FROM atoms WHERE atom_id=$1")).bind(atoms[1]).execute(f.owner()).await.unwrap();
        assert_eq!(transcript(&f).await, ("pending".to_string(), None, None), "{what}: the summary built from the gone chunk is cleared");
        let pending = jobs(&f).await.into_iter().filter(|(s, _)| s == "pending").count();
        assert_eq!(pending, 1, "{what}: one regeneration queued");

        // The regeneration reads only the evidence that is left.
        tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 6, "{what}");
        assert_eq!(transcript(&f).await.0, "done", "{what}");
        let prompts = llm.prompts.lock().unwrap().clone();
        assert!(prompts[4].contains("meter every summary call"), "{what}: the remaining chunk is summarised");
        assert!(!prompts[4].contains("ship the managed worker"), "{what}: the gone chunk never reaches the model: {}", prompts[4]);
        let latest = receipts(&f).await.last().unwrap().0;
        let atoms_bound: Vec<Uuid> = sqlx::query_scalar("SELECT input_id FROM managed_enrichment_inputs WHERE receipt_id=$1 AND input_kind='atom'")
            .bind(latest).fetch_all(f.owner()).await.unwrap();
        assert_eq!(atoms_bound, vec![atoms[0]], "{what}: only the remaining chunk is bound");

        // With no chunk left, the retry is refused without a call.
        sqlx::query(gone("DELETE FROM atoms WHERE atom_id=$1")).bind(atoms[0]).execute(f.owner()).await.unwrap();
        assert_eq!(transcript(&f).await.1, None, "{what}");
        tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 6, "{what}: refused without a model call");
        assert_eq!(transcript(&f).await, ("failed".to_string(), None, Some("evidence unavailable".to_string())), "{what}");
        assert!(jobs(&f).await.iter().all(|(s, _)| s == "done"), "{what}: never dead-lettered");
        f.finish().await;
    }
}
