//! The journal sweep, coral titles and reef digests on `managed_enrichment`
//! (source-agnostic plan PRs 6-8): the real workers against funded two-plane
//! fixtures. Each kind: receipts under `managed_enrichment`, stale inputs
//! publish nothing, the shared cap and the daily bound hold without
//! dead-lettering, the per-kind switch, and one bill per receipt. Deletion
//! invalidation runs through `managed_enrichment_inputs`. The org plane also
//! runs as a Neon-shaped table-owner runtime.
use super::*;
use crate::jobs::{queue, JobKind};
use crate::llm::{CompletionResponse, LlmError, LlmProvider, StreamEvent};
use crate::store::PostgresStore;
use crate::workers::tenant_harness::WorkerTenant;
use crate::workers::{atom_title_backfill, digest, journal_sweep};
use chrono::{DateTime, Duration as ChronoDuration, DurationRound, Utc};
use coralswarm_core::embed::{EmbedBatchFuture, EmbedProvider};
use sqlx::types::Decimal;
use std::ffi::OsString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Process env for one test; restored on drop (the suite runs with
/// `--test-threads=1`).
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

/// Production's switches (ENRICHMENT on) with every consumer's own switch,
/// bound and cap at its default, Jev off.
const ON: &[(&str, Option<&str>)] = &[
    ("ENRICHMENT_PROCESSING_ENABLED", Some("true")),
    ("MANAGED_PROCESSING_DISABLED", None),
    ("MANAGED_DAILY_CAP_USD_ENRICHMENT", None),
    ("JOURNAL_SWEEP_ENABLED", None),
    ("JOURNAL_DAILY_CALLS_PER_GRAPH", None),
    ("JOURNAL_EPOCH", None),
    ("JOURNAL_TZ", None),
    ("ATOM_TITLES_ENABLED", None),
    ("ATOM_TITLES_DAILY_CALLS_PER_ACCOUNT", None),
    ("DIGESTS_ENABLED", Some("true")),
    ("DIGEST_DAILY_CALLS_PER_GRAPH", None),
    ("DIGEST_DAILY_USD_PER_ACCOUNT", None),
    ("DIGEST_DAILY_SHARE_OF_ENRICHMENT", None),
    ("ER_DAILY_SHARE_OF_ENRICHMENT", None),
    ("ER_DAILY_CALLS_PER_GRAPH", None),
    ("ER_BLOCKING", None),
    ("ER_PASS_BUDGET_SECS", None),
    ("RESOLVE_ESCALATE_MODEL", None),
    ("JEV_JUDGE_MODE", None),
    ("JEV_PROVIDER", None),
];

/// Scripted provider: answers by task (system prompt), counts calls, can run
/// one SQL statement mid-call (an edit racing the call).
struct Script {
    calls: AtomicUsize,
    jev_calls: AtomicUsize,
    tasks: std::sync::Mutex<Vec<String>>,
    during: tokio::sync::Mutex<Option<(PgPool, String)>>,
    /// The provider-reported cost of every call (what the gate settles).
    cost_usd: f64,
}

impl Script {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            jev_calls: AtomicUsize::new(0),
            tasks: Default::default(),
            during: tokio::sync::Mutex::new(None),
            cost_usd: 0.00011,
        }
    }
    fn costing(cost_usd: f64) -> Self {
        Self { cost_usd, ..Self::new() }
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    async fn during(&self, pool: &PgPool, sql: String) {
        *self.during.lock().await = Some((pool.clone(), sql));
    }
}

fn titles_reply(prompt: &str) -> String {
    let mut titles = Vec::new();
    for (i, part) in prompt.split("\n\n[").enumerate() {
        let index = i + 1;
        let title = if part.contains("contentless") { "none".to_string() } else { format!("Headline {index} from the batch") };
        titles.push(serde_json::json!({"index": index, "title": title}));
    }
    serde_json::json!({ "titles": titles }).to_string()
}

#[async_trait::async_trait]
impl LlmProvider for Script {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((pool, sql)) = self.during.lock().await.take() {
            sqlx::raw_sql(&sql).execute(&pool).await.expect("mid-call edit");
        }
        let system = req.system.clone().unwrap_or_default();
        let prompt = req.messages[0].content.clone();
        let jev = req.tool_schema.as_ref().is_some_and(|t| t.name == crate::decision::openrouter::TOOL_NAME);
        let (task, content) = if jev {
            self.jev_calls.fetch_add(1, Ordering::SeqCst);
            let answers: serde_json::Map<String, serde_json::Value> =
                (0..64).map(|i| (format!("q{i}"), serde_json::json!(0.95))).collect();
            ("jev", serde_json::Value::Object(answers).to_string())
        } else if req.tool_schema.as_ref().is_some_and(|t| t.name == "emit_titles") {
            ("titles", titles_reply(&prompt))
        } else if prompt.starts_with("Reef title:") {
            ("digest", r#"{"summary":"Postgres runs the tenant databases.","excerpts":[{"index":1,"excerpt":"Postgres fact 0","contribution":"names the host"}]}"#.to_string())
        } else {
            let _ = system;
            ("journal", "- Worked through the managed journal fixture".to_string())
        };
        self.tasks.lock().unwrap().push(task.to_string());
        Ok(CompletionResponse {
            content,
            input_tokens: 900,
            output_tokens: 40,
            model: req.model.clone(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: None,
            cost_usd: Some(self.cost_usd),
        })
    }
    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError> {
        unimplemented!("managed consumers never stream")
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

async fn atom(f: &Fixture, source_id: &str, at: DateTime<Utc>, text: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by) \
         VALUES($1,'note',$2,'fixture-only',$3,$4,$5) RETURNING atom_id",
    )
    .bind(f.graph)
    .bind(source_id)
    .bind(text)
    .bind(at)
    .bind(f.payer)
    .fetch_one(f.owner())
    .await
    .unwrap()
}

async fn count(f: &Fixture, sql: &str) -> i64 {
    sqlx::query_scalar(sql).bind(f.graph).fetch_one(f.owner()).await.unwrap()
}

/// Every receipt on this account: (purpose, state, attributed user).
async fn receipts(f: &Fixture) -> Vec<(Uuid, String, String, Option<Uuid>, Option<Uuid>)> {
    sqlx::query_as(
        "SELECT receipt_id,purpose,state,attributed_user_id,job_id FROM managed_attempt_receipts WHERE account_id=$1 ORDER BY created_at",
    )
    .bind(f.account)
    .fetch_all(f.owner())
    .await
    .unwrap()
}

async fn outcomes(f: &Fixture) -> Vec<(String, String)> {
    sqlx::query_as("SELECT input_kind,outcome FROM managed_enrichment_inputs WHERE graph_id=$1 ORDER BY receipt_id,ordinal")
        .bind(f.graph)
        .fetch_all(f.owner())
        .await
        .unwrap()
}

/// Two derivation passes; every `usage_%` debit must be one managed debit
/// per charged receipt, never a job- or journal-axis debit.
async fn assert_billed_once(f: &Fixture, pool: &PgPool) {
    for _ in 0..2 {
        crate::workers::billing_derivation::tick(&worker(f, pool), &f.control.app).await.unwrap();
    }
    let debits: Vec<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT entry_kind,source_receipt_id FROM credit_ledger WHERE account_id=$1 AND entry_kind LIKE 'usage_%' ORDER BY source_receipt_id",
    )
    .bind(f.account)
    .fetch_all(&f.control.owner)
    .await
    .unwrap();
    let mut expected: Vec<(String, Option<Uuid>)> =
        receipts(f).await.into_iter().map(|r| ("usage_managed".to_string(), Some(r.0))).collect();
    expected.sort_by_key(|d| d.1);
    assert!(!expected.is_empty());
    assert_eq!(debits, expected, "one managed debit per receipt, nothing on the job or journal axes");
    let keyed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM llm_usage WHERE source_journal_period IS NOT NULL OR source_query_id IS NOT NULL",
    )
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(keyed, 0, "managed usage rows carry no journal or query key");
}

// ── Journal (PR 6) ───────────────────────────────────────────────────────────

/// A closed hour two days back, so the hour and its UTC day are both closed.
fn closed_hour() -> DateTime<Utc> {
    (Utc::now() - ChronoDuration::days(2)).duration_trunc(ChronoDuration::hours(1)).unwrap()
}

async fn journal_compiles_with_receipts(org: bool, owner: bool) {
    let _env = Env::set(ON);
    let f = funded(org).await;
    let runtime = if owner { super::entity_resolution::owner_runtime(&f).await } else { f.app().clone() };
    let hour = closed_hour();
    atom(&f, "j1", hour + ChronoDuration::minutes(5), "Moved the journal sweep onto managed enrichment.").await;
    atom(&f, "j2", hour + ChronoDuration::minutes(9), "Bounded the sweep per graph per day.").await;
    let llm = Script::new();
    let tick = journal_sweep::sweep_pass(&worker(&f, &runtime), &f.control.app, &crate::llm::testing::GatedScript(&llm), Utc::now())
        .await
        .unwrap();
    assert!(tick.processed >= 2, "the hour and its day compiled: {tick:?}");
    let hour_rows = count(&f, "SELECT count(*) FROM journal_rollups WHERE graph_id=$1 AND granularity='hour'").await;
    let day_rows = count(&f, "SELECT count(*) FROM journal_rollups WHERE graph_id=$1 AND granularity='day'").await;
    assert_eq!((hour_rows, day_rows), (1, 1));
    let receipts = receipts(&f).await;
    assert_eq!(receipts.len(), llm.calls(), "one receipt per call");
    for (_, purpose, state, payer, job) in &receipts {
        assert_eq!((purpose.as_str(), state.as_str(), *payer, *job), ("managed_enrichment", "billed", Some(f.payer), None),
            "charged to the attributed user, no job row");
    }
    let outcomes = outcomes(&f).await;
    assert!(outcomes.iter().all(|(_, o)| o == "written"), "{outcomes:?}");
    let kinds: std::collections::BTreeSet<&str> = outcomes.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(kinds, ["atom", "journal_prompt", "journal_rollup"].into_iter().collect(), "atoms, child rollups and the prompt are bound");
    // A second pass finds nothing dirty and spends nothing.
    journal_sweep::sweep_pass(&worker(&f, &runtime), &f.control.app, &crate::llm::testing::GatedScript(&llm), Utc::now()).await.unwrap();
    assert_eq!(llm.calls(), receipts.len(), "clean hours are not recompiled");
    assert_billed_once(&f, f.app()).await;
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn journal_compiles_with_receipts_consumer() {
    journal_compiles_with_receipts(false, false).await;
}
#[tokio::test]
async fn journal_compiles_with_receipts_tenant_app_role() {
    journal_compiles_with_receipts(true, false).await;
}
#[tokio::test]
async fn journal_compiles_with_receipts_tenant_owner_runtime() {
    journal_compiles_with_receipts(true, true).await;
}

/// An hour input edited during the call: billed, nothing published, the hour
/// stays dirty and no day is built on it.
#[tokio::test]
async fn journal_stale_inputs_publish_nothing_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let hour = closed_hour();
        let a = atom(&f, "s1", hour + ChronoDuration::minutes(5), "Original text.").await;
        let llm = Script::new();
        llm.during(f.owner(), format!("UPDATE atoms SET content_inline='Edited during the call.' WHERE atom_id='{a}'")).await;
        journal_sweep::sweep_pass(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&llm), Utc::now()).await.unwrap();
        assert_eq!(llm.calls(), 1);
        assert_eq!(count(&f, "SELECT count(*) FROM journal_rollups WHERE graph_id=$1").await, 0, "nothing published");
        assert!(outcomes(&f).await.iter().all(|(_, o)| o == "discarded_stale"));
        assert_eq!(receipts(&f).await[0].2, "billed", "the sent call is still charged");
        f.finish().await;
    }
}

/// The daily bound and the shared cap hold before any send; the switch and
/// the shared switch keep the sweep idle without touching the plane.
#[tokio::test]
async fn journal_bound_cap_and_switches_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let hour = closed_hour();
        atom(&f, "b1", hour + ChronoDuration::minutes(5), "First hour.").await;
        atom(&f, "b2", hour - ChronoDuration::hours(3) + ChronoDuration::minutes(5), "An earlier hour.").await;
        let llm = Script::new();
        let gated = crate::llm::testing::GatedScript(&llm);
        {
            let _off = Env::set(&[("JOURNAL_SWEEP_ENABLED", Some("false"))]);
            assert_eq!(journal_sweep::tick(&worker(&f, f.app()), &f.control.app, &gated).await.unwrap().processed, 0);
            assert_eq!(journal_sweep::sweep_pass(&worker(&f, f.app()), &f.control.app, &gated, Utc::now()).await.unwrap().processed, 0);
        }
        {
            let _off = Env::set(&[("ENRICHMENT_PROCESSING_ENABLED", None)]);
            assert_eq!(journal_sweep::tick(&worker(&f, f.app()), &f.control.app, &gated).await.unwrap().processed, 0);
        }
        {
            let _cap = Env::set(&[("MANAGED_DAILY_CAP_USD_ENRICHMENT", Some("0.000001"))]);
            journal_sweep::sweep_pass(&worker(&f, f.app()), &f.control.app, &gated, Utc::now()).await.unwrap();
        }
        assert_eq!(llm.calls(), 0, "switched off or capped: nothing sent");
        assert_eq!(receipts(&f).await.len(), 0, "a held reservation leaves no receipt");
        {
            let _bound = Env::set(&[("JOURNAL_DAILY_CALLS_PER_GRAPH", Some("1"))]);
            for _ in 0..2 {
                journal_sweep::sweep_pass(&worker(&f, f.app()), &f.control.app, &gated, Utc::now()).await.unwrap();
            }
        }
        assert_eq!(llm.calls(), 1, "one compile per graph per day under a bound of 1, across passes");
        assert_eq!(count(&f, "SELECT count(*) FROM journal_rollups WHERE graph_id=$1").await, 1);
        f.finish().await;
    }
}

// ── Coral titles (PR 7) ──────────────────────────────────────────────────────

async fn fallback_atom(f: &Fixture, source_id: &str, text: &str, kind: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by,title,title_kind) \
         VALUES($1,'slack',$2,'fixture-only',$3,now(),$4,$5,$6) RETURNING atom_id",
    )
    .bind(f.graph)
    .bind(source_id)
    .bind(text)
    .bind(f.payer)
    .bind(format!("old title of {source_id}"))
    .bind(kind)
    .fetch_one(f.owner())
    .await
    .unwrap()
}

async fn title_of(f: &Fixture, atom: Uuid) -> (Option<String>, Option<String>) {
    sqlx::query_as("SELECT title,title_kind FROM atoms WHERE atom_id=$1").bind(atom).fetch_one(f.owner()).await.unwrap()
}

async fn enqueue_titles(f: &Fixture, pool: &PgPool) -> Uuid {
    queue::enqueue(pool, &JobKind::BackfillAtomTitles { after_atom_id: None }, Some(f.account), None, 0)
        .await
        .unwrap()
        .expect("a fresh batch")
}

async fn job_state(f: &Fixture, job: Uuid) -> String {
    sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1").bind(job).fetch_one(f.owner()).await.unwrap()
}

async fn titles_tick(f: &Fixture, pool: &PgPool, llm: &Script) -> usize {
    atom_title_backfill::tick(
        &worker(f, pool),
        &f.control.app,
        Some(&crate::llm::testing::GatedScript(llm)),
        None,
        &atom_title_backfill::SeedClock::new(),
    )
    .await
    .unwrap()
    .processed
}

async fn titles_batch_with_receipts(org: bool, owner: bool) {
    let _env = Env::set(ON);
    let f = funded(org).await;
    let runtime = if owner { super::entity_resolution::owner_runtime(&f).await } else { f.app().clone() };
    let a = fallback_atom(&f, "t1", "Raised the managed enrichment cap for titles.", "fallback").await;
    let b = fallback_atom(&f, "t2", "Wired journal sweep receipts.", "fallback").await;
    let blank = fallback_atom(&f, "t3", "contentless", "fallback").await;
    let recipe = fallback_atom(&f, "t4", "COR-1 body text.", "source").await;
    let model = fallback_atom(&f, "t5", "Already titled by the model.", "llm").await;
    let job = enqueue_titles(&f, &runtime).await;
    let llm = Script::new();
    assert_eq!(titles_tick(&f, &runtime, &llm).await, 1);
    assert_eq!(llm.calls(), 1, "one call for the whole batch");
    assert_eq!(job_state(&f, job).await, "done");
    for id in [a, b] {
        let (title, kind) = title_of(&f, id).await;
        assert!(title.unwrap().starts_with("Headline"), "model headline stored");
        assert_eq!(kind.as_deref(), Some("llm"));
    }
    let (title, kind) = title_of(&f, blank).await;
    assert_eq!((title.as_deref(), kind.as_deref()), (Some("contentless"), Some("declined")),
        "a declined coral keeps its fallback and is not asked again");
    assert_eq!(title_of(&f, recipe).await, (Some("old title of t4".into()), Some("source".into())), "a recipe title is never overwritten");
    assert_eq!(title_of(&f, model).await, (Some("old title of t5".into()), Some("llm".into())), "a model title is never overwritten");
    let receipts = receipts(&f).await;
    assert_eq!(receipts.len(), 1);
    assert_eq!((receipts[0].1.as_str(), receipts[0].2.as_str(), receipts[0].4), ("managed_enrichment", "billed", Some(job)));
    let outcomes = outcomes(&f).await;
    assert_eq!(outcomes.iter().filter(|(k, _)| k == "atom").count(), 3, "the three candidates are bound");
    assert!(outcomes.iter().any(|(k, _)| k == "title_batch"));
    assert!(outcomes.iter().all(|(_, o)| o == "written"));
    let usage: (i64, i64) = sqlx::query_as("SELECT count(*), count(*) FILTER (WHERE source_job_id=$1) FROM llm_usage WHERE task='atom_title'")
        .bind(job).fetch_one(f.owner()).await.unwrap();
    assert_eq!(usage, (1, 1), "usage is recorded on the content plane, against the job");
    // A second batch finds no candidate and spends nothing.
    enqueue_titles(&f, &runtime).await;
    titles_tick(&f, &runtime, &llm).await;
    assert_eq!(llm.calls(), 1);
    assert_billed_once(&f, f.app()).await;
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn titles_batch_with_receipts_consumer() {
    titles_batch_with_receipts(false, false).await;
}
#[tokio::test]
async fn titles_batch_with_receipts_tenant_app_role() {
    titles_batch_with_receipts(true, false).await;
}
#[tokio::test]
async fn titles_batch_with_receipts_tenant_owner_runtime() {
    titles_batch_with_receipts(true, true).await;
}

#[tokio::test]
async fn titles_stale_inputs_publish_nothing_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let a = fallback_atom(&f, "s1", "Original body.", "fallback").await;
        let b = fallback_atom(&f, "s2", "Another body.", "fallback").await;
        let llm = Script::new();
        llm.during(f.owner(), format!("UPDATE atoms SET content_inline='Edited during the call.' WHERE atom_id='{a}'")).await;
        let job = enqueue_titles(&f, f.app()).await;
        titles_tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 1);
        for id in [a, b] {
            assert_eq!(title_of(&f, id).await.1.as_deref(), Some("fallback"), "nothing published from a stale batch");
        }
        assert!(outcomes(&f).await.iter().all(|(_, o)| o == "discarded_stale"));
        assert_eq!(job_state(&f, job).await, "done");
        f.finish().await;
    }
}

/// The account's daily bound and the shared cap stop the batch before any
/// send; the same job, held in place (`jobs::queue::hold_until`), resumes
/// from the same cursor after the UTC day turns, never dead-lettered and
/// with no fresh row. The switch leaves the queue untouched.
#[tokio::test]
async fn titles_bound_cap_and_switch_reschedule_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        fallback_atom(&f, "c1", "A body to title.", "fallback").await;
        let llm = Script::new();
        for (var, value) in [("ATOM_TITLES_DAILY_CALLS_PER_ACCOUNT", "0"), ("MANAGED_DAILY_CAP_USD_ENRICHMENT", "0.000001")] {
            let _held = Env::set(&[(var, Some(value))]);
            let job = enqueue_titles(&f, f.app()).await;
            titles_tick(&f, f.app(), &llm).await;
            let held: Vec<(Uuid, String, bool, i32)> = sqlx::query_as(
                "SELECT id, status::text, run_after > now() + interval '30 seconds', hold_streak FROM jobs WHERE kind='BackfillAtomTitles'",
            )
            .fetch_all(f.owner())
            .await
            .unwrap();
            assert_eq!(held, vec![(job, "pending".into(), true, 0)],
                "{var}: the same job, held in place, resumes after the UTC day turns; a known clear time leaves the streak alone");
            sqlx::query("DELETE FROM jobs WHERE kind='BackfillAtomTitles'").execute(f.owner()).await.unwrap();
        }
        assert_eq!((llm.calls(), receipts(&f).await.len()), (0, 0));
        {
            let _off = Env::set(&[("ATOM_TITLES_ENABLED", Some("false"))]);
            let job = enqueue_titles(&f, f.app()).await;
            assert_eq!(titles_tick(&f, f.app(), &llm).await, 0, "nothing claimed while off");
            assert_eq!(job_state(&f, job).await, "pending");
        }
        assert_eq!(llm.calls(), 0);
        f.finish().await;
    }
}

// ── Reef digests (PR 8) ──────────────────────────────────────────────────────

const DIM: usize = 384;

fn axis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[i] = 1.0;
    v
}

/// The title embeds to axis 0; each coral to axis 0 + its own axis (cosine
/// 0.707 to the title, 0.5 to each other), so all eight are selected.
struct TitleEmbedder;
impl EmbedProvider for TitleEmbedder {
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        Box::pin(async move { Ok(texts.iter().map(|_| axis(0)).collect()) })
    }
}

/// An entity reef with eight corals that name it. Linking the eighth crosses
/// the eligibility bar, and the coral-count trigger mints one pending job.
async fn reef_with_corals(f: &Fixture) -> (Uuid, Vec<Uuid>) {
    let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Postgres') RETURNING node_id")
        .bind(f.graph)
        .fetch_one(f.owner())
        .await
        .unwrap();
    let mut atoms = Vec::new();
    for i in 0..8 {
        let mut v = axis(0);
        v[i + 1] = 1.0;
        let norm = (2.0f32).sqrt();
        let v: Vec<f32> = v.into_iter().map(|x| x / norm).collect();
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by,embedding_next) \
             VALUES($1,'note',$2,'fixture-only',$3,now(),$4,$5) RETURNING atom_id",
        )
        .bind(f.graph)
        .bind(format!("digest-{i}"))
        .bind(format!("Postgres fact {i}: the tenant database runs on Postgres."))
        .bind(f.payer)
        .bind(pgvector::Vector::from(v))
        .fetch_one(f.owner())
        .await
        .unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(id).execute(f.owner()).await.unwrap();
        atoms.push(id);
    }
    (node, atoms)
}

async fn digest_jobs(f: &Fixture, node: Uuid) -> Vec<(String, Option<String>)> {
    sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE kind='RegenerateDigest' AND payload->>'node_id'=$1::text ORDER BY created_at")
        .bind(node)
        .fetch_all(f.owner())
        .await
        .unwrap()
}

async fn digest_tick(f: &Fixture, pool: &PgPool, llm: &Script) -> usize {
    digest::tick(&worker(f, pool), &f.control.app, &TitleEmbedder, &crate::llm::testing::GatedScript(llm), None)
        .await
        .unwrap()
        .processed
}

async fn digest_of(f: &Fixture, node: Uuid) -> (Option<serde_json::Value>, i32) {
    sqlx::query_as("SELECT body->'digest', digest_coral_count FROM nodes WHERE node_id=$1").bind(node).fetch_one(f.owner()).await.unwrap()
}

async fn digest_publishes_with_receipts(org: bool, owner: bool) {
    let _env = Env::set(ON);
    let f = funded(org).await;
    let runtime = if owner { super::entity_resolution::owner_runtime(&f).await } else { f.app().clone() };
    let (node, corals) = reef_with_corals(&f).await;
    assert_eq!(digest_jobs(&f, node).await, vec![("pending".to_string(), None)], "the coral-count trigger mints one admitted pass");
    let llm = Script::new();
    assert_eq!(digest_tick(&f, &runtime, &llm).await, 1);
    assert_eq!(llm.calls(), 1, "Jev off: one writer call");
    let (digest, counted) = digest_of(&f, node).await;
    let digest = digest.expect("digest published");
    assert_eq!(digest["summary"], "Postgres runs the tenant databases.");
    assert_eq!(counted, 8);
    let receipts = receipts(&f).await;
    assert_eq!(receipts.len(), 1);
    assert_eq!((receipts[0].1.as_str(), receipts[0].2.as_str()), ("managed_enrichment", "billed"));
    assert_eq!(digest["managed_receipt"], serde_json::json!(receipts[0].0), "the digest names its receipt");
    let outcomes = outcomes(&f).await;
    assert_eq!(outcomes.iter().filter(|(k, _)| k == "atom").count(), 8);
    assert!(outcomes.iter().any(|(k, _)| k == "digest_node"));
    assert!(outcomes.iter().all(|(_, o)| o == "written"));
    assert_eq!(digest_jobs(&f, node).await, vec![("done".to_string(), None)]);
    assert_billed_once(&f, f.app()).await;

    // Deleting a coral the published digest read: the purge lifecycle strips
    // the digest, and the inputs table queues one regeneration.
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1").bind(corals[0]).execute(f.owner()).await.unwrap();
    let (digest, counted) = digest_of(&f, node).await;
    assert!(digest.is_none(), "a digest built from a deleted coral is stripped");
    assert_eq!(counted, 0);
    assert_eq!(digest_jobs(&f, node).await.last().unwrap().0, "pending", "one regeneration queued");
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn digest_publishes_with_receipts_consumer() {
    digest_publishes_with_receipts(false, false).await;
}
#[tokio::test]
async fn digest_publishes_with_receipts_tenant_app_role() {
    digest_publishes_with_receipts(true, false).await;
}
#[tokio::test]
async fn digest_publishes_with_receipts_tenant_owner_runtime() {
    digest_publishes_with_receipts(true, true).await;
}

/// A coral edited during the call, and a coral deleted during the call
/// (deletion invalidation flips its in-flight input), both publish nothing.
#[tokio::test]
async fn digest_stale_and_in_flight_deletion_publish_nothing_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let (node, corals) = reef_with_corals(&f).await;
        let llm = Script::new();
        llm.during(f.owner(), format!("UPDATE atoms SET content_inline='Postgres, edited during the call.' WHERE atom_id='{}'", corals[1])).await;
        digest_tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 1);
        assert!(digest_of(&f, node).await.0.is_none(), "nothing published from an edited coral");
        assert!(outcomes(&f).await.iter().all(|(_, o)| o == "discarded_stale"));

        // Next pass: a coral is deleted mid-call.
        queue::enqueue(f.app(), &JobKind::RegenerateDigest { node_id: node, graph_id: f.graph }, Some(f.account), Some(&format!("digest:{node}")), 0)
            .await
            .unwrap();
        llm.during(f.owner(), format!("DELETE FROM atoms WHERE atom_id='{}'", corals[2])).await;
        digest_tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 2);
        assert!(digest_of(&f, node).await.0.is_none(), "nothing published after an in-flight deletion");
        let second: Vec<(String, String)> = sqlx::query_as(
            "SELECT i.input_kind,i.outcome FROM managed_enrichment_inputs i JOIN managed_attempt_receipts r ON r.receipt_id=i.receipt_id \
              WHERE i.graph_id=$1 ORDER BY r.created_at DESC, i.ordinal LIMIT 9",
        )
        .bind(f.graph)
        .fetch_all(f.owner())
        .await
        .unwrap();
        assert_eq!(second.iter().filter(|(_, o)| o == "invalidated").count(), 1, "{second:?}");
        assert!(second.iter().all(|(_, o)| o == "invalidated" || o == "discarded_stale"));
        f.finish().await;
    }
}

/// The daily bound and the cap reschedule the reef for the next UTC day
/// without dead-lettering; the switch (off by default) claims nothing; Jev's
/// picker is metered as its own managed call.
#[tokio::test]
async fn digest_bound_cap_switch_and_jev_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let (node, _) = reef_with_corals(&f).await;
        let llm = Script::new();
        {
            let _off = Env::set(&[("DIGESTS_ENABLED", None)]);
            assert_eq!(digest_tick(&f, f.app(), &llm).await, 0, "digests are off unless DIGESTS_ENABLED=true");
            assert_eq!(digest_jobs(&f, node).await, vec![("pending".to_string(), None)]);
        }
        for (var, value) in [("DIGEST_DAILY_CALLS_PER_GRAPH", "2"), ("MANAGED_DAILY_CAP_USD_ENRICHMENT", "0.000001")] {
            let _held = Env::set(&[(var, Some(value))]);
            digest_tick(&f, f.app(), &llm).await;
            let jobs = digest_jobs(&f, node).await;
            assert!(jobs.iter().all(|(s, _)| s != "dead"), "{var}: never dead-lettered: {jobs:?}");
            let next: bool = sqlx::query_scalar(
                "SELECT run_after > now() + interval '30 seconds' FROM jobs WHERE kind='RegenerateDigest' AND status='pending' AND payload->>'node_id'=$1::text",
            )
            .bind(node)
            .fetch_one(f.owner())
            .await
            .unwrap();
            assert!(next, "{var}: the reef comes back after the UTC day turns");
            sqlx::query("UPDATE jobs SET run_after=now() WHERE kind='RegenerateDigest' AND status='pending'").execute(f.owner()).await.unwrap();
        }
        assert_eq!((llm.calls(), receipts(&f).await.len()), (0, 0), "bounded or capped: nothing sent, no receipt");
        {
            let _jev = Env::set(&[("JEV_JUDGE_MODE", Some("live"))]);
            digest_tick(&f, f.app(), &llm).await;
        }
        assert_eq!((llm.calls(), llm.jev_calls.load(Ordering::SeqCst)), (2, 1), "Jev's picker and the writer");
        let receipts = receipts(&f).await;
        assert_eq!(receipts.len(), 2);
        assert!(receipts.iter().all(|r| r.1 == "managed_enrichment" && r.2 == "billed"));
        // `digest_withheld` rows are the $0 records of the capped attempt above.
        let tasks: Vec<String> = sqlx::query_scalar("SELECT task FROM llm_usage WHERE graph_id=$1 AND task NOT LIKE '%withheld' ORDER BY task")
            .bind(f.graph).fetch_all(f.owner()).await.unwrap();
        assert_eq!(tasks, ["digest", "jev_judge"]);
        assert!(digest_of(&f, node).await.0.is_some());
        f.finish().await;
    }
}

// ── Digest coverage (COR-66): eligibility, the lane, the daily bound, debounce ──

/// An entity reef titled `title` with `n` corals, each embedded off the
/// title's axis (all selectable) when `embedded`, else with no embedding
/// (linked, counted, never a selection candidate).
async fn reef_with(f: &Fixture, title: &str, n: usize, embedded: bool) -> (Uuid, Vec<Uuid>) {
    let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity',$2) RETURNING node_id")
        .bind(f.graph)
        .bind(title)
        .fetch_one(f.owner())
        .await
        .unwrap();
    let atoms = add_corals(f, node, title, 0, n, embedded).await;
    (node, atoms)
}

async fn add_corals(f: &Fixture, node: Uuid, title: &str, from: usize, n: usize, embedded: bool) -> Vec<Uuid> {
    let mut atoms = Vec::new();
    for i in from..from + n {
        let embedding = embedded.then(|| {
            let mut v = axis(0);
            v[i + 1] = 1.0;
            pgvector::Vector::from(v.into_iter().map(|x| x / 2f32.sqrt()).collect::<Vec<f32>>())
        });
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by,embedding_next) \
             VALUES($1,'note',$2,'fixture-only',$3,now(),$4,$5) RETURNING atom_id",
        )
        .bind(f.graph)
        .bind(format!("{title}-{i}"))
        .bind(format!("{title} fact {i}."))
        .bind(f.payer)
        .bind(embedding)
        .fetch_one(f.owner())
        .await
        .unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(id).execute(f.owner()).await.unwrap();
        atoms.push(id);
    }
    atoms
}

async fn digested(f: &Fixture, node: Uuid) -> bool {
    digest_of(f, node).await.0.is_some_and(|d| d.get("managed_receipt").is_some())
}

/// Reefs of 3 or more corals are due, and the lane claims a reader's request
/// first, then the reefs with the most corals, whatever the queue order.
#[tokio::test]
async fn digest_lane_claims_requested_then_most_corals_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let (three, _) = reef_with(&f, "Three", 3, true).await;
        assert_eq!(digest_jobs(&f, three).await, vec![("pending".to_string(), None)], "a 3-coral reef is due");
        let (five, _) = reef_with(&f, "Five", 5, true).await;
        let (six, _) = reef_with(&f, "Six", 6, true).await;
        let (four, _) = reef_with(&f, "Four", 4, true).await;
        let (twelve, _) = reef_with(&f, "Twelve", 12, true).await;
        let (twenty, _) = reef_with(&f, "Twenty", 20, true).await;
        // Oldest-first would claim the four small reefs.
        sqlx::query("UPDATE jobs SET run_after = now() - CASE WHEN payload->>'node_id' = ANY($1) THEN interval '1 minute' ELSE interval '10 minutes' END WHERE kind='RegenerateDigest'")
            .bind(vec![twelve.to_string(), twenty.to_string()])
            .execute(f.owner())
            .await
            .unwrap();
        assert!(digest::queue_regeneration(Some(f.app()), four, f.graph, f.account).await, "a reader asks for Four");
        let llm = Script::new();
        assert_eq!(digest_tick(&f, f.app(), &llm).await, 4);
        for (node, name) in [(four, "the reader's reef"), (twenty, "20 corals"), (twelve, "12 corals"), (six, "6 corals")] {
            assert!(digested(&f, node).await, "{name} is claimed first");
        }
        for node in [three, five] {
            assert!(!digested(&f, node).await, "the smallest wait");
            assert_eq!(digest_jobs(&f, node).await.last().unwrap().0, "pending");
        }
        // The next tick drains the rest: everything due converges.
        digest_tick(&f, f.app(), &llm).await;
        assert!(digested(&f, three).await && digested(&f, five).await);
        f.finish().await;
    }
}

/// The digests' dollar bound and the shared cap both hold before any send,
/// never dead-letter, and defer every due pass of the graph to the next UTC
/// day in one hold rather than claiming each of them.
#[tokio::test]
async fn digest_daily_bound_defers_the_graph_in_one_hold_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        for (var, value) in [
            ("DIGEST_DAILY_USD_PER_ACCOUNT", "0.01"),
            ("DIGEST_DAILY_SHARE_OF_ENRICHMENT", "0.001"),
            ("MANAGED_DAILY_CAP_USD_ENRICHMENT", "0.000001"),
        ] {
            let f = funded(org).await;
            let mut nodes = Vec::new();
            for i in 0..6 {
                nodes.push(reef_with(&f, &format!("Held {i}"), 3, true).await.0);
            }
            let llm = Script::new();
            {
                let _held = Env::set(&[(var, Some(value))]);
                digest_tick(&f, f.app(), &llm).await;
            }
            assert_eq!((llm.calls(), receipts(&f).await.len()), (0, 0), "{var}: nothing sent, no receipt");
            let (dead, due, later): (i64, i64, i64) = sqlx::query_as(
                "SELECT count(*) FILTER (WHERE status='dead'), \
                        count(*) FILTER (WHERE status='pending' AND run_after <= now() + interval '30 seconds'), \
                        count(*) FILTER (WHERE status='pending' AND run_after > now() + interval '30 seconds') \
                   FROM jobs WHERE kind='RegenerateDigest'",
            )
            .fetch_one(f.owner())
            .await
            .unwrap();
            assert_eq!((dead, due, later), (0, 0, 6), "{var}: every reef of the graph comes back after the UTC day turns");
            for node in nodes {
                assert!(!digested(&f, node).await);
            }
            f.finish().await;
        }
    }
}

/// A pass whose inputs are the stored digest's spends nothing: growth that
/// adds no selectable coral restamps the counter only. An edited coral, or a
/// digest from before managed enrichment, is regenerated.
#[tokio::test]
async fn digest_unchanged_inputs_are_not_regenerated_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let (node, corals) = reef_with(&f, "Postgres", 8, true).await;
        let llm = Script::new();
        digest_tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 1);
        let (first, counted) = digest_of(&f, node).await;
        let first = first.expect("digest published");
        assert_eq!(counted, 8);
        assert!(first["selection_key"].is_string(), "the digest records what it was written from");

        // Five more corals, not embedded yet: the growth rule fires, the
        // selection does not change.
        add_corals(&f, node, "Postgres", 8, 5, false).await;
        assert_eq!(digest_jobs(&f, node).await.last().unwrap().0, "pending", "growth queues a pass");
        digest_tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 1, "unchanged inputs: no model call");
        let (same, counted) = digest_of(&f, node).await;
        assert_eq!(same.unwrap()["managed_receipt"], first["managed_receipt"], "the stored digest is kept");
        assert_eq!(counted, 13, "the counter is restamped so growth measures from now");
        assert_eq!(digest_jobs(&f, node).await.last().unwrap().0, "done");

        // An edited coral changes the inputs.
        sqlx::query("UPDATE atoms SET content_inline='Postgres fact 0, edited.' WHERE atom_id=$1").bind(corals[0]).execute(f.owner()).await.unwrap();
        assert!(digest::queue_regeneration(Some(f.app()), node, f.graph, f.account).await);
        digest_tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 2, "an edited coral is regenerated");
        assert!(digested(&f, node).await);

        // A digest from before managed enrichment: the counters call it
        // fresh, the reader never serves it.
        sqlx::query("UPDATE nodes SET body = jsonb_set(body, '{digest}', '{\"summary\":\"legacy\",\"digested_title\":\"Postgres\"}'::jsonb), digest_coral_count = coral_count WHERE node_id=$1")
            .bind(node).execute(f.owner()).await.unwrap();
        assert!(digest::queue_regeneration(Some(f.app()), node, f.graph, f.account).await);
        digest_tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), 3, "an unattributed digest is regenerated");
        assert!(digested(&f, node).await);
        f.finish().await;
    }
}

async fn last_digest_job(f: &Fixture, node: Uuid) -> String {
    digest_jobs(f, node).await.last().expect("a digest job").0.clone()
}

/// After a reader's request, reefs with no digest yet are claimed before
/// regenerations of reefs that have one, whatever their coral counts and
/// queue age; within each tier the most corals go first. 2026-10-27: the
/// same ~240 hub reefs were re-digested daily while 64k reefs never got one.
#[tokio::test]
async fn digest_lane_claims_first_digests_before_regenerations_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let mut hubs = Vec::new();
        for (title, n) in [("Hub A", 20), ("Hub B", 12), ("Hub C", 10)] {
            hubs.push((reef_with(&f, title, n, true).await.0, title, n));
        }
        let llm = Script::new();
        digest_tick(&f, f.app(), &llm).await;
        for (hub, title, _) in &hubs {
            assert!(digested(&f, *hub).await, "{title} has its first digest");
        }
        // Every hub grows past the regeneration bar.
        for (hub, title, n) in &hubs {
            add_corals(&f, *hub, title, *n, 5, true).await;
            assert_eq!(last_digest_job(&f, *hub).await, "pending", "{title}: growth queues a regeneration");
        }
        // Reefs with no digest yet, every one smaller than any hub.
        let mut fresh = Vec::new();
        for n in 3..=6 {
            fresh.push((reef_with(&f, &format!("New {n}"), n, true).await.0, n));
        }
        // The regenerations are the oldest in the queue.
        let hub_ids: Vec<String> = hubs.iter().map(|(id, ..)| id.to_string()).collect();
        sqlx::query("UPDATE jobs SET run_after = now() - CASE WHEN payload->>'node_id' = ANY($1) THEN interval '10 minutes' ELSE interval '1 minute' END WHERE kind='RegenerateDigest' AND status='pending'")
            .bind(&hub_ids)
            .execute(f.owner())
            .await
            .unwrap();
        let (hub_a, hub_b, hub_c) = (hubs[0].0, hubs[1].0, hubs[2].0);
        assert!(digest::queue_regeneration(Some(f.app()), hub_c, f.graph, f.account).await, "a reader asks for Hub C");

        assert_eq!(digest_tick(&f, f.app(), &llm).await, 4);
        assert_eq!(last_digest_job(&f, hub_c).await, "done", "the reader's regeneration goes first");
        for (node, n) in &fresh[1..] {
            assert_eq!(last_digest_job(&f, *node).await, "done", "New {n}: a first digest before any regeneration");
            assert!(digested(&f, *node).await);
        }
        assert_eq!(last_digest_job(&f, fresh[0].0).await, "pending", "New 3: fewest corals of the first-digest tier, next");
        for (node, name) in [(hub_a, "Hub A"), (hub_b, "Hub B")] {
            assert_eq!(last_digest_job(&f, node).await, "pending", "{name}: a regeneration waits behind first digests");
        }

        // The next tick drains the rest: everything due converges.
        digest_tick(&f, f.app(), &llm).await;
        for node in [fresh[0].0, hub_a, hub_b] {
            assert_eq!(last_digest_job(&f, node).await, "done");
        }
        assert!(digested(&f, fresh[0].0).await);
        f.finish().await;
    }
}

// ── Enrichment shares: the journal, titles and supersession keep headroom ────

/// An entity reef for entity resolution. Same-title copies differ only by
/// `identity_scope`.
async fn er_reef(f: &Fixture, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes(graph_id,type,title,entity_type,identity_scope) VALUES($1,'entity',$2,'org',gen_random_uuid()) RETURNING node_id",
    )
    .bind(f.graph)
    .bind(title)
    .fetch_one(f.owner())
    .await
    .unwrap()
}

/// Micro-USD charged (else reserved) today on the account's receipts that
/// bound a `marker` input: one consumer's spend.
async fn spend_by(f: &Fixture, marker: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(sum(COALESCE(r.settled_micro_usd,r.reserved_micro_usd)),0)::bigint FROM managed_attempt_receipts r \
          WHERE r.account_id=$1 AND r.purpose='managed_enrichment' \
            AND EXISTS(SELECT 1 FROM managed_enrichment_inputs i WHERE i.receipt_id=r.receipt_id AND i.input_kind=$2)",
    )
    .bind(f.account)
    .bind(marker)
    .fetch_one(f.owner())
    .await
    .unwrap()
}

/// Pending jobs of `kind`: (dead or failed, due within 30 s, held for later).
async fn job_spread(f: &Fixture, kind: &str) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT count(*) FILTER (WHERE status::text IN ('dead','failed')), \
                count(*) FILTER (WHERE status='pending' AND run_after <= now() + interval '30 seconds'), \
                count(*) FILTER (WHERE status='pending' AND run_after > now() + interval '30 seconds') \
           FROM jobs WHERE kind=$1",
    )
    .bind(kind)
    .fetch_one(f.owner())
    .await
    .unwrap()
}

/// Production's shape on 2026-10-27: digests and the entity-resolution
/// judge (Jev live, `jev_judge`) together want more than the whole daily
/// enrichment cap. Each stops at its share of the cap and waits for the
/// next UTC day without failing, and a coral-title batch the same day is
/// still sent rather than withheld.
#[tokio::test]
async fn enrichment_reserve_survives_digest_and_er_demand_both_planes() {
    let _env = Env::set(ON);
    // $0.10 a day, every call settles at $0.01. Demand: 8 digests at two
    // calls each (Jev's picker and the writer) and 6 judged pairs, $0.22.
    let _day = Env::set(&[("MANAGED_DAILY_CAP_USD_ENRICHMENT", Some("0.10")), ("JEV_JUDGE_MODE", Some("live"))]);
    const CALL: i64 = 10_000;
    const DIGEST_SHARE: i64 = 50_000;
    const ER_SHARE: i64 = 30_000;
    for org in [false, true] {
        let f = funded(org).await;
        let mut reefs = Vec::new();
        for i in 0..8 {
            reefs.push(reef_with(&f, &format!("Hub {i}"), 3, true).await.0);
        }
        for name in ["Composio", "Linear", "Fly.io", "GitHub", "Neon", "Clerk"] {
            er_reef(&f, name).await;
            er_reef(&f, &name.to_lowercase()).await;
        }
        let llm = Script::costing(0.01);
        for _ in 0..4 {
            digest_tick(&f, f.app(), &llm).await;
        }
        queue::enqueue(f.app(), &JobKind::EntityResolution { graph_id: f.graph }, Some(f.account), Some(&format!("er:{}", f.graph)), 0)
            .await
            .unwrap();
        for _ in 0..4 {
            crate::workers::entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&llm))
                .await
                .unwrap();
            // A pass that continues comes back in seconds; run it now.
            sqlx::query("UPDATE jobs SET run_after=now() WHERE kind='EntityResolution' AND status='pending' AND run_after < now() + interval '1 minute'")
                .execute(f.owner())
                .await
                .unwrap();
        }

        let (digests, judge) = (spend_by(&f, "digest_node").await, spend_by(&f, "er_facts").await);
        assert!(digests > 0 && digests <= DIGEST_SHARE, "digests stay inside their half of the cap: {digests}");
        assert!(judge >= ER_SHARE && judge <= ER_SHARE + CALL,
            "the judge spends its share and at most the one call that crossed it: {judge}");
        assert!(reefs.len() > (digests / (2 * CALL)) as usize, "digest demand was larger than the share");
        let jev: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE graph_id=$1 AND task='jev_judge'")
            .bind(f.graph).fetch_one(f.owner()).await.unwrap();
        assert!(jev > 0, "the judge's calls are Jev's");
        let (failed, due, later) = job_spread(&f, "RegenerateDigest").await;
        assert_eq!((failed, due), (0, 0), "no digest failed or is still due today");
        assert!(later > 0, "the undigested reefs come back after the UTC day turns");
        let (failed, due, later) = job_spread(&f, "EntityResolution").await;
        assert_eq!((failed, due, later), (0, 0, 1), "the graph's judging waits for the next UTC day, not failed");

        // The rest of the cap is still there for coral titles.
        let atom = fallback_atom(&f, "reserve-1", "Gave digests and the judge a share of the enrichment cap.", "fallback").await;
        let job = enqueue_titles(&f, f.app()).await;
        let sent = llm.calls();
        titles_tick(&f, f.app(), &llm).await;
        assert_eq!(llm.calls(), sent + 1, "the title batch is sent, not withheld");
        assert_eq!(job_state(&f, job).await, "done");
        assert_eq!(title_of(&f, atom).await.1.as_deref(), Some("llm"));
        let withheld: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE graph_id=$1 AND task LIKE 'atom_title%withheld'")
            .bind(f.graph).fetch_one(f.owner()).await.unwrap();
        assert_eq!(withheld, 0);
        f.finish().await;
    }
}

/// The attributed reader-side digest read over digests this worker published.
#[path = "digest_read.rs"]
mod digest_read;
