//! EntityResolution on `managed_enrichment` (source-agnostic plan PR 5): the
//! real worker (`workers::entity_resolution::tick`) against funded two-plane
//! fixtures. Judged duplicates merge with receipts; a "different" or unsure
//! judgement keeps reefs apart; stale inputs publish nothing; the shared cap
//! holds without dead-lettering; the switch is off by default; receipts bill
//! once; the identifier auto-merge and POSITIVE materialisation stay free.
//! The org plane also runs as a Neon-shaped table-owner runtime.
use super::*;
use crate::jobs::{queue, JobKind};
use crate::llm::{CompletionResponse, LlmError, LlmProvider, StreamEvent};
use crate::store::{PostgresStore, Store};
use crate::workers::entity_resolution;
use crate::workers::tenant_harness::WorkerTenant;
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

const ON: &[(&str, Option<&str>)] = &[
    ("ENRICHMENT_PROCESSING_ENABLED", Some("true")),
    ("MANAGED_PROCESSING_DISABLED", None),
    ("MANAGED_DAILY_CAP_USD_ENRICHMENT", None),
    ("JUDGEMENT_GRAPH_WRITES", Some("on")),
    ("JEV_JUDGE_MODE", None),
    ("JEV_INGEST_DOOR_MODE", None),
    ("JEV_PROVIDER", None),
    ("ER_BLOCKING", None),
    ("ER_DAILY_CALLS_PER_GRAPH", None),
    ("ER_PASS_BUDGET_SECS", None),
    ("RESOLVE_ESCALATE_MODEL", None),
];

/// Scripted provider: answers the forced-tool verdict (or Jev's `jev_answers`)
/// by the first rule whose needle appears in the prompt, and can run one SQL
/// statement mid-call (an edit racing the judgement).
struct Judge {
    rules: Vec<(&'static str, &'static str)>,
    jev: &'static str,
    calls: AtomicUsize,
    jev_calls: AtomicUsize,
    during: tokio::sync::Mutex<Option<(PgPool, String)>>,
}

impl Judge {
    fn new(rules: &[(&'static str, &'static str)]) -> Self {
        Self {
            rules: rules.to_vec(),
            jev: r#"{"q0":{"choice":"same","confidence":0.96}}"#,
            calls: AtomicUsize::new(0),
            jev_calls: AtomicUsize::new(0),
            during: tokio::sync::Mutex::new(None),
        }
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl LlmProvider for Judge {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((pool, sql)) = self.during.lock().await.take() {
            sqlx::query(&sql).execute(&pool).await.expect("mid-call edit");
        }
        let jev = req.tool_schema.as_ref().is_some_and(|t| t.name == crate::decision::openrouter::TOOL_NAME);
        let content = if jev {
            self.jev_calls.fetch_add(1, Ordering::SeqCst);
            self.jev.to_string()
        } else {
            let prompt = &req.messages[0].content;
            self.rules
                .iter()
                .find(|(needle, _)| prompt.contains(needle))
                .map(|(_, body)| body.to_string())
                .unwrap_or_else(|| r#"{"verdict":"unsure","confidence":0.1,"reason":"no rule"}"#.into())
        };
        Ok(CompletionResponse {
            content,
            input_tokens: 900,
            output_tokens: 20,
            model: "deepseek/deepseek-v4-flash-0731".into(),
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
        unimplemented!("entity resolution never streams")
    }
}

const SAME: &str = r#"{"verdict":"same","confidence":0.97,"reason":"same database"}"#;
const DIFFERENT: &str = r#"{"verdict":"different","confidence":0.92,"reason":"a branch and a chat tool"}"#;
const UNSURE: &str = r#"{"verdict":"unsure","confidence":0.6,"reason":"too little evidence"}"#;

async fn funded(org: bool) -> Fixture {
    let f = Fixture::new(org).await;
    f.balance(Decimal::from(100)).await;
    f
}

fn worker(f: &Fixture, pool: &PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: f.tenant.clone(),
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())),
    }
}

/// An entity reef. Per-version duplicates share a title and differ only by
/// `identity_scope` (the unique key is graph, type, lower(title), scope).
async fn reef(f: &Fixture, title: &str, entity_type: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes(graph_id,type,title,entity_type,identity_scope) VALUES($1,'entity',$2,$3,gen_random_uuid()) RETURNING node_id",
    )
    .bind(f.graph)
    .bind(title)
    .bind(entity_type)
    .fetch_one(f.owner())
    .await
    .unwrap()
}

/// A statement from the fixture's retained source, linked to `node`.
async fn statement(f: &Fixture, node: Uuid, ordinal: i32, text: &str) -> Uuid {
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) \
         VALUES($1,$2,$3,'fact',$4,'source-atoms-v2','m') RETURNING semantic_atom_id",
    )
    .bind(f.graph)
    .bind(f.version)
    .bind(ordinal)
    .bind(text)
    .fetch_one(f.owner())
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)")
        .bind(node)
        .bind(id)
        .bind(f.graph)
        .execute(f.owner())
        .await
        .unwrap();
    id
}

async fn enqueue(f: &Fixture, pool: &PgPool) -> Uuid {
    queue::enqueue(pool, &JobKind::EntityResolution { graph_id: f.graph }, Some(f.account), Some(&format!("er:{}", f.graph)), 0)
        .await
        .unwrap()
        .expect("a fresh pass")
}

async fn exists(f: &Fixture, node: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)").bind(node).fetch_one(f.owner()).await.unwrap()
}

async fn scalar_i64(f: &Fixture, sql: &str) -> i64 {
    sqlx::query_scalar(sql).bind(f.graph).fetch_one(f.owner()).await.unwrap()
}

async fn job_state(f: &Fixture, job: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE id=$1").bind(job).fetch_one(f.owner()).await.unwrap()
}

/// The judgement row for a pair, live or not.
async fn judgement(f: &Fixture, a: Uuid, b: Uuid) -> Option<(Uuid, String, String, serde_json::Value, Option<chrono::DateTime<chrono::Utc>>)> {
    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
    sqlx::query_as(
        "SELECT judgement_id,verdict,source,evidence,annulled_at FROM reef_judgement WHERE graph_id=$1 AND node_a_id=$2 AND node_b_id=$3 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(f.graph)
    .bind(lo)
    .bind(hi)
    .fetch_optional(f.owner())
    .await
    .unwrap()
}

/// A Neon-shaped production tenant runtime: a non-superuser BYPASSRLS LOGIN
/// that owns every public table and routine and inherits
/// coralswarm_tenant_app (tenant 0159), with no SET path to it.
pub(super) async fn owner_runtime(f: &Fixture) -> PgPool {
    let content = f.content.as_ref().expect("org fixture");
    let (role, password) = (content.db.role("cor22_er_owner"), Uuid::new_v4().simple().to_string());
    sqlx::query(&format!("CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEROLE BYPASSRLS NOREPLICATION PASSWORD '{password}'"))
        .execute(&f.admin).await.unwrap();
    sqlx::query(&format!("GRANT coralswarm_tenant_app TO {role} WITH INHERIT TRUE, SET FALSE")).execute(&f.admin).await.unwrap();
    sqlx::raw_sql(&format!(r#"DO $$ DECLARE r record; BEGIN
      FOR r IN SELECT c.oid::regclass AS rel, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m') AND c.relowner='{su}'::regrole
          AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.objid=c.oid AND d.deptype='e') LOOP
        EXECUTE format('ALTER %s %s OWNER TO {role}', CASE r.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END, r.rel);
      END LOOP;
      FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind='S' AND c.relowner='{su}'::regrole
          AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.objid=c.oid AND d.deptype IN ('a','i','e')) LOOP
        EXECUTE format('ALTER SEQUENCE %s OWNER TO {role}', r.rel);
      END LOOP;
      FOR r IN SELECT p.oid::regprocedure AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='public' AND p.proowner='{su}'::regrole AND p.prokind IN ('f','p')
          AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.objid=p.oid AND d.deptype='e') LOOP
        EXECUTE format('ALTER ROUTINE %s OWNER TO {role}', r.fn);
      END LOOP;
    END $$;"#, su = current_user(f.owner()).await)).execute(&content.owner).await.unwrap();
    let url = std::env::var("ORGT_TEST_BASE_URL").unwrap();
    let options = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.database(&content.db).username(&role).password(&password))
        .await
        .unwrap();
    let shape: (bool, bool, bool, bool, bool) = sqlx::query_as(
        "SELECT r.rolsuper,r.rolbypassrls,pg_get_userbyid(c.relowner)=current_user, \
                pg_has_role(current_user,'coralswarm_tenant_app','USAGE'),pg_has_role(current_user,'coralswarm_tenant_app','SET') \
           FROM pg_roles r,pg_class c WHERE r.rolname=current_user AND c.oid='public.nodes'::regclass",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(shape, (false, true, true, true, false), "owner runtime: non-superuser BYPASSRLS table owner inheriting tenant_app, no SET");
    pool
}

async fn current_user(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT current_user::text").fetch_one(pool).await.unwrap()
}

/// Judged duplicates merge through the human-merge machinery, with receipts
/// under `managed_enrichment`, undoably, keeping links, edges and aliases.
async fn judged_duplicates_merge(org: bool, owner: bool) {
    let _env = Env::set(ON);
    let f = funded(org).await;
    let runtime = if owner { owner_runtime(&f).await } else { f.app().clone() };
    let keep = reef(&f, "Postgres", "topic").await;
    let dup = reef(&f, "postgres", "topic").await;
    let neon = reef(&f, "Neon", "org").await;
    statement(&f, keep, 0, "Postgres 16 runs the tenant databases.").await;
    let moved_fact = statement(&f, dup, 1, "The postgres pooler serves an empty search_path.").await;
    sqlx::query("INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,status) VALUES($1,$2,'hosted_by',$3,'confirmed')")
        .bind(f.graph).bind(dup).bind(neon).execute(f.owner()).await.unwrap();
    let job = enqueue(&f, &runtime).await;
    let judge = Judge::new(&[("Postgres", SAME)]);
    let tick = entity_resolution::tick(&worker(&f, &runtime), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
    assert_eq!(tick.processed, 1);
    assert_eq!(judge.calls(), 1, "one judged pair, one paid call");
    assert!(exists(&f, keep).await && !exists(&f, dup).await, "the copy merged into the older keeper");
    assert_eq!(job_state(&f, job).await, ("done".into(), None));

    let (judgement_id, verdict, source, evidence, _) = judgement(&f, keep, dup).await.expect("judgement written");
    assert_eq!((verdict.as_str(), source.as_str()), ("positive", "llm"));
    let receipts: Vec<(Uuid, String, Option<Uuid>, String)> = sqlx::query_as(
        "SELECT receipt_id,purpose,job_id,state FROM managed_attempt_receipts WHERE account_id=$1 ORDER BY created_at",
    ).bind(f.account).fetch_all(f.owner()).await.unwrap();
    assert_eq!(receipts.len(), 1, "{receipts:?}");
    let (receipt, purpose, receipt_job, state) = receipts[0].clone();
    assert_eq!((purpose.as_str(), receipt_job, state.as_str()), ("managed_enrichment", Some(job), "billed"));
    assert_eq!(evidence["managed_receipt"], serde_json::json!(receipt), "the judgement names the receipt it was published from");
    let inputs: Vec<(String, Uuid, String)> = sqlx::query_as(
        "SELECT input_kind,input_id,outcome FROM managed_enrichment_inputs WHERE receipt_id=$1 ORDER BY ordinal",
    ).bind(receipt).fetch_all(f.owner()).await.unwrap();
    let (lo, hi) = if keep < dup { (keep, dup) } else { (dup, keep) };
    assert_eq!(inputs.iter().map(|i| (i.0.as_str(), i.2.as_str())).collect::<Vec<_>>(),
        [("node", "written"), ("node", "written"), ("er_facts", "written")], "both reefs and the facts hash, published");
    assert_eq!((inputs[0].1, inputs[1].1), (lo, hi));

    let (merge_id, merge_judgement): (Uuid, Option<Uuid>) = sqlx::query_as(
        "SELECT merge_id,judgement_id FROM node_merges WHERE keep_id=$1 AND dup_id=$2",
    ).bind(keep).bind(dup).fetch_one(f.owner()).await.unwrap();
    assert_eq!(merge_judgement, Some(judgement_id), "the undo row links the judgement");
    let linked: Uuid = sqlx::query_scalar("SELECT node_id FROM node_semantic_atoms WHERE semantic_atom_id=$1")
        .bind(moved_fact).fetch_one(f.owner()).await.unwrap();
    assert_eq!(linked, keep, "the copy's statement moved");
    let edge_src: Uuid = sqlx::query_scalar("SELECT src_node_id FROM relationship_edge WHERE graph_id=$1 AND dst_node_id=$2")
        .bind(f.graph).bind(neon).fetch_one(f.owner()).await.unwrap();
    assert_eq!(edge_src, keep, "the copy's edge was re-pointed");

    // No double billing: the receipt is debited once across settle and two
    // derivation passes, and the job axis never bills EntityResolution.
    for _ in 0..2 {
        crate::workers::billing_derivation::tick(&worker(&f, f.app()), &f.control.app).await.unwrap();
    }
    let debits: Vec<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT entry_kind,source_receipt_id FROM credit_ledger WHERE account_id=$1 AND entry_kind LIKE 'usage_%' ORDER BY entry_kind",
    ).bind(f.account).fetch_all(&f.control.owner).await.unwrap();
    assert_eq!(debits, vec![("usage_managed".to_string(), Some(receipt))], "one managed debit, no job-axis debit");

    // Undo restores the copy and annuls the judgement.
    let store = PostgresStore::new(f.app().clone());
    let mut tx = match f.tenant {
        TenantId::Consumer => begin_account_scoped(f.app(), f.account).await.unwrap(),
        TenantId::Org(_) => crate::tenancy::scope::begin_tenant_system_scoped_without_account(f.app()).await.unwrap(),
    };
    store.unmerge_nodes_scoped(&mut tx, merge_id).await.unwrap();
    tx.commit().await.unwrap();
    assert!(exists(&f, dup).await, "undo restores the merged copy");
    let relinked: Uuid = sqlx::query_scalar("SELECT node_id FROM node_semantic_atoms WHERE semantic_atom_id=$1")
        .bind(moved_fact).fetch_one(f.owner()).await.unwrap();
    assert_eq!(relinked, dup, "undo moves the statement back");
    let (_, _, _, _, annulled) = judgement(&f, keep, dup).await.unwrap();
    assert!(annulled.is_some(), "undo annuls the merge's judgement");
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn er_judged_duplicates_merge_with_receipts_consumer() {
    judged_duplicates_merge(false, false).await;
}
#[tokio::test]
async fn er_judged_duplicates_merge_with_receipts_tenant_app_role() {
    judged_duplicates_merge(true, false).await;
}
#[tokio::test]
async fn er_judged_duplicates_merge_with_receipts_tenant_owner_runtime() {
    judged_duplicates_merge(true, true).await;
}

/// Extraction mints its per-version copies `identity_status: unresolved`.
/// The same-title sweep judges and merges such copies when both carry the
/// same known type, and never judges a same-title pair whose types differ.
async fn unresolved_same_title_copies_are_judged(org: bool, owner: bool) {
    let _env = Env::set(ON);
    let f = funded(org).await;
    let runtime = if owner { owner_runtime(&f).await } else { f.app().clone() };
    let unresolved = |title: &'static str, entity_type: &'static str, age_days: i32| {
        let f = &f;
        async move {
            sqlx::query_scalar::<_, Uuid>(
                "INSERT INTO nodes(graph_id,type,title,entity_type,body,identity_scope,created_at) \
                 VALUES($1,'entity',$2,$3,jsonb_build_object('entity_type',$3::text,'identity_status','unresolved'),gen_random_uuid(),now()-make_interval(days=>$4)) RETURNING node_id",
            )
            .bind(f.graph).bind(title).bind(entity_type).bind(age_days).fetch_one(f.owner()).await.unwrap()
        }
    };
    let keep = unresolved("example-founder", "person", 3).await;
    let dup = unresolved("example-founder", "person", 1).await;
    let slack_org = unresolved("Slack", "org", 3).await;
    let slack_topic = unresolved("slack", "topic", 1).await;
    statement(&f, keep, 0, "example-founder owns the extraction pipeline.").await;
    statement(&f, dup, 1, "example-founder reviewed the binding fix.").await;
    let job = enqueue(&f, &runtime).await;
    let judge = Judge::new(&[("example-founder", SAME), ("lack", SAME)]);
    entity_resolution::tick(&worker(&f, &runtime), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
    assert_eq!(job_state(&f, job).await, ("done".into(), None));
    assert_eq!(judge.calls(), 1, "only the same-type pair is judged");
    assert!(exists(&f, keep).await && !exists(&f, dup).await, "the unresolved copy merged into the older keeper");
    let (_, verdict, source, _, _) = judgement(&f, keep, dup).await.expect("judgement written");
    assert_eq!((verdict.as_str(), source.as_str()), ("positive", "llm"));
    assert!(exists(&f, slack_org).await && exists(&f, slack_topic).await, "an org and a topic that share a title are never merged");
    assert!(judgement(&f, slack_org, slack_topic).await.is_none(), "and never judged");
    let outcome: Option<String> = sqlx::query_scalar("SELECT outcome::text FROM jobs WHERE id=$1").bind(job).fetch_one(f.owner()).await.unwrap();
    assert_eq!(outcome.as_deref(), Some("did_work"));
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn er_unresolved_same_title_copies_are_judged_consumer() {
    unresolved_same_title_copies_are_judged(false, false).await;
}
#[tokio::test]
async fn er_unresolved_same_title_copies_are_judged_tenant_app_role() {
    unresolved_same_title_copies_are_judged(true, false).await;
}
#[tokio::test]
async fn er_unresolved_same_title_copies_are_judged_tenant_owner_runtime() {
    unresolved_same_title_copies_are_judged(true, true).await;
}

/// "different" and unsure keep reefs apart and are recorded, so the next
/// pass does not pay for them again.
#[tokio::test]
async fn er_different_and_unsure_keep_reefs_apart_and_are_not_rejudged_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let slack_a = reef(&f, "slack", "topic").await;
        let slack_b = reef(&f, "Slack", "topic").await;
        let main_a = reef(&f, "main", "topic").await;
        let main_b = reef(&f, "MAIN", "topic").await;
        let judge = Judge::new(&[("slack", DIFFERENT), ("Slack", DIFFERENT), ("main", UNSURE), ("MAIN", UNSURE)]);
        enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 2);
        for node in [slack_a, slack_b, main_a, main_b] {
            assert!(exists(&f, node).await, "nothing merged");
        }
        assert_eq!(scalar_i64(&f, "SELECT count(*) FROM node_merges WHERE graph_id=$1").await, 0);
        let (_, verdict, source, _, _) = judgement(&f, slack_a, slack_b).await.unwrap();
        assert_eq!((verdict.as_str(), source.as_str()), ("negative", "llm"));
        let (_, verdict, _, evidence, _) = judgement(&f, main_a, main_b).await.unwrap();
        assert_eq!(verdict, "negative", "a low-impact unsure pair is not queued for review");
        assert_eq!(evidence["recorded_as"], "unsure");
        assert_eq!(evidence["llm_verdict"], "unsure");
        let written: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_enrichment_inputs WHERE graph_id=$1 AND outcome='written'")
            .bind(f.graph).fetch_one(f.owner()).await.unwrap();
        assert_eq!(written, 6, "both calls published (recorded) their decision");
        // A second pass finds both pairs settled and spends nothing.
        enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 2, "settled pairs are not re-judged");
        f.finish().await;
    }
}

/// An input that changes while the judge is answering publishes nothing:
/// the call is billed (it was sent) but no merge or judgement is written.
#[tokio::test]
async fn er_stale_inputs_publish_nothing_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let keep = reef(&f, "Composio", "org").await;
        let dup = reef(&f, "composio", "org").await;
        let judge = Judge::new(&[("omposio", SAME)]);
        *judge.during.lock().await = Some((f.owner().clone(), format!("UPDATE nodes SET title='Composio Inc' WHERE node_id='{keep}'")));
        let job = enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 1);
        assert!(exists(&f, keep).await && exists(&f, dup).await, "no merge from a stale judgement");
        assert!(judgement(&f, keep, dup).await.is_none(), "no judgement from a stale judgement");
        let outcomes: Vec<String> = sqlx::query_scalar("SELECT outcome FROM managed_enrichment_inputs WHERE graph_id=$1 ORDER BY ordinal")
            .bind(f.graph).fetch_all(f.owner()).await.unwrap();
        assert_eq!(outcomes, ["discarded_stale"; 3]);
        let state: String = sqlx::query_scalar("SELECT state FROM managed_attempt_receipts WHERE account_id=$1").bind(f.account).fetch_one(f.owner()).await.unwrap();
        assert_eq!(state, "billed", "the sent call is still charged");
        assert_eq!(job_state(&f, job).await.0, "done");
        f.finish().await;
    }
}

/// The shared daily cap holds the pass before any send, leaves no receipt,
/// and reschedules the graph for the next UTC day instead of dead-lettering.
#[tokio::test]
async fn er_daily_cap_holds_without_dead_letter_both_planes() {
    let _env = Env::set(ON);
    let _cap = Env::set(&[("MANAGED_DAILY_CAP_USD_ENRICHMENT", Some("0.000001"))]);
    for org in [false, true] {
        let f = funded(org).await;
        let keep = reef(&f, "GitHub", "org").await;
        let dup = reef(&f, "github", "org").await;
        let judge = Judge::new(&[("ithub", SAME)]);
        let job = enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 0, "the cap holds before the send");
        assert!(exists(&f, keep).await && exists(&f, dup).await);
        assert_eq!(f.count_receipts().await, 0, "a held reservation leaves no receipt");
        assert_eq!(job_state(&f, job).await, ("done".into(), None), "held, not dead-lettered");
        let next: (String, bool) = sqlx::query_as(
            "SELECT status::text, run_after > now() + interval '30 seconds' FROM jobs WHERE kind='EntityResolution' AND id<>$1",
        ).bind(job).fetch_one(f.owner()).await.unwrap();
        assert_eq!(next, ("pending".into(), true), "the graph comes back after the UTC day turns");
        f.finish().await;
    }
}

/// Off by default: nothing is claimed, produced or spent, and a job handed to
/// the worker directly is held in place untouched (`jobs::queue::hold`).
#[tokio::test]
async fn er_switch_is_off_by_default_both_planes() {
    let _env = Env::set(ON);
    let _off = Env::set(&[("ENRICHMENT_PROCESSING_ENABLED", None)]);
    for org in [false, true] {
        let f = funded(org).await;
        let keep = reef(&f, "Linear", "org").await;
        let dup = reef(&f, "linear", "org").await;
        let judge = Judge::new(&[("inear", SAME)]);
        // The producer hook schedules nothing while off.
        let mut conn = f.app().acquire().await.unwrap();
        entity_resolution::enqueue_after_binding(&mut conn, f.account, f.graph).await.unwrap();
        drop(conn);
        assert_eq!(scalar_i64(&f, "SELECT count(*) FROM jobs WHERE payload->>'graph_id'=$1::text").await, 0);
        let job = enqueue(&f, f.app()).await;
        let tick = entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(tick.processed, 0, "nothing claimed while off");
        assert_eq!(job_state(&f, job).await, ("pending".into(), None));
        // Claimed before a flip-off: handed back, nothing spent or written.
        let row = queue::dequeue(f.app(), &["EntityResolution"], 4).await.unwrap().pop().unwrap();
        crate::workers::entity_resolution::process_job(&f.tenant, f.app(), &f.control.app,
            &PostgresStore::new(f.app().clone()), &crate::llm::testing::GatedScript(&judge), row).await;
        assert_eq!(judge.calls(), 0);
        assert!(exists(&f, keep).await && exists(&f, dup).await);
        assert_eq!(job_state(&f, job).await, ("pending".into(), Some("held: managed_processing_disabled".into())));
        assert_eq!(scalar_i64(&f, "SELECT count(*) FROM jobs WHERE kind='EntityResolution' AND payload->>'graph_id'=$1::text").await, 1,
            "the graph is handed back for a later pass, on the same row");
        for table in ["managed_attempt_receipts", "managed_budget_periods", "managed_enrichment_inputs", "reef_judgement", "node_merges"] {
            assert_eq!(f.count_all(table).await, 0, "{table} untouched while off");
        }
        // Switched on, the same hook schedules one debounced pass per graph.
        {
            let _on = Env::set(&[("ENRICHMENT_PROCESSING_ENABLED", Some("true"))]);
            sqlx::query("DELETE FROM jobs WHERE kind='EntityResolution'").execute(f.owner()).await.unwrap();
            for _ in 0..2 {
                let mut conn = f.app().acquire().await.unwrap();
                entity_resolution::enqueue_after_binding(&mut conn, f.account, f.graph).await.unwrap();
            }
            let pending: (i64, bool) = sqlx::query_as(
                "SELECT count(*), bool_and(run_after > now()) FROM jobs WHERE kind='EntityResolution' AND status='pending' AND idempotency_key=$1",
            ).bind(format!("er:{}", f.graph)).fetch_one(f.owner()).await.unwrap();
            assert_eq!(pending, (1, true), "one debounced pass per graph");
        }
        f.finish().await;
    }
}

/// The free paths spend nothing: a shared decisive identifier is decided with
/// no model call (while the ingest door is not active, unchanged), and an
/// existing POSITIVE materialises with no model call. Neither opens a
/// receipt or a budget period. With `JEV_INGEST_DOOR_MODE=shadow` (today's
/// production value) the identifier shortcut stays paused and the pair is
/// judged instead.
///
/// `reef_identifiers` is UNIQUE(graph_id,kind,value_norm), so on Postgres two
/// reefs of one graph can never share a value: the shortcut is unreachable in
/// production, and `merge_nodes_impl`'s identifier union (`ON CONFLICT` on
/// that key) cannot run once the key is relaxed. This scratch database drops
/// the key to drive the branch and asserts what it decides and spends (an
/// `identifier_auto` POSITIVE, no call, no receipt), not the merge.
#[tokio::test]
async fn er_identifier_auto_merge_and_materialisation_stay_free_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        // 1. A confirmed POSITIVE materialises with no model call.
        let c = reef(&f, "Orbit", "project").await;
        let d = reef(&f, "orbit", "project").await;
        PostgresStore::new(f.owner().clone()).write_judgement(crate::store::NewJudgement {
            graph_id: f.graph, node_a_id: c, node_b_id: d,
            verdict: crate::store::JudgementVerdict::Positive, source: crate::store::JudgementSource::Human,
            confidence: None, model_id: None, prompt_version: None,
            evidence: serde_json::json!({"route": "merge_review_confirm"}), created_by: Some(f.payer), expires_at: None,
        }).await.unwrap();
        let judge = Judge::new(&[("rbit", DIFFERENT)]);
        enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 0, "materialisation makes no model call");
        assert_eq!(exists(&f, c).await as u8 + exists(&f, d).await as u8, 1, "confirmed POSITIVE materialised");
        // 2. Relax the identifier key (see the doc above): the shared-email
        //    pair is decided by the shortcut with no model call.
        sqlx::raw_sql("DO $$ DECLARE c text; BEGIN FOR c IN SELECT conname FROM pg_constraint WHERE conrelid='reef_identifiers'::regclass AND contype='u' LOOP EXECUTE format('ALTER TABLE reef_identifiers DROP CONSTRAINT %I', c); END LOOP; END $$;")
            .execute(f.owner()).await.unwrap();
        let a = reef(&f, "Dana Whitfield", "person").await;
        let b = reef(&f, "dana whitfield", "person").await;
        for node in [a, b] {
            sqlx::query("INSERT INTO reef_identifiers(graph_id,node_id,kind,value_norm,source) VALUES($1,$2,'email','dana@example.test','extraction')")
                .bind(f.graph).bind(node).execute(f.owner()).await.unwrap();
        }
        let judge = Judge::new(&[("ana", DIFFERENT)]);
        enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 0, "the identifier shortcut makes no model call");
        let (_, verdict, source, _, _) = judgement(&f, a, b).await.unwrap();
        assert_eq!((verdict.as_str(), source.as_str()), ("positive", "identifier_auto"));
        for table in ["managed_attempt_receipts", "managed_budget_periods", "managed_enrichment_inputs"] {
            assert_eq!(f.count_all(table).await, 0, "{table}: the free paths open no receipt");
        }
        // Ingest door in shadow (production today): the shortcut is paused,
        // so a fresh identifier pair is judged (and kept apart here).
        let _door = Env::set(&[("JEV_INGEST_DOOR_MODE", Some("shadow"))]);
        let e = reef(&f, "Rui Costa", "person").await;
        let g = reef(&f, "rui costa", "person").await;
        for node in [e, g] {
            sqlx::query("INSERT INTO reef_identifiers(graph_id,node_id,kind,value_norm,source) VALUES($1,$2,'email','rui@example.test','extraction')")
                .bind(f.graph).bind(node).execute(f.owner()).await.unwrap();
        }
        let judge = Judge::new(&[("ui", DIFFERENT)]);
        enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 1, "door shadow keeps the identifier shortcut paused");
        assert!(exists(&f, e).await && exists(&f, g).await);
        let (_, verdict, source, _, _) = judgement(&f, e, g).await.unwrap();
        assert_eq!((verdict.as_str(), source.as_str()), ("negative", "llm"), "judged and kept apart instead");
        f.finish().await;
    }
}

/// Same title, same type, two different emails: two people. ER never asks
/// the model (which would say "same" on the title) and never merges; it
/// records the conflict so the pair is not picked again.
#[tokio::test]
async fn er_never_merges_conflicting_identities_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let a = reef(&f, "Alex Kim", "person").await;
        let b = reef(&f, "alex kim", "person").await;
        for (node, email) in [(a, "alex.one@example.test"), (b, "alex.two@example.test")] {
            sqlx::query("INSERT INTO reef_identifiers(graph_id,node_id,kind,value_norm,source) VALUES($1,$2,'email',$3,'extraction')")
                .bind(f.graph).bind(node).bind(email).execute(f.owner()).await.unwrap();
        }
        let judge = Judge::new(&[("lex", SAME)]);
        enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 0, "a conflicting identifier is decided without the model");
        assert!(exists(&f, a).await && exists(&f, b).await, "never merged");
        let (_, verdict, source, evidence, _) = judgement(&f, a, b).await.unwrap();
        assert_eq!((verdict.as_str(), source.as_str()), ("negative", "identifier_auto"));
        assert!(evidence["identifier_conflict"].to_string().contains("conflicting email"), "{evidence}");
        assert_eq!(f.count_all("managed_attempt_receipts").await, 0, "no call, no receipt");
        enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 0);
        assert_eq!(scalar_i64(&f, "SELECT count(*) FROM node_merges WHERE graph_id=$1").await, 0);
        f.finish().await;
    }
}

/// Jev stays in the loop and is metered: with `JEV_JUDGE_MODE=live` the
/// verdict comes from Jev's Choice, sent through the managed gate as its
/// own `managed_enrichment` call; the LLM verdict is never asked.
#[tokio::test]
async fn er_jev_judge_is_metered_under_managed_enrichment_both_planes() {
    let _env = Env::set(ON);
    let _jev = Env::set(&[("JEV_JUDGE_MODE", Some("live"))]);
    for org in [false, true] {
        let f = funded(org).await;
        let keep = reef(&f, "Fly.io", "org").await;
        let dup = reef(&f, "fly.io", "org").await;
        let judge = Judge::new(&[("ly.io", DIFFERENT)]);
        enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!((judge.calls(), judge.jev_calls.load(Ordering::SeqCst)), (1, 1), "one call, and it is Jev's");
        assert!(exists(&f, keep).await && !exists(&f, dup).await, "Jev's same at 0.96 merged");
        let (purpose, state): (String, String) = sqlx::query_as("SELECT purpose,state FROM managed_attempt_receipts WHERE account_id=$1")
            .bind(f.account).fetch_one(f.owner()).await.unwrap();
        assert_eq!((purpose.as_str(), state.as_str()), ("managed_enrichment", "billed"));
        let task: String = sqlx::query_scalar("SELECT task FROM llm_usage WHERE graph_id=$1").bind(f.graph).fetch_one(f.owner()).await.unwrap();
        assert_eq!(task, "jev_judge");
        f.finish().await;
    }
}

/// Pairs the pass can only leave unresolved (unresolved identities of two
/// different types, no identifier in common) used to sit at the head of
/// every pass for good: the most corals, the first 200 of the same-title
/// stream, and lexical slots. With 251 of them ahead (more than a lexical
/// page and the stream's 200), a judgeable pair still gets judged in the
/// first pass, and the stuck pairs are neither judged nor merged.
#[tokio::test]
async fn er_unjudgeable_pairs_do_not_starve_the_backlog_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        sqlx::query(
            "INSERT INTO nodes(graph_id,type,title,entity_type,body,identity_scope,coral_count,created_at,updated_at) \
             SELECT $1,'entity',md5(i::text),t,jsonb_build_object('entity_type',t,'identity_status','unresolved'),gen_random_uuid(),100, \
                    now()-interval '2 days', now()-interval '2 days' \
               FROM generate_series(1,251) i, unnest(ARRAY['person','topic']) t",
        )
        .bind(f.graph)
        .execute(f.owner())
        .await
        .unwrap();
        let keep = reef(&f, "Composio", "org").await;
        let dup = reef(&f, "composio", "org").await;
        let judge = Judge::new(&[("omposio", SAME)]);
        let job = enqueue(&f, f.app()).await;
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(job_state(&f, job).await, ("done".into(), None));
        assert_eq!(judge.calls(), 1, "the judgeable pair is judged in the first pass, and only it");
        assert!(exists(&f, keep).await && !exists(&f, dup).await, "merged");
        assert_eq!(scalar_i64(&f, "SELECT count(*) FROM node_merges WHERE graph_id=$1").await, 1, "no stuck pair merged");
        assert_eq!(scalar_i64(&f, "SELECT count(*) FROM nodes WHERE graph_id=$1 AND body->>'identity_status'='unresolved'").await, 502,
            "ambiguous stays unresolved");
        f.finish().await;
    }
}

/// A pass stops at its wall-clock budget, publishes what it judged, and
/// hands the rest to a pass seconds later (not the old 2 minutes), so a
/// pass never outlives the reaper's claim window.
#[tokio::test]
async fn er_pass_budget_hands_the_rest_to_a_prompt_next_pass_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let pairs = [
            (reef(&f, "Composio", "org").await, reef(&f, "composio", "org").await),
            (reef(&f, "Linear", "org").await, reef(&f, "linear", "org").await),
        ];
        let judge = Judge::new(&[("omposio", SAME), ("inear", SAME)]);
        let job = enqueue(&f, f.app()).await;
        {
            let _spent = Env::set(&[("ER_PASS_BUDGET_SECS", Some("0"))]);
            entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        }
        assert_eq!(judge.calls(), 0, "no pair starts once the budget is spent");
        assert_eq!(job_state(&f, job).await, ("done".into(), None), "not dead-lettered");
        let next: (String, bool) = sqlx::query_as(
            "SELECT status::text, run_after <= now() + interval '60 seconds' FROM jobs WHERE kind='EntityResolution' AND id<>$1",
        )
        .bind(job)
        .fetch_one(f.owner())
        .await
        .unwrap();
        assert_eq!(next, ("pending".into(), true), "the rest continues within a minute");
        sqlx::query("UPDATE jobs SET run_after=now() WHERE kind='EntityResolution' AND status='pending'").execute(f.owner()).await.unwrap();
        entity_resolution::tick(&worker(&f, f.app()), &f.control.app, &crate::llm::testing::GatedScript(&judge)).await.unwrap();
        assert_eq!(judge.calls(), 2, "the next pass judges both");
        for (keep, dup) in pairs {
            assert!(exists(&f, keep).await && !exists(&f, dup).await);
        }
        f.finish().await;
    }
}

impl Fixture {
    async fn count_receipts(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE account_id=$1")
            .bind(self.account).fetch_one(self.owner()).await.unwrap()
    }
    async fn count_all(&self, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table}")).fetch_one(self.owner()).await.unwrap()
    }
}
