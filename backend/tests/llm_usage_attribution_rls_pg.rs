//! `llm_usage` attribution and current unsupported-worker refusal proofs.
//!
//! The two direct record() tests keep actual coralswarm_app attribution,
//! strict unattributed-drop/RLS, provider defaults and nullable ledger joins.
//! The four legacy automatic purposes are Unsupported under P10 (0307/0142,
//! retained by 0324/0156); their real typed queue/tick paths must make no
//! provider call, usage/pending row, reservation, charge or private-row change.
//! This suite no longer claims those dormant worker call sites executed.
//! Restoring them requires managed typed inputs/receipts/budgets, not a fixture
//! or test-only bypass. See docs/connectors/CORPUS_ENRICHMENT_CONTRACT.md.
//!
//! Gated on ORGT_TEST_BASE_URL; no database prerequisite or CI gate is relaxed.

mod support;

use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

use coralswarm_backend::jobs::{queue, JobKind};
use coralswarm_backend::llm::{
    CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent,
};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::workers::entity_resolution;
use sqlx::PgPool;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

// ── Stub LLM ──────────────────────────────────────────────────────────────────

/// Any unsupported worker provider invocation is an immediate failure.
#[derive(Default)]
struct RefusalSpy(AtomicUsize);

#[async_trait::async_trait]
impl LlmProvider for RefusalSpy {
    async fn complete(&self, _: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("Unsupported automatic purpose must not call the provider")
    }
    async fn stream_complete(
        &self, _: CompletionRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item=Result<StreamEvent,LlmError>> + Send>>,LlmError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("Unsupported automatic purpose must not stream")
    }
}

// ── Fixture helpers (all seeding via the owner/BYPASSRLS pool) ────────────────

fn consumer_tenant(pool: &PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: TenantId::Consumer,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
    }
}

async fn seed_inline_atom(
    owner: &PgPool,
    graph_id: Uuid,
    content: &str,
    author: Option<&str>,
    with_embedding: bool,
) -> Uuid {
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \
                            content_inline, author_name, timestamp) \
         VALUES ($1, 'test', $2, '', $3, $4, now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("usage-{}", Uuid::new_v4()))
    .bind(content)
    .bind(author)
    .fetch_one(owner)
    .await
    .expect("insert atom");
    if with_embedding {
        let vec_literal = format!("[{}]", vec!["0.1"; 384].join(","));
        sqlx::query("UPDATE atoms SET embedding = $1::vector WHERE atom_id = $2")
            .bind(vec_literal)
            .bind(atom_id)
            .execute(owner)
            .await
            .expect("stamp embedding");
    }
    atom_id
}

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

async fn seed_confirmed_edge(
    owner: &PgPool,
    graph_id: Uuid,
    src: Uuid,
    predicate: &str,
    dst: Uuid,
) {
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

/// Read the `llm_usage` rows this test cares about via the OWNER (BYPASSRLS)
/// pool — the honest "what actually reached disk" view, unfiltered by the very
/// policy under test.
async fn usage_rows(owner: &PgPool, task: &str) -> Vec<(Option<Uuid>, Option<Uuid>, i32)> {
    sqlx::query_as(
        "SELECT account_id, graph_id, input_tokens FROM llm_usage WHERE task = $1 \
         ORDER BY created_at",
    )
    .bind(task)
    .fetch_all(owner)
    .await
    .expect("read llm_usage rows")
}

async fn count_all_usage(owner: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM llm_usage")
        .fetch_one(owner)
        .await
        .expect("count llm_usage")
}

async fn count_unattributed_usage(owner: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM llm_usage WHERE account_id IS NULL")
        .fetch_one(owner)
        .await
        .expect("count unattributed llm_usage")
}

/// Check that `task` recorded at least one row and that EVERY row it recorded
/// carries `account` (never NULL, never the other tenant's id).
///
/// Returns a description for the actual record() attribution assertions below.
/// Legacy worker dispatch is now blocked; this helper does not prove those
/// dormant call sites still execute or correctly meter after restoration.
fn attribution_failure(
    rows: &[(Option<Uuid>, Option<Uuid>, i32)],
    task: &str,
    account: Uuid,
    graph: Uuid,
) -> Option<String> {
    if rows.is_empty() {
        return Some(format!(
            "task {task:?}: recorded NO llm_usage row. This is exactly the post-OPS-4 \
             regression the suite exists to catch — `record` is best-effort, so an \
             unattributed insert is rejected by 0083's policy and vanishes with only a WARN."
        ));
    }
    for (account_id, graph_id, input_tokens) in rows {
        if *account_id != Some(account) {
            return Some(format!(
                "task {task:?}: llm_usage row attributed to {account_id:?}, expected the job's \
                 own account {account}"
            ));
        }
        if *graph_id != Some(graph) {
            return Some(format!(
                "task {task:?}: llm_usage row carries ocean {graph_id:?}, expected {graph}"
            ));
        }
        if *input_tokens != 11 {
            return Some(format!(
                "task {task:?}: token counts did not round-trip (input_tokens = {input_tokens}, \
                 expected 11)"
            ));
        }
    }
    None
}

// ── 1. The mechanism + its negative control ───────────────────────────────────

/// `record()` against a REAL `coralswarm_app` login, both ways round:
///
/// * `Some(account)` — the row lands, readable back and correctly attributed.
/// * `None` — NOTHING lands. This half is the negative control for the whole
///   suite: it is precisely the call shape all five worker sites used before
///   this change, and it demonstrates that the loss is silent (`record` is
///   best-effort and returns `()`, so there is no error for a caller to
///   notice). It is also what makes test 2 meaningful rather than vacuous.
#[tokio::test]
async fn record_attributed_row_lands_unattributed_row_is_dropped() {
    let Some(probe) = RlsProbe::setup("orgt_llm_usage_record").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    // ── Attributed: lands ────────────────────────────────────────────────────
    coralswarm_backend::llm::usage::record(
        &probe.app_pool,
        Some(probe.a.account_id),
        None, // consumer-plane probe — no tenant user concept here
        Some(probe.a.graph_id),
        "probe_attributed",
        "stub-model",
        11,
        7,
        0,
        0,
        // Billing Phase 1's `attributed_user_id` — a DIFFERENT, unrelated
        // "attribution" from this file's own account_id/RLS sense; not what
        // this suite is proving, so left None.
        None,
    )
    .await;
    let attributed = usage_rows(&probe.owner, "probe_attributed").await;
    assert_eq!(
        attributed.len(),
        1,
        "an attributed record() must write exactly one row as coralswarm_app"
    );
    assert_eq!(
        attribution_failure(&attributed, "probe_attributed", probe.a.account_id, probe.a.graph_id),
        None
    );

    // ── Unattributed: silently dropped (the regression, pinned) ──────────────
    coralswarm_backend::llm::usage::record(
        &probe.app_pool,
        None,
        None, // consumer-plane probe — no tenant user concept here
        Some(probe.a.graph_id),
        "probe_unattributed",
        "stub-model",
        11,
        7,
        0,
        0,
        None,
    )
    .await;
    let unattributed = usage_rows(&probe.owner, "probe_unattributed").await;
    assert!(
        unattributed.is_empty(),
        "an UNattributed record() must write nothing under 0083's strict policy — it is \
         rejected by the `current_setting('app.account_id')::uuid` cast and swallowed by the \
         best-effort contract. Got {unattributed:?}. If this row now lands, the RLS policy or \
         the strict current_setting has been loosened, and the accounting fix in this suite is \
         resting on a property that no longer holds."
    );

    // ── And a scoped app-role read sees its own row ──────────────────────────
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let visible: Vec<Uuid> =
        sqlx::query_scalar("SELECT account_id FROM llm_usage WHERE task = 'probe_attributed'")
            .fetch_all(&mut *tx)
            .await
            .expect("scoped read of llm_usage");
    tx.rollback().await.ok();
    assert_eq!(
        visible,
        vec![probe.a.account_id],
        "account A, scoped, must be able to read back its own usage row"
    );

    probe.teardown().await;
}

// ── 2. Current Unsupported worker admission, not dormant paid positives ──────

async fn fund_accounts(probe: &RlsProbe) {
    for a in [&probe.a, &probe.b] {
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
            .bind(a.account_id).bind(a.user_id).execute(&probe.owner).await.unwrap();
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&probe.owner,a.account_id).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx,a.account_id).await.unwrap();
        sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,1000000) ON CONFLICT(account_id) DO UPDATE SET balance=excluded.balance")
            .bind(a.account_id).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
    }
}
async fn private_rows(probe: &RlsProbe) -> serde_json::Value {
    sqlx::query_scalar("SELECT jsonb_build_object('atoms',(SELECT jsonb_agg(to_jsonb(a) ORDER BY atom_id) FROM atoms a),'nodes',(SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id) FROM nodes n),'links',(SELECT jsonb_agg(to_jsonb(l) ORDER BY node_id,atom_id) FROM node_atoms l),'edges',(SELECT jsonb_agg(to_jsonb(e) ORDER BY edge_id) FROM relationship_edge e),'operations',(SELECT jsonb_agg(to_jsonb(o) ORDER BY seq) FROM op_log o))")
        .fetch_one(&probe.owner).await.unwrap()
}
async fn financial_rows(probe: &RlsProbe) -> serde_json::Value {
    sqlx::query_scalar("SELECT jsonb_build_object('balances',(SELECT jsonb_agg(jsonb_build_array(account_id,balance) ORDER BY account_id) FROM credit_balances),'ledger',(SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM credit_ledger l),'attempts',(SELECT count(*) FROM managed_attempt_receipts),'reservations',(SELECT count(*) FROM managed_credit_reservations),'pending',(SELECT count(*) FROM llm_usage_pending),'usage',(SELECT count(*) FROM llm_usage))")
        .fetch_one(&probe.owner).await.unwrap()
}

/// The old worker-positive oracle cannot run under approved Unsupported
/// admission. Keep actual queue receipts/ticks and no-provider/no-charge invariants;
/// direct record() attribution and its negative control remain separately retained.
#[tokio::test]
async fn unsupported_worker_purposes_refuse_without_usage_or_private_changes() {
    let Some(probe) = RlsProbe::setup("orgt_llm_usage_workers").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let wt = consumer_tenant(&probe.app_pool);
    let account = probe.a.account_id;
    let graph = probe.a.graph_id;
    fund_accounts(&probe).await;
    let role: (String,bool,bool,i64) = sqlx::query_as("SELECT current_user::text,rolsuper,rolbypassrls,(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user")
        .fetch_one(&probe.app_pool).await.unwrap();
    assert_eq!(role,("coralswarm_app".into(),false,false,0));
    let atom = seed_inline_atom(&probe.owner,graph,"alice ships payments",None,true).await;
    let rel_atom = seed_inline_atom(&probe.owner,graph,"alice works on payments",Some("Carol"),false).await;
    seed_node(&probe.owner,graph,"Probe Node Duplicate").await;
    let dana = seed_node(&probe.owner,graph,"Dana").await;
    let payments = seed_node(&probe.owner,graph,"Payments Platform").await;
    seed_confirmed_edge(&probe.owner,graph,dana,"works_on",payments).await;
    probe.assert_scoped_returns_own_rows(account,"SELECT atom_id FROM atoms WHERE atom_id=$1",&[atom],&[atom]).await;
    probe.assert_cross_account_returns_nothing(account,"SELECT atom_id FROM atoms WHERE atom_id=$1",&[probe.b.atom_id]).await;
    probe.assert_unscoped_hard_errors("SELECT atom_id FROM atoms",&[]).await;
    let private = private_rows(&probe).await;
    assert!(private["atoms"].as_array().unwrap().len()>=4,"both accounts and real worker input atoms must exist");
    let money = financial_rows(&probe).await;
    assert_eq!(money["balances"].as_array().unwrap().len(),2);
    assert!(money["balances"].as_array().unwrap().iter().all(|r|r[1]==1_000_000));
    for key in ["attempts","reservations","pending","usage"] {assert_eq!(money[key],0);}
    assert_eq!(count_all_usage(&probe.owner).await,0);
    let mut receipts = Vec::new();
    // The per-atom extraction pair is retired (source-agnostic PR 11): no
    // variant, so no Rust path can enqueue it. A row written by hand, as the
    // app role or the owner prod connects as, is dead on arrival.
    for (kind,atom_id,pool) in [
        ("ExtractEntities",atom,&probe.app_pool),
        ("ExtractRelationships",rel_atom,&probe.owner),
    ] {
        let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool,account).await.unwrap();
        let stored:(Uuid,String,String,Option<Uuid>)=sqlx::query_as("INSERT INTO jobs(kind,payload,account_id) VALUES($1,$2,$3) RETURNING id,status::text,error_message,account_id")
            .bind(kind).bind(serde_json::json!({"atom_id":atom_id})).bind(account).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!((stored.1.as_str(),stored.2.as_str(),stored.3),("dead","managed_purpose_unsupported",Some(account)),"{kind}");
        receipts.push(stored.0);
    }
    // EntityResolution is managed enrichment (source-agnostic PR 5): admitted
    // pending, and never claimed while ENRICHMENT_PROCESSING_ENABLED is off.
    std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
    let er_job = queue::enqueue(&probe.app_pool,&JobKind::EntityResolution{graph_id:graph},Some(account),None,0).await.unwrap().unwrap();
    let er_stored:(String,String,Option<String>,Option<Uuid>)=sqlx::query_as("SELECT kind,status::text,error_message,account_id FROM jobs WHERE id=$1")
        .bind(er_job).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(er_stored,("EntityResolution".into(),"pending".into(),None,Some(account)));
    // InferExpertise is retired (no variant): a hand-inserted row is still
    // dead-lettered by the admission trigger, so no worker ever waits on it.
    let retired:(Uuid,String,String)=sqlx::query_as("INSERT INTO jobs(kind,payload,account_id) VALUES('InferExpertise',$1,$2) RETURNING id,status::text,error_message")
        .bind(serde_json::json!({"graph_id":graph})).bind(account).fetch_one(&probe.owner).await.unwrap();
    assert_eq!((retired.1.as_str(),retired.2.as_str()),("dead","managed_purpose_unsupported"));
    receipts.push(retired.0);
    let spy = RefusalSpy::default();
    assert_eq!(entity_resolution::tick(&wt,&probe.app_pool,&spy).await.unwrap().processed,0,"switched-off resolution is not dequeued");
    assert_eq!(spy.0.load(Ordering::SeqCst),0);
    let unchanged:i64=sqlx::query_scalar("SELECT count(*) FROM jobs WHERE id=ANY($1) AND account_id=$2 AND status='dead' AND error_message='managed_purpose_unsupported'")
        .bind(receipts).bind(account).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(unchanged,3,"all three real refusal receipts must remain terminal");
    let er_status:String=sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1").bind(er_job).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(er_status,"pending","the switched-off worker leaves the admitted job untouched");
    assert_eq!(private_rows(&probe).await,private,"no own/foreign content, title, relation or operation may change");
    assert_eq!(financial_rows(&probe).await,money,"Unsupported work reserves/spends no funded credits and records no usage");
    assert_eq!(count_all_usage(&probe.owner).await,0);
    assert_eq!(count_unattributed_usage(&probe.owner).await,0,"no unattributed usage may be manufactured");
    probe.teardown().await;
}

// ── 3. PR2 schema — provider default + nullable ledger join keys ──────────────

/// Usage-metering PR2 (`.scratch/usage-metering/plan.md` §1 PR2) adds
/// `provider`/`cost_usd`/`source_job_id`/`source_query_id` to `llm_usage`
/// ahead of any code that writes them — that code is PR4. This test is BOTH
/// PR2's "done + verified" proof and its second negative control:
///
///   * a row written through the EXISTING, UNMODIFIED-by-PR2 `record()` path
///     lands with `provider`'s column DEFAULT (`'anthropic'`) and `cost_usd`
///     NULL — nothing here computes a real cost yet (PR3/PR9);
///   * critically, the insert SUCCEEDS at all despite `record()`'s SQL never
///     mentioning `source_job_id`/`source_query_id` — which is only possible
///     if both columns are NULLABLE. If either were ever tightened to `NOT
///     NULL`, this exact insert (identical in shape to every one of the 24
///     existing `usage::record` call sites) would start failing, which is
///     precisely the regression plan §1 PR2's negative control #2 exists to
///     catch.
#[tokio::test]
async fn record_lands_with_provider_default_and_nullable_ledger_join_keys() {
    let Some(probe) = RlsProbe::setup("orgt_llm_usage_pr2_columns").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    // Same call shape as every existing `usage::record` site (PR4 has not
    // run — nothing threads a job/query id here).
    coralswarm_backend::llm::usage::record(
        &probe.app_pool,
        Some(probe.a.account_id),
        None,
        Some(probe.a.graph_id),
        "probe_pr2_columns",
        "stub-model",
        11,
        7,
        0,
        0,
        None,
    )
    .await;

    // `cost_usd::float8`: the crate's sqlx build has no bigdecimal/rust_decimal
    // feature enabled (see routes/admin.rs's own `SUM(bigint)` cast note for
    // the same pitfall), so a bare `numeric` column cannot be decoded directly
    // — cast to a type sqlx already knows, matching that file's convention.
    let row: (String, Option<f64>, Option<Uuid>, Option<Uuid>) = sqlx::query_as(
        "SELECT provider, cost_usd::float8, source_job_id, source_query_id FROM llm_usage \
         WHERE task = 'probe_pr2_columns'",
    )
    .fetch_one(&probe.owner)
    .await
    .expect(
        "a row written through the existing record() path must exist, and reading these four \
         columns back must succeed — if this panics with a decode/column error, the migration \
         did not apply the shape this test expects",
    );

    assert_eq!(
        row.0, "anthropic",
        "provider must default to 'anthropic' — the column DEFAULT is the whole backfill, no \
         one-shot script exists or should exist for this"
    );
    assert_eq!(row.1, None, "cost_usd must be NULL — nothing computes a real cost until PR3/PR9");
    assert_eq!(
        row.2, None,
        "source_job_id must be NULL post-insert, which also PROVES it is nullable: `record()`'s \
         INSERT never mentions this column, so a NOT NULL constraint would have made this whole \
         insert fail loudly instead of landing with NULL here"
    );
    assert_eq!(
        row.3, None,
        "source_query_id must be NULL post-insert, which also PROVES it is nullable — same \
         reasoning as source_job_id above"
    );

    probe.teardown().await;
}
