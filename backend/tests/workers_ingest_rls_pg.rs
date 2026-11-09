//! W.2 group (a) — RLS-scope conversion proofs for the ingest-pipeline
//! workers (`embed`, `embed_retry`) and the source-atoms v2/v3 writers.
//!
//! Each worker's per-job processing now runs its RLS-table statements inside
//! `tenancy::scope::begin_account_scoped` transactions keyed by the job's own
//! `account_id` (Phase W.1). These tests drive the REAL worker ticks against
//! the P.1 [`RlsProbe`] harness — i.e. on a pool authenticated AS the
//! non-BYPASSRLS `coralswarm_app` role with every `0083_consumer_rls` policy
//! live — and prove, per worker:
//!
//!   1. **Scoped processing**: a job stamped for account A processes A's own
//!      rows successfully (writes land in A's graph, the job completes).
//!   2. **Cross-account isolation** (the security property): a job stamped
//!      for account A whose PAYLOAD references account B's atom ids cannot
//!      affect B's rows —
//!        * `embed` REFUSES the job outright (the DEK resolver's
//!          payload-vs-stamp ownership check, which holds even pre-OPS-4);
//!        * `embed_retry` claims per account inside per-account scopes, so a
//!          claim can never mix accounts by construction (asserted on the
//!          jobs it enqueues).
//!   3. The **defensive `None`-`account_id` failure** is covered by in-module
//!      live tests next to each worker (`process_job_without_account_id_fails_defensively`
//!      in `embed.rs`): that
//!      state is unreachable through the tick path — `queue::dequeue`
//!      backfills or dead-letters every NULL row of an RLS-relevant kind
//!      before a worker sees it — so it must be exercised by calling the
//!      private `process_job` directly, which only an in-module test can do.
//!
//! Gated on `ORGT_TEST_BASE_URL`, one scratch DB per test (the P.1 harness
//! convention):
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test workers_ingest_rls_pg

mod support;

use std::sync::Arc;
use std::time::Instant;

use coralswarm_backend::jobs::{queue, JobKind};
use coralswarm_backend::llm::{
    CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent,
};
use coralswarm_backend::shutdown::Shutdown;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::workers::{embed, embed_retry};
use coralswarm_core::embed::{EmbedBatchFuture, EmbedProvider};
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use sqlx::PgPool;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

// ── Stub providers ────────────────────────────────────────────────────────────

/// 384-dim constant-vector embedder (matches `atoms.embedding vector(384)`).
/// It has no gated transport, so the managed embedding gate can never admit
/// it: every call would be an ungated cloud dispatch, and tests assert this
/// stays zero. (The admitted path is proven against the real gate and a
/// loopback provider in `managed::embedding::pg_tests`.)
static CLOUD_EMBED_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
async fn embed_stamp(pool: &PgPool, atom_id: Uuid) -> (bool, Option<String>, bool) {
    sqlx::query_as("SELECT embedding_next IS NOT NULL, embed_model_next, content_tsv IS NOT NULL FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(pool)
        .await
        .unwrap()
}
struct StubEmbedder;
impl EmbedProvider for StubEmbedder {
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        CLOUD_EMBED_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let n = texts.len();
        Box::pin(async move { Ok(vec![vec![0.1_f32; 384]; n]) })
    }
}

/// LLM returning a fixed extraction/relationship payload.
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
/// payload never makes it past the scoped atom load.
struct PanicLlm;
#[async_trait::async_trait]
impl LlmProvider for PanicLlm {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        panic!("LLM must not be called for a cross-account payload (RLS should have filtered it)");
    }
    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        panic!("LLM must not be called for a cross-account payload (RLS should have filtered it)");
    }
}

// ── Fixture helpers ───────────────────────────────────────────────────────────

/// Fixed test KEK (same pattern + safety reasoning as `worker_dek_egress_pg.rs`:
/// every test writes the SAME constant value, so concurrent writes are benign).
fn set_test_kek() {
    unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) };
}

/// Give an account a real wrapped DEK on the control plane (the consumer-tick
/// DEK source) via the owner pool.
async fn give_account_dek(owner: &PgPool, account_id: Uuid) {
    let kek = load_kek().expect("test KEK loads");
    let wrapped = wrap_dek(&kek, &generate_dek()).unwrap();
    sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
        .bind(&wrapped)
        .bind(account_id)
        .execute(owner)
        .await
        .expect("set account dek_wrapped");
}

/// Seed an atom with inline content (the r2 = None embed path) via the owner
/// (BYPASSRLS) pool. `with_embedding` also stamps a 384-dim vector so the
/// extract worker's `embedding IS NOT NULL` filter admits it.
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
    .bind(format!("w2a-{}", Uuid::new_v4()))
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

/// Seed an embed-FAILED atom (for the retry sweep): backoff-elapsed, still
/// under the attempts cap, embedding NULL.
async fn seed_failed_atom(owner: &PgPool, graph_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                            timestamp, embed_attempts, embed_failed_at) \
         VALUES ($1, 'test', $2, '', 'failed content', now(), 1, now() - INTERVAL '300 seconds') \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("w2a-fail-{}", Uuid::new_v4()))
    .fetch_one(owner)
    .await
    .expect("insert failed atom")
}

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

async fn atom_has_embedding(owner: &PgPool, atom_id: Uuid) -> bool {
    sqlx::query_scalar("SELECT embedding_next IS NOT NULL FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(owner)
        .await
        .expect("read embedding state")
}

async fn count_scalar(owner: &PgPool, sql: &str, bind: Uuid) -> i64 {
    sqlx::query_scalar(sql).bind(bind).fetch_one(owner).await.expect("count query")
}

// ── embed ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn embed_worker_scoped_processing_and_cross_account_isolation() {
    let Some(probe) = RlsProbe::setup("orgt_w2a_embed").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    set_test_kek();
    give_account_dek(&probe.owner, probe.a.account_id).await;
    give_account_dek(&probe.owner, probe.b.account_id).await;

    let atom_a = seed_inline_atom(&probe.owner, probe.a.graph_id, "alpha content", None, false).await;
    let atom_b = seed_inline_atom(&probe.owner, probe.b.graph_id, "beta content", None, false).await;

    let wt = consumer_tenant(&probe.app_pool);
    let depth = tokio::sync::Mutex::new(None::<Instant>);

    // ── 1. Scoped processing: A's job embeds A's atom under A's scope ────────
    let job_a = queue::enqueue(
        &probe.app_pool,
        &JobKind::EmbedAtoms { atom_ids: vec![atom_a] },
        Some(probe.a.account_id),
        None,
        0,
    )
    .await
    .expect("enqueue A job")
    .expect("job id");
    let tick = embed::tick(&wt, &StubEmbedder, Some(&StubEmbedder), None, None, &depth).await.expect("tick ok");
    assert_eq!(tick.processed, 1);
    let (status, err) = job_state(&probe.owner, job_a).await;
    assert_ne!(status, "done", "an ungated provider is never admitted, so the job is not a success");
    assert!(
        err.unwrap_or_default().contains("all atoms in job failed to embed"),
        "the refusal is recorded on the job"
    );
    // No cloud vector, but model-free keyword indexing still ran under A's
    // scope with A's DEK, and the atom is left retryable (not stamped).
    assert_eq!(
        embed_stamp(&probe.owner, atom_a).await,
        (false, None, true),
        "A's atom keyword-indexed under A's scope without a cloud vector"
    );
    let a_attempts: i32 = sqlx::query_scalar("SELECT embed_attempts FROM atoms WHERE atom_id = $1")
        .bind(atom_a)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(a_attempts, 1, "a refused atom is a retryable failure, not a success");
    assert_eq!(CLOUD_EMBED_CALLS.load(std::sync::atomic::Ordering::SeqCst), 0, "automatic embed job dispatched to an ungated cloud provider");
    let tsv: bool = sqlx::query_scalar("SELECT content_tsv IS NOT NULL FROM atoms WHERE atom_id = $1")
        .bind(atom_a)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert!(tsv, "content_tsv written under A's scope");
    // Downstream jobs carry A's account stamp forward.
    let downstream: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE kind IN ('ExtractEntities','ExtractRelationships') \
         AND account_id = $1",
    )
    .bind(probe.a.account_id)
    .fetch_one(&probe.owner)
    .await
    .unwrap();
    assert_eq!(downstream, 0, "unsupported paid consumers are not minted per atom");

    // ── 2. Cross-account isolation: A-stamped job naming B's atom is REFUSED ─
    let job_forged = queue::enqueue(
        &probe.app_pool,
        &JobKind::EmbedAtoms { atom_ids: vec![atom_b] },
        Some(probe.a.account_id),
        None,
        0,
    )
    .await
    .expect("enqueue forged job")
    .expect("job id");
    let tick2 = embed::tick(&wt, &StubEmbedder, Some(&StubEmbedder), None, None, &depth).await.expect("tick ok");
    assert_eq!(tick2.processed, 1);
    let (status2, err2) = job_state(&probe.owner, job_forged).await;
    assert_ne!(status2, "done", "a cross-account payload must never complete");
    assert!(
        err2.unwrap_or_default().contains("account"),
        "failure must be the cross-account ownership check"
    );
    assert!(
        !atom_has_embedding(&probe.owner, atom_b).await,
        "B's atom must be untouched by A's forged job"
    );
    let b_attempts: i32 = sqlx::query_scalar("SELECT embed_attempts FROM atoms WHERE atom_id = $1")
        .bind(atom_b)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(b_attempts, 0, "B's atom must not even be marked failed by A's job");
    assert_eq!(embed_stamp(&probe.owner, atom_b).await.1, None, "B's atom is not stamped by A's forged job");

    // ── 3. Fail-closed sanity: the worker's table hard-errors unscoped ───────
    probe
        .assert_unscoped_hard_errors("SELECT atom_id FROM atoms WHERE atom_id = $1", &[atom_a])
        .await;

    probe.teardown().await;
}

// ── embed_retry ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn embed_retry_sweep_claims_per_account_under_scope() {
    let Some(probe) = RlsProbe::setup("orgt_w2a_embedretry").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let failed_a = seed_failed_atom(&probe.owner, probe.a.graph_id).await;
    let failed_b = seed_failed_atom(&probe.owner, probe.b.graph_id).await;

    let wt = consumer_tenant(&probe.app_pool);
    let tick = embed_retry::tick(&wt, &Shutdown::new()).await.expect("sweep tick ok");
    assert_eq!(tick.processed, 2, "both accounts' failed atoms claimed (one scoped claim each)");

    // One EmbedAtomsRetry job per account, each stamped with ITS OWN account
    // and naming ONLY that account's atom — a scoped per-account claim cannot
    // mix accounts by construction.
    let jobs: Vec<(Option<Uuid>, serde_json::Value)> = sqlx::query_as(
        "SELECT account_id, payload FROM jobs WHERE kind = 'EmbedAtomsRetry' ORDER BY account_id",
    )
    .fetch_all(&probe.owner)
    .await
    .expect("read retry jobs");
    assert_eq!(jobs.len(), 2, "one retry job per account");
    for (account_id, payload) in &jobs {
        let account_id = account_id.expect("retry job must carry its account stamp");
        let atom_ids: Vec<Uuid> = payload["atom_ids"]
            .as_array()
            .expect("atom_ids array")
            .iter()
            .map(|v| v.as_str().unwrap().parse().unwrap())
            .collect();
        let expected = if account_id == probe.a.account_id {
            failed_a
        } else {
            assert_eq!(account_id, probe.b.account_id, "job stamped with a known account");
            failed_b
        };
        assert_eq!(atom_ids, vec![expected], "job names only its own account's atom");
    }

    // The claim bumped both atoms' backoff anchors (claimed = not re-eligible).
    for atom in [failed_a, failed_b] {
        let recent: bool = sqlx::query_scalar(
            "SELECT embed_failed_at > now() - INTERVAL '60 seconds' FROM atoms WHERE atom_id = $1",
        )
        .bind(atom)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
        assert!(recent, "claimed atom's embed_failed_at was bumped by the scoped claim");
    }

    probe.teardown().await;
}

// ── source atoms v2: relationship edges under the job's scope ─────────────────
//
// Stated edges come from the funded source-atoms v2 call (the per-atom
// relationship worker is retired); these are the scope proofs for them.

const V2_TEXT: &str = "Alice manages Bob on Atlas.";
const V2_STUB_JSON: &str = r#"{"atoms":[{"kind":"fact","text":"Alice manages Bob on Atlas.","evidence_quote":"Alice manages Bob on Atlas.","entities":[{"mention":"Alice","type":"person"},{"mention":"Bob","type":"person"}],"relations":[{"subject":"Alice","predicate":"manages","object":"Bob","valid_from":null,"valid_until":null,"validity_quote":null}],"valid_from":null,"valid_until":null,"validity_quote":null}]}"#;

/// A v2-pinned retained version in `graph`, owned by a running cohort unit.
async fn v2_version(probe: &RlsProbe, account: Uuid, user: Uuid, graph: Uuid) -> Uuid {
    pinned_version(probe, account, user, graph, "source-atoms-v2", V2_TEXT).await
}

/// A retained version pinned to `profile`, owned by a running cohort unit.
async fn pinned_version(probe: &RlsProbe, account: Uuid, user: Uuid, graph: Uuid, profile: &str, text: &str) -> Uuid {
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
        .bind(account).bind(user).execute(&probe.owner).await.unwrap();
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&probe.owner, account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
    sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,100) ON CONFLICT DO NOTHING")
        .bind(account).execute(&mut *financial).await.unwrap();
    financial.commit().await.unwrap();
    let mut tx = probe.scoped_tx(account).await;
    let capture = coralswarm_backend::source_evidence::CaptureSource {
        source_type: "note", provider_namespace: "fixture:w2a", source_id: "w2a-v2", normalized_text: text,
        representation: "original_text", metadata: &serde_json::json!({}), occurred_at: None, participants: &[],
        ingested_by: Some(user), scope: "private", duration_ms: None, segments: None,
    };
    let saved = coralswarm_backend::source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[42; 32])
        .await.unwrap();
    tx.commit().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version=$2 WHERE version_id=$1")
        .bind(saved.version_id).bind(profile).execute(&probe.owner).await.unwrap();
    sqlx::query("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).execute(&probe.owner).await.unwrap();
    let (_, owner_account, _) = support::managed_extraction::own_version(&probe.owner, saved.version_id).await;
    assert_eq!(owner_account, account);
    saved.version_id
}

async fn v2_job(probe: &RlsProbe, stamp: Uuid, graph: Uuid, version: Uuid) -> queue::JobRow {
    let id = queue::enqueue(
        &probe.owner,
        &JobKind::ExtractSourceAtoms { graph_id: graph, source_version_id: version, start_char: 0 },
        Some(stamp),
        None,
        0,
    ).await.unwrap().expect("job id");
    let (kind, payload): (String, serde_json::Value) = sqlx::query_as("SELECT kind,payload FROM jobs WHERE id=$1")
        .bind(id).fetch_one(&probe.owner).await.unwrap();
    queue::JobRow { id, kind, payload, attempts: 0, account_id: Some(stamp) }
}

#[tokio::test]
async fn source_atoms_v2_edges_scoped_processing_and_cross_account_isolation() {
    let Some(probe) = RlsProbe::setup("orgt_w2a_v2edges").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let version_a = v2_version(&probe, probe.a.account_id, probe.a.user_id, probe.a.graph_id).await;
    let version_b = v2_version(&probe, probe.b.account_id, probe.b.user_id, probe.b.graph_id).await;
    let store = PostgresStore::new(probe.app_pool.clone());

    // ── 1. Scoped processing: edges and their oplog ops land in A's graph ───
    let job = v2_job(&probe, probe.a.account_id, probe.a.graph_id, version_a).await;
    coralswarm_backend::workers::source_atoms::process_job(
        &TenantId::Consumer, &probe.app_pool, &probe.owner, &store,
        &coralswarm_backend::llm::testing::GatedScript(&FixedLlm(V2_STUB_JSON)), None, job,
    ).await;
    let edges = count_scalar(&probe.owner, "SELECT COUNT(*) FROM relationship_edge WHERE graph_id = $1", probe.a.graph_id).await;
    assert_eq!(edges, 2, "forward + inverse (reports_to) edge written under A's scope");
    let edge_ops = count_scalar(&probe.owner, "SELECT COUNT(*) FROM op_log WHERE graph_id = $1 AND entity = 'relationship_edge'", probe.a.graph_id).await;
    assert_eq!(edge_ops, 2, "each scoped edge upsert emitted its oplog op");
    for (account, expected) in [(probe.a.account_id, 2i64), (probe.b.account_id, 0)] {
        let mut tx = probe.scoped_tx(account).await;
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge").fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(visible, expected, "RLS: only A's scope sees A's edges");
    }

    // ── 2. Cross-account: an A-stamped job naming B's version is refused
    //       before any model call; B's graph gains no edge ─────────────────
    let forged = v2_job(&probe, probe.a.account_id, probe.b.graph_id, version_b).await;
    let forged_id = forged.id;
    coralswarm_backend::workers::source_atoms::process_job(
        &TenantId::Consumer, &probe.app_pool, &probe.owner, &store,
        &coralswarm_backend::llm::testing::GatedScript(&PanicLlm), None, forged,
    ).await;
    let b_edges = count_scalar(&probe.owner, "SELECT COUNT(*) FROM relationship_edge WHERE graph_id = $1", probe.b.graph_id).await;
    assert_eq!(b_edges, 0, "B's graph untouched by A's forged job");
    let (status, _) = job_state(&probe.owner, forged_id).await;
    assert_ne!(status, "done", "a foreign version never completes under A's stamp");
    probe.teardown().await;
}

// ── source atoms v3: a stated change under the job's scope ───────────────────

const V3_TEXT: &str = "Atlas moved from Bob to Alice.";
const V3_STUB_JSON: &str = r#"{"atoms":[{"kind":"fact","text":"Atlas moved from Bob to Alice.","evidence_quote":"Atlas moved from Bob to Alice.","entities":[{"mention":"Atlas","type":"project"},{"mention":"Bob","type":"person"},{"mention":"Alice","type":"person"}],"relations":[],"changes":[{"subject":"Atlas","attribute":"owner","old_value":"Bob","new_value":"Alice","effective_from":null,"effective_quote":null,"quote":"Atlas moved from Bob to Alice."}],"valid_from":null,"valid_until":null,"validity_quote":null}]}"#;

/// The change, its stated edge and its supersession proposal land in the
/// job's graph only, and RLS shows them only to that account's scope.
#[tokio::test]
async fn source_atoms_v3_stated_change_scoped_processing_and_cross_account_isolation() {
    let Some(probe) = RlsProbe::setup("orgt_w2a_v3change").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let version_a = pinned_version(&probe, probe.a.account_id, probe.a.user_id, probe.a.graph_id, "source-atoms-v3", V3_TEXT).await;
    let version_b = pinned_version(&probe, probe.b.account_id, probe.b.user_id, probe.b.graph_id, "source-atoms-v3", V3_TEXT).await;
    let store = PostgresStore::new(probe.app_pool.clone());
    let job = v2_job(&probe, probe.a.account_id, probe.a.graph_id, version_a).await;
    coralswarm_backend::workers::source_atoms::process_job(
        &TenantId::Consumer, &probe.app_pool, &probe.owner, &store,
        &coralswarm_backend::llm::testing::GatedScript(&FixedLlm(V3_STUB_JSON)), None, job,
    ).await;
    let changes = count_scalar(&probe.owner, "SELECT COUNT(*) FROM semantic_atoms WHERE graph_id = $1 AND stated_changes IS NOT NULL", probe.a.graph_id).await;
    assert_eq!(changes, 1, "the change is recorded on A's atom");
    let edges = count_scalar(&probe.owner, "SELECT COUNT(*) FROM relationship_edge WHERE graph_id = $1 AND predicate IN ('responsible_for','responsibility_of')", probe.a.graph_id).await;
    assert_eq!(edges, 1, "the new owner edge is stated; the old one is not");
    let new_owner: String = sqlx::query_scalar("SELECT s.title FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id WHERE e.graph_id=$1 AND e.predicate='responsible_for'")
        .bind(probe.a.graph_id).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(new_owner, "Alice");
    let proposals = count_scalar(&probe.owner, "SELECT COUNT(*) FROM fact_supersession_edges WHERE graph_id = $1 AND rule = 'stated_change' AND status = 'proposed'", probe.a.graph_id).await;
    assert_eq!(proposals, 1, "dry run: one proposal from the one sentence");
    for (account, expected) in [(probe.a.account_id, 1i64), (probe.b.account_id, 0)] {
        let mut tx = probe.scoped_tx(account).await;
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM fact_supersession_edges").fetch_one(&mut *tx).await.unwrap();
        let stated: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE stated_changes IS NOT NULL").fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!((visible, stated), (expected, expected), "RLS: only A's scope sees A's change and decision");
    }
    let forged = v2_job(&probe, probe.a.account_id, probe.b.graph_id, version_b).await;
    coralswarm_backend::workers::source_atoms::process_job(
        &TenantId::Consumer, &probe.app_pool, &probe.owner, &store,
        &coralswarm_backend::llm::testing::GatedScript(&PanicLlm), None, forged,
    ).await;
    let b_changes = count_scalar(&probe.owner, "SELECT COUNT(*) FROM fact_supersession_edges WHERE graph_id = $1", probe.b.graph_id).await;
    assert_eq!(b_changes, 0, "B's graph untouched by A's forged job");
    probe.teardown().await;
}
