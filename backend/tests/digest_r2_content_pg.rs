//! Regression coverage for the 2026-08-04 R2-cutover defect in
//! `workers::digest` — the same D5 shape PR #569 fixed in the relationship
//! workers, which was never ported to the digest worker.
//!
//! Every atom written after the cutover is R2-only (`content_inline` NULL,
//! `content_blob_key` set). The digest worker loaded coral text with a flat
//! `AND content_inline IS NOT NULL` and dropped the misses, producing two
//! defects that these tests pin from opposite directions:
//!
//!   * **Silent** — a reef mixing pre- and post-cutover corals summarised
//!     only the pre-cutover ones and still reported `done`. Measured in
//!     production on 2026-08-07: 77 reefs, 2,549 corals missing from their
//!     own digests, nothing anywhere reporting it. This is the half a
//!     loader-only "does it error" assertion would MISS, so
//!     `mixed_reef_loads_r2_only_corals_instead_of_dropping_them` asserts
//!     the R2-only content is actually PRESENT and correct, not merely that
//!     no error was returned.
//!   * **Loud** — a reef whose corals are ALL post-cutover selected nothing
//!     and dead-lettered on "no inline coral content" (108 jobs, 8 reefs).
//!
//! The exact loader is tested independently: an unavailable coral refuses
//! the batch, while an empty inline coral contributes nothing. The worker
//! test pins the managed-enrichment boundary (source-agnostic plan PR 8,
//! tenant 0171 / control 0338): RegenerateDigest is admitted pending, and
//! while its switches are off a direct worker invocation hands the reef back
//! before any R2, embedding or provider work. The switched-on managed path
//! (receipts, stale inputs, bounds, deletion invalidation) is proven in
//! `managed::gate::pg_tests::enrichment_consumers`.
//!
//! ## What runs where
//!
//! Three PostgreSQL paths run in normal CI: unavailable-loader refusal,
//! empty-inline loading, and the switched-off worker hand-back. The fourth test,
//! `mixed_reef_loads_r2_only_corals_instead_of_dropping_them`, needs the four
//! explicit R2_TEST_* settings and returns early without them. An `ok` for
//! that optional test without those settings is not a blob round-trip proof.
//!
//! Scratch-DB pattern shared by every `*_pg.rs` file in this directory,
//! gated on `ORGT_TEST_BASE_URL` — same as `digest_selection_pg.rs` /
//! `digest_trigger_pg.rs`.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test digest_r2_content_pg -- --nocapture
//!
//! Add the MinIO test with:
//!
//!   docker run -d -p 19000:9000 --name cs-minio \
//!     -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//!     minio/minio server /data
//!   # then create the bucket, e.g. via `mc mb`
//!
//!   R2_TEST_ENDPOINT_OVERRIDE=http://localhost:19000 \
//!   R2_TEST_ACCESS_KEY=minioadmin R2_TEST_SECRET_KEY=minioadmin \
//!   R2_TEST_BUCKET=cs-test-bucket ... cargo test ...

use coralswarm_backend::jobs::queue::JobRow;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::digest::test_support_load_selected_coral_content;
use coralswarm_core::embed::{EmbedBatchFuture, EmbedProvider};
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::r2::R2Client;
use pgvector::Vector;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

const DIM: usize = 384;

// Digest selection ranks `embedding_next` (migration 0231), not the legacy
// `embedding` column. Both are `vector(384)`. Inserts write the same vector
// into both so a coral is selectable and the R2-load path is actually reached.

// ── Harness ───────────────────────────────────────────────────────────────────

/// A fixed, deterministic KEK so wrap/unwrap round-trips without touching the
/// real `MASTER_KEK_HEX` (same value `r2_extract_success_pg.rs` uses).
///
/// Written exactly once per process. `std::env::set_var` is unsound while
/// another thread reads the environment — which is why it needs `unsafe` —
/// and `load_kek`/`resolve_dek_for_tenant` read `MASTER_KEK_HEX` from the
/// other tests in this binary. `--test-threads=1` (how CI runs this suite)
/// hides that, but a bare `cargo test` locally does not. The value is
/// identical for every caller, so one initialization is all that is needed.
fn set_test_kek() {
    static KEK_ONCE: std::sync::Once = std::sync::Once::new();
    KEK_ONCE.call_once(|| unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) });
}

/// Neither provider may be reached by a switched-off digest pass.
#[derive(Default)]
struct RefusalEmbedder(AtomicUsize);
impl EmbedProvider for RefusalEmbedder {
    fn embed_batch<'a>(
        &'a self,
        _texts: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { panic!("unsupported digest must not batch embed") })
    }
}

#[derive(Default)]
struct RefusalLlm(AtomicUsize);
#[async_trait::async_trait]
impl coralswarm_backend::llm::LlmProvider for RefusalLlm {
    async fn complete(
        &self,
        _req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<coralswarm_backend::llm::CompletionResponse, coralswarm_backend::llm::LlmError>
    {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported digest must not synthesize")
    }
    async fn stream_complete(
        &self,
        _req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<
                            coralswarm_backend::llm::StreamEvent,
                            coralswarm_backend::llm::LlmError,
                        >,
                    > + Send,
            >,
        >,
        coralswarm_backend::llm::LlmError,
    > {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported digest must not stream")
    }
}

/// Full retained input/output and accounting state, excluding only the job
/// receipt whose terminal refusal this test deliberately advances.
async fn retained_state(f: &Fixture, excluded_job: Option<Uuid>) -> serde_json::Value {
    sqlx::query_scalar(r#"SELECT jsonb_build_object(
        'nodes', (SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id) FROM nodes n),
        'atoms', (SELECT jsonb_agg(to_jsonb(a) ORDER BY atom_id) FROM atoms a),
        'links', (SELECT jsonb_agg(to_jsonb(na) ORDER BY node_id,atom_id) FROM node_atoms na),
        'usage', (SELECT jsonb_agg(to_jsonb(u) ORDER BY id) FROM llm_usage u),
        'pending_usage', (SELECT jsonb_agg(to_jsonb(u) ORDER BY id) FROM llm_usage_pending u),
        'budgets', (SELECT jsonb_agg(to_jsonb(b) ORDER BY account_id,purpose,period_start) FROM managed_budget_periods b),
        'attempts', (SELECT jsonb_agg(to_jsonb(a) ORDER BY receipt_id) FROM managed_attempt_receipts a),
        'reservations', (SELECT jsonb_agg(to_jsonb(r) ORDER BY receipt_id) FROM managed_credit_reservations r),
        'balances', (SELECT jsonb_agg(to_jsonb(b) ORDER BY account_id) FROM credit_balances b),
        'ledger', (SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM credit_ledger l),
        'other_jobs', (SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM jobs j WHERE $1::uuid IS NULL OR id<>$1)
    )"#).bind(excluded_job).fetch_one(&f.pool).await.expect("snapshot retained state")
}

struct Fixture {
    pool: PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    dek: [u8; 32],
}

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// A user + account (carrying a real wrapped DEK, so `resolve_dek_for_tenant`
/// succeeds) + a graph, in a freshly migrated scratch database named for the
/// calling test — same pattern as `digest_selection_pg.rs`, so this suite
/// self-migrates and can be wired into any Postgres-service CI job without
/// depending on that job's schema setup.
async fn fixture(base: &str, db: &str) -> Fixture {
    set_test_kek();
    let kek = load_kek().expect("test KEK loads");
    let dek = generate_dek();
    let wrapped = wrap_dek(&kek, &dek).expect("wrap test DEK");

    let mut admin = PgConnection::connect(base).await.expect("connect admin");
    admin
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .expect("drop scratch db");
    admin
        .execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .expect("create scratch db");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(base, db))
        .await
        .expect("connect to scratch db");
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .expect("migrate scratch db");

    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("digest-r2-{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(&wrapped)
    .fetch_one(&pool)
    .await
    .expect("insert account with wrapped DEK");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &format!("digest-r2-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");

    Fixture { pool, account_id, graph_id: graph.id, dek }
}

/// A PRE-cutover coral: content lives in `content_inline`.
async fn insert_inline_atom(f: &Fixture, source: &str, content: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_inline, content_blob_key, \
         timestamp, embedding, embedding_next) VALUES ($1, 'slack', $2, $3, $4, now(), $5, $5) \
         RETURNING atom_id",
    )
    .bind(f.graph_id)
    .bind(source)
    .bind(content)
    .bind(format!("blob-{source}"))
    .bind(Vector::from(vec![0.1_f32; DIM]))
    .fetch_one(&f.pool)
    .await
    .expect("insert inline atom")
}

/// A POST-cutover coral: `content_inline` NULL, content reachable only at
/// `blob_key`. This is the shape every atom written since 2026-08-04 has.
async fn insert_r2_only_atom(f: &Fixture, source: &str, blob_key: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp, \
         embedding, embedding_next) VALUES ($1, 'slack', $2, $3, now(), $4, $4) RETURNING atom_id",
    )
    .bind(f.graph_id)
    .bind(source)
    .bind(blob_key)
    .bind(Vector::from(vec![0.1_f32; DIM]))
    .fetch_one(&f.pool)
    .await
    .expect("insert R2-only atom")
}

const DB_SKIP: &str =
    "skipping: set ORGT_TEST_BASE_URL (a Postgres this test may CREATE DATABASE against) to run \
     the digest R2-cutover regression";

// ── The silent half: R2-only corals must be LOADED, not dropped ──────────────

/// The defect that never surfaced: a reef mixing pre- and post-cutover corals
/// silently digested only the pre-cutover ones.
///
/// Asserts the R2-only corals' actual TEXT comes back — not merely that the
/// call returned `Ok`. Before the fix this returned `Ok` with only the inline
/// corals in it, which is exactly why nothing caught it for three days.
#[tokio::test]
async fn mixed_reef_loads_r2_only_corals_instead_of_dropping_them() {
    let Some(base) = base_url() else {
        eprintln!("{DB_SKIP}");
        return;
    };
    let (Ok(endpoint), Ok(access), Ok(secret), Ok(bucket)) = (
        std::env::var("R2_TEST_ENDPOINT_OVERRIDE"),
        std::env::var("R2_TEST_ACCESS_KEY"),
        std::env::var("R2_TEST_SECRET_KEY"),
        std::env::var("R2_TEST_BUCKET"),
    ) else {
        // Expected in CI. The regression itself is still gated there by
        // `unresolvable_coral_refuses_the_batch_instead_of_returning_a_subset`,
        // which fails against the pre-fix loader without needing any R2 —
        // this test adds the positive round-trip on top. See the module doc.
        eprintln!(
            "skipping mixed_reef_loads_r2_only_corals_instead_of_dropping_them: needs a local \
             S3-compatible fake (R2_TEST_*; see this file's module doc)"
        );
        return;
    };

    let f = fixture(&base, "orgt_digestr2_mixed_ctl").await;
    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &access,
        &secret,
        &bucket,
        &endpoint,
    )
    .await;

    let inline_id = insert_inline_atom(&f, "pre-cutover-1", "PRE-CUTOVER CORAL TEXT").await;

    // A genuine PUT, so the loader's GET is a real round-trip.
    let r2_atom_id = Uuid::new_v4();
    let plaintext = b"POST-CUTOVER CORAL TEXT that only exists in R2";
    let blob_key = r2
        .put_atom_blob(None, f.graph_id, r2_atom_id, plaintext, &f.dek)
        .await
        .expect("put_atom_blob against the local R2 fake must succeed");
    let r2_only_id = insert_r2_only_atom(&f, "post-cutover-1", &blob_key).await;

    let loaded = test_support_load_selected_coral_content(
        &TenantId::Consumer,
        &f.pool,
        Some(&r2),
        f.account_id,
        &[inline_id, r2_only_id],
    )
    .await
    .expect("a mixed reef must load, not error");

    assert_eq!(
        loaded.len(),
        2,
        "both corals must be loaded; before the fix the R2-only one was silently dropped and this \
         was 1"
    );
    assert_eq!(
        loaded.get(&inline_id).map(String::as_str),
        Some("PRE-CUTOVER CORAL TEXT"),
        "the inline coral must be unaffected by the R2 path"
    );
    assert_eq!(
        loaded.get(&r2_only_id).map(String::as_str),
        Some("POST-CUTOVER CORAL TEXT that only exists in R2"),
        "the R2-only coral's real blob content must reach the digest"
    );
}

// ── The loud half: an unresolvable coral must fail, not degrade ──────────────

/// With R2 unconfigured, an R2-only coral cannot be resolved. The loader must
/// refuse the whole batch rather than hand back the inline subset — a digest
/// built from a subset is indistinguishable from a complete one downstream.
#[tokio::test]
async fn unresolvable_coral_refuses_the_batch_instead_of_returning_a_subset() {
    let Some(base) = base_url() else {
        eprintln!("{DB_SKIP}");
        return;
    };
    let f = fixture(&base, "orgt_digestr2_subset_ctl").await;

    let inline_id = insert_inline_atom(&f, "pre-cutover-2", "PRE-CUTOVER CORAL TEXT").await;
    let r2_only_id = insert_r2_only_atom(&f, "post-cutover-2", "missing/blob/key").await;

    let err = test_support_load_selected_coral_content(
        &TenantId::Consumer,
        &f.pool,
        None, // R2 unconfigured — the R2-only coral is unreachable
        f.account_id,
        &[inline_id, r2_only_id],
    )
    .await
    .expect_err("an unresolvable coral must fail the batch, never degrade to the inline subset");
    assert!(
        err.contains("R2 not configured"),
        "the error must name why the coral could not be loaded, got: {err}"
    );

    // Negative control for the assertion above: the SAME call with only the
    // inline coral must succeed. Without this, the test would pass just as
    // well against a loader that errors unconditionally.
    let ok = test_support_load_selected_coral_content(
        &TenantId::Consumer,
        &f.pool,
        None,
        f.account_id,
        &[inline_id],
    )
    .await
    .expect("an all-inline reef must still load with R2 unconfigured");
    assert_eq!(
        ok.len(),
        1,
        "pre-cutover reefs must keep working exactly as before the fix"
    );
}

/// An atom with `content_inline = ''` must be skipped, NOT fetched from R2.
///
/// Review finding on this PR (CodeRabbit, Major). Pre-cutover atoms carry a
/// `content_blob_key` that was never uploaded — R2 was not configured before
/// the cutover, which is why `routes::queries` logs "blob not found" against
/// those same keys today. If an empty-inline atom were routed down the R2
/// path, that unfetchable key would fail the batch and dead-letter the reef's
/// digest permanently: a NEW failure mode on pre-cutover data, introduced by
/// a fix aimed at post-cutover data.
///
/// Production currently holds zero such rows (6,591 with content, 693 NULL,
/// none empty), so this pins a shape that is one ingest-edge-case away rather
/// than one in flight. The reef must still digest from its other corals.
#[tokio::test]
async fn empty_inline_coral_is_skipped_not_fetched_from_r2() {
    let Some(base) = base_url() else {
        eprintln!("{DB_SKIP}");
        return;
    };
    let f = fixture(&base, "orgt_digestr2_empty_ctl").await;

    let good_id = insert_inline_atom(&f, "pre-cutover-3", "REAL CORAL TEXT").await;
    // Empty content plus a blob key that resolves to nothing — exactly the
    // pre-cutover shape. R2 is unconfigured here, so ANY attempt to treat
    // this as R2-only would return Err and fail the whole batch.
    let empty_id = insert_inline_atom(&f, "pre-cutover-4", "").await;

    let loaded = test_support_load_selected_coral_content(
        &TenantId::Consumer,
        &f.pool,
        None,
        f.account_id,
        &[good_id, empty_id],
    )
    .await
    .expect("an empty-inline coral must not be treated as R2-only");

    assert_eq!(
        loaded.get(&good_id).map(String::as_str),
        Some("REAL CORAL TEXT"),
        "the reef must still digest from its remaining corals"
    );
    assert!(
        !loaded.contains_key(&empty_id),
        "an empty coral contributes nothing and must simply be absent"
    );
}

/// Dispatch contract: an admitted digest pass whose switches are off is
/// handed back before private input/provider work, never marked done with a
/// digest and never dead-lettered. The exact loader below still proves
/// missing R2 is an error rather than an empty/partial successful batch.
#[tokio::test]
async fn switched_off_digest_with_unfetchable_r2_corals_is_handed_back_without_provider_work() {
    let Some(base) = base_url() else {
        eprintln!("{DB_SKIP}");
        return;
    };
    let f = fixture(&base, "orgt_digestr2_e2e_ctl").await;
    let store = PostgresStore::new(f.pool.clone());

    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', 'R2 Cutover Reef') \
         RETURNING node_id",
    )
    .bind(f.graph_id)
    .fetch_one(&f.pool)
    .await
    .expect("insert reef");

    // Retain the eight-corals historical digest-eligible shape. The
    // switched-off hand-back must happen before attempting any blob read.
    let mut atom_ids = Vec::new();
    for i in 0..8 {
        let atom_id =
            insert_r2_only_atom(&f, &format!("post-cutover-e2e-{i}"), "missing/blob/key").await;
        atom_ids.push(atom_id);
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(&f.pool)
            .await
            .expect("link node_atoms");
    }

    assert_eq!(atom_ids.len(), 8);
    let err = test_support_load_selected_coral_content(
        &TenantId::Consumer,
        &f.pool,
        None,
        f.account_id,
        &atom_ids,
    )
    .await
    .expect_err("all eight R2-only corals are genuinely unavailable");
    assert!(err.contains("R2 not configured"), "{err}");
    let before = retained_state(&f, None).await;

    let job_id: Uuid = sqlx::query_scalar(
        "INSERT INTO jobs (kind, payload, status, account_id, started_at) \
         VALUES ('RegenerateDigest', $1, 'running', $2, now()) RETURNING id",
    )
    .bind(json!({"node_id": node_id, "graph_id": f.graph_id}))
    .bind(f.account_id)
    .fetch_one(&f.pool)
    .await
    .expect("insert job");

    let admitted: (String, Option<String>, i32, bool) = sqlx::query_as(
        "SELECT status::text,error_message,attempts,finished_at IS NOT NULL FROM jobs WHERE id=$1",
    )
    .bind(job_id)
    .fetch_one(&f.pool)
    .await
    .expect("read admission");
    assert_eq!(admitted, ("running".into(), None, 0, false), "admitted on managed_enrichment, not dead-lettered");
    std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
    std::env::remove_var("DIGESTS_ENABLED");
    let embedder = RefusalEmbedder::default();
    let llm = RefusalLlm::default();

    let job = JobRow {
        id: job_id,
        kind: "RegenerateDigest".to_string(),
        payload: json!({"node_id": node_id, "graph_id": f.graph_id}),
        attempts: 0,
        account_id: Some(f.account_id),
    };

    // A direct invocation must not bypass the switches.
    coralswarm_backend::workers::digest::process_job(
        &TenantId::Consumer,
        &f.pool,
        &f.pool,
        &store,
        &embedder,
        &llm,
        None, // R2 unconfigured — every coral is unreachable
        job,
    )
    .await;

    let row = sqlx::query("SELECT status::text AS status,error_message,outcome::text AS outcome FROM jobs WHERE id=$1")
        .bind(job_id)
        .fetch_one(&f.pool)
        .await
        .expect("read job back");
    // Switched off: the claimed pass is held in place under the shared retry
    // rule (`jobs::queue::hold`), never a completion, so never a DidWork one
    // when nothing could be loaded (that silent success is #569's defect).
    let status: String = row.get("status");
    assert_eq!(status, "pending", "switched off: the pass is handed back in place");
    assert_eq!(row.get::<Option<String>, _>("error_message").as_deref(), Some("held: switched_off"));
    assert_eq!(row.get::<Option<String>, _>("outcome"), None);
    let handed_back: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='RegenerateDigest' AND status='pending' AND idempotency_key=$1",
    )
    .bind(format!("digest:{node_id}"))
    .fetch_one(&f.pool)
    .await
    .expect("read hand-back");
    assert_eq!(handed_back, 1, "the reef is handed back for a later pass, as one pending row");
    assert_eq!(embedder.0.load(Ordering::SeqCst), 0);
    assert_eq!(llm.0.load(Ordering::SeqCst), 0);
    assert_eq!(retained_state(&f, Some(job_id)).await, before);

    // Digest must be untouched — no partial write on the failure path.
    let digest: Option<String> =
        sqlx::query_scalar("SELECT body->'digest'->>'summary' FROM nodes WHERE node_id = $1")
            .bind(node_id)
            .fetch_one(&f.pool)
            .await
            .expect("read reef digest");
    assert!(
        digest.is_none(),
        "no digest may be written when the corals behind it could not be loaded"
    );
}
