//! `workers::atom_title_backfill` against a REAL R2-only atom (title-always-
//! present PR, 2026-09-09).
//!
//! `atom_title_backfill_pg.rs` proves the sweep's RLS scoping and re-run
//! safety against `content_inline` atoms — every atom that file seeds carries
//! inline content, so it never actually exercises the R2 fetch branch this PR
//! added. This file closes that gap: it seeds an atom with `content_inline =
//! NULL` and a REAL `content_blob_key` (put through an actual R2-shaped PUT
//! against a local MinIO fake, same fixture `r2_extract_success_pg.rs`
//! pioneered for `workers::extract`), and proves three things a purely
//! candidate-SQL unit test (`atom_title_backfill::tests::
//! candidate_sql_admits_both_null_and_fallback_titles_and_both_storage_backends`)
//! cannot:
//!
//!   1. **The widened candidate predicate actually reaches a blob-only atom**
//!      — `content_inline IS NULL` no longer means "not a candidate".
//!   2. **The sweep can fetch and decrypt it** — DEK resolution +
//!      `R2Client::get_atom_blob` through the real
//!      `workers::extract::fetch_r2_only_content` path this worker reuses.
//!   3. **`title_kind` is stamped correctly** — `'llm'` when a model answers,
//!      `'fallback'` when it doesn't — so a later pass can tell an
//!      ingest-synchronous fallback from an already-upgraded LLM title.
//!
//! The sweep runs on `managed_enrichment` (source-agnostic plan PR 7): the
//! model pass is one gated call for the batch, so account A is funded, the
//! shared switch is on and the job is a real running queue row.
//!
//! # Requirements (both must be set, or this file skips loudly)
//! - `ORGT_TEST_BASE_URL` — a base Postgres connection with CREATE DATABASE +
//!   CREATE/ALTER ROLE rights (same convention as `atom_title_backfill_pg.rs`'s
//!   `RlsProbe`).
//! - `R2_TEST_ENDPOINT_OVERRIDE` — a local S3-compatible fake's endpoint (e.g.
//!   MinIO: `http://localhost:19000`), plus `R2_TEST_ACCESS_KEY`,
//!   `R2_TEST_SECRET_KEY`, and `R2_TEST_BUCKET` (must already exist) — see
//!   `r2_extract_success_pg.rs`'s module doc for the exact docker commands to
//!   stand one up.

mod support;

use coralswarm_backend::jobs::queue::JobRow;
use coralswarm_backend::llm::{
    CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent,
};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::atom_title_backfill;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::r2::R2Client;
use futures::Stream;
use sqlx::Row;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// Mirrors `r2_extract_success_pg.rs::set_test_kek` exactly.
fn set_test_kek() {
    unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) };
}

struct R2TestEnv {
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
}

fn r2_test_env() -> Option<R2TestEnv> {
    Some(R2TestEnv {
        endpoint: std::env::var("R2_TEST_ENDPOINT_OVERRIDE").ok()?,
        access_key: std::env::var("R2_TEST_ACCESS_KEY").ok()?,
        secret_key: std::env::var("R2_TEST_SECRET_KEY").ok()?,
        bucket: std::env::var("R2_TEST_BUCKET").ok()?,
    })
}

const SKIP_REASON: &str =
    "skipping: set ORGT_TEST_BASE_URL (a base Postgres with CREATE DATABASE rights) AND \
     R2_TEST_ENDPOINT_OVERRIDE / R2_TEST_ACCESS_KEY / R2_TEST_SECRET_KEY / R2_TEST_BUCKET (a \
     local S3-compatible fake, e.g. MinIO — see r2_extract_success_pg.rs's module doc for the \
     docker commands) to run the R2-only atom-title sweep regression";

struct FixedLlm(&'static str);

#[async_trait::async_trait]
impl LlmProvider for FixedLlm {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let sent = req.messages.first().map(|m| m.content.clone()).unwrap_or_default();
        assert!(
            sent.contains("R2-only body"),
            "the model must see the DECRYPTED R2 body, not a placeholder; got: {sent:?}"
        );
        Ok(CompletionResponse {
            // One entry per numbered coral in the batch ("[k]\n<body>"); only
            // the R2-only coral gets the fixed headline.
            content: serde_json::json!({"titles": sent
                .split("\n\n[")
                .enumerate()
                .map(|(i, part)| serde_json::json!({
                    "index": i + 1,
                    "title": if part.contains("R2-only body") { self.0 } else { "none" },
                }))
                .collect::<Vec<_>>()})
            .to_string(),
            input_tokens: 10,
            output_tokens: 5,
            model: req.model.clone(),
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
        std::pin::Pin<Box<dyn Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        unimplemented!("not needed in this test")
    }
}

/// A real claimed child job: the managed gate fences every attempt on the
/// queue row `(id, kind, attempts)` while it is `running`.
async fn child_job(owner: &sqlx::PgPool, account_id: Uuid) -> JobRow {
    let payload = serde_json::json!({ "after_atom_id": null });
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO jobs(kind,payload,account_id,status,attempts,started_at) VALUES('BackfillAtomTitles',$1,$2,'running',1,now()) RETURNING id",
    )
    .bind(&payload)
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("seed a running child job");
    JobRow { id, kind: "BackfillAtomTitles".to_string(), payload, account_id: Some(account_id), attempts: 1 }
}

/// Fund account A so the managed gate can reserve: an owner seat, credits,
/// and the serving-identity lifecycle contract every managed call checks
/// (`corpus_enrichment::lifecycle_ready_tx`), installed the way
/// `standing_execution_pg.rs` does.
async fn fund(probe: &RlsProbe) {
    let mut conn = probe.owner.acquire().await.unwrap();
    sqlx::query("SET app.identity_serving_contract='composite-v1'").execute(&mut *conn).await.unwrap();
    sqlx::raw_sql(include_str!("../schema/identity-contract.sql")).execute(&mut *conn).await.unwrap();
    drop(conn);
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
        .bind(probe.a.account_id).bind(probe.a.user_id).execute(&probe.owner).await.unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&probe.owner, probe.a.account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, probe.a.account_id).await.unwrap();
    sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,100) ON CONFLICT(account_id) DO UPDATE SET balance=excluded.balance")
        .bind(probe.a.account_id).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}

async fn title_and_kind(owner: &sqlx::PgPool, atom_id: Uuid) -> (Option<String>, Option<String>) {
    let row = sqlx::query("SELECT title, title_kind FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(owner)
        .await
        .expect("read title/title_kind");
    (row.get::<Option<String>, _>("title"), row.get::<Option<String>, _>("title_kind"))
}

#[tokio::test]
async fn sweep_fetches_titles_and_stamps_kind_for_a_real_r2_only_atom() {
    let Some(probe) = RlsProbe::setup("orgt_atom_title_r2").await else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    set_test_kek();
    std::env::set_var("ENRICHMENT_PROCESSING_ENABLED", "true");
    fund(&probe).await;
    let kek = load_kek().expect("test KEK loads");
    let dek = generate_dek();
    let wrapped = wrap_dek(&kek, &dek).expect("wrap test DEK");

    // Give account A's row a real wrapped DEK — `RlsProbe::setup` seeds
    // `accounts(owner_user_id)` with no DEK at all, since the RLS probe's own
    // proofs never need one.
    sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
        .bind(&wrapped)
        .bind(probe.a.account_id)
        .execute(&probe.owner)
        .await
        .expect("stamp account A with a wrapped DEK");

    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    // Seed an R2-only atom: `content_inline` stays NULL, `content_blob_key`
    // is a REAL key an actual PUT went through — the exact shape prod atoms
    // have taken since the 2026-08-04 R2 cutover (0 of 13,367 atoms in the
    // verified tenant carry inline content).
    let atom_id = Uuid::new_v4();
    let plaintext = b"R2-only body: shipped the title-always-present PR end to end.";
    let blob_key = r2
        .put_atom_blob(None, probe.a.graph_id, atom_id, plaintext, &dek)
        .await
        .expect("put_atom_blob against the local R2 fake must succeed");
    sqlx::query(
        "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, $2, 'note', 'r2-only-note', $3, now())",
    )
    .bind(atom_id)
    .bind(probe.a.graph_id)
    .bind(&blob_key)
    .execute(&probe.owner)
    .await
    .expect("seed R2-only atom");

    // ── Pass 1: a real model answers ⇒ title_kind = 'llm' ────────────────────
    let model = FixedLlm("Shipped the title-always-present PR end to end");
    let llm = coralswarm_backend::llm::testing::GatedScript(&model);
    atom_title_backfill::process_job(
        &TenantId::Consumer,
        &probe.app_pool,
        &probe.app_pool,
        Some(&llm),
        Some(&r2),
        child_job(&probe.owner, probe.a.account_id).await,
    )
    .await;
    let receipts: Vec<String> = sqlx::query_scalar("SELECT purpose FROM managed_attempt_receipts WHERE account_id=$1")
        .bind(probe.a.account_id).fetch_all(&probe.owner).await.unwrap();
    assert_eq!(receipts, vec!["managed_enrichment".to_string()], "one metered call for the batch");

    let (title, kind) = title_and_kind(&probe.owner, atom_id).await;
    assert_eq!(
        title.as_deref(),
        Some("Shipped the title-always-present PR end to end"),
        "the sweep must fetch, decrypt, and title a real R2-only atom"
    );
    assert_eq!(kind.as_deref(), Some("llm"), "a model-produced title must be stamped 'llm'");

    // ── Pass 2 (separate atom): no LLM at all ⇒ title_kind = 'fallback',
    //    and the row stays a CANDIDATE for a later upgrade ────────────────────
    let atom_id_2 = Uuid::new_v4();
    let plaintext_2 =
        b"R2-only body: second atom, titled deterministically with no model configured.";
    let blob_key_2 = r2
        .put_atom_blob(None, probe.a.graph_id, atom_id_2, plaintext_2, &dek)
        .await
        .expect("put_atom_blob for the second atom must succeed");
    sqlx::query(
        "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, $2, 'note', 'r2-only-note-2', $3, now())",
    )
    .bind(atom_id_2)
    .bind(probe.a.graph_id)
    .bind(&blob_key_2)
    .execute(&probe.owner)
    .await
    .expect("seed second R2-only atom");

    atom_title_backfill::process_job(
        &TenantId::Consumer,
        &probe.app_pool,
        &probe.app_pool,
        None,
        Some(&r2),
        child_job(&probe.owner, probe.a.account_id).await,
    )
    .await;

    let (title_2, kind_2) = title_and_kind(&probe.owner, atom_id_2).await;
    assert!(
        title_2.as_deref().is_some_and(|t| t.starts_with("R2-only body: second atom")),
        "the deterministic fallback must still title an R2-only atom with no LLM configured; \
         got {title_2:?}"
    );
    assert_eq!(
        kind_2.as_deref(),
        Some("fallback"),
        "a fallback-produced title must be stamped 'fallback' so a later pass can upgrade it"
    );

    std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
    // Cleanup: leave no objects behind in the shared test bucket.
    r2.delete_atom_blob(&blob_key).await.expect("cleanup: delete first test blob");
    r2.delete_atom_blob(&blob_key_2).await.expect("cleanup: delete second test blob");

    probe.teardown().await;
}
