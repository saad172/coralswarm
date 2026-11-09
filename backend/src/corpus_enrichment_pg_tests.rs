//! Deterministic disposable PG controller/worker proofs with a mock model.
use super::*;
use crate::jobs::queue::JobRow;
use crate::llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent};
use crate::{
    source_evidence::{self, CaptureSource},
    store::PostgresStore,
    tenancy::{scope::begin_account_scoped, TenantId},
    workers::source_atoms,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::{
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Fixture {
    admin: PgPool,
    owner: PgPool,
    app: PgPool,
    db: crate::test_db::TestDatabase,
    user: Uuid,
    account: Uuid,
    graph: Uuid,
}
impl Fixture {
    async fn new() -> Self {
        let base = std::env::var("ORGT_TEST_BASE_URL")
            .expect("disposable ORGT_TEST_BASE_URL required; tests never skip");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
        let admin = PgPoolOptions::new()
            .connect_with(opts.clone())
            .await
            .unwrap();
        let db = crate::test_db::TestDatabase::new(&opts, "corpus_p3");
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&admin)
            .await
            .unwrap();
        let owner = PgPoolOptions::new()
            .max_connections(6)
            .connect_with(opts.clone().database(&db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&owner)
            .await
            .unwrap();
        // SET ROLE exercises the real app policy without modifying a cluster
        // password used concurrently by the other disposable test suites.
        let app = PgPoolOptions::new()
            .max_connections(6)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET ROLE coralswarm_app").execute(conn).await?;
                    Ok(())
                })
            })
            .connect_with(opts.database(&db))
            .await
            .unwrap();
        let user: Uuid =
            sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$1) RETURNING id")
                .bind(format!("p3-{}@test.local", Uuid::new_v4()))
                .fetch_one(&owner)
                .await
                .unwrap();
        let account: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
                .bind(user)
                .fetch_one(&owner)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
            .bind(account)
            .bind(user)
            .execute(&owner)
            .await
            .unwrap();
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id,name) VALUES($1,'synthetic fixture') RETURNING id",
        )
        .bind(account)
        .fetch_one(&owner)
        .await
        .unwrap();
        Self {
            admin,
            owner,
            app,
            db,
            user,
            account,
            graph,
        }
    }
    async fn finish(self) {
        self.app.close().await;
        self.owner.close().await;
        sqlx::query(&format!("DROP DATABASE {} WITH (FORCE)", self.db))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
    async fn capture(&self, id: &str, text: &str) -> Uuid {
        self.capture_as(id, text, "original_text").await
    }
    async fn capture_as(&self, id: &str, text: &str, representation: &str) -> Uuid {
        let mut tx = begin_account_scoped(&self.app, self.account).await.unwrap();
        let capture = CaptureSource {
            source_type: "note",
            provider_namespace: "fixture:trusted",
            source_id: id,
            normalized_text: text,
            representation,
            metadata: &json!({}),
            occurred_at: None,
            participants: &[],
            ingested_by: Some(self.user),
            scope: "private",
            duration_ms: None,
            segments: None,
        };
        let saved = source_evidence::capture_source_version_tx(
            &mut tx,
            None,
            self.graph,
            self.account,
            &capture,
            None,
            &[42; 32],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        saved.version_id
    }
    async fn run(&self, execute: bool, calls: i64) -> Uuid {
        self.run_kind(execute,calls,"real").await
    }
    async fn run_kind(&self,execute:bool,calls:i64,corpus:&str)->Uuid {
        sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,status,extraction_version,model,max_items,max_characters,max_model_calls,local_holdings_declared,local_inventory_reason,corpus_kind) VALUES($1,$2,$3,$4,$5,'source-atoms-v2','deepseek/deepseek-v4-flash-0731',2,50000,$6,true,'device_key_read_timeout_15s_no_retry_or_upload',$7) RETURNING run_id")
            .bind(self.graph).bind(self.account).bind(self.user).bind(if execute{"execute"}else{"dry_run"}).bind(if execute{"running"}else{"inventory"}).bind(calls).bind(corpus).fetch_one(&self.owner).await.unwrap()
    }
    async fn advance(&self, id: Uuid, limit: i64) {
        let mut tx = begin_account_scoped(&self.app, self.account).await.unwrap();
        let run: Run =
            sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE run_id=$1 FOR UPDATE")
                .bind(id)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        advance_tx(&mut tx, &run, limit).await.unwrap();
        tx.commit().await.unwrap();
    }
    async fn contract(&self) {
        // This is the disposable identity-contract transition; the real
        // operator script separately requires all-serving binary attestation.
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT to_regclass('nodes_graph_type_lower_title_identity_unique') IS NOT NULL"
        )
        .fetch_one(&self.owner)
        .await
        .unwrap());
        let mut connection = self.owner.acquire().await.unwrap();
        sqlx::query("SET app.identity_serving_contract='composite-v1'")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../schema/identity-contract.sql"
        ))
        .execute(&mut *connection)
        .await
        .unwrap();
    }
    async fn job(&self, version: Uuid, start: i64) -> JobRow {
        let row:(Uuid,String,Value)=sqlx::query_as("UPDATE jobs SET status='running' WHERE id=(SELECT id FROM jobs WHERE payload->>'source_version_id'=$1 AND (payload->>'start_char')::bigint=$2 AND status='pending' ORDER BY created_at DESC LIMIT 1) RETURNING id,kind,payload").bind(version.to_string()).bind(start).fetch_one(&self.owner).await.unwrap();
        JobRow {
            id: row.0,
            kind: row.1,
            payload: row.2,
            attempts: 0,
            account_id: Some(self.account),
        }
    }
    // Execution counters are not money. Only positive-dispatch cases seed a
    // finite allowance; every negative fixture keeps its original prerequisites.
    async fn fund_execution(&self) -> Uuid {
        let mut tx = begin_account_scoped(&self.owner, self.account).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx, self.account).await.unwrap();
        let allowance: Uuid = sqlx::query_scalar(
            "INSERT INTO member_allowance_periods(account_id,user_id,period_start,period_end,granted_credits) VALUES($1,$2,now()-interval '1 day',now()+interval '1 day',10) RETURNING id")
            .bind(self.account).bind(self.user).fetch_one(&mut *tx).await.unwrap();
        let actual: (Uuid, Uuid, sqlx::types::Decimal, bool) = sqlx::query_as(
            "SELECT account_id,user_id,granted_credits,period_start<=clock_timestamp() AND period_end>clock_timestamp() FROM member_allowance_periods WHERE id=$1")
            .bind(allowance).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(actual, (self.account, self.user, sqlx::types::Decimal::from(10), true));
        tx.commit().await.unwrap();
        allowance
    }
    async fn assert_funded_attempts(&self, run: Uuid, version: Uuid, allowance: Uuid, count: i64, state: &str) {
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM managed_attempt_receipts WHERE source_version_id=$1")
            .bind(version).fetch_one(&self.owner).await.unwrap();
        assert_eq!(total, count, "every actual send must have exactly one attempt receipt");
        let receipts: Vec<Value> = sqlx::query_scalar(
            "SELECT jsonb_build_object('account',r.account_id,'graph',r.graph_id,'actor',r.attributed_user_id,'run',u.run_id,'job_account',j.account_id,'job_kind',j.kind,'state',r.state,'phase',r.dispatch_phase,'protocol',r.protocol_version,'plane',r.content_plane,'funding_account',f.account_id,'payer',f.payer_id,'member_period',f.member_period_id,'basis',f.funding_basis,'financial_state',f.state,'positive_bound',f.bound_credits>0 AND f.bound_micro_usd>0 AND f.reference_cost_per_credit>0,'same_binding',f.logical_call=r.logical_call AND f.request_hash=r.request_hash AND f.graph_id=r.graph_id AND f.bound_micro_usd=r.reserved_micro_usd,'same_settlement',f.settled_micro_usd=r.settled_micro_usd,'settled_micro_usd',r.settled_micro_usd) FROM managed_attempt_receipts r JOIN managed_credit_reservations f ON f.receipt_id=r.receipt_id AND f.plane=r.content_plane JOIN corpus_enrichment_units u ON u.unit_id=r.unit_id JOIN jobs j ON j.id=r.job_id WHERE r.source_version_id=$1")
            .bind(version).fetch_all(&self.owner).await.unwrap();
        assert_eq!(receipts.len() as i64, count, "a missing financial/unit/job binding cannot disappear in a JOIN");
        for receipt in receipts {
            for field in ["account", "job_account", "funding_account"] {
                assert_eq!(receipt[field], json!(self.account), "{receipt}");
            }
            assert_eq!(receipt["graph"], json!(self.graph), "{receipt}");
            assert_eq!(receipt["actor"], json!(self.user), "{receipt}");
            assert_eq!(receipt["payer"], json!(self.user), "{receipt}");
            assert_eq!(receipt["run"], json!(run), "{receipt}");
            assert_eq!(receipt["member_period"], json!(allowance), "{receipt}");
            assert_eq!(receipt["job_kind"], "ExtractSourceAtoms", "{receipt}");
            assert_eq!(receipt["state"], state, "{receipt}");
            assert_eq!(receipt["phase"], "armed", "{receipt}");
            assert_eq!(receipt["protocol"], 1, "{receipt}");
            assert_eq!(receipt["plane"], "consumer", "{receipt}");
            assert_eq!(receipt["basis"], "allowance", "{receipt}");
            assert_eq!(receipt["financial_state"], "settled", "{receipt}");
            for field in ["positive_bound", "same_binding", "same_settlement"] {
                assert_eq!(receipt[field], true, "{receipt}");
            }
            let settled = receipt["settled_micro_usd"].as_i64().expect("exact terminal settlement");
            if state == "not_generated" { assert_eq!(settled, 0, "{receipt}"); }
            else { assert!(settled > 0, "a completed fake call still has real financial accounting: {receipt}"); }
        }
        let held: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_credit_reservations WHERE account_id=$1 AND state='held'")
            .bind(self.account).fetch_one(&self.owner).await.unwrap();
        assert_eq!(held, 0, "completed/failed calls must not leak a held financial reservation");
    }
    async fn extract(&self, model: &Model, job: JobRow) {
        source_atoms::process_job(
            &TenantId::Consumer,
            &self.app,
            &self.owner,
            &PostgresStore::new(self.app.clone()),
            &crate::llm::testing::GatedScript(model),
            None,
            job,
        )
        .await;
    }
}
struct Model {
    calls: AtomicUsize,
    owner: PgPool,
    pause: Option<Uuid>,
    fail: bool,
    revoke_requester: Option<(Uuid, Uuid)>,
}
#[async_trait::async_trait]
impl LlmProvider for Model {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        assert_eq!(req.model, "deepseek/deepseek-v4-flash-0731");
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(run) = self.pause {
            sqlx::query("UPDATE corpus_enrichment_runs SET status='paused',pause_reason='test_pause_during_model' WHERE run_id=$1").bind(run).execute(&self.owner).await.unwrap();
        }
        if let Some((account, user)) = self.revoke_requester {
            sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
                .bind(account)
                .bind(user)
                .execute(&self.owner)
                .await
                .unwrap();
        }
        if self.fail {
            return Err(LlmError::Upstream {
                status: 503,
                body: "synthetic failure".into(),
            });
        }
        let input = &req.messages[0].content;
        let quote = if input.contains("FIRST WINDOW FACT") {
            "FIRST WINDOW FACT"
        } else if input.contains("SECOND WINDOW FACT") {
            "SECOND WINDOW FACT"
        } else {
            "Retained evidence."
        };
        Ok(CompletionResponse {
            content:
                json!({"atoms":[{"kind":"fact","text":quote,"evidence_quote":quote,"entities":[]}]})
                    .to_string(),
            input_tokens: 10,
            output_tokens: 10,
            model: "fixture".into(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: Some("tool_use".into()),
            cost_usd: None,
        })
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        panic!("nonstreaming only")
    }
}

#[tokio::test]
async fn dry_run_cohort_cursors_and_accounting_are_idempotent_and_scoped() {
    let f = Fixture::new().await;
    let original = f.capture("original", "A retained original.").await;
    let synthetic = f.capture("synthetic", "Explicit synthetic demo.").await;
    sqlx::query("UPDATE source_versions SET source_metadata=jsonb_build_object('synthetic_demo',true) WHERE version_id=$1").bind(synthetic).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'legacy','orphan-chunk','fixture/legacy',now())").bind(f.graph).execute(&f.owner).await.unwrap();
    let mut in_flight = f.owner.begin().await.unwrap();
    let late_committed_atom: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'legacy','in-flight-before-cutoff','fixture/pending',now()) RETURNING atom_id")
        .bind(f.graph).fetch_one(&mut *in_flight).await.unwrap();
    let run = f.run(false, 10).await;
    in_flight.commit().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_error='snapshot fixture state update',inventory_created_xid=pg_current_xact_id() WHERE version_id=$1")
        .bind(original).execute(&f.owner).await.unwrap();
    let late = f.capture("after-cutoff", "Later content.").await;
    for _ in 0..10 {
        f.advance(run, 1).await;
    }
    let rows:Vec<(String,String,Option<String>)>=sqlx::query_as("SELECT kind,state,reason FROM corpus_enrichment_units WHERE run_id=$1 ORDER BY kind,holding_id").bind(run).fetch_all(&f.owner).await.unwrap();
    assert_eq!(rows.len(), 4);
    assert!(!sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2)")
        .bind(run).bind(late_committed_atom).fetch_one(&f.owner).await.unwrap(), "a pre-cutoff transaction committed later belongs to the next cohort");
    assert!(rows.contains(&(
        "legacy_chunk".into(),
        "pending".into(),
        Some("retained_legacy_baseline_promotion_required".into())
    )));
    assert!(rows.contains(&(
        "local_holdings".into(),
        "unavailable".into(),
        Some("device_key_read_timeout_15s_no_retry_or_upload".into())
    )));
    assert!(rows.contains(&(
        "retained_version".into(),
        "excluded".into(),
        Some("synthetic_demo_outside_real_corpus".into())
    )));
    assert!(!sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE holding_id=$1)"
    )
    .bind(late)
    .fetch_one(&f.owner)
    .await
    .unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT attempted_model_calls FROM corpus_enrichment_runs WHERE run_id=$1"
        )
        .bind(run)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        0
    );
    let mut scoped = begin_account_scoped(&f.app, Uuid::new_v4()).await.unwrap();
    assert!(describe_tx(&mut scoped, f.graph, run)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM corpus_enrichment_units WHERE holding_id=$1"
        )
        .bind(original)
        .fetch_one(&mut *scoped)
        .await
        .unwrap(),
        0
    );
    scoped.rollback().await.unwrap();
    let demo=f.run_kind(false,10,"synthetic_demo").await;
    assert!(sqlx::query("UPDATE corpus_enrichment_runs SET corpus_kind='real' WHERE run_id=$1").bind(demo).execute(&f.owner).await.is_err(),"a cohort cannot be reclassified after creation");
    for _ in 0..6 {
        f.advance(demo, 100).await;
    }
    let demo_legacy: (String, String) = sqlx::query_as(
        "SELECT state,reason FROM corpus_enrichment_units WHERE run_id=$1 AND kind='legacy_chunk' ORDER BY unit_id LIMIT 1",
    )
    .bind(demo)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert_eq!(
        demo_legacy,
        (
            "excluded".into(),
            "unclassified_legacy_outside_demo_corpus".into()
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2"
        )
        .bind(demo)
        .bind(original)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        "excluded"
    );
    f.finish().await;
}

#[tokio::test]
async fn unfunded_execution_holds_before_the_model_with_exact_financial_reason() {
    let f = Fixture::new().await;
    f.contract().await;
    let funding: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM member_allowance_periods WHERE account_id=$1),(SELECT count(*) FROM credit_balances WHERE account_id=$1)")
        .bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(funding, (0, 0), "negative control must have neither allowance nor pool funding");
    let version = f.capture("unfunded-source", "Retained evidence.").await;
    let run = f.run(true, 2).await;
    f.advance(run, 20).await;
    let model = Model { calls: AtomicUsize::new(0), owner: f.owner.clone(), pause: None,
        fail: false, revoke_requester: None };
    f.extract(&model, f.job(version, 0).await).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 0, "execution budget cannot mint a funded send");
    let held: (String, Option<String>, i64) = sqlx::query_as(
        "SELECT status,pause_reason,attempted_model_calls FROM corpus_enrichment_runs WHERE run_id=$1")
        .bind(run).fetch_one(&f.owner).await.unwrap();
    assert_eq!(held, ("paused".into(), Some("credits_exhausted".into()), 1),
        "observe the actual unfunded hold rather than infer it from zero calls");
    let output: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1),(SELECT next_start_char FROM source_versions WHERE version_id=$1),(SELECT count(*) FROM managed_attempt_receipts WHERE source_version_id=$1),(SELECT count(*) FROM managed_credit_reservations WHERE account_id=$2)")
        .bind(version).bind(f.account).fetch_one(&f.owner).await.unwrap();
    assert_eq!(output, (0, 0, 0, 0), "unfunded attempt must neither publish nor arm/fund a receipt");
    f.finish().await;
}

#[tokio::test]
async fn managed_windows_charge_retries_pause_before_persistence_and_resume() {
    let f = Fixture::new().await;
    // Tenant 0203 / control 0386 (#1516) drop the title-only unique index on
    // every plane, so a migration-built database is past the expand phase.
    // Re-create the expand-phase marker to prove the gate still holds there;
    // the contract below drops it again.
    sqlx::query("CREATE INDEX nodes_graph_type_lower_title_unique ON nodes(node_id)")
        .execute(&f.owner)
        .await
        .unwrap();
    let mut tx = begin_account_scoped(&f.app, f.account).await.unwrap();
    assert!(
        !lifecycle_ready_tx(&mut tx).await.unwrap(),
        "expand must not allow corpus execution"
    );
    tx.rollback().await.unwrap();
    f.contract().await;
    let allowance = f.fund_execution().await;
    let version = f
        .capture(
            "long-source",
            &format!(
                "FIRST WINDOW FACT\n{}\nSECOND WINDOW FACT",
                "x".repeat(9000)
            ),
        )
        .await;
    let run = f.run(true, 2).await;
    f.advance(run, 20).await;
    let model = Model {
        calls: AtomicUsize::new(0),
        owner: f.owner.clone(),
        pause: Some(run),
        fail: false,
        revoke_requester: None,
    };
    f.extract(&model, f.job(version, 0).await).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1"
        )
        .bind(version)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        0,
        "pause during model must prevent semantic writes"
    );
    sqlx::query(
        "UPDATE corpus_enrichment_runs SET status='running',pause_reason=NULL WHERE run_id=$1",
    )
    .bind(run)
    .execute(&f.owner)
    .await
    .unwrap();
    for _ in 0..4 {
        f.advance(run, 100).await;
    }
    let model = Model {
        calls: AtomicUsize::new(0),
        owner: f.owner.clone(),
        pause: None,
        fail: false,
        revoke_requester: None,
    };
    f.extract(&model, f.job(version, 0).await).await;
    let next: i64 =
        sqlx::query_scalar("SELECT next_start_char FROM source_versions WHERE version_id=$1")
            .bind(version)
            .fetch_one(&f.owner)
            .await
            .unwrap();
    assert!(next > 0);
    f.extract(&model, f.job(version, next).await).await;
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        1,
        "continuation cannot exceed run calls budget"
    );
    let state: (String, i64) = sqlx::query_as(
        "SELECT status,attempted_model_calls FROM corpus_enrichment_runs WHERE run_id=$1",
    )
    .bind(run)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    assert_eq!(state, ("paused".into(), 2));
    sqlx::query(
        "UPDATE corpus_enrichment_runs SET status='running',max_model_calls=3 WHERE run_id=$1",
    )
    .bind(run)
    .execute(&f.owner)
    .await
    .unwrap();
    f.advance(run, 100).await;
    f.extract(&model, f.job(version, next).await).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1"
        )
        .bind(version)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1"
        )
        .bind(version)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        2,
        "resumed windows must not duplicate semantic facts"
    );
    let rerun = f.run(false, 10).await;
    f.advance(rerun, 100).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2"
        )
        .bind(rerun)
        .bind(version)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        "processed"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2, "explicit resume admits exactly the final continuation");
    f.assert_funded_attempts(run, version, allowance, 3, "billed").await;
    f.finish().await;
}

#[tokio::test]
async fn two_runs_never_steal_versions_and_failed_calls_consume_budget() {
    let f = Fixture::new().await;
    f.contract().await;
    let allowance = f.fund_execution().await;
    let version = f.capture("retry-source", "Retained evidence.").await;
    let first = f.run(true, 1).await;
    f.advance(first, 20).await;
    let second = f.run(true, 1).await;
    f.advance(second, 20).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT reason FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2"
        )
        .bind(second)
        .bind(version)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        "owned_by_another_run"
    );
    let model = Model {
        calls: AtomicUsize::new(0),
        owner: f.owner.clone(),
        pause: None,
        fail: true,
        revoke_requester: None,
    };
    f.extract(&model, f.job(version, 0).await).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let job = f.job(version, 0).await;
    f.extract(&model, job).await;
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        1,
        "retry cannot bypass exhausted budget"
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT attempted_model_calls FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2").bind(first).bind(version).fetch_one(&f.owner).await.unwrap(),1);
    f.assert_funded_attempts(first, version, allowance, 1, "not_generated").await;
    f.finish().await;
}

#[tokio::test]
async fn requester_revoked_during_model_cannot_write_evidence() {
    let f = Fixture::new().await;
    f.contract().await;
    let allowance = f.fund_execution().await;
    let version = f.capture("permission-source", "Retained evidence.").await;
    let run = f.run(true, 2).await;
    f.advance(run, 20).await;
    let model = Model {
        calls: AtomicUsize::new(0),
        owner: f.owner.clone(),
        pause: None,
        fail: false,
        revoke_requester: Some((f.account, f.user)),
    };
    f.extract(&model, f.job(version, 0).await).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1"
        )
        .bind(version)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT next_start_char FROM source_versions WHERE version_id=$1"
        )
        .bind(version)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        0
    );
    f.assert_funded_attempts(run, version, allowance, 1, "billed").await;
    f.finish().await;
}

#[test]
fn controller_routes_register_with_supported_axum_path_syntax() {
    let _ = crate::v1_routes();
}

#[tokio::test]
async fn permission_changed_during_blob_read_prevents_model_dispatch() {
    // The invocation supplies only this public fixture key; do not inspect or
    // borrow any deployment credentials to exercise the encrypted local fake.
    assert!(
        std::env::var("MASTER_KEK_HEX").is_ok_and(|v| v == "07".repeat(32)),
        "fixed disposable MASTER_KEK_HEX fixture required"
    );
    use axum::{routing::get, Router};
    use coralswarm_core::{crypto::encrypt, kek::wrap_dek, r2::R2Client};
    {
        let f = Fixture::new().await;
        f.contract().await;
        let version = f.capture("blob-authorization", "Retained evidence.").await;
        let key = format!("{}/{}", f.graph, version);
        sqlx::query("UPDATE accounts SET dek_wrapped=$2 WHERE id=$1")
            .bind(f.account)
            .bind(wrap_dek(&[7; 32], &[42; 32]).unwrap())
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE source_versions SET raw_inline=NULL,raw_blob_key=$2 WHERE version_id=$1",
        )
        .bind(version)
        .bind(key)
        .execute(&f.owner)
        .await
        .unwrap();
        let run = f.run(true, 2).await;
        f.advance(run, 20).await;
        let owner = f.owner.clone();
        let account = f.account;
        let graph = f.graph;
        let bytes =
            serde_json::to_vec(&encrypt(b"Retained evidence.", &[42; 32]).unwrap()).unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let app=Router::new().fallback(get(move || {let owner=owner.clone();let bytes=bytes.clone();let observed=observed.clone();async move {
            observed.fetch_add(1,Ordering::SeqCst);
            sqlx::query("UPDATE source_items SET access_state='restricted' WHERE graph_id=$1").bind(graph).execute(&owner).await.unwrap();
            ([("content-type","application/json")],bytes)
        }}));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let r2 = R2Client::new_with_endpoint_override(
            "fixture",
            "fixture-access",
            "fixture-secret",
            "fixture-bucket",
            &endpoint,
        )
        .await;
        let model = Model {
            calls: AtomicUsize::new(0),
            owner: f.owner.clone(),
            pause: None,
            fail: false,
            revoke_requester: None,
        };
        source_atoms::process_job(
            &TenantId::Consumer,
            &f.app,
            &f.owner,
            &PostgresStore::new(f.app.clone()),
            &crate::llm::testing::GatedScript(&model),
            Some(&r2),
            f.job(version, 0).await,
        )
        .await;
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "actual encrypted blob GET must execute"
        );
        assert_eq!(
            model.calls.load(Ordering::SeqCst),
            0,
            "permission change during blob I/O must prevent private prompt dispatch"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT attempted_model_calls FROM corpus_enrichment_runs WHERE run_id=$1"
            )
            .bind(run)
            .fetch_one(&f.owner)
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1"
            )
            .bind(version)
            .fetch_one(&f.owner)
            .await
            .unwrap(),
            0
        );
        server.abort();
        f.finish().await;
    }
}

#[tokio::test]
async fn creator_budget_resume_and_access_aware_receipts_preserve_private_run_history() {
    use crate::tenancy::extractor::Tenant;
    let f = Fixture::new().await;
    f.contract().await;
    sqlx::query("UPDATE graphs SET kind='org' WHERE id=$1")
        .bind(f.graph)
        .execute(&f.owner)
        .await
        .unwrap();
    let first = f.capture("budget-one", "Retained evidence.").await;
    let second = f.capture("budget-two", "Second retained evidence.").await;
    let run = f.run(true, 10).await;
    sqlx::query("UPDATE corpus_enrichment_runs SET max_items=1 WHERE run_id=$1")
        .bind(run)
        .execute(&f.owner)
        .await
        .unwrap();
    f.advance(run, 20).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM corpus_enrichment_runs WHERE run_id=$1"
        )
        .bind(run)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        "paused"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2"
        )
        .bind(run)
        .bind(second)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        "pending"
    );
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(
        f.app.clone(),
        None,
    ));
    let tenant = Tenant::legacy(&state);
    let actor = UserContext {
        account_id: f.account,
        user_id: f.user,
        role: "owner".into(),
        device_id: "p3-fixture".into(),
    };
    let peer: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
        .bind(format!("p3-peer-{}@test.local", Uuid::new_v4()))
        .fetch_one(&f.owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
        .bind(f.account)
        .bind(peer)
        .execute(&f.owner)
        .await
        .unwrap();
    assert!(
        crate::tenancy::reachability::ReachabilityResolver::graph_reachable_by_user(
            None,
            &PostgresStore::new(f.app.clone()),
            f.graph,
            peer
        )
        .await
        .unwrap()
    );
    let peer = UserContext {
        user_id: peer,
        role: "member".into(),
        ..actor.clone()
    };
    let request = || RunRequest {
        graph_id: f.graph,
        page_size: None,
        budget: Some(Budget {
            items: 2,
            characters: 50000,
            model_calls: 10,
        }),
    };
    assert_eq!(
        resume_run(
            State(state.clone()),
            tenant.clone(),
            Extension(peer.clone()),
            Path(run),
            Json(request())
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        get_run(tenant.clone(), Extension(peer), Path(run), Query(request()))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        resume_run(
            State(state.clone()),
            tenant.clone(),
            Extension(actor.clone()),
            Path(run),
            Json(request())
        )
        .await
        .0,
        StatusCode::OK
    );
    f.advance(run, 20).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT admitted_items FROM corpus_enrichment_runs WHERE run_id=$1"
        )
        .bind(run)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM corpus_enrichment_units WHERE run_id=$1 AND execution_owner"
        )
        .bind(run)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        2
    );
    sqlx::query("UPDATE source_items SET access_state='unknown' WHERE source_item_id=(SELECT source_item_id FROM source_versions WHERE version_id=$1)").bind(first).execute(&f.owner).await.unwrap();
    // Separate holding tables may share UUIDs, including client-chosen legacy
    // IDs. This cannot bridge to denied source-version job receipts.
    sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,$2,'legacy','receipt-collision','fixture/legacy','Own normalized baseline',now())")
        .bind(first).bind(f.graph).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,state,processing_path) VALUES($1,$2,'legacy_chunk',$3,'pending','legacy_normalized_baseline_pending')")
        .bind(run).bind(f.graph).bind(first).execute(&f.owner).await.unwrap();
    let (status, Json(body)) = get_units(
        tenant.clone(),
        Extension(actor.clone()),
        Path(run),
        Query(UnitsQuery {
            graph_id: f.graph,
            before: None,
            limit: Some(20),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let units = body["units"].as_array().unwrap();
    assert_eq!(units.len(), 2);
    assert!(units
        .iter()
        .any(|u| u["holding_id"] == second.to_string() && u["kind"] == "retained_version"));
    let legacy = units.iter().find(|u| u["kind"] == "legacy_chunk").unwrap();
    assert_eq!(
        legacy["jobs"],
        json!([]),
        "legacy UUID cannot expose denied version job receipts"
    );
    let (status, Json(body)) = get_run(tenant, Extension(actor), Path(run), Query(request())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["admitted_items"], 2,
        "private cost/admission history stays intact"
    );
    let retained = body["coverage"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == "retained_version")
        .unwrap();
    assert_eq!(
        retained["holding_count"], 1,
        "denied source never contributes to visible counts or receipts"
    );
    f.finish().await;
}

#[tokio::test]
async fn concurrent_windows_reserve_one_shared_budget_atomically() {
    let f = Fixture::new().await;
    f.contract().await;
    let first = f.capture("concurrent-one", "Retained evidence.").await;
    let second = f
        .capture("concurrent-two", "Second retained evidence.")
        .await;
    let run = f.run(true, 1).await;
    f.advance(run, 20).await;
    let reserve = |version| {
        let pool = f.app.clone();
        let account = f.account;
        let graph = f.graph;
        async move {
            let mut tx = begin_account_scoped(&pool, account).await.unwrap();
            let gate = reserve_window_tx(&mut tx, graph, version, "source-atoms-v2", 20, true)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            gate
        }
    };
    let (a, b) = tokio::join!(reserve(first), reserve(second));
    assert_eq!(
        usize::from(matches!(a, WindowGate::Permit { .. }))
            + usize::from(matches!(b, WindowGate::Permit { .. })),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT attempted_model_calls FROM corpus_enrichment_runs WHERE run_id=$1"
        )
        .bind(run)
        .fetch_one(&f.owner)
        .await
        .unwrap(),
        1
    );
    f.finish().await;
}

#[tokio::test]
async fn predeployment_unowned_running_job_is_excluded_and_cannot_dispatch() {
 let f=Fixture::new().await;f.contract().await;
 let version=f.capture("predeployment-running","Retained evidence.").await;
 assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM jobs WHERE payload->>'source_version_id'=$1").bind(version.to_string()).fetch_one(&f.owner).await.unwrap(),0,"ordinary capture has no implicit model job");
 let id:Uuid=sqlx::query_scalar("INSERT INTO jobs(kind,payload,account_id,status) VALUES('ExtractSourceAtoms',$1,$2,'running') RETURNING id").bind(json!({"graph_id":f.graph,"source_version_id":version,"start_char":0})).bind(f.account).fetch_one(&f.owner).await.unwrap();
 let run=f.run(true,2).await;f.advance(run,20).await;
 assert_eq!(sqlx::query_scalar::<_,String>("SELECT reason FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=$2").bind(run).bind(version).fetch_one(&f.owner).await.unwrap(),"already_running_outside_run");
 let model=Model{calls:AtomicUsize::new(0),owner:f.owner.clone(),pause:None,fail:false,revoke_requester:None};
 f.extract(&model,JobRow{id,kind:"ExtractSourceAtoms".into(),payload:json!({"graph_id":f.graph,"source_version_id":version,"start_char":0}),attempts:0,account_id:Some(f.account)}).await;
 assert_eq!(model.calls.load(Ordering::SeqCst),0);f.finish().await;
}

#[tokio::test]
async fn original_availability_reports_retained_bytes_and_representation_separately() {
    use crate::tenancy::extractor::Tenant;
    use std::collections::BTreeMap;
    let f = Fixture::new().await;
    let original = f.capture("provider-original", "Provider original.").await;
    let inline = f
        .capture_as("normalized-inline", "Normalized, inline.", "normalized_text")
        .await;
    let blob = f
        .capture_as("normalized-blob", "Normalized, blob.", "normalized_text")
        .await;
    sqlx::query("UPDATE source_versions SET raw_blob_key='fixture/blob',raw_inline=NULL WHERE version_id=$1")
        .bind(blob).execute(&f.owner).await.unwrap();
    let lost = f
        .capture_as("normalized-lost", "Normalized, lost.", "normalized_text")
        .await;
    sqlx::query("UPDATE source_versions SET raw_inline=NULL WHERE version_id=$1")
        .bind(lost)
        .execute(&f.owner)
        .await
        .unwrap();
    let run = f.run(false, 10).await;
    for _ in 0..6 {
        f.advance(run, 100).await;
    }
    let units: BTreeMap<Uuid, (String, String, Option<String>)> = sqlx::query_as::<_, (Uuid, String, String, Option<String>)>(
        "SELECT holding_id,original_availability,state,reason FROM corpus_enrichment_units WHERE run_id=$1 AND kind='retained_version'",
    )
    .bind(run)
    .fetch_all(&f.owner)
    .await
    .unwrap()
    .into_iter()
    .map(|(id, availability, state, reason)| (id, (availability, state, reason)))
    .collect();
    let pending = |availability: &str| (availability.to_string(), "pending".to_string(), None);
    assert_eq!(
        units,
        BTreeMap::from([
            (original, pending("available")),
            (inline, pending("available")),
            (blob, pending("unknown")),
            (
                lost,
                (
                    "unavailable".into(),
                    "unavailable".into(),
                    Some("original_unavailable".into())
                )
            ),
        ]),
        "availability follows retained bytes, not the representation label"
    );
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(
        f.app.clone(),
        None,
    ));
    let tenant = Tenant::legacy(&state);
    let actor = UserContext {
        account_id: f.account,
        user_id: f.user,
        role: "owner".into(),
        device_id: "p3-fixture".into(),
    };
    let (status, Json(page)) = get_units(
        tenant.clone(),
        Extension(actor.clone()),
        Path(run),
        Query(UnitsQuery {
            graph_id: f.graph,
            before: None,
            limit: Some(20),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let manifest: BTreeMap<String, Value> = page["units"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| (u["holding_id"].as_str().unwrap().to_string(), u["representation"].clone()))
        .collect();
    assert_eq!(
        manifest,
        BTreeMap::from([
            (original.to_string(), json!("original_text")),
            (inline.to_string(), json!("normalized_text")),
            (blob.to_string(), json!("normalized_text")),
            (lost.to_string(), json!("normalized_text")),
            (run.to_string(), Value::Null),
        ]),
        "the manifest names the retained representation; the local_holdings unit has none"
    );
    let (status, Json(body)) = get_run(
        tenant,
        Extension(actor),
        Path(run),
        Query(RunRequest {
            graph_id: f.graph,
            page_size: None,
            budget: None,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mut coverage: Vec<(String, String, Option<String>, i64)> = body["coverage"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["kind"] == "retained_version")
        .map(|c| {
            (
                c["state"].as_str().unwrap().to_string(),
                c["original_availability"].as_str().unwrap().to_string(),
                c["representation"].as_str().map(str::to_string),
                c["holding_count"].as_i64().unwrap(),
            )
        })
        .collect();
    coverage.sort();
    assert_eq!(
        coverage,
        vec![
            ("pending".into(), "available".into(), Some("normalized_text".into()), 1),
            ("pending".into(), "available".into(), Some("original_text".into()), 1),
            ("pending".into(), "unknown".into(), Some("normalized_text".into()), 1),
            ("unavailable".into(), "unavailable".into(), Some("normalized_text".into()), 1),
        ],
        "coverage groups by bytes availability and representation as separate axes"
    );
    f.finish().await;
}

#[tokio::test]
async fn unchanged_recapture_never_requeues_a_failed_version() {
    let f = Fixture::new().await;
    f.contract().await;
    let jobs = |version: Uuid| {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE payload->>'source_version_id'=$1")
            .bind(version.to_string())
            .fetch_one(&f.owner)
    };
    let status = |version: Uuid| {
        sqlx::query_scalar::<_, String>("SELECT extraction_status FROM source_versions WHERE version_id=$1")
            .bind(version)
            .fetch_one(&f.owner)
    };
    let version = f.capture("resynced-item", "Retained evidence.").await;
    assert_eq!(status(version).await.unwrap(), "blocked");
    assert_eq!(f.capture("resynced-item", "Retained evidence.").await, version);
    assert_eq!(status(version).await.unwrap(), "blocked", "unowned blocked re-capture stays blocked");
    assert_eq!(jobs(version).await.unwrap(), 0);

    let run = f.run(true, 5).await;
    f.advance(run, 20).await;
    assert_eq!(jobs(version).await.unwrap(), 1, "the managed run queues the owned version");
    sqlx::query("UPDATE jobs SET status='dead' WHERE payload->>'source_version_id'=$1")
        .bind(version.to_string())
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("UPDATE source_versions SET extraction_status='failed',extraction_error='synthetic failure' WHERE version_id=$1")
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();

    assert_eq!(f.capture("resynced-item", "Retained evidence.").await, version);
    assert_eq!(jobs(version).await.unwrap(), 1, "identical re-capture queued no new job");
    assert_eq!(status(version).await.unwrap(), "failed", "identical re-capture left the failure visible");

    let changed = f.capture("resynced-item", "Revised evidence.").await;
    assert_ne!(changed, version, "changed content is a new version");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM corpus_capture_admissions WHERE source_version_id=$1")
            .bind(changed)
            .fetch_one(&f.owner)
            .await
            .unwrap(),
        "blocked",
        "changed version goes through managed admission like any new capture"
    );
    assert_eq!(status(changed).await.unwrap(), "blocked");
    let next = f.run(true, 5).await;
    f.advance(next, 20).await;
    assert_eq!(jobs(changed).await.unwrap(), 1, "the changed version is extracted through a managed run");
    assert_eq!(status(changed).await.unwrap(), "pending");
    assert_eq!(jobs(version).await.unwrap(), 1);
    assert_eq!(status(version).await.unwrap(), "failed");
    f.finish().await;
}

/// #1506: a cohort window whose managed call is denied `Deny::Unavailable`
/// (here the real cause: another process holds the account's billing lock, so
/// the gate's bounded lock wait gives up) is held under the shared hold rule,
/// like every other managed worker. The same job row goes back to `pending`
/// with no attempt charged and a clearable backoff, the window is pending
/// rather than failed, and the job runs and publishes once the lock is free.
/// A real storage error on the same path still fails onto the attempts ladder.
#[tokio::test]
async fn unavailable_managed_window_is_held_without_an_attempt_and_storage_errors_still_count() {
    let f = Fixture::new().await;
    f.contract().await;
    let allowance = f.fund_execution().await;
    let version = f.capture("unavailable-source", "Retained evidence.").await;
    let broken = f.capture("storage-error-source", "Retained evidence.").await;
    let run = f.run(true, 5).await;
    f.advance(run, 20).await;
    let model = Model { calls: AtomicUsize::new(0), owner: f.owner.clone(), pause: None,
        fail: false, revoke_requester: None };
    let job_state = |id: Uuid| {
        sqlx::query_as::<_, (String, i32, i32, Option<String>, bool)>(
            "SELECT status::text,attempts,hold_streak,error_message,run_after>now() FROM jobs WHERE id=$1")
            .bind(id)
            .fetch_one(&f.owner)
    };
    let version_state = |id: Uuid| {
        sqlx::query_as::<_, (String, Option<String>, i64)>(
            "SELECT extraction_status,extraction_error,next_start_char FROM source_versions WHERE version_id=$1")
            .bind(id)
            .fetch_one(&f.owner)
    };

    let unit_state = |id: Uuid| {
        sqlx::query_as::<_, (String, Option<String>, String, Option<String>)>(
            "SELECT u.state,u.reason,r.status,r.pause_reason FROM corpus_enrichment_units u JOIN corpus_enrichment_runs r ON r.run_id=u.run_id WHERE u.run_id=$1 AND u.holding_id=$2")
            .bind(run)
            .bind(id)
            .fetch_one(&f.owner)
    };
    let unit_before = unit_state(version).await.unwrap();
    assert_eq!(unit_before.2, "running");

    let job = f.job(version, 0).await;
    let held_id = job.id;
    let mut holder = begin_account_scoped(&f.owner, f.account).await.unwrap();
    crate::billing::renewal::lock_period_rotation(&mut holder, f.account).await.unwrap();
    f.extract(&model, job).await;
    holder.rollback().await.unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 0, "the gate refused before any send");
    assert_eq!(
        job_state(held_id).await.unwrap(),
        ("pending".into(), 0, 1, Some(format!("held: {}", crate::jobs::queue::MANAGED_UNAVAILABLE)), true),
        "Unavailable holds the same row: no attempt charged, a clearable backoff, not a storage error"
    );
    assert_eq!(
        crate::jobs::queue::hold_class(crate::jobs::queue::MANAGED_UNAVAILABLE),
        crate::jobs::queue::HoldClass::Clears,
        "the hold can clear, so it never dead-letters"
    );
    assert_eq!(version_state(version).await.unwrap(), ("pending".into(), None, 0),
        "the window is pending again, not failed");
    assert_eq!(unit_state(version).await.unwrap(), unit_before,
        "a transient denial neither fails the unit nor pauses its run");

    // Retryable: once due, the same row runs and publishes.
    sqlx::query("UPDATE jobs SET run_after=now() WHERE id=$1").bind(held_id).execute(&f.owner).await.unwrap();
    let job = f.job(version, 0).await;
    assert_eq!(job.id, held_id, "the held row itself carries the retry");
    f.extract(&model, job).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let (status, attempts, _, _, _) = job_state(held_id).await.unwrap();
    assert_eq!((status.as_str(), attempts), ("done", 0), "the retry completed on its first attempt");
    assert_eq!(version_state(version).await.unwrap().0, "done");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1")
            .bind(version).fetch_one(&f.owner).await.unwrap(),
        1
    );
    f.assert_funded_attempts(run, version, allowance, 1, "billed").await;

    // A real storage error on the window's managed run ledger still counts.
    sqlx::raw_sql(
        "CREATE FUNCTION test_refuse_run_reservation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic run ledger failure'; END $$;
         CREATE TRIGGER test_refuse_run_reservation BEFORE UPDATE OF attempted_model_calls ON corpus_enrichment_runs FOR EACH ROW EXECUTE FUNCTION test_refuse_run_reservation();")
        .execute(&f.owner).await.unwrap();
    let job = f.job(broken, 0).await;
    let broken_id = job.id;
    f.extract(&model, job).await;
    assert_eq!(model.calls.load(Ordering::SeqCst), 1, "nothing sent behind a storage error");
    assert_eq!(
        job_state(broken_id).await.unwrap(),
        ("pending".into(), 1, 0, Some("managed run storage error".into()), true),
        "a storage error fails onto the attempts ladder"
    );
    assert_eq!(version_state(broken).await.unwrap().0, "failed");
    f.finish().await;
}
