//! Semantic meeting extraction with real scoped Postgres and a mock model.
//! Requires a migrated, disposable LOCAL RELINFER_TEST_DATABASE_URL.
mod support;

use std::{
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
};

use coralswarm_backend::{
    jobs::{
        queue::{self, JobRow},
        JobKind,
    },
    llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent},
    store::PostgresStore,
    tenancy::TenantId,
    workers::meeting_atoms,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use uuid::Uuid;

struct MockModel {
    calls: AtomicUsize,
    output: String,
    delete_source: Option<(PgPool, Uuid, String)>,
    replace_current: Option<(PgPool, Uuid, Uuid)>,
}

#[tokio::test]
async fn meeting_summary_switched_off_never_dispatches_and_stays_pending() {
    // Managed enrichment (PR 10): with ENRICHMENT_PROCESSING_ENABLED unset
    // (the default) a claimed summary spends nothing and is handed back.
    std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(app.clone(), None));
    let source = "switched-off-summary";
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&app, account).await.unwrap();
    let first = coralswarm_backend::meeting_evidence::capture_version_tx(
        &mut tx, &state, graph, account, source, "Alice: Version A is the current plan.",
        None, None, None, &[42; 32],
    ).await.unwrap();
    tx.commit().await.unwrap();
    let kind = JobKind::GenerateMeetingSummary {
        graph_id: graph, source_id: source.into(), allow_cloud_override: false,
    };
    let key = format!("summary:{graph}:{source}");
    let id = queue::enqueue(&app, &kind, Some(account), Some(&key), 0).await.unwrap().unwrap();
    sqlx::query("UPDATE jobs SET status='running',attempts=1 WHERE id=$1").bind(id).execute(&owner).await.unwrap();
    let model = MockModel::new("{}".into());
    coralswarm_backend::workers::summary::process_job(
        &TenantId::Consumer, &app, &owner, &PostgresStore::new(app.clone()), &model, None,
        JobRow { id, kind: "GenerateMeetingSummary".into(),
            payload: serde_json::to_value(&kind).unwrap()["payload"].clone(), attempts: 1, account_id: Some(account) },
    ).await;
    assert_eq!(model.calls(), 0);
    let current: (Uuid, String, String, Option<Value>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT current_version_id,raw_inline,summary_status,summary,summary_error,summary_model \
         FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2",
    ).bind(graph).bind(source).fetch_one(&owner).await.unwrap();
    assert_eq!(current, (first.version_id, "Alice: Version A is the current plan.".into(),
        "pending".into(), None, None, None));
    // Held in place under the shared retry rule (`jobs::queue::hold`): the
    // same row, its key kept, due later, no attempt charged.
    let job: (String, Option<String>, bool, i32) = sqlx::query_as(
        "SELECT status::text,error_message,run_after > now() + interval '60 seconds',attempts FROM jobs WHERE id=$1",
    ).bind(id).fetch_one(&owner).await.unwrap();
    assert_eq!(job, ("pending".into(), Some("held: switched_off".into()), true, 1),
        "handed back for a later pass, never dead-lettered");
    let others: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE idempotency_key=$1 AND id<>$2")
        .bind(&key).bind(id).fetch_one(&owner).await.unwrap();
    assert_eq!(others, 0, "no fresh row: the meeting keeps its one pass");
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM llm_usage WHERE source_job_id=$1")
        .bind(id).fetch_one(&owner).await.unwrap(), 0);
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

impl MockModel {
    fn new(output: String) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            output,
            delete_source: None,
            replace_current: None,
        }
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl LlmProvider for MockModel {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let schema = request.tool_schema.unwrap();
        assert_eq!(schema.name, "emit_meeting_atoms");
        assert!(schema.strict);
        assert!(request.messages[0].content.chars().count() <= 8_000);
        if let Some((pool, graph, source)) = &self.delete_source {
            sqlx::query("DELETE FROM source_items WHERE graph_id=$1 AND source_id=$2")
                .bind(graph)
                .bind(source)
                .execute(pool)
                .await
                .unwrap();
        }
        if let Some((pool, graph, version)) = &self.replace_current {
            sqlx::query("UPDATE meeting_transcripts SET current_version_id=$2 WHERE graph_id=$1")
                .bind(graph)
                .bind(version)
                .execute(pool)
                .await
                .unwrap();
        }
        Ok(CompletionResponse {
            content: self.output.clone(),
            input_tokens: 10,
            output_tokens: 10,
            model: "mock-meeting-extractor".to_string(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: Some("tool_use".to_string()),
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
        panic!("meeting extraction uses forced nonstreaming tool output")
    }
}

async fn pools() -> Option<(coralswarm_backend::test_db::TestDatabase, PgPool, PgPool)> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        eprintln!("skipping meeting atom extraction test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(
        matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"),
        "test database must be local"
    );
    let (db, owner) = support::managed_extraction::isolated_database(&opts).await;
    let opts = owner.connect_options().as_ref().clone();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let app = PgPoolOptions::new()
        .max_connections(6)
        .connect_with(
            opts.username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    Some((db, owner, app))
}

async fn account(pool: &PgPool) -> (Uuid, Uuid) {
    let suffix = Uuid::new_v4();
    let user: Uuid =
        sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$2) RETURNING id")
            .bind(format!("meeting-atoms-{suffix}@test.local"))
            .bind(format!("meeting-atoms-{suffix}"))
            .fetch_one(pool)
            .await
            .unwrap();
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
            .bind(user)
            .fetch_one(pool)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
        .bind(account).bind(user).execute(pool).await.unwrap();
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
    sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,100)")
        .bind(account).execute(&mut *financial).await.unwrap();
    financial.commit().await.unwrap();
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id,name) VALUES($1,'semantic test') RETURNING id",
    )
    .bind(account)
    .fetch_one(pool)
    .await
    .unwrap();
    (account, graph)
}

async fn version(pool: &PgPool, graph: Uuid, source: &str, text: Option<&str>) -> Uuid {
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting',$2) ON CONFLICT(graph_id,source_type,provider_namespace,source_id) DO UPDATE SET source_id=EXCLUDED.source_id RETURNING source_item_id")
        .bind(graph).bind(source).fetch_one(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO meeting_transcripts(graph_id,source_id,source_item_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
    )
    .bind(graph)
    .bind(source)
    .bind(item)
    .execute(pool)
    .await
    .unwrap();
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_id,raw_inline,extraction_status,source_item_id,extraction_version) VALUES($1,$2,$3,'pending',$4,'meeting-atoms-v1') RETURNING version_id")
        .bind(graph).bind(source).bind(text).bind(item).fetch_one(pool).await.unwrap();
    sqlx::query(
        "UPDATE meeting_transcripts SET current_version_id=$3 WHERE graph_id=$1 AND source_id=$2",
    )
    .bind(graph)
    .bind(source)
    .bind(version)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(pool)
        .await
        .unwrap();
    version
}

async fn job(pool: &PgPool, account: Uuid, graph: Uuid, version: Uuid, start: i64) -> JobRow {
    support::managed_extraction::own_version(pool, version).await;
    let kind = JobKind::ExtractMeetingAtoms {
        graph_id: graph,
        source_version_id: version,
        start_char: start,
    };
    let id = queue::enqueue(pool, &kind, Some(account), None, 0)
        .await
        .unwrap()
        .unwrap();
    let wire = serde_json::to_value(kind).unwrap();
    JobRow {
        id,
        kind: "ExtractMeetingAtoms".to_string(),
        payload: wire["payload"].clone(),
        attempts: 0,
        account_id: Some(account),
    }
}

async fn run(app: &PgPool, control: &PgPool, model: &MockModel, job: JobRow) {
    meeting_atoms::process_job(
        &TenantId::Consumer,
        app,
        control,
        &PostgresStore::new(app.clone()),
        &coralswarm_backend::llm::testing::GatedScript(model),
        None,
        job,
    )
    .await;
}

fn atom_output(quote: &str) -> String {
    json!({"atoms":[{"kind":"action","text":"Alice will deliver Roadmap.","evidence_quote":quote,
        "entities":[{"mention":"Alice","type":"person"},{"mention":"Roadmap","type":"project"}]}]})
    .to_string()
}

async fn state(pool: &PgPool, version: Uuid) -> (String, i64, Option<String>) {
    sqlx::query_as("SELECT extraction_status,next_start_char,extraction_error FROM source_versions WHERE version_id=$1")
        .bind(version).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn meeting_atoms_attach_exact_evidence_and_retries_are_durable() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let quote = "Alice will deliver Roadmap.\r\n";
    let text = format!("🪸\r\n{quote}");
    let ver = version(&owner, graph, "meeting:%_🪸", Some(&text)).await;
    let existing: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body,identity_scope) VALUES($1,'entity','Alice','{\"entity_type\":\"person\"}',$2) RETURNING node_id")
        .bind(graph).bind(ver).fetch_one(&owner).await.unwrap();
    assert_eq!(sqlx::query_scalar::<_, Uuid>("SELECT identity_scope FROM nodes WHERE node_id=$1")
        .bind(existing).fetch_one(&owner).await.unwrap(), ver);
    let model = MockModel::new(atom_output(quote));
    let first = job(&owner, account, graph, ver, 0).await;
    let first_id = first.id;
    run(&app, &owner, &model, first).await;
    assert_eq!(
        state(&owner, ver).await,
        ("done".to_string(), text.chars().count() as i64, None)
    );
    let evidence: (i32, i32, String) = sqlx::query_as("SELECT p.start_char,p.end_char,p.quote FROM semantic_atom_passages p WHERE p.source_version_id=$1")
        .bind(ver).fetch_one(&owner).await.unwrap();
    assert_eq!(
        evidence,
        (3, text.chars().count() as i32, quote.to_string())
    );
    let links: Vec<Uuid> = sqlx::query_scalar("SELECT n.node_id FROM node_semantic_atoms n JOIN semantic_atoms a USING(semantic_atom_id) WHERE a.source_version_id=$1")
        .bind(ver).fetch_all(&owner).await.unwrap();
    assert_eq!(links.len(), 2);
    assert!(links.contains(&existing));
    let usage: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM llm_usage WHERE source_job_id=$1 AND task='meeting_atoms'",
    )
    .bind(first_id)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(usage, 1, "automatic extraction records metered usage");
    run(
        &app,
        &owner,
        &model,
        job(&owner, account, graph, ver, 0).await,
    )
    .await;
    assert_eq!(
        model.calls(),
        1,
        "durable cursor prevents a second model call after job dedup expires"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1")
            .bind(ver)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(count, 1);
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

#[tokio::test]
async fn meeting_atoms_continue_every_window_and_invalid_output_remains_retryable() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let text = "α🪸".repeat(9_001);
    let ver = version(&owner, graph, "long-meeting", Some(&text)).await;
    let empty = MockModel::new(json!({"atoms":[]}).to_string());
    run(
        &app,
        &owner,
        &empty,
        job(&owner, account, graph, ver, 0).await,
    )
    .await;
    for expected in [8_000, 16_000] {
        assert_eq!(state(&owner, ver).await.1, expected);
        let row: (Uuid, Value) = sqlx::query_as("SELECT id,payload FROM jobs WHERE kind='ExtractMeetingAtoms' AND payload->>'source_version_id'=$1 AND (payload->>'start_char')::bigint=$2 AND status='pending'")
            .bind(ver.to_string()).bind(expected).fetch_one(&owner).await.unwrap();
        run(
            &app,
            &owner,
            &empty,
            JobRow {
                id: row.0,
                kind: "ExtractMeetingAtoms".to_string(),
                payload: row.1,
                attempts: 0,
                account_id: Some(account),
            },
        )
        .await;
    }
    assert_eq!(
        state(&owner, ver).await,
        ("done".to_string(), text.chars().count() as i64, None)
    );
    assert_eq!(empty.calls(), 3);
    let windows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM semantic_atom_windows WHERE source_version_id=$1")
            .bind(ver)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(
        windows, 3,
        "an empty valid extraction still completes each durable window"
    );
    let bad = version(
        &owner,
        graph,
        "bad-response",
        Some("Alice will deliver Roadmap."),
    )
    .await;
    let malformed = MockModel::new("{}".to_string());
    run(
        &app,
        &owner,
        &malformed,
        job(&owner, account, graph, bad, 0).await,
    )
    .await;
    assert_eq!(
        state(&owner, bad).await,
        (
            "failed".to_string(),
            0,
            Some("invalid extraction schema".to_string())
        )
    );
    let valid = MockModel::new(atom_output("Alice will deliver Roadmap."));
    run(
        &app,
        &owner,
        &valid,
        job(&owner, account, graph, bad, 0).await,
    )
    .await;
    assert_eq!(state(&owner, bad).await.0, "done");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

#[tokio::test]
async fn meeting_atoms_fail_closed_and_do_not_recreate_deleted_sources() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account_a, graph) = account(&owner).await;
    let (account_b, _) = account(&owner).await;
    let ver = version(
        &owner,
        graph,
        "private",
        Some("Alice will deliver Roadmap."),
    )
    .await;
    let model = MockModel::new(atom_output("Alice will deliver Roadmap."));
    run(
        &app,
        &owner,
        &model,
        job(&owner, account_b, graph, ver, 0).await,
    )
    .await;
    assert_eq!(model.calls(), 0, "wrong owner never sends source content");
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1")
        .bind(ver).fetch_one(&owner).await.unwrap(), 0);
    let missing = version(&owner, graph, "unavailable", None).await;
    run(
        &app,
        &owner,
        &model,
        job(&owner, account_a, graph, missing, 0).await,
    )
    .await;
    assert_eq!(model.calls(), 0);
    assert_eq!(
        state(&owner, missing).await.2.as_deref(),
        Some("source text unavailable")
    );
    let deleted_version = version(&owner, graph, "delete-during-call", Some("Alice will deliver Roadmap.")).await;
    assert_ne!(deleted_version, ver);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(DISTINCT source_item_id) FROM source_versions WHERE version_id=ANY($1)")
        .bind(vec![ver, deleted_version]).fetch_one(&owner).await.unwrap(), 2);
    let deletion_job = job(&owner, account_a, graph, deleted_version, 0).await;
    let previous_owner: (Uuid, Uuid, String) = sqlx::query_as(
        "SELECT r.run_id,u.unit_id,r.status FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u USING(run_id) WHERE u.holding_id=$1 AND u.execution_owner",
    ).bind(ver).fetch_one(&owner).await.unwrap();
    let deletion_owner: (Uuid, Uuid, String) = sqlx::query_as(
        "SELECT r.run_id,u.unit_id,r.status FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u USING(run_id) WHERE u.holding_id=$1 AND u.execution_owner",
    ).bind(deleted_version).fetch_one(&owner).await.unwrap();
    assert_ne!(previous_owner.0, deletion_owner.0);
    assert_ne!(previous_owner.1, deletion_owner.1);
    assert_eq!(previous_owner.2, "running");
    assert_eq!(deletion_owner.2, "running");
    let mut deleting = MockModel::new(atom_output("Alice will deliver Roadmap."));
    deleting.delete_source = Some((owner.clone(), graph, "delete-during-call".to_string()));
    run(&app, &owner, &deleting, deletion_job).await;
    assert_eq!(deleting.calls(), 1, "deletion race must execute the model hook");
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_items WHERE graph_id=$1 AND source_id='delete-during-call'")
        .bind(graph).fetch_one(&owner).await.unwrap(), 0);
    let nodes: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1")
        .bind(graph)
        .fetch_one(&owner)
        .await
        .unwrap();
    let atoms: i64 =
        sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1")
            .bind(deleted_version)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM atoms WHERE graph_id=$1")
        .bind(graph).fetch_one(&owner).await.unwrap(), 0);
    assert_eq!(
        (nodes, atoms),
        (0, 0),
        "deleted during model call => no orphan nodes or atoms"
    );
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

#[tokio::test]
async fn meeting_atoms_old_completion_cannot_overwrite_the_current_version() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let old = version(
        &owner,
        graph,
        "updated",
        Some("Alice will deliver Roadmap."),
    )
    .await;
    let new = version(&owner, graph, "updated", Some("The plan changed.")).await;
    let mut model = MockModel::new(atom_output("Alice will deliver Roadmap."));
    model.replace_current = Some((owner.clone(), graph, new));
    run(
        &app,
        &owner,
        &model,
        job(&owner, account, graph, old, 0).await,
    )
    .await;
    let current: Uuid = sqlx::query_scalar("SELECT current_version_id FROM meeting_transcripts WHERE graph_id=$1 AND source_id='updated'")
        .bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!(current, new);
    assert_eq!(state(&owner, old).await.0, "done");
    assert_eq!(state(&owner, new).await.0, "pending");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}
