//! Generic preserved-source extraction and durable cleanup with real local
//! scoped Postgres and mock transports. No network/model calls.
mod support;
use coralswarm_backend::{
    jobs::{
        queue::{self, JobRow},
        JobKind,
    },
    llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent},
    services::blob_store::PromotionBlobStore,
    source_evidence::{self, CaptureSource},
    store::PostgresStore,
    tenancy::{scope::begin_account_scoped, TenantId},
    workers::{source_atoms, source_blob_cleanup, tenant_harness::WorkerTenant},
};
use coralswarm_core::r2::R2Error;
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use uuid::Uuid;

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

/// The normal tick claims at most 20 jobs. Give its proof a private queue so
/// cleanup from earlier CI suites cannot displace this test's own job.
async fn cleanup_pools() -> Option<(coralswarm_backend::test_db::TestDatabase, PgPool, PgPool, coralswarm_backend::test_db::TestDatabase, PgPool)> {
    let (base_db, base_owner, base_app) = pools().await?;
    let db = coralswarm_backend::test_db::TestDatabase::new(base_owner.connect_options().as_ref(), "source_cleanup");
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&base_owner)
        .await
        .unwrap();
    let owner = PgPoolOptions::new()
        .connect_with(base_owner.connect_options().as_ref().clone().database(&db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
    let app = PgPoolOptions::new()
        .connect_with(base_app.connect_options().as_ref().clone().database(&db))
        .await
        .unwrap();
    base_app.close().await;
    Some((db, owner, app, base_db, base_owner))
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

struct Model {
    output: String,
    requests: Mutex<Vec<CompletionRequest>>,
    expected_tool: &'static str,
    stop_reason: &'static str,
    output_tokens: u32,
}
impl Model {
    fn new(output: Value) -> Self {
        Self {
            output: output.to_string(),
            requests: Mutex::new(Vec::new()),
            expected_tool: "emit_source_atoms_v2",
            stop_reason: "tool_use",
            output_tokens: 10,
        }
    }
}
#[async_trait::async_trait]
impl LlmProvider for Model {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        assert_eq!(req.tool_schema.as_ref().unwrap().name, self.expected_tool);
        assert!(req.tool_schema.as_ref().unwrap().strict);
        self.requests.lock().unwrap().push(req);
        Ok(CompletionResponse {
            content: self.output.clone(),
            input_tokens: 10,
            output_tokens: self.output_tokens,
            model: "mock-source-extractor".into(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: Some(self.stop_reason.into()),
            cost_usd: Some(0.00042),
        })
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        panic!("forced nonstreaming tool only")
    }
}
async fn capture(
    app: &PgPool,
    account: Uuid,
    graph: Uuid,
    kind: &str,
    provider: &str,
    text: &str,
    user: Uuid,
    representation: &str,
) -> source_evidence::CaptureOutcome {
    let mut tx = begin_account_scoped(app, account).await.unwrap();
    let capture = CaptureSource {
        source_type: kind,
        provider_namespace: provider,
        source_id: "native-shared-id",
        normalized_text: text,
        representation,
        metadata: &json!({"title":"Retained source"}),
        occurred_at: None,
        participants: &[],
        ingested_by: Some(user),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let saved = source_evidence::capture_source_version_tx(
        &mut tx, None, graph, account, &capture, None, &[42; 32],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    saved
}
async fn queued(pool: &PgPool, account: Uuid, version: Uuid, start: i64) -> JobRow {
    let (graph, actual_account, created) = support::managed_extraction::own_version(pool, version).await;
    assert_eq!(actual_account, account);
    if created {
        let mut tx = begin_account_scoped(pool, account).await.unwrap();
        source_evidence::queue_extraction_tx(&mut tx, graph, version, account, start).await.unwrap();
        tx.commit().await.unwrap();
    }
    let row:(Uuid,String,Value)=sqlx::query_as("SELECT id,kind,payload FROM jobs WHERE payload->>'source_version_id'=$1 AND (payload->>'start_char')::bigint=$2 AND status='pending' ORDER BY created_at DESC LIMIT 1")
        .bind(version.to_string()).bind(start).fetch_one(pool).await.unwrap();
    JobRow {
        id: row.0,
        kind: row.1,
        payload: row.2,
        attempts: 0,
        account_id: Some(account),
    }
}
async fn extract(app: &PgPool, owner: &PgPool, model: &Model, job: JobRow) {
    source_atoms::process_job(
        &TenantId::Consumer,
        app,
        owner,
        &PostgresStore::new(app.clone()),
        &coralswarm_backend::llm::testing::GatedScript(model),
        None,
        job,
    )
    .await;
}

#[tokio::test]
async fn source_atoms_capture_extracts_typed_nodes_with_durable_version_attribution() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account)
        .fetch_one(&owner)
        .await
        .unwrap();
    let other: Uuid =
        sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$2) RETURNING id")
            .bind(format!("other-{}@test.local", Uuid::new_v4()))
            .bind(Uuid::new_v4().to_string())
            .fetch_one(&owner)
            .await
            .unwrap();
    // The same native identifier belongs to distinct typed/workspace identities.
    let email = capture(
        &app,
        account,
        graph,
        "google_gmail",
        "mail:workspace-a",
        "🪸\r\nAlice will deliver Roadmap.",
        user,
        "normalized_text",
    )
    .await;
    let note = capture(
        &app,
        account,
        graph,
        "note",
        "notes:workspace-b",
        "Alice may deliver Roadmap.",
        other,
        "original_text",
    )
    .await;
    assert_ne!(email.source_item_id, note.source_item_id);
    let pinned:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,entity_type,entity_type_sticky) VALUES($1,'entity','Alice','org',true) RETURNING node_id")
        .bind(graph).fetch_one(&owner).await.unwrap();
    let model = Model::new(
        json!({"atoms":[{"kind":"action","text":"Alice will deliver Roadmap.","evidence_quote":"Alice will deliver Roadmap.","entities":[{"mention":"Alice","type":"person"},{"mention":"Roadmap","type":"project"}]}]}),
    );
    let job = queued(&owner, account, email.version_id, 0).await;
    assert_eq!(job.kind, "ExtractSourceAtoms");
    let id = job.id;
    extract(&app, &owner, &model, job).await;
    let attribution: Uuid = sqlx::query_scalar(
        "SELECT attributed_user_id FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v2'",
    )
    .bind(id)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(attribution, user);
    let candidates: Vec<Vec<String>> = sqlx::query_scalar("SELECT candidate_models FROM managed_attempt_receipts WHERE job_id=$1")
        .bind(id).fetch_all(&owner).await.unwrap();
    assert_eq!(candidates, vec![vec!["deepseek/deepseek-v4-flash-0731".to_string(), "deepseek/deepseek-v4.1-flash".to_string()]],
        "source extraction falls back within the DeepSeek family");
    let evidence: (i32, i32, String) = sqlx::query_as(
        "SELECT start_char,end_char,quote FROM semantic_atom_passages WHERE source_version_id=$1",
    )
    .bind(email.version_id)
    .fetch_one(&owner)
    .await
    .unwrap();
    assert_eq!(evidence, (3, 30, "Alice will deliver Roadmap.".into()));
    let typed:Vec<(String,String)>=sqlx::query_as("SELECT n.title,n.entity_type FROM nodes n JOIN node_semantic_atoms l USING(node_id) JOIN semantic_atoms a USING(semantic_atom_id) WHERE a.source_version_id=$1 ORDER BY n.title")
        .bind(email.version_id).fetch_all(&owner).await.unwrap();
    assert_eq!(
        typed,
        vec![("Alice".into(), "person".into()), ("Roadmap".into(), "project".into())],
        "contracted identity must keep the person separate from the pinned organization"
    );
    assert_eq!(
        sqlx::query_as::<_, (String, bool)>("SELECT entity_type,entity_type_sticky FROM nodes WHERE node_id=$1")
            .bind(pinned)
            .fetch_one(&owner)
            .await
            .unwrap(),
        ("org".into(), true)
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM node_semantic_atoms WHERE node_id=$1")
        .bind(pinned).fetch_one(&owner).await.unwrap(), 0, "same-name pinned organization must not acquire false person support");
    let person: (Uuid, Uuid, String, String) = sqlx::query_as(
        "SELECT n.node_id,n.identity_scope,n.entity_type,n.body->>'identity_status' FROM nodes n \
         JOIN node_semantic_atoms l USING(node_id) JOIN semantic_atoms a USING(semantic_atom_id) \
         WHERE a.source_version_id=$1 AND n.title='Alice'",
    ).bind(email.version_id).fetch_one(&owner).await.unwrap();
    assert_ne!(person.0, pinned);
    assert_eq!((person.1, person.2, person.3), (email.version_id, "person".into(), "unresolved".into()));
    let linked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM semantic_atoms a CROSS JOIN LATERAL jsonb_array_elements(a.entities) e WHERE a.source_version_id=$1 AND e->>'mention'='Alice' AND e->>'type'='person' AND e->>'status'='linked' AND e->'reason'='null'::jsonb)")
        .bind(email.version_id).fetch_one(&owner).await.unwrap();
    assert!(linked, "literal mention links to its distinct context-scoped identity while identity quality remains unresolved");
    let reqs = model.requests.lock().unwrap();
    assert!(reqs[0].messages[0]
        .content
        .contains("\"source_type\":\"google_gmail\""));
    assert!(reqs[0].messages[0].content.contains("normalized_text"));
    assert!(reqs[0].system.as_ref().unwrap().contains("uncertainty"));
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// Extraction v2 on the cohort (non-standing) path: same funded call, now with
/// relations and temporal validity; no Jev seat (only standing windows carry
/// one), and the node_atoms bridge still links every reef to the chunks.
#[tokio::test]
async fn source_atoms_v2_cohort_writes_relations_validity_and_bridge() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let text = "Ana Ruiz manages Bo Chen until 2026-12-31.";
    let saved = capture(&app, account, graph, "linear_issue", "composio:linear", text, user, "normalized_text").await;
    let chunk: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by) VALUES($1,'linear_issue','v2-chunk','',$2,now(),$3) RETURNING atom_id")
        .bind(graph).bind(text).bind(user).fetch_one(&owner).await.unwrap();
    let mut tx = owner.begin().await.unwrap();
    source_evidence::link_verified_chunks_tx(&mut tx, graph, saved.version_id, &[chunk]).await.unwrap();
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":"2026-12-31","validity_quote":"until 2026-12-31"});
    let mut model = Model::new(json!({"atoms":[{"kind":"fact","text":"Ana Ruiz manages Bo Chen until 2026-12-31.","evidence_quote":text,
        "entities":[{"mention":"Ana Ruiz","type":"person"},{"mention":"Bo Chen","type":"person"}],
        "relations":[rel("Ana Ruiz","manages","Bo Chen"), rel("Ana Ruiz","knows","Bo Chen"), rel("Bo Chen","expert_in","Ana Ruiz")],
        "valid_from":null,"valid_until":"2026-12-31","validity_quote":"until 2026-12-31"}]}));
    model.expected_tool = "emit_source_atoms_v2";
    let job = queued(&owner, account, saved.version_id, 0).await;
    assert_eq!(job.kind, "ExtractSourceAtoms");
    extract(&app, &owner, &model, job).await;
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(status, "done");
    let edges: Vec<(String, String, String, Option<chrono::DateTime<chrono::Utc>>, Value)> = sqlx::query_as(
        "SELECT e.predicate,s.title,d.title,e.valid_until,e.provenance FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes d ON d.node_id=e.dst_node_id WHERE e.graph_id=$1 ORDER BY 1")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(edges.iter().map(|e| (e.0.as_str(), e.1.as_str(), e.2.as_str())).collect::<Vec<_>>(),
        vec![("manages", "Ana Ruiz", "Bo Chen"), ("reports_to", "Bo Chen", "Ana Ruiz")], "retired and inference-only predicates are dropped");
    for edge in &edges {
        assert_eq!(edge.3.map(|d| d.date_naive().to_string()).as_deref(), Some("2026-12-31"));
        assert_eq!(edge.4["atom_ids"], json!([chunk.to_string()]));
    }
    let validity: Option<String> = sqlx::query_scalar("SELECT validity_quote FROM semantic_atoms WHERE source_version_id=$1")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(validity.as_deref(), Some("until 2026-12-31"));
    let bridged: i64 = sqlx::query_scalar("SELECT count(*) FROM node_atoms WHERE atom_id=$1").bind(chunk).fetch_one(&owner).await.unwrap();
    assert_eq!(bridged, 2, "both v2 reefs gain the chunk coral");
    let usage: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE task='source_atoms_v2'").fetch_one(&owner).await.unwrap();
    assert_eq!(usage, 1);
    let candidates: Vec<Vec<String>> = sqlx::query_scalar("SELECT candidate_models FROM managed_attempt_receipts WHERE source_version_id=$1")
        .bind(saved.version_id).fetch_all(&owner).await.unwrap();
    assert_eq!(candidates, vec![vec!["deepseek/deepseek-v4-flash-0731".to_string(), "deepseek/deepseek-v4.1-flash".to_string()]],
        "v2 extraction prices and sends its own reasoning-off fallback, not GLM");
    let reqs = model.requests.lock().unwrap();
    assert_eq!(reqs.len(), 1, "no Jev seat outside a standing window");
    assert!(reqs[0].messages[0].content.contains("\"source_date\":\""), "{}", reqs[0].messages[0].content);
    assert!(reqs[0].system.as_ref().unwrap().contains("Allowed predicates"));
    drop(reqs);
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// Extraction v3 on the cohort path, through the real gate and database: a
/// one-sentence change is recorded with its reefs and becomes a supersession
/// proposal in the same transaction; a relative date resolves against the
/// source's `occurred_at`; a single-valued employer relation is stated and
/// keyed; and a malformed change, an unbelievable date and a bad relation
/// each drop alone. One billed call; re-running the job changes nothing.
#[tokio::test]
async fn source_atoms_v3_cohort_records_changes_dates_and_single_valued_relations() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let (l1, l2, l3) = (
        "Atlas was previously owned by Bo Chen; since March Ana Ruiz owns it.",
        "Cy Lo works at Initech.",
        "Dee Park starts next week.",
    );
    let text = format!("{l1}\n{l2}\n{l3}");
    let saved = capture(&app, account, graph, "slack", "fixture:slack", &text, user, "original_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3',occurred_at='2026-10-19T15:00:00Z' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let change = |subject: &str| json!({"subject":subject,"attribute":"owner","old_value":"Bo Chen","new_value":"Ana Ruiz",
        "effective_from":"2026-03-01","effective_quote":"since March","quote":l1});
    let mut model = Model::new(json!({"atoms":[
        {"kind":"fact","text":"Ana Ruiz took over Atlas from Bo Chen in March.","evidence_quote":l1,
         "entities":[{"mention":"Atlas","type":"project"},{"mention":"Bo Chen","type":"person"},{"mention":"Ana Ruiz","type":"person"}],
         // A retired predicate, and the replaced owner as if still current: both drop alone.
         "relations":[rel("Ana Ruiz","knows","Bo Chen"), rel("Bo Chen","responsible_for","Atlas")],
         // A change about a subject the window never mentions drops alone.
         "changes":[change("Zed"), change("Atlas")],
         "valid_from":"2026-03-01","valid_until":null,"validity_quote":"since March"},
        {"kind":"fact","text":"Cy Lo works at Initech.","evidence_quote":l2,
         "entities":[{"mention":"Cy Lo","type":"person"},{"mention":"Initech","type":"org"}],
         "relations":[rel("Cy Lo","works_at","Initech")],"changes":[],
         "valid_from":null,"valid_until":null,"validity_quote":null},
        // "next week" resolved three years out: the date drops, the atom stays.
        {"kind":"fact","text":"Dee Park starts next week.","evidence_quote":l3,
         "entities":[{"mention":"Dee Park","type":"person"}],"relations":[],"changes":[],
         "valid_from":"2029-10-26","valid_until":null,"validity_quote":"next week"}
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    assert_eq!(job.kind, "ExtractSourceAtoms");
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(status, "done");
    let candidates: Vec<Vec<String>> = sqlx::query_scalar("SELECT candidate_models FROM managed_attempt_receipts WHERE job_id=$1")
        .bind(job_id).fetch_all(&owner).await.unwrap();
    assert_eq!(candidates, vec![vec!["deepseek/deepseek-v4-flash-0731".to_string(), "deepseek/deepseek-v4.1-flash".to_string()]],
        "v3 extraction shares v2's reasoning-off fallback");
    let node = |title: &'static str| {
        let owner = owner.clone();
        async move {
            sqlx::query_scalar::<_, Uuid>("SELECT node_id FROM nodes WHERE graph_id=$1 AND title=$2")
                .bind(graph).bind(title).fetch_one(&owner).await.unwrap()
        }
    };
    let (atlas, ana, bo) = (node("Atlas").await, node("Ana Ruiz").await, node("Bo Chen").await);

    // Atoms: all three kept; the relative date resolved, the far one dropped.
    let atoms: Vec<(Uuid, String, Option<chrono::DateTime<chrono::Utc>>, Option<String>, Option<Value>)> = sqlx::query_as(
        "SELECT semantic_atom_id,statement,valid_from,validity_quote,stated_changes FROM semantic_atoms WHERE source_version_id=$1 AND extraction_version='source-atoms-v3' ORDER BY ordinal")
        .bind(saved.version_id).fetch_all(&owner).await.unwrap();
    assert_eq!(atoms.len(), 3, "no bad item costs its atom");
    assert_eq!((atoms[0].2.map(|d| d.date_naive().to_string()), atoms[0].3.as_deref()), (Some("2026-03-01".into()), Some("since March")));
    assert_eq!((atoms[2].2, atoms[2].3.as_deref()), (None, None));
    let changes = atoms[0].4.as_ref().expect("the change is recorded");
    assert_eq!(changes.as_array().unwrap().len(), 1);
    let c = &changes[0];
    assert_eq!((c["state_key"].as_str(), c["subject_node_id"].clone(), c["new_node_id"].clone(), c["old_node_id"].clone()),
        (Some("rel.owner"), json!(atlas), json!(ana), json!(bo)));
    assert_eq!(c["effective_from"].as_str().map(|d| &d[..10]), Some("2026-03-01"));
    assert!(atoms[1].4.is_none() && atoms[2].4.is_none());

    // Edges: the new owner (dated by the change) and the employer; never the
    // replaced owner or a retired predicate.
    let edges: Vec<(String, String, String, Option<chrono::DateTime<chrono::Utc>>, Value)> = sqlx::query_as(
        "SELECT e.predicate,s.title,d.title,e.valid_from,e.provenance FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes d ON d.node_id=e.dst_node_id WHERE e.graph_id=$1 ORDER BY 1")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(edges.iter().map(|e| (e.0.as_str(), e.1.as_str(), e.2.as_str())).collect::<Vec<_>>(),
        vec![("responsible_for", "Ana Ruiz", "Atlas"), ("works_at", "Cy Lo", "Initech")]);
    assert_eq!(edges[0].3.map(|d| d.date_naive().to_string()).as_deref(), Some("2026-03-01"));
    assert!(edges.iter().all(|e| e.4["extraction_version"] == "source-atoms-v3"));

    // Supersession on the extraction path (dry run, the default): the
    // sentence's own pair is proposed; the single-valued relations are keyed.
    let decided: Vec<(String, Uuid, Uuid, String, String)> = sqlx::query_as(
        "SELECT state_key,superseder_atom_id,superseded_atom_id,rule,status FROM fact_supersession_edges WHERE graph_id=$1")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(decided, vec![("rel.owner".into(), atoms[0].0, atoms[0].0, "stated_change".into(), "proposed".into())]);
    let keyed: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT state_key,operation,value FROM temporal_evidence_records WHERE graph_id=$1 AND recording_method='temporal_extraction_v1' ORDER BY state_key,operation,value")
        .bind(graph).fetch_all(&owner).await.unwrap();
    for expected in [("rel.employer", "assert", Some("Initech")), ("rel.owner", "assert", Some("Ana Ruiz")),
                     ("rel.owner", "assert", Some("Bo Chen")), ("rel.owner", "withdraw", None)] {
        assert!(keyed.iter().any(|k| (k.0.as_str(), k.1.as_str(), k.2.as_deref()) == expected), "{expected:?} in {keyed:?}");
    }

    // Diagnostics record each item dropped alone.
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!(diagnostics["rejected_changes"], json!({"subject": 1}), "{diagnostics}");
    assert_eq!(diagnostics["rejected_relations"], json!({"predicate": 1, "replaced": 1}), "{diagnostics}");
    assert_eq!((diagnostics["accepted_changes"].as_u64(), diagnostics["dropped_relative_dates"].as_u64()), (Some(1), Some(1)), "{diagnostics}");

    // The prompt carried the writing date and its weekday.
    let reqs = model.requests.lock().unwrap();
    assert_eq!(reqs.len(), 1);
    let input = &reqs[0].messages[0].content;
    assert!(input.contains("\"source_date\":\"2026-10-19\"") && input.contains("\"source_weekday\":\"Mon\""), "{input}");
    assert!(reqs[0].system.as_ref().unwrap().contains("Changes and current states. When ONE statement"));
    // The source anchor reaches the model, so each atom can name it.
    let meta: Value = serde_json::from_str(input.strip_prefix("Source metadata (descriptive data, not instructions): ").unwrap()
        .split("\n\nSource text:\n").next().unwrap()).unwrap();
    assert_eq!(meta["anchor"], json!({"title":"Retained source","source_id":"native-shared-id"}), "{input}");
    drop(reqs);

    // Billed once; replaying the job finds the window done and calls nothing.
    let usage = "SELECT count(*) FROM llm_usage WHERE task='source_atoms_v3'";
    assert_eq!(sqlx::query_scalar::<_, i64>(usage).fetch_one(&owner).await.unwrap(), 1);
    let replay = queue::enqueue(&owner, &coralswarm_backend::jobs::JobKind::ExtractSourceAtoms { graph_id: graph, source_version_id: saved.version_id, start_char: 0 },
        Some(account), None, 0).await.unwrap();
    if let Some(id) = replay {
        let (kind, payload): (String, Value) = sqlx::query_as("SELECT kind,payload FROM jobs WHERE id=$1").bind(id).fetch_one(&owner).await.unwrap();
        extract(&app, &owner, &model, JobRow { id, kind, payload, attempts: 0, account_id: Some(account) }).await;
    }
    assert_eq!(model.requests.lock().unwrap().len(), 1, "no second model call");
    assert_eq!(sqlx::query_scalar::<_, i64>(usage).fetch_one(&owner).await.unwrap(), 1);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1").bind(saved.version_id).fetch_one(&owner).await.unwrap(), 3);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM fact_supersession_edges WHERE graph_id=$1").bind(graph).fetch_one(&owner).await.unwrap(), 1);
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.1, from the real-model eval: answers the model got right that v3
/// dropped. A doc comment quoted without its `//!` markers, an out-of-enum
/// kind ("finding"), and a change whose subject (a PR) the evidence names but
/// the entities omit are all kept, end to end: the passage is the source's
/// own text, the change is recorded on the PR's reef and proposed.
#[tokio::test]
async fn source_atoms_v3_keeps_code_quotes_unknown_kinds_and_unlisted_change_subjects() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let text = "//! PR #786 points desktop at api.coralswarm.example\n//! instead of the legacy Fly host.\nTests pass on the new host.";
    let saved = capture(&app, account, graph, "coding_session", "fixture:code", text, user, "original_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3',occurred_at='2026-10-19T15:00:00Z' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let said = "PR #786 points desktop at api.coralswarm.example instead of the legacy Fly host.";
    let mut model = Model::new(json!({"atoms":[
        {"kind":"finding","text":said,"evidence_quote":said,"entities":[],"relations":[],
         "changes":[{"subject":"PR #786","attribute":"api host","old_value":"the legacy Fly host","new_value":"api.coralswarm.example",
            "effective_from":null,"effective_quote":null,"quote":said}],
         "valid_from":null,"valid_until":null,"validity_quote":null},
        {"kind":"observation","text":"Tests pass on the new host.","evidence_quote":"Tests pass on the new host.","entities":[],"relations":[],"changes":[],
         "valid_from":null,"valid_until":null,"validity_quote":null}
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(status, "done");
    let atoms: Vec<(String, String, Option<Value>)> = sqlx::query_as(
        "SELECT a.kind,p.quote,a.stated_changes FROM semantic_atoms a JOIN semantic_atom_passages p USING(semantic_atom_id) \
          WHERE a.source_version_id=$1 ORDER BY a.ordinal")
        .bind(saved.version_id).fetch_all(&owner).await.unwrap();
    assert_eq!(atoms.len(), 2, "no atom is lost to its kind or its quoting: {atoms:?}");
    assert_eq!((atoms[0].0.as_str(), atoms[0].1.as_str()),
        ("observation", "PR #786 points desktop at api.coralswarm.example\n//! instead of the legacy Fly host."),
        "the passage is the source's own text, markers included");
    let change = &atoms[0].2.as_ref().expect("the change is recorded")[0];
    let pr: Uuid = sqlx::query_scalar("SELECT node_id FROM nodes WHERE graph_id=$1 AND title='PR #786'")
        .bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!((change["state_key"].as_str(), change["subject_node_id"].clone(), change["old_value"].as_str()),
        (Some("attr.api_host"), json!(pr), Some("the legacy Fly host")));
    let proposed: i64 = sqlx::query_scalar("SELECT count(*) FROM fact_supersession_edges WHERE graph_id=$1 AND rule='stated_change' AND status='proposed'")
        .bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!(proposed, 1, "the one sentence is proposed to supersession");
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((diagnostics["coerced_kinds"].as_u64(), diagnostics["adopted_subjects"].as_u64(), diagnostics["accepted_changes"].as_u64()),
        (Some(1), Some(1), Some(1)), "{diagnostics}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.2, end to end: a date and a change sentence the model took from just
/// outside the atom's evidence (same paragraph) are kept with the window's
/// own text and counted; a quote that dropped Markdown markup anchors; v3
/// asks for a 16k output budget. Red on v3.1: the date, the change and the
/// table atom are lost.
#[tokio::test]
async fn source_atoms_v3_keeps_paragraph_dates_change_sentences_and_markup_quotes() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let text = "**Since March**, Atlas moved from Bo Chen to Ana Ruiz; Ana Ruiz owns Atlas now.\n\n| Service | Host |\n| billing | `api.coralswarm.example` |";
    let saved = capture(&app, account, graph, "github_pull_request", "fixture:github", text, user, "normalized_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3',occurred_at='2026-10-19T15:00:00Z' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let sentence = "Since March, Atlas moved from Bo Chen to Ana Ruiz;";
    let mut model = Model::new(json!({"atoms":[
        {"kind":"fact","text":"Ana Ruiz owns Atlas since March.","evidence_quote":"Ana Ruiz owns Atlas now.",
         "entities":[{"mention":"Ana Ruiz","type":"person"},{"mention":"Atlas","type":"project"}],"relations":[],
         "changes":[{"subject":"Atlas","attribute":"owner","old_value":"Bo Chen","new_value":"Ana Ruiz",
            "effective_from":null,"effective_quote":null,"quote":sentence}],
         "valid_from":"2026-03-01","valid_until":null,"validity_quote":"Since March"},
        {"kind":"fact","text":"billing runs on api.coralswarm.example.","evidence_quote":"billing api.coralswarm.example",
         "entities":[],"relations":[],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null}
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    assert_eq!(model.requests.lock().unwrap()[0].max_tokens, 16_000, "v3's output budget");
    let atoms: Vec<(Option<chrono::DateTime<chrono::Utc>>, Option<String>, Option<Value>, String)> = sqlx::query_as(
        "SELECT a.valid_from,a.validity_quote,a.stated_changes,p.quote FROM semantic_atoms a JOIN semantic_atom_passages p USING(semantic_atom_id) \
          WHERE a.source_version_id=$1 ORDER BY a.ordinal")
        .bind(saved.version_id).fetch_all(&owner).await.unwrap();
    assert_eq!(atoms.len(), 2, "{atoms:?}");
    assert_eq!((atoms[0].0.map(|d| d.date_naive().to_string()), atoms[0].1.as_deref()), (Some("2026-03-01".into()), Some("Since March")),
        "the date from the evidence's own line");
    let change = &atoms[0].2.as_ref().expect("the change sentence is kept")[0];
    assert_eq!((change["quote"].as_str(), change["quote_in_evidence"].as_bool(), change["old_value"].as_str(), change["old_node_id"].clone()),
        (Some("Since March**, Atlas moved from Bo Chen to Ana Ruiz;"), Some(false), Some("Bo Chen"), Value::Null),
        "stored as the window's own text, outside the evidence, the old owner named by its words");
    assert!(change["quote_span"].as_array().is_some_and(|s| s.len() == 2));
    assert_eq!(atoms[1].3, "billing | `api.coralswarm.example", "the table quote anchors without its markup (the span ends at its last matched character)");
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((diagnostics["relaxed_dates"].as_u64(), diagnostics["relaxed_change_quotes"].as_u64(), diagnostics["accepted_changes"].as_u64()),
        (Some(1), Some(1), Some(1)), "{diagnostics}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.3: a code source's fixture people are not a family. A `parent_of`
/// the model reads out of test data in a `github_code` capture is dropped
/// (counted as `fixture`); since v3.4.2 so is the employment relation and
/// the people themselves (code atoms carry files, symbols and behaviour), and
/// the file path reaches the model as the anchor. Red on v3.2: the fixture
/// family edge is written.
#[tokio::test]
async fn source_atoms_v3_code_sources_never_write_fixture_families() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let text = "// fixture: Ana Ruiz is the parent of Bo Chen.\n// Bo Chen works at Initech.\nfn family() {}";
    let mut tx = begin_account_scoped(&app, account).await.unwrap();
    let capture = CaptureSource {
        source_type: "github_code",
        provider_namespace: "fixture:github",
        source_id: "repo:blob:backend/src/family.rs",
        normalized_text: text,
        representation: "original_text",
        metadata: &json!({"title":"backend/src/family.rs","code_path":"backend/src/family.rs"}),
        occurred_at: None,
        participants: &[],
        ingested_by: Some(user),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let saved = source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[42; 32]).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let mut model = Model::new(json!({"atoms":[
        {"kind":"fact","text":"backend/src/family.rs: Ana Ruiz is the parent of Bo Chen.","evidence_quote":"Ana Ruiz is the parent of Bo Chen.",
         "entities":[{"mention":"Ana Ruiz","type":"person"},{"mention":"Bo Chen","type":"person"}],
         "relations":[rel("Ana Ruiz","parent_of","Bo Chen")],
         "changes":[{"subject":"Bo Chen","attribute":"parent","old_value":null,"new_value":"Ana Ruiz","effective_from":null,"effective_quote":null,
            "quote":"Ana Ruiz is the parent of Bo Chen"}],
         "valid_from":null,"valid_until":null,"validity_quote":null},
        {"kind":"fact","text":"backend/src/family.rs: Bo Chen works at Initech.","evidence_quote":"Bo Chen works at Initech.",
         "entities":[{"mention":"Bo Chen","type":"person"},{"mention":"Initech","type":"org"}],
         "relations":[rel("Bo Chen","works_at","Initech")],"changes":[],
         "valid_from":null,"valid_until":null,"validity_quote":null}
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    let edges: Vec<String> = sqlx::query_scalar("SELECT predicate FROM relationship_edge WHERE graph_id=$1 ORDER BY 1")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert!(edges.is_empty(), "code states no family and no employment: {edges:?}");
    let stated: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1 AND stated_changes IS NOT NULL")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(stated, 0, "no fixture family change");
    let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1").bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(atoms, 2, "the atoms stay");
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((diagnostics["rejected_relations"]["fixture"].as_u64(), diagnostics["rejected_changes"]["fixture"].as_u64()), (Some(2), Some(1)), "{diagnostics}");
    assert_eq!(diagnostics["dropped_code_people"].as_u64(), Some(3), "three person mentions over two atoms: {diagnostics}");
    let input = model.requests.lock().unwrap()[0].messages[0].content.clone();
    assert!(input.contains("\"code_path\":\"backend/src/family.rs\""), "{input}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.2: test code reaches no model. A `github_code` file at a test path
/// makes no call (no model call reserved), keeps one "test file for X" atom
/// (so the coverage gate still counts it) and finishes its version; a Rust
/// file sends its window without its `#[cfg(test)]` module.
/// Red on v3.4.1: both windows send their test code.
#[tokio::test]
async fn source_atoms_v3_skips_test_files_and_cuts_rust_test_modules() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let code = |path: &'static str, text: &'static str| {
        let (app, owner) = (app.clone(), owner.clone());
        async move {
            let mut tx = begin_account_scoped(&app, account).await.unwrap();
            let source_id = format!("repo:blob:{path}");
            let metadata = json!({"title":path,"code_path":path});
            let capture = CaptureSource {
                source_type: "github_code",
                provider_namespace: "fixture:github",
                source_id: &source_id,
                normalized_text: text,
                representation: "original_text",
                metadata: &metadata,
                occurred_at: None,
                participants: &[],
                ingested_by: Some(user),
                scope: "private",
                duration_ms: None,
                segments: None,
            };
            let saved = source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[42; 32]).await.unwrap();
            tx.commit().await.unwrap();
            let mut tx = owner.begin().await.unwrap();
            sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
                .bind(saved.version_id).execute(&mut *tx).await.unwrap();
            assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
                .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
            tx.commit().await.unwrap();
            saved.version_id
        }
    };
    let calls = |version: Uuid| {
        let owner = owner.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT attempted_model_calls FROM corpus_enrichment_units WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND execution_owner")
                .bind(graph).bind(version).fetch_one(&owner).await.unwrap()
        }
    };
    let quote = "pub fn retry_once() -> bool { true }";
    let mut model = Model::new(json!({"atoms":[
        {"kind":"fact","text":"backend/src/retry.rs: retry_once returns true.","evidence_quote":quote,
         "entities":[{"mention":"retry_once","type":"topic"}],"relations":[],"changes":[],
         "valid_from":null,"valid_until":null,"validity_quote":null}]}));
    model.expected_tool = "emit_source_atoms_v3";

    let test_file = code("backend/tests/retry_pg.rs", "pub fn retry_once() -> bool { true }\n// Ana Ruiz is the parent of Bo Chen.\n").await;
    extract(&app, &owner, &model, queued(&owner, account, test_file, 0).await).await;
    assert!(model.requests.lock().unwrap().is_empty(), "a test file reaches no model");
    assert_eq!(version_state(&owner, test_file).await.0, "done");
    let atoms: Vec<(String, String)> = sqlx::query_as("SELECT kind,statement FROM semantic_atoms WHERE source_version_id=$1").bind(test_file).fetch_all(&owner).await.unwrap();
    assert_eq!(atoms, vec![("observation".to_string(), "backend/tests/retry_pg.rs: test file for retry_pg.".to_string())],
        "one test-file atom keeps the file extracted and findable");
    assert_eq!(stored_quotes(&owner, test_file).await, vec![quote.to_string()]);
    let edges: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge WHERE graph_id=$1").bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!((edges, calls(test_file).await), (0, 0), "no fixture family, no model call reserved");

    let source = "pub fn retry_once() -> bool { true }\n\n#[cfg(test)]\nmod tests {\n    // Ana Ruiz is the parent of Bo Chen.\n    #[test]\n    fn family() { assert!(super::retry_once()) }\n}\n";
    let file = code("backend/src/retry.rs", source).await;
    extract(&app, &owner, &model, queued(&owner, account, file, 0).await).await;
    let input = model.requests.lock().unwrap()[0].messages[0].content.clone();
    assert!(input.contains(quote) && !input.contains("Ana Ruiz") && !input.contains("mod tests"), "{input}");
    assert_eq!(version_state(&owner, file).await.0, "done");
    assert_eq!(stored_quotes(&owner, file).await, vec![quote.to_string()]);
    assert_eq!(calls(file).await, 1);
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.3: a Slack PR notification names who opened the PR, not who works
/// on it. Its `works_on` / `responsibility_of` are dropped (counted as
/// `authorship`); a stated "is working on" in the same window is written.
/// Red on v3.4.2: the notification's two edges are written.
#[tokio::test]
async fn source_atoms_v3_authorship_lines_write_no_involvement_edges() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let first = "Pull request opened by Dana Smith: #812 Fix login redirect";
    let second = "Bo Chen is working on #813 Cache warmup.";
    let third = "Ana Ruiz opened PR #876 Retry budget";
    let fourth = "main <- feat/retry-budget";
    let text = format!("{first}\n{second}\n{third}\n{fourth}");
    let saved = capture(&app, account, graph, "slack", "fixture:slack", &text, user, "normalized_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let mut model = Model::new(json!({"atoms":[
        {"kind":"fact","text":"Dana Smith opened PR #812 Fix login redirect.","evidence_quote":first,
         "entities":[{"mention":"Dana Smith","type":"person"},{"mention":"#812 Fix login redirect","type":"project"}],
         "relations":[rel("Dana Smith","works_on","#812 Fix login redirect"),rel("#812 Fix login redirect","responsibility_of","Dana Smith")],
         "changes":[],"valid_from":null,"valid_until":null,"validity_quote":null},
        {"kind":"fact","text":"Bo Chen is working on #813 Cache warmup.","evidence_quote":second,
         "entities":[{"mention":"Bo Chen","type":"person"},{"mention":"#813 Cache warmup","type":"project"}],
         "relations":[rel("Bo Chen","works_on","#813 Cache warmup")],
         "changes":[],"valid_from":null,"valid_until":null,"validity_quote":null},
        {"kind":"fact","text":"Ana Ruiz opened PR #876 Retry budget.","evidence_quote":third,
         "entities":[{"mention":"Ana Ruiz","type":"person"},{"mention":"PR #876 Retry budget","type":"project"}],
         "relations":[rel("PR #876 Retry budget","mentioned_by","Ana Ruiz")],
         "changes":[],"valid_from":null,"valid_until":null,"validity_quote":null},
        {"kind":"fact","text":"The PR merges feat/retry-budget into main.","evidence_quote":fourth,
         "entities":[{"mention":"main","type":"project"},{"mention":"feat/retry-budget","type":"project"}],
         "relations":[rel("main","mentions","feat/retry-budget")],
         "changes":[],"valid_from":null,"valid_until":null,"validity_quote":null}
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let edges: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.title,e.predicate,o.title FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes o ON o.node_id=e.dst_node_id WHERE e.graph_id=$1 AND e.predicate IN ('works_on','responsibility_of','worked_on_by','responsible_for','mentions','mentioned_by') ORDER BY 1,2")
        .bind(graph).fetch_all(&owner).await.unwrap();
    // Founder decision (2026-10-21): the author of a PR works on it; authoring
    // is not owning, and opening is not mentioning.
    let dana: Vec<_> = edges.iter().filter(|(s, _, o)| s.contains("Dana") || o.contains("Dana")).cloned().collect();
    assert_eq!(dana, vec![("Dana Smith".to_string(), "works_on".to_string(), "#812 Fix login redirect".to_string())], "{edges:?}");
    // v3.4.5: the opener's `mentioned_by` of her own PR is the `works_on` it means.
    let ana: Vec<_> = edges.iter().filter(|(s, _, o)| s.contains("Ana Ruiz") || o.contains("Ana Ruiz")).cloned().collect();
    assert_eq!(ana, vec![("Ana Ruiz".to_string(), "works_on".to_string(), "PR #876 Retry budget".to_string())], "{edges:?}");
    assert!(edges.iter().all(|(s, _, o)| s != "main" && o != "main"), "a PR header line is no mention: {edges:?}");
    assert!(edges.iter().any(|(s, p, _)| s == "Bo Chen" && p == "works_on"), "the stated involvement stays: {edges:?}");
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((diagnostics["rejected_relations"]["authorship"].as_u64(), diagnostics["rejected_relations"]["header"].as_u64(),
        diagnostics["author_mentions_as_work"].as_u64()), (Some(1), Some(1), Some(1)), "{diagnostics}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4, end to end: the answer's `atoms` comes as a JSON string (a Linear
/// comment window in prod) and is still extracted; a merge dated to the
/// source date (a PR's last update) loses that date; and an end date one
/// atom quotes is not borrowed by its neighbour. Red on v3.3: the window
/// fails as a schema error.
#[tokio::test]
async fn source_atoms_v3_parses_string_envelopes_and_keeps_only_dates_the_words_state() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let text = "PR #786 merged after review.\nAna Ruiz owns Atlas until 2026-12-31. Bo Chen owns Hermes.";
    let saved = capture(&app, account, graph, "github_pull_request", "fixture:github", text, user, "normalized_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3',occurred_at='2026-10-19T15:00:00Z' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let dated = |quote: &str, from: Option<&str>, until: Option<&str>, words: &str| json!({"kind":"fact","text":quote,"evidence_quote":quote,
        "entities":[],"relations":[],"changes":[],"valid_from":from,"valid_until":until,"validity_quote":words});
    let atoms = json!([
        // The model dated the merge to the PR's last update; the words do not.
        dated("PR #786 merged after review.", Some("2026-10-19"), None, "merged"),
        dated("Ana Ruiz owns Atlas until 2026-12-31.", None, Some("2026-12-31"), "until 2026-12-31"),
        dated("Bo Chen owns Hermes.", None, Some("2026-12-31"), "until 2026-12-31"),
    ]);
    let mut model = Model::new(json!({"atoms": atoms.to_string()}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    let status: String = sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(status, "done", "a stringified atoms array is the same answer");
    let rows: Vec<(Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>)> = sqlx::query_as(
        "SELECT valid_from,valid_until FROM semantic_atoms WHERE source_version_id=$1 ORDER BY ordinal")
        .bind(saved.version_id).fetch_all(&owner).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (None, None), "the merge is not dated to the PR's last update");
    assert_eq!(rows[1].1.map(|d| d.date_naive().to_string()).as_deref(), Some("2026-12-31"), "the atom that states its end keeps it");
    assert_eq!(rows[2], (None, None), "the neighbour does not borrow it");
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!(diagnostics["stringified_atoms"], json!(true), "{diagnostics}");
    assert_eq!((diagnostics["relations"]["dropped_source_dates"].as_u64(), diagnostics["relations"]["dropped_shared_dates"].as_u64()),
        (Some(1), Some(1)), "{diagnostics}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.2 (#1398 G2): a relation endpoint the text names but the model did
/// not list as a mention is kept at validation and bound graph-wide through
/// the door, `Resolved` only: here a reef this same source version already
/// carries (an earlier window, or the version's earlier extraction). The
/// same name reefed only by ANOTHER source stays unbound (the identity
/// contract, COR-36: a bare name in another evidence context is not the
/// same entity), and an unknown name is never minted. Red on v3.4.1: both
/// relations are dropped at validation.
#[tokio::test]
async fn source_atoms_v3_binds_named_relation_endpoints_to_existing_reefs_only() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let plain = |text: &str, rels: Value| json!({"kind":"fact","text":text,"evidence_quote":text,
        "entities":[{"mention":"Bo Chen","type":"person"}],"relations":rels,"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null});
    let text = "Bo Chen works at Initech. Bo Chen advises Hooli. Bo Chen used to work at Globex.";
    let saved = capture(&app, account, graph, "linear_issue", "fixture:linear", text, user, "normalized_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    // Initech: a reef this version already carries. Hooli: a reef another
    // source minted. Globex: no reef.
    let reef = |title: &'static str, scope: Uuid| {
        let owner = owner.clone();
        async move {
            sqlx::query_scalar::<_, Uuid>("INSERT INTO nodes(graph_id,type,title,body,identity_scope) VALUES($1,'entity',$2,jsonb_build_object('entity_type','org'),$3) RETURNING node_id")
                .bind(graph).bind(title).bind(scope).fetch_one(&owner).await.unwrap()
        }
    };
    let initech = reef("Initech", saved.version_id).await;
    reef("Hooli", Uuid::new_v4()).await;
    let mut model = Model::new(json!({"atoms":[
        plain("Bo Chen works at Initech.", json!([rel("Bo Chen","works_at","Initech")])),
        plain("Bo Chen advises Hooli.", json!([rel("Bo Chen","advises","Hooli")])),
        plain("Bo Chen used to work at Globex.", json!([rel("Bo Chen","works_at","Globex")]))]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!(diagnostics["unlisted_endpoints"].as_u64(), Some(3), "all three kept at validation: {diagnostics}");
    let edges: Vec<(String, String, Uuid)> = sqlx::query_as(
        "SELECT e.predicate,s.title,e.dst_node_id FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id WHERE e.graph_id=$1 AND e.predicate='works_at'")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(edges, vec![("works_at".to_string(), "Bo Chen".to_string(), initech)], "bound to the reef the door resolves");
    let advises: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge WHERE graph_id=$1 AND predicate IN ('advises','advised_by')")
        .bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!(advises, 0, "another source's same-named reef is not this entity (identity contract)");
    let globex: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1 AND lower(title)='globex'").bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!(globex, 0, "an unknown name is never minted");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.2, end to end on a PR capture: a recommended status is not stored;
/// a PR's own status is not given to the product; the PR itself may be the
/// subject without being named in the sentence; and an old value the text
/// never states is dropped while the change stays a current state. Red on
/// v3.4.1: the recommendation and the product-wide status are stored.
#[tokio::test]
async fn source_atoms_v3_keeps_only_stated_specific_changes() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let text = "We should close COR-65 once the eval passes.\nThis is not deployed yet.\nCOR-66 is now done.";
    let mut tx = begin_account_scoped(&app, account).await.unwrap();
    let capture = CaptureSource {
        source_type: "github_pull_request",
        provider_namespace: "fixture:github",
        source_id: "coralswarm/coralswarm#786",
        normalized_text: text,
        representation: "normalized_text",
        metadata: &json!({"title":"PR #786: point desktop at api","number":786}),
        occurred_at: None,
        participants: &[],
        ingested_by: Some(user),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let saved = source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[42; 32]).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let change = |subject: &str, old: Option<&str>, new: &str, quote: &str| json!({"subject":subject,"attribute":"status","old_value":old,
        "new_value":new,"effective_from":null,"effective_quote":null,"quote":quote});
    let atom = |quote: &str, ents: Value, changes: Value| json!({"kind":"fact","text":quote,"evidence_quote":quote,"entities":ents,
        "relations":[],"changes":changes,"valid_from":null,"valid_until":null,"validity_quote":null});
    let mut model = Model::new(json!({"atoms":[
        atom("We should close COR-65 once the eval passes.", json!([{"mention":"COR-65","type":"project"}]),
            json!([change("COR-65", None, "close", "We should close COR-65 once the eval passes.")])),
        atom("This is not deployed yet.", json!([]), json!([
            change("CoralSwarm", None, "not deployed", "This is not deployed yet."),
            change("PR #786", None, "not deployed", "This is not deployed yet.")])),
        atom("COR-66 is now done.", json!([{"mention":"COR-66","type":"project"}]),
            json!([change("COR-66", Some("in review"), "done", "COR-66 is now done.")])),
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    let stored: Vec<Value> = sqlx::query_scalar("SELECT stated_changes FROM semantic_atoms WHERE source_version_id=$1 AND stated_changes IS NOT NULL ORDER BY ordinal")
        .bind(saved.version_id).fetch_all(&owner).await.unwrap();
    let flat: Vec<(String, Option<String>, String)> = stored.iter().flat_map(|a| a.as_array().unwrap().iter().map(|c| (
        c["subject"].as_str().unwrap().to_owned(), c["old_value"].as_str().map(str::to_owned), c["new_value"].as_str().unwrap().to_owned()))).collect();
    assert_eq!(flat, vec![
        ("PR #786".to_string(), None, "not deployed".to_string()),
        ("COR-66".to_string(), None, "done".to_string()),
    ], "only stated, specific changes; the unstated old value is gone");
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((diagnostics["rejected_changes"]["tentative"].as_u64(), diagnostics["rejected_changes"]["subject"].as_u64()
            .or(diagnostics["rejected_changes"]["subject_scope"].as_u64()), diagnostics["dropped_old_values"].as_u64()),
        (Some(1), Some(1), Some(1)), "{diagnostics}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

#[tokio::test]
async fn source_atoms_pin_profiles_across_continuations_and_reject_metadata_quotes() {
    let Some((_db, owner, app)) = pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account)
        .fetch_one(&owner)
        .await
        .unwrap();
    for legacy in [false, true] {
        let text = "α🪸".repeat(4_001);
        let version = if legacy {
            let mut tx = begin_account_scoped(&app, account).await.unwrap();
            let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'coding_session','code:legacy','native-shared-id') RETURNING source_item_id")
                .bind(graph).fetch_one(&mut *tx).await.unwrap();
            let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,representation,source_metadata,ingested_by,scope,extraction_version,extraction_status) VALUES($1,$2,'native-shared-id','coding_session','code:legacy',$3,'generated_summary',$4,$5,'private','meeting-atoms-v1','pending') RETURNING version_id")
                .bind(graph).bind(item).bind(&text).bind(json!({"title":"Retained source"})).bind(user)
                .fetch_one(&mut *tx).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
                .bind(item).bind(version).execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            version
        } else {
            capture(
                &app,
                account,
                graph,
                "coding_session",
                "code:current",
                &text,
                user,
                "generated_summary",
            )
            .await
            .version_id
        };
        let mut model = Model::new(json!({"atoms":[]}));
        if legacy {
            model.expected_tool = "emit_meeting_atoms";
        }
        extract(
            &app,
            &owner,
            &model,
            queued(&owner, account, version, 0).await,
        )
        .await;
        let next = queued(&owner, account, version, 8000).await;
        assert_eq!(
            next.kind,
            if legacy {
                "ExtractMeetingAtoms"
            } else {
                "ExtractSourceAtoms"
            }
        );
        extract(&app, &owner, &model, next).await;
        let pins:Vec<String>=sqlx::query_scalar("SELECT DISTINCT extraction_version FROM semantic_atom_windows WHERE source_version_id=$1").bind(version).fetch_all(&owner).await.unwrap();
        assert_eq!(
            pins,
            vec![if legacy {
                "meeting-atoms-v1"
            } else {
                "source-atoms-v2"
            }]
        );
        let reqs = model.requests.lock().unwrap();
        assert_eq!(reqs.len(), 2);
        if !legacy {
            assert!(reqs[0].messages[0].content.contains("generated_summary"));
        }
    }
    let saved = capture(
        &app,
        account,
        graph,
        "document",
        "docs:workspace",
        "The source body has no header.",
        user,
        "normalized_text",
    )
    .await;
    let forged = Model::new(
        json!({"atoms":[{"kind":"fact","text":"Header is a source fact.","evidence_quote":"Source text:","entities":[]}]}),
    );
    extract(
        &app,
        &owner,
        &forged,
        queued(&owner, account, saved.version_id, 0).await,
    )
    .await;
    let status: String =
        sqlx::query_scalar("SELECT extraction_status FROM source_versions WHERE version_id=$1")
            .bind(saved.version_id)
            .fetch_one(&owner)
            .await
            .unwrap();
    // A prompt header is never evidence: the only atom is rejected, so the
    // window (retried once) fails rather than completing as if empty.
    assert_eq!(status, "failed");
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM semantic_atom_passages WHERE source_version_id=$1")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap(), 0);
    let diagnostics: Vec<Value> = sqlx::query_scalar("SELECT diagnostics FROM llm_usage WHERE source_job_id IN (SELECT id FROM jobs WHERE payload->>'source_version_id'=$1) AND task='source_atoms_v2' ORDER BY created_at")
        .bind(saved.version_id.to_string()).fetch_all(&owner).await.unwrap();
    assert_eq!(diagnostics.len(), 2);
    for row in &diagnostics {
        assert_eq!(row["extraction"]["rejected_atoms"]["quote_missing"], 1);
    }
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

#[tokio::test]
async fn source_atoms_synthetic_replay_persists_canonical_evidence_and_bounded_diagnostics() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let replay: Value = serde_json::from_str(include_str!("fixtures/source_atoms_replay_synthetic.json")).unwrap();
    for case in replay["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let text = case["source"].as_str().unwrap();
        let saved = capture(&app, account, graph, "note", name, text, user, "original_text").await;
        let model = Model::new(case["response"].clone());
        let job = queued(&owner, account, saved.version_id, 0).await;
        let job_id = job.id;
        extract(&app, &owner, &model, job).await;
        let status: (String, i64, Option<String>) = sqlx::query_as("SELECT extraction_status,next_start_char,extraction_error FROM source_versions WHERE version_id=$1")
            .bind(saved.version_id).fetch_one(&owner).await.unwrap();
        if let Some(error) = case["expected_error"].as_str() {
            // Nothing valid remained: the window is retried once, then fails.
            assert_eq!(status, ("failed".into(), 0, Some(error.into())), "{name}");
            assert_eq!(model.requests.lock().unwrap().len(), 2, "{name}");
            let attempts: (i64, i64) = sqlx::query_as("SELECT count(*),COALESCE(sum(settled_micro_usd),0)::bigint FROM managed_attempt_receipts WHERE job_id=$1 AND state='billed'")
                .bind(job_id).fetch_one(&owner).await.unwrap();
            assert_eq!(attempts, (2, 840), "{name}: each call billed once");
            let retry: Vec<Option<Value>> = sqlx::query_scalar("SELECT diagnostics->'extraction'->'window_retry' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v2' ORDER BY created_at")
                .bind(job_id).fetch_all(&owner).await.unwrap();
            assert_eq!(retry, vec![None, Some(json!(error))], "{name}");
            continue;
        }
        assert_eq!(status, ("done".into(), text.chars().count() as i64, None), "{name}");
        let passages: Vec<(i32, i32, String)> = sqlx::query_as("SELECT start_char,end_char,quote FROM semantic_atom_passages WHERE source_version_id=$1 ORDER BY start_char")
            .bind(saved.version_id).fetch_all(&owner).await.unwrap();
        let mut expected: Vec<&str> = case["expected_quotes"].as_array().unwrap().iter().map(|value| value.as_str().unwrap()).collect();
        // v2 coerces the unknown kind instead of rejecting the atom, so its
        // copy of the quote is kept too.
        if name == "mixed_schema_and_evidence_rejections" {
            expected.push("Bob waits.");
        }
        assert_eq!(passages.iter().map(|row| row.2.as_str()).collect::<Vec<_>>(), expected, "{name}");
        for (start, end, quote) in passages {
            assert_eq!(text.chars().skip(start as usize).take((end - start) as usize).collect::<String>(), quote, "{name}");
        }
        let (diagnostics, cost): (Value, f64) = sqlx::query_as("SELECT diagnostics,cost_usd::float8 FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v2'")
            .bind(job_id).fetch_one(&owner).await.unwrap();
        assert_eq!(cost, 0.00042);
        let expected_rejections = match name {
            "mixed_schema_and_evidence_rejections" => json!({"schema":2,"quote_missing":1}),
            _ => json!({}),
        };
        let mut expected_diagnostics = json!({
            "accepted_atoms":expected.len(),
            // The unsupported-mention atom survives without the mention and
            // then coalesces with its identical sibling.
            "duplicate_atoms":if name == "mixed_schema_and_evidence_rejections" {1} else {0},
            "whitespace_matches":if matches!(name, "ascii_whitespace_canonical_source" | "typographic_quotes_fold_to_source" | "edge_ellipsis_is_stripped") {1} else {0},
            "rejected_atoms":expected_rejections, "envelope_error":null,
            "relations":{"accepted_relations":0,"dropped_validity":0,"rejected_relations":{}}
        });
        if name == "mixed_schema_and_evidence_rejections" {
            expected_diagnostics["relations"]["coerced_kinds"] = json!(1);
        }
        if name == "unsupported_entity_drops_only_the_entity" {
            expected_diagnostics["dropped_entities"] = json!({"schema":1,"entity_unsupported":1});
        }
        assert_eq!(diagnostics["extraction"], expected_diagnostics, "{name}");
        let attempts: (i64, i64) = sqlx::query_as("SELECT count(*),COALESCE(sum(settled_micro_usd),0)::bigint FROM managed_attempt_receipts WHERE job_id=$1 AND state='billed'")
            .bind(job_id).fetch_one(&owner).await.unwrap();
        assert_eq!(attempts, (1, 420), "{name}");
        assert_eq!(model.requests.lock().unwrap().len(), 1, "{name}");
    }
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

#[tokio::test]
async fn source_atoms_invalid_envelopes_and_truncation_keep_paid_receipts_and_diagnostics() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account).fetch_one(&owner).await.unwrap();
    let valid_atom = json!({"kind":"fact","text":"Alice delivers.","evidence_quote":"Alice delivers.","entities":[]});
    // (name, response, stop, output tokens, stored error, envelope error,
    // calls, extra diagnostics): truncation (stated, or a full token budget
    // under a normal stop), an all-invalid answer and, since v3.4.3, a
    // malformed envelope are retried once (this window is too short to
    // shrink). A schema error records the field path, never content.
    for (name, response, stop_reason, output_tokens, expected_error, envelope, calls, extra) in [
        ("malformed", json!({}), "tool_use", 10, Some("invalid extraction schema"), json!("schema"), 2, json!({})),
        ("null_atoms", json!({"atoms":null}), "tool_use", 10, Some("invalid extraction schema"), json!("schema"), 2, json!({"schema_error_path":"atoms"})),
        ("truncated", json!({"atoms":[valid_atom.clone()]}), "length", 10, Some("truncated model response"), json!("truncated"), 2, json!({})),
        ("silently_truncated", json!({"atoms":[valid_atom.clone()]}), "tool_use", 8_000, Some("truncated model response"), json!("truncated"), 2, json!({"silent_truncation":true})),
        ("empty", json!({"atoms":[]}), "tool_use", 10, None, Value::Null, 1, json!({})),
        ("extra_envelope_field", json!({"atoms":[],"note":"none"}), "tool_use", 10, None, Value::Null, 1, json!({})),
        ("all_invalid", json!({"atoms":[{}]}), "tool_use", 10, Some("no valid atoms"), json!("no_valid_atoms"), 2, json!({})),
    ] {
        let saved = capture(&app, account, graph, "note", name, "Alice delivers.", user, "original_text").await;
        let mut model = Model::new(response);
        model.stop_reason = stop_reason;
        model.output_tokens = output_tokens;
        let job = queued(&owner, account, saved.version_id, 0).await;
        let job_id = job.id;
        extract(&app, &owner, &model, job).await;
        let status: (String, i64, Option<String>) = sqlx::query_as("SELECT extraction_status,next_start_char,extraction_error FROM source_versions WHERE version_id=$1")
            .bind(saved.version_id).fetch_one(&owner).await.unwrap();
        assert_eq!(status, if let Some(error) = expected_error { ("failed".into(), 0, Some(error.into())) }
            else { ("done".into(), 15, None) }, "{name}");
        let rows: Vec<(Value, f64)> = sqlx::query_as("SELECT diagnostics,cost_usd::float8 FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v2' ORDER BY created_at")
            .bind(job_id).fetch_all(&owner).await.unwrap();
        assert_eq!(rows.len(), calls, "{name}");
        for (index, (diagnostics, cost)) in rows.iter().enumerate() {
            assert_eq!(*cost, 0.00042, "{name}");
            assert_eq!(diagnostics["completion_reason"], "success", "validator rejection must not become a provider failure");
            let mut expected = json!({"accepted_atoms":0,"duplicate_atoms":0,"whitespace_matches":0,
                "rejected_atoms":if name == "all_invalid" {json!({"schema":1})} else {json!({})}, "envelope_error":envelope});
            if envelope.is_null() {
                expected["relations"] = json!({"accepted_relations":0,"dropped_validity":0,"rejected_relations":{}});
            }
            for (key, value) in extra.as_object().unwrap() {
                expected[key] = value.clone();
            }
            if index == 1 {
                expected["window_retry"] = json!(expected_error);
            }
            assert_eq!(diagnostics["extraction"], expected, "{name}");
        }
        assert_eq!(model.requests.lock().unwrap().len(), calls, "{name}");
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM semantic_atoms WHERE source_version_id=$1")
            .bind(saved.version_id).fetch_one(&owner).await.unwrap(), 0, "{name}");
        let attempts: (i64, i64) = sqlx::query_as("SELECT count(*),COALESCE(sum(settled_micro_usd),0)::bigint FROM managed_attempt_receipts WHERE job_id=$1 AND state='billed'")
            .bind(job_id).fetch_one(&owner).await.unwrap();
        assert_eq!(attempts, (calls as i64, 420 * calls as i64), "{name}: every call billed exactly once");
    }
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// A provider that answers from a script, one entry per call; `Err` entries
/// are transport failures as the gated transport reports them.
struct Scripted {
    script: Mutex<std::collections::VecDeque<Result<Value, LlmError>>>,
    requests: Mutex<Vec<CompletionRequest>>,
}
impl Scripted {
    fn new(script: Vec<Result<Value, LlmError>>) -> Self {
        Self { script: Mutex::new(script.into()), requests: Mutex::new(Vec::new()) }
    }
}
#[async_trait::async_trait]
impl LlmProvider for Scripted {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.requests.lock().unwrap().push(req);
        let next = self.script.lock().unwrap().pop_front().expect("unscripted model call");
        next.map(|output| CompletionResponse {
            content: output.to_string(),
            input_tokens: 10,
            output_tokens: 10,
            model: "mock-source-extractor".into(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: Some("tool_use".into()),
            cost_usd: Some(0.00042),
        })
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        panic!("forced nonstreaming tool only")
    }
}

async fn run_scripted(app: &PgPool, owner: &PgPool, model: &Scripted, job: JobRow) {
    source_atoms::process_job(
        &TenantId::Consumer,
        app,
        owner,
        &PostgresStore::new(app.clone()),
        &coralswarm_backend::llm::testing::GatedScript(model),
        None,
        job,
    )
    .await;
}

fn fact(quote: &str, entities: Value) -> Value {
    json!({"kind":"fact","text":format!("States: {quote}"),"evidence_quote":quote,"entities":entities})
}

async fn version_state(owner: &PgPool, version: Uuid) -> (String, i64, Option<String>) {
    sqlx::query_as("SELECT extraction_status,next_start_char,extraction_error FROM source_versions WHERE version_id=$1")
        .bind(version).fetch_one(owner).await.unwrap()
}

async fn stored_quotes(owner: &PgPool, version: Uuid) -> Vec<String> {
    sqlx::query_scalar("SELECT quote FROM semantic_atom_passages WHERE source_version_id=$1 ORDER BY start_char")
        .bind(version).fetch_all(owner).await.unwrap()
}

/// Receipts of one job: (billed, not generated, still reserved, settled micro-USD).
async fn receipts(owner: &PgPool, job: Uuid) -> (i64, i64, i64, i64) {
    sqlx::query_as("SELECT count(*) FILTER (WHERE state='billed'),count(*) FILTER (WHERE state='not_generated'),count(*) FILTER (WHERE state='reserved'),COALESCE(sum(settled_micro_usd),0)::bigint FROM managed_attempt_receipts WHERE job_id=$1")
        .bind(job).fetch_one(owner).await.unwrap()
}

/// One inexact quote costs its own atom, never the window's other atoms.
#[tokio::test]
async fn source_atoms_one_bad_quote_keeps_the_windows_other_atoms() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let text = "Alice will deliver Roadmap.\nBob waits for review.\nCara ships Friday.";
    let saved = capture(&app, account, graph, "slack", "one-bad-quote", text, user, "original_text").await;
    let model = Scripted::new(vec![Ok(json!({"atoms":[
        fact("Alice will deliver Roadmap.", json!([])),
        fact("Bob will wait for a review.", json!([])),
        fact("Cara ships Friday.", json!([])),
    ]}))]);
    let job = queued(&owner, account, saved.version_id, 0).await;
    run_scripted(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await, ("done".into(), text.chars().count() as i64, None));
    assert_eq!(stored_quotes(&owner, saved.version_id).await, ["Alice will deliver Roadmap.", "Cara ships Friday."]);
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// An unsupported or malformed mention is dropped on its own: the atom, its
/// other mentions and their reef links are kept. Unknown fields are ignored.
#[tokio::test]
async fn source_atoms_unsupported_mention_is_dropped_individually() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let text = "Alice will deliver Roadmap.";
    let saved = capture(&app, account, graph, "github_pr", "unsupported-mention", text, user, "original_text").await;
    let mut atom = fact(text, json!([{"mention":"Alice","type":"person"},{"mention":"Mallory","type":"person"},
        {"mention":"Roadmap","type":"project","confidence":0.8},{"mention":"Roadmap","type":"galaxy"}]));
    atom["confidence"] = json!(0.9);
    let model = Scripted::new(vec![Ok(json!({"atoms":[atom],"reasoning":"extra envelope field"}))]);
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    run_scripted(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let mentions: Value = sqlx::query_scalar("SELECT entities FROM semantic_atoms WHERE source_version_id=$1")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    let names: Vec<&str> = mentions.as_array().unwrap().iter().map(|m| m["mention"].as_str().unwrap()).collect();
    assert_eq!(names, ["Alice", "Roadmap"]);
    let linked: Vec<String> = sqlx::query_scalar("SELECT n.title FROM nodes n JOIN node_semantic_atoms l USING(node_id) JOIN semantic_atoms a USING(semantic_atom_id) WHERE a.source_version_id=$1 ORDER BY n.title")
        .bind(saved.version_id).fetch_all(&owner).await.unwrap();
    assert_eq!(linked, ["Alice", "Roadmap"], "Mallory never becomes a reef");
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v2'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!(diagnostics["dropped_entities"], json!({"schema":1,"entity_unsupported":1}));
    assert_eq!(diagnostics["accepted_atoms"], 1);
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// A window with no valid atom left still fails, after one retry, and each
/// of its two calls is billed exactly once.
#[tokio::test]
async fn source_atoms_all_bad_window_still_fails() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let saved = capture(&app, account, graph, "slack", "all-bad", "Alice will deliver Roadmap.", user, "original_text").await;
    let bad = json!({"atoms":[fact("Alice delivered it.", json!([])), fact("Bob agreed.", json!([]))]});
    let model = Scripted::new(vec![Ok(bad.clone()), Ok(bad)]);
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    run_scripted(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await, ("failed".into(), 0, Some("no valid atoms".into())));
    assert!(stored_quotes(&owner, saved.version_id).await.is_empty());
    assert_eq!(model.requests.lock().unwrap().len(), 2);
    assert_eq!(receipts(&owner, job_id).await, (2, 0, 0, 840));
    let unit: (String, Option<String>) = sqlx::query_as("SELECT state,reason FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(unit, ("failed".into(), Some("extraction_failed".into())));
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.3: a v3 Slack window whose every quote the window does not contain
/// (a near-empty message the model still wrote atoms for) is retried, then
/// finishes with no atoms instead of failing its version: a failed version
/// hides its item's earlier facts. The retry's diagnostics say so. A window
/// rejected for any other reason still fails. Red on v3.4.2: the version
/// fails with "no valid atoms".
#[tokio::test]
async fn source_atoms_v3_unanchorable_window_finishes_empty() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let v3 = |text: &'static str, provider: &'static str| {
        let (app, owner) = (app.clone(), owner.clone());
        async move {
            let saved = capture(&app, account, graph, "slack", provider, text, user, "original_text").await;
            let mut tx = owner.begin().await.unwrap();
            sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
                .bind(saved.version_id).execute(&mut *tx).await.unwrap();
            assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
                .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
            tx.commit().await.unwrap();
            saved.version_id
        }
    };
    let unanchored = json!({"atoms":[fact("Dana thanked the team for the launch.", json!([]))]});
    let version = v3("thanks all :tada:", "unanchorable").await;
    let model = Scripted::new(vec![Ok(unanchored.clone()), Ok(unanchored)]);
    let job = queued(&owner, account, version, 0).await;
    let job_id = job.id;
    run_scripted(&app, &owner, &model, job).await;
    assert_eq!(model.requests.lock().unwrap().len(), 2, "retried once first");
    assert_eq!(version_state(&owner, version).await, ("done".into(), 17, None), "the version finishes, empty");
    assert!(stored_quotes(&owner, version).await.is_empty());
    let rows: Vec<Value> = sqlx::query_scalar("SELECT diagnostics->'extraction' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3' ORDER BY created_at")
        .bind(job_id).fetch_all(&owner).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!((rows[0].get("unanchorable_window"), rows[1]["unanchorable_window"].as_bool()), (None, Some(true)), "{rows:?}");
    // Not every rejection is a quote: the window still fails.
    let mixed = json!({"atoms":[fact("Dana thanked the team for the launch.", json!([])), {}]});
    let version = v3("thanks all :tada:", "mixed").await;
    let model = Scripted::new(vec![Ok(mixed.clone()), Ok(mixed)]);
    run_scripted(&app, &owner, &model, queued(&owner, account, version, 0).await).await;
    assert_eq!(version_state(&owner, version).await, ("failed".into(), 0, Some("no valid atoms".into())));
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// Link ambiguity in the binder (v3.4.3, after its gate): a near match
/// binds the reef with the mention's exact title; a candidate that is a
/// shorter or longer form of the mention ("Atlas" for "Atlas rework",
/// "c2980b" for "c2980b CI") is a rival, so the mention stays unresolved and
/// nothing is minted (the gate found 16–29% of such mints were duplicates);
/// a person rival stays unresolved (COR-36). Red before the gate fix: "Atlas
/// rework" and "c2980b CI" were minted.
#[tokio::test]
async fn source_atoms_v3_near_matches_bind_exact_titles_and_never_mint_name_variants() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let reef = |title: &'static str, entity_type: &'static str, corals: i32| {
        let owner = owner.clone();
        async move {
            sqlx::query_scalar::<_, Uuid>("INSERT INTO nodes(graph_id,type,title,body,entity_type,coral_count,identity_scope) VALUES($1,'entity',$2,$3,$4,$5,gen_random_uuid()) RETURNING node_id")
                .bind(graph).bind(title).bind(json!({"entity_type": entity_type})).bind(entity_type).bind(corals)
                .fetch_one(&owner).await.unwrap()
        }
    };
    let atlas = reef("Atlas", "project", 40).await;
    let redesign = reef("Atlas scale redesign", "project", 1).await;
    reef("Bea Park", "person", 40).await;
    reef("c2980b", "topic", 40).await;
    let google = reef("Google", "project", 40).await;
    let text = "Bea said the Atlas scale redesign and the Atlas rework ship together after c2980b CI passes, synced to Google Tasks.";
    let saved = capture(&app, account, graph, "slack", "fixture:slack", text, user, "original_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let mut model = Model::new(json!({"atoms":[{"kind":"fact","text":"Bea said the Atlas scale redesign and the Atlas rework ship together after c2980b CI passes, synced to Google Tasks.","evidence_quote":text,
        "entities":[{"mention":"Bea","type":"person"},{"mention":"Atlas scale redesign","type":"project"},{"mention":"Atlas rework","type":"project"},{"mention":"c2980b CI","type":"topic"},{"mention":"Google Tasks","type":"project"}],
        "relations":[],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null}]}));
    model.expected_tool = "emit_source_atoms_v3";
    extract(&app, &owner, &model, queued(&owner, account, saved.version_id, 0).await).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let entities: Value = sqlx::query_scalar("SELECT entities FROM semantic_atoms WHERE source_version_id=$1").bind(saved.version_id).fetch_one(&owner).await.unwrap();
    let state = |mention: &str| {
        let e = entities.as_array().unwrap().iter().find(|e| e["mention"] == mention).unwrap();
        (e["status"].as_str().map(str::to_owned), e["reason"].as_str().map(str::to_owned), e["how"].as_str().map(str::to_owned))
    };
    assert_eq!(state("Bea"), (Some("unresolved".into()), Some("ambiguous".into()), Some("ladder".into())), "a person rival stays unresolved: {entities}");
    // #1516: a non-person exact title binds in the door's ladder (step 3a).
    assert_eq!(state("Atlas scale redesign"), (Some("linked".into()), None, Some("ladder".into())), "{entities}");
    let ambiguous = (Some("unresolved".into()), Some("ambiguous".into()), Some("ladder".into()));
    assert_eq!(state("Atlas rework"), ambiguous, "a narrower rival: never minted: {entities}");
    assert_eq!(state("c2980b CI"), ambiguous, "a narrower rival: never minted: {entities}");
    // A specific thing is not its generic parent: "Google Tasks" never binds
    // to "Google" (the prod repair dry run's false equivalence).
    assert_eq!(state("Google Tasks"), ambiguous, "{entities}");
    let minted: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1 AND title IN ('Atlas rework','c2980b CI','Google Tasks')").bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!(minted, 0, "no duplicate reef");
    let linked: Vec<(Uuid, String)> = sqlx::query_as("SELECT n.node_id,n.title FROM node_semantic_atoms l JOIN nodes n USING(node_id) JOIN semantic_atoms a USING(semantic_atom_id) WHERE a.source_version_id=$1 ORDER BY n.title")
        .bind(saved.version_id).fetch_all(&owner).await.unwrap();
    assert_eq!(linked.iter().map(|(_, t)| t.as_str()).collect::<Vec<_>>(), ["Atlas scale redesign"]);
    assert!(linked.iter().any(|(id, _)| *id == redesign) && !linked.iter().any(|(id, _)| *id == atlas || *id == google), "never the near match");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.4, relation precision (the v3.4.3 gate's errors): an authorship
/// verb on the line before the evidence still marks an opener, a PR title's
/// "work"/"build" does not make the opener an owner, "pushed a commit" is
/// authorship, and a predicate the linking words do not state is dropped
/// (`unsupported`): a copy is not an owner, storage is not a mention, a
/// workspace is not employment. A stated `works_at` stays. Red on v3.4.3:
/// every one of these edges is written.
#[tokio::test]
async fn source_atoms_v3_relations_need_their_words() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let lines = [
        "Pull request opened by Dana Smith",
        "#900 Build the work queue",
        "Ana Ruiz pushed a commit to #901 Retry budget.",
        "Atlas v2 is a copy of Atlas.",
        "The deploy key secret is stored in Vault.",
        "Acme has a CoralSwarm workspace.",
        "Bo Chen works at Initech.",
        "deploy-agent closed #902 Cache warmup.",
        "dependabot[bot] opened #903 Bump serde.",
        "COR-7 started on Opus, which is working on it.",
    ];
    let text = lines.join("\n");
    let saved = capture(&app, account, graph, "slack", "fixture:slack", &text, user, "original_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let atom = |quote: &str, entities: Value, relation: Value| json!({"kind":"fact","text":quote,"evidence_quote":quote,
        "entities":entities,"relations":[relation],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null});
    let p = |m: &str| json!({"mention":m,"type":"person"});
    let o = |m: &str| json!({"mention":m,"type":"org"});
    let j = |m: &str| json!({"mention":m,"type":"project"});
    let mut model = Model::new(json!({"atoms":[
        atom(lines[1], json!([j("#900 Build the work queue")]), rel("Dana Smith","works_on","#900 Build the work queue")),
        atom(lines[2], json!([p("Ana Ruiz"), j("#901 Retry budget")]), rel("Ana Ruiz","works_on","#901 Retry budget")),
        atom(lines[3], json!([j("Atlas v2"), j("Atlas")]), rel("Atlas v2","responsibility_of","Atlas")),
        atom(lines[4], json!([j("deploy key"), j("Vault")]), rel("deploy key","mentions","Vault")),
        atom(lines[5], json!([o("Acme"), j("CoralSwarm")]), rel("Acme","works_at","CoralSwarm")),
        atom(lines[6], json!([p("Bo Chen"), o("Initech")]), rel("Bo Chen","works_at","Initech")),
        atom(lines[7], json!([j("deploy-agent"), j("#902 Cache warmup")]), rel("deploy-agent","mentions","#902 Cache warmup")),
        atom(lines[8], json!([p("dependabot[bot]"), j("#903 Bump serde")]), rel("dependabot[bot]","works_on","#903 Bump serde")),
        atom(lines[9], json!([j("Opus"), j("COR-7")]), rel("Opus","works_on","COR-7")),
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let edges: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.title,e.predicate,o.title FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes o ON o.node_id=e.dst_node_id WHERE e.graph_id=$1 AND e.predicate IN ('works_on','responsibility_of','responsible_for','mentions','works_at') ORDER BY 1,2")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(edges, vec![("Bo Chen".to_string(), "works_at".to_string(), "Initech".to_string())], "only the stated employment is written");
    let d: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((d["rejected_relations"]["authorship"].as_u64(), d["rejected_relations"]["unsupported"].as_u64()), (Some(3), Some(3)), "pushed, a closer, a bot; copy, workspace, a model: {d}");
    assert_eq!(d["rejected_relations"]["not_a_reference"].as_u64(), Some(1), "storage is no reference (v3.4.6 drops it first): {d}");
    // The PR's author (named on the line before, not a mention of this
    // atom) is kept: accepted, and bound only to an existing reef.
    assert_eq!((d["accepted_relations"].as_u64(), d["unlisted_endpoints"].as_u64()), (Some(2), Some(1)), "{d}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.5 (the v3.4.4 gate's lost and wrong relations): a Slack "Pull
/// request opened by dana: <title>" and "Ana Ruiz's own PR #877" keep the
/// author's `works_on` (title-only PR, untyped handle), an author's
/// `mentioned_by` of their own PR becomes that `works_on`, and a branch, a
/// commit-list entry and a file inside its own path are not mentions. Red
/// on v3.4.4: the authors' relations are dropped and the three mentions
/// written.
#[tokio::test]
async fn source_atoms_v3_authors_work_on_their_prs_and_mentions_are_references() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let lines = [
        "Pull request opened by dana: Fix login redirect",
        "Ana Ruiz's own PR #877 Retry budget is ready.",
        "PR #878 merges feat/cache into main",
        "- 3e6bc96a fix: retry budget for COR-65",
        "Edits backend/src/ocean_health.rs",
        // v3.4.6: the real Slack notification form, HTML-escaped link markup.
        "Pull request opened by &lt;https://github.com/example-founder|example-founder&gt;",
        "&lt;https://github.com/coralswarm/coralswarm/pull/9|#9 Retry jitter&gt;",
        "example-founder pushed a commit to main",
        "Opus is served on CoreWeave",
    ];
    let text = lines.join("\n");
    let saved = capture(&app, account, graph, "slack", "fixture:slack", &text, user, "original_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let atom = |quote: &str, entities: Value, relation: Value| json!({"kind":"fact","text":quote,"evidence_quote":quote,
        "entities":entities,"relations":[relation],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null});
    let e = |m: &str, t: &str| json!({"mention":m,"type":t});
    let mut model = Model::new(json!({"atoms":[
        atom(lines[0], json!([e("dana","topic"), e("Fix login redirect","project")]), rel("Fix login redirect","mentioned_by","dana")),
        atom(lines[1], json!([e("Ana Ruiz","person"), e("PR #877 Retry budget","project")]), rel("Ana Ruiz","works_on","PR #877 Retry budget")),
        atom(lines[2], json!([e("PR #878","project"), e("feat/cache","project")]), rel("PR #878","mentions","feat/cache")),
        atom(lines[3], json!([e("retry budget","topic"), e("COR-65","project")]), rel("retry budget","mentions","COR-65")),
        atom(lines[4], json!([e("ocean_health.rs","project"), e("backend/src/ocean_health.rs","project")]), rel("ocean_health.rs","mentions","backend/src/ocean_health.rs")),
        atom(lines[6], json!([e("#9 Retry jitter","project")]), rel("example-founder","works_on","#9 Retry jitter")),
        atom(lines[7], json!([e("example-founder","person"), e("main","project")]), rel("example-founder","parent_of","main")),
        atom(lines[8], json!([e("Opus","topic"), e("CoreWeave","org")]), rel("Opus","mentions","CoreWeave")),
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let edges: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.title,e.predicate,o.title FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes o ON o.node_id=e.dst_node_id WHERE e.graph_id=$1 AND e.predicate IN ('works_on','mentions','mentioned_by') ORDER BY 1,2")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(edges, vec![
        ("Ana Ruiz".to_string(), "works_on".to_string(), "PR #877 Retry budget".to_string()),
        ("dana".to_string(), "works_on".to_string(), "Fix login redirect".to_string()),
        ("example-founder".to_string(), "works_on".to_string(), "#9 Retry jitter".to_string()),
    ], "the authors work on their PRs; no branch, commit, path or hosting mention");
    let d: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((d["author_mentions_as_work"].as_u64(), d["rejected_relations"]["not_a_reference"].as_u64()), (Some(1), Some(4)), "branch, commit, path, hosting: {d}");
    // The escaped Slack author works on the PR on the next line (example-founder is
    // a mention of another atom of the window), and a commit to a branch is
    // no family.
    assert_eq!((d["accepted_relations"].as_u64(), d["rejected_relations"]["unsupported"].as_u64()), (Some(3), Some(1)), "{d}");
    let kin: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge WHERE graph_id=$1 AND predicate='parent_of'").bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!(kin, 0);
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.7 (v3.4.6 gate): a possessive author ("Alex's own PR #287") works on
/// that PR only, not on a "decision still pending Alex" in the same context;
/// a list entry and a code call are references. Red on v3.4.6: Alex
/// works_on the decision, and both mentions are dropped.
#[tokio::test]
async fn source_atoms_v3_possessive_author_names_only_its_item() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let lines = [
        "Alex's own PR #287 lands the escalate rule.",
        "Ballot & Propositions escalate-rule decision still pending Alex",
        "- #290 (3e6bc96a): sets the retry budget",
        "render_digest calls fetch_corals",
    ];
    let text = lines.join("\n");
    let saved = capture(&app, account, graph, "coding_session", "fixture:session", &text, user, "original_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let atom = |quote: &str, entities: Value, relation: Value| json!({"kind":"fact","text":quote,"evidence_quote":quote,
        "entities":entities,"relations":[relation],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null});
    let e = |m: &str, t: &str| json!({"mention":m,"type":t});
    let mut model = Model::new(json!({"atoms":[
        atom(lines[0], json!([e("Alex","person"), e("PR #287","project")]), rel("Alex","works_on","PR #287")),
        atom(lines[1], json!([e("Alex","person"), e("Ballot & Propositions escalate-rule decision","project")]), rel("Alex","works_on","Ballot & Propositions escalate-rule decision")),
        atom(lines[2], json!([e("#290","project"), e("retry budget","topic")]), rel("#290","mentions","retry budget")),
        atom(lines[3], json!([e("render_digest","topic"), e("fetch_corals","topic")]), rel("render_digest","mentions","fetch_corals")),
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    extract(&app, &owner, &model, queued(&owner, account, saved.version_id, 0).await).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let edges: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.title,e.predicate,o.title FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes o ON o.node_id=e.dst_node_id WHERE e.graph_id=$1 AND e.predicate IN ('works_on','mentions') ORDER BY 1,2,3")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(edges, vec![
        ("#290".to_string(), "mentions".to_string(), "retry budget".to_string()),
        ("render_digest".to_string(), "mentions".to_string(), "fetch_corals".to_string()),
        ("Alex".to_string(), "works_on".to_string(), "PR #287".to_string()),
    ], "Alex works on his own PR only; the list entry and the call are references");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.8 (v3.4.7 gate): in a PR's own body, the PR mentions what it plainly
/// names ("release v638"); the issue author works on the issue, not on the
/// table its title discusses; and "Morgan owns pagination validation"
/// stored inverted is written Morgan -> pagination validation. Red on
/// v3.4.7: the PR's mention is dropped, the author works on the table, and
/// the inverted owner is dropped.
#[tokio::test]
async fn source_atoms_v3_source_item_mentions_authored_items_and_owner_direction() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let lines = [
        "PR #640: Release v638 rolls the extraction worker.",
        "Issue opened by Dana: Add an index to the graphs table",
        "Morgan owns pagination validation.",
    ];
    let text = lines.join("\n");
    let mut tx = begin_account_scoped(&app, account).await.unwrap();
    let metadata = json!({"title":"Ship v638","number":640});
    let capture = CaptureSource {
        source_type: "github_pull_request", provider_namespace: "fixture:github", source_id: "repo:pull:640",
        normalized_text: &text, representation: "original_text", metadata: &metadata, occurred_at: None,
        participants: &[], ingested_by: Some(user), scope: "private", duration_ms: None, segments: None,
    };
    let saved = source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[42; 32]).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    let _ = sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let atom = |quote: &str, entities: Value, relation: Value| json!({"kind":"fact","text":quote,"evidence_quote":quote,
        "entities":entities,"relations":[relation],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null});
    let e = |m: &str, t: &str| json!({"mention":m,"type":t});
    let mut model = Model::new(json!({"atoms":[
        atom(lines[0], json!([e("PR #640","project"), e("Release v638","topic")]), rel("PR #640","mentions","Release v638")),
        atom(lines[1], json!([e("Dana","person"), e("graphs table","topic")]), rel("Dana","works_on","graphs table")),
        atom(lines[2], json!([e("Morgan","person"), e("pagination validation","topic")]), rel("pagination validation","responsible_for","Morgan")),
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let edges: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.title,e.predicate,o.title FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes o ON o.node_id=e.dst_node_id WHERE e.graph_id=$1 AND e.predicate IN ('works_on','mentions','responsible_for') ORDER BY 1,2,3")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(edges, vec![
        ("Morgan".to_string(), "responsible_for".to_string(), "pagination validation".to_string()),
        ("PR #640".to_string(), "mentions".to_string(), "Release v638".to_string()),
    ], "the PR names its release; no author on a discussed table; the owner the right way round");
    let d: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!(d["reoriented_ownership"].as_u64(), Some(1), "{d}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.4.9 (v3.4.8 gate): an "owner" CHANGE with subject and value swapped
/// made its edge directly; edges from relational changes now pass the same
/// guards (here: reorientation). Ownership of a part of an item moves to the
/// part, and "found by" is no mention. Red on v3.4.8: the change writes
/// pagination validation responsible_for Morgan, Morgan owns DEMO-1, and
/// COR-92 mentioned_by the COR-91 plan.
#[tokio::test]
async fn source_atoms_v3_change_edges_pass_the_relation_guards() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let lines = [
        "Morgan owns pagination validation now.",
        "Morgan owns the retry budget part of DEMO-1.",
        "COR-92 was found by the COR-91 plan.",
    ];
    let text = lines.join("\n");
    let saved = capture(&app, account, graph, "linear_issue", "fixture:linear", &text, user, "original_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let e = |m: &str, t: &str| json!({"mention":m,"type":t});
    // The swapped change: subject Morgan, owner = pagination validation.
    let swapped = json!({"subject":"Morgan","attribute":"owner","old_value":null,"new_value":"pagination validation",
        "effective_from":null,"effective_quote":null,"quote":lines[0]});
    let mut model = Model::new(json!({"atoms":[
        {"kind":"fact","text":lines[0],"evidence_quote":lines[0],"entities":[e("Morgan","person"), e("pagination validation","topic")],
         "relations":[],"changes":[swapped],"valid_from":null,"valid_until":null,"validity_quote":null},
        {"kind":"fact","text":lines[1],"evidence_quote":lines[1],"entities":[e("Morgan","person"), e("retry budget","topic"), e("DEMO-1","project")],
         "relations":[rel("Morgan","responsible_for","DEMO-1")],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null},
        {"kind":"fact","text":lines[2],"evidence_quote":lines[2],"entities":[e("COR-92","project"), e("COR-91 plan","project")],
         "relations":[rel("COR-92","mentioned_by","COR-91 plan")],"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null},
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let edges: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.title,e.predicate,o.title FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes o ON o.node_id=e.dst_node_id WHERE e.graph_id=$1 AND e.predicate IN ('responsible_for','mentions','mentioned_by') ORDER BY 1,2,3")
        .bind(graph).fetch_all(&owner).await.unwrap();
    assert_eq!(edges, vec![
        ("Morgan".to_string(), "responsible_for".to_string(), "pagination validation".to_string()),
        ("Morgan".to_string(), "responsible_for".to_string(), "retry budget".to_string()),
    ], "the change's owner the right way round; the part, not DEMO-1; no found-by mention");
    let d: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((d["reoriented_ownership"].as_u64(), d["narrowed_ownership"].as_u64(), d["rejected_relations"]["not_a_reference"].as_u64()),
        (Some(1), Some(1), Some(1)), "{d}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.5 (v3.4.9 gate) in one issue body: the finder references its PR
/// ("found by the bench (PR #1256)"); "Morgan owns pagination validation"
/// is not Morgan owning the issue (DEMO-1); a "red on main" example is no
/// fact; an offer elsewhere in the sentence is no work. Red on v3.4.9: the
/// bench mention drops and the other three are written.
#[tokio::test]
async fn source_atoms_v3_found_by_parts_examples_and_offers() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let lines = [
        "DEMO-1: Paginate the API",
        "Morgan owns pagination validation.",
        "The regression was found by the bench (PR #1256).",
        "Red on main: Dana works on the graphs table.",
        "Offered to the team: Alex works on the drift check.",
    ];
    let text = lines.join("\n");
    let mut tx = begin_account_scoped(&app, account).await.unwrap();
    let metadata = json!({"title":"Paginate the API","identifier":"DEMO-1"});
    let capture = CaptureSource {
        source_type: "linear_issue", provider_namespace: "fixture:linear", source_id: "DEMO-1",
        normalized_text: &text, representation: "original_text", metadata: &metadata, occurred_at: None,
        participants: &[], ingested_by: Some(user), scope: "private", duration_ms: None, segments: None,
    };
    let saved = source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[42; 32]).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    let _ = sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let e = |m: &str, t: &str| json!({"mention":m,"type":t});
    let atom = |quote: &str, entities: Value, relations: Value| json!({"kind":"fact","text":quote,"evidence_quote":quote,
        "entities":entities,"relations":relations,"changes":[],"valid_from":null,"valid_until":null,"validity_quote":null});
    let mut model = Model::new(json!({"atoms":[
        atom(lines[1], json!([e("Morgan","person"), e("pagination validation","topic")]),
            json!([rel("Morgan","responsible_for","pagination validation"), rel("Morgan","responsible_for","DEMO-1")])),
        atom(lines[2], json!([e("bench","topic"), e("PR #1256","project")]), json!([rel("bench","mentions","PR #1256")])),
        atom(lines[3], json!([e("Dana","person"), e("graphs table","topic")]), json!([rel("Dana","works_on","graphs table")])),
        atom(lines[4], json!([e("Alex","person"), e("drift check","topic")]), json!([rel("Alex","works_on","drift check")])),
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let edges: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.title,e.predicate,o.title FROM relationship_edge e JOIN nodes s ON s.node_id=e.src_node_id JOIN nodes o ON o.node_id=e.dst_node_id WHERE e.graph_id=$1 AND e.predicate IN ('responsible_for','mentions','works_on') ORDER BY 1,2,3")
        .bind(graph).fetch_all(&owner).await.unwrap();
    let mut edges = edges;
    edges.sort();
    assert_eq!(edges, vec![
        ("Morgan".to_string(), "responsible_for".to_string(), "pagination validation".to_string()),
        ("bench".to_string(), "mentions".to_string(), "PR #1256".to_string()),
    ], "the part's owner and the finder's PR; not the issue, the example or the offer");
    let d: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((d["rejected_relations"]["example_or_hypothetical"].as_u64(), d["rejected_relations"]["unsupported"].as_u64()), (Some(1), Some(2)),
        "the example; the issue owner and the offer: {d}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// v3.5.1 (eval k02): "Merged #1420; backend is now v702 (was v701)" was
/// stored as #1420's status, so no supersession linked backend v701 ->
/// v702 and Ask answered an older version. It is the backend's version.
/// Red on v3.5: subject #1420, attr.status.
#[tokio::test]
async fn source_atoms_v3_release_note_versions_bind_to_the_deployed_thing() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let note = "Merged #1420; backend is now v702 (was v701)";
    let saved = capture(&app, account, graph, "slack", "fixture:slack", note, user, "original_text").await;
    let mut tx = owner.begin().await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3' WHERE version_id=$1")
        .bind(saved.version_id).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph).bind(saved.version_id).fetch_one(&mut *tx).await.unwrap());
    tx.commit().await.unwrap();
    let change = json!({"subject":"#1420","attribute":"status","old_value":"v701","new_value":"v702",
        "effective_from":null,"effective_quote":null,"quote":note});
    let mut model = Model::new(json!({"atoms":[
        {"kind":"fact","text":"#1420 merged; the backend is on v702.","evidence_quote":note,
         "entities":[{"mention":"#1420","type":"project"}],"relations":[],"changes":[change],
         "valid_from":null,"valid_until":null,"validity_quote":null},
    ]}));
    model.expected_tool = "emit_source_atoms_v3";
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    extract(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await.0, "done");
    let stored: Value = sqlx::query_scalar("SELECT stated_changes FROM semantic_atoms WHERE source_version_id=$1 AND stated_changes IS NOT NULL")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    let c = &stored[0];
    assert_eq!((c["subject"].as_str(), c["state_key"].as_str(), c["old_value"].as_str(), c["new_value"].as_str()),
        (Some("backend"), Some("attr.version"), Some("v701"), Some("v702")), "{stored}");
    let d: Value = sqlx::query_scalar("SELECT diagnostics->'extraction'->'relations' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v3'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!(d["rebound_versions"].as_u64(), Some(1), "{d}");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// Code captures still pinned to v2 (the backlog drain) get v3's code rules:
/// a test file reaches no model, and a fixture family in a source file's
/// comments writes no kinship edge (prod 2026-10-22: 30 such edges from v2,
/// out of `backend/tests/*` and `relationships/inference.rs`). Red on main:
/// the test file is sent to the model and `parent_of` is written.
#[tokio::test]
async fn source_atoms_v2_code_sources_never_write_fixture_families() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let code = |path: &'static str, text: &'static str| {
        let app = app.clone();
        async move {
            let mut tx = begin_account_scoped(&app, account).await.unwrap();
            let source_id = format!("repo:blob:{path}");
            let metadata = json!({"title":path,"code_path":path});
            let capture = CaptureSource {
                source_type: "github_code", provider_namespace: "fixture:github", source_id: &source_id,
                normalized_text: text, representation: "original_text", metadata: &metadata, occurred_at: None,
                participants: &[], ingested_by: Some(user), scope: "private", duration_ms: None, segments: None,
            };
            let saved = source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[42; 32]).await.unwrap();
            tx.commit().await.unwrap();
            saved.version_id
        }
    };
    let text = "// fixture: Ana Ruiz is the parent of Bo Chen.\nfn family() {}";
    let quote = "Ana Ruiz is the parent of Bo Chen.";
    let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
    let mut model = Model::new(json!({"atoms":[
        {"kind":"fact","text":"inference.rs: Ana Ruiz is the parent of Bo Chen.","evidence_quote":quote,
         "entities":[{"mention":"Ana Ruiz","type":"person"},{"mention":"Bo Chen","type":"person"}],
         "relations":[rel("Ana Ruiz","parent_of","Bo Chen")],"valid_from":null,"valid_until":null,"validity_quote":null},
    ]}));
    model.expected_tool = "emit_source_atoms_v2";
    let test_file = code("backend/tests/relationship_generality.rs", text).await;
    let source = code("backend/src/relationships/inference.rs", text).await;
    let pin: String = sqlx::query_scalar("SELECT extraction_version FROM source_versions WHERE version_id=$1").bind(source).fetch_one(&owner).await.unwrap();
    assert_eq!(pin, "source-atoms-v2");
    extract(&app, &owner, &model, queued(&owner, account, test_file, 0).await).await;
    assert!(model.requests.lock().unwrap().is_empty(), "a v2 test file reaches no model");
    assert_eq!(version_state(&owner, test_file).await.0, "done");
    extract(&app, &owner, &model, queued(&owner, account, source, 0).await).await;
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    assert_eq!(version_state(&owner, source).await.0, "done");
    let kin: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge WHERE graph_id=$1 AND predicate IN ('parent_of','child_of')")
        .bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!(kin, 0, "no fixture family from v2 code");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// A timed-out call is retried once on half the window; the failed attempt
/// is settled once (not generated, nothing charged), the retry is billed
/// once, and the rest of the source continues from the smaller window's end.
#[tokio::test]
async fn source_atoms_timeout_is_retried_on_a_smaller_window_and_billed_once() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let lines: Vec<String> = (0..120).map(|i| format!("Line {i:03}: the team shipped item {i:03}.")).collect();
    let text = lines.join("\n");
    let saved = capture(&app, account, graph, "github_code", "timeout", &text, user, "original_text").await;
    let model = Scripted::new(vec![
        Err(LlmError::Timeout),
        Ok(json!({"atoms":[fact(&lines[0], json!([])), fact(&lines[3], json!([]))]})),
    ]);
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    run_scripted(&app, &owner, &model, job).await;
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages[0].content.len() < requests[0].messages[0].content.len(), "the retry sends a smaller window");
    drop(requests);
    let (status, cursor, error) = version_state(&owner, saved.version_id).await;
    let total = text.chars().count() as i64;
    assert_eq!((status.as_str(), error), ("pending", None));
    assert!(cursor > 0 && cursor < total, "the window ends early ({cursor} of {total}) and the source continues");
    let window_end: i32 = sqlx::query_scalar("SELECT end_char FROM semantic_atom_windows WHERE source_version_id=$1 AND start_char=0")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(window_end as i64, cursor);
    let next: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE payload->>'source_version_id'=$1 AND (payload->>'start_char')::bigint=$2 AND status='pending'")
        .bind(saved.version_id.to_string()).bind(cursor).fetch_one(&owner).await.unwrap();
    assert_eq!(next, 1, "the continuation starts at the retried window's end");
    assert_eq!(stored_quotes(&owner, saved.version_id).await, [lines[0].clone(), lines[3].clone()]);
    assert_eq!(receipts(&owner, job_id).await, (1, 1, 0, 420), "one attempt not generated, one billed, nothing left open");
    let run_calls: i64 = sqlx::query_scalar("SELECT attempted_model_calls FROM corpus_enrichment_units WHERE holding_id=$1 AND execution_owner")
        .bind(saved.version_id).fetch_one(&owner).await.unwrap();
    assert_eq!(run_calls, 2, "the retry is admitted against the run budget like any call");
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

/// More atoms than the window cap: the first valid ones are kept, the cut is
/// recorded, and the window completes.
#[tokio::test]
async fn source_atoms_too_many_atoms_keeps_the_cap_and_records_the_cut() {
    let Some((_db, owner, app)) = pools().await else { return; };
    let (account, graph) = account(&owner).await;
    let user: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1").bind(account).fetch_one(&owner).await.unwrap();
    let lines: Vec<String> = (0..50).map(|i| format!("Fact {i:02} holds.")).collect();
    let text = lines.join("\n");
    let saved = capture(&app, account, graph, "slack", "too-many", &text, user, "original_text").await;
    let atoms: Vec<Value> = lines.iter().map(|line| fact(line, json!([]))).collect();
    let model = Scripted::new(vec![Ok(json!({"atoms":atoms}))]);
    let job = queued(&owner, account, saved.version_id, 0).await;
    let job_id = job.id;
    run_scripted(&app, &owner, &model, job).await;
    assert_eq!(version_state(&owner, saved.version_id).await, ("done".into(), text.chars().count() as i64, None));
    let kept = stored_quotes(&owner, saved.version_id).await;
    assert_eq!(kept.len(), 40);
    assert_eq!(kept, lines[..40].to_vec());
    let diagnostics: Value = sqlx::query_scalar("SELECT diagnostics->'extraction' FROM llm_usage WHERE source_job_id=$1 AND task='source_atoms_v2'")
        .bind(job_id).fetch_one(&owner).await.unwrap();
    assert_eq!((diagnostics["accepted_atoms"].clone(), diagnostics["capped_atoms"].clone()), (json!(40), json!(10)));
    assert_eq!(receipts(&owner, job_id).await, (1, 0, 0, 420));
    app.close().await;
    support::managed_extraction::drop_database(&owner).await;
}

struct Blobs {
    attempts: AtomicUsize,
    fail_once: bool,
    missing: bool,
    deleted: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl PromotionBlobStore for Blobs {
    async fn get_blob(&self, _: &str, _: &[u8; 32]) -> Result<Vec<u8>, R2Error> {
        panic!("cleanup must not read content")
    }
    async fn put_blob(&self, _: &str, _: &[u8], _: &[u8; 32]) -> Result<(), R2Error> {
        panic!("cleanup must not write content")
    }
    async fn delete_blob(&self, key: &str) -> Result<(), R2Error> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 && self.fail_once {
            return Err(R2Error::S3("synthetic transient error".into()));
        }
        self.deleted.lock().unwrap().push(key.into());
        if self.missing {
            Err(R2Error::NotFound("already removed".into()))
        } else {
            Ok(())
        }
    }
}
async fn cleanup_job(
    pool: &PgPool,
    account: Uuid,
    graph: Uuid,
    key: String,
    org: Option<String>,
) -> JobRow {
    let kind = JobKind::DeleteSourceBlob {
        graph_id: graph,
        account_id: account,
        org_id: org,
        blob_key: key,
    };
    let id = queue::enqueue(pool, &kind, Some(account), None, 0)
        .await
        .unwrap()
        .unwrap();
    JobRow {
        id,
        kind: "DeleteSourceBlob".into(),
        payload: serde_json::to_value(kind).unwrap()["payload"].clone(),
        attempts: 0,
        account_id: Some(account),
    }
}
async fn reload_job(pool: &PgPool, id: Uuid) -> JobRow {
    sqlx::query_as::<_, (String, Value, i32, Option<Uuid>)>(
        "SELECT kind,payload,attempts,account_id FROM jobs WHERE id=$1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .map(|(kind, payload, attempts, account_id)| JobRow {
        id,
        kind,
        payload,
        attempts,
        account_id,
    })
    .unwrap()
}
#[tokio::test]
async fn source_blob_cleanup_retries_without_llm_and_rejects_foreign_keys() {
    let Some((db, owner, app, _base_db, base_owner)) = cleanup_pools().await else {
        return;
    };
    let (account, graph) = account(&owner).await;
    let key = format!("{graph}/{}", Uuid::new_v4());
    let rollback = JobKind::DeleteSourceBlob {
        graph_id: graph,
        account_id: account,
        org_id: None,
        blob_key: key.clone(),
    };
    let mut tx = begin_account_scoped(&app, account).await.unwrap();
    let aborted = queue::enqueue_tx(&mut tx, &rollback, Some(account), None, 0)
        .await
        .unwrap()
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1)")
            .bind(aborted)
            .fetch_one(&owner)
            .await
            .unwrap()
    );
    let blobs = Blobs {
        attempts: AtomicUsize::new(0),
        fail_once: true,
        missing: false,
        deleted: Mutex::new(vec![]),
    };
    let job = cleanup_job(&app, account, graph, key.clone(), None).await;
    let id = job.id;
    // No source/version rows exist. Cleanup outlives them by design.
    source_blob_cleanup::process_job(&TenantId::Consumer, &app, None, job).await;
    assert_eq!(reload_job(&owner, id).await.attempts, 1);
    source_blob_cleanup::process_job(
        &TenantId::Consumer,
        &app,
        Some(&blobs),
        reload_job(&owner, id).await,
    )
    .await;
    assert_eq!(reload_job(&owner, id).await.attempts, 2);
    source_blob_cleanup::process_job(
        &TenantId::Consumer,
        &app,
        Some(&blobs),
        reload_job(&owner, id).await,
    )
    .await;
    let status: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1")
        .bind(id)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(status, "done");
    assert_eq!(*blobs.deleted.lock().unwrap(), vec![key]);
    for (key, org, stamp) in [
        (
            format!("{}/{}", Uuid::new_v4(), Uuid::new_v4()),
            None,
            Some(account),
        ),
        (
            format!("org_other/{graph}/{}", Uuid::new_v4()),
            Some("org_other".into()),
            Some(account),
        ),
        (
            format!("{graph}/{}", Uuid::new_v4()),
            None,
            Some(Uuid::new_v4()),
        ),
        (format!("{graph}/{}", Uuid::new_v4()), None, None),
    ] {
        let mut job = cleanup_job(&app, account, graph, key, org).await;
        job.account_id = stamp;
        source_blob_cleanup::process_job(&TenantId::Consumer, &app, Some(&blobs), job).await;
    }
    assert_eq!(
        blobs.attempts.load(Ordering::SeqCst),
        2,
        "foreign graph/plane/envelope keys never reach storage"
    );
    let next = cleanup_job(
        &app,
        account,
        graph,
        format!("{graph}/{}", Uuid::new_v4()),
        None,
    )
    .await;
    let next_id = next.id;
    let tenant = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: app.clone(),
        store: Arc::new(PostgresStore::new(app.clone())),
    };
    let tick = source_blob_cleanup::tick(&tenant, Some(&blobs))
        .await
        .unwrap();
    assert!(
        tick.processed >= 1,
        "normal harness tick has no model dependency"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status::text FROM jobs WHERE id=$1")
            .bind(next_id)
            .fetch_one(&owner)
            .await
            .unwrap(),
        "done"
    );
    let after_graph = cleanup_job(
        &app,
        account,
        graph,
        format!("{graph}/{}", Uuid::new_v4()),
        None,
    )
    .await;
    let after_graph_id = after_graph.id;
    sqlx::query("DELETE FROM graphs WHERE id=$1")
        .bind(graph)
        .execute(&owner)
        .await
        .unwrap();
    let missing = Blobs {
        attempts: AtomicUsize::new(0),
        fail_once: false,
        missing: true,
        deleted: Mutex::new(vec![]),
    };
    source_blob_cleanup::process_job(&TenantId::Consumer, &app, Some(&missing), after_graph).await;
    assert_eq!(missing.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status::text FROM jobs WHERE id=$1")
            .bind(after_graph_id)
            .fetch_one(&owner)
            .await
            .unwrap(),
        "done",
        "trusted cleanup survives missing graph and already removed object"
    );
    app.close().await;
    owner.close().await;
    sqlx::query(&format!("DROP DATABASE {db} WITH (FORCE)"))
        .execute(&base_owner)
        .await
        .unwrap();
    support::managed_extraction::drop_database(&base_owner).await;
}
