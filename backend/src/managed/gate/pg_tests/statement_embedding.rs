//! Statement embeddings (`EmbedStatements`) on the managed `cloud_embedding`
//! purpose: receipted inputs, the daily cap, and the idempotent backfill
//! sweep. Real Postgres on both planes, the restricted app role for every
//! worker statement, and a loopback OpenRouter stub for the vectors.
use super::*;
use crate::jobs::{queue, JobKind};
use crate::workers::{statement_embed, tenant_harness::WorkerTenant};
use axum::{extract::State, routing::post, Json, Router};
use coralswarm_core::embed_openrouter::OpenRouterEmbedder;
use serde_json::{json, Value};
use sha2::Digest;
use sqlx::{types::Decimal, Row};
use std::sync::{Arc, Mutex};

struct StatementFixture {
    finance: Fixture,
    /// Drive the worker on the table owner (prod's tenant role) instead of
    /// the restricted app role.
    as_owner: bool,
    /// Statements of the fixture's retained (permitted) version.
    statements: Vec<(Uuid, String)>,
    /// A second permitted version with one statement.
    other: (Uuid, Uuid, String),
    /// A restricted source item's version and its statement.
    restricted: (Uuid, Uuid, String),
}

async fn statement(pool: &PgPool, graph: Uuid, version: Uuid, ordinal: i32, text: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,$3,'fact',$4,'source-atoms-v1','fixture-model') RETURNING semantic_atom_id")
        .bind(graph).bind(version).bind(ordinal).bind(text).fetch_one(pool).await.unwrap()
}

async fn version(pool: &PgPool, graph: Uuid, id: &str, access: &str) -> Uuid {
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','fixture:statements',$2,$3) RETURNING source_item_id")
        .bind(graph).bind(id).bind(access).fetch_one(pool).await.unwrap();
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version) VALUES($1,$2,$3,'note','fixture:statements','synthetic statement fixture','source-atoms-v1') RETURNING version_id")
        .bind(graph).bind(item).bind(id).fetch_one(pool).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item).bind(version).execute(pool).await.unwrap();
    version
}

impl StatementFixture {
    async fn new(org: bool) -> Self {
        let f = Fixture::new(org).await;
        f.balance(Decimal::from(100)).await;
        let owner = f.owner().clone();
        sqlx::query("DELETE FROM corpus_enrichment_units WHERE graph_id=$1").bind(f.graph).execute(&owner).await.unwrap();
        sqlx::query("DELETE FROM corpus_enrichment_runs WHERE graph_id=$1").bind(f.graph).execute(&owner).await.unwrap();
        let texts = ["Acme approved the green release", "The launch moved to Friday ✓"];
        let mut statements = Vec::new();
        for (n, text) in texts.iter().enumerate() {
            statements.push((statement(&owner, f.graph, f.version, n as i32, text).await, text.to_string()));
        }
        let other_version = version(&owner, f.graph, "other-permitted", "permitted").await;
        let other_text = "Beta sign-off is pending".to_string();
        let other = (other_version, statement(&owner, f.graph, other_version, 0, &other_text).await, other_text);
        let restricted_version = version(&owner, f.graph, "restricted", "restricted").await;
        let restricted_text = "RESTRICTED statement never leaves".to_string();
        let restricted = (restricted_version, statement(&owner, f.graph, restricted_version, 0, &restricted_text).await, restricted_text);
        sqlx::query("INSERT INTO model_rates(provider,model_pattern,priority,input_per_million,output_per_million,cache_write_multiplier,cache_read_multiplier,effective_from,is_blend_fallback) VALUES('openrouter','text-embedding-3-small',-100,0.02,0,1,1,'2000-01-01',false)")
            .execute(&f.control.owner).await.unwrap();
        Self { finance: f, as_owner: false, statements, other, restricted }
    }

    async fn job(&self, version: Uuid) -> queue::JobRow {
        let id = queue::enqueue(
            self.finance.app(),
            &JobKind::EmbedStatements { graph_id: self.finance.graph, statement_version_id: version },
            Some(self.finance.account),
            None,
            0,
        )
        .await
        .unwrap()
        .unwrap();
        self.claim(id).await
    }

    async fn claim(&self, id: Uuid) -> queue::JobRow {
        queue::dequeue_ids(self.finance.app(), &[statement_embed::JOB_KIND], &[id])
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .expect("the job is claimable")
    }

    async fn drive(&self, job: queue::JobRow, cloud: &OpenRouterEmbedder) {
        let (pool, control) = if self.as_owner {
            (self.finance.owner(), &self.finance.control.owner)
        } else {
            (self.finance.app(), &self.finance.control.app)
        };
        statement_embed::process_job(&self.finance.tenant, pool, Some(cloud), Some(control), job)
        .await;
    }

    fn worker(&self) -> WorkerTenant {
        WorkerTenant {
            tenant: self.finance.tenant.clone(),
            pool: self.finance.app().clone(),
            store: Arc::new(crate::store::PostgresStore::new(self.finance.app().clone())),
        }
    }

    async fn sweep(&self) -> usize {
        statement_embed::sweep_tick(&self.worker(), &crate::shutdown::Shutdown::new())
            .await
            .unwrap()
            .processed
    }

    async fn pending_versions(&self) -> Vec<Uuid> {
        let mut tx = begin_plane(&self.finance.plane()).await.unwrap();
        let rows: Vec<String> = sqlx::query_scalar("SELECT payload->>'statement_version_id' FROM jobs WHERE kind='EmbedStatements' AND status::text='pending' ORDER BY 1")
            .fetch_all(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        rows.into_iter().map(|v| v.parse().unwrap()).collect()
    }

    async fn vectors(&self) -> Vec<(Uuid, String, Vec<u8>, bool, Option<String>)> {
        let mut tx = begin_plane(&self.finance.plane()).await.unwrap();
        let rows = sqlx::query_as("SELECT semantic_atom_id,embed_model,statement_sha256,embedding IS NOT NULL,held_reason FROM semantic_atom_embeddings ORDER BY semantic_atom_id")
            .fetch_all(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        rows
    }

    async fn finish(self) {
        self.finance.finish().await;
    }
}

#[derive(Clone, Default)]
struct Wire {
    bodies: Arc<Mutex<Vec<Value>>>,
}

async fn serve(State(wire): State<Wire>, Json(body): Json<Value>) -> Json<Value> {
    wire.bodies.lock().unwrap().push(body.clone());
    let n = body["input"].as_array().unwrap().len();
    Json(json!({"id":"gen-statements","model":"openai/text-embedding-3-small",
        "data":(0..n).rev().map(|index| json!({"index":index,"embedding":vec![(index+1) as f64;384]})).collect::<Vec<_>>(),
        "usage":{"prompt_tokens":7*n,"total_tokens":7*n,"cost":0.000001}}))
}

async fn loopback() -> (OpenRouterEmbedder, Wire, tokio::task::JoinHandle<()>) {
    let wire = Wire::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().route("/v1/embeddings", post(serve)).with_state(wire.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let cloud = OpenRouterEmbedder::new(
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap(),
        "local-fixture-only",
    )
    .with_base_url(url);
    (cloud, wire, server)
}

fn sha(text: &str) -> Vec<u8> {
    sha2::Sha256::digest(text.as_bytes()).to_vec()
}

/// Every statement sent is receipted under `cloud_embedding` before the
/// send, with exactly the bytes sent; the vector lands tagged with the cloud
/// model and the statement's hash; a restricted item's statement is never
/// sent.
async fn statements_are_receipted(org: bool, as_owner: bool) {
    let mut f = StatementFixture::new(org).await;
    f.as_owner = as_owner;
    let (cloud, wire, server) = loopback().await;
    let job = f.job(f.finance.version).await;
    let job_id = job.id;
    f.drive(job, &cloud).await;
    let restricted = f.job(f.restricted.0).await;
    let restricted_id = restricted.id;
    f.drive(restricted, &cloud).await;
    let bodies = wire.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1, "one call for the permitted version, none for the restricted one");
    assert_eq!(bodies[0]["model"], "openai/text-embedding-3-small");
    assert_eq!(bodies[0]["dimensions"], 384);
    assert_eq!(bodies[0]["provider"]["zdr"], true);
    assert_eq!(bodies[0]["provider"]["max_price"]["prompt"], 0.02);
    let sent: Vec<String> = serde_json::from_value(bodies[0]["input"].clone()).unwrap();
    assert_eq!(sent, f.statements.iter().map(|s| s.1.clone()).collect::<Vec<_>>(), "only the statements, in ordinal order");
    assert!(!bodies[0].to_string().contains("RESTRICTED"));
    let mut content = begin_plane(&f.finance.plane()).await.unwrap();
    let receipts: Vec<(Uuid, String, Uuid, Option<Uuid>)> = sqlx::query_as("SELECT receipt_id,state,attributed_user_id,unit_id FROM managed_attempt_receipts WHERE job_id=$1 AND purpose='cloud_embedding' AND protocol_version=1 AND dispatch_phase='armed'")
        .bind(job_id).fetch_all(&mut *content).await.unwrap();
    assert_eq!(receipts.len(), 1, "one receipted attempt");
    assert_eq!((receipts[0].1.as_str(), receipts[0].2, receipts[0].3), ("billed", f.finance.payer, None));
    let inputs: Vec<(Uuid, i16, i64, Vec<u8>, i64, String, String)> = sqlx::query_as("SELECT atom_id,ordinal,content_revision,content_sha256,input_bytes,outcome,subject FROM managed_embedding_inputs WHERE receipt_id=$1 ORDER BY ordinal")
        .bind(receipts[0].0).fetch_all(&mut *content).await.unwrap();
    assert_eq!(inputs.len(), 2);
    for (n, input) in inputs.iter().enumerate() {
        assert_eq!(input.0, f.statements[n].0);
        assert_eq!((input.1, input.2), (n as i16, 1));
        assert_eq!(input.3, sha(&sent[n]), "the receipt hashes exactly what was sent");
        assert_eq!(input.4, sent[n].len() as i64);
        assert_eq!((input.5.as_str(), input.6.as_str()), ("written", "statement"));
    }
    let none: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE job_id=$1").bind(restricted_id).fetch_one(&mut *content).await.unwrap();
    assert_eq!(none, 0, "a restricted statement is neither receipted nor sent");
    content.commit().await.unwrap();
    let vectors = f.vectors().await;
    assert_eq!(vectors.len(), 2);
    for (id, text) in &f.statements {
        let row = vectors.iter().find(|v| v.0 == *id).expect("vector written");
        assert_eq!((row.1.as_str(), &row.2, row.3, &row.4), (coralswarm_core::embed::EMBED_MODEL_CLOUD, &sha(text), true, &None));
    }
    let financial: (String, Uuid) = sqlx::query_as("SELECT state,payer_id FROM managed_credit_reservations WHERE receipt_id=$1")
        .bind(receipts[0].0).fetch_one(&f.finance.control.owner).await.unwrap();
    assert_eq!(financial, ("settled".into(), f.finance.payer), "settled once against the payer");
    let status: (String, String) = sqlx::query_as("SELECT status::text,outcome FROM jobs WHERE id=$1").bind(job_id).fetch_one(f.finance.owner()).await.unwrap();
    assert_eq!(status, ("done".into(), "did_work".into()));
    server.abort();
    f.finish().await;
}

#[tokio::test]
async fn consumer_statement_embedding_is_receipted_under_cloud_embedding() {
    statements_are_receipted(false, false).await;
}
#[tokio::test]
async fn tenant_statement_embedding_is_receipted_under_cloud_embedding() {
    statements_are_receipted(true, false).await;
}
/// Prod's tenant pool connects as the table owner, not the app role.
#[tokio::test]
async fn tenant_owner_statement_embedding_is_receipted_under_cloud_embedding() {
    statements_are_receipted(true, true).await;
}

/// An exhausted daily `cloud_embedding` budget sends nothing and holds the
/// statements until the next UTC day; the sweep does not re-enqueue them
/// before then, and they embed once the hold has passed.
async fn cap_exhaustion_holds(org: bool) {
    let f = StatementFixture::new(org).await;
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,charged_micro_usd) VALUES($1,'cloud_embedding',(clock_timestamp() AT TIME ZONE 'UTC')::date,1,1)")
        .bind(f.finance.account).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let (cloud, wire, server) = loopback().await;
    let job = f.job(f.finance.version).await;
    let job_id = job.id;
    f.drive(job, &cloud).await;
    assert!(wire.bodies.lock().unwrap().is_empty(), "nothing is sent past the cap");
    let armed: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE purpose='cloud_embedding' AND dispatch_phase='armed'")
        .fetch_one(f.finance.owner()).await.unwrap();
    assert_eq!(armed, 0);
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let held: Vec<(Option<String>, bool, bool)> = sqlx::query_as("SELECT held_reason,embedding IS NULL,retry_after=(date_trunc('day',clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')+interval '1 day' FROM semantic_atom_embeddings WHERE semantic_atom_id=ANY($1)")
        .bind(f.statements.iter().map(|s| s.0).collect::<Vec<_>>()).fetch_all(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(held, vec![(Some("daily_budget_exhausted".into()), true, true); 2], "held until the next UTC day");
    let status: (String, Option<String>) = sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE id=$1").bind(job_id).fetch_one(f.finance.owner()).await.unwrap();
    assert_eq!(status, ("done".into(), Some("managed_embedding_held".into())));
    // The sweep leaves held statements alone (only the other permitted
    // version is due); the budget row is unchanged.
    f.sweep().await;
    assert_eq!(f.pending_versions().await, vec![f.other.0]);
    let budget: (i64, i64) = sqlx::query_as("SELECT reserved_micro_usd,charged_micro_usd FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding'")
        .bind(f.finance.account).fetch_one(f.finance.owner()).await.unwrap();
    assert_eq!(budget, (0, 1), "a held batch reserves nothing");
    // Next day: the hold has passed and the budget is fresh.
    sqlx::query("UPDATE semantic_atom_embeddings SET retry_after=now()-interval '1 second'").execute(f.finance.owner()).await.unwrap();
    sqlx::query("DELETE FROM managed_budget_periods WHERE account_id=$1 AND purpose='cloud_embedding'").bind(f.finance.account).execute(f.finance.owner()).await.unwrap();
    let retry = f.job(f.finance.version).await;
    f.drive(retry, &cloud).await;
    assert_eq!(wire.bodies.lock().unwrap().len(), 1);
    let vectors = f.vectors().await;
    assert!(f.statements.iter().all(|(id, _)| vectors.iter().any(|v| v.0 == *id && v.3 && v.4.is_none())), "embedded after the hold, hold cleared");
    server.abort();
    f.finish().await;
}

#[tokio::test]
async fn consumer_statement_embedding_cap_exhaustion_holds() {
    cap_exhaustion_holds(false).await;
}
#[tokio::test]
async fn tenant_statement_embedding_cap_exhaustion_holds() {
    cap_exhaustion_holds(true).await;
}

/// The backfill sweep enqueues each due version once, never a restricted
/// one; re-running it or the jobs sends nothing new; a changed statement is
/// due again and only it is re-sent.
async fn backfill_is_idempotent(org: bool) {
    let f = StatementFixture::new(org).await;
    let (cloud, wire, server) = loopback().await;
    assert_eq!(f.sweep().await, 2);
    let mut due = vec![f.finance.version, f.other.0];
    due.sort();
    assert_eq!(f.pending_versions().await, due, "both permitted versions, not the restricted one");
    assert_eq!(f.sweep().await, 0, "a version with a pending job is not enqueued again");
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM jobs WHERE kind='EmbedStatements' AND status::text='pending' ORDER BY id")
        .fetch_all(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    for id in ids {
        let job = f.claim(id).await;
        f.drive(job, &cloud).await;
    }
    assert_eq!(wire.bodies.lock().unwrap().len(), 2);
    assert_eq!(f.vectors().await.len(), 3, "every permitted statement, never the restricted one");
    assert!(f.vectors().await.iter().all(|v| v.0 != f.restricted.1));
    assert_eq!(f.sweep().await, 0, "nothing is due once every statement is current");
    // A repeated job over current vectors sends and receipts nothing.
    let receipts = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM managed_attempt_receipts WHERE purpose='cloud_embedding'")
            .fetch_one(f.finance.owner()).await.unwrap()
    };
    let before = receipts().await;
    let replay = f.job(f.finance.version).await;
    f.drive(replay, &cloud).await;
    assert_eq!(wire.bodies.lock().unwrap().len(), 2);
    assert_eq!(receipts().await, before);
    // A changed statement is stale: the reader ignores the old vector, the
    // sweep finds exactly its version, and only it is re-sent.
    sqlx::query("UPDATE semantic_atoms SET statement='Beta sign-off landed' WHERE semantic_atom_id=$1")
        .bind(f.other.1).execute(f.finance.owner()).await.unwrap();
    assert_eq!(f.sweep().await, 1);
    assert_eq!(f.pending_versions().await, vec![f.other.0]);
    let mut tx = begin_plane(&f.finance.plane()).await.unwrap();
    let id: Uuid = sqlx::query_scalar("SELECT id FROM jobs WHERE kind='EmbedStatements' AND status::text='pending'").fetch_one(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let job = f.claim(id).await;
    f.drive(job, &cloud).await;
    let bodies = wire.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 3);
    assert_eq!(bodies[2]["input"], json!(["Beta sign-off landed"]));
    let row = f.vectors().await.into_iter().find(|v| v.0 == f.other.1).unwrap();
    assert_eq!(row.2, sha("Beta sign-off landed"));
    assert_eq!(f.sweep().await, 0);
    server.abort();
    f.finish().await;
}

#[tokio::test]
async fn consumer_statement_backfill_is_idempotent() {
    backfill_is_idempotent(false).await;
}
#[tokio::test]
async fn tenant_statement_backfill_is_idempotent() {
    backfill_is_idempotent(true).await;
}

async fn active_statement_jobs_use_the_active_index(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::query("INSERT INTO jobs(kind,payload,status,run_after,finished_at) SELECT CASE WHEN i%50=0 THEN 'EmbedStatements' ELSE 'GraphMessageIngest' END,jsonb_build_object('statement_version_id',gen_random_uuid()),CASE WHEN i%1000=0 THEN 'pending'::job_status WHEN i%1000=1 THEN 'running' ELSE 'done' END,now(),now() FROM generate_series(1,200000) i")
        .execute(f.owner()).await.unwrap();
    sqlx::query("VACUUM ANALYZE jobs").execute(f.owner()).await.unwrap();
    let mut tx = begin_plane(&f.plane()).await.unwrap();
    let plan: Vec<String> = sqlx::raw_sql(&format!("EXPLAIN (GENERIC_PLAN, COSTS OFF) {}", statement_embed::CANDIDATES_SQL))
        .fetch_all(&mut *tx).await.unwrap()
        .iter().map(|row| row.get::<String, _>(0)).collect();
    tx.commit().await.unwrap();
    let plan = plan.join("\n");
    let jobs_scans: Vec<&str> = plan.lines().filter(|line| line.split_whitespace().any(|word| word == "jobs")).collect();
    assert!(!jobs_scans.is_empty() && jobs_scans.iter().all(|line| line.contains("Scan using jobs_active_kind_status_run_after on jobs")), "the active CTE must walk jobs_active_kind_status_run_after and nothing else on jobs:\n{plan}");
    f.finish().await;
}

#[tokio::test]
async fn consumer_active_statement_jobs_use_the_active_index() {
    active_statement_jobs_use_the_active_index(false).await;
}
#[tokio::test]
async fn tenant_active_statement_jobs_use_the_active_index() {
    active_statement_jobs_use_the_active_index(true).await;
}
