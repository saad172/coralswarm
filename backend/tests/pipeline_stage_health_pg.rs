//! Pipeline stage health: every alert rule fires on a seeded silent stage and
//! stays quiet on a healthy plane.
//!
//! Real local Postgres: a fresh control database and fresh org tenant
//! databases per test, the real worker `measure`, `evaluate`, `store` and the
//! real `/internal/admin/pipeline-stage-health` handler. The org tenant is
//! read as the table owner (production's `neondb_owner` shape) AND as the
//! restricted `coralswarm_tenant_app` role through the tenant-system scope;
//! the consumer plane as the RLS-enforcing `coralswarm_app` role.
//!
//!   REQUIRE_PG_SUITE=1 RELINFER_TEST_DATABASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test pipeline_stage_health_pg -- --test-threads=1
mod support;

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use coralswarm_backend::pipeline_stage_health::{evaluate, StageReport, Thresholds};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::pipeline_stage_health::{measure, store};
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::{admin_pipeline_stage_health, PipelineStageHealthQuery};
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

const ORG: &str = "org_stage_health";

/// Every rule `silent_stages_alert_on_both_roles` must see fire.
const SILENT_EVENTS: &[&str] = &[
    "pipeline_embed_stalled",
    "pipeline_extraction_stalled",
    "pipeline_linking_stalled",
    "pipeline_chunks_missing",
    "pipeline_reef_duplicates",
    "pipeline_er_stalled",
    "pipeline_edges_stalled",
    "pipeline_journal_stalled",
    "pipeline_titles_stalled",
    "pipeline_jev_degraded",
    "pipeline_jobs_dead",
    "pipeline_spend_at_cap",
    "pipeline_hold_stuck",
];

struct Planes {
    base: PgPool,
    control: PgPool,
    control_app: PgPool,
    dbs: Vec<coralswarm_backend::test_db::TestDatabase>,
    pools: Vec<PgPool>,
}

impl Planes {
    async fn drop(self) {
        for pool in self.pools.iter().chain([&self.control, &self.control_app]) {
            pool.close().await;
        }
        for db in &self.dbs {
            sqlx::query(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")).execute(&self.base).await.unwrap();
        }
    }
}

fn base_opts() -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "RELINFER_TEST_DATABASE_URL required");
        eprintln!("skipping pipeline stage health test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"), "test database must be local");
    Some(opts)
}

async fn control_plane(opts: &PgConnectOptions) -> Planes {
    let base = PgPoolOptions::new().max_connections(2).connect_with(opts.clone()).await.unwrap();
    let db = coralswarm_backend::test_db::TestDatabase::new(opts, "stage_ctl");
    sqlx::query(&format!("CREATE DATABASE {db}")).execute(&base).await.unwrap();
    let control = PgPoolOptions::new().connect_with(opts.clone().database(&db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    support::app_role::set_app_role_password_with_retry(&control).await;
    let control_app = PgPoolOptions::new()
        .connect_with(
            opts.clone()
                .database(&db)
                .username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    Planes { base, control, control_app, dbs: vec![db], pools: Vec::new() }
}

/// An org tenant database: (owner pool, `coralswarm_tenant_app` pool).
async fn tenant_plane(p: &mut Planes, opts: &PgConnectOptions) -> (PgPool, PgPool) {
    let db = coralswarm_backend::test_db::TestDatabase::new(opts, "stage_ten");
    sqlx::query(&format!("CREATE DATABASE {db}")).execute(&p.base).await.unwrap();
    let owner = PgPoolOptions::new().connect_with(opts.clone().database(&db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
    support::tenant_role::set_app_role_password_with_retry(&owner).await;
    let app = PgPoolOptions::new()
        .connect_with(
            opts.clone()
                .database(&db)
                .username("coralswarm_tenant_app")
                .password(support::tenant_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)").bind(ORG).execute(&owner).await.unwrap();
    // Production (phase two of the identity contract) dropped the legacy
    // title-unique index, so same-title copies exist there; the duplicate
    // probe must see them here too.
    sqlx::query("DROP INDEX IF EXISTS nodes_graph_type_lower_title_unique").execute(&owner).await.unwrap();
    p.dbs.push(db);
    p.pools.extend([owner.clone(), app.clone()]);
    (owner, app)
}

/// A control-plane account (and, for the consumer plane, its graph).
async fn account(control: &PgPool, graph: bool) -> (Uuid, Uuid, Option<Uuid>) {
    let tag = Uuid::new_v4();
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$2) RETURNING id")
        .bind(format!("stage-{tag}@test.local"))
        .bind(format!("stage-{tag}"))
        .fetch_one(control)
        .await
        .unwrap();
    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
        .bind(user)
        .fetch_one(control)
        .await
        .unwrap();
    let g = if graph {
        Some(
            sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'stage') RETURNING id")
                .bind(account)
                .fetch_one(control)
                .await
                .unwrap(),
        )
    } else {
        None
    };
    (user, account, g)
}

fn worker(tenant: TenantId, pool: &PgPool) -> WorkerTenant {
    WorkerTenant { tenant, pool: pool.clone(), store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store> }
}

async fn exec(pool: &PgPool, sql: &str, graph: Uuid, account: Uuid) {
    sqlx::query(sql).bind(graph).bind(account).execute(pool).await.unwrap_or_else(|e| panic!("{e}\n{sql}"));
}

/// `n` atoms of `source_type`, created `age` ago, with optional embedding and
/// title kind.
async fn atoms(pool: &PgPool, graph: Uuid, source_type: &str, n: i32, age: &str, embedded: bool, stamp: Option<&str>, title_kind: Option<&str>) -> Vec<Uuid> {
    let tag = Uuid::new_v4().simple().to_string();
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,created_at, \
           embedding_next,embed_model_next,title,title_kind) \
         SELECT $1,$2,$3||'-'||i,'','body',now()-$4::interval,now()-$4::interval, \
           CASE WHEN $5 THEN array_fill(0.1::real, ARRAY[384])::vector END,$6, \
           CASE WHEN $7::text IS NOT NULL THEN 'title '||i END,$7 \
         FROM generate_series(1,$8) i RETURNING atom_id",
    )
    .bind(graph)
    .bind(source_type)
    .bind(tag)
    .bind(age)
    .bind(embedded)
    .bind(stamp)
    .bind(title_kind)
    .bind(n)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// One current, permitted source version per atom, captured with the atom,
/// optionally with its chunk row and a link to `reef`.
async fn versions(pool: &PgPool, graph: Uuid, source_type: &str, atom_ids: &[Uuid], status: &str, chunks: bool, reef: Option<Uuid>) {
    for atom in atom_ids {
        let item: Uuid = sqlx::query_scalar(
            "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,$2,'test:stage',$3) RETURNING source_item_id",
        )
        .bind(graph)
        .bind(source_type)
        .bind(atom.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        let version: Uuid = sqlx::query_scalar(
            "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,extraction_status,captured_at,extraction_version) \
             SELECT $1,$2,$3,$4,'test:stage',$5,a.created_at,'source-atoms-v2' FROM atoms a WHERE a.atom_id=$6 RETURNING version_id",
        )
        .bind(graph)
        .bind(item)
        .bind(atom.to_string())
        .bind(source_type)
        .bind(status)
        .bind(atom)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(item)
            .bind(version)
            .execute(pool)
            .await
            .unwrap();
        if chunks {
            sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
                .bind(graph)
                .bind(version)
                .bind(atom)
                .execute(pool)
                .await
                .unwrap();
        }
        if let Some(reef) = reef {
            sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(reef).bind(atom).execute(pool).await.unwrap();
        }
    }
}

async fn reef(pool: &PgPool, graph: Uuid, title: &str, age: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes(graph_id,type,title,created_at,updated_at,identity_scope) \
         VALUES($1,'entity',$2,now()-$3::interval,now()-$3::interval,gen_random_uuid()) RETURNING node_id",
    )
    .bind(graph)
    .bind(title)
    .bind(age)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Seeds one content plane. `silent` stops every stage; otherwise every stage
/// flows. Rows live in `pool` (a tenant DB, or the control DB for the
/// consumer plane); `plane` names the control-plane reservation plane.
async fn seed(pool: &PgPool, control: &PgPool, graph: Uuid, account: Uuid, user: Uuid, plane: &str, silent: bool) {
    // Baseline (3 days old): every stage did its work once.
    let base_reef = reef(pool, graph, "Baseline", "3 days").await;
    let other = reef(pool, graph, "Other", "3 days").await;
    for source_type in ["github_code", "linear_issue"] {
        let a = atoms(pool, graph, source_type, 1, "3 days", true, None, Some("llm")).await;
        versions(pool, graph, source_type, &a, "done", true, Some(base_reef)).await;
    }
    let a = atoms(pool, graph, "github_pr", 1, "3 days", true, None, Some("llm")).await;
    versions(pool, graph, "github_pr", &a, "done", true, Some(base_reef)).await;
    exec(pool, "INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,created_at) \
        SELECT $1,(SELECT node_id FROM nodes WHERE graph_id=$1 AND title='Baseline'),'mentions', \
        (SELECT node_id FROM nodes WHERE graph_id=$1 AND title='Other'),now()-interval '3 days' WHERE $2::uuid IS NOT NULL", graph, account).await;
    let _ = other;
    sqlx::query("INSERT INTO journal_rollups(account_id,user_id,graph_id,hour_start,period_start,granularity,summary,input_fingerprint,model) \
        VALUES($1,$2,$3,date_trunc('hour',now()-interval '3 days'),date_trunc('hour',now()-interval '3 days'),'hour','s','f','m')")
        .bind(account).bind(user).bind(graph).execute(pool).await.unwrap();

    // Embedding: slack captured in the last hour.
    if silent {
        atoms(pool, graph, "slack", 5, "1 hour", false, Some("blocked:managed_purpose_unsupported"), None).await;
    } else {
        atoms(pool, graph, "slack", 5, "1 hour", true, None, None).await;
    }
    // Extraction: github_code captured two hours ago.
    let a = atoms(pool, graph, "github_code", 4, "2 hours", true, None, Some("llm")).await;
    versions(pool, graph, "github_code", &a, if silent { "pending" } else { "done" }, true, (!silent).then_some(base_reef)).await;
    // A wait that is a decision: the admission excluded the source.
    let a = atoms(pool, graph, "note", 4, "2 hours", true, None, Some("llm")).await;
    versions(pool, graph, "note", &a, "pending", true, None).await;
    let n = atoms(pool, graph, "note", 1, "3 days", true, None, Some("llm")).await;
    versions(pool, graph, "note", &n, "done", true, Some(base_reef)).await;
    exec(pool, "INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,extraction_version,state,hold_basis) \
        SELECT v.version_id,v.graph_id,$2,v.source_item_id,'source-atoms-v2','excluded','backlog_policy_excluded' \
        FROM source_versions v WHERE v.graph_id=$1 AND v.source_type='note' AND v.extraction_status='pending'", graph, account).await;
    // Linking: linear_issue extracted, never linked (silent) or linked.
    let a = atoms(pool, graph, "linear_issue", 3, "2 hours", true, None, Some("llm")).await;
    versions(pool, graph, "linear_issue", &a, "done", true, (!silent).then_some(base_reef)).await;
    // Chunk rows: zendesk_ticket extracted without (silent) or with chunk rows.
    let a = atoms(pool, graph, "zendesk_ticket", 3, "2 hours", true, None, Some("llm")).await;
    versions(pool, graph, "zendesk_ticket", &a, "done", !silent, Some(base_reef)).await;
    // Reefs: twelve minted in the last hour, each repeating an older reef's
    // title (silent) or each new.
    for i in 0..12 {
        let title = format!("Topic {i}");
        if silent {
            reef(pool, graph, &title, "10 days").await;
        }
        reef(pool, graph, &title, "1 hour").await;
    }
    // Titles: github_pr all fallback (silent) or titled.
    atoms(pool, graph, "github_pr", 20, "5 hours", true, None, Some(if silent { "fallback" } else { "llm" })).await;
    if !silent {
        exec(pool, "INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,created_at) \
            SELECT $1,a.node_id,'relates_to',b.node_id,now()-interval '1 hour' FROM nodes a, nodes b \
            WHERE a.graph_id=$1 AND b.graph_id=$1 AND a.title='Topic 0' AND b.title='Topic 1' AND $2::uuid IS NOT NULL", graph, account).await;
        exec(pool, "INSERT INTO reef_judgement(graph_id,node_a_id,node_b_id,verdict,source,created_at) \
            SELECT $1,LEAST(a.node_id,b.node_id),GREATEST(a.node_id,b.node_id),'negative','test',now()-interval '1 hour' FROM nodes a, nodes b \
            WHERE a.graph_id=$1 AND b.graph_id=$1 AND a.title='Topic 2' AND b.title='Topic 3' AND $2::uuid IS NOT NULL", graph, account).await;
        sqlx::query("INSERT INTO journal_rollups(account_id,user_id,graph_id,hour_start,period_start,granularity,summary,input_fingerprint,model) \
            VALUES($1,$2,$3,date_trunc('hour',now()-interval '2 hours'),date_trunc('hour',now()-interval '2 hours'),'hour','s','f','m')")
            .bind(account).bind(user).bind(graph).execute(pool).await.unwrap();
    }
    // Jev binding: managed usage rows carry a NULL account_id, as in prod.
    let (ok, withheld, errored, pending) = if silent { (1, 3, 2, 1) } else { (6, 0, 0, 0) };
    for (suffix, n) in [("", ok), ("_withheld", withheld), ("_errored", errored)] {
        sqlx::query("INSERT INTO llm_usage(graph_id,task,model,created_at) SELECT $1,'jev_extraction_binding'||$2,'m',now()-interval '1 hour' FROM generate_series(1,$3)")
            .bind(graph).bind(suffix).bind(n).execute(pool).await.unwrap();
    }
    sqlx::query("INSERT INTO llm_usage_pending(graph_id,task,provider,model,estimated_input_tokens,kind,started_at) \
        SELECT $1,'jev_extraction_binding','p','m',1,'sync',now()-interval '10 minutes' FROM generate_series(1,$2)")
        .bind(graph).bind(pending).execute(pool).await.unwrap();
    // Dead letters.
    sqlx::query("INSERT INTO jobs(kind,payload,status,finished_at,account_id) SELECT 'RegenerateDigest','{}','dead',now()-interval '30 minutes',$1 FROM generate_series(1,$2)")
        .bind(account).bind(if silent { 101 } else { 5 }).execute(pool).await.unwrap();
    // Managed spend today, and one open reservation.
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,reserved_micro_usd,charged_micro_usd,attempts) \
        VALUES($1,'source_extraction',(now() AT TIME ZONE 'UTC')::date,1000000,1,$2,1)")
        .bind(account).bind(if silent { 960_000i64 } else { 100_000 }).execute(pool).await.unwrap();
    // Protocol-0 receipts are only legal for cloud embedding.
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) \
        VALUES($1,'cloud_embedding',(now() AT TIME ZONE 'UTC')::date,1000000)")
        .bind(account).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO managed_attempt_receipts(receipt_id,account_id,purpose,period_start,graph_id,logical_call,attempt_no,candidate_models, \
          input_bound_tokens,max_tokens,reserved_micro_usd,state,created_at) \
        VALUES(gen_random_uuid(),$1,'cloud_embedding',(now() AT TIME ZONE 'UTC')::date,$2,gen_random_uuid(),1,'{m}',1,1,1,'reserved',now()-$3::interval)")
        .bind(account).bind(graph).bind(if silent { "45 minutes" } else { "1 minute" }).execute(pool).await.unwrap();
    // A control-plane credit hold for this plane, inside the financial entry
    // the billing guard requires.
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(control, account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account).await.unwrap();
    sqlx::query("INSERT INTO managed_credit_reservations(plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash,bound_micro_usd, \
          bound_credits,conversion_effective_from,reference_cost_per_credit,funding_basis,state,created_at) \
        VALUES($1,gen_random_uuid(),$2,$2,$3,gen_random_uuid(),repeat('a',64),1,1,now(),1,'pool','held',now()-$4::interval)")
        .bind(plane).bind(account).bind(graph).bind(if silent { "45 minutes" } else { "1 minute" }).execute(&mut *financial).await.unwrap();
    financial.commit().await.unwrap();
}

fn event_set(report: &StageReport) -> BTreeSet<String> {
    evaluate(report, &Thresholds::default()).into_iter().map(|f| f.event).collect()
}

/// The parts of a report that do not depend on which role read it.
fn stages(report: &StageReport) -> Value {
    let mut v = serde_json::to_value(report).unwrap();
    v.as_object_mut().unwrap().remove("measured_at");
    v
}

async fn org_plane(p: &mut Planes, opts: &PgConnectOptions, silent: bool) -> (PgPool, PgPool) {
    let (owner, app) = tenant_plane(p, opts).await;
    let (user, account, _) = account(&p.control, false).await;
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Org','member',$2) RETURNING id")
        .bind(account)
        .bind(user)
        .fetch_one(&owner)
        .await
        .unwrap();
    seed(&owner, &p.control, graph, account, user, &format!("org:{ORG}"), silent).await;
    (owner, app)
}

async fn admin(p: &Planes, format: Option<&str>) -> axum::response::Response {
    std::env::set_var("INTERNAL_TOKEN", "stage-health-test-token");
    let mut headers = HeaderMap::new();
    headers.insert("x-internal-token", "stage-health-test-token".parse().unwrap());
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db(p.control_app.clone(), None));
    admin_pipeline_stage_health(
        State(state),
        headers,
        Query(PipelineStageHealthQuery { format: format.map(str::to_string), tenant: Some(ORG.into()), hours: Some(24) }),
    )
    .await
}

async fn body(resp: axum::response::Response) -> (StatusCode, String) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 22).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_stages_alert_on_both_roles() {
    let Some(opts) = base_opts() else { return };
    let mut p = control_plane(&opts).await;
    let (owner, app) = org_plane(&mut p, &opts, true).await;
    let th = Thresholds::default();
    let now = chrono::Utc::now();
    let as_owner = measure(&worker(TenantId::Org(ORG.into()), &owner), &p.control, now, &th).await.unwrap();
    let as_app = measure(&worker(TenantId::Org(ORG.into()), &app), &p.control_app, now, &th).await.unwrap();
    assert_eq!(stages(&as_owner), stages(&as_app), "the restricted role reads exactly what the owner reads");
    let fired = event_set(&as_app);
    let expected: BTreeSet<String> = SILENT_EVENTS.iter().map(|e| e.to_string()).collect();
    assert_eq!(fired, expected, "every silent stage alerts, and nothing else does");
    let findings = evaluate(&as_app, &th);
    let stalled: BTreeSet<_> = findings
        .iter()
        .filter(|f| f.event == "pipeline_extraction_stalled")
        .filter_map(|f| f.source_type.clone())
        .collect();
    assert_eq!(stalled, BTreeSet::from(["github_code".to_string()]), "the excluded `note` waits are held, not stalled");
    let w2 = as_app.window(2).unwrap().source("slack").unwrap();
    assert_eq!((w2.atoms, w2.embedded, w2.embed_blocked), (5, 0, 5));
    let holds: BTreeSet<_> = as_app.snapshot.stuck_holds.iter().map(|h| h.purpose.clone()).collect();
    assert_eq!(holds, BTreeSet::from(["cloud_embedding".to_string(), "credit_reservation".to_string()]));

    store(&p.control_app, &as_app).await.unwrap();
    let (status, json) = body(admin(&p, None).await).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    let json: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(json["healthy"], false);
    let events: BTreeSet<String> = json["planes"][0]["findings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["event"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(events, expected);
    let (status, text) = body(admin(&p, Some("text")).await).await;
    assert_eq!(status, StatusCode::OK);
    assert!(text.contains("github_code") && text.contains("[pipeline_embed_stalled]"), "{text}");
    p.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthy_stages_stay_quiet_on_both_roles() {
    let Some(opts) = base_opts() else { return };
    let mut p = control_plane(&opts).await;
    let (owner, app) = org_plane(&mut p, &opts, false).await;
    let th = Thresholds::default();
    let now = chrono::Utc::now();
    let as_owner = measure(&worker(TenantId::Org(ORG.into()), &owner), &p.control, now, &th).await.unwrap();
    let as_app = measure(&worker(TenantId::Org(ORG.into()), &app), &p.control_app, now, &th).await.unwrap();
    assert_eq!(stages(&as_owner), stages(&as_app));
    assert_eq!(event_set(&as_app), BTreeSet::new(), "{:#?}", evaluate(&as_app, &th));
    // The fixture really flows: the quiet result is not an empty read.
    let w24 = as_app.window(24).unwrap();
    assert!(w24.source("slack").is_some_and(|s| s.embedded == 5));
    assert!(w24.source("linear_issue").is_some_and(|s| s.done_linked == 3));
    assert!(w24.graph.relationship_edges == 1 && w24.graph.er_judgements == 1 && w24.graph.jev_ok == 6);
    assert_eq!(w24.dead_jobs.iter().map(|k| k.count).sum::<i64>(), 5);
    p.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_plane_alerts_per_account_and_skips_processing_off() {
    let Some(opts) = base_opts() else { return };
    let p = control_plane(&opts).await;
    sqlx::query("DROP INDEX IF EXISTS nodes_graph_type_lower_title_unique").execute(&p.control).await.unwrap();
    let (user, live, graph) = account(&p.control, true).await;
    seed(&p.control, &p.control, graph.unwrap(), live, user, "consumer", true).await;
    let (off_user, off, off_graph) = account(&p.control, true).await;
    sqlx::query("INSERT INTO account_prefs(account_id,prefs) VALUES($1,'{\"managed_processing\":\"off\"}')")
        .bind(off)
        .execute(&p.control)
        .await
        .unwrap();
    atoms(&p.control, off_graph.unwrap(), "gmail", 9, "1 hour", false, None, None).await;
    let _ = off_user;
    let th = Thresholds::default();
    let report = measure(&worker(TenantId::Consumer, &p.control_app), &p.control_app, chrono::Utc::now(), &th).await.unwrap();
    assert_eq!((report.accounts_measured, report.accounts_processing_off), (1, 1));
    let fired = event_set(&report);
    assert!(fired.contains("pipeline_embed_stalled") && fired.contains("pipeline_hold_stuck"), "{fired:?}");
    assert!(report.window(2).unwrap().source("gmail").is_none(), "a processing-off account is not measured");
    p.drop().await;
}
