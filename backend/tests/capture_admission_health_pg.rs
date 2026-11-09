//! COR-71: captures that wait too long to become Atoms page through
//! `/internal/admin/chain-health`, and intentional holds do not.
//!
//! Real local Postgres: a fresh control database and a fresh org tenant
//! database per test, the real capture path, the real health worker, and the
//! real chain-health handler on the RLS-enforcing app role.
//!
//!   REQUIRE_PG_SUITE=1 RELINFER_TEST_DATABASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test capture_admission_health_pg -- --test-threads=1
mod support;

use std::str::FromStr;
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use coralswarm_backend::admin_chain_health;
use coralswarm_backend::source_evidence::{self, CaptureSource};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::capture_admission_health::{measure, store};
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

const ORG: &str = "org_cor71";

struct Planes {
    base: PgPool,
    control: PgPool,
    app: PgPool,
    tenant: PgPool,
    dbs: [coralswarm_backend::test_db::TestDatabase; 2],
}

impl Planes {
    async fn drop(self) {
        for pool in [&self.control, &self.app, &self.tenant] {
            pool.close().await;
        }
        for db in &self.dbs {
            sqlx::query(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)"))
                .execute(&self.base)
                .await
                .unwrap();
        }
    }
}

async fn planes() -> Option<Planes> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "RELINFER_TEST_DATABASE_URL required"
        );
        eprintln!("skipping capture admission health test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let opts = PgConnectOptions::from_str(&url).unwrap();
    assert!(
        matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"),
        "test database must be local"
    );
    let base = PgPoolOptions::new().max_connections(2).connect_with(opts.clone()).await.unwrap();
    let dbs = [coralswarm_backend::test_db::TestDatabase::new(&opts, "cor71_ctl"), coralswarm_backend::test_db::TestDatabase::new(&opts, "cor71_ten")];
    for db in &dbs {
        sqlx::query(&format!("CREATE DATABASE {db}")).execute(&base).await.unwrap();
    }
    let control = PgPoolOptions::new().connect_with(opts.clone().database(&dbs[0])).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    // 0083 creates `coralswarm_app`, so on a fresh cluster the role exists only now.
    support::app_role::set_app_role_password_with_retry(&control).await;
    let app = PgPoolOptions::new()
        .connect_with(
            opts.clone()
                .database(&dbs[0])
                .username("coralswarm_app")
                .password(support::app_role::APP_ROLE_TEST_PASSWORD),
        )
        .await
        .unwrap();
    let tenant = PgPoolOptions::new().connect_with(opts.database(&dbs[1])).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(ORG)
        .execute(&tenant)
        .await
        .unwrap();
    Some(Planes { base, control, app, tenant, dbs })
}

async fn consumer_account(control: &PgPool) -> (Uuid, Uuid, Uuid) {
    let tag = Uuid::new_v4();
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$2) RETURNING id")
        .bind(format!("cor71-{tag}@test.local"))
        .bind(format!("cor71-{tag}"))
        .fetch_one(control)
        .await
        .unwrap();
    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
        .bind(user)
        .fetch_one(control)
        .await
        .unwrap();
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'cor71') RETURNING id")
        .bind(account)
        .fetch_one(control)
        .await
        .unwrap();
    (user, account, graph)
}

/// The org's account lives on control; its oceans live in the tenant database.
async fn org_account(control: &PgPool, tenant: &PgPool) -> (Uuid, Uuid, Uuid) {
    let (user, account, _) = consumer_account(control).await;
    let mut tx = begin_account_scoped(control, account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account).await.unwrap();
    sqlx::query("UPDATE accounts SET clerk_org_id=$1,is_org_account=true WHERE id=$2")
        .bind(ORG)
        .bind(account)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active))")
        .bind(account)
        .bind(user)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) VALUES($1,$2,'cor71',$3,'active')")
        .bind(ORG)
        .bind(account)
        .bind(format!("secret-{ORG}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let graph = Uuid::new_v4();
    sqlx::query("INSERT INTO graphs(id,account_id,name,kind,is_primary) VALUES($1,$2,'commons','org',true)")
        .bind(graph)
        .bind(account)
        .execute(tenant)
        .await
        .unwrap();
    (user, account, graph)
}

async fn capture_with(
    mut tx: sqlx::Transaction<'static, sqlx::Postgres>,
    account: Uuid,
    graph: Uuid,
    user: Uuid,
) -> Uuid {
    let source_id = Uuid::new_v4().to_string();
    let capture = CaptureSource {
        source_type: "slack",
        provider_namespace: "slack:T1",
        source_id: &source_id,
        normalized_text: "Alice will ship the roadmap on Friday.",
        representation: "normalized_text",
        metadata: &json!({"title": "standup"}),
        occurred_at: None,
        participants: &[],
        ingested_by: Some(user),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let saved = source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[7; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    saved.version_id
}

async fn capture_consumer(app: &PgPool, account: Uuid, graph: Uuid, user: Uuid) -> Uuid {
    capture_with(begin_account_scoped(app, account).await.unwrap(), account, graph, user).await
}

async fn capture_org(tenant: &PgPool, account: Uuid, graph: Uuid, user: Uuid) -> Uuid {
    capture_with(tenant.begin().await.unwrap(), account, graph, user).await
}

async fn receipt(pool: &PgPool, version: Uuid) -> (String, Option<String>, String) {
    sqlx::query_as(
        "SELECT r.state, r.reason, v.extraction_status FROM corpus_capture_admissions r \
         JOIN source_versions v ON v.version_id=r.source_version_id WHERE r.source_version_id=$1",
    )
    .bind(version)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn captured_at(pool: &PgPool, version: Uuid, at: &str) {
    sqlx::query("UPDATE source_versions SET captured_at=$2::timestamptz WHERE version_id=$1")
        .bind(version)
        .bind(at)
        .execute(pool)
        .await
        .unwrap();
}

fn worker(tenant: TenantId, pool: &PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
    }
}

async fn sweep(p: &Planes) {
    let now = chrono::Utc::now();
    for t in [worker(TenantId::Consumer, &p.app), worker(TenantId::Org(ORG.into()), &p.tenant)] {
        let report = measure(&t, &p.control, now).await.unwrap();
        store(&p.control, &report).await.unwrap();
    }
}

async fn chain_health(p: &Planes) -> Value {
    std::env::set_var("INTERNAL_TOKEN", "cor71-test-token");
    let mut headers = HeaderMap::new();
    headers.insert("x-internal-token", "cor71-test-token".parse().unwrap());
    let state = Arc::new(coralswarm_backend::test_app_state_owned_with_db(p.app.clone(), None));
    let (status, body) = admin_chain_health(axum::extract::State(state), headers).await;
    assert_eq!(status, StatusCode::OK, "{:?}", body.0);
    body.0
}

fn stalled(body: &Value) -> Vec<(String, String, String, i64)> {
    let mut out: Vec<_> = body["capture_admission"]["stalled"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            let c = &s["captures"];
            (
                s["tenant"].as_str().unwrap().to_string(),
                c["state"].as_str().unwrap().to_string(),
                c["reason"].as_str().unwrap_or("").to_string(),
                c["over_alert_age"].as_i64().unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

/// Production since 2026-10-13: every capture is retained but stamped
/// `blocked/managed_admission_required`, and nothing adopts it.
#[tokio::test]
async fn captures_blocked_since_the_fence_alert_on_both_planes() {
    let Some(p) = planes().await else { return };
    let (c_user, c_account, c_graph) = consumer_account(&p.control).await;
    let (o_user, o_account, o_graph) = org_account(&p.control, &p.tenant).await;

    let mut versions = Vec::new();
    for _ in 0..2 {
        versions.push((p.control.clone(), capture_consumer(&p.app, c_account, c_graph, c_user).await));
        versions.push((p.tenant.clone(), capture_org(&p.tenant, o_account, o_graph, o_user).await));
    }
    for (pool, version) in &versions {
        assert_eq!(
            receipt(pool, *version).await,
            ("blocked".into(), Some("managed_admission_required".into()), "blocked".into()),
            "the real capture path reproduces production's admission state"
        );
        captured_at(pool, *version, "2026-10-13T04:31:00Z").await;
    }

    let before = chain_health(&p).await;
    assert_eq!(before["healthy"], json!(false), "a plane nobody measured is not healthy");
    assert_eq!(
        before["capture_admission"]["missing_planes"],
        json!(["__consumer__", ORG])
    );

    sweep(&p).await;
    let body = chain_health(&p).await;
    assert_eq!(body["healthy"], json!(false));
    assert_eq!(body["capture_admission"]["violated"], json!(true));
    assert_eq!(body["capture_admission"]["missing_planes"], json!([]));
    assert_eq!(
        stalled(&body),
        vec![
            ("__consumer__".into(), "blocked".into(), "managed_admission_required".into(), 2),
            (ORG.into(), "blocked".into(), "managed_admission_required".into(), 2),
        ]
    );
    for plane in body["capture_admission"]["planes"].as_array().unwrap() {
        let waiting = &plane["waiting"][0];
        assert_eq!(waiting["oldest_captured_at"], json!("2026-10-13T04:31:00Z"));
        assert_eq!(waiting["disposition"], json!("stalled"));
    }
    p.drop().await;
}

/// An account that switched processing off is reported, not paged; a capture
/// younger than 15 minutes has no stall alert; unknown clock history remains red.
#[tokio::test]
async fn held_and_young_captures_preserve_stall_semantics_and_report_unknown_clocks() {
    let Some(p) = planes().await else { return };
    let (off_user, off_account, off_graph) = consumer_account(&p.control).await;
    let (on_user, on_account, on_graph) = consumer_account(&p.control).await;
    let (o_user, o_account, o_graph) = org_account(&p.control, &p.tenant).await;
    for (account, prefs) in [
        (off_account, json!({"managed_processing": "off"})),
        (o_account, json!({"managed_processing": "off"})),
    ] {
        sqlx::query("INSERT INTO account_prefs(account_id,prefs) VALUES($1,$2)")
            .bind(account)
            .bind(prefs)
            .execute(&p.control)
            .await
            .unwrap();
    }
    for (pool, version) in [
        (&p.control, capture_consumer(&p.app, off_account, off_graph, off_user).await),
        (&p.tenant, capture_org(&p.tenant, o_account, o_graph, o_user).await),
    ] {
        captured_at(pool, version, "2026-10-13T04:31:00Z").await;
    }
    let young = capture_consumer(&p.app, on_account, on_graph, on_user).await;
    let finished = capture_consumer(&p.app, on_account, on_graph, on_user).await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '7 minutes',extraction_status='done' WHERE version_id=$1")
        .bind(finished)
        .execute(&p.control)
        .await
        .unwrap();
    sqlx::query("INSERT INTO semantic_atom_windows(source_version_id,graph_id,extraction_version,start_char,end_char,completed_at) SELECT version_id,graph_id,extraction_version,0,10,captured_at+interval '5 minutes' FROM source_versions WHERE version_id=$1")
        .bind(finished)
        .execute(&p.control)
        .await
        .unwrap();

    sweep(&p).await;
    let body = chain_health(&p).await;
    assert_eq!(body["capture_admission"]["violated"], json!(true), "unknown receipt history must remain red: {body:#}");
    assert!(!body["capture_admission"]["unknown_clock"].as_array().unwrap().is_empty());
    assert!(stalled(&body).is_empty());

    let planes = body["capture_admission"]["planes"].as_array().unwrap();
    let mut seen: Vec<(String, String, Option<String>, i64)> = planes
        .iter()
        .flat_map(|plane| plane["waiting"].as_array().unwrap().iter())
        .map(|w| {
            (
                w["disposition"].as_str().unwrap().to_string(),
                w["graph_id"].as_str().unwrap().to_string(),
                w["hold"].as_str().map(str::to_owned),
                w["over_alert_age"].as_i64().unwrap(),
            )
        })
        .collect();
    seen.sort();
    let mut expected = vec![
        ("held".to_string(), off_graph.to_string(), Some("account_processing_off".to_string()), 1),
        ("held".to_string(), o_graph.to_string(), Some("account_processing_off".to_string()), 1),
        ("stalled".to_string(), on_graph.to_string(), None, 0),
    ];
    expected.sort();
    assert_eq!(seen, expected);
    assert_eq!(receipt(&p.control, young).await.0, "blocked");

    assert!(planes.iter().flat_map(|plane|plane["lag"].as_array().unwrap()).all(|row|row["graph_id"]!=json!(on_graph.to_string())),"a completed source without trusted never-held receipt history is not an eligible percentile sample");
    p.drop().await;
}

/// Rewrites a real capture's receipt into a clock state the standing worker
/// would produce, with the receipt's immutability trigger off for the edit.
async fn set_clock(pool: &PgPool, version: Uuid, sql: &str) {
    sqlx::query("ALTER TABLE corpus_capture_admissions DISABLE TRIGGER corpus_admission_observation_immutable")
        .execute(pool).await.unwrap();
    sqlx::query(sql).bind(version).execute(pool).await.unwrap();
    sqlx::query("ALTER TABLE corpus_capture_admissions ENABLE TRIGGER corpus_admission_observation_immutable")
        .execute(pool).await.unwrap();
}

/// Production, 2026-10-20 to 21: an operator repinned 457 versions for
/// re-extraction and re-captured about 1,300 week-old Slack messages. The
/// queue kept moving (about 300 windows an hour), yet every hourly check
/// failed and emailed: 2,300 queued captures read as "stalled", and the
/// re-captured history counted as fresh captures with a 7-hour lag.
///
/// A draining backlog is an SLO breach, not a page; history is not a live
/// capture; a capture outside the queue that nothing retries for 6 hours, or
/// a graph that stops completing windows, still pages.
#[tokio::test]
async fn a_draining_backlog_breaches_the_slo_without_paging_and_history_is_not_fresh() {
    let Some(p) = planes().await else { return };
    let (o_user, o_account, o_graph) = org_account(&p.control, &p.tenant).await;
    let live = capture_org(&p.tenant, o_account, o_graph, o_user).await;
    let history = capture_org(&p.tenant, o_account, o_graph, o_user).await;
    let queued = capture_org(&p.tenant, o_account, o_graph, o_user).await;
    // Both finished an hour ago, two hours after their receipts: 7,200 s.
    for version in [live, history] {
        sqlx::query("UPDATE source_versions SET extraction_status='done',captured_at=now()-interval '3 hours' WHERE version_id=$1")
            .bind(version).execute(&p.tenant).await.unwrap();
        set_clock(&p.tenant, version, "UPDATE corpus_capture_admissions SET state='processed',reason=NULL,hold_basis='standing_admitted',ever_held=false,observed_at=now()-interval '3 hours',processed_at=now()-interval '1 hour' WHERE source_version_id=$1").await;
    }
    sqlx::query("UPDATE source_versions SET occurred_at=captured_at-interval '9 days' WHERE version_id=$1")
        .bind(history).execute(&p.tenant).await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_status='pending',extraction_error=NULL,captured_at=now()-interval '2 hours' WHERE version_id=$1")
        .bind(queued).execute(&p.tenant).await.unwrap();
    set_clock(&p.tenant, queued, "UPDATE corpus_capture_admissions SET state='queued',reason=NULL,hold_basis='standing_admitted',ever_held=false,observed_at=now()-interval '2 hours' WHERE source_version_id=$1").await;
    // The graph completed a window five minutes ago: its queue is moving.
    sqlx::query("INSERT INTO semantic_atom_windows(source_version_id,graph_id,extraction_version,start_char,end_char,completed_at) SELECT version_id,graph_id,extraction_version,0,10,now()-interval '5 minutes' FROM source_versions WHERE version_id=$1")
        .bind(live).execute(&p.tenant).await.unwrap();

    sweep(&p).await;
    let body = chain_health(&p).await;
    let capture = &body["capture_admission"];
    assert_eq!(body["healthy"], json!(false), "{body:#}");
    assert_eq!((body["page"].clone(), capture["page"].clone(), capture["slo_breached"].clone()), (json!(false), json!(false), json!(true)), "{capture:#}");
    assert!(stalled(&body).is_empty(), "a queue that is draining is not stalled: {capture:#}");
    let lag: Vec<(String, i64, i64)> = capture["lag_violations"].as_array().unwrap().iter()
        .map(|row| (row["lag"]["graph_id"].as_str().unwrap().to_string(), row["lag"]["processed"].as_i64().unwrap(), row["lag"]["max_secs"].as_f64().unwrap().round() as i64))
        .collect();
    assert_eq!(lag, vec![(o_graph.to_string(), 1, 7200)], "only the live capture is an SLO sample; re-captured history is not");
    let queued_lag: Vec<i64> = capture["queued_lag_violations"].as_array().unwrap().iter()
        .map(|row| row["captures"]["over_lag_age"].as_i64().unwrap()).collect();
    assert_eq!(queued_lag, vec![1], "{capture:#}");

    // A capture blocked without a decision for 7 hours is stuck, draining or not.
    let stuck = capture_org(&p.tenant, o_account, o_graph, o_user).await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '7 hours' WHERE version_id=$1")
        .bind(stuck).execute(&p.tenant).await.unwrap();
    sweep(&p).await;
    let body = chain_health(&p).await;
    assert_eq!(body["page"], json!(true));
    assert_eq!(stalled(&body), vec![(ORG.into(), "blocked".into(), "managed_admission_required".into(), 1)]);

    // The graph stops completing windows: its queue is stalled too.
    sqlx::query("UPDATE semantic_atom_windows SET completed_at=now()-interval '2 hours' WHERE graph_id=$1")
        .bind(o_graph).execute(&p.tenant).await.unwrap();
    sweep(&p).await;
    let body = chain_health(&p).await;
    assert_eq!(body["page"], json!(true));
    assert_eq!(
        stalled(&body),
        vec![
            (ORG.into(), "blocked".into(), "managed_admission_required".into(), 1),
            (ORG.into(), "queued".into(), "".into(), 1),
        ]
    );
    p.drop().await;
}

async fn capture_item(tenant: &PgPool, account: Uuid, graph: Uuid, user: Uuid, id: &str, text: &str, metadata: Value) -> Uuid {
    let mut tx = tenant.begin().await.unwrap();
    let capture = CaptureSource {
        source_type: "slack",
        provider_namespace: "slack:T1",
        source_id: id,
        normalized_text: text,
        representation: "normalized_text",
        metadata: &metadata,
        occurred_at: None,
        participants: &[],
        ingested_by: Some(user),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let saved = source_evidence::capture_source_version_tx(&mut tx, None, graph, account, &capture, None, &[7; 32]).await.unwrap();
    tx.commit().await.unwrap();
    saved.version_id
}

/// #1411 follow-ups, in a graph whose queue is moving:
/// * a capture queued for 48 hours without its receipt changing state is
///   stranded and pages (a stale capture re-queued an hour ago does not);
/// * an edit of old content (new content, original `occurred_at`) is a live
///   SLO sample, while a same-content re-capture of old content is not.
#[tokio::test]
async fn a_stranded_queued_capture_pages_and_an_edit_is_a_live_sample() {
    let Some(p) = planes().await else { return };
    let (o_user, o_account, o_graph) = org_account(&p.control, &p.tenant).await;
    sqlx::query("INSERT INTO semantic_atom_windows(source_version_id,graph_id,extraction_version,start_char,end_char,completed_at) SELECT version_id,graph_id,extraction_version,0,10,now()-interval '5 minutes' FROM source_versions WHERE version_id=$1")
        .bind(capture_org(&p.tenant, o_account, o_graph, o_user).await).execute(&p.tenant).await.unwrap();
    sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE graph_id=$1").bind(o_graph).execute(&p.tenant).await.unwrap();

    let stranded = capture_org(&p.tenant, o_account, o_graph, o_user).await;
    let requeued = capture_org(&p.tenant, o_account, o_graph, o_user).await;
    for (version, since) in [(stranded, "49 hours"), (requeued, "1 hour")] {
        sqlx::query("UPDATE source_versions SET extraction_status='pending',extraction_error=NULL,captured_at=now()-interval '3 days' WHERE version_id=$1")
            .bind(version).execute(&p.tenant).await.unwrap();
        set_clock(&p.tenant, version, &format!("UPDATE corpus_capture_admissions SET state='queued',reason=NULL,hold_basis='backlog_policy_excluded',ever_held=true,updated_at=now()-interval '{since}' WHERE source_version_id=$1")).await;
    }

    // Two items first captured a week ago; each gets a second version now.
    let first_edit = capture_item(&p.tenant, o_account, o_graph, o_user, "edited", "Alice will ship on Friday.", json!({})).await;
    let first_recapture = capture_item(&p.tenant, o_account, o_graph, o_user, "recaptured", "Bob owns the launch.", json!({})).await;
    sqlx::query("UPDATE source_versions SET captured_at=now()-interval '7 days',occurred_at=now()-interval '7 days',extraction_status='done' WHERE version_id=ANY($1)")
        .bind(vec![first_edit, first_recapture]).execute(&p.tenant).await.unwrap();
    let edit = capture_item(&p.tenant, o_account, o_graph, o_user, "edited", "Alice will ship on Monday.", json!({})).await;
    let recapture = capture_item(&p.tenant, o_account, o_graph, o_user, "recaptured", "Bob owns the launch.", json!({"anchor": "added"})).await;
    for version in [edit, recapture] {
        sqlx::query("UPDATE source_versions SET extraction_status='done',occurred_at=now()-interval '7 days',captured_at=now()-interval '3 hours' WHERE version_id=$1")
            .bind(version).execute(&p.tenant).await.unwrap();
        set_clock(&p.tenant, version, "UPDATE corpus_capture_admissions SET state='processed',reason=NULL,hold_basis='standing_admitted',ever_held=false,observed_at=now()-interval '3 hours',processed_at=now()-interval '1 hour' WHERE source_version_id=$1").await;
    }

    sweep(&p).await;
    let body = chain_health(&p).await;
    let capture = &body["capture_admission"];
    let stalled_groups: Vec<(String, i64)> = capture["stalled"].as_array().unwrap().iter()
        .map(|s| (s["captures"]["state"].as_str().unwrap().to_string(), s["captures"]["captures"].as_i64().unwrap())).collect();
    assert_eq!(body["page"], json!(true), "{capture:#}");
    assert_eq!(stalled_groups, vec![("queued".to_string(), 2)], "the group pages because its oldest has sat queued for 48 h: {capture:#}");
    let lag: Vec<i64> = capture["lag_violations"].as_array().unwrap().iter()
        .filter(|row| row["lag"]["graph_id"] == json!(o_graph.to_string()))
        .map(|row| row["lag"]["processed"].as_i64().unwrap()).collect();
    assert_eq!(lag, vec![1], "the edit is a live sample; the same-content re-capture is not: {capture:#}");

    // The stranded capture's receipt changes state (re-queued): no page.
    set_clock(&p.tenant, stranded, "UPDATE corpus_capture_admissions SET updated_at=now()-interval '2 hours' WHERE source_version_id=$1").await;
    sweep(&p).await;
    let body = chain_health(&p).await;
    assert_eq!(body["page"], json!(false), "{:#}", body["capture_admission"]);
    p.drop().await;
}
