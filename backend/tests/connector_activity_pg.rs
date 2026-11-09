//! Connector admission regression, including the PostgreSQL 16 lateral-join plan.
//! Uses the canonical page-model fixture and the registered worker's real budget.
#[path = "support/page_model_fixture.rs"]
mod fixture;

use coralswarm_backend::{
    read_models::{self as rm, connectors::ConnectorDetail, ReadModel},
    tenancy::scope::begin_tenant_scoped,
    workers::read_model_refresh,
};
use fixture::Fixture;
use serde_json::Value;
use sqlx::Row;
use std::time::Instant;
use uuid::Uuid;

async fn scope(f: &Fixture) -> rm::Scope<rm::connectors::DetailParams> {
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let mut viewer = rm::Viewer::load(&mut tx, f.account, f.user).await.unwrap();
    let mut control = begin_tenant_scoped(&f.control, f.account, f.user)
        .await
        .unwrap();
    viewer.certify_control(&mut control).await.unwrap();
    viewer
        .scope::<ConnectorDetail>(rm::connectors::DetailParams {
            provider: "google".into(),
        })
        .unwrap()
}

async fn atom(f: &Fixture, graph: Uuid, user: Uuid, source: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,ingested_by,content_blob_key,content_inline,title,timestamp) VALUES($1,'gmail',$2,$3,'fixture','Connector evidence','Connector evidence',now()) RETURNING atom_id")
        .bind(graph).bind(source).bind(user).fetch_one(&f.owner).await.unwrap()
}

async fn activity(
    f: &Fixture,
    c: &rm::store::Claim,
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    control: &mut sqlx::Transaction<'static, sqlx::Postgres>,
) -> (Value, [u32; 7]) {
    let params = rm::connectors::DetailParams {
        provider: "google".into(),
    };
    let payload = ConnectorDetail::compute(
        rm::ComputeCx {
            tx,
            control_tx: Some(control),
            blobs: None,
            account_id: f.account,
            compute_as: f.user,
            graphs: &c.graph_ids,
            now: chrono::Utc::now(),
        },
        &params,
    )
    .await
    .unwrap();
    let gmail = payload["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["key"] == "gmail")
        .unwrap();
    (
        gmail["last_captured_at"].clone(),
        serde_json::from_value(gmail["week"].clone()).unwrap(),
    )
}

async fn small_admission(org: bool) {
    let f = Fixture::new(org).await;
    let good = atom(&f, f.graph, f.user, "own").await;
    atom(&f, f.graph, f.other, "other-ingester").await;
    atom(&f, f.foreign, f.user, "other-member-graph").await;
    for (name, column) in [
        ("retracted", "evidence_retracted"),
        ("derived", "derived_copy_bound"),
    ] {
        let id = atom(&f, f.graph, f.user, name).await;
        sqlx::query(&format!("UPDATE atoms SET {column}=true WHERE atom_id=$1"))
            .bind(id)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    atom(&f, f.graph, f.user, "denied-source").await;
    sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'gmail','connector-regression','denied-source','restricted')")
        .bind(f.graph).execute(&f.owner).await.unwrap();
    let old = atom(&f, f.graph, f.user, "old").await;
    sqlx::query("UPDATE atoms SET created_at=now()-interval '10 days' WHERE atom_id=$1")
        .bind(old)
        .execute(&f.owner)
        .await
        .unwrap();
    if org {
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Windowed connector graph','team') RETURNING id")
            .bind(f.account).fetch_one(&f.owner).await.unwrap();
        let team: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Connector history floor',$3) RETURNING id")
            .bind(f.account).bind(graph).bind(f.user).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')")
            .bind(team)
            .bind(f.user)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'seed',now()-interval '2 days')")
            .bind(f.account).bind(team).bind(f.user).execute(&f.owner).await.unwrap();
        let before = atom(&f, graph, f.user, "before-floor").await;
        sqlx::query("UPDATE atoms SET timestamp=now()-interval '3 days' WHERE atom_id=$1")
            .bind(before)
            .execute(&f.owner)
            .await
            .unwrap();
        atom(&f, graph, f.user, "after-floor").await;
    }
    let s = scope(&f).await;
    rm::store::request_refresh::<ConnectorDetail>(&f.app, &s)
        .await
        .unwrap();
    let c = rm::store::claim(&f.app).await.unwrap().remove(0);
    let mut tx = rm::store::begin_compute(&f.app, &c, ConnectorDetail::COMPUTE_TIMEOUT)
        .await
        .unwrap();
    let mut control = rm::store::begin_compute(&f.control, &c, ConnectorDetail::COMPUTE_TIMEOUT)
        .await
        .unwrap();
    let (last, week) = activity(&f, &c, &mut tx, &mut control).await;
    assert!(!last.is_null());
    assert_eq!(
        week.iter().sum::<u32>(),
        if org { 2 } else { 1 },
        "own-ingester, graph, retraction, derived-copy, source and history fences"
    );
    sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1")
        .bind(good)
        .execute(&f.owner)
        .await
        .unwrap();
    let same = activity(&f, &c, &mut tx, &mut control).await;
    assert_eq!(
        same,
        (last, week),
        "admission stays inside the worker snapshot"
    );
    tx.rollback().await.unwrap();
    control.rollback().await.unwrap();
    let mut tx = rm::store::begin_compute(&f.app, &c, ConnectorDetail::COMPUTE_TIMEOUT)
        .await
        .unwrap();
    let mut control = rm::store::begin_compute(&f.control, &c, ConnectorDetail::COMPUTE_TIMEOUT)
        .await
        .unwrap();
    let changed = activity(&f, &c, &mut tx, &mut control).await;
    assert_eq!(changed.1.iter().sum::<u32>(), if org { 1 } else { 0 });
    tx.rollback().await.unwrap();
    control.rollback().await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn connector_activity_control_admission_and_snapshot() {
    small_admission(false).await;
}

#[tokio::test]
async fn connector_activity_tenant_admission_and_snapshot() {
    small_admission(true).await;
}

#[tokio::test]
#[ignore = "explicit canonical 600k PG16 regression; run with --ignored"]
async fn connector_detail_registered_worker_600k() {
    let f = Fixture::new(true).await;
    // Same source and substitutions as the existing L3/L4 scale harnesses.
    // Keep the fixture's small private graphs too, exercising both admission arms.
    let mut seed = include_str!("fixtures/page_models_600k.sql")
        .lines()
        .filter(|l| !l.starts_with('\\') && !l.starts_with("VACUUM"))
        .map(|l| l.split("--").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    for (name, id) in [("acct", f.account), ("u1", f.user), ("u2", f.other)] {
        seed = seed.replace(&format!(":{name}"), &format!("'{id}'"));
    }
    for (name, id) in [
        ("g_org", "22222222-0000-0000-0000-000000000001"),
        ("g_team", "22222222-0000-0000-0000-000000000002"),
        ("g_mem", "22222222-0000-0000-0000-000000000003"),
    ] {
        seed = seed.replace(&format!(":{name}"), &format!("'{id}'"));
    }
    let mut conn = f.owner.acquire().await.unwrap();
    for statement in seed.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::raw_sql(statement).execute(&mut *conn).await.unwrap();
    }
    drop(conn);
    for table in [
        "atoms",
        "nodes",
        "node_atoms",
        "source_items",
        "source_versions",
        "source_version_chunks",
        "semantic_atoms",
        "node_semantic_atoms",
    ] {
        sqlx::query(&format!("VACUUM ANALYZE {table}"))
            .execute(&f.owner)
            .await
            .unwrap();
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms")
        .fetch_one(&f.owner)
        .await
        .unwrap();
    assert_eq!(count, 600_000);
    let own = atom(&f, f.graph, f.user, "scale-own-capture").await;
    let foreign = atom(&f, f.foreign, f.user, "scale-private-capture").await;
    let s = scope(&f).await;
    rm::store::request_refresh::<ConnectorDetail>(&f.app, &s)
        .await
        .unwrap();
    let model = rm::registry::by_name(ConnectorDetail::NAME).unwrap();
    let start = Instant::now();
    // tick uses the registered model's 30s statement / 35s outer compute budget.
    read_model_refresh::tick(&f.worker(), &f.control)
        .await
        .unwrap();
    let elapsed = start.elapsed();
    let mut tx = begin_tenant_scoped(&f.app, f.account, f.user)
        .await
        .unwrap();
    let stored = rm::store::read::<ConnectorDetail>(&mut tx, &s)
        .await
        .unwrap();
    assert!(
        stored.computed_at.is_some(),
        "real registered worker must publish within its unchanged budget; elapsed={elapsed:?}"
    );
    assert!(!stored.stale);
    assert!(elapsed < read_model_refresh::compute_total_timeout(model));
    let captures = stored.payload["captures"]["items"].as_array().unwrap();
    assert!(captures.iter().any(|c| c["atom_id"] == own.to_string()));
    assert!(!captures.iter().any(|c| c["atom_id"] == foreign.to_string()));
    let state = sqlx::query("SELECT failed_attempts,last_error,retry_after FROM read_models WHERE model=$1 AND scope_key=$2")
        .bind(ConnectorDetail::NAME).bind(s.key()).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(state.get::<i32, _>("failed_attempts"), 0);
    assert!(state.get::<Option<String>, _>("last_error").is_none());
    assert!(state
        .get::<Option<chrono::DateTime<chrono::Utc>>, _>("retry_after")
        .is_none());
    eprintln!(
        "CONNECTOR_WORKER_600K_PG elapsed_ms={}",
        elapsed.as_millis()
    );
    tx.rollback().await.unwrap();
    f.close().await;
}
