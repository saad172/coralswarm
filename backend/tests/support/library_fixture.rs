//! A control-plane database with one Model-ready graph, and the request and
//! log plumbing the Library serving-path suites share. The suites panic
//! without `LIBRARY_PROOF_TEST_BASE_URL`, so they cannot pass vacuously.
use axum::{body::to_bytes, http::StatusCode, Extension};
use coralswarm_backend::{
    library_read::{self, evidence::READ_MODEL_VERSION, LibraryJson as Json, LibraryScope, SearchRequest},
    store::PostgresStore,
    tenancy::extractor::Tenant,
    workers::library_evidence::{self, Limits},
    UserContext,
};
use serde_json::{json, Value};
use sqlx::{postgres::{PgConnectOptions, PgPoolOptions}, Connection, Executor, PgConnection, PgPool};
use std::{str::FromStr, sync::{Arc, Mutex}};
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct LogBuf(pub Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogBuf {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}
impl LogBuf {
    /// Every JSON line written since the last drain.
    pub fn drain_lines(&self) -> Vec<Value> {
        let bytes = std::mem::take(&mut *self.0.lock().unwrap());
        String::from_utf8(bytes).unwrap().lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
    }
    /// Fallback event lines and fallback metric lines written since the last drain.
    pub fn drain(&self) -> (Vec<Value>, Vec<Value>) {
        let lines = self.drain_lines();
        let events = lines.iter().filter(|v| v["fields"]["event"] == "library_live_fallback").cloned().collect();
        let metrics = lines.iter().filter(|v| v["fields"]["metric"] == "library_live_fallback").cloned().collect();
        (events, metrics)
    }
}

/// The loopback server the suite runs against, checked against the engine
/// major the caller named.
pub fn base_options() -> (PgConnectOptions, String) {
    let base = std::env::var("LIBRARY_PROOF_TEST_BASE_URL").expect("real isolated PostgreSQL required; no skipped serving-path proof");
    let options = PgConnectOptions::from_str(&base).unwrap();
    assert!(matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"));
    (options, base)
}

pub async fn assert_engine(admin: &mut PgConnection) -> String {
    let version: String = sqlx::query_scalar("SHOW server_version").fetch_one(admin).await.unwrap();
    let expected = std::env::var("LIBRARY_PROOF_TEST_PG_MAJOR").expect("explicit engine major required");
    assert!(version.starts_with(&format!("{expected}.")), "wrong engine: {version}");
    version
}

pub struct Member {
    pub user: UserContext,
    pub graph: Uuid,
}
pub async fn member(owner: &PgPool) -> Member {
    let (user, account, graph) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)").bind(user).bind(format!("{user}@fallback.invalid")).execute(owner).await.unwrap();
    sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)").bind(account).bind(user).execute(owner).await.unwrap();
    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')").bind(account).bind(user).execute(owner).await.unwrap();
    sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES($1,$2,'Fallback fixture','member',$3)").bind(graph).bind(account).bind(user).execute(owner).await.unwrap();
    Member { user: UserContext { user_id: user, account_id: account, role: "owner".into(), device_id: "library-fallback".into() }, graph }
}

/// Links the node to a permitted captured outcome so a node-scoped search
/// passes object validation and reaches the program.
pub async fn permit(owner: &PgPool, graph: Uuid, node: Uuid) {
    let origin = json!({"service_id":"slack","content_kind":"message","provider_namespace":"fallback","connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"C1","provider_namespace":"fallback"}});
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack','fallback','fallback',$2,'permitted') RETURNING source_item_id")
        .bind(graph).bind(&origin).fetch_one(owner).await.unwrap();
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,extraction_status) VALUES($1,$2,'fallback','slack','fallback',$3,'Atlas evidence.','{}','2026-02-01','done') RETURNING version_id")
        .bind(graph).bind(item).bind(&origin).fetch_one(owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(owner).await.unwrap();
    let outcome: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact','Atlas evidence.','source-atoms-v1','fixture','[]') RETURNING semantic_atom_id")
        .bind(graph).bind(version).fetch_one(owner).await.unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(graph).bind(node).bind(outcome).execute(owner).await.unwrap();
}

/// Drives the worker's graph refresh until both evidence and listing state
/// are certified, as `library_read_model_pg` does.
pub async fn certify(app: &PgPool, account: Uuid, graph: Uuid) {
    for _ in 0..5000 {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.tenant_actor','',true)").bind(account.to_string()).execute(&mut *tx).await.unwrap();
        library_evidence::refresh_graph_tx(&mut tx, graph, Limits { nodes: 128, changes: 128 }).await.unwrap();
        let ready: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM library_evidence_state WHERE graph_id=$1 AND ready_version=$2) AND EXISTS(SELECT 1 FROM library_listing_state WHERE graph_id=$1 AND ready_version=3 AND sources_complete AND nodes_complete AND aggregates_complete AND sources_old_complete) AND NOT EXISTS(SELECT 1 FROM library_evidence_changes WHERE graph_id=$1) AND NOT EXISTS(SELECT 1 FROM library_listing_changes WHERE graph_id=$1)")
            .bind(graph).bind(READ_MODEL_VERSION).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        if ready {
            return;
        }
    }
    panic!("fixture graph never became Model-ready");
}

/// The served search handler. Returns the status and the response body.
pub async fn search(tenant: &Tenant, user: &UserContext, scope: Value) -> (StatusCode, Value) {
    let scope: LibraryScope = serde_json::from_value(scope).unwrap();
    let request = SearchRequest { scope, query: "".into(), page_size: Some(5), sources_after: None, outcomes_after: None, nodes_after: None };
    let response = library_read::search(tenant.clone(), Extension(user.clone()), Json(request)).await;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

/// A fresh migrated control database with the shipped listing switch
/// stamped, as the worker's first tick does, plus the app-role pool the
/// served request uses.
pub struct Harness {
    pub admin: PgConnection,
    pub db: coralswarm_backend::test_db::TestDatabase,
    pub owner: PgPool,
    pub app: PgPool,
    pub tenant: Tenant,
    pub version: String,
}
impl Harness {
    pub async fn new(prefix: &str) -> Self {
        let (options, _) = base_options();
        let mut admin = PgConnection::connect_with(&options).await.unwrap();
        let version = assert_engine(&mut admin).await;
        let db = coralswarm_backend::test_db::migrated_database(&options, coralswarm_backend::test_db::Plane::Control, prefix).await;
        let owner = PgPoolOptions::new().max_connections(3).connect_with(options.clone().database(&db)).await.unwrap();
        assert!(library_evidence::stamp_listing_barrier(&owner).await.unwrap(), "the migration ships the switch On with an unstamped generation");
        let app = Self::app_pool(&options, &db).await;
        let tenant = Tenant {
            store: Some(Arc::new(PostgresStore::new(app.clone()))),
            pool: Some(app.clone()),
            decision: None,
            class: coralswarm_backend::route_class::RouteClass::Other,
        };
        Self { admin, db, owner, app, tenant, version }
    }
    pub async fn app_pool(options: &PgConnectOptions, db: &str) -> PgPool {
        PgPoolOptions::new().max_connections(4)
            .after_connect(|connection, _| Box::pin(async move { connection.execute("SET ROLE coralswarm_app").await?; Ok(()) }))
            .connect_with(options.clone().database(db)).await.unwrap()
    }
    /// A member whose graph serves the Model, with one node linked to a
    /// permitted outcome.
    pub async fn ready_member(&self) -> (Member, Uuid) {
        let member = member(&self.owner).await;
        let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Atlas','{}') RETURNING node_id")
            .bind(member.graph).fetch_one(&self.owner).await.unwrap();
        permit(&self.owner, member.graph, node).await;
        certify(&self.app, member.user.account_id, member.graph).await;
        (member, node)
    }
    pub async fn close(mut self) {
        self.app.close().await;
        self.owner.close().await;
        self.admin.execute(format!("DROP DATABASE {} WITH(FORCE)", self.db).as_str()).await.unwrap();
    }
}
