//! Workspace handlers under actual app roles. Scratch databases are retained.
use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
    Router,
};
use coralswarm_backend::{
    store::{PostgresStore, Store},
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
    },
    test_support_workspace_app, UserContext,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor, PgPool,
};
use std::{
    fs::OpenOptions,
    io::Write,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tower::ServiceExt;
use uuid::Uuid;

struct Fixture {
    owner: PgPool,
    app: PgPool,
    tenant: Tenant,
    user: UserContext,
    graph: Uuid,
    foreign: Uuid,
    empty: Uuid,
    account: Uuid,
    other_account: Uuid,
    other_actor: Uuid,
}
impl Fixture {
    async fn new(org: bool) -> Self {
        let base = std::env::var("WORKSPACE_TEST_BASE_URL")
            .expect("WORKSPACE_TEST_BASE_URL is required; no skipped workspace proof");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        assert_eq!(opts.get_database(), Some("postgres"));
        let cluster = Uuid::parse_str(
            &std::env::var("WORKSPACE_TEST_OWNED_CLUSTER_ID")
                .expect("fresh exclusively owned cluster allocation is required"),
        )
        .unwrap();
        let port: u16 = std::env::var("WORKSPACE_TEST_OWNED_PORT")
            .expect("audited fresh cluster port is required")
            .parse()
            .unwrap();
        assert_eq!(opts.get_port(), port);
        assert!(![52984, 61247, 61248, 61249].contains(&port));
        let journal = std::env::var("WORKSPACE_TEST_RESOURCE_LOG")
            .expect("append-only retained resource journal is required");
        let nonce = Uuid::new_v4().simple().to_string();
        let role = format!("workspace_login_{nonce}");
        let password = Uuid::new_v4().simple().to_string();
        let name = format!("workspace_{}_{nonce}", if org { "org" } else { "personal" });
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(journal)
            .unwrap();
        writeln!(
            log,
            "planned retained cluster={cluster} port={port} database={name} role={role}"
        )
        .unwrap();
        log.sync_all().unwrap();
        eprintln!("retained workspace fixture cluster={cluster} database={name} role={role}");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    conn.execute("SET statement_timeout='8s'").await?;
                    Ok(())
                })
            })
            .connect_with(opts.clone())
            .await
            .unwrap();
        admin
            .execute(format!("CREATE DATABASE {name}").as_str())
            .await
            .unwrap();
        admin.close().await;
        let owner = PgPoolOptions::new()
            .max_connections(4)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    conn.execute("SET statement_timeout='8s'").await?;
                    Ok(())
                })
            })
            .connect_with(opts.clone().database(&name))
            .await
            .unwrap();
        let base_role = if org {
            sqlx::migrate!("./migrations_tenant")
                .run(&owner)
                .await
                .unwrap();
            "coralswarm_tenant_app"
        } else {
            coralswarm_backend::CONTROL_MIGRATOR
                .run(&owner)
                .await
                .unwrap();
            "coralswarm_app"
        };
        owner.execute(format!("CREATE ROLE {role} LOGIN INHERIT NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '{password}' IN ROLE {base_role}").as_str()).await.unwrap();
        let app = PgPoolOptions::new()
            .max_connections(3)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout='8s'")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(opts.database(&name).username(&role).password(&password))
            .await
            .unwrap();
        let identity: (String, String, bool, bool, bool, bool, bool, bool) = sqlx::query_as("SELECT current_user::text,session_user::text,rolsuper,rolbypassrls,rolcreatedb,rolcreaterole,rolreplication,rolinherit FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
        assert_eq!(
            identity,
            (
                role.clone(),
                role.clone(),
                false,
                false,
                false,
                false,
                false,
                true
            )
        );
        let effective: bool = sqlx::query_scalar("SELECT pg_has_role(current_user,$1,'USAGE')")
            .bind(base_role)
            .fetch_one(&app)
            .await
            .unwrap();
        assert!(effective);
        let dangerous: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER') AND (r.rolsuper OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR EXISTS(SELECT 1 FROM pg_class c WHERE c.relowner=r.oid AND c.relnamespace='public'::regnamespace))").fetch_one(&app).await.unwrap();
        assert_eq!(dangerous, 0);
        let grant_mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_class c WHERE c.relnamespace='public'::regnamespace AND c.relkind IN ('r','p','v') AND has_table_privilege($1,c.oid,'SELECT') IS DISTINCT FROM has_table_privilege(current_user,c.oid,'SELECT')").bind(base_role).fetch_one(&app).await.unwrap();
        assert_eq!(grant_mismatches, 0);
        let function_mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_proc p WHERE p.pronamespace='public'::regnamespace AND has_function_privilege($1,p.oid,'EXECUTE') IS DISTINCT FROM has_function_privilege(current_user,p.oid,'EXECUTE')").bind(base_role).fetch_one(&app).await.unwrap();
        assert_eq!(function_mismatches, 0);
        let extra_memberships: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_roles r WHERE pg_has_role(current_user,r.oid,'MEMBER') AND r.rolname<>current_user AND r.rolname<>$1").bind(base_role).fetch_one(&app).await.unwrap();
        assert_eq!(extra_memberships, 0);
        let policy_mismatches: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_policy p WHERE p.polrelid IN (SELECT oid FROM pg_class WHERE relnamespace='public'::regnamespace) AND EXISTS(SELECT 1 FROM unnest(p.polroles) r WHERE CASE WHEN r=0 THEN true ELSE pg_has_role(current_user,r,'MEMBER') END) IS DISTINCT FROM EXISTS(SELECT 1 FROM unnest(p.polroles) r WHERE CASE WHEN r=0 THEN true ELSE pg_has_role($1::text,r,'MEMBER') END)").bind(base_role).fetch_one(&app).await.unwrap();
        assert_eq!(policy_mismatches, 0);
        eprintln!("workspace role audit login={role} base={base_role} super=false bypass=false owner_memberships=0 extra_memberships=0 table_grant_mismatches=0 function_grant_mismatches=0 policy_mismatches=0");
        let (account, actor, other_account, other_actor) = if org {
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_workspace_fixture')")
                .execute(&owner)
                .await
                .unwrap();
            let account = Uuid::new_v4();
            (account, Uuid::new_v4(), account, Uuid::new_v4())
        } else {
            let mut identities = Vec::new();
            for _ in 0..2 {
                let actor: Uuid =
                    sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
                        .bind(format!("workspace-{}@test.local", Uuid::new_v4()))
                        .fetch_one(&owner)
                        .await
                        .unwrap();
                let account: Uuid = sqlx::query_scalar(
                    "INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id",
                )
                .bind(actor)
                .fetch_one(&owner)
                .await
                .unwrap();
                sqlx::query(
                    "INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')",
                )
                .bind(account)
                .bind(actor)
                .execute(&owner)
                .await
                .unwrap();
                identities.push((account, actor));
            }
            (
                identities[0].0,
                identities[0].1,
                identities[1].0,
                identities[1].1,
            )
        };
        let mut gs = Vec::new();
        for (acc, actor) in [
            (account, actor),
            (other_account, other_actor),
            (account, actor),
        ] {
            let g: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Workspace synthetic fixture','member',$2) RETURNING id").bind(acc).bind(actor).fetch_one(&owner).await.unwrap();
            gs.push(g);
        }
        let tenant = Tenant {
            store: Some(Arc::new(PostgresStore::new(app.clone()))),
            pool: Some(app.clone()),
            decision: Some(if org {
                TenancyDecision::Org(TenantContext {
                    org_id: "org_workspace_fixture".into(),
                    account_id: account,
                    user_id: actor,
                    pool: app.clone(),
                    page_pool: None,
                })
            } else {
                TenancyDecision::Personal
            }),
            class: coralswarm_backend::route_class::RouteClass::Other,
        };
        Self {
            owner,
            app,
            tenant,
            user: UserContext {
                user_id: actor,
                account_id: account,
                role: "owner".into(),
                device_id: "workspace-fixture".into(),
            },
            graph: gs[0],
            foreign: gs[1],
            empty: gs[2],
            account,
            other_account,
            other_actor,
        }
    }
    /// Serve requests as the migrating owner instead of the app role. The
    /// production tenant DSN connects as the table owner, which bypasses
    /// RLS (`neondb_owner` has BYPASSRLS), so permission parity there rests
    /// on the permission functions alone. Reference rows still come from
    /// the app role (`self.app`), so owner-served pages are compared with
    /// what RLS plus the permission functions allow.
    fn serve_as_owner(mut self) -> Self {
        let owner = self.owner.clone();
        self.tenant.store = Some(Arc::new(PostgresStore::new(owner.clone())));
        self.tenant.pool = Some(owner.clone());
        if let Some(TenancyDecision::Org(ctx)) = &mut self.tenant.decision {
            ctx.pool = owner;
        }
        self
    }
    fn router(&self) -> Router {
        test_support_workspace_app(self.tenant.clone(), self.user.clone())
    }
    async fn get(&self, url: &str) -> (StatusCode, HeaderMap, Value) {
        let started = Instant::now();
        let response = tokio::time::timeout(
            Duration::from_secs(12),
            self.router()
                .oneshot(Request::builder().uri(url).body(Body::empty()).unwrap()),
        )
        .await
        .expect("workspace handler exceeded request budget")
        .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body =
            serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await.unwrap())
                .unwrap();
        eprintln!(
            "workspace HTTP url={url} status={status} elapsed_ms={} has_more={:?} body={body}",
            started.elapsed().as_millis(),
            headers.get("x-has-more")
        );
        (status, headers, body)
    }
    async fn stats(&self, graph: Uuid) -> Value {
        warm_model(self, graph, self.user.user_id).await;
        let (status, _, body) = self.get(&format!("/v1/graphs/{graph}/stats")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(!body["computed_at"].is_null(), "{body}");
        body["payload"].clone()
    }
    async fn page(&self, graph: Uuid, page: i64, size: i64, kind: &str) -> (HeaderMap, Value) {
        let suffix = if kind.is_empty() {
            String::new()
        } else {
            format!("&node_type={kind}")
        };
        let (status, headers, body) = self
            .get(&format!(
                "/v1/nodes?graph_id={graph}&page={page}&page_size={size}{suffix}"
            ))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        (headers, body)
    }
    async fn close(self) {
        self.app.close().await;
        self.owner.close().await;
    }
}
async fn atom(f: &Fixture, source: &str, content: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note',$2,'synthetic-fixture',$3,now()) RETURNING atom_id").bind(f.graph).bind(source).bind(content).fetch_one(&f.owner).await.unwrap()
}
async fn source(f: &Fixture, id: &str, access: &str) -> (Uuid, Uuid) {
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','fixture:workspace',$2,$3) RETURNING source_item_id").bind(f.graph).bind(id).bind(access).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version) VALUES($1,$2,$3,'note','fixture:workspace','Synthetic retained evidence','source-atoms-v1') RETURNING version_id").bind(f.graph).bind(item).bind(id).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    (item, version)
}
async fn map(f: &Fixture, a: Uuid, v: Uuid) {
    sqlx::query(
        "INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(v)
    .bind(a)
    .execute(&f.owner)
    .await
    .unwrap();
}
async fn node(f: &Fixture, id: u128, kind: &str, title: &str, atoms: &[Uuid]) -> Uuid {
    let n = Uuid::from_u128(id);
    sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body,updated_at) VALUES($1,$2,$3,$4,$5,'2026-01-01T00:00:00Z')").bind(n).bind(f.graph).bind(kind).bind(title).bind(if kind=="decision" { json!({"decided_at":"2026-01-01T00:00:00Z"}) } else { json!({}) }).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT $1,unnest($2::uuid[])")
        .bind(n)
        .bind(atoms)
        .execute(&f.owner)
        .await
        .unwrap();
    n
}
async fn semantic(f: &Fixture, n: Uuid, version: Uuid, title: &str, ordinal: i32) {
    let a:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,$3,'fact','Synthetic semantic support','source-atoms-v1','deterministic-fixture',$4) RETURNING semantic_atom_id").bind(f.graph).bind(version).bind(ordinal).bind(json!([{"mention":title}])).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
    )
    .bind(f.graph)
    .bind(n)
    .bind(a)
    .execute(&f.owner)
    .await
    .unwrap();
}
fn ids(v: &Value) -> Vec<Uuid> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|n| Uuid::parse_str(n["node_id"].as_str().unwrap()).unwrap())
        .collect()
}

async fn populated_graph(f: &Fixture, account: Uuid, actor: Uuid, kind: &str) -> (Uuid, Uuid) {
    let owner_user_id = if kind == "member" {
        Some(actor)
    } else {
        None::<Uuid>
    };
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Synthetic shared workspace',$2,$3) RETURNING id").bind(account).bind(kind).bind(owner_user_id).fetch_one(&f.owner).await.unwrap();
    let node = populate_graph(f, graph).await;
    (graph, node)
}
async fn populate_graph(f: &Fixture, graph: Uuid) -> Uuid {
    let atom: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','foreign:chunk:0','synthetic-fixture','Legacy Common',now()) RETURNING atom_id").bind(graph).fetch_one(&f.owner).await.unwrap();
    let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Common','{}') RETURNING node_id").bind(graph).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node)
        .bind(atom)
        .execute(&f.owner)
        .await
        .unwrap();
    node
}
async fn assert_denied(f: &Fixture, graph: Uuid) {
    for url in [
        format!("/v1/graphs/{graph}/stats"),
        format!("/v1/nodes?graph_id={graph}"),
    ] {
        let (status, _, body) = f.get(&url).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body, json!({"error":"graph not found or access denied"}));
    }
}
async fn parity(org: bool, as_owner: bool) {
    let f = Fixture::new(org).await;
    let f = if as_owner { f.serve_as_owner() } else { f };
    populate_graph(&f, f.foreign).await;
    let shared_account = if org { f.account } else { f.other_account };
    if !org {
        assert_ne!(shared_account, f.user.account_id);
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'member')")
            .bind(shared_account)
            .bind(f.user.user_id)
            .execute(&f.owner)
            .await
            .unwrap();
    }
    assert_ne!(f.other_actor, f.user.user_id);
    let (shared, shared_node) =
        populated_graph(&f, shared_account, f.other_actor, "personal").await;
    assert_eq!(
        f.stats(shared).await,
        json!({"reef_total":1,"reef_counts":{"entity":1,"thread":0,"decision":0,"reference":0},"atom_count":1,"source_count":1})
    );
    let (headers, shared_page) = f.page(shared, 1, 20, "").await;
    assert_eq!(ids(&shared_page), vec![shared_node]);
    assert_eq!(shared_page[0]["title"], "Common");
    assert_eq!(shared_page[0]["atom_count"], 1);
    assert_eq!(headers["x-has-more"], "false");
    let (team, _) = populated_graph(&f, f.account, f.user.user_id, "team").await;
    let team_id: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Synthetic team',$3) RETURNING id").bind(f.account).bind(team).bind(f.user.user_id).fetch_one(&f.owner).await.unwrap();
    assert_denied(&f, team).await;
    sqlx::query(
        "INSERT INTO team_members(team_id,user_id,role,departing_at) VALUES($1,$2,'member',now())",
    )
    .bind(team_id)
    .bind(f.user.user_id)
    .execute(&f.owner)
    .await
    .unwrap();
    assert_denied(&f, team).await;
    let (archived, _) = populated_graph(&f, f.account, f.user.user_id, "member").await;
    sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1")
        .bind(archived)
        .execute(&f.owner)
        .await
        .unwrap();
    assert_denied(&f, archived).await;
    let allowed = source(&f, "allowed", "permitted").await;
    let denied = source(&f, "denied", "restricted").await;
    let unknown = source(&f, "unknown", "unknown").await;
    sqlx::query("UPDATE source_versions SET source_metadata=$2 WHERE version_id=$1")
        .bind(allowed.1)
        .bind(json!({"independence":null}))
        .execute(&f.owner)
        .await
        .unwrap();
    let orphan = atom(&f, "orphan:chunk:0", "Synthetic orphan evidence").await;
    assert_ne!(orphan, Uuid::nil());
    let legacy = atom(&f, "legacy:chunk:0", "Legacy Common").await;
    let second = atom(&f, "legacy:chunk:1", "Legacy Common").await;
    let mapped = atom(&f, "mapped", "Mapped Common").await;
    map(&f, mapped, allowed.1).await;
    let mixed = atom(&f, "mixed", "PrivateOnly").await;
    map(&f, mixed, allowed.1).await;
    map(&f, mixed, denied.1).await;
    let hidden = atom(&f, "hidden", "PrivateOnly").await;
    map(&f, hidden, denied.1).await;
    let unknowable = atom(&f, "unknown", "PrivateOnly").await;
    map(&f, unknowable, unknown.1).await;
    for (source_id, column) in [
        ("missing", "source_evidence_bound"),
        ("retracted", "evidence_retracted"),
        ("derived", "derived_copy_bound"),
    ] {
        let private_title = format!("PrivateOnly {source_id}");
        let a = atom(&f, source_id, &private_title).await;
        sqlx::query(&format!("UPDATE atoms SET {column}=true WHERE atom_id=$1"))
            .bind(a)
            .execute(&f.owner)
            .await
            .unwrap();
        node(
            &f,
            100 + if source_id == "missing" {
                0
            } else if source_id == "retracted" {
                1
            } else {
                2
            },
            "entity",
            &private_title,
            &[a],
        )
        .await;
    }
    let common = node(&f, 1, "entity", "Common", &[legacy, mapped, hidden]).await;
    let legacy_node = node(&f, 2, "entity", "Legacy", &[legacy, second]).await;
    let blank = node(&f, 3, "entity", "PrivateStoredTitle", &[legacy]).await;
    sqlx::query("UPDATE nodes SET updated_at='2026-02-01T00:00:00Z' WHERE node_id=$1")
        .bind(blank)
        .execute(&f.owner)
        .await
        .unwrap();
    let semantic_only = node(&f, 4, "thread", "Semantic", &[]).await;
    semantic(&f, semantic_only, allowed.1, "Semantic", 0).await;
    semantic(&f, common, allowed.1, "Common", 1).await;
    semantic(&f, common, allowed.1, "Common", 2).await;
    let mixed_node = node(
        &f,
        5,
        "reference",
        "PrivateOnly",
        &[mixed, hidden, unknowable],
    )
    .await;
    semantic(&f, mixed_node, denied.1, "PrivateOnly", 0).await;
    let reference = node(&f, 6, "reference", "Mapped", &[mapped]).await;
    let decision = node(&f, 7, "decision", "Mapped", &[mapped]).await;
    let later: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version) VALUES($1,$2,'allowed','note','fixture:workspace','Later synthetic snapshot','source-atoms-v1') RETURNING version_id")
        .bind(f.graph).bind(allowed.0).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_versions SET source_metadata=$2 WHERE version_id=$1")
        .bind(later)
        .bind(json!({"independence":"derived_copy"}))
        .execute(&f.owner)
        .await
        .unwrap();
    map(&f, mapped, later).await;
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(allowed.0)
        .bind(later)
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("ANALYZE nodes,atoms,node_atoms,source_items,source_versions,source_version_chunks,semantic_atoms,node_semantic_atoms").execute(&f.owner).await.unwrap();
    let store = PostgresStore::new(f.app.clone());
    for access in ["permitted", "restricted", "unknown"] {
        sqlx::query("UPDATE source_items SET access_state=$2 WHERE source_item_id=$1")
            .bind(allowed.0)
            .bind(access)
            .execute(&f.owner)
            .await
            .unwrap();
        let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
            &f.app,
            f.user.account_id,
            f.user.user_id,
        )
        .await
        .unwrap();
        let scalar:Vec<(Uuid,i64,String)>=sqlx::query_as("SELECT n.node_id,(SELECT count(*) FROM node_atoms l WHERE l.node_id=n.node_id AND library_atom_permitted(n.graph_id,l.atom_id)),COALESCE(library_node_title(n.graph_id,n.node_id),'') FROM nodes n WHERE n.graph_id=$1 AND library_node_permitted(n.graph_id,n.node_id) ORDER BY CASE n.type WHEN 'entity' THEN 0 WHEN 'thread' THEN 1 WHEN 'decision' THEN 2 WHEN 'reference' THEN 3 ELSE 4 END,2 DESC,n.updated_at DESC,n.node_id ASC").bind(f.graph).fetch_all(&mut *tx).await.unwrap();
        let scalar_counts:(i64,i64)=sqlx::query_as("SELECT count(*),count(DISTINCT split_part(source_id,':chunk:',1)) FROM atoms WHERE graph_id=$1 AND library_atom_permitted(graph_id,atom_id)").bind(f.graph).fetch_one(&mut *tx).await.unwrap();
        let direct = store
            .list_nodes_scoped(&mut tx, f.graph, None, 100, 0)
            .await
            .unwrap();
        assert_eq!(
            direct
                .iter()
                .map(|n| (n.node_id, n.atom_count, n.title.clone()))
                .collect::<Vec<_>>(),
            scalar
        );
        tx.commit().await.unwrap();
        let expected = if access == "permitted" {
            vec![
                common,
                legacy_node,
                blank,
                semantic_only,
                decision,
                reference,
            ]
        } else {
            vec![legacy_node, blank, common]
        };
        let stats = f.stats(f.graph).await;
        assert_eq!(
            stats,
            json!({"reef_total":expected.len(),"reef_counts":{"entity":3,"thread":if access=="permitted" {1}else{0},"decision":if access=="permitted" {1}else{0},"reference":if access=="permitted" {1}else{0}},"atom_count":if access=="permitted" {4}else{3},"source_count":if access=="permitted" {3}else{2}})
        );
        assert_eq!(
            (
                stats["atom_count"].as_i64().unwrap(),
                stats["source_count"].as_i64().unwrap()
            ),
            scalar_counts
        );
        let (headers, page) = f.page(f.graph, 1, 2, "").await;
        assert_eq!(ids(&page), expected[..2]);
        assert_eq!(headers["x-has-more"], "true");
        let (_, rest) = f.page(f.graph, 2, 2, "").await;
        assert_eq!(ids(&rest), expected[2..expected.len().min(4)]);
        let (headers, all) = f.page(f.graph, 1, 100, "").await;
        assert_eq!(ids(&all), expected);
        assert_eq!(headers["x-has-more"], "false");
        assert!(all.as_array().unwrap().iter().all(|n| !n["title"]
            .as_str()
            .unwrap()
            .starts_with("PrivateOnly")
            && n["title"] != "PrivateStoredTitle"));
        assert_eq!(
            all.as_array()
                .unwrap()
                .iter()
                .find(|n| n["node_id"] == json!(blank))
                .unwrap()["title"],
            ""
        );
        if access == "permitted" {
            assert_eq!(all[0]["atom_count"], 2);
            assert_eq!(all[3]["atom_count"], 0);
        }
        let (_, filtered) = f.page(f.graph, 1, 100, "entity").await;
        assert_eq!(
            ids(&filtered),
            expected
                .iter()
                .copied()
                .filter(|n| [common, legacy_node, blank].contains(n))
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(
        f.stats(f.empty).await,
        json!({"reef_total":0,"reef_counts":{"entity":0,"thread":0,"decision":0,"reference":0},"atom_count":0,"source_count":0})
    );
    let (headers, empty) = f.page(f.empty, 1, 20, "").await;
    assert_eq!(empty, json!([]));
    assert_eq!(headers["x-has-more"], "false");
    assert_denied(&f, f.foreign).await;
    let mut stranger = f.user.clone();
    stranger.user_id = f.other_actor;
    let mut stranger_tenant = f.tenant.clone();
    if let Some(TenancyDecision::Org(ctx)) = &mut stranger_tenant.decision {
        ctx.user_id = stranger.user_id;
    }
    for url in [
        format!("/v1/graphs/{}/stats", f.graph),
        format!("/v1/nodes?graph_id={}&page=1&page_size=20", f.graph),
    ] {
        let response = tokio::time::timeout(
            Duration::from_secs(12),
            test_support_workspace_app(stranger_tenant.clone(), stranger.clone())
                .oneshot(Request::builder().uri(url).body(Body::empty()).unwrap()),
        )
        .await
        .expect("stranger workspace handler exceeded request budget")
        .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    f.close().await;
}
async fn large(org: bool) {
    let f = Fixture::new(org).await;
    let count = 20_000i64;
    for sql in [
        "INSERT INTO nodes(node_id,graph_id,type,title,body,updated_at) SELECT ('00000000-0000-4000-8000-'||lpad(to_hex(i),12,'0'))::uuid,$1,'entity','Legacy '||i,'{}','2026-01-01T00:00:00Z' FROM generate_series(1,$2) i",
        "INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT ('00000000-0000-4000-9000-'||lpad(to_hex(i),12,'0'))::uuid,$1,'note','legacy:'||i,'synthetic-fixture','Legacy '||i,now() FROM generate_series(1,$2) i",
        "INSERT INTO node_atoms(node_id,atom_id) SELECT ('00000000-0000-4000-8000-'||lpad(to_hex(i),12,'0'))::uuid,('00000000-0000-4000-9000-'||lpad(to_hex(i),12,'0'))::uuid FROM generate_series(1,$2) i",
    ] { sqlx::query(sql).bind(f.graph).bind(count).execute(&f.owner).await.unwrap(); }
    let captured = 400i64;
    for sql in [
        "INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,access_state) SELECT ('00000000-0000-4000-a000-'||lpad(to_hex(i),12,'0'))::uuid,$1,'note','fixture:scale','captured:'||i,CASE WHEN i<=300 THEN 'permitted' WHEN i<=350 THEN 'restricted' ELSE 'unknown' END FROM generate_series(1,$2) i",
        "INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version,source_metadata) SELECT ('00000000-0000-4000-b000-'||lpad(to_hex((i-1)*3+v),12,'0'))::uuid,$1,('00000000-0000-4000-a000-'||lpad(to_hex(i),12,'0'))::uuid,'captured:'||i,'note','fixture:scale','Synthetic captured snapshot '||i||' version '||v,'source-atoms-v1',CASE v WHEN 1 THEN 'null'::jsonb WHEN 2 THEN jsonb_build_object('independence','derived_copy') ELSE '{}'::jsonb END FROM generate_series(1,$2) i CROSS JOIN generate_series(1,3) v",
        "UPDATE source_items s SET current_version_id=('00000000-0000-4000-b000-'||lpad(to_hex((i-1)*3+3),12,'0'))::uuid FROM generate_series(1,$2) i WHERE s.graph_id=$1 AND s.provider_namespace='fixture:scale' AND s.source_id='captured:'||i",
        "INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) SELECT $1,('00000000-0000-4000-b000-'||lpad(to_hex((i-1)*3+v),12,'0'))::uuid,('00000000-0000-4000-9000-'||lpad(to_hex(i),12,'0'))::uuid FROM generate_series(1,$2) i CROSS JOIN generate_series(1,3) v",
        "INSERT INTO nodes(node_id,graph_id,type,title,body,updated_at) SELECT ('00000000-0000-4000-8000-'||lpad(to_hex(20000+i),12,'0'))::uuid,$1,'thread','Captured '||i,'{}','2026-01-01T00:00:00Z' FROM generate_series(1,$2) i",
        "INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT ('00000000-0000-4000-c000-'||lpad(to_hex((i-1)*3+v),12,'0'))::uuid,$1,('00000000-0000-4000-b000-'||lpad(to_hex((i-1)*3+v),12,'0'))::uuid,0,'fact','Synthetic scale semantic support','source-atoms-v1','deterministic-fixture',jsonb_build_array(jsonb_build_object('mention','Captured '||i),jsonb_build_object('mention','Legacy '||i)) FROM generate_series(1,$2) i CROSS JOIN generate_series(1,3) v",
        "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,('00000000-0000-4000-8000-'||lpad(to_hex(i+off),12,'0'))::uuid,('00000000-0000-4000-c000-'||lpad(to_hex((i-1)*3+v),12,'0'))::uuid FROM generate_series(1,$2) i CROSS JOIN generate_series(1,3) v CROSS JOIN (VALUES(0),(20000)) offsets(off)",
    ] { sqlx::query(sql).bind(f.graph).bind(captured).execute(&f.owner).await.unwrap(); }
    sqlx::query("ANALYZE nodes,atoms,node_atoms,source_items,source_versions,source_version_chunks,semantic_atoms,node_semantic_atoms").execute(&f.owner).await.unwrap();
    let mut observed = Vec::new();
    for (page_number, first, last, title, atom_count, has_more) in [
        (1, 1, 20, "Legacy", 1, "true"),
        (2, 21, 40, "Legacy", 1, "true"),
        (3, 41, 60, "Legacy", 1, "true"),
        (995, 19981, 20000, "Legacy", 1, "true"),
        (996, 20001, 20020, "Captured", 0, "true"),
        (1010, 20281, 20300, "Captured", 0, "false"),
    ] {
        let (headers, page) = f.page(f.graph, page_number, 20, "").await;
        let expected: Vec<Uuid> = (first..=last)
            .map(|i| Uuid::parse_str(&format!("00000000-0000-4000-8000-{i:012x}")).unwrap())
            .collect();
        assert_eq!(ids(&page), expected);
        assert_eq!(headers["x-has-more"], has_more);
        for (i, n) in (first..=last).zip(page.as_array().unwrap()) {
            assert_eq!(
                n["title"],
                format!("{title} {}", if atom_count == 0 { i - 20000 } else { i })
            );
            assert_eq!(n["atom_count"], atom_count);
        }
        observed.extend(expected);
    }
    let mut unique = observed.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), observed.len());
    let (headers, filtered) = f.page(f.graph, 2, 20, "thread").await;
    assert_eq!(
        ids(&filtered),
        (20021..=20040)
            .map(|i| Uuid::parse_str(&format!("00000000-0000-4000-8000-{i:012x}")).unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(headers["x-has-more"], "true");
    assert_eq!(
        f.stats(f.graph).await,
        json!({"reef_total":20200,"reef_counts":{"entity":19900,"thread":300,"decision":0,"reference":0},"atom_count":19900,"source_count":19900})
    );
    f.close().await;
}
/// Canonical rows, from the scalar predicates `library_node_permitted`,
/// `library_atom_permitted` and `library_node_title`, in canonical order.
async fn canonical_rows(f: &Fixture, graph: Uuid) -> Vec<(Uuid, i64, String)> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
        &f.app,
        f.user.account_id,
        f.user.user_id,
    )
    .await
    .unwrap();
    let rows: Vec<(Uuid, i64, String)> = sqlx::query_as("SELECT n.node_id,(SELECT count(*) FROM node_atoms l WHERE l.node_id=n.node_id AND library_atom_permitted(n.graph_id,l.atom_id)),COALESCE(library_node_title(n.graph_id,n.node_id),'') FROM nodes n WHERE n.graph_id=$1 AND library_node_permitted(n.graph_id,n.node_id) ORDER BY CASE n.type WHEN 'entity' THEN 0 WHEN 'thread' THEN 1 WHEN 'decision' THEN 2 WHEN 'reference' THEN 3 ELSE 4 END,2 DESC,n.updated_at DESC,n.node_id ASC").bind(graph).fetch_all(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    rows
}
fn rows_of(v: &Value) -> Vec<(Uuid, i64, String)> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|n| {
            (
                Uuid::parse_str(n["node_id"].as_str().unwrap()).unwrap(),
                n["atom_count"].as_i64().unwrap(),
                n["title"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}
/// Every page of one size, concatenated, checking `x-has-more` on the way.
async fn drain(f: &Fixture, graph: Uuid, size: i64, kind: &str) -> Vec<(Uuid, i64, String)> {
    let mut all = Vec::new();
    for page in 1.. {
        let (headers, body) = f.page(graph, page, size, kind).await;
        let rows = rows_of(&body);
        assert!(rows.len() as i64 <= size);
        all.extend(rows);
        if headers["x-has-more"] == "false" {
            break;
        }
        assert_eq!(body.as_array().unwrap().len() as i64, size);
    }
    all
}
/// COR-113: reefs are visited in bound order (`coral_count` minus withheld
/// links), so a reef with many links that are withheld, or that point into
/// another graph, must still land where its permitted count puts it, and a
/// reef whose links are all withheld must never appear. The graph is larger
/// than one candidate window, so small pages take the windowed path.
async fn withheld_hubs(org: bool, as_owner: bool) {
    let f = Fixture::new(org).await;
    let f = if as_owner { f.serve_as_owner() } else { f };
    let allowed = source(&f, "hub-allowed", "permitted").await;
    let denied = source(&f, "hub-denied", "restricted").await;
    let mut permitted = Vec::new();
    let mut withheld = Vec::new();
    for i in 0..40 {
        let a = atom(&f, &format!("hub-allowed:chunk:{i}"), &format!("Topic {i} Hub {i} Cross {i}")).await;
        map(&f, a, allowed.1).await;
        permitted.push(a);
        let d = atom(&f, &format!("hub-denied:chunk:{i}"), &format!("PrivateOnly Topic {i} Hub {i}")).await;
        map(&f, d, denied.1).await;
        withheld.push(d);
    }
    let foreign: Vec<Uuid> = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT $1,'note','foreign-hub:chunk:'||i,'synthetic-fixture','PrivateOnly foreign '||i,now() FROM generate_series(1,40) i RETURNING atom_id").bind(f.foreign).fetch_all(&f.owner).await.unwrap();
    let mut hubs = Vec::new();
    for k in 1..=5usize {
        // 40 withheld links and k permitted ones: bound and exact are both k.
        let mut links = withheld.clone();
        links.extend_from_slice(&permitted[..k]);
        hubs.push(node(&f, 1_000 + k as u128, "entity", &format!("Hub {k}"), &links).await);
    }
    for c in 0..80usize {
        // 40 links into another graph and one permitted: bound 41, exact 1.
        // They fill the first window of a small page, so only a bigger
        // window can find the reefs that really come first.
        let mut links = foreign.clone();
        links.push(permitted[c % 40]);
        hubs.push(node(&f, 1_100 + c as u128, "entity", &format!("Cross {c}"), &links).await);
    }
    let mut topics = Vec::new();
    for i in 0..120u128 {
        let j = (i % 7) as usize + 1;
        let links: Vec<Uuid> = permitted.iter().cycle().skip(i as usize).take(j).copied().collect();
        topics.push(node(&f, 2_000 + i, "entity", &format!("Topic {i}"), &links).await);
    }
    let all_withheld = node(&f, 3_000, "entity", "PrivateOnly Topic 1", &withheld[..30]).await;
    let all_foreign = node(&f, 3_001, "entity", "PrivateOnly foreign 1", &foreign[..30]).await;
    let semantic_only = node(&f, 4_000, "thread", "Semantic hub", &[]).await;
    semantic(&f, semantic_only, allowed.1, "Semantic hub", 0).await;
    node(&f, 5_000, "decision", "Topic 3", &permitted[3..5]).await;
    node(&f, 5_001, "reference", "Topic 4", &[withheld[4], permitted[4]]).await;
    sqlx::query("ANALYZE nodes,atoms,node_atoms,source_items,source_versions,source_version_chunks,semantic_atoms,node_semantic_atoms").execute(&f.owner).await.unwrap();
    let canonical = canonical_rows(&f, f.graph).await;
    assert_eq!(canonical.len(), 208, "85 hubs, 120 topics, semantic, decision, reference");
    assert!(!canonical.iter().any(|r| r.0 == all_withheld || r.0 == all_foreign));
    assert!(canonical.iter().all(|r| !r.2.starts_with("PrivateOnly")));
    // Hubs sort by their permitted count, not their link count.
    let hub_positions: Vec<usize> = hubs
        .iter()
        .map(|h| canonical.iter().position(|r| r.0 == *h).unwrap())
        .collect();
    assert!(hub_positions.iter().all(|p| *p > 30), "{hub_positions:?}");
    for size in [1, 2, 7, 1000] {
        assert_eq!(drain(&f, f.graph, size, "").await, canonical, "page size {size}");
    }
    let entities: Vec<_> = canonical.iter().take(205).cloned().collect();
    assert_eq!(drain(&f, f.graph, 4, "entity").await, entities);
    let expected_stats = json!({"reef_total":208,"reef_counts":{"entity":205,"thread":1,"decision":1,"reference":1},"atom_count":40,"source_count":1});
    assert_eq!(f.stats(f.graph).await, expected_stats);
    // A drifted `coral_count` never admits a reef, changes a count or
    // exposes a title. It can misplace reefs across small pages until a
    // window visits the drifted reef; a window that does falls back to the
    // exact full-admission statement, as the whole-graph page shows.
    // Two topics with seven permitted links each now claim no links at all.
    let drifted: Vec<Uuid> = topics.iter().enumerate().filter(|(i, _)| i % 7 == 6).map(|(_, t)| *t).take(2).collect();
    sqlx::query("UPDATE nodes SET coral_count=CASE WHEN node_id=$2 THEN 10000 ELSE 0 END WHERE node_id=ANY($1) OR node_id=$2")
        .bind(&drifted)
        .bind(all_withheld)
        .execute(&f.owner)
        .await
        .unwrap();
    for size in [1, 3] {
        for row in drain(&f, f.graph, size, "").await {
            assert!(canonical.contains(&row), "page size {size} after drift returned {row:?}");
        }
    }
    assert_eq!(drain(&f, f.graph, 1000, "").await, canonical, "whole graph after drift");
    assert_eq!(f.stats(f.graph).await, expected_stats);
    f.close().await;
}
/// COR-113 acceptance: 20,000 reefs and 600,000 links, 4% withheld. The first
/// pages must not cost what a deep page costs, and every page checked must be
/// the canonical rows at that offset.
async fn hub_corpus(org: bool, as_owner: bool) {
    let f = Fixture::new(org).await;
    let f = if as_owner { f.serve_as_owner() } else { f };
    let (nodes, atoms) = (20_000i64, 2_000i64);
    for sql in [
        "INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,access_state) VALUES ('00000000-0000-4000-a000-000000000001',$1,'note','fixture:hub','hub-allowed','permitted'),('00000000-0000-4000-a000-000000000002',$1,'note','fixture:hub','hub-denied','restricted')",
        "INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version) SELECT ('00000000-0000-4000-b000-00000000000'||i)::uuid,$1,('00000000-0000-4000-a000-00000000000'||i)::uuid,CASE i WHEN 1 THEN 'hub-allowed' ELSE 'hub-denied' END,'note','fixture:hub','Synthetic hub snapshot','source-atoms-v1' FROM generate_series(1,2) i",
        "UPDATE source_items SET current_version_id=('00000000-0000-4000-b000-'||right(source_item_id::text,12))::uuid WHERE graph_id=$1",
    ] { sqlx::query(sql).bind(f.graph).execute(&f.owner).await.unwrap(); }
    sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT ('00000000-0000-4000-9000-'||lpad(to_hex(i),12,'0'))::uuid,$1,'note',CASE WHEN i%25=0 THEN 'hub-denied' ELSE 'hub-allowed' END||':chunk:'||i,'synthetic-fixture','Hub '||i,now() FROM generate_series(1,$2) i").bind(f.graph).bind(atoms).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) SELECT $1,CASE WHEN i%25=0 THEN '00000000-0000-4000-b000-000000000002'::uuid ELSE '00000000-0000-4000-b000-000000000001'::uuid END,('00000000-0000-4000-9000-'||lpad(to_hex(i),12,'0'))::uuid FROM generate_series(1,$2) i").bind(f.graph).bind(atoms).execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO nodes(node_id,graph_id,type,title,body,updated_at) SELECT ('00000000-0000-4000-8000-'||lpad(to_hex(i),12,'0'))::uuid,$1,'entity','Hub '||i,'{}','2026-01-01T00:00:00Z' FROM generate_series(1,$2) i").bind(f.graph).bind(nodes).execute(&f.owner).await.unwrap();
    // Bulk links without the per-row triggers, then one exact counter recount.
    let mut owner = f.owner.begin().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout='120s'").execute(&mut *owner).await.unwrap();
    sqlx::query("ALTER TABLE node_atoms DISABLE TRIGGER USER").execute(&mut *owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT DISTINCT ('00000000-0000-4000-8000-'||lpad(to_hex(i),12,'0'))::uuid,('00000000-0000-4000-9000-'||lpad(to_hex(1+(i*131+k*17)%$2),12,'0'))::uuid FROM generate_series(1,$1) i CROSS JOIN LATERAL generate_series(1,1+(i*37)%60) k").bind(nodes).bind(atoms).execute(&mut *owner).await.unwrap();
    sqlx::query("ALTER TABLE node_atoms ENABLE TRIGGER USER").execute(&mut *owner).await.unwrap();
    sqlx::query("UPDATE nodes n SET coral_count=c.k FROM (SELECT node_id,count(*) k FROM node_atoms GROUP BY node_id) c WHERE c.node_id=n.node_id AND n.graph_id=$1").bind(f.graph).execute(&mut *owner).await.unwrap();
    owner.commit().await.unwrap();
    let links: i64 = sqlx::query_scalar("SELECT count(*) FROM node_atoms").fetch_one(&f.owner).await.unwrap();
    assert!(links > 550_000, "{links}");
    sqlx::query("VACUUM ANALYZE nodes").execute(&f.owner).await.unwrap();
    sqlx::query("VACUUM ANALYZE node_atoms").execute(&f.owner).await.unwrap();
    sqlx::query("ANALYZE atoms,source_items,source_versions,source_version_chunks").execute(&f.owner).await.unwrap();
    // Reference order from the canonical set selector, independent of the
    // queries under test.
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app, f.user.account_id, f.user.user_id).await.unwrap();
    sqlx::query("SET LOCAL statement_timeout='60s'").execute(&mut *tx).await.unwrap();
    let reference: Vec<(Uuid, i64)> = sqlx::query_as("WITH p AS MATERIALIZED (SELECT atom_id FROM library_permitted_atoms($1,NULL::uuid[])), c AS (SELECT l.node_id,count(*) k FROM node_atoms l JOIN nodes n ON n.node_id=l.node_id WHERE n.graph_id=$1 AND l.atom_id IN (SELECT atom_id FROM p) GROUP BY l.node_id) SELECT n.node_id,c.k FROM nodes n JOIN c ON c.node_id=n.node_id WHERE n.graph_id=$1 ORDER BY c.k DESC,n.updated_at DESC,n.node_id").bind(f.graph).fetch_all(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert!(reference.len() > 19_000, "{}", reference.len());
    let untainted: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id=$1 AND coral_count>0 AND node_id NOT IN (SELECT l.node_id FROM node_atoms l WHERE l.atom_id IN (SELECT atom_id FROM atoms WHERE source_id LIKE 'hub-denied%') )").bind(f.graph).fetch_one(&f.owner).await.unwrap();
    assert!(untainted > 0 && untainted < nodes, "some reefs, not all, must carry withheld links: {untainted}");
    let mut timings = Vec::new();
    for (page, size) in [(1i64, 20i64), (2, 20), (50, 20), (1, 1000), (400, 20)] {
        let mut best = u128::MAX;
        for _ in 0..3 {
            let started = Instant::now();
            let (_, body) = f.page(f.graph, page, size, "").await;
            best = best.min(started.elapsed().as_millis());
            let got: Vec<(Uuid, i64)> = rows_of(&body).into_iter().map(|r| (r.0, r.1)).collect();
            let from = ((page - 1) * size) as usize;
            assert_eq!(got, reference[from..from + size as usize], "page {page} size {size}");
            for row in body.as_array().unwrap() {
                let title = row["title"].as_str().unwrap();
                assert!(title.is_empty() || title.starts_with("Hub "), "{title}");
            }
        }
        timings.push(((page, size), best));
    }
    let started = Instant::now();
    let stats = f.stats(f.graph).await;
    let stats_ms = started.elapsed().as_millis();
    assert_eq!(stats["reef_total"], json!(reference.len()));
    eprintln!("workspace hub corpus org={org} owner={as_owner} links={links} best_page_ms={timings:?} stats_ms={stats_ms}");
    if as_owner {
        // Production tenant reads run as the owner. There the first pages must
        // not read the link table: a full-admission statement reads every
        // link (the deep page below is the control for the measurement).
        let store = PostgresStore::new(f.owner.clone());
        for (offset, size, bounded) in [(0i64, 21i64, true), (20, 21, true), (7_980, 21, false)] {
            // Rows read from the link table by sequential scans plus
            // entries read from any of its indexes (index-only scans never
            // touch the heap, so heap counters alone undercount). Backend
            // counters can still hold unflushed work from earlier
            // transactions on this connection, so measure a delta.
            const READS: &str = "SELECT (pg_stat_get_xact_tuples_returned('node_atoms'::regclass)+COALESCE((SELECT sum(pg_stat_get_xact_tuples_returned(i.indexrelid)) FROM pg_index i WHERE i.indrelid='node_atoms'::regclass),0))::bigint";
            let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.owner, f.user.account_id, f.user.user_id).await.unwrap();
            let before: i64 = sqlx::query_scalar(READS).fetch_one(&mut *tx).await.unwrap();
            let rows = store.list_nodes_scoped(&mut tx, f.graph, None, size, offset).await.unwrap();
            let after: i64 = sqlx::query_scalar(READS).fetch_one(&mut *tx).await.unwrap();
            let read = after - before;
            tx.rollback().await.unwrap();
            let got: Vec<(Uuid, i64)> = rows.iter().map(|n| (n.node_id, n.atom_count)).collect();
            assert_eq!(got, reference[offset as usize..(offset + size) as usize], "offset {offset}");
            eprintln!("workspace hub corpus owner offset={offset} node_atoms_tuples_read={read} links={links}");
            if bounded {
                assert!(read < links / 10, "offset {offset} read {read} of {links} links");
            } else {
                assert!(read >= links / 2, "control: full admission read only {read} of {links} links");
            }
        }
    }
    assert!(stats_ms < 8_000, "{stats_ms}");
    f.close().await;
}
/// A reef whose only semantic support is an atom its version's finished
/// re-extraction replaced (kept in `semantic_atoms` for a citer) is not in
/// the workspace. While the re-extraction is still pending, the earlier atom
/// is all the version has, so its reef stays.
async fn superseded_atoms_are_not_served(org: bool) {
    let f = Fixture::new(org).await;
    let version = |id: &'static str, status: &'static str| {
        let f = &f;
        async move {
            let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','fixture:workspace',$2,'permitted') RETURNING source_item_id")
                .bind(f.graph).bind(id).fetch_one(&f.owner).await.unwrap();
            let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version,extraction_status) VALUES($1,$2,$3,'note','fixture:workspace','Synthetic retained evidence','source-atoms-v3',$4) RETURNING version_id")
                .bind(f.graph).bind(item).bind(id).bind(status).fetch_one(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&f.owner).await.unwrap();
            version
        }
    };
    let reextracted = version("reextracted", "done").await;
    let reextracting = version("reextracting", "pending").await;
    let stale = node(&f, 10, "thread", "Stale", &[]).await;
    semantic(&f, stale, reextracted, "Stale", 0).await;
    let waiting = node(&f, 11, "thread", "Waiting", &[]).await;
    semantic(&f, waiting, reextracting, "Waiting", 0).await;
    let (_, page) = f.page(f.graph, 1, 20, "").await;
    assert_eq!(ids(&page), vec![waiting], "the replaced v1 atom supports nothing; the pending one still does");
    assert_eq!(f.stats(f.graph).await["reef_total"], 1);
    f.close().await;
}
/// Ground truth for one reader, per row in that reader's own scope and
/// independent of `graph_workspace_counts_compute` and of anything stored:
/// a reef counts when one of its legacy links passes the scalar
/// `library_atom_permitted`, or one of its semantic links is served (the
/// workspace admits only served semantic atoms; see
/// `superseded_atoms_are_not_served`).
async fn truth(f: &Fixture, graph: Uuid, user: Uuid) -> Value {
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app, f.user.account_id, user).await.unwrap();
    let by_type: Vec<(String, i64)> = sqlx::query_as("SELECT n.type,count(*) FROM nodes n WHERE n.graph_id=$1 AND (EXISTS(SELECT 1 FROM node_atoms l WHERE l.node_id=n.node_id AND library_atom_permitted(n.graph_id,l.atom_id)) OR EXISTS(SELECT 1 FROM node_semantic_atoms l JOIN served_semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id WHERE l.graph_id=n.graph_id AND l.node_id=n.node_id)) GROUP BY n.type").bind(graph).fetch_all(&mut *tx).await.unwrap();
    let (atoms, sources): (i64, i64) = sqlx::query_as("SELECT count(*),count(DISTINCT split_part(source_id,':chunk:',1)) FROM atoms WHERE graph_id=$1 AND library_atom_permitted(graph_id,atom_id)").bind(graph).fetch_one(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let of = |k: &str| by_type.iter().find(|r| r.0 == k).map(|r| r.1).unwrap_or(0);
    json!({"reef_total":by_type.iter().map(|r| r.1).sum::<i64>(),"reef_counts":{"entity":of("entity"),"thread":of("thread"),"decision":of("decision"),"reference":of("reference")},"atom_count":atoms,"source_count":sources})
}
/// Equality fixtures explicitly compute on the worker before asserting the
/// payload. Missing/stale/revoke/claim/handler behavior lives in read_models_pg
/// and read-model-lifecycle.spec.ts; requests never seed their own answers.
async fn warm_model(f:&Fixture,graph:Uuid,user:Uuid){
    use coralswarm_backend::{read_models::{self as rm,oceans::{WorkspaceCounts as Model,WorkspaceCountsParams}},workers::{tenant_harness::WorkerTenant,read_model_refresh},tenancy::{scope::begin_tenant_scoped,TenantId}};
    let mut tx=begin_tenant_scoped(&f.app,f.account,user).await.unwrap();
    let account:Option<Uuid>=sqlx::query_scalar("SELECT read_model_graph_account($1)").bind(graph).fetch_one(&mut *tx).await.unwrap();
    let account=account.expect("reachable equality fixture graph");
    sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(account.to_string()).execute(&mut *tx).await.unwrap();
    let viewer=rm::Viewer::load(&mut tx,account,user).await.unwrap();
    let scope=viewer.scope::<Model>(WorkspaceCountsParams{graph}).unwrap();tx.rollback().await.unwrap();
    rm::store::request_refresh::<Model>(&f.app,&scope).await.unwrap();
    let worker=WorkerTenant{tenant:TenantId::Consumer,pool:f.app.clone(),store:Arc::new(PostgresStore::new(f.app.clone()))};
    // An asynchronous GET enqueue can commit after a tick's transaction-time
    // deadline. Drive the real worker to quiescence, outside the handler.
    for _ in 0..5 {
        read_model_refresh::tick(&worker,&f.app).await.unwrap();
        let mut tx=begin_tenant_scoped(&f.app,account,user).await.unwrap();
        let answer=rm::store::read::<Model>(&mut tx,&scope).await.unwrap();tx.rollback().await.unwrap();
        if answer.computed_at.is_some() && !answer.stale{return;}
    }
    panic!("worker did not finish the explicitly warmed equality fixture");
}
async fn stored_counts(org:bool,as_owner:bool){
    let f=Fixture::new(org).await;let f=if as_owner{f.serve_as_owner()}else{f};
    let source=source(&f,"stored-allowed","permitted").await;
    let a=atom(&f,"stored-allowed:chunk:0","Stored chunk").await;map(&f,a,source.1).await;
    node(&f,1,"entity","First",&[a]).await;
    let semantic_node=node(&f,2,"reference","Semantic",&[]).await;
    semantic(&f,semantic_node,source.1,"Semantic",0).await;
    assert_eq!(f.stats(f.graph).await,truth(&f,f.graph,f.user.user_id).await);
    let added=atom(&f,"stored-added:chunk:0","Added").await;node(&f,3,"decision","Added",&[added]).await;
    assert_eq!(f.stats(f.graph).await,truth(&f,f.graph,f.user.user_id).await);
    sqlx::query("UPDATE source_versions SET extraction_version='source-atoms-v3',extraction_status='done' WHERE version_id=$1").bind(source.1).execute(&f.owner).await.unwrap();
    assert_eq!(f.stats(f.graph).await,truth(&f,f.graph,f.user.user_id).await);
    sqlx::query("DELETE FROM atoms WHERE atom_id=$1").bind(added).execute(&f.owner).await.unwrap();
    let before=f.stats(f.graph).await;assert_eq!(before,truth(&f,f.graph,f.user.user_id).await);
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(source.0).execute(&f.owner).await.unwrap();
    // The reducing write serves the last answer stale, never current, until recompute.
    let (_,_,behind)=f.get(&format!("/v1/graphs/{}/stats",f.graph)).await;
    assert!(!behind["computed_at"].is_null());assert_eq!(behind["stale"],true);assert_eq!(behind["payload"],before);
    assert_eq!(f.stats(f.graph).await,truth(&f,f.graph,f.user.user_id).await);
    f.close().await;
}
#[tokio::test]
async fn personal_workspace_stored_counts_match_ground_truth() {
    stored_counts(false, false).await;
}
#[tokio::test]
async fn org_workspace_stored_counts_match_ground_truth() {
    stored_counts(true, false).await;
}
#[tokio::test]
async fn org_workspace_owner_role_stored_counts_match_ground_truth() {
    stored_counts(true, true).await;
}
#[tokio::test]
async fn personal_workspace_serves_only_current_extraction_atoms() {
    superseded_atoms_are_not_served(false).await;
}
#[tokio::test]
async fn org_workspace_serves_only_current_extraction_atoms() {
    superseded_atoms_are_not_served(true).await;
}
#[tokio::test]
async fn personal_workspace_permission_and_pagination_parity() {
    parity(false, false).await;
}
#[tokio::test]
async fn org_workspace_permission_and_pagination_parity() {
    parity(true, false).await;
}
#[tokio::test]
async fn personal_workspace_twenty_thousand_nodes_within_request_budget() {
    large(false).await;
}
#[tokio::test]
async fn org_workspace_twenty_thousand_nodes_within_request_budget() {
    large(true).await;
}
#[tokio::test]
async fn org_workspace_owner_role_permission_and_pagination_parity() {
    parity(true, true).await;
}
#[tokio::test]
async fn personal_workspace_withheld_hubs_keep_canonical_pages() {
    withheld_hubs(false, false).await;
}
#[tokio::test]
async fn org_workspace_withheld_hubs_keep_canonical_pages() {
    withheld_hubs(true, false).await;
}
#[tokio::test]
async fn org_workspace_owner_role_withheld_hubs_keep_canonical_pages() {
    withheld_hubs(true, true).await;
}
#[tokio::test]
async fn personal_workspace_hub_corpus_first_pages_are_bounded() {
    hub_corpus(false, false).await;
}
#[tokio::test]
async fn org_workspace_owner_role_hub_corpus_first_pages_are_bounded() {
    hub_corpus(true, true).await;
}
