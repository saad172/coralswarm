//! Actual source/node/edge reads and scope validation on both low-privilege
//! PostgreSQL planes. Every prerequisite is asserted; there are no model calls.
mod support;
use axum::{body::to_bytes, http::StatusCode, response::Response, Extension};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use coralswarm_backend::{
    library_disagreements::{self, DisagreementRequest},
    library_read::{
        self, LibraryJson as Json, LibraryScope, NeighborhoodRequest, ScopeRequest, SearchRequest,
    },
    source_evidence,
    store::PostgresStore,
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
    },
    UserContext,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection, PgPool,
};
use std::{str::FromStr, sync::Arc};
use support::rls_probe::RlsProbe;
use uuid::Uuid;

enum Cleanup {
    Consumer(RlsProbe),
    Tenant(PgConnection, coralswarm_backend::test_db::TestDatabase),
}
struct Fixture {
    owner: PgPool,
    app: PgPool,
    tenant: Tenant,
    user: UserContext,
    graph: Uuid,
    foreign_graph: Uuid,
    cleanup: Cleanup,
}
impl Fixture {
    async fn new(tenant_plane: bool, suffix: &str) -> Self {
        let base = std::env::var("ORGT_TEST_BASE_URL")
            .expect("ORGT_TEST_BASE_URL required for actual Library proof");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        if !tenant_plane {
            let f = RlsProbe::setup(&format!("library_consumer_{suffix}"))
                .await
                .unwrap();
            for account in [&f.a, &f.b] {
                sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
                    .bind(account.account_id).bind(account.user_id).execute(&f.owner).await.unwrap();
            }
            return Self {
                owner: f.owner.clone(),
                app: f.app_pool.clone(),
                tenant: Tenant {
                    store: Some(Arc::new(PostgresStore::new(f.app_pool.clone()))),
                    pool: Some(f.app_pool.clone()),
                    decision: None,
                    class: coralswarm_backend::route_class::RouteClass::Other,
                },
                user: UserContext {
                    user_id: f.a.user_id,
                    account_id: f.a.account_id,
                    role: "owner".into(),
                    device_id: "library-proof".into(),
                },
                graph: f.a.graph_id,
                foreign_graph: f.b.graph_id,
                cleanup: Cleanup::Consumer(f),
            };
        }
        let mut admin = PgConnection::connect_with(&opts).await.unwrap();
        let name = coralswarm_backend::test_db::migrated_database(
            &opts,
            coralswarm_backend::test_db::Plane::Tenant,
            &format!("library_tenant_{suffix}"),
        )
        .await;
        let owner = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts.clone().database(&name))
            .await
            .unwrap();
        // This constant is a disposable local fixture login, never a real secret.
        sqlx::query("ALTER ROLE coralswarm_tenant_app PASSWORD 'library-local-fixture'")
            .execute(&owner)
            .await
            .unwrap();
        let app = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                opts.database(&name)
                    .username("coralswarm_tenant_app")
                    .password("library-local-fixture"),
            )
            .await
            .unwrap();
        let actor = Uuid::new_v4();
        let other = Uuid::new_v4();
        let account = Uuid::new_v4();
        let graph = Uuid::new_v4();
        let foreign_graph = Uuid::new_v4();
        // Blob locators require a valid canonical org segment (`org_…`).
        let fixture_org = if suffix == "capture_ingest" || suffix == "blob_fence" {
            "org_library_proof"
        } else {
            "org-library-proof"
        };
        sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1)")
            .bind(fixture_org)
            .execute(&owner)
            .await
            .unwrap();
        for (g, u) in [(graph, actor), (foreign_graph, other)] {
            sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES($1,$2,'Library fixture','member',$3)").bind(g).bind(account).bind(u).execute(&owner).await.unwrap();
        }
        let tenant = Tenant {
            store: Some(Arc::new(PostgresStore::new(app.clone()))),
            pool: Some(app.clone()),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: fixture_org.into(),
                account_id: account,
                user_id: actor,
                pool: app.clone(),
                page_pool: None,
            })),
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
                device_id: "library-proof".into(),
            },
            graph,
            foreign_graph,
            cleanup: Cleanup::Tenant(admin, name),
        }
    }
    async fn close(self) {
        self.app.close().await;
        self.owner.close().await;
        match self.cleanup {
            Cleanup::Consumer(f) => f.teardown().await,
            Cleanup::Tenant(mut admin, name) => {
                admin
                    .execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
                    .await
                    .unwrap();
            }
        }
    }
}
async fn body(response: Response) -> (StatusCode, Value) {
    assert_eq!(response.headers()["cache-control"], "no-store");
    let status = response.status();
    (
        status,
        serde_json::from_slice(&to_bytes(response.into_body(), 4_000_000).await.unwrap()).unwrap(),
    )
}
fn scope(g: Uuid, extra: Value) -> LibraryScope {
    let mut v = json!({"graph_ids":[g]});
    v.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    serde_json::from_value(v).unwrap()
}
/// What `resolve_scope` answers for a valid scope: the normalized scope
/// alone. No count of sources, outcomes or nodes rides along.
fn resolved_scope(s: &LibraryScope) -> Value {
    let mut normalized = s.clone();
    normalized.normalize().unwrap();
    json!({"scope": normalized})
}
async fn search(
    f: &Fixture,
    s: LibraryScope,
    query: &str,
    limit: i64,
    source_cursor: Option<String>,
) -> (StatusCode, Value) {
    body(
        library_read::search(
            f.tenant.clone(),
            Extension(f.user.clone()),
            Json(SearchRequest {
                scope: s,
                query: query.into(),
                page_size: Some(limit),
                sources_after: source_cursor,
                outcomes_after: None,
                nodes_after: None,
            }),
        )
        .await,
    )
    .await
}
async fn node(f: &Fixture, g: Uuid, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity',$2,'{}') RETURNING node_id",
    )
    .bind(g)
    .bind(title)
    .fetch_one(&f.owner)
    .await
    .unwrap()
}
struct Source {
    item: Uuid,
    version: Uuid,
    atom: Uuid,
    semantic: Uuid,
}
async fn source(
    f: &Fixture,
    g: Uuid,
    namespace: &str,
    origin: Value,
    access: &str,
    title: &str,
    text: &str,
    nodes: &[Uuid],
    mention: &str,
) -> Source {
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack',$2,'same-provider-local-id',$3,$4) RETURNING source_item_id").bind(g).bind(namespace).bind(&origin).bind(access).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) VALUES($1,$2,'same-provider-local-id','slack',$3,$4,$5,$6,'2026-01-01T00:00:00Z','2026-02-01T00:00:00Z') RETURNING version_id")
  .bind(g).bind(item).bind(namespace).bind(&origin).bind(text).bind(json!({"title":title,"synthetic_demo":origin["transport"]=="synthetic"})).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item)
        .bind(version)
        .execute(&f.owner)
        .await
        .unwrap();
    let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'slack',$2,'fixture',$3,now()) RETURNING atom_id")
  .bind(g).bind(format!("{namespace}:chunk")).bind(text).fetch_one(&f.owner).await.unwrap();
    sqlx::query(
        "INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)",
    )
    .bind(g)
    .bind(version)
    .bind(atom)
    .execute(&f.owner)
    .await
    .unwrap();
    let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact',$3,'source-atoms-v1','mock',$4) RETURNING semantic_atom_id")
  .bind(g).bind(version).bind(text).bind(json!([{"mention":mention,"type":"project"}])).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5)").bind(g).bind(version).bind(semantic).bind(text.chars().count() as i32).bind(text).execute(&f.owner).await.unwrap();
    for n in nodes {
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(g)
        .bind(n)
        .bind(semantic)
        .execute(&f.owner)
        .await
        .unwrap();
    }
    Source {
        item,
        version,
        atom,
        semantic,
    }
}
fn origin(service: &str, workspace: &str, grant: &str, synthetic: bool) -> Value {
    json!({"service_id":service,"transport":if synthetic{"synthetic"}else{"native"},"content_kind":"message","connection":{"kind":"oauth_grant","id":grant},"container":{"kind":"channel","id":"C1","provider_namespace":workspace,"label":"Engineering"}})
}
async fn edge(
    f: &Fixture,
    g: Uuid,
    a: Uuid,
    b: Uuid,
    predicate: &str,
    status: &str,
    prov: Value,
) -> Uuid {
    sqlx::query_scalar("INSERT INTO relationship_edge(graph_id,src_node_id,dst_node_id,predicate,status,provenance) VALUES($1,$2,$3,$4,$5,$6) RETURNING edge_id").bind(g).bind(a).bind(b).bind(predicate).bind(status).bind(prov).fetch_one(&f.owner).await.unwrap()
}

#[tokio::test]
async fn actual_library_scopes_counts_titles_facets_citations_and_legacy_permission_on_both_planes()
{
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "source_reads").await;
        let role: (bool, bool) =
            sqlx::query_as("SELECT rolbypassrls,rolsuper FROM pg_roles WHERE rolname=current_user")
                .fetch_one(&f.app)
                .await
                .unwrap();
        assert_eq!(role, (false, false));
        let atlas = node(&f, f.graph, "Atlas").await;
        let mixed = node(&f, f.graph, "Private Alias").await;
        let secret = node(&f, f.graph, "Restricted Secret").await;
        sqlx::query("UPDATE nodes SET body=$2 WHERE node_id=$1")
            .bind(mixed)
            .bind(json!({"aliases":["DO NOT DISCLOSE"],"summary":"DO NOT DISCLOSE"}))
            .execute(&f.owner)
            .await
            .unwrap();
        let a = source(
            &f,
            f.graph,
            "slack:workspace:W1",
            origin("slack", "slack:workspace:W1", "grant-a", false),
            "permitted",
            "Atlas launch",
            "Atlas ready.",
            &[atlas, mixed],
            "Atlas",
        )
        .await;
        let b = source(
            &f,
            f.graph,
            "demo:slack:workspace:W2",
            origin("slack", "slack:workspace:W2", "grant-b", true),
            "permitted",
            "Other capture",
            "Atlas reviews.",
            &[atlas],
            "Atlas",
        )
        .await;
        let denied = source(
            &f,
            f.graph,
            "slack:workspace:denied",
            origin("slack", "slack:workspace:denied", "denied-grant", false),
            "restricted",
            "DO NOT DISCLOSE",
            "DO NOT DISCLOSE Private Alias.",
            &[mixed, secret],
            "Private Alias",
        )
        .await;
        let unknown = source(
            &f,
            f.graph,
            "slack:workspace:unknown",
            origin("slack", "slack:workspace:unknown", "unknown-grant", false),
            "unknown",
            "DO NOT DISCLOSE UNKNOWN",
            "DO NOT DISCLOSE UNKNOWN.",
            &[secret],
            "Restricted Secret",
        )
        .await;
        let foreign_node = node(&f, f.foreign_graph, "Foreign Secret").await;
        let foreign = source(
            &f,
            f.foreign_graph,
            "slack:workspace:W1",
            origin("slack", "slack:workspace:W1", "grant-a", false),
            "permitted",
            "FOREIGN PRIVATE",
            "FOREIGN PRIVATE.",
            &[foreign_node],
            "Foreign Secret",
        )
        .await;
        sqlx::query("INSERT INTO assertion_disagreements(graph_id,left_node_id,right_node_id) VALUES($1,LEAST($2::uuid,$3::uuid),GREATEST($2::uuid,$3::uuid))")
            .bind(f.graph).bind(atlas).bind(mixed).execute(&f.owner).await.unwrap();
        let public = scope(f.graph, json!({}));
        let (status, v) = search(&f, public.clone(), "", 1, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["sources"]["total"], 2);
        assert_eq!(v["outcomes"]["total"], 2);
        assert_eq!(v["nodes"]["total"], 2);
        assert!(!v.to_string().contains("DO NOT DISCLOSE"));
        assert!(!v.to_string().contains("Private Alias"));
        assert!(!v.to_string().contains("FOREIGN"));
        assert_eq!(
            v["facets"]["services"][0]["source_count"], 2,
            "facets count the full accessible set, not the first page"
        );
        assert_eq!(v["prominent_work"][0]["node_id"], json!(atlas));
        assert_eq!(
            v["prominent_work"].as_array().unwrap().len(),
            1,
            "a project mention cannot promote an unrelated or unnamed linked node"
        );
        assert_eq!(v["prominent_work"][0]["synthetic_demo"], true);
        let second = search(
            &f,
            public.clone(),
            "",
            1,
            v["sources"]["next_cursor"].as_str().map(str::to_string),
        )
        .await;
        assert_eq!(second.0, StatusCode::OK);
        assert_ne!(
            v["sources"]["items"][0]["source_item_id"],
            second.1["sources"]["items"][0]["source_item_id"]
        );
        assert!(second.1["sources"]["next_cursor"].is_null());
        let bad_cursor = search(
            &f,
            public.clone(),
            "changed request",
            1,
            v["sources"]["next_cursor"].as_str().map(str::to_string),
        )
        .await;
        assert_eq!(bad_cursor.0, StatusCode::BAD_REQUEST);
        let container = scope(
            f.graph,
            json!({"service_id":"slack","container":{"kind":"channel","id":"C1","provider_namespace":"slack:workspace:W1"}}),
        );
        let (_, filtered) = search(&f, container.clone(), "", 20, None).await;
        assert_eq!(filtered["sources"]["total"], 1);
        let atlas_scoped = filtered["nodes"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["node_id"] == json!(atlas))
            .expect("selected Atlas contribution");
        let mixed_scoped = filtered["nodes"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["node_id"] == json!(mixed))
            .expect("independently supported anonymized node");
        assert_eq!(mixed_scoped["wider_context"]["source_support_count"], 1);
        assert!(mixed_scoped["title"].is_null());
        assert_eq!(
            atlas_scoped["current_understanding_status"],
            "unresolved_disagreement"
        );
        assert_eq!(atlas_scoped["unresolved_disagreement"], true);
        assert_eq!(atlas_scoped["contribution"]["source_support_count"], 1);
        assert_eq!(atlas_scoped["wider_context"]["source_support_count"], 2);
        let quote = &filtered["outcomes"]["items"][0]["citations"][0];
        assert_eq!(quote["source_item_id"], json!(a.item));
        assert_eq!(quote["source_version_id"], json!(a.version));
        assert_eq!(quote["quote"], "Atlas ready.");
        assert_ne!(quote["occurred_at"], quote["captured_at"]);
        assert_eq!(
            search(&f, public.clone(), "Private Alias", 20, None)
                .await
                .1["nodes"]["total"],
            0
        );
        for item in [denied.item, unknown.item, foreign.item] {
            assert_eq!(
                search(
                    &f,
                    scope(f.graph, json!({"source_item_id":item})),
                    "",
                    20,
                    None
                )
                .await
                .0,
                StatusCode::NOT_FOUND
            );
        }
        assert_eq!(
            search(
                &f,
                scope(
                    f.graph,
                    json!({"source_item_id":a.item,"source_version_id":b.version})
                ),
                "",
                20,
                None
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            search(
                &f,
                scope(
                    f.graph,
                    json!({"connection":{"kind":"oauth_grant","id":"grant-a"}})
                ),
                "",
                20,
                None
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            search(&f, scope(f.foreign_graph, json!({})), "", 20, None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        assert!(
            !sqlx::query_scalar::<_, bool>("SELECT library_atom_permitted($1,$2)")
                .bind(f.graph)
                .bind(denied.atom)
                .fetch_one(&mut *tx)
                .await
                .unwrap()
        );
        let counts =
            source_evidence::node_evidence_counts_tx(&mut tx, f.graph, &[atlas, mixed, secret])
                .await
                .unwrap();
        assert_eq!(counts.len(), 2);
        assert!(counts.iter().all(|c| c.source_support_count <= 2));
        tx.rollback().await.unwrap();
        let resolved = body(
            library_read::resolve_scope(
                f.tenant.clone(),
                Extension(f.user.clone()),
                Json(ScopeRequest {
                    scope: container.clone(),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(resolved.0, StatusCode::OK);
        // Resolution echoes the validated scope and counts nothing; the
        // scope's totals are the reader's (`scope_cte` through search).
        assert_eq!(resolved.1, resolved_scope(&container), "resolve-scope is scope-only");
        let retrieved = library_read::retrieve_scoped(&f.tenant, &f.user, container, "", 1)
            .await
            .expect("shared authorized retrieval");
        assert_eq!(retrieved["sources"]["total"], 1);
        assert_eq!(
            retrieved["outcomes"]["items"][0]["source_item_id"],
            json!(a.item)
        );
        assert_eq!(
            library_read::retrieve_scoped(
                &f.tenant,
                &f.user,
                scope(f.graph, json!({"source_item_id":denied.item})),
                "",
                1
            )
            .await
            .unwrap_err()
            .status(),
            StatusCode::NOT_FOUND
        );
        // An origin-only later observation changes the item filter but never
        // fabricates origin on its retained immutable version.
        let mut coding_origin = origin("slack", "slack:workspace:W2", "grant-b", true);
        coding_origin["content_kind"] = json!("coding_session");
        sqlx::query("UPDATE source_items SET origin=$2 WHERE source_item_id=$1")
            .bind(b.item)
            .bind(coding_origin)
            .execute(&f.owner)
            .await
            .unwrap();
        let (_, sessions) = search(
            &f,
            scope(f.graph, json!({"collection":"agent_sessions"})),
            "",
            20,
            None,
        )
        .await;
        assert_eq!(sessions["sources"]["total"], 1);
        assert_eq!(
            sessions["sources"]["items"][0]["origin"]["content_kind"],
            "coding_session"
        );
        assert_eq!(
            sessions["sources"]["items"][0]["version_origin"]["content_kind"],
            "message"
        );
        assert_eq!(
            sessions["outcomes"]["items"][0]["origin"]["content_kind"],
            "message"
        );
        assert_eq!(
            search(
                &f,
                scope(f.graph, json!({"collection":"meetings"})),
                "",
                20,
                None
            )
            .await
            .1["sources"]["total"],
            0
        );
        // Bound legacy chunks cannot resurrect as allegedly unmapped evidence after
        // a source/version FK cascade. This retains no deleted content in tombstones.
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        sqlx::query("UPDATE atoms SET source_evidence_bound=false WHERE atom_id=$1")
            .bind(a.atom)
            .execute(&mut *tx)
            .await
            .unwrap();
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT source_evidence_bound FROM atoms WHERE atom_id=$1"
            )
            .bind(a.atom)
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
            "a verified chunk never loses its binding history flag"
        );
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
            .bind(a.item)
            .execute(&mut *tx)
            .await
            .unwrap();
        assert!(
            !sqlx::query_scalar::<_, bool>("SELECT library_atom_permitted($1,$2)")
                .bind(f.graph)
                .bind(a.atom)
                .fetch_one(&mut *tx)
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        let (_, after_delete) = search(&f, public.clone(), "", 20, None).await;
        assert_eq!(after_delete["sources"]["total"], 1);
        assert_eq!(after_delete["nodes"]["total"], 1);
        assert_eq!(after_delete["nodes"]["items"][0]["node_id"], json!(atlas));
        f.close().await;
    }
}

#[tokio::test]
async fn neighborhoods_are_supported_scoped_paginated_and_never_reveal_raw_provenance_on_both_planes(
) {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "neighbors").await;
        let a = node(&f, f.graph, "Atlas").await;
        let b = node(&f, f.graph, "Roadmap").await;
        let support = source(
            &f,
            f.graph,
            "slack:workspace:W1",
            origin("slack", "slack:workspace:W1", "grant-a", false),
            "permitted",
            "Launch",
            "Atlas supports Roadmap.",
            &[a, b],
            "Atlas",
        )
        .await;
        let parent = edge(
            &f,
            f.graph,
            a,
            b,
            "supports",
            "confirmed",
            json!({"atom_ids":[support.atom],"private_reason":"DO NOT DISCLOSE"}),
        )
        .await;
        edge(
            &f,
            f.graph,
            a,
            b,
            "derived-support",
            "confirmed",
            json!({"source_edge_ids":[parent]}),
        )
        .await;
        let other = source(
            &f,
            f.graph,
            "gmail:mailbox:other",
            origin("google_gmail", "gmail:mailbox:other", "grant-a", false),
            "permitted",
            "Other provider",
            "Atlas has another source.",
            &[a, b],
            "Atlas",
        )
        .await;
        edge(
            &f,
            f.graph,
            a,
            b,
            "other-provider",
            "confirmed",
            json!({"atom_ids":[other.atom]}),
        )
        .await;
        edge(
            &f,
            f.graph,
            a,
            b,
            "human-rejected",
            "rejected",
            json!({"atom_ids":[support.atom],"reviewed_by":f.user.user_id}),
        )
        .await;
        edge(&f,f.graph,a,b,"unsupported","rejected",json!({"atom_ids":[support.atom],"lifecycle":"unsupported","human_review_status":"confirmed"})).await;
        let foreign_a = node(&f, f.foreign_graph, "Foreign A").await;
        let foreign_b = node(&f, f.foreign_graph, "Foreign B").await;
        let foreign_support = source(
            &f,
            f.foreign_graph,
            "slack:workspace:W1",
            origin("slack", "slack:workspace:W1", "foreign-grant", false),
            "permitted",
            "FOREIGN PRIVATE",
            "FOREIGN PRIVATE.",
            &[foreign_a, foreign_b],
            "Foreign A",
        )
        .await;
        let foreign_edge = edge(
            &f,
            f.foreign_graph,
            foreign_a,
            foreign_b,
            "foreign",
            "confirmed",
            json!({"atom_ids":[foreign_support.atom]}),
        )
        .await;
        edge(
            &f,
            f.graph,
            a,
            b,
            "forged-foreign",
            "confirmed",
            json!({"source_edge_ids":[foreign_edge]}),
        )
        .await;
        let request = || NeighborhoodRequest {
            scope: scope(f.graph, json!({"source_item_id":support.item})),
            node_id: a,
            page_size: Some(1),
            after: None,
        };
        let first = body(
            library_read::neighborhood(
                f.tenant.clone(),
                Extension(f.user.clone()),
                Json(request()),
            )
            .await,
        )
        .await;
        assert_eq!(first.0, StatusCode::OK);
        assert_eq!(first.1["items"].as_array().unwrap().len(), 1);
        assert_eq!(first.1["truncated"], true);
        assert!(!first.1.to_string().contains("DO NOT DISCLOSE"));
        assert!(!first.1.to_string().contains("forged-foreign"));
        let mut q = request();
        q.after = first.1["next_cursor"].as_str().map(str::to_string);
        let second = body(
            library_read::neighborhood(f.tenant.clone(), Extension(f.user.clone()), Json(q)).await,
        )
        .await;
        assert_eq!(second.0, StatusCode::OK);
        assert_eq!(second.1["items"].as_array().unwrap().len(), 1);
        assert_ne!(
            first.1["items"][0]["edge_id"],
            second.1["items"][0]["edge_id"]
        );
        assert_eq!(second.1["truncated"], false);
        assert_eq!(
            first.1["items"][0]["support"][0]["source_item_id"],
            json!(support.item)
        );
        let narrow=body(library_read::neighborhood(f.tenant.clone(),Extension(f.user.clone()),Json(NeighborhoodRequest{scope:scope(f.graph,json!({"service_id":"slack","connection":{"kind":"oauth_grant","id":"grant-a"}})),node_id:a,page_size:Some(50),after:None})).await).await;
        assert_eq!(narrow.0, StatusCode::OK);
        assert_eq!(narrow.1["items"].as_array().unwrap().len(), 2);
        assert!(!narrow.1.to_string().contains("other-provider"));
        assert!(!narrow.1.to_string().contains(&other.item.to_string()));
        let broadened = body(
            library_read::neighborhood(
                f.tenant.clone(),
                Extension(f.user.clone()),
                Json(NeighborhoodRequest {
                    scope: scope(f.graph, json!({})),
                    node_id: a,
                    page_size: Some(50),
                    after: None,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(broadened.0, StatusCode::OK);
        assert_eq!(broadened.1["items"].as_array().unwrap().len(), 3);
        assert!(broadened.1.to_string().contains("other-provider"));
        f.close().await;
    }
}

/// Extraction v2 (#1339) writes `valid_from` / `valid_until` on stated edges.
/// The neighborhood read returns them as RFC 3339 UTC instants (a stated
/// calendar date is UTC midnight), and null on an edge that states none.
#[tokio::test]
async fn neighborhood_items_carry_stated_edge_validity_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "neighbor_validity").await;
        let a = node(&f, f.graph, "Atlas").await;
        let b = node(&f, f.graph, "Dana").await;
        let support = source(&f, f.graph, "slack:workspace:W1", origin("slack", "slack:workspace:W1", "grant-a", false),
            "permitted", "Ownership", "Dana owns Atlas from 2026-03-01 until 2026-12-31.", &[a, b], "Atlas").await;
        let dated = edge(&f, f.graph, b, a, "responsible_for", "confirmed", json!({"atom_ids":[support.atom]})).await;
        let open_ended = edge(&f, f.graph, a, b, "responsibility_of", "confirmed", json!({"atom_ids":[support.atom]})).await;
        let undated = edge(&f, f.graph, a, b, "works_on", "confirmed", json!({"atom_ids":[support.atom]})).await;
        for (id, from, until) in [(dated, Some("2026-03-01T00:00:00Z"), Some("2026-12-31T00:00:00Z")), (open_ended, Some("2026-03-01T00:00:00Z"), None)] {
            sqlx::query("UPDATE relationship_edge SET valid_from=$2::timestamptz,valid_until=$3::timestamptz WHERE edge_id=$1")
                .bind(id).bind(from).bind(until).execute(&f.owner).await.unwrap();
        }
        let read = body(library_read::neighborhood(f.tenant.clone(), Extension(f.user.clone()), Json(NeighborhoodRequest {
            scope: scope(f.graph, json!({})), node_id: a, page_size: Some(50), after: None,
        })).await).await;
        assert_eq!(read.0, StatusCode::OK, "{}", read.1);
        let items = read.1["items"].as_array().unwrap();
        assert_eq!(items.len(), 3, "{}", read.1);
        let item = |id: Uuid| items.iter().find(|i| i["edge_id"] == json!(id)).unwrap_or_else(|| panic!("edge {id} missing: {}", read.1));
        let validity = |id: Uuid| (item(id)["valid_from"].clone(), item(id)["valid_until"].clone());
        assert_eq!(validity(dated), (json!("2026-03-01T00:00:00Z"), json!("2026-12-31T00:00:00Z")));
        assert_eq!(validity(open_ended), (json!("2026-03-01T00:00:00Z"), Value::Null));
        assert_eq!(validity(undated), (Value::Null, Value::Null));
        f.close().await;
    }
}

#[tokio::test]
async fn verified_legacy_binding_survives_denied_mapping_withdrawal_and_genuine_unmapped_chunks_stay_separate(
) {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "legacy_binding").await;
        let legacy = node(&f, f.graph, "Legacy").await;
        let old = node(&f, f.graph, "Old unmapped").await;
        let allowed = source(
            &f,
            f.graph,
            "legacy:allowed",
            origin("slack", "legacy:allowed", "allowed", false),
            "permitted",
            "Legacy",
            "Legacy retained.",
            &[],
            "Legacy",
        )
        .await;
        let denied = source(
            &f,
            f.graph,
            "legacy:denied",
            origin("slack", "legacy:denied", "denied", false),
            "unknown",
            "DO NOT DISCLOSE",
            "DO NOT DISCLOSE.",
            &[],
            "Legacy",
        )
        .await;
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(denied.version).bind(allowed.atom).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(legacy)
            .bind(allowed.atom)
            .execute(&f.owner)
            .await
            .unwrap();
        let raw:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','unmapped','fixture','Old unmapped chunk.',now()) RETURNING atom_id").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(old)
            .bind(raw)
            .execute(&f.owner)
            .await
            .unwrap();
        let (status, initial) = search(&f, scope(f.graph, json!({})), "", 20, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(initial["nodes"]["total"], 1);
        assert_eq!(initial["nodes"]["items"][0]["node_id"], json!(old));
        assert_eq!(
            initial["nodes"]["items"][0]["contribution"]["source_support_count"],
            0
        );
        assert_eq!(
            initial["nodes"]["items"][0]["contribution"]["unmapped_legacy_chunk_count"],
            1
        );
        let (status, axum::Json(items)) = source_evidence::list_items(
            f.tenant.clone(),
            Extension(f.user.clone()),
            axum::extract::Query(source_evidence::ItemsQuery {
                graph_id: f.graph,
                before: None,
                source_type: None,
                atom_id: Some(allowed.atom),
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            items["items"].as_array().unwrap().len(),
            0,
            "shared denied mapping suppresses verified legacy lookup"
        );
        let (status, _) = source_evidence::get_node_atoms(
            f.tenant.clone(),
            Extension(f.user.clone()),
            axum::extract::Path(legacy),
            axum::extract::Query(source_evidence::NodeAtomsQuery {
                graph_id: f.graph,
                before: None,
            }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        // Withdrawing the denied mapping restores the binding through the
        // permitted one. Deleting the denied item instead would retract the
        // shared atom for good (library_capture_purge, monotonic flag).
        sqlx::query("DELETE FROM source_version_chunks WHERE source_version_id=$1 AND atom_id=$2")
            .bind(denied.version)
            .bind(allowed.atom)
            .execute(&f.owner)
            .await
            .unwrap();
        let retracted: bool = sqlx::query_scalar("SELECT evidence_retracted FROM atoms WHERE atom_id=$1")
            .bind(allowed.atom)
            .fetch_one(&f.owner)
            .await
            .unwrap();
        assert!(!retracted, "mapping withdrawal is not a purge");
        let (_, recovered) = search(&f, scope(f.graph, json!({})), "", 20, None).await;
        assert_eq!(recovered["nodes"]["total"], 2);
        let row = recovered["nodes"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["node_id"] == json!(legacy))
            .unwrap();
        assert_eq!(row["title"], "Legacy");
        assert_eq!(row["contribution"]["source_support_count"], 1);
        assert_eq!(row["contribution"]["semantic_atom_count"], 0);
        let (_, selected) = search(
            &f,
            scope(f.graph, json!({"service_id":"slack"})),
            "",
            20,
            None,
        )
        .await;
        assert_eq!(
            selected["nodes"]["total"], 1,
            "unmapped chunks cannot claim connector contribution"
        );
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
            .bind(allowed.item)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let retracted: bool = sqlx::query_scalar("SELECT evidence_retracted FROM atoms WHERE atom_id=$1")
            .bind(allowed.atom)
            .fetch_one(&f.owner)
            .await
            .unwrap();
        assert!(retracted, "deleting the sole mapped item retracts its chunk");
        let (_, deleted) = search(&f, scope(f.graph, json!({})), "", 20, None).await;
        assert_eq!(deleted["sources"]["total"], 0);
        assert_eq!(deleted["nodes"]["total"], 1);
        assert_eq!(deleted["nodes"]["items"][0]["node_id"], json!(old));
        // Facet totals remain full-set counts while each option dimension and all
        // result groups are bounded independently. A legacy null origin stays null.
        for i in 0..101 {
            sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,'note','facet:fixture',$2,$3)").bind(f.graph).bind(format!("facet-{i}"))
    .bind(json!({"service_id":format!("fixture_{i:03}"),"transport":"native","content_kind":"note"})).execute(&f.owner).await.unwrap();
        }
        sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'note','facet:fixture','legacy-null')").bind(f.graph).execute(&f.owner).await.unwrap();
        let (_, facets) = search(&f, scope(f.graph, json!({})), "", 1, None).await;
        assert_eq!(facets["sources"]["total"], 102);
        assert_eq!(facets["sources"]["items"].as_array().unwrap().len(), 1);
        assert_eq!(facets["facets"]["services"].as_array().unwrap().len(), 100);
        assert_eq!(facets["facets"]["truncated"], true);
        assert_eq!(facets["facets"]["unknown_origin_count"], 1);
        let unicode = "界".repeat(4001);
        let long = source(
            &f,
            f.graph,
            "unicode:fixture",
            origin("slack", "unicode:fixture", "unicode", false),
            "permitted",
            "Unicode capture",
            &unicode,
            &[],
            "Unicode",
        )
        .await;
        for i in 0..21 {
            let linked = node(&f, f.graph, &format!("Quote node {i}")).await;
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(linked).bind(long.semantic).execute(&f.owner).await.unwrap();
        }
        for start in 1..=8i32 {
            let quote: String = unicode.chars().skip(start as usize).collect();
            sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,$4,4001,$5)").bind(f.graph).bind(long.version).bind(long.semantic).bind(start).bind(quote).execute(&f.owner).await.unwrap();
        }
        let (_, bounded) = search(
            &f,
            scope(f.graph, json!({"source_item_id":long.item})),
            "",
            1,
            None,
        )
        .await;
        let outcome = &bounded["outcomes"]["items"][0];
        assert_eq!(outcome["nodes"].as_array().unwrap().len(), 20);
        assert_eq!(outcome["nodes_truncated"], true);
        assert_eq!(outcome["citations"].as_array().unwrap().len(), 8);
        assert_eq!(outcome["citations_truncated"], true);
        let quote = &outcome["citations"][0];
        assert_eq!(quote["quote"].as_str().unwrap().chars().count(), 4000);
        assert_eq!(quote["end_char"], 4000);
        assert_eq!(quote["quote_truncated"], true);
        f.close().await;
    }
}

#[tokio::test]
async fn library_routes_construct_and_malformed_json_is_uniform_no_store_400() {
    use axum::{body::Body, extract::FromRequest, http::Request, routing::post, Router};
    let _: Router<Arc<coralswarm_backend::AppState>> = Router::new()
        .route("/library/search", post(library_read::search))
        .route("/library/resolve-scope", post(library_read::resolve_scope))
        .route("/library/neighborhood", post(library_read::neighborhood))
        .route("/library/node-sessions", post(library_read::node_sessions))
        .route("/library/facets", post(library_read::facets));
    for raw in [
        r#"{"scope":{"graph_ids":["not-a-uuid"]}}"#,
        r#"{"scope":{"graph_ids":[]},"trust_me":true}"#,
        r#"{"scope":{"graph_ids":[],"outcome_versions":"newest_means_valid"}}"#,
        "{",
    ] {
        let request = Request::builder()
            .header("content-type", "application/json")
            .body(Body::from(raw))
            .unwrap();
        let result = library_read::LibraryJson::<SearchRequest>::from_request(request, &()).await;
        let response = match result {
            Err(response) => response,
            Ok(_) => panic!("malformed request admitted"),
        };
        assert_eq!(body(response).await.0, StatusCode::BAD_REQUEST);
    }
}
#[tokio::test]
async fn updated_verified_mapping_marks_both_atoms_bound_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "updated_binding").await;
        let original_node = node(&f, f.graph, "Original").await;
        let replacement_node = node(&f, f.graph, "Replacement").await;
        let s = source(
            &f,
            f.graph,
            "binding:update",
            origin("slack", "binding:update", "fixture", false),
            "permitted",
            "Original",
            "Original retained.",
            &[],
            "Original",
        )
        .await;
        let replacement: Uuid = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'note','replacement','fixture','Replacement retained.',now()) RETURNING atom_id")
            .bind(f.graph).fetch_one(&f.owner).await.unwrap();
        for (n, a) in [(original_node, s.atom), (replacement_node, replacement)] {
            sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
                .bind(n)
                .bind(a)
                .execute(&f.owner)
                .await
                .unwrap();
        }
        assert!(!sqlx::query_scalar::<_, bool>(
            "SELECT source_evidence_bound FROM atoms WHERE atom_id=$1"
        )
        .bind(replacement)
        .fetch_one(&f.owner)
        .await
        .unwrap());
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        sqlx::query("UPDATE source_version_chunks SET atom_id=$3 WHERE graph_id=$1 AND source_version_id=$2 AND atom_id=$4").bind(f.graph).bind(s.version).bind(replacement).bind(s.atom).execute(&mut *tx).await.unwrap();
        sqlx::query("UPDATE atoms SET source_evidence_bound=false WHERE atom_id=ANY($1)")
            .bind(vec![s.atom, replacement])
            .execute(&mut *tx)
            .await
            .unwrap();
        let marked: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM atoms WHERE atom_id=ANY($1) AND source_evidence_bound",
        )
        .bind(vec![s.atom, replacement])
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(
            marked, 2,
            "old and replacement atoms retain monotonic verified-source binding"
        );
        sqlx::query("DELETE FROM source_versions WHERE graph_id=$1 AND version_id=$2")
            .bind(f.graph)
            .bind(s.version)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        for deleted_source in [false, true] {
            if deleted_source {
                let mut tx = f
                    .tenant
                    .begin_member_scoped(f.user.account_id)
                    .await
                    .unwrap();
                sqlx::query("DELETE FROM source_items WHERE graph_id=$1 AND source_item_id=$2")
                    .bind(f.graph)
                    .bind(s.item)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
            let (status, result) = search(&f, scope(f.graph, json!({})), "", 20, None).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                result["nodes"]["total"], 0,
                "formerly verified mappings never become genuine unmapped legacy evidence"
            );
        }
        f.close().await;
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn admission_and_source_revocation_during_blob_io_suppress_evidence_on_both_planes() {
    use axum::{
        extract::{Query, State},
        routing::get,
        Router,
    };
    use coralswarm_core::{crypto::encrypt, kek::wrap_dek, r2::R2Client};
    use std::sync::atomic::{AtomicUsize, Ordering};
    assert!(
        std::env::var("MASTER_KEK_HEX").is_ok_and(|v| v == "07".repeat(32)),
        "fixed public disposable fixture key required"
    );
    for tenant_plane in [false, true] {
        let f = Fixture::new(tenant_plane, "blob_fence").await;
        let control_fixture = if tenant_plane {
            Some(RlsProbe::setup("library_blob_control").await.unwrap())
        } else {
            None
        };
        let (control_owner, control_app) = if let Some(c) = &control_fixture {
            (&c.owner, &c.app_pool)
        } else {
            (&f.owner, &f.app)
        };
        if tenant_plane {
            sqlx::query(
                "INSERT INTO users(id,email) VALUES($1,'library-blob-fixture@example.test')",
            )
            .bind(f.user.user_id)
            .execute(control_owner)
            .await
            .unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,'org_library_proof')")
                .bind(f.user.account_id)
                .bind(f.user.user_id)
                .execute(control_owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO account_keys(account_id,dek_wrapped) VALUES($1,$2)")
                .bind(f.user.account_id)
                .bind(wrap_dek(&[7; 32], &[42; 32]).unwrap())
                .execute(&f.owner)
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE accounts SET dek_wrapped=$2 WHERE id=$1")
                .bind(f.user.account_id)
                .bind(wrap_dek(&[7; 32], &[42; 32]).unwrap())
                .execute(control_owner)
                .await
                .unwrap();
        }
        let s = source(
            &f,
            f.graph,
            "blob:fixture",
            origin("slack", "blob:fixture", "fixture", false),
            "permitted",
            "Retained evidence",
            "Private retained evidence.",
            &[],
            "Retained",
        )
        .await;
        sqlx::query(
            "UPDATE source_versions SET raw_inline=NULL,raw_blob_key=$2 WHERE version_id=$1",
        )
        .bind(s.version)
        // Canonical graph/tenant locator: a DEK/org prefix alone is not a grant.
        .bind(coralswarm_core::r2::blob_key_for(f.tenant.org_context().map(|c| c.org_id.as_str()), f.graph, s.version).unwrap())
        .execute(&f.owner)
        .await
        .unwrap();
        // The meeting/legacy route delegates to the same canonical source fence.
        sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,source_item_id) VALUES($1,'blob-wrapper',$2)").bind(f.graph).bind(s.item).execute(&f.owner).await.unwrap();
        let mut actions = vec!["none", "source", "membership", "graph"];
        if tenant_plane {
            actions.extend(["org", "binding"]);
        }
        for action in actions {
            sqlx::query("DELETE FROM org_revocations WHERE clerk_org_id='org_library_proof'")
                .execute(control_owner)
                .await
                .unwrap();
            if tenant_plane {
                sqlx::query("UPDATE accounts SET clerk_org_id='org_library_proof' WHERE id=$1")
                    .bind(f.user.account_id)
                    .execute(control_owner)
                    .await
                    .unwrap();
            }
            sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1)) ON CONFLICT DO NOTHING").bind(f.user.account_id).bind(f.user.user_id).execute(control_owner).await.unwrap();
            // Any graph kind/owner change re-denies every permitted item
            // (source_graph_permission_loss), so restore the graph first.
            sqlx::query("UPDATE graphs SET kind=$2,owner_user_id=$3 WHERE id=$1")
                .bind(f.graph)
                .bind(if tenant_plane { "member" } else { "personal" })
                .bind(if tenant_plane {
                    Some(f.user.user_id)
                } else {
                    None
                })
                .execute(&f.owner)
                .await
                .unwrap();
            sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1")
                .bind(s.item)
                .execute(&f.owner)
                .await
                .unwrap();
            let owner = f.owner.clone();
            let control = control_owner.clone();
            let actor = f.user.user_id;
            let account = f.user.account_id;
            let graph = f.graph;
            let item = s.item;
            let bytes =
                serde_json::to_vec(&encrypt(b"Private retained evidence.", &[42; 32]).unwrap())
                    .unwrap();
            let requests = Arc::new(AtomicUsize::new(0));
            let observed = requests.clone();
            let app=Router::new().fallback(get(move || {let owner=owner.clone();let control=control.clone();let bytes=bytes.clone();let observed=observed.clone();async move {
                observed.fetch_add(1,Ordering::SeqCst);
                match action {
                    "source"=>{sqlx::query("UPDATE source_items SET access_state='unknown' WHERE source_item_id=$1").bind(item).execute(&owner).await.unwrap();},
                    "membership"=>{sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(account).bind(actor).execute(&control).await.unwrap();},
                    "graph"=>{sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1").bind(graph).execute(&owner).await.unwrap();},
                    "binding"=>{
                        let mut financial=coralswarm_backend::tenancy::scope::begin_account_scoped(&control,account).await.unwrap();
                        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial,account).await.unwrap();
                        sqlx::query("UPDATE accounts SET clerk_org_id='org-library-other' WHERE id=$1").bind(account).execute(&mut *financial).await.unwrap();
                        financial.commit().await.unwrap();
                    },
                    "org"=>{sqlx::query("INSERT INTO org_revocations(clerk_org_id) VALUES('org_library_proof')").execute(&control).await.unwrap();},
                    _=>{}
                }
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
            let mut state =
                coralswarm_backend::test_app_state_owned_with_db(control_app.clone(), None);
            state.r2 = Some(Arc::new(r2));
            let (status, axum::Json(result)) = if matches!(action, "membership" | "org" | "binding")
            {
                coralswarm_backend::meeting_evidence::get_evidence(
                    State(Arc::new(state)),
                    f.tenant.clone(),
                    Extension(f.user.clone()),
                    Query(coralswarm_backend::meeting_evidence::EvidenceQuery {
                        graph_id: f.graph,
                        source_id: "blob-wrapper".into(),
                        version_id: Some(s.version),
                        before: None,
                        atoms_after: None,
                        atom_id: None,
                    }),
                )
                .await
            } else {
                source_evidence::get_evidence(
                    State(Arc::new(state)),
                    f.tenant.clone(),
                    Extension(f.user.clone()),
                    Query(source_evidence::EvidenceQuery {
                        graph_id: f.graph,
                        source_item_id: s.item,
                        version_id: Some(s.version),
                        before: None,
                        atoms_after: None,
                        atom_id: None,
                    }),
                )
                .await
            };
            assert_eq!(
                requests.load(Ordering::SeqCst),
                1,
                "actual encrypted GET required: {tenant_plane}/{action}"
            );
            if action == "none" {
                assert_eq!(status, StatusCode::OK);
                assert_eq!(
                    result["selected_version"]["raw_text"],
                    "Private retained evidence."
                );
            } else {
                assert_eq!(status, StatusCode::NOT_FOUND, "{tenant_plane}/{action}");
                assert!(result.get("selected_version").is_none());
                assert!(result.get("versions").is_none());
                assert!(!result.to_string().contains("Private retained"));
            }
            server.abort();
        }
        f.close().await;
        if let Some(c) = control_fixture {
            c.teardown().await;
        }
    }
}

#[tokio::test]
async fn p6_current_capture_is_explicit_pending_and_exact_history_overrides_aggregate_mode() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "p6_current").await;
        let project = node(&f, f.graph, "Atlas").await;
        let peer = node(&f, f.graph, "Peer").await;
        let a = source(
            &f,
            f.graph,
            "slack:W1",
            origin("slack", "slack:W1", "shared-grant", false),
            "permitted",
            "First capture",
            "Atlas prior decision.",
            &[project],
            "Atlas",
        )
        .await;
        let b = source(
            &f,
            f.graph,
            "slack:W2",
            origin("slack", "slack:W2", "shared-grant", false),
            "permitted",
            "Independent capture",
            "Atlas independent evidence.",
            &[project, peer],
            "Atlas",
        )
        .await;
        sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE version_id=ANY($1)")
            .bind(&[a.version, b.version])
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO assertion_disagreements(graph_id,left_node_id,right_node_id) VALUES($1,LEAST($2::uuid,$3::uuid),GREATEST($2::uuid,$3::uuid))").bind(f.graph).bind(project).bind(peer).execute(&f.owner).await.unwrap();
        edge(
            &f,
            f.graph,
            project,
            peer,
            "retained-support",
            "confirmed",
            json!({"atom_ids":[a.atom]}),
        )
        .await;
        // Deliberately earlier clocks: pointer membership, not capture/event order,
        // selects current evidence. No contradiction is resolved by this update.
        let current:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'Atlas current capture.','{\"title\":\"New capture pending\"}','2009-01-01','2010-01-01','pending' FROM source_items WHERE source_item_id=$1 RETURNING version_id").bind(a.item).fetch_one(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
            .bind(a.item)
            .bind(current)
            .execute(&f.owner)
            .await
            .unwrap();
        let (status, retained) =
            search(&f, scope(f.graph, json!({"node_id":project})), "", 20, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(retained["scope"]["outcome_versions"], "all_retained");
        assert_eq!(retained["outcomes"]["total"], 2);
        assert_eq!(
            retained["outcomes"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| v["source_version_is_current"] == false)
                .count(),
            1
        );
        let current_scope = scope(
            f.graph,
            json!({"node_id":project,"outcome_versions":"current"}),
        );
        let (status, v) = search(&f, current_scope.clone(), "", 20, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["sources"]["total"], 2);
        assert_eq!(v["outcomes"]["total"], 1);
        assert_eq!(v["outcomes"]["items"][0]["source_item_id"], json!(b.item));
        assert_eq!(v["outcomes"]["items"][0]["source_version_is_current"], true);
        let n = &v["nodes"]["items"][0];
        assert_eq!(n["node_id"], json!(project));
        assert_eq!(n["contribution"]["source_support_count"], 2);
        assert_eq!(n["contribution"]["semantic_atom_count"], 1);
        assert_eq!(n["current_capture"]["status"], "pending");
        assert_eq!(n["current_capture"]["pending_version_count"], 1);
        assert_eq!(n["current_capture"]["current_semantic_atom_count"], 1);
        assert_eq!(n["current_understanding_status"], "unresolved_disagreement");
        let (status, pending) = search(
            &f,
            scope(
                f.graph,
                json!({"node_id":project,"source_item_id":a.item,"outcome_versions":"current"}),
            ),
            "",
            20,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(pending["outcomes"]["total"], 0);
        assert_eq!(pending["nodes"]["total"], 1);
        assert_eq!(
            pending["nodes"]["items"][0]["current_capture"]["status"],
            "pending"
        );
        assert_eq!(
            pending["nodes"]["items"][0]["wider_context"]["semantic_atom_count"],
            2
        );
        assert_eq!(
            pending["sources"]["items"][0]["selected_extraction_status"],
            "pending"
        );
        assert_eq!(
            pending["sources"]["items"][0]["current_extraction_status"],
            "pending"
        );
        let exact = scope(
            f.graph,
            json!({"source_item_id":a.item,"source_version_id":a.version,"outcome_versions":"current"}),
        );
        let (status, history) = search(&f, exact, "", 20, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(history["scope"]["outcome_versions"], "all_retained");
        assert_eq!(history["outcomes"]["total"], 1);
        assert_eq!(
            history["outcomes"]["items"][0]["source_version_is_current"],
            false
        );
        assert_eq!(
            history["sources"]["items"][0]["selected_version_id"],
            json!(a.version)
        );
        assert_eq!(
            history["sources"]["items"][0]["current_version_id"],
            json!(current)
        );
        assert_eq!(
            history["sources"]["items"][0]["selected_extraction_status"],
            "done"
        );
        assert_eq!(
            history["sources"]["items"][0]["current_extraction_status"],
            "pending"
        );
        let neighborhood = body(
            library_read::neighborhood(
                f.tenant.clone(),
                Extension(f.user.clone()),
                Json(NeighborhoodRequest {
                    scope: current_scope.clone(),
                    node_id: project,
                    page_size: Some(20),
                    after: None,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(neighborhood.0, StatusCode::OK);
        assert_eq!(
            neighborhood.1["items"][0]["support"][0]["source_version_is_current"], false,
            "retained typed support is not advertised as current capture evidence"
        );
        // Queue state is authoritative if a process dies before recording the
        // version failure. A different capture/window cannot override readiness.
        sqlx::query("UPDATE source_versions SET extraction_status='running' WHERE version_id=$1")
            .bind(current)
            .execute(&f.owner)
            .await
            .unwrap();
        let job:Uuid=sqlx::query_scalar("INSERT INTO jobs(account_id,kind,payload,status) VALUES($1,'ExtractSourceAtoms',jsonb_build_object('graph_id',$2::uuid,'source_version_id',$3::uuid,'start_char',0),'dead') RETURNING id").bind(f.user.account_id).bind(f.graph).bind(current).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO jobs(account_id,kind,payload,status,created_at) VALUES($1,'ExtractSourceAtoms',jsonb_build_object('graph_id',$2::uuid,'source_version_id',$3::uuid,'start_char',0),'dead',now()+INTERVAL '1 hour')").bind(f.user.account_id).bind(f.graph).bind(a.version).execute(&f.owner).await.unwrap();
        for (queue, status, readiness) in [
            ("dead", "failed", "incomplete"),
            ("pending", "pending", "pending"),
            ("running", "running", "pending"),
        ] {
            sqlx::query("UPDATE jobs SET status=$2::job_status WHERE id=$1")
                .bind(job)
                .bind(queue)
                .execute(&f.owner)
                .await
                .unwrap();
            let (code, view) = search(
                &f,
                scope(
                    f.graph,
                    json!({"node_id":project,"source_item_id":a.item,"outcome_versions":"current"}),
                ),
                "",
                20,
                None,
            )
            .await;
            assert_eq!(code, StatusCode::OK);
            assert_eq!(
                view["sources"]["items"][0]["current_extraction_status"],
                status
            );
            assert_eq!(
                view["sources"]["items"][0]["selected_extraction_status"],
                status
            );
            assert_eq!(
                view["nodes"]["items"][0]["current_capture"]["status"],
                readiness
            );
            assert_eq!(
                view["nodes"]["items"][0]["current_capture"]["incomplete_version_count"],
                if queue == "dead" { 1 } else { 0 }
            );
        }
        let (_, pinned) = search(
            &f,
            scope(
                f.graph,
                json!({"node_id":project,"source_item_id":a.item,"source_version_id":a.version}),
            ),
            "",
            20,
            None,
        )
        .await;
        assert_eq!(
            pinned["sources"]["items"][0]["selected_extraction_status"], "done",
            "a completed pinned historical capture ignores its stale dead job"
        );
        assert_eq!(
            pinned["sources"]["items"][0]["current_extraction_status"], "running",
            "historical jobs cannot override another current capture"
        );
        sqlx::query("UPDATE source_versions SET next_start_char=10 WHERE version_id=$1")
            .bind(current)
            .execute(&f.owner)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE jobs SET status='dead',created_at=now()+INTERVAL '2 hours' WHERE id=$1",
        )
        .bind(job)
        .execute(&f.owner)
        .await
        .unwrap();
        let next_job:Uuid=sqlx::query_scalar("INSERT INTO jobs(account_id,kind,payload,status) VALUES($1,'ExtractSourceAtoms',jsonb_build_object('graph_id',$2::uuid,'source_version_id',$3::uuid,'start_char',10),'pending') RETURNING id").bind(f.user.account_id).bind(f.graph).bind(current).fetch_one(&f.owner).await.unwrap();
        let (_, continued) = search(
            &f,
            scope(f.graph, json!({"node_id":project,"source_item_id":a.item})),
            "",
            20,
            None,
        )
        .await;
        assert_eq!(
            continued["sources"]["items"][0]["current_extraction_status"], "pending",
            "a dead earlier window cannot override the exact continuation"
        );
        sqlx::query("UPDATE jobs SET status='dead' WHERE id=$1")
            .bind(next_job)
            .execute(&f.owner)
            .await
            .unwrap();
        let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'decision','Atlas current capture.','source-atoms-v1','mock','[{\"mention\":\"Atlas\",\"type\":\"project\"}]') RETURNING semantic_atom_id").bind(f.graph).bind(current).fetch_one(&f.owner).await.unwrap();
        sqlx::query(
            "INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)",
        )
        .bind(f.graph)
        .bind(project)
        .bind(semantic)
        .execute(&f.owner)
        .await
        .unwrap();
        sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,22,'Atlas current capture.')").bind(f.graph).bind(current).bind(semantic).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_versions SET extraction_status='done' WHERE version_id=$1")
            .bind(current)
            .execute(&f.owner)
            .await
            .unwrap();
        let (_, ready) = search(&f, current_scope, "", 20, None).await;
        assert_eq!(ready["outcomes"]["total"], 2);
        assert!(ready["outcomes"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["source_version_is_current"] == true));
        assert_eq!(
            ready["nodes"]["items"][0]["current_capture"]["status"],
            "ready"
        );
        assert_eq!(
            ready["nodes"]["items"][0]["current_understanding_status"],
            "unresolved_disagreement"
        );
        assert_eq!(
            search(
                &f,
                scope(
                    f.graph,
                    json!({"node_id":project,"outcome_versions":"all_retained"})
                ),
                "",
                20,
                None
            )
            .await
            .1["outcomes"]["total"],
            3
        );
        f.close().await;
    }
}

async fn facet_page(
    f: &Fixture,
    s: LibraryScope,
    dimension: library_read::FacetDimension,
    query: &str,
    limit: i64,
    after: Option<String>,
) -> (StatusCode, Value) {
    body(
        library_read::facets(
            f.tenant.clone(),
            Extension(f.user.clone()),
            Json(library_read::FacetRequest {
                scope: s,
                dimension,
                option_query: query.into(),
                page_size: Some(limit),
                after,
            }),
        )
        .await,
    )
    .await
}

#[tokio::test]
async fn p6_facets_page_search_and_count_only_authorized_options_on_generated_corpus() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "p6_facets").await;
        // Local generated history: 5,000 independent source items, 10,000 retained
        // outcomes, 160 services and 240 qualified containers, with denied sources.
        sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) SELECT $1,'coding_session','perf:workspace','perf-'||i,jsonb_build_object('service_id','svc_'||lpad((i%160)::text,3,'0'),'transport','native','content_kind','coding_session','connection',jsonb_build_object('kind','oauth_grant','id','same-opaque-grant'),'container',jsonb_build_object('kind','project','id','container-'||(i%240),'provider_namespace','perf:workspace:'||(i%2),'label','Project '||(i%240))),CASE WHEN i%10=0 THEN 'restricted' WHEN i%10=1 THEN 'unknown' ELSE 'permitted' END FROM generate_series(1,5000) i").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'Perf project '||(split_part(source_id,'-',2)::int%64)||' has generated local evidence.',jsonb_build_object('title','Captured '||source_id),'2026-01-01'::timestamptz+rev*INTERVAL '1 day','done' FROM source_items CROSS JOIN generate_series(0,1) rev WHERE graph_id=$1 AND source_id LIKE 'perf-%'").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("UPDATE source_items s SET current_version_id=v.version_id FROM source_versions v WHERE v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.captured_at='2026-01-02' AND s.graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT graph_id,version_id,0,'fact',raw_inline,'source-atoms-v1','mock',jsonb_build_array(jsonb_build_object('mention','Perf project '||(split_part(source_id,'-',2)::int%64),'type','project')) FROM source_versions WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO nodes(graph_id,type,title) SELECT $1,'entity','Perf project '||i FROM generate_series(0,63) i").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT a.graph_id,n.node_id,a.semantic_atom_id FROM semantic_atoms a JOIN source_versions v ON v.graph_id=a.graph_id AND v.version_id=a.source_version_id JOIN nodes n ON n.graph_id=a.graph_id AND n.title='Perf project '||(split_part(v.source_id,'-',2)::int%64) WHERE a.graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT graph_id,'coding_session','chunk-'||source_id,'fixture','Perf project '||(split_part(source_id,'-',2)::int%64)||' has generated local evidence.',now() FROM source_items WHERE graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) SELECT v.graph_id,v.version_id,a.atom_id FROM source_versions v JOIN atoms a ON a.graph_id=v.graph_id AND a.source_id='chunk-'||v.source_id WHERE v.graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) SELECT n.node_id,a.atom_id FROM atoms a JOIN nodes n ON n.graph_id=a.graph_id AND n.title='Perf project '||((CASE WHEN a.source_id~'^chunk-perf-[0-9]+$' THEN split_part(a.source_id,'-',3)::int ELSE NULL END)%64) WHERE a.graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO relationship_edge(graph_id,src_node_id,dst_node_id,predicate,status,provenance) SELECT n.graph_id,n.node_id,d.node_id,'supports','confirmed',jsonb_build_object('atom_ids',jsonb_build_array((SELECT a.atom_id FROM node_atoms l JOIN atoms a ON a.atom_id=l.atom_id AND a.graph_id=n.graph_id WHERE l.node_id=n.node_id ORDER BY a.atom_id LIMIT 1))) FROM nodes n JOIN nodes d ON d.graph_id=n.graph_id AND d.title='Perf project '||(((CASE WHEN n.title~'^Perf project [0-9]+$' THEN split_part(n.title,' ',3)::int ELSE NULL END)+1)%64) WHERE n.graph_id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        let secret = source(
            &f,
            f.graph,
            "private:workspace",
            origin(
                "private_service",
                "private:workspace",
                "private-grant",
                false,
            ),
            "restricted",
            "DO NOT DISCLOSE",
            "DO NOT DISCLOSE.",
            &[],
            "Private",
        )
        .await;
        let foreign = source(
            &f,
            f.foreign_graph,
            "perf:workspace",
            origin(
                "foreign_service",
                "perf:workspace",
                "same-opaque-grant",
                false,
            ),
            "permitted",
            "FOREIGN PRIVATE",
            "FOREIGN PRIVATE.",
            &[],
            "Foreign",
        )
        .await;
        // Partially extracted current captures exercise queue-aware readiness
        // cost too; historical completed versions stay retained and distinct.
        sqlx::query("UPDATE source_versions v SET extraction_status='pending',next_start_char=10 FROM source_items s WHERE s.graph_id=$1 AND v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.version_id=s.current_version_id").bind(f.graph).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO jobs(account_id,kind,payload,status) SELECT $2,'ExtractSourceAtoms',jsonb_build_object('graph_id',v.graph_id,'source_version_id',v.version_id,'start_char',10),CASE (CASE WHEN s.source_id~'^perf-[0-9]+$' THEN split_part(s.source_id,'-',2)::int END)%3 WHEN 0 THEN 'dead'::job_status WHEN 1 THEN 'pending'::job_status ELSE 'running'::job_status END FROM source_items s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.version_id=s.current_version_id WHERE s.graph_id=$1 AND s.source_id~'^perf-[0-9]+$'").bind(f.graph).bind(f.user.account_id).execute(&f.owner).await.unwrap();
        // Generated bulk loads need planner statistics before representative
        // application-role timing; authorization remains enabled for reads.
        sqlx::query("ANALYZE").execute(&f.owner).await.unwrap();
        let role: (bool, bool) =
            sqlx::query_as("SELECT rolbypassrls,rolsuper FROM pg_roles WHERE rolname=current_user")
                .fetch_one(&f.app)
                .await
                .unwrap();
        assert_eq!(role, (false, false));
        let s = scope(f.graph, json!({"collection":"agent_sessions"}));
        let started = std::time::Instant::now();
        let (status, summary) = search(&f, s.clone(), "", 1, None).await;
        let search_ms = started.elapsed().as_millis();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(summary["sources"]["total"], 4000);
        assert_eq!(summary["nodes"]["total"], 64);
        assert_eq!(summary["prominent_work"].as_array().unwrap().len(), 6);
        assert_eq!(summary["outcomes"]["total"], 8000);
        assert_eq!(summary["facets"]["services"].as_array().unwrap().len(), 100);
        assert_eq!(summary["facets"]["truncated"], true);
        let start = std::time::Instant::now();
        let (status, first) = facet_page(
            &f,
            s.clone(),
            library_read::FacetDimension::Services,
            "",
            50,
            None,
        )
        .await;
        let facet_ms = start.elapsed().as_millis();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            first["total"], 128,
            "denied-only services contribute zero options"
        );
        assert_eq!(first["items"].as_array().unwrap().len(), 50);
        assert_eq!(first["truncated"], true);
        let mut keys = std::collections::BTreeSet::new();
        let mut count = 0i64;
        let mut page = first.clone();
        loop {
            for item in page["items"].as_array().unwrap() {
                assert!(keys.insert(item["key"].as_str().unwrap().to_string()));
                count += item["source_count"].as_i64().unwrap();
            }
            let Some(cursor) = page["next_cursor"].as_str().map(str::to_string) else {
                break;
            };
            let (status, next) = facet_page(
                &f,
                s.clone(),
                library_read::FacetDimension::Services,
                "",
                50,
                Some(cursor),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            page = next;
        }
        assert_eq!(keys.len(), 128);
        assert_eq!(count, 4000);
        assert!(!keys.contains("private_service"));
        assert!(!keys.contains("foreign_service"));
        let (_, selected) = facet_page(
            &f,
            s.clone(),
            library_read::FacetDimension::Services,
            "svc_159",
            50,
            None,
        )
        .await;
        assert_eq!(selected["total"], 1);
        assert_eq!(
            selected["items"][0]["key"], "svc_159",
            "a selected option beyond the first100 remains discoverable"
        );
        let (_, connections) = facet_page(
            &f,
            s.clone(),
            library_read::FacetDimension::Connections,
            "same-opaque-grant",
            100,
            None,
        )
        .await;
        assert_eq!(
            connections["total"], 128,
            "service qualifies otherwise-colliding grant IDs"
        );
        let (_, containers) = facet_page(
            &f,
            s.clone(),
            library_read::FacetDimension::Containers,
            "",
            100,
            None,
        )
        .await;
        assert!(containers["total"].as_i64().unwrap() > 100);
        assert_eq!(containers["truncated"], true);
        assert_eq!(
            facet_page(
                &f,
                s.clone(),
                library_read::FacetDimension::Services,
                "changed",
                50,
                first["next_cursor"].as_str().map(str::to_string)
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            facet_page(
                &f,
                s.clone(),
                library_read::FacetDimension::Containers,
                "",
                50,
                first["next_cursor"].as_str().map(str::to_string)
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        for item in [secret.item, foreign.item] {
            assert_eq!(
                facet_page(
                    &f,
                    scope(f.graph, json!({"source_item_id":item})),
                    library_read::FacetDimension::Services,
                    "",
                    50,
                    None
                )
                .await
                .0,
                StatusCode::NOT_FOUND
            );
        }
        assert_eq!(
            facet_page(
                &f,
                scope(f.graph, json!({"service_id":"private_service"})),
                library_read::FacetDimension::Services,
                "",
                50,
                None
            )
            .await
            .1["total"],
            0
        );
        let (_, current) = search(
            &f,
            scope(
                f.graph,
                json!({"collection":"agent_sessions","outcome_versions":"current"}),
            ),
            "",
            1,
            None,
        )
        .await;
        assert_eq!(current["outcomes"]["total"], 4000);
        assert_eq!(current["sources"]["total"], 4000);
        assert!(
            search_ms < 5000 && facet_ms < 5000,
            "bounded local query budget violated"
        );
        eprintln!("P6 local corpus tenant={tenant}:5000sourceitems/10000outcomes/5000chunks/64nodes/64typededges search={search_ms}ms facet={facet_ms}ms; permitted4000sources/128services");
        f.close().await;
    }
}

async fn captured_history(
    f: &Fixture,
    s: LibraryScope,
    session: Option<&str>,
    namespace: Option<&str>,
    children: bool,
    limit: i64,
    after: Option<String>,
) -> (StatusCode, Value) {
    captured_history_at(f, s, None, session, namespace, children, limit, after).await
}
#[allow(clippy::too_many_arguments)]
async fn captured_history_at(
    f: &Fixture,
    s: LibraryScope,
    temporal: Option<coralswarm_backend::temporal::TemporalSelection>,
    session: Option<&str>,
    namespace: Option<&str>,
    children: bool,
    limit: i64,
    after: Option<String>,
) -> (StatusCode, Value) {
    body(
        coralswarm_backend::library_history::history(
            axum::extract::State(Arc::new(coralswarm_backend::test_app_state_owned_with_db(f.app.clone(), None))),
            f.tenant.clone(),
            Extension(f.user.clone()),
            Json(coralswarm_backend::library_history::HistoryRequest {
                scope: s,
                temporal,
                session_id: session.map(str::to_string),
                provider_namespace: namespace.map(|n| coralswarm_backend::source_origin::ProviderNamespace::new(n).unwrap()),
                include_subagents: children,
                page_size: Some(limit),
                after,
            }),
        )
        .await,
    )
    .await
}
async fn context_session(f: &Fixture, id: &str, parent: Option<&str>, root: &str) {
    use coralswarm_backend::store::{SessionIdentity, Store};
    let mut tx = f
        .tenant
        .begin_member_scoped(f.user.account_id)
        .await
        .unwrap();
    PostgresStore::new(f.app.clone())
        .upsert_coding_session_scoped(
            &mut tx,
            f.graph,
            id,
            "agent",
            Some("PRIVATE MUTABLE PROJECT"),
            Some("PRIVATE MUTABLE TITLE"),
            None,
            None,
            None,
            Some("PRIVATE MUTABLE HARNESS"),
            Some("PRIVATE MUTABLE MODEL"),
            Some("PRIVATE MUTABLE BRANCH"),
            &SessionIdentity {
                parent_session_id: parent.map(str::to_string),
                root_session_id: Some(root.into()),
                task: Some("PRIVATE MUTABLE TASK".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
async fn context_capture(
    f: &Fixture,
    namespace: &str,
    id: &str,
    ctx: Value,
) -> source_evidence::CaptureOutcome {
    let meta = json!({"capture_context":ctx,"capture_extent":"checkpoint"});
    let capture = source_evidence::CaptureSource {
        source_type: "agent_session",
        provider_namespace: namespace,
        source_id: id,
        normalized_text: "Useful project evidence.",
        representation: "generated_summary",
        metadata: &meta,
        occurred_at: Some("2009-01-01T00:00:00Z".parse().unwrap()),
        participants: &["Fixture participant".into()],
        ingested_by: Some(f.user.user_id),
        scope: "private",
        duration_ms: None,
        segments: None,
    };
    let mut tx = f
        .tenant
        .begin_member_scoped(f.user.account_id)
        .await
        .unwrap();
    let outcome = source_evidence::capture_source_version_with_origin_tx(
        &mut tx,
        None,
        f.graph,
        f.user.account_id,
        &capture,
        None,
        &[7; 32],
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    outcome
}
#[tokio::test]
async fn p6_ambiguous_legacy_lineage_requires_unique_same_namespace_support() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "ambiguous_lineage").await;
        let role: (bool, bool) =
            sqlx::query_as("SELECT rolbypassrls,rolsuper FROM pg_roles WHERE rolname=current_user")
                .fetch_one(&f.app)
                .await
                .unwrap();
        assert_eq!(role, (false, false));
        context_session(&f, "amb", None, "amb").await;
        context_session(&f, "amb:detail", None, "amb:detail").await;
        context_session(&f, "child", Some("amb"), "amb").await;
        let legacy = source(
            &f,
            f.graph,
            "fake:history",
            origin("capture", "fake:history", "fixture-grant", false),
            "permitted",
            "Ambiguous historical prefix",
            "Synthetic legacy body.",
            &[],
            "Legacy",
        )
        .await;
        sqlx::query("UPDATE atoms SET source_type='agent_session',source_id='cc:amb:detail:0' WHERE atom_id=$1").bind(legacy.atom).execute(&f.owner).await.unwrap();
        let child=context_capture(&f,"fake:history","child-capture",json!({"revision":1,"session_id":"child","session_kind":"agent","parent_session_id":"amb","root_session_id":"amb"})).await;
        let child_scope = scope(f.graph, json!({"source_item_id":child.source_item_id}));
        let (status, history) =
            captured_history(&f, child_scope.clone(), Some("child"), None, false, 20, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            history["versions"][0]["context"]["parent_session_id"],
            Value::Null
        );
        assert_eq!(
            history["versions"][0]["context"]["root_session_id"],
            Value::Null
        );
        assert_eq!(
            history["versions"][0]["context"]["lineage_status"],
            "partial"
        );
        assert_eq!(
            captured_history(&f, child_scope.clone(), Some("amb"), Some("fake:history"), true, 20, None)
                .await
                .0,
            StatusCode::NOT_FOUND,
            "ambiguous chunk prefix must not select a child through inherited root identity"
        );
        // A uniquely mapped legacy source remains an independent positive control.
        context_session(&f, "unique", None, "unique").await;
        let unique = source(
            &f,
            f.graph,
            "fake:unique",
            origin("capture", "fake:unique", "fixture-grant", false),
            "permitted",
            "Unique legacy source",
            "Synthetic unique body.",
            &[],
            "Unique",
        )
        .await;
        sqlx::query(
            "UPDATE atoms SET source_type='agent_session',source_id='cc:unique:0' WHERE atom_id=$1",
        )
        .bind(unique.atom)
        .execute(&f.owner)
        .await
        .unwrap();
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT library_session_supported($1,'fake:unique','unique')"
        )
        .bind(f.graph)
        .fetch_one(&mut *tx)
        .await
        .unwrap());
        tx.commit().await.unwrap();
        // Exact reported captures from a different namespace cannot certify it.
        let root_context =
            json!({"revision":1,"session_id":"amb","session_kind":"agent","root_session_id":"amb"});
        context_capture(&f, "fake:other", "root-capture", root_context.clone()).await;
        assert_eq!(
            captured_history(&f, child_scope.clone(), Some("amb"), Some("fake:history"), true, 20, None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        let root = context_capture(&f, "fake:history", "root-capture", root_context).await;
        let (status, history) =
            captured_history(&f, child_scope.clone(), Some("amb"), Some("fake:history"), true, 20, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(history["total"], 1);
        assert_eq!(
            history["versions"][0]["context"]["parent_session_id"],
            "amb"
        );
        assert_eq!(history["versions"][0]["context"]["root_session_id"], "amb");
        assert_eq!(
            history["versions"][0]["context"]["lineage_status"],
            "captured"
        );
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
            .bind(root.source_item_id)
            .execute(&f.owner)
            .await
            .unwrap();
        assert_eq!(
            captured_history(&f, child_scope.clone(), Some("amb"), Some("fake:history"), true, 20, None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        let (_, history) = captured_history(&f, child_scope, Some("child"), None, false, 20, None).await;
        assert_eq!(
            history["versions"][0]["context"]["parent_session_id"],
            Value::Null
        );
        assert_eq!(
            history["versions"][0]["context"]["root_session_id"],
            Value::Null
        );
        f.close().await;
    }
}

#[tokio::test]
async fn p6_immutable_history_keeps_metadata_reversals_and_masks_source_context_before_legacy_filters(
) {
    use coralswarm_backend::store::{FacetDimension, SessionListFilter, Store};
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "capture_history").await;
        context_session(&f, "root", None, "root").await;
        context_session(&f, "child", Some("root"), "root").await;
        let root = source(
            &f,
            f.graph,
            "fake:history",
            origin("capture", "fake:history", "fixture-grant", false),
            "unknown",
            "Old reported root",
            "Old recorded root.",
            &[],
            "Root",
        )
        .await;
        sqlx::query("UPDATE source_versions SET source_metadata=source_metadata||$2 WHERE version_id=$1").bind(root.version).bind(json!({"session_id":"root","session_kind":"agent","session_title":"Old reported root","branch":"historical-main","model":"UNCONTRACTED OLD MODEL"})).execute(&f.owner).await.unwrap();
        let ctx = json!({"revision":1,"session_id":"child","session_kind":"agent","session_title":"Useful reported child","project":"Fixture project","project_key":"example.test/org/repo","repo_path":"/fixture/repo","remote_sanitized":"example.test/org/repo","branch":"A","hostname":"fixture-host","harness_version":"fixture-harness1","model":"captured-model","agent_name":"Reported helper","platform":"openclaw","task":"Bounded reported task","subagent_name":"Reported helper","parent_session_id":"root","root_session_id":"root"});
        let a = context_capture(&f, "fake:history", "checkpoint-child", ctx.clone()).await;
        let mut bctx = ctx.clone();
        bctx["branch"] = json!("B");
        let b = context_capture(&f, "fake:history", "checkpoint-child", bctx).await;
        let a2 = context_capture(&f, "fake:history", "checkpoint-child", ctx.clone()).await;
        assert_eq!(a.source_item_id, b.source_item_id);
        assert_eq!(a.source_item_id, a2.source_item_id);
        assert_ne!(a.version_id, b.version_id);
        assert_ne!(a.version_id, a2.version_id);
        let stored:Vec<(Uuid,Value)>=sqlx::query_as("SELECT version_id,capture_context FROM source_versions WHERE source_item_id=$1 ORDER BY captured_at,version_id").bind(a.source_item_id).fetch_all(&f.owner).await.unwrap();
        assert_eq!(stored.len(), 3);
        assert_eq!(
            stored
                .iter()
                .map(|(_, c)| c["branch"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["A", "B", "A"]
        );
        let public_metadata: Vec<Value> = sqlx::query_scalar(
            "SELECT source_metadata FROM source_versions WHERE source_item_id=$1",
        )
        .bind(a.source_item_id)
        .fetch_all(&f.owner)
        .await
        .unwrap();
        assert!(
            public_metadata
                .iter()
                .all(|m| m["capture_context"].get("parent_session_id").is_none()
                    && m["capture_context"].get("root_session_id").is_none()),
            "old readers must not disclose private lineage references"
        );
        let project = node(&f, f.graph, "Useful project").await;
        for v in [a.version_id, b.version_id] {
            let sem:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'decision','Useful project uses retained evidence.','source-atoms-v1','mock','[{\"mention\":\"Useful project\",\"type\":\"project\"}]') RETURNING semantic_atom_id").bind(f.graph).bind(v).fetch_one(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,23,'Useful project evidence.')").bind(f.graph).bind(v).bind(sem).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(project).bind(sem).execute(&f.owner).await.unwrap();
        }
        let selected = scope(
            f.graph,
            json!({"source_item_id":a.source_item_id,"node_id":project}),
        );
        let (_, first) =
            captured_history(&f, selected.clone(), Some("child"), None, false, 1, None).await;
        assert_eq!(first["total"], 3);
        assert_eq!(
            first["versions"][0]["source_version_id"],
            a2.version_id.to_string()
        );
        assert_eq!(first["versions"][0]["source_version_is_current"], true);
        // Unmanaged captures are held at admission, never silently queued.
        assert_eq!(first["versions"][0]["extraction_status"], "blocked");
        assert!(first["versions"][0]["extraction_reason"].as_str().is_some_and(|r| r.ends_with("managed_admission_required")));
        assert_eq!(first["versions"][0]["outcomes"]["total"], 0);
        assert_eq!(first["versions"][0]["context"]["branch"], "A");
        assert_eq!(first["versions"][0]["context"]["model"], "captured-model");
        assert_eq!(
            first["versions"][0]["context"]["parent_session_id"],
            Value::Null
        );
        assert_eq!(
            first["versions"][0]["context"]["root_session_id"],
            Value::Null
        );
        assert_eq!(first["versions"][0]["context"]["lineage_status"], "partial");
        assert_eq!(
            first["versions"][0]["participants"],
            json!(["Fixture participant"])
        );
        assert_ne!(
            first["versions"][0]["captured_at"],
            first["versions"][0]["source_event_at"]
        );
        assert!(first["versions"][0]["source_event_at"]
            .as_str()
            .unwrap()
            .starts_with("2009"));
        assert!(!first.to_string().contains("PRIVATE MUTABLE"));
        let mut seen = std::collections::HashSet::new();
        seen.insert(a2.version_id.to_string());
        let mut next = first["next_cursor"].as_str().map(str::to_string);
        while next.is_some() {
            let (status, p) =
                captured_history(&f, selected.clone(), Some("child"), None, false, 1, next).await;
            assert_eq!(status, StatusCode::OK);
            for v in p["versions"].as_array().unwrap() {
                assert!(seen.insert(v["source_version_id"].as_str().unwrap().to_string()));
            }
            next = p["next_cursor"].as_str().map(str::to_string);
        }
        assert_eq!(seen.len(), 3);
        let (_,exact)=captured_history(&f,scope(f.graph,json!({"source_item_id":a.source_item_id,"source_version_id":a.version_id,"node_id":project,"outcome_versions":"current"})),None,None,false,20,None).await;
        assert_eq!(exact["scope"]["outcome_versions"], "all_retained");
        assert_eq!(exact["total"], 1);
        assert_eq!(exact["versions"][0]["outcomes"]["total"], 1);
        assert_eq!(exact["versions"][0]["passages"]["total"], 1);
        assert_eq!(
            exact["versions"][0]["outcomes"]["items"][0]["source_version_is_current"],
            false
        );
        assert_eq!(
            exact["versions"][0]["outcomes"]["items"][0]["citations"][0]["source_version_id"],
            a.version_id.to_string()
        );
        assert_eq!(
            captured_history(
                &f,
                scope(f.graph, json!({"outcome_versions":"current"})),
                None,
                None,
                false,
                20,
                None
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            captured_history(
                &f,
                selected.clone(),
                Some("root"),
                Some("fake:history"),
                true,
                20,
                first["next_cursor"].as_str().map(str::to_string)
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        let store = PostgresStore::new(f.app.clone());
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        let rows = store
            .list_coding_sessions_scoped(
                &mut tx,
                f.graph,
                None,
                None,
                None,
                None,
                20,
                &SessionListFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title.as_deref(), Some("Useful reported child"));
        assert_eq!(rows[0].model.as_deref(), Some("captured-model"));
        assert_eq!(
            store
                .list_coding_sessions_scoped(
                    &mut tx,
                    f.graph,
                    None,
                    None,
                    None,
                    None,
                    20,
                    &SessionListFilter {
                        model: Some("PRIVATE MUTABLE MODEL".into()),
                        ..Default::default()
                    }
                )
                .await
                .unwrap()
                .len(),
            0
        );
        let facets = store
            .session_facets_scoped(
                &mut tx,
                f.graph,
                None,
                None,
                &SessionListFilter::default(),
                None,
                200,
            )
            .await
            .unwrap();
        assert_eq!(facets.values(FacetDimension::Model)[0].value, "captured-model");
        assert!(facets
            .values(FacetDimension::Branch)
            .iter()
            .all(|v| v.value != "PRIVATE MUTABLE BRANCH"));
        tx.commit().await.unwrap();
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        assert!(sqlx::query("UPDATE source_versions SET capture_context=capture_context||'{\"branch\":\"rewrite\"}',source_metadata=jsonb_set(source_metadata,'{capture_context,branch}','\"rewrite\"') WHERE version_id=$1").bind(a.version_id).execute(&mut *tx).await.is_err());
        tx.rollback().await.unwrap();
        let mut status_tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        sqlx::query("UPDATE source_versions SET extraction_status='running',next_start_char=7 WHERE version_id=$1").bind(a.version_id).execute(&mut *status_tx).await.unwrap();
        status_tx.commit().await.unwrap();
        let mut parent_tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        assert!(sqlx::query("UPDATE coding_sessions SET parent_session_id='forged-parent' WHERE graph_id=$1 AND session_id='child'").bind(f.graph).execute(&mut *parent_tx).await.is_err(),"first-fixed parent must not be mutable");
        parent_tx.rollback().await.unwrap();
        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1")
            .bind(root.item)
            .execute(&f.owner)
            .await
            .unwrap();
        let (_, with_parent) =
            captured_history(&f, selected.clone(), Some("child"), None, false, 20, None).await;
        assert_eq!(
            with_parent["versions"][0]["context"]["parent_session_id"],
            "root"
        );
        let (_, root_tree) =
            captured_history(&f, scope(f.graph, json!({})), Some("root"), Some("fake:history"), true, 20, None).await;
        assert_eq!(
            root_tree["total"], 4,
            "root plus child versions, no chunk duplicates"
        );
        assert_eq!(
            root_tree["versions"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| v["source_item_id"] == a.source_item_id.to_string())
                .count(),
            3
        );
        let historical = root_tree["versions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["source_version_id"] == root.version.to_string())
            .unwrap();
        assert_eq!(historical["context_status"], "historical_partial");
        assert_eq!(historical["context"]["model"], Value::Null);
        assert_eq!(historical["context"]["branch"], "historical-main");
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
            .bind(a.source_item_id)
            .execute(&f.owner)
            .await
            .unwrap();
        assert_eq!(
            captured_history(&f, selected.clone(), Some("child"), None, false, 20, None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        assert!(store
            .session_facets_scoped(
                &mut tx,
                f.graph,
                None,
                None,
                &SessionListFilter {
                    model: Some("captured-model".into()),
                    ..Default::default()
                },
                None,
                200
            )
            .await
            .unwrap()
            .values(FacetDimension::Branch)
            .is_empty());
        tx.commit().await.unwrap();
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
            .bind(a.source_item_id)
            .execute(&f.owner)
            .await
            .unwrap();
        assert_eq!(
            captured_history(
                &f,
                scope(f.graph, json!({})),
                Some("child"),
                None,
                false,
                20,
                None
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        f.close().await;
    }
}

#[tokio::test]
async fn p6_session_history_qualifies_namespace_collisions_and_never_uses_unverified_prefix_descriptors(
) {
    use coralswarm_backend::store::{FacetDimension, SessionListFilter, Store};
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "history_namespaces").await;
        let id = "fixture%_:session";
        context_session(&f, id, None, id).await;
        let a = source(
            &f,
            f.graph,
            "history:A",
            origin("capture", "history:A", "same-grant", false),
            "permitted",
            "Readable A",
            "Recorded A.",
            &[],
            "A",
        )
        .await;
        let b = source(
            &f,
            f.graph,
            "history:B",
            origin("capture", "history:B", "same-grant", false),
            "permitted",
            "Readable B",
            "Recorded B.",
            &[],
            "B",
        )
        .await;
        for (s, branch) in [(&a, "A"), (&b, "B")] {
            sqlx::query("UPDATE source_versions SET source_metadata=source_metadata||$2 WHERE version_id=$1").bind(s.version).bind(json!({"session_id":id,"session_kind":"agent","session_title":format!("Reported {branch}"),"branch":branch})).execute(&f.owner).await.unwrap();
        }
        let all = || scope(f.graph, json!({}));
        let (status, ambiguous) = captured_history(&f, all(), Some(id), None, false, 20, None).await;
        assert_eq!(status, StatusCode::CONFLICT, "two namespaces must never merge into one history");
        assert_eq!(ambiguous["selection"], "ambiguous_session");
        assert_eq!(ambiguous["session_id"], id);
        assert!(ambiguous.get("versions").is_none());
        let candidates = ambiguous["candidates"].as_array().unwrap();
        assert_eq!(
            candidates.iter().map(|c| (c["provider_namespace"].as_str().unwrap(), c["title"].as_str().unwrap(), c["version_count"].as_u64().unwrap())).collect::<Vec<_>>(),
            vec![("history:A", "Reported A", 1), ("history:B", "Reported B", 1)]
        );
        assert!(candidates.iter().all(|c| c["graph_id"] == f.graph.to_string() && c["session_id"] == id));
        let (status, exact) =
            captured_history(&f, all(), Some(id), Some("history:A"), false, 1, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(exact["total"], 1);
        assert_eq!(
            exact["session"],
            json!({"graph_id":f.graph,"provider_namespace":"history:A","session_id":id})
        );
        assert_eq!(exact["versions"][0]["source_item_id"], a.item.to_string());
        assert!(exact.get("session_groups").is_none());
        let (status, selected) = captured_history(&f, scope(f.graph, json!({"service_id":"capture","connection":{"kind":"oauth_grant","id":"same-grant"},"container":{"kind":"channel","id":"C1","provider_namespace":"history:A"}})), Some(id), None, false, 20, None).await;
        assert_eq!(status, StatusCode::OK, "a scope holding one namespace resolves");
        assert_eq!(selected["total"], 1);
        assert_eq!(selected["session"]["provider_namespace"], "history:A");
        assert_eq!(selected["versions"][0]["source_item_id"], a.item.to_string());
        let (status, exact_b) =
            captured_history(&f, all(), Some(id), Some("history:B"), false, 20, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(exact_b["versions"][0]["source_item_id"], b.item.to_string());
        assert_eq!(
            captured_history(&f, all(), Some(id), Some("history:C"), false, 20, None).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            captured_history(&f, all(), None, Some("history:A"), false, 20, None).await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            captured_history(&f, all(), Some(id), None, true, 20, None).await.0,
            StatusCode::BAD_REQUEST,
            "a subagent tree is only read from a qualified session"
        );
        // A second version under A so a qualified page mints a cursor.
        sqlx::query("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at+interval '1 day' FROM source_versions WHERE version_id=$1").bind(a.version).execute(&f.owner).await.unwrap();
        let (_, first_a) =
            captured_history(&f, all(), Some(id), Some("history:A"), false, 1, None).await;
        assert_eq!(first_a["total"], 2);
        let cursor = first_a["next_cursor"].as_str().map(str::to_string);
        assert!(cursor.is_some());
        assert_eq!(
            captured_history(&f, all(), Some(id), Some("history:B"), false, 1, cursor.clone()).await.0,
            StatusCode::BAD_REQUEST,
            "a cursor minted for A must not page B"
        );
        assert_eq!(
            captured_history(&f, all(), Some(id), None, false, 1, cursor.clone()).await.0,
            StatusCode::BAD_REQUEST
        );
        let (status, second_a) =
            captured_history(&f, all(), Some(id), Some("history:A"), false, 1, cursor).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(second_a["versions"][0]["provider_namespace"], "history:A");
        let store = PostgresStore::new(f.app.clone());
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        assert!(
            store
                .list_coding_sessions_scoped(
                    &mut tx,
                    f.graph,
                    None,
                    None,
                    None,
                    None,
                    20,
                    &SessionListFilter::default()
                )
                .await
                .unwrap()
                .is_empty(),
            "unqualified legacy ID cannot merge two provider identities"
        );
        tx.commit().await.unwrap();
        sqlx::query("UPDATE source_items SET access_state='unknown' WHERE source_item_id=$1")
            .bind(b.item)
            .execute(&f.owner)
            .await
            .unwrap();
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        let rows = store
            .list_coding_sessions_scoped(
                &mut tx,
                f.graph,
                None,
                None,
                None,
                None,
                20,
                &SessionListFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title.as_deref(), Some("Reported A"));
        assert_eq!(rows[0].model, None);
        let facets = store
            .session_facets_scoped(
                &mut tx,
                f.graph,
                None,
                None,
                &SessionListFilter::default(),
                None,
                200,
            )
            .await
            .unwrap();
        assert_eq!(facets.values(FacetDimension::Branch).len(), 1);
        assert_eq!(facets.values(FacetDimension::Branch)[0].value, "A");
        tx.commit().await.unwrap();
        // A plausible raw checkpoint prefix without a verified mapping is no
        // captured-session descriptor or source evidence.
        context_session(&f, "unmapped", None, "unmapped").await;
        sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'coding_session','cc:unmapped:checkpoint','fixture','UNPROVED PRIVATE DESCRIPTOR',now())").bind(f.graph).execute(&f.owner).await.unwrap();
        assert_eq!(
            captured_history(
                &f,
                scope(f.graph, json!({})),
                Some("unmapped"),
                None,
                false,
                20,
                None
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        sqlx::query("DELETE FROM source_items WHERE source_item_id=$1")
            .bind(a.item)
            .execute(&f.owner)
            .await
            .unwrap();
        assert_eq!(
            captured_history(&f, scope(f.graph, json!({})), Some(id), None, false, 20, None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        f.close().await;
    }
}

fn version_ids(page: &Value) -> Vec<String> {
    page["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["source_version_id"].as_str().unwrap().to_string())
        .collect()
}
fn sorted(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids
}

#[tokio::test]
async fn cor7_subagent_history_walks_same_namespace_lineage_and_counts_cross_namespace_claims() {
    let (p, c1, g, x, y_child) =
        ("tree-parent", "tree-child", "tree-grandchild", "tree-cross", "tree-unrelated-child");
    let ctx = |id: &str, parent: Option<&str>, root: &str| {
        let mut c = json!({"revision":1,"session_id":id,"session_kind":"agent","root_session_id":root});
        if let Some(parent) = parent {
            c["parent_session_id"] = json!(parent);
        }
        c
    };
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "subagent_tree").await;
        for (id, parent) in [(p, None), (c1, Some(p)), (g, Some(c1)), (x, Some(p)), (y_child, Some(p))] {
            context_session(&f, id, parent, p).await;
        }
        let mut own = vec![];
        for source_id in ["p-1", "p-2"] {
            own.push(context_capture(&f, "tree:A", source_id, ctx(p, None, p)).await.version_id.to_string());
        }
        let child = context_capture(&f, "tree:A", "c1-1", ctx(c1, Some(p), p)).await.version_id.to_string();
        let grandchild = context_capture(&f, "tree:A", "g-1", ctx(g, Some(c1), p)).await.version_id.to_string();
        let mut cross = vec![];
        for source_id in ["x-1", "x-2"] {
            cross.push(context_capture(&f, "tree:B", source_id, ctx(x, Some(p), p)).await.version_id.to_string());
        }
        context_capture(&f, "tree:C", "yp-1", ctx(p, None, p)).await;
        context_capture(&f, "tree:C", "yc-1", ctx(y_child, Some(p), p)).await;
        let all = || scope(f.graph, json!({}));

        let (status, mid) = captured_history(&f, all(), Some(c1), Some("tree:A"), true, 50, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            sorted(version_ids(&mid)),
            sorted(vec![child.clone(), grandchild.clone()]),
            "a mid-level subagent reaches its own children through their parent pointer"
        );
        assert_eq!(mid["total"], 2);
        assert_eq!(
            mid["subagents"],
            json!({"sessions":[{"session_id":g,"parent_session_id":c1,"version_count":1}],"sessions_truncated":false,"excluded":[],"excluded_truncated":false})
        );

        let (status, tree) = captured_history(&f, all(), Some(p), Some("tree:A"), true, 50, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(tree["session"], json!({"graph_id":f.graph,"provider_namespace":"tree:A","session_id":p}));
        let expected = sorted(own.iter().cloned().chain([child.clone(), grandchild.clone()]).collect());
        assert_eq!(sorted(version_ids(&tree)), expected, "each tree version exactly once");
        assert_eq!(tree["total"], 4);
        assert_eq!(
            tree["subagents"],
            json!({"sessions":[
                {"session_id":c1,"parent_session_id":p,"version_count":1},
                {"session_id":g,"parent_session_id":c1,"version_count":1}],
             "sessions_truncated":false,
             "excluded":[{"provider_namespace":"tree:B","version_count":2}],
             "excluded_truncated":false})
        );
        let by_id = |id: &str| {
            tree["versions"].as_array().unwrap().iter().find(|v| v["source_version_id"] == id).unwrap().clone()
        };
        assert_eq!(by_id(&child)["context"]["parent_session_id"], p);
        assert_eq!(by_id(&grandchild)["context"]["parent_session_id"], c1);
        assert_eq!(by_id(&grandchild)["context"]["root_session_id"], p);
        assert!(tree["versions"].as_array().unwrap().iter().all(|v| v["provider_namespace"] == "tree:A"));
        let wire = tree.to_string();
        for hidden in cross.iter().map(String::as_str).chain([x, y_child]) {
            assert!(!wire.contains(hidden), "excluded or unrelated lineage leaked: {hidden}");
        }

        let mut walked = vec![];
        let mut after = None;
        loop {
            let (status, page) = captured_history(&f, all(), Some(p), Some("tree:A"), true, 1, after).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(page["total"], 4);
            assert_eq!(page["subagents"], tree["subagents"]);
            walked.extend(version_ids(&page));
            after = page["next_cursor"].as_str().map(str::to_string);
            if after.is_none() {
                break;
            }
            assert!(walked.len() < 8, "cursor walk does not terminate");
        }
        assert_eq!(walked.len(), 4, "no duplicates across pages");
        assert_eq!(sorted(walked), expected, "no gaps across pages");

        let (status, flat) = captured_history(&f, all(), Some(p), Some("tree:A"), false, 50, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(sorted(version_ids(&flat)), sorted(own.clone()));
        assert_eq!(flat["total"], 2);
        assert_eq!(flat["subagents"], Value::Null);

        let (status, ambiguous) = captured_history(&f, all(), Some(p), None, false, 50, None).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            ambiguous["candidates"].as_array().unwrap().iter()
                .map(|c| (c["provider_namespace"].as_str().unwrap(), c["version_count"].as_u64().unwrap()))
                .collect::<Vec<_>>(),
            vec![("tree:A", 2), ("tree:C", 1)],
            "candidates count the requested session's own versions only"
        );
        f.close().await;
    }
}

#[tokio::test]
async fn cor7_ambiguous_session_with_historical_temporal_read_writes_no_receipt() {
    let id = "temporal-session";
    let historical: coralswarm_backend::temporal::TemporalSelection = serde_json::from_value(json!({
        "mode":"historical","revision":1,"state_key":"status",
        "effective_at":"2026-01-01T00:00:00Z","as_known_at":"2026-01-02T00:00:00Z"}))
    .unwrap();
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "history_temporal_ambiguity").await;
        let subject = node(&f, f.graph, "Temporal subject").await;
        for namespace in ["hist:A", "hist:C"] {
            let s = source(&f, f.graph, namespace, origin("capture", namespace, "same-grant", false), "permitted",
                "Temporal capture", "Recorded status.", &[subject], "Subject").await;
            sqlx::query("UPDATE source_versions SET source_metadata=source_metadata||$2 WHERE version_id=$1")
                .bind(s.version).bind(json!({"session_id":id,"session_kind":"agent"})).execute(&f.owner).await.unwrap();
        }
        let receipts = || async {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM answer_temporal_receipts").fetch_one(&f.owner).await.unwrap()
        };
        let at_node = || scope(f.graph, json!({"node_id":subject}));
        let before = receipts().await;
        let (status, ambiguous) =
            captured_history_at(&f, at_node(), Some(historical.clone()), Some(id), None, false, 20, None).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(ambiguous["selection"], "ambiguous_session");
        assert!(ambiguous.get("temporal_state").is_none());
        assert_eq!(receipts().await, before, "a refused selection leaves before any temporal receipt");
        let (status, exact) =
            captured_history_at(&f, at_node(), Some(historical.clone()), Some(id), Some("hist:A"), false, 20, None).await;
        assert_eq!(receipts().await, before + 1, "the resolved historical read records exactly one receipt");
        if tenant {
            assert_eq!(status, StatusCode::NOT_FOUND);
        } else {
            assert_eq!(status, StatusCode::OK);
            assert_eq!(exact["session"]["provider_namespace"], "hist:A");
            assert_eq!(exact["total"], 1);
            assert!(exact["temporal_state"].is_object());
        }
        f.close().await;
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn p6_actual_ingest_fixes_concurrent_first_parent_and_failed_identity_has_zero_capture_or_blob_writes(
) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .with_test_writer()
        .try_init();
    use axum::{routing::put, Router};
    use coralswarm_backend::tenancy::TenantId;
    use coralswarm_core::r2::R2Client;
    use std::sync::atomic::{AtomicUsize, Ordering};
    async fn ingest_value(
        state: &Arc<coralswarm_backend::AppState>,
        f: &Fixture,
        value: Value,
    ) -> (StatusCode, Value) {
        let r = coralswarm_backend::test_ingest_with_source_context(
            state,
            &f.tenant,
            &f.user,
            serde_json::from_value(value).unwrap(),
            "fake:ingest",
        )
        .await;
        let status = r.status();
        let bytes = to_bytes(r.into_body(), 4_000_000).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "capture_ingest").await;
        // Native capture now fences fresh CONTROL membership/organization
        // before R2 and commit. Tenant content is never a CONTROL substitute.
        let control_fixture=if tenant {Some(RlsProbe::setup("library_capture_control").await.unwrap())}else{None};
        let control_app=if let Some(c)=&control_fixture {
            sqlx::query("INSERT INTO users(id,email) VALUES($1,'capture-control-fixture@example.test')").bind(f.user.user_id).execute(&c.owner).await.unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,'org_library_proof')").bind(f.user.account_id).bind(f.user.user_id).execute(&c.owner).await.unwrap();
            sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT id FROM seat_tiers WHERE code='standard' AND is_active))").bind(f.user.account_id).bind(f.user.user_id).execute(&c.owner).await.unwrap();
            c.app_pool.clone()
        }else{f.app.clone()};

        let role: (bool, bool) =
            sqlx::query_as("SELECT rolbypassrls,rolsuper FROM pg_roles WHERE rolname=current_user")
                .fetch_one(&f.app)
                .await
                .unwrap();
        assert_eq!(role, (false, false));
        let mut base =
            coralswarm_backend::test_app_state_owned_with_db_and_store(control_app.clone(), None);
        let tenant_id = if tenant {
            TenantId::Org(f.tenant.org_context().unwrap().org_id.clone())
        } else {
            TenantId::Consumer
        };
        base.dek_cache
            .insert(&tenant_id, f.user.account_id, [7; 32]);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let app = Router::new().fallback(put(move || {
            let n = observed.clone();
            async move {
                n.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base.r2 = Some(Arc::new(
            R2Client::new_with_endpoint_override(
                "fixture",
                "fixture-access",
                "fixture-secret",
                "fixture-bucket",
                &endpoint,
            )
            .await,
        ));
        let failing = Arc::new(base);
        sqlx::raw_sql("CREATE FUNCTION fixture_refuse_session() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.session_id='identity-failure' THEN RAISE EXCEPTION 'fake unavailable identity'; END IF; RETURN NEW; END $$;CREATE TRIGGER fixture_refuse_session BEFORE INSERT ON coding_sessions FOR EACH ROW EXECUTE FUNCTION fixture_refuse_session()")
            .execute(&f.owner).await.unwrap();
        let before_versions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        let before_jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
            .fetch_one(&f.owner)
            .await
            .unwrap();
        let req = json!({"graph_id":f.graph,"source_type":"coding_session","source_id":"cc:identity-failure:checkpoint","content":"Reported local fixture checkpoint.","participants":[],"timestamp":"2009-01-01T00:00:00Z","session_id":"identity-failure","session_kind":"coding","model":"fixture-model","branch":"A"});
        let (status, result) = ingest_value(&failing, &f, req).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{result}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE graph_id=$1")
                .bind(f.graph)
                .fetch_one(&f.owner)
                .await
                .unwrap(),
            before_versions
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs")
                .fetch_one(&f.owner)
                .await
                .unwrap(),
            before_jobs
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM coding_sessions WHERE graph_id=$1 AND session_id='identity-failure'").bind(f.graph).fetch_one(&f.owner).await.unwrap(),0);
        server.abort();
        context_session(&f, "root-a", None, "root-a").await;
        context_session(&f, "root-b", None, "root-b").await;
        let good = Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(
            control_app.clone(),
            None,
        ));
        good.dek_cache
            .insert(&tenant_id, f.user.account_id, [7; 32]);
        let request = |parent: &str, checkpoint: &str| json!({"graph_id":f.graph,"source_type":"agent_session","source_id":format!("cc:competing:{checkpoint}"),"content":"Reported concurrent fixture checkpoint.","participants":[],"timestamp":"2009-01-01T00:00:00Z","session_id":"competing","session_kind":"agent","agent_name":"Fixture helper","platform":"openclaw","task":"Reported bounded task","parent_session_id":parent,"remote":"https://user:fake-secret@example.test/org/repo.git","repo_path":"/fixture/repo","model":"fixture-model","harness_version":"fixture-harness","branch":"captured-branch"});
        let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(
                ingest_value(&good, &f, request("root-a", "a")),
                ingest_value(&good, &f, request("root-b", "b"))
            )
        })
        .await
        .expect("session lock must not self-deadlock");
        assert!(
            (a.0 == StatusCode::OK && b.0 == StatusCode::CONFLICT)
                || (b.0 == StatusCode::OK && a.0 == StatusCode::CONFLICT),
            "concurrent first parents: {a:?} {b:?}"
        );
        let parent:String=sqlx::query_scalar("SELECT parent_session_id FROM coding_sessions WHERE graph_id=$1 AND session_id='competing'").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        let snapshots:Vec<Value>=sqlx::query_scalar("SELECT capture_context FROM source_versions WHERE graph_id=$1 AND capture_context->>'session_id'='competing'").bind(f.graph).fetch_all(&f.owner).await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0]["parent_session_id"], parent);
        assert_eq!(snapshots[0]["root_session_id"], parent);
        assert_eq!(snapshots[0]["task"], "Reported bounded task");
        assert_eq!(snapshots[0]["model"], "fixture-model");
        assert_eq!(snapshots[0]["remote_sanitized"], "example.test/org/repo");
        assert!(!snapshots[0].to_string().contains("fake-secret"));
        let (again, _) = ingest_value(&good, &f, request(&parent, "again")).await;
        assert_eq!(again, StatusCode::OK);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM source_versions WHERE graph_id=$1 AND capture_context->>'session_id'='competing' AND capture_context->>'parent_session_id'<>$2").bind(f.graph).bind(&parent).fetch_one(&f.owner).await.unwrap(),0);
        // Source writer validates captured lineage on every internal entry.
        let meta = json!({"capture_context":{"revision":1,"session_id":"competing","session_kind":"agent","parent_session_id":"forged-parent","root_session_id":"forged-root"}});
        let cap = source_evidence::CaptureSource {
            source_type: "agent_session",
            provider_namespace: "fake:writer",
            source_id: "forged-context",
            normalized_text: "Fixture",
            representation: "generated_summary",
            metadata: &meta,
            occurred_at: None,
            participants: &[],
            ingested_by: Some(f.user.user_id),
            scope: "private",
            duration_ms: None,
            segments: None,
        };
        let mut tx = f
            .tenant
            .begin_member_scoped(f.user.account_id)
            .await
            .unwrap();
        assert!(source_evidence::capture_source_version_tx(
            &mut tx,
            None,
            f.graph,
            f.user.account_id,
            &cap,
            None,
            &[7; 32]
        )
        .await
        .is_err());
        tx.rollback().await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM source_items WHERE graph_id=$1 AND source_id='forged-context'").bind(f.graph).fetch_one(&f.owner).await.unwrap(),0);
        // Revocation during actual encrypted localhost PUT must roll back all
        // rows/jobs and remove newly uploaded source/chunk blobs. Consumer
        // admission reuses its held txn; tenant CONTROL is separate.
        use axum::http::Method;
        for cleanup_fails in [false,true] {
            if cleanup_fails {
                let db=control_fixture.as_ref().map(|c|&c.owner).unwrap_or(&f.owner);
                sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))")
                    .bind(f.user.account_id).bind(f.user.user_id).execute(db).await.unwrap();
            }
            let source_id=format!("revoke-during-upload-{cleanup_fails}");
        let put_calls=Arc::new(AtomicUsize::new(0));let deletes=Arc::new(AtomicUsize::new(0));
        let c_owner=control_fixture.as_ref().map(|c|c.owner.clone()).unwrap_or_else(||f.owner.clone());
        let member_account=f.user.account_id;let member_user=f.user.user_id;
        let pc=put_calls.clone();let dc=deletes.clone();
        let revoke_app=Router::new().fallback(move |method:Method| {let p=pc.clone();let d=dc.clone();let db=c_owner.clone();async move {
            if method==Method::PUT && p.fetch_add(1,Ordering::SeqCst)==0 {
                sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(member_account).bind(member_user).execute(&db).await.unwrap();
            }
            if method==Method::DELETE {d.fetch_add(1,Ordering::SeqCst);if cleanup_fails {return StatusCode::BAD_REQUEST;}}
            StatusCode::OK
        }});
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint=format!("http://{}",listener.local_addr().unwrap());
        let revoke_server=tokio::spawn(async move {axum::serve(listener,revoke_app).await.unwrap()});
        let mut revoking=coralswarm_backend::test_app_state_owned_with_db_and_store(control_app.clone(),None);
        revoking.dek_cache.insert(&tenant_id,f.user.account_id,[7;32]);
        revoking.r2=Some(Arc::new(R2Client::new_with_endpoint_override("fixture","fixture-access","fixture-secret","fixture-bucket",&endpoint).await));
        let (denied,body)=tokio::time::timeout(std::time::Duration::from_secs(10),ingest_value(&Arc::new(revoking),&f,json!({"graph_id":f.graph,"source_type":"note","source_id":source_id,"content":"Retained capture revoked during upload.","timestamp":"2009-01-01T00:00:00Z","participants":[]}))).await.expect("revocation fence must not wait for its own held pool connection");
        assert_eq!(denied,StatusCode::FORBIDDEN,"{body}");
        // A refused DELETE leaves durable, abort-verified receipts that the
        // cleanup worker drains later; the response never claims deletion.
        assert_eq!(body["cleanup_status"],if cleanup_fails {"queued"}else{"completed"},"cleanup failure must not claim physical deletion");
        assert!(put_calls.load(Ordering::SeqCst)>=2);assert!(deletes.load(Ordering::SeqCst)>=2,"both source snapshot and raw chunk cleanup attempts are observed");
        let (pending,undeleted):(i64,i64)=sqlx::query_as("SELECT count(*) FILTER (WHERE state='pending' AND abort_verified),count(*) FILTER (WHERE state<>'deleted') FROM unattached_capture_uploads WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        if cleanup_fails {assert!(pending>=2,"refused deletes stay queued as verified receipts: {pending}");} else {assert_eq!(undeleted,0,"successful cleanup leaves no live receipt");}
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM source_items WHERE graph_id=$1 AND source_id=$2").bind(f.graph).bind(&source_id).fetch_one(&f.owner).await.unwrap(),0);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_id LIKE $2").bind(f.graph).bind(format!("{source_id}:%")).fetch_one(&f.owner).await.unwrap(),0);
        revoke_server.abort();
        }
        f.close().await;
        if let Some(c)=control_fixture {c.teardown().await;}
    }
}

/// A whole-ocean scope over a graph the size of a real member ocean. Every
/// node is supported by one permitted legacy atom that no capture chunk maps,
/// which is how production oceans look. The routes keep their own 5 s
/// statement timeout, so a plan that pays per-node permission calls or
/// correlated counts fails this with 503 rather than passing slowly.
#[tokio::test]
async fn whole_ocean_scope_over_twenty_thousand_legacy_nodes_resolves_within_the_request_timeout() {
    const LEGACY_NODES: i64 = 20_000;
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "whole_ocean").await;
        let atlas = node(&f, f.graph, "Atlas").await;
        let a = source(
            &f,
            f.graph,
            "slack:workspace:W1",
            origin("slack", "slack:workspace:W1", "grant-a", false),
            "permitted",
            "Atlas launch",
            "Atlas ready.",
            &[atlas],
            "Atlas",
        )
        .await;
        for sql in [
            "INSERT INTO nodes(node_id,graph_id,type,title,body) SELECT ('00000000-0000-4000-8000-'||lpad(to_hex(i),12,'0'))::uuid,$1,'entity','Legacy '||i,'{}' FROM generate_series(1,$2) i",
            "INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT ('00000000-0000-4000-9000-'||lpad(to_hex(i),12,'0'))::uuid,$1,'slack','legacy:'||i,'fixture','Legacy '||i,now() FROM generate_series(1,$2) i",
            "INSERT INTO node_atoms(node_id,atom_id) SELECT ('00000000-0000-4000-8000-'||lpad(to_hex(i),12,'0'))::uuid,('00000000-0000-4000-9000-'||lpad(to_hex(i),12,'0'))::uuid FROM generate_series(1,$2) i",
        ] {
            sqlx::query(sql)
                .bind(f.graph)
                .bind(LEGACY_NODES)
                .execute(&f.owner)
                .await
                .unwrap();
        }
        // One statement grew three tables from a handful of rows to 20k. Autoanalyze
        // has not run yet, and a plan built on empty-table statistics measures nothing
        // about production, where statistics track the data.
        sqlx::query("ANALYZE nodes, atoms, node_atoms")
            .execute(&f.owner)
            .await
            .unwrap();
        let whole = scope(f.graph, json!({}));
        let (status, resolved) = body(
            library_read::resolve_scope(
                f.tenant.clone(),
                Extension(f.user.clone()),
                Json(ScopeRequest {
                    scope: whole.clone(),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resolved}");
        assert_eq!(resolved, resolved_scope(&whole), "resolve-scope echoes the scope and counts nothing");

        // The whole-scope totals (`scope_sources`, `scope_atoms`, `scope_nodes`)
        // are the search reader's; this is where `scope_cte` is evaluated.
        let (status, v) = search(&f, whole.clone(), "", 5, None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["sources"]["total"], 1);
        assert_eq!(v["outcomes"]["total"], 1);
        assert_eq!(v["nodes"]["total"], LEGACY_NODES + 1);
        assert_eq!(v["nodes"]["items"][0]["node_id"], json!(atlas), "captured support ranks first");
        assert_eq!(v["nodes"]["items"][0]["title"], "Atlas");
        assert_eq!(v["nodes"]["items"][0]["contribution"]["unmapped_legacy_chunk_count"], 0);
        assert_eq!(v["nodes"]["items"][1]["contribution"]["unmapped_legacy_chunk_count"], 1);
        assert_eq!(v["nodes"]["items"][1]["wider_context"]["unmapped_legacy_chunk_count"], 1);
        assert_eq!(v["prominent_work"][0]["node_id"], json!(atlas));

        let (status, v) = search(&f, whole.clone(), "Legacy 20000", 5, None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["nodes"]["total"], 1);
        assert_eq!(v["nodes"]["items"][0]["title"], "Legacy 20000");
        assert_eq!(v["nodes"]["items"][0]["contribution"]["source_support_count"], 0);
        assert_eq!(v["nodes"]["items"][0]["contribution"]["unmapped_legacy_chunk_count"], 1);

        let legacy_node: Uuid = "00000000-0000-4000-8000-000000004e20".parse().unwrap();
        let legacy_scope = scope(f.graph, json!({"node_id": legacy_node}));
        let (status, resolved) = body(
            library_read::resolve_scope(
                f.tenant.clone(),
                Extension(f.user.clone()),
                Json(ScopeRequest {
                    scope: legacy_scope.clone(),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resolved}");
        assert_eq!(resolved, resolved_scope(&legacy_scope));
        let (status, v) = search(&f, legacy_scope, "", 5, None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["nodes"]["total"], 1);
        assert_eq!(v["sources"]["total"], 0, "a legacy-only node brings no captured source into scope");

        let atlas_scope = scope(f.graph, json!({"node_id": atlas}));
        let (status, resolved) = body(
            library_read::resolve_scope(
                f.tenant.clone(),
                Extension(f.user.clone()),
                Json(ScopeRequest {
                    scope: atlas_scope.clone(),
                }),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resolved}");
        assert_eq!(resolved, resolved_scope(&atlas_scope));
        assert_eq!(resolved["scope"]["graph_ids"], json!([f.graph]));
        let (status, v) = search(&f, atlas_scope, "", 5, None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["sources"]["total"], 1);
        assert_eq!(v["nodes"]["total"], 1);
        let (status, v) = search(&f, scope(f.graph, json!({"source_item_id": a.item})), "", 5, None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["nodes"]["total"], 1, "an exact item never widens into legacy-only nodes");
        f.close().await;
    }
}

#[tokio::test]
async fn node_outcomes_order_by_source_event_time_not_capture_order_across_pages_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "outcome_order").await;
        let atlas = node(&f, f.graph, "Atlas").await;
        // A backfill captures old events last; capture order must not decide what reads as latest.
        let rows = [
            ("Older event, captured last.", Some("2026-09-20T17:51:00Z"), "2026-10-13T00:36:00Z"),
            ("Latest event.", Some("2026-10-12T20:47:00Z"), "2026-10-13T00:35:00Z"),
            ("Same event time, captured earlier.", Some("2026-10-12T20:47:00Z"), "2026-10-13T00:34:00Z"),
            ("Unknown event time, newest capture.", None, "2026-10-14T00:00:00Z"),
        ];
        for (index, (text, occurred, captured)) in rows.iter().enumerate() {
            let s = source(&f, f.graph, &format!("slack:workspace:order{index}"), origin("slack", "slack:workspace:W1", "grant-1", false), "permitted", text, text, &[atlas], "Atlas").await;
            sqlx::query("UPDATE source_versions SET occurred_at=$2::timestamptz,captured_at=$3::timestamptz WHERE version_id=$1")
                .bind(s.version).bind(occurred).bind(captured).execute(&f.owner).await.unwrap();
        }
        let expected = ["Latest event.", "Same event time, captured earlier.", "Older event, captured last.", "Unknown event time, newest capture."];
        let page = |cursor: Option<String>, size: i64| {
            let f = &f;
            async move {
                body(library_read::search(f.tenant.clone(), Extension(f.user.clone()), Json(SearchRequest {
                    scope: scope(f.graph, json!({"node_id": atlas})),
                    query: String::new(),
                    page_size: Some(size),
                    sources_after: None,
                    outcomes_after: cursor,
                    nodes_after: None,
                })).await).await
            }
        };
        let statements = |v: &Value| v["outcomes"]["items"].as_array().unwrap().iter().map(|o| o["statement"].as_str().unwrap().to_string()).collect::<Vec<_>>();
        let (status, whole) = page(None, 20).await;
        assert_eq!(status, StatusCode::OK, "{whole}");
        assert_eq!(statements(&whole), expected);
        let mut paged = Vec::new();
        let mut cursor = None;
        for _ in 0..expected.len() {
            let (status, v) = page(cursor.take(), 1).await;
            assert_eq!(status, StatusCode::OK, "{v}");
            paged.extend(statements(&v));
            cursor = v["outcomes"]["next_cursor"].as_str().map(str::to_string);
        }
        assert_eq!(paged, expected, "keyset pages keep the same order with no gaps or repeats");
        assert!(cursor.is_none(), "the null-event outcome is the final page");
        f.close().await;
    }
}

/// Production hub nodes ("CoralSwarm": 607 supporting sources) timed out the
/// neighborhood read for every page size (COR-83). The fixture reproduces that
/// shape: a hub with hundreds of supported edges inside a graph whose node,
/// atom and edge tables are production-sized, so a plan that pays per-edge
/// lineage work before the page limit shows up as wall time here.
#[tokio::test]
async fn hub_node_neighborhood_pages_within_the_request_timeout_at_production_edge_counts() {
    const PEERS: i64 = 55_000;
    const LEGACY_ATOMS: i64 = 60_000;
    const SUPPORTED_EDGES: i64 = 600;
    const DERIVED_EDGES: i64 = 200;
    const BACKGROUND_EDGES: i64 = 20_000;
    const BUDGET_MS: u128 = 2_000;
    fn id(prefix: char) -> String {
        format!("('00000000-0000-4000-{prefix}000-'||lpad(to_hex(i),12,'0'))::uuid")
    }
    let (peer, legacy, item, version, chunk, semantic, supported, derived, background) = (
        id('8'), id('9'), id('a'), id('b'), id('c'), id('d'), id('e'), id('f'), id('7'),
    );
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "hub_neighborhood").await;
        let hub = node(&f, f.graph, "Hub").await;
        let slack = origin("slack", "slack:workspace:W1", "grant-a", false);
        let statements = [
            format!("INSERT INTO nodes(node_id,graph_id,type,title,body) SELECT {peer},$1,'entity','Peer '||i,'{{}}' FROM generate_series(1,{PEERS}) i"),
            format!("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT {legacy},$1,'slack','legacy:'||i,'fixture','Legacy '||i,now() FROM generate_series(1,{LEGACY_ATOMS}) i"),
            format!("INSERT INTO node_atoms(node_id,atom_id) SELECT {peer},{legacy} FROM generate_series(1,{PEERS}) i"),
            format!("INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin,access_state) SELECT {item},$1,'slack','slack:workspace:W1','msg-'||i,$3,'permitted' FROM generate_series(1,{SUPPORTED_EDGES}) i"),
            format!("INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) SELECT {version},$1,{item},'msg-'||i,'slack','slack:workspace:W1',$3,'Hub supports Peer '||i,jsonb_build_object('title','Launch '||i),'2026-01-01T00:00:00Z','2026-02-01T00:00:00Z' FROM generate_series(1,{SUPPORTED_EDGES}) i"),
            format!("UPDATE source_items s SET current_version_id=v.version_id FROM source_versions v WHERE v.graph_id=$1 AND v.source_item_id=s.source_item_id AND s.graph_id=$1 AND $2=$2 AND $3=$3"),
            format!("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT {chunk},$1,'slack','slack:workspace:W1:chunk:'||i,'fixture','Hub supports Peer '||i,now() FROM generate_series(1,{SUPPORTED_EDGES}) i"),
            format!("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) SELECT $1,{version},{chunk} FROM generate_series(1,{SUPPORTED_EDGES}) i"),
            format!("INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT {semantic},$1,{version},0,'fact','Hub supports Peer '||i,'source-atoms-v1','mock','[{{\"mention\":\"Hub\",\"type\":\"project\"}}]' FROM generate_series(1,{SUPPORTED_EDGES}) i"),
            format!("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,$2,{semantic} FROM generate_series(1,{SUPPORTED_EDGES}) i"),
            format!("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,{peer},{semantic} FROM generate_series(1,{SUPPORTED_EDGES}) i"),
            format!("INSERT INTO relationship_edge(edge_id,graph_id,src_node_id,dst_node_id,predicate,status,provenance) SELECT {supported},$1,$2,{peer},'supports','confirmed',jsonb_build_object('atom_ids',jsonb_build_array({chunk}::text)) FROM generate_series(1,{SUPPORTED_EDGES}) i"),
            format!("INSERT INTO relationship_edge(edge_id,graph_id,src_node_id,dst_node_id,predicate,status,provenance) SELECT {derived},$1,{peer},$2,'derived-support','confirmed',jsonb_build_object('source_edge_ids',jsonb_build_array({supported}::text)) FROM generate_series(1,{DERIVED_EDGES}) i"),
            format!("INSERT INTO relationship_edge(edge_id,graph_id,src_node_id,dst_node_id,predicate,status,provenance) SELECT {background},$1,{peer},('00000000-0000-4000-8000-'||lpad(to_hex(i%{PEERS}+1),12,'0'))::uuid,'mentions','confirmed',jsonb_build_object('atom_ids',jsonb_build_array({legacy}::text)) FROM generate_series(1,{BACKGROUND_EDGES}) i"),
        ];
        for sql in &statements {
            sqlx::query(sql).bind(f.graph).bind(hub).bind(&slack).execute(&f.owner).await.unwrap();
        }
        sqlx::query("ANALYZE nodes, atoms, node_atoms, source_items, source_versions, source_version_chunks, semantic_atoms, node_semantic_atoms, relationship_edge")
            .execute(&f.owner)
            .await
            .unwrap();
        let read = |node_id: Uuid, page_size: i64, after: Option<String>, source: Option<Uuid>| {
            let f = &f;
            async move {
                let started = std::time::Instant::now();
                let (status, v) = body(
                    library_read::neighborhood(
                        f.tenant.clone(),
                        Extension(f.user.clone()),
                        Json(NeighborhoodRequest {
                            scope: scope(f.graph, json!({"outcome_versions":"current","node_id":node_id,"source_item_id":source})),
                            node_id,
                            page_size: Some(page_size),
                            after,
                        }),
                    )
                    .await,
                )
                .await;
                let elapsed = started.elapsed().as_millis();
                eprintln!("neighborhood tenant={tenant} node={node_id} source={source:?} page_size={page_size} status={status} elapsed_ms={elapsed}");
                assert_eq!(status, StatusCode::OK, "{v}");
                assert!(elapsed < BUDGET_MS, "neighborhood took {elapsed} ms against a {BUDGET_MS} ms budget");
                v
            }
        };
        let first = read(hub, 20, None, None).await;
        let items = first["items"].as_array().unwrap();
        assert_eq!(items.len(), 20);
        assert_eq!(first["truncated"], true);
        for item in items {
            assert_eq!(item["predicate"], "derived-support", "newest edges page first: {item}");
            assert_eq!(item["support"].as_array().unwrap().len(), 1, "lineage resolves to the parent edge's one source: {item}");
            assert!(item["support"][0]["source_item_id"].is_string(), "{item}");
            assert_eq!(item["support"][0]["source_version_is_current"], true);
            assert_eq!(item["support_truncated"], false);
            assert_eq!(item["dst"]["title"], "Hub");
        }
        let second = read(hub, 20, first["next_cursor"].as_str().map(str::to_string), None).await;
        assert_eq!(second["items"].as_array().unwrap().len(), 20);
        assert_eq!(second["truncated"], true);
        assert!(second["items"][0]["edge_id"].as_str() < first["items"][19]["edge_id"].as_str(), "the cursor continues below the first page");
        let small = read(hub, 5, None, None).await;
        assert_eq!(small["items"].as_array().unwrap().len(), 5);
        assert_eq!(small["truncated"], true);
        // One source supports one hub edge and the derived edge built on it; every
        // other hub edge is walked and rejected for lack of in-scope support.
        let item_one: Uuid = "00000000-0000-4000-a000-000000000001".parse().unwrap();
        let narrowed = read(hub, 20, None, Some(item_one)).await;
        assert_eq!(narrowed["truncated"], false);
        let narrowed_edges: Vec<&str> = narrowed["items"].as_array().unwrap().iter().map(|i| i["edge_id"].as_str().unwrap()).collect();
        assert_eq!(narrowed_edges, ["00000000-0000-4000-f000-000000000001", "00000000-0000-4000-e000-000000000001"], "{narrowed}");
        for item in narrowed["items"].as_array().unwrap() {
            assert_eq!(item["support"], json!([{"source_item_id":item_one,"source_version_id":"00000000-0000-4000-b000-000000000001","source_version_is_current":true}]), "{item}");
        }
        let peer_one: Uuid = "00000000-0000-4000-8000-000000000001".parse().unwrap();
        let few = read(peer_one, 20, None, None).await;
        assert_eq!(few["truncated"], false);
        let few_support: Vec<(&str, &Value)> = few["items"].as_array().unwrap().iter().map(|i| (i["predicate"].as_str().unwrap(), &i["support"][0])).collect();
        assert_eq!(few_support.len(), 3, "{few}");
        assert_eq!(few_support[0].0, "derived-support");
        assert_eq!(few_support[0].1["source_item_id"], json!(item_one));
        assert_eq!(few_support[1].0, "supports");
        assert_eq!(few_support[1].1["source_item_id"], json!(item_one));
        assert_eq!(few_support[2], ("mentions", &json!({"kind":"legacy_chunk","graph_id":f.graph,"atom_id":"00000000-0000-4000-9000-000000000001"})), "an unmapped legacy atom is typed legacy support");
        f.close().await;
    }
}

/// `library_permitted_atoms(g, NULL)` means "every atom in the graph", so an
/// edge without atom provenance must never reach it with a NULL id list. The
/// second pass marks the parser STRICT, which turns NULL input into NULL output,
/// to prove the callers' COALESCE holds without relying on the parser's body.
#[tokio::test]
async fn edge_without_atom_provenance_is_denied_and_never_supported_by_every_atom() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "null_provenance").await;
        let a = node(&f, f.graph, "Atlas").await;
        let b = node(&f, f.graph, "Roadmap").await;
        let slack = origin("slack", "slack:workspace:W1", "grant-a", false);
        let support = source(&f, f.graph, "slack:workspace:W1", slack.clone(), "permitted", "Launch", "Atlas supports Roadmap.", &[a, b], "Atlas").await;
        source(&f, f.graph, "slack:workspace:W2", origin("slack", "slack:workspace:W2", "grant-a", false), "permitted", "Unrelated", "Unrelated permitted text.", &[], "Other").await;
        sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'slack','legacy:1','fixture','Unrelated legacy text.',now())")
            .bind(f.graph).execute(&f.owner).await.unwrap();
        let supported = edge(&f, f.graph, a, b, "supports", "confirmed", json!({"atom_ids":[support.atom]})).await;
        let missing = edge(&f, f.graph, a, b, "missing-atoms", "confirmed", json!({})).await;
        let null = edge(&f, f.graph, a, b, "null-atoms", "confirmed", json!({"atom_ids":null})).await;
        for strict in [false, true] {
            if strict {
                sqlx::query("ALTER FUNCTION library_provenance_ids(JSONB) STRICT").execute(&f.owner).await.unwrap();
                let parsed: Option<Vec<Uuid>> = sqlx::query_scalar("SELECT library_provenance_ids(NULL)").fetch_one(&f.owner).await.unwrap();
                assert_eq!(parsed, None, "STRICT parser yields NULL for NULL provenance");
            }
            for denied in [missing, null] {
                let permitted: bool = sqlx::query_scalar("SELECT library_edge_permitted($1,$2)").bind(f.graph).bind(denied).fetch_one(&f.owner).await.unwrap();
                assert!(!permitted, "edge {denied} with no atom ids is denied (tenant={tenant} strict={strict})");
            }
            let (status, v) = body(
                library_read::neighborhood(
                    f.tenant.clone(),
                    Extension(f.user.clone()),
                    Json(NeighborhoodRequest { scope: scope(f.graph, json!({"outcome_versions":"current","node_id":a})), node_id: a, page_size: Some(20), after: None }),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{v}");
            assert_eq!(
                v["items"].as_array().unwrap().iter().map(|i| (i["edge_id"].clone(), i["support"].clone())).collect::<Vec<_>>(),
                [(json!(supported), json!([{"source_item_id":support.item,"source_version_id":support.version,"source_version_is_current":true}]))],
                "only the supported edge appears, citing only its own source (tenant={tenant} strict={strict}): {v}"
            );
        }
        f.close().await;
    }
}

async fn disagreements(f: &Fixture, graph: Uuid, node: Uuid, page_size: Option<i64>, after: Option<String>) -> (StatusCode, String, Value) {
    let response = library_disagreements::disagreements(
        f.tenant.clone(),
        Extension(f.user.clone()),
        Json(DisagreementRequest { graph_id: graph, node_id: node, page_size, after }),
    )
    .await;
    assert_eq!(response.headers()["cache-control"], "no-store");
    let status = response.status();
    let raw = String::from_utf8(to_bytes(response.into_body(), 4_000_000).await.unwrap().to_vec()).unwrap();
    let value = serde_json::from_str(&raw).unwrap();
    (status, raw, value)
}
async fn disagree(f: &Fixture, a: Uuid, b: Uuid, recorded_at: &str, resolved: bool) {
    sqlx::query("INSERT INTO assertion_disagreements(graph_id,left_node_id,right_node_id,created_at,resolved_at) VALUES($1,LEAST($2::uuid,$3::uuid),GREATEST($2::uuid,$3::uuid),$4::timestamptz,CASE WHEN $5 THEN now() END)")
        .bind(f.graph).bind(a).bind(b).bind(recorded_at).bind(resolved).execute(&f.owner).await.unwrap();
}
fn other_ids(page: &Value) -> Vec<Value> {
    page["items"].as_array().unwrap().iter().map(|d| d["other"]["node_id"].clone()).collect()
}

#[tokio::test]
async fn disagreement_sides_show_permitted_evidence_times_and_only_count_a_restricted_side_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "disagreement_sides").await;
        let w1 = origin("slack", "slack:workspace:W1", "grant-1", false);
        let atlas = node(&f, f.graph, "Atlas").await;
        let beacon = node(&f, f.graph, "Beacon").await;
        let secret = node(&f, f.graph, "Secret title").await;
        let settled = node(&f, f.graph, "Settled").await;
        let lonely = node(&f, f.graph, "Lonely").await;
        let a = source(&f, f.graph, "slack:workspace:a", w1.clone(), "permitted", "Atlas plan", "Atlas ships in March.", &[atlas], "Atlas").await;
        let b = source(&f, f.graph, "slack:workspace:b", w1.clone(), "permitted", "Beacon plan", "Beacon says Atlas slips to May.", &[beacon], "Beacon").await;
        let c = source(&f, f.graph, "slack:workspace:c", w1.clone(), "restricted", "Secret title", "secret quote about Atlas", &[secret], "Secret title").await;
        source(&f, f.graph, "slack:workspace:d", w1.clone(), "permitted", "Settled plan", "Settled view.", &[settled], "Settled").await;
        source(&f, f.graph, "slack:workspace:e", w1.clone(), "permitted", "Lonely plan", "Lonely view.", &[lonely], "Lonely").await;
        disagree(&f, atlas, beacon, "2026-09-01T00:00:00Z", false).await;
        disagree(&f, atlas, secret, "2026-09-02T00:00:00Z", false).await;
        disagree(&f, atlas, settled, "2026-09-03T00:00:00Z", true).await;
        disagree(&f, lonely, secret, "2026-09-04T00:00:00Z", false).await;
        let (passage, observation): (Uuid, Uuid) = sqlx::query_as("SELECT p.passage_id,o.observation_id FROM semantic_atom_passages p JOIN source_version_observations o ON o.source_version_id=p.source_version_id WHERE p.semantic_atom_id=$1")
            .bind(b.semantic).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO temporal_evidence_records(graph_id,subject_node_id,state_key,source_item_id,source_version_id,semantic_atom_id,passage_id,observation_id,operation,value,valid_from,recording_method,recorded_by) VALUES($1,$2,'release.month',$3,$4,$5,$6,$7,'assert','may','2026-05-01T00:00:00Z','human_curated_v1',$8)")
            .bind(f.graph).bind(beacon).bind(b.item).bind(b.version).bind(b.semantic).bind(passage).bind(observation).bind(f.user.user_id).execute(&f.owner).await.unwrap();
        let first_known: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT min(known_at) FROM source_version_observations WHERE source_version_id=$1")
            .bind(a.version).fetch_one(&f.owner).await.unwrap();

        let (status, raw, v) = disagreements(&f, f.graph, atlas, None, None).await;
        assert_eq!(status, StatusCode::OK, "{raw}");
        for hidden in [secret.to_string(), "Secret title".into(), "secret quote".into(), c.item.to_string(), c.version.to_string(), settled.to_string()] {
            assert!(!raw.contains(&hidden), "{hidden} leaked: {raw}");
        }
        assert_eq!(other_ids(&v), [json!(beacon)]);
        assert_eq!(v["withheld"], json!({"count":1,"complete":true}));
        assert_eq!((v["next_cursor"].clone(), v["truncated"].clone()), (Value::Null, json!(false)));
        assert_eq!(v["items"][0]["recorded_at"], "2026-09-01T00:00:00Z");
        let anchor = &v["anchor"];
        assert_eq!((anchor["node_id"].clone(), anchor["title"].clone(), anchor["evidence_truncated"].clone()), (json!(atlas), json!("Atlas"), json!(false)));
        let own = &anchor["evidence"][0];
        assert_eq!(own["outcome"]["statement"], "Atlas ships in March.");
        assert_eq!(own["outcome"]["citations"][0]["quote"], "Atlas ships in March.");
        assert_eq!(own["outcome"]["citations"][0]["source_version_id"], json!(a.version));
        assert!(own["outcome"].get("_id").is_none() && own["outcome"].get("_at").is_none());
        assert_eq!(own["effective_time"], json!([]), "no temporal record means not recorded");
        let known: chrono::DateTime<chrono::Utc> = serde_json::from_value(own["knowledge_time"].clone()).unwrap();
        assert_eq!(known, first_known);
        assert_ne!(own["knowledge_time"], own["outcome"]["captured_at"]);
        let theirs = &v["items"][0]["other"]["evidence"][0];
        assert_eq!(theirs["outcome"]["statement"], "Beacon says Atlas slips to May.");
        assert_eq!(theirs["effective_time"].as_array().unwrap().len(), 1);
        let interval = &theirs["effective_time"][0];
        assert_eq!((interval["state_key"].clone(), interval["operation"].clone(), interval["value"].clone(), interval["passage_id"].clone()), (json!("release.month"), json!("assert"), json!("may"), json!(passage)));
        assert_eq!((interval["valid_from"].clone(), interval["valid_until"].clone()), (json!("2026-05-01T00:00:00Z"), Value::Null));
        assert_eq!(theirs["effective_time_truncated"], false);

        let (_, _, only_withheld) = disagreements(&f, f.graph, lonely, None, None).await;
        assert_eq!(other_ids(&only_withheld), Vec::<Value>::new());
        assert_eq!(only_withheld["withheld"], json!({"count":1,"complete":true}));
        let badge = |v: &Value, id: Uuid| v["nodes"]["items"].as_array().unwrap().iter().find(|n| n["node_id"] == json!(id)).map(|n| (n["unresolved_disagreement"].clone(), n["current_understanding_status"].clone())).unwrap();
        let (_, lonely_card) = search(&f, scope(f.graph, json!({"node_id":lonely})), "", 20, None).await;
        assert_eq!(badge(&lonely_card, lonely), (json!(true), json!("unresolved_disagreement")), "a withheld-only disagreement still shows the badge");
        assert!(!lonely_card.to_string().contains("Secret title"));
        let (_, _, resolved_only) = disagreements(&f, f.graph, settled, None, None).await;
        assert_eq!((other_ids(&resolved_only), resolved_only["withheld"].clone()), (Vec::<Value>::new(), json!({"count":0,"complete":true})));
        let (_, settled_card) = search(&f, scope(f.graph, json!({"node_id":settled})), "", 20, None).await;
        assert_eq!(badge(&settled_card, settled), (json!(false), json!("evidence_only")));

        sqlx::query("DELETE FROM source_version_observations WHERE source_version_id=$1").bind(a.version).execute(&f.owner).await.unwrap();
        let (_, _, unobserved) = disagreements(&f, f.graph, atlas, None, None).await;
        assert_eq!(unobserved["anchor"]["evidence"][0]["knowledge_time"], Value::Null, "absent knowledge time never falls back to a capture date");

        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(c.item).execute(&f.owner).await.unwrap();
        let (_, _, readable) = disagreements(&f, f.graph, atlas, None, None).await;
        assert_eq!(other_ids(&readable), [json!(secret), json!(beacon)]);
        assert_eq!(readable["items"][0]["other"]["title"], "Secret title");
        assert_eq!(readable["withheld"], json!({"count":0,"complete":true}));
        f.close().await;
    }
}

#[tokio::test]
async fn disagreement_pages_walk_permitted_sides_without_revealing_withheld_positions_on_both_planes() {
    use axum::{body::Body, extract::FromRequest, http::Request};
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "disagreement_pages").await;
        let w1 = origin("slack", "slack:workspace:W1", "grant-1", false);
        let anchor = node(&f, f.graph, "Anchor").await;
        source(&f, f.graph, "slack:workspace:anchor", w1.clone(), "permitted", "Anchor", "Anchor view.", &[anchor], "Anchor").await;
        let mut sides = Vec::new();
        for (name, access, at) in [
            ("Permitted one", "permitted", "2026-09-05T00:00:00Z"),
            ("Restricted one", "restricted", "2026-09-04T00:00:00Z"),
            ("Permitted two", "permitted", "2026-09-03T00:00:00Z"),
            ("Restricted two", "restricted", "2026-09-02T00:00:00Z"),
            ("Permitted three", "permitted", "2026-09-02T00:00:00Z"),
        ] {
            let id = node(&f, f.graph, name).await;
            source(&f, f.graph, &format!("slack:workspace:{name}"), w1.clone(), access, name, &format!("{name} view."), &[id], name).await;
            disagree(&f, anchor, id, at, false).await;
            sides.push((id, access));
        }
        let restricted: Vec<Uuid> = sides.iter().filter(|(_, a)| *a == "restricted").map(|(id, _)| *id).collect();
        let permitted_order = [json!(sides[0].0), json!(sides[2].0), json!(sides[4].0)];

        let mut walked = Vec::new();
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let (status, raw, v) = disagreements(&f, f.graph, anchor, Some(1), cursor.take()).await;
            assert_eq!(status, StatusCode::OK, "{raw}");
            pages += 1;
            assert_eq!(v["withheld"], json!({"count":2,"complete":true}), "the withheld count is the same on every page");
            assert!(restricted.iter().all(|id| !raw.contains(&id.to_string())));
            walked.extend(other_ids(&v));
            let Some(next) = v["next_cursor"].as_str() else { break };
            let decoded = String::from_utf8(URL_SAFE_NO_PAD.decode(next).unwrap()).unwrap();
            assert!(restricted.iter().all(|id| !decoded.contains(&id.to_string())), "{decoded}");
            assert!(decoded.contains(&walked.last().unwrap().as_str().unwrap().to_string()));
            cursor = Some(next.to_string());
        }
        assert_eq!((pages, walked.as_slice()), (3, permitted_order.as_slice()));

        let (_, _, first) = disagreements(&f, f.graph, anchor, Some(1), None).await;
        let next = first["next_cursor"].as_str().unwrap().to_string();
        let (replayed, _, _) = disagreements(&f, f.graph, sides[0].0, Some(1), Some(next.clone())).await;
        assert_eq!(replayed, StatusCode::BAD_REQUEST, "a cursor is bound to its anchor");
        let mut tampered: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&next).unwrap()).unwrap();
        tampered["digest"] = json!("0".repeat(64));
        let tampered = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&tampered).unwrap());
        for bad in [tampered, "not-a-cursor".into()] {
            assert_eq!(disagreements(&f, f.graph, anchor, Some(1), Some(bad)).await.0, StatusCode::BAD_REQUEST);
        }
        for size in [0, 11] {
            assert_eq!(disagreements(&f, f.graph, anchor, Some(size), None).await.0, StatusCode::BAD_REQUEST);
        }
        let foreign = node(&f, f.foreign_graph, "Foreign").await;
        for (graph, id) in [(f.graph, restricted[0]), (f.graph, Uuid::new_v4()), (f.graph, foreign)] {
            let (status, raw, _) = disagreements(&f, graph, id, None, None).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{raw}");
            assert!(!raw.contains("Restricted"));
        }
        let request = Request::builder()
            .header("content-type", "application/json")
            .body(Body::from(json!({"graph_id":f.graph,"node_id":anchor,"scope":{"graph_ids":[f.graph]}}).to_string()))
            .unwrap();
        match library_read::LibraryJson::<DisagreementRequest>::from_request(request, &()).await {
            Err(response) => assert_eq!(body(response).await.0, StatusCode::BAD_REQUEST),
            Ok(_) => panic!("unknown field admitted"),
        }
        f.close().await;
    }
}

async fn outcome_on(f: &Fixture, version: Uuid, text: &str, nodes: &[Uuid]) -> Uuid {
    let semantic:Uuid=sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) VALUES($1,$2,0,'fact',$3,'source-atoms-v1','mock','[]') RETURNING semantic_atom_id")
        .bind(f.graph).bind(version).bind(text).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5)")
        .bind(f.graph).bind(version).bind(semantic).bind(text.chars().count() as i32).bind(text).execute(&f.owner).await.unwrap();
    for n in nodes {
        sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
            .bind(f.graph).bind(n).bind(semantic).execute(&f.owner).await.unwrap();
    }
    semantic
}
/// `source` reuses one provider id per namespace; session fixtures need many items per namespace.
async fn session_source(f: &Fixture, namespace: &str, id: &str, origin: Value, access: &str, nodes: &[Uuid]) -> Source {
    let item:Uuid=sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'agent_session',$2,$3,$4,$5) RETURNING source_item_id")
        .bind(f.graph).bind(namespace).bind(id).bind(&origin).bind(access).fetch_one(&f.owner).await.unwrap();
    let version:Uuid=sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) VALUES($1,$2,$3,'agent_session',$4,$5,'Session statement.',jsonb_build_object('title',$3::text),'2026-01-01T00:00:00Z','2026-02-01T00:00:00Z') RETURNING version_id")
        .bind(f.graph).bind(item).bind(id).bind(namespace).bind(&origin).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&f.owner).await.unwrap();
    let atom:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,'slack',$2,'fixture','Session statement.',now()) RETURNING atom_id")
        .bind(f.graph).bind(format!("{namespace}:{id}:chunk")).fetch_one(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(f.graph).bind(version).bind(atom).execute(&f.owner).await.unwrap();
    let semantic = outcome_on(f, version, "Session statement.", nodes).await;
    Source { item, version, atom, semantic }
}
async fn report_session(f: &Fixture, version: Uuid, session_id: &str) {
    sqlx::query("UPDATE source_versions SET source_metadata=source_metadata||jsonb_build_object('session_id',$2::text) WHERE version_id=$1")
        .bind(version).bind(session_id).execute(&f.owner).await.unwrap();
}
async fn legacy_session_chunk(f: &Fixture, atom: Uuid, prefix: &str) {
    sqlx::query("UPDATE atoms SET source_type='agent_session',source_id=$2 WHERE atom_id=$1")
        .bind(atom).bind(format!("cc:{prefix}:0")).execute(&f.owner).await.unwrap();
}

/// Expected session key per version, written out by hand rather than read back
/// from the resolver function, so a drift in either side fails.
struct SessionCorpus {
    expected: Vec<(Uuid, Option<(&'static str, &'static str)>)>,
    reported_item: Uuid,
    reported_old: Uuid,
    node: Uuid,
}
async fn session_corpus(f: &Fixture) -> SessionCorpus {
    let node = node(f, f.graph, "Session work").await;
    let plain = |ns: &'static str, id: &'static str| session_source(f, ns, id, origin("capture", ns, "fixture-grant", false), "permitted", &[]);
    context_session(f, "ctx", None, "ctx").await;
    let captured = context_capture(f, "history:A", "ctx-capture", json!({"revision":1,"session_id":"ctx","session_kind":"agent","root_session_id":"ctx"})).await;
    outcome_on(f, captured.version_id, "Captured context outcome.", &[node]).await;
    let reported = plain("history:A", "Reported").await;
    report_session(f, reported.version, "meta").await;
    context_session(f, "legacy", None, "legacy").await;
    let legacy = plain("history:L", "Legacy").await;
    legacy_session_chunk(f, legacy.atom, "legacy").await;
    context_session(f, "amb", None, "amb").await;
    context_session(f, "amb:detail", None, "amb:detail").await;
    let ambiguous = plain("history:L", "Ambiguous").await;
    legacy_session_chunk(f, ambiguous.atom, "amb:detail").await;
    let unrelated = plain("slack:workspace:W1", "Not a session").await;
    let dup_a = plain("history:A", "Dup A").await;
    let dup_b = plain("history:B", "Dup B").await;
    report_session(f, dup_a.version, "dup").await;
    report_session(f, dup_b.version, "dup").await;
    for s in [&reported, &legacy, &ambiguous, &unrelated, &dup_a, &dup_b] {
        sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
            .bind(f.graph).bind(node).bind(s.semantic).execute(&f.owner).await.unwrap();
    }
    // A newer reported version of the same item names a different session.
    let reported_new: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata||'{\"session_id\":\"meta-new\"}',occurred_at,captured_at+interval '1 day' FROM source_versions WHERE version_id=$1 RETURNING version_id")
        .bind(reported.version).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(reported.item).bind(reported_new).execute(&f.owner).await.unwrap();
    SessionCorpus {
        expected: vec![
            (captured.version_id, Some(("history:A", "ctx"))),
            (reported.version, Some(("history:A", "meta"))),
            (reported_new, Some(("history:A", "meta-new"))),
            (legacy.version, Some(("history:L", "legacy"))),
            (ambiguous.version, None),
            (unrelated.version, None),
            (dup_a.version, Some(("history:A", "dup"))),
            (dup_b.version, Some(("history:B", "dup"))),
        ],
        reported_item: reported.item,
        reported_old: reported.version,
        node,
    }
}
impl SessionCorpus {
    fn key(&self, g: Uuid, version: &Value) -> Value {
        let version: Uuid = version.as_str().unwrap().parse().unwrap();
        match self.expected.iter().find(|(v, _)| *v == version).unwrap_or_else(|| panic!("unexpected version {version}")).1 {
            Some((ns, id)) => json!({"graph_id":g,"provider_namespace":ns,"session_id":id}),
            None => Value::Null,
        }
    }
}
/// The history page for `key` must be 200, name `key`, and list `version`.
async fn assert_opens(f: &Fixture, key: &Value, version: &Value) {
    let (status, page) = captured_history(f, scope(f.graph, json!({})), key["session_id"].as_str(), key["provider_namespace"].as_str(), false, 50, None).await;
    assert_eq!(status, StatusCode::OK, "{key} must open its session: {page}");
    assert_eq!(&page["session"], key);
    assert!(page["versions"].as_array().unwrap().iter().any(|v| &v["source_version_id"] == version), "{key} history lists {version}");
}

#[tokio::test]
async fn cor94_outcome_and_source_session_keys_are_the_resolver_attribution_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "cor94_keys").await;
        let c = session_corpus(&f).await;
        let (status, page) = search(&f, scope(f.graph, json!({})), "", 50, None).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        let outcomes = page["outcomes"]["items"].as_array().unwrap();
        assert_eq!(outcomes.len(), 7, "one outcome per fixture version except the newer reported one");
        for o in outcomes {
            let key = c.key(f.graph, &o["source_version_id"]);
            assert_eq!(o["session"], key, "outcome {}", o["source_version_id"]);
            if !key.is_null() {
                assert_opens(&f, &key, &o["source_version_id"]).await;
            }
        }
        let sources = page["sources"]["items"].as_array().unwrap();
        assert_eq!(sources.len(), 7);
        for s in sources {
            let key = c.key(f.graph, &s["selected_version_id"]);
            assert_eq!(s["session"], key, "source {}", s["source_item_id"]);
            if !key.is_null() {
                assert_opens(&f, &key, &s["selected_version_id"]).await;
            }
        }
        assert!(sources.iter().any(|s| s["session"]["session_id"] == "meta-new"), "an unscoped source shows its current version's session");
        let (status, old) = search(&f, scope(f.graph, json!({"source_item_id":c.reported_item,"source_version_id":c.reported_old})), "", 50, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(old["sources"]["items"][0]["session"], json!({"graph_id":f.graph,"provider_namespace":"history:A","session_id":"meta"}), "an explicit older version links its own session");
        let (_, at_node) = search(&f, scope(f.graph, json!({"node_id":c.node})), "", 50, None).await;
        assert_eq!(at_node["outcomes"]["items"].as_array().unwrap().len(), 7);
        for o in at_node["outcomes"]["items"].as_array().unwrap() {
            assert_eq!(o["session"], c.key(f.graph, &o["source_version_id"]));
        }
        f.close().await;
    }
}

#[tokio::test]
async fn cor94_history_outcomes_carry_the_same_key_as_their_version() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "cor94_history").await;
        let c = session_corpus(&f).await;
        let (status, page) = captured_history(&f, scope(f.graph, json!({})), None, None, false, 50, None).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        let mut checked = 0;
        for v in page["versions"].as_array().unwrap() {
            let expected = match v["context"]["session_id"].as_str() {
                Some(id) => json!({"graph_id":v["graph_id"],"provider_namespace":v["provider_namespace"],"session_id":id}),
                None => Value::Null,
            };
            assert_eq!(expected, c.key(f.graph, &v["source_version_id"]), "version {}", v["source_version_id"]);
            for o in v["outcomes"]["items"].as_array().unwrap() {
                assert_eq!(o["session"], expected, "outcome in version {}", v["source_version_id"]);
                checked += 1;
            }
        }
        assert_eq!(checked, 7);
        f.close().await;
    }
}

fn plan_names_function(plan: &Value, name: &str) -> bool {
    match plan {
        Value::Object(o) => o.get("Function Name").is_some_and(|n| n == name) || o.values().any(|v| plan_names_function(v, name)),
        Value::Array(a) => a.iter().any(|v| plan_names_function(v, name)),
        _ => false,
    }
}

/// The per-row session lookup is cheap only while Postgres inlines
/// `library_session_versions`; a function scan would attribute the whole graph per row.
#[tokio::test]
async fn cor94_session_key_lookup_inlines_the_resolver_function() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "cor94_inline").await;
        let c = session_corpus(&f).await;
        let explain = |s: LibraryScope| {
            let f = &f;
            async move {
                let mut tx = f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
                let sql = format!("EXPLAIN (FORMAT JSON) {}", library_read::search_sql_for_explain(&s));
                let plan: Value = sqlx::query_scalar(&sql)
                    .bind(&s.graph_ids).bind(json!({})).bind(s.source_item_id).bind(s.source_version_id).bind(s.node_id).bind(None::<String>)
                    .bind(None::<String>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>)
                    .bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(51_i64).bind(None::<chrono::DateTime<chrono::Utc>>)
                    .fetch_one(&mut *tx).await.unwrap();
                tx.rollback().await.unwrap();
                plan
            }
        };
        let mut node_scope = scope(f.graph, json!({"node_id":c.node}));
        node_scope.normalize().unwrap();
        assert!(!plan_names_function(&explain(node_scope.clone()).await, "library_session_versions"), "session keys must not scan the resolver function");
        let mut tx = f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {}", library_read::node_sessions_sql_for_explain(&node_scope)))
            .bind(&node_scope.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(node_scope.node_id).bind(None::<String>)
            .bind(None::<Uuid>).bind(None::<chrono::DateTime<chrono::Utc>>).bind(21_i64)
            .fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        assert!(!plan_names_function(&plan, "library_session_versions"), "node sessions must not scan the resolver function");
        sqlx::query("ALTER FUNCTION library_session_versions(uuid) STRICT").execute(&f.owner).await.unwrap();
        assert!(plan_names_function(&explain(node_scope).await, "library_session_versions"), "control: a STRICT resolver cannot inline and the check sees its scan");
        sqlx::query("ALTER FUNCTION library_session_versions(uuid) CALLED ON NULL INPUT").execute(&f.owner).await.unwrap();
        f.close().await;
    }
}

async fn node_sessions(f: &Fixture, s: LibraryScope, node_id: Uuid, page_size: i64, after: Option<String>) -> (StatusCode, Value) {
    body(
        library_read::node_sessions(
            f.tenant.clone(),
            Extension(f.user.clone()),
            Json(NeighborhoodRequest { scope: s, node_id, page_size: Some(page_size), after }),
        )
        .await,
    )
    .await
}
async fn captured_at(f: &Fixture, version: Uuid, at: &str) {
    sqlx::query("UPDATE source_versions SET captured_at=$2::timestamptz WHERE version_id=$1")
        .bind(version).bind(at).execute(&f.owner).await.unwrap();
}

#[tokio::test]
async fn cor94_node_sessions_list_real_support_scoped_and_paged_on_both_planes() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "cor94_node_sessions").await;
        let n = node(&f, f.graph, "Supported work").await;
        let plain = |ns: &'static str| origin("capture", ns, "fixture-grant", false);
        let s1_old = session_source(&f, "history:A", "s1-old", plain("history:A"), "permitted", &[n]).await;
        let s1_new = session_source(&f, "history:A", "s1-new", plain("history:A"), "permitted", &[n]).await;
        let s2_a = session_source(&f, "history:A", "s2", plain("history:A"), "permitted", &[n]).await;
        let mut meeting = plain("history:B");
        meeting["content_kind"] = json!("meeting");
        let s2_b = session_source(&f, "history:B", "s2", meeting, "permitted", &[n]).await;
        context_session(&f, "legacy", None, "legacy").await;
        let legacy = session_source(&f, "history:L", "legacy", plain("history:L"), "permitted", &[]).await;
        legacy_session_chunk(&f, legacy.atom, "legacy").await;
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(n).bind(legacy.atom).execute(&f.owner).await.unwrap();
        let elsewhere = session_source(&f, "history:A", "elsewhere", plain("history:A"), "permitted", &[]).await;
        let revoked = session_source(&f, "history:A", "revoked", plain("history:A"), "restricted", &[n]).await;
        session_source(&f, "history:A", "no-session", plain("history:A"), "permitted", &[n]).await;
        for (s, id, at) in [(&s1_old, "s1", "2026-03-01T00:00:00Z"), (&s1_new, "s1", "2026-03-05T00:00:00Z"), (&s2_a, "s2", "2026-03-04T00:00:00Z"), (&s2_b, "s2", "2026-03-03T00:00:00Z"), (&elsewhere, "elsewhere", "2026-03-09T00:00:00Z"), (&revoked, "revoked", "2026-03-09T00:00:00Z")] {
            report_session(&f, s.version, id).await;
            captured_at(&f, s.version, at).await;
        }
        captured_at(&f, legacy.version, "2026-03-02T00:00:00Z").await;
        let all = || scope(f.graph, json!({}));
        let expected = [("history:A", "s1", 2, s1_new.version), ("history:A", "s2", 1, s2_a.version), ("history:B", "s2", 1, s2_b.version), ("history:L", "legacy", 1, legacy.version)];
        let mut walked = Vec::new();
        let mut after = None;
        loop {
            let (status, page) = node_sessions(&f, all(), n, 1, after).await;
            assert_eq!(status, StatusCode::OK, "{page}");
            assert_eq!(page["total"], 4);
            assert_eq!(page["node_id"], json!(n));
            let items = page["items"].as_array().unwrap();
            assert_eq!(items.len(), 1);
            walked.push(items[0].clone());
            after = page["next_cursor"].as_str().map(str::to_string);
            assert_eq!(page["truncated"], after.is_some());
            if after.is_none() { break; }
        }
        assert_eq!(walked.len(), expected.len());
        for (row, (ns, id, count, latest)) in walked.iter().zip(expected) {
            assert_eq!((row["graph_id"].clone(), row["provider_namespace"].as_str().unwrap(), row["session_id"].as_str().unwrap(), row["version_count"].as_i64().unwrap(), row["latest_version_id"].clone()), (json!(f.graph), ns, id, count, json!(latest)), "{row}");
            let key = json!({"graph_id":f.graph,"provider_namespace":ns,"session_id":id});
            assert_opens(&f, &key, &row["latest_version_id"]).await;
        }
        assert_eq!(walked[0]["last_captured_at"].as_str().map(|t| t.starts_with("2026-03-05")), Some(true), "{}", walked[0]);
        let (_, whole) = node_sessions(&f, all(), n, 50, None).await;
        assert_eq!(whole["items"].as_array().unwrap(), &walked);
        assert_eq!(whole["truncated"], false);
        assert_eq!(whole["next_cursor"], Value::Null);
        let (_, exact) = node_sessions(&f, scope(f.graph, json!({"source_item_id":s2_b.item,"source_version_id":s2_b.version})), n, 20, None).await;
        assert_eq!(exact["items"].as_array().unwrap().iter().map(|r| (r["provider_namespace"].as_str().unwrap(), r["session_id"].as_str().unwrap())).collect::<Vec<_>>(), [("history:B", "s2")]);
        let (_, meetings) = node_sessions(&f, scope(f.graph, json!({"collection":"meetings"})), n, 20, None).await;
        assert_eq!(meetings["total"], 1, "{meetings}");
        assert_eq!(meetings["items"][0]["provider_namespace"], "history:B");
        let (_, first) = node_sessions(&f, all(), n, 1, None).await;
        let cursor = first["next_cursor"].as_str().map(str::to_string);
        let other = node(&f, f.graph, "Other").await;
        session_source(&f, "history:A", "other", plain("history:A"), "permitted", &[other]).await;
        assert_eq!(node_sessions(&f, all(), other, 1, cursor.clone()).await.0, StatusCode::BAD_REQUEST, "a cursor is bound to its node");
        assert_eq!(node_sessions(&f, scope(f.graph, json!({"collection":"meetings"})), n, 1, cursor).await.0, StatusCode::BAD_REQUEST, "a cursor is bound to its scope");
        for size in [0, 51] {
            assert_eq!(node_sessions(&f, all(), n, size, None).await.0, StatusCode::BAD_REQUEST);
        }
        assert_eq!(node_sessions(&f, scope(f.graph, json!({"node_id":other})), n, 20, None).await.0, StatusCode::BAD_REQUEST);
        let foreign = node(&f, f.foreign_graph, "Foreign").await;
        assert_eq!(node_sessions(&f, all(), foreign, 20, None).await.0, StatusCode::NOT_FOUND);
        f.close().await;
    }
}

#[tokio::test]
async fn hub_node_sessions_page_within_the_request_timeout() {
    const PLAIN: i64 = 5_000;
    const SESSION_VERSIONS: i64 = 500;
    const SESSIONS: i64 = 50;
    const BUDGET_MS: u128 = 2_000;
    let id = |prefix: char| format!("('00000000-0000-4000-{prefix}000-'||lpad(to_hex(i),12,'0'))::uuid");
    let (item, version, semantic) = (id('a'), id('b'), id('d'));
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "cor94_hub_sessions").await;
        let hub = node(&f, f.graph, "Hub").await;
        let capture = origin("capture", "history:A", "grant-a", false);
        let total = PLAIN + SESSION_VERSIONS;
        let statements = [
            format!("INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin,access_state) SELECT {item},$1,'agent_session','history:A','item-'||i,$3,'permitted' FROM generate_series(1,{total}) i"),
            format!("INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) SELECT {version},$1,{item},'item-'||i,'agent_session','history:A',$3,'Hub statement '||i,CASE WHEN i<={SESSION_VERSIONS} THEN jsonb_build_object('session_id','hub-'||(i%{SESSIONS}),'session_title','Hub session '||(i%{SESSIONS})) ELSE jsonb_build_object('title','Plain '||i) END,'2026-01-01T00:00:00Z','2026-02-01T00:00:00Z'::timestamptz+i*interval '1 minute' FROM generate_series(1,{total}) i"),
            format!("UPDATE source_items s SET current_version_id=v.version_id FROM source_versions v WHERE v.graph_id=$1 AND v.source_item_id=s.source_item_id AND s.graph_id=$1 AND $2=$2 AND $3=$3"),
            format!("INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) SELECT {semantic},$1,{version},0,'fact','Hub statement '||i,'source-atoms-v1','mock','[]' FROM generate_series(1,{total}) i"),
            format!("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) SELECT $1,$2,{semantic} FROM generate_series(1,{total}) i"),
        ];
        for sql in &statements {
            sqlx::query(sql).bind(f.graph).bind(hub).bind(&capture).execute(&f.owner).await.unwrap();
        }
        sqlx::query("ANALYZE source_items, source_versions, semantic_atoms, node_semantic_atoms").execute(&f.owner).await.unwrap();
        let mut after = None;
        let mut seen = 0;
        for page_number in 0.. {
            let started = std::time::Instant::now();
            let (status, page) = node_sessions(&f, scope(f.graph, json!({"outcome_versions":"current","node_id":hub})), hub, 20, after).await;
            let elapsed = started.elapsed().as_millis();
            eprintln!("node_sessions tenant={tenant} page={page_number} status={status} elapsed_ms={elapsed}");
            assert_eq!(status, StatusCode::OK, "{page}");
            assert!(elapsed < BUDGET_MS, "node sessions took {elapsed} ms against a {BUDGET_MS} ms budget");
            assert_eq!(page["total"], SESSIONS);
            seen += page["items"].as_array().unwrap().len() as i64;
            assert!(page["items"].as_array().unwrap().iter().all(|r| r["version_count"] == SESSION_VERSIONS / SESSIONS), "{page}");
            after = page["next_cursor"].as_str().map(str::to_string);
            if after.is_none() { break; }
        }
        assert_eq!(seen, SESSIONS);
        f.close().await;
    }
}

/// Positional chunk atoms are inserted ON CONFLICT DO NOTHING and only newly
/// inserted ids get a source_version_chunks row, so an atom of a captured
/// source item can stay `source_evidence_bound=false`. Such an atom resolves
/// to its item by (graph, source_type, source_id minus `:chunk:<n>`) and must
/// follow that item's access state. Atoms with no item keep today's behavior.
#[tokio::test]
async fn unbound_atom_of_revoked_source_item_is_not_permitted_on_both_planes_and_roles() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "unbound_source_gate").await;
        let ns = "slack:workspace:W1";
        let slack = origin("slack", ns, "grant-a", false);
        let mut items = Vec::new();
        for base in ["C1/111.222", "C1/333.444"] {
            let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack',$2,$3,$4,'permitted') RETURNING source_item_id")
                .bind(f.graph).bind(ns).bind(base).bind(&slack).fetch_one(&f.owner).await.unwrap();
            let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) VALUES($1,$2,$3,'slack',$4,$5,'Retained text.','{}','2026-01-01T00:00:00Z','2026-02-01T00:00:00Z') RETURNING version_id")
                .bind(f.graph).bind(item).bind(base).bind(ns).bind(&slack).fetch_one(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&f.owner).await.unwrap();
            items.push((item, version));
        }
        let atom = |source_type: &'static str, source_id: &'static str| {
            let owner = f.owner.clone();
            let graph = f.graph;
            async move {
                sqlx::query_scalar::<_, Uuid>("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,$2,$3,'fixture','Fixture text.',now()) RETURNING atom_id")
                    .bind(graph).bind(source_type).bind(source_id).fetch_one(&owner).await.unwrap()
            }
        };
        // Item one: chunk 0 is bound, chunk 1 kept its positional id and was
        // never linked (the ON CONFLICT path). Item two stays permitted.
        let bound = atom("slack", "C1/111.222:chunk:0").await;
        sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
            .bind(f.graph).bind(items[0].1).bind(bound).execute(&f.owner).await.unwrap();
        let unbound_revoked = atom("slack", "C1/111.222:chunk:1").await;
        let unbound_active = atom("slack", "C1/333.444:chunk:0").await;
        // No source item at all: a note, an MCP capture and a legacy Slack row
        // that predates source items. Same base under another type is not the item.
        let note = atom("note", "note-1").await;
        let mcp = atom("mcp", "mcp:ctx-1").await;
        let legacy = atom("slack", "C9/999.000:chunk:0").await;
        let other_type = atom("note", "C1/111.222:chunk:2").await;
        let unbound: bool = sqlx::query_scalar("SELECT NOT source_evidence_bound FROM atoms WHERE atom_id=$1").bind(unbound_revoked).fetch_one(&f.owner).await.unwrap();
        assert!(unbound, "fixture: the positional sibling is unbound");
        let node_of_revoked = node(&f, f.graph, "Revoked-only").await;
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node_of_revoked).bind(unbound_revoked).execute(&f.owner).await.unwrap();

        let everyone = [bound, unbound_revoked, unbound_active, note, mcp, legacy, other_type];
        // Restriction durably retracts the item's bound and unbound evidence
        // alike (0162), so re-permitting brings neither back.
        for (state, hidden) in [("permitted", vec![]), ("restricted", vec![bound, unbound_revoked]), ("unknown", vec![bound, unbound_revoked]), ("permitted", vec![bound, unbound_revoked])] {
            sqlx::query("UPDATE source_items SET access_state=$2 WHERE source_item_id=$1").bind(items[0].0).bind(state).execute(&f.owner).await.unwrap();
            let expected: std::collections::BTreeSet<Uuid> = everyone.iter().copied().filter(|a| !hidden.contains(a)).collect();
            for role in ["owner", "app"] {
                let mut conn = if role == "owner" {
                    f.owner.begin().await.unwrap()
                } else {
                    f.tenant.begin_member_scoped(f.user.account_id).await.unwrap()
                };
                // The consumer probe seeds its own atom; compare this fixture's atoms only.
                let all: std::collections::BTreeSet<Uuid> = sqlx::query_scalar::<_, Uuid>("SELECT atom_id FROM library_permitted_atoms($1,NULL) WHERE atom_id=ANY($2)")
                    .bind(f.graph).bind(&everyone[..]).fetch_all(&mut *conn).await.unwrap().into_iter().collect();
                assert_eq!(all, expected, "whole-graph set (tenant={tenant} role={role} state={state})");
                let listed: std::collections::BTreeSet<Uuid> = sqlx::query_scalar::<_, Uuid>("SELECT atom_id FROM library_permitted_atoms($1,$2)")
                    .bind(f.graph).bind(&everyone[..]).fetch_all(&mut *conn).await.unwrap().into_iter().collect();
                assert_eq!(listed, expected, "id-list set (tenant={tenant} role={role} state={state})");
                for a in everyone {
                    let permitted: bool = sqlx::query_scalar("SELECT library_atom_permitted($1,$2)").bind(f.graph).bind(a).fetch_one(&mut *conn).await.unwrap();
                    assert_eq!(permitted, expected.contains(&a), "scalar predicate for {a} (tenant={tenant} role={role} state={state})");
                }
                let node_ok: bool = sqlx::query_scalar("SELECT library_node_permitted($1,$2)").bind(f.graph).bind(node_of_revoked).fetch_one(&mut *conn).await.unwrap();
                assert_eq!(node_ok, !hidden.contains(&unbound_revoked), "a node supported only by the unbound atom follows its item (tenant={tenant} role={role} state={state})");
                conn.rollback().await.unwrap();
            }
        }
        f.close().await;
    }
}

/// The unbound-atom gate keeps the set-based selector shape: a 68k-atom graph
/// with thousands of items, a share of them restricted, stays bounded.
#[tokio::test]
async fn unbound_atom_source_gate_stays_bounded_at_68k_atoms() {
    const ATOMS: i64 = 68_000;
    const ITEMS: i64 = 6_800;
    const BUDGET_MS: u128 = 1_000;
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "unbound_source_gate_perf").await;
        let slack = origin("slack", "slack:workspace:W1", "grant-a", false);
        let seeded: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) SELECT $1,'slack','slack:workspace:W1','C1/'||i,$2,CASE WHEN i%10=0 THEN 'restricted' WHEN i%10=1 THEN 'unknown' ELSE 'permitted' END FROM generate_series(1,$3) i")
            .bind(f.graph).bind(&slack).bind(ITEMS).execute(&f.owner).await.unwrap();
        // Ten unbound chunk atoms per item, plus a no-item legacy tail.
        sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT $1,'slack',CASE WHEN n<=$2*10-1000 THEN 'C1/'||(1+(n-1)/10)||':chunk:'||((n-1)%10) ELSE 'legacy-'||n END,'fixture','Fixture text.',now() FROM generate_series(1,$3) n")
            .bind(f.graph).bind(ITEMS).bind(ATOMS).execute(&f.owner).await.unwrap();
        sqlx::query("ANALYZE atoms, source_items, source_versions, source_version_chunks").execute(&f.owner).await.unwrap();
        let denied_items: i64 = (1..=ITEMS).filter(|i| i % 10 <= 1).filter(|i| *i <= (ITEMS * 10 - 1000) / 10).count() as i64;
        for role in ["owner", "app"] {
            let mut conn = if role == "owner" {
                f.owner.begin().await.unwrap()
            } else {
                f.tenant.begin_member_scoped(f.user.account_id).await.unwrap()
            };
            let started = std::time::Instant::now();
            let permitted: i64 = sqlx::query_scalar("SELECT count(*) FROM library_permitted_atoms($1,NULL)").bind(f.graph).fetch_one(&mut *conn).await.unwrap();
            let whole = started.elapsed().as_millis();
            let ids: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id=$1 ORDER BY source_id LIMIT 1000").bind(f.graph).fetch_all(&mut *conn).await.unwrap();
            let started = std::time::Instant::now();
            let listed: i64 = sqlx::query_scalar("SELECT count(*) FROM library_permitted_atoms($1,$2)").bind(f.graph).bind(&ids).fetch_one(&mut *conn).await.unwrap();
            let point = started.elapsed().as_millis();
            eprintln!("unbound gate tenant={tenant} role={role} whole_ms={whole} list1000_ms={point} permitted={permitted} listed={listed}");
            assert_eq!(permitted - seeded, ATOMS - denied_items * 10, "every atom of a non-permitted item is excluded");
            assert!(listed > 0 && listed < 1000);
            assert!(whole < BUDGET_MS && point < BUDGET_MS, "unbound gate exceeded {BUDGET_MS} ms (whole={whole} list={point})");
            conn.rollback().await.unwrap();
        }
        f.close().await;
    }
}

/// Seeds one source item with a version and returns (item, version).
async fn unbound_purge_item(f: &Fixture, ns: &str, base: &str, origin: &Value) -> (Uuid, Uuid) {
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack',$2,$3,$4,'permitted') RETURNING source_item_id")
        .bind(f.graph).bind(ns).bind(base).bind(origin).fetch_one(&f.owner).await.unwrap();
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at) VALUES($1,$2,$3,'slack',$4,$5,'Retained text.','{}','2026-01-01T00:00:00Z','2026-02-01T00:00:00Z') RETURNING version_id")
        .bind(f.graph).bind(item).bind(base).bind(ns).bind(origin).fetch_one(&f.owner).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&f.owner).await.unwrap();
    (item, version)
}

/// Inserts an atom whose content lives at a valid, unique blob locator for
/// this plane, so the purge drain can queue its DeleteSourceBlob job.
async fn unbound_purge_atom(f: &Fixture, tenant: bool, source_type: &str, source_id: &str) -> (Uuid, String) {
    let prefix = if tenant { "org-library-proof/" } else { "" };
    let key = format!("{prefix}{}/{}", f.graph, Uuid::new_v4());
    let atom = sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,$2,$3,$4,'Fixture text.',now()) RETURNING atom_id")
        .bind(f.graph).bind(source_type).bind(source_id).bind(&key).fetch_one(&f.owner).await.unwrap();
    (atom, key)
}

/// A restricted item's unbound atoms (no source_version_chunks row, mapped by
/// the 0161 rule) are purged exactly like its bound atoms: retracted at once,
/// deleted by the drain with their blob cleanup queued, their reefs retracted,
/// edges they alone support rejected, and they do not return on re-permit.
/// `atom_support_permitted` follows the same mapping. Other atoms, including
/// near-miss source ids, stay untouched. Both planes; the restriction and the
/// drain run once as the table owner and once through the app role.
#[tokio::test]
async fn restricting_an_item_purges_its_unbound_atoms_like_bound_ones_on_both_planes_and_roles() {
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "unbound_purge").await;
        let ns = "slack:workspace:W1";
        let slack = origin("slack", ns, "grant-a", false);
        for role in ["owner", "app"] {
            let base = format!("C{role}/111.222");
            let (item, version) = unbound_purge_item(&f, ns, &base, &slack).await;
            let (other_item, _) = unbound_purge_item(&f, ns, &format!("C{role}/333.444"), &slack).await;
            let (bound, bound_key) = unbound_purge_atom(&f, tenant, "slack", &format!("{base}:chunk:0")).await;
            sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
                .bind(f.graph).bind(version).bind(bound).execute(&f.owner).await.unwrap();
            let (unbound, unbound_key) = unbound_purge_atom(&f, tenant, "slack", &format!("{base}:chunk:1")).await;
            let (whole, whole_key) = unbound_purge_atom(&f, tenant, "slack", &base).await;
            let (active, active_key) = unbound_purge_atom(&f, tenant, "slack", &format!("C{role}/333.444:chunk:0")).await;
            // Near misses the mapping must not reach: another type, a nested
            // suffix, a non-numeric or empty suffix, a longer base, no item.
            let mut untouched = vec![active];
            for (source_type, source_id) in [
                ("note", format!("{base}:chunk:2")),
                ("slack", format!("{base}:chunk:1:chunk:2")),
                ("slack", format!("{base}:chunk:x")),
                ("slack", format!("{base}:chunk:")),
                ("slack", format!("{base}9:chunk:3")),
                ("slack", format!("C{role}/999.000:chunk:0")),
            ] {
                untouched.push(unbound_purge_atom(&f, tenant, source_type, &source_id).await.0);
            }
            let purged = [bound, unbound, whole];
            let all: Vec<Uuid> = purged.iter().chain(untouched.iter()).copied().collect();
            let unbound_flags: Vec<bool> = sqlx::query_scalar("SELECT NOT source_evidence_bound FROM atoms WHERE atom_id=ANY($1) ORDER BY atom_id=$2")
                .bind(&all[..]).bind(bound).fetch_all(&f.owner).await.unwrap();
            assert_eq!(unbound_flags.iter().filter(|u| **u).count(), all.len() - 1, "fixture: only chunk 0 is bound");

            let reef = |title: String, atom: Uuid| {
                let f = &f;
                async move {
                    let n = node(f, f.graph, &title).await;
                    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(n).bind(atom).execute(&f.owner).await.unwrap();
                    n
                }
            };
            let reef_bound = reef(format!("{role} bound reef"), bound).await;
            let reef_unbound = reef(format!("{role} unbound reef"), unbound).await;
            let reef_active = reef(format!("{role} active reef"), active).await;
            let x = node(&f, f.graph, &format!("{role} X")).await;
            let y = node(&f, f.graph, &format!("{role} Y")).await;
            let edge_bound = edge(&f, f.graph, x, y, "bound-only", "suggested", json!({"atom_ids":[bound]})).await;
            let edge_unbound = edge(&f, f.graph, x, y, "unbound-only", "suggested", json!({"atom_ids":[unbound, whole]})).await;
            let edge_mixed = edge(&f, f.graph, x, y, "mixed", "suggested", json!({"atom_ids":[unbound, active]})).await;
            let edge_active = edge(&f, f.graph, x, y, "active-only", "suggested", json!({"atom_ids":[active]})).await;

            // Restrict through the production writer as the app role, or a
            // direct UPDATE as the owner.
            if role == "app" {
                let mut tx = f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
                let changed = source_evidence::set_source_identity_access_tx(&mut tx, f.graph, "slack", ns, &base, "restricted").await.unwrap();
                assert_eq!(changed, 1);
                tx.commit().await.unwrap();
            } else {
                sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
            }

            let ctx = format!("tenant={tenant} role={role}");
            let retracted: Vec<(Uuid, bool)> = sqlx::query_as("SELECT atom_id,evidence_retracted FROM atoms WHERE atom_id=ANY($1)").bind(&all[..]).fetch_all(&f.owner).await.unwrap();
            for (atom, flag) in retracted {
                assert_eq!(flag, purged.contains(&atom), "{ctx}: atom {atom} is retracted iff it belongs to the restricted item");
            }
            let deps: Vec<Uuid> = sqlx::query_scalar("SELECT d.object_id FROM source_purge_dependencies d JOIN source_purge_events e ON e.event_id=d.event_id WHERE e.graph_id=$1 AND e.source_item_id=$2 AND e.event_kind='restricted' AND d.kind='atom' ORDER BY 1")
                .bind(f.graph).bind(item).fetch_all(&f.owner).await.unwrap();
            let mut want = purged.to_vec();
            want.sort();
            assert_eq!(deps, want, "{ctx}: the item's event records its bound and unbound atoms, nothing else");
            let foreign_deps: i64 = sqlx::query_scalar("SELECT count(*) FROM source_purge_dependencies WHERE object_id=ANY($1)").bind(&untouched[..]).fetch_one(&f.owner).await.unwrap();
            assert_eq!(foreign_deps, 0, "{ctx}: no other atom is queued for purge");
            let other_state: String = sqlx::query_scalar("SELECT access_state FROM source_items WHERE source_item_id=$1").bind(other_item).fetch_one(&f.owner).await.unwrap();
            assert_eq!(other_state, "permitted");

            // Edge support follows the same mapping, for each role.
            for probe in ["owner", "app"] {
                let mut conn = if probe == "owner" { f.owner.begin().await.unwrap() } else { f.tenant.begin_member_scoped(f.user.account_id).await.unwrap() };
                for (atom, expected) in [(bound, false), (unbound, false), (whole, false), (active, true)] {
                    let ok: bool = sqlx::query_scalar("SELECT atom_support_permitted($1,$2)").bind(f.graph).bind(atom.to_string()).fetch_one(&mut *conn).await.unwrap();
                    assert_eq!(ok, expected, "{ctx} probe={probe}: atom_support_permitted({atom})");
                }
                for odd in [active.to_string().to_uppercase(), format!("{{{active}}}"), "not-a-uuid".into(), String::new()] {
                    let ok: bool = sqlx::query_scalar("SELECT atom_support_permitted($1,$2)").bind(f.graph).bind(&odd).fetch_one(&mut *conn).await.unwrap();
                    assert!(!ok, "{ctx} probe={probe}: only the canonical id text matches ({odd:?})");
                }
                let foreign: bool = sqlx::query_scalar("SELECT atom_support_permitted($1,$2)").bind(f.foreign_graph).bind(active.to_string()).fetch_one(&mut *conn).await.unwrap();
                assert!(!foreign, "{ctx} probe={probe}: support is graph-scoped");
                conn.rollback().await.unwrap();
            }
            for (e, status) in [(edge_bound, "rejected"), (edge_unbound, "rejected"), (edge_mixed, "suggested"), (edge_active, "suggested")] {
                let (got, lifecycle): (String, Option<String>) = sqlx::query_as("SELECT status,provenance->>'lifecycle' FROM relationship_edge WHERE edge_id=$1").bind(e).fetch_one(&f.owner).await.unwrap();
                assert_eq!(got, status, "{ctx}: edge {e} at restriction");
                assert_eq!(lifecycle.as_deref() == Some("unsupported"), status == "rejected", "{ctx}: edge {e} lifecycle");
            }

            // Drain as the same role.
            if role == "app" {
                let mut tx = f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
                sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(f.user.account_id.to_string()).execute(&mut *tx).await.unwrap();
                for _ in 0..5 { sqlx::query("SELECT library_drain_purge(100)").execute(&mut *tx).await.unwrap(); }
                tx.commit().await.unwrap();
            } else {
                let mut tx = f.owner.begin().await.unwrap();
                sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(f.user.account_id.to_string()).execute(&mut *tx).await.unwrap();
                for _ in 0..5 { sqlx::query("SELECT library_drain_purge(100)").execute(&mut *tx).await.unwrap(); }
                tx.commit().await.unwrap();
            }
            let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM source_purge_dependencies d JOIN source_purge_events e ON e.event_id=d.event_id WHERE e.graph_id=$1 AND e.source_item_id=$2 AND (d.processed_at IS NULL OR d.error_category IS NOT NULL)")
                .bind(f.graph).bind(item).fetch_one(&f.owner).await.unwrap();
            assert_eq!(pending, 0, "{ctx}: the item's purge drains completely");
            let left: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM atoms WHERE atom_id=ANY($1)").bind(&all[..]).fetch_all(&f.owner).await.unwrap();
            let left: std::collections::BTreeSet<Uuid> = left.into_iter().collect();
            assert_eq!(left, untouched.iter().copied().collect(), "{ctx}: bound and unbound atoms of the item are deleted; others remain");
            let blob_jobs: Vec<String> = sqlx::query_scalar("SELECT payload->>'blob_key' FROM jobs WHERE kind='DeleteSourceBlob' AND payload->>'graph_id'=$1 ORDER BY 1")
                .bind(f.graph.to_string()).fetch_all(&f.owner).await.unwrap();
            for key in [&bound_key, &unbound_key, &whole_key] {
                assert!(blob_jobs.contains(key), "{ctx}: blob cleanup is queued for {key}");
            }
            assert!(!blob_jobs.contains(&active_key), "{ctx}: another item's blob is kept");
            let reef_state = |n: Uuid| {
                let owner = f.owner.clone();
                async move {
                    sqlx::query_scalar::<_, Option<String>>("SELECT body->>'lifecycle' FROM nodes WHERE node_id=$1").bind(n).fetch_optional(&owner).await.unwrap()
                }
            };
            let bound_reef = reef_state(reef_bound).await;
            assert_ne!(bound_reef, Some(None), "{ctx}: the bound atom's reef is retracted");
            assert_eq!(reef_state(reef_unbound).await, bound_reef, "{ctx}: the unbound atom's reef is handled like the bound one");
            assert_eq!(reef_state(reef_active).await, Some(None), "{ctx}: another item's reef is untouched");
            for (e, status) in [(edge_bound, "rejected"), (edge_unbound, "rejected"), (edge_mixed, "suggested"), (edge_active, "suggested")] {
                let got: String = sqlx::query_scalar("SELECT status FROM relationship_edge WHERE edge_id=$1").bind(e).fetch_one(&f.owner).await.unwrap();
                assert_eq!(got, status, "{ctx}: edge {e} after drain");
            }

            // Re-permitting restores neither kind: both were purged.
            sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(item).execute(&f.owner).await.unwrap();
            let visible: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM library_permitted_atoms($1,$2)").bind(f.graph).bind(&all[..]).fetch_all(&f.owner).await.unwrap();
            let visible: std::collections::BTreeSet<Uuid> = visible.into_iter().collect();
            assert_eq!(visible, untouched.iter().copied().collect(), "{ctx}: after re-permit only the untouched atoms are readable");
        }
        f.close().await;
    }
}

/// The unbound-atom purge stays indexed on a 68k-atom graph: the item -> atoms
/// lookup is a range scan on idx_atoms_graph_source_id_pattern (checked on the
/// generic plan plpgsql uses), and restriction events with 5k supported edges
/// in the graph stay bounded for one item and for a 100-item batch.
#[tokio::test]
async fn unbound_atom_purge_stays_bounded_at_68k_atoms() {
    const ITEMS: i64 = 6_800;
    const EDGES: i64 = 5_000;
    const BUDGET_MS: u128 = 1_000;
    const BATCH_BUDGET_MS: u128 = 5_000;
    for tenant in [false, true] {
        let f = Fixture::new(tenant, "unbound_purge_perf").await;
        let ns = "slack:workspace:W1";
        let slack = origin("slack", ns, "grant-a", false);
        sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) SELECT $1,'slack',$2,'C1/'||i,$3,'permitted' FROM generate_series(1,$4) i")
            .bind(f.graph).bind(ns).bind(&slack).bind(ITEMS).execute(&f.owner).await.unwrap();
        // Ten unbound chunk atoms per item, plus a 1k no-item legacy tail.
        sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) SELECT $1,'slack',CASE WHEN n<=$2*10 THEN 'C1/'||(1+(n-1)/10)||':chunk:'||((n-1)%10) ELSE 'legacy-'||n END,'fixture','Fixture text.',now() FROM generate_series(1,$2*10+1000) n")
            .bind(f.graph).bind(ITEMS).execute(&f.owner).await.unwrap();
        let x = node(&f, f.graph, "Perf X").await;
        let y = node(&f, f.graph, "Perf Y").await;
        // Edge i is supported by chunk 0 of item i.
        sqlx::query("INSERT INTO relationship_edge(graph_id,src_node_id,dst_node_id,predicate,status,provenance) SELECT $1,$2,$3,'p'||a.source_id,'suggested',jsonb_build_object('atom_ids',jsonb_build_array(a.atom_id::text)) FROM generate_series(1,$4) i JOIN atoms a ON a.graph_id=$1 AND a.source_id='C1/'||i||':chunk:0'")
            .bind(f.graph).bind(x).bind(y).bind(EDGES).execute(&f.owner).await.unwrap();
        sqlx::query("ANALYZE atoms, source_items, source_versions, source_version_chunks, relationship_edge").execute(&f.owner).await.unwrap();

        // The plpgsql statement runs with parameters; check its generic plan.
        let mut conn = f.owner.acquire().await.unwrap();
        sqlx::query("SET plan_cache_mode=force_generic_plan").execute(&mut *conn).await.unwrap();
        sqlx::query("PREPARE unbound_deps(uuid,text,text) AS SELECT atom_id FROM library_item_unbound_atoms($1,$2,$3)")
            .execute(&mut *conn).await.unwrap();
        let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN EXECUTE unbound_deps('{}','slack','C1/77')", f.graph)).fetch_all(&mut *conn).await.unwrap();
        let plan = plan.join("\n");
        sqlx::query("DEALLOCATE unbound_deps").execute(&mut *conn).await.unwrap();
        sqlx::query("RESET plan_cache_mode").execute(&mut *conn).await.unwrap();
        drop(conn);
        assert!(plan.contains("idx_atoms_graph_source_id_pattern") && !plan.contains("Seq Scan"), "item -> unbound atoms must be an index scan:\n{plan}");

        let deps = |item_source: String| {
            let owner = f.owner.clone();
            let graph = f.graph;
            async move {
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_purge_dependencies d JOIN source_purge_events e ON e.event_id=d.event_id JOIN source_items s ON s.source_item_id=e.source_item_id WHERE e.graph_id=$1 AND s.source_id=$2 AND d.kind='atom'")
                    .bind(graph).bind(item_source).fetch_one(&owner).await.unwrap()
            }
        };
        let started = std::time::Instant::now();
        sqlx::query("UPDATE source_items SET access_state='restricted' WHERE graph_id=$1 AND source_id='C1/1'").bind(f.graph).execute(&f.owner).await.unwrap();
        let owner_one = started.elapsed().as_millis();
        assert_eq!(deps("C1/1".into()).await, 10);
        let started = std::time::Instant::now();
        let mut tx = f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
        source_evidence::set_source_identity_access_tx(&mut tx, f.graph, "slack", ns, "C1/2", "restricted").await.unwrap();
        tx.commit().await.unwrap();
        let app_one = started.elapsed().as_millis();
        assert_eq!(deps("C1/2".into()).await, 10);
        let started = std::time::Instant::now();
        sqlx::query("UPDATE source_items SET access_state='unknown' WHERE graph_id=$1 AND source_id IN(SELECT 'C1/'||i FROM generate_series(3,102) i)").bind(f.graph).execute(&f.owner).await.unwrap();
        let batch = started.elapsed().as_millis();
        let batch_deps: i64 = sqlx::query_scalar("SELECT count(*) FROM source_purge_dependencies d JOIN source_purge_events e ON e.event_id=d.event_id WHERE e.graph_id=$1 AND e.event_kind='unknown' AND d.kind='atom'").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(batch_deps, 1_000);
        let rejected: i64 = sqlx::query_scalar("SELECT count(*) FROM relationship_edge WHERE graph_id=$1 AND status='rejected'").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(rejected, 102, "exactly the edges supported by the restricted items' unbound atoms are rejected");
        let started = std::time::Instant::now();
        let mut tx = f.owner.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(f.user.account_id.to_string()).execute(&mut *tx).await.unwrap();
        let mut drained = 0i32;
        loop {
            let n: i32 = sqlx::query_scalar("SELECT library_drain_purge(200)").fetch_one(&mut *tx).await.unwrap();
            if n == 0 { break; }
            drained += n;
        }
        tx.commit().await.unwrap();
        let drain = started.elapsed().as_millis();
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1 AND source_id ~ '^C1/([1-9]|[1-9][0-9]|10[0-2]):chunk:'").bind(f.graph).fetch_one(&f.owner).await.unwrap();
        assert_eq!(remaining, 0, "every restricted item's unbound atoms are deleted by the drain");
        eprintln!("unbound purge tenant={tenant} owner_one_ms={owner_one} app_one_ms={app_one} batch100_ms={batch} drain_ms={drain} drained={drained}");
        assert!(owner_one < BUDGET_MS && app_one < BUDGET_MS, "single-item restriction exceeded {BUDGET_MS} ms ({owner_one}/{app_one})");
        assert!(batch < BATCH_BUDGET_MS, "100-item restriction exceeded {BATCH_BUDGET_MS} ms ({batch})");
        f.close().await;
    }
}

/// Interim (until the per-node metrics read model): ONLY the Library search
/// read runs with the raised `statement_timeout`. Its transaction carries
/// `SEARCH_STATEMENT_TIMEOUT` (12 s) in effect, transaction-locally; a plain
/// member-scoped transaction keeps the plane default (10 s on the org plane,
/// none on the consumer plane), the other Library reads keep their 5 s, and
/// nothing survives COMMIT onto the next borrower.
#[tokio::test]
async fn only_library_search_runs_with_the_raised_statement_timeout_on_both_planes() {
    use coralswarm_backend::sqlx_statement_log::record;
    assert_eq!(library_read::SEARCH_STATEMENT_TIMEOUT, "12s");
    for tenant_plane in [false, true] {
        let f = Fixture::new(tenant_plane, "search_timeout").await;
        let plane_default = if tenant_plane { "10s" } else { "0" };

        // The search transaction, opened by the same function the route uses.
        let mut tx = library_read::begin_search_tx_for_test(&f.tenant, f.user.account_id)
            .await
            .expect("begin search tx");
        let (timeout, work_mem, pid): (String, String, i32) = sqlx::query_as(
            "SELECT current_setting('statement_timeout'), current_setting('work_mem'), pg_backend_pid()",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        eprintln!("plane tenant={tenant_plane}: search tx statement_timeout={timeout} work_mem={work_mem} backend_pid={pid}");
        assert_eq!(timeout, library_read::SEARCH_STATEMENT_TIMEOUT, "search tx must run with the raised budget");
        assert_eq!(work_mem, "64MB");
        tx.commit().await.unwrap();

        // Any other member-scoped request transaction keeps the plane default.
        let mut tx = f.tenant.begin_member_scoped(f.user.account_id).await.unwrap();
        let timeout: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(timeout, plane_default, "other routes keep the request default");
        tx.rollback().await.unwrap();

        // The real routes: search sends the 12 s setting; scope resolution
        // and facets-style reads send their own 5 s and never 12 s.
        let (response, log) = record(library_read::search(
            f.tenant.clone(),
            Extension(f.user.clone()),
            Json(SearchRequest {
                scope: scope(f.graph, json!({})),
                query: String::new(),
                page_size: Some(5),
                sources_after: None,
                outcomes_after: None,
                nodes_after: None,
            }),
        ))
        .await;
        let (status, v) = body(response).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(
            log.count_containing("set_config('statement_timeout','12s',true)"),
            1,
            "the search route must apply the raised budget: {:?}",
            log.statements()
        );
        let (response, log) = record(library_read::resolve_scope(
            f.tenant.clone(),
            Extension(f.user.clone()),
            Json(ScopeRequest { scope: scope(f.graph, json!({})) }),
        ))
        .await;
        let (status, v) = body(response).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(log.count_containing("12s"), 0, "scope resolution must not get the search budget");
        assert_eq!(log.count_containing("statement_timeout='5s'"), 1, "{:?}", log.statements());

        // Nothing leaks onto the next transaction.
        let mut tx = f.app.begin().await.unwrap();
        let timeout: String = sqlx::query_scalar("SELECT current_setting('statement_timeout')")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_ne!(timeout, library_read::SEARCH_STATEMENT_TIMEOUT, "SET LOCAL semantics: reverted at COMMIT");
        tx.rollback().await.unwrap();
        f.close().await;
    }
}


/// Main ba5bec59's composed SQL is deliberately frozen, rather than rebuilt
/// from the new definition: this proof cannot change both sides by accident.
#[cfg(feature = "test-support")]
mod evidence_definition_proof {
    use super::*;
    use coralswarm_backend::library_read::evidence;
    use chrono::{DateTime, Utc};

    const OLD_RETAINED: &str = include_str!("support/library_read_main/search_retained.sql");
    const OLD_SERVED: &str = include_str!("support/library_read_main/search_served.sql");

    async fn run_search(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, sql: &str, s: &LibraryScope, query: Option<&str>, previous: Option<&Value>) -> Value {
        let serialized = serde_json::to_value(s).unwrap();
        let mut origin = json!({});
        for key in ["service_id", "connection", "container", "content_kind"] {
            if !serialized[key].is_null() { origin[key] = serialized[key].clone(); }
        }
        let last = |key: &str| previous.and_then(|p| p[key]["items"].as_array()).and_then(|a| a.first());
        let stamp = |v: Option<&Value>, key: &str| v.and_then(|v| v[key].as_str()).map(|s| s.parse::<DateTime<Utc>>().unwrap());
        let id = |v: Option<&Value>, key: &str| v.and_then(|v| v[key].as_str()).map(|s| s.parse::<Uuid>().unwrap());
        let src = last("sources"); let atom = last("outcomes"); let reef = last("nodes");
        sqlx::query_scalar(sql)
            .bind(&s.graph_ids).bind(origin).bind(s.source_item_id).bind(s.source_version_id).bind(s.node_id)
            .bind(serialized["collection"].as_str()).bind(query)
            .bind(stamp(src, "captured_at")).bind(id(src, "source_item_id"))
            .bind(stamp(atom, "captured_at")).bind(id(atom, "semantic_atom_id"))
            .bind(reef.and_then(|n| n["source_count"].as_i64())).bind(reef.and_then(|n| n["atom_count"].as_i64())).bind(id(reef, "node_id"))
            .bind(2_i64).bind(stamp(atom, "occurred_at"))
            .fetch_one(&mut **tx).await.unwrap()
    }

    #[tokio::test]
    async fn evidence_definition_matches_main_search_and_node_metrics_on_both_planes_and_roles() {
        for tenant in [false, true] {
            let f = Fixture::new(tenant, "evidence_definition").await;
            let atlas = node(&f, f.graph, "Atlas").await;
            let legacy = node(&f, f.graph, "Legacy").await;
            let hidden = node(&f, f.graph, "Hidden").await;
            let empty = node(&f, f.graph, "Empty").await;
            let mut sources = Vec::new();
            for (i, kind) in ["message", "coding_session", "work_session", "meeting", "message", "message", "message"].iter().enumerate() {
                let mut o = origin("slack", &format!("evidence:{i}"), "fixture", i == 4);
                o["content_kind"] = json!(kind);
                let source = source(&f, f.graph, &format!("evidence:{i}"), o, if i == 6 {"restricted"} else {"permitted"}, "Atlas capture", "Atlas evidence.", &[atlas], "Atlas").await;
                sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2),($3,$2)").bind(atlas).bind(source.atom).bind(legacy).execute(&f.owner).await.unwrap();
                sources.push(source);
            }
            sqlx::query("UPDATE source_versions SET is_legacy_baseline=true WHERE version_id=$1").bind(sources[1].version).execute(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_versions SET source_metadata=source_metadata||'{\"independence\":\"derived_copy\"}' WHERE version_id=$1").bind(sources[2].version).execute(&f.owner).await.unwrap();
            // Legacy-only item with no synthetic metadata: nullable legacy
            // SQL flags normalize to false at the new stored-row boundary.
            sqlx::query("DELETE FROM node_semantic_atoms WHERE semantic_atom_id=$1").bind(sources[5].semantic).execute(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_versions SET source_metadata=source_metadata-'synthetic_demo' WHERE version_id=$1").bind(sources[5].version).execute(&f.owner).await.unwrap();
            // Metadata-only recapture serves the older byte-identical outcomes.
            sqlx::query("UPDATE source_versions SET extraction_status='done',content_sha256=repeat('a',64) WHERE version_id=$1").bind(sources[0].version).execute(&f.owner).await.unwrap();
            let current: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,content_sha256,source_metadata,captured_at,extraction_status,is_legacy_baseline) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,content_sha256,source_metadata,'2026-03-01','pending',true FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(sources[0].version).fetch_one(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(sources[0].item).bind(current).execute(&f.owner).await.unwrap();
            // Changed content with no extracted outcomes has retained history
            // but no served outcomes; source support still counts in both modes.
            let changed: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,content_sha256,source_metadata,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'Changed content.',repeat('b',64),source_metadata,'2026-03-02','pending' FROM source_versions WHERE version_id=$1 RETURNING version_id").bind(sources[3].version).fetch_one(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(sources[3].item).bind(changed).execute(&f.owner).await.unwrap();
            let (unmapped, _) = unbound_purge_atom(&f, tenant, "note", "unmapped Atlas").await;
            sqlx::query("UPDATE atoms SET content_inline='Legacy is independently supported.' WHERE atom_id=$1").bind(unmapped).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(legacy).bind(unmapped).execute(&f.owner).await.unwrap();
            let (denied_unbound, _) = unbound_purge_atom(&f, tenant, "slack", "same-provider-local-id:chunk:9").await;
            let (retracted, _) = unbound_purge_atom(&f, tenant, "note", "retracted").await;
            let (derived, _) = unbound_purge_atom(&f, tenant, "note", "derived").await;
            sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1").bind(retracted).execute(&f.owner).await.unwrap();
            sqlx::query("UPDATE atoms SET derived_copy_bound=true WHERE atom_id=$1").bind(derived).execute(&f.owner).await.unwrap();
            for atom in [denied_unbound, retracted, derived] {
                sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(hidden).bind(atom).execute(&f.owner).await.unwrap();
            }
            // A supported identity correction changes title visibility, not metrics.
            let mut correction = f.owner.begin().await.unwrap();
            sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','user',true)").bind(f.user.account_id.to_string()).bind(f.user.user_id.to_string()).execute(&mut *correction).await.unwrap();
            sqlx::query("INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,content_revision,input_sha256) SELECT graph_id,$2,'Legacy',$3,atom_id,'legacy',content_revision,reef_identity_atom_fingerprint(a) FROM atoms a WHERE atom_id=$1").bind(unmapped).bind(legacy).bind(f.user.user_id).execute(&mut *correction).await.unwrap();
            correction.commit().await.unwrap();
            let foreign = node(&f, f.foreign_graph, "Foreign").await;
            source(&f, f.foreign_graph, "evidence:foreign", origin("slack","evidence:foreign","fixture",false),"permitted","Foreign capture","Foreign private.",&[foreign],"Foreign").await;

            for role in ["owner", "app"] {
                let mut tx = if role == "owner" { f.owner.begin().await.unwrap() } else { f.tenant.begin_member_scoped(f.user.account_id).await.unwrap() };
                sqlx::query("SELECT set_config('statement_timeout','120s',true)").execute(&mut *tx).await.unwrap();
                let shapes = [json!({}), json!({"node_id":atlas}), json!({"node_id":legacy}), json!({"source_item_id":sources[0].item}), json!({"source_version_id":sources[0].version}), json!({"source_item_id":sources[0].item,"source_version_id":sources[0].version}), json!({"service_id":"slack"}), json!({"content_kind":"meeting"}), json!({"connection":{"kind":"oauth_grant","id":"fixture"}}), json!({"container":{"kind":"channel","id":"C1","provider_namespace":"evidence:0"}}), json!({"graph_ids":[f.graph,f.foreign_graph]}), json!({"collection":"agent_sessions"}), json!({"collection":"work_sessions"}), json!({"collection":"meetings"})];
                let mut comparisons = 0;
                for mode in ["all_retained", "current"] {
                    for extra in &shapes {
                        let mut s = scope(f.graph, extra.clone());
                        s.outcome_versions = Some(if mode == "current" { library_read::OutcomeVersions::Current } else { library_read::OutcomeVersions::AllRetained });
                        let old = if mode == "current" && s.source_version_id.is_none() { OLD_SERVED } else { OLD_RETAINED };
                        let new = library_read::search_sql_for_explain(&s);
                        // PR5 intentionally changes text matching. Keep the
                        // frozen independent SQL and pin its admission/version
                        // and evidence prefix; the full JSON/page comparisons
                        // below still prove the complete response is unchanged.
                        let prefix_end = new.find("\n, text_matched_outcomes AS")
                            .or_else(|| new.find("\n, matched_atoms AS")).unwrap();
                        // #1525 adds the reader's history floor to the scope's
                        // semantic reads; this fixture's reader has none, so
                        // the frozen SQL stays the oracle for everything else.
                        assert_eq!(old.split("\n, matched_sources").next().unwrap().trim(), without_history_floor(&new[..prefix_end]).trim(), "expanded scope SQL must stay byte-identical to ba5bec59 apart from boundary whitespace and the #1525 floor");
                        for query in [None, Some("%Atlas%"), Some("%no-match%"), Some("%Legacy%")] {
                            let mut previous = None;
                            for page in 1..=3 {
                                let before = run_search(&mut tx, old, &s, query, previous.as_ref()).await;
                                let after = run_search(&mut tx, &new, &s, query, previous.as_ref()).await;
                                assert_eq!(before.to_string().as_bytes(), after.to_string().as_bytes(), "tenant={tenant} role={role} mode={mode} shape={extra} query={query:?} page={page}");
                                previous = Some(before); comparisons += 1;
                            }
                        }
                    }
                }
                // Node-keyed rows and per-item links are the same definition
                // over a subset, with zero rows filled for unsupported nodes.
                let nodes = [atlas, legacy, hidden, empty];
                let node_sql = format!("SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY node_id),'[]') FROM ({}) e", &*evidence::NODE_EVIDENCE_SQL);
                let rows: Value = sqlx::query_scalar(&node_sql).bind(f.graph).bind(&nodes[..]).fetch_one(&mut *tx).await.unwrap();
                assert_eq!(rows.as_array().unwrap().len(), nodes.len());
                let atlas_row = rows.as_array().unwrap().iter().find(|r| r["node_id"]==json!(atlas)).unwrap();
                assert!(atlas_row["atom_count_retained"].as_i64().unwrap()>atlas_row["atom_count_served"].as_i64().unwrap());
                assert_eq!(atlas_row["unknown_independence_record_count"],3,"current baseline classifies the older independent link as unknown");
                assert_eq!(rows.as_array().unwrap().iter().find(|r| r["node_id"]==json!(hidden)).unwrap()["admitted_unfiltered"],false);

                for mode in ["all_retained", "current"] {
                    let s = scope(f.graph, json!({"outcome_versions":mode}));
                    let live_sql = format!("{} SELECT COALESCE(jsonb_agg(to_jsonb(m) ORDER BY node_id),'[]') FROM scope_node_metrics m WHERE node_id=ANY($7)", library_read::scope_sql_for_explain(&s));
                    let metrics: Value = sqlx::query_scalar(&live_sql).bind(&s.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind(&nodes[..]).fetch_one(&mut *tx).await.unwrap();
                    for row in rows.as_array().unwrap() {
                        let old = metrics.as_array().unwrap().iter().find(|r| r["node_id"] == row["node_id"]);
                        for (before, after) in [("source_count","source_count"),("unknown_independence_record_count","unknown_independence_record_count"),("current_atom_count","atom_count_served"),("unmapped_legacy_count","unmapped_legacy_count"),("atom_count",if mode=="current" {"atom_count_served"} else {"atom_count_retained"})] {
                            assert_eq!(old.map(|r| r[before].clone()).unwrap_or(json!(0)),row[after],"tenant={tenant} role={role} mode={mode} {before}: {row}");
                        }
                        assert_eq!(old.map(|r| r["admitted"].clone()).unwrap_or(json!(false)), row["admitted_unfiltered"]);
                    }
                }
                let link_sql = format!("SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY node_id,source_item_id),'[]') FROM ({}) e", &*evidence::NODE_ITEM_EVIDENCE_SQL);
                let links: Value = sqlx::query_scalar(&link_sql).bind(f.graph).bind(&nodes[..]).fetch_one(&mut *tx).await.unwrap();
                assert!(links.as_array().unwrap().iter().any(|r| r["semantic_link"]==true && r["legacy_link"]==true && r["project_mention"]==true));
                assert!(!links.to_string().contains(&sources[6].item.to_string()));
                assert!(links.as_array().unwrap().iter().any(|r| r["source_item_id"]==json!(sources[0].item) && r["legacy_independent_version"]==true),"current baseline independence must not rewrite linked historical version flags");
                assert!(links.as_array().unwrap().iter().filter(|r| r["source_item_id"]==json!(sources[5].item)).all(|r| r["legacy_synthetic"]==false && r["semantic_link"]==false));
                let s = scope(f.graph, json!({"outcome_versions":"all_retained"}));
                let expected_sql = format!(r#"{} SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY node_id,source_item_id),'[]') FROM (
 SELECT k.graph_id,k.node_id,k.source_item_id,
  EXISTS(SELECT 1 FROM scoped_semantic_links x WHERE (x.graph_id,x.node_id,x.source_item_id)=(k.graph_id,k.node_id,k.source_item_id)) semantic_link,
  EXISTS(SELECT 1 FROM scoped_legacy_links x WHERE (x.graph_id,x.node_id,x.source_item_id)=(k.graph_id,k.node_id,k.source_item_id)) legacy_link,
  COALESCE((SELECT independent FROM scoped_legacy_links x WHERE (x.graph_id,x.node_id,x.source_item_id)=(k.graph_id,k.node_id,k.source_item_id)),false) legacy_independent_version,
  COALESCE((SELECT synthetic FROM scoped_legacy_links x WHERE (x.graph_id,x.node_id,x.source_item_id)=(k.graph_id,k.node_id,k.source_item_id)),false) legacy_synthetic,
  (SELECT count(DISTINCT x.semantic_atom_id) FROM scoped_semantic_links x WHERE (x.graph_id,x.node_id,x.source_item_id)=(k.graph_id,k.node_id,k.source_item_id)) atom_count_retained,
  (SELECT count(DISTINCT x.semantic_atom_id) FROM scoped_semantic_links x WHERE (x.graph_id,x.node_id,x.source_item_id)=(k.graph_id,k.node_id,k.source_item_id) AND source_version_is_current) atom_count_served,
  EXISTS(SELECT 1 FROM retained_scope_atoms a JOIN scoped_semantic_links l ON l.graph_id=a.graph_id AND l.semantic_atom_id=a.semantic_atom_id JOIN nodes n ON n.graph_id=l.graph_id AND n.node_id=l.node_id CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.entities)='array' THEN a.entities ELSE '[]'::jsonb END) e WHERE (l.graph_id,l.node_id,a.source_item_id)=(k.graph_id,k.node_id,k.source_item_id) AND e->>'type'='project' AND lower(e->>'mention')=lower(substring(n.title FOR 256))) project_mention
 FROM scoped_node_sources k WHERE node_id=ANY($7)) e"#, OLD_RETAINED.split("\n, matched_sources").next().unwrap());
                let expected: Value = sqlx::query_scalar(&expected_sql).bind(&s.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind(&nodes[..]).fetch_one(&mut *tx).await.unwrap();
                // The independent frozen oracle predates the additive v2
                // page-context fields. Keep its complete ten-field contract
                // intact, and verify every new field from fixture facts rather
                // than rebuilding the oracle from the production definition.
                let added_fields = ["semantic_independent_version", "semantic_synthetic", "semantic_latest_captured_at",
                    "entity_type_has_match", "entity_type_value", "entity_type_captured_at", "entity_type_semantic_atom_id", "entity_type_ambiguous"];
                for row in links.as_array().unwrap() {
                    assert_eq!(row.as_object().unwrap().len(), 18);
                    let i = sources.iter().position(|source| row["source_item_id"] == json!(source.item)).unwrap();
                    assert!(i < 6, "restricted source must never enter either grain contract");
                    let semantic = row["node_id"] == json!(atlas) && i != 5;
                    assert_eq!(row["semantic_independent_version"], semantic && ![1, 2].contains(&i));
                    assert_eq!(row["semantic_synthetic"], semantic && i == 4);
                    assert_eq!(row["entity_type_has_match"], semantic);
                    assert_eq!(row["entity_type_value"], if semantic { json!("project") } else { Value::Null });
                    assert_eq!(row["entity_type_semantic_atom_id"], if semantic { json!(sources[i].semantic) } else { Value::Null });
                    assert_eq!(row["entity_type_ambiguous"], false);
                    for field in ["semantic_latest_captured_at", "entity_type_captured_at"] {
                        if semantic {
                            let actual = row[field].as_str().expect("semantic timestamp is JSON text").parse::<DateTime<Utc>>().unwrap();
                            assert_eq!(actual, "2026-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(), "historical linked-version timestamp {field}");
                        } else { assert_eq!(row[field], Value::Null, "absent semantic timestamp is JSON null: {field}"); }
                    }
                }
                let mut original_links = links.clone();
                for row in original_links.as_array_mut().unwrap() {
                    for field in added_fields { assert!(row.as_object_mut().unwrap().remove(field).is_some()); }
                }
                assert_eq!(original_links, expected, "complete original per-(node,item) link grain equality tenant={tenant} role={role}");
                for subset in [vec![atlas], vec![foreign], vec![]] {
                    let subset_rows: Value = sqlx::query_scalar(&node_sql).bind(f.graph).bind(&subset).fetch_one(&mut *tx).await.unwrap();
                    let expected: Vec<Value> = rows.as_array().unwrap().iter().filter(|r| subset.iter().any(|id| r["node_id"]==json!(id))).cloned().collect();
                    assert_eq!(subset_rows,json!(expected),"node-keyed batches must be independent of their neighbors");
                }

                for row in rows.as_array().unwrap() {
                    let id: Uuid = row["node_id"].as_str().unwrap().parse().unwrap();
                    let admitted: bool = sqlx::query_scalar("SELECT n.type IS NOT NULL AND (EXISTS(SELECT 1 FROM node_semantic_atoms l JOIN served_semantic_atoms a ON a.graph_id=l.graph_id AND a.semantic_atom_id=l.semantic_atom_id WHERE l.node_id=n.node_id AND l.graph_id=n.graph_id) OR EXISTS(SELECT 1 FROM node_atoms l JOIN library_permitted_atoms(n.graph_id,NULL) a ON a.atom_id=l.atom_id WHERE l.node_id=n.node_id)) FROM nodes n WHERE node_id=$1").bind(id).fetch_one(&mut *tx).await.unwrap();
                    assert_eq!(row["stats_admitted"],admitted,"stats admission for {id}");
                }
                eprintln!("evidence proof tenant={tenant} role={role}: {comparisons} byte-identical search pages, both node metrics modes, link grain, stats admission");
                tx.rollback().await.unwrap();
            }
            f.close().await;
        }
    }
    #[tokio::test]
    async fn evidence_definition_inherits_the_tenant_history_floor() {
        let f = Fixture::new(true, "evidence_floor").await;
        sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
        let team: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Evidence window fixture',$3) RETURNING id").bind(f.user.account_id).bind(f.graph).bind(f.user.user_id).fetch_one(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(f.user.user_id).execute(&f.owner).await.unwrap();
        sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture','2026-01-01')").bind(f.user.account_id).bind(team).bind(f.user.user_id).execute(&f.owner).await.unwrap();
        let reef = node(&f,f.graph,"Window").await;
        for (source_id,stamp) in [("old","2025-01-01"),("recent","2026-02-01")] {
            let (atom,_) = unbound_purge_atom(&f,true,"note",source_id).await;
            sqlx::query("UPDATE atoms SET timestamp=$2::text::timestamptz,content_inline='Window evidence.' WHERE atom_id=$1").bind(atom).bind(stamp).execute(&f.owner).await.unwrap();
            sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(reef).bind(atom).execute(&f.owner).await.unwrap();
        }
        for (role,expected) in [("owner",2_i64),("app",1_i64)] {
            let mut tx = if role=="owner" {f.owner.begin().await.unwrap()} else {f.tenant.begin_member_scoped(f.user.account_id).await.unwrap()};
            let sql = format!("SELECT unmapped_legacy_count FROM ({}) e",&*evidence::NODE_EVIDENCE_SQL);
            let count: i64 = sqlx::query_scalar(&sql).bind(f.graph).bind(&[reef][..]).fetch_one(&mut *tx).await.unwrap();
            assert_eq!(count,expected,"history floor role={role}");
            let s=scope(f.graph,json!({}));
            let sql=format!("{} SELECT unmapped_legacy_count FROM scope_node_metrics WHERE node_id=$7",library_read::scope_sql_for_explain(&s));
            let live: i64=sqlx::query_scalar(&sql).bind(&s.graph_ids).bind(json!({})).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<Uuid>).bind(None::<String>).bind(reef).fetch_one(&mut *tx).await.unwrap();
            assert_eq!(count,live);
            tx.rollback().await.unwrap();
        }
        f.close().await;
    }

}

#[cfg(feature = "test-support")]
mod text_search_reuse_proof {
    use super::*;

    fn before_sql(sql: &str) -> String {
        let start = sql.find("\n, text_matched_outcomes AS")
            .or_else(|| sql.find("\n, matched_atoms AS")).expect("matching CTE");
        let end = sql.find("), title_matched_nodes AS").expect("title CTE") + 1;
        let mut before = sql.to_owned();
        before.replace_range(start..end, include_str!("fixtures/library_text_matches_before.sql"));
        before
    }

    async fn execute(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, sql: &str,
        s: &LibraryScope, query: &str,
    ) -> Value {
        let mut origin_filter = json!({});
        for key in ["service_id", "connection", "container", "content_kind"] {
            let value = serde_json::to_value(s).unwrap()[key].clone();
            if !value.is_null() { origin_filter[key] = value; }
        }
        let collection = s.collection.as_ref().map(|c| serde_json::to_value(c).unwrap().as_str().unwrap().to_owned());
        let pattern = (!query.is_empty()).then(|| format!("%{}%", query.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")));
        sqlx::query_scalar(sql)
            .bind(&s.graph_ids).bind(origin_filter).bind(s.source_item_id)
            .bind(s.source_version_id).bind(s.node_id).bind(collection).bind(pattern)
            .bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>)
            .bind(None::<chrono::DateTime<chrono::Utc>>).bind(None::<Uuid>)
            .bind(None::<i64>).bind(None::<i64>).bind(None::<Uuid>).bind(3_i64)
            .bind(None::<chrono::DateTime<chrono::Utc>>)
            .fetch_one(&mut **tx).await.unwrap()
    }

    #[tokio::test]
    async fn text_match_reuse_equals_frozen_predicates_on_both_rls_planes_and_history_floor() {
        let mut comparisons = 0;
        for tenant in [false, true] {
            let f = Fixture::new(tenant, "text_reuse").await;
            let reef = node(&f, f.graph, "Deploy work").await;
            let mut sources = Vec::new();
            // Match paths are deliberately disjoint. In particular source-id
            // hits must NOT admit outcomes, and source-title hits still do.
            for (i, title, statement) in [
                (0, "deploy title only", "quiet title outcome"),
                (1, "quiet statement title", "deploy statement only"),
                (2, "quiet quote title", "quiet quote outcome"),
                (3, "quiet source id title", "quiet source id outcome"),
                (4, "literal title", "literal % _ \\ and café"),
            ] {
                sources.push(source(&f, f.graph, &format!("text:{i}"), origin("slack", "text", "fixture", false), "permitted", title, statement, &[reef], "Deploy work").await);
            }
            sqlx::query("UPDATE semantic_atom_passages SET quote='deploy passage only' WHERE semantic_atom_id=$1")
                .bind(sources[2].semantic).execute(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_items SET source_id='deploy-id-only' WHERE source_item_id=$1")
                .bind(sources[3].item).execute(&f.owner).await.unwrap();
            for access in ["restricted", "unknown"] {
                source(&f, f.graph, &format!("text:{access}"), origin("slack", "text", "fixture", false), access, "DO NOT LEAK deploy", "DO NOT LEAK deploy", &[reef], "Deploy work").await;
            }
            let foreign_reef = node(&f, f.foreign_graph, "FOREIGN deploy").await;
            source(&f, f.foreign_graph, "text:foreign", origin("slack", "text", "fixture", false), "permitted", "FOREIGN deploy", "FOREIGN deploy", &[foreign_reef], "Foreign").await;
            // A changed current capture with no extracted outcomes keeps its
            // old deploy outcome in retained mode, but not current mode.
            let changed: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,content_sha256,source_metadata,captured_at,extraction_status) SELECT graph_id,source_item_id,source_id,source_type,provider_namespace,origin,'changed bytes',repeat('b',64),source_metadata,'2026-03-02','pending' FROM source_versions WHERE version_id=$1 RETURNING version_id")
                .bind(sources[1].version).fetch_one(&f.owner).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(sources[1].item).bind(changed).execute(&f.owner).await.unwrap();

            if tenant {
                sqlx::query("UPDATE graphs SET kind='team',owner_user_id=NULL WHERE id=$1").bind(f.graph).execute(&f.owner).await.unwrap();
                let team: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Text history fixture',$3) RETURNING id").bind(f.user.account_id).bind(f.graph).bind(f.user.user_id).fetch_one(&f.owner).await.unwrap();
                sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(f.user.user_id).execute(&f.owner).await.unwrap();
                sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'fixture','2026-01-01')").bind(f.user.account_id).bind(team).bind(f.user.user_id).execute(&f.owner).await.unwrap();
                for (id, stamp) in [("old", "2025-01-01"), ("new", "2026-02-01")] {
                    let legacy = node(&f, f.graph, &format!("legacy {id}")).await;
                    let (atom, _) = unbound_purge_atom(&f, true, "note", id).await;
                    sqlx::query("UPDATE atoms SET timestamp=$2::text::timestamptz,content_inline='deploy legacy evidence' WHERE atom_id=$1").bind(atom).bind(stamp).execute(&f.owner).await.unwrap();
                    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(legacy).bind(atom).execute(&f.owner).await.unwrap();
                }
            }
            for role in ["owner", "app"] {
                let mut tx = if role == "owner" { f.owner.begin().await.unwrap() } else { f.app.begin().await.unwrap() };
                sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *tx).await.unwrap();
                if role == "app" {
                    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true),set_config('app.tenant_actor','user',true)")
                        .bind(f.user.account_id.to_string()).bind(f.user.user_id.to_string()).execute(&mut *tx).await.unwrap();
                }
                sqlx::query("SELECT set_config('statement_timeout','120s',true)").execute(&mut *tx).await.unwrap();
                for mode in ["all_retained", "current"] {
                    for shape in [json!({}), json!({"service_id":"slack"}), json!({"collection":"meetings"}), json!({"source_item_id":sources[1].item}), json!({"source_version_id":sources[1].version}), json!({"node_id":reef})] {
                        let mut s = scope(f.graph, shape);
                        s.outcome_versions = Some(if mode == "current" { library_read::OutcomeVersions::Current } else { library_read::OutcomeVersions::AllRetained });
                        s.normalize().unwrap();
                        for query in ["", "deploy", "DEPLOY", "a", "de", "%", "_", "\\", "café"] {
                            let sql = library_read::search_sql_for_text_explain(&s, query);
                            assert_eq!(sql.contains("text_matched_outcomes AS"), query.chars().count() >= 3 && s.source_item_id.is_none() && s.source_version_id.is_none() && s.node_id.is_none());
                            let before = execute(&mut tx, &before_sql(&sql), &s, query).await;
                            let after = execute(&mut tx, &sql, &s, query).await;
                            assert_eq!(after, before, "plane={tenant} role={role} mode={mode} query={query:?} scope={s:?}");
                            assert!(!after.to_string().contains("DO NOT LEAK"));
                            assert!(!after.to_string().contains("FOREIGN"));
                            comparisons += 1;
                        }
                    }
                }
                tx.rollback().await.unwrap();
            }
            // Both concurrent index files must really have run successfully
            // through SQLx, not just parse as CREATE INDEX statements.
            let valid: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname IN ('idx_library_semantic_atoms_graph_statement_trgm','idx_library_semantic_atom_passages_graph_quote_trgm') AND i.indisvalid AND i.indisready").fetch_one(&f.owner).await.unwrap();
            assert_eq!(valid, 2);
            f.close().await;
        }
        assert_eq!(comparisons, 432);
        println!("432 exact JSON comparisons passed across both RLS planes, owner/app roles, modes, filtered/version scopes and history-floor fixture");
    }
}

/// `sql` with exactly the #1525 history-floor fragments removed, for the
/// frozen-SQL oracle above. Each fragment must be present at most where
/// `library_read::scope_cte` puts it; anything else still has to match.
fn without_history_floor(sql: &str) -> String {
    let floor = coralswarm_backend::history_floor::admits_version;
    let served_old = "WHERE $5::uuid IS NULL OR (a.graph_id,a.semantic_atom_id) IN (SELECT l.graph_id,l.semantic_atom_id FROM node_semantic_atoms l WHERE l.node_id=$5)";
    let served_new = format!("WHERE ($5::uuid IS NULL OR (a.graph_id,a.semantic_atom_id) IN (SELECT l.graph_id,l.semantic_atom_id FROM node_semantic_atoms l WHERE l.node_id=$5))\n  AND {}", floor("v"));
    // #1545: the item floor on scope_sources, and the boundary claims the
    // guard (scripts/check_history_floor_boundary.sh) reads in the SQL.
    let item_floor = format!(
        "\n  -- The reader's team-seat history floor on the item at its shown version (#1545).\n  AND {}",
        coralswarm_backend::history_floor::admits_source_item_at("s", "v")
    );
    sql.replace(&item_floor, "")
        .replace("\n -- history_floor: keys only, from the floored scope_sources; the served\n -- version's atoms are floored where they are read (SERVED_SCOPE_ATOMS_SQL).", "")
        .replace("\n -- history_floor: scoped_permitted_atoms is library_permitted_atoms, which floors.", "")
        .replace(&served_new, served_old)
        .replace(&format!(" AND {}", floor("mapped")), "")
        .replace("\n -- The reader's team-seat history floor (crate::history_floor), per version.", "")
        .replace(&format!(" AND {}", floor("v")), "")
}

#[derive(Clone, Default)]
struct LogBuf(Arc<std::sync::Mutex<Vec<u8>>>);
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
    fn fallback_events(&self) -> Vec<Value> {
        let bytes = std::mem::take(&mut *self.0.lock().unwrap());
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|v| v["fields"]["event"] == "library_live_fallback")
            .collect()
    }
}

/// The default mode is On, and a graph whose Model is not built yet falls
/// back to Live: each 200 logs exactly one `library_live_fallback` line after
/// commit, and a refused or not-found search logs none. The kill switch's
/// INFO line is covered by `live_fallback`'s unit tests and by
/// `library_read_mode_pg`.
#[tokio::test]
async fn a_live_served_search_logs_library_live_fallback() {
    for tenant_plane in [false, true] {
        let f = Fixture::new(tenant_plane, "live_fallback").await;
        let buf = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(buf.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let (status, _) = search(&f, scope(f.graph, json!({})), "Atlas", 5, None).await;
        assert_eq!(status, StatusCode::OK);
        let events = buf.fallback_events();
        assert_eq!(events.len(), 1, "plane={tenant_plane}: {events:?}");
        let fields = &events[0]["fields"];
        assert_eq!(events[0]["level"], "WARN");
        assert_eq!((&fields["reason"], &fields["mode"]), (&json!("not_ready"), &json!("on")));
        assert_eq!(fields["plane"], if tenant_plane { "org" } else { "consumer" });
        assert_eq!(fields["account_id"], f.user.account_id.to_string());
        assert_eq!(fields["graphs"], 1);
        if tenant_plane {
            assert_eq!(fields["org_id"], "org-library-proof");
        } else {
            assert!(fields.get("org_id").is_none(), "{fields}");
        }

        let (foreign, _) = search(&f, scope(f.foreign_graph, json!({})), "Atlas", 5, None).await;
        assert!(foreign.is_client_error(), "{foreign}");
        let (missing, _) = search(&f, scope(f.graph, json!({"node_id": Uuid::new_v4()})), "", 5, None).await;
        assert_eq!(missing, StatusCode::NOT_FOUND);
        let events = buf.fallback_events();
        assert!(events.is_empty(), "a refused search is not a Live serve: {events:?}");
        drop(_guard);
        f.close().await;
    }
}
