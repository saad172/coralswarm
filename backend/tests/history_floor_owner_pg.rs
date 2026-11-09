//! #1525: team-seat history floors hold for the production connection.
//!
//! Production tenant requests connect as the table owner, which bypasses RLS,
//! so `history_from_pg.rs` (which runs as `coralswarm_tenant_app`) cannot see
//! the leak. Every read here runs on the OWNER pool, through the production
//! scope openers, exactly as prod does. A floored member B and a full-history
//! member A read the same team ocean, which holds a legacy atom and a semantic
//! atom from before B's floor and one of each from after it. B must never see
//! the pre-floor rows; A must see all four (the positive control).
//!
//!   ORGT_TEST_BASE_URL=postgres://localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test history_floor_owner_pg -- --test-threads=1

use std::{str::FromStr, sync::Arc};

use axum::{body::to_bytes, Extension};
use chrono::{DateTime, Duration, SubsecRound, Utc};
use coralswarm_backend::{
    library_read::{self, LibraryJson, LibraryScope, SearchRequest},
    retrieval::RetrievalFilter,
    store::{PostgresStore, Store},
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
        scope::begin_tenant_scoped,
    },
    UserContext,
};
use pgvector::Vector;
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection, PgPool,
};
use uuid::Uuid;

const ORG: &str = "org_history_floor";

struct Seeded {
    legacy_pre: Uuid,
    legacy_post: Uuid,
    semantic_pre: Uuid,
    semantic_post: Uuid,
    node: Uuid,
}

struct Fx {
    admin: PgConnection,
    name: coralswarm_backend::test_db::TestDatabase,
    owner: PgPool,
    account: Uuid,
    graph: Uuid,
    /// Full history: no grant at all (a legacy seat).
    full: Uuid,
    /// Floored at `cutoff`.
    floored: Uuid,
    cutoff: DateTime<Utc>,
}

fn probe_vector() -> Vec<f32> {
    (0..384).map(|i| (i as f32 * 0.01).sin()).collect()
}

impl Fx {
    async fn setup(suffix: &str) -> Option<Self> {
        let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
            assert!(std::env::var("REQUIRE_PG_SUITE").is_err(), "REQUIRE_PG_SUITE set without ORGT_TEST_BASE_URL");
            eprintln!("skipping history_floor_owner_pg: set ORGT_TEST_BASE_URL");
            return None;
        };
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let mut admin = PgConnection::connect_with(&opts).await.unwrap();
        let name = coralswarm_backend::test_db::TestDatabase::new(&opts, &format!("history_floor_owner_{suffix}"));
        admin.execute(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)").as_str()).await.unwrap();
        admin.execute(format!("CREATE DATABASE {name}").as_str()).await.unwrap();
        let owner = PgPoolOptions::new().max_connections(4).connect_with(opts.database(&name)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
        // The owner bypasses RLS, as `neondb_owner` does in production. If
        // this ever stops holding, the suite no longer proves the prod case.
        let bypass: bool = sqlx::query_scalar(
            "SELECT r.rolsuper OR r.rolbypassrls OR c.relowner=r.oid AND NOT c.relforcerowsecurity \
             FROM pg_roles r, pg_class c WHERE r.rolname=current_user AND c.oid='public.atoms'::regclass",
        )
        .fetch_one(&owner)
        .await
        .unwrap();
        assert!(bypass, "the fixture must connect as an RLS-bypassing owner, like production");
        sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1)").bind(ORG).execute(&owner).await.unwrap();

        let account = Uuid::new_v4();
        let full = Uuid::new_v4();
        let floored = Uuid::new_v4();
        let cutoff = Utc::now().trunc_subsecs(6) - Duration::days(30);
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind) VALUES($1,'Team ocean','team') RETURNING id")
            .bind(account).fetch_one(&owner).await.unwrap();
        let team: Uuid = sqlx::query_scalar("INSERT INTO teams(account_id,graph_id,name,created_by) VALUES($1,$2,'Eng',$3) RETURNING id")
            .bind(account).bind(graph).bind(full).fetch_one(&owner).await.unwrap();
        for u in [full, floored] {
            sqlx::query("INSERT INTO team_members(team_id,user_id,role) VALUES($1,$2,'member')").bind(team).bind(u).execute(&owner).await.unwrap();
        }
        sqlx::query("INSERT INTO team_seat_grants(account_id,team_id,user_id,source,history_from) VALUES($1,$2,$3,'test:late-join',$4)")
            .bind(account).bind(team).bind(floored).bind(cutoff).execute(&owner).await.unwrap();
        Some(Self { admin, name, owner, account, graph, full, floored, cutoff })
    }

    async fn seed(&self) -> Seeded {
        let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','quokka expedition briefing','{}') RETURNING node_id")
            .bind(self.graph).fetch_one(&self.owner).await.unwrap();
        let mut legacy = vec![];
        let mut semantic = vec![];
        for (tag, offset) in [("pre", -3600_i64), ("post", 3600)] {
            let ts = self.cutoff + Duration::seconds(offset);
            let atom: Uuid = sqlx::query_scalar(
                "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,content_tsv,embedding,embed_model,embedding_next,embed_model_next,\"timestamp\") \
                 VALUES($1,'note',$2,'blob',$3,to_tsvector('simple','quokka expedition briefing'),$4,$5,$4,$6,$7) RETURNING atom_id",
            )
            .bind(self.graph).bind(format!("hf-legacy-{tag}")).bind(format!("quokka expedition briefing legacy {tag}"))
            .bind(Vector::from(probe_vector())).bind(coralswarm_core::embed::EMBED_MODEL_NAME).bind(coralswarm_core::embed::EMBED_MODEL_CLOUD).bind(ts)
            .fetch_one(&self.owner).await.unwrap();
            sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(atom).execute(&self.owner).await.unwrap();
            legacy.push(atom);

            let id = format!("hf-item-{tag}");
            let origin = json!({"service_id":"slack","content_kind":"message","provider_namespace":id,"connection":{"kind":"oauth_grant","id":"fixture"},"container":{"kind":"channel","id":"C1","provider_namespace":id}});
            let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin,access_state) VALUES($1,'slack',$2,$2,$3,'permitted') RETURNING source_item_id")
                .bind(self.graph).bind(&id).bind(&origin).fetch_one(&self.owner).await.unwrap();
            let statement = format!("quokka expedition briefing outcome {tag}");
            // The version's occurred_at is the semantic atom's moment; its
            // captured_at is after the floor for both, so only occurred_at
            // can decide.
            let version: Uuid = sqlx::query_scalar(
                "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,origin,raw_inline,source_metadata,occurred_at,captured_at,extraction_status) \
                 VALUES($1,$2,$3,'slack',$3,$4,$5,$6,$7,now(),'done') RETURNING version_id",
            )
            .bind(self.graph).bind(item).bind(&id).bind(&origin).bind(&statement).bind(json!({"title":format!("Quokka capture {tag}")})).bind(ts)
            .fetch_one(&self.owner).await.unwrap();
            sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&self.owner).await.unwrap();
            let outcome: Uuid = sqlx::query_scalar(
                "INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities) \
                 VALUES($1,$2,0,'fact',$3,'source-atoms-v1','fixture','[]') RETURNING semantic_atom_id",
            )
            .bind(self.graph).bind(version).bind(&statement).fetch_one(&self.owner).await.unwrap();
            sqlx::query("INSERT INTO semantic_atom_passages(semantic_atom_id,source_version_id,graph_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5)")
                .bind(outcome).bind(version).bind(self.graph).bind(statement.len() as i32).bind(&statement).execute(&self.owner).await.unwrap();
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(self.graph).bind(node).bind(outcome).execute(&self.owner).await.unwrap();
            semantic.push(outcome);
        }
        Seeded { legacy_pre: legacy[0], legacy_post: legacy[1], semantic_pre: semantic[0], semantic_post: semantic[1], node }
    }

    fn tenant(&self, user: Uuid) -> Tenant {
        Tenant {
            pool: Some(self.owner.clone()),
            store: Some(Arc::new(PostgresStore::new(self.owner.clone()))),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: ORG.into(),
                account_id: self.account,
                user_id: user,
                pool: self.owner.clone(),
                page_pool: None,
            })),
            class: coralswarm_backend::route_class::RouteClass::Other,
        }
    }

    fn user(&self, user: Uuid) -> UserContext {
        UserContext { user_id: user, account_id: self.account, role: "member".into(), device_id: "history-floor".into() }
    }

    fn scope(&self) -> LibraryScope {
        let mut scope: LibraryScope = serde_json::from_value(json!({"graph_ids":[self.graph]})).unwrap();
        scope.normalize().unwrap();
        scope
    }

    async fn teardown(mut self) {
        self.owner.close().await;
        self.admin.execute(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name).as_str()).await.ok();
    }
}

fn ids(v: &Value, pointer: &str, key: &str) -> Vec<Uuid> {
    v.pointer(pointer)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("{pointer} missing in {v}"))
        .iter()
        .filter_map(|e| e[key].as_str().and_then(|s| Uuid::parse_str(s).ok()))
        .collect()
}

/// Positive control for A, exclusion for B: A sees both, B sees only `post`.
fn assert_floor(path: &str, full: &[Uuid], floored: &[Uuid], pre: Uuid, post: Uuid) {
    assert!(full.contains(&pre) && full.contains(&post), "{path}: the full-history member must see both rows, got {full:?}");
    assert!(floored.contains(&post), "{path}: the floored member must still see the post-floor row, got {floored:?}");
    assert!(!floored.contains(&pre), "{path}: the floored member saw a pre-floor row: {floored:?}");
}

#[tokio::test]
async fn store_scoped_reads_and_atom_detail_respect_the_floor_as_owner() {
    let Some(fx) = Fx::setup("store").await else { return };
    let s = fx.seed().await;
    let store: Box<dyn Store> = Box::new(PostgresStore::new(fx.owner.clone()));
    let filter = RetrievalFilter::default();
    let far = Duration::days(3650);
    let mut seen: Vec<(&str, Vec<Uuid>, Vec<Uuid>)> = vec![];
    for user in [fx.full, fx.floored] {
        let mut tx = begin_tenant_scoped(&fx.owner, fx.account, user).await.unwrap();
        let vector: Vec<Uuid> = store.vector_search_scoped(&mut tx, fx.graph, &probe_vector(), 10, &filter).await.unwrap().into_iter().map(|r| r.0).collect();
        let keyword: Vec<Uuid> = store.keyword_search_scoped(&mut tx, fx.graph, "quokka expedition briefing", 10, &filter).await.unwrap().into_iter().map(|r| r.0).collect();
        let window: Vec<Uuid> = store.timestamp_window_atoms_scoped(&mut tx, fx.graph, fx.cutoff - far, fx.cutoff + far, None, &filter).await.unwrap().into_iter().map(|r| r.0).collect();
        let expand: Vec<Uuid> = store.expand_scoped(&mut tx, fx.graph, &[s.legacy_post], Some(&probe_vector())).await.unwrap().into_iter().flat_map(|n| n.atom_ids).collect();
        let node_kw: Vec<Uuid> = store.keyword_search_node_atoms_scoped(&mut tx, fx.graph, "Quokka Briefing", 10, &filter).await.unwrap().into_iter().map(|r| r.0).collect();
        let mut detail = vec![];
        for atom in [s.legacy_pre, s.legacy_post] {
            if store.atom_detail_scoped(&mut tx, atom, fx.account).await.unwrap().is_some() {
                detail.push(atom);
            }
        }
        tx.rollback().await.unwrap();
        for (path, got) in [("vector_search_scoped", vector), ("keyword_search_scoped", keyword), ("timestamp_window_atoms_scoped", window),
            ("expand_scoped", expand), ("keyword_search_node_atoms_scoped", node_kw), ("atom_detail_scoped (get_atom, GET /atoms)", detail)] {
            if user == fx.full { seen.push((path, got, vec![])); } else { seen.iter_mut().find(|e| e.0 == path).unwrap().2 = got; }
        }
    }
    for (path, full, floored) in &seen {
        assert_floor(path, full, floored, s.legacy_pre, s.legacy_post);
    }
    fx.teardown().await;
}

#[tokio::test]
async fn served_semantic_atoms_and_the_permission_boundary_respect_the_floor_as_owner() {
    let Some(fx) = Fx::setup("boundary").await else { return };
    let s = fx.seed().await;
    let mut got = vec![];
    for user in [fx.full, fx.floored] {
        let mut tx = begin_tenant_scoped(&fx.owner, fx.account, user).await.unwrap();
        let served: Vec<Uuid> = sqlx::query_scalar("SELECT semantic_atom_id FROM served_semantic_atoms WHERE graph_id=$1").bind(fx.graph).fetch_all(&mut *tx).await.unwrap();
        let permitted: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM library_permitted_atoms($1,NULL)").bind(fx.graph).fetch_all(&mut *tx).await.unwrap();
        let by_id: Vec<Uuid> = sqlx::query_scalar("SELECT atom_id FROM library_permitted_atoms($1,$2)").bind(fx.graph).bind(vec![s.legacy_pre, s.legacy_post]).fetch_all(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        got.push((served, permitted, by_id));
    }
    assert_floor("served_semantic_atoms", &got[0].0, &got[1].0, s.semantic_pre, s.semantic_post);
    assert_floor("library_permitted_atoms(g,NULL)", &got[0].1, &got[1].1, s.legacy_pre, s.legacy_post);
    assert_floor("library_permitted_atoms(g,ids)", &got[0].2, &got[1].2, s.legacy_pre, s.legacy_post);
    fx.teardown().await;
}

#[tokio::test]
async fn library_live_search_respects_the_floor_as_owner() {
    let Some(fx) = Fx::setup("library").await else { return };
    let s = fx.seed().await;
    let mut out = vec![];
    for user in [fx.full, fx.floored] {
        let q = SearchRequest { scope: fx.scope(), query: String::new(), page_size: Some(40), sources_after: None, outcomes_after: None, nodes_after: None };
        let response = library_read::search(fx.tenant(user), Extension(fx.user(user)), LibraryJson(q)).await;
        assert_eq!(response.status(), 200);
        let v: Value = serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
        out.push(v);
    }
    assert_floor("Library search outcomes", &ids(&out[0], "/outcomes/items", "semantic_atom_id"), &ids(&out[1], "/outcomes/items", "semantic_atom_id"), s.semantic_pre, s.semantic_post);
    let node = |v: &Value| v["nodes"]["items"].as_array().unwrap().iter().find(|n| n["node_id"] == json!(s.node)).cloned().unwrap_or_else(|| panic!("node missing: {v}"));
    let (a, b) = (node(&out[0]), node(&out[1]));
    assert_eq!(a["contribution"]["semantic_atom_count"], 2, "{a}");
    assert_eq!(b["contribution"]["semantic_atom_count"], 1, "the floored member's node count must exclude the pre-floor outcome: {b}");
    assert_eq!(a["contribution"]["unmapped_legacy_chunk_count"], 2, "{a}");
    assert_eq!(b["contribution"]["unmapped_legacy_chunk_count"], 1, "the floored member's node count must exclude the pre-floor legacy chunk: {b}");
    assert_eq!(a["wider_context"]["semantic_atom_count"], 2, "{a}");
    assert_eq!(b["wider_context"]["semantic_atom_count"], 1, "wider context must exclude the pre-floor outcome: {b}");
    fx.teardown().await;
}

#[tokio::test]
async fn workspace_counts_sql_preserves_owner_and_rls_history_floors() {
    let Some(fx) = Fx::setup("counts_sql").await else { return };
    let s = fx.seed().await;
    // Separate pre-floor-only reefs prove both admission arms are excluded;
    // the original mixed reef remains supported by post-floor evidence.
    let semantic_node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'reference','Pre-floor semantic reef','{}') RETURNING node_id")
        .bind(fx.graph).fetch_one(&fx.owner).await.unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
        .bind(fx.graph).bind(semantic_node).bind(s.semantic_pre).execute(&fx.owner).await.unwrap();
    let legacy_node:Uuid=sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'thread','Pre-floor legacy reef','{}') RETURNING node_id")
        .bind(fx.graph).fetch_one(&fx.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(legacy_node).bind(s.legacy_pre).execute(&fx.owner).await.unwrap();
    for owner in [false,true] {
        for (user,reefs,atoms) in [
            (fx.full,json!({"entity":1,"reference":1,"thread":1}),2_i64),
            (fx.floored,json!({"entity":1}),1_i64),
        ] {
            let mut tx=begin_tenant_scoped(&fx.owner,fx.account,user).await.unwrap();
            sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await.unwrap();
            if !owner { sqlx::query("SET LOCAL ROLE coralswarm_tenant_app").execute(&mut *tx).await.unwrap(); }
            let rls:bool=sqlx::query_scalar("SELECT row_security_active('atoms')").fetch_one(&mut *tx).await.unwrap();
            assert_eq!(rls,!owner);
            let old:(Value,i64,i64)=sqlx::query_as("SELECT * FROM graph_workspace_counts_compute($1)")
                .bind(fx.graph).fetch_one(&mut *tx).await.unwrap();
            let new:(Value,i64,i64)=sqlx::query_as(coralswarm_backend::read_models::oceans::COUNTS_SQL)
                .bind(fx.graph).fetch_one(&mut *tx).await.unwrap();
            assert_eq!(new,old,"same actor, role and read-only snapshot");
            assert_eq!(new,(reefs,atoms,atoms));
            tx.rollback().await.unwrap();
        }
    }
    fx.teardown().await;
}

#[tokio::test]
async fn ask_candidates_respect_the_floor_as_owner() {
    let Some(fx) = Fx::setup("ask").await else { return };
    let s = fx.seed().await;
    let terms = vec!["quokka".to_string()];
    let mut out = vec![];
    for user in [fx.full, fx.floored] {
        let v = library_read::retrieve_ask_candidates(&fx.tenant(user), &fx.user(user), fx.scope(), &terms, None, None, 20).await.unwrap_or_else(|_| panic!("ask retrieval failed"));
        out.push(v);
    }
    assert_floor("Ask outcomes", &ids(&out[0], "/outcomes", "semantic_atom_id"), &ids(&out[1], "/outcomes", "semantic_atom_id"), s.semantic_pre, s.semantic_post);
    assert_floor("Ask legacy evidence", &ids(&out[0], "/legacy_evidence", "atom_id"), &ids(&out[1], "/legacy_evidence", "atom_id"), s.legacy_pre, s.legacy_post);

    // A citation minted before the seat was floored is refused at emission.
    let mut scope = fx.scope();
    scope.serve_current();
    let mut admitted = vec![];
    for user in [fx.full, fx.floored] {
        let mut tx = fx.tenant(user).begin_member_scoped(fx.account).await.unwrap();
        let mut ok = vec![];
        for (citation, id) in out[0]["outcomes"].as_array().unwrap().iter().map(|o| (o["citations"].clone(), o["semantic_atom_id"].as_str().unwrap().to_owned())) {
            if library_read::ask_inputs_in_scope_tx(&mut tx, &scope, &citation).await.unwrap() {
                ok.push(Uuid::parse_str(&id).unwrap());
            }
        }
        for legacy in out[0]["legacy_evidence"].as_array().unwrap() {
            let support = json!([{"kind":"legacy_chunk","graph_id":legacy["graph_id"],"atom_id":legacy["atom_id"]}]);
            if library_read::ask_inputs_in_scope_tx(&mut tx, &scope, &support).await.unwrap() {
                ok.push(Uuid::parse_str(legacy["atom_id"].as_str().unwrap()).unwrap());
            }
        }
        tx.rollback().await.unwrap();
        admitted.push(ok);
    }
    assert_floor("Ask receipt fence (semantic)", &admitted[0], &admitted[1], s.semantic_pre, s.semantic_post);
    assert_floor("Ask receipt fence (legacy)", &admitted[0], &admitted[1], s.legacy_pre, s.legacy_post);
    fx.teardown().await;
}

#[tokio::test]
async fn the_accelerated_library_reader_refuses_a_floored_member_as_owner() {
    let Some(fx) = Fx::setup("model").await else { return };
    fx.seed().await;
    let scope = fx.scope();
    for (user, floored) in [(fx.full, false), (fx.floored, true)] {
        let mut tx = fx.tenant(user).begin_member_repeatable_read(fx.account).await.unwrap();
        library_read::configure_read_model_proof_tx(&mut tx).await.unwrap();
        let r = library_read::candidate_program_interleaved_for_test(&mut tx, &scope, "", || async {}).await.unwrap();
        tx.rollback().await.unwrap();
        if floored {
            assert_eq!(r["path"], "live", "{r}");
            assert_eq!(r["fallback_reason"], "permission_window", "a floored member must fall to Live: {r}");
            let served = ids(&r, "/value/outcomes/items", "semantic_atom_id");
            assert_eq!(served.len(), 1, "the Live fallback the floored member gets is floor-filtered: {r}");
        } else {
            assert_ne!(r["fallback_reason"], "permission_window", "an unfloored member is not refused for a floor: {r}");
        }
    }
    fx.teardown().await;
}
