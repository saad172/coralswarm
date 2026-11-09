//! P11 recorded historical state on actual consumer/tenant app roles: explicit
//! effective validity, immutable knowledge time, scoped Ask cutoff inheritance
//! and live source revocation. No network model; sources-only Ask.
mod support;

use axum::{body::to_bytes, extract::State, http::StatusCode, response::Response, Extension};
use coralswarm_backend::{
    library_read::{LibraryJson, LibraryScope},
    scoped_ask::{self, AskRequest},
    store::PostgresStore,
    temporal::{self, RecordRequest, StateRequest, TemporalSelection},
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
    },
    AppState, UserContext,
};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Executor, PgPool,
};
use std::{str::FromStr, sync::Arc};
use uuid::Uuid;

struct Fixture {
    admin: PgPool,
    control_owner: PgPool,
    control_app: PgPool,
    owner: PgPool,
    app: PgPool,
    names: Vec<coralswarm_backend::test_db::TestDatabase>,
    tenant: Tenant,
    user: UserContext,
    graph: Uuid,
    foreign_graph: Uuid,
    node: Uuid,
}
struct Src {
    item: Uuid,
    version: Uuid,
    atom: Uuid,
    passage: Uuid,
}
impl Fixture {
    async fn new(org: bool) -> Option<Self> {
        let Ok(dsn) = std::env::var("P7_TEST_BASE_URL") else {
            assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "local PG required");
            return None;
        };
        let opts = PgConnectOptions::from_str(&dsn).unwrap();
        assert!(matches!(opts.get_host(), "127.0.0.1" | "localhost" | "::1"));
        assert_eq!(opts.get_database(), Some("postgres"));
        let admin = PgPoolOptions::new().max_connections(2).connect_with(opts.clone()).await.unwrap();
        let control_name = coralswarm_backend::test_db::TestDatabase::new(&opts, "p11_temporal_control");
        admin.execute(format!("CREATE DATABASE {control_name}").as_str()).await.unwrap();
        let control_owner = PgPoolOptions::new().max_connections(3).connect_with(opts.clone().database(&control_name)).await.unwrap();
        coralswarm_backend::CONTROL_MIGRATOR.run(&control_owner).await.unwrap();
        support::app_role::set_app_role_password_with_retry(&control_owner).await;
        let control_app = PgPoolOptions::new().max_connections(4)
            .connect_with(opts.clone().database(&control_name).username("coralswarm_app").password(support::app_role::APP_ROLE_TEST_PASSWORD))
            .await.unwrap();
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES($1) RETURNING id")
            .bind(format!("p11-{}@test.local", Uuid::new_v4())).fetch_one(&control_owner).await.unwrap();
        let org_id = format!("org_fake_{}", Uuid::new_v4().simple());
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id,clerk_org_id) VALUES($1,$2) RETURNING id")
            .bind(user_id).bind(org.then_some(&org_id)).fetch_one(&control_owner).await.unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) VALUES($1,$2,'owner',(SELECT CASE WHEN a.is_org_account THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active) END FROM accounts a WHERE a.id=$1))")
            .bind(account).bind(user_id).execute(&control_owner).await.unwrap();
        let mut names = vec![control_name];
        let (owner, app) = if org {
            let name = coralswarm_backend::test_db::TestDatabase::new(&opts, "p11_temporal_tenant");
            admin.execute(format!("CREATE DATABASE {name}").as_str()).await.unwrap();
            let owner = PgPoolOptions::new().max_connections(3).connect_with(opts.clone().database(&name)).await.unwrap();
            sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
            support::tenant_role::set_app_role_password_with_retry(&owner).await;
            let app = PgPoolOptions::new().max_connections(4)
                .connect_with(opts.database(&name).username("coralswarm_tenant_app").password(support::tenant_role::APP_ROLE_TEST_PASSWORD))
                .await.unwrap();
            names.push(name);
            (owner, app)
        } else {
            (control_owner.clone(), control_app.clone())
        };
        // Evidence that the proof runs on the low-privilege role, never as owner.
        let privs: (bool, bool, i64) = sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&app).await.unwrap();
        assert_eq!(privs, (false, false, 0));
        let role: String = sqlx::query_scalar("SELECT current_user::text").fetch_one(&app).await.unwrap();
        assert_eq!(role, if org { "coralswarm_tenant_app" } else { "coralswarm_app" });
        eprintln!("p11 temporal proof executing on role {role} (org={org})");
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'P11 temporal','member',$2) RETURNING id")
            .bind(account).bind(user_id).fetch_one(&owner).await.unwrap();
        let foreign_graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,owner_user_id) VALUES($1,'Other private','member',$2) RETURNING id")
            .bind(account).bind(Uuid::new_v4()).fetch_one(&owner).await.unwrap();
        let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title,body) VALUES($1,'entity','Release train','{}') RETURNING node_id")
            .bind(graph).fetch_one(&owner).await.unwrap();
        let tenant = Tenant {
            store: Some(Arc::new(PostgresStore::new(app.clone()))),
            pool: Some(app.clone()),
            decision: Some(if org {
                TenancyDecision::Org(TenantContext {
                    org_id,
                    account_id: account,
                    user_id,
                    pool: app.clone(),
                    page_pool: None,
                })
            } else {
                TenancyDecision::Personal
            }),
            class: coralswarm_backend::route_class::RouteClass::Other,
        };
        Some(Self {
            admin, control_owner, control_app, owner, app, names, tenant,
            user: UserContext { user_id, account_id: account, role: "owner".into(), device_id: "p11-fixture".into() },
            graph, foreign_graph, node,
        })
    }
    fn state(&self) -> Arc<AppState> {
        Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(self.control_app.clone(), None))
    }
    async fn source(&self, graph: Uuid, node: Option<Uuid>, id: &str, text: &str) -> Src {
        let origin = json!({"service_id":"linear","transport":"synthetic","content_kind":"issue","capture_surface":"synthetic_demo"});
        let o = &self.owner;
        let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,'linear_issue','demo:linear:workspace:fixture',$2,$3) RETURNING source_item_id")
            .bind(graph).bind(id).bind(&origin).fetch_one(o).await.unwrap();
        // Capture time is deliberately unrelated to the declared effective dates.
        let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,extraction_version,origin,captured_at,occurred_at) VALUES($1,$2,$3,'linear_issue','demo:linear:workspace:fixture',$4,'source-atoms-v1',$5,'2026-09-30T00:00:00Z','2026-09-29T00:00:00Z') RETURNING version_id")
            .bind(graph).bind(item).bind(id).bind(text).bind(&origin).fetch_one(o).await.unwrap();
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(o).await.unwrap();
        let atom: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'decision',$3,'source-atoms-v1','fake-model') RETURNING semantic_atom_id")
            .bind(graph).bind(version).bind(text).fetch_one(o).await.unwrap();
        let passage: Uuid = sqlx::query_scalar("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,$4,$5) RETURNING passage_id")
            .bind(graph).bind(version).bind(atom).bind(text.chars().count() as i32).bind(text).fetch_one(o).await.unwrap();
        if let Some(n) = node {
            sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)").bind(graph).bind(n).bind(atom).execute(o).await.unwrap();
        }
        // The version-registration trigger records knowledge without backfilling history.
        let kinds: Vec<String> = sqlx::query_scalar("SELECT observation_kind FROM source_version_observations WHERE source_version_id=$1")
            .bind(version).fetch_all(o).await.unwrap();
        assert_eq!(kinds, ["version_registered"]);
        Src { item, version, atom, passage }
    }
    fn node_scope(&self) -> LibraryScope {
        serde_json::from_value(json!({"graph_ids":[self.graph],"node_id":self.node})).unwrap()
    }
    fn record_req(&self, s: &Src, op: &str, value: Option<&str>, from: Option<&str>, quote: Option<&str>, pred: Option<Uuid>) -> RecordRequest {
        serde_json::from_value(json!({
            "scope":{"graph_ids":[self.graph],"node_id":self.node,"source_item_id":s.item,"source_version_id":s.version},
            "state_key":"release.status","semantic_atom_id":s.atom,"passage_id":s.passage,"operation":op,"value":value,
            "valid_from":from.map(|d|format!("{d}T00:00:00Z")),"valid_until":null,"validity_quote":quote,"predecessor_event_id":pred
        })).unwrap()
    }
    async fn record(&self, q: RecordRequest) -> (StatusCode, Value) {
        body(temporal::record(State(self.state()), self.tenant.clone(), Extension(self.user.clone()), LibraryJson(q)).await).await
    }
    fn historical(&self, effective: &str, known: chrono::DateTime<chrono::Utc>) -> TemporalSelection {
        TemporalSelection::Historical {
            revision: 1,
            state_key: "release.status".into(),
            effective_at: format!("{effective}T00:00:00Z").parse().unwrap(),
            as_known_at: known,
        }
    }
    async fn read_state(&self, scope: LibraryScope, t: TemporalSelection) -> (StatusCode, Value) {
        body(temporal::state(State(self.state()), self.tenant.clone(), Extension(self.user.clone()), LibraryJson(StateRequest { scope, temporal: t })).await).await
    }
    async fn now(&self) -> chrono::DateTime<chrono::Utc> {
        sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(&self.owner).await.unwrap()
    }
    async fn permitted(&self, receipt: Uuid) -> bool {
        let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&self.app, self.user.account_id, self.user.user_id).await.unwrap();
        let ok: bool = sqlx::query_scalar("SELECT library_answer_permitted($1)").bind(receipt).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        ok
    }
    async fn done(self) {
        self.app.close().await;
        self.control_app.close().await;
        self.owner.close().await;
        self.control_owner.close().await;
        for name in self.names {
            self.admin.execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str()).await.unwrap();
        }
        self.admin.close().await;
    }
}
async fn body(r: Response) -> (StatusCode, Value) {
    let status = r.status();
    let bytes = to_bytes(r.into_body(), 4_000_000).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}
fn values(state: &Value) -> Vec<String> {
    state["values"].as_array().unwrap().iter().map(|v| v["value"].as_str().unwrap().to_string()).collect()
}
fn ask(text: &str, scope: Option<LibraryScope>, conv: Option<Value>, t: Option<TemporalSelection>, action: Option<&str>) -> AskRequest {
    serde_json::from_value(json!({"text":text,"library_scope":scope,"conversation_id":conv,"temporal":t,"scope_action":action,"style":"sources-only"})).unwrap()
}

async fn run(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    coralswarm_backend::test_modern_cached_task_admission(&f.state(), &f.tenant, &f.user).await;
    let before_any = f.now().await;
    let a = f.source(f.graph, Some(f.node), "plan", "Release status is green effective 2026-01-01 per the steering decision.").await;
    let b = f.source(f.graph, Some(f.node), "change", "From 2026-03-01 the release status changed to red after the audit.").await;
    let unlinked = f.source(f.graph, None, "unlinked", "Release status is amber effective 2026-01-01.").await;

    // Declared validity must be anchored in the retained words; no date is invented.
    let (s, _) = f.record(f.record_req(&a, "assert", Some("green"), Some("2026-01-01"), None, None)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "validity without its quote");
    let (s, _) = f.record(f.record_req(&a, "assert", Some("green"), Some("2026-02-01"), Some("effective 2026-01-01"), None)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "date not in quote");
    let (s, _) = f.record(f.record_req(&a, "assert", Some("purple"), Some("2026-01-01"), Some("effective 2026-01-01"), None)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "value not in passage");
    let (s, _) = f.record(f.record_req(&a, "assert", Some("green"), Some("2026-01-01"), Some("status 2026-01-01"), None)).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "quote not in passage");
    // The subject must canonically support the atom.
    let (s, _) = f.record(f.record_req(&unlinked, "assert", Some("amber"), Some("2026-01-01"), Some("effective 2026-01-01"), None)).await;
    assert_ne!(s, StatusCode::CREATED);

    let (s, first) = f.record(f.record_req(&a, "assert", Some("green"), Some("2026-01-01"), Some("effective 2026-01-01"), None)).await;
    assert_eq!(s, StatusCode::CREATED, "{first}");
    assert_eq!(first["recording_method"], "human_curated_v1");
    let first_id: Uuid = serde_json::from_value(first["event_id"].clone()).unwrap();
    let between = f.now().await;
    let (s, second) = f.record(f.record_req(&b, "transition", Some("red"), Some("2026-03-01"), Some("From 2026-03-01"), Some(first_id))).await;
    assert_eq!(s, StatusCode::CREATED, "{second}");

    // Effective time and knowledge time are independent axes.
    let now = f.now().await;
    let (s, v) = f.read_state(f.node_scope(), f.historical("2026-02-01", now)).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let st = &v["temporal_state"];
    assert_eq!((st["status"].as_str(), values(st)), (Some("supported"), vec!["green".to_string()]));
    assert_eq!(st["state_basis"], "declared_interpretations_not_verified_provider_state");
    assert_eq!(st["history_coverage"], "explicit_records_only");
    assert_eq!(st["records"][0]["epistemic_status"], "human_interpretation");
    let (_, v) = f.read_state(f.node_scope(), f.historical("2026-04-01", now)).await;
    assert_eq!(values(&v["temporal_state"]), ["red"]);
    // As known before the transition was recorded, April was still green.
    let (_, v) = f.read_state(f.node_scope(), f.historical("2026-04-01", between)).await;
    assert_eq!(values(&v["temporal_state"]), ["green"]);
    let (_, v) = f.read_state(f.node_scope(), f.historical("2026-04-01", before_any)).await;
    assert_eq!(v["temporal_state"]["status"], "unsupported");
    // Before any declared validity the state is unknown, not back-filled from capture time.
    let (_, v) = f.read_state(f.node_scope(), f.historical("2025-06-01", now)).await;
    assert_eq!(v["temporal_state"]["status"], "unknown");

    // Invalid selections: future knowledge, no node, aggregate current mode.
    let future = chrono::Utc::now() + chrono::Duration::days(1);
    assert_eq!(f.read_state(f.node_scope(), f.historical("2026-04-01", future)).await.0, StatusCode::BAD_REQUEST);
    let no_node: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.graph]})).unwrap();
    assert_eq!(f.read_state(no_node, f.historical("2026-04-01", now)).await.0, StatusCode::BAD_REQUEST);
    // Another member's private graph shares not-found semantics with the P7 boundary.
    let foreign: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.foreign_graph],"node_id":f.node})).unwrap();
    assert_eq!(f.read_state(foreign, f.historical("2026-04-01", now)).await.0, StatusCode::NOT_FOUND);

    // An independent contradictory assertion stays disputed; nothing picks a winner.
    let c = f.source(f.graph, Some(f.node), "dissent", "Ops note: release status stays green effective 2026-03-15.").await;
    let (s, dissent) = f.record(f.record_req(&c, "assert", Some("green"), Some("2026-03-15"), Some("effective 2026-03-15"), None)).await;
    assert_eq!(s, StatusCode::CREATED, "{dissent}");
    let now = f.now().await;
    let (_, v) = f.read_state(f.node_scope(), f.historical("2026-04-01", now)).await;
    assert_eq!(v["temporal_state"]["status"], "disputed");
    assert_eq!(values(&v["temporal_state"]), ["green", "red"]);
    assert_eq!(v["temporal_state"]["coverage_scope"], "selected_scope");
    // Narrowing to one source hides the readable transition and dissent: the
    // slice must not be presented as a confident "green".
    let narrowed: LibraryScope = serde_json::from_value(json!({"graph_ids":[f.graph],"node_id":f.node,"source_item_id":a.item})).unwrap();
    let (s, v) = f.read_state(narrowed, f.historical("2026-04-01", now)).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["temporal_state"]["status"], "incomplete");
    assert_eq!(v["temporal_state"]["coverage_scope"], "scope_limited");
    // Evidence observation time is the version's first server observation, not the annotation.
    let ts = |k: &str| -> chrono::DateTime<chrono::Utc> { serde_json::from_value(v["temporal_state"]["records"][0][k].clone()).unwrap() };
    assert!(ts("evidence_observed_at") < ts("known_at"));

    // Immutable knowledge: interpretation and knowledge time cannot be rewritten by the app role.
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app, f.user.account_id, f.user.user_id).await.unwrap();
    let rewrite = sqlx::query("UPDATE temporal_evidence_records SET value='blue' WHERE event_id=$1").bind(first_id).execute(&mut *tx).await;
    assert!(rewrite.is_err(), "temporal record value must be immutable");
    tx.rollback().await.unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&f.app, f.user.account_id, f.user.user_id).await.unwrap();
    let backdate = sqlx::query("UPDATE source_version_observations SET known_at='2000-01-01' WHERE source_version_id=$1").bind(a.version).execute(&mut *tx).await;
    assert!(backdate.is_err(), "knowledge observations must be immutable");
    tx.rollback().await.unwrap();

    // Scoped Ask: historical cutoff, inheritance on follow-up, explicit replace required.
    let state = f.state();
    let t = f.historical("2026-04-01", between);
    let first_ask = scoped_ask::execute(&state, &f.tenant, &f.user, ask("What was the release status?", Some(f.node_scope()), None, Some(t.clone()), None)).await.unwrap();
    assert_eq!(first_ask.payload["temporal"], json!(t));
    assert_eq!(values(&first_ask.payload["temporal_state"]), ["green"]);
    assert_eq!(first_ask.payload["answer_status"], "supported");
    let conv = first_ask.payload["conversation_id"].clone();
    let follow = scoped_ask::execute(&state, &f.tenant, &f.user, ask("And who decided it?", None, Some(conv.clone()), None, None)).await.unwrap();
    assert_eq!(follow.payload["temporal"], json!(t), "follow-up inherits the historical cutoff");
    assert_eq!(follow.payload["temporal_changed"], false);
    let clear = scoped_ask::execute(&state, &f.tenant, &f.user, ask("What now?", None, Some(conv.clone()), Some(TemporalSelection::default()), None)).await;
    assert_eq!(clear.err().unwrap().status(), StatusCode::CONFLICT, "clearing a cutoff requires replace");
    let replaced = scoped_ask::execute(&state, &f.tenant, &f.user, ask("What now?", None, Some(conv.clone()), Some(TemporalSelection::default()), Some("replace"))).await.unwrap();
    assert_eq!(replaced.payload["temporal"]["mode"], "ordinary");
    assert_eq!(replaced.payload["temporal_changed"], true);
    assert_eq!(replaced.payload["previous_temporal"], json!(t));
    // Records exist but no state applies before declared validity: unresolved, no prose.
    let unresolved = scoped_ask::execute(&state, &f.tenant, &f.user, ask("Status in 2025?", Some(f.node_scope()), None, Some(f.historical("2025-06-01", between)), None)).await.unwrap();
    assert_eq!(unresolved.payload["answer_status"], "temporal_unresolved");
    assert_eq!(unresolved.payload["temporal_state"]["status"], "unknown");
    assert_eq!(unresolved.payload["text"], "");
    // The question snapshot keeps the selection it was asked under.
    let snaps: Vec<Option<Value>> = sqlx::query_scalar("SELECT temporal_selection FROM conversation_messages WHERE conversation_id=$1 AND role='user' ORDER BY seq")
        .bind(Uuid::parse_str(conv.as_str().unwrap()).unwrap()).fetch_all(&f.owner).await.unwrap();
    assert_eq!(snaps, vec![Some(json!(t)), Some(json!(t)), Some(json!(TemporalSelection::default()))]);
    // Conversation read echoes the per-turn temporal state.
    let read = coralswarm_backend::test_conversation_read(f.state(), f.tenant.clone(), f.user.clone(), Uuid::parse_str(conv.as_str().unwrap()).unwrap()).await;
    let (s, conv_body) = body(read).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(conv_body["conversation"]["temporal_selection"]["mode"], "ordinary");
    assert!(conv_body.to_string().contains("recorded_evidence_state"));

    // Receipts bind every temporal input; P8-style revocation invalidates them.
    let (early,): (Vec<Uuid>,) = sqlx::query_as("SELECT event_ids FROM answer_temporal_receipts WHERE receipt_id=$1")
        .bind(first_ask.receipt_id).fetch_one(&f.owner).await.unwrap();
    assert_eq!(early, vec![first_id], "only evidence known at the cutoff enters the receipt");
    let current = scoped_ask::execute(&state, &f.tenant, &f.user, ask("What is recorded for April?", Some(f.node_scope()), None, Some(f.historical("2026-04-01", f.now().await)), None)).await.unwrap();
    assert_eq!(current.payload["temporal_state"]["status"], "disputed");
    let receipt: Uuid = current.receipt_id;
    let (events, support): (Vec<Uuid>, Value) = sqlx::query_as("SELECT t.event_ids,s.support FROM answer_temporal_receipts t JOIN answer_support_receipts s USING(receipt_id) WHERE receipt_id=$1")
        .bind(receipt).fetch_one(&f.owner).await.unwrap();
    assert_eq!(events.len(), 3, "assertion, transition and dissent are all bound inputs, active or not");
    let passages = support.as_array().unwrap().iter().filter(|s| s["kind"] == "source_passage").count();
    assert_eq!(passages, 3, "every temporal input's exact passage is in the server receipt: {support}");
    assert!(f.permitted(receipt).await);
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1").bind(b.item).execute(&f.owner).await.unwrap();
    assert!(!f.permitted(receipt).await, "revoked temporal input must invalidate the saved answer");
    // Revoked evidence is not resurrected by an as-known query; its dependent is visibly gone.
    let now = f.now().await;
    let (_, v) = f.read_state(f.node_scope(), f.historical("2026-04-01", now)).await;
    assert!(!v.to_string().contains("after the audit"), "revoked passage must not leak");
    assert!(!values(&v["temporal_state"]).contains(&"red".to_string()));
    sqlx::query("UPDATE source_items SET access_state='permitted' WHERE source_item_id=$1").bind(b.item).execute(&f.owner).await.unwrap();
    // Rejecting the root assertion makes the dependent transition explicitly incomplete.
    sqlx::query("UPDATE temporal_evidence_records SET lifecycle='rejected' WHERE event_id=$1").bind(first_id).execute(&f.owner).await.unwrap();
    assert!(!f.permitted(receipt).await, "rejected temporal input must invalidate the saved answer");
    let (_, v) = f.read_state(f.node_scope(), f.historical("2026-04-01", now)).await;
    assert_eq!(v["temporal_state"]["status"], "incomplete");
    let resurrect = sqlx::query("UPDATE temporal_evidence_records SET lifecycle='supported' WHERE event_id=$1").bind(first_id).execute(&f.owner).await;
    assert!(resurrect.is_err(), "rejected evidence cannot be silently restored");
    f.done().await;
}

#[tokio::test]
async fn consumer_recorded_temporal_state_is_bitemporal_scoped_and_revocable() {
    run(false).await;
}
#[tokio::test]
async fn tenant_recorded_temporal_state_is_bitemporal_scoped_and_revocable() {
    run(true).await;
}
