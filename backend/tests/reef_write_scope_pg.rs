//! Reef WRITE scope (settings 2.10): who may confirm, reject, edit, merge,
//! supersede or review a reef, through MCP and REST.
//!
//! Before this suite, the node-anchored writers (`confirm_reef`,
//! `reject_reef`, `update_reef`, `PATCH /v1/nodes/{id}`, `POST
//! /v1/nodes/{id}/confirm|reject`) authorized on `graphs.account_id =
//! caller's account` alone. In an org every ocean shares that account, so any
//! member could rewrite any reef in the org: the commons, and another member's
//! private ocean if they knew the reef id. The tenant `member_ocean_isolation`
//! RLS policy does not help in production, where the tenant connection is the
//! table owner.
//!
//! The rule this suite pins (`routes::nodes::ReefWriter`):
//!
//! * Everyone may write their own writable private ocean — exactly
//!   `ingest::is_own_writable_ocean`, the predicate `add_context`
//!   already enforces: a `personal` ocean, or the caller's own unarchived
//!   `member` ocean.
//! * MCP stops there. Shared oceans are populated through `promote` only.
//! * REST additionally lets an org owner/admin moderate a shared (`org` /
//!   `team`) ocean they can reach. Nobody, admin included, writes another
//!   member's private ocean.
//! * A refusal is indistinguishable from "no such reef".
//!
//! Real router, real Clerk-shaped tokens, separate control and tenant
//! Postgres databases. Gated on `ORGT_TEST_BASE_URL` (a base connection with
//! CREATE DATABASE rights); every test returns early when it is unset. Wired
//! into CI's `cross-tenant-probe` job.
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test reef_write_scope_pg -- --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const TEST_MASTER_KEK_HEX: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";

/// Serializes the process-global env vars both tests set.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin_conn(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}
async fn recreate(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}
fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks_body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local JWKS listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

struct Env {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl Env {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: every env mutation in this binary is serialized by ENV_LOCK,
        // held in `_guard` until Drop.
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MCP_RESOURCE_URL", "http://localhost/mcp");
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
            std::env::remove_var("MCP_WRITE_FOR_EVERYONE");
            std::env::remove_var("MCP_DEFAULT_SCOPES");
        }
        Self { _guard: guard }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        // SAFETY: ENV_LOCK is still held by `_guard`.
        unsafe {
            for k in [
                "MCP_ENABLED",
                "CLERK_ISSUER",
                "CLERK_JWKS_URL",
                "MCP_RESOURCE_URL",
                "RATE_LIMIT_BACKEND",
                "MASTER_KEK_HEX",
            ] {
                std::env::remove_var(k);
            }
        }
    }
}

// ── Tokens ───────────────────────────────────────────────────────────────────

/// OAuth access token for `/mcp`, carrying read+write product scopes. The
/// per-(user, account) grant row is seeded separately; both are required.
fn mcp_token(sub: &str, org: Option<&str>) -> String {
    let mut claims = json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "reef_write_scope_client", "jti": Uuid::new_v4().to_string(),
        "scope": "coralswarm:read coralswarm:write",
    });
    if let Some(org) = org {
        claims["org_id"] = json!(org);
        claims["org_role"] = json!("org:member");
    }
    mint_oauth(claims)
}

/// Clerk session token for the REST API. `org_role` must agree with the
/// seeded `account_members.role`: membership is re-provisioned from claims.
fn rest_token(sub: &str, org: Option<(&str, &str)>) -> String {
    let mut claims = json!({"iss": ISSUER, "sub": sub, "exp": future_exp(), "sid": "sess_rws"});
    if let Some((org, role)) = org {
        claims["org_id"] = json!(org);
        claims["org_role"] = json!(role);
    }
    mint(claims)
}

// ── Transport ────────────────────────────────────────────────────────────────

fn parse_sse_json(bytes: &[u8]) -> Value {
    let text = String::from_utf8_lossy(bytes);
    let data: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data:"))
        .map(|r| r.strip_prefix(' ').unwrap_or(r))
        .collect();
    serde_json::from_str(&data.join("\n"))
        .unwrap_or_else(|e| panic!("unparseable SSE JSON-RPC payload: {e}\n{text}"))
}

/// `tools/call` → `(is_error, tool body)`.
async fn call_tool(app: &axum::Router, token: &str, name: &str, args: Value) -> (bool, Value) {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                      "params": {"name": name, "arguments": args}});
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", APEX)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
    let rpc = if ct.contains("text/event-stream") {
        parse_sse_json(&bytes)
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    assert_eq!(status, StatusCode::OK, "tools/call transport failure for {name}: {rpc:?}");
    let result = rpc.get("result").unwrap_or_else(|| panic!("no result for {name}: {rpc:?}"));
    let is_error = result.get("isError").and_then(Value::as_bool).unwrap_or(false);
    let text = result["content"][0]["text"].as_str().expect("text content");
    (is_error, serde_json::from_str(text).expect("tool text is JSON"))
}

async fn rest(
    app: &axum::Router,
    token: &str,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("host", APEX)
        .header("authorization", format!("Bearer {token}"));
    let body = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(b.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

// ── Seeding ──────────────────────────────────────────────────────────────────

async fn seed_user(pool: &PgPool, label: &str) -> (Uuid, String) {
    let sub = format!("rws_{label}_{}", Uuid::new_v4().simple());
    let id: Uuid =
        sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{sub}@test.local"))
            .bind(&sub)
            .fetch_one(pool)
            .await
            .expect("seed user");
    (id, sub)
}

/// Org memberships carry a seat tier; personal ones must not
/// (`account_members_seat_tier_iff_org`).
async fn seed_membership(pool: &PgPool, account: Uuid, user: Uuid, role: &str) {
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         SELECT $1, $2, $3, CASE WHEN a.clerk_org_id IS NULL THEN NULL \
                ELSE (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active) END \
         FROM accounts a WHERE a.id = $1",
    )
    .bind(account)
    .bind(user)
    .bind(role)
    .execute(pool)
    .await
    .expect("seed membership");
}

async fn grant_mcp_write(pool: &PgPool, user: Uuid, account: Uuid) {
    sqlx::query(
        "INSERT INTO user_account_prefs (user_id, account_id, mcp_write_enabled) VALUES ($1, $2, true) \
         ON CONFLICT (user_id, account_id) DO UPDATE SET mcp_write_enabled = true",
    )
    .bind(user)
    .bind(account)
    .execute(pool)
    .await
    .expect("seed MCP write grant");
}

async fn seed_graph(pool: &PgPool, account: Uuid, kind: &str, owner: Option<Uuid>) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(account)
    .bind(format!("{kind} ocean {}", Uuid::new_v4().simple()))
    .bind(kind)
    .bind(owner)
    .bind(kind != "org")
    .fetch_one(pool)
    .await
    .expect("seed graph")
}

/// An entity reef with one supporting inline coral, so `GET /v1/nodes/{id}`'s
/// library admission (`library_node_permitted`) can read it.
async fn seed_entity(pool: &PgPool, graph: Uuid, title: &str) -> Uuid {
    let node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph)
    .bind(title)
    .fetch_one(pool)
    .await
    .expect("seed entity");
    let atom: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                            \"timestamp\", content_inline) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3) RETURNING atom_id",
    )
    .bind(graph)
    .bind(format!("seed-{node}"))
    .bind(format!("a note about {title}"))
    .fetch_one(pool)
    .await
    .expect("seed coral");
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(node)
        .bind(atom)
        .execute(pool)
        .await
        .expect("link coral");
    node
}

async fn seed_pending_decision(pool: &PgPool, graph: Uuid, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'decision', $2, \
                 jsonb_build_object('review_status', 'pending', 'decided_at', now()::text)) \
         RETURNING node_id",
    )
    .bind(graph)
    .bind(title)
    .fetch_one(pool)
    .await
    .expect("seed pending decision")
}

async fn seed_suggested_edge(pool: &PgPool, graph: Uuid, src: Uuid, dst: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO relationship_edge (graph_id, src_node_id, predicate, dst_node_id, origin, \
                                        status, confidence, provenance) \
         VALUES ($1, $2, 'works_on', $3, 'inferred', 'suggested', 0.6, '{}') RETURNING edge_id",
    )
    .bind(graph)
    .bind(src)
    .bind(dst)
    .fetch_one(pool)
    .await
    .expect("seed suggested edge")
}

/// The fields a refused write must leave untouched.
async fn node_state(pool: &PgPool, node: Uuid) -> (String, Option<String>, i32) {
    sqlx::query_as("SELECT title, body->>'review_status', version FROM nodes WHERE node_id = $1")
        .bind(node)
        .fetch_one(pool)
        .await
        .expect("node state")
}

async fn edge_status(pool: &PgPool, edge: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM relationship_edge WHERE edge_id = $1")
        .bind(edge)
        .fetch_one(pool)
        .await
        .expect("edge status")
}

async fn supersession_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM supersession_edge")
        .fetch_one(pool)
        .await
        .expect("supersession count")
}

fn build_app(control: &PgPool, tenant_dsn: Option<(&str, String)>) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    let pairs: Vec<(&str, String)> = tenant_dsn.into_iter().collect();
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(pairs)))),
    ));
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// The one denial shape every MCP write tool returns: `not_found`, never a
/// code that would confirm the reef exists somewhere the caller cannot write.
fn assert_mcp_denied(label: &str, (is_error, body): (bool, Value)) {
    assert!(is_error, "{label}: must be refused: {body:?}");
    assert_eq!(body["error"]["code"], "not_found", "{label}: {body:?}");
}

// ── Org plane ────────────────────────────────────────────────────────────────

#[allow(dead_code)]
struct Ocean {
    graph: Uuid,
    entity: Uuid,
    entity2: Uuid,
    decision: Uuid,
    decision2: Uuid,
    edge: Uuid,
}

async fn seed_ocean(pool: &PgPool, account: Uuid, kind: &str, owner: Option<Uuid>, label: &str) -> Ocean {
    let graph = seed_graph(pool, account, kind, owner).await;
    let entity = seed_entity(pool, graph, &format!("{label} entity")).await;
    let entity2 = seed_entity(pool, graph, &format!("{label} entity two")).await;
    let decision = seed_pending_decision(pool, graph, &format!("{label} decision")).await;
    let decision2 = seed_pending_decision(pool, graph, &format!("{label} decision two")).await;
    let edge = seed_suggested_edge(pool, graph, entity, entity2).await;
    Ocean { graph, entity, entity2, decision, decision2, edge }
}
struct OrgFx {
    _env: Env,
    base: String,
    dbs: [&'static str; 2],
    control: PgPool,
    tenant: PgPool,
    app: axum::Router,
    commons: Ocean,
    a: Ocean,
    b: Ocean,
    adm: Ocean,
    a_mcp: String,
    admin_mcp: String,
    a_rest: String,
    admin_rest: String,
}

/// Org fixture: the commons plus one private ocean each for members A and B
/// and an org admin, every ocean holding entities, pending decisions and a
/// suggested edge. `None` when `ORGT_TEST_BASE_URL` is unset.
async fn org_fixture(ctl: &'static str, ten: &'static str, org_id: &'static str) -> Option<OrgFx> {
    let base = base_url()?;
    let _env = Env::enable().await;
    let mut conn = admin_conn(&base).await;
    for db in [ctl, ten] {
        recreate(&mut conn, db).await;
    }
    // ── Control: org account, routing row, three members (A, B, admin). ──────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    let (owner, _) = seed_user(&control, "owner").await;
    let account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(org_id)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        account,
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
             VALUES ($1, $2, $3, $4, 'active')",
        )
        .bind(org_id)
        .bind(account)
        .bind(org_id.replace('_', "-"))
        .bind(ten),
    )
    .await
    .unwrap();
    let (user_a, sub_a) = seed_user(&control, "a").await;
    let (user_b, _sub_b) = seed_user(&control, "b").await;
    let (user_admin, sub_admin) = seed_user(&control, "admin").await;
    seed_membership(&control, account, user_a, "member").await;
    seed_membership(&control, account, user_b, "member").await;
    seed_membership(&control, account, user_admin, "admin").await;
    for u in [user_a, user_b, user_admin] {
        grant_mcp_write(&control, u, account).await;
    }

    // ── Tenant: the commons + one private ocean per member. ──────────────────
    let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, ten)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&tenant)
        .await
        .unwrap();
    let commons = seed_ocean(&tenant, account, "org", None, "Commons").await;
    let a = seed_ocean(&tenant, account, "member", Some(user_a), "A").await;
    let b = seed_ocean(&tenant, account, "member", Some(user_b), "B").await;
    let adm = seed_ocean(&tenant, account, "member", Some(user_admin), "Admin").await;

    let app = build_app(&control, Some((ten, with_db(&base, ten))));
    let a_mcp = mcp_token(&sub_a, Some(org_id));
    let admin_mcp = mcp_token(&sub_admin, Some(org_id));
    let a_rest = rest_token(&sub_a, Some((org_id, "org:member")));
    let admin_rest = rest_token(&sub_admin, Some((org_id, "org:admin")));
    Some(OrgFx {
        _env, base, dbs: [ctl, ten], control, tenant, app, commons, a, b, adm,
        a_mcp, admin_mcp, a_rest, admin_rest,
    })
}

impl OrgFx {
    async fn teardown(self) {
        self.control.close().await;
        self.tenant.close().await;
        let mut conn = admin_conn(&self.base).await;
        for db in self.dbs {
            conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
        }
    }
}

#[tokio::test]
async fn mcp_reef_writes_are_limited_to_the_callers_own_private_ocean() {
    let Some(fx) = org_fixture("orgt_rws_mcp_ctl", "orgt_rws_mcp_ten", "org_rws_mcp").await else {
        eprintln!("skipping mcp_reef_writes_are_limited_to_the_callers_own_private_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let OrgFx { app, tenant, commons, a, b, adm, a_mcp, admin_mcp, .. } = &fx;
    // ═══ MCP: member A ═══════════════════════════════════════════════════════
    //
    // B's private ocean and the commons are both refused, with the SAME code a
    // nonexistent reef gets, and nothing is written.
    for (label, target) in [("B's private ocean", &b), ("the commons", &commons)] {
        let before = (
            node_state(&tenant, target.decision).await,
            node_state(&tenant, target.entity).await,
        );
        assert_mcp_denied(
            &format!("confirm_reef on {label}"),
            call_tool(&app, &a_mcp, "confirm_reef", json!({"reef_id": target.decision, "title": "Hijacked"})).await,
        );
        assert_mcp_denied(
            &format!("reject_reef on {label}"),
            call_tool(&app, &a_mcp, "reject_reef", json!({"reef_id": target.decision})).await,
        );
        assert_mcp_denied(
            &format!("update_reef on {label}"),
            call_tool(&app, &a_mcp, "update_reef", json!({"reef_id": target.entity, "title": "Hijacked"})).await,
        );
        let after = (
            node_state(&tenant, target.decision).await,
            node_state(&tenant, target.entity).await,
        );
        assert_eq!(before, after, "a refused MCP write on {label} must change nothing");
    }
    // Non-leak: an id that exists nowhere gets the identical answer.
    let (_, ghost) = call_tool(&app, &a_mcp, "confirm_reef", json!({"reef_id": Uuid::new_v4()})).await;
    let (_, foreign) = call_tool(&app, &a_mcp, "confirm_reef", json!({"reef_id": b.decision})).await;
    assert_eq!(ghost, foreign, "a reef in B's ocean must be indistinguishable from no reef at all");
    let (_, ghost) = call_tool(&app, &a_mcp, "update_reef", json!({"reef_id": Uuid::new_v4(), "title": "x"})).await;
    let (_, foreign) = call_tool(&app, &a_mcp, "update_reef", json!({"reef_id": b.entity, "title": "x"})).await;
    assert_eq!(ghost, foreign, "update_reef must not distinguish B's reef from a missing one");

    // A's own private ocean still works.
    let (is_error, body) = call_tool(&app, &a_mcp, "confirm_reef", json!({"reef_id": a.decision})).await;
    assert!(!is_error, "A may confirm in A's own ocean: {body:?}");
    assert_eq!(node_state(&tenant, a.decision).await.1.as_deref(), Some("confirmed"));
    let (is_error, body) = call_tool(&app, &a_mcp, "reject_reef", json!({"reef_id": a.decision2})).await;
    assert!(!is_error, "A may reject in A's own ocean: {body:?}");
    assert_eq!(node_state(&tenant, a.decision2).await.1.as_deref(), Some("rejected"));
    let (is_error, body) =
        call_tool(&app, &a_mcp, "update_reef", json!({"reef_id": a.entity, "title": "A renamed"})).await;
    assert!(!is_error, "A may update in A's own ocean: {body:?}");
    assert_eq!(node_state(&tenant, a.entity).await.0, "A renamed");

    // ═══ MCP: an org admin is held to the same private-only rule ═════════════
    let before = node_state(&tenant, commons.entity).await;
    assert_mcp_denied(
        "admin update_reef on the commons via MCP",
        call_tool(&app, &admin_mcp, "update_reef", json!({"reef_id": commons.entity, "title": "x"})).await,
    );
    assert_mcp_denied(
        "admin confirm_reef on the commons via MCP",
        call_tool(&app, &admin_mcp, "confirm_reef", json!({"reef_id": commons.decision})).await,
    );
    assert_mcp_denied(
        "admin update_reef on B's private ocean via MCP",
        call_tool(&app, &admin_mcp, "update_reef", json!({"reef_id": b.entity, "title": "x"})).await,
    );
    assert_eq!(node_state(&tenant, commons.entity).await, before);
    let (is_error, body) =
        call_tool(&app, &admin_mcp, "update_reef", json!({"reef_id": adm.entity, "title": "Admin renamed"})).await;
    assert!(!is_error, "the admin may update their own private ocean: {body:?}");

    fx.teardown().await;
}

#[tokio::test]
async fn rest_reef_writes_allow_own_ocean_and_admin_moderation_of_the_commons() {
    let Some(fx) = org_fixture("orgt_rws_rest_ctl", "orgt_rws_rest_ten", "org_rws_rest").await else {
        eprintln!("skipping rest_reef_writes_allow_own_ocean_and_admin_moderation_of_the_commons: set ORGT_TEST_BASE_URL");
        return;
    };
    let OrgFx { app, tenant, commons, a, b, a_rest, admin_rest, .. } = &fx;
    // ═══ REST: member A ══════════════════════════════════════════════════════
    for (label, target) in [("B's private ocean", &b), ("the commons", &commons)] {
        let before = (
            node_state(&tenant, target.decision).await,
            node_state(&tenant, target.entity).await,
            edge_status(&tenant, target.edge).await,
        );
        let (s, body) = rest(&app, &a_rest, "PATCH", &format!("/v1/nodes/{}", target.entity),
                             Some(json!({"title": "Hijacked"}))).await;
        assert!(matches!(s, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND), "PATCH {label}: {s} {body:?}");
        let (s, body) = rest(&app, &a_rest, "POST", &format!("/v1/nodes/{}/confirm", target.decision),
                             Some(json!({}))).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "confirm {label}: {body:?}");
        let (s, body) = rest(&app, &a_rest, "POST", &format!("/v1/nodes/{}/reject", target.decision),
                             Some(json!({}))).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "reject {label}: {body:?}");
        let (s, body) = rest(&app, &a_rest, "POST", &format!("/v1/relationships/{}/confirm", target.edge), None).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "relationship confirm {label}: {body:?}");
        let (s, body) = rest(&app, &a_rest, "POST", &format!("/v1/nodes/{}/supersede", target.entity),
                             Some(json!({"superseded_by": target.entity2}))).await;
        assert_eq!(s, StatusCode::NOT_FOUND, "supersede {label}: {body:?}");
        let after = (
            node_state(&tenant, target.decision).await,
            node_state(&tenant, target.entity).await,
            edge_status(&tenant, target.edge).await,
        );
        assert_eq!(before, after, "a refused REST write on {label} must change nothing");
    }
    assert_eq!(supersession_count(&tenant).await, 0, "no refused supersession may land");

    // The reef page's `can_edit` reports exactly what PATCH will do.
    let (s, body) = rest(&app, &a_rest, "GET", &format!("/v1/nodes/{}", commons.entity), None).await;
    assert_eq!(s, StatusCode::OK, "A can still READ the commons: {body:?}");
    assert_eq!(body["can_edit"], json!(false), "a plain member cannot edit the commons: {body:?}");
    let (s, body) = rest(&app, &a_rest, "GET", &format!("/v1/nodes/{}", a.entity2), None).await;
    assert_eq!(s, StatusCode::OK, "{body:?}");
    assert_eq!(body["can_edit"], json!(true), "A can edit A's own reef: {body:?}");

    // A's own ocean through REST.
    let (s, body) = rest(&app, &a_rest, "PATCH", &format!("/v1/nodes/{}", a.entity2),
                         Some(json!({"title": "A renamed via REST"}))).await;
    assert_eq!(s, StatusCode::OK, "A may PATCH A's own reef: {body:?}");
    let (s, body) = rest(&app, &a_rest, "POST", &format!("/v1/relationships/{}/confirm", a.edge), None).await;
    assert_eq!(s, StatusCode::OK, "A may review A's own edge: {body:?}");

    // ═══ REST: the org admin moderates the commons, never B's ocean ══════════
    let (s, body) = rest(&app, &admin_rest, "GET", &format!("/v1/nodes/{}", commons.entity), None).await;
    assert_eq!(s, StatusCode::OK, "{body:?}");
    assert_eq!(body["can_edit"], json!(true), "an admin can moderate the commons: {body:?}");
    let (s, body) = rest(&app, &admin_rest, "PATCH", &format!("/v1/nodes/{}", commons.entity),
                         Some(json!({"title": "Commons renamed by admin"}))).await;
    assert_eq!(s, StatusCode::OK, "admin PATCH on the commons: {body:?}");
    let (s, body) = rest(&app, &admin_rest, "POST", &format!("/v1/nodes/{}/confirm", commons.decision),
                         Some(json!({}))).await;
    assert_eq!(s, StatusCode::OK, "admin confirm on the commons: {body:?}");
    let (s, body) = rest(&app, &admin_rest, "POST", &format!("/v1/nodes/{}/reject", commons.decision2),
                         Some(json!({}))).await;
    assert_eq!(s, StatusCode::OK, "admin reject on the commons: {body:?}");
    let (s, body) = rest(&app, &admin_rest, "POST", &format!("/v1/relationships/{}/reject", commons.edge), None).await;
    assert_eq!(s, StatusCode::OK, "admin relationship review on the commons: {body:?}");
    let (s, body) = rest(&app, &admin_rest, "POST", &format!("/v1/nodes/{}/supersede", commons.entity),
                         Some(json!({"superseded_by": commons.entity2}))).await;
    assert_eq!(s, StatusCode::OK, "admin supersession on the commons: {body:?}");
    assert_eq!(node_state(&tenant, commons.entity).await.0, "Commons renamed by admin");
    assert_eq!(node_state(&tenant, commons.decision).await.1.as_deref(), Some("confirmed"));
    assert_eq!(node_state(&tenant, commons.decision2).await.1.as_deref(), Some("rejected"));
    assert_eq!(edge_status(&tenant, commons.edge).await, "rejected");

    let before = (node_state(&tenant, b.entity).await, node_state(&tenant, b.decision).await);
    let (s, body) = rest(&app, &admin_rest, "PATCH", &format!("/v1/nodes/{}", b.entity),
                         Some(json!({"title": "Hijacked"}))).await;
    assert!(matches!(s, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND), "admin PATCH on B's ocean: {s} {body:?}");
    let (s, body) = rest(&app, &admin_rest, "POST", &format!("/v1/nodes/{}/confirm", b.decision),
                         Some(json!({}))).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "admin confirm on B's ocean: {body:?}");
    assert_eq!(before, (node_state(&tenant, b.entity).await, node_state(&tenant, b.decision).await));

    fx.teardown().await;
}

// ── Personal plane ───────────────────────────────────────────────────────────

#[tokio::test]
async fn personal_account_reef_writes_are_unchanged() {
    let Some(base) = base_url() else {
        eprintln!("skipping personal_account_reef_writes_are_unchanged: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = Env::enable().await;
    let ctl = "orgt_rws_personal_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin_conn(&base).await;
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let (user, sub) = seed_user(&control, "personal").await;
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user)
            .fetch_one(&control)
            .await
            .unwrap();
    seed_membership(&control, account, user, "owner").await;
    grant_mcp_write(&control, user, account).await;
    let ocean = seed_ocean(&control, account, "personal", None, "Personal").await;
    // A stranger's personal ocean on the same plane.
    let (stranger, _) = seed_user(&control, "stranger").await;
    let stranger_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(stranger)
            .fetch_one(&control)
            .await
            .unwrap();
    let other = seed_ocean(&control, stranger_account, "personal", None, "Stranger").await;

    let app = build_app(&control, None);
    let mcp = mcp_token(&sub, None);
    let rest_tok = rest_token(&sub, None);

    let (is_error, body) = call_tool(&app, &mcp, "confirm_reef", json!({"reef_id": ocean.decision})).await;
    assert!(!is_error, "personal confirm_reef: {body:?}");
    let (is_error, body) = call_tool(&app, &mcp, "reject_reef", json!({"reef_id": ocean.decision2})).await;
    assert!(!is_error, "personal reject_reef: {body:?}");
    let (is_error, body) =
        call_tool(&app, &mcp, "update_reef", json!({"reef_id": ocean.entity, "title": "Mine"})).await;
    assert!(!is_error, "personal update_reef: {body:?}");
    assert_eq!(node_state(&control, ocean.decision).await.1.as_deref(), Some("confirmed"));
    assert_eq!(node_state(&control, ocean.decision2).await.1.as_deref(), Some("rejected"));
    assert_eq!(node_state(&control, ocean.entity).await.0, "Mine");

    let (s, body) = rest(&app, &rest_tok, "PATCH", &format!("/v1/nodes/{}", ocean.entity2),
                         Some(json!({"title": "Mine via REST"}))).await;
    assert_eq!(s, StatusCode::OK, "personal PATCH: {body:?}");
    let (s, body) = rest(&app, &rest_tok, "GET", &format!("/v1/nodes/{}", ocean.entity2), None).await;
    assert_eq!(s, StatusCode::OK, "{body:?}");
    assert_eq!(body["can_edit"], json!(true), "{body:?}");
    let (s, body) = rest(&app, &rest_tok, "POST", &format!("/v1/relationships/{}/confirm", ocean.edge), None).await;
    assert_eq!(s, StatusCode::OK, "personal relationship review: {body:?}");
    let (s, body) = rest(&app, &rest_tok, "POST", &format!("/v1/nodes/{}/supersede", ocean.entity),
                         Some(json!({"superseded_by": ocean.entity2}))).await;
    assert_eq!(s, StatusCode::OK, "personal supersession: {body:?}");

    // Cross-account isolation is unchanged too.
    assert_mcp_denied(
        "update_reef on a stranger's personal ocean",
        call_tool(&app, &mcp, "update_reef", json!({"reef_id": other.entity, "title": "x"})).await,
    );
    let (s, _) = rest(&app, &rest_tok, "POST", &format!("/v1/nodes/{}/confirm", other.decision),
                      Some(json!({}))).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {ctl} WITH (FORCE)").as_str()).await.ok();
}
