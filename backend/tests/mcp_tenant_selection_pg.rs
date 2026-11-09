//! Postgres integration tests for claim-based tenancy §Phase 3 — originally
//! PR 3.2's `list_tenants`/`select_tenant` MCP tools and their real-request
//! wiring into `tenant_selection::resolve_selected_tenant`; **updated for PR
//! 3.3**, which (a) retires the org MCP resource (`McpConfig::
//! resolve_resource`'s `Subdomain` arm now collapses to `Personal`, so the
//! org-subdomain address this file's tests used to carve out as "a
//! completely separate, untouched arm" no longer exists as a distinct arm at
//! all) and (b) adds the decision-5 catalogue gate (`server::CoralSwarmMcp::
//! tool_catalog_restricted_to_tenancy_tools`): a caller with ≥1 live org
//! membership and no active selection now has every non-tenancy tool refused
//! outright (`tenant_selection_required`), not merely content-empty.
//!
//! Proves, through the REAL public HTTP router (real RS256 tokens against a
//! local JWKS listener, real `mcp_auth` → `resolve_mcp_tenancy` →
//! `resolve_personal_tenancy` → `tenant_selection::resolve_request_tenancy`
//! → rmcp dispatch) against real Postgres databases (control/consumer +
//! tenant A + tenant B):
//!
//! * `list_tenants` lists exactly the caller's OWN live, non-revoked org
//!   memberships (never another user's) — role included, name null (no
//!   `CLERK_SECRET_KEY` in test env, so `name_lookup_unavailable` is set);
//! * `select_tenant` refuses an org the caller does not currently belong to,
//!   and refuses without ever persisting a row;
//! * pre-selection, with ≥1 live org membership, every non-tenancy tool
//!   (`list_oceans`, etc.) is refused with `tenant_selection_required` — PR
//!   3.3's decision-5 gate, checked BEFORE `select_tenant` accepts anything;
//! * `select_tenant` accepts a real membership, and the VERY NEXT call on the
//!   PERSONAL/apex host — same token, no `org_id` claim, no Host change —
//!   resolves onto that org's TENANT pool (content + `mcp_tool_calls` audit
//!   land there, not on the control/consumer pool);
//! * removing the membership AFTER selecting causes the NEXT call to 403
//!   ("selected organization no longer includes this member") — the fresh
//!   live-membership re-check, never a cached/stale selection;
//! * `select_tenant` with `org_id` omitted clears the selection and reverts
//!   the very next call to `StoredSelection::None` — which, if the caller
//!   still has ≥1 live org membership and NO consent-screen org claim,
//!   immediately re-triggers the SAME decision-5 gate (a documented
//!   consequence of `StoredSelection` having no third "explicitly personal"
//!   state, not a bug introduced here). For a caller whose TOKEN names an
//!   organization (#825), that same call is REFUSED instead — clearing the
//!   row would just let the claim decide again, so reporting `"cleared"`
//!   would be reporting an outcome it cannot deliver
//!   (`clearing_a_selection_is_refused_when_the_token_itself_names_an_org`);
//! * **the org MCP resource is retired**: a request to `<slug>.<apex>` is no
//!   longer a distinct arm at all — it resolves through the EXACT SAME
//!   path as the apex (in fact that Host is 410 Gone before `/mcp` ever runs
//!   at all — see `mcp_tenant_pg.rs`), so the Host is irrelevant;
//! * **precedence between the two things that CAN name a tenant** (#825): a
//!   consent-screen `org_id` claim binds its own org when no selection row
//!   exists, and a stored selection overrules that claim once one does
//!   (`a_consent_claim_decides_until_a_stored_selection_overrules_it`);
//! * a selection naming an org whose OWN routing row is anything other than
//!   `Active` — missing, provisioning, suspended, or offboarded — is REFUSED
//!   (403/503) and NEVER falls through to the personal plane, across every
//!   non-`Active` `tenancy::gate::RoutingState` variant, exhaustively by
//!   construction (`selected_org_with_a_non_active_routing_row_is_refused_
//!   never_personal`). This is the specific hazard `tenancy::gate::decide`'s
//!   own Apex+Org(Missing) comment names: "NONE of them may fall through to
//!   the consumer plane, because reachability there has no account filter at
//!   all" — that comment now also names this PR's selection-sourced path.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same scratch-DB fixture as `mcp_tenant_pg.rs`; a plain
//! `cargo test` with no DB configured skips every test in this file.
//!
//! ## Running
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test mcp_tenant_selection_pg -- --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::gate::RoutingState;
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

/// Serializes tests in this file against the process-global env vars `McpEnv`
/// sets — same rationale as `mcp_tenant_pg.rs`'s `ENV_LOCK`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

// ── Scratch-DB helpers (pattern: mcp_tenant_pg.rs) ───────────────────────────

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
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

// ── Env + JWKS fixture (pattern: mcp_tenant_pg.rs) ───────────────────────────

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

struct McpEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl McpEnv {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: every env mutation across this test binary is serialized by
        // `ENV_LOCK`, whose guard is held in `self._guard` for the lifetime of
        // this `McpEnv` (through `Drop` below). No other thread reads or writes
        // these vars concurrently, so the process-global mutation is sound.
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MCP_RESOURCE_URL", "http://localhost/mcp");
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
            std::env::remove_var("MCP_DEFAULT_SCOPES");
            // Never configured in this file — `list_tenants` must degrade to
            // `name_lookup_unavailable: true` rather than erroring or
            // fabricating a name (see the module doc).
            std::env::remove_var("CLERK_SECRET_KEY");
        }
        Self { _guard: guard }
    }
}

impl Drop for McpEnv {
    fn drop(&mut self) {
        // SAFETY: see `McpEnv::enable` — `self._guard` still holds `ENV_LOCK`.
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MCP_RESOURCE_URL");
            std::env::remove_var("RATE_LIMIT_BACKEND");
            std::env::remove_var("MASTER_KEK_HEX");
        }
    }
}

// ── Token minting ────────────────────────────────────────────────────────────

/// OAuth-shaped PERSONAL access token: `client_id` + `jti`, no `sid`, no
/// `org_id` — the exact shape every `/mcp` caller has after claim-based
/// tenancy §Phase 3 PR 3.3 drops `user:org:read` (not yet dropped in THIS
/// PR, but this file's whole point is proving the selection path works for
/// a caller who never had an org claim at all).
fn personal_token(sub: &str, client_id: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": client_id, "jti": Uuid::new_v4().to_string(),
    }))
}

// ── JSON-RPC plumbing (pattern: mcp_tenant_pg.rs) ────────────────────────────

fn parse_sse_json(bytes: &[u8]) -> Value {
    let text = String::from_utf8_lossy(bytes);
    let mut data_lines = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    let joined = data_lines.join("\n");
    serde_json::from_str(&joined)
        .unwrap_or_else(|e| panic!("failed to parse SSE JSON-RPC payload: {e}\nraw body: {text}"))
}

/// A Host on the PERSONAL plane: allowlisted (`McpConfig::allowed_hosts`
/// always contains loopback) but not a tenant subdomain, so `resolve_resource`
/// classifies it as the personal resource — the ONLY resource
/// `resolve_personal_tenancy`'s stored-selection lookup ever runs on.
const PERSONAL_HOST: &str = "localhost";

async fn mcp_rpc(
    app: &axum::Router,
    host: &str,
    token: &str,
    method: &str,
    params: Value,
) -> (StatusCode, Value) {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", host)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
    let parsed = if content_type.contains("text/event-stream") {
        parse_sse_json(&bytes)
    } else if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, parsed)
}

async fn call_tool(
    app: &axum::Router,
    host: &str,
    token: &str,
    name: &str,
    arguments: Value,
) -> (bool, Value) {
    let (status, rpc) = mcp_rpc(
        app,
        host,
        token,
        "tools/call",
        json!({ "name": name, "arguments": arguments }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "tools/call transport failure for {name}: {rpc:?}");
    let result = rpc
        .get("result")
        .unwrap_or_else(|| panic!("no result in tools/call response for {name}: {rpc:?}"));
    let is_error = result.get("isError").and_then(|v| v.as_bool()).unwrap_or(false);
    let content = result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool result has no text content for {name}: {result:?}"));
    let body: Value = serde_json::from_str(content)
        .unwrap_or_else(|e| panic!("tool result text is not JSON for {name}: {e}\n{content}"));
    (is_error, body)
}

fn error_code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or("")
}

fn error_message(body: &Value) -> &str {
    body["error"]["message"].as_str().unwrap_or("")
}

// ── Fixture seeding (pattern: mcp_tenant_pg.rs) ──────────────────────────────

async fn seed_user(pool: &PgPool, sub: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
        .bind(format!("{sub}@users.clerk.local"))
        .bind(sub)
        .fetch_one(pool)
        .await
        .expect("seed user")
}

async fn seed_org_account(pool: &PgPool, owner: Uuid, clerk_org_id: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(clerk_org_id)
    .fetch_one(pool)
    .await
    .expect("seed org account")
}

async fn seed_membership(pool: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(role)
    .execute(pool)
    .await
    .expect("seed membership");
}

async fn delete_membership(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
    sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("delete membership");
}

async fn seed_routing_row(
    pool: &PgPool,
    clerk_org_id: &str,
    account_id: Uuid,
    slug: &str,
    secret_ref: &str,
    status: &str,
) {
    financial_fixture::execute(
        pool,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(clerk_org_id)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref)
        .bind(status),
    )
    .await
    .expect("seed tenant_databases row");
}

async fn seed_graph_with_atom(
    pool: &PgPool,
    account_id: Uuid,
    kind: &str,
    is_primary: bool,
    name: &str,
    content: &str,
) -> (Uuid, Uuid) {
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .bind(kind)
    .bind(is_primary)
    .fetch_one(pool)
    .await
    .expect("seed graph");
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
         participants, timestamp) \
         VALUES ($1, 'note', $2, 'unused', $3, '{}', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(content)
    .fetch_one(pool)
    .await
    .expect("seed atom");
    (graph_id, atom_id)
}

async fn selection_row_org(pool: &PgPool, user_id: Uuid, client_id: &str) -> Option<String> {
    sqlx::query_scalar(
        "SELECT clerk_org_id FROM mcp_tenant_selection WHERE user_id = $1 AND client_id = $2",
    )
    .bind(user_id)
    .bind(client_id)
    .fetch_optional(pool)
    .await
    .expect("read mcp_tenant_selection")
}

/// Poll (bounded) an `mcp_tool_calls` count for `account_id` on `pool` —
/// audit inserts are fire-and-forget `tokio::spawn`s, so rows can land
/// slightly after the tool response returns.
async fn poll_audit_count(pool: &PgPool, account_id: Uuid, expect_at_least: i64) -> i64 {
    let mut count = 0;
    for _ in 0..100 {
        count = sqlx::query_scalar("SELECT count(*) FROM mcp_tool_calls WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap();
        if count >= expect_at_least {
            return count;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    count
}

// ── The suite ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn selection_lifecycle_lists_selects_routes_revokes_and_clears() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping selection_lifecycle_lists_selects_routes_revokes_and_clears: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = McpEnv::enable().await;

    let (ctl, ta) = ("orgt_sel32_ctl", "orgt_sel32_tenant_a");
    let mut conn = admin(&base).await;
    for db in [ctl, ta] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB ────────────────────────────────────────────────────────────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("sel32_member_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;
    let org_a_account = seed_org_account(&control, user, "org_sel32_a").await;
    seed_membership(&control, org_a_account, user, "member").await;
    seed_routing_row(&control, "org_sel32_a", org_a_account, "acme-sel32", "SECRET_SEL32_TA", "active").await;

    // A second, real org the caller does NOT belong to — proves `select_tenant`
    // refuses on a real-but-foreign org, not merely a made-up id.
    let other_user = seed_user(&control, &format!("sel32_other_{}", Uuid::new_v4())).await;
    let org_foreign = seed_org_account(&control, other_user, "org_sel32_foreign").await;
    seed_membership(&control, org_foreign, other_user, "owner").await;

    // A third org the caller has a live `account_members` row for, but which
    // is WHOLESALE-REVOKED (`org_revocations`) — proves `list_tenants` applies
    // the same revocation guard `live_org_membership` applies, not just a bare
    // `account_members` join.
    let org_revoked = seed_org_account(&control, user, "org_sel32_revoked").await;
    seed_membership(&control, org_revoked, user, "member").await;
    sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)")
        .bind("org_sel32_revoked")
        .execute(&control)
        .await
        .expect("seed org_revocations");

    // ── Tenant DB ─────────────────────────────────────────────────────────────
    let tenant_a: PgPool = PgPoolOptions::new().connect(&with_db(&base, ta)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_a).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_sel32_a')")
        .execute(&tenant_a)
        .await
        .unwrap();
    let (graph_a, _atom_a) = seed_graph_with_atom(
        &tenant_a,
        org_a_account,
        "org",
        false,
        "Acme org ocean",
        "the quokka roadmap ships in march",
    )
    .await;

    // ── AppState ──────────────────────────────────────────────────────────────
    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_SEL32_TA",
        with_db(&base, ta),
    )]))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let client_id = "sel32_client";
    let token = personal_token(&sub, client_id);

    // ── 1. list_tenants: exactly this caller's org, name null (no Clerk
    //    secret in test env), name_lookup_unavailable set. ─────────────────────
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_tenants", json!({})).await;
    assert!(!is_error, "list_tenants must not tool-error: {body:?}");
    let orgs = body["organizations"].as_array().expect("organizations array");
    assert_eq!(orgs.len(), 1, "list_tenants must list exactly the caller's own org: {orgs:?}");
    assert_eq!(orgs[0]["org_id"], "org_sel32_a");
    assert_eq!(orgs[0]["role"], "member");
    assert!(orgs[0]["name"].is_null(), "no Clerk secret configured — name must be null, not fabricated");
    assert_eq!(body["name_lookup_unavailable"], true);
    // Well under LIST_TENANTS_ORG_CAP — must never be reported as truncated.
    assert!(body.get("truncated").is_none(), "1 membership must never read as truncated: {body:?}");
    // The foreign org (a real org, just not this caller's) must never appear.
    assert!(
        !orgs.iter().any(|o| o["org_id"] == "org_sel32_foreign"),
        "list_tenants leaked a foreign org: {orgs:?}"
    );
    // The revoked org — a real `account_members` row for THIS caller — must
    // also be excluded (the wholesale-revocation guard, not just a raw join).
    assert!(
        !orgs.iter().any(|o| o["org_id"] == "org_sel32_revoked"),
        "list_tenants listed a wholesale-revoked org: {orgs:?}"
    );

    // ── 2. select_tenant on a real org the caller is NOT a member of: refused,
    //    and nothing is persisted. ───────────────────────────────────────────
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({"org_id": "org_sel32_foreign"})).await;
    assert!(is_error, "select_tenant must refuse a foreign org: {body:?}");
    assert_eq!(error_code(&body), "invalid_params");
    assert!(
        selection_row_org(&control, user, client_id).await.is_none(),
        "a refused select_tenant must never persist a row"
    );

    // ── 2b. select_tenant with an org_id over the cap: refused before any
    //    membership check or persistence. Asserts the MESSAGE text, not just
    //    the error code: "not a member" (the live-membership rejection) and
    //    "at most N chars" (the cap rejection) share the same
    //    `invalid_params` code, so a code-only assertion would stay green
    //    even with the cap check deleted (the oversized string would simply
    //    fail the membership check instead, for the wrong reason). ──────────
    let oversized = "x".repeat(200);
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({"org_id": oversized})).await;
    assert!(is_error, "select_tenant must refuse an oversized org_id: {body:?}");
    assert_eq!(error_code(&body), "invalid_params");
    assert!(
        error_message(&body).contains("chars"),
        "an oversized org_id must be refused for its LENGTH, not fall through to a membership \
         rejection that happens to share the same error code: {body:?}"
    );

    // ── 3. Before any selection: claim-based-tenancy §Phase 3 PR 3.3, decision
    //    5's catalogue gate now REFUSES a non-tenancy tool outright — this
    //    caller has ≥1 live org membership (org_sel32_a) and no active
    //    selection, so `list_oceans` is not merely content-empty, it is not
    //    CALLABLE at all until the caller chooses. (Before PR 3.3, this step
    //    asserted the opposite: `list_oceans` succeeded, resolving to the
    //    PERSONAL plane. `server::CoralSwarmMcp::tool_catalog_restricted_to_
    //    tenancy_tools` is what changed that — see its own doc.) ─────────────
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_oceans", json!({})).await;
    assert!(is_error, "list_oceans must now be refused pre-selection: {body:?}");
    assert_eq!(error_code(&body), "tenant_selection_required");

    // The `tools/list` catalogue itself must be narrowed to EXACTLY the two
    // tenancy tools — not merely that calling something else errors.
    let (status, rpc) = mcp_rpc(&app, PERSONAL_HOST, &token, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK, "tools/list transport must succeed: {rpc:?}");
    let mut names: Vec<&str> = rpc["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().expect("tool name"))
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["list_tenants", "select_tenant"],
        "pre-selection catalogue must be exactly the two tenancy tools: {rpc:?}"
    );

    // ── 4. select_tenant on the REAL membership: accepted, persisted. ────────
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({"org_id": "org_sel32_a"})).await;
    assert!(!is_error, "select_tenant must accept a real live membership: {body:?}");
    assert_eq!(body["status"], "selected");
    assert_eq!(
        selection_row_org(&control, user, client_id).await.as_deref(),
        Some("org_sel32_a"),
        "select_tenant must persist the selection row"
    );

    // ── 4b. Re-selecting the SAME org from the SAME (user, client_id) is an
    //    UPSERT, not a duplicate-key error — the migration's PK is exactly
    //    this pair, so a second selection call must REPLACE, never conflict. ──
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({"org_id": "org_sel32_a"})).await;
    assert!(!is_error, "re-selecting the same org must not error: {body:?}");
    let row_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM mcp_tenant_selection WHERE user_id = $1 AND client_id = $2",
    )
    .bind(user)
    .bind(client_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(row_count, 1, "re-selecting must UPSERT, never create a second row for the same (user, client_id)");

    // ── 5. THE CORE PROPERTY: the very next call, same token (still no org_id
    //    claim), same PERSONAL host, now resolves onto the ORG's tenant pool —
    //    content AND its mcp_tool_calls audit row land there, not on control. ──
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_oceans", json!({})).await;
    assert!(!is_error, "list_oceans must not error post-selection: {body:?}");
    let text = body.to_string();
    assert!(
        text.contains(&graph_a.to_string()),
        "post-selection, list_oceans must reach the SELECTED org's tenant ocean: {text}"
    );
    let tenant_audit = poll_audit_count(&tenant_a, org_a_account, 1).await;
    assert!(tenant_audit >= 1, "post-selection tool calls must audit on the TENANT pool");
    // The org's OWN account_id must never appear in the control/consumer
    // pool's audit table at all — every `mcp_tool_calls` row for it belongs
    // on the tenant pool exclusively. (Step 3's PRE-selection `list_oceans`
    // call legitimately audited under the caller's PERSONAL account_id on
    // control — this checks the ORG account specifically, not "any account
    // other than org_a_account", which that earlier row would wrongly trip.)
    let control_audit: i64 = sqlx::query_scalar("SELECT count(*) FROM mcp_tool_calls WHERE account_id = $1")
        .bind(org_a_account)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        control_audit, 0,
        "a selected caller's list_oceans audit row must never land on the control/consumer pool"
    );

    // ── 6. Revoke the membership AFTER selecting: the NEXT call re-checks live
    //    membership FRESH (never trusts the stored row alone) and 403s. ───────
    delete_membership(&control, org_a_account, user).await;
    let (status, body) = mcp_rpc(&app, PERSONAL_HOST, &token, "tools/call", json!({"name": "list_oceans", "arguments": {}})).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a selection whose live membership was just revoked must 403 on the very next call: {body:?}"
    );
    assert_eq!(body["error"], "selected organization no longer includes this member");

    // Restore membership so the clear-selection step below exercises a normal
    // "user chooses to go back to personal" flow, not a revoked one.
    seed_membership(&control, org_a_account, user, "member").await;

    // ── 7. select_tenant with org_id omitted clears the selection; the next
    //    call reverts to `StoredSelection::None` (personal). ─────────────────
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({})).await;
    assert!(!is_error, "clearing a selection must not error: {body:?}");
    assert_eq!(body["status"], "cleared");
    assert!(selection_row_org(&control, user, client_id).await.is_none());

    // **Known consequence of decision 5's catalogue gate, not a bug**:
    // `StoredSelection::None` is the SAME storage-level state whether the
    // caller never selected at all, or explicitly cleared back to personal —
    // there is no third "explicitly personal" state (`tenant_selection::
    // StoredSelection` doc). Since `user` still has a live org membership
    // (restored on step 6), clearing lands back in "≥1 org membership, no
    // active selection" — exactly step 3's gate — so `list_oceans` is
    // refused again here too, not silently re-admitted to the personal
    // plane. A caller who wants to STAY on personal after this must call
    // `list_tenants`/`select_tenant` again on their next real turn; this is
    // an accepted trade-off of the current `(user_id, client_id)` schema
    // (PR 3.2), not something PR 3.3 introduces new content-safety risk
    // from — `list_tenants`/`select_tenant` remain unconditionally callable
    // regardless.
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_oceans", json!({})).await;
    assert!(
        is_error,
        "list_oceans must be refused again after clearing, for the same reason as step 3: {body:?}"
    );
    assert_eq!(error_code(&body), "tenant_selection_required");
}

/// **Claim-based-tenancy §Phase 3 PR 3.3 retires the org MCP resource**:
/// `McpConfig::resolve_resource`'s `Subdomain` arm now collapses to
/// `Personal`, so `<slug>.<apex>` is no longer a distinct arm of
/// `resolve_mcp_tenancy` at all — it resolves through the EXACT SAME
/// stored-selection path as the apex. This replaces
/// `org_subdomain_address_is_unaffected_by_the_new_selection_path` (PR 3.2's
/// proof that the OPPOSITE was true: a claim-sourced org token against
/// `<slug>.<apex>` bypassed the selection mechanism entirely, resolving
/// straight off its own claim) with the new truth: **the Host is
/// irrelevant; only the stored selection is truth.**
///
/// **Why `PERSONAL_HOST`, not org_a's own (former) subdomain.** This test's
/// original shape called from `org_host("acme-sel32b")` — org_a's own
/// address — specifically to prove that even THAT host couldn't override a
/// selection naming org_b. That address is no longer reachable for `/mcp` AT
/// ALL as of this same PR's rebase onto `main`: `retired_org_host_guard`
/// (claim-based tenancy Phase 4 cutover step 3, #807) now 410s any
/// single-label subdomain host before `/mcp` ever runs, because this PR's
/// item 3 deletes the `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS` exemption that used
/// to let `/mcp` through. `PERSONAL_HOST` (an ordinary, ADMITTED, non-subdomain
/// host) is the strongest shape still available to make the same point: a
/// claim naming org_a, presented on a host that is neither org_a's nor
/// org_b's, is STILL overridden by a stored selection naming org_b.
/// **Renamed** from
/// `claim_and_host_are_both_irrelevant_only_the_stored_selection_decides`.
/// The Host half is still irrelevant and still asserted. The CLAIM half
/// stopped being true with #825: with no stored row, the token's own
/// consent-screen `org_id` claim now decides — that is the first assertion
/// below, and the regression test for the bug this PR fixes. What survives,
/// and is the reason this test still exists, is PRECEDENCE: once a row
/// exists, the row wins and the claim is ignored.
#[tokio::test]
async fn a_consent_claim_decides_until_a_stored_selection_overrules_it() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping a_consent_claim_decides_until_a_stored_selection_overrules_it: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = McpEnv::enable().await;

    let (ctl, ta, tb) = ("orgt_sel32b_ctl", "orgt_sel32b_tenant_a", "orgt_sel32b_tenant_b");
    let mut conn = admin(&base).await;
    for db in [ctl, ta, tb] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    // The caller is a live member of BOTH orgs — org_a (whose FORMER address
    // this test calls throughout, and whose claim the token below carries)
    // and org_b (never named by the Host OR the claim — only ever reached
    // via a stored `select_tenant`).
    let sub = format!("sel32b_member_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;
    let org_a_account = seed_org_account(&control, user, "org_sel32b_a").await;
    seed_membership(&control, org_a_account, user, "member").await;
    seed_routing_row(&control, "org_sel32b_a", org_a_account, "acme-sel32b", "SECRET_SEL32B_TA", "active").await;
    let org_b_account = seed_org_account(&control, user, "org_sel32b_b").await;
    seed_membership(&control, org_b_account, user, "member").await;
    seed_routing_row(&control, "org_sel32b_b", org_b_account, "beta-sel32b", "SECRET_SEL32B_TB", "active").await;

    let tenant_a: PgPool = PgPoolOptions::new().connect(&with_db(&base, ta)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_a).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_sel32b_a')")
        .execute(&tenant_a)
        .await
        .unwrap();
    let (graph_a, _atom_a) =
        seed_graph_with_atom(&tenant_a, org_a_account, "org", false, "Acme org ocean", "unrelated content").await;

    let tenant_b: PgPool = PgPoolOptions::new().connect(&with_db(&base, tb)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_b).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_sel32b_b')")
        .execute(&tenant_b)
        .await
        .unwrap();
    let (graph_b, _atom_b) =
        seed_graph_with_atom(&tenant_b, org_b_account, "org", false, "Beta org ocean", "also unrelated content").await;

    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([
        ("SECRET_SEL32B_TA", with_db(&base, ta)),
        ("SECRET_SEL32B_TB", with_db(&base, tb)),
    ]))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let client_id = "sel32b_client";
    // Carries org_a's OWN claim — the exact shape a not-yet-expired,
    // pre-cutover grant still sends. `McpConfig::resolve_resource` strips
    // this before it ever reaches `resolve_mcp_tenancy` (see that
    // function's doc on why the `Org` arm it used to drive is now
    // unreachable).
    let token = mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": client_id, "jti": Uuid::new_v4().to_string(),
        "org_id": "org_sel32b_a", "org_role": "org:member",
    }));

    // ── #825 regression: no stored row, and the token names org_a. ─────────
    //
    // Before this PR the claim was stripped at the door, the decision stayed
    // `Personal`, and decision 5's catalogue gate refused `list_oceans` with
    // `tenant_selection_required` — asking the user, in-band, to pick the org
    // they had already picked on the consent screen. Now the claim decides.
    assert!(selection_row_org(&control, user, client_id).await.is_none());

    // The catalogue is the FULL roster, not the two tenancy tools: the gate
    // fires only on a `Personal` decision, and this call resolves to `Org`.
    let (status, listed) = mcp_rpc(&app, PERSONAL_HOST, &token, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK, "tools/list must succeed: {listed:?}");
    let names: Vec<String> = listed["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        names.len() > 2 && names.iter().any(|n| n == "list_oceans"),
        "a consent-bound caller must see the full roster, not just the tenancy tools: {names:?}"
    );

    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_oceans", json!({})).await;
    assert!(
        !is_error,
        "list_oceans must succeed pre-selection when the token itself names org_a: {body:?}"
    );
    let text = body.to_string();
    assert!(
        text.contains(&graph_a.to_string()),
        "the consent-screen claim must bind org_a's OWN tenant ocean: {text}"
    );
    assert!(
        !text.contains(&graph_b.to_string()),
        "and never org_b's, which nothing has named yet: {text}"
    );

    // `select_tenant` selects ORG_B — proving both that the tenancy tools are
    // reachable regardless of the (irrelevant) host and of the org_a claim,
    // and that the selection it persists is exactly what the caller asked
    // for, not something derived from either.
    let (is_error, body) = call_tool(
        &app,
        PERSONAL_HOST,
        &token,
        "select_tenant",
        json!({"org_id": "org_sel32b_b"}),
    )
    .await;
    assert!(!is_error, "select_tenant must succeed regardless of the caller's org_a claim: {body:?}");
    assert_eq!(
        selection_row_org(&control, user, client_id).await.as_deref(),
        Some("org_sel32b_b"),
        "the persisted selection must be exactly what was asked for — org_b, never org_a"
    );

    // THE CORE PROPERTY (precedence): the very next `list_oceans`, with the
    // SAME org_a-claiming token, resolves onto ORG_B's tenant ocean — never
    // org_a's, even though the claim names org_a and just decided this same
    // call moments ago. An explicit in-band selection OUTRANKS the consent
    // screen; the claim only ever fills the hole where no row exists.
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_oceans", json!({})).await;
    assert!(!is_error, "list_oceans must succeed post-selection: {body:?}");
    let text = body.to_string();
    assert!(
        text.contains(&graph_b.to_string()),
        "must resolve to the SELECTED org (b)'s tenant ocean: {text}"
    );
    assert!(
        !text.contains(&graph_a.to_string()),
        "must NEVER resolve to org_a's ocean, despite the Host and the claim both naming it: {text}"
    );
}

/// Two DIFFERENT users sharing the SAME OAuth `client_id` (a plausible real
/// shape — one corporate MCP client installed by many users) must NEVER share
/// a selection: `mcp_tenant_selection` is keyed `(user_id, client_id)`, and
/// every lookup MUST filter on both. If a lookup ever dropped the `user_id`
/// half of that key, user B would silently inherit user A's org selection.
#[tokio::test]
async fn same_client_id_different_users_never_share_a_selection() {
    let Some(base) = base_url() else {
        eprintln!("skipping same_client_id_different_users_never_share_a_selection: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let (ctl, ta) = ("orgt_sel32c_ctl", "orgt_sel32c_tenant_a");
    let mut conn = admin(&base).await;
    for db in [ctl, ta] {
        let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub_a = format!("sel32c_a_{}", Uuid::new_v4());
    let user_a = seed_user(&control, &sub_a).await;
    let org_account = seed_org_account(&control, user_a, "org_sel32c").await;
    seed_membership(&control, org_account, user_a, "member").await;
    seed_routing_row(&control, "org_sel32c", org_account, "acme-sel32c", "SECRET_SEL32C_TA", "active").await;

    // user_b has NO membership in org_sel32c at all.
    let sub_b = format!("sel32c_b_{}", Uuid::new_v4());
    let user_b = seed_user(&control, &sub_b).await;

    let tenant_a: PgPool = PgPoolOptions::new().connect(&with_db(&base, ta)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_a).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_sel32c')")
        .execute(&tenant_a)
        .await
        .unwrap();
    let (graph_a, _atom_a) =
        seed_graph_with_atom(&tenant_a, org_account, "org", false, "Acme org ocean", "shared-client isolation content")
            .await;

    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_SEL32C_TA",
        with_db(&base, ta),
    )]))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // The SAME client_id for both users — the shared-corporate-app shape.
    let shared_client_id = "shared_corp_client";
    let token_a = personal_token(&sub_a, shared_client_id);
    let token_b = personal_token(&sub_b, shared_client_id);

    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token_a, "select_tenant", json!({"org_id": "org_sel32c"})).await;
    assert!(!is_error, "user_a's select_tenant must succeed: {body:?}");

    // user_a's selection row exists, keyed to user_a specifically.
    assert_eq!(
        selection_row_org(&control, user_a, shared_client_id).await.as_deref(),
        Some("org_sel32c")
    );
    // user_b has NO row at all under the same client_id.
    assert!(selection_row_org(&control, user_b, shared_client_id).await.is_none());

    // user_b, same client_id, still resolves to THEIR OWN (personal) plane —
    // never user_a's selected org.
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token_b, "list_oceans", json!({})).await;
    assert!(!is_error, "user_b's list_oceans must not error: {body:?}");
    assert!(
        !body.to_string().contains(&graph_a.to_string()),
        "user_b must NEVER inherit user_a's org selection via a shared client_id: {body}"
    );

    // And user_a's own next call is still correctly bound to the org.
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token_a, "list_oceans", json!({})).await;
    assert!(!is_error, "user_a's list_oceans must not error: {body:?}");
    assert!(body.to_string().contains(&graph_a.to_string()), "user_a must still resolve their own selection: {body}");
}

/// A personal token with a stored org selection writes into the selected
/// org's tenant plane. Write is always on (settings launch plan decision 6),
/// so no grant row exists or is needed; the test pins that the selection-routed
/// path reaches the tenant pool and the write lands, with the account resync
/// in `mcp_auth` pointing the member-ocean lookup at the ORG's account.
#[tokio::test]
async fn write_through_a_selected_org_needs_no_grant_row() {
    let Some(base) = base_url() else {
        eprintln!("skipping write_through_a_selected_org_needs_no_grant_row: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let (ctl, ta) = ("orgt_sel32d_ctl", "orgt_sel32d_tenant_a");
    let mut conn = admin(&base).await;
    for db in [ctl, ta] {
        let _database_cleanup_3 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("sel32d_member_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;
    let org_account = seed_org_account(&control, user, "org_sel32d").await;
    seed_membership(&control, org_account, user, "member").await;
    seed_routing_row(&control, "org_sel32d", org_account, "acme-sel32d", "SECRET_SEL32D_TA", "active").await;
    // No write-grant row exists anywhere: write is gated by the server ceiling
    // and token scope only.

    let tenant_a: PgPool = PgPoolOptions::new().connect(&with_db(&base, ta)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_a).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_sel32d')")
        .execute(&tenant_a)
        .await
        .unwrap();
    // A member-private ocean, seeded directly rather than via the
    // MEMBER_OCEANS_ENABLED first-sight trigger (not set in this file's env),
    // so `add_context`'s omitted-`ocean_id` default resolution
    // (`resolve_primary_ocean`'s precedence 1: `owner_user_id == caller`,
    // `is_primary`, not archived) has somewhere to land.
    sqlx::query(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
         VALUES ($1, 'Member ocean', 'member', $2, true)",
    )
    .bind(org_account)
    .bind(user)
    .execute(&tenant_a)
    .await
    .expect("seed member ocean");
    // Org DEK (pattern: member_ocean_probe_pg.rs) — `account_keys` is
    // tenant-only; without it `add_context` 503s with "encryption service
    // unavailable" before write governance is ever reached.
    let kek = coralswarm_core::kek::load_kek().expect("test KEK loads");
    let org_dek = coralswarm_core::kek::generate_dek();
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(org_account)
        .bind(coralswarm_core::kek::wrap_dek(&kek, &org_dek).unwrap())
        .execute(&tenant_a)
        .await
        .unwrap();

    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_SEL32D_TA",
        with_db(&base, ta),
    )]))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let client_id = "sel32d_client";
    let token = personal_token(&sub, client_id);

    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({"org_id": "org_sel32d"})).await;
    assert!(!is_error, "select_tenant must succeed: {body:?}");

    // add_context (a WRITE tool) must be ALLOWED with no grant row at all.
    let (is_error, body) = call_tool(
        &app,
        PERSONAL_HOST,
        &token,
        "add_context",
        json!({"content": "write through a selected org needs no grant row"}),
    )
    .await;
    assert!(!is_error, "add_context must be permitted with no grant row: {body:?}");
}

// ── The fail-closed matrix for the NEW (selection-sourced) path ─────────────
//
// `tenancy::gate::decide`'s Apex+`TokenKind::Org` arm already has exhaustive
// PURE coverage for every non-`Active` `RoutingState`
// (`gate::tests::apex_org_token_matrix_binds_only_on_an_active_row`) — but
// `decide` is token-ORIGIN-agnostic: it cannot tell a claim-sourced
// `org_claim` from a selection-sourced one, so that test proves nothing
// about whether THIS PR's wiring (`mcp::auth::resolve_personal_tenancy`)
// actually reaches `decide`/`finish_binding` for a stored selection at all,
// or whether its own `Ok(None) => user.account_id` fallback (for
// `resolve_routing_by_org` returning no row) is genuinely inert rather than
// a live foothold onto the consumer plane. The tests below drive the REAL
// public HTTP router end to end, once per non-`Active` `RoutingState`.

/// The HTTP surface a non-`Active` routing row must produce, expressed as an
/// EXHAUSTIVE match over `tenancy::gate::RoutingState` — no wildcard arm, so
/// a future variant fails to COMPILE here until this function says what it
/// means (same discipline as `tenant_selection::resolve_selected_tenant`'s
/// own exhaustive matrices, see that module's tests). `Active` is excluded
/// on purpose (`unreachable!`): the happy path is proven elsewhere
/// (`selection_lifecycle_lists_selects_routes_revokes_and_clears`), and this
/// function's whole job is the REFUSAL surface.
fn expected_refusal(routing: &RoutingState) -> (StatusCode, &'static str) {
    match routing {
        RoutingState::Missing => (StatusCode::FORBIDDEN, "organization is not provisioned for service"),
        RoutingState::Provisioning => (StatusCode::SERVICE_UNAVAILABLE, "service unavailable"),
        RoutingState::Suspended | RoutingState::Offboarded => (StatusCode::FORBIDDEN, "organization is not active"),
        RoutingState::Active(_) => {
            unreachable!("Active is the proceed case, tested elsewhere — never a refusal fixture")
        }
    }
}

/// **The highest-value test in this PR** (coordinator review, PR #806): pins
/// that a stored selection naming an org with a non-`Active` routing row is
/// refused — 403 or 503 — and NEVER resolves `TenancyDecision::Personal`.
/// Falling through to `Personal` here would land a selection-sourced caller
/// on the consumer plane with NO account filter at all — exactly the hazard
/// `tenancy::gate::decide`'s own comment names for the claim-sourced case,
/// now also reachable from this PR's selection-sourced one.
///
/// One (user, org, selection) triple per non-`Active` `RoutingState` variant,
/// enumerated the same way `expected_refusal`'s match is written (adding a
/// state there without a row here is a silent gap — but the compile-time
/// exhaustiveness on `expected_refusal` itself is the real backstop: a NEW
/// `RoutingState` variant cannot be classified there without a human
/// decision, forcing this test to be revisited too).
#[tokio::test]
async fn selected_org_with_a_non_active_routing_row_is_refused_never_personal() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping selected_org_with_a_non_active_routing_row_is_refused_never_personal: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = McpEnv::enable().await;

    let ctl = "orgt_sel32f_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_4 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    // Empty resolver: NONE of these four states may ever reach
    // `TenantRegistry::get_or_build` at all (`decide` refuses before the
    // `GateOutcome::Org` branch that calls it). An empty map makes any
    // attempt to build a pool fail (`StaticMapResolver::resolve` returns
    // `Err(SecretError::NotFound)`, never a panic) rather than silently
    // succeed — so if `decide` were ever wrongly reached here, this fixture
    // would surface as an (still-red) 503 from `GateReject::Unavailable`,
    // not a false-positive 200.
    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
        Vec::<(String, String)>::new(),
    ))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let fixtures: &[(&str, Option<&str>, RoutingState)] = &[
        ("missing", None, RoutingState::Missing),
        ("provisioning", Some("provisioning"), RoutingState::Provisioning),
        ("suspended", Some("suspended"), RoutingState::Suspended),
        ("offboarded", Some("offboarded"), RoutingState::Offboarded),
    ];

    for (label, seed_status, routing) in fixtures {
        let sub = format!("sel32f_{label}_{}", Uuid::new_v4());
        let user = seed_user(&control, &sub).await;
        let clerk_org_id = format!("org_sel32f_{label}");
        let org_account = seed_org_account(&control, user, &clerk_org_id).await;
        seed_membership(&control, org_account, user, "member").await;
        if let Some(status) = seed_status {
            seed_routing_row(
                &control,
                &clerk_org_id,
                org_account,
                &format!("slug-sel32f-{label}"),
                &format!("SECRET_SEL32F_{label}"),
                status,
            )
            .await;
        }
        // else ("missing"): deliberately NO `tenant_databases` row at all.

        let client_id = format!("sel32f_{label}_client");
        let token = personal_token(&sub, &client_id);

        // select_tenant itself checks only LIVE MEMBERSHIP
        // (`account_members`/`org_revocations`) — never the routing row's
        // status, which is an entirely separate concern (the plan's own
        // "advisory, may name a routing row that's since gone bad" model).
        // It must succeed here exactly as it would for a healthy org: the
        // refusal under test happens on the NEXT call, not at selection time.
        let (is_error, body) =
            call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({"org_id": clerk_org_id})).await;
        assert!(
            !is_error,
            "{label}: select_tenant itself must succeed — routing status is not its concern: {body:?}"
        );
        assert_eq!(
            selection_row_org(&control, user, &client_id).await.as_deref(),
            Some(clerk_org_id.as_str()),
            "{label}: select_tenant must persist despite the bad routing row"
        );

        // `tools/list` is a plain, content-free RPC — the refusal must
        // happen in `mcp_auth`'s tenancy resolution, before rmcp/any tool
        // ever dispatches, so this alone is sufficient (and side-steps
        // needing a working tenant DB/DEK for a request that must never
        // reach one).
        let (status, resp) = mcp_rpc(&app, PERSONAL_HOST, &token, "tools/list", json!({})).await;
        let (expected_status, expected_msg) = expected_refusal(routing);
        assert_eq!(
            status, expected_status,
            "{label}: wrong HTTP status for a non-Active routing row: {resp:?}"
        );
        assert_eq!(
            resp["error"], expected_msg,
            "{label}: wrong rejection body for a non-Active routing row: {resp:?}"
        );
        // THE hazard this test exists to catch, stated independently of the
        // exact status/message above: a non-Active routing row must NEVER
        // produce a bare 200 — that would mean the request silently fell
        // through to `TenancyDecision::Personal`, serving the shared
        // consumer plane with no account filter at all.
        assert_ne!(
            status,
            StatusCode::OK,
            "{label}: a non-Active routing row must NEVER resolve to Personal (200): {resp:?}"
        );
    }
}

// ── Personal organizations (control migration 0208) ──────────────────────────
//
// A personal org is a real Clerk organization that STANDS FOR one person's own
// ocean, created so Clerk's OAuth consent screen — which lists only
// organizations — has a row a user can pick for their personal ocean. It has
// no tenant database and no routing row on purpose;
// `middleware::auth::normalize_personal_org_claim` translates its id back to
// the caller's ordinary personal path before `resolve_clerk_context` runs.
//
// These two tests exist because that translation is invisible from outside
// when it works AND when it is missing — the #804 hazard. They pin the one
// behaviour that differs observably on the MCP path.

/// OAuth-shaped access token that DOES carry an `org_id` claim — the shape
/// Clerk mints once `user:org:read` is advertised again and the consent
/// screen's organization picker is back.
fn org_claim_token(sub: &str, client_id: &str, org_id: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": client_id, "jti": Uuid::new_v4().to_string(),
        "org_id": org_id, "org_role": "org:admin",
    }))
}

async fn seed_personal_org(pool: &PgPool, clerk_org_id: &str, clerk_user_id: &str) {
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
        .bind(clerk_org_id)
        .bind(clerk_user_id)
        .execute(pool)
        .await
        .unwrap();
}

/// **[SEC — HIGH] Another user's personal org is REFUSED, not quietly served
/// as your own ocean.**
///
/// This is the test that proves the translation is wired at all on the MCP
/// path, and it is the only MCP-observable difference the translation makes.
///
/// Read the failure mode carefully. With the personal-org translation absent,
/// a caller presenting somebody ELSE'S personal-org id is never classified as
/// such, and nothing downstream can tell that id apart from an ordinary
/// organization. Refusing at the door is what makes that visible.
///
/// This used to be an argument about ORDER: `claims_for_resource` stripped
/// every org claim on the personal resource, so a translation that ran second
/// left the caller served THEIR OWN ocean with a `200` on it — no error, no
/// log, no way to tell the organization they picked was not the one they got.
/// The strip is gone (#825), so there is no second normalization to be
/// ordered against; the refusal is now simply the only thing standing between
/// this shape and a wrong answer.
#[tokio::test]
async fn another_users_personal_org_is_refused_not_silently_served_as_your_own() {
    let Some(base) = base_url() else {
        eprintln!("skipping another_users_personal_org_is_refused_not_silently_served_as_your_own: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let ctl = "orgt_porg_refuse_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_5 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    // user_a owns personal org `org_personal_a`.
    let sub_a = format!("porg_a_{}", Uuid::new_v4());
    let _user_a = seed_user(&control, &sub_a).await;
    seed_personal_org(&control, "org_personal_a", &sub_a).await;

    // user_b owns nothing, and presents user_a's personal org id.
    let sub_b = format!("porg_b_{}", Uuid::new_v4());
    let _user_b = seed_user(&control, &sub_b).await;

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
            Vec::<(String, String)>::new(),
        )))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token_b = org_claim_token(&sub_b, "porg_client", "org_personal_a");
    let (status, body) = mcp_rpc(&app, PERSONAL_HOST, &token_b, "tools/list", json!({})).await;
    let text = body.to_string();

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "presenting another user's personal org must be refused, not served. \
         Got {status} with body: {text}"
    );
    assert!(
        text.contains("another user's personal ocean"),
        "the refusal must say WHY — a bare 403 here reads as 'org not provisioned' \
         and sends the next reader hunting for a routing row that is missing on \
         purpose. Body: {text}"
    );
}

/// Your own personal-org claim reaches your own ocean, and does not 403.
///
/// **This test got STRONGER with #825**, and the reason is worth recording.
/// It used to be weaker than the one above by construction: `/mcp` stripped
/// every org claim on the personal resource anyway, so a 200 here did not by
/// itself prove the translation had run — the strip would have produced the
/// same 200 either way. With the strip gone, an unclassified personal-org
/// claim would be an ordinary `ConsentOrg::Org`, would be handed to
/// `finish_binding` as a `TokenKind::Org`, would find the routing row a
/// personal org deliberately never has, and would 403. So a 200 here now
/// proves the translation ran, exactly as it always did on the REST path.
#[tokio::test]
async fn your_own_personal_org_claim_reaches_your_own_ocean() {
    let Some(base) = base_url() else {
        eprintln!("skipping your_own_personal_org_claim_reaches_your_own_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let ctl = "orgt_porg_own_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_6 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("porg_own_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;
    seed_personal_org(&control, "org_personal_own", &sub).await;
    // The `accounts` + `account_members` rows Clerk's `organization.created`
    // webhook writes for any new org, personal ones included. They matter now:
    // the claim is KEPT rather than stripped, so `resolve_clerk_context`
    // resolves THIS account from it. Before the shared-plane change the claim
    // was discarded and these rows were irrelevant — which is exactly why this
    // test did not seed them.
    let org_account = seed_org_account(&control, user, "org_personal_own").await;
    seed_membership(&control, org_account, user, "admin").await;
    // The caller's OLD personal account — the one a stripped claim would send
    // this request to. Without it there is nothing for the org account to be
    // distinguished FROM, and the audit assertion below proves nothing.
    let personal_account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id",
    )
    .bind(user)
    .fetch_one(&control)
    .await
    .expect("seed personal account");
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')",
    )
    .bind(personal_account)
    .bind(user)
    .execute(&control)
    .await
    .expect("seed personal membership");
    // Deliberately NO routing row: a personal org has no tenant database, and
    // `gate::decide` must serve it the shared plane anyway.

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
            Vec::<(String, String)>::new(),
        )))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token = org_claim_token(&sub, "porg_client", "org_personal_own");
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_oceans", json!({})).await;

    assert!(
        !is_error,
        "a caller presenting their OWN personal org must be served, not refused. \
         A 403 here means the personal-org claim reached `gate::decide` as an \
         ordinary `TokenKind::Org` and was refused for lacking the routing row it \
         is never supposed to have: {body}"
    );

    // THE POINT OF THE WHOLE STEP: the request must resolve to the ORG's OWN
    // account, not the caller's old personal one.
    //
    // "Not refused" alone cannot show that — stripping the claim also returns
    // 200, just against the wrong account, and on the shared plane the
    // reachability union makes both accounts' content look identical from
    // outside. The audit row is the discriminator: `mcp_tool_calls.account_id`
    // is written from `user.account_id`, which is resolved from the claim. Keep
    // the claim and it is the org's account; strip it and it is the personal
    // one.
    let on_org = poll_audit_count(&control, org_account, 1).await;
    assert!(
        on_org >= 1,
        "the call was not audited against the personal ORG's account \
         ({org_account}) — the claim was stripped somewhere and the request \
         resolved to the caller's old personal account instead"
    );
    let on_personal: i64 =
        sqlx::query_scalar("SELECT count(*) FROM mcp_tool_calls WHERE account_id = $1")
            .bind(personal_account)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        on_personal, 0,
        "nothing may be audited against the caller's OLD personal account \
         ({personal_account}) — a row here is the stripped-claim behaviour this \
         step replaced"
    );
}

/// **A stored `select_tenant` choice still wins over a personal-org claim —
/// pinned deliberately, because the combination was previously unspecified.**
///
/// Raised by review on #821: a caller can hold BOTH a personal-organization
/// claim (their own ocean, chosen on Clerk's consent screen) and a stored
/// selection naming an ordinary org. Something has to lose, and until now
/// nothing said which.
///
/// The founder's model decides it: *the picker is the front door; the tools
/// are how you change your mind.* A `select_tenant` call is a later, explicit
/// act than a consent screen clicked at connect time, so it wins. Reversing
/// this would make `select_tenant` unusable for anyone holding a personal-org
/// token — the claim would silently re-assert itself on the very next call.
///
/// The cost, accepted with eyes open: a caller who reconnects and picks "my
/// own ocean" is still served their stored org until they clear the selection
/// (`select_tenant` with `org_id` omitted). That is surprising, and it is why
/// this test exists rather than a comment.
#[tokio::test]
async fn a_stored_selection_still_wins_over_a_personal_org_claim() {
    let Some(base) = base_url() else {
        eprintln!("skipping a_stored_selection_still_wins_over_a_personal_org_claim: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let (ctl, ta) = ("orgt_porg_prec_ctl", "orgt_porg_prec_tenant_a");
    let mut conn = admin(&base).await;
    for db in [ctl, ta] {
        let _database_cleanup_7 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("porg_prec_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;

    // The caller's own personal organization (no routing row, by design).
    seed_personal_org(&control, "org_personal_prec", &sub).await;
    let porg_account = seed_org_account(&control, user, "org_personal_prec").await;
    seed_membership(&control, porg_account, user, "admin").await;

    // A REAL org they also belong to, with a tenant database behind it.
    let real_account = seed_org_account(&control, user, "org_real_prec").await;
    seed_membership(&control, real_account, user, "member").await;
    seed_routing_row(&control, "org_real_prec", real_account, "acme-prec", "SECRET_PREC_TA", "active").await;

    let tenant_a: PgPool = PgPoolOptions::new().connect(&with_db(&base, ta)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_a).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_real_prec')")
        .execute(&tenant_a)
        .await
        .unwrap();
    seed_graph_with_atom(&tenant_a, real_account, "org", false, "Acme ocean", "precedence content").await;

    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_PREC_TA",
        with_db(&base, ta),
    )]))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let client_id = "porg_prec_client";
    // Select the REAL org first, with an ordinary personal token.
    let (is_error, body) = call_tool(
        &app,
        PERSONAL_HOST,
        &personal_token(&sub, client_id),
        "select_tenant",
        json!({"org_id": "org_real_prec"}),
    )
    .await;
    assert!(!is_error, "select_tenant must succeed: {body:?}");
    assert_eq!(
        selection_row_org(&control, user, client_id).await.as_deref(),
        Some("org_real_prec")
    );

    // Now call with a token whose claim names the caller's OWN personal org.
    let token = org_claim_token(&sub, client_id, "org_personal_prec");
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_oceans", json!({})).await;
    assert!(!is_error, "the call must be served, not refused: {body}");

    // THE PINNED PROPERTY: the stored selection decided, so the caller is on
    // the REAL org's tenant plane and sees its ocean.
    //
    // Asserted on CONTENT, not on a control-plane audit row: an org-bound call
    // writes `mcp_tool_calls` to the TENANT pool (`audit.rs`), so counting rows
    // on the control plane finds nothing and proves nothing. That mistake cost
    // a debugging round here — the earlier personal-org test can use the audit
    // row precisely because its calls stay on the shared plane.
    let text = body.to_string();
    assert!(
        text.contains("Acme ocean"),
        "the stored selection must win — expected the REAL org's tenant ocean: {text}"
    );
    assert!(
        !text.contains("Personal"),
        "the personal organization's plane must not be served while a stored selection \
         exists — if this fires, the precedence flipped and `select_tenant` has become \
         unusable for anyone holding a personal-org token: {text}"
    );
}

/// OAuth-shaped access token with `org_id`, for the personal-org tests.
fn porg_claim_token(sub: &str, client_id: &str, org_id: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": client_id, "jti": Uuid::new_v4().to_string(),
        "org_id": org_id, "org_role": "org:admin",
    }))
}

/// **After the adopt tool runs, a token with NO org claim must still find the
/// caller's real content — not be handed a brand-new empty account.**
///
/// `adopt_personal_org_account` leaves a user with NO `clerk_org_id IS NULL`
/// account at all: their personal home now carries their personal
/// organization's id. `ensure_personal_account` used to look only for the
/// NULL-org shape, so it would fall through to its INSERT and create a fresh
/// account with a fresh "Personal" ocean, while the real content sat under the
/// org-labelled account.
///
/// This is not a corner case. **MCP tokens carry no `org_id` while
/// `user:org:read` is unadvertised**, so every connector call takes exactly
/// this path. The failure is silent — the caller sees an empty ocean and
/// concludes their data is gone.
#[tokio::test]
async fn a_relabelled_personal_home_is_found_without_an_org_claim() {
    let Some(base) = base_url() else {
        eprintln!("skipping a_relabelled_personal_home_is_found_without_an_org_claim: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let ctl = "orgt_porg_home_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_8 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("porg_home_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;
    seed_personal_org(&control, "org_personal_home", &sub).await;

    // The post-adopt shape: ONE account, owned by the user, carrying their
    // personal org. Deliberately NO `clerk_org_id IS NULL` account exists.
    let home = seed_org_account(&control, user, "org_personal_home").await;
    seed_membership(&control, home, user, "admin").await;
    let (home_graph, _) =
        seed_graph_with_atom(&control, home, "personal", true, "Real Ocean", "the real content").await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM accounts WHERE owner_user_id = $1 AND clerk_org_id IS NULL"
        )
        .bind(user).fetch_one(&control).await.unwrap(),
        0,
        "fixture precondition: the caller must have NO null-org account, or this proves nothing"
    );

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
            Vec::<(String, String)>::new(),
        )))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // A token with NO org claim — the shape every MCP caller has today.
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &personal_token(&sub, "porg_home_client"), "list_oceans", json!({})).await;
    assert!(!is_error, "the call must be served: {body}");
    assert!(
        body.to_string().contains(&home_graph.to_string()),
        "a no-org-claim token must resolve to the caller's REAL home ({home}) and see its \
         ocean. If this fails, `ensure_personal_account` fell through and minted a new empty \
         account — the caller's data looks gone: {body}"
    );

    // And nothing new was created.
    let extra: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts WHERE owner_user_id = $1")
        .bind(user)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        extra, 1,
        "exactly one account must exist for this user — a second one means a fresh empty \
         personal account was minted alongside their real home"
    );
}

// ── The personal-org selection lockout ───────────────────────────────────────
//
// A user could select their OWN personal organization as an MCP tenant and be
// permanently locked out. `list_tenants` offered it (its query filtered
// `clerk_org_id IS NOT NULL` and excluded `org_revocations`, but was missing
// the `personal_orgs` exclusion its direct sibling `has_any_org_membership`
// got in #824); `select_tenant` accepted it (the adopted `accounts` row makes
// `live_org_membership` answer truthfully "yes, a member"); and every request
// afterwards resolved the stored row to `TokenKind::Org`, found no routing row
// — a personal org deliberately has none — and 403'd at `gate::decide`.
//
// The 403 fires inside `mcp_auth`, BEFORE tool dispatch, so `select_tenant`
// itself became unreachable and the choice could not be undone.
//
// Unreachable while `user:org:read` was dropped; #825 re-advertised that
// scope, which is what put a personal org on the consent screen and made this
// live.

/// **`list_tenants` never offers you your own personal org — and still offers
/// somebody else's.**
///
/// The second half is not decoration. The exclusion is matched on
/// `u.id = am.user_id`, never on the org id alone, because being invited into
/// another person's personal organization is an ordinary org membership from
/// this caller's point of view. An exclusion written on the org id alone would
/// pass the first assertion here and silently delete real organizations from
/// everyone else's picker.
#[tokio::test]
async fn your_own_personal_org_is_never_offered_or_selectable() {
    let Some(base) = base_url() else {
        eprintln!("skipping your_own_personal_org_is_never_offered_or_selectable: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let ctl = "orgt_porg_offer_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_9 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    // The caller, with their own personal org in the post-adopt shape.
    let sub_a = format!("porg_offer_a_{}", Uuid::new_v4());
    let user_a = seed_user(&control, &sub_a).await;
    seed_personal_org(&control, "org_porg_mine", &sub_a).await;
    let mine = seed_org_account(&control, user_a, "org_porg_mine").await;
    seed_membership(&control, mine, user_a, "admin").await;

    // Somebody ELSE's personal org, which the caller has been invited into.
    // An ordinary organization from the caller's point of view.
    let sub_b = format!("porg_offer_b_{}", Uuid::new_v4());
    let user_b = seed_user(&control, &sub_b).await;
    seed_personal_org(&control, "org_porg_theirs", &sub_b).await;
    let theirs = seed_org_account(&control, user_b, "org_porg_theirs").await;
    seed_membership(&control, theirs, user_b, "admin").await;
    seed_membership(&control, theirs, user_a, "member").await;

    // And a plain real organization.
    let real = seed_org_account(&control, user_a, "org_porg_real").await;
    seed_membership(&control, real, user_a, "admin").await;

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
            Vec::<(String, String)>::new(),
        )))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let client = "porg_offer_client";
    let token = personal_token(&sub_a, client);

    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_tenants", json!({})).await;
    assert!(!is_error, "list_tenants must succeed: {body}");
    let listed: Vec<String> = body["organizations"]
        .as_array()
        .expect("organizations array")
        .iter()
        .map(|o| o["org_id"].as_str().unwrap_or("").to_string())
        .collect();

    assert!(
        !listed.contains(&"org_porg_mine".to_string()),
        "the caller's OWN personal org must never be offered as a selectable organization. \
         It has no tenant database by design, so selecting it 403s every later request \
         before tool dispatch — a permanent lockout. Listed: {listed:?}"
    );
    assert!(
        listed.contains(&"org_porg_real".to_string()),
        "a real organization must still be offered: {listed:?}"
    );
    assert!(
        listed.contains(&"org_porg_theirs".to_string()),
        "ANOTHER user's personal org is an ordinary organization from this caller's point of \
         view and must still be offered — an exclusion matched on the org id alone instead of \
         `u.id = am.user_id` would wrongly drop it: {listed:?}"
    );

    // Defence in depth: even passed directly, never via `list_tenants`, the
    // id is refused — and no row is written.
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({"org_id": "org_porg_mine"})).await;
    assert!(is_error, "selecting your own personal org must be refused: {body}");
    assert_eq!(error_code(&body), "invalid_params", "refusal code: {body}");
    assert!(
        error_message(&body).contains("your own personal ocean"),
        "the refusal must say WHY. A bare 'not a member' would be false — the caller IS a \
         member — and would send them hunting for a permission they already have. Got: {}",
        error_message(&body)
    );
    assert_eq!(
        selection_row_org(&control, user_a, client).await,
        None,
        "a refused selection must never persist a row: the row IS the lockout"
    );

    // The same tool still accepts another user's personal org, which is an
    // ordinary membership. Pins that the write-side refusal is scoped by
    // caller too, not by "is in personal_orgs at all".
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({"org_id": "org_porg_theirs"})).await;
    assert!(
        !is_error,
        "another user's personal org is an ordinary org membership and must stay selectable: {body}"
    );
    assert_eq!(
        selection_row_org(&control, user_a, client).await,
        Some("org_porg_theirs".to_string()),
        "the accepted selection must persist"
    );
}

/// **An ALREADY-STORED selection naming your own personal org heals, instead
/// of locking you out.**
///
/// This is the test for users who are stuck *right now*. Fixing `list_tenants`
/// and `select_tenant` closes the door, but it does not reach a row that is
/// already in the table — and by construction, every affected user has one.
/// The row here is written directly, exactly as production has it.
///
/// The assertion is deliberately the DECISION (`200`, own content, and
/// `select_tenant` reachable again), not the branch taken. A test that checked
/// "did `resolve_request_tenancy` return `OwnPersonalOrg`" would pass just as
/// happily if the outcome were a 403.
#[tokio::test]
async fn a_stored_selection_naming_your_own_personal_org_heals_instead_of_locking_you_out() {
    let Some(base) = base_url() else {
        eprintln!("skipping a_stored_selection_naming_your_own_personal_org_heals_instead_of_locking_you_out: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let ctl = "orgt_porg_heal_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_10 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("porg_heal_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;
    seed_personal_org(&control, "org_porg_stuck", &sub).await;
    let home = seed_org_account(&control, user, "org_porg_stuck").await;
    seed_membership(&control, home, user, "admin").await;
    let (home_graph, _) =
        seed_graph_with_atom(&control, home, "personal", true, "Home Ocean", "the real content").await;

    // Deliberately NO `tenant_databases` row for `org_porg_stuck` — a personal
    // org never has one (migration 0208). That absence is what produced the
    // 403 "organization is not provisioned for service".
    let routing_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM tenant_databases WHERE clerk_org_id = 'org_porg_stuck'",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        routing_rows, 0,
        "fixture precondition: a personal org must have no routing row, or this proves nothing"
    );

    // The poisoned row, exactly as a stuck user has it today.
    let client = "porg_heal_client";
    sqlx::query(
        "INSERT INTO mcp_tenant_selection (user_id, client_id, clerk_org_id) VALUES ($1, $2, $3)",
    )
    .bind(user)
    .bind(client)
    .bind("org_porg_stuck")
    .execute(&control)
    .await
    .expect("seed the poisoned selection row");

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
            Vec::<(String, String)>::new(),
        )))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));
    let token = personal_token(&sub, client);

    // 1. The request is served at all. Before the fix this was a hard 403
    //    from inside `mcp_auth`, on every single call.
    let (status, rpc) = mcp_rpc(&app, PERSONAL_HOST, &token, "tools/list", json!({})).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a stale personal-org selection must not 403. That refusal fires before tool dispatch, \
         so it takes `select_tenant` down with it and the user can never undo their own \
         choice. Body: {rpc}"
    );

    // 2. And it is served on the caller's OWN plane, with their own content —
    //    not merely "not a 403".
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "list_oceans", json!({})).await;
    assert!(!is_error, "list_oceans must be served: {body}");
    assert!(
        body.to_string().contains(&home_graph.to_string()),
        "the healed request must land on the caller's own home ({home}) and see its ocean: {body}"
    );

    // 3. The escape hatch works in band: `select_tenant` is reachable again,
    //    so the user can clear the row themselves without an operator.
    let (is_error, body) = call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({})).await;
    assert!(!is_error, "select_tenant must be reachable again to clear the stale row: {body}");
    assert_eq!(
        selection_row_org(&control, user, client).await,
        None,
        "clearing must delete the row"
    );
}

/// **With the tenancy runtime OFF, a personal-org caller still has write on
/// their own ocean.**
///
/// A personal org carries an `org_id` claim like any other Clerk org, so a
/// bare "has an org_id" test used to govern the caller's own ocean as an
/// organization and refuse writes by default. Write is now always on
/// (settings launch plan decision 6) and no longer depends on that
/// classification, but this pins the default-deployment shape
/// (`MULTITENANT_ENABLED` unset) end to end.
///
/// The assertion is the DECISION — write actually permitted.
#[tokio::test]
async fn a_personal_org_is_governed_as_personal_when_the_tenancy_runtime_is_off() {
    let Some(base) = base_url() else {
        eprintln!("skipping a_personal_org_is_governed_as_personal_when_the_tenancy_runtime_is_off: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;

    let ctl = "orgt_porg_gov_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_11 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("porg_gov_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;
    seed_personal_org(&control, "org_porg_gov", &sub).await;
    let home = seed_org_account(&control, user, "org_porg_gov").await;
    seed_membership(&control, home, user, "admin").await;
    seed_graph_with_atom(&control, home, "personal", true, "Home Ocean", "seed").await;

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    // The flag-off shape: no tenancy runtime at all, so `resolve_mcp_tenancy`
    // returns `Ok(None)`. This is the DEFAULT deployment shape.
    state.tenant_runtime = None;
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // A token whose `org_id` names the caller's OWN personal org. Never
    // stripped on the personal resource (`claims_for_resource` keeps it on
    // purpose — the id names which account to serve), so it is still present
    // when `is_org` is computed.
    let token = porg_claim_token(&sub, "porg_gov_client", "org_porg_gov");

    // `tools/list` filters write tools on exactly `auth.scopes.write` (see
    // `server.rs`'s `list_tools`), so the presence of a write tool in the
    // catalogue IS the governance decision, read directly — no dependence on
    // the rest of the write pipeline (encryption, DEKs, ocean resolution),
    // whose unrelated failures would otherwise be indistinguishable from a
    // scope denial.
    let (status, rpc) = mcp_rpc(&app, PERSONAL_HOST, &token, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK, "tools/list must be served: {rpc}");
    let tools: Vec<String> = rpc["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap_or("").to_string())
        .collect();
    assert!(
        tools.iter().any(|t| t == "add_context"),
        "a personal org is the caller's OWN ocean and must be able to write on the DEFAULT \
         configuration. Catalogue was: {tools:?}"
    );
}

/// **The operator recovery SQL published in this PR's description actually
/// works — and deletes nothing else.**
///
/// The fix above is self-healing, so no operator action is strictly required.
/// This exists anyway for two reasons. An operator may want the stale rows
/// gone rather than merely inert; and a recovery instruction that lives only
/// in prose is a claim nothing checks. This repo has been bitten by copy that
/// asserted a property the code did not have — so the one-liner is executed
/// here, verbatim, against the REAL migrated control schema.
///
/// Keep this statement and the PR/runbook copy identical. If you change one,
/// change the other.
#[tokio::test]
async fn the_documented_operator_recovery_sql_deletes_exactly_the_poisoned_rows() {
    let Some(base) = base_url() else {
        eprintln!("skipping the_documented_operator_recovery_sql_deletes_exactly_the_poisoned_rows: set ORGT_TEST_BASE_URL");
        return;
    };

    let ctl = "orgt_porg_recov_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_12 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub_a = format!("recov_a_{}", Uuid::new_v4());
    let user_a = seed_user(&control, &sub_a).await;
    seed_personal_org(&control, "org_recov_a", &sub_a).await;

    let sub_b = format!("recov_b_{}", Uuid::new_v4());
    let user_b = seed_user(&control, &sub_b).await;
    seed_personal_org(&control, "org_recov_b", &sub_b).await;

    let sub_c = format!("recov_c_{}", Uuid::new_v4());
    let user_c = seed_user(&control, &sub_c).await;

    let insert = |user: Uuid, client: &'static str, org: &'static str| {
        let pool = control.clone();
        async move {
            sqlx::query(
                "INSERT INTO mcp_tenant_selection (user_id, client_id, clerk_org_id) \
                 VALUES ($1, $2, $3)",
            )
            .bind(user)
            .bind(client)
            .bind(org)
            .execute(&pool)
            .await
            .expect("seed selection row");
        }
    };

    // Poisoned: each user selected their OWN personal org.
    insert(user_a, "cli", "org_recov_a").await;
    insert(user_b, "cli", "org_recov_b").await;
    // Must SURVIVE: a real organization.
    insert(user_a, "cli2", "org_recov_real").await;
    // Must SURVIVE: user_c selected user_b's personal org, which is an
    // ordinary organization from user_c's point of view. A recovery statement
    // matched on the org id alone instead of `u.id = s.user_id` would delete
    // this row too — silently un-selecting a tenant somebody chose on purpose.
    insert(user_c, "cli", "org_recov_b").await;

    // ── THE PUBLISHED ONE-LINER ──────────────────────────────────────────────
    let deleted = sqlx::query(
        "DELETE FROM mcp_tenant_selection s USING personal_orgs p, users u \
         WHERE s.clerk_org_id = p.clerk_org_id \
           AND u.clerk_user_id = p.clerk_user_id \
           AND u.id = s.user_id",
    )
    .execute(&control)
    .await
    .expect("recovery statement must run against the real control schema")
    .rows_affected();

    assert_eq!(deleted, 2, "the recovery statement must delete exactly the two poisoned rows");
    assert_eq!(selection_row_org(&control, user_a, "cli").await, None, "user_a's poisoned row");
    assert_eq!(selection_row_org(&control, user_b, "cli").await, None, "user_b's poisoned row");
    assert_eq!(
        selection_row_org(&control, user_a, "cli2").await,
        Some("org_recov_real".to_string()),
        "a real-org selection must survive recovery"
    );
    assert_eq!(
        selection_row_org(&control, user_c, "cli").await,
        Some("org_recov_b".to_string()),
        "another user's personal org is an ordinary org from user_c's point of view — their \
         deliberate selection must survive recovery"
    );
}

/// **`select_tenant` with no `org_id` cannot return a consent-bound
/// connection to its personal ocean, and says so instead of pretending.**
///
/// The tool's own description promises "omit org_id to clear the selection
/// and return to your personal ocean". Since #825 that promise is only
/// keepable for a token that names no organization: clearing the row for a
/// token whose consent screen DID name one just hands the decision back to
/// the claim, which binds the same organization again on the very next call.
///
/// Reporting `"status": "cleared"` for that would be a success message for an
/// unperformed action — the absence-renders-as-success failure class. So the
/// call is refused, with the only instruction that can actually work
/// (reconnect and pick a different row on the consent screen), and switching
/// between organizations is left working normally.
#[tokio::test]
async fn clearing_a_selection_is_refused_when_the_token_itself_names_an_org() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping clearing_a_selection_is_refused_when_the_token_itself_names_an_org: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = McpEnv::enable().await;

    let (ctl, ta, tb) = ("orgt_clr825_ctl", "orgt_clr825_tenant_a", "orgt_clr825_tenant_b");
    let mut conn = admin(&base).await;
    for db in [ctl, ta, tb] {
        let _database_cleanup_13 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("clr825_member_{}", Uuid::new_v4());
    let user = seed_user(&control, &sub).await;
    let org_a_account = seed_org_account(&control, user, "org_clr825_a").await;
    seed_membership(&control, org_a_account, user, "member").await;
    seed_routing_row(&control, "org_clr825_a", org_a_account, "acme-clr825", "SECRET_CLR825_TA", "active").await;
    let org_b_account = seed_org_account(&control, user, "org_clr825_b").await;
    seed_membership(&control, org_b_account, user, "member").await;
    seed_routing_row(&control, "org_clr825_b", org_b_account, "beta-clr825", "SECRET_CLR825_TB", "active").await;

    for (db, org) in [(ta, "org_clr825_a"), (tb, "org_clr825_b")] {
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org)
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }

    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([
        ("SECRET_CLR825_TA", with_db(&base, ta)),
        ("SECRET_CLR825_TB", with_db(&base, tb)),
    ]))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let client_id = "clr825_client";
    let token = org_claim_token(&sub, client_id, "org_clr825_a");

    // Switching BETWEEN organizations still works for a consent-bound caller:
    // a stored selection outranks the claim, so this is not a lockout.
    let (is_error, body) = call_tool(
        &app,
        PERSONAL_HOST,
        &token,
        "select_tenant",
        json!({"org_id": "org_clr825_b"}),
    )
    .await;
    assert!(!is_error, "select_tenant must still be able to switch organizations: {body:?}");
    assert_eq!(
        selection_row_org(&control, user, client_id).await.as_deref(),
        Some("org_clr825_b")
    );

    // Clearing is what cannot work — refused, and the row is left alone
    // rather than deleted-and-silently-re-decided.
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &token, "select_tenant", json!({})).await;
    assert!(is_error, "clearing must be refused for a consent-bound connection: {body:?}");
    assert_eq!(error_code(&body), "consent_bound_to_organization");
    assert!(
        error_message(&body).contains("org_clr825_a"),
        "the refusal must name the organization the token carries: {body:?}"
    );
    assert_eq!(
        selection_row_org(&control, user, client_id).await.as_deref(),
        Some("org_clr825_b"),
        "a refused clear must not delete the row it refused to act on"
    );

    // A caller with NO org claim is untouched: clearing still works exactly
    // as it did before #825. Same tool, same call, different token — which is
    // what makes the refusal above narrow rather than a blanket removal.
    let plain_sub = format!("clr825_plain_{}", Uuid::new_v4());
    let plain_user = seed_user(&control, &plain_sub).await;
    seed_membership(&control, org_a_account, plain_user, "member").await;
    let plain_token = personal_token(&plain_sub, "clr825_plain_client");
    let (is_error, body) = call_tool(
        &app,
        PERSONAL_HOST,
        &plain_token,
        "select_tenant",
        json!({"org_id": "org_clr825_a"}),
    )
    .await;
    assert!(!is_error, "a personal-shaped token must still be able to select: {body:?}");
    let (is_error, body) =
        call_tool(&app, PERSONAL_HOST, &plain_token, "select_tenant", json!({})).await;
    assert!(!is_error, "a personal-shaped token must still be able to clear: {body:?}");
    assert_eq!(body["status"], json!("cleared"));
    assert!(
        selection_row_org(&control, plain_user, "clr825_plain_client").await.is_none(),
        "and the row must actually be gone"
    );

    control.close().await;
    for db in [ctl, ta, tb] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
