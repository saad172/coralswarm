//! Postgres integration tests for MCP governance: the live `account_members`
//! re-check in `mcp::auth::mcp_auth`, the webhook-authoritative offboarding
//! model (migration `0081_org_membership_tombstones`: a still-valid token
//! replayed against `require_auth` must not resurrect a webhook-removed
//! membership), and (settings launch plan decision 6, PR 2.1) that MCP write
//! is ALWAYS ON: the only write gates are the server ceiling and the token's
//! `coralswarm:write` scope, and old `user_account_prefs` / `user_prefs`
//! `mcp_write_enabled = false` rows (migrations 0060/0080) block nothing.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights — same scratch-DB fixture pattern as `binding_gate_pg.rs` /
//! `reachability_pg.rs`); a plain `cargo test` with no DB configured skips
//! every test in this file.
//!
//! `mcp` is a crate-private module, so (like `mcp_pg_e2e.rs`) these tests
//! drive the REAL public HTTP router (`coralswarm_backend::test_support_app`)
//! with genuine RS256-signed tokens against a local JWKS listener. The
//! observable for "does this caller have write scope" is the `tools/list`
//! catalog: write tools (`add_context`, `confirm_reef`, ...) are filtered out
//! of the listing when the caller lacks write scope (`mcp/server.rs`).
//!
//! Tokens are minted WITHOUT a `coralswarm:*` `scope` claim unless a test says
//! otherwise (just `client_id`/`jti`, the OAuth-shaped minimum).
//!
//! `MULTITENANT_ENABLED` is never set in this file: the live membership
//! re-check holds even on the shared plane.
//!
//! Org-tenancy PR 4.2 (legacy-token fence) extends that same flag-independence
//! to `require_auth`'s own live membership re-check (`middleware::auth::
//! live_org_membership`). `tests/legacy_token_fence_pg.rs` is the dedicated
//! test file for that fence.
//!
//! ## Running
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test mcp_governance_pg -- --nocapture

use std::borrow::Cow;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

/// The version of this PR's governance migration. Tests that need to observe
/// the backfill split the control tree at this boundary.
const GOVERNANCE_MIGRATION_VERSION: i64 = 80;

/// Serializes every test in this file against the process-global env vars
/// `McpEnv` sets — same rationale (and same separate-compilation-unit
/// duplication) as `mcp_pg_e2e.rs`'s `ENV_LOCK`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

// ── Scratch-DB helpers (pattern: binding_gate_pg.rs) ─────────────────────────

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
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

async fn scratch_pool(base: &str, db: &str) -> PgPool {
    let mut conn = admin(base).await;
    recreate(&mut conn, db).await;
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(base, db))
        .await
        .expect("connect scratch DB")
}

/// Apply the control tree only UP TO (excluding) `before_version`, through the
/// real sqlx migrator (so `-- no-transaction` migrations like 0059/0074/0075
/// run correctly and the `_sqlx_migrations` ledger stays valid for the
/// follow-up full run).
async fn migrate_control_until(pool: &PgPool, before_version: i64) {
    let full = sqlx::migrate!("./migrations_control");
    let subset: Vec<sqlx::migrate::Migration> = full
        .iter()
        .filter(|m| m.version < before_version)
        .cloned()
        .collect();
    let truncated = sqlx::migrate::Migrator {
        migrations: Cow::Owned(subset),
        ..full
    };
    truncated.run(pool).await.expect("run pre-split migrations");
}

/// Apply the full control tree (ledger-aware: after `migrate_control_until`
/// this applies exactly the remaining migrations, i.e. the 0080 backfill).
async fn migrate_control_all(pool: &PgPool) {
    sqlx::migrate!("./migrations_control")
        .run(pool)
        .await
        .expect("run remaining control migrations");
}

// ── Env + router fixture (pattern: mcp_pg_e2e.rs) ────────────────────────────

const TEST_MASTER_KEK_HEX: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";

/// Raw HMAC key for the Clerk/Svix webhook signature; `McpEnv` exports it as
/// `CLERK_WEBHOOK_SECRET=whsec_<base64>` so the offboarding tests can drive
/// the REAL `/v1/webhooks/clerk` endpoint (signature-verified) rather than
/// simulating removal with raw SQL.
const WEBHOOK_RAW_KEY: &[u8] = b"mcp-governance-webhook-test-key";

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
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MCP_RESOURCE_URL", "http://localhost/mcp");
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
            {
                use base64::{engine::general_purpose::STANDARD, Engine};
                std::env::set_var(
                    "CLERK_WEBHOOK_SECRET",
                    format!("whsec_{}", STANDARD.encode(WEBHOOK_RAW_KEY)),
                );
            }
            // Flag-off is the condition under test (PR 2.2 spec item 5); make
            // it explicit even though nothing in this test binary sets it.
            std::env::remove_var("MULTITENANT_ENABLED");
            std::env::remove_var("MCP_DEFAULT_SCOPES");
        }
        Self { _guard: guard }
    }
}

impl Drop for McpEnv {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MCP_RESOURCE_URL");
            std::env::remove_var("RATE_LIMIT_BACKEND");
            std::env::remove_var("MASTER_KEK_HEX");
            std::env::remove_var("CLERK_WEBHOOK_SECRET");
        }
    }
}

fn app_for_pool(pool: PgPool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    // No Clerk key in this suite: membership webhooks use the payload-role
    // fallback, which only `APP_ENV=development` allows (settings PR 2.9).
    state.env.is_dev = true;
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    coralswarm_backend::test_support_app(Arc::new(state))
}

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

/// OAuth-shaped access token (client_id + jti, no sid) with NO `scope` claim
/// and NO org context — the personal-account default branch.
fn personal_token(sub: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "mcp_gov_client", "jti": Uuid::new_v4().to_string(),
    }))
}

/// Interactive browser-session JWT for a PERSONAL account: carries `sid`, as
/// every real Clerk browser session token does, and no org context. REST
/// team-mutation routes require an interactive session (settings launch PR
/// 2.8, `interactive_session_required`); an OAuth-shaped token like
/// [`personal_token`] (no `sid`) is correctly refused there.
fn session_personal_token(sub: &str) -> String {
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "sid": "sess_gov",
    }))
}

/// Same, but with an active-org context (claim-only org selection at `/mcp`).
fn org_token(sub: &str, org_id: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "mcp_gov_client", "jti": Uuid::new_v4().to_string(),
        "org_id": org_id, "org_role": "org:member",
    }))
}

/// Org token carrying an explicit `coralswarm:*` `scope` claim — exercises
/// `effective_scopes`' token-scope-wins branch.
fn org_token_with_scope(sub: &str, org_id: &str, scope: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "mcp_gov_client", "jti": Uuid::new_v4().to_string(),
        "scope": scope,
        "org_id": org_id, "org_role": "org:member",
    }))
}

/// A Clerk SESSION-shaped JWT (carries `sid`, no OAuth-only claims) with an
/// active-org context — what the interactive web path sends to
/// `require_auth`.
fn session_org_token(sub: &str, org_id: &str, org_role: &str) -> String {
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "sid": "sess_gov",
        "org_id": org_id, "org_role": org_role,
    }))
}

// ── require_auth + webhook plumbing ──────────────────────────────────────────

/// GET a `require_auth`-protected route with a bearer token; returns the
/// status. Used to REPLAY a still-valid token against the main API — the P0
/// resurrection vector (`resolve_clerk_context` runs in the auth middleware
/// before any handler, so even the trivial `/v1/me` exercises it fully).
async fn api_get(app: &axum::Router, path: &str, token: &str) -> StatusCode {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

/// Send a JSON-body request to a `require_auth` route; returns (status, body).
async fn api_json(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let builder = Request::builder()
        .method(method)
        .uri(path)
        .header("Authorization", format!("Bearer {token}"));
    let req = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let parsed = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, parsed)
}

/// Deliver a signed Clerk/Svix webhook event to the REAL
/// `/v1/webhooks/clerk` endpoint (signature built exactly as Svix sends it:
/// HMAC-SHA256 over `{id}.{ts}.{body}` with the `whsec_` key `McpEnv`
/// exported). Asserts the 200 ack.
async fn post_clerk_webhook(app: &axum::Router, event: Value) {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use hmac::{Hmac, Mac};

    let body = event.to_string();
    let id = "msg_gov_test";
    let ts = Utc::now().timestamp().to_string();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(WEBHOOK_RAW_KEY).unwrap();
    mac.update(format!("{id}.{ts}.{body}").as_bytes());
    let sig = format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()));

    let req = Request::builder()
        .method("POST")
        .uri("/v1/webhooks/clerk")
        .header("content-type", "application/json")
        .header("svix-id", id)
        .header("svix-timestamp", ts)
        .header("svix-signature", sig)
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "clerk webhook delivery must be acked"
    );
}

async fn org_membership_count(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(account_id)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

// ── JSON-RPC plumbing (pattern: mcp_pg_e2e.rs, trimmed to tools/list) ────────

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

async fn mcp_rpc(
    app: &axum::Router,
    token: &str,
    method: &str,
    params: Value,
) -> (StatusCode, Value) {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        // `tower::oneshot` synthesizes no Host; rmcp's DNS-rebinding guard
        // requires one (see mcp_pg_e2e.rs for the full note).
        .header("host", "localhost")
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

/// `tools/list` -> the sorted tool-name catalog. Panics on any non-200 —
/// callers asserting a rejection use `mcp_rpc` directly.
async fn list_tool_names(app: &axum::Router, token: &str) -> Vec<String> {
    let (status, rpc) = mcp_rpc(app, token, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK, "tools/list failed: {rpc:?}");
    let tools = rpc["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tools array in tools/list response: {rpc:?}"));
    let mut names: Vec<String> = tools
        .iter()
        .map(|t| t["name"].as_str().expect("tool name").to_string())
        .collect();
    names.sort();
    names
}

fn has_write_tools(names: &[String]) -> bool {
    names.iter().any(|n| n == "add_context")
}

// ── Fixture seeding (direct SQL — no provisioning side effects) ──────────────

async fn seed_user(pool: &PgPool, sub: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(format!("{sub}@users.clerk.local"))
    .bind(sub)
    .fetch_one(pool)
    .await
    .expect("seed user")
}

async fn seed_personal_account(pool: &PgPool, user_id: Uuid) -> Uuid {
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
    .expect("seed personal account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("seed personal membership");
    account_id
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

/// Seed an `account_members` row, on EITHER schema this file exercises.
///
/// Two tests here call `migrate_control_until(GOVERNANCE_MIGRATION_VERSION)`
/// (version < 80) on purpose, to drive the 0080 backfill against the
/// pre-split schema. `account_members.seat_tier_id` does not arrive until
/// `0169_account_members_seat_tier.sql` — 89 migrations later — so a helper
/// that names that column unconditionally cannot run on the truncated
/// schema, and this one used to: it failed with
/// `42703 column "seat_tier_id" of relation "account_members" does not exist`.
///
/// On the FULL schema the column must still be populated rather than skipped:
/// `0176_seat_lifecycle.sql`'s `account_members_default_seat_tier` trigger
/// raises on an org-account member insert that supplies no tier. So this
/// branches on what the database in front of it actually has, instead of
/// assuming either era.
async fn seed_membership(pool: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    let has_seat_tier: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'account_members' AND column_name = 'seat_tier_id')",
    )
    .fetch_one(pool)
    .await
    .expect("probe account_members.seat_tier_id");

    let sql = if has_seat_tier {
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))"
    } else {
        "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)"
    };

    sqlx::query(sql)
        .bind(account_id)
        .bind(user_id)
        .bind(role)
        .execute(pool)
        .await
        .expect("seed membership");
}

async fn set_legacy_flag(pool: &PgPool, user_id: Uuid, enabled: bool) {
    sqlx::query(
        "INSERT INTO user_prefs (user_id, mcp_write_enabled) VALUES ($1, $2) \
         ON CONFLICT (user_id) DO UPDATE SET mcp_write_enabled = EXCLUDED.mcp_write_enabled",
    )
    .bind(user_id)
    .bind(enabled)
    .execute(pool)
    .await
    .expect("seed user_prefs");
}

/// Seed an OLD `user_account_prefs` row (what the removed Settings toggle or
/// org-join provisioning used to write). Nothing reads it any more.
async fn set_account_grant(pool: &PgPool, user_id: Uuid, account_id: Uuid, enabled: bool) {
    sqlx::query(
        "INSERT INTO user_account_prefs (user_id, account_id, mcp_write_enabled) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (user_id, account_id) DO UPDATE \
            SET mcp_write_enabled = EXCLUDED.mcp_write_enabled, updated_at = now()",
    )
    .bind(user_id)
    .bind(account_id)
    .bind(enabled)
    .execute(pool)
    .await
    .expect("seed user_account_prefs");
}

// ── (a) Backfill scope ────────────────────────────────────────────────────────

/// Migration 0080's backfill grants EXACTLY the user's personal account:
/// legacy `user_prefs` values (true AND false) are copied onto the personal
/// account-of-one, org accounts the user belongs to get NO row, and users
/// with no legacy row get no rows at all.
#[tokio::test]
async fn backfill_grants_exactly_the_personal_account_and_nothing_else() {
    let Some(base) = base_url() else {
        eprintln!("skipping backfill_grants_exactly_the_personal_account_and_nothing_else: set ORGT_TEST_BASE_URL");
        return;
    };
    let pool = scratch_pool(&base, "orgt_mcpgov_backfill").await;
    migrate_control_until(&pool, GOVERNANCE_MIGRATION_VERSION).await;

    // u1: legacy flag ON, has a personal account AND an org membership.
    let u1 = seed_user(&pool, "gov_backfill_u1").await;
    let p1 = seed_personal_account(&pool, u1).await;
    let org = seed_org_account(&pool, u1, "org_backfill").await;
    seed_membership(&pool, org, u1, "owner").await;
    set_legacy_flag(&pool, u1, true).await;
    // u2: legacy flag explicitly OFF.
    let u2 = seed_user(&pool, "gov_backfill_u2").await;
    let p2 = seed_personal_account(&pool, u2).await;
    set_legacy_flag(&pool, u2, false).await;
    // u3: no legacy row at all.
    let u3 = seed_user(&pool, "gov_backfill_u3").await;
    let _p3 = seed_personal_account(&pool, u3).await;

    migrate_control_all(&pool).await;

    let rows: Vec<(Uuid, Uuid, bool)> = sqlx::query_as(
        "SELECT user_id, account_id, mcp_write_enabled FROM user_account_prefs \
         ORDER BY user_id, account_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let mut expected = vec![(u1, p1, true), (u2, p2, false)];
    expected.sort();
    let mut got = rows.clone();
    got.sort();
    assert_eq!(
        got, expected,
        "backfill must copy the legacy flag onto personal accounts only \
         (no org rows, no rows for users without a legacy flag): {rows:?}"
    );
    // Belt-and-suspenders: the org account specifically has no row.
    let org_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM user_account_prefs WHERE account_id = $1",
    )
    .bind(org)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(org_rows, 0, "org accounts must never be backfilled");
}

// ── (b) Org write: gated by scope + ceiling only ─────────────────────────────

/// Settings launch plan decision 6 (PR 2.1): MCP write is always on. An org
/// member with NO pref rows writes; an OLD explicit `false` row (and a legacy
/// `user_prefs` false) does not block; a token that carries product scopes is
/// held to them (`coralswarm:read` only => no write tools).
#[tokio::test]
async fn org_write_is_gated_by_scope_and_ceiling_not_by_pref_rows() {
    let Some(base) = base_url() else {
        eprintln!("skipping org_write_is_gated_by_scope_and_ceiling_not_by_pref_rows: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_org").await;
    migrate_control_all(&pool).await;

    let sub = "gov_org_member";
    let user = seed_user(&pool, sub).await;
    let _personal = seed_personal_account(&pool, user).await;
    let org = seed_org_account(&pool, user, "org_gov").await;
    seed_membership(&pool, org, user, "member").await;

    let app = app_for_pool(pool.clone());
    let token = org_token(sub, "org_gov");

    let names = list_tool_names(&app, &token).await;
    assert!(
        has_write_tools(&names),
        "an org member with no pref rows must have write (always on): {names:?}"
    );

    // OLD explicit denies (the removed toggle / the old admin-deny lever)
    // must not block anything now.
    set_account_grant(&pool, user, org, false).await;
    set_legacy_flag(&pool, user, false).await;
    let names_old_false = list_tool_names(&app, &token).await;
    assert!(
        has_write_tools(&names_old_false),
        "an OLD explicit-false user_account_prefs/user_prefs row must not block write: \
         {names_old_false:?}"
    );

    // The token scope is still a real gate.
    let scoped_write = org_token_with_scope(sub, "org_gov", "coralswarm:read coralswarm:write");
    assert!(
        has_write_tools(&list_tool_names(&app, &scoped_write).await),
        "a coralswarm:write token must write even with an old false row present"
    );
    let scoped_read = org_token_with_scope(sub, "org_gov", "coralswarm:read");
    let names_read_only = list_tool_names(&app, &scoped_read).await;
    assert!(!names_read_only.is_empty(), "a read-scoped token still lists read tools");
    assert!(
        !has_write_tools(&names_read_only),
        "a token without coralswarm:write must be refused write: {names_read_only:?}"
    );
}

// ── (c) Live membership re-check (offboarding revocation) ────────────────────

#[tokio::test]
async fn removed_org_member_fails_closed_on_the_next_call() {
    let Some(base) = base_url() else {
        eprintln!("skipping removed_org_member_fails_closed_on_the_next_call: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_revoke").await;
    migrate_control_all(&pool).await;

    let sub = "gov_revoked_member";
    let user = seed_user(&pool, sub).await;
    let _personal = seed_personal_account(&pool, user).await;
    let org = seed_org_account(&pool, user, "org_revoke").await;
    seed_membership(&pool, org, user, "member").await;

    let app = app_for_pool(pool.clone());
    // The token stays valid for its full TTL — the point of the live re-check.
    let token = org_token(sub, "org_revoke");

    let names = list_tool_names(&app, &token).await;
    assert!(has_write_tools(&names), "org member should have write");

    // Simulate the membership row vanishing with raw SQL — this test isolates
    // the /mcp live re-check itself (row gone => 403, whatever removed it).
    // The full webhook offboarding path (tombstone + the
    // require_auth resurrection regression) is covered by
    // `stale_oauth_token_cannot_resurrect_webhook_removed_membership`.
    sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(org)
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();

    // Same still-valid token, next call: 403, before any tool runs.
    let (status, body) = mcp_rpc(&app, &token, "tools/list", json!({})).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "removed member's next call must fail closed: {body:?}"
    );
    assert_eq!(body["error"], "not a member of this organization");

    // The revocation is org-scoped: the same user's PERSONAL token still works.
    let personal = personal_token(sub);
    let personal_names = list_tool_names(&app, &personal).await;
    assert!(
        !personal_names.is_empty(),
        "personal-account access must survive org offboarding"
    );
}

// ── (d) Personal accounts: old explicit denies no longer block write ─────────

/// Before PR 2.1 a personal user who had turned MCP write off (the legacy
/// `user_prefs` flag, backfilled by migration 0080 onto their personal
/// account, or an explicit `user_account_prefs` row) stayed write-denied.
/// Decision 6 made write always-on and removed the setting, so those old
/// explicit `false` rows must stop blocking — whether they arrived through
/// the 0080 backfill or were written afterwards.
#[tokio::test]
async fn old_explicit_false_rows_no_longer_block_personal_write() {
    let Some(base) = base_url() else {
        eprintln!("skipping old_explicit_false_rows_no_longer_block_personal_write: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_parity").await;

    // Pre-0080 world: a user who explicitly turned the legacy flag off.
    migrate_control_until(&pool, GOVERNANCE_MIGRATION_VERSION).await;
    let sub_backfilled = "gov_oldfalse_backfilled";
    let u_backfilled = seed_user(&pool, sub_backfilled).await;
    let p_backfilled = seed_personal_account(&pool, u_backfilled).await;
    set_legacy_flag(&pool, u_backfilled, false).await;

    // 0080 copies that `false` onto the personal account.
    migrate_control_all(&pool).await;
    let copied: Option<bool> = sqlx::query_scalar(
        "SELECT mcp_write_enabled FROM user_account_prefs WHERE user_id = $1 AND account_id = $2",
    )
    .bind(u_backfilled)
    .bind(p_backfilled)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(copied, Some(false), "fixture precondition: 0080 backfilled an explicit false");

    // Post-migration explicit denies: a legacy-only false and a per-account false.
    let sub_legacy = "gov_oldfalse_legacy_only";
    let u_legacy = seed_user(&pool, sub_legacy).await;
    let _p_legacy = seed_personal_account(&pool, u_legacy).await;
    set_legacy_flag(&pool, u_legacy, false).await;
    let sub_acct = "gov_oldfalse_account_row";
    let u_acct = seed_user(&pool, sub_acct).await;
    let p_acct = seed_personal_account(&pool, u_acct).await;
    set_account_grant(&pool, u_acct, p_acct, false).await;

    let app = app_for_pool(pool.clone());
    for sub in [sub_backfilled, sub_legacy, sub_acct] {
        let names = list_tool_names(&app, &personal_token(sub)).await;
        assert!(
            has_write_tools(&names),
            "{sub}: an old explicit-false pref row must not block personal write: {names:?}"
        );
        // Write tools are exactly the five write tools — nothing else changed.
        let write_names: Vec<&String> = names
            .iter()
            .filter(|n| ["add_context", "confirm_reef", "reject_reef", "update_reef", "promote"].contains(&n.as_str()))
            .collect();
        assert_eq!(write_names.len(), 5, "{sub}: all five write tools listed: {names:?}");
    }
}

/// A personal account with no pref rows anywhere writes by default (the
/// MCP listing-prep default, now unconditional).
#[tokio::test]
async fn mcp_write_default_on_for_a_never_configured_personal_account() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping mcp_write_default_on_for_a_never_configured_personal_account: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_default_on").await;
    migrate_control_all(&pool).await;

    let sub = "gov_default_on_never_configured";
    let user = seed_user(&pool, sub).await;
    let _account = seed_personal_account(&pool, user).await;

    let app = app_for_pool(pool.clone());
    let names = list_tool_names(&app, &personal_token(sub)).await;
    assert!(
        has_write_tools(&names),
        "a personal account with no pref rows must have MCP write: {names:?}"
    );
}

// ── P0 regression: stale-token resurrection via require_auth ─────────────────

/// Adversarial-review P0 on PR 2.2: offboard through the REAL webhook, then
/// REPLAY the member's still-valid OAuth token against a `require_auth` route
/// (the auth middleware's ProvisionFromClaims path used to `ensure_org_membership`-
/// upsert the deleted row right back). Post-fix: the tombstone blocks the
/// claims-path INSERT and `/mcp` keeps 403ing. A genuine webhook re-add restores
/// membership.
///
/// Updated for org-tenancy PR 4.2 (legacy-token fence): before 4.2 the
/// replayed token was still ACCEPTED by `require_auth` (200 on `/v1/me`) —
/// the tombstone only stopped the row from being re-created, it did not
/// reject the request itself, so a removed member kept full REST access for
/// the rest of their token's TTL. As of 4.2 the live re-check
/// (`middleware::auth::live_org_membership`) makes `require_auth` reject the
/// SAME way `/mcp` already did; the assertion below is the regression test
/// for that gap. `tests/legacy_token_fence_pg.rs` covers the fence's content-
/// route behavior (zero content served, never-removed member unaffected,
/// revoked-org, personal-token parity) end to end.
#[tokio::test]
async fn stale_oauth_token_cannot_resurrect_webhook_removed_membership() {
    let Some(base) = base_url() else {
        eprintln!("skipping stale_oauth_token_cannot_resurrect_webhook_removed_membership: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_resurrect").await;
    migrate_control_all(&pool).await;

    let sub = "gov_resurrect_member";
    let user = seed_user(&pool, sub).await;
    let _personal = seed_personal_account(&pool, user).await;
    let org = seed_org_account(&pool, user, "org_resurrect").await;
    seed_membership(&pool, org, user, "member").await;

    let app = app_for_pool(pool.clone());
    // Minted BEFORE offboarding; stays signature-valid for the whole test —
    // exactly the artifact the attack replays.
    let token = org_token(sub, "org_resurrect");
    assert!(
        has_write_tools(&list_tool_names(&app, &token).await),
        "member starts with write"
    );

    // Offboard through the real signed webhook (tombstone + membership DELETE,
    // one transaction).
    post_clerk_webhook(
        &app,
        json!({
            "type": "organizationMembership.deleted",
            "data": {
                "organization": { "id": "org_resurrect" },
                "public_user_data": { "user_id": sub }
            }
        }),
    )
    .await;
    assert_eq!(org_membership_count(&pool, org, user).await, 0);
    let tombstones: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM org_membership_tombstones WHERE clerk_org_id = 'org_resurrect' AND user_id = $1",
    )
    .bind(user)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tombstones, 1, "offboarding must record a tombstone");

    // THE ATTACK: replay the same still-valid OAuth token against the main
    // API. Post-4.2 the live re-check fences this the same way `/mcp` always
    // has: 403, before any handler runs.
    let status = api_get(&app, "/v1/me", &token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "org-tenancy PR 4.2: a require_auth request with the stale (offboarded) token must be \
         fenced by the live membership re-check, not served"
    );
    // …and the offboarding is still what it always was: no resurrection.
    assert_eq!(
        org_membership_count(&pool, org, user).await,
        0,
        "P0 regression: a require_auth request with the stale token must NOT resurrect account_members"
    );

    // /mcp stays revoked.
    let (status, body) = mcp_rpc(&app, &token, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "still revoked after replay: {body:?}");

    // Genuine re-invite: the webhook (sole authorized writer) clears the
    // tombstone and restores membership…
    post_clerk_webhook(
        &app,
        json!({
            "type": "organizationMembership.created",
            "data": {
                "organization": { "id": "org_resurrect" },
                "public_user_data": { "user_id": sub },
                "role": "org:member"
            }
        }),
    )
    .await;
    assert_eq!(org_membership_count(&pool, org, user).await, 1);
    // …so reads and writes work again: write is always on, so re-adding a
    // member needs no grant bookkeeping.
    let names = list_tool_names(&app, &token).await;
    assert!(!names.is_empty(), "re-added member reads again");
    assert!(
        has_write_tools(&names),
        "re-added member writes again (write is always on): {names:?}"
    );
}

/// Whole-org offboarding (`organization.deleted`): every member is
/// tombstoned, every membership is cleared, and a member's
/// stale token can neither read via `/mcp` nor resurrect anything via
/// `require_auth`.
#[tokio::test]
async fn org_deleted_revokes_every_member() {
    let Some(base) = base_url() else {
        eprintln!("skipping org_deleted_revokes_every_member: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_orgdel").await;
    migrate_control_all(&pool).await;

    let (sub_a, sub_b) = ("gov_orgdel_a", "gov_orgdel_b");
    let ua = seed_user(&pool, sub_a).await;
    let _pa = seed_personal_account(&pool, ua).await;
    let ub = seed_user(&pool, sub_b).await;
    let org = seed_org_account(&pool, ua, "org_del").await;
    seed_membership(&pool, org, ua, "owner").await;
    seed_membership(&pool, org, ub, "member").await;

    let app = app_for_pool(pool.clone());
    let token_a = org_token(sub_a, "org_del");

    post_clerk_webhook(&app, json!({ "type": "organization.deleted", "data": { "id": "org_del" } }))
        .await;

    let (members, tombstones): (i64, i64) = (
        sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
            .bind(org)
            .fetch_one(&pool)
            .await
            .unwrap(),
        sqlx::query_scalar(
            "SELECT count(*) FROM org_membership_tombstones WHERE clerk_org_id = 'org_del'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
    );
    assert_eq!((members, tombstones), (0, 2));
    // Re-review P1: whole-org deletion must ALSO record the org-LEVEL
    // revocation (per-member tombstones can't cover never-provisioned users).
    let revoked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM org_revocations WHERE clerk_org_id = 'org_del'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(revoked, 1, "organization.deleted must write an org_revocations row");

    // Stale token: post-4.2, require_auth now fences a revoked org itself
    // (`live_org_membership`'s `NOT EXISTS org_revocations` guard), so the
    // replay 403s here too (no resurrection either way), then /mcp 403.
    assert_eq!(api_get(&app, "/v1/me", &token_a).await, StatusCode::FORBIDDEN);
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
            .bind(org)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(remaining, 0, "org-deleted members must not be resurrectable");
    let (status, _) = mcp_rpc(&app, &token_a, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Re-review P1 regression: a user who was in the Clerk org but NEVER signed
/// into this backend has no `account_members` row when `organization.deleted`
/// fires — so per-member tombstones alone give them no tombstone. Their
/// still-valid org token, replayed after the deletion, must NOT be able to
/// join the deleted org via the claims path (the org-level `org_revocations`
/// guard), and `/mcp` must 403.
#[tokio::test]
async fn org_deleted_blocks_a_never_provisioned_token_holder() {
    let Some(base) = base_url() else {
        eprintln!("skipping org_deleted_blocks_a_never_provisioned_token_holder: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_neverprov").await;
    migrate_control_all(&pool).await;

    // The org exists with one provisioned owner…
    let owner = seed_user(&pool, "gov_ghost_owner").await;
    let _p = seed_personal_account(&pool, owner).await;
    let org = seed_org_account(&pool, owner, "org_ghost").await;
    seed_membership(&pool, org, owner, "owner").await;

    let app = app_for_pool(pool.clone());
    post_clerk_webhook(&app, json!({ "type": "organization.deleted", "data": { "id": "org_ghost" } }))
        .await;

    // …and THIS token holder was added on the Clerk side but never touched
    // the backend: no users row, no membership, hence no per-member tombstone.
    let ghost_sub = "gov_ghost_never_signed_in";
    let ghost_token = org_token(ghost_sub, "org_ghost");
    let none: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE clerk_user_id = $1")
        .bind(ghost_sub)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(none, 0, "fixture must start truly never-provisioned");

    // Replay on require_auth: user-level first-sight provisioning still runs
    // (a `users` row is created for the ghost identity — that's independent
    // of any org), but org-tenancy PR 4.2's live re-check now rejects the
    // request itself (403) because the org has no live membership row and is
    // wholesale-revoked — before 4.2 this request was accepted (200) even
    // though it correctly refused to create the membership row.
    let status = api_get(&app, "/v1/me", &ghost_token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "org-tenancy PR 4.2: a never-provisioned token holder for a revoked org must be \
         rejected by require_auth, not merely denied a membership row"
    );
    // …and NO membership may appear on the deleted org either way.
    let members: i64 =
        sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
            .bind(org)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        members, 0,
        "P1 regression: a never-provisioned token holder must not join a deleted org"
    );
    // And /mcp fails closed.
    let (status, body) = mcp_rpc(&app, &ghost_token, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "deleted org must 403 at /mcp: {body:?}");
}

/// Migration 0082's DEFERRED constraint trigger: even a RAW SQL insert (any
/// writer that skips the guarded provisioning functions) cannot violate a
/// per-member tombstone or an org-level revocation — while personal-account
/// membership inserts remain unaffected.
#[tokio::test]
async fn db_trigger_blocks_membership_resurrection_at_the_database_level() {
    let Some(base) = base_url() else {
        eprintln!("skipping db_trigger_blocks_membership_resurrection_at_the_database_level: set ORGT_TEST_BASE_URL");
        return;
    };
    let pool = scratch_pool(&base, "orgt_mcpgov_trigger").await;
    migrate_control_all(&pool).await;

    let u = seed_user(&pool, "gov_trigger_user").await;
    let _p = seed_personal_account(&pool, u).await; // personal insert passes the trigger
    let org_a = seed_org_account(&pool, u, "org_trig_a").await;
    let org_b = seed_org_account(&pool, u, "org_trig_b").await;

    // Tombstoned member on org_a: direct INSERT must be rejected by the DB.
    sqlx::query("INSERT INTO org_membership_tombstones (clerk_org_id, user_id) VALUES ('org_trig_a', $1)")
        .bind(u)
        .execute(&pool)
        .await
        .unwrap();
    let err = sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
        .bind(org_a)
        .bind(u)
        .execute(&pool)
        .await
        .expect_err("tombstoned insert must be blocked by the trigger");
    assert!(
        err.to_string().contains("tombstoned"),
        "unexpected trigger error: {err}"
    );

    // Revoked org_b: direct INSERT rejected even with NO per-member tombstone.
    sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ('org_trig_b')")
        .execute(&pool)
        .await
        .unwrap();
    let err = sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
        .bind(org_b)
        .bind(u)
        .execute(&pool)
        .await
        .expect_err("revoked-org insert must be blocked by the trigger");
    assert!(err.to_string().contains("revoked"), "unexpected trigger error: {err}");

    // The webhook re-add pattern (clear tombstone + insert, ONE transaction)
    // still commits — the deferred check sees the transaction's own deletion.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("DELETE FROM org_membership_tombstones WHERE clerk_org_id = 'org_trig_a' AND user_id = $1")
        .bind(u)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
        .bind(org_a)
        .bind(u)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit()
        .await
        .expect("tombstone-clear + insert in one tx must satisfy the deferred trigger");
}

/// Re-review P2: the legacy `/teams` ACCOUNT-membership write endpoints must
/// refuse org accounts (Clerk is the sole org-membership authority; an org
/// owner could otherwise re-add a Clerk-offboarded member out-of-band) —
/// while personal accounts keep the endpoints, which also proves the 0082
/// trigger leaves personal membership inserts alone end-to-end.
#[tokio::test]
async fn teams_membership_routes_refuse_org_accounts_but_serve_personal() {
    let Some(base) = base_url() else {
        eprintln!("skipping teams_membership_routes_refuse_org_accounts_but_serve_personal: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_teams").await;
    migrate_control_all(&pool).await;

    let sub = "gov_teams_owner";
    let user = seed_user(&pool, sub).await;
    let personal = seed_personal_account(&pool, user).await;
    let org = seed_org_account(&pool, user, "org_teams").await;
    seed_membership(&pool, org, user, "owner").await;

    let app = app_for_pool(pool.clone());

    // Org-context: both write endpoints 403 with the Clerk-managed message,
    // and nothing is written.
    let org_tok = session_org_token(sub, "org_teams", "org:admin");
    let (status, body) = api_json(
        &app,
        "POST",
        "/v1/teams/invite",
        &org_tok,
        Some(json!({"email": "invitee@example.com", "role": "member"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "org invite must 403: {body:?}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("identity provider"),
        "403 must explain org membership is Clerk-managed: {body:?}"
    );
    let (status, _) = api_json(
        &app,
        "DELETE",
        &format!("/v1/teams/members/{}", Uuid::new_v4()),
        &org_tok,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "org member-remove must 403");
    let org_members: i64 =
        sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
            .bind(org)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(org_members, 1, "org membership must be untouched by refused writes");

    // Personal-context: invite still works end-to-end (and the DB trigger
    // permits the personal account_members insert).
    let personal_tok = session_personal_token(sub);
    let (status, body) = api_json(
        &app,
        "POST",
        "/v1/teams/invite",
        &personal_tok,
        Some(json!({"email": "gov-teams-invitee@example.com", "role": "member"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "personal invite must still work: {body:?}");
    let personal_members: i64 =
        sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
            .bind(personal)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(personal_members, 2, "personal invite must insert the membership");
}

/// The session-UX invariant the P0 fix must NOT break: a NEVER-removed member
/// whose `organizationMembership.created` webhook hasn't landed yet (or local
/// dev with no webhook receiver) is still provisioned on first sign-in by the
/// claims path — and the claims path is INSERT-ONLY: a later token carrying an
/// escalated `org_role` must not rewrite the existing row's role (that is the
/// webhook's job).
#[tokio::test]
async fn claims_path_still_provisions_a_never_removed_member_but_never_updates_role() {
    let Some(base) = base_url() else {
        eprintln!("skipping claims_path_still_provisions_a_never_removed_member_but_never_updates_role: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = McpEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_mcpgov_firstsight").await;
    migrate_control_all(&pool).await;

    let sub = "gov_firstsight_member";
    let user = seed_user(&pool, sub).await;
    let _personal = seed_personal_account(&pool, user).await;
    let org = seed_org_account(&pool, user, "org_first").await;
    // Deliberately NO account_members row and NO tombstone: the webhook-race
    // first-sign-in case.

    let app = app_for_pool(pool.clone());
    let status = api_get(&app, "/v1/me", &session_org_token(sub, "org_first", "org:member")).await;
    assert_eq!(status, StatusCode::OK);
    let role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2",
    )
    .bind(org)
    .bind(user)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(
        role.as_deref(),
        Some("member"),
        "first-sight claims provisioning must still create the membership"
    );
    // Insert-only: an (attacker-replayed or stale) token with an escalated
    // role claim must not rewrite the row.
    let status = api_get(&app, "/v1/me", &session_org_token(sub, "org_first", "org:admin")).await;
    assert_eq!(status, StatusCode::OK);
    let role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2",
    )
    .bind(org)
    .bind(user)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(
        role.as_deref(),
        Some("member"),
        "claims path must never update an existing membership's role"
    );
}
