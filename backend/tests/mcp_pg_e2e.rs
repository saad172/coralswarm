//! Live end-to-end tests for the remote MCP server against a REAL Postgres
//! (`docs/plans/remote-mcp-v1.md` §5 Phase 4, §6 Layer C).
//!
//! Pattern-copied from `merge_unmerge_pg.rs`: gated on
//! `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations applied); a
//! plain `cargo test` with no DB configured skips every test in this file.
//!
//! `mcp` is a crate-private module (see `mcp_http.rs`'s own doc comment for
//! why), so this file cannot call into `CoralSwarmMcp`/tool dispatch directly
//! the way the in-crate `#[cfg(test)]` suites in `tools/read.rs` /
//! `tools/write.rs` do. Instead it drives the REAL public HTTP router
//! (`coralswarm_backend::test_support_app`) exactly as a real MCP client
//! would: JSON-RPC 2.0 `tools/call` / `tools/list` requests over the
//! streamable-HTTP transport, with genuine RS256 signature verification
//! against a local JWKS listener (same fixtures as `mcp_http.rs` /
//! `clerk::test_support`), backed by a REAL `PostgresStore`. In stateless
//! mode (`legacy_session_mode: false`, the mounted configuration — rmcp 3.x's
//! rename of `stateful_mode`) rmcp needs no `initialize` handshake per
//! request — see `StreamableHttpService::peer_info_for_stateless_request` —
//! so every call below is a single self-contained POST.
//!
//! Most tests build their `AppState` directly (`app_for_pool`) and so run
//! against the in-memory rate limiter `test_app_state()` supplies (harmless
//! at these call volumes). The one test that needs the REAL
//! Postgres-backed `PgRateLimiter` (`rate_limit::PgRateLimiter` — a
//! crate-private type this file cannot name or construct directly, per
//! `backend/src/lib.rs`'s `mod rate_limit;`) instead boots the actual server
//! process via `coralswarm_backend::run()` with `RATE_LIMIT_BACKEND=pg` and
//! drives it over real HTTP (`reqwest`) — the only way to exercise that
//! code path from outside the crate.
//!
//! Covers the Layer A gaps the Phase 3 handoff flagged as pg-only:
//!   (a) personal-ocean-only ingest rejection (ADR-0007 D7).
//!   (b) `confirm_reef`/`reject_reef`/`update_reef` against a REAL
//!       `PostgresStore`.
//!   (c) `list_meetings`/`get_transcript` owner-SUCCESS paths.
//!
//! Plus the full per-tool tenancy-isolation matrix (all 14 tools), an
//! `add_context` → `search_atoms` round trip, decision-reef state
//! transitions, a deterministic (no-LLM) `ask_ocean` call, a real-LLM
//! `ask_ocean` smoke gated on `OPENROUTER_API_KEY` (pattern:
//! `relationship_llm_e2e.rs`), and the `PgRateLimiter` 11th-call rejection on
//! the `mcp:ask:{user_id}` bucket.
//!
//! ## Environment this test provisions for itself
//!
//! - `MASTER_KEK_HEX`: a fixed test-only 32-byte hex key. Cloud ingest
//!   (`add_context` against a REAL Postgres pool, i.e. every test in this
//!   file) resolves a per-account DEK (`crypto::resolve_dek_cached`), which
//!   requires a master KEK to unwrap it — without one, `add_context` 503s
//!   with "encryption service unavailable" (see `clerk_provision.rs`'s
//!   `provision_account_dek` / `crypto.rs`'s `resolve_dek`). Sole ownership
//!   of this env var for the file's lifetime is guaranteed by `ENV_LOCK`.
//! - `content_tsv` backfill (`mark_atoms_searchable`): keyword search
//!   (`Store::keyword_search`) reads `atoms.content_tsv`, which is normally
//!   populated by the embed WORKER (`workers::embed::update_content_tsv_and_clear_failure`)
//!   asynchronously after the `EmbedAtoms` job it enqueues runs. `workers` is
//!   a crate-private module this file cannot invoke directly, and no worker
//!   loop is started against this harness's `AppState` (that only happens
//!   inside `run()`). Tests that need retrieval to find a just-ingested atom
//!   backfill `content_tsv` with one direct `UPDATE` immediately after
//!   `add_context` — a deliberate, documented substitute for the worker, not
//!   a claim that the worker itself is covered here (it has its own unit
//!   tests in `workers/embed.rs`).
//!
//! ## Running
//!
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test mcp_pg_e2e -- --nocapture
//!
//! Add `OPENROUTER_API_KEY=...` to also run the real-LLM `ask_ocean` smoke
//! (skipped otherwise, like `relationship_llm_e2e.rs`).

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::{Datelike, Utc};
use coralswarm_backend::clerk::test_support::{mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::AppState;
use rmcp::transport::common::http_header::HEADER_MCP_PROTOCOL_VERSION;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

/// A fixed, test-only 32-byte (64 hex char) master KEK. Never used outside
/// this harness — see the module doc's "environment this test provisions"
/// section for why cloud ingest needs one at all.
const TEST_MASTER_KEK_HEX: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";

/// Serializes every test in this file against the process-global env vars
/// `McpEnv` sets (`MCP_ENABLED`, `CLERK_ISSUER`, `MASTER_KEK_HEX`, ...) —
/// same rationale as `mcp_http.rs`'s own `ENV_LOCK`, duplicated here because
/// integration test files are separate compilation units and cannot share
/// `#[cfg(test)]`-only state. Each test holds the guard for its ENTIRE body
/// (see `McpEnv`), so tests in this file are effectively serialized even
/// under `cargo test`'s default in-binary parallelism — which also means the
/// `PgRateLimiter` 11th-call test never races another test's calls into the
/// same account/user rate-limit keys (each test additionally uses a fresh
/// Clerk `sub`/`user_id` per run, so keys never collide across runs either).
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

fn fresh_sub(label: &str) -> String {
    format!("mcp_pg_e2e_{label}_{}", Uuid::new_v4())
}

/// Spin up a local JWKS listener serving the shared test keypair (identical
/// fixture to `mcp_http.rs`'s own helper — duplicated for the same
/// separate-compilation-unit reason as `ENV_LOCK`).
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

/// Points MCP at a local JWKS listener with a fresh, unique-per-test resource
/// URL, forces `RATE_LIMIT_BACKEND=pg` (silences `McpConfig::from_env`'s
/// startup warning; the REAL rate limiter used by every call in this file is
/// whatever `AppState.rate_limiter` the test wires up directly — see
/// `app_for_pool`), and sets the test master KEK cloud ingest needs.
struct McpEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl McpEnv {
    async fn enable(resource_url: &str) -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: `ENV_LOCK` serializes every environment mutation in this
        // test binary — `self._guard` (returned below) holds it for this
        // `McpEnv`'s entire lifetime, so no other thread reads or writes
        // these vars concurrently while they're set.
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MCP_RESOURCE_URL", resource_url);
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
            // Pinned to 0 (MCP listing-prep item 5): `ask_ocean_and_wait`
            // below asserts the immediate `{status: "working", task_id, ...}`
            // shape right after the call, before ever polling
            // `get_task_result` — that's the pre-item-5 legacy contract this
            // whole file's `ask_ocean` coverage is built on. Item 5's own
            // synchronous-wait behavior has its dedicated tests in
            // `mcp_task_based_ask_ocean_pg.rs` instead, which opts a wait
            // budget back in explicitly.
            std::env::set_var("MCP_ASK_SYNC_WAIT_MS", "0");
        }
        Self { _guard: guard }
    }
}

impl Drop for McpEnv {
    fn drop(&mut self) {
        // SAFETY: see `enable`'s SAFETY comment — `self._guard` still holds
        // `ENV_LOCK` here (`Drop` runs before the guard field itself drops).
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MCP_RESOURCE_URL");
            std::env::remove_var("RATE_LIMIT_BACKEND");
            std::env::remove_var("MASTER_KEK_HEX");
            std::env::remove_var("MCP_ASK_SYNC_WAIT_MS");
        }
    }
}

/// Connect to the test Postgres, or skip (print + early-return) exactly like
/// `merge_unmerge_pg.rs` when `RELINFER_TEST_DATABASE_URL` is unset.
macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live MCP e2e suite");
                return;
            }
        }
    }};
}

/// Build the real public router backed by a REAL Postgres pool.
/// `test_app_state()` supplies the stub embedder + `resend_api_key`/`r2`/
/// `dek_cache`/`rate_limiter` defaults so this file never has to name
/// `coralswarm_backend`'s crate-private `StubEmbedder`/`InMemoryRateLimiter`/
/// `rate_limit::RateLimiter` types directly (`rate_limit` is a private module
/// — `PgRateLimiter` is genuinely unreachable from an external test binary;
/// see the dedicated rate-limiter test below, which boots the real server
/// process via `coralswarm_backend::run()` instead of this helper, since
/// that is the only way to exercise the REAL `PgRateLimiter` from outside
/// the crate). Only `db` and `store` are overridden onto a real pool — the
/// in-memory rate limiter is harmless for every OTHER test here (their call
/// volumes stay far under its ceilings).
fn app_for_pool(pool: sqlx::PgPool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    coralswarm_backend::test_support_app(Arc::new(state))
}

// ── JSON-RPC / SSE plumbing ───────────────────────────────────────────────────
//
// `mcp/mod.rs` mounts rmcp's `StreamableHttpService` with
// `StreamableHttpServerConfig::default().with_legacy_session_mode(false)
// .with_json_response(true)` — `json_response: true` (rmcp 3.x) makes every
// ordinary request/response `/mcp` call answer `application/json` directly
// instead of `text/event-stream`. None of our 20 tools emit a notification or
// server-initiated request before their final result, so rmcp never needs the
// SSE fallback in practice — but `mcp_rpc` still branches on the response
// `Content-Type` and parses SSE when it sees one, both because rmcp falls
// back to SSE automatically whenever a handler DOES emit something
// out-of-band first (so this stays correct if that ever changes here) and so
// this file keeps working unmodified against an older/reconfigured server.
// Non-2xx responses (401/403/503) are plain JSON either way and pass through
// unchanged.

/// Parse an SSE body (`data: <json>`, one event, stream closes once
/// `OneshotTransport`'s single message is answered) back into the JSON-RPC
/// message it carries. Only reached when a `/mcp` response actually comes
/// back as `text/event-stream` — see the module note above.
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

/// POST one JSON-RPC 2.0 request to `/mcp` with the given bearer token
/// (`None` omits the `Authorization` header entirely) and return
/// `(status, parsed_body)`.
async fn mcp_rpc(
    app: &axum::Router,
    token: Option<&str>,
    method: &str,
    params: Value,
) -> (StatusCode, Value) {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        // `tower::oneshot` drives the router directly (no real TCP
        // connection), so nothing synthesizes a `Host` header the way a real
        // HTTP/1.1 client/hyper would. rmcp's streamable-HTTP transport
        // requires ONE of `Host` or the HTTP/2 `:authority` pseudo-header for
        // its DNS-rebinding guard (`validate_dns_rebinding_headers` ->
        // `parse_host_header`) — without it every request 400s before ever
        // reaching `mcp_auth`.
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    let req = builder.body(Body::from(body.to_string())).unwrap();
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

/// Call a tool via `tools/call`. Returns `(is_error, body)` where `body` is
/// the tool's own `{"error":{"code","message"}}` or success JSON, parsed out
/// of the `CallToolResult`'s first text content block (`tool_error`/
/// `tool_success`'s wire shape — plan §3 conventions).
async fn call_tool(app: &axum::Router, token: &str, name: &str, arguments: Value) -> (bool, Value) {
    let (status, rpc) = mcp_rpc(
        app,
        Some(token),
        "tools/call",
        json!({ "name": name, "arguments": arguments }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "tools/call transport-level failure for {name}: {rpc:?}"
    );
    let result = rpc
        .get("result")
        .unwrap_or_else(|| panic!("no result in tools/call response for {name}: {rpc:?}"));
    let is_error = result
        .get("isError")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
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

fn terminal_task(is_error: bool, body: Value, task_id: Uuid) -> Option<(bool, Value)> {
    assert_eq!(
        body["task_id"],
        task_id.to_string(),
        "task identity changed: {body}"
    );
    match body["status"].as_str().expect("task status") {
        "working" => {
            assert!(!is_error, "working task is an error: {body}");
            None
        }
        "completed" => {
            assert!(!is_error, "completed task is an error: {body}");
            assert!(
                body["result"].is_object(),
                "completed task lacks object result: {body}"
            );
            Some((false, body["result"].clone()))
        }
        "failed" | "cancelled" => {
            assert!(
                body["result"].is_null(),
                "failed task exposed result: {body}"
            );
            Some((true, body))
        }
        other => panic!("unknown task status {other}: {body}"),
    }
}

async fn ask_ocean_and_wait(app: &axum::Router, token: &str, arguments: Value) -> (bool, Value) {
    let (is_error, body) = call_tool(app, token, "ask_ocean", arguments).await;
    if is_error && body.get("status").is_none() {
        assert!(
            body["error"]["code"].is_string(),
            "admission failure lacks code: {body}"
        );
        return (true, body);
    }
    let task_id: Uuid = body["task_id"]
        .as_str()
        .expect("task UUID")
        .parse()
        .unwrap();
    if let Some(result) = terminal_task(is_error, body, task_id) {
        return result;
    }
    for _ in 0..40 {
        let (error, body) =
            call_tool(app, token, "get_task_result", json!({"task_id":task_id})).await;
        if let Some(result) = terminal_task(error, body, task_id) {
            return result;
        }
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
    panic!("get_task_result never reached a terminal state for task {task_id}");
}

/// Same wire contract as [`call_tool`], but with an optional explicit
/// `User-Agent` header on the request (`None` omits it entirely) —
/// [`mcp_rpc`]/[`call_tool`] never set one at all, so this is the only way
/// this file can prove `mcp::auth::McpAuthContext::mcp_client_name`
/// (migration 0072) is actually populated from a REAL HTTP request's header,
/// rather than just from a directly-injected `McpAuthContext` (the in-crate
/// `mcp::tools::write` tests bypass the HTTP layer entirely — see that
/// module's `add_context_stamps_project_and_mcp_client_name` test doc
/// comment). Returns the RAW `CallToolResult` JSON (`result`) unparsed,
/// rather than destructuring it into `(is_error, body)` off the first text
/// content block the way [`call_tool`] does: needed for the
/// `structuredContent`-only negotiation
/// (`mcp::tools::structured_only_client`) — a `User-Agent` matching its
/// built-in `claude-code` hint gets back an EMPTY `content` array, which
/// [`call_tool`]'s `result["content"][0]["text"]` extraction would panic
/// on. [`call_tool`] itself stays unchanged (every OTHER test in this file
/// uses it, all with no `User-Agent` header set, i.e. `mcp_client_name:
/// None` / dual-payload, so its extraction is still valid there) rather than
/// being made negotiation-aware — duplicating this HTTP-request plumbing
/// rather than parameterizing [`mcp_rpc`]/[`call_tool`] themselves, which
/// are called from dozens of sites in this file that have no reason to grow
/// extra always-default arguments for the two tests that need this.
async fn raw_call_tool(
    app: &axum::Router,
    token: &str,
    user_agent: Option<&str>,
    name: &str,
    arguments: Value,
) -> Value {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    });
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"));
    if let Some(ua) = user_agent {
        builder = builder.header(axum::http::header::USER_AGENT, ua);
    }
    let req = builder.body(Body::from(body.to_string())).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
    let rpc = if content_type.contains("text/event-stream") {
        parse_sse_json(&bytes)
    } else if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    assert_eq!(
        status,
        StatusCode::OK,
        "tools/call transport-level failure for {name}: {rpc:?}"
    );
    rpc.get("result")
        .unwrap_or_else(|| panic!("no result in tools/call response for {name}: {rpc:?}"))
        .clone()
}

/// Same wire contract as [`call_tool`], but over a REAL TCP connection via
/// `reqwest` against a booted server process (`base`, e.g.
/// `http://127.0.0.1:18080`) instead of an in-process `tower::oneshot` call.
/// Used only by the `PgRateLimiter` test, which needs the actual server
/// binary running (see that test's doc comment for why).
async fn call_tool_http(
    http: &reqwest::Client,
    base: &str,
    token: &str,
    name: &str,
    arguments: Value,
) -> (bool, Value) {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    });
    let resp = http
        .post(format!("{base}/mcp"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"))
        .body(body.to_string())
        .send()
        .await
        .unwrap_or_else(|e| panic!("HTTP request to /mcp failed: {e}"));
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = resp.bytes().await.unwrap();
    assert!(
        status.is_success(),
        "tools/call HTTP-level failure for {name}: {status} {}",
        String::from_utf8_lossy(&bytes)
    );
    let rpc = if content_type.contains("text/event-stream") {
        parse_sse_json(&bytes)
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    let result = rpc
        .get("result")
        .unwrap_or_else(|| panic!("no result in tools/call response for {name}: {rpc:?}"));
    let is_error = result
        .get("isError")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let content = result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool result has no text content for {name}: {result:?}"));
    let body: Value = serde_json::from_str(content)
        .unwrap_or_else(|e| panic!("tool result text is not JSON for {name}: {e}\n{content}"));
    (is_error, body)
}

async fn drain_ask_task_http(http: &reqwest::Client, base: &str, token: &str, task_id: &str) {
    let task: Uuid = task_id.parse().expect("task UUID");
    for _ in 0..100 {
        let (error, body) = call_tool_http(
            http,
            base,
            token,
            "get_task_result",
            json!({"task_id":task_id}),
        )
        .await;
        if let Some((failed, body)) = terminal_task(error, body, task) {
            assert!(!failed, "Ask failed while draining rate-limit test: {body}");
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
    panic!("get_task_result never reached a terminal state for task {task_id}");
}

// ── Token helpers ──────────────────────────────────────────────────────────────

/// Mint a write-scope OAuth token for a fresh Clerk `sub`. The
/// `coralswarm:*` `scope` claim exercises `mcp::auth::effective_scopes`'
/// token-scope branch: write needs `coralswarm:write` AND the server ceiling,
/// and nothing else (there is no per-user grant).
fn write_token(sub: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "scope": "coralswarm:read coralswarm:write",
        "client_id": "mcp_pg_e2e_client",
    }))
}

// ── Provisioning + seeding helpers ─────────────────────────────────────────────

/// Trigger first-sight Clerk provisioning (`resolve_clerk_context` ->
/// `ensure_user` + `ensure_personal_account`, which also mints the account's
/// Primary personal ocean) via one real `/mcp` call, then read back the
/// `(user_id, account_id, graph_id)` triple via direct SQL — mirrors
/// `merge_unmerge_pg.rs`'s own "seed via raw SQL, then hand the ids to the
/// Store API" pattern.
///
/// Thin wrapper over [`provision_with_name`] (`name: None`) — kept as its own
/// function since it's by far the common case across this file and dropping
/// the unused 4th (token) return value at every call site would be noise.
async fn provision(app: &axum::Router, pool: &sqlx::PgPool, sub: &str) -> (Uuid, Uuid, Uuid) {
    let (user_id, account_id, graph_id, _token) = provision_with_name(app, pool, sub, None).await;
    (user_id, account_id, graph_id)
}

/// Same as [`provision`], but the FIRST-auth token optionally carries an OIDC
/// `name` claim (`None` reproduces `provision`'s plain `write_token` shape
/// exactly, byte-for-byte). Lets a test provision a Clerk identity that
/// already has a display name from its very first sign-in — `ensure_user`
/// persists it onto `users.name` synchronously as part of the provisioning
/// INSERT (see that function's own doc comment) — without hand-duplicating
/// the list_oceans-then-look-up-ids boilerplate. Returns the token used
/// alongside the resolved ids so the caller can keep authenticating as the
/// same identity afterward (e.g. to drive a SECOND auth that exercises
/// `sync_user_display_name`'s fire-and-forget refresh path instead of
/// first-sight provisioning).
async fn provision_with_name(
    app: &axum::Router,
    pool: &sqlx::PgPool,
    sub: &str,
    name: Option<&str>,
) -> (Uuid, Uuid, Uuid, String) {
    let mut claims = json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "scope": "coralswarm:read coralswarm:write",
        "client_id": "mcp_pg_e2e_client",
    });
    if let Some(n) = name {
        claims["name"] = json!(n);
    }
    let token = mint_oauth(claims);

    let (is_error, body) = call_tool(app, &token, "list_oceans", json!({})).await;
    assert!(
        !is_error,
        "list_oceans provisioning call failed for {sub}: {body:?}"
    );

    let user_id: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(sub)
        .fetch_one(pool)
        .await
        .expect("provisioned user row");
    let account_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE owner_user_id = $1 AND clerk_org_id IS NULL ORDER BY created_at LIMIT 1",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
    .expect("provisioned personal account");
    let graph_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM graphs WHERE account_id = $1 AND kind = 'personal' AND is_primary LIMIT 1",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .expect("provisioned Primary personal ocean");
    (user_id, account_id, graph_id, token)
}

/// Poll `users.name` until it equals `expected` or `timeout` elapses (short
/// fixed-interval retry, not a fixed sleep) — used to assert on
/// `middleware::auth::resolve_clerk_context`'s fire-and-forget `users.name`
/// refresh (`sync_user_display_name`, spawned off the request's critical
/// path and bounded by `DISPLAY_NAME_REFRESH_TIMEOUT`), which is explicitly
/// NOT guaranteed to have landed by the time the triggering HTTP response
/// returns. Returns whatever `users.name` holds when it stops polling
/// (matched or timed out) so the caller's own assertion produces the honest
/// failure value rather than this helper panicking on the caller's behalf.
async fn poll_users_name(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    expected: &str,
    timeout: std::time::Duration,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let name: Option<String> = sqlx::query_scalar("SELECT name FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("users row must exist");
        if name.as_deref() == Some(expected) || tokio::time::Instant::now() >= deadline {
            return name;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Seed a pending decision candidate (CS-069 shape) linked to a fresh atom.
/// `source_id` is derived from the fresh `atom_id` rather than a fixed
/// `"seed"` literal: the `atoms` table has a
/// `(graph_id, source_type, source_id)` uniqueness constraint, and two calls
/// against the SAME `graph_id` with a fixed `source_id` would collide —
/// `insert_atoms`' `ON CONFLICT DO NOTHING` would silently
/// skip the second atom row entirely (returning it as "not newly inserted"),
/// leaving `create_decision_candidate_and_link_atom` to link a `node_atoms`
/// row to an `atom_id` that was never actually persisted (FK violation).
async fn seed_pending_decision(
    store: &PostgresStore,
    graph_id: Uuid,
    user_id: Uuid,
    title: &str,
) -> Uuid {
    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph_id,
            "note",
            &[format!("seed-{atom_id}")],
            &["blob".to_string()],
            &[],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .unwrap();
    let body = json!({
        "decided_at": Utc::now().to_rfc3339(),
        "review_status": "pending",
    })
    .to_string();
    store
        .create_decision_candidate_and_link_atom(graph_id, title, &body, atom_id)
        .await
        .unwrap()
}

/// Backfill `content_tsv` for freshly `add_context`-ed atoms — see the module
/// doc's "environment this test provisions" note on why this substitutes for
/// the (crate-private, not spawned here) embed worker.
async fn mark_atoms_searchable(pool: &sqlx::PgPool, atom_ids: &[Uuid]) {
    sqlx::query(
        "UPDATE atoms SET content_tsv = to_tsvector(fts_language::regconfig, content_inline) \
         WHERE atom_id = ANY($1)",
    )
    .bind(atom_ids)
    .execute(pool)
    .await
    .expect("content_tsv test backfill");
}

fn atom_ids_of(body: &Value) -> Vec<Uuid> {
    body["atom_ids"]
        .as_array()
        .expect("atom_ids array")
        .iter()
        .map(|v| Uuid::parse_str(v.as_str().expect("atom_id string")).expect("atom_id uuid"))
        .collect()
}

async fn cleanup(pool: &sqlx::PgPool, account_ids: &[Uuid], user_ids: &[Uuid]) {
    for a in account_ids {
        let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, *a)
            .await
            .unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, *a)
            .await
            .unwrap();
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(a)
            .execute(&mut *financial)
            .await
            .ok();
        financial.commit().await.unwrap();
    }
    for u in user_ids {
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(u)
            .execute(pool)
            .await
            .ok();
    }
}

struct PendingEvidence<'a> {
    item: Uuid,
    version: Uuid,
    quote: &'a str,
    project: Option<&'a str>,
}

struct RetainedFixture {
    item: Uuid,
    version: Uuid,
    passage: Uuid,
    semantic_atom: Uuid,
    start: i32,
    end: i32,
    quote: String,
}

impl RetainedFixture {
    fn assert_cited(&self, body: &Value) {
        let citations = body["retained_citations"]
            .as_array()
            .expect("retained citations");
        let citation = citations
            .iter()
            .find(|c| c["source_version_id"] == self.version.to_string())
            .unwrap_or_else(|| panic!("published version is absent: {body}"));
        assert_eq!(citation["source_item_id"], self.item.to_string());
        assert_eq!(citation["semantic_atom_id"], self.semantic_atom.to_string());
        assert_eq!(citation["passage_id"], self.passage.to_string());
        assert_eq!(citation["start_char"], self.start);
        assert_eq!(citation["end_char"], self.end);
        assert_eq!(citation["quote"], self.quote);
        assert!(self.end > self.start);
    }
}

struct EmbeddingObserver(std::sync::atomic::AtomicUsize);
impl coralswarm_core::embed::EmbedProvider for EmbeddingObserver {
    fn embed_batch<'a>(
        &'a self,
        _: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> coralswarm_core::embed::EmbedBatchFuture<'a> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { panic!("retained Sources-only must not embed") })
    }
}

struct EvidenceModel<'a> {
    quote: &'a str,
    project: Option<&'a str>,
    calls: std::sync::atomic::AtomicUsize,
    prompts: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl coralswarm_backend::llm::LlmProvider for EvidenceModel<'_> {
    async fn complete(
        &self,
        req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<coralswarm_backend::llm::CompletionResponse, coralswarm_backend::llm::LlmError>
    {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.prompts.lock().unwrap().push(req.messages.iter().map(|m|m.content.as_str()).collect::<Vec<_>>().join("\n"));
        let entities = self
            .project
            .map(|name| json!([{"mention":name,"type":"project"}]))
            .unwrap_or(json!([]));
        Ok(coralswarm_backend::llm::CompletionResponse {
            content: json!({"atoms":[{"kind":"fact","text":self.quote,"evidence_quote":self.quote,"entities":entities}]}).to_string(),
            input_tokens: 10, output_tokens: 10, model: req.model,
            cache_creation_input_tokens:0, cache_read_input_tokens:0,
            stop_reason: Some("tool_use".into()), cost_usd: None,
        })
    }
    async fn stream_complete(
        &self,
        _: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<
                            coralswarm_backend::llm::StreamEvent,
                            coralswarm_backend::llm::LlmError,
                        >,
                    > + Send,
            >,
        >,
        coralswarm_backend::llm::LlmError,
    > {
        panic!("fixture extraction does not stream")
    }
    fn name(&self) -> &'static str {
        "mcp-retained-fixture"
    }
}

async fn enrichment_post(app: &axum::Router, token: &str, path: &str, body: Value) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("Authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(status.is_success(), "enrichment HTTP {status}: {value}");
    value
}

async fn publish_retained(
    app: &axum::Router,
    pool: &sqlx::PgPool,
    token: &str,
    graph: Uuid,
    captured: &[(&Value, &str, Option<&str>)],
) -> Vec<RetainedFixture> {
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("SET app.identity_serving_contract='composite-v1'")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../schema/identity-contract.sql"
    ))
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    let mut versions = Vec::new();
    for (body, quote, project) in captured {
        let raw = atom_ids_of(body);
        assert!(!raw.is_empty());
        let row: (Uuid, Uuid) = sqlx::query_as("SELECT v.source_item_id,v.version_id FROM source_versions v JOIN source_version_chunks c ON c.graph_id=v.graph_id AND c.source_version_id=v.version_id WHERE c.graph_id=$1 AND c.atom_id=$2")
            .bind(graph).bind(raw[0]).fetch_one(pool).await.unwrap();
        versions.push(PendingEvidence {item:row.0,version:row.1,quote,project:*project});
    }
    publish_versions(app, pool, token, graph, versions).await
}

async fn publish_versions(
    app: &axum::Router,
    pool: &sqlx::PgPool,
    token: &str,
    graph: Uuid,
    versions: Vec<PendingEvidence<'_>>,
) -> Vec<RetainedFixture> {
    let run = enrichment_post(app,token,"/v1/sources/enrichment/runs",json!({"graph_id":graph,"execute":true,"local_holdings_unavailable":true,"budget":{"items":100,"characters":500000,"model_calls":100}})).await;
    let run_id: Uuid = run["run_id"].as_str().unwrap().parse().unwrap();
    for _ in 0..6 {
        enrichment_post(
            app,
            token,
            &format!("/v1/sources/enrichment/runs/{run_id}/advance"),
            json!({"graph_id":graph,"page_size":100}),
        )
        .await;
        let ready: i64 = sqlx::query_scalar("SELECT count(*) FROM corpus_enrichment_units WHERE run_id=$1 AND holding_id=ANY($2) AND execution_owner AND state='queued'")
            .bind(run_id).bind(versions.iter().map(|v|v.version).collect::<Vec<_>>()).fetch_one(pool).await.unwrap();
        if ready == versions.len() as i64 {
            break;
        }
    }
    let jobs = coralswarm_backend::jobs::queue::dequeue(pool, &["ExtractSourceAtoms"], 100)
        .await
        .unwrap();
    assert_eq!(
        jobs.len(),
        versions.len(),
        "one actual queued job per fixture version"
    );
    for job in jobs {
        let version: Uuid = job.payload["source_version_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let index = versions
            .iter()
            .position(|v| v.version == version)
            .expect("unexpected queued version");
        let model = EvidenceModel {
            quote: versions[index].quote,
            project: versions[index].project,
            calls: std::sync::atomic::AtomicUsize::new(0),
            prompts: std::sync::Mutex::new(vec![]),
        };
        coralswarm_backend::test_p10_source_worker(
            &coralswarm_backend::tenancy::TenantId::Consumer,
            pool,
            pool,
            &coralswarm_backend::llm::testing::GatedScript(&model),
            job,
        )
        .await;
        assert_eq!(model.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
    let mut fixtures = Vec::new();
    for PendingEvidence {item,version,quote,..} in versions {
        let (passage,semantic_atom,start,end,stored):(Uuid,Uuid,i32,i32,String)=sqlx::query_as("SELECT passage_id,semantic_atom_id,start_char,end_char,quote FROM semantic_atom_passages WHERE graph_id=$1 AND source_version_id=$2")
            .bind(graph).bind(version).fetch_one(pool).await.unwrap();
        assert_eq!(stored, quote);
        assert_eq!(end - start, quote.chars().count() as i32);
        fixtures.push(RetainedFixture {
            item,
            version,
            passage,
            semantic_atom,
            start,
            end,
            quote: stored,
        });
    }
    fixtures
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Per-tool tenancy isolation (plan §4.5, §6 Layer C): for each of the 14
/// tools, a caller authenticated as user B cannot reach a resource that
/// belongs to user A's tenancy — always `not_found` (IDOR non-leak
/// convention), except `list_oceans` (B's own list simply must not contain
/// A's ocean) and `recent_activity` (a list-with-filter tool: B's own
/// account-scoped query must simply return no rows for A's ocean, not a
/// distinct error).
#[tokio::test]
async fn tenancy_isolation_across_all_14_tools_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-tenancy.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub_a = fresh_sub("tenant_a");
    let sub_b = fresh_sub("tenant_b");
    let (user_a, account_a, graph_a) = provision(&app, &pool, &sub_a).await;
    let (_user_b, account_b, _graph_b) = provision(&app, &pool, &sub_b).await;
    let token_b = write_token(&sub_b);

    // A's resources, seeded directly against the real store.
    let entity_id = store
        .upsert_node(graph_a, "entity", "A's Entity")
        .await
        .unwrap();
    let decision_id = seed_pending_decision(&store, graph_a, user_a, "A's decision").await;
    let atom_id = Uuid::new_v4();
    store
        .insert_atoms(
            &[atom_id],
            graph_a,
            "note",
            &["a-src-1".to_string()],
            &["a-blob-1".to_string()],
            &[],
            Utc::now(),
            user_a,
            "private",
            "english",
        )
        .await
        .unwrap();
    store
        .set_atoms_inline_content(&[atom_id], &["A's private coral content".to_string()])
        .await
        .unwrap();
    store
        .upsert_meeting_transcript(
            graph_a,
            "meeting-a-1",
            None,
            Some("A's meeting transcript text"),
            Some(60_000),
            None,
        )
        .await
        .unwrap();

    // 1. list_oceans — B's list must not contain A's ocean.
    let (is_error, body) = call_tool(&app, &token_b, "list_oceans", json!({})).await;
    assert!(!is_error, "list_oceans should succeed for B: {body:?}");
    let oceans = body["oceans"].as_array().unwrap();
    assert!(
        !oceans.iter().any(|o| o["id"] == graph_a.to_string()),
        "B's list_oceans must not contain A's ocean: {oceans:?}"
    );

    // 2. ask_ocean — no ocean in ocean_ids is reachable by B -> not_found.
    // Task-based (MCP spec 2026-07-28 PR 2): the reachability check runs
    // inside the spawned task, not synchronously, so the denial only
    // surfaces once the task completes.
    let (is_error, body) = ask_ocean_and_wait(
        &app,
        &token_b,
        json!({"question": "anything?", "ocean_ids": [graph_a]}),
    )
    .await;
    assert!(is_error, "ask_ocean should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 3. list_reefs
    let (is_error, body) =
        call_tool(&app, &token_b, "list_reefs", json!({"ocean_id": graph_a})).await;
    assert!(is_error, "list_reefs should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 4. get_reef
    let (is_error, body) =
        call_tool(&app, &token_b, "get_reef", json!({"reef_id": entity_id})).await;
    assert!(is_error, "get_reef should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 5. list_meetings
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "list_meetings",
        json!({"ocean_id": graph_a}),
    )
    .await;
    assert!(is_error, "list_meetings should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 6. get_meeting
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "get_meeting",
        json!({"ocean_id": graph_a, "meeting_id": "meeting-a-1"}),
    )
    .await;
    assert!(is_error, "get_meeting should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 7. get_atom
    let (is_error, body) = call_tool(&app, &token_b, "get_atom", json!({"atom_id": atom_id})).await;
    assert!(is_error, "get_atom should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 8. search_atoms
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "search_atoms",
        json!({"query": "anything", "ocean_ids": [graph_a]}),
    )
    .await;
    assert!(is_error, "search_atoms should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 9. get_transcript
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "get_transcript",
        json!({"ocean_id": graph_a, "meeting_id": "meeting-a-1"}),
    )
    .await;
    assert!(is_error, "get_transcript should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 10. recent_activity — an explicit ocean_id is now reachability-gated
    //     (security review 2026-07-18, see mcp/tools/read.rs's recent_activity
    //     doc comment): a caller-supplied ocean_id the caller can't reach
    //     returns `not_found`, matching every other tool in this suite,
    //     rather than the old silent-empty-success behavior.
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "recent_activity",
        json!({"ocean_id": graph_a}),
    )
    .await;
    assert!(is_error, "recent_activity should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 11. add_context
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "add_context",
        json!({"ocean_id": graph_a, "content": "injected by B"}),
    )
    .await;
    assert!(is_error, "add_context should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 12. confirm_reef
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "confirm_reef",
        json!({"reef_id": decision_id}),
    )
    .await;
    assert!(is_error, "confirm_reef should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 13. reject_reef (same decision node — B's attempt must fail regardless,
    //     without mutating A's row, so re-using it after the confirm attempt
    //     above is safe).
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "reject_reef",
        json!({"reef_id": decision_id}),
    )
    .await;
    assert!(is_error, "reject_reef should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // 14. update_reef
    let (is_error, body) = call_tool(
        &app,
        &token_b,
        "update_reef",
        json!({"reef_id": entity_id, "title": "Hijacked"}),
    )
    .await;
    assert!(is_error, "update_reef should deny B: {body:?}");
    assert_eq!(error_code(&body), "not_found");

    // Sanity: A's own decision is still pending (B's denied attempts above
    // never mutated it) — confirm it as A to prove the resource was real.
    let (is_error, body) = call_tool(
        &app,
        &write_token(&fresh_sub("unused")),
        "confirm_reef",
        json!({"reef_id": decision_id}),
    )
    .await;
    // A different (unprovisioned) identity also can't reach it — belt and
    // suspenders on the same assertion the loop above already covers.
    assert!(is_error);
    assert_eq!(error_code(&body), "not_found");

    cleanup(&pool, &[account_a, account_b], &[user_a]).await;
}

#[tokio::test]
async fn add_context_lands_atom_and_search_atoms_finds_it_round_trip() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-roundtrip.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("roundtrip");
    let (user, account, graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let quote = "The zzqcoral77 migration moved storage onto PostgreSQL with pgvector.";
    let (error, capture) = call_tool(
        &app,
        &token,
        "add_context",
        json!({"ocean_id":graph,"content":quote}),
    )
    .await;
    assert!(!error, "{capture}");
    let raw = atom_ids_of(&capture);
    assert_eq!(raw.len(), 1);
    assert_eq!(capture["ocean_id"], graph.to_string());
    assert!(capture["source_id"].as_str().unwrap().starts_with("mcp:"));
    let (error, pending) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query":"zzqcoral77","ocean_ids":[graph]}),
    )
    .await;
    assert!(!error, "{pending}");
    assert!(pending["retained_citations"].as_array().unwrap().is_empty());
    assert!(pending["legacy_evidence"].as_array().unwrap().is_empty());
    let evidence = publish_retained(&app, &pool, &token, graph, &[(&capture, quote, None)]).await;
    let foreign_sub=fresh_sub("roundtrip_foreign");
    let (foreign_user,foreign_account,foreign_graph)=provision(&app,&pool,&foreign_sub).await;
    let foreign_token=write_token(&foreign_sub);
    let foreign_quote="The zzqcoral77 private foreign project must never be cited.";
    let (error,foreign_capture)=call_tool(&app,&foreign_token,"add_context",json!({"ocean_id":foreign_graph,"content":foreign_quote})).await;
    assert!(!error,"{foreign_capture}");
    let foreign=publish_retained(&app,&pool,&foreign_token,foreign_graph,&[(&foreign_capture,foreign_quote,None)]).await;
    let (error, result) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query":"zzqcoral77","ocean_ids":[graph]}),
    )
    .await;
    assert!(!error, "{result}");
    evidence[0].assert_cited(&result);
    assert_eq!(result["retained_citations"].as_array().unwrap().len(), 1);
    assert!(result["retained_citations"].as_array().unwrap().iter().all(|c|c["source_version_id"]!=foreign[0].version.to_string()));
    let mut denied_bodies=Vec::new();
    for target in [graph,Uuid::new_v4()] {
        let (error,denied)=call_tool(&app,&foreign_token,"search_atoms",json!({"query":"zzqcoral77","ocean_ids":[target]})).await;
        assert!(error,"foreign or absent source must be denied: {denied}");
        assert_eq!(error_code(&denied),"not_found");
        denied_bodies.push(denied);
    }
    assert_eq!(denied_bodies[0],denied_bodies[1],"scope denial must not expose existence");
    let (error, atom) = call_tool(&app, &token, "get_atom", json!({"atom_id":raw[0]})).await;
    assert!(!error, "{atom}");
    assert!(atom["content"].as_str().unwrap().contains("zzqcoral77"));
    cleanup(&pool, &[account,foreign_account], &[user,foreign_user]).await;
}

// ── Billing gate (real-pricing-and-limits PR) ───────────────────────────────
//
// `billing::enforcement::is_blocked` (crate-private — this file cannot call
// it directly, only observe its wiring's effect over real HTTP/MCP) is wired
// into exactly three request paths: `POST /v1/queries`, `POST
// /v1/queries/stream`, and the shared `services::ask::run_ask` pipeline
// behind BOTH `ask_ocean` and `search_atoms`. This test proves the gate
// fires on all four surfaces for a genuinely out-of-credits account (zero
// balance, `billing_enforcement_enabled` on) AND — the load-bearing
// invariant of the whole design — that `add_context` (a write/capture) is
// completely unaffected by the SAME account being blocked. A final negative
// control clears the balance and re-proves `search_atoms` un-blocks.

#[tokio::test]
async fn billing_gate_blocks_ask_and_search_and_queries_but_never_add_context() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-billing-gate.example/mcp").await;
    let model = Arc::new(EvidenceModel {
        quote: "unused observer response",
        project: None,
        calls: std::sync::atomic::AtomicUsize::new(0),
        prompts: std::sync::Mutex::new(vec![]),
    });
    let embed = Arc::new(EmbeddingObserver(std::sync::atomic::AtomicUsize::new(0)));
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool.clone())));
    state.llm = Some(model.clone());
    state.embedder = embed.clone();
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let sub = fresh_sub("billing_gate");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // Seed this account into the BLOCKED state: zero org pool + exhausted
    // member seat allowance + enforcement enabled — all conditions
    // `is_blocked` requires (see that function's own doc comment in
    // `src/billing/enforcement.rs`). `provision` already auto-grants this
    // personal account a `free` plan at signup (real-grants-at-signup PR),
    // which writes a `member_allowance_periods` row with `granted_credits >
    // 0, consumed_credits = 0` — left alone, `consumed < granted` would
    // short-circuit `is_blocked` at step 2 before it ever reaches the pool
    // check below. Marking the period fully consumed (`consumed ==
    // granted`) falls through to the per-member cap check without tripping
    // it (`overage_used == 0 < cap`), then to the org-pool check this test
    // is actually trying to exercise.
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id)
        .await
        .unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id)
        .await
        .unwrap();
    sqlx::query("INSERT INTO credit_balances (account_id, balance) VALUES ($1, 0)")
        .bind(account_id)
        .execute(&mut *financial)
        .await
        .expect("seed zero balance");
    sqlx::query(
        "UPDATE member_allowance_periods SET consumed_credits = granted_credits \
         WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .execute(&mut *financial)
    .await
    .expect("exhaust member allowance period");
    financial.commit().await.unwrap();
    sqlx::query("UPDATE accounts SET billing_enforcement_enabled = true WHERE id = $1")
        .bind(account_id)
        .execute(&pool)
        .await
        .expect("enable enforcement");

    // ── Writes/captures must succeed regardless — the whole point ────────────
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({"ocean_id": graph_id, "content": "the roof contractor is zzbillinggate99 Roofing"}),
    )
    .await;
    assert!(
        !is_error,
        "add_context must succeed for a blocked account — writes/captures are never gated by \
         billing enforcement: {body:?}"
    );
    let capture = body.clone();
    let before:Value=sqlx::query_scalar("SELECT jsonb_build_array((SELECT count(*) FROM mcp_tasks WHERE account_id=$1),(SELECT count(*) FROM conversations WHERE account_id=$1),(SELECT count(*) FROM query_history WHERE account_id=$1))")
        .bind(account_id).fetch_one(&pool).await.unwrap();

    // ── search_atoms must be blocked ──────────────────────────────────────────
    let (is_error, body) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query": "zzbillinggate99", "ocean_ids": [graph_id]}),
    )
    .await;
    assert!(
        is_error,
        "search_atoms must be blocked for an out-of-credits account: {body:?}"
    );
    assert_eq!(
        error_code(&body),
        "payment_required",
        "unexpected error code: {body:?}"
    );

    // ── ask_ocean must be blocked (task settles `failed`) ─────────────────────
    let (is_error, body) = ask_ocean_and_wait(
        &app,
        &token,
        json!({"question": "zzbillinggate99", "ocean_ids": [graph_id]}),
    )
    .await;
    assert!(
        is_error,
        "ask_ocean must be blocked for an out-of-credits account: {body:?}"
    );
    assert_eq!(
        error_code(&body),
        "payment_required",
        "unexpected error code: {body:?}"
    );

    // ── POST /v1/queries must 402 ──────────────────────────────────────────────
    let req = Request::builder()
        .method("POST")
        .uri("/v1/queries")
        .header("Authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"text": "zzbillinggate99", "graph_id": graph_id}).to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::PAYMENT_REQUIRED,
        "POST /v1/queries must 402 when blocked"
    );
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        v["reason"], "insufficient_credits",
        "unexpected /v1/queries error body: {v:?}"
    );

    // ── POST /v1/queries/stream must ALSO 402 ─────────────────────────────────
    let req = Request::builder()
        .method("POST")
        .uri("/v1/queries/stream")
        .header("Authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"text": "zzbillinggate99", "graph_id": graph_id}).to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::PAYMENT_REQUIRED,
        "POST /v1/queries/stream must 402 when blocked"
    );

    let after:Value=sqlx::query_scalar("SELECT jsonb_build_array((SELECT count(*) FROM mcp_tasks WHERE account_id=$1),(SELECT count(*) FROM conversations WHERE account_id=$1),(SELECT count(*) FROM query_history WHERE account_id=$1))")
        .bind(account_id).fetch_one(&pool).await.unwrap();
    assert_eq!(
        after, before,
        "denied reads create no tasks, conversations or query history"
    );
    assert_eq!(model.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(embed.0.load(std::sync::atomic::Ordering::SeqCst), 0);

    // ── Negative control: clearing the balance un-blocks the gate ────────────
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id)
        .await
        .unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id)
        .await
        .unwrap();
    sqlx::query("UPDATE credit_balances SET balance = 500 WHERE account_id = $1")
        .bind(account_id)
        .execute(&mut *financial)
        .await
        .expect("clear balance");
    financial.commit().await.unwrap();
    let evidence = publish_retained(
        &app,
        &pool,
        &token,
        graph_id,
        &[(
            &capture,
            "the roof contractor is zzbillinggate99 Roofing",
            None,
        )],
    )
    .await;
    let (is_error, body) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query": "zzbillinggate99", "ocean_ids": [graph_id]}),
    )
    .await;
    assert!(
        !is_error,
        "search_atoms must succeed once the balance is positive again: {body:?}"
    );

    evidence[0].assert_cited(&body);
    let (error, funded) = ask_ocean_and_wait(
        &app,
        &token,
        json!({"question":"zzbillinggate99","ocean_ids":[graph_id],"style":"sources-only"}),
    )
    .await;
    assert!(!error, "{funded}");
    evidence[0].assert_cited(&funded);
    assert_eq!(model.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(embed.0.load(std::sync::atomic::Ordering::SeqCst), 0);

    cleanup(&pool, &[account_id], &[user_id]).await;
}

// ── Coding-session capture Phase 1 (audit A1/A2/A3/A4/A5/A10/P0-1/P1-3/P1-4) ──
//
// These prove the parts of the design that only a REAL Postgres can exercise:
// `coding_sessions`/`projects` (migration 0076) need a real database, so
// only this file can prove the happy path end to end.

/// `add_context` with `session_id` + a credential-bearing `remote` (audit
/// P0-1): the token must never survive into `coding_sessions.remote_sanitized`
/// (server-side re-normalization, A5) or leak into the persisted atom content
/// (the header only ever carries repo_path/branch/title, never the raw
/// remote). `project_key` is derived from the sanitized remote (A4), and a
/// `projects` alias-table row is lazily upserted for it (P1-4) in the same
/// call. The last-reported `branch` is stored on the session row
/// (migration 0220) AND still rides the embedded contextual chunk header
/// (A3) so per-checkpoint history stays searchable.
#[tokio::test]
async fn add_context_coding_session_strips_credentials_and_computes_project_key() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-coding-session.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("coding_session_creds");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "Fixed the off-by-one in the retrieval ranker.",
            "session_id": "sess-creds-1",
            "session_title": "Fix retrieval ranker bug",
            "repo_path": "/home/dev/coralswarm/coralswarm",
            "remote": "https://x-access-token:ghp_SUPERSECRET123@github.com/coralswarm/coralswarm.git",
            "hostname": "devbox-1",
            "harness_version": "1.2.3",
            "model": "claude-sonnet-5",
            "branch": "fix/ranker-off-by-one",
        }),
    )
    .await;
    assert!(!is_error, "add_context should succeed: {body:?}");

    let source_id = body["source_id"].as_str().expect("source_id echoed");
    assert!(
        source_id.starts_with("cc:sess-creds-1:"),
        "expected the cc:{{session_id}}:{{checkpoint}} convention, got {source_id}"
    );

    // ── coding_sessions row: credential stripped, project_key derived ──────
    let session = store
        .get_coding_session(graph_id, "sess-creds-1")
        .await
        .expect("get_coding_session")
        .expect("coding_sessions row must exist after add_context");
    assert_eq!(session.project_key.as_deref(), Some("github.com/coralswarm/coralswarm"));
    assert_eq!(
        session.remote_sanitized.as_deref(),
        Some("github.com/coralswarm/coralswarm")
    );
    assert_eq!(session.title.as_deref(), Some("Fix retrieval ranker bug"));
    assert_eq!(session.repo_path.as_deref(), Some("/home/dev/coralswarm/coralswarm"));
    assert_eq!(session.hostname.as_deref(), Some("devbox-1"));
    assert_eq!(session.harness_version.as_deref(), Some("1.2.3"));
    assert_eq!(session.model.as_deref(), Some("claude-sonnet-5"));
    assert_eq!(session.branch.as_deref(), Some("fix/ranker-off-by-one"));
    let dump = format!("{session:?}");
    assert!(!dump.contains("ghp_SUPERSECRET123"), "token must never reach coding_sessions: {dump}");
    assert!(!dump.contains("x-access-token"), "userinfo must never reach coding_sessions: {dump}");

    // ── projects alias table: lazily upserted for the derived key ──────────
    let (project_key, display_name): (String, Option<String>) = sqlx::query_as(
        "SELECT project_key, display_name FROM projects WHERE graph_id = $1 AND project_key = $2",
    )
    .bind(graph_id)
    .bind("github.com/coralswarm/coralswarm")
    .fetch_one(&pool)
    .await
    .expect("projects row must exist");
    assert_eq!(project_key, "github.com/coralswarm/coralswarm");
    assert_eq!(display_name.as_deref(), Some("github.com/coralswarm/coralswarm"));

    // ── the atom's persisted content must never leak the credential either,
    // and DOES carry the branch/repo contextual header (A3) ────────────────
    // `get_atom` deliberately STRIPS the header back out via `header_len`
    // (migration 0066's citation-cleanliness contract — see
    // `header_len_pg.rs`), so the header itself is only observable by
    // reading the raw stored text directly (this test harness has no R2
    // configured, so ingest stores it inline in `content_inline`).
    let ids = atom_ids_of(&body);
    assert_eq!(ids.len(), 1);
    let (is_error, atom_body) = call_tool(&app, &token, "get_atom", json!({"atom_id": ids[0]})).await;
    assert!(!is_error, "get_atom should succeed: {atom_body:?}");
    let displayed_content = atom_body["content"].as_str().expect("content string");
    assert!(
        !displayed_content.contains("ghp_SUPERSECRET123"),
        "token must never reach atom content: {displayed_content}"
    );
    assert!(
        !displayed_content.contains("x-access-token"),
        "userinfo must never reach atom content: {displayed_content}"
    );

    let (raw_content, header_len): (Option<String>, Option<i32>) =
        sqlx::query_as("SELECT content_inline, header_len FROM atoms WHERE atom_id = $1")
            .bind(ids[0])
            .fetch_one(&pool)
            .await
            .expect("fetch raw atom row");
    let raw_content = raw_content.expect("inline content (no R2 configured in this test harness)");
    assert!(header_len.unwrap_or(0) > 0, "header_len must be stamped when a coding-session header was built");
    assert!(
        !raw_content.contains("ghp_SUPERSECRET123"),
        "token must never reach even the RAW stored atom content: {raw_content}"
    );
    assert!(
        raw_content.contains("Branch: fix/ranker-off-by-one."),
        "coding-session header (A3) must be embedded in the raw atom content: {raw_content}"
    );
    assert!(
        raw_content.contains("Repo: /home/dev/coralswarm/coralswarm."),
        "coding-session header must carry repo_path: {raw_content}"
    );
    assert!(
        raw_content.contains("Session: Fix retrieval ranker bug."),
        "coding-session header must carry the session title: {raw_content}"
    );
    assert_eq!(
        coralswarm_backend::store::strip_header_prefix(&raw_content, header_len.unwrap()),
        displayed_content,
        "get_atom's displayed content must equal the raw content with the header stripped"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// `Store::upsert_coding_session`'s single-statement `ON CONFLICT` upsert
/// (audit P1-3): a second checkpoint on the SAME session bumps
/// `last_activity_at`, leaves `started_at` untouched, COALESCEs an omitted
/// field forward from the first call (`session_title` here), and applies a
/// supplied field as an update (`model` here) — proven end-to-end through
/// two real `add_context` calls, not just a direct `Store` call, so the
/// whole ingest-boundary wiring (truncation, normalization, upsert) is
/// covered together.
#[tokio::test]
async fn add_context_coding_session_upsert_coalesces_and_bumps_activity() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-coding-session-upsert.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("coding_session_upsert");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, _body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "First checkpoint of the session.",
            "session_id": "sess-upsert-1",
            "session_title": "Initial title",
            "model": "model-a",
        }),
    )
    .await;
    assert!(!is_error, "first add_context should succeed");

    let first = store
        .get_coding_session(graph_id, "sess-upsert-1")
        .await
        .expect("get_coding_session")
        .expect("row must exist after first checkpoint");
    assert_eq!(first.title.as_deref(), Some("Initial title"));
    assert_eq!(first.model.as_deref(), Some("model-a"));

    // Ensure a measurable time delta for the last_activity_at bump assertion.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let (is_error, _body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "Second checkpoint of the session.",
            "session_id": "sess-upsert-1",
            // session_title OMITTED — must NOT blank out "Initial title".
            "model": "model-b",
        }),
    )
    .await;
    assert!(!is_error, "second add_context should succeed");

    let second = store
        .get_coding_session(graph_id, "sess-upsert-1")
        .await
        .expect("get_coding_session")
        .expect("row must still exist after second checkpoint");
    assert_eq!(second.id, first.id, "same (graph_id, session_id) must upsert the SAME row");
    assert_eq!(
        second.title.as_deref(),
        Some("Initial title"),
        "an omitted title on the second call must COALESCE-preserve the first"
    );
    assert_eq!(second.model.as_deref(), Some("model-b"), "a supplied field must update");
    assert_eq!(second.started_at, first.started_at, "started_at must never change after the first insert");
    assert!(
        second.last_activity_at > first.last_activity_at,
        "last_activity_at must bump on every checkpoint: {:?} vs {:?}",
        second.last_activity_at,
        first.last_activity_at
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

// ── session_kind discriminator (capture blocker B1) ─────────────────────────

/// THE backward-compatibility proof for the `session_kind` discriminator, on
/// real Postgres and over real HTTP.
///
/// The unit tests in `ingest::atom` pin the enum's default without a
/// database, so they never touch the column migration `0185` actually
/// added. This does: the payload below is byte-for-byte what ships TODAY
/// (the MCP plugin, the `cs` CLI and the Claude Code harness primer all post
/// `session_id` and none of them sends `session_kind`), and it must still
/// produce `atoms.source_type = 'coding_session'` AND
/// `coding_sessions.session_kind = 'coding'`.
///
/// If either ever changes, every capture path that exists silently relabels
/// itself on deploy — permanently, because `atoms.source_type` is INSERT-only
/// across sync (migration 0098's KNOWN LIMITATION block).
///
/// `source_type: "note"` is still sent on purpose: the OTHER half of the old
/// rule (derive, never trust) must survive the change too.
#[tokio::test]
async fn add_context_without_session_kind_still_lands_coding_on_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-session-kind-default.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("session_kind_default");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "Checkpoint from a caller that predates session_kind.",
            "source_type": "note",
            "session_id": "sess-kind-default-1",
            // NOTE: no "session_kind" key at all — that is the whole point.
        }),
    )
    .await;
    assert!(!is_error, "add_context with no session_kind must succeed: {body:?}");

    let session = store
        .get_coding_session(graph_id, "sess-kind-default-1")
        .await
        .expect("get_coding_session")
        .expect("coding_sessions row must exist after add_context");
    assert_eq!(
        session.session_kind, "coding",
        "an absent session_kind must store the migration's 'coding' default"
    );

    let ids = atom_ids_of(&body);
    assert!(!ids.is_empty(), "the checkpoint must have produced at least one atom");
    for id in &ids {
        let source_type: String =
            sqlx::query_scalar("SELECT source_type FROM atoms WHERE atom_id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("fetch atom source_type");
        assert_eq!(
            source_type, "coding_session",
            "a session capture with NO session_kind must still derive \
             coding_session, byte for byte (atom {id})"
        );
    }

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// A declared `"meeting"` kind derives `meeting_session` end to end (ticket 11).
///
/// This is the SCREEN channel of an armed meeting. The audio channel of the
/// same meeting keeps `"transcript"` and never passes through this path — so
/// what this test pins is that the choke point produces a label DISTINCT from
/// `transcript`, not merely a non-empty one. Asserted against the string
/// literal rather than `MEETING_SESSION_SOURCE_TYPE` so it cannot go
/// tautological if the constant is edited, and `source_type: "note"` is sent
/// on purpose to prove the derive-never-trust half still holds for this kind.
#[tokio::test]
async fn add_context_with_meeting_session_kind_lands_meeting_session_on_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-session-kind-meeting.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("session_kind_meeting");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "Shared the north stair detail during the coordination call.",
            "source_type": "note",
            "session_id": "sess-kind-meeting-1",
            "session_kind": "meeting",
        }),
    )
    .await;
    assert!(
        !is_error,
        "add_context with session_kind=meeting must succeed: {body:?}"
    );

    let session = store
        .get_coding_session(graph_id, "sess-kind-meeting-1")
        .await
        .expect("get_coding_session")
        .expect("coding_sessions row must exist");
    assert_eq!(session.session_kind, "meeting");

    let ids = atom_ids_of(&body);
    assert!(!ids.is_empty(), "the meeting checkpoint must produce atoms");
    for id in &ids {
        let source_type: String =
            sqlx::query_scalar("SELECT source_type FROM atoms WHERE atom_id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("fetch atom source_type");
        assert_eq!(
            source_type, "meeting_session",
            "a session declared \"meeting\" must derive meeting_session (atom {id})"
        );
        assert_ne!(
            source_type, "transcript",
            "the SCREEN channel must never land under the audio channel's label"
        );
    }

    cleanup(&pool, &[account_id], &[user_id]).await;
}

#[tokio::test]
async fn add_context_cannot_flip_an_existing_sessions_kind_and_rolls_the_request_back() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-session-kind-conflict.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("session_kind_conflict");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // First checkpoint fixes the kind (default → "coding").
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "First checkpoint — this session is a coding session.",
            "session_id": "sess-kind-conflict-1",
        }),
    )
    .await;
    assert!(!is_error, "first add_context should succeed: {body:?}");

    let atoms_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .expect("count atoms before");
    assert!(
        atoms_before > 0,
        "the first checkpoint must have written atoms"
    );

    let writes_before:Value=sqlx::query_scalar("SELECT jsonb_build_array((SELECT count(*) FROM source_items WHERE graph_id=$1),(SELECT count(*) FROM source_versions WHERE graph_id=$1),(SELECT count(*) FROM coding_sessions WHERE graph_id=$1),(SELECT count(*) FROM jobs WHERE payload->>'graph_id'=$1::text))")
        .bind(graph_id).fetch_one(&pool).await.unwrap();

    // Second checkpoint on the SAME (graph_id, session_id) declares a
    // DIFFERENT kind. A fresh `checkpoint_id` is generated per call, so its
    // `source_id` is distinct — nothing but the rollback can keep these atoms
    // out of the table.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "Second checkpoint, now claiming to be a meeting.",
            "session_id": "sess-kind-conflict-1",
            "session_kind": "meeting",
        }),
    )
    .await;
    assert!(
        is_error,
        "flipping an existing session's kind must be refused: {body:?}"
    );
    // NOT `service_unavailable` — that code reads as transient and tells a
    // model to retry, and this failure is permanent by construction (the
    // identical call fails identically forever). Pins the `CONFLICT` arm in
    // `ingest_response_to_tool_result`, without which a 409 falls through to
    // the `_ =>` default and every MCP client retries a doomed checkpoint.
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("invalid_params"),
        "a kind conflict must surface as a permanent caller error: {body:?}"
    );
    // The stored kind is untouched...
    let session = store
        .get_coding_session(graph_id, "sess-kind-conflict-1")
        .await
        .expect("get_coding_session")
        .expect("coding_sessions row must still exist");
    assert_eq!(
        session.session_kind, "coding",
        "a refused checkpoint must not flip the stored kind"
    );

    // ...and the refused request wrote NO atoms.
    let atoms_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .expect("count atoms after");
    assert_eq!(
        atoms_after, atoms_before,
        "the refused checkpoint's atoms must have rolled back with the \
         request transaction — a committed meeting_session atom under a coding \
         session could never be relabelled"
    );
    let stray: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM atoms WHERE graph_id = $1 AND source_type = 'meeting_session'",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .expect("count meeting_session atoms");
    assert_eq!(stray, 0, "no meeting_session atom may survive the refusal");

    let writes_after:Value=sqlx::query_scalar("SELECT jsonb_build_array((SELECT count(*) FROM source_items WHERE graph_id=$1),(SELECT count(*) FROM source_versions WHERE graph_id=$1),(SELECT count(*) FROM coding_sessions WHERE graph_id=$1),(SELECT count(*) FROM jobs WHERE payload->>'graph_id'=$1::text))")
        .bind(graph_id).fetch_one(&pool).await.unwrap();
    assert_eq!(writes_after,writes_before,"permanent refusal writes no source identity, version, session or job");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Over-cap machine-generated coding-session labels are TRUNCATED, never
/// rejected with a 422 (audit A10) — an automated harness stamping a
/// too-long `hostname` on every checkpoint must never fail the whole ingest
/// over cosmetic metadata overflow.
#[tokio::test]
async fn add_context_coding_session_truncates_overlong_machine_labels() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-coding-session-truncate.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("coding_session_truncate");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let oversized_hostname = "h".repeat(500);
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "checkpoint with an oversized hostname",
            "session_id": "sess-truncate-1",
            "hostname": oversized_hostname,
        }),
    )
    .await;
    assert!(!is_error, "over-cap machine labels must be truncated, never rejected: {body:?}");

    let session = store
        .get_coding_session(graph_id, "sess-truncate-1")
        .await
        .expect("get_coding_session")
        .expect("row must exist");
    let stored_hostname = session.hostname.expect("hostname must be stored (truncated)");
    assert_eq!(
        stored_hostname.chars().count(),
        256,
        "hostname must be truncated to the 256-char cap, got {} chars",
        stored_hostname.chars().count()
    );
    assert!(oversized_hostname.starts_with(&stored_hostname));

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// `add_context` with `session_id` when no `remote` is supplied falls back to
/// the `repo_path` basename for `project_key` (audit A5's fallback path,
/// never URL-parsed).
#[tokio::test]
async fn add_context_coding_session_falls_back_to_repo_path_basename_for_project_key() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-coding-session-fallback.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("coding_session_fallback");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "a checkpoint with no git remote at all",
            "session_id": "sess-fallback-1",
            "repo_path": "/home/dev/Scratchpad",
        }),
    )
    .await;
    assert!(!is_error, "add_context should succeed: {body:?}");

    let session = store
        .get_coding_session(graph_id, "sess-fallback-1")
        .await
        .expect("get_coding_session")
        .expect("row must exist");
    assert_eq!(session.remote_sanitized, None, "no remote was supplied");
    assert_eq!(
        session.project_key.as_deref(),
        Some("scratchpad"),
        "project_key should fall back to the lowercased repo_path basename"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// `search_atoms` negotiates the `structuredContent`-only response shape
/// (`mcp::tools::structured_only_client`, Track B of the search/ask
/// retrieval roadmap) end-to-end over a REAL `/mcp` HTTP request: a caller
/// whose `User-Agent` matches the built-in `claude-code` hint gets
/// `structuredContent` with an EMPTY `content` array (no duplicated text
/// block), while a caller with an unrecognized `User-Agent` (or none at
/// all — every other test in this file) gets today's unchanged dual
/// `structuredContent` + text-duplicate payload. Both requests carry
/// identical arguments and hit the identical dispatcher path
/// (`CoralSwarmMcp::search_atoms` via the real streamable-HTTP transport in
/// stateless mode) — the ONLY difference is the `User-Agent` header, so this
/// isolates the negotiation itself rather than anything about the tool.
#[tokio::test]
async fn search_atoms_negotiates_structured_only_response_over_real_http() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-negotiation.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("negotiation");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "The zzqnegotiate99 rollout shipped the structured-only MCP negotiation.",
        }),
    )
    .await;
    assert!(!is_error, "add_context should succeed: {body:?}");
    let evidence=publish_retained(&app,&pool,&token,graph_id,&[(&body,"The zzqnegotiate99 rollout shipped the structured-only MCP negotiation.",None)]).await;

    // ── Known structured-capable client: structuredContent only ────────────
    let raw = raw_call_tool(
        &app,
        &token,
        Some("claude-code/1.0"),
        "search_atoms",
        json!({"query": "zzqnegotiate99", "ocean_ids": [graph_id]}),
    )
    .await;
    assert_eq!(raw["isError"].as_bool().unwrap_or(false), false, "{raw:?}");
    let content = raw["content"].as_array().expect("content array");
    assert!(
        content.is_empty(),
        "known structured-capable client must get an EMPTY content array: {raw:?}"
    );
    let structured = raw
        .get("structuredContent")
        .unwrap_or_else(|| panic!("structuredContent missing: {raw:?}"));
    evidence[0].assert_cited(structured);
    assert_eq!(structured["retained_citations"].as_array().unwrap().len(),1);

    // ── Unknown client: unchanged dual-payload behavior ─────────────────────
    let raw = raw_call_tool(
        &app,
        &token,
        Some("some-other-tool/9.9"),
        "search_atoms",
        json!({"query": "zzqnegotiate99", "ocean_ids": [graph_id]}),
    )
    .await;
    assert_eq!(raw["isError"].as_bool().unwrap_or(false), false, "{raw:?}");
    let content = raw["content"].as_array().expect("content array");
    assert_eq!(
        content.len(),
        1,
        "unknown client must keep the legacy text-duplicate content block: {raw:?}"
    );
    let text = content[0]["text"].as_str().expect("text content block");
    let text_body: Value = serde_json::from_str(text).expect("text block is JSON");
    let structured = raw
        .get("structuredContent")
        .unwrap_or_else(|| panic!("structuredContent missing: {raw:?}"));
    assert_eq!(
        &text_body, structured,
        "text block must still equal structuredContent verbatim for a dual-payload response"
    );

    evidence[0].assert_cited(structured);
    assert_eq!(structured["retained_citations"].as_array().unwrap().len(),1);

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// `add_context` attributes the coral to the authenticated caller
/// (`author_id`/`author_name`) and, when `participants` is omitted entirely,
/// defaults it to the caller's own identity — the fix this file's other
/// `add_context` tests predate. `author_id`/`participants`-auto-append can
/// only be proven against a REAL Postgres pool: `ingest::submit`'s
/// `has_metadata`-gated `UPDATE atoms` (which stamps `author_id`/
/// `author_name`) only runs on the cloud branch, and `caller_display_name`
/// (`mcp/tools/write.rs`) needs `state.db` to resolve the caller's email.
///
/// No `CLERK_SECRET_KEY` is set for this test env, so `clerk_provision::
/// ensure_user`'s `fetch_primary_email` short-circuits and the caller's
/// email is the synthetic `"{sub}@users.clerk.local"` fallback — asserted
/// against directly rather than re-deriving it, so a regression in that
/// fallback format would fail loudly here too.
///
/// `write_token` mints no `name` claim, and `CLERK_SECRET_KEY` is unset (so
/// `fetch_display_name`'s API fallback short-circuits too), so `users.name`
/// stays `NULL` throughout — this test doubles as the "name absent -> falls
/// back to email" coverage for `caller_display_name` (migration 0067). The
/// companion "name present -> preferred over email" case is
/// `add_context_prefers_users_name_over_email_when_both_present_against_real_postgres`.
#[tokio::test]
async fn add_context_stamps_caller_author_and_defaults_participants_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-author-attribution.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("author_attribution");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let expected_email = format!("{sub}@users.clerk.local");

    let stored_name: Option<String> = sqlx::query_scalar("SELECT name FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("provisioned user row");
    assert_eq!(
        stored_name, None,
        "no name claim + no CLERK_SECRET_KEY -> users.name stays NULL: {stored_name:?}"
    );

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "attribution smoke test coral, no participants supplied",
        }),
    )
    .await;
    assert!(!is_error, "add_context should succeed: {body:?}");
    let ids = atom_ids_of(&body);
    assert_eq!(ids.len(), 1);

    let (is_error, body) = call_tool(&app, &token, "get_atom", json!({"atom_id": ids[0]})).await;
    assert!(!is_error, "get_atom should succeed: {body:?}");
    assert_eq!(
        body["author_id"],
        user_id.to_string(),
        "author_id must be stamped from the authenticated caller: {body:?}"
    );
    assert_eq!(
        body["author_name"], expected_email,
        "author_name must fall back to the caller's email when users.name is NULL: {body:?}"
    );
    assert_eq!(
        body["participants"],
        json!([expected_email]),
        "omitted participants must default to the caller's own identity: {body:?}"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Companion to the fallback test above: when the caller's token DOES carry
/// an OIDC `name` claim (mirroring a real Clerk OAuth token minted with the
/// `profile` scope — the MCP flow always requests `openid profile email`),
/// `clerk_provision::ensure_user` stores it on `users.name` at first sight,
/// and `caller_display_name` (`mcp/tools/write.rs`) must prefer it over the
/// caller's email for both `author_name` and the auto-appended
/// `participants` entry.
///
/// Also exercises the SECOND way `users.name` can be set:
/// `sync_user_display_name`'s fire-and-forget refresh for an EXISTING user
/// (`middleware::auth::resolve_clerk_context`'s `tokio::spawn` +
/// `DISPLAY_NAME_REFRESH_TIMEOUT`-bounded task). That path is deliberately
/// NOT awaited on the request's own critical path, so the re-auth phase
/// below polls (`poll_users_name`) rather than asserting immediately after
/// the triggering call returns.
#[tokio::test]
async fn add_context_prefers_users_name_over_email_when_both_present_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-name-preferred.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("name_preferred");

    // First sight: `ensure_user` persists the name claim synchronously as
    // part of the provisioning INSERT — no polling needed for this phase.
    let (user_id, account_id, graph_id, token) =
        provision_with_name(&app, &pool, &sub, Some("Ada Lovelace")).await;

    let stored_name: Option<String> = sqlx::query_scalar("SELECT name FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("provisioned user row");
    assert_eq!(
        stored_name.as_deref(),
        Some("Ada Lovelace"),
        "ensure_user must persist the name claim onto users.name synchronously at first sight: {stored_name:?}"
    );

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "name-preference smoke test coral, no participants supplied",
        }),
    )
    .await;
    assert!(!is_error, "add_context should succeed: {body:?}");
    let ids = atom_ids_of(&body);
    assert_eq!(ids.len(), 1);

    let (is_error, body) = call_tool(&app, &token, "get_atom", json!({"atom_id": ids[0]})).await;
    assert!(!is_error, "get_atom should succeed: {body:?}");
    assert_eq!(
        body["author_id"],
        user_id.to_string(),
        "author_id must be stamped from the authenticated caller: {body:?}"
    );
    assert_eq!(
        body["author_name"], "Ada Lovelace",
        "author_name must prefer users.name over users.email: {body:?}"
    );
    assert_eq!(
        body["participants"],
        json!(["Ada Lovelace"]),
        "omitted participants must default to the caller's name, not their email: {body:?}"
    );

    // Re-authenticate as the SAME (now-existing) identity with an UPDATED
    // name claim. `ensure_user` takes its early-return branch this time
    // (existing `clerk_user_id`), so the new name can only land via
    // `sync_user_display_name`'s fire-and-forget spawned task.
    let updated_token = mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "scope": "coralswarm:read coralswarm:write",
        "client_id": "mcp_pg_e2e_client",
        "name": "Ada Lovelace-Byron",
    }));
    let (is_error, body) = call_tool(&app, &updated_token, "list_oceans", json!({})).await;
    assert!(!is_error, "re-auth list_oceans call failed: {body:?}");

    let refreshed_name =
        poll_users_name(&pool, user_id, "Ada Lovelace-Byron", std::time::Duration::from_secs(3)).await;
    assert_eq!(
        refreshed_name.as_deref(),
        Some("Ada Lovelace-Byron"),
        "background users.name refresh (existing user, updated name claim) must eventually land: {refreshed_name:?}"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Caller-supplied `participants` (even non-empty) must be used verbatim —
/// never appended to with the caller's own identity — while `author_id`/
/// `author_name` are still stamped independently of what `participants`
/// contains.
#[tokio::test]
async fn add_context_preserves_caller_supplied_participants_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-participants-preserved.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("participants_preserved");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let expected_email = format!("{sub}@users.clerk.local");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "attribution smoke test coral, explicit participants supplied",
            "participants": ["Alice", "Bob"],
        }),
    )
    .await;
    assert!(!is_error, "add_context should succeed: {body:?}");
    let ids = atom_ids_of(&body);
    assert_eq!(ids.len(), 1);

    let (is_error, body) = call_tool(&app, &token, "get_atom", json!({"atom_id": ids[0]})).await;
    assert!(!is_error, "get_atom should succeed: {body:?}");
    assert_eq!(
        body["participants"],
        json!(["Alice", "Bob"]),
        "caller-supplied participants must be used verbatim, not appended to: {body:?}"
    );
    assert_eq!(
        body["author_id"],
        user_id.to_string(),
        "author_id is stamped independently of caller-supplied participants: {body:?}"
    );
    assert_eq!(body["author_name"], expected_email);

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// `mcp::auth::McpAuthContext::mcp_client_name` (migration 0072) is populated
/// from the REAL request's `User-Agent` HTTP header — the in-crate
/// `mcp::tools::write` unit tests inject an `McpAuthContext` directly and so
/// never actually exercise `mcp_auth`'s own header-reading code path (see
/// that test module's `add_context_stamps_project_and_mcp_client_name` doc
/// comment); this is that missing end-to-end proof, driven through the real
/// public HTTP router with a genuine `User-Agent` header, against a REAL
/// Postgres pool so the cloud `ingest::submit` branch (which now calls
/// `Store::stamp_atom_provenance` instead of a hand-rolled inline UPDATE —
/// CodeRabbit review) is what actually persists it.
///
/// Regression coverage for a review finding: an earlier version of this
/// end-to-end proof lived as an in-crate `mcp::auth` unit test against
/// `test_app_state()` (no `db`), which 503s BEFORE `mcp_auth` ever reaches
/// its `extract_client_name` call (that function runs after the
/// `mcp.app.db` presence check, not before) — so that test passed without
/// actually proving the header was read or stored anywhere.
#[tokio::test]
async fn add_context_stamps_mcp_client_name_from_user_agent_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-client-name.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("mcp_client_name");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // `raw_call_tool`, not `call_tool`: "claude-code/1.0" now also matches
    // the structuredContent-only negotiation's built-in hint
    // (`mcp::tools::structured_only_client`), so `content` comes back EMPTY
    // for this call — `call_tool`'s `result["content"][0]["text"]`
    // extraction would panic on that. Reading `structuredContent` instead is
    // unaffected by the negotiation either way, so it stays the right
    // assertion surface for this test's actual subject (mcp_client_name
    // provenance stamping, not response-payload shape — that's
    // `search_atoms_negotiates_structured_only_response_over_real_http`'s
    // job).
    let raw = raw_call_tool(
        &app,
        &token,
        Some("claude-code/1.0"),
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "mcp_client_name smoke test coral",
            "project": "coralswarm",
        }),
    )
    .await;
    assert_eq!(raw["isError"].as_bool().unwrap_or(false), false, "add_context should succeed: {raw:?}");
    let body = raw
        .get("structuredContent")
        .unwrap_or_else(|| panic!("structuredContent missing: {raw:?}"));
    let ids = atom_ids_of(body);
    assert_eq!(ids.len(), 1);

    let (is_error, body) = call_tool(&app, &token, "get_atom", json!({"atom_id": ids[0]})).await;
    assert!(!is_error, "get_atom should succeed: {body:?}");
    assert_eq!(
        body["mcp_client_name"], "claude-code/1.0",
        "mcp_client_name must be stamped from the request's real User-Agent header: {body:?}"
    );
    assert_eq!(
        body["project"], "coralswarm",
        "caller-supplied project must land alongside mcp_client_name: {body:?}"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Layer A gap (a): `add_context` into a non-personal (team/org) ocean must
/// reject with the route's EXACT message (ADR-0007 D7).
#[tokio::test]
async fn cloud_only_personal_ocean_ingest_rejection_matches_route_message() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-personal-only.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("personal_only");
    let (user_id, account_id, _personal_graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // `kind = "org"`, not `"team"`: `graph_reachable_by_user` requires an
    // EXTRA `teams`/`team_members` join for `kind = 'team'` ocean (nothing
    // this test seeds), so a bare `store.create_graph(..., "team")` would be
    // unreachable-by-account-membership alone and the tool would deny with
    // `not_found` before ever reaching the personal-ocean-only check this
    // test targets. `"org"` reachability only requires `account_members`
    // membership (which `ensure_personal_account`'s owner already has),
    // while still being non-personal — exactly what `ingest::submit`'s
    // `g.kind == "personal"` gate needs to reject.
    let org_graph = store
        .create_graph(account_id, "Org Ocean", "org")
        .await
        .unwrap();

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({"ocean_id": org_graph.id, "content": "should not land in an org ocean"}),
    )
    .await;
    assert!(
        is_error,
        "add_context into an org ocean must be rejected: {body:?}"
    );
    assert_eq!(error_code(&body), "invalid_params");
    assert_eq!(
        body["error"]["message"], "can only ingest into your own private ocean; share via promotion",
        "must mirror routes/ingest.rs's exact message verbatim (updated by member-private-oceans \
         PR 5 to cover the widened personal/member gate): {body:?}"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// HTTP `/v1/ingest/atom` and MCP `add_context` both enter through
/// `ingest::submit` (ADR-0016). Pins what each returns for a new capture, an
/// identical re-capture (content-hash dedupe), and a shared-ocean destination
/// (D7), so moving either transport cannot change it.
#[tokio::test]
async fn http_and_mcp_ingest_share_the_door_contract() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-ingest-door.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());
    let sub = fresh_sub("ingest_door");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let org_graph = store.create_graph(account_id, "Org Ocean", "org").await.unwrap();

    let http = |body: Value| {
        let app = app.clone();
        let token = token.clone();
        async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/ingest/atom")
                        .header("Authorization", format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            (status, serde_json::from_slice::<Value>(&bytes).unwrap())
        }
    };
    let note = |source_id: &str, graph: Uuid| {
        json!({"graph_id": graph, "source_type": "note", "source_id": source_id,
               "content": "The door pin: zzqdoor42 lands once.", "participants": [],
               "timestamp": "2026-10-01T12:00:00Z"})
    };

    let (status, first) = http(note("door-http-1", graph_id)).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(atom_ids_of(&first).len(), 1, "{first}");
    assert_eq!(first["chunks_created"], 1);
    assert_eq!(first["jobs_enqueued"], 1);
    assert_eq!(first["skipped_duplicate_chunks"], 0);
    assert_eq!(first["replaced_chunks"], 0);
    assert_eq!(first["retired_chunks"], 0);
    assert!(first["processing_status"].is_string(), "{first}");
    let (status, again) = http(note("door-http-1", graph_id)).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert!(atom_ids_of(&again).is_empty(), "identical re-capture writes nothing: {again}");
    assert_eq!(again["chunks_created"], 1);
    assert_eq!(again["jobs_enqueued"], 0);
    assert_eq!(again["skipped_duplicate_chunks"], 1);
    let (status, refused) = http(note("door-http-2", org_graph.id)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");
    assert_eq!(refused, json!({"error": "can only ingest into your own private ocean; share via promotion"}));

    let add = json!({"ocean_id": graph_id, "content": "The door pin: zzqdoor43 lands once.", "source_id": "door-mcp-1"});
    let (is_error, first) = call_tool(&app, &token, "add_context", add.clone()).await;
    assert!(!is_error, "{first}");
    assert_eq!(atom_ids_of(&first).len(), 1, "{first}");
    assert_eq!(first["chunks_created"], 1);
    assert_eq!(first["jobs_enqueued"], 1);
    assert_eq!(first["skipped_duplicate_chunks"], 0);
    assert_eq!(first["ocean_id"], graph_id.to_string());
    assert_eq!(first["source_id"], "door-mcp-1");
    let (is_error, again) = call_tool(&app, &token, "add_context", add).await;
    assert!(!is_error, "{again}");
    assert!(atom_ids_of(&again).is_empty(), "identical re-capture writes nothing: {again}");
    assert_eq!(again["jobs_enqueued"], 0);
    assert_eq!(again["skipped_duplicate_chunks"], 1);
    let (is_error, refused) = call_tool(
        &app,
        &token,
        "add_context",
        json!({"ocean_id": org_graph.id, "content": "never lands", "source_id": "door-mcp-2"}),
    )
    .await;
    assert!(is_error, "{refused}");
    assert_eq!(error_code(&refused), "invalid_params");
    assert_eq!(refused["error"]["message"], "can only ingest into your own private ocean; share via promotion");

    let landed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE graph_id = $1 OR source_id IN ('door-http-2:chunk:0', 'door-mcp-2:chunk:0')",
    )
    .bind(org_graph.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(landed, 0, "a refused destination writes nothing anywhere");
    let chunks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_id IN ('door-http-1:chunk:0', 'door-mcp-1:chunk:0')",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(chunks, 2, "one chunk per source, deduped on re-capture");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// [`app_for_pool`] with an object store: a fresh local S3 stand-in
/// (`coralswarm_backend::test_object_store`) where the first `accept` object
/// PUTs succeed and every later one answers 503, as R2 does during an outage.
/// Each call gets its own stand-in and counts.
async fn app_with_failing_r2(pool: sqlx::PgPool, accept: usize) -> (axum::Router, coralswarm_backend::test_object_store::Counts) {
    let (r2, counts) = coralswarm_backend::test_object_store::failing_after(accept).await;
    (app_with_r2(pool, r2), counts)
}

/// [`app_for_pool`] with `r2` as its object store.
fn app_with_r2(pool: sqlx::PgPool, r2: coralswarm_core::r2::R2Client) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    state.r2 = Some(Arc::new(r2));
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// `POST /v1/ingest/atom` with `note` as `token`: the status and JSON body.
async fn post_note(app: axum::Router, token: &str, note: &Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/ingest/atom")
                .header("Authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(note.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    (status, serde_json::from_slice::<Value>(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap())
}

/// The documented storage-outage body.
fn assert_storage_unavailable(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], "storage_unavailable", "{body}");
    assert_eq!(body["error"], "object storage is unavailable; nothing was stored, retry ingest");
    assert!(body["cleanup_status"].is_string(), "{body}");
}

/// What landed for one source: its versions and its chunk atoms.
async fn landed(pool: &sqlx::PgPool, graph_id: Uuid, source_id: &str) -> (i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM source_versions v JOIN source_items s ON s.source_item_id = v.source_item_id \
                  WHERE s.graph_id = $1 AND s.source_id = $2), \
                (SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_id LIKE $2 || ':chunk:%')",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// ADR-0016 §4: an upload failure aborts every capture. HTTP answers 503
/// with a retryable code and MCP `add_context` a `service_unavailable` tool
/// error, and nothing is stored: no version, no coral, no embed job. Before
/// layer 4 the private arm dropped the bodiless row and answered 200 with
/// the version kept and the coral gone. Both the first upload failing (the
/// source's own body, `accept` 0) and a later one (the chunk body after the
/// source's landed, `accept` 1) abort, on HTTP and on MCP: each call gets
/// its own stand-in, so each sees exactly that failure. What landed is
/// deleted again. Once the store is back, the same request lands.
#[tokio::test]
async fn a_failed_upload_stores_nothing_and_answers_a_retryable_error() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-r2-abort.example/mcp").await;
    for accept in [0, 1] {
        let sub = fresh_sub(&format!("r2_abort_{accept}"));
        let (user_id, account_id, graph_id) = provision(&app_for_pool(pool.clone()), &pool, &sub).await;
        let token = write_token(&sub);
        let http_source = format!("r2-abort-http-{accept}");
        let note = json!({"graph_id": graph_id, "source_type": "note", "source_id": http_source,
                          "content": "This coral waits for the object store to come back.",
                          "participants": [], "timestamp": "2026-10-01T12:00:00Z"});

        // One body to the source, one to its single chunk: `accept` 0 fails
        // the source's, `accept` 1 the chunk's, and nothing follows it.
        let (app, store) = app_with_failing_r2(pool.clone(), accept).await;
        let (status, body) = post_note(app, &token, &note).await;
        assert_storage_unavailable(status, &body);
        assert_eq!(store.puts(), accept + 1, "accept {accept}: the refused upload is the last one");
        assert_eq!(store.deletes(), accept, "accept {accept}: what landed is deleted again");
        assert_eq!(landed(&pool, graph_id, &http_source).await, (0, 0), "accept {accept}: nothing stored");

        let (app, store) = app_with_failing_r2(pool.clone(), accept).await;
        let mcp_source = format!("r2-abort-mcp-{accept}");
        let (is_error, refused) = call_tool(
            &app,
            &token,
            "add_context",
            json!({"ocean_id": graph_id, "content": "Nor does this one land.", "source_id": mcp_source}),
        )
        .await;
        assert!(is_error, "{refused}");
        assert_eq!(error_code(&refused), "service_unavailable", "a model reads this code as retry: {refused}");
        assert_eq!(refused["error"]["message"], "object storage is unavailable; nothing was stored, retry ingest");
        assert_eq!(store.puts(), accept + 1, "accept {accept}: MCP sees the same failure");
        assert_eq!(store.deletes(), accept, "accept {accept}: what landed is deleted again");
        assert_eq!(landed(&pool, graph_id, &mcp_source).await, (0, 0));

        let embed_jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'EmbedAtoms' AND account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(embed_jobs, 0, "accept {accept}: no embed job for a coral that never landed");

        // The store is back: the identical request lands.
        let (status, body) = post_note(app_for_pool(pool.clone()), &token, &note).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(atom_ids_of(&body).len(), 1, "{body}");
        assert_eq!(body["jobs_enqueued"], 1);
        assert_eq!(landed(&pool, graph_id, &http_source).await, (1, 1));

        cleanup(&pool, &[account_id], &[user_id]).await;
    }
}

/// A capture with many chunks uploads its bodies in batches of eight
/// (`ingest::capture::UPLOAD_CONCURRENCY`). Once a batch has a failed
/// upload the capture aborts, so no later batch is uploaded, and every body
/// that did land (the source's, the whole first batch and part of the
/// second) is deleted by the rollback's cleanup.
#[tokio::test]
async fn a_failed_upload_batch_stops_the_capture_and_cleans_up_what_landed() {
    const BATCH: usize = 8;
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-r2-batch.example/mcp").await;
    let sub = fresh_sub("r2_batch");
    let (user_id, account_id, graph_id) = provision(&app_for_pool(pool.clone()), &pool, &sub).await;
    let token = write_token(&sub);
    let content = (0..1500).map(|i| format!("Sentence number {i} about the rollout plan.")).collect::<Vec<_>>().join(" ");
    let note = json!({"graph_id": graph_id, "source_type": "note", "source_id": "r2-batch",
                      "content": content, "participants": [], "timestamp": "2026-10-01T12:00:00Z"});

    // The source's body, the first batch, and two of the second land.
    let accept = 1 + BATCH + 2;
    let (app, store) = app_with_failing_r2(pool.clone(), accept).await;
    let (status, body) = post_note(app, &token, &note).await;
    assert_storage_unavailable(status, &body);
    assert_eq!(body["cleanup_status"], "completed", "{body}");
    assert_eq!(store.puts(), 1 + 2 * BATCH, "no batch is uploaded after the failing one");
    assert_eq!(store.deletes(), accept, "every body that landed is deleted again");
    assert_eq!(landed(&pool, graph_id, "r2-batch").await, (0, 0), "nothing stored");

    // The store is back: the capture lands, with more chunks than the two
    // batches the failed attempt reached.
    let (status, body) = post_note(app_for_pool(pool.clone()), &token, &note).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let chunks = body["chunks_created"].as_u64().unwrap() as usize;
    assert!(chunks > 2 * BATCH, "the source needs a third batch: {chunks} chunks");
    assert_eq!(landed(&pool, graph_id, "r2-batch").await, (1, chunks as i64));

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// An edited re-capture during an outage: the new version's body lands, and
/// the edited chunk's replacement body is refused inside the chunk
/// reconcile. That is the same storage outage as a refused chunk upload:
/// 503 `storage_unavailable`, not a 500, and nothing is stored (no second
/// version, the chunk keeps its old text and revision, no embed job). The
/// retry lands the edit.
#[tokio::test]
async fn an_edited_recapture_during_an_outage_answers_storage_unavailable() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-r2-recapture.example/mcp").await;
    let sub = fresh_sub("r2_recapture");
    let (user_id, account_id, graph_id) = provision(&app_for_pool(pool.clone()), &pool, &sub).await;
    let token = write_token(&sub);
    let note = |content: &str| {
        json!({"graph_id": graph_id, "source_type": "note", "source_id": "r2-recapture",
               "content": content, "participants": [], "timestamp": "2026-10-01T12:00:00Z"})
    };
    let chunk_state = || async {
        sqlx::query_as::<_, (Uuid, i64, Option<String>)>(
            "SELECT atom_id, content_revision, chunk_sha256 FROM atoms WHERE graph_id = $1 AND source_id = 'r2-recapture:chunk:0'",
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap()
    };
    let embed_jobs = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE kind = 'EmbedAtoms' AND account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap()
    };

    let (app, _) = app_with_failing_r2(pool.clone(), usize::MAX).await;
    let (status, body) = post_note(app, &token, &note("The pilot ships on Friday.")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let before = chunk_state().await;
    let jobs_before = embed_jobs().await;

    // The new version's body is accepted; the replacement chunk body is not.
    let (app, store) = app_with_failing_r2(pool.clone(), 1).await;
    let (status, body) = post_note(app, &token, &note("The pilot ships on Monday.")).await;
    assert_storage_unavailable(status, &body);
    assert_eq!(store.puts(), 2, "the version's body, then the refused replacement");
    assert_eq!(store.deletes(), 1, "the version's body is deleted again");
    assert_eq!(landed(&pool, graph_id, "r2-recapture").await, (1, 1), "no second version");
    assert_eq!(chunk_state().await, before, "the chunk keeps its text and revision");
    assert_eq!(embed_jobs().await, jobs_before, "no embed job");

    let (app, _) = app_with_failing_r2(pool.clone(), usize::MAX).await;
    let (status, body) = post_note(app, &token, &note("The pilot ships on Monday.")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["replaced_chunks"], 1, "{body}");
    assert_eq!(landed(&pool, graph_id, "r2-recapture").await, (2, 1));

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Layer A gap (b): `confirm_reef` / `reject_reef` / `update_reef` against a
/// REAL `PostgresStore`.
#[tokio::test]
async fn confirm_reject_update_reef_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-reef-transitions.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("reef_transitions");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // confirm_reef: succeeds once, then not_found on repeat.
    let confirm_id =
        seed_pending_decision(&store, graph_id, user_id, "Adopt pgvector for retrieval").await;
    let (is_error, body) = call_tool(
        &app,
        &token,
        "confirm_reef",
        json!({"reef_id": confirm_id, "rationale": "cheaper than a dedicated vector DB"}),
    )
    .await;
    assert!(!is_error, "confirm_reef should succeed: {body:?}");
    assert_eq!(body["review_status"], "confirmed");
    assert_eq!(body["rationale"], "cheaper than a dedicated vector DB");

    let (is_error, body) =
        call_tool(&app, &token, "confirm_reef", json!({"reef_id": confirm_id})).await;
    assert!(
        is_error,
        "repeat confirm_reef must be idempotent-not_found: {body:?}"
    );
    assert_eq!(error_code(&body), "not_found");

    // reject_reef: succeeds once, then not_found on repeat.
    let reject_id =
        seed_pending_decision(&store, graph_id, user_id, "Skip the vendor migration").await;
    let (is_error, body) = call_tool(
        &app,
        &token,
        "reject_reef",
        json!({"reef_id": reject_id, "reason": "duplicate"}),
    )
    .await;
    assert!(!is_error, "reject_reef should succeed: {body:?}");
    assert_eq!(body["review_status"], "rejected");

    let (is_error, body) =
        call_tool(&app, &token, "reject_reef", json!({"reef_id": reject_id})).await;
    assert!(
        is_error,
        "repeat reject_reef must be idempotent-not_found: {body:?}"
    );
    assert_eq!(error_code(&body), "not_found");

    // update_reef: before/after snapshot.
    let entity_id = store
        .upsert_node(graph_id, "entity", "Roof Contractor")
        .await
        .unwrap();
    let (is_error, body) = call_tool(
        &app,
        &token,
        "update_reef",
        json!({"reef_id": entity_id, "title": "Acme Roofing"}),
    )
    .await;
    assert!(!is_error, "update_reef should succeed: {body:?}");
    assert_eq!(body["before"]["title"], "Roof Contractor");
    assert_eq!(body["after"]["title"], "Acme Roofing");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Batch 5 (10-agent review panel): the `expected_version`
/// optimistic-concurrency guard (batch 1 fix) is Postgres-specific — its
/// `patch_node` implementation runs `SELECT ... FOR UPDATE` on `nodes` inside
/// the same transaction as the write (`store/mod.rs`) to close the TOCTOU
/// window a separate pre-check would leave open. Driven through the REAL
/// public `update_reef` MCP tool call against a REAL Postgres pool: create a reef, read its version,
/// patch it once with that version (succeeds, bumps the version), then patch
/// it again reusing the now-stale version — the second call must be rejected
/// as a conflict, proving the `FOR UPDATE` guard works end-to-end.
#[tokio::test]
async fn update_reef_rejects_stale_expected_version_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-stale-version.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("stale_version");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let node_id = store
        .upsert_node(graph_id, "entity", "Someone")
        .await
        .unwrap();
    let v1 = store.node_version(node_id).await.unwrap().unwrap();
    assert_eq!(v1, 1);

    // First patch, with the correct (current) expected_version, succeeds and
    // bumps the version.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "update_reef",
        json!({"reef_id": node_id, "title": "Someone V2", "expected_version": v1}),
    )
    .await;
    assert!(!is_error, "first update_reef should succeed: {body:?}");
    assert_eq!(body["after"]["title"], "Someone V2");
    assert_eq!(body["after"]["version"], v1 + 1);

    // Second patch reuses the now-stale `v1` expected_version, against the
    // REAL Postgres `FOR UPDATE` guard — this must be rejected as a
    // conflict, not silently clobber the first write.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "update_reef",
        json!({"reef_id": node_id, "title": "Someone V3 (stale writer)", "expected_version": v1}),
    )
    .await;
    assert!(
        is_error,
        "a stale expected_version must be rejected against real Postgres: {body:?}"
    );
    assert_eq!(error_code(&body), "invalid_params");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("modified concurrently"),
        "conflict message must surface the real version_conflict_msg wording: {body:?}"
    );

    // The stale write must NOT have taken effect: title and version are
    // unchanged from the first (successful) patch.
    let unchanged = store.get_node(node_id).await.unwrap().unwrap();
    assert_eq!(unchanged.title, "Someone V2");
    assert_eq!(
        store.node_version(node_id).await.unwrap().unwrap(),
        v1 + 1,
        "the rejected stale-version write must not have bumped the version further"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Batch 5 (10-agent review panel): a title/type edit that collides with
/// `nodes_graph_type_title_unique` must surface as a clean `invalid_params`
/// conflict, not an opaque 500 — the Postgres `23505` unique-violation catch
/// `patch_node`'s Postgres implementation added (`store/mod.rs`), exercised
/// here through the real MCP `update_reef` tool against a REAL Postgres pool:
/// create two reefs of the same type, then attempt to
/// rename one to collide with the other's title.
#[tokio::test]
async fn update_reef_rejects_title_type_collision_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-title-collision.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("title_collision");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let _n1 = store
        .upsert_node(graph_id, "entity", "Acme Corp")
        .await
        .unwrap();
    let n2 = store
        .upsert_node(graph_id, "entity", "Beta Corp")
        .await
        .unwrap();

    let (is_error, body) = call_tool(
        &app,
        &token,
        "update_reef",
        json!({"reef_id": n2, "title": "Acme Corp"}),
    )
    .await;
    assert!(
        is_error,
        "renaming into a title/type collision must be rejected against real Postgres: {body:?}"
    );
    assert_eq!(error_code(&body), "invalid_params");
    assert_eq!(
        body["error"]["message"], "a reef with that title and type already exists",
        "must surface the real 23505 unique-violation catch's message, not an opaque 500: {body:?}"
    );

    // The colliding write must NOT have taken effect.
    let unchanged = store.get_node(n2).await.unwrap().unwrap();
    assert_eq!(unchanged.title, "Beta Corp");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Batch 5 (10-agent review panel): `search_atoms`'s `since`/`until` filters
/// are bound as real `timestamptz` predicates against `atoms.timestamp`
/// inside `Store::keyword_search`'s SQL (`store/mod.rs`:
/// `($7::timestamptz IS NULL OR timestamp >= $7) AND ($8::timestamptz IS
/// NULL OR timestamp <= $8)`), so this test runs against a real Postgres
/// pool. Ingests three atoms sharing a distinctive
/// keyword at three well-separated but RECENT (hours-apart, not
/// calendar-distant) timestamps, then confirms unbounded / `since`-only /
/// `until`-only / `since`+`until` searches include and exclude the expected
/// atoms. Deliberately recent: `rerank`'s recency-decay term
/// (`retrieval/rerank.rs`, `exp(-ln(2) * days_since / half_life)` with a
/// 90-day half-life) would otherwise crush a years-old atom's score below
/// `ask_relevance_floor`'s honesty-guard cutoff before the since/until
/// predicate itself is ever exercised — `run_ask` silently returns
/// `results: []` (no error) in that case, exactly like a real "no relevant
/// results" query, which would make this test pass for the wrong reason
/// (or fail confusingly on the unbounded control case).
/// `now - back`, but never earlier than today (UTC).
///
/// Tests that ask "what happened today" resolve their window deterministically
/// against the UTC DATE, so a seeded atom has to land inside it. A plain
/// `now - 1h` does not guarantee that: for any run executing in the first hour
/// after UTC midnight it lands on YESTERDAY, the date scan correctly finds
/// nothing, and the assertion fails.
///
/// That is a ~1-in-24 flake, and it took `main` red on 2026-07-27 — the run
/// started at 23:47 UTC and reached the test at 00:21. It was mistaken for a
/// breakage in the commit under test, which is the more expensive cost: a
/// time-dependent flake spends other people's time before it is recognised as
/// one.
///
/// The offset exists to keep the atom comfortably in the past. Clamping to
/// just after midnight preserves that on every run except the ones where the
/// two goals actually conflict.
fn earlier_but_still_today(
    now: chrono::DateTime<Utc>,
    back: chrono::Duration,
) -> chrono::DateTime<Utc> {
    let today_start = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is a valid time")
        .and_utc();
    std::cmp::max(now - back, today_start + chrono::Duration::minutes(1))
}

#[test]
fn earlier_but_still_today_never_crosses_into_yesterday() {
    use chrono::TimeZone;
    let back = chrono::Duration::hours(1);

    // Mid-afternoon: the offset applies untouched.
    let afternoon = Utc.with_ymd_and_hms(2026, 7, 26, 15, 30, 0).unwrap();
    assert_eq!(earlier_but_still_today(afternoon, back), afternoon - back);

    // The failing case: 00:21 UTC. `now - 1h` would be 23:21 YESTERDAY.
    let just_after_midnight = Utc.with_ymd_and_hms(2026, 7, 27, 0, 21, 0).unwrap();
    let clamped = earlier_but_still_today(just_after_midnight, back);
    assert_eq!(
        clamped.date_naive(),
        just_after_midnight.date_naive(),
        "must stay on today's date"
    );
    assert!(clamped < just_after_midnight, "must still be in the past");

    // Exactly midnight — the most extreme case.
    let midnight = Utc.with_ymd_and_hms(2026, 7, 27, 0, 0, 0).unwrap();
    let clamped = earlier_but_still_today(midnight, back);
    assert_eq!(clamped.date_naive(), midnight.date_naive());

    // Across a month boundary, so the arithmetic is not accidentally
    // day-of-month arithmetic.
    let month_roll = Utc.with_ymd_and_hms(2026, 8, 1, 0, 5, 0).unwrap();
    assert_eq!(
        earlier_but_still_today(month_roll, back).date_naive(),
        month_roll.date_naive()
    );
}

#[tokio::test]
async fn a_date_range_recap_covers_every_coral_via_titles_not_bodies() {
    // The payoff of per-coral titles, end to end through the REAL MCP router.
    //
    // A coral that only the deterministic date-range scan surfaces — no textual
    // tie to the question at all — must still reach the answer, and must arrive
    // as its TITLE with `body_omitted: true` rather than as a body. That is what
    // lets a recap cover a whole range: bodies cost a per-atom content read and
    // their full length in the prompt, which is why the two earlier attempts to
    // bound this dropped whole days (a measured 139-atom day evicted all 82 of
    // the day before).
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-recap.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("recap_titles");
    let (user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // Two corals from today. Neither shares a word with the question below —
    // only the timestamp scan can find them.
    let titled = Uuid::new_v4();
    let untitled = Uuid::new_v4();
    let now = Utc::now();
    let seeded_at = earlier_but_still_today(now, chrono::Duration::hours(1));
    for (id, label) in [(titled, "titled"), (untitled, "untitled")] {
        store
            .insert_atoms(
                &[id],
                graph_id,
                "note",
                &[format!("recap-src-{label}")],
                &[format!("recap-blob-{label}")],
                &[],
                seeded_at,
                user_id,
                "private",
                "english",
            )
            .await
            .unwrap();
    }
    store
        .set_atoms_inline_content(
            &[titled, untitled],
            &[
                "Rewired the widget carousel and shipped it.".to_string(),
                "Repainted the bikeshed a calmer colour.".to_string(),
            ],
        )
        .await
        .unwrap();
    // Only ONE of them has been reached by the titling sweep. The other proves
    // the honest degradation: an untitled coral contributes nothing rather than
    // having a body invented for it.
    sqlx::query("UPDATE atoms SET title = $1 WHERE atom_id = $2")
        .bind("Widget carousel rewired and shipped")
        .bind(titled)
        .execute(&pool)
        .await
        .unwrap();
    mark_atoms_searchable(&pool, &[titled, untitled]).await;

    // A recap phrase Tier 1 resolves deterministically — no LLM needed for the
    // window, so this test does not depend on a classifier call.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query": "recap what happened today", "ocean_ids": [graph_id]}),
    )
    .await;
    assert!(!is_error, "search_atoms should succeed: {body:?}");
    let results = body["results"].as_array().expect("results array");

    let hit = results
        .iter()
        .find(|r| r["atom_id"] == titled.to_string())
        .unwrap_or_else(|| {
            panic!(
                "the titled coral must reach the answer via the date scan even with \
                 zero textual overlap with the question: {body:?}"
            )
        });
    assert_eq!(
        hit["title"], "Widget carousel rewired and shipped",
        "the stored headline must be exposed: {hit:?}"
    );
    assert_eq!(
        hit["body_omitted"], true,
        "a window-only coral must be flagged as headline-not-body, or a consumer \
         will read a ~10-word title as a short document: {hit:?}"
    );
    assert_eq!(
        hit["snippet"], "Widget carousel rewired and shipped",
        "its snippet IS the headline — no body was read for it: {hit:?}"
    );
    assert_eq!(
        hit["source_type"], "note",
        "a title-only coral must still report its REAL source type, not a default"
    );
    assert!(
        hit["timestamp"].as_str().is_some(),
        "date must be present: {hit:?}"
    );

    // The untitled coral degrades honestly: absent, or present with no title —
    // never present with a fabricated body.
    if let Some(u) = results.iter().find(|r| r["atom_id"] == untitled.to_string()) {
        assert!(
            u["title"].is_null() || u["body_omitted"] == false,
            "an untitled coral must not claim a headline it does not have: {u:?}"
        );
    }
}

#[tokio::test]
async fn search_atoms_since_until_bounds_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-since-until.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("since_until");
    let (user, account, graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let now = Utc::now();
    let quotes = [
        "zzqtimebound99 old coral",
        "zzqtimebound99 mid coral",
        "zzqtimebound99 new coral",
    ];
    let mut captures = Vec::new();
    for (index, quote) in quotes.into_iter().enumerate() {
        let (error,body)=call_tool(&app,&token,"add_context",json!({"ocean_id":graph,"content":quote,"timestamp":(now-chrono::Duration::hours(3-index as i64)).to_rfc3339()})).await;
        assert!(!error, "{body}");
        captures.push(body);
    }
    let evidence = publish_retained(
        &app,
        &pool,
        &token,
        graph,
        &[
            (&captures[0], quotes[0], None),
            (&captures[1], quotes[1], None),
            (&captures[2], quotes[2], None),
        ],
    )
    .await;
    let (error, all) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query":"zzqtimebound99","ocean_ids":[graph]}),
    )
    .await;
    assert!(!error, "{all}");
    assert_eq!(all["retained_citations"].as_array().unwrap().len(), 3);
    for source in &evidence {
        source.assert_cited(&all);
    }
    let boundary = (now - chrono::Duration::hours(2)).to_rfc3339();
    for filter in [
        json!({"since":boundary}),
        json!({"until":boundary}),
        json!({"since":boundary,"until":boundary}),
    ] {
        let mut args = json!({"query":"zzqtimebound99","ocean_ids":[graph]});
        args.as_object_mut()
            .unwrap()
            .extend(filter.as_object().unwrap().clone());
        let (error, body) = call_tool(&app, &token, "search_atoms", args).await;
        assert!(error, "{body}");
        assert_eq!(error_code(&body), "invalid_params");
    }
    let (error,one)=call_tool(&app,&token,"search_atoms",json!({"query":"zzqtimebound99","library_scope":{"graph_ids":[graph],"source_version_id":evidence[1].version,"source_item_id":evidence[1].item}})).await;
    assert!(!error, "{one}");
    evidence[1].assert_cited(&one);
    assert_eq!(one["retained_citations"].as_array().unwrap().len(), 1);
    cleanup(&pool, &[account], &[user]).await;
}
#[tokio::test]
async fn list_meetings_and_get_transcript_owner_success_against_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-meetings.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("meetings");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let transcript_text = "Alice: let's ship the MCP server. Bob: agreed, targeting next sprint.";
    store
        .upsert_meeting_transcript(
            graph_id,
            "projection-only",
            None,
            Some("Unbound legacy projection."),
            None,
            None,
        )
        .await
        .unwrap();
    let (error, projection) = call_tool(
        &app,
        &token,
        "get_transcript",
        json!({"ocean_id":graph_id,"meeting_id":"projection-only","variant":"raw"}),
    )
    .await;
    assert!(error, "{projection}");
    assert_eq!(error_code(&projection), "not_found");
    let dek = coralswarm_backend::crypto::resolve_dek(&pool, account_id)
        .await
        .unwrap()
        .unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id)
        .await
        .unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_id)
        .await
        .unwrap();
    let captured = coralswarm_backend::meeting_evidence::capture_version_tx(
        &mut tx,
        &coralswarm_backend::test_app_state(),
        graph_id,
        account_id,
        "standup-2026-07-14",
        transcript_text,
        Some(120_000),
        None,
        None,
        &dek,
    )
    .await
    .unwrap();
    assert!(captured.meeting_projection);
    tx.commit().await.unwrap();

    let (is_error, body) =
        call_tool(&app, &token, "list_meetings", json!({"ocean_id": graph_id})).await;
    assert!(
        !is_error,
        "list_meetings should succeed for the owner: {body:?}"
    );
    let meetings = body["meetings"].as_array().expect("meetings array");
    assert!(
        meetings
            .iter()
            .any(|m| m["meeting_id"] == "standup-2026-07-14" && m["duration_ms"] == 120_000),
        "list_meetings must surface the seeded meeting: {body:?}"
    );

    let (is_error, body) = call_tool(
        &app,
        &token,
        "get_transcript",
        json!({"ocean_id": graph_id, "meeting_id": "standup-2026-07-14"}),
    )
    .await;
    assert!(
        !is_error,
        "get_transcript should succeed for the owner: {body:?}"
    );
    assert_eq!(body["chunk_index"], 0);
    assert_eq!(body["total_chars"], transcript_text.chars().count());
    assert!(
        body["text"]
            .as_str()
            .unwrap()
            .contains("ship the MCP server"),
        "get_transcript text should contain the seeded transcript: {body:?}"
    );

    assert_eq!(body["text"], transcript_text);
    let (error, raw) = call_tool(
        &app,
        &token,
        "get_transcript",
        json!({"ocean_id":graph_id,"meeting_id":"standup-2026-07-14","variant":"raw"}),
    )
    .await;
    assert!(!error, "{raw}");
    assert_eq!(raw["text"], transcript_text);
    let (error, corrected) = call_tool(
        &app,
        &token,
        "get_transcript",
        json!({"ocean_id":graph_id,"meeting_id":"standup-2026-07-14","variant":"corrected"}),
    )
    .await;
    assert!(error, "{corrected}");
    assert_eq!(error_code(&corrected), "unsupported_evidence_variant");
    cleanup(&pool, &[account_id], &[user_id]).await;
}

#[tokio::test]
async fn ask_ocean_without_llm_is_deterministic_sources_mode() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-ask-sources.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("ask_sources");
    let (user, account, graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let quote = "The roof replacement vendor is zzqroofing88 Inc.";
    let (error, capture) = call_tool(
        &app,
        &token,
        "add_context",
        json!({"ocean_id":graph,"content":quote}),
    )
    .await;
    assert!(!error, "{capture}");
    let evidence = publish_retained(&app, &pool, &token, graph, &[(&capture, quote, None)]).await;
    let (error, body) = ask_ocean_and_wait(
        &app,
        &token,
        json!({"question":"zzqroofing88","ocean_ids":[graph],"style":"sources-only"}),
    )
    .await;
    assert!(!error, "{body}");
    assert_eq!(body["mode"], "retrieval_only");
    assert!(body["answer"].is_null());
    assert_eq!(body["text"], "");
    assert_eq!(body["timing"]["embed_ms"], 0);
    assert_eq!(body["timing"]["synthesize_ms"], 0);
    evidence[0].assert_cited(&body);
    let (error, body) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query":"zzqroofing88","ocean_ids":[graph]}),
    )
    .await;
    assert!(!error, "{body}");
    evidence[0].assert_cited(&body);
    let (error, absent) = ask_ocean_and_wait(
        &app,
        &token,
        json!({"question":"absentzzfixture","ocean_ids":[graph]}),
    )
    .await;
    assert!(!error, "{absent}");
    assert_eq!(absent["answer_status"], "no_evidence");
    assert!(absent["retained_citations"].as_array().unwrap().is_empty());
    cleanup(&pool, &[account], &[user]).await;
}

#[tokio::test]
async fn ask_ocean_what_happened_today_includes_topically_unrelated_same_day_atom_against_real_postgres(
) {
    use chrono::TimeZone;
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-event-time-scope.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("event_time_scope");
    let (user, account, graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    PostgresStore::new(pool.clone())
        .set_user_timezone(user, "America/Chicago")
        .await
        .unwrap();
    let tz: chrono_tz::Tz = "America/Chicago".parse().unwrap();
    let now = Utc::now().with_timezone(&tz);
    let midnight = tz
        .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
        .single()
        .unwrap()
        .with_timezone(&Utc);
    let quotes = [
        "The espresso machine in the break room finally got fixed.",
        "Standup notebook asked what happened today eight days ago.",
        "The printer tray was repaired.",
        "The bicycle chain was replaced.",
    ];
    let times = [
        midnight,
        midnight - chrono::Duration::days(8),
        midnight - chrono::Duration::microseconds(1),
        midnight + chrono::Duration::hours(1),
    ];
    let mut captures = Vec::new();
    for (quote, time) in quotes.into_iter().zip(times) {
        let (error, body) = call_tool(
            &app,
            &token,
            "add_context",
            json!({"ocean_id":graph,"content":quote,"timestamp":time.to_rfc3339()}),
        )
        .await;
        assert!(!error, "{body}");
        captures.push(body);
    }
    let evidence = publish_retained(
        &app,
        &pool,
        &token,
        graph,
        &[
            (&captures[0], quotes[0], None),
            (&captures[1], quotes[1], None),
            (&captures[2], quotes[2], None),
            (&captures[3], quotes[3], None),
        ],
    )
    .await;
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account)
        .await
        .unwrap();
    sqlx::query("UPDATE source_items SET access_state='restricted' WHERE source_item_id=$1")
        .bind(evidence[3].item)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let dek = coralswarm_backend::crypto::resolve_dek(&pool, account)
        .await
        .unwrap()
        .unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account)
        .await
        .unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account)
        .await
        .unwrap();
    let quote = "What happened today has an unknown event clock.";
    let metadata = json!({});
    let unknown = coralswarm_backend::source_evidence::capture_source_version_tx(
        &mut tx,
        None,
        graph,
        account,
        &coralswarm_backend::source_evidence::CaptureSource {
            source_type: "note",
            provider_namespace: "fixture:mcp:unknown-event",
            source_id: "unknown-clock",
            normalized_text: quote,
            representation: "normalized_text",
            metadata: &metadata,
            occurred_at: None,
            participants: &[],
            ingested_by: Some(user),
            scope: "private",
            duration_ms: None,
            segments: None,
        },
        None,
        &dek,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let unknown = publish_versions(
        &app,
        &pool,
        &token,
        graph,
        vec![PendingEvidence {item:unknown.source_item_id,version:unknown.version_id,quote,project:None}],
    )
    .await;
    let (error, body) = ask_ocean_and_wait(
        &app,
        &token,
        json!({"question":"what happened today?","ocean_ids":[graph],"style":"sources-only"}),
    )
    .await;
    assert!(!error, "{body}");
    assert_eq!(body["mode"], "retrieval_only");
    assert_eq!(body["retrieval_reason"], "permitted_event_time_evidence");
    assert_eq!(body["timing"]["embed_ms"], 0);
    assert_eq!(body["timing"]["synthesize_ms"], 0);
    evidence[0].assert_cited(&body);
    let citations = body["retained_citations"].as_array().unwrap();
    assert_eq!(
        citations.len(),
        1,
        "old lexical decoy, prior local day, denied and null clocks are excluded: {body}"
    );
    assert!(citations
        .iter()
        .all(|c| c["source_version_id"] != unknown[0].version.to_string()));
    let (error,scoped)=ask_ocean_and_wait(&app,&token,json!({"question":"what happened today?","style":"sources-only","library_scope":{"graph_ids":[graph],"source_item_id":evidence[1].item}})).await;
    assert!(!error, "{scoped}");
    assert!(scoped["retained_citations"].as_array().unwrap().is_empty());
    assert_eq!(scoped["answer_status"], "no_evidence");
    cleanup(&pool, &[account], &[user]).await;
}
/// `PgRateLimiter` 11th-call rejection on the `mcp:ask:{user_id}` bucket
/// (§4.3 tiering: `ask_ocean` is capped at 10/min, tighter than the generic
/// 60/min read bucket) — against the REAL Postgres-backed limiter, not the
/// in-memory one every other test in this file (and Layer A's `server.rs`
/// tests) use.
///
/// `rate_limit` is a crate-private module (`backend/src/lib.rs`'s `mod
/// rate_limit;`) — `PgRateLimiter` cannot be named or constructed from an
/// external test binary (confirmed: `use coralswarm_backend::rate_limit::
/// PgRateLimiter` fails to compile with E0603 "module `rate_limit` is
/// private"). The only way to exercise the REAL `PgRateLimiter` from outside
/// the crate is to run the actual server process, which selects it
/// internally when `RATE_LIMIT_BACKEND=pg` (`lib.rs`'s `run()`) — so this
/// test boots `coralswarm_backend::run()` in a background task against a
/// dedicated port and drives it over real HTTP with `reqwest`, the same way
/// `scripts/mcp-smoke.sh` will against CI's `mcp-e2e` job.
#[tokio::test]
async fn pg_rate_limiter_rejects_the_11th_ask_ocean_call_against_real_postgres() {
    let database_url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live MCP e2e suite");
            return;
        }
    };

    let _env = McpEnv::enable("https://mcp-pg-e2e-ratelimit-real-server.example/mcp").await;
    // `McpEnv` already set `CLERK_ISSUER`/`CLERK_JWKS_URL`/`MCP_RESOURCE_URL`/
    // `MASTER_KEK_HEX`; `run()` additionally needs `DATABASE_URL`, a
    // dedicated `PORT`/`INTERNAL_PORT` pair (avoid colliding with a real dev
    // server on 8080/8081), and `RATE_LIMIT_BACKEND=pg` (the actual point of
    // this test — `McpEnv` sets this too, but `run()` is what actually
    // CONSULTS it to select `PgRateLimiter`, unlike `app_for_pool`'s tests).
    let port: u16 = 18080;
    let internal_port: u16 = 18081;
    unsafe {
        std::env::set_var("DATABASE_URL", &database_url);
        std::env::set_var("PORT", port.to_string());
        std::env::set_var("INTERNAL_PORT", internal_port.to_string());
    }

    let server = tokio::spawn(coralswarm_backend::run());
    let base = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::new();

    // Wait for /readyz (DB pool connected + migrations applied) — bounded
    // retry, not a fixed sleep.
    let mut ready = false;
    for _ in 0..50 {
        if let Ok(resp) = http.get(format!("{base}/readyz")).send().await {
            if resp.status().is_success() {
                ready = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(ready, "real server did not become ready in time");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .expect("connect to test Postgres for seeding/cleanup");

    let sub = fresh_sub("ratelimit_real_server");
    let token = write_token(&sub);
    let (is_error, body) = call_tool_http(&http, &base, &token, "list_oceans", json!({})).await;
    assert!(!is_error, "provisioning list_oceans call failed: {body:?}");
    let user_id: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(&sub)
        .fetch_one(&pool)
        .await
        .expect("provisioned user row");
    let account_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE owner_user_id = $1 AND clerk_org_id IS NULL ORDER BY created_at LIMIT 1",
    )
    .bind(user_id)
    .fetch_one(&pool)
    .await
    .expect("provisioned personal account");
    let graph_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM graphs WHERE account_id = $1 AND kind = 'personal' AND is_primary LIMIT 1",
    )
    .bind(account_id)
    .fetch_one(&pool)
    .await
    .expect("provisioned Primary personal ocean");

    // ── Window-boundary guard (pre-existing flake, unrelated to shadow-floor
    // work) ──────────────────────────────────────────────────────────────────
    // `PgRateLimiter`/`InMemoryRateLimiter` (`src/rate_limit.rs`) use a FIXED
    // 60s window: `window_start = now - (now % 60)`. This test fires 11 rapid
    // HTTP round trips and assumes they all land in the SAME window. If they
    // instead straddle a window boundary (e.g. call 10 lands at :59.99 and
    // call 11 at :00.01), the count resets for the 11th call and it is
    // ALLOWED through instead of rejected — the test then falls into the
    // tool body and observes a normal (non-rate-limited) `mode: "no_answer"`
    // response (this fixture seeds no coral at all, so every allowed call
    // abstains) instead of `rate_limited`. That is exactly the failure mode
    // seen in CI. Guard against it deterministically, without touching any
    // production rate-limiting code: if we're within
    // `WINDOW_SAFETY_MARGIN_SECS` of the next window boundary, wait for the
    // boundary to pass first, so the entire 11-call burst (a few ms locally,
    // generously margined here) is guaranteed to land inside a single fixed
    // window.
    const RATE_LIMIT_WINDOW_SECS: i64 = 60;
    const WINDOW_SAFETY_MARGIN_SECS: i64 = 5;
    let secs_into_window = Utc::now().timestamp().rem_euclid(RATE_LIMIT_WINDOW_SECS);
    let secs_left = RATE_LIMIT_WINDOW_SECS - secs_into_window;
    if secs_left < WINDOW_SAFETY_MARGIN_SECS {
        tokio::time::sleep(std::time::Duration::from_secs(secs_left as u64)).await;
    }

    // `ocean_ids: [graph_id]` (a real, reachable ocean) so the dispatcher's
    // rate-limit gate — which runs BEFORE the tool body — is the intended
    // trigger for `rate_limited` here. It is no longer the ONLY thing that
    // can produce that code, though (follow-up review, D6): `ask_ocean`'s
    // tool body now also rejects with `rate_limited` once a caller's
    // `AskInFlight` in-flight cap (default 3, `ask_max_in_flight`) is
    // reached (issue #593 item 2) — CONFIRMED live in CI 2026-09-04 ("call 7
    // (of 10 allowed) should not be rate-limited yet: ... too many ask_ocean
    // tasks already in flight for this user (3 max) ..."). The doc this
    // comment used to carry argued the in-flight path could never race in
    // here because this fixture seeds no coral, so every admitted call
    // settles in roughly a millisecond — that argument assumed the
    // completing task is always scheduled promptly, which does not hold on a
    // busy/CPU-starved CI runner: `ask_ocean` returns immediately and
    // completes the actual work (and releases its `AskInFlight` permit) on a
    // SEPARATE spawned tokio task (`tracker.spawn` in
    // `mcp::tools::read::ask_ocean`), so firing 10+ calls back to back with
    // no synchronization can legitimately leave more than 3 of them not yet
    // released when the next call lands, tripping the in-flight cap instead
    // of the per-minute bucket this test exists to exercise. Fix: DRAIN each
    // call's task (poll `get_task_result` to a terminal state,
    // `drain_ask_task_http`) before issuing the next one, so at most 1 ask is
    // ever actually in flight for this user at a time — comfortably under
    // the in-flight cap regardless of its configured value — and the ONLY
    // thing that can trip `rate_limited` across these 11 calls is the
    // `mcp:ask:{user_id}` 10/min bucket itself.
    for i in 0..10 {
        let (is_error, body) = call_tool_http(
            &http,
            &base,
            &token,
            "ask_ocean",
            json!({"question": "anything", "ocean_ids": [graph_id]}),
        )
        .await;
        assert_ne!(
            error_code(&body),
            "rate_limited",
            "call {i} (of 10 allowed) should not be rate-limited yet: {body:?}"
        );
        assert!(!is_error, "call {i}: ask_ocean itself must not tool-error before task creation: {body:?}");
        assert_eq!(
            body["status"], "working",
            "call {i}: a freshly-created task must start working: {body:?}"
        );
        let task_id = body["task_id"]
            .as_str()
            .unwrap_or_else(|| panic!("call {i}: ask_ocean did not return a task_id: {body:?}"));
        // Drain BEFORE the next call so the in-flight cap can never be the
        // thing that rejects call i+1 — see this loop's own comment above.
        drain_ask_task_http(&http, &base, &token, task_id).await;
    }

    let (_is_error, body) = call_tool_http(
        &http,
        &base,
        &token,
        "ask_ocean",
        json!({"question": "anything", "ocean_ids": [graph_id]}),
    )
    .await;
    assert_eq!(
        error_code(&body),
        "rate_limited",
        "the 11th ask_ocean call should be rejected: {body:?}"
    );
    // Every prior call was drained before the next was issued (at most 1 ever
    // in flight, well under the 3-max cap), so this rejection can only be the
    // dispatcher-level `mcp:ask:{user_id}` 10/min bucket
    // (`mcp::server::check_rate_limits`), never `ask_ocean`'s tool-body
    // in-flight-cap rejection — the two share the same `rate_limited` code
    // but distinct message text (`mcp::tools::read::ask_ocean`'s "too many
    // ask_ocean tasks already in flight..." vs `check_rate_limits`'s generic
    // "rate limit exceeded for this tool; retry in about a minute"); pin the
    // distinction so a regression that made the in-flight cap fire here
    // instead is caught rather than silently passing on the shared code.
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        !message.contains("already in flight"),
        "the 11th call was rejected by the in-flight cap, not the per-minute limiter this test \
         exercises: {body:?}"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
    server.abort();
    unsafe {
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("PORT");
        std::env::remove_var("INTERNAL_PORT");
    }
}

/// Real-LLM `ask_ocean` smoke — gated on `OPENROUTER_API_KEY` (pattern:
/// `relationship_llm_e2e.rs`); skips (does not fail) when unset, so a plain
/// `cargo test` (even with `RELINFER_TEST_DATABASE_URL` set) never makes a
/// paid API call.
///
/// Run with:
///   RELINFER_TEST_DATABASE_URL=... OPENROUTER_API_KEY=$(grep ^OPENROUTER_API_KEY= ../../.env | cut -d= -f2-) \
///     cargo test -p coralswarm-backend --test mcp_pg_e2e ask_ocean_synthesizes_with_real_llm -- --nocapture --ignored
#[tokio::test]
#[ignore = "makes a real, paid OpenRouter API call; gated on OPENROUTER_API_KEY, run explicitly with --ignored"]
async fn ask_ocean_synthesizes_with_real_llm_against_real_postgres() {
    let api_key = match std::env::var("OPENROUTER_API_KEY") {
        Ok(k) if !k.trim().is_empty() => k,
        _ => {
            eprintln!("skipping: set OPENROUTER_API_KEY to run the real-LLM ask_ocean e2e");
            return;
        }
    };
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-ask-synth.example/mcp").await;

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>);
    state.llm = Some(Arc::new(
        coralswarm_backend::llm::openai::OpenAiProvider::openrouter(reqwest::Client::new(), api_key),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let sub = fresh_sub("ask_synth");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({"ocean_id": graph_id, "content": "Harper is the project lead for the MCP server rollout."}),
    )
    .await;
    assert!(!is_error, "add_context should succeed: {body:?}");
    let ids = atom_ids_of(&body);
    mark_atoms_searchable(&pool, &ids).await;

    // Avoid stopwords ("who"/"the"/"is") in the question: `to_tsvector('english', ...)`
    // strips them when building `content_tsv`, but `websearch_to_tsquery('simple', ...)`
    // (the query side, `Store::keyword_search`) does NOT strip them from the
    // query — an AND'd term absent from `content_tsv` fails the whole match
    // and the honesty guard fires (`no_answer`) before synthesis ever runs.
    let (is_error, body) = ask_ocean_and_wait(
        &app,
        &token,
        json!({"question": "project lead mcp server rollout", "ocean_ids": [graph_id]}),
    )
    .await;
    assert!(!is_error, "ask_ocean should succeed: {body:?}");
    assert_eq!(
        body["mode"], "synthesized",
        "real LLM configured -> synthesized mode: {body:?}"
    );
    let answer = body["answer"].as_str().expect("synthesized answer");
    println!("--- ask_ocean synthesized answer ---\n{answer}\n");
    assert!(
        answer.to_lowercase().contains("harper"),
        "answer should name Harper: {answer}"
    );

    // Additive score breakdown (dogfooded ranking-transparency gap, 2026-07):
    // this is the one genuinely `AskMode::Synthesized` path in this file, so
    // it's the correct place to verify `citation_details` actually carries
    // `score_parts` on THAT mode (the no-LLM sources-mode test above can
    // never exercise this — `citation_details` is hard-coded empty there).
    let citations = body["citations"].as_array().expect("citations array");
    assert!(!citations.is_empty(), "synthesized answer should cite the seeded coral: {body:?}");
    let cite_details = body["citation_details"]
        .as_array()
        .expect("citation_details array");
    assert!(
        !cite_details.is_empty(),
        "synthesized mode citation_details should be populated: {body:?}"
    );
    for detail in cite_details {
        assert!(
            detail["score"].as_f64().is_some(),
            "citation_details score must be present: {detail:?}"
        );
        let parts = &detail["score_parts"];
        assert!(
            parts["vector"].as_f64().is_some()
                && parts["keyword"].as_f64().is_some()
                && parts["recency"].as_f64().is_some(),
            "citation_details score_parts must carry vector/keyword/recency: {detail:?}"
        );
    }

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// rmcp's `StreamableHttpService` DNS-rebinding guard must reject a foreign
/// `Host` header even for an otherwise-valid, authenticated request — this is
/// the exact gate that 403'd the real `*.fly.dev` host in production before the
/// allowlist was derived from `MCP_RESOURCE_URL`. The allowed-host path is
/// already covered implicitly by every other test here (all POST `Host:
/// localhost`); this asserts both sides explicitly.
#[tokio::test]
async fn dns_rebinding_guard_rejects_a_foreign_host_header() {
    let pool = test_pool!();
    // Allowed hosts for this env: loopback + the resource authority
    // (`mcp-pg-e2e-host.example`). `evil.example` is neither.
    let _env = McpEnv::enable("https://mcp-pg-e2e-host.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("host_guard");
    let (user_id, account_id, _graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let rpc = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}});

    let build = |host: &str| {
        Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", host)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::from(rpc.to_string()))
            .unwrap()
    };

    // The deployment authority itself (the exact Host that 403'd in prod) must
    // be allowed AND drive a fully-successful tools/list — assert 200, not just
    // "not 403" (which would also pass for a 401/404/500).
    let ok_status = app
        .clone()
        .oneshot(build("mcp-pg-e2e-host.example"))
        .await
        .unwrap()
        .status();
    assert_eq!(
        ok_status,
        StatusCode::OK,
        "the deployment authority Host must be allowed and succeed"
    );

    // Foreign host → 403 from rmcp's guard, even with a valid bearer token.
    let bad_status = app.clone().oneshot(build("evil.example")).await.unwrap().status();
    assert_eq!(
        bad_status,
        StatusCode::FORBIDDEN,
        "a foreign Host must be rejected by the DNS-rebinding guard"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

// ── Coding-session capture Phase 3: MCP read surface over real HTTP ────────
//
// `add_context` (Phase 1) already lands `coding_sessions` rows and `cc:
// {session_id}:` corals end-to-end (see the coding-session tests above);
// these tests drive the Phase 3 READ tools (`list_sessions`, and the
// `project`/`session_id` filters on `search_atoms`/`ask_ocean`) through the
// SAME real HTTP `/mcp` transport, proving the whole write -> read round
// trip, not just each half in isolation.

/// `list_sessions`: most-recent-activity-first ordering, `coral_count`,
/// `project` filter (incl. an empty-project — zero matches — case), and
/// keyset `cursor` pagination, all over real HTTP.
#[tokio::test]
async fn list_sessions_returns_ordered_sessions_with_project_filter_and_pagination() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-list-sessions.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("list_sessions");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // sess-x (project x, 2 checkpoints), then sess-y (project y, 1
    // checkpoint) — sess-y is therefore the most-recently-active.
    for i in 0..2 {
        let (is_error, body) = call_tool(
            &app,
            &token,
            "add_context",
            json!({
                "ocean_id": graph_id,
                "content": format!("checkpoint {i} of session x"),
                "session_id": "sess-x",
                "remote": "https://github.com/coralswarm/project-x.git",
            }),
        )
        .await;
        assert!(!is_error, "add_context (sess-x #{i}) should succeed: {body:?}");
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "checkpoint 0 of session y",
            "session_id": "sess-y",
            "remote": "https://github.com/coralswarm/project-y.git",
        }),
    )
    .await;
    assert!(!is_error, "add_context (sess-y) should succeed: {body:?}");

    // ── zero-session case: a FRESH ocean has none ───────────────────────────
    let sub2 = fresh_sub("list_sessions_empty");
    let (user_id2, account_id2, graph_id2) = provision(&app, &pool, &sub2).await;
    let token2 = write_token(&sub2);
    let (is_error, body) = call_tool(&app, &token2, "list_sessions", json!({"ocean_id": graph_id2})).await;
    assert!(!is_error, "list_sessions on a fresh ocean should succeed: {body:?}");
    assert_eq!(body["sessions"].as_array().unwrap().len(), 0, "zero-session case: {body:?}");
    assert_eq!(body["has_more"], json!(false));
    assert!(body["next_cursor"].is_null());

    // ── unfiltered: both sessions, most-recent-activity first, coral_count
    //    computed correctly ─────────────────────────────────────────────────
    let (is_error, body) = call_tool(&app, &token, "list_sessions", json!({"ocean_id": graph_id})).await;
    assert!(!is_error, "list_sessions should succeed: {body:?}");
    let sessions = body["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0]["session_id"], "sess-y", "most-recently-active session must come first");
    assert_eq!(sessions[1]["session_id"], "sess-x");
    assert_eq!(sessions[1]["coral_count"], json!(2), "sess-x has 2 checkpoints");
    assert_eq!(sessions[0]["coral_count"], json!(1), "sess-y has 1 checkpoint");
    assert!(sessions[1]["branch"].is_null(), "no branch was sent, so the last-reported row value stays null");
    assert_eq!(body["has_more"], json!(false));

    // ── project filter: proj-x's key was derived from the sanitized remote ─
    let project_key = sessions[1]["project_key"].as_str().expect("project_key present").to_string();
    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "project": project_key}),
    )
    .await;
    assert!(!is_error, "list_sessions with project filter should succeed: {body:?}");
    let filtered = body["sessions"].as_array().unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0]["session_id"], "sess-x");

    // ── empty-project case: a project with zero matching sessions ──────────
    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "project": "no-such-project-exists"}),
    )
    .await;
    assert!(!is_error, "list_sessions with a non-matching project should succeed empty: {body:?}");
    assert_eq!(body["sessions"].as_array().unwrap().len(), 0);

    // ── cursor pagination: limit=1 across the 2 sessions ────────────────────
    let (is_error, page1) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "limit": 1}),
    )
    .await;
    assert!(!is_error, "list_sessions page 1 should succeed: {page1:?}");
    assert_eq!(page1["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(page1["sessions"][0]["session_id"], "sess-y");
    assert_eq!(page1["has_more"], json!(true));
    let cursor = page1["next_cursor"].as_str().expect("next_cursor present on page 1").to_string();

    let (is_error, page2) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "limit": 1, "cursor": cursor}),
    )
    .await;
    assert!(!is_error, "list_sessions page 2 should succeed: {page2:?}");
    assert_eq!(page2["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(page2["sessions"][0]["session_id"], "sess-x");
    assert_eq!(page2["has_more"], json!(false), "second page must be the last");
    assert!(page2["next_cursor"].is_null());

    cleanup(&pool, &[account_id, account_id2], &[user_id, user_id2]).await;
}

/// `list_sessions` rejects a malformed `cursor` over real HTTP, through the
/// real dispatcher/Postgres path.
#[tokio::test]
async fn list_sessions_rejects_a_malformed_cursor_over_real_http() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-list-sessions-cursor.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("list_sessions_bad_cursor");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "cursor": "not-a-real-cursor"}),
    )
    .await;
    assert!(is_error, "a malformed cursor must be rejected: {body:?}");
    assert_eq!(error_code(&body), "invalid_params");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// `list_sessions` labels every row with its kind, and filters on it in SQL.
///
/// `coding_sessions` has held every kind in one table since migration
/// `0185_coding_sessions_session_kind`, so this tool has ALWAYS returned
/// meeting and agent rows alongside coding ones — it simply had no way to say which
/// was which. The moment a caller writes a `meeting` row, the tool's own
/// RESUMING A PROJECT recipe can land on one, and before this change the
/// caller could not tell: the serialized row carried session_id / title /
/// project_key / repo_path / branch / harness_version / model / started_at /
/// last_activity_at / coral_count, and no kind at all.
///
/// Two halves, and both matter:
///
///   1. **Every row carries `session_kind`**, whether or not the caller
///      narrowed — the discriminator a resuming agent needs. Deleting the
///      field from the serializer turns the first assertion below into a
///      `Value::Null` comparison and fails it.
///   2. **The optional `session_kind` input narrows in SQL, before LIMIT.**
///      The one coding session here is the OLDEST, so the unfiltered first
///      page of size 2 holds none of it (asserted as a precondition, so this
///      test cannot pass vacuously). A caller that filtered a fetched page in
///      memory would therefore report "no coding sessions" while one sat in
///      the ocean — `absence-renders-as-success`, the same failure the
///      per-kind facets exist to close.
#[tokio::test]
async fn list_sessions_labels_every_row_with_its_kind_and_filters_on_it() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-list-sessions-kind.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("list_sessions_kind");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // Oldest first, so `last_activity_at DESC` puts the two meeting sessions at
    // the head of the unfiltered list and the coding session behind them.
    // `sess-code-1` sends no `session_kind` at all — the shape every shipped
    // caller posts today, which must still land `coding`.
    for (session_id, kind) in [
        ("sess-code-1", None),
        ("sess-meet-1", Some("meeting")),
        ("sess-meet-2", Some("meeting")),
    ] {
        let mut payload = json!({
            "ocean_id": graph_id,
            "content": format!("checkpoint of {session_id}"),
            "session_id": session_id,
        });
        if let Some(k) = kind {
            payload["session_kind"] = json!(k);
        }
        let (is_error, body) = call_tool(&app, &token, "add_context", payload).await;
        assert!(!is_error, "add_context ({session_id}) should succeed: {body:?}");
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }

    // ── 1. every row is labelled, unfiltered ────────────────────────────────
    let (is_error, body) = call_tool(&app, &token, "list_sessions", json!({"ocean_id": graph_id})).await;
    assert!(!is_error, "list_sessions should succeed: {body:?}");
    let sessions = body["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 3, "every kind is listed when no filter is passed: {sessions:?}");
    let kind_of = |session_id: &str| -> String {
        sessions
            .iter()
            .find(|s| s["session_id"] == session_id)
            .unwrap_or_else(|| panic!("{session_id} must be listed: {sessions:?}"))["session_kind"]
            .as_str()
            .unwrap_or_else(|| {
                panic!("{session_id} must carry a session_kind STRING, got: {sessions:?}")
            })
            .to_string()
    };
    assert_eq!(kind_of("sess-code-1"), "coding");
    assert_eq!(kind_of("sess-meet-1"), "meeting");
    assert_eq!(kind_of("sess-meet-2"), "meeting");

    // ── 2. the filter is applied before LIMIT ───────────────────────────────
    //
    // Precondition: unfiltered, page size 2 — no coding session in sight.
    // This is what a browser-side filter would have had to work from.
    let (is_error, body) =
        call_tool(&app, &token, "list_sessions", json!({"ocean_id": graph_id, "limit": 2})).await;
    assert!(!is_error, "list_sessions (limit 2) should succeed: {body:?}");
    let first_page = body["sessions"].as_array().unwrap();
    assert_eq!(first_page.len(), 2);
    assert!(
        first_page.iter().all(|s| s["session_kind"] == "meeting"),
        "precondition: the unfiltered first page must hold no coding session, \
         else this test proves nothing: {first_page:?}"
    );

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "coding", "limit": 2}),
    )
    .await;
    assert!(!is_error, "list_sessions (session_kind=coding) should succeed: {body:?}");
    let coding = body["sessions"].as_array().unwrap();
    assert_eq!(
        coding.len(),
        1,
        "the kind filter must be applied in SQL, before LIMIT — a coding session \
         past the first unfiltered page must still come back: {coding:?}"
    );
    assert_eq!(coding[0]["session_id"], "sess-code-1");
    assert_eq!(coding[0]["session_kind"], "coding");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "meeting"}),
    )
    .await;
    assert!(!is_error, "list_sessions (session_kind=meeting) should succeed: {body:?}");
    let meetings = body["sessions"].as_array().unwrap();
    assert_eq!(meetings.len(), 2, "both meeting sessions, and nothing else: {meetings:?}");
    assert!(meetings.iter().all(|s| s["session_kind"] == "meeting"));

    // A kind nobody has written yet is a normal empty list, not an error.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "agent"}),
    )
    .await;
    assert!(!is_error, "an unmatched kind is an empty list, not an error: {body:?}");
    assert_eq!(body["sessions"].as_array().unwrap().len(), 0);

    // ── 3. an unknown kind is rejected, never silently ignored ──────────────
    //
    // `coding_session` is the SOURCE TYPE, not the wire kind — the exact typo a
    // caller reading `atoms.source_type` would make. Ignoring it would hand
    // back every kind under a heading that names one.
    for bad in ["work", "Coding", "coding_session", ""] {
        let (is_error, body) = call_tool(
            &app,
            &token,
            "list_sessions",
            json!({"ocean_id": graph_id, "session_kind": bad}),
        )
        .await;
        assert!(is_error, "session_kind={bad:?} must be rejected: {body:?}");
        assert_eq!(error_code(&body), "invalid_params", "session_kind={bad:?}");
    }

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Three coding sessions that differ on project / harness / model / repo /
/// branch: each `list_sessions` filter returns the matching one, `repo`
/// matches either column, and `kind=coding` + empty `platform` + `project`
/// still works over real HTTP.
#[tokio::test]
async fn list_sessions_filters_coding_sessions_by_harness_model_repo_and_branch() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-coding-filters.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("coding_filters");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "alpha checkpoint",
            "session_id": "sess-alpha",
            "repo_path": "/src/alpha",
            "remote": "https://github.com/org/alpha.git",
            "harness_version": "claude-code/1",
            "model": "opus",
            "branch": "main",
        }),
    )
    .await;
    assert!(!is_error, "alpha: {body:?}");
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "beta checkpoint",
            "session_id": "sess-beta",
            "repo_path": "/src/beta",
            "remote": "https://github.com/org/beta.git",
            "harness_version": "cursor/2",
            "model": "sonnet",
            "branch": "feat/b",
        }),
    )
    .await;
    assert!(!is_error, "beta: {body:?}");
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "gamma checkpoint",
            "session_id": "sess-gamma",
            "remote": "https://github.com/org/gamma.git",
            "harness_version": "codex/3",
            "model": "gpt",
            "branch": "feat/c",
        }),
    )
    .await;
    assert!(!is_error, "gamma: {body:?}");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "coding", "harness": "claude-code/1"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["sess-alpha"]);
    let row = &body["sessions"][0];
    assert_eq!(row["branch"], "main");
    assert_eq!(row["remote_sanitized"], "github.com/org/alpha");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "model": "sonnet"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["sess-beta"]);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "branch": "feat/c"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["sess-gamma"]);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "repo": "/src/alpha"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["sess-alpha"], "repo matches repo_path");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "repo": "github.com/org/gamma"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["sess-gamma"], "repo matches remote_sanitized");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({
            "ocean_id": graph_id,
            "session_kind": "coding",
            "platform": "",
            "project": "github.com/org/alpha",
        }),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(
        session_ids_of(&body),
        vec!["sess-alpha"],
        "kind=coding + empty platform + project still works"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

#[tokio::test]
async fn search_atoms_project_and_session_id_filters_narrow_results_over_real_http() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-search-filters.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("search_filters");
    let (user, account, graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let mut captures = Vec::new();
    for (project, session, quote) in [
        ("alpha", "sess-alpha", "zzqfilterbind88 from project alpha"),
        ("beta", "sess-beta", "zzqfilterbind88 from project beta"),
    ] {
        let (error, body) = call_tool(
            &app,
            &token,
            "add_context",
            json!({"ocean_id":graph,"content":quote,"project":project,"session_id":session}),
        )
        .await;
        assert!(!error, "{body}");
        captures.push(body);
    }
    let evidence = publish_retained(
        &app,
        &pool,
        &token,
        graph,
        &[
            (
                &captures[0],
                "zzqfilterbind88 from project alpha",
                Some("alpha"),
            ),
            (
                &captures[1],
                "zzqfilterbind88 from project beta",
                Some("beta"),
            ),
        ],
    )
    .await;
    let (error, all) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query":"zzqfilterbind88","ocean_ids":[graph]}),
    )
    .await;
    assert!(!error, "{all}");
    assert_eq!(all["retained_citations"].as_array().unwrap().len(), 2);
    for source in &evidence {
        source.assert_cited(&all);
    }
    for filters in [
        json!({"project":"alpha"}),
        json!({"session_id":"sess-beta"}),
    ] {
        let mut args = json!({"query":"zzqfilterbind88","ocean_ids":[graph]});
        args.as_object_mut()
            .unwrap()
            .extend(filters.as_object().unwrap().clone());
        let (error, body) = call_tool(&app, &token, "search_atoms", args).await;
        assert!(error, "{body}");
        assert_eq!(error_code(&body), "invalid_params");
    }
    for source in &evidence {
        let (error,body)=call_tool(&app,&token,"search_atoms",json!({"query":"zzqfilterbind88","library_scope":{"graph_ids":[graph],"source_item_id":source.item}})).await;
        assert!(!error, "{body}");
        source.assert_cited(&body);
        assert_eq!(body["retained_citations"].as_array().unwrap().len(), 1);
    }
    cleanup(&pool, &[account], &[user]).await;
}

#[tokio::test]
async fn ask_ocean_project_filter_confines_citations_over_real_http() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-ask-project.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("ask_project");
    let (user, account, graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let mut captures = Vec::new();
    let quotes = [
        "zzqaskbind55 the answer lives in project gamma only",
        "zzqaskbind55 a decoy in project delta",
    ];
    for (project, quote) in ["gamma", "delta"].into_iter().zip(quotes) {
        let (error, body) = call_tool(
            &app,
            &token,
            "add_context",
            json!({"ocean_id":graph,"content":quote,"project":project}),
        )
        .await;
        assert!(!error, "{body}");
        captures.push(body);
    }
    let evidence = publish_retained(
        &app,
        &pool,
        &token,
        graph,
        &[
            (&captures[0], quotes[0], Some("gamma")),
            (&captures[1], quotes[1], Some("delta")),
        ],
    )
    .await;
    let gamma:Uuid=sqlx::query_scalar("SELECT n.node_id FROM nodes n JOIN node_semantic_atoms l ON l.node_id=n.node_id AND l.graph_id=n.graph_id WHERE n.graph_id=$1 AND l.semantic_atom_id=$2 AND n.entity_type='project'")
        .bind(graph).bind(evidence[0].semantic_atom).fetch_one(&pool).await.unwrap();
    let (error, legacy) = ask_ocean_and_wait(
        &app,
        &token,
        json!({"question":"zzqaskbind55","ocean_ids":[graph],"project":"gamma"}),
    )
    .await;
    assert!(error, "{legacy}");
    assert_eq!(error_code(&legacy), "invalid_params");
    let (error,body)=ask_ocean_and_wait(&app,&token,json!({"question":"zzqaskbind55","style":"sources-only","library_scope":{"graph_ids":[graph],"node_id":gamma}})).await;
    assert!(!error, "{body}");
    assert_eq!(body["mode"], "retrieval_only");
    evidence[0].assert_cited(&body);
    assert_eq!(body["retained_citations"].as_array().unwrap().len(), 1);
    assert!(body["retained_citations"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["source_version_id"] != evidence[1].version.to_string()));
    cleanup(&pool, &[account], &[user]).await;
}

// ── Ticket 18: duplicate re-ingest must not emit noise `atom_ingested` events ─
//
// `routes::ingest`'s cloud path (`ingest::submit`, exercised here through
// `add_context` against a REAL Postgres pool — the ONLY way this file can
// reach it) used to fire `insert_activity_event(..., "atom_ingested", ...)`
// unconditionally, even when every chunk of a re-ingest hit
// `atoms`' `ON CONFLICT (graph_id, source_type, source_id) DO NOTHING` and
// nothing was actually inserted (`atom_count: 0`). A prod hour of duplicate
// Teams-channel re-ingests produced 25 such zero-count events, and the
// Today-journal narrated "25 items were captured" for an hour that held
// nothing. The fix gates the emitter on `!new_atom_ids.is_empty()` —
// `new_atom_ids` is the post-insert, post-R2-upload set `insert_atoms_scoped`
// actually persisted, so it is the true "did anything land" signal.
//
// Both tests below rely on the chunker's `ON CONFLICT` key being POSITIONAL
// (`{source_id}:chunk:{position}`), not content-keyed (see
// `chunk_source_id_format` in `routes/ingest.rs` and the `ON CONFLICT
// (graph_id, source_type, source_id)` clause in `store/mod.rs`'s
// `insert_atoms`): re-supplying the SAME `source_id` collides at the SAME
// chunk position no matter what content is there, and a LONGER re-ingest
// only adds new chunks at positions past whatever was chunked before.

/// `n` distinct single-token words (`count_tokens` splits on whitespace),
/// e.g. `words("alpha", 3)` -> `"alpha0000 alpha0001 alpha0002"`. Used to
/// build `chunk_generic` input with a precisely known token count
/// (`n * 4 / 3`, matching `coralswarm_core::chunker::count_tokens`'s own
/// integer-division heuristic) so this file — which cannot name the
/// crate-private chunker directly — can still deterministically control how
/// many chunks a paragraph produces.
fn words(prefix: &str, n: usize) -> String {
    (0..n).map(|i| format!("{prefix}{i:04}")).collect::<Vec<_>>().join(" ")
}

/// Poll `count(*)` of `atom_ingested` rows for `account_id` until it differs
/// from `before` (an unwanted/awaited event landed) OR `timeout` elapses,
/// whichever comes first — same short fixed-interval-retry shape as this
/// file's own `poll_users_name` / `poll_activity_event_payload`. Used to
/// prove ABSENCE: unlike polling for a row to *appear* (which can return the
/// instant it's found), proving nothing landed means this must actually run
/// out the full `timeout` — the same deadline `poll_activity_event_payload`
/// gives a genuine emission to land — rather than a short fixed `sleep`
/// racing ahead of a slow (but real) `tokio::spawn`, which would let the
/// assertion pass vacuously.
async fn poll_activity_event_count_settles(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    event_type: &str,
    before: i64,
    timeout: std::time::Duration,
) -> i64 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM activity_events WHERE account_id = $1 AND event_type = $2",
        )
        .bind(account_id)
        .bind(event_type)
        .fetch_one(pool)
        .await
        .expect("count activity_events");
        if count != before || tokio::time::Instant::now() >= deadline {
            return count;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Poll `activity_events` for a row matching `(account_id, graph_id,
/// event_type)` whose `payload->>'chunks_created'` equals `chunks_created`,
/// up to `timeout`. `insert_activity_event` is fire-and-forget
/// (`tokio::spawn`), so a plain post-request `SELECT` would race it — same
/// short fixed-interval-retry shape as this file's own `poll_users_name`.
/// Returns `None` if no matching row appears before the deadline.
async fn poll_activity_event_payload(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    event_type: &str,
    chunks_created: i64,
    timeout: std::time::Duration,
) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let row: Option<Value> = sqlx::query_scalar(
            "SELECT payload FROM activity_events \
             WHERE account_id = $1 AND graph_id = $2 AND event_type = $3 \
               AND (payload->>'chunks_created')::bigint = $4 \
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(event_type)
        .bind(chunks_created)
        .fetch_optional(pool)
        .await
        .expect("poll activity_events");
        if row.is_some() || tokio::time::Instant::now() >= deadline {
            return row;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A fully-duplicate re-ingest (every chunk collides with an already-used
/// `source_id` position) must emit NO `atom_ingested` event at all — not
/// even a zero-`atom_count` one. First call establishes the one chunk at
/// `dup-note-1:chunk:0`; the second call re-supplies the SAME single-chunk
/// content under the same `source_id`, so it hits `ON CONFLICT DO NOTHING`
/// on that one position and inserts nothing.
#[tokio::test]
async fn ingest_full_duplicate_reingest_emits_no_atom_ingested_event() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-ingest-noop-noise.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("ingest_noop_noise");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let content = words("dupnoise", 290);

    // First ingest: genuinely new, must emit atom_ingested with chunks_created=1.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": content,
            "source_id": "dup-note-1",
        }),
    )
    .await;
    assert!(!is_error, "first add_context should succeed: {body:?}");
    assert_eq!(atom_ids_of(&body).len(), 1, "first ingest should land exactly one atom: {body:?}");

    let first_event = poll_activity_event_payload(
        &pool,
        account_id,
        graph_id,
        "atom_ingested",
        1,
        std::time::Duration::from_secs(2),
    )
    .await;
    let first_event = first_event.expect("genuinely-new ingest must emit an atom_ingested event");
    assert_eq!(first_event["atom_count"], 1, "first event's atom_count: {first_event:?}");

    let before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM activity_events WHERE account_id = $1 AND event_type = 'atom_ingested'",
    )
    .bind(account_id)
    .fetch_one(&pool)
    .await
    .expect("count events before re-ingest");

    // Second ingest: SAME source_id, SAME content -> the one chunk collides
    // at position 0, nothing is inserted (`atom_count: 0`). Must emit no event.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": content,
            "source_id": "dup-note-1",
        }),
    )
    .await;
    assert!(!is_error, "duplicate re-ingest add_context should still succeed: {body:?}");
    assert_eq!(
        atom_ids_of(&body).len(),
        0,
        "fully-duplicate re-ingest must land no new atoms: {body:?}"
    );
    assert_eq!(body["skipped_duplicate_chunks"], 1);

    // Run out the SAME 2-second deadline `poll_activity_event_payload` gives
    // a genuine emission to land above, rather than a short fixed `sleep`
    // that could race ahead of a slow (but real) `tokio::spawn` and let this
    // absence assertion pass vacuously. `poll_activity_event_count_settles`
    // returns the instant the count actually moves (a real bug: an unwanted
    // event landing late) OR once the full deadline has elapsed with no
    // movement (the correctly-suppressed case) — either way it never returns
    // before genuinely waiting out the window a late emission could still
    // land in.
    let after = poll_activity_event_count_settles(
        &pool,
        account_id,
        "atom_ingested",
        before,
        std::time::Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        after, before,
        "a fully-duplicate re-ingest must not add any new atom_ingested event row"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// A partial-duplicate re-ingest (some chunks collide, at least one is new)
/// must still emit `atom_ingested`, carrying the CORRECT (non-zero)
/// `atom_count` for only the newly-inserted atoms. First call ingests one
/// paragraph (`A`, chunked to exactly one chunk at position 0 — its token
/// count is comfortably under `chunk_generic`'s 400-token target). The
/// second call re-supplies the SAME `source_id` with `A` followed by a
/// second paragraph `B` large enough that `A` alone already fills the first
/// chunk's budget, so `B` is forced into a brand-new chunk at position 1:
/// position 0 collides (skipped), position 1 is new (inserted) — exactly one
/// genuinely-new atom out of two chunks.
#[tokio::test]
async fn ingest_partial_duplicate_reingest_emits_event_with_correct_atom_count() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-ingest-partial-noise.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("ingest_partial_noise");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let para_a = words("partA", 290);
    let para_b = words("partB", 290);

    // First ingest: paragraph A alone -> exactly one chunk at position 0.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": para_a.clone(),
            "source_id": "partial-note-1",
        }),
    )
    .await;
    assert!(!is_error, "first add_context should succeed: {body:?}");
    assert_eq!(atom_ids_of(&body).len(), 1, "first ingest should land exactly one atom: {body:?}");
    assert_eq!(body["chunks_created"], 1);

    poll_activity_event_payload(
        &pool,
        account_id,
        graph_id,
        "atom_ingested",
        1,
        std::time::Duration::from_secs(2),
    )
    .await
    .expect("genuinely-new first ingest must emit an atom_ingested event");

    // Second ingest: SAME source_id, content = A + B -> two chunks. Position
    // 0 (A) collides with the first call's chunk; position 1 (B) is new.
    let content = format!("{para_a}\n\n{para_b}");
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": content,
            "source_id": "partial-note-1",
        }),
    )
    .await;
    assert!(!is_error, "partial-duplicate add_context should succeed: {body:?}");
    assert_eq!(body["chunks_created"], 2, "A+B must chunk to exactly two chunks: {body:?}");
    assert_eq!(
        atom_ids_of(&body).len(),
        1,
        "partial-duplicate re-ingest must land exactly the one new (position-1) atom: {body:?}"
    );
    assert_eq!(body["skipped_duplicate_chunks"], 1);

    let second_event = poll_activity_event_payload(
        &pool,
        account_id,
        graph_id,
        "atom_ingested",
        2,
        std::time::Duration::from_secs(2),
    )
    .await;
    let second_event =
        second_event.expect("partial-duplicate ingest must still emit an atom_ingested event");
    assert_eq!(
        second_event["atom_count"], 1,
        "event must carry the count of ACTUALLY-inserted atoms, not chunks_created: {second_event:?}"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

// ── Agent sessions (`SessionKind::Agent`, migration 0219) ────────────────────
//
// The agent identity columns, the platform policy's stored outcome
// (`platform_detected` / `platform_verified`), the list/search filters and
// the subagent roll-up all live on real `coding_sessions` rows and real
// correlated SQL, so — like the rest of the coding-session tests in this
// file — only Postgres can prove them.

/// [`call_tool`] with a `User-Agent`, for the tests that need the server to
/// DETECT a platform. Uses [`raw_call_tool`] and reads the dual payload's
/// text block, so the UA must NOT match the `claude-code` structured-only
/// hint (which empties `content`) — `cursor/…` is used throughout.
async fn call_tool_ua(
    app: &axum::Router,
    token: &str,
    user_agent: &str,
    name: &str,
    arguments: Value,
) -> (bool, Value) {
    let result = raw_call_tool(app, token, Some(user_agent), name, arguments).await;
    let is_error = result.get("isError").and_then(|v| v.as_bool()).unwrap_or(false);
    let content = result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool result has no text content for {name}: {result:?}"));
    let body: Value = serde_json::from_str(content)
        .unwrap_or_else(|e| panic!("tool result text is not JSON for {name}: {e}\n{content}"));
    (is_error, body)
}

/// The SEP-2575 protocol version this backend runs (MCP `2026-07-28`; rmcp
/// 3.1.2) — matched against `Cargo.lock`'s pinned `rmcp` version. Reused for
/// both the `MCP-Protocol-Version` HTTP header AND the request `_meta`'s own
/// `io.modelcontextprotocol/protocolVersion`, which rmcp requires to be
/// byte-identical (`validate_request_protocol_version_meta`,
/// `transport/streamable_http_server/tower.rs`) whenever BOTH are present —
/// see [`raw_call_tool_meta`]'s doc comment for why both are sent at all.
const SEP_2575_PROTOCOL_VERSION: &str = "2026-07-28";

/// [`raw_call_tool`], but with a SEP-2575 `_meta` block attached to the
/// `tools/call` request's `params` — `io.modelcontextprotocol/clientInfo`
/// (`{name, version}`), `io.modelcontextprotocol/protocolVersion`, and
/// `io.modelcontextprotocol/clientCapabilities` — plus the matching
/// `MCP-Protocol-Version` HTTP header, so a tool handler's
/// `RequestContext::meta.client_info()` (rmcp 3.1.2's raw per-request `_meta`
/// reader) resolves the `clientInfo` this test actually sends. This file
/// deliberately does NOT exercise `RequestContext::client_info()` (the
/// higher-level convenience method) at all: verified against this exact
/// server, its stateless-mode legacy fallback returns a SYNTHETIC non-`None`
/// `Implementation::default()` (`{"rmcp", "3.1.2"}` — rmcp's own crate name/
/// version) for every request that carries no `_meta.clientInfo`, which is
/// why `add_context` reads `ctx.meta.client_info()` in production too — see
/// `McpAuthContext::mcp_client_name`'s doc for the full story.
///
/// The header AND the `_meta` protocol version are both required, and must
/// MATCH: rmcp's `validate_request_protocol_version_meta` runs unconditionally
/// (it does not gate on `with_stateless_protocol_metadata_required`, which
/// this server leaves `false` — see `mcp/mod.rs`'s `mcp_router` doc comment)
/// and, once it sees a `2026-07-28`-or-later `MCP-Protocol-Version` header
/// with NO `_meta.protocolVersion`, treats that as a modern-era request
/// missing its required `_meta` keys (`protocolVersion` AND
/// `clientCapabilities` — `RequestMetaObject::DRAFT_REQUIRED_KEYS`) and 400s
/// it before dispatch; when both header and `_meta.protocolVersion` ARE
/// present, it 400s instead if they disagree. `clientCapabilities` is
/// likewise sent (an empty object suffices — it's the draft schema's other
/// required key) so no test in this file has to special-case a partially
/// negotiated request. `clientInfo` itself stays OPTIONAL per the draft
/// schema, but every test using this helper sends it — that's the whole
/// point.
async fn raw_call_tool_meta(
    app: &axum::Router,
    token: &str,
    user_agent: Option<&str>,
    client_info: Option<(&str, &str)>,
    name: &str,
    arguments: Value,
) -> Value {
    let mut meta = json!({
        "io.modelcontextprotocol/protocolVersion": SEP_2575_PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
    });
    if let Some((client_name, client_version)) = client_info {
        meta["io.modelcontextprotocol/clientInfo"] = json!({
            "name": client_name,
            "version": client_version,
        });
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments, "_meta": meta},
    });
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"))
        .header(HEADER_MCP_PROTOCOL_VERSION, SEP_2575_PROTOCOL_VERSION);
    if let Some(ua) = user_agent {
        builder = builder.header(axum::http::header::USER_AGENT, ua);
    }
    let req = builder.body(Body::from(body.to_string())).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
    let rpc = if content_type.contains("text/event-stream") {
        parse_sse_json(&bytes)
    } else if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    assert_eq!(
        status,
        StatusCode::OK,
        "tools/call (with SEP-2575 _meta) transport-level failure for {name}: {rpc:?}"
    );
    rpc.get("result")
        .unwrap_or_else(|| panic!("no result in tools/call response for {name}: {rpc:?}"))
        .clone()
}

/// [`call_tool_ua`]'s sibling for [`raw_call_tool_meta`]: reads the dual
/// payload's text block the same way, so `client_info`'s name must not
/// canonicalize to `claude-code` (the structured-only hint) unless the test
/// wants that.
async fn call_tool_meta(
    app: &axum::Router,
    token: &str,
    user_agent: Option<&str>,
    client_info: Option<(&str, &str)>,
    name: &str,
    arguments: Value,
) -> (bool, Value) {
    let result = raw_call_tool_meta(app, token, user_agent, client_info, name, arguments).await;
    let is_error = result.get("isError").and_then(|v| v.as_bool()).unwrap_or(false);
    let content = result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool result has no text content for {name}: {result:?}"));
    let body: Value = serde_json::from_str(content)
        .unwrap_or_else(|e| panic!("tool result text is not JSON for {name}: {e}\n{content}"));
    (is_error, body)
}

fn session_ids_of(body: &Value) -> Vec<String> {
    body["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .map(|s| s["session_id"].as_str().expect("session_id").to_string())
        .collect()
}

fn result_atom_ids_of(body: &Value) -> Vec<Uuid> {
    body["results"]
        .as_array()
        .expect("results array")
        .iter()
        .map(|r| Uuid::parse_str(r["atom_id"].as_str().expect("atom_id")).expect("uuid"))
        .collect()
}

#[tokio::test]
async fn agent_sessions_carry_identity_and_filter_by_agent_platform_and_task_on_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-agent-sessions.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("agent_sessions");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // grokbot declares `grok` but the request comes from a cursor harness →
    // declared wins, detection is recorded, verified = false.
    let (is_error, body) = call_tool_ua(
        &app,
        &token,
        "cursor/2.0",
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "grokbot opened PR zzagentgrok42 for the billing story",
            "source_type": "note",
            "session_id": "agent-grokbot-1",
            "session_kind": "agent",
            "agent_name": "grokbot",
            "platform": "grok",
            "task": "CS-042 billing",
        }),
    )
    .await;
    assert!(
        !is_error,
        "agent add_context (grokbot) must succeed: {body:?}"
    );
    let grok_capture = body.clone();
    let grok_atoms = atom_ids_of(&body);
    assert!(!grok_atoms.is_empty());
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;

    // pr-reviewer declares an ALIAS spelling; no User-Agent → nothing to
    // disagree with → verified.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "pr-reviewer left review notes zzagentgrok42 on the migration",
            "session_id": "agent-reviewer-1",
            "session_kind": "agent",
            "agent_name": "pr-reviewer",
            "platform": "Claude Code",
            "task": "review CS-042",
        }),
    )
    .await;
    assert!(
        !is_error,
        "agent add_context (pr-reviewer) must succeed: {body:?}"
    );
    let reviewer_capture = body.clone();
    let reviewer_atoms = atom_ids_of(&body);

    // A plain coding session that happens to share the search token — must
    // never match an agent filter.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "human coding session mentions zzagentgrok42 too",
            "session_id": "coding-human-1",
        }),
    )
    .await;
    assert!(!is_error, "coding add_context must succeed: {body:?}");
    let human_capture = body.clone();

    // ── the rows ────────────────────────────────────────────────────────────
    let grok = store
        .get_coding_session(graph_id, "agent-grokbot-1")
        .await
        .expect("get")
        .expect("grokbot row");
    assert_eq!(grok.session_kind, "agent");
    assert_eq!(grok.identity.agent_name.as_deref(), Some("grokbot"));
    assert_eq!(
        grok.identity.platform.as_deref(),
        Some("grok"),
        "declared value is kept"
    );
    assert_eq!(
        grok.identity.platform_detected.as_deref(),
        Some("cursor"),
        "the User-Agent's harness is recorded separately"
    );
    assert!(
        !grok.identity.platform_verified(),
        "declared ≠ detected → unverified"
    );
    assert_eq!(grok.identity.task.as_deref(), Some("CS-042 billing"));
    assert_eq!(
        grok.identity.root_session_id.as_deref(),
        Some("agent-grokbot-1"),
        "a root is its own root"
    );
    assert_eq!(grok.identity.parent_session_id, None);

    let reviewer = store
        .get_coding_session(graph_id, "agent-reviewer-1")
        .await
        .expect("get")
        .expect("reviewer row");
    assert_eq!(
        reviewer.identity.platform.as_deref(),
        Some("claude-code"),
        "alias canonicalized"
    );
    assert_eq!(reviewer.identity.platform_detected, None);
    assert!(reviewer.identity.platform_verified());

    for id in grok_atoms.iter().chain(reviewer_atoms.iter()) {
        let source_type: String =
            sqlx::query_scalar("SELECT source_type FROM atoms WHERE atom_id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("fetch source_type");
        assert_eq!(source_type, "agent_session");
    }

    // ── list_sessions filters ───────────────────────────────────────────────
    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "agent"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    let mut ids = session_ids_of(&body);
    ids.sort();
    assert_eq!(
        ids,
        vec!["agent-grokbot-1", "agent-reviewer-1"],
        "kind=agent excludes the coding session"
    );
    let grok_row = body["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["session_id"] == "agent-grokbot-1")
        .unwrap();
    assert_eq!(grok_row["agent_name"], "grokbot");
    assert_eq!(grok_row["platform"], "grok");
    assert_eq!(grok_row["platform_detected"], "cursor");
    assert_eq!(grok_row["platform_verified"], json!(false));
    assert_eq!(grok_row["task"], "CS-042 billing");
    assert_eq!(grok_row["subagent_count"], json!(0));
    assert_eq!(grok_row["root_session_id"], "agent-grokbot-1");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "agent_name": "grokbot"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["agent-grokbot-1"]);

    // Alias spelling on the FILTER side canonicalizes too.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "platform": "claude_code"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["agent-reviewer-1"]);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "task": "cs-042"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    let mut ids = session_ids_of(&body);
    ids.sort();
    assert_eq!(
        ids,
        vec!["agent-grokbot-1", "agent-reviewer-1"],
        "task is a case-insensitive substring"
    );

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "task": "billing"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["agent-grokbot-1"]);

    let evidence = publish_retained(
        &app,
        &pool,
        &token,
        graph_id,
        &[
            (
                &grok_capture,
                "grokbot opened PR zzagentgrok42 for the billing story",
                None,
            ),
            (
                &reviewer_capture,
                "pr-reviewer left review notes zzagentgrok42 on the migration",
                None,
            ),
            (
                &human_capture,
                "human coding session mentions zzagentgrok42 too",
                None,
            ),
        ],
    )
    .await;
    let (error, all) = call_tool(
        &app,
        &token,
        "search_atoms",
        json!({"query":"zzagentgrok42","ocean_ids":[graph_id]}),
    )
    .await;
    assert!(!error, "{all}");
    assert_eq!(all["retained_citations"].as_array().unwrap().len(), 3);
    for source in &evidence {
        source.assert_cited(&all);
    }
    for filter in [
        json!({"agent_name":"grokbot"}),
        json!({"platform":"Claude Code"}),
        json!({"source_type":"agent_session"}),
    ] {
        let mut args = json!({"query":"zzagentgrok42","ocean_ids":[graph_id]});
        args.as_object_mut()
            .unwrap()
            .extend(filter.as_object().unwrap().clone());
        let (error, body) = call_tool(&app, &token, "search_atoms", args).await;
        assert!(error, "{body}");
        assert_eq!(error_code(&body), "invalid_params");
    }
    let (error,one)=call_tool(&app,&token,"search_atoms",json!({"query":"zzagentgrok42","library_scope":{"graph_ids":[graph_id],"source_item_id":evidence[0].item}})).await;
    assert!(!error, "{one}");
    evidence[0].assert_cited(&one);
    assert_eq!(one["retained_citations"].as_array().unwrap().len(), 1);

    let (error,agents)=call_tool(&app,&token,"search_atoms",json!({"query":"zzagentgrok42","library_scope":{"graph_ids":[graph_id],"collection":"agent_sessions"}})).await;
    assert!(!error,"{agents}");
    assert_eq!(agents["retained_citations"].as_array().unwrap().len(),3);
    for source in &evidence {source.assert_cited(&agents);}

    let (error, agent_only) = call_tool(&app, &token, "search_atoms", json!({"query":"zzagentgrok42","library_scope":{"graph_ids":[graph_id],"content_kind":"agent_session"}})).await;
    assert!(!error, "{agent_only}");
    assert_eq!(agent_only["retained_citations"].as_array().unwrap().len(), 2);
    for source in &evidence[..2] { source.assert_cited(&agent_only); }
    let (error, coding_only) = call_tool(&app, &token, "search_atoms", json!({"query":"zzagentgrok42","library_scope":{"graph_ids":[graph_id],"content_kind":"coding_session"}})).await;
    assert!(!error, "{coding_only}");
    assert_eq!(coding_only["retained_citations"].as_array().unwrap().len(), 1);
    evidence[2].assert_cited(&coding_only);
    let (error, unobserved_service) = call_tool(&app, &token, "search_atoms", json!({"query":"zzagentgrok42","library_scope":{"graph_ids":[graph_id],"service_id":"mcp","content_kind":"agent_session"}})).await;
    assert!(!error, "{unobserved_service}");
    assert!(unobserved_service["retained_citations"].as_array().unwrap().is_empty());
    let facets = enrichment_post(&app, &token, "/v1/library/facets", json!({"scope":{"graph_ids":[graph_id]},"dimension":"content_kinds"})).await;
    for (kind, count) in [("agent_session", 2), ("coding_session", 1)] {
        let facet = facets["items"].as_array().unwrap().iter().find(|v| v["value"] == kind).expect("native content kind facet");
        assert_eq!(facet["source_count"], count);
    }
    let discovery = enrichment_post(&app, &token, "/v1/library/search", json!({"scope":{"graph_ids":[graph_id]}})).await;
    for (kind, count) in [("agent_session", 2), ("coding_session", 1)] {
        let facet = discovery["facets"]["content_kinds"].as_array().unwrap().iter().find(|v| v["value"] == kind).expect("summary native content kind facet");
        assert_eq!(facet["source_count"], count);
    }
    assert_eq!(discovery["facets"]["unknown_origin_count"], 3);
    let dek = coralswarm_backend::crypto::resolve_dek(&pool, account_id).await.unwrap().unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_id).await.unwrap();
    let quote = "explicit source origin zzagentorigin42 is authoritative";
    let metadata = json!({"title":"Observed source origin fixture"});
    let origin = coralswarm_backend::source_origin::SourceOrigin::native("mcp", "note", "mcp_contract_fixture").unwrap();
    let captured = coralswarm_backend::source_evidence::capture_source_version_with_origin_tx(&mut tx, None, graph_id, account_id,
        &coralswarm_backend::source_evidence::CaptureSource { source_type:"agent_session",provider_namespace:"fixture:mcp:explicit-origin",source_id:"explicit-origin",normalized_text:quote,representation:"normalized_text",metadata:&metadata,occurred_at:Some(chrono::Utc::now()),participants:&[],ingested_by:Some(user_id),scope:"private",duration_ms:None,segments:None }, None, &dek, Some(&origin)).await.unwrap();
    tx.commit().await.unwrap();
    let explicit = publish_versions(&app, &pool, &token, graph_id, vec![PendingEvidence {item:captured.source_item_id,version:captured.version_id,quote,project:None}]).await;
    let (error, conflicting) = call_tool(&app, &token, "search_atoms", json!({"query":"zzagentorigin42","library_scope":{"graph_ids":[graph_id],"source_item_id":captured.source_item_id,"content_kind":"agent_session"}})).await;
    assert!(error, "{conflicting}");
    assert_eq!(error_code(&conflicting), "not_found");
    let (error, observed) = call_tool(&app, &token, "search_atoms", json!({"query":"zzagentorigin42","library_scope":{"graph_ids":[graph_id],"source_item_id":captured.source_item_id,"service_id":"mcp","content_kind":"note"}})).await;
    assert!(!error, "{observed}");
    assert_eq!(observed["retained_citations"].as_array().unwrap().len(), 1);
    explicit[0].assert_cited(&observed);
    let facets = enrichment_post(&app, &token, "/v1/library/facets", json!({"scope":{"graph_ids":[graph_id]},"dimension":"content_kinds"})).await;
    let note = facets["items"].as_array().unwrap().iter().find(|v| v["value"] == "note").unwrap();
    assert_eq!(note["source_count"], 1);
    let agents = facets["items"].as_array().unwrap().iter().find(|v| v["value"] == "agent_session").unwrap();
    assert_eq!(agents["source_count"], 2);

    // ── kind conflict still 409s ────────────────────────────────────────────
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "now claiming to be a plain coding session",
            "session_id": "agent-grokbot-1",
        }),
    )
    .await;
    assert!(
        is_error,
        "flipping an agent session to coding must be refused: {body:?}"
    );
    assert_eq!(error_code(&body), "invalid_params");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// SEP-2575 `clientInfo` (clientinfo-platform-detection) drives
/// `platform_detected` ahead of the `User-Agent` fallback and fills an
/// undeclared `harness_version` — exactly the case a UA-only harness never
/// could: `node` names no product, but `clientInfo.name: "OpenCode"` does.
/// A declared `platform`/`harness_version` is still never overwritten by it,
/// same rule as every other coding-session field.
#[tokio::test]
async fn add_context_detects_platform_and_harness_version_from_mcp_client_info_on_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-client-info.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("client_info_platform");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // 1. No declared platform/harness_version. `User-Agent: node` names
    //    nothing (the whole motivating case — a generic MCP-SDK runtime UA);
    //    clientInfo names the harness AND its version.
    let (is_error, body) = call_tool_meta(
        &app,
        &token,
        Some("node"),
        Some(("OpenCode", "1.4.2")),
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "opencode agent session checkpoint one",
            "session_id": "client-info-opencode-1",
            "session_kind": "agent",
            "agent_name": "opencode-bot",
        }),
    )
    .await;
    assert!(!is_error, "add_context (opencode via clientInfo, no declared platform) must succeed: {body:?}");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "agent", "agent_name": "opencode-bot"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    let row = body["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["session_id"] == "client-info-opencode-1")
        .unwrap_or_else(|| panic!("no client-info-opencode-1 row in {body:?}"));
    assert_eq!(row["platform"], "opencode", "clientInfo.name fills the undeclared platform");
    assert_eq!(row["platform_detected"], "opencode", "clientInfo, not the generic `node` UA, was detected");
    assert_eq!(row["platform_verified"], json!(true), "nothing declared to disagree with");
    assert_eq!(row["harness_version"], "1.4.2", "clientInfo.version fills the undeclared harness_version");

    // 2. Declares `platform: claude-code` while clientInfo says `opencode` —
    //    declared wins for the effective platform, detection is recorded
    //    underneath it, and the disagreement flips `platform_verified` to
    //    false — the same declared-vs-detected contract clientInfo now
    //    feeds into, unchanged from the User-Agent-only world.
    let (is_error, body) = call_tool_meta(
        &app,
        &token,
        Some("node"),
        Some(("opencode", "2.0.0")),
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "claude-code agent session checkpoint, opencode client_info disagrees",
            "session_id": "client-info-declared-wins-1",
            "session_kind": "agent",
            "agent_name": "declared-wins-bot",
            "platform": "claude-code",
        }),
    )
    .await;
    assert!(!is_error, "add_context (declared platform overrides clientInfo) must succeed: {body:?}");

    let declared_wins = store
        .get_coding_session(graph_id, "client-info-declared-wins-1")
        .await
        .expect("get")
        .expect("declared-wins row");
    assert_eq!(declared_wins.identity.platform.as_deref(), Some("claude-code"), "a declared platform is kept");
    assert_eq!(
        declared_wins.identity.platform_detected.as_deref(),
        Some("opencode"),
        "clientInfo's harness is still recorded separately"
    );
    assert!(
        !declared_wins.identity.platform_verified(),
        "declared claude-code != clientInfo-detected opencode -> unverified"
    );

    // 3. Declares `harness_version` explicitly — clientInfo's own (different)
    //    version must NOT overwrite it, same "never rewrite a declared
    //    value" rule as `platform`.
    let (is_error, body) = call_tool_meta(
        &app,
        &token,
        Some("node"),
        Some(("opencode", "9.9.9")),
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "a plain coding session that already declared its own harness_version",
            "session_id": "client-info-declared-harness-1",
            "harness_version": "1.2.3",
        }),
    )
    .await;
    assert!(!is_error, "add_context (declared harness_version) must succeed: {body:?}");

    let declared_harness = store
        .get_coding_session(graph_id, "client-info-declared-harness-1")
        .await
        .expect("get")
        .expect("declared-harness row");
    assert_eq!(
        declared_harness.harness_version.as_deref(),
        Some("1.2.3"),
        "a declared harness_version is never overwritten by clientInfo.version"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// grokbot spawns two helpers, one of which spawns a third: every child
/// inherits `agent_name=grokbot` / `platform` from the root (its own name is
/// kept as `subagent_name`), `root_session_id` points at the root at every
/// depth, the default listing is ONE root with `subagent_count=3` and a
/// tree-wide `coral_count`, `parent_session_id` lists direct children,
/// and a parent that is missing or not an agent is refused.
#[tokio::test]
async fn subagents_roll_up_to_their_root_agent_on_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-agent-rollup.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let store = PostgresStore::new(pool.clone());

    let sub = fresh_sub("agent_rollup");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let mut all_atoms: Vec<Uuid> = Vec::new();
    let mut captures: Vec<(Value, String)> = Vec::new();

    // Root: two checkpoints.
    for i in 0..2 {
        let (is_error, body) = call_tool(
            &app,
            &token,
            "add_context",
            json!({
                "ocean_id": graph_id,
                "content": format!("grokbot root checkpoint {i} zzrollup77"),
                "session_id": "rollup-root",
                "session_kind": "agent",
                "agent_name": "grokbot",
                "platform": "grok",
                "task": "CS-099 orchestrate",
            }),
        )
        .await;
        assert!(!is_error, "root add_context #{i}: {body:?}");
        captures.push((body.clone(), format!("grokbot root checkpoint {i} zzrollup77")));
        let ids = atom_ids_of(&body);
        mark_atoms_searchable(&pool, &ids).await;
        all_atoms.extend(ids);
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }

    // Helper A: declares its own name and task, NO platform, unknown harness
    // — inheritance satisfies the platform rule.
    let (is_error, body) = call_tool_ua(
        &app,
        &token,
        "node-fetch/1.0",
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "helper-a wrote the migration zzrollup77",
            "session_id": "rollup-child-a",
            "session_kind": "agent",
            "agent_name": "helper-a",
            "task": "CS-099 migration",
            "parent_session_id": "rollup-root",
        }),
    )
    .await;
    assert!(!is_error, "child A add_context: {body:?}");
    captures.push((body.clone(), "helper-a wrote the migration zzrollup77".into()));
    let a_atoms = atom_ids_of(&body);
    mark_atoms_searchable(&pool, &a_atoms).await;
    all_atoms.extend(a_atoms.iter().copied());

    // Helper B: declares nothing but the link → inherits everything.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "helper-b ran the tests zzrollup77",
            "session_id": "rollup-child-b",
            "session_kind": "agent",
            "parent_session_id": "rollup-root",
        }),
    )
    .await;
    assert!(!is_error, "child B add_context: {body:?}");
    captures.push((body.clone(), "helper-b ran the tests zzrollup77".into()));
    let b_atoms = atom_ids_of(&body);
    mark_atoms_searchable(&pool, &b_atoms).await;
    all_atoms.extend(b_atoms.iter().copied());

    // Grandchild C under A: two levels deep, same root.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "helper-c reviewed the migration zzrollup77",
            "session_id": "rollup-grandchild-c",
            "session_kind": "agent",
            "agent_name": "helper-c",
            "parent_session_id": "rollup-child-a",
        }),
    )
    .await;
    assert!(!is_error, "grandchild C add_context: {body:?}");
    captures.push((body.clone(), "helper-c reviewed the migration zzrollup77".into()));
    let c_atoms = atom_ids_of(&body);
    mark_atoms_searchable(&pool, &c_atoms).await;
    all_atoms.extend(c_atoms.iter().copied());

    // ── inheritance on the rows ─────────────────────────────────────────────
    let a = store.get_coding_session(graph_id, "rollup-child-a").await.unwrap().unwrap();
    assert_eq!(a.session_kind, "agent");
    assert_eq!(a.identity.agent_name.as_deref(), Some("grokbot"), "effective name is the root's");
    assert_eq!(a.identity.subagent_name.as_deref(), Some("helper-a"), "declared name kept as subagent_name");
    assert_eq!(a.identity.platform.as_deref(), Some("grok"), "platform inherited");
    assert_eq!(a.identity.task.as_deref(), Some("CS-099 migration"), "a child may declare its own task");
    assert_eq!(a.identity.parent_session_id.as_deref(), Some("rollup-root"));
    assert_eq!(a.identity.root_session_id.as_deref(), Some("rollup-root"));

    let b = store.get_coding_session(graph_id, "rollup-child-b").await.unwrap().unwrap();
    assert_eq!(b.identity.agent_name.as_deref(), Some("grokbot"));
    assert_eq!(b.identity.subagent_name, None);
    assert_eq!(b.identity.task.as_deref(), Some("CS-099 orchestrate"), "task defaults to the root's");

    let c = store.get_coding_session(graph_id, "rollup-grandchild-c").await.unwrap().unwrap();
    assert_eq!(c.identity.agent_name.as_deref(), Some("grokbot"));
    assert_eq!(c.identity.subagent_name.as_deref(), Some("helper-c"));
    assert_eq!(c.identity.parent_session_id.as_deref(), Some("rollup-child-a"));
    assert_eq!(c.identity.root_session_id.as_deref(), Some("rollup-root"), "a grandchild rolls up to the root");
    assert_eq!(c.identity.task.as_deref(), Some("CS-099 orchestrate"), "task defaults to the ROOT's, not the parent's");

    for id in &all_atoms {
        let source_type: String = sqlx::query_scalar("SELECT source_type FROM atoms WHERE atom_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(source_type, "agent_session");
    }

    // ── list_sessions: roots only by default ────────────────────────────────
    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "agent"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["rollup-root"], "default listing is roots only: {body:?}");
    let root_row = &body["sessions"][0];
    assert_eq!(root_row["subagent_count"], json!(3), "every descendant counts, at any depth");
    assert_eq!(
        root_row["coral_count"],
        json!(all_atoms.len() as i64),
        "coral_count aggregates the whole tree"
    );

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "parent_session_id": "rollup-root"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    let mut ids = session_ids_of(&body);
    ids.sort();
    assert_eq!(ids, vec!["rollup-child-a", "rollup-child-b"], "direct children only");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "roots_only": false}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body).len(), 4, "flat listing shows every session");
    let a_row = body["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["session_id"] == "rollup-child-a")
        .unwrap();
    assert_eq!(a_row["coral_count"], json!(a_atoms.len() as i64), "flat listing counts per session");
    assert_eq!(a_row["subagent_name"], "helper-a");
    assert_eq!(a_row["agent_name"], "grokbot");

    // agent_name matches the whole tree because children inherit it.
    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "agent_name": "grokbot", "roots_only": false}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body).len(), 4);

    for (session,task,subagent,parent) in [
        ("rollup-child-a","CS-099 migration",Some("helper-a"),"rollup-root"),
        ("rollup-child-b","CS-099 orchestrate",None,"rollup-root"),
        ("rollup-grandchild-c","CS-099 orchestrate",Some("helper-c"),"rollup-child-a"),
    ] {
        let mut tx=coralswarm_backend::tenancy::scope::begin_account_scoped(&pool,account_id).await.unwrap();
        coralswarm_backend::tenancy::scope::set_tenant_user(&mut tx,user_id).await.unwrap();
        let captured:Value=sqlx::query_scalar("SELECT library_capture_context(graph_id,version_id) FROM source_versions WHERE graph_id=$1 AND source_metadata->>'session_id'=$2 ORDER BY captured_at DESC,version_id DESC LIMIT 1")
            .bind(graph_id).bind(session).fetch_one(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(captured["agent_name"],"grokbot");
        assert_eq!(captured["platform"],"grok");
        assert_eq!(captured["task"],task);
        assert_eq!(captured["subagent_name"],json!(subagent));
        assert_eq!(captured["parent_session_id"],parent);
        assert_eq!(captured["root_session_id"],"rollup-root");
    }
    let (error,default_task)=call_tool(&app,&token,"list_sessions",json!({"ocean_id":graph_id,"task":"CS-099 orchestrate","roots_only":false})).await;
    assert!(!error,"{default_task}");
    let mut task_ids=session_ids_of(&default_task);task_ids.sort();
    assert_eq!(task_ids,vec!["rollup-child-b","rollup-grandchild-c","rollup-root"]);
    let (error,platform)=call_tool(&app,&token,"list_sessions",json!({"ocean_id":graph_id,"platform":"grok","roots_only":false})).await;
    assert!(!error,"{platform}");assert_eq!(session_ids_of(&platform).len(),4);

    // ── the detail route's subagents array (store-level) ────────────────────
    let subs = store.list_session_subagents(graph_id, "rollup-root", 50).await.unwrap();
    let mut sub_ids: Vec<&str> = subs.iter().map(|s| s.session_id.as_str()).collect();
    sub_ids.sort();
    assert_eq!(sub_ids, vec!["rollup-child-a", "rollup-child-b", "rollup-grandchild-c"]);
    let c_sub = subs.iter().find(|s| s.session_id == "rollup-grandchild-c").unwrap();
    assert_eq!(c_sub.parent_session_id.as_deref(), Some("rollup-child-a"));
    assert_eq!(c_sub.coral_count, c_atoms.len() as i64);
    assert!(
        store.list_session_subagents(graph_id, "rollup-child-a", 50).await.unwrap().is_empty(),
        "only the root owns the tree"
    );

    // ── search_atoms across the tree ────────────────────────────────────────
    let captured:Vec<_>=captures.iter().map(|(body,quote)|(body,quote.as_str(),None)).collect();
    let evidence=publish_retained(&app,&pool,&token,graph_id,&captured).await;
    for filter in [
        json!({"agent_name": "grokbot"}),
        json!({"root_session_id": "rollup-root"}),
        json!({"platform": "grok"}),
        json!({"session_id": "rollup-child-a"}),
    ] {
        let mut args = json!({"query": "zzrollup77", "ocean_ids": [graph_id]});
        args.as_object_mut().unwrap().extend(filter.as_object().unwrap().clone());
        let (error, body) = call_tool(&app, &token, "search_atoms", args).await;
        assert!(error, "{filter}: {body}");
        assert_eq!(error_code(&body),"invalid_params");
    }
    let (error,tree)=call_tool(&app,&token,"search_atoms",json!({"query":"zzrollup77","library_scope":{"graph_ids":[graph_id],"content_kind":"agent_session"}})).await;
    assert!(!error,"{tree}");
    assert_eq!(tree["retained_citations"].as_array().unwrap().len(),evidence.len());
    for source in &evidence {source.assert_cited(&tree);}
    let child=&evidence[2];
    let (error,one)=call_tool(&app,&token,"search_atoms",json!({"query":"zzrollup77","library_scope":{"graph_ids":[graph_id],"source_item_id":child.item}})).await;
    assert!(!error,"{one}");
    assert_eq!(one["retained_citations"].as_array().unwrap().len(),1);
    child.assert_cited(&one);

    // ── refusals ────────────────────────────────────────────────────────────
    let atoms_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    let refusal_writes_sql = "SELECT jsonb_build_array((SELECT count(*) FROM atoms WHERE graph_id=$1),(SELECT count(*) FROM source_items WHERE graph_id=$1),(SELECT count(*) FROM source_versions WHERE graph_id=$1),(SELECT count(*) FROM coding_sessions WHERE graph_id=$1),(SELECT count(*) FROM jobs WHERE payload->>'graph_id'=$1::text))";
    let orphan_writes_before: Value = sqlx::query_scalar(refusal_writes_sql)
        .bind(graph_id).fetch_one(&pool).await.unwrap();

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "orphan",
            "session_id": "rollup-orphan",
            "session_kind": "agent",
            "parent_session_id": "rollup-does-not-exist",
        }),
    )
    .await;
    assert!(is_error, "a missing parent must be refused: {body:?}");
    assert_eq!(error_code(&body), "invalid_params");
    let orphan_writes_after: Value = sqlx::query_scalar(refusal_writes_sql)
        .bind(graph_id).fetch_one(&pool).await.unwrap();
    assert_eq!(orphan_writes_after, orphan_writes_before, "missing-parent refusal wrote no graph content, source identity, session or job");
    assert!(store.get_coding_session(graph_id, "rollup-orphan").await.unwrap().is_none());

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({"ocean_id": graph_id, "content": "a human coding session", "session_id": "rollup-human"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    let human_atoms = atom_ids_of(&body).len() as i64;
    assert!(human_atoms > 0);
    let human = store.get_coding_session(graph_id, "rollup-human").await.unwrap().unwrap();
    assert_eq!(human.session_kind, "coding");
    let human_writes_before: Value = sqlx::query_scalar(refusal_writes_sql)
        .bind(graph_id).fetch_one(&pool).await.unwrap();
    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "child of a human",
            "session_id": "rollup-child-of-human",
            "session_kind": "agent",
            "platform": "cursor",
            "parent_session_id": "rollup-human",
        }),
    )
    .await;
    assert!(is_error, "a non-agent parent must be refused: {body:?}");
    assert_eq!(error_code(&body), "invalid_params");
    let human_writes_after: Value = sqlx::query_scalar(refusal_writes_sql)
        .bind(graph_id).fetch_one(&pool).await.unwrap();
    assert_eq!(human_writes_after, human_writes_before, "non-agent-parent refusal wrote no graph content, source identity, session or job");
    assert!(store.get_coding_session(graph_id, "rollup-child-of-human").await.unwrap().is_none());
    let human = store.get_coding_session(graph_id, "rollup-human").await.unwrap().unwrap();
    assert_eq!(human.session_kind, "coding");

    let atoms_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(atoms_after, atoms_before + human_atoms, "refused children wrote no atoms");
    assert!(
        store.get_coding_session(graph_id, "rollup-orphan").await.unwrap().is_none(),
        "a refused child has no session row"
    );

    cleanup(&pool, &[account_id], &[user_id]).await;
}

// ── session_kind inference (session-kind-inference plan) ─────────────────────
//
// `resolve_undeclared_session_kind` / `apply_agent_platform_safety_valve` /
// `effective_root_platform` are unit-tested directly (no HTTP, no database)
// in `ingest::atom`'s `tests` module — this is the full-stack proof that
// the SAME behavior holds through the real `add_context` MCP tool, real
// clientInfo/User-Agent detection, and real `list_sessions` reads against
// Postgres.

/// Scenario (a): a caller that names its agent but sends no `session_kind`
/// at all is inferred as `agent` — and, because that's the agent-kind
/// platform policy (not the coding-kind one), a platform its User-Agent
/// alone identifies still lands on the row.
#[tokio::test]
async fn undeclared_session_with_agent_name_is_inferred_as_agent_and_gets_the_agent_platform_policy() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-kind-inference-a.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("kind_infer_a");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    // `raw_call_tool`, not `call_tool_ua`: "claude-code/2.1 (cli)" matches the
    // structuredContent-only negotiation's built-in hint
    // (`mcp::tools::structured_only_client`), so `content` comes back EMPTY —
    // `call_tool_ua`'s text-block extraction would panic on that (see
    // `add_context_stamps_mcp_client_name_from_user_agent_against_real_postgres`
    // for the same idiom).
    let raw = raw_call_tool(
        &app,
        &token,
        Some("claude-code/2.1.263 (cli)"),
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "grokbot checkpoint with no session_kind at all",
            "session_id": "kind-infer-a",
            "agent_name": "grokbot",
            // NOTE: no "session_kind" key — inference is the whole point.
        }),
    )
    .await;
    assert_eq!(raw["isError"].as_bool().unwrap_or(false), false, "add_context should succeed: {raw:?}");
    let structured = raw
        .get("structuredContent")
        .unwrap_or_else(|| panic!("structuredContent missing: {raw:?}"));
    assert!(!atom_ids_of(structured).is_empty());

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "agent"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["kind-infer-a"], "inferred agent must list under kind=agent");
    let row = &body["sessions"][0];
    assert_eq!(row["agent_name"], "grokbot");
    assert_eq!(row["platform"], "claude-code", "agent-kind policy: no declared platform falls back to detected");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Scenario (b): no `agent_name`, no `parent_session_id`, no declared
/// `session_kind` — but the MCP client's clientInfo names a known agent
/// framework (`openclaw`), which alone is enough to infer `agent`. The
/// runtime `User-Agent` (`node`) is exactly the generic string clientInfo
/// detection exists for — it could never identify a harness on its own.
#[tokio::test]
async fn undeclared_session_with_agent_framework_client_info_is_inferred_as_agent() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-kind-inference-b.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("kind_infer_b");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool_meta(
        &app,
        &token,
        Some("node"),
        Some(("OpenClaw", "0.9")),
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "openclaw checkpoint, no agent_name, no session_kind",
            "session_id": "kind-infer-b",
        }),
    )
    .await;
    assert!(!is_error, "add_context should succeed: {body:?}");
    assert!(!atom_ids_of(&body).is_empty());

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "agent"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["kind-infer-b"], "openclaw clientInfo alone must infer kind=agent");
    let row = &body["sessions"][0];
    assert!(row["agent_name"].is_null(), "the framework name is never borrowed as agent_name: {row:?}");
    assert_eq!(row["platform"], "openclaw");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Scenario (c): the platform safety valve. A structural tell
/// (`agent_name`) infers `agent`, but NOTHING identifies a platform — no
/// declared `platform`, a generic `User-Agent` (`node`), and no clientInfo at
/// all. A DECLARED `session_kind=agent` in this exact shape 422s
/// (`add_context_agent_kind_requires_a_declared_or_detected_platform`); an
/// INFERRED one must instead succeed and land as `coding` — nothing that
/// saves today may start failing because the server got smarter about
/// filing it.
#[tokio::test]
async fn undeclared_agent_like_session_with_no_platform_anywhere_degrades_to_coding() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-kind-inference-c.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("kind_infer_c");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool_ua(
        &app,
        &token,
        "node",
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "solo-bot checkpoint, agent_name but no platform anywhere",
            "session_id": "kind-infer-c",
            "agent_name": "solo-bot",
        }),
    )
    .await;
    assert!(
        !is_error,
        "an inferred agent with no platform must degrade to coding, not 422: {body:?}"
    );
    assert!(!atom_ids_of(&body).is_empty());

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "coding"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["kind-infer-c"], "degraded session must list under kind=coding");

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "agent"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert!(session_ids_of(&body).is_empty(), "a degraded session must never list under kind=agent: {body:?}");

    cleanup(&pool, &[account_id], &[user_id]).await;
}

/// Scenario (d): a DECLARED `session_kind` always wins, in both directions —
/// `"coding"` alongside `agent_name` stays `coding`, the mirror image of
/// `add_context_with_agent_session_kind_derives_agent_session`'s declared
/// `"agent"` alongside no other signal staying `agent`.
#[tokio::test]
async fn declared_coding_kind_wins_over_agent_name_on_real_postgres() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-kind-inference-d.example/mcp").await;
    let app = app_for_pool(pool.clone());

    let sub = fresh_sub("kind_infer_d");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "add_context",
        json!({
            "ocean_id": graph_id,
            "content": "declared coding despite an agent_name",
            "session_id": "kind-infer-d",
            "session_kind": "coding",
            "agent_name": "should-not-flip-the-kind",
        }),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert!(!atom_ids_of(&body).is_empty());

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "coding"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert_eq!(session_ids_of(&body), vec!["kind-infer-d"]);

    let (is_error, body) = call_tool(
        &app,
        &token,
        "list_sessions",
        json!({"ocean_id": graph_id, "session_kind": "agent"}),
    )
    .await;
    assert!(!is_error, "{body:?}");
    assert!(session_ids_of(&body).is_empty(), "a declared coding kind must never list under kind=agent: {body:?}");

    cleanup(&pool, &[account_id], &[user_id]).await;
}


#[tokio::test]
async fn date_recap_pages_cover_two_days_without_promoting_headlines_to_evidence() {
    let local=coralswarm_backend::test_app_state();
    assert!(local.db.is_none());
    let local_tenant=coralswarm_backend::tenancy::extractor::Tenant {
        pool: None,
        store: local.store.clone(),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let local_user=coralswarm_backend::UserContext {user_id:Uuid::new_v4(),account_id:Uuid::new_v4(),role:"owner".into(),device_id:"local-recap-negative".into()};
    let local_result=coralswarm_backend::test_retained_mcp_dispatch(local,&local_tenant,&local_user,json!({"method":"tools/call","params":{"name":"ask_ocean","arguments":{"question":"recap today","recap_after":"cloud-only-cursor"}}})).await.unwrap();
    assert_eq!(local_result["isError"],true,"{local_result}");
    let local_body:Value=serde_json::from_str(local_result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(error_code(&local_body),"invalid_params");

    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-recap-pages.example/mcp").await;
    let model = Arc::new(EvidenceModel {
        quote: "unused generation",
        project: None,
        calls: std::sync::atomic::AtomicUsize::new(0),
        prompts: std::sync::Mutex::new(vec![]),
    });
    let embed = Arc::new(EmbeddingObserver(std::sync::atomic::AtomicUsize::new(0)));
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool.clone())));
    state.llm = Some(model.clone());
    state.embedder = embed.clone();
    let app = coralswarm_backend::test_support_app(Arc::new(state));
    let sub = fresh_sub("recap_pages");
    let (user, account, graph) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    PostgresStore::new(pool.clone())
        .set_user_timezone(user, "America/Chicago")
        .await
        .unwrap();
    use chrono::{Datelike, TimeZone};
    let tz: chrono_tz::Tz = "America/Chicago".parse().unwrap();
    let local = Utc::now().with_timezone(&tz);
    let midnight = tz
        .with_ymd_and_hms(local.year(), local.month(), local.day(), 0, 0, 0)
        .single()
        .unwrap()
        .with_timezone(&Utc);
    let yesterday = tz
        .with_ymd_and_hms(
            (local.date_naive() - chrono::Duration::days(1)).year(),
            (local.date_naive() - chrono::Duration::days(1)).month(),
            (local.date_naive() - chrono::Duration::days(1)).day(),
            0,
            0,
            0,
        )
        .single()
        .unwrap()
        .with_timezone(&Utc);
    let retained_quote = "The retained source has exact passage evidence.";
    let (error, capture) = call_tool(
        &app,
        &token,
        "add_context",
        json!({"ocean_id":graph,"content":retained_quote,"timestamp":midnight}),
    )
    .await;
    assert!(!error, "{capture}");
    let retained = publish_retained(
        &app,
        &pool,
        &token,
        graph,
        &[(&capture, retained_quote, None)],
    )
    .await;
    let expected:Vec<Uuid>=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,title_kind,timestamp) SELECT $1,'note','recap-page-'||n,'fixture-only','PRIVATE BODY MUST NEVER BE OUTPUT',CASE WHEN n=0 THEN 'Unicode café 🪸' WHEN n=1 THEN repeat('長',1200) ELSE 'Unrelated headline '||n END,'generated',CASE WHEN n<139 THEN $2 ELSE $3 END FROM generate_series(0,220) n RETURNING atom_id")
        .bind(graph).bind(midnight).bind(yesterday).fetch_all(&pool).await.unwrap();
    for (title, time) in [
        (None, Some(midnight)),
        (Some("   "), Some(midnight)),
        (
            Some("Before yesterday"),
            Some(yesterday - chrono::Duration::microseconds(1)),
        ),
    ] {
        sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,timestamp) VALUES($1,'note',$2,'fixture-only','PRIVATE OMITTED BODY',$3,$4)")
            .bind(graph).bind(Uuid::new_v4().to_string()).bind(title).bind(time).execute(&pool).await.unwrap();
    }
    let unavailable_clock:Uuid=sqlx::query_scalar("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,title,timestamp) VALUES($1,'note','unavailable-clock','fixture-only','PRIVATE UNAVAILABLE CLOCK BODY','Unknown event clock','infinity'::timestamptz) RETURNING atom_id").bind(graph).fetch_one(&pool).await.unwrap();
    let query = "recap what happened today and yesterday";
    let mut after: Option<String> = None;
    let mut seen = std::collections::HashSet::new();
    let mut retained_seen = 0;
    let mut first_cursor = None;
    let mut saw_long = false;
    let mut saw_unicode = false;
    let mut finished = false;
    for page in 0..8 {
        let (error, body) = call_tool(
            &app,
            &token,
            "search_atoms",
            json!({"query":query,"ocean_ids":[graph],"limit":40,"recap_after":after}),
        )
        .await;
        assert!(!error, "{body}");
        assert_eq!(body["untitled_legacy_count"], 2);
        assert_eq!(body["event_time_window"]["since"], json!(yesterday));
        assert!(body["legacy_evidence"].as_array().unwrap().is_empty());
        assert!(!body.to_string().contains("PRIVATE BODY"));
        assert!(!body.to_string().contains("Unknown event clock"));
        let headers = body["legacy_headlines"].as_array().unwrap();
        assert_eq!(body["results"].as_array().unwrap().len(), headers.len());
        let citations = body["retained_citations"].as_array().unwrap();
        assert!(headers.len() + citations.len() <= 40);
        if !citations.is_empty() {
            retained[0].assert_cited(&body);
            retained_seen += 1;
        }
        for header in headers {
            let id: Uuid = serde_json::from_value(header["atom_id"].clone()).unwrap();
            assert!(seen.insert(id), "duplicate on page {page}: {header}");
            assert_eq!(header["body_omitted"], true);
            assert_eq!(header["snippet"], header["title"]);
            assert!(header.get("content").is_none());
            assert!(header.get("input_sha256").is_none());
            assert!(header.get("passage_id").is_none());
            assert!(header["title"].as_str().unwrap().chars().count() <= 1024);
            saw_long |= header["title_truncated"] == true;
            saw_unicode |= header["title"] == "Unicode café 🪸";
        }
        after = body["next_recap_cursor"].as_str().map(str::to_owned);
        if first_cursor.is_none() {
            first_cursor = after.clone();
        }
        assert_eq!(body["truncated"], after.is_some());
        if after.is_none() {
            finished = true;
            break;
        }
    }
    assert!(finished, "recap did not exhaust");
    assert!(!seen.contains(&unavailable_clock));
    assert_eq!(seen, expected.into_iter().collect());
    assert_eq!(retained_seen, 1);
    assert!(saw_long && saw_unicode);
    let cursor = first_cursor.unwrap();
    for args in [
        json!({"query":"recap yesterday","ocean_ids":[graph],"limit":40,"recap_after":cursor}),
        json!({"query":query,"ocean_ids":[graph],"limit":39,"recap_after":cursor}),
        json!({"query":query,"library_scope":{"graph_ids":[graph],"source_item_id":retained[0].item},"limit":40,"recap_after":cursor}),
        json!({"query":"ordinary lexical query","ocean_ids":[graph],"limit":40,"recap_after":cursor}),
    ] {
        let (status, rpc) = mcp_rpc(
            &app,
            Some(&token),
            "tools/call",
            json!({"name":"search_atoms","arguments":args}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(rpc["result"]["isError"],true,"{rpc}");
        let body:Value=serde_json::from_str(rpc["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(error_code(&body),"invalid_params");
    }
    PostgresStore::new(pool.clone())
        .set_user_timezone(user, "Pacific/Auckland")
        .await
        .unwrap();
    let (status,rpc)=mcp_rpc(&app,Some(&token),"tools/call",json!({"name":"search_atoms","arguments":{"query":query,"ocean_ids":[graph],"limit":40,"recap_after":cursor}})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        rpc["result"]["isError"], true,
        "window change must reject the old cursor: {rpc}"
    );
    assert_eq!(model.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(embed.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    PostgresStore::new(pool.clone())
        .set_user_timezone(user, "America/Chicago")
        .await
        .unwrap();
    let idempotent=json!({"question":query,"ocean_ids":[graph],"top_k":40,"style":"sources-only","idempotency_key":"recap-window-probe"});
    let (error,first_window)=ask_ocean_and_wait(&app,&token,idempotent.clone()).await;
    assert!(!error,"{first_window}");
    PostgresStore::new(pool.clone()).set_user_timezone(user,"Pacific/Auckland").await.unwrap();
    let (error,second_window)=ask_ocean_and_wait(&app,&token,idempotent).await;
    assert!(!error,"{second_window}");
    assert_ne!(first_window["event_time_window"],second_window["event_time_window"]);
    assert_ne!(first_window["query_id"],second_window["query_id"],"idempotency must bind the resolved window");
    PostgresStore::new(pool.clone()).set_user_timezone(user,"America/Chicago").await.unwrap();
    let (error, mixed) = ask_ocean_and_wait(
        &app,
        &token,
        json!({"question":query,"ocean_ids":[graph],"top_k":40,"style":"concise"}),
    )
    .await;
    assert!(!error, "{mixed}");
    retained[0].assert_cited(&mixed);
    assert!(!mixed["legacy_headlines"].as_array().unwrap().is_empty());
    assert_eq!(model.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let prompt: Value =
        serde_json::from_str(model.prompts.lock().unwrap().last().unwrap()).unwrap();
    assert!(prompt.get("legacy_headlines").is_none());
    assert!(prompt["legacy_evidence"].as_array().unwrap().is_empty());
    assert_eq!(prompt["outcomes"][0]["citations"][0]["n"], 1);
    assert!(!prompt.to_string().contains("Unrelated headline"));
    cleanup(&pool, &[account], &[user]).await;
}

// ── Attributed reef digests: MCP `get_reef` and `GET /v1/nodes/{id}/digest` ──

async fn http_get(app: &axum::Router, token: &str, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

/// A reef with eight corals and a digest published under a billed
/// `managed_enrichment` receipt whose inputs bind every coral and the reef
/// title, the shape `workers::digest` writes. Returns (node, corals, receipt).
async fn seed_attributed_digest(pool: &sqlx::PgPool, account_id: Uuid, graph_id: Uuid, user_id: Uuid) -> (Uuid, Vec<Uuid>, Uuid) {
    let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Postgres') RETURNING node_id")
        .bind(graph_id).fetch_one(pool).await.unwrap();
    let mut corals = Vec::new();
    for i in 0..8 {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by) \
             VALUES($1,'note',$2,'fixture-only',$3,now(),$4) RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("digest-e2e-{i}-{node}"))
        .bind(format!("Postgres fact {i}: the tenant database runs on Postgres."))
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(id).execute(pool).await.unwrap();
        corals.push(id);
    }
    sqlx::query(
        "INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) VALUES($1,'managed_enrichment',current_date,2000000) \
         ON CONFLICT DO NOTHING",
    )
    .bind(account_id).execute(pool).await.unwrap();
    let receipt: Uuid = sqlx::query_scalar(
        "INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,attributed_user_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd,state,settled_micro_usd,served_model,settled_at,protocol_version,dispatch_phase,content_plane,request_hash) \
         VALUES($1,'managed_enrichment',current_date,$2,$3,gen_random_uuid(),1,ARRAY['deepseek/deepseek-v4-flash-0731'],900,800,2000,'billed',1500,'deepseek/deepseek-v4-flash-0731',now(),1,'armed','consumer',encode(sha256('digest-e2e'::bytea),'hex')) RETURNING receipt_id",
    )
    .bind(account_id).bind(graph_id).bind(user_id).fetch_one(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO managed_enrichment_inputs(receipt_id,account_id,graph_id,ordinal,input_kind,input_id,content_revision,content_sha256,outcome) \
         SELECT $1,$2,$3,(o-1)::smallint,'atom',a.atom_id,a.content_revision,sha256(convert_to(a.content_inline,'UTF8')),'written' \
           FROM unnest($4::uuid[]) WITH ORDINALITY u(id,o) JOIN atoms a ON a.atom_id=u.id",
    )
    .bind(receipt).bind(account_id).bind(graph_id).bind(&corals).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO managed_enrichment_inputs(receipt_id,account_id,graph_id,ordinal,input_kind,input_id,content_revision,content_sha256,outcome) \
         VALUES($1,$2,$3,8,'digest_node',$4,0,sha256(convert_to('Postgres','UTF8')),'written')",
    )
    .bind(receipt).bind(account_id).bind(graph_id).bind(node).execute(pool).await.unwrap();
    let digest = json!({
        "atoms_key": "", "digested_title": "Postgres",
        "summary": "Postgres runs the tenant databases.",
        "excerpts": [{"atom_id": corals[0], "excerpt": "Postgres fact 0", "contribution": "names the host"}],
        "managed_receipt": receipt,
    });
    sqlx::query("UPDATE nodes SET body=jsonb_set(body,'{digest}',$2) WHERE node_id=$1").bind(node).bind(digest).execute(pool).await.unwrap();
    (node, corals, receipt)
}

/// MCP `get_reef.digest` and the HTTP digest route serve ONE attributed
/// digest object through the real router, withhold it together when the
/// reader loses permission to a single coral or a coral goes stale, and a
/// stale miss queues a regeneration when digests are switched on.
#[tokio::test]
async fn get_reef_and_digest_route_serve_one_attributed_digest_and_withhold_it_together() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-pg-e2e-reef-digest.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("reef_digest");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = write_token(&sub);
    let (node, corals, receipt) = seed_attributed_digest(&pool, account_id, graph_id, user_id).await;
    let path = format!("/v1/nodes/{node}/digest");

    let (is_error, reef) = call_tool(&app, &token, "get_reef", json!({"reef_id": node})).await;
    assert!(!is_error, "{reef}");
    let digest = reef["digest"].clone();
    assert_eq!(digest["summary"], "Postgres runs the tenant databases.");
    assert_eq!(digest["attribution"]["receipt_id"], json!(receipt));
    assert_eq!(digest["attribution"]["model"], "deepseek/deepseek-v4-flash-0731");
    assert_eq!(digest["attribution"]["input_count"], 8);
    assert!(digest["attribution"]["generated_at"].is_string());
    let (status, mut http) = http_get(&app, &token, &path).await;
    assert_eq!(status, StatusCode::OK, "{http}");
    assert_eq!(http["cached"], true);
    http.as_object_mut().unwrap().remove("cached");
    assert_eq!(http, digest, "MCP and HTTP serve one digest object");

    // One coral's source is restricted: both surfaces withhold everything.
    sqlx::query(
        "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) \
         SELECT graph_id,source_type,'fixture:digest-e2e',source_id,'restricted' FROM atoms WHERE atom_id=$1",
    )
    .bind(corals[3]).execute(&pool).await.unwrap();
    let (is_error, reef) = call_tool(&app, &token, "get_reef", json!({"reef_id": node})).await;
    assert!(!is_error, "{reef}");
    assert!(reef["digest"].is_null(), "MCP withholds: {reef}");
    let (status, http) = http_get(&app, &token, &path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(http, json!({"summary": null, "excerpts": [], "unavailable": true, "regenerating": false}));
    assert!(!http.to_string().contains("Postgres runs"), "no stored text leaks");
    sqlx::query("UPDATE source_items SET access_state='permitted' WHERE provider_namespace='fixture:digest-e2e'").execute(&pool).await.unwrap();
    let (_, reef) = call_tool(&app, &token, "get_reef", json!({"reef_id": node})).await;
    assert_eq!(reef["digest"]["attribution"]["receipt_id"], json!(receipt));

    // A bound coral changes after generation: stale on both surfaces; with
    // digests switched on the miss queues one regeneration. (Clear the pass
    // the coral-count trigger minted when the eighth coral was linked.)
    sqlx::query("DELETE FROM jobs WHERE idempotency_key=$1").bind(format!("digest:{node}")).execute(&pool).await.unwrap();
    sqlx::query("UPDATE atoms SET content_inline='Postgres fact, edited after the digest.' WHERE atom_id=$1")
        .bind(corals[1]).execute(&pool).await.unwrap();
    // SAFETY: `_env` holds `ENV_LOCK` for this whole test.
    unsafe {
        std::env::set_var("ENRICHMENT_PROCESSING_ENABLED", "true");
        std::env::set_var("DIGESTS_ENABLED", "true");
    }
    let (_, reef) = call_tool(&app, &token, "get_reef", json!({"reef_id": node})).await;
    assert!(reef["digest"].is_null(), "{reef}");
    let (_, http) = http_get(&app, &token, &path).await;
    assert_eq!(http, json!({"summary": null, "excerpts": [], "unavailable": true, "regenerating": true}));
    unsafe {
        std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
        std::env::remove_var("DIGESTS_ENABLED");
    }
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='RegenerateDigest' AND status='pending' AND idempotency_key=$1")
        .bind(format!("digest:{node}")).fetch_one(&pool).await.unwrap();
    assert_eq!(pending, 1, "one deduped regeneration for both surfaces' misses");
    sqlx::query("DELETE FROM jobs WHERE idempotency_key=$1").bind(format!("digest:{node}")).execute(&pool).await.unwrap();
    cleanup(&pool, &[account_id], &[user_id]).await;
}
