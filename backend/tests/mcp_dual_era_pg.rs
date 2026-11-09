//! Behavior-pinning tests for the rmcp 3.1.2 dual-era Streamable HTTP
//! transport at `/mcp` (spec revision 2026-07-28; PR 1 of the rmcp
//! 2.2.0 -> 3.1.2 migration, `mod.rs`'s `mcp_router_with_validator`:
//! `legacy_session_mode: false`, `json_response: true`,
//! `stateless_protocol_metadata_required: false`,
//! `NeverSessionManager` with no `EventStore`).
//!
//! Pattern-copied from `mcp_pg_e2e.rs` (env/JWKS fixtures, `app_for_pool`,
//! `RELINFER_TEST_DATABASE_URL` gating) — duplicated rather than shared
//! because integration test files are separate compilation units; see that
//! file's own doc comment for why this crate's `mcp` module can't be named
//! directly from here.
//!
//! Transport behavior is pinned against rmcp 3.1.2 plus CoralSwarm's
//! header_backfill middleware (#922). The underlying SDK behavior comes from
//! (`~/.cargo/registry/src/*/rmcp-3.1.2/src/transport/
//! streamable_http_server/tower.rs`, `src/handler/server.rs`,
//! `src/model/meta.rs`, `src/model.rs`) BEFORE being pinned here — each
//! test's doc comment cites the exact mechanism, not a guess. No cleanup
//! step: every test uses a `fresh_sub` (a fresh Clerk `sub` per call, so
//! provisioning never collides across tests) and this suite is meant to run
//! against a disposable scratch database, never the shared dev DB.
//!
//! ## Running
//!
//!   RELINFER_TEST_DATABASE_URL=postgres://user@localhost:5432/scratch_db \
//!     cargo test -p coralswarm-backend --test mcp_dual_era_pg -- --nocapture

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{HeaderMap, Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

/// Serializes every test in this file against the process-global env vars
/// `McpEnv` sets — same rationale as `mcp_pg_e2e.rs`'s own `ENV_LOCK`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

fn fresh_sub(label: &str) -> String {
    format!("mcp_dual_era_{label}_{}", Uuid::new_v4())
}

/// Spin up a local JWKS listener serving the shared test keypair (duplicated
/// from `mcp_http.rs`/`mcp_pg_e2e.rs` for the same separate-compilation-unit
/// reason as `ENV_LOCK`).
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
    async fn enable(resource_url: &str) -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: every env mutation across this test binary is serialized by
        // `ENV_LOCK`, whose guard is held in `self._guard` for the lifetime
        // of this `McpEnv` (through `Drop` below).
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MCP_RESOURCE_URL", resource_url);
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
        }
        Self { _guard: guard }
    }
}

impl Drop for McpEnv {
    fn drop(&mut self) {
        // SAFETY: see `enable`'s SAFETY comment — `self._guard` still holds
        // `ENV_LOCK` here.
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MCP_RESOURCE_URL");
            std::env::remove_var("RATE_LIMIT_BACKEND");
        }
    }
}

/// Connect to the test Postgres, or skip exactly like `mcp_pg_e2e.rs` when
/// `RELINFER_TEST_DATABASE_URL` is unset.
macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the dual-era /mcp suite"
                );
                return;
            }
        }
    }};
}

/// Build the real public router backed by a REAL Postgres pool — identical
/// shape to `mcp_pg_e2e.rs`'s own `app_for_pool`.
fn app_for_pool(pool: sqlx::PgPool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// A plain read-scoped OAuth token for a fresh Clerk `sub` — every test here
/// calls read-only tools (`list_oceans`, `tools/list`), so the default
/// `effective_scopes` read grant (any authenticated OAuth token) is enough;
/// no write scope needed.
fn read_token(sub: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "mcp_dual_era_client",
    }))
}

/// Parse an SSE body back into the JSON-RPC message it carries (only reached
/// if a response ever comes back `text/event-stream`, which none of the
/// terminal error/success responses below do — `json_response: true` plus
/// every path exercised here returning a single terminal message means every
/// response in this file is plain `application/json`; kept for parity with
/// the other `/mcp` pg suites and as a safety net).
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

/// POST one JSON-RPC 2.0 request to `/mcp` with full control over extra
/// headers (`MCP-Protocol-Version` / `Mcp-Method` / `Mcp-Name`, on top of the
/// baseline every request needs), returning `(status, response_headers,
/// parsed_body)`. `mcp_pg_e2e.rs`'s own `mcp_rpc` has no seam for setting
/// those, since none of its tests need to.
async fn raw_post(
    app: &axum::Router,
    token: Option<&str>,
    extra_headers: &[(&str, &str)],
    body: Value,
) -> (StatusCode, HeaderMap, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        // Tower's `oneshot` drives the router directly with no real TCP
        // connection, so nothing synthesizes a `Host` header — rmcp's own
        // DNS-rebinding guard (`parse_host_header`) 400s without one.
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    for (name, value) in extra_headers {
        builder = builder.header(*name, *value);
    }
    let req = builder.body(Body::from(body.to_string())).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
    let parsed = if bytes.is_empty() {
        Value::Null
    } else if content_type.contains("text/event-stream") {
        parse_sse_json(&bytes)
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, headers, parsed)
}

/// GET or DELETE `/mcp` with an optional bearer token, returning
/// `(status, response_headers)` — no body to parse for either method.
async fn raw_method(
    app: &axum::Router,
    method: &str,
    token: Option<&str>,
) -> (StatusCode, HeaderMap) {
    let mut builder = Request::builder()
        .method(method)
        .uri("/mcp")
        .header("host", "localhost");
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    let req = builder.body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    (resp.status(), resp.headers().clone())
}

// ── A. Method pins on /mcp (authenticated) ──────────────────────────────────
//
// Unauthenticated GET -> 405 and DELETE -> 401 need no Postgres and live in
// `mcp_http.rs` instead. `mcp_auth` now answers every GET with 405 itself, so
// the GET pin below no longer reaches rmcp; DELETE needs a REAL identity to
// get past `mcp_auth` and reach rmcp's own method dispatch.

/// Verified against rmcp 3.1.2 source (`tower.rs::handle`): with
/// `legacy_session_mode: false` and a session manager with no `EventStore`
/// (our `NeverSessionManager::default()`, `supports_stateless_replay =
/// self.session_manager.event_store().is_some()` = `false`), the method
/// match arm is `Method::GET if self.config.legacy_session_mode ||
/// supports_stateless_replay` = `Method::GET if false` — GET never reaches
/// `handle_get` at all and falls straight to the `_` arm: `405 Method Not
/// Allowed` with `Allow: POST`. This differs from the handoff's informal
/// expectation of a bare 405 by being MORE specific (rmcp also names the
/// allowed method), but the status code matches and the behavior is
/// spec-conformant for a stateless server advertising no SSE stream.
#[tokio::test]
async fn authenticated_get_is_405_with_allow_post_no_sse_stream_in_stateless_mode() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-get.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("get_405"));

    let (status, headers) = raw_method(&app, "GET", Some(&token)).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        headers.get(axum::http::header::ALLOW).unwrap(),
        "POST",
        "rmcp's own Allow header must name the one method this stateless config accepts"
    );
}

/// Same source path (`tower.rs::handle`): `Method::DELETE if
/// self.config.legacy_session_mode` = `Method::DELETE if false` — DELETE
/// never reaches `handle_delete` (which would otherwise require a
/// `Mcp-Session-Id` to close) and falls to the same `405` / `Allow: POST`
/// arm. There is no session for a stateless server to delete, so 405 (not
/// 400/404) is the spec-conformant response.
#[tokio::test]
async fn authenticated_delete_is_405_with_allow_post_no_session_to_delete_in_stateless_mode() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-delete.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("delete_405"));

    let (status, headers) = raw_method(&app, "DELETE", Some(&token)).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(axum::http::header::ALLOW).unwrap(), "POST");
}

// ── B. Modern-era (2026-07-28) request handling ─────────────────────────────

/// Verified against `transport/streamable_http_server/tower.rs`'s
/// `validate_request_protocol_version_meta`, called unconditionally in
/// `handle_post`'s stateless branch: when a request's own `_meta` declares
/// `io.modelcontextprotocol/protocolVersion` AND the `MCP-Protocol-Version`
/// HTTP header is also present but names a DIFFERENT version, the server
/// returns `header_mismatch_jsonrpc_response` — HTTP 400, JSON-RPC error
/// code `-32020` (`ErrorCode::HEADER_MISMATCH`, `model.rs`). Matches the
/// handoff's expectation exactly.
#[tokio::test]
async fn header_mismatch_between_protocol_version_header_and_request_meta_is_400_header_mismatch() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-mismatch.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("header_mismatch"));

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28"
            }
        }
    });
    // Header names an older, still-well-known version — deliberately
    // different from the body's own declared version.
    let (status, _headers, rpc) = raw_post(
        &app,
        Some(&token),
        &[("MCP-Protocol-Version", "2025-11-25")],
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rpc["error"]["code"].as_i64(), Some(-32020));
    let message = rpc["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("does not match"),
        "unexpected HeaderMismatch message: {message}"
    );
}

/// Verified against `handler/server.rs`'s blanket `impl<H: ServerHandler>
/// Service<RoleServer> for H`: for a non-`initialize` request whose `_meta`
/// declares an inline `protocolVersion` NOT in
/// `self.supported_protocol_versions()` (default: `ProtocolVersion::
/// KNOWN_VERSIONS`, unoverridden by `CoralSwarmMcp`), the handler returns
/// `McpError::unsupported_protocol_version(requested, &supported)` BEFORE
/// any tool dispatch. `model.rs::jsonrpc_http_status` maps
/// `ErrorCode::UNSUPPORTED_PROTOCOL_VERSION` (-32022) to HTTP 400, and
/// `ErrorData::unsupported_protocol_version` embeds `{"requested":
/// ..., "supported": [...]}` in `error.data` — the "supported list" the
/// handoff expects. Uses a version string lexically BELOW
/// `ProtocolVersion::STANDARD_HEADERS` ("2026-07-28") so the SEP-2243
/// `Mcp-Method`/`Mcp-Name` gate (tested separately below) never engages —
/// isolates this one behavior.
#[tokio::test]
async fn unsupported_protocol_version_in_request_meta_is_400_with_supported_list() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-unsupported.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("unsupported_version"));

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2020-01-01"
            }
        }
    });
    let (status, _headers, rpc) = raw_post(
        &app,
        Some(&token),
        &[("MCP-Protocol-Version", "2020-01-01")],
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rpc["error"]["code"].as_i64(), Some(-32022));
    assert_eq!(rpc["error"]["data"]["requested"], "2020-01-01");
    // `ProtocolVersion::KNOWN_VERSIONS`, in its declared order (model.rs) —
    // a future rmcp bump that adds a new known version SHOULD change this
    // literal, and that diff is exactly the signal this test exists to give.
    assert_eq!(
        rpc["error"]["data"]["supported"],
        json!([
            "2024-11-05",
            "2025-03-26",
            "2025-06-18",
            "2025-11-25",
            "2026-07-28"
        ])
    );
}

/// #922 backfills an absent `Mcp-Method` from the JSON-RPC body before
/// rmcp validates standard headers. A modern request with complete inline
/// metadata must therefore reach `tools/list` successfully without it.
#[tokio::test]
async fn modern_era_request_missing_mcp_method_header_succeeds() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-missing-method.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("missing_mcp_method"));

    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/list",
        "params": {"_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {}
        }}
    });
    let (status, _headers, rpc) = raw_post(
        &app,
        Some(&token),
        &[("MCP-Protocol-Version", "2026-07-28")],
        body,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected rpc body: {rpc:?}");
    assert!(rpc.get("error").is_none(), "unexpected rpc error: {rpc:?}");
    assert!(
        rpc["result"]["tools"]
            .as_array()
            .is_some_and(|tools| !tools.is_empty()),
        "tools/list must return the caller's tools: {rpc:?}"
    );
}

/// Header backfill must not bypass SEP-2575 inline negotiation. Both
/// tools/list (absent method header) and tools/call (absent name header)
/// still reject missing `_meta` with rmcp's -32602 invalid-params error.
#[tokio::test]
async fn modern_era_backfilled_headers_still_require_inline_metadata() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-missing-meta.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("missing_inline_metadata"));

    for (method, params, headers) in [
        (
            "tools/list",
            json!({}),
            vec![("MCP-Protocol-Version", "2026-07-28")],
        ),
        (
            "tools/call",
            json!({"name": "list_oceans", "arguments": {}}),
            vec![
                ("MCP-Protocol-Version", "2026-07-28"),
                ("Mcp-Method", "tools/call"),
            ],
        ),
    ] {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let (status, _headers, rpc) = raw_post(&app, Some(&token), &headers, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{method}: {rpc:?}");
        assert_eq!(
            rpc["error"]["code"].as_i64(),
            Some(-32602),
            "{method}: {rpc:?}"
        );
        let message = rpc["error"]["message"].as_str().unwrap_or_default();
        assert!(message.contains("request _meta"), "{method}: {rpc:?}");
    }
}

/// Same gate, the other direction: `Mcp-Method` present but naming a
/// DIFFERENT method than the JSON-RPC body (`mcp_headers.rs`:
/// `Some(value) if value != method`) — also `-32020` / 400.
#[tokio::test]
async fn modern_era_request_with_mismatched_mcp_method_header_is_400_header_mismatch() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-mismatched-method.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("mismatched_mcp_method"));

    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}});
    let (status, _headers, rpc) = raw_post(
        &app,
        Some(&token),
        &[
            ("MCP-Protocol-Version", "2026-07-28"),
            ("Mcp-Method", "tools/call"),
        ],
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rpc["error"]["code"].as_i64(), Some(-32020));
    let message = rpc["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("does not match body method"),
        "unexpected message: {message}"
    );
}

/// #922 backfills an absent `Mcp-Name` from `params.name` for tools/call.
/// A supplied conflicting name must still be rejected, never rewritten.
#[tokio::test]
async fn modern_era_tools_call_backfills_missing_mcp_name_but_rejects_mismatch() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-missing-name.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("missing_mcp_name"));

    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "list_oceans", "arguments": {},
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }
    });
    let mut headers = vec![
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
    ];
    let (status, _headers, rpc) = raw_post(&app, Some(&token), &headers, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "unexpected rpc body: {rpc:?}");
    assert!(rpc.get("error").is_none(), "unexpected rpc error: {rpc:?}");
    assert_eq!(
        rpc["result"]["isError"].as_bool().unwrap_or(false),
        false,
        "list_oceans must succeed: {rpc:?}"
    );

    headers.push(("Mcp-Name", "wrong_tool"));
    let (status, _headers, rpc) = raw_post(&app, Some(&token), &headers, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rpc["error"]["code"].as_i64(), Some(-32020));
    let message = rpc["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("Mcp-Name"),
        "unexpected message: {message}"
    );
}

/// The positive case: a FULLY modern-era client — `MCP-Protocol-Version` >=
/// 2026-07-28 on the wire, matching `Mcp-Method`/`Mcp-Name` (SEP-2243), AND
/// the request's own `_meta` carrying BOTH
/// `io.modelcontextprotocol/protocolVersion` and
/// `io.modelcontextprotocol/clientCapabilities` — is served normally.
///
/// The `_meta` pair is NOT optional once the header alone says
/// `>= 2026-07-28`: an earlier version of this test sent only the SEP-2243
/// headers with a legacy-shaped (no `_meta`) body and got a 400 back —
/// `tower.rs::validate_request_protocol_version_meta`'s `None` branch
/// requires self-contained `_meta` (SEP-2575 inline negotiation) the moment
/// `header_version >= ProtocolVersion::V_2026_07_28`, independent of the
/// SEP-2243 header gate. That is a genuine, useful pin in its own right —
/// the standard HTTP headers and the inline-negotiation `_meta` are two
/// SEPARATE modern-era contracts a real 2026-07-28 client must satisfy
/// together, not alternatives — so this test now exercises the actual
/// full-compliance shape and both are covered.
#[tokio::test]
async fn modern_era_tools_call_with_correct_standard_headers_succeeds() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-correct-headers.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("correct_standard_headers"));

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "list_oceans",
            "arguments": {},
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }
    });
    let (status, _headers, rpc) = raw_post(
        &app,
        Some(&token),
        &[
            ("MCP-Protocol-Version", "2026-07-28"),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "list_oceans"),
        ],
        body,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected rpc body: {rpc:?}");
    assert!(
        rpc.get("error").is_none(),
        "well-formed modern-era request must not be rejected: {rpc:?}"
    );
    let result = &rpc["result"];
    assert_eq!(
        result["isError"].as_bool().unwrap_or(false),
        false,
        "list_oceans must succeed for a freshly-provisioned caller: {result:?}"
    );
}

/// The dual-era guarantee (SEP-2567 + this deployment's `legacy_session_mode:
/// false`): a LEGACY-era `initialize` (protocol version older than
/// 2026-07-28, no inline `_meta`) is answered directly with NO
/// `Mcp-Session-Id` response header — proven by `tower.rs::handle_post`'s
/// stateless branch, which for a non-per-request-version `InitializeRequest`
/// dispatches through the same session-less `OneshotTransport` +
/// `serve_directly_with_ct` path every other stateless request uses, never
/// touching `self.session_manager` at all (matches `mod.rs`'s own doc
/// comment on `NeverSessionManager`'s methods "simply never being called").
/// A SECOND, unrelated call with no `Mcp-Session-Id` header at all then
/// succeeds too — this is what keeps today's Claude clients (which never
/// speak the 2026-07-28 per-request-metadata dialect) working unmodified
/// against a stateless server.
#[tokio::test]
async fn legacy_era_initialize_is_served_sessionless_and_no_session_id_is_ever_required() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-dual-era-legacy-init.example/mcp").await;
    let app = app_for_pool(pool);
    let token = read_token(&fresh_sub("legacy_sessionless"));

    let init_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "dual-era-pin-test", "version": "1.0.0"}
        }
    });
    let (status, headers, rpc) = raw_post(&app, Some(&token), &[], init_body).await;
    assert_eq!(status, StatusCode::OK, "initialize failed: {rpc:?}");
    assert!(
        rpc.get("result")
            .and_then(|r| r.get("protocolVersion"))
            .is_some(),
        "expected an InitializeResult: {rpc:?}"
    );
    assert!(
        headers.get("mcp-session-id").is_none(),
        "a legacy_session_mode:false deployment must never mint a session id, \
         even for a legacy-era initialize — statelessness is the whole point"
    );

    // A completely independent follow-up call, carrying no session id and no
    // reference to the initialize above, must still succeed.
    let list_body = json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}});
    let (status2, headers2, rpc2) = raw_post(&app, Some(&token), &[], list_body).await;
    assert_eq!(
        status2,
        StatusCode::OK,
        "sessionless tools/list failed: {rpc2:?}"
    );
    assert!(
        rpc2.get("result").and_then(|r| r.get("tools")).is_some(),
        "expected a ListToolsResult: {rpc2:?}"
    );
    assert!(headers2.get("mcp-session-id").is_none());
}
