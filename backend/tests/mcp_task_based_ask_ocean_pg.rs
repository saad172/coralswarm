//! Behavior-pinning tests for task-based `ask_ocean` (MCP spec 2026-07-28
//! adoption, PR 2 — see PR 1's `mcp_dual_era_pg.rs` for the dual-era
//! transport this builds on).
//!
//! Pattern-copied from `mcp_dual_era_pg.rs`/`mcp_pg_e2e.rs` (env/JWKS
//! fixtures, `app_for_pool`, `RELINFER_TEST_DATABASE_URL` gating,
//! provisioning via a real `list_oceans` call) — duplicated rather than
//! shared because integration test files are separate compilation units;
//! see those files' own doc comments for why this crate's `mcp` module
//! can't be named directly from here.
//!
//! Covers the locked spec's five explicitly-named behaviors:
//!   1. **Legacy poll flow** — `ask_ocean` (no declared tasks capability)
//!      returns an ordinary tool result carrying a `task_id`; `get_task_result`
//!      polls it to `completed`.
//!   2. **Modern task handles** — `ask_ocean` (tasks capability declared,
//!      SEP-2575 inline `_meta`) returns a real `CreateTaskResult`
//!      (`resultType: "task"`); `tasks/get` polls it to `completed`.
//!   3. **Era divergence** — the identical `ask_ocean` call shape produces
//!      the two different wire shapes above depending solely on whether the
//!      caller declared the tasks capability.
//!   4. **RLS/ownership scoping** — a task created by one caller is
//!      unreachable (`not_found` / unknown-task) via either poll path when a
//!      DIFFERENT caller (different Clerk `sub`, different account) presents
//!      the same task id.
//!   5. **TTL** — `get_task_result` treats an expired-but-unswept row as
//!      `not_found` (the READ-TIME half of the 24h TTL contract; the
//!      periodic sweep itself is covered by `workers::mcp_task_sweep`'s own
//!      in-module `RELINFER_TEST_DATABASE_URL`-gated test, since `workers` is
//!      a crate-private module this external file cannot call into).
//!
//! Plus two adversarial-review follow-ups (GitHub issue #593, items 1-2 —
//! items 3-6 are out of scope for this file):
//!   6. **`get_task_result`'s dedicated/exempt rate bucket** — a poll storm
//!      well past the shared `READ_USER_MAX` (60/min) must never see
//!      `rate_limited`, and must not silently drain the shared bucket every
//!      OTHER read tool depends on either.
//!   7. **`ask_ocean` in-flight concurrency backpressure** — a burst of
//!      concurrent `ask_ocean` calls past the per-user cap
//!      (`MCP_ASK_MAX_IN_FLIGHT`, pinned to 3 by [`McpEnv`] for determinism)
//!      must see at least one clean `rate_limited` backpressure rejection;
//!      once every admitted task drains, a fresh ask is admitted again; a
//!      second user's own calls are never affected by the first user's count.
//!
//! ## Running
//!
//!   RELINFER_TEST_DATABASE_URL=postgres://user@localhost:5432/scratch_db \
//!     cargo test -p coralswarm-backend --test mcp_task_based_ask_ocean_pg -- --nocapture

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{HeaderMap, Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Mutex, Notify};
use tower::ServiceExt;
use uuid::Uuid;

/// Serializes every test in this file against the process-global env vars
/// `McpEnv` sets — same rationale as `mcp_dual_era_pg.rs`'s own `ENV_LOCK`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

fn fresh_sub(label: &str) -> String {
    format!("mcp_task_ask_ocean_{label}_{}", Uuid::new_v4())
}

/// Spin up a local JWKS listener serving the shared test keypair (duplicated
/// from `mcp_dual_era_pg.rs`/`mcp_pg_e2e.rs` for the same separate-
/// compilation-unit reason as `ENV_LOCK`).
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

/// Pinned (issue #593 item 2), not left to the process default: the
/// in-flight-cap tests below need a KNOWN small ceiling to burst past
/// deterministically, independent of whatever `AskInFlight`'s own default
/// happens to be today. Also the single source `get_task_result`'s own
/// derived poll ceiling below is computed FROM (follow-up review, F1 — the
/// derivation itself lives in `mcp::server::get_task_result_poll_max`,
/// crate-private and thus unreachable from this external test file, so the
/// same `cap × 60 + 60` arithmetic is duplicated here deliberately, the same
/// "independently declared, not shared" precedent as this crate's own
/// `SESSION_ID_FILTER_CAP` doc comment).
const PINNED_ASK_MAX_IN_FLIGHT: u32 = 3;

struct McpEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl McpEnv {
    /// Pins `MCP_ASK_SYNC_WAIT_MS=0` (MCP listing-prep item 5's synchronous
    /// wait, skipped entirely) — every test in this file predates that
    /// feature and asserts on the immediate `{status: "working", task_id,
    /// ...}` shape `legacy_call_tool(..., "ask_ocean", ...)` returns before
    /// this env var existed. The dedicated sync-wait tests near the bottom
    /// of this file (`ask_ocean_sync_wait_*`) use
    /// [`Self::enable_with_sync_wait_ms`] instead to opt a real budget back
    /// in.
    async fn enable(resource_url: &str) -> Self {
        Self::enable_with_sync_wait_ms(resource_url, 0).await
    }

    async fn enable_with_sync_wait_ms(resource_url: &str, sync_wait_ms: u64) -> Self {
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
            std::env::set_var("MCP_ASK_MAX_IN_FLIGHT", PINNED_ASK_MAX_IN_FLIGHT.to_string());
            std::env::set_var("MCP_ASK_SYNC_WAIT_MS", sync_wait_ms.to_string());
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
            std::env::remove_var("MCP_ASK_MAX_IN_FLIGHT");
            std::env::remove_var("MCP_ASK_SYNC_WAIT_MS");
        }
    }
}

/// Connect to the test Postgres, or skip exactly like the other `_pg.rs`
/// suites when `RELINFER_TEST_DATABASE_URL` is unset.
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
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the task-based ask_ocean suite"
                );
                return;
            }
        }
    }};
}

/// Build the real public router backed by a REAL Postgres pool — identical
/// shape to `mcp_dual_era_pg.rs`'s own `app_for_pool`.
fn app_for_pool(pool: sqlx::PgPool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// Same as [`app_for_pool`], with a caller-chosen `state.llm` — needed for
/// [`ask_ocean_sync_wait_falls_back_when_the_task_is_still_working_at_the_deadline`],
/// which needs `run_ask`'s SYNTHESIS branch (not the LLM-free `no_answer`/
/// `sources` degrade every other test in this file relies on) to actually
/// run long enough to observe. `test_app_state()`'s default `llm: None`
/// never calls out at all (`run_ask`'s own doc: "mode:'sources' when
/// state.llm is None") — ingestion (`add_context`) never touches this
/// field either way, so building the WHOLE test around one llm-equipped
/// state from the start (rather than swapping it in only for the `ask_ocean`
/// call) is safe.
fn app_for_pool_with_llm(
    pool: sqlx::PgPool,
    llm: Arc<dyn coralswarm_backend::llm::LlmProvider + Send + Sync>,
) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    state.llm = Some(llm);
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// LLM stub whose `stream_complete` sleeps before ever producing anything —
/// used ONLY by the sync-wait "still working at the deadline" test to make
/// `run_ask`'s synthesis branch genuinely still be in flight after a small
/// (sub-second) wait budget elapses, without racing real timing against
/// whatever a no-LLM-call degrade happens to take on a given CI runner.
/// Precedent: `routes::queries::pg_tests::SlowLlm` (same shape, same
/// rationale, different call site — that one proves a timeout releases a
/// semaphore permit promptly; this one proves the sync-wait loop falls back
/// to the task_id response promptly instead of waiting out a slow LLM).
struct SlowLlm {
    delay: std::time::Duration,
}

#[derive(Default)]
struct HeldFailureLlm {
    entered: Notify,
    resume: Notify,
}

const PRIVATE_FAILURE: &str = "private model failure sentinel";

#[async_trait::async_trait]
impl coralswarm_backend::llm::LlmProvider for HeldFailureLlm {
    async fn complete(
        &self,
        _req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<coralswarm_backend::llm::CompletionResponse, coralswarm_backend::llm::LlmError> {
        self.entered.notify_one();
        self.resume.notified().await;
        Err(coralswarm_backend::llm::LlmError::Parse(PRIVATE_FAILURE.into()))
    }

    async fn stream_complete(
        &self,
        _req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<coralswarm_backend::llm::StreamEvent, coralswarm_backend::llm::LlmError>> + Send>>,
        coralswarm_backend::llm::LlmError,
    > {
        panic!("retained Ask must use the non-streaming completion path")
    }
}

async fn seed_ask_evidence(pool: &sqlx::PgPool, graph_id: Uuid) {
    let content = "The Q3 budget review approved a marketing spend increase.";
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
         \"timestamp\", content_inline, content_tsv) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3, to_tsvector('simple', $3))",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(content)
    .execute(pool)
    .await
    .expect("seed retrievable legacy evidence");
}

#[async_trait::async_trait]
impl coralswarm_backend::llm::LlmProvider for SlowLlm {
    async fn complete(
        &self,
        _req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<coralswarm_backend::llm::CompletionResponse, coralswarm_backend::llm::LlmError> {
        tokio::time::sleep(self.delay).await;
        Ok(coralswarm_backend::llm::CompletionResponse {
            content: r#"{"answer": "slow answer", "citations": []}"#.to_string(),
            input_tokens: 10,
            output_tokens: 5,
            model: "mock-slow".to_string(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: None,
            cost_usd: None,
        })
    }

    async fn stream_complete(
        &self,
        _req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<coralswarm_backend::llm::StreamEvent, coralswarm_backend::llm::LlmError>>
                    + Send,
            >,
        >,
        coralswarm_backend::llm::LlmError,
    > {
        // `synthesize()` routes through the streaming path (production
        // regression 2026-08 — escapes `complete()`'s own total deadline),
        // so the delay has to live here, before the stream is even handed
        // back, not inside a delta the caller might not `.next()` for a
        // while anyway.
        tokio::time::sleep(self.delay).await;
        let events: Vec<Result<coralswarm_backend::llm::StreamEvent, coralswarm_backend::llm::LlmError>> = vec![
            Ok(coralswarm_backend::llm::StreamEvent::Delta(
                coralswarm_backend::llm::StreamDelta {
                    text: r#"{"answer": "slow answer", "citations": []}"#.to_string(),
                },
            )),
            Ok(coralswarm_backend::llm::StreamEvent::Final(
                coralswarm_backend::llm::StreamFinalEvent {
                    input_tokens: 10,
                    output_tokens: 5,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                    stop_reason: None,
                    model: None,
                    cost_usd: None,
                },
            )),
        ];
        Ok(Box::pin(futures::stream::iter(events)))
    }
}

/// A plain read-scoped OAuth token for a fresh Clerk `sub` — `ask_ocean` and
/// `get_task_result` are both read tools, so the default `effective_scopes`
/// read grant (any authenticated OAuth token) is enough.
fn read_token(sub: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "mcp_task_ask_ocean_client",
    }))
}

fn parse_sse_json(bytes: &[u8]) -> Value {
    let text = String::from_utf8_lossy(bytes);
    let mut last_rpc = None;
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("data:") else {
            continue;
        };
        let payload = rest.strip_prefix(' ').unwrap_or(rest).trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        // Progress notifications (`notifications/message`) have no `id`.
        // Keep the last JSON-RPC response so packing-phase SSE events
        // emitted before the result do not join into invalid JSON.
        if v.get("id").is_some() {
            last_rpc = Some(v);
        }
    }
    last_rpc.unwrap_or_else(|| {
        let snippet: String = text.chars().take(4096).collect();
        panic!(
            "failed to parse SSE JSON-RPC payload\nraw body ({} chars, showing ≤4096):\n{snippet}",
            text.chars().count()
        )
    })
}

/// POST one JSON-RPC 2.0 request to `/mcp` with full control over extra
/// headers (SEP-2243 `Mcp-Method`/`Mcp-Name`), returning `(status,
/// response_headers, parsed_body)` — identical shape to `mcp_dual_era_pg.rs`'s
/// own `raw_post`.
async fn raw_post(
    app: &axum::Router,
    token: Option<&str>,
    extra_headers: &[(&str, &str)],
    body: Value,
) -> (StatusCode, HeaderMap, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
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

/// A plain LEGACY-shaped `tools/call` — no SEP-2243 headers, no inline
/// `_meta` — the shape every Claude client sends today. Returns the parsed
/// `result` object (panics if the call produced a JSON-RPC protocol error,
/// which none of these tests expect).
async fn legacy_call_tool(app: &axum::Router, token: &str, name: &str, arguments: Value) -> Value {
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": name, "arguments": arguments }
    });
    let (status, _headers, rpc) = raw_post(app, Some(token), &[], body).await;
    assert_eq!(status, StatusCode::OK, "{name} failed: {rpc:?}");
    assert!(rpc.get("error").is_none(), "{name} returned a protocol error: {rpc:?}");
    rpc["result"].clone()
}

/// Parse a legacy `tools/call` result's `structuredContent`/text-JSON body
/// (the tool's own `{"error":...}` or success shape), asserting the result
/// itself is not a tool-level error unless `allow_error` is set.
fn tool_json(result: &Value) -> Value {
    let text = result["content"][0]["text"]
        .as_str()
        .expect("tool result has text content");
    serde_json::from_str(text).expect("tool result text is JSON")
}

/// The `_meta` object a MODERN-era (2026-07-28) request must carry inline on
/// every request once its `MCP-Protocol-Version` header says so (SEP-2575) —
/// `clientCapabilities.extensions` additionally declares the SEP-2663 tasks
/// extension, which is what `ask_ocean` (era divergence) and `tasks/*`
/// (`validate_tasks_capability`) both key off.
fn modern_meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {
            "extensions": { "io.modelcontextprotocol/tasks": {} }
        }
    })
}

const MODERN_HEADER: (&str, &str) = ("MCP-Protocol-Version", "2026-07-28");

/// A MODERN-era `tools/call`, with the SEP-2243 standard headers AND the
/// SEP-2575 inline `_meta` pair. Returns the raw `(status, rpc)` — callers
/// assert on the shape themselves (a `CreateTaskResult`, not an ordinary tool
/// result).
async fn modern_call_tool(app: &axum::Router, token: &str, name: &str, arguments: Value) -> (StatusCode, Value) {
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": name, "arguments": arguments, "_meta": modern_meta() }
    });
    let (status, _headers, rpc) = raw_post(
        app,
        Some(token),
        &[MODERN_HEADER, ("Mcp-Method", "tools/call"), ("Mcp-Name", name)],
        body,
    )
    .await;
    (status, rpc)
}

/// A MODERN-era `tasks/get` poll.
async fn modern_get_task(app: &axum::Router, token: &str, task_id: &str) -> (StatusCode, Value) {
    let body = json!({
        "jsonrpc": "2.0", "id": 2, "method": "tasks/get",
        "params": { "taskId": task_id, "_meta": modern_meta() }
    });
    let (status, _headers, rpc) = raw_post(
        app,
        Some(token),
        &[MODERN_HEADER, ("Mcp-Method", "tasks/get"), ("Mcp-Name", task_id)],
        body,
    )
    .await;
    (status, rpc)
}

/// Provision via a plain legacy `list_oceans` call (first-sight Clerk
/// provisioning), then read back `(user_id, account_id, graph_id)` — mirrors
/// `mcp_pg_e2e.rs`'s own `provision`.
async fn provision(app: &axum::Router, pool: &sqlx::PgPool, sub: &str) -> (Uuid, Uuid, Uuid) {
    let token = read_token(sub);
    let _ = legacy_call_tool(app, &token, "list_oceans", json!({})).await;

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
    (user_id, account_id, graph_id)
}

/// Poll `get_task_result` (legacy) until terminal or `max_attempts` elapses,
/// sleeping `delay` between attempts. Returns the final parsed tool JSON.
async fn poll_legacy_task_result(
    app: &axum::Router,
    token: &str,
    task_id: &str,
    max_attempts: u32,
    delay: std::time::Duration,
) -> Value {
    for attempt in 0..max_attempts {
        let result = legacy_call_tool(app, token, "get_task_result", json!({"task_id": task_id})).await;
        let body = tool_json(&result);
        if body.get("status").and_then(Value::as_str) != Some("working") {
            return body;
        }
        if attempt + 1 < max_attempts {
            tokio::time::sleep(delay).await;
        }
    }
    panic!("get_task_result never reached a terminal state for task {task_id} within {max_attempts} attempts");
}

/// Poll `tasks/get` (modern era) until terminal or `max_attempts` elapses.
/// Returns the final `result` object (`GetTaskResult`, SEP-2663).
async fn poll_modern_task(
    app: &axum::Router,
    token: &str,
    task_id: &str,
    max_attempts: u32,
    delay: std::time::Duration,
) -> Value {
    for attempt in 0..max_attempts {
        let (status, rpc) = modern_get_task(app, token, task_id).await;
        assert_eq!(status, StatusCode::OK, "tasks/get failed: {rpc:?}");
        let result = &rpc["result"];
        let task_status = result["status"].as_str().unwrap_or_default();
        if task_status == "completed" || task_status == "failed" || task_status == "cancelled" {
            return result.clone();
        }
        if attempt + 1 < max_attempts {
            tokio::time::sleep(delay).await;
        }
    }
    panic!("tasks/get never reached a terminal state for task {task_id} within {max_attempts} attempts");
}

const POLL_ATTEMPTS: u32 = 40;
const POLL_DELAY: std::time::Duration = std::time::Duration::from_millis(150);

// ── 1. Legacy poll flow ──────────────────────────────────────────────────────

#[tokio::test]
async fn legacy_ask_ocean_returns_a_task_id_pollable_via_get_task_result() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-legacy.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("legacy_poll");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    let result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": "what happened this week?", "ocean_ids": [graph_id]}),
    )
    .await;
    assert_ne!(result["isError"].as_bool(), Some(true), "ask_ocean itself must not tool-error: {result:?}");
    let body = tool_json(&result);
    assert_eq!(body["status"], "working", "a freshly-created task must start working: {body:?}");
    assert_eq!(body["poll_with"], "get_task_result");
    let task_id = body["task_id"].as_str().expect("task_id is a string").to_string();

    let completed = poll_legacy_task_result(&app, &token, &task_id, POLL_ATTEMPTS, POLL_DELAY).await;
    assert_eq!(completed["status"], "completed", "expected a completed delivery: {completed:?}");
    assert_eq!(completed["task_id"], task_id);
    assert_eq!(completed["result"]["mode"], "retrieval_only");
    assert_eq!(completed["result"]["library_scope"]["graph_ids"], json!([graph_id]));
    assert_eq!(completed["result"]["support_status"], "complete");
    assert!(completed["result"]["citations"].is_array());
}

// ── 2. Modern task handles ───────────────────────────────────────────────────

#[tokio::test]
async fn modern_era_ask_ocean_returns_a_protocol_task_handle_pollable_via_tasks_get() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-modern.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("modern_poll");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    let (status, rpc) = modern_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": "what happened this week?", "ocean_ids": [graph_id]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "modern-era ask_ocean failed: {rpc:?}");
    assert!(rpc.get("error").is_none(), "unexpected protocol error: {rpc:?}");
    let result = &rpc["result"];
    assert_eq!(result["resultType"], "task", "modern-era ask_ocean must return a task handle: {result:?}");
    assert_eq!(result["status"], "working");
    let task_id = result["taskId"].as_str().expect("taskId is a string").to_string();

    let completed = poll_modern_task(&app, &token, &task_id, POLL_ATTEMPTS, POLL_DELAY).await;
    assert_eq!(completed["status"], "completed", "task must settle completed: {completed:?}");
    let inner = tool_json(&completed["result"]);
    assert!(inner.get("mode").is_some(), "completed task result must carry ask_ocean's mode: {inner:?}");
}

// ── 3. Era divergence on the identical call shape ───────────────────────────

#[tokio::test]
async fn era_divergence_same_ask_ocean_question_diverges_on_declared_tasks_capability() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-era-divergence.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("era_divergence");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);
    let args = json!({"question": "what happened this week?", "ocean_ids": [graph_id]});

    let legacy_result = legacy_call_tool(&app, &token, "ask_ocean", args.clone()).await;
    let legacy_body = tool_json(&legacy_result);
    assert!(
        legacy_body.get("task_id").is_some() && legacy_body.get("resultType").is_none(),
        "legacy-era caller must see an ordinary tool result carrying task_id, not a protocol task: {legacy_body:?}"
    );

    let (status, rpc) = modern_call_tool(&app, &token, "ask_ocean", args).await;
    assert_eq!(status, StatusCode::OK, "modern-era ask_ocean failed: {rpc:?}");
    let modern_result = &rpc["result"];
    assert_eq!(
        modern_result["resultType"], "task",
        "modern-era caller (same question, same ocean) must see a real protocol task handle: {modern_result:?}"
    );
}

// ── 4. Ownership / RLS scoping ───────────────────────────────────────────────

#[tokio::test]
async fn get_task_result_refuses_a_different_callers_task() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-ownership-legacy.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub_a = fresh_sub("owner_a");
    let sub_b = fresh_sub("owner_b");
    let (_user_a, _account_a, graph_a) = provision(&app, &pool, &sub_a).await;
    let (_user_b, _account_b, _graph_b) = provision(&app, &pool, &sub_b).await;
    let token_a = read_token(&sub_a);
    let token_b = read_token(&sub_b);

    let result = legacy_call_tool(
        &app,
        &token_a,
        "ask_ocean",
        json!({"question": "owner-only question", "ocean_ids": [graph_a]}),
    )
    .await;
    let task_id = tool_json(&result)["task_id"].as_str().unwrap().to_string();

    // The SAME task id, presented by a DIFFERENT caller's own account/token.
    let result_b = legacy_call_tool(&app, &token_b, "get_task_result", json!({"task_id": task_id})).await;
    assert_eq!(result_b["isError"], Value::Bool(true), "cross-caller poll must fail: {result_b:?}");
    let body_b = tool_json(&result_b);
    assert_eq!(body_b["error"]["code"], "not_found");

    // Sanity: the OWNING caller can still poll it (proves the denial above is
    // ownership-specific, not a general query bug).
    let own_poll = legacy_call_tool(&app, &token_a, "get_task_result", json!({"task_id": task_id})).await;
    assert_ne!(own_poll["isError"].as_bool(), Some(true), "the owning caller must still be able to poll: {own_poll:?}");
}

#[tokio::test]
async fn tasks_get_refuses_a_different_callers_task() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-ownership-modern.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub_a = fresh_sub("owner_a_modern");
    let sub_b = fresh_sub("owner_b_modern");
    let (_user_a, _account_a, graph_a) = provision(&app, &pool, &sub_a).await;
    let (_user_b, _account_b, _graph_b) = provision(&app, &pool, &sub_b).await;
    let token_a = read_token(&sub_a);
    let token_b = read_token(&sub_b);

    let (status, rpc) = modern_call_tool(
        &app,
        &token_a,
        "ask_ocean",
        json!({"question": "owner-only question", "ocean_ids": [graph_a]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "modern-era ask_ocean failed: {rpc:?}");
    let task_id = rpc["result"]["taskId"].as_str().unwrap().to_string();

    // A DIFFERENT caller (also tasks-capable) presenting the SAME task id.
    // `get_task` returning `Err(McpError)` is a JSON-RPC PROTOCOL error, not
    // an ordinary tool result — rmcp's `jsonrpc_http_status` maps
    // `ErrorCode::INVALID_PARAMS` to HTTP 400 (same convention
    // `mcp_dual_era_pg.rs` pins for `-32020`/`-32022`).
    let (status_b, rpc_b) = modern_get_task(&app, &token_b, &task_id).await;
    assert_eq!(status_b, StatusCode::BAD_REQUEST, "cross-caller tasks/get must be a protocol error: {rpc_b:?}");
    assert!(rpc_b.get("error").is_some(), "cross-caller tasks/get must fail: {rpc_b:?}");
    assert_eq!(rpc_b["error"]["code"].as_i64(), Some(-32602), "unknown/not-yours task must be -32602 invalid_params: {rpc_b:?}");

    // Sanity: the owning caller can still poll it.
    let (status_a, rpc_a) = modern_get_task(&app, &token_a, &task_id).await;
    assert_eq!(status_a, StatusCode::OK);
    assert!(rpc_a.get("error").is_none(), "the owning caller must still be able to poll: {rpc_a:?}");
}

// ── 5. TTL (read-time expiry) ────────────────────────────────────────────────

#[tokio::test]
async fn get_task_result_treats_an_expired_task_as_not_found() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-ttl.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("ttl");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    let result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": "will this expire?", "ocean_ids": [graph_id]}),
    )
    .await;
    let task_id: Uuid = tool_json(&result)["task_id"].as_str().unwrap().parse().unwrap();

    // Backdate `expires_at` directly — proves the READ-TIME contract
    // (`get_task_result`'s own `expires_at > now()` predicate), independent
    // of whether the periodic sweep has physically reaped the row yet.
    sqlx::query("UPDATE mcp_tasks SET expires_at = now() - interval '1 hour' WHERE id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("backdate expires_at");

    let result = legacy_call_tool(&app, &token, "get_task_result", json!({"task_id": task_id})).await;
    assert_eq!(result["isError"], Value::Bool(true), "expired task must be refused: {result:?}");
    let body = tool_json(&result);
    assert_eq!(body["error"]["code"], "not_found");
}

// ── 6. get_task_result's dedicated/exempt rate bucket (issue #593 item 1) ────
//
// `ask_ocean`'s own tool description tells a legacy-era caller to poll
// `get_task_result` every 1-2s until a task settles. Before this fix,
// `get_task_result` counted against the SAME shared `mcp:u:r:{user}` 60/min
// bucket as every other read tool — a task polled at 1s could exhaust that
// bucket in exactly 60s and come back `rate_limited` on the poll itself,
// stranding an already-finished answer AND starving every other read tool
// for the rest of the window. Exercises the REAL `PgRateLimiter` path
// (`McpEnv` pins `RATE_LIMIT_BACKEND=pg`), unlike `mcp::server`'s own
// in-process unit tests (`test_app_state()`'s `InMemoryRateLimiter`).

/// `get_task_result`'s poll ceiling is DERIVED from the (pinned) in-flight
/// cap (follow-up review, F1: `ask_max_in_flight × 60 + 60`, floor 120) —
/// see [`PINNED_ASK_MAX_IN_FLIGHT`]'s doc comment for why the formula is
/// duplicated here rather than called.
const GET_TASK_RESULT_USER_MAX_FOR_TESTS: u32 = PINNED_ASK_MAX_IN_FLIGHT * 60 + 60;
const READ_USER_MAX_FOR_TESTS: u32 = 60;

#[tokio::test]
async fn get_task_result_poll_storm_does_not_rate_limit_while_other_reads_still_enforce_their_bucket() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-poll-storm.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("poll_storm");
    let (_user_id, _account_id, _graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    // Comfortably more than READ_USER_MAX (60) — proves get_task_result's
    // OWN (larger) ceiling, not the shared one, is what gates it. The task
    // id doesn't need to resolve to anything real; only the rate-limit
    // outcome is under test here.
    for i in 0..GET_TASK_RESULT_USER_MAX_FOR_TESTS {
        let result =
            legacy_call_tool(&app, &token, "get_task_result", json!({"task_id": Uuid::new_v4()})).await;
        let body = tool_json(&result);
        assert_ne!(
            body["error"]["code"], "rate_limited",
            "get_task_result poll {i} (of {GET_TASK_RESULT_USER_MAX_FOR_TESTS} allowed) should not \
             be rate-limited yet: {body:?}"
        );
    }

    // list_oceans must still have its ENTIRE READ_USER_MAX budget — none of
    // it was silently spent by the get_task_result storm above. `provision`
    // already spent 1 of that budget on its own `list_oceans` call, so this
    // loop runs READ_USER_MAX - 1 times (the budget's true remaining size)
    // rather than tripping the ceiling on an off-by-one of our own making.
    for i in 0..(READ_USER_MAX_FOR_TESTS - 1) {
        let result = legacy_call_tool(&app, &token, "list_oceans", json!({})).await;
        let body = tool_json(&result);
        assert_ne!(
            body["error"]["code"], "rate_limited",
            "list_oceans call {i} (of {READ_USER_MAX_FOR_TESTS} allowed, minus 1 already spent by \
             provision) must not be rate-limited by a prior get_task_result poll storm: {body:?}"
        );
    }
}

// ── 7. ask_ocean in-flight concurrency backpressure (issue #593 item 2) ──────
//
// Now that `ask_ocean` returns immediately, the old 10/min rate bucket no
// longer bounds CONCURRENCY — a caller could hold many syntheses open at
// once, each pinning a pooled DB connection and billed LLM spend.
// `MCP_ASK_MAX_IN_FLIGHT` is pinned to `PINNED_ASK_MAX_IN_FLIGHT` by
// `McpEnv::enable` for these tests.

const IN_FLIGHT_CAP_FOR_TESTS: u32 = PINNED_ASK_MAX_IN_FLIGHT;

#[tokio::test]
async fn ask_ocean_in_flight_cap_rejects_a_burst_past_the_cap_then_admits_again_after_drain() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-in-flight-cap.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("in_flight_cap");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    // Fire a burst comfortably larger than the pinned cap (3) essentially
    // simultaneously, so at least one call is virtually certain to observe
    // the cap already reached and be rejected — even though completion (no
    // corals ingested into this fresh ocean, so run_ask's honesty guard
    // settles `no_answer` without ever calling an LLM) races against
    // admission for the others.
    const BURST: usize = 8;
    let mut handles = Vec::with_capacity(BURST);
    for i in 0..BURST {
        let app = app.clone();
        let token = token.clone();
        let args = json!({"question": format!("in-flight burst question {i}"), "ocean_ids": [graph_id]});
        handles.push(tokio::spawn(
            async move { legacy_call_tool(&app, &token, "ask_ocean", args).await },
        ));
    }

    let mut admitted_task_ids = Vec::new();
    let mut rejected = 0usize;
    for h in handles {
        let result = h.await.expect("ask_ocean burst call task panicked");
        let body = tool_json(&result);
        if result["isError"].as_bool() == Some(true) {
            assert_eq!(body["error"]["code"], "rate_limited", "unexpected error shape: {body:?}");
            let message = body["error"]["message"].as_str().unwrap_or_default();
            assert!(
                message.contains("in flight"),
                "backpressure error must explain WHY and point at polling existing tasks: {body:?}"
            );
            rejected += 1;
        } else {
            assert_eq!(body["status"], "working", "an admitted call must carry the usual working shape: {body:?}");
            admitted_task_ids.push(body["task_id"].as_str().expect("task_id is a string").to_string());
        }
    }

    assert!(
        rejected >= 1,
        "with {BURST} concurrent asks against a cap of {IN_FLIGHT_CAP_FOR_TESTS}, at least one must \
         be rejected as backpressure (0 rejections means the cap did nothing)"
    );

    // Every ADMITTED task must still complete normally — the cap rejects
    // cleanly, it doesn't wedge the ones that DID get in.
    for task_id in &admitted_task_ids {
        let completed = poll_legacy_task_result(&app, &token, task_id, POLL_ATTEMPTS, POLL_DELAY).await;
        assert_eq!(completed["status"], "completed", "every admitted task must complete: {completed:?}");
        assert_eq!(completed["result"]["support_status"], "complete");
    }

    // Now that every admitted task has drained (permits released on
    // completion), a fresh ask must be admitted again — the cap is not a
    // one-shot trip.
    let result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": "post-drain question", "ocean_ids": [graph_id]}),
    )
    .await;
    assert_ne!(
        result["isError"].as_bool(), Some(true),
        "a fresh ask after every prior admitted task drained must be admitted again: {result:?}"
    );
    assert_eq!(tool_json(&result)["status"], "working");
}

#[tokio::test]
async fn ask_ocean_in_flight_cap_is_per_user_a_second_users_call_is_unaffected() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-in-flight-isolation.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub_a = fresh_sub("in_flight_a");
    let sub_b = fresh_sub("in_flight_b");
    let (_user_a, _account_a, graph_a) = provision(&app, &pool, &sub_a).await;
    let (_user_b, _account_b, graph_b) = provision(&app, &pool, &sub_b).await;
    let token_a = read_token(&sub_a);
    let token_b = read_token(&sub_b);

    // Burst user A well past the pinned cap (3) — same technique as above.
    // This test does not care about A's individual outcomes, only that B is
    // never affected by them.
    const BURST: usize = 8;
    let mut handles = Vec::with_capacity(BURST);
    for i in 0..BURST {
        let app = app.clone();
        let token_a = token_a.clone();
        let args = json!({"question": format!("A burst {i}"), "ocean_ids": [graph_a]});
        handles.push(tokio::spawn(
            async move { legacy_call_tool(&app, &token_a, "ask_ocean", args).await },
        ));
    }

    // User B's own call, issued while A's burst is in flight, must never be
    // rejected by A's exhausted counter — a completely separate per-user
    // bucket, deterministic regardless of A's timing.
    let result_b = legacy_call_tool(
        &app,
        &token_b,
        "ask_ocean",
        json!({"question": "B's own question", "ocean_ids": [graph_b]}),
    )
    .await;
    assert_ne!(
        result_b["isError"].as_bool(), Some(true),
        "user B must be unaffected by user A's in-flight count: {result_b:?}"
    );
    assert_eq!(tool_json(&result_b)["status"], "working");

    for h in handles {
        let _ = h.await;
    }
}

// ── F4 (follow-up review): the in-flight-cap rejection must not misdirect a
// modern-era (tasks-capable) caller to get_task_result — that tool refuses a
// protocol task id with `not_found` (it only ever reads the LEGACY-era
// `mcp_tasks` table); a modern-era caller's in-flight tasks were created as
// protocol task handles and must be polled with `tasks/get` instead. The
// review confirmed a modern-era `ask_ocean` call CAN hit this same
// in-flight-cap rejection path (it runs before any era branch), so the
// message must say so correctly for whichever era actually got rejected.

#[tokio::test]
async fn ask_ocean_in_flight_cap_rejection_points_a_modern_era_caller_at_tasks_get_not_get_task_result()
{
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-in-flight-modern-era.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("in_flight_modern");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    // Same burst-past-the-cap technique as the legacy-era test above, but
    // every call in this burst declares the tasks capability.
    const BURST: usize = 8;
    let mut handles = Vec::with_capacity(BURST);
    for i in 0..BURST {
        let app = app.clone();
        let token = token.clone();
        let args = json!({"question": format!("modern-era burst {i}"), "ocean_ids": [graph_id]});
        handles.push(tokio::spawn(
            async move { modern_call_tool(&app, &token, "ask_ocean", args).await },
        ));
    }

    let mut saw_rejection = false;
    for h in handles {
        let (status, rpc) = h.await.expect("modern-era ask_ocean burst call task panicked");
        assert_eq!(status, StatusCode::OK, "unexpected protocol error: {rpc:?}");
        let result = &rpc["result"];
        // Admitted calls get a real protocol task handle (`resultType:
        // "task"`) — a REJECTION never reaches that branch at all, so it is
        // always an ordinary (non-task) tool-error result, exactly the same
        // shape `legacy_call_tool`'s callers parse with `tool_json`.
        if result.get("resultType").and_then(Value::as_str) == Some("task") {
            continue;
        }
        let body = tool_json(result);
        if body["error"]["code"] == "rate_limited" {
            let message = body["error"]["message"].as_str().unwrap_or_default();
            assert!(
                message.contains("tasks/get"),
                "a modern-era caller's backpressure rejection must point at tasks/get: {body:?}"
            );
            assert!(
                !message.contains("get_task_result"),
                "a modern-era caller's backpressure rejection must NOT tell it to poll \
                 get_task_result (that tool refuses a protocol task id with not_found): {body:?}"
            );
            saw_rejection = true;
        }
    }

    assert!(
        saw_rejection,
        "expected at least one modern-era backpressure rejection in this burst of {BURST} against a \
         cap of {IN_FLIGHT_CAP_FOR_TESTS}"
    );
}

#[tokio::test]
async fn ask_ocean_ask_error_settles_the_legacy_task_failed_with_error_and_staleness_fields() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-ask-error-failed.example/mcp").await;
    let llm = Arc::new(HeldFailureLlm::default());
    let app = app_for_pool_with_llm(pool.clone(), llm.clone());
    let sub_a = fresh_sub("ask_error_failed_a");
    let sub_b = fresh_sub("ask_error_failed_b");
    let (user_a, _account_a, graph_a) = provision(&app, &pool, &sub_a).await;
    let (_user_b, _account_b, graph_b) = provision(&app, &pool, &sub_b).await;
    let token_a = read_token(&sub_a);

    let denied = legacy_call_tool(
        &app,
        &token_a,
        "ask_ocean",
        json!({"question": "can I read someone else's ocean?", "ocean_ids": [graph_b]}),
    )
    .await;
    assert_eq!(denied["isError"], true);
    let denial = json!({"error":{"code":"not_found","message":"scope not found or access denied"}});
    assert_eq!(tool_json(&denied), denial);
    assert_eq!(denied["structuredContent"], denial);
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM mcp_tasks WHERE owner_user_id=$1")
        .bind(user_a).fetch_one(&pool).await.unwrap();
    assert_eq!(tasks, 0, "denied admission must not create a durable task");

    seed_ask_evidence(&pool, graph_a).await;
    let admitted = legacy_call_tool(
        &app,
        &token_a,
        "ask_ocean",
        json!({"question": "What happened with the Q3 budget review?", "ocean_ids": [graph_a]}),
    )
    .await;
    assert_ne!(admitted["isError"].as_bool(), Some(true), "{admitted:?}");
    let body = tool_json(&admitted);
    assert_eq!(body["status"], "working");
    let task_id = body["task_id"].as_str().unwrap().to_string();
    tokio::time::timeout(std::time::Duration::from_secs(5), llm.entered.notified())
        .await.expect("permitted task must reach controlled completion");
    llm.resume.notify_one();

    let terminal = poll_legacy_task_result(&app, &token_a, &task_id, POLL_ATTEMPTS, POLL_DELAY).await;
    assert_eq!(terminal["status"], "failed", "controlled post-admission failure must settle: {terminal:?}");
    assert_eq!(terminal["error"], json!({"code":"unavailable","message":"Scoped answer unavailable"}));
    assert!(terminal["result"].is_null());
    assert!(!terminal.to_string().contains(PRIVATE_FAILURE));
    assert!(terminal["created_at"].as_str().is_some());
    let age = terminal["age_seconds"].as_i64().expect("age_seconds is a number");
    assert!((0..300).contains(&age));
    let (status, result, error): (String, Option<Value>, Option<Value>) = sqlx::query_as(
        "SELECT status,result,error FROM mcp_tasks WHERE id=$1 AND owner_user_id=$2",
    ).bind(Uuid::parse_str(&task_id).unwrap()).bind(user_a).fetch_one(&pool).await.unwrap();
    assert_eq!(status, "failed");
    assert!(result.is_none());
    assert_eq!(error, Some(json!({"code":"unavailable","message":"Scoped answer unavailable"})));

    let again = legacy_call_tool(&app, &token_a, "get_task_result", json!({"task_id": task_id})).await;
    let again_body = tool_json(&again);
    assert_eq!(again_body["status"], "failed");
    assert_eq!(again_body["error"], terminal["error"]);
    assert_eq!(again_body["created_at"], terminal["created_at"]);
}

#[tokio::test]
async fn get_task_result_working_response_carries_created_at_and_age_seconds() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-working-staleness.example/mcp").await;
    let llm = Arc::new(HeldFailureLlm::default());
    let app = app_for_pool_with_llm(pool.clone(), llm.clone());
    let sub = fresh_sub("working_staleness");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);
    seed_ask_evidence(&pool, graph_id).await;
    let admitted = legacy_call_tool(
        &app, &token, "ask_ocean",
        json!({"question":"What happened with the Q3 budget review?","ocean_ids":[graph_id]}),
    ).await;
    assert_ne!(admitted["isError"].as_bool(), Some(true), "{admitted:?}");
    let task_id = tool_json(&admitted)["task_id"].as_str().unwrap().to_string();
    tokio::time::timeout(std::time::Duration::from_secs(5), llm.entered.notified())
        .await.expect("actual admitted task must enter the controlled model");

    let result = legacy_call_tool(&app, &token, "get_task_result", json!({"task_id": task_id})).await;
    let body = tool_json(&result);
    llm.resume.notify_one();
    assert_eq!(body["status"], "working", "held model must remain working: {body:?}");
    assert!(body["created_at"].as_str().is_some());
    let age = body["age_seconds"].as_i64().expect("age_seconds is a number");
    assert!((0..60).contains(&age));
    assert!(body.get("error").is_none());
    assert!(body["result"].is_null());
    let terminal = poll_legacy_task_result(&app, &token, &task_id, POLL_ATTEMPTS, POLL_DELAY).await;
    assert_eq!(terminal["status"], "failed");
    assert_eq!(terminal["created_at"], body["created_at"]);
}

// ── MCP listing-prep item 5: `ask_ocean`'s synchronous wait for a ──────────
// non-tasks-capable client (every test above pins `MCP_ASK_SYNC_WAIT_MS=0`
// via `McpEnv::enable`'s default — these two are the ones that opt a real
// budget back in via `McpEnv::enable_with_sync_wait_ms`).

/// Answer-ready branch: with a wait budget comfortably longer than this
/// fixture's synthesis takes (no corals ingested, so `run_ask`'s honesty
/// guard settles `no_answer` without ever calling an LLM — the same fast
/// path the pre-existing tests above rely on), the legacy-era caller gets
/// the full answer INLINE from the `ask_ocean` call itself — no `task_id`,
/// no `status: "working"`, no separate `get_task_result` round trip needed.
#[tokio::test]
async fn ask_ocean_sync_wait_returns_the_answer_inline_when_it_finishes_in_time() {
    let pool = test_pool!();
    let _env = McpEnv::enable_with_sync_wait_ms("https://mcp-task-sync-wait-ready.example/mcp", 5_000).await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("sync_wait_ready");
    let (user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    let result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": "what happened this week?", "ocean_ids": [graph_id]}),
    )
    .await;
    assert_ne!(result["isError"].as_bool(), Some(true), "ask_ocean itself must not tool-error: {result:?}");
    let body = tool_json(&result);
    assert!(
        body.get("task_id").is_none() && body.get("status").is_none(),
        "an answer that finished inside the wait budget must come back as the answer itself, \
         not the task_id/status envelope: {body:?}"
    );
    assert!(body.get("mode").is_some(), "inline answer must carry ask_ocean's mode: {body:?}");
    assert!(body.get("citations").is_some(), "inline answer must carry citations: {body:?}");
    assert_eq!(body["library_scope"]["graph_ids"], json!([graph_id]));
    assert_eq!(body["support_status"], "complete");
    assert_eq!(body["mode"], "retrieval_only");

    let (db_status, db_result): (String, Value) = sqlx::query_as(
        "SELECT status, result FROM mcp_tasks WHERE owner_user_id = $1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(user_id)
    .fetch_one(&pool)
    .await
    .expect("the background task must have left a row behind even though we never polled for it");
    assert_eq!(db_status, "completed");
    assert_eq!(
        db_result, body,
        "the inline response must equal the persisted retained result payload"
    );
}

/// Deadline-hit branch: with the wait budget pinned to zero, `ask_ocean`
/// never even attempts to wait on its own background task — it falls back to
/// the ordinary `task_id` response immediately, now carrying an explanatory
/// `note`. This is also, structurally, the boundary case of the same
/// mechanism the answer-ready test above exercises (a wait budget so short
/// no completion can ever land inside it), so it doubles as this feature's
/// "the wait can time out" regression test without depending on real timing.
#[tokio::test]
async fn ask_ocean_sync_wait_falls_back_to_the_task_id_response_when_the_deadline_passes_first() {
    let pool = test_pool!();
    let _env =
        McpEnv::enable_with_sync_wait_ms("https://mcp-task-sync-wait-deadline.example/mcp", 0).await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("sync_wait_deadline");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    let result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": "what happened this week?", "ocean_ids": [graph_id]}),
    )
    .await;
    assert_ne!(result["isError"].as_bool(), Some(true), "ask_ocean itself must not tool-error: {result:?}");
    let body = tool_json(&result);
    assert_eq!(body["status"], "working", "a zero-budget deadline must fall back immediately: {body:?}");
    assert_eq!(body["poll_with"], "get_task_result");
    assert!(
        body["note"].as_str().is_some_and(|n| n.contains("get_task_result")),
        "the fallback response must explain that the answer wasn't ready and point at \
         get_task_result: {body:?}"
    );
    let task_id = body["task_id"].as_str().expect("task_id is a string").to_string();

    // The task itself is real and still resolves normally — the deadline
    // falling back to `task_id` must never mean the underlying ask was lost
    // or abandoned, only that this particular caller has to poll for it.
    let completed = poll_legacy_task_result(&app, &token, &task_id, POLL_ATTEMPTS, POLL_DELAY).await;
    assert_eq!(completed["status"], "completed", "every admitted task must complete: {completed:?}");
    assert_eq!(completed["result"]["support_status"], "complete");
    assert_eq!(completed["result"]["mode"], "retrieval_only");
}

/// Security review follow-up item 8: the deadline-hit fallback must fire
/// correctly against a task that is GENUINELY still `working` when the
/// budget elapses — not just the budget-0 boundary case
/// (`ask_ocean_sync_wait_falls_back_to_the_task_id_response_when_the_deadline_passes_first`
/// above, which never even enters the wait loop). Uses [`SlowLlm`] to make
/// `run_ask`'s synthesis branch take several real seconds, comfortably
/// longer than a small (600ms) wait budget, so the fallback is observed
/// against real elapsed time rather than a zero-length race.
#[tokio::test]
async fn ask_ocean_sync_wait_falls_back_when_the_task_is_still_working_at_the_deadline() {
    let pool = test_pool!();
    let _env =
        McpEnv::enable_with_sync_wait_ms("https://mcp-task-sync-wait-slow.example/mcp", 600).await;
    let app = app_for_pool_with_llm(
        pool.clone(),
        Arc::new(SlowLlm { delay: std::time::Duration::from_secs(3) }),
    );
    let sub = fresh_sub("sync_wait_slow");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    // Seed a reef + coral DIRECTLY (bypassing the MCP `add_context` tool
    // entirely) — the same pattern `member_ocean_probe_pg.rs`'s
    // `seed_reef_and_coral` uses, and for the same reason: `add_context`'s
    // embedding/entity-extraction pipeline runs asynchronously, so a coral
    // ingested through it is not reliably retrievable on the very next
    // call, which would make this test's timing flaky rather than a clean
    // proof of the sync-wait mechanism. `content_tsv` must be populated by
    // hand here since production only stamps it during real ingest —
    // without it, keyword search finds zero rows for this fixture.
    let reef_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph_id)
    .bind("Q3 budget review")
    .fetch_one(&pool)
    .await
    .expect("seed reef");
    let coral_content = "The Q3 budget review happened on Tuesday; the team approved a marketing spend increase.";
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             \"timestamp\", content_inline, content_tsv) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3, \
                 to_tsvector('simple', $3)) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(coral_content)
    .fetch_one(&pool)
    .await
    .expect("seed coral");
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(reef_id)
        .bind(atom_id)
        .execute(&pool)
        .await
        .expect("link reef to coral");

    let start = std::time::Instant::now();
    let result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": "What happened with the Q3 budget review?", "ocean_ids": [graph_id]}),
    )
    .await;
    let elapsed = start.elapsed();
    assert_ne!(result["isError"].as_bool(), Some(true), "ask_ocean itself must not tool-error: {result:?}");
    let body = tool_json(&result);

    assert_eq!(
        body["status"], "working",
        "the LLM sleeps 3s and the budget is 600ms — this must fall back to the task_id \
         envelope, never wait out the slow synthesis: {body:?}"
    );
    assert_eq!(body["poll_with"], "get_task_result");
    assert!(
        body["note"].as_str().is_some_and(|n| n.contains("get_task_result")),
        "fallback response must explain and point at get_task_result: {body:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "the call itself must return promptly at the (600ms) budget, not after the full 3s \
         synthesis: took {elapsed:?}"
    );
    let task_id = body["task_id"].as_str().expect("task_id is a string").to_string();

    // The background synthesis is not abandoned — it keeps running and the
    // task eventually goes terminal (whatever the outcome; this stub's
    // response shape isn't the point), provable by polling well past the
    // stub's 3s delay.
    for _ in 0..40 {
        let result = legacy_call_tool(&app, &token, "get_task_result", json!({"task_id": task_id})).await;
        let poll_body = tool_json(&result);
        if poll_body.get("status").and_then(Value::as_str) != Some("working") {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
    panic!("task {task_id} never reached a terminal state after the slow LLM should have finished");
}

/// Security review follow-up item 5: a retried `ask_ocean` call carrying the
/// SAME `idempotency_key` while the first is still `working` must reuse
/// that task — not spawn (and separately bill) a second `run_ask`. Uses
/// [`SlowLlm`] again so the first call is still genuinely in flight when the
/// retry lands.
#[tokio::test]
async fn ask_ocean_idempotency_key_reuses_an_in_flight_task_instead_of_spawning_a_duplicate() {
    let pool = test_pool!();
    let _env = McpEnv::enable_with_sync_wait_ms(
        "https://mcp-task-idempotency.example/mcp",
        600,
    )
    .await;
    let app = app_for_pool_with_llm(
        pool.clone(),
        Arc::new(SlowLlm { delay: std::time::Duration::from_secs(3) }),
    );
    let sub = fresh_sub("idempotency");
    let (user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    // Seed a reef + coral directly — same rationale and pattern as
    // `ask_ocean_sync_wait_falls_back_when_the_task_is_still_working_at_the_deadline`.
    let reef_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph_id)
    .bind("Q3 budget review")
    .fetch_one(&pool)
    .await
    .expect("seed reef");
    let coral_content = "The Q3 budget review happened on Tuesday; the team approved a marketing spend increase.";
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             \"timestamp\", content_inline, content_tsv) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3, \
                 to_tsvector('simple', $3)) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(coral_content)
    .fetch_one(&pool)
    .await
    .expect("seed coral");
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(reef_id)
        .bind(atom_id)
        .execute(&pool)
        .await
        .expect("link reef to coral");

    let idempotency_key = format!("idem-{}", Uuid::new_v4());
    let question = "What happened with the Q3 budget review?";

    let first_result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": question, "ocean_ids": [graph_id], "idempotency_key": idempotency_key}),
    )
    .await;
    assert_ne!(first_result["isError"].as_bool(), Some(true), "first call must not tool-error: {first_result:?}");
    let first_body = tool_json(&first_result);
    assert_eq!(first_body["status"], "working", "the slow LLM must still be running: {first_body:?}");
    let first_task_id = first_body["task_id"].as_str().expect("task_id is a string").to_string();

    // Retry with the SAME key while the first is still in flight. CodeRabbit
    // re-review #8: this must fall into the SAME bounded sync-wait a freshly
    // spawned task uses (this fixture's 600ms budget is comfortably shorter
    // than the SlowLlm's 3s delay, so it times out and falls back to the
    // ordinary task_id/working envelope) rather than returning an immediate
    // "reusing" response that bypasses the wait entirely — the whole point
    // of reusing the task is so a legacy client that can't reliably poll
    // still gets a chance at an inline answer.
    let second_result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": question, "ocean_ids": [graph_id], "idempotency_key": idempotency_key}),
    )
    .await;
    assert_ne!(second_result["isError"].as_bool(), Some(true), "retry must not tool-error: {second_result:?}");
    let second_body = tool_json(&second_result);
    assert_eq!(second_body["status"], "working", "the slow LLM must still be running: {second_body:?}");
    assert_eq!(
        second_body["task_id"].as_str(),
        Some(first_task_id.as_str()),
        "the retry must reuse the FIRST call's task_id, not mint a new one: {second_body:?}"
    );
    assert!(
        second_body["note"].as_str().is_some_and(|n| n.contains("wait budget")),
        "the retry falls into the same sync-wait as a fresh task and, once ITS budget elapses \
         too, gets the ordinary wait-budget fallback note, not a bespoke 'reusing' message: \
         {second_body:?}"
    );

    // The proof that matters: exactly ONE row exists for this key — the
    // retry never inserted (and so never spawned `run_ask` for) a second
    // one.
    let row_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM mcp_tasks WHERE owner_user_id = $1 AND idempotency_key = $2",
    )
    .bind(user_id)
    .bind(&idempotency_key)
    .fetch_one(&pool)
    .await
    .expect("count mcp_tasks rows for this idempotency_key");
    assert_eq!(
        row_count, 1,
        "exactly one mcp_tasks row must exist for this idempotency_key — a second row would mean \
         a duplicate (separately billed) synthesis was spawned"
    );

    // And a DIFFERENT idempotency_key on an otherwise-identical call must
    // NOT be deduplicated against the first — the key is what scopes reuse,
    // not the question text.
    let third_result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": question, "ocean_ids": [graph_id], "idempotency_key": format!("idem-{}", Uuid::new_v4())}),
    )
    .await;
    assert_ne!(third_result["isError"].as_bool(), Some(true), "{third_result:?}");
    let third_body = tool_json(&third_result);
    assert_ne!(
        third_body["task_id"].as_str(),
        Some(first_task_id.as_str()),
        "a DIFFERENT idempotency_key must start its own task, never reuse another key's: {third_body:?}"
    );
}

/// Second security review, M3: the SAME `idempotency_key` string used
/// against two DIFFERENT oceans of the same account must NOT be treated as
/// "the same call" — each ocean's question is a genuinely different
/// request, so each must get its own independent task, never an error and
/// never one silently answering for the other. This is exactly what
/// `claim_ask_task`'s conflict target (which includes the request-content
/// hash, not just the key) is for: a different ocean id changes the hash,
/// so the two claims never collide at the DB level at all.
#[tokio::test]
async fn ask_ocean_same_idempotency_key_across_two_oceans_yields_two_independent_tasks() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-idempotency-two-oceans.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("idempotency_two_oceans");
    let (user_id, account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    let graph_id_2 = PostgresStore::new(pool.clone())
        .create_graph(account_id, "Second ocean", "org")
        .await.expect("create a reachable account ocean").id;

    let idempotency_key = format!("idem-two-oceans-{}", Uuid::new_v4());
    let question = "what happened this week?";

    let first_result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": question, "ocean_ids": [graph_id], "idempotency_key": idempotency_key}),
    )
    .await;
    assert_ne!(first_result["isError"].as_bool(), Some(true), "{first_result:?}");
    let first_task_id = tool_json(&first_result)["task_id"].as_str().unwrap().to_string();

    let second_result = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": question, "ocean_ids": [graph_id_2], "idempotency_key": idempotency_key}),
    )
    .await;
    assert_ne!(second_result["isError"].as_bool(), Some(true), "{second_result:?}");
    let second_task_id = tool_json(&second_result)["task_id"].as_str().unwrap().to_string();

    assert_ne!(
        first_task_id, second_task_id,
        "the SAME idempotency_key against two DIFFERENT oceans must produce two independent \
         tasks, never a collision/reuse or an error"
    );

    let row_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM mcp_tasks WHERE owner_user_id = $1 AND idempotency_key = $2",
    )
    .bind(user_id)
    .bind(&idempotency_key)
    .fetch_one(&pool)
    .await
    .expect("count mcp_tasks rows for this idempotency_key");
    assert_eq!(row_count, 2, "exactly two independent mcp_tasks rows must exist: {row_count}");
}

/// CodeRabbit re-review, MAJOR #7: two `ask_ocean` calls carrying the SAME
/// `idempotency_key`, fired essentially simultaneously (not sequentially
/// awaited), must claim the SAME row — exactly one `mcp_tasks` row and one
/// `llm_usage` row, never two billed syntheses for one logical call. Before
/// this fix, the lookup and the insert ran in separate transactions against
/// a NON-unique index, so both concurrent calls could miss the lookup and
/// both insert. Also exercises CodeRabbit #8's deferred-permit ordering as
/// a side effect: `CONCURRENT` is intentionally larger than the pinned
/// in-flight cap (3) — since only the ONE call that wins the atomic claim
/// ever acquires a permit, none of the other calls (which all find
/// `Existing`) can be rejected by the cap, however many of them there are.
#[tokio::test]
async fn ask_ocean_concurrent_same_key_calls_claim_exactly_one_task_and_bill_exactly_once() {
    let pool = test_pool!();
    let _env = McpEnv::enable_with_sync_wait_ms(
        "https://mcp-task-idempotency-race.example/mcp",
        0,
    )
    .await;
    let app = app_for_pool_with_llm(
        pool.clone(),
        Arc::new(SlowLlm { delay: std::time::Duration::from_millis(500) }),
    );
    let sub = fresh_sub("idempotency_race");
    let (user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    // Seed a reef + coral directly so `run_ask` actually reaches synthesis
    // (and therefore writes an `llm_usage` row) instead of degrading to
    // `no_answer` before ever calling the LLM — same pattern as the
    // sync-wait/idempotency tests above.
    let reef_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph_id)
    .bind("Q3 budget review")
    .fetch_one(&pool)
    .await
    .expect("seed reef");
    let coral_content = "The Q3 budget review happened on Tuesday; the team approved a marketing spend increase.";
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             \"timestamp\", content_inline, content_tsv) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3, \
                 to_tsvector('simple', $3)) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(coral_content)
    .fetch_one(&pool)
    .await
    .expect("seed coral");
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(reef_id)
        .bind(atom_id)
        .execute(&pool)
        .await
        .expect("link reef to coral");

    let idempotency_key = format!("idem-race-{}", Uuid::new_v4());
    let question = "What happened with the Q3 budget review?";

    const CONCURRENT: usize = 6;
    let mut handles = Vec::with_capacity(CONCURRENT);
    for _ in 0..CONCURRENT {
        let app = app.clone();
        let token = token.clone();
        let args = json!({"question": question, "ocean_ids": [graph_id], "idempotency_key": idempotency_key});
        handles.push(tokio::spawn(async move { legacy_call_tool(&app, &token, "ask_ocean", args).await }));
    }
    let mut task_ids = std::collections::HashSet::new();
    for h in handles {
        let result = h.await.expect("ask_ocean concurrent call task panicked");
        assert_ne!(result["isError"].as_bool(), Some(true), "{result:?}");
        let body = tool_json(&result);
        assert_ne!(
            body["error"].get("code").and_then(Value::as_str),
            Some("rate_limited"),
            "none of these calls spawn more than one genuinely NEW task, so none should ever be \
             rejected by the in-flight cap, however many concurrent callers there are: {body:?}"
        );
        task_ids.insert(body["task_id"].as_str().expect("task_id is a string").to_string());
    }
    assert_eq!(
        task_ids.len(),
        1,
        "every concurrent call with the SAME idempotency_key must observe the SAME task_id: {task_ids:?}"
    );
    let task_id = task_ids.into_iter().next().unwrap();

    let row_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM mcp_tasks WHERE owner_user_id = $1 AND idempotency_key = $2",
    )
    .bind(user_id)
    .bind(&idempotency_key)
    .fetch_one(&pool)
    .await
    .expect("count mcp_tasks rows");
    assert_eq!(row_count, 1, "exactly one mcp_tasks row must exist for this key, even under real concurrency");

    // Let the background synthesis finish, then prove exactly one
    // `llm_usage` row was billed for it — the actual dollar-cost proof that
    // no duplicate synthesis was spawned.
    let _ = poll_legacy_task_result(&app, &token, &task_id, POLL_ATTEMPTS, POLL_DELAY).await;
    let usage_count: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .expect("count llm_usage rows");
    assert_eq!(
        usage_count, 1,
        "exactly one llm_usage row must be billed — a duplicate claim would have spawned a \
         second, separately-billed synthesis"
    );
}

/// CodeRabbit re-review, MAJOR #8 (part 1 — permit ordering): a retry
/// against EXISTING work must never be rejected by the in-flight cap, even
/// when the cap is fully exhausted by other calls. Before this fix, the
/// permit was acquired BEFORE the idempotency lookup, so a legitimate retry
/// competed for a new-work slot it never needed.
#[tokio::test]
async fn ask_ocean_a_retry_against_existing_work_is_never_rejected_by_an_exhausted_in_flight_cap() {
    let pool = test_pool!();
    let _env = McpEnv::enable_with_sync_wait_ms(
        "https://mcp-task-idempotency-permit-ordering.example/mcp",
        0,
    )
    .await;
    let app = app_for_pool_with_llm(
        pool.clone(),
        Arc::new(SlowLlm { delay: std::time::Duration::from_secs(5) }),
    );
    let sub = fresh_sub("idempotency_permit_ordering");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    // Seed a reef + coral directly so `run_ask` actually reaches the
    // (5s-slow) synthesis branch instead of degrading to `no_answer` before
    // ever calling the LLM — without this, every task here would settle
    // ALMOST INSTANTLY (no retrievable content), and by the time the retry
    // below runs, call 1 would already be `completed`, not `working`,
    // defeating the whole point of this test.
    let reef_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph_id)
    .bind("Q3 budget review")
    .fetch_one(&pool)
    .await
    .expect("seed reef");
    let coral_content = "The Q3 budget review happened on Tuesday; the team approved a marketing spend increase.";
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             \"timestamp\", content_inline, content_tsv) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3, \
                 to_tsvector('simple', $3)) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(coral_content)
    .fetch_one(&pool)
    .await
    .expect("seed coral");
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(reef_id)
        .bind(atom_id)
        .execute(&pool)
        .await
        .expect("link reef to coral");

    // Call 1: WITH a key — held open by the 5s SlowLlm delay, comfortably
    // longer than this whole test. This is the one we retry against below.
    let key = format!("idem-ordering-{}", Uuid::new_v4());
    let first = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({"question": "What happened with the Q3 budget review?", "ocean_ids": [graph_id], "idempotency_key": key}),
    )
    .await;
    let first_body = tool_json(&first);
    assert_eq!(first_body["status"], "working", "{first_body:?}");
    let first_task_id = first_body["task_id"].as_str().unwrap().to_string();

    // Calls 2..cap: keyless (genuinely new work each), to fill the
    // remaining in-flight permits. Each must ALSO reach the (5s-slow)
    // synthesis branch — not just call 1 — or it would release its permit
    // almost instantly (no retrievable content => the honesty guard's
    // `no_answer` degrade never calls the LLM at all), leaving the cap NOT
    // actually exhausted by the time the fixture-precondition check below
    // runs. Keyword-search relevance is what matters here, not exact text,
    // so each just needs to still mention the seeded content.
    for i in 1..PINNED_ASK_MAX_IN_FLIGHT {
        let result = legacy_call_tool(
            &app,
            &token,
            "ask_ocean",
            json!({
                "question": format!("What happened with the Q3 budget review? (filler {i})"),
                "ocean_ids": [graph_id],
            }),
        )
        .await;
        assert_eq!(tool_json(&result)["status"], "working", "{result:?}");
    }

    // Fixture precondition: the cap really is fully exhausted now — a
    // brand-new, distinct (keyless) call must be rejected.
    let saturated = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({
            "question": "What happened with the Q3 budget review? (a brand new distinct question)",
            "ocean_ids": [graph_id],
        }),
    )
    .await;
    let saturated_body = tool_json(&saturated);
    assert_eq!(
        saturated_body["error"]["code"], "rate_limited",
        "fixture precondition failed: the in-flight cap must be exhausted at this point: {saturated_body:?}"
    );

    // The actual assertion: a retry of call 1's own idempotency_key (same
    // question, same ocean — same request hash) must succeed, reusing the
    // existing task, even though the cap is fully exhausted by the calls
    // above.
    let retry = legacy_call_tool(
        &app,
        &token,
        "ask_ocean",
        json!({
            "question": "What happened with the Q3 budget review?",
            "ocean_ids": [graph_id],
            "idempotency_key": key,
        }),
    )
    .await;
    assert_ne!(retry["isError"].as_bool(), Some(true), "a retry against existing work must not tool-error: {retry:?}");
    let retry_body = tool_json(&retry);
    assert_eq!(
        retry_body["status"], "working",
        "the retry must not be rejected by the in-flight cap: {retry_body:?}"
    );
    assert_eq!(
        retry_body["task_id"].as_str(),
        Some(first_task_id.as_str()),
        "the retry must reuse the ORIGINAL call's task_id: {retry_body:?}"
    );
}

/// CodeRabbit re-review, MAJOR #8 (part 2 — reuse falls into sync-wait): a
/// retry that finds an EXISTING `working` task must fall into the SAME
/// bounded sync-wait a freshly spawned task uses, not return the task_id
/// immediately — so it, too, gets a chance at an inline answer once the
/// task it's reusing completes. Fires the original call and the retry
/// concurrently (a small head start for the original, so it reliably wins
/// the atomic claim) rather than sequentially, so the retry's own sync-wait
/// genuinely overlaps the still-running background synthesis.
#[tokio::test]
async fn ask_ocean_a_retry_against_a_working_task_can_receive_the_answer_inline_via_sync_wait() {
    let pool = test_pool!();
    // Comfortably longer than the SlowLlm's delay AND the sync-wait loop's
    // own ~2s DB-poll-fallback tick (the retry's own `completion_signal` is
    // never fired directly — see `ask_ocean`'s doc comment on that binding
    // — so it always waits for at least one such tick).
    let _env = McpEnv::enable_with_sync_wait_ms(
        "https://mcp-task-idempotency-retry-inline.example/mcp",
        5_000,
    )
    .await;
    let app = app_for_pool_with_llm(
        pool.clone(),
        Arc::new(SlowLlm { delay: std::time::Duration::from_millis(800) }),
    );
    let sub = fresh_sub("idempotency_retry_inline");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    let reef_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph_id)
    .bind("Q3 budget review")
    .fetch_one(&pool)
    .await
    .expect("seed reef");
    let coral_content = "The Q3 budget review happened on Tuesday; the team approved a marketing spend increase.";
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             \"timestamp\", content_inline, content_tsv) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3, \
                 to_tsvector('simple', $3)) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(coral_content)
    .fetch_one(&pool)
    .await
    .expect("seed coral");
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(reef_id)
        .bind(atom_id)
        .execute(&pool)
        .await
        .expect("link reef to coral");

    let key = format!("idem-retry-inline-{}", Uuid::new_v4());
    let question = "What happened with the Q3 budget review?";

    let app1 = app.clone();
    let token1 = token.clone();
    let args1 = json!({"question": question, "ocean_ids": [graph_id], "idempotency_key": key});
    let first_handle = tokio::spawn(async move { legacy_call_tool(&app1, &token1, "ask_ocean", args1).await });

    // A brief, generous head start so the FIRST call reliably wins the
    // atomic claim (`Inserted`) and the retry below reliably finds
    // `Existing` — not a hard real-time guarantee, just enough margin that
    // flipping the order would require an extremely loaded CI runner.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let args2 = json!({"question": question, "ocean_ids": [graph_id], "idempotency_key": key});
    let retry_result = legacy_call_tool(&app, &token, "ask_ocean", args2).await;

    let first_result = first_handle.await.expect("first call task panicked");
    assert_ne!(first_result["isError"].as_bool(), Some(true), "{first_result:?}");

    assert_ne!(retry_result["isError"].as_bool(), Some(true), "retry must not tool-error: {retry_result:?}");
    let retry_body = tool_json(&retry_result);
    assert!(
        retry_body.get("task_id").is_none() && retry_body.get("status").is_none(),
        "the retry's OWN sync-wait must observe the reused task's completion (via the DB-poll \
         fallback) and return the answer inline, not the task_id/status envelope: {retry_body:?}"
    );
    assert!(retry_body.get("mode").is_some(), "inline answer must carry ask_ocean's mode: {retry_body:?}");
}

/// Correctness review follow-up item 10 — investigated, and the premise
/// didn't hold up: the concern was that `RequestContext::client_capabilities()`
/// (which reads ONLY the current request's own inline `_meta` in stateless
/// mode — see `ask_ocean`'s own `modern_era` comment) would let a request
/// carrying `MCP-Protocol-Version: 2026-07-28` but NO per-call `_meta` sneak
/// through and be silently misclassified LEGACY. It does not: rmcp's own
/// transport-level `validate_request_protocol_version_meta` rejects exactly
/// that combination with a loud `-32602 Invalid params` before the request
/// ever reaches tool dispatch. This test pins THAT behavior — a clean
/// rejection, not a successful-but-misclassified call — so a future rmcp
/// upgrade that loosened this check would be caught here rather than
/// silently reopening the gap `ask_ocean`'s own `modern_era` comment
/// documents as investigated-and-closed.
#[tokio::test]
async fn a_protocol_version_header_without_per_call_meta_is_rejected_not_silently_downgraded() {
    let pool = test_pool!();
    let _env = McpEnv::enable("https://mcp-task-header-only.example/mcp").await;
    let app = app_for_pool(pool.clone());
    let sub = fresh_sub("header_only_no_meta");
    let (_user_id, _account_id, graph_id) = provision(&app, &pool, &sub).await;
    let token = read_token(&sub);

    // The header, and ONLY the header — no `_meta` at all, unlike
    // `modern_call_tool`'s combined shape.
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "ask_ocean",
            "arguments": {"question": "what happened this week?", "ocean_ids": [graph_id]},
        }
    });
    let (status, _headers, rpc) = raw_post(
        &app,
        Some(&token),
        // The header set SEP-2243 requires once MCP-Protocol-Version names
        // the tasks-era protocol — `Mcp-Method`/`Mcp-Name` are a transport
        // requirement independent of `_meta`, so they're included even
        // though this test's whole point is that `_meta` itself is absent.
        &[MODERN_HEADER, ("Mcp-Method", "tools/call"), ("Mcp-Name", "ask_ocean")],
        body,
    )
    .await;
    // rmcp answers this shape with an HTTP 400 carrying a JSON-RPC error
    // body (not a 200 with an `error` field) — assert on the JSON-RPC error
    // itself, which is the stable part of the contract this test cares
    // about; the transport status code is a implementation detail of how
    // rmcp surfaces it.
    assert!(
        rpc.get("error").is_some(),
        "a MCP-Protocol-Version: 2026-07-28 request with no per-call _meta must be REJECTED \
         (never silently served as a legacy-shaped success): {status} {rpc:?}"
    );
    assert_eq!(
        rpc["error"]["code"], -32602,
        "expected rmcp's own 'Invalid params: request _meta is missing...' rejection: {rpc:?}"
    );
    let message = rpc["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("_meta"),
        "the rejection reason must name _meta as what's missing, not some unrelated failure: {rpc:?}"
    );
}
