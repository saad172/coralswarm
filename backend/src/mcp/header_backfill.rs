//! SEP-2243 header back-fill for lenient interop.
//!
//! rmcp 3.1.2 (`transport/common/mcp_headers.rs`) REQUIRES the standardized
//! `Mcp-Method` / `Mcp-Name` request headers from any client that declares
//! `MCP-Protocol-Version >= 2026-07-28`, and rejects the request with HTTP 400
//! / JSON-RPC `-32020` before dispatch when they are missing or disagree with
//! the body. `Mcp-Name` is sourced from `params.name` for `tools/call` and
//! `prompts/get`, from `params.uri` for `resources/read|subscribe|unsubscribe`,
//! and from `params.taskId` for `tasks/get|update|cancel`.
//!
//! Measured against production on 2026-09-14 with a real reviewer token:
//! `tools/list` and `tools/call` succeed for a 2026-07-28 client, but
//! `resources/read ui://widget/ask_ocean.html` answers
//! `-32020 missing required Mcp-Method header` — exactly the shape a host
//! shows as "Error loading app — Failed to fetch template" after a tool call
//! worked. A client that derives `Mcp-Name` from `params.name` alone never
//! sends one for `resources/read`.
//!
//! This layer is Postel's-law leniency: when a 2026-07-28 request carries no
//! `Mcp-Method` and/or no `Mcp-Name`, derive them from the body the same way
//! rmcp itself would, and add them. Headers that are already present are NEVER
//! rewritten — a client that sends a mismatching value still gets rmcp's own
//! `-32020`, so the validation this layer exists to satisfy is not weakened,
//! only the "absent" case is filled in. Batches, notifications without a
//! `method`, non-POSTs, older protocol versions and bodies larger than
//! [`MAX_INSPECTED_BODY_BYTES`] pass through untouched.

use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue, Method, header};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use serde_json::Value;

/// Same constants rmcp uses (`transport/common/http_header.rs`).
const HEADER_MCP_METHOD: &str = "mcp-method";
const HEADER_MCP_NAME: &str = "mcp-name";
const HEADER_PROTOCOL_VERSION: &str = "mcp-protocol-version";
/// `ProtocolVersion::STANDARD_HEADERS` — the first version that mandates the
/// SEP-2243 headers. Versions are `YYYY-MM-DD` strings, so a plain string
/// comparison orders them correctly.
const STANDARD_HEADERS_VERSION: &str = "2026-07-28";
/// Bodies above this size are passed through unread: every MCP request this
/// layer cares about is a few hundred bytes, and buffering is the only cost
/// the layer adds.
pub(crate) const MAX_INSPECTED_BODY_BYTES: usize = 64 * 1024;

const NAME_FROM_NAME: &[&str] = &["tools/call", "prompts/get"];
const NAME_FROM_URI: &[&str] = &[
    "resources/read",
    "resources/subscribe",
    "resources/unsubscribe",
];
const NAME_FROM_TASK_ID: &[&str] = &["tasks/get", "tasks/update", "tasks/cancel"];

/// Derive the SEP-2243 `Mcp-Name` value for a single JSON-RPC request body.
/// Mirrors rmcp's `extract_name`: `None` for methods that carry no name.
pub(crate) fn extract_name(method: &str, params: Option<&Value>) -> Option<String> {
    let params = params?;
    let key = if NAME_FROM_NAME.contains(&method) {
        "name"
    } else if NAME_FROM_URI.contains(&method) {
        "uri"
    } else if NAME_FROM_TASK_ID.contains(&method) {
        "taskId"
    } else {
        return None;
    };
    params.get(key)?.as_str().map(str::to_owned)
}

/// rmcp's `requires_base64`: a value that cannot travel as a bare header
/// value (leading/trailing blank, control or non-ASCII byte, or one that
/// already looks like the `=?base64?…?=` sentinel) is wrapped the way the
/// SDK's own client wraps it, so the server-side decoder reads it back
/// verbatim.
pub(crate) fn encode_header_value(value: &str) -> String {
    let bytes = value.as_bytes();
    let needs_wrap = !value.is_empty()
        && (matches!(bytes.first(), Some(b' ' | b'\t'))
            || matches!(bytes.last(), Some(b' ' | b'\t'))
            || value.chars().any(|c| (c as u32) < 0x20 || (c as u32) > 0x7E)
            || (value.starts_with("=?base64?") && value.ends_with("?=")));
    if needs_wrap {
        format!("=?base64?{}?=", BASE64_STANDARD.encode(value))
    } else {
        value.to_owned()
    }
}

/// Pure decision: given the request's existing headers and the parsed body,
/// which headers (name, value) should be ADDED. Never returns a header that
/// is already present. Returns nothing for anything that is not a single
/// JSON-RPC request object with a string `method`.
pub(crate) fn headers_to_add(
    has_method_header: bool,
    has_name_header: bool,
    body: &Value,
) -> Vec<(HeaderName, HeaderValue)> {
    let mut out = Vec::new();
    let Some(method) = body.get("method").and_then(Value::as_str) else {
        return out;
    };
    if !has_method_header {
        if let Ok(v) = HeaderValue::from_str(method) {
            out.push((HeaderName::from_static(HEADER_MCP_METHOD), v));
        }
    }
    if !has_name_header {
        if let Some(name) = extract_name(method, body.get("params")) {
            if let Ok(v) = HeaderValue::from_str(&encode_header_value(&name)) {
                out.push((HeaderName::from_static(HEADER_MCP_NAME), v));
            }
        }
    }
    out
}

fn declares_standard_headers(req: &Request) -> bool {
    req.headers()
        .get(HEADER_PROTOCOL_VERSION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim() >= STANDARD_HEADERS_VERSION)
}

/// axum middleware: see the module docs. Must sit INSIDE the body-size limit
/// and auth layers (it only ever reads authenticated, size-bounded bodies)
/// and directly outside rmcp's `StreamableHttpService`.
pub(crate) async fn backfill_mcp_headers(req: Request, next: Next) -> Response {
    if req.method() != Method::POST || !declares_standard_headers(&req) {
        return next.run(req).await;
    }
    let has_method = req.headers().contains_key(HEADER_MCP_METHOD);
    let has_name = req.headers().contains_key(HEADER_MCP_NAME);
    if has_method && has_name {
        return next.run(req).await;
    }
    // A Content-Length above the inspection cap is passed through unread.
    // A MISSING Content-Length (chunked transfer, which some hosts use) is
    // not a reason to skip: the body is buffered anyway — the outer
    // `RequestBodyLimitLayer` already bounds it to `MCP_MAX_BODY_BYTES`, so
    // this never reads more than rmcp itself would — and only inspected
    // when it turns out to be small (CodeRabbit #922).
    let declared_len = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    if declared_len.is_some_and(|len| len > MAX_INSPECTED_BODY_BYTES) {
        return next.run(req).await;
    }

    let (mut parts, body) = req.into_parts();
    let bytes = match to_bytes(body, super::MCP_MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            // The body could not be read; there is nothing sensible left to
            // forward, and rmcp would fail on it the same way. Surface a
            // plain 400 rather than a half-consumed stream.
            return Response::builder()
                .status(axum::http::StatusCode::BAD_REQUEST)
                .body(Body::from("unreadable request body"))
                .unwrap_or_else(|_| Response::new(Body::empty()));
        }
    };
    if bytes.len() <= MAX_INSPECTED_BODY_BYTES {
        if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
            for (name, val) in headers_to_add(has_method, has_name, &value) {
                parts.headers.insert(name, val);
            }
        }
    }
    next.run(Request::from_parts(parts, Body::from(bytes))).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::routing::post;
    use serde_json::json;
    use tower::ServiceExt;

    fn hv(v: &HeaderValue) -> &str {
        v.to_str().unwrap()
    }

    #[test]
    fn resources_read_gets_method_and_name_from_uri() {
        let body = json!({"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}});
        let added = headers_to_add(false, false, &body);
        assert_eq!(added.len(), 2);
        assert_eq!(added[0].0.as_str(), "mcp-method");
        assert_eq!(hv(&added[0].1), "resources/read");
        assert_eq!(added[1].0.as_str(), "mcp-name");
        assert_eq!(hv(&added[1].1), "ui://widget/ask_ocean.html");
    }

    #[test]
    fn tools_call_gets_name_from_params_name() {
        let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ask_ocean","arguments":{}}});
        let added = headers_to_add(false, false, &body);
        assert_eq!(hv(&added[0].1), "tools/call");
        assert_eq!(hv(&added[1].1), "ask_ocean");
    }

    #[test]
    fn tasks_get_gets_name_from_task_id() {
        let body = json!({"jsonrpc":"2.0","id":1,"method":"tasks/get","params":{"taskId":"t-1"}});
        let added = headers_to_add(false, false, &body);
        assert_eq!(hv(&added[1].1), "t-1");
    }

    #[test]
    fn present_headers_are_never_touched() {
        let body = json!({"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}});
        assert!(headers_to_add(true, true, &body).is_empty());
        let only_name = headers_to_add(true, false, &body);
        assert_eq!(only_name.len(), 1);
        assert_eq!(only_name[0].0.as_str(), "mcp-name");
        let only_method = headers_to_add(false, true, &body);
        assert_eq!(only_method.len(), 1);
        assert_eq!(only_method[0].0.as_str(), "mcp-method");
    }

    #[test]
    fn methods_without_a_name_only_get_mcp_method() {
        let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/list"});
        let added = headers_to_add(false, false, &body);
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].0.as_str(), "mcp-method");
    }

    #[test]
    fn batches_and_bodies_without_method_are_ignored() {
        let batch = json!([{"jsonrpc":"2.0","id":1,"method":"tools/list"}]);
        assert!(headers_to_add(false, false, &batch).is_empty());
        let response_shaped = json!({"jsonrpc":"2.0","id":1,"result":{}});
        assert!(headers_to_add(false, false, &response_shaped).is_empty());
    }

    #[test]
    fn non_header_safe_names_are_base64_wrapped_like_rmcp() {
        assert_eq!(encode_header_value("ui://widget/x.html"), "ui://widget/x.html");
        let wrapped = encode_header_value("héllo");
        assert!(wrapped.starts_with("=?base64?") && wrapped.ends_with("?="));
        assert!(encode_header_value(" pad").starts_with("=?base64?"));
        // A value that already looks like the sentinel is wrapped again so the
        // decoder cannot misread it.
        assert!(encode_header_value("=?base64?abc?=").starts_with("=?base64?"));
    }

    /// Echo handler: reports which Mcp-* headers reached the inner service and
    /// the body it received, proving the buffered body is re-attached intact.
    async fn echo(req: Request) -> Response {
        let m = req.headers().get("mcp-method").map(|v| hv(v).to_owned());
        let n = req.headers().get("mcp-name").map(|v| hv(v).to_owned());
        let body = to_bytes(req.into_body(), usize::MAX).await.unwrap();
        Response::new(Body::from(
            json!({"method": m, "name": n, "body_len": body.len()}).to_string(),
        ))
    }

    fn app() -> Router {
        Router::new()
            .route("/mcp", post(echo))
            .layer(axum::middleware::from_fn(backfill_mcp_headers))
    }

    async fn send(
        version: Option<&str>,
        extra: &[(&str, &str)],
        body: &str,
    ) -> serde_json::Value {
        send_with_length(version, extra, body, true).await
    }

    async fn send_with_length(
        version: Option<&str>,
        extra: &[(&str, &str)],
        body: &str,
        declare_length: bool,
    ) -> serde_json::Value {
        let mut b = Request::builder()
            .method(Method::POST)
            .uri("/mcp")
            .header(header::CONTENT_TYPE, "application/json");
        if declare_length {
            b = b.header(header::CONTENT_LENGTH, body.len().to_string());
        }
        if let Some(v) = version {
            b = b.header("MCP-Protocol-Version", v);
        }
        for (k, v) in extra {
            b = b.header(*k, *v);
        }
        let resp = app()
            .oneshot(b.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn middleware_backfills_for_2026_07_28_and_reattaches_the_body() {
        let body = r#"{"jsonrpc":"2.0","id":7,"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}}"#;
        let out = send(Some("2026-07-28"), &[], body).await;
        assert_eq!(out["method"], "resources/read");
        assert_eq!(out["name"], "ui://widget/ask_ocean.html");
        assert_eq!(out["body_len"], body.len());
    }

    #[tokio::test]
    async fn middleware_leaves_older_protocol_versions_alone() {
        let body = r#"{"jsonrpc":"2.0","id":7,"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}}"#;
        for v in [None, Some("2025-06-18"), Some("2025-11-25")] {
            let out = send(v, &[], body).await;
            assert!(out["method"].is_null(), "version {v:?} must not be back-filled");
            assert!(out["name"].is_null());
            assert_eq!(out["body_len"], body.len());
        }
    }

    #[tokio::test]
    async fn middleware_never_overwrites_a_present_header() {
        let body = r#"{"jsonrpc":"2.0","id":7,"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}}"#;
        let out = send(
            Some("2026-07-28"),
            &[("Mcp-Method", "resources/read"), ("Mcp-Name", "something-else")],
            body,
        )
        .await;
        assert_eq!(out["name"], "something-else");
    }

    #[tokio::test]
    async fn middleware_backfills_without_a_content_length_header() {
        // Chunked transfer: no Content-Length at all (CodeRabbit #922). The
        // body is buffered under the outer 2 MiB bound and still inspected.
        let body = r#"{"jsonrpc":"2.0","id":7,"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}}"#;
        let out = send_with_length(Some("2026-07-28"), &[], body, false).await;
        assert_eq!(out["method"], "resources/read");
        assert_eq!(out["name"], "ui://widget/ask_ocean.html");
        assert_eq!(out["body_len"], body.len());
    }

    #[tokio::test]
    async fn middleware_leaves_a_large_undeclared_body_uninspected_but_intact() {
        let padding = "x".repeat(MAX_INSPECTED_BODY_BYTES + 1);
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"resources/read","params":{{"uri":"ui://widget/ask_ocean.html","pad":"{padding}"}}}}"#
        );
        let out = send_with_length(Some("2026-07-28"), &[], &body, false).await;
        assert!(out["method"].is_null());
        assert_eq!(out["body_len"], body.len());
    }

    #[tokio::test]
    async fn middleware_passes_oversized_bodies_through_unread() {
        let padding = "x".repeat(MAX_INSPECTED_BODY_BYTES + 1);
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"resources/read","params":{{"uri":"ui://widget/ask_ocean.html","pad":"{padding}"}}}}"#
        );
        let out = send(Some("2026-07-28"), &[], &body).await;
        assert!(out["method"].is_null());
        assert_eq!(out["body_len"], body.len());
    }
}
