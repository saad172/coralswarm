//! Opt-in, content-free template-fetch diagnostics. Request frames are observed
//! as they pass through, with a bounded 16 KiB diagnostic copy so split frames
//! are supported. No frames are withheld/replayed; response bodies are untouched.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use axum::{
    body::Body,
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct WidgetTrace(Uuid);

fn protocol(value: Option<&str>) -> &'static str {
    match value {
        None => "absent",
        Some("2025-03-26") => "2025-03-26",
        Some("2025-06-18") => "2025-06-18",
        Some("2025-11-25") => "2025-11-25",
        Some("2026-07-28") => "2026-07-28",
        Some(_) => "other",
    }
}

fn response_format(value: Option<&str>) -> &'static str {
    match value.map(|v| v.split(';').next().unwrap_or("").trim()) {
        Some("application/json") => "json",
        Some("text/event-stream") => "sse",
        None => "absent",
        Some(_) => "other",
    }
}

pub(crate) fn widget_key(uri: &str) -> Option<&'static str> {
    match uri {
        "ui://coralswarm/result-card-v1.html" => Some("result_card_v1"),
        "ui://coralswarm/ask-ocean-v2.html" => Some("ask_ocean_v2"),
        "ui://coralswarm/search-atoms-v2.html" => Some("search_atoms_v2"),
        "ui://coralswarm/reef-overview-v2.html" => Some("get_reef_v2"),
        "ui://coralswarm/recent-activity-v2.html" => Some("recent_activity_v2"),
        "ui://widget/ask_ocean.html" => Some("ask_ocean"),
        "ui://widget/search_atoms.html" => Some("search_atoms"),
        "ui://widget/get_reef.html" => Some("get_reef"),
        "ui://widget/recent_activity.html" => Some("recent_activity"),
        _ => None,
    }
}

#[derive(Debug, PartialEq)]
struct RequestSummary {
    widget: &'static str,
    body_protocol: &'static str,
    protocol_present: bool,
    capabilities_present: bool,
    capabilities_object: bool,
    client_info_present: bool,
    client_info_object: bool,
}

fn summarize(value: &Value) -> Option<RequestSummary> {
    if value.get("method")?.as_str()? != "resources/read" {
        return None;
    }
    let params = value.get("params")?;
    let widget = widget_key(params.get("uri")?.as_str()?)?;
    let meta = params.get("_meta");
    let version = meta.and_then(|m| m.get("io.modelcontextprotocol/protocolVersion"));
    let capabilities = meta.and_then(|m| m.get("io.modelcontextprotocol/clientCapabilities"));
    let info = meta.and_then(|m| m.get("io.modelcontextprotocol/clientInfo"));
    Some(RequestSummary {
        widget,
        body_protocol: protocol(version.and_then(Value::as_str)),
        protocol_present: version.is_some(),
        capabilities_present: capabilities.is_some(),
        capabilities_object: capabilities.is_some_and(Value::is_object),
        client_info_present: info.is_some(),
        client_info_object: info.is_some_and(Value::is_object),
    })
}

const INSPECTION_CAP: usize = 16 * 1024;

#[derive(Default)]
struct Inspection {
    bytes: Vec<u8>,
    over_cap: bool,
    finished: bool,
}

impl Inspection {
    fn push(&mut self, bytes: &[u8]) {
        if self.over_cap {
            return;
        }
        if self.bytes.len().saturating_add(bytes.len()) > INSPECTION_CAP {
            self.bytes.clear();
            self.over_cap = true;
        } else {
            self.bytes.extend_from_slice(bytes);
        }
    }
    fn abort(&mut self, trace: &WidgetTrace) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.bytes.clear();
        tracing::info!(target: "coralswarm_backend::mcp::widget_trace", trace_id = %trace.0,
            phase = "inspection", inspection_status = "incomplete", "MCP widget fetch diagnostic");
    }
    fn finish(&mut self, trace: &WidgetTrace) {
        if self.finished {
            return;
        }
        self.finished = true;
        let value = (!self.over_cap)
            .then(|| serde_json::from_slice::<Value>(&self.bytes).ok())
            .flatten();
        let summary = value.as_ref().and_then(summarize);
        let status = if self.over_cap {
            "over_cap"
        } else if value.is_none() {
            "invalid_json"
        } else if summary.is_none() {
            "not_known_widget_read"
        } else {
            "observed"
        };
        tracing::info!(target: "coralswarm_backend::mcp::widget_trace", trace_id = %trace.0,
            phase = "inspection", inspection_status = status, "MCP widget fetch diagnostic");
        if let Some(s) = summary {
            tracing::info!(target: "coralswarm_backend::mcp::widget_trace",
                trace_id = %trace.0, phase = "request", method = "resources/read",
                widget = s.widget, body_protocol = s.body_protocol,
                protocol_present = s.protocol_present,
                capabilities_present = s.capabilities_present,
                capabilities_object = s.capabilities_object,
                client_info_present = s.client_info_present,
                client_info_object = s.client_info_object,
                "MCP widget fetch diagnostic");
        }
        self.bytes.clear();
    }
}

struct ObservedRequest {
    inner: Body,
    trace: WidgetTrace,
    inspection: Inspection,
}

impl Drop for ObservedRequest {
    fn drop(&mut self) {
        self.inspection.abort(&self.trace);
    }
}

impl HttpBody for ObservedRequest {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let frame = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &frame {
            if let Some(bytes) = frame.data_ref() {
                self.inspection.push(bytes);
            }
        }
        if matches!(&frame, Poll::Ready(Some(Err(_)))) {
            let trace = self.trace.clone();
            self.inspection.abort(&trace);
        } else if matches!(&frame, Poll::Ready(None)) || self.inner.is_end_stream() {
            let trace = self.trace.clone();
            self.inspection.finish(&trace);
        }
        // Preserve the exact frame, pending/error signal, trailers and timing.
        frame
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Mounted outside the MCP guards to observe HTTP denials. The lazy tee reads
/// only when downstream reads, so an auth rejection never parses its body.
/// State is resolved once when the router is built; disabled means no wrapper.
pub(crate) async fn observe(State(enabled): State<bool>, mut req: Request, next: Next) -> Response {
    if !enabled {
        return next.run(req).await;
    }
    let trace = WidgetTrace(Uuid::new_v4());
    tracing::info!(target: "coralswarm_backend::mcp::widget_trace", trace_id = %trace.0,
        phase = "transport", header_protocol = protocol(req.headers().get("mcp-protocol-version").and_then(|v| v.to_str().ok())),
        authorization_present = req.headers().contains_key(axum::http::header::AUTHORIZATION),
        method_header_present = req.headers().contains_key("mcp-method"),
        name_header_present = req.headers().contains_key("mcp-name"),
        "MCP widget fetch diagnostic");
    req.extensions_mut().insert(trace.clone());
    let (parts, body) = req.into_parts();
    let req = Request::from_parts(
        parts,
        Body::new(ObservedRequest {
            inner: body,
            trace: trace.clone(),
            inspection: Inspection::default(),
        }),
    );
    let response = next.run(req).await;
    tracing::info!(target: "coralswarm_backend::mcp::widget_trace", trace_id = %trace.0,
        phase = "http_response", status = response.status().as_u16(),
        response_format = response_format(response.headers().get("content-type").and_then(|v| v.to_str().ok())),
        "MCP widget fetch diagnostic");
    response
}

pub(crate) fn outcome(
    ctx: &rmcp::service::RequestContext<rmcp::RoleServer>,
    uri: &str,
    code: Option<i32>,
) {
    let Some(widget) = widget_key(uri) else {
        return;
    };
    let Some(trace) = ctx
        .extensions
        .get::<axum::http::request::Parts>()
        .and_then(|p| p.extensions.get::<WidgetTrace>())
    else {
        return;
    };
    tracing::info!(target: "coralswarm_backend::mcp::widget_trace", trace_id = %trace.0,
        phase = "handler_result", widget, jsonrpc_error_code = code,
        result_complete = code.is_none(), "MCP widget fetch diagnostic");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use futures_util::stream;
    use serde_json::json;

    #[test]
    fn versioned_designed_templates_have_bounded_diagnostic_keys() {
        for (uri, key) in [
            ("ui://coralswarm/ask-ocean-v2.html", "ask_ocean_v2"),
            ("ui://coralswarm/search-atoms-v2.html", "search_atoms_v2"),
            ("ui://coralswarm/reef-overview-v2.html", "get_reef_v2"),
            (
                "ui://coralswarm/recent-activity-v2.html",
                "recent_activity_v2",
            ),
        ] {
            assert_eq!(widget_key(uri), Some(key));
            let request = json!({"method":"resources/read","params":{"uri":uri}});
            assert_eq!(summarize(&request).unwrap().widget, key);
        }
        assert_eq!(widget_key("ui://coralswarm/SECRET.html"), None);
    }

    #[test]
    fn modern_missing_and_present_metadata_are_distinguished_without_values() {
        assert_eq!(
            widget_key("ui://coralswarm/result-card-v1.html"),
            Some("result_card_v1")
        );
        let mut request =
            json!({"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}});
        let missing = summarize(&request).unwrap();
        assert_eq!(missing.body_protocol, "absent");
        assert!(
            !missing.protocol_present
                && !missing.capabilities_present
                && !missing.client_info_present
        );
        request["params"]["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientCapabilities":{},
            "io.modelcontextprotocol/clientInfo":{"name":"SECRET_CLIENT_NAME"}
        });
        let present = summarize(&request).unwrap();
        assert_eq!(present.body_protocol, "2026-07-28");
        assert!(
            present.protocol_present && present.capabilities_object && present.client_info_object
        );
        assert!(!format!("{present:?}").contains("SECRET"));
        request["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] = json!("SECRET");
        let malformed = summarize(&request).unwrap();
        assert!(malformed.capabilities_present && !malformed.capabilities_object);
    }

    #[test]
    fn arbitrary_uri_methods_and_protocol_values_are_not_logged() {
        assert!(
            summarize(&json!({"method":"resources/read","params":{"uri":"ui://SECRET"}})).is_none()
        );
        assert!(summarize(
            &json!({"method":"SECRET","params":{"uri":"ui://widget/ask_ocean.html"}})
        )
        .is_none());
        assert_eq!(protocol(Some("SECRET")), "other");
        assert_eq!(protocol(None), "absent");
        assert_eq!(
            response_format(Some("application/json; charset=utf-8")),
            "json"
        );
        assert_eq!(response_format(Some("text/event-stream")), "sse");
        assert_eq!(response_format(Some("SECRET")), "other");
        assert_eq!(response_format(None), "absent");
    }

    #[test]
    fn split_json_is_inspected_and_cap_is_bounded() {
        let request =
            br#"{"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}}"#;
        let mut inspection = Inspection::default();
        for chunk in request.chunks(7) {
            inspection.push(chunk);
        }
        let value: Value = serde_json::from_slice(&inspection.bytes).unwrap();
        assert_eq!(summarize(&value).unwrap().widget, "ask_ocean");
        inspection.push(&vec![b'x'; INSPECTION_CAP]);
        assert!(inspection.over_cap);
        assert!(inspection.bytes.is_empty());
        inspection.push(b"ignored");
        assert!(inspection.bytes.is_empty());
    }

    #[tokio::test]
    async fn observation_preserves_split_request_frames_and_size_hints() {
        let chunks = [
            Bytes::from_static(b"{\"method\":"),
            Bytes::from_static(b"\"resources/read\"}"),
        ];
        let expected = chunks.concat();
        let inner = Body::from_stream(stream::iter(
            chunks.into_iter().map(Ok::<_, std::io::Error>),
        ));
        let hint = inner.size_hint();
        let observed = ObservedRequest {
            inner,
            trace: WidgetTrace(Uuid::new_v4()),
            inspection: Inspection::default(),
        };
        assert_eq!(observed.size_hint().lower(), hint.lower());
        assert_eq!(observed.size_hint().upper(), hint.upper());
        assert_eq!(
            to_bytes(Body::new(observed), usize::MAX)
                .await
                .unwrap()
                .as_ref(),
            expected
        );
    }
    #[tokio::test]
    async fn enabled_and_disabled_preserve_request_bytes_and_response_sse() {
        use axum::{routing::post, Router};
        use tower::ServiceExt;
        async fn echo(req: Request) -> Response {
            let bytes = to_bytes(req.into_body(), usize::MAX).await.unwrap();
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream::iter([
                    Ok::<_, std::io::Error>(Bytes::from_static(b"data: ")),
                    Ok(bytes),
                    Ok(Bytes::from_static(b"\n\n")),
                ])))
                .unwrap()
        }
        let request =
            br#"{"method":"resources/read","params":{"uri":"ui://widget/ask_ocean.html"}}"#;
        for enabled in [false, true] {
            let app = Router::new()
                .route("/mcp", post(echo))
                .layer(axum::middleware::from_fn_with_state(enabled, observe));
            let body = Body::from_stream(stream::iter(
                request
                    .chunks(5)
                    .map(|c| Ok::<_, std::io::Error>(Bytes::copy_from_slice(c)))
                    .collect::<Vec<_>>(),
            ));
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/mcp")
                        .body(body)
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.headers()["content-type"], "text/event-stream");
            let actual = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(
                actual.as_ref(),
                [b"data: ".as_slice(), request.as_slice(), b"\n\n".as_slice()].concat()
            );
        }
    }
    #[tokio::test]
    async fn early_auth_rejection_never_polls_the_request_body() {
        use axum::{routing::post, Router};
        use tower::ServiceExt;
        let app = Router::new()
            .route(
                "/mcp",
                post(|| async { axum::http::StatusCode::UNAUTHORIZED }),
            )
            .layer(axum::middleware::from_fn_with_state(true, observe));
        let body = Body::from_stream(stream::poll_fn(
            |_| -> Poll<Option<Result<Bytes, std::io::Error>>> {
                panic!("auth-rejected request must not be read for diagnostics")
            },
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn real_transport_retains_modern_metadata_rejection_and_success_with_trace_enabled() {
        use super::super::server::{CoralSwarmMcp, LEGACY_WIDGET_RESOURCE_URI, WIDGET_MIME_TYPE};
        use super::super::test_support::test_auth;
        use axum::Router;
        use rmcp::transport::streamable_http_server::{
            session::never::NeverSessionManager, StreamableHttpServerConfig, StreamableHttpService,
        };
        use tower::ServiceExt;

        for enabled in [false, true] {
            let state = crate::test_app_state();
            let service = StreamableHttpService::new(
                move || Ok(CoralSwarmMcp::new(state.clone())),
                std::sync::Arc::new(NeverSessionManager::default()),
                StreamableHttpServerConfig::default()
                    .with_legacy_session_mode(false)
                    .with_json_response(true)
                    .disable_allowed_hosts(),
            );
            let app = Router::new()
                .route_service("/mcp", service)
                .layer(axum::middleware::from_fn(
                    super::super::header_backfill::backfill_mcp_headers,
                ))
                .layer(axum::Extension(test_auth(true, false)))
                .layer(axum::middleware::from_fn_with_state(enabled, observe));
            // Exercise the actual HTTP transport for every template advertised
            // by tools/list, plus the card retained for existing conversations.
            for (uri, html) in [
                (
                    "ui://coralswarm/ask-ocean-v2.html",
                    include_str!("../../assets/widgets/ask_ocean.html"),
                ),
                (
                    "ui://coralswarm/search-atoms-v2.html",
                    include_str!("../../assets/widgets/search_atoms.html"),
                ),
                (
                    "ui://coralswarm/reef-overview-v2.html",
                    include_str!("../../assets/widgets/get_reef.html"),
                ),
                (
                    "ui://coralswarm/recent-activity-v2.html",
                    include_str!("../../assets/widgets/recent_activity.html"),
                ),
                (
                    LEGACY_WIDGET_RESOURCE_URI,
                    include_str!("../../assets/widgets/result-card-v1.html"),
                ),
            ] {
                for has_meta in [false, true] {
                    let mut value = json!({"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":uri}});
                    if has_meta {
                        value["params"]["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}});
                    }
                    let bytes = serde_json::to_vec(&value).unwrap();
                    let chunks = bytes
                        .chunks(5)
                        .map(|c| Ok::<_, std::io::Error>(Bytes::copy_from_slice(c)))
                        .collect::<Vec<_>>();
                    let req = Request::builder()
                        .method("POST")
                        .uri("/mcp")
                        .header("host", "localhost")
                        .header("content-type", "application/json")
                        .header("accept", "application/json, text/event-stream")
                        .header("mcp-protocol-version", "2026-07-28")
                        .body(Body::from_stream(stream::iter(chunks)))
                        .unwrap();
                    let resp = app.clone().oneshot(req).await.unwrap();
                    assert_eq!(
                        resp.status(),
                        if has_meta {
                            axum::http::StatusCode::OK
                        } else {
                            axum::http::StatusCode::BAD_REQUEST
                        }
                    );
                    let is_sse = resp
                        .headers()
                        .get("content-type")
                        .and_then(|v| v.to_str().ok())
                        .is_some_and(|v| v.starts_with("text/event-stream"));
                    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
                    let body: Value = if is_sse {
                        let text = std::str::from_utf8(&bytes).unwrap();
                        let data = text
                            .lines()
                            .find_map(|l| {
                                l.strip_prefix("data: ").or_else(|| l.strip_prefix("data:"))
                            })
                            .unwrap();
                        serde_json::from_str(data).unwrap()
                    } else {
                        serde_json::from_slice(&bytes).unwrap()
                    };
                    if has_meta {
                        assert_eq!(body["result"]["resultType"], "complete");
                        assert_eq!(body["result"]["ttlMs"], 0);
                        assert_eq!(body["result"]["cacheScope"], "private");
                        assert_eq!(body["result"]["contents"][0]["uri"], uri);
                        assert_eq!(body["result"]["contents"][0]["mimeType"], WIDGET_MIME_TYPE);
                        assert_eq!(body["result"]["contents"][0]["text"], html);
                        let meta = &body["result"]["contents"][0]["_meta"];
                        let resource_domains = if uri == LEGACY_WIDGET_RESOURCE_URI {
                            json!([])
                        } else {
                            json!(["https://fonts.googleapis.com", "https://fonts.gstatic.com"])
                        };
                        assert_eq!(meta["ui"]["csp"]["resourceDomains"], resource_domains);
                        assert_eq!(meta["ui"]["csp"]["connectDomains"], json!([]));
                        assert_eq!(
                            meta["openai/widgetCSP"]["resource_domains"],
                            resource_domains
                        );
                        assert_eq!(meta["openai/widgetCSP"]["connect_domains"], json!([]));
                        assert_eq!(
                            meta["openai/widgetCSP"]["redirect_domains"],
                            if uri == LEGACY_WIDGET_RESOURCE_URI {
                                json!([])
                            } else {
                                json!(["https://app.coralswarm.example"])
                            }
                        );
                    } else {
                        assert_eq!(body["error"]["code"], -32602);
                    }
                }
            }
        }
    }
}
