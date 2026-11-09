//! W6 — metrics, SLIs, and the client failure taxonomy.
//!
//! Plan: `docs/plans/request-reliability-and-scale.md` §6.6.
//!
//! # Why this is Phase 1 and not later
//!
//! Nothing else in that plan can be *operated* without it. Today the backend
//! emits JSON `tracing` to stdout and nothing else: no metrics, no APM, no
//! error reporting. That means **no SLI can be computed and no alert can
//! fire** — the incident that motivated this work was found by a user, not by
//! a monitor. It also means W2's admission control, which ships observe-only
//! precisely so its ceilings can be set from real data, has nowhere to put
//! that data. W2 without W6 is a limiter that never graduates.
//!
//! # Instrument once, choose the backend later
//!
//! Decision §12-G: vendor-neutral, backend chosen later, start on a free
//! tier. The load-bearing half of that decision is *"swapping backends must
//! never require re-instrumenting"* — so call sites talk to [`Metrics`], an
//! internal facade, and never to an exporter SDK. Today the only
//! implementation is [`OtlpMetrics`]; adding Prometheus, or swapping vendors,
//! is a new impl and a config value, not an edit to every call site.
//!
//! # Inert unless configured
//!
//! With `OTEL_EXPORTER_OTLP_ENDPOINT` unset, [`init`] returns [`NoopMetrics`]:
//! no background task, no allocation per request, no network. That is the
//! default for tests and for any deploy that has not chosen a
//! backend yet — so this can land and sit dormant rather than becoming a
//! deploy-blocking dependency.
//!
//! # Cardinality is a correctness property, not a cost concern
//!
//! Every label here is a closed or bounded set: route TEMPLATES (never
//! concrete paths), methods, status classes, admission layers and buckets.
//! An id in a metric label is two bugs at once — it explodes series count,
//! and it leaks reef/atom/user identifiers into a third-party observability
//! vendor (plan §10). The client-side half of this pipeline templates paths
//! *before* they leave the browser for exactly that reason; the server
//! re-validates rather than trusting it (see [`sanitize_route`]).

use std::sync::Arc;
use std::time::Duration;

// ── The facade ───────────────────────────────────────────────────────────────

/// Where a measurement came from. Server-side and client-side requests are
/// different populations — a server-side p95 says nothing about what the user
/// experienced if the browser queued the request for four seconds first — so
/// they are never mixed into one series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Server,
    Client,
}

impl Origin {
    fn as_str(self) -> &'static str {
        match self {
            Origin::Server => "server",
            Origin::Client => "client",
        }
    }
}

/// One completed request, server- or client-side.
#[derive(Debug, Clone)]
pub struct RequestSample {
    pub origin: Origin,
    /// Route template, e.g. `/v1/nodes/{id}/atoms`. Never a concrete path.
    pub route: String,
    pub method: String,
    /// `None` when the request never produced a response (network failure,
    /// timeout, refused before dispatch). Distinguishing that from a 500 is
    /// the entire point of the client failure taxonomy: `ERR_INSUFFICIENT_
    /// RESOURCES` never reached the server, so no server-side metric could
    /// ever have shown it.
    pub status: Option<u16>,
    pub duration: Duration,
    /// Client-side only: time spent waiting for a transport slot before the
    /// request was dispatched. Reported separately from `duration` so a
    /// queueing problem is never mistaken for a slow server.
    pub queued: Option<Duration>,
    /// Taxonomy bucket: `ok`, `http-error`, `timeout`, `queue-timeout`,
    /// `queue-full`, `breaker-open`, `aborted`, `network`.
    pub outcome: String,
    /// Client-side only: the response's `Server-Timing` as the browser read
    /// it, so a slow client sample can be split into server and network time.
    pub server_timing: Option<ServerTimingMs>,
    /// Server-side only: the route class (`page_read` | `stream` | `other`,
    /// `crate::route_class`). `None` on client samples, which report the
    /// route they called and not the budget it ran under.
    pub route_class: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ServerTimingMs {
    pub total: Option<u64>,
    pub auth: Option<u64>,
    pub app: Option<u64>,
    pub tx_open: Option<u64>,
}

/// An admission-control decision (W2). Recorded whether or not it was
/// enforced — observe-only mode produces exactly these, and reading a week of
/// them is how the ceilings get set.
#[derive(Debug, Clone)]
pub struct AdmissionSample {
    /// `rate` | `concurrency`
    pub layer: &'static str,
    /// `user` | `tenant` | `pool`
    pub bucket: &'static str,
    /// False in observe-only mode: the limit was exceeded but the request was
    /// served anyway. Splitting on this is what makes "what would we have
    /// rejected last week" answerable.
    pub enforced: bool,
    /// The route class the decision was made for. `Other` is the class the
    /// `CONCURRENCY_ENFORCE` flag governs, so a week of these samples split
    /// by class is what decides whether to flip it (design §4.4).
    pub route_class: &'static str,
}

/// Connection-pool health. `acquire_wait` and `acquire_timeouts` are the two
/// that actually predict an outage; `active` alone does not, because a pool
/// can be fully busy and perfectly healthy.
#[derive(Debug, Clone)]
pub struct PoolSample {
    /// `control`, `org:{clerk_org_id}:work` or `org:{clerk_org_id}:page` —
    /// the same keys W2's in-flight registry uses, so the two can be joined
    /// in a dashboard.
    pub pool: String,
    pub size: u32,
    pub idle: u32,
    pub max: u32,
}

/// The instrumentation surface. Call sites depend on this trait, never on an
/// exporter.
pub trait Metrics: Send + Sync {
    fn record_request(&self, sample: RequestSample);
    fn record_admission(&self, sample: AdmissionSample);
    fn record_pool(&self, sample: PoolSample);
    /// True when something is actually exporting. Lets a caller skip building
    /// a sample it would only throw away.
    fn enabled(&self) -> bool {
        true
    }
}

/// The default. Genuinely free: no task, no channel, no allocation.
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn record_request(&self, _: RequestSample) {}
    fn record_admission(&self, _: AdmissionSample) {}
    fn record_pool(&self, _: PoolSample) {}
    fn enabled(&self) -> bool {
        false
    }
}

// ── Label hygiene ────────────────────────────────────────────────────────────

/// Longest route template accepted. Anything longer is not a route.
const MAX_ROUTE_LEN: usize = 128;

/// Reduce a path to a bounded-cardinality route template, and refuse anything
/// that still looks like an identifier.
///
/// **The client already does this before sending** (`web/lib/transport.ts`'s
/// `routeTemplate`). This is not redundancy — it is the trust boundary. The
/// client-side sample arrives over an HTTP endpoint that a caller controls, so
/// a compromised or buggy client could otherwise mint unbounded series names
/// (a cheap denial-of-wallet against a metered observability vendor) or
/// smuggle identifiers into a third party. Never trust a label you did not
/// derive.
pub fn sanitize_route(raw: &str) -> String {
    let path = raw.split('?').next().unwrap_or("");
    if path.is_empty() || !path.starts_with('/') {
        return "/other".to_string();
    }
    let mut out = String::with_capacity(path.len().min(MAX_ROUTE_LEN));
    // Tracks whether the PRECEDING segment was the literal `source-groups`
    // (`routes::graphs::get_source_group`'s path param). A Slack channel_key
    // (`C0123ABC`) or a Teams one (`19:abc@thread.tacv2`) is exactly the kind
    // of short, punctuation-bearing, caller-chosen value this function exists
    // to keep out of a label (see the doc comment above) — but it's often
    // NOT shaped like a uuid/all-digits/24+-char opaque token, so
    // `is_identifier_like` alone would let many of them straight through as
    // literal segments. Forcing the segment after `source-groups` closes
    // that gap by position instead of by (unreliable) shape.
    let mut prev_was_source_groups = false;
    for seg in path.split('/') {
        if seg.is_empty() {
            continue;
        }
        out.push('/');
        if prev_was_source_groups {
            out.push_str("{key}");
        } else if is_identifier_like(seg) {
            out.push_str("{id}");
        } else {
            // Bound the segment too: a 100-character "route word" is not one.
            out.push_str(&seg.chars().take(32).collect::<String>());
        }
        prev_was_source_groups = seg == "source-groups";
        if out.len() > MAX_ROUTE_LEN {
            return "/other".to_string();
        }
    }
    if out.is_empty() {
        "/".to_string()
    } else {
        out
    }
}

fn is_identifier_like(seg: &str) -> bool {
    // UUID shape.
    if seg.len() == 36 && seg.as_bytes().iter().filter(|b| **b == b'-').count() == 4 {
        return true;
    }
    // All digits.
    if seg.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    // Long opaque token (session ids, Clerk ids, base64 blobs).
    if seg.len() > 24 {
        return true;
    }
    false
}

/// Status classes, not statuses: `2xx`/`4xx`/`5xx` plus `none` for a request
/// that never got a response. Keeps the series count flat while preserving
/// every distinction an SLI or an alert actually makes.
pub fn status_class(status: Option<u16>) -> &'static str {
    match status {
        None => "none",
        Some(s) if s < 200 => "1xx",
        Some(s) if s < 300 => "2xx",
        Some(s) if s < 400 => "3xx",
        Some(s) if s < 500 => "4xx",
        _ => "5xx",
    }
}

/// The client-reported outcome taxonomy. An unknown value collapses to
/// `other` rather than becoming a new series — same trust-boundary reasoning
/// as [`sanitize_route`].
pub fn sanitize_outcome(raw: &str) -> &'static str {
    match raw {
        "ok" => "ok",
        "http-error" => "http-error",
        "timeout" => "timeout",
        "queue-timeout" => "queue-timeout",
        "queue-full" => "queue-full",
        "breaker-open" => "breaker-open",
        "aborted" => "aborted",
        // The signal that was missing during the incident: a browser-level
        // resource failure that never reached the server at all.
        "network" => "network",
        _ => "other",
    }
}

pub fn sanitize_method(raw: &str) -> &'static str {
    match raw {
        "GET" => "GET",
        "POST" => "POST",
        "PATCH" => "PATCH",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "other",
    }
}

// ── Structured-log exporter ──────────────────────────────────────────────────

/// Emits each sample as a structured `tracing` event on a dedicated target.
///
/// This is the "start on a free tier" step made concrete: the deploy already
/// ships JSON `tracing` to stdout, and Fly already collects stdout, so this
/// makes every SLI in §6.6 computable **today** — from logs, with no vendor,
/// no new dependency, and no cost — while the OTLP decision is made. Because
/// call sites talk to [`Metrics`] rather than to this, replacing it with a
/// real OTLP exporter changes one constructor.
///
/// The dedicated `target` (`coralswarm::metrics`) is what makes this usable:
/// it can be routed, sampled, or dropped independently of application logs
/// via the existing `RUST_LOG` filter, so turning metrics on does not mean
/// drowning the application's own logs.
pub struct LogMetrics;

pub const METRICS_TARGET: &str = "coralswarm::metrics";

impl Metrics for LogMetrics {
    fn record_request(&self, s: RequestSample) {
        tracing::info!(
            target: METRICS_TARGET,
            metric = "http_request",
            origin = s.origin.as_str(),
            route = %sanitize_route(&s.route),
            method = sanitize_method(&s.method),
            status_class = status_class(s.status),
            outcome = sanitize_outcome(&s.outcome),
            duration_ms = s.duration.as_millis() as u64,
            queued_ms = s.queued.map(|d| d.as_millis() as u64),
            server_total_ms = s.server_timing.and_then(|t| t.total),
            server_auth_ms = s.server_timing.and_then(|t| t.auth),
            server_app_ms = s.server_timing.and_then(|t| t.app),
            server_tx_open_ms = s.server_timing.and_then(|t| t.tx_open),
            route_class = s.route_class,
        );
    }

    fn record_admission(&self, s: AdmissionSample) {
        tracing::info!(
            target: METRICS_TARGET,
            metric = "admission_decision",
            layer = s.layer,
            bucket = s.bucket,
            enforced = s.enforced,
            route_class = s.route_class,
        );
    }

    fn record_pool(&self, s: PoolSample) {
        tracing::info!(
            target: METRICS_TARGET,
            metric = "db_pool",
            pool = %s.pool,
            size = s.size,
            idle = s.idle,
            max = s.max,
            // Pre-computed rather than left to the dashboard: this is the
            // number an alert fires on, and computing it in one place means
            // every consumer agrees on what "saturated" means.
            in_use = s.size.saturating_sub(s.idle),
        );
    }
}

// ── Construction ─────────────────────────────────────────────────────────────

/// Build the metrics sink from the environment.
///
/// `CORALSWARM_METRICS=log` enables the structured-log exporter. Anything else
/// — including unset — yields [`NoopMetrics`], so this is inert by default and
/// cannot become a deploy-blocking dependency on a vendor that has not been
/// chosen yet.
pub fn init() -> Arc<dyn Metrics> {
    match std::env::var("CORALSWARM_METRICS").ok().as_deref().map(str::trim) {
        Some("log") => {
            tracing::info!(
                target: METRICS_TARGET,
                "metrics enabled: structured-log exporter on target {METRICS_TARGET}"
            );
            Arc::new(LogMetrics)
        }
        _ => Arc::new(NoopMetrics),
    }
}

// ── Server-side request metrics middleware ───────────────────────────────────

/// RED metrics for every authenticated request.
///
/// **Layered between `require_auth` and W2's admission control**, which is the
/// only placement that records what actually happened: outside admission, so
/// a 429/503 it produced is counted as a real outcome rather than invisibly
/// dropped; inside auth, so an unauthenticated probe never creates series.
///
/// The route label comes from axum's own [`MatchedPath`] — the router already
/// knows the template it matched (`/v1/nodes/{node_id}/atoms`), so deriving it
/// again from the URI would be both redundant and less accurate. Falls back to
/// [`sanitize_route`] only when no route matched.
pub async fn http_metrics(
    axum::extract::State(state): axum::extract::State<Arc<crate::AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if !state.metrics.enabled() {
        // Skip building the sample entirely when nothing is exporting. The
        // point of the `enabled()` probe: no per-request cost when off.
        return next.run(req).await;
    }

    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| sanitize_route(req.uri().path()));
    let method = req.method().as_str().to_string();
    let route_class = crate::route_class::RouteClass::of(req.method(), req.extensions()).as_str();

    let started = std::time::Instant::now();
    let response = next.run(req).await;
    let status = response.status().as_u16();

    // STREAMING RESPONSES ARE NOT TIMED HERE, and recording them would be
    // worse than skipping them.
    //
    // `next.run` returns as soon as the response HEAD is ready. For
    // `POST /v1/queries/stream` that is the moment the SSE stream is handed
    // over — tokens keep flowing for seconds afterwards. Recording
    // `started.elapsed()` for those would put a near-zero duration into the
    // same histogram as ordinary reads, dragging the p95 DOWN for the single
    // longest-running path in the product. A latency dashboard that gets
    // *better* the slower ask becomes is actively misleading.
    //
    // So they are excluded until the stream itself is instrumented (an
    // end-of-stream hook, which belongs with the ask work rather than here).
    // §6.6's own SLI table already treats ask separately —
    // "time-to-first-token", explicitly excluded from the read-latency
    // target — so the omission matches the stated contract rather than
    // silently dropping a route.
    if is_streaming(&response) {
        return response;
    }

    state.metrics.record_request(RequestSample {
        origin: Origin::Server,
        route,
        method,
        status: Some(status),
        duration: started.elapsed(),
        queued: None,
        outcome: if response.status().is_success() {
            "ok".to_string()
        } else {
            "http-error".to_string()
        },
        server_timing: None,
        route_class: Some(route_class),
    });

    response
}

/// Is this an event-stream response whose body outlives its head?
fn is_streaming(response: &axum::response::Response) -> bool {
    response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream"))
}

// ── Pool sampling ────────────────────────────────────────────────────────────

/// Interval for the background pool sampler.
const POOL_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

/// Periodically record pool health: the control pool and both request lanes
/// of every tenant pool that is already built.
///
/// Gauges are sampled rather than event-driven because that is what they are:
/// asking "how full is the pool" at a point in time. Thirty seconds is chosen
/// against the alert this feeds (`acquire_timeout rate > 0`, plus sustained
/// saturation) — fast enough to catch a burst that lasts a minute, slow enough
/// that the sampler itself is free.
///
/// Tenant pools are read through `TenantRegistry::live_lane_stats`, which reports
/// only ALREADY-BUILT request pools: a dormant tenant is never woken to be
/// measured, and a tenant that was never touched by this process is simply
/// absent from the series. The page lane's `in_use` against its `max` is the
/// number the hourly check reads for lane saturation (design §4.3).
pub fn spawn_pool_sampler(state: Arc<crate::AppState>, shutdown: crate::shutdown::Shutdown) {
    if !state.metrics.enabled() {
        return;
    }
    if state.db.is_none() && state.tenant_runtime.is_none() {
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(POOL_SAMPLE_INTERVAL);
        // Skip the immediate first tick: `tokio::interval` fires at once, and
        // a pool sampled before anything has connected reports a misleading
        // zero.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if shutdown.is_triggered() {
                break;
            }
            for s in pool_samples(&state) {
                state.metrics.record_pool(s);
            }
        }
    });
}

/// One `PoolSample` per pool this process holds right now.
pub fn pool_samples(state: &crate::AppState) -> Vec<PoolSample> {
    use crate::tenancy::registry::PoolStats;
    let sample = |name: String, stats: PoolStats| PoolSample {
        pool: name,
        size: stats.size,
        idle: stats.idle,
        max: stats.max,
    };
    let mut out = Vec::new();
    if let Some(pool) = &state.db {
        out.push(sample("control".to_string(), PoolStats::of(pool)));
    }
    if let Some(rt) = &state.tenant_runtime {
        for (name, stats) in rt.registry.live_lane_stats() {
            out.push(sample(name, stats));
        }
    }
    out
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_are_templated_so_series_count_stays_bounded() {
        assert_eq!(
            sanitize_route("/v1/nodes/3f346a2a-71ca-4779-9701-baf2e5e7bddd/atoms"),
            "/v1/nodes/{id}/atoms"
        );
        assert_eq!(sanitize_route("/v1/meetings/42"), "/v1/meetings/{id}");
        assert_eq!(sanitize_route("/v1/graphs"), "/v1/graphs");
    }

    #[test]
    fn source_group_channel_key_is_always_templated() {
        // No trailing channel_key: the literal segment survives untouched.
        assert_eq!(
            sanitize_route("/v1/graphs/3f346a2a-71ca-4779-9701-baf2e5e7bddd/source-groups"),
            "/v1/graphs/{id}/source-groups"
        );
        // A Slack-shaped channel_key: short, alphanumeric — NOT a uuid, not
        // all digits, not a 24+-char opaque token, so `is_identifier_like`
        // would otherwise let it straight through as a literal label value.
        assert_eq!(
            sanitize_route("/v1/graphs/3f346a2a-71ca-4779-9701-baf2e5e7bddd/source-groups/C0123ABC"),
            "/v1/graphs/{id}/source-groups/{key}"
        );
        // A Teams-shaped channel_key: punctuation the shape heuristic alone
        // would never recognize as identifier-like either.
        assert_eq!(
            sanitize_route(
                "/v1/graphs/3f346a2a-71ca-4779-9701-baf2e5e7bddd/source-groups/19:abc@thread.tacv2"
            ),
            "/v1/graphs/{id}/source-groups/{key}"
        );
    }

    #[test]
    fn query_strings_never_become_labels() {
        // Query strings carry ocean ids and free-text search terms. Letting
        // them through would make this both unbounded and a content leak.
        assert_eq!(
            sanitize_route("/v1/nodes?graph_id=3f346a2a-71ca-4779-9701-baf2e5e7bddd&q=salary"),
            "/v1/nodes"
        );
    }

    #[test]
    fn a_hostile_client_cannot_mint_unbounded_series() {
        // The client templates before sending; this is the trust boundary
        // behind that. A caller controls the client-report endpoint, so an
        // untemplated or absurd path must not become a new series.
        //
        // Note what the right answer is here: a 500-character segment is
        // MORE obviously an identifier than a uuid, so it templates to
        // `{id}` and the series stays bounded. Collapsing to `/other` would
        // also be safe but would throw away a route we can still name.
        assert_eq!(sanitize_route(&format!("/v1/{}", "a".repeat(500))), "/v1/{id}");
        assert_eq!(sanitize_route("not-a-path"), "/other");
        assert_eq!(sanitize_route(""), "/other");
        // A long opaque segment is an id even if it is not a uuid.
        assert_eq!(
            sanitize_route("/v1/sessions/sess_2xKqPvB9nQwErTyUiOpAsDfGhJkL"),
            "/v1/sessions/{id}"
        );
        // Deep paths are bounded too.
        let deep = "/v1".to_string() + &"/seg".repeat(200);
        assert_eq!(sanitize_route(&deep), "/other");
    }

    #[test]
    fn route_words_survive_templating() {
        // Over-aggressive templating would collapse distinct routes into one
        // series, which is a worse failure than high cardinality: it silently
        // merges unrelated latency distributions.
        assert_eq!(
            sanitize_route("/v1/nodes/3f346a2a-71ca-4779-9701-baf2e5e7bddd/candidates"),
            "/v1/nodes/{id}/candidates"
        );
        assert_ne!(
            sanitize_route("/v1/nodes/{id}/atoms"),
            sanitize_route("/v1/nodes/{id}/candidates")
        );
    }

    #[test]
    fn status_none_is_its_own_class() {
        // The whole point of the client taxonomy: a request that never
        // reached the server cannot be represented as a 5xx, and
        // ERR_INSUFFICIENT_RESOURCES is exactly that case.
        assert_eq!(status_class(None), "none");
        assert_eq!(status_class(Some(200)), "2xx");
        assert_eq!(status_class(Some(404)), "4xx");
        assert_eq!(status_class(Some(503)), "5xx");
    }

    #[test]
    fn unknown_outcomes_collapse_rather_than_creating_series() {
        assert_eq!(sanitize_outcome("timeout"), "timeout");
        assert_eq!(sanitize_outcome("queue-full"), "queue-full");
        assert_eq!(sanitize_outcome("something-invented"), "other");
        assert_eq!(sanitize_outcome(""), "other");
        assert_eq!(sanitize_method("TRACE"), "other");
    }

    #[test]
    fn streaming_responses_are_excluded_from_the_latency_histogram() {
        use axum::response::IntoResponse;
        // A near-zero duration for the longest-running path would drag the
        // read-latency p95 DOWN as ask got slower — a dashboard that improves
        // when the product degrades.
        let sse = ([(axum::http::header::CONTENT_TYPE, "text/event-stream")], "data: x\n\n")
            .into_response();
        assert!(is_streaming(&sse));

        let json = ([(axum::http::header::CONTENT_TYPE, "application/json")], "{}").into_response();
        assert!(!is_streaming(&json));

        let bare = "hello".into_response();
        assert!(!is_streaming(&bare));
    }

    #[test]
    fn noop_is_the_default_and_reports_itself_disabled() {
        std::env::remove_var("CORALSWARM_METRICS");
        assert!(!init().enabled());
        std::env::set_var("CORALSWARM_METRICS", "log");
        assert!(init().enabled());
        std::env::set_var("CORALSWARM_METRICS", "otlp-someday");
        // An unrecognized value must not silently enable something.
        assert!(!init().enabled());
        std::env::remove_var("CORALSWARM_METRICS");
    }
}
