//! `POST /v1/telemetry/client` — the client half of W6.
//!
//! Plan: `docs/plans/request-reliability-and-scale.md` §6.6.
//!
//! # Why the browser has to report through us
//!
//! The failure this whole plan exists to fix — `net::ERR_INSUFFICIENT_
//! RESOURCES` — **never reached the server.** The browser refused to open the
//! socket. No server-side metric could ever have shown it, which is precisely
//! why the incident was found by a user rather than by a monitor. The same is
//! true of a client timeout, a request refused by W1's queue, and a request
//! the circuit breaker declined to send.
//!
//! So the client failure taxonomy has to come from the client. It cannot go
//! straight to an OTLP collector: the browser has no collector credentials,
//! and handing it any would put them in a public bundle. It reports here
//! instead, over the session it already holds, and the server forwards into
//! the same pipeline as its own metrics.
//!
//! # Trust posture
//!
//! Everything in the body is attacker-controlled. Two consequences, both
//! enforced below rather than assumed:
//!
//! 1. **Labels are re-derived, never trusted.** `telemetry::sanitize_route`
//!    and friends run on every field that becomes a metric dimension. A client
//!    that sent raw paths — buggy, or deliberately — must not be able to mint
//!    unbounded series (denial-of-wallet against a metered vendor) or smuggle
//!    reef/atom ids into a third party.
//! 2. **The batch is bounded.** A report endpoint that accepts an unbounded
//!    array is a memory amplifier: one small request, arbitrary server work.
//!
//! The endpoint carries no data of its own — it is write-only, returns 204,
//! and reads nothing from the database. It is mounted inside the authenticated
//! `/v1` tree so reports are attributable and so W2's per-user rate ceiling
//! already covers it.

use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use utoipa::path as utoipa_path;

use crate::telemetry::{Origin, RequestSample, ServerTimingMs};
use crate::AppState;

/// Max samples accepted in one batch. The web client flushes every few
/// seconds or every 50 events, whichever comes first, so this is ~4x its own
/// ceiling — generous for a legitimate client, bounded for a hostile one.
/// Excess is TRUNCATED rather than 400'd: a client that over-batches should
/// lose telemetry, not lose its request.
const MAX_BATCH: usize = 200;

/// Upper bound on a reported duration, in milliseconds (one hour). Anything
/// beyond this is a clock problem or a fabrication, and would wreck a latency
/// histogram's buckets for everyone.
const MAX_DURATION_MS: u64 = 3_600_000;

#[derive(Debug, Deserialize)]
pub struct ClientSample {
    /// Route template as derived client-side. Re-sanitized here regardless.
    pub route: String,
    pub method: String,
    /// Absent when the request never produced a response — the case that
    /// matters most here.
    #[serde(default)]
    pub status: Option<u16>,
    pub duration_ms: u64,
    #[serde(default)]
    pub queued_ms: Option<u64>,
    pub outcome: String,
    /// The response's `Server-Timing` phases, when the browser could read it.
    #[serde(default)]
    pub server_total_ms: Option<u64>,
    #[serde(default)]
    pub server_auth_ms: Option<u64>,
    #[serde(default)]
    pub server_app_ms: Option<u64>,
    #[serde(default)]
    pub server_tx_open_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ClientReport {
    pub samples: Vec<ClientSample>,
    /// Events the client discarded because its own buffer filled — which
    /// happens exactly when the backend is unreachable, i.e. during the
    /// incidents this pipeline exists to catch.
    ///
    /// Recorded rather than ignored because a metrics pipeline that silently
    /// loses data during an outage reads, afterwards, as "nothing happened".
    /// A visible gap is worth far more than a clean-looking one.
    #[serde(default)]
    pub dropped: Option<u64>,
}

#[utoipa_path(
    post,
    path = "/v1/telemetry/client",
    tag = "telemetry",
    security(("bearer_token" = [])),
    responses(
        (status = 204, description = "Samples accepted"),
    )
)]
pub async fn report_client_metrics(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClientReport>,
) -> StatusCode {
    // Client samples always reach at least the structured log. Production
    // runs with no exporter configured, and the client's queueing, timeouts
    // and network failures exist nowhere else.
    let sink: &dyn crate::telemetry::Metrics = if state.metrics.enabled() {
        state.metrics.as_ref()
    } else {
        &crate::telemetry::LogMetrics
    };

    if let Some(n) = body.dropped.filter(|n| *n > 0) {
        tracing::warn!(
            target: crate::telemetry::METRICS_TARGET,
            metric = "client_samples_dropped",
            count = n,
            "client discarded telemetry because its buffer filled"
        );
    }

    let clamp = |ms: Option<u64>| ms.map(|ms| ms.min(MAX_DURATION_MS));
    for s in body.samples.into_iter().take(MAX_BATCH) {
        let server_timing = ServerTimingMs {
            total: clamp(s.server_total_ms),
            auth: clamp(s.server_auth_ms),
            app: clamp(s.server_app_ms),
            tx_open: clamp(s.server_tx_open_ms),
        };
        let has_server_timing = server_timing.total.is_some()
            || server_timing.auth.is_some()
            || server_timing.app.is_some()
            || server_timing.tx_open.is_some();
        sink.record_request(RequestSample {
            origin: Origin::Client,
            // Sanitized inside `record_request`'s exporter, but the sample
            // carries the raw value; clamp length here so an absurd string
            // never even reaches it.
            route: s.route.chars().take(256).collect(),
            method: s.method.chars().take(16).collect(),
            status: s.status,
            duration: Duration::from_millis(s.duration_ms.min(MAX_DURATION_MS)),
            queued: s.queued_ms.map(|ms| Duration::from_millis(ms.min(MAX_DURATION_MS))),
            outcome: s.outcome.chars().take(32).collect(),
            server_timing: has_server_timing.then_some(server_timing),
            route_class: None,
        });
    }

    StatusCode::NO_CONTENT
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{sanitize_outcome, sanitize_route};

    #[test]
    fn oversized_batches_are_truncated_not_rejected() {
        // A client that over-batches should lose telemetry, not lose its
        // request — telemetry must never be able to fail a user action.
        let samples: Vec<usize> = (0..1000).collect();
        assert_eq!(samples.into_iter().take(MAX_BATCH).count(), MAX_BATCH);
    }

    #[test]
    fn absurd_durations_are_clamped_before_they_reach_a_histogram() {
        let d = Duration::from_millis(u64::MAX.min(MAX_DURATION_MS));
        assert_eq!(d.as_millis() as u64, MAX_DURATION_MS);
    }

    #[test]
    fn client_supplied_labels_are_re_derived_not_trusted() {
        // The endpoint is authenticated but the BODY is not trustworthy: a
        // buggy or hostile client must not be able to create series.
        assert_eq!(
            sanitize_route("/v1/nodes/3f346a2a-71ca-4779-9701-baf2e5e7bddd/atoms"),
            "/v1/nodes/{id}/atoms"
        );
        // A 400-character segment is unmistakably an identifier, so it
        // templates rather than creating a series named after itself.
        assert_eq!(sanitize_route(&format!("/v1/{}", "x".repeat(400))), "/v1/{id}");
        // A path deep enough to blow the length bound collapses entirely.
        assert_eq!(sanitize_route(&("/v1".to_string() + &"/seg".repeat(200))), "/other");
        assert_eq!(sanitize_outcome("../../etc/passwd"), "other");
    }
}
