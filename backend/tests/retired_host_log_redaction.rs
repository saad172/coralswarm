//! **The retired-address WARN must never log a share token.**
//!
//! Claim-based tenancy Phase 4 cutover step 3 added `retired_org_host_guard`
//! (`lib.rs`), which logs the Host and path of every request to the retired org
//! API address so an old client can be identified and moved to the apex. That
//! guard runs BEFORE routing, so it logs whatever path the client sent — and
//! `app()`'s open routes include two where the path segment IS the entire
//! credential:
//!
//!   * `GET /v1/shared/{token}`           (`routes::shares`, unauthenticated)
//!   * `GET /v1/shared/sessions/{token}`  (`routes::session_shares`, ditto)
//!
//! The codebase already made this decision one layer out: `redact_share_token`
//! exists for exactly this, and `request_id_and_latency`'s access log — which
//! this guard runs INSIDE — routes the same path through it. Without the same
//! treatment here, one request produced one redacted line and one raw line,
//! three lines apart in the same file. (CodeRabbit, PR #807.)
//!
//! ## Why this is its own test binary
//!
//! `tracing` caches per-callsite `Interest` **process-globally**, and the
//! default subscriber is a **thread-local**. Asserting on captured log output
//! from inside `backend/src/lib.rs`'s unit-test module is therefore racy: when
//! the rest of the `--lib` suite runs in parallel, another test's subscriber
//! can register interest for this callsite first, after which this thread's
//! capturing subscriber is never consulted and the buffer comes back empty.
//!
//! That was measured, not assumed. Three successive shapes were tried in the
//! full `cargo test -p coralswarm-backend --lib` run:
//!
//!   * `#[tokio::test]` + `set_default`            — passed alone, failed in-suite
//!   * `#[test]` + `with_default` + `block_on`     — still failed ~1 run in 3
//!   * ...plus `rebuild_interest_cache()`          — still failed 1 run in 5
//!
//! The race is between the rebuild and the emit, and no amount of local care
//! closes it while another thread can invalidate the cache. An integration test
//! is its own process with its own harness, and this is the only test in it, so
//! there is no second subscriber to lose to. Deterministic by construction
//! rather than by hoping.
//!
//! **A flaky assertion about a credential leak is worse than none**: it trains
//! people to re-run it. The anti-vacuity assertion below is what turned this
//! into a visible flake instead of a silently empty buffer, and it is the first
//! assertion for that reason.
//!
//! No database is required — the guard answers before anything reaches a route.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use tower::ServiceExt;

const APEX: &str = "api.coralswarm.example";
const RETIRED_HOST: &str = "acme.api.coralswarm.example";
/// A value that appears nowhere else, so `!contains` is a real statement about
/// this request rather than about the alphabet.
const SECRET: &str = "sharetok-6f2b9c1e-do-not-log-me";

/// A `tracing` writer that keeps every emitted line in memory, so the test can
/// assert on what the log line ACTUALLY contains rather than on what the source
/// appears to format. The property here is negative — a secret must not appear
/// — and a negative property checked against a format string is satisfied by
/// any later edit that reintroduces the secret.
#[derive(Clone)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn the_retired_address_warn_never_logs_a_share_token() {
    let buf = CapturedLogs(Arc::new(Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();

    let status = tracing::subscriber::with_default(subscriber, || {
        let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
        state.tenant_runtime = Some(TenantRuntime::new(
            true,
            APEX,
            Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::new(
                Default::default(),
            )))),
        ));
        let app = coralswarm_backend::test_support_app(Arc::new(state));

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                app.oneshot(
                    HttpRequest::builder()
                        .method("GET")
                        .uri(format!("/v1/shared/{SECRET}"))
                        .header("host", RETIRED_HOST)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
            })
    });

    assert_eq!(
        status,
        StatusCode::GONE,
        "fixture sanity: this request must actually reach the guard, or every log \
         assertion below is about a line that was never emitted"
    );

    let logged = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();

    // ── Anti-vacuity, FIRST and deliberately.
    //
    // An empty buffer satisfies every `!contains(SECRET)` assertion below for
    // free. Prove the line exists before believing anything about what is
    // missing from it.
    assert!(
        logged.contains("retired org API address"),
        "no retired-address WARN was captured at all, so the redaction assertions below \
         would pass against an empty buffer: {logged:?}"
    );
    assert!(
        logged.contains(RETIRED_HOST),
        "the WARN must still name the Host — redaction must not cost the diagnostic the \
         line exists for: {logged:?}"
    );

    // ── The property.
    assert!(
        !logged.contains(SECRET),
        "the retired-address WARN logged a share token verbatim. That path segment is the \
         entire credential for an unauthenticated route; route it through \
         `redact_share_token`, exactly as `request_id_and_latency` already does: {logged:?}"
    );

    // ── ...and the obvious over-correction is refused too. Deleting the field
    //    would fix the leak by destroying the diagnostic the line exists for.
    assert!(
        logged.contains("/v1/shared/{token}"),
        "the WARN must still show WHICH route was hit, in redacted form: {logged:?}"
    );
}
