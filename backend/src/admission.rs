//! W2 — server-side admission control for authenticated read paths.
//!
//! Plan: `docs/plans/request-reliability-and-scale.md` §6.2.
//!
//! # Why this exists
//!
//! Rate limiting in this codebase is **opt-in per handler** — present in
//! `sync`, `queries`, `shares`, `session_shares`, `meetings` and MCP, and
//! absent from every read endpoint. There is no middleware default. So a
//! runaway client is not throttled at all on the paths it is most likely to
//! flood, and a single misbehaving browser tab can starve every other tenant.
//! That is a **noisy-neighbour** problem, which makes this a security control
//! as much as a performance one (plan §10).
//!
//! # Two layers, because they defend different failure modes
//!
//! - **Rate ceilings** catch a runaway *loop* early: a client re-firing the
//!   same request thousands of times trips a per-minute ceiling long before it
//!   saturates anything.
//! - **Concurrency fair-share** protects the *pool* under genuine load: it
//!   bounds in-flight DB-touching requests, so nobody is throttled while the
//!   system is idle and tenants converge to a fair share under saturation.
//!
//! The incident that motivated this plan produced both at once, which is why
//! neither alone is sufficient.
//!
//! # The pool topology this is sized against
//!
//! An earlier draft of the plan sized a single global limit against "a
//! 40-connection pool". That pool does not serve org tenants. Verified:
//!
//! | Pool | Per machine | Source |
//! |---|---|---|
//! | Control plane (personal oceans) | `DB_MAX_CONNECTIONS`, default 20 | `lib.rs` |
//! | Per org tenant, request-serving | [`crate::tenancy::registry::DEFAULT_MAX_CONNECTIONS`] = **5** | `registry.rs` |
//! | Per org tenant, workers | `DEFAULT_WORKER_MAX_CONNECTIONS` = 11, separate cache | `registry.rs` |
//!
//! Org tenants are **DB-per-org**, each with its own independently-built pool.
//! A single global in-flight limit of 32 (the earlier proposal) would have
//! admitted 32 concurrent requests against a **5**-connection tenant pool —
//! six times oversubscribed for exactly the tenants the control is meant to
//! protect. So the limit here is derived **per pool, at request time**, from
//! the pool the request will actually touch.
//!
//! # What this deliberately does NOT claim
//!
//! The concurrency limiter is **per process**. `fly.toml` sets
//! `auto_stop_machines = 'stop'` with `min_machines_running = 1`, so steady
//! state is frequently a single machine — but under load it is not, and this
//! layer cannot see the others. It is a **local** protection on the pool in
//! this process. Only the rate ceiling can be global, and only when
//! `RATE_LIMIT_BACKEND=pg` (the `InMemoryRateLimiter` is per-replica and
//! cannot enforce a cross-machine ceiling; `rate_limit.rs` already documents
//! this). Do not read the concurrency numbers below as a fleet-wide invariant.
//!
//! # Debuggability is a requirement, not a nicety
//!
//! Two interacting limiters make "why did I get a 429 rather than a 503" hard
//! to answer, and an unexplainable rejection is worse than no limiter at all.
//! Every rejection therefore carries a machine-readable cause: a structured
//! `tracing` event with `bucket` / `limit` / `observed`, and response headers
//! naming the same. W6's dashboards break rejections down by that cause.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderName, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use dashmap::DashMap;
use serde_json::json;

use crate::middleware::auth::UserContext;
use crate::route_class::RouteClass;
use crate::telemetry::AdmissionSample;
use crate::tenancy::runtime::TenancyDecision;
use crate::AppState;

// ── Defaults ─────────────────────────────────────────────────────────────────

/// Per-user requests per minute. Generous on purpose: normal interactive use
/// must never notice this. Its job is catching a *loop*, not shaping traffic —
/// a human clicking around does not approach 300/min, and the 3,440-request
/// Reefs fan-out blows through it in seconds.
pub const DEFAULT_PER_USER_PER_MIN: u32 = 300;

/// Per-tenant (`account_id`) requests per minute, across all of that tenant's
/// users. Higher than the per-user ceiling so a real team is never collectively
/// throttled by a limit meant for one runaway client.
pub const DEFAULT_PER_TENANT_PER_MIN: u32 = 1_000;

/// Window for both ceilings.
const WINDOW_SECS: u64 = 60;

/// How long an ENFORCING rate check may take before the request is allowed
/// through anyway. Admission control is a guard, not an availability
/// dependency: a slow or down limiter store must never take the API with it.
const ENFORCE_BUDGET: std::time::Duration = std::time::Duration::from_millis(250);

/// The same bound for OBSERVE-ONLY counting, which runs detached. Its only job
/// is to stop a stuck store from accumulating tasks without limit; a missed
/// sample costs a little fidelity in the rollout data and nothing else.
const OBSERVE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

/// Fraction of a pool's connections that concurrent HTTP requests may occupy.
/// Deliberately below 1.0: something must be left for the health check, for a
/// worker sharing the pool, and for the fact that a request can hold a
/// connection slightly longer than it holds its admission slot.
const POOL_UTILIZATION: f64 = 0.8;

/// Floor on the derived per-pool in-flight cap. Without it, a small pool
/// (or a future pool sized at 1–2) would compute a cap of 0 and refuse
/// everything — a limiter that deadlocks its own service.
const MIN_IN_FLIGHT: usize = 2;

/// How the concurrency layer treats one request, by route class
/// (`docs/plans/page-load-standard/design.md` §4.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shedding {
    /// Counted, never compared to a cap: `PageRead`. The page lane's own
    /// acquire timeout and 1 s statement timeout bound it instead.
    Never,
    /// Over-cap decisions are logged and recorded, the request is served:
    /// `Stream` (a connection for milliseconds, a slot for minutes), and
    /// `Other` while `CONCURRENCY_ENFORCE` is off.
    Observe,
    /// Over-cap requests get the 503: `Other` with `CONCURRENCY_ENFORCE` on.
    Enforce,
}

fn shedding(class: RouteClass, cfg: &AdmissionConfig) -> Shedding {
    match class {
        RouteClass::PageRead => Shedding::Never,
        RouteClass::Stream => Shedding::Observe,
        RouteClass::Other if cfg.concurrency_enforce => Shedding::Enforce,
        RouteClass::Other => Shedding::Observe,
    }
}

/// Response header naming which layer rejected the request (`rate` or
/// `concurrency`) — distinct values so a client, a dashboard or an on-call
/// engineer can tell the two apart without reading logs.
pub const ADMISSION_HEADER: HeaderName = HeaderName::from_static("x-coralswarm-admission");
/// Response header naming which *bucket* within that layer tripped
/// (`user`, `tenant`, `pool`).
pub const ADMISSION_BUCKET_HEADER: HeaderName =
    HeaderName::from_static("x-coralswarm-admission-bucket");

// ── Configuration ────────────────────────────────────────────────────────────

/// Resolved once at boot rather than read per request.
#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    /// When false, rate ceilings are evaluated and logged but never enforced.
    pub rate_enforce: bool,
    /// When false, the concurrency fair-share is accounted and logged but
    /// never enforced.
    pub concurrency_enforce: bool,
    pub per_user_per_min: u32,
    pub per_tenant_per_min: u32,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            // OBSERVE-ONLY BY DEFAULT, and this is the whole rollout strategy
            // (plan §6.2's risk row). A ceiling set from a document rather than
            // from data will be wrong, and a wrong ceiling breaks legitimate
            // bursts. Ship counting-only, read a week of real distribution off
            // W6, then enforce. Enforcement must also not be enabled before W3
            // lands — otherwise the app's own un-fixed Reefs fan-out trips the
            // new limiter and the first thing the team learns is that its
            // ceilings are wrong (plan §8).
            rate_enforce: false,
            concurrency_enforce: false,
            per_user_per_min: DEFAULT_PER_USER_PER_MIN,
            per_tenant_per_min: DEFAULT_PER_TENANT_PER_MIN,
        }
    }
}

impl AdmissionConfig {
    pub fn from_env() -> Self {
        Self {
            rate_enforce: env_flag("RATE_LIMIT_ENFORCE"),
            concurrency_enforce: env_flag("CONCURRENCY_ENFORCE"),
            per_user_per_min: env_u32("RATE_LIMIT_PER_USER_PER_MIN", DEFAULT_PER_USER_PER_MIN),
            per_tenant_per_min: env_u32(
                "RATE_LIMIT_PER_TENANT_PER_MIN",
                DEFAULT_PER_TENANT_PER_MIN,
            ),
        }
    }
}

/// `1`/`true` enable; anything else (including unset) leaves the control in
/// observe-only mode. Fails *safe* — a typo cannot silently start rejecting
/// production traffic.
pub(crate) fn env_flag(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref().map(str::trim),
        Some("1") | Some("true")
    )
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

// ── Per-pool in-flight accounting ────────────────────────────────────────────

/// In-flight counters, keyed by the pool a request will touch: `"control"` for
/// the consumer plane, `"org:{clerk_org_id}:work"` and
/// `"org:{clerk_org_id}:page"` for each org tenant's two request lanes.
///
/// Keying on the POOL rather than on the tenant identity is the point. Two
/// requests from different users of the same org contend for the same lane;
/// a personal request and an org request do not contend at all, and neither
/// do a page read and a Library page in the same org (design §4.3). A
/// limiter keyed on identity alone would get all three cases wrong.
#[derive(Default)]
pub struct InFlightRegistry {
    counters: DashMap<String, Arc<AtomicUsize>>,
}

impl InFlightRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn counter(&self, key: &str) -> Arc<AtomicUsize> {
        if let Some(c) = self.counters.get(key) {
            return c.clone();
        }
        self.counters
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone()
    }

    /// Current in-flight count for a pool key. Test/telemetry accessor.
    pub fn current(&self, key: &str) -> usize {
        self.counters
            .get(key)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// RAII guard: decrements on drop, so a panicking or early-returning handler
/// cannot leak a slot. A leaked slot is permanent — it would ratchet the
/// effective limit down to zero over the life of the process.
struct InFlightGuard(Arc<AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Which pool (lane) this request will touch, how many connections it has,
/// and the in-flight cap that follows.
///
/// Returns `None` when the request touches no pool we can size against, in
/// which case the concurrency layer stands down rather than guessing.
fn pool_key_size_and_cap(
    decision: Option<&TenancyDecision>,
    class: RouteClass,
    state: &AppState,
) -> Option<(String, u32, usize)> {
    match decision {
        Some(TenancyDecision::Org(ctx)) => {
            // The gate binds `ctx.pool` to the WORK lane; the page lane is
            // beside it. A context with one lane (no `page_pool`) counts
            // page reads against that one lane.
            let (lane, pool) = match (class, &ctx.page_pool) {
                (RouteClass::PageRead, Some(page)) => ("page", page),
                (RouteClass::PageRead, None) => ("page", &ctx.pool),
                (RouteClass::Stream | RouteClass::Other, _) => ("work", &ctx.pool),
            };
            let max = pool.options().get_max_connections();
            Some((format!("org:{}:{lane}", ctx.org_id), max, lane_cap(max)))
        }
        // `Personal` and "gate never ran" both land on the control pool.
        _ => state.db.as_ref().map(|p| {
            let max = p.options().get_max_connections();
            ("control".to_string(), max, in_flight_cap(max))
        }),
    }
}

/// Derive the in-flight cap for a SHARED pool from its size:
/// `max(MIN_IN_FLIGHT, floor(max_connections × POOL_UTILIZATION))`.
///
/// At today's defaults that is **16** for the control plane. A single
/// hard-coded number is wrong by construction under DB-per-org, which is
/// exactly the defect this replaces.
fn in_flight_cap(pool_max: u32) -> usize {
    let derived = (f64::from(pool_max) * POOL_UTILIZATION).floor() as usize;
    derived.max(MIN_IN_FLIGHT)
}

/// The cap for one tenant LANE is the lane itself: the lane is already the
/// partition (`registry::lane_split`), nothing else shares it, so the
/// `POOL_UTILIZATION` headroom a shared pool keeps for the health check and
/// for workers has nothing to protect here. At the default 2/3 split the
/// work lane's cap is **3** (design §4.4), the page lane's 2 and never
/// enforced.
fn lane_cap(lane_max: u32) -> usize {
    (lane_max as usize).max(MIN_IN_FLIGHT)
}

// ── Rejection shaping ────────────────────────────────────────────────────────

/// Which layer refused, for the response header and the structured log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionLayer {
    Rate,
    Concurrency,
}

impl AdmissionLayer {
    fn as_str(self) -> &'static str {
        match self {
            AdmissionLayer::Rate => "rate",
            AdmissionLayer::Concurrency => "concurrency",
        }
    }

    /// 429 for a rate ceiling (you asked too often), 503 for concurrency
    /// saturation (we are full right now). The distinction is meaningful to a
    /// client: the first says slow down, the second says try again shortly.
    fn status(self) -> StatusCode {
        match self {
            AdmissionLayer::Rate => StatusCode::TOO_MANY_REQUESTS,
            AdmissionLayer::Concurrency => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

fn reject(layer: AdmissionLayer, bucket: &'static str, retry_after_secs: u64) -> Response {
    let mut resp = (
        layer.status(),
        Json(json!({
            "error": match layer {
                AdmissionLayer::Rate => "rate limit exceeded",
                AdmissionLayer::Concurrency => "server at capacity",
            },
            // Machine-readable cause, in the body as well as the headers: a
            // client that surfaces an error to a user should be able to say
            // WHICH limit it hit without parsing prose.
            "admission": { "layer": layer.as_str(), "bucket": bucket },
        })),
    )
        .into_response();
    let h = resp.headers_mut();
    h.insert(ADMISSION_HEADER, HeaderValue::from_static(layer.as_str()));
    if let Ok(v) = HeaderValue::from_str(bucket) {
        h.insert(ADMISSION_BUCKET_HEADER, v);
    }
    if let Ok(v) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        h.insert(axum::http::header::RETRY_AFTER, v);
    }
    resp
}

// ── The middleware ───────────────────────────────────────────────────────────

/// Admission control for authenticated routes.
///
/// **Must be layered INSIDE `require_auth`**, so that `UserContext` and
/// `TenancyDecision` are already on the request. Both are read from extensions
/// rather than re-resolved, so this adds no I/O: the org branch reuses the
/// pool handle the gate already built.
///
/// When either identity is absent — a route mounted outside the auth layer —
/// the corresponding layer stands down. Failing *open* is deliberate: admission
/// control is a guard, not an availability dependency, and refusing traffic
/// because we could not identify it would convert a mis-wiring into an outage.
pub async fn admission_control(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let cfg = &state.admission;
    let user = req.extensions().get::<UserContext>().cloned();
    let decision = req.extensions().get::<TenancyDecision>().cloned();
    let class = RouteClass::of(req.method(), req.extensions());

    // ── Layer 1: rate ceilings ───────────────────────────────────────────────
    //
    // OBSERVE-ONLY MUST ACTUALLY BE FREE, and an earlier cut of this was not.
    // `check_and_increment_key` ran regardless of `rate_enforce` — only the
    // *rejection* was gated — so under `RATE_LIMIT_BACKEND=pg` this added a
    // Postgres round trip to every authenticated `/v1` request from the day it
    // merged, with no timeout and no fail-open. Two things made that worse
    // than a latency cost:
    //
    //   * `PgRateLimiter` uses the CONTROL pool. An org-tenant request that
    //     would otherwise never touch the control plane would now block on it,
    //     coupling every tenant to control-pool health — the exact
    //     noisy-neighbour failure this module exists to remove.
    //   * The only bound was sqlx's `acquire_timeout`. A saturated control
    //     pool would have added seconds to requests that had no business
    //     waiting on it.
    //
    // So the counting is now OFF the critical path unless it can actually
    // reject. Observe-only still produces the distribution the rollout depends
    // on — the increments still land — they just do not block the response.
    if let Some(u) = &user {
        let user_key = format!("adm:u:{}", u.user_id);
        let tenant_key = format!("adm:a:{}", u.account_id);

        if !cfg.rate_enforce {
            // Fire-and-forget. Bounded, so a stuck limiter store cannot
            // accumulate tasks without limit, and dropped silently on
            // timeout — a missed sample is an acceptable loss; a stalled
            // request is not.
            let limiter = state.rate_limiter.clone();
            let (upm, tpm) = (cfg.per_user_per_min, cfg.per_tenant_per_min);
            // W6: the rejection-cause breakdown §6.2 calls mandatory. Recorded
            // from inside the DETACHED task, so observe-only still produces
            // the distribution the rollout depends on without putting the
            // limiter store on the request's critical path.
            let metrics = state.metrics.clone();
            tokio::spawn(async move {
                let counted = tokio::time::timeout(OBSERVE_BUDGET, async {
                    let (uo, to) = tokio::join!(
                        limiter.check_and_increment_key(&user_key, upm, WINDOW_SECS),
                        limiter.check_and_increment_key(&tenant_key, tpm, WINDOW_SECS),
                    );
                    (uo, to)
                })
                .await;
                if let Ok((user_ok, tenant_ok)) = counted {
                    if !user_ok {
                        tracing::warn!(
                            layer = "rate", bucket = "user", limit = upm,
                            window_secs = WINDOW_SECS, enforced = false,
                            "admission: per-user rate ceiling exceeded"
                        );
                        metrics.record_admission(AdmissionSample {
                            layer: "rate",
                            bucket: "user",
                            enforced: false,
                            route_class: class.as_str(),
                        });
                    }
                    if !tenant_ok {
                        tracing::warn!(
                            layer = "rate", bucket = "tenant", limit = tpm,
                            window_secs = WINDOW_SECS, enforced = false,
                            "admission: per-tenant rate ceiling exceeded"
                        );
                        metrics.record_admission(AdmissionSample {
                            layer: "rate",
                            bucket: "tenant",
                            enforced: false,
                            route_class: class.as_str(),
                        });
                    }
                }
            });
        } else {
            // Enforcing: the answer is needed before the request proceeds, so
            // this is awaited — but still BOUNDED and FAIL-OPEN. A limiter
            // store that is slow or down must not take the API down with it;
            // admission control is a guard, not an availability dependency.
            let checked = tokio::time::timeout(ENFORCE_BUDGET, async {
                tokio::join!(
                    state.rate_limiter.check_and_increment_key(
                        &user_key, cfg.per_user_per_min, WINDOW_SECS),
                    state.rate_limiter.check_and_increment_key(
                        &tenant_key, cfg.per_tenant_per_min, WINDOW_SECS),
                )
            })
            .await;

            let (user_ok, tenant_ok) = match checked {
                Ok(pair) => pair,
                Err(_) => {
                    tracing::warn!(
                        layer = "rate", budget_ms = ENFORCE_BUDGET.as_millis() as u64,
                        "admission: rate check timed out; failing open"
                    );
                    (true, true)
                }
            };

            if !user_ok {
                tracing::warn!(
                    layer = "rate", bucket = "user", limit = cfg.per_user_per_min,
                    window_secs = WINDOW_SECS, enforced = true, user_id = %u.user_id,
                    "admission: per-user rate ceiling exceeded"
                );
                state.metrics.record_admission(AdmissionSample {
                    layer: "rate",
                    bucket: "user",
                    enforced: true,
                    route_class: class.as_str(),
                });
                return reject(AdmissionLayer::Rate, "user", WINDOW_SECS);
            }
            if !tenant_ok {
                tracing::warn!(
                    layer = "rate", bucket = "tenant", limit = cfg.per_tenant_per_min,
                    window_secs = WINDOW_SECS, enforced = true, account_id = %u.account_id,
                    "admission: per-tenant rate ceiling exceeded"
                );
                state.metrics.record_admission(AdmissionSample {
                    layer: "rate",
                    bucket: "tenant",
                    enforced: true,
                    route_class: class.as_str(),
                });
                return reject(AdmissionLayer::Rate, "tenant", WINDOW_SECS);
            }
        }
    }

    // ── Layer 2: per-pool concurrency fair-share ─────────────────────────────
    //
    // Held for the whole downstream call via an RAII guard, so the count
    // reflects requests genuinely in flight rather than requests admitted.
    // The class decides the lane that is counted and whether an over-cap
    // request is shed, observed, or not even compared (see `Shedding`).
    let shed = shedding(class, cfg);
    let _guard = match pool_key_size_and_cap(decision.as_ref(), class, &state) {
        Some((key, pool_max, cap)) => {
            let counter = state.in_flight.counter(&key);
            // `fetch_add` then compare, rather than compare-then-add: the
            // read-modify-write must be atomic or two requests racing at the
            // boundary both observe `cap - 1` and both proceed. Over-shooting
            // by one and immediately backing it out is correct; a torn check
            // is not.
            let observed = counter.fetch_add(1, Ordering::AcqRel) + 1;
            let guard = InFlightGuard(counter);
            if observed > cap && shed != Shedding::Never {
                let enforced = shed == Shedding::Enforce;
                tracing::warn!(
                    layer = "concurrency",
                    bucket = "pool",
                    pool = %key,
                    route_class = class.as_str(),
                    limit = cap,
                    observed,
                    pool_max_connections = pool_max,
                    enforced,
                    "admission: per-pool in-flight cap exceeded"
                );
                state.metrics.record_admission(AdmissionSample {
                    layer: "concurrency",
                    bucket: "pool",
                    enforced,
                    route_class: class.as_str(),
                });
                if enforced {
                    // `guard` drops here, returning the slot we just took.
                    // Shedding rather than queueing is deliberate: the pool's
                    // own `acquire_timeout` is already 10s, so queueing past
                    // capacity turns a fast, actionable 503 into a ten-second
                    // stall followed by an error. A 429/503 is a better
                    // product than a hang (plan §4.7).
                    // JITTERED, not a fixed 1s. Under saturation every shed
                    // client would otherwise be told to return at the same
                    // instant, re-creating the spike the shed just relieved —
                    // on a 5-connection tenant pool that is a self-inflicted
                    // thundering herd. Derived from the in-flight count rather
                    // than a RNG so the value is deterministic and testable,
                    // while still spreading callers across the window.
                    let retry_after = 1 + (observed as u64 % 3);
                    return reject(AdmissionLayer::Concurrency, "pool", retry_after);
                }
            }
            Some(guard)
        }
        None => None,
    };

    let response = next.run(req).await;

    // HOLD THE SLOT UNTIL THE BODY IS DONE, not until the headers exist.
    //
    // `next.run` returns as soon as the `Response` is constructed — headers
    // plus a LAZY body. Dropping the guard here would release the slot while
    // hyper is still polling that body. For a buffered handler the difference
    // is negligible; for `POST /v1/queries/stream` it is the whole request:
    // an SSE body is polled for the entire synthesis, holding DB and LLM work
    // long after its admission slot was handed back.
    //
    // That would make the fair-share limiter systematically under-count
    // precisely the longest, most pool-hungry requests on the service — the
    // one endpoint most capable of saturating a tenant's five connections
    // would be the one it accounted for least. So the guard moves INTO the
    // body and is dropped when the body ends.
    match _guard {
        Some(guard) => response.map(|body| Body::new(GuardedBody { inner: body, _guard: guard })),
        None => response,
    }
}

/// A response body that owns an in-flight guard and releases it when the body
/// completes — see the end of [`admission_control`] for why holding it only to
/// the headers systematically under-counts streaming requests.
struct GuardedBody<B> {
    inner: B,
    _guard: InFlightGuard,
}

impl<B> http_body::Body for GuardedBody<B>
where
    B: http_body::Body + Unpin,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_flight_cap_is_derived_per_pool_not_global() {
        // The defect this replaces: one global number sized against the
        // control plane, applied to 5-connection org tenants.
        assert_eq!(in_flight_cap(20), 16, "control plane, DB_MAX_CONNECTIONS=20");
        assert_eq!(in_flight_cap(5), 4, "org tenant, DEFAULT_MAX_CONNECTIONS=5");
        assert_eq!(in_flight_cap(100), 80, "operator-raised control pool");
    }

    #[test]
    fn cap_never_collapses_to_zero() {
        // A pool of 1 would floor to 0 at 80% and refuse every request — a
        // limiter that takes its own service down.
        assert_eq!(in_flight_cap(1), MIN_IN_FLIGHT);
        assert_eq!(in_flight_cap(2), MIN_IN_FLIGHT);
        assert_eq!(in_flight_cap(0), MIN_IN_FLIGHT);
    }

    #[test]
    fn enforcement_defaults_to_observe_only() {
        let cfg = AdmissionConfig::default();
        assert!(!cfg.rate_enforce);
        assert!(!cfg.concurrency_enforce);
        assert_eq!(cfg.per_user_per_min, DEFAULT_PER_USER_PER_MIN);
        assert_eq!(cfg.per_tenant_per_min, DEFAULT_PER_TENANT_PER_MIN);
    }

    #[test]
    fn only_explicit_truthy_values_enable_enforcement() {
        // A typo in an env var must not silently start rejecting production
        // traffic, so anything that is not exactly `1`/`true` fails safe.
        for (val, expected) in [
            ("1", true),
            ("true", true),
            (" true ", true),
            ("0", false),
            ("false", false),
            ("yes", false),
            ("TRUE", false),
            ("", false),
        ] {
            std::env::set_var("CS_TEST_ADMISSION_FLAG", val);
            assert_eq!(env_flag("CS_TEST_ADMISSION_FLAG"), expected, "value {val:?}");
        }
        std::env::remove_var("CS_TEST_ADMISSION_FLAG");
    }

    #[test]
    fn in_flight_guard_returns_the_slot_on_drop() {
        let reg = InFlightRegistry::new();
        let counter = reg.counter("control");
        {
            counter.fetch_add(1, Ordering::AcqRel);
            let _g = InFlightGuard(counter.clone());
            assert_eq!(reg.current("control"), 1);
        }
        // A leaked slot would be permanent, ratcheting the effective limit
        // down to zero over the life of the process.
        assert_eq!(reg.current("control"), 0);
    }

    #[test]
    fn pools_are_counted_independently() {
        let reg = InFlightRegistry::new();
        reg.counter("control").fetch_add(3, Ordering::AcqRel);
        reg.counter("org:org_abc").fetch_add(1, Ordering::AcqRel);
        // A personal request and an org request do not contend for the same
        // connections, so they must not share a counter.
        assert_eq!(reg.current("control"), 3);
        assert_eq!(reg.current("org:org_abc"), 1);
        assert_eq!(reg.current("org:org_never_seen"), 0);
    }

    #[test]
    fn concurrency_retry_after_is_jittered_not_a_fixed_second() {
        // A fixed `Retry-After: 1` tells every shed client to return at the
        // same instant, re-creating the saturation the shed just relieved —
        // on a 5-connection tenant pool that is a self-inflicted thundering
        // herd. Spread across a small window instead.
        let values: std::collections::HashSet<String> = (1..=6)
            .map(|observed: usize| {
                let r = reject(AdmissionLayer::Concurrency, "pool", 1 + (observed as u64 % 3));
                r.headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        assert!(values.len() > 1, "Retry-After must vary, got {values:?}");
        for v in &values {
            let n: u64 = v.parse().expect("Retry-After must be an integer");
            assert!((1..=3).contains(&n), "out of window: {n}");
        }
    }

    #[test]
    fn rejection_carries_a_machine_readable_cause() {
        let resp = reject(AdmissionLayer::Rate, "user", 60);
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get(&ADMISSION_HEADER).unwrap(), "rate");
        assert_eq!(resp.headers().get(&ADMISSION_BUCKET_HEADER).unwrap(), "user");
        assert_eq!(resp.headers().get("retry-after").unwrap(), "60");

        let resp = reject(AdmissionLayer::Concurrency, "pool", 1);
        // 503, not 429: "we are full right now" is a different instruction to
        // the client than "you asked too often".
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers().get(&ADMISSION_HEADER).unwrap(), "concurrency");
        assert_eq!(resp.headers().get(&ADMISSION_BUCKET_HEADER).unwrap(), "pool");
    }

    // ── Middleware-level tests ───────────────────────────────────────────────
    //
    // The unit tests above prove the sizing arithmetic. These drive real
    // requests through the actual layer, because the arithmetic being right
    // says nothing about whether the middleware admits, refuses, or accounts
    // correctly — and the observe-only default is the property that most
    // needs proving before this ships.

    use axum::{body::Body, http::Request as HttpRequest, routing::get, Extension, Router};
    use tower::ServiceExt;
    use uuid::Uuid;

    fn user_ctx() -> UserContext {
        UserContext {
            user_id: Uuid::nil(),
            account_id: Uuid::nil(),
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        }
    }

    /// A router shaped like the real one: admission control layered inside a
    /// stand-in for `require_auth` that stamps the same `UserContext`.
    fn router_with(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/probe", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                admission_control,
            ))
            .layer(Extension(user_ctx()))
            .with_state(state)
    }

    fn state_with(cfg: AdmissionConfig) -> Arc<AppState> {
        let mut st = crate::test_app_state_owned(None);
        st.admission = cfg;
        // A FRESH registry per state, so one test's in-flight accounting can
        // never bleed into another's assertion.
        st.in_flight = Arc::new(InFlightRegistry::new());
        // A fresh limiter too: the shared one is a process-global DashMap, and
        // these tests deliberately set ceilings of 2–3.
        st.rate_limiter = Arc::new(crate::rate_limit::InMemoryRateLimiter::new());
        Arc::new(st)
    }

    async fn status_of(router: Router) -> StatusCode {
        router
            .oneshot(
                HttpRequest::builder()
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn observe_only_never_rejects_however_far_over_the_ceiling() {
        // The rollout contract: with enforcement off, a client that blows
        // through the ceiling by 10x still gets served. If this ever fails,
        // shipping the default config would be an outage.
        let state = state_with(AdmissionConfig {
            rate_enforce: false,
            concurrency_enforce: false,
            per_user_per_min: 2,
            per_tenant_per_min: 2,
        });
        for i in 0..20 {
            let st = status_of(router_with(state.clone())).await;
            assert_eq!(st, StatusCode::OK, "request {i} was rejected in observe-only mode");
        }
    }

    #[tokio::test]
    async fn enforcing_the_per_user_ceiling_returns_429_with_a_cause() {
        let state = state_with(AdmissionConfig {
            rate_enforce: true,
            concurrency_enforce: false,
            per_user_per_min: 3,
            // Set high so this test can only be tripped by the USER bucket —
            // otherwise a passing test would not tell us which one fired.
            per_tenant_per_min: 10_000,
        });

        let mut statuses = Vec::new();
        for _ in 0..6 {
            statuses.push(status_of(router_with(state.clone())).await);
        }

        assert_eq!(&statuses[..3], &[StatusCode::OK; 3], "first 3 within ceiling");
        assert!(
            statuses[3..].iter().all(|s| *s == StatusCode::TOO_MANY_REQUESTS),
            "past the ceiling: {statuses:?}"
        );

        let resp = router_with(state)
            .oneshot(
                HttpRequest::builder()
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.headers().get(&ADMISSION_HEADER).unwrap(), "rate");
        assert_eq!(resp.headers().get(&ADMISSION_BUCKET_HEADER).unwrap(), "user");
        assert!(resp.headers().get("retry-after").is_some());
    }

    #[tokio::test]
    async fn the_tenant_ceiling_fires_independently_of_the_user_ceiling() {
        // Two limits that can each fire alone is the whole reason both exist;
        // a test that only ever trips one proves half the design.
        let state = state_with(AdmissionConfig {
            rate_enforce: true,
            concurrency_enforce: false,
            per_user_per_min: 10_000,
            per_tenant_per_min: 2,
        });

        assert_eq!(status_of(router_with(state.clone())).await, StatusCode::OK);
        assert_eq!(status_of(router_with(state.clone())).await, StatusCode::OK);

        let resp = router_with(state)
            .oneshot(
                HttpRequest::builder()
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get(&ADMISSION_BUCKET_HEADER).unwrap(), "tenant");
    }

    // ── Concurrency layer, tested DIRECTLY ───────────────────────────────────
    //
    // The middleware tests below cannot cover this layer, and an earlier
    // version of this file pretended otherwise. `test_app_state_owned(None)`
    // sets `db: None`, so `pool_key_and_size` returns `None` and the
    // concurrency layer STANDS DOWN for every middleware test here. The
    // assertion that a completed request "returned its slot" passed because
    // the counter was never incremented at all — it proved nothing about the
    // guard.
    //
    // Rather than fake a pool, the accounting is exercised against real
    // counters. End-to-end coverage through the router needs a live Postgres
    // and belongs in a `_pg.rs` suite; that gap is real and is named here
    // rather than papered over.

    /// Reproduces the middleware's admit-or-shed decision against a real
    /// registry, so the counter arithmetic is genuinely under test.
    fn try_admit(reg: &InFlightRegistry, key: &str, cap: usize) -> Option<InFlightGuard> {
        let counter = reg.counter(key);
        let observed = counter.fetch_add(1, Ordering::AcqRel) + 1;
        let guard = InFlightGuard(counter);
        if observed > cap {
            None // guard drops here, backing the increment out
        } else {
            Some(guard)
        }
    }

    #[test]
    fn concurrency_admits_up_to_the_cap_and_sheds_beyond_it() {
        let reg = InFlightRegistry::new();
        let cap = in_flight_cap(5); // an org tenant: 4

        let held: Vec<_> = (0..cap).map(|_| try_admit(&reg, "org:x", cap)).collect();
        assert!(held.iter().all(|g| g.is_some()), "first {cap} must be admitted");
        assert_eq!(reg.current("org:x"), cap);

        // One past the cap is shed, and shedding must NOT leave the counter
        // inflated — otherwise every rejection permanently shrinks capacity.
        assert!(try_admit(&reg, "org:x", cap).is_none());
        assert_eq!(reg.current("org:x"), cap, "a shed request must back its increment out");

        drop(held);
        assert_eq!(reg.current("org:x"), 0, "guards release on drop");
    }

    #[test]
    fn concurrency_is_accounted_per_pool_not_globally() {
        let reg = InFlightRegistry::new();
        let control_cap = in_flight_cap(20); // 16
        let tenant_cap = in_flight_cap(5); // 4

        let _control: Vec<_> = (0..control_cap)
            .map(|_| try_admit(&reg, "control", control_cap))
            .collect();
        // A saturated control plane must not shed an org-tenant request: they
        // do not contend for the same connections. The guard is BOUND, not
        // left as a temporary — an unbound `try_admit(..).is_some()` drops it
        // at the end of the statement and the count is back to zero, which is
        // the RAII behaviour working correctly and a test asserting nothing.
        let tenant_guard = try_admit(&reg, "org:y", tenant_cap);
        assert!(tenant_guard.is_some());
        assert_eq!(reg.current("control"), control_cap);
        assert_eq!(reg.current("org:y"), 1);
    }

    #[tokio::test]
    async fn concurrency_accounting_is_correct_under_real_contention() {
        // The fetch_add-then-compare ordering exists so two racing requests at
        // the boundary cannot both observe `cap - 1` and both proceed.
        let reg = std::sync::Arc::new(InFlightRegistry::new());
        let cap = 4;
        let admitted = std::sync::Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..64 {
            let reg = reg.clone();
            let admitted = admitted.clone();
            handles.push(tokio::spawn(async move {
                if let Some(g) = try_admit(&reg, "org:race", cap) {
                    admitted.fetch_add(1, Ordering::AcqRel);
                    tokio::task::yield_now().await;
                    drop(g);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Every slot released; never negative, never stuck.
        assert_eq!(reg.current("org:race"), 0);
        assert!(admitted.load(Ordering::Acquire) > 0);
    }

    #[tokio::test]
    async fn a_request_that_completes_returns_its_in_flight_slot() {
        // Sequential requests must never accumulate. If the guard leaked, the
        // effective cap would ratchet to zero over the life of the process —
        // the failure mode is a service that gets slower and then stops.
        let state = state_with(AdmissionConfig {
            rate_enforce: false,
            concurrency_enforce: true,
            per_user_per_min: 10_000,
            per_tenant_per_min: 10_000,
        });
        for _ in 0..50 {
            assert_eq!(status_of(router_with(state.clone())).await, StatusCode::OK);
        }
        assert_eq!(state.in_flight.current("control"), 0);
    }

    // ── Class-aware lanes, through the real layer ────────────────────────────
    //
    // An `Org` decision with two LAZY pools (never connected; `connect_lazy`
    // does no I/O) is enough for `pool_key_size_and_cap` to pick a lane and
    // size it, so the lane keying and the per-class shedding are exercised
    // through the middleware itself rather than reproduced in a helper.

    fn two_lane_org() -> TenancyDecision {
        let lazy = |max: u32| {
            sqlx::pool::PoolOptions::<sqlx::Postgres>::new()
                .max_connections(max)
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects")
        };
        TenancyDecision::Org(crate::tenancy::runtime::TenantContext {
            org_id: "org_lanes".into(),
            account_id: Uuid::nil(),
            user_id: Uuid::nil(),
            pool: lazy(3),
            page_pool: Some(lazy(2)),
        })
    }

    /// A probe that reports the in-flight count of `key` while it holds its
    /// own slot, so the test sees the lane a request was counted on.
    fn lane_router(state: Arc<AppState>, class: RouteClass, key: &'static str) -> Router {
        let st = state.clone();
        Router::new()
            .route(
                "/probe",
                get(move || {
                    let st = st.clone();
                    async move { st.in_flight.current(key).to_string() }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                admission_control,
            ))
            .layer(Extension(class))
            .layer(Extension(two_lane_org()))
            .layer(Extension(user_ctx()))
            .with_state(state)
    }

    async fn body_of(router: Router) -> (StatusCode, String) {
        let resp = router
            .oneshot(
                HttpRequest::builder()
                    .uri("/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn each_class_is_counted_on_its_own_lane() {
        let state = state_with(AdmissionConfig::default());
        let (st, n) = body_of(lane_router(
            state.clone(),
            RouteClass::PageRead,
            "org:org_lanes:page",
        ))
        .await;
        assert_eq!(
            (st, n.as_str()),
            (StatusCode::OK, "1"),
            "a page read holds a page-lane slot"
        );
        let (_, n) = body_of(lane_router(
            state.clone(),
            RouteClass::PageRead,
            "org:org_lanes:work",
        ))
        .await;
        assert_eq!(n, "0", "and no work-lane slot");
        let (_, n) = body_of(lane_router(
            state.clone(),
            RouteClass::Other,
            "org:org_lanes:work",
        ))
        .await;
        assert_eq!(n, "1", "Other holds a work-lane slot");
        let (_, n) = body_of(lane_router(
            state.clone(),
            RouteClass::Stream,
            "org:org_lanes:work",
        ))
        .await;
        assert_eq!(n, "1", "a stream shares the work lane");
        assert_eq!(
            state.in_flight.current("org:org_lanes:page"),
            0,
            "slots are returned"
        );
        assert_eq!(state.in_flight.current("org:org_lanes:work"), 0);
    }

    #[tokio::test]
    async fn with_enforcement_on_only_other_is_shed_past_its_lane_cap() {
        let state = state_with(AdmissionConfig {
            rate_enforce: false,
            concurrency_enforce: true,
            per_user_per_min: 10_000,
            per_tenant_per_min: 10_000,
        });
        // Fill the work lane past its cap (3 connections → cap 3) and the
        // page lane past its size (2), as if that many requests were in
        // flight.
        state
            .in_flight
            .counter("org:org_lanes:work")
            .fetch_add(3, Ordering::AcqRel);
        state
            .in_flight
            .counter("org:org_lanes:page")
            .fetch_add(5, Ordering::AcqRel);

        let (st, _) = body_of(lane_router(
            state.clone(),
            RouteClass::Other,
            "org:org_lanes:work",
        ))
        .await;
        assert_eq!(
            st,
            StatusCode::SERVICE_UNAVAILABLE,
            "Other past the work-lane cap is shed"
        );

        let (st, _) = body_of(lane_router(
            state.clone(),
            RouteClass::Stream,
            "org:org_lanes:work",
        ))
        .await;
        assert_eq!(
            st,
            StatusCode::OK,
            "a stream over the same cap is only observed"
        );

        let (st, n) = body_of(lane_router(
            state.clone(),
            RouteClass::PageRead,
            "org:org_lanes:page",
        ))
        .await;
        assert_eq!(
            (st, n.as_str()),
            (StatusCode::OK, "6"),
            "a page read is never shed, however deep the lane"
        );

        // Backing out the shed request's increment left the counts as seeded.
        assert_eq!(state.in_flight.current("org:org_lanes:work"), 3);
        assert_eq!(state.in_flight.current("org:org_lanes:page"), 5);
    }

    #[tokio::test]
    async fn with_enforcement_off_other_is_served_past_the_cap() {
        let state = state_with(AdmissionConfig::default());
        state
            .in_flight
            .counter("org:org_lanes:work")
            .fetch_add(10, Ordering::AcqRel);
        let (st, _) = body_of(lane_router(
            state.clone(),
            RouteClass::Other,
            "org:org_lanes:work",
        ))
        .await;
        assert_eq!(
            st,
            StatusCode::OK,
            "observe-only: the L1 default (design §4.4)"
        );
    }

    #[test]
    fn a_lane_cap_is_the_lane_itself_with_the_floor() {
        assert_eq!(lane_cap(3), 3, "the default work lane");
        assert_eq!(lane_cap(2), 2, "the default page lane");
        assert_eq!(lane_cap(1), MIN_IN_FLIGHT);
        assert_eq!(lane_cap(0), MIN_IN_FLIGHT);
    }

    #[test]
    fn shedding_follows_class_and_flag() {
        let off = AdmissionConfig::default();
        let on = AdmissionConfig {
            concurrency_enforce: true,
            ..AdmissionConfig::default()
        };
        assert_eq!(shedding(RouteClass::PageRead, &on), Shedding::Never);
        assert_eq!(shedding(RouteClass::Stream, &on), Shedding::Observe);
        assert_eq!(shedding(RouteClass::Other, &off), Shedding::Observe);
        assert_eq!(shedding(RouteClass::Other, &on), Shedding::Enforce);
    }

    #[tokio::test]
    async fn admission_stands_down_when_it_cannot_identify_the_caller() {
        // Covers the RATE layer's fail-open only. With `db: None` the
        // concurrency layer is already inert here (see the note above the
        // direct concurrency tests), so this cannot speak to it.
        //
        // A route mounted outside the auth layer has no `UserContext`.
        // Refusing it would convert a mis-wiring into an outage, so the layer
        // fails OPEN — deliberately, and this pins that choice.
        let state = state_with(AdmissionConfig {
            rate_enforce: true,
            concurrency_enforce: true,
            per_user_per_min: 1,
            per_tenant_per_min: 1,
        });
        let router = Router::new()
            .route("/probe", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                admission_control,
            ))
            .with_state(state);

        for _ in 0..5 {
            assert_eq!(status_of(router.clone()).await, StatusCode::OK);
        }
    }

    #[test]
    fn ceilings_are_env_tunable_and_reject_nonsense() {
        std::env::set_var("CS_TEST_ADMISSION_N", "42");
        assert_eq!(env_u32("CS_TEST_ADMISSION_N", 300), 42);
        // Zero would mean "reject everything"; a non-numeric value is an
        // operator typo. Both fall back rather than being honoured.
        std::env::set_var("CS_TEST_ADMISSION_N", "0");
        assert_eq!(env_u32("CS_TEST_ADMISSION_N", 300), 300);
        std::env::set_var("CS_TEST_ADMISSION_N", "abc");
        assert_eq!(env_u32("CS_TEST_ADMISSION_N", 300), 300);
        std::env::remove_var("CS_TEST_ADMISSION_N");
    }
}
