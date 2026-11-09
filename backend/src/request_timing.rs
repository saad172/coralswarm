//! Per-request phase timing: the `Server-Timing` response header, the phase
//! fields on the access log, and the per-route 5xx ledger that
//! `/internal/admin/chain-health` pages on.
//!
//! # Phases
//!
//! * `auth` — request entry until `require_auth` hands the request on
//!   ([`mark_authenticated`] sits directly inside it). Includes the session
//!   lookup and binding gate. Absent on routes `require_auth` does not cover,
//!   and on requests it rejected.
//! * `txopen` — time inside `tenancy::scope::begin_account_scoped`: pool
//!   acquire, `BEGIN`, and the scope GUCs. NOT query time. Attributing real
//!   per-query time needs either sqlx's statement events (which run
//!   `sqlformat` on every statement once enabled, far over budget) or a
//!   wrapper around every `Transaction` a handler holds. This is the
//!   connection-pressure half: a saturated pool shows here first.
//! * `app` — everything after auth: `total - auth`. Overlaps `txopen`.
//! * `total` — time to response HEAD. For SSE that is time to first byte.
//!
//! The accumulator is a `tokio::task_local!`, so code off the request task
//! (spawned work, workers) records nothing rather than into the wrong request.
//!
//! # 5xx ledger
//!
//! Process memory, one entry per matched route TEMPLATE (bounded by the
//! router), twelve five-minute buckets. A deploy resets it, and with more than
//! one machine each sees only its own traffic. The chain-health poll reaches
//! one machine through `flyctl proxy`, so a 5xx storm confined to a different
//! machine is not seen until that one is polled.
//!
//! # Page-read ledger
//!
//! The same shape for `RouteClass::PageRead` requests only
//! (`docs/plans/page-load-standard/design.md` §9.2): per route, twelve
//! five-minute buckets of a fixed latency histogram plus counts of statement
//! timeouts (SQLSTATE 57014 inside a page-read transaction) and page-lane
//! acquire timeouts. `GET /internal/admin/page-read-health` serves
//! [`PageReadLedger::snapshot`]; the hourly workflow pages on p95 and on any
//! timeout. Same one-machine caveat as the 5xx ledger.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use dashmap::DashMap;
use serde::Serialize;

use crate::route_class::RouteClass;

tokio::task_local! {
    static TIMING: Arc<RequestTiming>;
}

const UNSET: u64 = u64::MAX;

pub struct RequestTiming {
    start: Instant,
    class: RouteClass,
    auth_us: AtomicU64,
    tx_open_us: AtomicU64,
    tx_count: AtomicU32,
    /// Page-read failure signals noted during the request (see
    /// [`note_sqlx_error`]).
    statement_timeouts: AtomicU32,
    acquire_timeouts: AtomicU32,
}

impl RequestTiming {
    pub fn new(start: Instant, class: RouteClass) -> Self {
        Self {
            start,
            class,
            auth_us: AtomicU64::new(UNSET),
            tx_open_us: AtomicU64::new(0),
            tx_count: AtomicU32::new(0),
            statement_timeouts: AtomicU32::new(0),
            acquire_timeouts: AtomicU32::new(0),
        }
    }

    pub fn class(&self) -> RouteClass {
        self.class
    }

    pub fn phases(&self, total: Duration) -> Phases {
        let auth = match self.auth_us.load(Ordering::Relaxed) {
            UNSET => None,
            us => Some(Duration::from_micros(us)),
        };
        Phases {
            total,
            auth,
            app: total.saturating_sub(auth.unwrap_or_default()),
            tx_open: Duration::from_micros(self.tx_open_us.load(Ordering::Relaxed)),
            tx_count: self.tx_count.load(Ordering::Relaxed),
            class: self.class,
        }
    }

    /// What the page-read ledger records for this request, besides its
    /// duration and status.
    pub fn page_read_signals(&self) -> PageReadSignals {
        PageReadSignals {
            statement_timeouts: self.statement_timeouts.load(Ordering::Relaxed),
            acquire_timeouts: self.acquire_timeouts.load(Ordering::Relaxed),
        }
    }
}

/// Run `fut` with a fresh accumulator in scope; returns the accumulator so the
/// caller can read it after the response is produced.
pub async fn scope<F: std::future::Future>(
    start: Instant,
    class: RouteClass,
    fut: F,
) -> (F::Output, Arc<RequestTiming>) {
    let timing = Arc::new(RequestTiming::new(start, class));
    let out = TIMING.scope(timing.clone(), fut).await;
    (out, timing)
}

/// Class resolved by outer middleware, available to auth/control read helpers.
/// Spawned jobs have no request scope and retain Other behavior.
pub fn current_class() -> RouteClass {
    TIMING.try_with(|t| t.class).unwrap_or(RouteClass::Other)
}

/// SQLSTATE Postgres raises when `statement_timeout` cancels a statement.
pub const QUERY_CANCELED: &str = "57014";

/// The two database failures the page-read ledger counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageReadFailure {
    /// `statement_timeout` cancelled a statement (SQLSTATE 57014): a page
    /// read ran past its 1 s budget.
    StatementTimeout,
    /// The page lane had no free connection within the acquire timeout.
    AcquireTimeout,
}

impl PageReadFailure {
    pub fn of(e: &sqlx::Error) -> Option<Self> {
        match e {
            sqlx::Error::Database(db) if db.code().as_deref() == Some(QUERY_CANCELED) => {
                Some(Self::StatementTimeout)
            }
            sqlx::Error::PoolTimedOut => Some(Self::AcquireTimeout),
            sqlx::Error::Io(io) if io.get_ref().is_some_and(|e| e.is::<RecordedAcquireTimeout>()) => Some(Self::AcquireTimeout),
            _ => None,
        }
    }
}

/// Scoped transaction failures are recorded on `StoreError` conversion.
/// Raw read executors record before returning, since callers may turn an
/// error into a string or a fallback response without that conversion.
pub fn note_sqlx_error(e: &sqlx::Error) {
    if let Some(failure) = PageReadFailure::of(e) {
        note_page_read_failure(failure);
    }
}

/// A raw executor's already-recorded database failure. Delegate SQLx's
/// driver downcasts as well as its metadata so callers still see the
/// original Postgres error. The outer error type is only an accounting mark.
#[derive(Debug)]
struct RecordedDatabaseFailure(Box<dyn sqlx::error::DatabaseError>);

impl std::fmt::Display for RecordedDatabaseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}

impl std::error::Error for RecordedDatabaseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl sqlx::error::DatabaseError for RecordedDatabaseFailure {
    fn message(&self) -> &str {
        self.0.message()
    }
    fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
        self.0.code()
    }
    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.0.as_error()
    }
    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self.0.as_error_mut()
    }
    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self.0.into_error()
    }
    fn is_transient_in_connect_phase(&self) -> bool {
        self.0.is_transient_in_connect_phase()
    }
    fn constraint(&self) -> Option<&str> {
        self.0.constraint()
    }
    fn table(&self) -> Option<&str> {
        self.0.table()
    }
    fn kind(&self) -> sqlx::error::ErrorKind {
        self.0.kind()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("pool timed out while waiting for an open connection")]
struct RecordedAcquireTimeout;

fn already_recorded(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(db) => {
            // Trait upcasting checks our outer mark; SQLx's as_error()
            // deliberately continues to expose the original driver error.
            let outer: &(dyn std::error::Error + 'static) = db.as_ref();
            outer.is::<RecordedDatabaseFailure>()
        }
        sqlx::Error::Io(io) => io
            .get_ref()
            .is_some_and(|e| e.is::<RecordedAcquireTimeout>()),
        _ => false,
    }
}

/// Record a raw read failure even when its caller consumes it as a fallback.
/// Carry the mark with the error, rather than deduplicating by request or
/// pointer: two separate cancelled statements must still count twice.
pub(crate) fn record_raw_sqlx_error(class: RouteClass, e: sqlx::Error) -> sqlx::Error {
    if class != RouteClass::PageRead || already_recorded(&e) || PageReadFailure::of(&e).is_none() {
        return e;
    }
    note_sqlx_error(&e);
    match e {
        sqlx::Error::Database(db) => sqlx::Error::Database(Box::new(RecordedDatabaseFailure(db))),
        sqlx::Error::PoolTimedOut => sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            RecordedAcquireTimeout,
        )),
        other => other,
    }
}

/// Remove a raw executor's mark before storing the original SQLx error;
/// otherwise record an ordinary scoped-transaction failure here.
pub(crate) fn record_store_sqlx_error(e: sqlx::Error) -> sqlx::Error {
    if !already_recorded(&e) {
        note_sqlx_error(&e);
        return e;
    }
    match e {
        sqlx::Error::Database(db) => {
            let outer: Box<dyn std::error::Error + Send + Sync> = db;
            let marked = outer
                .downcast::<RecordedDatabaseFailure>()
                .expect("checked accounting mark");
            sqlx::Error::Database(marked.0)
        }
        sqlx::Error::Io(_) => sqlx::Error::PoolTimedOut,
        other => other,
    }
}

/// Only a `PageRead` request records anything; off a request task this is a
/// no-op. Raw executors record before returning; scoped SQL records on
/// `StoreError` conversion.
pub fn note_page_read_failure(failure: PageReadFailure) {
    let _ = TIMING.try_with(|t| {
        if t.class != RouteClass::PageRead {
            return;
        }
        match failure {
            PageReadFailure::StatementTimeout => {
                t.statement_timeouts.fetch_add(1, Ordering::Relaxed)
            }
            PageReadFailure::AcquireTimeout => t.acquire_timeouts.fetch_add(1, Ordering::Relaxed),
        };
    });
}

/// Placed directly inside `require_auth`: reaching it means auth finished.
pub async fn mark_authenticated(req: Request, next: Next) -> Response {
    let _ = TIMING.try_with(|t| {
        let us = t.start.elapsed().as_micros() as u64;
        t.auth_us.store(us.min(UNSET - 1), Ordering::Relaxed);
    });
    next.run(req).await
}

pub fn record_tx_open(elapsed: Duration) {
    let _ = TIMING.try_with(|t| {
        t.tx_open_us.fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        t.tx_count.fetch_add(1, Ordering::Relaxed);
    });
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Phases {
    pub total: Duration,
    pub auth: Option<Duration>,
    pub app: Duration,
    pub tx_open: Duration,
    pub tx_count: u32,
    pub class: RouteClass,
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

impl Phases {
    /// `auth;dur=1.2, txopen;dur=3.4;desc="2 tx", app;dur=40.1, total;dur=41.3, class;desc="other"`
    ///
    /// `class` is a metric with no `dur`, only a `desc`: the nightly speed
    /// test and the hourly alert select page-read samples by it without a
    /// route list (design §4.2).
    pub fn header_value(&self) -> HeaderValue {
        let mut s = String::with_capacity(128);
        if let Some(auth) = self.auth {
            s.push_str(&format!("auth;dur={:.1}, ", ms(auth)));
        }
        if self.tx_count > 0 {
            s.push_str(&format!(
                "txopen;dur={:.1};desc=\"{} tx\", ",
                ms(self.tx_open),
                self.tx_count
            ));
        }
        s.push_str(&format!(
            "app;dur={:.1}, total;dur={:.1}, class;desc=\"{}\"",
            ms(self.app),
            ms(self.total),
            self.class.as_str()
        ));
        HeaderValue::from_str(&s).expect("Server-Timing value is ASCII")
    }

    pub fn total_ms(&self) -> u64 {
        self.total.as_millis() as u64
    }
    pub fn auth_ms(&self) -> Option<u64> {
        self.auth.map(|d| d.as_millis() as u64)
    }
    pub fn app_ms(&self) -> u64 {
        self.app.as_millis() as u64
    }
    pub fn tx_open_ms(&self) -> u64 {
        self.tx_open.as_millis() as u64
    }
}

/// Slower than this is logged at `warn` alongside 5xx.
pub const SLOW: Duration = Duration::from_secs(1);

/// Route label for logs and the ledger: axum's matched template, or a constant
/// for requests no route matched (a 404 probe path must not become a key).
pub fn route_of(req: &Request) -> String {
    req.extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string())
}

/// Copy the CORS layer's verdict into `Timing-Allow-Origin`, so the browser's
/// Resource Timing entries carry `serverTiming` for exactly the origins CORS
/// already admits. Layered OUTSIDE the CORS layer so it sees the final headers.
pub async fn timing_allow_origin(response: Response) -> Response {
    let mut response = response;
    if let Some(origin) = response
        .headers()
        .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .cloned()
    {
        response.headers_mut().insert("timing-allow-origin", origin);
    }
    response
}

// ── 5xx ledger ───────────────────────────────────────────────────────────────

const BUCKET_SECS: u64 = 300;
const BUCKETS: usize = 12;
pub const WINDOW_MINUTES: u64 = BUCKET_SECS * BUCKETS as u64 / 60;
/// A route pages at this many 5xx in the window...
pub const MIN_ERRORS: u32 = 3;
/// ...when they are also at least 1 in this many of its requests. The floor
/// keeps one bad minute on a busy route quiet; the share keeps a route that
/// fails on every visit loud even at one user's traffic (Library, 2026-10).
pub const MIN_SHARE_DENOMINATOR: u32 = 5;

#[derive(Clone, Copy, Default)]
struct Bucket {
    slot: u64,
    total: u32,
    errors: u32,
    unavailable: u32,
}

#[derive(Default)]
pub struct RouteErrorLedger {
    routes: DashMap<String, Mutex<[Bucket; BUCKETS]>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RouteErrorViolation {
    pub route: String,
    pub requests: u32,
    pub errors_5xx: u32,
    pub status_503: u32,
}

pub static LEDGER: LazyLock<RouteErrorLedger> = LazyLock::new(RouteErrorLedger::default);

pub fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

impl RouteErrorLedger {
    pub fn record(&self, route: &str, status: u16, now_secs: u64) {
        let slot = now_secs / BUCKET_SECS;
        let bump = |buckets: &mut [Bucket; BUCKETS]| {
            let b = &mut buckets[(slot % BUCKETS as u64) as usize];
            if b.slot != slot {
                *b = Bucket { slot, ..Bucket::default() };
            }
            b.total = b.total.saturating_add(1);
            if status >= 500 {
                b.errors = b.errors.saturating_add(1);
            }
            if status == 503 {
                b.unavailable = b.unavailable.saturating_add(1);
            }
        };
        if let Some(entry) = self.routes.get(route) {
            bump(&mut entry.lock().unwrap_or_else(|p| p.into_inner()));
            return;
        }
        let entry = self.routes.entry(route.to_string()).or_default();
        bump(&mut entry.lock().unwrap_or_else(|p| p.into_inner()));
    }

    pub fn violations(&self, now_secs: u64) -> Vec<RouteErrorViolation> {
        let current = now_secs / BUCKET_SECS;
        let mut out: Vec<RouteErrorViolation> = self
            .routes
            .iter()
            .filter_map(|entry| {
                let buckets = *entry.value().lock().unwrap_or_else(|p| p.into_inner());
                let live = buckets
                    .iter()
                    .filter(|b| b.total > 0 && b.slot + (BUCKETS as u64) > current && b.slot <= current);
                let (mut total, mut errors, mut unavailable) = (0u32, 0u32, 0u32);
                for b in live {
                    total = total.saturating_add(b.total);
                    errors = errors.saturating_add(b.errors);
                    unavailable = unavailable.saturating_add(b.unavailable);
                }
                (errors >= MIN_ERRORS && errors.saturating_mul(MIN_SHARE_DENOMINATOR) >= total).then(|| {
                    RouteErrorViolation {
                        route: entry.key().clone(),
                        requests: total,
                        errors_5xx: errors,
                        status_503: unavailable,
                    }
                })
            })
            .collect();
        out.sort_by(|a, b| b.errors_5xx.cmp(&a.errors_5xx).then_with(|| a.route.cmp(&b.route)));
        out
    }
}

// ── Page-read ledger ─────────────────────────────────────────────────────────

/// Upper edges, in ms, of the latency histogram bins. The last bin is open.
/// Chosen around the two thresholds that matter: 250 ms (the hourly alert)
/// and 500 ms (the nightly test), with enough resolution below to see a
/// regression coming.
pub const LATENCY_BINS_MS: [u64; 10] = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];
const BINS: usize = LATENCY_BINS_MS.len() + 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageReadSignals {
    pub statement_timeouts: u32,
    pub acquire_timeouts: u32,
}

#[derive(Clone, Copy)]
struct PageBucket {
    slot: u64,
    hist: [u32; BINS],
    samples: u32,
    errors_5xx: u32,
    statement_timeouts: u32,
    acquire_timeouts: u32,
}

impl Default for PageBucket {
    fn default() -> Self {
        Self {
            slot: 0,
            hist: [0; BINS],
            samples: 0,
            errors_5xx: 0,
            statement_timeouts: 0,
            acquire_timeouts: 0,
        }
    }
}

#[derive(Default)]
pub struct PageReadLedger {
    routes: DashMap<String, Mutex<[PageBucket; BUCKETS]>>,
}

/// What `GET /internal/admin/page-read-health` returns.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PageReadHealth {
    pub window_minutes: u64,
    pub samples: u32,
    /// Upper edge of the bin holding the percentile; `None` with no samples.
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub errors_5xx: u32,
    pub statement_timeouts: u32,
    pub acquire_timeouts: u32,
    pub per_route: Vec<PageReadRouteHealth>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PageReadRouteHealth {
    pub route: String,
    pub samples: u32,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub errors_5xx: u32,
    pub statement_timeouts: u32,
    pub acquire_timeouts: u32,
}

pub static PAGE_READ_LEDGER: LazyLock<PageReadLedger> = LazyLock::new(PageReadLedger::default);

fn bin_of(duration: Duration) -> usize {
    let ms = duration.as_millis() as u64;
    LATENCY_BINS_MS
        .iter()
        .position(|edge| ms <= *edge)
        .unwrap_or(BINS - 1)
}

/// Upper edge of the bin where the cumulative count first reaches
/// `pct` percent of `samples`. The open last bin reports its lower edge
/// plus one, so a value there reads as "over 5000".
fn percentile(hist: &[u32; BINS], samples: u32, pct: u32) -> Option<u64> {
    if samples == 0 {
        return None;
    }
    let target = (u64::from(samples) * u64::from(pct)).div_ceil(100);
    let mut seen = 0u64;
    for (i, n) in hist.iter().enumerate() {
        seen += u64::from(*n);
        if seen >= target {
            return Some(
                LATENCY_BINS_MS
                    .get(i)
                    .copied()
                    .unwrap_or(LATENCY_BINS_MS[BINS - 2] + 1),
            );
        }
    }
    None
}

impl PageReadLedger {
    pub fn record(
        &self,
        route: &str,
        duration: Duration,
        status: u16,
        signals: PageReadSignals,
        now_secs: u64,
    ) {
        let slot = now_secs / BUCKET_SECS;
        let bump = |buckets: &mut [PageBucket; BUCKETS]| {
            let b = &mut buckets[(slot % BUCKETS as u64) as usize];
            if b.slot != slot {
                *b = PageBucket {
                    slot,
                    ..PageBucket::default()
                };
            }
            b.hist[bin_of(duration)] = b.hist[bin_of(duration)].saturating_add(1);
            b.samples = b.samples.saturating_add(1);
            if status >= 500 {
                b.errors_5xx = b.errors_5xx.saturating_add(1);
            }
            b.statement_timeouts = b
                .statement_timeouts
                .saturating_add(signals.statement_timeouts);
            b.acquire_timeouts = b.acquire_timeouts.saturating_add(signals.acquire_timeouts);
        };
        if let Some(entry) = self.routes.get(route) {
            bump(&mut entry.lock().unwrap_or_else(|p| p.into_inner()));
            return;
        }
        let entry = self.routes.entry(route.to_string()).or_default();
        bump(&mut entry.lock().unwrap_or_else(|p| p.into_inner()));
    }

    pub fn snapshot(&self, now_secs: u64) -> PageReadHealth {
        let current = now_secs / BUCKET_SECS;
        let mut all = PageBucket::default();
        let mut per_route: Vec<PageReadRouteHealth> = self
            .routes
            .iter()
            .filter_map(|entry| {
                let buckets = *entry.value().lock().unwrap_or_else(|p| p.into_inner());
                let mut sum = PageBucket::default();
                for b in buckets.iter().filter(|b| {
                    b.samples > 0 && b.slot + (BUCKETS as u64) > current && b.slot <= current
                }) {
                    for (acc, n) in sum.hist.iter_mut().zip(b.hist.iter()) {
                        *acc = acc.saturating_add(*n);
                    }
                    sum.samples = sum.samples.saturating_add(b.samples);
                    sum.errors_5xx = sum.errors_5xx.saturating_add(b.errors_5xx);
                    sum.statement_timeouts =
                        sum.statement_timeouts.saturating_add(b.statement_timeouts);
                    sum.acquire_timeouts = sum.acquire_timeouts.saturating_add(b.acquire_timeouts);
                }
                if sum.samples == 0 {
                    return None;
                }
                for (acc, n) in all.hist.iter_mut().zip(sum.hist.iter()) {
                    *acc = acc.saturating_add(*n);
                }
                all.samples = all.samples.saturating_add(sum.samples);
                all.errors_5xx = all.errors_5xx.saturating_add(sum.errors_5xx);
                all.statement_timeouts = all
                    .statement_timeouts
                    .saturating_add(sum.statement_timeouts);
                all.acquire_timeouts = all.acquire_timeouts.saturating_add(sum.acquire_timeouts);
                Some(PageReadRouteHealth {
                    route: entry.key().clone(),
                    samples: sum.samples,
                    p50_ms: percentile(&sum.hist, sum.samples, 50),
                    p95_ms: percentile(&sum.hist, sum.samples, 95),
                    errors_5xx: sum.errors_5xx,
                    statement_timeouts: sum.statement_timeouts,
                    acquire_timeouts: sum.acquire_timeouts,
                })
            })
            .collect();
        per_route.sort_by(|a, b| b.p95_ms.cmp(&a.p95_ms).then_with(|| a.route.cmp(&b.route)));
        PageReadHealth {
            window_minutes: WINDOW_MINUTES,
            samples: all.samples,
            p50_ms: percentile(&all.hist, all.samples, 50),
            p95_ms: percentile(&all.hist, all.samples, 95),
            errors_5xx: all.errors_5xx,
            statement_timeouts: all.statement_timeouts,
            acquire_timeouts: all.acquire_timeouts,
            per_route,
        }
    }
}

/// Server-Timing grammar (W3C): `metric *( "," metric )`, metric =
/// token *( ";" param ), param = token "=" ( token / quoted-string ).
#[cfg(test)]
pub(crate) fn assert_server_timing_grammar(v: &str) {
    let token = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b));
    for metric in v.split(',') {
        let mut parts = metric.trim().split(';');
        assert!(token(parts.next().unwrap()), "bad metric name in {v}");
        for param in parts {
            let (k, val) = param.split_once('=').unwrap_or_else(|| panic!("bad param {param} in {v}"));
            assert!(token(k), "bad param name in {v}");
            let quoted = val.len() >= 2 && val.starts_with('"') && val.ends_with('"');
            assert!(token(val) || quoted, "bad param value {val} in {v}");
            if k == "dur" {
                val.parse::<f64>().unwrap_or_else(|_| panic!("dur not a number in {v}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode, routing::get, Router};
    use tower::ServiceExt;

    #[test]
    fn header_is_well_formed_server_timing() {
        let p = Phases {
            total: Duration::from_micros(41_349),
            auth: Some(Duration::from_micros(1_200)),
            app: Duration::from_micros(40_149),
            tx_open: Duration::from_micros(3_400),
            tx_count: 2,
            class: RouteClass::PageRead,
        };
        assert_eq!(
            p.header_value().to_str().unwrap(),
            "auth;dur=1.2, txopen;dur=3.4;desc=\"2 tx\", app;dur=40.1, total;dur=41.3, class;desc=\"page_read\""
        );
        let bare = Phases {
            auth: None,
            tx_count: 0,
            class: RouteClass::Other,
            ..p
        };
        assert_eq!(
            bare.header_value().to_str().unwrap(),
            "app;dur=40.1, total;dur=41.3, class;desc=\"other\""
        );
        assert_server_timing_grammar(p.header_value().to_str().unwrap());
        assert_server_timing_grammar(bare.header_value().to_str().unwrap());
    }

    #[tokio::test]
    async fn scope_records_auth_and_tx_only_inside_the_request_task() {
        record_tx_open(Duration::from_millis(5)); // outside any scope: a no-op, not a panic
        let app = Router::new()
            .route(
                "/v1/things/{id}",
                get(|| async {
                    record_tx_open(Duration::from_millis(2));
                    record_tx_open(Duration::from_millis(3));
                    StatusCode::OK
                }),
            )
            .layer(axum::middleware::from_fn(mark_authenticated));
        let req = Request::builder().uri("/v1/things/42").body(Body::empty()).unwrap();
        let start = Instant::now();
        let (res, timing) = scope(start, RouteClass::Other, app.oneshot(req)).await;
        assert_eq!(res.unwrap().status(), StatusCode::OK);
        let p = timing.phases(start.elapsed());
        assert!(p.auth.is_some());
        assert_eq!(p.tx_count, 2);
        assert_eq!(p.tx_open, Duration::from_millis(5));
    }

    #[tokio::test]
    async fn a_page_read_counts_its_statement_and_acquire_timeouts_and_other_does_not() {
        let start = Instant::now();
        let ((), t) = scope(start, RouteClass::PageRead, async {
            note_page_read_failure(PageReadFailure::StatementTimeout);
            note_sqlx_error(&sqlx::Error::PoolTimedOut);
            note_sqlx_error(&sqlx::Error::RowNotFound);
            note_page_read_failure(PageReadFailure::AcquireTimeout);
        })
        .await;
        assert_eq!(
            t.page_read_signals(),
            PageReadSignals {
                statement_timeouts: 1,
                acquire_timeouts: 2
            }
        );

        let ((), t) = scope(start, RouteClass::Other, async {
            note_page_read_failure(PageReadFailure::StatementTimeout);
            note_sqlx_error(&sqlx::Error::PoolTimedOut);
        })
        .await;
        assert_eq!(
            t.page_read_signals(),
            PageReadSignals::default(),
            "only page reads feed the ledger"
        );
        note_page_read_failure(PageReadFailure::StatementTimeout); // outside any scope: a no-op
    }

    #[test]
    fn only_a_pool_timeout_or_a_cancelled_statement_is_a_page_read_failure() {
        assert_eq!(
            PageReadFailure::of(&sqlx::Error::PoolTimedOut),
            Some(PageReadFailure::AcquireTimeout)
        );
        assert_eq!(PageReadFailure::of(&sqlx::Error::RowNotFound), None);
        assert_eq!(PageReadFailure::of(&sqlx::Error::PoolClosed), None);
    }

    #[test]
    fn page_read_ledger_reports_bin_edges_and_timeouts_over_the_window() {
        let l = PageReadLedger::default();
        let t = 4_000_000 - (4_000_000 % BUCKET_SECS);
        for ms in [3, 4, 8, 12, 30, 40, 60, 80, 120, 400] {
            l.record(
                "/v1/me",
                Duration::from_millis(ms),
                200,
                PageReadSignals::default(),
                t,
            );
        }
        l.record(
            "/v1/graphs",
            Duration::from_millis(1_200),
            500,
            PageReadSignals {
                statement_timeouts: 1,
                acquire_timeouts: 0,
            },
            t + 60,
        );
        let h = l.snapshot(t + 120);
        assert_eq!(h.samples, 11);
        assert_eq!(
            h.p50_ms,
            Some(50),
            "6th of 11 samples lands in the 25..50 bin"
        );
        assert_eq!(h.p95_ms, Some(2_500), "the 1.2 s sample is the 95th");
        assert_eq!(h.errors_5xx, 1);
        assert_eq!(h.statement_timeouts, 1);
        assert_eq!(h.acquire_timeouts, 0);
        assert_eq!(
            h.per_route[0].route, "/v1/graphs",
            "sorted by p95, worst first"
        );
        assert_eq!(h.per_route[1].p95_ms, Some(500));

        // An hour later every bucket has aged out.
        let later = l.snapshot(t + 3_600);
        assert_eq!(later.samples, 0);
        assert_eq!(later.p95_ms, None);
        assert!(later.per_route.is_empty());
    }

    #[test]
    fn open_bin_reads_as_over_the_last_edge() {
        let l = PageReadLedger::default();
        let t = 5_000_000;
        l.record(
            "/v1/me",
            Duration::from_secs(9),
            200,
            PageReadSignals::default(),
            t,
        );
        assert_eq!(l.snapshot(t).p95_ms, Some(5_001));
    }

    #[test]
    fn a_route_that_fails_every_visit_pages_even_at_low_traffic() {
        let l = RouteErrorLedger::default();
        let t = 1_000_000;
        for i in 0..3 {
            l.record("/v1/library", 503, t + i * 60);
        }
        let v = l.violations(t + 200);
        assert_eq!(
            v,
            vec![RouteErrorViolation { route: "/v1/library".into(), requests: 3, errors_5xx: 3, status_503: 3 }]
        );
    }

    #[test]
    fn isolated_errors_on_a_busy_route_stay_quiet() {
        let l = RouteErrorLedger::default();
        let t = 2_000_000;
        for _ in 0..2 {
            l.record("/v1/me", 500, t);
        }
        assert!(l.violations(t).is_empty(), "two errors is under the floor");
        l.record("/v1/me", 502, t);
        for _ in 0..100 {
            l.record("/v1/me", 200, t);
        }
        assert!(l.violations(t).is_empty(), "3 of 103 is under the share");
    }

    #[test]
    fn errors_age_out_of_the_window() {
        let l = RouteErrorLedger::default();
        let t = 3_000_000 - (3_000_000 % BUCKET_SECS);
        for _ in 0..5 {
            l.record("/v1/journal", 500, t);
        }
        assert_eq!(l.violations(t + 3_599).len(), 1);
        assert!(l.violations(t + 3_600).is_empty());
        // A bucket slot reused an hour later starts from zero, not from the
        // stale counts.
        l.record("/v1/journal", 200, t + 3_600);
        assert!(l.violations(t + 3_600).is_empty());
    }

    /// Run with `cargo test -p coralswarm-backend --lib request_timing::tests::overhead -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore]
    async fn overhead() {
        let app = Router::new().route("/v1/x/{id}", get(|| async { StatusCode::OK }));
        let n = 50_000u32;
        let bare = {
            let started = Instant::now();
            for _ in 0..n {
                let req = Request::builder().uri("/v1/x/1").body(Body::empty()).unwrap();
                let _ = app.clone().oneshot(req).await;
            }
            started.elapsed()
        };
        let timed = {
            let app = app.clone().layer(axum::middleware::from_fn(mark_authenticated));
            let started = Instant::now();
            for _ in 0..n {
                let req = Request::builder().uri("/v1/x/1").body(Body::empty()).unwrap();
                let start = Instant::now();
                let route = route_of(&req);
                let (res, timing) = scope(start, RouteClass::Other, app.clone().oneshot(req)).await;
                let mut res = res.unwrap();
                let p = timing.phases(start.elapsed());
                res.headers_mut().insert("server-timing", p.header_value());
                LEDGER.record(&route, res.status().as_u16(), now_secs());
            }
            started.elapsed()
        };
        let per = (timed.saturating_sub(bare)) / n;
        println!("bare {:?}/req, timed {:?}/req, overhead {:?}/req", bare / n, timed / n, per);
        assert!(per < Duration::from_millis(1));
    }
}
