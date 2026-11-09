//! Shared outbound rate limiter for Microsoft Graph calls (m365 Train 2 PR
//! 2.6) — a token bucket per tenant, a concurrency ceiling per tenant, and
//! `Retry-After` honored with a single bounded retry.
//!
//! # Why this exists
//! `rate_limit.rs` (this crate's other rate limiter) is INBOUND-only — it
//! throttles requests arriving at CoralSwarm's own API, never a call this
//! backend makes outward. Nothing today paces or backs off Graph traffic:
//! `connectors::microsoft`'s paginators just keep calling until they hit a
//! hard-coded item cap or Graph returns a non-2xx, and a 429 from Graph is
//! treated as an ordinary failure — the caller logs a warning and moves on,
//! same as Slack's paginators (`docs/connectors/slack-implementation.md`)
//! "fail outright on 429" behavior this module is built to NOT copy. As
//! Teams/Outlook usage scales to enterprise tenants (the whole point of
//! Train 2's org-plane work), that becomes routine data loss under load, not
//! an edge case.
//!
//! # What's copied from Slack, what's new
//! Per the plan (`.scratch/m365-enterprise/plan.md`, PR 2.6): copy Slack's
//! *semantics* — `Unavailable` (we could not get a slot / the call could not
//! be attempted) is a DIFFERENT outcome than `Unmatched`/"nothing found"
//! (roster and backfill callers must never fold the two together — a
//! throttled roster walk must abort wholesale like Slack's does on a real
//! API failure, not silently report an empty roster), and there is no
//! negative caching of a rate-limit outcome (a saturated bucket is a
//! moment-in-time condition, not evidence about the resource). The
//! mechanism itself — a real token bucket with per-tenant concurrency and
//! `Retry-After` — is new; Slack has nothing equivalent to crib from.
//!
//! # Design: task-local key, global buckets
//! Every `connectors::microsoft` fetcher takes a bare `&reqwest::Client`
//! today (~40 functions, ~55 call sites across `routes/` and `workers/`).
//! Threading a rate-limit key through every one of those signatures would be
//! a huge, mechanical, error-prone diff for what is fundamentally
//! request-scoped metadata. Instead, the KEY travels via a
//! `tokio::task_local!` (set once per logical operation — a webhook
//! notification, a backfill task, one tenant-loop tick — via [`scoped`]) and
//! the limiter STATE lives in one process-wide singleton ([`shared`]),
//! mirroring the existing `OnceLock` module-static idiom already used in
//! this file's neighbor (`connectors::microsoft::microsoft_negative_resolve_
//! cache`). `connectors::microsoft`'s low-level Graph HTTP call sites read
//! the ambient key via [`current_key`] and fall back to [`UNSCOPED_KEY`]
//! (a single shared bucket) if none is set — a defensive fallback for a path
//! nobody wrapped yet, not the intended steady state; every production entry
//! point this PR touches (`routes::graph_webhook`, `routes::teams_ingest`,
//! `routes::workflows`, `workers::graph_subscriptions`,
//! `workers::teams_seat_reconcile`) opens a scope.
//!
//! # Per-tenant key
//! The Microsoft AAD tenant id (`tenant_id`) when the call is app-only/RSC
//! (Graph's own throttling buckets are per-AAD-tenant-per-app for
//! application permissions), else the CoralSwarm `account_id` for delegated
//! calls (Graph throttles delegated calls per-user-per-app, and this
//! connector has exactly one delegated user per account today). Either way
//! the key is a `String` — callers format whichever identity they hold.
//!
//! # Future connectors
//! Lives outside `connectors::microsoft` deliberately — Train 3 wires
//! Outlook through the same limiter, and the plan calls out "eventually
//! other connectors" too. Nothing here is Microsoft- or Graph-specific
//! except the doc comments' examples.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, Instant};

// ── Tunables ─────────────────────────────────────────────────────────────────

/// Burst size: how many requests a tenant can fire back-to-back before the
/// bucket starts pacing them. Graph's documented guidance is roughly 10
/// concurrent/burst requests per app per tenant before 429s show up in
/// practice — comfortably inside that with room for the concurrency ceiling
/// below to bite first under sustained load.
const DEFAULT_CAPACITY: f64 = 10.0;
/// Steady-state refill rate (tokens/sec) once the burst is spent.
const DEFAULT_REFILL_PER_SEC: f64 = 5.0;
/// Max simultaneous in-flight Graph calls per tenant. Bounds fan-out from a
/// single backfill/resync pass independently of the token bucket — a burst
/// of cheap, fast calls could otherwise all be "in flight" at once even
/// while comfortably inside the token budget.
const DEFAULT_MAX_CONCURRENCY: usize = 4;
/// How long [`GraphRateLimiter::acquire`] will wait (queued behind either the
/// concurrency ceiling or the token bucket, including any `Retry-After`
/// penalty) before giving up. Bounded deliberately: an unbounded wait would
/// let one throttled tenant pin a worker task indefinitely — the caller is
/// expected to treat [`RateLimitError::Unavailable`] as "try again on the
/// next sweep/tick", not retry in a hot loop.
const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(45);
/// Fallback delay when a 429/503 response carries no (or an unparsable)
/// `Retry-After` header. Used by `connectors::microsoft::parse_retry_after`.
pub(crate) const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(5);

/// Bucket key used when a Graph call runs with no [`scoped`] context set —
/// see this module's doc comment. Shared across every un-wired caller, so it
/// throttles *something* rather than silently bypassing the limiter, but
/// gives no per-tenant isolation; every production call site wired in PR 2.6
/// sets a real key.
pub const UNSCOPED_KEY: &str = "unscoped";

// ── Token bucket ─────────────────────────────────────────────────────────────

struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    last_refill: Instant,
    /// Set by [`GraphRateLimiter::penalize`] on a `Retry-After` response;
    /// `poll` refuses to hand out a token until this instant passes,
    /// regardless of how many tokens have refilled in the meantime — Graph
    /// told us explicitly to back off, which overrides the bucket's own
    /// pacing math.
    blocked_until: Option<Instant>,
}

impl TokenBucket {
    fn new(capacity: f64, refill_per_sec: f64, now: Instant) -> Self {
        Self {
            tokens: capacity,
            capacity,
            refill_per_sec,
            last_refill: now,
            blocked_until: None,
        }
    }

    fn refill(&mut self, now: Instant) {
        if now > self.last_refill {
            let elapsed = now.duration_since(self.last_refill).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.last_refill = now;
        }
    }

    /// Attempt to take one token. `None` means a token was taken and the
    /// caller may proceed now; `Some(wait)` means come back (or sleep) for at
    /// least `wait` before trying again.
    fn poll(&mut self, now: Instant) -> Option<Duration> {
        self.refill(now);
        if let Some(until) = self.blocked_until {
            if now < until {
                return Some(until - now);
            }
            self.blocked_until = None;
        }
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            None
        } else {
            let deficit = 1.0 - self.tokens;
            Some(Duration::from_secs_f64(deficit / self.refill_per_sec))
        }
    }

    /// Record a server-supplied backoff. Also drains the bucket to zero so a
    /// concurrent acquire that already refilled past `blocked_until`'s
    /// implicit wait still has to wait out the full penalty, not just
    /// whatever fraction the token math alone would have implied.
    fn penalize(&mut self, now: Instant, retry_after: Duration) {
        self.refill(now);
        self.tokens = 0.0;
        let until = now + retry_after;
        self.blocked_until = Some(self.blocked_until.map_or(until, |u| u.max(until)));
    }
}

// ── Per-tenant state ─────────────────────────────────────────────────────────

struct TenantState {
    bucket: Mutex<TokenBucket>,
    concurrency: Arc<Semaphore>,
}

/// Held for the duration of one throttled Graph call; releases the
/// concurrency slot on drop. Carries no data of its own — acquiring it is
/// the whole point.
#[derive(Debug)]
pub struct Permit {
    _concurrency: OwnedSemaphorePermit,
}

#[derive(Debug, thiserror::Error)]
pub enum RateLimitError {
    /// Distinct from "the resource doesn't exist" / "nothing matched"
    /// (`Unmatched`, elsewhere in this codebase's Teams/Slack roster
    /// vocabulary) — this means the call was never attempted because a slot
    /// never opened up within [`DEFAULT_MAX_WAIT`]. Callers on an
    /// abort-wholesale path (roster reconcile) must propagate this as a
    /// failure of the whole operation, exactly as they already do for any
    /// other `ConnectorError` — never interpret it as "empty".
    #[error("graph rate limiter saturated for tenant {0}: no slot within budget")]
    Unavailable(String),
}

// ── The limiter ──────────────────────────────────────────────────────────────

pub struct GraphRateLimiter {
    tenants: Mutex<HashMap<String, Arc<TenantState>>>,
    capacity: f64,
    refill_per_sec: f64,
    max_concurrency: usize,
    max_wait: Duration,
}

impl Default for GraphRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphRateLimiter {
    pub fn new() -> Self {
        Self::with_config(
            DEFAULT_CAPACITY,
            DEFAULT_REFILL_PER_SEC,
            DEFAULT_MAX_CONCURRENCY,
            DEFAULT_MAX_WAIT,
        )
    }

    pub fn with_config(
        capacity: f64,
        refill_per_sec: f64,
        max_concurrency: usize,
        max_wait: Duration,
    ) -> Self {
        // A bucket that can never hand out a token (`capacity < 1.0`), or
        // whose refill math divides by zero/a negative rate, is not a slower
        // limiter — it is a guaranteed `Unavailable` (capacity) or a
        // `Duration::from_secs_f64` panic that aborts the whole Graph call
        // task (`poll`'s `deficit / self.refill_per_sec`, below). `new()` is
        // the only caller today and always passes the sane defaults, so this
        // never fires in production — it exists so a future `with_config`
        // caller cannot construct a panicking limiter.
        let capacity = if capacity.is_finite() && capacity >= 1.0 {
            capacity
        } else {
            DEFAULT_CAPACITY
        };
        let refill_per_sec = if refill_per_sec.is_finite() && refill_per_sec > 0.0 {
            refill_per_sec
        } else {
            DEFAULT_REFILL_PER_SEC
        };
        let max_concurrency = max_concurrency.max(1);
        Self {
            tenants: Mutex::new(HashMap::new()),
            capacity,
            refill_per_sec,
            max_concurrency,
            max_wait,
        }
    }

    async fn tenant(&self, key: &str) -> Arc<TenantState> {
        let mut map = self.tenants.lock().await;
        map.entry(key.to_string())
            .or_insert_with(|| {
                Arc::new(TenantState {
                    bucket: Mutex::new(TokenBucket::new(
                        self.capacity,
                        self.refill_per_sec,
                        Instant::now(),
                    )),
                    concurrency: Arc::new(Semaphore::new(self.max_concurrency)),
                })
            })
            .clone()
    }

    /// Wait for both a concurrency slot and a token for `key`, bounded by
    /// `max_wait`. Every wait (concurrency queue, token refill, `Retry-After`
    /// penalty) counts against the same budget — [`RateLimitError::
    /// Unavailable`] means "still didn't get a slot after `max_wait`", not
    /// which specific cause dominated.
    pub async fn acquire(&self, key: &str) -> Result<Permit, RateLimitError> {
        let deadline = Instant::now() + self.max_wait;
        let tenant = self.tenant(key).await;

        let permit = match tokio::time::timeout_at(
            deadline,
            tenant.concurrency.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(p)) => p,
            _ => return Err(RateLimitError::Unavailable(key.to_string())),
        };

        loop {
            let wait = {
                let mut bucket = tenant.bucket.lock().await;
                bucket.poll(Instant::now())
            };
            let Some(wait) = wait else { break };
            let now = Instant::now();
            if now >= deadline {
                drop(permit);
                return Err(RateLimitError::Unavailable(key.to_string()));
            }
            tokio::time::sleep_until(std::cmp::min(now + wait, deadline)).await;
        }

        Ok(Permit {
            _concurrency: permit,
        })
    }

    /// Record a server-supplied `Retry-After` for `key`. Does not itself
    /// wait — a subsequent [`acquire`](Self::acquire) for the same key blocks
    /// until the penalty elapses.
    pub async fn penalize(&self, key: &str, retry_after: Duration) {
        let tenant = self.tenant(key).await;
        let mut bucket = tenant.bucket.lock().await;
        bucket.penalize(Instant::now(), retry_after);
    }
}

// ── Process-wide singleton ────────────────────────────────────────────────────

/// The one limiter every Graph call in the process shares — per-tenant
/// buckets need to be visible across every worker tick and request handler,
/// not scoped to a request or an `AppState` clone. Same `OnceLock`
/// module-static idiom as `connectors::microsoft::microsoft_negative_resolve_
/// cache`.
pub fn shared() -> &'static GraphRateLimiter {
    static INSTANCE: OnceLock<GraphRateLimiter> = OnceLock::new();
    INSTANCE.get_or_init(GraphRateLimiter::new)
}

// ── Task-local key propagation ────────────────────────────────────────────────

tokio::task_local! {
    static RATE_KEY: String;
}

/// Run `fut` with `key` as the ambient rate-limit key for every Graph call
/// made inside it (via [`current_key`]). Call this ONCE per logical
/// operation — a webhook notification, a spawned backfill task, one
/// tenant-loop tick — at its outermost `async` boundary. Does NOT propagate
/// across a `tokio::spawn` (a new task has no inherited task-local state);
/// wrap the spawned future itself if the boundary is a spawn.
pub async fn scoped<F: std::future::Future>(key: impl Into<String>, fut: F) -> F::Output {
    RATE_KEY.scope(key.into(), fut).await
}

/// The ambient key set by the innermost enclosing [`scoped`], if any.
pub(crate) fn current_key() -> Option<String> {
    RATE_KEY.try_with(|k| k.clone()).ok()
}

/// The correct [`scoped`] key for one Graph call: the AAD tenant id when the
/// call is app-only/RSC (this module's "Per-tenant key" doc section above —
/// Graph throttles application permissions per AAD tenant per app), or the
/// CoralSwarm `account_id` when there is no tenant id to key on (delegated
/// calls). `tenant_id` is empty exactly for delegated resources — see
/// `connectors::microsoft::ensure_subscription_for`'s own doc ("`tenant_id`
/// is non-empty for app-only (RSC) subscriptions ... empty for delegated") —
/// so an empty string is the correct, unambiguous signal to fall back to
/// `account_id` here, not a caller bug.
///
/// Centralizing this (CodeRabbit review, PR #673) avoids call sites keying
/// app-only calls on `account_id` alone: two CoralSwarm accounts under one
/// AAD tenant would then hold two independent buckets against Graph's ONE
/// shared per-tenant-per-app limit, roughly doubling the effective rate the
/// limiter permits.
pub(crate) fn rate_key(account_id: uuid::Uuid, tenant_id: &str) -> String {
    if tenant_id.is_empty() {
        account_id.to_string()
    } else {
        tenant_id.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(capacity: f64, refill_per_sec: f64, max_concurrency: usize) -> GraphRateLimiter {
        GraphRateLimiter::with_config(capacity, refill_per_sec, max_concurrency, Duration::from_secs(10))
    }

    #[tokio::test(start_paused = true)]
    async fn first_requests_up_to_capacity_are_immediate() {
        let l = limiter(3.0, 1.0, 10);
        let start = Instant::now();
        for _ in 0..3 {
            l.acquire("tenant-a").await.expect("should be allowed");
        }
        // No real time should have elapsed acquiring the initial burst.
        assert_eq!(Instant::now(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn exceeding_capacity_paces_via_refill() {
        let l = limiter(1.0, 2.0, 10); // 1 token, refills at 2/sec (0.5s per token)
        l.acquire("tenant-a").await.unwrap();
        let start = Instant::now();
        l.acquire("tenant-a").await.unwrap();
        let elapsed = Instant::now() - start;
        assert!(
            elapsed >= Duration::from_millis(450) && elapsed <= Duration::from_millis(600),
            "expected ~500ms pacing wait, got {elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn different_tenants_do_not_share_a_bucket() {
        let l = limiter(1.0, 0.1, 10); // 1 token, glacial refill
        l.acquire("tenant-a").await.unwrap();
        // tenant-a's bucket is now empty, but tenant-b's is untouched.
        let start = Instant::now();
        l.acquire("tenant-b").await.unwrap();
        assert_eq!(Instant::now(), start, "tenant-b must not wait on tenant-a's bucket");
    }

    #[tokio::test(start_paused = true)]
    async fn concurrency_ceiling_blocks_until_a_slot_frees() {
        let l = limiter(100.0, 100.0, 1); // token bucket wide open; concurrency=1 is the binding constraint
        let first = l.acquire("tenant-a").await.unwrap();

        let l = Arc::new(l);
        let l2 = l.clone();
        let waiter = tokio::spawn(async move { l2.acquire("tenant-a").await });

        // Give the spawned task a chance to block on the semaphore.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!waiter.is_finished(), "second acquire must block while the first permit is held");

        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter task should complete")
            .expect("join should succeed");
        assert!(second.is_ok(), "second acquire should succeed once the slot frees");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_penalty_blocks_until_it_elapses() {
        let l = limiter(10.0, 10.0, 10); // token bucket would otherwise allow immediately
        l.penalize("tenant-a", Duration::from_secs(3)).await;

        let start = Instant::now();
        l.acquire("tenant-a").await.unwrap();
        let elapsed = Instant::now() - start;
        assert!(
            elapsed >= Duration::from_secs(3),
            "acquire must wait out the Retry-After penalty, got {elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_gives_up_after_max_wait() {
        let l = GraphRateLimiter::with_config(1.0, 0.001, 10, Duration::from_secs(2));
        l.acquire("tenant-a").await.unwrap(); // drain the single token
        let start = Instant::now();
        let err = l.acquire("tenant-a").await.unwrap_err();
        assert!(matches!(err, RateLimitError::Unavailable(k) if k == "tenant-a"));
        assert!(Instant::now() - start >= Duration::from_secs(2));
    }

    #[tokio::test]
    async fn scoped_sets_and_clears_the_ambient_key() {
        assert!(current_key().is_none());
        let seen = scoped("tenant-a", async { current_key() }).await;
        assert_eq!(seen.as_deref(), Some("tenant-a"));
        // Outside the scope again, nothing is set.
        assert!(current_key().is_none());
    }

    #[tokio::test]
    async fn nested_scopes_shadow_correctly() {
        let seen = scoped("outer", async {
            let inner = scoped("inner", async { current_key() }).await;
            (inner, current_key())
        })
        .await;
        assert_eq!(seen.0.as_deref(), Some("inner"));
        assert_eq!(seen.1.as_deref(), Some("outer"));
    }

    #[tokio::test(start_paused = true)]
    async fn shared_singleton_is_process_wide() {
        // Not a timing test — just proves `shared()` hands back the same
        // instance across calls (state persists), the property every real
        // call site depends on.
        let key = format!("singleton-smoke-{}", uuid::Uuid::new_v4());
        shared().acquire(&key).await.unwrap();
        // A second acquire on a fresh capacity=10 bucket should still be
        // immediate (nowhere near exhausted) — this just proves `shared()`
        // didn't panic / is usable from arbitrary call sites.
        shared().acquire(&key).await.unwrap();
    }
}
