//! Rate-limiter abstraction for per-user request throttling.
//!
//! The `RateLimiter` trait decouples the rate-limiting contract from the
//! concrete implementation so that a Redis-backed limiter can be swapped in
//! for multi-instance deployments without touching handler code.

use async_trait::async_trait;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

// ── Constants ─────────────────────────────────────────────────────────────────

const WINDOW_SECS: u64 = 60;
const MAX_REQUESTS: u32 = 10;

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Returns `true` if the request is allowed; `false` if it should be rejected.
///
/// Implementations must be `Send + Sync` so the trait object can live inside
/// an `Arc` shared across Tokio tasks.
#[async_trait]
pub trait RateLimiter: Send + Sync {
    async fn check_and_increment(&self, user_id: Uuid) -> bool;

    /// Keyed variant with a caller-supplied ceiling and window
    /// (`docs/plans/remote-mcp-v1.md` §4.3, §1 "Rate limiting" resolution):
    /// remote MCP needs several independent, differently-tiered buckets
    /// (`mcp:u:{user_id}`, `mcp:ask:{user_id}`, `mcp:a:{account_id}`) rather
    /// than the one fixed 10/min bucket [`check_and_increment`] enforces.
    ///
    /// Default implementation delegates to [`check_and_increment`] for
    /// back-compat with any caller that hasn't adopted keyed buckets yet —
    /// `max`/`window_secs` are ignored in that fallback, so callers that care
    /// about a specific ceiling/window MUST use an implementation (both
    /// shipped ones do) that honors them.
    async fn check_and_increment_key(&self, key: &str, max: u32, window_secs: u64) -> bool {
        let _ = (key, max, window_secs);
        // Fall back to a fixed per-caller identity derived from the key so
        // the default still throttles *something* rather than silently
        // always allowing. Real implementations below override this. Derived
        // via a sha256 digest truncated to 16 bytes (rather than a UUIDv5
        // namespace hash) so this doesn't need the `uuid` crate's `v5`
        // feature, which the workspace `uuid` dependency does not enable.
        let digest = Sha256::digest(key.as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        self.check_and_increment(Uuid::from_bytes(bytes)).await
    }
}

// ── In-process implementation ─────────────────────────────────────────────────

/// In-process DashMap rate limiter — single instance only.
///
/// Tracks `(request_count, window_start)` per key. Keyed on `String` (not
/// `Uuid`) so a single map backs both the legacy fixed 10/min-per-user bucket
/// (`check_and_increment`, keyed on `user_id.to_string()`) and arbitrary
/// caller-defined buckets (`check_and_increment_key`, e.g.
/// `"mcp:ask:{user_id}"` or `"mcp:a:{account_id}"`) — see the `RateLimiter`
/// trait doc. Resets the counter when the elapsed time exceeds the window.
/// Not suitable for horizontally scaled deployments (no shared state between
/// replicas) — remote MCP's startup check requires `RATE_LIMIT_BACKEND=pg`
/// for exactly this reason.
pub struct InMemoryRateLimiter {
    map: Arc<DashMap<String, (u32, Instant)>>,
}

impl InMemoryRateLimiter {
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
        }
    }

    /// Shared fixed-window check/increment logic for both trait methods.
    fn check_and_increment_window(&self, key: String, max: u32, window_secs: u64) -> bool {
        let now = Instant::now();
        let window = Duration::from_secs(window_secs);

        let mut entry = self.map.entry(key).or_insert((0, now));

        // Reset the window if it has expired.
        if now.duration_since(entry.1) > window {
            *entry = (0, now);
        }

        if entry.0 >= max {
            return false;
        }

        entry.0 += 1;
        true
    }
}

impl Default for InMemoryRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RateLimiter for InMemoryRateLimiter {
    async fn check_and_increment(&self, user_id: Uuid) -> bool {
        self.check_and_increment_window(user_id.to_string(), MAX_REQUESTS, WINDOW_SECS)
    }

    async fn check_and_increment_key(&self, key: &str, max: u32, window_secs: u64) -> bool {
        self.check_and_increment_window(key.to_string(), max, window_secs)
    }
}

// ── Postgres-backed implementation ──────────────────────────────────────────────

/// Postgres-backed fixed-window rate limiter — shared across all replicas.
///
/// State lives in the `rate_limit_window` table (migration 0042): one row per
/// `(key, window_start)`, where `window_start` is the current time floored to the
/// `WINDOW_SECS` boundary. Each request atomically upserts+increments that row
/// (`INSERT … ON CONFLICT DO UPDATE … RETURNING count`) and is allowed while the
/// post-increment count is `<= MAX_REQUESTS`. Because every replica reads/writes
/// the same row, the limit is enforced globally rather than per VM.
///
/// Uses runtime `sqlx::query` (not the compile-time `query!` macro) so the build
/// never requires a live database.
///
/// Fail-open: if the database errors (e.g. transient connectivity), the request
/// is allowed. Throttling is a soft guard, not an availability dependency — the
/// in-memory limiter is the default and this path is opt-in via
/// `RATE_LIMIT_BACKEND=pg`.
pub struct PgRateLimiter {
    pool: sqlx::PgPool,
}

impl PgRateLimiter {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Floors `now` to the start of the current fixed window of `window_secs`
    /// length. Computed in Rust so the semantics match the in-memory
    /// limiter's window handling exactly and don't depend on Postgres
    /// `date_trunc` granularity.
    fn window_start(window_secs: u64) -> chrono::DateTime<chrono::Utc> {
        let now = chrono::Utc::now();
        let secs = now.timestamp();
        let floored = secs - secs.rem_euclid(window_secs.max(1) as i64);
        chrono::DateTime::from_timestamp(floored, 0).unwrap_or(now)
    }

    /// Shared upsert-and-check logic for both trait methods.
    async fn check_and_increment_window(&self, key: &str, max: u32, window_secs: u64) -> bool {
        let window_start = Self::window_start(window_secs);

        let row: Result<(i32,), sqlx::Error> = sqlx::query_as(
            "INSERT INTO rate_limit_window (key, window_start, count) \
             VALUES ($1, $2, 1) \
             ON CONFLICT (key, window_start) \
             DO UPDATE SET count = rate_limit_window.count + 1 \
             RETURNING count",
        )
        .bind(key)
        .bind(window_start)
        .fetch_one(&self.pool)
        .await;

        match row {
            Ok((count,)) => count <= max as i32,
            Err(e) => {
                // Fail open: a throttling-store hiccup must not reject traffic.
                tracing::warn!(error = %e, "PgRateLimiter query failed; allowing request");
                true
            }
        }
    }
}

#[async_trait]
impl RateLimiter for PgRateLimiter {
    async fn check_and_increment(&self, user_id: Uuid) -> bool {
        self.check_and_increment_window(&user_id.to_string(), MAX_REQUESTS, WINDOW_SECS)
            .await
    }

    async fn check_and_increment_key(&self, key: &str, max: u32, window_secs: u64) -> bool {
        self.check_and_increment_window(key, max, window_secs).await
    }
}

// TODO(S4): token-spend limiter — a per-tenant cost cap (sum of llm_usage tokens
// over a window) is a related S4 follow-up; not implemented here.

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// First request for a user must always be allowed.
    #[tokio::test]
    async fn first_request_is_allowed() {
        let limiter = InMemoryRateLimiter::new();
        let user = Uuid::new_v4();
        assert!(limiter.check_and_increment(user).await);
    }

    /// After `MAX_REQUESTS` calls the next call must be blocked.
    #[tokio::test]
    async fn blocked_after_max_requests() {
        let limiter = InMemoryRateLimiter::new();
        let user = Uuid::new_v4();

        for _ in 0..MAX_REQUESTS {
            assert!(
                limiter.check_and_increment(user).await,
                "should be allowed before max"
            );
        }

        assert!(
            !limiter.check_and_increment(user).await,
            "should be blocked after max"
        );
    }

    /// Independent users must not share state.
    #[tokio::test]
    async fn different_users_are_independent() {
        let limiter = InMemoryRateLimiter::new();
        let user_a = Uuid::new_v4();
        let user_b = Uuid::new_v4();

        // Exhaust user_a's quota.
        for _ in 0..MAX_REQUESTS {
            limiter.check_and_increment(user_a).await;
        }

        // user_b's first request must still be allowed.
        assert!(limiter.check_and_increment(user_b).await);
    }

    // ── check_and_increment_key (remote MCP tiered buckets) ───────────────────

    #[tokio::test]
    async fn keyed_bucket_blocks_after_its_own_max() {
        let limiter = InMemoryRateLimiter::new();
        let key = "mcp:ask:11111111-1111-1111-1111-111111111111";

        for _ in 0..10 {
            assert!(
                limiter.check_and_increment_key(key, 10, 60).await,
                "should be allowed before max"
            );
        }
        assert!(
            !limiter.check_and_increment_key(key, 10, 60).await,
            "should be blocked after max"
        );
    }

    #[tokio::test]
    async fn keyed_buckets_with_different_names_are_independent() {
        let limiter = InMemoryRateLimiter::new();
        let user_bucket = "mcp:u:11111111-1111-1111-1111-111111111111";
        let ask_bucket = "mcp:ask:11111111-1111-1111-1111-111111111111";

        // Exhaust the tighter `ask` bucket.
        for _ in 0..10 {
            limiter.check_and_increment_key(ask_bucket, 10, 60).await;
        }
        assert!(!limiter.check_and_increment_key(ask_bucket, 10, 60).await);

        // A differently-named bucket for the same user must be unaffected.
        assert!(limiter.check_and_increment_key(user_bucket, 60, 60).await);
    }

    #[tokio::test]
    async fn keyed_bucket_respects_its_own_ceiling_distinct_from_the_legacy_bucket() {
        // `check_and_increment` (legacy, keyed on user_id.to_string()) and
        // `check_and_increment_key` with an unrelated key string must not
        // collide even for the "same" user, since the legacy bucket's key is
        // just the bare UUID string while MCP buckets are always prefixed
        // (`mcp:u:`, `mcp:ask:`, `mcp:a:`).
        let limiter = InMemoryRateLimiter::new();
        let user = Uuid::new_v4();
        for _ in 0..MAX_REQUESTS {
            limiter.check_and_increment(user).await;
        }
        assert!(!limiter.check_and_increment(user).await);

        // A keyed bucket that happens to reference the same user is separate.
        let key = format!("mcp:u:{user}");
        assert!(limiter.check_and_increment_key(&key, 60, 60).await);
    }
}
