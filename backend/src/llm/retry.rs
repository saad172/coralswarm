//! Shared per-call retry policy (audit S2), used by `OpenAiProvider` for
//! `complete()` and the STREAM-OPEN phase of `stream_complete()` — never
//! mid-stream. Factored out of the old Anthropic client so OpenRouter 429/5xx
//! retries cannot drift from that backoff/jitter/`Retry-After` policy.

use std::time::Duration;

/// Default max number of *attempts* for a non-streaming completion (1 initial +
/// retries). Override with `LLM_MAX_RETRIES` (which counts total attempts).
const DEFAULT_MAX_ATTEMPTS: u32 = 4;
/// Ceiling on `LLM_MAX_RETRIES`: a managed receipt's `attempt_no` CHECK allows
/// 1..=8.
const MAX_ATTEMPTS_CEILING: u32 = 8;
/// Default base backoff. Override with `LLM_RETRY_BASE_MS`.
const DEFAULT_RETRY_BASE_MS: u64 = 500;
/// Hard ceiling on a single backoff sleep so a huge `Retry-After` can't stall a
/// worker indefinitely. Override with `LLM_RETRY_MAX_DELAY_MS`.
const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 30_000;

/// Resolved retry policy, read from env once per `complete()`/stream-open call.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryPolicy {
    pub(crate) max_attempts: u32,
    pub(crate) base: Duration,
    pub(crate) max_delay: Duration,
}

impl RetryPolicy {
    pub(crate) fn from_env() -> Self {
        let max_attempts = std::env::var("LLM_MAX_RETRIES")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .map(|n| n.clamp(1, MAX_ATTEMPTS_CEILING))
            .unwrap_or(DEFAULT_MAX_ATTEMPTS);
        let base_ms = std::env::var("LLM_RETRY_BASE_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_BASE_MS);
        let max_delay_ms = std::env::var("LLM_RETRY_MAX_DELAY_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_MAX_DELAY_MS);
        Self {
            max_attempts,
            base: Duration::from_millis(base_ms),
            max_delay: Duration::from_millis(max_delay_ms),
        }
    }

    /// Backoff for the given zero-based attempt index: `base * 2^attempt` plus
    /// full jitter, clamped to `max_delay`. A `retry_after` hint (from the
    /// `Retry-After` header) takes precedence as a lower bound.
    pub(crate) fn backoff(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        let exp = self.base.saturating_mul(1u32 << attempt.min(16));
        let capped = exp.min(self.max_delay);
        // Full jitter in [0, capped].
        let jittered = if capped.is_zero() {
            Duration::ZERO
        } else {
            let millis = capped.as_millis() as u64;
            Duration::from_millis(rand::random::<u64>() % (millis + 1))
        };
        let chosen = retry_after.map(|ra| ra.max(jittered)).unwrap_or(jittered);
        chosen.min(self.max_delay)
    }
}

/// Parse a `Retry-After` header value: either an integer number of seconds or an
/// HTTP-date (the latter is ignored here — we only honour the simple seconds
/// form, which is what Anthropic/OpenRouter return).
pub(crate) fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let secs: u64 = raw.trim().parse().ok()?;
    Some(Duration::from_secs(secs))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Serializes EVERY test in this tree — `retry.rs`'s own below,
    /// `anthropic.rs`'s `stream_complete_retries_transient_529_then_succeeds`,
    /// and `openai.rs`'s analogous retry tests — that mutates the
    /// process-global `LLM_MAX_RETRIES`/`LLM_RETRY_BASE_MS`/
    /// `LLM_RETRY_MAX_DELAY_MS` env vars. A single shared lock so the two
    /// providers' test modules (which run concurrently by default) can never
    /// race each other's env mutations.
    pub(crate) static RETRY_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn retry_policy_defaults() {
        let _guard = RETRY_ENV_LOCK.lock().unwrap();
        std::env::remove_var("LLM_MAX_RETRIES");
        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");
        let p = RetryPolicy::from_env();
        assert_eq!(p.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(p.base, Duration::from_millis(DEFAULT_RETRY_BASE_MS));
        assert_eq!(
            p.max_delay,
            Duration::from_millis(DEFAULT_RETRY_MAX_DELAY_MS)
        );
    }

    #[test]
    fn retry_policy_caps_attempts_at_the_receipt_ceiling() {
        let _guard = RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_MAX_RETRIES", "50");
        let high = RetryPolicy::from_env().max_attempts;
        std::env::set_var("LLM_MAX_RETRIES", "0");
        let low = RetryPolicy::from_env().max_attempts;
        std::env::remove_var("LLM_MAX_RETRIES");
        assert_eq!((high, low), (8, 1));
    }

    #[test]
    fn backoff_respects_max_delay_and_retry_after() {
        let p = RetryPolicy {
            max_attempts: 5,
            base: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        };
        // Jittered backoff never exceeds max_delay.
        for attempt in 0..10 {
            let d = p.backoff(attempt, None);
            assert!(
                d <= p.max_delay,
                "attempt {attempt} exceeded max_delay: {d:?}"
            );
        }
        // A Retry-After hint acts as a lower bound (still clamped to max_delay).
        let d = p.backoff(0, Some(Duration::from_secs(10)));
        assert!(d >= Duration::from_secs(10) && d <= p.max_delay);
        // A Retry-After larger than max_delay is clamped down.
        let d = p.backoff(0, Some(Duration::from_secs(120)));
        assert_eq!(d, p.max_delay);
    }

    #[test]
    fn parse_retry_after_seconds() {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert(reqwest::header::RETRY_AFTER, "42".parse().unwrap());
        assert_eq!(parse_retry_after(&h), Some(Duration::from_secs(42)));

        // HTTP-date form is ignored (returns None).
        let mut h2 = reqwest::header::HeaderMap::new();
        h2.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2026 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(parse_retry_after(&h2), None);

        // Missing header → None.
        assert_eq!(parse_retry_after(&reqwest::header::HeaderMap::new()), None);
    }
}
