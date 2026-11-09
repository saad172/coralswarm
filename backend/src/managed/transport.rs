//! Per-purpose transport bounds for managed calls.
//!
//! A managed attempt cut after it was sent settles `ambiguous` at its full
//! reservation, and the answer is thrown away. Measured on the coralswarm
//! tenant (2026-10-19/20, 24 h): 446 of 2,940 extraction-window receipts
//! settled ambiguous, 385 of them 60 to 70 s after creation. That is the
//! shared HTTP client's 60 s total deadline (`LLM_TIMEOUT_SECS`), not a hung
//! provider: billed generations of the same calls ran 27 s at the median and
//! up to 61 s, and one served host (Near AI) streamed at ~19 tokens/s against
//! Parasail's ~120 on outputs of 2.5k to 8k tokens. Those receipts were 53 %
//! of the day's extraction charge, at 6x a billed attempt each.
//!
//! So background purposes get bounds sized to that distribution, enforced in
//! the transport (never by dropping the call, which leaves the receipt to the
//! reaper): a streamed read cut only on a real stall, an attempt bound past
//! the measured tail, a whole-call deadline that keeps the job inside the
//! job reaper's window, and a throughput-sorted provider order. Ask stays on
//! the transport default: its own client bounds it at the Ask budget.

use std::time::{Duration, Instant};

use coralswarm_core::send_gate::TransportPolicy;

use super::ManagedPurpose;

/// Bounds for one purpose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PurposeBounds {
    pub attempt: Duration,
    /// Whole logical call, every attempt and backoff. `None`: the caller
    /// supplies the deadline ([`within_call_deadline`]).
    pub call: Option<Duration>,
    pub stall: Duration,
}

/// Extraction window: billed generations ran to 61 s and outputs reach the
/// 8k-token cap. 300 s holds 8k tokens at ~27 tokens/s. The call budget
/// leaves one fast retry; a window plus its smaller retry plus Jev's binding
/// stays under the job reaper's 20 minutes (`workers::reaper::stale_after`).
pub(crate) const EXTRACTION: PurposeBounds = PurposeBounds {
    attempt: Duration::from_secs(300),
    call: Some(Duration::from_secs(360)),
    stall: Duration::from_secs(120),
};
/// Jev's binding question on an extraction window: the caller's deadline
/// bounds the call; ~500 output tokens, so a minute of silence is a stall.
pub(crate) const EXTRACTION_ASIDE: PurposeBounds = PurposeBounds {
    attempt: Duration::from_secs(300),
    call: None,
    stall: Duration::from_secs(60),
};
/// Graph-level enrichment (titles, resolution, journal, digests): short
/// outputs, background.
pub(crate) const ENRICHMENT: PurposeBounds = PurposeBounds {
    attempt: Duration::from_secs(180),
    call: Some(Duration::from_secs(300)),
    stall: Duration::from_secs(90),
};
/// Room past an attempt's bound for its settlement (and a generation-cost
/// lookup) before a caller's backstop drops the future.
pub(crate) const BACKSTOP_GRACE: Duration = Duration::from_secs(30);
/// No attempt may outlive the receipt reaper: it must settle live.
const REAP_MARGIN: Duration = Duration::from_secs(60);
const MIN_ATTEMPT: Duration = Duration::from_secs(10);

tokio::task_local! {
    static CALL_DEADLINE: Instant;
}

/// Runs `fut` with a whole-call deadline `budget` from now, which every
/// managed gate opened inside it enforces in the transport. The caller keeps
/// a backstop past it ([`BACKSTOP_GRACE`]) for the settle, rather than its
/// own cut.
pub(crate) async fn within_call_deadline<F: std::future::Future>(budget: Duration, fut: F) -> F::Output {
    CALL_DEADLINE.scope(Instant::now() + budget, fut).await
}

pub(crate) fn current_call_deadline() -> Option<Instant> {
    CALL_DEADLINE.try_with(|d| *d).ok()
}

/// Operator overrides, read once per gate.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct TransportEnv {
    pub attempt_extraction: Option<Duration>,
    pub attempt_enrichment: Option<Duration>,
    pub stall: Option<Duration>,
    pub streaming_disabled: bool,
    /// `Some(None)`: sorting turned off.
    pub sort: Option<Option<&'static str>>,
}

fn secs(raw: Option<String>) -> Option<Duration> {
    raw?.trim().parse::<u64>().ok().filter(|s| *s > 0).map(Duration::from_secs)
}

impl TransportEnv {
    pub(crate) fn from_env() -> Self {
        let var = |k: &str| std::env::var(k).ok();
        Self {
            attempt_extraction: secs(var("MANAGED_ATTEMPT_TIMEOUT_SECS_SOURCE_EXTRACTION")),
            attempt_enrichment: secs(var("MANAGED_ATTEMPT_TIMEOUT_SECS_ENRICHMENT")),
            stall: secs(var("MANAGED_STREAM_STALL_SECS")),
            streaming_disabled: crate::admission::env_flag("MANAGED_STREAMING_DISABLED"),
            sort: var("MANAGED_PROVIDER_SORT").map(|v| parse_sort(&v)),
        }
    }
}

/// An unknown value turns sorting off rather than sending OpenRouter a value
/// it may reject.
fn parse_sort(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "throughput" => Some("throughput"),
        "latency" => Some("latency"),
        "price" => Some("price"),
        _ => None,
    }
}

/// The transport policy for one managed logical call opened at `opened`.
pub(crate) fn policy_for(
    purpose: ManagedPurpose,
    aside: bool,
    opened: Instant,
    caller_deadline: Option<Instant>,
    reap_after: Duration,
    env: &TransportEnv,
) -> TransportPolicy {
    let (bounds, attempt_override) = match purpose {
        ManagedPurpose::SourceExtraction if aside => (EXTRACTION_ASIDE, env.attempt_extraction),
        ManagedPurpose::SourceExtraction => (EXTRACTION, env.attempt_extraction),
        ManagedPurpose::Enrichment => (ENRICHMENT, env.attempt_enrichment),
        // Ask is bounded by its own client at the Ask budget; embeddings do
        // not use this transport.
        ManagedPurpose::InteractiveAsk | ManagedPurpose::CloudEmbedding => {
            return TransportPolicy::default()
        }
    };
    let ceiling = reap_after.saturating_sub(REAP_MARGIN).max(MIN_ATTEMPT);
    let attempt = attempt_override.unwrap_or(bounds.attempt).clamp(MIN_ATTEMPT, ceiling);
    let own = bounds.call.map(|budget| opened + budget.max(attempt));
    let deadline = match (own, caller_deadline) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    TransportPolicy {
        attempt_timeout: Some(attempt),
        stream_stall: (!env.streaming_disabled).then(|| env.stall.unwrap_or(bounds.stall)),
        deadline,
        provider_sort: env.sort.unwrap_or(Some("throughput")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REAP: Duration = Duration::from_secs(600);

    #[test]
    fn extraction_outlasts_the_measured_tail_and_streams() {
        let now = Instant::now();
        let p = policy_for(ManagedPurpose::SourceExtraction, false, now, None, REAP, &TransportEnv::default());
        let attempt = p.attempt_timeout.unwrap();
        // The client default this replaces cut 86 % of ambiguous receipts at 60 s.
        assert!(attempt >= Duration::from_secs(240), "{attempt:?}");
        assert!(attempt < REAP - REAP_MARGIN + Duration::from_secs(1));
        assert_eq!(p.stream_stall, Some(Duration::from_secs(120)));
        assert_eq!(p.deadline, Some(now + Duration::from_secs(360)));
        assert_eq!(p.provider_sort, Some("throughput"));
        assert!(p.bounds_attempts());
    }

    #[test]
    fn a_caller_deadline_bounds_the_aside_call() {
        let now = Instant::now();
        let caller = now + Duration::from_secs(120);
        let p = policy_for(ManagedPurpose::SourceExtraction, true, now, Some(caller), REAP, &TransportEnv::default());
        assert_eq!(p.deadline, Some(caller));
        assert_eq!(p.stream_stall, Some(Duration::from_secs(60)));
        let p = policy_for(ManagedPurpose::Enrichment, false, now, Some(now + Duration::from_secs(5)), REAP, &TransportEnv::default());
        assert_eq!(p.deadline, Some(now + Duration::from_secs(5)), "the earlier deadline wins");
    }

    #[test]
    fn ask_and_embedding_keep_the_transport_default() {
        let now = Instant::now();
        for purpose in [ManagedPurpose::InteractiveAsk, ManagedPurpose::CloudEmbedding] {
            let p = policy_for(purpose, false, now, Some(now), REAP, &TransportEnv::default());
            assert_eq!(p, TransportPolicy::default());
        }
    }

    #[test]
    fn no_attempt_outlives_the_receipt_reaper() {
        let env = TransportEnv { attempt_extraction: Some(Duration::from_secs(3_600)), ..Default::default() };
        let p = policy_for(ManagedPurpose::SourceExtraction, false, Instant::now(), None, REAP, &env);
        assert_eq!(p.attempt_timeout, Some(REAP - REAP_MARGIN));
    }

    #[test]
    fn operator_switches() {
        let env = TransportEnv {
            streaming_disabled: true,
            sort: Some(parse_sort("bogus")),
            stall: Some(Duration::from_secs(7)),
            ..Default::default()
        };
        let p = policy_for(ManagedPurpose::Enrichment, false, Instant::now(), None, REAP, &env);
        assert_eq!(p.stream_stall, None);
        assert_eq!(p.provider_sort, None);
        assert_eq!(p.attempt_timeout, Some(Duration::from_secs(180)));
        assert_eq!(parse_sort(" Latency "), Some("latency"));
        assert_eq!(secs(Some("0".into())), None);
        assert_eq!(secs(Some(" 42 ".into())), Some(Duration::from_secs(42)));
    }

    #[tokio::test]
    async fn the_call_deadline_is_task_scoped() {
        assert_eq!(current_call_deadline(), None);
        let inside = within_call_deadline(Duration::from_secs(30), async { current_call_deadline() }).await;
        assert!(inside.is_some());
        assert_eq!(current_call_deadline(), None);
    }
}
