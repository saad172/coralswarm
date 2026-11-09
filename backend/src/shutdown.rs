//! Process-wide shutdown signal shared between the HTTP servers and the
//! background job workers.
//!
//! `shutdown_signal()` in `lib.rs` already gives the two axum listeners a
//! graceful drain of in-flight HTTP requests on SIGTERM/CTRL-C. The
//! `tokio::spawn`ed job workers had no equivalent: a SIGTERM (which Fly
//! sends before stopping or redeploying a machine) did nothing to them —
//! they kept dequeueing right up until the process was hard-killed
//! mid-`process_job`, which is the root cause of the orphaned-`running`-job
//! production incident that `workers::reaper` exists to clean up after the
//! fact (see `jobs::queue`'s module docs for the full writeup).
//!
//! [`Shutdown`] gives the embed/extract workers — the two directly in the
//! incident's causal chain, `EmbedAtoms` → `ExtractEntities` /
//! `ExtractRelationships` — a best-effort drain: stop claiming new jobs as
//! soon as the signal fires, while letting anything already claimed run to
//! completion. `lib.rs::run()` then gives those worker tasks a bounded
//! grace window to finish before the process exits regardless.
//!
//! `workers::slack_name_backfill` is wired the same way (dequeue-check +
//! bounded grace window, identical contract) even though it wasn't part of
//! the original incident — added later per this repo's own path instructions
//! (`.coderabbit.yaml`: "async outbound work must be checked for missing
//! cancellation"), and the concrete failure mode is the same class: without
//! it, a SIGTERM lets the worker keep claiming (and holding, as `running`)
//! Slack API-calling jobs right up until the process is hard-killed.
//!
//! `workers::reaper` and `workers::embed_retry` also consume this signal, but
//! for a different reason: neither is draining a claimed job, both are
//! periodic sweepers, so they stop *starting new sweeps* and cancel their
//! inter-sweep sleep promptly on shutdown. Neither gets a grace window in
//! `lib.rs::run()` — a sweep is one bounded DB round-trip, not a multi-step
//! job with an LLM call or R2 fetch, so there's nothing worth draining.
//!
//! ## Residual gap (documented, not fixed here)
//!
//! The other job-queue-consuming workers (e.g. `entity_resolution`) are
//! **not** wired to this signal
//! in this change — doing so for every worker would mean touching each of
//! their loops individually, which is more than a "minimal but real" fix
//! warrants in one pass. They remain exposed to the same abrupt-kill
//! orphaning as before this PR. This is a latency difference, not a
//! correctness gap: `workers::reaper` is the backstop for every job kind
//! regardless of whether its owning worker drains cleanly on shutdown, so an
//! orphaned job from one of these workers simply waits up to the reaper's
//! `STALE_AFTER` threshold before being retried, instead of being caught
//! immediately. Recommended follow-up: factor the dequeue-check +
//! grace-drain wiring done here for embed/extract/slack_name_backfill into a
//! shared helper and apply it to the remaining workers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Cheap, cloneable handle to a process-wide "shutdown has been requested"
/// flag.
#[derive(Clone, Default)]
pub struct Shutdown(Arc<AtomicBool>);

impl Shutdown {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// True once shutdown has been signalled. Workers should check this
    /// before calling `queue::dequeue` and, if true, stop claiming new work
    /// — but let anything already claimed in the current batch run to
    /// completion rather than abandoning it mid-flight.
    pub fn is_triggered(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// Signal shutdown. Idempotent — safe to call more than once (e.g. a
    /// slow CTRL-C after SIGTERM already fired).
    pub fn trigger(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_untriggered() {
        let s = Shutdown::new();
        assert!(!s.is_triggered());
    }

    #[test]
    fn trigger_is_observed_through_clones() {
        let s = Shutdown::new();
        let clone = s.clone();
        assert!(!clone.is_triggered());
        s.trigger();
        assert!(clone.is_triggered(), "clones share the same underlying flag");
    }

    #[test]
    fn trigger_is_idempotent() {
        let s = Shutdown::new();
        s.trigger();
        s.trigger();
        assert!(s.is_triggered());
    }
}
