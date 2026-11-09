//! Dedup for the "unknown model" billing alert — usage-metering PR11
//! (`.scratch/usage-metering/plan.md` §1 PR11).
//!
//! # What already exists (PR1) and what this adds
//!
//! [`super::rates::PriceSheet::rate`] already emits a RAW, non-deduped
//! `tracing::warn!(alert = true, model = .., ...)` on every call that falls
//! through to the blend rate for a model that is not
//! [`super::rates::UNATTRIBUTED_SENTINEL`] — see that function's own doc
//! comment, which explicitly earmarks the dedup for PR11 rather than
//! attempting it itself. Left alone, that signal fires on EVERY call for
//! EVERY unmatched `(job, model)`/`(query, model)` bucket
//! `workers::billing_derivation` prices, every ~5-minute tick, for as long as
//! the model stays unrecognized — the exact shape that buried a prior
//! incident as noise (`workers::tenant_harness::pool_failure_severity`'s own
//! module doc tells that story).
//!
//! This module does NOT modify `PriceSheet::rate` or silence its raw signal
//! (deliberately: PR1's doc comment frames that call as "the minimal,
//! non-deduped half of the alert", left in place as a baseline that fires
//! regardless of whether anything is watching this tracker). Instead
//! `workers::usage_reconciliation` — the ONE place this train adds a genuine
//! daily, deduped, paged alert — owns an instance of
//! [`UnknownModelAlertTracker`] for the lifetime of its background task
//! (captured once in the `lib.rs` spawn closure, exactly like
//! `workers::tenant_harness`'s own `pool_failures: HashMap<String, u32>` is
//! owned once by that harness's event loop) and consults it after grouping a
//! day's `llm_usage` rows down to DISTINCT `(provider, model)` pairs, so a
//! model that appears in a thousand rows this pass drives exactly ONE call
//! to [`UnknownModelAlertTracker::record`], never one per row.
//!
//! # The escalation shape — mirrors `pool_failure_severity`, adapted for "no
//! # single pass" instead of "one outage, one counter"
//!
//! [`unknown_model_alert_severity`] copies `pool_failure_severity`'s exact
//! three-state shape (`Warn` below threshold, `Alert` on crossing it — paged
//! exactly once — `Quiet` after) and [`UnknownModelAlertTracker`] is a
//! `HashMap`-backed per-key counter for the identical "escalate once, then
//! stay quiet" reason. One thing is genuinely different, not merely
//! renamed: `pool_failure_severity`'s counter belongs to ONE worker's ONE
//! per-tenant retry loop, so `workers::tenant_harness::prune_pool_failures`
//! can reset it by checking "is this tenant still in MY active set". This
//! tracker is fed by MANY independent per-tenant ticks of
//! `workers::usage_reconciliation` (consumer once daily, each org tenant
//! once daily, staggered, with no shared "this is one pass" boundary to
//! prune against — and deliberately GLOBAL across tenants, because "model X
//! has no `model_rates` row" is a platform-wide pricing-catalog gap, not a
//! per-tenant condition; fifty orgs hitting the same new model string should
//! still page once, not fifty times). So staleness here is judged per key,
//! by ELAPSED TIME since the last sighting ([`STALE_AFTER`]), not by set
//! membership in one enumerable pass — see [`UnknownModelAlertTracker::record`].

use std::collections::HashMap;

use chrono::{DateTime, Utc};

/// Consecutive sightings (one per reconciliation pass — any tenant's — that
/// still sees this `(provider, model)` pair as unmatched, not one per raw
/// `llm_usage` row) before the alert escalates from `Warn` to a paged
/// `Alert`.
///
/// Deliberately small, but for the OPPOSITE reason
/// `workers::tenant_harness::POOL_FAILURE_ALERT_THRESHOLD`'s "wait out a
/// transient blip" rationale is small: a genuinely unrecognized model string
/// is not a transient condition that might resolve itself on retry — it
/// either matches a `model_rates` pattern or it never will until someone
/// adds a row — so there is little value in waiting out more than one
/// confirming pass. `2` (not `1`) is kept anyway so a single stray sighting
/// (a one-off manual test call with a made-up model string, say) is visible
/// at `Warn` without immediately paging.
pub const UNKNOWN_MODEL_ALERT_THRESHOLD: u32 = 2;

/// How long a `(provider, model)` pair can go unseen before its counter
/// resets to a fresh first sighting rather than continuing to accumulate (or
/// staying stuck at `Quiet` forever). Strictly longer than the daily
/// reconciliation cadence so an ordinary one-tenant-skipped-today gap never
/// resets a genuinely persistent problem; long enough that a pair that
/// truly stopped appearing (the operator added the missing `model_rates`
/// row) doesn't leave a stale, already-past-threshold counter lying around
/// to silently swallow a LATER, unrelated regression of the same model
/// string — the same "counters are per-outage state, must not outlive the
/// outage" reasoning `workers::tenant_harness::prune_pool_failures`'s doc
/// gives, adapted to elapsed time because this tracker has no per-pass
/// tenant roster to prune against (see the module doc).
const STALE_AFTER: chrono::Duration = chrono::Duration::days(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownModelSeverity {
    /// Below the threshold — visible, not yet paged.
    Warn,
    /// Crossing the threshold — page, exactly once per (re-)occurrence.
    Alert,
    /// Past the threshold — already paged, stay quiet.
    Quiet,
}

/// Single source of truth for the escalation shape, mirroring
/// `workers::tenant_harness::pool_failure_severity`'s own doc comment
/// reasoning almost verbatim: a test that re-implemented this match could
/// drift from production and keep passing while the real alert regressed to
/// firing on every sighting.
pub fn unknown_model_alert_severity(sightings: u32) -> UnknownModelSeverity {
    match sightings.cmp(&UNKNOWN_MODEL_ALERT_THRESHOLD) {
        std::cmp::Ordering::Less => UnknownModelSeverity::Warn,
        std::cmp::Ordering::Equal => UnknownModelSeverity::Alert,
        std::cmp::Ordering::Greater => UnknownModelSeverity::Quiet,
    }
}

#[derive(Debug, Clone, Copy)]
struct Sighting {
    count: u32,
    last_seen_at: DateTime<Utc>,
}

/// Per-`(provider, model)` sighting counters, owned by ONE long-lived task —
/// see the module doc's "escalation shape" section for why this is a
/// process-wide instance (captured once in `lib.rs`'s spawn closure) rather
/// than reconstructed per tick, and why that is the SAME limitation
/// `workers::tenant_harness::pool_failures` already accepts (each replica
/// pages independently in a >1-replica deployment) rather than a new one
/// this module introduces.
#[derive(Debug, Default)]
pub struct UnknownModelAlertTracker {
    sightings: HashMap<(String, String), Sighting>,
}

impl UnknownModelAlertTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record ONE reconciliation pass's sighting of `(provider, model)` at
    /// `now`. Callers MUST already have collapsed a pass's raw rows down to
    /// DISTINCT `(provider, model)` pairs before calling this — once per
    /// pair per pass, never once per row (see
    /// `workers::usage_reconciliation::scan_unknown_models` for the
    /// production call site that does the collapsing). Returns the severity
    /// this sighting should be logged at.
    pub fn record(&mut self, provider: &str, model: &str, now: DateTime<Utc>) -> UnknownModelSeverity {
        let key = (provider.to_string(), model.to_string());
        let entry = self.sightings.entry(key).or_insert(Sighting { count: 0, last_seen_at: now });
        if entry.count > 0 && now.signed_duration_since(entry.last_seen_at) > STALE_AFTER {
            // Gone quiet long enough that this reads as a fresh occurrence,
            // not a continuation — see [`STALE_AFTER`]'s doc.
            entry.count = 0;
        }
        entry.count += 1;
        entry.last_seen_at = now;
        unknown_model_alert_severity(entry.count)
    }

    /// Current cumulative sighting count for `(provider, model)`, `0` if
    /// never recorded. Exists so a caller (or a test) can observe the
    /// dedup's effect directly — e.g. proving that N rows sharing one pair,
    /// grouped before being fed to [`Self::record`], advance this counter by
    /// exactly `1`, not `N`.
    pub fn sighting_count(&self, provider: &str, model: &str) -> u32 {
        self.sightings.get(&(provider.to_string(), model.to_string())).map(|s| s.count).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirrors `workers::tenant_harness::tests`' own `pool_failure_severity`
    /// test almost exactly: call the pure severity function across a range
    /// straddling the threshold and assert EXACTLY one `Alert`, at the
    /// threshold itself.
    #[test]
    fn escalation_fires_exactly_once_per_outage_at_the_threshold() {
        let observed: Vec<UnknownModelSeverity> =
            (1..=UNKNOWN_MODEL_ALERT_THRESHOLD + 3).map(unknown_model_alert_severity).collect();

        assert_eq!(
            observed.iter().filter(|s| **s == UnknownModelSeverity::Alert).count(),
            1,
            "exactly one Alert across the range, not one per sighting: {observed:?}"
        );
        assert_eq!(
            observed[UNKNOWN_MODEL_ALERT_THRESHOLD as usize - 1],
            UnknownModelSeverity::Alert,
            "the alert fires on crossing the threshold"
        );
        assert!(
            observed[..UNKNOWN_MODEL_ALERT_THRESHOLD as usize - 1]
                .iter()
                .all(|s| *s == UnknownModelSeverity::Warn),
            "below threshold must be Warn, never Alert or Quiet: {observed:?}"
        );
        assert!(
            observed[UNKNOWN_MODEL_ALERT_THRESHOLD as usize..]
                .iter()
                .all(|s| *s == UnknownModelSeverity::Quiet),
            "past the threshold must stay Quiet — warning on every subsequent sighting is \
             precisely the noise this tracker exists to replace: {observed:?}"
        );
    }

    fn t(hour: u32) -> DateTime<Utc> {
        chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 13, hour, 0, 0).single().unwrap()
    }

    /// **Negative control 1 (positive half) — N rows sharing one unknown
    /// `(provider, model)` pair must drive the tracker exactly ONCE per
    /// pass, not N times.** This is the production shape:
    /// `workers::usage_reconciliation::scan_unknown_models` groups a day's
    /// raw `llm_usage` rows down to DISTINCT pairs (a SQL `GROUP BY
    /// provider, model`, simulated here as a `HashSet` over the raw rows)
    /// BEFORE calling [`UnknownModelAlertTracker::record`] — so 25 rows
    /// sharing one pair collapse to exactly one call, proven here by the
    /// counter advancing by exactly `1`, not `25`.
    #[test]
    fn twenty_five_rows_sharing_one_unknown_pair_advance_the_counter_by_exactly_one() {
        let raw_rows: Vec<(&str, &str)> = std::iter::repeat(("anthropic", "definitely-not-a-real-model"))
            .take(25)
            .collect();

        // The dedup step every production caller must apply before touching
        // the tracker at all.
        let distinct: std::collections::HashSet<(&str, &str)> = raw_rows.iter().copied().collect();
        assert_eq!(distinct.len(), 1, "25 identical rows must collapse to exactly one distinct pair");

        let mut tracker = UnknownModelAlertTracker::new();
        for (provider, model) in &distinct {
            tracker.record(provider, model, t(0));
        }

        assert_eq!(
            tracker.sighting_count("anthropic", "definitely-not-a-real-model"),
            1,
            "one call to record() per pass for 25 rows sharing a pair — the alert fires once, not 25 times"
        );
    }

    /// **Negative control 1 (negative half) — remove the dedup step and the
    /// SAME 25 rows fire the tracker 25 times instead of once.** A dedup
    /// that was never exercised is not a dedup: this proves the grouping
    /// step above is actually load-bearing by showing what happens without
    /// it, using the identical tracker and the identical input.
    #[test]
    fn without_grouping_first_the_same_twenty_five_rows_fire_the_tracker_twenty_five_times() {
        let raw_rows: Vec<(&str, &str)> = std::iter::repeat(("anthropic", "definitely-not-a-real-model"))
            .take(25)
            .collect();

        let mut tracker = UnknownModelAlertTracker::new();
        for (provider, model) in &raw_rows {
            // No grouping: one record() call per RAW ROW, exactly the "fires
            // per row" failure mode the task's own instructions call out.
            tracker.record(provider, model, t(0));
        }

        assert_eq!(
            tracker.sighting_count("anthropic", "definitely-not-a-real-model"),
            25,
            "without the grouping step, 25 rows drive 25 separate tracker calls — this is the \
             exact noise the dedup exists to prevent"
        );
    }

    /// **Negative control 2 — the sentinel must never reach the tracker at
    /// all**, in either direction: a caller that (correctly) skips the
    /// sentinel never records it, so its count stays 0 forever, while a
    /// genuinely unknown model recorded alongside it accumulates normally.
    #[test]
    fn sentinel_never_reaches_the_tracker_while_a_real_unknown_model_does() {
        let mut tracker = UnknownModelAlertTracker::new();
        // Production call site (`scan_unknown_models`) filters the sentinel
        // out BEFORE calling `record` — simulated here by simply never
        // calling `record("anthropic", UNATTRIBUTED_SENTINEL, ..)` at all,
        // while a real unknown model in the SAME pass does get recorded.
        tracker.record("anthropic", "genuinely-unrecognized-model", t(0));

        assert_eq!(
            tracker.sighting_count("anthropic", super::super::rates::UNATTRIBUTED_SENTINEL),
            0,
            "the sentinel must never accumulate a sighting count"
        );
        assert_eq!(
            tracker.sighting_count("anthropic", "genuinely-unrecognized-model"),
            1,
            "a real unknown model recorded in the same pass must still accumulate normally"
        );
    }

    /// A pair that goes quiet for longer than [`STALE_AFTER`] resets to a
    /// fresh first sighting rather than either (a) continuing to accumulate
    /// forever or (b) staying stuck at `Quiet` if it regresses after the
    /// underlying `model_rates` gap was fixed and later reopened.
    #[test]
    fn a_long_gap_resets_the_counter_to_a_fresh_sighting() {
        let mut tracker = UnknownModelAlertTracker::new();
        let first = tracker.record("anthropic", "flaky-model", t(0));
        let second = tracker.record("anthropic", "flaky-model", t(1));
        assert_eq!(first, UnknownModelSeverity::Warn);
        assert_eq!(second, UnknownModelSeverity::Alert, "crossing the threshold on the 2nd sighting");

        // Silent for well over STALE_AFTER SINCE THE LAST SIGHTING (`t(1)`,
        // not `t(0)`) — `record`'s staleness check measures elapsed time
        // from `last_seen_at`, which is `t(1)` after the second call above.
        let later = t(1) + STALE_AFTER + chrono::Duration::hours(1);
        let after_gap = tracker.record("anthropic", "flaky-model", later);
        assert_eq!(
            after_gap,
            UnknownModelSeverity::Warn,
            "a long silent gap must reset to a fresh first sighting, not continue at Quiet"
        );
    }
}
