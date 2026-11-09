//! Job-queue reaper (backstop for crashed/killed workers).
//!
//! `jobs::queue`'s module docs describe the gap this worker closes: a worker
//! that claims a job (`status = 'running'`) and then dies — Fly deploys,
//! HTTP-idle autostop, OOM — leaves that row `running` forever. Nothing else
//! in the queue ever revisits it, so the job silently vanishes from the
//! pipeline. In production this orphaned 5 `EmbedAtoms` jobs over 3 days
//! (all `attempts=0`, no error, claimed milliseconds after creation, worker
//! dead moments later); because `ExtractEntities` / `ExtractRelationships`
//! are only enqueued *after* embedding completes
//! (`workers::embed::process_job`), every orphaned embed batch silently
//! produced zero downstream entities/decisions/reefs, with nothing anywhere
//! logging an error to alert on.
//!
//! **OOM is a confirmed producer of these orphans, not just a hypothetical
//! one.** A separate incident reproduced in prod: `EmbedAtoms` jobs OOM-killed
//! the entire backend process (`kernel: Out of memory: Killed process
//! (coralswarm-back)`, Fly `exit_code=137, oom_killed=true`) roughly 1.5s
//! after claim, root-caused to `packages/core/src/embed_ort.rs` padding every
//! embed batch to a fixed 512 tokens regardless of content combined with
//! `workers::embed::EMBED_BATCH_SIZE` at 64 — see the doc comments on
//! `embed_ort::DEFAULT_MAX_LENGTH` and `workers::embed::EMBED_BATCH_SIZE` for
//! the exact byte-for-byte numbers and the fix. A SIGKILL from the OOM killer
//! bypasses `queue::complete`/`queue::fail` entirely — exactly like a Fly
//! deploy or autostop kill — so it orphans the job in `running` the same way.
//! **Important interaction:** without the `embed_ort.rs`/`EMBED_BATCH_SIZE`
//! fix, this reaper alone would make the incident *worse*, not better —
//! requeuing an OOM-doomed job just retries it into a recurring crash loop,
//! and each crash also kills HTTP serving on that machine, not just the
//! embed worker. The reaper is a safe backstop only once the underlying OOM
//! is fixed; both fixes ship together in the same change for that reason.
//!
//! This worker runs a sweep every [`REAP_INTERVAL`], calling
//! `jobs::queue::reap` to reset `running` jobs whose `started_at` is older
//! than [`stale_after`] back to `pending` (with `attempts` incremented,
//! mirroring `queue::fail`'s bookkeeping) so a healthy worker re-claims them.
//! A job reaped so many times it would exceed its `max_attempts` is instead
//! moved to the terminal `dead` state with a clear `error_message`, so a job
//! that reliably kills its worker doesn't loop between "reaped" and
//! "claimed, killed again" forever — see `queue::reap` for the exact
//! requeue-vs-dead logic.
//!
//! Shutdown and the inter-sweep sleep are owned by the tenant harness that
//! drives [`tick`] (`workers::tenant_harness`, org-tenancy PR 2.1): the
//! harness stops scheduling ticks as soon as shutdown fires and caps its
//! scheduler sleeps at its own shutdown-poll interval, so a shutdown mid
//! [`REAP_INTERVAL`] is still noticed promptly. This worker is not given a
//! post-HTTP-drain grace window in `lib.rs::run()` — see [`tick`]'s doc
//! comment for why a sub-second sweep doesn't need one.

use std::time::Duration;

use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// How often the reaper sweeps for stale `running` jobs.
///
/// A fraction of [`stale_after`]'s default (20 minutes) so a job orphaned
/// right after a sweep is still caught reasonably close to the threshold
/// (worst case: just under `stale_after() + REAP_INTERVAL` before it's
/// noticed), without polling so often it adds meaningful load — the query is
/// a single indexed `UPDATE … WHERE status = 'running'` either way.
/// `pub(crate)`: fed to the tenant harness as this worker's sweep cadence
/// (see `workers::tenant_harness`).
pub(crate) const REAP_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Number of jobs `workers::extract` processes **sequentially** (a plain
/// `for` loop, not concurrent) per poll batch, each making at least one LLM
/// call bounded by the operator-tunable `LLM_TIMEOUT_SECS` — mirrors
/// `extract::JOB_BATCH_SIZE`, which is private to that module (kept as its
/// own named constant here, rather than reaching into `extract`, so this
/// derivation is readable in isolation; update both if `extract`'s batch
/// size ever changes).
///
/// This is the worst-case sequential LLM fan-out [`stale_after`] must budget
/// for: with `LLM_TIMEOUT_SECS` fixed at its historical default (60s) this
/// worst case is ~8 minutes, but a deployment that tunes `LLM_TIMEOUT_SECS`
/// up (e.g. for a slower/larger model) scales this budget linearly instead
/// of silently falling behind a hardcoded threshold.
const N_SEQUENTIAL_LLM_CALLS: u64 = 8;

/// Extra headroom added on top of the raw worst-case LLM-call budget so a
/// batch that finishes right at the timeout boundary (scheduling jitter, a
/// slow DB round-trip between calls, etc.) is never reaped out from under a
/// worker that is about to legitimately finish it.
const STALE_AFTER_SAFETY_MARGIN: Duration = Duration::from_secs(5 * 60);

/// Floor on [`stale_after`]: even if `LLM_TIMEOUT_SECS` were tuned very low,
/// never treat a job as orphaned faster than this. `EmbedAtoms` makes no LLM
/// calls at all, so it isn't covered by the `LLM_TIMEOUT_SECS`-derived budget
/// above, yet up to `embed::JOB_BATCH_SIZE` (8) jobs are dequeued and
/// processed concurrently, each with an unbounded `atom_ids` list chunked
/// into `embed::EMBED_BATCH_SIZE` (64)-atom embed calls plus a per-atom R2
/// fetch — a large re-embed/backfill batch can legitimately run several
/// minutes end to end. **Residual assumption:** this floor assumes embed
/// jobs stay in the "several minutes, not more" range that holds true
/// post-OOM-fix (see the module doc comment above); if embed batch duration
/// ever becomes unbounded (e.g. `EMBED_BATCH_SIZE` grows substantially, or a
/// new job kind with open-ended duration is added), a fixed floor stops being
/// suffient and the real fix is heartbeat/lease renewal (the worker
/// periodically extends its own claim while still alive, so the reaper only
/// ever reaps rows with no recent heartbeat) rather than raising this
/// constant further — deliberately NOT built in this PR; noted here as the
/// escalation path.
const STALE_AFTER_FLOOR: Duration = Duration::from_secs(20 * 60);

/// How long a `running` job may go without finishing before the reaper
/// treats it as orphaned by a dead worker, rather than merely slow.
///
/// `STALE_AFTER = max(STALE_AFTER_FLOOR, N_SEQUENTIAL_LLM_CALLS *
/// LLM_TIMEOUT_SECS + STALE_AFTER_SAFETY_MARGIN)`, using the *same*
/// `LLM_TIMEOUT_SECS` accessor (`crate::llm_timeout_secs`) the LLM HTTP
/// client itself is built from — see that function's doc comment. This
/// closes the gap a fixed 20-minute constant had: `LLM_TIMEOUT_SECS` is
/// operator-tunable and `extract` legitimately runs up to
/// [`N_SEQUENTIAL_LLM_CALLS`] of them sequentially per poll batch, so a
/// deployment that raises the per-call timeout well above its 60s default
/// could previously make a still-legitimately-running extract batch exceed a
/// fixed 20-minute threshold — the reaper would then reset a *live* job
/// mid-processing, risking duplicate side effects (duplicate
/// entities/relationships written once the original worker also finishes).
///
/// With the historical 60s default this evaluates to exactly the previous
/// fixed constant (8 * 60 + 300 = 780s, below the 1200s floor → 20 minutes),
/// so behaviour is unchanged for the common case; it only grows past 20
/// minutes once an operator actually tunes `LLM_TIMEOUT_SECS` up.
pub fn stale_after() -> Duration {
    derive_stale_after(crate::llm_timeout_secs())
}

/// Pure derivation logic behind [`stale_after`], factored out so the
/// threshold math is unit-testable without touching the environment —
/// mirrors `jobs::queue::backoff_secs` / `reap_should_dead_letter`'s
/// pure-logic-helper pattern.
fn derive_stale_after(llm_timeout_secs: u64) -> Duration {
    let llm_budget = Duration::from_secs(llm_timeout_secs.saturating_mul(N_SEQUENTIAL_LLM_CALLS))
        + STALE_AFTER_SAFETY_MARGIN;
    STALE_AFTER_FLOOR.max(llm_budget)
}

/// Per-sweep timeout on the DB call, mirroring `tombstone_purge`'s defensive
/// pattern: a sweep that hasn't finished in 30s is stuck (pool exhaustion,
/// network partition); bound it so a hung query degrades to a logged failure
/// and the next tick still runs, rather than parking `ticker.tick()` forever.
const REAP_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on how many stale rows a single sweep reaps (see the `limit` param on
/// `jobs::queue::reap`). If more than this pile up (e.g. after a bad deploy
/// that kills many workers mid-batch), the excess is left for the next
/// [`REAP_INTERVAL`] sweep rather than locking and rewriting an unbounded
/// number of rows in one transaction.
const REAP_BATCH_LIMIT: i64 = 100;

/// One reap sweep on one tenant's pool (the store is unused): reset stale
/// `running` jobs back to `pending` (or `dead` past `max_attempts`). Driven by
/// `workers::tenant_harness::run_multi_tenant` (org-tenancy PR 2.1) as a
/// sweep worker with cadence [`REAP_INTERVAL`] — every tenant DB has its own
/// `jobs` queue that can orphan `running` rows, so the reaper joins the
/// tenant loop. The harness owns the loop and the shutdown-aware inter-sweep
/// sleep (its scheduler sleeps are capped at its own shutdown-poll interval,
/// the same promptness the old in-loop `tokio::select!` race provided). The
/// reaper is **not** given a grace window in `lib.rs::run()`'s drain step — a
/// single sweep is one indexed `UPDATE` bounded by `REAP_TIMEOUT` (30s), not
/// a multi-step job with an LLM call or R2 fetch in it, so there's nothing
/// worth draining: not starting new sweeps is enough.
///
/// `stale_after` is derived once at startup from `LLM_TIMEOUT_SECS` (see
/// [`stale_after`]'s doc comment) and passed in by the spawn site — not
/// re-derived per sweep since the env var an operator tunes this from
/// doesn't change over a running process's lifetime.
pub async fn tick(t: &WorkerTenant, stale_after: Duration) -> Result<Tick, String> {
    let mut processed = 0;
    {
        let stale_after_secs = stale_after.as_secs() as i64;
        match tokio::time::timeout(
            REAP_TIMEOUT,
            // A dead-lettered receive job never ran its own completion
            // path, so the same statement redacts its payload.
            crate::jobs::queue::reap(
                &t.pool,
                stale_after_secs,
                REAP_BATCH_LIMIT,
                Some(&crate::workers::ingest_receive::REAP_REDACTION),
            ),
        )
        .await
        {
            Err(_) => tracing::warn!(
                timeout_secs = REAP_TIMEOUT.as_secs(),
                "reaper: sweep timed out; will retry next cycle"
            ),
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "reaper: sweep failed; will retry next cycle");
            }
            Ok(Ok(reaped)) if reaped.is_empty() => {
                tracing::debug!("reaper: no stale jobs found");
            }
            Ok(Ok(reaped)) => {
                processed = reaped.len();
                for job in &reaped {
                    let age_secs = (chrono::Utc::now() - job.claimed_at).num_seconds().max(0);
                    if job.permanently_failed {
                        tracing::warn!(
                            job_id = %job.id,
                            kind = %job.kind,
                            age_secs,
                            attempts = job.attempts,
                            "reaper: job exceeded max_attempts after repeated reaping — marked dead"
                        );
                    } else {
                        tracing::warn!(
                            job_id = %job.id,
                            kind = %job.kind,
                            age_secs,
                            attempts = job.attempts,
                            "reaper: reset orphaned 'running' job back to 'pending' (worker likely died mid-job)"
                        );
                    }
                }
                tracing::warn!(count = reaped.len(), "reaper: swept stale running jobs");
            }
        }
    }
    Ok(Tick::worked(processed))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_llm_timeout_matches_previous_fixed_20_minute_constant() {
        // 8 * 60 + 300 = 780s, below the 1200s floor — must land exactly on
        // the floor so operators who never touch LLM_TIMEOUT_SECS see no
        // behaviour change from the previous hardcoded `STALE_AFTER`.
        assert_eq!(derive_stale_after(60), Duration::from_secs(20 * 60));
    }

    #[test]
    fn tuned_up_llm_timeout_pushes_past_the_floor() {
        // 8 * 200 + 300 = 1900s (~31.7 min) > 1200s floor — a deployment
        // that tunes LLM_TIMEOUT_SECS up must get a proportionally larger
        // stale threshold, not the fixed 20-minute one that motivated this
        // fix (PR #171 review: a legitimate 8-call sequential extract batch
        // could otherwise exceed a fixed threshold and get reaped mid-job).
        assert_eq!(
            derive_stale_after(200),
            Duration::from_secs(8 * 200 + 5 * 60)
        );
    }

    #[test]
    fn very_low_llm_timeout_still_respects_the_floor() {
        // Even a near-zero LLM_TIMEOUT_SECS must not push stale_after below
        // the 20-minute floor — EmbedAtoms makes no LLM calls at all and can
        // legitimately run several minutes on its own, unrelated to
        // LLM_TIMEOUT_SECS entirely.
        assert_eq!(derive_stale_after(1), Duration::from_secs(20 * 60));
        assert_eq!(derive_stale_after(0), Duration::from_secs(20 * 60));
    }

    #[test]
    fn derivation_is_monotonically_non_decreasing_in_llm_timeout() {
        // A higher configured LLM_TIMEOUT_SECS must never *shrink* the
        // stale-job threshold — nothing about the derivation should be able
        // to make the reaper more aggressive as the operator dials the
        // timeout up.
        let mut prev = derive_stale_after(0);
        for secs in [1, 30, 60, 90, 120, 180, 300, 600] {
            let cur = derive_stale_after(secs);
            assert!(
                cur >= prev,
                "stale_after must not decrease as llm_timeout_secs grows"
            );
            prev = cur;
        }
    }

    #[test]
    fn exactly_covers_the_documented_worst_case_extract_batch_plus_margin() {
        // The worst case this derivation exists to cover, spelled out
        // directly: N_SEQUENTIAL_LLM_CALLS sequential calls each taking the
        // full configured timeout, plus the safety margin.
        let llm_timeout_secs = 90;
        let expected = Duration::from_secs(N_SEQUENTIAL_LLM_CALLS * llm_timeout_secs)
            + STALE_AFTER_SAFETY_MARGIN;
        assert_eq!(
            derive_stale_after(llm_timeout_secs),
            expected.max(STALE_AFTER_FLOOR)
        );
    }
}
