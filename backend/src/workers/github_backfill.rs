//! GitHub backfill worker (docs/connectors/github.md §3.6) — **Phase 2: the
//! real worker.** Drains `JobKind::GithubBackfill { repo_row_id }` and drives
//! `connectors::github_backfill::run_tick` — a thin tenant-loop wrapper
//! around that module's real fetch/normalise/ingest/cursor engine. See that
//! module's own doc comment for the cursor shape, time-slicing, and
//! rate-limit contracts this worker's `process_job` implements the queue
//! side of.
//!
//! Tenant loop, same reasoning as `workers::slack_ingest`: `github_watched_
//! repos`/`jobs` are tenant-tree tables (paired migration `0230`/`0093`), so
//! a backfill job for an org account lands on that org's own tenant `jobs`
//! table, invisible to a dequeuer scoped to any other plane.
//!
//! # Continuation contract
//!
//! `jobs::queue::JobOutcome` has exactly two variants (`DidWork`/
//! `NothingToDo`) — there is no "leave this job pending with updated state"
//! primitive, by design (see that type's own doc comment). So a
//! `RunOutcome::Continue` tick **completes** the current job (`DidWork` —
//! real progress was durably persisted to `github_watched_repos.narrative_
//! cursor`/`backfill_progress`) and re-enqueues a FRESH job with the SAME
//! `repo_row_id` and the SAME idempotency key
//! (`github_backfill:{repo_row_id}`) `routes::github::put_repos` used for the
//! original enqueue — mirroring the `SlackNameBackfill`/`BackfillAtomTitles`
//! continuation convention already established in this tree. A
//! `RateLimited` tick does the same, but with `delay_secs` set to GitHub's
//! own reported wait.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::connectors::github_backfill::{
    classify_tick_failure, clear_sync_error, record_sync_error, run_tick, RunOutcome,
};
use crate::jobs::{self, queue::{self, FailureClass, JobOutcome, JobRow}};
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

const JOB_BATCH_SIZE: usize = 8;
pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

pub async fn tick(t: &WorkerTenant, state: &Arc<AppState>) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["GithubBackfill"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(t, state, job).await;
    }
    Ok(Tick::worked(n))
}

async fn process_job(t: &WorkerTenant, state: &Arc<AppState>, job: JobRow) {
    let repo_row_id: Option<Uuid> = job.payload.get("repo_row_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok());
    let Some(repo_row_id) = repo_row_id else {
        tracing::error!(job_id = %job.id, payload = %job.payload, "github_backfill: malformed payload (missing repo_row_id)");
        if let Err(e) = queue::could_not_do(&t.pool, job.id, FailureClass::Structural, "malformed payload: missing repo_row_id").await {
            tracing::error!(error = %e, job_id = %job.id, "github_backfill: failed to dead-letter malformed job");
        }
        return;
    };
    let Some(account_id) = job.account_id else {
        tracing::error!(job_id = %job.id, %repo_row_id, "github_backfill: job carries no account_id — refusing unscoped");
        if let Err(e) = queue::could_not_do(&t.pool, job.id, FailureClass::Structural, "job missing account_id").await {
            tracing::error!(error = %e, job_id = %job.id, "github_backfill: failed to dead-letter unscoped job");
        }
        return;
    };

    let outcome = run_tick(&t.tenant, &t.pool, state, account_id, repo_row_id).await;

    // ── Make the outcome visible on the REPO ROW, not just in `jobs` ────────
    // (migration `0250`/`0102`; see `record_sync_error`'s doc comment for the
    // incident). Done here, around the whole `match` below, rather than
    // inside each arm: the rule is "every tick that reached a verdict records
    // that verdict", and spreading it across five arms is how one of them
    // eventually stops doing it. Both calls are best-effort — a failure to
    // record WHY a tick failed must never become a second reason it failed —
    // so each only logs.
    //
    // `Paused` clears too: an inactive row is not a failing one, and leaving
    // a stale error on a repo an admin has deliberately toggled off would
    // make the badge say the wrong thing about a state they chose.
    //
    // Only a CLASSIFICATION reaches the row — `routes::github::list_repos`
    // serializes that column, and the worker's error string is internal
    // detail that must not leave through an API response (CodeRabbit round
    // 1, Major/Security). The verbatim message still goes to
    // `jobs.error_message` via the `queue::fail` call in the `Err` arm below
    // and to the `tracing::warn!(error = %e, …)` line beside it — both
    // operator-only. `classify_tick_failure` re-checks the destination's
    // reachability against the live row rather than parsing that message, so
    // the label is grounded in a fact rather than in wording.
    match &outcome {
        Ok(_) => {
            if let Err(e) = clear_sync_error(&t.pool, account_id, repo_row_id).await {
                tracing::warn!(error = %e, %repo_row_id, "github_backfill: could not clear the repo row's sync_error_code");
            }
        }
        Err(_) => {
            let code = classify_tick_failure(state, &t.tenant, &t.pool, account_id, repo_row_id).await;
            if let Err(re) = record_sync_error(&t.pool, account_id, repo_row_id, code).await {
                tracing::warn!(error = %re, %repo_row_id, "github_backfill: could not record the repo row's sync_error_code");
            }
        }
    }

    match outcome {
        Ok(RunOutcome::Continue) => {
            if let Err(e) = enqueue_continuation(&t.pool, account_id, repo_row_id, 0).await {
                tracing::error!(error = %e, %repo_row_id, "github_backfill: failed to enqueue continuation job");
                if let Err(qe) = queue::fail(&t.pool, job.id, &e.to_string()).await {
                    tracing::error!(error = %qe, job_id = %job.id, "github_backfill: failed to mark job as failed after enqueue error");
                }
                return;
            }
            if let Err(e) = queue::complete(&t.pool, job.id, JobOutcome::DidWork).await {
                tracing::error!(error = %e, job_id = %job.id, "github_backfill: failed to complete DidWork job");
            }
        }
        Ok(RunOutcome::RateLimited(retry_after)) => {
            tracing::info!(job_id = %job.id, %repo_row_id, retry_after_secs = retry_after.as_secs(), "github_backfill: rate limited, rescheduling");
            if let Err(e) = enqueue_continuation(&t.pool, account_id, repo_row_id, retry_after.as_secs() as i64).await {
                tracing::error!(error = %e, %repo_row_id, "github_backfill: failed to enqueue rate-limit continuation job");
                if let Err(qe) = queue::fail(&t.pool, job.id, &e.to_string()).await {
                    tracing::error!(error = %qe, job_id = %job.id, "github_backfill: failed to mark job as failed after enqueue error");
                }
                return;
            }
            if let Err(e) = queue::complete(&t.pool, job.id, JobOutcome::DidWork).await {
                tracing::error!(error = %e, job_id = %job.id, "github_backfill: failed to complete rate-limited job");
            }
        }
        Ok(RunOutcome::Done) => {
            tracing::info!(job_id = %job.id, %repo_row_id, "github_backfill: fully synced");
            if let Err(e) = queue::complete(&t.pool, job.id, JobOutcome::DidWork).await {
                tracing::error!(error = %e, job_id = %job.id, "github_backfill: failed to complete Done job");
            }
        }
        Ok(RunOutcome::Paused) => {
            tracing::info!(job_id = %job.id, %repo_row_id, "github_backfill: repo is not active — nothing to do, not re-enqueuing");
            if let Err(e) = queue::complete(&t.pool, job.id, JobOutcome::NothingToDo).await {
                tracing::error!(error = %e, job_id = %job.id, "github_backfill: failed to complete Paused job");
            }
        }
        Err(e) => {
            tracing::warn!(job_id = %job.id, %repo_row_id, error = %e, "github_backfill: tick failed transiently — retrying on the ordinary backoff ladder");
            if let Err(qe) = queue::fail(&t.pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "github_backfill: failed to mark job as failed");
            }
        }
    }
}

async fn enqueue_continuation(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    repo_row_id: Uuid,
    delay_secs: i64,
) -> Result<(), sqlx::Error> {
    queue::enqueue(
        pool,
        &jobs::JobKind::GithubBackfill { repo_row_id },
        Some(account_id),
        Some(&format!("github_backfill:{repo_row_id}")),
        delay_secs,
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_poll_sleep_is_reasonable() {
        assert!(EMPTY_POLL_SLEEP >= Duration::from_millis(100));
        assert!(EMPTY_POLL_SLEEP <= Duration::from_secs(5));
    }
}
