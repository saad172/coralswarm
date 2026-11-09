//! Access-log purge worker (platform-admin-metrics PR 1 of 7).
//!
//! `user_access_log` (migration `0095_platform_metrics`) is a 30-day, raw-IP
//! login audit trail — see that migration's header for the full retention
//! rationale. Left unbounded it accumulates forever exactly like the
//! `op_log` tombstones `workers::tombstone_purge` exists to sweep; this
//! worker is modelled closely on that one (same constant shapes, same
//! bounded-timeout-then-log-and-retry error handling, same tracing
//! conventions) and differs only in WHERE it runs and WHY.
//!
//! ## Why this is NOT a `workers::tenant_harness` sweep worker
//!
//! Every existing sweep worker in this module (`tombstone_purge`, `reaper`,
//! `embed_retry`) is wired through `tenant_harness::run_multi_tenant` because
//! the table each one sweeps (`op_log`, `jobs`, atoms/nodes queues) exists
//! ONCE PER TENANT — every org tenant DB and the shared consumer DB each have
//! their own copy, so the sweep genuinely needs to loop over every known
//! tenant. `user_access_log` is different in kind, not just in name: it is a
//! CONTROL-ONLY table (migration 0095's header) with no tenant-tree
//! counterpart at all, exactly one physical copy across the whole system,
//! living in the same database as `users`/`accounts`. There is no tenant
//! dimension to loop over — a single `DELETE ... WHERE created_at < cutoff`
//! against the one control pool is the complete sweep, so this worker owns a
//! small standalone loop instead of registering with the harness.
//!
//! It is also NOT RLS-scoped (unlike `tombstone_purge`'s consumer-plane
//! per-account loop over `op_log`): `user_access_log` was deliberately never
//! added to migration `0083_consumer_rls`'s policy roster (it is
//! platform-operator data, not per-account content — see
//! `scripts/rls_coverage.manifest`'s header for the "deliberate non-scoped
//! surface" category this falls into), so a single unscoped `DELETE` across
//! every user's rows is exactly the right shape, not a shortcut around a
//! missing scope.
//!
//! ## Shutdown
//!
//! A periodic sweeper, not a job-queue drain — same category `workers::
//! reaper`/`workers::embed_retry` fall into per `shutdown.rs`'s module doc:
//! nothing here is worth a bounded grace window (one indexed `DELETE` is not
//! a multi-step job with an LLM call or blob fetch to finish), so this only
//! needs to stop STARTING new sweeps and cancel its inter-sweep sleep
//! promptly on shutdown. [`poll_until_triggered`] + `tokio::select!` against
//! the sleep is the same shutdown race `workers::embed_retry` uses.

use std::time::Duration;

use chrono::Utc;
use sqlx::PgPool;

use crate::shutdown::Shutdown;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// Retention window for `user_access_log` rows (migration 0095's header:
/// "RETENTION: 30 DAYS, RAW IP DATA"). Named so the one number the
/// migration's retention promise depends on lives in exactly one place.
pub const ACCESS_LOG_RETENTION_DAYS: i64 = 30;

/// How often the purge runs. Daily is ample for a 30-day retention window,
/// mirroring `workers::tombstone_purge::PURGE_INTERVAL`.
pub const PURGE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Per-sweep timeout on the DB call, mirroring `workers::tombstone_purge`'s
/// `PURGE_TIMEOUT`: a purge that hasn't finished in a minute is stuck (pool
/// exhaustion, network partition) and should degrade to a logged failure
/// rather than parking this worker's loop forever.
const PURGE_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the inter-sweep sleep polls [`Shutdown`] — same value and
/// rationale as `workers::embed_retry::SHUTDOWN_POLL_INTERVAL` /
/// `workers::reaper`'s: fast enough that a SIGTERM is noticed promptly
/// without busy-polling.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ── Worker entry point ────────────────────────────────────────────────────────

/// Run the access-log-purge worker loop until `shutdown` is signalled.
///
/// Spawned directly from `lib.rs` with the control pool — no tenant
/// dimension to thread through (see the module doc). Fires once immediately
/// on start (matching the old `tokio::time::interval` fire-once-at-boot
/// behavior every other sweep worker in this codebase preserves), then every
/// [`PURGE_INTERVAL`] thereafter.
pub async fn run(pool: PgPool, shutdown: Shutdown) {
    tracing::info!("access-log-purge worker started");

    loop {
        if shutdown.is_triggered() {
            tracing::info!("access-log-purge worker: shutdown signalled, stopping");
            return;
        }

        match tick(&pool).await {
            Ok(0) => tracing::debug!("access log purge: nothing expired"),
            Ok(n) => tracing::info!(purged = n, "access log purge complete"),
            Err(e) => {
                tracing::warn!(error = %e, "access log purge failed; will retry next cycle")
            }
        }

        // Inter-sweep sleep, racing shutdown so a SIGTERM during the 24h wait
        // is noticed within SHUTDOWN_POLL_INTERVAL instead of riding out the
        // full interval — same shape as `workers::embed_retry`'s
        // `enqueue_racing_shutdown`.
        tokio::select! {
            _ = tokio::time::sleep(PURGE_INTERVAL) => {}
            _ = poll_until_triggered(&shutdown) => {
                tracing::info!("access-log-purge worker: shutdown signalled during inter-sweep sleep, stopping");
                return;
            }
        }
    }
}

/// Resolve once `shutdown` is triggered, polling every
/// [`SHUTDOWN_POLL_INTERVAL`]. Never returns otherwise — used only as the
/// "shutdown" arm of [`run`]'s `tokio::select!`. Same pattern as
/// `workers::embed_retry::poll_until_triggered`.
async fn poll_until_triggered(shutdown: &Shutdown) {
    loop {
        if shutdown.is_triggered() {
            return;
        }
        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }
}

/// One purge sweep: delete every `user_access_log` row older than
/// [`ACCESS_LOG_RETENTION_DAYS`]. `pub` (like `workers::content_tsv_backfill::
/// process_job`) so integration tests can drive one sweep against a real
/// Postgres without spawning [`run`]'s infinite loop.
///
/// Bounded by [`PURGE_TIMEOUT`]: a hung `DELETE` (pool exhaustion, network
/// partition) degrades to a logged failure and the next cycle retries,
/// rather than blocking this worker's loop indefinitely — same reasoning as
/// `workers::tombstone_purge::tick`.
pub async fn tick(pool: &PgPool) -> Result<u64, String> {
    let cutoff = Utc::now() - chrono::Duration::days(ACCESS_LOG_RETENTION_DAYS);

    match tokio::time::timeout(PURGE_TIMEOUT, delete_expired(pool, cutoff)).await {
        Err(_) => Err(format!(
            "access log purge timed out after {}s",
            PURGE_TIMEOUT.as_secs()
        )),
        Ok(Err(e)) => Err(e.to_string()),
        Ok(Ok(n)) => Ok(n),
    }
}

async fn delete_expired(
    pool: &PgPool,
    cutoff: chrono::DateTime<Utc>,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query("DELETE FROM user_access_log WHERE created_at < $1")
        .bind(cutoff)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_and_interval_constants_are_sane() {
        // Regression guard: the migration's documented "RETENTION: 30 DAYS"
        // promise and this worker's constant must never silently drift apart.
        assert_eq!(ACCESS_LOG_RETENTION_DAYS, 30);
        assert!(PURGE_INTERVAL <= Duration::from_secs(24 * 60 * 60));
        assert!(PURGE_TIMEOUT < PURGE_INTERVAL);
    }
}
