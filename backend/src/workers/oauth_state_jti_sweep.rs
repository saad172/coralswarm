//! `oauth_state_jti` TTL sweep (foundation train PR 1.4,
//! `.scratch/m365-enterprise/plan.md`).
//!
//! `oauth_state_jti` (migration `0173_oauth_state_jti`) records every signed
//! OAuth `state` token's `jti` the moment `routes::oauth::callback` consumes
//! it (`oauth::jti_store::consume`), so a replayed token is rejected even
//! though its signature and expiry still check out. Left unbounded it
//! accumulates forever exactly like `user_access_log` does without
//! `workers::access_log_purge`; this worker is modelled directly on that one
//! (same "control-only table, standalone loop, no tenant dimension" shape)
//! and differs only in what it sweeps and why the retention window is safe.
//!
//! # Why this is NOT a `workers::tenant_harness` sweep worker
//!
//! Same reasoning as `workers::access_log_purge`'s own module doc: every
//! tenant-harness-registered sweep worker (`tombstone_purge`, `reaper`,
//! `usage_pending_sweep`) exists because the table it sweeps has one copy
//! PER TENANT. `oauth_state_jti` does not — OAuth `state` tokens are minted
//! and verified entirely against the control-plane `accounts` table (see
//! `routes::oauth`'s own file-header route table: `authorize`/`callback`
//! carry no Clerk JWT and resolve on the consumer/control plane by design),
//! so this table has exactly one physical copy, in the control DB, with no
//! tenant dimension to loop over. A single `DELETE ... WHERE consumed_at <
//! cutoff` against the one control pool is the complete sweep.
//!
//! # Why the retention window is always safe
//!
//! Any signed state token whose `jti` could still pass
//! `oauth::state::verify_state`'s own expiry check must have been minted
//! within the last `oauth::state::STATE_TTL_SECS` (600s) — `verify_state`
//! itself enforces `exp`. A consumed-jti row older than that can therefore
//! never again match a still-verifiable token, so purging it loses nothing:
//! the ONLY thing a surviving row could ever block is a replay of a token
//! that would already be rejected on expiry grounds alone. [`JTI_RETENTION_SECS`]
//! adds a clock-skew buffer on top of `STATE_TTL_SECS` for pure margin, not
//! because a tighter window would be incorrect.
//!
//! # Shutdown
//!
//! A periodic sweeper, not a job-queue drain — same category
//! `workers::access_log_purge`/`workers::reaper`/`workers::embed_retry` fall
//! into per `shutdown.rs`'s module doc: nothing here is worth a bounded
//! grace window (one indexed `DELETE` is not a multi-step job), so this only
//! needs to stop STARTING new sweeps and cancel its inter-sweep sleep
//! promptly on shutdown — the identical `poll_until_triggered` +
//! `tokio::select!` shape `workers::access_log_purge::run` uses.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::shutdown::Shutdown;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How long a consumed-`jti` row survives before the sweep purges it. See
/// this module's own doc, "Why the retention window is always safe" — the
/// `5 * 60` term is pure clock-skew margin between this process and whatever
/// wall clock signed the token, not a correctness requirement.
pub const JTI_RETENTION_SECS: i64 = crate::oauth::state::STATE_TTL_SECS + 5 * 60;

/// How often the purge runs. Comfortably shorter than
/// [`JTI_RETENTION_SECS`] so expired rows do not linger for anywhere close
/// to their own retention window before being reaped, mirroring
/// `workers::access_log_purge::PURGE_INTERVAL`'s "ample for the retention
/// window" posture.
pub const PURGE_INTERVAL: Duration = Duration::from_secs(3 * 60);

/// Per-sweep DB-call timeout, mirroring `workers::access_log_purge::
/// PURGE_TIMEOUT` / `workers::tombstone_purge::PURGE_TIMEOUT`: a hung
/// `DELETE` (pool exhaustion, network partition) degrades to a logged
/// failure and retries next cycle rather than parking this worker's loop
/// forever.
const PURGE_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the inter-sweep sleep polls [`Shutdown`] — same value and
/// rationale as `workers::access_log_purge::SHUTDOWN_POLL_INTERVAL`: fast
/// enough that a SIGTERM is noticed promptly without busy-polling.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ── Errors ────────────────────────────────────────────────────────────────────

/// Typed error for this sweep's single fallible step (the timed `DELETE`) —
/// per this repo's convention of defining typed errors with `thiserror`
/// rather than threading a bare `String` through (see
/// `workers::reembed::ReembedError`'s own doc for the same rule, there
/// citing a CodeRabbit review). [`run`] still logs via `tracing::warn!(error
/// = %e, ...)`, which uses this type's `Display` impl, so the text a caller
/// sees is unchanged — only the type carrying it changed, from `String` to
/// this enum (which still carries the real `sqlx::Error` source via
/// `#[from]`, rather than discarding it into a formatted string at the
/// point of return).
#[derive(Debug, thiserror::Error)]
pub enum OAuthStateJtiSweepError {
    #[error("oauth state jti purge timed out after {0:?}")]
    Timeout(Duration),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

// ── Worker entry point ────────────────────────────────────────────────────────

/// Run the `oauth_state_jti` purge worker loop until `shutdown` is
/// signalled.
///
/// Spawned directly from `lib.rs` with the control pool — no tenant
/// dimension to thread through (see the module doc). Fires once immediately
/// on start (matching every other sweep worker's boot-time-fire behavior),
/// then every [`PURGE_INTERVAL`] thereafter.
pub async fn run(pool: PgPool, shutdown: Shutdown) {
    tracing::info!("oauth-state-jti-purge worker started");

    loop {
        if shutdown.is_triggered() {
            tracing::info!("oauth-state-jti-purge worker: shutdown signalled, stopping");
            return;
        }

        match tick(&pool).await {
            Ok(0) => tracing::debug!("oauth state jti purge: nothing expired"),
            Ok(n) => tracing::info!(purged = n, "oauth state jti purge complete"),
            Err(e) => {
                tracing::warn!(error = %e, "oauth state jti purge failed; will retry next cycle")
            }
        }

        // Inter-sweep sleep, racing shutdown so a SIGTERM during the wait is
        // noticed within SHUTDOWN_POLL_INTERVAL instead of riding out the
        // full interval — same shape as `workers::access_log_purge::run`'s.
        tokio::select! {
            _ = tokio::time::sleep(PURGE_INTERVAL) => {}
            _ = poll_until_triggered(&shutdown) => {
                tracing::info!("oauth-state-jti-purge worker: shutdown signalled during inter-sweep sleep, stopping");
                return;
            }
        }
    }
}

/// Resolve once `shutdown` is triggered, polling every
/// [`SHUTDOWN_POLL_INTERVAL`]. Never returns otherwise — used only as the
/// "shutdown" arm of [`run`]'s `tokio::select!`. Same pattern as
/// `workers::access_log_purge::poll_until_triggered`.
async fn poll_until_triggered(shutdown: &Shutdown) {
    loop {
        if shutdown.is_triggered() {
            return;
        }
        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }
}

/// One purge sweep: delete every `oauth_state_jti` row older than
/// [`JTI_RETENTION_SECS`]. `pub` so tests can drive one sweep against a real
/// Postgres without spawning [`run`]'s infinite loop.
///
/// Bounded by [`PURGE_TIMEOUT`]: a hung `DELETE` degrades to a logged
/// failure and the next cycle retries, rather than blocking this worker's
/// loop indefinitely — same reasoning as `workers::access_log_purge::tick`.
pub async fn tick(pool: &PgPool) -> Result<u64, OAuthStateJtiSweepError> {
    let cutoff = Utc::now() - chrono::Duration::seconds(JTI_RETENTION_SECS);

    match tokio::time::timeout(PURGE_TIMEOUT, delete_expired(pool, cutoff)).await {
        Err(_) => Err(OAuthStateJtiSweepError::Timeout(PURGE_TIMEOUT)),
        Ok(Err(e)) => Err(e.into()),
        Ok(Ok(n)) => Ok(n),
    }
}

async fn delete_expired(pool: &PgPool, cutoff: DateTime<Utc>) -> Result<u64, sqlx::Error> {
    let result = sqlx::query("DELETE FROM oauth_state_jti WHERE consumed_at < $1")
        .bind(cutoff)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

// ── Tests ─────────────────────────────────────────────────────────────────────
//
// The pg-gated test is on `RELINFER_TEST_DATABASE_URL`, same convention as
// `workers::access_log_purge`'s sibling workers — a plain `cargo test` with
// no DB configured skips it (prints and returns rather than failing).
//
// Standing rule: NEVER round-trip `Utc::now()` through Postgres and assert
// equality — every fixture row below ages itself via `now() - interval '1
// second' * $N`, computed entirely DB-side.
#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn retention_and_interval_constants_are_sane() {
        // Regression guard: retention must always outlive the token's own
        // validity window (the whole reason this sweep is safe at all), and
        // the sweep cadence must be comfortably tighter than the retention
        // window rather than merely shorter than it.
        assert!(
            JTI_RETENTION_SECS > crate::oauth::state::STATE_TTL_SECS,
            "retention must exceed the signed state token's own TTL"
        );
        assert!(
            (PURGE_INTERVAL.as_secs() as i64) * 3 < JTI_RETENTION_SECS,
            "sweep cadence should run comfortably more than once per retention window"
        );
        assert!(PURGE_TIMEOUT < PURGE_INTERVAL);
    }

    #[tokio::test]
    async fn tick_purges_expired_rows_and_keeps_fresh_ones() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run \
                     tick_purges_expired_rows_and_keeps_fresh_ones"
                );
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        let old_jti = Uuid::new_v4().to_string();
        let fresh_jti = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO oauth_state_jti (jti, consumed_at) \
             VALUES ($1, now() - (interval '1 second' * $2))",
        )
        .bind(&old_jti)
        .bind((JTI_RETENTION_SECS + 60) as f64)
        .execute(&pool)
        .await
        .expect("insert aged jti fixture");

        sqlx::query("INSERT INTO oauth_state_jti (jti) VALUES ($1)")
            .bind(&fresh_jti)
            .execute(&pool)
            .await
            .expect("insert fresh jti fixture");

        tick(&pool).await.expect("sweep tick");

        let old_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM oauth_state_jti WHERE jti = $1")
                .bind(&old_jti)
                .fetch_one(&pool)
                .await
                .expect("count old jti");
        let fresh_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM oauth_state_jti WHERE jti = $1")
                .bind(&fresh_jti)
                .fetch_one(&pool)
                .await
                .expect("count fresh jti");

        assert_eq!(old_count, 0, "an expired jti row must be purged");
        assert_eq!(
            fresh_count, 1,
            "a freshly-consumed jti row must survive the sweep"
        );

        sqlx::query("DELETE FROM oauth_state_jti WHERE jti = $1")
            .bind(&fresh_jti)
            .execute(&pool)
            .await
            .ok();
    }
}
