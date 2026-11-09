//! Carry `TenantActivity::note_enqueue` wakes from HTTP machines to the worker
//! machine (#1495).
//!
//! # Why this exists
//!
//! Request and webhook handlers that enqueue a `jobs` row for an ORG tenant
//! call `state.worker_activity.note_enqueue(org)` so the tenant harness pulls
//! that tenant forward instead of leaving the row to wait out an idle-backoff
//! slot of up to `DORMANT_POLL_PERIOD` (20 min). That signal is an in-process
//! `DashMap` bump. Once `BACKEND_ROLE=http` and `BACKEND_ROLE=workers` run in
//! different processes, the bump lands in a process with no harness and an
//! externally enqueued org job (a Slack message, a Graph webhook) goes back to
//! the 0-20 min wait the wake was added to remove.
//!
//! # How
//!
//! * HTTP role: [`spawn_publisher`] installs a channel sender on the local
//!   `TenantActivity`; a task drains it, de-duplicates each batch, and runs
//!   `SELECT pg_notify('coralswarm_worker_wake', <org>)` on the control pool.
//! * Workers role: [`spawn_listener`] holds one dedicated `LISTEN` connection
//!   to the control database and calls `note_enqueue(org)` locally for every
//!   notification, which is exactly what the in-process call used to do.
//!
//! The control database is the `coralswarm-db` Fly Postgres reached directly
//! on :5433 (no PgBouncer in front), so `LISTEN` works on `DATABASE_URL`.
//! `WORKER_WAKE_DATABASE_URL` overrides it if that ever changes (a
//! transaction-mode pooler accepts `LISTEN` and silently never delivers).
//!
//! # Failure mode
//!
//! Best effort by design. A lost notification (listener reconnecting, publisher
//! error) degrades to the pre-wake behaviour for that one job: it waits for the
//! tenant's next dormant poll. Nothing is lost from the queue itself; the
//! `jobs` row is durable and this only shortens how soon it is noticed.
//! Consumer-plane producers wake through
//! `TenantActivity::note_consumer_enqueue`, whose reserved key
//! (`tenant_harness::CONSUMER_WAKE`) rides this channel like any org id.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::shutdown::Shutdown;
use crate::workers::tenant_harness::TenantActivity;

/// Postgres `NOTIFY` channel shared by publisher and listener.
pub const CHANNEL: &str = "coralswarm_worker_wake";

/// Postgres rejects `NOTIFY` payloads of 8000 bytes or more. Org ids are
/// short slugs; anything near the limit is not an org id.
const MAX_PAYLOAD_BYTES: usize = 512;

/// How often the publisher and listener re-check the shutdown flag while idle.
const SHUTDOWN_POLL: Duration = Duration::from_millis(500);

/// Delay before re-opening a dropped `LISTEN` connection.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

/// Whether a wake key is carried over `NOTIFY`, checked on both ends.
fn relayable(payload: &str) -> bool {
    !payload.is_empty() && payload.len() <= MAX_PAYLOAD_BYTES
}

/// HTTP role: forward every local `note_enqueue` to the worker process.
pub fn spawn_publisher(
    pool: PgPool,
    activity: &Arc<TenantActivity>,
    shutdown: Shutdown,
) -> Option<tokio::task::JoinHandle<()>> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    if !activity.forward_enqueues_to(tx) {
        tracing::warn!("worker wake publisher already installed; not spawning a second");
        return None;
    }
    let handle = tokio::spawn(async move {
        loop {
            let first = match tokio::time::timeout(SHUTDOWN_POLL, rx.recv()).await {
                Ok(Some(org)) => org,
                Ok(None) => return,
                Err(_) => {
                    if shutdown.is_triggered() {
                        return;
                    }
                    continue;
                }
            };
            let mut batch = BTreeSet::from([first]);
            while let Ok(org) = rx.try_recv() {
                batch.insert(org);
            }
            for org in batch {
                if !relayable(&org) {
                    continue;
                }
                if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(CHANNEL)
                    .bind(&org)
                    .execute(&pool)
                    .await
                {
                    tracing::warn!(
                        error = %e,
                        org = %org,
                        "worker wake NOTIFY failed; the job waits for the tenant's next dormant poll"
                    );
                }
            }
        }
    });
    tracing::info!(channel = CHANNEL, "worker wake publisher spawned (BACKEND_ROLE=http)");
    Some(handle)
}

/// The DSN the listener uses: `WORKER_WAKE_DATABASE_URL`, else `DATABASE_URL`.
pub fn listener_url_from_env() -> Option<String> {
    std::env::var("WORKER_WAKE_DATABASE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| std::env::var("DATABASE_URL").ok().filter(|v| !v.trim().is_empty()))
}

/// Workers role: turn each `NOTIFY` into a local `note_enqueue`.
pub fn spawn_listener(
    url: String,
    activity: Arc<TenantActivity>,
    shutdown: Shutdown,
) -> tokio::task::JoinHandle<()> {
    let handle = tokio::spawn(async move {
        while !shutdown.is_triggered() {
            let mut listener = match sqlx::postgres::PgListener::connect(&url).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(error = %e, "worker wake listener could not connect; retrying");
                    sleep_unless_shutdown(RECONNECT_BACKOFF, &shutdown).await;
                    continue;
                }
            };
            if let Err(e) = listener.listen(CHANNEL).await {
                tracing::error!(error = %e, "worker wake LISTEN failed; retrying");
                sleep_unless_shutdown(RECONNECT_BACKOFF, &shutdown).await;
                continue;
            }
            tracing::info!(channel = CHANNEL, "worker wake listener connected");
            loop {
                if shutdown.is_triggered() {
                    return;
                }
                // `recv` is cancel-safe (sqlx 0.8); the timeout only exists to
                // re-check the shutdown flag. `try_recv` returns `Ok(None)`
                // when sqlx reconnected underneath us, i.e. wakes in that gap
                // were missed; that is logged and otherwise tolerated.
                match tokio::time::timeout(SHUTDOWN_POLL, listener.try_recv()).await {
                    Err(_) => continue,
                    Ok(Ok(Some(n))) => {
                        let org = n.payload();
                        if relayable(org) {
                            activity.note_enqueue(org);
                        }
                    }
                    Ok(Ok(None)) => {
                        tracing::warn!(
                            "worker wake listener reconnected; wakes sent during the gap wait for the dormant poll"
                        );
                    }
                    Ok(Err(e)) => {
                        tracing::error!(error = %e, "worker wake listener dropped; reconnecting");
                        break;
                    }
                }
            }
            sleep_unless_shutdown(RECONNECT_BACKOFF, &shutdown).await;
        }
    });
    tracing::info!(channel = CHANNEL, "worker wake listener spawned (BACKEND_ROLE=workers)");
    handle
}

async fn sleep_unless_shutdown(total: Duration, shutdown: &Shutdown) {
    let mut left = total;
    while !left.is_zero() && !shutdown.is_triggered() {
        let step = left.min(SHUTDOWN_POLL);
        tokio::time::sleep(step).await;
        left -= step;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_is_a_plain_identifier() {
        // `LISTEN` takes an identifier; keep it lower-case ASCII so no quoting
        // differences between publisher (bound text) and listener can arise.
        assert!(CHANNEL.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'));
    }

    #[test]
    fn payload_limit_is_well_under_postgres_cap() {
        assert!(MAX_PAYLOAD_BYTES < 8000);
    }

    /// A consumer-plane wake from an HTTP process must reach the worker
    /// process through the same filter an org id passes.
    #[test]
    fn reserved_consumer_key_is_relayed() {
        use crate::workers::tenant_harness::CONSUMER_WAKE;
        assert!(relayable(CONSUMER_WAKE));
        assert!(relayable("org_2abc"));
        assert!(!relayable(""));
        assert!(!relayable(&"x".repeat(MAX_PAYLOAD_BYTES + 1)));
    }
}
