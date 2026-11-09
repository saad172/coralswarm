//! Self-heal for a pooled server connection stuck in
//! `default_transaction_read_only = on` (COR-82).
//!
//! On 2026-10-14 an ad-hoc script sent a bare session `SET
//! default_transaction_read_only = on` through an org tenant's Neon pooler
//! DSN. PgBouncer runs in transaction mode, so the setting stayed on that one
//! server connection, and PgBouncer hands its most recently released server
//! connection to the next client transaction. The app drew that connection
//! for nearly every short statement and every write on the org failed with
//! SQLSTATE 25006 for 27 minutes, until PgBouncer's `server_lifetime`
//! retired it.
//!
//! Nothing in this codebase sets a transaction read-only, so a 25006 on any
//! pool is a poisoned connection, never a legitimate rejection. The cure has
//! to happen at the server. Dropping the sqlx connection only returns the
//! PgBouncer client slot, and a `SET` from the app would land on whichever
//! server PgBouncer picks next. A backend that terminates itself is the one
//! statement that is guaranteed to hit the poisoned server, because it runs
//! on the server that answered the probe.
//!
//! [`heal`] probes several backends at once. Each probe holds its own
//! transaction while it asks, which forces PgBouncer to link a distinct server
//! connection to each. The poisoned server has just answered the rejected
//! write, so PgBouncer usually hands it to the first probe. A miss is not
//! fatal. The next 25006 re-arms the heal, so convergence takes a few worker
//! ticks at worst rather than an hour.
//!
//! The worker harness is the one place every worker's database error passes
//! through, which is why detection lives there and not at each query site.
//! Its error type is `String`, so the SQLSTATE is gone by then and
//! [`is_read_only_rejection`] matches Postgres's 25006 message text instead.

use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;
use sqlx::PgPool;

use super::TenantId;

/// Backends one heal pass probes concurrently, capped by the pool's
/// `max_connections`. PgBouncer hands out the most recently released server
/// first, so the poisoned one is almost always among the first eight. A miss
/// re-arms on the next 25006.
pub const PROBE_FANOUT: u32 = 8;

/// Wall-clock bound on one heal pass. Probes acquire with the pool's own
/// `acquire_timeout` (5s on tenant pools), so this only caps a saturated pool.
const HEAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Postgres's message for SQLSTATE 25006 is `cannot execute %s in a read-only
/// transaction`. The harness sees it after sqlx's `error returned from
/// database: ` prefix and the worker's own context, so a substring match is
/// the only form that survives.
pub fn is_read_only_rejection(err: &str) -> bool {
    err.contains("read-only transaction")
}

/// What one tenant's pool has seen since the process started.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ReadOnlyRecord {
    pub rejections: u64,
    pub discarded_backends: u64,
    pub last_rejection_at: DateTime<Utc>,
    pub last_discard_at: Option<DateTime<Utc>>,
}

/// Per-tenant 25006 counters, read by `/internal/admin/chain-health`.
/// Process memory only. A deploy resets it, which is fine for an alarm whose
/// job is to make a recurrence visible while it is happening.
#[derive(Default)]
pub struct ReadOnlyLedger {
    by_tenant: DashMap<String, ReadOnlyRecord>,
}

fn tenant_key(tenant: &TenantId) -> String {
    match tenant {
        TenantId::Consumer => "consumer".to_string(),
        TenantId::Org(org) => org.clone(),
    }
}

impl ReadOnlyLedger {
    pub fn note_rejection(&self, tenant: &TenantId) {
        let now = Utc::now();
        self.by_tenant
            .entry(tenant_key(tenant))
            .and_modify(|r| {
                r.rejections += 1;
                r.last_rejection_at = now;
            })
            .or_insert(ReadOnlyRecord {
                rejections: 1,
                discarded_backends: 0,
                last_rejection_at: now,
                last_discard_at: None,
            });
    }

    pub fn note_discarded(&self, tenant: &TenantId, backends: u64) {
        if backends == 0 {
            return;
        }
        let now = Utc::now();
        if let Some(mut r) = self.by_tenant.get_mut(&tenant_key(tenant)) {
            r.discarded_backends += backends;
            r.last_discard_at = Some(now);
        }
    }

    /// Every tenant with at least one rejection, sorted by tenant key.
    pub fn snapshot(&self) -> Vec<(String, ReadOnlyRecord)> {
        let mut rows: Vec<(String, ReadOnlyRecord)> = self
            .by_tenant
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }
}

/// Outcome of one heal pass.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct HealReport {
    /// Probes that reached a backend and read its setting.
    pub probed: u32,
    /// Backend pids that reported `on` and were terminated.
    pub discarded: Vec<i32>,
}

/// Hold a transaction, read the backend's setting, and terminate the backend
/// when it is read-only. Returns the pid when a backend was discarded.
async fn probe_one(pool: &PgPool) -> Result<Option<i32>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let (pid, read_only): (i32, bool) = sqlx::query_as(
        "SELECT pg_backend_pid(), current_setting('default_transaction_read_only')::bool",
    )
    .fetch_one(&mut *tx)
    .await?;
    if !read_only {
        tx.rollback().await?;
        return Ok(None);
    }
    // The backend dies before it can answer, so this statement errors with
    // 57P01 on success. sqlx pings the connection on release and drops it.
    let _ = sqlx::query("SELECT pg_terminate_backend(pg_backend_pid())")
        .execute(&mut *tx)
        .await;
    Ok(Some(pid))
}

/// Probe up to [`PROBE_FANOUT`] backends of `pool` concurrently and terminate
/// every one that is read-only.
pub async fn discard_read_only_backends(pool: &PgPool) -> HealReport {
    let fanout = PROBE_FANOUT.min(pool.options().get_max_connections()).max(1);
    let probes = futures::future::join_all((0..fanout).map(|_| probe_one(pool)));
    let mut report = HealReport::default();
    let Ok(results) = tokio::time::timeout(HEAL_TIMEOUT, probes).await else {
        return report;
    };
    for result in results {
        match result {
            Ok(Some(pid)) => {
                report.probed += 1;
                report.discarded.push(pid);
            }
            Ok(None) => report.probed += 1,
            Err(_) => {}
        }
    }
    report
}

/// Record a 25006 for `tenant`, discard the poisoned backends behind `pool`,
/// and log the outcome as an alert.
pub async fn heal(tenant: &TenantId, pool: &PgPool, ledger: &ReadOnlyLedger) -> HealReport {
    ledger.note_rejection(tenant);
    let report = discard_read_only_backends(pool).await;
    ledger.note_discarded(tenant, report.discarded.len() as u64);
    tracing::error!(
        tenant = ?tenant,
        probed = report.probed,
        discarded = ?report.discarded,
        alert = true,
        "read-only poisoned backend: a pooled connection rejected a write with SQLSTATE 25006 \
         (someone ran a session-level SET through the pooler); probed the pool and terminated \
         every backend that reported read-only"
    );
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_harness_shape_of_a_25006() {
        assert!(is_read_only_rejection(
            "dequeue failed: error returned from database: cannot execute UPDATE in a read-only transaction"
        ));
        assert!(is_read_only_rejection(
            "write failed: error returned from database: cannot execute INSERT in a read-only transaction"
        ));
        assert!(!is_read_only_rejection(
            "dequeue failed: error returned from database: relation \"jobs\" does not exist"
        ));
        assert!(!is_read_only_rejection("pool timed out while waiting for an open connection"));
    }

    #[test]
    fn ledger_counts_per_tenant_and_only_credits_discards_after_a_rejection() {
        let ledger = ReadOnlyLedger::default();
        let org = TenantId::Org("org_a".into());
        ledger.note_discarded(&org, 3);
        assert!(ledger.snapshot().is_empty(), "a discard with no rejection is not recorded");

        ledger.note_rejection(&org);
        ledger.note_rejection(&org);
        ledger.note_discarded(&org, 1);
        ledger.note_rejection(&TenantId::Consumer);

        let rows = ledger.snapshot();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "consumer");
        assert_eq!(rows[0].1.rejections, 1);
        assert_eq!(rows[0].1.discarded_backends, 0);
        assert!(rows[0].1.last_discard_at.is_none());
        assert_eq!(rows[1].0, "org_a");
        assert_eq!(rows[1].1.rejections, 2);
        assert_eq!(rows[1].1.discarded_backends, 1);
        assert!(rows[1].1.last_discard_at.is_some());
    }
}
