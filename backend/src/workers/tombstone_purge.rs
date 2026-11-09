//! Tombstone purge worker (ADR-0005).
//!
//! Soft-deletes append a delete op to `op_log`; those tombstones are the
//! 30-day trash. Left unbounded they accumulate forever:
//! `Store::purge_expired_tombstones` had **no scheduler**. This worker runs it
//! once a day, deleting delete-ops older than
//! [`TOMBSTONE_RETENTION_DAYS`].
//!
//! ## Consumer-plane RLS scoping (org-tenancy Phase W.2)
//!
//! `op_log` is on migration `0083_consumer_rls`'s policy roster (direct
//! `account_id` policy), and this purge was ONE deliberate cross-account
//! sweep — post-OPS-4 (app role switched to the non-`BYPASSRLS`
//! `coralswarm_app`) an unscoped `DELETE FROM op_log …` hard-errors on the
//! strict `current_setting('app.account_id')` predicate. The consumer-plane
//! tick therefore now runs **loop-per-account** (same flagged-decision shape
//! as Phase B.4's admin backfill): enumerate account ids from `accounts` (a
//! control/identity table deliberately OUTSIDE the RLS roster — the read
//! needs no scope), then run one `begin_account_scoped` transaction per
//! account calling `Store::purge_expired_tombstones_scoped`. Pre-OPS-4 this
//! is behavior-identical in aggregate (the owner role's BYPASSRLS means the
//! FIRST account's scoped delete already sweeps every expired tombstone and
//! the rest no-op); post-OPS-4 each account's iteration purges exactly its
//! own rows.
//!
//! ## Tenant-plane RLS scoping (Phase 2 slice 4)
//!
//! The org-tenant tick used to say "tenant DBs get whole-database isolation,
//! not RLS", and call `purge_expired_tombstones` unscoped. That is true of the
//! CROSS-ORG boundary and false of the boundary INSIDE a tenant DB.
//! `migrations_tenant/0009_tenant_rls` §3 puts a member-ocean policy on
//! `op_log`, resolved through `tenant_rls_visible_graph_ids()`, which returns
//! the EMPTY SET unless the transaction has set `app.tenant_actor` or
//! `app.user_id`. Both are `set_config(..,true)` and so are transaction-local:
//! a statement on a pooled `&PgPool` cannot carry either. At OPS-4-T the
//! unscoped call would have deleted nothing and reported "nothing expired".
//! The org tick now opens `begin_tenant_system_scoped_without_account` and
//! calls `purge_expired_tombstones_scoped`, the same twin the consumer tick
//! uses. A daily sweep has no acting user, so the system principal is the
//! honest choice.

use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use crate::store::{StoreError, TOMBSTONE_RETENTION_DAYS};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// How often the purge runs. Daily is ample for a 30-day retention window and
/// keeps the delete workload well away from peak sync traffic. `pub(crate)`:
/// fed to the tenant harness as this worker's sweep cadence.
pub(crate) const PURGE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Per-sweep timeout on the DB call. A purge that hasn't finished in a minute is
/// stuck (pool exhaustion, network partition); bound it so a hung query degrades
/// to a logged failure and the next daily tick still runs, rather than parking
/// the harness's schedule forever and silently stopping the purge until a restart.
const PURGE_TIMEOUT: Duration = Duration::from_secs(60);

/// One tombstone-purge sweep on one tenant's store (on the consumer plane
/// the pool also carries the account enumeration + per-account scoped
/// transactions — see the module doc's RLS section). Driven by
/// `workers::tenant_harness::run_multi_tenant` (org-tenancy PR 2.1) as a
/// sweep worker with cadence [`PURGE_INTERVAL`]; the harness polls a
/// newly-seen tenant immediately, preserving the old `tokio::time::interval`
/// fire-once-at-boot behavior. `op_log` is a tenant-tree table (tenant
/// Stores emit sync ops on writes), so this joins the tenant loop — a tenant
/// DB's tombstones would otherwise accumulate forever.
pub async fn tick(t: &WorkerTenant) -> Result<Tick, String> {
    let cutoff = Utc::now() - chrono::Duration::days(TOMBSTONE_RETENTION_DAYS);
    match &t.tenant {
        // Org tenant DB. Whole-database isolation covers the CROSS-ORG
        // boundary only. `op_log` also carries a member-ocean policy
        // (migrations_tenant/0009_tenant_rls §3) resolving through
        // `tenant_rls_visible_graph_ids()`, which returns the EMPTY SET
        // unless the transaction has set `app.tenant_actor` or `app.user_id`.
        // Both are `set_config(..,true)`, so the previous unscoped
        // `t.store.purge_expired_tombstones(cutoff)` against the pooled
        // `&PgPool` could not carry either. A purge sweep has no acting user,
        // so this takes the system principal.
        TenantId::Org(_) => {
            let purge_one = async {
                let mut tx = begin_tenant_system_scoped_without_account(&t.pool).await?;
                let n = t.store.purge_expired_tombstones_scoped(&mut tx, cutoff).await?;
                tx.commit().await?;
                Ok::<u64, StoreError>(n)
            };
            match tokio::time::timeout(PURGE_TIMEOUT, purge_one).await {
                // The outbound DB call hung past PURGE_TIMEOUT: log and fall
                // through to the next daily tick rather than blocking the
                // schedule.
                Err(_) => {
                    tracing::warn!(
                        timeout_secs = PURGE_TIMEOUT.as_secs(),
                        "tombstone purge timed out; will retry next cycle"
                    );
                    Ok(Tick::idle())
                }
                Ok(Ok(0)) => {
                    tracing::debug!(%cutoff, "tombstone purge: nothing expired");
                    Ok(Tick::idle())
                }
                Ok(Ok(n)) => {
                    tracing::info!(purged = n, %cutoff, "tombstone purge complete");
                    Ok(Tick::worked(n as usize))
                }
                Ok(Err(e)) => {
                    // Non-fatal: an unbounded op_log degrades slowly, so a
                    // transient DB error just waits for the next cycle rather
                    // than crashing the process (mirrors the other cloud
                    // workers' error handling).
                    tracing::warn!(error = %e, "tombstone purge failed; will retry next cycle");
                    Ok(Tick::idle())
                }
            }
        }
        // Consumer plane: loop-per-account under the RLS contract — see the
        // module doc's "Consumer-plane RLS scoping" section.
        TenantId::Consumer => consumer_tick(t, cutoff).await,
    }
}

/// The consumer-plane sweep: one scoped transaction per account. A single
/// account's failure (or timeout) is logged and skipped — the other accounts
/// still get purged this cycle, and an unbounded `op_log` degrades slowly
/// enough that "retry that account next cycle" matches the pre-conversion
/// error posture (which retried the WHOLE sweep next cycle).
async fn consumer_tick(t: &WorkerTenant, cutoff: chrono::DateTime<Utc>) -> Result<Tick, String> {
    // Account roster — control-table read, needs no scope (see module doc).
    // ORDER BY makes per-cycle progress deterministic/observable.
    let account_ids: Vec<Uuid> = match tokio::time::timeout(
        PURGE_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts ORDER BY id").fetch_all(&t.pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                timeout_secs = PURGE_TIMEOUT.as_secs(),
                "tombstone purge: account enumeration timed out; will retry next cycle"
            );
            return Ok(Tick::idle());
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "tombstone purge: account enumeration failed; will retry next cycle");
            return Ok(Tick::idle());
        }
        Ok(Ok(ids)) => ids,
    };

    let mut total_purged = 0u64;
    let mut failed_accounts = 0usize;
    for account_id in account_ids {
        let purge_one = async {
            let mut tx = begin_account_scoped(&t.pool, account_id)
                .await
                .map_err(|e| e.to_string())?;
            let n = t
                .store
                .purge_expired_tombstones_scoped(&mut tx, cutoff)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok::<u64, String>(n)
        };
        match tokio::time::timeout(PURGE_TIMEOUT, purge_one).await {
            Err(_) => {
                failed_accounts += 1;
                tracing::warn!(
                    %account_id,
                    timeout_secs = PURGE_TIMEOUT.as_secs(),
                    "tombstone purge timed out for account; will retry next cycle"
                );
            }
            Ok(Err(e)) => {
                failed_accounts += 1;
                tracing::warn!(%account_id, error = %e, "tombstone purge failed for account; will retry next cycle");
            }
            Ok(Ok(n)) => total_purged += n,
        }
    }

    if total_purged > 0 {
        tracing::info!(purged = total_purged, failed_accounts, %cutoff, "tombstone purge complete");
        Ok(Tick::worked(total_purged as usize))
    } else {
        tracing::debug!(failed_accounts, %cutoff, "tombstone purge: nothing expired");
        Ok(Tick::idle())
    }
}
