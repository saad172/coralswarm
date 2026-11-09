//! `slot_reservations` expiry sweep — CS-435 (org-onboarding plan §3 item
//! 15, D9): the sweep half of the reservation D9 requires. See
//! `billing::slot_pool`'s own module doc for the full D9 framing and
//! `billing::slot_pool::expire_reservations` for the guarded statement this
//! worker runs.
//!
//! # Shape: `tenant_harness` sweep worker, Consumer-only
//!
//! Registered with [`crate::workers::tenant_harness::HarnessConfig::
//! sweep_worker`], the SAME registration shape `workers::billing_derivation`
//! uses — but unlike that worker, [`tick`] does real work only on the
//! `TenantId::Consumer` pass and is a clean no-op for every `TenantId::Org`
//! entry the harness also calls it for. `slot_reservations` (migration
//! `0233`) is control-only, one physical copy, no tenant-tree twin (its own
//! migration header) — there is nothing for an Org tenant's own pass to
//! sweep, and running the identical control-plane sweep once per active org
//! tenant (as `billing_derivation` legitimately does for `llm_usage`, which
//! genuinely differs per plane) would just be the same statement executed
//! redundantly on every tick.
//!
//! # Why a per-account loop, not one global `UPDATE`
//!
//! `slot_reservations` carries the same `account_isolation` RLS shape every
//! other account-scoped control table in this crate does (`0233`'s own
//! header). `scripts/check_rls_coverage.sh`'s own header records that
//! `coralswarm_app` (the runtime role, `rolbypassrls = f`) genuinely
//! `unrecognized configuration parameter`-errors an unscoped query against
//! an `account_isolation`-shaped table — there is no "sweep every account in
//! one statement" shape available. So this worker enumerates every account
//! (`workers::billing_derivation::enumerate_consumer_accounts`, reused
//! verbatim — `accounts` itself carries no RLS, so that read needs no scope)
//! and opens `tenancy::scope::begin_account_scoped` once per account, same
//! per-account-loop shape `billing_derivation::tick`'s own Consumer branch
//! already uses for the identical reason (that module's own doc, "Per-account
//! loop on BOTH branches"). Cheap in the common case: an account with no live
//! reservations is one indexed `UPDATE` touching zero rows.
//!
//! # No advisory lock
//!
//! [`billing::slot_pool::expire_reservations`]'s own doc comment: the guard
//! clause (`WHERE released_at IS NULL AND expires_at <= now()`) IS the
//! atomicity boundary. A reservation `billing::slot_pool::reserve_slot`/
//! `release_reservation` is concurrently resolving loses the race to
//! whichever transaction commits first; the other's guarded `UPDATE` simply
//! affects 0 rows. No cross-table decision is made here, so there is nothing
//! for `billing::renewal::lock_period_rotation` to serialize against.

use std::time::Duration;

use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// Default sweep cadence — comfortably shorter than
/// [`crate::billing::slot_pool::DEFAULT_RESERVATION_TTL_SECS`] (5 min) so an
/// expired reservation does not linger for anywhere close to its own TTL
/// before being reclaimed, mirroring `workers::oauth_state_jti_sweep::
/// PURGE_INTERVAL`'s identical "ample relative to the retention window"
/// posture.
pub const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 60;

/// [`DEFAULT_SWEEP_INTERVAL_SECS`], overridable via
/// `SLOT_RESERVATION_SWEEP_SECS` — same env-override shape
/// `workers::billing_derivation::derivation_interval` uses.
pub fn sweep_interval() -> Duration {
    std::env::var("SLOT_RESERVATION_SWEEP_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_SWEEP_INTERVAL_SECS))
}

pub async fn tick(t: &WorkerTenant) -> Result<Tick, String> {
    if !matches!(t.tenant, TenantId::Consumer) {
        // Control-only table, one physical copy — see this module's own doc
        // comment. Every `TenantId::Org` pass is a clean no-op.
        return Ok(Tick::idle());
    }

    let account_ids = crate::workers::billing_derivation::enumerate_consumer_accounts(&t.pool).await?;
    let mut expired_total = 0usize;
    for account_id in account_ids {
        let mut tx = match crate::tenancy::scope::begin_account_scoped(&t.pool, account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(
                    %account_id, error = %e,
                    "workers::slot_reservations: failed to open a scoped transaction — skipping this account this tick"
                );
                continue;
            }
        };
        let expired = match crate::billing::slot_pool::expire_reservations(&mut tx).await {
            Ok(n) => n,
            Err(e) => {
                let _ = tx.rollback().await;
                tracing::warn!(
                    %account_id, error = %e,
                    "workers::slot_reservations: expiry sweep failed for this account — skipping"
                );
                continue;
            }
        };
        if expired == 0 {
            // A read-only commit is a no-op — cheaper than an explicit
            // rollback for the common (nothing-to-expire) case, and avoids a
            // branch per account for what is, in steady state, every
            // account.
            let _ = tx.commit().await;
            continue;
        }
        if let Err(e) = tx.commit().await {
            tracing::warn!(
                %account_id, error = %e,
                "workers::slot_reservations: commit failed after expiring reservations for this account"
            );
            continue;
        }
        expired_total += expired;
    }

    Ok(if expired_total > 0 { Tick::worked(expired_total) } else { Tick::idle() })
}
