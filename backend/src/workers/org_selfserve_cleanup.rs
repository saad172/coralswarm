//! Runs [`crate::tenancy::org_selfserve_cleanup`]'s sweep (self-serve signup
//! PR A2): deletes abandoned sign-ups and the Clerk orgs of unpaid orgs whose
//! provisioning failed, requeueing instead when a late payment made one paid.
//!
//! A `tenant_harness` sweep worker on the `Consumer` pass only: every table
//! it touches is control-plane. Spawned only in the workers process group
//! (`startup::connectors`, behind `backend_role.runs_workers()`).
//!
//! Every 5 minutes by default (`ORG_SELFSERVE_CLEANUP_SWEEP_SECS`); nothing
//! here is latency-sensitive (an abandoned org waits 24 h + 1 h anyway).
//! Without `CLERK_SECRET_KEY` the pass logs a warning and does nothing: it
//! never deletes an account whose Clerk org it could not delete first.

use std::time::Duration;

use crate::tenancy::org_selfserve_cleanup::{CleanupPolicy, Sweeper};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

pub const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 300;

pub fn sweep_interval() -> Duration {
    std::env::var("ORG_SELFSERVE_CLEANUP_SWEEP_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_SWEEP_INTERVAL_SECS))
}

pub async fn tick(t: &WorkerTenant, state: &AppState) -> Result<Tick, String> {
    if !matches!(t.tenant, TenantId::Consumer) {
        return Ok(Tick::idle());
    }
    let Some(clerk) = crate::clerk_admin::ClerkAdminClient::from_env() else {
        tracing::warn!("workers::org_selfserve_cleanup: CLERK_SECRET_KEY unset; nothing is cleaned up");
        return Ok(Tick::idle());
    };
    let sweeper = Sweeper {
        control: &t.pool,
        clerk: &clerk,
        stripe: state.stripe.as_deref(),
        policy: CleanupPolicy::default(),
    };
    let report = sweeper.sweep_once(chrono::Utc::now()).await;
    let worked = report.worked();
    if worked > 0 || !report.released.is_empty() {
        tracing::info!(?report, "workers::org_selfserve_cleanup: pass done");
    }
    Ok(if worked > 0 { Tick::worked(worked) } else { Tick::idle() })
}
