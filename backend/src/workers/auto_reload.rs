//! Extra-usage auto-reload worker (settings launch plan PR 3.5). The design,
//! and every double-charge and over-the-limit rule, is in
//! [`crate::billing::extra_usage::reload`]; this worker only runs a pass on a
//! timer.
//!
//! # Shape
//!
//! A `tenant_harness` sweep worker on the `Consumer` pass only. Every table it
//! touches is control-plane, so `t.pool` on that pass is the control pool (same
//! as `workers::billing_notices`). One pass reconciles stale in-flight reloads
//! (the reaper) and then reloads accounts that are due. Which accounts are due
//! comes from `extra_usage_reload_candidates()` (migration 0374), so a pass
//! does not open a transaction per account in the system.
//!
//! # Kill switch
//!
//! `AUTO_RELOAD_DISABLED=1` stops NEW charges: no candidate is reloaded and no
//! PaymentIntent request is sent, including re-sends of rows already claimed.
//! Reconciling what Stripe already did still runs, and the webhook keeps
//! crediting money that was charged. Without `STRIPE_SECRET_KEY` the worker is
//! idle (there is nothing it could charge).

use std::time::Duration;

use crate::billing::extra_usage::reload;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

pub const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 60;

pub fn sweep_interval() -> Duration {
    std::env::var("AUTO_RELOAD_SWEEP_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_SWEEP_INTERVAL_SECS))
}

/// Whether new charges are allowed. Anything but an explicit truthy value
/// leaves auto-reload running.
pub fn new_charges_enabled() -> bool {
    !matches!(
        std::env::var("AUTO_RELOAD_DISABLED").ok().as_deref().map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

pub async fn tick(t: &WorkerTenant, state: &AppState) -> Result<Tick, String> {
    if !matches!(t.tenant, TenantId::Consumer) {
        return Ok(Tick::idle());
    }
    let Some(stripe) = state.stripe.as_deref() else {
        return Ok(Tick::idle());
    };
    let stats = reload::run_pass(&t.pool, stripe, new_charges_enabled())
        .await
        .map_err(|e| format!("auto-reload pass failed: {e}"))?;
    let worked = stats.reloaded + stats.reconciled;
    if stats.errors > 0 {
        tracing::warn!(errors = stats.errors, "workers::auto_reload: some accounts failed this pass");
    }
    Ok(if worked > 0 { Tick::worked(worked) } else { Tick::idle() })
}
