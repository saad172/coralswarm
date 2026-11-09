//! Moves self-serve trials along (signup PR C′): abandons a Checkout left
//! unpaid for 24 hours, claims the 80% / 100% / 7-days-left emails, and
//! converts a trial whose credits are used up or whose 90 days are over,
//! charging the seats and granting the paid allowance. The rules are
//! [`crate::billing::org_trial`]; this worker only runs
//! [`crate::billing::org_trial::lifecycle::process_account`] for each account
//! that still has a live trial.
//!
//! # Shape
//!
//! A `tenant_harness` sweep worker on the `Consumer` pass only (every table
//! it touches is control-plane, like `workers::billing_notices`). The live
//! accounts come from the 0382 definer `org_trial_accounts_due()`, so a pass
//! reads only the handful of orgs in a trial, never every account. Emails
//! are claimed here and delivered by `workers::billing_notices`.
//!
//! # Latency
//!
//! The sweep runs every 30 s by default (`ORG_TRIALS_SWEEP_SECS`). Between
//! the trial pool running dry and this worker converting, members' asks are
//! refused by the ordinary pool-exhausted gate; a shorter interval only
//! shortens that window. Stripe's own `trial_end` (day 90) and the
//! `invoice.paid` webhook convert without this worker if it is down.
//!
//! # Not drained at shutdown
//!
//! No transaction is held across a Stripe call, and every step is guarded
//! by the row's state and a fixed Stripe idempotency key, so a hard kill is
//! recovered by the next pass.

use std::time::Duration;

use uuid::Uuid;

use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

pub const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 30;

pub fn sweep_interval() -> Duration {
    std::env::var("ORG_TRIALS_SWEEP_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_SWEEP_INTERVAL_SECS))
}

/// The accounts with a live trial (`checkout_open`, `trialing`,
/// `converting`), through the 0382 definer.
pub(crate) async fn accounts_due(control: &sqlx::PgPool) -> Result<Vec<Uuid>, String> {
    sqlx::query_scalar("SELECT public.org_trial_accounts_due()")
        .fetch_all(control)
        .await
        .map_err(|e| format!("workers::org_trials: listing live trials failed: {e}"))
}

pub async fn tick(t: &WorkerTenant, state: &AppState) -> Result<Tick, String> {
    if !matches!(t.tenant, TenantId::Consumer) {
        return Ok(Tick::idle());
    }
    let accounts = accounts_due(&t.pool).await?;
    let now = chrono::Utc::now();
    let mut worked = 0usize;
    for account in accounts {
        match crate::billing::org_trial::lifecycle::process_account(&t.pool, state.stripe.as_deref(), account, now)
            .await
        {
            Ok(report) if report.worked() => {
                tracing::info!(%account, actions = ?report.actions, "workers::org_trials: trial advanced");
                worked += report.actions.len();
            }
            Ok(_) => {}
            // #1528 S3: a warning, escalated to an alert once the trial has
            // been stuck in `converting` for more than 24 hours.
            Err(e) => {
                crate::billing::org_trial::lifecycle::report_pass_failure(&t.pool, account, now, &e).await;
            }
        }
    }
    Ok(if worked > 0 { Tick::worked(worked) } else { Tick::idle() })
}
