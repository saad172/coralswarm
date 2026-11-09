//! Delivers billing notices (Settings launch plan PR 3.6): the "your
//! organization reached its monthly extra usage limit" family of emails. The
//! design (claim, detect, lease, deliver, never twice) is in
//! [`crate::billing::notices`]; this worker only runs it.
//!
//! # Shape
//!
//! A `tenant_harness` sweep worker that works on the `Consumer` pass only:
//! every table it touches (`billing_notices`, `extra_usage_*`, `accounts`,
//! `account_members`, `users`) is control-plane, so `t.pool` on that pass is
//! the control pool, exactly like `workers::slot_reservations`. Each tick
//! visits every account in its own account-scoped transaction (the tables
//! are row-level-secured by `app.account_id`, so there is no cross-account
//! read to be had). The per-account work is a handful of indexed statements
//! that insert nothing in the common case; sending happens outside any
//! transaction.
//!
//! # Not drained at shutdown
//!
//! Unlike the join-request digest, this worker holds no transaction open
//! across a Resend call. A hard kill mid-send leaves a lease that expires and
//! a retry that carries the same Resend `Idempotency-Key`, so there is
//! nothing for a drain to protect.
//!
//! # Mail not configured
//!
//! Without `RESEND_API_KEY` the worker still claims notices and leaves them
//! unsent; they go out once a key is set. It does not mark them failed.

use std::time::Duration;

use uuid::Uuid;

use crate::billing::notices::{self, email::Mailer};
use crate::clerk_admin::ClerkAdminClient;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

pub const DEFAULT_SWEEP_INTERVAL_SECS: u64 = 120;

pub fn sweep_interval() -> Duration {
    std::env::var("BILLING_NOTICES_SWEEP_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_SWEEP_INTERVAL_SECS))
}

/// The organization's display name, best effort: a Clerk lookup failure
/// degrades the copy to "your organization", it never blocks the email.
async fn org_name(clerk_org_id: &str) -> Option<String> {
    let clerk = ClerkAdminClient::from_env()?;
    let org = clerk.get_organization(clerk_org_id).await.ok()?;
    let name = org.name.trim();
    (!name.is_empty()).then(|| name.chars().take(80).collect())
}

pub async fn tick(t: &WorkerTenant, state: &AppState) -> Result<Tick, String> {
    if !matches!(t.tenant, TenantId::Consumer) {
        return Ok(Tick::idle());
    }
    let mailer = Mailer::from_state(state);
    let accounts = crate::workers::billing_derivation::enumerate_consumer_accounts(&t.pool).await?;
    let mut worked = 0usize;
    for account in accounts {
        match run_one(&t.pool, state, mailer.as_ref(), account).await {
            Ok(stats) => worked += stats.claimed as usize + stats.sent,
            Err(e) => tracing::warn!(
                %account, error = %e,
                "workers::billing_notices: pass failed for this account; continuing with the rest"
            ),
        }
    }
    Ok(if worked > 0 { Tick::worked(worked) } else { Tick::idle() })
}

async fn run_one(
    control: &sqlx::PgPool,
    state: &AppState,
    mailer: Option<&Mailer>,
    account: Uuid,
) -> Result<notices::PassStats, sqlx::Error> {
    notices::run_account(control, mailer, account, || async {
        // Only reached when something is due.
        let clerk_org_id: Option<String> =
            sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
                .bind(account)
                .fetch_optional(control)
                .await
                .ok()
                .flatten()
                .flatten();
        let web_base = state.web_base_url(control, clerk_org_id.as_deref()).await;
        let name = match clerk_org_id.as_deref() {
            Some(id) => org_name(id).await,
            None => None,
        };
        (web_base, name)
    })
    .await
}
