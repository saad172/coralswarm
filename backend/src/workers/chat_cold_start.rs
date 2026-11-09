//! Hourly repeat of Chat's cold-start notice (org-onboarding plan item 25).
//!
//! The first post happens on `ADDED_TO_SPACE`. This sweep posts again, at
//! most once a day, until a directory-capable connect retires the space or
//! the thirty-day cap is reached. Control-plane only. Org ticks are a
//! no-op so the consumer pass is the one scan.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub fn interval() -> Duration {
    std::env::var("CHAT_COLD_START_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_INTERVAL)
}

pub async fn tick(
    t: &WorkerTenant,
    control: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    if !matches!(t.tenant, TenantId::Consumer) {
        return Ok(Tick::idle());
    }
    if !crate::connectors::chat_app_identity::chat_bot_configured() {
        return Ok(Tick::idle());
    }
    let accounts = crate::connectors::chat_cold_start::due_account_ids(control).await?;
    let mut posted = 0usize;
    for account_id in accounts {
        posted += crate::connectors::chat_cold_start::repeat_due(state, control, account_id).await;
    }
    if posted == 0 {
        Ok(Tick::idle())
    } else {
        Ok(Tick::worked(posted))
    }
}
