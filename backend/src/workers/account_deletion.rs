//! Worker for `services::account_deletion`: claims one due `account_deletions`
//! row at a time (lease + `FOR UPDATE SKIP LOCKED`, migration 0370) and runs it
//! until it completes, waits, yields or fails.
//!
//! Control-plane only and not tenant-looped: the table has one physical copy,
//! and the one tenant-plane concern (member workspaces in org databases) is
//! handled inside the job through the tenant runtime. Same standalone-loop
//! shape as `workers::access_log_purge`.
//!
//! A crashed worker's lease expires (`services::account_deletion::LEASE_SECS`)
//! and any worker re-claims the row; the steps are idempotent and checkpointed,
//! so the re-claim resumes rather than repeats. A failing step is retried with
//! exponential backoff (30s, doubling, capped at one hour) and logged at ERROR
//! each time, so a stuck deletion is visible in the same place every other
//! stuck worker is.

use std::sync::Arc;
use std::time::Duration;

use crate::services::account_deletion::{self, Ctx};
use crate::services::account_deletion_live::{LiveConnectors, LiveMailer, LiveTenantPlane};
use crate::shutdown::Shutdown;
use crate::AppState;

/// Poll interval when nothing is due.
pub const IDLE_INTERVAL: Duration = Duration::from_secs(5);
const ALERT_SWEEP_INTERVAL: Duration = Duration::from_secs(600);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub async fn run(state: Arc<AppState>, shutdown: Shutdown) {
    tracing::info!("account-deletion worker started");
    let connectors = LiveConnectors { http: state.http_client.clone() };
    let tenant_plane = LiveTenantPlane { state: state.clone() };
    let mailer = LiveMailer { http: state.http_client.clone(), resend_api_key: state.resend_api_key.clone() };
    let cancel_key = account_deletion::cancel_key_from_env();
    let alert_hours: i32 = std::env::var("ACCOUNT_DELETION_ALERT_HOURS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|h| *h > 0)
        .unwrap_or(6);
    let mut last_alert_sweep: Option<std::time::Instant> = None;
    loop {
        if shutdown.is_triggered() {
            tracing::info!("account-deletion worker: shutdown signalled, stopping");
            return;
        }
        let web_base = state.personal_web_base_url();
        let worked = match state.control_db() {
            Some(control) => {
                let clerk = crate::clerk_admin::ClerkAdminClient::from_env();
                let ctx = Ctx {
                    control,
                    stripe: state.stripe.as_deref(),
                    clerk: clerk.as_ref(),
                    blobs: state.r2.as_deref().map(|r2| r2 as &dyn crate::services::blob_store::PromotionBlobStore),
                    connectors: &connectors,
                    tenant_plane: &tenant_plane,
                    dek_cache: Some(&state.dek_cache),
                    mailer: &mailer,
                    cancel_key: cancel_key.as_ref().map(|k| k.as_slice()),
                    web_base: &web_base,
                };
                // Stuck-deletion alert sweep, at most every ten minutes.
                if last_alert_sweep.map_or(true, |t| t.elapsed() > ALERT_SWEEP_INTERVAL) {
                    last_alert_sweep = Some(std::time::Instant::now());
                    if let Err(e) = account_deletion::alert_stuck(control, alert_hours).await {
                        tracing::error!(error = %e, "account-deletion worker: stuck sweep failed");
                    }
                }
                match account_deletion::tick(&ctx).await {
                    Ok(worked) => worked,
                    Err(e) => {
                        tracing::error!(error = %e, "account-deletion worker: claim failed");
                        false
                    }
                }
            }
            None => false,
        };
        if worked {
            continue;
        }
        tokio::select! {
            _ = tokio::time::sleep(IDLE_INTERVAL) => {}
            _ = poll_until_triggered(&shutdown) => {
                tracing::info!("account-deletion worker: shutdown signalled during idle sleep, stopping");
                return;
            }
        }
    }
}

async fn poll_until_triggered(shutdown: &Shutdown) {
    loop {
        if shutdown.is_triggered() {
            return;
        }
        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }
}
