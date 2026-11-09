//! Worker for `services::org_deletion`: claims one due `org_deletions` row at
//! a time (lease + `FOR UPDATE SKIP LOCKED`, control 0387) and runs it until
//! it is done, waits, yields or fails; every ten minutes it also runs the
//! system-initiated entries (30 days unpaid, Clerk-dashboard deletes), the
//! 30-day export retention and the stuck-job alert.
//!
//! Idles (one `info!` at boot, no database access) while
//! `ORG_DELETION_ENABLED` is unset; the flag is re-read every tick.
//! Workers process group only. Control-plane only: the tenant database is
//! reached through the tenant runtime's registry for the export, and through
//! the A1 provisioner (with its own `coralswarm_provisioner` login,
//! `PROVISIONER_DATABASE_URL`) for the destroy step. A missing provisioner
//! does not stop the worker: the request phase, grace and approval still
//! run, and the destroy step parks with an alert until it is configured.

use std::sync::Arc;
use std::time::Duration;

use crate::services::account_deletion_live::LiveMailer;
use crate::services::org_deletion::{self, Ctx, Destroyer, ExportStore, OrgDirectory};
use crate::services::org_deletion_live::{LiveDataPlane, R2ExportStore};
use crate::shutdown::Shutdown;
use crate::tenancy::provisioner::{LocalPgProvisioner, NeonProvisioner, TenantDbProvisioner};
use crate::AppState;

const IDLE_INTERVAL: Duration = Duration::from_secs(5);
const FLAG_OFF_INTERVAL: Duration = Duration::from_secs(60);
const SWEEP_INTERVAL: Duration = Duration::from_secs(600);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// The A1 provisioner and its control pool, when both are configured.
fn destroyer_from_env() -> Option<Destroyer> {
    let provisioner: Arc<dyn TenantDbProvisioner> = match std::env::var("TENANT_PROVISIONER").ok().as_deref() {
        Some("neon") => Arc::new(NeonProvisioner::from_env()?),
        Some("local") => Arc::new(LocalPgProvisioner::new(
            std::env::var("TENANT_PROVISIONER_ADMIN_DSN").ok().filter(|v| !v.trim().is_empty())?,
        )),
        _ => return None,
    };
    let provisioner_control = match crate::workers::tenant_provisioning::provisioner_pool_from_env() {
        Ok(Some(p)) => p,
        Ok(None) => return None,
        Err(e) => {
            tracing::error!(connector_health = true, error = %e, "org deletion: provisioner pool unusable");
            return None;
        }
    };
    Some(Destroyer { provisioner, provisioner_control })
}

pub async fn run(state: Arc<AppState>, shutdown: Shutdown) {
    tracing::info!("org-deletion worker started (idle while {} is unset)", org_deletion::FLAG);
    let data = LiveDataPlane { state: state.clone() };
    let mailer = LiveMailer { http: state.http_client.clone(), resend_api_key: state.resend_api_key.clone() };
    let exports: Option<R2ExportStore> = state
        .r2
        .clone()
        .map(|r2| R2ExportStore { blobs: r2 as Arc<dyn crate::services::blob_store::PromotionBlobStore> });
    let destroyer = destroyer_from_env();
    let clerk = crate::clerk_admin::ClerkAdminClient::from_env();
    let kek = coralswarm_core::kek::load_kek().ok();
    let mut last_sweep: Option<std::time::Instant> = None;
    let mut warned = false;
    loop {
        if shutdown.is_triggered() {
            tracing::info!("org-deletion worker: shutdown signalled, stopping");
            return;
        }
        if !org_deletion::enabled() {
            if sleep_or_stop(&shutdown, FLAG_OFF_INTERVAL).await {
                return;
            }
            continue;
        }
        if !warned {
            warned = true;
            if destroyer.is_none() {
                tracing::warn!("org deletion: no provisioner configured; deletions will park at the destroy step");
            }
            if exports.is_none() || kek.is_none() {
                tracing::warn!("org deletion: R2 or MASTER_KEK_HEX missing; deletions will park at the export step");
            }
        }
        let web_base = state.personal_web_base_url();
        let worked = match state.control_db() {
            Some(control) => {
                let ctx = Ctx {
                    control,
                    stripe: state.stripe.as_deref(),
                    directory: clerk.as_ref().map(|c| c as &dyn OrgDirectory),
                    data: &data,
                    exports: exports.as_ref().map(|e| e as &dyn ExportStore),
                    destroyer: destroyer.as_ref(),
                    mailer: &mailer,
                    kek,
                    web_base: &web_base,
                    approval_first_n: org_deletion::approval_first_n_from_env(),
                    crash_after: None,
                };
                if last_sweep.map_or(true, |t| t.elapsed() > SWEEP_INTERVAL) {
                    last_sweep = Some(std::time::Instant::now());
                    let report = org_deletion::sweep(&ctx).await;
                    if report != org_deletion::SweepReport::default() {
                        tracing::info!(?report, "org deletion sweep");
                    }
                }
                match org_deletion::tick(&ctx).await {
                    Ok(worked) => worked,
                    Err(e) => {
                        tracing::error!(error = %e, "org-deletion worker: claim failed");
                        false
                    }
                }
            }
            None => false,
        };
        if worked {
            continue;
        }
        if sleep_or_stop(&shutdown, IDLE_INTERVAL).await {
            return;
        }
    }
}

/// Sleep, or return `true` as soon as shutdown is signalled.
async fn sleep_or_stop(shutdown: &Shutdown, d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => false,
        _ = async {
            loop {
                if shutdown.is_triggered() {
                    return;
                }
                tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
            }
        } => true,
    }
}
