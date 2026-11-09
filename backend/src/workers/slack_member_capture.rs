//! Continues Slack per-member history backfill (Phase G wave 3, item 46b).
//!
//! Connect refreshes the index and pulls one page immediately. This tick
//! pulls one more page per due channel, on that member's `slack_user` token,
//! and writes the cursor on that member's row only.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::slack_member_capture::{backfill_one_channel, due_backfill_rows, load_member_token};
use crate::routes::slack::AccountPlane;
use crate::tenancy::TenantId;
use crate::AppState;

use super::tenant_harness::{Tick, WorkerTenant};

pub const INTERVAL: Duration = Duration::from_secs(30 * 60);

pub async fn tick(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    let only_account = match &t.tenant {
        TenantId::Consumer => None,
        TenantId::Org(org_id) => Some(resolve_account(control_pool, org_id).await?),
    };
    let rows = due_backfill_rows(control_pool, only_account)
        .await
        .map_err(|e| e.to_string())?;
    if rows.is_empty() {
        return Ok(Tick::idle());
    }
    let (plane, clerk_org_id) = match &t.tenant {
        TenantId::Consumer => (AccountPlane::Personal, None),
        TenantId::Org(org_id) => (AccountPlane::Org, Some(org_id.as_str())),
    };
    let mut worked = 0usize;
    for (account_id, user_id, workspace_id, channel_id) in rows {
        let Some(token) = load_member_token(state, control_pool, account_id, user_id).await else {
            continue;
        };
        if backfill_one_channel(
            state,
            &state.http_client,
            control_pool,
            &t.pool,
            account_id,
            user_id,
            &workspace_id,
            &channel_id,
            &token,
            plane,
            clerk_org_id,
        )
        .await
        .is_ok()
        {
            worked += 1;
        }
    }
    Ok(if worked > 0 { Tick::worked(worked) } else { Tick::idle() })
}

async fn resolve_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}
