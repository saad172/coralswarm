//! Durable per-watcher Teams history requests, resumed by the Graph worker.
use super::window_floor;
use crate::connectors::catalog::{AdapterCx, AdapterError};
use chrono::Utc;
pub async fn start(cx: &AdapterCx<'_>, source: &str, days: u32) -> Result<(), AdapterError> {
    let pool = cx.tenant.pool.as_ref().ok_or_else(|| AdapterError::Database("content plane unavailable".into()))?;
    let until = Utc::now();
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, cx.account_id).await?;
    sqlx::query("UPDATE teams_watched_resources SET backfill_since = $4, backfill_until = $5, backfill_cursor = NULL, backfill_complete = FALSE WHERE account_id = $1 AND user_id = $2 AND kind = $3")
        .bind(cx.account_id).bind(cx.user_id).bind(if source == "teams_chats" { "chat" } else { "channel" })
        .bind(window_floor(until, days)).bind(until).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn importing(cx: &AdapterCx<'_>, source: &str) -> Result<Option<crate::connectors::catalog::Importing>, AdapterError> {
    let Some(row) = cx.facts.source("microsoft", source) else { return Ok(None); };
    let Some(days) = row.history_days.filter(|d| *d > 0) else { return Ok(None); };
    if row.history_requested_at.is_none() { return Ok(None); }
    let Some(pool) = cx.tenant.pool.as_ref() else { return Ok(None); };
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, cx.account_id).await?;
    let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM teams_watched_resources WHERE account_id = $1 AND user_id = $2 AND kind = $3 AND NOT backfill_complete)")
        .bind(cx.account_id).bind(cx.user_id).bind(if source == "teams_chats" { "chat" } else { "channel" }).fetch_one(&mut *tx).await?;
    tx.rollback().await?;
    Ok(pending.then_some(crate::connectors::catalog::Importing { window_days: days as u32, percent: None }))
}
