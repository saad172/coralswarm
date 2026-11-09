//! Member-scoped Chat history, resumed by the existing sweep.
use crate::connectors::catalog::{AdapterCx, AdapterError, Importing};
pub async fn start(cx: &AdapterCx<'_>, days: u32) -> Result<(), AdapterError> {
    let pool = cx.tenant.pool.as_ref().ok_or_else(|| AdapterError::Database("content plane unavailable".into()))?;
    let until = chrono::Utc::now();
    let cursor = super::super::super::google_chat::HistoryCursor::new(until, days).encode();
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, cx.account_id).await?;
    let spaces: Vec<String> = sqlx::query_scalar("SELECT space_id FROM google_chat_watched_spaces WHERE account_id = $1 AND owner_user_id = $2 AND active ORDER BY space_id")
        .bind(cx.account_id).bind(cx.user_id).fetch_all(&mut *tx).await?;
    for space in spaces {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(crate::workers::google_chat_sweep::mailbox_space_lock_key(cx.account_id, &space, cx.user_id))
            .execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE google_chat_watched_spaces SET backfill_cursor = $3, backfill_complete = $4, sync_token = CASE WHEN $4 THEN $5 ELSE sync_token END WHERE account_id = $1 AND owner_user_id = $2 AND active")
        .bind(cx.account_id).bind(cx.user_id).bind(if days == 0 { None } else { Some(cursor) }).bind(days == 0).bind(until.to_rfc3339()).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
pub async fn importing(cx: &AdapterCx<'_>) -> Result<Option<Importing>, AdapterError> {
    let Some(row) = cx.facts.source("google", "chat") else { return Ok(None); };
    let Some(days) = row.history_days.filter(|d| *d > 0) else { return Ok(None); };
    let Some(pool) = cx.tenant.pool.as_ref() else { return Ok(None); };
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, cx.account_id).await?;
    let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM google_chat_watched_spaces WHERE account_id = $1 AND owner_user_id = $2 AND active AND NOT backfill_complete)")
        .bind(cx.account_id).bind(cx.user_id).fetch_one(&mut *tx).await?;
    tx.rollback().await?;
    Ok(pending.then_some(Importing { window_days: days as u32, percent: None }))
}
