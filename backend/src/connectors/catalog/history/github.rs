//! GitHub's import history. The automatic backfill (started when repos are
//! picked) stays unbounded; a window the member chooses restarts each of their
//! active repos from a fresh cursor that carries a floor. `process_type` in
//! `connectors::github_backfill` stops a type at the first item older than it,
//! and the floor lives in the cursor, so a resumed or rate-limited tick keeps it.
//!
//! Progress is the existing per-repo `backfill_progress`, read by
//! `github_facts`. The reset writes zeros into it so the source reads
//! `importing` from this moment and not from the worker's first tick.

use chrono::Utc;
use uuid::Uuid;

use super::window_floor;
use crate::connectors::catalog::{AdapterCx, AdapterError};
use crate::connectors::github_backfill::NarrativeCursor;
use crate::jobs::{queue, JobKind};

/// Restart the member's active repos at `days`. The adapter's `start_backfill`
/// delegates here.
pub async fn start(cx: &AdapterCx<'_>, days: u32) -> Result<(), AdapterError> {
    let pool = cx
        .tenant
        .pool
        .as_ref()
        .ok_or_else(|| AdapterError::Database("content plane unavailable".into()))?;
    let cursor = serde_json::to_value(NarrativeCursor::windowed(window_floor(Utc::now(), days)))
        .map_err(|e| AdapterError::Database(e.to_string()))?;

    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, cx.account_id).await?;
    let repos: Vec<Uuid> = sqlx::query_scalar(
        "UPDATE github_watched_repos \
         SET narrative_cursor = $3, \
             backfill_progress = COALESCE(backfill_progress, '{}'::jsonb) \
                 || '{\"prs\":0,\"issues\":0,\"releases\":0,\"commits\":0}'::jsonb, \
             updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND active \
         RETURNING id",
    )
    .bind(cx.account_id)
    .bind(cx.user_id)
    .bind(cursor)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;

    // The same key `put_repos` uses: a job still pending for the repo is the
    // job, because the cursor is read when it runs.
    for repo_row_id in repos {
        queue::enqueue(
            pool,
            &JobKind::GithubBackfill { repo_row_id },
            Some(cx.account_id),
            Some(&format!("github_backfill:{repo_row_id}")),
            0,
        )
        .await?;
    }
    Ok(())
}
