//! Slack's import history: one `HistoryBackfill` job per channel, the same job
//! `routes::slack::request_backfill` queues, so the worker, the window cap
//! (`connectors::history_backfill::HISTORY_BACKFILL_MAX_DAYS`) and the
//! durability rule are the existing ones.
//!
//! Progress is the share of those jobs that have ended. `jobs` rows carry the
//! request in their idempotency key (`connector_history:slack:{user}:{ms}:{channel}`),
//! where `{ms}` is `connector_sources.history_requested_at`, so the count is
//! exactly the jobs of the latest request and an older run never leaks into it.
//! A job counts as ended when it is `done` or `dead`: a channel the bot cannot
//! read ends `dead` after its retries, and must not hold the bar at 90% for ever.
//! `jobs` has no index on the key; the count runs only for a member with a window
//! on file, for the day after the request (`history::IN_FLIGHT_SECS`).

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::{recent, requested_at};
use crate::connectors::catalog::facts::SourceRow;
use crate::connectors::catalog::{AdapterCx, AdapterError, Importing};
use crate::jobs::{queue, JobKind};
use crate::routes::slack::{resolve_primary_graph, AccountPlane, IngestCaller};

/// Channels queued per request, the same bound `request_backfill` puts on a
/// request: someone in two hundred channels is two hundred durable jobs
/// otherwise. Channels past it are logged, not queued.
pub const MAX_CHANNELS: usize = 25;

/// The job key prefix of one request, shared by every channel's job.
pub fn key_prefix(user_id: Uuid, requested_at: DateTime<Utc>) -> String {
    format!(
        "connector_history:slack:{user_id}:{}:",
        requested_at.timestamp_millis()
    )
}

/// The share of a request's jobs that have ended, while some have not. `None`
/// means nothing is running: no jobs were queued, or all of them ended.
pub fn in_flight_percent(ended: i64, total: i64) -> Option<u8> {
    if total <= 0 || ended >= total {
        return None;
    }
    Some((ended.max(0) * 100 / total) as u8)
}

/// Split the channels capture covers into those queued and the count left out.
pub fn plan_channels(mut channels: Vec<String>) -> (Vec<String>, usize) {
    channels.sort();
    channels.dedup();
    let left_out = channels.len().saturating_sub(MAX_CHANNELS);
    channels.truncate(MAX_CHANNELS);
    (channels, left_out)
}

/// The channels the member's capture covers: their `users.conversations` index.
/// The one place the set is chosen; the scope layer narrows it to what the
/// member picked.
async fn captured_channels(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    workspace_id: &str,
) -> Result<Vec<String>, AdapterError> {
    Ok(sqlx::query_scalar(
        "SELECT channel_id FROM slack_member_channels \
         WHERE account_id = $1 AND user_id = $2 AND workspace_id = $3",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(workspace_id)
    .fetch_all(control)
    .await?)
}

/// Queue the window. The adapter's `start_backfill` delegates here.
pub async fn start(cx: &AdapterCx<'_>, days: u32) -> Result<(), AdapterError> {
    let facts = cx.facts;
    // The jobs read with the workspace app's token; without it they would only fail.
    if facts.token("slack").is_none() {
        return Err(AdapterError::Refused {
            status: 409,
            code: "app_not_installed",
        });
    }
    let workspace_id = facts
        .token("slack_user")
        .filter(|t| t.mine)
        .or(facts.token("slack"))
        .and_then(|t| t.provider_workspace_id.clone())
        .filter(|w| !w.is_empty())
        .ok_or(AdapterError::NotConnected)?;

    let org = cx.tenant.org_context();
    if let Some(ctx) = org {
        if !crate::routes::oauth::org_slack_oauth_enabled_for(cx.state, &ctx.org_id).await {
            return Err(AdapterError::Refused {
                status: 403,
                code: "forbidden",
            });
        }
    }
    let plane = if org.is_some() {
        AccountPlane::Org
    } else {
        AccountPlane::Personal
    };
    let control = cx
        .state
        .control_db()
        .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
    let tenant_pool = cx
        .tenant
        .pool
        .as_ref()
        .ok_or_else(|| AdapterError::Database("content plane unavailable".into()))?;

    let requested = requested_at(cx, "slack", "channels")
        .await?
        .ok_or_else(|| AdapterError::Database("no history request on file".into()))?;
    let (channels, left_out) =
        plan_channels(captured_channels(control, cx.account_id, cx.user_id, &workspace_id).await?);
    if left_out > 0 {
        tracing::info!(
            account_id = %cx.account_id, queued = channels.len(), left_out,
            "slack history: more channels than one request queues; the rest were left out"
        );
    }
    if channels.is_empty() {
        return Ok(());
    }

    // The one ocean every job writes into, resolved from the authenticated
    // caller here and never re-derived by the worker.
    let target_graph_id = resolve_primary_graph(
        tenant_pool,
        cx.account_id,
        plane,
        IngestCaller::Attributable(cx.user_id),
    )
    .await?;
    let prefix = key_prefix(cx.user_id, requested);
    for channel_id in channels {
        let job = JobKind::HistoryBackfill {
            provider: "slack".to_string(),
            account_id: cx.account_id,
            workspace_id: workspace_id.clone(),
            container_id: channel_id.clone(),
            target_graph_id,
            target_user_id: cx.user_id,
            clerk_org_id: org.map(|c| c.org_id.clone()),
            window_days: Some(days),
        };
        queue::enqueue(
            tenant_pool,
            &job,
            Some(cx.account_id),
            Some(&format!("{prefix}{channel_id}")),
            0,
        )
        .await?;
    }
    Ok(())
}

/// `(ended, total)` of the jobs one request queued.
async fn job_counts(
    pool: &PgPool,
    account_id: Uuid,
    prefix: &str,
) -> Result<(i64, i64), AdapterError> {
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE status IN ('done', 'dead')), count(*) \
         FROM jobs \
         WHERE kind = 'HistoryBackfill' AND account_id = $1 AND starts_with(idempotency_key, $2)",
    )
    .bind(account_id)
    .bind(prefix)
    .fetch_one(pool)
    .await?;
    Ok(counts)
}

/// The run in flight for this member's Slack channels, if any.
pub async fn importing(
    cx: &AdapterCx<'_>,
    row: Option<&SourceRow>,
) -> Result<Option<Importing>, AdapterError> {
    let Some(row) = row else { return Ok(None) };
    let (Some(days), Some(requested)) = (row.history_days.filter(|d| *d > 0), row.history_requested_at)
    else {
        return Ok(None);
    };
    if !recent(requested, Utc::now()) {
        return Ok(None);
    }
    let Some(pool) = cx.tenant.pool.as_ref() else {
        return Ok(None);
    };
    let (ended, total) = job_counts(pool, cx.account_id, &key_prefix(cx.user_id, requested)).await?;
    Ok(in_flight_percent(ended, total).map(|percent| Importing {
        percent: Some(percent),
        window_days: days as u32,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_prefix_names_the_member_and_the_request_to_the_millisecond() {
        let user = Uuid::nil();
        let at: DateTime<Utc> = "2026-10-30T12:00:00.123456Z".parse().unwrap();
        assert_eq!(
            key_prefix(user, at),
            format!("connector_history:slack:{user}:1793361600123:")
        );
        // Another request, or another member, never shares a prefix.
        let later = at + chrono::Duration::milliseconds(1);
        assert_ne!(key_prefix(user, at), key_prefix(user, later));
        assert_ne!(key_prefix(user, at), key_prefix(Uuid::new_v4(), at));
        // Neither prefix is a prefix of the other, whatever follows.
        assert!(!key_prefix(user, later).starts_with(&key_prefix(user, at)));
    }

    #[test]
    fn percent_is_ended_over_queued_while_any_job_is_left() {
        assert_eq!(in_flight_percent(0, 4), Some(0));
        assert_eq!(in_flight_percent(1, 4), Some(25));
        assert_eq!(in_flight_percent(3, 4), Some(75));
        assert_eq!(in_flight_percent(1, 3), Some(33), "rounds down: 100% means finished");
    }

    #[test]
    fn nothing_is_importing_when_all_ended_or_none_were_queued() {
        assert_eq!(in_flight_percent(4, 4), None);
        assert_eq!(in_flight_percent(5, 4), None);
        assert_eq!(in_flight_percent(0, 0), None);
    }

    #[test]
    fn channels_are_deduped_sorted_and_bounded() {
        let many: Vec<String> = (0..40).rev().map(|i| format!("C{i:03}")).collect();
        let (queued, left_out) = plan_channels(many);
        assert_eq!(queued.len(), MAX_CHANNELS);
        assert_eq!(left_out, 15);
        assert_eq!(queued.first().unwrap(), "C000");
        assert!(queued.windows(2).all(|w| w[0] < w[1]));

        let (queued, left_out) = plan_channels(vec!["C2".into(), "C1".into(), "C2".into()]);
        assert_eq!(queued, ["C1", "C2"]);
        assert_eq!(left_out, 0);
        assert_eq!(plan_channels(vec![]), (vec![], 0));
    }
}
