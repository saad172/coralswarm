//! Slack message ingest worker — the tenant-looped consumer of the durable
//! hand-off `routes::slack_events::enqueue_slack_message_ingest` writes onto
//! each resolved plane's own `jobs` table (org-slack step 2).
//!
//! # Why a job queue, not the bare `tokio::spawn` this replaced
//!
//! Before this worker existed, `/v1/slack/events`' message dispatch called
//! `routes::slack_events::ingest_slack_message` directly from a background
//! `state.task_tracker.spawn` — best-effort, in-process, and CONSUMER-plane
//! only (it always resolved `account_id`/`pool` against `state.db`). Giving
//! the webhook tenant reach means resolving WHICH tenant(s) an event belongs
//! to and handing the actual ingest work to whichever plane's `jobs` table
//! that tenant owns — the durable version of the same hand-off `workers::
//! huddle` already established for `JobKind::SlackHuddleCapture` (see that
//! module's own doc comment), reusing the queue this codebase already has
//! rather than inventing a second hand-off table (founder decision — no new
//! table for this).
//!
//! # Plane
//!
//! Tenant loop. `JobKind::SlackMessageIngest` jobs land on WHICHEVER plane
//! `enqueue_slack_message_ingest` resolved at enqueue time — the consumer
//! `jobs` table for a personal (or not-yet-tenant-migrated org) account, or a
//! specific org's own tenant `jobs` table once one is resolved. This worker
//! therefore has to run against every plane the harness knows about, exactly
//! like `reaper`/`tombstone_purge`: each tenant DB (and the consumer plane)
//! has its OWN `jobs` table, invisible to a dequeuer scoped to any other one.
//!
//! # Content-at-rest (PR-ready commit message has the full reasoning)
//!
//! `JobKind::SlackMessageIngest`'s payload carries the raw message TEXT —
//! plaintext, in `jobs.payload` (JSONB) — for as long as the job is
//! `pending`/`running`. `jobs` rows are retained indefinitely once `done`
//! (migration `0050`'s own header), so a job left un-scrubbed after a
//! successful ingest would leave that plaintext sitting at rest in a place
//! it never lived before this hand-off (previously: straight into an
//! encrypted atom blob, never through an intermediate plaintext row).
//! [`tick`] scrubs it: [`jobs::queue::complete_with_payload`] overwrites
//! `payload` with `text` cleared (everything else — `account_id`,
//! `workspace_id`, `channel_id`, `ts` — kept for observability) the moment
//! `ingest_slack_message` returns `Ok`. A `dead`-lettered job (ingest
//! permanently failed after `max_attempts`) is NOT scrubbed — an operator
//! may need the body to diagnose or hand-replay it — an accepted, narrow
//! residual against the unbounded default every OTHER terminal state would
//! otherwise carry.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// How many `SlackMessageIngest` jobs to dequeue per poll cycle, per tenant.
/// Small and deliberately similar to `workers::huddle`'s own batch size:
/// ordinary channel messages are comparatively high-volume and this worker's
/// harness config (`HarnessConfig::queue_worker`) already re-polls
/// immediately whenever a batch is non-empty (`hot_loop_on_work: true`), so a
/// small batch does not add latency — it bounds how much of ONE tenant's
/// backlog a single tick can starve the other tenants' round-robin slot with.
const JOB_BATCH_SIZE: usize = 8;

/// Idle-poll interval on the consumer plane / floor for org tenants — mirrors
/// `workers::huddle::EMPTY_POLL_SLEEP`.
pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// One `SlackMessageIngest` pass on one tenant's `(pool, store)` pair: dequeue
/// up to [`JOB_BATCH_SIZE`] jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a queue worker
/// (`HarnessConfig::queue_worker`), which owns the loop, the empty-poll
/// sleep, per-tenant error backoff, and the org idle-backoff/dormant-slot
/// scheduling — see that module's doc for the full contract.
///
/// `control_pool` is the shared control/consumer pool — needed ONLY to
/// resolve each job's `super::routes::slack::AccountPlane` (`accounts.
/// clerk_org_id`, a control-only table that does not exist on `t.pool` when
/// `t.tenant` is an org tenant); every other read/write in `ingest_slack_
/// message` runs on `t.pool`/`t.store`. `state` is threaded through purely
/// for `ingest_slack_message`'s plane-agnostic shared clients
/// (`http_client`, `r2`, `dek_cache`) — same pattern `workers::
/// graph_subscriptions::tick_backstop` uses for a full `Arc<AppState>`
/// capture (org-tenancy m365 Train 2 PR 2.2).
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["SlackMessageIngest"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(t, control_pool, state, job).await;
    }
    Ok(Tick::worked(n))
}

/// Fields `JobKind::SlackMessageIngest` carries — deserialized straight from
/// `job.payload` (see that variant's own doc comment for why every field is
/// captured at enqueue time rather than re-derived later).
#[derive(serde::Deserialize)]
struct SlackMessageIngestPayload {
    account_id: Uuid,
    workspace_id: String,
    channel_id: String,
    user_id: String,
    ts: String,
    thread_ts: Option<String>,
    text: String,
    /// Absent on every job enqueued before bot-message ingest shipped — see
    /// `JobKind::SlackMessageIngest`'s own doc comment for why this and
    /// `bot_name` both carry `#[serde(default)]`.
    #[serde(default)]
    bot_id: Option<String>,
    #[serde(default)]
    bot_name: Option<String>,
}

async fn process_job(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>, job: JobRow) {
    let payload: SlackMessageIngestPayload = match serde_json::from_value(job.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("malformed SlackMessageIngest payload: {e}");
            tracing::error!(job_id = %job.id, error = %msg, "slack ingest worker: malformed job payload");
            if let Err(qe) = queue::fail(&t.pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // Every `SlackMessageIngest` job is stamped with `account_id` at enqueue
    // time (`enqueue_slack_message_ingest` always passes `Some(account_id)`)
    // — the payload's OWN `account_id` field is the same value, kept because
    // `ingest_slack_message` needs it as a typed argument regardless of
    // `jobs.account_id`'s bookkeeping role. A mismatch between the two would
    // mean an upstream bug, not a legitimate state; prefer the payload's
    // (what `ingest_slack_message` will actually act on) and just note the
    // drift if it ever happens.
    if job.account_id.is_some_and(|id| id != payload.account_id) {
        tracing::warn!(
            job_id = %job.id,
            payload_account_id = %payload.account_id,
            jobs_row_account_id = ?job.account_id,
            "slack ingest worker: jobs.account_id disagrees with the payload's own account_id \
             — proceeding with the payload's (what ingest_slack_message will act on)"
        );
    }

    let account_plane = match crate::routes::slack::account_plane(control_pool, payload.account_id).await {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("account plane lookup failed: {e}");
            tracing::warn!(job_id = %job.id, error = %msg, "slack ingest worker: {msg}");
            if let Err(qe) = queue::fail(&t.pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job as failed after account-plane lookup error");
            }
            return;
        }
    };

    let result = crate::routes::slack_events::ingest_slack_message(
        state.clone(),
        control_pool.clone(),
        t.pool.clone(),
        t.store.clone(),
        t.tenant.clone(),
        payload.account_id,
        account_plane,
        payload.workspace_id,
        payload.channel_id,
        payload.user_id,
        payload.ts,
        payload.thread_ts,
        payload.text,
        payload.bot_id,
        payload.bot_name,
    )
    .await;

    match result {
        Ok(()) => {
            // Content-at-rest mitigation (see this module's doc comment):
            // clear the plaintext message text before this `done` row starts
            // its indefinite retention. Every other payload field is kept —
            // cheap, useful for debugging, and carries no message content.
            let mut redacted = job.payload;
            if let Some(obj) = redacted.as_object_mut() {
                obj.insert(
                    "text".to_string(),
                    serde_json::Value::String("[redacted on completion]".to_string()),
                );
            }
            if let Err(e) =
                queue::complete_with_payload(&t.pool, job.id, queue::JobOutcome::DidWork, redacted).await
            {
                tracing::error!(error = %e, job_id = %job.id, "failed to complete SlackMessageIngest job");
            }
        }
        Err(e) => {
            let msg = e.to_string();
            tracing::warn!(job_id = %job.id, error = %msg, "slack ingest worker: ingest_slack_message failed");
            // Deliberately NOT scrubbed on this path — see this module's doc
            // comment: a dead-lettered job may need its body for diagnosis
            // or a hand-replay, so the redaction only ever happens on the
            // success path above.
            if let Err(qe) = queue::fail(&t.pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job as failed after ingest error");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JobKind;

    #[test]
    fn slack_message_ingest_payload_roundtrips_through_job_kind() {
        let account_id = Uuid::new_v4();
        let kind = JobKind::SlackMessageIngest {
            account_id,
            workspace_id: "T123".to_string(),
            channel_id: "C123".to_string(),
            user_id: "U123".to_string(),
            ts: "1234.5678".to_string(),
            thread_ts: None,
            text: "hello".to_string(),
            bot_id: Some("B0C021UR1CY".to_string()),
            bot_name: Some("GitHub".to_string()),
        };
        let tagged = serde_json::to_value(&kind).unwrap();
        assert_eq!(tagged["kind"], "SlackMessageIngest");
        let payload: SlackMessageIngestPayload =
            serde_json::from_value(tagged["payload"].clone()).unwrap();
        assert_eq!(payload.account_id, account_id);
        assert_eq!(payload.workspace_id, "T123");
        assert_eq!(payload.text, "hello");
        assert_eq!(payload.bot_id.as_deref(), Some("B0C021UR1CY"));
        assert_eq!(payload.bot_name.as_deref(), Some("GitHub"));
    }

    /// A job enqueued BEFORE bot-message ingest shipped has no `bot_id`/
    /// `bot_name` keys at all. Without `#[serde(default)]` on both, every one
    /// of those still-`pending` rows would dead-letter on the first deploy
    /// with "malformed SlackMessageIngest payload" — this asserts the
    /// compatibility, not merely the presence of the attribute.
    #[test]
    fn a_payload_enqueued_before_bot_fields_existed_still_deserialises() {
        let legacy = serde_json::json!({
            "account_id": Uuid::new_v4(),
            "workspace_id": "T123",
            "channel_id": "C123",
            "user_id": "U123",
            "ts": "1234.5678",
            "thread_ts": serde_json::Value::Null,
            "text": "hello from before the change",
        });
        let payload: SlackMessageIngestPayload = serde_json::from_value(legacy).unwrap();
        assert_eq!(payload.text, "hello from before the change");
        assert!(payload.bot_id.is_none());
        assert!(payload.bot_name.is_none());
    }

    /// The content-at-rest mitigation's redaction shape: `text` cleared,
    /// every other field preserved.
    #[test]
    fn redacted_payload_clears_text_but_keeps_other_fields() {
        let account_id = Uuid::new_v4();
        let original = serde_json::json!({
            "account_id": account_id,
            "workspace_id": "T123",
            "channel_id": "C123",
            "user_id": "U123",
            "ts": "1234.5678",
            "thread_ts": null,
            "text": "the secret message body",
        });

        let mut redacted = original.clone();
        if let Some(obj) = redacted.as_object_mut() {
            obj.insert(
                "text".to_string(),
                serde_json::Value::String("[redacted on completion]".to_string()),
            );
        }

        assert_ne!(redacted["text"], original["text"]);
        assert!(!redacted.to_string().contains("the secret message body"));
        assert_eq!(redacted["workspace_id"], original["workspace_id"]);
        assert_eq!(redacted["channel_id"], original["channel_id"]);
        assert_eq!(redacted["ts"], original["ts"]);
    }
}
