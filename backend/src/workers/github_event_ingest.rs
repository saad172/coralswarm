//! GitHub event-ingest worker (docs/connectors/github.md §3.7) — **Phase 2:
//! the real worker.** Drains `JobKind::GithubEventIngest` jobs — the durable
//! hand-off `routes::github::github_events` writes for every webhook event
//! it does NOT handle synchronously (`push`, `pull_request`, `issues`,
//! `issue_comment`, `pull_request_review`, `pull_request_review_comment`,
//! `release` — see that route's own doc comment for why `installation`/
//! `installation_repositories` are handled inline instead and never reach
//! this queue) — and calls `connectors::github_event_ingest::handle_event`,
//! the SAME dispatcher `workers::github_reconcile_sweep` calls (§3.7 "same
//! handlers").
//!
//! Tenant loop, mirroring `workers::slack_ingest`/`workers::github_backfill`.
//!
//! # Idempotency
//!
//! GitHub redelivers a webhook on a timeout/5xx with the SAME `X-GitHub-
//! Delivery` id. This tree's `jobs` table has an `idempotency_key` mechanism
//! (migration `0050`, `ON CONFLICT (idempotency_key) WHERE status =
//! 'pending'`), and `routes::github::github_events` already keys the enqueue
//! on `github_event:{delivery_id}` — but that dedup only protects a
//! redelivery that arrives WHILE the first delivery's job is still
//! `pending`. A redelivery that arrives after the first has already
//! completed enqueues a SECOND job (the idempotency key was cleared on
//! completion, same as every other job kind — `jobs::queue::complete`
//! doesn't special-case this one). That second job is safe anyway: every
//! atom this worker writes goes through `routes::ingest::ingest_atom`, whose
//! `atoms` insert is `ON CONFLICT (graph_id, source_type, source_id) DO
//! NOTHING` for an identical body, or a genuine re-ingest (re-chunk/re-embed)
//! for an edited one — exactly the same "re-ingest same source_id" behaviour
//! a live edit is SUPPOSED to trigger. So GitHub redeliveries are idempotent
//! by construction, not by a bespoke delivery-id ledger (documented in
//! docs/connectors/github.md §9).

use std::sync::Arc;
use std::time::Duration;

use crate::connectors::github_event_ingest::{handle_event, EventOutcome};
use crate::jobs::queue::{self, FailureClass, JobOutcome, JobRow};
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

const JOB_BATCH_SIZE: usize = 16;
pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

pub async fn tick(t: &WorkerTenant, state: &Arc<AppState>) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["GithubEventIngest"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(t, state, job).await;
    }
    Ok(Tick::worked(n))
}

#[derive(serde::Deserialize)]
struct EventPayload {
    installation_id: i64,
    #[allow(dead_code)]
    delivery_id: String,
    event: String,
    payload: serde_json::Value,
}

fn redacted_event_payload(mut payload: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = payload.as_object_mut() {
        obj.insert(
            "payload".to_string(),
            serde_json::json!({"_redacted": "[redacted on completion]"}),
        );
    }
    payload
}

async fn process_job(t: &WorkerTenant, state: &Arc<AppState>, job: JobRow) {
    let parsed: Result<EventPayload, _> = serde_json::from_value(job.payload.clone());
    let Ok(parsed) = parsed else {
        tracing::error!(job_id = %job.id, payload = %job.payload, "github_event_ingest: malformed payload");
        if let Err(e) = queue::could_not_do(&t.pool, job.id, FailureClass::Structural, "malformed GithubEventIngest payload").await {
            tracing::error!(error = %e, job_id = %job.id, "github_event_ingest: failed to dead-letter malformed job");
        }
        return;
    };
    let Some(account_id) = job.account_id else {
        tracing::error!(job_id = %job.id, "github_event_ingest: job carries no account_id — refusing unscoped");
        if let Err(e) = queue::could_not_do(&t.pool, job.id, FailureClass::Structural, "job missing account_id").await {
            tracing::error!(error = %e, job_id = %job.id, "github_event_ingest: failed to dead-letter unscoped job");
        }
        return;
    };

    match handle_event(&t.tenant, &t.pool, state, account_id, parsed.installation_id, &parsed.event, &parsed.payload).await {
        Ok(EventOutcome::DidWork { ingested }) => {
            tracing::info!(job_id = %job.id, event = %parsed.event, ingested, "github_event_ingest: event processed");
            if let Err(e) = queue::complete_with_payload(&t.pool, job.id, JobOutcome::DidWork, redacted_event_payload(job.payload)).await {
                tracing::error!(error = %e, job_id = %job.id, "github_event_ingest: failed to complete DidWork job");
            }
        }
        Ok(EventOutcome::NothingToDo) => {
            if let Err(e) = queue::complete_with_payload(&t.pool, job.id, JobOutcome::NothingToDo, redacted_event_payload(job.payload)).await {
                tracing::error!(error = %e, job_id = %job.id, "github_event_ingest: failed to complete NothingToDo job");
            }
        }
        Err(e) => {
            tracing::warn!(job_id = %job.id, event = %parsed.event, error = %e, "github_event_ingest: event processing failed transiently");
            if let Err(qe) = queue::fail(&t.pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "github_event_ingest: failed to mark job as failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_poll_sleep_is_reasonable() {
        assert!(EMPTY_POLL_SLEEP >= Duration::from_millis(100));
        assert!(EMPTY_POLL_SLEEP <= Duration::from_secs(5));
    }

    #[test]
    fn redacted_event_payload_clears_webhook_body() {
        let raw = serde_json::json!({
            "account_id": "00000000-0000-0000-0000-000000000001",
            "installation_id": 1,
            "delivery_id": "abc",
            "event": "issues",
            "payload": {"issue": {"body": "secret text"}}
        });
        let redacted = redacted_event_payload(raw);
        assert_eq!(redacted["event"], "issues");
        assert_eq!(redacted["payload"]["_redacted"], "[redacted on completion]");
        assert!(redacted["payload"].get("issue").is_none());
    }
}
