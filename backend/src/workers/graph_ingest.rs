//! Microsoft Graph connector message ingest worker (m365 Train 2 PR 2.3) —
//! the tenant-looped consumer of the durable hand-off `routes::graph_webhook`
//! writes onto each resolved plane's own `jobs` table. Mirrors `workers::
//! slack_ingest`'s own module doc for the parallel design (org-slack step
//! 2's `JobKind::SlackMessageIngest` precedent).
//!
//! # Why a job queue, not `Tenant::legacy(state)`
//!
//! Before this worker existed, `routes::graph_webhook::ingest_message_with`
//! always built `tenancy::extractor::Tenant::legacy(state)` — the personal
//! plane — REGARDLESS of which plane the notifying account was actually on.
//! `routes::graph_webhook::resolve_graph_ingest_target` now decides: a
//! `Personal` account keeps that exact synchronous path (unchanged, never
//! touches this worker); an `Org` account is handed off through
//! `JobKind::GraphMessageIngest` instead, onto whichever plane's own `jobs`
//! table the account's tenant-routing status resolved to. This worker is the
//! consumer side of that hand-off.
//!
//! # Plane
//!
//! Tenant loop. `JobKind::GraphMessageIngest` jobs land on WHICHEVER plane
//! `resolve_graph_ingest_target` resolved at enqueue time — the consumer
//! `jobs` table for an org not (yet) tenant-migrated, or a specific org's own
//! tenant `jobs` table once one is `Active`. This worker therefore has to run
//! against every plane the harness knows about, same reasoning `workers::
//! slack_ingest`'s own module doc gives (each tenant DB has its OWN `jobs`
//! table, invisible to a dequeuer scoped to any other one).
//!
//! **No `control_pool` parameter, unlike `slack_ingest::tick`.** Slack's
//! worker re-derives each job's `AccountPlane` from the control plane at
//! dequeue time; this worker doesn't need to; the plane decision already
//! happened once, at enqueue time, in `resolve_graph_ingest_target` — a
//! `GraphMessageIngest` job's presence on a given tenant's `jobs` table IS
//! the plane decision. Re-deriving it here would only be useful as a
//! TOCTOU re-check (e.g. an org revoked between enqueue and dequeue); that
//! refinement is left for a follow-up if it proves necessary, mirroring how
//! `slack_ingest` itself does not re-check `org_revocations` either.
//!
//! # A genuine `TenancyDecision::Org`, not `Tenant::legacy` with a swapped pool
//!
//! On an ORG tenant tick, this worker builds a REAL
//! [`crate::tenancy::runtime::TenancyDecision::Org`] — the exact shape
//! `tenancy::extractor::Tenant`'s own axum `FromRequestParts` impl builds for
//! a genuine org request — rather than reusing `Tenant::legacy` with a
//! swapped-in pool. This matters structurally: `ingest::submit`'s
//! reachability check
//! ([`crate::tenancy::reachability::ReachabilityResolver::
//! graph_reachable_by_user`]) branches on `tenant.decision` itself, and its
//! `None`/`Personal` arm calls SECURITY DEFINER functions
//! (`resolve_graph_reachable_by_user`, migration `0084`) that exist ONLY on
//! the control/consumer database — calling them against a genuine org tenant
//! pool would fail with "function does not exist". Only a real
//! `TenancyDecision::Org` routes through the tenant-pool-native resolver
//! (`resolve_tenant_graph_reachable_by_user`, `migrations_tenant/0006`) that
//! actually exists there. `TenantContext::user_id` is set to the job's own
//! `user_id` (never a verified caller — see `JobKind::GraphMessageIngest`'s
//! own doc comment on why this is bookkeeping provenance, not an attributed
//! identity) so `ReachabilityResolver`'s `require_matching_user` check —
//! which refuses to serve any `user_id` other than the context's own — is
//! satisfied by construction: this worker only ever asks for the SAME
//! `user_id` it just stamped the context with.
//!
//! # `graph_id` is NOT re-derived here
//!
//! Unlike `workers::slack_ingest` (which re-resolves the channel destination
//! fresh inside `ingest_slack_message`), this worker trusts the `graph_id`/
//! `user_id` the webhook already resolved at enqueue time — see
//! `JobKind::GraphMessageIngest`'s own doc comment for why (the
//! watched-resource tables that map a Teams/Outlook container to a
//! `graph_id`, `teams_watched_resources`/`outlook_watched_conversations`, are
//! still control-plane-only as of this PR; m365 Train 2 PR 2.4 is what makes
//! THEIR OWN resolution plane-aware). On an org tenant tick this means: a job
//! whose `graph_id` does not actually exist on `t.pool` yet (the ordinary
//! case until PR 2.4 lands) fails closed — `ingest::submit`'s own
//! `graph_account_id` lookup returns `None`, the request 403s, and this
//! worker records that as a normal job failure — never a panic, never a
//! silent success. This is expected and documented, not a bug: PR 2.3's job
//! is the hand-off MECHANISM, not the destination-resolution PR 2.4 owns.
//!
//! # Content-at-rest
//!
//! Same mitigation as `workers::slack_ingest`'s own module doc:
//! `JobKind::GraphMessageIngest::content` is the plaintext connector body,
//! landing in `jobs.payload` (JSONB) for as long as the job is
//! `pending`/`running`. On success, [`tick`] scrubs it via
//! [`crate::jobs::queue::complete_with_payload`] (content cleared, every
//! other field kept for observability); a `dead`-lettered job is left
//! un-scrubbed so an operator can diagnose or hand-replay it.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::middleware::auth::UserContext;
use crate::ingest::atom::IngestAtomRequest;
use crate::store::{PostgresStore, Store};
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::{TenancyDecision, TenantContext};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// How many `GraphMessageIngest` jobs to dequeue per poll cycle, per tenant —
/// mirrors `workers::slack_ingest::JOB_BATCH_SIZE`'s own reasoning (bounds how
/// much of one tenant's backlog a single tick can starve the round-robin
/// slot with; the harness re-polls immediately whenever a batch is
/// non-empty).
const JOB_BATCH_SIZE: usize = 8;

/// Idle-poll interval on the consumer plane / floor for org tenants — mirrors
/// `workers::slack_ingest::EMPTY_POLL_SLEEP`.
pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// One `GraphMessageIngest` pass on one tenant's `(pool, store)` pair:
/// dequeue up to [`JOB_BATCH_SIZE`] jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a queue worker
/// (`HarnessConfig::queue_worker`), which owns the loop, the empty-poll
/// sleep, per-tenant error backoff, and the org idle-backoff/dormant-slot
/// scheduling — see that module's doc for the full contract.
pub async fn tick(t: &WorkerTenant, state: &Arc<AppState>) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["GraphMessageIngest"], JOB_BATCH_SIZE)
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

/// Fields `JobKind::GraphMessageIngest` carries — deserialized straight from
/// `job.payload` (see that variant's own doc comment for why every field is
/// captured at enqueue time rather than re-derived later).
#[derive(serde::Deserialize)]
struct GraphMessageIngestPayload {
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    channel_name: Option<String>,
    source_type: String,
    source_id: String,
    content: String,
    participants: Vec<String>,
    timestamp: chrono::DateTime<chrono::Utc>,
    scope: Option<String>,
    channel_id: Option<String>,
    author_id: Option<String>,
    author_name: Option<String>,
}

async fn process_job(t: &WorkerTenant, state: &Arc<AppState>, job: JobRow) {
    let payload: GraphMessageIngestPayload = match serde_json::from_value(job.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("malformed GraphMessageIngest payload: {e}");
            tracing::error!(job_id = %job.id, error = %msg, "graph ingest worker: malformed job payload");
            if let Err(qe) = queue::fail(&t.pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // Same drift-tolerance posture as `workers::slack_ingest::process_job`:
    // every `GraphMessageIngest` job is stamped with `account_id` at enqueue
    // time, and the payload's OWN copy is what actually gets acted on.
    if job.account_id.is_some_and(|id| id != payload.account_id) {
        tracing::warn!(
            job_id = %job.id,
            payload_account_id = %payload.account_id,
            jobs_row_account_id = ?job.account_id,
            "graph ingest worker: jobs.account_id disagrees with the payload's own account_id \
             — proceeding with the payload's (what this job will actually act on)"
        );
    }

    // Consent can be revoked while an already fetched transcript is queued.
    if payload.source_type == "teams_transcript" {
        let lifecycle_ok = match &t.tenant {
            TenantId::Consumer => false,
            TenantId::Org(org) => {
                if let Some(control) = state.control_db() {
                    crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, org).await
                        && matches!(crate::middleware::auth::live_org_membership_for_capture(control,org,payload.user_id).await,Ok(Some((account,_))) if account==payload.account_id)
                } else {
                    false
                }
            }
        };
        let allowed = lifecycle_ok
            && if let (Some(control), Some((meeting, _))) =
                (state.control_db(), payload.source_id.rsplit_once(':'))
            {
                if let Ok(Some((tid, aad))) = crate::routes::teams_onboarding::identity(
                    control,
                    payload.account_id,
                    payload.user_id,
                )
                .await
                {
                    crate::services::teams_meeting_capture::active_graph_at(
                        &t.pool,
                        payload.account_id,
                        payload.user_id,
                        &tid,
                        &aad,
                        meeting,
                        payload.timestamp,
                    )
                    .await
                    .ok()
                    .flatten()
                        == Some(payload.graph_id)
                } else {
                    false
                }
            } else {
                false
            };
        if !allowed {
            let mut redacted = job.payload;
            if let Some(o) = redacted.as_object_mut() {
                o.insert(
                    "content".into(),
                    serde_json::json!("[redacted: meeting consent no longer active]"),
                );
            }
            let _ =
                queue::complete_with_payload(&t.pool, job.id, queue::JobOutcome::DidWork, redacted)
                    .await;
            return;
        }
    }
    // The pause gate again, at the other end of the hand-off: the member may
    // have paused the source while this job waited in the queue. The message is
    // dropped and its body scrubbed, as when meeting consent has gone.
    if let Some(key) =
        crate::connectors::catalog::pause::SourceKey::microsoft_for_atom(&payload.source_type)
    {
        if crate::connectors::catalog::pause::is_paused_in(state, payload.account_id, payload.user_id, key).await {
            let mut redacted = job.payload;
            if let Some(o) = redacted.as_object_mut() {
                o.insert(
                    "content".into(),
                    serde_json::json!("[redacted: source paused]"),
                );
            }
            let _ =
                queue::complete_with_payload(&t.pool, job.id, queue::JobOutcome::DidWork, redacted)
                    .await;
            return;
        }
    }
    // Build the plane-correct `Tenant` — see this module's own doc comment
    // for why the Org arm is a genuine `TenancyDecision::Org`, not
    // `Tenant::legacy` with a swapped-in pool.
    let tenant = match &t.tenant {
        TenantId::Consumer => Tenant::legacy(state),
        TenantId::Org(org_id) => {
            let store: Arc<dyn Store> = Arc::new(PostgresStore::new(t.pool.clone()));
            Tenant {
                store: Some(store),
                pool: Some(t.pool.clone()),
                decision: Some(TenancyDecision::Org(TenantContext {
                    org_id: org_id.clone(),
                    account_id: payload.account_id,
                    user_id: payload.user_id,
                    pool: t.pool.clone(),
                    page_pool: None,
                })),
                class: crate::route_class::RouteClass::Other,
            }
        }
    };

    let user_ctx = UserContext {
        user_id: payload.user_id,
        account_id: payload.account_id,
        role: "member".into(),
        device_id: String::new(),
    };
    let req = IngestAtomRequest {
        graph_id: payload.graph_id,
        source_type: payload.source_type.clone(),
        source_id: payload.source_id.clone(),
        content: payload.content.clone(),
        participants: payload.participants.clone(),
        timestamp: payload.timestamp,
        scope: payload.scope.clone(),
        fts_language: None,
        channel_id: payload.channel_id.clone(),
        channel_name: payload.channel_name.clone(),
        author_id: payload.author_id.clone(),
        author_name: payload.author_name.clone(),
        project: None,
        mcp_client_name: None,
        raw_content: None,
        duration_ms: None,
        segments: None,
        session_id: None,
        session_kind: None,
        session_title: None,
        repo_path: None,
        remote: None,
        hostname: None,
        harness_version: None,
        model: None,
        branch: None,
        agent_name: None,
        platform: None,
        task: None,
        parent_session_id: None,
        trusted_user_agent: None,
        client_info: None,
        code_path: None,
    };

    let namespace=format!("microsoft:connection:{}:{}",payload.account_id,payload.user_id);
    let origin=crate::source_origin::native_connector_origin("microsoft",&payload.source_type,"microsoft_graph_ingest")
        .expect("static origin identifiers");
    let resp=crate::ingest::submit(crate::ingest::IngestRequest {
        origin: origin.as_ref(),
        ..crate::ingest::IngestRequest::private(crate::ingest::Member {
            provider_namespace: Some(&namespace),
            ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
        })
    }).await;
    let succeeded = resp.is_ok();

    if succeeded {
        // Content-at-rest mitigation (see this module's doc comment): clear
        // the plaintext `content` before this `done` row starts its
        // indefinite retention. Every other field is kept — cheap, useful
        // for debugging, and carries no message content.
        let mut redacted = job.payload;
        if let Some(obj) = redacted.as_object_mut() {
            obj.insert(
                "content".to_string(),
                serde_json::Value::String("[redacted on completion]".to_string()),
            );
        }
        if let Err(e) =
            queue::complete_with_payload(&t.pool, job.id, queue::JobOutcome::DidWork, redacted)
                .await
        {
            tracing::error!(error = %e, job_id = %job.id, "failed to complete GraphMessageIngest job");
        }
    } else {
        let msg = format!(
            "graph ingest: the ingest door refused the message for account {} graph {}: {}",
            payload.account_id,
            payload.graph_id,
            resp.err().map(|e| e.to_string()).unwrap_or_default()
        );
        tracing::warn!(job_id = %job.id, %msg, "graph ingest worker: ingest failed");
        // Deliberately NOT scrubbed on this path — see this module's doc
        // comment: a dead-lettered job may need its body for diagnosis or a
        // hand-replay, so the redaction only ever happens on the success
        // path above.
        if let Err(qe) = queue::fail(&t.pool, job.id, &msg).await {
            tracing::error!(error = %qe, job_id = %job.id, "failed to mark job as failed after ingest error");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JobKind;

    #[test]
    fn graph_message_ingest_payload_roundtrips_through_job_kind() {
        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let kind = JobKind::GraphMessageIngest {
            account_id,
            user_id,
            graph_id,
            channel_name: Some("general".to_string()),
            source_type: "teams".to_string(),
            source_id: "msg-1".to_string(),
            content: "hello".to_string(),
            participants: vec!["u1".to_string()],
            timestamp: chrono::Utc::now(),
            scope: None,
            channel_id: Some("chan-1".to_string()),
            author_id: Some("u1".to_string()),
            author_name: Some("Alice".to_string()),
        };
        let tagged = serde_json::to_value(&kind).unwrap();
        assert_eq!(tagged["kind"], "GraphMessageIngest");
        let payload: GraphMessageIngestPayload =
            serde_json::from_value(tagged["payload"].clone()).unwrap();
        assert_eq!(payload.account_id, account_id);
        assert_eq!(payload.user_id, user_id);
        assert_eq!(payload.graph_id, graph_id);
        assert_eq!(payload.content, "hello");
    }

    /// The content-at-rest mitigation's redaction shape: `content` cleared,
    /// every other field preserved — mirrors `workers::slack_ingest`'s own
    /// identical test.
    #[test]
    fn redacted_payload_clears_content_but_keeps_other_fields() {
        let account_id = Uuid::new_v4();
        let graph_id = Uuid::new_v4();
        let original = serde_json::json!({
            "account_id": account_id,
            "user_id": Uuid::new_v4(),
            "graph_id": graph_id,
            "channel_name": "general",
            "source_type": "teams",
            "source_id": "msg-1",
            "content": "the secret message body",
            "participants": [],
            "timestamp": chrono::Utc::now(),
            "scope": null,
            "channel_id": "chan-1",
            "author_id": "u1",
            "author_name": "Alice",
        });

        let mut redacted = original.clone();
        if let Some(obj) = redacted.as_object_mut() {
            obj.insert(
                "content".to_string(),
                serde_json::Value::String("[redacted on completion]".to_string()),
            );
        }

        assert_ne!(redacted["content"], original["content"]);
        assert!(!redacted.to_string().contains("the secret message body"));
        assert_eq!(redacted["source_type"], original["source_type"]);
        assert_eq!(redacted["graph_id"], original["graph_id"]);
    }
}
