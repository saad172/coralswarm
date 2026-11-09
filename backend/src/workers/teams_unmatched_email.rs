//! Teams invite worker (org-onboarding plan item 19).
//!
//! One job is one team's roster, read on a connected member's delegated
//! token. [`crate::connectors::microsoft::resolve_team_roster_members`]
//! returns the unmatched addresses; [`crate::connectors::microsoft::email_team_roster_resolution`]
//! emails the three audiences the connect-time walk already knows:
//! unknown address (connect), known user not in the org (join, with
//! `MembershipClass::Unavailable` so auto-add does not treat them as a Slack
//! full member), and an org member with no live Microsoft token (connect
//! nudge, shared cooldown with `member_connect`).
//!
//! There is no app-token fallback. A missing member token fails the job.
//! The worker never reads channel or chat message bodies — only the roster
//! the resolver already walks — so one member's credential is not used to
//! copy another member's messages.
//!
//! The connect callback still calls `email_unmatched_joined_teams`. This
//! worker is the later walk: that callback also enqueues one job per joined
//! team, and `workers::teams_seat_reconcile` enqueues again after a seat
//! sweep. The per-recipient cooldown makes the second pass a no-op inside
//! the window.

use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::{queue, JobKind};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

const JOB_BATCH_SIZE: usize = 5;
pub const EMPTY_POLL_SLEEP: std::time::Duration = std::time::Duration::from_secs(30);

async fn process_job(
    state: &Arc<AppState>,
    tenant: &TenantId,
    control_pool: &PgPool,
    content_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    tenant_id: &str,
    team_id: &str,
) -> Result<usize, String> {
    if team_id.is_empty() {
        return Err("teams invite job missing team_id".into());
    }
    let dek = crate::crypto::DekPlane::from_tenant(tenant, content_pool.clone(), account_id);
    let resolution = crate::graph_rate_limit::scoped(
        crate::graph_rate_limit::rate_key(account_id, tenant_id),
        crate::connectors::microsoft::resolve_team_roster_members(
            control_pool,
            content_pool,
            http,
            &dek,
            account_id,
            tenant_id,
            team_id,
        ),
    )
    .await?;
    Ok(crate::connectors::microsoft::email_team_roster_resolution(
        state,
        control_pool,
        account_id,
        team_id,
        &resolution,
    )
    .await)
}

pub async fn tick(
    t: &WorkerTenant,
    state: &Arc<AppState>,
    control_pool: &PgPool,
    http: &reqwest::Client,
) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["EmailUnmatchedTeamsMembers"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
        let (account_id, tenant_id, team_id) = match serde_json::from_value::<JobKind>(tagged) {
            Ok(JobKind::EmailUnmatchedTeamsMembers {
                account_id,
                tenant_id,
                team_id,
            }) => (account_id, tenant_id, team_id),
            Ok(_) => {
                tracing::error!(job_id = %job.id, "teams-unmatched-email: wrong job kind dequeued");
                queue::fail(&t.pool, job.id, "wrong job kind").await.ok();
                continue;
            }
            Err(e) => {
                tracing::error!(job_id = %job.id, error = %e, "teams-unmatched-email: malformed payload");
                queue::fail(&t.pool, job.id, &format!("malformed payload: {e}"))
                    .await
                    .ok();
                continue;
            }
        };

        match process_job(
            state,
            &t.tenant,
            control_pool,
            &t.pool,
            http,
            account_id,
            &tenant_id,
            &team_id,
        )
        .await
        {
            Ok(sent) => {
                let outcome = if sent == 0 {
                    queue::JobOutcome::NothingToDo
                } else {
                    queue::JobOutcome::DidWork
                };
                if let Err(e) = queue::complete(&t.pool, job.id, outcome).await {
                    tracing::error!(error = %e, job_id = %job.id, "teams-unmatched-email: complete failed");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, job_id = %job.id, "teams-unmatched-email: job failed");
                queue::fail(&t.pool, job.id, &e).await.ok();
            }
        }
    }
    Ok(Tick::worked(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invite_job_round_trips_without_a_slack_provider() {
        let job = JobKind::EmailUnmatchedTeamsMembers {
            account_id: Uuid::nil(),
            tenant_id: String::new(),
            team_id: "team-1".into(),
        };
        let tagged = serde_json::to_value(&job).unwrap();
        assert_eq!(tagged["kind"], "EmailUnmatchedTeamsMembers");
        assert_eq!(tagged["payload"]["team_id"], "team-1");
        assert!(tagged["payload"].get("provider").is_none());
        let back: JobKind = serde_json::from_value(tagged).unwrap();
        match back {
            JobKind::EmailUnmatchedTeamsMembers {
                tenant_id, team_id, ..
            } => {
                assert!(tenant_id.is_empty(), "a blank home tenant is a valid job");
                assert_eq!(team_id, "team-1");
            }
            other => panic!("unexpected kind: {other:?}"),
        }
    }
}
