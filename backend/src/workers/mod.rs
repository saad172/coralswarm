pub mod access_log_purge;
pub mod account_deletion;
pub mod atom_title_backfill;
pub mod auto_reload;
pub mod auto_add_drain_sweep;
pub mod billing_derivation;
pub mod billing_notices;
pub mod org_deletion;
pub mod org_selfserve_cleanup;
pub mod org_trials;
pub mod capture_admission_health;
pub mod chat_cold_start;
pub mod composio_capture;
pub mod content_tsv_backfill;
pub mod digest;
pub mod embed;
pub mod embed_retry;
pub mod statement_embed;
pub mod entity_resolution;
pub mod extract;
pub mod geo_backfill;
pub mod github_backfill;
pub mod github_event_ingest;
pub mod github_reconcile_sweep;
pub mod gmail_everything_mode;
pub mod google_chat_invite;
pub mod google_calendar_sync;
pub mod google_chat_sweep;
pub mod google_meet_sweep;
pub mod google_watch_renewal;
pub mod graph_ingest;
pub mod graph_subscriptions;
pub mod history_backfill;
pub mod huddle;
pub mod inference_closure;
pub mod ingest_receive;
pub mod jobs_retention;
pub mod journal_sweep;
pub mod library_evidence;
pub mod mcp_task_sweep;
pub mod meeting_atoms;
pub mod source_atoms;
pub mod member_connect_nudge;
pub mod member_ocean_reconciler;
pub mod microsoft_calendar_sync;
pub mod oauth_state_jti_sweep;
pub mod ocean_health;
pub mod pipeline_stage_health;
pub mod org_join_request_digest;
pub mod outlook_everything_mode;
pub mod platform_rollup;
pub mod promote;
pub mod propagate_corrections;
pub mod reaper;
pub mod reembed;
pub mod review_deadline;
pub mod slack_ingest;
pub mod slack_member_capture;
pub mod slack_name_backfill;
pub mod slack_seat_reconcile;
pub mod slack_unmatched_email;
pub mod slot_reservations;
pub mod summary;
pub mod teams_meeting_sync;
pub mod outlook_schedule;
pub mod teams_seat_reconcile;
pub mod teams_unmatched_email;
pub mod tenant_harness;
pub mod tenant_provisioning;
pub mod tombstone_purge;
pub mod usage_pending_sweep;
pub mod usage_reconciliation;
pub mod waitlist_sync;
pub mod wake_bridge;

// ── Shared per-job scoping guard (org-tenancy Phase W.2) ──────────────────────

use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::workers::embed::UNSCOPED_JOB_ERR;

/// The two fail-closed checks every graph-payload job worker runs BEFORE it
/// builds a [`crate::workers::embed::JobScope`], factored out of the three
/// workers that share them verbatim (`entity_resolution`, `infer_expertise`,
/// `inference_closure`). Returns the account to scope to, or `None` after
/// having already failed the job on the queue.
///
/// 1. **No `account_id`.** Post-W.1 every RLS-relevant job carries one
///    (`queue::dequeue` backfills or dead-letters NULL legacy rows), so `None`
///    can only be an upstream bug. The job is failed with [`UNSCOPED_JOB_ERR`]
///    rather than processed unscoped.
/// 2. **Cross-account payload.** 0084's SECURITY DEFINER
///    `resolve_graph_account_id` — the ONE sanctioned pre-scope lookup,
///    correct on either plane's pool — must map the payload's `graph_id` to
///    the job's stamped account. A job stamped for account A whose payload
///    names account B's graph is refused, which is effective even PRE-OPS-4
///    (where the BYPASSRLS owner role would otherwise happily serve B's rows).
///
/// A graph that resolves to NO account (deleted since enqueue) is deliberately
/// NOT an error: the caller's pass finds nothing and the job completes
/// cleanly, exactly the pre-conversion behaviour.
///
/// `worker` is the log/error prefix (e.g. `"entity-resolution"`).
pub(crate) async fn verify_job_scope(
    pool: &PgPool,
    job: &JobRow,
    graph_id: Uuid,
    worker: &str,
) -> Option<Uuid> {
    let Some(account_id) = job.account_id else {
        tracing::error!(job_id = %job.id, kind = %job.kind, worker, error = UNSCOPED_JOB_ERR, "refusing unscoped job");
        if let Err(qe) = queue::fail(pool, job.id, UNSCOPED_JOB_ERR).await {
            tracing::error!(error = %qe, job_id = %job.id, "failed to mark unscoped job as failed");
        }
        return None;
    };

    match sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_graph_account_id($1)")
        .bind(graph_id)
        .fetch_one(pool)
        .await
    {
        Ok(Some(owner)) if owner != account_id => {
            let msg = format!(
                "graph {graph_id} belongs to account {owner} but the job is stamped for account {account_id} — refusing cross-account processing"
            );
            tracing::error!(job_id = %job.id, worker, %msg, "cross-account payload refused");
            if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark cross-account job as failed");
            }
            None
        }
        Ok(_) => Some(account_id),
        Err(e) => {
            let msg = format!("resolve_graph_account_id({graph_id}) errored: {e}");
            tracing::error!(job_id = %job.id, worker, error = %msg, "ownership verification failed — failing job");
            if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job failed");
            }
            None
        }
    }
}

pub mod source_blob_cleanup;

pub mod read_model_refresh;
