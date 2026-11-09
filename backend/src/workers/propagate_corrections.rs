//! Polls for `PropagateCorrections` jobs and runs `services::corrections::
//! propagate` — the durable, restart-safe replacement for `routes::meetings`'
//! former bare `tokio::spawn` (CodeRabbit round 2 on the PR that introduced
//! `services::corrections`, finding 1). An untracked spawn is lost on process
//! restart with nothing left to re-trigger it: a correction could commit
//! `applied` while its atoms were never rewritten, permanently — only some
//! LATER, unrelated correction event on the same source would ever call
//! `propagate` again. Enqueuing a job instead, in the SAME transaction as the
//! correction write (`jobs::queue::enqueue_tx`, from
//! `routes::meetings::create_speaker_relabel` / `set_correction_status`),
//! makes "the correction is durable" and "the propagation work is durably
//! queued" the same atomic fact — exactly the discipline every other
//! content-mutation-adjacent job in this tree already has (e.g. `EmbedAtoms`
//! after `routes::ingest`'s atom insert).
//!
//! # Consumer-plane only
//!
//! Mirrors `workers::reembed`'s own "not tenant-dispatched" decision:
//! `services::corrections::propagate` itself takes a bare `&PgPool` and has
//! never been tenant-aware (no org-tenant DB dispatch, no `WorkerTenant`).
//! `crypto::resolve_dek` (uncached, direct) is used for the same reason
//! `workers::embed`'s own doc comment gives for itself: "the embed worker,
//! which lacks `AppState` access, should continue to call `resolve_dek`
//! directly."
//!
//! # Concurrency (CodeRabbit round 2, finding 3)
//!
//! `propagate()` holds up to two Postgres connections at once for the
//! duration of ONE call: a dedicated connection for its per-`(account,
//! source)` `pg_advisory_xact_lock` (held across the whole call, including
//! every per-atom R2 round trip — see that function's own doc comment), plus
//! one short-lived scoped transaction per rewritten atom. This worker
//! processes its dequeued batch STRICTLY SEQUENTIALLY — a plain `for` loop
//! over [`run`]'s jobs, never spawned or `join_all`'d — so at most ONE
//! `propagate()` call is ever in flight per worker process. The actual
//! concurrent connection usage this mechanism can produce is therefore
//! bounded by `2 × (number of worker replicas)` — a small, known number
//! tracking Fly instance count, not by the number of concurrent HTTP
//! requests that used to trigger propagation directly before this PR (which
//! was the unbounded quantity CodeRabbit's finding was about).

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_core::r2::R2Client;

use crate::jobs::queue::{self, JobRow};
use crate::jobs::JobKind;
use crate::services::blob_store::PromotionBlobStore;
use crate::services::corrections::propagate;
use crate::shutdown::Shutdown;
use crate::store::CLOUD_DEVICE_ID;

/// Jobs dequeued per poll cycle. Purely a DB-round-trip batching knob — see
/// the module doc's concurrency section for why this does NOT change how
/// many `propagate()` calls run concurrently (always at most one, per
/// worker process, regardless of this constant).
const JOB_BATCH_SIZE: usize = 5;

/// How long to sleep between polls when the queue is empty.
const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// How long to sleep after a DB error before retrying.
const DB_ERROR_SLEEP: Duration = Duration::from_secs(5);

/// Run the correction-propagation worker loop, until `shutdown` is signalled.
///
/// Same shutdown contract as `workers::reembed`/`workers::embed`: stop
/// claiming new jobs (checked at the top of every loop iteration) but let a
/// job already claimed this iteration finish.
pub async fn run(pool: PgPool, r2: Option<Arc<R2Client>>, shutdown: Shutdown) {
    tracing::info!("corrections propagation worker started");

    loop {
        if shutdown.is_triggered() {
            tracing::info!(
                "corrections propagation worker: shutdown signalled, stopping (no new jobs claimed)"
            );
            return;
        }

        let jobs = match queue::dequeue(&pool, &["PropagateCorrections"], JOB_BATCH_SIZE).await {
            Ok(j) => j,
            Err(e) => {
                tracing::error!(error = %e, "corrections propagation worker: dequeue failed, sleeping");
                tokio::time::sleep(DB_ERROR_SLEEP).await;
                continue;
            }
        };

        if jobs.is_empty() {
            tokio::time::sleep(EMPTY_POLL_SLEEP).await;
            continue;
        }

        // Sequential, deliberately — see the module doc's concurrency
        // section. Never `futures::future::join_all`'d.
        for job in jobs {
            process_job(&pool, r2.as_deref(), job).await;
        }
    }
}

fn parse_payload(job: &JobRow) -> Result<(Uuid, String), String> {
    let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
    let kind: JobKind = serde_json::from_value(tagged).map_err(|e| e.to_string())?;
    match kind {
        JobKind::PropagateCorrections {
            graph_id,
            source_id,
        } => Ok((graph_id, source_id)),
        other => Err(format!("expected PropagateCorrections, got {other:?}")),
    }
}

/// `pub` (matching `workers::reembed::process_job` / `workers::summary::
/// process_job`) so integration tests can drive one dequeued job end-to-end
/// against a real Postgres without going through the poll loop.
pub async fn process_job(pool: &PgPool, r2: Option<&R2Client>, job: JobRow) {
    let (graph_id, source_id) = match parse_payload(&job) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "corrections propagation worker: malformed job payload");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // Every enqueue site (`routes::meetings`) stamps `account_id` at insert
    // time via `jobs::queue::enqueue_tx` — a `None` here is an upstream bug,
    // not a legitimate unscoped-sweep shape this job kind ever has.
    let Some(account_id) = job.account_id else {
        let msg = "corrections propagation job missing account_id — refusing unscoped processing";
        tracing::error!(job_id = %job.id, %graph_id, %source_id, msg);
        if let Err(qe) = queue::fail(pool, job.id, msg).await {
            tracing::error!(error = %qe, job_id = %job.id, "failed to mark unscoped job as failed");
        }
        return;
    };

    let dek = match crate::crypto::resolve_dek(pool, account_id).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                job_id = %job.id,
                error = %e,
                "corrections propagation worker: DEK resolution failed; R2-backed atoms will be \
                 skipped this pass"
            );
            None
        }
    };
    let blobs = r2.map(|r2| r2 as &dyn PromotionBlobStore);

    match propagate(pool, blobs, dek, CLOUD_DEVICE_ID, graph_id, &source_id).await {
        Ok(outcome) => {
            if !outcome.rewritten_atoms.is_empty() {
                tracing::info!(
                    job_id = %job.id,
                    rewritten = outcome.rewritten_atoms.len(),
                    invalidated_reefs = outcome.invalidated_reef_ids.len(),
                    embed_job_enqueued = outcome.embed_job_enqueued,
                    "corrections propagation worker: propagated correction to atoms/embeddings/reefs"
                );
            }
            // `NothingToDo` is a legitimate outcome (module doc: no
            // currently-`applied` correction for this source at the time
            // this job actually ran — e.g. it was reverted again before the
            // job was claimed), not a failure.
            let job_outcome = if outcome.rewritten_atoms.is_empty() {
                queue::JobOutcome::NothingToDo
            } else {
                queue::JobOutcome::DidWork
            };
            if let Err(e) = queue::complete(pool, job.id, job_outcome).await {
                tracing::error!(job_id = %job.id, error = %e, "corrections propagation worker: failed to complete job");
            }
        }
        Err(e) => {
            tracing::error!(
                job_id = %job.id,
                error = %e,
                "corrections propagation worker: propagate() failed — retrying via bounded attempts"
            );
            if let Err(qe) = queue::fail(pool, job.id, &e.to_string()).await {
                tracing::error!(error = %qe, job_id = %job.id, "corrections propagation worker: failed to mark job failed");
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn job_row(payload: serde_json::Value, account_id: Option<Uuid>) -> JobRow {
        JobRow {
            id: Uuid::new_v4(),
            kind: "PropagateCorrections".to_string(),
            payload,
            attempts: 0,
            account_id,
        }
    }

    #[test]
    fn parse_payload_extracts_graph_id_and_source_id() {
        let graph_id = Uuid::new_v4();
        let job = job_row(
            serde_json::json!({ "graph_id": graph_id, "source_id": "meeting:123" }),
            Some(Uuid::new_v4()),
        );
        let (g, s) = parse_payload(&job).expect("valid payload parses");
        assert_eq!(g, graph_id);
        assert_eq!(s, "meeting:123");
    }

    #[test]
    fn parse_payload_rejects_a_different_job_kind() {
        let atom_id = Uuid::new_v4();
        let mut job = job_row(
            serde_json::json!({ "atom_id": atom_id }),
            Some(Uuid::new_v4()),
        );
        job.kind = "ExtractEntities".to_string();
        assert!(parse_payload(&job).is_err());
    }

    #[test]
    fn parse_payload_rejects_malformed_payload() {
        let job = job_row(
            serde_json::json!({ "not_graph_id": "x" }),
            Some(Uuid::new_v4()),
        );
        assert!(parse_payload(&job).is_err());
    }
}
