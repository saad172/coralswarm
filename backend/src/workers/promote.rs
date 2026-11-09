//! `workers::promote` — the backgrounded, batched tail of
//! `Store::promote_node_scoped` (async-promote PR).
//!
//! # Why this worker exists
//! `promote_node_scoped` used to copy a promoted reef's corals into the
//! target ocean and link them, per-atom, INSIDE the HTTP request that called
//! `POST /v1/nodes/{id}/promote` (and the MCP `promote` tool, which delegates
//! to that same route handler — see `mcp::tools::write::promote`'s own doc
//! comment). For a reef with a handful of attached corals that was fine. For
//! a reef with thousands, it was not: a production promotion of a
//! 3,129-coral reef performed on the order of 10,000+ sequential round trips
//! (`op_log`/`node_atoms` inserts, one at a time) inside ONE transaction and
//! never completed — every prior successful promotion in that same tenant
//! database had carried 1-2 corals, so this shape had simply never been
//! exercised at real scale before.
//!
//! `Store::promote_node_scoped` now publishes the target node synchronously
//! (an O(1) `INSERT ... ON CONFLICT`, never the part that scaled badly) and
//! enqueues a [`crate::jobs::JobKind::PromoteNode`] job, inside the SAME
//! transaction, to do everything else. This module is that job's worker.
//!
//! # Batching
//! Every DB write this worker performs is already batched at the `Store`
//! layer — `Store::copy_atoms_to_graph_scoped` (its own internal `op_log`
//! write is now `append_ops_tx`, a single multi-row `INSERT`, not one row
//! per atom) and `Store::link_node_atoms_batch_scoped`. This
//! worker's own job is to feed those in [`CHUNK_SIZE`]-sized batches rather
//! than to batch anything itself — see [`CHUNK_SIZE`]'s doc comment for why
//! a bound still exists even though each batch is O(1) round trips.
//!
//! # Idempotency and resumability
//! Every atom copy is `ON CONFLICT (graph_id, source_type, source_id) DO
//! NOTHING`; every `node_atoms` link is `ON CONFLICT DO NOTHING`. A retried
//! job (this worker's own `queue::fail` backoff ladder, or a worker restart
//! that leaves a `running` row for [`workers::reaper`] to reset — see
//! `jobs::queue`'s module doc on delivery guarantees) simply re-walks the
//! FULL `atom_ids` list from chunk zero: already-landed atoms cost one
//! no-op statement each, not a duplicate or an error. There is deliberately
//! no separate persisted watermark/cursor — the same accepted simplification
//! `bin/migrate_personal_to_member_ocean`'s atoms phase documents for the
//! identical shape, and for the same reason: a promotion's corals count in
//! the thousands, not the millions, so a full idempotent rescan on retry is
//! cheap relative to the state it would take to avoid one.
//!
//! # Progress reporting
//! [`jobs::queue::update_progress`] is called once up front (`0` /
//! `atom_ids.len()`, so a poller sees a real denominator immediately even
//! before the first chunk lands) and again after every committed chunk. See
//! that function's own doc comment for why this is a plain overwrite rather
//! than an increment.
//!
//! # RLS scope
//! Mirrors `workers::slack_name_backfill`'s (not `workers::embed`'s
//! `JobScope`) convention: every write runs inside an explicitly scoped
//! transaction on EITHER plane, rather than leaving the org-tenant arm
//! ambient. `job.account_id` (Phase W.1's stamp, set by
//! `Store::promote_node_scoped`'s `enqueue_tx` call from the TARGET ocean's
//! own `graphs.account_id`) is the account; `promoted_by` (carried in the
//! job's own payload, not re-derived) is the acting user for the org-tenant
//! arm's `begin_tenant_scoped`, matching `tenancy::scope::begin_tenant_scoped`'s
//! own doc comment ("prefer this with that owner" when a call site has a real
//! user in hand, which this one always does — the route/MCP tool that
//! enqueued this job had a gate-verified caller).

use std::time::Duration;

use coralswarm_core::r2::R2Client;
use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::jobs::JobKind;
use crate::store::{AtomBlobCopyCtx, BlobCopyMode, Store};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_scoped};
use crate::tenancy::TenantId;
use crate::workers::embed::UNSCOPED_JOB_ERR;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// How many `PromoteNode` jobs to dequeue per poll cycle. Small — unlike a
/// per-atom job (`ExtractEntities`), ONE `PromoteNode` job can already
/// represent thousands of atoms of work, so a high per-tick concurrency here
/// would not parallelise usefully, only hold open more scoped transactions
/// at once.
const JOB_BATCH_SIZE: usize = 2;

/// Atoms copied+linked per chunk (one scoped transaction, a small constant
/// number of batched statements each). Even though each chunk's statements
/// are already O(1) round trips regardless of chunk size, an unbounded
/// single chunk would mean:
///   * one `unnest`-bound array parameter list scaling with the WHOLE
///     promotion (a 50,000-coral reef would bind 50,000-element arrays —
///     still far cheaper than 50,000 round trips, but not free), and
///   * one scoped transaction held open for the whole job, which for a very
///     large promotion could itself run long enough to matter for pool
///     pressure and progress-poll granularity (a poller would see 0% then
///     100%, nothing between).
/// 500 mirrors `bin/migrate_personal_to_member_ocean`'s `ATOM_BATCH_SIZE`
/// (200) order of magnitude — chosen the same way: large enough that the
/// per-chunk overhead is negligible, small enough that progress updates
/// stay meaningfully granular and one chunk's transaction stays short.
const CHUNK_SIZE: usize = 500;

/// Idle poll interval when the queue is empty — same order of magnitude as
/// every other queue worker's `EMPTY_POLL_SLEEP` (`conflict_detection`,
/// `entity_resolution`, …).
pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// One promote-worker pass on one tenant's (pool, store) pair: dequeue up to
/// [`JOB_BATCH_SIZE`] `PromoteNode` jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant`, same shape as every other
/// converted queue worker.
///
/// `r2` is `None` only in a deployment with no R2 configured at all
/// (dev/local, `content_inline`-only ingest) — see [`process_job`]'s doc
/// comment for what that means for an R2-backed coral it encounters anyway.
pub async fn tick(t: &WorkerTenant, r2: Option<&R2Client>) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["PromoteNode"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        crate::jobs::publication::scope(job.id, process_job(&t.tenant, &t.pool, t.store.as_ref(), r2, job)).await;
    }
    Ok(Tick::worked(n))
}

/// The fields of [`JobKind::PromoteNode`], pulled out of a dequeued
/// [`JobRow`]. A free function (not a method) for the same reason
/// `workers::embed::parse_atom_ids` is: it needs no `&self` and is easiest
/// to unit-test standalone (see `mod tests` below).
fn parse_promote_payload(
    job: &JobRow,
) -> Result<(Uuid, Uuid, Uuid, Uuid, Vec<Uuid>), String> {
    let tagged = serde_json::json!({"kind": job.kind, "payload": job.payload});
    let kind: JobKind = serde_json::from_value(tagged)
        .map_err(|e| format!("cannot deserialise JobKind from payload: {e}"))?;

    match kind {
        JobKind::PromoteNode {
            target_node_id,
            target_graph_id,
            source_node_id,
            promoted_by,
            atom_ids,
        } => Ok((target_node_id, target_graph_id, source_node_id, promoted_by, atom_ids)),
        other => Err(format!("expected PromoteNode, got {other:?}")),
    }
}

/// Process one dequeued `PromoteNode` job: copy+link its atoms in
/// [`CHUNK_SIZE`]-sized chunks, re-uploading each R2-backed coral's content
/// under a freshly-computed, correctly org-scoped key (org-tenancy PR 2.4's
/// `blob_key_for`) BEFORE the chunk's transaction commits — see
/// `Store::copy_atoms_to_graph_scoped`'s doc comment (via
/// [`AtomBlobCopyCtx`]) for the crash-safety contract this satisfies. `r2 =
/// None` is only valid when this job's target account genuinely has no
/// R2-backed content to copy (dev/local); `copy_atoms_to_graph_scoped`
/// itself fails the chunk closed — never a silent contentless copy — if it
/// discovers otherwise.
async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn Store,
    r2: Option<&R2Client>,
    job: JobRow,
) {
    if !crate::selected_publication::legacy_whole_source_authorized() {
        let _=queue::could_not_do(pool,job.id,queue::FailureClass::Structural,"selected evidence consent required").await;
        return;
    }
    let (target_node_id, target_graph_id, source_node_id, promoted_by, atom_ids) =
        match parse_promote_payload(&job) {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::error!(job_id = %job.id, error = %e, "promote-worker: malformed payload");
                if let Err(qe) = queue::fail(pool, job.id, &e).await {
                    tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed promote job failed");
                }
                return;
            }
        };

    // ── Per-job RLS scope (org-tenancy Phase W.1/W.2 convention) ─────────────
    // Every `PromoteNode` job is stamped with an `account_id` by
    // `Store::promote_node_scoped`'s `enqueue_tx` call (the target ocean's
    // own `graphs.account_id`) — a job reaching this worker unscoped is an
    // upstream bug, never processed unscoped. Mirrors
    // `workers::verify_job_scope`'s two checks but resolves against the
    // TARGET graph (this job's writes are all target-ocean writes; the
    // source ocean is read-only here and was already reachability-checked
    // by the route before this job ever existed).
    let Some(account_id) = job.account_id else {
        tracing::error!(job_id = %job.id, kind = %job.kind, error = UNSCOPED_JOB_ERR, "refusing unscoped promote job");
        if let Err(qe) = queue::fail(pool, job.id, UNSCOPED_JOB_ERR).await {
            tracing::error!(error = %qe, job_id = %job.id, "failed to mark unscoped promote job as failed");
        }
        return;
    };

    if atom_ids.is_empty() {
        // Nothing to copy — `Store::promote_node_scoped` never enqueues this
        // job with an empty list (see that method's doc comment), but stay
        // defensive rather than assume: complete cleanly, no work done.
        if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await {
            tracing::error!(error = %e, job_id = %job.id, "promote-worker: failed to complete empty job");
        }
        return;
    }

    // ── R2 re-upload context ──────────────────────────────────────────────
    // Resolved ONCE per job, not per chunk: the DEK and org segment are
    // constant across every chunk this job processes, and a large promotion
    // can walk many. Same-pool promotion never crosses accounts (the
    // cross-DB case is served by the separate `services::promotion`
    // sibling), so the TARGET account's DEK (`account_id`, this job's own
    // scope) is also the SOURCE atoms' DEK.
    //
    // `r2` is only `None` in a deployment with no R2 at all (dev/local,
    // `content_inline`-only ingest) — see `Store::copy_atoms_to_graph_scoped`
    // for the fail-closed behavior if a chunk turns out to need this anyway.
    let dek: Option<[u8; 32]> = if r2.is_some() {
        match crate::crypto::resolve_dek_for_tenant(tenant, pool, account_id).await {
            Ok(Some(dek)) => Some(dek),
            Ok(None) => {
                let msg = format!(
                    "account {account_id} has no DEK — cannot re-upload R2-backed content"
                );
                tracing::error!(job_id = %job.id, %account_id, "promote-worker: {msg}");
                if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                    tracing::error!(error = %qe, job_id = %job.id, "failed to mark promote job failed");
                }
                return;
            }
            Err(e) => {
                let msg = format!("DEK resolution failed: {e}");
                tracing::error!(job_id = %job.id, %account_id, error = %msg, "promote-worker: DEK resolution failed — failing job");
                if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                    tracing::error!(error = %qe, job_id = %job.id, "failed to mark promote job failed");
                }
                return;
            }
        }
    } else {
        None
    };

    let org_id: Option<&str> = match tenant {
        TenantId::Consumer => None,
        TenantId::Org(org) => Some(org.as_str()),
    };

    let blob_ctx: Option<AtomBlobCopyCtx<'_>> = match (r2, dek.as_ref()) {
        (Some(r2), Some(dek)) => Some(AtomBlobCopyCtx {
            blobs: r2 as &dyn crate::services::blob_store::PromotionBlobStore,
            org_id,
            dek,
        }),
        _ => None,
    };
    let blob_mode = BlobCopyMode::from_ctx(blob_ctx.as_ref());

    let total = atom_ids.len() as i64;
    if let Err(e) = queue::update_progress(pool, job.id, 0, total).await {
        tracing::warn!(error = %e, job_id = %job.id, "promote-worker: failed to initialise progress (continuing)");
    }

    let mut landed: i64 = 0;
    for chunk in atom_ids.chunks(CHUNK_SIZE) {
        let tx = match tenant {
            TenantId::Consumer => begin_account_scoped(pool, account_id).await,
            TenantId::Org(_) => begin_tenant_scoped(pool, account_id, promoted_by).await,
        };
        let mut tx = match tx {
            Ok(tx) => tx,
            Err(e) => {
                let msg = format!("opening scoped transaction: {e}");
                tracing::error!(job_id = %job.id, %source_node_id, %target_graph_id, error = %msg, "promote-worker: DB error — failing job");
                if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                    tracing::error!(error = %qe, job_id = %job.id, "failed to mark promote job failed");
                }
                return;
            }
        };

        let result: Result<(), crate::store::StoreError> = async {
            let target_atom_ids = store
                .copy_atoms_to_graph_scoped(&mut tx, chunk, target_graph_id, blob_mode)
                .await?;
            store
                .link_node_atoms_batch_scoped(&mut tx, target_node_id, &target_atom_ids)
                .await?;
            tx.commit().await.map_err(crate::store::StoreError::from)?;
            Ok(())
        }
        .await;

        if let Err(e) = result {
            let msg = format!("chunk starting at offset {landed}: {e}");
            tracing::error!(job_id = %job.id, %source_node_id, %target_graph_id, error = %msg, "promote-worker: DB error — failing job");
            if let Err(qe) = queue::fail(pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark promote job failed");
            }
            // Deliberately NOT `return` after updating progress to `landed`
            // (already accurate from the last successful chunk) — the next
            // attempt (this job's own retry, idempotent per the module doc)
            // re-walks from chunk zero, so there is nothing further to do
            // here.
            return;
        }

        landed += chunk.len() as i64;
        if let Err(e) = queue::update_progress(pool, job.id, landed, total).await {
            tracing::warn!(error = %e, job_id = %job.id, landed, total, "promote-worker: failed to update progress (continuing)");
        }
    }

    if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::DidWork).await {
        tracing::error!(error = %e, job_id = %job.id, "promote-worker: failed to complete job");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job(kind: &JobKind, account_id: Option<Uuid>) -> JobRow {
        let tagged = serde_json::to_value(kind).unwrap();
        JobRow {
            id: Uuid::new_v4(),
            kind: tagged["kind"].as_str().unwrap().to_owned(),
            payload: tagged["payload"].clone(),
            attempts: 0,
            account_id,
        }
    }

    #[test]
    fn parse_promote_payload_roundtrips_every_field() {
        let target_node_id = Uuid::new_v4();
        let target_graph_id = Uuid::new_v4();
        let source_node_id = Uuid::new_v4();
        let promoted_by = Uuid::new_v4();
        let atom_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let kind = JobKind::PromoteNode {
            target_node_id,
            target_graph_id,
            source_node_id,
            promoted_by,
            atom_ids: atom_ids.clone(),
        };
        let job = sample_job(&kind, Some(Uuid::new_v4()));

        let parsed = parse_promote_payload(&job).expect("parses");
        assert_eq!(parsed.0, target_node_id);
        assert_eq!(parsed.1, target_graph_id);
        assert_eq!(parsed.2, source_node_id);
        assert_eq!(parsed.3, promoted_by);
        assert_eq!(parsed.4, atom_ids);
    }

    #[test]
    fn parse_promote_payload_rejects_the_wrong_kind() {
        let job = sample_job(&JobKind::RouteCopresence { session_id: Uuid::new_v4() }, Some(Uuid::new_v4()));
        let err = parse_promote_payload(&job).expect_err("must refuse a non-PromoteNode job");
        assert!(err.contains("expected PromoteNode"), "{err}");
    }

    /// Pure logic: chunking never drops or duplicates an atom id, and the
    /// last chunk is short rather than padded — pins the exact
    /// `atom_ids.chunks(CHUNK_SIZE)` behavior `process_job`'s loop relies on
    /// for its progress accounting (`landed += chunk.len()`) to sum to
    /// `total` at completion.
    #[test]
    fn chunking_covers_every_atom_exactly_once() {
        let atom_ids: Vec<Uuid> = (0..(CHUNK_SIZE * 2 + 7)).map(|_| Uuid::new_v4()).collect();
        let chunks: Vec<&[Uuid]> = atom_ids.chunks(CHUNK_SIZE).collect();
        assert_eq!(chunks.len(), 3, "two full chunks plus one short one");
        assert_eq!(chunks[0].len(), CHUNK_SIZE);
        assert_eq!(chunks[1].len(), CHUNK_SIZE);
        assert_eq!(chunks[2].len(), 7);
        let landed: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(landed, atom_ids.len());
    }
}
