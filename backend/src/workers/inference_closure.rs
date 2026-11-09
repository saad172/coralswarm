//! Inference-closure worker (ADR-0008, Phase 2).
//!
//! Per `InferRelationships { graph_id }` job: load the graph's **confirmed**
//! edges, run the deterministic composition pass ([`crate::relationships::inference`]),
//! and persist the derived edges with a confidence gate:
//!
//!   * `confidence >= AUTO_CONFIRM_CONFIDENCE` → written `Confirmed` with
//!     `provenance.auto_confirmed = true` (structural entailments like
//!     parent∘parent⇒grandparent).
//!   * `SOFT_FLOOR <= confidence < AUTO_CONFIRM_CONFIDENCE` → written
//!     `Suggested` (defeasible guesses like wife⇒mother — surface in the
//!     review queue / hedged answers).
//!   * `confidence < SOFT_FLOOR` → dropped (too speculative to surface).
//!
//! `AUTO_CONFIRM_CONFIDENCE` ([`crate::relationships::AUTO_CONFIRM_CONFIDENCE`])
//! is shared with the other edge producers so they never drift on what counts
//! as "confident enough to skip review". This pass makes no model call, so it
//! runs without a managed purpose (`managed_history::automatic_purpose`).
//!
//! Candidates whose `(src, predicate, dst)` already exists in any state are
//! skipped, so stated edges are never clobbered and rejected edges never return.
//!
//! ## Per-job RLS scope (org-tenancy Phase W.2)
//!
//! `relationship_edge` is on migration `0083_consumer_rls`'s policy roster, so
//! every `Store` call below runs under the job's [`JobScope`]: on the consumer
//! plane each statement gets a fresh short `tenancy::scope::begin_account_scoped`
//! transaction keyed by `job.account_id` (Phase W.1's per-job scoping input);
//! on an org tenant each gets a `begin_tenant_system_scoped_without_account`
//! transaction instead. Whole-database isolation does NOT stand in for that
//! scope. The isolation is real and covers the CROSS-ORG boundary only, while
//! `migrations_tenant/0009_tenant_rls` puts a member-ocean policy on
//! `relationship_edge` inside a tenant DB too. Unscoped, once OPS-4-T moves
//! the tenant DSN to the NOBYPASSRLS role, closure would run over the org and
//! personal oceans and see no edge in any member or team ocean — inferring
//! nothing there, and reporting a normal pass. Per statement rather
//! than per job, for the reason spelled out on [`JobScope`] itself.
//!
//! A job whose payload `graph_id` resolves (via 0084's SECURITY DEFINER
//! `resolve_graph_account_id`) to a DIFFERENT account than the job's stamp is
//! refused before any edge query runs — the same cross-account payload guard
//! `workers::infer_expertise` and `workers::entity_resolution` apply. A job of
//! this kind with NO `account_id` is failed with
//! [`UNSCOPED_JOB_ERR`](crate::workers::embed::UNSCOPED_JOB_ERR) rather than
//! processed unscoped (unreachable post-W.1; a defence against upstream bugs).

use std::collections::HashSet;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::relationships::inference::{self, EdgeInput};
use crate::relationships::{
    EdgeOrigin, EdgeStatus, RelationshipEdge, AUTO_CONFIRM_CONFIDENCE,
};
use crate::store::{Store, StoreError};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::embed::JobScope;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::workers::verify_job_scope;

const JOB_BATCH_SIZE: usize = 4;
/// Idle poll interval when the queue is empty. `pub(crate)`: fed to the
/// tenant harness as this worker's `base_poll` (see `workers::tenant_harness`).
pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// Below this, the inference is too speculative to surface at all.
const SOFT_FLOOR: f32 = 0.5;

/// One inference-closure pass on one tenant's (pool, store) pair: dequeue up
/// to [`JOB_BATCH_SIZE`] `InferRelationships` jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` (org-tenancy PR 2.1), which
/// owns the loop, sleeps, shutdown, and per-org idle backoff. No
/// control-table reads: jobs/relationship_edge are tenant-tree tables.
pub async fn tick(t: &WorkerTenant) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["InferRelationships"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(&t.tenant, &t.pool, t.store.as_ref(), job).await;
    }
    Ok(Tick::worked(n))
}

/// `pub` (org-tenancy Phase W.2) so integration tests can drive ONE dequeued
/// job end-to-end against a real Postgres — including the scoping guards —
/// without going through the worker loop.
pub async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    store: &dyn crate::store::Store,
    job: JobRow,
) {
    let graph_id = match job
        .payload
        .get("graph_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Uuid>().ok())
    {
        Some(id) => id,
        None => {
            tracing::error!(job_id = %job.id, "inference-closure: malformed payload (missing graph_id)");
            if let Err(e) = queue::fail(pool, job.id, "malformed payload: missing graph_id").await {
                tracing::error!(error = %e, job_id = %job.id, "failed to mark malformed job failed");
            }
            return;
        }
    };

    // ── Per-job RLS scope (org-tenancy Phase W.2) — see the module doc's RLS
    // section and `workers::verify_job_scope`. ────────────────────────────────
    let Some(account_id) = verify_job_scope(pool, &job, graph_id, "inference-closure").await
    else {
        return;
    };
    let scope = JobScope::for_job(tenant, pool, account_id);

    let result = async {
        let n = run_closure(&scope, store, graph_id).await?;
        let r = refresh_co_occurrence(&scope, graph_id).await?;
        if r.changed() > 0 {
            tracing::info!(job_id = %job.id, %graph_id, inserted = r.inserted, updated = r.updated,
                deleted = r.deleted, desired = r.desired, "inference-closure: refreshed co-occurrence edges");
        }
        Ok::<_, String>(n + r.changed())
    }
    .await;
    match result {
        Ok(n) => {
            let job_outcome = if n > 0 {
                tracing::info!(job_id = %job.id, %graph_id, written = n, "inference-closure: materialized inferred edges");
                queue::JobOutcome::DidWork
            } else {
                queue::JobOutcome::NothingToDo
            };
            if let Err(e) = queue::complete(pool, job.id, job_outcome).await {
                tracing::error!(error = %e, job_id = %job.id, "inference-closure: failed to complete job");
            }
        }
        Err(e) => {
            tracing::error!(job_id = %job.id, %graph_id, error = %e, "inference-closure: DB error");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job failed");
            }
        }
    }
}

/// Returns the number of inferred edges written.
///
/// Every `relationship_edge` statement runs under `scope` (org-tenancy Phase
/// W.2): one short `begin_account_scoped` transaction per statement on the
/// consumer plane, one short `begin_tenant_system_scoped_without_account`
/// transaction per statement on an org tenant. Neither is ambient.
async fn run_closure(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
) -> Result<usize, String> {
    // Compose only CONFIRMED edges (stated + user-confirmed) so we never build
    // inferences on top of unreviewed guesses.
    let confirmed = scoped_edges_by_status(scope, store, graph_id, EdgeStatus::Confirmed)
        .await
        .map_err(|e| e.to_string())?;

    if confirmed.len() < 2 {
        return Ok(0);
    }

    // Existing (src,predicate,dst) across ALL states — never clobber a stated
    // edge, never resurrect a rejected one, never duplicate a suggestion.
    let mut existing: HashSet<(Uuid, String, Uuid)> = HashSet::new();
    for status in [EdgeStatus::Confirmed, EdgeStatus::Suggested, EdgeStatus::Rejected] {
        let edges = scoped_edges_by_status(scope, store, graph_id, status)
            .await
            .map_err(|e| e.to_string())?;
        for e in edges {
            existing.insert((e.src_node_id, e.predicate, e.dst_node_id));
        }
    }

    let inputs: Vec<EdgeInput> = confirmed
        .iter()
        .map(|e| EdgeInput {
            edge_id: e.edge_id,
            src: e.src_node_id,
            predicate: e.predicate.clone(),
            dst: e.dst_node_id,
            confidence: e.confidence,
        })
        .collect();

    let mut written = 0usize;
    for c in inference::infer(&inputs) {
        let key = (c.src, c.predicate.to_string(), c.dst);
        if existing.contains(&key) {
            continue;
        }
        let auto_confirmed = c.confidence >= AUTO_CONFIRM_CONFIDENCE;
        let status = if auto_confirmed {
            EdgeStatus::Confirmed
        } else if c.confidence >= SOFT_FLOOR {
            EdgeStatus::Suggested
        } else {
            continue; // too speculative to surface
        };

        // ── High-signal review gate ───────────────────────────────────────────
        // A Suggested composition only queues when an endpoint is structurally
        // consequential; otherwise the graph stays blank and the ledger anchors
        // the verdict. Unlike the expertise pass there is no LLM cost here —
        // the ledger row is audit + churn suppression, and the `existing`-key
        // skip above already prevents re-writes. On an impact-fetch error the
        // candidate is deferred (no write, no ledger), never blanked blind.
        //
        // This no longer mirrors `infer_expertise`, which now gates on its
        // subject (the topic) because its src is always a person hub. A
        // composed edge has no endpoint that is a hub BY CONSTRUCTION, so the
        // symmetric OR stays right here — but it inherits the OR's weakness:
        // any rule whose output happens to land on a hub queues on the hub's
        // degree alone. If closure items start flooding `/review`, that is the
        // cause, and `review_policy::subject_high_impact` is the fix. Same
        // caveat applies to the `max()` below.
        let mut queued_impact: Option<serde_json::Value> = None;
        if status == EdgeStatus::Suggested {
            let impacts = match scoped_node_impact(scope, store, graph_id, &[c.src, c.dst]).await {
                Ok(i) => i,
                Err(e) => {
                    tracing::warn!(predicate = %c.predicate, error = %e, "inference-closure: impact fetch failed — deferring candidate");
                    continue;
                }
            };
            if !crate::review_policy::any_high_impact(&impacts) {
                if let Err(e) =
                    scoped_upsert_ledger(scope, store, graph_id, c.src, c.predicate, c.confidence).await
                {
                    tracing::warn!(predicate = %c.predicate, error = %e, "inference-closure: evaluation-ledger upsert failed");
                }
                continue;
            }
            queued_impact = Some(serde_json::json!({
                "degree": impacts.iter().map(|i| i.degree).max().unwrap_or(0),
                "corals": impacts.iter().map(|i| i.coral_count).max().unwrap_or(0),
                "contradiction": impacts.iter().any(|i| i.conflict_involved),
            }));
        }

        let mut provenance = serde_json::json!({
            "rule": c.rule,
            "source_edge_ids": c.source_edge_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            "defeasible": c.defeasible,
        });
        if auto_confirmed {
            provenance["auto_confirmed"] = serde_json::Value::Bool(true);
        }
        if let Some(impact) = queued_impact {
            provenance["impact"] = impact;
        }

        if let Err(e) = scoped_upsert_relationship_edge(
            scope,
            store,
            graph_id,
            c.src,
            c.predicate,
            c.dst,
            status,
            c.confidence,
            provenance,
        )
        .await
        {
            tracing::warn!(predicate = %c.predicate, error = %e, "inference-closure: upsert inferred edge failed");
            continue;
        }
        existing.insert(key);
        written += 1;
    }

    Ok(written)
}

/// [`crate::co_occurrence::refresh_tx`] in one transaction under the job's
/// scope; the refresh reads the whole graph, so it cannot be split per
/// statement without converging on a moving input.
async fn refresh_co_occurrence(scope: &JobScope<'_>, graph_id: Uuid) -> Result<crate::co_occurrence::Refresh, String> {
    let mut tx = match scope {
        JobScope::Consumer { pool, account_id } => begin_account_scoped(pool, *account_id).await,
        JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool).await,
    }
    .map_err(|e| e.to_string())?;
    let out = crate::co_occurrence::refresh_tx(&mut tx, graph_id, crate::co_occurrence::Write::Apply)
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(out)
}

// ── Scope-aware Store wrappers (org-tenancy Phase W.2) ────────────────────────
//
// Each wrapper runs ONE of this worker's RLS-covered `Store` calls under the
// job's [`JobScope`]: consumer plane ⇒ the `_scoped` variant inside a fresh
// `begin_account_scoped` transaction; org tenant ⇒ the `_scoped` variant
// inside a fresh `begin_tenant_system_scoped_without_account` transaction.
// NEITHER arm is ambient — a new wrapper that runs the plain method on the
// org arm re-opens the gap this module's doc describes. Per statement rather
// than per job — see [`JobScope`]'s doc.

/// Scope-dispatched [`Store::node_structural_impact`].
async fn scoped_node_impact(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<crate::review_policy::NodeImpact>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.node_structural_impact_scoped(&mut tx, graph_id, node_ids).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.node_structural_impact_scoped(&mut tx, graph_id, node_ids).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// Scope-dispatched [`Store::upsert_evaluation_ledger`].
async fn scoped_upsert_ledger(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    subject: Uuid,
    predicate: &str,
    confidence: f32,
) -> Result<(), StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            store
                .upsert_evaluation_ledger_scoped(&mut tx, graph_id, subject, predicate, Some(confidence))
                .await?;
            tx.commit().await?;
            Ok(())
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let _ = store.upsert_evaluation_ledger_scoped(&mut tx, graph_id, subject, predicate, Some(confidence)).await?;
            tx.commit().await?;
            Ok(())
        }
    }
}

async fn scoped_edges_by_status(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    status: EdgeStatus,
) -> Result<Vec<RelationshipEdge>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out =
                store.list_relationship_edges_by_status_scoped(&mut tx, graph_id, status).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.list_relationship_edges_by_status_scoped(&mut tx, graph_id, status).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn scoped_upsert_relationship_edge(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    src: Uuid,
    predicate: &str,
    dst: Uuid,
    status: EdgeStatus,
    confidence: f32,
    provenance: serde_json::Value,
) -> Result<(), StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            store
                .upsert_relationship_edge_scoped(
                    &mut tx,
                    graph_id,
                    src,
                    predicate,
                    dst,
                    EdgeOrigin::Inferred,
                    status,
                    confidence,
                    provenance,
                )
                .await?;
            tx.commit().await?;
            Ok(())
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let _ = store.upsert_relationship_edge_scoped( &mut tx, graph_id, src, predicate, dst, EdgeOrigin::Inferred, status, confidence, provenance, ).await?;
            tx.commit().await?;
            Ok(())
        }
    }
}
