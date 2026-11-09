//! Review-deadline sweep (high-signal review, Stage 1 PR 3).
//!
//! The zero-debt half of the redesign: the review queue is an OFFER, never a
//! debt. Every queued clarification carries a deadline
//! ([`crate::review_policy::REVIEW_DEADLINE_DAYS`], 7 days from
//! `relationship_edge.queued_at` / `body.pending_since`); once it lapses this
//! worker applies the machine's best guess:
//!
//!   * **Edges** — `confidence >= DEADLINE_LEAN_CONFIRM (0.70)` → confirmed
//!     with `auto_confirmed + auto_resolved` provenance (machine-revisable; a
//!     later human touch upgrades to durable). Below → blanked: the verdict
//!     lands in `evaluation_ledger` and the row leaves with a delete op. The
//!     lean threshold equals the traversability floor (pinned by test in
//!     `review_policy`), so a deadline-confirm can only widen traversal
//!     within the band a human was being asked about. `rejected` is NEVER
//!     machine-written.
//!   * **Decisions** — a pending (conflict-flipped) decision reef flips back
//!     to confirmed with `auto_resolved` stamps; synthesis renders the
//!     `[SUPERSEDED]` annotation either way, so nothing is lost.
//!
//! Every store-side write is a guarded single UPDATE/DELETE on the
//! still-queued state, so a human racing the sweep collapses to
//! first-commit-wins (`store::deadline_sweep_edges_in_tx`).
//!
//! ## Consumer-plane RLS scoping
//!
//! Same loop-per-account shape as `workers::tombstone_purge` (see its module
//! doc for the OPS-4 rationale): enumerate `accounts`, one
//! `begin_account_scoped` transaction per account calling the `_scoped`
//! sweep.
//!
//! ## Tenant-plane RLS scoping (Phase 2, the ninth slice)
//!
//! The org arm used to call `deadline_sweep_relationship_edges` and
//! `deadline_sweep_pending_decisions` straight on the pooled worker handle,
//! on the reasoning that org tenant DBs are whole-database isolated and so
//! need no scope. That is true of the CROSS-ORG boundary and false of the
//! boundary INSIDE a tenant DB. `relationship_edge` carries
//! `migrations_tenant/0009_tenant_rls`'s `member_ocean_isolation` policy, and
//! `nodes` — which the decisions half updates — carries one too.
//! `workers::tombstone_purge` carried the same sentence and PR #843 deleted
//! it there for the same reason.
//!
//! What the unscoped call did at OPS-4-T was not "nothing". Both policies
//! resolve through `tenant_rls_visible_graph_ids()`, whose conjuncts are
//! `(g.kind <> 'member' OR g.owner_user_id = tenant_rls_actor_user_id())` and
//! `(g.kind <> 'team' OR EXISTS (… tm.user_id = tenant_rls_actor_user_id()))`.
//! With no GUC set `tenant_rls_actor_user_id()` is NULL, so a member graph's
//! first conjunct is NULL and a team graph's `EXISTS` is false, while both
//! are trivially true for an `org` or a `personal` graph. The hourly sweep
//! would therefore have resolved the org and personal oceans and silently
//! passed over every member and team ocean: nothing confirmed, nothing
//! blanked, `Tick::idle`, no error. An expired review in a private or a team
//! ocean would never resolve, and the queue would grow without a symptom.
//! Both GUCs are `set_config(.., true)` and so transaction-local; a statement
//! on a pooled `&PgPool` can carry neither.
//!
//! The org arm now opens
//! `tenancy::scope::begin_tenant_system_scoped_without_account` and calls the
//! same two `_scoped` twins the consumer arm calls. An hourly sweep has no
//! acting user, so the reviewed service principal is the honest primitive.
//! Both sweeps WRITE (`UPDATE`/`DELETE` on `relationship_edge`, `UPDATE` on
//! `nodes`, plus the `evaluation_ledger` verdict rows), so the transaction
//! commits. One transaction covers both calls, which is exactly what
//! [`consumer_tick`] already does per account — the pair is one deadline
//! sweep and its two halves are committed together on both planes.

use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use crate::review_policy::{DEADLINE_LEAN_CONFIRM, REVIEW_DEADLINE_DAYS};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// Hourly: a 7-day deadline needs no finer resolution, and hourly keeps the
/// idle-tenant backoff arithmetic comfortable. `pub(crate)`: fed to the
/// tenant harness as this worker's sweep cadence.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Per-call DB timeout — a hung sweep degrades to a logged skip, never a
/// parked harness (same posture as `tombstone_purge`).
const SWEEP_TIMEOUT: Duration = Duration::from_secs(60);

/// Edge batch bound per tenant per tick. Well above any realistic post-gate
/// queue; purely a runaway backstop, and the hourly cadence drains any
/// remainder.
const EDGE_BATCH: i64 = 500;

/// One deadline sweep on one tenant.
pub async fn tick(t: &WorkerTenant) -> Result<Tick, String> {
    let cutoff = Utc::now() - chrono::Duration::days(REVIEW_DEADLINE_DAYS);
    match &t.tenant {
        // Org tenant plane: one system-scoped transaction around both halves
        // of the sweep, then commit — see the module doc's "Tenant-plane RLS
        // scoping" section. Both halves WRITE.
        //
        // KNOWN GAP, stated rather than implied. A scope that fails to open
        // produces `Err` inside `sweep`, which the match below logs at WARN
        // and then turns into `Ok(Tick::idle())`. To the tick accounting that
        // is indistinguishable from a cycle with nothing to resolve. So a
        // permanently failing scope reads as a quiet, healthy sweep, and only
        // the WARN line says otherwise. Returning `Err` here instead would
        // surface it as a failed tick, but that changes sweep semantics for
        // every caller and belongs in its own change, not in this conversion.
        // What this conversion fixes is narrower and real: the sweep no longer
        // silently skips member and team oceans while reporting a clean pass.
        TenantId::Org(_) => {
            let sweep = async {
                let mut tx = begin_tenant_system_scoped_without_account(&t.pool)
                    .await
                    .map_err(|e| format!("failed to open tenant system scope: {e}"))?;
                let (c, b) = t
                    .store
                    .deadline_sweep_relationship_edges_scoped(&mut tx, cutoff, DEADLINE_LEAN_CONFIRM, EDGE_BATCH)
                    .await
                    .map_err(|e| e.to_string())?;
                let d = t
                    .store
                    .deadline_sweep_pending_decisions_scoped(&mut tx, cutoff)
                    .await
                    .map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())?;
                Ok::<(u64, u64, u64), String>((c, b, d))
            };
            match tokio::time::timeout(SWEEP_TIMEOUT, sweep).await {
                Err(_) => {
                    tracing::warn!(
                        timeout_secs = SWEEP_TIMEOUT.as_secs(),
                        "review-deadline sweep timed out; will retry next cycle"
                    );
                    Ok(Tick::idle())
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "review-deadline sweep failed; will retry next cycle");
                    Ok(Tick::idle())
                }
                Ok(Ok((0, 0, 0))) => Ok(Tick::idle()),
                Ok(Ok((confirmed, blanked, decisions))) => {
                    tracing::info!(
                        confirmed, blanked, decisions, %cutoff,
                        "review-deadline sweep resolved expired items"
                    );
                    Ok(Tick::worked((confirmed + blanked + decisions) as usize))
                }
            }
        }
        TenantId::Consumer => consumer_tick(t, cutoff).await,
    }
}

/// Consumer-plane sweep: one scoped transaction per account; a single
/// account's failure is logged and skipped, the rest still sweep this cycle.
async fn consumer_tick(t: &WorkerTenant, cutoff: chrono::DateTime<Utc>) -> Result<Tick, String> {
    let account_ids: Vec<Uuid> = match tokio::time::timeout(
        SWEEP_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts ORDER BY id").fetch_all(&t.pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                timeout_secs = SWEEP_TIMEOUT.as_secs(),
                "review-deadline sweep: account enumeration timed out; will retry next cycle"
            );
            return Ok(Tick::idle());
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "review-deadline sweep: account enumeration failed; will retry next cycle");
            return Ok(Tick::idle());
        }
        Ok(Ok(ids)) => ids,
    };

    let mut total = 0u64;
    for account_id in account_ids {
        let sweep_one = async {
            let mut tx = begin_account_scoped(&t.pool, account_id)
                .await
                .map_err(|e| e.to_string())?;
            let (c, b) = t
                .store
                .deadline_sweep_relationship_edges_scoped(&mut tx, cutoff, DEADLINE_LEAN_CONFIRM, EDGE_BATCH)
                .await
                .map_err(|e| e.to_string())?;
            let d = t
                .store
                .deadline_sweep_pending_decisions_scoped(&mut tx, cutoff)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok::<u64, String>(c + b + d)
        };
        match tokio::time::timeout(SWEEP_TIMEOUT, sweep_one).await {
            Err(_) => {
                tracing::warn!(
                    %account_id,
                    timeout_secs = SWEEP_TIMEOUT.as_secs(),
                    "review-deadline sweep timed out for account; will retry next cycle"
                );
            }
            Ok(Err(e)) => {
                tracing::warn!(%account_id, error = %e, "review-deadline sweep failed for account; will retry next cycle");
            }
            Ok(Ok(n)) => total += n,
        }
    }

    if total > 0 {
        tracing::info!(resolved = total, %cutoff, "review-deadline sweep complete");
        Ok(Tick::worked(total as usize))
    } else {
        Ok(Tick::idle())
    }
}
