//! Shared plumbing for member-ocean provisioning
//! (`docs/plans/member-private-oceans.md` §2.4 "Provisioning — three
//! triggers, order-independent"), used by the two REQUEST-adjacent triggers
//! (member-private-oceans PR 4):
//!
//! 1. **The Clerk webhook** — `routes::clerk_webhook::handle_membership_upsert`
//!    (`organizationMembership.created|.updated`), best-effort, never fatal
//!    to the webhook.
//! 2. **First-sight on the request path** — `tenancy::runtime::TenantRuntime::
//!    finish_binding`, guarded by a process-local TTL cache so it costs one
//!    statement per (org, user) per process per TTL window, not one per
//!    request.
//!
//! Trigger 3, the reconciler sweep (`workers::member_ocean_reconciler`, PR 3),
//! does **not** call [`provision_one`]: it provisions an org's whole roster in
//! one multi-row `INSERT ... SELECT ... FROM UNNEST(...)` per tick, which is
//! the right shape for "every member of this org" but the wrong shape for
//! "this one member I have in hand" — inventing a one-element roster just to
//! share a code path would trade a clearer call site for no real
//! deduplication. What IS shared, to avoid re-deriving (and risking a drift
//! in) the two properties that must stay identical across all three triggers:
//!
//! * [`FALLBACK_OCEAN_NAME`] and [`ocean_name`] — the same generic name a
//!   member ocean gets when no display name is on file, reused directly from
//!   `workers::member_ocean_reconciler` rather than a second definition that
//!   could silently diverge.
//! * [`member_oceans_enabled`] — ditto, for the `MEMBER_OCEANS_ENABLED`
//!   fail-closed flag contract.
//! * The `ON CONFLICT` target itself: `uq_graphs_one_primary_per_owner`
//!   (`(account_id, owner_user_id) WHERE is_primary AND owner_user_id IS NOT
//!   NULL AND archived_at IS NULL`) — **not** `uq_graphs_one_member_ocean_per_owner`,
//!   which never shipped past an early plan draft (see the reconciler's own
//!   doc comment for the full history: secondary, non-primary member oceans
//!   are allowed by design). Because all three triggers target this exact
//!   partial unique index with `DO NOTHING`, any two (or three) of them
//!   racing to provision the SAME member's primary ocean — a webhook and a
//!   first-sight request, two backend processes' first-sight requests, a
//!   first-sight request and a reconciler tick — collide on it at the
//!   database level and exactly one wins. No app-level locking or
//!   coordination between the triggers is needed or attempted.
//!
//! `tenant_pool` in [`provision_one`] MUST be the org's own fingerprint-
//! verified tenant pool, never the control/consumer pool — member oceans are
//! an org-tenant-only concept and `kind='member'` must never exist on the
//! consumer plane (plan §6 P0 #7). Both call sites obtain it from
//! `TenantRegistry::get_or_build`, which is itself the reason a "tenant pool
//! won't build" is a normal, non-fatal outcome here: a cold build can fail on
//! a transient connect error or a tenant mid-migration, and the reconciler
//! sweep (which polls only `active`, already-fingerprinted tenants) is the
//! backstop that converges regardless.

use sqlx::PgPool;
use uuid::Uuid;

use crate::store::Store;
use crate::workers::member_ocean_reconciler::{member_oceans_enabled, FALLBACK_OCEAN_NAME};

/// Whether member-ocean provisioning is turned on — see this module's doc
/// comment for why this delegates to the reconciler's own flag check rather
/// than re-parsing `MEMBER_OCEANS_ENABLED` a third time. Checked fresh on
/// every call (never cached), so flipping the flag takes effect on the very
/// next webhook delivery / first-sight request with no redeploy, matching
/// the reconciler's own per-tick check.
pub(crate) fn enabled() -> bool {
    member_oceans_enabled()
}

/// The ocean name to provision with: the caller's own display name when
/// present and non-blank, else [`FALLBACK_OCEAN_NAME`] — so a member's ocean
/// is named identically no matter which of the three triggers happens to win
/// the race for it.
fn ocean_name(display_name: Option<&str>) -> String {
    display_name
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .unwrap_or(FALLBACK_OCEAN_NAME)
        .to_string()
}

/// Race-safe, idempotent single-member provision-or-restore attempt (member-
/// private-oceans PR 7, plan §2.6 "Restore"). The same `RETURNING id` shape
/// as `workers::member_ocean_reconciler::tick`'s batch statements (see this
/// module's doc comment for why that file keeps its own batch form rather
/// than calling this).
///
/// **Restore-before-provision, in ONE statement.** A member rejoining within
/// the 60-day window must get back their EXACT archived ocean (plan §2.6:
/// "restore ... returns the same graph id with content intact"), not a fresh
/// empty one. Before PR 7, this function only ever inserted — which meant
/// whichever of the three provisioning triggers (this webhook/first-sight
/// helper, or the reconciler sweep) reached a rejoining member FIRST decided
/// the outcome: `uq_graphs_one_primary_per_owner`'s `ON CONFLICT` target only
/// blocks a fresh insert when a NON-archived primary already exists, so an
/// archived-but-in-window ocean never stopped this INSERT from succeeding.
/// In practice the reconciler sweep (every [`crate::workers::
/// member_ocean_reconciler::RECONCILE_INTERVAL`], 5 minutes) essentially
/// never wins that race against a webhook delivery or the very next
/// authenticated request after rejoin — so restore, as specified, would
/// almost never actually fire via these two triggers. Folding the restore
/// predicate into this same statement (as a preceding CTE the INSERT's `WHERE
/// NOT EXISTS` depends on, forcing it to run first within the one round
/// trip) closes that race structurally rather than by scheduling luck: ANY of
/// the three triggers that reaches this row first now restores it, and only
/// falls back to a fresh insert when no eligible archived ocean exists.
///
/// **At most one archived ocean is restored per owner.** Plan §2.6/§2.1: an
/// owner may have accumulated more than one archived `kind='member'` row
/// (e.g. a secondary, non-primary member ocean created while active, then
/// swept into `archived_at` alongside the primary by the reconciler's archive
/// arm at offboarding — see that arm's doc comment). Restoring ALL of them as
/// `is_primary = true` simultaneously would violate
/// `uq_graphs_one_primary_per_owner` outright. This picks exactly one
/// candidate — ordered by `archived_at DESC, created_at DESC`, i.e. the most
/// recently archived (and, tie-broken, most recently created) row, which is
/// the ocean most likely to have been the member's primary — and leaves any
/// other archived ocean for that owner untouched (it stays archived until a
/// future purge worker, PR 8, reaps it past the 60-day window; no data is
/// lost, it is simply not the one this rejoin resurrects).
///
/// **`org_revocations` is checked by the CALLERS, not this function.** This
/// function itself has no `control_pool` and so cannot check
/// `org_revocations` (a control-plane-only table) directly. Both call sites
/// are covered, by two different mechanisms:
/// * `routes::clerk_webhook::provision_member_ocean_best_effort` (the
///   webhook trigger) calls [`crate::tenancy::routing::org_is_revoked`]
///   itself, before ever reaching this function — a Clerk webhook delivery
///   carries no upstream fence of its own, so this had to be added there
///   explicitly (fixed alongside this restore change: restoring content for
///   a revoked-but-not-yet-torn-down org is a strictly worse outcome than
///   provisioning an empty one would have been).
/// * `runtime::TenantRuntime::maybe_provision_member_ocean` (first-sight)
///   needs no such check: it only ever runs after `finish_binding` has a live
///   `TenantContext`, which `middleware::auth::live_org_membership`'s
///   `account_members` re-check already refuses to produce for a revoked
///   org's member (see that function's own doc comment) — `finish_binding`,
///   and therefore this function, is structurally unreachable for a revoked
///   org via that path.
///
/// **Also not audited from this call site.** `member_ocean_lifecycle_log`
/// rows for a `'restore'` action are written only by the reconciler's own
/// restore arm (which has `control_pool` in hand already); a restore that
/// happens here (webhook or first-sight) is logged via `tracing` only by the
/// caller, not persisted to the control-plane audit trail. Documented as a
/// residual gap in the PR description rather than silently claiming complete
/// audit coverage.
///
/// Returns:
/// * `Ok(Some(graph_id))` — this call won the race and either restored an
///   eligible archived ocean or provisioned a new primary member ocean.
///   Callers cannot distinguish which from the return value alone (both are
///   "you now have a primary member ocean, id `graph_id`").
/// * `Ok(None))` — a primary member ocean for this `(account_id,
///   owner_user_id)` already existed and nothing was eligible to restore
///   (this call lost the race to another trigger, or simply found the
///   steady state). Both are success, not error, from the caller's point of
///   view — callers must not distinguish them as pass/fail.
/// * `Err(_)` — the statement itself failed (connection error, etc.).
///   Callers in this codebase treat this as best-effort: log and move on,
///   never fail the request/webhook it rode in on (the reconciler sweep is
///   the convergence backstop for whatever this attempt missed).
/// `control_pool` (identity-resolution-at-the-door §10.3): after this call
/// wins the race and provisions/restores the member's own ocean, it's used
/// to bind a self reef on the TENANT plane for that ocean's owner — org
/// members are exactly the "org members on first authenticated ingest" case
/// §10.3 calls out, and a member's private ocean is otherwise the SAME class
/// of "owner discovers themselves as a stranger" gap the primary personal
/// ocean has (§1's evidence). `users` lives on the control plane only (no
/// `users` table exists in an org's own tenant database — see
/// `docs/coralswarm-database-architecture`'s decisions), so `users.name`/
/// `email`/`clerk_user_id` are looked up HERE, on `control_pool`, and only
/// the already-resolved values cross onto `tenant_pool` — see
/// [`crate::store::bind_principal_identifiers_values`]'s doc for why the
/// values-based entry point exists specifically for this cross-plane shape.
/// Best-effort and never fails this function's own `Result`: this runs on
/// the request hot path (`tenancy::runtime::TenantRuntime::
/// maybe_provision_member_ocean`), and a self-reef binding failure must
/// never turn a successful ocean provision into a request-path error.
pub(crate) async fn provision_one(
    tenant_pool: &PgPool,
    control_pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    display_name: Option<&str>,
) -> Result<Option<Uuid>, sqlx::Error> {
    let graph_id = provision_graph(tenant_pool, account_id, user_id, display_name).await?;

    if let Some(gid) = graph_id {
        bind_member_self_reef_best_effort(tenant_pool, control_pool, gid, user_id).await;
    }

    Ok(graph_id)
}

/// See [`provision_one`]'s doc comment for the cross-plane rationale. Every
/// failure here is logged and swallowed — the ocean itself is already
/// provisioned by the time this runs, and losing the self reef is strictly
/// better than losing the ocean.
async fn bind_member_self_reef_best_effort(
    tenant_pool: &PgPool,
    control_pool: &PgPool,
    graph_id: Uuid,
    user_id: Uuid,
) {
    let user: Option<(Option<String>, String, Option<String>)> = match sqlx::query_as(
        "SELECT name, email, clerk_user_id FROM users WHERE id = $1",
    )
    .bind(user_id)
    .fetch_optional(crate::tenancy::scope::request_read(control_pool))
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::warn!(error = %e, %graph_id, %user_id, "member-ocean self reef: control-plane users lookup failed (non-fatal)");
            return;
        }
    };
    let Some((name, email, clerk_user_id)) = user else {
        return;
    };

    let mut tx = match tenant_pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, %graph_id, %user_id, "member-ocean self reef: tenant tx begin failed (non-fatal)");
            return;
        }
    };

    let (title, needs_name) =
        crate::store::resolve_principal_title_scoped(&mut tx, graph_id, name.as_deref(), &email).await;

    let store = crate::store::PostgresStore::new(tenant_pool.clone());
    let bind_result: Result<(), String> = async {
        let (node_id, _self_title) = store
            .ensure_self_node_scoped(&mut tx, graph_id, user_id, &title)
            .await
            .map_err(|e| e.to_string())?;
        if needs_name {
            crate::store::door_stamp_needs_name(&mut tx, node_id)
                .await
                .map_err(|e| e.to_string())?;
        }
        crate::store::bind_principal_identifiers_values(
            &mut tx,
            graph_id,
            node_id,
            clerk_user_id.as_deref(),
            Some(&email),
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }
    .await;

    match bind_result {
        Ok(()) => {
            if let Err(e) = tx.commit().await {
                tracing::warn!(error = %e, %graph_id, %user_id, "member-ocean self reef: commit failed (non-fatal)");
            }
        }
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %graph_id, %user_id, "member-ocean self reef: binding failed (non-fatal)");
        }
    }
}

/// First authenticated Teams enrollment: provision only the verified member's
/// ocean under the same RLS actor as capture. Legacy bootstrap callers retain
/// their existing entry point; this one never borrows a system-wide actor.
pub(crate) async fn provision_member_scoped(
    tenant_pool: &PgPool, control_pool: &PgPool, account_id: Uuid, user_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    let mut tx=crate::tenancy::scope::begin_tenant_scoped(tenant_pool,account_id,user_id)
        .await.map_err(|e|sqlx::Error::Protocol(e.to_string()))?;
    let graph_id=provision_graph(&mut *tx,account_id,user_id,None).await?;
    tx.commit().await?;
    if let Some(graph_id)=graph_id {
        bind_member_self_reef_best_effort(tenant_pool,control_pool,graph_id,user_id).await;
    }
    Ok(graph_id)
}
async fn provision_graph<'e, E: sqlx::Executor<'e, Database=sqlx::Postgres>>(
    executor:E, account_id:Uuid, user_id:Uuid, display_name:Option<&str>,
) -> Result<Option<Uuid>,sqlx::Error> {
    let name = ocean_name(display_name);
    let graph_id: Option<Uuid> = sqlx::query_scalar::<_, Uuid>(
        "WITH restore_candidate AS ( \
             SELECT id FROM graphs \
             WHERE account_id = $1 AND owner_user_id = $3 AND kind = 'member' \
               AND archived_at IS NOT NULL \
               AND archived_at > now() - interval '60 days' \
               AND NOT EXISTS ( \
                     SELECT 1 FROM graphs g2 \
                     WHERE g2.account_id = $1 AND g2.owner_user_id = $3 \
                       AND g2.kind = 'member' AND g2.archived_at IS NULL) \
             ORDER BY archived_at DESC, created_at DESC \
             LIMIT 1 \
         ), \
         restored AS ( \
             UPDATE graphs g SET archived_at = NULL, is_primary = true \
             WHERE g.id = (SELECT id FROM restore_candidate) \
               AND g.archived_at IS NOT NULL \
               AND NOT EXISTS ( \
                     SELECT 1 FROM graphs g2 \
                     WHERE g2.account_id = g.account_id AND g2.owner_user_id = g.owner_user_id \
                       AND g2.kind = 'member' AND g2.archived_at IS NULL) \
             RETURNING g.id \
         ), \
         provisioned AS ( \
             INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             SELECT $1, $2, 'member', $3, true \
             WHERE NOT EXISTS (SELECT 1 FROM restored) \
             ON CONFLICT (account_id, owner_user_id) \
               WHERE is_primary AND owner_user_id IS NOT NULL AND archived_at IS NULL \
             DO NOTHING \
             RETURNING id \
         ) \
         SELECT id FROM restored \
         UNION ALL \
         SELECT id FROM provisioned",
    )
    .bind(account_id)
    .bind(&name)
    .bind(user_id)
    .fetch_optional(executor)
    .await?;

    Ok(graph_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ocean_name_falls_back_when_blank_or_missing() {
        for case in [None, Some(""), Some("   ")] {
            assert_eq!(ocean_name(case), FALLBACK_OCEAN_NAME, "case: {case:?}");
        }
    }

    #[test]
    fn ocean_name_uses_real_name_verbatim() {
        assert_eq!(ocean_name(Some("Jane Smith")), "Jane Smith");
    }

    #[test]
    fn ocean_name_trims_whitespace() {
        assert_eq!(ocean_name(Some("  Jane Smith  ")), "Jane Smith");
    }
}
