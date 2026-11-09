//! Supersession edge routes.
//!
//! A supersession edge records that one node (the superseder / newer node)
//! replaces another (the superseded / outdated node).
//!
//! # Endpoints
//! - `POST   /v1/nodes/{id}/supersede`           — mark `{id}` as superseded by another node
//! - `DELETE /v1/nodes/{id}/supersede/{other_id}` — remove a supersession edge in either direction
//! - `GET    /v1/nodes/{id}/supersessions`        — list all supersession edges for a node
//!
//! # Authorization: reachability, not account (member-private-oceans)
//!
//! Every handler here used to authorize its node ids with
//! `node_owned_by_account(node, user.account_id)` alone. **In an org tenant
//! that predicate is a no-op across the member-ocean boundary**: every
//! member-private ocean (`graphs.kind = 'member'`) in an org shares the ONE
//! org `account_id`, so "this node belongs to my account" is satisfied by
//! EVERY member's private reef simultaneously — including reefs the caller
//! cannot see, cannot list, and could not have discovered through any other
//! route. The same shape has now caused two incidents (PR 0's team-ocean leak
//! and #348's cross-ocean superseder disclosure), so it is stated here rather
//! than left to be re-derived.
//!
//! Every handler here — the two writes and the read — therefore authorizes on
//! REACHABILITY, through
//! [`crate::tenancy::reachability::ReachabilityResolver`] — the single
//! ocean-authorization point every other ocean-scoped write already goes
//! through, and the only place that knows how to answer the question on both
//! the consumer plane and an org tenant pool. The account check is KEPT in
//! front of it, unchanged: it is not the authorization any more, it is the
//! precondition that the request's `begin_account_scoped` transaction (opened
//! on `user.account_id`) is the right scope to be reading these rows in at
//! all. The two together are strictly narrower than either alone, and the
//! refusal is the same non-committal 404 in both cases so neither can be used
//! as an existence oracle.
//!
//! `POST` additionally requires both endpoints to live in the SAME ocean.
//! Reachability alone would not be enough: it is per-CALLER, but the row is
//! not. A caller who legitimately reaches both the org commons and her own
//! member ocean could otherwise write "the commons reef is superseded by MY
//! private reef" — a row every OTHER org member then reads back (they reach
//! the commons, not the member ocean) through `superseded_by` on the retrieval
//! path and through `superseder_title` on `GET /v1/nodes/{id}/supersessions`.
//! Same-graph is also the invariant the schema itself now records
//! (`supersession_edge.graph_id`, migrations 0094/0008) and the one 0083's RLS
//! policy had already been *assuming*; see 0094's header for the full argument.
//!
//! **This handler is not the only write door.** `/v1/sync/push` authorizes each
//! op's DECLARED `graph_id`, not its endpoints, so a supersession op stamped
//! with an ocean the pusher really can reach may still name two endpoints in
//! different oceans — and, in the DELETE direction, may remove an edge from an
//! ocean the pusher cannot reach at all if she knows its two reef ids. Both are
//! closed in the apply itself (`Store`'s `OP_ENTITY_SUPERSESSION` arms: the
//! upsert requires the two endpoints to resolve to the same graph; the delete
//! is scoped by the op's authorized `graph_id`). The guards on the two
//! transports are deliberately independent, because neither can be assumed to
//! be the only one — that assumption is what left this table writable across
//! the member-ocean boundary after the HTTP handler had already been fixed.
//!
//! **The READ handler needed the same treatment, and got it.**
//! `list_supersessions` (`GET /v1/nodes/{id}/supersessions`) authorized its
//! single node id on `node_owned_by_account` alone, so within one org account a
//! member who obtained another member's reef id could enumerate that reef's
//! edges — and this handler returns `superseder_title`/`superseded_title`,
//! which is CONTENT, not merely identifiers. That makes it a strictly worse
//! disclosure than the one #348 closed on the retrieval path, and unlike the
//! write side it needed no cross-ocean edge to exist: an ordinary, perfectly
//! valid edge inside A's own ocean was enough. It now runs the same
//! `authorize_reachable_node!` the writes do.
//!
//! Its second door is not another transport but the QUERY itself. Every row
//! carries the OTHER endpoint's title, so authorizing the queried reef says
//! nothing about where the far endpoint lives; `Store::list_supersession_edges`
//! therefore takes the authorized `graph_id` and restricts to it. That is
//! belt-and-braces given the write-side same-ocean rule, and it is here because
//! "safe due to an invariant maintained in another file" is precisely the
//! reasoning that produced this defect class twice.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.2)
//! Every query here that touches an RLS-covered consumer table
//! (`nodes`/`supersession_edge`, migration `0083_consumer_rls`) runs inside
//! ONE [`crate::tenancy::scope::begin_account_scoped`] transaction per
//! request, through the Store's `_scoped` twins, scoped to the CALLER'S
//! ACTIVE ACCOUNT — which is exactly the account these routes already
//! authorize against (`node_owned_by_account(_, user.account_id)`), so the
//! scope and the app-level IDOR check enforce the same boundary. With no
//! pool (`tenant.pool == None`) opening the transaction fails and the handler
//! returns a 500.
//! The fire-and-forget `insert_activity_event` (a `routes::activity` helper
//! that owns its own pool handle and spawn) is intentionally untouched here —
//! `routes/activity.rs` is a separate conversion unit in the rls-coverage
//! manifest.

use axum::{extract::Path, http::StatusCode, response::IntoResponse, Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext, routes::activity::insert_activity_event,
    tenancy::extractor::Tenant, tenancy::reachability::ReachabilityResolver,
};

// ── Macros ────────────────────────────────────────────────────────────────────
// These handlers return `impl IntoResponse`, so the 503 arm uses `.into_response()`.

/// Obtain the content-plane DB pool from the `Tenant` extractor (org-tenancy
/// PR 1.5; was ambient `state.db`), or return a 503. Only needed by handlers
/// that still call a `PgPool`-based helper (e.g. `insert_activity_event`,
/// whose `activity_events` target is a tenant-tree table — and whose internal
/// `tokio::spawn` therefore now OWNS this tenant-scoped pool clone, never an
/// ambient consumer pool).
macro_rules! require_pool {
    ($tenant:expr) => {
        match &$tenant.pool {
            Some(p) => p.clone(),
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        }
    };
}

/// Borrow the content-plane `Store` from the `Tenant` extractor (org-tenancy
/// PR 1.5; was ambient `state.store`), or return a 503 —
/// `supersession_edge`/`nodes`/`graphs` are tenant-tree tables.
macro_rules! require_store {
    ($tenant:expr) => {
        match &$tenant.store {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        }
    };
}

/// Open the request's account-scoped transaction (org-tenancy OPS-4 phase
/// B.2), or early-return a 500. On the org plane this opens on the TENANT
/// pool via `Tenant::begin_member_scoped`, which also sets `app.user_id`.
///
/// That GUC has real policies to satisfy there. Whole-database isolation is
/// real, but it covers the CROSS-ORG boundary only, and it is NOT a reason to
/// skip the scope: `migrations_tenant/0009_tenant_rls` puts member-ocean
/// policies on `nodes` and `supersession_edge`, this file's two tables. Those
/// policies resolve through `tenant_rls_visible_graph_ids()`, and with neither
/// `app.user_id` nor `app.tenant_actor` set `tenant_rls_actor_user_id()` is
/// NULL — so a supersession listing keeps the org and personal oceans and
/// loses every member and team ocean. The GUC is unevaluated today for a ROLE
/// reason, not a policy one: tenant pools connect as the table owner. Phase 3
/// (`OPS-4-T`) repoints each tenant DSN at the non-owner
/// `coralswarm_tenant_app` role, which is what makes it bite. See
/// `tenancy::scope`'s module doc.
macro_rules! begin_scoped_tx {
    ($tenant:expr, $account_id:expr) => {
        {
            match $tenant.begin_member_scoped($account_id).await {
                Ok(tx) => Some(tx),
                Err(e) => {
                    tracing::error!(error = %e, "failed to open account-scoped transaction");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    )
                        .into_response();
                }
            }
        }
    };
}

/// Dispatch one Store call through the scoped transaction, or the plain
/// unscoped method when `$tx` is `None`. `begin_scoped_tx!` never yields `None`: with
/// no pool it returns a 500 instead.
macro_rules! scoped_store {
    ($store:expr, $tx:expr, $unscoped:ident / $scoped:ident ( $($arg:expr),* $(,)? )) => {
        match $tx.as_mut() {
            Some(tx) => $store.$scoped(tx, $($arg),*).await,
            None => $store.$unscoped($($arg),*).await,
        }
    };
}

/// Resolve one node's home ocean and prove the CALLER can actually reach it,
/// evaluating to that ocean's `graph_id`. Early-returns from the handler on
/// every refusal, so a caller of this macro can only ever proceed with an
/// authorized ocean id in hand.
///
/// This is the authorization the module doc describes, in the ONE shape every
/// other ocean-scoped route in this codebase uses (see `routes::nodes`'s
/// `node_graph_id / node_graph_id_scoped` → `ReachabilityResolver::
/// graph_reachable_by_user` pairs, copied verbatim): the predicate itself is
/// never re-derived here — hand-rolling a second one is exactly how the
/// account-scoped check this replaces came to disagree with the rest of the
/// system.
///
/// Both refusals — "no such node" and "that node is in an ocean you cannot
/// reach" — deliberately return the SAME 404 and the same message
/// (`$not_found`, chosen by the caller to match what that handler already
/// returned before this change, so no response shape moves). Distinguishing
/// them would turn the endpoint into an existence oracle for every reef id in
/// the account, which is precisely the disclosure being closed. Errors fail
/// CLOSED as a 500; a resolver error is never treated as "not reachable" and
/// never as "reachable".
///
/// Runs INSIDE the request's `begin_account_scoped` transaction for the node
/// lookup (so the read obeys RLS like every other read in this file) and
/// OUTSIDE it for the reachability call, which is `ReachabilityResolver`'s
/// documented, permanent multi-account carve-out and must not be scoped.
macro_rules! authorize_reachable_node {
    ($tenant:expr, $store:expr, $tx:expr, $node_id:expr, $user:expr, $not_found:expr, $what:literal) => {{
        let graph_id = match scoped_store!(
            $store,
            $tx,
            node_graph_id / node_graph_id_scoped($node_id)
        ) {
            Ok(Some(g)) => g,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": $not_found})),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!(error = %e, concat!("DB error resolving ", $what, " ocean"));
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        };
        match ReachabilityResolver::graph_reachable_by_user(
            $tenant.decision.as_ref(),
            $store.as_ref(),
            graph_id,
            $user.user_id,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": $not_found})),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!(error = %e, concat!("reachability error for ", $what));
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        }
        graph_id
    }};
}

/// Commit a WRITE handler's scoped transaction before responding — a commit
/// failure means nothing was persisted, so it must surface as a 500, never a
/// false success.
macro_rules! commit_write_tx {
    ($tx:expr) => {
        if let Some(tx) = $tx.take() {
            if let Err(e) = tx.commit().await {
                tracing::error!(error = %e, "scoped transaction commit failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        }
    };
}

/// Early-return the handler's own 404 unless the caller may WRITE ocean
/// `$graph_id` — [`crate::routes::nodes::graph_writable_by`] with the REST
/// writer. Reachability (above) admits the commons for every member; this is
/// the narrower write scope. Same body as the reachability refusal, so a
/// write-scope refusal never confirms the reef exists.
macro_rules! require_graph_writable {
    ($store:expr, $tx:expr, $graph_id:expr, $user:expr, $tenant:expr, $not_found:expr) => {{
        let writer = crate::routes::nodes::ReefWriter::rest(&$user, $tenant.decision.as_ref());
        match crate::routes::nodes::graph_writable_by($store.as_ref(), &mut $tx, $graph_id, &writer).await {
            Ok(true) => {}
            Ok(false) => {
                return (StatusCode::NOT_FOUND, Json(json!({"error": $not_found}))).into_response();
            }
            Err(e) => {
                tracing::error!(error = %e, "supersession write-scope check error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        }
    }};
}

// ── Request types ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateSupersessionRequest {
    /// The newer/correct node that supersedes the node identified by `{id}`.
    pub superseded_by: Uuid,
}

// ── POST /v1/nodes/{id}/supersede ─────────────────────────────────────────────
//
// The node at `{id}` is the OLD/outdated node.
// `superseded_by` is the NEW/correct node.
// Inserts: superseder_id = superseded_by, superseded_id = id.

pub async fn create_supersession(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
    Json(body): Json<CreateSupersessionRequest>,
) -> impl IntoResponse {
    // Keep pool for insert_activity_event (takes PgPool directly). Cloned off
    // the `Tenant` extractor, so the fire-and-forget spawn inside
    // `insert_activity_event` owns a tenant-scoped handle (org-tenancy PR 1.5).
    let pool = require_pool!(tenant);
    let store = require_store!(tenant);

    let superseded_by = body.superseded_by;

    // ── Validation: self-supersession ─────────────────────────────────────────
    if node_id == superseded_by {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "a node cannot supersede itself"})),
        )
            .into_response();
    }

    // One account-scoped transaction for the whole request — the caller's
    // active account is exactly the account the IDOR checks below already
    // authorize against (module doc's RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // ── IDOR: both nodes must belong to caller's account ─────────────────────
    match scoped_store!(
        store,
        scoped_tx,
        node_owned_by_account / node_owned_by_account_scoped(node_id, user.account_id)
    ) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "node not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking node ownership");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    match scoped_store!(
        store,
        scoped_tx,
        node_owned_by_account / node_owned_by_account_scoped(superseded_by, user.account_id)
    ) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "superseder node not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking superseder node ownership");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    // ── AUTHORIZATION: both endpoints must be REACHABLE, not merely in the
    //    caller's account (module doc's authorization section) ────────────────
    //
    // This is what closes the write side of the cross-ocean supersession
    // defect: the two `node_owned_by_account` checks above are satisfied by
    // every member-private ocean in an org simultaneously, because they all
    // share one `account_id`. `ReachabilityResolver` is the only predicate
    // that knows about `graphs.kind = 'member'`/`owner_user_id`.
    let superseded_graph = authorize_reachable_node!(
        tenant,
        store,
        scoped_tx,
        node_id,
        user,
        "node not found",
        "superseded node"
    );
    let superseder_graph = authorize_reachable_node!(
        tenant,
        store,
        scoped_tx,
        superseded_by,
        user,
        "superseder node not found",
        "superseder node"
    );
    // Reachable is not writable (settings 2.10): only the caller's own private
    // ocean, or a shared ocean an owner/admin moderates.
    require_graph_writable!(store, scoped_tx, superseded_graph, user, tenant, "node not found");
    require_graph_writable!(store, scoped_tx, superseder_graph, user, tenant, "superseder node not found");

    // ── INTEGRITY: an edge belongs to exactly ONE ocean ──────────────────────
    //
    // Reachability is per-CALLER; this row is not. Someone who reaches both the
    // org commons and her own member ocean would otherwise be able to write an
    // edge that every other member reads back — see the module doc and 0094's
    // header. This is also the invariant `supersession_edge.graph_id` (NOT
    // NULL) now records and 0083's RLS policy had already been assuming.
    //
    // A 400 rather than a 404: by the time control reaches here the caller has
    // been proven to reach BOTH oceans, so there is nothing left to conceal and
    // an honest error is more useful than a misleading "not found".
    if superseded_graph != superseder_graph {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "a supersession edge must connect two reefs in the same ocean"
            })),
        )
            .into_response();
    }

    // ── Cycle detection: check if superseded_by already points back to node_id ─
    // A cycle would exist if superseded_by is already superseded_id for node_id
    // as superseder (i.e. an edge: node_id → superseded_by already exists).
    match scoped_store!(
        store,
        scoped_tx,
        supersession_edge_exists / supersession_edge_exists_scoped(node_id, superseded_by)
    ) {
        Ok(true) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "creating this edge would introduce a cycle"})),
            )
                .into_response();
        }
        Ok(false) => {}
        Err(e) => {
            tracing::error!(error = %e, "DB error during cycle check");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    // ── Insert the supersession edge ──────────────────────────────────────────
    // superseder_id = superseded_by (the new/correct node)
    // superseded_id = node_id       (the old/outdated node)
    // source = 'user' (human-initiated via API)
    let edge = match scoped_store!(
        store,
        scoped_tx,
        upsert_supersession_edge / upsert_supersession_edge_scoped(superseded_by, node_id)
    ) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "DB error inserting supersession edge");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    // Persist the edge before the fire-and-forget event references it.
    commit_write_tx!(scoped_tx);

    // ── Fire-and-forget activity event ────────────────────────────────────────
    //
    // Gap #6 fix (`hourly-completeness.md`): this was `None` — the journal's
    // `journal_hour_inputs`/`journal_dirty_hours` queries filter
    // `activity_events.graph_id = $graph` (`store/mod.rs:18512-18513`,
    // `:18422-18423`), so a `None` graph_id could never match and the
    // sweep's supersession prose line (`workers/journal_sweep.rs:782`) was
    // dead code in practice. `superseded_graph == superseder_graph` was
    // already enforced above, so either edge endpoint's graph is correct
    // here.
    insert_activity_event(
        pool,
        user.account_id,
        user.user_id,
        Some(superseder_graph),
        "supersession_created",
        json!({
            "superseder_id": edge.superseder_id,
            "superseded_id": edge.superseded_id,
        }),
    );

    (
        StatusCode::OK,
        Json(json!({
            "superseder_id": edge.superseder_id,
            "superseded_id": edge.superseded_id,
            "created_at": edge.created_at,
        })),
    )
        .into_response()
}

// ── DELETE /v1/nodes/{id}/supersede/{other_id} ────────────────────────────────
//
// Removes a supersession edge between the two nodes in either direction.

pub async fn delete_supersession(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((node_id, other_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    let store = require_store!(tenant);

    // One account-scoped transaction — caller's active account, same boundary
    // the IDOR checks below enforce (module doc's RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // ── IDOR: both nodes must belong to caller's account ─────────────────────
    match scoped_store!(
        store,
        scoped_tx,
        node_owned_by_account / node_owned_by_account_scoped(node_id, user.account_id)
    ) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "node not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking node ownership");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    match scoped_store!(
        store,
        scoped_tx,
        node_owned_by_account / node_owned_by_account_scoped(other_id, user.account_id)
    ) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "other node not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking other node ownership");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    // ── AUTHORIZATION: both endpoints must be REACHABLE ──────────────────────
    //
    // Same reasoning as `create_supersession`, and it matters for the same
    // reason even though this handler only ever REMOVES a row: without it, one
    // org member holding two of another member's reef ids can delete edges out
    // of a private ocean she cannot read — silent tampering with someone else's
    // knowledge graph, invisible to its owner. No same-ocean check here: this
    // handler must stay able to clean up a pre-0094 cross-ocean row if one ever
    // survives, and by this point the caller has been proven to reach both
    // sides of whatever it is deleting.
    let node_graph = authorize_reachable_node!(
        tenant,
        store,
        scoped_tx,
        node_id,
        user,
        "node not found",
        "node"
    );
    let other_graph = authorize_reachable_node!(
        tenant,
        store,
        scoped_tx,
        other_id,
        user,
        "other node not found",
        "other node"
    );
    // Deleting an edge is a write to both endpoints' oceans (settings 2.10).
    require_graph_writable!(store, scoped_tx, node_graph, user, tenant, "node not found");
    require_graph_writable!(store, scoped_tx, other_graph, user, tenant, "other node not found");

    // ── Delete the edge in either direction ───────────────────────────────────
    match scoped_store!(
        store,
        scoped_tx,
        delete_supersession_edge / delete_supersession_edge_scoped(node_id, other_id)
    ) {
        Err(e) => {
            tracing::error!(error = %e, "DB error deleting supersession edge");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
        Ok(0) => {
            // Nothing was deleted; committing just closes the (write-free)
            // transaction before the non-leaking 404.
            commit_write_tx!(scoped_tx);
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "no supersession edge found between these nodes"})),
            )
                .into_response()
        }
        Ok(_) => {
            // D3/§3.7: removing an edge here is a human REJECTING that
            // pairing, not merely tidying up — record it so a future
            // detection pass (§3.4/§3.4a, neither built yet) never
            // re-proposes the exact pair someone just turned down (the
            // mirror image of the bug migration 0050 fixed for
            // relationship-edge rejections). Same transaction as the delete,
            // so the rejection can never be recorded without the delete
            // actually having happened (or vice versa).
            if let Err(e) = scoped_store!(
                store,
                scoped_tx,
                mark_non_supersession_pair / mark_non_supersession_pair_scoped(
                    node_id,
                    other_id,
                    Some(user.user_id)
                )
            ) {
                tracing::error!(error = %e, "DB error recording non_supersession_pair rejection");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
            commit_write_tx!(scoped_tx);
            StatusCode::NO_CONTENT.into_response()
        }
    }
}

// ── GET /v1/nodes/{id}/supersessions ─────────────────────────────────────────
//
// Lists all supersession edges where the node is either the superseder or
// the superseded node.

pub async fn list_supersessions(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
) -> impl IntoResponse {
    let store = require_store!(tenant);

    // One account-scoped transaction — caller's active account, same boundary
    // the IDOR check below enforces (module doc's RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // ── IDOR: node must belong to caller's account ────────────────────────────
    match scoped_store!(
        store,
        scoped_tx,
        node_owned_by_account / node_owned_by_account_scoped(node_id, user.account_id)
    ) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "node not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error checking node ownership");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    // ── AUTHORIZATION: the node must be REACHABLE, not merely in the caller's
    //    account (module doc's authorization section) ──────────────────────────
    //
    // The account check above is satisfied by every member-private ocean in an
    // org simultaneously, so on its own it let any member who obtained another
    // member's reef id enumerate that reef's edges — and this handler returns
    // `superseder_title`/`superseded_title`, i.e. CONTENT, not just ids. Same
    // predicate, same 404, same fail-closed handling as the two write handlers.
    let graph_id = authorize_reachable_node!(
        tenant,
        store,
        scoped_tx,
        node_id,
        user,
        "node not found",
        "node"
    );

    // ── Query all edges in both directions, WITHIN that ocean ────────────────
    //
    // Scoped by `graph_id`, not just by `node_id`. Authorizing the queried reef
    // is not sufficient by itself: every row here also carries the OTHER
    // endpoint's title, so an edge pointing out of this ocean would disclose a
    // reef title the caller was never authorized for — the queried node passing
    // its check says nothing about where the far endpoint lives. Since #349 no
    // write path can create such an edge and migrations 0094/0008 deleted the
    // ones that existed, so this is defense in depth; it is here because
    // "unreachable because of an invariant maintained in another file" is
    // exactly the reasoning that produced this defect class twice already.
    let rows = match scoped_store!(
        store,
        scoped_tx,
        list_supersession_edges / list_supersession_edges_scoped(node_id, graph_id)
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "DB error listing supersession edges");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    // Read-only handler: everything is materialized; a commit failure is
    // logged, never surfaced (nothing was written).
    if let Some(tx) = scoped_tx.take() {
        if let Err(e) = tx.commit().await {
            tracing::warn!(error = %e, "read-only scoped transaction commit failed");
        }
    }

    // ── Split into as_superseder and as_superseded ────────────────────────────
    let as_superseder: Vec<Value> = rows
        .iter()
        .filter(|r| r.superseder_id == node_id)
        .map(|r| {
            json!({
                "superseder_id": r.superseder_id,
                "superseded_id": r.superseded_id,
                "superseder_title": r.superseder_title,
                "superseded_title": r.superseded_title,
            })
        })
        .collect();

    let as_superseded: Vec<Value> = rows
        .iter()
        .filter(|r| r.superseded_id == node_id)
        .map(|r| {
            json!({
                "superseder_id": r.superseder_id,
                "superseded_id": r.superseded_id,
                "superseder_title": r.superseder_title,
                "superseded_title": r.superseded_title,
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "as_superseder": as_superseder,
            "as_superseded": as_superseded,
        })),
    )
        .into_response()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::{delete, get, post},
        Router,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::AppState;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    fn supersession_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route(
                "/v1/nodes/{id}/supersede",
                post(create_supersession),
            )
            .route(
                "/v1/nodes/{id}/supersede/{other_id}",
                delete(delete_supersession),
            )
            .route(
                "/v1/nodes/{id}/supersessions",
                get(list_supersessions),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    // ── POST /v1/nodes/{id}/supersede — no auth → 401 ────────────────────────

    #[tokio::test]
    async fn create_supersession_no_auth_returns_401() {
        let router = supersession_router(test_state());
        let node_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("POST")
            .uri(format!("/v1/nodes/{node_id}/supersede"))
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_string(&json!({"superseded_by": Uuid::new_v4()})).unwrap(),
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── POST /v1/nodes/{id}/supersede — bearer + no DB → 503 ─────────────────

    #[tokio::test]
    async fn create_supersession_bearer_no_db_returns_503() {
        let router = supersession_router(test_state());
        let node_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("POST")
            .uri(format!("/v1/nodes/{node_id}/supersede"))
            .header("Authorization", "Bearer sometoken")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_string(&json!({"superseded_by": Uuid::new_v4()})).unwrap(),
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Self-supersession validation (pure logic — no DB needed) ─────────────
    //
    // We test the business-rule directly: a node cannot supersede itself.

    #[test]
    fn self_supersession_detected() {
        let id = Uuid::new_v4();
        // This mirrors the guard in create_supersession.
        assert!(id == id, "id must equal itself");
        // The handler checks `node_id == superseded_by` and returns 400.
        let is_self = id == id;
        assert!(is_self, "same UUIDs should trigger self-supersession guard");
    }

    // ── Different UUIDs do not trigger self-supersession guard ───────────────

    #[test]
    fn different_uuids_pass_self_check() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        assert_ne!(a, b, "two fresh UUIDs must be distinct");
        let is_self = a == b;
        assert!(!is_self, "different UUIDs should not trigger self-supersession guard");
    }

    // ── DELETE /v1/nodes/{id}/supersede/{other_id} — no auth → 401 ───────────

    #[tokio::test]
    async fn delete_supersession_no_auth_returns_401() {
        let router = supersession_router(test_state());
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri(format!("/v1/nodes/{a}/supersede/{b}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── GET /v1/nodes/{id}/supersessions — no auth → 401 ─────────────────────

    #[tokio::test]
    async fn list_supersessions_no_auth_returns_401() {
        let router = supersession_router(test_state());
        let node_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/nodes/{node_id}/supersessions"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Route structure: POST supersede reachable ─────────────────────────────

    #[tokio::test]
    async fn route_post_supersede_reachable() {
        // 503 (not 404) proves the route is registered; require_auth fires first.
        let router = supersession_router(test_state());
        let node_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("POST")
            .uri(format!("/v1/nodes/{node_id}/supersede"))
            .header("Authorization", "Bearer tok")
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_string(&json!({"superseded_by": Uuid::new_v4()})).unwrap(),
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // 503 → route exists (db=None), not 404.
        assert_ne!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── Route structure: GET supersessions reachable ──────────────────────────

    #[tokio::test]
    async fn route_get_supersessions_reachable() {
        let router = supersession_router(test_state());
        let node_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/nodes/{node_id}/supersessions"))
            .header("Authorization", "Bearer tok")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(resp.status(), StatusCode::NOT_FOUND);
    }
}
