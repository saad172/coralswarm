//! Node CRUD endpoints.
//!
//! - `GET  /v1/nodes`                    — paginated list with atom count
//! - `PATCH /v1/nodes/{id}`              — update title and/or type
//! - `GET  /v1/nodes/{id}/candidates`    — top-K candidate atoms ranked by vector similarity
//! - `GET  /v1/graphs/{graph_id}/merge-review` — one keyset page of the standing
//!   UNSURE merge-review queue (judgement graph, PR4)
//! - `POST /v1/graphs/{graph_id}/merge-review/confirm` — resolve one card as
//!   "same reef". Graph-anchored so it works on any REACHABLE ocean, not only
//!   one the caller's ACTIVE account owns; Reject stays the existing
//!   merge-suggestions/dismiss route
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.2)
//! Queries touching RLS-covered consumer tables (`nodes`/`node_atoms`/
//! `node_merges`/`atoms`/`graphs`/…, migration `0083_consumer_rls`) run inside
//! [`crate::tenancy::scope::begin_account_scoped`] transactions through the
//! Store's `_scoped` twins, so these routes keep working once OPS-4 switches
//! the app's connection role to the non-`BYPASSRLS` `coralswarm_app`.
//! Scope-account choice: graph-anchored handlers scope by the OCEAN'S owning
//! account (via the `Store::graph_account_id` SECURITY DEFINER bootstrap
//! resolver, migration 0084); node-/merge-id-anchored handlers scope by the
//! CALLER'S ACTIVE ACCOUNT — the only account known before the row is loaded
//! (no node→account bootstrap resolver exists, deliberately), failing closed
//! onto each handler's existing non-leaking 404/403 when the resource belongs
//! to a different account under live RLS.
//!
//! Deliberate exceptions, each documented at its call site:
//!   * `ReachabilityResolver` — permanent multi-account SECURITY DEFINER
//!     carve-out; runs outside any scoped transaction.
//!   * `Store::graph_account_id` — the bootstrap resolver itself.
//!   * `atom_inline_content` — no `_scoped` twin exists yet; the remaining
//!     best-effort call site (candidate snippets) degrades gracefully.
//!     TODO(org-tenancy). `node_digest` no longer uses it — it reads content
//!     through `routes::queries::fetch_atom_content` instead (D5), which
//!     scopes itself per graph internally and is R2-aware.
//!   * `dedup_entities` / `promote_source` / `node_digest` run more than one
//!     scoped transaction per request (loop-per-merge, loop-per-reef, and
//!     read-vs-cache-write respectively) — each keeps a per-item
//!     warn-and-continue (or best-effort cache-write) contract that a single
//!     shared transaction cannot express, since one failed item would abort
//!     every item after it. Same precedent as the B.4 backfill loop; see each
//!     handler's doc comment.
//!   * The consumer-plane promotion routes are cross-OCEAN but NOT
//!     cross-account: migration 0083's `node_promotions` INSERT policy is a
//!     strict AND requiring the acting account to own BOTH the source reef's
//!     ocean and the target ocean (its own comment: same-account "by
//!     construction", and the strict AND "never rejects a legitimate write").
//!     Genuinely cross-account promotion is the ORG-plane branch, and it is
//!     confined to ONE database — see the next bullet.
//!   * **Promotion never crosses a database.** An org-plane promotion's source
//!     must live in that org's own tenant DB (the caller's own `member` ocean);
//!     anything else — most notably the caller's PERSONAL ocean on the consumer
//!     plane — is refused with 403 by `route_org_promotion`. The former
//!     cross-plane service (`services::promotion`, which read the consumer pool
//!     and re-encrypted content from the personal DEK to the org DEK) is
//!     deleted, not merely unreachable, and `scripts/check_no_cross_plane_
//!     promotion.sh` fails CI if a consumer-pool handle reappears under
//!     `routes/` or `services/`.
//!   * `resolve_tenant_node_graph` / `resolve_tenant_graph` (member-private-
//!     oceans PR 6) — the routing pre-check that decides whether an org-plane
//!     promotion is served same-pool below or refused outright.
//!     Deliberately unscoped plain reads against the ORG TENANT pool (not
//!     `begin_account_scoped` — inert there, same as the ORG-plane branches
//!     immediately above), scoped instead by an explicit `account_id = $N`
//!     equality. When the routing check selects the same-pool path, the
//!     handler falls through into the SAME scoped body personal/team
//!     promotions already use — no additional exception, just a new way to
//!     reach code that was previously unreachable in org context.
//!   * no pool (`tenant.pool == None`): opening the scoped transaction fails
//!     and the handler returns a 500; there is no unscoped fallback.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::routes::queries::collapse_snippet;
use crate::services::blob_store::PromotionBlobStore;
use crate::services::retraction;
use crate::store::{JudgementSource, JudgementVerdict, NewJudgement};
use crate::tenancy::reachability::{row_to_graph as tenant_row_to_graph, GraphRow as TenantGraphRow};
use crate::workers::entity_resolution::{judgement_write_mode, JudgementWriteMode};
use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::{ReachabilityResolver, ReefAccess, ReefAccessError}, AppState,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

// Reads through the `Tenant` extractor's resolved store (org-tenancy PR 1.5)
// rather than ambient `state.store` — everything this file touches (`nodes`,
// `node_atoms`, `node_merges`, `node_promotions`, `atoms`, `graphs`,
// `llm_usage`) is tenant-tree. The one ambient `AppState` field still used
// here is `state.llm` in `node_digest` (the process-wide LLM client — not a
// data plane).
macro_rules! require_store {
    ($tenant:expr) => {
        match &$tenant.store {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
            }
        }
    };
}

/// The request's account-scoped transaction. `None` only when an unscoped
/// entry point such as `patch_node_impl` passes `&mut None`, which runs the
/// plain unscoped Store methods. See the module doc's RLS section.
pub(crate) type ScopedTx = Option<sqlx::Transaction<'static, sqlx::Postgres>>;

/// Open the request's account-scoped transaction (org-tenancy OPS-4 phase
/// B.2), or early-return a 500. On the org plane this opens on the TENANT pool
/// via `Tenant::begin_member_scoped`, which also sets `app.user_id` — and that
/// GUC has real policies to satisfy there: migration `0009_tenant_rls` gives
/// tenant DBs their own member-ocean RLS, keyed on exactly this GUC. (An
/// earlier revision of this comment called the org-plane GUC "inert by design
/// (tenant DBs are whole-database isolated, never RLS'd)"; that was true only
/// of the CROSS-ORG boundary, and stopped being a complete account of the
/// tenant plane the moment 0009 shipped the boundary INSIDE a tenant DB.) It
/// is still UNEVALUATED today — but for a role reason, not a policy one: the
/// app connects to tenant pools as the table owner, and Phase 3 (`OPS-4-T`)
/// repointing each tenant DSN at the non-owner `coralswarm_tenant_app` role is
/// what makes it bite. See `tenancy::scope`'s module doc.
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
                    );
                }
            }
        }
    };
}

/// Dispatch one Store call through the scoped transaction, or the plain
/// unscoped method when `$tx` is `None`. `begin_scoped_tx!` never yields
/// `None` (with no pool it returns a 500); `None` comes only from callers that
/// pass `&mut None`, such as `patch_node_impl`.
macro_rules! scoped_store {
    ($store:expr, $tx:expr, $unscoped:ident / $scoped:ident ( $($arg:expr),* $(,)? )) => {
        match $tx.as_mut() {
            Some(tx) => $store.$scoped(tx, $($arg),*).await,
            None => $store.$unscoped($($arg),*).await,
        }
    };
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
                );
            }
        }
    };
}

/// Commit a read-only handler's scoped transaction. Failures are logged and
/// swallowed: the response data is already materialized, and a read-only
/// `SET LOCAL` transaction only fails to commit when a tolerated best-effort
/// sub-query already aborted it (those sub-results then fell back to their
/// existing best-effort defaults — same degradation as before this change).
async fn commit_read_tx(tx: ScopedTx) {
    if let Some(tx) = tx {
        if let Err(e) = tx.commit().await {
            tracing::warn!(error = %e, "read-only scoped transaction commit failed");
        }
    }
}

/// One merge in its OWN scoped transaction. `dedup_entities`' per-merge
/// warn-and-continue contract needs each merge isolated — a single shared
/// transaction cannot give that (one failed merge would abort every merge
/// after it, then the final commit). Loop-per-write, the same precedent as
/// the Phase B.4 backfill's loop-per-account.
async fn merge_nodes_isolated(
    tenant: &Tenant,
    store: &Arc<dyn crate::store::Store>,
    account_id: Uuid,
    keep_id: Uuid,
    dup_id: Uuid,
) -> Result<u64, crate::store::StoreError> {
    {
        let mut tx = tenant.begin_member_scoped(account_id).await?;
        let moved = store.merge_nodes_scoped(&mut tx, keep_id, dup_id).await?;
        tx.commit().await?;
        Ok(moved)
    }
}

/// One §5.0 active-judgement lookup in its OWN scoped transaction — F2
/// (review round 2): `dedup_entities`' per-pair "don't merge past an active
/// human NEGATIVE" check needs the same per-call isolation as
/// [`merge_nodes_isolated`] (the sweep's list transaction is already
/// committed by the time this per-pair loop runs, same reason that fn
/// exists). Read-only, so a lookup failure here does NOT block the merge —
/// `dedup_entities` is explicitly a listed judgement-graph write-coverage
/// bypass (it writes no judgement of its own); this check only stops it from
/// overriding an EXISTING ruling when it can see one, it does not become a
/// write gate that fails closed when it can't.
async fn get_active_judgement_isolated(
    tenant: &Tenant,
    store: &Arc<dyn crate::store::Store>,
    account_id: Uuid,
    graph_id: Uuid,
    a: Uuid,
    b: Uuid,
) -> Result<Option<crate::store::ReefJudgement>, crate::store::StoreError> {
    {
        let mut tx = tenant.begin_member_scoped(account_id).await?;
        let out = store.get_active_judgement_scoped(&mut tx, graph_id, a, b).await?;
        tx.commit().await?;
        Ok(out)
    }
}

const ALLOWED_TYPES: &[&str] = &["entity", "decision", "thread", "reference"];

/// Max characters of a `PATCH /v1/nodes/{id}` (and MCP `update_reef`) title.
/// Shared by [`validate_patch_fields`] so both callers reject an oversized
/// title identically (checked+fixed items #4, later widened to the REST
/// route — see `validate_patch_fields`'s doc comment).
pub const PATCH_TITLE_CAP: usize = 512;

/// Max characters of a candidate atom's content shown as its `snippet`
/// (candidate corals list — CS-176 follow-up).
const CANDIDATE_SNIPPET_CHARS: usize = 140;

fn is_valid_node_type(t: &str) -> bool {
    ALLOWED_TYPES.contains(&t)
}

// ── Request / response types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ListNodesQuery {
    pub graph_id: Option<Uuid>,
    pub node_type: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct PatchNodeRequest {
    pub title: Option<String>,
    /// Guard concurrent updates when provided.
    pub expected_version: Option<i64>,
    /// Explicit member supporting a human identity interpretation. Omission
    /// retains the ordinary metadata-only rename behavior.
    pub support_atom_id: Option<Uuid>,
    #[serde(rename = "type")]
    pub node_type: Option<String>,
    /// Library kind (`person`/`org`/`topic`/`project`). Sets sticky so
    /// extract / 100-coral re-eval cannot overwrite it.
    pub entity_type: Option<String>,
}

// ── GET /v1/nodes ─────────────────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/nodes",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(
        ("graph_id"  = Uuid,            Query, description = "Graph identifier (required)"),
        ("node_type" = Option<String>,  Query, description = "Filter by type: entity|decision|thread|reference"),
        ("page"      = Option<i64>,     Query, description = "Page number (default 1)"),
        ("page_size" = Option<i64>,     Query, description = "Items per page (default 20, max 100)"),
    ),
    responses(
        (status = 200, description = "List of nodes with atom counts (bare JSON array; \
            an `X-Has-More: true|false` response header indicates whether a further page \
            exists — the body shape is intentionally unchanged for existing consumers, \
            see CS-reef-pagination)", body = Value),
        (status = 400, description = "Invalid query parameters"),
        (status = 403, description = "Graph not owned by caller"),
        (status = 422, description = "graph_id is required"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_nodes(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<ListNodesQuery>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    // ── Require graph_id ──────────────────────────────────────────────────────
    let graph_id = match params.graph_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                HeaderMap::new(),
                Json(json!({"error": "graph_id is required"})),
            )
        }
    };

    // ── Validate node_type if provided ────────────────────────────────────────
    if let Some(ref t) = params.node_type {
        if !is_valid_node_type(t) {
            return (
                StatusCode::BAD_REQUEST,
                HeaderMap::new(),
                Json(
                    json!({"error": "node_type must be one of: entity, decision, thread, reference"}),
                ),
            );
        }
    }

    // Not `require_store!` here (unlike sibling handlers in this file): that
    // macro's early return is a bare `(StatusCode, Json<Value>)` 2-tuple,
    // which no longer matches this handler's 3-tuple return type (the extra
    // `HeaderMap` slot carries `X-Has-More` on success, below) — same 503
    // behavior, just spelled out inline so every branch shares one type. Same
    // `Tenant`-resolved content store as the macro (org-tenancy PR 1.5).
    let store = match &tenant.store {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                HeaderMap::new(),
                Json(json!({"error": "service unavailable"})),
            )
        }
    };

    // ── IDOR check: graph must be reachable by the caller (ADR-0007 D9 union:
    // their personal ocean, org ocean, or a team they belong to), via
    // `ReachabilityResolver` with the request's real decision (org-tenancy
    // PR 1.5) ─────────────────────────────────────────────────────────────────
    let owned = match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(o) => o,
        Err(e) => {
            tracing::error!(error = %e, "list_nodes ownership check error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    };

    if !owned {
        return (
            StatusCode::FORBIDDEN,
            HeaderMap::new(),
            Json(json!({"error": "graph not found or access denied"})),
        );
    }

    // ── Scope account: the ocean's owning account, via the SECURITY DEFINER
    // bootstrap resolver (module doc's RLS section). Not `begin_scoped_tx!`
    // (that macro early-returns this file's 2-tuple shape; this handler's is
    // the 3-tuple with the `X-Has-More` HeaderMap slot) — same dispatch,
    // spelled out inline, mirroring the inline `require_store!` above. ───────
    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        // Reachability above already proved the graph exists; refuse
        // non-leakingly on the effectively-unreachable arm.
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                HeaderMap::new(),
                Json(json!({"error": "graph not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_nodes account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx: ScopedTx = {
        match tenant.begin_member_scoped(account_id).await {
            Ok(tx) => Some(tx),
            Err(e) => {
                tracing::error!(error = %e, "failed to open account-scoped transaction");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HeaderMap::new(),
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    };

    // ── Pagination ────────────────────────────────────────────────────────────
    let page = params.page.unwrap_or(1).max(1);
    // Ceiling raised 100 → 1,000 (W3,
    // `docs/plans/request-reliability-and-scale.md` §6.3). At 3,440 reefs the
    // old ceiling made `reefs.listAll` 35 sequential round trips no matter
    // what the client asked for — a SERVER-side limit, which is why W3's
    // request budget was unreachable without changing it here. At 1,000 the
    // same drain is 4 requests.
    //
    // Safe because the row shape is small and fixed: id, type, title, scope,
    // two timestamps and a count — no bodies, no content, no embeddings. A
    // 1,000-row page is a few hundred KB, comparable to a single atom's
    // content, and the query is already `LIMIT`-bounded at the database.
    // The DEFAULT stays 20, so no existing caller's behaviour changes.
    let page_size = params.page_size.unwrap_or(20).clamp(1, 1000);
    let offset = (page - 1) * page_size;

    // Fetch one extra row to compute `has_more` without a second COUNT query —
    // same pattern the MCP `list_reefs` tool uses (`mcp/tools/read.rs`).
    let rows = match scoped_store!(
        store,
        scoped_tx,
        list_nodes / list_nodes_scoped(graph_id, params.node_type.as_deref(), page_size + 1, offset)
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_nodes DB error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    };

    commit_read_tx(scoped_tx).await;

    let has_more = rows.len() as i64 > page_size;

    let nodes: Vec<Value> = rows
        .into_iter()
        .take(page_size as usize)
        .map(|n| {
            json!({
                "node_id":    n.node_id,
                "type":       n.node_type,
                "title":      n.title,
                "scope":      n.scope,
                "atom_count": n.atom_count,
                "created_at": n.created_at,
                "updated_at": n.updated_at,
            })
        })
        .collect();

    // The response BODY stays a bare JSON array — deliberately unchanged shape.
    // Pagination metadata rides on a response header instead: purely
    // additive, ignored by any consumer not looking for it.
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-has-more",
        HeaderValue::from_static(if has_more { "true" } else { "false" }),
    );

    (StatusCode::OK, headers, Json(json!(nodes)))
}

// ── GET /v1/graphs/{graph_id}/node-atoms ──────────────────────────────────────

/// Query for [`list_graph_node_atoms`].
#[derive(Debug, Deserialize)]
pub struct GraphNodeAtomsQuery {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
    /// When true, return exact server-computed facet counts instead of the
    /// edge page. Same authorization, different projection — kept on one
    /// route rather than two so there is exactly ONE place where "who may
    /// read this graph's edges" is decided.
    pub facets: Option<bool>,
}

/// Ceiling on `page_size`. Paired with the client's 20-page drain budget this
/// gives 20,000 edges before the budget binds (plan §6.3).
const NODE_ATOMS_MAX_PAGE_SIZE: i64 = 1_000;

/// Largest `OFFSET` this endpoint will accept. Comfortably above §7's
/// 600–750k target edge count, and far below where `(page - 1) * page_size`
/// could overflow — so a caller-chosen `page` is refused with a 422 rather
/// than panicking (debug) or wrapping to a negative offset that Postgres
/// rejects (release).
const MAX_EDGE_OFFSET: i64 = 100_000_000;

/// The bulk reef↔coral edge contract (W3,
/// `docs/plans/request-reliability-and-scale.md` §6.3).
///
/// # What this replaces
///
/// The Reefs workbench needs every `(node_id, atom_id)` link in the ocean to
/// build its membership map, "grown from" facet and weekly activity bars. It
/// got them by calling `GET /v1/nodes/{id}/atoms` once per reef — **3,440
/// requests, and 3,440 authorization checks**, each doing ~5 sequential DB
/// operations against a small pool. This answers the same question in one
/// paginated query.
///
/// # Why this is the security-relevant part of the plan
///
/// **The efficiency win and the risk are the same sentence.** The gain comes
/// precisely from doing ONE authorization check instead of 3,440 — so that
/// one check must be exactly right. Under the old shape a wrong check leaked
/// one reef; here it would leak an entire ocean's edge graph. Three
/// independent boundaries, deliberately not collapsed into one:
///
/// 1. **Reachability** — `ReachabilityResolver::graph_reachable_by_user` with
///    the request's real tenancy decision, identical to `list_nodes`. This is
///    the same union (personal / org / team) every other graph-scoped read
///    uses; it is not a new policy, which is the point.
/// 2. **RLS** — the query runs inside an account-scoped transaction, so
///    `node_atoms`, `nodes` and `atoms` are additionally filtered by the
///    database itself. If boundary 1 were ever wrong, this still holds.
/// 3. **The SQL's own join** — `WHERE n.graph_id = $1` through `nodes`, so
///    the rows returned are by construction the rows of the graph that was
///    authorized. No caller-supplied node list is trusted.
#[utoipa_path(
    get,
    path = "/v1/graphs/{graph_id}/node-atoms",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("graph_id" = Uuid, Path, description = "Ocean (graph) identifier")),
    responses(
        (status = 200, description = "Reef-coral edges, or exact facet counts with ?facets=true", body = Value),
        (status = 403, description = "Graph not reachable by caller"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_graph_node_atoms(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Query(params): Query<GraphNodeAtomsQuery>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    let store = match &tenant.store {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                HeaderMap::new(),
                Json(json!({"error": "service unavailable"})),
            )
        }
    };

    // ── Boundary 1: reachability ─────────────────────────────────────────────
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::FORBIDDEN,
                HeaderMap::new(),
                Json(json!({"error": "graph not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_graph_node_atoms reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // ── Boundary 2: RLS account scope ────────────────────────────────────────
    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                HeaderMap::new(),
                Json(json!({"error": "graph not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_graph_node_atoms account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx: ScopedTx = {
        match tenant.begin_member_scoped(account_id).await {
            Ok(tx) => Some(tx),
            Err(e) => {
                tracing::error!(error = %e, "failed to open account-scoped transaction");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HeaderMap::new(),
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    };

    // ── Facets projection ────────────────────────────────────────────────────
    if params.facets.unwrap_or(false) {
        let facets = match scoped_store!(
            store,
            scoped_tx,
            graph_node_atom_facets / graph_node_atom_facets_scoped(graph_id)
        ) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(error = %e, "list_graph_node_atoms facets error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HeaderMap::new(),
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        commit_read_tx(scoped_tx).await;
        return (
            StatusCode::OK,
            HeaderMap::new(),
            Json(json!({
                "total_edges": facets.total_edges,
                "by_source_type": facets.by_source_type.into_iter()
                    .map(|(k, v)| json!({"source_type": k, "count": v}))
                    .collect::<Vec<_>>(),
                "by_week": facets.by_week.into_iter()
                    .map(|(k, v)| json!({"week": k, "count": v}))
                    .collect::<Vec<_>>(),
            })),
        );
    }

    // ── Edge page ────────────────────────────────────────────────────────────
    let page = params.page.unwrap_or(1).max(1);
    let page_size = params
        .page_size
        .unwrap_or(1_000)
        .clamp(1, NODE_ATOMS_MAX_PAGE_SIZE);
    // `(page - 1) * page_size` OVERFLOWS on a caller-chosen `page`: at
    // `i64::MAX` it panics in debug and wraps to a negative in release, and a
    // negative OFFSET is a Postgres error — so an attacker picks the failure
    // mode. `checked_mul` turns that into an honest 422 instead.
    //
    // The page is also bounded: past the point where `offset` exceeds any
    // plausible edge count there is nothing to return, and accepting the
    // request only invites a full-index scan for an empty page.
    let offset = match page.checked_sub(1).and_then(|p| p.checked_mul(page_size)) {
        Some(o) if o <= MAX_EDGE_OFFSET => o,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                HeaderMap::new(),
                Json(json!({"error": "page out of range"})),
            )
        }
    };

    // One extra row to compute `has_more` without a second COUNT — the same
    // pattern as `list_nodes`.
    let rows = match scoped_store!(
        store,
        scoped_tx,
        graph_node_atoms / graph_node_atoms_scoped(graph_id, page_size + 1, offset)
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_graph_node_atoms DB error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    };

    commit_read_tx(scoped_tx).await;

    let has_more = rows.len() as i64 > page_size;
    let edges: Vec<Value> = rows
        .into_iter()
        .take(page_size as usize)
        .map(|e| {
            json!({
                "node_id":     e.node_id,
                "atom_id":     e.atom_id,
                "source_type": e.source_type,
                "timestamp":   e.timestamp,
            })
        })
        .collect();

    // Bare array + `X-Has-More`, matching `list_nodes`'s convention so the
    // client's existing `reqPaged` helper works unchanged.
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-has-more",
        HeaderValue::from_static(if has_more { "true" } else { "false" }),
    );

    (StatusCode::OK, headers, Json(json!(edges)))
}

// ── PATCH /v1/nodes/{id} ──────────────────────────────────────────────────────

/// Errors from [`patch_node_impl`]. `NotOwned` (the ownership check itself
/// failed) and `NotFound` (ownership passed, but the mutation matched no row —
/// a same-request race) are kept as distinct variants because the HTTP route
/// surfaces them as different status codes (403 vs 404, unchanged behavior);
/// the MCP `update_reef` tool (`mcp/tools/write.rs`) maps BOTH to a single
/// `not_found` tool error per the IDOR non-leak convention (plan §3
/// conventions: "IDOR misses always map to `not_found`").
pub enum PatchNodeError {
    /// Neither `title` nor `type` was provided.
    NoFields,
    /// `type` was provided but isn't one of the allowed reef types.
    InvalidType,
    /// `node_owned_by_account` returned `false`.
    NotOwned,
    /// Ownership passed but `patch_node`'s own lookup found nothing.
    NotFound,
    /// `title` was provided but is empty or whitespace-only.
    EmptyTitle,
    /// `title` was provided but exceeds [`PATCH_TITLE_CAP`] chars.
    TitleTooLong,
    /// `type` was provided as `"decision"` — `patch_node` has no
    /// `decided_at` handling, so this transition almost always trips the
    /// `decisions_require_decided_at` CHECK constraint downstream as an
    /// opaque internal error; rejected up front instead (checked+fixed
    /// items #2, later widened to the REST route).
    DecisionTypeNotAllowed,
    /// A caller-fixable conflict: either the new title/type collides with
    /// another reef in the same ocean (`nodes_graph_type_title_unique`), or
    /// `expected_version` didn't match the reef's current version
    /// (optimistic-concurrency guard). Carries a human-readable message.
    Conflict(String),
    Store(crate::store::StoreError),
}

/// Field validation shared by the HTTP `patch_node` handler and
/// [`patch_node_impl`] — split out as its own (store-free) function so the
/// HTTP handler can run it BEFORE `require_store!` (preserving its original
/// behavior: a store-less environment still 400s on bad input rather than
/// 503ing first) while `patch_node_impl` also runs it for callers (the MCP
/// `update_reef` tool) that go straight through the impl.
///
/// This is also where the empty/oversized-title and `type == "decision"`
/// rejections live (checked+fixed items #2/#4) — they were originally added
/// only inside the MCP `update_reef` tool handler, which left the REST
/// `PATCH /v1/nodes/{id}` route able to accept the exact bad input those
/// checks exist to reject. Centralizing here means both callers inherit the
/// same guard from the one shared function.
fn validate_patch_fields(title: Option<&str>, node_type: Option<&str>) -> Result<(), PatchNodeError> {
    if title.is_none() && node_type.is_none() {
        return Err(PatchNodeError::NoFields);
    }
    if let Some(t) = title {
        if t.trim().is_empty() {
            return Err(PatchNodeError::EmptyTitle);
        }
        if t.chars().count() > PATCH_TITLE_CAP {
            return Err(PatchNodeError::TitleTooLong);
        }
    }
    if let Some(t) = node_type {
        if !is_valid_node_type(t) {
            return Err(PatchNodeError::InvalidType);
        }
        if t == "decision" {
            return Err(PatchNodeError::DecisionTypeNotAllowed);
        }
    }
    Ok(())
}

/// Core patch logic for `PATCH /v1/nodes/{id}`, extracted (precedent:
/// `ingest::submit`) so both the HTTP handler and the MCP
/// `update_reef` tool share ONE guarded-UPDATE implementation rather than the
/// tool re-deriving it. Field validation ("at least one of title/type",
/// allowed type value) lives here too — not just in the HTTP handler — so a
/// caller of either surface gets the same rejections.
pub(crate) async fn patch_node_impl(
    store: &dyn crate::store::Store,
    writer: &ReefWriter<'_>,
    node_id: Uuid,
    title: Option<&str>,
    node_type: Option<&str>,
    expected_version: Option<i64>,
) -> Result<crate::store::Node, PatchNodeError> {
    // Unscoped entry point, kept for the MCP `update_reef` tool (whose RLS
    // conversion rides with the MCP-surface work, not this route chunk):
    // `&mut None` makes the scoped twin below run every query through the
    // plain unscoped Store methods, byte-identical to the pre-B.2 body.
    patch_node_impl_scoped(store, &mut None, writer, node_id, title, node_type, expected_version)
        .await
}

/// Who is writing to a reef, and on which surface's terms (settings 2.10).
///
/// Every user-initiated write to a reef, edge, merge or supersession asks
/// [`node_writable_by`] / [`graph_writable_by`] with one of these. The rule:
///
/// * Anyone may write their own writable private ocean — exactly
///   [`crate::ingest::is_own_writable_ocean`], the predicate D7's
///   ingest destination check (and so `add_context`) already enforces: a
///   `personal` ocean, or the caller's own unarchived `member` ocean.
/// * [`ReefWriter::private_only`] (MCP) stops there. Shared oceans are
///   populated through `promote` only — the rule the MCP server
///   instructions state.
/// * [`ReefWriter::rest`] (web REST, Slack review buttons) also lets
///   an account owner/admin MODERATE a shared ocean (`org`/`team`) they can
///   reach. Plain members review only their own private ocean.
/// * Nobody — admin included — writes another member's private ocean.
///
/// Before this, the node-anchored writers authorized on `graphs.account_id =
/// caller's account` alone. In an org every ocean shares that account, so any
/// member could confirm, reject or rename any reef in the org, including
/// another member's private reefs, given the id. The tenant
/// `member_ocean_isolation` RLS policy does not stop it in production: the
/// tenant connection is the table owner.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReefWriter<'a> {
    pub account_id: Uuid,
    pub user_id: Uuid,
    /// Owner/admin moderation of shared oceans. Never set for MCP.
    moderates_shared: bool,
    /// The request's tenancy decision; consulted only for moderation, where
    /// the shared ocean must also be reachable (a team ocean the admin is not
    /// on stays out of reach).
    decision: Option<&'a crate::tenancy::runtime::TenancyDecision>,
}

impl<'a> ReefWriter<'a> {
    /// Own private ocean only. MCP's rule for every caller.
    pub(crate) fn private_only(user: &UserContext) -> Self {
        Self { account_id: user.account_id, user_id: user.user_id, moderates_shared: false, decision: None }
    }

    /// Own private ocean, plus shared-ocean moderation when the caller's live
    /// role on the active account is owner/admin.
    pub(crate) fn rest(user: &UserContext, decision: Option<&'a crate::tenancy::runtime::TenancyDecision>) -> Self {
        Self::with_role(user.account_id, user.user_id, &user.role, decision)
    }

    /// [`Self::rest`] for callers that resolved the role themselves (the
    /// Slack review buttons, which identify a Slack user, not a session).
    pub(crate) fn with_role(
        account_id: Uuid,
        user_id: Uuid,
        role: &str,
        decision: Option<&'a crate::tenancy::runtime::TenancyDecision>,
    ) -> Self {
        Self { account_id, user_id, moderates_shared: crate::routes::org_invites::is_admin(role), decision }
    }

    /// The pure half of the decision. Moderation additionally needs
    /// reachability, which [`graph_writable_by`] checks.
    fn decide(&self, graph: &crate::store::Graph) -> GraphWrite {
        if crate::ingest::is_own_writable_ocean(graph, self.user_id) {
            GraphWrite::Own
        } else if self.moderates_shared && is_moderatable_shared_ocean(graph) {
            GraphWrite::ModerateIfReachable
        } else {
            GraphWrite::Denied
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum GraphWrite {
    Own,
    ModerateIfReachable,
    Denied,
}

/// Allowlist of shared ocean kinds an admin may moderate. `member` and
/// `personal` are private and never appear here.
fn is_moderatable_shared_ocean(graph: &crate::store::Graph) -> bool {
    matches!(graph.kind.as_str(), "org" | "team") && graph.archived_at.is_none()
}

/// May `writer` write into ocean `graph_id`? `Ok(false)` covers "no such
/// ocean" and "not in the writer's active account" too, so callers can map
/// every refusal to the same response a missing reef gets.
///
/// The ocean is read inside `scoped_tx` with the writer's ACTIVE account as
/// the filter: an ocean owned by another account (a team ocean reached by
/// membership) is never writable through this predicate, whatever the role.
pub(crate) async fn graph_writable_by(
    store: &dyn crate::store::Store,
    scoped_tx: &mut ScopedTx,
    graph_id: Uuid,
    writer: &ReefWriter<'_>,
) -> Result<bool, crate::store::StoreError> {
    let graph = match scoped_tx.as_mut() {
        Some(tx) => store.get_graph_scoped(tx, graph_id, writer.account_id).await?,
        None => store.get_graph(graph_id, writer.account_id).await?,
    };
    let Some(graph) = graph else { return Ok(false) };
    match writer.decide(&graph) {
        GraphWrite::Own => Ok(true),
        GraphWrite::Denied => Ok(false),
        GraphWrite::ModerateIfReachable => {
            ReachabilityResolver::graph_reachable_by_user(writer.decision, store, graph_id, writer.user_id).await
        }
    }
}

/// [`graph_writable_by`] for the ocean that holds `node_id`. `Ok(false)` for a
/// node that does not exist.
///
/// This is the predicate `PATCH /v1/nodes/{id}`, `GET /v1/nodes/{id}`'s
/// `can_edit`, confirm/reject (REST, MCP, Slack) and `update_reef` share, so
/// `can_edit` can only ever report what PATCH will actually do.
pub(crate) async fn node_writable_by(
    store: &dyn crate::store::Store,
    scoped_tx: &mut ScopedTx,
    node_id: Uuid,
    writer: &ReefWriter<'_>,
) -> Result<bool, crate::store::StoreError> {
    let graph_id = scoped_store!(store, *scoped_tx, node_graph_id / node_graph_id_scoped(node_id))?;
    match graph_id {
        Some(g) => graph_writable_by(store, scoped_tx, g, writer).await,
        None => Ok(false),
    }
}

/// Early-return the handler's own "not found" response unless `writer` may
/// write ocean `$graph` ([`graph_writable_by`]). Callers pass the SAME
/// not-found body their reachability refusal already returns, so a write-scope
/// refusal is indistinguishable from a reef that does not exist.
macro_rules! require_graph_writable {
    ($store:expr, $tx:expr, $graph:expr, $writer:expr, $not_found:expr) => {
        match graph_writable_by($store, &mut $tx, $graph, &$writer).await {
            Ok(true) => {}
            Ok(false) => return (StatusCode::NOT_FOUND, Json(json!({"error": $not_found}))),
            Err(e) => {
                tracing::error!(error = %e, "reef write-scope check error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    };
}

/// Scoped twin of [`patch_node_impl`] — same guarded-UPDATE logic, with every
/// Store call dispatched through the caller's account-scoped transaction when
/// one exists (org-tenancy OPS-4 phase B.2). The HTTP `PATCH /v1/nodes/{id}`
/// route goes through here.
pub(crate) async fn patch_node_impl_scoped(
    store: &dyn crate::store::Store,
    scoped_tx: &mut ScopedTx,
    writer: &ReefWriter<'_>,
    node_id: Uuid,
    title: Option<&str>,
    node_type: Option<&str>,
    expected_version: Option<i64>,
) -> Result<crate::store::Node, PatchNodeError> {
    validate_patch_fields(title, node_type)?;

    // ── Write scope: the reef's ocean must be one this writer may write ──────
    let owned = node_writable_by(store, scoped_tx, node_id, writer)
        .await
        .map_err(PatchNodeError::Store)?;
    if !owned {
        return Err(PatchNodeError::NotOwned);
    }

    // ── Update ────────────────────────────────────────────────────────────────
    // COALESCE keeps the existing value when the caller omits a field.
    match scoped_store!(
        store,
        *scoped_tx,
        patch_node / patch_node_scoped(node_id, title, node_type, expected_version)
    ) {
        Ok(Some(n)) => Ok(n),
        Ok(None) => Err(PatchNodeError::NotFound),
        Err(crate::store::StoreError::Conflict(msg)) => Err(PatchNodeError::Conflict(msg)),
        Err(e) => Err(PatchNodeError::Store(e)),
    }
}

/// Shared REST/MCP adapter. Only an explicit support selector enters the
/// authenticated identity writer; ordinary metadata writes retain their contract.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn patch_node_with_identity_impl_scoped(
    store: &dyn crate::store::Store,
    scoped_tx: &mut ScopedTx,
    writer: &ReefWriter<'_>,
    node_id: Uuid,
    title: Option<&str>,
    node_type: Option<&str>,
    expected_version: Option<i64>,
    support_atom_id: Option<Uuid>,
) -> Result<crate::store::Node, PatchNodeError> {
    validate_patch_fields(title, node_type)?;
    if expected_version.is_some_and(|v| v < 1) {
        return Err(PatchNodeError::Conflict("expected_version must be positive".into()));
    }
    let Some(support) = support_atom_id else {
        return patch_node_impl_scoped(store, scoped_tx, writer, node_id,
            title, node_type, expected_version).await;
    };
    // `patch_supported` checks only account ownership; the write scope is
    // enforced here so the supported-rename path cannot bypass it.
    if !node_writable_by(store, scoped_tx, node_id, writer).await.map_err(PatchNodeError::Store)? {
        return Err(PatchNodeError::NotOwned);
    }
    let title = title.ok_or_else(|| PatchNodeError::Conflict("identity support requires a title".into()))?;
    let tx = scoped_tx.as_mut().ok_or_else(|| PatchNodeError::Conflict("supported rename requires an authenticated PostgreSQL scope".into()))?;
    crate::services::reef_identity::patch_supported(store, tx, writer.account_id, writer.user_id,
        node_id, title, node_type, expected_version, support).await.map_err(|e| match e {
        crate::store::StoreError::NotFound => PatchNodeError::NotFound,
        crate::store::StoreError::Conflict(message) => PatchNodeError::Conflict(message),
        other => PatchNodeError::Store(other),
    })
}

#[utoipa_path(
    patch,
    path = "/v1/nodes/{id}",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "Node identifier")),
    responses(
        (status = 200, description = "Updated node", body = Value),
        (status = 400, description = "No fields provided or invalid type"),
        (status = 403, description = "Node not owned by caller"),
        (status = 404, description = "Node not found"),
        (status = 409, description = "Title/type collides with an existing reef"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn patch_node(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
    Json(body): Json<PatchNodeRequest>,
) -> (StatusCode, Json<Value>) {
    // Validated BEFORE `require_store!` (unchanged from pre-extraction
    // behavior): a store-less environment still 400s on bad input rather than
    // 503ing first.
    if let Err(e) = validate_patch_fields(body.title.as_deref(), body.node_type.as_deref()) {
        if body.entity_type.is_none() {
            return patch_error_response(e);
        }
        if !matches!(e, PatchNodeError::NoFields) {
            return patch_error_response(e);
        }
    }
    if let Some(kind) = body.entity_type.as_deref() {
        if crate::store::library::parse_library_kind(kind).is_none() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "entity_type must be one of: person, org, topic, project"})),
            );
        }
    }
    if body.title.is_none() && body.node_type.is_none() && body.entity_type.is_none() {
        return patch_error_response(PatchNodeError::NoFields);
    }

    if body.expected_version.is_some_and(|v| v < 1)
        || (body.support_atom_id.is_some() && body.title.is_none())
        || (body.expected_version.is_some() && body.title.is_none() && body.node_type.is_none())
    {
        return patch_error_response(PatchNodeError::Conflict("invalid guarded identity update".into()));
    }
    let store = require_store!(tenant);

    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());

    let patched = if body.title.is_some() || body.node_type.is_some() {
        match patch_node_with_identity_impl_scoped(
            store.as_ref(),
            &mut scoped_tx,
            &writer,
            node_id,
            body.title.as_deref(),
            body.node_type.as_deref(),
            body.expected_version,
            body.support_atom_id,
        )
        .await
        {
            Ok(n) => n,
            Err(e) => return patch_error_response(e),
        }
    } else {
        let owned = match node_writable_by(store.as_ref(), &mut scoped_tx, node_id, &writer)
            .await
        {
            Ok(b) => b,
            Err(e) => return patch_error_response(PatchNodeError::Store(e)),
        };
        if !owned {
            return patch_error_response(PatchNodeError::NotOwned);
        }
        match scoped_store!(store, scoped_tx, get_node / get_node_scoped(node_id)) {
            Ok(Some(n)) => n,
            Ok(None) => return patch_error_response(PatchNodeError::NotFound),
            Err(e) => return patch_error_response(PatchNodeError::Store(e)),
        }
    };

    if let Some(kind) = body.entity_type.as_deref() {
        if let Some(tx) = scoped_tx.as_mut() {
            if let Err(e) = crate::store::library::set_library_kind_sticky(tx, node_id, kind).await {
                return patch_error_response(match e {
                    crate::store::StoreError::NotFound => PatchNodeError::NotFound,
                    other => PatchNodeError::Store(other),
                });
            }
        }
    }

    let entity_type = if let Some(tx) = scoped_tx.as_mut() {
        match crate::store::library::persisted_library_kind(tx, node_id).await {
            Ok(k) => k,
            Err(e) => return patch_error_response(PatchNodeError::Store(e)),
        }
    } else {
        body.entity_type.clone()
    };

    commit_write_tx!(scoped_tx);
    (
        StatusCode::OK,
        Json(json!({
            "node_id":    patched.node_id,
            "type":       patched.node_type,
            "title":      patched.title,
            "scope":      patched.scope,
            "atom_count": patched.atom_count,
            "created_at": patched.created_at,
            "updated_at": patched.updated_at,
            "entity_type": entity_type,
        })),
    )
}

/// Shared HTTP-response mapping for [`PatchNodeError`] (both the early
/// field-validation return above and `patch_node_impl`'s later failure modes
/// funnel through here).
fn patch_error_response(e: PatchNodeError) -> (StatusCode, Json<Value>) {
    match e {
        PatchNodeError::NoFields => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "at least one of title or type must be provided"})),
        ),
        PatchNodeError::InvalidType => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "type must be one of: entity, decision, thread, reference"})),
        ),
        PatchNodeError::NotOwned => (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "node not found or access denied"})),
        ),
        PatchNodeError::NotFound => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "node not found"})),
        ),
        PatchNodeError::EmptyTitle => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "title must not be empty"})),
        ),
        PatchNodeError::TitleTooLong => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("title must be at most {PATCH_TITLE_CAP} chars")})),
        ),
        PatchNodeError::DecisionTypeNotAllowed => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "changing a node's type to 'decision' isn't supported via this route"
            })),
        ),
        PatchNodeError::Conflict(msg) => (StatusCode::CONFLICT, Json(json!({"error": msg}))),
        PatchNodeError::Store(e) => {
            tracing::error!(error = %e, "patch_node update error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
        }
    }
}

// ── GET /v1/nodes/{id}/candidates ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CandidatesQuery {
    pub limit: Option<i64>,
}

#[utoipa_path(
    get,
    path = "/v1/nodes/{id}/candidates",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(
        ("id"    = Uuid,         Path,  description = "Node identifier"),
        ("limit" = Option<i64>,  Query, description = "Max results (default 10, max 50)"),
    ),
    responses(
        (status = 200, description = "Ranked candidate atoms", body = Value),
        (status = 400, description = "Node has no atoms to seed retrieval from"),
        (status = 403, description = "Node not owned by caller"),
        (status = 404, description = "Node not found"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_node_candidates(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
    Query(params): Query<CandidatesQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // Node-id-anchored: scope by the caller's active account (module doc,
    // RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // ── IDOR check: node → graph reachable by caller (ADR-0007 D9) ───────────
    let graph_id = match scoped_store!(
        store,
        scoped_tx,
        node_graph_id / node_graph_id_scoped(node_id)
    ) {
        Ok(Some(id)) => id,
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "node not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "get_node_candidates ownership check error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "node not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "get_node_candidates reachability check error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // ── Load atom IDs already linked to this node ─────────────────────────────
    let existing_atom_ids = match scoped_store!(
        store,
        scoped_tx,
        node_atom_ids / node_atom_ids_scoped(node_id)
    ) {
        Ok(ids) => ids,
        Err(e) => {
            tracing::error!(error = %e, "get_node_candidates fetch atom ids error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    if existing_atom_ids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "node has no atoms to seed retrieval from"})),
        );
    }

    // ── Load embeddings for the node's atoms (up to 5) ────────────────────────
    // We average them element-wise for a centroid query vector.
    let embeddings: Vec<Vec<f32>> = match scoped_store!(
        store,
        scoped_tx,
        node_atom_embeddings / node_atom_embeddings_scoped(&existing_atom_ids, 5)
    ) {
        Ok(vecs) => vecs,
        Err(e) => {
            tracing::error!(error = %e, "get_node_candidates fetch embeddings error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    if embeddings.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "node has no atoms to seed retrieval from"})),
        );
    }

    // ── Average the embeddings (element-wise mean) ────────────────────────────
    let dim = embeddings[0].len();
    let count = embeddings.len() as f32;
    let mut avg = vec![0.0f32; dim];
    for emb in &embeddings {
        for (i, v) in emb.iter().enumerate() {
            avg[i] += v;
        }
    }
    for v in &mut avg {
        *v /= count;
    }

    // ── Clamp limit ───────────────────────────────────────────────────────────
    let limit = params.limit.unwrap_or(10).clamp(1, 50);

    // ── Vector similarity search — exclude already-linked atoms ───────────────
    let rows = match scoped_store!(
        store,
        scoped_tx,
        node_candidates / node_candidates_scoped(graph_id, &avg, &existing_atom_ids, limit)
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "get_node_candidates vector search error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // Titles-always-present: fetched through the STILL-OPEN scoped
    // transaction (CodeRabbit round 1 on #837 — `atom_titles_and_excerpts_
    // scoped` didn't exist when this route was written, so this used to run
    // ambient AFTER `commit_read_tx` below; a non-exempt consumer-plane RLS
    // role could silently see zero rows, degrading every candidate's title).
    let atom_ids: Vec<Uuid> = rows.iter().map(|c| c.atom_id).collect();
    let titles_excerpts_res = match scoped_tx.as_mut() {
        Some(tx) => store.atom_titles_and_excerpts_scoped(tx, &atom_ids).await,
        None => store.atom_titles_and_excerpts(&atom_ids).await,
    };
    let titles_excerpts: std::collections::HashMap<Uuid, (Option<String>, Option<String>)> =
        titles_excerpts_res
            .unwrap_or_default()
            .into_iter()
            .map(|(id, title, excerpt)| (id, (title, excerpt)))
            .collect();

    // Read phase complete — close the scoped transaction before the
    // best-effort snippet load below (which has no `_scoped` twin yet; see
    // the module doc's RLS section).
    commit_read_tx(scoped_tx).await;

    // ── Best-effort content snippets (dev/no-R2: content_inline is populated;
    // in prod content lives only in R2, so this is `None` there — mirrors the
    // same best-effort pattern used by entity_resolution::gather_facts and
    // infer_expertise::load_atom_snippets). Frontend falls back to the
    // humanized source type + date when `snippet` is null/empty. Unscoped:
    // no `atom_inline_content_scoped` twin exists yet — post-OPS-4 this
    // degrades to null snippets until it lands. TODO(org-tenancy).
    let content_by_atom: std::collections::HashMap<Uuid, String> = store
        .atom_inline_content(&atom_ids)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "get_node_candidates snippet load failed; returning null snippets");
            Vec::new()
        })
        .into_iter()
        .filter_map(|(atom_id, content, _source_type, _ts, header_len)| {
            content.map(|c| (atom_id, crate::store::strip_header_prefix(&c, header_len).to_string()))
        })
        .collect();

    let candidates: Vec<Value> = rows
        .into_iter()
        .map(|c| {
            let (title, excerpt) = titles_excerpts.get(&c.atom_id).cloned().unwrap_or((None, None));
            let body = content_by_atom.get(&c.atom_id);
            // `snippet` falls back to the persisted excerpt when there is no
            // inline body to collapse (the common case post-R2-cutover) —
            // never bare `null` just because `content_inline` is empty.
            let snippet = body
                .map(|content| collapse_snippet(content, CANDIDATE_SNIPPET_CHARS))
                .filter(|s| !s.is_empty())
                .or_else(|| excerpt.clone());
            let display_title = crate::services::atom_title::display_title(
                crate::services::atom_title::DisplayTitleInput {
                    title: title.as_deref(),
                    excerpt: excerpt.as_deref(),
                    stripped_body: body.map(String::as_str),
                    source_type: &c.source_type,
                    channel_name: None,
                    author_name: None,
                    timestamp: c.timestamp,
                },
            );
            json!({
                "atom_id":     c.atom_id,
                "source_type": c.source_type,
                "source_id":   c.source_id,
                "timestamp":   c.timestamp,
                "participants": c.participants,
                "similarity":  c.similarity,
                "snippet":     snippet,
                "title":       display_title,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(candidates)))
}

// ── GET /v1/candidates — decision review queue (CS-069) ───────────────────────
//
// Surfaces `type = 'decision'` reefs the extractor created with
// `body.review_status = 'pending'`, account-scoped, newest first. The
// `/review` screen renders these as confirm/reject cards.
//
// Keyset-paginated as of 2026-08-12, bringing it in line with the two sections
// it sits beside on `/review`. It previously took only a `limit`, so the web
// "load more" re-asked with a bigger one and the queue became unreachable past
// the 200 clamp — a queue larger than that could not be worked to empty at all.
// Response is now an envelope (`{items, has_more, next_cursor}`) rather than a
// bare array, so callers can tell a full page from a final one.

/// `created_at|node_id` keyset cursor. Same shape and opaque-to-callers
/// contract as `routes::relationships`' `encode_edges_cursor` and
/// [`encode_merge_review_cursor`] — a local twin because those helpers are
/// private to their own modules.
fn encode_candidate_cursor(created_at: chrono::DateTime<chrono::Utc>, node_id: Uuid) -> String {
    format!("{}|{node_id}", created_at.to_rfc3339())
}

fn decode_candidate_cursor(cursor: &str) -> Option<(chrono::DateTime<chrono::Utc>, Uuid)> {
    let (ts, id) = cursor.split_once('|')?;
    let ts = chrono::DateTime::parse_from_rfc3339(ts).ok()?.with_timezone(&chrono::Utc);
    let id = Uuid::parse_str(id).ok()?;
    Some((ts, id))
}

/// Page size for the decision queue: default 50, clamped 1..200 — the same
/// convention as the relationship and merge-review lists.
///
/// A named function rather than an inline `unwrap_or(..).clamp(..)` so a test
/// can exercise the REAL bound. The two unit tests that used to cover this
/// re-implemented the expression in the test body and had drifted to asserting
/// `unwrap_or(10).clamp(1, 50)` — neither the route's default nor its ceiling,
/// so they passed while describing a route that did not exist.
fn candidate_page_limit(raw: Option<i64>) -> i64 {
    raw.unwrap_or(50).clamp(1, 200)
}

#[derive(Debug, Deserialize)]
pub struct ReviewQueueQuery {
    pub limit: Option<i64>,
    /// Opaque keyset cursor from a previous response's `next_cursor`.
    pub cursor: Option<String>,
}

#[utoipa_path(
    get,
    path = "/v1/candidates",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(
        ("limit" = Option<i64>, Query, description = "Page size (default 50, max 200)"),
        ("cursor" = Option<String>, Query, description = "Keyset cursor from a previous next_cursor"),
    ),
    responses(
        (status = 200, description = "One keyset page of pending decision reefs", body = Value),
        (status = 400, description = "Malformed cursor"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_decision_candidates(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<ReviewQueueQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    let limit = candidate_page_limit(params.limit);
    let cursor = match params.cursor.as_deref() {
        None => None,
        Some(c) => match decode_candidate_cursor(c) {
            Some(c) => Some(c),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "malformed cursor"})),
                )
            }
        },
    };

    // Read scope (settings 2.12): only oceans this caller can READ. The
    // account alone is not a scope in an org — every member's private ocean
    // shares it — and the tenant connection owns the tables, so
    // `member_ocean_isolation` RLS does not narrow this read in production.
    let readable = match ReachabilityResolver::readable_graph_ids(
        tenant.decision.as_ref(),
        store.as_ref(),
        user.user_id,
    )
    .await
    {
        Ok(ids) => ids,
        Err(e) => {
            tracing::error!(error = %e, "list_decision_candidates reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // Account-anchored listing: the caller's active account is both the query
    // parameter and the scope (module doc, RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // Fetch one extra row to compute `has_more`/`next_cursor` without a second
    // query — same trick the relationship and merge-review lists use.
    let result = scoped_store!(
        store,
        scoped_tx,
        list_pending_decision_candidates
            / list_pending_decision_candidates_scoped(user.account_id, &readable, cursor, limit + 1)
    );
    let mut cands = match result {
        Ok(c) => c,
        Err(e) => {
            commit_read_tx(scoped_tx).await;
            tracing::error!(error = %e, "list_decision_candidates error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    let has_more = cands.len() as i64 > limit;
    cands.truncate(limit as usize);
    // Cursor comes from the last row RENDERED, so the probe row is the first
    // row of the next page rather than a skipped one.
    let next_cursor = if has_more {
        cands.last().map(|c| encode_candidate_cursor(c.created_at, c.node_id))
    } else {
        None
    };

    // Structural impact for the consequence-first card copy, batched per
    // ocean (candidates are account-wide). Best-effort: a failed fetch
    // degrades the copy, never the list.
    let mut by_graph: std::collections::HashMap<Uuid, Vec<Uuid>> = std::collections::HashMap::new();
    for c in &cands {
        by_graph.entry(c.graph_id).or_default().push(c.node_id);
    }
    let mut impact_by_node: std::collections::HashMap<Uuid, crate::review_policy::NodeImpact> =
        std::collections::HashMap::new();
    for (g, ids) in &by_graph {
        match scoped_store!(
            store,
            scoped_tx,
            node_structural_impact / node_structural_impact_scoped(*g, ids)
        ) {
            Ok(rows) => impact_by_node.extend(rows.into_iter().map(|i| (i.node_id, i))),
            Err(e) => {
                tracing::warn!(error = %e, graph_id = %g, "candidate impact lookup error");
            }
        }
    }
    commit_read_tx(scoped_tx).await;

    let out: Vec<Value> = cands
        .into_iter()
        .map(|c| {
            let imp = impact_by_node.get(&c.node_id);
            let deadline_at = c
                .pending_since
                .or(Some(c.created_at))
                .map(|q| q + chrono::Duration::days(crate::review_policy::REVIEW_DEADLINE_DAYS));
            json!({
                "node_id":         c.node_id,
                "graph_id":        c.graph_id,
                "title":           c.title,
                "rationale":       c.rationale,
                "confidence":      c.confidence,
                "decided_at":      c.decided_at,
                "created_at":      c.created_at,
                "source_atom_ids": c.source_atom_ids,
                "impact": {
                    "degree":        imp.map(|i| i.degree).unwrap_or(0),
                    "corals":        imp.map(|i| i.coral_count).unwrap_or(0),
                    // Never CLAIM contradiction without data — a failed
                    // impact fetch previously defaulted this to true and
                    // stamped "contradicts an existing fact" on every card
                    // (bloat-RCA amplifier, 2026-08-05).
                    "contradiction": imp.map(|i| i.conflict_involved).unwrap_or(false),
                },
                "pending_since":   c.pending_since,
                "deadline_at":     deadline_at,
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(json!({"items": out, "has_more": has_more, "next_cursor": next_cursor})),
    )
}

// ── POST /v1/nodes/{id}/confirm · /reject — decision verdict (CS-069) ──────────
//
// Confirm promotes the candidate to a live decision reef; reject marks it so it
// drops out of the queue (and the extractor can learn not to re-surface it).
// Both flow through `set_node_review_status`, whose single guarded UPDATE is the
// IDOR + pending-state check: anything not a pending decision owned by the
// caller matches no row → 404.

/// Optional edits applied when **confirming** a candidate (CS-069b inline edit).
/// All fields optional: an unedited confirm sends `{}`.
#[derive(Debug, Default, Deserialize)]
pub struct ConfirmRequest {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
    #[serde(default)]
    pub decided_at: Option<String>,
}

/// Optional reason captured when **rejecting** (CS-069b reject-with-reason),
/// e.g. "wrong" | "not_a_decision" | "duplicate".
#[derive(Debug, Default, Deserialize)]
pub struct RejectRequest {
    #[serde(default)]
    pub reason: Option<String>,
}

/// Errors from [`confirm_node_impl`] / [`reject_node_impl`]. `set_node_review_status`'s
/// single guarded UPDATE (IDOR check + pending-decision state guard fused into
/// one `WHERE`) means a caller-mismatch, wrong-state, or genuinely-absent node
/// are indistinguishable from here — all collapse to `NotFound`, exactly as
/// the pre-extraction handler behaved (404, never 403, for this pair of
/// routes).
pub enum ReviewError {
    NotFound,
    Store(crate::store::StoreError),
}

/// Shared guarded-UPDATE + read-back for confirm/reject, extracted from the
/// pre-Phase-3 `review_decision` helper. `body_merge` is shallow-merged into
/// the node's `body` JSONB by `set_node_review_status`; on success this reads
/// the merged state back via `Store::get_decision_review` (Phase 3 addition)
/// so callers — the HTTP routes below and the MCP `confirm_reef`/`reject_reef`
/// tools — get the authoritative post-merge `rationale`/`decided_at`, not just
/// an echo of what THIS call happened to pass.
async fn review_node_impl_scoped(
    store: &dyn crate::store::Store,
    scoped_tx: &mut ScopedTx,
    writer: &ReefWriter<'_>,
    node_id: Uuid,
    body_merge: Value,
    title: Option<&str>,
) -> Result<crate::store::DecisionReview, ReviewError> {
    // Write scope first. The guarded UPDATE below only checks the account,
    // which every ocean in an org shares. A refusal is `NotFound`, the same
    // answer a missing or already-decided reef gets.
    if !node_writable_by(store, scoped_tx, node_id, writer).await.map_err(ReviewError::Store)? {
        return Err(ReviewError::NotFound);
    }
    let ok = scoped_store!(
        store,
        *scoped_tx,
        set_node_review_status
            / set_node_review_status_scoped(node_id, writer.account_id, &body_merge.to_string(), title)
    )
    .map_err(ReviewError::Store)?;
    if !ok {
        return Err(ReviewError::NotFound);
    }
    scoped_store!(
        store,
        *scoped_tx,
        get_decision_review / get_decision_review_scoped(node_id)
    )
    .map_err(ReviewError::Store)?
    .ok_or(ReviewError::NotFound)
}

/// Confirm a pending decision candidate (CS-069), optionally editing
/// `title`/`rationale`/`decided_at` inline (CS-069b). Extracted (precedent:
/// `ingest::submit`) so the HTTP route and the MCP
/// `confirm_reef` tool share one implementation. Unscoped entry point, kept
/// for the MCP tool (see [`patch_node_impl`]'s note); the HTTP route goes
/// through [`confirm_node_impl_scoped`].
pub(crate) async fn confirm_node_impl(
    store: &dyn crate::store::Store,
    writer: &ReefWriter<'_>,
    node_id: Uuid,
    req: ConfirmRequest,
    via: &str,
) -> Result<crate::store::DecisionReview, ReviewError> {
    confirm_node_impl_scoped(store, &mut None, writer, node_id, req, via).await
}

/// Scoped twin of [`confirm_node_impl`] (org-tenancy OPS-4 phase B.2).
pub(crate) async fn confirm_node_impl_scoped(
    store: &dyn crate::store::Store,
    scoped_tx: &mut ScopedTx,
    writer: &ReefWriter<'_>,
    node_id: Uuid,
    req: ConfirmRequest,
    via: &str,
) -> Result<crate::store::DecisionReview, ReviewError> {
    let mut merge = serde_json::Map::new();
    merge.insert("review_status".into(), json!("confirmed"));
    stamp_reviewer(&mut merge, Some(writer.user_id), via);
    if let Some(r) = &req.rationale {
        merge.insert("rationale".into(), json!(r));
    }
    if let Some(d) = &req.decided_at {
        merge.insert("decided_at".into(), json!(d));
    }
    review_node_impl_scoped(
        store,
        scoped_tx,
        writer,
        node_id,
        Value::Object(merge),
        req.title.as_deref(),
    )
    .await
}

/// Reject a pending decision candidate (CS-069), optionally recording a reason
/// (CS-069b). Extracted for the same reason as [`confirm_node_impl`], and
/// split into unscoped/scoped entry points the same way.
pub(crate) async fn reject_node_impl(
    store: &dyn crate::store::Store,
    writer: &ReefWriter<'_>,
    node_id: Uuid,
    req: RejectRequest,
    via: &str,
) -> Result<crate::store::DecisionReview, ReviewError> {
    reject_node_impl_scoped(store, &mut None, writer, node_id, req, via).await
}

/// Scoped twin of [`reject_node_impl`] (org-tenancy OPS-4 phase B.2).
pub(crate) async fn reject_node_impl_scoped(
    store: &dyn crate::store::Store,
    scoped_tx: &mut ScopedTx,
    writer: &ReefWriter<'_>,
    node_id: Uuid,
    req: RejectRequest,
    via: &str,
) -> Result<crate::store::DecisionReview, ReviewError> {
    let mut merge = serde_json::Map::new();
    merge.insert("review_status".into(), json!("rejected"));
    stamp_reviewer(&mut merge, Some(writer.user_id), via);
    if let Some(r) = &req.reason {
        merge.insert("reject_reason".into(), json!(r));
    }
    review_node_impl_scoped(store, scoped_tx, writer, node_id, Value::Object(merge), None).await
}

/// Stamp the HUMAN actor onto a decision verdict's body merge — who resolved
/// it, when, and via which surface ("web" | "mcp" | later "slack"). The
/// review-conversion metric reads these keys; their absence combined with
/// `auto_resolved` marks a machine resolution.
fn stamp_reviewer(merge: &mut serde_json::Map<String, Value>, reviewed_by: Option<Uuid>, via: &str) {
    if let Some(u) = reviewed_by {
        merge.insert("reviewed_by".into(), json!(u.to_string()));
    }
    merge.insert("reviewed_at".into(), json!(chrono::Utc::now().to_rfc3339()));
    merge.insert("via".into(), json!(via));
}

/// Shared HTTP-response mapping for the confirm/reject routes below —
/// unchanged from the pre-extraction `review_decision` helper's behavior.
fn review_response(result: Result<crate::store::DecisionReview, ReviewError>, node_id: Uuid) -> (StatusCode, Json<Value>) {
    match result {
        Ok(_) => (StatusCode::OK, Json(json!({"node_id": node_id}))),
        Err(ReviewError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no pending decision candidate with that id"})),
        ),
        Err(ReviewError::Store(e)) => {
            tracing::error!(error = %e, "set_node_review_status error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
        }
    }
}

#[utoipa_path(
    post,
    path = "/v1/nodes/{id}/confirm",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "Decision node identifier")),
    responses(
        (status = 200, description = "Decision confirmed"),
        (status = 404, description = "Not a pending decision the caller may review"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn confirm_node(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
    Json(req): Json<ConfirmRequest>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    // Caller-account scope — the same account the guarded UPDATE checks
    // (module doc, RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());
    let result = confirm_node_impl_scoped(store.as_ref(), &mut scoped_tx, &writer, node_id, req, "web").await;
    if result.is_ok() {
        commit_write_tx!(scoped_tx);
    }
    review_response(result, node_id)
}

#[utoipa_path(
    post,
    path = "/v1/nodes/{id}/reject",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "Decision node identifier")),
    responses(
        (status = 200, description = "Decision rejected"),
        (status = 404, description = "Not a pending decision the caller may review"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn reject_node(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
    Json(req): Json<RejectRequest>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    // Caller-account scope — the same account the guarded UPDATE checks
    // (module doc, RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());
    let result = reject_node_impl_scoped(store.as_ref(), &mut scoped_tx, &writer, node_id, req, "web").await;
    if result.is_ok() {
        commit_write_tx!(scoped_tx);
    }
    review_response(result, node_id)
}

// ── POST /v1/nodes/{id}/promote — publish a reef into a wider ocean (ADR-0006) ──
//
// One-way, copy-on-promote: the source reef stays authoritative in its origin
// ocean; a provenance-tagged copy is published into the target shared ocean.
// Re-promoting the same reef into the same ocean re-publishes (updates) the copy.
//
// Corals (the reef's atoms) can be selected per-promote via `coral_ids`:
// omitted/null copies all of them (back-compat default), an explicit subset
// copies exactly those — the route validates every id is actually attached
// to the source reef first (400, nothing promoted, otherwise). Coral copies
// are additive/widening only: a narrower re-promote never removes corals an
// earlier, wider promote already copied into the target.
//
// §4.3 (docs/plans/graph-temporal-memory.md): reef-only promotion (`[]`, or
// omitted against a reef with zero attached corals) is REJECTED with 400 —
// under §4.2's native derivation a coral-less reef derives nothing in the
// target and is uncitable there. The privacy case (publish the conclusion,
// withhold the argument) is served by a non-empty SUBSET instead.

/// Body for `POST /v1/nodes/{id}/promote`: the shared ocean to publish into,
/// plus an optional per-coral selection.
#[derive(Debug, Deserialize)]
pub struct PromoteRequest {
    pub target_graph_id: Uuid,
    /// Corals (atoms) to copy alongside the reef. `None`/omitted copies every
    /// coral attached to the source reef (back-compat default) — rejected
    /// with 400 if the reef has none. `Some(vec![])` (reef-only promote) is
    /// ALWAYS rejected with 400 (§4.3 — a coral-less reef derives nothing and
    /// is uncitable in a shared ocean). `Some(ids)` copies exactly those —
    /// every id must already be attached to the source reef, and `ids` must
    /// be non-empty, or the whole request is rejected with 400 before
    /// anything is promoted.
    #[serde(default)]
    pub coral_ids: Option<Vec<Uuid>>,
}

#[utoipa_path(
    post,
    path = "/v1/nodes/{id}/promote",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "Source reef (node) identifier")),
    responses(
        (status = 200, description = "Reef re-published into the target ocean"),
        (status = 201, description = "Reef promoted into the target ocean"),
        (status = 400, description = "Target is not a shared (team/org) ocean, or coral_ids contains an id not attached to the source reef"),
        (status = 403, description = "Promotion cannot cross databases — the source reef must live in the caller's own ocean inside this organization's tenant database"),
        (status = 404, description = "Reef or target ocean not owned by caller"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn promote_node(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
    Json(_req): Json<PromoteRequest>,
) -> (StatusCode, Json<Value>) {
    // ── Cross-database refusal (member-private-oceans PR 6, plan §2.5) ──────
    // An org-bound request may only name a reef in the caller's own private
    // MEMBER ocean in THIS org's tenant database (gate 1,
    // `is_owned_active_member_ocean`, resolved on the tenant pool). Anything
    // else is refused at the database boundary with a 403. Personal requests
    // skip this `if`.
    if let Some(org_ctx) = tenant.org_context() {
        let org_ctx = org_ctx.clone();
        match route_org_promotion(
            || resolve_tenant_node_graph(&org_ctx, node_id),
            user.user_id,
        )
        .await
        {
            Ok(PromotionRoute::SamePool) => {}
            Ok(PromotionRoute::Denied) => return cross_database_promotion_denied(),
            Err(e) => {
                tracing::error!(error = %e, "promote_node tenant source-graph lookup error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    }

    // After the cross-database refusal, so a consumer-plane source under an
    // org context is still answered by the database boundary (the 403 the
    // cross_tenant_probe_pg suite pins), not masked by this product gate.
    return (StatusCode::CONFLICT,Json(json!({"error":"Select exact retained passages using /sources/publications","code":"selected_evidence_consent_required"})));
}

// ── Intra-DB promotion fork (member-private-oceans PR 6, plan §2.5) ──────────
//
// The org-tenant promotion branches used to delegate EVERY org-context
// promotion to the cross-DB `services::promotion` unconditionally, because
// the only valid source was ever the caller's personal ocean on the consumer
// plane (a member ocean living in the tenant DB did not yet exist). Now that
// it can, the source's actual location is a question of fact that must be
// answered BEFORE delegating — resolved directly against the TENANT pool,
// not through `resolve_node_account_id` (control-tree-only, migration
// `0086_webhook_share_bootstrap_functions`, built for callers that don't yet
// know which account owns a node — the webhook/share bootstrap paths). Here
// the account is already known (`org_ctx.account_id`, gate-verified before a
// `TenantContext` is ever built), so there is no bootstrap-deadlock to avoid,
// and pairing that function into the tenant tree would just be an extra
// migration for no behavioral gain over a scoped `SELECT`. Hence the direct
// two-hop read below, not a paired SQL function.
//
// Neither read is wrapped in `begin_account_scoped`: tenant RLS policies
// (migration 0009) exist in ENABLE-only form today and do not yet enforce —
// these two reads work only because the tenant pool currently connects as
// the table OWNER, and owner connections are exempt from ENABLE-only RLS
// regardless of any `app.account_id` GUC. The explicit `account_id = $N`
// equality in each query is therefore the ONLY scope actually in effect
// right now — same convention as `tenancy::reachability`'s
// `tenant_graph_reachable_by_user`. At Phase 3 (tenant pool switched to a
// non-owner role, per the cutover runbook), ENABLE-only becomes enforcing:
// an unscoped read here would then return no rows and fail closed, but
// these two reads are already scoped by `account_id`, so they keep working
// unchanged. Do not remove the explicit equality on the theory that RLS
// will "handle it" — until Phase 3 lands, RLS handles nothing here.

/// `promote_node`'s routing read: resolve the source NODE's graph directly on
/// the org's tenant pool via a `nodes -> graphs` two-hop join. `None` when the
/// node does not exist in THIS tenant database at all — the ordinary case,
/// meaning the reef is (as before this PR) the caller's personal ocean on the
/// consumer plane, so the caller falls back to the unmodified cross-DB path.
///
/// Binds into `tenancy::reachability::GraphRow` — the SAME tuple type its
/// tenant-pool queries use for this exact column shape, reused rather than
/// declared again here (see that type's doc comment for why: two independent
/// tuples of the same shape would let a column reorder in either copy compile
/// silently and hand gate 1 the wrong field).
///
/// `my_team_role` is a literal `NULL::text`, not a subquery: this routing
/// read takes no caller `user_id` at all (only `node_id`/`org_ctx`), so there
/// is no role to compute — see `store::Graph::my_team_role`'s doc comment for
/// the general rule.
async fn resolve_tenant_node_graph(
    org_ctx: &crate::tenancy::runtime::TenantContext,
    node_id: Uuid,
) -> Result<Option<crate::store::Graph>, crate::store::StoreError> {
    let row: Option<TenantGraphRow> = sqlx::query_as(
        "SELECT g.id, g.name, g.kind, g.is_primary, g.created_at, g.owner_user_id, g.archived_at, \
         (SELECT t.id FROM teams t WHERE t.graph_id = g.id ORDER BY t.created_at ASC, t.id ASC LIMIT 1), \
         NULL::text \
         FROM nodes n JOIN graphs g ON g.id = n.graph_id \
         WHERE n.node_id = $1 AND g.account_id = $2",
    )
    .bind(node_id)
    .bind(org_ctx.account_id)
    .fetch_optional(&org_ctx.pool)
    .await?;
    Ok(row.map(tenant_row_to_graph))
}

/// `promote_source`'s routing read: resolve the source GRAPH directly on the
/// org's tenant pool by id — no node hop needed, the route already carries
/// `graph_id` as its own path parameter. `None` when the graph does not exist
/// in THIS tenant database at all (or belongs to a different account, which
/// cannot happen for a tenant pool — kept as an explicit equality anyway for
/// the same reason `resolve_tenant_node_graph` carries one). Same shared
/// `GraphRow`/`row_to_graph` as that function, for the same reason —
/// including the literal `NULL::text` for `my_team_role` (no caller
/// `user_id` on this path either).
async fn resolve_tenant_graph(
    org_ctx: &crate::tenancy::runtime::TenantContext,
    graph_id: Uuid,
) -> Result<Option<crate::store::Graph>, crate::store::StoreError> {
    let row: Option<TenantGraphRow> = sqlx::query_as(
        "SELECT g.id, g.name, g.kind, g.is_primary, g.created_at, g.owner_user_id, g.archived_at, \
         (SELECT t.id FROM teams t WHERE t.graph_id = g.id ORDER BY t.created_at ASC, t.id ASC LIMIT 1), \
         NULL::text \
         FROM graphs g WHERE g.id = $1 AND g.account_id = $2",
    )
    .bind(graph_id)
    .bind(org_ctx.account_id)
    .fetch_optional(&org_ctx.pool)
    .await?;
    Ok(row.map(tenant_row_to_graph))
}

/// Gate 1 (plan §2.5): true only for the caller's OWN, non-archived,
/// `kind='member'` ocean — the one and only source shape this PR routes
/// same-pool instead of through the cross-DB service. Without this exact
/// check (kind AND owner AND non-archived, not kind alone), member A could
/// promote member B's reef merely by naming its node/graph id — reachability
/// re-confirms this same predicate independently once the same-pool body
/// below runs (`ReachabilityResolver`'s tenant arm, org-tenancy PR 2/plan
/// §2.2), but the ROUTING decision itself must not fall through on a false
/// positive, so it is checked here explicitly rather than assumed. Gate 2
/// (target must be `kind IN ('team','org')`) and gate 3 (target
/// reachability) are unchanged, existing checks in that same-pool body — see
/// the call sites below for exactly where.
fn is_owned_active_member_ocean(graph: &crate::store::Graph, user_id: Uuid) -> bool {
    graph.kind == "member" && graph.owner_user_id == Some(user_id) && graph.archived_at.is_none()
}

/// The routing decision for an org-context promotion (member-private-oceans
/// PR 6). `promote_node` and `promote_source` both match on this rather than
/// each re-implementing gate 1's condition inline — a future change to the
/// gate (e.g. a second owner-scoped kind) is then one match arm, not two
/// separately-maintained copies that could drift.
enum PromotionRoute {
    /// Gate 1 satisfied — fall through into the SAME-POOL body below instead
    /// of returning.
    SamePool,
    /// Everything else (not resident in THIS tenant DB at all — most commonly
    /// the caller's personal ocean on the consumer plane — a different
    /// member's ocean, or archived). Refused: promotion never crosses a
    /// database boundary.
    Denied,
}

/// Resolve [`PromotionRoute`] for an org-context promotion by running
/// `resolve_source` (either [`resolve_tenant_node_graph`] or
/// [`resolve_tenant_graph`], threaded in as a closure since the two callers
/// resolve their source differently — one hops through `nodes`, the other
/// reads `graphs` directly by the id the route already carries) and applying
/// gate 1 to whatever it finds. `Err` only for a genuine tenant-pool DB
/// failure resolving the source — callers map that to a 500 themselves (this
/// helper never builds a response, so it stays reusable for either handler,
/// or a future third org-promotion route).
async fn route_org_promotion<F, Fut>(
    resolve_source: F,
    user_id: Uuid,
) -> Result<PromotionRoute, crate::store::StoreError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Option<crate::store::Graph>, crate::store::StoreError>>,
{
    let src_graph = resolve_source().await?;
    Ok(match src_graph {
        Some(g) if is_owned_active_member_ocean(&g, user_id) => PromotionRoute::SamePool,
        _ => PromotionRoute::Denied,
    })
}

/// The single 403 every org-plane promotion route returns when its source is
/// not resident in this org's own tenant database.
///
/// Deliberately the same response for "your personal ocean, on the other
/// plane", "another member's ocean" and "archived": the caller learns the
/// request is refused, not which of those a given graph id happens to be —
/// the old cross-DB path is exactly how a member could otherwise probe for
/// the existence of ids on a plane they cannot read.
fn cross_database_promotion_denied() -> (StatusCode, Json<Value>) {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "promotion cannot cross databases — an org promotion's \
                      source must be your own ocean inside this organization"
        })),
    )
}

// ── Legacy source promotion entry point ────────────────────────────────
// Delegates to a durable, evidence-scoped one-time source publication.
// Unlike topic promotion, it derives destination topics from the selected
// source rather than copying summaries assembled from other private sources.

/// `true` — [`PromoteSourceRequest::with_corals`]'s §4.3 default. A plain
/// `#[serde(default)]` would resolve to `bool::default() == false`, which is
/// exactly the reef-only default §4.3 retires.
fn default_true() -> bool {
    true
}

/// Body for `POST /v1/graphs/{graph_id}/promote-source`.
#[derive(Debug, Deserialize)]
pub struct PromoteSourceRequest {
    pub source_id: String,
    pub target_graph_id: Uuid,
    /// Copy the source's corals (with embeddings) into the target ocean and
    /// link them to the promoted reefs — so every reef carries its evidence.
    ///
    /// §4.3 (docs/plans/graph-temporal-memory.md): defaults to **true**, and
    /// an explicit `false` is now REJECTED with 400 rather than honoured.
    /// This is a behavior change from the old reef-only-by-default: under
    /// §4.2's native derivation a coral-less reef derives nothing in the
    /// target and is uncitable there, and `promote-source` fans that defect
    /// out across every reef grown from the source at once — the exact
    /// class of request `promote_node`'s own `coral_ids` non-empty rule
    /// already refuses one reef at a time. The privacy case (publish the
    /// conclusion, withhold the argument) is still available: promote the
    /// individual reef(s) via `POST /v1/nodes/{id}/promote` with an explicit
    /// non-empty `coral_ids` subset instead of using this whole-source route.
    #[serde(default = "default_true")]
    pub with_corals: bool,
}

#[utoipa_path(
    post,
    path = "/v1/graphs/{graph_id}/promote-source",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("graph_id" = Uuid, Path, description = "Source ocean identifier")),
    responses(
        (status = 202, description = "Source publication accepted; poll the returned job or connection"),
        (status = 400, description = "Target is not a shared ocean"),
        (status = 403, description = "Promotion cannot cross databases — the source ocean must live in the caller's own ocean inside this organization's tenant database"),
        (status = 404, description = "Ocean or source not reachable"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn promote_source(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Json(_req): Json<PromoteSourceRequest>,
) -> (StatusCode, Json<Value>) {
    if let Some(org) = tenant.org_context() {
        match route_org_promotion(|| resolve_tenant_graph(org, graph_id), user.user_id).await {
            Ok(PromotionRoute::SamePool) => {},
            Ok(PromotionRoute::Denied) => return cross_database_promotion_denied(),
            Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error":"Could not resolve source workspace"}))),
        }
    }
    // After the cross-database refusal, same ordering and reason as promote_node.
    return (StatusCode::CONFLICT,Json(json!({"error":"Select exact retained passages using /sources/publications","code":"selected_evidence_consent_required"})));
}

// ── DELETE /v1/graphs/{graph_id}/atoms/{atom_id} — unpublish a coral (§4.9) ──
//
// Explicit, targeted retraction: removes ONE coral from a shared ocean,
// along with its `node_atoms` link, its R2 blob, and — if that coral was the
// LAST one keeping its target reef non-empty — the reef itself (§4.3/§4.9:
// `nodes` has no FK to `atoms`, so a natively-derived reef would otherwise
// survive its own evidence). See `services::retraction::unpublish_atom`'s
// module doc for the full ordering/idempotency contract (this is, per the
// plan, "the hardest transaction in the system").
//
// Never touches the SOURCE ocean — unpublish is purely a target-side
// operation, matching ADR-0006's one-way copy-on-promote model; the person
// who originally created the content in their own ocean is unaffected.
//
// Idempotent: retracting an already-gone (or never-existing) atom id is a
// 200, not a 404 — matching `unpublish_atom`'s own "retracting something
// already retracted is success" contract, so a client can retry blindly on
// any transport failure without first checking whether its previous attempt
// landed.
//
// AUTHORIZATION: **only the ORIGINAL PROMOTER may retract.** Two gates, in
// this order:
//
//   1. `ReachabilityResolver::graph_reachable_by_user` — unchanged, and now a
//      PRECONDITION rather than the authorization itself. A caller who cannot
//      reach the ocean still gets a 404 that does not distinguish "no such
//      ocean" from "not yours", so this endpoint leaks nothing new about
//      oceans the caller has no business knowing exist.
//   2. `services::retraction::unpublish_atom`'s own promoter check, executed
//      in application SQL INSIDE the transaction that performs the deletes.
//      Being able to read/curate a shared ocean is NOT authority to reverse
//      somebody else's disclosure decision — see that module's
//      "Authorization" section for the full argument, which column the
//      promoter is read from, and why the same rule holds identically on the
//      same-pool and cross-DB org promotion paths.
//
// Two consequences worth reading before changing anything here:
//
//   * There is deliberately NO ocean-owner / admin override. That variant was
//     considered and explicitly not chosen. Adding one is a product decision.
//   * Content with NO recorded promoter — cross-DB org promotions made before
//     migration 0111, whose provenance is not backfillable by construction —
//     FAILS CLOSED with a distinct `provenance_unavailable` error rather than
//     falling back to the old "anyone in the ocean" behaviour. That content is
//     currently unretractable through this endpoint; that is a known, logged,
//     deliberate trade (destroying the wrong content is irreversible, refusing
//     to is not).
//
// RLS SCOPING (FIXED — the gap here was REAL, not merely theoretical):
// `unpublish_atom` and `revoke_source_publication` (the sibling verb,
// `POST .../revoke-publication` below) both now open their own transaction
// through `tenancy::scope::begin_account_scoped` (account_id resolved
// internally via the `resolve_graph_account_id` bootstrap, same pattern as
// this file's own `graph_account_id` call sites) instead of a bare
// `pool.begin()` — each still fully owns its transaction start-to-finish
// (the ordering guarantee in `services::retraction`'s module doc is
// unaffected: `begin_account_scoped` is `pool.begin()` plus one extra
// `set_config`, nothing more). This was NOT harmless by the time it was
// fixed: `DATABASE_URL`'s user is `coralswarm_app` with `rolbypassrls = f`
// in production — the OPS-4 role switch this comment used to say "hasn't
// happened yet" already had, so the unscoped form was live-breaking
// `DELETE /v1/graphs/{graph_id}/atoms/{atom_id}` AND
// `POST /v1/graphs/{graph_id}/revoke-publication` with a hard database error
// on every call (confirmed for the latter even with `source_publications`
// completely EMPTY in production — see `services::retraction`'s module doc
// for why an empty table was never a mitigation). See that module's doc for
// the full account and `backend/tests/retraction_deletion_rls_pg.rs` for the
// app-role reproduction of both verbs.
//
// The promoter check added above deliberately does NOT lean on that
// scoping: it names `n.graph_id = $2` (and the publication's full
// `(source_id, source_graph_id, target_graph_id)` key on the revoke side)
// explicitly in its own SQL, so it enforces the same thing before and after
// the OPS-4 role switch. An authorization predicate that only holds because
// the connection happens to be scoped would be exactly the wrong thing to
// build on a connection that is documented as unscoped.

#[derive(Debug, Deserialize, Default)]
pub struct UnpublishAtomQuery {
    /// Best-effort device id for the sync tombstone op this write emits.
    /// Defaults to a fixed cloud-write marker (same convention as
    /// `services::promotion`'s `CLOUD_DEVICE_ID`) when omitted.
    #[serde(default)]
    pub device_id: Option<String>,
}

#[utoipa_path(
    delete,
    path = "/v1/graphs/{graph_id}/atoms/{atom_id}",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(
        ("graph_id" = Uuid, Path, description = "Target ocean the coral currently lives in"),
        ("atom_id" = Uuid, Path, description = "The coral (atom) to retract")
    ),
    responses(
        (status = 200, description = "Retracted (or already absent — idempotent)"),
        (status = 403, description = "Caller is not the original promoter (`not_promoter`), \
                                      or no promotion provenance is recorded for this coral so \
                                      the promoter cannot be established (`provenance_unavailable`)"),
        (status = 404, description = "Ocean not reachable by caller"),
        (status = 503, description = "Database or blob storage unavailable"),
    )
)]
pub async fn unpublish_atom_route(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((graph_id, atom_id)): Path<(Uuid, Uuid)>,
    Query(q): Query<UnpublishAtomQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "unpublish_atom reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };
    let blobs = state.r2.as_deref().map(|r| r as &dyn PromotionBlobStore);
    let device_id = q.device_id.as_deref().unwrap_or(crate::store::CLOUD_DEVICE_ID);

    // `user.user_id` — the JWT-verified acting user from `UserContext`, NOT
    // anything the request could name. There is no request field anywhere on
    // this endpoint that can influence WHO the promoter check compares
    // against; `device_id` above is a free-text query parameter but it only
    // labels the sync tombstone, and is never consulted for authorization.
    match retraction::unpublish_atom(&pool, blobs, device_id, graph_id, atom_id, user.user_id).await
    {
        Ok(outcome) => (
            StatusCode::OK,
            Json(json!({
                "graph_id": graph_id,
                "atom_id": atom_id,
                "atom_was_present": outcome.atom_was_present,
                "deleted_reef_ids": outcome.deleted_reef_ids,
            })),
        ),
        // 403, not 404: the caller already passed the reachability gate, so
        // they can see this ocean and this coral. Hiding the REASON at that
        // point protects nothing and makes a legitimate "why can't I retract
        // my team-mate's post?" indistinguishable from a bug.
        Err(e @ retraction::RetractionError::NotPromoter) => {
            tracing::warn!(error = %e, graph_id = %graph_id, atom_id = %atom_id,
                user_id = %user.user_id, "unpublish_atom refused: not the original promoter");
            (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "not_promoter",
                    "message": "only the person who promoted this coral into this ocean can \
                                retract it",
                })),
            )
        }
        // Distinct from `not_promoter` on the wire on purpose: this is a DATA
        // gap (pre-0111 cross-plane promotions recorded no promoter), not a
        // policy denial, and the two need very different follow-up.
        Err(e @ retraction::RetractionError::ProvenanceUnavailable) => {
            tracing::warn!(error = %e, graph_id = %graph_id, atom_id = %atom_id,
                user_id = %user.user_id,
                "unpublish_atom refused: no promotion provenance recorded — failing closed");
            (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "provenance_unavailable",
                    "message": "no record of who promoted this coral into this ocean, so \
                                retraction authority cannot be established; this content \
                                predates promotion-provenance tracking",
                })),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, graph_id = %graph_id, atom_id = %atom_id,
                "unpublish_atom failed — see services::retraction's module doc for the \
                 retry-safety contract; this call is safe to retry unchanged");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "unpublish failed — safe to retry"})),
            )
        }
    }
}

// ── POST /v1/graphs/{graph_id}/revoke-publication — stop future flow (§4.9) ──
//
// "Revoke" — the OTHER §4.9 verb: stops FUTURE corals flowing out of a
// published source under live sync (§4.4, not yet implemented). Published
// history is untouched by design — this flips exactly one column
// (`source_publications.revoked_at`), never deletes a row. `graph_id` in the
// path is the SOURCE ocean (matching `promote-source`'s own path-parameter
// convention, where `graph_id` names the source, not the target).
//
// Idempotent: revoking an already-revoked (or never-published) source is a
// 200, not an error.
//
// AUTHORIZATION: identical rule to `unpublish_atom_route` above, and enforced
// through the SAME `check_promoter` decision function in
// `services::retraction` so the two §4.9 verbs cannot drift apart — an
// authorization check on one entry point is worth nothing if the other is
// open. Here the recorded promoter is `source_publications.consented_by`
// (§4.5), i.e. the person who agreed to the standing publication in the first
// place. Reachability on the SOURCE ocean stays as a precondition (a caller
// who cannot reach it gets the same 404 as before); `target_graph_id` arrives
// in the request body and is deliberately NOT trusted as an authorization
// input — it only narrows WHICH publication row is looked up, and that row's
// own `consented_by` is what decides.
//
// `consented_by IS NULL` fails closed with `provenance_unavailable`, same as
// unpublish. Note that nothing in the tree WRITES `source_publications` yet
// (§4.4 live sync is unimplemented), so in a real deployment this endpoint has
// no rows to act on at all today. **This did NOT make the pre-fix RLS-scoping
// gap harmless** — confirmed empirically (see `services::retraction`'s module
// doc) that an unscoped `coralswarm_app` connection hits the SAME hard
// `42704 unrecognized configuration parameter "app.account_id"` error on a
// completely EMPTY `source_publications` table, because Postgres evaluates
// the RLS qual while planning the statement, not lazily per matched row. So
// this endpoint was NOT a working no-op before this fix — it was a 503 on
// every single call, table-empty or not.

#[derive(Debug, Deserialize)]
pub struct RevokePublicationRequest {
    pub source_id: String,
    pub target_graph_id: Uuid,
}

#[utoipa_path(
    post,
    path = "/v1/graphs/{graph_id}/revoke-publication",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("graph_id" = Uuid, Path, description = "Source ocean identifier")),
    responses(
        (status = 200, description = "Revoked (or already revoked/never published — idempotent)"),
        (status = 403, description = "Caller did not consent to this publication (`not_promoter`), \
                                      or `consented_by` is NULL so the consenting publisher cannot \
                                      be established (`provenance_unavailable`)"),
        (status = 404, description = "Source ocean not reachable by caller"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn revoke_publication(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Json(req): Json<RevokePublicationRequest>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "revoke_publication reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    // `user.user_id` — JWT-verified, same as the unpublish route. Nothing in
    // `req` can steer who the consent check compares against.
    match retraction::revoke_source_publication(
        &pool,
        &req.source_id,
        graph_id,
        req.target_graph_id,
        user.user_id,
    )
    .await
    {
        Ok(outcome) => (
            StatusCode::OK,
            Json(json!({
                "source_id": req.source_id,
                "source_graph_id": graph_id,
                "target_graph_id": req.target_graph_id,
                // Unchanged legacy field: true only when this call actually
                // flipped `revoked_at`.
                "revoked_now": outcome.revoked_now(),
                // New, additive: distinguishes the two no-op shapes that used
                // to both surface as `revoked_now: false`.
                "outcome": outcome.as_str(),
            })),
        ),
        Err(e @ retraction::RetractionError::NotPromoter) => {
            tracing::warn!(error = %e, source_graph_id = %graph_id,
                target_graph_id = %req.target_graph_id, user_id = %user.user_id,
                "revoke_publication refused: caller did not consent to this publication");
            (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "not_promoter",
                    "message": "only the person who consented to this publication can revoke it",
                })),
            )
        }
        Err(e @ retraction::RetractionError::ProvenanceUnavailable) => {
            tracing::warn!(error = %e, source_graph_id = %graph_id,
                target_graph_id = %req.target_graph_id, user_id = %user.user_id,
                "revoke_publication refused: consented_by is NULL — failing closed");
            (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "provenance_unavailable",
                    "message": "no record of who consented to this publication, so revocation \
                                authority cannot be established",
                })),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "revoke_publication store error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
        }
    }
}

// ── POST /v1/graphs/{graph_id}/promote-session — promote a coding session ────
//
// Publish ONE coding-agent session — its container row, every coral captured
// under it, and every reef those corals grew — into a shared ocean, together.
//
// Why this is a route and not N calls to `promote-source`. A coding session is
// a container of many `source_id`s: `ingest::submit` builds each checkpoint's
// id as `"cc:{session_id}:{checkpoint_id}"` and then chunk-suffixes it. Driving
// `promote-source` per checkpoint from the client would mean an unbounded fan-out
// of non-atomic calls whose partial failures the client would have to reconcile,
// and it still could not move the `coding_sessions` row — so the session would
// arrive in the target ocean as a loose pile of corals that its Sessions page
// cannot see. This route carries the container.
//
// Two deliberate differences from `promote_source`'s loop:
//
//   1. EVERY coral of the session is copied, not only those attached to a
//      promoted reef. The target's session detail reads corals back by the
//      `"cc:{session_id}:"` prefix (`Store::list_session_atoms`), so a
//      checkpoint that never grew a reef would otherwise be silently missing
//      there — a promoted session that is quietly incomplete is worse than one
//      that visibly failed. This is also why `Store::session_atom_ids` is
//      uncapped.
//   2. The session row is copied FIRST, before any reef work. Reef promotion
//      is warn-and-continue (inherited from `promote_source`, whose contract
//      is that one bad reef must not sink the sweep), so a later failure
//      leaves a session that exists in the target with some of its reefs —
//      re-runnable, every step being idempotent. The reverse order would leave
//      corals in the target with no container to find them by.
//
// Copy-on-promote (ADR-0006) throughout: the source ocean is never written to,
// and the copy keeps the same `session_id`, which is what makes a re-promote
// converge rather than duplicate.

/// Body for `POST /v1/graphs/{graph_id}/promote-session`.
#[derive(Debug, Deserialize)]
pub struct PromoteSessionRequest {
    /// The coding session's caller-supplied id (`coding_sessions.session_id`),
    /// not the row's UUID primary key — the same value the session routes and
    /// the `"cc:"` `source_id` convention key on.
    pub session_id: String,
    pub target_graph_id: Uuid,
}

#[utoipa_path(
    post,
    path = "/v1/graphs/{graph_id}/promote-session",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("graph_id" = Uuid, Path, description = "Source ocean identifier")),
    responses(
        (status = 200, description = "Coding session, its corals and its reefs promoted"),
        (status = 400, description = "Target is not a shared ocean"),
        (status = 403, description = "Promotion cannot cross databases — the source ocean must live in the caller's own ocean inside this organization's tenant database"),
        (status = 404, description = "Ocean not reachable, or no such session in it"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn promote_session(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Json(_req): Json<PromoteSessionRequest>,
) -> (StatusCode, Json<Value>) {
    // Cross-database refusal — identical shape and rationale to
    // `promote_node`'s (see there); `graph_id` is the route's own path
    // parameter, so the routing read needs no node hop.
    if let Some(org_ctx) = tenant.org_context() {
        let org_ctx = org_ctx.clone();
        match route_org_promotion(|| resolve_tenant_graph(&org_ctx, graph_id), user.user_id).await {
            Ok(PromotionRoute::SamePool) => {}
            Ok(PromotionRoute::Denied) => return cross_database_promotion_denied(),
            Err(e) => {
                tracing::error!(error = %e, "promote_session tenant source-graph lookup error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    }

    // After the cross-database refusal, same ordering and reason as promote_node.
    return (StatusCode::CONFLICT,Json(json!({"error":"Select exact retained passages using /sources/publications","code":"selected_evidence_consent_required"})));

}

// ── POST /v1/nodes/{id}/atoms — link an existing coral to a reef (curation) ───
//
// Makes the candidate-corals surface actionable: add a suggested coral to the
// reef. The coral must live in the same ocean as the reef.

/// Body for `POST /v1/nodes/{id}/atoms`: the coral to link.
#[derive(Debug, Deserialize)]
pub struct LinkAtomRequest {
    pub atom_id: Uuid,
}

#[utoipa_path(
    post,
    path = "/v1/nodes/{id}/atoms",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "Reef (node) identifier")),
    responses(
        (status = 201, description = "Coral linked to the reef"),
        (status = 400, description = "Coral is not in this reef's ocean"),
        (status = 404, description = "Reef or coral not found"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn add_node_atom(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
    Json(req): Json<LinkAtomRequest>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // Node-id-anchored write: scope by the caller's active account (module
    // doc, RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // The reef must be reachable by the caller (ADR-0007 D9).
    let node_graph = match scoped_store!(
        store,
        scoped_tx,
        node_graph_id / node_graph_id_scoped(node_id)
    ) {
        Ok(Some(g)) => g,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "add_node_atom node lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        node_graph,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "add_node_atom reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());
    require_graph_writable!(store.as_ref(), scoped_tx, node_graph, writer, "reef not found");

    // The coral must live in the same ocean as the reef (no cross-ocean links).
    match scoped_store!(
        store,
        scoped_tx,
        atom_graph_id / atom_graph_id_scoped(req.atom_id)
    ) {
        Ok(Some(g)) if g == node_graph => {}
        Ok(Some(_)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "coral is not in this reef's ocean"})),
            )
        }
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "coral not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "add_node_atom atom lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    match scoped_store!(
        store,
        scoped_tx,
        link_node_atom / link_node_atom_scoped(node_id, req.atom_id)
    ) {
        Ok(()) => {
            commit_write_tx!(scoped_tx);
            (
                StatusCode::CREATED,
                Json(json!({"linked": true, "node_id": node_id, "atom_id": req.atom_id})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "add_node_atom link error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
        }
    }
}

// ── GET /v1/nodes/{node_id} ────────────────────────────────────────────────
//
// Reef-by-id — the one basic read this file was missing. Every other reef
// operation (`PATCH`, `/atoms`, `/promote`, `/digest`, `/merge`, `/confirm`,
// `/reject`) is already id-addressable; reading the reef itself was not. A
// client holding only a `node_id` (e.g. a promoted reef's web detail page,
// which knows the reef it's showing but not which ocean it lives in) had no
// way to ask "what is this reef, and which ocean is it in" — it could only
// list an entire ocean and scan, which forces guessing the ocean from
// ambient session state ("Topic unavailable" when that guess is wrong).
//
// Deliberately does NOT widen `list_nodes_scoped`'s SELECT (or `get_node`/
// `get_node_scoped`'s) to also return `graph_id` — that value is already in
// hand for free: `ReachabilityResolver::resolve_reef_access` resolves the
// node's `graph_id` as its own first step (auth can't run without it), so
// this handler reads it from there rather than adding a column every list
// row would otherwise also pay for.
//
// Also returns `can_edit`: this GET's reachability check is broader than
// `PATCH /v1/nodes/{id}`'s write scope (see [`ReefWriter`]), so a reef this
// endpoint can show is not necessarily one `PATCH` will accept from the same
// caller — the org commons for a plain member, or a team ocean reached via
// team membership, are the concrete cases. Rather than have the client guess
// that gate from `kind` or any other proxy, the server states it directly,
// computed from the SAME predicate `patch_node` itself uses.
#[utoipa_path(
    get,
    path = "/v1/nodes/{id}",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "Reef (node) identifier")),
    responses(
        (status = 200, description = "The reef's detail", body = Value),
        (status = 404, description = "Reef not found or access denied"),
        (status = 500, description = "Internal error"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_node_by_id(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // Node-id-anchored: scope by the caller's active account (module doc, RLS
    // section) — same posture as `get_node_atoms`, which this mirrors.
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // Auth dance (node -> graph -> reachability (ADR-0007 D9) -> owning
    // account) shared with `get_node_atoms` and MCP's `get_reef` —
    // `ReachabilityResolver::resolve_reef_access`. The web client
    // string-matches this exact 404 body, so it must stay byte-identical to
    // `get_node_atoms`'s.
    let ReefAccess { graph_id, owning_account_id: _ } = match ReachabilityResolver::resolve_reef_access(
        tenant.decision.as_ref(),
        store.as_ref(),
        &mut scoped_tx,
        node_id,
        user.user_id,
    )
    .await
    {
        Ok(access) => access,
        Err(ReefAccessError::NotFound) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found or access denied"})),
            )
        }
        Err(ReefAccessError::Store(e)) => {
            tracing::error!(error = %e, "get_node_by_id lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // Same node-fields projection `list_nodes` returns per row (and the same
    // `get_node`/`get_node_scoped` Store call MCP's `get_reef` already uses),
    // plus `graph_id` from the auth step above.
    let node = match scoped_store!(store, scoped_tx, get_node / get_node_scoped(node_id)) {
        Ok(Some(n)) => n,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "get_node_by_id get_node error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // `can_edit`: whether `PATCH /v1/nodes/{id}` would actually let this
    // caller write to this reef. Computed via `node_writable_by` with the
    // same REST writer `patch_node` builds — never a second, hand-rolled copy
    // of that rule. Reachability (which just authorized this GET) is broader,
    // so this is legitimately `false` for a reef the caller can see but not
    // write (the commons for a plain member).
    let can_edit = match node_writable_by(
        store.as_ref(),
        &mut scoped_tx,
        node_id,
        &ReefWriter::rest(&user, tenant.decision.as_ref()),
    )
    .await
    {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, "get_node_by_id ownership check error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // The trusted node identity getter is also used by writers. Keep its raw
    // lookup separate; this read DTO admits only independently readable support.
    let (safe_title, safe_atom_count) = if let Some(tx) = scoped_tx.as_mut() {
        let admission: Result<Option<(Option<String>, i64)>, sqlx::Error> = sqlx::query_as(
            crate::store::verdict::NODE_ADMISSION_SQL,
        ).bind(graph_id).bind(node_id).fetch_optional(&mut **tx).await;
        match admission {
            Ok(Some(row)) => row,
            Ok(None) => return (StatusCode::NOT_FOUND, Json(json!({"error":"reef not found or access denied"}))),
            Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"service unavailable"}))),
        }
    } else { (Some(node.title.clone()), node.atom_count) };

    let extras = if let Some(tx) = scoped_tx.as_mut() {
        let graphs = match ReachabilityResolver::list_reachable_graphs(
            tenant.decision.as_ref(),
            store.as_ref(),
            user.user_id,
        )
        .await
        {
            Ok(g) => g,
            Err(e) => {
                tracing::error!(error = %e, "get_node_by_id lineage reachability error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        let ids: Vec<Uuid> = graphs.iter().map(|g| g.id).collect();
        match crate::store::library::library_node_extras(tx, node_id, &ids).await {
            Ok(x) => Some(x),
            Err(e) => {
                tracing::error!(error = %e, "get_node_by_id library extras error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    } else {
        None
    };

    let evidence_counts = if let Some(tx) = scoped_tx.as_mut() {
        match crate::source_evidence::node_evidence_counts_tx(tx, graph_id, &[node_id]).await {
            Ok(mut counts) => counts.pop(),
            Err(error) => {
                tracing::error!(%error, "get_node_by_id evidence counts failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
            }
        }
    } else {
        None
    };

    commit_read_tx(scoped_tx).await;

    let extras = extras.unwrap_or(crate::store::library::LibraryNodeExtras {
        entity_type: None,
        entity_type_sticky: false,
        last_happened_at: None,
        lineage: Vec::new(),
        decisions: Vec::new(),
    });

    (
        StatusCode::OK,
        Json(json!({
            "node_id":    node.node_id,
            "type":       node.node_type,
            "title":      safe_title,
            "scope":      node.scope,
            "atom_count": safe_atom_count,
            "semantic_atom_count": evidence_counts.as_ref().map(|c| c.semantic_atom_count),
            "source_support_count": evidence_counts.as_ref().map(|c| c.source_support_count),
            "unmapped_legacy_chunk_count": evidence_counts.as_ref().map(|c| c.unmapped_legacy_chunk_count),
            "created_at": node.created_at,
            "updated_at": node.updated_at,
            "graph_id":   graph_id,
            "can_edit":   can_edit,
            "entity_type": extras.entity_type,
            "entity_type_sticky": extras.entity_type_sticky,
            "last_happened_at": extras.last_happened_at,
            "lineage": extras.lineage.iter().map(|l| json!({
                "node_id": l.node_id,
                "title": l.title,
                "graph_id": l.graph_id,
                "graph_name": l.graph_name,
            })).collect::<Vec<_>>(),
            "decisions": extras.decisions.iter().map(|d| json!({
                "node_id": d.node_id,
                "title": d.title,
                "created_at": d.created_at,
            })).collect::<Vec<_>>(),
        })),
    )
}

// ── GET /v1/nodes/{node_id}/atoms ─────────────────────────────────────────────
//
// Drill-through: the corals making up a reef. Lightweight per-row (no body
// content) so the UI can list quickly and drill into each atom for content.

/// Backstop cap on `GET /v1/nodes/{node_id}/atoms` (dogfooding gap-sweep #2,
/// item 2, 2026-07-18) — this endpoint has no pagination params yet, so
/// rather than leave the query fully unbounded (a hub reef with thousands of
/// linked corals would force a full-table fetch), cap it generously here. Well
/// above any real reef's atom count today; revisit with real pagination if
/// that stops being true.
const NODE_ATOMS_HTTP_LIMIT: i64 = 5_000;

#[utoipa_path(
    get,
    path = "/v1/nodes/{node_id}/atoms",
    tag = "nodes",
    security(("bearer_token" = [])),
    params(("node_id" = Uuid, Path, description = "Reef (node) identifier")),
    responses(
        (status = 200, description = "Atoms linked to the reef", body = Value),
        (status = 403, description = "Reef not owned by caller"),
        (status = 404, description = "Reef not found"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_node_atoms(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // Node-id-anchored: scope by the caller's active account (module doc,
    // RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // Auth dance (node -> graph -> reachability (ADR-0007 D9) -> owning
    // account) shared with `get_node_by_id` and MCP's `get_reef` —
    // `ReachabilityResolver::resolve_reef_access`.
    let ReefAccess { graph_id: _, owning_account_id: node_acct } = match ReachabilityResolver::resolve_reef_access(
        tenant.decision.as_ref(),
        store.as_ref(),
        &mut scoped_tx,
        node_id,
        user.user_id,
    )
    .await
    {
        Ok(access) => access,
        Err(ReefAccessError::NotFound) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found or access denied"})),
            )
        }
        Err(ReefAccessError::Store(e)) => {
            tracing::error!(error = %e, "get_node_atoms lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let atoms = match scoped_store!(
        store,
        scoped_tx,
        reef_atoms / reef_atoms_scoped(node_id, node_acct, NODE_ATOMS_HTTP_LIMIT)
    ) {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found or access denied"})),
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "get_node_atoms DB error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    commit_read_tx(scoped_tx).await;

    let body: Vec<Value> = atoms
        .into_iter()
        .map(|a| {
            json!({
                "atom_id":      a.atom_id,
                "source_type":  a.source_type,
                "source_id":    a.source_id,
                "timestamp":    a.timestamp,
                "participants": a.participants,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(body)))
}

// ── POST /v1/graphs/{graph_id}/dedup-entities — auto-merge duplicate reefs ─────
//
// The "auto-realize" sweep: group a graph's entity reefs that are the same thing
// under spelling variants (normalized-equal, or a tiny edit-distance for longer
// names) and merge each cluster into its richest reef via merge_nodes. Safe,
// conservative defaults (short names need an exact normalized match). Runnable
// on demand (a button) or periodically.

/// Normalize an entity name for grouping: lowercase, drop non-alphanumeric,
/// collapse runs. "Project Apollo" / "project-apollo" / "ProjectApollo" → same.
fn normalize_entity(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// Levenshtein distance (small inputs — entity names).
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// Two normalized names are "the same entity" for auto-merge: equal, or — for
/// names long enough that a 1-char difference is almost surely a typo, not a
/// distinct short name (Sam/Sams) — within edit-distance 1.
fn same_entity(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    a.len() >= 6 && b.len() >= 6 && edit_distance(a, b) <= 1
}

pub async fn dedup_entities(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "dedup_entities reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Graph-anchored: scope by the ocean's owning account (SECURITY DEFINER
    // bootstrap resolver — module doc's RLS section). Reachability above
    // already proved the graph exists.
    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "dedup_entities account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // Read transaction for the clustering scan; the merges below each run in
    // their OWN scoped transaction (`merge_nodes_isolated`) so this sweep's
    // per-merge warn-and-continue contract survives — pre-B.2, every
    // `merge_nodes` was its own private transaction anyway.
    let mut scoped_tx = begin_scoped_tx!(tenant, account_id);
    // Reachable is not writable (settings 2.10): a plain member reaches the
    // commons but may not rewrite it.
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());
    require_graph_writable!(store.as_ref(), scoped_tx, graph_id, writer, "ocean not found");
    let nodes = match scoped_store!(
        store,
        scoped_tx,
        list_nodes / list_nodes_scoped(graph_id, Some("entity"), 1000, 0)
    ) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "dedup_entities list error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    commit_read_tx(scoped_tx).await;

    // Cluster greedily: each reef joins the first existing cluster it matches by
    // normalized name (or near-match), else starts its own. Within a cluster the
    // reef with the most corals is the keeper.
    struct Cluster {
        norm: String,
        keep: Uuid,
        keep_count: i64,
        dups: Vec<Uuid>,
    }
    let mut clusters: Vec<Cluster> = Vec::new();
    for n in &nodes {
        let norm = normalize_entity(&n.title);
        if let Some(c) = clusters.iter_mut().find(|c| same_entity(&c.norm, &norm)) {
            if n.atom_count > c.keep_count {
                c.dups.push(c.keep);
                c.keep = n.node_id;
                c.keep_count = n.atom_count;
            } else {
                c.dups.push(n.node_id);
            }
        } else {
            clusters.push(Cluster {
                norm,
                keep: n.node_id,
                keep_count: n.atom_count,
                dups: vec![],
            });
        }
    }

    let mut merged_groups = 0u64;
    let mut reefs_removed = 0u64;
    for c in &clusters {
        if c.dups.is_empty() {
            continue;
        }
        merged_groups += 1;
        for dup in &c.dups {
            // F2 (review round 2): don't let the bulk dedup sweep merge past
            // an active human "keep separate" ruling recorded on the
            // judgement graph — the ORIGINAL PR2 rewire only reached the
            // ER worker and the two manual routes, leaving this bulk
            // surface still calling plain `merge_nodes[_scoped]` with no
            // judgement consulted at all. Any source counts (not just
            // human): an llm/backfill NEGATIVE equally means "already ruled
            // different", not just a human one. This does NOT make dedup a
            // judgement WRITER (still a listed bypass — see
            // `get_active_judgement_isolated`'s doc for the fail-open
            // rationale on a lookup error); it only stops it from
            // overriding an existing ruling it CAN see.
            if judgement_write_mode() == JudgementWriteMode::On {
                match get_active_judgement_isolated(&tenant, store, account_id, graph_id, c.keep, *dup)
                    .await
                {
                    Ok(Some(j)) if j.verdict == "negative" => {
                        tracing::info!(
                            keep = %c.keep, dup = %dup, judgement_id = %j.judgement_id, source = %j.source,
                            "dedup: skipping pair — an active NEGATIVE judgement already rules them different"
                        );
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(
                            keep = %c.keep, dup = %dup, error = %e,
                            "dedup: active-judgement lookup failed — proceeding with merge \
                             (dedup is not a judgement write gate; this check is best-effort)"
                        );
                    }
                }
            }
            match merge_nodes_isolated(&tenant, store, account_id, c.keep, *dup).await {
                Ok(_) => reefs_removed += 1,
                Err(e) => {
                    tracing::warn!(keep = %c.keep, dup = %dup, error = %e, "dedup: merge failed")
                }
            }
        }
    }

    (
        StatusCode::OK,
        Json(json!({ "merged_groups": merged_groups, "reefs_removed": reefs_removed })),
    )
}

// ── GET /v1/graphs/{id}/merge-suggestions — possible duplicate entity reefs ────
//
// Lower-confidence than the auto-dedup sweep: surfaces pairs that *might* be the
// same entity (one name contained in the other, or a small edit-distance) for
// the user to confirm in the Review tab — instead of merging them silently.
// Excludes pairs the user previously marked "keep separate".

// (standing merge-suggestions listing removed 2026-08-05 — see lib.rs comment)

#[derive(Deserialize)]
pub struct DismissMergeRequest {
    pub a: Uuid,
    pub b: Uuid,
}

pub async fn dismiss_merge_suggestion(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Json(req): Json<DismissMergeRequest>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "dismiss_merge reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Graph-anchored write: scope by the ocean's owning account (SECURITY
    // DEFINER bootstrap resolver — module doc's RLS section).
    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "dismiss_merge account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx = begin_scoped_tx!(tenant, account_id);
    // Reachable is not writable (settings 2.10): a plain member reaches the
    // commons but may not rewrite it.
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());
    require_graph_writable!(store.as_ref(), scoped_tx, graph_id, writer, "ocean not found");

    if let Err(e) = scoped_store!(
        store,
        scoped_tx,
        insert_merge_dismissal / insert_merge_dismissal_scoped(graph_id, req.a, req.b)
    ) {
        tracing::error!(error = %e, "dismiss_merge insert error");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        );
    }

    // Judgement-graph dual-write (design doc §6/§9 PR2, founder-Q4 bypass
    // coverage). This is also the merge-review UI's "Reject" (PR4): the human
    // NEGATIVE written below supersedes and annuls the pair's live UNSURE row
    // via `write_judgement`'s own step 3, exactly as `merge_node`'s POSITIVE
    // does for "Confirm" — same source class as the ER worker's "different" arm:
    // a human "keep separate" is exactly as authoritative as a human
    // NEGATIVE from any other surface). Flag-gated to `On` (see
    // `merge_node`'s doc for the full "one kill-switch governs every
    // writer" rationale) AND only on the scoped path (`scoped_tx.is_some()`).
    if judgement_write_mode() == JudgementWriteMode::On && scoped_tx.is_some() {
        let new = NewJudgement {
            graph_id,
            node_a_id: req.a,
            node_b_id: req.b,
            verdict: JudgementVerdict::Negative,
            source: JudgementSource::Human,
            confidence: None,
            model_id: None,
            prompt_version: None,
            evidence: json!({"route": "dismiss_merge_suggestion"}),
            created_by: Some(user.user_id),
            expires_at: None,
        };
        match scoped_store!(store, scoped_tx, write_judgement / write_judgement_scoped(new)) {
            Ok(_) => {}
            // F4 (review round 2): a stale suggestion — the worker already
            // merged one of the pair's endpoints away since the suggestion
            // was listed — or an a==b request body both fail
            // `write_judgement`'s endpoint-liveness check the same way.
            // That check is a code-level `SELECT count(*)` compared against
            // 2 in Rust, returning `Err` with NO SQL statement having
            // failed — so this transaction is not SQL-aborted here, and
            // falling through to the commit below is legal and restores the
            // pre-PR2 200 no-op for this case instead of a 500 that would
            // also roll back the legacy `not_same` dismissal already
            // written above (same transaction).
            Err(crate::store::StoreError::JudgementEndpointMissing { .. }) => {
                tracing::info!(
                    %graph_id, a = %req.a, b = %req.b,
                    "dismiss_merge: judgement dual-write skipped (stale pair or a==b) — \
                     legacy dismissal still recorded"
                );
            }
            Err(e) => {
                tracing::error!(error = %e, "dismiss_merge judgement write error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    }

    commit_write_tx!(scoped_tx);
    (StatusCode::OK, Json(json!({"ok": true})))
}

// ── GET /v1/graphs/{graph_id}/merge-review — the standing UNSURE queue ────────
//
// The human half of the judgement graph (design doc §5.3, PR4). The ER worker
// writes an UNSURE `reef_judgement` row when it can neither merge nor separate
// a high-impact pair on its own; those rows are standing review items with a
// ~30d `expires_at`, and this is the list a person resolves them from.
//
// "Reject" is the EXISTING `POST /v1/graphs/{graph_id}/merge-suggestions/
// dismiss` (legacy `not_same` dual-write + a human NEGATIVE, which supersedes
// the pair's live UNSURE row inside `write_judgement`'s step 3).
//
// "Confirm" was ALSO meant to be an existing route — `POST /v1/nodes/{keep}/
// merge` — and that was wrong, which review round 1 of this PR caught. That
// handler is NODE-anchored: it scopes by `user.account_id` because no
// node→account bootstrap resolver exists (module doc's scope-account choice).
// Every OTHER route on this surface is GRAPH-anchored, scoping by the ocean's
// owning account via `graph_account_id`, precisely because a reachable ocean
// may be a team ocean owned by an account that is not the caller's ACTIVE one
// (`resolve_graph_reachable_by_user`, migration 0084, admits any account the
// caller is a member of — not just the active one). On such an ocean the
// queue listed cards, Reject worked, and every Confirm 404'd at
// `merge_node`'s own `node_graph_id_scoped` lookup, because `nodes`' RLS
// policy hides rows in graphs the ACTIVE account does not own. The queue was
// undrainable by its primary action.
//
// `confirm_merge_review` below is that action, graph-anchored and therefore
// symmetric with Reject. It is deliberately NARROWER than `merge_node`, not a
// second general-purpose merge: it can only resolve a pair that already
// carries a LIVE UNSURE judgement in the named ocean, identified by that
// judgement's own id. The supersession contract still has exactly ONE
// implementation — both this route and `merge_node`'s flag-on branch call
// `write_human_positive_and_merge`.
//
// `merge_node` itself is left alone. Its node-anchored scoping is a
// PRE-EXISTING asymmetry that also affects the workbench merge and the MCP
// surface on a non-active-account ocean; fixing it needs either a new
// SECURITY DEFINER node→account bootstrap (a new migration and a new
// RLS-bypass surface) or multi-account probing inside a write handler.
// Neither belongs in this PR, and neither is needed for the review queue to
// work.

/// Server-side page clamp for the merge-review queue. Local twins of
/// `routes::relationships`' identically-valued `MAX_PAGE`/`DEFAULT_PAGE`
/// (which are private to that module) rather than a cross-module import of a
/// private const — same 1..200 / default-100 convention as `/v1/candidates`
/// and the relationship list.
const MERGE_REVIEW_MAX_PAGE: i64 = 200;
const MERGE_REVIEW_DEFAULT_PAGE: i64 = 100;

/// `created_at|judgement_id` keyset cursor, the same shape (and the same
/// opaque-to-callers contract) as `routes::relationships`'
/// `encode_edges_cursor` — a local twin for the same reason the page consts
/// are: those helpers are private to that module.
fn encode_merge_review_cursor(created_at: chrono::DateTime<chrono::Utc>, judgement_id: Uuid) -> String {
    format!("{}|{judgement_id}", created_at.to_rfc3339())
}

fn decode_merge_review_cursor(cursor: &str) -> Option<(chrono::DateTime<chrono::Utc>, Uuid)> {
    let (ts, id) = cursor.split_once('|')?;
    let ts = chrono::DateTime::parse_from_rfc3339(ts).ok()?.with_timezone(&chrono::Utc);
    let id = Uuid::parse_str(id).ok()?;
    Some((ts, id))
}

#[derive(Debug, Deserialize)]
pub struct ListMergeReviewQuery {
    /// Page size, clamped 1..200 (default 100).
    pub limit: Option<i64>,
    /// Opaque keyset cursor from a previous response's `next_cursor`.
    pub cursor: Option<String>,
}

/// One keyset page of live UNSURE judgements, oldest first, each enriched
/// with both reefs' title + coral count. A row whose reefs have since
/// disappeared is dropped from the page (the ER sweep annuls it shortly
/// after), so a page can legitimately come back shorter than `limit` while
/// still reporting `has_more` — the cursor tracks rows FETCHED, not rows
/// rendered, which is what keeps the next page from skipping a live row.
///
/// # Flag gate: one kill-switch darkens the WHOLE feature
/// When `JUDGEMENT_GRAPH_WRITES` is anything but `On` this returns an empty
/// page WITHOUT querying `reef_judgement` at all. That is not an optimization,
/// it is a correctness requirement: both resolution routes gate their
/// judgement writes on the same flag, so with the flag off a Confirm or Reject
/// could not retire the UNSURE row it was resolving. Listing rows a user
/// cannot act on would strand them in a queue that never drains — worse than
/// showing nothing. Flipping the flag off must make the entire surface
/// (listing, badge count, and the two writers) go quiet together.
///
/// # Scoping
/// `ReachabilityResolver` first, OUTSIDE any transaction (the permanent
/// SECURITY DEFINER carve-out — module doc), 404 on refusal exactly like
/// `dismiss_merge_suggestion`. Then GRAPH-anchored scoping via the
/// `graph_account_id` bootstrap resolver, not `user.account_id`: this is a
/// graph-anchored read, and a reachable ocean may be a team ocean owned by
/// another account (module doc's scope-account choice).
pub async fn list_merge_review(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Query(params): Query<ListMergeReviewQuery>,
) -> (StatusCode, Json<Value>) {
    let limit = params.limit.unwrap_or(MERGE_REVIEW_DEFAULT_PAGE).clamp(1, MERGE_REVIEW_MAX_PAGE);
    let cursor = match params.cursor.as_deref() {
        None => None,
        Some(c) => match decode_merge_review_cursor(c) {
            Some(c) => Some(c),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "malformed cursor"})),
                )
            }
        },
    };

    let store = require_store!(tenant);
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_merge_review reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // The gate runs AFTER reachability (an unreachable ocean stays a 404 in
    // every flag mode — the kill switch must not become an IDOR oracle) but
    // BEFORE the account bootstrap and the transaction, so a darkened feature
    // costs one reachability check and nothing else.
    if judgement_write_mode() != JudgementWriteMode::On {
        return (
            StatusCode::OK,
            Json(json!({"items": [], "has_more": false, "next_cursor": null})),
        );
    }

    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_merge_review account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx = begin_scoped_tx!(tenant, account_id);

    // Fetch one extra row to compute `has_more`/`next_cursor` without a
    // second query — same trick the relationship list uses.
    let rows = match scoped_store!(
        store,
        scoped_tx,
        list_live_unsure_judgements
            / list_live_unsure_judgements_scoped(graph_id, cursor, limit + 1)
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_merge_review query error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let has_more = rows.len() as i64 > limit;
    let page: Vec<crate::store::ReefJudgement> = rows.into_iter().take(limit as usize).collect();
    // The cursor is the PAGE's last row, not the last row skipped by the
    // liveness/endpoint filters below — a dropped card must not also drop the
    // page boundary, or the next page would skip live rows.
    let next_cursor = if has_more {
        page.last().map(|j| encode_merge_review_cursor(j.created_at, j.judgement_id))
    } else {
        None
    };

    // Both endpoints of every row on this page, in one lookup.
    let mut wanted: Vec<Uuid> = page.iter().flat_map(|j| [j.node_a_id, j.node_b_id]).collect();
    wanted.sort_unstable();
    wanted.dedup();
    let endpoint_by_node: std::collections::HashMap<Uuid, (String, i64)> = match scoped_store!(
        store,
        scoped_tx,
        nodes_by_ids_with_counts / nodes_by_ids_with_counts_scoped(graph_id, &wanted)
    ) {
        Ok(rows) => rows.into_iter().map(|(id, title, n)| (id, (title, n))).collect(),
        // NOT best-effort, unlike the relationship list's title lookup. A
        // merge card with no reef names is unactionable, so degrading would
        // drop every row on the page — and an empty merge-review section
        // renders as "nothing to review", which is a false statement, not a
        // degraded one. Fail loudly instead; the scoped transaction rolls
        // back on drop.
        Err(e) => {
            tracing::error!(error = %e, "list_merge_review endpoint lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    commit_read_tx(scoped_tx).await;

    let items: Vec<Value> = page
        .iter()
        .filter_map(|j| {
            // `reef_judgement` deliberately carries NO endpoint FK (migration
            // 0139's header: audit history must survive node deletion), so a
            // row can outlive one of its reefs. Skip the whole row rather than
            // render half a card — the ER sweep's dead-endpoint pass annuls it
            // shortly after, and Confirm on a dead pair would 404 anyway.
            let (a_title, a_count) = endpoint_by_node.get(&j.node_a_id)?;
            let (b_title, b_count) = endpoint_by_node.get(&j.node_b_id)?;
            Some(json!({
                "judgement_id": j.judgement_id,
                "a": {"node_id": j.node_a_id, "title": a_title, "atom_count": a_count},
                "b": {"node_id": j.node_b_id, "title": b_title, "atom_count": b_count},
                "confidence": j.confidence,
                "created_at": j.created_at.to_rfc3339(),
                "expires_at": j.expires_at.map(|t| t.to_rfc3339()),
            }))
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "items": items,
            "has_more": has_more,
            "next_cursor": next_cursor,
        })),
    )
}

// ── POST /v1/graphs/{graph_id}/merge-review/confirm ──────────────────────────

#[derive(Deserialize)]
pub struct ConfirmMergeReviewRequest {
    /// The card being resolved. Not redundant with the pair: it is what makes
    /// this route "resolve THIS review item" instead of "merge any two reefs",
    /// and it makes a double-click on a stale card a clean 404 instead of a
    /// second merge.
    pub judgement_id: Uuid,
    /// The reef that survives (the user may have swapped the default).
    pub keep_id: Uuid,
    /// The reef merged into it, then deleted.
    pub dup_id: Uuid,
}

/// Resolve one merge-review card as "same reef" — the queue's Confirm.
///
/// GRAPH-anchored, and that is the entire point of its existence: see the
/// section header above `list_merge_review` for why routing Confirm through
/// `merge_node` left the queue undrainable on any ocean owned by an account
/// other than the caller's active one.
///
/// # Why this is narrower than `merge_node`, not wider
/// The only caller-supplied authority is `graph_id`, gated by the same
/// `ReachabilityResolver` check every other route on this surface uses. Past
/// that gate, this route can merge a pair ONLY if:
///   * a LIVE judgement exists for that pair in THAT ocean
///     ([`Store::get_active_judgement`] applies the annul + expiry predicate),
///   * it is the judgement the caller named, and
///   * its verdict is UNSURE — i.e. it is an open question, not a settled
///     ruling being quietly overwritten.
/// Anything else is a `404`, so this cannot be used as a general cross-reef
/// (or cross-ocean) merge tool. `write_judgement`'s own endpoint check then
/// re-proves both reefs are live IN this graph, under the graph's own scope.
///
/// # One 404 rule
/// Every "this card is no longer actionable" outcome — already resolved,
/// expired, superseded by another ruling, endpoints gone — is a `404`. The UI
/// drops a card on 404 and leaves it in place on anything else, so a single
/// status carries the whole "stop showing this" decision.
///
/// # Flag gate
/// `409`, not an empty success, when `JUDGEMENT_GRAPH_WRITES != On` (or with
/// no scoped transaction). A card can only
/// be on screen from a stale tab in that case, and silently dropping it would
/// tell the user a merge happened when none did. `409` leaves the card put.
pub async fn confirm_merge_review(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Json(req): Json<ConfirmMergeReviewRequest>,
) -> (StatusCode, Json<Value>) {
    if req.keep_id == req.dup_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "cannot merge a reef into itself"})),
        );
    }

    let store = require_store!(tenant);
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "confirm_merge_review reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Reachability first so the flag gate can never become an IDOR oracle, the
    // same ordering `list_merge_review` uses. `tenant.pool.is_none()` is the
    // cloud-only half — same rationale as `merge_node`'s `scoped_tx.is_some()`.
    if judgement_write_mode() != JudgementWriteMode::On || tenant.pool.is_none() {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "merge review is not enabled"})),
        );
    }

    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "ocean not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "confirm_merge_review account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx = begin_scoped_tx!(tenant, account_id);
    // Reachable is not writable (settings 2.10): a plain member reaches the
    // commons but may not rewrite it.
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());
    require_graph_writable!(store.as_ref(), scoped_tx, graph_id, writer, "ocean not found");

    // The card must still be the live, open question the caller thinks it is.
    // Order-normalizing, so keep/dup may arrive either way round.
    let live = match scoped_store!(
        store,
        scoped_tx,
        get_active_judgement / get_active_judgement_scoped(graph_id, req.keep_id, req.dup_id)
    ) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "confirm_merge_review judgement lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let stale = match &live {
        None => true,
        Some(j) => j.judgement_id != req.judgement_id || j.verdict != "unsure",
    };
    if stale {
        tracing::info!(
            %graph_id, keep_id = %req.keep_id, dup_id = %req.dup_id,
            requested = %req.judgement_id,
            current = ?live.as_ref().map(|j| (j.judgement_id, j.verdict.as_str())),
            "confirm_merge_review: stale card"
        );
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "merge review card is no longer open"})),
        );
    }

    match write_human_positive_and_merge(
        store,
        &mut scoped_tx,
        graph_id,
        req.keep_id,
        req.dup_id,
        user.user_id,
        "merge_review_confirm",
    )
    .await
    {
        Ok(moved) => {
            commit_write_tx!(scoped_tx);
            (
                StatusCode::OK,
                Json(json!({"ok": true, "corals_moved": moved})),
            )
        }
        Err(e) => {
            // Same mapping `merge_node` uses — see `judgement_error_response`.
            // Nothing is committed on this path, so the scoped transaction
            // rolls back on drop.
            if let Some((status, body)) = judgement_error_response(&e) {
                tracing::info!(
                    error = %e, %graph_id, keep_id = %req.keep_id, dup_id = %req.dup_id,
                    "confirm_merge_review refused"
                );
                return (status, Json(body));
            }
            tracing::error!(error = %e, "confirm_merge_review merge error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "merge failed"})),
            )
        }
    }
}

// ── POST /v1/nodes/{id}/merge — merge a duplicate reef into another ───────────
//
// Entity resolution: combine two reefs that are the same thing (e.g. the same
// person spelled differently across transcripts). The reef in the path is the
// one KEPT; `dup_id` in the body is merged into it (its corals re-pointed, then
// it's deleted). Both must be in an ocean the caller can reach, same graph.

#[derive(Deserialize)]
pub struct MergeRequest {
    /// The duplicate reef to merge into the path reef (it is deleted).
    pub dup_id: Uuid,
}

/// Map a judgement-graph refusal onto [`merge_node`]'s response (PR4).
/// `None` = "not a judgement-graph refusal", and the caller keeps its
/// existing `500 "merge failed"`.
///
/// Before PR4 all three of these collapsed into that 500, which made the
/// single most likely failure — a review card whose pair merged or was
/// deleted between listing and clicking Confirm — indistinguishable from a
/// real outage, to the user and to the on-call dashboard alike.
///
/// * [`StoreError::JudgementEndpointMissing`](crate::store::StoreError) →
///   `404`. `write_judgement`'s endpoint check requires BOTH ids to resolve
///   to live nodes IN this judgement's graph, so this arm covers both "the
///   pair died since it was listed" (the race left open by the handler's own
///   earlier `node_graph_id` 404 — that read and this write are separate
///   statements under READ COMMITTED) and "these two ids are not a pair in
///   one ocean at all". Per #587's F4 reasoning, documented at
///   `dismiss_merge_suggestion`'s call site: the check is a Rust-side count
///   comparison, no SQL statement failed, so the transaction is NOT aborted
///   — but unlike dismiss, `merge_node` has nothing else worth committing, so
///   the caller returns early and the scoped transaction rolls back on drop,
///   the same way every other early-return arm in that handler already
///   behaves.
/// * [`StoreError::HumanJudgementExists`](crate::store::StoreError) → `409`,
///   defensively. This route always writes `source = human`, and
///   `write_judgement` raises this only when the incoming write is NOT human
///   (human supersedes human), so it should be UNREACHABLE from here. A 500
///   would be the wrong answer if that invariant ever moves, and the error
///   already names the judgement that blocked the write.
/// * [`StoreError::JudgementMergeMismatch`](crate::store::StoreError) →
///   `409`. `merge_nodes_with_judgement`'s back-link validation: the
///   judgement and the pair disagree. A conflict, not an outage.
/// The ONE "a human said these are the same reef" sequence, shared by
/// [`merge_node`]'s flag-on branch and [`confirm_merge_review`] so the
/// judgement-graph supersession contract has a single implementation no
/// matter which surface the human ruled from (PR4, review round 1).
///
/// Writes the human POSITIVE — which annuls and supersedes the pair's live
/// UNSURE row inside `write_judgement`'s own step 3 — then materializes via
/// `merge_nodes_with_judgement`, so `node_merges.judgement_id` back-links the
/// ruling that authorized the merge.
///
/// Deliberately does NOT commit: the caller owns its transaction, and the two
/// callers have different work left on the way out. `route` is recorded as
/// `evidence.route`, which is the only field that differs between them — the
/// audit trail must still say which surface a ruling came from.
async fn write_human_positive_and_merge(
    store: &Arc<dyn crate::store::Store>,
    scoped_tx: &mut ScopedTx,
    graph_id: Uuid,
    keep_id: Uuid,
    dup_id: Uuid,
    user_id: Uuid,
    route: &'static str,
) -> Result<u64, crate::store::StoreError> {
    let new = NewJudgement {
        graph_id,
        node_a_id: keep_id,
        node_b_id: dup_id,
        verdict: JudgementVerdict::Positive,
        source: JudgementSource::Human,
        confidence: None,
        model_id: None,
        prompt_version: None,
        evidence: json!({"route": route}),
        created_by: Some(user_id),
        expires_at: None,
    };
    let judgement_id = scoped_store!(
        store,
        *scoped_tx,
        write_judgement / write_judgement_scoped(new)
    )?;
    scoped_store!(
        store,
        *scoped_tx,
        merge_nodes_with_judgement
            / merge_nodes_with_judgement_scoped(keep_id, dup_id, judgement_id)
    )
}

fn judgement_error_response(e: &crate::store::StoreError) -> Option<(StatusCode, Value)> {
    match e {
        crate::store::StoreError::JudgementEndpointMissing { .. } => Some((
            StatusCode::NOT_FOUND,
            json!({"error": "reef pair no longer exists"}),
        )),
        crate::store::StoreError::HumanJudgementExists { judgement_id }
        | crate::store::StoreError::JudgementMergeMismatch { judgement_id } => Some((
            StatusCode::CONFLICT,
            json!({"error": e.to_string(), "judgement_id": judgement_id}),
        )),
        _ => None,
    }
}

pub async fn merge_node(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(keep_id): Path<Uuid>,
    Json(req): Json<MergeRequest>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    if keep_id == req.dup_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "cannot merge a reef into itself"})),
        );
    }

    // Node-id-anchored write: scope by the caller's active account (module
    // doc, RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // Both reefs must be reachable by the caller (ADR-0007 D9). The
    // reachability half runs outside the scoped transaction (permanent
    // SECURITY DEFINER carve-out). Also captures `keep_id`'s graph (PR2):
    // both nodes must resolve to the same graph anyway (`merge_nodes`
    // itself rejects a cross-graph pair), so `keep_id`'s is the graph a
    // judgement write below is anchored to.
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());
    let mut keep_graph_id: Option<Uuid> = None;
    for id in [keep_id, req.dup_id] {
        let graph = match scoped_store!(store, scoped_tx, node_graph_id / node_graph_id_scoped(id))
        {
            Ok(Some(g)) => g,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "reef not found or access denied"})),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "merge_node lookup error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        if id == keep_id {
            keep_graph_id = Some(graph);
        }
        match ReachabilityResolver::graph_reachable_by_user(
            tenant.decision.as_ref(),
            store.as_ref(),
            graph,
            user.user_id,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "reef not found or access denied"})),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "merge_node reachability error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
        // Reachable is not writable: merging rewrites both reefs' ocean.
        require_graph_writable!(store.as_ref(), scoped_tx, graph, writer, "reef not found or access denied");
    }

    // Judgement-graph coverage (design doc §6/§9 PR2, founder-Q4): flag-
    // gated to `On` AND requires a scoped transaction (see
    // `dismiss_merge_suggestion`'s doc for the identical `scoped_tx.is_some()`
    // rationale — the unscoped path below is unaffected by the flag and stays
    // a plain, judgement-less merge). A human write can't hit
    // `HumanJudgementExists` (human supersedes human — `write_judgement`'s
    // own doc), so any judgement-write failure here fails the whole
    // request's transaction — atomic, acceptable under the flag (PR5's
    // node_merges/not_same backfill reconciles anything from a gated
    // window; nothing is permanently lost). PR4 refines only the STATUS
    // CODE that failure surfaces as, never its atomicity: see
    // `judgement_error_response` for the three refusals that are a 404/409
    // rather than a 500. This is also the route the merge-review UI's
    // "Confirm" calls — the human POSITIVE written here is what supersedes
    // and annuls the pair's live UNSURE row, inside `write_judgement`'s own
    // step 3. There is deliberately no separate "resolve this UNSURE"
    // writer.
    if judgement_write_mode() == JudgementWriteMode::On && scoped_tx.is_some() {
        // CR2 (CodeRabbit): the loop above always sets `keep_graph_id` on
        // its `keep_id` iteration before this point is reached, so `None`
        // here should never happen — but an `expect` landmine turns any
        // future refactor of that loop into a worker-crashing panic on a
        // user-facing request instead of a graceful 500.
        let Some(graph_id) = keep_graph_id else {
            tracing::error!(
                %keep_id,
                "merge_node: keep_id's graph was not resolved by the reachability loop above"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        };
        // Both judgement-graph calls live in `write_human_positive_and_merge`,
        // shared with `confirm_merge_review` — see that helper's doc.
        return match write_human_positive_and_merge(
            store,
            &mut scoped_tx,
            graph_id,
            keep_id,
            req.dup_id,
            user.user_id,
            "manual_merge",
        )
        .await
        {
            Ok(moved) => {
                commit_write_tx!(scoped_tx);
                (
                    StatusCode::OK,
                    Json(json!({"ok": true, "corals_moved": moved})),
                )
            }
            Err(e) => {
                // PR4: the judgement-graph refusals are user-facing outcomes,
                // not outages — see `judgement_error_response`'s doc. Anything
                // else keeps the pre-PR4 500. Either way this returns before
                // any commit, so the scoped transaction rolls back on drop.
                //
                // `JudgementMergeMismatch` is reachable only from the helper's
                // SECOND call, and is defensive by construction — that call
                // always receives the judgement id `write_judgement` just
                // returned for THIS pair in THIS graph, which is exactly what
                // the back-link validates. It is therefore proven only by
                // `judgement_conflicts_map_to_409_and_carry_the_judgement_id`,
                // never by a live route test.
                if let Some((status, body)) = judgement_error_response(&e) {
                    tracing::info!(
                        error = %e, %keep_id, dup_id = %req.dup_id,
                        "merge_node judgement-graph merge refused"
                    );
                    return (status, Json(body));
                }
                tracing::error!(error = %e, "merge_node judgement-graph merge error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "merge failed"})),
                )
            }
        };
    }

    match scoped_store!(
        store,
        scoped_tx,
        merge_nodes / merge_nodes_scoped(keep_id, req.dup_id)
    ) {
        Ok(moved) => {
            commit_write_tx!(scoped_tx);
            (
                StatusCode::OK,
                Json(json!({"ok": true, "corals_moved": moved})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "merge_node merge error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "merge failed"})),
            )
        }
    }
}

// ── POST /v1/nodes/merges/{merge_id}/undo — reverse an entity-resolution merge ─
//
// CS-ER: auto-merges hard-delete the dup reef (keep stays the single live row),
// so a wrong merge would be irreversible. `merge_nodes` records a `node_merges`
// undo-log row; this route replays it — recreating the dup, moving its atoms +
// edges back, and stamping the record undone. Idempotent (a second undo is a
// no-op). Authorized like the merge route: the merge's ocean must be reachable
// by the caller (ADR-0007 D9).

pub async fn undo_merge(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(merge_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // Merge-id-anchored write: scope by the caller's active account (module
    // doc, RLS section).
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // IDOR: resolve the merge's ocean and require the caller can reach it. A
    // missing record reads as "not found" (don't leak existence).
    let graph_id = match scoped_store!(
        store,
        scoped_tx,
        node_merge_graph_id / node_merge_graph_id_scoped(merge_id)
    ) {
        Ok(Some(g)) => g,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "merge not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "undo_merge lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "merge not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "undo_merge reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }
    let writer = ReefWriter::rest(&user, tenant.decision.as_ref());
    require_graph_writable!(store.as_ref(), scoped_tx, graph_id, writer, "merge not found or access denied");

    match scoped_store!(
        store,
        scoped_tx,
        unmerge_nodes / unmerge_nodes_scoped(merge_id)
    ) {
        Ok(()) => {
            commit_write_tx!(scoped_tx);
            (StatusCode::OK, Json(json!({"ok": true, "merge_id": merge_id})))
        }
        Err(e) => {
            tracing::error!(error = %e, "undo_merge error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "unmerge failed"})),
            )
        }
    }
}

// ── GET /v1/nodes/{id}/digest — LLM summary of a reef + per-coral excerpts ─────
//
// A high-level summary synthesizing the corals linked to a reef, plus, per
// coral, just the excerpt relevant to this reef. Served from the cached
// `body.digest` only: generation is the background `RegenerateDigest`
// worker's job under `managed_enrichment` (source-agnostic plan PR 8). A miss
// queues it and returns `unavailable: true`, the free-tier shape. On Postgres
// a stored digest is served only through `crate::reef_digest_read`, which
// re-verifies its receipt for the reader and attributes it.

/// The digest as cached in `node.body.digest` (synced across devices).
///
/// `atoms_key` is a legacy field (kept only so old / test-fixture JSON blobs
/// still parse) — freshness is no longer decided by it. §2.1/§2.2 of the
/// graph-temporal-memory plan diagnosed the old `atoms_key` scheme (a hash
/// of the FULL sorted linked-atom-id list) as half the reason digests never
/// stayed warm: it invalidated on every single coral added. Freshness is now
/// the SAME growth/rename rule the background `RegenerateDigest` trigger and
/// worker use (`workers::digest::should_regenerate_digest`), driven by the
/// reef's write-time `coral_count`/`digest_coral_count` counters — see
/// `digested_title` below for the piece those counters alone can't answer.
#[derive(Deserialize)]
struct CachedDigest {
    // Never read (see the struct doc comment) — parsed only so old JSON
    // blobs that carry it still deserialize.
    #[allow(dead_code)]
    #[serde(default)]
    atoms_key: String,
    /// The reef title this digest was generated against. `coral_count`/
    /// `digest_coral_count` alone can't detect a rename (a title edit
    /// touches neither counter), so this is compared against the reef's
    /// CURRENT title directly. `None` (a digest cached before this field
    /// existed) is treated as "renamed" — forces exactly one regeneration
    /// to adopt the new scheme; self-healing, not an error.
    #[serde(default)]
    digested_title: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    excerpts: Value,
}

/// Is a cached digest still fresh (§2.2/§2.3)? `digested_title` is what the
/// cached digest itself was generated against (`None` = a cache written
/// before this field existed); `title`/`coral_count`/`digest_coral_count`
/// are the reef's CURRENT state. Factored out of `node_digest` as a pure
/// function purely so this decision — the replacement for the old
/// full-atom-id-list-hash cache key (§2.1) — is unit-testable without a
/// database or HTTP harness.
fn cached_digest_is_fresh(
    digested_title: Option<&str>,
    title: &str,
    coral_count: i64,
    digest_coral_count: i64,
) -> bool {
    let renamed = digested_title != Some(title);
    !crate::workers::digest::should_regenerate_digest(coral_count, digest_coral_count, renamed)
}

pub async fn node_digest(
    // Unused since source-agnostic PR 8 (no model call from this route); kept
    // so the route's extractor signature is unchanged. All content-plane
    // access goes through `tenant` (org-tenancy PR 1.5).
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(node_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // Node-id-anchored: scope by the caller's active account (module doc,
    // RLS section). This handler is the documented TWO-transaction exception:
    // this read transaction closes BEFORE the (multi-second) LLM call below —
    // never pin a pooled connection across network I/O — and the digest
    // cache-write afterwards opens its own short scoped transaction.
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);

    // Authorize by reachability (ADR-0007 D9), same as get_node_atoms.
    let node_graph = match scoped_store!(
        store,
        scoped_tx,
        node_graph_id / node_graph_id_scoped(node_id)
    ) {
        Ok(Some(g)) => g,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "node_digest lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        node_graph,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "node_digest reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Postgres: the attributed read (`crate::reef_digest_read`). A stored
    // digest is served only when its managed receipt still verifies for THIS
    // reader: every coral it read is permitted to them, linked and unchanged,
    // and the reef title and edge facts are as generated. Anything else is
    // `unavailable` and queues one deduped regeneration (when eligible and
    // `DIGESTS_ENABLED`). MCP `get_reef` serves the same object.
    if let Some(tx) = scoped_tx.as_mut() {
        let read = crate::reef_digest_read::read_for_reader(tx, node_id).await;
        commit_read_tx(scoped_tx).await;
        let read = match read {
            Ok(Some(read)) => read,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "reef not found or access denied"})),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "node_digest read error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        if let crate::reef_digest_read::DigestOutcome::Unavailable(why) = read.outcome {
            tracing::debug!(%node_id, ?why, "node_digest: stored digest not served");
        }
        let regenerating = read.should_regenerate()
            && crate::workers::digest::queue_regeneration(
                tenant.pool.as_ref(),
                node_id,
                read.graph_id,
                user.account_id,
            )
            .await;
        return (StatusCode::OK, Json(read.http_body(regenerating)));
    }

    // No scoped transaction: the reef's cached digest via the plain Store.
    // Read the reef's stored title, write-time coral counters (§2.2/§2.3) and
    // cached digest (in body.digest). Use the node's real title — not a
    // client param — so cache validity is stable.
    let digest_ctx = match scoped_store!(
        store,
        scoped_tx,
        node_digest_context / node_digest_context_scoped(node_id)
    ) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "reef not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "node_digest read error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let title = digest_ctx.title;
    let node_type = digest_ctx.node_type;
    let cached_digest_json = digest_ctx.cached_digest;
    let coral_count = digest_ctx.coral_count;
    let digest_coral_count = digest_ctx.digest_coral_count;

    let ids = match scoped_store!(
        store,
        scoped_tx,
        node_atom_ids / node_atom_ids_scoped(node_id)
    ) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "node_digest node_atom_ids error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // Read phase complete — release the scoped transaction (and its pooled
    // connection) before the enqueue below.
    commit_read_tx(scoped_tx).await;

    if ids.is_empty() {
        return (
            StatusCode::OK,
            Json(json!({"summary": Value::Null, "excerpts": []})),
        );
    }

    // Serve the cached digest when it is still fresh (§2.2/§2.3): the SAME
    // growth/rename rule the background `RegenerateDigest` trigger and worker
    // use, driven by the reef's write-time coral counters.
    if let Some(dj) = &cached_digest_json {
        if let Ok(cd) = serde_json::from_str::<CachedDigest>(dj) {
            if cached_digest_is_fresh(
                cd.digested_title.as_deref(),
                &title,
                coral_count,
                digest_coral_count,
            ) {
                return (
                    StatusCode::OK,
                    Json(json!({ "summary": cd.summary, "excerpts": cd.excerpts, "cached": true })),
                );
            }
        }
    }

    // Managed enrichment (source-agnostic plan PR 8): this route makes NO
    // model call. Before PR 8 a cache miss called the digest model directly,
    // outside the managed gate, on every open (no cap, no receipt). A miss
    // now queues the background `RegenerateDigest` worker instead — deduped
    // on the same `digest:{node_id}` key as the coral-count trigger, metered
    // under `managed_enrichment`, bounded per graph per day — when the reef
    // is eligible and digests are switched on, and answers `unavailable`
    // (the shape the free tier already gets). `state.llm` is therefore no
    // longer read here.
    let regenerating = if crate::workers::digest::digest_eligible(&node_type, coral_count)
        && crate::workers::digest::digests_claim_allowed()
    {
        match tenant.pool.as_ref() {
            Some(pool) => match crate::jobs::queue::enqueue(
                pool,
                &crate::jobs::JobKind::RegenerateDigest { node_id, graph_id: node_graph },
                Some(user.account_id),
                Some(&format!("digest:{node_id}")),
                0,
            )
            .await
            {
                Ok(_) => true,
                Err(e) => {
                    tracing::warn!(error = %e, %node_id, "node_digest: failed to queue a background regeneration");
                    false
                }
            },
            None => false,
        }
    } else {
        false
    };
    (
        StatusCode::OK,
        Json(json!({"summary": Value::Null, "excerpts": [], "unavailable": true, "regenerating": regenerating})),
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod dedup_tests {
    use super::{edit_distance, normalize_entity, same_entity};

    #[test]
    fn normalize_collapses_case_punct_space() {
        assert_eq!(normalize_entity("Project Apollo"), "project apollo");
        assert_eq!(normalize_entity("project-apollo"), "projectapollo");
        assert_eq!(normalize_entity("  Project   Apollo  "), "project apollo");
        assert_eq!(normalize_entity("ProjectApollo"), "projectapollo");
    }

    #[test]
    fn same_entity_merges_typos_on_long_names_only() {
        // Long names within edit-distance 1 → same (transcription typo).
        assert!(same_entity("project apollo", "project apollu"));
        assert!(same_entity("anthropic", "anthropics"));
        // Short names are NOT fuzzily merged (distinct people).
        assert!(!same_entity("sam", "sams"));
        assert!(!same_entity("jon", "jan"));
        // Clearly different long names stay separate.
        assert!(!same_entity("project apollo", "project zephyr"));
        // Exact matches always merge.
        assert!(same_entity("rust", "rust"));
    }

    #[test]
    fn edit_distance_basic() {
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(edit_distance("abc", "abc"), 0);
        assert_eq!(edit_distance("abc", "abd"), 1);
    }

}

#[cfg(test)]
mod digest_freshness_tests {
    use super::cached_digest_is_fresh;

    #[test]
    fn fresh_when_no_growth_and_no_rename() {
        assert!(cached_digest_is_fresh(Some("Chris Marchetti"), "Chris Marchetti", 50, 50));
    }

    #[test]
    fn stale_when_growth_crosses_the_cadence_threshold() {
        // digest_coral_count=50: floor is max(5, ceil(10))=10. growth 10 >= 10.
        assert!(!cached_digest_is_fresh(Some("Chris Marchetti"), "Chris Marchetti", 60, 50));
    }

    #[test]
    fn fresh_when_growth_is_below_the_cadence_threshold() {
        assert!(cached_digest_is_fresh(Some("Chris Marchetti"), "Chris Marchetti", 55, 50));
    }

    #[test]
    fn stale_on_rename_even_with_zero_growth() {
        assert!(!cached_digest_is_fresh(Some("Chris P."), "Chris Marchetti", 50, 50));
    }

    #[test]
    fn stale_when_no_digested_title_recorded_yet() {
        // A cached digest written before `digested_title` existed (pre-
        // migration): treated as renamed, forcing exactly one regeneration
        // to adopt the new freshness scheme — not a crash, self-healing.
        assert!(!cached_digest_is_fresh(None, "Chris Marchetti", 50, 50));
    }

    #[test]
    fn a_background_warmed_digest_is_not_immediately_shadowed_by_one_more_coral() {
        // The §2.1 regression this whole mechanism exists to prevent: a
        // background job just regenerated the digest (digest_coral_count
        // now equals coral_count); ONE more coral landing afterwards must
        // NOT immediately invalidate it, unlike the old full-atom-id-hash
        // scheme.
        assert!(cached_digest_is_fresh(Some("Chris Marchetti"), "Chris Marchetti", 101, 100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::{get, patch},
        Router,
    };
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    fn nodes_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/nodes", get(list_nodes))
            .route("/v1/nodes/{id}", patch(patch_node))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    // ── GET /v1/nodes — missing graph_id → 422 ────────────────────────────────

    #[tokio::test]
    async fn list_nodes_missing_graph_id_returns_422() {
        // Auth fires first (db=None → 503), so we bypass auth by inserting a
        // UserContext directly and calling the handler via a router that skips auth.
        // Instead, test the extraction layer: without auth the middleware fires
        // before the handler, so a missing graph_id with a Bearer token and no DB
        // returns 503, not 422.  We verify the 422 path by calling the handler
        // through a router wired with a fake UserContext injector.
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "member".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/nodes", get(list_nodes))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/nodes")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // ── GET /v1/nodes — no auth header → 401 ─────────────────────────────────

    #[tokio::test]
    async fn list_nodes_requires_auth() {
        let router = nodes_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/nodes?graph_id=00000000-0000-0000-0000-000000000000")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── GET /v1/nodes — bearer with no DB → 503 (wrong account path needs DB) ──

    #[tokio::test]
    async fn list_nodes_bearer_no_db_returns_503() {
        let router = nodes_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/nodes?graph_id=00000000-0000-0000-0000-000000000000")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // require_auth middleware returns 503 when db=None.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── PATCH /v1/nodes/{id} — no auth → 401 ─────────────────────────────────

    #[tokio::test]
    async fn patch_node_requires_auth() {
        let router = nodes_router(test_state());
        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"title":"new title"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── PATCH /v1/nodes/{id} — empty body (no fields) → 400 via fake auth ────

    #[tokio::test]
    async fn patch_node_empty_body_returns_400() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "member".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/nodes/{id}", patch(patch_node))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000")
            .header("content-type", "application/json")
            .body(Body::from(r#"{}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── PATCH /v1/nodes/{id} — invalid type → 400 ────────────────────────────

    #[tokio::test]
    async fn patch_node_invalid_type_returns_400() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "member".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/nodes/{id}", patch(patch_node))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"type":"invalid_type"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── PATCH /v1/nodes/{id} — empty/whitespace title → 400 ─────────────────
    //
    // checked+fixed items #4, widened to the REST route: this guard was
    // originally added only inside the MCP `update_reef` tool handler
    // (`mcp/tools/write.rs`), leaving this route able to accept an
    // empty/whitespace title. Now enforced once, in `validate_patch_fields`,
    // and inherited by both callers.

    #[tokio::test]
    async fn patch_node_empty_title_returns_400() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "member".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/nodes/{id}", patch(patch_node))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"title":"   "}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── PATCH /v1/nodes/{id} — title over PATCH_TITLE_CAP → 400 ─────────────

    #[tokio::test]
    async fn patch_node_title_over_the_cap_returns_400() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "member".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/nodes/{id}", patch(patch_node))
            .layer(from_fn(inject_user))
            .with_state(state);

        let oversized = "x".repeat(PATCH_TITLE_CAP + 1);
        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000")
            .header("content-type", "application/json")
            .body(Body::from(json!({"title": oversized}).to_string()))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── PATCH /v1/nodes/{id} — type = "decision" → 400 ───────────────────────
    //
    // checked+fixed items #2, widened to the REST route: `patch_node` has no
    // `decided_at` handling, so this transition would otherwise trip the
    // `decisions_require_decided_at` CHECK constraint as an opaque 500.

    #[tokio::test]
    async fn patch_node_type_decision_returns_400() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "member".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/nodes/{id}", patch(patch_node))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"type":"decision"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── PATCH /v1/nodes/{id} — valid shape, no DB → 503 ─────────────────────

    #[tokio::test]
    async fn patch_node_bearer_no_db_returns_503() {
        let router = nodes_router(test_state());
        let req = HttpRequest::builder()
            .method("PATCH")
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000")
            .header("Authorization", "Bearer sometoken")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"title":"new title"}"#))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Unit: page_size clamping logic ────────────────────────────────────────

    #[test]
    fn page_size_clamps_to_100() {
        let raw: i64 = 9999;
        let clamped = raw.clamp(1, 100);
        assert_eq!(clamped, 100);
    }

    #[test]
    fn page_size_clamps_to_1_minimum() {
        let raw: i64 = 0;
        let clamped = raw.clamp(1, 100);
        assert_eq!(clamped, 1);
    }

    // ── Unit: `has_more` math (CS-reef-pagination) ────────────────────────────
    //
    // `list_nodes` fetches `page_size + 1` rows and derives `has_more` from
    // whether that extra row came back — same "fetch one extra" pattern as
    // the MCP `list_reefs` tool (`mcp/tools/read.rs`), asserted here in
    // isolation from any store.

    #[test]
    fn has_more_true_when_extra_row_present() {
        let page_size: i64 = 20;
        let rows_len = (page_size + 1) as usize; // store returned the full page + 1 extra
        assert!(rows_len as i64 > page_size);
    }

    #[test]
    fn has_more_false_when_rows_exactly_fill_page() {
        let page_size: i64 = 20;
        let rows_len = page_size as usize; // fewer rows than requested → last page
        assert!(!(rows_len as i64 > page_size));
    }

    #[test]
    fn has_more_false_when_fewer_rows_than_page_size() {
        let page_size: i64 = 20;
        let rows_len = 3usize; // far short of a full page
        assert!(!(rows_len as i64 > page_size));
    }

    // ── Unit: is_valid_node_type ──────────────────────────────────────────────

    #[test]
    fn valid_node_types_accepted() {
        for t in &["entity", "decision", "thread", "reference"] {
            assert!(is_valid_node_type(t), "{t} should be valid");
        }
    }

    #[test]
    fn invalid_node_type_rejected() {
        assert!(!is_valid_node_type("person"));
        assert!(!is_valid_node_type(""));
        assert!(!is_valid_node_type("ENTITY"));
    }

    // ── GET /v1/nodes/{id}/candidates tests ───────────────────────────────────

    fn candidates_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/nodes/{id}/candidates", get(get_node_candidates))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    // ── No auth header → 401 ─────────────────────────────────────────────────

    #[tokio::test]
    async fn get_candidates_requires_auth() {
        let router = candidates_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000/candidates")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Bearer + no DB → 503 (auth middleware fires first) ───────────────────

    #[tokio::test]
    async fn get_candidates_bearer_no_db_returns_503() {
        let router = candidates_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000/candidates")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Handler-level: no store → 503 (via fake auth injection) ──────────────

    #[tokio::test]
    async fn get_candidates_handler_no_db_returns_503() {
        use axum::middleware::from_fn;

        async fn inject_user(
            mut req: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            req.extensions_mut().insert(UserContext {
                user_id: Uuid::nil(),
                account_id: Uuid::nil(),
                role: "member".into(),
                device_id: "test-device".into(),
            });
            next.run(req).await
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/nodes/{id}/candidates", get(get_node_candidates))
            .layer(from_fn(inject_user))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/nodes/00000000-0000-0000-0000-000000000000/candidates")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // store=None → require_store! macro returns 503.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Unit: candidates limit clamping + keyset cursor ───────────────────────

    #[test]
    fn candidates_limit_clamps_and_defaults() {
        assert_eq!(candidate_page_limit(Some(9999)), 200, "ceiling");
        assert_eq!(candidate_page_limit(Some(0)), 1, "floor");
        assert_eq!(candidate_page_limit(Some(-5)), 1, "negative floors, not wraps");
        assert_eq!(candidate_page_limit(None), 50, "default");
        assert_eq!(candidate_page_limit(Some(20)), 20, "in-range passes through");
    }

    #[test]
    fn candidate_cursor_round_trips() {
        let ts = chrono::Utc::now();
        let id = Uuid::new_v4();
        let decoded = decode_candidate_cursor(&encode_candidate_cursor(ts, id)).unwrap();
        assert_eq!(decoded.1, id);
        // RFC 3339 round-trips to the same instant.
        assert_eq!(decoded.0, ts);
    }

    #[test]
    fn candidate_cursor_rejects_garbage() {
        assert!(decode_candidate_cursor("").is_none());
        assert!(decode_candidate_cursor("not-a-cursor").is_none());
        assert!(decode_candidate_cursor("2026-01-01T00:00:00Z|not-a-uuid").is_none());
        assert!(
            decode_candidate_cursor("not-a-ts|00000000-0000-0000-0000-000000000000").is_none()
        );
    }

    // ── Unit: embedding average logic ─────────────────────────────────────────

    #[test]
    fn embedding_average_two_vectors() {
        let a = vec![1.0f32, 2.0, 3.0];
        let b = vec![3.0f32, 4.0, 5.0];
        let embeddings: Vec<Vec<f32>> = vec![a, b];
        let dim = embeddings[0].len();
        let count = embeddings.len() as f32;
        let mut avg = vec![0.0f32; dim];
        for emb in &embeddings {
            for (i, v) in emb.iter().enumerate() {
                avg[i] += v;
            }
        }
        for v in &mut avg {
            *v /= count;
        }
        assert!((avg[0] - 2.0).abs() < 1e-6, "expected 2.0, got {}", avg[0]);
        assert!((avg[1] - 3.0).abs() < 1e-6, "expected 3.0, got {}", avg[1]);
        assert!((avg[2] - 4.0).abs() < 1e-6, "expected 4.0, got {}", avg[2]);
    }

    #[test]
    fn embedding_average_single_vector_unchanged() {
        let a = vec![0.5f32, 1.5, 2.5];
        let embeddings: Vec<Vec<f32>> = vec![a.clone()];
        let dim = embeddings[0].len();
        let count = embeddings.len() as f32;
        let mut avg = vec![0.0f32; dim];
        for emb in &embeddings {
            for (i, v) in emb.iter().enumerate() {
                avg[i] += v;
            }
        }
        for v in &mut avg {
            *v /= count;
        }
        assert_eq!(avg, a);
    }

    // ── GET /v1/graphs/{graph_id}/merge-review (judgement graph, PR4) ─────────
    //
    // The no-DB tier: cursor codec + route wiring/auth. Everything that needs
    // a real `reef_judgement` table (live-only filtering, queue order,
    // pagination, the flag gate) lives in
    // `backend/tests/judgement_graph_worker_pg.rs`.

    fn merge_review_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/graphs/{graph_id}/merge-review", get(list_merge_review))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    #[test]
    fn merge_review_cursor_round_trips() {
        let ts = chrono::Utc::now();
        let id = Uuid::new_v4();
        let decoded = decode_merge_review_cursor(&encode_merge_review_cursor(ts, id)).unwrap();
        assert_eq!(decoded.1, id);
        // RFC 3339 keeps sub-second precision; equality holds through the trip.
        assert_eq!(decoded.0, ts);
    }

    #[test]
    fn merge_review_cursor_rejects_garbage() {
        assert!(decode_merge_review_cursor("").is_none());
        assert!(decode_merge_review_cursor("not-a-cursor").is_none());
        assert!(decode_merge_review_cursor("2026-01-01T00:00:00Z|not-a-uuid").is_none());
        assert!(
            decode_merge_review_cursor("not-a-ts|00000000-0000-0000-0000-000000000000").is_none()
        );
    }

    #[tokio::test]
    async fn list_merge_review_no_auth_returns_401() {
        let router = merge_review_router(test_state());
        let graph_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/graphs/{graph_id}/merge-review"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // Not 404 proves the route is REGISTERED; require_auth fires first, so the
    // status here is the auth layer's, not the handler's.
    #[tokio::test]
    async fn list_merge_review_reachable() {
        let router = merge_review_router(test_state());
        let graph_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/graphs/{graph_id}/merge-review?limit=5"))
            .header("Authorization", "Bearer tok")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── merge_node's judgement-error mapping (PR4) ───────────────────────────

    #[test]
    fn judgement_endpoint_missing_maps_to_404_not_500() {
        let e = crate::store::StoreError::JudgementEndpointMissing { graph_id: Uuid::new_v4() };
        let (status, body) = judgement_error_response(&e).expect("must be mapped");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], json!("reef pair no longer exists"));
    }

    #[test]
    fn judgement_conflicts_map_to_409_and_carry_the_judgement_id() {
        let judgement_id = Uuid::new_v4();
        for e in [
            crate::store::StoreError::HumanJudgementExists { judgement_id },
            crate::store::StoreError::JudgementMergeMismatch { judgement_id },
        ] {
            let (status, body) = judgement_error_response(&e).expect("must be mapped");
            assert_eq!(status, StatusCode::CONFLICT, "for {e}");
            assert_eq!(body["judgement_id"], json!(judgement_id));
            // The error's own context, not a flattened placeholder.
            assert_eq!(body["error"], json!(e.to_string()));
        }
    }

    #[test]
    fn non_judgement_store_errors_keep_the_500() {
        let e = crate::store::StoreError::Db(Box::from("connection reset"));
        assert!(
            judgement_error_response(&e).is_none(),
            "an outage must not be reclassified as a user-facing 404/409"
        );
    }
}
