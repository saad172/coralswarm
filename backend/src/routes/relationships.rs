//! Relationship review-queue routes (ADR-0008 Phase 2).
//!
//! The human-in-the-loop surface over the typed relationship graph: list
//! inferred/suggested edges for an ocean, then confirm or reject each one. A
//! rejected edge is retained as a negative constraint so the inference worker
//! does not re-propose it.
//!
//! # Endpoints
//! - `GET  /v1/graphs/{graph_id}/relationships?status=suggested` — one keyset
//!   page of edges by review status (default `suggested`), enriched with
//!   src/dst reef titles; `&limit=` (1..200, default 100), `&cursor=` (opaque,
//!   from the previous response's `next_cursor`), `&predicate=` filter.
//! - `POST /v1/relationships/{edge_id}/confirm`                  — set status=Confirmed
//! - `POST /v1/relationships/{edge_id}/reject`                   — set status=Rejected
//! - `GET  /v1/review/count?graph_id=`                           — the sidebar badge's
//!   `{decisions, suggested_edges, merge_reviews}` totals as three COUNTs,
//!   replacing its full-row list fetches per navigation. `merge_reviews` is
//!   the judgement graph's live UNSURE queue (PR4) and is `0` whenever
//!   `JUDGEMENT_GRAPH_WRITES != on`.
//!
//! Authorization mirrors the rest of the API (ADR-0007 D9): an ocean / edge is
//! reachable when it lives in the caller's personal ocean, their org ocean, or a
//! team they belong to — checked via `graph_reachable_by_user` /
//! `list_reachable_graphs`.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.2)
//! Every query in this file that touches an RLS-covered consumer table
//! (`relationship_edge`/`nodes`, migration `0083_consumer_rls`) runs inside ONE
//! [`crate::tenancy::scope::begin_account_scoped`] transaction per request,
//! through the Store's `_scoped` twins, so the routes keep working once OPS-4
//! switches the app's connection role to the non-`BYPASSRLS` `coralswarm_app`.
//! Deliberately OUTSIDE the transaction:
//!   * `ReachabilityResolver` — the permanent multi-account SECURITY DEFINER
//!     carve-out (migration 0084); runs before/alongside scoping, never inside.
//!   * `Store::graph_account_id` — the SECURITY DEFINER bootstrap resolver
//!     that DISCOVERS the account to scope by.
//!   * no pool (`tenant.pool == None`): opening the scoped transaction fails
//!     and the handler returns a 500; there is no unscoped fallback.

use std::collections::HashMap;

use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use chrono::{DateTime, Utc};

use crate::relationships::{predicates, EdgeStatus};
use crate::workers::entity_resolution::{judgement_write_mode, JudgementWriteMode};
use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

// Reads through the `Tenant` extractor's resolved store (org-tenancy PR 1.5)
// rather than ambient `state.store` — `relationship_edge`/`nodes`/`graphs` are
// tenant-tree tables.
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

/// The request's one account-scoped transaction, when the content plane is
/// Postgres. `None` when no Postgres pool (and no RLS) exists — see the
/// module doc's RLS section.
type ScopedTx = Option<sqlx::Transaction<'static, sqlx::Postgres>>;

/// Open the request's account-scoped transaction (org-tenancy OPS-4 phase
/// B.2), or early-return a 500. `None` (not an error) when the request has no
/// Postgres pool at all. On the org plane this opens on the TENANT pool via
/// `Tenant::begin_member_scoped`, which also sets `app.user_id`.
///
/// That GUC has real policies to satisfy there. Whole-database isolation is
/// real, but it covers the CROSS-ORG boundary only, and it is NOT a reason to
/// skip the scope: `migrations_tenant/0009_tenant_rls` puts member-ocean
/// policies on `relationship_edge` and `nodes`, this file's two tables. Those
/// policies resolve through `tenant_rls_visible_graph_ids()`, and with neither
/// `app.user_id` nor `app.tenant_actor` set `tenant_rls_actor_user_id()` is
/// NULL — so an edge listing keeps the org and personal oceans and loses every
/// member and team ocean. The GUC is unevaluated today for a ROLE reason, not
/// a policy one: tenant pools connect as the table owner. Phase 3 (`OPS-4-T`)
/// repoints each tenant DSN at the non-owner `coralswarm_tenant_app` role,
/// which is what makes it bite. See `tenancy::scope`'s module doc.
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

/// Server-side page clamp for the edge list, mirroring `/v1/candidates`'s
/// 1..200 convention (`routes::nodes`).
const MAX_PAGE: i64 = 200;
/// Default page size when the caller sends no `limit`.
const DEFAULT_PAGE: i64 = 100;

// ── GET /v1/graphs/{graph_id}/relationships ───────────────────────────────────

/// `created_at|edge_id` keyset cursor, same shape as
/// `routes::journal::encode_hour_items_cursor`. Opaque to callers: hand back
/// `next_cursor` verbatim.
fn encode_edges_cursor(created_at: DateTime<Utc>, edge_id: Uuid) -> String {
    format!("{}|{edge_id}", created_at.to_rfc3339())
}

fn decode_edges_cursor(cursor: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let (ts, id) = cursor.split_once('|')?;
    let ts = DateTime::parse_from_rfc3339(ts).ok()?.with_timezone(&Utc);
    let id = Uuid::parse_str(id).ok()?;
    Some((ts, id))
}

#[derive(Debug, Deserialize)]
pub struct ListRelationshipsQuery {
    /// Review status to list: `suggested` (default) | `confirmed` | `rejected`.
    pub status: Option<String>,
    /// Narrow to one predicate (must be in the registry). Absent = all.
    pub predicate: Option<String>,
    /// Page size, clamped 1..200 (default 100).
    pub limit: Option<i64>,
    /// Opaque keyset cursor from a previous response's `next_cursor`.
    pub cursor: Option<String>,
}

pub async fn list_relationships(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Query(params): Query<ListRelationshipsQuery>,
) -> (StatusCode, Json<Value>) {
    // ── Parse the status filter (default: suggested) ──────────────────────────
    let status = match params.status.as_deref() {
        None => EdgeStatus::Suggested,
        Some(s) => match EdgeStatus::parse(s) {
            Some(st) => st,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "status must be one of: suggested, confirmed, rejected"})),
                )
            }
        },
    };

    let limit = params.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    // Registry-validated predicate filter: a typo'd predicate can never match
    // anything, so refuse it loudly instead of returning a silently-empty list.
    let predicate = match params.predicate.as_deref() {
        None => None,
        Some(p) if predicates::is_known(p) => Some(p.to_string()),
        Some(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "unknown predicate"})),
            )
        }
    };
    let cursor = match params.cursor.as_deref() {
        None => None,
        Some(c) => match decode_edges_cursor(c) {
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

    // ── IDOR: the ocean must be reachable by the caller (ADR-0007 D9), via
    // `ReachabilityResolver` with the request's real decision (org-tenancy
    // PR 1.5) ─────────────────────────────────────────────────────────────────
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
                Json(json!({"error": "graph not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_relationships reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // ── Scope account: the ocean's owning account (a reachable ocean may be a
    // team ocean owned by a different account than the caller's active one).
    // `graph_account_id` is the SECURITY DEFINER bootstrap resolver (migration
    // 0084) — deliberately callable BEFORE any scope exists. ──────────────────
    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        // Reachability above already proved the graph exists, so this arm is
        // effectively unreachable; treated as the same non-leaking refusal.
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "graph not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_relationships account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx = begin_scoped_tx!(tenant, account_id);

    // ── One page of edges in this ocean with the requested review status.
    // Fetch one extra row to compute `has_more`/`next_cursor` without a second
    // query — same trick `routes::sessions`/`routes::journal` use. ───────────
    let rows = match scoped_store!(
        store,
        scoped_tx,
        list_relationship_edges_by_status_page
            / list_relationship_edges_by_status_page_scoped(
                graph_id,
                status,
                predicate.as_deref(),
                cursor,
                limit + 1,
            )
    ) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "list_relationships edge query error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let has_more = rows.len() as i64 > limit;
    let edges: Vec<_> = rows.into_iter().take(limit as usize).collect();
    let next_cursor = if has_more {
        edges.last().map(|e| encode_edges_cursor(e.created_at, e.edge_id))
    } else {
        None
    };

    // ── Reef titles for a usable review UI — just this page's endpoints, not
    // the whole ocean's nodes. Best-effort: a missing entry renders a null
    // title. ──────────────────────────────────────────────────────────────────
    let mut wanted: Vec<Uuid> = edges
        .iter()
        .flat_map(|e| [e.src_node_id, e.dst_node_id])
        .collect();
    wanted.sort_unstable();
    wanted.dedup();
    let title_by_node: HashMap<Uuid, String> = match scoped_store!(
        store,
        scoped_tx,
        nodes_by_ids / nodes_by_ids_scoped(graph_id, &wanted)
    ) {
        Ok(pairs) => pairs.into_iter().collect(),
        Err(e) => {
            tracing::error!(error = %e, "list_relationships title lookup error");
            HashMap::new()
        }
    };

    // ── Consequence-first enrichment (all best-effort: a failure degrades the
    // card copy, never the list) ──────────────────────────────────────────────
    // Structural impact for this page's endpoints (why the item queued),
    let impact_by_node: HashMap<Uuid, crate::review_policy::NodeImpact> = match scoped_store!(
        store,
        scoped_tx,
        node_structural_impact / node_structural_impact_scoped(graph_id, &wanted)
    ) {
        Ok(rows) => rows.into_iter().map(|i| (i.node_id, i)).collect(),
        Err(e) => {
            tracing::warn!(error = %e, "list_relationships impact lookup error");
            HashMap::new()
        }
    };
    // the deadline clock (queued_at is cloud-only, outside RelationshipEdge),
    let queued_by_edge: HashMap<Uuid, DateTime<Utc>> = {
        let ids: Vec<Uuid> = edges.iter().map(|e| e.edge_id).collect();
        match scoped_store!(store, scoped_tx, edge_queued_at / edge_queued_at_scoped(graph_id, &ids)) {
            Ok(rows) => rows.into_iter().filter_map(|(id, q)| q.map(|q| (id, q))).collect(),
            Err(e) => {
                tracing::warn!(error = %e, "list_relationships queued_at lookup error");
                HashMap::new()
            }
        }
    };
    // and the last 30 days of ask texts, matched against endpoint titles in
    // memory ("cited in N answers this month" — honest ILIKE-grade proxy;
    // query_history stores no citation ids).
    let recent_asks: Vec<String> = match scoped_store!(
        store,
        scoped_tx,
        recent_query_texts
            / recent_query_texts_scoped(graph_id, Utc::now() - chrono::Duration::days(30), 500)
    ) {
        Ok(texts) => texts,
        Err(e) => {
            tracing::warn!(error = %e, "list_relationships recent-asks lookup error");
            Vec::new()
        }
    };
    let recent_asks_lc: Vec<(String, &str)> =
        recent_asks.iter().map(|t| (t.to_lowercase(), t.as_str())).collect();

    commit_read_tx(scoped_tx).await;

    let out: Vec<Value> = edges
        .into_iter()
        .map(|e| {
            // Impact = max over the edge's two endpoints (mirrors the write
            // gate); recent asks = texts mentioning either endpoint title.
            let (mut degree, mut corals, mut conflict) = (0i64, 0i64, false);
            for id in [e.src_node_id, e.dst_node_id] {
                if let Some(i) = impact_by_node.get(&id) {
                    degree = degree.max(i.degree);
                    corals = corals.max(i.coral_count);
                    conflict = conflict || i.conflict_involved;
                }
            }
            let mut ask_hits = 0usize;
            let mut sample_ask: Option<&str> = None;
            let titles_lc: Vec<String> = [e.src_node_id, e.dst_node_id]
                .iter()
                .filter_map(|id| title_by_node.get(id))
                .map(|t| t.to_lowercase())
                .filter(|t| t.len() >= 3)
                .collect();
            if !titles_lc.is_empty() {
                for (ask_lc, ask) in &recent_asks_lc {
                    if titles_lc.iter().any(|t| ask_lc.contains(t.as_str())) {
                        ask_hits += 1;
                        sample_ask.get_or_insert(ask);
                    }
                }
            }
            let queued_at = queued_by_edge.get(&e.edge_id).copied();
            let deadline_at = queued_at
                .map(|q| q + chrono::Duration::days(crate::review_policy::REVIEW_DEADLINE_DAYS));
            json!({
                "edge_id":      e.edge_id,
                "src_node_id":  e.src_node_id,
                "dst_node_id":  e.dst_node_id,
                "src_title":    title_by_node.get(&e.src_node_id),
                "dst_title":    title_by_node.get(&e.dst_node_id),
                "predicate":    e.predicate,
                "origin":       e.origin.as_str(),
                "status":       e.status.as_str(),
                "confidence":   e.confidence,
                "provenance":   e.provenance,
                "created_at":   e.created_at,
                "updated_at":   e.updated_at,
                "impact": {
                    "degree": degree,
                    "corals": corals,
                    "contradiction": conflict,
                    "recent_asks": ask_hits,
                    "sample_ask": sample_ask,
                },
                "queued_at":    queued_at,
                "deadline_at":  deadline_at,
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "edges": out,
            "has_more": has_more,
            "next_cursor": next_cursor,
        })),
    )
}

// ── POST /v1/relationships/{edge_id}/confirm · /reject ────────────────────────
//
// The store's `set_relationship_edge_status` is unguarded (no account/user
// param) and there is no get-edge-by-id helper, so we authorize by locating the
// edge inside the caller's reachable oceans, then require that the caller may
// WRITE that ocean (`routes::nodes::graph_writable_by`: own private ocean, or a
// shared ocean an owner/admin moderates); otherwise → 404. This keeps the IDOR
// check in the route without touching the store layer.

/// The account that OWNS this relationship edge, via migration 0105's SECURITY
/// DEFINER bootstrap — `None` with no pool or when the
/// edge does not exist. Bare account id: edge → graph → account, no content.
async fn edge_owner_account(tenant: &Tenant, edge_id: Uuid) -> Option<Uuid> {
    let pool = tenant.pool.as_ref()?;
    sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_relationship_edge_account_id($1)")
        .bind(edge_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .flatten()
}

async fn review_edge(
    tenant: Tenant,
    user: UserContext,
    edge_id: Uuid,
    new_status: EdgeStatus,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // Scope: the edge's OWNING account, via migration 0105's
    // `resolve_relationship_edge_account_id` bootstrap.
    //
    // This used to scope to the caller's ACTIVE account, on the reasoning that it
    // was "the only account known before the edge row is loaded (there is no
    // edge→account SECURITY DEFINER bootstrap resolver, deliberately)". That
    // resolver now exists, and the old behaviour was not fail-closed in the
    // harmless direction: under live RLS an edge whose ocean belongs to a
    // different account than the caller's active one was filtered to "absent",
    // so `GET /v1/relationships` (graph-anchored — it resolves the ocean owner)
    // listed edges that Confirm/Reject then 404'd. The review queue rendered rows
    // the user could not act on.
    //
    // Authorization is UNCHANGED and does not come from the scope: the
    // `graph_reachable_by_user` check below still gates every edge, and remains
    // the permanent multi-account SECURITY DEFINER carve-out. Widening RLS
    // visibility only lets that check run. An unresolvable id falls back to the
    // caller's account, preserving the existing non-leaking 404.
    let scope_account = edge_owner_account(&tenant, edge_id)
        .await
        .unwrap_or(user.account_id);
    let mut scoped_tx = begin_scoped_tx!(tenant, scope_account);

    // ── IDOR: the edge must live in an ocean the caller can reach ─────────────
    // O(1): one indexed PK lookup for the edge, then a single reachability
    // check on its ocean. The reachability half runs OUTSIDE the scoped
    // transaction — it is the permanent multi-account SECURITY DEFINER
    // carve-out (migration 0084), threaded through `ReachabilityResolver`
    // with the request's real decision (org-tenancy PR 1.5).
    let edge = match scoped_store!(
        store,
        scoped_tx,
        get_relationship_edge / get_relationship_edge_scoped(edge_id)
    ) {
        Ok(Some(e)) => e,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "relationship edge not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "review_edge edge lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        edge.graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "relationship edge not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "review_edge reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }
    // Reachable is not writable (settings 2.10): reviewing an edge rewrites the
    // ocean's graph, so only the caller's own private ocean — or a shared
    // ocean an owner/admin moderates — qualifies. Same 404 as above.
    let writer = crate::routes::nodes::ReefWriter::rest(&user, tenant.decision.as_ref());
    match crate::routes::nodes::graph_writable_by(store.as_ref(), &mut scoped_tx, edge.graph_id, &writer).await {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "relationship edge not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "review_edge write-scope error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    match scoped_store!(
        store,
        scoped_tx,
        set_relationship_edge_status
            / set_relationship_edge_status_scoped(edge_id, new_status, Some(user.user_id), "web")
    ) {
        Ok(()) => {
            commit_write_tx!(scoped_tx);
            (
                StatusCode::OK,
                Json(json!({"edge_id": edge_id, "status": new_status.as_str()})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "review_edge set status error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
        }
    }
}

pub async fn confirm_relationship(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(edge_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    review_edge(tenant, user, edge_id, EdgeStatus::Confirmed).await
}

pub async fn reject_relationship(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(edge_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    review_edge(tenant, user, edge_id, EdgeStatus::Rejected).await
}

// ── GET /v1/review/count ──────────────────────────────────────────────────────
//
// One cheap call for the sidebar badge: pending decision candidates (every
// ocean the caller can read, like `GET /v1/candidates`) + suggested relationship edges (ocean-
// scoped, like the list above) + live UNSURE merge reviews (ocean-scoped,
// judgement graph PR4), as three COUNTs instead of full-row list fetches on
// every route navigation.
//
// `merge_reviews` is what the retired lexical merge-SUGGESTION count could
// never be: `count_live_unsure_judgements` is one `count(*)` over a partial
// index, not an O(N²) title sweep, which is why the badge can afford it now
// and could not then (`web/components/useReviewCount.ts` carries the same
// correction). It shares the edge count's graph-anchored scope block — same
// ocean, same owning account, one transaction.
//
// Gated on `JUDGEMENT_GRAPH_WRITES == On` for the SAME reason
// `routes::nodes::list_merge_review` is: with the flag off neither resolution
// route writes a judgement, so nothing could retire a counted row. A badge
// that counts an undrainable queue is worse than a badge that counts zero.

#[derive(Debug, Deserialize)]
pub struct ReviewCountQuery {
    /// The ocean whose suggested edges to count (must be reachable by the
    /// caller). The decisions count spans every ocean the caller can read.
    pub graph_id: Uuid,
}

pub async fn review_count(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<ReviewCountQuery>,
) -> (StatusCode, Json<Value>) {
    let graph_id = params.graph_id;
    let store = require_store!(tenant);

    // IDOR: same reachability gate as the edge list above.
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
                Json(json!({"error": "graph not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "review_count reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Decisions: the caller's active account, narrowed to the oceans they can
    // READ (settings 2.12) — exactly `GET /v1/candidates`' scope, so the
    // badge never counts another member's private decisions.
    let readable = match ReachabilityResolver::readable_graph_ids(
        tenant.decision.as_ref(),
        store.as_ref(),
        user.user_id,
    )
    .await
    {
        Ok(ids) => ids,
        Err(e) => {
            tracing::error!(error = %e, "review_count readable-oceans error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx = begin_scoped_tx!(tenant, user.account_id);
    let decisions = match scoped_store!(
        store,
        scoped_tx,
        count_pending_decision_candidates
            / count_pending_decision_candidates_scoped(user.account_id, &readable)
    ) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "review_count decisions query error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    commit_read_tx(scoped_tx).await;

    // Edges: scoped by the ocean's OWNING account (a reachable ocean may be a
    // team ocean owned by a different account), mirroring the edge list above.
    let edge_account = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "graph not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "review_count account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx = begin_scoped_tx!(tenant, edge_account);
    let suggested_edges = match scoped_store!(
        store,
        scoped_tx,
        count_relationship_edges_by_status
            / count_relationship_edges_by_status_scoped(graph_id, EdgeStatus::Suggested)
    ) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "review_count edges query error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    // Same ocean, same scope block — see this section's header for the flag
    // gate's rationale and for why this count is cheap enough to belong here.
    let merge_reviews = if judgement_write_mode() == JudgementWriteMode::On {
        match scoped_store!(
            store,
            scoped_tx,
            count_live_unsure_judgements / count_live_unsure_judgements_scoped(graph_id)
        ) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(error = %e, "review_count merge-review query error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    } else {
        0
    };
    commit_read_tx(scoped_tx).await;

    (
        StatusCode::OK,
        Json(json!({
            "decisions": decisions,
            "suggested_edges": suggested_edges,
            "merge_reviews": merge_reviews,
        })),
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::{get, post},
        Router,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::AppState;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    #[test]
    fn edges_cursor_round_trips() {
        let ts = Utc::now();
        let id = Uuid::new_v4();
        let decoded = decode_edges_cursor(&encode_edges_cursor(ts, id)).unwrap();
        assert_eq!(decoded.1, id);
        // RFC 3339 keeps sub-second precision; equality holds through the trip.
        assert_eq!(decoded.0, ts);
    }

    #[test]
    fn edges_cursor_rejects_garbage() {
        assert!(decode_edges_cursor("").is_none());
        assert!(decode_edges_cursor("not-a-cursor").is_none());
        assert!(decode_edges_cursor("2026-01-01T00:00:00Z|not-a-uuid").is_none());
        assert!(decode_edges_cursor("not-a-ts|00000000-0000-0000-0000-000000000000").is_none());
    }

    fn relationships_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route(
                "/v1/graphs/{graph_id}/relationships",
                get(list_relationships),
            )
            .route("/v1/review/count", get(review_count))
            .route(
                "/v1/relationships/{edge_id}/confirm",
                post(confirm_relationship),
            )
            .route(
                "/v1/relationships/{edge_id}/reject",
                post(reject_relationship),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn list_relationships_no_auth_returns_401() {
        let router = relationships_router(test_state());
        let graph_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/graphs/{graph_id}/relationships"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn confirm_relationship_no_auth_returns_401() {
        let router = relationships_router(test_state());
        let edge_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("POST")
            .uri(format!("/v1/relationships/{edge_id}/confirm"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn reject_relationship_no_auth_returns_401() {
        let router = relationships_router(test_state());
        let edge_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("POST")
            .uri(format!("/v1/relationships/{edge_id}/reject"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn review_count_no_auth_returns_401() {
        let router = relationships_router(test_state());
        let graph_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/review/count?graph_id={graph_id}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // 503 (not 404) proves the routes are registered; require_auth fires first.
    #[tokio::test]
    async fn list_relationships_reachable() {
        let router = relationships_router(test_state());
        let graph_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/graphs/{graph_id}/relationships?status=suggested"))
            .header("Authorization", "Bearer tok")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn confirm_relationship_reachable() {
        let router = relationships_router(test_state());
        let edge_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("POST")
            .uri(format!("/v1/relationships/{edge_id}/confirm"))
            .header("Authorization", "Bearer tok")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(resp.status(), StatusCode::NOT_FOUND);
    }
}
