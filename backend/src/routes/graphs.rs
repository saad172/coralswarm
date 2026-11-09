use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver, AppState,
};

/// Char cap on a coral's inline-content-derived `preview` (`list_atoms` /
/// `list_sources`'s `corals[]`). Named so both endpoints share one bound
/// instead of two separately-hand-tuned bare `300`s (title-always-present
/// PR) — a future change to the preview length only needs one edit, and a
/// reader of either handler sees the SAME number came from the SAME decision.
const ATOM_PREVIEW_MAX_CHARS: usize = 300;

// ── Request types ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateGraphRequest {
    pub name: String,
    /// Ocean visibility level (ADR-0006). Defaults to `"personal"`. Only
    /// `"personal"` and `"org"` are accepted here — `"team"` oceans are created
    /// through the teams API (which also provisions the team-key envelopes).
    #[serde(default)]
    pub kind: Option<String>,
}

/// Render a graph (ocean) as the API JSON shape — shared by list/get/create so
/// `kind` and `is_primary` (ADR-0006) are always present.
///
/// `owner_user_id`/`archived_at` (member-private-oceans PR 2, plan §2.1/§8)
/// surface unconditionally, same as every other field here — no row can have
/// either set today (nothing creates a `kind = 'member'` row before PR 4, and
/// nothing archives before PR 7), so this is a no-op shape addition: existing
/// clients see two new `null` fields, nothing more, until the flag flips.
///
/// `team_id` (workspace-members team-id fix): the id every `/v1/teams/*`
/// route (`routes::teams`) actually expects, DISTINCT from `id` above
/// (`graphs.id` — what every OTHER route calls the ocean id). Before this
/// field existed, the Members panel (`web/components/workspaces/members/`)
/// had nothing but `id` to send to `GET /v1/teams/{team_id}/members` and
/// friends, which 404'd "team not found" for every shared workspace even
/// though the workspace plainly existed — `id` and `team_id` are two
/// different primary keys (`backend/migrations_control/0022_teams.sql:
/// 35-42`). `null` for every ocean whose `kind != "team"`, and `Some` for a
/// `kind == "team"` ocean unless its `teams` row is somehow missing (should
/// not happen — see `store::Graph::team_id`'s own doc comment).
///
/// `my_team_role` (workspace-manage design doc's "Making role cheap"
/// section): the CALLER's role (`"admin"` | `"member"`) in the team
/// `team_id` identifies — `null` when `team_id` is `null` (not a team ocean)
/// OR the caller is not an active member of it. Only ever non-null off
/// `GET /v1/graphs` (backed by `ReachabilityResolver::list_reachable_graphs`,
/// which threads the caller's `user.user_id` through); every other route
/// that renders through this same helper has no caller to compute a role
/// for and always sends `null` here — see `store::Graph::my_team_role`'s own
/// doc comment for exactly which paths those are.
fn graph_json(g: &crate::store::Graph) -> Value {
    json!({
        "id": g.id,
        "name": g.name,
        "kind": g.kind,
        "is_primary": g.is_primary,
        "created_at": g.created_at,
        "owner_user_id": g.owner_user_id,
        "archived_at": g.archived_at,
        "team_id": g.team_id,
        "my_team_role": g.my_team_role,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Content-plane store, resolved by the `Tenant` extractor (org-tenancy PR
// 1.4) — `graphs`/`atoms`/etc. are tenant-tree tables. `account_is_real_org`
// below is the one exception in this file: it reads the CONTROL-plane
// `accounts` table and always uses `state.store` directly, never this macro.
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

/// The control-plane store for calls that legitimately belong on it (e.g.
/// `account_is_real_org`, which reads `accounts` — control tree, not tenant tree).
/// Distinct from `require_store!`, which resolves the tenant-scoped content
/// store. A 503 here means the ambient consumer/control store isn't
/// configured at all (a misconfigured deploy) — the
/// same condition `require_store!` already guards for content access.
macro_rules! require_control_store {
    ($state:expr) => {
        match &$state.store {
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

// ── GET /v1/graphs ────────────────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/graphs",
    tag = "graphs",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "List of graphs", body = Value),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_graphs(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // The D9 read union (ADR-0007): every ocean the user can read across their
    // personal account, org account(s), and team memberships — not just the
    // active account. This is what makes promoted (shared) oceans reach readers.
    //
    // RLS note (org-tenancy B.1): this handler's ONLY query is the
    // reachability union, which is the PR #284 SECURITY DEFINER carve-out
    // (`resolve_reachable_graphs`) — a deliberately multi-account read that
    // must NOT gain a single `app.account_id` predicate. The resolver uses
    // the request read executor for READ ONLY + 1s without narrowing that union.
    let graphs = match ReachabilityResolver::list_reachable_graphs(
        tenant.decision.as_ref(),
        store.as_ref(),
        user.user_id,
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, "list_graphs store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    let graphs: Vec<Value> = graphs.iter().map(graph_json).collect();

    (StatusCode::OK, Json(json!(graphs)))
}

// ── POST /v1/graphs ───────────────────────────────────────────────────────────

#[utoipa_path(
    post,
    path = "/v1/graphs",
    tag = "graphs",
    security(("bearer_token" = [])),
    responses(
        (status = 201, description = "Graph created", body = Value),
        (status = 400, description = "Invalid request body"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn create_graph(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<CreateGraphRequest>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    let name = body.name.trim().to_owned();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "name is required"})),
        );
    }

    // Ocean kind (ADR-0006). Team oceans go through the teams API, so only
    // 'personal' and 'org' are creatable here.
    let kind = body.kind.as_deref().unwrap_or("personal");
    if kind != "personal" && kind != "org" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "kind must be 'personal' or 'org'"})),
        );
    }

    // A personal ocean must never be created under a REAL org account — it
    // would be returned to every org member by the reachable-ocean union
    // (ADR-0007 D2: private oceans live only in the user's personal
    // account-of-one). A personal org (control migration 0208
    // `personal_orgs`) is exempted: it has no `tenant_databases` row and no
    // database of its own, so it always resolves to the caller's own
    // account-of-one on the shared plane, and "switch to your personal
    // account" below is unreachable advice for it — adoption
    // (`bin/adopt_personal_org_account.rs`) relabels the user's ONLY personal
    // account, so there is no other personal account left to switch to.
    //
    // `account_is_real_org` reads the CONTROL-plane `accounts` table
    // (`clerk_org_id IS NOT NULL`, joined against `personal_orgs`), which
    // doesn't exist on a tenant database — this always goes through
    // `require_control_store!`/`state.store`, never the tenant-scoped `store`
    // this handler otherwise uses (org-tenancy PR 1.4: the split is
    // content-plane only).
    //
    // `account_is_real_org` alone answers a CALLER-relative question ("is
    // this org real from THIS caller's point of view") and reads `false` for
    // a personal org's account unconditionally — but the D2 invariant this
    // whole check exists to enforce ("a `kind='personal'` ocean must not sit
    // in an account more than one person can read") is a property of the
    // ACCOUNT, not the caller. `resolve_reachable_graphs` (control migration
    // `0132_reachable_graphs_team_id.sql`) returns every `kind='personal'`
    // ocean to EVERY `account_members` row of the account, with no further
    // filter — so a personal org that has (against its own one-member
    // design) gained a second member would leak a newly-created personal
    // ocean to that second member the moment this check trusted
    // `account_is_real_org` alone. `account_has_multiple_members` closes
    // that gap by checking the account itself (PR 829 review, finding 1).
    if kind == "personal" {
        let control_store = require_control_store!(state);
        match control_store.account_is_real_org(user.account_id, user.user_id).await {
            Ok(true) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "cannot create a personal ocean in an organization; switch to your personal account"})),
                );
            }
            Ok(false) => match control_store.account_has_multiple_members(user.account_id).await {
                Ok(true) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "cannot create a personal ocean in an organization; switch to your personal account"})),
                    );
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(error = %e, "create_graph account-membership-count check error");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "internal error"})),
                    );
                }
            },
            Err(e) => {
                tracing::error!(error = %e, "create_graph account-kind check error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    }

    // ── RLS scoping (org-tenancy B.1) ─────────────────────────────────────────
    // `graphs` is RLS-covered (migration 0083): the org-uniqueness check and
    // the INSERT run inside ONE `begin_account_scoped` transaction on the
    // caller's account. A pool-less `Tenant` keeps the plain Store path.
    let mut scoped_tx = match &tenant.pool {
        Some(_) => match tenant.begin_member_scoped(user.account_id).await {
            Ok(tx) => Some(tx),
            Err(e) => {
                tracing::error!(error = %e, "create_graph: begin_account_scoped failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        },
        None => None,
    };

    // One organization ocean per account: surface a clear 409 rather than the
    // partial-unique-index violation that would otherwise bubble up as a 500.
    if kind == "org" {
        let existing = match scoped_tx.as_mut() {
            Some(tx) => store.list_graphs_scoped(tx, user.account_id).await,
            None => store.list_graphs(user.account_id).await,
        };
        match existing {
            Ok(existing) if existing.iter().any(|g| g.kind == "org") => {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({"error": "an organization ocean already exists"})),
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, "create_graph org-check store error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    }

    let created = match scoped_tx.as_mut() {
        Some(tx) => store.create_graph_scoped(tx, user.account_id, &name, kind).await,
        None => store.create_graph(user.account_id, &name, kind).await,
    };
    let graph = match created {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, "create_graph store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    if let Some(tx) = scoped_tx.take() {
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "create_graph: scoped commit failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    (StatusCode::CREATED, Json(graph_json(&graph)))
}

// ── GET /v1/graphs/:graph_id ──────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/graphs/{graph_id}",
    tag = "graphs",
    params(("graph_id" = Uuid, Path, description = "Graph identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Graph details", body = Value),
        (status = 404, description = "Graph not found"),
    )
)]
pub async fn get_graph(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // IDOR guard (Group-B security fix, PR 0 of the member-private-oceans
    // plan §1.4(c)/item 1): this route used to call `get_graph_scoped`/
    // `get_graph` scoped ONLY to `user.account_id` — the underlying query is
    // literally `WHERE id = $1 AND account_id = $2`, an account-OWNERSHIP
    // check with no team gate at all. Any member of the account could fetch
    // ANY graph belonging to it, including a `kind='team'` ocean whose
    // `team_members` row they never had — reachable here even though every
    // other team-ocean-aware route (e.g. `get_ocean_stats` below,
    // `list_reachable_graphs`) already denies them. Route through
    // `ReachabilityResolver` FIRST — the one authoritative "may this user
    // touch this graph" predicate (`tenancy/reachability.rs`), which DOES
    // gate `kind='team'` on `team_members` on both planes. Same pattern
    // `get_ocean_stats` already uses; this brings `get_graph` in line with it.
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
                Json(json!({"error": "graph not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "get_graph reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // RLS scoping (org-tenancy B.1): fetch scoped to the graph's OWNING
    // account, not the caller's — reachability above may have admitted a
    // reader whose own account differs from the graph's (a promoted/shared
    // ocean via the D9 union), so `user.account_id` would be the wrong RLS
    // scope and the read would come back empty even though access was
    // legitimately granted. Resolved via the same `graph_account_id`
    // SECURITY DEFINER bootstrap `get_ocean_stats` uses. With no pool,
    // `begin_member_scoped` fails and this returns a 500.
    let row = {
        let owner_account = match store.graph_account_id(graph_id).await {
            Ok(Some(acc)) => acc,
            Ok(None) => {
                // Reachability passed above, so a vanished graph here is a
                // race with deletion — same opaque 404 as unreachable.
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "graph not found"})),
                );
            }
            Err(e) => {
                tracing::error!(error = %e, "get_graph: graph_account_id error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        let mut tx = match tenant.begin_member_scoped(owner_account).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, "get_graph: begin_account_scoped failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        let row = match store.get_graph_scoped(&mut tx, graph_id, owner_account).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "get_graph store error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "get_graph: scoped commit failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
        row
    };

    match row {
        Some(g) => (StatusCode::OK, Json(graph_json(&g))),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "graph not found"})),
        ),
    }
}

// ── GET /v1/graphs/:graph_id/atoms ────────────────────────────────────────────

/// Query params for `GET /v1/graphs/{graph_id}/atoms` (CS-reefs-raw-material-
/// all-time). Both optional so a caller that doesn't pass them sees the exact
/// same first-100-newest shape the route always returned.
#[derive(Debug, Deserialize)]
pub struct ListAtomsQuery {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

#[utoipa_path(
    get,
    path = "/v1/graphs/{graph_id}/atoms",
    tag = "graphs",
    params(
        ("graph_id" = Uuid, Path, description = "Graph identifier"),
        ("page" = Option<i64>, Query, description = "Page number (default 1)"),
        ("page_size" = Option<i64>, Query, description = "Items per page (default 100, max 200)"),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "List of atoms (bare JSON array; an `X-Has-More: true|false` \
            response header indicates whether a further page exists — body shape unchanged for \
            existing consumers, same convention as GET /v1/nodes)", body = Value),
        (status = 404, description = "Graph not found"),
    )
)]
pub async fn list_atoms(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Query(params): Query<ListAtomsQuery>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    // Not `require_store!` here (unlike sibling handlers in this file): that
    // macro's early return is a bare `(StatusCode, Json<Value>)` 2-tuple,
    // which no longer matches this handler's 3-tuple return type (the extra
    // `HeaderMap` slot carries `X-Has-More`, same reason `nodes::list_nodes`
    // spells this out inline instead of using the macro). Reads through the
    // `Tenant` extractor's resolved store (org-tenancy PR 1.4), not ambient
    // `state.store`.
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

    // IDOR guard (Group-B security fix, PR 0 of the member-private-oceans
    // plan §1.4(c)/item 2): `graph_exists_for_account[_scoped]`'s query is
    // literally `WHERE id = $1 AND account_id = $2` — bare account
    // OWNERSHIP, no team gate — so any account member could list the FULL
    // CORAL CONTENT + PREVIEWS of a `kind='team'` ocean they were never
    // added to. Route through `ReachabilityResolver` instead — the one
    // predicate that gates `kind='team'` on `team_members` membership (same
    // fix as `get_graph`, above, and `get_ocean_stats`, below).
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
                HeaderMap::new(),
                Json(json!({"error": "graph not found"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "list_atoms reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // RLS scoping (org-tenancy B.1): the `atoms` reads below run in ONE
    // `begin_account_scoped` transaction on the graph's OWNING account, not
    // necessarily the caller's — reachability above may have admitted a
    // reader from a different account via the D9 union, so `user.account_id`
    // would be the wrong RLS scope (same reasoning as `get_graph`/
    // `get_ocean_stats`). A pool-less `Tenant` keeps the plain Store path.
    let mut scoped_tx = match &tenant.pool {
        Some(_) => {
            let owner_account = match store.graph_account_id(graph_id).await {
                Ok(Some(acc)) => acc,
                Ok(None) => {
                    // Reachability passed above, so a vanished graph here is
                    // a race with deletion — same opaque 404 as unreachable.
                    return (
                        StatusCode::NOT_FOUND,
                        HeaderMap::new(),
                        Json(json!({"error": "graph not found"})),
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "list_atoms: graph_account_id error");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        HeaderMap::new(),
                        Json(json!({"error": "internal error"})),
                    );
                }
            };
            match tenant.begin_member_scoped(owner_account).await {
                Ok(tx) => Some(tx),
                Err(e) => {
                    tracing::error!(error = %e, "list_atoms: begin_account_scoped failed");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        HeaderMap::new(),
                        Json(json!({"error": "internal error"})),
                    );
                }
            }
        }
        None => None,
    };

    // Pagination (CS-reefs-raw-material-all-time): defaults reproduce the
    // route's original un-paginated shape (page 1 of 100, newest first) byte
    // for byte, so any caller that doesn't pass page/page_size is unaffected.
    // A caller that pages through page=1,2,3… via `X-Has-More` can now reach
    // every atom in the graph instead of only the newest 100 — the Reefs
    // workbench's Raw material view (web/app/(app)/reefs/page.tsx) does this
    // for its "search/filter over all corals, not just the recent slice"
    // requirement. `routes/atlas.rs` is untouched — it calls the separate,
    // still-capped-at-100 `Store::list_atoms`.
    let page = params.page.unwrap_or(1).max(1);
    // Ceiling raised 200 → 1,000 (W3,
    // `docs/plans/request-reliability-and-scale.md` §6.3), for the same reason
    // as `nodes::list_nodes`: the ceiling here made `graphs.atomsAll` 20
    // sequential round trips at today's ocean size, and it is a server-side
    // limit the client cannot opt out of. At 1,000 the same drain is 4.
    //
    // CORRECTION to an earlier version of this comment, which claimed the row
    // shape is "metadata only" and that content "is never in this response".
    // That is false: this handler fetches `atom_inline_content` below to give
    // the client a human-readable preview, so a page DOES carry content — the
    // previews are truncated, not absent.
    //
    // The ceiling is still safe, but for a different and weaker reason than
    // the one originally given: previews are truncated server-side, so a
    // 1,000-row page is bounded by (rows x preview cap) rather than by row
    // metadata alone. If that truncation cap is ever raised, THIS ceiling
    // needs revisiting with it — which is exactly the link the wrong comment
    // would have hidden.
    //
    // The DEFAULT stays 100, so no existing caller changes.
    let page_size = params.page_size.unwrap_or(100).clamp(1, 1000);
    let offset = (page - 1) * page_size;

    // Fetch one extra row to compute `has_more` without a second COUNT query
    // (same pattern as `nodes::list_nodes`).
    let atoms_res = match scoped_tx.as_mut() {
        Some(tx) => store.list_atoms_page_scoped(tx, graph_id, page_size + 1, offset).await,
        None => store.list_atoms_page(graph_id, page_size + 1, offset).await,
    };
    let atoms = match atoms_res {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_atoms store error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let has_more = atoms.len() as i64 > page_size;
    let atoms: Vec<_> = atoms.into_iter().take(page_size as usize).collect();
    let ids: Vec<Uuid> = atoms.iter().map(|a| a.atom_id).collect();

    // Titles-always-present (founder requirement): `title` is guaranteed
    // non-null via `services::atom_title::display_title`, never the bare
    // `atoms.title` column, which is commonly NULL (R2-only storage predating
    // the sweep having reached this tenant, or a bulk copy that dropped it —
    // see that module's doc for the verified prod findings). Fetched through
    // the STILL-OPEN scoped transaction (CodeRabbit round 1 on #837:
    // `atom_titles_and_excerpts_scoped` didn't exist yet, so this used to run
    // ambient AFTER the commit below — a non-exempt consumer-plane RLS role
    // could silently see zero rows and `unwrap_or_default()` would degrade
    // every title on the page, never surfacing as an error).
    let titles_excerpts_res = match scoped_tx.as_mut() {
        Some(tx) => store.atom_titles_and_excerpts_scoped(tx, &ids).await,
        None => store.atom_titles_and_excerpts(&ids).await,
    };
    let titles_excerpts: std::collections::HashMap<Uuid, (Option<String>, Option<String>)> =
        titles_excerpts_res
            .unwrap_or_default()
            .into_iter()
            .map(|(id, title, excerpt)| (id, (title, excerpt)))
            .collect();

    // Scoped reads done — commit before the preview fetch below.
    // NOTE (org-tenancy B.1 known gap): `atom_inline_content` has no `_scoped`
    // twin yet (not covered by the B.0 chunks #285/#289/#290 — needs a
    // follow-up Store chunk), so the preview enrichment below still runs
    // unscoped. It is best-effort by construction (`unwrap_or_default()`):
    // post-OPS-4 it degrades to empty previews rather than leaking or failing
    // the route; converting it is tracked in the B.1 PR body.
    if let Some(tx) = scoped_tx.take() {
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "list_atoms: scoped commit failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                Json(json!({"error": "internal error"})),
            );
        }
    }

    // Fetch inline content so the client can show a human-readable preview
    // (and parse the "{sender} in #{channel} · {when}" header for provenance)
    // instead of the opaque source_id. Best-effort: atoms whose content lives
    // only in an R2 blob (no content_inline) simply carry no preview. Truncated
    // server-side to keep the list payload small.
    let previews: std::collections::HashMap<Uuid, String> = store
        .atom_inline_content(&ids)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(id, c, _, _, header_len)| {
            // Strip the contextual chunk-header (cs-phase0-chunk-headers)
            // before truncating — the preview must show original text, not
            // the synthetic "Meeting: ..." embedding aid.
            c.map(|c| {
                (
                    id,
                    crate::store::strip_header_prefix(&c, header_len)
                        .chars()
                        .take(ATOM_PREVIEW_MAX_CHARS)
                        .collect::<String>(),
                )
            })
        })
        .collect();

    let atoms: Vec<Value> = atoms
        .iter()
        .map(|a| {
            let (title, excerpt) = titles_excerpts
                .get(&a.atom_id)
                .cloned()
                .unwrap_or((None, None));
            let stripped_body = previews.get(&a.atom_id);
            let display_title = crate::services::atom_title::display_title(
                crate::services::atom_title::DisplayTitleInput {
                    title: title.as_deref(),
                    excerpt: excerpt.as_deref(),
                    stripped_body: stripped_body.map(String::as_str),
                    source_type: &a.source_type,
                    channel_name: a.channel_name.as_deref(),
                    author_name: a.author_name.as_deref(),
                    timestamp: a.timestamp,
                },
            );
            json!({
                "atom_id": a.atom_id,
                "graph_id": graph_id,
                "source_type": a.source_type,
                "source_id": a.source_id,
                "timestamp": a.timestamp,
                "created_at": a.created_at,
                // `preview` = the inline-content-derived snippet when one
                // exists, else the persisted `excerpt` (populated at ingest
                // regardless of storage backend — see `store::
                // derive_journal_excerpt`) — so an R2-only atom with no
                // `content_inline` still gets a real preview instead of
                // `null`.
                "preview": stripped_body.or(excerpt.as_ref()),
                "title": display_title,
                "excerpt": excerpt,
                // Structured provenance (migration 0034) so the corals list can
                // render a human title ("{author} in #{channel}") without a raw id.
                "channel_name": a.channel_name,
                "author_name": a.author_name,
            })
        })
        .collect();

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-has-more",
        HeaderValue::from_static(if has_more { "true" } else { "false" }),
    );

    (StatusCode::OK, headers, Json(json!(atoms)))
}

// ── GET /v1/graphs/:graph_id/stats — true reef/coral/source totals ───────────
//
// Powers the Reefs page tab-bar counts ("All reefs / Entities / Decisions /
// Threads / References / Corals / Sources"). Those used to be computed
// client-side from whatever was already loaded into state — accurate only
// while `/v1/nodes` (reefs) and the atoms/sources lists were fetched in full.
// Paginating `/v1/nodes` (CS-reef-pagination) broke that for the reef-type
// tabs: the client only ever sees the page(s) fetched so far. This route
// returns the ocean's totals independently of pagination. Postgres computes
// only these counts; Ask and MCP keep their full OceanStats projection.

#[utoipa_path(
    get,
    path = "/v1/graphs/{graph_id}/stats",
    tag = "graphs",
    params(("graph_id" = Uuid, Path, description = "Graph (ocean) identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "True reef/coral/source totals for the ocean", body = Value),
        (status = 403, description = "Ocean not reachable by caller"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_ocean_stats(
    state: axum::extract::State<std::sync::Arc<crate::AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    path: Path<std::collections::BTreeMap<String, String>>,
    query: axum::extract::Query<std::collections::BTreeMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    crate::routes::page_read::serve::<crate::read_models::oceans::WorkspaceCounts>(
        state, tenant, user, original, path, query,
    )
    .await
    .into_response()
}

// ── GET /v1/graphs/:graph_id/sources ──────────────────────────────────────────
//
// Group this ocean's corals by the source instance that spawned them (the base
// `source_id`, i.e. `{base}:chunk:{n}` collapsed to `{base}`) so the client can
// show "what we ingested and which corals came from it" and promote a whole
// source at once. Assembled from existing Store seams: `list_all_atoms_for_sources`
// (below — the full corpus, not just the newest 100) → `atom_inline_content`
// (previews + channel header) → `expand` (which reefs each coral belongs to).
// The web folds these source instances by `channel_key` for the channel/
// meeting grouping.

/// Parse the channel from a coral's "{sender} in #{channel} · {when}" header
/// line, if present. Returns the `#channel` token (incl. the leading `#`).
fn channel_label_from_preview(preview: &str) -> Option<String> {
    let first = preview.lines().next()?;
    let idx = first.find(" in #")?;
    let chan = first[idx + 4..].split(" · ").next()?.trim();
    (chan.starts_with('#') && chan.len() > 1).then(|| chan.to_string())
}

/// Every atom in a graph (CS-reefs-raw-material-all-time), paging through
/// `Store::list_atoms_page` until a page comes back short. `list_sources`
/// (unlike `list_atoms`, which now has its own client-driven `page`/
/// `page_size` params) keeps its response shape as one bare array — the
/// grouping-by-channel it does only makes sense over the whole corpus, so
/// this does the pagination server-side instead of pushing it onto callers.
/// `SOURCES_ATOM_CAP` is a sanity backstop, not an expected ceiling — the
/// design handoff's own sizing note is "local oceans are a few thousand
/// atoms".
const SOURCES_ATOM_PAGE: i64 = 500;
const SOURCES_ATOM_CAP: i64 = 20_000;

async fn list_all_atoms_for_sources(
    store: &dyn crate::store::Store,
    mut scoped_tx: Option<&mut sqlx::Transaction<'static, sqlx::Postgres>>,
    graph_id: Uuid,
) -> Result<Vec<crate::store::Atom>, crate::store::StoreError> {
    let mut all = Vec::new();
    let mut offset = 0i64;
    loop {
        // RLS scoping (org-tenancy B.1): when the caller opened a
        // `begin_account_scoped` transaction, every page read runs on it.
        let page = match scoped_tx.as_deref_mut() {
            Some(tx) => {
                store
                    .list_atoms_page_scoped(tx, graph_id, SOURCES_ATOM_PAGE, offset)
                    .await?
            }
            None => {
                store
                    .list_atoms_page(graph_id, SOURCES_ATOM_PAGE, offset)
                    .await?
            }
        };
        let got = page.len() as i64;
        all.extend(page);
        if got < SOURCES_ATOM_PAGE || all.len() as i64 >= SOURCES_ATOM_CAP {
            break;
        }
        offset += SOURCES_ATOM_PAGE;
    }
    Ok(all)
}

#[utoipa_path(
    get,
    path = "/v1/graphs/{graph_id}/sources",
    tag = "graphs",
    params(("graph_id" = Uuid, Path, description = "Graph identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Ingested sources grouped from this ocean's corals", body = Value),
        (status = 404, description = "Graph not found"),
    )
)]
pub async fn list_sources(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // IDOR guard (Group-B security fix, PR 0 of the member-private-oceans
    // plan §1.4(c)/item 3): `graph_exists_for_account[_scoped]`'s query is
    // literally `WHERE id = $1 AND account_id = $2` — bare account
    // OWNERSHIP, no team gate — so any account member could list the FULL
    // CORPUS (grouped by source) of a `kind='team'` ocean they were never
    // added to. Route through `ReachabilityResolver` instead — the one
    // predicate that gates `kind='team'` on `team_members` membership (same
    // fix as `get_graph`/`list_atoms`, above).
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return (StatusCode::NOT_FOUND, Json(json!({"error": "graph not found"}))),
        Err(e) => {
            tracing::error!(error = %e, "list_sources reachability error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    }

    // RLS scoping (org-tenancy B.1): all `graphs`/`atoms`/`nodes`/`node_atoms`
    // reads below share ONE `begin_account_scoped` transaction on the graph's
    // OWNING account, not necessarily the caller's — reachability above may
    // have admitted a reader from a different account via the D9 union, so
    // `user.account_id` would be the wrong RLS scope (same reasoning as
    // `get_graph`/`list_atoms`/`get_ocean_stats`). A pool-less `Tenant` keeps
    // the plain Store path.
    let mut scoped_tx = match &tenant.pool {
        Some(_) => {
            let owner_account = match store.graph_account_id(graph_id).await {
                Ok(Some(acc)) => acc,
                Ok(None) => {
                    // Reachability passed above, so a vanished graph here is
                    // a race with deletion — same opaque 404 as unreachable.
                    return (StatusCode::NOT_FOUND, Json(json!({"error": "graph not found"})));
                }
                Err(e) => {
                    tracing::error!(error = %e, "list_sources: graph_account_id error");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
                }
            };
            match tenant.begin_member_scoped(owner_account).await {
                Ok(tx) => Some(tx),
                Err(e) => {
                    tracing::error!(error = %e, "list_sources: begin_account_scoped failed");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
                }
            }
        }
        None => None,
    };

    let atoms = match list_all_atoms_for_sources(store.as_ref(), scoped_tx.as_mut(), graph_id).await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "list_sources store error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    };

    let ids: Vec<Uuid> = atoms.iter().map(|a| a.atom_id).collect();

    // Which reefs each coral belongs to (best-effort; counts reefs per source).
    let idset: HashSet<Uuid> = ids.iter().copied().collect();
    // No query embedding here — this walks every atom in the SOURCE (not a
    // retrieval result), so there is no query vector to rank fillers against.
    // Seeds-first ordering still applies; fillers fall back to the
    // deterministic secondary order (see `Store::expand`'s doc comment).
    let nodes = match scoped_tx.as_mut() {
        Some(tx) => store
            .expand_scoped(tx, graph_id, &ids, None)
            .await
            .unwrap_or_default(),
        None => store.expand(graph_id, &ids, None).await.unwrap_or_default(),
    };

    // Titles-always-present — fetched through the STILL-OPEN scoped
    // transaction, same fix as `list_atoms` above (CodeRabbit round 1 on
    // #837): `atom_titles_and_excerpts_scoped` didn't exist when this route
    // was written, so this used to run ambient AFTER the commit below.
    let titles_excerpts_res = match scoped_tx.as_mut() {
        Some(tx) => store.atom_titles_and_excerpts_scoped(tx, &ids).await,
        None => store.atom_titles_and_excerpts(&ids).await,
    };
    let titles_excerpts: HashMap<Uuid, (Option<String>, Option<String>)> = titles_excerpts_res
        .unwrap_or_default()
        .into_iter()
        .map(|(id, title, excerpt)| (id, (title, excerpt)))
        .collect();

    // Scoped reads done — commit before the preview fetch below.
    // NOTE (org-tenancy B.1 known gap): `atom_inline_content` has no `_scoped`
    // twin yet (not covered by the B.0 chunks #285/#289/#290 — needs a
    // follow-up Store chunk), so the preview enrichment still runs unscoped.
    // Best-effort by construction (`unwrap_or_default()`): post-OPS-4 it
    // degrades to empty previews rather than leaking or failing the route.
    if let Some(tx) = scoped_tx.take() {
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "list_sources: scoped commit failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    }

    let previews: HashMap<Uuid, String> = store
        .atom_inline_content(&ids)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(id, c, _, _, header_len)| {
            c.map(|c| {
                (
                    id,
                    crate::store::strip_header_prefix(&c, header_len)
                        .chars()
                        .take(ATOM_PREVIEW_MAX_CHARS)
                        .collect::<String>(),
                )
            })
        })
        .collect();
    let mut atom_reefs: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    for n in &nodes {
        for aid in &n.atom_ids {
            if idset.contains(aid) {
                atom_reefs.entry(*aid).or_default().insert(n.node_id);
            }
        }
    }

    // Group atom indices by base source_id, preserving list order (newest first).
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, a) in atoms.iter().enumerate() {
        let base = a.source_id.split(":chunk:").next().unwrap_or(&a.source_id).to_string();
        if !groups.contains_key(&base) {
            order.push(base.clone());
        }
        groups.entry(base).or_default().push(i);
    }

    let sources: Vec<Value> = order
        .iter()
        .map(|base| {
            let g = &groups[base];
            let a0 = &atoms[g[0]];
            // Channel grouping key. Prefer the structured `channel_id` (migration
            // 0034) — it's stable and identical across every message in a channel.
            // Only fall back to the legacy heuristic of splitting the source_id on
            // `/` (Slack-style `channel/ts`) when no channel_id is present. Teams
            // source_ids are `‹channel_id›:‹ts›` with no `/`, so the old heuristic
            // collapsed to the per-message id and showed each message as its own
            // one-message "channel" (many duplicate rows for one named channel).
            let channel_key = a0
                .channel_id
                .as_deref()
                .filter(|c| !c.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| base.split('/').next().unwrap_or(base).to_string());
            // Prefer the structured `channel_name` (migration 0034); fall back to
            // parsing the body header for atoms not yet backfilled.
            let channel_label = g
                .iter()
                .find_map(|&i| {
                    atoms[i]
                        .channel_name
                        .as_deref()
                        .filter(|c| !c.is_empty())
                        .map(|c| format!("#{c}"))
                })
                .or_else(|| {
                    g.iter().find_map(|&i| {
                        previews.get(&atoms[i].atom_id).and_then(|p| channel_label_from_preview(p))
                    })
                });
            let reef_ids: HashSet<Uuid> = g
                .iter()
                .flat_map(|&i| atom_reefs.get(&atoms[i].atom_id).into_iter().flatten().copied())
                .collect();
            let first_ts = g.iter().map(|&i| atoms[i].timestamp).min();
            let last_ts = g.iter().map(|&i| atoms[i].timestamp).max();
            let corals: Vec<Value> = g
                .iter()
                .map(|&i| {
                    let atom = &atoms[i];
                    let (title, excerpt) = titles_excerpts
                        .get(&atom.atom_id)
                        .cloned()
                        .unwrap_or((None, None));
                    let stripped_body = previews.get(&atom.atom_id);
                    let display_title = crate::services::atom_title::display_title(
                        crate::services::atom_title::DisplayTitleInput {
                            title: title.as_deref(),
                            excerpt: excerpt.as_deref(),
                            stripped_body: stripped_body.map(String::as_str),
                            source_type: &atom.source_type,
                            channel_name: atom.channel_name.as_deref(),
                            author_name: atom.author_name.as_deref(),
                            timestamp: atom.timestamp,
                        },
                    );
                    json!({
                        "atom_id": atom.atom_id,
                        // Full stored source_id (incl. :chunk:N) — the unit
                        // promote-source matches on. Distinct from the grouped
                        // base `source_id` above.
                        "source_id": atom.source_id,
                        // See `list_atoms`'s identical fallback: inline preview
                        // when present, else the persisted excerpt.
                        "preview": stripped_body.or(excerpt.as_ref()),
                        "title": display_title,
                        "excerpt": excerpt,
                        "timestamp": atom.timestamp,
                    })
                })
                .collect();
            json!({
                "source_id": base,
                "source_type": a0.source_type,
                "channel_key": channel_key,
                "channel_label": channel_label,
                "coral_count": g.len(),
                "reef_count": reef_ids.len(),
                "first_ts": first_ts,
                "last_ts": last_ts,
                "corals": corals,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(sources)))
}

// ── GET /v1/graphs/:graph_id/source-groups — channel-level aggregate ─────────
// GET /v1/graphs/:graph_id/source-groups/:channel_key — one group, paginated ──
//
// Perf fix for `list_sources` above (15.2s in prod on a ~20k-atom ocean: an
// O(atoms) Rust walk that fetches every atom, `expand`s it, and pulls its
// inline body just to cut a preview). These two routes are additive surface
// the web page is written against instead:
//
//   * `source-groups` mirrors `list_sources`'s per-source-instance grouping
//     collapsed one level further, to `channel_key` (the same key
//     `web/components/library/sourceGroups.ts` folds `list_sources`'s array
//     into client-side today) — computed by [`Store::source_groups`], one or
//     two grouped SQL statements, O(atoms) inside the database but O(groups)
//     in Rust here. No corals, no previews, no bodies.
//   * `source-groups/{channel_key}` returns one group's full metadata (ALL
//     its `source_ids`/`reefs`, not paginated) plus ONE PAGE of its corals,
//     with the exact same preview/title/excerpt enrichment `list_sources`
//     already does per atom — reused as-is, just scoped to ≤200 atoms
//     instead of the whole corpus.
//
// Grouping/RLS-scoping discipline matches `list_sources` exactly: IDOR gate
// via `ReachabilityResolver`, then `graph_account_id` → `begin_member_scoped`,
// every read on that scoped transaction via `_scoped` Store methods, and the
// same `atom_inline_content`-has-no-`_scoped`-twin best-effort gap (commit the
// scoped tx before that one call).

#[derive(Debug, Deserialize)]
pub struct SourceGroupDetailParams {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

#[utoipa_path(
    get,
    path = "/v1/graphs/{graph_id}/source-groups",
    tag = "graphs",
    params(("graph_id" = Uuid, Path, description = "Graph identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Channel-level source-group rollups for this ocean", body = Value),
        (status = 404, description = "Graph not found"),
    )
)]
pub async fn list_source_groups(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // IDOR guard — identical to `list_sources` (see its own comment on why
    // `ReachabilityResolver`, not the older account-ownership check).
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph_id,
        user.user_id,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return (StatusCode::NOT_FOUND, Json(json!({"error": "graph not found"}))),
        Err(e) => {
            tracing::error!(error = %e, "list_source_groups reachability error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    }

    // RLS scoping — identical pattern to `list_sources` (see its comment).
    let mut scoped_tx = match &tenant.pool {
        Some(_) => {
            let owner_account = match store.graph_account_id(graph_id).await {
                Ok(Some(acc)) => acc,
                Ok(None) => {
                    return (StatusCode::NOT_FOUND, Json(json!({"error": "graph not found"})));
                }
                Err(e) => {
                    tracing::error!(error = %e, "list_source_groups: graph_account_id error");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
                }
            };
            match tenant.begin_member_scoped(owner_account).await {
                Ok(tx) => Some(tx),
                Err(e) => {
                    tracing::error!(error = %e, "list_source_groups: begin_member_scoped failed");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
                }
            }
        }
        None => None,
    };

    let groups = match scoped_tx.as_mut() {
        Some(tx) => store.source_groups_scoped(tx, graph_id).await,
        None => store.source_groups(graph_id).await,
    };
    let groups = match groups {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, "list_source_groups store error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    };

    if let Some(tx) = scoped_tx.take() {
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "list_source_groups: scoped commit failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    }

    let groups_json: Vec<Value> = groups
        .iter()
        .map(|g| {
            json!({
                "channel_key": g.channel_key,
                "channel_label": g.channel_label,
                "source_type": g.source_type,
                "coral_count": g.coral_count,
                "instance_count": g.instance_count,
                "reef_count": g.reef_count,
                "first_ts": g.first_ts,
                "last_ts": g.last_ts,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({"groups": groups_json})))
}

#[utoipa_path(
    get,
    path = "/v1/graphs/{graph_id}/source-groups/{channel_key}",
    tag = "graphs",
    params(
        ("graph_id" = Uuid, Path, description = "Graph identifier"),
        ("channel_key" = String, Path, description = "A `channel_key` returned by GET /v1/graphs/{graph_id}/source-groups"),
        ("page" = Option<i64>, Query, description = "1-based page number of corals (default 1)"),
        ("page_size" = Option<i64>, Query, description = "Corals per page, clamped 1..=200 (default 50)"),
    ),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "One channel group's full metadata plus one page of its corals", body = Value),
        (status = 404, description = "Graph or source group not found"),
    )
)]
pub async fn get_source_group(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((graph_id, channel_key)): Path<(Uuid, String)>,
    Query(params): Query<SourceGroupDetailParams>,
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
        Ok(false) => return (StatusCode::NOT_FOUND, Json(json!({"error": "graph not found"}))),
        Err(e) => {
            tracing::error!(error = %e, "get_source_group reachability error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    }

    // Pagination: clamp, never 400 (contract for this route).
    let page = params.page.unwrap_or(1).max(1);
    let page_size = params.page_size.unwrap_or(50).clamp(1, 200);
    let offset = (page - 1) * page_size;

    let mut scoped_tx = match &tenant.pool {
        Some(_) => {
            let owner_account = match store.graph_account_id(graph_id).await {
                Ok(Some(acc)) => acc,
                Ok(None) => {
                    return (StatusCode::NOT_FOUND, Json(json!({"error": "graph not found"})));
                }
                Err(e) => {
                    tracing::error!(error = %e, "get_source_group: graph_account_id error");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
                }
            };
            match tenant.begin_member_scoped(owner_account).await {
                Ok(tx) => Some(tx),
                Err(e) => {
                    tracing::error!(error = %e, "get_source_group: begin_member_scoped failed");
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
                }
            }
        }
        None => None,
    };

    let meta = match scoped_tx.as_mut() {
        Some(tx) => store.source_group_detail_meta_scoped(tx, graph_id, &channel_key).await,
        None => store.source_group_detail_meta(graph_id, &channel_key).await,
    };
    let meta = match meta {
        Ok(Some(m)) => m,
        Ok(None) => {
            // No atoms under this key in this graph — not an error, just a
            // miss. The still-open scoped tx (a pure read) drops/rolls back
            // harmlessly, same as every other early-return in this file.
            return (StatusCode::NOT_FOUND, Json(json!({"error": "source group not found"})));
        }
        Err(e) => {
            tracing::error!(error = %e, "get_source_group: source_group_detail_meta error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    };

    // Fetch one extra row to compute `has_more` without a second COUNT query
    // (same pattern as `list_atoms`/`nodes::list_nodes`).
    let atoms_res = match scoped_tx.as_mut() {
        Some(tx) => {
            store
                .list_atoms_page_for_channel_scoped(tx, graph_id, &channel_key, page_size + 1, offset)
                .await
        }
        None => {
            store
                .list_atoms_page_for_channel(graph_id, &channel_key, page_size + 1, offset)
                .await
        }
    };
    let atoms = match atoms_res {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "get_source_group: list_atoms_page_for_channel error");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    };
    let has_more = atoms.len() as i64 > page_size;
    let atoms: Vec<_> = atoms.into_iter().take(page_size as usize).collect();
    let ids: Vec<Uuid> = atoms.iter().map(|a| a.atom_id).collect();

    // Titles-always-present — same fix/reasoning as `list_sources` above.
    let titles_excerpts_res = match scoped_tx.as_mut() {
        Some(tx) => store.atom_titles_and_excerpts_scoped(tx, &ids).await,
        None => store.atom_titles_and_excerpts(&ids).await,
    };
    let titles_excerpts: HashMap<Uuid, (Option<String>, Option<String>)> = titles_excerpts_res
        .unwrap_or_default()
        .into_iter()
        .map(|(id, title, excerpt)| (id, (title, excerpt)))
        .collect();

    // Scoped reads done — commit before the preview fetch below (same
    // known-gap note as `list_sources`: `atom_inline_content` has no
    // `_scoped` twin yet).
    if let Some(tx) = scoped_tx.take() {
        if let Err(e) = tx.commit().await {
            tracing::error!(error = %e, "get_source_group: scoped commit failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})));
        }
    }

    let previews: HashMap<Uuid, String> = store
        .atom_inline_content(&ids)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(id, c, _, _, header_len)| {
            c.map(|c| {
                (
                    id,
                    crate::store::strip_header_prefix(&c, header_len)
                        .chars()
                        .take(ATOM_PREVIEW_MAX_CHARS)
                        .collect::<String>(),
                )
            })
        })
        .collect();

    // `channel_label`: the structured `channel_name` rung (whole-group,
    // computed in `meta`) wins when present; otherwise fall back to the
    // body-header parser over THIS PAGE's previews only (contract note: the
    // aggregate list route doesn't need this fallback, this one does).
    let channel_label = meta.summary.channel_label.clone().or_else(|| {
        atoms
            .iter()
            .find_map(|a| previews.get(&a.atom_id).and_then(|p| channel_label_from_preview(p)))
    });

    let corals: Vec<Value> = atoms
        .iter()
        .map(|atom| {
            let (title, excerpt) = titles_excerpts.get(&atom.atom_id).cloned().unwrap_or((None, None));
            let stripped_body = previews.get(&atom.atom_id);
            let display_title = crate::services::atom_title::display_title(
                crate::services::atom_title::DisplayTitleInput {
                    title: title.as_deref(),
                    excerpt: excerpt.as_deref(),
                    stripped_body: stripped_body.map(String::as_str),
                    source_type: &atom.source_type,
                    channel_name: atom.channel_name.as_deref(),
                    author_name: atom.author_name.as_deref(),
                    timestamp: atom.timestamp,
                },
            );
            json!({
                "atom_id": atom.atom_id,
                "source_id": atom.source_id,
                "preview": stripped_body.or(excerpt.as_ref()),
                "title": display_title,
                "excerpt": excerpt,
                "timestamp": atom.timestamp,
            })
        })
        .collect();

    let reefs_json: Vec<Value> = meta
        .reefs
        .iter()
        .map(|r| {
            json!({
                "node_id": r.node_id,
                "type": r.node_type,
                "title": r.title,
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "channel_key": channel_key,
            "channel_label": channel_label,
            "source_type": meta.summary.source_type,
            "coral_count": meta.summary.coral_count,
            "instance_count": meta.summary.instance_count,
            "reef_count": meta.summary.reef_count,
            "first_ts": meta.summary.first_ts,
            "last_ts": meta.summary.last_ts,
            "source_ids": meta.source_ids,
            "reefs": reefs_json,
            "corals": corals,
            "page": page,
            "page_size": page_size,
            "has_more": has_more,
        })),
    )
}

#[cfg(test)]
mod stats_tests {
    use super::*;
    use axum::{body::Body, http::Request as HttpRequest, routing::get, Router};
    use tower::ServiceExt;

    // ── GET /v1/graphs/{graph_id}/stats ──────────────────────────────────────

    #[tokio::test]
    async fn get_ocean_stats_requires_auth() {
        let router = Router::new()
            .route("/v1/graphs/{graph_id}/stats", get(get_ocean_stats))
            .layer(axum::middleware::from_fn_with_state(
                crate::test_app_state(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(crate::test_app_state());
        let req = HttpRequest::builder()
            .uri("/v1/graphs/00000000-0000-0000-0000-000000000000/stats")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}

// ── Live-Postgres tests: `create_graph`'s personal-ocean-in-org guard ───────
//
// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as every other
// `pg_tests` module in this crate (`routes::checkout::pg_tests`,
// `routes::admin::pg_tests`, `routes::usage::pg_tests`). `cargo test --lib`
// with no env var set SKIPS these with an `eprintln!` marker rather than
// failing. Every test mints FRESH `Uuid::new_v4()` accounts/users, so tests
// never collide with whatever else is in the shared test database.
//
// Calls `create_graph` DIRECTLY with a hand-built `Tenant`/`UserContext`
// (both plain structs with public fields) rather than through a real HTTP
// router + minted Clerk JWT — same discipline `routes::usage::pg_tests`'s own
// module doc explains: this handler's interesting behavior is what it does
// with the control-plane read, not auth/routing.
#[cfg(test)]
mod personal_ocean_in_org_pg_tests {
    use super::*;
    use crate::store::PostgresStore;

    async fn pg_test_pool(test_name: &str) -> Option<sqlx::PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    async fn seed_personal_account(pool: &sqlx::PgPool) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@create-graph-prd.test", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
        (account_id, user_id)
    }

    /// A personal org's own account (control migration 0208 `personal_orgs`)
    /// — the end state `bin/adopt_personal_org_account.rs` leaves a user in.
    /// No `account_members` row is needed here: `create_graph`'s guard reads
    /// only `accounts`/`personal_orgs`/`users`, never membership.
    async fn seed_personal_org_account(pool: &sqlx::PgPool) -> (Uuid, Uuid, String) {
        let clerk_user_id = format!("user_{}", Uuid::new_v4().simple());
        let clerk_org_id = format!("org_{}", Uuid::new_v4().simple());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
                .bind(format!("{}@create-graph-prd.test", Uuid::new_v4()))
                .bind(&clerk_user_id)
                .fetch_one(pool)
                .await
                .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(&clerk_org_id)
        .fetch_one(pool)
        .await
        .expect("insert personal-org account");
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&clerk_org_id)
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("insert personal_orgs row");
        (account_id, user_id, clerk_org_id)
    }

    /// **The account-relative D2 case (PR 829 review, finding 1).** A
    /// personal org's own account, but with a SECOND `account_members` row —
    /// against `personal_orgs.clerk_user_id UNIQUE`'s one-member design, but
    /// nothing in this codebase controls a personal org's Clerk-side
    /// membership. Returns `(account_id, owner_user_id, clerk_org_id)`.
    async fn seed_personal_org_account_with_two_members(pool: &sqlx::PgPool) -> (Uuid, Uuid, Uuid, String) {
        let (account_id, owner, clerk_org_id) = seed_personal_org_account(pool).await;
        let second_member: Uuid =
            sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
                .bind(format!("{}@create-graph-prd.test", Uuid::new_v4()))
                .fetch_one(pool)
                .await
                .expect("insert second user");
        // `0176_seat_lifecycle.sql`'s `account_members_default_seat_tier`
        // trigger hard-refuses (RAISE EXCEPTION) an org-account
        // `account_members` INSERT with no `seat_tier_id` — must be supplied
        // explicitly here.
        let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .fetch_one(pool)
            .await
            .expect("standard seat tier must be seeded (0165)");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(tier_id)
        .execute(pool)
        .await
        .expect("insert the owner's own account_members row");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)",
        )
        .bind(account_id)
        .bind(second_member)
        .bind(tier_id)
        .execute(pool)
        .await
        .expect("insert the second member's account_members row");
        (account_id, owner, second_member, clerk_org_id)
    }

    /// A REAL org account — `clerk_org_id` set, no `personal_orgs` row.
    async fn seed_real_org_account(pool: &sqlx::PgPool) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@create-graph-prd.test", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(format!("org_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert real org account");
        (account_id, user_id)
    }

    fn user_context(account_id: Uuid, user_id: Uuid) -> UserContext {
        UserContext { user_id, account_id, role: "owner".to_string(), device_id: "create-graph-prd-test".to_string() }
    }

    fn tenant(pool: sqlx::PgPool) -> Tenant {
        Tenant {
            store: Some(Arc::new(PostgresStore::new(pool.clone()))),
            pool: Some(pool),
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid, user_id: Uuid, personal_org_id: Option<&str>) {
        sqlx::query("DELETE FROM graphs WHERE account_id = $1").bind(account_id).execute(pool).await.ok();
        sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(pool).await.ok();
        sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(pool).await.ok();
        if let Some(org_id) = personal_org_id {
            sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id = $1")
                .bind(org_id)
                .execute(pool)
                .await
                .ok();
        }
    }

    /// **Shape 1 (unchanged control) — a plain personal account may create a
    /// personal ocean.**
    #[tokio::test]
    async fn plain_personal_account_may_create_a_personal_ocean() {
        let Some(pool) = pg_test_pool("plain_personal_account_may_create_a_personal_ocean").await else {
            return;
        };
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let (status, _) = create_graph(
            State(state),
            tenant(pool.clone()),
            Extension(user_context(account_id, user_id)),
            Json(CreateGraphRequest { name: "Mine".to_string(), kind: Some("personal".to_string()) }),
        )
        .await;

        cleanup(&pool, account_id, user_id, None).await;
        assert_eq!(status, StatusCode::CREATED, "a plain personal account must be able to create a personal ocean");
    }

    /// **Shape 2 (the fix) — a personal org's own account may ALSO create a
    /// personal ocean.** Before this PR, `account_is_org` read `true` for
    /// this account (the seat-tier trigger derives `is_org_account` from
    /// `clerk_org_id IS NOT NULL` alone, with no personal-org exception), so
    /// this request 400'd with "switch to your personal account" — advice
    /// that is unreachable for a personal-org owner, since adoption
    /// (`bin/adopt_personal_org_account.rs`) relabels their ONLY personal
    /// account and leaves no other one to switch to.
    #[tokio::test]
    async fn personal_org_account_may_create_a_personal_ocean() {
        let Some(pool) = pg_test_pool("personal_org_account_may_create_a_personal_ocean").await else {
            return;
        };
        let (account_id, user_id, org_id) = seed_personal_org_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let (status, body) = create_graph(
            State(state),
            tenant(pool.clone()),
            Extension(user_context(account_id, user_id)),
            Json(CreateGraphRequest { name: "Mine".to_string(), kind: Some("personal".to_string()) }),
        )
        .await;

        cleanup(&pool, account_id, user_id, Some(&org_id)).await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "a personal org's own account must be able to create a personal ocean, same as a plain \
             personal account: {body:?}"
        );
    }

    /// **Shape 3 (unchanged control) — a REAL org account is still refused.**
    /// The risk in this PR is loosening a check that should stay tight for
    /// real organizations — this proves it did not.
    #[tokio::test]
    async fn real_org_account_is_still_refused_a_personal_ocean() {
        let Some(pool) = pg_test_pool("real_org_account_is_still_refused_a_personal_ocean").await else {
            return;
        };
        let (account_id, user_id) = seed_real_org_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let (status, _) = create_graph(
            State(state),
            tenant(pool.clone()),
            Extension(user_context(account_id, user_id)),
            Json(CreateGraphRequest { name: "Mine".to_string(), kind: Some("personal".to_string()) }),
        )
        .await;

        cleanup(&pool, account_id, user_id, None).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a real org account must still be refused a personal ocean"
        );
    }

    /// **Shape 5 (the account-relative D2 gap, PR 829 review finding 1) — a
    /// personal org's account with a SECOND `account_members` row must be
    /// REFUSED a personal ocean, even though `account_is_real_org` alone
    /// reads `false` for this caller.** `resolve_reachable_graphs` would
    /// return a `kind='personal'` ocean created here to BOTH members — the
    /// exact leak `create_graph`'s org-account refusal exists to prevent.
    #[tokio::test]
    async fn personal_org_account_with_a_second_member_is_refused_a_personal_ocean() {
        let Some(pool) =
            pg_test_pool("personal_org_account_with_a_second_member_is_refused_a_personal_ocean").await
        else {
            return;
        };
        let (account_id, owner, second_member, org_id) =
            seed_personal_org_account_with_two_members(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let (status, body) = create_graph(
            State(state),
            tenant(pool.clone()),
            Extension(user_context(account_id, owner)),
            Json(CreateGraphRequest { name: "Mine".to_string(), kind: Some("personal".to_string()) }),
        )
        .await;

        cleanup(&pool, account_id, owner, Some(&org_id)).await;
        sqlx::query("DELETE FROM users WHERE id = $1").bind(second_member).execute(&pool).await.ok();

        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a personal org account with a second member must be refused a personal ocean: {body:?}"
        );
    }
}
