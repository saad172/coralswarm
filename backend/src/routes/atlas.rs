//! Atlas route — the per-ocean knowledge graph as a single node+link payload.
//!
//! `GET /v1/graphs/{graph_id}/atlas` assembles, for one ocean, the heterogeneous
//! graph the Atlas canvas renders: a synthetic ocean hub, its reefs
//! (entity/thread/reference) and decisions (node type `decision`), the corals
//! (atoms) that hang off them, the source platforms those corals were ingested
//! from, and the people who authored them — plus every edge between them.
//!
//! It is composed entirely from existing Store seams (no new tables): reefs from
//! `list_nodes`, corals from `list_atoms`, reef↔coral membership from `expand`,
//! authors from `atom_authors`, and reef↔reef typed edges from
//! `list_relationship_edges_by_status`, and cross-ocean promotion provenance
//! from `node_promotion_origins`. The backend returns *semantics* (node type,
//! reef kind, source type, edge status/origin/confidence, promoted-from
//! ocean); the client maps those to hues / glyphs / line styles.
//!
//! Scale note (honest, not hidden): `list_atoms` returns the newest 100 corals
//! and `expand` caps sibling atoms per reef, so a busy ocean's atlas is a recency
//! sample, not the full census. The response carries `truncated.atoms` so the UI
//! can say so. Reef and decision counts are exact.
//!
//! Authorization mirrors the rest of the API (ADR-0007 D9): the ocean must be
//! reachable by the caller — checked here by locating it in
//! `list_reachable_graphs`, which also yields the hub's name/kind.
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.2)
//! Every query here that touches an RLS-covered consumer table
//! (`nodes`/`atoms`/`node_atoms`/`relationship_edge`, migration
//! `0083_consumer_rls`) runs inside ONE
//! [`crate::tenancy::scope::begin_account_scoped`] transaction per request —
//! scoped to the OCEAN'S owning account (resolved via the
//! `Store::graph_account_id` SECURITY DEFINER bootstrap resolver, migration
//! 0084) — so the route keeps working once OPS-4 switches the app's
//! connection role to the non-`BYPASSRLS` `coralswarm_app`. Exceptions:
//!   * `list_reachable_graphs` — the permanent multi-account SECURITY
//!     DEFINER carve-out; runs BEFORE scoping, never inside.
//!   * ~~`atom_authors` / `atom_inline_content`~~ — RESOLVED. These two were
//!     carved out at B.2 because no `_scoped` twins existed yet; the twins
//!     have since landed (`Store::atom_authors_scoped`,
//!     `Store::atom_inline_content_scoped`) and both call sites now route
//!     through `scoped_store!` like every other read here. The carve-out was
//!     written as "post-OPS-4 they degrade" — but OPS-4 has happened (#554),
//!     so it described a live degradation in the future tense for as long as
//!     it stood. See #561.
//!   * no pool (`tenant.pool == None`): opening the scoped transaction fails
//!     and the handler returns a 500; there is no unscoped fallback.

use std::collections::{HashMap, HashSet};

use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::relationships::{predicates, EdgeStatus, RelationshipEdge};
use crate::{
    middleware::auth::UserContext, tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver,
};

// Reads through the `Tenant` extractor's resolved store (org-tenancy PR 1.5)
// rather than ambient `state.store` — everything this route touches (`graphs`,
// `nodes`, `atoms`, `node_atoms`, `relationship_edge`) is tenant-tree.
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

/// Open the request's account-scoped transaction (org-tenancy OPS-4 phase
/// B.2), or early-return a 500. `None` (not an error) when no Postgres pool
/// (and no RLS) exists. On the org plane this opens on
/// the TENANT pool via `Tenant::begin_member_scoped`, which also sets
/// `app.user_id`.
///
/// That GUC has real policies to satisfy there. Whole-database isolation is
/// real, but it covers the CROSS-ORG boundary only, and it is NOT a reason to
/// skip the scope: `migrations_tenant/0009_tenant_rls` puts member-ocean
/// policies on `nodes`/`atoms`/`node_atoms`/`relationship_edge`, every table
/// this file reads. Those policies resolve through
/// `tenant_rls_visible_graph_ids()`, and with neither `app.user_id` nor
/// `app.tenant_actor` set `tenant_rls_actor_user_id()` is NULL — so an atlas
/// read keeps the org and personal oceans and loses every member and team
/// ocean. The GUC is unevaluated today for a ROLE reason, not a policy one:
/// tenant pools connect as the table owner. Phase 3 (`OPS-4-T`) repoints each
/// tenant DSN at the non-owner `coralswarm_tenant_app` role, which is what
/// makes it bite. See `tenancy::scope`'s module doc.
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

/// Upper bound on reefs pulled for one ocean's atlas. Primary sits around
/// 16k reefs+decisions; the old 5k cap would silently drop the derived
/// layers' endpoints.
const MAX_ATLAS_NODES: i64 = 25_000;

/// Default co-occurrence threshold. Bumped to 3 when an implicit (no
/// `?cooccur=`) request would emit more than `COOCCUR_EDGE_BUDGET` pairs.
const DEFAULT_COOCCUR: i64 = 2;
const COOCCUR_EDGE_BUDGET: usize = 5_000;
const ANCHOR_TOP_K: usize = 2;

#[derive(Debug, Default, Deserialize)]
pub struct AtlasQuery {
    /// Shared-coral threshold 1–5. Omit to use the edge-budget default.
    pub cooccur: Option<i64>,
    /// `all` draws every decision–reef anchor; anything else is top-2.
    pub anchors: Option<String>,
}

/// Topical predicates that feed expertise inference + synthesis but are
/// intentionally NOT drawn on the Atlas canvas (hairball control): a generic
/// `related_to` or a passing `mentions` would connect almost everything to
/// everything and drown out the strong involvement/expertise edges. The edges
/// still live in the DB and inform inference — they're just not rendered.
fn is_hidden_atlas_predicate(predicate: &str) -> bool {
    matches!(
        predicate,
        predicates::MENTIONS | predicates::MENTIONED_BY | predicates::RELATED_TO
    )
}

/// Canonical `(lo, hi)` key for an undirected reef pair, so `a→b` and `b→a`
/// (any predicate) collapse to one rendered edge.
fn undirected_pair(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// The wire shape of one rendered reef↔reef typed edge. Beyond the endpoints
/// and `predicate`, it carries what the Graph explorer needs to show HOW two
/// reefs intersect and to act on it: `edge_id` (so a suggested edge can be
/// confirmed/rejected from the canvas via `/v1/relationships/{id}/…`),
/// `status` (`confirmed` draws as a fact, `suggested` as a tentative guess),
/// `origin` (`stated` in the corals vs `inferred` by the relationship engine)
/// and `confidence`. A decision endpoint reclassifies the kind to
/// `decision-reef`; the client hues those by the decision, not the reef.
fn reef_edge_link(
    e: &RelationshipEdge,
    reef_is_decision: &HashMap<Uuid, bool>,
    owner: bool,
) -> Value {
    let touches_decision = *reef_is_decision.get(&e.src_node_id).unwrap_or(&false)
        || *reef_is_decision.get(&e.dst_node_id).unwrap_or(&false);
    let kind = if touches_decision {
        "decision-reef"
    } else {
        "reef-reef"
    };
    json!({
        "a": e.src_node_id,
        "b": e.dst_node_id,
        "kind": kind,
        "predicate": e.predicate,
        "edge_id": e.edge_id,
        "status": e.status.as_str(),
        "origin": e.origin.as_str(),
        "confidence": e.confidence,
        "owner": owner,
    })
}

/// Pick the co-occurrence threshold the atlas will actually emit.
/// An explicit `?cooccur=` wins (clamped 1–5). Otherwise start at 2 and
/// bump to 3 when the ≥2 pair count would blow the edge budget.
fn effective_cooccur_threshold(requested: Option<i64>, pairs_at_two: usize) -> i64 {
    match requested {
        Some(n) => n.clamp(1, 5),
        None if pairs_at_two > COOCCUR_EDGE_BUDGET => 3,
        None => DEFAULT_COOCCUR,
    }
}

/// Rank one decision's reef candidates: title containment first, then
/// most-specific (fewest corals), then longer title. Returns `(reef_id,
/// shared, 1-based rank)`.
fn rank_decision_anchors(
    decision_title: &str,
    mut candidates: Vec<(Uuid, i32, i64, String)>,
) -> Vec<(Uuid, i32, usize)> {
    let hay = decision_title.to_lowercase();
    let contained = |title: &str| {
        let t = title.trim().to_lowercase();
        t.chars().count() >= 3 && hay.contains(&t)
    };
    candidates.sort_by(|a, b| {
        contained(&b.3)
            .cmp(&contained(&a.3))
            .then(a.2.cmp(&b.2))
            .then(b.3.len().cmp(&a.3.len()))
            .then(a.0.cmp(&b.0))
    });
    candidates
        .into_iter()
        .enumerate()
        .map(|(i, (id, shared, _, _))| (id, shared, i + 1))
        .collect()
}

/// First non-empty line of a coral's inline content, trimmed to a node-label
/// length. Atlas labels are glanceable, so we keep them short.
fn coral_label(preview: &str) -> Option<String> {
    let line = preview.lines().map(str::trim).find(|l| !l.is_empty())?;
    let s: String = line.chars().take(60).collect();
    (!s.is_empty()).then(|| {
        if line.chars().count() > 60 {
            format!("{s}…")
        } else {
            s
        }
    })
}

/// Builds the Atlas people layer from raw `(atom_id, author_id, author_name)`
/// author provenance rows: a stable `person:<key>` node id per author (preferring
/// the provider `author_id`, falling back to `author_name` when no id is given),
/// and that key's display label.
///
/// Label contract: the label comes ONLY from `author_name` — raw provider ids
/// (Slack user ids, GUIDs, ...) must never be user-visible, so an author with no
/// name anywhere in their rows gets `None` (the client renders "Unknown author").
///
/// Row-order independence: rows for the same author can arrive in any order and
/// a given author_id's rows don't all have to carry the same author_name (e.g. a
/// stale display name on an older atom). We keep the first non-empty name we see
/// and never overwrite or downgrade it once set — a later unresolved row can't
/// blank out an already-resolved entry, and a later resolved row *does* upgrade
/// an entry that started unresolved.
fn build_people_layer(
    authors: &[(Uuid, Option<String>, Option<String>)],
) -> (HashMap<Uuid, String>, HashMap<String, Option<String>>) {
    let mut coral_person: HashMap<Uuid, String> = HashMap::new();
    let mut people: HashMap<String, Option<String>> = HashMap::new();
    for (atom_id, author_id, author_name) in authors {
        let key = match (author_id, author_name) {
            (Some(id), _) if !id.trim().is_empty() => format!("person:{id}"),
            (_, Some(name)) if !name.trim().is_empty() => format!("person:{name}"),
            _ => continue,
        };
        let label = author_name.clone().filter(|n| !n.trim().is_empty());
        people
            .entry(key.clone())
            .and_modify(|existing| {
                if existing.is_none() {
                    if let Some(l) = &label {
                        *existing = Some(l.clone());
                    }
                }
            })
            .or_insert(label);
        coral_person.insert(*atom_id, key);
    }
    (coral_person, people)
}

#[utoipa::path(
    get,
    path = "/v1/graphs/{graph_id}/atlas",
    tag = "graphs",
    params(("graph_id" = Uuid, Path, description = "Ocean (graph) identifier")),
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "The ocean's knowledge graph (nodes + links)", body = Value),
        (status = 403, description = "Ocean not found or not reachable by the caller"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_atlas(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Query(q): Query<AtlasQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // ── Reachability (ADR-0007 D9) + the ocean hub's identity in one shot —
    // routed through `ReachabilityResolver` with the request's real decision
    // (org-tenancy PR 1.5): Personal → the same consumer-store query as
    // before; Org → the tenant-side D9 union. ─────────────────────────────────
    let ocean = match ReachabilityResolver::list_reachable_graphs(
        tenant.decision.as_ref(),
        store.as_ref(),
        user.user_id,
    )
    .await
    {
        Ok(graphs) => match graphs.into_iter().find(|g| g.id == graph_id) {
            Some(g) => g,
            None => {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({"error": "graph not found or access denied"})),
                )
            }
        },
        Err(e) => {
            tracing::error!(error = %e, "get_atlas reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // ── Scope account: the ocean's owning account, via the SECURITY DEFINER
    // bootstrap resolver (module doc's RLS section). Reachability above
    // already proved the graph exists, so the `None` arm is effectively
    // unreachable and refuses with the same non-leaking 403. ─────────────────
    let account_id = match store.graph_account_id(graph_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "graph not found or access denied"})),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "get_atlas account lookup error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let mut scoped_tx = begin_scoped_tx!(tenant, account_id);

    // ── Reefs + decisions (all node types), each with its atom_count ──────────
    let reefs = match scoped_store!(
        store,
        scoped_tx,
        list_nodes / list_nodes_scoped(graph_id, None, MAX_ATLAS_NODES, 0)
    ) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "get_atlas list_nodes error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let reef_ids: HashSet<Uuid> = reefs.iter().map(|n| n.node_id).collect();
    let reef_is_decision: HashMap<Uuid, bool> = reefs
        .iter()
        .map(|n| (n.node_id, n.node_type == "decision"))
        .collect();
    let reef_by_id: HashMap<Uuid, &crate::store::Node> =
        reefs.iter().map(|n| (n.node_id, n)).collect();

    let self_node_id = match scoped_store!(
        store,
        scoped_tx,
        graph_self_node_id / graph_self_node_id_scoped(graph_id)
    ) {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(error = %e, "get_atlas graph_self_node_id error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // ── Corals (newest 100 — the atlas is a recency sample) ───────────────────
    let atoms = match scoped_store!(store, scoped_tx, list_atoms / list_atoms_scoped(graph_id)) {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "get_atlas list_atoms error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let truncated_atoms = atoms.len() >= 100;
    let atom_ids: Vec<Uuid> = atoms.iter().map(|a| a.atom_id).collect();
    let atom_set: HashSet<Uuid> = atom_ids.iter().copied().collect();

    // ── Reef↔coral membership (best-effort; expand caps siblings per reef) ────
    // No query embedding here — the atlas renders EVERY coral in the ocean,
    // not a retrieval result, so seeds-first ordering still applies but
    // filler ranking falls back to the deterministic secondary order (see
    // `Store::expand`'s doc comment).
    let membership = scoped_store!(
        store,
        scoped_tx,
        expand / expand_scoped(graph_id, &atom_ids, None)
    )
    .unwrap_or_default();
    let mut coral_parents: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for nw in &membership {
        if !reef_ids.contains(&nw.node_id) {
            continue; // only reefs we're rendering; ignore expand's extra frontier
        }
        for aid in &nw.atom_ids {
            if atom_set.contains(aid) {
                coral_parents.entry(*aid).or_default().push(nw.node_id);
            }
        }
    }

    // ── Authors → people layer (only atoms carrying author provenance) ────────
    // Scoped like every other read in this handler. Still best-effort
    // (`unwrap_or_default`) because an empty people layer is a degraded atlas,
    // not a failed one — but on the consumer plane the unscoped call could
    // only ever RAISE (`atoms` is RLS-covered), so the default was the sole
    // outcome there rather than a fallback.
    let authors = scoped_store!(
        store,
        scoped_tx,
        atom_authors / atom_authors_scoped(&atom_ids)
    )
    .unwrap_or_default();
    let (coral_person, people) = build_people_layer(&authors);

    // ── Coral labels (best-effort; readable beats opaque source_id) ───────────
    // Scoped, for the same reason as the people layer above.
    let previews: HashMap<Uuid, String> = scoped_store!(
        store,
        scoped_tx,
        atom_inline_content / atom_inline_content_scoped(&atom_ids)
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|(id, c, _, _, header_len)| {
        c.and_then(|c| coral_label(crate::store::strip_header_prefix(&c, header_len)))
            .map(|l| (id, l))
    })
    .collect();

    // ── Promotion provenance (best-effort): which reefs were copied in from
    // another ocean, and from which. Drives the Graph explorer's cross-ocean
    // "promoted" ring; an empty map is a degraded atlas, not a failed one. ──
    let promoted_from: HashMap<Uuid, Uuid> = scoped_store!(
        store,
        scoped_tx,
        node_promotion_origins / node_promotion_origins_scoped(graph_id)
    )
    .unwrap_or_default()
    .into_iter()
    .collect();

    // ── Build nodes ───────────────────────────────────────────────────────────
    let mut nodes: Vec<Value> = Vec::new();

    // Ocean hub.
    nodes.push(json!({
        "id": "ocean",
        "type": "ocean",
        "label": ocean.name,
        "kind": ocean.kind,
        "is_primary": ocean.is_primary,
    }));

    // Reefs + decisions.
    for n in &reefs {
        // `null` (not omitted) when the reef is native to this ocean, so the
        // wire shape is stable and the client can key off one field.
        let promoted = promoted_from.get(&n.node_id);
        let is_self = self_node_id == Some(n.node_id);
        if n.node_type == "decision" {
            nodes.push(json!({
                "id": n.node_id,
                "type": "decision",
                "label": n.title,
                "count": n.atom_count,
                "date": n.updated_at,
                "promoted_from_graph_id": promoted,
                "is_self": is_self,
            }));
        } else {
            nodes.push(json!({
                "id": n.node_id,
                "type": "reef",
                "reef_kind": n.node_type,
                "label": n.title,
                "count": n.atom_count,
                "promoted_from_graph_id": promoted,
                "is_self": is_self,
            }));
        }
    }

    // Sources (distinct source_type), in first-seen order for stable layout.
    let mut source_order: Vec<String> = Vec::new();
    let mut seen_sources: HashSet<String> = HashSet::new();
    for a in &atoms {
        if seen_sources.insert(a.source_type.clone()) {
            source_order.push(a.source_type.clone());
        }
    }
    for st in &source_order {
        nodes.push(json!({
            "id": format!("src:{st}"),
            "type": "source",
            "source_type": st,
            "label": st,
        }));
    }

    // People. `label` serializes as JSON `null` (not empty-stringed) when
    // unresolved, and `resolved` tells the client whether to render the real
    // name or its own "Unknown author" fallback. The raw author_id may still
    // appear in `id` (a stable "person:<author_id>" node id, needed so the
    // same author's corals link to one node) — the guarantee is narrower than
    // "the id never crosses the wire": the raw id is never used as the
    // display label.
    for (key, label) in &people {
        nodes.push(json!({
            "id": key,
            "type": "person",
            "label": label,
            "resolved": label.is_some(),
        }));
    }

    // Corals.
    for a in &atoms {
        let parents = coral_parents.get(&a.atom_id);
        let label = previews
            .get(&a.atom_id)
            .cloned()
            .unwrap_or_else(|| a.source_type.clone());
        nodes.push(json!({
            "id": a.atom_id,
            "type": "coral",
            "label": label,
            "source_type": a.source_type,
            "src_id": format!("src:{}", a.source_type),
            "reef_id": parents.and_then(|p| p.first()),
            "person_id": coral_person.get(&a.atom_id),
        }));
    }

    // ── Build links ───────────────────────────────────────────────────────────
    let mut links: Vec<Value> = Vec::new();

    // Ocean → reef / decision.
    for n in &reefs {
        let kind = if n.node_type == "decision" {
            "ocean-decision"
        } else {
            "ocean-reef"
        };
        links.push(json!({ "a": "ocean", "b": n.node_id, "kind": kind }));
    }

    // Reef/decision → coral (membership). A decision's members are "supports".
    for (atom_id, parents) in &coral_parents {
        for pid in parents {
            let kind = if *reef_is_decision.get(pid).unwrap_or(&false) {
                "supports"
            } else {
                "reef-coral"
            };
            links.push(json!({ "a": pid, "b": atom_id, "kind": kind }));
        }
    }

    // Coral → source, coral → person.
    for a in &atoms {
        links.push(json!({
            "a": a.atom_id,
            "b": format!("src:{}", a.source_type),
            "kind": "coral-source",
        }));
        if let Some(key) = coral_person.get(&a.atom_id) {
            links.push(json!({ "a": a.atom_id, "b": key, "kind": "authored" }));
        }
    }

    // Ocean → person ("member").
    for key in people.keys() {
        links.push(json!({ "a": "ocean", "b": key, "kind": "member" }));
    }

    // Reef ↔ reef typed edges (ADR-0008): confirmed + still-suggested, both
    // endpoints present. A decision endpoint renders the edge as "decision-reef".
    // Confirmed is iterated FIRST so that when a pair carries both a confirmed
    // and a suggested edge, the one-pair-once dedupe keeps the confirmed one —
    // the client draws suggested edges as tentative, and a confirmed fact must
    // never be downgraded to a guess by iteration order.
    let mut edge_seen: HashSet<(Uuid, Uuid)> = HashSet::new();
    for status in [EdgeStatus::Confirmed, EdgeStatus::Suggested] {
        let edges = scoped_store!(
            store,
            scoped_tx,
            list_relationship_edges_by_status
                / list_relationship_edges_by_status_scoped(graph_id, status)
        )
        .unwrap_or_default();
        for e in edges {
            if !reef_ids.contains(&e.src_node_id) || !reef_ids.contains(&e.dst_node_id) {
                continue;
            }
            // Hairball control: weak topical predicates are stored (they feed
            // expertise inference + synthesis) but NOT rendered on the canvas.
            if is_hidden_atlas_predicate(&e.predicate) {
                continue;
            }
            // One undirected pair once, regardless of predicate / direction.
            if !edge_seen.insert(undirected_pair(e.src_node_id, e.dst_node_id)) {
                continue;
            }
            let owner_spoke = self_node_id
                .map(|s| e.src_node_id == s || e.dst_node_id == s)
                .unwrap_or(false);
            links.push(reef_edge_link(&e, &reef_is_decision, owner_spoke));
        }
    }

    // Derived co-occurrence. Owner-touching pairs are excluded so the
    // you-hub never re-enters the layout through this layer.
    let requested_cooccur = q.cooccur.map(|n| n.clamp(1, 5));
    let fetch_min = requested_cooccur.unwrap_or(DEFAULT_COOCCUR);
    let entity_ids: Vec<Uuid> = reefs
        .iter()
        .filter(|n| n.node_type != "decision" && Some(n.node_id) != self_node_id)
        .map(|n| n.node_id)
        .collect();
    let mut co_pairs = scoped_store!(
        store,
        scoped_tx,
        list_cooccurrence_neighbors
            / list_cooccurrence_neighbors_scoped(graph_id, &entity_ids, fetch_min, self_node_id)
    )
    .unwrap_or_default();
    let used_threshold = effective_cooccur_threshold(requested_cooccur, co_pairs.len());
    if used_threshold > fetch_min {
        co_pairs.retain(|(_, _, shared)| i64::from(*shared) >= used_threshold);
    }
    // Both ends must be in the rendered node set (list_nodes is capped).
    // Ask still uses one-endpoint OR; Atlas already passed the selected
    // entity ids, so this only drops a pair whose other reef sat outside
    // MAX_ATLAS_NODES.
    co_pairs.retain(|(a, b, _)| reef_ids.contains(a) && reef_ids.contains(b));
    for (a, b, shared) in &co_pairs {
        links.push(json!({
            "a": a,
            "b": b,
            "kind": "co-occurs",
            "origin": "derived",
            "weight": shared,
            "owner": false,
        }));
    }

    // Derived decision anchors. Owner is dropped unless it is the only
    // candidate. Ranked title-containment then most-specific; `anchor_rank`
    // lets the client draw top-2 or all.
    let show_all_anchors = q.anchors.as_deref() == Some("all");
    let raw_anchors = scoped_store!(
        store,
        scoped_tx,
        list_decision_anchor_candidates
            / list_decision_anchor_candidates_scoped(graph_id, None)
    )
    .unwrap_or_default();
    let mut by_decision: HashMap<Uuid, Vec<(Uuid, i32, i64, String)>> = HashMap::new();
    for (decision_id, reef_id, shared) in raw_anchors {
        if !reef_ids.contains(&decision_id) || !reef_ids.contains(&reef_id) {
            continue;
        }
        let Some(reef) = reef_by_id.get(&reef_id) else {
            continue;
        };
        by_decision.entry(decision_id).or_default().push((
            reef_id,
            shared,
            reef.atom_count,
            reef.title.clone(),
        ));
    }
    for (decision_id, cands) in by_decision {
        let others: Vec<_> = cands
            .iter()
            .filter(|(rid, _, _, _)| Some(*rid) != self_node_id)
            .cloned()
            .collect();
        let pool = if others.is_empty() { cands } else { others };
        let Some(decision) = reef_by_id.get(&decision_id) else {
            continue;
        };
        let ranked = rank_decision_anchors(&decision.title, pool);
        for (reef_id, shared, rank) in ranked {
            if !show_all_anchors && rank > ANCHOR_TOP_K {
                continue;
            }
            links.push(json!({
                "a": decision_id,
                "b": reef_id,
                "kind": "decision-reef",
                "origin": "derived",
                "weight": shared,
                "anchor_rank": rank,
                "owner": self_node_id == Some(reef_id),
            }));
        }
    }

    // Read-only handler: everything is materialized; a commit failure (only
    // possible when a tolerated best-effort sub-query already aborted the
    // transaction) is logged, never surfaced.
    if let Some(tx) = scoped_tx.take() {
        if let Err(e) = tx.commit().await {
            tracing::warn!(error = %e, "read-only scoped transaction commit failed");
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "ocean": {
                "id": ocean.id,
                "name": ocean.name,
                "kind": ocean.kind,
                "is_primary": ocean.is_primary,
            },
            "self_node_id": self_node_id,
            "cooccur": used_threshold,
            "nodes": nodes,
            "links": links,
            "truncated": { "atoms": truncated_atoms },
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
        routing::get,
        Router,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::AppState;

    #[test]
    fn build_people_layer_upgrades_unresolved_entry_on_later_named_row() {
        // Same author_id, unresolved row arrives first: the person should still
        // resolve once a later row for that id carries a name — order must not
        // matter.
        let id = "U0AB12CD34".to_string();
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();
        let authors = vec![
            (a1, Some(id.clone()), None),
            (a2, Some(id.clone()), Some("Alice".to_string())),
        ];
        let (coral_person, people) = build_people_layer(&authors);
        let key = format!("person:{id}");
        assert_eq!(people.get(&key), Some(&Some("Alice".to_string())));
        assert_eq!(coral_person.get(&a1), Some(&key));
        assert_eq!(coral_person.get(&a2), Some(&key));
    }

    #[test]
    fn build_people_layer_keeps_first_name_never_downgrades() {
        // Once resolved, a later row for the same id — named or unnamed — must
        // not change the label.
        let id = "U0AB12CD34".to_string();
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();
        let a3 = Uuid::new_v4();
        let authors = vec![
            (a1, Some(id.clone()), Some("Alice".to_string())),
            (a2, Some(id.clone()), None),
            (a3, Some(id.clone()), Some("Bob".to_string())),
        ];
        let (_coral_person, people) = build_people_layer(&authors);
        let key = format!("person:{id}");
        assert_eq!(people.get(&key), Some(&Some("Alice".to_string())));
    }

    #[test]
    fn build_people_layer_label_never_from_raw_id() {
        // No author_name anywhere for this id: label stays None (never falls
        // back to the raw id) so the client renders "Unknown author".
        let id = "U0AB12CD34".to_string();
        let a1 = Uuid::new_v4();
        let authors = vec![(a1, Some(id.clone()), None)];
        let (_coral_person, people) = build_people_layer(&authors);
        let key = format!("person:{id}");
        assert_eq!(people.get(&key), Some(&None));
    }

    #[test]
    fn build_people_layer_whitespace_only_name_is_unresolved() {
        // A whitespace-only author_name must not count as resolved: it should
        // neither become the label nor stand in for a missing author_id as
        // the person key.
        let id = "U0AB12CD34".to_string();
        let a1 = Uuid::new_v4();
        let authors = vec![(a1, Some(id.clone()), Some("   ".to_string()))];
        let (_coral_person, people) = build_people_layer(&authors);
        let key = format!("person:{id}");
        assert_eq!(people.get(&key), Some(&None));

        // With no author_id at all, a whitespace-only name must not be used
        // as the fallback key either — the row is skipped entirely.
        let a2 = Uuid::new_v4();
        let authors_no_id = vec![(a2, None, Some("   ".to_string()))];
        let (coral_person_no_id, people_no_id) = build_people_layer(&authors_no_id);
        assert!(coral_person_no_id.is_empty());
        assert!(people_no_id.is_empty());
    }

    #[test]
    fn hidden_atlas_predicates_are_weak_topical_only() {
        // Weak topical edges are stored but not rendered.
        assert!(is_hidden_atlas_predicate(predicates::MENTIONS));
        assert!(is_hidden_atlas_predicate(predicates::MENTIONED_BY));
        assert!(is_hidden_atlas_predicate(predicates::RELATED_TO));
        // Strong involvement, inferred expertise, and interpersonal/org edges
        // are all rendered.
        for p in [
            predicates::WORKS_ON,
            predicates::RESPONSIBLE_FOR,
            predicates::EXPERT_IN,
            predicates::SPOUSE_OF,
            predicates::MANAGES,
            predicates::WORKS_AT,
        ] {
            assert!(
                !is_hidden_atlas_predicate(p),
                "{p} should render on the atlas"
            );
        }
    }

    fn edge(src: Uuid, dst: Uuid, status: EdgeStatus) -> RelationshipEdge {
        use crate::relationships::EdgeOrigin;
        RelationshipEdge {
            edge_id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            src_node_id: src,
            predicate: predicates::WORKS_ON.to_string(),
            dst_node_id: dst,
            origin: EdgeOrigin::Inferred,
            status,
            confidence: 0.42,
            provenance: json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            version: 1,
        }
    }

    #[test]
    fn undirected_pair_is_direction_independent() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        assert_eq!(undirected_pair(a, b), undirected_pair(b, a));
        let (lo, hi) = undirected_pair(a, b);
        assert!(lo <= hi);
    }

    #[test]
    fn reef_edge_link_carries_review_fields() {
        // The Graph explorer needs edge_id/status/origin/confidence to draw a
        // suggested edge as tentative and to confirm/reject it in place.
        let src = Uuid::new_v4();
        let dst = Uuid::new_v4();
        let e = edge(src, dst, EdgeStatus::Suggested);
        let v = reef_edge_link(&e, &HashMap::new(), false);
        assert_eq!(v["kind"], "reef-reef");
        assert_eq!(v["predicate"], predicates::WORKS_ON);
        assert_eq!(v["edge_id"], json!(e.edge_id));
        assert_eq!(v["status"], "suggested");
        assert_eq!(v["origin"], "inferred");
        assert!((v["confidence"].as_f64().unwrap() - 0.42).abs() < 1e-6);
    }

    #[test]
    fn reef_edge_link_reclassifies_decision_endpoint() {
        let src = Uuid::new_v4();
        let dst = Uuid::new_v4();
        let e = edge(src, dst, EdgeStatus::Confirmed);
        let mut is_decision = HashMap::new();
        is_decision.insert(dst, true);
        let v = reef_edge_link(&e, &is_decision, false);
        assert_eq!(v["kind"], "decision-reef");
        assert_eq!(v["status"], "confirmed");
    }

    #[test]
    fn pair_dedupe_keeps_first_seen_so_confirmed_wins() {
        // Mirrors the handler's loop: Confirmed is iterated before Suggested,
        // and the undirected-pair set admits a pair once — so a confirmed edge
        // and a reversed suggested duplicate collapse to the confirmed one.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let confirmed = edge(a, b, EdgeStatus::Confirmed);
        let suggested = edge(b, a, EdgeStatus::Suggested);
        let mut seen: HashSet<(Uuid, Uuid)> = HashSet::new();
        let mut links = Vec::new();
        for e in [&confirmed, &suggested] {
            if seen.insert(undirected_pair(e.src_node_id, e.dst_node_id)) {
                links.push(reef_edge_link(e, &HashMap::new(), false));
            }
        }
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["status"], "confirmed");
        assert_eq!(links[0]["edge_id"], json!(confirmed.edge_id));
    }

    #[test]
    fn reef_edge_link_flags_owner_spokes() {
        let src = Uuid::new_v4();
        let dst = Uuid::new_v4();
        let e = edge(src, dst, EdgeStatus::Confirmed);
        let v = reef_edge_link(&e, &HashMap::new(), true);
        assert_eq!(v["owner"], true);
        assert_eq!(reef_edge_link(&e, &HashMap::new(), false)["owner"], false);
    }

    #[test]
    fn cooccur_threshold_bumps_when_pairs_exceed_budget() {
        assert_eq!(effective_cooccur_threshold(None, 100), 2);
        assert_eq!(effective_cooccur_threshold(None, 5_001), 3);
        assert_eq!(effective_cooccur_threshold(Some(1), 9_999), 1);
        assert_eq!(effective_cooccur_threshold(Some(9), 10), 5);
    }

    #[test]
    fn decision_anchors_prefer_title_containment_then_specificity() {
        let coral = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let slack = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let generic = Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
        let ranked = rank_decision_anchors(
            "Ship the Slack connector on Primary",
            vec![
                (generic, 2, 12, "CoralSwarm".into()),
                (coral, 1, 80, "CoralSwarm".into()),
                (slack, 1, 40, "Slack connector".into()),
            ],
        );
        assert_eq!(ranked[0].0, slack);
        assert_eq!(ranked[0].2, 1);
        // Same containment miss: fewer corals wins (most specific).
        assert_eq!(ranked[1].0, generic);
        assert_eq!(ranked[2].0, coral);
    }

    fn atlas_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/graphs/{graph_id}/atlas", get(get_atlas))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn get_atlas_no_auth_returns_401() {
        let router = atlas_router(crate::test_app_state());
        let graph_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/graphs/{graph_id}/atlas"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // 503 (not 404) proves the route is registered; require_auth fires first.
    #[tokio::test]
    async fn get_atlas_registered() {
        let router = atlas_router(crate::test_app_state());
        let graph_id = Uuid::new_v4();
        let req = HttpRequest::builder()
            .method("GET")
            .uri(format!("/v1/graphs/{graph_id}/atlas"))
            .header("Authorization", "Bearer tok")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_ne!(resp.status(), StatusCode::NOT_FOUND);
    }
}
