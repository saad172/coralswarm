//! Pure Atlas source read and assembly. Shared with the stored model worker.
use crate::{
    relationships::{predicates, EdgeStatus, RelationshipEdge},
    store::page_sources,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
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

pub async fn compute(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    q: &AtlasQuery,
) -> Result<Value, crate::read_models::Error> {
    let (id, name, kind, is_primary): (Uuid, String, String, bool) = sqlx::query_as(
        "SELECT id,name,kind,is_primary FROM graphs WHERE id=$1 AND archived_at IS NULL",
    )
    .bind(graph_id)
    .fetch_one(&mut **tx)
    .await?;
    let ocean = Ocean {
        id,
        name,
        kind,
        is_primary,
    };
    let reefs = page_sources::list_nodes_scoped(tx, graph_id, None, MAX_ATLAS_NODES, 0).await?;
    let reef_ids: HashSet<Uuid> = reefs.iter().map(|n| n.node_id).collect();
    let reef_is_decision: HashMap<Uuid, bool> = reefs
        .iter()
        .map(|n| (n.node_id, n.node_type == "decision"))
        .collect();
    let reef_by_id: HashMap<Uuid, &crate::store::Node> =
        reefs.iter().map(|n| (n.node_id, n)).collect();

    let self_node_id = page_sources::graph_self_node_id_scoped(tx, graph_id).await?;

    // ── Corals (newest 100 — the atlas is a recency sample) ───────────────────
    let atoms = page_sources::list_atoms_scoped(tx, graph_id).await?;
    let truncated_atoms = atoms.len() >= 100;
    let atom_ids: Vec<Uuid> = atoms.iter().map(|a| a.atom_id).collect();
    let atom_set: HashSet<Uuid> = atom_ids.iter().copied().collect();

    // ── Reef↔coral membership (best-effort; expand caps siblings per reef) ────
    // No query embedding here — the atlas renders EVERY coral in the ocean,
    // not a retrieval result, so seeds-first ordering still applies but
    // filler ranking falls back to the deterministic secondary order (see
    // `Store::expand`'s doc comment).
    let membership = page_sources::expand_scoped(tx, graph_id, &atom_ids, None).await?;
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
    let authors = page_sources::atom_authors_scoped(tx, &atom_ids).await?;
    let (coral_person, people) = build_people_layer(&authors);

    // ── Coral labels (best-effort; readable beats opaque source_id) ───────────
    // Scoped, for the same reason as the people layer above.
    let previews: HashMap<Uuid, String> = page_sources::atom_inline_content_scoped(tx, &atom_ids)
        .await?
        .into_iter()
        .filter_map(|(id, c, _, _, header_len)| {
            c.and_then(|c| coral_label(crate::store::strip_header_prefix(&c, header_len)))
                .map(|l| (id, l))
        })
        .collect();

    // ── Promotion provenance (best-effort): which reefs were copied in from
    // another ocean, and from which. Drives the Graph explorer's cross-ocean
    // "promoted" ring; an empty map is a degraded atlas, not a failed one. ──
    let promoted_from: HashMap<Uuid, Uuid> =
        page_sources::node_promotion_origins_scoped(tx, graph_id)
            .await?
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
        let edges =
            page_sources::list_relationship_edges_by_status_scoped(tx, graph_id, status).await?;
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
    let mut co_pairs = page_sources::list_cooccurrence_neighbors_scoped(
        tx,
        graph_id,
        &entity_ids,
        fetch_min,
        self_node_id,
    )
    .await?;
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
    let raw_anchors =
        page_sources::list_decision_anchor_candidates_scoped(tx, graph_id, None).await?;
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

    Ok(
        json!({"ocean":{"id":ocean.id,"name":ocean.name,"kind":ocean.kind,"is_primary":ocean.is_primary},
        "self_node_id":self_node_id,"cooccur":used_threshold,"nodes":nodes,"links":links,"truncated":{"atoms":truncated_atoms}}),
    )
}
struct Ocean {
    id: Uuid,
    name: String,
    kind: String,
    is_primary: bool,
}
