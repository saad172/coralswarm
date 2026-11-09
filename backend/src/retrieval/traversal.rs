//! §5.4 — currents (relationship-edge) traversal: two-hop decay, hopped-reef
//! cap, and the atom-level flatten step that hands the result back in the
//! same `(atom_id, score)` shape [`super::rerank::rerank`] already accepts
//! (§5.2: "no `rerank` contract change"). Pure computation only — no I/O,
//! mirroring [`super::rerank`]'s own module contract. The caller
//! (`routes::queries::traverse_currents`) fetches [`RelationshipEdge`]s and
//! reef member atoms per hop and calls into this module to do the score
//! bookkeeping, capping, and flattening.
//!
//! Shipped ENTIRELY behind `GRAPH_TRAVERSAL_ENABLED` (default OFF, shadow
//! mode) — see `routes::queries::graph_traversal_enabled` and
//! `routes::queries::traverse_currents` for the flag-gated orchestration.
//! This module itself has no notion of the flag: it is pure math, always
//! safe to call, and it is the CALLER's job to decide whether to act on the
//! output for real or only log it (§5.5).

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::relationships::{
    is_traversable_edge,
    predicates::{self, Domain},
    RelationshipEdge,
};
use crate::store::MAX_SIBLING_ATOMS_PER_NODE;

/// Multiplicative decay applied to a reef's traversal score per additional
/// hop away from a seed reef (§5.4, §10's tuning table — an ESTIMATE with no
/// evidence yet, per §11: "None is derived; §7.1's fixture ocean and §5.5's
/// shadow mode exist to set them"). A SINGLE consistent per-hop multiplier,
/// not two independently chosen constants: hop 1 → `seed_score * 0.4`; hop 2
/// (reached through a hop-1 reef) → `seed_score * 0.4 * 0.4 == seed_score *
/// 0.16`, which is exactly the spec's literal "hop 1 × 0.4, hop 2 × 0.16"
/// table entry (0.4² = 0.16 is not a coincidence — see
/// `hop_decay_matches_the_two_hop_table` below).
pub const HOP_DECAY: f64 = 0.4;

/// Maximum currents-traversal depth (§5.4). Stated currents are walked
/// outbound, plus inbound for [`crate::relationships::predicates::is_undirected_for_ask`]
/// (the topical predicates whose reverse row is not written). Derived
/// co-occurrence pairs, when the caller supplies them, are undirected.
pub const MAX_HOPS: u8 = 2;

/// Minimum shared-coral count for a derived co-occurrence current to enter
/// Ask traversal (connectivity plan §9.1 / item 5).
pub const ASK_CO_OCCUR_MIN_SHARED: i64 = 2;

/// Tokens that mark a kinship question. Kinship currents are skipped unless
/// at least one of these appears after stopword stripping.
const KINSHIP_QUERY_TOKENS: &[&str] = &[
    "mom",
    "dad",
    "mother",
    "father",
    "parent",
    "parents",
    "child",
    "children",
    "wife",
    "husband",
    "spouse",
    "family",
    "daughter",
    "son",
    "sister",
    "brother",
    "aunt",
    "uncle",
    "cousin",
    "grandma",
    "grandpa",
    "grandmother",
    "grandfather",
];

/// True when the question is about family, so kinship currents may be walked.
pub fn query_looks_kinship(query_tokens: &[String]) -> bool {
    query_tokens
        .iter()
        .any(|t| KINSHIP_QUERY_TOKENS.contains(&t.as_str()))
}

/// Per-query walk policy. Fan-out ranks neighbours by the question, not by
/// extraction confidence. UUID is the last tie-break only.
///
/// Layers, highest first:
/// 1. Destination title overlap with the question (a named reef wins).
/// 2. Predicate fit: topical (`works_on`) beats professional (`reports_to`)
///    beats kinship.
/// 3. Derived shared-coral weight (small: stated topical still wins when
///    the dest is not named).
/// 4. Edge confidence.
///
/// Hard filters: never hop into `exclude` (the self reef); skip kinship
/// unless [`HopPolicy::kinship_ok`].
#[derive(Clone, Debug, Default)]
pub struct HopPolicy {
    pub query_tokens: Vec<String>,
    pub dest_titles: HashMap<Uuid, String>,
    pub exclude: HashSet<Uuid>,
    pub kinship_ok: bool,
    /// `(src, dst, shared)` with `src` in the current frontier. Undirected
    /// pairs; the caller has already dropped the self reef as `dst`.
    pub derived: Vec<(Uuid, Uuid, i32)>,
}

/// Maximum number of hopped-in reefs kept per query (§5.4), ranked by
/// decayed score, after ALL hops have been walked.
pub const HOPPED_REEF_CAP: usize = 5;

/// Maximum neighbours ONE frontier reef may contribute to the next hop.
///
/// Without this, a hub decides the whole result by itself. Every neighbour of
/// a given frontier reef receives the identical score `seed_score *
/// HOP_DECAY` — the decay is per-hop, not per-edge — so a reef's neighbours
/// arrive as one enormous tied block, and [`HOPPED_REEF_CAP`] then fills all
/// of its slots from whichever members of that block sort first. Measured on
/// the 2026-08-13 prod graph, the founder's person reef has 2,927 traversable
/// outbound edges; every query seeded anywhere near it would hop into five
/// arbitrary members of that 2,927 and nothing else.
///
/// Connectedness is not relevance. A reef that neighbours everything should
/// not thereby own the traversal budget, so each frontier reef gets a bounded
/// number of nominations and the cap chooses ACROSS reefs rather than within
/// one.
pub const MAX_FANOUT_PER_REEF: usize = 8;

/// Maximum reefs carried forward as the next hop's frontier.
///
/// Distinct from [`HOPPED_REEF_CAP`], which bounds the FINAL result: this
/// bounds the working set between hops. The loop previously advanced the
/// entire uncapped hop-1 map into hop 2, so a hub seed meant hop 2 issued one
/// `list_relationship_edges_for_nodes` over ~2,927 ids — and could never
/// place a single reef in the output, because every hop-2 reef scores
/// `seed * 0.16` against every hop-1 reef's `seed * 0.4`. Pure cost with a
/// structurally guaranteed zero yield.
///
/// Kept comfortably above [`HOPPED_REEF_CAP`] so hop 2 still has somewhere to
/// come from when hop 1 is thin — hop 2 wins slots only when hop 1 produced
/// fewer than [`HOPPED_REEF_CAP`] reefs, which is exactly when it should.
pub const MAX_FRONTIER_PER_HOP: usize = 32;

/// Hit-derived seed reefs (from retrieval atom hits), distinct from
/// named-query seeds. `node_ids_for_atoms` can fan ~108 hits into every
/// associated reef; those ids become the first-hop frontier before
/// [`MAX_FRONTIER_PER_HOP`] applies. Named title matches do not consume
/// this budget.
pub const MAX_HIT_SEED_REEFS: usize = 8;

/// One hop of currents traversal: given the CURRENT frontier's decayed
/// scores and the outbound edges leaving that frontier, compute the next
/// hop's frontier — `dst_node_id -> decayed_score`, max-merged across every
/// edge that reaches the same destination (a reef reachable via two
/// different traversable edges keeps its BEST path's score, not a summed
/// one — mirrors `store::flatten_reef_atoms`'s existing max-merge
/// convention). `visited` excludes any reef already reached at an earlier
/// hop (including the original seeds), so a cycle in the currents graph can
/// neither re-inflate a score nor let a reef re-enter the frontier and
/// extend traversal past [`MAX_HOPS`] through a loop.
///
/// Edges failing [`is_traversable_edge`] (§5.3 — predicate/confidence/trust
/// gate) are silently skipped; callers MAY pre-filter for efficiency (fewer
/// rows to pass in), but this is the single point of truth, so passing an
/// unfiltered edge set is always safe. An edge whose `src_node_id` is not in
/// `frontier` at all (e.g. the caller over-fetched) is likewise skipped —
/// only genuine hop-N -> hop-(N+1) edges contribute.
/// Per-hop fan-out is bounded by [`MAX_FANOUT_PER_REEF`]: each frontier reef
/// nominates only its strongest neighbours, ranked by [`HopPolicy`]
/// (question overlap, then predicate fit, then derived weight, then
/// confidence; `dst_node_id` last). Without that bound a single hub reef
/// supplies every candidate the cap can hold — see
/// [`MAX_FANOUT_PER_REEF`] for the measured case.
pub fn hop_scores(
    frontier: &HashMap<Uuid, f64>,
    outbound_edges: &[RelationshipEdge],
    visited: &HashSet<Uuid>,
    policy: &HopPolicy,
) -> HopOutcome {
    let mut by_src: HashMap<Uuid, Vec<WalkCandidate>> = HashMap::new();

    for edge in outbound_edges {
        if !is_traversable_edge(edge) {
            continue;
        }
        if skip_kinship(policy, &edge.predicate) {
            continue;
        }
        if frontier.contains_key(&edge.src_node_id) {
            consider_walk(
                &mut by_src,
                edge.src_node_id,
                edge.dst_node_id,
                &edge.predicate,
                edge.confidence,
                0,
                visited,
                policy,
            );
        }
        if predicates::is_undirected_for_ask(&edge.predicate)
            && frontier.contains_key(&edge.dst_node_id)
        {
            consider_walk(
                &mut by_src,
                edge.dst_node_id,
                edge.src_node_id,
                &edge.predicate,
                edge.confidence,
                0,
                visited,
                policy,
            );
        }
    }

    for &(src, dst, shared) in &policy.derived {
        if !frontier.contains_key(&src) {
            continue;
        }
        let conf = derived_confidence(shared);
        consider_walk(
            &mut by_src,
            src,
            dst,
            predicates::CO_OCCURS_WITH,
            conf,
            shared,
            visited,
            policy,
        );
    }

    let mut next: HashMap<Uuid, HopCandidate> = HashMap::new();
    let mut currents = Vec::new();
    for (src, mut edges) in by_src {
        edges.sort_by(|a, b| {
            b.nomination
                .total_cmp(&a.nomination)
                .then_with(|| a.dst.cmp(&b.dst))
        });
        let score = frontier[&src] * HOP_DECAY;
        for edge in edges.into_iter().take(MAX_FANOUT_PER_REEF) {
            let slot = next.entry(edge.dst).or_insert(HopCandidate {
                score: f64::MIN,
                best_confidence: f32::MIN,
            });
            if score > slot.score || (score == slot.score && edge.nomination > slot.best_confidence)
            {
                slot.score = score;
                slot.best_confidence = edge.nomination;
            }
            currents.push(WalkedCurrent {
                src,
                dst: edge.dst,
                predicate: edge.predicate,
                derived: edge.derived,
            });
        }
    }
    HopOutcome { next, currents }
}

/// One hop's destinations plus the currents that actually made the per-reef
/// fan-out cut. Ask's side panel renders `currents`; [`cap_hopped_reefs`]
/// still sees only `next`.
#[derive(Debug, Clone, Default)]
pub struct HopOutcome {
    pub next: HashMap<Uuid, HopCandidate>,
    pub currents: Vec<WalkedCurrent>,
}

/// A current the walk nominated (stated, inferred, or derived co-occurrence).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkedCurrent {
    pub src: Uuid,
    pub dst: Uuid,
    pub predicate: String,
    pub derived: bool,
}

/// Reefs + currents this question's walk produced. Always computed (shadow
/// mode included); `merged` says whether `GRAPH_TRAVERSAL_ENABLED` actually
/// folded the hopped atoms into retrieval.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraversalWalk {
    pub merged: bool,
    pub seeds: Vec<WalkReef>,
    pub hops: Vec<WalkReef>,
    pub currents: Vec<WalkedCurrent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkReef {
    pub id: Uuid,
    pub title: String,
    pub hop: u8,
}

struct WalkCandidate {
    dst: Uuid,
    nomination: f32,
    predicate: String,
    derived: bool,
}

fn skip_kinship(policy: &HopPolicy, predicate: &str) -> bool {
    !policy.kinship_ok && predicates::meta(predicate).is_some_and(|m| m.domain == Domain::Kinship)
}

fn consider_walk(
    by_src: &mut HashMap<Uuid, Vec<WalkCandidate>>,
    src: Uuid,
    dst: Uuid,
    predicate: &str,
    confidence: f32,
    shared: i32,
    visited: &HashSet<Uuid>,
    policy: &HopPolicy,
) {
    if visited.contains(&dst) || policy.exclude.contains(&dst) || src == dst {
        return;
    }
    if skip_kinship(policy, predicate) {
        return;
    }
    let nomination = nomination_score(policy, predicate, dst, confidence, shared);
    by_src.entry(src).or_default().push(WalkCandidate {
        dst,
        nomination,
        predicate: predicate.to_string(),
        derived: shared > 0,
    });
}

fn dest_title_overlap(policy: &HopPolicy, dst: Uuid) -> f32 {
    let Some(title) = policy.dest_titles.get(&dst) else {
        return 0.0;
    };
    if policy.query_tokens.is_empty() {
        return 0.0;
    }
    let dest_toks = super::node_match_tokens(title);
    if dest_toks.is_empty() {
        return 0.0;
    }
    let q: HashSet<&str> = policy.query_tokens.iter().map(|s| s.as_str()).collect();
    let hits = dest_toks.iter().filter(|t| q.contains(t.as_str())).count();
    hits as f32 / dest_toks.len() as f32
}

fn predicate_fit(predicate: &str) -> f32 {
    match predicates::meta(predicate).map(|m| m.domain) {
        Some(Domain::Topical) => 0.85,
        Some(Domain::Professional) => 0.45,
        Some(Domain::Kinship) => 0.15,
        _ => 0.25,
    }
}

fn derived_weight(shared: i32) -> f32 {
    if shared <= 0 {
        return 0.0;
    }
    ((shared as f32).ln() / 10f32.ln()).clamp(0.0, 1.0)
}

fn derived_confidence(shared: i32) -> f32 {
    (0.70 + 0.03 * shared as f32).clamp(predicates::TRAVERSABLE_MIN_CONFIDENCE, 1.0)
}

/// Combined fan-out rank. Stored on [`HopCandidate::best_confidence`] so the
/// hopped-reef cap and the hop-2 frontier prefer the same neighbours.
fn nomination_score(
    policy: &HopPolicy,
    predicate: &str,
    dst: Uuid,
    confidence: f32,
    shared: i32,
) -> f32 {
    dest_title_overlap(policy, dst) * 10.0
        + predicate_fit(predicate)
        + derived_weight(shared) * 0.2
        + confidence * 0.1
}

/// One reef reached by a hop: its decayed score, plus the confidence of the
/// edge it was reached by.
///
/// The confidence exists solely to break score ties meaningfully. Every
/// neighbour of a given frontier reef receives an IDENTICAL score (the decay
/// is per-hop, not per-edge), so ties are the normal case rather than an edge
/// case, and the previous `node_id`-ascending tiebreak resolved them by
/// random UUID — deterministic, but with no relationship to the graph. A
/// reef reached by a 0.95-confidence `manages` edge is a better bet than one
/// reached by a 0.71-confidence `works_on`, and this is the only signal at
/// hand that says so.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HopCandidate {
    /// Seed score decayed once per hop travelled (§5.4's table).
    pub score: f64,
    /// Combined fan-out rank of the winning path. Used only to break hop-score
    /// ties in [`cap_hopped_reefs`] / [`cap_frontier`].
    pub best_confidence: f32,
}

/// Bound the working set carried into the next hop at
/// [`MAX_FRONTIER_PER_HOP`], best-scoring first, ties broken by edge
/// confidence then `node_id`.
///
/// This is deliberately NOT [`cap_hopped_reefs`]: that one bounds the final
/// answer, this one bounds the query the next hop issues. See
/// [`MAX_FRONTIER_PER_HOP`] for why advancing the uncapped map was both
/// expensive and incapable of changing the result.
/// Returns the plain `node_id -> score` map [`hop_scores`] takes as its next
/// `frontier`, so the caller never has to project the candidates back by hand.
pub fn cap_frontier(hopped: &HashMap<Uuid, HopCandidate>) -> HashMap<Uuid, f64> {
    if hopped.len() <= MAX_FRONTIER_PER_HOP {
        return hopped.iter().map(|(&id, c)| (id, c.score)).collect();
    }
    let mut v: Vec<(&Uuid, &HopCandidate)> = hopped.iter().collect();
    v.sort_by(|a, b| {
        b.1.score
            .total_cmp(&a.1.score)
            .then_with(|| b.1.best_confidence.total_cmp(&a.1.best_confidence))
            .then_with(|| a.0.cmp(b.0))
    });
    v.truncate(MAX_FRONTIER_PER_HOP);
    v.into_iter().map(|(&id, c)| (id, c.score)).collect()
}

/// Cap a merged hop-score map at [`HOPPED_REEF_CAP`], highest decayed score
/// first, ties broken by the reaching edge's confidence and then by `node_id`
/// ascending (the last purely for determinism — see [`HopCandidate`] for why
/// `node_id` alone was the wrong primary tiebreak).
pub fn cap_hopped_reefs(hopped: HashMap<Uuid, HopCandidate>) -> Vec<(Uuid, f64)> {
    let mut v: Vec<(Uuid, HopCandidate)> = hopped.into_iter().collect();
    v.sort_by(|a, b| {
        b.1.score
            .total_cmp(&a.1.score)
            .then_with(|| b.1.best_confidence.total_cmp(&a.1.best_confidence))
            .then_with(|| a.0.cmp(&b.0))
    });
    v.truncate(HOPPED_REEF_CAP);
    v.into_iter().map(|(id, c)| (id, c.score)).collect()
}

/// Flatten the capped hopped reefs into atom-level candidates: each reef
/// contributes up to [`MAX_SIBLING_ATOMS_PER_NODE`] atoms — the caller
/// supplies them ALREADY ranked by cosine similarity to the query embedding
/// (`Store::reef_atoms_ranked_by_similarity`, the D1 fix applied to hopped
/// reefs — never heap/insertion order) — and every included atom is scored
/// at its contributing reef's decayed score (max-merged when an atom belongs
/// to more than one hopped reef, same convention as
/// `store::flatten_reef_atoms`). The output slots directly into
/// `routes::queries::merge_keyword_hits` unchanged (§5.2: no `rerank`
/// contract change).
pub fn flatten_hopped_atoms(
    hopped_reefs: &[(Uuid, f64)],
    atoms_by_node: &HashMap<Uuid, Vec<Uuid>>,
) -> Vec<(Uuid, f64)> {
    let mut best: HashMap<Uuid, f64> = HashMap::new();
    for &(node_id, score) in hopped_reefs {
        let Some(atoms) = atoms_by_node.get(&node_id) else {
            continue;
        };
        for &atom_id in atoms.iter().take(MAX_SIBLING_ATOMS_PER_NODE) {
            let slot = best.entry(atom_id).or_insert(f64::MIN);
            if score > *slot {
                *slot = score;
            }
        }
    }
    let mut out: Vec<(Uuid, f64)> = best.into_iter().collect();
    out.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relationships::{predicates, EdgeOrigin, EdgeStatus};
    use chrono::Utc;

    fn traversable_edge(src: Uuid, predicate: &str, dst: Uuid) -> RelationshipEdge {
        RelationshipEdge {
            edge_id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            src_node_id: src,
            predicate: predicate.to_string(),
            dst_node_id: dst,
            origin: EdgeOrigin::Stated,
            status: EdgeStatus::Confirmed,
            confidence: 0.9,
            provenance: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version: 1,
        }
    }

    /// A `HopCandidate` literal, for the cap tests.
    fn cand(score: f64, best_confidence: f32) -> HopCandidate {
        HopCandidate {
            score,
            best_confidence,
        }
    }

    /// A traversable edge with a caller-chosen confidence.
    fn edge_with_conf(src: Uuid, dst: Uuid, confidence: f32) -> RelationshipEdge {
        RelationshipEdge {
            confidence,
            ..traversable_edge(src, predicates::MANAGES, dst)
        }
    }

    fn hop(
        frontier: &HashMap<Uuid, f64>,
        edges: &[RelationshipEdge],
        visited: &HashSet<Uuid>,
    ) -> HashMap<Uuid, HopCandidate> {
        hop_scores(frontier, edges, visited, &HopPolicy::default()).next
    }

    // ── Decay arithmetic ──────────────────────────────────────────────────

    #[test]
    fn hop_decay_matches_the_two_hop_table() {
        // §5.4 / §10: "hop 1 × 0.4, hop 2 × 0.16" — a single per-hop
        // multiplier compounds to exactly this two-level table.
        assert_eq!(HOP_DECAY, 0.4);
        let hop1 = 1.0 * HOP_DECAY;
        let hop2 = hop1 * HOP_DECAY;
        assert!((hop1 - 0.4).abs() < 1e-12, "hop1 = {hop1}");
        assert!((hop2 - 0.16).abs() < 1e-12, "hop2 = {hop2}");
    }

    #[test]
    fn max_hops_and_cap_match_the_spec() {
        assert_eq!(MAX_HOPS, 2);
        assert_eq!(HOPPED_REEF_CAP, 5);
    }

    #[test]
    fn one_hop_applies_the_decay_to_the_seed_score() {
        let seed = Uuid::new_v4();
        let dst = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(seed, 1.0);
        let edges = vec![traversable_edge(seed, predicates::MANAGES, dst)];
        let visited: HashSet<Uuid> = [seed].into_iter().collect();

        let next = hop(&frontier, &edges, &visited);
        assert_eq!(next.len(), 1);
        assert!((next[&dst].score - 0.4).abs() < 1e-12);
    }

    #[test]
    fn two_hops_compound_to_point_one_six() {
        let seed = Uuid::new_v4();
        let hop1 = Uuid::new_v4();
        let hop2 = Uuid::new_v4();

        let mut frontier = HashMap::new();
        frontier.insert(seed, 1.0);
        let mut visited: HashSet<Uuid> = [seed].into_iter().collect();

        let hop1_edges = vec![traversable_edge(seed, predicates::MANAGES, hop1)];
        let hop1_scores = hop(&frontier, &hop1_edges, &visited);
        assert!((hop1_scores[&hop1].score - 0.4).abs() < 1e-12);
        visited.insert(hop1);

        let hop2_edges = vec![traversable_edge(hop1, predicates::WORKS_ON, hop2)];
        let hop2_scores = hop(&cap_frontier(&hop1_scores), &hop2_edges, &visited);
        assert!(
            (hop2_scores[&hop2].score - 0.16).abs() < 1e-9,
            "hop2 score {} != 0.16",
            hop2_scores[&hop2].score
        );
    }

    #[test]
    fn non_traversable_edges_are_skipped() {
        let seed = Uuid::new_v4();
        let dst = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(seed, 1.0);
        // `mentions` is on §5.3's Never list.
        let edges = vec![traversable_edge(seed, predicates::MENTIONS, dst)];
        let next = hop(&frontier, &edges, &HashSet::new());
        assert!(next.is_empty());
    }

    #[test]
    fn visited_reefs_are_excluded_from_the_next_hop_cycle_guard() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(a, 1.0);
        // A cycle back into an already-visited reef must not re-enter the frontier.
        let edges = vec![traversable_edge(a, predicates::MANAGES, b)];
        let visited: HashSet<Uuid> = [a, b].into_iter().collect(); // b already visited
        let next = hop(&frontier, &edges, &visited);
        assert!(
            next.is_empty(),
            "already-visited destination must not re-enter the frontier"
        );
    }

    #[test]
    fn multiple_paths_to_the_same_reef_keep_the_best_score() {
        let strong_seed = Uuid::new_v4();
        let weak_seed = Uuid::new_v4();
        let dst = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(strong_seed, 1.0);
        frontier.insert(weak_seed, 0.1);
        let edges = vec![
            traversable_edge(weak_seed, predicates::MANAGES, dst),
            traversable_edge(strong_seed, predicates::MANAGES, dst),
        ];
        let next = hop(&frontier, &edges, &HashSet::new());
        assert!(
            (next[&dst].score - 1.0 * HOP_DECAY).abs() < 1e-12,
            "must keep the stronger path's score"
        );
    }

    #[test]
    fn edge_whose_src_is_not_in_the_frontier_is_ignored() {
        let outside = Uuid::new_v4();
        let dst = Uuid::new_v4();
        let frontier: HashMap<Uuid, f64> = HashMap::new(); // empty — `outside` never seeded
        let edges = vec![traversable_edge(outside, predicates::MANAGES, dst)];
        let next = hop(&frontier, &edges, &HashSet::new());
        assert!(next.is_empty());
    }

    // ── Cap ───────────────────────────────────────────────────────────────

    // ── Hub containment (the reason this module changed) ──────────────────

    /// The measured prod shape: one seed with a huge outbound degree, one
    /// seed with a small one. Before the fan-out bound the hub supplied every
    /// candidate and the modest seed's neighbours could not place at all,
    /// because all 2,927 tied at the same score and `node_id` decided.
    #[test]
    fn a_hub_cannot_crowd_out_a_modest_seeds_neighbours() {
        let hub = Uuid::new_v4();
        let modest = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(hub, 1.0);
        frontier.insert(modest, 1.0);
        let visited: HashSet<Uuid> = [hub, modest].into_iter().collect();

        let mut edges: Vec<RelationshipEdge> = Vec::new();
        let mut hub_dsts: HashSet<Uuid> = HashSet::new();
        for _ in 0..500 {
            let dst = Uuid::new_v4();
            hub_dsts.insert(dst);
            edges.push(edge_with_conf(hub, dst, 0.80));
        }
        // One modest neighbour, reached by a STRONGER edge.
        let modest_dst = Uuid::new_v4();
        edges.push(edge_with_conf(modest, modest_dst, 0.99));

        let next = hop(&frontier, &edges, &visited);
        let from_hub = next.keys().filter(|k| hub_dsts.contains(k)).count();
        assert_eq!(
            from_hub, MAX_FANOUT_PER_REEF,
            "the hub must nominate exactly its allowance, not all 500"
        );
        assert!(
            next.contains_key(&modest_dst),
            "the modest seed must still be represented"
        );

        // And in the final cap the stronger edge outranks the hub's block.
        let capped = cap_hopped_reefs(next);
        assert_eq!(
            capped[0].0, modest_dst,
            "a 0.99 edge must beat a tied block of 0.80 edges"
        );
    }

    #[test]
    fn fanout_bound_is_per_reef_not_per_edge_list() {
        // Two reefs, each over the allowance: each keeps its own budget.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(a, 1.0);
        frontier.insert(b, 1.0);
        let visited: HashSet<Uuid> = [a, b].into_iter().collect();

        let mut edges = Vec::new();
        for _ in 0..(MAX_FANOUT_PER_REEF * 3) {
            edges.push(edge_with_conf(a, Uuid::new_v4(), 0.9));
            edges.push(edge_with_conf(b, Uuid::new_v4(), 0.9));
        }
        let next = hop(&frontier, &edges, &visited);
        assert_eq!(
            next.len(),
            MAX_FANOUT_PER_REEF * 2,
            "each frontier reef gets its own allowance"
        );
    }

    #[test]
    fn fanout_bound_keeps_the_strongest_edges() {
        let src = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(src, 1.0);
        let visited: HashSet<Uuid> = [src].into_iter().collect();

        // One clearly-strongest edge buried among many weak ones.
        let strong_dst = Uuid::new_v4();
        let mut edges = vec![edge_with_conf(src, strong_dst, 0.99)];
        for _ in 0..(MAX_FANOUT_PER_REEF * 4) {
            edges.push(edge_with_conf(src, Uuid::new_v4(), 0.71));
        }
        let next = hop(&frontier, &edges, &visited);
        assert_eq!(next.len(), MAX_FANOUT_PER_REEF);
        assert!(
            next.contains_key(&strong_dst),
            "the allowance must be spent on the strongest edges, not an arbitrary slice"
        );
    }

    #[test]
    fn inbound_works_on_hops_from_the_object_reef() {
        let person = Uuid::new_v4();
        let topic = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(topic, 1.0);
        let visited: HashSet<Uuid> = [topic].into_iter().collect();
        let edges = vec![traversable_edge(person, predicates::WORKS_ON, topic)];
        let next = hop(&frontier, &edges, &visited);
        assert!(
            next.contains_key(&person),
            "Ask must walk inbound works_on: the reverse row is not stored"
        );
    }

    #[test]
    fn inbound_manages_does_not_flip() {
        let manager = Uuid::new_v4();
        let report = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(report, 1.0);
        let visited: HashSet<Uuid> = [report].into_iter().collect();
        let edges = vec![traversable_edge(manager, predicates::MANAGES, report)];
        let next = hop(&frontier, &edges, &visited);
        assert!(
            next.is_empty(),
            "manages is not undirected_for_ask; reports_to is the materialized inverse"
        );
    }

    #[test]
    fn dest_named_in_the_query_beats_higher_confidence() {
        let src = Uuid::new_v4();
        let named = Uuid::from_u128(0xAA);
        let other = Uuid::from_u128(0xBB);
        let mut frontier = HashMap::new();
        frontier.insert(src, 1.0);
        let visited: HashSet<Uuid> = [src].into_iter().collect();
        let mut named_edge = traversable_edge(src, predicates::WORKS_ON, named);
        named_edge.confidence = 0.71;
        let mut other_edge = traversable_edge(src, predicates::WORKS_ON, other);
        other_edge.confidence = 0.99;
        let mut policy = HopPolicy {
            query_tokens: vec!["coderabbit".into()],
            dest_titles: HashMap::new(),
            ..HopPolicy::default()
        };
        policy.dest_titles.insert(named, "CodeRabbit".into());
        policy.dest_titles.insert(other, "PR #208".into());
        let next = hop_scores(&frontier, &[named_edge, other_edge], &visited, &policy).next;
        let capped = cap_hopped_reefs(next);
        assert_eq!(capped[0].0, named);
    }

    #[test]
    fn kinship_is_skipped_unless_the_question_looks_kinship() {
        let src = Uuid::new_v4();
        let child = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(src, 1.0);
        let visited: HashSet<Uuid> = [src].into_iter().collect();
        let edges = vec![traversable_edge(src, predicates::PARENT_OF, child)];
        let next = hop(&frontier, &edges, &visited);
        assert!(next.is_empty(), "parent_of must not hop on a work question");

        let mut kin = HopPolicy::default();
        kin.kinship_ok = true;
        let next = hop_scores(&frontier, &edges, &visited, &kin).next;
        assert!(next.contains_key(&child));
    }

    #[test]
    fn self_reef_is_never_a_hop_target() {
        let src = Uuid::new_v4();
        let me = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(src, 1.0);
        let visited: HashSet<Uuid> = [src].into_iter().collect();
        let edges = vec![traversable_edge(src, predicates::REPORTS_TO, me)];
        let policy = HopPolicy {
            exclude: [me].into_iter().collect(),
            ..HopPolicy::default()
        };
        let next = hop_scores(&frontier, &edges, &visited, &policy).next;
        assert!(next.is_empty());
    }

    #[test]
    fn registering_co_occurs_with_keeps_the_derived_current_rank() {
        // Ask's derived currents carry this predicate id; registering it as
        // a stored derived edge must not lift them to the topical rank.
        assert_eq!(predicate_fit(predicates::CO_OCCURS_WITH), predicate_fit("not_a_predicate"));
    }

    #[test]
    fn derived_cooccurrence_hops_when_stated_edges_are_absent() {
        let src = Uuid::new_v4();
        let partner = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(src, 1.0);
        let visited: HashSet<Uuid> = [src].into_iter().collect();
        let policy = HopPolicy {
            derived: vec![(src, partner, 12)],
            ..HopPolicy::default()
        };
        let next = hop_scores(&frontier, &[], &visited, &policy);
        assert!(next.next.contains_key(&partner));
        assert!((next.next[&partner].score - 0.4).abs() < 1e-12);
        assert_eq!(next.currents.len(), 1);
        assert_eq!(next.currents[0].predicate, "co_occurs_with");
        assert!(next.currents[0].derived);
    }

    #[test]
    fn hop_scores_records_the_fanout_current() {
        let seed = Uuid::new_v4();
        let dst = Uuid::new_v4();
        let mut frontier = HashMap::new();
        frontier.insert(seed, 1.0);
        let visited: HashSet<Uuid> = [seed].into_iter().collect();
        let out = hop_scores(
            &frontier,
            &[traversable_edge(seed, predicates::MANAGES, dst)],
            &visited,
            &HopPolicy::default(),
        );
        assert_eq!(out.currents.len(), 1);
        assert_eq!(out.currents[0].src, seed);
        assert_eq!(out.currents[0].dst, dst);
        assert_eq!(out.currents[0].predicate, predicates::MANAGES);
        assert!(!out.currents[0].derived);
    }

    #[test]
    fn query_looks_kinship_is_token_exact() {
        assert!(query_looks_kinship(&["Casey".into(), "mom".into()]));
        assert!(!query_looks_kinship(&["coralswarm".into(), "work".into()]));
    }

    // ── Frontier bound between hops ───────────────────────────────────────

    #[test]
    fn cap_frontier_bounds_the_working_set_and_projects_scores() {
        let mut hopped = HashMap::new();
        for i in 0..(MAX_FRONTIER_PER_HOP * 2) {
            hopped.insert(Uuid::new_v4(), cand(i as f64 * 0.01, 0.9));
        }
        let capped = cap_frontier(&hopped);
        assert_eq!(capped.len(), MAX_FRONTIER_PER_HOP);
        // Kept the HIGHEST scores, not an arbitrary slice.
        let min_kept = capped.values().cloned().fold(f64::MAX, f64::min);
        let expected_min = ((MAX_FRONTIER_PER_HOP * 2 - MAX_FRONTIER_PER_HOP) as f64) * 0.01;
        assert!(
            (min_kept - expected_min).abs() < 1e-9,
            "kept min {min_kept} should be {expected_min}"
        );
    }

    #[test]
    fn cap_frontier_is_a_no_op_under_the_bound() {
        let mut hopped = HashMap::new();
        let id = Uuid::new_v4();
        hopped.insert(id, cand(0.4, 0.9));
        let capped = cap_frontier(&hopped);
        assert_eq!(capped.len(), 1);
        assert!(
            (capped[&id] - 0.4).abs() < 1e-12,
            "score must survive the projection"
        );
    }

    /// Hop 2 can only ever place when hop 1 under-fills the result cap — the
    /// decay guarantees `seed*0.16 < seed*0.4`. This pins the ordering fact
    /// the frontier bound relies on, so shrinking the working set cannot
    /// cost a result that would otherwise have been returned.
    #[test]
    fn hop_two_never_outranks_a_full_hop_one() {
        assert!(HOP_DECAY * HOP_DECAY < HOP_DECAY);
        assert!(
            MAX_FRONTIER_PER_HOP >= HOPPED_REEF_CAP,
            "the working set must never be narrower than the result it feeds"
        );
        assert!(
            MAX_HIT_SEED_REEFS <= MAX_FRONTIER_PER_HOP,
            "hit-derived seeds must not overflow the first-hop working set"
        );
    }

    #[test]
    fn cap_hopped_reefs_keeps_the_top_five_by_score() {
        let mut hopped = HashMap::new();
        let ids: Vec<Uuid> = (0..8).map(|_| Uuid::new_v4()).collect();
        for (i, &id) in ids.iter().enumerate() {
            hopped.insert(id, cand(i as f64 * 0.1, 0.9));
        }
        let capped = cap_hopped_reefs(hopped);
        assert_eq!(capped.len(), HOPPED_REEF_CAP);
        // Highest scores first: the top 5 by index (3..8) in descending order.
        let expected: Vec<Uuid> = ids[3..8].iter().rev().copied().collect();
        let got: Vec<Uuid> = capped.iter().map(|(id, _)| *id).collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn cap_hopped_reefs_is_a_no_op_under_the_cap() {
        let mut hopped = HashMap::new();
        hopped.insert(Uuid::new_v4(), cand(0.4, 0.9));
        hopped.insert(Uuid::new_v4(), cand(0.16, 0.9));
        let capped = cap_hopped_reefs(hopped);
        assert_eq!(capped.len(), 2);
    }

    #[test]
    fn cap_hopped_reefs_ties_break_on_confidence_then_node_id() {
        // Equal score AND equal confidence: node_id ascending, as before.
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let mut hopped = HashMap::new();
        hopped.insert(b, cand(0.4, 0.9));
        hopped.insert(a, cand(0.4, 0.9));
        let capped = cap_hopped_reefs(hopped);
        assert_eq!(
            capped[0].0, a,
            "fully equal candidates break on node_id ascending"
        );

        // Equal score, DIFFERENT confidence: the stronger edge wins even
        // though its node_id sorts later. This is the fix for selection by
        // random UUID — with a hub, equal scores are the normal case.
        let mut hopped = HashMap::new();
        hopped.insert(a, cand(0.4, 0.71));
        hopped.insert(b, cand(0.4, 0.95));
        let capped = cap_hopped_reefs(hopped);
        assert_eq!(
            capped[0].0, b,
            "on equal scores the higher-confidence edge must outrank a lower node_id"
        );
    }

    // ── Flatten ───────────────────────────────────────────────────────────

    #[test]
    fn flatten_hopped_atoms_scores_every_atom_at_its_reefs_decayed_score() {
        let reef = Uuid::new_v4();
        let a1 = Uuid::new_v4();
        let a2 = Uuid::new_v4();
        let mut atoms_by_node = HashMap::new();
        atoms_by_node.insert(reef, vec![a1, a2]);

        let flat = flatten_hopped_atoms(&[(reef, 0.4)], &atoms_by_node);
        assert_eq!(flat.len(), 2);
        for (_, score) in &flat {
            assert_eq!(*score, 0.4);
        }
    }

    #[test]
    fn flatten_hopped_atoms_caps_at_max_sibling_atoms_per_node() {
        let reef = Uuid::new_v4();
        let atoms: Vec<Uuid> = (0..(MAX_SIBLING_ATOMS_PER_NODE + 5))
            .map(|_| Uuid::new_v4())
            .collect();
        let mut atoms_by_node = HashMap::new();
        atoms_by_node.insert(reef, atoms.clone());

        let flat = flatten_hopped_atoms(&[(reef, 0.4)], &atoms_by_node);
        assert_eq!(flat.len(), MAX_SIBLING_ATOMS_PER_NODE);
        // The caller is responsible for pre-ranking `atoms_by_node`'s Vec by
        // cosine similarity (§5.4's D1-style fix); this function just takes
        // the first `cap`, so it must be the FIRST N of the input order.
        let flat_ids: std::collections::HashSet<Uuid> = flat.iter().map(|(id, _)| *id).collect();
        for id in atoms.iter().take(MAX_SIBLING_ATOMS_PER_NODE) {
            assert!(flat_ids.contains(id));
        }
    }

    #[test]
    fn flatten_hopped_atoms_max_merges_an_atom_shared_by_two_hopped_reefs() {
        let reef_a = Uuid::new_v4();
        let reef_b = Uuid::new_v4();
        let shared_atom = Uuid::new_v4();
        let mut atoms_by_node = HashMap::new();
        atoms_by_node.insert(reef_a, vec![shared_atom]);
        atoms_by_node.insert(reef_b, vec![shared_atom]);

        let flat = flatten_hopped_atoms(&[(reef_a, 0.16), (reef_b, 0.4)], &atoms_by_node);
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].0, shared_atom);
        assert_eq!(
            flat[0].1, 0.4,
            "shared atom keeps the higher of the two reef scores"
        );
    }

    #[test]
    fn flatten_hopped_atoms_empty_input_is_empty_output() {
        let flat = flatten_hopped_atoms(&[], &HashMap::new());
        assert!(flat.is_empty());
    }
}
