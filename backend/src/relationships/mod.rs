//! Relationship knowledge graph (ADR-0008).
//!
//! Typed relationship edges between entity reefs (graph nodes), plus the
//! scaffolding for automatic inference and human-in-the-loop review.
//!
//! An edge is a directed `(src) --predicate--> (dst)` fact, e.g.
//! `Alex --spouse_of--> Harper`. Edges carry:
//!   * an [`EdgeOrigin`] — was it directly **stated** in an atom, or **inferred**
//!     by composing other edges?
//!   * an [`EdgeStatus`] — review lifecycle (`suggested` → `confirmed`/`rejected`).
//!   * a `confidence` in `[0, 1]`.
//!   * `provenance` — for stated edges the source atom ids; for inferred edges the
//!     composition rule + the source edge ids it was derived from.
//!
//! The predicate vocabulary and the composition rules that drive inference live in
//! [`predicates`].

pub mod inference;
pub mod predicates;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// At/above this confidence, an `Inferred` edge is auto-written `Confirmed`
/// rather than `Suggested` — it skips the human review queue entirely.
///
/// Shared by every inference-producing worker
/// ([`crate::workers::inference_closure`]'s structural composition pass, and
/// the expertise pass until its 2026-10-18 retirement) so the threshold cannot
/// silently drift between them. A `Confirmed` edge written
/// this way MUST carry `"auto_confirmed": true` in its `provenance` JSON —
/// that flag is the only way to later distinguish a machine-confirmed edge
/// from a human-reviewed one for audit or bulk-revert purposes.
pub const AUTO_CONFIRM_CONFIDENCE: f32 = 0.85;

/// Where an edge came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EdgeOrigin {
    /// Directly extracted from atom text (e.g. "Harper is my wife").
    Stated,
    /// Derived by composing other edges via a [`predicates::CompositionRule`].
    Inferred,
}

impl EdgeOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            EdgeOrigin::Stated => "stated",
            EdgeOrigin::Inferred => "inferred",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "stated" => Some(EdgeOrigin::Stated),
            "inferred" => Some(EdgeOrigin::Inferred),
            _ => None,
        }
    }
}

/// Review lifecycle of an edge.
///
/// Stated edges and high-confidence inferences are written as `Confirmed`; lower
/// confidence inferences land as `Suggested` and surface in the review queue. A user
/// `Rejected` edge is retained as a negative constraint so inference does not
/// re-propose it (cf. `non_supersession_pair`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EdgeStatus {
    Suggested,
    Confirmed,
    Rejected,
}

impl EdgeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            EdgeStatus::Suggested => "suggested",
            EdgeStatus::Confirmed => "confirmed",
            EdgeStatus::Rejected => "rejected",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "suggested" => Some(EdgeStatus::Suggested),
            "confirmed" => Some(EdgeStatus::Confirmed),
            "rejected" => Some(EdgeStatus::Rejected),
            _ => None,
        }
    }
}

/// A typed relationship edge between two reefs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipEdge {
    pub edge_id: Uuid,
    pub graph_id: Uuid,
    pub src_node_id: Uuid,
    pub predicate: String,
    pub dst_node_id: Uuid,
    pub origin: EdgeOrigin,
    pub status: EdgeStatus,
    pub confidence: f32,
    pub provenance: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i64,
}

/// Provenance payload for a **stated** edge: the atoms that asserted it.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatedProvenance {
    pub atom_ids: Vec<Uuid>,
}

/// Provenance payload for an **inferred** edge: the rule + the edges it composed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredProvenance {
    /// Human-readable rule id, e.g. `"spouse_of∘parent_of→parent_of"`.
    pub rule: String,
    /// The edges (by id) that were composed to derive this one.
    pub source_edge_ids: Vec<Uuid>,
}

impl RelationshipEdge {
    /// Convenience: typed view of `provenance` when this is a stated edge.
    pub fn stated_provenance(&self) -> Option<StatedProvenance> {
        serde_json::from_value(self.provenance.clone()).ok()
    }

    /// Convenience: typed view of `provenance` when this is an inferred edge.
    pub fn inferred_provenance(&self) -> Option<InferredProvenance> {
        serde_json::from_value(self.provenance.clone()).ok()
    }
}

/// The full §5.3 traversability predicate — the ONE gate §5.4's currents
/// traversal applies to every candidate edge before walking it:
///
/// ```text
/// traversable(edge) =
///      predicate.traversable
///   && confidence >= 0.70
///   && status != Rejected
///   && ( origin = Stated
///     || (origin = Inferred && status = Confirmed) )
/// ```
///
/// The trust clause deliberately mirrors the
/// synthesis prompt's own citation tiers — `[stated]` and `[inferred,
/// confirmed]` render as facts, `[inferred, unconfirmed]` must be hedged — so
/// retrieval and synthesis share one definition of "established fact"
/// instead of drifting into two. This framing exists specifically BECAUSE
/// `status = Confirmed` is otherwise a no-op today (D6:
/// stated-edge writers write `Confirmed` for every STATED edge,
/// bypassing the review queue's `suggested` default) — without the
/// `origin = Stated` arm this predicate would need every one of the ~1,650
/// already-written stated edges to be re-reviewed before any of them could
/// ever be walked, which was explicitly rejected (§5.3) as dumping a queue
/// nobody will work through. An INFERRED edge gets no such pass: it must
/// have actually been reviewed and confirmed (the review queue D6 leaves
/// alive for exactly this population) before this predicate trusts it enough
/// to traverse.
///
/// `predicate.traversable` (see [`predicates::is_traversable_predicate`]) is
/// a SEPARATE axis from [`predicates::is_extractable`]/
/// `is_retired_from_extraction` (§6.1) — a predicate can be non-extractable
/// (no NEW rows written) while legacy rows under it still exist and must
/// still be excluded from traversal by this same flag; the two must never be
/// conflated into one check.
pub fn is_traversable_edge(edge: &RelationshipEdge) -> bool {
    edge.status != EdgeStatus::Rejected
        && predicates::is_traversable_predicate(&edge.predicate)
        && edge.confidence >= predicates::TRAVERSABLE_MIN_CONFIDENCE
        && match edge.origin {
            EdgeOrigin::Stated => true,
            EdgeOrigin::Inferred => edge.status == EdgeStatus::Confirmed,
        }
}

#[cfg(test)]
mod traversability_tests {
    use super::*;

    fn edge(
        predicate: &str,
        origin: EdgeOrigin,
        status: EdgeStatus,
        confidence: f32,
    ) -> RelationshipEdge {
        RelationshipEdge {
            edge_id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            src_node_id: Uuid::new_v4(),
            predicate: predicate.to_string(),
            dst_node_id: Uuid::new_v4(),
            origin,
            status,
            confidence,
            provenance: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version: 1,
        }
    }

    #[test]
    fn stated_high_confidence_traversable_predicate_is_traversable() {
        let e = edge(
            predicates::MANAGES,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            0.95,
        );
        assert!(is_traversable_edge(&e));
    }

    #[test]
    fn stated_edge_needs_no_confirmed_status_to_be_traversable() {
        // D6: stated edges are written `Confirmed` regardless, but the
        // predicate itself must not REQUIRE it on the stated arm — this
        // exercises the stated bypass for Suggested; Rejected is always excluded.
        let e = edge(
            predicates::WORKS_AT,
            EdgeOrigin::Stated,
            EdgeStatus::Suggested,
            0.95,
        );
        assert!(is_traversable_edge(&e));
    }

    #[test]
    fn inferred_unconfirmed_edge_is_never_traversable_even_with_high_confidence() {
        let e = edge(
            predicates::MANAGES,
            EdgeOrigin::Inferred,
            EdgeStatus::Suggested,
            0.99,
        );
        assert!(!is_traversable_edge(&e));
    }

    #[test]
    fn inferred_confirmed_edge_is_traversable() {
        let e = edge(
            predicates::REPORTS_TO,
            EdgeOrigin::Inferred,
            EdgeStatus::Confirmed,
            0.8,
        );
        assert!(is_traversable_edge(&e));
    }

    #[test]
    fn inferred_rejected_edge_is_never_traversable() {
        let e = edge(
            predicates::MANAGES,
            EdgeOrigin::Inferred,
            EdgeStatus::Rejected,
            0.99,
        );
        assert!(!is_traversable_edge(&e));
    }

    #[test]
    fn stated_rejected_edge_is_never_traversable() {
        let e = edge(
            predicates::MANAGES,
            EdgeOrigin::Stated,
            EdgeStatus::Rejected,
            0.99,
        );
        assert!(!is_traversable_edge(&e));
    }

    #[test]
    fn below_confidence_floor_is_not_traversable() {
        let e = edge(
            predicates::MANAGES,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            0.69,
        );
        assert!(!is_traversable_edge(&e));
    }

    #[test]
    fn exactly_at_confidence_floor_is_traversable() {
        let e = edge(
            predicates::MANAGES,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            0.70,
        );
        assert!(is_traversable_edge(&e));
    }

    #[test]
    fn never_traversable_predicate_is_excluded_regardless_of_confidence_or_trust() {
        for p in [
            predicates::MENTIONS,
            predicates::RELATED_TO,
            predicates::KNOWS,
            predicates::COLLEAGUE_OF,
            predicates::PEER_OF,
        ] {
            let e = edge(p, EdgeOrigin::Stated, EdgeStatus::Confirmed, 1.0);
            assert!(!is_traversable_edge(&e), "{p} must never be traversable");
        }
    }

    #[test]
    fn retired_from_extraction_predicates_stay_non_traversable_on_legacy_rows() {
        // A legacy `related_to`/`knows`/`colleague_of` row (§6.1's population
        // that keeps existing after extraction stops writing new ones) must
        // still be excluded — this is the scenario the task's own caveat
        // about #423 calls out explicitly.
        for p in [predicates::RELATED_TO, predicates::KNOWS, predicates::COLLEAGUE_OF] {
            assert!(
                predicates::is_known(p),
                "{p} must remain known (legacy rows)"
            );
            let e = edge(p, EdgeOrigin::Stated, EdgeStatus::Confirmed, 1.0);
            assert!(!is_traversable_edge(&e));
        }
    }
}
