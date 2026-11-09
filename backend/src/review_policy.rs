//! Review-queue policy: the constants and impact predicate behind
//! "high-signal review" — the queue only ever asks a human about items that are
//! BOTH genuinely uncertain AND structurally consequential; everything else
//! auto-resolves (see the staged plan in the 2026-08-05 design artifact).
//!
//! One module owns every threshold so the policy can be retuned in a one-line
//! PR and can never drift between the producers that consult it
//! (`workers::infer_expertise`, `workers::inference_closure`, the merge
//! suggester, and — later stages — the deadline sweep and chat-card gate).
//!
//! ## The impact predicate
//!
//! An item (edge / decision / merge pair) is **high-impact** iff any involved
//! node is, and a node is high-impact iff:
//!   * it is involved in a supersession/contradiction (that IS a clarification
//!     that must happen), or
//!   * its relationship-edge degree ≥ [`IMPACT_DEGREE`], or
//!   * its citing-coral count (`nodes.coral_count`, migration 0109) ≥
//!     [`IMPACT_ATOMS`].
//!
//! Deliberately deterministic and LLM-free: the gate decides what a human
//! never sees, so it must be cheap, explainable on the review card
//! ("touches N connections · M corals"), and immune to model-calibration
//! drift. The counts come from [`crate::store::Store::node_structural_impact`].
//!
//! ## Which nodes count: symmetric vs. asymmetric items
//!
//! "Any involved node" ([`any_high_impact`]) is right for a SYMMETRIC item — a
//! merge pair, where the ruling is about both reefs at once. It is wrong for an
//! ASYMMETRIC one, where a hub sits on one side by construction and makes the
//! OR unconditionally true; those gate on the subject alone
//! ([`subject_high_impact`]). Getting this backwards does not fail loudly: the
//! gate keeps returning `true`, every item queues, and the only symptom is a
//! review surface nobody can trust. Pick the predicate that matches the item's
//! shape, and state on the call site which shape it is.

use uuid::Uuid;

/// Relationship-edge degree (src + dst appearances) at/above which a node is
/// structurally consequential.
pub const IMPACT_DEGREE: i64 = 5;

/// Citing-coral count (`nodes.coral_count`) at/above which a node is
/// structurally consequential.
pub const IMPACT_ATOMS: i64 = 3;

/// Below this confidence an inference is too speculative to act on at all —
/// matches the workers' existing suggest/soft floors; nothing below it is
/// queued OR auto-resolved, it is simply not asserted.
pub const UNCERTAINTY_FLOOR: f32 = 0.5;

/// How long a queued clarification may wait for a human before the machine
/// applies its best guess (Stage-1 deadline sweep). The queue is an offer,
/// never a debt: the badge is structurally bounded by this window's inflow.
pub const REVIEW_DEADLINE_DAYS: i64 = 7;

/// At/above this confidence the deadline sweep resolves an expired edge by
/// CONFIRMING it; below, the edge is blanked into the evaluation ledger.
///
/// This is pinned (by test, below) to
/// [`crate::relationships::predicates::TRAVERSABLE_MIN_CONFIDENCE`]: an edge
/// the sweep blanks could never have been traversable anyway, and an edge it
/// confirms becomes traversable only above the same floor a human confirm
/// would have applied — deadline resolution can widen traversal only within
/// the band the reviewer was being asked about.
pub const DEADLINE_LEAN_CONFIRM: f32 = 0.70;

/// Structural counts for one node, as returned by
/// [`crate::store::Store::node_structural_impact`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeImpact {
    pub node_id: Uuid,
    /// `relationship_edge` rows where the node is src OR dst (any status),
    /// excluding derived `co_occurs_with` rows.
    pub degree: i64,
    /// `nodes.coral_count` — the reef's live atom-link count.
    pub coral_count: i64,
    /// The node appears on either side of a `supersession_edge` row.
    pub conflict_involved: bool,
}

impl NodeImpact {
    /// The impact predicate for a single node (module doc).
    pub fn is_high_impact(&self) -> bool {
        self.conflict_involved
            || self.degree >= IMPACT_DEGREE
            || self.coral_count >= IMPACT_ATOMS
    }
}

/// An item is high-impact iff ANY involved node is. Empty input (no impact
/// rows fetched, e.g. endpoints vanished mid-flight) is LOW impact: the gate
/// must fail toward "don't bother a human", never toward queueing noise.
///
/// Correct only when the item's endpoints are SYMMETRIC — a merge pair is the
/// motivating case: either reef being consequential makes the ruling
/// consequential, because the ruling is about both. When one endpoint is a hub
/// **by construction**, use [`subject_high_impact`] instead; see its doc for
/// why the OR silently stops filtering there.
pub fn any_high_impact(nodes: &[NodeImpact]) -> bool {
    nodes.iter().any(NodeImpact::is_high_impact)
}

/// The impact row for one specific node, or `None` if it isn't in `nodes`.
///
/// Callers that gate on a subject also want to REPORT that subject's counts on
/// the review card, so the lookup is exposed rather than folded into
/// [`subject_high_impact`].
pub fn subject_impact(nodes: &[NodeImpact], subject: Uuid) -> Option<&NodeImpact> {
    nodes.iter().find(|n| n.node_id == subject)
}

/// High-impact predicate for an item with an ASYMMETRIC endpoint pair: only
/// the `subject` — the node whose meaning is actually in doubt — counts.
///
/// [`any_high_impact`] is unusable when the other endpoint is a hub by
/// construction. An `expert_in` edge always has a person on the src side, and
/// a person reef in any active ocean clears [`IMPACT_DEGREE`] within days and
/// never falls back below it. The OR then evaluates to `true` for every claim
/// about that person no matter how trivial the object of the claim, so the
/// gate admits everything and the review queue fills with noise —
/// `Alex expert_in "PR #595"` queues on the strength of the person's 4,678
/// edges, which say nothing about whether that topic is worth a human's
/// attention. Gating on the subject asks the question the card is actually
/// asking about.
///
/// A missing subject row is LOW impact, matching [`any_high_impact`]'s
/// fail-toward-not-queueing rule: absent evidence never queues.
pub fn subject_high_impact(nodes: &[NodeImpact], subject: Uuid) -> bool {
    subject_impact(nodes, subject).is_some_and(NodeImpact::is_high_impact)
}

/// Is this topic substantive enough to carry an ASSERTION about it — as
/// opposed to consequential enough to be worth QUEUEING (the predicates
/// above)?
///
/// Deliberately narrower than [`NodeImpact::is_high_impact`]: citing corals
/// ONLY, no `degree` limb and no `conflict_involved` limb.
///
/// `degree` is excluded because it does not measure what it appears to.
/// It counts raw `relationship_edge` rows touching the node, so it
/// double-counts every inverse-materialized twin (`works_on` and
/// `worked_on_by` are ONE fact stored as two rows; likewise
/// `mentions`/`mentioned_by`, `responsible_for`/`responsibility_of`) and it
/// includes the candidate edge being judged. Measured on the 2026-08-13 prod
/// corpus: 29 of 29 topics that cleared [`IMPACT_DEGREE`] did so purely on
/// those artifacts — recomputing degree as distinct neighbours excluding the
/// candidate dropped every one of them below the bar. A limb that admits
/// everything is not a gate.
///
/// `conflict_involved` is excluded because it argues for a human's attention,
/// not for the truth of an unrelated expertise claim.
///
/// A reef with zero citing corals is something nothing in the ocean points
/// at; asserting expertise in it manufactures a fact from an artifact.
pub fn topic_is_substantive(nodes: &[NodeImpact], subject: Uuid) -> bool {
    subject_impact(nodes, subject).is_some_and(|n| n.coral_count >= IMPACT_ATOMS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relationships::predicates::TRAVERSABLE_MIN_CONFIDENCE;

    fn ni(degree: i64, coral_count: i64, conflict: bool) -> NodeImpact {
        NodeImpact { node_id: Uuid::new_v4(), degree, coral_count, conflict_involved: conflict }
    }

    /// Same, with a caller-chosen id so a subject can be addressed.
    fn ni_id(id: Uuid, degree: i64, coral_count: i64, conflict: bool) -> NodeImpact {
        NodeImpact { node_id: id, degree, coral_count, conflict_involved: conflict }
    }

    /// The deadline sweep's safety argument depends on this equality — see
    /// [`DEADLINE_LEAN_CONFIRM`]'s doc. If the traversability floor moves,
    /// this test forces a deliberate decision here rather than a silent skew.
    #[test]
    fn deadline_lean_confirm_equals_traversability_floor() {
        assert_eq!(DEADLINE_LEAN_CONFIRM, TRAVERSABLE_MIN_CONFIDENCE);
    }

    #[test]
    fn conflict_involvement_auto_qualifies() {
        assert!(ni(0, 0, true).is_high_impact());
    }

    #[test]
    fn degree_and_coral_thresholds_are_inclusive() {
        assert!(ni(IMPACT_DEGREE, 0, false).is_high_impact());
        assert!(!ni(IMPACT_DEGREE - 1, 0, false).is_high_impact());
        assert!(ni(0, IMPACT_ATOMS, false).is_high_impact());
        assert!(!ni(0, IMPACT_ATOMS - 1, false).is_high_impact());
    }

    #[test]
    fn item_impact_is_any_not_all() {
        assert!(any_high_impact(&[ni(0, 0, false), ni(IMPACT_DEGREE, 0, false)]));
        assert!(!any_high_impact(&[ni(1, 1, false), ni(0, 0, false)]));
        // Empty fails LOW: the gate must never queue noise on missing data.
        assert!(!any_high_impact(&[]));
    }

    /// The regression that let 20 of 23 review items be ephemeral expertise
    /// claims: a hub on the non-subject side satisfies `any_high_impact` on its
    /// own, so the OR admits an item whose subject is trivial. This asserts the
    /// two predicates DISAGREE on exactly that shape — if a refactor ever makes
    /// `subject_high_impact` delegate to the OR, this fails.
    #[test]
    fn hub_endpoint_does_not_carry_a_trivial_subject() {
        let topic = Uuid::new_v4();
        // person: the founder's real prod degree. topic: a fresh PR-title reef.
        let pair = [ni(4678, 40, false), ni_id(topic, 1, 1, false)];

        assert!(any_high_impact(&pair), "the OR is satisfied by the hub alone");
        assert!(!subject_high_impact(&pair, topic), "the subject gate must reject the trivial topic");
    }

    #[test]
    fn subject_gate_admits_a_consequential_subject() {
        let topic = Uuid::new_v4();
        // Each of the three impact routes must carry the subject on its own.
        for subject in [
            ni_id(topic, IMPACT_DEGREE, 0, false),
            ni_id(topic, 0, IMPACT_ATOMS, false),
            ni_id(topic, 0, 0, true),
        ] {
            assert!(subject_high_impact(&[ni(0, 0, false), subject], topic));
        }
        // ...and just below both thresholds, without a conflict, it must not.
        assert!(!subject_high_impact(
            &[ni(0, 0, false), ni_id(topic, IMPACT_DEGREE - 1, IMPACT_ATOMS - 1, false)],
            topic
        ));
    }

    #[test]
    fn absent_subject_row_fails_low() {
        // Endpoint vanished mid-flight, or the impact query returned only the
        // other node: never queue on evidence we don't have.
        let topic = Uuid::new_v4();
        assert!(!subject_high_impact(&[ni(4678, 40, true)], topic));
        assert!(!subject_high_impact(&[], topic));
        assert!(subject_impact(&[], topic).is_none());
    }

    #[test]
    fn subject_impact_returns_the_subjects_own_counts() {
        // The card reports these numbers; picking the wrong row is how every
        // card came to read "touches 4678 connections".
        let topic = Uuid::new_v4();
        let pair = [ni(4678, 40, false), ni_id(topic, 7, 2, false)];
        let got = subject_impact(&pair, topic).expect("subject row present");
        assert_eq!((got.degree, got.coral_count), (7, 2));
    }

    /// `topic_is_substantive` must be citing-corals ONLY. Each of the other
    /// two `is_high_impact` limbs is asserted to be non-admitting here, so a
    /// refactor that "unifies" the predicates fails loudly instead of
    /// silently restoring the gate that admitted everything.
    #[test]
    fn substance_is_corals_only_not_degree_or_conflict() {
        let topic = Uuid::new_v4();

        // Degree alone must not admit — the person-hub shape (4,701 edges in
        // prod) that made the old OR unconditionally true.
        let by_degree = [ni_id(topic, 4701, 0, false)];
        assert!(any_high_impact(&by_degree), "precondition: is_high_impact admits it");
        assert!(!topic_is_substantive(&by_degree, topic));

        // Conflict alone must not admit — a contradiction elsewhere in the
        // graph is not evidence that an expertise claim is worth asserting.
        let by_conflict = [ni_id(topic, 0, 0, true)];
        assert!(any_high_impact(&by_conflict), "precondition: is_high_impact admits it");
        assert!(!topic_is_substantive(&by_conflict, topic));

        // Corals at the threshold admit; one below does not.
        assert!(topic_is_substantive(&[ni_id(topic, 0, IMPACT_ATOMS, false)], topic));
        assert!(!topic_is_substantive(&[ni_id(topic, 0, IMPACT_ATOMS - 1, false)], topic));
    }

    /// Same fail-toward-not-asserting rule the queueing predicates use: an
    /// absent subject row is never treated as substantive.
    #[test]
    fn absent_subject_is_not_substantive() {
        let topic = Uuid::new_v4();
        assert!(!topic_is_substantive(&[], topic));
        assert!(!topic_is_substantive(&[ni(4701, 2816, true)], topic));
    }

    #[test]
    fn uncertainty_band_is_coherent() {
        assert!(UNCERTAINTY_FLOOR < DEADLINE_LEAN_CONFIRM);
        assert!(DEADLINE_LEAN_CONFIRM < crate::relationships::AUTO_CONFIRM_CONFIDENCE);
    }
}
