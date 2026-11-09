//! Generality / anti-overfitting proof for the relationship engine.
//!
//! The relationship-graph code was *developed against* one real ocean (a specific
//! family). This suite proves the engine is **principle-based, not data-shaped**:
//! the SAME `infer()` + predicate registry + retrieval scorer, run on data that
//! looks nothing like that family (different names, a pure professional org chart,
//! in-laws, names ending in "s"), produces correct results with ZERO code changes.
//!
//! Everything here is pure (no DB, no LLM), so it runs in normal CI. If any of
//! these fail, the engine was relying on something specific to the original data.

use coralswarm_backend::relationships::inference::{infer, EdgeInput, InferredEdge};
use coralswarm_backend::relationships::predicates::{self, *};
use coralswarm_backend::relationships::AUTO_CONFIRM_CONFIDENCE as AUTO_CONFIRM;
use coralswarm_backend::retrieval::{node_query_tokens, node_match_score};
use uuid::Uuid;

const SOFT_FLOOR: f32 = 0.5; // mirrors inference_closure::SOFT_FLOOR

fn e(src: Uuid, predicate: &str, dst: Uuid, confidence: f32) -> EdgeInput {
    EdgeInput { edge_id: Uuid::new_v4(), src, predicate: predicate.to_string(), dst, confidence }
}

fn find<'a>(out: &'a [InferredEdge], src: Uuid, p: &str, dst: Uuid) -> Option<&'a InferredEdge> {
    out.iter().find(|x| x.src == src && x.predicate == p && x.dst == dst)
}

/// The registered base confidence for a composition rule `a ∘ b`.
fn base_conf(a: &str, b: &str) -> f32 {
    predicates::compositions()
        .iter()
        .find(|r| r.a == a && r.b == b)
        .unwrap_or_else(|| panic!("no composition rule {a} ∘ {b}"))
        .base_confidence
}

// ── Inference: the headline rule on a DIFFERENT family ──────────────────────────

#[test]
fn spouse_of_a_parent_is_inferred_parent_for_arbitrary_names() {
    // "Wei is Bjorn's spouse; Bjorn is Priya's parent" ⇒ Wei is (likely) Priya's parent.
    // Same shape as the original "who is Casey's mom → Harper", zero shared names.
    let (wei, bjorn, priya) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let edges = vec![
        e(wei, SPOUSE_OF, bjorn, 0.95),
        e(bjorn, SPOUSE_OF, wei, 0.95),
        e(bjorn, PARENT_OF, priya, 0.9),
        e(priya, CHILD_OF, bjorn, 0.9),
    ];
    let out = infer(&edges);
    let hit = find(&out, wei, PARENT_OF, priya).expect("Wei must be inferred as Priya's parent");
    assert!(hit.defeasible, "spouse-of-a-parent must be defeasible (step/social parent)");
    assert!(
        hit.confidence > SOFT_FLOOR && hit.confidence < AUTO_CONFIRM,
        "defeasible guess lands in the review band, got {}",
        hit.confidence
    );
    // Confidence comes from the RULE × weakest source — not from the names.
    let expected = base_conf(SPOUSE_OF, PARENT_OF) * 0.9_f32.min(0.95);
    assert!((hit.confidence - expected).abs() < 1e-6, "confidence is rule-derived");
}

#[test]
fn no_false_bridges_between_unconnected_people() {
    // Two disjoint families sharing NO node must produce NO cross-inferences —
    // the generalization of "must never infer Jordan is Casey's parent".
    let (wei, bjorn, priya) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let (mara, sol) = (Uuid::new_v4(), Uuid::new_v4());
    let edges = vec![
        e(wei, SPOUSE_OF, bjorn, 0.95),
        e(bjorn, SPOUSE_OF, wei, 0.95),
        e(bjorn, PARENT_OF, priya, 0.9),
        e(mara, PARENT_OF, sol, 0.9), // a totally separate parent/child
    ];
    let out = infer(&edges);
    // No edge may connect the two components in any direction.
    for x in &out {
        let crosses = (x.src == wei || x.src == bjorn || x.src == priya) && (x.dst == mara || x.dst == sol)
            || (x.src == mara || x.src == sol) && (x.dst == wei || x.dst == bjorn || x.dst == priya);
        assert!(!crosses, "inference invented a bridge across unconnected people: {x:?}");
    }
}

// ── Inference: structural closures are confident + non-defeasible, any names ────

#[test]
fn grandparent_and_aunt_uncle_are_structural_for_arbitrary_names() {
    let (olga, mateo, nadia, ingrid) =
        (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let edges = vec![
        e(olga, PARENT_OF, mateo, 0.95),
        e(mateo, PARENT_OF, nadia, 0.95),
        e(ingrid, SIBLING_OF, mateo, 0.95),
        e(mateo, SIBLING_OF, ingrid, 0.95),
    ];
    let out = infer(&edges);

    let gp = find(&out, olga, GRANDPARENT_OF, nadia).expect("parent∘parent ⇒ grandparent");
    assert!(!gp.defeasible, "grandparent closure is structural, not defeasible");
    assert!(gp.confidence >= AUTO_CONFIRM, "structural closure auto-confirms, got {}", gp.confidence);

    let au = find(&out, ingrid, AUNT_UNCLE_OF, nadia).expect("sibling∘parent ⇒ aunt/uncle");
    assert!(!au.defeasible);
}

#[test]
fn in_law_relations_compose_for_arbitrary_names() {
    let (bjorn, aiko, chen, wei, priya, sol) = (
        Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(),
        Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(),
    );
    let edges = vec![
        // sibling ∘ spouse ⇒ sibling-in-law
        e(bjorn, SIBLING_OF, aiko, 0.9),
        e(aiko, SIBLING_OF, bjorn, 0.9),
        e(aiko, SPOUSE_OF, chen, 0.9),
        e(chen, SPOUSE_OF, aiko, 0.9),
        // parent ∘ spouse ⇒ parent-in-law
        e(wei, PARENT_OF, priya, 0.9),
        e(priya, SPOUSE_OF, sol, 0.9),
        e(sol, SPOUSE_OF, priya, 0.9),
    ];
    let out = infer(&edges);
    assert!(find(&out, bjorn, SIBLING_IN_LAW_OF, chen).is_some(), "sibling∘spouse ⇒ sibling-in-law");
    assert!(find(&out, wei, PARENT_IN_LAW_OF, sol).is_some(), "parent∘spouse ⇒ parent-in-law");
}

// ── Inference: a PURE professional org chart — zero kinship ─────────────────────

#[test]
fn skip_level_reporting_and_management_compose_with_no_kinship() {
    let (dana, omar, yuki, lena, park, sol) = (
        Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(),
        Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(),
    );
    let edges = vec![
        e(dana, REPORTS_TO, omar, 0.95),
        e(omar, REPORTS_TO, yuki, 0.95),
        e(lena, MANAGES, park, 0.95),
        e(park, MANAGES, sol, 0.95),
    ];
    let out = infer(&edges);
    let skip = find(&out, dana, REPORTS_TO, yuki).expect("reports_to∘reports_to ⇒ skip-level");
    assert!(skip.defeasible, "skip-level is defeasible (matrix orgs defeat it)");
    assert!(find(&out, lena, MANAGES, sol).is_some(), "manages∘manages ⇒ skip-level management");
}

// ── The engine is a general algebra, not a one-rule patch ───────────────────────

#[test]
fn registry_is_a_broad_algebra_across_domains() {
    // If the system had been reverse-engineered for the one "who is X's mom"
    // query, you'd see ~one rule. Instead it's a multi-domain algebra.
    let comps = predicates::compositions();
    assert!(comps.len() >= 8, "expected a broad rule set, got {}", comps.len());
    let domains: std::collections::HashSet<_> =
        predicates::registry().iter().map(|m| m.domain).collect();
    assert!(domains.len() >= 3, "expected kinship + professional + social domains");
    // Both a defeasible social rule AND structural non-defeasible closures exist.
    assert!(comps.iter().any(|r| r.defeasible), "has defeasible (social) rules");
    assert!(comps.iter().any(|r| !r.defeasible), "has structural (certain) closures");
}

#[test]
fn inference_never_invents_self_edges_for_any_data() {
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    // A⇄B spouse + B parent A would, naively composed, yield parent(A,A).
    let edges = vec![
        e(a, SPOUSE_OF, b, 0.9),
        e(b, SPOUSE_OF, a, 0.9),
        e(b, PARENT_OF, a, 0.9),
    ];
    for x in infer(&edges) {
        assert_ne!(x.src, x.dst, "inference produced a self-edge: {x:?}");
    }
}

// ── Retrieval: possessive/plural matching is a general rule, not "Casey" ─────────

#[test]
fn retrieval_handles_possessives_for_arbitrary_names() {
    // Possessive without apostrophe, non-family name: "danas" ⇒ matches reef "Dana".
    // Queries are kept short (<=2 non-stopword tokens) so a single-token hit
    // clears §5.6's `REEF_COVERAGE_FLOOR` (0.5) — this test is about
    // `possessive_stem`, not the coverage gate (see `retrieval::mod::tests`
    // for coverage-floor-specific cases).
    // `who`/`is` are question-word stopwords (2026-09-13) so a one-word
    // entity title can still seed Ask traversal on a natural question.
    let q = node_query_tokens("who is danas manager");
    assert_eq!(q, vec!["danas".to_string(), "manager".to_string()]);
    let q_short = node_query_tokens("danas manager");
    assert!(
        node_match_score(&q_short, true, "Dana", "", "").is_some(),
        "danas should match reef Dana"
    );

    // A different name + topic.
    let q2 = node_query_tokens("kubernetes migration");
    assert!(node_match_score(&q2, true, "Kubernetes", "", "").is_some());
}

#[test]
fn retrieval_does_not_over_or_under_match_names_ending_in_s() {
    // A name ending in "s" matches itself directly (no spurious stemming needed).
    let q = node_query_tokens("the paris office");
    assert!(node_match_score(&q, true, "Paris", "", "").is_some());
    assert!(node_match_score(&node_query_tokens("ping chris"), true, "Chris", "", "").is_some());

    // Stemming only STRIPS a trailing s; it never fabricates a match: "pari" (no
    // trailing s) must NOT match "Paris", and an unrelated query matches nothing.
    assert_eq!(node_match_score(&node_query_tokens("pari"), true, "Paris", "", ""), None);
    assert_eq!(
        node_match_score(&node_query_tokens("quarterly budget"), true, "Kubernetes", "", ""),
        None
    );
}

// ── Extraction v2's closed vocabulary ────────────────────────────────────────

#[test]
fn extraction_v2_vocabulary_is_the_registry_minus_inference_only_and_retired() {
    // The v2 source-atoms schema offers exactly this list as its predicate enum.
    let offered = predicates::extractable_predicates();
    for meta in predicates::registry() {
        let expected = !predicates::is_inference_only(meta.id) && !predicates::is_retired_from_extraction(meta.id);
        assert_eq!(offered.contains(&meta.id), expected, "{}", meta.id);
    }
    for never in [EXPERT_IN, EXPERTISE_OF, RELATED_TO, KNOWS, COLLEAGUE_OF] {
        assert!(!offered.contains(&never), "{never} must never be extracted");
    }
    // Every offered predicate is usable by the inference engine on arbitrary
    // data: it is known and its inverse (if any) is known too.
    for p in &offered {
        assert!(predicates::is_known(p));
        if let Some(inv) = predicates::inverse(p) {
            assert!(predicates::is_known(inv), "{p} -> {inv}");
        }
    }
}

#[test]
fn extraction_v2_edges_compose_like_any_other_stated_edges() {
    // v2 writes `manages` plus its materialized inverse `reports_to`, at the
    // stated-edge confidence (0.9). Skip-level management must still compose
    // for names that share nothing with the original ocean.
    let (oyelaran, mbeki, tanaka) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let edges = vec![
        e(oyelaran, MANAGES, mbeki, 0.9),
        e(mbeki, predicates::inverse(MANAGES).unwrap(), oyelaran, 0.9),
        e(mbeki, MANAGES, tanaka, 0.9),
        e(tanaka, predicates::inverse(MANAGES).unwrap(), mbeki, 0.9),
    ];
    let out = infer(&edges);
    assert!(out.iter().any(|x| x.src == oyelaran && x.dst == tanaka), "skip-level edge from v2-shaped stated edges: {out:?}");
}

// ── Extraction v3: changes keep to the same vocabulary ──────────────────────

#[test]
fn extraction_v3_change_attributes_use_the_closed_vocabulary_and_its_state_keys() {
    use coralswarm_backend::fact_supersession::relation_key;
    use coralswarm_backend::workers::source_atoms::v3::RELATIONAL_ATTRIBUTES;
    // No predicate is added: every relational change attribute is an
    // extractable predicate that supersession already keys.
    let offered = predicates::extractable_predicates();
    let mut single_valued = Vec::new();
    for attribute in RELATIONAL_ATTRIBUTES {
        assert!(offered.contains(&attribute.predicate), "{}", attribute.name);
        let key = relation_key(attribute.predicate).unwrap_or_else(|| panic!("{} is keyed", attribute.predicate));
        if key.functional {
            single_valued.push(key.key);
        }
    }
    // The four single-valued relationships the plan names, each reachable.
    single_valued.sort();
    assert_eq!(single_valued, ["rel.employer", "rel.lead", "rel.manager", "rel.owner"]);
}
