//! Canonical relationship-predicate registry for the entity knowledge graph.
//!
//! Reefs (entity nodes) are connected by typed relationship **edges**, each
//! carrying a `predicate` string (e.g. `spouse_of`, `parent_of`). This module
//! is the single source of truth for:
//!
//!   1. The canonical set of predicates we recognise.
//!   2. Per-predicate metadata: inverse, symmetry, and domain.
//!   3. Composition rules used by the inference engine to *derive* new edges
//!      from existing ones (`a(X,Y) ∧ b(Y,Z) ⇒ yields(X,Z)`).
//!
//! ## Materializing inverse / symmetric edges
//!
//! Storage is directional: an edge is `(src, predicate, dst)`. Callers that
//! want the graph to be queryable from both endpoints should, for every stored
//! edge `(src, p, dst)`:
//!
//!   * If `is_symmetric(p)` — also store `(dst, p, src)` (same predicate).
//!   * Else if `let Some(inv) = inverse(p)` — also store `(dst, inv, src)`.
//!
//! `derive_inverse_edge` is provided as the convenience entry point for that
//! second case. Materialization is the caller's responsibility; this module is
//! pure data + lookups and performs no I/O.
//!
//! The only external dependency is `serde` (derive), so this module can be
//! compiled and tested in isolation.

use serde::{Deserialize, Serialize};

/// Broad classification of a predicate, used for grouping and policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Domain {
    /// Family / blood / marriage relationships.
    Kinship,
    /// Work, org-chart, and company relationships.
    Professional,
    /// Generic social ties.
    Social,
    /// Person ↔ topic/project ties (involvement, expertise) and generic
    /// entity-to-entity association. These connect people to the subjects,
    /// deliverables, and projects they work on — not other people.
    Topical,
    /// Associations derived from evidence shape, never stated by a source
    /// (`co_occurs_with`). Readers of stated relationships exclude them.
    Derived,
}

// ---------------------------------------------------------------------------
// Canonical predicate id constants
// ---------------------------------------------------------------------------
//
// Use these constants instead of bare string literals at call sites so that
// typos are caught at compile time.

// Kinship
pub const SPOUSE_OF: &str = "spouse_of";
pub const PARENT_OF: &str = "parent_of";
pub const CHILD_OF: &str = "child_of";
pub const SIBLING_OF: &str = "sibling_of";
pub const COUSIN_OF: &str = "cousin_of";
pub const GRANDPARENT_OF: &str = "grandparent_of";
pub const GRANDCHILD_OF: &str = "grandchild_of";
pub const AUNT_UNCLE_OF: &str = "aunt_uncle_of";
pub const NIECE_NEPHEW_OF: &str = "niece_nephew_of";
pub const PARENT_IN_LAW_OF: &str = "parent_in_law_of";
pub const SIBLING_IN_LAW_OF: &str = "sibling_in_law_of";

// Professional
pub const MANAGES: &str = "manages";
pub const REPORTS_TO: &str = "reports_to";
pub const COLLEAGUE_OF: &str = "colleague_of";
pub const FOUNDER_OF: &str = "founder_of";
pub const WORKS_AT: &str = "works_at";
pub const MENTORS: &str = "mentors";
pub const MENTORED_BY: &str = "mentored_by";
pub const ADVISES: &str = "advises";
pub const ADVISED_BY: &str = "advised_by";
pub const BUSINESS_PARTNER_OF: &str = "business_partner_of";
pub const INVESTOR_IN: &str = "investor_in";
pub const MEMBER_OF: &str = "member_of";
pub const LEADS_TEAM: &str = "leads_team";
pub const PEER_OF: &str = "peer_of";

// Social / generic
pub const FRIEND_OF: &str = "friend_of";
pub const KNOWS: &str = "knows";

// Topical (person ↔ topic/project + generic association)
//
// Direction convention for the directional ones: subject = person,
// object = topic/project (the extraction prompt enforces this). The expertise
// inference layer relies on it to tell people from topics, since all reefs are
// stored with node_type "entity" and carry no person/topic flag.
pub const RELATED_TO: &str = "related_to";
pub const WORKS_ON: &str = "works_on";
pub const WORKED_ON_BY: &str = "worked_on_by";
pub const RESPONSIBLE_FOR: &str = "responsible_for";
pub const RESPONSIBILITY_OF: &str = "responsibility_of";
pub const MENTIONS: &str = "mentions";
pub const MENTIONED_BY: &str = "mentioned_by";
// Inference-only (never extracted): the conclusion of the expertise pass.
pub const EXPERT_IN: &str = "expert_in";
pub const EXPERTISE_OF: &str = "expertise_of";
// Derived (never extracted, never walked as a stated current): two reefs named
// in the same statement. Written by `crate::co_occurrence` only.
pub const CO_OCCURS_WITH: &str = "co_occurs_with";

/// Metadata describing a single canonical predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredicateMeta {
    /// The canonical predicate id (matches one of the `*_OF` / verb constants).
    pub id: &'static str,
    /// The directional inverse predicate, if one exists.
    ///
    /// `parent_of` ↔ `child_of`, `manages` ↔ `reports_to`, etc. Symmetric
    /// predicates have `inverse == Some(self.id)` (their own inverse) but
    /// callers should prefer `is_symmetric` to decide how to materialize the
    /// reverse edge.
    pub inverse: Option<&'static str>,
    /// `true` when the relationship reads the same in both directions, i.e.
    /// `p(X,Y) ⇔ p(Y,X)` (`spouse_of`, `sibling_of`, `friend_of`, …).
    pub symmetric: bool,
    /// Broad domain classification.
    pub domain: Domain,
    /// `true` when an edge under this predicate is eligible to be WALKED by
    /// §5.4's currents traversal — one input to the full [`is_traversable_edge`]
    /// predicate, which additionally requires `confidence >=
    /// TRAVERSABLE_MIN_CONFIDENCE` and the stated/confirmed-inferred trust
    /// clause (see that function's doc comment for the full rationale). This
    /// flag alone answers only "is this predicate the KIND of fact currents
    /// traversal should ever follow" — a low-signal predicate is `false`
    /// regardless of how confident or well-reviewed any individual edge under
    /// it is. Set per the design-locked table in
    /// `docs/plans/graph-temporal-memory.md` §5.3: every kinship predicate,
    /// the strong professional-affiliation predicates (`manages`,
    /// `reports_to`, `works_at`, `member_of`, `leads_team`, `founder_of`,
    /// `business_partner_of`), and the strong topical-involvement predicates
    /// (`works_on`, `responsible_for`, `expert_in`) are `true`. Everything
    /// else defaults `false` — including `mentions`/`mentioned_by`/
    /// `related_to`/`knows`/`colleague_of`/`peer_of` (explicitly named
    /// "Never" in §5.3) AND every predicate the table does not name at all
    /// (`mentors`/`mentored_by`, `advises`/`advised_by`, `investor_in`,
    /// `friend_of`, `worked_on_by`, `responsibility_of`, `expertise_of`) —
    /// §5.3's table is deliberately a narrow allow-list, not a narrow
    /// deny-list, so an predicate it is silent on stays untraversable until a
    /// future revision explicitly opts it in. §11 flags `colleague_of` as
    /// "the closest judgement call" in this list, already decided `false`.
    pub traversable: bool,
}

/// The full registry of canonical predicates.
///
/// This is the single authoritative table; every other lookup in this module
/// is derived from it.
static REGISTRY: &[PredicateMeta] = &[
    // ---- Kinship (§5.3: ALL kinship predicates are traversable) -----------
    PredicateMeta {
        id: SPOUSE_OF,
        inverse: Some(SPOUSE_OF),
        symmetric: true,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: PARENT_OF,
        inverse: Some(CHILD_OF),
        symmetric: false,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: CHILD_OF,
        inverse: Some(PARENT_OF),
        symmetric: false,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: SIBLING_OF,
        inverse: Some(SIBLING_OF),
        symmetric: true,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: COUSIN_OF,
        inverse: Some(COUSIN_OF),
        symmetric: true,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: GRANDPARENT_OF,
        inverse: Some(GRANDCHILD_OF),
        symmetric: false,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: GRANDCHILD_OF,
        inverse: Some(GRANDPARENT_OF),
        symmetric: false,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: AUNT_UNCLE_OF,
        inverse: Some(NIECE_NEPHEW_OF),
        symmetric: false,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: NIECE_NEPHEW_OF,
        inverse: Some(AUNT_UNCLE_OF),
        symmetric: false,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        // X is a parent-in-law of Y  ⇔  Y is a sibling/child-in-law of X.
        // We do not model a distinct "child_in_law_of" predicate, so this is
        // left as a non-symmetric predicate with no registered inverse.
        id: PARENT_IN_LAW_OF,
        inverse: None,
        symmetric: false,
        domain: Domain::Kinship,
        traversable: true,
    },
    PredicateMeta {
        id: SIBLING_IN_LAW_OF,
        inverse: Some(SIBLING_IN_LAW_OF),
        symmetric: true,
        domain: Domain::Kinship,
        traversable: true,
    },
    // ---- Professional -------------------------------------------------------
    PredicateMeta {
        id: MANAGES,
        inverse: Some(REPORTS_TO),
        symmetric: false,
        domain: Domain::Professional,
        traversable: true,
    },
    PredicateMeta {
        id: REPORTS_TO,
        inverse: Some(MANAGES),
        symmetric: false,
        domain: Domain::Professional,
        traversable: true,
    },
    PredicateMeta {
        // §5.3 "Never" list — low-signal social/professional tie, discounted
        // in the synthesis prompt and unscored by `infer_expertise`. §11
        // flags this as "the closest judgement call" in the whole traversal
        // design; still decided `false`.
        id: COLLEAGUE_OF,
        inverse: Some(COLLEAGUE_OF),
        symmetric: true,
        domain: Domain::Professional,
        traversable: false,
    },
    PredicateMeta {
        // X is a founder of org Y. There is no canonical inverse predicate
        // ("founded_by" is not in the registry), so leave it `None`.
        id: FOUNDER_OF,
        inverse: None,
        symmetric: false,
        domain: Domain::Professional,
        traversable: true,
    },
    PredicateMeta {
        // X works at org Y. No canonical "employs" inverse in the registry.
        id: WORKS_AT,
        inverse: None,
        symmetric: false,
        domain: Domain::Professional,
        traversable: true,
    },
    PredicateMeta {
        // Not named in §5.3's traversable allow-list — the table is a narrow
        // allow-list, not a deny-list, so an unnamed predicate stays `false`.
        id: MENTORS,
        inverse: Some(MENTORED_BY),
        symmetric: false,
        domain: Domain::Professional,
        traversable: false,
    },
    PredicateMeta {
        id: MENTORED_BY,
        inverse: Some(MENTORS),
        symmetric: false,
        domain: Domain::Professional,
        traversable: false,
    },
    PredicateMeta {
        id: ADVISES,
        inverse: Some(ADVISED_BY),
        symmetric: false,
        domain: Domain::Professional,
        traversable: false,
    },
    PredicateMeta {
        id: ADVISED_BY,
        inverse: Some(ADVISES),
        symmetric: false,
        domain: Domain::Professional,
        traversable: false,
    },
    PredicateMeta {
        // X and Y are business partners (co-owners / joint venture). Reads the
        // same in both directions.
        id: BUSINESS_PARTNER_OF,
        inverse: Some(BUSINESS_PARTNER_OF),
        symmetric: true,
        domain: Domain::Professional,
        traversable: true,
    },
    PredicateMeta {
        // Not named in §5.3's traversable allow-list.
        id: INVESTOR_IN,
        inverse: None,
        symmetric: false,
        domain: Domain::Professional,
        traversable: false,
    },
    PredicateMeta {
        // X is a member of org/team Y. No canonical "has_member" inverse.
        id: MEMBER_OF,
        inverse: None,
        symmetric: false,
        domain: Domain::Professional,
        traversable: true,
    },
    PredicateMeta {
        // X leads team/org Y. No canonical "led_by" inverse in the registry.
        id: LEADS_TEAM,
        inverse: None,
        symmetric: false,
        domain: Domain::Professional,
        traversable: true,
    },
    PredicateMeta {
        // §5.3 "Never" list — same-level social tie, not a fact-carrying edge.
        id: PEER_OF,
        inverse: Some(PEER_OF),
        symmetric: true,
        domain: Domain::Professional,
        traversable: false,
    },
    // ---- Social ---------------------------------------------------------------
    PredicateMeta {
        // Not named in §5.3's traversable allow-list.
        id: FRIEND_OF,
        inverse: Some(FRIEND_OF),
        symmetric: true,
        domain: Domain::Social,
        traversable: false,
    },
    PredicateMeta {
        // §5.3 "Never" list.
        id: KNOWS,
        inverse: Some(KNOWS),
        symmetric: true,
        domain: Domain::Social,
        traversable: false,
    },
    // ---- Topical (person ↔ topic/project + generic association) -----------
    PredicateMeta {
        // §5.3 "Never" list — the generic catch-all is explicitly zero-weighted
        // by `infer_expertise` and provably dead (§6.1); also retired from
        // extraction there, but that is a SEPARATE mechanism from this flag
        // (see `is_traversable_edge`'s doc comment for why the two must not be
        // conflated) — this predicate stays `is_known` for legacy rows.
        id: RELATED_TO,
        inverse: Some(RELATED_TO),
        symmetric: true,
        domain: Domain::Topical,
        traversable: false,
    },
    PredicateMeta {
        id: WORKS_ON,
        inverse: Some(WORKED_ON_BY),
        symmetric: false,
        domain: Domain::Topical,
        traversable: true,
    },
    PredicateMeta {
        // The materialized pure-inverse of `works_on` — not itself named in
        // §5.3's allow-list (traversal is outbound-only per §5.4, and the
        // forward `works_on` edge already carries the signal).
        id: WORKED_ON_BY,
        inverse: Some(WORKS_ON),
        symmetric: false,
        domain: Domain::Topical,
        traversable: false,
    },
    PredicateMeta {
        id: RESPONSIBLE_FOR,
        inverse: Some(RESPONSIBILITY_OF),
        symmetric: false,
        domain: Domain::Topical,
        traversable: true,
    },
    PredicateMeta {
        id: RESPONSIBILITY_OF,
        inverse: Some(RESPONSIBLE_FOR),
        symmetric: false,
        domain: Domain::Topical,
        traversable: false,
    },
    PredicateMeta {
        // §5.3 "Never" list — "a bare `mentions` must never be read as
        // ownership" (§5.1's own calibration note).
        id: MENTIONS,
        inverse: Some(MENTIONED_BY),
        symmetric: false,
        domain: Domain::Topical,
        traversable: false,
    },
    PredicateMeta {
        // §5.3 "Never" list.
        id: MENTIONED_BY,
        inverse: Some(MENTIONS),
        symmetric: false,
        domain: Domain::Topical,
        traversable: false,
    },
    // Inference-only (excluded from the extraction allow-list by
    // `is_inference_only`): the conclusion of the expertise pass.
    PredicateMeta {
        id: EXPERT_IN,
        inverse: Some(EXPERTISE_OF),
        symmetric: false,
        domain: Domain::Topical,
        traversable: true,
    },
    PredicateMeta {
        // The inverse of `expert_in` — not named in §5.3's allow-list;
        // outbound-only traversal never needs it (see `WORKED_ON_BY`).
        id: EXPERTISE_OF,
        inverse: Some(EXPERT_IN),
        symmetric: false,
        domain: Domain::Topical,
        traversable: false,
    },
    // One row per unordered pair (`src < dst`), see `crate::co_occurrence`.
    PredicateMeta {
        id: CO_OCCURS_WITH,
        inverse: Some(CO_OCCURS_WITH),
        symmetric: true,
        domain: Domain::Derived,
        traversable: false,
    },
];

/// Returns the full predicate registry.
pub fn registry() -> &'static [PredicateMeta] {
    REGISTRY
}

/// Returns the canonical comma-separated list of every allowed predicate id,
/// in registry order.
///
/// This is the single source of truth for the "use ONLY these" allow-list that
/// is mirrored in `backend/prompts/extract_relationships_v1.txt`. When the
/// registry changes, update that prompt to match this output (see the comment
/// in the prompt's "Allowed predicates" section).
pub fn allowed_predicate_list() -> String {
    REGISTRY
        .iter()
        .filter(|m| !is_inference_only(m.id) && !is_retired_from_extraction(m.id))
        .map(|m| m.id)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The closed vocabulary an extraction prompt may offer, in registry order:
/// every known predicate that is neither inference-only nor retired from
/// extraction. Source-atoms v2 sends exactly this list as its schema enum.
pub fn extractable_predicates() -> Vec<&'static str> {
    REGISTRY.iter().map(|m| m.id).filter(|id| is_extractable(id)).collect()
}

/// `true` for predicates produced **only** by an inference pass and never
/// extracted from text. These are registered (so they have inverse/symmetry
/// metadata and can be stored/queried like any edge) but are deliberately
/// excluded from `allowed_predicate_list()` so the extraction prompt never
/// invites the LLM to emit them.
pub fn is_inference_only(id: &str) -> bool {
    matches!(id, EXPERT_IN | EXPERTISE_OF) || is_derived(id)
}

/// `true` for a derived association (`co_occurs_with`): evidence that two
/// reefs were named together, not a relationship any source stated. Readers
/// that present edges as facts (Ask synthesis, digests, traversal, review,
/// entity resolution, inference closure) never see these rows; the store's
/// edge listings exclude them.
pub fn is_derived(id: &str) -> bool {
    meta(id).is_some_and(|m| m.domain == Domain::Derived)
}

/// `true` for predicates **retired from extraction** (Workstream E §6.1 of
/// docs/plans/graph-temporal-memory.md): `related_to` is zero-weighted by
/// explicit decision in `infer_expertise::predicate_weight`, excluded from
/// §5.3 traversal, and discounted in the synthesis prompt — provably dead.
/// `knows`/`colleague_of` are read by nothing at all. All three are symmetric,
/// so every extracted fact under them costs two rows for zero downstream
/// value; on the measured Chris Marchetti reef this removed 11 of 17 edges.
///
/// Unlike `is_inference_only`, these predicates are NOT the conclusion of
/// some other pass — they simply stop being written. They stay in the
/// registry (kept `is_known`) because rows extracted before this change
/// remain in the database and are still read by legacy code that renders or
/// classifies edges by predicate (`routes::atlas::is_hidden_atlas_predicate`,
/// `routes::queries::humanize_predicate`) — dropping the constant would break
/// those call sites for no reason. No data migration accompanies this: existing
/// `related_to`/`knows`/`colleague_of` rows (and the pure-inverse rows for
/// `mentions`/`works_on`/`responsible_for` written by the retired per-atom
/// relationship worker) are simply never read by anything that
/// matters (expertise, traversal, synthesis) and are left in place. Deleting
/// them is a separate, deliberately NOT-taken destructive call — flag it to
/// the founder before ever writing that migration.
pub fn is_retired_from_extraction(id: &str) -> bool {
    matches!(id, RELATED_TO | KNOWS | COLLEAGUE_OF)
}

/// `true` if the predicate may be emitted by the extraction LLM, i.e. it is a
/// known predicate that is neither inference-only nor retired from extraction.
pub fn is_extractable(id: &str) -> bool {
    is_known(id) && !is_inference_only(id) && !is_retired_from_extraction(id)
}

/// Looks up the metadata for a predicate id, if it is known.
pub fn meta(id: &str) -> Option<&'static PredicateMeta> {
    REGISTRY.iter().find(|m| m.id == id)
}

/// Returns the canonical inverse of a predicate, if one is registered.
///
/// For symmetric predicates this returns the predicate itself (e.g.
/// `inverse("spouse_of") == Some("spouse_of")`).
pub fn inverse(id: &str) -> Option<&'static str> {
    meta(id).and_then(|m| m.inverse)
}

/// Alias of [`inverse`] expressing caller intent: "what predicate should the
/// reverse edge carry?" See the module docs for materialization guidance.
pub fn derive_inverse_edge(predicate: &str) -> Option<&'static str> {
    inverse(predicate)
}

/// Returns `true` if the predicate reads the same in both directions.
pub fn is_symmetric(id: &str) -> bool {
    meta(id).map(|m| m.symmetric).unwrap_or(false)
}

/// Returns `true` if the predicate id is part of the canonical registry.
pub fn is_known(id: &str) -> bool {
    meta(id).is_some()
}

/// `true` when one endpoint holds at most one value at a time under this
/// predicate: an employee has one manager (`manages` / `reports_to`), a
/// thing has one owner (`responsible_for` / `responsibility_of`), a team has
/// one lead (`leads_team`), a person has one employer (`works_at`). Only
/// these can be replaced by a later value, so fact supersession compares
/// relation facts under these alone (`fact_supersession::relation_key`).
/// Everything else is many-valued: working on a second project, joining a
/// second team or mentioning a second topic never ends the first.
pub fn is_single_valued(id: &str) -> bool {
    matches!(id, MANAGES | REPORTS_TO | RESPONSIBLE_FOR | RESPONSIBILITY_OF | LEADS_TEAM | WORKS_AT)
}

/// `true` for a FORWARD predicate whose materialized inverse is a pure
/// direction-flip that nothing downstream ever reads (Workstream E §6.1):
/// `mentioned_by`, `worked_on_by`, `responsibility_of` are the inverses of
/// `mentions`/`works_on`/`responsible_for`, and `infer_expertise` reads only
/// the forward predicates (see its module doc — "we only look at the FORWARD
/// predicates ... their materialized inverses carry no new information").
///
/// `expert_in` joins this set for the same reason: nothing reads
/// `expertise_of` (its outbound-only traversal — see `EXPERTISE_OF`'s own
/// `traversable: false` above — never needs the reverse edge), so
/// materializing it only doubled the review queue for zero downstream value.
/// Unlike the extraction-worker trio, `infer_expertise` is the ONLY producer
/// of `expert_in` edges (it is `is_inference_only`), so this flag is honored
/// by that worker directly rather than by an extraction writer.
///
/// This is deliberately a DIFFERENT mechanism from
/// [`is_retired_from_extraction`]: `mentions`/`works_on`/`responsible_for`/
/// `expert_in` stay fully extractable/inferrable (this only silences the
/// automatic reverse-edge write the producing worker performs for every
/// non-symmetric triple), and their inverse predicates
/// (`mentioned_by`/`worked_on_by`/`responsibility_of`/`expertise_of`) stay in
/// the registry and stay valid in principle — the producing worker never
/// actually needs to emit them directly, so in practice this only suppresses
/// the worker-generated reverse row.
pub fn skip_inverse_materialization(id: &str) -> bool {
    matches!(id, MENTIONS | WORKS_ON | RESPONSIBLE_FOR | EXPERT_IN)
}

/// Predicates Ask walks in both directions at read time.
///
/// [`skip_inverse_materialization`] refuses to write the reverse row for
/// `works_on` / `responsible_for` / `expert_in`, so an outbound-only walk
/// cannot leave the object reef (`CodeRabbit` has no current out). Ask
/// treats the traversable members of that set as undirected: it does not
/// persist the inverse, and it still requires [`super::is_traversable_edge`].
/// `mentions` is in the skip set but is never traversable, so it stays out.
pub fn is_undirected_for_ask(id: &str) -> bool {
    skip_inverse_materialization(id) && is_traversable_predicate(id)
}

/// `true` when `id` is a KNOWN predicate whose [`PredicateMeta::traversable`]
/// flag is set (§5.3) — i.e. the predicate is the kind of fact §5.4's
/// currents traversal is willing to walk AT ALL. An unknown predicate id is
/// `false` (fails closed, matching [`is_known`]'s own unknown-id behavior).
///
/// This is necessary but not SUFFICIENT for a given edge to be traversable —
/// see [`super::is_traversable_edge`] for the full predicate (this flag AND
/// `confidence >= `[`TRAVERSABLE_MIN_CONFIDENCE`]` AND` the stated/confirmed-
/// inferred trust clause), which is what traversal actually calls.
pub fn is_traversable_predicate(id: &str) -> bool {
    meta(id).is_some_and(|m| m.traversable)
}

/// Confidence floor for a traversable edge (§5.3, §10's tuning table — an
/// ESTIMATE with no evidence yet, gated behind `GRAPH_TRAVERSAL_ENABLED`
/// shadow mode until real deltas justify it).
pub const TRAVERSABLE_MIN_CONFIDENCE: f32 = 0.70;

/// A composition (chaining) rule for the inference engine.
///
/// Semantics: `a(X,Y) ∧ b(Y,Z) ⇒ yields(X,Z)`. The shared middle node `Y` is
/// the object of `a` and the subject of `b`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CompositionRule {
    /// First edge predicate, `a(X,Y)`.
    pub a: &'static str,
    /// Second edge predicate, `b(Y,Z)`.
    pub b: &'static str,
    /// Predicate of the derived edge, `yields(X,Z)`.
    pub yields: &'static str,
    /// Prior confidence in the derived edge before any LLM/evidence review.
    pub base_confidence: f32,
    /// `true` when the conclusion is defeasible — it can be overturned by
    /// further evidence (step-relations, divorce, adoption, prior marriages).
    pub defeasible: bool,
    /// Human-readable rationale / caveat for the rule.
    pub note: &'static str,
}

/// The set of composition rules driving automatic edge inference.
static COMPOSITIONS: &[CompositionRule] = &[
    // === Headline rule =====================================================
    // spouse_of(X,Y) ∧ parent_of(Y,Z) ⇒ parent_of(X,Z)
    // "the spouse of someone whose child is Z is *likely* a parent of Z" —
    // covers step-parents and social parents, hence defeasible + modest prior.
    CompositionRule {
        a: SPOUSE_OF,
        b: PARENT_OF,
        yields: PARENT_OF,
        base_confidence: 0.6,
        defeasible: true,
        note: "social/step-parent; spouse of a parent is a likely but not certain parent",
    },
    // === Strong kinship closures ==========================================
    // parent_of(X,Y) ∧ parent_of(Y,Z) ⇒ grandparent_of(X,Z)
    CompositionRule {
        a: PARENT_OF,
        b: PARENT_OF,
        yields: GRANDPARENT_OF,
        base_confidence: 0.95,
        defeasible: false,
        note: "parent of a parent is a grandparent",
    },
    // parent_of(X,Y) ∧ sibling_of(Y,Z) ⇒ parent_of(X,Z)
    // A parent of someone's sibling is that someone's parent.
    CompositionRule {
        a: PARENT_OF,
        b: SIBLING_OF,
        yields: PARENT_OF,
        base_confidence: 0.9,
        defeasible: false,
        note: "parent of one's sibling is also one's parent (modulo half/step siblings)",
    },
    // sibling_of(X,Y) ∧ parent_of(Y,Z) ⇒ aunt_uncle_of(X,Z)
    CompositionRule {
        a: SIBLING_OF,
        b: PARENT_OF,
        yields: AUNT_UNCLE_OF,
        base_confidence: 0.85,
        defeasible: false,
        note: "sibling of a parent is an aunt/uncle",
    },
    // child_of(X,Y) ∧ sibling_of(Y,Z) ⇒ niece_nephew_of(X,Z)
    // X is Y's child and Z is Y's sibling, so Z is X's parent's sibling — i.e. X's
    // aunt/uncle — and X is therefore Z's niece/nephew, NOT Z's child. (Inverse-
    // form companion to sibling_of∘parent_of⇒aunt_uncle_of. Previously mis-yielded
    // child_of, which falsely made a parent's sibling a parent — the Jordan/Casey
    // conflation. Confidence matches the aunt/uncle rule, 0.85.)
    CompositionRule {
        a: CHILD_OF,
        b: SIBLING_OF,
        yields: NIECE_NEPHEW_OF,
        base_confidence: 0.85,
        defeasible: false,
        note: "child of someone whose sibling is Z is Z's niece/nephew (a parent's sibling is an aunt/uncle, not a parent)",
    },
    // child_of(X,Y) ∧ child_of(Y,Z) ⇒ grandchild_of(X,Z)
    CompositionRule {
        a: CHILD_OF,
        b: CHILD_OF,
        yields: GRANDCHILD_OF,
        base_confidence: 0.95,
        defeasible: false,
        note: "child of a child is a grandchild (inverse-form of parent∘parent)",
    },
    // sibling_of(X,Y) ∧ child_of(Y,Z) ⇒ child_of(X,Z)
    // X is Y's sibling and Y is Z's child, so Z is Y's parent and therefore X's
    // parent too — X is Z's child. (Inverse-form companion to
    // parent_of∘sibling_of⇒parent_of. Previously mis-yielded niece_nephew_of;
    // confidence matches the parent rule, 0.9.)
    CompositionRule {
        a: SIBLING_OF,
        b: CHILD_OF,
        yields: CHILD_OF,
        base_confidence: 0.9,
        defeasible: false,
        note: "sibling of someone who is Z's child is also Z's child (shared parents)",
    },
    // sibling_of(X,Y) ∧ spouse_of(Y,Z) ⇒ sibling_in_law_of(X,Z)
    CompositionRule {
        a: SIBLING_OF,
        b: SPOUSE_OF,
        yields: SIBLING_IN_LAW_OF,
        base_confidence: 0.9,
        defeasible: false,
        note: "sibling of a spouse is a sibling-in-law",
    },
    // parent_of(X,Y) ∧ spouse_of(Y,Z) ⇒ parent_in_law_of(X,Z)
    CompositionRule {
        a: PARENT_OF,
        b: SPOUSE_OF,
        yields: PARENT_IN_LAW_OF,
        base_confidence: 0.9,
        defeasible: false,
        note: "parent of a spouse is a parent-in-law",
    },
    // === Professional ======================================================
    // reports_to(X,Y) ∧ reports_to(Y,Z) ⇒ reports_to(X,Z)  (skip-level)
    CompositionRule {
        a: REPORTS_TO,
        b: REPORTS_TO,
        yields: REPORTS_TO,
        base_confidence: 0.7,
        defeasible: true,
        note: "skip-level: reporting chains compose transitively but matrix orgs defeat it",
    },
    // manages(X,Y) ∧ manages(Y,Z) ⇒ manages(X,Z)  (skip-level, inverse-form)
    CompositionRule {
        a: MANAGES,
        b: MANAGES,
        yields: MANAGES,
        base_confidence: 0.7,
        defeasible: true,
        note: "skip-level management; inverse-form companion to reports_to∘reports_to",
    },
];

/// Returns the set of composition rules.
pub fn compositions() -> &'static [CompositionRule] {
    COMPOSITIONS
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headline_rule_spouse_then_parent_yields_defeasible_parent() {
        let rule = compositions()
            .iter()
            .find(|r| r.a == SPOUSE_OF && r.b == PARENT_OF)
            .expect("headline spouse_of ∘ parent_of rule must exist");
        assert_eq!(rule.yields, PARENT_OF);
        assert!(rule.defeasible, "spouse-of-a-parent inference must be defeasible");
        assert!(rule.base_confidence > 0.0 && rule.base_confidence < 1.0);
    }

    #[test]
    fn parent_and_child_are_mutual_inverses() {
        assert_eq!(inverse(PARENT_OF), Some(CHILD_OF));
        assert_eq!(inverse(CHILD_OF), Some(PARENT_OF));
        assert_eq!(derive_inverse_edge(PARENT_OF), Some(CHILD_OF));
    }

    #[test]
    fn manages_and_reports_to_are_mutual_inverses() {
        assert_eq!(inverse(MANAGES), Some(REPORTS_TO));
        assert_eq!(inverse(REPORTS_TO), Some(MANAGES));
    }

    #[test]
    fn grandparent_and_aunt_uncle_inverses() {
        assert_eq!(inverse(GRANDPARENT_OF), Some(GRANDCHILD_OF));
        assert_eq!(inverse(GRANDCHILD_OF), Some(GRANDPARENT_OF));
        assert_eq!(inverse(AUNT_UNCLE_OF), Some(NIECE_NEPHEW_OF));
        assert_eq!(inverse(NIECE_NEPHEW_OF), Some(AUNT_UNCLE_OF));
    }

    #[test]
    fn symmetric_predicates_are_flagged() {
        for p in [SPOUSE_OF, SIBLING_OF, COUSIN_OF, COLLEAGUE_OF, FRIEND_OF, KNOWS] {
            assert!(is_symmetric(p), "{p} should be symmetric");
            assert_eq!(inverse(p), Some(p), "symmetric {p} must be its own inverse");
        }
    }

    #[test]
    fn directional_predicates_are_not_symmetric() {
        for p in [PARENT_OF, CHILD_OF, MANAGES, REPORTS_TO, GRANDPARENT_OF] {
            assert!(!is_symmetric(p), "{p} should be directional");
        }
    }

    #[test]
    fn is_known_accepts_registry_rejects_garbage() {
        assert!(is_known(PARENT_OF));
        assert!(is_known(WORKS_AT));
        assert!(!is_known("married_to"));
        assert!(!is_known(""));
        assert!(!is_known("PARENT_OF")); // case-sensitive
        assert!(!is_known("definitely_not_a_predicate"));
    }

    #[test]
    fn every_predicate_id_is_unique() {
        let ids: Vec<&str> = registry().iter().map(|m| m.id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(ids.len(), sorted.len(), "duplicate predicate id in registry");
    }

    #[test]
    fn every_inverse_is_known_and_mutually_consistent() {
        for m in registry() {
            if let Some(inv) = m.inverse {
                assert!(is_known(inv), "inverse {inv} of {} is not registered", m.id);
                assert_eq!(
                    inverse(inv),
                    Some(m.id),
                    "inverse of {} is {inv}, but inverse({inv}) != {}",
                    m.id,
                    m.id
                );
                assert_eq!(
                    m.symmetric,
                    inv == m.id,
                    "symmetry flag for {} disagrees with its inverse",
                    m.id
                );
            } else {
                assert!(!m.symmetric, "{} is symmetric but has no inverse", m.id);
            }
        }
    }

    #[test]
    fn every_composition_references_only_known_predicates() {
        for r in compositions() {
            assert!(is_known(r.a), "composition `a` {} not in registry", r.a);
            assert!(is_known(r.b), "composition `b` {} not in registry", r.b);
            assert!(
                is_known(r.yields),
                "composition `yields` {} not in registry",
                r.yields
            );
            assert!(
                r.base_confidence >= 0.0 && r.base_confidence <= 1.0,
                "base_confidence for {}∘{} out of range",
                r.a,
                r.b
            );
            assert!(!r.note.is_empty(), "composition {}∘{} missing note", r.a, r.b);
        }
    }

    #[test]
    fn meta_round_trips() {
        let m = meta(SIBLING_OF).expect("sibling_of registered");
        assert_eq!(m.id, SIBLING_OF);
        assert_eq!(m.domain, Domain::Kinship);
        assert!(m.symmetric);
        assert!(meta("nonsense").is_none());
    }

    #[test]
    fn domains_are_assigned_sensibly() {
        assert_eq!(meta(SPOUSE_OF).unwrap().domain, Domain::Kinship);
        assert_eq!(meta(MANAGES).unwrap().domain, Domain::Professional);
        assert_eq!(meta(FRIEND_OF).unwrap().domain, Domain::Social);
    }

    #[test]
    fn back_compat_predicates_all_still_known() {
        // These predicates predate the business/org expansion; callers in other
        // modules rely on every one of them remaining registered.
        for p in [
            SPOUSE_OF,
            PARENT_OF,
            CHILD_OF,
            SIBLING_OF,
            COUSIN_OF,
            GRANDPARENT_OF,
            GRANDCHILD_OF,
            AUNT_UNCLE_OF,
            NIECE_NEPHEW_OF,
            PARENT_IN_LAW_OF,
            SIBLING_IN_LAW_OF,
            MANAGES,
            REPORTS_TO,
            COLLEAGUE_OF,
            FOUNDER_OF,
            WORKS_AT,
            FRIEND_OF,
            KNOWS,
        ] {
            assert!(is_known(p), "back-compat predicate {p} must remain registered");
        }
    }

    #[test]
    fn business_org_predicates_are_registered() {
        for p in [
            MENTORS,
            MENTORED_BY,
            ADVISES,
            ADVISED_BY,
            BUSINESS_PARTNER_OF,
            INVESTOR_IN,
            MEMBER_OF,
            LEADS_TEAM,
            PEER_OF,
        ] {
            assert!(is_known(p), "{p} should be registered");
            assert_eq!(
                meta(p).unwrap().domain,
                Domain::Professional,
                "{p} should be Professional"
            );
        }
        // New symmetric ones.
        for p in [BUSINESS_PARTNER_OF, PEER_OF] {
            assert!(is_symmetric(p), "{p} should be symmetric");
        }
        // New directional inverse pairs.
        assert_eq!(inverse(MENTORS), Some(MENTORED_BY));
        assert_eq!(inverse(MENTORED_BY), Some(MENTORS));
        assert_eq!(inverse(ADVISES), Some(ADVISED_BY));
        assert_eq!(inverse(ADVISED_BY), Some(ADVISES));
    }

    #[test]
    fn allowed_predicate_list_covers_every_extractable_predicate() {
        let list = allowed_predicate_list();
        for m in registry() {
            let present = list.split(", ").any(|p| p == m.id);
            if is_inference_only(m.id) {
                assert!(
                    !present,
                    "inference-only {} must NOT appear in allowed_predicate_list()",
                    m.id
                );
            } else if is_retired_from_extraction(m.id) {
                assert!(
                    !present,
                    "retired-from-extraction {} must NOT appear in allowed_predicate_list() (§6.1)",
                    m.id
                );
            } else {
                assert!(present, "allowed_predicate_list() missing {}", m.id);
            }
        }
        // One entry per extractable predicate, no stray separators.
        let extractable = registry()
            .iter()
            .filter(|m| !is_inference_only(m.id) && !is_retired_from_extraction(m.id))
            .count();
        assert_eq!(list.split(", ").count(), extractable);
        assert!(extractable < registry().len(), "expected some inference-only predicates");
    }

    #[test]
    fn extract_relationships_prompt_allowed_list_matches_the_registry() {
        // The prompt's "Allowed predicates" line is hand-maintained (its own
        // comment says to keep it in sync); this test is the enforcement the
        // comment alone can't provide. Catches exactly the drift §6.1 removed:
        // `related_to`/`knows`/`colleague_of` dropped from both, together.
        let prompt = include_str!("../../prompts/extract_relationships_v1.txt");
        let marker = "-->\n\n";
        let start = prompt.find(marker).expect("allow-list comment block present") + marker.len();
        let line = prompt[start..].lines().next().expect("allow-list line present");
        assert_eq!(
            line.trim(),
            allowed_predicate_list(),
            "extract_relationships_v1.txt's allowed-predicates line has drifted from allowed_predicate_list()"
        );
    }

    #[test]
    fn retired_predicates_stay_known_but_not_extractable() {
        // §6.1: `related_to`/`knows`/`colleague_of` are dropped from the
        // extraction allow-list, but stay in the registry (legacy rows +
        // `routes::atlas`/`routes::queries` still read them by predicate id).
        for p in [RELATED_TO, KNOWS, COLLEAGUE_OF] {
            assert!(is_retired_from_extraction(p), "{p} should be retired from extraction");
            assert!(is_known(p), "{p} must remain a known predicate (legacy rows)");
            assert!(!is_extractable(p), "{p} must not be extractable after §6.1");
            assert!(
                !allowed_predicate_list().split(", ").any(|x| x == p),
                "{p} must not appear in the extraction allow-list"
            );
        }
        // Nothing else is accidentally caught by the retirement list.
        for p in [WORKS_ON, RESPONSIBLE_FOR, MENTIONS, SPOUSE_OF, MANAGES] {
            assert!(!is_retired_from_extraction(p), "{p} must NOT be retired");
        }
    }

    #[test]
    fn skip_inverse_materialization_covers_exactly_the_unread_pure_inverses() {
        // §6.1: `mentions`/`works_on`/`responsible_for` remain extractable
        // (expertise scores `mentions` at 1.0 — never drop it), but their
        // materialized inverse row is no longer worth writing.
        for p in [MENTIONS, WORKS_ON, RESPONSIBLE_FOR] {
            assert!(skip_inverse_materialization(p), "{p} should skip inverse materialization");
            assert!(is_extractable(p), "{p} must remain extractable");
        }
        // `expert_in` joins the set for the same reason, but it is
        // inference-only (never extracted), so assert that instead.
        assert!(skip_inverse_materialization(EXPERT_IN), "expert_in should skip inverse materialization");
        assert!(is_inference_only(EXPERT_IN), "expert_in must remain inference-only");
        // The inverse predicates themselves are a different set — this flag
        // is read off the FORWARD triple's predicate, not the inverse's.
        for p in [MENTIONED_BY, WORKED_ON_BY, RESPONSIBILITY_OF, EXPERTISE_OF] {
            assert!(
                !skip_inverse_materialization(p),
                "{p} (an inverse predicate itself) is not what this flag gates on"
            );
            assert!(is_known(p), "{p} must remain registered for legacy rows");
        }
        // Nothing else is caught by this.
        for p in [SPOUSE_OF, MANAGES, RELATED_TO, KNOWS] {
            assert!(!skip_inverse_materialization(p), "{p} must NOT skip inverse materialization");
        }
    }

    #[test]
    fn undirected_for_ask_is_the_traversable_skip_inverse_set() {
        for p in [WORKS_ON, RESPONSIBLE_FOR, EXPERT_IN] {
            assert!(is_undirected_for_ask(p), "{p} must walk both ways in Ask");
        }
        assert!(
            !is_undirected_for_ask(MENTIONS),
            "mentions is skip-inverse but never traversable"
        );
        assert!(
            !is_undirected_for_ask(MANAGES),
            "manages already has a materialized inverse"
        );
    }

    #[test]
    fn topical_predicates_registered_with_correct_metadata() {
        // Generic catch-all is symmetric.
        assert!(is_symmetric(RELATED_TO));
        assert_eq!(inverse(RELATED_TO), Some(RELATED_TO));
        assert_eq!(meta(RELATED_TO).unwrap().domain, Domain::Topical);

        // Directional involvement pairs are mutual inverses, not symmetric.
        for (a, b) in [
            (WORKS_ON, WORKED_ON_BY),
            (RESPONSIBLE_FOR, RESPONSIBILITY_OF),
            (MENTIONS, MENTIONED_BY),
            (EXPERT_IN, EXPERTISE_OF),
        ] {
            assert_eq!(inverse(a), Some(b), "{a} inverse should be {b}");
            assert_eq!(inverse(b), Some(a), "{b} inverse should be {a}");
            assert!(!is_symmetric(a), "{a} should be directional");
            assert!(!is_symmetric(b), "{b} should be directional");
            assert_eq!(meta(a).unwrap().domain, Domain::Topical);
            assert_eq!(meta(b).unwrap().domain, Domain::Topical);
        }
    }

    #[test]
    fn expertise_predicates_are_inference_only_and_extraction_predicates_are_not() {
        assert!(is_inference_only(EXPERT_IN));
        assert!(is_inference_only(EXPERTISE_OF));
        assert!(!is_extractable(EXPERT_IN));
        assert!(!is_extractable(EXPERTISE_OF));
        // The topical extraction predicates ARE extractable — `related_to` is
        // NOT among them post-§6.1 (retired from extraction, see the
        // dedicated `retired_predicates_stay_known_but_not_extractable` test).
        for p in [WORKS_ON, RESPONSIBLE_FOR, MENTIONS] {
            assert!(is_extractable(p), "{p} should be extractable");
        }
        // Unknown predicates are neither.
        assert!(!is_extractable("not_a_predicate"));
        assert!(!is_inference_only("not_a_predicate"));
    }

    // ── §5.3 traversability ────────────────────────────────────────────────

    #[test]
    fn every_kinship_predicate_is_traversable() {
        for m in registry().iter().filter(|m| m.domain == Domain::Kinship) {
            assert!(
                m.traversable,
                "kinship predicate {} must be traversable",
                m.id
            );
            assert!(is_traversable_predicate(m.id));
        }
    }

    #[test]
    fn the_named_professional_and_topical_predicates_are_traversable() {
        for p in [
            MANAGES,
            REPORTS_TO,
            WORKS_AT,
            MEMBER_OF,
            LEADS_TEAM,
            FOUNDER_OF,
            BUSINESS_PARTNER_OF,
            WORKS_ON,
            RESPONSIBLE_FOR,
            EXPERT_IN,
        ] {
            assert!(
                is_traversable_predicate(p),
                "{p} should be traversable per §5.3's table"
            );
        }
    }

    #[test]
    fn co_occurs_with_is_a_derived_association_and_never_a_stated_fact() {
        let m = meta(CO_OCCURS_WITH).expect("registered");
        assert_eq!((m.domain, m.symmetric, m.traversable), (Domain::Derived, true, false));
        assert!(is_derived(CO_OCCURS_WITH) && is_inference_only(CO_OCCURS_WITH));
        assert!(!is_extractable(CO_OCCURS_WITH));
        assert!(!extractable_predicates().contains(&CO_OCCURS_WITH));
        assert_eq!(registry().iter().filter(|m| is_derived(m.id)).count(), 1);
    }

    #[test]
    fn the_never_list_is_non_traversable() {
        // §5.3's explicit "Never" row.
        for p in [MENTIONS, MENTIONED_BY, RELATED_TO, KNOWS, COLLEAGUE_OF, PEER_OF] {
            assert!(
                !is_traversable_predicate(p),
                "{p} must be non-traversable (§5.3 Never list)"
            );
        }
    }

    #[test]
    fn related_to_knows_and_colleague_of_are_non_traversable_even_though_still_known() {
        // These three are the predicates §6.1 retires from EXTRACTION
        // (`is_extractable` == false there), but that is a DIFFERENT axis
        // from traversability — a legacy row under any of them can still
        // exist and must never be walked, regardless of whatever extraction
        // policy governs whether NEW rows get written. Assert both axes
        // explicitly so they can never be conflated.
        for p in [RELATED_TO, KNOWS, COLLEAGUE_OF] {
            assert!(
                is_known(p),
                "{p} must remain a known predicate (legacy rows)"
            );
            assert!(!is_traversable_predicate(p), "{p} must be non-traversable");
        }
    }

    #[test]
    fn predicates_not_named_in_either_5_3_list_default_to_non_traversable() {
        // §5.3's table is a narrow ALLOW-list, not a deny-list — anything it
        // doesn't name (including some predicates that read as plausibly
        // "professional") stays untraversable by default.
        for p in [
            MENTORS,
            MENTORED_BY,
            ADVISES,
            ADVISED_BY,
            INVESTOR_IN,
            FRIEND_OF,
            WORKED_ON_BY,
            RESPONSIBILITY_OF,
            EXPERTISE_OF,
        ] {
            assert!(
                !is_traversable_predicate(p),
                "{p} was not named traversable in §5.3"
            );
        }
    }

    #[test]
    fn is_traversable_predicate_is_false_for_unknown_ids() {
        assert!(!is_traversable_predicate("not_a_predicate"));
        assert!(!is_traversable_predicate(""));
    }

    #[test]
    fn traversable_min_confidence_matches_the_spec() {
        assert_eq!(TRAVERSABLE_MIN_CONFIDENCE, 0.70);
    }
}
