//! Ingest-door Jev judgments (ADR-0013).
//!
//! Cheap extract proposes mentions, non-catalog triples, and claims. Jev
//! judges identity and claims, and scores catalog relationship slots
//! (`works_on` / `manages` / `member_of`). Code composes binds, mints, edge
//! status, and supersession kind. The worker owns I/O; this module is the
//! typed request/compose seam so shadow can retune **level wording** without
//! touching write policy.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::{
    jev_model_for, Answer, JevOp, DecisionError, DecisionProvider, DecisionRequest, DecisionResponse, Question,
};

/// TypeSafe Choice option cap is 255. Pass 1 uses 254 ids + `none`.
pub const PASS1_CHUNK_IDS: usize = 254;
pub const NONE: &str = "none";
pub const NEW: &str = "new";
pub const CURATOR: &str = "curator";

pub const ENTITY_TYPES: [&str; 4] = ["person", "org", "topic", "project"];

/// Author×other catalog predicates (ADR-0013 pass 3). Jev Scores titled
/// slots; extract does not propose these. Not a binary Noul.
pub const EXTRA_PREDICATES: [&str; 3] = ["works_on", "manages", "member_of"];

pub const HOLD_SCORE_CRITERIA: [&str; 3] = [
    "Drop: the coral does not support this, an endpoint is wrong, or it is not a real relationship or claim.",
    "Suggested: the coral supports this, but it should wait for confirmation before treating it as settled.",
    "Confirmed: the coral clearly states this as a current relationship or claim.",
];

pub const SUPERSESSION_SCORE_CRITERIA: [&str; 4] = [
    "Unrelated: the two claims are about different things.",
    "Same-fold: they are the same decision; attach the new coral, do not mint another claim reef.",
    "Supersede: the later claim replaces the earlier one.",
    "Curator: a human should decide; do not auto-write.",
];

pub const PASS1_INSTRUCTIONS: &str = "\
Does this mention refer to one of the catalog reefs in this chunk? \
Pick that reef's id. Pick none if it is not in this chunk. \
Ids and titles live in state.catalog and state.mentions.";

pub const PASS2_ID_INSTRUCTIONS: &str = "\
Which catalog reef is this mention? Pick a survivor id if it is the same \
real-world thing, new if nothing in the survivors is this mention, or \
curator if identity is ambiguous and a human should decide. \
Mutually exclusive — pick exactly one. \
Same title string is not identity: a fragment, name-order permutation, or \
zero-coral ghost can share a title with a different survivor. \
`mentions[].door_hit` is a retrieval hint (title, alias, permutation, or \
identifier), not the bind. Judge from `cards` (aliases, identifiers, \
coral_count, digest, currents). Prefer the established same-entity survivor \
over a same-string fragment or permutation. Pick new when no survivor is \
this mention; sharing a word, nickname, or related topic is not enough. \
Judge this mention's `name` or `surface_form`, not whether a survivor is \
about the same topic as `coral`. \
Pick curator only when two survivors could both be this mention.";

pub const PASS2_TYPE_INSTRUCTIONS: &str = "\
What kind of entity is this mention? person, org, topic, or project.";

pub const HOLD_SCORE_INSTRUCTIONS: &str = "\
Should this proposed relationship or claim be dropped, written as Suggested, \
or written as Confirmed? The proposal is the candidate; evidence comes only \
from state.coral. Keep the verbatim evidence span; do not invent endpoints. \
Do not treat the proposal's endpoint names as coral support. \
Confirmed when the coral states this as a current fact: first-person shipping \
or doing work on a named project is works_on; someone reporting to the author \
is manages; a first-person current membership statement is member_of. \
Do not Drop a stated current edge because the coral used different \
verbs than the predicate label. Do not write Suggested for a settled current statement. \
Suggested only when the coral supports this but hedges it (might, considering, \
later). Drop when either named endpoint does not appear in the coral, or the \
coral never states that predicate between them. Silent is Drop, not Suggested. \
An extract proposal or a bound reef is not evidence. Sharing leftover ops, a vendor \
name, or a related topic is not enough. Do not write Suggested for an unsupported invention. \
Binds record identity, not a relationship. Do not raise HoldLevel because a reef is bound.";

/// Pass-4 claim Hold. Pass 3 catalog/extra Scores keep
/// [`HOLD_SCORE_INSTRUCTIONS`]. Same [`HOLD_SCORE_CRITERIA`].
pub const CLAIM_HOLD_SCORE_INSTRUCTIONS: &str = "\
Should this proposed claim be dropped, written as Suggested, or written as \
Confirmed? The proposal is the candidate statement in state.claims; evidence \
comes only from state.coral. Confirmed when the coral clearly asserts this as \
a current decision, rule, standing state, completed action, or factual claim \
— including the plan is now, X is Y now, a standing flag or policy, or a \
family fact. Do not write Suggested for a settled current statement. \
Suggested only when the coral supports this same claim but hedges it (might, \
considering, later). Drop when the coral does not support this statement; \
when the proposal states as present fact something the coral says is \
undecided or only considering; or when the proposal is a question, TODO, or \
task rather than a durable claim. Silent is Drop, not Suggested. Do not write \
Suggested for an unsupported invention. A name in leftover ops, a sidebar, \
or the coral is not this statement — Drop unless the coral actually asserts \
it. An extract proposal or a bound reef is not evidence. This Score is for \
claims, not relationships; do not treat works_on, manages, or member_of as \
the question.";

pub const SUPERSESSION_SCORE_INSTRUCTIONS: &str = "\
How do these two claims relate? unrelated, same-fold (same decision, attach \
coral, no mint), supersede (later decided_at replaces earlier), or curator.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldLevel {
    Drop = 0,
    Suggested = 1,
    Confirmed = 2,
}

impl HoldLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Suggested => "Suggested",
            Self::Confirmed => "Confirmed",
        }
    }

    pub fn from_index(i: usize) -> Self {
        match i {
            1 => Self::Suggested,
            2 => Self::Confirmed,
            _ => Self::Drop,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupersessionLevel {
    Unrelated = 0,
    SameFold = 1,
    Supersede = 2,
    Curator = 3,
}

impl SupersessionLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unrelated => "unrelated",
            Self::SameFold => "same-fold",
            Self::Supersede => "supersede",
            Self::Curator => "curator",
        }
    }

    pub fn from_index(i: usize) -> Self {
        match i {
            1 => Self::SameFold,
            2 => Self::Supersede,
            3 => Self::Curator,
            _ => Self::Unrelated,
        }
    }
}

/// Round a Score to the nearest level. Tune wording in shadow, not cutoffs.
pub fn nearest_level(score: f64, n_levels: usize) -> usize {
    let max = n_levels.saturating_sub(1) as f64;
    score.round().clamp(0.0, max) as usize
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DoorHow {
    Author,
    Identifier,
    Title,
    Alias,
    Permutation,
}

impl DoorHow {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Author => "author",
            Self::Identifier => "identifier",
            Self::Title => "title",
            Self::Alias => "alias",
            Self::Permutation => "permutation",
        }
    }

    pub fn from_resolve_how(how: crate::store::ResolveHow) -> Self {
        match how {
            crate::store::ResolveHow::Author => Self::Author,
            crate::store::ResolveHow::Identifier => Self::Identifier,
            crate::store::ResolveHow::Title => Self::Title,
            crate::store::ResolveHow::Alias => Self::Alias,
            crate::store::ResolveHow::Permutation => Self::Permutation,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DoorHit {
    pub reef_id: Uuid,
    pub how: DoorHow,
}

#[derive(Debug, Clone, Serialize)]
pub struct SkinnyCard {
    pub id: Uuid,
    pub title: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestDigestCard {
    pub id: Uuid,
    pub title: String,
    pub entity_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identifiers: Vec<String>,
    pub coral_count: i64,
    pub degree: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<CachedDigest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub currents: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CachedDigest {
    pub summary: String,
    pub excerpts: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MentionProposal {
    pub index: usize,
    pub name: String,
    pub surface_form: String,
    pub haiku_entity_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub door_hit: Option<DoorHit>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TripleProposal {
    pub index: usize,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub evidence: String,
    pub subject_is_author: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClaimProposal {
    pub index: usize,
    pub statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityAction {
    Bind { reef_id: Uuid, reroute: bool },
    Mint,
    Curator,
}

#[derive(Debug, Clone)]
pub struct MentionJudgment {
    pub index: usize,
    pub action: IdentityAction,
    pub entity_type: String,
    pub mint_title: String,
    pub door_hit: Option<DoorHit>,
    pub survivors: Vec<Uuid>,
}

#[derive(Debug, Clone)]
pub struct HoldJudgment {
    pub index: usize,
    pub level: HoldLevel,
    pub dropped_unbound: bool,
}

#[derive(Debug, Clone)]
pub struct ExtraPairJudgment {
    pub other_reef_id: Uuid,
    pub predicate: String,
    pub level: HoldLevel,
}

/// Bound non-author reef on this coral, with names Jev and the mention gate see.
#[derive(Debug, Clone)]
pub struct BoundOther {
    pub reef_id: Uuid,
    pub title: String,
    pub mention_name: String,
}

/// One author×other×predicate catalog slot (pass 3).
#[derive(Debug, Clone)]
pub struct CatalogRelSlot {
    pub other_reef_id: Uuid,
    pub title: String,
    pub predicate: String,
}

#[derive(Debug, Clone)]
pub struct PairJudgment {
    pub claim_index: usize,
    pub old_claim_id: Uuid,
    pub level: SupersessionLevel,
}

pub fn chunk_ids(ids: &[Uuid]) -> Vec<Vec<Uuid>> {
    if ids.is_empty() {
        return Vec::new();
    }
    ids.chunks(PASS1_CHUNK_IDS).map(|c| c.to_vec()).collect()
}

pub fn pass1_question_id(mention_index: usize, chunk_index: usize) -> String {
    format!("p1_{mention_index}_{chunk_index}")
}

pub fn pass2_id_question_id(mention_index: usize) -> String {
    format!("id_{mention_index}")
}

pub fn pass2_type_question_id(mention_index: usize) -> String {
    format!("etype_{mention_index}")
}

pub fn hold_question_id(kind: &str, index: usize) -> String {
    format!("{kind}_{index}")
}

pub fn extra_question_id(other: Uuid, predicate: &str) -> String {
    format!("extra_{other}_{predicate}")
}

pub fn pair_question_id(claim_index: usize, old_id: Uuid) -> String {
    format!("pair_{claim_index}_{old_id}")
}

fn choice_criteria(
    ids: impl Iterator<Item = String>,
    extra: &[(&str, Option<&str>)],
) -> BTreeMap<String, Option<String>> {
    let mut criteria = BTreeMap::new();
    for id in ids {
        criteria.insert(id, None);
    }
    for (k, desc) in extra {
        criteria.insert((*k).to_string(), desc.map(str::to_string));
    }
    criteria
}

/// Pass 1: one Choice per mention×chunk, all in one request. Option
/// descriptions are null — ids live in state.
pub fn pass1_questions(
    mentions: &[MentionProposal],
    chunks: &[Vec<Uuid>],
) -> BTreeMap<String, Question> {
    let mut questions = BTreeMap::new();
    if chunks.is_empty() {
        return questions;
    }
    for m in mentions {
        for (ci, chunk) in chunks.iter().enumerate() {
            questions.insert(
                pass1_question_id(m.index, ci),
                Question::Choice {
                    instructions: json!({
                        "task": PASS1_INSTRUCTIONS,
                        "mention_index": m.index,
                        "chunk_index": ci,
                    }),
                    criteria: choice_criteria(
                        chunk.iter().map(|id| id.to_string()),
                        &[(NONE, None)],
                    ),
                },
            );
        }
    }
    questions
}

/// A reef reaches pass 2 if any chunk is not `none`.
pub fn pass1_survivors(
    mentions: &[MentionProposal],
    chunks: &[Vec<Uuid>],
    resp: &DecisionResponse,
) -> Result<HashMap<usize, Vec<Uuid>>, DecisionError> {
    let mut out: HashMap<usize, Vec<Uuid>> = HashMap::new();
    if chunks.is_empty() {
        for m in mentions {
            out.insert(m.index, Vec::new());
        }
        return Ok(out);
    }
    for m in mentions {
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        for (ci, chunk) in chunks.iter().enumerate() {
            let qid = pass1_question_id(m.index, ci);
            let choice = resp
                .answers
                .get(&qid)
                .and_then(Answer::as_choice)
                .ok_or_else(|| DecisionError::Parse(format!("missing {qid}")))?;
            if choice == NONE {
                continue;
            }
            let id = Uuid::parse_str(choice)
                .map_err(|_| DecisionError::Parse(format!("invalid choice for {qid}: {choice}")))?;
            if !chunk.contains(&id) {
                return Err(DecisionError::Parse(format!(
                    "choice for {qid} is outside its chunk: {choice}"
                )));
            }
            if seen.insert(id) {
                ids.push(id);
            }
        }
        out.insert(m.index, ids);
    }
    Ok(out)
}

pub fn pass2_questions(
    mentions: &[MentionProposal],
    survivors: &HashMap<usize, Vec<Uuid>>,
) -> BTreeMap<String, Question> {
    let mut questions = BTreeMap::new();
    for m in mentions {
        let ids = survivors.get(&m.index).cloned().unwrap_or_default();
        questions.insert(
            pass2_id_question_id(m.index),
            Question::Choice {
                instructions: json!({
                    "task": PASS2_ID_INSTRUCTIONS,
                    "mention_index": m.index,
                }),
                criteria: choice_criteria(
                    ids.iter().map(|id| id.to_string()),
                    &[
                        (NEW, Some("This mention is not any survivor; mint a new reef using the Haiku name.")),
                        (CURATOR, Some("Identity is ambiguous; a human should decide. Do not mint or bind.")),
                    ],
                ),
            },
        );
        questions.insert(
            pass2_type_question_id(m.index),
            Question::Choice {
                instructions: json!({
                    "task": PASS2_TYPE_INSTRUCTIONS,
                    "mention_index": m.index,
                }),
                criteria: ENTITY_TYPES
                    .iter()
                    .map(|t| ((*t).to_string(), Some((*t).to_string())))
                    .collect(),
            },
        );
    }
    questions
}

/// Pass-2 composition (ADR-0013):
/// - Choice `new` and `door_hit.how` is identifier → force curator, no mint
/// - `new` against title / alias / permutation (or no hit) may mint
/// - picking another survivor is a reroute
/// - mint title = Haiku name / surface_form
pub fn compose_pass2(
    mention: &MentionProposal,
    survivors: &[Uuid],
    id_choice: &str,
    type_choice: Option<&str>,
) -> MentionJudgment {
    let entity_type = type_choice
        .filter(|t| ENTITY_TYPES.contains(t))
        .unwrap_or(mention.haiku_entity_type.as_str())
        .to_string();
    let mint_title = if mention.surface_form.trim().is_empty() {
        mention.name.clone()
    } else {
        mention.surface_form.clone()
    };
    let survivor_set: HashSet<Uuid> = survivors.iter().copied().collect();
    let action = if id_choice == CURATOR {
        IdentityAction::Curator
    } else if id_choice == NEW {
        match mention.door_hit.as_ref().map(|h| h.how) {
            Some(DoorHow::Identifier) => IdentityAction::Curator,
            _ => IdentityAction::Mint,
        }
    } else if let Ok(id) = Uuid::parse_str(id_choice) {
        if survivor_set.contains(&id) {
            let reroute = mention
                .door_hit
                .as_ref()
                .map(|h| h.reef_id != id)
                .unwrap_or(false);
            IdentityAction::Bind {
                reef_id: id,
                reroute,
            }
        } else {
            IdentityAction::Curator
        }
    } else {
        IdentityAction::Curator
    };
    MentionJudgment {
        index: mention.index,
        action,
        entity_type,
        mint_title,
        door_hit: mention.door_hit.clone(),
        survivors: survivors.to_vec(),
    }
}

pub fn compose_pass2_from_response(
    mentions: &[MentionProposal],
    survivors: &HashMap<usize, Vec<Uuid>>,
    resp: &DecisionResponse,
) -> Result<Vec<MentionJudgment>, DecisionError> {
    let mut out = Vec::with_capacity(mentions.len());
    for m in mentions {
        let qid = pass2_id_question_id(m.index);
        let id_choice = resp
            .answers
            .get(&qid)
            .and_then(Answer::as_choice)
            .ok_or_else(|| DecisionError::Parse(format!("missing {qid}")))?;
        let type_choice = resp
            .answers
            .get(&pass2_type_question_id(m.index))
            .and_then(Answer::as_choice);
        let surv = survivors.get(&m.index).map(Vec::as_slice).unwrap_or(&[]);
        out.push(compose_pass2(m, surv, id_choice, type_choice));
    }
    Ok(out)
}

fn hold_score_task(prefix: &str) -> &'static str {
    if prefix == "claim" {
        CLAIM_HOLD_SCORE_INSTRUCTIONS
    } else {
        HOLD_SCORE_INSTRUCTIONS
    }
}

pub fn hold_questions(prefix: &str, n: usize) -> BTreeMap<String, Question> {
    let mut questions = BTreeMap::new();
    for i in 0..n {
        questions.insert(
            hold_question_id(prefix, i),
            Question::Score {
                instructions: json!({
                    "task": hold_score_task(prefix),
                    "index": i,
                    "kind": prefix,
                }),
                criteria: HOLD_SCORE_CRITERIA
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
            },
        );
    }
    questions
}

pub fn is_catalog_rel_predicate(pred: &str) -> bool {
    EXTRA_PREDICATES
        .iter()
        .any(|p| p.eq_ignore_ascii_case(pred))
}

pub fn title_mentioned(coral: &str, title: &str) -> bool {
    let needle = title.trim().to_ascii_lowercase();
    !needle.is_empty() && coral.to_ascii_lowercase().contains(&needle)
}

/// Author × each other bound reef × works_on/manages/member_of.
/// `mention_gate` drops slots whose title and mention name are both absent
/// from the coral (code compose Drop; Jev is not asked).
pub fn catalog_rel_slots(
    author_reef: Option<Uuid>,
    others: &[BoundOther],
    coral: &str,
    mention_gate: bool,
) -> Vec<CatalogRelSlot> {
    if author_reef.is_none() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for other in others {
        if Some(other.reef_id) == author_reef || !seen.insert(other.reef_id) {
            continue;
        }
        if mention_gate
            && !title_mentioned(coral, &other.title)
            && !title_mentioned(coral, &other.mention_name)
        {
            continue;
        }
        for pred in EXTRA_PREDICATES {
            out.push(CatalogRelSlot {
                other_reef_id: other.reef_id,
                title: other.title.clone(),
                predicate: pred.to_string(),
            });
        }
    }
    out
}

pub fn extra_pair_questions(slots: &[CatalogRelSlot]) -> BTreeMap<String, Question> {
    let mut questions = BTreeMap::new();
    for s in slots {
        questions.insert(
            extra_question_id(s.other_reef_id, &s.predicate),
            Question::Score {
                instructions: json!({
                    "task": HOLD_SCORE_INSTRUCTIONS,
                    "other_reef_id": s.other_reef_id.to_string(),
                    "title": s.title,
                    "predicate": s.predicate,
                    "kind": "available_edge",
                }),
                criteria: HOLD_SCORE_CRITERIA
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
            },
        );
    }
    questions
}

pub fn hold_from_score(score: f64) -> HoldLevel {
    HoldLevel::from_index(nearest_level(score, 3))
}

pub fn supersession_from_score(score: f64) -> SupersessionLevel {
    SupersessionLevel::from_index(nearest_level(score, 4))
}

/// Extract triples whose both endpoints are bound get a Score. Unbound → drop.
/// `skip_score` is catalog-covered (`works_on` / `manages` / `member_of`):
/// Drop without a Jev round; the titled catalog slot is the write source.
pub fn compose_hold_from_response(
    prefix: &str,
    n: usize,
    unbound: &HashSet<usize>,
    skip_score: &HashSet<usize>,
    resp: &DecisionResponse,
) -> Result<Vec<HoldJudgment>, DecisionError> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if unbound.contains(&i) {
            out.push(HoldJudgment {
                index: i,
                level: HoldLevel::Drop,
                dropped_unbound: true,
            });
            continue;
        }
        if skip_score.contains(&i) {
            out.push(HoldJudgment {
                index: i,
                level: HoldLevel::Drop,
                dropped_unbound: false,
            });
            continue;
        }
        let qid = hold_question_id(prefix, i);
        let score = resp
            .answers
            .get(&qid)
            .and_then(Answer::as_score)
            .ok_or_else(|| DecisionError::Parse(format!("missing {qid}")))?;
        out.push(HoldJudgment {
            index: i,
            level: hold_from_score(score),
            dropped_unbound: false,
        });
    }
    Ok(out)
}

pub fn compose_extra_from_response(
    slots: &[CatalogRelSlot],
    resp: &DecisionResponse,
) -> Result<Vec<ExtraPairJudgment>, DecisionError> {
    let mut out = Vec::with_capacity(slots.len());
    for s in slots {
        let qid = extra_question_id(s.other_reef_id, &s.predicate);
        let score = resp
            .answers
            .get(&qid)
            .and_then(Answer::as_score)
            .ok_or_else(|| DecisionError::Parse(format!("missing {qid}")))?;
        out.push(ExtraPairJudgment {
            other_reef_id: s.other_reef_id,
            predicate: s.predicate.clone(),
            level: hold_from_score(score),
        });
    }
    Ok(out)
}

/// Skip extra author×other Scores when extract already emitted that triple.
/// Extract-then-score leftover (eval clone). Production pass 3 uses
/// [`catalog_rel_slots`] and does not skip extract-emitted predicates.
pub fn extra_pairs_needed(
    author_reef: Option<Uuid>,
    bound_others: &[Uuid],
    haiku: &[TripleProposal],
    mention_name_of: &HashMap<Uuid, String>,
) -> Vec<(Uuid, String)> {
    let Some(author) = author_reef else {
        return Vec::new();
    };
    let mut already: HashSet<(String, String)> = HashSet::new();
    for t in haiku {
        if t.subject_is_author {
            already.insert((
                t.predicate.to_ascii_lowercase(),
                t.object.to_ascii_lowercase(),
            ));
        }
        let author_name = mention_name_of
            .get(&author)
            .map(|s| s.to_ascii_lowercase())
            .unwrap_or_default();
        if !author_name.is_empty() && t.subject.to_ascii_lowercase() == author_name {
            already.insert((
                t.predicate.to_ascii_lowercase(),
                t.object.to_ascii_lowercase(),
            ));
        }
    }
    let mut out = Vec::new();
    let mut seen_others = HashSet::new();
    for &other in bound_others {
        if other == author || !seen_others.insert(other) {
            continue;
        }
        let other_name = mention_name_of
            .get(&other)
            .map(|s| s.to_ascii_lowercase())
            .unwrap_or_default();
        for pred in EXTRA_PREDICATES {
            if already.contains(&(pred.to_string(), other_name.clone())) {
                continue;
            }
            out.push((other, pred.to_string()));
        }
    }
    out
}

pub fn pair_questions(
    surviving_claims: &[usize],
    old_ids_by_claim: &HashMap<usize, Vec<Uuid>>,
) -> BTreeMap<String, Question> {
    let mut questions = BTreeMap::new();
    for &ci in surviving_claims {
        let Some(olds) = old_ids_by_claim.get(&ci) else {
            continue;
        };
        for old in olds {
            questions.insert(
                pair_question_id(ci, *old),
                Question::Score {
                    instructions: json!({
                        "task": SUPERSESSION_SCORE_INSTRUCTIONS,
                        "claim_index": ci,
                        "old_claim_id": old.to_string(),
                    }),
                    criteria: SUPERSESSION_SCORE_CRITERIA
                        .iter()
                        .map(|s| (*s).to_string())
                        .collect(),
                },
            );
        }
    }
    questions
}

pub fn compose_pairs_from_response(
    surviving_claims: &[usize],
    old_ids_by_claim: &HashMap<usize, Vec<Uuid>>,
    resp: &DecisionResponse,
) -> Result<Vec<PairJudgment>, DecisionError> {
    let mut out = Vec::new();
    for &ci in surviving_claims {
        let Some(olds) = old_ids_by_claim.get(&ci) else {
            continue;
        };
        for old in olds {
            let qid = pair_question_id(ci, *old);
            let score = resp
                .answers
                .get(&qid)
                .and_then(Answer::as_score)
                .ok_or_else(|| DecisionError::Parse(format!("missing {qid}")))?;
            out.push(PairJudgment {
                claim_index: ci,
                old_claim_id: *old,
                level: supersession_from_score(score),
            });
        }
    }
    Ok(out)
}

pub fn pass1_state(coral: &str, mentions: &[MentionProposal], catalog: &[SkinnyCard]) -> Value {
    json!({
        "coral": coral,
        "mentions": mentions,
        "catalog": catalog,
    })
}

pub fn pass2_state(
    coral: &str,
    mentions: &[MentionProposal],
    cards: &[IngestDigestCard],
    survivors: &HashMap<usize, Vec<Uuid>>,
) -> Value {
    json!({
        "coral": coral,
        "mentions": mentions,
        "survivors": survivors.iter().map(|(k, v)| (k.to_string(), v)).collect::<BTreeMap<_, _>>(),
        "cards": cards,
    })
}

pub fn pass3_state(
    coral: &str,
    triples: &[TripleProposal],
    slots: &[CatalogRelSlot],
    binds: &BTreeMap<String, Value>,
) -> Value {
    json!({
        "coral": coral,
        "triples": triples,
        "available_edges": slots.iter().map(|s| json!({
            "other_reef_id": s.other_reef_id,
            "title": s.title,
            "predicate": s.predicate,
        })).collect::<Vec<_>>(),
        "binds": binds,
    })
}

pub fn pass4_state(
    coral: &str,
    claims: &[ClaimProposal],
    old_cards: &[IngestDigestCard],
    binds: &BTreeMap<String, Value>,
) -> Value {
    json!({
        "coral": coral,
        "claims": claims,
        "old_claims": old_cards,
        "binds": binds,
    })
}

pub async fn evaluate(
    provider: &dyn DecisionProvider,
    state: Value,
    questions: BTreeMap<String, Question>,
) -> Result<DecisionResponse, DecisionError> {
    if questions.is_empty() {
        return Ok(DecisionResponse {
            model: jev_model_for(JevOp::Ingest),
            answers: BTreeMap::new(),
            usage: None,
        });
    }
    provider
        .evaluate(DecisionRequest {
            state,
            model: jev_model_for(JevOp::Ingest),
            questions,
            op: JevOp::Ingest,
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mention(i: usize, name: &str, hit: Option<DoorHit>) -> MentionProposal {
        MentionProposal {
            index: i,
            name: name.into(),
            surface_form: name.into(),
            haiku_entity_type: "person".into(),
            door_hit: hit,
        }
    }

    fn choice_resp(pairs: &[(&str, &str)]) -> DecisionResponse {
        let mut answers = BTreeMap::new();
        for (k, v) in pairs {
            answers.insert(
                (*k).into(),
                Answer::Choice {
                    choice: (*v).into(),
                    probabilities: BTreeMap::new(),
                    confidence: Some(1.0),
                },
            );
        }
        DecisionResponse {
            model: "jev-latest".into(),
            answers,
            usage: None,
        }
    }

    fn score_resp(pairs: &[(&str, f64)]) -> DecisionResponse {
        let mut answers = BTreeMap::new();
        for (k, v) in pairs {
            answers.insert(
                (*k).into(),
                Answer::Score {
                    score: *v,
                    legend: BTreeMap::new(),
                    probabilities: BTreeMap::new(),
                    confidence: 1.0,
                },
            );
        }
        DecisionResponse {
            model: "jev-latest".into(),
            answers,
            usage: None,
        }
    }

    #[test]
    fn chunks_254_plus_none() {
        let ids: Vec<Uuid> = (0..300).map(|_| Uuid::new_v4()).collect();
        let chunks = chunk_ids(&ids);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 254);
        assert_eq!(chunks[1].len(), 46);
        let qs = pass1_questions(&[mention(0, "Alex", None)], &chunks);
        assert_eq!(qs.len(), 2);
        match &qs[&pass1_question_id(0, 0)] {
            Question::Choice { criteria, .. } => {
                assert_eq!(criteria.len(), 255);
                assert!(criteria.contains_key(NONE));
            }
            _ => panic!("expected choice"),
        }
    }

    #[test]
    fn pass1_union_across_chunks() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let chunks = vec![vec![a], vec![b]];
        let mentions = [mention(0, "Alex", None)];
        let resp = choice_resp(&[
            (&pass1_question_id(0, 0), &a.to_string()),
            (&pass1_question_id(0, 1), NONE),
        ]);
        let surv = pass1_survivors(&mentions, &chunks, &resp).unwrap();
        assert_eq!(surv[&0], vec![a]);
    }

    #[test]
    fn identifier_new_forces_curator() {
        let hit = DoorHit {
            reef_id: Uuid::new_v4(),
            how: DoorHow::Identifier,
        };
        let m = mention(0, "Alex@x.com", Some(hit));
        let j = compose_pass2(&m, &[], NEW, Some("person"));
        assert_eq!(j.action, IdentityAction::Curator);
    }

    #[test]
    fn title_new_may_mint() {
        let hit = DoorHit {
            reef_id: Uuid::new_v4(),
            how: DoorHow::Title,
        };
        let m = mention(0, "Alex", Some(hit));
        let j = compose_pass2(&m, &[], NEW, Some("person"));
        assert_eq!(j.action, IdentityAction::Mint);
        assert_eq!(j.mint_title, "Alex");
    }

    #[test]
    fn permutation_new_may_mint() {
        let hit = DoorHit {
            reef_id: Uuid::new_v4(),
            how: DoorHow::Permutation,
        };
        let j = compose_pass2(&mention(0, "Alex Alex", Some(hit)), &[], NEW, None);
        assert_eq!(j.action, IdentityAction::Mint);
    }

    #[test]
    fn picking_other_survivor_is_reroute() {
        let door = Uuid::new_v4();
        let other = Uuid::new_v4();
        let hit = DoorHit {
            reef_id: door,
            how: DoorHow::Title,
        };
        let j = compose_pass2(
            &mention(0, "Alex", Some(hit)),
            &[door, other],
            &other.to_string(),
            Some("person"),
        );
        assert_eq!(
            j.action,
            IdentityAction::Bind {
                reef_id: other,
                reroute: true
            }
        );
    }

    #[test]
    fn bind_same_as_door_is_not_reroute() {
        let door = Uuid::new_v4();
        let hit = DoorHit {
            reef_id: door,
            how: DoorHow::Alias,
        };
        let j = compose_pass2(
            &mention(0, "Alex", Some(hit)),
            &[door],
            &door.to_string(),
            Some("person"),
        );
        assert_eq!(
            j.action,
            IdentityAction::Bind {
                reef_id: door,
                reroute: false
            }
        );
    }

    #[test]
    fn unbound_triple_drops_without_score() {
        let resp = score_resp(&[]);
        let unbound = HashSet::from([0]);
        let out = compose_hold_from_response("rel", 1, &unbound, &HashSet::new(), &resp).unwrap();
        assert!(out[0].dropped_unbound);
        assert_eq!(out[0].level, HoldLevel::Drop);
    }

    #[test]
    fn hold_rounds_to_nearest_level() {
        assert_eq!(hold_from_score(0.4), HoldLevel::Drop);
        assert_eq!(hold_from_score(0.6), HoldLevel::Suggested);
        assert_eq!(hold_from_score(1.6), HoldLevel::Confirmed);
    }

    #[test]
    fn extra_pairs_skip_haiku_triples() {
        let author = Uuid::new_v4();
        let other = Uuid::new_v4();
        let haiku = [TripleProposal {
            index: 0,
            subject: "Alex Example".into(),
            predicate: "works_on".into(),
            object: "CoralSwarm".into(),
            evidence: "works on CoralSwarm".into(),
            subject_is_author: true,
        }];
        let names = HashMap::from([
            (author, "Alex Example".into()),
            (other, "CoralSwarm".into()),
        ]);
        let extra = extra_pairs_needed(Some(author), &[other], &haiku, &names);
        assert!(!extra.iter().any(|(id, p)| *id == other && p == "works_on"));
        assert!(extra.iter().any(|(id, p)| *id == other && p == "manages"));
        assert!(extra.iter().any(|(id, p)| *id == other && p == "member_of"));
    }

    #[test]
    fn extra_pairs_dedup_bound_others() {
        let author = Uuid::new_v4();
        let other = Uuid::new_v4();
        let names = HashMap::from([
            (author, "Alex Example".into()),
            (other, "CoralSwarm".into()),
        ]);
        let extra = extra_pairs_needed(Some(author), &[other, other], &[], &names);
        assert_eq!(extra.len(), EXTRA_PREDICATES.len());
    }

    #[test]
    fn catalog_rel_mention_gate_drops_unspoken_titles() {
        let author = Uuid::new_v4();
        let fly = Uuid::new_v4();
        let cs = Uuid::new_v4();
        let others = [
            BoundOther {
                reef_id: fly,
                title: "Fly.io".into(),
                mention_name: "Fly.io".into(),
            },
            BoundOther {
                reef_id: cs,
                title: "CoralSwarm".into(),
                mention_name: "CoralSwarm".into(),
            },
        ];
        let coral = "I work on CoralSwarm. Identifier auto-merge is off.";
        let asked = catalog_rel_slots(Some(author), &others, coral, true);
        assert!(asked.iter().all(|s| s.other_reef_id == cs));
        assert_eq!(asked.len(), EXTRA_PREDICATES.len());
        let all = catalog_rel_slots(Some(author), &others, coral, false);
        assert_eq!(all.len(), EXTRA_PREDICATES.len() * 2);
    }

    #[test]
    fn catalog_rel_mention_name_hits_when_canonical_title_absent() {
        let author = Uuid::new_v4();
        let fly = Uuid::new_v4();
        let others = [BoundOther {
            reef_id: fly,
            title: "Fly.io".into(),
            mention_name: "Fly".into(),
        }];
        let asked = catalog_rel_slots(
            Some(author),
            &others,
            "Deployed the worker to Fly last night.",
            true,
        );
        assert_eq!(asked.len(), EXTRA_PREDICATES.len());
        assert!(asked.iter().all(|s| s.other_reef_id == fly));
    }

    #[test]
    fn catalog_rel_asks_all_three_even_if_extract_proposed_works_on() {
        let author = Uuid::new_v4();
        let other = Uuid::new_v4();
        let others = [BoundOther {
            reef_id: other,
            title: "CoralSwarm".into(),
            mention_name: "CoralSwarm".into(),
        }];
        let slots = catalog_rel_slots(
            Some(author),
            &others,
            "I shipped the ingest-door worker to live on CoralSwarm last night.",
            true,
        );
        let preds: HashSet<_> = slots.iter().map(|s| s.predicate.as_str()).collect();
        assert_eq!(preds, HashSet::from(["works_on", "manages", "member_of"]));
    }

    #[test]
    fn title_mentioned_is_casefold_substring() {
        assert!(title_mentioned("I cut over to OpenRouter.", "openrouter"));
        assert!(!title_mentioned("School pickup is at 3.", "Fly.io"));
    }

    #[test]
    fn catalog_predicate_skips_extract_score() {
        let resp = score_resp(&[]);
        let skip = HashSet::from([0]);
        let out = compose_hold_from_response("rel", 1, &HashSet::new(), &skip, &resp).unwrap();
        assert!(!out[0].dropped_unbound);
        assert_eq!(out[0].level, HoldLevel::Drop);
    }

    #[test]
    fn pass1_rejects_choice_outside_chunk() {
        let a = Uuid::new_v4();
        let outside = Uuid::new_v4();
        let chunks = vec![vec![a]];
        let mentions = [mention(0, "Alex", None)];
        let resp = choice_resp(&[(&pass1_question_id(0, 0), &outside.to_string())]);
        assert!(pass1_survivors(&mentions, &chunks, &resp).is_err());
    }

    #[test]
    fn claim_hold_questions_use_claim_instructions() {
        let qs = hold_questions("claim", 1);
        match &qs[&hold_question_id("claim", 0)] {
            Question::Score { instructions, .. } => {
                assert_eq!(
                    instructions.get("task").and_then(|v| v.as_str()),
                    Some(CLAIM_HOLD_SCORE_INSTRUCTIONS)
                );
                assert_eq!(
                    instructions.get("kind").and_then(|v| v.as_str()),
                    Some("claim")
                );
            }
            _ => panic!("expected score"),
        }
        let rel = hold_questions("rel", 1);
        match &rel[&hold_question_id("rel", 0)] {
            Question::Score { instructions, .. } => {
                assert_eq!(
                    instructions.get("task").and_then(|v| v.as_str()),
                    Some(HOLD_SCORE_INSTRUCTIONS)
                );
            }
            _ => panic!("expected score"),
        }
    }

    #[test]
    fn pass2_questions_include_new_and_curator() {
        let m = mention(0, "Alex", None);
        let surv = HashMap::from([(0, vec![Uuid::new_v4()])]);
        let qs = pass2_questions(&[m], &surv);
        match &qs[&pass2_id_question_id(0)] {
            Question::Choice { criteria, .. } => {
                assert!(criteria.contains_key(NEW));
                assert!(criteria.contains_key(CURATOR));
                assert_eq!(criteria.len(), 3);
            }
            _ => panic!("expected choice"),
        }
        match &qs[&pass2_type_question_id(0)] {
            Question::Choice { criteria, .. } => {
                assert_eq!(criteria.len(), 4);
            }
            _ => panic!("expected type choice"),
        }
    }

    #[test]
    fn supersession_rounds_to_nearest_level() {
        assert_eq!(supersession_from_score(0.4), SupersessionLevel::Unrelated);
        assert_eq!(supersession_from_score(0.6), SupersessionLevel::SameFold);
        assert_eq!(supersession_from_score(1.6), SupersessionLevel::Supersede);
        assert_eq!(supersession_from_score(2.6), SupersessionLevel::Curator);
    }

    #[test]
    fn compose_claim_hold_uses_claim_prefix() {
        let resp = score_resp(&[("claim_0", 1.6)]);
        let out = compose_hold_from_response("claim", 1, &HashSet::new(), &HashSet::new(), &resp)
            .unwrap();
        assert_eq!(out[0].level, HoldLevel::Confirmed);
        assert!(!out[0].dropped_unbound);
    }

    #[test]
    fn empty_survivors_emit_no_pair_questions() {
        let qs = pair_questions(&[], &HashMap::new());
        assert!(qs.is_empty());
    }

    #[test]
    fn compose_pairs_from_scores() {
        let old = Uuid::new_v4();
        let surviving = vec![0usize];
        let olds = HashMap::from([(0usize, vec![old])]);
        let qid = pair_question_id(0, old);
        let resp = score_resp(&[(&qid, 1.6)]);
        let out = compose_pairs_from_response(&surviving, &olds, &resp).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].claim_index, 0);
        assert_eq!(out[0].old_claim_id, old);
        assert_eq!(out[0].level, SupersessionLevel::Supersede);
    }
}
