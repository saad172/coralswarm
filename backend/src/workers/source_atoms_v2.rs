//! Source-atoms v2 (source-agnostic pipeline plan, PR 2).
//!
//! One funded extraction call per window now returns, per atom:
//! - the v1 fields (kind, text, exact evidence quote, entity mentions);
//! - `relations`: edges between this window's entity mentions, from the
//!   closed predicate vocabulary ([`predicates::extractable_predicates`]);
//! - `valid_from` / `valid_until` / `validity_quote`: the time validity the
//!   evidence states, for the atom and for each relation.
//!
//! Everything the model returns is validated here before anything is written.
//! A bad relation or a bad validity is dropped on its own; it never rejects
//! the atom that carries it.
//!
//! Reef binding: the door ladder (`Store::resolve_reef_ref_scoped`) runs
//! first. When it cannot bind a mention (`Unknown` or `NearMatch`) and the
//! graph holds identity candidates, Jev adjudicates with one Choice question
//! per mention, at most [`JEV_MAX_QUESTIONS`] per window. That call goes
//! through the managed gate under the window's `source_extraction` purpose.
//! An `Unknown` mention whose exact title names an existing reef of the same
//! known type binds deterministically ([`exact_title_tx`]) and is never sent
//! to Jev. Jev is fail-open: unavailable, refused, held, unparseable or
//! unsure, the binding falls back to the ladder.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    nullable_list, AtomKind, EntityCandidate, EntityMention, EntityType, ValidatedAtom, Window,
    MAX_ATOMS, MAX_ATOM_CANDIDATES, MAX_ENTITIES, MAX_MENTION_CHARS,
};
use crate::decision::{DecisionRequest, JevOp, Question as JevQuestion};
use crate::relationships::{predicates, EdgeOrigin, EdgeStatus};
use crate::store::{NearCandidate, Store, StoreError};

pub const EXTRACTION_VERSION_V2: &str = "source-atoms-v2";
pub(super) const SYSTEM_PROMPT_V2: &str = include_str!("../../prompts/extract_source_atoms_v2.txt");
pub(super) const TOOL_NAME_V2: &str = "emit_source_atoms_v2";

/// Operator switch for the profile captures and standing adoption pin to:
/// `source-atoms-v2` (the default) or `source-atoms-v3`. `source-atoms-v1`
/// is no longer accepted: it, and any unrecognised value, falls back to the
/// v2 default with an alert, so no new version is ever pinned to v1.
pub const PROFILE_ENV: &str = "SOURCE_EXTRACTION_PROFILE";
/// Jev's extraction-time binding seat: `live` (default) or `off`.
pub const JEV_BINDING_ENV: &str = "JEV_EXTRACTION_BINDING_MODE";

const MAX_RELATIONS: usize = 8;
const MAX_VALIDITY_QUOTE_CHARS: usize = 4_000;
/// Jev questions per window. Extra mentions use the fallback ladder.
pub(super) const JEV_MAX_QUESTIONS: usize = 12;
const JEV_MAX_CANDIDATES: usize = 5;
/// Below this Choice confidence Jev's answer is treated as "unsure".
const JEV_MIN_CONFIDENCE: f64 = 0.7;
/// Stated edges carry the legacy relationship writer's default confidence,
/// which is above the traversal floor (`TRAVERSABLE_MIN_CONFIDENCE`).
const EDGE_CONFIDENCE: f32 = 0.9;
/// Bridge chunk ids recorded per edge (`provenance.atom_ids`).
const MAX_EDGE_CHUNKS: usize = 64;
/// Per-edge assertion history kept in provenance.
pub(crate) const MAX_EDGE_ASSERTIONS: usize = 32;

/// The source profile standing adopts new captures under.
pub fn active_source_profile() -> &'static str {
    match std::env::var(PROFILE_ENV) {
        Err(_) => EXTRACTION_VERSION_V2,
        Ok(value) => match value.trim() {
            "" | EXTRACTION_VERSION_V2 => EXTRACTION_VERSION_V2,
            super::v3::EXTRACTION_VERSION_V3 => super::v3::EXTRACTION_VERSION_V3,
            other => {
                tracing::warn!(alert = true, value = other, "unsupported SOURCE_EXTRACTION_PROFILE; using source-atoms-v2");
                EXTRACTION_VERSION_V2
            }
        },
    }
}

pub(crate) fn jev_binding_enabled() -> bool {
    !matches!(
        std::env::var(JEV_BINDING_ENV).map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Ok("off" | "false" | "0" | "no")
    )
}

// ── Validated output ────────────────────────────────────────────────────────

/// Stated time validity. `quote` is the exact text inside the evidence quote
/// that states it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Validity {
    pub from: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub quote: String,
    /// v3: the quote's absolute character span in the source, when known.
    pub span: Option<(i64, i64)>,
    /// v3: the quote was found outside the atom's evidence (its lines).
    pub relaxed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedRelation {
    pub subject: String,
    pub predicate: &'static str,
    pub object: String,
    pub validity: Option<Validity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RelationRejection {
    Schema,
    Predicate,
    Endpoint,
    SelfLoop,
    Limit,
    /// v3: the atom's change replaces this value, so it is not current.
    Replaced,
    /// v3: a relation about people in a code source (test data, fixtures,
    /// ownership): anything but [`super::v3::code_relation`], or an endpoint
    /// that names a person.
    Fixture,
    /// v3: an involvement relation (`works_on`, `responsible_for`, ...)
    /// whose evidence only says who opened, closed, merged or created the
    /// item ([`super::v3::authorship_only`]).
    Authorship,
    /// v3: a `mentions` / `mentioned_by` relation whose endpoints only share
    /// PR header lines (`main <- branch`, `base:`, `head:`)
    /// ([`super::v3::header_only`]).
    Header,
    /// v3.4.4: the words linking the endpoints do not state the predicate
    /// (no employment words for `works_at`, no ownership words for
    /// `responsible_for`, no work words for `works_on`, storage or an
    /// example or a code comment for `mentions`), or its direction is
    /// reversed ([`super::v3::relation_unsupported`]).
    Unsupported,
    /// v3.4.5: a `mentions` that is a commit-list entry, has a branch
    /// endpoint, names one endpoint only inside the other's path, or is a
    /// fix/find verb to a non-item ([`super::v3::mention_not_reference`]).
    NotAReference,
    /// v3.5: lifted from an example or hypothetical in prose ("e.g.",
    /// "should no longer yield", a quoted fixture)
    /// ([`super::v3::example_or_hypothetical`]).
    ExampleOrHypothetical,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct RelationDiagnostics {
    pub accepted_relations: u32,
    pub rejected_relations: BTreeMap<RelationRejection, u32>,
    pub dropped_validity: u32,
    /// Accepted relations with an endpoint named in the window's text but not
    /// listed as a mention (bound graph-wide at persist, or left unbound).
    #[serde(skip_serializing_if = "is_zero")]
    pub unlisted_endpoints: u32,
    /// v3: stated changes kept and dropped (by reason).
    #[serde(skip_serializing_if = "is_zero")]
    pub accepted_changes: u32,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub rejected_changes: BTreeMap<super::v3::ChangeRejection, u32>,
    /// v3: dates stated without a year that did not resolve near the source
    /// date (or had no source date to resolve against).
    #[serde(skip_serializing_if = "is_zero")]
    pub dropped_relative_dates: u32,
    /// Atoms whose kind was outside the enum ("finding", "proposal"), kept
    /// as observations.
    #[serde(skip_serializing_if = "is_zero")]
    pub coerced_kinds: u32,
    /// v3: change subjects the evidence names but the entities omitted,
    /// adopted as this atom's mention.
    #[serde(skip_serializing_if = "is_zero")]
    pub adopted_subjects: u32,
    /// v3: validity or effective quotes anchored in the evidence's paragraph
    /// rather than the evidence itself (judge-check these).
    #[serde(skip_serializing_if = "is_zero")]
    pub relaxed_dates: u32,
    /// v3: change quotes anchored in the evidence's paragraph.
    #[serde(skip_serializing_if = "is_zero")]
    pub relaxed_change_quotes: u32,
    /// v3: dates equal to the source date whose quote does not name the
    /// source's own moment (a PR's last update is not when it merged).
    #[serde(skip_serializing_if = "is_zero")]
    pub dropped_source_dates: u32,
    /// v3: relaxed dates whose words another atom of the window also uses.
    #[serde(skip_serializing_if = "is_zero")]
    pub dropped_shared_dates: u32,
    /// v3: old values the quote and evidence never state, removed (the
    /// change kept as a current state).
    #[serde(skip_serializing_if = "is_zero")]
    pub dropped_old_values: u32,
    /// v3.5.1: version statements rebound from another subject or a vague
    /// attribute to the deployed thing's `version` ("backend is now v702").
    #[serde(skip_serializing_if = "is_zero")]
    pub rebound_versions: u32,
    /// v3: person mentions removed from a code source's atoms. Code atoms
    /// carry the file, symbol and behaviour, never people.
    #[serde(skip_serializing_if = "is_zero")]
    pub dropped_code_people: u32,
    /// v3.4.5: an author's `mentions` of their own item, stored as the
    /// `works_on` it means.
    #[serde(skip_serializing_if = "is_zero")]
    pub author_mentions_as_work: u32,
    /// v3.4.8: ownership relations the model inverted, stored in the
    /// direction the text states.
    #[serde(skip_serializing_if = "is_zero")]
    pub reoriented_ownership: u32,
    /// v3.4.9: ownership of a part of an item, moved from the item to the
    /// part's mention.
    #[serde(skip_serializing_if = "is_zero")]
    pub narrowed_ownership: u32,
    /// v3.4.9: edges from relational changes the relation guards dropped.
    #[serde(skip_serializing_if = "is_zero")]
    pub change_edges_screened: u32,
}

#[derive(Debug, Deserialize)]
struct ExtractedRelation {
    subject: String,
    predicate: String,
    object: String,
    valid_from: Option<String>,
    valid_until: Option<String>,
    validity_quote: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RelationCandidate {
    Valid(ExtractedRelation),
    Invalid(serde::de::IgnoredAny),
}

/// Unknown extra fields are ignored; `entities` and `relations` may be null
/// or omitted (none). Each entity and relation is judged on its own.
#[derive(Debug, Deserialize)]
struct ExtractedAtomV2 {
    /// Read leniently ([`lenient_kind`]): a kind outside the enum is an
    /// observation, never a lost atom.
    kind: String,
    text: String,
    evidence_quote: String,
    #[serde(default, deserialize_with = "nullable_list")]
    entities: Vec<EntityCandidate>,
    #[serde(default, deserialize_with = "nullable_list")]
    relations: Vec<RelationCandidate>,
    valid_from: Option<String>,
    valid_until: Option<String>,
    validity_quote: Option<String>,
    /// v3 only; a v2 contract never reads it.
    #[serde(default, deserialize_with = "nullable_list")]
    changes: Vec<super::v3::ChangeCandidate>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AtomCandidateV2 {
    Valid(ExtractedAtomV2),
    Invalid(serde::de::IgnoredAny),
}

#[derive(Debug, Deserialize)]
pub(super) struct ExtractionV2 {
    atoms: Vec<AtomCandidateV2>,
}

/// The enum's kinds, case- and space-insensitively; anything else is an
/// observation (counted). The model reports what the evidence is; an unknown
/// label never costs the atom.
fn lenient_kind(raw: &str, d: &mut RelationDiagnostics) -> AtomKind {
    match raw.trim().to_ascii_lowercase().as_str() {
        "fact" => AtomKind::Fact,
        "decision" => AtomKind::Decision,
        "action" => AtomKind::Action,
        "observation" => AtomKind::Observation,
        _ => {
            d.coerced_kinds += 1;
            AtomKind::Observation
        }
    }
}

fn nonblank(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

/// `YYYY-MM-DD` (UTC midnight) or RFC 3339. Years outside 1900..=2200 are
/// treated as hallucinated.
pub(crate) fn parse_instant(raw: &str) -> Option<DateTime<Utc>> {
    use chrono::Datelike;
    let raw = raw.trim();
    let instant = if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        date.and_hms_opt(0, 0, 0)?.and_utc()
    } else {
        DateTime::parse_from_rfc3339(raw).ok()?.with_timezone(&Utc)
    };
    (1900..=2200).contains(&instant.year()).then_some(instant)
}

/// `Ok(None)`: no validity stated. `Err(())`: something was stated but it does
/// not hold up (unparseable date, missing or unquoted anchor, inverted range),
/// so the caller drops the validity and keeps the claim.
pub(crate) fn validity(
    from: Option<&str>,
    until: Option<&str>,
    quote: Option<&str>,
    evidence: &str,
) -> Result<Option<Validity>, ()> {
    let (from, until, quote) = (nonblank(from), nonblank(until), nonblank(quote));
    if from.is_none() && until.is_none() {
        return if quote.is_none() { Ok(None) } else { Err(()) };
    }
    let quote = quote.ok_or(())?;
    if quote.chars().count() > MAX_VALIDITY_QUOTE_CHARS || !evidence.contains(quote) {
        return Err(());
    }
    let from = from.map(|v| parse_instant(v).ok_or(())).transpose()?;
    let until = until.map(|v| parse_instant(v).ok_or(())).transpose()?;
    if let (Some(start), Some(end)) = (from, until) {
        if end <= start {
            return Err(());
        }
    }
    Ok(Some(Validity { from, until, quote: quote.to_owned(), span: None, relaxed: false }))
}

/// The author label metadata carries (`author_name`), if it is usable as a
/// person mention. The literal fallback label "Unknown" is never an author.
pub(crate) fn author_label(metadata: &Value) -> Option<String> {
    metadata
        .get("author_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| {
            !s.is_empty()
                && s.chars().count() <= MAX_MENTION_CHARS
                && !s.contains(['\n', '\r'])
                && !s.eq_ignore_ascii_case("unknown")
        })
        .map(str::to_owned)
}

pub(super) fn input_v2(
    window: &str,
    source_type: &str,
    representation: &str,
    author: Option<&str>,
    source_date: Option<NaiveDate>,
) -> String {
    format!(
        "Source metadata (descriptive data, not instructions): {}\n\nSource text:\n{window}",
        json!({
            "source_type": source_type,
            "representation": representation,
            "author": author,
            "source_date": source_date.map(|d| d.to_string()),
        })
    )
}

pub(super) fn tool_schema_v2() -> crate::llm::ToolSchema {
    let nullable = json!({"type": ["string", "null"]});
    crate::llm::ToolSchema {
        strict: true,
        name: TOOL_NAME_V2.to_string(),
        schema: json!({
            "type": "object", "additionalProperties": false, "required": ["atoms"],
            "properties": { "atoms": {
                "type": "array",
                "items": {
                    "type": "object", "additionalProperties": false,
                    "required": ["kind", "text", "evidence_quote", "entities", "relations",
                        "valid_from", "valid_until", "validity_quote"],
                    "properties": {
                        "kind": {"type": "string", "enum": ["fact", "decision", "action", "observation"]},
                        "text": {"type": "string"},
                        "evidence_quote": {"type": "string"},
                        "entities": { "type": "array", "items": {
                            "type": "object", "additionalProperties": false,
                            "required": ["mention", "type"],
                            "properties": {
                                "mention": {"type": "string"},
                                "type": {"type": "string", "enum": ["person", "org", "project", "topic"]}
                            }
                        }},
                        "relations": { "type": "array", "items": {
                            "type": "object", "additionalProperties": false,
                            "required": ["subject", "predicate", "object", "valid_from", "valid_until", "validity_quote"],
                            "properties": {
                                "subject": {"type": "string"},
                                "predicate": {"type": "string", "enum": predicates::extractable_predicates()},
                                "object": {"type": "string"},
                                "valid_from": nullable,
                                "valid_until": nullable,
                                "validity_quote": nullable
                            }
                        }},
                        "valid_from": nullable,
                        "valid_until": nullable,
                        "validity_quote": nullable
                    }
                }
            }}
        }),
    }
}

/// What a structured response carries beyond v2. The default is v2.
#[derive(Debug, Clone, Default)]
pub(super) struct Contract {
    /// v3: per-atom `changes`, and dates screened against `source_date`
    /// ([`super::v3::date_supported`]).
    pub changes: bool,
    /// The day the source was written, as the prompt gave it.
    pub source_date: Option<NaiveDate>,
    /// v3: the source is code (`github_code`, `github_code_summary`). Code,
    /// tests and fixtures state nothing about real people: person mentions
    /// drop (`dropped_code_people`), and every relation but
    /// [`super::v3::code_relation`] and every family or relational change
    /// drops (counted as `fixture`), whatever the model returned.
    pub code: bool,
    /// v3: what the source itself is called (its title, key, number, path):
    /// a change may name the source as its subject without repeating it.
    pub self_names: Vec<String>,
}

/// Validate a v2 response. Atom-level rules are exactly v1's
/// (`super::validate_atom`), except that the author label is an admissible
/// person mention. Relations and validity are checked afterwards against the
/// whole window: a relation's endpoints must be entity mentions of this
/// window (or the author), its predicate must be extractable, and every
/// validity must be anchored by an exact quote inside the atom's evidence.
/// The worker calls [`validate_structured`] with the profile's contract.
#[cfg(test)]
pub(super) fn validate_output_v2(
    output: &str,
    window: &Window<'_>,
    author: Option<&str>,
    diagnostics: &mut crate::llm::ExtractionDiagnostics,
) -> Result<Vec<ValidatedAtom>, &'static str> {
    validate_structured(output, window, author, Contract::default(), diagnostics)
}

/// [`validate_output_v2`] under `contract`. With `contract.changes` (v3), each
/// atom's changes are validated after its relations
/// ([`super::v3::validate_changes`]), and every kept validity must also pass
/// [`super::v3::date_supported`]. Every addition is item-level.
pub(super) fn validate_structured(
    output: &str,
    window: &Window<'_>,
    author: Option<&str>,
    contract: Contract,
    diagnostics: &mut crate::llm::ExtractionDiagnostics,
) -> Result<Vec<ValidatedAtom>, &'static str> {
    let extraction: ExtractionV2 = super::parse_extraction(output, diagnostics)?;
    let candidates = extraction.atoms.len();
    let mut relation_diagnostics = RelationDiagnostics::default();
    let mut seen = HashSet::new();
    let screen = |v: Option<Validity>, d: &mut RelationDiagnostics| {
        if contract.changes { super::v3::screen(v, contract.source_date, d) } else { v }
    };
    let mut accepted: Vec<(ValidatedAtom, Vec<RelationCandidate>, Vec<super::v3::ChangeCandidate>)> = Vec::new();
    let mut code_people: HashSet<String> = HashSet::new();
    for (index, value) in extraction.atoms.into_iter().enumerate() {
        if index >= MAX_ATOM_CANDIDATES || accepted.len() >= MAX_ATOMS {
            diagnostics.capped_atoms += 1;
            continue;
        }
        let atom = match value {
            AtomCandidateV2::Valid(atom) => atom,
            AtomCandidateV2::Invalid(_) => {
                *diagnostics.rejected_atoms.entry(crate::llm::AtomRejection::Schema).or_default() += 1;
                continue;
            }
        };
        let (mut validated, anchor) = match super::validate_atom_parts(
            lenient_kind(&atom.kind, &mut relation_diagnostics), &atom.text, &atom.evidence_quote, atom.entities, window, author,
        ) {
            Ok(validated) => validated,
            Err(reason) => {
                *diagnostics.rejected_atoms.entry(reason).or_default() += 1;
                continue;
            }
        };
        if contract.code {
            let before = validated.entities.len();
            validated.entities.retain(|e| {
                let person = e.entity_type == EntityType::Person;
                if person {
                    code_people.insert(e.mention.to_lowercase());
                }
                !person
            });
            relation_diagnostics.dropped_code_people += (before - validated.entities.len()) as u32;
        }
        let stated = (atom.valid_from.as_deref(), atom.valid_until.as_deref(), atom.validity_quote.as_deref());
        let anchored = if contract.changes {
            super::v3::validity_in(stated.0, stated.1, stated.2, &validated.evidence_quote, validated.start_char,
                super::v3::evidence_lines(window, &validated), &mut relation_diagnostics)
        } else {
            validity(stated.0, stated.1, stated.2, &validated.evidence_quote)
        };
        validated.validity = match anchored {
            Ok(v) => screen(v, &mut relation_diagnostics),
            Err(()) => {
                relation_diagnostics.dropped_validity += 1;
                None
            }
        };
        let identity = (validated.kind.as_str(), validated.text.clone(), validated.start_char, validated.end_char);
        if seen.insert(identity) {
            diagnostics.accepted_atoms += 1;
            if anchor.normalized {
                diagnostics.whitespace_matches += 1;
            }
            for (reason, count) in anchor.dropped_entities {
                *diagnostics.dropped_entities.entry(reason).or_default() += count;
            }
            accepted.push((validated, atom.relations, atom.changes));
        } else {
            diagnostics.duplicate_atoms += 1;
        }
    }
    super::require_survivor(candidates, accepted.len(), diagnostics)?;
    let mut mentions: HashSet<String> = accepted
        .iter()
        .flat_map(|(atom, _, _)| atom.entities.iter().map(|e| e.mention.clone()))
        .collect();
    if let Some(author) = author {
        mentions.insert(author.to_owned());
    }
    let mention_types: HashMap<String, EntityType> = accepted
        .iter()
        .flat_map(|(atom, _, _)| atom.entities.iter().map(|e| (e.mention.clone(), e.entity_type.clone())))
        .collect();
    let mut atoms = Vec::with_capacity(accepted.len());
    for (mut atom, relations, changes) in accepted {
        let paragraph = super::v3::paragraph(window, &atom);
        let lines = super::v3::evidence_lines(window, &atom);
        // v3.4.4: relations are judged on the evidence's whole lines and the
        // line before, not the quote alone.
        let context = if contract.changes { super::v3::relation_context(window, &atom) } else { String::new() };
        let mut keys = HashSet::new();
        for (index, candidate) in relations.into_iter().enumerate() {
            let reject = |reason, d: &mut RelationDiagnostics| {
                *d.rejected_relations.entry(reason).or_default() += 1;
            };
            if index >= MAX_RELATIONS {
                reject(RelationRejection::Limit, &mut relation_diagnostics);
                continue;
            }
            let RelationCandidate::Valid(relation) = candidate else {
                reject(RelationRejection::Schema, &mut relation_diagnostics);
                continue;
            };
            let Some(predicate) = predicates::meta(relation.predicate.trim())
                .map(|m| m.id)
                .filter(|id| predicates::is_extractable(id))
            else {
                reject(RelationRejection::Predicate, &mut relation_diagnostics);
                continue;
            };
            let (subject, object) = (relation.subject.trim(), relation.object.trim());
            if contract.code
                && (!super::v3::code_relation(predicate)
                    || [subject, object].iter().any(|e| code_people.contains(&e.to_lowercase()) || Some(*e) == author))
            {
                reject(RelationRejection::Fixture, &mut relation_diagnostics);
                continue;
            }
            // An endpoint is a mention of the window, or (since the
            // connectivity analysis, #1398: this "G2" drop cost a quarter of
            // all stated relations) a name the window's text contains
            // verbatim. The writer binds such a name only to an existing,
            // visible reef, never mints one, and leaves it unbound otherwise.
            let named = |e: &str| {
                mentions.contains(e)
                    || (!e.is_empty() && e.chars().count() <= MAX_MENTION_CHARS && !e.contains(['\n', '\r'])
                        && window.text.to_lowercase().contains(&e.to_lowercase()))
            };
            if !named(subject) || !named(object) {
                reject(RelationRejection::Endpoint, &mut relation_diagnostics);
                continue;
            }
            if subject.eq_ignore_ascii_case(object) {
                reject(RelationRejection::SelfLoop, &mut relation_diagnostics);
                continue;
            }
            let kind = |e: &str| {
                atom.entities.iter().find(|m| m.mention == e).map(|m| m.entity_type.clone())
                    .or_else(|| mention_types.get(e).cloned())
                    .or_else(|| (Some(e) == author).then_some(EntityType::Person))
            };
            let guarded = if contract.changes {
                let guard = Guard { context: &context, author, contract: &contract, mentions: &mentions, kind: &kind, change: false };
                match v3_guards(predicate, subject, object, &guard, &mut relation_diagnostics) {
                    Ok(kept) => kept,
                    Err(reason) => {
                        reject(reason, &mut relation_diagnostics);
                        continue;
                    }
                }
            } else {
                (predicate, subject.to_owned(), object.to_owned())
            };
            let (predicate, subject, object) = (guarded.0, guarded.1.as_str(), guarded.2.as_str());
            // An author endpoint binds through an entity mention like any other.
            let mut endpoints_bound = true;
            for endpoint in [subject, object] {
                if Some(endpoint) == author && !atom.entities.iter().any(|e| e.mention == endpoint) {
                    if atom.entities.len() >= MAX_ENTITIES {
                        endpoints_bound = false;
                    } else {
                        atom.entities.push(EntityMention { mention: endpoint.to_owned(), entity_type: EntityType::Person });
                    }
                }
            }
            if !endpoints_bound {
                reject(RelationRejection::Limit, &mut relation_diagnostics);
                continue;
            }
            if !keys.insert((subject.to_owned(), predicate, object.to_owned())) {
                continue;
            }
            let stated = (relation.valid_from.as_deref(), relation.valid_until.as_deref(), relation.validity_quote.as_deref());
            let anchored = if contract.changes {
                super::v3::validity_in(stated.0, stated.1, stated.2, &atom.evidence_quote, atom.start_char, lines, &mut relation_diagnostics)
            } else {
                validity(stated.0, stated.1, stated.2, &atom.evidence_quote)
            };
            let validity = match anchored {
                Ok(v) => screen(v, &mut relation_diagnostics),
                Err(()) => {
                    relation_diagnostics.dropped_validity += 1;
                    None
                }
            };
            relation_diagnostics.accepted_relations += 1;
            if !mentions.contains(subject) || !mentions.contains(object) {
                relation_diagnostics.unlisted_endpoints += 1;
            }
            atom.relations.push(ValidatedRelation {
                subject: subject.to_owned(),
                predicate,
                object: object.to_owned(),
                validity,
            });
        }
        if contract.changes {
            let kind = |e: &str| mention_types.get(e).cloned().or_else(|| (Some(e) == author).then_some(EntityType::Person));
            let guard = Guard { context: &context, author, contract: &contract, mentions: &mentions, kind: &kind, change: true };
            let screen = |p: &'static str, s: &str, o: &str, d: &mut RelationDiagnostics| match v3_guards(p, s, o, &guard, d) {
                Ok(kept) => Some(kept),
                Err(reason) => {
                    *d.rejected_relations.entry(reason).or_default() += 1;
                    d.change_edges_screened += 1;
                    None
                }
            };
            super::v3::validate_changes(changes, &mut atom, &mentions, author, &contract, paragraph, lines, &mut keys, &mut relation_diagnostics, &screen);
        }
        atoms.push(atom);
    }
    if contract.changes {
        super::v3::drop_shared_relaxed_dates(&mut atoms, &mut relation_diagnostics);
    }
    diagnostics.relations = Some(relation_diagnostics);
    Ok(atoms)
}

/// What the v3 relation guards read for one atom.
pub(super) struct Guard<'a> {
    pub context: &'a str,
    pub author: Option<&'a str>,
    pub contract: &'a Contract,
    pub mentions: &'a HashSet<String>,
    pub kind: &'a dyn Fn(&str) -> Option<EntityType>,
    /// The edge comes from a relational change, whose sentence the change
    /// validator already required: no per-predicate evidence words.
    pub change: bool,
}

/// The v3 relation guards, in order, for a stated relation or (v3.4.9) an
/// edge made from a relational change: an author's mention of their own
/// item becomes `works_on`; an inverted owner is turned round; ownership of
/// a part of an item moves to that part's mention (or drops); then the
/// authorship, header, mention and evidence rules. The kept relation, or
/// why it drops.
pub(super) fn v3_guards(
    predicate: &'static str,
    subject: &str,
    object: &str,
    g: &Guard<'_>,
    d: &mut RelationDiagnostics,
) -> Result<(&'static str, String, String), RelationRejection> {
    let context = g.context;
    if super::v3::example_or_hypothetical(context, subject, object) {
        return Err(RelationRejection::ExampleOrHypothetical);
    }
    let (predicate, subject, object) = match super::v3::author_mention(predicate, subject, object, context) {
        Some((worker, item)) => {
            d.author_mentions_as_work += 1;
            (predicates::WORKS_ON, worker, item)
        }
        None => (predicate, subject, object),
    };
    let (subject, object) = match super::v3::reoriented_owner(predicate, subject, object, context) {
        Some(swapped) => {
            d.reoriented_ownership += 1;
            swapped
        }
        None => (subject, object),
    };
    // v3.4.9: "Morgan owns the pagination validation part of DEMO-1" is not
    // ownership of DEMO-1: it moves to the part's mention, or drops.
    let (mut subject, mut object) = (subject.to_owned(), object.to_owned());
    let (owner, item) = match predicate {
        predicates::RESPONSIBLE_FOR => (subject.clone(), object.clone()),
        predicates::RESPONSIBILITY_OF => (object.clone(), subject.clone()),
        _ => (String::new(), String::new()),
    };
    // v3.5: the source item's body assigning someone a named part ("Morgan
    // owns pagination validation") is not ownership of the source item.
    if !owner.is_empty() && super::v3::names_source(&item, &g.contract.self_names) && super::v3::owns_only_other(context, &owner, &item) {
        return Err(RelationRejection::Unsupported);
    }
    if !owner.is_empty() {
        if let Some(part) = super::v3::owned_part(context, &owner, &item) {
            let Some(mention) = g.mentions.iter().find(|m| m.to_lowercase() == part || m.to_lowercase().contains(&part) && !super::v3::item_like(m)) else {
                return Err(RelationRejection::Unsupported);
            };
            d.narrowed_ownership += 1;
            if predicate == predicates::RESPONSIBLE_FOR { object = mention.clone() } else { subject = mention.clone() }
        }
    }
    let (s, o) = (subject.as_str(), object.as_str());
    // The source's own author working on the source itself (a PR's author
    // to that PR): the author works on their own item.
    let own_item = match predicate {
        predicates::WORKS_ON => Some(s) == g.author && super::v3::names_source(o, &g.contract.self_names),
        predicates::WORKED_ON_BY => Some(o) == g.author && super::v3::names_source(s, &g.contract.self_names),
        _ => false,
    };
    if super::v3::authorship_drop(predicate, s, o, context, own_item) {
        return Err(RelationRejection::Authorship);
    }
    if super::v3::header_only(predicate, s, o, context) {
        return Err(RelationRejection::Header);
    }
    let source_item = match predicate {
        predicates::MENTIONS => super::v3::names_source(s, &g.contract.self_names),
        predicates::MENTIONED_BY => super::v3::names_source(o, &g.contract.self_names),
        _ => false,
    };
    if super::v3::mention_not_reference(predicate, s, o, context, source_item) {
        return Err(RelationRejection::NotAReference);
    }
    let unsupported = if g.change {
        super::v3::change_edge_unsupported(predicate, s, o, context)
    } else {
        super::v3::relation_unsupported(predicate, s, o, context, g.contract.code, own_item, g.kind)
    };
    if unsupported {
        return Err(RelationRejection::Unsupported);
    }
    Ok((predicate, subject, object))
}

// ── Reef binding with Jev ─────────────────────────────────────────────────

/// (mention, entity type) — one binding decision per window.
pub(super) type MentionKey = (String, &'static str);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JevBinding {
    Existing(Uuid),
    New,
}

/// Jev's accepted answers for one window. A mention without an entry uses the
/// fallback ladder.
#[derive(Debug, Default)]
pub(super) struct BindingPlan {
    pub decisions: HashMap<MentionKey, JevBinding>,
}

pub(super) struct PendingQuestion {
    key: MentionKey,
    evidence: String,
    candidates: Vec<NearCandidate>,
}

fn entity_key(entity: &EntityMention) -> MentionKey {
    (entity.mention.clone(), entity.entity_type.as_str())
}

/// Mentions the ladder cannot bind but the graph holds candidates for.
/// Read-only; runs in a short transaction before the model-free persist.
pub(super) async fn pending_questions(
    store: &dyn Store,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    atoms: &[ValidatedAtom],
    worker: &'static str,
    model: &str,
) -> Result<Vec<PendingQuestion>, StoreError> {
    let mut seen = HashSet::new();
    let mut questions = Vec::new();
    'atoms: for atom in atoms {
        for entity in &atom.entities {
            let key = entity_key(entity);
            if !seen.insert(key.clone()) {
                continue;
            }
            let node_type = super::map_node_type(entity.entity_type.as_str());
            let ctx = super::reef_ctx(version_id, entity, worker, model);
            let resolution = store
                .resolve_reef_ref_scoped(tx, graph_id, node_type, &entity.mention, &ctx)
                .await?;
            let mut candidates = match resolution {
                crate::store::ReefResolution::NearMatch { candidates } => candidates,
                crate::store::ReefResolution::Unknown
                    if exact_title_tx(tx, graph_id, node_type, &entity.mention, entity.entity_type.as_str(), version_id)
                        .await?
                        .is_some() =>
                {
                    continue;
                }
                crate::store::ReefResolution::Unknown => Vec::new(),
                _ => continue,
            };
            for extra in crate::store::identity_candidates_tx(
                tx, graph_id, node_type, &entity.mention, Some(entity.entity_type.as_str()), JEV_MAX_CANDIDATES,
            )
            .await?
            {
                if !candidates.iter().any(|c| c.node_id == extra.node_id) {
                    candidates.push(extra);
                }
            }
            let ids: Vec<Uuid> = candidates.iter().map(|c| c.node_id).collect();
            let visible: Vec<Uuid> = sqlx::query_scalar(&format!(
                "SELECT n.node_id FROM nodes n WHERE n.node_id = ANY($1) AND {}",
                visible_to_capture(2)
            ))
            .bind(&ids)
            .bind(version_id)
            .fetch_all(&mut **tx)
            .await?;
            candidates.retain(|c| visible.contains(&c.node_id));
            candidates.truncate(JEV_MAX_CANDIDATES);
            if candidates.is_empty() {
                continue;
            }
            questions.push(PendingQuestion {
                key,
                evidence: atom.evidence_quote.chars().take(600).collect(),
                candidates,
            });
            if questions.len() >= JEV_MAX_QUESTIONS {
                break 'atoms;
            }
        }
    }
    Ok(questions)
}

/// Up to two recent statements per candidate reef, so Jev sees what each
/// candidate is about rather than a bare title.
pub(super) async fn candidate_context_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    questions: &[PendingQuestion],
) -> Result<HashMap<Uuid, Vec<String>>, sqlx::Error> {
    let ids: Vec<Uuid> = questions.iter().flat_map(|q| q.candidates.iter().map(|c| c.node_id)).collect();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT node_id, statement FROM (SELECT l.node_id, a.statement, \
                row_number() OVER (PARTITION BY l.node_id ORDER BY a.created_at DESC, a.semantic_atom_id) AS rank \
           FROM node_semantic_atoms l JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id \
          WHERE l.graph_id=$1 AND l.node_id=ANY($2)) ranked WHERE rank<=2",
    )
    .bind(graph_id)
    .bind(&ids)
    .fetch_all(&mut **tx)
    .await?;
    let mut out: HashMap<Uuid, Vec<String>> = HashMap::new();
    for (node, statement) in rows {
        out.entry(node).or_default().push(statement.chars().take(300).collect());
    }
    Ok(out)
}

fn question_id(index: usize) -> String {
    format!("m{index:02}")
}

/// One Jev request for the window: one Choice per pending mention, options
/// `c0..cN` (the candidates) and `new`.
pub(super) fn jev_request(
    questions: &[PendingQuestion],
    context: &HashMap<Uuid, Vec<String>>,
    source_type: &str,
    author: Option<&str>,
    model: String,
) -> DecisionRequest {
    let mut mentions = serde_json::Map::new();
    let mut asked = BTreeMap::new();
    for (index, question) in questions.iter().enumerate() {
        let id = question_id(index);
        let mut candidates = serde_json::Map::new();
        let mut criteria = BTreeMap::new();
        for (c, candidate) in question.candidates.iter().enumerate() {
            let key = format!("c{c}");
            candidates.insert(key.clone(), json!({
                "title": candidate.title,
                "aliases": candidate.aliases.iter().take(5).collect::<Vec<_>>(),
                "corals": candidate.coral_count,
                "recent_statements": context.get(&candidate.node_id).cloned().unwrap_or_default(),
            }));
            criteria.insert(key, Some(candidate.title.chars().take(120).collect::<String>()));
        }
        criteria.insert(
            "new".to_string(),
            Some("None of the candidates: a different entity, or the evidence cannot tell".to_string()),
        );
        mentions.insert(id.clone(), json!({
            "mention": question.key.0,
            "type": question.key.1,
            "evidence": question.evidence,
            "candidates": candidates,
        }));
        asked.insert(id.clone(), JevQuestion::Choice {
            instructions: json!(format!(
                "Does mention {id} in this source name the same real-world {} as one of its candidates? \
                 Answer the candidate key, or `new` when it is a different {} or the evidence cannot tell.",
                question.key.1, question.key.1
            )),
            criteria,
        });
    }
    DecisionRequest {
        state: json!({
            "task": "reef_identity",
            "source": {"source_type": source_type, "author": author},
            "mentions": mentions,
        }),
        model,
        questions: asked,
        op: JevOp::Judge,
    }
}

/// Keep only confident answers that name a real option.
pub(super) fn plan_from_answers(
    questions: &[PendingQuestion],
    response: &crate::decision::DecisionResponse,
) -> BindingPlan {
    let mut plan = BindingPlan::default();
    for (index, question) in questions.iter().enumerate() {
        let Some(answer) = response.answers.get(&question_id(index)) else { continue };
        let Some(choice) = answer.as_choice() else { continue };
        if answer.choice_confidence() < JEV_MIN_CONFIDENCE {
            continue;
        }
        let decision = if choice == "new" {
            JevBinding::New
        } else {
            let Some(candidate) = choice
                .strip_prefix('c')
                .and_then(|n| n.parse::<usize>().ok())
                .and_then(|n| question.candidates.get(n))
            else {
                continue;
            };
            JevBinding::Existing(candidate.node_id)
        };
        plan.decisions.insert(question.key.clone(), decision);
    }
    plan
}

/// Jev's pick is re-checked inside the persist transaction: the reef must
/// still exist in this graph with the mention's node type, visible to the
/// capture.
pub(super) async fn jev_target_valid_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    node_type: &str,
    node_id: Uuid,
    version_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT EXISTS(SELECT 1 FROM nodes n WHERE n.node_id=$1 AND n.graph_id=$2 AND n.type=$3 \
           AND n.body->>'lifecycle' IS DISTINCT FROM 'evidence_retracted' AND {})",
        visible_to_capture(4)
    ))
    .bind(node_id)
    .bind(graph_id)
    .bind(node_type)
    .bind(version_id)
    .fetch_one(&mut **tx)
    .await
}

/// A reef a capture may bind: graph-level (no owner), shared, or the capture
/// actor's own private reef. `version` is the bind index of the capture's
/// source version id.
fn visible_to_capture(version: u8) -> String {
    format!(
        "(n.owner_id IS NULL OR n.scope <> 'private' OR n.owner_id = \
          (SELECT v.ingested_by FROM source_versions v WHERE v.version_id = ${version}))"
    )
}

/// Deterministic binding for a mention the ladder left `Unknown`.
///
/// Binds when a reef in the graph has the mention as its title
/// (case-insensitive), the same node type and the same known entity type, is
/// visible to the capture, is not retracted, and has no active human identity
/// correction. Single-token names bind like any other.
///
/// Several such reefs are the per-version copies of one name: the self reef
/// wins, else the oldest. When any two of them are recorded as distinct, the
/// name covers different entities and nothing binds here; Jev decides.
/// Distinct means a ruling that they differ (an active NEGATIVE judgement
/// that is not an unsure model verdict, or a `non_supersession_pair`) or
/// conflicting decisive identifiers. An unsure verdict is not a ruling:
/// minting another copy for it would only add a third unresolved reef.
///
/// That ruling rule is for `person` mentions only. Every other kind binds by
/// [`crate::store::door_exact_title_reef_tx`] within its title identity
/// class (#1516; topic and project share one): only conflicting decisive
/// identifiers keep same-title copies apart.
pub(crate) async fn exact_title_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    node_type: &str,
    mention: &str,
    entity_type: &str,
    version_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    // #1516: a non-person entity is its title within its identity class
    // (topic and project share one). The store's rule ignores model
    // verdicts that same-title copies differ; the person rule below keeps
    // them.
    if node_type == "entity" {
        if let Some(class) = crate::store::door_title_identity_class(Some(entity_type)) {
            return crate::store::door_exact_title_reef_tx(tx, graph_id, node_type, mention, class, Some(version_id)).await;
        }
    }
    let candidates: Vec<Uuid> = sqlx::query_scalar(&format!(
        "SELECT n.node_id FROM nodes n JOIN graphs g ON g.id = n.graph_id \
          WHERE n.graph_id = $1 AND n.type = $2 AND lower(n.title) = lower($3) \
            AND COALESCE(n.entity_type, n.body->>'entity_type') = $4 \
            AND n.body->>'lifecycle' IS DISTINCT FROM 'evidence_retracted' \
            AND {} \
            AND NOT EXISTS(SELECT 1 FROM reef_identity_corrections c \
                 WHERE c.graph_id = n.graph_id AND c.node_id = n.node_id AND c.state = 'active') \
          ORDER BY g.self_node_id IS NOT DISTINCT FROM n.node_id DESC, n.created_at, n.node_id \
          LIMIT 50",
        visible_to_capture(5)
    ))
    .bind(graph_id)
    .bind(node_type)
    .bind(mention.trim())
    .bind(entity_type)
    .bind(version_id)
    .fetch_all(&mut **tx)
    .await?;
    if candidates.len() > 1 {
        let distinct: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM reef_judgement j WHERE j.graph_id = $1 AND j.annulled_at IS NULL \
                 AND j.verdict = 'negative' AND (j.expires_at IS NULL OR j.expires_at > now()) \
                 AND (j.source <> 'llm' OR j.evidence->>'llm_verdict' = 'different') \
                 AND j.node_a_id = ANY($2) AND j.node_b_id = ANY($2)) \
             OR EXISTS(SELECT 1 FROM non_supersession_pair p WHERE p.node_a_id = ANY($2) AND p.node_b_id = ANY($2))",
        )
        .bind(graph_id)
        .bind(&candidates)
        .fetch_one(&mut **tx)
        .await?;
        if distinct || crate::workers::entity_resolution::identities_conflict_tx(tx, &candidates).await? {
            return Ok(None);
        }
    }
    Ok(candidates.first().copied())
}

// ── Writes ─────────────────────────────────────────────────────────────────

fn assertion(semantic_atom_id: Uuid, validity: Option<&Validity>) -> Value {
    json!({
        "semantic_atom_id": semantic_atom_id.to_string(),
        "valid_from": validity.and_then(|v| v.from),
        "valid_until": validity.and_then(|v| v.until),
        "validity_quote": validity.map(|v| v.quote.as_str()),
    })
}

/// Write one atom's stated relations. Bind-or-drop: both endpoints must have
/// bound to exactly one reef in this window (a mention, or a named endpoint
/// the caller resolved graph-wide), or the relation is dropped. Returns
/// (edges written, relations dropped unbound: logged per window as
/// `unbound_relations`).
/// The forward edge and its symmetric/inverse counterpart follow the legacy
/// writer's rule (`predicates::skip_inverse_materialization`).
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_relations_tx(
    store: &dyn Store,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    semantic_atom_id: Uuid,
    relations: &[ValidatedRelation],
    bound: &HashMap<String, Option<Uuid>>,
    chunk_atoms: &[Uuid],
    extraction_version: &'static str,
) -> Result<(usize, usize), StoreError> {
    let mut written = 0;
    let mut unbound = 0;
    let chunks: Vec<String> = chunk_atoms.iter().take(MAX_EDGE_CHUNKS).map(Uuid::to_string).collect();
    for relation in relations {
        let (Some(Some(src)), Some(Some(dst))) = (bound.get(&relation.subject), bound.get(&relation.object)) else {
            unbound += 1;
            continue;
        };
        if src == dst {
            continue;
        }
        let reverse = if predicates::skip_inverse_materialization(relation.predicate) {
            None
        } else if predicates::is_symmetric(relation.predicate) {
            Some(relation.predicate)
        } else {
            predicates::inverse(relation.predicate)
        };
        for (from, predicate, to, derived) in std::iter::once((*src, relation.predicate, *dst, false))
            .chain(reverse.map(|p| (*dst, p, *src, true)))
        {
            let mut provenance = json!({
                "semantic_atom_ids": [semantic_atom_id.to_string()],
                "atom_ids": chunks,
                "source_version_id": version_id.to_string(),
                "extraction_version": extraction_version,
                "assertions": [assertion(semantic_atom_id, relation.validity.as_ref())],
            });
            if derived {
                provenance["derived"] = json!("inverse");
            }
            let edge = store
                .upsert_relationship_edge_scoped(
                    tx, graph_id, from, predicate, to, EdgeOrigin::Stated, EdgeStatus::Confirmed,
                    EDGE_CONFIDENCE, provenance,
                )
                .await?;
            if let Some(validity) = &relation.validity {
                // The latest explicit statement sets the edge's validity.
                // Judging which statement wins is supersession (plan PR 9).
                sqlx::query("UPDATE relationship_edge SET valid_from=$2, valid_until=$3 WHERE edge_id=$1")
                    .bind(edge.edge_id)
                    .bind(validity.from)
                    .bind(validity.until)
                    .execute(&mut **tx)
                    .await?;
            }
            written += 1;
        }
    }
    Ok((written, unbound))
}

/// On the last window of a re-extracted version, retire the atoms an earlier
/// profile wrote for it so readers do not see each fact twice: v1 atoms under
/// v2, v1 and v2 atoms under v3. An atom that something durable cites (an
/// answer receipt, a temporal record, or a judged or curated supersession
/// decision) is kept. For a v2 atom, the temporal
/// records supersession derived from it (`temporal_extraction_v1`) are not
/// durable citations: they are re-derived from the v3 atoms in the same
/// transaction, and they cascade with the atom.
pub(super) async fn retire_superseded_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    profile: &str,
) -> Result<u64, sqlx::Error> {
    let earlier: &[&str] = if profile == super::v3::EXTRACTION_VERSION_V3 {
        &[super::EXTRACTION_VERSION, EXTRACTION_VERSION_V2]
    } else {
        &[super::EXTRACTION_VERSION]
    };
    Ok(sqlx::query(
        "DELETE FROM semantic_atoms a WHERE a.graph_id=$1 AND a.source_version_id=$2 \
           AND a.extraction_version=ANY($3) \
           AND NOT EXISTS(SELECT 1 FROM temporal_evidence_records e WHERE e.graph_id=a.graph_id AND e.semantic_atom_id=a.semantic_atom_id \
                 AND (a.extraction_version<>$4 OR e.recording_method<>'temporal_extraction_v1')) \
           AND NOT EXISTS(SELECT 1 FROM fact_supersession_edges f WHERE f.graph_id=a.graph_id \
                 AND a.semantic_atom_id IN (f.superseder_atom_id,f.superseded_atom_id) \
                 AND ((f.rule='judge' AND f.status<>'ambiguous') OR f.status IN ('curator','rejected','unresolved'))) \
           AND NOT EXISTS(SELECT 1 FROM answer_support_receipts r WHERE r.graph_id=a.graph_id \
                 AND r.support @> jsonb_build_array(jsonb_build_object('kind','source_passage','semantic_atom_id',a.semantic_atom_id::text)))",
    )
    .bind(graph_id)
    .bind(version_id)
    .bind(earlier)
    .bind(EXTRACTION_VERSION_V2)
    .execute(&mut **tx)
    .await?
    .rows_affected())
}

/// The earlier pins standing adoption may move forward into `profile`.
pub(crate) fn repinnable_into(profile: &str) -> &'static [&'static str] {
    match profile {
        EXTRACTION_VERSION_V2 => &[super::EXTRACTION_VERSION],
        super::v3::EXTRACTION_VERSION_V3 => &[super::EXTRACTION_VERSION, EXTRACTION_VERSION_V2],
        _ => &[],
    }
}

/// Repin one version forward (v1 -> v2, v1 -> v3, v2 -> v3; tenant 0180 /
/// control 0348), in the caller's transaction (which holds the version's
/// advisory and row locks). Refuses while an owner under `from` is still in
/// flight, while an extraction job is pending or running, or while the
/// version backs an active selected publication. `reason` becomes the
/// blocked status adoption looks for.
pub(crate) async fn repin_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    from: &str,
    to: &str,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    repin_where(tx, graph_id, version_id, from, to, reason, false).await
}

/// [`repin_tx`] for standing adoption: only a version still `blocked`
/// (never extracted under its pin, or held). A version that is `done`,
/// `pending` or `failed` is never moved by adoption, even if it finished
/// between the sweep's candidate read and this locked write. Re-extracting a
/// done version is only ever the explicit operator request
/// (`source_reextract`, `reextract_source_atoms_v2 --to v3`).
pub(crate) async fn repin_blocked_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    from: &str,
    to: &str,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    repin_where(tx, graph_id, version_id, from, to, reason, true).await
}

async fn repin_where(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    from: &str,
    to: &str,
    reason: &str,
    only_blocked: bool,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(
        "UPDATE source_versions v SET extraction_version=$3, next_start_char=0, extraction_status='blocked', \
                extraction_error=CASE WHEN v.extraction_status='blocked' THEN v.extraction_error ELSE $5 END \
          WHERE v.graph_id=$1 AND v.version_id=$2 AND v.extraction_version=$4 \
            AND (NOT $6 OR v.extraction_status='blocked') \
            AND NOT EXISTS(SELECT 1 FROM corpus_enrichment_units u WHERE u.graph_id=v.graph_id \
                  AND COALESCE(u.promoted_version_id,u.holding_id)=v.version_id AND u.extraction_version=$4 \
                  AND u.execution_owner AND u.state NOT IN ('processed','failed','excluded','unavailable')) \
            AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.payload->>'source_version_id'=v.version_id::text \
                  AND j.status::text IN ('pending','running')) \
            AND NOT EXISTS(SELECT 1 FROM selected_evidence_publications p WHERE p.status='active' \
                  AND (p.source_version_id=v.version_id OR p.target_source_version_id=v.version_id))",
    )
    .bind(graph_id)
    .bind(version_id)
    .bind(to)
    .bind(from)
    .bind(reason)
    .bind(only_blocked)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if updated != 1 {
        return Ok(false);
    }
    sqlx::query_scalar::<_, bool>("SELECT corpus_capture_admission_follow_pin($1,$2)")
        .bind(graph_id)
        .bind(version_id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(text: &str) -> Window<'_> {
        super::super::window(text, 0).unwrap()
    }

    fn atom(quote: &str, entities: Value, relations: Value) -> Value {
        json!({"kind":"fact","text":"A fact.","evidence_quote":quote,"entities":entities,
            "relations":relations,"valid_from":null,"valid_until":null,"validity_quote":null})
    }

    fn rel(s: &str, p: &str, o: &str) -> Value {
        json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null})
    }

    fn validate(output: Value, source: &str, author: Option<&str>) -> (Vec<ValidatedAtom>, crate::llm::ExtractionDiagnostics) {
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let atoms = validate_output_v2(&output.to_string(), &window(source), author, &mut d).unwrap();
        (atoms, d)
    }

    #[test]
    fn prompt_lists_exactly_the_extractable_vocabulary() {
        let listed: String = SYSTEM_PROMPT_V2
            .split("Allowed predicates (use ONLY these; direction is subject -> object):\n")
            .nth(1).unwrap().split("\nFor works_on").next().unwrap().replace('\n', " ");
        let listed: Vec<&str> = listed.split(',').map(str::trim).collect();
        assert_eq!(listed, predicates::extractable_predicates());
        let schema = tool_schema_v2().schema;
        let offered = &schema["properties"]["atoms"]["items"]["properties"]["relations"]["items"]["properties"]["predicate"]["enum"];
        assert_eq!(offered, &json!(predicates::extractable_predicates()));
        for retired in ["related_to", "knows", "colleague_of", "expert_in", "expertise_of"] {
            assert!(!offered.as_array().unwrap().contains(&json!(retired)), "{retired}");
        }
    }

    #[test]
    fn relations_keep_only_extractable_predicates_between_window_mentions() {
        let source = "Ana manages Bo on Atlas. Cy works at Initech.";
        let output = json!({"atoms":[
            atom("Ana manages Bo on Atlas.", json!([{"mention":"Ana","type":"person"},{"mention":"Bo","type":"person"},{"mention":"Atlas","type":"project"}]),
                json!([rel("Ana","manages","Bo"), rel("Ana","expert_in","Atlas"), rel("Ana","related_to","Bo"),
                       rel("Ana","married_to","Bo"), rel("Ana","works_on","Zed"), rel("Bo","works_on","Atlas"),
                       rel("Bo","works_on","Atlas"), rel("Ana","manages","Ana"), {"subject":"Ana"}])),
            atom("Cy works at Initech.", json!([{"mention":"Cy","type":"person"},{"mention":"Initech","type":"org"}]),
                json!([rel("Cy","works_at","Initech"), rel("Cy","peer_of","Ana")])),
        ]});
        let (atoms, d) = validate(output, source, None);
        let kept: Vec<(String, &str, String)> = atoms.iter()
            .flat_map(|a| a.relations.iter().map(|r| (r.subject.clone(), r.predicate, r.object.clone()))).collect();
        assert_eq!(kept, vec![
            ("Ana".into(), "manages", "Bo".into()),
            ("Bo".into(), "works_on", "Atlas".into()),
            ("Cy".into(), "works_at", "Initech".into()),
            // Endpoints may be any entity mention of the window, across atoms.
            ("Cy".into(), "peer_of", "Ana".into()),
        ]);
        let r = d.relations.unwrap();
        assert_eq!(r.accepted_relations, 4);
        assert_eq!(r.rejected_relations[&RelationRejection::Predicate], 3);
        assert_eq!(r.rejected_relations[&RelationRejection::Endpoint], 1);
        assert_eq!(r.rejected_relations[&RelationRejection::SelfLoop], 1);
        // The 9th relation is over the per-atom limit before its shape is read.
        assert_eq!(r.rejected_relations[&RelationRejection::Limit], 1);
        assert_eq!(atoms.len(), 2, "a bad relation never rejects its atom");
        let (_, d) = validate(json!({"atoms":[atom("Cy works at Initech.",
            json!([{"mention":"Cy","type":"person"},{"mention":"Initech","type":"org"}]),
            json!([{"subject":"Cy"}]))]}), source, None);
        assert_eq!(d.relations.unwrap().rejected_relations[&RelationRejection::Schema], 1);
    }

    #[test]
    fn first_person_relations_bind_the_author_as_a_person_mention() {
        let source = "I will own the Atlas migration.";
        let output = json!({"atoms":[atom(source, json!([{"mention":"Atlas","type":"project"}]),
            json!([rel("Dana Kim","responsible_for","Atlas")]))]});
        let (atoms, _) = validate(output.clone(), source, Some("Dana Kim"));
        assert_eq!(atoms[0].relations[0].subject, "Dana Kim");
        assert!(atoms[0].entities.contains(&EntityMention { mention: "Dana Kim".into(), entity_type: EntityType::Person }));
        // Without a known author the same response has no admissible subject.
        let (atoms, d) = validate(output, source, None);
        assert!(atoms[0].relations.is_empty());
        assert_eq!(d.relations.unwrap().rejected_relations[&RelationRejection::Endpoint], 1);
        // An author entity listed by the model is admissible without appearing in the quote.
        let listed = json!({"atoms":[atom(source, json!([{"mention":"Dana Kim","type":"person"}]), json!([]))]});
        assert_eq!(validate(listed.clone(), source, Some("Dana Kim")).0.len(), 1);
        // Without an author the mention is unsupported: it is dropped, the atom stays.
        let (atoms, d) = validate(listed, source, None);
        assert_eq!((atoms.len(), atoms[0].entities.len()), (1, 0));
        assert_eq!(d.dropped_entities[&crate::llm::AtomRejection::EntityUnsupported], 1);
    }

    #[test]
    fn v2_drops_bad_items_individually_and_fails_only_an_all_bad_window() {
        use crate::llm::AtomRejection;
        let source = "Ana manages Bo on Atlas.";
        let output = json!({"atoms":[
            {"kind":"fact","text":"Ana manages Bo.","evidence_quote":"Ana manages Bo","extra":true,
             "entities":[{"mention":"Ana","type":"person"},{"mention":"Bo","type":"person"},
                         {"mention":"Mallory","type":"person"},{"mention":"Atlas","type":"galaxy"}],
             "relations":[rel("Ana","manages","Bo"), rel("Ana","manages","Mallory")],
             "valid_from":null,"valid_until":null,"validity_quote":null},
            // Relations and validity omitted or null: none.
            {"kind":"fact","text":"Work on Atlas.","evidence_quote":"on Atlas.","entities":[{"mention":"Atlas","type":"project"}],"relations":null},
            atom("absent quote", json!([]), json!([])),
        ]});
        let (atoms, d) = validate(output, source, None);
        assert_eq!(atoms.len(), 2, "one bad quote costs one atom");
        assert_eq!(atoms[0].entities.iter().map(|e| e.mention.as_str()).collect::<Vec<_>>(), ["Ana", "Bo"]);
        assert_eq!(atoms[0].relations.len(), 1, "a relation to a dropped mention is dropped on its own");
        assert_eq!(d.dropped_entities[&AtomRejection::EntityUnsupported], 1);
        assert_eq!(d.dropped_entities[&AtomRejection::Schema], 1);
        assert_eq!(d.rejected_atoms[&AtomRejection::QuoteMissing], 1);
        assert_eq!(d.relations.unwrap().rejected_relations[&RelationRejection::Endpoint], 1);
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let all_bad = json!({"atoms":[atom("absent", json!([]), json!([])), {}]});
        assert_eq!(validate_output_v2(&all_bad.to_string(), &window(source), None, &mut d).unwrap_err(), super::super::NO_VALID_ATOMS);
        assert!(matches!(d.envelope_error, Some(crate::llm::ExtractionEnvelopeError::NoValidAtoms)));
        let mut d = crate::llm::ExtractionDiagnostics::default();
        assert!(validate_output_v2(r#"{"atoms":[]}"#, &window(source), None, &mut d).unwrap().is_empty(), "an honest empty answer completes");
        // Over the cap: the first MAX_ATOMS valid atoms are kept.
        let text: String = (0..50).map(|i| format!("Row {i:02} is here.\n")).collect();
        let many: Vec<Value> = (0..50).map(|i| atom(&format!("Row {i:02} is here."), json!([]), json!([]))).collect();
        let (atoms, d) = validate(json!({"atoms":many}), &text, None);
        assert_eq!((atoms.len(), d.capped_atoms), (MAX_ATOMS, 50 - MAX_ATOMS as u32));
    }

    #[test]
    fn validity_needs_an_exact_anchor_and_an_ordered_range() {
        let evidence = "Bo leads Atlas from 2026-03-01 until 2026-06-30.";
        let ok = validity(Some("2026-03-01"), Some("2026-06-30"), Some("from 2026-03-01 until 2026-06-30"), evidence).unwrap().unwrap();
        assert_eq!(ok.from.unwrap().to_rfc3339(), "2026-03-01T00:00:00+00:00");
        assert_eq!(ok.until.unwrap().to_rfc3339(), "2026-06-30T00:00:00+00:00");
        assert_eq!(validity(None, None, None, evidence), Ok(None));
        assert_eq!(validity(Some(""), Some(" "), Some(""), evidence), Ok(None));
        assert!(validity(Some("2026-03-01"), None, None, evidence).is_err(), "no anchor");
        assert!(validity(Some("2026-03-01"), None, Some("in March"), evidence).is_err(), "anchor not in evidence");
        assert!(validity(Some("March 1"), None, Some("from 2026-03-01"), evidence).is_err(), "not ISO");
        assert!(validity(Some("2026-06-30"), Some("2026-03-01"), Some("until 2026-06-30"), evidence).is_err(), "inverted");
        assert!(validity(None, None, Some("until 2026-06-30"), evidence).is_err(), "quote without a date");
        assert!(validity(Some("0026-03-01"), None, Some("from 2026-03-01"), evidence).is_err(), "implausible year");
        let rfc = validity(None, Some("2026-06-30T17:00:00-05:00"), Some("until 2026-06-30"), evidence).unwrap().unwrap();
        assert_eq!(rfc.until.unwrap().to_rfc3339(), "2026-06-30T22:00:00+00:00");
    }

    #[test]
    fn atom_and_relation_validity_are_kept_or_dropped_independently() {
        let source = "Ana manages Bo until 2026-12-31.";
        let mut a = atom(source, json!([{"mention":"Ana","type":"person"},{"mention":"Bo","type":"person"}]),
            json!([{"subject":"Ana","predicate":"manages","object":"Bo","valid_from":null,"valid_until":"2026-12-31","validity_quote":"until 2026-12-31"}]));
        a["valid_until"] = json!("2027-01-01");
        a["validity_quote"] = json!("next year");
        let (atoms, d) = validate(json!({"atoms":[a]}), source, None);
        assert_eq!(atoms.len(), 1);
        assert_eq!(atoms[0].validity, None, "unanchored atom validity is dropped");
        let v = atoms[0].relations[0].validity.as_ref().unwrap();
        assert_eq!((v.from, v.until.map(|u| u.date_naive().to_string()), v.quote.as_str()), (None, Some("2026-12-31".into()), "until 2026-12-31"));
        assert_eq!(d.relations.unwrap().dropped_validity, 1);
    }

    #[test]
    fn relation_limits_and_duplicates() {
        let source = "Ana, Bo, Cy, Di, Ed, Fy, Gu, Hal and Io all report to Jo.";
        let names = ["Ana", "Bo", "Cy", "Di", "Ed", "Fy", "Gu", "Hal", "Io", "Jo"];
        let entities: Vec<Value> = names[..8].iter().map(|n| json!({"mention":n,"type":"person"})).collect();
        let relations: Vec<Value> = names[1..].iter().map(|n| rel("Ana", "peer_of", n)).collect();
        let (atoms, d) = validate(json!({"atoms":[atom(source, json!(entities), json!(relations))]}), source, None);
        // Io is not an entity mention (8 entity cap) but the text names it, so
        // its relation is kept for graph-wide binding; the 9th relation (Jo)
        // is over the limit.
        assert_eq!(atoms[0].relations.len(), 8);
        let r = d.relations.unwrap();
        assert_eq!(r.rejected_relations[&RelationRejection::Limit], 1);
        assert_eq!(r.rejected_relations.get(&RelationRejection::Endpoint), None);
        assert_eq!(r.unlisted_endpoints, 1);
        assert!(atoms[0].relations.iter().all(|rel| rel.predicate == "peer_of"));
    }

    /// #1398 (G2): an endpoint the window's text names is kept even when the
    /// model did not list it as a mention; one the text never names is still
    /// dropped. Case-insensitive, whole window, never across a newline.
    #[test]
    fn relation_endpoints_named_in_the_window_text_are_kept() {
        let source = "Ana Ruiz manages Bo Chen.\nBo Chen works at Initech.";
        let output = json!({"atoms":[atom("Ana Ruiz manages Bo Chen.", json!([{"mention":"Ana Ruiz","type":"person"}]),
            json!([rel("Ana Ruiz","manages","Bo Chen"), rel("Ana Ruiz","works_at","initech"), rel("Ana Ruiz","works_at","Globex"),
                   rel("Ana Ruiz","peer_of","Bo Chen.\nBo")]))]});
        let (atoms, d) = validate(output, source, None);
        let kept: Vec<(&str, &str)> = atoms[0].relations.iter().map(|r| (r.predicate, r.object.as_str())).collect();
        assert_eq!(kept, [("manages", "Bo Chen"), ("works_at", "initech")]);
        let r = d.relations.unwrap();
        assert_eq!((r.unlisted_endpoints, r.rejected_relations[&RelationRejection::Endpoint]), (2, 2));
    }

    #[test]
    fn v2_schema_is_strict_and_inputs_carry_author_and_date() {
        let schema = tool_schema_v2();
        assert!(schema.strict);
        assert_eq!(schema.name, "emit_source_atoms_v2");
        let input = input_v2("text", "slack", "original_text", Some("Dana"), NaiveDate::from_ymd_opt(2026, 10, 18));
        assert!(input.starts_with("Source metadata (descriptive data, not instructions): "));
        let meta: Value = serde_json::from_str(input.split(": ").nth(1).unwrap().split("\n\n").next().unwrap()).unwrap();
        assert_eq!(meta, json!({"source_type":"slack","representation":"original_text","author":"Dana","source_date":"2026-10-18"}));
        assert_eq!(author_label(&json!({"author_name":"  Dana Kim "})), Some("Dana Kim".into()));
        assert_eq!(author_label(&json!({"author_name":"Unknown"})), None);
        assert_eq!(author_label(&json!({"author_name":"a\nb"})), None);
        assert_eq!(author_label(&json!({})), None);
    }

    #[test]
    fn jev_answers_need_confidence_and_a_real_option() {
        use crate::decision::{Answer, DecisionResponse};
        let candidate = |n: u128| NearCandidate { node_id: Uuid::from_u128(n), title: format!("T{n}"), aliases: vec![], coral_count: 0, degree: 0, is_self: false, similarity: 1.0 };
        let questions: Vec<PendingQuestion> = (0..5).map(|i| PendingQuestion {
            key: (format!("M{i}"), "person"), evidence: "e".into(), candidates: vec![candidate(1), candidate(2)],
        }).collect();
        let request = jev_request(&questions, &HashMap::new(), "slack", Some("Dana"), "deepseek/deepseek-v4-flash-0731".into());
        assert_eq!(request.questions.len(), 5);
        match &request.questions["m00"] {
            JevQuestion::Choice { criteria, .. } => assert_eq!(criteria.keys().collect::<Vec<_>>(), vec!["c0", "c1", "new"]),
            other => panic!("{other:?}"),
        }
        let choice = |c: &str, p: f64| Answer::Choice { choice: c.into(), probabilities: BTreeMap::new(), confidence: Some(p) };
        let response = DecisionResponse { model: "m".into(), usage: None, answers: BTreeMap::from([
            ("m00".into(), choice("c1", 0.9)),
            ("m01".into(), choice("new", 0.95)),
            ("m02".into(), choice("c0", 0.4)),
            ("m03".into(), choice("c9", 0.99)),
        ])};
        let plan = plan_from_answers(&questions, &response);
        assert_eq!(plan.decisions.get(&("M0".to_string(), "person")), Some(&JevBinding::Existing(Uuid::from_u128(2))));
        assert_eq!(plan.decisions.get(&("M1".to_string(), "person")), Some(&JevBinding::New));
        assert_eq!(plan.decisions.len(), 2, "unsure, invalid and missing answers fall back to the ladder");
    }

    #[test]
    fn active_profile_switch_defaults_to_v2_and_never_selects_v1() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var(PROFILE_ENV);
        assert_eq!(active_source_profile(), "source-atoms-v2");
        std::env::set_var(PROFILE_ENV, "source-atoms-v1");
        assert_eq!(active_source_profile(), "source-atoms-v2", "v1 is retired: no new pin");
        std::env::set_var(PROFILE_ENV, " source-atoms-v3 ");
        assert_eq!(active_source_profile(), "source-atoms-v3", "v3 is selectable, never the default");
        std::env::set_var(PROFILE_ENV, "garbage");
        assert_eq!(active_source_profile(), "source-atoms-v2");
        std::env::remove_var(PROFILE_ENV);
    }
}
