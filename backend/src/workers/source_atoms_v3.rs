//! Source-atoms v3: change statements, resolved dates and single-valued
//! relationships, the facts supersession needs.
//!
//! v3 is v2 (`source_atoms_v2.rs`) plus, per atom, `changes`: a statement
//! that ONE sentence makes about a value replacing another ("deploy.yml
//! previously fired on CI and is now manual dispatch"). A pair-of-facts rule
//! cannot see those; v3 records them so supersession
//! ([`crate::fact_supersession::changes`]) can turn each into an explicit
//! transition. The prompt also resolves relative dates against the source's
//! `occurred_at` and asks for single-valued relationships (owner, manager,
//! employer, lead) wherever the text states them.
//!
//! Validation is v2's ([`super::v2::validate_structured`]) with two
//! additions, both item-level: a bad change drops alone, and a date stated
//! without a year must resolve near the source date ([`date_supported`]) or
//! it drops alone. Neither ever rejects its atom or the window.
//!
//! The predicate vocabulary is unchanged (`predicates::extractable_predicates`).
//! A relational change maps onto an existing single-valued or many-valued
//! predicate ([`RELATIONAL_ATTRIBUTES`]); anything else is a free-text
//! attribute keyed `attr.<name>`.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::v2::{validity, RelationDiagnostics, RelationRejection, ValidatedRelation, Validity};
use super::{EntityMention, EntityType, ValidatedAtom, MAX_ENTITIES};
use crate::relationships::predicates;

pub const EXTRACTION_VERSION_V3: &str = "source-atoms-v3";
pub(super) const SYSTEM_PROMPT_V3: &str = include_str!("../../prompts/extract_source_atoms_v3.txt");
pub(super) const TOOL_NAME_V3: &str = "emit_source_atoms_v3";

/// Changes kept per atom.
pub(crate) const MAX_CHANGES: usize = 2;
const MAX_VALUE_CHARS: usize = 200;
const MAX_ATTRIBUTE_CHARS: usize = 48;
const MAX_CHANGE_QUOTE_CHARS: usize = 4_000;
/// A date whose quote names no year ("since March", "last week") resolves
/// near the day the source was written, or it is not believed.
pub(crate) const RELATIVE_DATE_SPAN_DAYS: i64 = 400;

// ── Attributes ──────────────────────────────────────────────────────────────

/// A change whose values are entities maps onto one stated predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalAttribute {
    /// The prompt's attribute name.
    pub name: &'static str,
    pub predicate: &'static str,
    /// The change's subject is the predicate's subject (else its object).
    pub subject_is_src: bool,
}

/// Each keeps the change's subject as the supersession subject of the
/// predicate's state key (`fact_supersession::relation_key`).
pub const RELATIONAL_ATTRIBUTES: &[RelationalAttribute] = &[
    RelationalAttribute { name: "owner", predicate: predicates::RESPONSIBLE_FOR, subject_is_src: false },
    RelationalAttribute { name: "manager", predicate: predicates::REPORTS_TO, subject_is_src: true },
    RelationalAttribute { name: "employer", predicate: predicates::WORKS_AT, subject_is_src: true },
    RelationalAttribute { name: "lead", predicate: predicates::LEADS_TEAM, subject_is_src: false },
    RelationalAttribute { name: "team", predicate: predicates::MEMBER_OF, subject_is_src: true },
    RelationalAttribute { name: "project", predicate: predicates::WORKS_ON, subject_is_src: true },
];

fn relational(name: &str) -> Option<&'static RelationalAttribute> {
    let canonical = match name {
        "owner" | "maintainer" | "dri" | "responsible_for" | "responsible" => "owner",
        "manager" | "reports_to" => "manager",
        "employer" | "works_at" | "company" => "employer",
        "lead" | "leads_team" | "team_lead" | "team lead" => "lead",
        "team" | "member_of" => "team",
        "project" | "works_on" => "project",
        _ => return None,
    };
    RELATIONAL_ATTRIBUTES.iter().find(|a| a.name == canonical)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeAttribute {
    Relation(&'static RelationalAttribute),
    /// A free-text attribute, keyed `attr.<slug>`.
    Text { name: String, key: String },
}

/// `deploy trigger` -> `deploy_trigger`. Lower-case ASCII letters, digits and
/// single underscores, starting with a letter; `None` when nothing usable
/// remains or it is too long.
fn slug(name: &str) -> Option<String> {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if matches!(c, ' ' | '-' | '_' | '/' | '.') {
            if !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
        } else {
            return None;
        }
    }
    let out = out.trim_end_matches('_').to_owned();
    (out.starts_with(|c: char| c.is_ascii_lowercase()) && out.len() <= MAX_ATTRIBUTE_CHARS).then_some(out)
}

pub(crate) fn parse_attribute(raw: &str) -> Option<ChangeAttribute> {
    let name = raw.trim().to_lowercase();
    if name.is_empty() || name.chars().count() > MAX_ATTRIBUTE_CHARS {
        return None;
    }
    if let Some(attribute) = relational(&name) {
        return Some(ChangeAttribute::Relation(attribute));
    }
    let slug = slug(&name)?;
    Some(ChangeAttribute::Text { key: format!("attr.{slug}"), name })
}

/// A version value: `v702`, `702`, `1.4.2`, `v1.4`.
fn version_like(value: &str) -> bool {
    let v = value.trim().trim_start_matches(['v', 'V']);
    !v.is_empty() && v.chars().next().is_some_and(|c| c.is_ascii_digit())
        && v.chars().all(|c| c.is_ascii_digit() || c == '.')
}

/// Attributes a model puts a deployed version under when it binds the
/// statement to the wrong subject (the PR's status) or a vague attribute.
const VERSION_ATTRIBUTES: &[&str] = &[
    "attr.status", "attr.state", "attr.version", "attr.release", "attr.deploy", "attr.deployment", "attr.deployed_version",
    "attr.live_version", "attr.current_version", "attr.release_version",
];

/// v3.5.1: the deployed thing a version statement names, and the `version`
/// attribute, when the model bound the version to something else. The
/// quote must say `<thing> is now / is on / now on / is at / moved to /
/// went to / runs / upgraded to / bumped to <value>`, or `released /
/// deployed / deploy / shipped / rolled out <thing> <value>`, with
/// `<value>` the change's version-shaped new value. The thing is one token
/// (`backend`, `web`, `desktop-app`), never the author. `None` when the
/// change already binds that thing's version, the value is not a version,
/// or the attribute is not a version-ish one.
pub(super) fn version_rebind(quote: &str, subject: &str, attribute: &ChangeAttribute, new_value: &str, author: Option<&str>) -> Option<(String, ChangeAttribute)> {
    let ChangeAttribute::Text { key, .. } = attribute else { return None };
    if !version_like(new_value) || !(VERSION_ATTRIBUTES.contains(&key.as_str()) || item_like(subject)) {
        return None;
    }
    let value = regex::escape(new_value.trim());
    let token = r"([A-Za-z][A-Za-z0-9_./-]*)";
    let patterns = [
        format!(r"(?i)\b{token}\s+(?:is\s+now|is\s+on|now\s+on|is\s+at|moved\s+to|went\s+to|runs|is\s+running|upgraded\s+to|bumped\s+to|now)\s+(?:release\s+|version\s+)?{value}\b"),
        format!(r"(?i)\b(?:released|deployed|deploy|deploying|shipped|rolled\s+out)\s+{token}\s+(?:to\s+)?{value}\b"),
    ];
    let thing = patterns.iter().find_map(|p| {
        regex::Regex::new(p).ok()?.captures(quote).and_then(|c| c.get(1)).map(|m| m.as_str().to_owned())
    })?;
    const NOT_A_THING: &[&str] = &["it", "this", "that", "which", "and", "we", "i", "they", "pr", "release", "version", "and"];
    if NOT_A_THING.contains(&thing.to_lowercase().as_str()) || Some(thing.as_str()) == author || item_like(&thing) {
        return None;
    }
    let version = parse_attribute("version")?;
    let same_subject = thing.eq_ignore_ascii_case(subject.trim());
    let same_attribute = matches!(&version, ChangeAttribute::Text { key: v, .. } if v == key);
    (!(same_subject && same_attribute)).then_some((thing, version))
}

// ── Validated output ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedChange {
    pub subject: String,
    pub attribute: ChangeAttribute,
    pub old_value: Option<String>,
    pub new_value: String,
    /// When the new value took effect (`from` only), anchored like any
    /// validity.
    pub effective: Option<Validity>,
    /// The exact words that state the change: inside the atom's evidence,
    /// or (`quote_span`) verbatim in its paragraph.
    pub quote: String,
    /// Absolute character span of `quote` when it lies outside the atom's
    /// evidence (the relaxed paragraph path), so it can be checked.
    pub quote_span: Option<(i64, i64)>,
}

impl ValidatedChange {
    pub fn state_key(&self) -> String {
        match &self.attribute {
            ChangeAttribute::Relation(a) => crate::fact_supersession::relation_key(a.predicate)
                .map(|k| k.key.to_owned())
                .expect("relational attributes are keyed"),
            ChangeAttribute::Text { key, .. } => key.clone(),
        }
    }

    /// Free-text attributes name one value at a time.
    pub fn functional(&self) -> bool {
        match &self.attribute {
            ChangeAttribute::Relation(a) => crate::fact_supersession::relation_key(a.predicate).is_some_and(|k| k.functional),
            ChangeAttribute::Text { .. } => true,
        }
    }

    /// The stated edge for `value` under a relational attribute.
    fn edge(&self, value: &str) -> Option<(String, &'static str, String)> {
        let ChangeAttribute::Relation(a) = &self.attribute else { return None };
        Some(if a.subject_is_src {
            (self.subject.clone(), a.predicate, value.to_owned())
        } else {
            (value.to_owned(), a.predicate, self.subject.clone())
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ChangeRejection {
    Schema,
    Limit,
    /// The change quote is missing, too long, or not inside the evidence.
    Quote,
    /// The subject is not an entity mention of the window (or the author).
    Subject,
    Attribute,
    /// A value is blank, too long, or not in the change quote.
    Value,
    /// A relational value is not an entity mention, or is the subject.
    Endpoint,
    /// Old and new are the same value.
    Unchanged,
    /// The quote proposes, recommends, supposes, or describes unmerged work.
    Tentative,
    /// The subject is not what the words are about (named neither in the
    /// quote nor as the source itself).
    SubjectScope,
    /// A family attribute ("parent", "spouse") or a relational one
    /// ("owner", "manager") in a code source.
    Fixture,
    Duplicate,
}

#[derive(Debug, Deserialize)]
pub(super) struct ExtractedChange {
    subject: String,
    attribute: String,
    old_value: Option<String>,
    new_value: String,
    effective_from: Option<String>,
    effective_quote: Option<String>,
    quote: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(super) enum ChangeCandidate {
    Valid(ExtractedChange),
    Invalid(serde::de::IgnoredAny),
}

/// The atom evidence's paragraph in the window: bounded by the nearest blank
/// lines around the evidence, and never more than
/// [`PARAGRAPH_REACH_CHARS`] beyond it on either side, whichever is tighter.
#[derive(Clone, Copy, Debug)]
pub(super) struct Paragraph<'w> {
    pub text: &'w str,
    /// Absolute character offset of `text` in the source.
    pub start_char: i64,
}

/// How far past the evidence a paragraph reaches, at most.
pub(crate) const PARAGRAPH_REACH_CHARS: usize = 300;

fn blank(line: &str) -> bool {
    line.trim().is_empty()
}

pub(super) fn paragraph<'w>(window: &super::Window<'w>, atom: &ValidatedAtom) -> Paragraph<'w> {
    let text = window.text;
    let rel_start = (atom.start_char as usize).saturating_sub(window.start_char);
    let rel_end = (atom.end_char as usize).saturating_sub(window.start_char);
    let byte_of = |chars: usize| text.char_indices().nth(chars).map(|(b, _)| b).unwrap_or(text.len());
    let (start, end) = (byte_of(rel_start), byte_of(rel_end));
    // Blank-line bounds: whole lines only, never the evidence's own lines.
    let mut lo = 0;
    let mut line_begin = 0;
    for (i, c) in text[..start].char_indices() {
        if c == '\n' {
            if i > line_begin && blank(&text[line_begin..i]) || i == line_begin && i > 0 {
                lo = i + 1;
            }
            line_begin = i + 1;
        }
    }
    let mut hi = text.len();
    let mut line_begin: Option<usize> = None;
    for (i, c) in text[end..].char_indices() {
        if c == '\n' {
            if let Some(b) = line_begin {
                if blank(&text[end + b..end + i]) {
                    hi = end + b;
                    break;
                }
            }
            line_begin = Some(i + 1);
        }
    }
    let lo = lo.max(byte_of(rel_start.saturating_sub(PARAGRAPH_REACH_CHARS)));
    let mut hi = hi.min(byte_of(rel_end + PARAGRAPH_REACH_CHARS)).max(end);
    while hi > end && matches!(text.as_bytes()[hi - 1], b'\n' | b'\r') {
        hi -= 1;
    }
    let lo = lo.min(start);
    Paragraph { text: &text[lo..hi], start_char: (window.start_char + text[..lo].chars().count()) as i64 }
}

/// `quote` exactly once in the paragraph, anchored like an evidence quote
/// (exact, then whitespace, quote marks, comment markers and Markdown markup
/// tolerated): the byte span of the paragraph's own text.
fn once_in(paragraph: &str, quote: &str) -> Option<(usize, usize)> {
    super::anchor_quote(paragraph, quote).ok().map(|(start, end, _)| (start, end))
}

/// [`validity`] against the atom's evidence; when its quote is not there, the
/// same validity anchored exactly once in the evidence's paragraph
/// (the model dated the atom with words just outside its quote). The kept
/// quote is the window's own text. Counted, so the dates can be checked.
pub(super) fn validity_in(
    from: Option<&str>,
    until: Option<&str>,
    quote: Option<&str>,
    evidence: &str,
    evidence_start: i64,
    lines: Paragraph<'_>,
    d: &mut RelationDiagnostics,
) -> Result<Option<Validity>, ()> {
    let strict = validity(from, until, quote, evidence);
    let Some(q) = quote.map(str::trim).filter(|q| !q.is_empty()) else { return strict };
    if strict.is_ok() || evidence.contains(q) {
        // Where it sits, so another atom's relaxed date cannot reuse it.
        return strict.map(|v| v.map(|mut v| {
            if let Some(at) = evidence.find(q) {
                let from = evidence_start + evidence[..at].chars().count() as i64;
                v.span = Some((from, from + q.chars().count() as i64));
            }
            v
        }));
    }
    let (s, e) = once_in(lines.text, q).ok_or(())?;
    let mut relaxed = validity(from, until, Some(&lines.text[s..e]), lines.text)?;
    if let Some(v) = relaxed.as_mut() {
        let from = lines.start_char + lines.text[..s].chars().count() as i64;
        v.span = Some((from, from + lines.text[s..e].chars().count() as i64));
        v.relaxed = true;
        d.relaxed_dates += 1;
    }
    Ok(relaxed)
}

/// The full lines that hold the atom's evidence, never more than
/// [`PARAGRAPH_REACH_CHARS`] beyond it: where a relaxed date may be found. A
/// neighbouring line dates its own atom, not this one.
pub(super) fn evidence_lines<'w>(window: &super::Window<'w>, atom: &ValidatedAtom) -> Paragraph<'w> {
    let text = window.text;
    let rel_start = (atom.start_char as usize).saturating_sub(window.start_char);
    let rel_end = (atom.end_char as usize).saturating_sub(window.start_char);
    let byte_of = |chars: usize| text.char_indices().nth(chars).map(|(b, _)| b).unwrap_or(text.len());
    let (start, end) = (byte_of(rel_start), byte_of(rel_end));
    let line_start = text[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let line_end = text[end..].find('\n').map(|i| end + i).unwrap_or(text.len());
    let lo = line_start.max(byte_of(rel_start.saturating_sub(PARAGRAPH_REACH_CHARS))).min(start);
    let hi = line_end.min(byte_of(rel_end + PARAGRAPH_REACH_CHARS)).max(end);
    Paragraph { text: &text[lo..hi], start_char: (window.start_char + text[..lo].chars().count()) as i64 }
}

/// A relaxed date whose words another atom of the window also uses (as its
/// own date, relaxed or not) belongs to that atom: drop it here. Each such
/// date is counted once; a change's stated edge loses the same date.
pub(super) fn drop_shared_relaxed_dates(atoms: &mut [ValidatedAtom], d: &mut RelationDiagnostics) {
    let mut claims: Vec<(usize, (i64, i64))> = Vec::new();
    for (i, atom) in atoms.iter().enumerate() {
        let dates = atom.validity.iter()
            .chain(atom.relations.iter().filter_map(|r| r.validity.as_ref()))
            .chain(atom.changes.iter().filter_map(|c| c.effective.as_ref()));
        for v in dates {
            if let Some(span) = v.span {
                claims.push((i, span));
            }
        }
    }
    let shared = |i: usize, (a, b): (i64, i64)| claims.iter().any(|(j, (c, e))| *j != i && a < *e && *c < b);
    for (i, atom) in atoms.iter_mut().enumerate() {
        let drop = |v: &mut Option<Validity>, d: &mut RelationDiagnostics| {
            if v.as_ref().is_some_and(|v| v.relaxed && v.span.is_some_and(|s| shared(i, s))) {
                *v = None;
                d.dropped_shared_dates += 1;
            }
        };
        drop(&mut atom.validity, d);
        let mut gone = Vec::new();
        for c in atom.changes.iter_mut() {
            let before = c.effective.clone();
            drop(&mut c.effective, d);
            if c.effective.is_none() {
                gone.extend(before);
            }
        }
        for r in atom.relations.iter_mut() {
            if r.validity.as_ref().is_some_and(|v| gone.contains(v)) {
                r.validity = None;
            } else {
                drop(&mut r.validity, d);
            }
        }
    }
}

const TENTATIVE: &[&str] = &[
    "should", "shall we", "propose", "proposes", "proposed", "proposal", "plan to", "plans to", "planning to",
    "would", "could", "might", "may want", "recommend", "recommends", "recommended", "recommendation",
    "suggest", "suggests", "suggested", "consider", "considering", "if we", "what if", "maybe", "perhaps",
    "draft", "wip", "work in progress", "not yet merged", "unmerged", "on branch", "on a branch", "pending review",
];

/// The quote proposes, recommends, supposes, or describes draft or unmerged
/// work. Whole words, case-insensitive.
pub(crate) fn tentative(quote: &str) -> bool {
    let words: Vec<String> = quote.split(|c: char| !c.is_alphanumeric()).filter(|w| !w.is_empty()).map(str::to_lowercase).collect();
    let joined = format!(" {} ", words.join(" "));
    TENTATIVE.iter().any(|phrase| joined.contains(&format!(" {phrase} ")))
}

/// What the source itself is called, from its anchor: title, key, number
/// (also as `#N` and `PR #N`), path and the path's file name.
pub(super) fn self_names(anchor: &Value) -> Vec<String> {
    let mut names = Vec::new();
    for key in ["title", "identifier", "path", "file_path", "code_path"] {
        if let Some(v) = anchor.get(key).and_then(Value::as_str) {
            names.push(v.to_owned());
            if key.ends_with("path") {
                if let Some(base) = v.rsplit('/').next() {
                    names.push(base.to_owned());
                }
            }
        }
    }
    if let Some(n) = anchor.get("number").and_then(Value::as_str) {
        names.extend([format!("#{n}"), format!("PR #{n}")]);
    }
    names.retain(|n| n.chars().count() >= 3);
    names
}

/// `subject` names the source itself: it is one of its names, or one of the
/// names is inside it ("PR #786" in "PR #786: point desktop at api").
pub(super) fn names_source(subject: &str, names: &[String]) -> bool {
    let s = subject.to_lowercase();
    names.iter().any(|n| {
        let n = n.to_lowercase();
        s == n || s.contains(&n) || n.contains(&s) && s.chars().count() >= 6
    })
}

/// Source types that are code: their people are fixtures and examples.
pub(super) fn is_code_source(source_type: &str) -> bool {
    matches!(source_type, "github_code" | "github_code_summary")
}

/// The predicates a code source may state: code naming other code
/// (`mentions`; `related_to` is retired from extraction). Every other
/// extractable predicate is about people (kinship, employment, ownership,
/// involvement), and code states none (`fixture`).
pub(super) fn code_relation(predicate: &str) -> bool {
    predicate == predicates::MENTIONS
}

// ── Authorship is not involvement ───────────────────────────────────────────

/// Predicates an authorship line ("opened by Dana") is misread as: the
/// involvement ones, and `mentions` / `mentioned_by` ("Dana opened PR #876"
/// stored as PR #876 `mentioned_by` Dana; v3.4.3 gate).
const INVOLVEMENT_PREDICATES: &[&str] = &[
    predicates::WORKS_ON, predicates::WORKED_ON_BY, predicates::RESPONSIBLE_FOR, predicates::RESPONSIBILITY_OF,
    predicates::MENTIONS, predicates::MENTIONED_BY,
];
/// Verbs that make the agent the item's author: the author of a PR or issue
/// works on it (founder decision, 2026-10-21).
const CREATION_VERBS: &[&str] = &["opened", "authored", "created", "filed", "submitted", "raised", "posted", "wrote"];
/// Verbs that act on an item someone else may have authored: never
/// `works_on` or `mentions` for the actor.
const STATUS_VERBS: &[&str] = &[
    "closed", "merged", "reviewed", "approved", "commented", "reopened", "pushed", "committed", "labeled",
];
/// Words that state involvement or ownership: their presence makes the
/// relation a stated one, whatever authorship words sit beside it.
const INVOLVEMENT_CUES: &[&str] = &[
    "work", "works", "worked", "working", "own", "owns", "owned", "owner", "owners", "ownership", "responsible",
    "responsibility", "lead", "leads", "leading", "led", "maintain", "maintains", "maintainer", "maintaining",
    "assignee", "dri", "driving", "drives", "handling", "handles", "building", "builds", "implementing",
];

fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !(c.is_alphanumeric() || c == '\'' || c == '-'))
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// How an endpoint acts on an item, whatever its mention type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Agency {
    /// Authored it: opened, created, filed, submitted, raised, posted.
    Created,
    /// Acted on it: closed, merged, reviewed, approved, commented, pushed.
    /// Outranks `Created`: an endpoint that both opens and closes things
    /// is not known to have authored this one.
    Acted,
}

/// The endpoint is the agent of a creation or status verb in `text`:
/// immediately before it ("Dana opened", "COR-7 closed", unless passive:
/// "COR-9 filed by …" names COR-9 the item) or after a "by" that follows
/// one ("opened by Dana"). `None`: not an agent.
pub(super) fn agency(text: &str, endpoint: &str) -> Option<Agency> {
    let lower = text.to_lowercase();
    let name = endpoint.trim().to_lowercase();
    if name.is_empty() {
        return None;
    }
    let kind = |w: &str| {
        if CREATION_VERBS.contains(&w) {
            Some(Agency::Created)
        } else if STATUS_VERBS.contains(&w) {
            Some(Agency::Acted)
        } else {
            None
        }
    };
    let mut found = None;
    for (at, _) in lower.match_indices(&name) {
        let rest = &lower[at + name.len()..];
        // "Dana's own PR #876": the possessive author.
        if ["'s own ", "’s own "].iter().any(|p| rest.starts_with(p)) {
            found = found.max(Some(Agency::Created));
        }
        let before = words(&lower[..at]);
        let after = words(rest);
        if let Some(k) = after.first().and_then(|w| kind(w)) {
            if after.get(1).is_none_or(|w| w != "by") {
                found = found.max(Some(k));
            }
        }
        if before.last().is_some_and(|w| w == "by") {
            if let Some(k) = before[before.len().saturating_sub(4)..before.len() - 1].iter().rev().find_map(|w| kind(w)) {
                found = found.max(Some(k));
            }
        }
    }
    found
}

/// The part of `rest` up to the end of its clause: a sentence stop, a line
/// break, a comma, or " and " / " but ".
fn clause_head(rest: &str) -> &str {
    let mut end = rest.len();
    for stop in [".", ";", "!", "?", "\n", ",", " and ", " but "] {
        if let Some(i) = rest.find(stop) {
            // A '.' inside a token ("x.rs", "v2.1") does not end the clause.
            if stop == "." && rest[i + 1..].chars().next().is_some_and(|c| !c.is_whitespace()) {
                continue;
            }
            end = end.min(i);
        }
    }
    &rest[..end]
}

/// The first `#N` or `KEY-N` token of a phrase.
fn first_item_token(phrase: &str) -> Option<String> {
    let key = |w: &str| w.split_once('-').is_some_and(|(p, n)| !p.is_empty() && p.chars().all(|c| c.is_ascii_alphabetic()) && !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()));
    phrase
        .split(|c: char| c.is_whitespace() || matches!(c, ':' | ',' | '(' | ')' | '.'))
        .find(|w| (w.starts_with('#') && w.len() > 1 && w[1..].chars().all(|c| c.is_ascii_digit())) || key(w))
        .map(str::to_owned)
}

/// [`agency`] scoped to one item: `agent` created or acted on `item` only
/// when the item is what the verb names. "Alex's own PR #287" makes Alex
/// the creator of PR #287, not of a "decision still pending Alex" elsewhere
/// in the context (v3.4.6 gate). The item must sit in the clause after the
/// possessive or active verb (or be the `#N` / key it names first); for the
/// passive "… opened by X", in the clause before it, after X on its line
/// ("opened by X: <title>"), or on the next line (a Slack notification).
pub(super) fn agency_toward(text: &str, agent: &str, item: &str) -> Option<Agency> {
    let lower = text.to_lowercase();
    let (name, target) = (agent.trim().to_lowercase(), item.trim().to_lowercase());
    if name.is_empty() || target.is_empty() {
        return None;
    }
    // The phrase names the authored item itself: its `#N` / key, or (a
    // title-only PR) the title that starts the phrase and makes up most of
    // it. A topic the title merely discusses ("Add an index to the graphs
    // table" for "graphs table") is not the item (v3.4.7 gate).
    let names_item = |phrase: &str| {
        let title = phrase.trim().trim_start_matches([':', '-', ' ']).trim_end_matches(['.', ' ']);
        first_item_token(phrase).is_some_and(|t| item_like(&target) && target.contains(&t))
            || title.starts_with(target.as_str()) && target.len() * 10 >= title.len() * 6
    };
    let kind = |w: &str| {
        if CREATION_VERBS.contains(&w) {
            Some(Agency::Created)
        } else if STATUS_VERBS.contains(&w) {
            Some(Agency::Acted)
        } else {
            None
        }
    };
    let mut found = None;
    for (at, _) in lower.match_indices(&name) {
        let rest = &lower[at + name.len()..];
        for p in ["'s own ", "’s own "] {
            if let Some(phrase) = rest.strip_prefix(p) {
                if names_item(clause_head(phrase)) {
                    found = found.max(Some(Agency::Created));
                }
            }
        }
        let after = words(rest);
        if let Some(k) = after.first().and_then(|w| kind(w)) {
            if after.get(1).is_none_or(|w| w != "by") {
                let verb_at = rest.find(after[0].as_str()).map_or(0, |i| i + after[0].len());
                if names_item(clause_head(&rest[verb_at..])) {
                    found = found.max(Some(k));
                }
            }
        }
        let before = words(&lower[..at]);
        if before.last().is_some_and(|w| w == "by") {
            if let Some(k) = before[before.len().saturating_sub(4)..before.len() - 1].iter().rev().find_map(|w| kind(w)) {
                let start = lower[..at].rfind(['.', ';', '!', '?', '\n']).map_or(0, |i| i + 1);
                let line_rest = rest.split('\n').next().unwrap_or("");
                let next_line = rest.split('\n').nth(1).unwrap_or("");
                if names_item(&lower[start..at]) || names_item(line_rest) || names_item(next_line) {
                    found = found.max(Some(k));
                }
            }
        }
    }
    found
}

/// A PR, issue or similar tracked item: `#123`, `PR #12`, `COR-65`, "pull
/// request", "issue".
pub(super) fn item_like(name: &str) -> bool {
    let lower = name.to_lowercase();
    let key = |w: &str| w.split_once('-').is_some_and(|(p, n)| !p.is_empty() && p.chars().all(|c| c.is_ascii_alphabetic()) && !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()));
    lower.split(|c: char| c.is_whitespace() || matches!(c, ':' | ',' | '(' | ')')).any(|w| {
        (w.starts_with('#') && w.len() > 1 && w[1..].chars().all(|c| c.is_ascii_digit())) || key(w)
    }) || lower.contains("pull request") || lower.starts_with("pr ") || lower.starts_with("issue")
}

/// A bot account: `dependabot[bot]`, `github-actions`, `coderabbitai`, …
pub(super) fn bot_like(name: &str) -> bool {
    let n = name.trim().to_lowercase();
    const BOTS: &[&str] = &[
        "github-actions", "dependabot", "renovate", "coderabbit", "coderabbitai", "greptile", "greptile-apps",
        "vercel", "netlify", "codecov", "sonarcloud", "mergify", "snyk", "copilot", "renovatebot", "imgbot",
        "allcontributors", "linear-bot", "slackbot", "cursor-agent",
    ];
    n.ends_with("[bot]") || n.ends_with("-bot") || n.ends_with("_bot") || n.starts_with("bot-") || n.ends_with(" bot")
        || BOTS.contains(&n.as_str())
}

/// Whether authorship decides this relation, and against it (founder
/// decision, 2026-10-21). The author of a PR or issue works on it:
/// `works_on` / `worked_on_by` from the agent of a creation verb to an item
/// stays, and so does one from the source's own author to the source
/// itself (`own_item`). A closing, merging, reviewing, approving,
/// commenting or pushing agent never gets `works_on`, `mentions` or
/// ownership, and authoring is never ownership. A bot never gets
/// `works_on` or `mentions`. Words in the linking clause that state
/// involvement (works, owns, leads, …) keep the relation.
pub(super) fn authorship_drop(predicate: &str, subject: &str, object: &str, evidence: &str, own_item: bool) -> bool {
    if !INVOLVEMENT_PREDICATES.contains(&predicate) {
        return false;
    }
    let person = match predicate {
        predicates::WORKED_ON_BY | predicates::RESPONSIBILITY_OF | predicates::MENTIONED_BY => object,
        _ => subject,
    };
    let work = matches!(predicate, predicates::WORKS_ON | predicates::WORKED_ON_BY);
    if bot_like(person) && (work || matches!(predicate, predicates::MENTIONS | predicates::MENTIONED_BY)) {
        return true;
    }
    if work && own_item {
        return false;
    }
    // Only the words that link the endpoints count: a PR title that says
    // "build" or "work" does not make its opener an owner (v3.4.3 gate).
    if linking_clauses(evidence, subject, object).iter().any(|c| words(c).iter().any(|w| INVOLVEMENT_CUES.contains(&w.as_str()))) {
        return false;
    }
    let item = if person == subject { object } else { subject };
    match (agency(evidence, subject).max(agency(evidence, object)), agency_toward(evidence, person, item)) {
        (None, _) => false,
        // The author works on what they opened, titled by number or not.
        (_, Some(Agency::Created)) if work => false,
        (_, Some(_)) => true,
        // An authorship verb about something else: authorship does not
        // decide this pair (the evidence-word check still runs), unless the
        // person only ever acts on items (closes, merges, reviews).
        (Some(_), None) => agency(evidence, person) == Some(Agency::Acted),
    }
}

#[cfg(test)]
pub(super) fn authorship_only(predicate: &str, subject: &str, object: &str, evidence: &str) -> bool {
    authorship_drop(predicate, subject, object, evidence, false)
}

/// The words that link two endpoints in `text`: the spans between an
/// occurrence of each that stay inside one sentence (no `.;!?` or line
/// break between them), or, with none, the closest pair's span. With one
/// endpoint absent (the author's first person), the sentence holding the
/// other; with neither, all of `text`.
pub(super) fn linking_clauses(text: &str, subject: &str, object: &str) -> Vec<String> {
    let lower = text.to_lowercase();
    let find = |name: &str| -> Vec<(usize, usize)> {
        let name = name.trim().to_lowercase();
        if name.is_empty() {
            return Vec::new();
        }
        lower.match_indices(&name).map(|(at, m)| (at, at + m.len())).collect()
    };
    let is_stop = |c: char| matches!(c, '.' | ';' | '!' | '?' | '\n');
    let (a, b) = (find(subject), find(object));
    let mut spans: Vec<(usize, usize)> = Vec::new();
    for &(a0, a1) in &a {
        for &(b0, b1) in &b {
            spans.push(if a1 <= b0 { (a1, b0) } else if b1 <= a0 { (b1, a0) } else { (a0.min(b0), a0.min(b0)) });
        }
    }
    if !spans.is_empty() {
        // A '.' inside a name-like token ("v2.1", "x.rs") is not a stop: only
        // a stop followed by whitespace or the end ends a sentence.
        let crosses = |&(x, y): &(usize, usize)| {
            let span = &lower[x..y];
            span.char_indices().any(|(i, c)| is_stop(c) && (c == '\n' || span[i + c.len_utf8()..].starts_with(char::is_whitespace) || i + c.len_utf8() == span.len()))
        };
        let inside: Vec<String> = spans.iter().filter(|s| !crosses(s)).map(|&(x, y)| lower[x..y].to_owned()).collect();
        if !inside.is_empty() {
            return inside;
        }
        let &(x, y) = spans.iter().min_by_key(|(x, y)| y - x).expect("spans");
        return vec![lower[x..y].to_owned()];
    }
    let Some(&(at, end)) = a.first().or(b.first()) else { return vec![lower] };
    let start = lower[..at].rfind(is_stop).map_or(0, |i| i + 1);
    let stop = lower[end..].find(is_stop).map_or(lower.len(), |i| end + i);
    vec![lower[start..stop].to_owned()]
}

/// The evidence an atom's relations are judged on: the window lines that
/// hold its evidence, plus the window line before them (a Slack PR
/// notification puts "opened by X" on one line and the PR on the next).
pub(super) fn relation_context(window: &super::Window<'_>, atom: &ValidatedAtom) -> String {
    let lines = evidence_lines(window, atom);
    let text = window.text;
    let rel = (lines.start_char as usize).saturating_sub(window.start_char);
    let lo = text.char_indices().nth(rel).map_or(text.len(), |(b, _)| b);
    let before = text[..lo].trim_end_matches(['\n', '\r']);
    let previous = before.rfind('\n').map_or(before, |i| &before[i + 1..]);
    let joined = if previous.trim().is_empty() { lines.text.to_owned() } else { format!("{previous}\n{}", lines.text) };
    unlink(&joined)
}

/// Link markup reduced to its label, so the words around a name are words:
/// Slack `<url|label>` (raw, or HTML-escaped as `&lt;url|label&gt;`) and
/// Markdown `[label](url)`. A bare `<url>` keeps its url. A real Slack PR
/// notification reads "Pull request opened by &lt;https://github.com/x|x&gt;".
pub(super) fn unlink(text: &str) -> String {
    let text = text.replace("&lt;", "<").replace("&gt;", ">");
    let mut out = String::with_capacity(text.len());
    let mut rest = text.as_str();
    while let Some(open) = rest.find(['<', '[']) {
        let (head, tail) = rest.split_at(open);
        out.push_str(head);
        if tail.starts_with('<') {
            if let Some(close) = tail.find('>').filter(|&c| !tail[..c].contains('\n')) {
                let inner = &tail[1..close];
                match inner.split_once('|') {
                    Some((_, label)) => out.push_str(label),
                    None => out.push_str(inner),
                }
                rest = &tail[close + 1..];
                continue;
            }
        } else if let Some(mid) = tail.find("](").filter(|&m| !tail[..m].contains(['\n', ']'])) {
            if let Some(end) = tail[mid + 2..].find(')').filter(|&e| !tail[mid + 2..mid + 2 + e].contains(char::is_whitespace)) {
                out.push_str(&tail[1..mid]);
                rest = &tail[mid + 2 + end + 1..];
                continue;
            }
        }
        out.push_str(&tail[..1]);
        rest = &tail[1..];
    }
    out.push_str(rest);
    out
}

const EMPLOYMENT_WORDS: &[&str] = &[
    "work", "works", "worked", "working", "employed", "employee", "employees", "employer", "employs", "hired",
    "joined", "joins", "staff", "intern", "contractor", "ceo", "cto", "cfo", "coo", "cpo", "founder", "cofounder",
    "co-founder", "engineer", "manager", "director", "head", "vp", "designer", "analyst", "consultant", "recruiter",
    "president", "lead", "developer", "researcher", "scientist", "officer", "partner", "team",
];
const OWNERSHIP_WORDS: &[&str] = &[
    "own", "owns", "owned", "owner", "owners", "ownership", "responsible", "responsibility", "accountable", "dri",
    "maintain", "maintains", "maintained", "maintainer", "maintaining", "lead", "leads", "leading", "led",
    "charge", "assigned", "assignee", "oversee", "oversees", "overseeing", "handles", "handling", "manages", "managing",
];
const WORK_WORDS: &[&str] = &[
    "work", "works", "worked", "working", "build", "builds", "building", "built", "implement", "implements", "implementing",
    "develop", "develops", "developing", "contribute", "contributes", "contributing", "fixing", "fixes", "debugging",
    "assigned", "assignee", "lead", "leads", "leading",
];
/// Words that offer, suggest or hand over for review: no work evidence
/// ("the drift check was offered to Alex", "for Alex to review").
const OFFER_WORDS: &[&str] = &[
    "offer", "offers", "offered", "offering", "suggest", "suggests", "suggested", "suggesting", "propose",
    "proposes", "proposed", "recommend", "recommends", "recommended", "review", "reviews", "reviewing", "reviewer",
    "asked", "asks", "could", "might",
];
/// A `mentions` clause about where a thing is kept, or an example: not one
/// thing naming another.
const NOT_A_REFERENCE: &[&str] = &[
    "stored", "store", "stores", "saved", "kept", "lives", "vault", "secret", "secrets", "e.g", "example",
    "examples", "eg", "i.e", "such",
];

/// A name that is code: an identifier with `_`, a path, a namespace, a
/// file with an extension, or a call.
fn code_like(name: &str) -> bool {
    let name = name.trim();
    name.contains('_') || name.contains("::") || name.contains('/') || name.ends_with("()")
        || name.rsplit_once('.').is_some_and(|(stem, ext)| !stem.is_empty() && (1..=5).contains(&ext.len()) && ext.chars().all(|c| c.is_ascii_alphanumeric()))
}

/// A comment line in code or config (`//`, `#`, `/*`, `*`, `--`, `<!--`).
fn comment_line(line: &str) -> bool {
    let line = line.trim_start();
    ["//", "#", "/*", "*", "--", "<!--", ";;"].iter().any(|m| line.starts_with(m))
}

/// Per-predicate evidence a relation needs in the words linking its
/// endpoints, and its direction by endpoint type (`kind` returns a known
/// entity type). Relations are rare and precision matters more than recall
/// (v3.4.3 gate: 0.61 pooled), so a relation the clause does not state is
/// dropped: `works_at` needs employment words ("customer has a workspace"
/// is not employment), `responsible_for` / `responsibility_of` ownership
/// words ("A is a copy of B" is not), `works_on` / `worked_on_by` work
/// words, and `mentions` / `mentioned_by` a reference that is not storage,
/// an example, or a comment line in code. A person-or-org endpoint on the
/// thing's side reverses ownership and employment, and drops it; work needs
/// a person or org (or an unknown type) on the worker's side, never a model
/// or tool. An item's author works on it (a creation verb, or `own_item`).
pub(super) fn relation_unsupported(
    predicate: &str,
    subject: &str,
    object: &str,
    text: &str,
    code: bool,
    own_item: bool,
    kind: impl Fn(&str) -> Option<super::EntityType>,
) -> bool {
    use super::EntityType::{Org, Person};
    let clauses: Vec<Vec<String>> = linking_clauses(text, subject, object).iter().map(|c| words(c)).collect();
    let has = |list: &[&str]| clauses.iter().any(|c| c.iter().any(|w| list.contains(&w.as_str())));
    // v3.5: a word anywhere in a sentence naming both endpoints ("Alex was
    // offered the drift check").
    let sentences: Vec<Vec<String>> = sentences_naming(text, &subject.trim().to_lowercase(), &object.trim().to_lowercase()).iter().map(|c| words(c)).collect();
    let in_sentence = |list: &[&str]| sentences.iter().any(|c| c.iter().any(|w| list.contains(&w.as_str())));
    let agent = |e: &str| matches!(kind(e), Some(Person | Org));
    let thing = |e: &str| kind(e).is_some_and(|t| !matches!(t, Person | Org));
    // Kinship between a person and a commit or branch ("example-founder pushed a
    // commit to main" as parent_of) is never a family.
    if predicates::meta(predicate).is_some_and(|m| m.domain == predicates::Domain::Kinship)
        && [subject, object].iter().any(|e| is_branch(text, e) || commit_like(e))
    {
        return true;
    }
    match predicate {
        predicates::WORKS_AT => !has(EMPLOYMENT_WORDS) || thing(subject) || kind(subject) == Some(Org) && kind(object) == Some(Person),
        predicates::RESPONSIBLE_FOR => !has(OWNERSHIP_WORDS) || thing(subject) && agent(object),
        predicates::RESPONSIBILITY_OF => !has(OWNERSHIP_WORDS) || agent(subject) && thing(object),
        // The author of an item works on it ("Dana opened PR #876"): a
        // creation verb is work words for an item. A model or tool doing the
        // work ("COR-7 started on Opus") is not a person working on it, and
        // there is no `uses` predicate: dropped.
        // The agent of a creation verb is never dropped here, whatever its
        // mention type (a Slack handle typed as a topic) or the item's form
        // (a PR named by title only).
        // A branch never works on anything (v3.4.8).
        predicates::WORKS_ON if is_branch(text, subject) || subject.contains("<-") => true,
        predicates::WORKED_ON_BY if is_branch(text, object) || object.contains("<-") => true,
        predicates::WORKS_ON if own_item || agency_toward(text, subject, object) == Some(Agency::Created) => false,
        predicates::WORKED_ON_BY if own_item || agency_toward(text, object, subject) == Some(Agency::Created) => false,
        // v3.4.9: something offered, suggested or handed over for review is
        // not work on it.
        predicates::WORKS_ON | predicates::WORKED_ON_BY if has(OFFER_WORDS) || in_sentence(OFFER_WORDS) => true,
        predicates::WORKS_ON => !has(WORK_WORDS) || thing(subject),
        predicates::WORKED_ON_BY => !has(WORK_WORDS) || thing(object),
        predicates::MENTIONS | predicates::MENTIONED_BY => {
            let lines: Vec<&str> = text.lines().filter(|l| {
                let l = l.to_lowercase();
                l.contains(&subject.trim().to_lowercase()) || l.contains(&object.trim().to_lowercase())
            }).collect();
            // In code, a comment line that cites something that is not code
            // ("see ChatGPT") is a citation, not a reference between code.
            // Storage and example words count anywhere in the lines that
            // name the endpoints ("…, for example."), not only between them.
            let near: Vec<String> = lines.iter().flat_map(|l| words(l)).collect();
            has(NOT_A_REFERENCE) || near.iter().any(|w| NOT_A_REFERENCE.contains(&w.as_str()))
                || (code && !lines.is_empty() && lines.iter().all(|l| comment_line(l)) && !(code_like(subject) && code_like(object)))
        }
        _ => false,
    }
}

/// v3.4.9: the structural part of [`relation_unsupported`] for an edge made
/// from a relational change. The change validator already required the
/// change sentence (its attribute is the evidence, "moved from Bo to Ana"
/// needs no "owns"), so only kinship to a commit or branch and a branch as
/// worker drop it.
pub(super) fn change_edge_unsupported(predicate: &str, subject: &str, object: &str, text: &str) -> bool {
    let kinship = predicates::meta(predicate).is_some_and(|m| m.domain == predicates::Domain::Kinship)
        && [subject, object].iter().any(|e| is_branch(text, e) || commit_like(e));
    let branch_worker = match predicate {
        predicates::WORKS_ON => is_branch(text, subject) || subject.contains("<-"),
        predicates::WORKED_ON_BY => is_branch(text, object) || object.contains("<-"),
        _ => false,
    };
    kinship || branch_worker
}

/// An author stored as a mention of their own item ("Pull request opened by
/// Dana: <title>" as Dana `mentions` / `mentioned_by` it): the
/// `works_on` it means, as (worker, item). `None` when the relation is not a
/// mention, or no endpoint alone is the agent of a creation verb.
pub(super) fn author_mention<'a>(predicate: &str, subject: &'a str, object: &'a str, text: &str) -> Option<(&'a str, &'a str)> {
    if !matches!(predicate, predicates::MENTIONS | predicates::MENTIONED_BY) {
        return None;
    }
    match (agency_toward(text, subject, object), agency_toward(text, object, subject)) {
        (Some(Agency::Created), None) => Some((subject, object)),
        (None, Some(Agency::Created)) => Some((object, subject)),
        _ => None,
    }
}

const OWNER_VERBS: &[&str] = &["owns", "maintains", "leads", "runs", "manages", "oversees"];
const OWNED_BY: &[&str] = &["owned by", "maintained by", "led by", "run by", "managed by", "overseen by"];

/// `owner` is stated as the owner of `thing` in `text`: "Morgan owns X",
/// "Morgan is responsible for X", "Morgan is the owner of X", or "X is owned
/// by Morgan" (within one clause).
fn stated_owner(text: &str, owner: &str, thing: &str) -> bool {
    let lower = text.to_lowercase();
    let (o, t) = (owner.trim().to_lowercase(), thing.trim().to_lowercase());
    if o.is_empty() || t.is_empty() {
        return false;
    }
    let follows = |a: &str, b: &str, links: &[&str]| {
        lower.match_indices(a).any(|(at, m)| {
            let rest = clause_head(&lower[at + m.len()..]);
            links.iter().any(|l| rest.find(l).is_some_and(|i| rest[i + l.len()..].contains(b)))
        })
    };
    follows(&o, &t, &[" owns ", " maintains ", " leads ", " runs ", " manages ", " oversees ", " is responsible for ", " is the owner of ", " owner of "])
        || follows(&t, &o, &OWNED_BY.iter().map(|l| *l).collect::<Vec<_>>())
}

/// v3.5: `owner` is stated to own something in `text`, and nothing they own
/// is `item` itself: an issue body saying "Morgan owns pagination
/// validation" does not make Morgan responsible for the issue.
pub(super) fn owns_only_other(text: &str, owner: &str, item: &str) -> bool {
    let lower = text.to_lowercase();
    let (o, i) = (owner.trim().to_lowercase(), item.trim().to_lowercase());
    if o.is_empty() || i.is_empty() {
        return false;
    }
    let token = first_item_token(&i);
    let mut owns_something = false;
    for (at, m) in lower.match_indices(&o) {
        let rest = clause_head(&lower[at + m.len()..]);
        for verb in [" owns ", " is responsible for ", " is the owner of ", " owner of ", " maintains ", " leads "] {
            if let Some(v) = rest.find(verb) {
                let owned = &rest[v + verb.len()..];
                if owned.contains(&i) || token.as_deref().is_some_and(|t| owned.contains(t)) {
                    return false;
                }
                owns_something = true;
            }
        }
    }
    owns_something
}

/// Markers of an example or hypothetical in prose: a PR body describing the
/// extractor quotes the facts it should no longer produce.
const EXAMPLE_MARKERS: &[&str] = &[
    "e.g:", "e.g ", "e.g,", "for example", "for instance", "such as the case where", "red on main", "red on v",
    "should no longer yield", "should no longer produce", "no longer yields", "no longer produces", "previously gave",
    "used to give", "used to produce", "would have produced", "would produce",
];

/// v3.5: the relation is lifted from an example or hypothetical: a
/// sentence naming both endpoints (or, failing one, the evidence) carries
/// an example marker, or both endpoints sit inside one quoted span (`"…"`,
/// `“…”`, or a backticked phrase with a space: a quoted fixture).
pub(super) fn example_or_hypothetical(text: &str, subject: &str, object: &str) -> bool {
    let (s, o) = (subject.trim().to_lowercase(), object.trim().to_lowercase());
    if s.is_empty() || o.is_empty() {
        return false;
    }
    // "e.g." would end a sentence at its own period.
    let lower = text.to_lowercase().replace("e.g.", "e.g:").replace("i.e.", "i.e:");
    let sentences = sentences_naming(&lower, &s, &o);
    let scopes: Vec<String> = if sentences.is_empty() { vec![lower.clone()] } else { sentences };
    if scopes.iter().any(|c| EXAMPLE_MARKERS.iter().any(|m| c.contains(m))) {
        return true;
    }
    let spans = |open: char, close: char| -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = lower.as_str();
        while let Some(a) = rest.find(open) {
            let after = &rest[a + open.len_utf8()..];
            let Some(b) = after.find(close) else { break };
            out.push(after[..b].to_owned());
            rest = &after[b + close.len_utf8()..];
        }
        out
    };
    spans('"', '"').into_iter()
        .chain(spans('“', '”'))
        .chain(spans('`', '`').into_iter().filter(|q| q.contains(' ')))
        .any(|q| q.contains(&s) && q.contains(&o))
}

/// v3.4.9: an ownership of an item whose text owns only a part of it
/// ("Morgan owns the pagination validation part of DEMO-1"): the owned
/// phrase before " of / in / for / on <item>", trimmed of articles and of
/// "part", "piece", "portion", "sub-task", "section". `None` when the owner
/// owns the item itself, or no ownership is stated.
pub(super) fn owned_part(text: &str, owner: &str, item: &str) -> Option<String> {
    let lower = text.to_lowercase();
    let (o, i) = (owner.trim().to_lowercase(), item.trim().to_lowercase());
    if o.is_empty() || i.is_empty() {
        return None;
    }
    for (at, m) in lower.match_indices(&o) {
        let rest = clause_head(&lower[at + m.len()..]);
        for verb in [" owns ", " is responsible for ", " is the owner of ", " owner of ", " maintains ", " leads "] {
            let Some(v) = rest.find(verb) else { continue };
            let owned = &rest[v + verb.len()..];
            if owned.trim_start().starts_with(i.as_str()) {
                return None;
            }
            for prep in [" of ", " in ", " for ", " on "] {
                if let Some(p) = owned.find(&format!("{prep}{i}")) {
                    let mut part = owned[..p].trim();
                    for article in ["the ", "a ", "an "] {
                        part = part.strip_prefix(article).unwrap_or(part);
                    }
                    for tail in [" part", " piece", " portion", " sub-task", " subtask", " section"] {
                        part = part.strip_suffix(tail).unwrap_or(part);
                    }
                    if !part.is_empty() {
                        return Some(part.to_owned());
                    }
                }
            }
        }
    }
    None
}

/// v3.4.8: an ownership relation the model inverted ("Morgan owns pagination
/// validation" as pagination validation `responsible_for` Morgan): the
/// endpoints in the stated direction, or `None` when they already are (or
/// the text states neither direction).
pub(super) fn reoriented_owner<'a>(predicate: &str, subject: &'a str, object: &'a str, text: &str) -> Option<(&'a str, &'a str)> {
    let _ = OWNER_VERBS;
    let (owner, thing) = match predicate {
        predicates::RESPONSIBLE_FOR => (subject, object),
        predicates::RESPONSIBILITY_OF => (object, subject),
        _ => return None,
    };
    (!stated_owner(text, owner, thing) && stated_owner(text, thing, owner)).then_some((object, subject))
}

/// A git branch: `main`, `master`, `develop`, `refs/heads/…`, or a
/// conventional branch prefix (`feat/`, `fix/`, `chore/`, …) on a name
/// with no file extension.
/// A branch by its context: an unprefixed worktree branch name
/// (`extraction-v349`, `profile-v3/fix`), kebab- or slash-cased with no
/// spaces, that `text` marks as a branch or worktree ("branch X",
/// "worktree X", "on X", "X branch", "pushed to X", "head X"). An arbitrary
/// kebab word without such context is not a branch.
pub(super) fn is_branch(text: &str, name: &str) -> bool {
    if branch_like(name) {
        return true;
    }
    let n = name.trim().to_lowercase();
    if n.is_empty() || n.contains(char::is_whitespace) || !(n.contains('-') || n.contains('/')) {
        return false;
    }
    let lower = text.to_lowercase();
    [
        format!("branch {n}"), format!("branch: {n}"), format!("worktree {n}"), format!("worktrees/{n}"),
        format!("on {n}"), format!("{n} branch"), format!("pushed to {n}"), format!("head {n}"), format!("head: {n}"),
        format!("checkout {n}"), format!("checked out {n}"),
    ]
    .iter()
    .any(|p| lower.contains(p.as_str()))
}

pub(super) fn branch_like(name: &str) -> bool {
    let n = name.trim().to_lowercase();
    if matches!(n.as_str(), "main" | "master" | "develop" | "development" | "trunk" | "staging") || n.starts_with("refs/heads/") {
        return true;
    }
    const PREFIXES: &[&str] = &[
        "feat/", "feature/", "fix/", "bugfix/", "hotfix/", "chore/", "release/", "refactor/", "perf/", "ci/",
        "claude/", "cursor/", "codex/", "dependabot/", "renovate/", "worktree-",
    ];
    let last = n.rsplit('/').next().unwrap_or(&n);
    let has_extension = last.rsplit_once('.').is_some_and(|(stem, ext)| !stem.is_empty() && (1..=5).contains(&ext.len()) && ext.chars().all(|c| c.is_ascii_alphabetic()));
    !n.contains(char::is_whitespace) && !has_extension && PREFIXES.iter().any(|p| n.starts_with(p) && n.len() > p.len())
}

/// A commit: a 7–40 character hex hash with a digit, or a name that says
/// "commit".
fn commit_like(name: &str) -> bool {
    let n = name.trim().to_lowercase();
    ((7..=40).contains(&n.len()) && n.chars().all(|c| c.is_ascii_hexdigit()) && n.chars().any(|c| c.is_ascii_digit()))
        || n.split_whitespace().any(|w| w == "commit" || w == "commits")
}

/// A commit-list entry: a short or full commit hash, then its subject.
fn commit_line(line: &str) -> bool {
    let line = line.trim_start().trim_start_matches(['-', '*', '•', ' ']);
    let hash: String = line.chars().take_while(|c| c.is_ascii_hexdigit()).collect();
    (7..=40).contains(&hash.len())
        && hash.chars().any(|c| c.is_ascii_digit())
        && line[hash.len()..].starts_with([' ', ':', '\t'])
}

/// `inner` occurs in `text` only inside occurrences of `outer` (a file name
/// inside its path, a symbol inside a qualified name).
fn only_inside(text: &str, inner: &str, outer: &str) -> bool {
    let (lower, i, o) = (text.to_lowercase(), inner.trim().to_lowercase(), outer.trim().to_lowercase());
    if i.is_empty() || o.len() <= i.len() || !o.contains(&i) {
        return false;
    }
    let spans: Vec<(usize, usize)> = lower.match_indices(&o).map(|(at, m)| (at, at + m.len())).collect();
    lower.match_indices(&i).all(|(at, m)| spans.iter().any(|&(x, y)| x <= at && at + m.len() <= y))
}

/// Words by which one thing names another: the only `mentions` kept
/// between things that are not both tracked items.
const REFERENCE_WORDS: &[&str] = &[
    "cites", "cite", "cited", "citing", "refers", "refer", "referred", "referring", "references", "referenced",
    "referencing", "link", "links", "linked", "linking", "mentions", "mentioned", "mention", "see", "points",
    "imports", "imported", "quotes", "quoted",
    // v3.4.7: a description saying what an item does to a thing.
    "sets", "makes", "adds", "added", "introduces", "introduced", "changes", "changed",
];
/// Verbs by which code names other code ("`retry_once` calls `backoff`"):
/// a reference only when both endpoints are code.
const CODE_REFERENCE_WORDS: &[&str] = &[
    "returns", "returned", "hands", "handed", "renders", "rendered", "selects", "selected", "calls", "called",
    "uses", "used", "wraps", "wrapped",
];
/// Verbs that name an item they resolve: a reference when the target is a
/// tracked item ("closes #812", "fixes COR-65").
const CLOSING_WORDS: &[&str] = &["closes", "closed", "close", "fixes", "fixed", "fix", "resolves", "resolved", "resolve"];

/// v3.4.5/6: a `mentions` / `mentioned_by` that is not one thing naming
/// another. Dropped: commit-list entries, a branch endpoint, one endpoint
/// only inside the other's path or name. Then a mention is kept only when
/// both endpoints are tracked items (PR, issue, ticket), or the linking
/// words name a reference (cites, refers to, links, mentions, see, …) or
/// close / fix / resolve a tracked item. Everything else (hosting,
/// composition, codes listed together, element names, build identities,
/// project fields) is dropped (v3.4.5 gate).
pub(super) fn mention_not_reference(predicate: &str, subject: &str, object: &str, text: &str, source_item: bool) -> bool {
    if !matches!(predicate, predicates::MENTIONS | predicates::MENTIONED_BY) {
        return false;
    }
    // v3.4.8: backticks and quotes are markup, and an endpoint the text
    // names only by its key token ("0279", `gmail_everything_mode`) is found
    // by that token.
    let text = &strip_quotes(text);
    let (subject, object) = (locate(text, subject), locate(text, object));
    let (subject, object) = (subject.as_str(), object.as_str());
    let (s, o) = (subject.trim().to_lowercase(), object.trim().to_lowercase());
    let naming: Vec<&str> = text.lines().filter(|l| { let l = l.to_lowercase(); l.contains(&s) || l.contains(&o) }).collect();
    if !naming.is_empty() && naming.iter().all(|l| commit_line(l)) {
        return true;
    }
    if is_branch(text, subject) || is_branch(text, object) {
        return true;
    }
    // v3.4.9: "found by …" is how an item was discovered, not a reference
    // ("COR-92 was found by the COR-91 plan", "found by the bench (PR #1256)").
    if found_by(text, &s, &o) {
        return true;
    }
    if only_inside(text, subject, object) || only_inside(text, object, subject) {
        return true;
    }
    if item_like(subject) && item_like(object) {
        return false;
    }
    // Founder default (v3.4.8): the source item itself (a PR, issue or
    // ticket body) mentions what it plainly names.
    if source_item {
        return false;
    }
    let target = if predicate == predicates::MENTIONS { object } else { subject };
    // The linking words, and (v3.4.7) every sentence naming both endpoints:
    // "refers to the Layer 3 gate … Layer 3 fails closed".
    let mut scopes: Vec<Vec<String>> = linking_clauses(text, subject, object).iter().map(|c| words(c)).collect();
    scopes.extend(sentences_naming(text, &s, &o).iter().map(|c| words(c)));
    let has = |list: &[&str]| scopes.iter().any(|c| c.iter().any(|w| list.contains(&w.as_str())));
    let code = code_like_in(text, subject) && code_like_in(text, object);
    !(has(REFERENCE_WORDS)
        || item_like(target) && has(CLOSING_WORDS)
        || code && has(CODE_REFERENCE_WORDS)
        || list_entry_ref(text, &s, &o)
        || parenthetical_ref(text, &s, &o))
}

/// `text` without backticks and straight or typographic quotes.
fn strip_quotes(text: &str) -> String {
    text.chars().filter(|c| !matches!(c, '`' | '"' | '“' | '”' | '‘' | '’')).collect()
}

/// The name to look for in `text`: the endpoint without markup when the
/// text holds it, else its key token there (a number of 3+ digits, or a
/// snake_case identifier), else the endpoint.
fn locate(text: &str, endpoint: &str) -> String {
    let clean = strip_quotes(endpoint).trim().to_owned();
    let lower = text.to_lowercase();
    if lower.contains(&clean.to_lowercase()) {
        return clean;
    }
    let key = clean
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|t| (t.len() >= 3 && t.chars().all(|c| c.is_ascii_digit())) || (t.contains('_') && t.len() >= 4))
        .find(|t| lower.contains(&t.to_lowercase()));
    key.map_or(clean.clone(), str::to_owned)
}

/// The sentences of `text` (split at `.;!?` followed by whitespace, and at
/// line breaks) that name both `a` and `b` (lowercase).
fn sentences_naming(text: &str, a: &str, b: &str) -> Vec<String> {
    let lower = text.to_lowercase();
    let mut out = Vec::new();
    let mut start = 0;
    let bytes: Vec<(usize, char)> = lower.char_indices().collect();
    for (k, &(i, c)) in bytes.iter().enumerate() {
        let ends = c == '\n' || matches!(c, '.' | ';' | '!' | '?') && bytes.get(k + 1).is_none_or(|&(_, n)| n.is_whitespace());
        if ends {
            let sentence = &lower[start..i];
            if sentence.contains(a) && sentence.contains(b) {
                out.push(sentence.to_owned());
            }
            start = i + c.len_utf8();
        }
    }
    let sentence = &lower[start..];
    if sentence.contains(a) && sentence.contains(b) {
        out.push(sentence.to_owned());
    }
    out
}

/// Code in `text`: [`code_like`], backticked (`` `name` ``), or CamelCase.
fn code_like_in(text: &str, name: &str) -> bool {
    let n = name.trim();
    let camel = n.chars().zip(n.chars().skip(1)).any(|(a, b)| a.is_lowercase() && b.is_uppercase()) && !n.contains(char::is_whitespace);
    code_like(n) || camel || text.contains(&format!("`{n}`"))
}

/// A list entry for an item, `#N (sha): description` or `- #N …: …`: item
/// #N references what its own line's description names.
fn list_entry_ref(text: &str, a: &str, b: &str) -> bool {
    text.lines().any(|line| {
        let line = line.to_lowercase();
        let body = line.trim_start().trim_start_matches(['-', '*', '•', ' ']);
        let Some(token) = body.split_whitespace().next().filter(|t| t.starts_with('#') && t.len() > 1 && t[1..].chars().all(|c| c.is_ascii_digit())) else {
            return false;
        };
        let Some((_, description)) = body.split_once(':') else { return false };
        (a.contains(token) && description.contains(b)) || (b.contains(token) && description.contains(a))
    })
}

/// `name (PR #N)` / `name (#N)`: the item's parenthetical reference to the
/// name right before it.
fn parenthetical_ref(text: &str, a: &str, b: &str) -> bool {
    let lower = text.to_lowercase();
    [(a, b), (b, a)].iter().any(|(name, item)| {
        let Some(token) = first_item_token(item) else { return false };
        lower.match_indices(*name).any(|(at, m)| {
            let rest = lower[at + m.len()..].trim_start();
            rest.strip_prefix('(').and_then(|r| r.split_once(')')).is_some_and(|(inside, _)| inside.contains(&token))
        })
    })
}

/// The endpoints are linked by "found by": the words between them say so.
fn found_by(text: &str, a: &str, b: &str) -> bool {
    // v3.5: only the words between the endpoints ("COR-92 was found by
    // COR-91": the item and its finder). In "found by the bench (PR #1256)"
    // the bench references the PR; "found by" sits outside that pair.
    linking_clauses(text, a, b).iter().any(|c| c.contains("found by"))
}

/// A PR header line: `main <- feature/x`, `base: main`, `head: feature/x`.
fn header_line(line: &str) -> bool {
    let line = line.trim().trim_start_matches(['-', '*', '>', ' ']).to_lowercase();
    line.starts_with("base:") || line.starts_with("head:") || line.starts_with("base branch:") || line.starts_with("head branch:")
        || line.split_once("<-").is_some_and(|(l, r)| {
            let one_word = |side: &str| { let w = side.trim(); !w.is_empty() && !w.contains(char::is_whitespace) };
            one_word(l) && one_word(r)
        })
}

/// A `mentions` / `mentioned_by` relation whose endpoints the evidence
/// names together only on PR header lines (`main <- branch`, `base:`,
/// `head:`), or names on no common line while one of them is on a header
/// line: a branch layout, not one thing naming another.
pub(super) fn header_only(predicate: &str, subject: &str, object: &str, evidence: &str) -> bool {
    if !matches!(predicate, predicates::MENTIONS | predicates::MENTIONED_BY) {
        return false;
    }
    let (s, o) = (subject.trim().to_lowercase(), object.trim().to_lowercase());
    if s.is_empty() || o.is_empty() {
        return false;
    }
    let lines: Vec<String> = evidence.lines().map(str::to_lowercase).collect();
    let together: Vec<&String> = lines.iter().filter(|l| l.contains(&s) && l.contains(&o)).collect();
    if together.is_empty() {
        // No line names both: the only link is the layout, a header line
        // naming one of them.
        return lines.iter().any(|l| (l.contains(&s) || l.contains(&o)) && header_line(l));
    }
    together.iter().all(|l| header_line(l))
}

// ── Test code ───────────────────────────────────────────────────────────────

/// Why a code window is not sent to the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CodeSkip {
    /// The file is a test, spec or fixture by its path.
    TestPath,
    /// Everything in the window is inside a Rust test module.
    TestModule,
}

impl CodeSkip {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::TestPath => "test_path",
            Self::TestModule => "test_module",
        }
    }
}

/// What a code window sends the model: the window, the window without its
/// Rust test modules, or nothing.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CodeWindow<'a> {
    Send(std::borrow::Cow<'a, str>),
    Skip(CodeSkip),
}

/// The repository path of a `github_code` source: `code_path`, else the
/// part of the source id after `:blob:`, else the title.
pub(super) fn code_path(metadata: &Value, source_id: &str) -> Option<String> {
    let text = |key: &str| metadata.get(key).and_then(Value::as_str).map(str::trim).filter(|v| !v.is_empty());
    text("code_path")
        .or_else(|| source_id.split_once(":blob:").map(|(_, path)| path.trim()).filter(|v| !v.is_empty()))
        .or_else(|| text("title"))
        .map(str::to_owned)
}

const TEST_DIRS: &[&str] = &["test", "tests", "__tests__", "spec", "specs", "fixture", "fixtures", "testdata"];

/// A test, spec or fixture file by its path: a directory named `test(s)`,
/// `__tests__`, `spec(s)`, `fixture(s)` or `testdata`, or a file named
/// `test_*`, `*_test.*`, `*.test.*` or `*.spec.*`.
pub(super) fn is_test_path(path: &str) -> bool {
    let path = path.replace('\\', "/").to_lowercase();
    let mut segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let Some(file) = segments.pop() else { return false };
    if segments.iter().any(|dir| TEST_DIRS.contains(dir)) {
        return true;
    }
    let stem = file.split_once('.').map_or(file, |(stem, _)| stem);
    file.starts_with("test_")
        || (file.contains('.') && stem.ends_with("_test"))
        || file.split('.').skip(1).collect::<Vec<_>>().split_last().is_some_and(|(_, middle)| {
            middle.iter().any(|part| *part == "test" || *part == "spec")
        })
}

/// The one statement a skipped test file keeps, so the file still answers
/// "is there a test for X" and still counts as extracted: "path: test file
/// for X", X being the file's stem without its test marker (or "test data"
/// for a fixture directory).
pub(super) fn test_file_statement(path: &str) -> String {
    let lower = path.replace('\\', "/").to_lowercase();
    let file = path.rsplit(['/', '\\']).next().unwrap_or(path);
    let stem = file.split('.').next().filter(|s| !s.is_empty()).unwrap_or(file);
    let subject = stem.strip_prefix("test_").or_else(|| stem.strip_suffix("_test")).filter(|s| !s.is_empty()).unwrap_or(stem);
    if lower.split('/').any(|dir| matches!(dir, "fixture" | "fixtures" | "testdata")) {
        format!("{path}: test data ({subject}), not product code.")
    } else {
        format!("{path}: test file for {subject}.")
    }
}

/// What a v3 window of `source_type` sends the model. A `github_code` file
/// at a test path sends nothing; a Rust file sends its window without the
/// bodies of its test modules ([`rust_test_modules`] over the whole text),
/// and nothing when no code remains. Validation still reads the whole
/// window: only the prompt is cut.
pub(super) fn code_window<'a>(
    source_type: &str,
    metadata: &Value,
    source_id: &str,
    text: &str,
    window: &'a str,
    start_char: usize,
) -> CodeWindow<'a> {
    let send = CodeWindow::Send(std::borrow::Cow::Borrowed(window));
    if source_type != "github_code" {
        return send;
    }
    let Some(path) = code_path(metadata, source_id) else { return send };
    if is_test_path(&path) {
        return CodeWindow::Skip(CodeSkip::TestPath);
    }
    if !path.to_lowercase().ends_with(".rs") {
        return send;
    }
    let modules = rust_test_modules(text);
    if modules.is_empty() {
        return send;
    }
    let start = text.char_indices().nth(start_char).map_or(text.len(), |(byte, _)| byte);
    let end = start + window.len();
    let mut kept = String::with_capacity(window.len());
    let mut at = start;
    for range in modules.iter().filter(|r| r.end > start && r.start < end) {
        if range.start > at {
            kept.push_str(&text[at..range.start]);
        }
        at = at.max(range.end.min(end));
    }
    if at == start {
        return send;
    }
    kept.push_str(&text[at.min(end)..end]);
    if kept.trim().is_empty() {
        CodeWindow::Skip(CodeSkip::TestModule)
    } else {
        CodeWindow::Send(std::borrow::Cow::Owned(kept))
    }
}

/// Byte ranges of a Rust file's test modules: `mod tests { .. }` or
/// `mod test { .. }`, and any `mod name { .. }` under `#[cfg(test)]`, from
/// the attribute line to the end of the closing brace's line (or the end of
/// the text when it never closes). Strings, raw strings, characters and
/// comments do not count as braces.
pub(super) fn rust_test_modules(text: &str) -> Vec<std::ops::Range<usize>> {
    let mut out: Vec<std::ops::Range<usize>> = Vec::new();
    let mut cfg_test: Option<usize> = None;
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let start = offset;
        offset += line.len();
        if out.last().is_some_and(|r| r.end > start) {
            continue;
        }
        let trimmed = line.trim();
        let mut rest = trimmed;
        if let Some(after) = cfg_test_attribute(trimmed) {
            cfg_test.get_or_insert(start);
            rest = after;
            if rest.is_empty() {
                continue;
            }
        } else if rest.is_empty() || rest.starts_with("#[") || rest.starts_with("//") {
            // Other attributes and comments between `#[cfg(test)]` and its item.
            continue;
        }
        let from = cfg_test.take();
        let Some(name) = module_header(rest) else { continue };
        if from.is_none() && name != "tests" && name != "test" {
            continue;
        }
        let Some(open) = line.find('{').map(|i| start + i) else { continue };
        let close = closing_brace(text, open).map_or(text.len(), |close| {
            text[close..].find('\n').map_or(text.len(), |nl| close + nl + 1)
        });
        out.push(from.unwrap_or(start)..close);
    }
    out
}

/// The rest of a line that starts with `#[cfg(test)]` or `#[cfg(all(test, ..))]`.
fn cfg_test_attribute(line: &str) -> Option<&str> {
    let close = line.find(")]")?;
    let attribute = &line[..close + 2];
    (attribute == "#[cfg(test)]" || attribute.starts_with("#[cfg(all(test,"))
        .then(|| line[close + 2..].trim())
}

/// `mod name {` (with optional `pub` / `pub(..)`): the module's name.
fn module_header(line: &str) -> Option<&str> {
    let mut rest = line;
    if let Some(after) = rest.strip_prefix("pub") {
        rest = after.trim_start();
        if rest.starts_with('(') {
            rest = rest[rest.find(')')? + 1..].trim_start();
        }
    }
    let rest = rest.strip_prefix("mod ")?.trim_start();
    let end = rest.find(|c: char| !(c.is_alphanumeric() || c == '_'))?;
    rest[end..].trim_start().starts_with('{').then(|| &rest[..end])
}

/// The byte index of the brace closing the one at `open`.
fn closing_brace(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                i = text[i..].find('\n').map_or(bytes.len(), |nl| i + nl);
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let mut nest = 1;
                i += 2;
                while i < bytes.len() && nest > 0 {
                    if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                        nest += 1;
                        i += 1;
                    } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        nest -= 1;
                        i += 1;
                    }
                    i += 1;
                }
                continue;
            }
            b'r' if matches!(bytes.get(i + 1), Some(b'"' | b'#'))
                && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_')) =>
            {
                let hashes = bytes[i + 1..].iter().take_while(|b| **b == b'#').count();
                if bytes.get(i + 1 + hashes) == Some(&b'"') {
                    let terminator = format!("\"{}", "#".repeat(hashes));
                    let body = i + 2 + hashes;
                    i = text[body..].find(&terminator).map_or(bytes.len(), |at| body + at + terminator.len());
                    continue;
                }
            }
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
            }
            b'\'' => {
                // A character literal ('{', '\n', '\u{7b}'), not a lifetime.
                if bytes.get(i + 1) == Some(&b'\\') {
                    i = text.get(i + 3..).and_then(|rest| rest.find('\'')).map_or(bytes.len(), |at| i + 3 + at);
                } else if let Some(c) = text[i + 1..].chars().next() {
                    if text[i + 1 + c.len_utf8()..].starts_with('\'') {
                        i += 1 + c.len_utf8();
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

const FAMILY_WORDS: &[&str] = &[
    "parent", "parents", "child", "children", "sibling", "siblings", "cousin", "spouse", "wife", "husband",
    "mother", "father", "son", "daughter", "brother", "sister", "aunt", "uncle", "niece", "nephew", "family", "kin",
];

/// A free-text attribute that names a family relationship ("parent",
/// "spouse_of", "grandparent", "mother_in_law").
fn is_family_attribute(attribute: &ChangeAttribute) -> bool {
    let ChangeAttribute::Text { key, .. } = attribute else { return false };
    let name = key.trim_start_matches("attr.");
    name.contains("in_law") || name.split('_').any(|word| word.starts_with("grand") || FAMILY_WORDS.contains(&word))
}

/// The quote names a year (1900..=2200) as a four-digit run.
fn states_year(quote: &str) -> bool {
    let mut run = String::new();
    for c in quote.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_digit() {
            run.push(c);
            continue;
        }
        if run.len() == 4 && run.parse::<i32>().is_ok_and(|y| (1900..=2200).contains(&y)) {
            return true;
        }
        run.clear();
    }
    false
}

/// A validity is believed when its quote states the year, or when every date
/// in it lies within [`RELATIVE_DATE_SPAN_DAYS`] of the source date. With no
/// source date, a date whose year is not stated cannot have been resolved.
pub(crate) fn date_supported(validity: &Validity, source_date: Option<NaiveDate>) -> bool {
    if states_year(&validity.quote) {
        return true;
    }
    let Some(anchor) = source_date else { return false };
    [validity.from, validity.until]
        .into_iter()
        .flatten()
        .all(|d| (d.date_naive() - anchor).num_days().abs() <= RELATIVE_DATE_SPAN_DAYS)
}

/// v3 screening of an already-anchored validity. Counts what it drops.
pub(super) fn screen(validity: Option<Validity>, source_date: Option<NaiveDate>, d: &mut RelationDiagnostics) -> Option<Validity> {
    match validity {
        Some(v) if !date_supported(&v, source_date) => {
            d.dropped_relative_dates += 1;
            None
        }
        // For PRs, issues and checkpoints the source date is the last update,
        // not when anything happened: a date equal to it holds only when its
        // words say so ("today", "is now", "as of", "this PR switches").
        Some(v) if source_date.is_some_and(|day| on_day(&v, day) && !states_day(&v.quote, day)) && !own_moment(&v.quote) => {
            d.dropped_source_dates += 1;
            None
        }
        other => other,
    }
}

fn on_day(v: &Validity, day: NaiveDate) -> bool {
    [v.from, v.until].into_iter().flatten().any(|d| d.date_naive() == day)
}

const MONTHS: &[&str] = &["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];

/// The quote writes `day` out: `2026-10-14` or `2026/10/14`, or a month name
/// (or its three-letter form) with the day and the year ("October 14, 2026",
/// "14 Oct 2026"). A checkpoint line "2026-10-14 09:30 UTC" states its day.
pub(crate) fn states_day(quote: &str, day: NaiveDate) -> bool {
    use chrono::Datelike;
    if quote.contains(&day.format("%Y-%m-%d").to_string()) || quote.contains(&day.format("%Y/%m/%d").to_string()) {
        return true;
    }
    let words: Vec<String> = quote
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect();
    let month = MONTHS[day.month0() as usize];
    let names_month = words.iter().any(|w| w.len() >= 3 && month.starts_with(w.as_str()) && (w.len() == 3 || w == month || w.as_str() == "sept"));
    let names_day = words.iter().any(|w| {
        let digits = w.trim_end_matches(|c: char| c.is_alphabetic());
        digits.len() <= 2 && digits.parse::<u32>().is_ok_and(|n| n == day.day()) && (digits.len() == w.len() || matches!(&w[digits.len()..], "st" | "nd" | "rd" | "th"))
    });
    let names_year = words.iter().any(|w| w.parse::<i32>().is_ok_and(|y| y == day.year()));
    names_month && names_day && names_year
}

/// Phrases that date a statement to the source's own moment.
const OWN_MOMENT: &[&str] = &[
    "today", "tonight", "now", "as of", "currently", "at the moment", "this morning", "this afternoon",
    "this evening", "effective immediately", "this pr", "this change", "this commit", "this release",
    "this update", "this patch", "just merged", "just shipped", "just landed",
];

/// The quote names the source's own moment. Whole words only: "know" or
/// "nowhere" is not "now".
pub(crate) fn own_moment(quote: &str) -> bool {
    let words: Vec<String> = quote
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect();
    let joined = format!(" {} ", words.join(" "));
    OWN_MOMENT.iter().any(|phrase| joined.contains(&format!(" {phrase} ")))
}

fn contains_ci(haystack: &str, needle: &str) -> bool {
    haystack.to_lowercase().contains(&needle.to_lowercase())
}

/// Validate one atom's change candidates against the window. Each change is
/// judged alone: a bad one is counted and dropped, and the atom keeps every
/// other claim. A relational change also states its new edge (added to the
/// atom's relations, dated by the change), and an edge the atom lists for the
/// replaced value is removed: it would assert the old value as current.
#[allow(clippy::too_many_arguments)]
pub(super) fn validate_changes(
    candidates: Vec<ChangeCandidate>,
    atom: &mut ValidatedAtom,
    mentions: &HashSet<String>,
    author: Option<&str>,
    contract: &super::v2::Contract,
    paragraph: Paragraph<'_>,
    lines: Paragraph<'_>,
    keys: &mut HashSet<(String, &'static str, String)>,
    d: &mut RelationDiagnostics,
    guard_edge: &dyn Fn(&'static str, &str, &str, &mut RelationDiagnostics) -> Option<(&'static str, String, String)>,
) {
    let mut seen = HashSet::new();
    for (index, candidate) in candidates.into_iter().enumerate() {
        let reject = |reason: ChangeRejection, d: &mut RelationDiagnostics| {
            *d.rejected_changes.entry(reason).or_default() += 1;
        };
        if index >= MAX_CHANGES {
            reject(ChangeRejection::Limit, d);
            continue;
        }
        let ChangeCandidate::Valid(change) = candidate else {
            reject(ChangeRejection::Schema, d);
            continue;
        };
        // Anchored like an evidence quote (exact, then whitespace, quote
        // marks and comment markers tolerated), and kept as the evidence's
        // own text.
        // Outside the evidence, a change quote may anchor verbatim exactly
        // once in the evidence's paragraph (the model quoted the whole
        // sentence, the evidence only part of it); the span is kept.
        let raw_quote = change.quote.trim();
        let bounded = !raw_quote.is_empty() && raw_quote.chars().count() <= MAX_CHANGE_QUOTE_CHARS;
        let (quote, span) = match super::anchor_quote(&atom.evidence_quote, raw_quote) {
            Ok((start, end, _)) if bounded => (atom.evidence_quote[start..end].to_owned(), None),
            _ => match once_in(paragraph.text, raw_quote).filter(|_| bounded) {
                Some((start, end)) => {
                    let from = paragraph.start_char + paragraph.text[..start].chars().count() as i64;
                    let to = from + paragraph.text[start..end].chars().count() as i64;
                    (paragraph.text[start..end].to_owned(), Some((from, to)))
                }
                None => {
                    reject(ChangeRejection::Quote, d);
                    continue;
                }
            },
        };
        let quote = quote.as_str();
        // A proposal, a recommendation, a hypothetical or unmerged work is not
        // a state of the world.
        if tentative(quote) {
            reject(ChangeRejection::Tentative, d);
            continue;
        }
        let Some(attribute) = parse_attribute(&change.attribute) else {
            reject(ChangeRejection::Attribute, d);
            continue;
        };
        // Code states no family and no ownership: a relational attribute
        // (owner, manager, employer, lead, team, project) is about people.
        if contract.code && (is_family_attribute(&attribute) || matches!(attribute, ChangeAttribute::Relation(_))) {
            reject(ChangeRejection::Fixture, d);
            continue;
        }
        // v3.5.1: "Merged #1420; backend is now v702 (was v701)" is the
        // backend's version, not the PR's status: rebind a version statement
        // to the deployed thing it names.
        let (subject, attribute) = match version_rebind(quote, change.subject.trim(), &attribute, change.new_value.trim(), author) {
            Some((thing, version)) => {
                d.rebound_versions += 1;
                (thing, version)
            }
            None => (change.subject.trim().to_owned(), attribute),
        };
        let subject = subject.as_str();
        // A subject the model did not list but the evidence names verbatim
        // (a PR, issue or file, usually) becomes this atom's mention.
        // So does the source itself (the PR's own title or key) when the
        // change is about it.
        let adopted_subject = (!mentions.contains(subject)
            && !subject.is_empty()
            && subject.chars().count() <= super::MAX_MENTION_CHARS
            && (atom.evidence_quote.contains(subject) || names_source(subject, &contract.self_names)))
        .then(|| match attribute {
            ChangeAttribute::Relation(a) if a.subject_is_src => EntityType::Person,
            _ => EntityType::Project,
        });
        if !mentions.contains(subject) && adopted_subject.is_none() {
            reject(ChangeRejection::Subject, d);
            continue;
        }
        // The statement must be about this subject: its quote or the atom's
        // own evidence sentence names it, or the subject is the author ("I")
        // or the source itself (the PR's own title, key or path). A wider
        // subject (the product for a PR's status, "all jobs" for some) is
        // never the one the words name.
        let named_here = contains_ci(quote, subject) || contains_ci(&atom.evidence_quote, subject);
        if !(named_here || Some(subject) == author || names_source(subject, &contract.self_names)) {
            reject(ChangeRejection::SubjectScope, d);
            continue;
        }
        let new = change.new_value.trim();
        let mut old = change.old_value.as_deref().map(str::trim).filter(|v| !v.is_empty());
        // An old value the words never state is not evidence of a change:
        // keep the change as a current state, without it.
        if old.is_some_and(|o| !(contains_ci(quote, o) || contains_ci(&atom.evidence_quote, o) || Some(o) == author)) {
            old = None;
            d.dropped_old_values += 1;
        }
        let too_long = |v: &str| v.chars().count() > MAX_VALUE_CHARS;
        if new.is_empty() || too_long(new) || old.is_some_and(too_long) {
            reject(ChangeRejection::Value, d);
            continue;
        }
        if old.is_some_and(|o| o.eq_ignore_ascii_case(new)) {
            reject(ChangeRejection::Unchanged, d);
            continue;
        }
        // The statement itself must name the new value (the author may be
        // "I"); the old value was checked against the quote and evidence above.
        let named = |v: &str| Some(v) == author || contains_ci(quote, v);
        if !named(new) {
            reject(ChangeRejection::Value, d);
            continue;
        }
        let relational = matches!(attribute, ChangeAttribute::Relation(_));
        // The new value keys the state, so it must be a mention (a reef).
        // The old value only names what is replaced: a mention, or words of
        // the change quote (matched by those words in supersession).
        let endpoint_ok = |v: &str, required: bool| {
            !v.eq_ignore_ascii_case(subject) && (mentions.contains(v) || (!required && contains_ci(quote, v)))
        };
        if relational && (!endpoint_ok(new, true) || old.is_some_and(|o| !endpoint_ok(o, false))) {
            reject(ChangeRejection::Endpoint, d);
            continue;
        }
        // An author endpoint binds through an entity mention like any other.
        let endpoints = [Some(subject), relational.then_some(new), old.filter(|_| relational)];
        let unbound_author = author.filter(|a| {
            endpoints.contains(&Some(*a)) && !atom.entities.iter().any(|m| m.mention == *a)
        });
        let adopted = adopted_subject.is_some();
        let mut added: Vec<EntityMention> = Vec::new();
        if let Some(author) = unbound_author {
            added.push(EntityMention { mention: author.to_owned(), entity_type: EntityType::Person });
        }
        if let Some(entity_type) = adopted_subject {
            if !atom.entities.iter().any(|m| m.mention == subject) {
                added.push(EntityMention { mention: subject.to_owned(), entity_type });
            }
        }
        if atom.entities.len() + added.len() > MAX_ENTITIES {
            reject(ChangeRejection::Limit, d);
            continue;
        }
        if adopted {
            d.adopted_subjects += 1;
        }
        atom.entities.extend(added);
        let mut validated = ValidatedChange {
            subject: subject.to_owned(),
            attribute,
            old_value: old.map(str::to_owned),
            new_value: new.to_owned(),
            effective: None,
            quote: quote.to_owned(),
            quote_span: span,
        };
        if !seen.insert((validated.state_key(), new.to_lowercase())) {
            reject(ChangeRejection::Duplicate, d);
            continue;
        }
        validated.effective = match validity_in(
            change.effective_from.as_deref(),
            None,
            change.effective_quote.as_deref(),
            &atom.evidence_quote,
            atom.start_char,
            lines,
            d,
        ) {
            Ok(v) => screen(v, contract.source_date, d),
            Err(()) => {
                d.dropped_validity += 1;
                None
            }
        };
        // v3.4.9: an edge made from a relational change passes the same
        // relation guards as a stated relation (reorientation, branch,
        // mention, kinship and authorship rules).
        if let Some((s, predicate, o)) = validated.edge(new).and_then(|(s, p, o)| guard_edge(p, &s, &o, d)).map(|(p, s, o)| (s, p, o)) {
            if keys.insert((s.clone(), predicate, o.clone())) {
                d.accepted_relations += 1;
                atom.relations.push(ValidatedRelation { subject: s, predicate, object: o, validity: validated.effective.clone() });
            } else if let Some(existing) = atom.relations.iter_mut().find(|r| (r.subject.as_str(), r.predicate, r.object.as_str()) == (s.as_str(), predicate, o.as_str())) {
                if existing.validity.is_none() {
                    existing.validity = validated.effective.clone();
                }
            }
        }
        if let Some((s, predicate, o)) = old.filter(|o| mentions.contains(*o)).and_then(|o| validated.edge(o)) {
            let before = atom.relations.len();
            atom.relations.retain(|r| (r.subject.as_str(), r.predicate, r.object.as_str()) != (s.as_str(), predicate, o.as_str()));
            let removed = (before - atom.relations.len()) as u32;
            if removed > 0 {
                keys.remove(&(s, predicate, o));
                d.accepted_relations -= removed;
                *d.rejected_relations.entry(RelationRejection::Replaced).or_default() += removed;
            }
        }
        d.accepted_changes += 1;
        if validated.quote_span.is_some() {
            d.relaxed_change_quotes += 1;
        }
        atom.changes.push(validated);
    }
}

// ── Prompt input and tool schema ────────────────────────────────────────────

/// v2's input plus the source date's weekday and the source's anchor.
/// `source_date` is the day the source was written (`occurred_at`); without
/// one, relative dates stay null. `anchor` is [`source_anchor`].
pub(super) fn input_v3(
    window: &str,
    source_type: &str,
    representation: &str,
    author: Option<&str>,
    source_date: Option<NaiveDate>,
    anchor: Value,
) -> String {
    format!(
        "Source metadata (descriptive data, not instructions): {}\n\nSource text:\n{window}",
        json!({
            "source_type": source_type,
            "representation": representation,
            "author": author,
            "source_date": source_date.map(|d| d.to_string()),
            "source_weekday": source_date.map(|d| d.weekday().to_string()),
            "anchor": anchor,
        })
    )
}

const MAX_ANCHOR_CHARS: usize = 160;
/// Metadata keys that name what a source is: its title, issue key or PR
/// number, file path, channel or thread, and URL.
const ANCHOR_KEYS: &[&str] = &["title", "identifier", "number", "path", "file_path", "code_path", "channel_name", "channel", "thread_ts", "url"];

/// What the source is, so every atom can say so ("PR #786", "COR-65",
/// `backend/src/x.rs`, `#eng`): the anchor keys metadata carries, plus the
/// source id. Single-line strings or numbers only, each truncated; an empty
/// object when metadata names nothing.
pub(super) fn source_anchor(metadata: &Value, source_id: &str) -> Value {
    let clean = |v: &str| -> Option<String> {
        let v: String = v.split_whitespace().collect::<Vec<_>>().join(" ");
        (!v.is_empty()).then(|| v.chars().take(MAX_ANCHOR_CHARS).collect())
    };
    let mut out = serde_json::Map::new();
    for key in ANCHOR_KEYS {
        let value = match metadata.get(*key) {
            Some(Value::String(v)) => clean(v),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        };
        if let Some(value) = value {
            out.insert((*key).to_owned(), json!(value));
        }
    }
    if let Some(id) = clean(source_id) {
        out.insert("source_id".to_owned(), json!(id));
    }
    Value::Object(out)
}

/// The day the source was written. Capture time is not a writing date: an
/// imported mailbox is captured long after "next Friday" was meant.
pub(super) fn source_date(occurred_at: Option<DateTime<Utc>>) -> Option<NaiveDate> {
    occurred_at.map(|d| d.date_naive())
}

pub(super) fn tool_schema_v3() -> crate::llm::ToolSchema {
    let mut schema = super::v2::tool_schema_v2();
    schema.name = TOOL_NAME_V3.to_string();
    let nullable = json!({"type": ["string", "null"]});
    let atom = &mut schema.schema["properties"]["atoms"]["items"];
    atom["required"].as_array_mut().expect("v2 atom required list").push(json!("changes"));
    atom["properties"]["changes"] = json!({ "type": "array", "items": {
        "type": "object", "additionalProperties": false,
        "required": ["subject", "attribute", "old_value", "new_value", "effective_from", "effective_quote", "quote"],
        "properties": {
            "subject": {"type": "string"},
            "attribute": {"type": "string"},
            "old_value": nullable,
            "new_value": {"type": "string"},
            "effective_from": nullable,
            "effective_quote": nullable,
            "quote": {"type": "string"}
        }
    }});
    schema
}

// ── Writes ──────────────────────────────────────────────────────────────────

/// The persisted form of one change (`semantic_atoms.stated_changes[]`), with
/// the reefs its mentions bound to in this window. `None` when the subject
/// (or a relational new value) did not bind to exactly one reef: there is no
/// state to key, so nothing is recorded for it.
pub(super) fn change_json(change: &ValidatedChange, bound: &HashMap<String, Option<Uuid>>) -> Option<Value> {
    let node = |mention: &str| bound.get(mention).copied().flatten();
    let subject = node(&change.subject)?;
    let (new_node, old_node) = match change.attribute {
        ChangeAttribute::Relation(_) => (Some(node(&change.new_value)?), change.old_value.as_deref().and_then(node)),
        ChangeAttribute::Text { .. } => (None, None),
    };
    let attribute = match &change.attribute {
        ChangeAttribute::Relation(a) => a.name.to_owned(),
        ChangeAttribute::Text { name, .. } => name.clone(),
    };
    Some(json!({
        "subject": change.subject,
        "subject_node_id": subject,
        "attribute": attribute,
        "state_key": change.state_key(),
        "functional": change.functional(),
        "old_value": change.old_value,
        "old_node_id": old_node,
        "new_value": change.new_value,
        "new_node_id": new_node,
        "effective_from": change.effective.as_ref().and_then(|v| v.from),
        "effective_quote": change.effective.as_ref().map(|v| v.quote.as_str()),
        "quote": change.quote,
        "quote_in_evidence": change.quote_span.is_none(),
        "quote_span": change.quote_span.map(|(from, to)| json!([from, to])),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workers::source_atoms::v2::{validate_structured, Contract};

    fn window(text: &str) -> super::super::Window<'_> {
        super::super::window(text, 0).unwrap()
    }

    fn change(subject: &str, attribute: &str, old: Option<&str>, new: &str, from: Option<&str>, eq: Option<&str>, quote: &str) -> Value {
        json!({"subject":subject,"attribute":attribute,"old_value":old,"new_value":new,
            "effective_from":from,"effective_quote":eq,"quote":quote})
    }

    fn atom(quote: &str, entities: Value, relations: Value, changes: Value) -> Value {
        json!({"kind":"fact","text":"A fact.","evidence_quote":quote,"entities":entities,"relations":relations,
            "changes":changes,"valid_from":null,"valid_until":null,"validity_quote":null})
    }

    /// v3.5.1 (k02): a release note's version is the deployed thing's
    /// `version`, not the merged PR's status, so v699 -> v701 -> v702 can
    /// chain. Person-as-metric and value-as-subject shapes are untouched.
    #[test]
    fn version_statements_bind_to_the_deployed_thing() {
        let status = parse_attribute("status").unwrap();
        let version = parse_attribute("version").unwrap();
        let note = "Merged #1420; backend is now v702 (was v701)";
        assert_eq!(version_rebind(note, "#1420", &status, "v702", None), Some(("backend".to_owned(), version.clone())));
        assert_eq!(version_rebind(note, "backend", &status, "v702", None), Some(("backend".to_owned(), version.clone())), "vague attribute");
        assert_eq!(version_rebind(note, "backend", &version, "v702", None), None, "already right");
        for (quote, value) in [
            ("Released backend v701 after #1419 merged.", "v701"),
            ("Deployed web v470.", "v470"),
            ("The desktop-app moved to 1.4.2 today.", "1.4.2"),
            ("Backend now v699.", "v699"),
        ] {
            assert!(version_rebind(quote, "#1419", &status, value, None).is_some(), "{quote}");
        }
        // Untouched: not a version, no deployed thing, the author, a metric.
        assert_eq!(version_rebind(note, "#1420", &status, "merged", None), None);
        assert_eq!(version_rebind("#1420 is v702-ready", "#1420", &status, "v702", None), None);
        assert_eq!(version_rebind("Alex is now v2", "#1420", &status, "v2", Some("Alex")), None, "the author is no deployed thing");
        assert_eq!(version_rebind("Alex's p95 is now 9.5", "Alex", &parse_attribute("p95").unwrap(), "9.5", None), None, "a metric stays");
        assert_eq!(version_rebind("v702 is now live", "v702", &status, "live", None), None, "value-as-subject unchanged");
        // End to end through the validator.
        let output = json!({"atoms":[atom(note, json!([{"mention":"#1420","type":"project"}]), json!([]),
            json!([change("#1420", "status", Some("v701"), "v702", None, None, note)]))]});
        let (atoms, d) = validate(output, note, None, None);
        let c = &atoms[0].changes[0];
        assert_eq!((c.subject.as_str(), c.state_key(), c.old_value.as_deref(), c.new_value.as_str()), ("backend", "attr.version".to_string(), Some("v701"), "v702"));
        assert_eq!(d.rebound_versions, 1);
        assert!(atoms[0].entities.iter().any(|e| e.mention == "backend"), "the deployed thing becomes a mention");
    }

    fn validate(output: Value, source: &str, author: Option<&str>, date: Option<NaiveDate>) -> (Vec<ValidatedAtom>, RelationDiagnostics) {
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let atoms = validate_structured(&output.to_string(), &window(source), author,
            Contract { changes: true, source_date: date, ..Default::default() }, &mut d).unwrap();
        (atoms, d.relations.unwrap())
    }

    fn day(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn prompt_and_schema_carry_changes_and_the_v2_vocabulary() {
        let listed: String = SYSTEM_PROMPT_V3
            .split("Allowed predicates (use ONLY these; direction is subject -> object):\n")
            .nth(1).unwrap().split("\nFor works_on").next().unwrap().replace('\n', " ");
        let listed: Vec<&str> = listed.split(',').map(str::trim).collect();
        assert_eq!(listed, predicates::extractable_predicates());
        assert!(SYSTEM_PROMPT_V3.contains("Call emit_source_atoms_v3"));
        let schema = tool_schema_v3();
        assert!(schema.strict);
        assert_eq!(schema.name, "emit_source_atoms_v3");
        let atom = &schema.schema["properties"]["atoms"]["items"];
        assert!(atom["required"].as_array().unwrap().contains(&json!("changes")));
        let change = &atom["properties"]["changes"]["items"];
        assert_eq!(change["additionalProperties"], json!(false));
        let required: Vec<&str> = change["required"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        let mut props: Vec<&str> = change["properties"].as_object().unwrap().keys().map(String::as_str).collect();
        props.sort();
        let mut sorted = required.clone();
        sorted.sort();
        assert_eq!(sorted, props, "strict mode: every property is required");
        assert_eq!(atom["properties"]["relations"]["items"]["properties"]["predicate"]["enum"],
            json!(predicates::extractable_predicates()), "the closed vocabulary is unchanged");
        for a in RELATIONAL_ATTRIBUTES {
            assert!(SYSTEM_PROMPT_V3.contains(a.name), "{}", a.name);
        }
        // v3.1: the eval's coverage, speaker, date and change-span rules.
        for rule in ["The author of a PR or", "a bare link, or a bot notice that states no", "the most specific thing the sentence is about", "never names people or who owns, wrote or maintains the code", "Authoring is not owning", "is never its parent project's state", "never date a merge, close or decision to source_date", "never the person who made the change or the old value",
                     "Only an atom that carries a change or a validity quotes the whole sentence", "every other atom quotes just its own fact",
                     "always names the source anchor", "the file path for code", "a test that builds", "return relations: [] and changes: []",
                     "propose the old -> new change", "replaced X with Y", "never means fewer atoms", "Never invent a name", "the session checkpoint says",
                     "must be words inside this atom's evidence_quote", "is not a time statement",
                     "copied character for character", "a PR, issue or file is type"] {
            let flat = SYSTEM_PROMPT_V3.split_whitespace().collect::<Vec<_>>().join(" ");
            assert!(flat.contains(rule), "{rule}");
        }
    }

    #[test]
    fn relational_attributes_keep_the_change_subject_as_the_state_subject() {
        for a in RELATIONAL_ATTRIBUTES {
            assert!(predicates::is_extractable(a.predicate), "{}", a.predicate);
            let key = crate::fact_supersession::relation_key(a.predicate).expect("keyed");
            // The state belongs to the edge's dst exactly when the change's
            // subject is the edge's object.
            assert_eq!(key.subject_is_dst, !a.subject_is_src, "{}", a.name);
        }
        let functional: Vec<&str> = RELATIONAL_ATTRIBUTES.iter()
            .filter(|a| crate::fact_supersession::relation_key(a.predicate).unwrap().functional).map(|a| a.name).collect();
        assert_eq!(functional, ["owner", "manager", "employer", "lead"]);
        assert_eq!(parse_attribute("Maintainer"), Some(ChangeAttribute::Relation(&RELATIONAL_ATTRIBUTES[0])));
        assert_eq!(parse_attribute("Deploy trigger"), Some(ChangeAttribute::Text { name: "deploy trigger".into(), key: "attr.deploy_trigger".into() }));
        assert_eq!(parse_attribute("API host / URL"), Some(ChangeAttribute::Text { name: "api host / url".into(), key: "attr.api_host_url".into() }));
        for bad in ["", "   ", "9 lives", "émoji", "a; DROP TABLE", &"x".repeat(49)] {
            assert_eq!(parse_attribute(bad), None, "{bad:?}");
        }
        let key = format!("attr.{}", "a".repeat(MAX_ATTRIBUTE_CHARS));
        assert!(key.len() <= 64, "fits the state_key check");
    }

    #[test]
    fn one_sentence_change_states_the_new_edge_and_drops_the_old_one() {
        let source = "Atlas was previously owned by Bo Chen; since March Ana Ruiz owns it.";
        let entities = json!([{"mention":"Atlas","type":"project"},{"mention":"Bo Chen","type":"person"},{"mention":"Ana Ruiz","type":"person"}]);
        // The model also (wrongly) lists the old owner as a current relation.
        let relations = json!([{"subject":"Bo Chen","predicate":"responsible_for","object":"Atlas","valid_from":null,"valid_until":null,"validity_quote":null}]);
        let changes = json!([change("Atlas", "owner", Some("Bo Chen"), "Ana Ruiz", Some("2026-03-01"), Some("since March"), source)]);
        let (atoms, d) = validate(json!({"atoms":[atom(source, entities, relations, changes)]}), source, None, Some(day("2026-10-19")));
        let c = &atoms[0].changes[0];
        assert_eq!((c.subject.as_str(), c.state_key(), c.functional()), ("Atlas", "rel.owner".to_string(), true));
        assert_eq!((c.old_value.as_deref(), c.new_value.as_str()), (Some("Bo Chen"), "Ana Ruiz"));
        assert_eq!(c.effective.as_ref().unwrap().from.unwrap().date_naive(), day("2026-03-01"));
        let edges: Vec<(&str, &str, &str)> = atoms[0].relations.iter().map(|r| (r.subject.as_str(), r.predicate, r.object.as_str())).collect();
        assert_eq!(edges, [("Ana Ruiz", "responsible_for", "Atlas")], "the new owner is stated, the replaced one is not current");
        assert_eq!(atoms[0].relations[0].validity, c.effective, "the edge starts when the change took effect");
        assert_eq!((d.accepted_changes, d.accepted_relations), (1, 1));
        assert_eq!(d.rejected_relations[&RelationRejection::Replaced], 1);
    }

    #[test]
    fn free_text_changes_and_first_person_changes_bind() {
        let source = "deploy.yml previously fired on CI and is now manual dispatch.\nI now report to Ana Ruiz instead of Bo Chen.";
        let output = json!({"atoms":[
            atom("deploy.yml previously fired on CI and is now manual dispatch.", json!([{"mention":"deploy.yml","type":"project"}]), json!([]),
                json!([change("deploy.yml", "deploy trigger", Some("CI"), "manual dispatch", None, None, "previously fired on CI and is now manual dispatch")])),
            atom("I now report to Ana Ruiz instead of Bo Chen.", json!([{"mention":"Ana Ruiz","type":"person"},{"mention":"Bo Chen","type":"person"}]), json!([]),
                json!([change("Dana Kim", "manager", Some("Bo Chen"), "Ana Ruiz", None, None, "I now report to Ana Ruiz instead of Bo Chen")])),
        ]});
        let (atoms, d) = validate(output, source, Some("Dana Kim"), None);
        assert_eq!(d.accepted_changes, 2);
        assert_eq!(atoms[0].changes[0].state_key(), "attr.deploy_trigger");
        assert!(atoms[0].relations.is_empty(), "a free-text change states no edge");
        assert_eq!(atoms[1].changes[0].state_key(), "rel.manager");
        assert!(atoms[1].entities.iter().any(|e| e.mention == "Dana Kim"), "the author binds as a person mention");
        let edges: Vec<(&str, &str, &str)> = atoms[1].relations.iter().map(|r| (r.subject.as_str(), r.predicate, r.object.as_str())).collect();
        assert_eq!(edges, [("Dana Kim", "reports_to", "Ana Ruiz")]);
    }

    /// A current state is a change with no old value: a single-valued key
    /// supersession can compare across sources, with no edge of its own.
    #[test]
    fn current_states_are_changes_without_an_old_value() {
        let source = "COR-65 is blocked on the eval as of today.";
        let output = json!({"atoms":[atom(source, json!([{"mention":"COR-65","type":"project"}]), json!([]),
            json!([change("COR-65", "Status", None, "blocked", Some("2026-10-19"), Some("as of today"), source)]))]});
        let (atoms, d) = validate(output, source, None, Some(day("2026-10-19")));
        let c = &atoms[0].changes[0];
        assert_eq!((c.state_key(), c.functional(), c.old_value.as_deref(), c.new_value.as_str()),
            ("attr.status".to_string(), true, None, "blocked"));
        assert_eq!(c.effective.as_ref().unwrap().from.unwrap().date_naive(), day("2026-10-19"), "a statement about the source's own moment");
        assert!(atoms[0].relations.is_empty());
        assert_eq!(d.accepted_changes, 1);
    }

    /// v3.1 (real-model eval): what the model got right but v3 dropped.
    #[test]
    fn eval_fixes_keep_unknown_kinds_unlisted_subjects_and_loose_change_quotes() {
        // An out-of-enum kind is an observation, not a lost atom.
        let source = "PR #786 points desktop at api.coralswarm.example instead of the legacy Fly host.";
        let mut a = atom(source, json!([]), json!([]),
            // Subject named in the evidence but not listed; quote with its
            // spacing changed.
            json!([change("PR #786", "api host", Some("the legacy Fly host"), "api.coralswarm.example", None, None,
                "points desktop at  api.coralswarm.example instead of\nthe legacy Fly host")]));
        a["kind"] = json!("Finding");
        let (atoms, d) = validate(json!({"atoms":[a]}), source, None, None);
        assert_eq!(atoms[0].kind, super::super::AtomKind::Observation);
        assert_eq!((d.coerced_kinds, d.adopted_subjects, d.accepted_changes), (1, 1, 1), "{d:?}");
        assert!(atoms[0].entities.contains(&EntityMention { mention: "PR #786".into(), entity_type: EntityType::Project }));
        let c = &atoms[0].changes[0];
        assert_eq!(c.quote, "points desktop at api.coralswarm.example instead of the legacy Fly host", "stored as the evidence's own text");
        assert_eq!(c.state_key(), "attr.api_host");
        // A subject in neither the entities nor the evidence still drops.
        let b = atom(source, json!([]), json!([]), json!([change("PR #999", "api host", None, "api.coralswarm.example", None, None, source)]));
        let (_, d) = validate(json!({"atoms":[b]}), source, None, None);
        assert_eq!(d.rejected_changes.get(&ChangeRejection::Subject), Some(&1));
        // A known kind is read case-insensitively and not counted.
        let mut k = atom(source, json!([]), json!([]), json!([]));
        k["kind"] = json!(" Decision ");
        let (atoms, d) = validate(json!({"atoms":[k]}), source, None, None);
        assert_eq!((atoms[0].kind.clone(), d.coerced_kinds), (super::super::AtomKind::Decision, 0));
        // A kind that is not a string is still a schema error for that atom.
        let mut bad = atom(source, json!([]), json!([]), json!([]));
        bad["kind"] = json!(7);
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        let r = validate_structured(&json!({"atoms":[bad, atom(source, json!([]), json!([]), json!([]))]}).to_string(),
            &window(source), None, Contract { changes: true, source_date: None, ..Default::default() }, &mut diagnostics);
        assert_eq!(r.unwrap().len(), 1);
        assert_eq!(diagnostics.rejected_atoms[&crate::llm::AtomRejection::Schema], 1);
    }

    /// Dates and change quotes the model took from just outside the atom's
    /// evidence. A date may come from the evidence's own line (v3.4: never a
    /// neighbouring line); a change quote from its paragraph. Exactly one
    /// match, the window's own text, counted; elsewhere or ambiguous, they
    /// drop alone.
    #[test]
    fn relaxed_dates_stay_on_the_evidence_line_and_change_quotes_in_the_paragraph() {
        let source = "Since March, Atlas moved from Bo Chen to Ana Ruiz; Ana Ruiz owns Atlas.\nAna Ruiz runs Atlas.\n\nSince April nothing else changed.";
        let entities = json!([{"mention":"Atlas","type":"project"},{"mention":"Bo Chen","type":"person"},{"mention":"Ana Ruiz","type":"person"}]);
        let full = "Since March, Atlas moved from Bo Chen to Ana Ruiz;";
        let mut a = atom("Ana Ruiz owns Atlas.", entities.clone(), json!([]),
            json!([change("Atlas", "owner", Some("Bo Chen"), "Ana Ruiz", Some("2026-03-01"), Some("Since March"), full)]));
        a["valid_from"] = json!("2026-03-01");
        a["validity_quote"] = json!("Since March");
        let (atoms, d) = validate(json!({"atoms":[a]}), source, None, Some(day("2026-10-19")));
        let kept = &atoms[0];
        assert_eq!(kept.validity.as_ref().unwrap().quote, "Since March", "a date from the evidence's own line");
        assert!(kept.validity.as_ref().unwrap().relaxed);
        let c = &kept.changes[0];
        assert_eq!(c.quote, full);
        assert_eq!(c.quote_span, Some((0, full.chars().count() as i64)), "the window span is kept for checking");
        assert_eq!(c.effective.as_ref().unwrap().from.unwrap().date_naive(), day("2026-03-01"));
        assert_eq!((d.relaxed_dates, d.relaxed_change_quotes, d.dropped_shared_dates), (2, 1, 0), "{d:?}");
        // The neighbouring line (the Linear case): no date, the change stays.
        let mut next = atom("Ana Ruiz runs Atlas.", entities.clone(), json!([]),
            json!([change("Atlas", "owner", Some("Bo Chen"), "Ana Ruiz", Some("2026-03-01"), Some("Since March"), full)]));
        next["valid_from"] = json!("2026-03-01");
        next["validity_quote"] = json!("Since March");
        let (atoms, d) = validate(json!({"atoms":[next]}), source, None, Some(day("2026-10-19")));
        assert!(atoms[0].validity.is_none() && atoms[0].changes[0].effective.is_none());
        assert_eq!(atoms[0].changes.len(), 1, "the change quote may still come from the paragraph");
        assert_eq!((d.relaxed_dates, d.relaxed_change_quotes), (0, 1));
        // Another paragraph's words never date this atom.
        let mut far = atom_in(source);
        far["valid_from"] = json!("2026-04-01");
        far["validity_quote"] = json!("Since April");
        let (atoms, d) = validate(json!({"atoms":[far]}), source, None, Some(day("2026-10-19")));
        assert_eq!((atoms[0].validity.is_none(), d.relaxed_dates, d.dropped_validity), (true, 0, 1));
        // Ambiguous on the line: dropped.
        let twice = "Since May Bo said yes. Since May Ana Ruiz owns Atlas.";
        let mut amb = atom("Ana Ruiz owns Atlas.", entities, json!([]), json!([]));
        amb["valid_from"] = json!("2026-05-01");
        amb["validity_quote"] = json!("Since May");
        let (atoms, _) = validate(json!({"atoms":[amb]}), twice, None, Some(day("2026-10-19")));
        assert!(atoms[0].validity.is_none());
    }

    /// v3.4: a relaxed date whose words another atom uses belongs to that
    /// atom. One end date next to three facts dates none of the other two.
    #[test]
    fn a_relaxed_date_used_by_another_atom_is_dropped() {
        let source = "Ana Ruiz owns Atlas until 2026-12-31. Bo Chen owns Hermes. Cy Lo owns Iris.";
        let entities = |a: &str, b: &str| json!([{"mention":a,"type":"person"},{"mention":b,"type":"project"}]);
        let dated = |quote: &str, ents: Value| {
            let mut a = atom(quote, ents, json!([]), json!([]));
            a["valid_until"] = json!("2026-12-31");
            a["validity_quote"] = json!("until 2026-12-31");
            a
        };
        let output = json!({"atoms":[
            dated("Ana Ruiz owns Atlas until 2026-12-31.", entities("Ana Ruiz", "Atlas")),
            dated("Bo Chen owns Hermes.", entities("Bo Chen", "Hermes")),
            dated("Cy Lo owns Iris.", entities("Cy Lo", "Iris")),
        ]});
        let (atoms, d) = validate(output, source, None, Some(day("2026-10-19")));
        assert!(atoms[0].validity.is_some(), "the atom that quotes its date keeps it");
        assert!(atoms[1].validity.is_none() && atoms[2].validity.is_none());
        assert_eq!((d.relaxed_dates, d.dropped_shared_dates), (2, 2), "{d:?}");
    }

    /// v3.4: for PRs, issues and checkpoints the source date is the last
    /// update. A date equal to it holds only when its words name the source's
    /// own moment; otherwise it drops alone (counted).
    /// v3.4 gate: 12 of 18 dropped source-day dates stated the day outright
    /// (a checkpoint line "2026-10-14 09:30 UTC", "(2026-10-14, …)"). A quote
    /// that writes the day out keeps it.
    #[test]
    fn a_source_day_date_whose_quote_states_the_day_is_kept() {
        let source = "2026-10-14 09:30 UTC: deploy.yml now runs on tags. Rollout finished (2026-10-14, after review). Merged on October 14, 2026. PR #9 merged.";
        let dated = |quote: &str, words: &str| {
            let mut a = atom(quote, json!([]), json!([]), json!([]));
            a["valid_from"] = json!("2026-10-14");
            a["validity_quote"] = json!(words);
            a
        };
        let output = json!({"atoms":[
            dated("2026-10-14 09:30 UTC: deploy.yml now runs on tags.", "2026-10-14 09:30 UTC"),
            dated("Rollout finished (2026-10-14, after review).", "(2026-10-14, after review)"),
            dated("Merged on October 14, 2026.", "on October 14, 2026"),
            dated("PR #9 merged.", "merged"),
        ]});
        let (atoms, d) = validate(output, source, None, Some(day("2026-10-14")));
        let kept: Vec<bool> = atoms.iter().map(|a| a.validity.is_some()).collect();
        assert_eq!(kept, [true, true, true, false], "only the undated 'merged' loses the source day");
        assert_eq!(d.dropped_source_dates, 1);
        let day14 = day("2026-10-14");
        for yes in ["2026-10-14 09:30 UTC", "on 2026/10/14", "14 Oct 2026", "Oct 14th, 2026", "October 14, 2026", "Tue, 14 October 2026"] {
            assert!(states_day(yes, day14), "{yes}");
        }
        for no in ["2026-10-15", "October 2026", "Oct 14", "14/10", "on the 14th", "merged"] {
            assert!(!states_day(no, day14), "{no}");
        }
    }

    #[test]
    fn a_date_equal_to_the_source_date_needs_its_own_moment_words() {
        let source = "PR #786 merged after review. As of today, billing runs on v2.3. Atlas launched on 2026-10-01.";
        let dated = |quote: &str, when: &str, words: &str| {
            let mut a = atom(quote, json!([]), json!([]), json!([]));
            a["valid_from"] = json!(when);
            a["validity_quote"] = json!(words);
            a
        };
        let output = json!({"atoms":[
            dated("PR #786 merged after review.", "2026-10-19", "merged"),
            dated("As of today, billing runs on v2.3.", "2026-10-19", "As of today"),
            dated("Atlas launched on 2026-10-01.", "2026-10-01", "on 2026-10-01"),
        ]});
        let (atoms, d) = validate(output, source, None, Some(day("2026-10-19")));
        assert!(atoms[0].validity.is_none(), "a merge dated to the last update");
        assert!(atoms[1].validity.is_some() && atoms[2].validity.is_some());
        assert_eq!(d.dropped_source_dates, 1);
        for yes in ["as of today", "is now", "This PR switches", "now", "effective immediately"] {
            assert!(own_moment(yes), "{yes}");
        }
        for no in ["merged", "known", "nowhere", "on 2026-10-19", "State: In Progress"] {
            assert!(!own_moment(no), "{no}");
        }
    }

    fn atom_in(source: &str) -> Value {
        let _ = source;
        atom("Ana Ruiz owns Atlas.", json!([{"mention":"Atlas","type":"project"}]), json!([]), json!([]))
    }

    #[test]
    fn paragraph_is_bounded_by_blank_lines_and_reach() {
        let long = "x".repeat(400);
        let source = format!("Head.\n\n{long} Ana Ruiz owns Atlas. {long}\n\nTail.");
        let w = super::super::window(&source, 0).unwrap();
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let atoms = validate_structured(&json!({"atoms":[atom("Ana Ruiz owns Atlas.", json!([]), json!([]), json!([]))]}).to_string(),
            &w, None, Contract { changes: true, source_date: None, ..Default::default() }, &mut d).unwrap();
        let p = paragraph(&w, &atoms[0]);
        assert!(!p.text.contains("Head.") && !p.text.contains("Tail."), "blank lines bound it");
        let reach = p.text.chars().count() - "Ana Ruiz owns Atlas.".chars().count();
        assert!(reach <= 2 * PARAGRAPH_REACH_CHARS, "±300 chars bound it: {reach}");
        let near = "Head.\nAna Ruiz owns Atlas.\nTail.\n\nOther.";
        let w = super::super::window(near, 0).unwrap();
        let atoms = validate_structured(&json!({"atoms":[atom("Ana Ruiz owns Atlas.", json!([]), json!([]), json!([]))]}).to_string(),
            &w, None, Contract { changes: true, source_date: None, ..Default::default() }, &mut crate::llm::ExtractionDiagnostics::default()).unwrap();
        assert_eq!(paragraph(&w, &atoms[0]).text, "Head.\nAna Ruiz owns Atlas.\nTail.");
    }

    /// v3.3: code is test data and examples. Kinship relations and family
    /// changes from a code source drop alone (counted); other relations and
    /// the atom stay. The same answer from prose keeps them.
    /// v3.5 (v3.4.9 gate): found-by drops only item -> finder, a source
    /// item's named part is not ownership of the item, prose examples are no
    /// facts, and offers count anywhere in the sentence.
    #[test]
    fn v35_found_by_parts_examples_and_offers() {
        // 1. The finder references its PR; the item does not mention its finder.
        assert!(!mention_not_reference("mentions", "bench", "PR #1256", "the regression was found by the bench (PR #1256)", false));
        assert!(mention_not_reference("mentions", "COR-92", "COR-91", "COR-92 was found by COR-91", false));
        // 2. Owning a named part is not owning the item.
        let body = "Morgan owns pagination validation; the rest of the issue is open.";
        assert!(owns_only_other(body, "Morgan", "DEMO-1"));
        assert!(!owns_only_other("Morgan owns DEMO-1.", "Morgan", "DEMO-1"));
        assert!(!owns_only_other("Morgan reviewed the plan.", "Morgan", "DEMO-1"), "no ownership stated");
        // 3. Prose examples and quoted fixtures.
        for text in [
            "e.g. Dana works on the graphs table",
            "For example, Dana works_on graphs table was written.",
            "Red on main: Dana works on the graphs table.",
            "The fixture should no longer yield Dana works on the graphs table.",
            "It previously gave Dana works on the graphs table.",
            "the test seeds \"Dana works on the graphs table\" and checks it",
            "the PR body quotes `Dana works on the graphs table` as the bug",
        ] {
            assert!(example_or_hypothetical(text, "Dana", "graphs table"), "{text}");
        }
        assert!(!example_or_hypothetical("Dana works on the graphs table migration.", "Dana", "graphs table"));
        assert!(!example_or_hypothetical("Dana renamed `graphs_table`.", "Dana", "graphs_table"), "one backticked identifier is not a fixture");
        // 4. Offer words anywhere in the sentence.
        let person = |e: &str| (e == "Alex").then_some(super::super::EntityType::Person);
        assert!(relation_unsupported("works_on", "Alex", "drift check", "Offered: Alex is working on the drift check tomorrow if he wants.", false, false, person));
        assert!(relation_unsupported("works_on", "Alex", "drift check", "Alex, working on alerts, was offered the drift check.", false, false, person));
    }

    /// v3.4.9 (v3.4.8 gate): worktree branches by context, "found by" is no
    /// reference, offers are not work, and ownership of a part of an item.
    #[test]
    fn v349_branches_found_by_offers_and_owned_parts() {
        // Unprefixed worktree branches, only with branch context.
        assert!(is_branch("pushed to extraction-v349", "extraction-v349"));
        assert!(is_branch("worktree extraction-v349 holds it", "extraction-v349"));
        assert!(is_branch("on profile-v3 the fix lands", "profile-v3"));
        assert!(is_branch("the extraction-v349 branch", "extraction-v349"));
        assert!(!is_branch("a self-serve flow", "self-serve"), "a kebab word is not a branch");
        assert!(!is_branch("branch out to new markets", "new markets"));
        assert!(mention_not_reference("mentions", "PR #900", "extraction-v349", "PR #900 adds jitter. Pushed to extraction-v349.", false));
        // "found by" is how an item was discovered.
        for (p, s, o, text) in [
            ("mentioned_by", "COR-92", "COR-91 plan", "COR-92 was found by the COR-91 plan"),
            ("mentioned_by", "COR-92", "COR-91", "COR-92 was found by COR-91"),
            ("mentioned_by", "COR-70", "PR #900", "COR-70 found by PR #900"),
        ] {
            assert!(mention_not_reference(p, s, o, text, false), "{text}");
        }
        assert!(!mention_not_reference("mentions", "the Layer 3 gate", "PR #290", "the Layer 3 gate (PR #290) fails closed", false), "a plain parenthetical stays");
        // Offers and reviews are not work.
        let person = |e: &str| (e == "Alex").then_some(super::super::EntityType::Person);
        for text in [
            "The drift check was offered to Alex.",
            "Suggested the drift check to Alex.",
            "The drift check is for Alex to review.",
            "Alex is working through review of the drift check.",
        ] {
            assert!(relation_unsupported("works_on", "Alex", "drift check", text, false, false, person), "{text}");
        }
        assert!(!relation_unsupported("works_on", "Alex", "drift check", "Alex is working on the drift check.", false, false, person));
        // Ownership of a part of an item.
        assert_eq!(owned_part("Morgan owns the pagination validation part of DEMO-1.", "Morgan", "DEMO-1").as_deref(), Some("pagination validation"));
        assert_eq!(owned_part("Morgan is responsible for retries in DEMO-1", "Morgan", "DEMO-1").as_deref(), Some("retries"));
        assert_eq!(owned_part("Morgan owns DEMO-1.", "Morgan", "DEMO-1"), None);
        assert_eq!(owned_part("Morgan reviewed DEMO-1.", "Morgan", "DEMO-1"), None);
    }

    /// v3.4.8 (v3.4.7 gate): key-token mention matching, no branch worker,
    /// the authored item itself, the source item's plain naming, and owners
    /// in the stated direction.
    #[test]
    fn v348_mentions_authored_items_and_owner_direction() {
        // 1. Markup and key tokens.
        let text = "migration 0279 adds five columns to `gmail_everything_mode`";
        assert!(!mention_not_reference("mentions", "migration 0279", "`gmail_everything_mode`", text, false));
        assert!(!mention_not_reference("mentions", "0279_gmail_everything_mode.sql", "gmail_everything_mode", text, false), "found by its key token");
        assert_eq!(locate(&strip_quotes(text), "migration 0279 (tenant)"), "0279");
        // 2. A branch never works on anything.
        let any = |_: &str| None;
        assert!(relation_unsupported("works_on", "main", "PR #900", "main <- feat/x builds PR #900", false, false, any));
        assert!(relation_unsupported("works_on", "main <- feat/x", "PR #900", "main <- feat/x working on PR #900", false, false, any));
        assert!(relation_unsupported("worked_on_by", "PR #900", "feat/retry", "PR #900 is worked on by feat/retry", false, false, any));
        // 3. The author works on the authored item, not on what it discusses.
        let issue = "Issue opened by Dana: Add an index to the graphs table";
        assert_eq!(agency_toward(issue, "Dana", "graphs table"), None);
        assert_eq!(agency_toward(issue, "Dana", "Add an index to the graphs table"), Some(Agency::Created));
        let person = |e: &str| (e == "Dana").then_some(super::super::EntityType::Person);
        assert!(relation_unsupported("works_on", "Dana", "graphs table", issue, false, false, person));
        assert!(!relation_unsupported("works_on", "Dana", "Add an index to the graphs table", issue, false, false, person));
        // 4. The source item mentions what it plainly names (founder default).
        for (o, text) in [
            ("release v638", "Release v638 rolls the extraction worker."),
            ("trusted_recapture observation kind", "the `trusted_recapture` observation kind"),
            ("P8 gap (b)", "Closes the last of P8 gap (b)."),
        ] {
            assert!(!mention_not_reference("mentions", "PR #640", o, text, true), "{text}");
        }
        for (s, o, text) in [
            ("Ballot decision", "Alex", "Ballot & Propositions escalate-rule decision still pending Alex"),
            ("standing extraction", "extraction v2", "standing extraction is built on extraction v2"),
            ("binding", "Jev", "the Jev-adjudicated binding"),
            ("embedding calls", "X", "embedding calls are metered under X"),
        ] {
            assert!(mention_not_reference("mentions", s, o, text, false), "not the source item: {text}");
        }
        assert!(mention_not_reference("mentions", "PR #640", "main", "PR #640 targets main", true), "a branch stays dropped");
        // 5. Owners in the stated direction.
        let morgan = "Morgan owns pagination validation.";
        assert_eq!(reoriented_owner("responsible_for", "pagination validation", "Morgan", morgan), Some(("Morgan", "pagination validation")));
        assert_eq!(reoriented_owner("responsible_for", "Morgan", "pagination validation", morgan), None);
        assert_eq!(reoriented_owner("responsibility_of", "Morgan", "pagination validation", morgan), Some(("pagination validation", "Morgan")));
        assert_eq!(reoriented_owner("responsible_for", "pagination validation", "Morgan", "Pagination validation is owned by Morgan"), Some(("Morgan", "pagination validation")));
        assert_eq!(reoriented_owner("responsible_for", "Atlas", "Morgan", "Atlas and Morgan"), None, "no stated direction: unchanged");
    }

    /// v3.4.5 (v3.4.4 gate): a PR's author works on it whatever its form,
    /// an author's mention of their own item is the `works_on` it means, and
    /// a commit list, a branch, a name inside a path, or a fix/find verb to a
    /// non-item is not a mention.
    #[test]
    fn authors_work_on_their_items_and_mentions_are_references() {
        // Title-only PRs and untyped handles: the opener still works on it.
        let untyped = |_: &str| Some(super::super::EntityType::Topic);
        for (s, o, text) in [
            ("dana", "Fix login redirect", "Pull request opened by dana: Fix login redirect"),
            ("Dana Smith", "PR #876", "Dana Smith's own PR #876 bumps the budget."),
            ("Dana Smith", "Retry budget", "Dana Smith authored Retry budget"),
        ] {
            assert!(!authorship_only("works_on", s, o, text), "{text}");
            assert!(!relation_unsupported("works_on", s, o, text, false, false, untyped), "{text}");
        }
        assert_eq!(agency("Dana Smith's own PR #876", "Dana Smith"), Some(Agency::Created));
        // An author's mention of their own item becomes works_on.
        assert_eq!(author_mention("mentioned_by", "Fix login redirect", "dana", "Pull request opened by dana: Fix login redirect"), Some(("dana", "Fix login redirect")));
        assert_eq!(author_mention("mentions", "Dana Smith", "PR #876", "Dana Smith opened PR #876"), Some(("Dana Smith", "PR #876")));
        assert_eq!(author_mention("mentions", "Dana Smith", "#819", "Dana Smith closed #819"), None, "acting is not authoring");
        assert_eq!(author_mention("works_on", "Dana Smith", "PR #876", "Dana Smith opened PR #876"), None);
        // Not references.
        let commits = "Commits\n- 3e6bc96a fix(extraction): authorship is not involvement\n- 4677d9dd retry budget for COR-65";
        assert!(mention_not_reference("mentions", "authorship", "COR-65", commits, false), "a commit list");
        assert!(mention_not_reference("mentions", "PR #876", "feat/extraction-v3.4.5", "PR #876 merges feat/extraction-v3.4.5", false), "a branch");
        assert!(mention_not_reference("mentions", "PR #876", "main", "PR #876 targets main", false), "a branch");
        assert!(mention_not_reference("mentions", "ocean_health.rs", "backend/src/ocean_health.rs", "Edits backend/src/ocean_health.rs", false), "a name inside its path");
        assert!(mention_not_reference("mentions", "PR #880", "drift detector", "PR #880 fixes drift detector noise", false), "a fix to a non-item");
        assert!(mention_not_reference("mentioned_by", "deploy drift", "plan X", "deploy drift found by plan X", false), "a finding");
        // v3.4.6 (v3.4.5 gate): no reference words, not both items: dropped.
        for (s, o, text) in [
            ("Opus", "CoreWeave", "Opus is served on CoreWeave"),
            ("head", "#298", "head = #298+#299"),
            ("ERR_42", "ERR_43", "codes ERR_42, ERR_43 and ERR_44"),
            ("div", "span", "a <div> wrapping a <span>"),
            ("backend616", "web470", "deployed backend616/web470"),
            ("Atlas", "Q4", "Project: Atlas | Target: Q4 | Lead: Dana"),
            ("docs/design/x.html", "Atlas", "docs/design/x.html describes Atlas"),
            // v3.4.7 negatives the widened rule must not re-admit.
            ("Ballot decision", "Alex", "Ballot & Propositions escalate-rule decision still pending Alex"),
            ("standing extraction", "extraction v2", "standing extraction is built on extraction v2"),
            ("binding", "Jev", "the Jev-adjudicated binding"),
            ("embedding calls", "X", "embedding calls are metered under X"),
            ("Atlas", "Hermes", "Atlas calls Hermes"),
        ] {
            assert!(mention_not_reference("mentions", s, o, text, false), "{text}");
        }
        // v3.4.7 recoveries.
        for (s, o, text) in [
            ("#287", "the escalate rule", "- #287 (3e6bc96a): sets the escalate rule for ballots"),
            ("PR #301", "the retry budget", "PR #301 adds the retry budget"),
            ("ocean_health.rs", "retry.rs", "ocean_health.rs calls retry.rs"),
            ("`render_digest`", "`DigestCard`", "`render_digest` returns `DigestCard`"),
            ("fetchDigest", "useOcean", "fetchDigest uses useOcean"),
            ("the Layer 3 gate", "PR #290", "the Layer 3 gate (PR #290) fails closed"),
            ("the runbook", "Layer 3", "The runbook refers to the Layer 3 gate. Later: the runbook and Layer 3 fail closed."),
        ] {
            assert!(!mention_not_reference("mentions", s, o, text, false), "{text}");
        }
        // Possessive and creation agency name only their own item.
        let Alex = "Alex's own PR #287 lands the fix.\nBallot & Propositions escalate-rule decision still pending Alex";
        assert_eq!(agency_toward(Alex, "Alex", "PR #287"), Some(Agency::Created));
        assert_eq!(agency_toward(Alex, "Alex", "#287"), Some(Agency::Created));
        assert_eq!(agency_toward(Alex, "Alex", "Ballot decision"), None, "the possessive names PR #287, not the decision");
        assert!(!authorship_only("works_on", "Alex", "PR #287", Alex));
        let topic = |_: &str| Some(super::super::EntityType::Topic);
        let person = |e: &str| (e == "Alex").then_some(super::super::EntityType::Person).or(Some(super::super::EntityType::Topic));
        assert!(relation_unsupported("works_on", "Alex", "Ballot decision", Alex, false, false, person), "no work words, not his item");
        assert!(!relation_unsupported("works_on", "Alex", "PR #287", Alex, false, false, topic));
        assert_eq!(agency_toward("Dana Smith opened #812 and reviewed #813", "Dana Smith", "#813"), None, "the verb names #812 only");
        for (p, s, o, text) in [
            ("mentions", "PR #876", "COR-65", "PR #876 fixes COR-65"),
            ("mentions", "PR #876", "COR-65", "PR #876 and COR-65"),
            ("mentions", "README", "deploy.yml", "README points at deploy.yml"),
            ("mentions", "the runbook", "Neon PITR", "the runbook refers to Neon PITR"),
            ("mentions", "the design doc", "#812", "the design doc closes #812"),
            ("mentions", "retry_once", "backoff.rs", "// retry_once mentions backoff.rs"),
        ] {
            assert!(!mention_not_reference(p, s, o, text, false), "{text}");
        }
        // Slack and Markdown link markup reads as its label.
        assert_eq!(unlink("Pull request opened by &lt;https://github.com/example-founder|example-founder&gt;"), "Pull request opened by example-founder");
        assert_eq!(unlink("Pull request opened by <https://github.com/example-founder|example-founder>: <https://x/pull/9|#9 Fix>"), "Pull request opened by example-founder: #9 Fix");
        assert_eq!(unlink("see [the runbook](https://x.dev/r) and <https://x.dev>"), "see the runbook and https://x.dev");
        assert_eq!(unlink("a < b and [x] stays"), "a < b and [x] stays");
        assert_eq!(agency(&unlink("Pull request opened by &lt;https://github.com/example-founder|example-founder&gt;"), "example-founder"), Some(Agency::Created));
        // Kinship to a commit or branch is never a family.
        let person = |_: &str| Some(super::super::EntityType::Person);
        assert!(relation_unsupported("parent_of", "example-founder", "main", "example-founder pushed a commit to main", false, false, person));
        assert!(relation_unsupported("parent_of", "example-founder", "3e6bc96a", "example-founder pushed 3e6bc96a", false, false, person));
        assert!(!relation_unsupported("parent_of", "Ana Ruiz", "Bo Chen", "Ana Ruiz is the parent of Bo Chen", false, false, person));
        assert!(authorship_only("works_on", "example-founder", "main", "example-founder pushed a commit to main"), "pushing is not authoring");
        assert!(branch_like("refs/heads/main") && branch_like("fix/login") && branch_like("claude/v3-gate"));
        assert!(!branch_like("docs/design/x.html") && !branch_like("fix/login.rs") && !branch_like("mainline"));
    }

    /// v3.4.4: a relation the words linking its endpoints do not state is
    /// dropped (the v3.4.3 gate's predicate misuse); stated ones stay.
    #[test]
    fn relations_need_the_words_their_predicate_means() {
        use super::super::EntityType::{Org, Person, Project, Topic};
        let types = |e: &str| match e {
            "Dana Smith" | "Ana Ruiz" => Some(Person),
            "Initech" | "Acme" | "OpenAI" => Some(Org),
            "ChatGPT" | "Opus" => Some(Topic),
            _ => Some(Project),
        };
        let unsupported = |p: &str, s: &str, o: &str, text: &str, code: bool| relation_unsupported(p, s, o, text, code, false, types);
        // The gate's misuses: dropped.
        for (p, s, o, text) in [
            ("responsibility_of", "Atlas v2", "Atlas", "Atlas v2 is a copy of Atlas."),
            ("mentions", "Deploy key", "Vault", "The deploy key secret is stored in Vault."),
            ("works_at", "Acme", "CoralSwarm", "Acme has a CoralSwarm workspace."),
            ("works_at", "Dana Smith", "Initech", "Dana Smith visited Initech."),
            ("responsible_for", "Atlas", "Dana Smith", "Atlas is owned by Dana Smith."),
            ("works_on", "Dana Smith", "#812", "Dana Smith pushed a commit to #812."),
            ("mentions", "COR-65", "Hermes", "COR-65 uses Hermes, for example."),
            // A model doing the work is not a person working on it.
            ("works_on", "Opus", "COR-7", "COR-7 started on Opus, which is working on it."),
            ("worked_on_by", "COR-7", "Opus", "COR-7 is being worked on by Opus."),
        ] {
            assert!(unsupported(p, s, o, text, false), "{p}: {text}");
        }
        assert!(unsupported("mentions", "retry.rs", "ChatGPT", "// Adapted from an answer by ChatGPT in retry.rs", true));
        // Stated: kept.
        for (p, s, o, text) in [
            ("works_at", "Dana Smith", "Initech", "Dana Smith works at Initech."),
            ("works_at", "Ana Ruiz", "Acme", "Ana Ruiz, the CTO of Acme, spoke."),
            ("responsible_for", "Dana Smith", "Atlas", "Dana Smith owns Atlas."),
            ("responsibility_of", "Atlas", "Dana Smith", "Atlas is owned by Dana Smith."),
            ("works_on", "Dana Smith", "#812", "Dana Smith is working on #812."),
            ("works_on", "Dana Smith", "#812", "Dana Smith opened #812."),
            ("worked_on_by", "#812", "Dana Smith", "Pull request opened by Dana Smith: #812"),
            ("worked_on_by", "#812", "Dana Smith", "#812 is being built by Dana Smith."),
            ("mentions", "PR #876", "COR-65", "PR #876 fixes COR-65."),
        ] {
            assert!(!unsupported(p, s, o, text, false), "{p}: {text}");
        }
        assert!(!unsupported("mentions", "retry_once", "backoff.rs", "// retry_once mentions backoff.rs", true), "code naming code");
        assert!(!relation_unsupported("works_on", "Dana Smith", "PR #876", "Bumps the retry budget.", false, true, types), "the source's own author");
        // Linking words: only the clause between the endpoints counts.
        assert_eq!(linking_clauses("Dana Smith opened #812: Build the work queue", "Dana Smith", "#812"), vec![" opened ".to_string()]);
        assert_eq!(linking_clauses("Bo works on Atlas. Bo opened it; Atlas", "Bo", "Atlas"), vec![" works on ".to_string()]);
    }

    /// v3.4.3: "opened by Dana" names an author, not someone who works on or
    /// owns the PR. A stated involvement in the same shape stays.
    #[test]
    fn authorship_lines_state_no_involvement() {
        let source = "Pull request opened by Dana Smith: #812 Fix login redirect\nBo Chen is working on #813 Cache warmup; Bo Chen opened it.";
        let entities = json!([{"mention":"Dana Smith","type":"person"},{"mention":"#812 Fix login redirect","type":"project"},
            {"mention":"Bo Chen","type":"person"},{"mention":"#813 Cache warmup","type":"project"}]);
        let rel = |s: &str, p: &str, o: &str| json!({"subject":s,"predicate":p,"object":o,"valid_from":null,"valid_until":null,"validity_quote":null});
        let first = "Pull request opened by Dana Smith: #812 Fix login redirect";
        let second = "Bo Chen is working on #813 Cache warmup; Bo Chen opened it.";
        let mut a = atom(first, json!([entities[0], entities[1]]), json!([
            rel("Dana Smith", "works_on", "#812 Fix login redirect"),
            rel("#812 Fix login redirect", "responsibility_of", "Dana Smith")]), json!([]));
        a["evidence_quote"] = json!(first);
        let mut b = atom(second, json!([entities[2], entities[3]]), json!([rel("Bo Chen", "works_on", "#813 Cache warmup")]), json!([]));
        b["evidence_quote"] = json!(second);
        let output = json!({"atoms":[a, b]});
        let run = |changes: bool| {
            let mut d = crate::llm::ExtractionDiagnostics::default();
            let atoms = validate_structured(&output.to_string(), &window(source), None,
                Contract { changes, ..Default::default() }, &mut d).unwrap();
            (atoms.iter().map(|a| a.relations.len()).collect::<Vec<_>>(), d.relations.unwrap())
        };
        let (kept, d) = run(true);
        assert_eq!(kept, [1, 1], "the author works on their PR; authoring is not owning; the stated one stays: {d:?}");
        assert_eq!(d.rejected_relations.get(&RelationRejection::Authorship), Some(&1));
        let (kept, _) = run(false);
        assert_eq!(kept, [2, 1], "v2 is unchanged");
        // Founder decision (2026-10-21): the author of a PR or issue works on
        // it; acting on someone else's item does not.
        for (evidence, subject, object) in [
            ("Dana Smith opened #812", "Dana Smith", "#812"),
            ("Pull request opened by Dana Smith: #812", "Dana Smith", "#812"),
            ("Issue COR-9 filed by Ana Ruiz", "Ana Ruiz", "COR-9"),
            ("Ana Ruiz created COR-9", "Ana Ruiz", "COR-9"),
            ("Dana Smith opened #812: Build the work queue", "Dana Smith", "#812"),
            ("Dana Smith opened #812 and closed #813", "Dana Smith", "#812"),
        ] {
            assert!(!authorship_only("works_on", subject, object, evidence), "{evidence}");
            assert!(authorship_only("responsible_for", subject, object, evidence), "authoring is not owning: {evidence}");
        }
        assert!(!authorship_only("worked_on_by", "#812", "Dana Smith", "Pull request opened by Dana Smith: #812"));
        for (evidence, subject, object) in [
            ("Dana Smith merged #812.", "Dana Smith", "#812"),
            ("#812 was closed by Dana Smith", "Dana Smith", "#812"),
            ("Dana Smith pushed a commit to #812", "Dana Smith", "#812"),
            ("Dana Smith reviewed #812", "Dana Smith", "#812"),
            ("Dana Smith approved #812", "Dana Smith", "#812"),
            ("Dana Smith commented on #812", "Dana Smith", "#812"),
        ] {
            assert!(authorship_only("works_on", subject, object, evidence), "{evidence}");
        }
        // Mentions: any opener or actor, whatever its type.
        assert!(authorship_only("mentioned_by", "PR #876", "Dana Smith", "Dana Smith opened PR #876"));
        assert!(authorship_only("mentions", "Dana Smith", "#819", "Dana Smith closed #819"));
        assert!(authorship_only("mentions", "deploy-agent", "#819", "deploy-agent closed #819"), "an untyped agent");
        assert!(!authorship_only("mentions", "PR #876", "COR-65", "PR #876 fixes COR-65"));
        assert!(!authorship_only("mentions", "COR-65", "PR #876", "COR-65 filed by Dana Smith mentions PR #876"), "a passive verb names the item, not the agent");
        // Bots never work on or mention anything.
        for bot in ["dependabot[bot]", "github-actions", "coderabbitai", "renovate-bot"] {
            assert!(authorship_only("works_on", bot, "#812", &format!("{bot} opened #812")), "{bot}");
            assert!(authorship_only("mentions", bot, "#812", &format!("{bot} reviewed #812")), "{bot}");
            assert!(bot_like(bot), "{bot}");
        }
        assert!(!bot_like("Dana Smith") && !bot_like("Abbot") && !bot_like("robot"));
        assert!(!authorship_drop("works_on", "Dana Smith", "PR #876", "Bumps the retry budget", true), "the source's own author");
        assert!(item_like("PR #876 Retry budget") && item_like("COR-65") && item_like("#812") && !item_like("Opus") && !item_like("Atlas"));
        // A PR header line is a branch layout, not a mention.
        assert!(header_only("mentions", "main", "feat/extraction-v3.4.3", "main <- feat/extraction-v3.4.3"));
        assert!(header_only("mentions", "PR #876", "feat/x", "PR #876\nbase: main\nhead: feat/x"));
        assert!(!header_only("mentions", "PR #876", "COR-65", "main <- feat/x\nPR #876 closes COR-65"));
        assert!(!header_only("works_on", "main", "feat/x", "main <- feat/x"));
        assert!(!header_only("mentions", "README", "deploy.yml", "README points at deploy.yml <- see there for details"));
        for (predicate, evidence, subject, object) in [
            ("responsible_for", "#812 is owned by Dana Smith", "Dana Smith", "#812"),
            ("works_on", "Dana Smith is fixing #812", "Dana Smith", "#812"),
            ("member_of", "Dana Smith opened the Infra channel", "Dana Smith", "Infra"),
        ] {
            assert!(!authorship_only(predicate, subject, object, evidence), "{evidence}");
        }
    }

    #[test]
    fn code_sources_name_files_and_symbols_never_owners() {
        let source = "// Owner: Dana Smith. Dana Smith manages Bo Chen.\n// retry_once mentions backoff.rs; the timeout is now 60s.";
        let entities = json!([{"mention":"Dana Smith","type":"person"},{"mention":"Bo Chen","type":"person"},
            {"mention":"retry_once","type":"topic"},{"mention":"backoff.rs","type":"project"}]);
        let relations = json!([
            {"subject":"Dana Smith","predicate":"manages","object":"Bo Chen","valid_from":null,"valid_until":null,"validity_quote":null},
            {"subject":"retry_once","predicate":"mentions","object":"Dana Smith","valid_from":null,"valid_until":null,"validity_quote":null},
            {"subject":"retry_once","predicate":"mentions","object":"backoff.rs","valid_from":null,"valid_until":null,"validity_quote":null}]);
        let changes = json!([
            change("retry_once", "owner", None, "Dana Smith", None, None, "Owner: Dana Smith"),
            change("retry_once", "timeout", None, "60s", None, None, "the timeout is now 60s")]);
        let output = json!({"atoms":[atom(source, entities, relations, changes)]});
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let atoms = validate_structured(&output.to_string(), &window(source), None,
            Contract { changes: true, code: true, ..Default::default() }, &mut d).unwrap();
        let kept: Vec<(&str, &str, &str)> = atoms[0].relations.iter().map(|r| (r.subject.as_str(), r.predicate, r.object.as_str())).collect();
        assert_eq!(kept, [("retry_once", "mentions", "backoff.rs")], "only code-to-code relations stay");
        let attributes: Vec<String> = atoms[0].changes.iter().map(|c| match &c.attribute {
            ChangeAttribute::Text { key, .. } => key.clone(),
            ChangeAttribute::Relation(r) => r.name.to_string(),
        }).collect();
        assert_eq!(attributes, ["attr.timeout"], "ownership is not a code change");
        let d = d.relations.unwrap();
        assert_eq!((d.rejected_relations.get(&RelationRejection::Fixture), d.rejected_changes.get(&ChangeRejection::Fixture), d.dropped_code_people), (Some(&2), Some(&1), 2));
    }

    #[test]
    fn test_paths_are_skipped_and_rust_test_modules_cut() {
        for path in ["backend/tests/source_atoms_pg.rs", "web/__tests__/a.tsx", "spec/models/user_spec.rb", "src/fixtures/a.json",
            "pkg/testdata/in.txt", "test_utils.py", "src/foo_test.go", "web/app/page.test.tsx", "web/a.spec.ts", "Test/Thing.cs"] {
            assert!(is_test_path(path), "{path}");
        }
        for path in ["backend/src/workers/source_atoms.rs", "src/testing.rs", "src/latest.rs", "docs/contest.md", "src/attest_test_helper/x.rs",
            "src/protest.rs", "web/app/spec_sheet.tsx", "README.md"] {
            assert!(!is_test_path(path), "{path}");
        }
        assert_eq!(code_path(&json!({"title":"t.rs"}), "9:blob:src/x_test.rs").as_deref(), Some("src/x_test.rs"));
        assert_eq!(test_file_statement("src/foo_test.go"), "src/foo_test.go: test file for foo.");
        assert_eq!(test_file_statement("tests/test_utils.py"), "tests/test_utils.py: test file for utils.");
        assert_eq!(test_file_statement("web/page.test.tsx"), "web/page.test.tsx: test file for page.");
        assert_eq!(test_file_statement("src/fixtures/users.json"), "src/fixtures/users.json: test data (users), not product code.");
        assert_eq!(code_path(&json!({"code_path":"src/a.rs","title":"b"}), "9:blob:src/c.rs").as_deref(), Some("src/a.rs"));
        let send = |text: &str| match code_window("github_code", &json!({"code_path":"src/a.rs"}), "", text, text, 0) {
            CodeWindow::Send(t) => Some(t.into_owned()),
            CodeWindow::Skip(_) => None,
        };
        assert_eq!(code_window("github_code", &json!({"code_path":"backend/tests/a.rs"}), "", "fn a() {}", "fn a() {}", 0),
            CodeWindow::Skip(CodeSkip::TestPath));
        assert_eq!(code_window("github_pull_request", &json!({"code_path":"backend/tests/a.rs"}), "", "x", "x", 0),
            CodeWindow::Send(std::borrow::Cow::Borrowed("x")));
        let file = "pub fn retry() {}\n\n#[cfg(test)]\n#[allow(dead_code)]\nmod checks {\n    const OPEN: char = '{';\n    // a } in a comment\n    fn s() -> &'static str { \"}}\" }\n    fn r() -> &'static str { r#\"{\"# }\n}\n\npub fn after() {}\nmod tests {\n    #[test]\n    fn t() { assert!(true) }\n}\n";
        assert_eq!(send(file).unwrap(), "pub fn retry() {}\n\n\npub fn after() {}\n");
        assert_eq!(rust_test_modules(file).len(), 2);
        // `mod name;`, a cfg(test) function and a non-test module stay.
        let kept = "#[cfg(test)]\nmod helpers;\n#[cfg(test)]\nfn only_in_tests() {}\nmod testing { fn a() {} }\n";
        assert_eq!(send(kept).unwrap(), kept);
        // A window wholly inside a test module sends nothing; a window
        // starting inside one sends what follows the module.
        let start = file.find("    #[test]").unwrap();
        let tail = &file[start..];
        let end = tail.find("assert").unwrap();
        assert_eq!(code_window("github_code", &json!({"code_path":"src/a.rs"}), "", file, &tail[..end], file[..start].chars().count()),
            CodeWindow::Skip(CodeSkip::TestModule));
        let start = file.find("    const OPEN").unwrap();
        let rest = &file[start..];
        assert_eq!(code_window("github_code", &json!({"code_path":"src/a.rs"}), "", file, rest, file[..start].chars().count()),
            CodeWindow::Send(std::borrow::Cow::Owned("\npub fn after() {}\n".to_string())));
        // An unclosed module runs to the end of the text.
        assert_eq!(send("fn a() {}\n#[cfg(test)]\nmod tests {\n    fn b() {").unwrap(), "fn a() {}\n");
    }

    #[test]
    fn code_sources_drop_fixture_family_relations_and_changes() {
        let source = "// Ana Ruiz is the parent of Bo Chen; Bo Chen works at Initech.";
        let entities = json!([{"mention":"Ana Ruiz","type":"person"},{"mention":"Bo Chen","type":"person"},{"mention":"Initech","type":"org"}]);
        let relations = json!([
            {"subject":"Ana Ruiz","predicate":"parent_of","object":"Bo Chen","valid_from":null,"valid_until":null,"validity_quote":null},
            {"subject":"Bo Chen","predicate":"works_at","object":"Initech","valid_from":null,"valid_until":null,"validity_quote":null}]);
        let changes = json!([change("Bo Chen", "parent", None, "Ana Ruiz", None, None, "Ana Ruiz is the parent of Bo Chen")]);
        let output = json!({"atoms":[atom(source, entities, relations, changes)]});
        let run = |code: bool| {
            let mut d = crate::llm::ExtractionDiagnostics::default();
            let atoms = validate_structured(&output.to_string(), &window(source), None,
                Contract { changes: true, source_date: None, code, ..Default::default() }, &mut d).unwrap();
            (atoms, d.relations.unwrap())
        };
        let (atoms, d) = run(true);
        assert_eq!(atoms.len(), 1, "the atom stays");
        assert!(atoms[0].relations.is_empty(), "code states no family and no employment");
        assert!(atoms[0].changes.is_empty());
        let mentions: Vec<&str> = atoms[0].entities.iter().map(|e| e.mention.as_str()).collect();
        assert_eq!(mentions, ["Initech"], "people leave code atoms");
        assert_eq!((d.rejected_relations.get(&RelationRejection::Fixture), d.rejected_changes.get(&ChangeRejection::Fixture), d.dropped_code_people), (Some(&2), Some(&1), 2));
        let (atoms, d) = run(false);
        assert_eq!((atoms[0].relations.len(), atoms[0].changes.len(), d.rejected_relations.get(&RelationRejection::Fixture)), (2, 1, None));
        assert!(is_code_source("github_code") && is_code_source("github_code_summary") && !is_code_source("github_pull_request"));
        for family in ["parent", "Grandparent", "mother in law", "spouse_of", "siblings"] {
            assert!(is_family_attribute(&parse_attribute(family).unwrap()), "{family}");
        }
        for other in ["status", "deploy trigger", "parental leave policy owner"] {
            assert!(!is_family_attribute(&parse_attribute(other).unwrap()), "{other}");
        }
    }

    /// v3.4.2 gate errors: proposals stored as states, subjects widened to
    /// the product or "all", and old values the text never states.
    #[test]
    fn tentative_widened_and_unstated_old_changes() {
        let run = |source: &str, ents: Value, changes: Value, names: Vec<String>| {
            let mut d = crate::llm::ExtractionDiagnostics::default();
            let atoms = validate_structured(&json!({"atoms":[atom(source, ents, json!([]), changes)]}).to_string(), &window(source), None,
                Contract { changes: true, self_names: names, ..Default::default() }, &mut d).unwrap();
            (atoms, d.relations.unwrap())
        };
        // A recommendation or unmerged work is not a state.
        for source in ["We should move the status to closed.", "The draft PR switches the deploy trigger to tags.", "If we ship it, status becomes done."] {
            let (atoms, d) = run(source, json!([]), json!([change("status", "status", None, "closed", None, None, source)]), vec![]);
            assert!(atoms[0].changes.is_empty(), "{source}");
            assert_eq!(d.rejected_changes.get(&ChangeRejection::Tentative), Some(&1), "{source}");
        }
        // The PR's "not deployed" is not the product's status.
        let source = "The fix is not deployed yet.";
        let ents = json!([{"mention":"fix","type":"topic"}]);
        let (atoms, d) = run(source, ents.clone(), json!([change("CoralSwarm", "status", None, "not deployed", None, None, source)]), vec![]);
        assert!(atoms[0].changes.is_empty());
        assert!(d.rejected_changes.get(&ChangeRejection::Subject).or(d.rejected_changes.get(&ChangeRejection::SubjectScope)).is_some(), "{d:?}");
        // The same words with the source itself as subject (the PR's title) stand.
        let names = self_names(&json!({"title":"PR #786: fix the deploy","number":"786"}));
        let (atoms, d) = run(source, json!([{"mention":"PR #786","type":"project"}]),
            json!([change("PR #786", "status", None, "not deployed", None, None, source)]), names);
        assert_eq!((atoms[0].changes.len(), d.rejected_changes.get(&ChangeRejection::SubjectScope)), (1, None), "{d:?}");
        // An old value the words never state is removed; the change stays a current state.
        let source = "Atlas status is now done.";
        let (atoms, d) = run(source, json!([{"mention":"Atlas","type":"project"}]),
            json!([change("Atlas", "status", Some("in progress"), "done", None, None, source)]), vec![]);
        assert_eq!((atoms[0].changes[0].old_value.clone(), atoms[0].changes[0].new_value.as_str()), (None, "done"));
        assert_eq!(d.dropped_old_values, 1);
        for yes in ["we should switch", "I propose X", "plan to migrate", "This could break", "WIP: new host", "not yet merged", "on branch foo"] {
            assert!(tentative(yes), "{yes}");
        }
        for no in ["shoulder", "is now done", "merged to main", "drafted", "it couldn't connect"] {
            assert!(!tentative(no), "{no}");
        }
    }

    #[test]
    fn malformed_changes_drop_alone() {
        let source = "Atlas moved from Bo Chen to Ana Ruiz.";
        let entities = json!([{"mention":"Atlas","type":"project"},{"mention":"Bo Chen","type":"person"},{"mention":"Ana Ruiz","type":"person"}]);
        let q = source;
        let cases = [
            (json!({"subject":"Atlas"}), ChangeRejection::Schema),
            (change("Atlas", "owner", Some("Bo Chen"), "Ana Ruiz", None, None, "not in the evidence"), ChangeRejection::Quote),
            (change("Zed", "owner", Some("Bo Chen"), "Ana Ruiz", None, None, q), ChangeRejection::Subject),
            (change("Atlas", "", Some("Bo Chen"), "Ana Ruiz", None, None, q), ChangeRejection::Attribute),
            (change("Atlas", "status", None, "", None, None, q), ChangeRejection::Value),
            (change("Atlas", "status", None, "archived", None, None, q), ChangeRejection::Value),
            (change("Atlas", "owner", Some("Ana Ruiz"), "ana ruiz", None, None, q), ChangeRejection::Unchanged),
            (change("Atlas", "owner", Some("Bo Chen"), "Ana", None, None, q), ChangeRejection::Endpoint),
            (change("Atlas", "owner", Some("Atlas"), "Ana Ruiz", None, None, q), ChangeRejection::Endpoint),
        ];
        for (bad, reason) in cases {
            let good = change("Atlas", "owner", Some("Bo Chen"), "Ana Ruiz", None, None, q);
            let (atoms, d) = validate(json!({"atoms":[atom(source, entities.clone(), json!([]), json!([bad, good]))]}), source, None, None);
            assert_eq!(atoms.len(), 1, "{reason:?}: the atom stays");
            assert_eq!(atoms[0].changes.len(), 1, "{reason:?}: the good change stays");
            assert_eq!(d.rejected_changes.get(&reason), Some(&1), "{reason:?}: {d:?}");
        }
        let good = change("Atlas", "owner", Some("Bo Chen"), "Ana Ruiz", None, None, q);
        let (atoms, d) = validate(json!({"atoms":[atom(source, entities.clone(), json!([]), json!([good.clone(), good.clone(), good]))]}), source, None, None);
        assert_eq!(atoms[0].changes.len(), 1);
        assert_eq!((d.rejected_changes[&ChangeRejection::Duplicate], d.rejected_changes[&ChangeRejection::Limit]), (1, 1));
        // A bad effective date drops only the date.
        let dated = change("Atlas", "owner", Some("Bo Chen"), "Ana Ruiz", Some("2026-13-40"), Some("moved"), q);
        let (atoms, d) = validate(json!({"atoms":[atom(source, entities, json!([]), json!([dated]))]}), source, None, None);
        assert_eq!((atoms[0].changes.len(), atoms[0].changes[0].effective.is_none(), d.dropped_validity), (1, true, 1));
    }

    #[test]
    fn relative_dates_resolve_near_the_source_date_or_drop_alone() {
        let source = "Since March Ana Ruiz leads Atlas. Bo Chen joined Initech on 2019-05-02. Cy Lo starts next week.";
        let ents = |names: &[(&str, &str)]| json!(names.iter().map(|(m, t)| json!({"mention":m,"type":t})).collect::<Vec<_>>());
        let mut a = atom("Since March Ana Ruiz leads Atlas.", ents(&[("Ana Ruiz", "person"), ("Atlas", "project")]),
            json!([{"subject":"Ana Ruiz","predicate":"leads_team","object":"Atlas","valid_from":"2026-03-01","valid_until":null,"validity_quote":"Since March"}]), json!([]));
        a["valid_from"] = json!("2026-03-01");
        a["validity_quote"] = json!("Since March");
        let mut b = atom("Bo Chen joined Initech on 2019-05-02.", ents(&[("Bo Chen", "person"), ("Initech", "org")]), json!([]), json!([]));
        b["valid_from"] = json!("2019-05-02");
        b["validity_quote"] = json!("on 2019-05-02");
        // "next week" resolved three years out: not believable.
        let mut c = atom("Cy Lo starts next week.", ents(&[("Cy Lo", "person")]), json!([]), json!([]));
        c["valid_from"] = json!("2029-10-26");
        c["validity_quote"] = json!("next week");
        let output = json!({"atoms":[a, b, c]});
        let (atoms, d) = validate(output.clone(), source, None, Some(day("2026-10-19")));
        assert_eq!(atoms[0].validity.as_ref().unwrap().from.unwrap().date_naive(), day("2026-03-01"));
        assert_eq!(atoms[0].relations[0].validity.as_ref().unwrap().quote, "Since March");
        assert_eq!(atoms[1].validity.as_ref().unwrap().from.unwrap().date_naive(), day("2019-05-02"), "a stated year is kept at any distance");
        assert_eq!(atoms[2].validity, None);
        assert_eq!(d.dropped_relative_dates, 1);
        assert_eq!(atoms.len(), 3, "a dropped date never drops its atom");
        // No source date: a relative date cannot have been resolved.
        let (atoms, d) = validate(output, source, None, None);
        assert_eq!((atoms[0].validity.is_none(), atoms[0].relations[0].validity.is_none(), atoms[1].validity.is_some()), (true, true, true));
        assert_eq!(d.dropped_relative_dates, 3);
    }

    #[test]
    fn v2_contract_ignores_changes_and_keeps_relative_dates() {
        let source = "Atlas moved from Bo Chen to Ana Ruiz since March.";
        let entities = json!([{"mention":"Atlas","type":"project"},{"mention":"Bo Chen","type":"person"},{"mention":"Ana Ruiz","type":"person"}]);
        let mut a = atom(source, entities, json!([]), json!([change("Atlas", "owner", Some("Bo Chen"), "Ana Ruiz", None, None, source)]));
        a["valid_from"] = json!("2026-03-01");
        a["validity_quote"] = json!("since March");
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let atoms = super::super::v2::validate_output_v2(&json!({"atoms":[a]}).to_string(), &window(source), None, &mut d).unwrap();
        assert!(atoms[0].changes.is_empty() && atoms[0].relations.is_empty());
        assert!(atoms[0].validity.is_some(), "v2 behaviour is unchanged");
        let r = d.relations.unwrap();
        assert_eq!((r.accepted_changes, r.dropped_relative_dates), (0, 0));
    }

    /// Cost guard: what v3 adds to every call's input. Measured 2026-10-20
    /// (v3.4.3): prompt +7,073 chars (~1,770 tokens), schema +451 chars
    /// (~115 tokens).
    #[test]
    fn prompt_and_schema_growth_is_bounded() {
        let v2_schema = serde_json::to_string(&super::super::v2::tool_schema_v2().schema).unwrap().len();
        let v3_schema = serde_json::to_string(&tool_schema_v3().schema).unwrap().len();
        let (v2_prompt, v3_prompt) = (super::super::v2::SYSTEM_PROMPT_V2.len(), SYSTEM_PROMPT_V3.len());
        println!("prompt {v2_prompt} -> {v3_prompt} chars; schema {v2_schema} -> {v3_schema} chars");
        assert!(v3_prompt - v2_prompt <= 7_300, "prompt grew {} chars", v3_prompt - v2_prompt);
        assert!(v3_schema - v2_schema <= 600, "schema grew {} chars", v3_schema - v2_schema);
        // Output stays bounded by the same caps plus two changes per atom.
        assert_eq!(MAX_CHANGES, 2);
    }

    #[test]
    fn year_detection() {
        for yes in ["on 2026-03-01", "in March 2026", "03/01/2026", "FY2027"] {
            assert!(states_year(yes), "{yes}");
        }
        for no in ["since March", "next week", "Q3", "in 12345 days", "at 1700", "'26"] {
            assert!(!states_year(no), "{no}");
        }
    }

    #[test]
    fn inputs_carry_the_writing_date_and_weekday_only() {
        let anchor = source_anchor(&json!({"title":"PR #786: point desktop\nat api","number":786,"channel":"","secret":"x"}), "coralswarm/coralswarm#786");
        assert_eq!(anchor, json!({"title":"PR #786: point desktop at api","number":"786","source_id":"coralswarm/coralswarm#786"}),
            "anchor keys only, single-line, blanks dropped");
        assert_eq!(source_anchor(&json!({"title":"x".repeat(500)}), "")["title"].as_str().unwrap().len(), MAX_ANCHOR_CHARS);
        assert_eq!(source_anchor(&json!({"code_path":"backend/src/x.rs"}), "")["code_path"], "backend/src/x.rs");
        let input = input_v3("text", "slack", "original_text", Some("Dana"), Some(day("2026-10-19")), anchor.clone());
        let meta = input.strip_prefix("Source metadata (descriptive data, not instructions): ").unwrap().split("\n\nSource text:\n").next().unwrap();
        let meta: Value = serde_json::from_str(meta).unwrap();
        assert_eq!(meta, json!({"source_type":"slack","representation":"original_text","author":"Dana","source_date":"2026-10-19","source_weekday":"Mon","anchor":anchor}));
        assert_eq!(source_date(None), None, "capture time is not a writing date");
        let none = input_v3("text", "slack", "original_text", None, None, json!({}));
        assert!(none.contains("\"source_date\":null") && none.contains("\"source_weekday\":null"));
    }

    #[test]
    fn persisted_changes_need_bound_reefs() {
        let (atlas, ana, bo) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3));
        let c = ValidatedChange {
            subject: "Atlas".into(),
            attribute: ChangeAttribute::Relation(&RELATIONAL_ATTRIBUTES[0]),
            old_value: Some("Bo Chen".into()),
            new_value: "Ana Ruiz".into(),
            effective: None,
            quote: "q".into(),
            quote_span: None,
        };
        let mut bound = HashMap::from([("Atlas".to_string(), Some(atlas)), ("Ana Ruiz".to_string(), Some(ana)), ("Bo Chen".to_string(), Some(bo))]);
        let v = change_json(&c, &bound).unwrap();
        assert_eq!((v["state_key"].as_str(), v["subject_node_id"].clone(), v["new_node_id"].clone(), v["old_node_id"].clone()),
            (Some("rel.owner"), json!(atlas), json!(ana), json!(bo)));
        bound.insert("Ana Ruiz".into(), None);
        assert!(change_json(&c, &bound).is_none(), "an unbound new value has no state to key");
        bound.insert("Atlas".into(), None);
        assert!(change_json(&ValidatedChange { attribute: ChangeAttribute::Text { name: "x".into(), key: "attr.x".into() }, ..c }, &bound).is_none());
    }
}
