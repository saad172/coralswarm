//! Supersession over extraction v2's temporal facts (source-agnostic pipeline
//! plan, PR 9).
//!
//! Extraction v2 (`workers::source_atoms::v2`) writes, per source version,
//! semantic atoms and stated relations that carry `valid_from` /
//! `valid_until` and the exact words stating them. This module turns those
//! into:
//!
//! 1. **Temporal evidence** (`temporal_evidence_records`,
//!    `recording_method='temporal_extraction_v1'`): one `assert` per fact,
//!    plus a `withdraw` chained to it when the evidence states only an end
//!    ("until March"). Idempotent per version (unique index, tenant 0167).
//! 2. **Supersession decisions** (`fact_supersession_edges`): when a fact
//!    about the same subject reef and state key replaces an older one. The
//!    deterministic rules come first ([`decide`]); only ambiguous pairs go to
//!    the metered judge ([`judge`], purpose `managed_enrichment`).
//! 3. **Predecessor chains**: in live mode, a decided replacement also records
//!    a `transition` whose predecessor is the replaced fact's `assert`, so the
//!    historical reconstruction (`temporal::read_tx`) ends the old state.
//!
//! ## Facts and state keys
//!
//! A fact is (semantic atom, subject reef, state key, value). Its stable
//! identity is [`Fact::key`].
//!
//! - **Relations** use a per-predicate key on the endpoint for which the
//!   predicate is single-valued ([`relation_key`]): `manages(M,E)` and
//!   `reports_to(E,M)` are both `E.rel.manager = M`; `responsible_for(P,X)` and
//!   `responsibility_of(X,P)` are both `X.rel.owner = P`; `leads_team(P,T)` is
//!   `T.rel.lead = P`; `works_at(P,O)` is `P.rel.employer = O`. Those four keys
//!   are single-valued (`predicates::is_single_valued`). `member_of` and
//!   `works_on` / `worked_on_by` are many-valued: their validity is recorded
//!   as temporal evidence, but a second value never replaces the first. Other
//!   predicates (kinship, social, mentions) are not state.
//! - **Atoms** that state a validity become `fact.<kind>` on every reef the
//!   atom canonically supports (`node_semantic_atoms`), value = the statement.
//!   An atom kind is not a state key (two dated decisions about one project
//!   are two decisions), so these are evidence only.
//! - **Stated changes** (extraction v3, [`changes`]): one statement that a
//!   value replaced another is a new fact and an old fact on the change's
//!   subject, keyed like a relation (`rel.*`) or by its attribute
//!   (`attr.<name>`), decided by `Rule::StatedChange` on any key: the
//!   evidence names the value it replaces.
//! - **Candidates.** Every other rule pairs facts only on a single-valued
//!   state key ([`single_valued_key`]): the four `rel.*` keys above, and
//!   extraction's `attr.<name>` keys (status, version, decision, ...), each
//!   of which names one value at a time.
//!
//! Facts are read from both structured profiles ([`STRUCTURED_VERSIONS`]).
//!
//! ## Modes (`FACT_SUPERSESSION_MODE`)
//!
//! - `off`: nothing is recorded or decided.
//! - `dry_run` (the default): temporal `assert`/`withdraw` evidence is
//!   recorded (it restates what the source says; no judgement), decisions are
//!   written with status `proposed` (deterministic) or `ambiguous`, and
//!   readers ignore both. No `transition` is written and the judge never runs.
//! - `live`: deterministic decisions are `active`, transitions are written,
//!   readers prefer current facts, and ambiguous pairs go to the judge when
//!   `ENRICHMENT_PROCESSING_ENABLED=true`.
//!
//! Flipping dry run to live: set the mode, then run
//! `backfill_fact_supersession --execute --mode live`, which promotes every
//! `proposed` row to `active` and writes the missing transitions.
//! `SUPERSESSION_AUTO_EDGES` is unrelated: it still gates only the legacy
//! Ask-time contradiction-hint writer for reef-level `supersession_edge`.
//!
//! ## Reef merges
//!
//! A merge (`merge_nodes_impl`) deletes the duplicate, which cascades its
//! records and decisions. Inside the merge transaction, before that delete,
//! [`after_merge_tx`] re-derives them on the keeper and on every reef whose
//! relation named the duplicate, retires what no current fact states, and
//! carries judged outcomes over; the undo runs the same reconciliation over
//! the restored rows ("Reef merges" below). Same modes, fail-open.
//! `backfill_fact_supersession --repair-merges` repairs merges made before
//! the hook.

pub mod changes;
pub mod judge;

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::relationships::predicates;
use crate::workers::source_atoms::{EXTRACTION_VERSION_V2, EXTRACTION_VERSION_V3};

/// The extraction profiles whose atoms and edges carry temporal facts.
pub const STRUCTURED_VERSIONS: &[&str] = &[EXTRACTION_VERSION_V2, EXTRACTION_VERSION_V3];

pub const MODE_ENV: &str = "FACT_SUPERSESSION_MODE";
pub const RECORDING_METHOD: &str = "temporal_extraction_v1";
/// Prior facts compared per (subject, key). Bounds every version's work.
pub const PRIOR_LIMIT: i64 = 32;
/// Facts read per version.
const VERSION_FACT_LIMIT: i64 = 512;
const MAX_VALUE_CHARS: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Off,
    DryRun,
    Live,
}

impl Mode {
    /// `off` / `live` are explicit; anything else (unset, blank, unknown) is
    /// dry run.
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("off") => Mode::Off,
            Some("live") => Mode::Live,
            _ => Mode::DryRun,
        }
    }
    pub fn from_env() -> Self {
        Self::parse(std::env::var(MODE_ENV).ok().as_deref())
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Off => "off",
            Mode::DryRun => "dry_run",
            Mode::Live => "live",
        }
    }
}

/// Readers prefer current facts only in live mode. Read per request, so the
/// read side rolls back with the env alone.
pub(crate) fn reads_live() -> bool {
    Mode::from_env() == Mode::Live
}

// ── State keys ──────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelationKey {
    pub key: &'static str,
    /// The state belongs to the edge's destination (else its source).
    pub subject_is_dst: bool,
    /// One value at a time (a later start replaces an earlier value).
    pub functional: bool,
}

pub fn relation_key(predicate: &str) -> Option<RelationKey> {
    let k = |key, subject_is_dst| Some(RelationKey { key, subject_is_dst, functional: predicates::is_single_valued(predicate) });
    match predicate {
        predicates::MANAGES => k("rel.manager", true),
        predicates::REPORTS_TO => k("rel.manager", false),
        predicates::RESPONSIBLE_FOR => k("rel.owner", true),
        predicates::RESPONSIBILITY_OF => k("rel.owner", false),
        predicates::LEADS_TEAM => k("rel.lead", true),
        predicates::WORKS_AT => k("rel.employer", false),
        predicates::MEMBER_OF => k("rel.member_of", false),
        predicates::WORKS_ON => k("rel.works_on", false),
        predicates::WORKED_ON_BY => k("rel.works_on", true),
        _ => None,
    }
}

/// Extraction's attribute keys (`attr.status`, `attr.version`,
/// `attr.decision`, ...): each names one value of its subject at a time.
/// A dated decision atom is not one (`fact.decision` is an atom kind).
pub const ATTRIBUTE_KEY_PREFIX: &str = "attr.";

/// The relation keys that hold one value at a time.
pub fn single_valued_relation_keys() -> Vec<&'static str> {
    let mut keys: Vec<&str> = KEYED_PREDICATES.iter().filter_map(|p| relation_key(p)).filter(|k| k.functional).map(|k| k.key).collect();
    keys.sort();
    keys.dedup();
    keys
}

/// One value replaces another only on these keys, so only these are paired.
pub fn single_valued_key(key: &str) -> bool {
    key.starts_with(ATTRIBUTE_KEY_PREFIX) || single_valued_relation_keys().contains(&key)
}

const KEYED_PREDICATES: &[&str] = &[
    predicates::MANAGES,
    predicates::REPORTS_TO,
    predicates::RESPONSIBLE_FOR,
    predicates::RESPONSIBILITY_OF,
    predicates::LEADS_TEAM,
    predicates::WORKS_AT,
    predicates::MEMBER_OF,
    predicates::WORKS_ON,
    predicates::WORKED_ON_BY,
];

fn predicates_for_key(key: &str) -> Vec<&'static str> {
    KEYED_PREDICATES
        .iter()
        .copied()
        .filter(|p| relation_key(p).is_some_and(|k| k.key == key))
        .collect()
}

// ── Facts ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct Fact {
    pub atom_id: Uuid,
    pub version_id: Uuid,
    pub source_item_id: Uuid,
    pub subject: Uuid,
    pub state_key: String,
    pub value: String,
    /// The reef the value names (relations only).
    pub value_node: Option<Uuid>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub validity_quote: Option<String>,
    pub passage_id: Uuid,
    pub quote: String,
    /// When the source says it happened, else when it was captured.
    pub observed_at: DateTime<Utc>,
    /// The subject canonically supports the atom (`node_semantic_atoms`),
    /// which temporal evidence requires.
    pub linked: bool,
    /// Extraction v3: this fact is one side of a stated change.
    pub change: Option<changes::StatedChange>,
}

impl Fact {
    fn value_identity(&self) -> String {
        match self.value_node {
            Some(node) => format!("node:{node}"),
            None => format!("text:{}", self.value.trim().to_lowercase()),
        }
    }

    /// 32 hex chars over (atom, subject, key, value).
    pub fn key(&self) -> String {
        let digest = Sha256::digest(
            format!("{}|{}|{}|{}", self.atom_id, self.subject, self.state_key, self.value_identity()).as_bytes(),
        );
        hex::encode(&digest[..16])
    }

    fn same_value(&self, other: &Fact) -> bool {
        self.value_identity() == other.value_identity() || (self.value_node.is_some() && other.value_node.is_some() && same_name(&self.value, &other.value))
    }
}

#[derive(sqlx::FromRow)]
struct AtomFactRow {
    semantic_atom_id: Uuid,
    source_version_id: Uuid,
    source_item_id: Uuid,
    node_id: Uuid,
    kind: String,
    statement: String,
    valid_from: Option<DateTime<Utc>>,
    valid_until: Option<DateTime<Utc>>,
    validity_quote: Option<String>,
    passage_id: Uuid,
    quote: String,
    observed_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct RelationFactRow {
    semantic_atom_id: Uuid,
    source_version_id: Uuid,
    source_item_id: Uuid,
    src_node_id: Uuid,
    dst_node_id: Uuid,
    predicate: String,
    valid_from: Option<DateTime<Utc>>,
    valid_until: Option<DateTime<Utc>>,
    validity_quote: Option<String>,
    passage_id: Uuid,
    quote: String,
    observed_at: DateTime<Utc>,
    src_title: String,
    dst_title: String,
}

/// Which facts to read: one version's, or the most recent ones on one
/// (subject, key).
#[derive(Clone, Debug)]
pub enum FactFilter {
    Version(Uuid),
    Subject { node: Uuid, key: String },
}

/// Sources that state facts about the world. Captured code (source files and
/// their summaries) and test files state fictional ones: fixtures ("Xavier
/// owns Apollo until March"), examples, doc comments. Their facts are never
/// recorded, paired or judged. Reads the source item as `s`.
pub(crate) const STATED_SOURCE_SQL: &str = "(s.source_type NOT IN ('github_code','github_code_summary') \
    AND s.source_id !~* '(^|/)(tests?|__tests__|specs?|fixtures?|testdata)/|(^|/)test_[^/]*$|_test\\.[a-z0-9]+$|\\.(test|spec)\\.[a-z0-9]+$')";

const PASSAGE_JOIN: &str = "JOIN LATERAL (SELECT p.passage_id,p.quote FROM semantic_atom_passages p \
     WHERE p.graph_id=a.graph_id AND p.source_version_id=a.source_version_id AND p.semantic_atom_id=a.semantic_atom_id \
     ORDER BY p.start_char,p.passage_id LIMIT 1) p ON true";

fn truncate(text: &str, max: usize) -> String {
    text.trim().chars().take(max).collect()
}

/// Reads facts from the canonical v2 rows (never from earlier decisions), so
/// the hook, the backfill and the judge agree on what a fact is.
///
/// Each filter gets its own SQL text rather than `($n IS NULL OR ...)`: these
/// statements repeat on one connection (the backfill runs them per version),
/// and Postgres's generic plan for the optional form took 20-30 s per call on
/// the production tenant, against milliseconds for the filtered form.
pub async fn load_facts(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    filter: &FactFilter,
) -> Result<Vec<Fact>, sqlx::Error> {
    let (id, node, key, limit) = match filter {
        FactFilter::Version(v) => (*v, None, None, VERSION_FACT_LIMIT),
        FactFilter::Subject { node, key } => (*node, Some(*node), Some(key.as_str()), PRIOR_LIMIT),
    };
    let (atom_filter, relation_filter) = match filter {
        FactFilter::Version(_) => ("a.source_version_id=$3", "a.source_version_id=$4"),
        FactFilter::Subject { .. } => ("l.node_id=$3 AND 'fact.'||a.kind=$5", "(e.src_node_id=$4 OR e.dst_node_id=$4)"),
    };
    let mut facts = Vec::new();
    let atom_key = key.filter(|k| k.starts_with("fact."));
    if key.is_none() || atom_key.is_some() {
        let sql = format!(
            "SELECT a.semantic_atom_id,a.source_version_id,v.source_item_id,l.node_id,a.kind,a.statement,\
                    a.valid_from,a.valid_until,a.validity_quote,p.passage_id,p.quote,\
                    COALESCE(v.occurred_at,v.captured_at) observed_at \
               FROM semantic_atoms a \
               JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id \
               JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id AND s.access_state='permitted' \
               JOIN node_semantic_atoms l ON l.graph_id=a.graph_id AND l.semantic_atom_id=a.semantic_atom_id \
               {PASSAGE_JOIN} \
              WHERE a.graph_id=$1 AND a.extraction_version=ANY($2) \
                AND (a.valid_from IS NOT NULL OR a.valid_until IS NOT NULL) \
                AND {atom_filter} AND {STATED_SOURCE_SQL} \
              ORDER BY COALESCE(v.occurred_at,v.captured_at) DESC,a.semantic_atom_id,l.node_id LIMIT $4"
        );
        let mut query = sqlx::query_as(&sql).bind(graph_id).bind(STRUCTURED_VERSIONS).bind(id).bind(limit);
        if let Some(key) = atom_key {
            query = query.bind(key);
        }
        let rows: Vec<AtomFactRow> = query.fetch_all(&mut **tx).await?;
        facts.extend(rows.into_iter().filter_map(atom_fact));
    }
    let relation_predicates: Vec<&str> = match key {
        None => KEYED_PREDICATES.to_vec(),
        Some(k) => predicates_for_key(k),
    };
    if !relation_predicates.is_empty() {
        let sql = format!(
            "SELECT a.semantic_atom_id,a.source_version_id,v.source_item_id,e.src_node_id,e.dst_node_id,e.predicate,\
                    NULLIF(x->>'valid_from','')::timestamptz valid_from,NULLIF(x->>'valid_until','')::timestamptz valid_until,\
                    NULLIF(x->>'validity_quote','') validity_quote,p.passage_id,p.quote,\
                    COALESCE(v.occurred_at,v.captured_at) observed_at,src.title src_title,dst.title dst_title \
               FROM relationship_edge e \
               CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(e.provenance->'assertions')='array' \
                    THEN e.provenance->'assertions' ELSE '[]'::jsonb END) x \
               JOIN semantic_atoms a ON a.graph_id=e.graph_id AND a.semantic_atom_id=(CASE WHEN x->>'semantic_atom_id' \
                    ~ '^[0-9a-fA-F-]{{36}}$' THEN (x->>'semantic_atom_id')::uuid END) \
               JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id \
               JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id AND s.access_state='permitted' \
               JOIN nodes src ON src.node_id=e.src_node_id AND src.graph_id=e.graph_id \
               JOIN nodes dst ON dst.node_id=e.dst_node_id AND dst.graph_id=e.graph_id \
               {PASSAGE_JOIN} \
              WHERE e.graph_id=$1 AND a.extraction_version=ANY($2) AND e.predicate=ANY($3) AND {relation_filter} AND {STATED_SOURCE_SQL} \
              ORDER BY COALESCE(v.occurred_at,v.captured_at) DESC,a.semantic_atom_id,e.edge_id LIMIT $5"
        );
        let rows: Vec<RelationFactRow> = sqlx::query_as(&sql)
            .bind(graph_id)
            .bind(STRUCTURED_VERSIONS)
            .bind(&relation_predicates)
            .bind(id)
            .bind(limit * 2)
            .fetch_all(&mut **tx)
            .await?;
        facts.extend(
            rows.into_iter()
                .filter_map(relation_fact)
                .filter(|f| node.is_none_or(|n| n == f.subject) && key.is_none_or(|k| k == f.state_key)),
        );
        mark_linked(tx, graph_id, &mut facts).await?;
    }
    // Stated changes first: a relational change's new value is also its
    // stated edge, and the dedupe below keeps the copy that knows what it
    // replaced. Read after `mark_linked`: a change sets its own links.
    let mut facts = [changes::load_facts(tx, graph_id, filter).await?, facts].concat();
    // A forward edge and its materialized inverse are the same fact.
    let mut seen = HashSet::new();
    facts.retain(|f| seen.insert(f.key()));
    Ok(facts)
}

fn atom_fact(r: AtomFactRow) -> Option<Fact> {
    let value = truncate(&r.statement, MAX_VALUE_CHARS);
    if value.is_empty() {
        return None;
    }
    Some(Fact {
        atom_id: r.semantic_atom_id,
        version_id: r.source_version_id,
        source_item_id: r.source_item_id,
        subject: r.node_id,
        state_key: format!("fact.{}", r.kind),
        value,
        value_node: None,
        valid_from: r.valid_from,
        valid_until: r.valid_until,
        validity_quote: r.validity_quote,
        passage_id: r.passage_id,
        quote: r.quote,
        observed_at: r.observed_at,
        linked: true,
        change: None,
    })
}

fn relation_fact(r: RelationFactRow) -> Option<Fact> {
    let rk = relation_key(&r.predicate)?;
    let (subject, value_node, title) = if rk.subject_is_dst {
        (r.dst_node_id, r.src_node_id, r.src_title)
    } else {
        (r.src_node_id, r.dst_node_id, r.dst_title)
    };
    let value = truncate(&title, MAX_VALUE_CHARS);
    if value.is_empty() || subject == value_node {
        return None;
    }
    Some(Fact {
        atom_id: r.semantic_atom_id,
        version_id: r.source_version_id,
        source_item_id: r.source_item_id,
        subject,
        state_key: rk.key.to_owned(),
        value,
        value_node: Some(value_node),
        valid_from: r.valid_from,
        // An inverted range is not a validity; keep the fact undated.
        valid_until: r.valid_until.filter(|end| r.valid_from.is_none_or(|start| *end > start)),
        validity_quote: r.validity_quote,
        passage_id: r.passage_id,
        quote: r.quote,
        observed_at: r.observed_at,
        linked: false,
        change: None,
    })
}

/// Temporal evidence needs the subject to canonically support the atom.
async fn mark_linked(tx: &mut Transaction<'_, Postgres>, graph_id: Uuid, facts: &mut [Fact]) -> Result<(), sqlx::Error> {
    let atoms: Vec<Uuid> = facts.iter().filter(|f| f.value_node.is_some()).map(|f| f.atom_id).collect();
    if atoms.is_empty() {
        return Ok(());
    }
    let links: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT node_id,semantic_atom_id FROM node_semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=ANY($2)")
            .bind(graph_id)
            .bind(&atoms)
            .fetch_all(&mut **tx)
            .await?;
    let links: HashSet<(Uuid, Uuid)> = links.into_iter().collect();
    for f in facts.iter_mut().filter(|f| f.value_node.is_some()) {
        f.linked = links.contains(&(f.subject, f.atom_id));
    }
    Ok(())
}

// ── Deterministic rules ─────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Rule {
    /// The older fact states its end and the newer one starts at or after it.
    ExplicitEnd,
    /// A single-valued key: a later stated start replaces the earlier value.
    LaterStart,
    /// The newer evidence says it replaces the older value.
    ReplacementLanguage,
    /// Decided by the metered judge.
    Judge,
    /// One statement names the old value and the new one (extraction v3,
    /// [`changes::stated_replaces`]).
    StatedChange,
}

impl Rule {
    pub fn as_str(self) -> &'static str {
        match self {
            Rule::ExplicitEnd => "explicit_end",
            Rule::LaterStart => "later_start",
            Rule::ReplacementLanguage => "replacement_language",
            Rule::Judge => "judge",
            Rule::StatedChange => "stated_change",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    A,
    B,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// `superseder` replaces the other fact.
    Supersede { superseder: Side, rule: Rule },
    /// A temporal signal exists but the rules cannot decide; `newer` is the
    /// candidate superseder.
    Ambiguous { newer: Side, signal: Rule },
    /// Coexisting, corroborating, or an undated conflict (left disputed:
    /// capture order never picks a winner).
    Independent,
}

/// Lower-case phrases that state a replacement. Matched as substrings of the
/// newer fact's exact evidence quote.
const REPLACEMENT_PHRASES: &[&str] = &[
    "replaces", "replaced", "replacing", "took over", "takes over", "taking over", "take over",
    "instead of", "no longer", "handed over", "hands over", "handing over", "handed off",
    "moved from", "switched from", "succeeded by", "in place of", "new owner", "now owns",
    "now leads", "now manages", "now reports to", "formerly", "previously",
];

pub fn has_replacement_language(quote: &str) -> bool {
    let lower = quote.to_lowercase();
    REPLACEMENT_PHRASES.iter().any(|p| lower.contains(p))
}

fn other(side: Side) -> Side {
    match side {
        Side::A => Side::B,
        Side::B => Side::A,
    }
}

/// One name is the other's leading words ("Ana" and "Ana Ruiz", "Acme"
/// and "Acme Corp"): the same entity under a short label, not two values.
pub fn same_name(a: &str, b: &str) -> bool {
    let words = |s: &str| -> Vec<String> {
        s.split(|c: char| !c.is_alphanumeric()).filter(|w| !w.is_empty()).map(str::to_lowercase).collect()
    };
    let (a, b) = (words(a), words(b));
    let (short, long) = if a.len() <= b.len() { (&a, &b) } else { (&b, &a) };
    !short.is_empty() && long.starts_with(short)
}

/// Words that put the value after them in the past ("previously owned by
/// X"). A quote that says its own value was the earlier one is the old
/// side, whatever order the sources were captured in.
const PAST_MARKERS: &[&str] = &["previously", "formerly", "used to be", "was owned by", "was led by", "was managed by"];

fn names_as_past(quote: &str, value: &str) -> bool {
    let (quote, value) = (quote.to_lowercase(), value.to_lowercase());
    PAST_MARKERS.iter().any(|m| {
        quote.match_indices(m).any(|(i, _)| {
            let after: String = quote[i + m.len()..].chars().take(60).collect();
            after.contains(&value)
        })
    })
}

/// Decide one pair of facts on the same subject and key. Pure.
pub fn decide(a: &Fact, b: &Fact) -> Verdict {
    if a.subject != b.subject || a.state_key != b.state_key || a.key() == b.key() || a.same_value(b) {
        return Verdict::Independent;
    }
    // R0: the evidence itself states the change. Any key: it names the value.
    if changes::stated_replaces(b, a) {
        return Verdict::Supersede { superseder: Side::B, rule: Rule::StatedChange };
    }
    if changes::stated_replaces(a, b) {
        return Verdict::Supersede { superseder: Side::A, rule: Rule::StatedChange };
    }
    // Many-valued keys hold both values at once: no pair rule applies.
    if !changes::comparable(a, b) || !single_valued_key(&a.state_key) {
        return Verdict::Independent;
    }
    let pick = |s: Side| -> &Fact { if s == Side::A { a } else { b } };
    // R1: x ends, y starts at or after the end.
    for y in [Side::A, Side::B] {
        let (x, yf) = (pick(other(y)), pick(y));
        if let (Some(end), Some(start)) = (x.valid_until, yf.valid_from) {
            if start >= end {
                return Verdict::Supersede { superseder: y, rule: Rule::ExplicitEnd };
            }
        }
    }
    // R2: both dated, the later start wins.
    if let (Some(sa), Some(sb)) = (a.valid_from, b.valid_from) {
        if sa != sb {
            let y = if sb > sa { Side::B } else { Side::A };
            return Verdict::Supersede { superseder: y, rule: Rule::LaterStart };
        }
    }
    // R3: the newer evidence states the replacement. Newer = strictly later
    // observed; on a tie (one document, one capture) nothing says which side
    // is newer, so nothing below decides.
    if a.observed_at == b.observed_at {
        return Verdict::Independent;
    }
    let newer = if b.observed_at > a.observed_at { Side::B } else { Side::A };
    let (old, new) = (pick(other(newer)), pick(newer));
    if names_as_past(&new.quote, &new.value) {
        return Verdict::Independent;
    }
    if has_replacement_language(&new.quote) {
        let names_old = old.value_node.is_some() && new.quote.to_lowercase().contains(&old.value.to_lowercase());
        return if names_old {
            Verdict::Supersede { superseder: newer, rule: Rule::ReplacementLanguage }
        } else {
            Verdict::Ambiguous { newer, signal: Rule::ReplacementLanguage }
        };
    }
    // One side dated: a signal, not a decision.
    if (a.valid_from.is_some() || a.valid_until.is_some()) != (b.valid_from.is_some() || b.valid_until.is_some()) {
        return Verdict::Ambiguous { newer, signal: Rule::LaterStart };
    }
    Verdict::Independent
}

/// `change` unless the superseding evidence states a correction
/// (`services::supersession::classify_supersession_kind`).
fn kind_of(superseder: &Fact) -> &'static str {
    crate::services::supersession::classify_supersession_kind(&superseder.quote).as_str()
}

// ── Writes ──────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Report {
    pub versions: u64,
    pub facts: u64,
    /// Facts whose subject does not canonically support the atom: decided,
    /// but no temporal evidence.
    pub facts_without_evidence: u64,
    pub asserts: u64,
    pub withdraws: u64,
    pub transitions: u64,
    pub proposed: u64,
    pub active: u64,
    pub promoted: u64,
    pub ambiguous: u64,
    pub blocked: u64,
    /// Extraction records a merge or undo left unsupported by any current
    /// fact, marked `evidence_retracted` (with everything chained to them).
    pub retracted: u64,
    /// Decisions about facts a merge or undo removed.
    pub decisions_removed: u64,
    /// Judged outcomes moved onto a re-derived decision.
    pub carried: u64,
    /// Re-derivations skipped because the change was larger than one
    /// transaction may read ([`RECONCILE_FACT_LIMIT`]); repair with
    /// `backfill_fact_supersession --repair-merges`.
    pub skipped: u64,
    pub by_rule: BTreeMap<String, u64>,
}

impl Report {
    pub fn add(&mut self, other: &Report) {
        self.versions += other.versions;
        self.facts += other.facts;
        self.facts_without_evidence += other.facts_without_evidence;
        self.asserts += other.asserts;
        self.withdraws += other.withdraws;
        self.transitions += other.transitions;
        self.proposed += other.proposed;
        self.active += other.active;
        self.promoted += other.promoted;
        self.ambiguous += other.ambiguous;
        self.blocked += other.blocked;
        self.retracted += other.retracted;
        self.decisions_removed += other.decisions_removed;
        self.carried += other.carried;
        self.skipped += other.skipped;
        for (k, v) in &other.by_rule {
            *self.by_rule.entry(k.clone()).or_default() += v;
        }
    }
}

/// The tenant plane carries `tenant_meta`; the control plane does not.
async fn is_tenant_plane(tx: &mut Transaction<'_, Postgres>) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT to_regclass('public.tenant_meta') IS NOT NULL").fetch_one(&mut **tx).await
}

/// Temporal evidence names who recorded it. A request scope already carries
/// the user. A worker on the consumer plane has only the account, and the
/// consumer temporal policy also requires a reachable user, so the account
/// owner records it there. The tenant plane's system scope needs no user.
/// Returns the previous `app.user_id` to restore, when one was set here.
async fn recording_actor(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
) -> Result<(Uuid, Option<String>), sqlx::Error> {
    let current: Option<String> = sqlx::query_scalar("SELECT NULLIF(current_setting('app.user_id',true),'')")
        .fetch_one(&mut **tx)
        .await?;
    if let Some(user) = current.as_deref().and_then(|u| Uuid::parse_str(u).ok()) {
        return Ok((user, None));
    }
    if is_tenant_plane(tx).await? {
        let owner: Option<Uuid> = sqlx::query_scalar("SELECT owner_user_id FROM graphs WHERE id=$1")
            .bind(graph_id)
            .fetch_optional(&mut **tx)
            .await?
            .flatten();
        return Ok((owner.unwrap_or(Uuid::nil()), None));
    }
    let owner: Option<Uuid> = sqlx::query_scalar(
        "SELECT COALESCE(g.owner_user_id,a.owner_user_id) FROM graphs g JOIN accounts a ON a.id=g.account_id WHERE g.id=$1",
    )
    .bind(graph_id)
    .fetch_optional(&mut **tx)
    .await?
    .flatten();
    let owner = owner.unwrap_or(Uuid::nil());
    sqlx::query("SELECT set_config('app.user_id',$1,true)")
        .bind(owner.to_string())
        .execute(&mut **tx)
        .await?;
    Ok((owner, Some(current.unwrap_or_default())))
}

async fn restore_actor(tx: &mut Transaction<'_, Postgres>, previous: Option<String>) -> Result<(), sqlx::Error> {
    if let Some(previous) = previous {
        sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(previous).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn observation(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    fact: &Fact,
) -> Result<Uuid, sqlx::Error> {
    let existing: Option<Uuid> = sqlx::query_scalar(
        "SELECT observation_id FROM source_version_observations WHERE graph_id=$1 AND source_version_id=$2 \
          ORDER BY known_at,observation_id LIMIT 1",
    )
    .bind(graph_id)
    .bind(fact.version_id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(id) = existing {
        return Ok(id);
    }
    sqlx::query_scalar(
        "INSERT INTO source_version_observations(graph_id,source_item_id,source_version_id,observation_kind) \
         VALUES($1,$2,$3,'annotation_recorded') RETURNING observation_id",
    )
    .bind(graph_id)
    .bind(fact.source_item_id)
    .bind(fact.version_id)
    .fetch_one(&mut **tx)
    .await
}

/// The supported extraction record for (fact, operation, predecessor).
async fn find_record(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    fact: &Fact,
    operation: &str,
    value: Option<&str>,
    predecessor: Option<Uuid>,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT event_id FROM temporal_evidence_records WHERE graph_id=$1 AND semantic_atom_id=$2 AND subject_node_id=$3 \
           AND state_key=$4 AND operation=$5 AND COALESCE(value,'')=COALESCE($6,'') \
           AND predecessor_event_id IS NOT DISTINCT FROM $7 AND recording_method=$8 AND lifecycle='supported' LIMIT 1",
    )
    .bind(graph_id)
    .bind(fact.atom_id)
    .bind(fact.subject)
    .bind(&fact.state_key)
    .bind(operation)
    .bind(value)
    .bind(predecessor)
    .bind(RECORDING_METHOD)
    .fetch_optional(&mut **tx)
    .await
}

struct Recorder {
    actor: Uuid,
    write: bool,
    /// Per pass: each version's observation, and each fact's assert (by
    /// [`Fact::key`]), so a fact compared with many others is looked up once.
    observations: std::sync::Mutex<HashMap<Uuid, Uuid>>,
    asserts: std::sync::Mutex<HashMap<String, Uuid>>,
}

impl Recorder {
    fn new(actor: Uuid, write: bool) -> Self {
        Self { actor, write, observations: Default::default(), asserts: Default::default() }
    }

    async fn observation(&self, tx: &mut Transaction<'_, Postgres>, graph_id: Uuid, fact: &Fact) -> Result<Uuid, sqlx::Error> {
        if let Some(id) = self.observations.lock().expect("observation cache").get(&fact.version_id) {
            return Ok(*id);
        }
        let id = observation(tx, graph_id, fact).await?;
        self.observations.lock().expect("observation cache").insert(fact.version_id, id);
        Ok(id)
    }

    /// Idempotent insert of one extraction record. Returns (event, written).
    #[allow(clippy::too_many_arguments)]
    async fn record(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        graph_id: Uuid,
        fact: &Fact,
        operation: &str,
        value: Option<&str>,
        valid_from: Option<DateTime<Utc>>,
        valid_until: Option<DateTime<Utc>>,
        quote: Option<&str>,
        predecessor: Option<Uuid>,
    ) -> Result<(Option<Uuid>, bool), sqlx::Error> {
        if let Some(id) = find_record(tx, graph_id, fact, operation, value, predecessor).await? {
            return Ok((Some(id), false));
        }
        if !self.write {
            return Ok((None, true));
        }
        let observation = self.observation(tx, graph_id, fact).await?;
        let quote = quote.filter(|_| valid_from.is_some()).map(|q| truncate(q, 4000)).filter(|q| !q.is_empty());
        let id: Option<Uuid> = sqlx::query_scalar(
            "INSERT INTO temporal_evidence_records(graph_id,subject_node_id,state_key,source_item_id,source_version_id,\
                semantic_atom_id,passage_id,observation_id,operation,value,valid_from,valid_until,validity_quote,\
                predecessor_event_id,recording_method,recorded_by) \
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT DO NOTHING RETURNING event_id",
        )
        .bind(graph_id)
        .bind(fact.subject)
        .bind(&fact.state_key)
        .bind(fact.source_item_id)
        .bind(fact.version_id)
        .bind(fact.atom_id)
        .bind(fact.passage_id)
        .bind(observation)
        .bind(operation)
        .bind(value)
        .bind(valid_from)
        .bind(valid_until.filter(|_| valid_from.is_some()))
        .bind(quote)
        .bind(predecessor)
        .bind(RECORDING_METHOD)
        .bind(self.actor)
        .fetch_optional(&mut **tx)
        .await?;
        match id {
            Some(id) => Ok((Some(id), true)),
            None => Ok((find_record(tx, graph_id, fact, operation, value, predecessor).await?, false)),
        }
    }

    /// `assert` (and, for an end-only validity, a chained `withdraw`).
    /// Returns the assert's event id when it exists or was written.
    async fn evidence(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        graph_id: Uuid,
        fact: &Fact,
        report: &mut Report,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        if !fact.linked {
            return Ok(None);
        }
        if let Some(id) = self.asserts.lock().expect("assert cache").get(&fact.key()) {
            return Ok(Some(*id));
        }
        let (assert, wrote) = self
            .record(tx, graph_id, fact, "assert", Some(&fact.value), fact.valid_from, fact.valid_until,
                fact.validity_quote.as_deref(), None)
            .await?;
        report.asserts += u64::from(wrote);
        if fact.valid_from.is_none() {
            if let Some(end) = fact.valid_until {
                match assert {
                    Some(assert) => {
                        let (_, wrote) = self
                            .record(tx, graph_id, fact, "withdraw", None, Some(end), None,
                                fact.validity_quote.as_deref(), Some(assert))
                            .await?;
                        report.withdraws += u64::from(wrote);
                    }
                    None => report.withdraws += u64::from(!self.write),
                }
            }
        }
        if let Some(id) = assert {
            self.asserts.lock().expect("assert cache").insert(fact.key(), id);
        }
        Ok(assert)
    }
}

/// The `non_supersession_pair` rows among these value reefs, read once per
/// (subject, key) group instead of once per pair.
async fn non_supersession_pairs(
    tx: &mut Transaction<'_, Postgres>,
    facts: &[Fact],
) -> Result<HashSet<(Uuid, Uuid)>, sqlx::Error> {
    let values: Vec<Uuid> = sorted(facts.iter().filter_map(|f| f.value_node).collect());
    if values.len() < 2 {
        return Ok(HashSet::new());
    }
    let rows: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT node_a_id,node_b_id FROM non_supersession_pair WHERE node_a_id=ANY($1) AND node_b_id=ANY($1)")
            .bind(&values)
            .fetch_all(&mut **tx)
            .await?;
    Ok(rows.into_iter().collect())
}

/// Each value reef's names (title and aliases, lower-cased), read once per
/// (subject, key) group.
async fn value_names(
    tx: &mut Transaction<'_, Postgres>,
    facts: &[Fact],
) -> Result<HashMap<Uuid, HashSet<String>>, sqlx::Error> {
    let values: Vec<Uuid> = sorted(facts.iter().filter_map(|f| f.value_node).collect());
    if values.len() < 2 {
        return Ok(HashMap::new());
    }
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT n.node_id,lower(btrim(x.name)) FROM nodes n \
           CROSS JOIN LATERAL (SELECT n.title name UNION ALL SELECT jsonb_array_elements_text(CASE WHEN \
             jsonb_typeof(n.body->'aliases')='array' THEN n.body->'aliases' ELSE '[]'::jsonb END)) x \
          WHERE n.node_id=ANY($1)",
    )
    .bind(&values)
    .fetch_all(&mut **tx)
    .await?;
    let mut names: HashMap<Uuid, HashSet<String>> = HashMap::new();
    for (node, name) in rows.into_iter().filter(|(_, n)| !n.is_empty()) {
        names.entry(node).or_default().insert(name);
    }
    Ok(names)
}

/// Two value reefs that share a name, or where one name is the other's
/// short label: one entity known twice, never one replacing the other.
fn values_name_one_entity(names: &HashMap<Uuid, HashSet<String>>, a: Option<Uuid>, b: Option<Uuid>) -> bool {
    let (Some(a), Some(b)) = (a, b) else { return false };
    let (Some(na), Some(nb)) = (names.get(&a), names.get(&b)) else { return false };
    na.iter().any(|x| nb.iter().any(|y| x == y || same_name(x, y)))
}

fn values_are_non_supersession_pair(pairs: &HashSet<(Uuid, Uuid)>, a: Option<Uuid>, b: Option<Uuid>) -> bool {
    let (Some(a), Some(b)) = (a, b) else { return false };
    a != b && pairs.contains(&(a.min(b), a.max(b)))
}

fn provenance(superseder: &Fact, superseded: &Fact, rule: Rule, mode: Mode) -> serde_json::Value {
    let side = |f: &Fact| {
        json!({
            "semantic_atom_id": f.atom_id, "source_version_id": f.version_id, "passage_id": f.passage_id,
            "value": f.value, "value_node_id": f.value_node,
            "valid_from": f.valid_from, "valid_until": f.valid_until,
            "validity_quote": f.validity_quote, "observed_at": f.observed_at,
        })
    };
    json!({
        "rule": rule.as_str(),
        "decided_in_mode": mode.as_str(),
        "recording_method": RECORDING_METHOD,
        "superseder": side(superseder),
        "superseded": side(superseded),
    })
}

/// Both sides of one stated change come from a single atom. Readers already
/// skip such a pair, and its subject is the extractor's guess from one
/// sentence, so it never ends a state in the temporal record either.
fn one_atom(superseder: &Fact, superseded: &Fact) -> bool {
    superseder.atom_id == superseded.atom_id
}

/// A live replacement also ends the replaced state in the temporal record:
/// a `transition` chained to the replaced fact's `assert`. Only when both
/// facts carry evidence and the replacement states its start.
async fn chain_transition(
    rec: &Recorder,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    superseder: &Fact,
    superseded: &Fact,
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    let Some(start) = superseder.valid_from else { return Ok(()) };
    if !superseder.linked || one_atom(superseder, superseded) {
        return Ok(());
    }
    let Some(predecessor) = rec.evidence(tx, graph_id, superseded, report).await? else {
        return Ok(());
    };
    let (_, wrote) = rec
        .record(tx, graph_id, superseder, "transition", Some(&superseder.value), Some(start), superseder.valid_until,
            superseder.validity_quote.as_deref(), Some(predecessor))
        .await?;
    report.transitions += u64::from(wrote);
    Ok(())
}

/// Record evidence and decide supersession for one source version. `write`
/// false computes the same report inside a read-only transaction.
pub async fn process_version_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    mode: Mode,
    write: bool,
) -> Result<Report, sqlx::Error> {
    process_version_in_pass(tx, graph_id, version_id, mode, write, &mut Pass::default()).await
}

/// Pairs already decided in one backfill pass. A dry run writes nothing, so
/// without this every pair would be counted once per version that holds it.
#[derive(Default)]
pub struct Pass {
    pairs: HashSet<(String, String)>,
    atoms: HashSet<(Uuid, Uuid)>,
    /// Judged outcomes of decisions a merge removed, by (state key,
    /// superseder atom, superseded atom): the re-derived pair takes them
    /// instead of going back to the judge.
    carry: HashMap<(String, Uuid, Uuid), Carried>,
}

/// A judge (or curator) outcome moved from a removed decision.
#[derive(Clone, Debug)]
struct Carried {
    edge_id: Uuid,
    status: String,
    judge_receipt_id: Option<Uuid>,
    judge_attempts: i16,
}

pub async fn process_version_in_pass(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    mode: Mode,
    write: bool,
    pass: &mut Pass,
) -> Result<Report, sqlx::Error> {
    let mut report = Report { versions: 1, ..Report::default() };
    if mode == Mode::Off {
        return Ok(report);
    }
    let facts = load_facts(tx, graph_id, &FactFilter::Version(version_id)).await?;
    if facts.is_empty() {
        return Ok(report);
    }
    let (actor, previous) = if write { recording_actor(tx, graph_id).await? } else { (Uuid::nil(), None) };
    let rec = Recorder::new(actor, write);
    let result = derive(&rec, tx, graph_id, &facts, mode, pass, &mut report).await;
    if write {
        restore_actor(tx, previous).await?;
    }
    result?;
    Ok(report)
}

/// Record evidence for `facts` and decide each against the other facts on
/// its subject and key (the most recent [`PRIOR_LIMIT`] of them).
async fn derive(
    rec: &Recorder,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    facts: &[Fact],
    mode: Mode,
    pass: &mut Pass,
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    report.facts += facts.len() as u64;
    for fact in facts {
        if !fact.linked {
            report.facts_without_evidence += 1;
        }
        rec.evidence(tx, graph_id, fact, report).await?;
    }
    // Relation keys first: a pair of atoms decided on a relation is not
    // sent to the judge again as two dated statements.
    let mut groups: BTreeMap<(bool, Uuid, String), Vec<&Fact>> = BTreeMap::new();
    for fact in facts {
        groups.entry((fact.value_node.is_none(), fact.subject, fact.state_key.clone())).or_default().push(fact);
    }
    for ((_, subject, key), new_facts) in groups {
        let priors = load_facts(tx, graph_id, &FactFilter::Subject { node: subject, key: key.clone() }).await?;
        // Each key once: it is a digest, and every pair below needs two.
        let new_keyed: Vec<(String, &Fact)> = new_facts.iter().map(|f| (f.key(), *f)).collect();
        let mut pool: HashMap<String, Fact> = priors.into_iter().map(|f| (f.key(), f)).collect();
        for (k, f) in &new_keyed {
            pool.insert(k.clone(), (*f).clone());
        }
        // Deterministic order: the first pair to reach an atom pair decides it.
        let mut pool: Vec<(String, Fact)> = pool.into_iter().collect();
        pool.sort_by(|(ka, a), (kb, b)| b.observed_at.cmp(&a.observed_at).then_with(|| ka.cmp(kb)));
        decide_group(rec, tx, graph_id, &new_keyed, &pool, mode, pass, report).await?;
    }
    Ok(())
}

fn atom_pair(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
    (a.min(b), a.max(b))
}

/// Atom pairs another decision already relates (decided, blocked, or sent
/// to the judge, under any key): one judge question per atom pair. One read
/// for every atom the group's new facts carry.
async fn related_atom_pairs(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    atoms: &[Uuid],
) -> Result<HashSet<(Uuid, Uuid)>, sqlx::Error> {
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT superseder_atom_id,superseded_atom_id FROM fact_supersession_edges \
          WHERE graph_id=$1 AND (superseder_atom_id=ANY($2) OR superseded_atom_id=ANY($2))",
    )
    .bind(graph_id)
    .bind(atoms)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().map(|(a, b)| atom_pair(a, b)).collect())
}

/// One decision to write: the superseder replaces the superseded fact.
struct Decision<'a> {
    superseder: &'a Fact,
    superseded: &'a Fact,
    superseder_key: &'a str,
    superseded_key: &'a str,
    rule: Rule,
    status: &'a str,
    signal: Option<Rule>,
    carried: Option<&'a Carried>,
}

/// Decide every new fact of one (subject, key) group against the pool, in
/// order, then write the decisions and their predecessor chains in a few
/// statements however many pairs there are. Ordering semantics match a
/// pair-at-a-time pass: the first pair to reach an atom pair decides it.
#[allow(clippy::too_many_arguments)]
async fn decide_group(
    rec: &Recorder,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    new_keyed: &[(String, &Fact)],
    pool: &[(String, Fact)],
    mode: Mode,
    pass: &mut Pass,
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    let Pass { pairs: judged, atoms: decided_atoms, carry } = pass;
    let mut pending: Vec<(&Fact, &str, &Fact, &str, Verdict)> = Vec::new();
    for (nk, n) in new_keyed {
        for (pk, p) in pool {
            if p.atom_id == n.atom_id && !changes::same_change(n, p) {
                continue;
            }
            let pair = if nk < pk { (nk.clone(), pk.clone()) } else { (pk.clone(), nk.clone()) };
            if !judged.insert(pair) {
                continue;
            }
            let verdict = decide(n, p);
            if verdict != Verdict::Independent {
                pending.push((n, nk.as_str(), p, pk.as_str(), verdict));
            }
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let related = if pending.iter().any(|x| matches!(x.4, Verdict::Ambiguous { .. })) {
        let atoms = sorted(new_keyed.iter().map(|(_, f)| f.atom_id).collect());
        related_atom_pairs(tx, graph_id, &atoms).await?
    } else {
        HashSet::new()
    };
    let pool_facts: Vec<Fact> = pool.iter().map(|(_, f)| f.clone()).collect();
    let non_pairs = non_supersession_pairs(tx, &pool_facts).await?;
    let names = value_names(tx, &pool_facts).await?;
    let mut decisions: Vec<Decision> = Vec::new();
    for (a, ak, b, bk, verdict) in pending {
        if values_name_one_entity(&names, a.value_node, b.value_node) {
            continue;
        }
        let pick = |s: Side| if s == Side::A { (a, ak) } else { (b, bk) };
        let pair = atom_pair(a.atom_id, b.atom_id);
        let decision = match verdict {
            Verdict::Independent => continue,
            Verdict::Supersede { superseder, rule } => {
                decided_atoms.insert(pair);
                let ((y, yk), (x, xk)) = (pick(superseder), pick(other(superseder)));
                let status = if values_are_non_supersession_pair(&non_pairs, x.value_node, y.value_node) {
                    "blocked"
                } else if mode == Mode::Live {
                    "active"
                } else {
                    "proposed"
                };
                Decision { superseder: y, superseded: x, superseder_key: yk, superseded_key: xk, rule, status, signal: None, carried: None }
            }
            Verdict::Ambiguous { newer, signal } => {
                if decided_atoms.contains(&pair) || related.contains(&pair) {
                    continue;
                }
                decided_atoms.insert(pair);
                let ((y, yk), (x, xk)) = (pick(newer), pick(other(newer)));
                let mut carried = None;
                let status = if values_are_non_supersession_pair(&non_pairs, x.value_node, y.value_node) {
                    "blocked"
                } else if let Some(c) = carry.get(&(y.state_key.clone(), y.atom_id, x.atom_id)) {
                    carried = Some(c);
                    c.status.as_str()
                } else {
                    "ambiguous"
                };
                Decision { superseder: y, superseded: x, superseder_key: yk, superseded_key: xk, rule: Rule::Judge, status, signal: Some(signal), carried }
            }
        };
        decisions.push(decision);
    }
    if decisions.is_empty() {
        return Ok(());
    }
    // (status before, status now) per decision.
    let outcomes = if rec.write { upsert_decisions(tx, graph_id, &decisions, mode).await? } else { preview_decisions(tx, graph_id, &decisions).await? };
    let mut chains: Vec<(&Fact, &Fact)> = Vec::new();
    for (d, (before, now)) in decisions.iter().zip(outcomes) {
        let promoted = before.as_deref() == Some("proposed") && now == "active";
        let status_now = if rec.write {
            let inserted = before.is_none();
            report.promoted += u64::from(promoted);
            report.carried += u64::from(inserted && d.carried.is_some());
            if !inserted && !promoted {
                if now == "active" && mode == Mode::Live {
                    chains.push((d.superseder, d.superseded));
                }
                continue;
            }
            now
        } else {
            match before.as_deref() {
                Some("proposed") if d.status == "active" => {
                    report.promoted += 1;
                    "active".to_owned()
                }
                Some(_) => continue,
                None => d.status.to_owned(),
            }
        };
        *report.by_rule.entry(d.rule.as_str().to_owned()).or_default() += 1;
        match status_now.as_str() {
            // A carried judge outcome can be `active` outside live mode;
            // only live mode chains.
            "active" if mode != Mode::Live => report.active += 1,
            "active" => {
                report.active += 1;
                if rec.write {
                    chains.push((d.superseder, d.superseded));
                } else if d.superseder.valid_from.is_some() && d.superseder.linked && d.superseded.linked
                    && !one_atom(d.superseder, d.superseded)
                {
                    report.transitions += 1;
                }
            }
            "proposed" => report.proposed += 1,
            "ambiguous" => report.ambiguous += 1,
            "blocked" => report.blocked += 1,
            _ => {}
        }
    }
    if !chains.is_empty() {
        write_chains(rec, tx, graph_id, &chains, report).await?;
    }
    Ok(())
}

/// Upsert decisions in one statement. A `proposed` row is promoted to
/// `active` in live mode; every other existing status (a judge or human
/// outcome) is kept. Returns (status before, status now) per decision.
async fn upsert_decisions(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    decisions: &[Decision<'_>],
    mode: Mode,
) -> Result<Vec<(Option<String>, String)>, sqlx::Error> {
    let col = |f: fn(&Decision) -> String| decisions.iter().map(f).collect::<Vec<String>>();
    let provenance: Vec<serde_json::Value> = decisions
        .iter()
        .map(|d| {
            let mut prov = provenance(d.superseder, d.superseded, d.rule, mode);
            if let Some(signal) = d.signal {
                prov["signal"] = json!(signal.as_str());
            }
            if let Some(c) = d.carried {
                prov["carried_from_edge_id"] = json!(c.edge_id);
            }
            prov
        })
        .collect();
    let rows: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
        "WITH input AS (SELECT * FROM unnest($2::uuid[],$3::text[],$4::uuid[],$5::uuid[],$6::text[],$7::text[],$8::text[],\
                $9::text[],$10::text[],$11::jsonb[],$12::uuid[],$13::int2[]) \
                AS t(subject,state_key,er_atom,ed_atom,er_fact,ed_fact,rule,kind,status,provenance,receipt,attempts)), \
              before AS (SELECT f.superseded_fact,f.superseder_fact,f.status FROM fact_supersession_edges f \
                JOIN input i ON f.graph_id=$1 AND f.superseded_fact=i.ed_fact AND f.superseder_fact=i.er_fact), \
              upserted AS (INSERT INTO fact_supersession_edges(graph_id,subject_node_id,state_key,superseder_atom_id,\
                superseded_atom_id,superseder_fact,superseded_fact,rule,kind,status,provenance,judge_receipt_id,judge_attempts) \
                SELECT $1,subject,state_key,er_atom,ed_atom,er_fact,ed_fact,rule,kind,status,provenance,receipt,COALESCE(attempts,0) FROM input \
                ON CONFLICT (graph_id,superseded_fact,superseder_fact) DO UPDATE SET \
                  status=CASE WHEN fact_supersession_edges.status='proposed' AND EXCLUDED.status='active' THEN 'active' \
                              ELSE fact_supersession_edges.status END, \
                  updated_at=CASE WHEN fact_supersession_edges.status='proposed' AND EXCLUDED.status='active' THEN now() \
                              ELSE fact_supersession_edges.updated_at END \
                RETURNING superseded_fact,superseder_fact,status) \
         SELECT u.superseded_fact,u.superseder_fact,b.status,u.status FROM upserted u \
           LEFT JOIN before b ON b.superseded_fact=u.superseded_fact AND b.superseder_fact=u.superseder_fact",
    )
    .bind(graph_id)
    .bind(decisions.iter().map(|d| d.superseder.subject).collect::<Vec<_>>())
    .bind(col(|d| d.superseder.state_key.clone()))
    .bind(decisions.iter().map(|d| d.superseder.atom_id).collect::<Vec<_>>())
    .bind(decisions.iter().map(|d| d.superseded.atom_id).collect::<Vec<_>>())
    .bind(col(|d| d.superseder_key.to_owned()))
    .bind(col(|d| d.superseded_key.to_owned()))
    .bind(col(|d| d.rule.as_str().to_owned()))
    .bind(col(|d| kind_of(d.superseder).to_owned()))
    .bind(col(|d| d.status.to_owned()))
    .bind(&provenance)
    .bind(decisions.iter().map(|d| d.carried.and_then(|c| c.judge_receipt_id)).collect::<Vec<_>>())
    .bind(decisions.iter().map(|d| d.carried.map(|c| c.judge_attempts)).collect::<Vec<_>>())
    .fetch_all(&mut **tx)
    .await?;
    let by_pair: HashMap<(String, String), (Option<String>, String)> =
        rows.into_iter().map(|(ed, er, before, now)| ((ed, er), (before, now))).collect();
    decisions
        .iter()
        .map(|d| {
            by_pair
                .get(&(d.superseded_key.to_owned(), d.superseder_key.to_owned()))
                .cloned()
                .ok_or_else(|| sqlx::Error::Protocol("fact supersession upsert returned no row for a decision".into()))
        })
        .collect()
}

/// The statuses already stored for these decisions (a read-only preview).
async fn preview_decisions(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    decisions: &[Decision<'_>],
) -> Result<Vec<(Option<String>, String)>, sqlx::Error> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT f.superseded_fact,f.superseder_fact,f.status FROM fact_supersession_edges f \
           JOIN unnest($2::text[],$3::text[]) AS i(ed,er) ON f.superseded_fact=i.ed AND f.superseder_fact=i.er \
          WHERE f.graph_id=$1",
    )
    .bind(graph_id)
    .bind(decisions.iter().map(|d| d.superseded_key.to_owned()).collect::<Vec<_>>())
    .bind(decisions.iter().map(|d| d.superseder_key.to_owned()).collect::<Vec<_>>())
    .fetch_all(&mut **tx)
    .await?;
    let stored: HashMap<(String, String), String> = rows.into_iter().map(|(ed, er, s)| ((ed, er), s)).collect();
    Ok(decisions
        .iter()
        .map(|d| (stored.get(&(d.superseded_key.to_owned(), d.superseder_key.to_owned())).cloned(), d.status.to_owned()))
        .collect())
}

/// Live replacements also end the replaced state in the temporal record:
/// a `transition` chained to the replaced fact's `assert` (see
/// [`chain_transition`]), for a whole group at once. Only when both facts
/// carry evidence and the replacement states its start.
async fn write_chains(
    rec: &Recorder,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    chains: &[(&Fact, &Fact)],
    report: &mut Report,
) -> Result<(), sqlx::Error> {
    let chains: Vec<&(&Fact, &Fact)> =
        chains.iter().filter(|(er, ed)| er.valid_from.is_some() && er.linked && ed.linked && !one_atom(er, ed)).collect();
    if chains.is_empty() {
        return Ok(());
    }
    // The replaced facts' asserts: the cache, else one read, else (an
    // end-only fact, whose withdraw must exist too, or evidence never
    // recorded) the per-fact path that writes what is missing.
    let mut asserts: HashMap<String, Uuid> = HashMap::new();
    let mut unknown: Vec<&Fact> = Vec::new();
    for (_, ed) in &chains {
        let key = ed.key();
        match rec.asserts.lock().expect("assert cache").get(&key) {
            Some(id) => {
                asserts.insert(key, *id);
            }
            None => unknown.push(ed),
        }
    }
    if !unknown.is_empty() {
        let rows: Vec<(Uuid, Uuid, String, Option<String>, Uuid)> = sqlx::query_as(
            "SELECT semantic_atom_id,subject_node_id,state_key,value,event_id FROM temporal_evidence_records \
              WHERE graph_id=$1 AND semantic_atom_id=ANY($2) AND subject_node_id=ANY($3) AND operation='assert' \
                AND predecessor_event_id IS NULL AND recording_method=$4 AND lifecycle='supported'",
        )
        .bind(graph_id)
        .bind(sorted(unknown.iter().map(|f| f.atom_id).collect()))
        .bind(sorted(unknown.iter().map(|f| f.subject).collect()))
        .bind(RECORDING_METHOD)
        .fetch_all(&mut **tx)
        .await?;
        let found: HashMap<(Uuid, Uuid, String, String), Uuid> = rows
            .into_iter()
            .filter_map(|(a, s, k, v, e)| v.map(|v| ((a, s, k, v), e)))
            .collect();
        for ed in unknown {
            let hit = found.get(&(ed.atom_id, ed.subject, ed.state_key.clone(), ed.value.clone())).copied();
            let end_only = ed.valid_from.is_none() && ed.valid_until.is_some();
            let id = match hit {
                Some(id) if !end_only => Some(id),
                _ => rec.evidence(tx, graph_id, ed, report).await?,
            };
            if let Some(id) = id {
                rec.asserts.lock().expect("assert cache").insert(ed.key(), id);
                asserts.insert(ed.key(), id);
            }
        }
    }
    let mut rows: Vec<(&Fact, Uuid)> = Vec::new();
    let mut seen = HashSet::new();
    for (er, ed) in &chains {
        if let Some(predecessor) = asserts.get(&ed.key()) {
            if seen.insert((er.key(), *predecessor)) {
                rows.push((er, *predecessor));
            }
        }
    }
    if rows.is_empty() {
        return Ok(());
    }
    let mut observations = Vec::with_capacity(rows.len());
    for (er, _) in &rows {
        observations.push(rec.observation(tx, graph_id, er).await?);
    }
    let quote = |f: &Fact| f.validity_quote.as_deref().map(|q| truncate(q, 4000)).filter(|q| !q.is_empty());
    let written: Vec<Uuid> = sqlx::query_scalar(
        "INSERT INTO temporal_evidence_records(graph_id,subject_node_id,state_key,source_item_id,source_version_id,\
            semantic_atom_id,passage_id,observation_id,operation,value,valid_from,valid_until,validity_quote,\
            predecessor_event_id,recording_method,recorded_by) \
         SELECT $1,t.subject,t.state_key,t.item,t.version,t.atom,t.passage,t.observation,'transition',t.value,t.valid_from,\
                t.valid_until,t.quote,t.predecessor,$14,$15 \
           FROM unnest($2::uuid[],$3::text[],$4::uuid[],$5::uuid[],$6::uuid[],$7::uuid[],$8::uuid[],$9::text[],\
                $10::timestamptz[],$11::timestamptz[],$12::text[],$13::uuid[]) \
                AS t(subject,state_key,item,version,atom,passage,observation,value,valid_from,valid_until,quote,predecessor) \
         ON CONFLICT DO NOTHING RETURNING event_id",
    )
    .bind(graph_id)
    .bind(rows.iter().map(|(f, _)| f.subject).collect::<Vec<_>>())
    .bind(rows.iter().map(|(f, _)| f.state_key.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|(f, _)| f.source_item_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|(f, _)| f.version_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|(f, _)| f.atom_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|(f, _)| f.passage_id).collect::<Vec<_>>())
    .bind(&observations)
    .bind(rows.iter().map(|(f, _)| f.value.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|(f, _)| f.valid_from).collect::<Vec<_>>())
    .bind(rows.iter().map(|(f, _)| f.valid_until).collect::<Vec<_>>())
    .bind(rows.iter().map(|(f, _)| quote(f)).collect::<Vec<_>>())
    .bind(rows.iter().map(|(_, p)| *p).collect::<Vec<_>>())
    .bind(RECORDING_METHOD)
    .bind(rec.actor)
    .fetch_all(&mut **tx)
    .await?;
    report.transitions += written.len() as u64;
    Ok(())
}

/// The extraction hook: on a version's last v2 window, inside the persist
/// transaction. Fail-open: an error rolls back only this step (a savepoint)
/// and is logged, so extraction never fails because of supersession.
pub(crate) async fn after_extraction_tx(tx: &mut Transaction<'_, Postgres>, graph_id: Uuid, version_id: Uuid) {
    let mode = Mode::from_env();
    if mode == Mode::Off {
        return;
    }
    let outcome = async {
        let mut savepoint = sqlx::Acquire::begin(&mut **tx).await?;
        match process_version_tx(&mut savepoint, graph_id, version_id, mode, true).await {
            Ok(report) => {
                savepoint.commit().await?;
                Ok(report)
            }
            Err(error) => {
                savepoint.rollback().await?;
                Err(error)
            }
        }
    }
    .await;
    match outcome {
        Ok(report) => {
            if report.facts > 0 {
                tracing::info!(%graph_id, %version_id, mode = mode.as_str(), facts = report.facts,
                    asserts = report.asserts, withdraws = report.withdraws, transitions = report.transitions,
                    proposed = report.proposed, active = report.active, ambiguous = report.ambiguous,
                    blocked = report.blocked, "fact_supersession_version_recorded");
            }
        }
        Err(error) => {
            tracing::warn!(%graph_id, %version_id, %error, "fact_supersession_version_failed_open");
        }
    }
}

// ── Reef merges ─────────────────────────────────────────────────────────────
//
// A merge moves the duplicate's `node_semantic_atoms` links and relationship
// edges onto the keeper, then deletes the duplicate, which cascades every
// temporal record and decision whose subject it was. Nothing in those rows
// can simply be re-pointed: temporal records are immutable (tenant 0144's
// integrity trigger), and a fact's key hashes its subject and, for a
// relation, the reef its value names, so both change with the merge. The
// merge also creates pairs that never met before (the copy's "X owns Apollo
// until March" and the keeper's "Z owns Apollo from April"), which only a
// fresh decision can see. So the facts the merge touched are re-derived from
// the canonical rows, in the merge transaction, and anything those rows no
// longer state is retired. The undo runs the same reconciliation over the
// restored rows.

/// Facts read per merge. A larger change is skipped (and logged) rather than
/// half-done; `backfill_fact_supersession --repair-merges` finishes it.
pub const RECONCILE_FACT_LIMIT: i64 = 4096;

/// What a merge or its undo changed.
#[derive(Clone, Debug, Default)]
pub struct Reconcile {
    /// Semantic atoms whose canonical support moved, plus every atom a moved
    /// relationship edge cites.
    pub atoms: Vec<Uuid>,
    /// Reefs whose facts about those atoms are re-derived: the keeper, the
    /// restored duplicate on undo, and every other endpoint of a moved edge
    /// (its relation now names a different reef).
    pub nodes: Vec<Uuid>,
    /// The reefs among `nodes` whose `node_semantic_atoms` links changed
    /// (keeper, duplicate). Only these get their atom facts (`fact.*`)
    /// re-derived; on the other endpoints only relations (`rel.*`) moved.
    pub linked: Vec<Uuid>,
    /// The duplicate a merge is about to delete: nothing is derived on it,
    /// and its judged decisions are carried to the re-derived ones.
    pub gone: Option<Uuid>,
}

/// The semantic atoms a relationship edge's v2 provenance cites.
pub(crate) fn cited_atoms(provenance: &serde_json::Value) -> Vec<Uuid> {
    provenance
        .get("assertions")
        .and_then(|a| a.as_array())
        .into_iter()
        .flatten()
        .filter_map(|x| x.get("semantic_atom_id").and_then(|v| v.as_str()).and_then(|v| Uuid::parse_str(v).ok()))
        .collect()
}

fn sorted(mut ids: Vec<Uuid>) -> Vec<Uuid> {
    ids.sort();
    ids.dedup();
    ids
}

/// Every current fact about `atoms` in scope: atom facts on each of
/// `linked` that supports the atom, and relation facts from every edge
/// touching one of `nodes` that cites one of the atoms. `None` when either
/// read reaches [`RECONCILE_FACT_LIMIT`] (the set would be incomplete).
async fn load_reconcile_facts(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    atoms: &[Uuid],
    nodes: &[Uuid],
    linked: &[Uuid],
    gone: Option<Uuid>,
) -> Result<Option<Vec<Fact>>, sqlx::Error> {
    let atom_sql = format!(
        "SELECT a.semantic_atom_id,a.source_version_id,v.source_item_id,l.node_id,a.kind,a.statement,\
                a.valid_from,a.valid_until,a.validity_quote,p.passage_id,p.quote,\
                COALESCE(v.occurred_at,v.captured_at) observed_at \
           FROM semantic_atoms a \
           JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id \
           JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id AND s.access_state='permitted' \
           JOIN node_semantic_atoms l ON l.graph_id=a.graph_id AND l.semantic_atom_id=a.semantic_atom_id AND l.node_id=ANY($4) \
           {PASSAGE_JOIN} \
          WHERE a.graph_id=$1 AND a.extraction_version=ANY($2) AND a.semantic_atom_id=ANY($3) \
            AND (a.valid_from IS NOT NULL OR a.valid_until IS NOT NULL) AND {STATED_SOURCE_SQL} \
          ORDER BY a.semantic_atom_id,l.node_id LIMIT $5"
    );
    let rows: Vec<AtomFactRow> = sqlx::query_as(&atom_sql)
        .bind(graph_id)
        .bind(STRUCTURED_VERSIONS)
        .bind(atoms)
        .bind(linked)
        .bind(RECONCILE_FACT_LIMIT)
        .fetch_all(&mut **tx)
        .await?;
    if rows.len() as i64 >= RECONCILE_FACT_LIMIT {
        return Ok(None);
    }
    let mut facts: Vec<Fact> = rows.into_iter().filter_map(atom_fact).collect();
    // Expand only the edges that cite one of the atoms (materialized first:
    // a keeper can carry thousands of edges, each a lateral expansion).
    let relation_sql = format!(
        "WITH e AS MATERIALIZED (SELECT e.* FROM relationship_edge e WHERE e.graph_id=$1 AND e.predicate=ANY($3) \
              AND (e.src_node_id=ANY($4) OR e.dst_node_id=ANY($4)) AND e.provenance->'assertions' @> ANY($7::jsonb[])) \
         SELECT a.semantic_atom_id,a.source_version_id,v.source_item_id,e.src_node_id,e.dst_node_id,e.predicate,\
                NULLIF(x->>'valid_from','')::timestamptz valid_from,NULLIF(x->>'valid_until','')::timestamptz valid_until,\
                NULLIF(x->>'validity_quote','') validity_quote,p.passage_id,p.quote,\
                COALESCE(v.occurred_at,v.captured_at) observed_at,src.title src_title,dst.title dst_title \
           FROM e \
           CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(e.provenance->'assertions')='array' \
                THEN e.provenance->'assertions' ELSE '[]'::jsonb END) x \
           JOIN semantic_atoms a ON a.graph_id=e.graph_id AND a.semantic_atom_id=(CASE WHEN x->>'semantic_atom_id' \
                ~ '^[0-9a-fA-F-]{{36}}$' THEN (x->>'semantic_atom_id')::uuid END) \
           JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id \
           JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id AND s.access_state='permitted' \
           JOIN nodes src ON src.node_id=e.src_node_id AND src.graph_id=e.graph_id \
           JOIN nodes dst ON dst.node_id=e.dst_node_id AND dst.graph_id=e.graph_id \
           {PASSAGE_JOIN} \
          WHERE a.extraction_version=ANY($2) AND a.semantic_atom_id=ANY($5) AND {STATED_SOURCE_SQL} \
          ORDER BY a.semantic_atom_id,e.edge_id LIMIT $6"
    );
    // v2 writes each cited id as `Uuid::to_string()`.
    let cites: Vec<serde_json::Value> = atoms.iter().map(|a| json!([{"semantic_atom_id": a.to_string()}])).collect();
    let rows: Vec<RelationFactRow> = sqlx::query_as(&relation_sql)
        .bind(graph_id)
        .bind(STRUCTURED_VERSIONS)
        .bind(KEYED_PREDICATES)
        .bind(nodes)
        .bind(atoms)
        .bind(RECONCILE_FACT_LIMIT)
        .bind(&cites)
        .fetch_all(&mut **tx)
        .await?;
    if rows.len() as i64 >= RECONCILE_FACT_LIMIT {
        return Ok(None);
    }
    facts.extend(rows.into_iter().filter_map(relation_fact).filter(|f| nodes.contains(&f.subject)));
    mark_linked(tx, graph_id, &mut facts).await?;
    // Stated changes about the atoms, on the reefs re-derived (subjects
    // resolved through the merge being made), first for the dedupe.
    let Some(stated) = changes::load_for_atoms(tx, graph_id, atoms, gone).await? else {
        return Ok(None);
    };
    let stated = stated.into_iter().filter(|f| nodes.contains(&f.subject) && in_scope(linked, f.subject, &f.state_key));
    let mut facts: Vec<Fact> = stated.chain(facts).collect();
    let mut seen = HashSet::new();
    facts.retain(|f| seen.insert(f.key()));
    Ok(Some(facts))
}

/// Is a stored row about (subject, key) within what was re-derived? Atom
/// facts only on `linked`; relations on every node.
fn in_scope(linked: &[Uuid], subject: Uuid, key: &str) -> bool {
    key.starts_with("rel.") || linked.binary_search(&subject).is_ok()
}

/// Does a current fact still state this extraction record?
fn record_is_current(facts: &[Fact], subject: Uuid, key: &str, atom: Uuid, operation: &str, value: Option<&str>) -> bool {
    facts.iter().any(|f| {
        f.linked
            && f.subject == subject
            && f.atom_id == atom
            && f.state_key == key
            && match operation {
                "assert" => value == Some(f.value.as_str()),
                "transition" => value == Some(f.value.as_str()) && f.valid_from.is_some(),
                "withdraw" => f.valid_from.is_none() && f.valid_until.is_some(),
                // Extraction never corrects; leave anything else alone.
                _ => true,
            }
    })
}

/// Retire the extraction records on `nodes` about `atoms` that no current
/// fact states (the relation now names another reef, or the subject no
/// longer supports the atom), and every extraction record chained to one.
/// Retired, not deleted: an answer receipt that cited one stops verifying,
/// exactly as when its source is retracted. Only permitted sources are
/// considered: a restricted source's records wait for its access to return.
async fn retract_stale_records(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    nodes: &[Uuid],
    linked: &[Uuid],
    atoms: &[Uuid],
    facts: &[Fact],
    write: bool,
) -> Result<u64, sqlx::Error> {
    let rows: Vec<(Uuid, Uuid, String, Uuid, String, Option<String>)> = sqlx::query_as(
        "SELECT e.event_id,e.subject_node_id,e.state_key,e.semantic_atom_id,e.operation,e.value \
           FROM temporal_evidence_records e \
           JOIN source_items s ON s.source_item_id=e.source_item_id AND s.graph_id=e.graph_id AND s.access_state='permitted' \
          WHERE e.graph_id=$1 AND e.semantic_atom_id=ANY($2) AND e.subject_node_id=ANY($3) \
            AND e.recording_method=$4 AND e.lifecycle='supported'",
    )
    .bind(graph_id)
    .bind(atoms)
    .bind(nodes)
    .bind(RECORDING_METHOD)
    .fetch_all(&mut **tx)
    .await?;
    let stale: Vec<Uuid> = rows
        .iter()
        .filter(|(_, subject, key, atom, op, value)| {
            in_scope(linked, *subject, key) && !record_is_current(facts, *subject, key, *atom, op, value.as_deref())
        })
        .map(|r| r.0)
        .collect();
    if stale.is_empty() {
        return Ok(0);
    }
    // A predecessor is always on the same subject and key (the integrity
    // trigger), so the chain search stays on `nodes`' index range.
    let chain = "WITH RECURSIVE r(id) AS (SELECT unnest($2::uuid[]) \
                   UNION SELECT e.event_id FROM temporal_evidence_records e JOIN r ON e.predecessor_event_id=r.id \
                    WHERE e.graph_id=$1 AND e.subject_node_id=ANY($3) AND e.recording_method=$4 AND e.lifecycle='supported')";
    let sql = if write {
        format!(
            "{chain}, u AS (UPDATE temporal_evidence_records t SET lifecycle='evidence_retracted' FROM r \
               WHERE t.event_id=r.id AND t.graph_id=$1 AND t.recording_method=$4 AND t.lifecycle='supported' RETURNING 1) \
             SELECT count(*) FROM u"
        )
    } else {
        format!(
            "{chain} SELECT count(*) FROM temporal_evidence_records t JOIN r ON t.event_id=r.id \
              WHERE t.graph_id=$1 AND t.recording_method=$4 AND t.lifecycle='supported'"
        )
    };
    let n: i64 = sqlx::query_scalar(&sql)
    .bind(graph_id)
    .bind(&stale)
    .bind(nodes)
    .bind(RECORDING_METHOD)
    .fetch_one(&mut **tx)
    .await?;
    Ok(n as u64)
}

#[derive(sqlx::FromRow)]
struct DecisionRow {
    edge_id: Uuid,
    subject_node_id: Uuid,
    state_key: String,
    superseder_atom_id: Uuid,
    superseded_atom_id: Uuid,
    superseder_fact: String,
    superseded_fact: String,
    rule: String,
    status: String,
    judge_receipt_id: Option<Uuid>,
    judge_attempts: i16,
}

/// Remove the decisions on `nodes` (and on the deleted duplicate) about a
/// fact over `atoms` that no longer exists, keeping their judged outcomes
/// for the re-derived pair. Deterministic decisions are simply re-decided.
#[allow(clippy::too_many_arguments)]
async fn remove_stale_decisions(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    nodes: &[Uuid],
    linked: &[Uuid],
    gone: Option<Uuid>,
    atoms: &[Uuid],
    facts: &[Fact],
    write: bool,
    report: &mut Report,
) -> Result<HashMap<(String, Uuid, Uuid), Carried>, sqlx::Error> {
    let subjects: Vec<Uuid> = nodes.iter().copied().chain(gone).collect();
    let permitted = |side: &str| {
        format!(
            "EXISTS(SELECT 1 FROM semantic_atoms a JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id \
               JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id AND s.access_state='permitted' \
              WHERE a.graph_id=f.graph_id AND a.semantic_atom_id=f.{side})"
        )
    };
    let rows: Vec<DecisionRow> = sqlx::query_as(&format!(
        "SELECT f.edge_id,f.subject_node_id,f.state_key,f.superseder_atom_id,f.superseded_atom_id,f.superseder_fact,\
                f.superseded_fact,f.rule,f.status,f.judge_receipt_id,f.judge_attempts \
           FROM fact_supersession_edges f \
          WHERE f.graph_id=$1 AND f.subject_node_id=ANY($2) \
            AND (f.superseder_atom_id=ANY($3) OR f.superseded_atom_id=ANY($3)) AND {} AND {}",
        permitted("superseder_atom_id"),
        permitted("superseded_atom_id")
    ))
    .bind(graph_id)
    .bind(&subjects)
    .bind(atoms)
    .fetch_all(&mut **tx)
    .await?;
    let keys: HashSet<String> = facts.iter().map(Fact::key).collect();
    let side_is_stale = |atom: Uuid, key: &str| atoms.binary_search(&atom).is_ok() && !keys.contains(key);
    let mut carry = HashMap::new();
    let mut stale = Vec::new();
    for r in rows {
        let gone_subject = Some(r.subject_node_id) == gone;
        if !gone_subject && !in_scope(linked, r.subject_node_id, &r.state_key) {
            continue;
        }
        if !gone_subject && !side_is_stale(r.superseder_atom_id, &r.superseder_fact) && !side_is_stale(r.superseded_atom_id, &r.superseded_fact) {
            continue;
        }
        if r.rule == Rule::Judge.as_str() && matches!(r.status.as_str(), "active" | "rejected" | "curator" | "unresolved") {
            carry.insert(
                (r.state_key.clone(), r.superseder_atom_id, r.superseded_atom_id),
                Carried { edge_id: r.edge_id, status: r.status.clone(), judge_receipt_id: r.judge_receipt_id, judge_attempts: r.judge_attempts },
            );
        }
        stale.push(r.edge_id);
    }
    if write && !stale.is_empty() {
        sqlx::query("DELETE FROM fact_supersession_edges WHERE graph_id=$1 AND edge_id=ANY($2)")
            .bind(graph_id)
            .bind(&stale)
            .execute(&mut **tx)
            .await?;
    }
    report.decisions_removed += stale.len() as u64;
    Ok(carry)
}

/// Re-derive temporal evidence and decisions after a merge or its undo.
/// `write` false computes the same report inside a read-only transaction.
pub async fn reconcile_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    change: &Reconcile,
    mode: Mode,
    write: bool,
) -> Result<Report, sqlx::Error> {
    reconcile_in_pass(tx, graph_id, change, mode, write, &mut Pass::default()).await
}

async fn reconcile_in_pass(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    change: &Reconcile,
    mode: Mode,
    write: bool,
    pass: &mut Pass,
) -> Result<Report, sqlx::Error> {
    let mut report = Report::default();
    let atoms = sorted(change.atoms.clone());
    let nodes = sorted(change.nodes.iter().chain(&change.linked).copied().filter(|n| Some(*n) != change.gone).collect());
    let linked = sorted(change.linked.iter().copied().filter(|n| Some(*n) != change.gone).collect());
    if mode == Mode::Off || atoms.is_empty() || nodes.is_empty() {
        return Ok(report);
    }
    let Some(facts) = load_reconcile_facts(tx, graph_id, &atoms, &nodes, &linked, change.gone).await? else {
        report.skipped = 1;
        return Ok(report);
    };
    let (actor, previous) = if write { recording_actor(tx, graph_id).await? } else { (Uuid::nil(), None) };
    let rec = Recorder::new(actor, write);
    let result = async {
        report.retracted = retract_stale_records(tx, graph_id, &nodes, &linked, &atoms, &facts, write).await?;
        pass.carry = remove_stale_decisions(tx, graph_id, &nodes, &linked, change.gone, &atoms, &facts, write, &mut report).await?;
        let derived = derive(&rec, tx, graph_id, &facts, mode, pass, &mut report).await;
        pass.carry.clear();
        derived
    }
    .await;
    if write {
        restore_actor(tx, previous).await?;
    }
    result?;
    Ok(report)
}

/// The merge hook: inside `merge_nodes_impl` (before the duplicate is
/// deleted) and the undo, in the caller's transaction. Fail-open like the
/// extraction hook: an error rolls back only this step (a savepoint) and is
/// logged, so a merge never fails because of supersession; the repair
/// backfill restores what it missed.
pub(crate) async fn after_merge_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    change: &Reconcile,
    event: &'static str,
) {
    let mode = Mode::from_env();
    if mode == Mode::Off {
        return;
    }
    let started = std::time::Instant::now();
    let outcome = async {
        let mut savepoint = sqlx::Acquire::begin(&mut **tx).await?;
        match reconcile_tx(&mut savepoint, graph_id, change, mode, true).await {
            Ok(report) => {
                savepoint.commit().await?;
                Ok(report)
            }
            Err(error) => {
                savepoint.rollback().await?;
                Err(error)
            }
        }
    }
    .await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    match outcome {
        Ok(report) if report.skipped > 0 => {
            tracing::error!(%graph_id, event, atoms = change.atoms.len(), nodes = change.nodes.len(),
                "fact_supersession_merge_skipped_too_large");
        }
        Ok(report) => {
            if report.facts > 0 || report.retracted > 0 || report.decisions_removed > 0 {
                tracing::info!(%graph_id, event, mode = mode.as_str(), elapsed_ms, facts = report.facts,
                    asserts = report.asserts, withdraws = report.withdraws, transitions = report.transitions,
                    retracted = report.retracted, decisions_removed = report.decisions_removed,
                    carried = report.carried, proposed = report.proposed, active = report.active,
                    ambiguous = report.ambiguous, "fact_supersession_merge_reconciled");
            }
        }
        Err(error) => {
            tracing::error!(%graph_id, event, %error, "fact_supersession_merge_failed_open");
        }
    }
}

// ── Backfill ────────────────────────────────────────────────────────────────

/// One bounded backfill pass over one database (operator bin
/// `backfill_fact_supersession`). Versions are taken oldest first so the
/// predecessor chains form in valid order; each runs in its own transaction.
/// `execute` false runs every version inside `BEGIN READ ONLY` and writes
/// nothing. Idempotent: a second pass writes nothing new.
#[derive(Debug, Clone)]
pub struct BackfillArgs {
    pub execute: bool,
    pub mode: Mode,
    pub graph_id: Option<Uuid>,
    /// Resume after this version (the last one a previous pass printed).
    pub after: Option<Uuid>,
    pub limit: i64,
}

pub const BACKFILL_DEFAULT_LIMIT: i64 = 2_000;

#[derive(Debug, Default, Clone)]
pub struct BackfillOutcome {
    pub report: Report,
    pub candidates: i64,
    pub failed: u64,
    pub last_version: Option<Uuid>,
}

pub async fn backfill(pool: &sqlx::PgPool, args: &BackfillArgs) -> Result<BackfillOutcome, String> {
    let candidates: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT a.source_version_id) FROM semantic_atoms a WHERE a.extraction_version=ANY($1) \
           AND ($2::uuid IS NULL OR a.graph_id=$2)",
    )
    .bind(STRUCTURED_VERSIONS)
    .bind(args.graph_id)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("count v2 versions: {e}"))?;
    let versions: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "WITH v2 AS (SELECT DISTINCT a.graph_id,a.source_version_id FROM semantic_atoms a \
                      WHERE a.extraction_version=ANY($1) AND ($2::uuid IS NULL OR a.graph_id=$2)), \
              cursor AS (SELECT v.captured_at,v.version_id FROM source_versions v WHERE v.version_id=$3) \
         SELECT v.graph_id,v.version_id FROM v2 JOIN source_versions v ON v.graph_id=v2.graph_id AND v.version_id=v2.source_version_id \
          WHERE $3::uuid IS NULL OR (v.captured_at,v.version_id)>(SELECT captured_at,version_id FROM cursor) \
          ORDER BY v.captured_at,v.version_id LIMIT $4",
    )
    .bind(STRUCTURED_VERSIONS)
    .bind(args.graph_id)
    .bind(args.after)
    .bind(args.limit)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("list v2 versions: {e}"))?;
    let mut out = BackfillOutcome { candidates, ..BackfillOutcome::default() };
    let mut pass = Pass::default();
    for (graph, version) in versions {
        let result = async {
            let mut tx = pool.begin().await?;
            if !args.execute {
                sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
            }
            let report = process_version_in_pass(&mut tx, graph, version, args.mode, args.execute, &mut pass).await?;
            if args.execute {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            Ok::<_, sqlx::Error>(report)
        }
        .await;
        match result {
            Ok(report) => out.report.add(&report),
            Err(error) => {
                out.failed += 1;
                eprintln!("  version {version} (graph {graph}) failed: {error}");
            }
        }
        out.last_version = Some(version);
    }
    Ok(out)
}

/// Why an undecided decision no longer stands under the current rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Ineligible {
    /// Either fact comes from captured code or a test file.
    CapturedCode,
    /// A pair rule on a key that holds several values at once.
    ManyValuedKey,
    /// The two values name one entity (a short label, a shared alias).
    SameEntity,
    /// Which side is newer was picked by a tied observation time.
    TiedObservation,
    /// The "newer" side's own quote puts its value in the past.
    ReversedReplacement,
}

impl Ineligible {
    pub fn as_str(self) -> &'static str {
        match self {
            Ineligible::CapturedCode => "captured_code",
            Ineligible::ManyValuedKey => "many_valued_key",
            Ineligible::SameEntity => "same_entity",
            Ineligible::TiedObservation => "tied_observation",
            Ineligible::ReversedReplacement => "reversed_replacement",
        }
    }
}

/// One stored decision as the prune reads it.
#[derive(Debug, Clone)]
pub struct StoredDecision {
    pub rule: String,
    pub state_key: String,
    /// Both atoms come from sources that state facts ([`STATED_SOURCE_SQL`]).
    pub stated_sources: bool,
    pub superseder_value: String,
    pub superseded_value: String,
    pub superseder_observed: Option<DateTime<Utc>>,
    pub superseded_observed: Option<DateTime<Utc>>,
    pub superseder_quote: String,
    /// Both values are reefs (relations), not free text.
    pub entity_values: bool,
    /// The two value reefs share a name or alias.
    pub values_share_a_name: bool,
}

/// The first current rule a stored decision fails, if any. Pure.
pub fn ineligible(d: &StoredDecision) -> Option<Ineligible> {
    const PAIR_RULES: [Rule; 4] = [Rule::ExplicitEnd, Rule::LaterStart, Rule::ReplacementLanguage, Rule::Judge];
    let pair_rule = PAIR_RULES.iter().any(|r| r.as_str() == d.rule);
    let newer_by_observation = d.rule == Rule::ReplacementLanguage.as_str() || d.rule == Rule::Judge.as_str();
    if !d.stated_sources {
        Some(Ineligible::CapturedCode)
    } else if pair_rule && !single_valued_key(&d.state_key) {
        Some(Ineligible::ManyValuedKey)
    } else if d.entity_values && (d.values_share_a_name || same_name(&d.superseder_value, &d.superseded_value)) {
        Some(Ineligible::SameEntity)
    } else if newer_by_observation && d.superseder_observed.is_some() && d.superseder_observed == d.superseded_observed {
        Some(Ineligible::TiedObservation)
    } else if d.rule == Rule::ReplacementLanguage.as_str() && names_as_past(&d.superseder_quote, &d.superseder_value) {
        Some(Ineligible::ReversedReplacement)
    } else {
        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct PruneReport {
    pub pruned: BTreeMap<&'static str, u64>,
    /// Every decision left, by (rule, status).
    pub remaining: BTreeMap<(String, String), u64>,
}

/// Undecided decisions (`ambiguous` or `proposed`, never judged) that the
/// current rules would not make: from captured code or test files, a pair
/// rule on a many-valued key, two names of one entity, a "newer" side
/// picked by a tied observation, or a replacement whose own quote names its
/// value as the past one. Left in place, live mode would promote them and
/// the judge would be paid to decide them. Judged, blocked and curated rows
/// are kept. `execute` false counts them in a read-only transaction.
pub async fn prune_ineligible(pool: &sqlx::PgPool, graph_id: Option<Uuid>, execute: bool) -> Result<PruneReport, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if !execute {
        sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
    }
    let stated = |side: &str| {
        format!(
            "EXISTS(SELECT 1 FROM semantic_atoms a JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id \
               JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id \
              WHERE a.graph_id=f.graph_id AND a.semantic_atom_id=f.{side} AND {STATED_SOURCE_SQL})"
        )
    };
    let names = |side: &str| {
        format!(
            "(SELECT array_agg(lower(btrim(x))) FROM nodes n CROSS JOIN LATERAL (SELECT n.title x UNION ALL \
               SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(n.body->'aliases')='array' THEN n.body->'aliases' ELSE '[]'::jsonb END)) t \
              WHERE n.graph_id=f.graph_id AND n.node_id=CASE WHEN f.provenance->'{side}'->>'value_node_id' ~ '^[0-9a-fA-F-]{{36}}$' \
                THEN (f.provenance->'{side}'->>'value_node_id')::uuid END)"
        )
    };
    type Row = (Uuid, String, String, String, bool, bool, String, String, Option<String>, Option<String>, Option<String>, bool);
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT f.edge_id,f.rule,f.status,f.state_key, \
                f.status IN ('ambiguous','proposed') AND f.judge_attempts=0 AND f.judge_receipt_id IS NULL, \
                {} AND {}, \
                COALESCE(f.provenance->'superseder'->>'value',''),COALESCE(f.provenance->'superseded'->>'value',''), \
                f.provenance->'superseder'->>'observed_at',f.provenance->'superseded'->>'observed_at', \
                (SELECT p.quote FROM semantic_atom_passages p WHERE p.graph_id=f.graph_id AND p.passage_id::text=f.provenance->'superseder'->>'passage_id'), \
                f.provenance->'superseder'->>'value_node_id' IS NOT NULL AND f.provenance->'superseded'->>'value_node_id' IS NOT NULL \
           FROM fact_supersession_edges f WHERE $1::uuid IS NULL OR f.graph_id=$1 ORDER BY f.edge_id",
        stated("superseder_atom_id"),
        stated("superseded_atom_id"),
    ))
    .bind(graph_id)
    .fetch_all(&mut *tx)
    .await?;
    let shared: HashSet<Uuid> = sqlx::query_scalar(&format!(
        "SELECT f.edge_id FROM fact_supersession_edges f WHERE ($1::uuid IS NULL OR f.graph_id=$1) AND {} && {}",
        names("superseder"),
        names("superseded"),
    ))
    .bind(graph_id)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .collect();
    let at = |v: Option<String>| v.and_then(|t| DateTime::parse_from_rfc3339(&t).ok()).map(|t| t.with_timezone(&Utc));
    let mut report = PruneReport::default();
    let mut doomed: Vec<Uuid> = Vec::new();
    for (edge, rule, status, state_key, undecided, stated_sources, er_value, ed_value, er_at, ed_at, quote, entity_values) in rows {
        let d = StoredDecision {
            rule: rule.clone(),
            state_key,
            stated_sources,
            superseder_value: er_value,
            superseded_value: ed_value,
            superseder_observed: at(er_at),
            superseded_observed: at(ed_at),
            superseder_quote: quote.unwrap_or_default(),
            entity_values,
            values_share_a_name: shared.contains(&edge),
        };
        match ineligible(&d).filter(|_| undecided) {
            Some(why) => {
                *report.pruned.entry(why.as_str()).or_default() += 1;
                doomed.push(edge);
            }
            None => *report.remaining.entry((rule, status)).or_default() += 1,
        }
    }
    if execute && !doomed.is_empty() {
        sqlx::query(
            "DELETE FROM fact_supersession_edges WHERE edge_id=ANY($1) AND status IN ('ambiguous','proposed') \
               AND judge_attempts=0 AND judge_receipt_id IS NULL",
        )
        .bind(&doomed)
        .execute(&mut *tx)
        .await?;
    }
    if execute {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(report)
}

/// What a past merge changed, from its undo row: the duplicate's semantic
/// atoms and every atom its edges cited; the keeper and each edge's other
/// endpoint. The duplicate itself is gone.
pub(crate) fn merge_change(keep: Uuid, dup: Uuid, dup_snapshot: &serde_json::Value, repointed_edges: &serde_json::Value) -> Reconcile {
    let ids = |key: &str| -> Vec<Uuid> {
        dup_snapshot.get(key).and_then(|v| v.as_array()).into_iter().flatten()
            .filter_map(|v| v.as_str().and_then(|v| Uuid::parse_str(v).ok())).collect()
    };
    let mut atoms = ids("semantic_atom_ids");
    atoms.extend(ids("moved_semantic_atom_ids"));
    let mut nodes = vec![keep, dup];
    for e in repointed_edges.as_array().into_iter().flatten() {
        if let Some(p) = e.get("provenance") {
            atoms.extend(cited_atoms(p));
        }
        for end in ["src_node_id", "dst_node_id"] {
            if let Some(n) = e.get(end).and_then(|v| v.as_str()).and_then(|v| Uuid::parse_str(v).ok()) {
                nodes.push(n);
            }
        }
    }
    Reconcile { atoms: sorted(atoms), nodes: sorted(nodes), linked: sorted(vec![keep, dup]), gone: None }
}

#[derive(Debug, Default, Clone)]
pub struct RepairOutcome {
    pub report: Report,
    /// Standing merges (not undone, keeper still present) in scope.
    pub candidates: i64,
    pub merges: u64,
    pub failed: u64,
    pub last_merge: Option<Uuid>,
}

/// Repair keepers that lost temporal evidence and decisions to merges made
/// before the merge hook existed (operator bin `backfill_fact_supersession
/// --repair-merges`). Each standing merge, oldest first, re-runs the merge
/// reconciliation over its undo row in its own transaction: the keeper gets
/// the duplicate's facts, relations that named the duplicate are re-recorded
/// on the reef they name now, and records no current fact states are
/// retired. A keeper merged away later is repaired through that later merge
/// (its undo row lists everything the keeper held). `execute` false runs each
/// inside `BEGIN READ ONLY`. Idempotent: a second pass writes nothing.
/// `args.after` resumes after that merge id.
pub async fn repair_merges(pool: &sqlx::PgPool, args: &BackfillArgs) -> Result<RepairOutcome, String> {
    let candidates: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM node_merges m JOIN nodes k ON k.node_id=m.keep_id AND k.graph_id=m.graph_id \
          WHERE m.undone_at IS NULL AND ($1::uuid IS NULL OR m.graph_id=$1)",
    )
    .bind(args.graph_id)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("count merges: {e}"))?;
    let merges: Vec<(Uuid, Uuid, Uuid, Uuid, serde_json::Value, serde_json::Value)> = sqlx::query_as(
        "WITH cursor AS (SELECT created_at,merge_id FROM node_merges WHERE merge_id=$2) \
         SELECT m.merge_id,m.graph_id,m.keep_id,m.dup_id,m.dup_snapshot,m.repointed_edges FROM node_merges m \
           JOIN nodes k ON k.node_id=m.keep_id AND k.graph_id=m.graph_id \
          WHERE m.undone_at IS NULL AND ($1::uuid IS NULL OR m.graph_id=$1) \
            AND ($2::uuid IS NULL OR (m.created_at,m.merge_id)>(SELECT created_at,merge_id FROM cursor)) \
          ORDER BY m.created_at,m.merge_id LIMIT $3",
    )
    .bind(args.graph_id)
    .bind(args.after)
    .bind(args.limit)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("list merges: {e}"))?;
    let mut out = RepairOutcome { candidates, ..RepairOutcome::default() };
    let mut pass = Pass::default();
    for (merge_id, graph, keep, dup, snapshot, edges) in merges {
        let change = merge_change(keep, dup, &snapshot, &edges);
        let result = async {
            let mut tx = pool.begin().await?;
            if !args.execute {
                sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
            }
            let report = reconcile_in_pass(&mut tx, graph, &change, args.mode, args.execute, &mut pass).await?;
            if args.execute {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            Ok::<_, sqlx::Error>(report)
        }
        .await;
        match result {
            Ok(report) => {
                out.merges += 1;
                out.report.add(&report);
            }
            Err(error) => {
                out.failed += 1;
                eprintln!("  merge {merge_id} (graph {graph}, keep {keep}) failed: {error}");
            }
        }
        out.last_merge = Some(merge_id);
    }
    Ok(out)
}

// ── Reads ───────────────────────────────────────────────────────────────────

/// Characters of the superseding fact's statement a read carries.
pub const SUPERSEDER_STATEMENT_CHARS: i32 = 280;

/// SQL that yields `'{}'` for a current outcome, else
/// `{"supersession":{"status":"superseded","superseded_by":[...]}}`, for the
/// semantic atom aliased `alias`. Only live, active decisions whose
/// superseding fact is still permitted count, and never a stated change's
/// own pair (the atom also states the current value). Concatenate onto an outcome
/// object with `||`.
///
/// Each `superseded_by` entry names the superseding fact: its `statement`
/// (first [`SUPERSEDER_STATEMENT_CHARS`] characters, `statement_truncated`),
/// `source_item_id`, `source_title`, and `occurred_at` / `captured_at` of its
/// source version. Permission: a reader sees exactly the semantic atoms whose
/// source item is `permitted`, in a graph RLS lets it read, and this query
/// runs under the reader's RLS and joins that same `access_state` gate. A
/// superseder the reader cannot read is therefore never disclosed at all, not
/// even as a marker (#1362): its text, id and existence stay hidden, and the
/// replaced fact reads as current. The successor may sit outside the
/// reader's selected scope; it is still a fact the reader may open.
pub(crate) fn outcome_supersession_sql(alias: &str) -> String {
    if !reads_live() {
        return "'{}'::jsonb".to_owned();
    }
    format!(
        "COALESCE((SELECT jsonb_build_object('supersession',jsonb_build_object('status','superseded',\
            'superseded_by',jsonb_agg(jsonb_build_object('semantic_atom_id',f.superseder_atom_id,'state_key',f.state_key,\
            'kind',f.kind,'rule',f.rule,\
            'statement',substring(f.statement FOR {SUPERSEDER_STATEMENT_CHARS}),'statement_truncated',char_length(f.statement)>{SUPERSEDER_STATEMENT_CHARS},\
            'source_item_id',f.source_item_id,'source_title',f.source_title,'occurred_at',f.occurred_at,'captured_at',f.captured_at) \
            ORDER BY f.created_at,f.edge_id))) \
          FROM (SELECT f.*,sa.statement,si.source_item_id,substring(NULLIF(sv.source_metadata->>'title','') FOR 256) source_title,\
                       sv.occurred_at,sv.captured_at FROM fact_supersession_edges f \
                 JOIN semantic_atoms sa ON sa.semantic_atom_id=f.superseder_atom_id AND sa.graph_id=f.graph_id \
                 JOIN source_versions sv ON sv.version_id=sa.source_version_id AND sv.graph_id=sa.graph_id \
                 JOIN source_items si ON si.source_item_id=sv.source_item_id AND si.graph_id=sv.graph_id AND si.access_state='permitted' \
                WHERE f.graph_id={alias}.graph_id AND f.superseded_atom_id={alias}.semantic_atom_id AND f.status='active' \
                  AND f.superseder_atom_id<>f.superseded_atom_id AND {floor} \
                ORDER BY f.created_at,f.edge_id LIMIT 8) f HAVING count(*)>0),'{{}}'::jsonb)",
        floor = crate::history_floor::admits_version("sv"),
    )
}

/// The superseding fact's display fields (statement, source, dates) are for
/// the reader, not the model: they carry no citation number, and the
/// successor may lie outside the selected scope. Ask removes them from its
/// prompt copy, leaving the marker and ids the system prompt describes.
pub(crate) fn strip_superseder_display(outcome: &mut serde_json::Value) {
    let Some(by) = outcome.pointer_mut("/supersession/superseded_by").and_then(|v| v.as_array_mut()) else {
        return;
    };
    for entry in by.iter_mut().filter_map(|e| e.as_object_mut()) {
        for field in ["statement", "statement_truncated", "source_item_id", "source_title", "occurred_at", "captured_at"] {
            entry.remove(field);
        }
    }
}

/// SQL boolean: the atom aliased `alias` is superseded (live reads only).
pub(crate) fn superseded_sql(alias: &str) -> String {
    if !reads_live() {
        return "false".to_owned();
    }
    format!(
        "EXISTS(SELECT 1 FROM fact_supersession_edges f \
           JOIN semantic_atoms sa ON sa.semantic_atom_id=f.superseder_atom_id AND sa.graph_id=f.graph_id \
           JOIN source_versions sv ON sv.version_id=sa.source_version_id AND sv.graph_id=sa.graph_id \
           JOIN source_items si ON si.source_item_id=sv.source_item_id AND si.graph_id=sv.graph_id AND si.access_state='permitted' \
          WHERE f.graph_id={alias}.graph_id AND f.superseded_atom_id={alias}.semantic_atom_id AND f.status='active' \
            AND f.superseder_atom_id<>f.superseded_atom_id AND {floor})",
        floor = crate::history_floor::admits_version("sv"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(day: &str) -> DateTime<Utc> {
        format!("{day}T00:00:00Z").parse().unwrap()
    }

    fn fact(atom: u128, key: &str, value: u128) -> Fact {
        Fact {
            atom_id: Uuid::from_u128(atom),
            version_id: Uuid::from_u128(atom + 100),
            source_item_id: Uuid::from_u128(atom + 200),
            subject: Uuid::from_u128(1),
            state_key: key.into(),
            value: format!("Person {value}"),
            value_node: Some(Uuid::from_u128(value + 1000)),
            valid_from: None,
            valid_until: None,
            validity_quote: None,
            passage_id: Uuid::from_u128(atom + 300),
            quote: "plain words".into(),
            observed_at: at("2026-01-01"),
            linked: true,
            change: None,
        }
    }

    #[test]
    fn prompt_copy_drops_only_the_superseder_display_fields() {
        let mut outcome = json!({"statement": "Xavier owns Apollo.", "supersession": {"status": "superseded", "superseded_by": [
            {"semantic_atom_id": "a", "state_key": "rel.owner", "kind": "change", "rule": "explicit_end",
             "statement": "Zoe owns Apollo.", "statement_truncated": false, "source_item_id": "s",
             "source_title": "Issue", "occurred_at": "2026-04-01T00:00:00+00:00", "captured_at": null}]}});
        strip_superseder_display(&mut outcome);
        assert_eq!(outcome, json!({"statement": "Xavier owns Apollo.", "supersession": {"status": "superseded", "superseded_by": [
            {"semantic_atom_id": "a", "state_key": "rel.owner", "kind": "change", "rule": "explicit_end"}]}}));
        let mut current = json!({"statement": "Zoe owns Apollo."});
        strip_superseder_display(&mut current);
        assert_eq!(current, json!({"statement": "Zoe owns Apollo."}));
    }

    #[test]
    fn mode_defaults_to_dry_run_and_parses_off_and_live() {
        for v in [None, Some(""), Some("dry_run"), Some("true"), Some("1"), Some("yes")] {
            assert_eq!(Mode::parse(v), Mode::DryRun, "{v:?}");
        }
        assert_eq!(Mode::parse(Some(" LIVE ")), Mode::Live);
        assert_eq!(Mode::parse(Some("off")), Mode::Off);
    }

    #[test]
    fn relation_keys_normalize_inverse_predicates_onto_one_subject() {
        let manages = relation_key("manages").unwrap();
        let reports = relation_key("reports_to").unwrap();
        assert_eq!((manages.key, manages.subject_is_dst), ("rel.manager", true));
        assert_eq!((reports.key, reports.subject_is_dst), ("rel.manager", false));
        assert!(manages.functional && relation_key("responsibility_of").unwrap().functional);
        assert!(!relation_key("works_on").unwrap().functional);
        assert!(relation_key("mentions").is_none() && relation_key("spouse_of").is_none());
        for p in KEYED_PREDICATES {
            assert!(predicates::extractable_predicates().contains(p), "{p} must be extractable");
            let key = relation_key(p).unwrap().key;
            assert!(key.len() <= 64 && key.starts_with("rel."));
        }
        assert_eq!(predicates_for_key("rel.owner"), vec!["responsible_for", "responsibility_of"]);
    }

    #[test]
    fn explicit_end_then_later_start_supersedes_on_a_single_valued_key() {
        let mut x = fact(1, "rel.owner", 1);
        x.valid_until = Some(at("2026-03-01"));
        let mut z = fact(2, "rel.owner", 2);
        z.valid_from = Some(at("2026-04-01"));
        assert_eq!(decide(&x, &z), Verdict::Supersede { superseder: Side::B, rule: Rule::ExplicitEnd });
        assert_eq!(decide(&z, &x), Verdict::Supersede { superseder: Side::A, rule: Rule::ExplicitEnd });
    }

    #[test]
    fn many_valued_keys_are_never_candidates() {
        for key in ["rel.works_on", "rel.member_of", "fact.decision", "fact.observation"] {
            let mut x = fact(1, key, 1);
            x.valid_until = Some(at("2026-03-01"));
            let mut z = fact(2, key, 2);
            z.valid_from = Some(at("2026-04-01"));
            z.observed_at = at("2026-04-02");
            z.quote = "Person 2 replaced Person 1.".into();
            assert_eq!(decide(&x, &z), Verdict::Independent, "{key}");
            assert_eq!(decide(&z, &x), Verdict::Independent, "{key}");
        }
    }

    #[test]
    fn single_valued_keys_are_the_functional_relations_and_attributes() {
        assert_eq!(single_valued_relation_keys(), ["rel.employer", "rel.lead", "rel.manager", "rel.owner"]);
        assert!(single_valued_key("attr.status") && single_valued_key("attr.deploy_trigger"));
        assert!(!single_valued_key("fact.decision") && !single_valued_key("rel.works_on") && !single_valued_key("rel.member_of"));
        for p in KEYED_PREDICATES {
            assert_eq!(single_valued_key(relation_key(p).unwrap().key), predicates::is_single_valued(p), "{p}");
        }
        assert!(!predicates::is_single_valued(predicates::MENTIONS) && !predicates::is_single_valued(predicates::WORKS_ON));
    }

    #[test]
    fn a_status_change_on_an_attribute_key_is_decided() {
        let mut open = fact(1, "attr.status", 1);
        open.value_node = None;
        open.value = "in review".into();
        open.valid_from = Some(at("2026-03-01"));
        let mut done = open.clone();
        done.atom_id = Uuid::from_u128(2);
        done.value = "merged".into();
        done.valid_from = Some(at("2026-03-05"));
        assert_eq!(decide(&open, &done), Verdict::Supersede { superseder: Side::B, rule: Rule::LaterStart });
    }

    #[test]
    fn later_start_wins_by_validity_not_capture_order() {
        let mut a = fact(1, "rel.manager", 1);
        a.valid_from = Some(at("2026-05-01"));
        a.observed_at = at("2026-01-01");
        let mut b = fact(2, "rel.manager", 2);
        b.valid_from = Some(at("2026-02-01"));
        b.observed_at = at("2026-06-01");
        assert_eq!(decide(&a, &b), Verdict::Supersede { superseder: Side::A, rule: Rule::LaterStart });
        b.valid_from = a.valid_from;
        assert_eq!(decide(&a, &b), Verdict::Independent, "equal starts stay disputed");
    }

    #[test]
    fn replacement_language_must_name_the_old_value_to_decide() {
        let old = fact(1, "rel.owner", 1);
        let mut new = fact(2, "rel.owner", 2);
        new.observed_at = at("2026-02-01");
        new.quote = "Person 2 took over Apollo from Person 1.".into();
        assert_eq!(decide(&old, &new), Verdict::Supersede { superseder: Side::B, rule: Rule::ReplacementLanguage });
        new.quote = "Person 2 took over Apollo.".into();
        assert_eq!(decide(&old, &new), Verdict::Ambiguous { newer: Side::B, signal: Rule::ReplacementLanguage });
    }

    #[test]
    fn undated_conflicts_same_values_and_unrelated_keys_are_independent() {
        let a = fact(1, "rel.owner", 1);
        let b = fact(2, "rel.owner", 2);
        assert_eq!(decide(&a, &b), Verdict::Independent, "recency never picks a winner");
        let same = fact(3, "rel.owner", 1);
        assert_eq!(decide(&a, &same), Verdict::Independent);
        let other_key = fact(4, "rel.manager", 2);
        assert_eq!(decide(&a, &other_key), Verdict::Independent);
        let mut dated = b.clone();
        dated.valid_from = Some(at("2026-04-01"));
        dated.observed_at = at("2026-04-02");
        assert_eq!(decide(&a, &dated), Verdict::Ambiguous { newer: Side::B, signal: Rule::LaterStart });
    }

    #[test]
    fn replacement_language_never_decides_on_a_tied_observation_or_a_reversed_quote() {
        let old = fact(1, "rel.owner", 1);
        let mut new = fact(2, "rel.owner", 2);
        new.quote = "Person 2 took over Apollo from Person 1.".into();
        assert_eq!(new.observed_at, old.observed_at);
        assert_eq!(decide(&old, &new), Verdict::Independent, "one capture time names no newer side");
        assert_eq!(decide(&new, &old), Verdict::Independent);
        new.observed_at = at("2026-02-01");
        new.quote = "Apollo is with Person 1 now; previously owned by Person 2.".into();
        assert_eq!(decide(&old, &new), Verdict::Independent, "the quote says its own value is the earlier one");
    }

    #[test]
    fn a_short_label_and_a_full_name_are_one_value() {
        assert!(same_name("Ana", "Ana Ruiz") && same_name("acme corp", "Acme") && same_name("Ana Ruiz", "ana ruiz"));
        assert!(!same_name("Ana Ruiz", "Ana Lopez") && !same_name("Ruiz", "Ana Ruiz") && !same_name("", "Ana"));
        let mut short = fact(1, "rel.owner", 1);
        short.value = "Ana".into();
        short.valid_until = Some(at("2026-03-01"));
        let mut full = fact(2, "rel.owner", 2);
        full.value = "Ana Ruiz".into();
        full.valid_from = Some(at("2026-04-01"));
        assert_eq!(decide(&short, &full), Verdict::Independent);
    }

    #[test]
    fn stored_decisions_fail_the_first_rule_they_break() {
        let d = StoredDecision {
            rule: "later_start".into(),
            state_key: "rel.owner".into(),
            stated_sources: true,
            superseder_value: "Zoe Park".into(),
            superseded_value: "Xavier Ortiz".into(),
            superseder_observed: Some(at("2026-04-01")),
            superseded_observed: Some(at("2026-02-01")),
            superseder_quote: "Zoe Park owns Apollo from 2026-04-01.".into(),
            entity_values: true,
            values_share_a_name: false,
        };
        assert_eq!(ineligible(&d), None);
        assert_eq!(ineligible(&StoredDecision { stated_sources: false, ..d.clone() }), Some(Ineligible::CapturedCode));
        assert_eq!(ineligible(&StoredDecision { state_key: "rel.works_on".into(), ..d.clone() }), Some(Ineligible::ManyValuedKey));
        assert_eq!(ineligible(&StoredDecision { rule: "stated_change".into(), state_key: "rel.works_on".into(), ..d.clone() }), None);
        assert_eq!(ineligible(&StoredDecision { superseded_value: "Zoe".into(), ..d.clone() }), Some(Ineligible::SameEntity));
        assert_eq!(ineligible(&StoredDecision { values_share_a_name: true, ..d.clone() }), Some(Ineligible::SameEntity));
        let tied = StoredDecision { rule: "judge".into(), superseded_observed: d.superseder_observed, ..d.clone() };
        assert_eq!(ineligible(&tied), Some(Ineligible::TiedObservation));
        assert_eq!(ineligible(&StoredDecision { rule: "later_start".into(), ..tied }), None, "dated rules do not read the observation");
        let reversed = StoredDecision { rule: "replacement_language".into(), superseder_quote: "Formerly owned by Zoe Park.".into(), ..d };
        assert_eq!(ineligible(&reversed), Some(Ineligible::ReversedReplacement));
    }

    #[test]
    fn fact_key_is_stable_and_distinguishes_values() {
        let a = fact(1, "rel.owner", 1);
        assert_eq!(a.key(), a.clone().key());
        assert_eq!(a.key().len(), 32);
        assert_ne!(a.key(), fact(1, "rel.owner", 2).key());
    }
}
