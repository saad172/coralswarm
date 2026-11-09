//! Chat "find" source result tiles (`source_results` on the scoped answer) and
//! the hidden title-only search (`GET /v1/find/titles`).
//!
//! Finding a specific source ("Find the Acme pricing meeting from two weeks
//! ago") is answered in chat with a one-sentence reply plus up to
//! [`MAX_TILES`] tiles, one per SOURCE (a meeting, a Slack thread, an agent or
//! doc session, a note), never one per atom or passage. This module is the
//! whole read: grouping, ranking, enrichment and the title search. It is an
//! in-process module, not a service, so the HTTP chat path and an MCP tool
//! share it; `routes::find_results` only authorizes and maps answers.
//! Contract: `docs/implementation/find-results-api.md`; validation:
//! `.../find-results-validation.md`.
//!
//! # Deterministic derivation (no model call)
//!
//! The tiles are derived from the answer's own evidence, after the answer is
//! built; answer synthesis and prompting are untouched:
//!
//! 1. Candidates: every citation in `retained_citations` (falling back to
//!    `outcomes[].citations`), plus each `legacy_evidence` chunk (resolved to
//!    its source by dropping the `:chunk:N` suffix of `atoms.source_id`).
//! 2. Group by source item (`source_item_id`), never by passage.
//! 3. Rank: highest citation `score` (absent = 0), then earliest first
//!    appearance (retrieval order), then most citations, then item id.
//! 4. Take the top [`MAX_TILES`] that are readable and have a kind; rank 1 is
//!    `best: true`.
//! 5. Enrich from the source's current version: kind, title, date, recorded
//!    duration (meetings), people, ocean name, outcome (meeting summary
//!    overview's first sentence, else the first decision, else `null`),
//!    decisions, reefs and up to [`QUOTES_PER_TILE`] quoted passages.
//!
//! # Kind mapping (`Kind::from_source_type`)
//!
//! Mirrors the table in `services::journal_activities` (PR #1611, not merged
//! when this was written; keep the two in step). Extra meeting types are the
//! ones `library_read`'s `meetings` collection already treats as meetings.
//!
//! | kind      | `source_items.source_type` (or `origin.content_kind`)                  |
//! |-----------|------------------------------------------------------------------------|
//! | `meeting` | `meeting`, `meeting_session`, `transcript`, `zoom`, `meet`, `teams_transcript`, `zoom_transcript`, `google_meet_transcript`, `slack_huddle_summary` |
//! | `slack`   | `slack`, `teams_chat`, `teams_channel`, `chat`, `google_chat` (label "slack thread") |
//! | `agent`   | `coding_session`, `agent_session`, `mcp`                               |
//! | `doc`     | `document`, `gdrive`, `notion`, `work_session`                         |
//! | `note`    | `note`                                                                 |
//!
//! Every other source type (mail, GitHub, calendar, ...) has no tile.
//!
//! # Access
//!
//! The caller passes nothing but its own request: the oceans it may read come
//! from `ReachabilityResolver` (ADR-0007 D9: its own private ocean, shared and
//! team oceans it belongs to; never another member's private ocean, never an
//! archived one), every statement runs in one `Tenant::begin_member_scoped`
//! transaction (RLS on the tenant plane) and applies `access_state =
//! 'permitted'` plus the team-seat history floor (`history_floor::admits_*`).
//! Citations naming an ocean the caller cannot read are dropped, so a tile can
//! never widen what the answer was allowed to cite.
//!
//! # Flag
//!
//! `CHAT_FIND_RESULTS_ENABLED` (unset = off): no `source_results` field is
//! emitted and `GET /v1/find/titles` answers 404.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    tenancy::{extractor::Tenant, reachability::ReachabilityResolver},
};

// ── Flag and limits ──────────────────────────────────────────────────────

pub const FLAG: &str = "CHAT_FIND_RESULTS_ENABLED";

/// `CHAT_FIND_RESULTS_ENABLED` is `1`/`true`. Unset = off.
pub fn enabled() -> bool {
    std::env::var(FLAG)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Tiles one answer carries.
pub const MAX_TILES: usize = 3;
/// Ranked sources hydrated before the readable top [`MAX_TILES`] are taken
/// (some may be unreadable, unmapped or gone).
const HYDRATE_CANDIDATES: usize = 12;
pub const QUOTES_PER_TILE: usize = 3;
pub const TILE_PEOPLE: usize = 8;
pub const TILE_DECISIONS: usize = 5;
pub const TILE_REEFS: usize = 6;
pub const DEFAULT_TITLE_LIMIT: i64 = 10;
pub const MAX_TITLE_LIMIT: i64 = 25;
pub const MAX_QUERY_CHARS: usize = 200;
const OUTCOME_MAX_CHARS: usize = 160;
const QUOTE_MAX_CHARS: usize = 280;
/// Citations / legacy chunks read from one answer.
const MAX_CANDIDATES: usize = 2000;
/// Legacy atoms read per source for its people (a transcript has hundreds).
const PEOPLE_ATOMS_PER_ITEM: i32 = 400;
/// The whole enrichment (reachability and every statement) must finish in this.
const ENRICH_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
const STATEMENT_TIMEOUT: &str = "SELECT set_config('statement_timeout','5s',true)";

// ── Kinds ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Meeting,
    Slack,
    Agent,
    Doc,
    Note,
}

const MEETING_TYPES: &[&str] = &[
    "meeting",
    "meeting_session",
    "transcript",
    "zoom",
    "meet",
    "teams_transcript",
    "zoom_transcript",
    "google_meet_transcript",
    "slack_huddle_summary",
];
const SLACK_TYPES: &[&str] = &[
    "slack",
    "teams_chat",
    "teams_channel",
    "chat",
    "google_chat",
];
const AGENT_TYPES: &[&str] = &["coding_session", "agent_session", "mcp"];
const DOC_TYPES: &[&str] = &["document", "gdrive", "notion", "work_session"];
const NOTE_TYPES: &[&str] = &["note"];

impl Kind {
    pub const ALL: [Kind; 5] = [
        Kind::Meeting,
        Kind::Slack,
        Kind::Agent,
        Kind::Doc,
        Kind::Note,
    ];

    pub fn source_types(self) -> &'static [&'static str] {
        match self {
            Kind::Meeting => MEETING_TYPES,
            Kind::Slack => SLACK_TYPES,
            Kind::Agent => AGENT_TYPES,
            Kind::Doc => DOC_TYPES,
            Kind::Note => NOTE_TYPES,
        }
    }

    pub fn from_source_type(source_type: &str) -> Option<Kind> {
        Kind::ALL
            .into_iter()
            .find(|k| k.source_types().contains(&source_type))
    }

    /// Every source type that has a tile.
    pub fn all_source_types() -> Vec<String> {
        Kind::ALL
            .iter()
            .flat_map(|k| k.source_types().iter().map(|s| s.to_string()))
            .collect()
    }

    /// The design's label ("slack thread", "agent session", ...).
    pub fn label(self) -> &'static str {
        match self {
            Kind::Meeting => "meeting",
            Kind::Slack => "slack thread",
            Kind::Agent => "agent session",
            Kind::Doc => "doc session",
            Kind::Note => "note",
        }
    }

    fn default_title(self) -> &'static str {
        match self {
            Kind::Meeting => "Meeting",
            Kind::Slack => "Slack thread",
            Kind::Agent => "Agent session",
            Kind::Doc => "Doc session",
            Kind::Note => "Note",
        }
    }
}

// ── Response types ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GraphRef {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Topic {
    pub node_id: Uuid,
    pub title: String,
}

/// A passage the answer quoted from the source.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Quote {
    pub quote: String,
    pub passage_id: Option<Uuid>,
    pub occurred_at: Option<DateTime<Utc>>,
}

/// One source result tile (and the payload of its detail side panel).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SourceResult {
    pub source_item_id: Uuid,
    pub kind: Kind,
    /// "meeting" | "slack thread" | "agent session" | "doc session" | "note"
    pub kind_label: &'static str,
    pub title: String,
    /// One terse line; `null` when nothing was stored to say it.
    pub outcome: Option<String>,
    pub occurred_at: Option<DateTime<Utc>>,
    /// Recorded duration; meetings only.
    pub duration_ms: Option<i64>,
    /// Up to [`TILE_PEOPLE`] names.
    pub people: Vec<String>,
    pub people_count: usize,
    pub graph: GraphRef,
    /// Rank 1 of a find answer. Always false for title search.
    pub best: bool,
    pub decisions: Vec<String>,
    pub reefs: Vec<Topic>,
    /// Passages of this source the answer cited (empty for title search).
    pub quotes: Vec<Quote>,
}

// ── Pure shaping ─────────────────────────────────────────────────────────

/// Collapse whitespace; cut at `max` characters with an ellipsis.
fn clean_line(s: &str, max: usize) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        return flat;
    }
    let cut: String = flat.chars().take(max.saturating_sub(1)).collect();
    format!("{}…", cut.trim_end())
}

/// The first sentence of `s`, as one terse line.
fn first_sentence(s: &str) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    let chars: Vec<char> = flat.chars().collect();
    let mut end = chars.len();
    for i in 0..chars.len() {
        if matches!(chars[i], '.' | '!' | '?') && chars.get(i + 1).is_none_or(|c| c.is_whitespace())
        {
            end = i + 1;
            break;
        }
    }
    clean_line(&chars[..end].iter().collect::<String>(), OUTCOME_MAX_CHARS)
}

/// Case-insensitive de-duplication, alphabetical, blanks dropped.
fn dedupe_people(names: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out: Vec<String> = names
        .iter()
        .map(|n| n.trim())
        .filter(|n| !n.is_empty())
        .filter(|n| seen.insert(n.to_lowercase()))
        .map(str::to_string)
        .collect();
    out.sort_by_key(|n| n.to_lowercase());
    out
}

fn dedupe_lines(lines: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    lines
        .into_iter()
        .map(|l| clean_line(&l, QUOTE_MAX_CHARS))
        .filter(|l| !l.is_empty() && seen.insert(l.to_lowercase()))
        .collect()
}

fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// `source_id` without a trailing `:chunk:N` (the legacy atoms of one source
/// are `<source_id>:chunk:0`, `:chunk:1`, ...).
pub fn base_source_id(source_id: &str) -> &str {
    if let Some((base, n)) = source_id.rsplit_once(":chunk:") {
        if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) {
            return base;
        }
    }
    source_id
}

/// Whether `text` asks to find a specific source ("Find the Acme pricing
/// meeting from two weeks ago", "Which call did we discuss billing on?").
/// A cheap lexical test, so the decision to attach tiles is deterministic and
/// costs no model call. A miss only means no tiles; the answer is unchanged.
pub fn is_find_question(text: &str) -> bool {
    let t = text.trim().to_lowercase();
    let t = t.trim_start_matches(|c: char| !c.is_alphanumeric());
    const LEADS: &[&str] = &[
        "find ",
        "find me",
        "locate ",
        "pull up",
        "look up",
        "lookup ",
        "search for",
        "show me the",
        "show me that",
        "where did",
        "where was",
        "where is the",
        "when was the",
        "when did we",
        "which meeting",
        "which call",
        "which thread",
        "which session",
        "which doc",
        "which note",
        "what meeting",
        "what call",
        "what thread",
        "what session",
        "i'm looking for",
        "im looking for",
        "i am looking for",
        "can you find",
        "could you find",
        "please find",
    ];
    LEADS.iter().any(|lead| t.starts_with(lead))
}

// ── Candidates, grouping and ranking ─────────────────────────────────────

/// Where a candidate points before it is resolved to a source item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ref {
    Item(Uuid),
    /// A legacy `atoms` row; its source is found by `atoms.source_id`.
    Atom(Uuid),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub graph_id: Uuid,
    pub target: Ref,
    /// Position in the answer's evidence (retrieval order).
    pub order: usize,
    pub score: f64,
    pub quote: Option<Quote>,
}

fn uuid_of(v: &Value, key: &str) -> Option<Uuid> {
    v.get(key)?.as_str()?.parse().ok()
}

fn quote_of(c: &Value) -> Option<Quote> {
    let text = c.get("quote")?.as_str()?;
    let quote = clean_line(text, QUOTE_MAX_CHARS);
    (!quote.is_empty()).then(|| Quote {
        quote,
        passage_id: uuid_of(c, "passage_id"),
        occurred_at: c
            .get("occurred_at")
            .and_then(Value::as_str)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc)),
    })
}

/// The answer's evidence as ordered candidates: `retained_citations` (else
/// the citations under `outcomes`), then `legacy_evidence` chunks.
pub fn candidates_from_payload(payload: &Value) -> Vec<Candidate> {
    let mut out = Vec::new();
    let retained = payload.get("retained_citations").and_then(Value::as_array);
    let cites: Vec<&Value> = match retained {
        Some(items) if !items.is_empty() => items.iter().collect(),
        _ => payload
            .get("outcomes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .flat_map(|o| {
                o.get("citations")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .collect(),
    };
    for c in cites {
        let (Some(graph_id), Some(item)) = (uuid_of(c, "graph_id"), uuid_of(c, "source_item_id"))
        else {
            continue;
        };
        out.push(Candidate {
            graph_id,
            target: Ref::Item(item),
            order: out.len(),
            score: c
                .get("score")
                .and_then(Value::as_f64)
                .filter(|s| s.is_finite())
                .unwrap_or(0.0),
            quote: quote_of(c),
        });
    }
    for c in payload
        .get("legacy_evidence")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let (Some(graph_id), Some(atom)) = (uuid_of(c, "graph_id"), uuid_of(c, "atom_id")) else {
            continue;
        };
        out.push(Candidate {
            graph_id,
            target: Ref::Atom(atom),
            order: out.len(),
            score: c
                .get("score")
                .and_then(Value::as_f64)
                .filter(|s| s.is_finite())
                .unwrap_or(0.0),
            quote: None,
        });
    }
    out.truncate(MAX_CANDIDATES);
    out
}

/// One source after grouping.
#[derive(Debug, Clone, PartialEq)]
pub struct Ranked {
    pub graph_id: Uuid,
    pub item: Uuid,
    pub first_order: usize,
    pub best_score: f64,
    pub citations: usize,
    pub quotes: Vec<Quote>,
}

/// Group resolved candidates by source item and rank them: best citation
/// score, then earliest appearance, then most citations, then item id.
pub fn group_and_rank(resolved: &[(Uuid, Candidate)], limit: usize) -> Vec<Ranked> {
    let mut by_item: BTreeMap<Uuid, Ranked> = BTreeMap::new();
    for (item, c) in resolved {
        let r = by_item.entry(*item).or_insert_with(|| Ranked {
            graph_id: c.graph_id,
            item: *item,
            first_order: c.order,
            best_score: c.score,
            citations: 0,
            quotes: Vec::new(),
        });
        r.citations += 1;
        r.first_order = r.first_order.min(c.order);
        r.best_score = r.best_score.max(c.score);
        if let Some(q) = &c.quote {
            if r.quotes.len() < QUOTES_PER_TILE
                && !r
                    .quotes
                    .iter()
                    .any(|e| e.quote.eq_ignore_ascii_case(&q.quote))
            {
                r.quotes.push(q.clone());
            }
        }
    }
    let mut ranked: Vec<Ranked> = by_item.into_values().collect();
    ranked.sort_by(|a, b| {
        b.best_score
            .total_cmp(&a.best_score)
            .then(a.first_order.cmp(&b.first_order))
            .then(b.citations.cmp(&a.citations))
            .then(a.item.cmp(&b.item))
    });
    ranked.truncate(limit);
    ranked
}

// ── Enrichment rows ──────────────────────────────────────────────────────

/// A source item as the base query reads it.
#[derive(Debug, Clone)]
struct ItemRow {
    item: Uuid,
    graph_id: Uuid,
    source_type: String,
    content_kind: Option<String>,
    source_id: String,
    title: Option<String>,
    occurred_at: Option<DateTime<Utc>>,
    duration_ms: Option<i64>,
    summary_overview: Option<String>,
    summary_decisions: Vec<String>,
    /// The current version's own `participants`.
    participants: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct Semantic {
    decisions: Vec<String>,
    /// `(node_id, title, type, hits)`
    nodes: Vec<(Uuid, String, String, i64)>,
}

fn kind_of(row: &ItemRow) -> Option<Kind> {
    Kind::from_source_type(&row.source_type)
        .or_else(|| row.content_kind.as_deref().and_then(Kind::from_source_type))
}

/// Decisions by trust: extracted decision atoms, else the meeting summary's,
/// else the titles of linked decision-type reefs.
fn decisions_of(row: &ItemRow, sem: &Semantic) -> Vec<String> {
    let atoms = dedupe_lines(sem.decisions.iter().cloned());
    if !atoms.is_empty() {
        return atoms;
    }
    let summary = dedupe_lines(row.summary_decisions.iter().cloned());
    if !summary.is_empty() {
        return summary;
    }
    dedupe_lines(
        sem.nodes
            .iter()
            .filter(|n| n.2 == "decision")
            .map(|n| n.1.clone()),
    )
}

/// Non-decision reefs, most-linked first.
fn reefs_of(sem: &Semantic) -> Vec<Topic> {
    let mut by_node: BTreeMap<Uuid, (String, i64)> = BTreeMap::new();
    for (id, title, ty, hits) in &sem.nodes {
        if ty == "decision" || title.trim().is_empty() {
            continue;
        }
        by_node.entry(*id).or_insert_with(|| (title.clone(), 0)).1 += hits;
    }
    let mut v: Vec<(Uuid, String, i64)> =
        by_node.into_iter().map(|(id, (t, h))| (id, t, h)).collect();
    v.sort_by(|a, b| {
        b.2.cmp(&a.2)
            .then_with(|| a.1.to_lowercase().cmp(&b.1.to_lowercase()))
            .then(a.0.cmp(&b.0))
    });
    v.into_iter()
        .map(|(node_id, title, _)| Topic { node_id, title })
        .collect()
}

fn build_result(
    row: &ItemRow,
    kind: Kind,
    graph: &GraphRef,
    people: &[String],
    sem: &Semantic,
    quotes: Vec<Quote>,
) -> SourceResult {
    let decisions = decisions_of(row, sem);
    let outcome = row
        .summary_overview
        .as_deref()
        .map(first_sentence)
        .filter(|s| !s.is_empty())
        .or_else(|| decisions.first().map(|d| clean_line(d, OUTCOME_MAX_CHARS)));
    let title = row
        .title
        .as_deref()
        .map(|t| clean_line(t, 120))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| kind.default_title().to_string());
    let people = dedupe_people(people);
    SourceResult {
        source_item_id: row.item,
        kind,
        kind_label: kind.label(),
        title,
        outcome,
        occurred_at: row.occurred_at,
        duration_ms: if kind == Kind::Meeting {
            row.duration_ms.filter(|ms| *ms > 0)
        } else {
            None
        },
        people_count: people.len(),
        people: people.into_iter().take(TILE_PEOPLE).collect(),
        graph: graph.clone(),
        best: false,
        decisions: decisions.into_iter().take(TILE_DECISIONS).collect(),
        reefs: reefs_of(sem).into_iter().take(TILE_REEFS).collect(),
        quotes,
    }
}

// ── SQL ──────────────────────────────────────────────────────────────────

/// The title precedence `scoped_ask::CITATION_TITLES_SQL` and Library history
/// use: session title, recorded title, then Slack channel or thread context.
/// Over the aliases `si` (source_items) and `v` (its current source_versions).
const TITLE_EXPR: &str = "substring(COALESCE(\
 NULLIF(v.capture_context->>'session_title',''),NULLIF(v.source_metadata->>'session_title',''),NULLIF(v.source_metadata->>'title',''),\
 CASE WHEN v.source_type='slack' THEN CASE WHEN NULLIF(v.source_metadata->>'thread_ts','') IS NULL THEN '#' ELSE 'Thread in #' END\
 ||COALESCE(NULLIF(v.source_metadata->>'channel_name',''),NULLIF(v.origin->'container'->>'label','')) END\
) FOR 256)";

type ItemSqlRow = (
    Uuid,
    Uuid,
    String,
    Option<String>,
    String,
    Option<String>,
    Option<DateTime<Utc>>,
    Option<i64>,
    Option<String>,
    Option<Value>,
    Vec<String>,
);

/// The readable items among `$2` (in the oceans `$1`), with their current
/// version's title and moment and the meeting summary.
fn items_sql() -> String {
    format!(
        "SELECT si.source_item_id,si.graph_id,si.source_type,si.origin->>'content_kind',si.source_id,{TITLE_EXPR},\
         COALESCE(v.occurred_at,v.captured_at,si.created_at),COALESCE(mt.duration_ms,v.duration_ms),mt.summary->>'overview',mt.summary->'decisions',COALESCE(v.participants,'{{}}'::text[]) \
         FROM unnest($2::uuid[]) k(id) \
         JOIN source_items si ON si.source_item_id=k.id AND si.graph_id=ANY($1) AND si.access_state='permitted' \
         LEFT JOIN source_versions v ON v.graph_id=si.graph_id AND v.version_id=si.current_version_id \
         LEFT JOIN meeting_transcripts mt ON mt.graph_id=si.graph_id AND mt.source_item_id=si.source_item_id AND {transcript_floor} \
         WHERE {item_floor}",
        transcript_floor = crate::history_floor::admits_transcript("mt"),
        item_floor = crate::history_floor::admits_source_item_at("si", "v"),
    )
}

/// Driven by `idx_source_versions_title_trgm` (migrations tenant 0217 / control
/// 0402), which indexes the exact `TITLE_EXPR` over `source_versions`; keep the
/// two identical.
///
/// Source items whose current title contains `$3` (ILIKE pattern), best
/// first: exact title (`$4`), prefix (`$5`), then newest.
fn title_search_sql() -> String {
    format!(
        "SELECT si.source_item_id FROM source_versions v \
         JOIN source_items si ON si.graph_id=v.graph_id AND si.current_version_id=v.version_id \
         WHERE v.graph_id=ANY($1) AND si.access_state='permitted' \
         AND (si.source_type=ANY($2) OR si.origin->>'content_kind'=ANY($2)) \
         AND {item_floor} AND {TITLE_EXPR} ILIKE $3 \
         ORDER BY (lower({TITLE_EXPR})=lower($4)) DESC,(lower({TITLE_EXPR}) LIKE lower($5)) DESC,\
         COALESCE(v.occurred_at,v.captured_at,si.created_at) DESC,si.source_item_id \
         LIMIT $6",
        item_floor = crate::history_floor::admits_source_item_at("si", "v"),
    )
}

/// Names around a source: the participants and authors of its legacy atoms.
/// Only atoms of the source's own type (`$5`) that `library_permitted_atoms`
/// admits (a retracted message's author is not shown), at most
/// [`PEOPLE_ATOMS_PER_ITEM`] per source.
fn people_sql() -> String {
    format!(
        "WITH k AS (SELECT * FROM unnest($1::uuid[],$2::uuid[],$3::text[],$4::text[],$5::text[]) k(id,graph_id,source_id,chunk_like,source_type)), \
         cand AS (SELECT k.id,a.graph_id,a.atom_id,a.participants,a.author_name FROM k \
           CROSS JOIN LATERAL (SELECT a.* FROM atoms a WHERE a.graph_id=k.graph_id AND a.source_type=k.source_type \
             AND (a.source_id=k.source_id OR a.source_id LIKE k.chunk_like) AND {floor} \
             ORDER BY a.\"timestamp\",a.atom_id LIMIT {PEOPLE_ATOMS_PER_ITEM}) a), \
         per_graph AS (SELECT graph_id,array_agg(atom_id) ids FROM cand GROUP BY graph_id), \
         permitted AS (SELECT p.atom_id FROM per_graph g CROSS JOIN LATERAL library_permitted_atoms(g.graph_id,g.ids) p) \
         SELECT c.id,p FROM cand c JOIN permitted ON permitted.atom_id=c.atom_id \
         CROSS JOIN LATERAL unnest(COALESCE(c.participants,'{{}}'::text[])||ARRAY[c.author_name]) p \
         WHERE p IS NOT NULL AND btrim(p)<>'' GROUP BY c.id,p",
        floor = crate::history_floor::admits_atom("a"),
    )
}

/// Decision atoms and linked reefs of each item's current version, through
/// `served_semantic_atoms` so corrections and the history floor apply.
fn semantic_sql() -> String {
    format!(
        "WITH cur AS (SELECT si.source_item_id,si.graph_id,si.current_version_id vid \
           FROM unnest($1::uuid[]) k(id) \
           JOIN source_items si ON si.source_item_id=k.id AND si.graph_id=ANY($2) \
           WHERE si.access_state='permitted' AND si.current_version_id IS NOT NULL AND {item_floor}) \
         SELECT 'decision'::text,cur.source_item_id,sa.statement,NULL::uuid,NULL::text,sa.ordinal::bigint \
           FROM cur JOIN served_semantic_atoms sa ON sa.graph_id=cur.graph_id AND sa.source_version_id=cur.vid \
           WHERE sa.kind='decision' \
         UNION ALL \
         SELECT 'node'::text,cur.source_item_id,library_node_title(n.graph_id,n.node_id),n.node_id,n.type,COUNT(*)::bigint \
           FROM cur \
           JOIN served_semantic_atoms sa ON sa.graph_id=cur.graph_id AND sa.source_version_id=cur.vid \
           JOIN node_semantic_atoms ns ON ns.semantic_atom_id=sa.semantic_atom_id AND ns.graph_id=sa.graph_id \
           JOIN nodes n ON n.node_id=ns.node_id AND n.graph_id=ns.graph_id \
           WHERE library_node_permitted(n.graph_id,n.node_id) \
           GROUP BY cur.source_item_id,n.graph_id,n.node_id,n.type \
         ORDER BY 2,6",
        item_floor = crate::history_floor::admits_source_item("si"),
    )
}

/// Legacy atoms `$2` (in oceans `$1`) to the source item they belong to.
fn atom_items_sql() -> String {
    format!(
        "SELECT a.atom_id,si.source_item_id FROM unnest($2::uuid[]) k(id) \
         JOIN atoms a ON a.atom_id=k.id AND a.graph_id=ANY($1) AND {atom_floor} \
           AND EXISTS (SELECT 1 FROM library_permitted_atoms(a.graph_id,ARRAY[a.atom_id])) \
         JOIN LATERAL (SELECT s.source_item_id FROM source_items s \
           WHERE s.graph_id=a.graph_id AND s.source_id=regexp_replace(a.source_id,':chunk:[0-9]+$','') \
           AND s.source_type=a.source_type \
           AND s.access_state='permitted' AND {item_floor} \
           ORDER BY s.updated_at DESC,s.source_item_id LIMIT 1) si ON true",
        atom_floor = crate::history_floor::admits_atom("a"),
        item_floor = crate::history_floor::admits_source_item("s"),
    )
}

// ── Reads ────────────────────────────────────────────────────────────────

/// A failure the caller reports as a 5xx; the cause is logged, never sent.
#[derive(Debug)]
pub struct FindError(pub String);

impl std::fmt::Display for FindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<sqlx::Error> for FindError {
    fn from(e: sqlx::Error) -> Self {
        FindError(e.to_string())
    }
}

/// The oceans the caller may read right now (ADR-0007 D9), the read
/// `GET /v1/graphs` uses: its own private ocean, shared and team oceans it
/// belongs to; never another member's private ocean, never archived.
pub async fn readable_graphs(
    tenant: &Tenant,
    user: &UserContext,
) -> Result<Vec<GraphRef>, FindError> {
    let Some(store) = tenant.store.as_ref() else {
        return Ok(vec![]);
    };
    let graphs = ReachabilityResolver::list_reachable_graphs(
        tenant.decision.as_ref(),
        store.as_ref(),
        user.user_id,
    )
    .await
    .map_err(|e| FindError(format!("reachability: {e}")))?;
    Ok(graphs
        .into_iter()
        .filter(|g| g.archived_at.is_none())
        .map(|g| GraphRef {
            id: g.id,
            name: g.name,
        })
        .collect())
}

/// The caller's readable oceans narrowed to its active account, inside a
/// member-scoped transaction. `list_reachable_graphs` spans every account the
/// user belongs to on the consumer plane (`tenancy::reachability`), so the
/// `account_id` predicate is what keeps another account's sources out.
async fn scope(
    tenant: &Tenant,
    user: &UserContext,
) -> Result<Option<(Transaction<'static, Postgres>, Vec<GraphRef>)>, FindError> {
    let graphs = readable_graphs(tenant, user).await?;
    if matches!(graphs.as_slice(), []) {
        return Ok(None);
    }
    let mut tx = begin(tenant, user).await?;
    let ids: Vec<Uuid> = graphs.iter().map(|g| g.id).collect();
    let in_account: HashSet<Uuid> =
        sqlx::query_scalar("SELECT id FROM graphs WHERE id = ANY($1) AND account_id = $2")
            .bind(&ids)
            .bind(user.account_id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
    let graphs: Vec<GraphRef> = graphs
        .into_iter()
        .filter(|g| in_account.contains(&g.id))
        .collect();
    Ok(Some((tx, graphs)))
}

async fn begin(
    tenant: &Tenant,
    user: &UserContext,
) -> Result<Transaction<'static, Postgres>, FindError> {
    let mut tx = tenant
        .begin_member_scoped(user.account_id)
        .await
        .map_err(|e| FindError(format!("begin_member_scoped: {e}")))?;
    sqlx::query(STATEMENT_TIMEOUT).execute(&mut *tx).await?;
    Ok(tx)
}

/// Tiles for `ids` (in that order), skipping anything unreadable or without a
/// kind. `quotes` are the passages an answer cited, by item.
async fn hydrate(
    tx: &mut Transaction<'_, Postgres>,
    graphs: &[GraphRef],
    ids: &[Uuid],
    quotes: &HashMap<Uuid, Vec<Quote>>,
) -> Result<Vec<SourceResult>, sqlx::Error> {
    if ids.is_empty() || matches!(graphs, []) {
        return Ok(vec![]);
    }
    let graph_ids: Vec<Uuid> = graphs.iter().map(|g| g.id).collect();
    let names: HashMap<Uuid, &GraphRef> = graphs.iter().map(|g| (g.id, g)).collect();

    let rows: Vec<ItemSqlRow> = sqlx::query_as(&items_sql())
        .bind(&graph_ids)
        .bind(ids)
        .fetch_all(&mut **tx)
        .await?;
    let items: HashMap<Uuid, ItemRow> = rows
        .into_iter()
        .map(
            |(
                item,
                graph_id,
                source_type,
                content_kind,
                source_id,
                title,
                occurred_at,
                duration_ms,
                overview,
                decisions,
                participants,
            )| {
                let summary_decisions = match decisions {
                    Some(Value::Array(list)) => list
                        .iter()
                        .filter_map(|d| {
                            d.get("text").and_then(Value::as_str).or_else(|| d.as_str())
                        })
                        .map(str::to_string)
                        .collect(),
                    _ => vec![],
                };
                (
                    item,
                    ItemRow {
                        item,
                        graph_id,
                        source_type,
                        content_kind,
                        source_id,
                        title,
                        occurred_at,
                        duration_ms,
                        summary_overview: overview.filter(|o| !o.trim().is_empty()),
                        summary_decisions,
                        participants,
                    },
                )
            },
        )
        .collect();
    let kept: Vec<&ItemRow> = ids
        .iter()
        .filter_map(|id| items.get(id))
        .filter(|r| kind_of(r).is_some() && names.contains_key(&r.graph_id))
        .collect();
    if kept.is_empty() {
        return Ok(vec![]);
    }

    let keep_ids: Vec<Uuid> = kept.iter().map(|r| r.item).collect();
    type PeopleKeys = (Vec<Uuid>, Vec<Uuid>, Vec<String>, Vec<String>, Vec<String>);
    let (p_ids, p_graphs, p_sources, p_likes, p_types): PeopleKeys =
        kept.iter()
            .fold((vec![], vec![], vec![], vec![], vec![]), |mut acc, r| {
                acc.0.push(r.item);
                acc.1.push(r.graph_id);
                acc.2.push(r.source_id.clone());
                acc.3.push(format!("{}:chunk:%", escape_like(&r.source_id)));
                acc.4.push(r.source_type.clone());
                acc
            });
    let people_rows: Vec<(Uuid, String)> = sqlx::query_as(&people_sql())
        .bind(&p_ids)
        .bind(&p_graphs)
        .bind(&p_sources)
        .bind(&p_likes)
        .bind(&p_types)
        .fetch_all(&mut **tx)
        .await?;
    let mut people: HashMap<Uuid, Vec<String>> = kept
        .iter()
        .map(|r| (r.item, r.participants.clone()))
        .collect();
    for (item, name) in people_rows {
        people.entry(item).or_default().push(name);
    }

    type SemanticRow = (
        String,
        Uuid,
        Option<String>,
        Option<Uuid>,
        Option<String>,
        i64,
    );
    let semantic_rows: Vec<SemanticRow> = sqlx::query_as(&semantic_sql())
        .bind(&keep_ids)
        .bind(&graph_ids)
        .fetch_all(&mut **tx)
        .await?;
    let mut semantic: HashMap<Uuid, Semantic> = HashMap::new();
    for (what, item, text, node, ty, n) in semantic_rows {
        let slot = semantic.entry(item).or_default();
        match (what.as_str(), text, node, ty) {
            ("decision", Some(statement), _, _) => slot.decisions.push(statement),
            ("node", Some(title), Some(node_id), Some(ty)) => {
                slot.nodes.push((node_id, title, ty, n))
            }
            _ => {}
        }
    }

    let empty_people: Vec<String> = vec![];
    let empty_sem = Semantic::default();
    Ok(kept
        .into_iter()
        .filter_map(|r| {
            let kind = kind_of(r)?;
            let graph = names.get(&r.graph_id)?;
            Some(build_result(
                r,
                kind,
                graph,
                people.get(&r.item).unwrap_or(&empty_people),
                semantic.get(&r.item).unwrap_or(&empty_sem),
                quotes.get(&r.item).cloned().unwrap_or_default(),
            ))
        })
        .collect())
}

/// The tiles of one answer payload, best first. Pure reads in `tx`.
pub async fn answer_tiles(
    tx: &mut Transaction<'_, Postgres>,
    graphs: &[GraphRef],
    payload: &Value,
) -> Result<Vec<SourceResult>, sqlx::Error> {
    Ok(answer_tiles_batch(tx, graphs, &[payload]).await?.remove(0))
}

/// Same derivation as `answer_tiles`, with shared SQL hydration for a bounded
/// selected conversation page. Quotes and ranking remain answer-local.
/// Never opens a transaction or resolves reachability independently of `tx`.
pub(crate) async fn answer_tiles_batch(
    tx: &mut Transaction<'_, Postgres>,
    graphs: &[GraphRef],
    payloads: &[&Value],
) -> Result<Vec<Vec<SourceResult>>, sqlx::Error> {
    let readable: HashSet<Uuid> = graphs.iter().map(|g| g.id).collect();
    let candidates: Vec<Vec<Candidate>> = payloads
        .iter()
        .map(|payload| {
            candidates_from_payload(payload)
                .into_iter()
                .filter(|c| readable.contains(&c.graph_id))
                .collect()
        })
        .collect();
    let atoms: Vec<Uuid> = candidates
        .iter()
        .flatten()
        .filter_map(|c| {
            if let Ref::Atom(a) = c.target {
                Some(a)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let mut atom_item: HashMap<Uuid, Uuid> = HashMap::new();
    if !atoms.is_empty() {
        let graph_ids: Vec<Uuid> = graphs.iter().map(|g| g.id).collect();
        let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(&atom_items_sql())
            .bind(&graph_ids)
            .bind(&atoms)
            .fetch_all(&mut **tx)
            .await?;
        atom_item.extend(rows);
    }
    let ranked: Vec<Vec<Ranked>> = candidates
        .into_iter()
        .map(|cs| {
            let resolved: Vec<(Uuid, Candidate)> = cs
                .into_iter()
                .filter_map(|c| {
                    let item = match c.target {
                        Ref::Item(i) => i,
                        Ref::Atom(a) => *atom_item.get(&a)?,
                    };
                    Some((item, c))
                })
                .collect();
            group_and_rank(&resolved, HYDRATE_CANDIDATES)
        })
        .collect();
    let ids: Vec<Uuid> = ranked
        .iter()
        .flatten()
        .map(|r| r.item)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let tiles: HashMap<Uuid, SourceResult> = hydrate(tx, graphs, &ids, &HashMap::new())
        .await?
        .into_iter()
        .map(|t| (t.source_item_id, t))
        .collect();
    Ok(ranked
        .into_iter()
        .map(|rs| {
            let mut answer: Vec<SourceResult> = rs
                .into_iter()
                .filter_map(|r| {
                    let mut tile = tiles.get(&r.item)?.clone();
                    tile.quotes = r.quotes;
                    Some(tile)
                })
                .take(MAX_TILES)
                .collect();
            if let Some(first) = answer.first_mut() {
                first.best = true;
            }
            answer
        })
        .collect())
}

/// Saved answers may outlive passage edits. A receipt certifies support identity,
/// but cards must not re-emit a persisted quote which no longer matches that
/// readable passage. Validate the selected cards together, in the caller's
/// snapshot, and withhold a card if any of its quoted passages cannot certify.
/// Live Ask keeps its existing derivation; no stored answer text is rewritten.
pub(crate) async fn certify_saved_quotes(
    tx: &mut Transaction<'_, Postgres>,
    graphs: &[GraphRef],
    answers: &mut [Vec<SourceResult>],
) -> Result<(), sqlx::Error> {
    let ids: Vec<Uuid> = answers
        .iter()
        .flatten()
        .flat_map(|t| &t.quotes)
        .filter_map(|q| q.passage_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if ids.is_empty() {
        for tiles in answers {
            tiles.retain(|t| t.quotes.is_empty());
            for (i, tile) in tiles.iter_mut().enumerate() {
                tile.best = i == 0;
            }
        }
        return Ok(());
    }
    let graph_ids: Vec<Uuid> = graphs.iter().map(|g| g.id).collect();
    let sql = format!(
        "SELECT p.passage_id,p.graph_id,v.source_item_id,p.quote \
         FROM semantic_atom_passages p \
         JOIN source_versions v ON v.graph_id=p.graph_id AND v.version_id=p.source_version_id \
         JOIN source_items si ON si.graph_id=v.graph_id AND si.source_item_id=v.source_item_id \
         WHERE p.passage_id=ANY($1) AND p.graph_id=ANY($2) AND si.access_state='permitted' AND {floor}",
        floor=crate::history_floor::admits_version("v"),
    );
    let rows: Vec<(Uuid, Uuid, Uuid, String)> = sqlx::query_as(&sql)
        .bind(&ids)
        .bind(&graph_ids)
        .fetch_all(&mut **tx)
        .await?;
    let certified: HashSet<_> = rows
        .into_iter()
        .map(|(passage, graph, item, quote)| {
            (passage, graph, item, clean_line(&quote, QUOTE_MAX_CHARS))
        })
        .collect();
    for tiles in answers {
        tiles.retain(|t| {
            t.quotes.iter().all(|q| {
                q.passage_id.is_some_and(|passage| {
                    certified.contains(&(passage, t.graph.id, t.source_item_id, q.quote.clone()))
                })
            })
        });
        // Withholding rank 1 must not lose the next readable card's best marker.
        for (i, tile) in tiles.iter_mut().enumerate() {
            tile.best = i == 0;
        }
    }
    Ok(())
}

/// Title-only search across `graphs`, case-insensitive substring, exact and
/// prefix matches first, then newest.
pub async fn title_tiles(
    tx: &mut Transaction<'_, Postgres>,
    graphs: &[GraphRef],
    q: &str,
    limit: i64,
) -> Result<Vec<SourceResult>, sqlx::Error> {
    if matches!(graphs, []) {
        return Ok(vec![]);
    }
    let graph_ids: Vec<Uuid> = graphs.iter().map(|g| g.id).collect();
    let escaped = escape_like(q);
    let ids: Vec<Uuid> = sqlx::query_scalar(&title_search_sql())
        .bind(&graph_ids)
        .bind(Kind::all_source_types())
        .bind(format!("%{escaped}%"))
        .bind(q)
        .bind(format!("{escaped}%"))
        .bind(limit)
        .fetch_all(&mut **tx)
        .await?;
    hydrate(tx, graphs, &ids, &HashMap::new()).await
}

/// Trim and validate a title query: 1..=[`MAX_QUERY_CHARS`] characters.
pub fn parse_query(raw: Option<&str>) -> Result<String, String> {
    let q = raw.map(str::trim).unwrap_or("");
    if q.is_empty() {
        return Err("q is required".into());
    }
    if q.chars().count() > MAX_QUERY_CHARS {
        return Err(format!("q must be at most {MAX_QUERY_CHARS} characters"));
    }
    Ok(q.to_string())
}

/// `limit=` clamped to 1..=[`MAX_TITLE_LIMIT`]; default [`DEFAULT_TITLE_LIMIT`].
pub fn parse_limit(raw: Option<i64>) -> i64 {
    raw.unwrap_or(DEFAULT_TITLE_LIMIT).clamp(1, MAX_TITLE_LIMIT)
}

/// `GET /v1/find/titles`: the caller's matching sources.
pub async fn search_titles(
    tenant: &Tenant,
    user: &UserContext,
    q: &str,
    limit: i64,
) -> Result<Vec<SourceResult>, FindError> {
    if tenant.pool.is_none() {
        return Ok(vec![]);
    }
    let work = async {
        let Some((mut tx, graphs)) = scope(tenant, user).await? else {
            return Ok(vec![]);
        };
        let tiles = title_tiles(&mut tx, &graphs, q, limit).await?;
        tx.commit().await?;
        Ok(tiles)
    };
    tokio::time::timeout(ENRICH_BUDGET, work)
        .await
        .map_err(|_| FindError("title search timed out".into()))?
}

/// Add `source_results` to a scoped-ask answer payload (`POST /v1/queries`
/// and the stream's `final` event). Additive and best-effort:
///
/// * flag off, or the question is not a find question: the field is absent;
/// * a find question with no readable cited source: `[]`;
/// * any failure is logged and leaves the field absent. The answer itself is
///   never delayed past this read's 5 s statement timeout and never failed.
pub async fn attach_to_answer(
    tenant: &Tenant,
    user: &UserContext,
    question: &str,
    payload: &mut Value,
) {
    if !enabled() || !is_find_question(question) || tenant.pool.is_none() {
        return;
    }
    let work = async {
        let Some((mut tx, graphs)) = scope(tenant, user).await? else {
            return Ok::<_, FindError>(vec![]);
        };
        let tiles = answer_tiles(&mut tx, &graphs, payload).await?;
        tx.commit().await?;
        Ok(tiles)
    };
    // One budget for the whole enrichment: the stream's `meta` event waits on it.
    let result = match tokio::time::timeout(ENRICH_BUDGET, work).await {
        Ok(r) => r,
        Err(_) => Err(FindError("timed out".into())),
    };
    match result {
        Ok(tiles) => payload["source_results"] = json!(tiles),
        Err(e) => {
            tracing::warn!(error = %e, "find results: tiles unavailable; answer sent without source_results")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn cite(graph: u128, item: u128, quote: &str, score: Option<f64>) -> Value {
        let mut c = json!({
            "kind": "source_passage", "graph_id": id(graph), "source_item_id": id(item),
            "passage_id": id(900 + item), "quote": quote, "occurred_at": "2026-10-15T15:00:00Z",
        });
        if let Some(s) = score {
            c["score"] = json!(s);
        }
        c
    }

    fn cand(item: u128, order: usize, score: f64, quote: Option<&str>) -> (Uuid, Candidate) {
        (
            id(item),
            Candidate {
                graph_id: id(1),
                target: Ref::Item(id(item)),
                order,
                score,
                quote: quote.map(|q| Quote {
                    quote: q.into(),
                    passage_id: None,
                    occurred_at: None,
                }),
            },
        )
    }

    #[test]
    fn kind_table_maps_every_source_type_once() {
        let mut seen = HashSet::new();
        for k in Kind::ALL {
            for t in k.source_types() {
                assert!(seen.insert(*t), "{t} maps twice");
                assert_eq!(Kind::from_source_type(t), Some(k));
            }
        }
        assert_eq!(Kind::from_source_type("transcript"), Some(Kind::Meeting));
        assert_eq!(Kind::from_source_type("google_chat"), Some(Kind::Slack));
        assert_eq!(Kind::from_source_type("gmail"), None);
        assert_eq!(Kind::Slack.label(), "slack thread");
        assert_eq!(Kind::Agent.label(), "agent session");
        assert_eq!(Kind::Doc.label(), "doc session");
    }

    #[test]
    fn base_source_id_drops_only_a_numeric_chunk_suffix() {
        assert_eq!(base_source_id("meet-1:chunk:12"), "meet-1");
        assert_eq!(base_source_id("meet-1"), "meet-1");
        assert_eq!(base_source_id("a:chunk:b"), "a:chunk:b");
        assert_eq!(base_source_id("a:chunk:"), "a:chunk:");
        assert_eq!(
            base_source_id("cc:team:alpha:ckpt-1"),
            "cc:team:alpha:ckpt-1"
        );
    }

    #[test]
    fn find_questions_are_recognized_and_others_are_not() {
        for q in [
            "Find the Acme pricing meeting from two weeks ago",
            "  find me the auth rewrite thread",
            "Which meeting did we decide on usage-based billing?",
            "where did we discuss the EU rollout",
            "\"Pull up the Halcyon launch doc",
            "Can you find the call with Tom?",
        ] {
            assert!(is_find_question(q), "{q}");
        }
        for q in [
            "Why did billing move to usage-based?",
            "Who owns the EU rollout?",
            "Summarize last week",
            "",
        ] {
            assert!(!is_find_question(q), "{q}");
        }
    }

    #[test]
    fn first_sentence_stops_at_the_first_terminator_and_clips() {
        assert_eq!(
            first_sentence("The team chose tiers.  Tom owns it."),
            "The team chose tiers."
        );
        assert_eq!(first_sentence("v1.2 is out. Next"), "v1.2 is out.");
        assert_eq!(
            first_sentence("no terminator at all"),
            "no terminator at all"
        );
        let long = "word ".repeat(100);
        let out = first_sentence(&long);
        assert!(out.chars().count() <= OUTCOME_MAX_CHARS && out.ends_with('…'));
    }

    #[test]
    fn candidates_prefer_retained_citations_then_outcomes_then_legacy() {
        let payload = json!({
            "retained_citations": [cite(1, 10, "a", None), cite(1, 11, "b", Some(0.9))],
            "outcomes": [{"citations": [cite(1, 99, "ignored", None)]}],
            "legacy_evidence": [{"graph_id": id(1), "atom_id": id(55)}],
        });
        let got = candidates_from_payload(&payload);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].target, Ref::Item(id(10)));
        assert_eq!(got[1].score, 0.9);
        assert_eq!(got[2].target, Ref::Atom(id(55)));
        assert_eq!(
            got.iter().map(|c| c.order).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );

        let outcomes_only = json!({"outcomes": [{"citations": [cite(1, 99, "x", None)]}]});
        assert_eq!(
            candidates_from_payload(&outcomes_only)[0].target,
            Ref::Item(id(99))
        );
        assert!(candidates_from_payload(&json!({})).is_empty());
        // Malformed ids are skipped, not fatal.
        let bad = json!({"retained_citations": [{"graph_id": "nope", "source_item_id": "x"}]});
        assert!(candidates_from_payload(&bad).is_empty());
    }

    #[test]
    fn grouping_is_per_source_not_per_passage() {
        let ranked = group_and_rank(
            &[
                cand(10, 0, 0.0, Some("p1")),
                cand(10, 1, 0.0, Some("p2")),
                cand(10, 2, 0.0, Some("p3")),
                cand(11, 3, 0.0, None),
            ],
            3,
        );
        assert_eq!(ranked.len(), 2);
        assert_eq!((ranked[0].item, ranked[0].citations), (id(10), 3));
        assert_eq!(ranked[0].quotes.len(), 3);
        assert_eq!((ranked[1].item, ranked[1].citations), (id(11), 1));
    }

    #[test]
    fn ranking_is_score_then_order_then_count_then_id_and_capped() {
        // Score wins over order.
        let r = group_and_rank(&[cand(10, 0, 0.1, None), cand(11, 1, 0.8, None)], 3);
        assert_eq!(
            r.iter().map(|x| x.item).collect::<Vec<_>>(),
            vec![id(11), id(10)]
        );
        // Equal scores: earliest first appearance.
        let r = group_and_rank(
            &[
                cand(12, 0, 0.0, None),
                cand(10, 1, 0.0, None),
                cand(11, 2, 0.0, None),
            ],
            3,
        );
        assert_eq!(
            r.iter().map(|x| x.item).collect::<Vec<_>>(),
            vec![id(12), id(10), id(11)]
        );
        // Same first appearance rank is impossible across items, so the count
        // breaks a tie only through orders: a later but heavier source loses.
        let r = group_and_rank(
            &[
                cand(10, 0, 0.0, None),
                cand(11, 1, 0.0, None),
                cand(11, 2, 0.0, None),
            ],
            3,
        );
        assert_eq!(r[0].item, id(10));
        // The best score of any citation represents the source.
        let r = group_and_rank(
            &[
                cand(10, 0, 0.0, None),
                cand(11, 1, 0.2, None),
                cand(11, 2, 0.9, None),
            ],
            3,
        );
        assert_eq!((r[0].item, r[0].best_score), (id(11), 0.9));
        // Capped, and deterministic under input permutation.
        let many: Vec<_> = (0..8)
            .map(|i| cand(20 + i, i as usize, 0.0, None))
            .collect();
        let mut reversed = many.clone();
        reversed.reverse();
        let a = group_and_rank(&many, MAX_TILES);
        let b = group_and_rank(&reversed, MAX_TILES);
        assert_eq!(a.len(), MAX_TILES);
        assert_eq!(a, b);
        assert_eq!(a[0].item, id(20));
    }

    #[test]
    fn quotes_are_deduplicated_and_capped_per_source() {
        let cs: Vec<_> = ["A", "a", "B", "C", "D"]
            .iter()
            .enumerate()
            .map(|(i, q)| cand(10, i, 0.0, Some(q)))
            .collect();
        let r = group_and_rank(&cs, 3);
        let quotes: Vec<&str> = r[0].quotes.iter().map(|q| q.quote.as_str()).collect();
        assert_eq!(quotes, vec!["A", "B", "C"]);
    }

    fn row(
        overview: Option<&str>,
        summary_decisions: &[&str],
        title: Option<&str>,
        kind: &str,
    ) -> ItemRow {
        ItemRow {
            item: id(10),
            graph_id: id(1),
            source_type: kind.into(),
            content_kind: None,
            source_id: "meet-1".into(),
            title: title.map(str::to_string),
            occurred_at: Some(Utc::now()),
            duration_ms: Some(42 * 60_000),
            summary_overview: overview.map(str::to_string),
            summary_decisions: summary_decisions.iter().map(|s| s.to_string()).collect(),
            participants: vec![],
        }
    }

    fn graph() -> GraphRef {
        GraphRef {
            id: id(1),
            name: "GTM".into(),
        }
    }

    #[test]
    fn outcome_is_summary_sentence_then_first_decision_then_null() {
        let sem = Semantic {
            decisions: vec!["Offer a soft cap".into()],
            nodes: vec![],
        };
        let t = build_result(
            &row(
                Some("Acme pushed back. Three tiers."),
                &[],
                Some("Acme pricing"),
                "transcript",
            ),
            Kind::Meeting,
            &graph(),
            &[],
            &sem,
            vec![],
        );
        assert_eq!(t.outcome.as_deref(), Some("Acme pushed back."));
        let t = build_result(
            &row(None, &["From summary"], None, "transcript"),
            Kind::Meeting,
            &graph(),
            &[],
            &sem,
            vec![],
        );
        assert_eq!(
            t.outcome.as_deref(),
            Some("Offer a soft cap"),
            "decision atoms outrank the summary's"
        );
        let t = build_result(
            &row(None, &["From summary"], None, "transcript"),
            Kind::Meeting,
            &graph(),
            &[],
            &Semantic::default(),
            vec![],
        );
        assert_eq!(t.outcome.as_deref(), Some("From summary"));
        assert_eq!(
            t.title, "Meeting",
            "an untitled source takes its kind's default"
        );
        let t = build_result(
            &row(None, &[], Some("  "), "note"),
            Kind::Note,
            &graph(),
            &[],
            &Semantic::default(),
            vec![],
        );
        assert_eq!((t.outcome, t.title.as_str()), (None, "Note"));
    }

    #[test]
    fn tile_carries_kind_duration_people_and_reefs() {
        let node = id(77);
        let sem = Semantic {
            decisions: vec![],
            nodes: vec![
                (node, "Acme renewal".into(), "project".into(), 2),
                (node, "Acme renewal".into(), "project".into(), 3),
                (id(78), "Propose tiers".into(), "decision".into(), 1),
                (id(79), " ".into(), "project".into(), 9),
            ],
        };
        let people: Vec<String> = ["tom becker", "Sara Kim", "Tom Becker", " ", "Priya Nair"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let t = build_result(
            &row(None, &[], Some("Acme pricing review"), "transcript"),
            Kind::Meeting,
            &graph(),
            &people,
            &sem,
            vec![],
        );
        assert_eq!((t.kind, t.kind_label), (Kind::Meeting, "meeting"));
        assert_eq!(t.duration_ms, Some(42 * 60_000));
        assert_eq!(t.people, vec!["Priya Nair", "Sara Kim", "tom becker"]);
        assert_eq!(t.people_count, 3);
        assert_eq!(
            t.reefs,
            vec![Topic {
                node_id: node,
                title: "Acme renewal".into()
            }]
        );
        assert_eq!(
            t.decisions,
            vec!["Propose tiers"],
            "a decision-type reef is the last-resort decision"
        );
        assert!(!t.best);
        // Only meetings carry a duration.
        let note = build_result(
            &row(None, &[], Some("n"), "note"),
            Kind::Note,
            &graph(),
            &[],
            &Semantic::default(),
            vec![],
        );
        assert_eq!(note.duration_ms, None);
    }

    #[test]
    fn query_and_limit_are_validated() {
        assert_eq!(parse_query(Some("  acme  ")).unwrap(), "acme");
        assert!(parse_query(Some("   ")).is_err());
        assert!(parse_query(None).is_err());
        assert!(parse_query(Some(&"x".repeat(MAX_QUERY_CHARS + 1))).is_err());
        assert_eq!(parse_limit(None), DEFAULT_TITLE_LIMIT);
        assert_eq!(parse_limit(Some(0)), 1);
        assert_eq!(parse_limit(Some(1000)), MAX_TITLE_LIMIT);
        assert_eq!(escape_like(r"50%_off\"), r"50\%\_off\\");
    }

    #[tokio::test]
    async fn the_field_is_absent_while_the_flag_is_off_or_the_question_is_not_a_find() {
        // No pool either way, so this proves the early returns only; the pg
        // suite proves the same with a live database.
        let tenant = Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        };
        let user = UserContext {
            user_id: id(1),
            account_id: id(2),
            role: "member".into(),
            device_id: "t".into(),
        };
        let mut payload = json!({"retained_citations": [cite(1, 10, "a", None)]});
        attach_to_answer(&tenant, &user, "Find the Acme meeting", &mut payload).await;
        assert!(payload.get("source_results").is_none());
    }
}
