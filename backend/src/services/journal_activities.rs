//! Today activity cards (`GET /v1/journal/activities`, `.../{key}`).
//!
//! The Today page is a timeline of ACTIVITY cards (one per meeting, agent
//! session, doc session, Slack thread, note) instead of hour rows. This module
//! is the whole read: grouping, kind mapping, enrichment and the detail
//! panel's payload. It is an in-process module, not a service, so an MCP tool
//! can call it later; `routes::journal_activities` only authorizes, resolves
//! owner names and maps answers. Contract: `docs/implementation/
//! journal-activities-api.md`; validation: `.../journal-activities-validation.md`.
//!
//! # One grouping
//!
//! Atoms group by `(graph, source_type, grouping_key)` with the key
//! expression `journal_grouping::grouping_key_sql` and the session join
//! `session_join_sql`. `Store::journal_range_aggregate` (the hour rows)
//! uses the very same two functions, so a card and an hour block can never
//! disagree about which atoms are one thing. This module differs from the hour
//! aggregate in two ways only: it does not filter on `ingested_by` (the Team
//! view reads everyone's), and it keeps the key.
//!
//! # Kind mapping (`Kind::from_source_type`)
//!
//! | kind      | `atoms.source_type`                                                  |
//! |-----------|----------------------------------------------------------------------|
//! | `meeting` | `meeting`, `meeting_session`, `transcript`, `zoom`, `meet`, `teams_transcript` |
//! | `slack`   | `slack`, `teams_chat`, `teams_channel`, `chat`, `google_chat` (label "slack thread") |
//! | `agent`   | `coding_session`, `agent_session`, `mcp`                             |
//! | `doc`     | `document`, `gdrive`, `notion`, `work_session`                       |
//! | `note`    | `note`                                                               |
//!
//! Every other source type (mail, GitHub, calendar, ...) has no card and is
//! left out of the timeline.
//!
//! # Access
//!
//! The caller passes the oceans it may read (`ReachabilityResolver`'s D9
//! union: its own private ocean, shared and team oceans it is a member of;
//! never another member's private ocean) and a member-scoped transaction
//! (`Tenant::begin_member_scoped`, RLS on the tenant plane). Every statement
//! also applies the team-seat history floor (`history_floor::admits_*`).
//! `Who::Mine` narrows to groups the caller ingested at least one atom of;
//! `Who::Team` is every member's activity in those same oceans. Nothing here
//! widens the ocean list.
//!
//! # Flag
//!
//! `TODAY_ACTIVITIES_ENABLED` (unset = off): both routes answer 404.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

// ── Flag and limits ──────────────────────────────────────────────────────

pub const FLAG: &str = "TODAY_ACTIVITIES_ENABLED";

/// `TODAY_ACTIVITIES_ENABLED` is `1`/`true`. Unset = off.
pub fn enabled() -> bool {
    std::env::var(FLAG)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Widest `to - from`: Day, Week and Month (about 30 days) all fit.
pub const MAX_WINDOW_DAYS: i64 = 62;
/// Days in `day_activity` (the date strip).
pub const STRIP_DAYS: i64 = 14;
/// Most cards one request returns; `truncated` says when it cut.
pub const MAX_ACTIVITIES: i64 = 1000;
/// People on a card / topics on a card.
pub const CARD_PEOPLE: usize = 8;
pub const CARD_TOPICS: usize = 4;
/// People / reefs / passages in the detail panel.
pub const DETAIL_PEOPLE: usize = 100;
pub const DETAIL_TOPICS: usize = 12;
pub const DETAIL_SOURCES: usize = 5;
/// Atoms per group sent to the enrichment queries (a transcript has hundreds
/// of chunk atoms; the first ones name its topics).
const ENRICH_ATOMS_PER_GROUP: i32 = 400;
const OUTCOME_MAX_CHARS: usize = 160;
const QUOTE_MAX_CHARS: usize = 280;
const MAX_KEY_LEN: usize = 2048;

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
    "meeting_transcript",
    "transcript",
    "zoom",
    "zoom_transcript",
    "meet",
    "google_meet_transcript",
    "teams_transcript",
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

    /// Every source type that has a card.
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

    /// The first word of a source's `meta_label` ("MEETING · 14:52").
    fn meta_word(self) -> &'static str {
        match self {
            Kind::Meeting => "MEETING",
            Kind::Slack => "SLACK",
            Kind::Agent => "AGENT",
            Kind::Doc => "DOC",
            Kind::Note => "NOTE",
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

// ── Request types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Who {
    Mine,
    Team,
}

impl Who {
    pub fn parse(raw: Option<&str>) -> Result<Who, String> {
        match raw.map(str::trim) {
            None | Some("") | Some("mine") => Ok(Who::Mine),
            Some("team") => Ok(Who::Team),
            Some(other) => Err(format!("who must be mine or team, got \"{other}\"")),
        }
    }
}

/// `[from, to)` from RFC3339 strings: both required, `from < to`, at most
/// [`MAX_WINDOW_DAYS`].
pub fn parse_window(
    from: Option<&str>,
    to: Option<&str>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), String> {
    let parse = |raw: Option<&str>, name: &str| -> Result<DateTime<Utc>, String> {
        let raw = raw.ok_or_else(|| format!("{name} is required"))?;
        DateTime::parse_from_rfc3339(raw.trim())
            .map(|d| d.with_timezone(&Utc))
            .map_err(|_| format!("{name} must be a valid RFC3339 timestamp"))
    };
    let (from, to) = (parse(from, "from")?, parse(to, "to")?);
    if from >= to {
        return Err("from must be before to".into());
    }
    if to - from > Duration::days(MAX_WINDOW_DAYS) {
        return Err(format!(
            "from/to span must not exceed {MAX_WINDOW_DAYS} days"
        ));
    }
    Ok((from, to))
}

/// `tz=` as an IANA zone; `None` when absent or blank.
pub fn parse_tz(raw: Option<&str>) -> Result<Option<Tz>, String> {
    match raw.map(str::trim) {
        None | Some("") => Ok(None),
        Some(z) => z
            .parse::<Tz>()
            .map(Some)
            .map_err(|_| "tz must be an IANA time zone name".to_string()),
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GraphRef {
    pub id: Uuid,
    pub name: String,
}

pub struct ListRequest<'a> {
    pub caller: Uuid,
    pub who: Who,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub tz: Tz,
    /// The oceans the caller may read (already narrowed by `graph_id=`).
    pub graphs: &'a [GraphRef],
}

pub struct DetailRequest<'a> {
    pub key: &'a str,
    pub tz: Tz,
    /// The one ocean asked about; the caller has already been shown to reach it.
    pub graph: &'a GraphRef,
}

// ── Response types ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Owner {
    pub user_id: Option<Uuid>,
    /// Filled by the route from the control plane; `None` until then.
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Topic {
    pub node_id: Uuid,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Activity {
    pub key: String,
    pub kind: Kind,
    pub kind_label: &'static str,
    pub owner: Owner,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub graph: GraphRef,
    pub title: String,
    pub outcome: Option<String>,
    /// Up to [`CARD_PEOPLE`] names (the detail carries up to [`DETAIL_PEOPLE`]).
    pub people: Vec<String>,
    pub people_count: usize,
    pub topics: Vec<Topic>,
    pub decision_count: usize,
    /// Corals (ingested atoms) behind the card.
    pub source_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DayCount {
    pub day: NaiveDate,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActivityList {
    pub activities: Vec<Activity>,
    pub day_activity: Vec<DayCount>,
    /// More than [`MAX_ACTIVITIES`] cards matched; the earliest are returned.
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Source {
    /// The card's kind label ("meeting").
    pub kind: &'static str,
    pub quote: String,
    /// "MEETING · 14:52" (time of day in the request's zone).
    pub meta_label: String,
    pub source_item_id: Option<Uuid>,
    /// The semantic atom the quote supports, or the coral for an excerpt.
    pub atom_id: Option<Uuid>,
    pub occurred_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActivityDetail {
    #[serde(flatten)]
    pub activity: Activity,
    pub decisions: Vec<String>,
    pub sources: Vec<Source>,
    pub reefs: Vec<Topic>,
}

// ── Keys ─────────────────────────────────────────────────────────────────

const KEY_SEP: char = '\u{1f}';

/// Opaque, URL-safe id of one group within an ocean.
pub fn make_key(source_type: &str, grouping_key: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{source_type}{KEY_SEP}{grouping_key}"))
}

/// `(source_type, grouping_key)`; `None` for anything malformed or unmapped.
pub fn parse_key(key: &str) -> Option<(String, String)> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return None;
    }
    let raw = String::from_utf8(URL_SAFE_NO_PAD.decode(key).ok()?).ok()?;
    let (source_type, grouping_key) = raw.split_once(KEY_SEP)?;
    if grouping_key.is_empty() || Kind::from_source_type(source_type).is_none() {
        return None;
    }
    Some((source_type.to_string(), grouping_key.to_string()))
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

/// One atom group (a future card) as the grouping query returns it.
#[derive(Debug, Clone)]
struct Group {
    graph_id: Uuid,
    source_type: String,
    grouping_key: String,
    first: DateTime<Utc>,
    last: DateTime<Utc>,
    atom_count: i64,
    owner: Option<Uuid>,
    any_title: Option<String>,
    session_title: Option<String>,
    channel_name: Option<String>,
    atom_ids: Vec<Uuid>,
    people: Vec<String>,
}

/// What the enrichment queries found for one group.
#[derive(Debug, Clone, Default)]
struct Enrichment {
    /// `(node_id, title, type, hits)` from both the legacy `node_atoms` links
    /// and the semantic `node_semantic_atoms` links.
    nodes: Vec<(Uuid, String, String, i64)>,
    /// Decision-kind semantic atoms of the item's current version, in order.
    semantic_decisions: Vec<String>,
    /// `meeting_transcripts.summary`: `overview` and `decisions[].text`.
    summary_overview: Option<String>,
    summary_decisions: Vec<String>,
    duration_ms: Option<i64>,
}

impl Enrichment {
    /// Decisions, by trust: extracted decision atoms, else the meeting
    /// summary's, else the titles of linked decision-type reefs.
    fn decisions(&self) -> Vec<String> {
        let atoms = dedupe_lines(self.semantic_decisions.iter().cloned());
        if !atoms.is_empty() {
            return atoms;
        }
        let summary = dedupe_lines(self.summary_decisions.iter().cloned());
        if !summary.is_empty() {
            return summary;
        }
        dedupe_lines(
            self.nodes
                .iter()
                .filter(|n| n.2 == "decision")
                .map(|n| n.1.clone()),
        )
    }

    /// Non-decision reefs, most-linked first.
    fn topics(&self) -> Vec<Topic> {
        let mut by_node: BTreeMap<Uuid, (String, i64)> = BTreeMap::new();
        for (id, title, ty, hits) in &self.nodes {
            if ty == "decision" || title.trim().is_empty() {
                continue;
            }
            let e = by_node.entry(*id).or_insert_with(|| (title.clone(), 0));
            e.1 += hits;
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
}

fn is_session(source_type: &str) -> bool {
    crate::ingest::atom::is_session_source_type(source_type)
}

fn build_activity(
    g: &Group,
    e: &Enrichment,
    graph: &GraphRef,
    people_cap: usize,
    topics_cap: usize,
) -> Activity {
    let kind = Kind::from_source_type(&g.source_type).unwrap_or(Kind::Doc);
    let clean = |s: &Option<String>| {
        s.as_deref()
            .map(|t| clean_line(t, 120))
            .filter(|t| !t.is_empty())
    };
    let title = (if is_session(&g.source_type) {
        clean(&g.session_title)
    } else {
        None
    })
    .or_else(|| clean(&g.any_title))
    .or_else(|| {
        (kind == Kind::Slack)
            .then(|| {
                clean(&g.channel_name).map(|c| {
                    if c.starts_with('#') {
                        c
                    } else {
                        format!("#{c}")
                    }
                })
            })
            .flatten()
    })
    .unwrap_or_else(|| kind.default_title().to_string());

    // A meeting ends when its recording does; everything else at its last atom.
    let end = match (kind, e.duration_ms) {
        (Kind::Meeting, Some(ms)) if ms > 0 => (g.first + Duration::milliseconds(ms)).max(g.last),
        _ => g.last,
    };

    let decisions = e.decisions();
    let outcome = e
        .summary_overview
        .as_deref()
        .map(first_sentence)
        .filter(|s| !s.is_empty())
        .or_else(|| decisions.first().map(|d| clean_line(d, OUTCOME_MAX_CHARS)));

    let people = dedupe_people(&g.people);
    Activity {
        key: make_key(&g.source_type, &g.grouping_key),
        kind,
        kind_label: kind.label(),
        owner: Owner {
            user_id: g.owner,
            name: None,
        },
        start: g.first,
        end,
        graph: graph.clone(),
        title,
        outcome,
        people_count: people.len(),
        people: people.into_iter().take(people_cap).collect(),
        topics: e.topics().into_iter().take(topics_cap).collect(),
        decision_count: decisions.len(),
        source_count: g.atom_count,
    }
}

/// `"MEETING · 14:52"` for `at` in `tz`.
fn meta_label(kind: Kind, at: DateTime<Utc>, tz: Tz) -> String {
    format!(
        "{} · {}",
        kind.meta_word(),
        at.with_timezone(&tz).format("%H:%M")
    )
}

/// Passages of a version's atoms as sources: decisions first, then document
/// order; the first passage of each atom; at most `cap`.
fn pick_sources(
    kind: Kind,
    atoms: &[Value],
    source_item_id: Uuid,
    base: DateTime<Utc>,
    tz: Tz,
    cap: usize,
) -> Vec<Source> {
    let mut ranked: Vec<(bool, i64, &Value)> = atoms
        .iter()
        .map(|a| {
            (
                a["kind"] != "decision",
                a["ordinal"].as_i64().unwrap_or(i64::MAX),
                a,
            )
        })
        .collect();
    ranked.sort_by_key(|(not_decision, ordinal, _)| (*not_decision, *ordinal));
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (_, _, atom) in ranked {
        let Some(passage) = atom["passages"].as_array().and_then(|p| p.first()) else {
            continue;
        };
        let quote = clean_line(passage["quote"].as_str().unwrap_or(""), QUOTE_MAX_CHARS);
        if quote.is_empty() || !seen.insert(quote.to_lowercase()) {
            continue;
        }
        let at = match (kind, passage["start_ms"].as_i64()) {
            (Kind::Meeting, Some(ms)) if ms >= 0 => base + Duration::milliseconds(ms),
            _ => base,
        };
        out.push(Source {
            kind: kind.label(),
            quote,
            meta_label: meta_label(kind, at, tz),
            source_item_id: Some(source_item_id),
            atom_id: atom["atom_id"].as_str().and_then(|s| s.parse().ok()),
            occurred_at: Some(at),
        });
        if out.len() >= cap {
            break;
        }
    }
    out
}

/// The 14 local days ending with the day that holds the instant just before
/// `to`: `(first local day, first instant of it in UTC)`.
fn strip_window(to: DateTime<Utc>, tz: Tz) -> (NaiveDate, DateTime<Utc>) {
    let last_day = (to - Duration::seconds(1)).with_timezone(&tz).date_naive();
    let first_day = last_day - Duration::days(STRIP_DAYS - 1);
    let start = tz
        .from_local_datetime(&first_day.and_hms_opt(0, 0, 0).expect("midnight"))
        .earliest()
        // A zone with no local midnight that day (a DST gap at 00:00): one
        // hour later is the first instant of the day.
        .or_else(|| {
            tz.from_local_datetime(&first_day.and_hms_opt(1, 0, 0).expect("01:00"))
                .earliest()
        })
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|| first_day.and_hms_opt(0, 0, 0).expect("midnight").and_utc());
    (first_day, start)
}

/// All [`STRIP_DAYS`] days, ascending, quiet days as 0.
fn fill_strip(first_day: NaiveDate, counts: &HashMap<NaiveDate, i64>) -> Vec<DayCount> {
    (0..STRIP_DAYS)
        .map(|i| {
            let day = first_day + Duration::days(i);
            DayCount {
                day,
                count: counts.get(&day).copied().unwrap_or(0),
            }
        })
        .collect()
}

/// The date strip with no activity (nothing readable, or no pool).
pub fn empty_strip(to: DateTime<Utc>, tz: Tz) -> Vec<DayCount> {
    fill_strip(strip_window(to, tz).0, &HashMap::new())
}

fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

// ── SQL ──────────────────────────────────────────────────────────────────

/// `grouped`: one row per in-scope, ADMITTED atom with its grouping key.
/// Parameters (shared by the list, the strip and the detail query):
/// `$1` ocean ids, `$2`/`$3` window (NULL = unbounded), `$4` source types,
/// `$7` group key (NULL = every group) with `$8`/`$9` its `LIKE` prefixes.
///
/// Admission is the Library's own, applied BEFORE grouping so a card, its
/// title, channel, people and counts only ever come from atoms the reader may
/// see: `win` is the candidate atoms (history floor applied), `per_graph`
/// their ids per ocean, and `grouped` keeps what
/// `library_permitted_atoms(graph, ids)` admits (retracted atoms, derived
/// copies and atoms of restricted or unknown source items are out). The
/// old hour aggregate was safe only because it filtered on `ingested_by`;
/// Team has no such filter, so this is not optional. Cost: it runs over the
/// window's candidate atoms (one set-returning call per ocean), so a Team
/// request costs in proportion to the window's atoms, not the number of cards.
///
/// `keyed` (the detail read) splits the key match into one index-friendly
/// branch per form (`= $7`, `LIKE $8`, `LIKE $9`) instead of an `OR`, each
/// using the `(graph_id, source_id text_pattern_ops)` index.
fn grouped_cte(keyed: bool) -> String {
    let branch = |extra: &str| {
        format!(
            r#"SELECT a.atom_id, a.graph_id, a.source_type, a.ingested_by,
                   a."timestamp" AS ts, a.participants, a.author_name,
                   a.title, a.channel_name, cs.title AS session_title,
                   {grouping_key} AS grouping_key
            FROM atoms a
            {session_join}
            WHERE a.graph_id = ANY($1)
              AND ($2::timestamptz IS NULL OR a."timestamp" >= $2)
              AND ($3::timestamptz IS NULL OR a."timestamp" < $3)
              AND a.source_type = ANY($4)
              AND {extra}
              AND {floor}"#,
            grouping_key = crate::journal_grouping::grouping_key_sql(),
            session_join = crate::journal_grouping::session_join_sql(),
            floor = crate::history_floor::admits_atom("a"),
        )
    };
    let candidates = if keyed {
        [
            branch("a.source_id = $7"),
            branch("a.source_id LIKE $8"),
            branch("a.source_id LIKE $9"),
        ]
        .join("\n UNION ALL \n")
    } else {
        branch("($7::text IS NULL AND $8::text IS NULL AND $9::text IS NULL)")
    };
    format!(
        r#"win AS MATERIALIZED (
            SELECT DISTINCT ON (u.atom_id) u.* FROM ({candidates}) u ORDER BY u.atom_id
        ),
        per_graph AS (
            SELECT graph_id, ARRAY_AGG(atom_id) AS ids FROM win GROUP BY graph_id
        ),
        grouped AS MATERIALIZED (
            SELECT w.*
            FROM per_graph pg
            CROSS JOIN LATERAL public.library_permitted_atoms(pg.graph_id,
                CASE WHEN cardinality(pg.ids)>10000 THEN NULL::uuid[] ELSE pg.ids END) p
            JOIN win w ON w.graph_id = pg.graph_id AND w.atom_id = p.atom_id
        )"#
    )
}

/// Groups of the window (or of one key), earliest first. `$5` narrows to
/// groups that hold an atom the caller ingested; `$6` is the row limit.
fn groups_sql(keyed: bool) -> String {
    format!(
        r#"WITH {grouped},
        people AS (
            SELECT g.graph_id, g.source_type, g.grouping_key,
                   ARRAY_AGG(DISTINCT p ORDER BY p) AS names
            FROM grouped g
            CROSS JOIN LATERAL unnest(g.participants || ARRAY[g.author_name]) AS p
            WHERE ($7::text IS NULL OR g.grouping_key = $7) AND p IS NOT NULL AND btrim(p) <> ''
            GROUP BY g.graph_id, g.source_type, g.grouping_key
        ),
        grp AS (
            SELECT g.graph_id, g.source_type, g.grouping_key,
                   MIN(g.ts) AS first_ts, MAX(g.ts) AS last_ts, COUNT(*)::bigint AS atom_count,
                   (ARRAY_AGG(g.ingested_by ORDER BY g.ts, g.atom_id)
                        FILTER (WHERE g.ingested_by IS NOT NULL))[1] AS owner,
                   MAX(g.title) AS any_title, MAX(g.session_title) AS session_title,
                   MAX(g.channel_name) AS channel_name,
                   (ARRAY_AGG(g.atom_id ORDER BY g.ts, g.atom_id))[1:{per_group}] AS atom_ids
            FROM grouped g
            WHERE $7::text IS NULL OR g.grouping_key = $7
            GROUP BY g.graph_id, g.source_type, g.grouping_key
            HAVING $5::uuid IS NULL OR BOOL_OR(g.ingested_by = $5)
        )
        SELECT grp.graph_id, grp.source_type, grp.grouping_key, grp.first_ts, grp.last_ts,
               grp.atom_count, grp.owner, grp.any_title, grp.session_title, grp.channel_name,
               grp.atom_ids, COALESCE(pe.names, '{{}}'::text[]) AS names
        FROM grp
        LEFT JOIN people pe USING (graph_id, source_type, grouping_key)
        ORDER BY grp.first_ts ASC, grp.graph_id ASC, grp.grouping_key ASC
        LIMIT $6"#,
        grouped = grouped_cte(keyed),
        per_group = ENRICH_ATOMS_PER_GROUP,
    )
}

/// Cards per local day: `$5` mine, `$6` row limit, `$10` the zone; the group
/// parameters `$7`-`$9` are bound NULL (every group).
fn strip_sql() -> String {
    format!(
        r#"WITH {grouped},
        grp AS (
            SELECT MIN(g.ts) AS first_ts
            FROM grouped g
            GROUP BY g.graph_id, g.source_type, g.grouping_key
            HAVING $5::uuid IS NULL OR BOOL_OR(g.ingested_by = $5)
        )
        SELECT (first_ts AT TIME ZONE $10)::date AS day, COUNT(*)::bigint
        FROM grp GROUP BY 1 ORDER BY 1 LIMIT $6"#,
        grouped = grouped_cte(false),
    )
}

/// `(what, group index, text, node id, node type, n)`: a decision statement or
/// a linked reef of a group's current version.
type SemanticRow = (
    String,
    i32,
    Option<String>,
    Option<Uuid>,
    Option<String>,
    i64,
);
/// `(group index, summary overview, summary decisions, duration_ms)`.
type MeetingRow = (i32, Option<String>, Option<Value>, Option<i64>);
/// `(atom id, timestamp, excerpt, head of content_inline)`.
type ExcerptRow = (Uuid, DateTime<Utc>, Option<String>, Option<String>);

type GroupRow = (
    Uuid,
    String,
    String,
    DateTime<Utc>,
    DateTime<Utc>,
    i64,
    Option<Uuid>,
    Option<String>,
    Option<String>,
    Option<String>,
    Vec<Uuid>,
    Vec<String>,
);

#[allow(clippy::too_many_arguments)]
async fn load_groups(
    tx: &mut Transaction<'_, Postgres>,
    graph_ids: &[Uuid],
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    types: &[String],
    mine: Option<Uuid>,
    limit: i64,
    key: Option<(&str, &str)>,
) -> Result<Vec<Group>, sqlx::Error> {
    let (k, like_chunk, like_session) = match key {
        Some((_, gk)) => {
            let e = escape_like(gk);
            (
                Some(gk.to_string()),
                Some(format!("{e}:chunk:%")),
                Some(format!("cc:{e}:%")),
            )
        }
        None => (None, None, None),
    };
    let rows: Vec<GroupRow> = sqlx::query_as(&groups_sql(key.is_some()))
        .bind(graph_ids)
        .bind(from)
        .bind(to)
        .bind(types)
        .bind(mine)
        .bind(limit)
        .bind(k)
        .bind(like_chunk)
        .bind(like_session)
        .fetch_all(&mut **tx)
        .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                graph_id,
                source_type,
                grouping_key,
                first,
                last,
                atom_count,
                owner,
                any_title,
                session_title,
                channel_name,
                atom_ids,
                people,
            )| {
                Group {
                    graph_id,
                    source_type,
                    grouping_key,
                    first,
                    last,
                    atom_count,
                    owner,
                    any_title,
                    session_title,
                    channel_name,
                    atom_ids,
                    people,
                }
            },
        )
        .collect())
}

/// Bulk equivalent of `library_node_title`, including reef identity corrections.
///
/// Callers pass parallel (graph,node) pairs from their verified graph set. This
/// reads all same-graph evidence of those nodes, rather than just the activity
/// window's sampled atoms. The authoritative 0137 title predicate has two base
/// witnesses: a semantic entity mention on a permitted source, or a linked
/// Library-admitted atom containing the label/participant. 0157 adds an active
/// supported correction. Every non-NULL title witness also proves
/// `library_node_permitted`, so no second scalar permission call is needed.
///
/// Semantic support uses served_semantic_atoms: unlike 0137's old raw branch,
/// pre-floor or superseded extraction evidence cannot certify a stored title.
/// This is deliberately stricter for those unsafe legacy cases; for admitted
/// current evidence the same semantic mention predicate applies. Legacy floors
/// remain inside library_permitted_atoms, correction floors remain inside
/// reef_identity_support_valid, and all RLS stays active.
/// No pool or transaction is opened here.
pub async fn project_node_titles(
    tx: &mut Transaction<'_, Postgres>,
    graphs: &[Uuid],
    nodes: &[Uuid],
) -> Result<HashMap<(Uuid, Uuid), String>, sqlx::Error> {
    project_node_titles_on(&mut **tx, graphs, nodes).await
}

/// Same admitted projection for pure compute helpers holding a connection.
/// The caller owns the readonly snapshot and its actor/graph scope.
pub async fn project_node_titles_on(
    tx: &mut sqlx::PgConnection,
    graphs: &[Uuid],
    nodes: &[Uuid],
) -> Result<HashMap<(Uuid, Uuid), String>, sqlx::Error> {
    if graphs.len() != nodes.len() {
        return Err(sqlx::Error::Protocol(
            "mismatched title candidate arrays".into(),
        ));
    }
    if nodes.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(Uuid, Uuid, String)> = sqlx::query_as(PROJECT_NODE_TITLES_SQL)
        .bind(graphs)
        .bind(nodes)
        .fetch_all(&mut *tx)
        .await?;
    Ok(rows.into_iter().map(|(g, n, t)| ((g, n), t)).collect())
}

/// Set expansion of tenant 0137 / 0157, with the 0204 served-semantic boundary.
/// Keep equality tests against public title/permitted functions for current
/// admitted evidence, and explicit negative checks for pre-floor/old extraction
/// evidence which the old scalar semantic branch does not exclude. Legacy
/// name witnesses are filtered before admission: admitting unrelated linked
/// atoms cannot change whether any admitted label witness exists. Above 1,000
/// witness ids, use the same Library whole-graph admission arm and join back
/// to those witnesses; this is query-plan selection, never an evidence cap.
pub const PROJECT_NODE_TITLES_SQL: &str = r#"
WITH candidates AS MATERIALIZED (
  SELECT DISTINCT n.graph_id,n.node_id,n.title
  FROM unnest($1::uuid[],$2::uuid[]) p(graph_id,node_id)
  JOIN nodes n ON n.graph_id=p.graph_id AND n.node_id=p.node_id
), linked AS MATERIALIZED (
  SELECT n.graph_id,n.node_id,n.title,a.atom_id
  FROM candidates n JOIN node_atoms l ON l.node_id=n.node_id
  JOIN atoms a ON a.graph_id=n.graph_id AND a.atom_id=l.atom_id
  WHERE n.title<>'' AND
    (position(lower(n.title) IN lower(COALESCE(a.content_inline,'')))>0
     OR EXISTS(SELECT 1 FROM unnest(a.participants) participant
               WHERE lower(participant)=lower(n.title)))
), per_graph AS (
  SELECT graph_id,array_agg(DISTINCT atom_id) ids FROM linked GROUP BY graph_id
), admitted AS MATERIALIZED (
  SELECT g.graph_id,p.atom_id FROM per_graph g
  CROSS JOIN LATERAL library_permitted_atoms(g.graph_id,
    CASE WHEN cardinality(g.ids)>1000 THEN NULL::uuid[] ELSE g.ids END) p
), legacy_named AS MATERIALIZED (
  SELECT DISTINCT l.graph_id,l.node_id
  FROM linked l JOIN admitted p ON p.graph_id=l.graph_id AND p.atom_id=l.atom_id
), semantic_named AS MATERIALIZED (
  SELECT DISTINCT n.graph_id,n.node_id
  FROM candidates n
  JOIN node_semantic_atoms l ON l.graph_id=n.graph_id AND l.node_id=n.node_id
  JOIN served_semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(a.entities)='array' THEN a.entities ELSE '[]'::jsonb END) e
  WHERE lower(e->>'mention')=lower(n.title)
), base_named AS MATERIALIZED (
  SELECT graph_id,node_id FROM legacy_named
  UNION SELECT graph_id,node_id FROM semantic_named
), corrected_named AS MATERIALIZED (
  SELECT DISTINCT n.graph_id,n.node_id
  FROM candidates n JOIN reef_identity_corrections c
    ON c.graph_id=n.graph_id AND c.node_id=n.node_id
  WHERE NOT EXISTS(SELECT 1 FROM base_named b
                   WHERE b.graph_id=n.graph_id AND b.node_id=n.node_id)
    AND c.state='active' AND c.label=n.title AND reef_identity_support_valid(c)
), named AS (
  SELECT graph_id,node_id FROM base_named
  UNION SELECT graph_id,node_id FROM corrected_named
)
SELECT n.graph_id,n.node_id,substring(n.title FOR 256)
FROM candidates n JOIN named t ON t.graph_id=n.graph_id AND t.node_id=n.node_id
"#;

/// Reefs, decisions, meeting summary and duration for `groups`.
async fn enrich(
    tx: &mut Transaction<'_, Postgres>,
    groups: &[Group],
    graph_ids: &[Uuid],
) -> Result<Vec<Enrichment>, sqlx::Error> {
    let mut out = vec![Enrichment::default(); groups.len()];
    if groups.is_empty() {
        return Ok(out);
    }
    let (mut idx, mut atoms) = (Vec::<i32>::new(), Vec::<Uuid>::new());
    let (mut gidx, mut ggraph, mut gkey) =
        (Vec::<i32>::new(), Vec::<Uuid>::new(), Vec::<String>::new());
    for (i, g) in groups.iter().enumerate() {
        gidx.push(i as i32);
        ggraph.push(g.graph_id);
        gkey.push(g.grouping_key.clone());
        for a in &g.atom_ids {
            idx.push(i as i32);
            atoms.push(*a);
        }
    }

    // Keep every hit until titles have been filtered and semantic/legacy hits
    // merged. A pre-title cap can lose visible topics behind hidden labels.
    let legacy: Vec<(i32, Uuid, Uuid, String, i64)> = sqlx::query_as(
        r#"SELECT t.gidx,n.graph_id,n.node_id,n.type,COUNT(DISTINCT t.atom_id)::bigint
           FROM unnest($1::int4[],$2::uuid[]) t(gidx,atom_id)
           JOIN node_atoms na ON na.atom_id=t.atom_id
           JOIN nodes n ON n.node_id=na.node_id
           WHERE n.graph_id=ANY($3)
           GROUP BY t.gidx,n.graph_id,n.node_id,n.type"#,
    )
    .bind(&idx)
    .bind(&atoms)
    .bind(graph_ids)
    .fetch_all(&mut **tx)
    .await?;

    // The item behind each group (its source_id is the grouping key): decision
    // atoms and reef links of its current version, through served_semantic_atoms
    // so corrections and the history floor apply.
    let item_floor = crate::history_floor::admits_source_item("si");
    let semantic: Vec<SemanticRow> = sqlx::query_as(&format!(
        r#"WITH g AS (SELECT * FROM unnest($1::int4[], $2::uuid[], $3::text[]) AS g(gidx, graph_id, key)),
           cur AS (
               SELECT DISTINCT g.gidx, si.graph_id, si.current_version_id AS vid
               FROM g JOIN source_items si ON si.graph_id = g.graph_id AND si.source_id = g.key
               WHERE si.access_state = 'permitted' AND si.current_version_id IS NOT NULL AND {item_floor}
           )
           SELECT 'decision'::text, cur.gidx, sa.statement, NULL::uuid, NULL::text, sa.ordinal::bigint
           FROM cur JOIN served_semantic_atoms sa ON sa.graph_id = cur.graph_id AND sa.source_version_id = cur.vid
           WHERE sa.kind = 'decision'
           UNION ALL
           SELECT 'node'::text, cur.gidx, NULL::text, n.node_id, n.type, COUNT(*)::bigint
           FROM cur
           JOIN served_semantic_atoms sa ON sa.graph_id = cur.graph_id AND sa.source_version_id = cur.vid
           JOIN node_semantic_atoms ns ON ns.semantic_atom_id = sa.semantic_atom_id AND ns.graph_id = sa.graph_id
           JOIN nodes n ON n.node_id = ns.node_id AND n.graph_id = ns.graph_id
           GROUP BY cur.gidx, n.graph_id, n.node_id, n.type
           ORDER BY 2, 6"#
    ))
    .bind(&gidx)
    .bind(&ggraph)
    .bind(&gkey)
    .fetch_all(&mut **tx)
    .await?;
    // Project the union once: a reef linked by many groups or both storage
    // representations must not repeat scalar admission/title work.
    let candidates: BTreeSet<(Uuid, Uuid)> = legacy
        .iter()
        .map(|(_, graph, node, _, _)| (*graph, *node))
        .chain(semantic.iter().filter_map(|(_, i, _, node, _, _)| {
            node.map(|node| (groups[*i as usize].graph_id, node))
        }))
        .collect();
    let (title_graphs, title_nodes): (Vec<_>, Vec<_>) = candidates.into_iter().unzip();
    let titles = project_node_titles(tx, &title_graphs, &title_nodes).await?;
    for (i, graph, node, ty, hits) in legacy {
        if let Some(title) = titles.get(&(graph, node)) {
            out[i as usize].nodes.push((node, title.clone(), ty, hits));
        }
    }
    for (what, i, text, node, ty, n) in semantic {
        let slot = &mut out[i as usize];
        match (what.as_str(), text, node, ty) {
            ("decision", Some(statement), _, _) => slot.semantic_decisions.push(statement),
            ("node", _, Some(node), Some(ty)) => {
                if let Some(title) = titles.get(&(groups[i as usize].graph_id, node)) {
                    slot.nodes.push((node, title.clone(), ty, n));
                }
            }
            _ => {}
        }
    }

    // Meeting summary and recorded duration.
    let transcript_floor = crate::history_floor::admits_transcript("mt");
    let meetings: Vec<MeetingRow> = sqlx::query_as(&format!(
        r#"WITH t AS MATERIALIZED (
             SELECT g.gidx,g.graph_id,g.key,mt.summary,mt.duration_ms
             FROM unnest($1::int4[], $2::uuid[], $3::text[]) AS g(gidx, graph_id, key)
             JOIN meeting_transcripts mt ON mt.graph_id=g.graph_id AND mt.source_id=g.key
             WHERE {transcript_floor}
           ), inputs AS MATERIALIZED (
             SELECT t.gidx,a.graph_id,a.atom_id FROM t JOIN atoms a
             ON a.graph_id=t.graph_id AND (a.source_id=t.key OR
                (a.source_id ~>=~ (t.key||':chunk:') AND a.source_id ~<~ (t.key||':chunk;')))
           ), per_graph AS (
             SELECT graph_id,array_agg(DISTINCT atom_id) ids FROM inputs GROUP BY graph_id
           ), admitted AS MATERIALIZED (
             SELECT g.graph_id,p.atom_id FROM per_graph g
             CROSS JOIN LATERAL library_permitted_atoms(g.graph_id,
                CASE WHEN cardinality(g.ids)>10000 THEN NULL::uuid[] ELSE g.ids END) p
           ), denied AS (
             SELECT DISTINCT i.gidx FROM inputs i LEFT JOIN admitted p
                ON p.graph_id=i.graph_id AND p.atom_id=i.atom_id WHERE p.atom_id IS NULL
           )
           SELECT t.gidx,
             CASE WHEN d.gidx IS NULL THEN t.summary->>'overview' END,
             CASE WHEN d.gidx IS NULL THEN t.summary->'decisions' END,t.duration_ms
           FROM t LEFT JOIN denied d ON d.gidx=t.gidx"#
    ))
    .bind(&gidx)
    .bind(&ggraph)
    .bind(&gkey)
    .fetch_all(&mut **tx)
    .await?;
    for (i, overview, decisions, duration_ms) in meetings {
        let slot = &mut out[i as usize];
        slot.summary_overview = overview.filter(|o| !o.trim().is_empty());
        slot.duration_ms = duration_ms;
        if let Some(Value::Array(items)) = decisions {
            slot.summary_decisions = items
                .iter()
                .filter_map(|d| d.get("text").and_then(Value::as_str).or_else(|| d.as_str()))
                .map(str::to_string)
                .collect();
        }
    }
    Ok(out)
}

// ── Entry points ─────────────────────────────────────────────────────────

/// The window's cards, earliest first, plus the date strip.
pub async fn list_activities(
    tx: &mut Transaction<'_, Postgres>,
    req: &ListRequest<'_>,
) -> Result<ActivityList, sqlx::Error> {
    if matches!(req.graphs, []) {
        let (first_day, _) = strip_window(req.to, req.tz);
        return Ok(ActivityList {
            activities: vec![],
            day_activity: fill_strip(first_day, &HashMap::new()),
            truncated: false,
        });
    }
    let graph_ids: Vec<Uuid> = req.graphs.iter().map(|g| g.id).collect();
    let names: HashMap<Uuid, &GraphRef> = req.graphs.iter().map(|g| (g.id, g)).collect();
    let types = Kind::all_source_types();
    let mine = (req.who == Who::Mine).then_some(req.caller);

    let mut groups = load_groups(
        tx,
        &graph_ids,
        Some(req.from),
        Some(req.to),
        &types,
        mine,
        MAX_ACTIVITIES + 1,
        None,
    )
    .await?;
    let truncated = groups.len() as i64 > MAX_ACTIVITIES;
    groups.truncate(MAX_ACTIVITIES as usize);
    let enriched = enrich(tx, &groups, &graph_ids).await?;
    let activities = groups
        .iter()
        .zip(&enriched)
        .filter_map(|(g, e)| {
            names
                .get(&g.graph_id)
                .map(|graph| build_activity(g, e, graph, CARD_PEOPLE, CARD_TOPICS))
        })
        .collect();

    let day_activity = list_strip(tx, req).await?;
    Ok(ActivityList {
        activities,
        day_activity,
        truncated,
    })
}

/// Date-strip computation without enriching activity cards or resolving titles.
pub async fn list_strip(
    tx: &mut Transaction<'_, Postgres>,
    req: &ListRequest<'_>,
) -> Result<Vec<DayCount>, sqlx::Error> {
    let (first_day, strip_from) = strip_window(req.to, req.tz);
    let rows: Vec<(NaiveDate, i64)> = sqlx::query_as(&strip_sql())
        .bind(req.graphs.iter().map(|g| g.id).collect::<Vec<_>>())
        .bind(strip_from)
        .bind(req.to)
        .bind(Kind::all_source_types())
        .bind((req.who == Who::Mine).then_some(req.caller))
        .bind(STRIP_DAYS)
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(req.tz.name())
        .fetch_all(&mut **tx)
        .await?;
    let counts: HashMap<NaiveDate, i64> = rows.into_iter().collect();
    Ok(fill_strip(first_day, &counts))
}

/// One card in full. `None` when the key is malformed or the group has no
/// atoms the caller may read in that ocean.
pub async fn activity_detail(
    tx: &mut Transaction<'_, Postgres>,
    req: &DetailRequest<'_>,
) -> Result<Option<ActivityDetail>, sqlx::Error> {
    let Some((source_type, grouping_key)) = parse_key(req.key) else {
        return Ok(None);
    };
    let graph_ids = [req.graph.id];
    let types = [source_type.clone()];
    let mut groups = load_groups(
        tx,
        &graph_ids,
        None,
        None,
        &types,
        None,
        1,
        Some((&source_type, &grouping_key)),
    )
    .await?;
    let Some(group) = groups.pop() else {
        return Ok(None);
    };
    let enriched = enrich(tx, std::slice::from_ref(&group), &graph_ids)
        .await?
        .pop()
        .unwrap_or_default();
    let mut activity = build_activity(&group, &enriched, req.graph, DETAIL_PEOPLE, DETAIL_TOPICS);
    let kind = activity.kind;
    let reefs = std::mem::take(&mut activity.topics);
    activity.topics = reefs.iter().take(CARD_TOPICS).cloned().collect();

    // Evidence: passages of the item's current version through the same
    // building block the source-evidence reader serves them with.
    let item_floor = crate::history_floor::admits_source_item("s");
    let item: Option<(Uuid, Uuid)> = sqlx::query_as(&format!(
        "SELECT s.source_item_id, s.current_version_id FROM source_items s \
         WHERE s.graph_id = $1 AND s.source_id = $2 AND s.access_state = 'permitted' \
           AND s.current_version_id IS NOT NULL AND {item_floor} \
         ORDER BY s.updated_at DESC, s.source_item_id LIMIT 1"
    ))
    .bind(req.graph.id)
    .bind(&grouping_key)
    .fetch_optional(&mut **tx)
    .await?;
    let mut sources = Vec::new();
    if let Some((source_item_id, version_id)) = item {
        let atoms = crate::source_evidence::atom_json(tx, req.graph.id, version_id, -1).await?;
        sources = pick_sources(
            kind,
            &atoms,
            source_item_id,
            group.first,
            req.tz,
            DETAIL_SOURCES,
        );
    }
    if sources.len() < DETAIL_SOURCES {
        // Not extracted yet (or no passages): the corals' own excerpts.
        let floor = crate::history_floor::admits_atom("a");
        let rows: Vec<ExcerptRow> = sqlx::query_as(&format!(
            "SELECT a.atom_id, a.\"timestamp\", a.excerpt, left(a.content_inline, 600) FROM atoms a \
             WHERE a.graph_id = $1 AND a.atom_id = ANY($2) AND {floor} \
               AND a.atom_id IN (SELECT atom_id FROM public.library_permitted_atoms($1, $2)) \
             ORDER BY a.\"timestamp\", a.atom_id"
        ))
        .bind(req.graph.id)
        .bind(&group.atom_ids)
        .fetch_all(&mut **tx)
        .await?;
        let mut seen: HashSet<String> = sources.iter().map(|s| s.quote.to_lowercase()).collect();
        for (atom_id, at, excerpt, inline) in rows {
            if sources.len() >= DETAIL_SOURCES {
                break;
            }
            let quote = clean_line(
                excerpt.as_deref().or(inline.as_deref()).unwrap_or(""),
                QUOTE_MAX_CHARS,
            );
            if quote.is_empty() || !seen.insert(quote.to_lowercase()) {
                continue;
            }
            sources.push(Source {
                kind: kind.label(),
                quote,
                meta_label: meta_label(kind, at, req.tz),
                source_item_id: None,
                atom_id: Some(atom_id),
                occurred_at: Some(at),
            });
        }
    }

    Ok(Some(ActivityDetail {
        activity,
        decisions: enriched.decisions(),
        sources,
        reefs,
    }))
}

/// The oceans of `graphs` that belong to `account` (the request's account):
/// reachability spans every account a user is in on the consumer plane, and
/// Today shows the current one only.
pub async fn narrow_to_account(
    tx: &mut Transaction<'_, Postgres>,
    graphs: Vec<GraphRef>,
    account: Uuid,
) -> Result<Vec<GraphRef>, sqlx::Error> {
    if matches!(graphs.as_slice(), []) {
        return Ok(graphs);
    }
    let ids: Vec<Uuid> = graphs.iter().map(|g| g.id).collect();
    let keep: HashSet<Uuid> =
        sqlx::query_scalar("SELECT id FROM graphs WHERE id = ANY($1) AND account_id = $2")
            .bind(&ids)
            .bind(account)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .collect();
    Ok(graphs
        .into_iter()
        .filter(|g| keep.contains(&g.id))
        .collect())
}

/// Name for an owner row: `users.name`, else the email's local part.
pub fn display_name(name: Option<&str>, email: Option<&str>) -> Option<String> {
    name.map(str::trim)
        .filter(|n| !n.is_empty())
        .map(str::to_string)
        .or_else(|| {
            email
                .and_then(|e| e.split('@').next())
                .filter(|l| !l.is_empty())
                .map(str::to_string)
        })
}

/// Control-plane `users` names for `ids`; `db` is the control database.
pub async fn resolve_owner_names<'e, E: sqlx::PgExecutor<'e>>(
    db: E,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, String>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(Uuid, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT id, name, email FROM users WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(db)
            .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, name, email)| {
            display_name(name.as_deref(), email.as_deref()).map(|n| (id, n))
        })
        .collect())
}

impl Activity {
    /// Set `owner.name` from `names`; an owner the control plane does not know
    /// stays `None`.
    pub fn apply_names(&mut self, names: &HashMap<Uuid, String>) {
        if let Some(id) = self.owner.user_id {
            self.owner.name = names.get(&id).cloned();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn graph() -> GraphRef {
        GraphRef {
            id: Uuid::nil(),
            name: "Private".into(),
        }
    }

    fn group(source_type: &str, key: &str) -> Group {
        Group {
            graph_id: Uuid::nil(),
            source_type: source_type.into(),
            grouping_key: key.into(),
            first: ts("2026-10-29T09:00:00Z"),
            last: ts("2026-10-29T09:30:00Z"),
            atom_count: 3,
            owner: None,
            any_title: None,
            session_title: None,
            channel_name: None,
            atom_ids: vec![],
            people: vec![],
        }
    }

    #[test]
    fn kind_mapping_is_the_documented_table() {
        for t in [
            "meeting",
            "meeting_session",
            "transcript",
            "meeting_transcript",
            "zoom_transcript",
            "google_meet_transcript",
            "slack_huddle_summary",
            "zoom",
            "meet",
            "teams_transcript",
        ] {
            assert_eq!(Kind::from_source_type(t), Some(Kind::Meeting), "{t}");
        }
        for t in ["slack", "teams_chat", "teams_channel", "chat"] {
            assert_eq!(Kind::from_source_type(t), Some(Kind::Slack), "{t}");
        }
        for t in ["coding_session", "agent_session", "mcp"] {
            assert_eq!(Kind::from_source_type(t), Some(Kind::Agent), "{t}");
        }
        for t in ["document", "gdrive", "notion"] {
            assert_eq!(Kind::from_source_type(t), Some(Kind::Doc), "{t}");
        }
        assert_eq!(Kind::from_source_type("note"), Some(Kind::Note));
        assert_eq!(Kind::from_source_type("gmail"), None);
        assert_eq!(Kind::Slack.label(), "slack thread");
    }

    #[test]
    fn every_session_source_type_has_a_card_and_no_type_has_two() {
        // A session atom the hour rows group must not vanish from the cards.
        for t in [
            "coding_session",
            "meeting_session",
            "agent_session",
            "work_session",
        ] {
            assert!(crate::ingest::atom::is_session_source_type(t));
            assert!(Kind::from_source_type(t).is_some(), "{t} has no kind");
        }
        let all = Kind::all_source_types();
        let unique: HashSet<&String> = all.iter().collect();
        assert_eq!(all.len(), unique.len(), "a source type maps to two kinds");
    }

    #[test]
    fn grouping_sql_is_the_one_the_hour_aggregate_uses() {
        let sql = groups_sql(false);
        assert!(sql.contains(&crate::journal_grouping::grouping_key_sql()));
        assert!(sql.contains(&crate::journal_grouping::session_join_sql()));
        assert!(
            sql.contains("public.history_floor_admits"),
            "the team-seat floor is applied"
        );
        // The hour aggregate filters on the author; the cards must not.
        assert!(!sql.contains("a.ingested_by = $"));
    }

    #[test]
    fn window_is_validated() {
        assert!(parse_window(Some("2026-10-01T00:00:00Z"), Some("2026-10-31T00:00:00Z")).is_ok());
        assert!(parse_window(Some("2026-10-01T00:00:00Z"), Some("2026-12-02T00:00:00Z")).is_ok()); // 62 days
        assert!(
            parse_window(Some("2026-10-01T00:00:00Z"), Some("2026-12-03T00:00:00Z"))
                .unwrap_err()
                .contains("62 days")
        );
        assert!(
            parse_window(Some("2026-10-02T00:00:00Z"), Some("2026-10-01T00:00:00Z"))
                .unwrap_err()
                .contains("before")
        );
        assert!(parse_window(Some("2026-10-01T00:00:00Z"), Some("2026-10-01T00:00:00Z")).is_err());
        assert!(parse_window(None, Some("2026-10-01T00:00:00Z"))
            .unwrap_err()
            .contains("from is required"));
        assert!(parse_window(Some("2026-10-01T00:00:00Z"), None)
            .unwrap_err()
            .contains("to is required"));
        assert!(
            parse_window(Some("yesterday"), Some("2026-10-01T00:00:00Z"))
                .unwrap_err()
                .contains("RFC3339")
        );
    }

    #[test]
    fn who_and_tz_parse() {
        assert_eq!(Who::parse(None), Ok(Who::Mine));
        assert_eq!(Who::parse(Some("mine")), Ok(Who::Mine));
        assert_eq!(Who::parse(Some("team")), Ok(Who::Team));
        assert!(Who::parse(Some("everyone")).is_err());
        assert_eq!(parse_tz(None), Ok(None));
        assert_eq!(
            parse_tz(Some("Europe/London")),
            Ok(Some(chrono_tz::Europe::London))
        );
        assert!(parse_tz(Some("Mars/Base")).is_err());
    }

    #[test]
    fn keys_round_trip_and_reject_garbage() {
        let key = make_key("coding_session", "team:alpha");
        assert_eq!(
            parse_key(&key),
            Some(("coding_session".into(), "team:alpha".into()))
        );
        let slack = make_key("slack", "C0912/1729000000.000100");
        assert_eq!(
            parse_key(&slack),
            Some(("slack".into(), "C0912/1729000000.000100".into()))
        );
        assert!(
            !key.contains('/') && !key.contains('+') && !key.contains('='),
            "URL-safe"
        );
        assert_eq!(parse_key(""), None);
        assert_eq!(parse_key("not base64!"), None);
        assert_eq!(parse_key(&URL_SAFE_NO_PAD.encode("no separator")), None);
        assert_eq!(
            parse_key(&make_key("gmail", "x")),
            None,
            "an unmapped type has no card"
        );
        assert_eq!(parse_key(&make_key("note", "")), None);
        assert_eq!(parse_key(&"a".repeat(MAX_KEY_LEN + 1)), None);
    }

    #[test]
    fn like_prefixes_are_escaped() {
        assert_eq!(escape_like("a_b%c\\d"), "a\\_b\\%c\\\\d");
    }

    #[test]
    fn first_sentence_is_one_terse_line() {
        assert_eq!(
            first_sentence("Pricing moved to usage tiers. Tom owns the rollout."),
            "Pricing moved to usage tiers."
        );
        assert_eq!(
            first_sentence("Version 2.1 ships Friday"),
            "Version 2.1 ships Friday"
        );
        assert_eq!(
            first_sentence("  spaced\n out  text. next"),
            "spaced out text."
        );
        let long = format!("{} end.", "word ".repeat(80));
        let s = first_sentence(&long);
        assert!(s.chars().count() <= OUTCOME_MAX_CHARS && s.ends_with('…'));
    }

    #[test]
    fn people_are_deduped_case_insensitively_and_sorted() {
        let got = dedupe_people(&[
            "tom becker".into(),
            "Sara Kim".into(),
            "Tom Becker".into(),
            " ".into(),
            "Aiko".into(),
        ]);
        assert_eq!(got, vec!["Aiko", "Sara Kim", "tom becker"]);
    }

    #[test]
    fn card_caps_people_but_reports_the_total() {
        let mut g = group("transcript", "m1");
        g.people = (0..12).map(|i| format!("Person {i:02}")).collect();
        let card = build_activity(
            &g,
            &Enrichment::default(),
            &graph(),
            CARD_PEOPLE,
            CARD_TOPICS,
        );
        assert_eq!((card.people.len(), card.people_count), (8, 12));
        let detail = build_activity(
            &g,
            &Enrichment::default(),
            &graph(),
            DETAIL_PEOPLE,
            DETAIL_TOPICS,
        );
        assert_eq!((detail.people.len(), detail.people_count), (12, 12));
    }

    #[test]
    fn title_precedence_and_labels() {
        let mut s = group("coding_session", "sess-1");
        s.session_title = Some("Rate limiter spike".into());
        s.any_title = Some("A checkpoint headline".into());
        assert_eq!(
            build_activity(&s, &Enrichment::default(), &graph(), 8, 4).title,
            "Rate limiter spike"
        );
        s.session_title = None;
        assert_eq!(
            build_activity(&s, &Enrichment::default(), &graph(), 8, 4).title,
            "A checkpoint headline"
        );
        s.any_title = None;
        assert_eq!(
            build_activity(&s, &Enrichment::default(), &graph(), 8, 4).title,
            "Agent session"
        );

        // A non-session coral's session_title is never used.
        let mut n = group("note", "n1");
        n.session_title = Some("ignored".into());
        assert_eq!(
            build_activity(&n, &Enrichment::default(), &graph(), 8, 4).title,
            "Note"
        );

        let mut sl = group("slack", "C1/1.2");
        sl.channel_name = Some("eng-platform".into());
        let card = build_activity(&sl, &Enrichment::default(), &graph(), 8, 4);
        assert_eq!(
            (card.title.as_str(), card.kind_label),
            ("#eng-platform", "slack thread")
        );
        assert_eq!(
            build_activity(
                &group("slack", "C1/1.2"),
                &Enrichment::default(),
                &graph(),
                8,
                4
            )
            .title,
            "Slack thread"
        );
    }

    #[test]
    fn meeting_ends_with_its_recorded_duration_others_at_the_last_atom() {
        let e = Enrichment {
            duration_ms: Some(42 * 60_000),
            ..Default::default()
        };
        let m = build_activity(&group("transcript", "m1"), &e, &graph(), 8, 4);
        assert_eq!(
            (m.start, m.end),
            (ts("2026-10-29T09:00:00Z"), ts("2026-10-29T09:42:00Z"))
        );
        // A duration shorter than the atom spread never shrinks the card.
        let short = Enrichment {
            duration_ms: Some(60_000),
            ..Default::default()
        };
        assert_eq!(
            build_activity(&group("transcript", "m1"), &short, &graph(), 8, 4).end,
            ts("2026-10-29T09:30:00Z")
        );
        assert_eq!(
            build_activity(&group("note", "n"), &e, &graph(), 8, 4).end,
            ts("2026-10-29T09:30:00Z")
        );
        let none = Enrichment {
            duration_ms: Some(0),
            ..Default::default()
        };
        assert_eq!(
            build_activity(&group("transcript", "m1"), &none, &graph(), 8, 4).end,
            ts("2026-10-29T09:30:00Z")
        );
    }

    #[test]
    fn outcome_prefers_the_meeting_summary_then_a_decision_then_null() {
        let mut e = Enrichment::default();
        assert_eq!(
            build_activity(&group("note", "n"), &e, &graph(), 8, 4).outcome,
            None
        );
        e.nodes.push((
            Uuid::from_u128(9),
            "Ship on Friday".into(),
            "decision".into(),
            2,
        ));
        let card = build_activity(&group("note", "n"), &e, &graph(), 8, 4);
        assert_eq!(
            (card.outcome.as_deref(), card.decision_count),
            (Some("Ship on Friday"), 1)
        );
        e.semantic_decisions.push("Adopt usage tiers".into());
        assert_eq!(
            build_activity(&group("note", "n"), &e, &graph(), 8, 4)
                .outcome
                .as_deref(),
            Some("Adopt usage tiers")
        );
        e.summary_overview = Some("Pricing review. The team agreed on tiers.".into());
        assert_eq!(
            build_activity(&group("transcript", "m"), &e, &graph(), 8, 4)
                .outcome
                .as_deref(),
            Some("Pricing review.")
        );
    }

    #[test]
    fn decisions_come_from_one_source_not_a_sum() {
        let e = Enrichment {
            semantic_decisions: vec![
                "Adopt usage tiers".into(),
                "adopt usage tiers".into(),
                "Cap free at 3 seats".into(),
            ],
            summary_decisions: vec!["Something else".into()],
            nodes: vec![(
                Uuid::from_u128(1),
                "Adopt usage tiers".into(),
                "decision".into(),
                1,
            )],
            ..Default::default()
        };
        assert_eq!(
            e.decisions(),
            vec!["Adopt usage tiers", "Cap free at 3 seats"]
        );
        let summary_only = Enrichment {
            summary_decisions: vec!["A".into(), "B".into()],
            ..Default::default()
        };
        assert_eq!(summary_only.decisions().len(), 2);
    }

    #[test]
    fn topics_skip_decision_nodes_and_rank_by_links() {
        let (a, b, c, d) = (
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
        );
        let e = Enrichment {
            nodes: vec![
                (a, "Rate limiter".into(), "entity".into(), 2),
                (b, "Auth rewrite".into(), "thread".into(), 5),
                (a, "Rate limiter".into(), "entity".into(), 4), // legacy + semantic link of one reef
                (c, "Ship it".into(), "decision".into(), 9),
                (d, "  ".into(), "entity".into(), 9),
            ],
            ..Default::default()
        };
        let titles: Vec<String> = e.topics().into_iter().map(|t| t.title).collect();
        assert_eq!(titles, vec!["Rate limiter", "Auth rewrite"]);
    }

    fn atom(kind: &str, ordinal: i64, quote: &str, start_ms: Option<i64>) -> Value {
        serde_json::json!({
            "atom_id": Uuid::from_u128(ordinal as u128 + 100).to_string(), "ordinal": ordinal, "kind": kind,
            "passages": [{"quote": quote, "start_ms": start_ms, "speaker": "Tom"}],
        })
    }

    #[test]
    fn sources_put_decisions_first_and_label_the_time_of_day() {
        let atoms = vec![
            atom("fact", 0, "We are at 40 percent of quota.", Some(60_000)),
            atom("decision", 1, "Let's go usage-based.", Some(5 * 60_000)),
            atom("fact", 2, "We are at 40 percent of quota.", None), // duplicate quote
            serde_json::json!({"atom_id": Uuid::nil().to_string(), "ordinal": 3, "kind": "fact", "passages": []}),
            atom("action", 4, "  Tom   will\nwrite it up.  ", None),
        ];
        let item = Uuid::from_u128(7);
        let out = pick_sources(
            Kind::Meeting,
            &atoms,
            item,
            ts("2026-10-29T14:00:00Z"),
            chrono_tz::UTC,
            5,
        );
        let quotes: Vec<&str> = out.iter().map(|s| s.quote.as_str()).collect();
        assert_eq!(
            quotes,
            vec![
                "Let's go usage-based.",
                "We are at 40 percent of quota.",
                "Tom will write it up."
            ]
        );
        assert_eq!(out[0].meta_label, "MEETING · 14:05");
        assert_eq!(out[1].meta_label, "MEETING · 14:01");
        assert_eq!(out[0].source_item_id, Some(item));
        assert_eq!(out[0].atom_id, Some(Uuid::from_u128(101)));
        assert_eq!(
            pick_sources(
                Kind::Meeting,
                &atoms,
                item,
                ts("2026-10-29T14:00:00Z"),
                chrono_tz::UTC,
                2
            )
            .len(),
            2
        );
        // A note's passages carry the note's own time, not an offset.
        let note = pick_sources(
            Kind::Note,
            &atoms,
            item,
            ts("2026-10-29T14:00:00Z"),
            chrono_tz::Europe::Paris,
            1,
        );
        assert_eq!(note[0].meta_label, "NOTE · 15:00");
    }

    #[test]
    fn strip_covers_14_local_days_ending_with_the_day_before_to() {
        // `to` is exclusive: midnight to means the previous day is the last.
        let (first, from) = strip_window(ts("2026-10-30T00:00:00Z"), chrono_tz::UTC);
        assert_eq!(first, NaiveDate::from_ymd_opt(2026, 10, 16).unwrap());
        assert_eq!(from, ts("2026-10-16T00:00:00Z"));
        // Tokyo is UTC+9: 2026-10-29T20:00Z is 05:00 on the 30th there.
        let (first, from) = strip_window(ts("2026-10-29T20:00:00Z"), chrono_tz::Asia::Tokyo);
        assert_eq!(first, NaiveDate::from_ymd_opt(2026, 10, 17).unwrap());
        assert_eq!(from, ts("2026-10-16T15:00:00Z"));
        let mut counts = HashMap::new();
        counts.insert(NaiveDate::from_ymd_opt(2026, 10, 20).unwrap(), 3);
        let strip = fill_strip(first, &counts);
        assert_eq!(strip.len(), 14);
        assert_eq!(
            (strip[0].day, strip[13].day),
            (first, NaiveDate::from_ymd_opt(2026, 10, 30).unwrap())
        );
        assert_eq!(strip.iter().map(|d| d.count).sum::<i64>(), 3);
        assert_eq!(strip[3].count, 3);
    }

    #[test]
    fn owner_names_fall_back_to_the_email_local_part() {
        assert_eq!(
            display_name(Some("Priya Nair"), Some("p@x.io")).as_deref(),
            Some("Priya Nair")
        );
        assert_eq!(
            display_name(Some("  "), Some("priya@x.io")).as_deref(),
            Some("priya")
        );
        assert_eq!(display_name(None, None), None);
        let mut card = build_activity(&group("note", "n"), &Enrichment::default(), &graph(), 8, 4);
        let id = Uuid::from_u128(5);
        card.owner.user_id = Some(id);
        card.apply_names(&HashMap::from([(id, "Priya Nair".to_string())]));
        assert_eq!(card.owner.name.as_deref(), Some("Priya Nair"));
        card.apply_names(&HashMap::new());
        assert_eq!(card.owner.name, None);
    }

    #[test]
    fn flag_is_read_from_the_environment() {
        // The unit tests share one process environment; only this test uses
        // the flag, so set/restore is safe.
        let prev = std::env::var(FLAG).ok();
        std::env::remove_var(FLAG);
        assert!(!enabled());
        for on in ["1", "true", "TRUE"] {
            std::env::set_var(FLAG, on);
            assert!(enabled(), "{on}");
        }
        std::env::set_var(FLAG, "0");
        assert!(!enabled());
        match prev {
            Some(v) => std::env::set_var(FLAG, v),
            None => std::env::remove_var(FLAG),
        }
    }
}
