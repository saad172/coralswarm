//! Home dashboard metrics (`GET /v1/home/metrics`): the data behind the KPI
//! tiles, the "Ingested per day" stacked chart, "Oceans by volume", the
//! source-type split and the activity heatmap. Contract:
//! `docs/implementation/home-metrics-api.md`; validation:
//! `docs/implementation/home-metrics-validation.md`.
//!
//! Compute-only helpers for the stored read-model worker. The HTTP route reads
//! the stored row and cannot invoke these aggregates.
//!
//! # Scope
//!
//! The caller's own reachable oceans in the CURRENT tenant and account (the
//! ADR-0007 D9 union `GET /v1/graphs` returns: their private ocean, the
//! org/personal oceans, the team oceans they are an active member of; never
//! another member's private ocean, never an archived one), narrowed to the
//! caller's `account_id` as `ReachabilityResolver::readable_graph_ids` asks
//! of its callers. Computation uses a verified actor's readonly transaction
//! under the serving role, with explicit graph predicates in addition to RLS:
//!
//! - corals: `library_permitted_atoms(graph, ids)`, the Library's own
//!   admission (retracted, derived-copy and denied-source atoms are out, and
//!   the team-seat history floor of `history_floor.rs` applies), run over the
//!   window's atom ids only, so the cost follows the window, not the ocean;
//! - reefs: `library_node_permitted`, the rule behind the workspace counts;
//! - agent sessions: `history_floor::admits` on the session start.
//!
//! # Bucketing
//!
//! Local-day buckets in the requested IANA zone. The Rust side computes the
//! `2 * days + 1` local-midnight instants (the prior window, then the
//! current one) with the journal's `resolve_local_midnight`, so a 23 or 25
//! hour DST day is exact, and Postgres buckets with
//! `width_bucket(ts, thresholds)` against that same array. One GROUP BY per
//! metric covers both windows; there is no per-day loop. No SQL time-zone
//! arithmetic means no way for the two sides to disagree.
//!
//! # Metric definitions
//!
//! | KPI | Rows counted | Time column |
//! |---|---|---|
//! | `corals_ingested` | permitted `atoms` in scope | `atoms.created_at` (when we ingested it, not the source event time) |
//! | `reefs_formed` | permitted `nodes` (any type) in scope | `nodes.created_at` |
//! | `questions_asked` | the CALLER's `query_history` rows in scope | `query_history.created_at` |
//! | `agent_sessions` | ROOT `coding_sessions` (no `parent_session_id`) of kind `coding`/`agent` in scope | `coding_sessions.started_at` |
//! | `meetings_captured` | distinct meeting sources: permitted atoms of a transcript-like type, one per base `source_id` (`:chunk:N` collapsed) | the source's earliest in-window `atoms.created_at` |
//!
//! `meeting_session` (the screen channel of a native recording) is not
//! counted: the audio channel of the same meeting already is, and counting
//! both would double a recorded meeting.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use chrono_tz::Tz;
use serde::Serialize;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::{history_floor, workers::journal_sweep::resolve_local_midnight};

// ── Flag ─────────────────────────────────────────────────────────────────

pub const FLAG: &str = "HOME_DASHBOARD_ENABLED";

/// `HOME_DASHBOARD_ENABLED` is `1`/`true`. Unset = off (the route answers 404).
/// Read per request, like `ORG_DELETION_ENABLED`.
pub fn enabled() -> bool {
    std::env::var(FLAG)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// ── Parameters and window ────────────────────────────────────────────────

pub const MIN_DAYS: u32 = 7;
pub const MAX_DAYS: u32 = 60;
pub const DEFAULT_DAYS: u32 = 30;

#[derive(Debug, PartialEq, Eq)]
pub enum ParamError {
    Days,
    Tz(String),
}

impl ParamError {
    pub fn message(&self) -> String {
        match self {
            ParamError::Days => format!("days must be an integer from {MIN_DAYS} to {MAX_DAYS}"),
            ParamError::Tz(z) => format!("tz must be an IANA time zone name, got \"{z}\""),
        }
    }
}

/// Validate the raw query values. `days` is taken as `i64` so `-3` and `1000`
/// are 400s rather than deserialization errors with a different body.
pub fn parse_params(days: Option<i64>, tz: Option<&str>) -> Result<(u32, Tz), ParamError> {
    let days = match days {
        None => DEFAULT_DAYS,
        Some(d) if (MIN_DAYS as i64..=MAX_DAYS as i64).contains(&d) => d as u32,
        Some(_) => return Err(ParamError::Days),
    };
    let tz = match tz.map(str::trim) {
        None | Some("") => Tz::UTC,
        Some(z) => z.parse::<Tz>().map_err(|_| ParamError::Tz(z.to_string()))?,
    };
    Ok((days, tz))
}

/// The two equal windows, as local-midnight instants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    pub days: u32,
    pub tz: Tz,
    /// Local dates of the current window, oldest first; the last is "today".
    pub dates: Vec<NaiveDate>,
    /// `2 * days + 1` ascending UTC instants: `[0]` starts the prior window,
    /// `[days]` starts the current one, `[2 * days]` ends it (the next local
    /// midnight after today, so today is a partial day).
    pub bounds: Vec<DateTime<Utc>>,
    /// Where the prior window stops counting, so the comparison is like for
    /// like: today is partial, so the prior window's matching day (the same
    /// local day `days` earlier) is cut at the same time-of-day offset.
    pub prior_cut: DateTime<Utc>,
}

impl Window {
    pub fn new(now: DateTime<Utc>, days: u32, tz: Tz) -> Self {
        let today = now.with_timezone(&tz).date_naive();
        let first_prior = today - Duration::days(2 * days as i64 - 1);
        let bounds: Vec<DateTime<Utc>> = (0..=2 * days as i64)
            .map(|i| {
                let d = first_prior + Duration::days(i);
                resolve_local_midnight(tz, d.and_hms_opt(0, 0, 0).expect("00:00:00 is valid"))
            })
            .collect();
        let dates = (0..days as i64)
            .map(|i| today - Duration::days(days as i64 - 1 - i))
            .collect();
        let d = days as usize;
        let elapsed = (now - bounds[2 * d - 1]).max(Duration::zero());
        let prior_cut = (bounds[d - 1] + elapsed).min(bounds[d]);
        Window {
            days,
            tz,
            dates,
            bounds,
            prior_cut,
        }
    }
    pub fn prior_cut(&self) -> DateTime<Utc> {
        self.prior_cut
    }
    pub fn prior_start(&self) -> DateTime<Utc> {
        self.bounds[0]
    }
    pub fn start(&self) -> DateTime<Utc> {
        self.bounds[self.days as usize]
    }
    pub fn end(&self) -> DateTime<Utc> {
        self.bounds[2 * self.days as usize]
    }
}

/// Where a `width_bucket` result falls: `Prior(i)` / `Current(i)` are day
/// indexes within their window; `None` is outside both.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Slot {
    Prior(usize),
    Current(usize),
}

fn slot(bucket: i64, days: u32) -> Option<Slot> {
    let d = days as i64;
    if bucket >= 1 && bucket <= d {
        Some(Slot::Prior((bucket - 1) as usize))
    } else if bucket > d && bucket <= 2 * d {
        Some(Slot::Current((bucket - 1 - d) as usize))
    } else {
        None
    }
}

// ── Source-type grouping ─────────────────────────────────────────────────

/// The five display groups, in display order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceGroup {
    Chat,
    Meetings,
    AgentSessions,
    DocsMail,
    CodeTickets,
}

impl SourceGroup {
    pub const ALL: [SourceGroup; 5] = [
        SourceGroup::Chat,
        SourceGroup::Meetings,
        SourceGroup::AgentSessions,
        SourceGroup::DocsMail,
        SourceGroup::CodeTickets,
    ];
    pub fn id(self) -> &'static str {
        match self {
            SourceGroup::Chat => "chat",
            SourceGroup::Meetings => "meetings",
            SourceGroup::AgentSessions => "agent_sessions",
            SourceGroup::DocsMail => "docs_mail",
            SourceGroup::CodeTickets => "code_tickets",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            SourceGroup::Chat => "Chat",
            SourceGroup::Meetings => "Meetings",
            SourceGroup::AgentSessions => "Agent sessions",
            SourceGroup::DocsMail => "Docs & mail",
            SourceGroup::CodeTickets => "Code & tickets",
        }
    }
    /// Exact `atoms.source_type` values, documented in the response.
    fn exact(self) -> &'static [&'static str] {
        match self {
            SourceGroup::Chat => &[
                "slack",
                "teams_chat",
                "teams_channel",
                "teams_message",
                "teams",
                "google_chat",
                "chat",
            ],
            SourceGroup::Meetings => &[
                "meeting",
                "meeting_session",
                "meeting_transcript",
                "transcript",
                "zoom",
                "meet",
                "teams_transcript",
            ],
            SourceGroup::AgentSessions => &["coding_session", "agent_session", "mcp"],
            SourceGroup::DocsMail => &[
                "document",
                "gdrive",
                "notion",
                "email",
                "gmail",
                "outlook",
                "outlook_thread",
                "note",
                "work_session",
                "google_calendar",
                "microsoft_calendar",
                "google",
                "microsoft",
            ],
            SourceGroup::CodeTickets => &["github", "linear", "jira"],
        }
    }
    /// Prefix families: `github_pr`, `linear_issue`, `jira_comment`, ...
    fn prefixes(self) -> &'static [&'static str] {
        match self {
            SourceGroup::CodeTickets => &["github_", "linear_", "jira_"],
            _ => &[],
        }
    }
}

/// A raw `source_type` with no rule falls back to Docs & mail, so every coral
/// lands in exactly one of the five groups the design draws.
pub const FALLBACK_GROUP: SourceGroup = SourceGroup::DocsMail;

pub fn group_for(source_type: &str) -> SourceGroup {
    let t = source_type.trim().to_ascii_lowercase();
    SourceGroup::ALL
        .into_iter()
        .find(|g| g.exact().contains(&t.as_str()) || g.prefixes().iter().any(|p| t.starts_with(p)))
        .unwrap_or(FALLBACK_GROUP)
}

/// Raw types that each count as one captured meeting per base `source_id`.
/// `meeting_session` is excluded on purpose (see the module doc). Also the
/// Home launchpad's meeting owner path (`services::home_launchpad`).
pub(crate) const MEETING_CAPTURE_TYPES: &[&str] = &[
    "meeting",
    "meeting_transcript",
    "transcript",
    "zoom",
    "meet",
    "teams_transcript",
];

// ── Response ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WindowOut {
    /// Start of the first local day of the current window (UTC instant).
    pub start: DateTime<Utc>,
    /// Start of the day after today (UTC instant); the window is `[start, end)`.
    pub end: DateTime<Utc>,
    pub days: u32,
    pub tz: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct Kpi {
    pub total: i64,
    /// The preceding window of equal length, for the delta.
    pub prior_total: i64,
    /// One entry per local day, oldest first; `daily.len() == window.days`.
    pub daily: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Kpis {
    pub corals_ingested: Kpi,
    pub reefs_formed: Kpi,
    pub questions_asked: Kpi,
    pub agent_sessions: Kpi,
    pub meetings_captured: Kpi,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GraphDay {
    pub graph_id: Uuid,
    /// Corals ingested that day, by display group id (`SourceGroup::id`).
    pub by_source_type: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestDay {
    /// Local date, `YYYY-MM-DD`. Always present for every day of the window.
    pub day: String,
    /// Only oceans with corals that day.
    pub by_graph: Vec<GraphDay>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OceanVolume {
    pub graph_id: Uuid,
    pub name: String,
    /// `member` (private), `org`, `personal` or `team`.
    pub kind: String,
    /// Stored colour. No ocean has one today, so this is omitted; the client
    /// assigns a stable hue per `graph_id`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hue: Option<String>,
    pub total: i64,
    pub by_source_type: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct GroupDef {
    pub id: &'static str,
    pub label: &'static str,
    /// Raw `source_type` values (and `prefix*` families) that map here.
    pub source_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HomeMetrics {
    pub window: WindowOut,
    pub kpis: Kpis,
    pub ingest_by_day: Vec<IngestDay>,
    pub oceans: Vec<OceanVolume>,
    /// Current-window corals by display group, every group present.
    pub source_type_totals: BTreeMap<String, i64>,
    /// The grouping, in display order. A raw type with no rule lands in
    /// `fallback_group`.
    pub source_type_groups: Vec<GroupDef>,
    pub fallback_group: &'static str,
}

pub fn group_defs() -> Vec<GroupDef> {
    SourceGroup::ALL
        .into_iter()
        .map(|g| GroupDef {
            id: g.id(),
            label: g.label(),
            source_types: g
                .exact()
                .iter()
                .map(|s| s.to_string())
                .chain(g.prefixes().iter().map(|p| format!("{p}*")))
                .collect(),
        })
        .collect()
}

// ── Raw rows and assembly (pure) ─────────────────────────────────────────

/// `(graph_id, raw source_type, width_bucket, count)`.
pub type AtomRow = (Uuid, String, i64, i64);
/// `(graph_id, width_bucket, count)`.
pub type MeetingRow = (Uuid, i64, i64);
/// `(width_bucket, count)`.
pub type CountRow = (i64, i64);

pub struct OceanInfo {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
}

fn fold_kpi(window: &Window, rows: impl IntoIterator<Item = (i64, i64)>) -> Kpi {
    let mut k = Kpi {
        daily: vec![0; window.days as usize],
        ..Kpi::default()
    };
    for (bucket, n) in rows {
        match slot(bucket, window.days) {
            Some(Slot::Prior(_)) => k.prior_total += n,
            Some(Slot::Current(i)) => {
                k.daily[i] += n;
                k.total += n;
            }
            None => {}
        }
    }
    k
}

pub fn assemble(
    window: &Window,
    oceans: &[OceanInfo],
    atoms: &[AtomRow],
    meetings: &[MeetingRow],
    sessions: &[CountRow],
    asks: &[CountRow],
    reefs: &[CountRow],
) -> HomeMetrics {
    let known: BTreeMap<Uuid, &OceanInfo> = oceans.iter().map(|o| (o.id, o)).collect();
    // Rows for an ocean outside the scope can't happen (the SQL filters on
    // it), but dropping them keeps the sums consistent if one ever did.
    let atoms: Vec<&AtomRow> = atoms.iter().filter(|r| known.contains_key(&r.0)).collect();

    let corals = fold_kpi(window, atoms.iter().map(|r| (r.2, r.3)));
    let meeting_kpi = fold_kpi(
        window,
        meetings
            .iter()
            .filter(|r| known.contains_key(&r.0))
            .map(|r| (r.1, r.2)),
    );

    let mut by_day: Vec<BTreeMap<Uuid, BTreeMap<String, i64>>> =
        vec![BTreeMap::new(); window.days as usize];
    let mut per_ocean: BTreeMap<Uuid, BTreeMap<String, i64>> = BTreeMap::new();
    let mut totals: BTreeMap<String, i64> = SourceGroup::ALL
        .iter()
        .map(|g| (g.id().to_string(), 0))
        .collect();
    for (graph, source_type, bucket, n) in atoms.iter().map(|r| (&r.0, &r.1, r.2, r.3)) {
        let Some(Slot::Current(i)) = slot(bucket, window.days) else {
            continue;
        };
        let g = group_for(source_type).id().to_string();
        *by_day[i]
            .entry(*graph)
            .or_default()
            .entry(g.clone())
            .or_default() += n;
        *per_ocean
            .entry(*graph)
            .or_default()
            .entry(g.clone())
            .or_default() += n;
        *totals.entry(g).or_default() += n;
    }

    let ingest_by_day = window
        .dates
        .iter()
        .zip(by_day)
        .map(|(d, graphs)| IngestDay {
            day: d.format("%Y-%m-%d").to_string(),
            by_graph: graphs
                .into_iter()
                .map(|(graph_id, by_source_type)| GraphDay {
                    graph_id,
                    by_source_type,
                })
                .collect(),
        })
        .collect();

    let mut ocean_rows: Vec<OceanVolume> = oceans
        .iter()
        .map(|o| {
            let by_source_type = per_ocean.remove(&o.id).unwrap_or_default();
            OceanVolume {
                graph_id: o.id,
                name: o.name.clone(),
                kind: o.kind.clone(),
                hue: None,
                total: by_source_type.values().sum(),
                by_source_type,
            }
        })
        .collect();
    ocean_rows.sort_by(|a, b| {
        b.total
            .cmp(&a.total)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.graph_id.cmp(&b.graph_id))
    });

    HomeMetrics {
        window: WindowOut {
            start: window.start(),
            end: window.end(),
            days: window.days,
            tz: window.tz.name().to_string(),
        },
        kpis: Kpis {
            corals_ingested: corals,
            reefs_formed: fold_kpi(window, reefs.iter().copied()),
            questions_asked: fold_kpi(window, asks.iter().copied()),
            agent_sessions: fold_kpi(window, sessions.iter().copied()),
            meetings_captured: meeting_kpi,
        },
        ingest_by_day,
        oceans: ocean_rows,
        source_type_totals: totals,
        source_type_groups: group_defs(),
        fallback_group: FALLBACK_GROUP.id(),
    }
}

// ── Reads ────────────────────────────────────────────────────────────────
//
// Reads return the driver's own `sqlx::Error`, never a string of it: the
// read-model worker classifies a failed compute by its type and SQLSTATE
// (`read_models::failure`), and a statement timeout must stay `57014`.

/// One statement: the window's candidate atoms, admitted by the Library's own
/// rule per graph, then two GROUP BYs over the admitted set (corals by
/// graph x type x day; meetings by graph x day, one per base source).
const ADMISSION_IDS_CEILING: i64 = 10_000;

/// One statement: the window's candidate atoms, admitted by the Library's own
/// rule per graph, then two GROUP BYs over the admitted set (corals by
/// graph x type x day; meetings by graph x day, one per base source).
fn atoms_sql() -> String {
    format!(
        r#"
WITH win AS MATERIALIZED (
  SELECT a.graph_id, a.atom_id, a.source_type, a.created_at,
         split_part(a.source_id, ':chunk:', 1) AS base
  FROM atoms a
  WHERE a.graph_id = ANY($1) AND a.created_at >= $2 AND a.created_at < $3
    AND (a.created_at < $6 OR a.created_at >= $7)
), per_graph AS (
  SELECT graph_id, array_agg(atom_id) AS ids FROM win GROUP BY graph_id
), ok AS MATERIALIZED (
  SELECT w.graph_id, w.source_type, w.created_at, w.base
  FROM per_graph pg
  CROSS JOIN LATERAL public.library_permitted_atoms(pg.graph_id,
    CASE WHEN cardinality(pg.ids) > {ceiling} THEN NULL ELSE pg.ids END) p
  JOIN win w ON w.graph_id = pg.graph_id AND w.atom_id = p.atom_id
)
SELECT 'atoms'::text AS metric, graph_id, source_type,
       width_bucket(created_at, $4::timestamptz[])::bigint AS bucket, count(*)::bigint AS n
FROM ok GROUP BY graph_id, source_type, 4
UNION ALL
SELECT 'meetings', graph_id, ''::text, width_bucket(first_seen, $4::timestamptz[])::bigint, count(*)::bigint
FROM (
  SELECT m.graph_id, m.base,
         -- First chunk of this meeting EVER, not merely in the window, so a
         -- meeting first seen before the window is not counted as new. The
         -- `~>=~`/`~<~` pair is the text_pattern_ops range that
         -- idx_atoms_graph_source_id_pattern serves (the chunk keys are
         -- `base:chunk:N`; ';' is ':' + 1).
         -- OFFSET 0 stops the planner turning min() into a backward scan of
         -- idx_atoms_graph_created with this as a filter (measured: 60 s vs
         -- ~1 s for 3k meetings over 600k atoms).
         (SELECT min(x.created_at) FROM (
            SELECT a.created_at FROM atoms a
            WHERE a.graph_id = m.graph_id AND a.source_type = ANY($5)
              AND (a.source_id = m.base
                   OR (a.source_id ~>=~ (m.base || ':chunk:') AND a.source_id ~<~ (m.base || ':chunk;')))
            OFFSET 0) x
         ) AS first_seen
  FROM (SELECT DISTINCT graph_id, base FROM ok WHERE source_type = ANY($5)) m
) f
WHERE first_seen >= $2 AND first_seen < $3 AND (first_seen < $6 OR first_seen >= $7)
GROUP BY graph_id, 4
"#,
        ceiling = ADMISSION_IDS_CEILING,
    )
}

/// Reefs created in the window, admitted exactly as `library_node_permitted`
/// admits: a semantic link to a served (permitted) source, or a legacy link
/// to a permitted atom of the reef's own ocean.
///
/// The CTE chain is shared with [`reefs_admitted_sql`], which lists the
/// admitted reefs so a test can compare them with `library_node_permitted`
/// one by one. `$1` graphs, `$2`/`$3` window, `$5` prior cut, `$6` current
/// start.
///
/// # Shape
///
/// Production (server-2026.11.02.1, org tenant with 195k reefs, 144k corals
/// and 7.3M `node_atoms` links, 37 per reef) timed out the earlier set-based
/// form at 30 s, twice over. Its plan materialised every link of every
/// windowed reef (7.3M rows), aggregated them per ocean, and then evaluated
/// `w.node_id IN (SELECT node_id FROM leg)` as a NON-hashed SubPlan: the
/// planner's estimate for `leg` (224k rows) exceeded the 4 MB `work_mem`
/// hash budget, so every windowed reef rescanned the materialised CTE
/// (194k x 192k comparisons; cost 2.6e8). The 600k-atom seed it was tuned
/// on has 60k reefs, 1.2M links and 20 links per reef, which kept both
/// estimates under the hash budget.
///
/// This form never touches more than one link per admitted reef:
///
/// 1. `ok_atoms`: the permitted corals of each ocean in scope, one
///    `library_permitted_atoms(graph, NULL)` call per ocean (the Library's
///    own rule, whole-ocean; 144k rows in 0.85 s on production). It is the
///    one admission set for the statement.
/// 2. `leg`: for each windowed reef, ONE `LIMIT 1` probe of
///    `node_atoms_pkey` in `(node_id, atom_id)` order, stopping at the first
///    link whose `(reef's ocean, atom)` pair is in `ok_atoms`. The `IN` is
///    wrapped in `IS TRUE` so the planner keeps it as a hashed SubPlan (built
///    once for the whole statement, in memory, never rescanned) instead of
///    pulling it up into a per-reef semi-join that would rebuild a hash or
///    rescan the CTE for every reef. The LATERAL with `LIMIT 1` is what makes
///    the probe per reef instead of a semi-join over all 7.3M links.
/// 3. `rest`: the reefs `leg` did not admit (hash anti join; 1.5k rows on
///    production), probed the same way through `node_semantic_atoms` and
///    `served_semantic_atoms` (index nested loops, `LIMIT 1`).
///
/// A reef is admitted when either probe hits; the two sets are disjoint by
/// construction, so the final count is a `UNION ALL`. Measured on the same
/// production tenant, read-only, with the worker's settings: 7.7 s cold
/// (78k page reads) and 4.6 s on the next run, against >45 s (timeout) for
/// the previous form; identical admission (192,828 legacy + 1,415 semantic
/// of 194,353 windowed reefs). `work_mem` does not enter this plan.
const REEFS_ADMITTED_CTES: &str = r#"
WITH win AS MATERIALIZED (
  SELECT n.graph_id, n.node_id, n.created_at FROM nodes n
  WHERE n.graph_id = ANY($1) AND n.created_at >= $2 AND n.created_at < $3
    AND (n.created_at < $5 OR n.created_at >= $6)
), ok_atoms AS MATERIALIZED (
  SELECT g.graph_id, p.atom_id FROM unnest($1::uuid[]) AS g(graph_id)
  CROSS JOIN LATERAL public.library_permitted_atoms(g.graph_id, NULL) p
), leg AS MATERIALIZED (
  SELECT w.node_id, w.created_at FROM win w
  CROSS JOIN LATERAL (
    SELECT 1 FROM node_atoms l
    WHERE l.node_id = w.node_id
      AND ((w.graph_id, l.atom_id) IN (SELECT o.graph_id, o.atom_id FROM ok_atoms o)) IS TRUE
    LIMIT 1) hit
), rest AS MATERIALIZED (
  SELECT w.graph_id, w.node_id, w.created_at FROM win w
  WHERE NOT EXISTS (SELECT 1 FROM leg x WHERE x.node_id = w.node_id)
), sem AS MATERIALIZED (
  SELECT r.node_id, r.created_at FROM rest r
  CROSS JOIN LATERAL (
    SELECT 1 FROM node_semantic_atoms l
    JOIN served_semantic_atoms a ON a.semantic_atom_id = l.semantic_atom_id AND a.graph_id = l.graph_id
    WHERE l.graph_id = r.graph_id AND l.node_id = r.node_id
    LIMIT 1) hit
)
"#;

/// `$4` bounds. See [`REEFS_ADMITTED_CTES`].
fn reefs_sql() -> String {
    format!(
        "{REEFS_ADMITTED_CTES}
SELECT width_bucket(x.created_at, $4::timestamptz[])::bigint, count(*)::bigint
FROM (SELECT created_at FROM leg UNION ALL SELECT created_at FROM sem) x
GROUP BY 1
"
    )
}

/// The admitted reefs of the window, one `node_id` per row, from the same
/// CTE chain [`reefs_sql`] counts. Binds `$1`..`$6` exactly as `reefs_sql`
/// (`$4`, the bounds, is typed but never selects a row, so the two share a
/// bind list). For tests that compare this admission with
/// `library_node_permitted`.
#[doc(hidden)]
pub fn reefs_admitted_sql() -> String {
    format!(
        "{REEFS_ADMITTED_CTES}
SELECT node_id FROM leg UNION ALL SELECT node_id FROM sem
UNION ALL SELECT NULL::uuid WHERE cardinality($4::timestamptz[]) < 0
"
    )
}

async fn fetch(
    conn: &mut PgConnection,
    user_id: Uuid,
    graphs: &[Uuid],
    w: &Window,
) -> Result<
    (
        Vec<AtomRow>,
        Vec<MeetingRow>,
        Vec<CountRow>,
        Vec<CountRow>,
        Vec<CountRow>,
    ),
    sqlx::Error,
> {
    let (from, to) = (w.prior_start(), w.end());
    let meeting_types: Vec<String> = MEETING_CAPTURE_TYPES
        .iter()
        .map(|s| s.to_string())
        .collect();

    let raw: Vec<(String, Uuid, String, i64, i64)> = sqlx::query_as(&atoms_sql())
        .bind(graphs)
        .bind(from)
        .bind(to)
        .bind(&w.bounds)
        .bind(&meeting_types)
        .bind(w.prior_cut())
        .bind(w.start())
        .fetch_all(&mut *conn)
        .await?;
    let mut atoms = Vec::new();
    let mut meetings = Vec::new();
    for (metric, graph, source_type, bucket, n) in raw {
        if metric == "atoms" {
            atoms.push((graph, source_type, bucket, n));
        } else {
            meetings.push((graph, bucket, n));
        }
    }

    let reefs: Vec<CountRow> = sqlx::query_as(&reefs_sql())
        .bind(graphs)
        .bind(from)
        .bind(to)
        .bind(&w.bounds)
        .bind(w.prior_cut())
        .bind(w.start())
        .fetch_all(&mut *conn)
        .await?;

    let asks: Vec<CountRow> = sqlx::query_as(
        "SELECT width_bucket(qh.created_at, $5::timestamptz[])::bigint, count(*)::bigint \
         FROM query_history qh \
         WHERE qh.user_id = $1 AND qh.graph_id = ANY($2) AND qh.created_at >= $3 AND qh.created_at < $4 \
           AND (qh.created_at < $6 OR qh.created_at >= $7) \
         GROUP BY 1",
    )
    .bind(user_id)
    .bind(graphs)
    .bind(from)
    .bind(to)
    .bind(&w.bounds)
    .bind(w.prior_cut())
    .bind(w.start())
    .fetch_all(&mut *conn)
    .await?;

    let sessions_sql = format!(
        "SELECT width_bucket(cs.started_at, $4::timestamptz[])::bigint, count(*)::bigint \
         FROM coding_sessions cs \
         WHERE cs.graph_id = ANY($1) AND cs.started_at >= $2 AND cs.started_at < $3 \
           AND (cs.started_at < $5 OR cs.started_at >= $6) \
           AND cs.session_kind IN ('coding', 'agent') AND cs.parent_session_id IS NULL \
           AND {floor} \
         GROUP BY 1",
        floor = history_floor::admits("cs.graph_id", "cs.started_at"),
    );
    let sessions: Vec<CountRow> = sqlx::query_as(&sessions_sql)
        .bind(graphs)
        .bind(from)
        .bind(to)
        .bind(&w.bounds)
        .bind(w.prior_cut())
        .bind(w.start())
        .fetch_all(&mut *conn)
        .await?;

    Ok((atoms, meetings, sessions, asks, reefs))
}

/// Compute-only entry point. The refresher supplies the verified graph set and
/// its member-scoped READ ONLY snapshot; this function cannot open a pool.
pub async fn compute(
    conn: &mut PgConnection,
    account_id: Uuid,
    user_id: Uuid,
    graphs: &[Uuid],
    days: u32,
    tz: Tz,
    now: DateTime<Utc>,
) -> Result<HomeMetrics, sqlx::Error> {
    let window = Window::new(now, days, tz);
    let rows: Vec<(Uuid,String,String)> = sqlx::query_as(
        "SELECT id,name,kind FROM graphs WHERE id=ANY($1) AND account_id=$2 AND archived_at IS NULL ORDER BY id")
        .bind(graphs).bind(account_id).fetch_all(&mut *conn).await?;
    let oceans: Vec<OceanInfo> = rows
        .into_iter()
        .map(|(id, name, kind)| OceanInfo { id, name, kind })
        .collect();
    let graph_ids: Vec<Uuid> = oceans.iter().map(|o| o.id).collect();
    let (atoms, meetings, sessions, asks, reefs) = if graph_ids.is_empty() {
        Default::default()
    } else {
        fetch(conn, user_id, &graph_ids, &window).await?
    };
    Ok(assemble(
        &window, &oceans, &atoms, &meetings, &sessions, &asks, &reefs,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0).unwrap()
    }

    #[test]
    fn params_default_and_bounds() {
        assert_eq!(parse_params(None, None), Ok((30, Tz::UTC)));
        assert_eq!(parse_params(Some(7), Some("")), Ok((7, Tz::UTC)));
        assert_eq!(
            parse_params(Some(60), Some("America/Chicago")).unwrap().1,
            chrono_tz::America::Chicago
        );
        assert_eq!(parse_params(Some(6), None), Err(ParamError::Days));
        assert_eq!(parse_params(Some(61), None), Err(ParamError::Days));
        assert_eq!(parse_params(Some(-1), None), Err(ParamError::Days));
        assert!(matches!(
            parse_params(None, Some("Mars/Base")),
            Err(ParamError::Tz(_))
        ));
    }

    #[test]
    fn utc_window_is_whole_days_ending_tomorrow_midnight() {
        let w = Window::new(utc(2026, 10, 29, 15), 30, Tz::UTC);
        assert_eq!(w.bounds.len(), 61);
        assert_eq!(w.end(), utc(2026, 10, 30, 0));
        assert_eq!(w.start(), utc(2026, 9, 30, 0));
        assert_eq!(w.prior_start(), utc(2026, 8, 31, 0));
        assert_eq!(w.dates.first().unwrap().to_string(), "2026-09-30");
        assert_eq!(w.dates.last().unwrap().to_string(), "2026-10-29");
        assert!(w.bounds.windows(2).all(|p| p[0] < p[1]));
    }

    #[test]
    fn local_days_follow_the_zone_not_utc() {
        // 2026-10-29 02:00 UTC is still the 28th in Chicago (CDT, UTC-5).
        let tz = chrono_tz::America::Chicago;
        let w = Window::new(utc(2026, 10, 29, 2), 7, tz);
        assert_eq!(w.dates.last().unwrap().to_string(), "2026-10-28");
        assert_eq!(w.end(), utc(2026, 10, 29, 5));
    }

    #[test]
    fn dst_fall_back_day_is_25_hours() {
        // Chicago leaves DST on 2026-11-01.
        let tz = chrono_tz::America::Chicago;
        let w = Window::new(utc(2026, 11, 3, 12), 7, tz);
        let i = w
            .dates
            .iter()
            .position(|d| d.to_string() == "2026-11-01")
            .unwrap();
        let bounds = &w.bounds[w.days as usize..];
        assert_eq!(bounds[i + 1] - bounds[i], Duration::hours(25));
    }

    #[test]
    fn slots_split_prior_and_current() {
        assert_eq!(slot(0, 7), None);
        assert_eq!(slot(1, 7), Some(Slot::Prior(0)));
        assert_eq!(slot(7, 7), Some(Slot::Prior(6)));
        assert_eq!(slot(8, 7), Some(Slot::Current(0)));
        assert_eq!(slot(14, 7), Some(Slot::Current(6)));
        assert_eq!(slot(15, 7), None);
    }

    #[test]
    fn grouping_covers_the_design_table() {
        for (t, g) in [
            ("slack", SourceGroup::Chat),
            ("teams_chat", SourceGroup::Chat),
            ("teams_channel", SourceGroup::Chat),
            ("chat", SourceGroup::Chat),
            ("meeting", SourceGroup::Meetings),
            ("meeting_session", SourceGroup::Meetings),
            ("transcript", SourceGroup::Meetings),
            ("zoom", SourceGroup::Meetings),
            ("meet", SourceGroup::Meetings),
            ("teams_transcript", SourceGroup::Meetings),
            ("coding_session", SourceGroup::AgentSessions),
            ("work_session", SourceGroup::DocsMail),
            ("meeting_transcript", SourceGroup::Meetings),
            ("agent_session", SourceGroup::AgentSessions),
            ("mcp", SourceGroup::AgentSessions),
            ("document", SourceGroup::DocsMail),
            ("gdrive", SourceGroup::DocsMail),
            ("notion", SourceGroup::DocsMail),
            ("email", SourceGroup::DocsMail),
            ("note", SourceGroup::DocsMail),
            ("github", SourceGroup::CodeTickets),
            ("github_pr", SourceGroup::CodeTickets),
            ("linear_issue", SourceGroup::CodeTickets),
            ("jira_comment", SourceGroup::CodeTickets),
            ("SLACK", SourceGroup::Chat),
            ("never_heard_of_it", FALLBACK_GROUP),
        ] {
            assert_eq!(group_for(t), g, "{t}");
        }
    }

    #[test]
    fn meeting_capture_types_are_all_meeting_group_and_exclude_screen_channel() {
        assert!(MEETING_CAPTURE_TYPES
            .iter()
            .all(|t| group_for(t) == SourceGroup::Meetings));
        assert!(!MEETING_CAPTURE_TYPES.contains(&"meeting_session"));
    }

    fn ocean(name: &str) -> OceanInfo {
        OceanInfo {
            id: Uuid::new_v4(),
            name: name.into(),
            kind: "team".into(),
        }
    }

    #[test]
    fn assemble_buckets_totals_and_sorts_oceans() {
        let w = Window::new(utc(2026, 10, 29, 15), 7, Tz::UTC);
        let (a, b) = (ocean("alpha"), ocean("beta"));
        let atoms: Vec<AtomRow> = vec![
            (a.id, "slack".into(), 14, 5),            // today, alpha
            (a.id, "github_pr".into(), 14, 2),        // today, alpha
            (b.id, "slack".into(), 13, 3),            // yesterday, beta
            (b.id, "weird".into(), 8, 1),             // first day of window, fallback group
            (a.id, "slack".into(), 3, 40),            // prior window
            (Uuid::new_v4(), "slack".into(), 14, 99), // outside scope, dropped
            (a.id, "slack".into(), 0, 7),             // before both windows
        ];
        let m = assemble(
            &w,
            &[a_ref(&a), a_ref(&b)],
            &atoms,
            &[(a.id, 14, 1)],
            &[(14, 2), (2, 9)],
            &[(8, 4)],
            &[],
        );

        let k = &m.kpis.corals_ingested;
        assert_eq!((k.total, k.prior_total), (11, 40));
        assert_eq!(k.daily.len(), 7);
        assert_eq!((k.daily[0], k.daily[5], k.daily[6]), (1, 3, 7));
        assert_eq!(m.kpis.meetings_captured.total, 1);
        assert_eq!(
            (
                m.kpis.agent_sessions.total,
                m.kpis.agent_sessions.prior_total
            ),
            (2, 9)
        );
        assert_eq!(m.kpis.questions_asked.daily[0], 4);
        assert_eq!(m.kpis.reefs_formed.total, 0);

        assert_eq!(m.ingest_by_day.len(), 7);
        assert_eq!(m.ingest_by_day[6].day, "2026-10-29");
        assert_eq!(m.ingest_by_day[6].by_graph.len(), 1);
        assert_eq!(m.ingest_by_day[6].by_graph[0].by_source_type["chat"], 5);
        assert_eq!(
            m.ingest_by_day[6].by_graph[0].by_source_type["code_tickets"],
            2
        );
        assert!(m.ingest_by_day[1].by_graph.is_empty());

        assert_eq!(m.oceans[0].name, "alpha");
        assert_eq!((m.oceans[0].total, m.oceans[1].total), (7, 4));
        assert_eq!(m.oceans[1].by_source_type["docs_mail"], 1);
        assert_eq!(m.source_type_totals["chat"], 8);
        assert_eq!(m.source_type_totals.len(), 5);
        assert_eq!(m.source_type_totals.values().sum::<i64>(), k.total);
        assert_eq!(m.oceans.iter().map(|o| o.total).sum::<i64>(), k.total);
    }

    fn a_ref(o: &OceanInfo) -> OceanInfo {
        OceanInfo {
            id: o.id,
            name: o.name.clone(),
            kind: o.kind.clone(),
        }
    }

    #[test]
    fn empty_scope_still_returns_dense_zeroes() {
        let w = Window::new(utc(2026, 10, 29, 0), 30, Tz::UTC);
        let m = assemble(&w, &[], &[], &[], &[], &[], &[]);
        assert_eq!(m.kpis.corals_ingested.daily, vec![0; 30]);
        assert_eq!(m.ingest_by_day.len(), 30);
        assert!(m.oceans.is_empty());
        let json = serde_json::to_value(&m).unwrap();
        assert_eq!(json["window"]["tz"], "UTC");
        assert_eq!(json["source_type_groups"][3]["id"], "docs_mail");
    }
    #[test]
    fn prior_window_is_cut_at_the_same_time_of_day() {
        let w = Window::new(utc(2026, 10, 29, 15), 30, Tz::UTC);
        assert_eq!(w.prior_cut(), utc(2026, 9, 29, 15));
        assert!(w.prior_cut() < w.start());
        let w = Window::new(utc(2026, 10, 29, 0), 7, Tz::UTC);
        assert_eq!(w.prior_cut(), utc(2026, 10, 22, 0));
    }
}
