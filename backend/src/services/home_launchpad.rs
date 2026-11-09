//! Home launchpad reads: `GET /v1/home/continue`, `GET /v1/home/whats-new`
//! and `GET /v1/home/attention`. Contract:
//! `docs/implementation/home-launchpad-api.md`; validation:
//! `docs/implementation/home-launchpad-validation.md`.
//!
//! # Plain bounded reads, not stored read models
//!
//! Each route is a `page_read::plain` GET: one member-scoped READ ONLY
//! transaction on the page lane with the 1 s statement budget, running a
//! handful of index-bounded statements. Nothing is stored, so nothing can be
//! withheld when a read-model fence moves, and nothing here ever answers with
//! an empty placeholder: a response is either the current answer or a 503.
//! Every statement reads through an index (see each SQL constant) and is
//! bounded by a candidate count that does not grow with the tenant: What's
//! new judges at most [`WHATS_NEW_CANDIDATES_PER_RANK`] reefs per rank,
//! `has_content` at most [`CONTENT_SCAN_TOTAL`] corals, Continue reads at most
//! [`CONTINUE_OWN_CORALS_PER_TYPE`] of the viewer's visible corals per type.
//! Where a bound can cut an answer short, the constant's doc says how. No
//! statement reads a whole ocean (attention does not read the connector
//! status page's 30-day counts).
//! A request holds one pooled connection at a time (attention releases its
//! control transaction before it opens the content one). The measured plans
//! are in `docs/implementation/home-launchpad-validation.md`.
//!
//! # Scope
//!
//! The oceans are the viewer's visible graphs in the active account
//! (`read_model_visible_windows()`, the set every stored Home model is keyed
//! by: the org ocean, the viewer's own private ocean, team oceans they are on,
//! never another member's private ocean, never an archived ocean). The
//! team-seat history floor applies to every coral, meeting and session read
//! through `crate::history_floor`.
//!
//! - **Continue** lists only the viewer's own items: chats they own
//!   (`conversations.user_id`), sessions and meetings whose first coral by
//!   creation time they ingested (`atoms.ingested_by`, found from the
//!   viewer's own corals, so other members' items never crowd them out), and
//!   the shared workspaces (org and team oceans) they belong to. A chat row
//!   carries the same admitted title the chat sidebar shows
//!   (`library_conversation_title`) and its ocean name; never message text.
//! - **What's new** admits and names a reef with the Library's own scalar
//!   rule, `library_node_title` (tenant 0137 + 0157 corrections): a non-NULL
//!   title proves `library_node_permitted`, and a reef whose title the viewer
//!   may not see is never listed. It is evaluated once per candidate, in
//!   display order, until five are admitted, over at most the rank's 200
//!   newest reefs (a rank comes up short only behind 200 newer hidden
//!   reefs); no fourth copy of the rule.
//!   Its semantic branch is 0137's (a permitted source), not
//!   `served_semantic_atoms` (which also drops superseded extractions and
//!   pre-history-floor evidence), exactly as the Library's own node reads.
//!   The set-based projector (`journal_activities::project_node_titles_on`)
//!   was measured and rejected here: it gathers every title witness and,
//!   above 1,000, admits the whole ocean (0.85 s on production), so a new
//!   hub reef would cost that on every visit; the scalar rule stops at the
//!   first admitted witness.
//! - **Attention** reads the connector-status inputs (the two halves of
//!   [`crate::read_models::connectors::status_inputs`]) and the same state
//!   rule and row order ([`crate::services::connector_status::assemble`]),
//!   without the 30-day counts: its first broken connector is the first
//!   `failing` row by name, which is `GET /v1/connectors/status`'s first
//!   unless a later-named failing row has more 30-day volume.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::{PgConnection, Postgres, Transaction};
use uuid::Uuid;

use crate::services::connector_status::{self as status, AtomStats, State};

// The flag is the other `/v1/home/*` routes' `HOME_DASHBOARD_ENABLED`, read
// through `services::home_metrics::enabled()` (unset = the routes answer 404).

// ── Limits ───────────────────────────────────────────────────────────────

/// Continue: rows returned, and rows of any one kind.
pub const CONTINUE_LIMIT: usize = 6;
pub const CONTINUE_PER_KIND: usize = 3;
/// Continue: sessions and meetings older than this are not "continue" work.
pub const CONTINUE_WINDOW_DAYS: i64 = 30;
/// Continue: the viewer's own newest corals read per session or meeting
/// source type (the owner path, `idx_atoms_connector_activity`) to find the
/// sessions and meetings they touched. Other members' items never use it up.
pub const CONTINUE_OWN_CORALS_PER_TYPE: i64 = 200;
/// Continue: owned sessions kept for grouping (newest first).
pub const CONTINUE_SESSION_ROWS: i64 = 100;
/// Continue: newest corals per shared ocean probed for one the viewer may
/// read, to date its workspace row.
pub const WORKSPACE_PROBE_PER_OCEAN: i64 = 25;
/// What's new: rows returned and the fixed window.
pub const WHATS_NEW_LIMIT: usize = 5;
pub const WHATS_NEW_WINDOW_DAYS: i64 = 7;
/// What's new: the newest reefs of one display rank that get a title
/// verdict. Bounds the verdicts whatever the window holds; more hidden reefs
/// than this ahead of every readable one in a rank leave that rank short.
pub const WHATS_NEW_CANDIDATES_PER_RANK: i64 = 200;
/// Reef kinds in display rank order: decisions and threads, then entities,
/// then references. One statement per rank, run only while fewer than
/// [`WHATS_NEW_LIMIT`] reefs are admitted.
pub const REEF_RANKS: &[&[&str]] = &[&["decision", "thread"], &["entity"], &["reference"]];
/// Attention: `has_content` first probes this many of each ocean's newest
/// corals, then the next ones up to [`CONTENT_SCAN_PER_OCEAN`].
pub const CONTENT_PROBE_PER_OCEAN: i64 = 8;
/// Attention: the most corals per ocean `has_content` examines (newest
/// first, whatever their state). An ocean whose newest this-many are all
/// unreadable counts as empty.
pub const CONTENT_SCAN_PER_OCEAN: i64 = 300;
/// Attention: the most corals `has_content` examines over all oceans; with
/// more than three oceans each gets an equal share (never below the first
/// probe). Each unreadable coral costs one verdict, so this, not the number
/// of oceans, bounds the statement.
pub const CONTENT_SCAN_TOTAL: i64 = 900;
/// Attention: the viewer's newest conversations per ocean checked for a
/// user message (`has_asked`).
pub const ASKED_PROBE_PER_OCEAN: i64 = 50;

const TITLE_MAX_CHARS: usize = 120;

// ── Errors ───────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("{0}")]
    Other(String),
}

impl From<status::Error> for Error {
    fn from(e: status::Error) -> Self {
        match e {
            status::Error::Db(db) => Error::Db(db),
        }
    }
}

impl From<crate::read_models::Error> for Error {
    fn from(e: crate::read_models::Error) -> Self {
        match e.downcast::<sqlx::Error>() {
            Ok(db) => Error::Db(*db),
            Err(other) => Error::Other(other.to_string()),
        }
    }
}

// ── Continue ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ContinueKind {
    Chat,
    Coding,
    Agent,
    Meeting,
    Workspace,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContinueItem {
    pub kind: ContinueKind,
    /// What to open: the chat id, the newest session id of a group, the
    /// meeting `source_id`, or the workspace's ocean id.
    pub id: String,
    pub title: String,
    pub context: Option<String>,
    /// Sessions in a grouped row (>= 2); otherwise `null`.
    pub count: Option<i64>,
    pub at: DateTime<Utc>,
    /// The ocean, where the route that opens the item needs it: a chat's and
    /// a meeting's (`/meeting?graph_id=&source_id=`); `null` for sessions and
    /// workspaces (a workspace's `id` is already its ocean).
    pub graph_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct ContinueResponse {
    pub items: Vec<ContinueItem>,
    pub generated_at: DateTime<Utc>,
}

/// One coding/agent/meeting session the viewer owns ([`SESSIONS_SQL`]'s row).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SessionRow {
    pub graph_id: Uuid,
    pub session_id: String,
    pub session_kind: String,
    pub project_key: Option<String>,
    pub branch: Option<String>,
    pub title: Option<String>,
    /// The harness it ran in: the declared platform, else the detected one
    /// (`SessionIdentity::effective_platform`'s rule), e.g. `claude-code`.
    pub platform: Option<String>,
    pub last_activity_at: DateTime<Utc>,
}

/// `(graph, source id, created at, duration ms, transcript head)`.
type MeetingTuple = (Uuid, String, DateTime<Utc>, Option<i64>, Option<String>);
/// `(graph, node, kind, formed at, admitted title)`.
type ReefTuple = (Uuid, Uuid, String, DateTime<Utc>, String);
/// `(graph, newest readable coral, viewer's team membership start)`.
type WorkspaceTuple = (Uuid, Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// The viewer's newest chats: per ocean an index range of
/// `idx_conversations_owner (graph_id, user_id, updated_at DESC)`, then the
/// title verdict once per returned row. `$1` oceans, `$2` viewer, `$3` limit.
pub const CHATS_SQL: &str = r#"
WITH mine AS MATERIALIZED (
  SELECT c.id, c.graph_id, c.updated_at
  FROM unnest($1::uuid[]) v(graph_id)
  CROSS JOIN LATERAL (
    SELECT c.id, c.graph_id, c.updated_at FROM conversations c
    WHERE c.graph_id = v.graph_id AND c.user_id = $2
    ORDER BY c.updated_at DESC LIMIT $3) c
  ORDER BY c.updated_at DESC, c.id LIMIT $3
)
SELECT m.id, m.graph_id, m.updated_at, t.title
FROM mine m
CROSS JOIN LATERAL (SELECT library_conversation_title(m.id) AS title OFFSET 0) t
ORDER BY m.updated_at DESC, m.id
"#;

/// The viewer's own top-level sessions, found from the owner side so other
/// members' sessions never crowd them out of a busy shared ocean.
///
/// 1. `mine`: the viewer's newest `$4` corals of each session source type in
///    the window, from `idx_atoms_connector_activity (ingested_by,
///    source_type, created_at)`. The visible oceans `$1` and the floor are
///    applied before the `LIMIT`, so the viewer's own corals in an ocean they
///    cannot see (archived, a team they left) are skipped, never counted.
/// 2. `touched`: the distinct sessions those corals belong to, by the
///    journal's longest-real-session rule
///    ([`crate::journal_grouping::session_join_sql`]; session ids may contain
///    colons, so `cc:team:alpha:c1` belongs to `team:alpha` when that session
///    exists), a sub-agent session folded into its root.
/// 3. Each top-level session of the window is kept only if its FIRST coral by
///    creation time was ingested by the viewer: one member touching
///    another's session never makes it theirs. The probe is an ordered walk
///    of `atoms_session_first_coral (graph_id, split_part(source_id, ':', 2),
///    created_at, atom_id) WHERE left(source_id, 3) = 'cc:'` (tenant 0235 /
///    control 0424): every coral of session `S` (`cc:S:...`) has the first
///    segment of `S` as its key, so the walk starts at the session's earliest
///    coral and `LIMIT 1` stops at the first one in the session's `cc:S:`
///    range that resolves to `S` (the same longest-real-session rule) and the
///    floor admits. Its cost does not grow with the session's length; only
///    corals of other sessions sharing that first segment (`team:alpha`
///    before `team`, both under `team`) that are older can be passed over.
///    The range test is `left(...) =`, which the source-id pattern index
///    cannot serve, so the planner never sorts the whole range instead.
///
/// Every lookup in steps 2 and 3 is a per-row index probe
/// (`coding_sessions_graph_id_session_id_key`), fenced with `OFFSET 0` so the
/// planner never hashes the whole `coding_sessions` table instead. Newest
/// first, at most `$5`. `$2` viewer, `$3` window start.
pub static SESSIONS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"
WITH mine AS MATERIALIZED (
  SELECT a.graph_id, a.source_type, a.source_id
  FROM unnest({sess}::text[]) t(source_type)
  CROSS JOIN LATERAL (
    SELECT a.graph_id, a.source_type, a.source_id FROM atoms a
    WHERE a.ingested_by = $2 AND a.source_type = t.source_type AND a.created_at >= $3
      AND a.graph_id = ANY($1) AND {floor}
    ORDER BY a.created_at DESC LIMIT $4) a
), touched AS MATERIALIZED (
  SELECT DISTINCT a.graph_id,
    CASE WHEN s.parent_session_id IS NULL THEN s.session_id
         ELSE COALESCE(s.root_session_id, s.parent_session_id) END AS session_id
  FROM mine a
  {session_join}
  CROSS JOIN LATERAL (
    SELECT s.session_id, s.parent_session_id, s.root_session_id FROM coding_sessions s
    WHERE s.graph_id = a.graph_id AND s.session_id = cs.session_id
    OFFSET 0) s
)
SELECT r.graph_id, r.session_id, r.session_kind, r.project_key, r.branch, r.title,
  r.platform, r.last_activity_at
FROM touched x
CROSS JOIN LATERAL (
  SELECT r.graph_id, r.session_id, r.session_kind, r.project_key, r.branch, r.title,
    COALESCE(r.platform, r.platform_detected) AS platform, r.last_activity_at
  FROM coding_sessions r
  WHERE r.graph_id = x.graph_id AND r.session_id = x.session_id
    AND r.parent_session_id IS NULL AND r.session_kind IN ('coding','agent','meeting')
    AND r.last_activity_at >= $3
  OFFSET 0) r
CROSS JOIN LATERAL (
  SELECT a.ingested_by FROM atoms a
  {session_join}
  WHERE a.graph_id = r.graph_id
    AND left(a.source_id, 3) = 'cc:'
    AND split_part(a.source_id, ':', 2) = split_part(r.session_id, ':', 1)
    AND left(a.source_id, length(r.session_id) + 4) = 'cc:' || r.session_id || ':'
    AND {floor}
    AND cs.session_id = r.session_id
  ORDER BY a.created_at, a.atom_id LIMIT 1) o
WHERE o.ingested_by = $2
ORDER BY r.last_activity_at DESC, r.session_id
LIMIT $5
"#,
        sess = *crate::ingest::atom::SESSION_SOURCE_TYPES_SQL_ARRAY,
        session_join = crate::journal_grouping::session_join_sql(),
        floor = crate::history_floor::admits_atom("a"),
    )
});

/// The viewer's own meeting transcripts, found from the owner side like
/// [`SESSIONS_SQL`]: the viewer's newest `$4` corals of each meeting capture
/// type `$6` (`home_metrics::MEETING_CAPTURE_TYPES`, the types Home counts as
/// meetings) in the window from `idx_atoms_connector_activity` (visible
/// oceans and floor applied before the `LIMIT`, as for sessions), their base
/// `source_id` (a `:chunk:N` suffix removed), the transcript of that id
/// (`meeting_transcripts_graph_id_source_id_key`, window and floor), kept
/// only if its FIRST coral by creation time (its own `source_id` or a
/// `:chunk:N` of it, `idx_atoms_graph_source_id_pattern`) is the viewer's.
/// Then the first 512 characters of the inline transcript for the title of
/// the `$5` rows kept. `$1` oceans, `$2` viewer, `$3` window start.
pub static MEETINGS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"
WITH mine AS MATERIALIZED (
  SELECT DISTINCT a.graph_id, regexp_replace(a.source_id, ':chunk:[0-9]+$', '') AS source_id
  FROM unnest($6::text[]) k(source_type)
  CROSS JOIN LATERAL (
    SELECT a.graph_id, a.source_id FROM atoms a
    WHERE a.ingested_by = $2 AND a.source_type = k.source_type AND a.created_at >= $3
      AND a.graph_id = ANY($1) AND {floor}
    ORDER BY a.created_at DESC LIMIT $4) a
), owned AS MATERIALIZED (
  SELECT t.graph_id, t.source_id, t.created_at, t.duration_ms
  FROM mine m
  JOIN meeting_transcripts t ON t.graph_id = m.graph_id AND t.source_id = m.source_id
  CROSS JOIN LATERAL (
    SELECT f.ingested_by FROM (
      SELECT a.ingested_by, a.created_at, a.atom_id FROM atoms a
      WHERE a.graph_id = t.graph_id AND a.source_id = t.source_id AND {floor}
      UNION ALL
      SELECT a.ingested_by, a.created_at, a.atom_id FROM atoms a
      WHERE a.graph_id = t.graph_id
        AND a.source_id ~>=~ (t.source_id || ':chunk:') AND a.source_id ~<~ (t.source_id || ':chunk;')
        AND {floor}
      OFFSET 0) f
    ORDER BY f.created_at, f.atom_id LIMIT 1) o
  WHERE t.created_at >= $3 AND {transcript_floor} AND o.ingested_by = $2
  ORDER BY t.created_at DESC, t.source_id LIMIT $5
)
SELECT o.graph_id, o.source_id, o.created_at, o.duration_ms,
  (SELECT left(x.raw_inline, 512) FROM meeting_transcripts x
    WHERE x.graph_id = o.graph_id AND x.source_id = o.source_id) AS head
FROM owned o
ORDER BY o.created_at DESC, o.source_id
"#,
        transcript_floor = crate::history_floor::admits_transcript("t"),
        floor = crate::history_floor::admits_atom("a"),
    )
});

/// When each shared workspace (org or team ocean, `$1`) was last active FOR
/// THE VIEWER: the newest of its `$3` newest floored corals that
/// `library_atom_permitted` admits (`idx_atoms_graph_created`, verdict in
/// order until the first admitted one), so a coral the viewer cannot read
/// never dates the row. `LEFT JOIN LATERAL`: an ocean with no readable coral
/// among them still has a row, dated by the viewer's current team membership
/// (`team_members.joined_at`); the caller falls back to the ocean's creation.
/// `$2` viewer.
pub static WORKSPACES_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"
SELECT v.graph_id, last.created_at, joined.at
FROM unnest($1::uuid[]) v(graph_id)
LEFT JOIN LATERAL (
  SELECT c.created_at FROM (
    SELECT a.graph_id, a.atom_id, a.created_at FROM atoms a
    WHERE a.graph_id = v.graph_id AND {floor}
    ORDER BY a.created_at DESC LIMIT $3) c
  CROSS JOIN LATERAL (SELECT library_atom_permitted(c.graph_id, c.atom_id) AS ok OFFSET 0) p
  WHERE p.ok
  ORDER BY c.created_at DESC LIMIT 1) last ON true
LEFT JOIN LATERAL (
  SELECT max(tm.joined_at) AS at FROM teams t JOIN team_members tm ON tm.team_id = t.id
  WHERE t.graph_id = v.graph_id AND tm.user_id = $2 AND tm.departing_at IS NULL) joined ON true
"#,
        floor = crate::history_floor::admits_atom("a"),
    )
});

const OCEANS_SQL: &str = "SELECT id, name, kind, created_at FROM graphs WHERE id = ANY($1::uuid[])";

#[derive(Debug, Clone)]
pub struct Ocean {
    pub name: String,
    pub kind: String,
    pub created_at: DateTime<Utc>,
}

async fn oceans(
    tx: &mut PgConnection,
    graphs: &[Uuid],
) -> Result<HashMap<Uuid, Ocean>, sqlx::Error> {
    let rows: Vec<(Uuid, String, String, DateTime<Utc>)> = sqlx::query_as(OCEANS_SQL)
        .bind(graphs)
        .fetch_all(&mut *tx)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name, kind, created_at)| {
            (
                id,
                Ocean {
                    name,
                    kind,
                    created_at,
                },
            )
        })
        .collect())
}

pub async fn continue_items(
    tx: &mut PgConnection,
    graphs: &[Uuid],
    viewer: Uuid,
    now: DateTime<Utc>,
) -> Result<ContinueResponse, sqlx::Error> {
    // No early return for an empty ocean list: every statement starts from
    // `unnest($1)`, so none returns a row.
    let since = now - Duration::days(CONTINUE_WINDOW_DAYS);
    let per_kind = CONTINUE_PER_KIND as i64;
    let names = oceans(tx, graphs).await?;
    let ocean_name = |g: &Uuid| names.get(g).map(|o| o.name.clone());
    let mut items = Vec::new();

    let chats: Vec<(Uuid, Uuid, DateTime<Utc>, Option<String>)> = sqlx::query_as(CHATS_SQL)
        .bind(graphs)
        .bind(viewer)
        .bind(per_kind)
        .fetch_all(&mut *tx)
        .await?;
    for (id, graph_id, at, title) in chats {
        items.push(ContinueItem {
            kind: ContinueKind::Chat,
            id: id.to_string(),
            title: clean(title.as_deref()).unwrap_or_else(|| "Conversation".into()),
            context: ocean_name(&graph_id),
            count: None,
            at,
            graph_id: Some(graph_id),
        });
    }

    let sessions: Vec<SessionRow> = sqlx::query_as(&SESSIONS_SQL)
        .bind(graphs)
        .bind(viewer)
        .bind(since)
        .bind(CONTINUE_OWN_CORALS_PER_TYPE)
        .bind(CONTINUE_SESSION_ROWS)
        .fetch_all(&mut *tx)
        .await?;
    items.extend(group_sessions(&sessions, &ocean_name));

    let meetings: Vec<MeetingTuple> = sqlx::query_as(&MEETINGS_SQL)
        .bind(graphs)
        .bind(viewer)
        .bind(since)
        .bind(CONTINUE_OWN_CORALS_PER_TYPE)
        .bind(per_kind)
        .bind(crate::services::home_metrics::MEETING_CAPTURE_TYPES)
        .fetch_all(&mut *tx)
        .await?;
    for (graph_id, source_id, at, duration_ms, head) in meetings {
        let title = head
            .as_deref()
            .and_then(crate::routes::meetings::meeting_title_from_raw)
            .unwrap_or_else(|| "Meeting".into());
        items.push(ContinueItem {
            kind: ContinueKind::Meeting,
            id: source_id,
            title,
            context: dotted([duration_label(duration_ms), ocean_name(&graph_id)]),
            count: None,
            at,
            graph_id: Some(graph_id),
        });
    }

    // Shared workspaces: the org and team oceans among the visible ones, named
    // from the map above (no second read of `graphs`).
    let shared: Vec<Uuid> = graphs
        .iter()
        .copied()
        .filter(|g| {
            names
                .get(g)
                .is_some_and(|o| o.kind == "org" || o.kind == "team")
        })
        .collect();
    let workspaces: Vec<WorkspaceTuple> = sqlx::query_as(&WORKSPACES_SQL)
        .bind(&shared)
        .bind(viewer)
        .bind(WORKSPACE_PROBE_PER_OCEAN)
        .fetch_all(&mut *tx)
        .await?;
    for (graph_id, last_readable, joined) in workspaces {
        let Some(ocean) = names.get(&graph_id) else {
            continue;
        };
        items.push(ContinueItem {
            kind: ContinueKind::Workspace,
            id: graph_id.to_string(),
            title: clean(Some(&ocean.name)).unwrap_or_else(|| "Workspace".into()),
            context: Some(
                if ocean.kind == "org" {
                    "organization ocean"
                } else {
                    "shared workspace"
                }
                .into(),
            ),
            count: None,
            at: workspace_at(last_readable, joined, ocean.created_at),
            graph_id: None,
        });
    }

    Ok(ContinueResponse {
        items: assemble_continue(items),
        generated_at: now,
    })
}

/// A workspace row's time: its newest coral the viewer can read; else (none
/// readable yet, e.g. a team ocean just joined) when the viewer joined it;
/// else when the ocean was created.
pub fn workspace_at(
    last_readable: Option<DateTime<Utc>>,
    joined: Option<DateTime<Utc>>,
    created: DateTime<Utc>,
) -> DateTime<Utc> {
    last_readable.or(joined).unwrap_or(created)
}

/// Coding and agent sessions of the same project and branch in the same ocean
/// collapse into one row (per kind: a row has one kind; the same repo and
/// branch in two oceans are two rows). A session with no project key is its
/// own row. A session of kind `meeting` (an agent-captured meeting, which has
/// no transcript for `/meeting` to open) is listed as its own `agent` row,
/// opening Today like every other session. The row opens the group's newest
/// session, and its context is that session's harness and title
/// (`claude code · last: <title>`), never the count.
///
/// Input is newest first ([`SESSIONS_SQL`]'s order), so a group's first row
/// is its newest and the groups come out newest first; the caller
/// ([`assemble_continue`]) sorts every kind together anyway.
pub fn group_sessions(
    rows: &[SessionRow],
    ocean_name: &dyn Fn(&Uuid) -> Option<String>,
) -> Vec<ContinueItem> {
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    enum Key<'a> {
        Project(Uuid, ContinueKind, &'a str, Option<&'a str>),
        One(Uuid, &'a str),
    }
    let mut order: Vec<Key> = Vec::new();
    let mut groups: HashMap<Key, Vec<&SessionRow>> = HashMap::new();
    for row in rows {
        let kind = session_kind(&row.session_kind);
        let project = row
            .project_key
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty());
        let key = match project {
            Some(p) if row.session_kind != "meeting" => Key::Project(
                row.graph_id,
                kind,
                p,
                row.branch
                    .as_deref()
                    .map(str::trim)
                    .filter(|b| !b.is_empty()),
            ),
            _ => Key::One(row.graph_id, &row.session_id),
        };
        if !groups.contains_key(&key) {
            order.push(key);
        }
        groups.entry(key).or_default().push(row);
    }
    order
        .into_iter()
        .map(|key| {
            let members = &groups[&key];
            let newest = members
                .iter()
                .max_by(|a, b| {
                    a.last_activity_at
                        .cmp(&b.last_activity_at)
                        .then_with(|| b.session_id.cmp(&a.session_id))
                })
                .expect("a group has a member");
            let n = members.len() as i64;
            let own_title = clean(newest.title.as_deref());
            let harness = harness_label(newest.platform.as_deref());
            let (title, context) = match &key {
                Key::Project(_, _, project, branch) => {
                    let repo = repo_name(project);
                    let title = match branch {
                        Some(b) => format!("{repo} \u{b7} {b}"),
                        None => repo.to_string(),
                    };
                    let last = own_title.map(|t| format!("last: {t}"));
                    (title, dotted([harness, last]))
                }
                Key::One(..) => (
                    own_title.unwrap_or_else(|| {
                        if newest.session_kind == "meeting" {
                            "Meeting session".into()
                        } else {
                            "Untitled session".into()
                        }
                    }),
                    dotted([harness, ocean_name(&newest.graph_id)]),
                ),
            };
            ContinueItem {
                kind: session_kind(&newest.session_kind),
                id: newest.session_id.clone(),
                title,
                context,
                count: (n >= 2).then_some(n),
                at: newest.last_activity_at,
                // Sessions open Today, which needs no ocean.
                graph_id: None,
            }
        })
        .collect()
}

/// Newest first, at most [`CONTINUE_PER_KIND`] of any kind, at most
/// [`CONTINUE_LIMIT`] rows. Ties break on kind then id so the order is stable.
pub fn assemble_continue(mut items: Vec<ContinueItem>) -> Vec<ContinueItem> {
    items.sort_by(|a, b| {
        b.at.cmp(&a.at)
            .then_with(|| (a.kind as u8).cmp(&(b.kind as u8)))
            .then_with(|| a.id.cmp(&b.id))
    });
    let mut per_kind: HashMap<ContinueKind, usize> = HashMap::new();
    let mut out = Vec::with_capacity(CONTINUE_LIMIT);
    for item in items {
        let n = per_kind.entry(item.kind).or_default();
        if *n >= CONTINUE_PER_KIND {
            continue;
        }
        *n += 1;
        out.push(item);
        if out.len() == CONTINUE_LIMIT {
            break;
        }
    }
    out
}

/// A session's Continue kind. `meeting` sessions are `agent` rows: a
/// meeting row's id is always a transcript `source_id` (the contract), and a
/// session has none.
fn session_kind(kind: &str) -> ContinueKind {
    match kind {
        "agent" | "meeting" => ContinueKind::Agent,
        _ => ContinueKind::Coding,
    }
}

/// `claude-code` -> `claude code`: the platform slug as words.
fn harness_label(platform: Option<&str>) -> Option<String> {
    let words = platform?
        .split(['-', '_', ' '])
        .filter(|w| !w.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    clean(Some(&words))
}

/// The present parts joined with ` · `; nothing present is `None`.
fn dotted<const N: usize>(parts: [Option<String>; N]) -> Option<String> {
    let parts: Vec<String> = parts.into_iter().flatten().collect();
    (!parts.is_empty()).then(|| parts.join(" \u{b7} "))
}

/// `github.com/acme/coralswarm` -> `coralswarm`; a fallback key is its own name.
fn repo_name(project_key: &str) -> &str {
    project_key
        .trim_end_matches('/')
        .rsplit('/')
        .find(|s| !s.is_empty())
        .unwrap_or(project_key)
}

fn duration_label(ms: Option<i64>) -> Option<String> {
    let minutes = ms.filter(|m| *m > 0)? / 60_000;
    Some(if minutes >= 1 {
        format!("{minutes} min")
    } else {
        "under a minute".into()
    })
}

/// One line, trimmed, at most [`TITLE_MAX_CHARS`] characters; empty is `None`.
fn clean(s: Option<&str>) -> Option<String> {
    let line = s?.lines().map(str::trim).find(|l| !l.is_empty())?;
    let mut out: String = line.chars().take(TITLE_MAX_CHARS).collect();
    if line.chars().count() > TITLE_MAX_CHARS {
        out.push('\u{2026}');
    }
    Some(out)
}

// ── What's new ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct OceanRef {
    pub graph_id: Uuid,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct WhatsNewItem {
    pub reef_id: Uuid,
    pub kind: String,
    pub title: String,
    pub ocean: OceanRef,
    pub formed_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct WhatsNewResponse {
    pub items: Vec<WhatsNewItem>,
    pub window_days: i64,
    pub generated_at: DateTime<Utc>,
}

/// The newest admitted reefs of ONE display rank (kinds `$2`) in the window.
/// Candidates: per (ocean `$1`, kind) the newest `$5` reefs formed at or
/// after `$3`, a backward range of `nodes_graph_type_created (graph_id, type,
/// created_at, node_id)` read index-only, merged newest first and cut to the
/// rank's `$5` newest. The title verdict then runs once per candidate in that
/// order (`OFFSET 0` lateral, verdict-once) and `LIMIT $4` stops it at the
/// last reef needed. So a rank costs at most `$5` verdicts: a hidden reef
/// delays the answer and, only when `$5` hidden reefs are newer than every
/// readable one of the rank, leaves the rank short (the next rank fills in).
/// A title must have a visible character (it is shown as one line). The outer
/// `ORDER BY` is the inner one, so SQL picks the rows and the plan needs no
/// second sort.
pub const WHATS_NEW_SQL: &str = r#"
SELECT c.graph_id, c.node_id, c.type, c.created_at, t.title
FROM (
  SELECT n.graph_id, n.node_id, n.type, n.created_at
  FROM unnest($1::uuid[]) v(graph_id)
  CROSS JOIN unnest($2::text[]) k(type)
  CROSS JOIN LATERAL (
    SELECT n.graph_id, n.node_id, n.type, n.created_at FROM nodes n
    WHERE n.graph_id = v.graph_id AND n.type = k.type AND n.created_at >= $3
    ORDER BY n.created_at DESC, n.node_id DESC LIMIT $5) n
  ORDER BY n.created_at DESC, n.node_id DESC
  LIMIT $5
) c
CROSS JOIN LATERAL (SELECT library_node_title(c.graph_id, c.node_id) AS title OFFSET 0) t
WHERE t.title ~ '\S'
ORDER BY c.created_at DESC, c.node_id DESC
LIMIT $4
"#;

/// Up to [`WHATS_NEW_LIMIT`] reefs: rank by rank ([`REEF_RANKS`]), each
/// rank's statement asked only for the rows still missing, and a lower rank
/// read only when the higher ones left room. The statements see only the
/// oceans the name map holds, so every row they return is listed and counted.
pub async fn whats_new(
    tx: &mut PgConnection,
    graphs: &[Uuid],
    now: DateTime<Utc>,
) -> Result<WhatsNewResponse, sqlx::Error> {
    let mut items = Vec::new();
    let since = now - Duration::days(WHATS_NEW_WINDOW_DAYS);
    let names = oceans(tx, graphs).await?;
    // The statements read only these oceans, so each returned row's ocean is
    // in `names`.
    let named: Vec<Uuid> = graphs
        .iter()
        .copied()
        .filter(|g| names.contains_key(g))
        .collect();
    for kinds in REEF_RANKS {
        // No ocean is not a special case: `unnest($1)` yields no candidate.
        let missing = WHATS_NEW_LIMIT.saturating_sub(items.len());
        if missing == 0 {
            break;
        }
        let rows: Vec<ReefTuple> = sqlx::query_as(WHATS_NEW_SQL)
            .bind(&named)
            .bind(*kinds)
            .bind(since)
            .bind(missing as i64)
            .bind(WHATS_NEW_CANDIDATES_PER_RANK)
            .fetch_all(&mut *tx)
            .await?;
        items.extend(rows.into_iter().map(|row| {
            let ocean = &names[&row.0];
            reef_item(row, ocean)
        }));
    }
    Ok(WhatsNewResponse {
        items,
        window_days: WHATS_NEW_WINDOW_DAYS,
        generated_at: now,
    })
}

/// One What's new row in `ocean`: the title as one line of at most
/// [`TITLE_MAX_CHARS`] (the same [`clean`] as Continue).
fn reef_item(
    (graph_id, node_id, kind, formed_at, title): ReefTuple,
    ocean: &Ocean,
) -> WhatsNewItem {
    WhatsNewItem {
        reef_id: node_id,
        kind,
        title: clean(Some(&title)).unwrap_or_else(|| "Untitled reef".into()),
        ocean: OceanRef {
            graph_id,
            name: ocean.name.clone(),
            kind: ocean.kind.clone(),
        },
        formed_at,
    }
}

// ── Attention ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AttentionReason {
    Reauth,
    Failing,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Attention {
    pub connector_id: String,
    pub provider: String,
    pub label: String,
    pub reason: AttentionReason,
}

#[derive(Debug, Serialize)]
pub struct AttentionResponse {
    pub has_content: bool,
    /// At least one non-MCP connector is connected, whatever its health
    /// (setup checklist step 1).
    pub has_source: bool,
    /// An MCP client is connected (setup checklist step 2).
    pub has_mcp: bool,
    /// The viewer has asked at least one question: one of their
    /// conversations, from any surface, has a user message (checklist step 3).
    pub has_asked: bool,
    pub attention: Option<Attention>,
    pub generated_at: DateTime<Utc>,
}

/// Can the viewer read at least one coral? An `EXISTS`, never a count: per
/// ocean the corals newest first, `created_at DESC, atom_id DESC`
/// (`corpus_atoms_snapshot_page (graph_id, created_at, atom_id)` backward),
/// each admitted by the Library's `library_atom_permitted` (floored;
/// retracted and derived-copy corals skipped before the verdict, which
/// rejects both anyway) until the first hit. The `atom_id` tiebreak makes the
/// order total: one capture writes all its chunk corals with one
/// `created_at`, and the two windows below must split such a tie without
/// judging a coral twice or skipping one.
///
/// One ordered statement. Its driving rows are `(ocean, offset, count)`
/// windows ([`content_windows`]): first every ocean's
/// [`CONTENT_PROBE_PER_OCEAN`] newest corals, so one ocean full of unreadable
/// corals cannot delay a hit in the next, then every ocean's next ones up to
/// [`content_scan_per_ocean`], starting after the first window (no coral is
/// judged twice). The windows count index entries, not admitted corals, so
/// retracted or floored corals use up the bound too: the statement reads at
/// most [`CONTENT_SCAN_PER_OCEAN`] corals per ocean and about
/// [`CONTENT_SCAN_TOTAL`] in all, whatever the oceans hold.
///
/// No semantic-atom branch. A semantic atom is never shown on its own, and
/// every captured source version comes with its chunk corals (the one
/// capture path, `ingest::capture`), which this verdict admits exactly when
/// the source is permitted. So "the viewer can read a coral" is the answer.
pub static HAS_CONTENT_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"
SELECT EXISTS (
  SELECT 1 FROM unnest($1::uuid[], $2::bigint[], $3::bigint[]) v(graph_id, skip, take)
  CROSS JOIN LATERAL (
    SELECT a.graph_id, a.atom_id, a."timestamp", a.evidence_retracted, a.derived_copy_bound
    FROM atoms a
    WHERE a.graph_id = v.graph_id
    ORDER BY a.created_at DESC, a.atom_id DESC OFFSET v.skip LIMIT v.take) a
  CROSS JOIN LATERAL (SELECT library_atom_permitted(a.graph_id, a.atom_id) AS ok OFFSET 0) p
  WHERE {floor} AND NOT a.evidence_retracted AND NOT a.derived_copy_bound AND p.ok
)
"#,
        floor = crate::history_floor::admits_atom("a"),
    )
});

/// [`HAS_CONTENT_SQL`]'s driving windows, in order: `(oceans, offsets,
/// counts)`, first `[0, probe)` of every ocean, then `[probe, scan)`.
pub fn content_windows(graphs: &[Uuid], probe: i64, scan: i64) -> (Vec<Uuid>, Vec<i64>, Vec<i64>) {
    let probe = probe.clamp(0, scan.max(0));
    let scan = scan.max(0);
    let mut out = (Vec::new(), Vec::new(), Vec::new());
    for (skip, take) in [(0, probe), (probe, scan - probe)] {
        if take <= 0 {
            continue;
        }
        for g in graphs {
            out.0.push(*g);
            out.1.push(skip);
            out.2.push(take);
        }
    }
    out
}

/// How many of each ocean's newest corals `has_content` examines:
/// [`CONTENT_SCAN_PER_OCEAN`], or an equal share of [`CONTENT_SCAN_TOTAL`]
/// when that is smaller, never below [`CONTENT_PROBE_PER_OCEAN`].
pub fn content_scan_per_ocean(oceans: usize) -> i64 {
    let share = CONTENT_SCAN_TOTAL / (oceans.max(1) as i64);
    share.clamp(CONTENT_PROBE_PER_OCEAN, CONTENT_SCAN_PER_OCEAN)
}

/// True iff a visible ocean has a coral the viewer may read among its
/// [`content_scan_per_ocean`] newest (300 with up to three oceans). A false
/// negative needs every ocean's newest that-many corals to be unreadable
/// while an older one is readable.
pub async fn has_content(tx: &mut PgConnection, graphs: &[Uuid]) -> Result<bool, sqlx::Error> {
    let (oceans, skips, takes) = content_windows(
        graphs,
        CONTENT_PROBE_PER_OCEAN,
        content_scan_per_ocean(graphs.len()),
    );
    sqlx::query_scalar(&HAS_CONTENT_SQL)
        .bind(oceans)
        .bind(skips)
        .bind(takes)
        .fetch_one(&mut *tx)
        .await
}

/// Has the viewer asked anything? Per ocean the viewer's newest `$3`
/// conversations (`idx_conversations_owner (graph_id, user_id, updated_at
/// DESC)`), each probed for one user message (`idx_conversation_messages_thread
/// (conversation_id, seq)`, the first rows of the thread). Any surface: the
/// web chat, scoped asks and MCP asks all write `conversations`. `EXISTS`, so
/// the first asked conversation ends it. `$1` oceans, `$2` viewer.
pub const HAS_ASKED_SQL: &str = r#"
SELECT EXISTS (
  SELECT 1 FROM unnest($1::uuid[]) v(graph_id)
  CROSS JOIN LATERAL (
    SELECT c.id FROM conversations c
    WHERE c.graph_id = v.graph_id AND c.user_id = $2
    ORDER BY c.updated_at DESC LIMIT $3) c
  WHERE EXISTS (SELECT 1 FROM conversation_messages m
                WHERE m.conversation_id = c.id AND m.role = 'user')
)
"#;

pub async fn has_asked(
    tx: &mut PgConnection,
    graphs: &[Uuid],
    viewer: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(HAS_ASKED_SQL)
        .bind(graphs)
        .bind(viewer)
        .bind(ASKED_PROBE_PER_OCEAN)
        .fetch_one(&mut *tx)
        .await
}

/// The first broken connector: the first `failing` row in the
/// connector-status order ([`status::sort_rows`]). [`attention`] assembles
/// the rows without the 30-day counts, so among failing rows that order is
/// by name, then id (the status page puts more volume first). `reauth` when
/// its credential is dead, else `failing`. `stale` and `healthy` rows never
/// need attention.
pub fn first_broken(rows: &[status::ConnectorStatusRow]) -> Option<Attention> {
    let row = rows.iter().find(|r| r.state == State::Failing)?;
    let (reason, label) = if row.reauth {
        (
            AttentionReason::Reauth,
            format!("{} needs you to sign in again", row.display_name),
        )
    } else {
        (
            AttentionReason::Failing,
            format!("{} stopped syncing", row.display_name),
        )
    };
    Some(Attention {
        connector_id: row.instance_id.clone(),
        provider: row.provider.clone(),
        label,
        reason,
    })
}

/// The setup checklist's first two steps from the connector-status rows:
/// `(has_source, has_mcp)`. A source is any non-MCP row (OAuth or Composio
/// connection, any health, after the org's connector flags); MCP is the row
/// `assemble` adds once the viewer's MCP client has made a call.
pub fn setup_steps(rows: &[status::ConnectorStatusRow]) -> (bool, bool) {
    let mcp = |r: &&status::ConnectorStatusRow| r.provider == status::MCP_PROVIDER;
    (rows.iter().any(|r| !mcp(&r)), rows.iter().any(|r| mcp(&r)))
}

/// The control-plane facts attention needs: the connector-status control
/// inputs and the org-hidden providers
/// ([`crate::read_models::connectors::status_control_inputs`]).
pub struct ControlFacts {
    inputs: status::ControlInputs,
    hidden: HashSet<String>,
}

/// Reads [`ControlFacts`] on `control` alone. The route runs it and releases
/// `control` BEFORE it opens the content transaction: on the personal plane
/// both come from one pool, and a request holding one pooled connection while
/// it waits for a second from the same pool can starve that pool under
/// concurrent load.
pub async fn attention_control_facts(
    control: &mut Transaction<'static, Postgres>,
    account_id: Uuid,
    viewer: Uuid,
) -> Result<ControlFacts, Error> {
    let (inputs, hidden) =
        crate::read_models::connectors::status_control_inputs(control, account_id, viewer).await?;
    Ok(ControlFacts { inputs, hidden })
}

/// Everything else, on the content transaction only: `has_content`,
/// `has_asked`, the content-plane connector inputs merged into `facts`, the
/// connector-status rows, and from them `has_source`, `has_mcp` and the first
/// broken connector.
pub async fn attention(
    content: &mut Transaction<'static, Postgres>,
    facts: ControlFacts,
    account_id: Uuid,
    viewer: Uuid,
    graphs: &[Uuid],
    now: DateTime<Utc>,
) -> Result<AttentionResponse, Error> {
    let has_content = has_content(content, graphs).await?;
    let has_asked = has_asked(content, graphs, viewer).await?;
    let ControlFacts { mut inputs, hidden } = facts;
    crate::read_models::connectors::merge_content_inputs(
        content,
        &mut inputs,
        account_id,
        viewer,
        graphs,
        now,
    )
    .await?;
    let visible = |p: &str| !hidden.contains(p);
    // No 30-day atom counts. `failing` never depends on them (it is a dead
    // credential or an unresolved error); they only decide `stale` and the
    // volume tiebreak between rows of one state. The status page reads them
    // through `fetch_atom_stats`, a whole-ocean admission (320 to 395 ms on
    // the seed) that could push this page read past its 1 s budget, and
    // Home's availability matters more than matching that tiebreak. So with
    // two or more failing rows the first is the first by name (then id),
    // where the status page may list a busier one first.
    let rows = status::assemble(&inputs, &AtomStats::default(), Some(&visible), now).connectors;
    let (has_source, has_mcp) = setup_steps(&rows);
    Ok(AttentionResponse {
        has_content,
        has_source,
        has_mcp,
        has_asked,
        attention: first_broken(&rows),
        generated_at: now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(h: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 3, 12, 0, 0).unwrap() - Duration::hours(h)
    }
    fn item(kind: ContinueKind, id: &str, hours_ago: i64) -> ContinueItem {
        ContinueItem {
            kind,
            id: id.into(),
            title: id.into(),
            context: None,
            count: None,
            at: at(hours_ago),
            graph_id: None,
        }
    }
    fn session(
        id: &str,
        kind: &str,
        project: Option<&str>,
        branch: Option<&str>,
        hours_ago: i64,
    ) -> SessionRow {
        SessionRow {
            graph_id: Uuid::from_u128(7),
            session_id: id.into(),
            session_kind: kind.into(),
            project_key: project.map(Into::into),
            branch: branch.map(Into::into),
            title: Some(format!("{id} title")),
            platform: Some("claude-code".into()),
            last_activity_at: at(hours_ago),
        }
    }
    fn no_ocean(_: &Uuid) -> Option<String> {
        Some("Platform".into())
    }

    #[test]
    fn continue_is_newest_first_capped_at_three_per_kind_and_six_rows() {
        let mut items = vec![];
        for i in 0..5 {
            items.push(item(ContinueKind::Chat, &format!("chat{i}"), i));
        }
        for i in 0..2 {
            items.push(item(ContinueKind::Meeting, &format!("mtg{i}"), 10 + i));
        }
        for i in 0..4 {
            items.push(item(ContinueKind::Workspace, &format!("ws{i}"), 20 + i));
        }
        let out = assemble_continue(items);
        let ids: Vec<&str> = out.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, ["chat0", "chat1", "chat2", "mtg0", "mtg1", "ws0"]);
        assert!(out.windows(2).all(|w| w[0].at >= w[1].at));
    }

    #[test]
    fn continue_never_exceeds_six_rows_or_three_of_a_kind() {
        let kinds = [
            ContinueKind::Chat,
            ContinueKind::Coding,
            ContinueKind::Agent,
            ContinueKind::Meeting,
            ContinueKind::Workspace,
        ];
        let mut items = vec![];
        for (k, kind) in kinds.iter().enumerate() {
            for i in 0..4 {
                items.push(item(*kind, &format!("{k}-{i}"), (i * 5 + k) as i64));
            }
        }
        let out = assemble_continue(items);
        assert_eq!(out.len(), CONTINUE_LIMIT);
        for kind in kinds {
            assert!(out.iter().filter(|i| i.kind == kind).count() <= CONTINUE_PER_KIND);
        }
        assert!(assemble_continue(vec![]).is_empty());
    }

    #[test]
    fn sessions_group_by_kind_project_and_branch() {
        let rows = vec![
            session(
                "s5",
                "coding",
                Some("github.com/acme/coralswarm"),
                Some("cs-home"),
                1,
            ),
            session(
                "s4",
                "agent",
                Some("github.com/acme/coralswarm"),
                Some("cs-home"),
                2,
            ),
            session(
                "s3",
                "coding",
                Some("github.com/acme/coralswarm"),
                Some("cs-home"),
                3,
            ),
            session(
                "s2",
                "coding",
                Some("github.com/acme/coralswarm"),
                Some("main"),
                4,
            ),
            session(
                "s1",
                "coding",
                Some("github.com/acme/coralswarm"),
                Some("cs-home"),
                5,
            ),
            session("s0", "coding", None, None, 6),
        ];
        let out = group_sessions(&rows, &no_ocean);
        let summary: Vec<(ContinueKind, &str, &str, Option<i64>)> = out
            .iter()
            .map(|i| (i.kind, i.id.as_str(), i.title.as_str(), i.count))
            .collect();
        assert_eq!(
            summary,
            [
                (
                    ContinueKind::Coding,
                    "s5",
                    "coralswarm \u{b7} cs-home",
                    Some(3)
                ),
                (ContinueKind::Agent, "s4", "coralswarm \u{b7} cs-home", None),
                (ContinueKind::Coding, "s2", "coralswarm \u{b7} main", None),
                (ContinueKind::Coding, "s0", "s0 title", None),
            ]
        );
        assert_eq!(
            out[0].context.as_deref(),
            Some("claude code \u{b7} last: s5 title"),
            "a grouped row's context is its newest session's harness and title, never the count"
        );
        assert_eq!(out[0].at, at(1), "a group is as new as its newest session");
        assert_eq!(
            out[1].context.as_deref(),
            Some("claude code \u{b7} last: s4 title")
        );
        assert_eq!(
            out[3].context.as_deref(),
            Some("claude code \u{b7} Platform")
        );
        assert!(
            out.iter().all(|i| i.graph_id.is_none()),
            "sessions open Today"
        );
    }

    #[test]
    fn the_same_repo_and_branch_in_two_oceans_are_two_rows() {
        let in_ocean = |id: &str, graph: u128, hours_ago: i64| SessionRow {
            graph_id: Uuid::from_u128(graph),
            ..session(
                id,
                "coding",
                Some("github.com/acme/coralswarm"),
                Some("cs-home"),
                hours_ago,
            )
        };
        let rows = vec![
            in_ocean("s4", 7, 1),
            in_ocean("s3", 8, 2),
            in_ocean("s2", 7, 3),
            in_ocean("s1", 8, 4),
        ];
        let out = group_sessions(&rows, &no_ocean);
        let summary: Vec<(&str, &str, Option<i64>)> = out
            .iter()
            .map(|i| (i.id.as_str(), i.title.as_str(), i.count))
            .collect();
        assert_eq!(
            summary,
            [
                ("s4", "coralswarm \u{b7} cs-home", Some(2)),
                ("s3", "coralswarm \u{b7} cs-home", Some(2)),
            ],
            "one row per ocean, each opening its own newest session"
        );
    }

    #[test]
    fn session_context_omits_what_is_missing() {
        let mut bare = session("s1", "coding", Some("github.com/acme/x"), Some("b"), 1);
        bare.platform = None;
        bare.title = None;
        let out = group_sessions(&[bare.clone()], &|_| None);
        assert_eq!(out[0].context, None);
        bare.platform = Some("gemini_cli".into());
        let out = group_sessions(&[bare], &|_| None);
        assert_eq!(out[0].context.as_deref(), Some("gemini cli"));
    }

    #[test]
    fn a_meeting_session_is_its_own_agent_row_never_a_meeting() {
        // A `coding_sessions` row of kind `meeting` has no transcript for
        // `/meeting?source_id=` to open, so it lists as an agent session.
        let rows = vec![
            session(
                "a1",
                "agent",
                Some("github.com/acme/coralswarm"),
                Some("cs-home"),
                1,
            ),
            session(
                "m1",
                "meeting",
                Some("github.com/acme/coralswarm"),
                Some("cs-home"),
                2,
            ),
            SessionRow {
                title: None,
                ..session("m2", "meeting", None, None, 3)
            },
        ];
        let out = group_sessions(&rows, &no_ocean);
        let summary: Vec<(ContinueKind, &str, &str, Option<i64>)> = out
            .iter()
            .map(|i| (i.kind, i.id.as_str(), i.title.as_str(), i.count))
            .collect();
        assert_eq!(
            summary,
            [
                (ContinueKind::Agent, "a1", "coralswarm \u{b7} cs-home", None),
                (ContinueKind::Agent, "m1", "m1 title", None),
                (ContinueKind::Agent, "m2", "Meeting session", None),
            ],
            "never grouped with agent sessions of the same branch"
        );
        assert!(out
            .iter()
            .all(|i| i.kind != ContinueKind::Meeting && i.graph_id.is_none()));
    }

    #[test]
    fn a_workspace_is_dated_by_readable_content_then_membership_then_creation() {
        assert_eq!(workspace_at(Some(at(1)), Some(at(5)), at(9)), at(1));
        assert_eq!(workspace_at(None, Some(at(5)), at(9)), at(5));
        assert_eq!(workspace_at(None, None, at(9)), at(9));
    }

    #[test]
    fn grouped_sessions_still_obey_the_per_kind_cap() {
        let rows: Vec<SessionRow> = (0..8)
            .map(|i| {
                session(
                    &format!("s{i}"),
                    "coding",
                    Some("github.com/acme/repo"),
                    Some(&format!("b{}", i % 4)),
                    i,
                )
            })
            .collect();
        let grouped = group_sessions(&rows, &no_ocean);
        assert_eq!(grouped.len(), 4);
        assert!(grouped.iter().all(|g| g.count == Some(2)));
        let out = assemble_continue(grouped);
        assert_eq!(out.len(), 3);
        assert_eq!(
            out.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            ["s0", "s1", "s2"]
        );
    }

    #[test]
    fn helpers_name_repos_durations_and_titles() {
        assert_eq!(repo_name("github.com/acme/coralswarm"), "coralswarm");
        assert_eq!(repo_name("github.com/acme/coralswarm/"), "coralswarm");
        assert_eq!(repo_name("scratch"), "scratch");
        assert_eq!(duration_label(Some(34 * 60_000)).as_deref(), Some("34 min"));
        assert_eq!(duration_label(Some(0)), None);
        assert_eq!(duration_label(None), None);
        assert_eq!(
            clean(Some("\n  First line \nsecond")).as_deref(),
            Some("First line")
        );
        assert_eq!(clean(Some("   ")), None);
        assert_eq!(
            clean(Some(&"x".repeat(130))).unwrap().chars().count(),
            TITLE_MAX_CHARS + 1
        );
    }

    fn row(provider: &str, name: &str, state: State, reauth: bool) -> status::ConnectorStatusRow {
        status::ConnectorStatusRow {
            provider: provider.into(),
            instance_id: format!("{provider}:-:member"),
            display_name: name.into(),
            kind_label: "chat".into(),
            state,
            last_synced_at: None,
            last_error_at: None,
            message: None,
            items_30d: 0,
            action: None,
            reauth,
        }
    }

    #[test]
    fn attention_is_the_first_failing_row_in_connector_status_order() {
        assert_eq!(first_broken(&[]), None);
        assert_eq!(
            first_broken(&[
                row("slack", "Slack", State::Stale, false),
                row("mcp", "MCP agents", State::Healthy, false)
            ]),
            None,
            "stale and healthy never need attention"
        );
        // Attention assembles the rows with no 30-day counts, so failing rows
        // sort by name (then id); Home takes the first, whatever its reason.
        let mut rows = vec![
            row("slack", "Slack", State::Failing, true),
            row("microsoft", "Microsoft", State::Failing, true),
            row("github", "GitHub", State::Failing, false),
            row("gmail", "Gmail", State::Stale, false),
        ];
        status::sort_rows(&mut rows);
        let a = first_broken(&rows).unwrap();
        assert_eq!(a.provider, "github", "first failing row by name");
        assert_eq!(a.reason, AttentionReason::Failing);
        assert_eq!(a.label, "GitHub stopped syncing");
        assert_eq!(a.connector_id, "github:-:member");
        let a = first_broken(&rows[1..]).unwrap();
        assert_eq!(a.provider, "microsoft");
        assert_eq!(a.reason, AttentionReason::Reauth);
        assert_eq!(a.label, "Microsoft needs you to sign in again");
    }

    #[test]
    fn setup_steps_read_sources_and_mcp_from_the_rows() {
        assert_eq!(setup_steps(&[]), (false, false));
        let mcp = row("mcp", "MCP agents", State::Healthy, false);
        assert_eq!(setup_steps(std::slice::from_ref(&mcp)), (false, true));
        let broken = row("slack", "Slack", State::Failing, true);
        assert_eq!(
            setup_steps(std::slice::from_ref(&broken)),
            (true, false),
            "a source counts whatever its health"
        );
        assert_eq!(setup_steps(&[broken, mcp]), (true, true));
    }

    #[test]
    fn responses_serialize_to_the_contract_shape() {
        let v = serde_json::to_value(item(ContinueKind::Workspace, "w", 0)).unwrap();
        assert_eq!(v["kind"], "workspace");
        assert!(v["count"].is_null() && v["context"].is_null() && v["graph_id"].is_null());
        assert!(v["at"].as_str().unwrap().ends_with('Z'));
        let v = serde_json::to_value(AttentionResponse {
            has_content: false,
            has_source: true,
            has_mcp: false,
            has_asked: true,
            attention: None,
            generated_at: at(0),
        })
        .unwrap();
        assert_eq!(v["has_content"], false);
        assert_eq!(v["has_source"], true);
        assert_eq!(v["has_mcp"], false);
        assert_eq!(v["has_asked"], true);
        assert!(v["attention"].is_null());
        assert_eq!(
            v.as_object().unwrap().len(),
            6,
            "exactly the contract fields: {v}"
        );
        assert_eq!(
            serde_json::to_value(AttentionReason::Reauth).unwrap(),
            "reauth"
        );
        let v = serde_json::to_value(row("slack", "Slack", State::Failing, true)).unwrap();
        assert!(
            v.get("reauth").is_none(),
            "the status route body is unchanged"
        );
    }

    #[test]
    fn content_windows_probe_every_ocean_first_then_continue_after_the_probe() {
        let (g1, g2) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let (oceans, skips, takes) = content_windows(&[g1, g2], 8, 300);
        assert_eq!(
            oceans,
            [g1, g2, g1, g2],
            "every ocean's first window before any second one"
        );
        assert_eq!(
            skips,
            [0, 0, 8, 8],
            "the second window starts after the first: no coral judged twice"
        );
        assert_eq!(takes, [8, 8, 292, 292]);
        for g in [g1, g2] {
            let total: i64 = oceans
                .iter()
                .zip(&takes)
                .filter(|(o, _)| **o == g)
                .map(|(_, t)| t)
                .sum();
            assert_eq!(
                total,
                CONTENT_SCAN_PER_OCEAN.min(300),
                "at most the scan bound per ocean"
            );
        }
        assert_eq!(content_windows(&[], 8, 300), (vec![], vec![], vec![]));
        assert_eq!(content_scan_per_ocean(0), CONTENT_SCAN_PER_OCEAN);
        assert_eq!(
            content_scan_per_ocean(3),
            300,
            "up to three oceans: 300 each"
        );
        assert_eq!(
            content_scan_per_ocean(10),
            90,
            "more oceans share the total"
        );
        assert_eq!(
            content_scan_per_ocean(500),
            CONTENT_PROBE_PER_OCEAN,
            "never below the probe"
        );
        for n in 1..40 {
            assert!(
                content_scan_per_ocean(n) * n as i64
                    <= CONTENT_SCAN_TOTAL.max(CONTENT_PROBE_PER_OCEAN * n as i64)
            );
        }
        let (_, skips, takes) = content_windows(&[g1], 8, 8);
        assert_eq!((skips, takes), (vec![0], vec![8]), "no empty second window");
        let (_, skips, takes) = content_windows(&[g1], 50, 20);
        assert_eq!(
            (skips, takes),
            (vec![0], vec![20]),
            "the probe never exceeds the bound"
        );
    }

    #[test]
    fn whats_new_titles_are_one_clean_line_like_continue() {
        let g = Uuid::from_u128(3);
        let ocean = Ocean {
            name: "Platform".into(),
            kind: "team".into(),
            created_at: at(9),
        };
        let row = |title: &str| {
            (
                g,
                Uuid::from_u128(4),
                "decision".to_string(),
                at(1),
                title.to_string(),
            )
        };
        let item = reef_item(row("\n  Ship usage billing \nsecond line"), &ocean);
        assert_eq!(item.title, "Ship usage billing");
        assert_eq!(
            item.ocean,
            OceanRef {
                graph_id: g,
                name: "Platform".into(),
                kind: "team".into()
            }
        );
        let long = reef_item(row(&"y".repeat(300)), &ocean);
        assert_eq!(
            long.title.chars().count(),
            TITLE_MAX_CHARS + 1,
            "120 characters and an ellipsis"
        );
        assert!(long.title.ends_with('\u{2026}'));
    }

    #[test]
    fn has_content_windows_split_a_created_at_tie_by_atom_id() {
        // One capture writes all its chunk corals with one `created_at`; the
        // probe window and the scan window must agree on one total order.
        assert!(
            HAS_CONTENT_SQL.contains("ORDER BY a.created_at DESC, a.atom_id DESC OFFSET v.skip"),
            "{}",
            *HAS_CONTENT_SQL
        );
    }
}
