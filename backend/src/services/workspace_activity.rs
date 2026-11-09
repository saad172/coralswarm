//! Home dashboard "From your workspaces" (`GET /v1/home/workspace-activity`):
//! one row per SHARED ocean in the current tenant that has new corals in the
//! window, with a one-line headline such as `Auth rewrite reef updated · 14
//! corals`. Contract: `docs/implementation/workspace-activity-api.md`;
//! validation: `docs/implementation/workspace-activity-validation.md`.
//!
//! Compute-only helpers for the stored read-model worker; HTTP reads its row.
//! There is no
//! events table; activity is derived from `atoms` (and their reef links in
//! `node_atoms`), so it can never disagree with the Library.
//!
//! # Scope
//!
//! The caller's reachable oceans in the CURRENT tenant and account (the
//! ADR-0007 D9 union `GET /v1/graphs` returns), narrowed to the shared kinds
//! `org` and `team`. The caller's own `member` (private) ocean, their
//! `personal` ocean, another member's private ocean, a team they are not on
//! and an archived ocean are never rows. Production connects as the table
//! owner, which bypasses RLS, so the member boundary is applied in SQL inside
//! `Tenant::begin_member_scoped`:
//!
//! - corals: `library_permitted_atoms(graph, ids)` (retracted, derived-copy and
//!   denied-source atoms are out, and the team-seat history floor of
//!   `history_floor.rs` applies), run over the window's atom ids only;
//! - reefs: the shared admitted title projector implements the Library's
//!   hidden-title rule: a reef whose title the caller may not see is
//!   never named. It is skipped as a headline and the next visible reef (or no
//!   reef at all) is used.

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::PgConnection;
use uuid::Uuid;

// ── Flag ─────────────────────────────────────────────────────────────────

pub const FLAG: &str = "HOME_DASHBOARD_ENABLED";

/// `HOME_DASHBOARD_ENABLED` is `1`/`true`. Unset = off (the route answers 404).
/// Read per request, like `ORG_DELETION_ENABLED`.
pub fn enabled() -> bool {
    std::env::var(FLAG)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// ── Parameters ───────────────────────────────────────────────────────────

pub const DEFAULT_LIMIT: u32 = 6;
pub const MAX_LIMIT: u32 = 20;
pub const DEFAULT_SINCE_DAYS: u32 = 7;
pub const MAX_SINCE_DAYS: u32 = 30;

#[derive(Debug, PartialEq, Eq)]
pub enum ParamError {
    Limit,
    SinceDays,
}

impl ParamError {
    pub fn message(&self) -> String {
        match self {
            ParamError::Limit => format!("limit must be an integer from 1 to {MAX_LIMIT}"),
            ParamError::SinceDays => {
                format!("since_days must be an integer from 1 to {MAX_SINCE_DAYS}")
            }
        }
    }
}

/// Validate the raw query values. Both are taken as `i64` so `-3` and `1000`
/// are 400s with the contract's message, not extractor rejections.
pub fn parse_params(limit: Option<i64>, since_days: Option<i64>) -> Result<(u32, u32), ParamError> {
    let limit = match limit {
        None => DEFAULT_LIMIT,
        Some(l) if (1..=MAX_LIMIT as i64).contains(&l) => l as u32,
        Some(_) => return Err(ParamError::Limit),
    };
    let since = match since_days {
        None => DEFAULT_SINCE_DAYS,
        Some(d) if (1..=MAX_SINCE_DAYS as i64).contains(&d) => d as u32,
        Some(_) => return Err(ParamError::SinceDays),
    };
    Ok((limit, since))
}

// ── Response ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HeadlineKind {
    /// The busiest reef already existed before the window.
    ReefUpdated,
    /// The busiest reef was created inside the window.
    ReefAdded,
    /// No nameable reef: just new corals.
    CoralsAdded,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReefRef {
    pub node_id: Uuid,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Headline {
    pub kind: HeadlineKind,
    pub reef: Option<ReefRef>,
    /// `reef_*`: new permitted corals linked to that reef in the window.
    /// `corals_added`: all new permitted corals of the ocean in the window.
    pub coral_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkspaceRow {
    pub graph_id: Uuid,
    pub name: String,
    /// `org` or `team`.
    pub kind: String,
    /// The newest permitted coral's `created_at`.
    pub last_activity_at: DateTime<Utc>,
    pub headline: Headline,
    /// New permitted corals of the ocean in the window.
    pub total_corals_window: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct WorkspaceActivity {
    pub workspaces: Vec<WorkspaceRow>,
}

// ── Raw rows and assembly (pure) ─────────────────────────────────────────

/// A shared ocean in scope.
pub struct OceanInfo {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
}

/// Per-ocean totals over the permitted new corals of the window.
#[derive(Debug, Clone)]
pub struct OceanTotals {
    pub graph_id: Uuid,
    pub corals: i64,
    pub last_at: DateTime<Utc>,
}

/// A reef touched by the window's permitted corals. `title` is `None` when the
/// Library would hide it from this caller.
#[derive(Debug, Clone)]
pub struct ReefCandidate {
    pub graph_id: Uuid,
    pub node_id: Uuid,
    pub title: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub corals: i64,
}

/// The headline for one ocean: the visible-titled reef with the most new
/// corals (ties: most recently updated, then lowest id), `reef_added` when it
/// was created inside the window. No visible reef falls back to
/// `corals_added` with the ocean's total.
pub fn pick_headline(
    reefs: &[&ReefCandidate],
    total_corals: i64,
    since: DateTime<Utc>,
) -> Headline {
    let best = reefs
        .iter()
        .filter(|r| r.corals > 0 && r.title.as_deref().is_some_and(|t| !t.trim().is_empty()))
        .min_by(|a, b| {
            b.corals
                .cmp(&a.corals)
                .then_with(|| b.updated_at.cmp(&a.updated_at))
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
    match best {
        Some(r) => Headline {
            kind: if r.created_at >= since {
                HeadlineKind::ReefAdded
            } else {
                HeadlineKind::ReefUpdated
            },
            reef: Some(ReefRef {
                node_id: r.node_id,
                title: r.title.clone().unwrap_or_default(),
            }),
            coral_count: r.corals,
        },
        None => Headline {
            kind: HeadlineKind::CoralsAdded,
            reef: None,
            coral_count: total_corals,
        },
    }
}

/// One row per ocean with activity, newest first, at most `limit`.
pub fn assemble(
    oceans: &[OceanInfo],
    totals: &[OceanTotals],
    reefs: &[ReefCandidate],
    since: DateTime<Utc>,
    limit: usize,
) -> WorkspaceActivity {
    let mut rows: Vec<WorkspaceRow> = oceans
        .iter()
        .filter_map(|o| {
            let t = totals.iter().find(|t| t.graph_id == o.id && t.corals > 0)?;
            let mine: Vec<&ReefCandidate> = reefs.iter().filter(|r| r.graph_id == o.id).collect();
            Some(WorkspaceRow {
                graph_id: o.id,
                name: o.name.clone(),
                kind: o.kind.clone(),
                last_activity_at: t.last_at,
                headline: pick_headline(&mine, t.corals, since),
                total_corals_window: t.corals,
            })
        })
        .collect();
    rows.sort_by(|a, b| {
        b.last_activity_at
            .cmp(&a.last_activity_at)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.graph_id.cmp(&b.graph_id))
    });
    rows.truncate(limit);
    WorkspaceActivity { workspaces: rows }
}

/// The ocean kinds that are shared spaces. `member` (private) and `personal`
/// are never rows.
pub fn is_shared_kind(kind: &str) -> bool {
    matches!(kind, "org" | "team")
}

// ── Reads ────────────────────────────────────────────────────────────────
//
// Reads return the driver's own `sqlx::Error`, never a string of it: the
// read-model worker classifies a failed compute by its type and SQLSTATE
// (`read_models::failure`), and a statement timeout must stay `57014`.

/// Initial reef batch per ocean. An entirely unnamed batch gets one bulk
/// remainder projection, preserving eligible headlines beyond hidden nodes.
const REEF_CANDIDATES_PER_OCEAN: i64 = 8;

/// Newest candidate corals considered per ocean. The window is capped BEFORE
/// admission, so `library_permitted_atoms` never sees more than this many ids
/// per ocean however busy the ocean is.
pub const WINDOW_ATOM_CAP: i64 = 5000;

/// ONE statement: the window's newest candidate atoms per ocean
/// (`WINDOW_ATOM_CAP`), admitted once by the Library's own rule, then both
/// aggregates over that admitted set. `'total'` rows are per-ocean
/// (count, newest); `'reef'` rows are the top reefs per ocean, ranked by their
/// admitted corals (a reef is only reached through an admitted coral). Only
/// those candidates pass the shared admitted title projector in this same
/// readonly snapshot (unsupported labels retain a NULL title).
const ACTIVITY_SQL: &str = r#"
WITH win AS MATERIALIZED (
  SELECT graph_id, atom_id, created_at FROM (
    SELECT a.graph_id, a.atom_id, a.created_at,
           row_number() OVER (PARTITION BY a.graph_id ORDER BY a.created_at DESC, a.atom_id) AS rn
    FROM atoms a
    WHERE a.graph_id = ANY($1) AND a.created_at >= $2 AND a.created_at <= $3
  ) c WHERE c.rn <= $4
), per_graph AS (
  SELECT graph_id, array_agg(atom_id) AS ids FROM win GROUP BY graph_id
), ok AS MATERIALIZED (
  SELECT w.graph_id, w.atom_id, w.created_at
  FROM per_graph pg
  CROSS JOIN LATERAL public.library_permitted_atoms(pg.graph_id, pg.ids) p
  JOIN win w ON w.graph_id = pg.graph_id AND w.atom_id = p.atom_id
), reef AS MATERIALIZED (
  SELECT ok.graph_id, n.node_id, n.created_at, n.updated_at,
         count(DISTINCT ok.atom_id)::bigint AS corals
  FROM ok
  JOIN node_atoms na ON na.atom_id = ok.atom_id
  JOIN nodes n ON n.node_id = na.node_id AND n.graph_id = ok.graph_id
  WHERE n.title <> ''
  GROUP BY ok.graph_id, n.node_id, n.created_at, n.updated_at
), ranked AS (
  SELECT r.*, row_number() OVER (
           PARTITION BY r.graph_id ORDER BY r.corals DESC, r.updated_at DESC, r.node_id) AS rn
  FROM reef r
)
SELECT 'total'::text AS row_kind, graph_id, NULL::uuid AS node_id,
       NULL::timestamptz AS created_at, NULL::timestamptz AS updated_at,
       count(*)::bigint AS corals, max(created_at) AS last_at, NULL::text AS title
FROM ok GROUP BY graph_id
UNION ALL
SELECT 'reef', graph_id, node_id, created_at, updated_at, corals, NULL::timestamptz,
       NULL::text
FROM ranked WHERE rn <= $5
"#;

type ActivityRow = (
    String,
    Uuid,
    Option<Uuid>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    i64,
    Option<DateTime<Utc>>,
    Option<String>,
);

async fn fetch(
    conn: &mut PgConnection,
    graphs: &[Uuid],
    since: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<(Vec<OceanTotals>, Vec<ReefCandidate>), sqlx::Error> {
    let rows: Vec<ActivityRow> = sqlx::query_as(ACTIVITY_SQL)
        .bind(graphs)
        .bind(since)
        .bind(now)
        .bind(WINDOW_ATOM_CAP)
        .bind(REEF_CANDIDATES_PER_OCEAN)
        .fetch_all(&mut *conn)
        .await?;
    let mut totals = Vec::new();
    let mut reefs = Vec::new();
    for (kind, graph_id, node_id, created_at, updated_at, corals, last_at, title) in rows {
        match (kind.as_str(), node_id, created_at, updated_at, last_at) {
            ("total", _, _, _, Some(last_at)) => totals.push(OceanTotals {
                graph_id,
                corals,
                last_at,
            }),
            ("reef", Some(node_id), Some(created_at), Some(updated_at), _) => {
                reefs.push(ReefCandidate {
                    graph_id,
                    node_id,
                    title,
                    created_at,
                    updated_at,
                    corals,
                })
            }
            _ => {}
        }
    }
    let graphs: Vec<_> = reefs.iter().map(|r| r.graph_id).collect();
    let nodes: Vec<_> = reefs.iter().map(|r| r.node_id).collect();
    let titles = super::journal_activities::project_node_titles_on(conn, &graphs, &nodes).await?;
    for reef in &mut reefs {
        reef.title = titles.get(&(reef.graph_id, reef.node_id)).cloned();
    }
    // Eight is the common-case batch, not a visibility cap. If a full batch
    // has no admitted title, examine the remaining ranked candidates once in
    // this same snapshot. The 5,000-atom input cap and compute limits stay fixed.
    let untitled_oceans: Vec<_> = totals
        .iter()
        .filter(|t| {
            reefs.iter().filter(|r| r.graph_id == t.graph_id).count()
                == REEF_CANDIDATES_PER_OCEAN as usize
                && !reefs.iter().any(|r| {
                    r.graph_id == t.graph_id
                        && r.title
                            .as_deref()
                            .is_some_and(|title| !title.trim().is_empty())
                })
        })
        .map(|t| t.graph_id)
        .collect();
    if !untitled_oceans.is_empty() {
        let seen: std::collections::HashSet<_> = reefs.iter().map(|r| r.node_id).collect();
        let rows: Vec<ActivityRow> = sqlx::query_as(ACTIVITY_SQL)
            .bind(&untitled_oceans)
            .bind(since)
            .bind(now)
            .bind(WINDOW_ATOM_CAP)
            .bind(i64::MAX)
            .fetch_all(&mut *conn)
            .await?;
        let mut remaining: Vec<_> = rows
            .into_iter()
            .filter_map(
                |(kind, graph_id, node_id, created_at, updated_at, corals, _, _)| match (
                    kind.as_str(),
                    node_id,
                    created_at,
                    updated_at,
                ) {
                    ("reef", Some(node_id), Some(created_at), Some(updated_at))
                        if !seen.contains(&node_id) =>
                    {
                        Some(ReefCandidate {
                            graph_id,
                            node_id,
                            title: None,
                            created_at,
                            updated_at,
                            corals,
                        })
                    }
                    _ => None,
                },
            )
            .collect();
        let gs: Vec<_> = remaining.iter().map(|r| r.graph_id).collect();
        let ns: Vec<_> = remaining.iter().map(|r| r.node_id).collect();
        let titles = super::journal_activities::project_node_titles_on(conn, &gs, &ns).await?;
        for reef in &mut remaining {
            reef.title = titles.get(&(reef.graph_id, reef.node_id)).cloned();
        }
        reefs.extend(remaining);
    }
    Ok((totals, reefs))
}

/// Compute-only entry point, using the refresher's verified snapshot.
pub async fn compute(
    conn: &mut PgConnection,
    account_id: Uuid,
    graphs: &[Uuid],
    limit: u32,
    since_days: u32,
    now: DateTime<Utc>,
) -> Result<WorkspaceActivity, sqlx::Error> {
    let since = now - Duration::days(since_days as i64);
    let rows: Vec<(Uuid,String,String)> = sqlx::query_as(
        "SELECT id,name,kind FROM graphs WHERE id=ANY($1) AND account_id=$2 AND archived_at IS NULL AND kind IN ('org','team') ORDER BY id")
        .bind(graphs).bind(account_id).fetch_all(&mut *conn).await?;
    let oceans: Vec<OceanInfo> = rows
        .into_iter()
        .map(|(id, name, kind)| OceanInfo { id, name, kind })
        .collect();
    let graph_ids: Vec<Uuid> = oceans.iter().map(|o| o.id).collect();
    let (totals, reefs) = if graph_ids.is_empty() {
        Default::default()
    } else {
        fetch(conn, &graph_ids, since, now).await?
    };
    Ok(assemble(&oceans, &totals, &reefs, since, limit as usize))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, d, h, 0, 0).unwrap()
    }

    fn reef(
        graph: Uuid,
        title: Option<&str>,
        corals: i64,
        created: DateTime<Utc>,
        updated: DateTime<Utc>,
    ) -> ReefCandidate {
        ReefCandidate {
            graph_id: graph,
            node_id: Uuid::new_v4(),
            title: title.map(String::from),
            created_at: created,
            updated_at: updated,
            corals,
        }
    }

    fn refs(v: &[ReefCandidate]) -> Vec<&ReefCandidate> {
        v.iter().collect()
    }

    #[test]
    fn params_default_and_bounds() {
        assert_eq!(parse_params(None, None), Ok((6, 7)));
        assert_eq!(parse_params(Some(1), Some(1)), Ok((1, 1)));
        assert_eq!(parse_params(Some(20), Some(30)), Ok((20, 30)));
        assert_eq!(parse_params(Some(0), None), Err(ParamError::Limit));
        assert_eq!(parse_params(Some(21), None), Err(ParamError::Limit));
        assert_eq!(parse_params(Some(-1), None), Err(ParamError::Limit));
        assert_eq!(parse_params(None, Some(0)), Err(ParamError::SinceDays));
        assert_eq!(parse_params(None, Some(31)), Err(ParamError::SinceDays));
    }

    #[test]
    fn busiest_reef_wins_and_old_reef_is_updated() {
        let g = Uuid::new_v4();
        let since = at(22, 0);
        let v = vec![
            reef(g, Some("Small"), 2, at(1, 0), at(28, 0)),
            reef(g, Some("Auth rewrite"), 14, at(1, 0), at(27, 0)),
        ];
        let h = pick_headline(&refs(&v), 20, since);
        assert_eq!(h.kind, HeadlineKind::ReefUpdated);
        assert_eq!(h.reef.unwrap().title, "Auth rewrite");
        assert_eq!(h.coral_count, 14);
    }

    #[test]
    fn reef_created_in_window_is_added() {
        let g = Uuid::new_v4();
        let since = at(22, 0);
        let v = vec![reef(g, Some("EU launch plan"), 6, at(25, 0), at(28, 0))];
        let h = pick_headline(&refs(&v), 6, since);
        assert_eq!(h.kind, HeadlineKind::ReefAdded);
        assert_eq!(h.coral_count, 6);
        // Created exactly at the window start counts as in-window.
        let edge = vec![reef(g, Some("Edge"), 1, since, since)];
        assert_eq!(
            pick_headline(&refs(&edge), 1, since).kind,
            HeadlineKind::ReefAdded
        );
    }

    #[test]
    fn ties_go_to_most_recently_updated_then_lowest_id() {
        let g = Uuid::new_v4();
        let since = at(22, 0);
        let older = reef(g, Some("Older"), 5, at(1, 0), at(23, 0));
        let newer = reef(g, Some("Newer"), 5, at(1, 0), at(26, 0));
        let v = vec![older, newer];
        assert_eq!(
            pick_headline(&refs(&v), 10, since).reef.unwrap().title,
            "Newer"
        );

        let mut a = reef(g, Some("A"), 5, at(1, 0), at(26, 0));
        let mut b = reef(g, Some("B"), 5, at(1, 0), at(26, 0));
        a.node_id = Uuid::from_u128(1);
        b.node_id = Uuid::from_u128(2);
        let v = vec![b, a];
        assert_eq!(pick_headline(&refs(&v), 10, since).reef.unwrap().title, "A");
    }

    #[test]
    fn hidden_titles_are_skipped_and_never_named() {
        let g = Uuid::new_v4();
        let since = at(22, 0);
        // The busiest reef is hidden from this caller: the next visible one is named.
        let v = vec![
            reef(g, None, 30, at(1, 0), at(28, 0)),
            reef(g, Some("Visible"), 3, at(1, 0), at(28, 0)),
        ];
        let h = pick_headline(&refs(&v), 40, since);
        assert_eq!(h.reef.unwrap().title, "Visible");
        assert_eq!(h.coral_count, 3);
        // Only hidden reefs: corals_added with the ocean total, no reef.
        let only_hidden = vec![
            reef(g, None, 30, at(1, 0), at(28, 0)),
            reef(g, Some("  "), 4, at(1, 0), at(28, 0)),
        ];
        let h = pick_headline(&refs(&only_hidden), 40, since);
        assert_eq!(h.kind, HeadlineKind::CoralsAdded);
        assert!(h.reef.is_none());
        assert_eq!(h.coral_count, 40);
    }

    #[test]
    fn no_reefs_falls_back_to_corals_added() {
        let h = pick_headline(&[], 9, at(22, 0));
        assert_eq!(h.kind, HeadlineKind::CoralsAdded);
        assert_eq!((h.reef, h.coral_count), (None, 9));
    }

    #[test]
    fn shared_kinds_exclude_private_and_personal() {
        assert!(is_shared_kind("org"));
        assert!(is_shared_kind("team"));
        assert!(!is_shared_kind("member"));
        assert!(!is_shared_kind("personal"));
    }

    fn ocean(name: &str, kind: &str) -> OceanInfo {
        OceanInfo {
            id: Uuid::new_v4(),
            name: name.into(),
            kind: kind.into(),
        }
    }

    #[test]
    fn assemble_sorts_newest_first_skips_quiet_oceans_and_limits() {
        let since = at(22, 0);
        let (a, b, quiet, c) = (
            ocean("Alpha", "org"),
            ocean("Beta", "team"),
            ocean("Quiet", "team"),
            ocean("Gamma", "team"),
        );
        let totals = vec![
            OceanTotals {
                graph_id: a.id,
                corals: 14,
                last_at: at(27, 9),
            },
            OceanTotals {
                graph_id: b.id,
                corals: 6,
                last_at: at(28, 9),
            },
            OceanTotals {
                graph_id: c.id,
                corals: 1,
                last_at: at(23, 9),
            },
        ];
        let reefs = vec![
            reef(a.id, Some("Auth rewrite"), 14, at(1, 0), at(27, 9)),
            reef(b.id, Some("EU launch plan"), 6, at(25, 0), at(28, 9)),
        ];
        let out = assemble(&[a, b, quiet, c], &totals, &reefs, since, 2);
        assert_eq!(out.workspaces.len(), 2);
        assert_eq!(out.workspaces[0].name, "Beta");
        assert_eq!(out.workspaces[0].headline.kind, HeadlineKind::ReefAdded);
        assert_eq!(out.workspaces[1].name, "Alpha");
        assert_eq!(out.workspaces[1].headline.kind, HeadlineKind::ReefUpdated);
        assert_eq!(out.workspaces[1].total_corals_window, 14);
    }

    #[test]
    fn rows_for_oceans_out_of_scope_are_dropped_and_json_shape_is_stable() {
        let since = at(22, 0);
        let a = ocean("Alpha", "org");
        let totals = vec![
            OceanTotals {
                graph_id: a.id,
                corals: 3,
                last_at: at(27, 9),
            },
            OceanTotals {
                graph_id: Uuid::new_v4(),
                corals: 99,
                last_at: at(28, 9),
            },
        ];
        let out = assemble(&[a], &totals, &[], since, 6);
        assert_eq!(out.workspaces.len(), 1);
        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(json["workspaces"][0]["headline"]["kind"], "corals_added");
        assert!(json["workspaces"][0]["headline"]["reef"].is_null());
        assert_eq!(json["workspaces"][0]["total_corals_window"], 3);
        assert_eq!(assemble(&[], &[], &[], since, 6).workspaces.len(), 0);
    }
}
