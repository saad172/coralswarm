//! Shared admission sets for workspace counts and paginated legacy reefs.
// Semantic admission deliberately accepts any permitted source version, matching
// library_node_permitted. Library scope/version filters do not belong here.
//
// COR-113: every query below computes `library_permitted_atoms` once per
// statement and admits a reef exactly as `library_node_permitted` does: a
// legacy link to a permitted atom, or a semantic link to a permitted source.
// What changed is how much of the graph each statement has to touch.
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use uuid::Uuid;

/// The full admission set. Still used for deep pages, where
/// [`CANDIDATE_PAGE_SQL`] would visit most of the graph anyway.
///
/// `legacy_counts` filters `node_atoms` through two hashed sets (this graph's
/// reefs and its permitted atoms). Joining `permitted_atoms` directly planned
/// as one index probe and heap fetch per permitted atom, plus a memoized
/// probe of `nodes` per link, which was about twice as slow on production.
pub(super) const ADMISSION_CTE: &str = r#"
WITH permitted_atoms AS MATERIALIZED (
 SELECT atom_id FROM library_permitted_atoms($1,NULL::uuid[])
), graph_nodes AS MATERIALIZED (
 SELECT node_id FROM nodes WHERE graph_id=$1
), legacy_counts AS MATERIALIZED (
 SELECT l.node_id,count(*) atom_count FROM node_atoms l
 WHERE l.node_id IN (SELECT node_id FROM graph_nodes)
   AND l.atom_id IN (SELECT atom_id FROM permitted_atoms)
 GROUP BY l.node_id
), admitted_node_ids AS MATERIALIZED (
 SELECT node_id FROM legacy_counts
 UNION
 SELECT l.node_id FROM node_semantic_atoms l
 JOIN served_semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
 WHERE l.graph_id=$1
)
"#;

/// Legacy explicit store API (no HTTP stats request calls this path).
/// HTTP page answers and worker computation live in `read_models::oceans`.
/// Workspace counts are a stored row per graph (`graph_workspace_counts`,
/// control 0353 / tenant 0184), computed by `graph_workspace_counts_compute`.
/// Computing them reads every permitted atom and every reef of the graph
/// (0.9-4.2 s on the production org tenant), so a request reads the stored
/// row and refreshes it in the background once it is older than
/// [`COUNTS_REFRESH_AFTER_SECS`].
///
/// A stored row is used only when `graph_workspace_counts_shareable` says
/// the caller sees every row the counts read, and only while no
/// permission-reducing write has happened since the snapshot it was computed
/// from (`invalidated_xid` must be visible in `computed_snapshot`). Otherwise
/// the request counts live in its own scope.
///
/// `$1` graph. One row: shareable, then the stored counts or NULLs, then
/// whether the stored row is due a refresh.
pub(super) const COUNTS_READ_SQL: &str = r#"
WITH s AS (SELECT graph_workspace_counts_shareable($1) ok)
SELECT s.ok,c.reef_counts,c.atom_count,c.source_count,
       COALESCE(c.computed_at < now()-make_interval(secs=>$2),false)
FROM s LEFT JOIN graph_workspace_counts c ON s.ok AND c.graph_id=$1
  AND c.computed_snapshot IS NOT NULL
  AND (c.invalidated_xid IS NULL OR pg_visible_in_snapshot(c.invalidated_xid,c.computed_snapshot))
"#;

/// Live counts in the caller's own scope. Used when the stored row may not
/// be shown to this caller (a team member with a history window, or a
/// caller whose scope cannot see the graph). Nothing is written.
pub(super) const COUNTS_LIVE_SQL: &str =
    "SELECT reef_counts,atom_count,source_count FROM graph_workspace_counts_compute($1)";

/// Compute and store, returning what was computed. The stored row records
/// the statement's snapshot, so a permission-reducing write that this
/// snapshot cannot see keeps the row unserved. A slower refresh never
/// overwrites a row computed from a later snapshot. The `WHERE` re-checks
/// that the caller may share the row; when it may not, nothing is stored
/// and the counts are still the caller's own.
pub(super) const COUNTS_STORE_SQL: &str = r#"
WITH c AS MATERIALIZED (SELECT * FROM graph_workspace_counts_compute($1)),
w AS (
 INSERT INTO graph_workspace_counts(graph_id,reef_counts,atom_count,source_count,computed_at,computed_snapshot)
 SELECT $1,c.reef_counts,c.atom_count,c.source_count,now(),pg_current_snapshot()
 FROM c WHERE graph_workspace_counts_shareable($1)
 ON CONFLICT (graph_id) DO UPDATE SET reef_counts=EXCLUDED.reef_counts,
  atom_count=EXCLUDED.atom_count,source_count=EXCLUDED.source_count,
  computed_at=EXCLUDED.computed_at,computed_snapshot=EXCLUDED.computed_snapshot,
  refresh_claimed_at=NULL
 WHERE graph_workspace_counts.computed_at IS NULL
    OR graph_workspace_counts.computed_at<=EXCLUDED.computed_at
)
SELECT reef_counts,atom_count,source_count FROM c
"#;

/// Claim a background refresh, so concurrent readers on any machine start
/// one refresh per graph, not one each. A claim older than two minutes is
/// treated as abandoned. `$2` is the staleness threshold in seconds.
pub(super) const COUNTS_CLAIM_SQL: &str = r#"
UPDATE graph_workspace_counts SET refresh_claimed_at=now()
WHERE graph_id=$1 AND graph_workspace_counts_shareable($1)
  AND (refresh_claimed_at IS NULL OR refresh_claimed_at<now()-interval '2 minutes')
  AND (computed_at IS NULL OR computed_at<now()-make_interval(secs=>$2))
RETURNING graph_id
"#;

/// A stored row older than this is still served, and refreshed in the
/// background.
pub(super) const COUNTS_REFRESH_AFTER_SECS: f64 = 30.0;

/// One window of reef candidates for a page.
///
/// Candidates come in "bound order": type rank, then the reef's bound
/// descending, then `updated_at` descending, then `node_id`. The bound is
/// `nodes.coral_count` (the trigger-maintained total of the reef's
/// `node_atoms` rows, control 0109 / tenant 0020) minus the reef's links to
/// this graph's withheld atoms. It is never below the exact permitted count,
/// and it equals it unless a reef links atoms of another graph. Finding the
/// withheld links costs one index probe per withheld atom, so a graph whose
/// sources are all permitted pays nothing for it.
///
/// For each candidate the statement returns the exact permitted link count
/// and whether `library_node_permitted` would admit it, reading only the
/// window's own links.
///
/// The bound is used ONLY to choose which reefs to look at first. It is
/// never shown, never used to admit a reef and never used as the count. See
/// [`settle_page`] for why visiting reefs in this order gives the canonical
/// page.
///
/// Two plan shapes are deliberate. The `OFFSET 0` lateral keeps the
/// withheld-link lookup an index probe per withheld atom, so an
/// all-permitted graph never scans `node_atoms`. The candidate link count is
/// a plain join with a hashed permitted set: as a per-candidate lateral,
/// production planned the permitted set as a hash probe rebuilt per reef.
///
/// `$1` graph, `$2` optional type filter, `$3` window size.
pub(super) const CANDIDATE_PAGE_SQL: &str = r#"
WITH permitted_atoms AS MATERIALIZED (
 SELECT atom_id FROM library_permitted_atoms($1,NULL::uuid[])
), withheld_atoms AS MATERIALIZED (
 SELECT a.atom_id FROM atoms a WHERE a.graph_id=$1
   AND a.atom_id NOT IN (SELECT atom_id FROM permitted_atoms)
), withheld_links AS MATERIALIZED (
 SELECT x.node_id,count(*) withheld FROM withheld_atoms w
 CROSS JOIN LATERAL (SELECT l.node_id FROM node_atoms l WHERE l.atom_id=w.atom_id OFFSET 0) x
 GROUP BY x.node_id
), candidates AS MATERIALIZED (
 SELECT n.node_id,n.type,n.scope,n.created_at,n.updated_at,
        (n.coral_count-COALESCE(w.withheld,0))::bigint bound
 FROM nodes n LEFT JOIN withheld_links w ON w.node_id=n.node_id
 WHERE n.graph_id=$1 AND ($2::text IS NULL OR n.type=$2)
 ORDER BY CASE n.type WHEN 'entity' THEN 0 WHEN 'thread' THEN 1
          WHEN 'decision' THEN 2 WHEN 'reference' THEN 3 ELSE 4 END,
          (n.coral_count-COALESCE(w.withheld,0)) DESC,n.updated_at DESC,n.node_id ASC
 LIMIT $3
), legacy AS MATERIALIZED (
 SELECT l.node_id,count(*) atom_count FROM candidates c
 JOIN node_atoms l ON l.node_id=c.node_id
 WHERE l.atom_id IN (SELECT atom_id FROM permitted_atoms)
 GROUP BY l.node_id
), semantic AS MATERIALIZED (
 SELECT DISTINCT l.node_id FROM node_semantic_atoms l
 JOIN served_semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
 WHERE l.graph_id=$1 AND l.node_id IN (SELECT node_id FROM candidates)
)
SELECT c.node_id,c.type,c.scope,c.created_at,c.updated_at,c.bound,
       COALESCE(lg.atom_count,0),(lg.node_id IS NOT NULL OR s.node_id IS NOT NULL)
FROM candidates c LEFT JOIN legacy lg ON lg.node_id=c.node_id
LEFT JOIN semantic s ON s.node_id=c.node_id
"#;

/// Turn JIT off for the rest of the read transaction. Both workspace reads
/// carry planner cost estimates past `jit_above_cost`, and on production the
/// compile step alone took 1.1 to 1.4 seconds per statement, longer than the
/// queries. `SET LOCAL` ends with the transaction, so it is safe behind the
/// Neon pooler.
pub(super) const JIT_OFF_SQL: &str = "SET LOCAL jit = off";

/// Titles for the rows of one settled page, through the canonical
/// `library_node_title` (unsupported titles stay empty).
pub(super) const PAGE_TITLES_SQL: &str = "SELECT u.node_id,COALESCE(library_node_title($1,u.node_id),'') FROM unnest($2::uuid[]) u(node_id)";

/// Above this many candidates the window would cover most of a large graph,
/// and the single full-admission statement is cheaper. Callers then fall back
/// to [`ADMISSION_CTE`].
pub(super) const CANDIDATE_WINDOW_CEILING: i64 = 5_000;

/// The first window to try for a page that needs `need` rows (offset plus
/// requested rows). Twice what is needed, so a few withheld reefs at the top
/// do not force a second statement.
pub(super) fn first_window(need: i64) -> i64 {
    need.saturating_mul(2).max(need.saturating_add(64))
}

/// One row of [`CANDIDATE_PAGE_SQL`].
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Candidate {
    pub node_id: Uuid,
    pub node_type: String,
    pub scope: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// `coral_count` minus links to this graph's withheld atoms.
    pub bound: i64,
    /// Exact permitted legacy link count.
    pub atom_count: i64,
    /// Admitted exactly as `library_node_permitted` admits.
    pub admitted: bool,
}

pub(super) type CandidateRow = (Uuid, String, String, DateTime<Utc>, DateTime<Utc>, i64, i64, bool);

impl From<CandidateRow> for Candidate {
    fn from(r: CandidateRow) -> Self {
        Self {
            node_id: r.0,
            node_type: r.1,
            scope: r.2,
            created_at: r.3,
            updated_at: r.4,
            bound: r.5,
            atom_count: r.6,
            admitted: r.7,
        }
    }
}

/// Same rank as the SQL `CASE n.type ...`.
fn type_rank(t: &str) -> u8 {
    match t {
        "entity" => 0,
        "thread" => 1,
        "decision" => 2,
        "reference" => 3,
        _ => 4,
    }
}

/// The canonical page order, `(type rank, count DESC, updated_at DESC,
/// node_id ASC)`, for a given count. Rust's `Uuid` ordering is bytewise, the
/// same as PostgreSQL's `uuid` ordering.
fn order(a: (&Candidate, i64), b: (&Candidate, i64)) -> Ordering {
    type_rank(&a.0.node_type)
        .cmp(&type_rank(&b.0.node_type))
        .then(b.1.cmp(&a.1))
        .then(b.0.updated_at.cmp(&a.0.updated_at))
        .then(a.0.node_id.cmp(&b.0.node_id))
}

/// Decide whether one candidate window already fixes the first `need`
/// admitted reefs in canonical order.
///
/// Returns the admitted reefs in canonical order, cut to `need`, when the
/// window settles them. Returns `None` when a bigger window is needed.
///
/// Why this is the canonical order: a reef's exact permitted count is at most
/// its `bound`. `window` holds the first `window` reefs in
/// bound order, so every reef outside it sorts after the window's last reef
/// even by bound, and its exact key sorts no earlier than its bound key. An
/// admitted window reef whose exact key sorts at or before the last
/// candidate's bound key therefore precedes every reef outside the window.
/// Once `need` of them do, the page is fixed. A window shorter than
/// requested is the whole graph, so it always settles.
///
/// This relies on `bound` never being below the exact count. Admission,
/// counts and titles never depend on `bound`, so a drifted `coral_count`
/// cannot expose anything; it could only misplace reefs across pages. The
/// caller checks [`bound_drifted`] on every window and falls back to the
/// full-admission statement when a visited reef breaks the bound.
pub(super) fn settle_page(window: &[Candidate], requested: i64, need: i64) -> Option<Vec<Candidate>> {
    let need = usize::try_from(need.max(0)).unwrap_or(usize::MAX);
    let exhausted = (window.len() as i64) < requested;
    let mut admitted: Vec<&Candidate> = window.iter().filter(|c| c.admitted).collect();
    admitted.sort_by(|a, b| order((a, a.atom_count), (b, b.atom_count)));
    // The statement's row order is not the window order, so find the last
    // candidate in bound order rather than taking the final row.
    let last = window.iter().max_by(|a, b| order((a, a.bound), (b, b.bound)));
    let settled = match (exhausted, last) {
        (true, _) | (_, None) => admitted.len(),
        (false, Some(last)) => admitted
            .iter()
            .take_while(|c| order((c, c.atom_count), (last, last.bound)) != Ordering::Greater)
            .count(),
    };
    if settled < need && !exhausted {
        return None;
    }
    Some(admitted.into_iter().take(need.min(settled)).cloned().collect())
}

/// True when a visited reef has more permitted links than its bound allows,
/// which only a drifted `coral_count` can cause, so the window order cannot
/// be trusted.
pub(super) fn bound_drifted(window: &[Candidate]) -> bool {
    window.iter().any(|c| c.atom_count > c.bound)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(id: u128, kind: &str, bound: i64, exact: i64, admitted: bool) -> Candidate {
        let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        Candidate {
            node_id: Uuid::from_u128(id),
            node_type: kind.into(),
            scope: "personal".into(),
            created_at: at,
            updated_at: at,
            bound,
            atom_count: exact,
            admitted,
        }
    }
    fn ids(v: &[Candidate]) -> Vec<u128> {
        v.iter().map(|c| c.node_id.as_u128()).collect()
    }

    #[test]
    fn untainted_window_settles_on_the_first_try() {
        let w = vec![c(1, "entity", 9, 9, true), c(2, "entity", 5, 5, true), c(3, "entity", 4, 4, true)];
        assert_eq!(ids(&settle_page(&w, 3, 2).unwrap()), vec![1, 2]);
    }

    #[test]
    fn withheld_hub_needs_a_bigger_window() {
        // Reef 1 has nine links but only one is permitted. Reef 3, outside a
        // two-row window, could still have up to four permitted links.
        let w = vec![c(1, "entity", 9, 1, true), c(2, "entity", 5, 5, true)];
        assert_eq!(settle_page(&w, 2, 2), None);
        let w = vec![
            c(1, "entity", 9, 1, true),
            c(2, "entity", 5, 5, true),
            c(3, "entity", 4, 4, true),
            c(4, "entity", 1, 1, true),
        ];
        assert_eq!(ids(&settle_page(&w, 4, 3).unwrap()), vec![2, 3, 1]);
    }

    #[test]
    fn unadmitted_reefs_never_appear() {
        let w = vec![c(1, "entity", 9, 0, false), c(2, "entity", 1, 1, true)];
        assert_eq!(ids(&settle_page(&w, 5, 5).unwrap()), vec![2]);
    }

    #[test]
    fn short_window_is_the_whole_graph() {
        let w = vec![c(1, "entity", 9, 0, true), c(2, "thread", 0, 0, true)];
        assert_eq!(ids(&settle_page(&w, 10, 10).unwrap()), vec![1, 2]);
        assert_eq!(settle_page(&[], 10, 10), Some(vec![]));
    }

    #[test]
    fn type_rank_outranks_counts() {
        let w = vec![c(1, "entity", 1, 1, true), c(2, "thread", 50, 50, true), c(3, "decision", 0, 0, true)];
        assert_eq!(ids(&settle_page(&w, 3, 3).unwrap()), vec![1, 2, 3]);
    }

    #[test]
    fn drift_is_detected_only_when_the_count_exceeds_the_bound() {
        assert!(!bound_drifted(&[c(1, "entity", 9, 1, true), c(2, "entity", 3, 3, true)]));
        assert!(bound_drifted(&[c(1, "entity", 0, 1, true)]));
    }

    #[test]
    fn statement_row_order_does_not_matter() {
        // The statement joins after the LIMIT, so rows can come back in any
        // order. The boundary is the last candidate in bound order.
        let mut w: Vec<Candidate> = (1..=6).map(|i| c(i, "entity", 5, 5, true)).collect();
        w.swap(0, 5);
        w.swap(2, 4);
        assert_eq!(ids(&settle_page(&w, 6, 4).unwrap()), vec![1, 2, 3, 4]);
    }

    #[test]
    fn ties_break_by_node_id() {
        let w = vec![c(2, "entity", 3, 3, true), c(1, "entity", 3, 3, true), c(9, "entity", 3, 3, true)];
        // The window's last bound key is (entity, 3, id 9): ids 1 and 2 sort
        // before it, so a two-row page is settled.
        assert_eq!(ids(&settle_page(&w, 3, 2).unwrap()), vec![1, 2]);
    }
}
