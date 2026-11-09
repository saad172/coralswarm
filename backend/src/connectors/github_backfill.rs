//! GitHub connector backfill engine (docs/connectors/github.md §3.6) — the
//! real, resumable, per-repo, per-object-type-cursored narrative backfill.
//! `workers::github_backfill` is the thin tenant-loop wrapper that dequeues
//! `JobKind::GithubBackfill { repo_row_id }` and calls [`run_tick`].
//!
//! # Why this is its OWN engine, not an extension of `connectors::history_backfill`
//!
//! `connectors::history_backfill`'s `BackfillTarget`/`run_slack` shape is
//! built around ONE container, ONE page cursor (Slack's opaque
//! `next_cursor`), and a single provider match inside `JobKind::
//! HistoryBackfill`. GitHub needs FOUR independent per-object-type cursors
//! per repo (`narrative_cursor.{pr,issue,release,commit}`), each resumable
//! independently, or­dered newest→oldest, with a `backfill_progress` shape
//! the selection UI reads per repo (§3.6) — reshaping the Slack engine to
//! carry that would mean widening `JobKind::HistoryBackfill`'s payload with
//! GitHub-only fields no other provider needs, and splitting its single
//! `run_after`-style page loop into a per-type state machine anyway. GitHub
//! already has its own `JobKind::GithubBackfill { repo_row_id }` (state
//! lives on `github_watched_repos`, not the job payload — a kill mid-run
//! must resume from the DB row, not from whatever payload a doomed job
//! happened to carry), so a parallel module is the natural boundary.
//!
//! # How content is written (§3.9/D10 — Path 1)
//!
//! [`write_atom_to_destinations`] is a hand-rolled per-destination write, the
//! same shape `workers::huddle::ingest_huddle_notes`,
//! `routes::slack_events::ingest_slack_message` and
//! `connectors::google_chat::ingest_space_items` use. It is deliberately NOT
//! `routes::ingest::ingest_atom` ("Path 2"), which this module DID use until
//! 2026-09-23 and which made every team/org-destination backfill ingest
//! exactly zero atoms — see that function's own doc comment for the D7
//! invariant that refuses a shared ocean, and for why the argument that used
//! to sit here reasoned about the wrong gate.
//!
//! # Cursor shape (`github_watched_repos.narrative_cursor`)
//!
//! ```json
//! {
//!   "pr":      {"page_url": "https://api.github.com/repos/o/r/pulls?...&page=3", "done": false, "seen": 50, "total_hint": 340, "since": "2026-09-01T00:00:00Z"},
//!   "issue":   {"page_url": null, "done": true, "seen": 12, "total_hint": 12, "since": "2026-08-15T00:00:00Z"},
//!   "release": {...},
//!   "commit":  {...}
//! }
//! ```
//!
//! `page_url: null` + `done: false` means "not started" (the initial URL is
//! recomputed from `owner`/`name`/`default_branch` — never itself persisted,
//! so a changed `per_page` constant across a deploy just changes the NEXT
//! page's size rather than invalidating a stored URL). `page_url` is set to
//! whatever `Link: rel="next"` GitHub returned, persisted after **every**
//! page — the resumability contract (§8 "resume from cursors after a kill
//! mid-backfill"): a killed worker leaves the row exactly where the last
//! completed page left it, and `atoms`' `(graph_id, source_type, source_id)`
//! `ON CONFLICT DO NOTHING` makes a re-fetch of an already-ingested item free
//! even if a page boundary is ever re-crossed. `since` is the newest
//! `occurred_at` seen for that type across the whole backfill — the
//! reconcile sweep's own resume point once `done` flips true.
//!
//! `total_hint` is read ONCE, from the first page's `Link: rel="last"` page
//! number × [`LIST_PAGE_SIZE`] (or `seen` itself when the whole type fits on
//! one page, i.e. GitHub omitted `rel="last"`). It never re-derives after
//! that — a repo growing new PRs mid-backfill would otherwise make the
//! denominator chase the numerator and the percentage could regress.
//!
//! # `backfill_progress` shape
//!
//! ```json
//! {"prs": 40, "issues": 0, "releases": 100, "commits": null, "updated_at": "2026-09-21T10:00:00Z"}
//! ```
//!
//! `null` means "not started yet" or "no `total_hint` known" (percent is
//! genuinely unknowable, never guessed at). `routes::github::list_repos`
//! reads this column straight into the `progress` field of its response
//! (previously always `null` — PR A's own §9 divergence entry).
//!
//! # Time-slicing
//!
//! One tick ([`run_tick`]) processes the FIRST not-yet-`done` type, up to
//! [`MAX_PAGES_PER_TICK`] pages, then returns [`RunOutcome::Continue`].
//! `workers::github_backfill` completes the current job (`DidWork` — real
//! progress was made and durably persisted) and re-enqueues a continuation
//! with the SAME `repo_row_id` and idempotency key — there is no queue
//! primitive to "leave this exact job pending with new state" (`jobs::queue`
//! has no requeue-with-payload-update API; `JobOutcome` has exactly
//! `DidWork`/`NothingToDo`, deliberately, see that type's own doc comment) —
//! this mirrors the `SlackNameBackfill`/`BackfillAtomTitles` continuation
//! convention already established in this tree: cursor state lives in the
//! DB row, not the job payload, so the SAME idempotency key
//! (`github_backfill:{repo_row_id}`) re-firing is exactly correct — a repeat
//! is a no-op resume, never a duplicate crawl.
//!
//! # Rate limits
//!
//! [`RunOutcome::RateLimited`] carries GitHub's own `retry_after` (from
//! `X-RateLimit-Reset`/`Retry-After` — `connectors::github::GithubApiError::
//! RateLimited`). The cursor is persisted before this is ever returned (page
//! boundaries only), so `workers::github_backfill` re-enqueues the SAME
//! continuation with `delay_secs = retry_after.as_secs()` — `jobs::queue`
//! has no per-job custom backoff override on an already-`running` job, but
//! `enqueue`'s own `delay_secs` parameter (`run_after = now() + delay_secs`)
//! is exactly this primitive for a FRESH job, which the continuation always
//! is. Minting the installation token itself getting rate-limited (a
//! separate, smaller bucket) is not given this treatment — `installation_
//! token` only reports that as a generic `ConnectorError::Exchange` string,
//! not a typed `retry_after` — so it falls back to the job's ordinary
//! `queue::fail` exponential-backoff ladder instead (documented divergence,
//! docs/connectors/github.md §9).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::github::{self, GithubApiError, github_api_base};
use crate::connectors::github_disconnect::{self, paused_reason};
use crate::connectors::github_narrative::{self, NormalizedAtom};
use crate::store::PostgresStore;
use crate::tenancy::TenantId;
use crate::AppState;

/// GitHub `per_page` for every list endpoint this engine drives. Deliberately
/// smaller than GitHub's own 100-item ceiling: each PR/issue item costs
/// several FOLLOW-UP calls (reviews, comments, commits), so a "page" here
/// must stay cheap enough that [`MAX_PAGES_PER_TICK`] pages is a bounded unit
/// of work, not a a multi-thousand-request tick.
pub(crate) const LIST_PAGE_SIZE: usize = 25;

/// How many list pages of the CURRENT type [`run_tick`] will fetch before
/// returning [`RunOutcome::Continue`] — the time-slice budget (§3.6 "process
/// at most N pages per tick"). At [`LIST_PAGE_SIZE`] = 25 this is up to 100
/// PRs/issues/releases/commits examined (each PR/issue costing up to 5
/// follow-up requests) per tick — bounded well short of GitHub's per-hour
/// installation rate budget for a single tenant slot.
pub(crate) const MAX_PAGES_PER_TICK: usize = 4;

// ── Cursor / progress shapes ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypeKind {
    Pr,
    Issue,
    Release,
    Commit,
}

impl TypeKind {
    pub(crate) fn key(self) -> &'static str {
        match self {
            TypeKind::Pr => "pr",
            TypeKind::Issue => "issue",
            TypeKind::Release => "release",
            TypeKind::Commit => "commit",
        }
    }

    /// The FIRST page URL for this type — recomputed on every "not started"
    /// resume rather than ever persisted itself (see this module's cursor
    /// doc comment).
    pub(crate) fn initial_url(self, owner: &str, name: &str, default_branch: &str) -> String {
        let base = format!("{}/repos/{owner}/{name}", github_api_base());
        match self {
            TypeKind::Pr => format!("{base}/pulls?state=all&sort=updated&direction=desc&per_page={LIST_PAGE_SIZE}"),
            TypeKind::Issue => format!("{base}/issues?state=all&sort=updated&direction=desc&per_page={LIST_PAGE_SIZE}"),
            TypeKind::Release => format!("{base}/releases?per_page={LIST_PAGE_SIZE}"),
            TypeKind::Commit => {
                let branch = urlencoding::encode(default_branch);
                format!("{base}/commits?sha={branch}&per_page={LIST_PAGE_SIZE}")
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct TypeCursor {
    #[serde(default)]
    pub(crate) page_url: Option<String>,
    #[serde(default)]
    pub(crate) done: bool,
    #[serde(default)]
    pub(crate) seen: u64,
    #[serde(default)]
    pub(crate) total_hint: Option<u64>,
    /// Newest `occurred_at` seen for this type so far — the reconcile
    /// sweep's resume point once `done` is true.
    #[serde(default)]
    pub(crate) since: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct NarrativeCursor {
    #[serde(default)]
    pub(crate) pr: TypeCursor,
    #[serde(default)]
    pub(crate) issue: TypeCursor,
    #[serde(default)]
    pub(crate) release: TypeCursor,
    #[serde(default)]
    pub(crate) commit: TypeCursor,
    /// A member-chosen history window (connectors redesign, "Import history"):
    /// nothing older than this is fetched. `None` is the automatic backfill,
    /// which stays unbounded. It lives in the cursor, not the job, so a resumed
    /// or rate-limited tick keeps the same bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) window_floor: Option<DateTime<Utc>>,
}

/// The URL the next page fetch for `kind` will use: the persisted cursor
/// page if one exists, otherwise the type's initial listing URL.
pub(crate) fn page_url_for(kind: TypeKind, cursor: &NarrativeCursor, owner: &str, name: &str, default_branch: &str) -> String {
    cursor
        .field(kind)
        .page_url
        .clone()
        .unwrap_or_else(|| kind.initial_url(owner, name, default_branch))
}

/// When a list item happened, for the type's own sort key: the lists are
/// newest-first by it (`updated` for PRs and issues, creation for releases and
/// commits), so the first item past a floor ends the walk.
pub(crate) fn item_time(kind: TypeKind, item: &Value) -> Option<DateTime<Utc>> {
    let text = match kind {
        TypeKind::Pr | TypeKind::Issue => item.get("updated_at"),
        TypeKind::Release => item.get("published_at").filter(|v| !v.is_null()).or_else(|| item.get("created_at")),
        TypeKind::Commit => item.get("commit").and_then(|c| c.get("committer")).and_then(|c| c.get("date")),
    }?;
    text.as_str()?.parse().ok()
}

/// Whether `item` is older than the member's window. An item with no readable
/// date is kept: a bound never drops what it cannot place.
pub(crate) fn before_floor(floor: Option<DateTime<Utc>>, kind: TypeKind, item: &Value) -> bool {
    match (floor, item_time(kind, item)) {
        (Some(floor), Some(at)) => at < floor,
        _ => false,
    }
}

impl NarrativeCursor {
    /// A fresh cursor that stops at `floor`.
    pub(crate) fn windowed(floor: DateTime<Utc>) -> Self {
        Self { window_floor: Some(floor), ..Self::default() }
    }

    fn field(&self, kind: TypeKind) -> &TypeCursor {
        match kind {
            TypeKind::Pr => &self.pr,
            TypeKind::Issue => &self.issue,
            TypeKind::Release => &self.release,
            TypeKind::Commit => &self.commit,
        }
    }
    fn field_mut(&mut self, kind: TypeKind) -> &mut TypeCursor {
        match kind {
            TypeKind::Pr => &mut self.pr,
            TypeKind::Issue => &mut self.issue,
            TypeKind::Release => &mut self.release,
            TypeKind::Commit => &mut self.commit,
        }
    }

    pub(crate) fn all_done(&self) -> bool {
        self.pr.done && self.issue.done && self.release.done && self.commit.done
    }

    fn from_json(v: Option<Value>) -> Self {
        v.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BackfillProgress {
    pub(crate) prs: Option<u8>,
    pub(crate) issues: Option<u8>,
    pub(crate) releases: Option<u8>,
    /// Always `null` in PR A/this phase — `code`-tier progress is PR B. Kept
    /// as a field (rather than omitted) so the JSON shape is stable across
    /// PR B landing without a reader needing to handle a newly-appearing key.
    pub(crate) commits: Option<u8>,
    pub(crate) updated_at: DateTime<Utc>,
}

fn percent(tc: &TypeCursor) -> Option<u8> {
    // A swept type is 100%, whatever the estimate said. `total_hint` is an
    // ESTIMATE (`Link: rel="last"` page count × page size, and for issues
    // that count includes the PRs the endpoint also returns), so a finished
    // type routinely has `seen` well under `total_hint` — the observed case
    // was a fully-swept repo whose selection page read "commits 25%" for
    // ever, and issues 0% on a repo that genuinely has none.
    if tc.done {
        return Some(100);
    }
    let total = tc.total_hint?;
    if total == 0 {
        return Some(100);
    }
    Some(((tc.seen.min(total) * 100) / total) as u8)
}

pub(crate) fn compute_progress(cursor: &NarrativeCursor) -> BackfillProgress {
    BackfillProgress {
        prs: percent(&cursor.pr),
        issues: percent(&cursor.issue),
        releases: percent(&cursor.release),
        commits: percent(&cursor.commit),
        updated_at: Utc::now(),
    }
}

// ── Row load / persist ──────────────────────────────────────────────────────

pub(crate) struct RepoRow {
    pub(crate) installation_id: i64,
    pub(crate) repo_id: i64,
    pub(crate) owner: String,
    pub(crate) name: String,
    pub(crate) default_branch: String,
    pub(crate) active: bool,
    pub(crate) graph_id: Option<Uuid>,
    pub(crate) owner_user_id: Option<Uuid>,
    pub(crate) narrative_cursor: NarrativeCursor,
}

impl RepoRow {
    /// The row's binding, for [`resolve_repo_grant`].
    pub(crate) fn binding(&self, row_id: Uuid) -> RepoBinding {
        RepoBinding { row_id, repo_id: self.repo_id, active: self.active, graph_id: self.graph_id, owner_user_id: self.owner_user_id }
    }
}

/// A `github_watched_repos` row's destination binding, as a caller loaded it
/// (`run_tick`'s [`RepoRow`], the webhook's watch rows). The ingest grant is
/// built from this rather than re-reading the row; the door re-reads it
/// under a share lock inside each write's transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RepoBinding {
    pub(crate) row_id: Uuid,
    pub(crate) repo_id: i64,
    pub(crate) active: bool,
    pub(crate) graph_id: Option<Uuid>,
    pub(crate) owner_user_id: Option<Uuid>,
}

impl RepoBinding {
    /// The one definition of an active binding: the row is active and names
    /// both an ocean and the member who chose it. `None` is
    /// `RepoRefusal::Unbound`, and nothing is written under the row.
    pub(crate) fn bound(&self) -> Option<(Uuid, Uuid)> {
        match (self.active, self.graph_id, self.owner_user_id) {
            (true, Some(graph_id), Some(owner_user_id)) => Some((graph_id, owner_user_id)),
            _ => None,
        }
    }
}

/// Load one watch row's [`RepoBinding`]. `None` if the row is not this
/// account's. The serving paths build the binding from a row they already
/// hold; this is for tests that start from a row id.
#[cfg(test)]
pub(crate) async fn load_binding(pool: &PgPool, account_id: Uuid, repo_row_id: Uuid) -> Result<Option<RepoBinding>, sqlx::Error> {
    Ok(load_row(pool, account_id, repo_row_id).await?.map(|row| row.binding(repo_row_id)))
}

async fn load_row(pool: &PgPool, account_id: Uuid, repo_row_id: Uuid) -> Result<Option<RepoRow>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_backfill load_row scoped tx: {e}")))?;
    #[allow(clippy::type_complexity)]
    let row: Option<(i64, i64, String, String, String, bool, Option<Uuid>, Option<Uuid>, Option<Value>)> = sqlx::query_as(
        "SELECT installation_id, repo_id, owner, name, default_branch, active, graph_id, owner_user_id, narrative_cursor \
         FROM github_watched_repos WHERE id = $1 AND account_id = $2",
    )
    .bind(repo_row_id)
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();

    Ok(row.map(
        |(installation_id, repo_id, owner, name, default_branch, active, graph_id, owner_user_id, cursor_json)| RepoRow {
            installation_id,
            repo_id,
            owner,
            name,
            default_branch,
            active,
            graph_id,
            owner_user_id,
            narrative_cursor: NarrativeCursor::from_json(cursor_json),
        },
    ))
}

/// Re-checked every page by `process_type`: did an admin toggle this repo
/// off mid-run?
async fn is_active(pool: &PgPool, account_id: Uuid, repo_row_id: Uuid) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_backfill is_active scoped tx: {e}")))?;
    let active: Option<bool> = sqlx::query_scalar("SELECT active FROM github_watched_repos WHERE id = $1 AND account_id = $2")
        .bind(repo_row_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;
    tx.rollback().await.ok();
    Ok(active.unwrap_or(false))
}

/// `pub(crate)` so `workers::github_reconcile_sweep` can persist an updated
/// `since` watermark after a delta pass, reusing the exact same write path
/// (and therefore the exact same `backfill_progress` recompute) the backfill
/// engine itself uses.
pub(crate) async fn persist_cursor_and_progress(
    pool: &PgPool,
    account_id: Uuid,
    repo_row_id: Uuid,
    cursor: &NarrativeCursor,
) -> Result<(), sqlx::Error> {
    let progress = compute_progress(cursor);
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_backfill persist_cursor scoped tx: {e}")))?;
    // `COALESCE(..., '{}'::jsonb) || $2` MERGES top-level keys rather than
    // replacing the whole object. The retired code tier (removed
    // 2026-10-27, the connector no longer captures source code) wrote
    // `code`/`code_skipped`/`summaries` into this SAME column; rows synced
    // before then still carry those keys, and a merge leaves them as the
    // historical record. `compute_progress`'s own keys
    // (prs/issues/releases/commits/updated_at) are always freshly
    // overwritten.
    sqlx::query(
        "UPDATE github_watched_repos SET narrative_cursor = $1, \
            backfill_progress = COALESCE(backfill_progress, '{}'::jsonb) || $2::jsonb, updated_at = now() \
         WHERE id = $3 AND account_id = $4",
    )
    .bind(serde_json::to_value(cursor).expect("NarrativeCursor serialises"))
    .bind(serde_json::to_value(&progress).expect("BackfillProgress serialises"))
    .bind(repo_row_id)
    .bind(account_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

/// `head_sha`, when known, seeds `head_tree_sha` (see this module's own note
/// on why that column stores the default branch's HEAD COMMIT sha, not a
/// literal git tree-object sha — docs/connectors/github.md §9) so the
/// reconcile sweep's very first pass after backfill has a real "last known
/// head" to compare-range from, instead of treating the whole default branch
/// as one giant delta the first time it runs.
async fn mark_fully_synced(pool: &PgPool, account_id: Uuid, repo_row_id: Uuid, head_sha: Option<&str>) -> Result<(), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_backfill mark_fully_synced scoped tx: {e}")))?;
    sqlx::query(
        "UPDATE github_watched_repos SET last_synced_at = now(), \
            head_tree_sha = COALESCE($3, head_tree_sha), updated_at = now() \
         WHERE id = $1 AND account_id = $2",
    )
    .bind(repo_row_id)
    .bind(account_id)
    .bind(head_sha)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

/// Can `user_id` still reach `graph_id` — the gate that must run before this
/// connector writes anything into a destination ocean.
///
/// # Why this exists as its own function
///
/// `routes::ingest::ingest_atom` applied TWO independent gates, and the
/// Path-1 conversion that removed the second one (D7's refusal of shared
/// oceans — the whole point) silently removed the first one too, because it
/// came free with the handler:
///
///   1. **Reachability** (`ingest.rs:1982`, `ReachabilityResolver::
///      graph_reachable_by_user` -> `resolve_tenant_graph_reachable_by_user`,
///      migrations `0091`/`0006`) — "is this ocean one this person may touch
///      AT ALL". That is the gate re-applied here.
///   2. **D7** (`ingest.rs:2040`, `is_own_writable_ocean`) — "is this ocean
///      the caller's OWN private one". Deliberately NOT re-applied: refusing
///      shared oceans is the bug this connector had, and the destination was
///      authorised at selection time by `github_repos::resolve_destination`'s
///      real admin gates instead.
///
/// Dropping (1) matters because `github_watched_repos.graph_id` is chosen
/// ONCE, at selection time, and nothing pauses the row when the chooser's
/// standing changes. A user who is removed from the team, or who leaves the
/// org, leaves behind an active watch row that would otherwise keep pushing
/// this repo's content into an ocean they can no longer reach. The other
/// Path-1 writers do not need an explicit check because they DERIVE their
/// destinations from a live query every time (`google_chat::
/// fan_out_graph_ids` re-reads `graphs` on every fan-out;
/// `workers::huddle` resolves seated attendees per huddle) — a stale
/// membership simply stops producing a destination. This connector reads a
/// stored column instead, so the equivalent freshness has to be asserted.
///
/// # What it checks
///
///   * `resolve_tenant_graph_reachable_by_user(graph_id, account_id,
///     user_id)` on the CONTENT pool — the same SECURITY DEFINER predicate
///     `ingest_atom`'s tenant arm calls, present byte-identically on both
///     trees (control `0091`, tenant `0006`). It covers an archived ocean, an
///     ocean that moved accounts, a `kind='team'` ocean whose `team_members`
///     row is gone or `departing_at`-marked, and a `kind='member'` ocean the
///     user does not own.
///   * On an ORG tenant only, `account_members` on the CONTROL pool — the
///     org-departure half, which the predicate above cannot see for a
///     `kind='org'` ocean (that kind has no per-user clause; membership of
///     the account IS the membership). Scoped to the org plane on purpose:
///     on the consumer plane the account is the user's own personal account
///     and "losing membership" of it is not a state that exists, so checking
///     there could only ever produce a false refusal.
///
/// # Fails closed
///
/// Every error path returns `Err`, and every caller treats `Err` as "do not
/// write". A missing control pool on an org tenant is a refusal, not a pass:
/// the alternative is a gate that silently stops gating the moment its input
/// is unavailable.
pub(crate) async fn destination_reachable(
    state: &Arc<AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    user_id: Uuid,
) -> Result<bool, String> {
    if let TenantId::Org(org_id) = tenant {
        let Some(control) = state.control_db() else {
            return Err(format!(
                "destination reachability: no control pool available to confirm {user_id} is still \
                 a member of org {org_id} — refusing rather than assuming"
            ));
        };
        let still_a_member = crate::routes::slack::membership::is_current_member(control, account_id, user_id)
            .await
            .map_err(|e| format!("destination reachability: account_members read failed: {e}"))?;
        if !still_a_member {
            return Ok(false);
        }
    }

    // Unscoped by design, exactly as `ingest_atom`'s own reachability check
    // is: this is a SECURITY DEFINER carve-out whose whole job is to answer
    // an authorization question about a graph the caller may turn out not to
    // be allowed to see. Wrapping it in a scope that could hide the row
    // would turn "not permitted" and "does not exist" into the same answer
    // one layer too early.
    let reachable: bool =
        sqlx::query_scalar("SELECT public.resolve_tenant_graph_reachable_by_user($1, $2, $3)")
            .bind(graph_id)
            .bind(account_id)
            .bind(user_id)
            .fetch_one(pool)
            .await
            .map_err(|e| format!("destination reachability: graph predicate failed: {e}"))?;
    Ok(reachable)
}

/// The classification a failed tick records on the watch row — the CLOSED
/// SET `github_watched_repos.sync_error_code`'s CHECK constraint accepts
/// (migration `0250`/`0102`).
///
/// **A code, never the error text.** `routes::github::list_repos` serializes
/// this value straight into its API response, and internal error detail must
/// not leak that way: a worker error string can carry a Postgres detail line,
/// a GitHub error payload, a fragment of a connection string. The verbatim
/// message is not lost — it goes to `jobs.error_message` (the queue writes it
/// on every `fail`) and to the worker's own `tracing::warn!(error = %e, …)`
/// line, both operator-only surfaces.
///
/// Only two variants, deliberately. A finer public taxonomy would be
/// precision the UI cannot act on and the classifier cannot honestly
/// justify: everything except a lost destination is ours to fix, not the
/// admin's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncErrorCode {
    /// The ocean this repo is routed into can no longer be reached by the
    /// person who chose it — they left the team, left the org, or the ocean
    /// was archived. The one ACTIONABLE cause (re-route the repo), and the
    /// one that can be established as a FACT rather than guessed from an
    /// error string: [`classify_tick_failure`] re-runs the real reachability
    /// predicate against the live row before recording it.
    DestinationUnreachable,
    /// The honest catch-all.
    SyncFailed,
}

impl SyncErrorCode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SyncErrorCode::DestinationUnreachable => "destination_unreachable",
            SyncErrorCode::SyncFailed => "sync_failed",
        }
    }
}

/// Decide which [`SyncErrorCode`] a just-failed tick should record, by
/// RE-CHECKING the destination's reachability against the live row rather
/// than by parsing the error message.
///
/// Grounding the classification in a fact the database can still answer is
/// the whole point. Substring-matching a worker error string would make the
/// label depend on wording nobody is holding still, and would quietly go
/// wrong the first time an error message was reworded — whereas "can this
/// repo's `owner_user_id` still reach its `graph_id`?" is the same question
/// [`destination_reachable`] asks on the write path, answered the same way.
///
/// Runs only on the failure path, so the extra queries cost nothing on a
/// healthy backfill. Anything it cannot determine (row gone, query failed)
/// falls back to [`SyncErrorCode::SyncFailed`] — the catch-all is the safe
/// answer, never a confident wrong one.
pub(crate) async fn classify_tick_failure(
    state: &Arc<AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    repo_row_id: Uuid,
) -> SyncErrorCode {
    let Ok(Some(row)) = load_row(pool, account_id, repo_row_id).await else {
        return SyncErrorCode::SyncFailed;
    };
    let (Some(graph_id), Some(owner_user_id)) = (row.graph_id, row.owner_user_id) else {
        return SyncErrorCode::SyncFailed;
    };
    match destination_reachable(state, tenant, pool, account_id, graph_id, owner_user_id).await {
        Ok(false) => SyncErrorCode::DestinationUnreachable,
        // Reachable, or we could not tell: either way this failure is not
        // one the admin can act on, so do not claim it is.
        Ok(true) | Err(_) => SyncErrorCode::SyncFailed,
    }
}

/// Record "this repo's last tick failed" on the watch row —
/// migration `0250`/`0102`.
///
/// Why this exists at all: both production `GithubBackfill` jobs sat `dead`
/// for days while `/connectors/github` rendered their repos as `backfilling`
/// with a climbing skip-counter line. Every signal the page had was
/// consistent with progress and none of them was — the code tier's skip
/// counts advance during the TREE WALK, before any ingest is attempted, so
/// they climb whether or not an atom ever lands; `code_cursor.done` stays
/// `false` for the same reason; and a NULL `last_synced_at` on a first
/// backfill is indistinguishable from "still working". The failure was only
/// ever visible in `jobs.error_message`, on the tenant plane, behind no
/// route.
///
/// Best-effort by construction (`Result` returned for the caller to log, not
/// to fail on): this is observability. A failure to record why a tick failed
/// must never become a second reason the tick fails.
///
/// **Written by `workers::github_backfill` only, on purpose.**
/// `workers::github_reconcile_sweep` has no single per-repo verdict to
/// record: its four legs (PR/issue/release/default-branch commits) are each
/// independently warn-and-continue, and a partial leg failure is already
/// handled correctly by `clamp_since_below_failures` holding that type's
/// `since` cursor back so the next sweep re-attempts exactly the items that
/// failed. Wiring a repo-level error out of that shape means giving each leg
/// a verdict it does not currently have. It is also not the surface the
/// 2026-09-23 incident came through: the sweep only ever runs against repos
/// whose `last_synced_at` is already set, and both dead installs were first
/// backfills that had never reached that point. Left as a named follow-up
/// rather than half-wired here, because a `sync_error` that only some
/// failures set is worse than one an operator knows the boundaries of.
pub(crate) async fn record_sync_error(
    pool: &PgPool,
    account_id: Uuid,
    repo_row_id: Uuid,
    code: SyncErrorCode,
) -> Result<(), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_backfill record_sync_error scoped tx: {e}")))?;
    sqlx::query(
        "UPDATE github_watched_repos SET sync_error_code = $3, sync_error_at = now(), updated_at = now() \
         WHERE id = $1 AND account_id = $2",
    )
    .bind(repo_row_id)
    .bind(account_id)
    .bind(code.as_str())
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

/// Clear [`record_sync_error`]'s state — called by every tick that made real
/// progress (or established there is nothing to do).
///
/// Clearing is as load-bearing as setting: without it a single transient
/// rate-limit blip would pin a permanent red badge on a repo that then went
/// on to sync perfectly, and an operator who learns the badge lies stops
/// reading it. Both columns are cleared TOGETHER — `sync_error_at` without
/// `sync_error_code` (or the reverse) is a state nothing knows how to render.
pub(crate) async fn clear_sync_error(
    pool: &PgPool,
    account_id: Uuid,
    repo_row_id: Uuid,
) -> Result<(), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_backfill clear_sync_error scoped tx: {e}")))?;
    // Guarded on `sync_error_code IS NOT NULL` so the common case (a healthy
    // repo ticking along) is a no-op read rather than an `updated_at` bump
    // on every single page of every single backfill.
    //
    // The second predicate is the one that is easy to get wrong. Every
    // non-error `RunOutcome` clears — including `Paused`, because an
    // admin-toggled-off row is not a failing one. But a row paused BECAUSE
    // its destination became unreachable is a failing one, and the very next
    // tick after that pause returns `Paused` (the row is inactive) and would
    // otherwise wipe the actionable code moments after it was written. So a
    // destination-unreachable pause is excluded: that state is cleared by
    // re-routing or re-activating the repo, not by the tick that observes it
    // is off.
    sqlx::query(
        "UPDATE github_watched_repos SET sync_error_code = NULL, sync_error_at = NULL, updated_at = now() \
         WHERE id = $1 AND account_id = $2 AND sync_error_code IS NOT NULL \
           AND NOT (active = false AND paused_reason = 'destination_unreachable')",
    )
    .bind(repo_row_id)
    .bind(account_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

/// Best-effort live HEAD commit sha of the default branch — `None` on any
/// failure (never a hard error: the reconcile sweep just treats a `NULL`
/// `head_tree_sha` as "no baseline yet" and skips the code-diff half of its
/// pass until a later sweep succeeds).
pub(crate) async fn fetch_default_branch_head_sha(http: &reqwest::Client, token: &str, owner: &str, name: &str, default_branch: &str) -> Option<String> {
    let url = format!("{}/repos/{owner}/{name}/branches/{default_branch}", github_api_base());
    let body = github::get_one(http, token, &url).await.ok()?;
    body.get("commit")?.get("sha")?.as_str().map(str::to_string)
}

async fn pause_repo_unavailable(pool: &PgPool, account_id: Uuid, installation_id: i64, repo_id: i64) -> Result<(), sqlx::Error> {
    github_disconnect::pause_repos(pool, account_id, installation_id, Some(repo_id), paused_reason::REPO_UNAVAILABLE).await?;
    Ok(())
}

// ── §3.9 / D10 — Path 1: the hand-rolled per-destination write ─────────────

/// What one [`ingest_one`] call did — used only for logging/telemetry
/// counters (§ report: "ingested / skipped_existing / refused_by_door"). Note
/// there is no synchronous "refused by the Jev door" outcome to observe
/// here: the write below only lands the atom rows and enqueues `EmbedAtoms`;
/// the ADR-0013 door itself runs later, asynchronously
/// (`workers::embed::process_job` enqueues `IngestDoor` right after
/// embedding — see that call site), and a door refusal does not retract the
/// atom row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IngestOutcome {
    Ingested,
    SkippedExisting,
    /// The write produced **zero chunks** — i.e. the body it was handed was
    /// empty or whitespace-only, so no atom could exist. This is NOT
    /// "already ingested": it is a normaliser defect, and it is a distinct
    /// variant because conflating the two is what let a backfill that
    /// ingested nothing at all report `fully synced` with a 100% progress
    /// bar (found during the §6 local verification against
    /// `CoralSwarm/coralswarm-connect`: every one of its 9 PRs came back
    /// "skipped_existing" while `atoms` held not a single github row).
    EmptyBody,
}

/// Classify one destination's write. Pure, so the whole decision table is
/// unit-testable without an `AppState` or a database.
///
/// | atoms | chunks | duplicates | verdict |
/// |---|---|---|---|
/// | >0 | any | any | [`IngestOutcome::Ingested`] |
/// | 0 | 0 | any | [`IngestOutcome::EmptyBody`] — normaliser defect |
/// | 0 | >0 | == chunks | [`IngestOutcome::SkippedExisting`] — real re-ingest |
/// | 0 | >0 | < chunks | `Err` — content dropped after chunking |
///
/// The last row is the one that matters: `ON CONFLICT DO NOTHING` returning
/// nothing means "already there", but an atom row DELETED again because its
/// R2 blob upload failed also returns nothing — and calling that
/// "skipped_existing" is precisely how a repo whose content never landed
/// reported 100%. `skipped_duplicates` is counted from the INSERT's own
/// `RETURNING` set, before any blob failure can trim it, which is what makes
/// the two distinguishable at all.
pub(crate) fn classify_ingest_response(
    atom_ids_len: usize,
    chunks_created: u64,
    skipped_duplicates: u64,
) -> Result<IngestOutcome, String> {
    if atom_ids_len > 0 {
        return Ok(IngestOutcome::Ingested);
    }
    if chunks_created == 0 {
        return Ok(IngestOutcome::EmptyBody);
    }
    if skipped_duplicates >= chunks_created {
        return Ok(IngestOutcome::SkippedExisting);
    }
    Err(format!(
        "ingest produced {chunks_created} chunk(s), {skipped_duplicates} duplicate(s), and no atom — \
         content was dropped after chunking (check the blob store); refusing to count this as progress"
    ))
}

/// What one [`write_atom_to_destinations`] call accomplished, per
/// destination and honestly — never collapsed into a single ambiguous count.
/// The caller commits a cursor ONLY when `fully_succeeded()`: the
/// commit-after-ingest discipline, expressed over Path 1's counters rather
/// than Path 2's HTTP statuses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WriteOutcome {
    /// Atom rows that are durably committed AND have a readable body
    /// (blob uploaded, or inlined in dev mode), summed over destinations.
    pub(crate) atoms_written: usize,
    /// Chunks this atom's body produced, summed over the destinations that
    /// actually ran. Zero means the body was empty — a normaliser defect,
    /// not a duplicate.
    pub(crate) chunks_created: usize,
    /// Chunks the `(graph_id, source_type, source_id)` unique key refused as
    /// already present, counted from the INSERT's `RETURNING` set BEFORE any
    /// blob failure can trim it.
    pub(crate) skipped_duplicate_chunks: usize,
    pub(crate) destinations_ok: usize,
    pub(crate) destinations_failed: usize,
    /// The FIRST destination failure's own message, kept so the caller can
    /// report WHY rather than only how many.
    ///
    /// Counts alone are not actionable: "1 of 1 destination(s) failed" tells
    /// an operator reading `jobs.error_message` nothing they can do, and the
    /// reachability refusal in particular ("the chooser left the team") is
    /// precisely the failure an admin CAN fix. Only the first is kept — a
    /// concatenation of N messages is a log line, not an error, and every
    /// failure is already logged individually beside the counter.
    pub(crate) first_destination_error: Option<String>,
}

impl WriteOutcome {
    pub(crate) fn fully_succeeded(&self) -> bool {
        self.destinations_failed == 0
    }
}

/// Write one normalised GitHub atom into every destination graph —
/// **Path 1** (§3.9/D10), the shape `workers::huddle::ingest_huddle_notes`
/// (the fully-worked multi-graph reference),
/// `routes::slack_events::ingest_slack_message` and
/// `connectors::google_chat::ingest_space_items` already use.
///
/// # Why NOT Path 2 (`routes::ingest::ingest_atom` in-process)
///
/// This connector shipped Path 2 and it was wrong. `ingest_atom` enforces
/// ADR-0007's **D7 destination invariant** via
/// `ingest::is_own_writable_ocean`, which admits only
/// `kind='personal'` or a `kind='member'` ocean the CALLER owns. A
/// `kind='team'`/`kind='org'` graph has `owner_user_id = NULL` and is
/// neither, so `ingest_atom` answered `400 "can only ingest into your own
/// private ocean; share via promotion"` unconditionally — every
/// team/org-destination backfill ingested exactly zero atoms while its
/// job died at `"ingest_atom failed: 400 Bad Request"` after three attempts
/// (confirmed in production, 2026-09-23, on both `CoralSwarm/coralswarm` and
/// `CoralSwarm/coralswarm-connect`).
///
/// **The argument that used to live here — on the deleted
/// `synthetic_ingest_identity` — reasoned about the WRONG GATE.** It cited
/// `resolve_tenant_graph_reachable_by_user` (migrations_tenant/0006) to
/// conclude the worker was "safe for all three destination kinds". That
/// function backs `ingest_atom`'s *reachability* check at `ingest.rs:1982`,
/// which does pass for a team/org graph the selecting human could reach. D7
/// is a **second, independent** gate sixty lines further down that then
/// refuses anyway. Reachability is not writability, and conflating them is
/// what made a structurally impossible write look designed-for.
///
/// `connectors::google_chat::ingest_space_items` had already written the
/// warning down verbatim — *"`is_own_writable_ocean` refuses `kind='org'`/
/// `'team'` by design — a shared ocean is promotion-fed. So `ingest_atom` is
/// structurally unavailable for the container archetype"*, and shipping the
/// Path-2 shape anyway *"would work today and break at G4.6"* precisely
/// because a personal destination passes the gate. GitHub's personal
/// destinations did work; its shared ones never could.
///
/// D7 itself is NOT relaxed by this change. It is a deliberate security
/// invariant protecting member-private oceans (PR #341 had to fix a
/// deny-bypass in it), and the destinations this function writes to were
/// authorised at SELECTION time instead, by
/// `connectors::github_repos::resolve_destination`'s real admin gates
/// (`check_team_ocean_admin` for an existing team, `verify_org_admin` for
/// the org ocean). That is the same trade every other Path-1 writer makes:
/// the route-level "is this YOUR ocean" rule is for interactive callers
/// naming their own destination, not for a background worker replaying a
/// destination an admin already chose and that `github_watched_repos` has
/// recorded ever since.
///
/// # The four lessons this loop is required to keep (§1.10)
///
/// * **No bare `?` inside the per-destination loop.** Each iteration is its
///   own fallible `async` block, so a transient failure on destination 2 of
///   N cannot silently skip 3..N. That bug was live in `slack_events.rs` and
///   *"unreachable while fan-out returned ≤1 graph, which is exactly why no
///   test caught it"* — GitHub is in that same trap today (one destination
///   per watch row), so the shape is what protects it, not the arithmetic.
/// * **An infrastructure gap never writes a terminal row.** A DEK or
///   blob-key failure returns before committing; nothing here writes an
///   `ON CONFLICT DO NOTHING` marker a later fix could not overwrite. This
///   is stronger than the three implementations above: the atom rows are
///   committed only AFTER their blobs are durable, so a crash in that window
///   rolls the rows back instead of stranding a blob-less atom that holds
///   the unique key forever. See the long comment at the commit itself.
/// * **Per-destination independence reported honestly**
///   ([`WriteOutcome::destinations_ok`]/[`WriteOutcome::destinations_failed`]),
///   never collapsed into one ambiguous count.
/// * **A failed body upload fails the destination.** Nothing it wrote
///   commits, so no bodiless atom holds the `(graph_id, source_type,
///   source_id)` unique key, and the retry recreates it cleanly.
///
/// # The destination is the watch row's grant
///
/// `repo_row_id` names the `github_watched_repos` binding. The ingest door's
/// [`OrgSharedGrant::github_repo`](crate::ingest::OrgSharedGrant::github_repo)
/// reads it, requires it active, and requires its chooser to still reach its
/// ocean ([`destination_reachable`]); the door re-reads the row in the write's
/// transaction. The chooser is the atoms' `ingested_by`.
///
/// # `atoms.symbols` (PR #1040) is carried through, deliberately
///
/// The three Path-1 implementations this one is modelled on all hand-roll
/// their `INSERT INTO atoms (...)` and none of them writes `symbols` — so a
/// verbatim port would have silently reverted #1040 for every GitHub atom,
/// which is the one source type where it matters most (`symbols` is
/// populated ONLY by the code-chunking path, i.e. `github_code`). This
/// function therefore goes through `Store::insert_atoms_scoped` — the exact
/// method `routes::ingest` uses — rather than its own SQL, so `excerpt`,
/// `title`, `title_kind` and `symbols` are all derived from the same
/// in-memory `Chunk` and land in the same one statement. Pinned by
/// `github_conformance::conformance_symbols_survive_the_path_1_write_pg`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_atom_to_destinations(
    state: &Arc<AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    grant: &crate::ingest::OrgSharedGrant,
    atom: &NormalizedAtom,
) -> WriteOutcome {
    write_chunks_to_destinations(state, tenant, pool, account_id, grant, atom, &chunk_atom(atom)).await
}

/// Resolve the grant for a watch row the caller has loaded, once per backfill
/// tick or per webhook row with atoms to write, to hand to every write under
/// it; the door still re-checks the row inside each write's transaction
/// (`OrgSharedGrant::verify_tx`). `Err` is why nothing may be written under
/// it: a transient lookup failure, an unbound row, or a chooser who lost the
/// ocean.
///
/// A DEFINITE refusal of a bound row (its chooser lost the ocean) also stops
/// the retry loop: refusing alone leaves the watch row `active`, so the queue
/// keeps re-enqueuing a backfill that can only ever fail; pausing turns that
/// into a terminal state an admin can see and fix. `routes::teams::
/// remove_team_member` pauses immediately on the roster event; this is the
/// backstop for every other way reachability is lost (an org departure, an
/// archived ocean) and for a roster event that never fired. It pauses once
/// per resolution, not once per atom. A transient failure resolving the grant
/// must NOT pause: a database blip is not a departure, and un-pausing is a
/// manual step. Refuse the writes, retry on the ladder.
pub(crate) async fn resolve_repo_grant(
    state: &Arc<AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    binding: &RepoBinding,
) -> Result<crate::ingest::OrgSharedGrant, String> {
    let repo_row_id = binding.row_id;
    match crate::ingest::OrgSharedGrant::github_repo(state, tenant, pool, account_id, binding).await {
        Ok(grant) => Ok(grant),
        Err(crate::ingest::RepoRefusal::Lookup(e)) => {
            tracing::warn!(connector_health = true, error = %e, %repo_row_id, %account_id, "github ingest: watch row grant lookup failed");
            Err(e)
        }
        Err(refusal) => {
            let why = match refusal {
                crate::ingest::RepoRefusal::Unbound => {
                    format!("watch row {repo_row_id} is not an active binding to a destination ocean — refusing to write")
                }
                crate::ingest::RepoRefusal::Lookup(e) => e,
                crate::ingest::RepoRefusal::Unreachable { graph_id, owner_user_id } => {
                    // Best-effort: the refusal is the safety property and
                    // does not depend on this succeeding.
                    if let Err(e) =
                        github_disconnect::pause_for_unreachable_destination(pool, account_id, owner_user_id, graph_id).await
                    {
                        tracing::warn!(
                            connector_health = true, error = %e, %graph_id, %account_id,
                            "github ingest: destination unreachable, but pausing the watch row failed \
                             — the write is still refused, the retry loop just will not stop yet"
                        );
                    }
                    format!(
                        "destination ocean {graph_id} is no longer reachable by {owner_user_id} \
                         (left the team or org, or the ocean was archived) — refusing to write"
                    )
                }
            };
            tracing::warn!(connector_health = true, error = %why, %repo_row_id, %account_id, "github ingest: no grant for this watch row");
            Err(why)
        }
    }
}

/// Chunk one normalised atom exactly the way
/// [`write_atom_to_destinations`] does — the SAME strategy selection
/// `routes::ingest::ingest_atom`'s step 4 makes, reproduced here rather than
/// inferred: `code_path` is the one seam `for_source_type` structurally
/// cannot cover (it only ever sees the bare source-type string), and it is
/// `Some` only for `github_code` atoms (see `NormalizedAtom::code_path`).
pub(crate) fn chunk_atom(atom: &NormalizedAtom) -> Vec<coralswarm_core::chunker::Chunk> {
    coralswarm_core::chunker::chunk(
        &atom.content,
        crate::ingest::capture::chunk_strategy(atom.source_type, atom.code_path.as_deref(), None),
    )
}

/// [`write_atom_to_destinations`] with the chunking already done — the whole
/// body of that function; see its doc comment for every design decision this
/// one implements.
///
/// Split out so a test can hand it chunks it built itself. That is not
/// cosmetic: `Chunk::symbols` is populated ONLY by the tree-sitter walk,
/// which is behind the non-default `code-chunking` feature, so in a build
/// without that feature EVERY chunk this connector produces has empty
/// `symbols` — and a test that went through [`chunk_atom`] would then
/// assert `NULL == NULL` and pass whether or not this function carries
/// `symbols` through at all. Production runs WITH the feature (see
/// `backend/Cargo.toml`), so that vacuous pass would be a green gate over a
/// real regression. Handing the chunks in makes the assertion honest in
/// every build — `github_conformance::
/// conformance_symbols_survive_the_path_1_write_pg`.
pub(crate) async fn write_chunks_to_destinations(
    state: &Arc<AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    grant: &crate::ingest::OrgSharedGrant,
    atom: &NormalizedAtom,
    chunks: &[coralswarm_core::chunker::Chunk],
) -> WriteOutcome {
    use sha2::Digest as _;

    let mut outcome = WriteOutcome::default();
    outcome.chunks_created = chunks.len();
    // The binding's chooser, whom the atoms are attributed to. Only a GitHub
    // grant has one; another grant here is a caller's mistake and writes
    // nothing rather than attributing the atoms to nobody.
    let Some(ingested_by) = grant.github_chooser() else {
        outcome.destinations_failed = grant.oceans().len();
        outcome.first_destination_error = Some("github ingest: the grant is not a GitHub repo binding".to_string());
        return outcome;
    };
    if chunks.is_empty() {
        // Zero chunks is a real, reportable verdict (`IngestOutcome::
        // EmptyBody`), not a failure — the caller's own classification
        // decides what to do with it. Returning here writes nothing at all,
        // which is exactly right: there is no row to mark.
        return outcome;
    }

    // One DEK per account, resolved once and shared across destinations. A
    // null-key "encryption" would be a security hole, so a failure here
    // bails the whole write rather than falling through to a placeholder —
    // `slack_events`'/`google_chat`'s posture. Lesson 2: this returns
    // BEFORE any row is committed, so a later fix can still ingest this
    // atom; there is no `ON CONFLICT DO NOTHING` tombstone in the way.
    let dek: [u8; 32] =
        match crate::crypto::resolve_dek_cached_for_tenant(tenant, pool, account_id, &state.dek_cache).await {
            Ok(Some(d)) => d,
            Ok(None) => {
                tracing::warn!(
                    connector_health = true, %account_id, source_id = %atom.source_id,
                    "github ingest: account has no DEK — writing nothing (run key rotation first)"
                );
                outcome.destinations_failed = grant.oceans().len();
                return outcome;
            }
            Err(e) => {
                tracing::warn!(
                    connector_health = true, error = %e, %account_id, source_id = %atom.source_id,
                    "github ingest: DEK resolution failed — writing nothing"
                );
                outcome.destinations_failed = grant.oceans().len();
                return outcome;
            }
        };

    let content_hash = hex::encode(sha2::Sha256::digest(atom.content.as_bytes()));

    // Everything below is graph-INDEPENDENT (same body, same chunking, same
    // derived columns for every destination) so it is prepared once, outside
    // the loop; only the atom ids and their blob keys are per-destination.
    //
    // No contextual chunk header: `ingest::atom::build_contextual_header`
    // returns `None` for every github source type (none of them is
    // `ChunkStrategy::Transcript`, and `provenance_source` does not list
    // them either), so Path 2 prepended nothing and left `header_len` NULL.
    // Reproduced exactly — a header introduced here would change the bytes
    // embedded for every github atom and desynchronise citation offsets
    // against the ones already ingested. Chunk ids are
    // `{source_id}:chunk:{position}`, the identity every writer uses, which is
    // what makes the switch to Path 1 idempotent against atoms an earlier
    // Path-2 run landed. `symbols` (PR #1040) rides along in the prepared
    // columns; dropping it is the regression this function's doc warns about.
    let prepared = crate::ingest::capture::Prepared::new(
        &atom.source_id,
        atom.source_type,
        &atom.content,
        chunks,
        &crate::ingest::capture::Header::None,
        None,
    );

    let store = PostgresStore::new(pool.clone());

    for &graph_id in grant.oceans() {
        // Lesson 1: ONE fallible async block per destination, awaited and
        // matched below. A `?` in here abandons THIS destination only.
        // Yields `(written, duplicates)`: "already there" is not "written".
        let destination: Result<(usize, usize), String> = async {
            // Native IDs are produced by the trusted GitHub normalizer as
            // `{repository_id}:...`; the door refuses one whose repository is
            // not the watch row's.
            let repository_id = atom.source_id.split(':').next().unwrap_or_default();
            if repository_id.is_empty() {
                return Err("github source is missing its repository identity".to_string());
            }
            let namespace = format!("github:repository:{repository_id}");
            let metadata = serde_json::json!({"title":atom.title,"code_path":atom.code_path,
                "author_id":atom.author_id,"author_name":atom.author_name});
            let origin = crate::source_origin::SourceOrigin::native("github",atom.source_type.strip_prefix("github_").unwrap_or(atom.source_type),"github_capture")?
                .with_container("repository",repository_id,&namespace,None)?;
            // Version, chunk atoms with their excerpt/title/symbols and
            // author provenance, bodies, reconcile/links and the embed job,
            // in one transaction through the ingest door. Bodies are uploaded
            // BEFORE the commit (CodeRabbit round 1, Major/Data Integrity): a
            // crash mid-upload rolls the rows back and the retry re-ingests
            // cleanly, and an upload error fails the destination, so the tick
            // fails rather than calling a bodiless repo "fully synced".
            // Reconcile adopts unchanged positions, replaces edited ones in
            // place and retires positions the file no longer has.
            let ingested = crate::ingest::submit(crate::ingest::IngestRequest {
                origin: Some(&origin),
                ..crate::ingest::IngestRequest::new(crate::ingest::Destination::OrgShared(crate::ingest::Routed {
                    ocean: graph_id,
                    grant,
                    writer: crate::ingest::SystemWriter { pool, tenant, dek: &dek, r2: state.r2.as_deref() },
                    source: crate::source_evidence::CaptureSource {
                        source_type: atom.source_type, provider_namespace: &namespace,
                        source_id: &atom.source_id, normalized_text: &atom.content,
                        representation: if atom.source_type == "github_code_summary" { "generated_summary" } else { "normalized_text" }, metadata: &metadata,
                        occurred_at: Some(atom.occurred_at), participants: &atom.participants,
                        ingested_by: Some(ingested_by), scope: "private", duration_ms: None, segments: None,
                    },
                    chunks: &prepared,
                    // The same defaults `IngestAtomRequest`'s `None`s resolved
                    // to on the Path-2 call this replaced (`scope` ->
                    // "private", `fts_language` -> "english"). No content
                    // hash: nothing re-fetches a GitHub atom by source id and
                    // asks "did it change" (that is the Calendar edit path).
                    atoms: crate::ingest::capture::AtomFields {
                        author_id: atom.author_id.as_deref(),
                        author_name: atom.author_name.as_deref(),
                        ..crate::ingest::capture::AtomFields::new(atom.source_type, &atom.participants, atom.occurred_at, Some(ingested_by))
                    },
                    content_hash: &content_hash,
                    sync_ops: Some(&store),
                }))
            })
            .await;
            let ingested = match ingested {
                Ok(ingested) => ingested,
                Err(refused) => return Err(refused.to_string()),
            };
            let surviving = ingested.body.atom_ids;
            if !surviving.is_empty() {
                // The journal's "N items captured" row. Path 2 fired this
                // per ingest call and only when something actually landed
                // (ticket 18: a fully-duplicate re-ingest must emit NO
                // event), so the same guard applies here.
                crate::routes::activity::insert_activity_event(
                    pool.clone(),
                    account_id,
                    ingested_by,
                    Some(graph_id),
                    "atom_ingested",
                    serde_json::json!({
                        "atom_count": surviving.len(),
                        "chunks_created": chunks.len(),
                        "source_type": atom.source_type,
                    }),
                );
            }

            Ok((surviving.len(), ingested.body.skipped_duplicate_chunks))
        }
        .await;

        match destination {
            Ok((written, duplicates)) => {
                outcome.destinations_ok += 1;
                outcome.atoms_written += written;
                outcome.skipped_duplicate_chunks += duplicates;
            }
            Err(e) => {
                outcome.destinations_failed += 1;
                tracing::warn!(
                    connector_health = true, error = %e, %graph_id, %account_id,
                    source_id = %atom.source_id,
                    "github ingest: destination failed — continuing with the remaining \
                     destinations (each is independently best-effort; a failure here must \
                     not skip the rest of the fan-out)"
                );
                if outcome.first_destination_error.is_none() {
                    outcome.first_destination_error = Some(e);
                }
            }
        }
    }
    outcome
}

/// Write one atom into ONE destination graph and classify the result — the
/// single-destination adapter every GitHub ingest call site uses
/// (`process_type` below and `connectors::github_event_ingest`). The
/// real write is [`write_atom_to_destinations`]; see its doc comment for why
/// this is Path 1 and not `routes::ingest::ingest_atom`.
///
/// A watch row has exactly one destination (§3.4: "personal / team / org,
/// always exactly one ocean"), so the slice handed down is always length 1
/// today. The fan-out shape is kept anyway, because "unreachable while
/// fan-out returned ≤1 graph" is exactly the condition under which
/// `slack_events`' skip-the-rest bug stayed invisible.
pub(crate) async fn ingest_one(
    state: &Arc<AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    grant: &crate::ingest::OrgSharedGrant,
    atom: NormalizedAtom,
) -> Result<IngestOutcome, String> {
    let outcome = write_atom_to_destinations(state, tenant, pool, account_id, grant, &atom).await;

    // Anything short of `fully_succeeded()` is an error for THIS item, which
    // the callers turn into a failed tick that resumes from the last
    // persisted cursor and re-attempts this exact item.
    if !outcome.fully_succeeded() {
        // The first destination's own message is carried through, not just
        // the counts: this string becomes `jobs.error_message`, which is the
        // surface an operator actually reads, and "1 of 1 destination(s)
        // failed" on its own names nothing anyone can act on.
        let why = outcome
            .first_destination_error
            .as_deref()
            .unwrap_or("no destination reported a reason");
        return Err(format!(
            "github ingest: {} of {} destination(s) failed, for {} {}: {why}",
            outcome.destinations_failed,
            outcome.destinations_ok + outcome.destinations_failed,
            atom.source_type,
            atom.source_id,
        ));
    }

    let verdict = classify_ingest_response(
        outcome.atoms_written,
        outcome.chunks_created as u64,
        outcome.skipped_duplicate_chunks as u64,
    );
    match &verdict {
        Ok(IngestOutcome::EmptyBody) => tracing::warn!(
            connector_health = true,
            source_type = %atom.source_type,
            source_id = %atom.source_id,
            body_len = atom.content.len(),
            "github ingest produced ZERO chunks — the normalised body was empty, no atom was written"
        ),
        Err(e) => tracing::warn!(
            connector_health = true,
            source_type = %atom.source_type,
            source_id = %atom.source_id,
            body_len = atom.content.len(),
            error = %e,
            "github ingest kept nothing and the chunks were not duplicates"
        ),
        _ => {}
    }
    verdict
}

// ── Per-item fetch dispatch ─────────────────────────────────────────────────

/// Fetch (if needed) + normalise one list item, per its type. `Ok(None)`
/// means "intentionally not an atom" — a PR row surfaced by the `/issues`
/// listing, or a commit already covered by a PR (association or squash-merge
/// pattern match) — never an error.
async fn fetch_item_atom(
    http: &reqwest::Client,
    token: &str,
    row: &RepoRow,
    kind: TypeKind,
    item: &Value,
    prev_release_tag: Option<&str>,
) -> Result<Option<NormalizedAtom>, GithubApiError> {
    match kind {
        TypeKind::Pr => {
            let number = item.get("number").and_then(Value::as_i64).unwrap_or(0);
            let atom = github_narrative::fetch_pr_atom(http, token, &row.owner, &row.name, row.repo_id, number).await?;
            Ok(Some(atom))
        }
        TypeKind::Issue => {
            if github_narrative::is_actually_a_pull_request(item) {
                return Ok(None);
            }
            let atom = github_narrative::fetch_issue_atom(http, token, &row.owner, &row.name, row.repo_id, item).await?;
            Ok(Some(atom))
        }
        TypeKind::Release => {
            let atom = github_narrative::normalize_release(row.repo_id, &row.owner, &row.name, item, prev_release_tag);
            Ok(Some(atom))
        }
        TypeKind::Commit => fetch_commit_if_direct(http, token, &row.owner, &row.name, row.repo_id, item).await,
    }
}

/// Fetch a full commit's detail and normalise it, UNLESS it is already
/// covered by a PR (squash-merge message pattern, a PR merge commit, or the
/// association endpoint reports a linked PR) — `pub(crate)` so `connectors::
/// github_event_ingest`'s push-compare-range handler reuses the exact same
/// direct-commit filter the backfill's own commit-type pass uses (§3.7 "same
/// handlers").
pub(crate) async fn fetch_commit_if_direct(
    http: &reqwest::Client,
    token: &str,
    owner: &str,
    repo: &str,
    repo_id: i64,
    item: &Value,
) -> Result<Option<NormalizedAtom>, GithubApiError> {
    let sha = item.get("sha").and_then(Value::as_str).unwrap_or_default();
    if sha.is_empty() {
        return Ok(None);
    }
    let message = item.get("commit").and_then(|c| c.get("message")).and_then(Value::as_str).unwrap_or_default();
    let first_line = message.lines().next().unwrap_or(message);
    if github_narrative::squash_merge_pr_number(first_line).is_some() {
        // Known squash-merge of a PR we already cover via the PR path —
        // skip WITHOUT hitting the association endpoint (§3.5's own
        // instruction).
        return Ok(None);
    }
    if github_narrative::is_pr_merge_commit(item) {
        // GitHub's own `Merge pull request #N from ...` merge commit: the PR
        // path covers it. Decided before the association endpoint, which
        // lags a just-merged PR by seconds (the push webhook lands inside
        // that window).
        return Ok(None);
    }

    let assoc_url = format!("{}/repos/{owner}/{repo}/commits/{sha}/pulls", github_api_base());
    let assoc = github::get_paginated(http, token, &assoc_url, 2).await?;
    if github_narrative::has_pr_association(&assoc) {
        return Ok(None);
    }

    let detail_url = format!("{}/repos/{owner}/{repo}/commits/{sha}", github_api_base());
    let detail = github::get_one(http, token, &detail_url).await?;
    Ok(Some(github_narrative::normalize_commit(repo_id, &detail)))
}

// ── Page/type processing ────────────────────────────────────────────────────

enum PageOutcome {
    /// Hit the per-tick page budget with more pages still pending.
    MoreWork,
    /// This type's `done` flag just flipped true.
    TypeDone,
    RateLimited(std::time::Duration),
    /// A 404 fetching the repo's own list endpoint — the repo vanished.
    RepoGone,
    /// `active` flipped false mid-run (re-checked every page).
    PausedMidRun,
}

#[allow(clippy::too_many_arguments)]
async fn process_type(
    tenant: &TenantId,
    pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    repo_row_id: Uuid,
    row: &RepoRow,
    grant: &crate::ingest::OrgSharedGrant,
    token: &str,
    kind: TypeKind,
    cursor: &mut NarrativeCursor,
) -> Result<PageOutcome, String> {
    let mut pages_this_tick = 0usize;
    let mut counters = (0usize, 0usize, 0usize); // (ingested, skipped_existing, empty_body)

    loop {
        if pages_this_tick >= MAX_PAGES_PER_TICK {
            tracing::info!(
                repo_row_id = %repo_row_id, kind = kind.key(),
                ingested = counters.0, skipped_existing = counters.1, empty_body = counters.2,
                "github_backfill: tick budget reached, more pages pending"
            );
            return Ok(PageOutcome::MoreWork);
        }
        match is_active(pool, account_id, repo_row_id).await {
            Ok(true) => {}
            Ok(false) => return Ok(PageOutcome::PausedMidRun),
            Err(e) => return Err(e.to_string()),
        }

        let url = page_url_for(kind, cursor, &row.owner, &row.name, &row.default_branch);

        let (items, next_url, last_page) = match github::get_page(&state.http_client, token, &url).await {
            Ok(v) => v,
            Err(GithubApiError::RateLimited { retry_after }) => return Ok(PageOutcome::RateLimited(retry_after)),
            Err(GithubApiError::Http { status: 404, .. }) => return Ok(PageOutcome::RepoGone),
            Err(e) => return Err(e.to_string()),
        };

        if cursor.field(kind).total_hint.is_none() {
            let hint = last_page.map(|lp| (lp * LIST_PAGE_SIZE) as u64).or_else(|| {
                // No `rel="last"` — the whole type fit on one page.
                if next_url.is_none() { Some(items.len() as u64) } else { None }
            });
            cursor.field_mut(kind).total_hint = hint;
        }

        let mut hit_floor = false;
        for (idx, item) in items.iter().enumerate() {
            // A member-chosen window ends the walk at the first item past it.
            if before_floor(cursor.window_floor, kind, item) {
                hit_floor = true;
                break;
            }
            let prev_release_tag = if kind == TypeKind::Release {
                items.get(idx + 1).and_then(|n| n.get("tag_name")).and_then(Value::as_str)
            } else {
                None
            };

            let fetch_result = fetch_item_atom(&state.http_client, token, row, kind, item, prev_release_tag).await;
            match fetch_result {
                Ok(None) => {}
                Ok(Some(atom)) => {
                    let occurred_at = atom.occurred_at;
                    match ingest_one(state, tenant, pool, account_id, grant, atom).await {
                        Ok(IngestOutcome::Ingested) => counters.0 += 1,
                        Ok(IngestOutcome::SkippedExisting) => counters.1 += 1,
                        Ok(IngestOutcome::EmptyBody) => counters.2 += 1,
                        Err(e) => {
                            // FATAL for this tick, deliberately not a
                            // `continue`. The cursor has already been
                            // persisted up to the previous page, so failing
                            // here means the retry resumes from there and
                            // re-attempts this item; carrying on instead
                            // would advance `seen`/`page_url` past content
                            // that was never stored and let the type report
                            // `done`. Same durability rule
                            // `connectors::history_backfill` documents:
                            // complete ONLY when everything in the window
                            // landed.
                            tracing::error!(
                                connector_health = true,
                                error = %e,
                                repo_row_id = %repo_row_id,
                                kind = kind.key(),
                                "github_backfill: ingest failed — failing the job so the retry re-runs from the last persisted cursor"
                            );
                            return Err(e);
                        }
                    }
                    let tc = cursor.field_mut(kind);
                    tc.seen += 1;
                    if tc.since.is_none_or(|s| occurred_at > s) {
                        tc.since = Some(occurred_at);
                    }
                }
                Err(GithubApiError::RateLimited { retry_after }) => return Ok(PageOutcome::RateLimited(retry_after)),
                Err(e) => {
                    tracing::error!(
                        connector_health = true,
                        error = %e,
                        repo_row_id = %repo_row_id,
                        kind = kind.key(),
                        "github_backfill: item fetch failed — failing the job so the retry re-runs from the last persisted cursor"
                    );
                    return Err(e.to_string());
                }
            }
        }

        // Past the floor there is nothing left to fetch: this type is swept.
        let next_url = if hit_floor { None } else { next_url };
        cursor.field_mut(kind).page_url = next_url.clone();
        if next_url.is_none() {
            cursor.field_mut(kind).done = true;
        }
        pages_this_tick += 1;

        if let Err(e) = persist_cursor_and_progress(pool, account_id, repo_row_id, cursor).await {
            return Err(e.to_string());
        }

        if next_url.is_none() {
            tracing::info!(
                repo_row_id = %repo_row_id, kind = kind.key(),
                ingested = counters.0, skipped_existing = counters.1, empty_body = counters.2,
                "github_backfill: type fully swept"
            );
            return Ok(PageOutcome::TypeDone);
        }
    }
}

// ── Entry point ──────────────────────────────────────────────────────────────

pub(crate) enum RunOutcome {
    /// Real progress was made this tick and durably persisted; more work
    /// remains (either more pages of the current type, or another type).
    Continue,
    /// Every type's cursor reports `done: true` — `last_synced_at` stamped.
    Done,
    RateLimited(std::time::Duration),
    /// The row is inactive (toggled off, uninstalled, repo removed, or just
    /// went 404) — nothing further to do until re-selected.
    Paused,
}

/// One time-sliced tick of the resumable backfill for `repo_row_id`. See
/// this module's doc comment for the cursor shape, time-slicing, and
/// rate-limit contracts.
pub(crate) async fn run_tick(
    tenant: &TenantId,
    pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    repo_row_id: Uuid,
) -> Result<RunOutcome, String> {
    let row = match load_row(pool, account_id, repo_row_id).await {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(RunOutcome::Paused),
        Err(e) => return Err(e.to_string()),
    };
    let binding = row.binding(repo_row_id);
    if binding.bound().is_none() {
        // Toggled off, or no destination picked yet: paused. An active row
        // with a destination but no chooser cannot exist (active requires a
        // resolved destination).
        if row.active && row.graph_id.is_some() {
            return Err(
                "github backfill: active row has no owner_user_id — should be unreachable (active requires a resolved destination)"
                    .to_string(),
            );
        }
        return Ok(RunOutcome::Paused);
    }

    let Some((_, owner_user_id)) = binding.bound() else { return Ok(RunOutcome::Paused); };

    // The member's pause or off switch for GitHub, before any token or fetch. The
    // job ends as for an inactive row ("not re-enqueuing"); the cursor stays, so
    // the backfill goes on from it once the reconcile sweep re-enqueues the row.
    if let Some(control) = state.control_db() {
        if crate::connectors::catalog::pause::is_paused(
            control, account_id, owner_user_id, crate::connectors::catalog::pause::GITHUB_REPOS,
        ).await {
            return Ok(RunOutcome::Paused);
        }
    }

    let token = github::installation_token(&state.http_client, row.installation_id)
        .await
        .map_err(|e| format!("installation token: {e}"))?;

    // The connector captures no repository source code (founder decision
    // 2026-10-27, issue #1516): no file-content tier and no package-summary
    // tier run here any more. A row whose retired `code_cursor` was still
    // mid-walk simply starts on its narrative types; the column is left as
    // it is, unread.
    let mut cursor = row.narrative_cursor.clone();

    if cursor.all_done() {
        let head_sha = fetch_default_branch_head_sha(&state.http_client, &token, &row.owner, &row.name, &row.default_branch).await;
        mark_fully_synced(pool, account_id, repo_row_id, head_sha.as_deref()).await.map_err(|e| e.to_string())?;
        return Ok(RunOutcome::Done);
    }
    // Once per tick, not per atom: a refused row fails the tick before its
    // first write, as the first write used to.
    let grant = resolve_repo_grant(state, tenant, pool, account_id, &binding)
        .await
        .map_err(|why| format!("github ingest: no grant for watch row {repo_row_id}: {why}"))?;
    let kind = if !cursor.pr.done {
        TypeKind::Pr
    } else if !cursor.issue.done {
        TypeKind::Issue
    } else if !cursor.release.done {
        TypeKind::Release
    } else {
        TypeKind::Commit
    };
    let outcome = process_type(tenant, pool, state, account_id, repo_row_id, &row, &grant, &token, kind, &mut cursor).await;

    match outcome? {
        PageOutcome::MoreWork | PageOutcome::TypeDone => Ok(RunOutcome::Continue),
        PageOutcome::RateLimited(d) => Ok(RunOutcome::RateLimited(d)),
        PageOutcome::RepoGone => {
            pause_repo_unavailable(pool, account_id, row.installation_id, row.repo_id).await.map_err(|e| e.to_string())?;
            Ok(RunOutcome::Paused)
        }
        PageOutcome::PausedMidRun => Ok(RunOutcome::Paused),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── classify_ingest_response — the decision table that a silent,
    // totally-empty backfill got through before it existed.

    #[test]
    fn classify_new_atoms_is_ingested() {
        assert_eq!(classify_ingest_response(3, 3, 0).unwrap(), IngestOutcome::Ingested);
    }

    #[test]
    fn classify_zero_chunks_is_empty_body_not_skipped() {
        // The trap: "no new atom ids" read as "already ingested". With zero
        // chunks there was never an atom, so this is a normaliser defect.
        assert_eq!(classify_ingest_response(0, 0, 0).unwrap(), IngestOutcome::EmptyBody);
    }

    #[test]
    fn classify_all_chunks_duplicate_is_skipped_existing() {
        assert_eq!(classify_ingest_response(0, 4, 4).unwrap(), IngestOutcome::SkippedExisting);
    }

    #[test]
    fn classify_chunks_created_but_nothing_kept_is_an_error() {
        // The observed local-verification shape: a failed content-blob
        // upload trimmed every atom, and the old code called it
        // "skipped_existing" — so a repo whose content never landed reported
        // `fully synced` at 100%.
        let err = classify_ingest_response(0, 5, 0).expect_err("must not be counted as progress");
        assert!(err.contains("dropped after chunking"), "message should name the cause: {err}");
    }

    #[test]
    fn classify_partial_duplicates_with_no_atoms_is_still_an_error() {
        assert!(classify_ingest_response(0, 5, 2).is_err());
    }

    // ── percent — a swept type reads 100% regardless of the estimate.

    #[test]
    fn percent_is_100_for_a_done_type_even_when_the_hint_overshot() {
        // `total_hint` is a page-count estimate; the issues listing's hint
        // even counts PRs. A finished type must not read 25% for ever.
        let tc = TypeCursor { done: true, seen: 3, total_hint: Some(12), ..Default::default() };
        assert_eq!(percent(&tc), Some(100));
    }

    #[test]
    fn percent_reports_the_ratio_while_still_running() {
        let tc = TypeCursor { done: false, seen: 3, total_hint: Some(12), ..Default::default() };
        assert_eq!(percent(&tc), Some(25));
    }

    #[test]
    fn percent_is_unknown_without_a_hint_while_running() {
        let tc = TypeCursor { done: false, seen: 3, total_hint: None, ..Default::default() };
        assert_eq!(percent(&tc), None);
    }

    #[test]
    fn type_kind_initial_urls() {
        assert_eq!(
            TypeKind::Pr.initial_url("acme", "repo", "main"),
            format!("{}/repos/acme/repo/pulls?state=all&sort=updated&direction=desc&per_page={LIST_PAGE_SIZE}", github_api_base())
        );
        assert_eq!(
            TypeKind::Commit.initial_url("acme", "repo", "main"),
            format!("{}/repos/acme/repo/commits?sha=main&per_page={LIST_PAGE_SIZE}", github_api_base())
        );
    }

    #[test]
    fn narrative_cursor_all_done() {
        let mut c = NarrativeCursor::default();
        assert!(!c.all_done());
        c.pr.done = true;
        c.issue.done = true;
        c.release.done = true;
        c.commit.done = true;
        assert!(c.all_done());
    }

    #[test]
    fn narrative_cursor_roundtrips_through_json() {
        let mut c = NarrativeCursor::default();
        c.pr.seen = 5;
        c.pr.total_hint = Some(20);
        c.pr.page_url = Some("https://api.github.com/x?page=2".to_string());
        let json = serde_json::to_value(&c).unwrap();
        let back = NarrativeCursor::from_json(Some(json));
        assert_eq!(back, c);
    }

    #[test]
    fn item_time_reads_each_types_own_sort_key() {
        let at = |s: &str| Some(s.parse::<DateTime<Utc>>().unwrap());
        let pr = serde_json::json!({"updated_at": "2026-10-01T00:00:00Z", "created_at": "2026-01-01T00:00:00Z"});
        assert_eq!(item_time(TypeKind::Pr, &pr), at("2026-10-01T00:00:00Z"));
        assert_eq!(item_time(TypeKind::Issue, &pr), at("2026-10-01T00:00:00Z"));
        let release = serde_json::json!({"published_at": "2026-09-01T00:00:00Z", "created_at": "2026-08-01T00:00:00Z"});
        assert_eq!(item_time(TypeKind::Release, &release), at("2026-09-01T00:00:00Z"));
        let draft = serde_json::json!({"published_at": null, "created_at": "2026-08-01T00:00:00Z"});
        assert_eq!(item_time(TypeKind::Release, &draft), at("2026-08-01T00:00:00Z"));
        let commit = serde_json::json!({"commit": {"committer": {"date": "2026-07-01T00:00:00Z"}}});
        assert_eq!(item_time(TypeKind::Commit, &commit), at("2026-07-01T00:00:00Z"));
        assert_eq!(item_time(TypeKind::Pr, &serde_json::json!({})), None);
    }

    #[test]
    fn a_window_floor_drops_older_items_and_never_an_undated_one() {
        let floor = Some("2026-10-10T00:00:00Z".parse::<DateTime<Utc>>().unwrap());
        let new = serde_json::json!({"updated_at": "2026-10-20T00:00:00Z"});
        let old = serde_json::json!({"updated_at": "2026-09-20T00:00:00Z"});
        let undated = serde_json::json!({"title": "x"});
        assert!(!before_floor(floor, TypeKind::Pr, &new));
        assert!(before_floor(floor, TypeKind::Pr, &old));
        assert!(!before_floor(floor, TypeKind::Pr, &undated), "a bound never drops what it cannot place");
        assert!(!before_floor(None, TypeKind::Pr, &old), "the automatic backfill stays unbounded");
    }

    #[test]
    fn the_floor_survives_a_roundtrip_and_is_absent_for_the_automatic_backfill() {
        let floor = "2026-10-10T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let windowed = NarrativeCursor::windowed(floor);
        let back = NarrativeCursor::from_json(Some(serde_json::to_value(&windowed).unwrap()));
        assert_eq!(back.window_floor, Some(floor));
        assert!(!back.all_done() && back.pr.page_url.is_none(), "a windowed cursor starts fresh");
        // Rows written before the floor existed, and every automatic cursor, carry no key.
        assert!(serde_json::to_value(NarrativeCursor::default()).unwrap().get("window_floor").is_none());
        assert_eq!(NarrativeCursor::from_json(Some(serde_json::json!({"pr": {"done": true}}))).window_floor, None);
    }

    #[test]
    fn narrative_cursor_from_json_none_is_default() {
        assert_eq!(NarrativeCursor::from_json(None), NarrativeCursor::default());
    }

    #[test]
    fn compute_progress_percentages() {
        let mut c = NarrativeCursor::default();
        c.pr.seen = 40;
        c.pr.total_hint = Some(100);
        c.issue.seen = 0;
        c.issue.total_hint = Some(50);
        c.release.seen = 10;
        c.release.total_hint = Some(10);
        // commit left with no total_hint -> unknown -> null
        let progress = compute_progress(&c);
        assert_eq!(progress.prs, Some(40));
        assert_eq!(progress.issues, Some(0));
        assert_eq!(progress.releases, Some(100));
        assert_eq!(progress.commits, None);
    }

    #[test]
    fn page_url_for_prefers_persisted_cursor() {
        let mut cursor = NarrativeCursor::default();
        cursor.pr.page_url = Some("https://api.github.com/repos/o/r/pulls?page=2".to_string());
        assert_eq!(
            page_url_for(TypeKind::Pr, &cursor, "o", "r", "main"),
            "https://api.github.com/repos/o/r/pulls?page=2"
        );
        let empty = NarrativeCursor::default();
        assert_eq!(
            page_url_for(TypeKind::Pr, &empty, "o", "r", "main"),
            TypeKind::Pr.initial_url("o", "r", "main")
        );
    }

    #[test]
    fn compute_progress_zero_total_hint_is_complete() {
        let mut c = NarrativeCursor::default();
        c.release.total_hint = Some(0);
        c.release.done = true;
        let progress = compute_progress(&c);
        assert_eq!(progress.releases, Some(100), "a repo with zero releases is trivially 100% complete");
    }
}
