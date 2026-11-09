//! GitHub per-event ingest dispatch (docs/connectors/github.md §3.7) — the
//! SAME handlers both `workers::github_event_ingest` (the webhook hand-off
//! consumer) and `workers::github_reconcile_sweep` (the periodic backstop)
//! call, per §3.7's "enqueuing only deltas through the same handlers the
//! webhook path uses."
//!
//! # Event -> atom mapping
//!
//! | Event | Action | Result |
//! |---|---|---|
//! | `pull_request` | any (`opened`/`edited`/`closed`/`reopened`/`synchronize`/...) | re-fetch + re-ingest the WHOLE PR atom, same `source_id` |
//! | `pull_request_review` / `pull_request_review_comment` | any | re-ingest the PARENT PR atom |
//! | `issue_comment` | any | re-ingest the parent — PR if `issue.pull_request` is present, else issue |
//! | `issues` | `deleted` | ignored, logged, atom NEVER deleted (§3.8/C9) |
//! | `issues` | anything else | re-ingest the issue atom |
//! | `release` | `deleted` | ignored, logged, atom NEVER deleted |
//! | `release` | `published`/`edited`/anything else | re-ingest the release atom |
//! | `push` | to the default branch | compare `before...after`: ingest the direct (non-PR) commits in that range as `github_commit` atoms (message, author, stats, changed-file NAMES — never file contents) — `forced: true` takes the IDENTICAL path (§8 "forced push re-fetches the compare range") |
//! | `push` | to a non-default branch | ignored |
//! | anything else | — | ignored |
//!
//! # No source code (founder decision 2026-10-27, issue #1516)
//!
//! The connector captures everything about a repository EXCEPT its code: no
//! event fetches a file blob, and nothing here writes a `github_code` or
//! `github_code_summary` atom. Until 2026-10-27 a push also re-fetched every
//! changed file as a `github_code` atom and a merged PR or push drove the
//! package-summary staleness gate (`github_code_summary`); both were
//! removed, not flagged off. Atoms of those types written before then are
//! kept as they are.
//!
//! One event can resolve to MULTIPLE watch rows (§3.4: "the same repo
//! selected by two different people into two different oceans is two watch
//! rows") — the fetch happens ONCE, the ingest happens once PER active
//! destination row.

use std::sync::Arc;

use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::github::{self, GithubApiError, github_api_base};
use crate::connectors::github_backfill::{self, RepoBinding, LIST_PAGE_SIZE};
use crate::connectors::github_narrative::{self, NormalizedAtom};
use crate::tenancy::TenantId;
use crate::AppState;

pub(crate) enum EventOutcome {
    /// No active watch row for this repo, an ignored action (C9 delete), a
    /// push to a non-default branch, or an unrecognised event type.
    NothingToDo,
    DidWork { ingested: usize },
}

/// [`handle_event_detailed`]'s result: the [`EventOutcome`] every caller
/// needs, plus what a default-branch push did to each watch row's
/// `head_tree_sha` cursor (the reconcile sweep reads this; the webhook worker
/// does not need it).
pub(crate) struct HandledEvent {
    pub(crate) outcome: EventOutcome,
    /// `Some` exactly when this was a push to the default branch with a real
    /// `before...after` range that the handler compared.
    pub(crate) push: Option<PushCursorReport>,
}

/// What one compared push range did to the `head_tree_sha` cursors.
pub(crate) struct PushCursorReport {
    /// Every direct commit in the range was fetched. A commit fetch that
    /// fails fails the whole event (the job retries), so a report that
    /// exists is always complete today; the field stays so the cursor rule
    /// below reads as stated.
    pub(crate) range_complete: bool,
    /// Watch rows whose cursor moved from `before` to `after`.
    pub(crate) advanced_rows: Vec<Uuid>,
}

/// The head cursor (`github_watched_repos.head_tree_sha`, which holds the
/// default branch's HEAD COMMIT sha, docs/connectors/github.md §9) moves
/// ONLY as a compare-and-swap from `from` to `to`, and only after the range
/// `from...to` was fully ingested into this row.
///
/// Why a compare-and-swap is the whole safety argument:
/// * The cursor only ever moves along a chain of ranges that were each
///   ingested, so no commit between the old and new cursor is skipped.
/// * An out-of-order or redelivered webhook whose `before` is not the stored
///   cursor matches no row, so the cursor never moves backwards and never
///   jumps over a range nobody ingested. (Its content is still ingested; the
///   sweep then re-compares from the unchanged cursor, which costs calls but
///   loses nothing.)
/// * A force-push carries `before` = the old head, so it advances exactly
///   like the sweep's own compare of the same range would.
/// * A `NULL` cursor (backfill not done yet) is never matched.
///
/// Ingest commits per atom before this runs, so a crash between the two
/// leaves the cursor behind the content, never ahead of it: the next sweep
/// re-compares the range and the re-ingest is idempotent.
pub(crate) async fn advance_head_cursor(pool: &PgPool, account_id: Uuid, repo_row_id: Uuid, from: &str, to: &str) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_event_ingest advance_head_cursor scoped tx: {e}")))?;
    let moved = sqlx::query(
        "UPDATE github_watched_repos SET head_tree_sha = $1, updated_at = now() \
         WHERE id = $2 AND account_id = $3 AND head_tree_sha = $4",
    )
    .bind(to)
    .bind(repo_row_id)
    .bind(account_id)
    .bind(from)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(moved == 1)
}

/// The atoms one compared push range produced, and whether the range was
/// covered in full (see [`PushCursorReport::range_complete`]).
pub(crate) struct RangeAtoms {
    pub(crate) atoms: Vec<NormalizedAtom>,
    pub(crate) complete: bool,
}

/// An active watch row for the event's repository. Whether its binding is
/// writable is the grant's call (`RepoBinding::bound`), made only when the
/// event produced atoms.
struct WatchRowRef {
    id: Uuid,
    owner: String,
    name: String,
    default_branch: String,
    binding: RepoBinding,
}

async fn load_active_rows_for_repo(
    pool: &PgPool,
    account_id: Uuid,
    installation_id: i64,
    repo_id: i64,
) -> Result<Vec<WatchRowRef>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_event_ingest load_active_rows scoped tx: {e}")))?;
    #[allow(clippy::type_complexity)]
    let rows: Vec<(Uuid, String, String, String, Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
        "SELECT id, owner, name, default_branch, graph_id, owner_user_id \
         FROM github_watched_repos \
         WHERE account_id = $1 AND installation_id = $2 AND repo_id = $3 AND active = true",
    )
    .bind(account_id)
    .bind(installation_id)
    .bind(repo_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();

    Ok(rows
        .into_iter()
        .map(|(id, owner, name, default_branch, graph_id, owner_user_id)| WatchRowRef {
            id,
            owner,
            name,
            default_branch,
            binding: RepoBinding { row_id: id, repo_id, active: true, graph_id, owner_user_id },
        })
        .collect())
}

/// Write the event's atoms under each watch row's grant. Returns how many
/// writes landed and the rows that did not take every atom (their head
/// cursor is held for the reconcile sweep).
///
/// The grant is resolved once per row, and only for a row with something to
/// write: an event that produced no atoms (a push of PR merges and their
/// branch commits, say) touches no grant, so a transient lookup failure
/// cannot fail a row, hold its cursor or pause it over a write that was
/// never going to happen.
async fn write_event_atoms(
    state: &Arc<AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    rows: &[WatchRowRef],
    atoms: &[NormalizedAtom],
    event: &str,
) -> (usize, Vec<Uuid>) {
    let mut ingested = 0usize;
    let mut failed_rows: Vec<Uuid> = Vec::new();
    if atoms.is_empty() {
        return (ingested, failed_rows);
    }
    for row in rows {
        let grant = match github_backfill::resolve_repo_grant(state, tenant, pool, account_id, &row.binding).await {
            Ok(grant) => grant,
            Err(e) => {
                failed_rows.push(row.id);
                tracing::warn!(error = %e, repo_row_id = %row.id, event, "github_event_ingest: no grant for one destination row, continuing with the rest");
                continue;
            }
        };
        for atom in atoms {
            match github_backfill::ingest_one(state, tenant, pool, account_id, &grant, atom.clone()).await {
                Ok(_) => ingested += 1,
                Err(e) => {
                    if !failed_rows.contains(&row.id) {
                        failed_rows.push(row.id);
                    }
                    tracing::warn!(error = %e, repo_row_id = %row.id, event, "github_event_ingest: ingest failed for one destination row, continuing with the rest")
                }
            }
        }
    }
    (ingested, failed_rows)
}

/// The default-branch `refs/heads/{branch}` this push's `ref` must match to
/// be processed at all — a pure predicate so the branch-filter decision is
/// unit-testable without a database or HTTP mock.
pub(crate) fn push_targets_default_branch(git_ref: &str, default_branch: &str) -> bool {
    git_ref == format!("refs/heads/{default_branch}")
}

/// Whether a push's `before`/`after` shas name an actual range to compare —
/// `false` for a branch CREATE (`before` all-zero) or DELETE (`after`
/// all-zero), where there is nothing to diff.
pub(crate) fn push_has_comparable_range(before: &str, after: &str) -> bool {
    !before.is_empty() && !after.is_empty() && !before.chars().all(|c| c == '0') && !after.chars().all(|c| c == '0')
}

/// The compare-range URL a push (forced or not — §8 "a forced push re-fetches
/// the compare range" is exactly this: `forced` carries no special case,
/// the SAME URL is built either way) dispatches to.
pub(crate) fn push_compare_url(owner: &str, repo: &str, before: &str, after: &str) -> String {
    format!("{}/repos/{owner}/{repo}/compare/{before}...{after}", github_api_base())
}

async fn find_prev_release_tag(http: &reqwest::Client, token: &str, owner: &str, repo: &str, tag: &str) -> Option<String> {
    let url = format!("{}/repos/{owner}/{repo}/releases?per_page={LIST_PAGE_SIZE}", github_api_base());
    let (items, _, _) = github::get_page(http, token, &url).await.ok()?;
    let idx = items.iter().position(|r| r.get("tag_name").and_then(Value::as_str) == Some(tag))?;
    items.get(idx + 1)?.get("tag_name")?.as_str().map(str::to_string)
}

/// One GitHub compare (`GET .../compare/{before}...{after}`) gives a push's
/// delta: the direct (non-PR) commits in the range, each fetched and
/// normalised as a `github_commit` atom. A squash merge (`... (#N)`), a PR
/// merge commit (`Merge pull request #N from ...`) and the commits that
/// merge brought in are PR work and make no commit atom; a range of nothing
/// but PR merges yields no atom and still advances the head cursor. The compare's `files` list is
/// deliberately NOT read: the connector does not capture source code
/// (2026-10-27), so no blob is ever fetched here.
async fn compare_range_atoms(
    http: &reqwest::Client,
    token: &str,
    owner: &str,
    repo: &str,
    repo_id: i64,
    before: &str,
    after: &str,
) -> Result<RangeAtoms, GithubApiError> {
    if !push_has_comparable_range(before, after) {
        return Ok(RangeAtoms { atoms: Vec::new(), complete: false });
    }
    let url = push_compare_url(owner, repo, before, after);
    let cmp = github::get_one(http, token, &url).await?;

    let mut atoms = Vec::new();
    let commits = cmp.get("commits").and_then(Value::as_array).cloned().unwrap_or_default();
    // Commits a PR merge commit in this same range brought in are the PR's
    // own commits, covered by the PR path. Skipped from the compare's
    // parent links, without asking the association endpoint, which has
    // often not indexed the PR yet when the push webhook arrives.
    let merged_in = github_narrative::commits_merged_in_by_prs(&commits, after);
    for c in &commits {
        if c.get("sha").and_then(Value::as_str).is_some_and(|sha| merged_in.contains(sha)) {
            continue;
        }
        if let Some(atom) = github_backfill::fetch_commit_if_direct(http, token, owner, repo, repo_id, c).await? {
            atoms.push(atom);
        }
    }

    // Over GitHub's compare listing cap the commit list stops short. That is
    // NOT a failure this handler can retry its way out of: the sweep's
    // compare of the same (or a longer) range is capped identically, so
    // holding the cursor would only make every sweep re-pay a range it can
    // never finish. Logged so an operator can see it.
    let total_commits = cmp.get("total_commits").and_then(Value::as_u64).unwrap_or(commits.len() as u64);
    if total_commits > commits.len() as u64 {
        tracing::warn!(
            owner,
            repo,
            before,
            after,
            total_commits,
            listed_commits = commits.len(),
            "github_event_ingest: compare range exceeds GitHub's listing cap, the range is ingested only as far as GitHub lists it"
        );
    }

    Ok(RangeAtoms { atoms, complete: true })
}

/// Resolve one webhook (or reconcile-sweep-discovered) event into its
/// normalised atom(s), then ingest into EVERY active watch row for this
/// repo. `payload` is the raw, already-signature-verified webhook body (or,
/// for the reconcile sweep, a synthetic payload it built from a live listing
/// — see `workers::github_reconcile_sweep`).
pub(crate) async fn handle_event(
    tenant: &TenantId,
    pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    installation_id: i64,
    event: &str,
    payload: &Value,
) -> Result<EventOutcome, String> {
    handle_event_detailed(tenant, pool, state, account_id, installation_id, event, payload).await.map(|h| h.outcome)
}

/// [`handle_event`], also reporting what a default-branch push did to each
/// watch row's head cursor ([`advance_head_cursor`]). A push range that was
/// fully ingested into a row advances that row's cursor from `before` to
/// `after`, so the reconcile sweep does not re-compare and re-fetch a range
/// the webhook already covered.
pub(crate) async fn handle_event_detailed(
    tenant: &TenantId,
    pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    installation_id: i64,
    event: &str,
    payload: &Value,
) -> Result<HandledEvent, String> {
    let nothing = || HandledEvent { outcome: EventOutcome::NothingToDo, push: None };
    let Some(repo_id) = payload.get("repository").and_then(|r| r.get("id")).and_then(Value::as_i64) else {
        tracing::debug!(installation_id, event, "github_event_ingest: payload carries no repository.id — nothing to route");
        return Ok(nothing());
    };

    let mut rows = load_active_rows_for_repo(pool, account_id, installation_id, repo_id).await.map_err(|e| e.to_string())?;
    // The pause gate: a watch row whose member paused or turned off GitHub is
    // not ingested into, whether the event came from the webhook or the
    // reconcile sweep (both end here). Dropped before the installation token or
    // any fetch, and its head cursor stays, so the sweep re-compares after a
    // resume. Event deliveries are not replayed.
    if let Some(control) = state.control_db() {
        let mut live = Vec::with_capacity(rows.len());
        for row in rows {
            let Some((_, owner_user_id)) = row.binding.bound() else { continue; };
            if crate::connectors::catalog::pause::is_paused(
                control, account_id, owner_user_id, crate::connectors::catalog::pause::GITHUB_REPOS,
            ).await {
                tracing::info!(repo_row_id = %row.id, "github_event_ingest: source is paused or off -- skipping this watch row");
            } else {
                live.push(row);
            }
        }
        rows = live;
    }
    if rows.is_empty() {
        tracing::debug!(installation_id, repo_id, event, "github_event_ingest: no active watch row for this repo — nothing to do");
        return Ok(nothing());
    }

    let token = github::installation_token(&state.http_client, installation_id).await.map_err(|e| format!("installation token: {e}"))?;
    let http = &state.http_client;
    let owner = rows[0].owner.clone();
    let name = rows[0].name.clone();
    let default_branch = rows[0].default_branch.clone();

    let (atoms, complete): (Vec<NormalizedAtom>, bool) = match fetch_event_atoms(http, &token, &owner, &name, &default_branch, repo_id, event, payload).await {
        Ok(fetched) => fetched,
        Err(EventFetchError::NothingToDo) => return Ok(nothing()),
        Err(EventFetchError::Api(e)) => return Err(e.to_string()),
    };

    let (ingested, failed_rows) = write_event_atoms(state, tenant, pool, account_id, &rows, &atoms, event).await;

    // Head cursor: only for a compared default-branch push (the branch
    // filter already passed inside `fetch_event_atoms`, or it would have
    // returned `NothingToDo`), only when the whole range was fetched, and
    // only for a row every atom landed in.
    let push = match push_range(event, payload) {
        Some((before, after)) => {
            let mut advanced_rows = Vec::new();
            for row in &rows {
                if !complete || failed_rows.contains(&row.id) {
                    tracing::info!(repo_row_id = %row.id, before, after, range_complete = complete, head_cursor = "held", "github_event_ingest: push range not fully ingested, head cursor stays for the reconcile sweep");
                    continue;
                }
                match advance_head_cursor(pool, account_id, row.id, before, after).await {
                    Ok(true) => {
                        tracing::info!(repo_row_id = %row.id, before, after, head_cursor = "advanced", "github_event_ingest: push range ingested, head cursor advanced");
                        advanced_rows.push(row.id);
                    }
                    Ok(false) => tracing::info!(repo_row_id = %row.id, before, after, head_cursor = "unchanged", "github_event_ingest: push range ingested, but the stored head cursor is not this push's `before` (out of order, redelivered, or not yet seeded) — left for the reconcile sweep"),
                    Err(e) => tracing::warn!(error = %e, repo_row_id = %row.id, before, after, "github_event_ingest: failed to advance the head cursor, the reconcile sweep will re-compare this range"),
                }
            }
            Some(PushCursorReport { range_complete: complete, advanced_rows })
        }
        None => None,
    };

    let outcome = if ingested == 0 { EventOutcome::NothingToDo } else { EventOutcome::DidWork { ingested } };
    Ok(HandledEvent { outcome, push })
}

/// `(before, after)` for a push event with a real range to compare. Only
/// meaningful after `fetch_event_atoms` accepted the push (default branch).
fn push_range<'a>(event: &str, payload: &'a Value) -> Option<(&'a str, &'a str)> {
    if event != "push" {
        return None;
    }
    let before = payload.get("before").and_then(Value::as_str)?;
    let after = payload.get("after").and_then(Value::as_str)?;
    push_has_comparable_range(before, after).then_some((before, after))
}

enum EventFetchError {
    NothingToDo,
    Api(GithubApiError),
}

impl From<GithubApiError> for EventFetchError {
    fn from(e: GithubApiError) -> Self {
        EventFetchError::Api(e)
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_event_atoms(
    http: &reqwest::Client,
    token: &str,
    owner: &str,
    name: &str,
    default_branch: &str,
    repo_id: i64,
    event: &str,
    payload: &Value,
) -> Result<(Vec<NormalizedAtom>, bool), EventFetchError> {
    let atoms = match event {
        "pull_request" | "pull_request_review" | "pull_request_review_comment" => {
            let number = payload
                .get("pull_request")
                .and_then(|p| p.get("number"))
                .and_then(Value::as_i64)
                .ok_or(EventFetchError::NothingToDo)?;
            match github_narrative::fetch_pr_atom(http, token, owner, name, repo_id, number).await {
                Ok(atom) => Ok(vec![atom]),
                Err(GithubApiError::Http { status: 404, .. }) => {
                    tracing::info!(owner, name, number, "github_event_ingest: PR vanished (404) between event and fetch — skipping");
                    Ok(Vec::new())
                }
                Err(e) => Err(e.into()),
            }
        }
        "issue_comment" => {
            let issue = payload.get("issue").ok_or(EventFetchError::NothingToDo)?;
            let number = issue.get("number").and_then(Value::as_i64).ok_or(EventFetchError::NothingToDo)?;
            if github_narrative::is_actually_a_pull_request(issue) {
                match github_narrative::fetch_pr_atom(http, token, owner, name, repo_id, number).await {
                    Ok(atom) => Ok(vec![atom]),
                    Err(GithubApiError::Http { status: 404, .. }) => Ok(Vec::new()),
                    Err(e) => Err(e.into()),
                }
            } else {
                match github_narrative::fetch_issue_atom(http, token, owner, name, repo_id, issue).await {
                    Ok(atom) => Ok(vec![atom]),
                    Err(GithubApiError::Http { status: 404, .. }) => Ok(Vec::new()),
                    Err(e) => Err(e.into()),
                }
            }
        }
        "issues" => {
            let action = payload.get("action").and_then(Value::as_str).unwrap_or("");
            if action == "deleted" {
                tracing::info!(owner, name, "github_event_ingest: issues.deleted ignored — atom never deleted (§3.8/C9)");
                return Err(EventFetchError::NothingToDo);
            }
            let issue = payload.get("issue").ok_or(EventFetchError::NothingToDo)?;
            match github_narrative::fetch_issue_atom(http, token, owner, name, repo_id, issue).await {
                Ok(atom) => Ok(vec![atom]),
                Err(GithubApiError::Http { status: 404, .. }) => Ok(Vec::new()),
                Err(e) => Err(e.into()),
            }
        }
        "release" => {
            let action = payload.get("action").and_then(Value::as_str).unwrap_or("");
            if action == "deleted" {
                tracing::info!(owner, name, "github_event_ingest: release.deleted ignored — atom never deleted (§3.8/C9)");
                return Err(EventFetchError::NothingToDo);
            }
            let release_json = payload.get("release").ok_or(EventFetchError::NothingToDo)?;
            let tag = release_json.get("tag_name").and_then(Value::as_str).unwrap_or_default();
            let prev_tag = find_prev_release_tag(http, token, owner, name, tag).await;
            Ok(vec![github_narrative::normalize_release(repo_id, owner, name, release_json, prev_tag.as_deref())])
        }
        "push" => {
            let git_ref = payload.get("ref").and_then(Value::as_str).unwrap_or_default();
            if !push_targets_default_branch(git_ref, default_branch) {
                tracing::debug!(git_ref, default_branch, "github_event_ingest: push to a non-default branch — ignored");
                return Err(EventFetchError::NothingToDo);
            }
            let before = payload.get("before").and_then(Value::as_str).unwrap_or_default();
            let after = payload.get("after").and_then(Value::as_str).unwrap_or_default();
            let range = compare_range_atoms(http, token, owner, name, repo_id, before, after).await?;
            return Ok((range.atoms, range.complete));
        }
        other => {
            tracing::debug!(event = other, "github_event_ingest: unhandled event type — nothing to do");
            Err(EventFetchError::NothingToDo)
        }
    }?;
    Ok((atoms, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn push_targets_default_branch_matches() {
        assert!(push_targets_default_branch("refs/heads/main", "main"));
        assert!(!push_targets_default_branch("refs/heads/feature-x", "main"));
        assert!(!push_targets_default_branch("refs/tags/v1.0.0", "main"));
    }

    #[test]
    fn push_has_comparable_range_rejects_all_zero_shas() {
        let zero = "0000000000000000000000000000000000000000";
        assert!(!push_has_comparable_range(zero, "abc123"));
        assert!(!push_has_comparable_range("abc123", zero));
        assert!(push_has_comparable_range("abc123", "def456"));
    }

    #[test]
    fn push_compare_url_is_before_dot_dot_dot_after() {
        let url = push_compare_url("acme", "repo", "aaa", "bbb");
        assert_eq!(url, format!("{}/repos/acme/repo/compare/aaa...bbb", github_api_base()));
    }

    /// §8 "a forced push re-fetches the compare range" — `forced: true`
    /// carries no special case in the dispatch: the SAME before/after pair
    /// produces the SAME compare URL regardless of the `forced` flag. This
    /// is the unit test the brief asks for on "the dispatch deciding the
    /// compare call".
    #[test]
    fn forced_push_uses_the_identical_compare_range_as_an_ordinary_push() {
        let ordinary = json!({
            "ref": "refs/heads/main", "before": "aaa111", "after": "bbb222", "forced": false,
            "repository": {"id": 1, "default_branch": "main"}
        });
        let forced = json!({
            "ref": "refs/heads/main", "before": "aaa111", "after": "bbb222", "forced": true,
            "repository": {"id": 1, "default_branch": "main"}
        });
        let url_ordinary = push_compare_url(
            "acme", "repo",
            ordinary["before"].as_str().unwrap(),
            ordinary["after"].as_str().unwrap(),
        );
        let url_forced = push_compare_url(
            "acme", "repo",
            forced["before"].as_str().unwrap(),
            forced["after"].as_str().unwrap(),
        );
        assert_eq!(url_ordinary, url_forced, "forced and ordinary pushes with the same before/after must compare the identical range");
    }

    /// Mock GitHub for `compare_range_atoms`: a compare that lists one
    /// direct commit and one changed file, the commit's association (none)
    /// and detail, and a COUNTED blob route the handler must never call.
    async fn spawn_mock_compare(blob_hits: Arc<std::sync::atomic::AtomicUsize>) -> (String, tokio::task::JoinHandle<()>) {
        use axum::extract::{Path, State};
        use axum::routing::get;
        use axum::{Json, Router};

        let app = Router::new()
            .route(
                "/repos/{owner}/{repo}/compare/{range}",
                get(|| async {
                    Json(json!({
                        "total_commits": 2,
                        "commits": [
                            // Squash-merge suffix: covered by the PR path, skipped.
                            {"sha": "midsha", "commit": {"message": "Earlier work (#11)"}},
                            // A direct push: becomes a `github_commit` atom.
                            {"sha": "headsha", "commit": {"message": "Hotfix the thing"}}
                        ],
                        "files": [{"filename": "src/lib.rs", "status": "modified", "sha": "blobsha1"}]
                    }))
                }),
            )
            .route("/repos/{owner}/{repo}/commits/{sha}/pulls", get(|| async { Json(json!([])) }))
            .route(
                "/repos/{owner}/{repo}/commits/{sha}",
                get(|Path((_o, _r, sha)): Path<(String, String, String)>| async move {
                    Json(json!({
                        "sha": sha,
                        "commit": {"message": "Hotfix the thing", "author": {"name": "Dev", "date": "2026-10-18T17:10:00Z"}},
                        "stats": {"additions": 1, "deletions": 0},
                        "files": [{"filename": "src/lib.rs", "patch": "+pub fn answer() -> u32 { 42 }"}]
                    }))
                }),
            )
            .route(
                "/repos/{owner}/{repo}/git/blobs/{sha}",
                get(|State(hits): State<Arc<std::sync::atomic::AtomicUsize>>| async move {
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Json(json!({"content": "", "encoding": "base64"}))
                }),
            )
            .with_state(blob_hits);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock github");
        let addr = listener.local_addr().expect("mock github addr");
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    /// The founder decision of 2026-10-27 (issue #1516): a push keeps its
    /// commit metadata but never its code. The compare lists a changed
    /// file; the handler returns exactly the direct commit, as a
    /// `github_commit` atom carrying the file NAME and not its contents, and
    /// never fetches the blob.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn push_range_yields_commit_atoms_and_never_code() {
        let _lock = github::test_env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var("GITHUB_API_BASE").ok();
        let blob_hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (base, mock) = spawn_mock_compare(blob_hits.clone()).await;
        unsafe { std::env::set_var("GITHUB_API_BASE", &base) };
        let http = reqwest::Client::new();

        let range = compare_range_atoms(&http, "t", "acme", "repo", 7, "beforesha", "headsha").await;

        mock.abort();
        match prev {
            Some(v) => unsafe { std::env::set_var("GITHUB_API_BASE", v) },
            None => unsafe { std::env::remove_var("GITHUB_API_BASE") },
        }

        let range = range.expect("compare succeeds");
        assert!(range.complete);
        let types: Vec<&str> = range.atoms.iter().map(|a| a.source_type).collect();
        assert_eq!(types, vec!["github_commit"], "only the direct commit, no code atom");
        let commit = &range.atoms[0];
        assert_eq!(commit.source_id, "7:commit:headsha");
        assert!(commit.code_path.is_none());
        assert!(commit.content.contains("src/lib.rs"), "the changed file's NAME is commit metadata and stays");
        assert!(!commit.content.contains("pub fn answer"), "file contents and patches never enter an atom");
        assert_eq!(blob_hits.load(std::sync::atomic::Ordering::SeqCst), 0, "no blob is ever fetched");
    }

    /// Mock GitHub whose association endpoint has NOT caught up (it answers
    /// `[]` for every sha, as it did for the 2026-09-30..10-04 prod pushes),
    /// over a range holding one direct commit, a PR's branch commit and the
    /// PR's merge commit. Counts association calls.
    async fn spawn_mock_lagging_association(assoc_hits: Arc<std::sync::atomic::AtomicUsize>) -> (String, tokio::task::JoinHandle<()>) {
        use axum::extract::{Path, State};
        use axum::routing::get;
        use axum::{Json, Router};

        let app = Router::new()
            .route(
                "/repos/{owner}/{repo}/compare/{range}",
                get(|| async {
                    Json(json!({
                        "total_commits": 3,
                        "commits": [
                            {"sha": "directsha", "commit": {"message": "Hotfix on main"}, "parents": [{"sha": "beforesha"}]},
                            {"sha": "branchsha", "commit": {"message": "Branch work"}, "parents": [{"sha": "directsha"}]},
                            {"sha": "mergesha", "commit": {"message": "Merge pull request #1116 from CoralSwarm/feat/x"}, "parents": [{"sha": "directsha"}, {"sha": "branchsha"}]}
                        ]
                    }))
                }),
            )
            .route(
                "/repos/{owner}/{repo}/commits/{sha}/pulls",
                get(|State(hits): State<Arc<std::sync::atomic::AtomicUsize>>| async move {
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Json(json!([]))
                }),
            )
            .route(
                "/repos/{owner}/{repo}/commits/{sha}",
                get(|Path((_o, _r, sha)): Path<(String, String, String)>| async move {
                    Json(json!({
                        "sha": sha,
                        "commit": {"message": "Hotfix on main", "author": {"name": "Dev", "date": "2026-10-28T03:10:00Z"}},
                        "stats": {"additions": 1, "deletions": 0},
                        "files": [{"filename": "src/lib.rs"}]
                    }))
                }),
            )
            .with_state(assoc_hits);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock github");
        let addr = listener.local_addr().expect("mock github addr");
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    /// Regression: every `github_commit` atom the webhook path wrote in prod
    /// (2026-09-30..10-04, 20 atoms) was a PR merge commit or PR branch
    /// commit, because the association endpoint had not indexed the merged
    /// PR when the push webhook ran. The PR's merge and branch commits are
    /// now skipped from the compare's own parent links, the direct commit
    /// still lands, and only the direct commit asks the association endpoint.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pr_merge_and_its_branch_commits_skip_without_the_association_endpoint() {
        let _lock = github::test_env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var("GITHUB_API_BASE").ok();
        let assoc_hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (base, mock) = spawn_mock_lagging_association(assoc_hits.clone()).await;
        unsafe { std::env::set_var("GITHUB_API_BASE", &base) };
        let http = reqwest::Client::new();

        let range = compare_range_atoms(&http, "t", "acme", "repo", 7, "beforesha", "mergesha").await;

        mock.abort();
        match prev {
            Some(v) => unsafe { std::env::set_var("GITHUB_API_BASE", v) },
            None => unsafe { std::env::remove_var("GITHUB_API_BASE") },
        }

        let range = range.expect("compare succeeds");
        assert!(range.complete, "a range of PR work is complete, so the head cursor advances");
        let ids: Vec<&str> = range.atoms.iter().map(|a| a.source_id.as_str()).collect();
        assert_eq!(ids, vec!["7:commit:directsha"], "only the direct commit becomes a github_commit atom");
        assert_eq!(assoc_hits.load(std::sync::atomic::Ordering::SeqCst), 1, "only the direct commit reaches the association endpoint");
    }

    #[test]
    fn push_range_only_for_a_comparable_push() {
        let zero = "0000000000000000000000000000000000000000";
        assert_eq!(push_range("push", &json!({"before": "a", "after": "b"})), Some(("a", "b")));
        assert_eq!(push_range("push", &json!({"before": zero, "after": "b"})), None, "branch create");
        assert_eq!(push_range("push", &json!({"before": "a", "after": zero})), None, "branch delete");
        assert_eq!(push_range("pull_request", &json!({"before": "a", "after": "b"})), None);
    }

    /// [`advance_head_cursor`]'s SQL under BOTH roles a tenant DB is reached
    /// as: the table owner (prod tenant DSNs) and `coralswarm_tenant_app`
    /// (RLS-enforced, `member_ocean_isolation`). The compare-and-swap must
    /// move a matching cursor, refuse a stale `from`, and never touch a NULL
    /// cursor, identically under each.
    #[tokio::test]
    async fn advance_head_cursor_is_a_compare_and_swap_under_both_roles_pg() {
        use sqlx::postgres::PgPoolOptions;
        use sqlx::{Connection, Executor, PgConnection};
        let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
            assert!(std::env::var("REQUIRE_PG_SUITE").is_err(), "REQUIRE_PG_SUITE is set but ORGT_TEST_BASE_URL is not");
            eprintln!("skipping advance_head_cursor_is_a_compare_and_swap_under_both_roles_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let with_db = |db: &str| {
            let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
            let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
            format!("{prefix}/{db}{params}")
        };
        let db = "cs_gh_head_cursor_roles";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin = PgConnection::connect(&base).await.expect("admin");
        admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        let owner = PgPoolOptions::new().max_connections(2).connect(&with_db(db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&owner).await.expect("tenant migrations");
        let app = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    conn.execute("SET ROLE coralswarm_tenant_app").await?;
                    Ok(())
                })
            })
            .connect(&with_db(db))
            .await
            .unwrap();

        let (account, user, graph) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        sqlx::query("INSERT INTO graphs(id,account_id,owner_user_id,kind,name) VALUES($1,$2,$3,'member','Private')")
            .bind(graph).bind(account).bind(user)
            .execute(&owner).await.unwrap();
        let seed = |head: Option<&'static str>| {
            let owner = owner.clone();
            async move {
                let id = Uuid::new_v4();
                sqlx::query(
                    "INSERT INTO github_watched_repos (id, account_id, installation_id, repo_id, owner, name, default_branch, active, graph_id, owner_user_id, destination_kind, head_tree_sha) \
                     VALUES ($1, $2, 1, $3, 'acme', 'repo', 'main', true, $4, $5, 'personal', $6)",
                )
                .bind(id).bind(account).bind(rand_repo_id()).bind(graph).bind(user).bind(head)
                .execute(&owner).await.unwrap();
                id
            }
        };
        fn rand_repo_id() -> i64 {
            (Uuid::new_v4().as_u128() % 1_000_000_000) as i64
        }
        let head = |id: Uuid| {
            let owner = owner.clone();
            async move {
                sqlx::query_scalar::<_, Option<String>>("SELECT head_tree_sha FROM github_watched_repos WHERE id = $1").bind(id).fetch_one(&owner).await.unwrap()
            }
        };

        for (label, pool) in [("owner", &owner), ("coralswarm_tenant_app", &app)] {
            let row = seed(Some("c0")).await;
            assert!(advance_head_cursor(pool, account, row, "c0", "c1").await.unwrap(), "{label}: matching cursor moves");
            assert_eq!(head(row).await.as_deref(), Some("c1"));
            assert!(!advance_head_cursor(pool, account, row, "c0", "c9").await.unwrap(), "{label}: a stale `from` is refused");
            assert_eq!(head(row).await.as_deref(), Some("c1"), "{label}: never backwards");
            let wrong_account = Uuid::new_v4();
            assert!(!advance_head_cursor(pool, wrong_account, row, "c1", "c2").await.unwrap(), "{label}: another account's row is untouched");
            let unseeded = seed(None).await;
            assert!(!advance_head_cursor(pool, account, unseeded, "c0", "c1").await.unwrap(), "{label}: a NULL cursor is not advanced");
            assert_eq!(head(unseeded).await, None);
        }

        app.close().await;
        owner.close().await;
        admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// An event that produced no atoms resolves no grant: a transient grant
    /// lookup failure cannot fail the row, so its head cursor is not held
    /// over a write that was never going to happen. The same lookup failure
    /// does fail the row once there is an atom to write.
    ///
    /// The transient failure here is an org tenant with no control pool
    /// (`destination_reachable` refuses to assume membership). Nothing
    /// reaches the database, so the pool is never connected.
    #[tokio::test]
    async fn an_event_with_no_atoms_resolves_no_grant_and_fails_no_row() {
        let pool = sqlx::postgres::PgPoolOptions::new().connect_lazy("postgres://nobody@127.0.0.1:1/never").unwrap();
        let state = crate::test_app_state();
        assert!(state.control_db().is_none(), "the lookup must fail on the missing control pool");
        let tenant = TenantId::Org("org_no_control".to_string());
        let (account, row_id, graph, chooser) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let rows = vec![WatchRowRef {
            id: row_id,
            owner: "acme".into(),
            name: "repo".into(),
            default_branch: "main".into(),
            binding: RepoBinding { row_id, repo_id: 7, active: true, graph_id: Some(graph), owner_user_id: Some(chooser) },
        }];

        let (ingested, failed) = write_event_atoms(&state, &tenant, &pool, account, &rows, &[], "push").await;
        assert_eq!((ingested, failed), (0, Vec::new()), "no atoms: no grant lookup, no failed row");

        let atom = NormalizedAtom {
            source_type: "github_commit",
            source_id: "7:commit:headsha".into(),
            title: "Hotfix".into(),
            content: "Hotfix the thing".into(),
            occurred_at: chrono::Utc::now(),
            author_id: None,
            author_name: None,
            participants: Vec::new(),
            code_path: None,
        };
        let (ingested, failed) = write_event_atoms(&state, &tenant, &pool, account, &rows, &[atom], "push").await;
        assert_eq!((ingested, failed), (0, vec![row_id]), "one atom: the failed lookup fails the row");
    }
}
