//! GitHub reconcile sweep (docs/connectors/github.md §3.7) — the periodic
//! backstop for whatever the webhook path misses: a dropped delivery, a
//! deploy window, a webhook secret rotation gap, or simply GitHub never
//! sending one (some `push`es to a protected branch, for instance). Copies
//! `workers::google_chat_sweep`'s harness shape: tenant loop, `tick_consumer`
//! enumerates accounts (bounded, oldest-synced-first), `tick_org` sweeps the
//! one account its tenant DB belongs to.
//!
//! Every delta this sweep finds is dispatched through `connectors::
//! github_event_ingest::handle_event` — the SAME handler the webhook path
//! uses (§3.7 "same handlers"), via a synthetic webhook-shaped payload this
//! module builds from a live listing rather than a real delivery.
//!
//! # What one pass does, per active + fully-backfilled watch row
//!
//! * **PRs** — `pulls?state=all&sort=updated&direction=desc`, paged, STOP at
//!   the first item whose `updated_at` is not newer than the stored
//!   `narrative_cursor.pr.since` (GitHub's `pulls` listing has no native
//!   `since` filter, unlike `issues` — this is why the walk-and-stop shape
//!   is needed here specifically, per §3.7's own instruction).
//! * **Issues** — `issues?...&since={since}` (native filter), filtering out
//!   items that are actually PRs.
//! * **Releases** — `releases`, first page, filtering to `published_at >
//!   since` (no native filter either; releases arrive newest-first already).
//! * **Default-branch commits** — compares the stored `head_tree_sha` (see docs/connectors/
//!   github.md §9: this column holds the default branch's HEAD COMMIT sha,
//!   not a literal git tree-object sha — that is what a compare-range
//!   operation actually needs) against the LIVE `GET .../branches/{default}`
//!   commit sha; when different, dispatches a synthetic `push` event over
//!   that exact range (the same handler a real `push` webhook drives).
//!   That handler moves `head_tree_sha` from `before` to `after` only when
//!   the range was fully ingested
//!   (`connectors::github_event_ingest::advance_head_cursor`). A real `push`
//!   webhook advances the same cursor the same way, so a range the webhook
//!   already ingested costs this sweep one `branches` call and no compare or
//!   blob fetch. Anything the webhook missed (a dropped delivery, a failed
//!   ingest, an out-of-order delivery) leaves the cursor behind the live
//!   head, and the sweep compares from there. The compare yields direct
//!   (non-PR) commits only: the connector captures no source code
//!   (2026-10-27, issue #1516), so no blob is fetched and no
//!   `github_code`/`github_code_summary` atom is written. The package-
//!   summary force-after leg this sweep used to run was removed with it.
//!
//! Each type's `since` watermark only advances past items this pass actually
//! examined — a partial page failure just means the NEXT sweep re-examines
//! the same window, which is free (idempotent via `atoms`' `ON CONFLICT`).

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::github::{self, github_api_base};
use crate::connectors::github_backfill::{self, NarrativeCursor, LIST_PAGE_SIZE};
use crate::connectors::github_event_ingest::{handle_event, handle_event_detailed, EventOutcome};
use crate::connectors::github_narrative;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// Default sweep cadence (§3.7: "every few hours"), overridable per the
/// brief's `GITHUB_RECONCILE_INTERVAL_SECS`. Read once, at worker
/// registration (`lib.rs`) — an interval constant, not a secret, so this
/// does not need the "read at call time" convention secrets get.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(3 * 60 * 60);

pub fn poll_interval() -> Duration {
    std::env::var("GITHUB_RECONCILE_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_POLL_INTERVAL)
}

/// Bound on GitHub `pulls`-listing pages walked per repo per pass, for the
/// "stop at the first older item" PR sweep — a repo with an enormous
/// same-tick update burst just gets the rest on the NEXT sweep (self-heals,
/// per §3.7's own "a missed push self-heals at the next sweep" precedent).
const MAX_PR_WALK_PAGES: usize = 4;

/// How many due repos one tick processes, across every account this tenant
/// covers.
const REPOS_PER_TICK: usize = 10;

struct DueRepo {
    id: Uuid,
    account_id: Uuid,
    installation_id: i64,
    repo_id: i64,
    owner: String,
    name: String,
    default_branch: String,
    narrative_cursor: NarrativeCursor,
    head_tree_sha: Option<String>,
    last_synced_at: DateTime<Utc>,
}

/// Active, fully-backfilled (`last_synced_at IS NOT NULL` — only ever set by
/// `connectors::github_backfill::mark_fully_synced`) rows for one account,
/// oldest-synced-first. Bounding by `limit` AND touching `last_synced_at` at
/// the end of every successful pass (see [`touch_last_synced_at`]) is this
/// sweep's fairness mechanism — the same "oldest gets swept next" rotation
/// `workers::google_chat_sweep`'s own `last_ingested_at`-ordered query uses.
async fn due_repos_for_account(pool: &PgPool, account_id: Uuid, limit: i64) -> Result<Vec<DueRepo>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_reconcile_sweep due_repos scoped tx: {e}")))?;
    #[allow(clippy::type_complexity)]
    let rows: Vec<(Uuid, i64, i64, String, String, String, Option<Value>, Option<String>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, installation_id, repo_id, owner, name, default_branch, narrative_cursor, head_tree_sha, last_synced_at \
         FROM github_watched_repos \
         WHERE account_id = $1 AND active = true AND last_synced_at IS NOT NULL \
         ORDER BY last_synced_at ASC LIMIT $2",
    )
    .bind(account_id)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();

    Ok(rows
        .into_iter()
        .map(
            |(id, installation_id, repo_id, owner, name, default_branch, cursor_json, head_tree_sha, last_synced_at)| DueRepo {
                id,
                account_id,
                installation_id,
                repo_id,
                owner,
                name,
                default_branch,
                narrative_cursor: cursor_json.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default(),
                head_tree_sha,
                last_synced_at,
            },
        )
        .collect())
}

async fn touch_last_synced_at(pool: &PgPool, account_id: Uuid, repo_row_id: Uuid) {
    let scoped = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await;
    let Ok(mut tx) = scoped else { return };
    let _ = sqlx::query("UPDATE github_watched_repos SET last_synced_at = now() WHERE id = $1 AND account_id = $2")
        .bind(repo_row_id)
        .bind(account_id)
        .execute(&mut *tx)
        .await;
    let _ = tx.commit().await;
}

/// Seed a baseline only where none exists. Never overwrites a cursor: a
/// webhook may have set one since this pass read the row.
async fn seed_head_sha_if_unset(pool: &PgPool, account_id: Uuid, repo_row_id: Uuid, sha: &str) {
    let scoped = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await;
    let Ok(mut tx) = scoped else { return };
    let _ = sqlx::query("UPDATE github_watched_repos SET head_tree_sha = $1, updated_at = now() WHERE id = $2 AND account_id = $3 AND head_tree_sha IS NULL")
        .bind(sha)
        .bind(repo_row_id)
        .bind(account_id)
        .execute(&mut *tx)
        .await;
    let _ = tx.commit().await;
}

/// The row's CURRENT head cursor. Re-read right before the code pass rather
/// than trusting the value [`due_repos_for_account`] loaded at the start of
/// the tick: a webhook may have advanced it while this pass walked PRs,
/// issues, and releases, and comparing from the stale value would re-fetch
/// the range that webhook just ingested.
async fn load_head_sha(pool: &PgPool, account_id: Uuid, repo_row_id: Uuid) -> Result<Option<String>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("github_reconcile_sweep load_head_sha scoped tx: {e}")))?;
    let sha: Option<Option<String>> = sqlx::query_scalar("SELECT head_tree_sha FROM github_watched_repos WHERE id = $1 AND account_id = $2")
        .bind(repo_row_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;
    tx.rollback().await.ok();
    Ok(sha.flatten())
}

fn parse_dt(v: &Value, key: &str) -> Option<DateTime<Utc>> {
    v.get(key).and_then(Value::as_str).and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|d| d.with_timezone(&Utc))
}

fn sweep_since(cursor_since: Option<DateTime<Utc>>, last_synced_at: DateTime<Utc>) -> DateTime<Utc> {
    cursor_since.unwrap_or(last_synced_at)
}

fn clamp_since_below_failures(new_since: DateTime<Utc>, oldest_failed: Option<DateTime<Utc>>) -> DateTime<Utc> {
    match oldest_failed {
        Some(failed) if new_since >= failed => failed - chrono::Duration::milliseconds(1),
        _ => new_since,
    }
}

async fn sweep_prs(state: &Arc<AppState>, tenant: &TenantId, pool: &PgPool, token: &str, repo: &DueRepo) -> usize {
    let since = sweep_since(repo.narrative_cursor.pr.since, repo.last_synced_at);
    let mut url = format!(
        "{}/repos/{}/{}/pulls?state=all&sort=updated&direction=desc&per_page={LIST_PAGE_SIZE}", github_api_base(),
        repo.owner, repo.name
    );
    let mut ingested = 0usize;
    let mut new_since = since;
    let mut oldest_failed: Option<DateTime<Utc>> = None;

    'pages: for _ in 0..MAX_PR_WALK_PAGES {
        let (items, next_url, _) = match github::get_page(&state.http_client, token, &url).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: PR page fetch failed, will retry next sweep");
                break;
            }
        };
        for item in &items {
            let Some(updated_at) = parse_dt(item, "updated_at") else { continue };
            if updated_at <= since {
                break 'pages;
            }
            let number = item.get("number").and_then(Value::as_i64).unwrap_or(0);
            let payload = json!({"repository": {"id": repo.repo_id}, "pull_request": {"number": number}});
            match handle_event(tenant, pool, state, repo.account_id, repo.installation_id, "pull_request", &payload).await {
                Ok(EventOutcome::DidWork { .. }) => {
                    ingested += 1;
                    if updated_at > new_since {
                        new_since = updated_at;
                    }
                }
                Ok(EventOutcome::NothingToDo) => {}
                Err(e) => {
                    tracing::warn!(error = %e, repo_row_id = %repo.id, number, "github_reconcile_sweep: PR delta ingest failed");
                    oldest_failed = Some(oldest_failed.map_or(updated_at, |f| f.min(updated_at)));
                }
            }
        }
        match next_url {
            Some(u) => url = u,
            None => break,
        }
    }

    let new_since = clamp_since_below_failures(new_since, oldest_failed);
    if new_since > since {
        let mut cursor = repo.narrative_cursor.clone();
        cursor.pr.since = Some(new_since);
        if let Err(e) = github_backfill::persist_cursor_and_progress(pool, repo.account_id, repo.id, &cursor).await {
            tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: failed to persist advanced PR since-cursor");
        }
    }
    ingested
}

async fn sweep_issues(state: &Arc<AppState>, tenant: &TenantId, pool: &PgPool, token: &str, repo: &DueRepo) -> usize {
    let since = sweep_since(repo.narrative_cursor.issue.since, repo.last_synced_at);
    let since_str = since.to_rfc3339();
    let url = format!(
        "{}/repos/{}/{}/issues?state=all&sort=updated&direction=desc&since={}&per_page={LIST_PAGE_SIZE}", github_api_base(),
        repo.owner,
        repo.name,
        urlencoding::encode(&since_str),
    );
    let (items, _, _) = match github::get_page(&state.http_client, token, &url).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: issues delta fetch failed, will retry next sweep");
            return 0;
        }
    };

    let mut ingested = 0usize;
    let mut new_since = since;
    let mut oldest_failed: Option<DateTime<Utc>> = None;
    for item in &items {
        if github_narrative::is_actually_a_pull_request(item) {
            continue;
        }
        let updated_at = parse_dt(item, "updated_at");
        let payload = json!({"repository": {"id": repo.repo_id}, "action": "reconcile", "issue": item});
        match handle_event(tenant, pool, state, repo.account_id, repo.installation_id, "issues", &payload).await {
            Ok(EventOutcome::DidWork { .. }) => {
                ingested += 1;
                if let Some(u) = updated_at {
                    if u > new_since {
                        new_since = u;
                    }
                }
            }
            Ok(EventOutcome::NothingToDo) => {}
            Err(e) => {
                tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: issue delta ingest failed");
                if let Some(u) = updated_at {
                    oldest_failed = Some(oldest_failed.map_or(u, |f| f.min(u)));
                }
            }
        }
    }

    let new_since = clamp_since_below_failures(new_since, oldest_failed);
    if new_since > since {
        let mut cursor = repo.narrative_cursor.clone();
        cursor.issue.since = Some(new_since);
        if let Err(e) = github_backfill::persist_cursor_and_progress(pool, repo.account_id, repo.id, &cursor).await {
            tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: failed to persist advanced issue since-cursor");
        }
    }
    ingested
}

async fn sweep_releases(state: &Arc<AppState>, tenant: &TenantId, pool: &PgPool, token: &str, repo: &DueRepo) -> usize {
    let since = sweep_since(repo.narrative_cursor.release.since, repo.last_synced_at);
    let url = format!("{}/repos/{}/{}/releases?per_page={LIST_PAGE_SIZE}", github_api_base(), repo.owner, repo.name);
    let (items, _, _) = match github::get_page(&state.http_client, token, &url).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: releases delta fetch failed, will retry next sweep");
            return 0;
        }
    };

    let mut ingested = 0usize;
    let mut new_since = since;
    let mut oldest_failed: Option<DateTime<Utc>> = None;
    for item in &items {
        let Some(published_at) = parse_dt(item, "published_at").or_else(|| parse_dt(item, "created_at")) else { continue };
        if published_at <= since {
            break;
        }
        let payload = json!({"repository": {"id": repo.repo_id}, "action": "reconcile", "release": item});
        match handle_event(tenant, pool, state, repo.account_id, repo.installation_id, "release", &payload).await {
            Ok(EventOutcome::DidWork { .. }) => {
                ingested += 1;
                if published_at > new_since {
                    new_since = published_at;
                }
            }
            Ok(EventOutcome::NothingToDo) => {}
            Err(e) => {
                tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: release delta ingest failed");
                oldest_failed = Some(oldest_failed.map_or(published_at, |f| f.min(published_at)));
            }
        }
    }

    let new_since = clamp_since_below_failures(new_since, oldest_failed);
    if new_since > since {
        let mut cursor = repo.narrative_cursor.clone();
        cursor.release.since = Some(new_since);
        if let Err(e) = github_backfill::persist_cursor_and_progress(pool, repo.account_id, repo.id, &cursor).await {
            tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: failed to persist advanced release since-cursor");
        }
    }
    ingested
}

async fn sweep_default_branch(state: &Arc<AppState>, tenant: &TenantId, pool: &PgPool, token: &str, repo: &DueRepo) -> usize {
    let Some(live_sha) = github_backfill::fetch_default_branch_head_sha(&state.http_client, token, &repo.owner, &repo.name, &repo.default_branch).await
    else {
        return 0;
    };

    let stored = match load_head_sha(pool, repo.account_id, repo.id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: could not re-read the head cursor, using the value this pass started with");
            repo.head_tree_sha.clone()
        }
    };
    let Some(before) = stored else {
        // No baseline yet (should be rare — `mark_fully_synced` normally
        // seeds this) — record one for next time rather than guess a range.
        seed_head_sha_if_unset(pool, repo.account_id, repo.id, &live_sha).await;
        return 0;
    };
    if before == live_sha {
        // The common case once webhooks advance the cursor: the webhook
        // already ingested up to the live head. No compare, no blob fetch.
        tracing::info!(repo_row_id = %repo.id, head = %live_sha, code_range = "skipped", "github_reconcile_sweep: default-branch head already covered, no compare");
        return 0;
    }

    let payload = json!({
        "repository": {"id": repo.repo_id, "default_branch": repo.default_branch},
        "ref": format!("refs/heads/{}", repo.default_branch),
        "before": before,
        "after": live_sha,
    });
    // The handler advances this row's cursor itself, and only if the whole
    // range landed. An incomplete range keeps the cursor where it is, so the
    // next sweep retries it.
    match handle_event_detailed(tenant, pool, state, repo.account_id, repo.installation_id, "push", &payload).await {
        Ok(handled) => {
            let (range_complete, advanced) = handled
                .push
                .as_ref()
                .map(|p| (p.range_complete, p.advanced_rows.contains(&repo.id)))
                .unwrap_or((false, false));
            let ingested = match handled.outcome {
                EventOutcome::DidWork { ingested } => ingested,
                EventOutcome::NothingToDo => 0,
            };
            tracing::info!(
                repo_row_id = %repo.id,
                before = %before,
                head = %live_sha,
                code_range = "fetched",
                range_complete,
                head_cursor_advanced = advanced,
                ingested,
                "github_reconcile_sweep: compared the default-branch range the webhook path had not covered"
            );
            ingested
        }
        Err(e) => {
            tracing::warn!(error = %e, repo_row_id = %repo.id, code_range = "failed", "github_reconcile_sweep: default-branch compare-range ingest failed");
            0
        }
    }
}

async fn sweep_repo(state: &Arc<AppState>, tenant: &TenantId, pool: &PgPool, repo: &DueRepo) -> usize {
    let token = match github::installation_token(&state.http_client, repo.installation_id).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, repo_row_id = %repo.id, "github_reconcile_sweep: could not mint installation token, skipping this repo this pass");
            return 0;
        }
    };

    let mut total = 0usize;
    total += sweep_prs(state, tenant, pool, &token, repo).await;
    total += sweep_issues(state, tenant, pool, &token, repo).await;
    total += sweep_releases(state, tenant, pool, &token, repo).await;
    total += sweep_default_branch(state, tenant, pool, &token, repo).await;

    touch_last_synced_at(pool, repo.account_id, repo.id).await;
    total
}

pub(crate) async fn sweep_account(state: &Arc<AppState>, tenant: &TenantId, pool: &PgPool, account_id: Uuid, budget: &mut usize) -> usize {
    if *budget == 0 {
        return 0;
    }
    let repos = match due_repos_for_account(pool, account_id, *budget as i64).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "github_reconcile_sweep: due_repos_for_account failed");
            return 0;
        }
    };
    let mut processed = 0usize;
    for repo in repos {
        if *budget == 0 {
            break;
        }
        *budget -= 1;
        processed += sweep_repo(state, tenant, pool, &repo).await;
    }
    processed
}

async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

async fn tick_org(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>, org_id: &str) -> Result<Tick, String> {
    let account_id = resolve_tenant_account(control_pool, org_id).await?;
    let mut budget = REPOS_PER_TICK;
    let processed = sweep_account(state, &t.tenant, &t.pool, account_id, &mut budget).await;
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

async fn tick_consumer(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    // Same "no SECURITY DEFINER bootstrap, enumerate `accounts` and probe
    // each one" shape `workers::google_chat_sweep::tick_consumer` uses, for
    // the identical reason: `github_watched_repos` carries `member_ocean_
    // isolation` RLS (graph_id-anchored), so an unscoped cross-account read
    // under `coralswarm_app` returns nothing — this table has no dedicated
    // `account_isolation` policy variant to read around that with.
    let accounts: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
        .fetch_all(control_pool)
        .await
        .map_err(|e| e.to_string())?;
    let mut budget = REPOS_PER_TICK;
    let mut processed = 0usize;
    for account_id in accounts {
        if budget == 0 {
            break;
        }
        processed += sweep_account(state, &t.tenant, &t.pool, account_id, &mut budget).await;
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    match &t.tenant {
        TenantId::Consumer => tick_consumer(t, control_pool, state).await,
        TenantId::Org(org_id) => tick_org(t, control_pool, state, org_id).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_interval_defaults_to_three_hours() {
        unsafe { std::env::remove_var("GITHUB_RECONCILE_INTERVAL_SECS") };
        assert_eq!(poll_interval(), DEFAULT_POLL_INTERVAL);
    }

    #[test]
    fn poll_interval_env_override() {
        // Serialised against the module's other env test via a dedicated
        // lock would be ideal, but this crate's convention (see
        // `connectors::github`'s own tests) only locks when multiple tests
        // in the SAME module race on the SAME var; this is the only such
        // test here.
        unsafe { std::env::set_var("GITHUB_RECONCILE_INTERVAL_SECS", "60") };
        assert_eq!(poll_interval(), Duration::from_secs(60));
        unsafe { std::env::remove_var("GITHUB_RECONCILE_INTERVAL_SECS") };
    }

    #[test]
    fn sweep_since_falls_back_to_last_synced_at() {
        let last = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        assert_eq!(sweep_since(None, last), last);
        let earlier = last - chrono::Duration::hours(1);
        assert_eq!(sweep_since(Some(earlier), last), earlier);
    }

    #[test]
    fn clamp_since_holds_at_or_below_oldest_failure() {
        let ok = DateTime::<Utc>::from_timestamp(1_700_000_100, 0).unwrap();
        let failed = DateTime::<Utc>::from_timestamp(1_700_000_050, 0).unwrap();
        assert_eq!(clamp_since_below_failures(ok, None), ok);
        assert!(clamp_since_below_failures(ok, Some(failed)) < failed);
    }

    #[test]
    fn poll_interval_zero_or_invalid_falls_back_to_default() {
        unsafe { std::env::set_var("GITHUB_RECONCILE_INTERVAL_SECS", "0") };
        assert_eq!(poll_interval(), DEFAULT_POLL_INTERVAL);
        unsafe { std::env::set_var("GITHUB_RECONCILE_INTERVAL_SECS", "not-a-number") };
        assert_eq!(poll_interval(), DEFAULT_POLL_INTERVAL);
        unsafe { std::env::remove_var("GITHUB_RECONCILE_INTERVAL_SECS") };
    }
}
