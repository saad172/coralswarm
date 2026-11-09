//! GitHub's per-repo state, which lives on the content plane
//! (`github_watched_repos`), folded to what the catalog needs.
//!
//! Its own file because it is the one catalog read that runs on the tenant pool
//! (`begin_tenant_system_scoped`, as `routes::github` and the backfill worker do),
//! while `facts.rs` reads control-plane rows in an account-scoped transaction.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::AdapterError;
use crate::tenancy::extractor::Tenant;

/// One `github_watched_repos` row, as far as the catalog reads it.
pub type RepoRow = (
    bool,
    Option<String>,
    Option<DateTime<Utc>>,
    Option<serde_json::Value>,
);

/// GitHub's per-repo state, folded to what the catalog needs.
#[derive(Debug, Clone, Default)]
pub struct GithubRepoFacts {
    pub active: i64,
    /// The closed `sync_error_code` set (`destination_unreachable` |
    /// `sync_failed`) of a failing repo, with when it was recorded.
    pub error: Option<(String, DateTime<Utc>)>,
    /// Mean backfill progress over active repos that report one, 0 to 100.
    /// `None` when no active repo is mid-backfill.
    pub importing_percent: Option<u8>,
}

/// Read the caller's repos. `Default` when the request has no content pool.
pub async fn load(
    tenant: &Tenant,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<GithubRepoFacts, AdapterError> {
    let Some(pool) = tenant.pool.as_ref() else {
        return Ok(GithubRepoFacts::default());
    };
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await?;
    let rows: Vec<RepoRow> = sqlx::query_as(
        r#"
        SELECT active, sync_error_code, sync_error_at, backfill_progress
        FROM github_watched_repos
        WHERE account_id = $1 AND owner_user_id = $2
        LIMIT 1000
        "#,
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(fold_repo_rows(&rows))
}

/// Pure half of [`github_repo_facts`].
pub(crate) fn fold_repo_rows(rows: &[RepoRow]) -> GithubRepoFacts {
    let mut facts = GithubRepoFacts::default();
    let mut progress: Vec<u32> = Vec::new();
    for (active, code, at, backfill) in rows {
        if *active {
            facts.active += 1;
        }
        if let (Some(code), Some(at)) = (code, at) {
            // Keep the oldest failure: `since` is when it started.
            if facts.error.as_ref().is_none_or(|(_, prev)| at < prev) {
                facts.error = Some((code.clone(), *at));
            }
        }
        if !*active {
            continue;
        }
        if let Some(pct) = repo_progress(backfill.as_ref()) {
            if pct < 100 {
                progress.push(pct);
            }
        }
    }
    if !progress.is_empty() {
        let sum: u32 = progress.iter().sum();
        facts.importing_percent = Some((sum / progress.len() as u32) as u8);
    }
    facts
}

/// A repo's overall backfill progress: the lowest of the per-type percentages it
/// reports (`prs`, `issues`, `releases`, `commits`), so one lagging type keeps the
/// repo "importing". `None` when it reports none, which includes every repo that
/// predates the progress column: those read as settled, not as stuck.
fn repo_progress(raw: Option<&serde_json::Value>) -> Option<u32> {
    let v = raw?;
    ["prs", "issues", "releases", "commits"]
        .iter()
        .filter_map(|k| v.get(k).and_then(serde_json::Value::as_u64))
        .map(|n| n.min(100) as u32)
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn t(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn repo_progress_is_the_lowest_reported_type() {
        assert_eq!(
            repo_progress(Some(&json!({"prs": 100, "issues": 40, "commits": 90}))),
            Some(40)
        );
        assert_eq!(repo_progress(Some(&json!({"updated_at": "x"}))), None);
        assert_eq!(repo_progress(None), None);
        assert_eq!(
            repo_progress(Some(&json!({"prs": 250}))),
            Some(100),
            "clamped"
        );
    }

    #[test]
    fn fold_counts_active_and_averages_unfinished_progress() {
        let rows = vec![
            (true, None, None, Some(json!({"prs": 20}))),
            (true, None, None, Some(json!({"prs": 60}))),
            (true, None, None, Some(json!({"prs": 100}))),
            (false, None, None, Some(json!({"prs": 5}))),
            (true, None, None, None),
        ];
        let f = fold_repo_rows(&rows);
        assert_eq!(f.active, 4);
        assert_eq!(
            f.importing_percent,
            Some(40),
            "finished, inactive and unreported repos do not count"
        );
        assert!(f.error.is_none());
    }

    #[test]
    fn fold_keeps_the_oldest_failure() {
        let rows = vec![
            (
                true,
                Some("sync_failed".into()),
                Some(t("2026-10-28T00:00:00Z")),
                None,
            ),
            (
                true,
                Some("destination_unreachable".into()),
                Some(t("2026-10-26T00:00:00Z")),
                None,
            ),
        ];
        let f = fold_repo_rows(&rows);
        assert_eq!(
            f.error,
            Some(("destination_unreachable".into(), t("2026-10-26T00:00:00Z")))
        );
    }

    #[test]
    fn nothing_importing_when_every_repo_is_done() {
        let rows = vec![(true, None, None, Some(json!({"prs": 100, "issues": 100})))];
        assert_eq!(fold_repo_rows(&rows).importing_percent, None);
    }
}
