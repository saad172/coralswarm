//! GitHub: repositories. The sign-in is the member's App installation; the
//! source's state is the per-repo selection on the content plane.

use async_trait::async_trait;

use super::{runtime_from_row, with_count};
use crate::connectors::catalog::history;
use crate::connectors::catalog::{
    connect, pause, scope, AdapterCx, AdapterError, ConnectStart, ConnectorAdapter, GrantHealth,
    GrantView, HistorySupport, Importing, Page, Problem, ReturnTo, ScopeItem, ScopeItemsQuery,
    ScopeMode, ScopeSelection, ScopeShape, SourceRuntime, SourceSpec,
};

const SOURCES: &[SourceSpec] = &[SourceSpec {
    key: "repos",
    label: "Repositories",
    // Only the first four are still produced (the code tier was retired on
    // 2026-10-27); the other two stay so atoms written before then keep a home.
    atom_source_types: &[
        "github_pr",
        "github_issue",
        "github_release",
        "github_commit",
        "github_code",
        "github_code_summary",
    ],
    scope: ScopeShape::Items {
        noun: "repositories",
    },
    // Automatic backfill is unbounded; 90 caps only a window the member chooses.
    history: HistorySupport::Backfill { max_days: 90 },
}];

pub struct GithubAdapter;

#[async_trait]
impl ConnectorAdapter for GithubAdapter {
    fn key(&self) -> &'static str {
        "github"
    }

    fn display_name(&self) -> &'static str {
        "GitHub"
    }

    fn sources(&self) -> &'static [SourceSpec] {
        SOURCES
    }

    fn grant_facts(&self, facts: &super::super::Facts) -> Result<Option<GrantView>, AdapterError> {
        // An install the member made is theirs only while they hold their own
        // token: disconnecting deletes it and pauses their repos but leaves the
        // App installed at GitHub, and that must read as "not connected". An
        // install with no recorded installer stays the account's.
        let has_token = facts.token("github").is_some_and(|t| t.mine);
        let installs: Vec<_> = facts
            .installs
            .iter()
            .filter(|i| !i.mine || has_token)
            .collect();
        let Some(first) = installs.first() else {
            return Ok(None);
        };
        // Suspended only when every installation is: one live install still
        // captures.
        let all_suspended = installs.iter().all(|i| i.suspended_at.is_some());
        let health = match installs.iter().filter_map(|i| i.suspended_at).min() {
            Some(since) if all_suspended => {
                GrantHealth::NeedsAttention(Problem::new("installation_suspended", since))
            }
            _ => GrantHealth::Healthy,
        };
        Ok(Some(GrantView {
            account_label: Some(first.account_login.clone()).filter(|l| !l.is_empty()),
            connected_at: first.created_at,
            health,
        }))
    }

    async fn grant(&self, cx: &AdapterCx<'_>) -> Result<Option<GrantView>, AdapterError> {
        self.grant_facts(cx.facts)
    }

    async fn source_runtime(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
    ) -> Result<SourceRuntime, AdapterError> {
        let repos = &cx.facts.github;
        // A member with repos picked before the catalog is on and set up; one
        // with none picked has not finished choosing.
        let mut rt = runtime_from_row(
            cx.facts.source("github", s.key),
            s,
            "github",
            repos.active > 0,
        );
        rt.importing = repos.importing_percent.map(|p| Importing {
            percent: Some(p),
            // Automatic backfill has no window; 0 reads "unbounded".
            window_days: rt.history_days.unwrap_or(0),
        });
        rt.problem = repos
            .error
            .as_ref()
            .map(|(code, at)| Problem::new(code, *at).for_source(s.key));
        Ok(with_count(rt, repos.active as u32))
    }

    /// The GitHub App install flow: `authorize` sends the member to the App's
    /// install page, and the callback records the installation and the member's
    /// own token. GitHub asks an organization owner for approval itself when the
    /// member cannot install there.
    async fn connect(
        &self,
        cx: &AdapterCx<'_>,
        ret: ReturnTo,
    ) -> Result<ConnectStart, AdapterError> {
        connect::keep_legacy_connection_set_up(cx, self).await?;
        connect::oauth_connect(cx, "github", None, ret).await
    }

    /// The existing GitHub disconnect: removes the member's token and pauses
    /// their watched repos. It never deletes, so captured atoms and the
    /// repos' history stay, and the App itself stays installed at GitHub.
    async fn disconnect(&self, cx: &AdapterCx<'_>) -> Result<(), AdapterError> {
        connect::any_removed(connect::disconnect_oauth(cx, "github").await?)
    }

    /// The repositories the App can see, across the member's installations, with
    /// whether each is captured (`github_watched_repos.active`).
    async fn scope_items(
        &self,
        cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        q: &ScopeItemsQuery,
    ) -> Result<Page<ScopeItem>, AdapterError> {
        let mut items = Vec::new();
        for install in installs(cx) {
            for r in repos_of(cx, install).await? {
                items.push(r.item());
            }
        }
        Ok(scope::paginate(items, q))
    }

    /// A pick makes the named repositories the captured ones, through the older
    /// picker's own `PUT .../repos` (so its destination, consent and backfill
    /// rules apply unchanged; a new repo goes to the member's own ocean).
    /// Everything selects the currently granted repositories. Later App grants
    /// still enter through the existing paused-grant admission path.
    async fn set_scope(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        sel: &ScopeSelection,
    ) -> Result<(), AdapterError> {
        scope::validate(s.scope, sel)?;
        let want: std::collections::HashSet<&str> =
            sel.item_ids.iter().map(String::as_str).collect();
        let mut known = 0usize;
        for install in installs(cx) {
            let mut selections = Vec::new();
            for r in repos_of(cx, install).await? {
                let wanted = sel.mode == ScopeMode::Everything || want.contains(r.id().as_str());
                known += usize::from(wanted);
                if wanted != r.active {
                    selections.push(if wanted {
                        serde_json::json!({"repo_id": r.repo_id, "active": true, "destination": {"kind": "personal"}})
                    } else {
                        serde_json::json!({"repo_id": r.repo_id, "active": false})
                    });
                }
            }
            if selections.is_empty() {
                continue;
            }
            let body: crate::routes::github::PutReposBody = serde_json::from_value(
                serde_json::json!({"installation_id": install, "selections": selections}),
            )
            .map_err(|e| AdapterError::Database(e.to_string()))?;
            let resp = crate::routes::github::put_repos(
                axum::extract::State(cx.state.clone()),
                cx.tenant.clone(),
                axum::Extension(cx.user.clone()),
                axum::Json(body),
            )
            .await;
            connect::expect_ok(resp).await?;
        }
        if known < want.len() && !want.is_empty() {
            // Applied what could be, and some named repository is not one the
            // App can see: the member's list is stale.
            tracing::info!(
                wanted = want.len(),
                known,
                "github scope: some picked repositories are unknown"
            );
        }
        scope::write_mode(cx, self, s.key, sel.mode).await
    }

    async fn set_enabled(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        on: bool,
    ) -> Result<(), AdapterError> {
        pause::write_enabled(cx, self, s.key, on).await
    }

    /// The pause gate reads this row where webhook events are turned into atoms
    /// (`github_event_ingest`).
    async fn set_paused(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        paused: bool,
    ) -> Result<(), AdapterError> {
        pause::write_paused(cx, self, s.key, paused).await?;
        if !paused {
            // A paused tick completes its queue job without consuming the
            // cursor. Resume must wake those repos; do not reset their bounds.
            let pool = cx
                .tenant
                .pool
                .as_ref()
                .ok_or_else(|| AdapterError::Database("content plane unavailable".into()))?;
            let mut tx =
                crate::tenancy::scope::begin_tenant_system_scoped(pool, cx.account_id).await?;
            let repos: Vec<uuid::Uuid> = sqlx::query_scalar(
                "SELECT id FROM github_watched_repos WHERE account_id=$1 AND owner_user_id=$2 AND active",
            )
            .bind(cx.account_id)
            .bind(cx.user_id)
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
            for repo_row_id in repos {
                crate::jobs::queue::enqueue(
                    pool,
                    &crate::jobs::JobKind::GithubBackfill { repo_row_id },
                    Some(cx.account_id),
                    Some(&format!("github_backfill:{repo_row_id}")),
                    0,
                )
                .await?;
            }
        }
        Ok(())
    }
    async fn start_backfill(
        &self,
        cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        days: u32,
    ) -> Result<(), AdapterError> {
        history::github::start(cx, days).await
    }
}

/// The installations the member made (or the account's own).
fn installs(cx: &AdapterCx<'_>) -> Vec<i64> {
    cx.facts
        .installs
        .iter()
        .map(|i| i.installation_id)
        .collect()
}

struct Repo {
    repo_id: i64,
    full_name: String,
    visibility: String,
    active: bool,
}

impl Repo {
    fn id(&self) -> String {
        self.repo_id.to_string()
    }

    fn item(&self) -> ScopeItem {
        ScopeItem {
            id: self.id(),
            label: self.full_name.clone(),
            meta: Some(self.visibility.clone()).filter(|v| !v.is_empty()),
            selected: self.active,
        }
    }
}

async fn repos_of(cx: &AdapterCx<'_>, installation_id: i64) -> Result<Vec<Repo>, AdapterError> {
    let params: crate::routes::github::ListReposParams =
        serde_json::from_value(serde_json::json!({"installation_id": installation_id.to_string()}))
            .map_err(|e| AdapterError::Database(e.to_string()))?;
    let resp = crate::routes::github::list_repos(
        axum::extract::State(cx.state.clone()),
        cx.tenant.clone(),
        axum::Extension(cx.user.clone()),
        axum::extract::Query(params),
    )
    .await;
    let body = scope::read_json(resp).await?;
    Ok(body["repos"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| {
            Some(Repo {
                repo_id: r["repo_id"].as_i64()?,
                full_name: r["full_name"].as_str()?.to_string(),
                visibility: r["visibility"].as_str().unwrap_or_default().to_string(),
                active: r["active"].as_bool() == Some(true),
            })
        })
        .collect())
}
