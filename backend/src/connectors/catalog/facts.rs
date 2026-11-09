//! One read of the rows the adapters derive status from.
//!
//! The control-plane rows (`oauth_tokens`, `composio_connections`,
//! `github_installations`, `slack_integrations`, `connector_sources`) are read
//! in ONE `begin_account_scoped` transaction, so listing ten connectors costs one
//! transaction and not ten. GitHub's per-repo state lives on the content plane
//! (`github_watched_repos`) and is read in one tenant-system-scoped transaction.
//!
//! Every statement filters on the caller's own `user_id`, never a request
//! parameter. `oauth_tokens` keeps the `(owner_user_id IS NULL OR owner_user_id =
//! $2)` shape `routes::oauth::list_connections` uses and for the same stated
//! reason: this is a metadata listing of what the caller may see, not a
//! credential read.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::github_facts::GithubRepoFacts;
use super::AdapterError;
use crate::tenancy::extractor::Tenant;
use crate::AppState;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TokenRow {
    pub provider: String,
    pub provider_workspace_id: Option<String>,
    pub scope: String,
    pub created_at: DateTime<Utc>,
    pub credential_invalid_at: Option<DateTime<Utc>>,
    pub credential_invalid_reason: Option<String>,
    pub mine: bool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ComposioRow {
    pub service: String,
    pub status: String,
    pub provider_namespace: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct InstallRow {
    pub installation_id: i64,
    pub account_login: String,
    pub suspended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    /// The caller installed it (`connected_by`), as opposed to an install with no
    /// recorded installer, which is the account's.
    pub mine: bool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SourceRow {
    pub provider: String,
    pub source: String,
    pub enabled: bool,
    pub paused_at: Option<DateTime<Utc>>,
    pub scope_mode: Option<String>,
    pub history_days: Option<i32>,
    pub history_requested_at: Option<DateTime<Utc>>,
    pub setup_completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct Facts {
    pub tokens: Vec<TokenRow>,
    pub composio: Vec<ComposioRow>,
    pub installs: Vec<InstallRow>,
    pub sources: Vec<SourceRow>,
    /// `(workspace_id, workspace_name)` of the account's Slack installs.
    pub slack_workspaces: Vec<(String, String)>,
    pub github: GithubRepoFacts,
}

impl Facts {
    /// The caller's row for `provider`: their own before an account-wide one,
    /// newest first (the order `list_connections` documents for the same rows).
    pub fn token(&self, provider: &str) -> Option<&TokenRow> {
        self.tokens.iter().find(|t| t.provider == provider)
    }

    /// The caller's live toolkit connection. A pending or disconnected one is no
    /// connection at all.
    pub fn composio(&self, toolkit: &str) -> Option<&ComposioRow> {
        self.composio.iter().find(|c| c.service == toolkit)
    }

    pub fn source(&self, provider: &str, source: &str) -> Option<&SourceRow> {
        self.sources
            .iter()
            .find(|s| s.provider == provider && s.source == source)
    }

    pub fn slack_workspace_name(&self, workspace_id: Option<&str>) -> Option<&str> {
        let by_id = workspace_id
            .and_then(|id| self.slack_workspaces.iter().find(|(w, _)| w == id))
            .or_else(|| self.slack_workspaces.first());
        by_id.map(|(_, name)| name.as_str())
    }

    /// Read everything for one caller: the control plane through
    /// `state.control_db()`, GitHub's repos through the request's content pool.
    pub async fn load(
        state: &AppState,
        tenant: &Tenant,
        account_id: Uuid,
        user_id: Uuid,
    ) -> Result<Self, AdapterError> {
        let control = state
            .control_db()
            .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id).await?;

        let mut facts = Self::load_scoped(&mut tx, account_id, user_id).await?;
        tx.rollback().await.ok();
        // Existing Other actions use their already-authorized content context.
        if !facts.installs.is_empty() {
            facts.github = super::github_facts::load(tenant, account_id, user_id).await?;
        }
        Ok(facts)
    }
    /// Read control facts within the worker's verified readonly transaction.
    pub async fn load_scoped(
        tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
        account_id: Uuid,
        user_id: Uuid,
    ) -> Result<Self, AdapterError> {
        let tokens = sqlx::query_as::<_, TokenRow>(
            r#"
            SELECT provider, provider_workspace_id, scope, created_at,
                   credential_invalid_at, credential_invalid_reason,
                   COALESCE(owner_user_id = $2, false) AS mine
            FROM oauth_tokens
            WHERE account_id = $1 AND (owner_user_id IS NULL OR owner_user_id = $2)
            ORDER BY provider, (owner_user_id IS NULL), updated_at DESC
            "#,
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_all(&mut **tx)
        .await?;

        let composio = sqlx::query_as::<_, ComposioRow>(
            r#"
            SELECT service, status, provider_namespace, created_at, updated_at
            FROM composio_connections
            WHERE account_id = $1 AND owner_user_id = $2
              AND status IN ('active', 'needs_attention', 'reconnect_needed')
            ORDER BY created_at DESC
            "#,
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_all(&mut **tx)
        .await?;

        // A GitHub App install belongs to the member who made it. A row with no
        // recorded installer (`connected_by` is SET NULL when that user goes) is
        // still the account's.
        let installs = sqlx::query_as::<_, InstallRow>(
            r#"
            SELECT installation_id, account_login, suspended_at, created_at,
                   COALESCE(connected_by = $2, false) AS mine
            FROM github_installations
            WHERE account_id = $1 AND (connected_by = $2 OR connected_by IS NULL)
            ORDER BY created_at
            LIMIT 50
            "#,
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_all(&mut **tx)
        .await?;

        let sources = sqlx::query_as::<_, SourceRow>(
            r#"
            SELECT provider, source, enabled, paused_at, scope_mode, history_days,
                   history_requested_at, setup_completed_at
            FROM connector_sources
            WHERE account_id = $1 AND owner_user_id = $2
            "#,
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_all(&mut **tx)
        .await?;

        let slack_workspaces = sqlx::query_as::<_, (String, String)>(
            "SELECT workspace_id, workspace_name FROM slack_integrations \
             WHERE account_id = $1 ORDER BY created_at",
        )
        .bind(account_id)
        .fetch_all(&mut **tx)
        .await?;

        Ok(Self {
            tokens,
            composio,
            installs,
            sources,
            slack_workspaces,
            github: GithubRepoFacts::default(),
        })
    }
}
