//! One generic adapter per Composio toolkit: Jira, Linear, Confluence, HubSpot,
//! Zendesk and Dart. Each is a single workspace-wide source that captures from
//! the moment of connect. The word "Composio" never leaves the backend.

use async_trait::async_trait;
use chrono::{Duration, Utc};
use uuid::Uuid;

use super::runtime_from_row;
use crate::connectors::catalog::facts::ComposioRow;
use crate::connectors::catalog::{
    connect, pause, scope, AdapterCx, AdapterError, ConnectStart, ConnectorAdapter, GrantHealth,
    GrantView, Problem, ReturnTo, ScopeSelection, SourceRuntime, SourceSpec,
};
use crate::connectors::composio::{recipe, Client};

/// How long the web keeps offering the hosted sign-in before it asks the member
/// to start again. Composio's link carries no expiry of its own; a connection
/// nobody finished is swept by the capture worker.
const HOSTED_SIGN_IN_SECS: i64 = 600;

macro_rules! toolkit_sources {
    ($name:ident, $label:expr, [$($t:expr),+ $(,)?]) => {
        const $name: &[SourceSpec] = &[SourceSpec {
            key: "workspace",
            label: $label,
            atom_source_types: &[$($t),+],
            scope: crate::connectors::catalog::ScopeShape::EverythingOnly,
            history: crate::connectors::catalog::HistorySupport::Unsupported {
                reason: "starts_at_connect",
            },
        }];
    };
}

// Must equal `{toolkit}_{kind}` for every kind in the toolkit's recipe; the test
// at the bottom of this file fails when a recipe gains or loses a kind.
toolkit_sources!(JIRA, "Workspace", ["jira_issue", "jira_comment"]);
toolkit_sources!(
    LINEAR,
    "Workspace",
    ["linear_issue", "linear_comment", "linear_project"]
);
toolkit_sources!(
    CONFLUENCE,
    "Workspace",
    ["confluence_content", "confluence_comment"]
);
toolkit_sources!(
    HUBSPOT,
    "Workspace",
    [
        "hubspot_company",
        "hubspot_contact",
        "hubspot_deal",
        "hubspot_note",
        "hubspot_call",
        "hubspot_meeting",
    ]
);
toolkit_sources!(ZENDESK, "Workspace", ["zendesk_ticket", "zendesk_comment"]);
toolkit_sources!(DART, "Workspace", ["dart_task"]);

pub struct ComposioAdapter {
    toolkit: &'static str,
    sources: &'static [SourceSpec],
}

impl ComposioAdapter {
    /// The adapter for one toolkit slug. `None` for a slug with no recipe, which
    /// has no capture path at all.
    pub fn for_toolkit(slug: &str) -> Option<Self> {
        let (toolkit, sources) = match slug {
            "jira" => ("jira", JIRA),
            "linear" => ("linear", LINEAR),
            "confluence" => ("confluence", CONFLUENCE),
            "hubspot" => ("hubspot", HUBSPOT),
            "zendesk" => ("zendesk", ZENDESK),
            "dart" => ("dart", DART),
            _ => return None,
        };
        recipe::get(toolkit)?;
        Some(Self { toolkit, sources })
    }
}

/// A toolkit connection's sign-in. The row's own `status` is the health: the
/// capture engine moves it to `needs_attention` when the recipe's self-check
/// fails and to `reconnect_needed` when the provider revokes access.
fn grant_from_row(c: &ComposioRow) -> GrantView {
    let health = match c.status.as_str() {
        "needs_attention" => {
            GrantHealth::NeedsAttention(Problem::new("capture_unavailable", c.updated_at))
        }
        // A sign-in that never bound a workspace expired or was abandoned; one
        // that did was revoked.
        "reconnect_needed" if c.provider_namespace.is_none() => {
            GrantHealth::ReconnectNeeded(Problem::new("sign_in_expired", c.updated_at))
        }
        "reconnect_needed" => {
            GrantHealth::ReconnectNeeded(Problem::new("access_revoked", c.updated_at))
        }
        _ => GrantHealth::Healthy,
    };
    GrantView {
        account_label: None,
        connected_at: c.created_at,
        health,
    }
}

#[async_trait]
impl ConnectorAdapter for ComposioAdapter {
    fn key(&self) -> &'static str {
        self.toolkit
    }

    fn display_name(&self) -> &'static str {
        // `for_toolkit` proved the recipe exists.
        recipe::get(self.toolkit).map_or(self.toolkit, |r| r.label.as_str())
    }

    /// Always Beta here. The older `GET /connectors/composio` drops the badge for
    /// a toolkit once any account's self-check passes; a catalog whose badge
    /// depends on other accounts' data would make the grid differ per
    /// deployment, and the Beta badge is meant to be the one constant
    /// difference (decision 3).
    fn beta(&self) -> bool {
        true
    }

    fn sources(&self) -> &'static [SourceSpec] {
        self.sources
    }

    fn grant_facts(&self, facts: &super::super::Facts) -> Result<Option<GrantView>, AdapterError> {
        Ok(facts.composio(self.toolkit).map(grant_from_row))
    }

    async fn grant(&self, cx: &AdapterCx<'_>) -> Result<Option<GrantView>, AdapterError> {
        self.grant_facts(cx.facts)
    }

    async fn source_runtime(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
    ) -> Result<SourceRuntime, AdapterError> {
        Ok(runtime_from_row(
            cx.facts.source(self.toolkit, s.key),
            s,
            self.toolkit,
            true,
        ))
    }

    /// One workspace-wide source: "everything" is the only scope, and storing it
    /// is a no-op beyond the row.
    async fn set_scope(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        sel: &ScopeSelection,
    ) -> Result<(), AdapterError> {
        scope::validate(s.scope, sel)?;
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

    /// Pause maps to the capture, not to a state of it: the capture's states
    /// (`active`, `retry`, `needs_attention`, ...) describe the connection's
    /// health, and a member's pause is not a fault. The poll reads this row
    /// before it takes its lease (`routes::composio::poll`) and defers the
    /// capture, so a paused toolkit reads nothing and a resumed one carries on
    /// from its cursor.
    async fn set_paused(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        paused: bool,
    ) -> Result<(), AdapterError> {
        pause::write_paused(cx, self, s.key, paused).await
    }

    /// A hosted sign-in: the toolkit's own link and the new connection's id as
    /// `pending_id`. The member signs in at the link; `complete_connect` then
    /// verifies the grant, binds the workspace and starts capture. The return
    /// target does not apply: there is no callback to redirect.
    async fn connect(
        &self,
        cx: &AdapterCx<'_>,
        _ret: ReturnTo,
    ) -> Result<ConnectStart, AdapterError> {
        // A connection made before the catalog that needs signing in again stays
        // set up rather than returning to the wizard.
        connect::keep_legacy_connection_set_up(cx, self).await?;
        // `for_toolkit` proved the recipe exists.
        let recipe = recipe::get(self.toolkit).ok_or(AdapterError::NotConnected)?;
        let (id, link) =
            match crate::routes::composio::start_connect(cx.state, cx.user, recipe).await {
                Ok(started) => started,
                Err(resp) => return Err(connect::refusal(resp).await),
            };
        Ok(ConnectStart {
            authorize_url: link,
            expires_at: Utc::now() + Duration::seconds(HOSTED_SIGN_IN_SECS),
            pending_id: Some(id.to_string()),
        })
    }

    /// Verify the grant, bind the workspace, run the toolkit's self-check and
    /// start capture, all through the existing `verify` path. Answers
    /// `SignInPending` while the member has not finished at the link.
    async fn complete_connect(
        &self,
        cx: &AdapterCx<'_>,
        pending_id: &str,
    ) -> Result<(), AdapterError> {
        let id = Uuid::parse_str(pending_id).map_err(|_| AdapterError::Refused {
            status: 400,
            code: "invalid_request",
        })?;
        let recipe = recipe::get(self.toolkit).ok_or(AdapterError::NotConnected)?;
        let client = Client::configured().map_err(|_| AdapterError::NotConfigured)?;
        let resp =
            crate::routes::composio::verify_with(cx.state, cx.tenant, cx.user, recipe, id, &client)
                .await;
        connect::expect_ok(resp).await
    }

    /// Stop capture and revoke the connected account at the provider, for every
    /// connection of this toolkit the member has that is not already
    /// disconnected (a sign-in nobody finished included), through the existing
    /// disconnect. A failed revocation answers 502 and is safe to retry.
    async fn disconnect(&self, cx: &AdapterCx<'_>) -> Result<(), AdapterError> {
        let recipe = recipe::get(self.toolkit).ok_or(AdapterError::NotConnected)?;
        let pool = cx
            .state
            .control_db()
            .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, cx.account_id).await?;
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM composio_connections \
             WHERE account_id = $1 AND owner_user_id = $2 AND service = $3 \
               AND status <> 'disconnected' \
             ORDER BY created_at",
        )
        .bind(cx.account_id)
        .bind(cx.user_id)
        .bind(self.toolkit)
        .fetch_all(&mut *tx)
        .await?;
        tx.rollback().await.ok();
        if ids.is_empty() {
            return Err(AdapterError::NotConnected);
        }
        let client = Client::configured().map_err(|_| AdapterError::NotConfigured)?;
        for id in ids {
            let resp = crate::routes::composio::disconnect_with(
                cx.state, cx.tenant, cx.user, recipe, id, &client,
            )
            .await;
            connect::expect_ok(resp).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::catalog::ConnectorAdapter;

    #[test]
    fn source_types_match_each_recipe() {
        for r in recipe::all() {
            let a = ComposioAdapter::for_toolkit(&r.toolkit)
                .unwrap_or_else(|| panic!("recipe {} has no adapter", r.toolkit));
            let mut want: Vec<String> = r
                .kinds()
                .iter()
                .map(|k| format!("{}_{k}", r.toolkit))
                .collect();
            let mut got: Vec<String> = a.sources()[0]
                .atom_source_types
                .iter()
                .map(|s| s.to_string())
                .collect();
            want.sort();
            got.sort();
            assert_eq!(got, want, "{}", r.toolkit);
        }
    }

    #[test]
    fn a_slug_without_a_recipe_has_no_adapter() {
        assert!(ComposioAdapter::for_toolkit("slack").is_none());
        assert!(ComposioAdapter::for_toolkit("nope").is_none());
    }
}
