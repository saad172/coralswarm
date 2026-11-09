//! Slack: channels. The sign-in is the member's own `slack_user` grant, with the
//! workspace app (`slack`, the bot) installed alongside it (decision 4).

use async_trait::async_trait;

use super::{credential_problem, grant_from_token, runtime_from_row, with_count};
use crate::connectors::catalog::history;
use crate::connectors::catalog::{
    connect, pause, scope, AdapterCx, AdapterError, ConnectStart, ConnectorAdapter, GrantHealth,
    GrantView, HistorySupport, Page, ParamValue, Problem, ReturnTo, ScopeItem, ScopeItemsQuery,
    ScopeMode, ScopeSelection, ScopeShape, SourceRuntime, SourceSpec,
};
use crate::routes::oauth::{SLACK_INSTALL_SURFACE, SLACK_MEMBER_SURFACE};

const SOURCES: &[SourceSpec] = &[SourceSpec {
    key: "channels",
    label: "Channels",
    atom_source_types: &["slack"],
    scope: ScopeShape::Items { noun: "channels" },
    history: HistorySupport::Backfill { max_days: 90 },
}];

pub struct SlackAdapter;

/// The workspace app is installed and still good: its credential is alive and it
/// holds every scope this build asks for. Then a member only needs to sign
/// themselves in; otherwise the connect installs the app too.
fn app_is_usable(cx: &AdapterCx<'_>) -> bool {
    cx.facts.token("slack").is_some_and(|bot| {
        credential_problem(bot).is_none()
            && !crate::connectors::slack::slack_missing_scopes(&bot.scope)
                .is_some_and(|missing| !missing.is_empty())
    })
}

#[async_trait]
impl ConnectorAdapter for SlackAdapter {
    fn key(&self) -> &'static str {
        "slack"
    }

    fn display_name(&self) -> &'static str {
        "Slack"
    }

    fn sources(&self) -> &'static [SourceSpec] {
        SOURCES
    }

    /// The member's `slack_user` row, else the workspace's bot row. A workspace
    /// installed before per-member grants has only the bot row, and that
    /// connection must not read "not connected" after the redesign.
    fn grant_facts(&self, facts: &super::super::Facts) -> Result<Option<GrantView>, AdapterError> {
        let facts = facts;
        let bot = facts.token("slack");
        let Some(row) = facts.token("slack_user").filter(|t| t.mine).or(bot) else {
            return Ok(None);
        };
        let label = facts
            .slack_workspace_name(row.provider_workspace_id.as_deref())
            .map(str::to_string);
        let mut grant = grant_from_token(row, label);
        if grant.health == GrantHealth::Healthy {
            grant.health = match bot {
                None => {
                    GrantHealth::NeedsAttention(Problem::new("app_not_installed", row.created_at))
                }
                Some(bot) => match credential_problem(bot) {
                    Some(p) => GrantHealth::ReconnectNeeded(p),
                    None => match crate::connectors::slack::slack_missing_scopes(&bot.scope) {
                        Some(missing) if !missing.is_empty() => GrantHealth::NeedsAttention(
                            Problem::new("missing_scopes", bot.created_at)
                                .with_param("count", ParamValue::Num(missing.len() as i64)),
                        ),
                        _ => GrantHealth::Healthy,
                    },
                },
            };
        }
        Ok(Some(grant))
    }

    async fn grant(&self, cx: &AdapterCx<'_>) -> Result<Option<GrantView>, AdapterError> {
        self.grant_facts(cx.facts)
    }

    async fn source_runtime(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
    ) -> Result<SourceRuntime, AdapterError> {
        let rt = runtime_from_row(cx.facts.source("slack", s.key), s, "slack", true);
        if rt.scope.is_none_or(|sc| sc.mode != ScopeMode::Pick) {
            return Ok(rt);
        }
        Ok(with_count(rt, picked_count(cx).await?))
    }

    /// One OAuth that covers the bot and the member's own scopes (decision 4),
    /// done by whoever connects. When the workspace app is already installed and
    /// usable, only the member's scopes are asked for, so connecting never
    /// reinstalls the app for everyone. Slack asks for admin approval itself when
    /// the workspace requires it. Under an org the bot install stays an admin's
    /// act (`mint_state_token` refuses anyone else), and a member of an org whose
    /// app is installed connects without being one.
    async fn connect(
        &self,
        cx: &AdapterCx<'_>,
        ret: ReturnTo,
    ) -> Result<ConnectStart, AdapterError> {
        connect::keep_legacy_connection_set_up(cx, self).await?;
        let surface = if app_is_usable(cx) {
            SLACK_MEMBER_SURFACE
        } else {
            SLACK_INSTALL_SURFACE
        };
        connect::oauth_connect(cx, "slack", Some(surface), ret).await
    }

    /// Revoke the member's own Slack token and stop their capture, through the
    /// existing disconnects. On a personal account the member is the account, so
    /// the workspace app goes too (`delete_connection("slack")` also drops the
    /// ingest rows and revokes the bot). Under an org the app belongs to the
    /// workspace and stays; only a caller with no sign-in of their own to remove
    /// falls through to the bot row, which that handler leaves to an admin.
    async fn disconnect(&self, cx: &AdapterCx<'_>) -> Result<(), AdapterError> {
        let mut removed = connect::disconnect_oauth(cx, "slack_user").await?;
        if cx.tenant.org_context().is_none() || !removed {
            removed |= connect::disconnect_oauth(cx, "slack").await?;
        }
        connect::any_removed(removed)
    }

    /// The member's channels, from `users.conversations` on their own token (the
    /// bot cannot see what they can). Searched and paged here: Slack's cursors
    /// do not survive a text filter, and 200 channels is one call.
    async fn scope_items(
        &self,
        cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        q: &ScopeItemsQuery,
    ) -> Result<Page<ScopeItem>, AdapterError> {
        let control = control(cx)?;
        let Some(token) = crate::connectors::slack_member_capture::load_member_token(
            cx.state,
            control,
            cx.account_id,
            cx.user_id,
        )
        .await
        else {
            // The sign-in has no usable user token (revoked, or never made).
            return Err(AdapterError::Refused {
                status: 409,
                code: "sign_in_unusable",
            });
        };
        let channels = crate::connectors::slack_member_capture::list_member_channels_detailed(
            &cx.state.http_client,
            &token,
        )
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "slack scope: users.conversations failed");
            AdapterError::Refused {
                status: 502,
                code: "provider_error",
            }
        })?;
        let picked: std::collections::HashSet<String> = sqlx::query_scalar(
            "SELECT channel_id FROM slack_member_channels \
             WHERE account_id = $1 AND user_id = $2 AND selected",
        )
        .bind(cx.account_id)
        .bind(cx.user_id)
        .fetch_all(control)
        .await?
        .into_iter()
        .collect();
        let items = channels
            .into_iter()
            .map(|c| ScopeItem {
                selected: picked.contains(&c.id),
                id: c.id,
                label: c.name,
                meta: c.num_members.map(|n| n.to_string()),
            })
            .collect();
        Ok(scope::paginate(items, q))
    }

    /// "Everything I can see" stores the mode and leaves the picks as they are
    /// (they come back if the member returns to "Only what I pick"). A pick
    /// stores the mode and replaces the picked channels; member capture reads
    /// both (`slack_member_capture`, `scope::Admission`).
    async fn set_scope(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        sel: &ScopeSelection,
    ) -> Result<(), AdapterError> {
        scope::validate(s.scope, sel)?;
        let control = control(cx)?;
        if sel.mode == ScopeMode::Pick {
            let workspace = cx
                .facts
                .token("slack_user")
                .filter(|t| t.mine)
                .or_else(|| cx.facts.token("slack"))
                .and_then(|t| t.provider_workspace_id.clone())
                .ok_or(AdapterError::NotConnected)?;
            scope::write_mode(cx, self, s.key, sel.mode).await?;
            scope::slack_set_selected(
                control,
                cx.account_id,
                cx.user_id,
                &workspace,
                &sel.item_ids,
            )
            .await?;
            return Ok(());
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

    /// The pause gate reads this row at every Slack door (`slack_member_capture`).
    async fn set_paused(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        paused: bool,
    ) -> Result<(), AdapterError> {
        pause::write_paused(cx, self, s.key, paused).await
    }
    async fn start_backfill(
        &self,
        cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        days: u32,
    ) -> Result<(), AdapterError> {
        history::slack::start(cx, days).await
    }
}

fn control<'a>(cx: &'a AdapterCx<'_>) -> Result<&'a sqlx::PgPool, AdapterError> {
    cx.state
        .control_db()
        .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))
}

/// How many channels the member picked. `slack_member_channels` is control-plane
/// routing, not RLS-covered, and filtered here by the caller's own ids.
async fn picked_count(cx: &AdapterCx<'_>) -> Result<u32, AdapterError> {
    let Some(control) = cx.state.control_db() else {
        return Ok(0);
    };
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM slack_member_channels \
         WHERE account_id = $1 AND user_id = $2 AND selected",
    )
    .bind(cx.account_id)
    .bind(cx.user_id)
    .fetch_one(control)
    .await?;
    Ok(n as u32)
}
