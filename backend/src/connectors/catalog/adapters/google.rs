//! Google: Gmail, Calendar, Meet and Chat. One connector over four grant rows
//! (`google` is Calendar's grandfathered literal), connected together through the
//! union consent in `routes::google_workspace_connect`.

use async_trait::async_trait;

use super::{credential_problem, grant_from_token, runtime_from_row};
use crate::connectors::catalog::{
    connect, pause, scope, AdapterCx, AdapterError, ConnectStart, ConnectorAdapter, GrantHealth,
    GrantView, HistorySupport, Page, ParamValue, Problem, ReturnTo, ScopeItem, ScopeItemsQuery,
    ScopeMode, ScopeSelection, ScopeShape, SourceRuntime, SourceSpec,
};

const SOURCES: &[SourceSpec] = &[
    SourceSpec {
        key: "gmail",
        label: "Gmail",
        atom_source_types: &["gmail"],
        scope: ScopeShape::Items { noun: "threads" },
        history: HistorySupport::Unsupported {
            reason: "followed_threads_only",
        },
    },
    SourceSpec {
        key: "calendar",
        label: "Calendar",
        atom_source_types: &["google_calendar"],
        scope: ScopeShape::EverythingOnly,
        history: HistorySupport::Unsupported {
            reason: "automatic_window",
        },
    },
    SourceSpec {
        key: "meet",
        label: "Meet",
        atom_source_types: &["meet"],
        scope: ScopeShape::PickOnly { noun: "meetings" },
        history: HistorySupport::Unsupported {
            reason: "per_meeting",
        },
    },
    SourceSpec {
        key: "chat",
        label: "Chat",
        atom_source_types: &["google_chat"],
        scope: ScopeShape::Items { noun: "spaces" },
        history: HistorySupport::Backfill { max_days: 90 },
    },
];

/// The `oauth_tokens.provider` literal of each source's grant row.
fn grant_provider(source: &str) -> &'static str {
    match source {
        "gmail" => "google_gmail",
        "meet" => "google_meet",
        "chat" => "google_chat",
        _ => "google",
    }
}

const GRANT_PROVIDERS: [&str; 4] = ["google", "google_gmail", "google_meet", "google_chat"];

pub struct GoogleAdapter;

#[async_trait]
impl ConnectorAdapter for GoogleAdapter {
    fn key(&self) -> &'static str {
        "google"
    }

    fn display_name(&self) -> &'static str {
        "Google"
    }

    fn sources(&self) -> &'static [SourceSpec] {
        SOURCES
    }

    fn grant_facts(&self, facts: &super::super::Facts) -> Result<Option<GrantView>, AdapterError> {
        let rows: Vec<_> = GRANT_PROVIDERS
            .iter()
            .filter_map(|p| facts.token(p))
            .collect();
        let Some(first) = rows.iter().min_by_key(|t| t.created_at) else {
            return Ok(None);
        };
        // The sign-in is dead as soon as any of its grants is: the member
        // reconnects once, through the one consent, for all of them.
        let dead = rows.iter().find_map(|t| credential_problem(t));
        let label = rows
            .iter()
            .find_map(|t| t.provider_workspace_id.clone())
            .filter(|l| !l.is_empty());
        let mut grant = grant_from_token(first, label);
        if let Some(p) = dead {
            grant.health = GrantHealth::ReconnectNeeded(p);
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
        let token = cx.facts.token(grant_provider(s.key));
        let mut rt = runtime_from_row(
            cx.facts.source("google", s.key),
            s,
            "google",
            token.is_some(),
        );
        // A Chat grant made before `directory.readonly` shipped.
        if s.key == "chat" {
            if let Some(t) = token {
                if let Some(missing) = crate::connectors::google::chat_missing_scopes(&t.scope) {
                    if !missing.is_empty() {
                        rt.problem = Some(
                            Problem::new("missing_scopes", t.created_at)
                                .for_source(s.key)
                                .with_param("count", ParamValue::Num(missing.len() as i64)),
                        );
                    }
                }
            }
        }
        let sql = match s.key {
            "gmail" => Some(
                "SELECT count(*) FROM gmail_watched_threads \
                 WHERE account_id = $1 AND ingested_by = $2 AND follow_replies",
            ),
            "meet" => Some(
                "SELECT count(*) FROM google_meet_captures \
                 WHERE account_id = $1 AND owner_user_id = $2 AND status <> 'revoked'",
            ),
            "chat" => Some(
                "SELECT count(*) FROM google_chat_watched_spaces \
                 WHERE account_id = $1 AND owner_user_id = $2 AND active",
            ),
            _ => None,
        };
        Ok(match sql {
            Some(sql) => scope::with_member_count(cx, rt, sql).await,
            None => rt,
        })
    }

    async fn start_backfill(
        &self,
        cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        days: u32,
    ) -> Result<(), AdapterError> {
        crate::connectors::catalog::history::google_chat::start(cx, days).await
    }

    /// The existing one-button union consent (`surface=workspace`): one sign-in
    /// that writes a grant row per surface the member ticked and the workspace's
    /// flags allow.
    async fn connect(
        &self,
        cx: &AdapterCx<'_>,
        ret: ReturnTo,
    ) -> Result<ConnectStart, AdapterError> {
        connect::keep_legacy_connection_set_up(cx, self).await?;
        connect::oauth_connect(
            cx,
            "google",
            Some(crate::connectors::google_workspace::WORKSPACE_SURFACE),
            ret,
        )
        .await
    }

    /// The existing disconnect, once per grant row: it revokes each token and
    /// purges that surface's watch channels and subscriptions.
    async fn disconnect(&self, cx: &AdapterCx<'_>) -> Result<(), AdapterError> {
        let mut removed = false;
        for provider in GRANT_PROVIDERS {
            removed |= connect::disconnect_oauth(cx, provider).await?;
        }
        connect::any_removed(removed)
    }

    /// Gmail: followed threads (there is no inbox listing). Chat: the member's
    /// group chats and direct messages, as Google lists them. Meet: the meetings
    /// they selected for capture.
    async fn scope_items(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        q: &ScopeItemsQuery,
    ) -> Result<Page<ScopeItem>, AdapterError> {
        let items = match s.key {
            "gmail" => scope::followed_threads(cx, "google").await?,
            "chat" => chat::items(cx).await?,
            "meet" => meet::items(cx).await?,
            _ => Vec::new(),
        };
        Ok(scope::paginate(items, q))
    }

    /// Gmail: everything turns on the mailbox's everything-mode, a pick turns it
    /// off and keeps only the named followed threads. Chat: Everything opts into
    /// all discoverable member conversations using the same ownership checks
    /// as explicit picks. Meet: a pick keeps the named selections and revokes the
    /// rest. Calendar: everything only.
    async fn set_scope(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        sel: &ScopeSelection,
    ) -> Result<(), AdapterError> {
        scope::validate(s.scope, sel)?;
        let _lock = super::super::everything::lock(cx, self.key(), s.key).await?;
        match (s.key, sel.mode) {
            ("gmail", ScopeMode::Everything) => {
                scope::set_mail_everything_mode(cx, "google", true).await?
            }
            ("gmail", ScopeMode::Pick) => {
                scope::set_mail_everything_mode(cx, "google", false).await?;
                scope::unfollow_threads_not_in(cx, "google", &sel.item_ids).await?;
            }
            ("chat", ScopeMode::Everything) => {
                refresh_everything(cx, s.key).await?;
            }
            ("chat", ScopeMode::Pick) => chat::set_picks(cx, &sel.item_ids).await?,
            ("meet", ScopeMode::Pick) => meet::keep(cx, &sel.item_ids).await?,
            _ => {}
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

    /// The pause gate reads this row at every Google door (`pause`).
    async fn set_paused(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        paused: bool,
    ) -> Result<(), AdapterError> {
        pause::write_paused(cx, self, s.key, paused).await
    }
}

/// The member's own Chat conversations (group chats and direct messages), which
/// they opt into one by one. A shared space is routed by whoever installed the
/// app, an organization act that stays out of this UI (decision 4).
mod chat {
    use axum::extract::{Path, State};
    use axum::Extension;

    use crate::connectors::catalog::{scope, AdapterCx, AdapterError, ScopeItem};
    use crate::routes::google_chat;

    pub async fn items(cx: &AdapterCx<'_>) -> Result<Vec<ScopeItem>, AdapterError> {
        let resp = google_chat::list_spaces(
            State(cx.state.clone()),
            cx.tenant.clone(),
            Extension(cx.user.clone()),
        )
        .await;
        let body = scope::read_json(resp).await?;
        Ok(body["spaces"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|sp| sp.get("mailbox_optin").is_some())
            .filter_map(|sp| {
                let id = sp["space_id"].as_str()?;
                Some(ScopeItem {
                    id: id.to_string(),
                    label: sp["display_name"]
                        .as_str()
                        .filter(|n| !n.is_empty())
                        .unwrap_or(id)
                        .to_string(),
                    meta: sp["space_type"].as_str().map(str::to_string),
                    selected: sp["mailbox_optin"]["opted_in"].as_bool() == Some(true),
                })
            })
            .collect())
    }

    /// Opt in to the named conversations and out of every other opted-in one,
    /// through the routes the older picker calls.
    pub async fn set_picks(cx: &AdapterCx<'_>, ids: &[String]) -> Result<(), AdapterError> {
        let have = items(cx).await?;
        let want: std::collections::HashSet<&str> = ids.iter().map(String::as_str).collect();
        // Reject stale or foreign IDs before removing any existing opt-ins.
        if ids.iter().any(|id| !have.iter().any(|item| &item.id == id)) {
            return Err(AdapterError::Refused {
                status: 422,
                code: "unknown_item",
            });
        }
        for item in have
            .iter()
            .filter(|i| i.selected && !want.contains(i.id.as_str()))
        {
            let resp = google_chat::opt_out_of_mailbox_space(
                State(cx.state.clone()),
                cx.tenant.clone(),
                Extension(cx.user.clone()),
                Path(item.id.clone()),
            )
            .await;
            connect_expect(resp).await?;
        }
        for id in ids {
            match have.iter().find(|i| &i.id == id) {
                Some(i) if i.selected => {}
                Some(_) => {
                    let resp = google_chat::opt_in_to_mailbox_space(
                        State(cx.state.clone()),
                        cx.tenant.clone(),
                        Extension(cx.user.clone()),
                        Path(id.clone()),
                    )
                    .await;
                    connect_expect(resp).await?;
                }
                None => {
                    return Err(AdapterError::Refused {
                        status: 422,
                        code: "unknown_item",
                    })
                }
            }
        }
        Ok(())
    }

    async fn connect_expect(resp: axum::response::Response) -> Result<(), AdapterError> {
        crate::connectors::catalog::connect::expect_ok(resp).await
    }
}

/// Meet: the meetings the member allowed for capture. A meeting is allowed from
/// the meeting itself (or by its code), so the list is what exists.
mod meet {
    use axum::extract::State;
    use axum::Extension;
    use uuid::Uuid;

    use crate::connectors::catalog::{scope, AdapterCx, AdapterError, ScopeItem};
    use crate::routes::google_meet;

    pub async fn items(cx: &AdapterCx<'_>) -> Result<Vec<ScopeItem>, AdapterError> {
        let resp = google_meet::list_captures(
            State(cx.state.clone()),
            cx.tenant.clone(),
            Extension(cx.user.clone()),
        )
        .await;
        let body = scope::read_json(resp).await?;
        Ok(body["captures"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| {
                let id = c["id"].as_str()?;
                let label = ["conference_record_id", "meeting_code", "space_name"]
                    .iter()
                    .find_map(|k| c[*k].as_str().filter(|v| !v.is_empty()))
                    .unwrap_or(id);
                Some(ScopeItem {
                    id: id.to_string(),
                    label: label.to_string(),
                    meta: c["created_at"].as_str().map(str::to_string),
                    selected: true,
                })
            })
            .collect())
    }

    /// Revoke every allowed meeting the pick does not name.
    pub async fn keep(cx: &AdapterCx<'_>, keep: &[String]) -> Result<(), AdapterError> {
        let pool = cx
            .tenant
            .pool
            .as_ref()
            .ok_or_else(|| AdapterError::Database("content plane unavailable".into()))?;
        let keep: std::collections::HashSet<&str> = keep.iter().map(String::as_str).collect();
        for item in items(cx).await? {
            if keep.contains(item.id.as_str()) {
                continue;
            }
            if let Ok(id) = Uuid::parse_str(&item.id) {
                crate::services::google_meet_capture::revoke_by_id(
                    pool,
                    cx.account_id,
                    cx.user_id,
                    id,
                )
                .await
                .map_err(|e| AdapterError::Database(e.to_string()))?;
            }
        }
        Ok(())
    }
}

/// Reconcile discovery without changing the persisted mode.
pub(crate) async fn refresh_everything(
    cx: &AdapterCx<'_>,
    source: &str,
) -> Result<(), AdapterError> {
    let _ = source;
    let ids = chat::items(cx)
        .await?
        .into_iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    chat::set_picks(cx, &ids).await
}
