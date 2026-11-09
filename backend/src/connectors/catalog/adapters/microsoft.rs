//! Microsoft: Outlook mail, Calendar and Teams chats, channels and meetings, all
//! behind one `microsoft` sign-in.

use async_trait::async_trait;

use super::{credential_problem, grant_from_token, runtime_from_row};
use crate::connectors::catalog::{
    connect, pause, scope, AdapterCx, AdapterError, ConnectStart, ConnectorAdapter, GrantHealth,
    GrantView, HistorySupport, Page, Problem, ReturnTo, ScopeItem, ScopeItemsQuery, ScopeMode,
    ScopeSelection, ScopeShape, SourceRuntime, SourceSpec,
};

/// `CredentialDeathReason::MicrosoftConsentRevoked`: Microsoft says nobody has
/// consented to the app for this tenant any more.
const CONSENT_REVOKED: &str = "ms_consent_revoked";

const SOURCES: &[SourceSpec] = &[
    SourceSpec {
        key: "mail",
        label: "Outlook mail",
        atom_source_types: &["outlook"],
        // Pick mode lists followed threads only; there is no inbox listing.
        scope: ScopeShape::Items { noun: "threads" },
        history: HistorySupport::Unsupported {
            reason: "followed_threads_only",
        },
    },
    SourceSpec {
        key: "calendar",
        label: "Calendar",
        atom_source_types: &["microsoft_calendar"],
        scope: ScopeShape::EverythingOnly,
        history: HistorySupport::Unsupported {
            reason: "automatic_window",
        },
    },
    SourceSpec {
        key: "teams_chats",
        label: "Teams chats",
        atom_source_types: &["teams_chat"],
        scope: ScopeShape::Items { noun: "chats" },
        history: HistorySupport::Backfill { max_days: 90 },
    },
    SourceSpec {
        key: "teams_channels",
        label: "Teams channels",
        atom_source_types: &["teams_channel"],
        scope: ScopeShape::Items { noun: "channels" },
        history: HistorySupport::Backfill { max_days: 90 },
    },
    SourceSpec {
        key: "teams_meetings",
        label: "Teams meetings",
        atom_source_types: &["teams_transcript"],
        scope: ScopeShape::PickOnly { noun: "meetings" },
        history: HistorySupport::Unsupported {
            reason: "per_meeting",
        },
    },
];

pub struct MicrosoftAdapter;

#[async_trait]
impl ConnectorAdapter for MicrosoftAdapter {
    fn key(&self) -> &'static str {
        "microsoft"
    }

    fn display_name(&self) -> &'static str {
        "Microsoft"
    }

    fn sources(&self) -> &'static [SourceSpec] {
        SOURCES
    }

    fn grant_facts(&self, facts: &super::super::Facts) -> Result<Option<GrantView>, AdapterError> {
        let Some(row) = facts.token("microsoft") else {
            return Ok(None);
        };
        let mut grant = grant_from_token(row, None);
        // Revoked tenant consent is not fixed by signing in again: approval
        // happens on Microsoft's side. It reads as needs-attention, not reconnect.
        if row.credential_invalid_reason.as_deref() == Some(CONSENT_REVOKED) {
            if let Some(p) = credential_problem(row) {
                grant.health = GrantHealth::NeedsAttention(Problem {
                    code: "tenant_consent_required".to_string(),
                    ..p
                });
            }
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
        let rt = runtime_from_row(cx.facts.source("microsoft", s.key), s, "microsoft", true);
        let sql = match s.key {
            "mail" => {
                "SELECT count(*) FROM outlook_watched_conversations \
                 WHERE account_id = $1 AND ingested_by = $2 AND follow_replies"
            }
            "teams_chats" => {
                "SELECT count(*) FROM teams_watched_resources \
                 WHERE account_id = $1 AND user_id = $2 AND kind = 'chat'"
            }
            "teams_channels" => {
                "SELECT count(*) FROM teams_watched_resources \
                 WHERE account_id = $1 AND user_id = $2 AND kind = 'channel'"
            }
            "teams_meetings" => {
                "SELECT count(*) FROM teams_meeting_captures \
                 WHERE account_id = $1 AND user_id = $2 AND revoked_at IS NULL"
            }
            _ => return Ok(rt),
        };
        Ok(scope::with_member_count(cx, rt, sql).await)
    }

    /// The member's own sign-in through `GET /v1/oauth/microsoft/authorize`,
    /// never the tenant admin-consent route. If the tenant needs an admin's
    /// approval, Microsoft asks for it on its side.
    async fn connect(
        &self,
        cx: &AdapterCx<'_>,
        ret: ReturnTo,
    ) -> Result<ConnectStart, AdapterError> {
        connect::keep_legacy_connection_set_up(cx, self).await?;
        connect::oauth_connect(cx, "microsoft", None, ret).await
    }

    /// The existing Microsoft disconnect: hands the Teams doorbell on, removes
    /// the token and stops the member's watches and subscriptions.
    async fn disconnect(&self, cx: &AdapterCx<'_>) -> Result<(), AdapterError> {
        connect::any_removed(connect::disconnect_oauth(cx, "microsoft").await?)
    }

    /// What the member can pick: followed threads (Outlook has no inbox
    /// listing), their Teams chats or channels as Microsoft lists them, or the
    /// meetings they selected for capture. `selected` is the member's own watch
    /// or capture.
    async fn scope_items(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        q: &ScopeItemsQuery,
    ) -> Result<Page<ScopeItem>, AdapterError> {
        let items = match s.key {
            "mail" => scope::followed_threads(cx, "microsoft").await?,
            "teams_chats" => teams::items(cx, teams::Kind::Chat).await?,
            "teams_channels" => teams::items(cx, teams::Kind::Channel).await?,
            "teams_meetings" => teams::meeting_items(cx).await?,
            _ => Vec::new(),
        };
        Ok(scope::paginate(items, q))
    }

    /// Outlook: everything turns on the mailbox's everything-mode, a pick turns
    /// it off and keeps only the named followed threads. Teams chats and
    /// channels: the pick is the member's watch list (a new watch backfills like
    /// the older picker does); Everything watches all discoverable items. Meetings: a pick keeps the named selections and
    /// revokes the rest; a meeting is added from the meeting itself.
    /// Calendar: everything only.
    async fn set_scope(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
        sel: &ScopeSelection,
    ) -> Result<(), AdapterError> {
        scope::validate(s.scope, sel)?;
        let _lock = super::super::everything::lock(cx, self.key(), s.key).await?;
        match (s.key, sel.mode) {
            ("mail", ScopeMode::Everything) => {
                scope::set_mail_everything_mode(cx, "microsoft", true).await?
            }
            ("mail", ScopeMode::Pick) => {
                scope::set_mail_everything_mode(cx, "microsoft", false).await?;
                scope::unfollow_threads_not_in(cx, "microsoft", &sel.item_ids).await?;
            }
            ("teams_chats" | "teams_channels", ScopeMode::Everything) => {
                refresh_everything(cx, s.key).await?;
            }
            ("teams_chats", ScopeMode::Pick) => {
                teams::set_picks(cx, teams::Kind::Chat, &sel.item_ids).await?
            }
            ("teams_channels", ScopeMode::Pick) => {
                teams::set_picks(cx, teams::Kind::Channel, &sel.item_ids).await?
            }
            ("teams_meetings", ScopeMode::Pick) => teams::keep_meetings(cx, &sel.item_ids).await?,
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

    /// The pause gate reads this row at every Microsoft door (`pause`).
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
        source: &SourceSpec,
        days: u32,
    ) -> Result<(), AdapterError> {
        crate::connectors::catalog::history::teams::start(cx, source.key, days).await
    }
}

/// Teams chats, channels and meetings through the existing picker routes.
mod teams {
    use axum::extract::{Path, Query, State};
    use axum::{Extension, Json};
    use uuid::Uuid;

    use crate::connectors::catalog::{scope, AdapterCx, AdapterError, ScopeItem};
    use crate::routes::teams_ingest;

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum Kind {
        Chat,
        Channel,
    }

    impl Kind {
        fn wire(self) -> &'static str {
            match self {
                Self::Chat => "chat",
                Self::Channel => "channel",
            }
        }
    }

    /// `chat:{chat_id}` or `channel:{team_id}:{channel_id}`: ids are opaque to
    /// the web and parsed only here.
    fn item_id(kind: Kind, team: &str, channel: &str, chat: &str) -> String {
        match kind {
            Kind::Chat => format!("chat:{chat}"),
            Kind::Channel => format!("channel:{team}:{channel}"),
        }
    }

    /// The member's watches of one kind: `(item id, watch row id)`.
    async fn watched(cx: &AdapterCx<'_>, kind: Kind) -> Result<Vec<(String, Uuid)>, AdapterError> {
        let resp = teams_ingest::list_watched(
            State(cx.state.clone()),
            cx.tenant.clone(),
            Extension(cx.user.clone()),
        )
        .await;
        let body = scope::read_json(resp).await?;
        Ok(body["watched"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|w| w["kind"].as_str() == Some(kind.wire()))
            .filter_map(|w| {
                let id = Uuid::parse_str(w["id"].as_str()?).ok()?;
                let item = item_id(
                    kind,
                    w["team_id"].as_str().unwrap_or_default(),
                    w["channel_id"].as_str().unwrap_or_default(),
                    w["chat_id"].as_str().unwrap_or_default(),
                );
                Some((item, id))
            })
            .collect())
    }

    /// Everything the member could pick of one kind, as Microsoft lists it.
    async fn live(cx: &AdapterCx<'_>, kind: Kind) -> Result<Vec<ScopeItem>, AdapterError> {
        let mut out = Vec::new();
        match kind {
            Kind::Chat => {
                let resp = teams_ingest::list_chats(
                    State(cx.state.clone()),
                    cx.tenant.clone(),
                    Extension(cx.user.clone()),
                )
                .await;
                let body = scope::read_json(resp).await?;
                for c in body["chats"].as_array().into_iter().flatten() {
                    let Some(id) = c["id"].as_str() else { continue };
                    out.push(ScopeItem {
                        id: item_id(kind, "", "", id),
                        label: c["display_name"]
                            .as_str()
                            .filter(|n| !n.is_empty())
                            .unwrap_or(id)
                            .to_string(),
                        meta: c["chat_type"].as_str().map(str::to_string),
                        selected: false,
                    });
                }
            }
            Kind::Channel => {
                let resp = teams_ingest::list_teams(
                    State(cx.state.clone()),
                    cx.tenant.clone(),
                    Extension(cx.user.clone()),
                )
                .await;
                let teams = scope::read_json(resp).await?;
                for t in teams["teams"].as_array().into_iter().flatten() {
                    let Some(team) = t["id"].as_str() else {
                        continue;
                    };
                    let team_name = t["display_name"].as_str().unwrap_or(team);
                    let resp = teams_ingest::list_channels(
                        State(cx.state.clone()),
                        cx.tenant.clone(),
                        Extension(cx.user.clone()),
                        Query(teams_ingest::ChannelsParams::new(team)),
                    )
                    .await;
                    let channels = scope::read_json(resp).await?;
                    for c in channels["channels"].as_array().into_iter().flatten() {
                        let Some(id) = c["id"].as_str() else { continue };
                        out.push(ScopeItem {
                            id: item_id(kind, team, id, ""),
                            label: format!(
                                "{team_name} / {}",
                                c["display_name"].as_str().unwrap_or(id)
                            ),
                            meta: c["membership_type"].as_str().map(str::to_string),
                            selected: false,
                        });
                    }
                }
            }
        }
        Ok(out)
    }

    pub async fn items(cx: &AdapterCx<'_>, kind: Kind) -> Result<Vec<ScopeItem>, AdapterError> {
        let picked: std::collections::HashSet<String> = watched(cx, kind)
            .await?
            .into_iter()
            .map(|(i, _)| i)
            .collect();
        let mut items = live(cx, kind).await?;
        for i in &mut items {
            i.selected = picked.contains(&i.id);
        }
        Ok(items)
    }

    /// Make the member's watches of this kind exactly `ids`: watch the new ones
    /// (through the picker's own opt-in, which backfills), stop watching the
    /// rest (through its own delete, which also drops the doorbell).
    pub async fn set_picks(
        cx: &AdapterCx<'_>,
        kind: Kind,
        ids: &[String],
    ) -> Result<(), AdapterError> {
        let have = watched(cx, kind).await?;
        // Validate the full replacement before deleting any existing watches.
        let live = live(cx, kind).await?;
        if ids.iter().any(|id| !live.iter().any(|item| &item.id == id)) {
            return Err(AdapterError::Refused {
                status: 422,
                code: "unknown_item",
            });
        }
        let want: std::collections::HashSet<&str> = ids.iter().map(String::as_str).collect();
        for (item, row) in &have {
            if !want.contains(item.as_str()) {
                let resp = teams_ingest::delete_watched(
                    State(cx.state.clone()),
                    cx.tenant.clone(),
                    Extension(cx.user.clone()),
                    Path(*row),
                )
                .await;
                crate::connectors::catalog::connect::expect_ok(resp).await?;
            }
        }
        let have: std::collections::HashSet<&str> = have.iter().map(|(i, _)| i.as_str()).collect();
        let new: Vec<&String> = ids.iter().filter(|i| !have.contains(i.as_str())).collect();
        if new.is_empty() {
            return Ok(());
        }
        for id in new {
            // Only what Microsoft lists for this member can be watched.
            let Some(found) = live.iter().find(|i| &i.id == id) else {
                return Err(AdapterError::Refused {
                    status: 422,
                    code: "unknown_item",
                });
            };
            let mut parts = id.splitn(3, ':').skip(1);
            let body = match kind {
                Kind::Chat => teams_ingest::IngestTeamsRequest::chat(
                    id.strip_prefix("chat:").unwrap_or_default(),
                    &found.label,
                ),
                Kind::Channel => teams_ingest::IngestTeamsRequest::channel(
                    parts.next().unwrap_or_default(),
                    parts.next().unwrap_or_default(),
                    &found.label,
                ),
            };
            let resp = teams_ingest::ingest_resource(
                State(cx.state.clone()),
                cx.tenant.clone(),
                Extension(cx.user.clone()),
                Json(body),
            )
            .await;
            crate::connectors::catalog::connect::expect_ok(resp).await?;
        }
        Ok(())
    }

    /// The meetings the member selected for capture. Picking a meeting happens
    /// from the meeting (the consent page), so this lists what exists.
    pub async fn meeting_items(cx: &AdapterCx<'_>) -> Result<Vec<ScopeItem>, AdapterError> {
        // Mutation reconciliation must enumerate the authoritative owned ledger,
        // including when the stored page model is absent, stale or withheld.
        let mut tx = cx.tenant.begin_member_scoped(cx.account_id).await?;
        // The personal opener binds the account; owner-ledger RLS also needs
        // the already verified actor, exactly as on the org member opener.
        crate::tenancy::scope::set_tenant_user(&mut tx, cx.user_id).await?;
        let rows: Vec<(Uuid, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT c.id,c.meeting_id,c.created_at FROM teams_meeting_captures c \
             JOIN graphs g ON g.id=c.graph_id WHERE c.account_id=$1 AND c.user_id=$2 \
             AND c.revoked_at IS NULL AND g.account_id=$1 AND g.archived_at IS NULL \
             AND ((g.kind='member' AND g.owner_user_id=$2) \
                  OR (g.kind='personal' AND g.owner_user_id IS NULL)) ORDER BY c.created_at DESC,c.id",
        ).bind(cx.account_id).bind(cx.user_id).fetch_all(&mut *tx).await?;
        tx.rollback().await?;
        Ok(rows
            .into_iter()
            .map(|(id, label, at)| ScopeItem {
                id: id.to_string(),
                label,
                meta: Some(at.to_rfc3339()),
                selected: true,
            })
            .collect())
    }

    /// Revoke every selected meeting the pick does not name.
    pub async fn keep_meetings(cx: &AdapterCx<'_>, keep: &[String]) -> Result<(), AdapterError> {
        let pool = cx
            .tenant
            .pool
            .as_ref()
            .ok_or_else(|| AdapterError::Database("content plane unavailable".into()))?;
        let keep: std::collections::HashSet<&str> = keep.iter().map(String::as_str).collect();
        for item in meeting_items(cx).await? {
            if keep.contains(item.id.as_str()) {
                continue;
            }
            if let Ok(id) = Uuid::parse_str(&item.id) {
                crate::services::teams_meeting_capture::revoke_owned(
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
    let kind = if source == "teams_chats" {
        teams::Kind::Chat
    } else {
        teams::Kind::Channel
    };
    let ids = teams::items(cx, kind)
        .await?
        .into_iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    teams::set_picks(cx, kind, &ids).await
}
