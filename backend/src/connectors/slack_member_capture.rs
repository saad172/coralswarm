//! Slack per-member capture (Phase G wave 3, item 46b / S8).
//!
//! `slack_user` tokens already exist (CS-454). This module is what reads
//! them: a control-plane index of channels the member can see, filled from
//! `users.conversations` on that member's own xoxp token, and a doorbell
//! path for workspace message events. The workspace event is one notification
//! per app; each indexed member is a separate ingest target. Fetches use
//! that member's live token. A disconnected member receives no event body,
//! even if a stale channel-index row remains.
//!
//! This does not call [`crate::connectors::slack::resolve_org_fanout_destinations`]
//! or [`crate::connectors::slack::ChannelDestination`]. Those stay for item 48
//! to delete.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::ingest::atom::IngestAtomRequest;
use crate::routes::slack::{resolve_primary_graph, AccountPlane, IngestCaller};
use crate::store::PostgresStore;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::{TenancyDecision, TenantContext};
use crate::AppState;

use super::catalog::scope::{self, Admission};
use super::slack::slack_api_base;
use super::slack_message_text::admit_slack_message;
use super::TokenOwner;

/// Same page size `users.conversations` and `conversations.history` already
/// use in this repo (`connectors::slack`, `connectors::history_backfill`).
pub(crate) const MEMBER_HISTORY_PAGE: &str = "200";

/// Channels pulled in one worker tick, so one member's backlog cannot starve
/// the rest. Each channel is one page of [`MEMBER_HISTORY_PAGE`].
const BACKFILL_CHANNELS_PER_TICK: i64 = 20;

/// SQL predicate: `$user` is a current member of `$acct`. An org account
/// needs an `account_members` row; a personal account (`clerk_org_id IS
/// NULL`) counts its own owner. Same rule as control migration 0282's
/// `resolve_slack_user_token_owners`. A member who leaves an org keeps a live
/// `slack_user` row until something revokes it, so every read in this module
/// that picks a member (and with it, a token or an ocean) applies this.
/// `account_members` and `accounts` carry no RLS, so this works both inside
/// `begin_account_scoped` and on the bare control pool.
macro_rules! owner_is_current_member_sql {
    ($acct:literal, $user:literal) => {
        concat!(
            "(EXISTS (SELECT 1 FROM account_members am \
                      WHERE am.account_id = ", $acct, " AND am.user_id = ", $user, ") \
              OR EXISTS (SELECT 1 FROM accounts a \
                         WHERE a.id = ", $acct, " AND a.clerk_org_id IS NULL \
                           AND a.owner_user_id = ", $user, "))"
        )
    };
}

/// Whether `user_id` is still a member of `account_id` (see
/// `owner_is_current_member_sql!`). A lookup error is returned, not folded
/// into `false`: callers must not treat "could not check" as either answer.
pub(crate) async fn owner_is_current_member(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(concat!("SELECT ", owner_is_current_member_sql!("$1", "$2")))
        .bind(account_id)
        .bind(user_id)
        .fetch_one(control)
        .await
}

/// Recheck a member grant in its RLS account scope before using a token
/// retained by an asynchronous connect/backfill task.
pub(crate) async fn member_grant_is_live(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    workspace_id: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let live: bool = sqlx::query_scalar(concat!(
        "SELECT EXISTS(SELECT 1 FROM oauth_tokens ot WHERE ot.account_id=$1 \
         AND ot.owner_user_id=$2 AND ot.provider='slack_user' AND ot.provider_workspace_id=$3 \
         AND ot.credential_invalid_at IS NULL AND ",
        owner_is_current_member_sql!("ot.account_id", "ot.owner_user_id"),
        ")"
    ))
    .bind(account_id).bind(user_id).bind(workspace_id)
    .fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(live)
}

/// One connected member whose index row says they can read this channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MemberCaptureTarget {
    pub account_id: Uuid,
    pub user_id: Uuid,
}

/// Ingest targets for one workspace channel. Members not in
/// `slack_member_channels` are absent, and so are index rows whose owner has
/// since left the account or disconnected their Slack grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemberCaptureTargets {
    pub targets: Vec<MemberCaptureTarget>,
}

/// What we know about the message body for one member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemberFetch {
    /// No `slack_user` token to re-read with. The event payload is the only copy.
    NotAttempted,
    /// The member's token could not read this message (missing, mismatched ts,
    /// or Slack refused). Do not fall back to the event body.
    Unreadable,
    /// Returned by `conversations.history` on this member's token.
    FromMemberToken { ts: String, text: String },
}

/// The body to write into this member's ocean, or nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemberMessageBody {
    FromMemberToken(String),
    FromEventPayload(String),
}

/// Indexed members get a body. A member-token fetch is used when it is the
/// same message (`ts`). The event payload is used only when no fetch was
/// attempted. A failed or mismatched fetch does not fall back to the payload.
pub(crate) fn message_for_member(
    indexed: bool,
    event_ts: &str,
    event_text: &str,
    fetch: MemberFetch,
) -> Option<MemberMessageBody> {
    if !indexed || event_ts.is_empty() {
        return None;
    }
    match fetch {
        MemberFetch::FromMemberToken { ts, text } if ts == event_ts => {
            Some(MemberMessageBody::FromMemberToken(text))
        }
        MemberFetch::FromMemberToken { .. } | MemberFetch::Unreadable => None,
        MemberFetch::NotAttempted => Some(MemberMessageBody::FromEventPayload(event_text.to_string())),
    }
}

pub(crate) async fn member_capture_targets(
    pool: &PgPool,
    workspace_id: &str,
    channel_id: &str,
) -> Result<MemberCaptureTargets, sqlx::Error> {
    let rows: Vec<(Uuid, Uuid, bool)> = sqlx::query_as(concat!(
        "SELECT c.account_id, c.user_id, c.selected FROM slack_member_channels c \
         WHERE c.workspace_id = $1 AND c.channel_id = $2 AND ",
        owner_is_current_member_sql!("c.account_id", "c.user_id"),
        " ORDER BY c.user_id"
    ))
    .bind(workspace_id)
    .bind(channel_id)
    .fetch_all(pool)
    .await?;
    // The channel index can outlive a grant if an in-flight refresh finishes
    // after disconnect. Check the credential in its account scope: unlike
    // the index, oauth_tokens is RLS-covered and cannot be read unscoped.
    //
    // The same scoped read carries the member's scope and pause state
    // (`connector_sources`, RLS-covered too): a member who paused or turned off
    // Slack, or who picked channels and did not pick this one, is not a target.
    // A member with no row keeps taking every channel (`scope::Admission`).
    let accounts: std::collections::BTreeSet<Uuid> = rows.iter().map(|(id, _, _)| *id).collect();
    let mut connected = std::collections::HashSet::new();
    let mut admission: std::collections::HashMap<(Uuid, Uuid), Admission> = Default::default();
    for account_id in accounts {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let owners: Vec<Uuid> = sqlx::query_scalar(
            "SELECT owner_user_id FROM oauth_tokens \
             WHERE account_id=$1 AND provider='slack_user' AND provider_workspace_id=$2 \
               AND owner_user_id IS NOT NULL AND credential_invalid_at IS NULL",
        )
        .bind(account_id)
        .bind(workspace_id)
        .fetch_all(&mut *tx)
        .await?;
        let stored = scope::slack_rows_in(&mut tx, account_id).await?;
        tx.commit().await?;
        connected.extend(owners.into_iter().map(|owner| (account_id, owner)));
        for (user, row) in stored {
            admission.insert((account_id, user), Admission::of(Some(row)));
        }
    }
    Ok(MemberCaptureTargets {
        targets: rows
            .into_iter()
            .filter(|(account_id, user_id, _)| connected.contains(&(*account_id, *user_id)))
            .filter(|(account_id, user_id, selected)| {
                admission
                    .get(&(*account_id, *user_id))
                    .copied()
                    .unwrap_or(Admission::of(None))
                    .takes(*selected)
            })
            .map(|(account_id, user_id, _)| MemberCaptureTarget { account_id, user_id })
            .collect(),
    })
}

/// `member_joined_channel` / `member_left_channel`. A join indexes only
/// `slack_user` rows that are live AND whose owner is still a member of the
/// account (`credential_live`, 0282). A leave removes the index row whatever
/// the token's state or the owner's membership.
///
/// Known gap, by design: a channel joined while the owner was NOT a member is
/// never indexed for them, and nothing replays that join. If they later
/// rejoin the account, those channels stay un-indexed until their next Slack
/// connect, whose `refresh_and_backfill_on_connect` rebuilds the full index
/// from `users.conversations`.
///
/// The event carries only the Slack workspace and user ids, never an
/// account id, so `oauth_tokens` (RLS-covered, 0083) cannot be read under a
/// `begin_account_scoped` transaction here. Owners come from the control-only
/// SECURITY DEFINER `resolve_slack_user_token_owners(text, text)` (migrations
/// 0281, 0282), which returns just `(account_id, owner_user_id,
/// credential_live)` for that one Slack user in that one workspace. `slack_member_channels`
/// itself has no RLS (0256).
pub(crate) async fn apply_membership_event(
    pool: &PgPool,
    workspace_id: &str,
    channel_id: &str,
    slack_user_id: &str,
    joined: bool,
) -> Result<(), sqlx::Error> {
    if workspace_id.is_empty() || channel_id.is_empty() || slack_user_id.is_empty() {
        return Ok(());
    }
    if joined {
        sqlx::query(
            "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) \
             SELECT r.account_id, r.owner_user_id, $1, $2 \
             FROM resolve_slack_user_token_owners($1, $3) r \
             WHERE r.credential_live \
             ON CONFLICT (account_id, user_id, workspace_id, channel_id) DO NOTHING",
        )
        .bind(workspace_id)
        .bind(channel_id)
        .bind(slack_user_id)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(
            "DELETE FROM slack_member_channels c \
             USING resolve_slack_user_token_owners($1, $3) r \
             WHERE r.owner_user_id = c.user_id \
               AND r.account_id = c.account_id \
               AND c.workspace_id = $1 \
               AND c.channel_id = $2",
        )
        .bind(workspace_id)
        .bind(channel_id)
        .bind(slack_user_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Replace this member's channel index for one workspace from a
/// `users.conversations` result. Existing cursors on channels that remain
/// are left alone, and no other member's row is written.
pub(crate) async fn sync_member_channels(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    workspace_id: &str,
    channel_ids: &[String],
) -> Result<(), sqlx::Error> {
    for channel_id in channel_ids {
        sqlx::query(
            "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (account_id, user_id, workspace_id, channel_id) \
             DO UPDATE SET updated_at = now()",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(workspace_id)
        .bind(channel_id)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "DELETE FROM slack_member_channels \
         WHERE account_id = $1 AND user_id = $2 AND workspace_id = $3 \
           AND NOT (channel_id = ANY($4))",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(workspace_id)
    .bind(channel_ids)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug)]
pub(crate) struct HistoryPage {
    pub messages: Vec<Value>,
    pub next_cursor: Option<String>,
}

/// One `conversations.history` page on `member_token`. The bearer token is
/// the member's xoxp, never a workspace bot token.
pub(crate) async fn fetch_history_page(
    http: &reqwest::Client,
    member_token: &str,
    channel_id: &str,
    cursor: Option<&str>,
) -> Result<HistoryPage, String> {
    let mut query = vec![
        ("channel", channel_id.to_string()),
        ("limit", MEMBER_HISTORY_PAGE.to_string()),
    ];
    if let Some(cursor) = cursor.filter(|c| !c.is_empty()) {
        query.push(("cursor", cursor.to_string()));
    }
    let resp = http
        .get(format!("{}/conversations.history", slack_api_base()))
        .bearer_auth(member_token)
        .query(&query)
        .send()
        .await
        .map_err(|e| format!("conversations.history transport: {e}"))?;
    let status = resp.status();
    if status.as_u16() == 403 || status.as_u16() == 404 {
        return Err(format!("conversations.history {status}"));
    }
    let json: Value = resp.json().await.map_err(|e| format!("conversations.history parse: {e}"))?;
    if json.get("ok").and_then(Value::as_bool) != Some(true) {
        let err = json.get("error").and_then(Value::as_str).unwrap_or("unknown");
        return Err(format!("conversations.history Slack error: {err}"));
    }
    let messages = json.get("messages").and_then(Value::as_array).cloned().unwrap_or_default();
    let next_cursor = json
        .get("response_metadata")
        .and_then(|m| m.get("next_cursor"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Ok(HistoryPage { messages, next_cursor })
}

/// The single message at `ts`, read with the member's token. `Ok(None)` when
/// Slack did not return that ts (the member cannot be shown to have it).
pub(crate) async fn fetch_message_at(
    http: &reqwest::Client,
    member_token: &str,
    channel_id: &str,
    ts: &str,
) -> Result<Option<(String, String)>, String> {
    let resp = http
        .get(format!("{}/conversations.history", slack_api_base()))
        .bearer_auth(member_token)
        .query(&[
            ("channel", channel_id),
            ("latest", ts),
            ("oldest", ts),
            ("inclusive", "true"),
            ("limit", "1"),
        ])
        .send()
        .await
        .map_err(|e| format!("conversations.history transport: {e}"))?;
    let status = resp.status();
    if status.as_u16() == 403 || status.as_u16() == 404 {
        return Ok(None);
    }
    let json: Value = resp.json().await.map_err(|e| format!("conversations.history parse: {e}"))?;
    if json.get("ok").and_then(Value::as_bool) != Some(true) {
        return Ok(None);
    }
    let msg = json
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|a| a.first());
    let Some(msg) = msg else { return Ok(None) };
    let got_ts = msg.get("ts").and_then(Value::as_str).unwrap_or("");
    if got_ts != ts {
        return Ok(None);
    }
    let text = msg.get("text").and_then(Value::as_str).unwrap_or("").to_string();
    Ok(Some((got_ts.to_string(), text)))
}

/// `users.conversations` on the member's xoxp token (no `user=` param — the
/// token already is that member). Public channels only: member scopes are
/// `channels:read`, and Slack fails the whole `types` list when a requested
/// type lacks a scope.
pub(crate) async fn list_member_channels(
    http: &reqwest::Client,
    member_token: &str,
) -> Result<Vec<String>, String> {
    Ok(list_member_channels_detailed(http, member_token)
        .await?
        .into_iter()
        .map(|c| c.id)
        .collect())
}

/// A channel the member can read, as `users.conversations` describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemberChannel {
    pub id: String,
    pub name: String,
    pub num_members: Option<u32>,
}

/// [`list_member_channels`] with names, for the scope picker.
pub(crate) async fn list_member_channels_detailed(
    http: &reqwest::Client,
    member_token: &str,
) -> Result<Vec<MemberChannel>, String> {
    const MAX_PAGES: usize = 50;
    let mut channels_out = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut query = vec![
            ("types", "public_channel".to_string()),
            ("exclude_archived", "true".to_string()),
            ("limit", MEMBER_HISTORY_PAGE.to_string()),
        ];
        if let Some(c) = &cursor {
            query.push(("cursor", c.clone()));
        }
        let resp = http
            .get(format!("{}/users.conversations", slack_api_base()))
            .bearer_auth(member_token)
            .query(&query)
            .send()
            .await
            .map_err(|e| format!("users.conversations transport: {e}"))?;
        let json: Value = resp.json().await.map_err(|e| format!("users.conversations parse: {e}"))?;
        if json.get("ok").and_then(Value::as_bool) != Some(true) {
            let err = json.get("error").and_then(Value::as_str).unwrap_or("unknown");
            return Err(format!("users.conversations Slack error: {err}"));
        }
        if let Some(channels) = json.get("channels").and_then(Value::as_array) {
            for c in channels {
                if let Some(id) = c.get("id").and_then(Value::as_str) {
                    channels_out.push(MemberChannel {
                        id: id.to_string(),
                        name: c.get("name").and_then(Value::as_str).unwrap_or(id).to_string(),
                        num_members: c.get("num_members").and_then(Value::as_u64).map(|n| n as u32),
                    });
                }
            }
        }
        cursor = json
            .get("response_metadata")
            .and_then(|m| m.get("next_cursor"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    Ok(channels_out)
}

/// Why [`refresh_and_backfill_on_connect`] did not refresh the index.
#[derive(Debug)]
pub(crate) enum ConnectRefreshError {
    /// The connecting user is not (or is no longer) a member of the account.
    /// A membership outcome, not a fault.
    NotMember,
    /// A lookup, Slack call or index write failed.
    Failed(String),
}

impl std::fmt::Display for ConnectRefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotMember => f.write_str("slack_user owner is not a current member of the account"),
            Self::Failed(e) => f.write_str(e),
        }
    }
}

pub(crate) async fn refresh_and_backfill_on_connect(
    state: &Arc<AppState>,
    http: &reqwest::Client,
    control: &PgPool,
    content: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    workspace_id: &str,
    member_token: &str,
    plane: AccountPlane,
    clerk_org_id: Option<&str>,
) -> Result<(), ConnectRefreshError> {
    match owner_is_current_member(control, account_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return Err(ConnectRefreshError::NotMember),
        Err(e) => return Err(ConnectRefreshError::Failed(format!("membership lookup: {e}"))),
    }
    if !member_grant_is_live(control, account_id, user_id, workspace_id)
        .await.map_err(|e| ConnectRefreshError::Failed(e.to_string()))? {
        return Ok(());
    }
    let channels = list_member_channels(http, member_token)
        .await
        .map_err(ConnectRefreshError::Failed)?;
    if !member_grant_is_live(control, account_id, user_id, workspace_id)
        .await.map_err(|e| ConnectRefreshError::Failed(e.to_string()))? {
        return Ok(());
    }
    sync_member_channels(control, account_id, user_id, workspace_id, &channels)
        .await
        .map_err(|e| ConnectRefreshError::Failed(e.to_string()))?;
    // The index above holds every channel the member can read; only the ones
    // capture admits are read: nothing for a new connection that has picked
    // nothing yet, the picked ones in pick mode, all of them otherwise. Each
    // `backfill_one_channel` re-checks, so a pause between here and there holds.
    let admission = scope::slack_admission(control, account_id, user_id).await;
    let channels: Vec<String> = match admission {
        Admission::Blocked => Vec::new(),
        Admission::Everything => channels,
        Admission::Picked => sqlx::query_scalar(
            "SELECT channel_id FROM slack_member_channels \
             WHERE account_id = $1 AND user_id = $2 AND workspace_id = $3 AND selected",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(workspace_id)
        .fetch_all(control)
        .await
        .map_err(|e| ConnectRefreshError::Failed(e.to_string()))?,
    };
    for channel_id in channels {
        if let Err(e) = backfill_one_channel(
            state,
            http,
            control,
            content,
            account_id,
            user_id,
            workspace_id,
            &channel_id,
            member_token,
            plane,
            clerk_org_id,
        )
        .await
        {
            tracing::warn!(error = %e, %account_id, %channel_id, "slack member backfill page failed");
        }
    }
    Ok(())
}

/// One history page for one indexed channel, using `member_token`, writing
/// the cursor on that row only.
pub(crate) async fn backfill_one_channel(
    state: &Arc<AppState>,
    http: &reqwest::Client,
    control: &PgPool,
    content: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
    member_token: &str,
    plane: AccountPlane,
    clerk_org_id: Option<&str>,
) -> Result<(), String> {
    if !member_grant_is_live(control, account_id, user_id, workspace_id)
        .await.map_err(|e| e.to_string())? {
        return Ok(());
    }
    // The pause gate and pick filter, before any Slack call: a paused or off
    // source, or a channel the member did not pick, is not read at all.
    if !scope::slack_channel_admitted(control, account_id, user_id, workspace_id, channel_id).await {
        return Ok(());
    }
    let cursor: Option<String> = sqlx::query_scalar(
        "SELECT history_cursor FROM slack_member_channels \
         WHERE account_id = $1 AND user_id = $2 AND workspace_id = $3 AND channel_id = $4",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(workspace_id)
    .bind(channel_id)
    .fetch_optional(control)
    .await
    .map_err(|e| e.to_string())?
    .flatten();
    let page = fetch_history_page(http, member_token, channel_id, cursor.as_deref()).await?;
    if !member_grant_is_live(control, account_id, user_id, workspace_id)
        .await.map_err(|e| e.to_string())? {
        return Ok(());
    }

    let graph_id = resolve_primary_graph(content, account_id, plane, IngestCaller::Attributable(user_id))
        .await
        .map_err(|e| e.to_string())?;
    let mut lookups = MemberAnchorLookups::new(state, content, account_id, workspace_id, channel_id, member_token);
    for msg in page.messages {
        let Some(admitted) = admit_slack_message(&msg) else { continue };
        let ts = msg.get("ts").and_then(Value::as_str).unwrap_or("");
        if ts.is_empty() {
            continue;
        }
        let author = msg.get("user").and_then(Value::as_str).unwrap_or("").to_string();
        let anchor = lookups.anchor(ts, &author, admitted.bot_name.as_deref()).await;
        let _ = ingest_member_message(
            state,
            content,
            account_id,
            user_id,
            graph_id,
            plane,
            clerk_org_id,
            workspace_id,
            channel_id,
            ts,
            &admitted.text,
            &author,
            &anchor,
        )
        .await;
    }
    let complete = page.next_cursor.is_none();
    sqlx::query(
        "UPDATE slack_member_channels \
         SET history_cursor = $5, backfill_complete = $6, updated_at = now() \
         WHERE account_id = $1 AND user_id = $2 AND workspace_id = $3 AND channel_id = $4",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(workspace_id)
    .bind(channel_id)
    .bind(page.next_cursor.as_deref())
    .bind(complete)
    .execute(control)
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// A connected member whose `slack_user` token can walk this channel's
/// roster for the invite pass (item 50). Prefer someone indexed in the
/// channel; otherwise any live member token in the workspace. The bot
/// token is not a reader.
///
/// `oauth_tokens` is RLS-covered (0083), so both reads run inside one
/// `begin_account_scoped` transaction on the control pool. A token whose
/// owner has left the account is never a reader.
pub(crate) async fn invite_reader_user_id(
    pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<Option<Uuid>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id).await?;
    let in_channel: Option<Uuid> = sqlx::query_scalar(concat!(
        "SELECT ot.owner_user_id \
         FROM oauth_tokens ot \
         JOIN slack_member_channels c \
           ON c.account_id = ot.account_id \
          AND c.user_id = ot.owner_user_id \
          AND c.workspace_id = ot.provider_workspace_id \
          AND c.channel_id = $3 \
         WHERE ot.account_id = $1 \
           AND ot.provider = 'slack_user' \
           AND ot.provider_workspace_id = $2 \
           AND ot.credential_invalid_at IS NULL \
           AND ot.owner_user_id IS NOT NULL \
           AND ",
        owner_is_current_member_sql!("ot.account_id", "ot.owner_user_id"),
        " ORDER BY ot.updated_at DESC \
         LIMIT 1"
    ))
    .bind(account_id)
    .bind(workspace_id)
    .bind(channel_id)
    .fetch_optional(&mut *tx)
    .await?;
    if in_channel.is_some() {
        tx.rollback().await.ok();
        return Ok(in_channel);
    }
    let any_member: Option<Uuid> = sqlx::query_scalar(concat!(
        "SELECT ot.owner_user_id FROM oauth_tokens ot \
         WHERE ot.account_id = $1 \
           AND ot.provider = 'slack_user' \
           AND ot.provider_workspace_id = $2 \
           AND ot.credential_invalid_at IS NULL \
           AND ot.owner_user_id IS NOT NULL \
           AND ",
        owner_is_current_member_sql!("ot.account_id", "ot.owner_user_id"),
        " ORDER BY ot.updated_at DESC \
         LIMIT 1"
    ))
    .bind(account_id)
    .bind(workspace_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(any_member)
}

/// Test-only seam for `backend/tests/unscoped_oauth_reads_rls_pg.rs`.
#[cfg(feature = "test-support")]
pub async fn test_support_invite_reader_user_id(
    pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<Option<Uuid>, crate::store::StoreError> {
    invite_reader_user_id(pool, account_id, workspace_id, channel_id).await
}

/// Test-only seam for `backend/tests/unscoped_oauth_reads_rls_pg.rs`.
#[cfg(feature = "test-support")]
pub async fn test_support_apply_membership_event(
    pool: &PgPool,
    workspace_id: &str,
    channel_id: &str,
    slack_user_id: &str,
    joined: bool,
) -> Result<(), sqlx::Error> {
    apply_membership_event(pool, workspace_id, channel_id, slack_user_id, joined).await
}

/// Test-only seam for `backend/tests/unscoped_oauth_reads_rls_pg.rs`.
#[cfg(feature = "test-support")]
pub async fn test_support_member_capture_targets(
    pool: &PgPool,
    workspace_id: &str,
    channel_id: &str,
) -> Result<Vec<(Uuid, Uuid)>, sqlx::Error> {
    Ok(member_capture_targets(pool, workspace_id, channel_id)
        .await?
        .targets
        .into_iter()
        .map(|t| (t.account_id, t.user_id))
        .collect())
}

/// Test-only seam for `backend/tests/unscoped_oauth_reads_rls_pg.rs`.
#[cfg(feature = "test-support")]
pub async fn test_support_due_backfill_rows(
    control: &PgPool,
    only_account: Option<Uuid>,
) -> Result<Vec<(Uuid, Uuid, String, String)>, sqlx::Error> {
    due_backfill_rows(control, only_account).await
}

/// Outcome of loading one member's `slack_user` token. Only `NoToken` means
/// "nothing to re-read with"; `NotMember` and `LookupError` mean "do not
/// write anything for this member", and must never be read as `NoToken`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemberToken {
    Token(String),
    /// A current member with no stored `slack_user` credential.
    NoToken,
    /// The owner is not (or is no longer) a member of the account.
    NotMember,
    /// Membership, DEK or credential lookup failed. Unknown, so skip.
    LookupError,
}

impl MemberToken {
    pub(crate) fn into_token(self) -> Option<String> {
        match self {
            Self::Token(t) => Some(t),
            Self::NoToken | Self::NotMember | Self::LookupError => None,
        }
    }
}

/// Membership gate, then `decrypt`. Split from [`load_member_token_outcome`]
/// so the gate can be driven without an `AppState`.
pub(crate) async fn member_token_with<D, DF>(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    decrypt: D,
) -> MemberToken
where
    D: FnOnce() -> DF,
    DF: std::future::Future<Output = Result<Option<String>, String>>,
{
    match owner_is_current_member(control, account_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return MemberToken::NotMember,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, %user_id, "slack member capture: membership lookup failed");
            return MemberToken::LookupError;
        }
    }
    match decrypt().await {
        Ok(Some(token)) => MemberToken::Token(token),
        Ok(None) => MemberToken::NoToken,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, %user_id, "slack member capture: token load failed");
            MemberToken::LookupError
        }
    }
}

/// Every caller (doorbell, backfill worker, unmatched-email roster walk)
/// goes through here, so an ex-member's live token stops here too.
pub(crate) async fn load_member_token_outcome(
    state: &AppState,
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> MemberToken {
    member_token_with(control, account_id, user_id, || async {
        let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, control, account_id)
            .await
            .map_err(|e| e.to_string())?;
        let creds = crate::routes::connectors::load_creds_bg(
            control,
            &dek_plane,
            account_id,
            "slack_user",
            TokenOwner::Member(user_id),
        )
        .await
        .map_err(|e| e.to_string())?;
        // Slack user tokens are used as stored. There is no Slack-side
        // `refresh_if_needed` (Microsoft and Google own that helper); a dead
        // token fails the history call and this member is skipped.
        Ok(creds.map(|c| c.access_token))
    })
    .await
}

/// The token, or `None` for any other outcome. For callers that skip the
/// member either way (backfill worker, unmatched-email roster walk).
pub(crate) async fn load_member_token(
    state: &AppState,
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Option<String> {
    load_member_token_outcome(state, control, account_id, user_id)
        .await
        .into_token()
}

/// The body each indexed member gets for one message event. `load` yields
/// that member's token outcome; `fetch` re-reads the message with a token.
/// A missing token, removed membership, or failed lookup gives no body.
/// This also closes a disconnect between target selection and token load.
pub(crate) async fn member_bodies<L, LF, F, FF>(
    control: &PgPool,
    workspace_id: &str,
    channel_id: &str,
    event_ts: &str,
    event_text: &str,
    load: L,
    fetch: F,
) -> Result<Vec<(MemberCaptureTarget, String)>, sqlx::Error>
where
    L: Fn(MemberCaptureTarget) -> LF,
    LF: std::future::Future<Output = MemberToken>,
    F: Fn(String) -> FF,
    FF: std::future::Future<Output = MemberFetch>,
{
    let targets = member_capture_targets(control, workspace_id, channel_id).await?;
    let mut out = Vec::new();
    for target in targets.targets {
        let fetch = match load(target).await {
            MemberToken::Token(token) => fetch(token).await,
            // No event-payload fallback after disconnect or a failed lookup.
            MemberToken::NoToken | MemberToken::NotMember | MemberToken::LookupError => continue,
        };
        // Disconnect can commit while the Slack history request is in flight.
        if !member_grant_is_live(control, target.account_id, target.user_id, workspace_id).await? {
            continue;
        }
        let Some(body) = message_for_member(true, event_ts, event_text, fetch) else {
            continue;
        };
        let text = match body {
            MemberMessageBody::FromMemberToken(t) | MemberMessageBody::FromEventPayload(t) => t,
        };
        out.push((target, text));
    }
    Ok(out)
}

/// Test-only seam for `backend/tests/unscoped_oauth_reads_rls_pg.rs`. Drives
/// the real [`member_bodies`] and membership gate with fixture credentials.
/// `gate` checks membership and credential existence; a simulated successful
/// member-token fetch returns `event_text`. `before_load` runs before each
/// member's token load to exercise removal/disconnect races.
#[cfg(feature = "test-support")]
pub async fn test_support_member_bodies<H, HF>(
    control: &PgPool,
    gate: &PgPool,
    workspace_id: &str,
    channel_id: &str,
    event_ts: &str,
    event_text: &str,
    before_load: H,
) -> Result<Vec<(Uuid, Uuid, String)>, sqlx::Error>
where
    H: Fn(Uuid, Uuid) -> HF,
    HF: std::future::Future<Output = ()>,
{
    let bodies = member_bodies(
        control,
        workspace_id,
        channel_id,
        event_ts,
        event_text,
        |t| {
            let hook = before_load(t.account_id, t.user_id);
            async move {
                hook.await;
                member_token_with(gate, t.account_id, t.user_id, || async {
                    let mut tx = crate::tenancy::scope::begin_account_scoped(gate, t.account_id)
                        .await.map_err(|e| e.to_string())?;
                    let exists: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM oauth_tokens WHERE account_id=$1 \
                         AND owner_user_id=$2 AND provider='slack_user' AND provider_workspace_id=$3 \
                         AND credential_invalid_at IS NULL)",
                    ).bind(t.account_id).bind(t.user_id).bind(workspace_id)
                        .fetch_one(&mut *tx).await.map_err(|e| e.to_string())?;
                    tx.commit().await.map_err(|e| e.to_string())?;
                    Ok(exists.then(|| "fixture-member-token".to_string()))
                }).await
            }
        },
        |_token| async { MemberFetch::FromMemberToken { ts: event_ts.to_string(), text: event_text.to_string() } },
    )
    .await?;
    Ok(bodies.into_iter().map(|(t, text)| (t.account_id, t.user_id, text)).collect())
}

/// Workspace message doorbell. One ingest per indexed member, each on that
/// member's own graph. Does not touch the org fan-out path.
pub(crate) async fn capture_message_event(
    state: &Arc<AppState>,
    control: &PgPool,
    workspace_id: &str,
    channel_id: &str,
    event_ts: &str,
    event_text: &str,
    author_id: &str,
) {
    // Each member's token, kept for the anchor lookups below (channel and
    // author names, workspace origin), which are cached per process.
    let tokens: std::sync::Mutex<std::collections::HashMap<(Uuid, Uuid), String>> = Default::default();
    let bodies = member_bodies(
        control,
        workspace_id,
        channel_id,
        event_ts,
        event_text,
        |t| {
            let tokens = &tokens;
            async move {
                let outcome = load_member_token_outcome(state, control, t.account_id, t.user_id).await;
                if let MemberToken::Token(token) = &outcome {
                    tokens.lock().unwrap().insert((t.account_id, t.user_id), token.clone());
                }
                outcome
            }
        },
        |token| async move {
            match fetch_message_at(&state.http_client, &token, channel_id, event_ts).await {
                Ok(Some((ts, text))) => MemberFetch::FromMemberToken { ts, text },
                Ok(None) => MemberFetch::Unreadable,
                Err(e) => {
                    tracing::info!(error = %e, "slack member capture: history fetch failed");
                    MemberFetch::Unreadable
                }
            }
        },
    )
    .await;
    let bodies = match bodies {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "slack member capture: target lookup failed");
            return;
        }
    };
    for (target, text) in bodies {
        let plane = match crate::routes::slack::account_plane(control, target.account_id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "slack member capture: plane lookup failed");
                continue;
            }
        };
        let clerk_org_id: Option<String> = sqlx::query_scalar(
            "SELECT clerk_org_id FROM accounts WHERE id = $1",
        )
        .bind(target.account_id)
        .fetch_optional(control)
        .await
        .ok()
        .flatten();
        let content = match plane {
            AccountPlane::Personal => control.clone(),
            AccountPlane::Org => {
                let Some(org) = clerk_org_id.as_deref() else { continue };
                match crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(
                    state, control, org,
                )
                .await
                {
                    Some(p) => p,
                    None => continue,
                }
            }
        };
        let graph_id = match resolve_primary_graph(
            &content,
            target.account_id,
            plane,
            IngestCaller::Attributable(target.user_id),
        )
        .await
        {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(error = %e, user_id = %target.user_id, "slack member capture: primary graph missing");
                continue;
            }
        };
        let token = tokens.lock().unwrap().get(&(target.account_id, target.user_id)).cloned();
        let anchor = match token {
            Some(token) => {
                MemberAnchorLookups::new(state, &content, target.account_id, workspace_id, channel_id, &token)
                    .anchor(event_ts, author_id, None)
                    .await
            }
            None => MemberAnchor::default(),
        };
        let _ = ingest_member_message(
            state,
            &content,
            target.account_id,
            target.user_id,
            graph_id,
            plane,
            clerk_org_id.as_deref(),
            workspace_id,
            channel_id,
            event_ts,
            &text,
            author_id,
            &anchor,
        )
        .await;
    }
}

/// What names a member-captured message for readers and extraction: the
/// channel and author display names and the message permalink. Each is
/// `None` when Slack could not resolve it; a raw id is never stored as a name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MemberAnchor {
    pub channel_name: Option<String>,
    pub author_name: Option<String>,
    pub url: Option<String>,
}

/// Resolves [`MemberAnchor`]s for messages of one channel on one member's
/// own token (`channels:read`, `users:read`; `auth.test` needs no scope).
/// Successful lookups are cached per process (`routes::slack`), and this
/// memo also remembers failures for its own lifetime, so a backfill page
/// costs at most one channel, one workspace and one call per distinct
/// author, even when Slack refuses or rate-limits; never one per message.
/// An app post is named by its bot name; a human by the persistent
/// `connector_identities` directory first (the name the event path
/// canonicalized), then `users.info`.
pub(crate) struct MemberAnchorLookups<'a> {
    state: &'a Arc<AppState>,
    content: &'a PgPool,
    account_id: Uuid,
    workspace_id: &'a str,
    channel_id: &'a str,
    member_token: &'a str,
    channel_name: Option<Option<String>>,
    origin: Option<Option<String>>,
    authors: std::collections::HashMap<String, Option<String>>,
}

impl<'a> MemberAnchorLookups<'a> {
    pub(crate) fn new(
        state: &'a Arc<AppState>,
        content: &'a PgPool,
        account_id: Uuid,
        workspace_id: &'a str,
        channel_id: &'a str,
        member_token: &'a str,
    ) -> Self {
        Self { state, content, account_id, workspace_id, channel_id, member_token, channel_name: None, origin: None, authors: Default::default() }
    }

    pub(crate) async fn anchor(&mut self, ts: &str, author_id: &str, bot_name: Option<&str>) -> MemberAnchor {
        use crate::routes::slack::{message_permalink, resolve_channel_name, resolve_user_name, resolve_workspace_url};
        let http = &self.state.http_client;
        let named = |name: String, id: &str| Some(name).filter(|n| !n.trim().is_empty() && n != id);
        if self.channel_name.is_none() {
            let name = resolve_channel_name(http, self.account_id, self.channel_id, self.member_token).await;
            self.channel_name = Some(named(name, self.channel_id));
        }
        if self.origin.is_none() {
            self.origin = Some(resolve_workspace_url(http, self.account_id, self.workspace_id, self.member_token).await);
        }
        let author_name = match bot_name.map(str::trim).filter(|n| !n.is_empty()) {
            Some(bot) => Some(bot.to_owned()),
            None if author_id.is_empty() => None,
            None => match self.authors.get(author_id) {
                Some(known) => known.clone(),
                None => {
                    let store = PostgresStore::new(self.content.clone());
                    let directory = crate::store::Store::connector_identity_name(&store, "slack", self.workspace_id, author_id)
                        .await
                        .ok()
                        .flatten();
                    let name = match directory {
                        Some(name) => named(name, author_id),
                        None => named(resolve_user_name(http, self.account_id, author_id, self.member_token).await, author_id),
                    };
                    self.authors.insert(author_id.to_owned(), name.clone());
                    name
                }
            },
        };
        let url = self.origin.clone().flatten().and_then(|origin| message_permalink(&origin, self.channel_id, ts, None));
        MemberAnchor { channel_name: self.channel_name.clone().flatten(), author_name, url }
    }
}

#[allow(clippy::too_many_arguments)]
async fn ingest_member_message(
    state: &Arc<AppState>,
    content: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    plane: AccountPlane,
    clerk_org_id: Option<&str>,
    workspace_id: &str,
    channel_id: &str,
    ts: &str,
    text: &str,
    author_id: &str,
    anchor: &MemberAnchor,
) -> Result<bool, String> {
    let user_ctx = UserContext {
        user_id,
        account_id,
        role: "member".into(),
        device_id: String::new(),
    };
    let store: Arc<dyn crate::store::Store> = Arc::new(PostgresStore::new(content.clone()));
    let decision = match plane {
        AccountPlane::Personal => None,
        AccountPlane::Org => {
            let org_id = clerk_org_id
                .ok_or_else(|| "slack member capture: org account with no clerk_org_id".to_string())?
                .to_string();
            Some(TenancyDecision::Org(TenantContext {
                org_id,
                account_id,
                user_id,
                pool: content.clone(),
                page_pool: None,
            }))
        }
    };
    let tenant = Tenant {
        store: Some(store),
        pool: Some(content.clone()),
        decision,
        class: crate::route_class::RouteClass::Other,
    };
    let secs: i64 = ts.split('.').next().unwrap_or("0").parse().unwrap_or(0);
    let timestamp: DateTime<Utc> = DateTime::from_timestamp(secs, 0).unwrap_or_else(Utc::now);
    let req = IngestAtomRequest {
        graph_id,
        source_type: "slack".to_string(),
        source_id: format!("{channel_id}/{ts}"),
        content: text.to_string(),
        participants: vec![author_id.to_string()],
        timestamp,
        scope: None,
        fts_language: None,
        channel_id: Some(channel_id.to_string()),
        channel_name: anchor.channel_name.clone(),
        author_id: Some(author_id.to_string()),
        author_name: anchor.author_name.clone(),
        project: None,
        mcp_client_name: None,
        raw_content: None,
        duration_ms: None,
        segments: None,
        session_id: None,
        session_kind: None,
        session_title: None,
        repo_path: None,
        remote: None,
        hostname: None,
        harness_version: None,
        model: None,
        branch: None,
        agent_name: None,
        platform: None,
        task: None,
        parent_session_id: None,
        trusted_user_agent: None,
        client_info: None,
        code_path: None,
    };
    let origin = crate::source_origin::SourceOrigin::native("slack","message","slack_member_capture")?
        .with_container("channel",channel_id,&format!("slack:workspace:{workspace_id}"),anchor.channel_name.as_deref())?
        .with_oauth_grant(account_id,"slack_user",workspace_id,Some(user_id))?;
    let hints = crate::ingest::capture::CaptureHints { url: anchor.url.clone(), ..Default::default() };
    let resp = crate::ingest::submit(crate::ingest::IngestRequest {
        origin: Some(&origin),
        ..crate::ingest::IngestRequest::private(crate::ingest::Member {
            hints,
            ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
        })
    }).await;
    match resp {
        // An item the door can never store as sent is consumed, as before:
        // retrying it would hold the channel's backfill forever.
        Ok(_) => Ok(true),
        Err(refused) if refused.is_invalid() => Ok(true),
        Err(refused) => Err(format!("ingest_atom {refused}")),
    }
}

/// Rows one scan of the due list reads, and how many scans a tick makes at most.
/// Rows capture does not admit stay due (their cursor never moves), so a tick
/// reads past them rather than letting them crowd the admitted ones out of its
/// [`BACKFILL_CHANNELS_PER_TICK`].
const DUE_SCAN_PAGE: i64 = 100;
const DUE_SCAN_PAGES: i64 = 10;

/// Due rows for one worker tick. `only_account` is the org tenant's account;
/// `None` is the consumer plane (personal accounts — `clerk_org_id` null).
/// Rows whose owner has left the account are not due, and neither are rows the
/// member's pause or pick filter does not admit (`scope::Admission`).
pub(crate) async fn due_backfill_rows(
    control: &PgPool,
    only_account: Option<Uuid>,
) -> Result<Vec<(Uuid, Uuid, String, String)>, sqlx::Error> {
    let mut out: Vec<(Uuid, Uuid, String, String)> = Vec::new();
    let mut admission: std::collections::HashMap<(Uuid, Uuid), Admission> = Default::default();
    for page in 0..DUE_SCAN_PAGES {
        let rows: Vec<(Uuid, Uuid, String, String, bool)> = match only_account {
            Some(account_id) => {
                sqlx::query_as(concat!(
                    "SELECT c.account_id, c.user_id, c.workspace_id, c.channel_id, c.selected \
                     FROM slack_member_channels c \
                     WHERE c.backfill_complete = FALSE AND c.account_id = $1 AND ",
                    owner_is_current_member_sql!("c.account_id", "c.user_id"),
                    " ORDER BY c.updated_at, c.channel_id \
                     LIMIT $2 OFFSET $3"
                ))
                .bind(account_id)
                .bind(DUE_SCAN_PAGE)
                .bind(page * DUE_SCAN_PAGE)
                .fetch_all(control)
                .await?
            }
            None => {
                sqlx::query_as(concat!(
                    "SELECT c.account_id, c.user_id, c.workspace_id, c.channel_id, c.selected \
                     FROM slack_member_channels c \
                     JOIN accounts acct ON acct.id = c.account_id \
                     WHERE c.backfill_complete = FALSE AND acct.clerk_org_id IS NULL AND ",
                    owner_is_current_member_sql!("c.account_id", "c.user_id"),
                    " ORDER BY c.updated_at, c.channel_id \
                     LIMIT $1 OFFSET $2"
                ))
                .bind(DUE_SCAN_PAGE)
                .bind(page * DUE_SCAN_PAGE)
                .fetch_all(control)
                .await?
            }
        };
        let last_page = (rows.len() as i64) < DUE_SCAN_PAGE;
        for (account_id, user_id, workspace_id, channel_id, selected) in rows {
            let adm = match admission.get(&(account_id, user_id)) {
                Some(a) => *a,
                None => {
                    let a = scope::slack_admission(control, account_id, user_id).await;
                    admission.insert((account_id, user_id), a);
                    a
                }
            };
            if adm.takes(selected) {
                out.push((account_id, user_id, workspace_id, channel_id));
                if out.len() as i64 >= BACKFILL_CHANNELS_PER_TICK {
                    return Ok(out);
                }
            }
        }
        if last_page {
            break;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }

    #[test]
    fn payload_is_used_only_when_no_member_fetch_was_attempted() {
        assert!(message_for_member(false, "1.0", "hi", MemberFetch::NotAttempted).is_none());
        assert_eq!(
            message_for_member(true, "1.0", "event body", MemberFetch::NotAttempted),
            Some(MemberMessageBody::FromEventPayload("event body".into()))
        );
        assert_eq!(
            message_for_member(
                true,
                "1.0",
                "event body",
                MemberFetch::FromMemberToken { ts: "1.0".into(), text: "member body".into() },
            ),
            Some(MemberMessageBody::FromMemberToken("member body".into()))
        );
        assert!(message_for_member(
            true,
            "1.0",
            "event body",
            MemberFetch::FromMemberToken { ts: "9.0".into(), text: "other".into() },
        )
        .is_none());
        assert!(message_for_member(true, "1.0", "event body", MemberFetch::Unreadable).is_none());
    }

    #[tokio::test]
    async fn indexed_member_is_a_target_and_another_member_is_not_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping indexed_member_is_a_target_and_another_member_is_not_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g3_slack_targets";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let user_a: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("a-{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let user_b: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("b-{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let account: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id",
        )
        .bind(user_a)
        .fetch_one(&pool)
        .await
        .unwrap();
        // Both are current members; only current members are targets.
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', NULL), ($1, $3, 'member', NULL)",
        )
        .bind(account)
        .bind(user_a)
        .bind(user_b)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO slack_member_channels \
                (account_id, user_id, workspace_id, channel_id, history_cursor) \
             VALUES ($1, $2, 'T1', 'C1', 'cursor-a')",
        )
        .bind(account)
        .bind(user_a)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO slack_member_channels \
                (account_id, user_id, workspace_id, channel_id, history_cursor) \
             VALUES ($1, $2, 'T1', 'C1', 'cursor-b')",
        )
        .bind(account)
        .bind(user_b)
        .execute(&pool)
        .await
        .unwrap();

        for user in [user_a, user_b] {
            sqlx::query("INSERT INTO oauth_tokens(account_id, owner_user_id, provider, scope, access_token, provider_workspace_id) VALUES ($1,$2,'slack_user','channels:read',$3,'T1')")
                .bind(account).bind(user).bind(vec![0u8]).execute(&pool).await.unwrap();
        }
        let targets = member_capture_targets(&pool, "T1", "C1").await.unwrap();
        let ids: Vec<Uuid> = targets.targets.iter().map(|t| t.user_id).collect();
        assert!(ids.contains(&user_a));
        assert!(ids.contains(&user_b));

        sync_member_channels(&pool, account, user_a, "T1", &["C1".into()]).await.unwrap();
        let cursor_b: String = sqlx::query_scalar(
            "SELECT history_cursor FROM slack_member_channels WHERE user_id = $1",
        )
        .bind(user_b)
        .fetch_one(&pool)
        .await
        .unwrap();
        let cursor_a: String = sqlx::query_scalar(
            "SELECT history_cursor FROM slack_member_channels WHERE user_id = $1",
        )
        .bind(user_a)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(cursor_a, "cursor-a");
        assert_eq!(cursor_b, "cursor-b", "refreshing A must not reset B's cursor");

        let only_a = member_capture_targets(&pool, "T1", "C-missing").await.unwrap();
        assert!(only_a.targets.is_empty(), "a member not in the index is not a target");
        let fetch_pool = pool.clone();
        let bodies = member_bodies(
            &pool, "T1", "C1", "1.0", "event body",
            |target| async move { MemberToken::Token(target.user_id.to_string()) },
            |token| {
                let pool = fetch_pool.clone();
                async move {
                    if token == user_a.to_string() {
                        sqlx::query("DELETE FROM oauth_tokens WHERE account_id=$1 AND owner_user_id=$2 AND provider='slack_user'")
                            .bind(account).bind(user_a).execute(&pool).await.unwrap();
                    }
                    MemberFetch::FromMemberToken { ts: "1.0".to_string(), text: "member body".to_string() }
                }
            },
        ).await.unwrap();
        assert_eq!(bodies, vec![(MemberCaptureTarget { account_id: account, user_id: user_b }, "member body".to_string())],
            "a disconnect during the fetch skips only that caller");
        assert!(!member_grant_is_live(&pool, account, user_a, "T1").await.unwrap());
        let targets = member_capture_targets(&pool, "T1", "C1").await.unwrap();
        assert_eq!(targets.targets, vec![MemberCaptureTarget { account_id: account, user_id: user_b }],
            "the disconnected caller's stale channel index is not a capture target");
        pool.close().await;
    }

    #[tokio::test]
    async fn indexed_channel_member_is_targeted_and_unindexed_is_not_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping indexed_channel_member_is_targeted_and_unindexed_is_not_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_g3_slack_index";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let user_a: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("idx-a-{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let user_b: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("idx-b-{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let account: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id",
        )
        .bind(user_a)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) \
             VALUES ($1, $2, 'T9', 'C9')",
        )
        .bind(account)
        .bind(user_a)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO oauth_tokens(account_id, owner_user_id, provider, scope, access_token, provider_workspace_id) VALUES ($1,$2,'slack_user','channels:read',$3,'T9')")
            .bind(account).bind(user_a).bind(vec![0u8]).execute(&pool).await.unwrap();
        let targets = member_capture_targets(&pool, "T9", "C9").await.unwrap();
        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].user_id, user_a);
        assert_ne!(targets.targets[0].user_id, user_b);
        pool.close().await;
    }

    #[tokio::test]
    async fn backfill_history_page_sends_the_member_token() {
        use axum::http::HeaderMap;
        use std::sync::Mutex;

        let seen = Arc::new(Mutex::new(String::new()));
        let seen_limit = Arc::new(Mutex::new(String::new()));
        let app = {
            let seen = seen.clone();
            let seen_limit = seen_limit.clone();
            axum::Router::new().route(
                "/conversations.history",
                axum::routing::get(
                    move |headers: HeaderMap, q: axum::extract::Query<std::collections::HashMap<String, String>>| {
                        let seen = seen.clone();
                        let seen_limit = seen_limit.clone();
                        async move {
                            *seen.lock().unwrap() = headers
                                .get(axum::http::header::AUTHORIZATION)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("")
                                .to_string();
                            *seen_limit.lock().unwrap() = q.get("limit").cloned().unwrap_or_default();
                            axum::Json(serde_json::json!({
                                "ok": true,
                                "messages": [],
                                "response_metadata": { "next_cursor": "" }
                            }))
                        }
                    },
                ),
            )
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok(); });
        let _lock = super::super::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap();
        let _guard = super::super::slack::SlackApiBaseTestGuard::set(&format!("http://{addr}"));
        let http = reqwest::Client::new();
        fetch_history_page(&http, "xoxp-member-alice", "C1", None).await.unwrap();
        assert_eq!(seen.lock().unwrap().as_str(), "Bearer xoxp-member-alice");
        assert_eq!(seen_limit.lock().unwrap().as_str(), MEMBER_HISTORY_PAGE);
    }

    /// A mock Slack for one member backfill page: `conversations.history`
    /// returns `messages`, `conversations.info` names the channel `eng`,
    /// `users.info` names U1 "Dana", and `auth.test` answers for workspace
    /// T1 at `https://acme.slack.com/`. Counts every lookup call.
    async fn spawn_anchor_mock(messages: serde_json::Value) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use axum::{routing::get, Json, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let lookups = Arc::new(AtomicUsize::new(0));
        let count = |n: &Arc<AtomicUsize>| { n.fetch_add(1, Ordering::SeqCst); };
        let (a, b, c) = (lookups.clone(), lookups.clone(), lookups.clone());
        let app = Router::new()
            .route("/conversations.history", get(move || {
                let messages = messages.clone();
                async move { Json(serde_json::json!({"ok": true, "messages": messages, "response_metadata": {"next_cursor": ""}})) }
            }))
            .route("/conversations.info", get(move || { count(&a); async { Json(serde_json::json!({"ok": true, "channel": {"id": "C1", "name": "eng"}})) } }))
            .route("/users.info", get(move || { count(&b); async { Json(serde_json::json!({"ok": true, "user": {"id": "U1", "profile": {"display_name": "Dana", "real_name": "Dana Q"}}})) } }))
            .route("/auth.test", get(move || { count(&c); async { Json(serde_json::json!({"ok": true, "team_id": "T1", "url": "https://acme.slack.com/"})) } }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok(); });
        (format!("http://{addr}"), lookups)
    }

    /// A refused lookup is not retried per message: a page of messages from
    /// one author whose `users.info` and `conversations.info` fail costs one
    /// call each, and no raw id is stored as a name.
    #[tokio::test]
    async fn refused_lookups_cost_one_call_per_page_not_per_message() {
        use axum::{routing::get, Json, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let (a, b, c) = (calls.clone(), calls.clone(), calls.clone());
        let refuse = || Json(serde_json::json!({"ok": false, "error": "ratelimited"}));
        let app = Router::new()
            .route("/conversations.info", get(move || { a.fetch_add(1, Ordering::SeqCst); async move { refuse() } }))
            .route("/users.info", get(move || { b.fetch_add(1, Ordering::SeqCst); async move { refuse() } }))
            .route("/auth.test", get(move || { c.fetch_add(1, Ordering::SeqCst); async move { refuse() } }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok(); });
        let _lock = super::super::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = super::super::slack::SlackApiBaseTestGuard::set(&format!("http://{addr}"));
        // No directory reachable: the lookup falls through to users.info.
        let content = PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .unwrap();
        let state = crate::test_app_state();
        let mut lookups = MemberAnchorLookups::new(&state, &content, Uuid::new_v4(), "T1", "C1", "xoxp-member");
        for ts in ["1729000000.000100", "1729000001.000100", "1729000002.000100"] {
            assert_eq!(lookups.anchor(ts, "U9", None).await, MemberAnchor::default());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3, "one conversations.info, users.info and auth.test for three messages");
        let bot = lookups.anchor("1729000003.000100", "", Some("GitHub")).await;
        assert_eq!(bot.author_name.as_deref(), Some("GitHub"), "an app post is named by its bot name");
    }

    /// Member capture records the anchor extraction reads: the channel and
    /// author display names and the message permalink land in
    /// `source_versions.source_metadata`, and the v2 (author) and v3
    /// (anchor) prompt inputs carry them. Lookups are cached: two messages
    /// from one author in one channel cost one call each.
    #[tokio::test]
    async fn member_backfill_records_channel_author_and_permalink_pg() {
        let Some(base) = base_url() else {
            assert!(std::env::var("REQUIRE_PG_SUITE").is_err(), "REQUIRE_PG_SUITE set without ORGT_TEST_BASE_URL");
            eprintln!("skipping member_backfill_records_channel_author_and_permalink_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        // SAFETY: every pg test in this crate sets the same constant KEK.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
        let db = "orgt_slack_member_anchor";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let pool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let user: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("anchor-{}@test.local", Uuid::new_v4())).fetch_one(&pool).await.unwrap();
        let kek = coralswarm_core::kek::load_kek().unwrap();
        let dek = coralswarm_core::kek::wrap_dek(&kek, &coralswarm_core::kek::generate_dek()).unwrap();
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id")
            .bind(user).bind(&dek).fetch_one(&pool).await.unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account).bind(user).execute(&pool).await.unwrap();
        let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Primary', 'personal', true) RETURNING id")
            .bind(account).fetch_one(&pool).await.unwrap();
        sqlx::query("INSERT INTO oauth_tokens(account_id, owner_user_id, provider, scope, access_token, provider_workspace_id) VALUES ($1,$2,'slack_user','channels:read users:read',$3,'T1')")
            .bind(account).bind(user).bind(vec![0u8]).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO slack_member_channels (account_id, user_id, workspace_id, channel_id) VALUES ($1, $2, 'T1', 'C1')")
            .bind(account).bind(user).execute(&pool).await.unwrap();

        let (mock, lookups) = spawn_anchor_mock(serde_json::json!([
            {"type": "message", "user": "U1", "ts": "1729000000.000100", "text": "We ship the eval on Friday."},
            {"type": "message", "user": "U1", "ts": "1729000060.000200", "text": "Owner is Dana."}
        ])).await;
        let _lock = super::super::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _guard = super::super::slack::SlackApiBaseTestGuard::set(&mock);
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
        let http = reqwest::Client::new();
        backfill_one_channel(&state, &http, &pool, &pool, account, user, "T1", "C1", "xoxp-member", AccountPlane::Personal, None)
            .await
            .unwrap();

        let versions: Vec<crate::source_evidence::SourceVersion> =
            sqlx::query_as("SELECT * FROM source_versions WHERE graph_id = $1 ORDER BY source_id")
                .bind(graph).fetch_all(&pool).await.unwrap();
        assert_eq!(versions.len(), 2);
        let v = &versions[0];
        assert_eq!(v.source_id, "C1/1729000000.000100");
        let m = &v.source_metadata;
        assert_eq!(m["channel_id"], "C1");
        assert_eq!(m["channel_name"], "eng");
        assert_eq!(m["author_id"], "U1");
        assert_eq!(m["author_name"], "Dana");
        assert_eq!(m["url"], "https://acme.slack.com/archives/C1/p1729000000000100");
        assert_eq!(versions[1].source_metadata["url"], "https://acme.slack.com/archives/C1/p1729000060000200");
        assert_eq!(lookups.load(std::sync::atomic::Ordering::SeqCst), 3, "one conversations.info, users.info and auth.test for the page");
        let origin = serde_json::to_value(&v.origin.as_ref().unwrap().0).unwrap();
        assert_eq!(origin["container"]["label"], "eng");

        let text = v.raw_inline.as_deref().unwrap();
        let v3 = crate::workers::source_atoms::input_for_version("source-atoms-v3", text, v).unwrap();
        for needle in [r#""channel_name":"eng""#, r#""url":"https://acme.slack.com/archives/C1/p1729000000000100""#, r#""author":"Dana""#] {
            assert!(v3.contains(needle), "v3 input lacks {needle}: {v3}");
        }
        let v2 = crate::workers::source_atoms::input_for_version("source-atoms-v2", text, v).unwrap();
        assert!(v2.contains(r#""author":"Dana""#), "v2 input lacks the author: {v2}");

        drop(_guard);
        drop(_lock);
        pool.close().await;
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
