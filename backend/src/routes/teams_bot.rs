//! Microsoft Teams bot endpoint (CS-122) — **presence-driven** ingestion.
//!
//! Public endpoint (`/v1/webhooks/teams`) registered as the messaging endpoint
//! of an Azure Bot. When an owner **adds CoralSwarm** to a channel or group
//! chat, Teams (via Azure Bot Service) POSTs a Bot Framework `Activity` here;
//! removing it POSTs the inverse. This mirrors Slack's `member_joined_channel`
//! model: nothing is ingested until the bot is added to that specific
//! conversation, and ingestion stops when it is removed.
//!
//! Flow:
//! 1. Validate the inbound JWT (Bot Connector tokens: issuer
//!    `https://api.botframework.com`, keys from the Bot Framework OpenID
//!    metadata, audience = our bot/app id).
//! 2. Detect bot **added**/**removed** from `installationUpdate` (action
//!    add/remove) or `conversationUpdate` (`membersAdded`/`membersRemoved`
//!    containing the bot's own id).
//! 3. Resolve the CoralSwarm account from the activity's tenant id via the
//!    existing Microsoft OAuth connection (`oauth_tokens.provider_workspace_id`
//!    = tenant) — there is no CoralSwarm user session on a bot call.
//! 4. Add → `teams_ingest::install_watch` (app-mode watch + backfill + live
//!    subscription). Remove → `teams_ingest::remove_watch`.
//!
//! We ACK 200 immediately and do the work off the hot path so Bot Framework
//! never times out.
//!
//! # Plane-resolved content pools (m365 Train 2 PR 2.4)
//!
//! Account/tenant ROUTING (`microsoft::resolve_microsoft_account_ids`, the
//! `account_members` representative-user lookup) always reads the CONTROL
//! pool — those tables genuinely live only there, on either plane. But every
//! CONTENT write this webhook makes (`teams_watched_resources` via
//! `install_watch`/`claim_history_choice`/`remove_watch`,
//! `team_has_channel_watches`'s read) used to run on that SAME control pool
//! unconditionally — correct for a Personal account (its content pool IS the
//! control pool) but silently wrong for an Org account whose content lives on
//! its own tenant database: every install/backfill-choice/uninstall for an
//! org's Teams presence was landing in the wrong place. [`resolve_teams_
//! content_pool`] fixes this: it resolves each matched account's OWN content
//! pool once per activity, mirroring `routes::graph_webhook::
//! resolve_graph_ingest_target`'s plane lookup (Personal ⇒ the control pool,
//! byte-identical; Org ⇒ `routes::graph_webhook::
//! resolve_org_tenant_pool_for_graph_ingest`'s tenant-routing table, reused
//! rather than copied a third time).
//!
//! Unlike message ingest (`JobKind::GraphMessageIngest`, PR 2.3's durable
//! hand-off), these actions are NOT deferred to a queue: `install_watch`,
//! `claim_history_choice`, and their siblings resolve and act SYNCHRONOUSLY,
//! in this already-`tokio::spawn`ed background task, against the resolved
//! pool — mirroring how org-slack step 3 (`POST /v1/slack/interactivity`,
//! PR #491) reaches the tenant synchronously within its own ack budget rather
//! than enqueuing a background hand-off. The Bot Framework ack itself (`200
//! OK`, above) is already sent before this task starts, so there is no wire
//! deadline pressing on THIS resolution the way there was on Slack's — the
//! discipline being mirrored is "resolve the tenant and act on it directly",
//! not "must finish within N seconds".

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use chrono::Utc;

use crate::{connectors::microsoft, routes::teams_ingest, AppState};

const BOT_OPENID_CONFIG: &str = "https://login.botframework.com/v1/.well-known/openidconfiguration";
const BOT_ISSUER: &str = "https://api.botframework.com";

/// Bot Connector activities are sent back to the `serviceUrl` carried in the
/// (signed) inbound activity. Binding that URL to Microsoft-operated hosts is
/// defense-in-depth: it stops a forwarded/forged token from being delivered to an
/// attacker-controlled origin even if a validly-signed activity smuggles one in.
/// Teams uses `https://smba.trafficmanager.net/<region>/…` (region in the PATH,
/// host is always the single `smba.` label) and regional `*.botframework.com`
/// hosts. CS-122.
///
/// SECURITY: do NOT trust `*.trafficmanager.net` by suffix — Azure Traffic
/// Manager subdomains are self-service provisionable by any customer, so a bare
/// suffix match lets an attacker stand up `https://attacker.trafficmanager.net/`
/// and harvest the bot connector bearer token (#134). Only the exact
/// `smba.trafficmanager.net` host is Microsoft-operated. `botframework.com` and
/// its subdomains are Microsoft-owned (not self-service) and stay trusted.
fn is_trusted_service_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if parsed.scheme() != "https" {
        return false;
    }
    match parsed.host_str() {
        Some(h) => {
            h == "smba.trafficmanager.net"
                || h == "botframework.com"
                || h.ends_with(".botframework.com")
        }
        None => false,
    }
}

/// Microsoft's documented serviceUrl binding: the inbound Bot Framework JWT
/// carries a `serviceUrl` claim that MUST equal the activity's `serviceUrl`, so a
/// validly-signed token can't be replayed to deliver the bot connector bearer
/// token to a different origin (#134). This is the canonical defense;
/// `is_trusted_service_url` is the host-allowlist backstop on top of it.
///
/// Fails CLOSED: if the activity carries a serviceUrl to act on but the token
/// bound none, we reject. When the activity has no serviceUrl there is nothing to
/// post to (handlers bail), so the binding is vacuously satisfied. Trailing-slash
/// variance between claim and activity is ignored.
fn service_url_matches_claim(activity_url: Option<&str>, claim_url: Option<&str>) -> bool {
    match (activity_url, claim_url) {
        (Some(a), Some(c)) => a.trim_end_matches('/') == c.trim_end_matches('/'),
        (None, _) => true,
        (Some(_), None) => false,
    }
}

// ── Bot Framework Activity (subset we care about) ────────────────────────────

#[derive(Debug, Deserialize)]
struct Activity {
    #[serde(default)]
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Stable provider activity identifier, authenticated with this event.
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "type")]
    activity_type: Option<String>,
    /// `installationUpdate`: "add" | "remove".
    #[serde(default)]
    action: Option<String>,
    /// Microsoft-authenticated sender; org actions require its AAD identity.
    #[serde(default)]
    from: Option<ChannelAccount>,
    /// The bot's own account on this activity.
    #[serde(default)]
    recipient: Option<ChannelAccount>,
    #[serde(default, rename = "membersAdded")]
    members_added: Vec<ChannelAccount>,
    #[serde(default, rename = "membersRemoved")]
    members_removed: Vec<ChannelAccount>,
    #[serde(default)]
    conversation: Option<Conversation>,
    #[serde(default, rename = "channelData")]
    channel_data: Option<ChannelData>,
    /// Bot Connector base URL to send/update activities back. CS-122.
    #[serde(default, rename = "serviceUrl")]
    service_url: Option<String>,
    /// On an Action.Submit click, the id of the prompt card to update. CS-122.
    #[serde(default, rename = "replyToId")]
    reply_to_id: Option<String>,
    /// Action.Submit data payload (carries the chosen backfill window). CS-122.
    #[serde(default)]
    value: Option<serde_json::Value>,
    /// Mentions etc. Used to detect a @mention of the bot (channel activation).
    #[serde(default)]
    entities: Vec<Entity>,
}

#[derive(Debug, Deserialize)]
struct Entity {
    #[serde(rename = "type", default)]
    entity_type: Option<String>,
    #[serde(default)]
    mentioned: Option<ChannelAccount>,
}

#[derive(Debug, Deserialize, Clone)]
struct ChannelAccount {
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "aadObjectId")]
    aad_object_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Conversation {
    #[serde(default)]
    id: Option<String>,
    /// "channel" | "groupChat" | "personal".
    #[serde(default, rename = "conversationType")]
    conversation_type: Option<String>,
    #[serde(default, rename = "tenantId")]
    tenant_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChannelData {
    #[serde(default)]
    meeting: Option<IdField>,
    #[serde(default)]
    team: Option<Team>,
    #[serde(default)]
    channel: Option<IdField>,
    #[serde(default)]
    tenant: Option<IdField>,
}

#[derive(Debug, Deserialize)]
struct Team {
    #[serde(default)]
    id: Option<String>,
    /// Graph addresses a team by its AAD group id (`teams/{aadGroupId}`).
    #[serde(default, rename = "aadGroupId")]
    aad_group_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IdField {
    #[serde(default)]
    id: Option<String>,
}

// ── Handler ──────────────────────────────────────────────────────────────────

/// `POST /v1/webhooks/teams` — Azure Bot messaging endpoint.
pub async fn teams_bot(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let claim_service_url = match validate_bot_jwt(&headers, &state.http_client).await {
        Ok(claim) => claim,
        Err(e) => {
            tracing::warn!(error = %e, "teams bot: JWT validation failed");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    let activity: Activity = match serde_json::from_slice::<Activity>(&body) {
        Ok(a) => a,
        Err(e) => {
            // ACK so Bot Framework doesn't retry; nothing actionable.
            tracing::warn!(error = %e, "teams bot: unparseable activity");
            return StatusCode::OK.into_response();
        }
    };

    // Bind the signed token to where we'd reply: the JWT's serviceUrl claim must
    // match the activity's serviceUrl (Microsoft's documented check). Without
    // this, a validly-signed activity could carry an attacker serviceUrl and
    // harvest the bot bearer token (#134); the host allowlist is the backstop.
    if !service_url_matches_claim(
        activity.service_url.as_deref(),
        claim_service_url.as_deref(),
    ) {
        tracing::warn!(
            activity_service_url = ?activity.service_url,
            claim_service_url = ?claim_service_url,
            "teams bot: serviceUrl does not match token claim; refusing"
        );
        return StatusCode::FORBIDDEN.into_response();
    }

    // Do the work off the hot path; ACK immediately.
    tokio::spawn(async move {
        process_activity(state, activity).await;
    });
    StatusCode::OK.into_response()
}

/// After `process_meeting_activity` has run, does a meeting-scoped activity
/// also continue into the ordinary chat path? Only an explicit @mention does.
fn meeting_activity_continues_to_chat(a: &Activity) -> bool {
    let activity_type = a.activity_type.as_deref().unwrap_or("");
    let bot_id = a.recipient.as_ref().and_then(|r| r.id.as_deref());
    is_bot_mention(a, activity_type, bot_id)
}

/// Is this a message that @mentions the bot itself?
fn is_bot_mention(a: &Activity, activity_type: &str, bot_id: Option<&str>) -> bool {
    activity_type == "message"
        && bot_id.is_some()
        && a.entities.iter().any(|e| {
            e.entity_type.as_deref() == Some("mention")
                && e.mentioned.as_ref().and_then(|m| m.id.as_deref()) == bot_id
        })
}

/// Was the bot itself among these members?
fn bot_among(members: &[ChannelAccount], bot_id: Option<&str>) -> bool {
    match bot_id {
        Some(b) => members.iter().any(|m| m.id.as_deref() == Some(b)),
        None => false,
    }
}

/// Resolved conversation scope for a bot activity (install/remove or a prompt
/// click). `conversation_id` is the Bot Connector conversation to post into.
struct Scope {
    tenant: String,
    kind: &'static str,
    team_id: String,
    channel_id: String,
    chat_id: String,
    conversation_id: String,
}

/// Derive the scope (tenant + channel/group-chat ids) from the signed activity
/// envelope — used identically for install events and prompt clicks, so a click
/// can never target a different conversation than the one it came from (CS-122).
fn extract_scope(a: &Activity) -> Option<Scope> {
    let tenant = a
        .channel_data
        .as_ref()
        .and_then(|c| c.tenant.as_ref())
        .and_then(|t| t.id.clone())
        .or_else(|| a.conversation.as_ref().and_then(|c| c.tenant_id.clone()))
        .filter(|t| !t.is_empty())?;
    let conversation_id = a
        .conversation
        .as_ref()
        .and_then(|c| c.id.clone())
        .filter(|s| !s.is_empty())?;

    let channel_scope = a.channel_data.as_ref().and_then(|cd| {
        let team = cd.team.as_ref()?;
        let team_id = team.aad_group_id.clone().or_else(|| team.id.clone())?;
        let channel_id = cd.channel.as_ref().and_then(|c| c.id.clone())?;
        if team_id.is_empty() || channel_id.is_empty() {
            None
        } else {
            Some((team_id, channel_id))
        }
    });
    let conv_type = a
        .conversation
        .as_ref()
        .and_then(|c| c.conversation_type.as_deref())
        .unwrap_or("");

    if let Some((team_id, channel_id)) = channel_scope {
        Some(Scope {
            tenant,
            kind: "channel",
            team_id,
            channel_id,
            chat_id: String::new(),
            conversation_id,
        })
    } else if conv_type == "groupChat" {
        Some(Scope {
            tenant,
            kind: "chat",
            team_id: String::new(),
            channel_id: String::new(),
            chat_id: conversation_id.clone(),
            conversation_id,
        })
    } else {
        // personal/1:1 or unsupported scope — nothing to ingest.
        None
    }
}

/// Resolve the content pool AND plane that `account_id`'s Teams state
/// (`teams_watched_resources`, and everything `install_watch`/`claim_history_
/// choice`/`remove_watch` touch) should read/write on for THIS activity — see
/// this module's own doc comment ("Plane-resolved content pools").
///
/// `None` means refuse rather than guess (I8): a revoked org, a mid-migration
/// or torn-down tenant routing row, or a lookup failure all resolve to `None`
/// here exactly like `resolve_org_tenant_pool_for_graph_ingest`'s own `None`
/// arms — this webhook drops the activity for that account rather than
/// falling back to the control/consumer pool the way pre-PR-2.4 code did
/// unconditionally.
async fn resolve_teams_content_pool(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    account_id: Uuid,
) -> Option<(sqlx::PgPool, crate::routes::slack::AccountPlane)> {
    let plane = match crate::routes::slack::account_plane(control_pool, account_id).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "teams bot: account plane lookup failed");
            return None;
        }
    };
    if plane == crate::routes::slack::AccountPlane::Personal {
        return Some((control_pool.clone(), plane));
    }

    // Org: one more control-plane read for the id `account_plane` confirmed
    // exists but did not return — same two-step `resolve_graph_ingest_
    // target` takes for the identical reason.
    let clerk_org_id: Option<String> =
        match sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_one(control_pool)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "teams bot: clerk_org_id lookup failed");
                return None;
            }
        };
    let Some(clerk_org_id) = clerk_org_id else {
        // Defensive only: `account_plane` already confirmed `clerk_org_id IS
        // NOT NULL` off the same row a moment ago.
        tracing::warn!(
            %account_id,
            "teams bot: account_plane said Org but clerk_org_id re-read NULL — refusing"
        );
        return None;
    };

    crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(
        state,
        control_pool,
        &clerk_org_id,
    )
    .await
    .map(|pool| (pool, plane))
}

/// Claims outbound card delivery for the current account if nobody has
/// claimed it yet this activity, returning whether THIS account gets it.
/// Pure and DB-free (only the caller's `claimed` flag is mutated) so the
/// "first USABLE account wins, never simply the first account the resolver
/// returned" invariant is unit-testable without a live database — see
/// `process_activity`'s own comment on `card_account_claimed` for the bug
/// this fixes (CodeRabbit review, PR #667).
fn claim_card_delivery(claimed: &mut bool) -> bool {
    if *claimed {
        false
    } else {
        *claimed = true;
        true
    }
}

async fn process_activity(state: Arc<AppState>, a: Activity) {
    // Meeting transcript consent is separate from ordinary chat capture.
    // Installation in a real signed meeting context is handled exclusively here.
    let activity_type = a.activity_type.as_deref().unwrap_or("");
    let bot_id = a.recipient.as_ref().and_then(|r| r.id.as_deref());
    if a.channel_data
        .as_ref()
        .and_then(|c| c.meeting.as_ref())
        .is_some()
    {
        process_meeting_activity(&state, &a).await;
        // A meeting chat is a group chat. Teams stamps `channelData.meeting`
        // on every message in it, so an explicit @mention of the bot must still
        // reach the ordinary chat path below (chat capture is a separate,
        // member-attributed consent from transcript capture, which stays
        // exclusively in `process_meeting_activity`). Install/remove and
        // non-mention traffic end here as before.
        if !meeting_activity_continues_to_chat(&a) {
            return;
        }
    }

    let Some(scope) = extract_scope(&a) else {
        // A team-wide install has no selected channel and cannot consent to
        // capture. Reply only in the signed installation conversation.
        let installed = (activity_type == "installationUpdate"
            && a.action.as_deref() == Some("add"))
            || (activity_type == "conversationUpdate" && bot_among(&a.members_added, bot_id));
        if installed {
            let tenant = a
                .channel_data
                .as_ref()
                .and_then(|cd| cd.tenant.as_ref())
                .and_then(|t| t.id.clone())
                .or_else(|| a.conversation.as_ref().and_then(|c| c.tenant_id.clone()));
            let conversation = a.conversation.as_ref().and_then(|c| c.id.clone());
            if let (Some(tenant), Some(conversation_id)) = (tenant, conversation) {
                if Uuid::parse_str(&tenant).is_ok() && !conversation_id.is_empty() {
                    let scope = Scope {
                        tenant,
                        kind: "team",
                        team_id: String::new(),
                        channel_id: String::new(),
                        chat_id: String::new(),
                        conversation_id,
                    };
                    post_mention_install_hint(&state, &a, &scope).await;
                }
            }
        }
        return;
    };

    // Is this an Action.Submit click on our backfill prompt?
    let click_window = a
        .value
        .as_ref()
        .filter(|v| v.get("cs").and_then(|c| c.as_str()) == Some("backfill"))
        .and_then(|v| v.get("window").and_then(|w| w.as_str()))
        .map(str::to_string);

    // Or a click on a per-channel consent ("offer") card from the fan-out? The
    // team/channel ids ride in the payload because an Action.Submit click doesn't
    // carry the team's AAD group id (we need it to create the watch). CS-122.
    let offer = a
        .value
        .as_ref()
        .filter(|v| v.get("cs").and_then(|c| c.as_str()) == Some("offer"))
        .map(|v| {
            let accept = v.get("act").and_then(|x| x.as_str()) == Some("accept");
            let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
            (accept, s("team"), s("chan"), s("name"))
        });

    // Otherwise, is the bot being added / removed?
    let (added, removed) = match activity_type {
        "installationUpdate" => (
            a.action.as_deref() == Some("add"),
            a.action.as_deref() == Some("remove"),
        ),
        "conversationUpdate" => (
            bot_among(&a.members_added, bot_id),
            bot_among(&a.members_removed, bot_id),
        ),
        _ => (false, false),
    };
    // Treat a @mention of the bot in a channel/group chat as "activate this
    // channel/chat" — the Teams-native, per-channel equivalent of adding the bot
    // to a Slack channel. (Channels only deliver mentioned messages anyway; the
    // explicit mention check keeps group chats from re-triggering on every msg.)
    let bot_mentioned = is_bot_mention(&a, activity_type, bot_id);
    let activate = added || bot_mentioned;
    if click_window.is_none() && offer.is_none() && !activate && !removed {
        return;
    }

    // Map tenant → every connected CoralSwarm account (+ each one's
    // representative user).
    let Some(pool) = state.db.clone() else {
        return;
    };
    // RLS bootstrap (org-tenancy B.3): this is a SIGNATURE-authenticated
    // webhook — Teams proves itself with a bot-framework JWT, not a
    // CoralSwarm session, so the request arrives knowing only a Microsoft
    // tenant id and the owning account(s) must be DISCOVERED before any scope
    // can be opened. That first hop goes through `resolve_microsoft_
    // account_ids` (migration `0121_connector_installs`'s routing index,
    // falling back to the audited `SECURITY DEFINER` `oauth_tokens` carve-out
    // — see that function's own doc comment), which replaces the old `LIMIT 1`
    // `resolve_oauth_account_id` (0084) here specifically because a tenant CAN
    // be installed under more than one CoralSwarm account (two customers, or a
    // customer plus someone's personal account) and the old resolver silently
    // orphaned every account but whichever one Postgres happened to return.
    // Every match is processed below — deterministic iteration order (the
    // resolver's own `created_at, account_id`), not a single arbitrary pick.
    let account_ids = match microsoft::resolve_microsoft_account_ids(&pool, &scope.tenant).await {
        Ok(ids) => ids,
        Err(e) => {
            // Return early rather than degrading to "resolved zero accounts":
            // a resolver ERROR (DB/query failure) and a genuine "nobody has
            // connected this tenant" are different events, and collapsing
            // them into the same `Vec::new()` path would log the wrong
            // message (the "connect Microsoft first" hint below is actively
            // misleading when the real cause is a transient resolve failure)
            // and would silently skip every other in-flight handler branch's
            // chance to distinguish the two on retry/alerting.
            tracing::warn!(error = %e, tenant = %scope.tenant, "teams bot: account resolve failed");
            return;
        }
    };
    // A mention is attributable consent for ONE member's ONE conversation.
    // Resolve every eligible organization before writing: tenant fan-out is
    // never permission to enroll multiple private oceans.
    if bot_mentioned && sender_aad(&a).is_none() {
        post_mention_install_hint(&state, &a, &scope).await;
        return;
    }
    if bot_mentioned || added {
        let mut eligible = Vec::new();
        let mut has_org = false;
        if let Some(aad) = sender_aad(&a) {
            for account in &account_ids {
                let org: Option<String> =
                    sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
                        .bind(account)
                        .fetch_one(&pool)
                        .await
                        .ok()
                        .flatten();
                let Some(org) = org else {
                    continue;
                };
                has_org = true;
                if !teams_ingest::org_microsoft_teams_enabled_for(&state, &org).await {
                    continue;
                }
                let Ok(members) =
                    resolve_org_bot_members(&pool, *account, &scope.tenant, &aad).await
                else {
                    return;
                };
                for member in members {
                    if matches!(crate::middleware::auth::live_org_membership(&pool, &org, member).await,
                        Ok(Some((live, _))) if live == *account)
                    {
                        eligible.push((*account, member));
                    }
                }
            }
            if has_org || account_ids.is_empty() {
                let target = crate::routes::teams_onboarding::Target {
                    tenant: scope.tenant.clone(),
                    aad,
                    intent_nonce: None,
                    activity_id: mention_enrollment_key(&a),
                    kind: scope.kind.to_owned(),
                    team: scope.team_id.clone(),
                    channel: scope.channel_id.clone(),
                    chat: scope.chat_id.clone(),
                };
                if bot_mentioned && eligible.len() == 1 {
                    let (account, member) = eligible[0];
                    match crate::routes::teams_onboarding::activate(
                        &state, account, member, &target,
                    )
                    .await
                    {
                        Ok(result) => {
                            post_mention_activation(&state, &a, &scope, result.created).await;
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(error=?e, %account, "teams mention activation requires setup or retry")
                        }
                    }
                }
                post_mention_setup(&state, &a, &scope, target, bot_mentioned).await;
                return;
            }
        } else if has_org || account_ids.is_empty() || added {
            // Install events cannot consent on behalf of a team or its members.
            post_mention_install_hint(&state, &a, &scope).await;
            return;
        }
    }
    if account_ids.is_empty() {
        tracing::warn!(
            tenant = %scope.tenant,
            "teams bot: no CoralSwarm account has connected this tenant; ignoring (connect Microsoft first)"
        );
        return;
    }

    // Outbound Teams cards (prompt/consent) are a property of the
    // CONVERSATION, not of any one CoralSwarm account — posting one per
    // resolved account would duplicate visible messages in the same
    // channel/chat when a tenant is installed under more than one account.
    // Card delivery is therefore pinned to exactly one account: the FIRST
    // one, in the resolver's own deterministic `created_at, account_id`
    // order, that actually reaches dispatch — every account still gets its
    // own `install_watch` row via `activate_single`/`activate_team_channels`
    // below, only the card side effects inside them are gated on this flag.
    //
    // CodeRabbit review (PR #667): this used to be `idx == 0`, computed
    // BEFORE the representative-user lookup and content-pool resolution
    // below could `continue` past a given account. If account 0 was skipped
    // (no members, a lookup failure, or an unresolvable org plane), `idx ==
    // 0` was never true again for the rest of the loop — every later,
    // perfectly usable account ran with `post_cards = false`: watches got
    // created, but no backfill/history-choice prompt or sibling-consent card
    // was ever posted. [`claim_card_delivery`] fixes this by claiming the
    // flag lazily, only once an account has actually passed both gates.
    let mut card_account_claimed = false;
    let mut org_hint_needed = false;

    for account_id in account_ids {
        // Org bot actions confirm the sender's existing delegated enrollment;
        // never attribute an app install to an arbitrary representative member.
        let plane = match crate::routes::slack::account_plane(&pool, account_id).await {
            Ok(plane) => plane,
            Err(_) => continue,
        };
        if plane == crate::routes::slack::AccountPlane::Org {
            if bot_mentioned || click_window.as_deref() == Some("none") {
                org_hint_needed |= sender_aad(&a).is_some();
                handle_org_member_activity(
                    &state,
                    &pool,
                    account_id,
                    &a,
                    &scope,
                    click_window.as_deref(),
                    &mut card_account_claimed,
                )
                .await;
            }
            // Install/remove events and offer payloads cannot enroll org members.
            continue;
        }
        // The representative-user half of the original join reads
        // `account_members`, which is a CONTROL-plane table carrying no 0083
        // policy — nothing to scope there, and it deliberately must stay
        // outside any scope anyway (Model B membership is multi-account).
        // Explicit `Result` handling (not `.ok().flatten()`): a query
        // FAILURE and a genuinely memberless account must log distinctly —
        // collapsing them hid a DB error behind the same "no members" line a
        // healthy-but-empty account would also produce.
        let user_id = match sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT am.user_id
            FROM account_members am
            WHERE am.account_id = $1
            ORDER BY am.joined_at
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&pool)
        .await
        {
            Ok(Some(user_id)) => user_id,
            Ok(None) => {
                tracing::warn!(
                    %account_id, tenant = %scope.tenant,
                    "teams bot: account resolved for this tenant has no members; skipping"
                );
                continue;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e, %account_id, tenant = %scope.tenant,
                    "teams bot: representative-user lookup failed; skipping this account"
                );
                continue;
            }
        };

        // Plane-resolved content pool for THIS account (m365 Train 2 PR 2.4)
        // — see this module's own doc comment. `pool` above stays the
        // CONTROL pool for routing only; every content read/write below uses
        // `content_pool`. Refuse (I8) rather than fall back to `pool` when
        // an org's tenant plane cannot be resolved.
        let Some((content_pool, plane)) =
            resolve_teams_content_pool(&state, &pool, account_id).await
        else {
            tracing::warn!(
                %account_id, tenant = %scope.tenant,
                "teams bot: could not resolve a content pool for this account's plane — \
                 refusing (never falling back to the control/consumer pool for an org)"
            );
            continue;
        };

        // Only an account that made it past BOTH gates above is a candidate
        // for card delivery — see this loop's own comment on
        // `card_account_claimed`.
        let post_cards = claim_card_delivery(&mut card_account_claimed);

        if let Some(window) = &click_window {
            handle_choice(&state, &content_pool, account_id, &a, &scope, window).await;
        } else if let Some((accept, team_id, channel_id, name)) = &offer {
            // A consent card was clicked (accept → activate that channel; decline → skip).
            handle_offer(
                &state,
                &content_pool,
                plane,
                account_id,
                user_id,
                &a,
                &scope,
                *accept,
                team_id,
                channel_id,
                name,
            )
            .await;
        } else if added && scope.kind == "channel" {
            // Bot installed to a team → auto-activate every standard channel (post a
            // prompt card in each), so it's seamless without a manual @mention.
            // `bot_mentioned` is false here (install event, not a message) → prompt only
            // on first insert, so duplicate install events don't double-post.
            activate_team_channels(
                &state,
                &content_pool,
                plane,
                account_id,
                user_id,
                &scope,
                &a,
                bot_mentioned,
                post_cards,
            )
            .await;
        } else if activate && scope.kind == "channel" {
            // A @mention in a channel. If it's the *first* channel we've seen in this
            // team, treat it as the team's onboarding trigger and fan out prompt cards
            // to every channel — the team-install event that would normally do this is
            // unreliable (and blocked outright in some tenants). A @mention in a later
            // channel of an already-seeded team just activates that one channel. An
            // explicit @mention always (re-)prompts so it recovers if a prior prompt
            // never landed. CS-122.
            if teams_ingest::team_has_channel_watches(&content_pool, account_id, &scope.team_id)
                .await
            {
                activate_single(
                    &state,
                    &content_pool,
                    plane,
                    account_id,
                    user_id,
                    &scope,
                    &a,
                    bot_mentioned,
                    post_cards,
                )
                .await;
            } else {
                activate_team_channels(
                    &state,
                    &content_pool,
                    plane,
                    account_id,
                    user_id,
                    &scope,
                    &a,
                    bot_mentioned,
                    post_cards,
                )
                .await;
            }
        } else if activate {
            // Group-chat install: single-conversation activation (no team to fan out).
            activate_single(
                &state,
                &content_pool,
                plane,
                account_id,
                user_id,
                &scope,
                &a,
                bot_mentioned,
                post_cards,
            )
            .await;
        } else if removed {
            let stopped = teams_ingest::remove_watch(
                &state,
                &content_pool,
                account_id,
                scope.kind,
                &scope.team_id,
                &scope.channel_id,
                &scope.chat_id,
            )
            .await;
            if stopped {
                tracing::info!(%account_id, kind = %scope.kind, "teams bot: removed → auto-ingest stopped");
            } else {
                tracing::info!(%account_id, kind = %scope.kind, "teams bot: remove event but no active watch was deleted");
            }
        }
    }
    if org_hint_needed && !card_account_claimed {
        post_org_config_hint(&state, &a, &scope).await;
    }
}

#[derive(serde::Deserialize)]
struct NativeMeetingDetails {
    details: NativeMeetingMetadata,
    conversation: NativeMeetingConversation,
    organizer: NativeMeetingOrganizer,
}
#[derive(serde::Deserialize)]
struct NativeMeetingMetadata {
    #[serde(rename = "msGraphResourceId")]
    graph_id: String,
}
#[derive(serde::Deserialize)]
struct NativeMeetingConversation {
    id: String,
}
#[derive(serde::Deserialize)]
struct NativeMeetingOrganizer {
    #[serde(rename = "tenantId")]
    tenant: String,
}

async fn process_meeting_activity(state: &Arc<AppState>, a: &Activity) {
    let Some(meeting_id) = a
        .channel_data
        .as_ref()
        .and_then(|c| c.meeting.as_ref())
        .and_then(|m| m.id.as_deref())
        .filter(|s| !s.is_empty())
    else {
        return;
    };
    let Some(scope) = extract_scope(a) else {
        return;
    };
    let bot_id = a.recipient.as_ref().and_then(|r| r.id.as_deref());
    let added = a.activity_type.as_deref() == Some("installationUpdate")
        && a.action.as_deref() == Some("add");
    let metadata_added = a.activity_type.as_deref() == Some("conversationUpdate")
        && bot_among(&a.members_added, bot_id);
    let removed = (a.activity_type.as_deref() == Some("installationUpdate")
        && a.action.as_deref() == Some("remove"))
        || (a.activity_type.as_deref() == Some("conversationUpdate")
            && bot_among(&a.members_removed, bot_id));
    // Messages, recording, meeting start/end and generic chat watches cannot grant consent.
    if !added && !metadata_added && !removed {
        return;
    }
    let Some(control) = state.control_db() else {
        return;
    };
    let (Some(event_at), Some(event_id)) = (a.timestamp, mention_enrollment_key(a)) else {
        return;
    };
    let Ok(accounts) = microsoft::resolve_microsoft_account_ids(control, &scope.tenant).await
    else {
        return;
    };
    if removed {
        if crate::services::teams_meeting_capture::remove_context(
            control,
            &scope.tenant,
            meeting_id,
            event_at,
            &event_id,
        )
        .await
        .ok()
            != Some(true)
        {
            return;
        }
        for account in accounts {
            if let Some((content, _)) = resolve_teams_content_pool(state, control, account).await {
                if let Err(e) = crate::services::teams_meeting_capture::remove_installations(
                    &content,
                    account,
                    &scope.tenant,
                    meeting_id,
                )
                .await
                {
                    tracing::warn!(error=%e,"meeting installation revoke failed");
                }
            }
        }
        return;
    }
    let Some(aad) = sender_aad(a) else {
        post_mention_install_hint(state, a, &scope).await;
        return;
    };
    let Some(service) = a
        .service_url
        .as_deref()
        .filter(|s| is_trusted_service_url(s))
    else {
        return;
    };
    let Ok(bot_token) = microsoft::bot_connector_token(&state.http_client, &scope.tenant).await
    else {
        return;
    };
    let url = format!(
        "{}/v1/meetings/{}",
        service.trim_end_matches('/'),
        urlencoding::encode(meeting_id)
    );
    let Ok(response) = state
        .http_client
        .get(url)
        .bearer_auth(bot_token)
        .send()
        .await
    else {
        return;
    };
    if !response.status().is_success() {
        return;
    }
    let Ok(details) = response.json::<NativeMeetingDetails>().await else {
        return;
    };
    if details.conversation.id != scope.conversation_id || details.organizer.tenant != scope.tenant
    {
        return;
    }
    if !added {
        let _ = crate::services::teams_meeting_capture::store_context_metadata(
            control,
            &scope.tenant,
            meeting_id,
            &scope.conversation_id,
            &details.details.graph_id,
            service,
            &aad,
            event_at,
            &event_id,
        )
        .await;
        return;
    }
    if crate::services::teams_meeting_capture::store_context(
        control,
        &scope.tenant,
        meeting_id,
        &scope.conversation_id,
        &details.details.graph_id,
        service,
        &aad,
        event_at,
        &event_id,
    )
    .await
    .ok()
        != Some(true)
    {
        return;
    }
    // A conversationUpdate is a duplicate membership notification, not a second consent.
    if !added {
        return;
    }
    let installation_generation = format!("{meeting_id}:{event_id}");
    for account in &accounts {
        if let Some((content, _)) = resolve_teams_content_pool(state, control, *account).await {
            if crate::services::teams_meeting_capture::installation_status(
                &content,
                *account,
                &scope.tenant,
                &installation_generation,
                meeting_id,
                true,
            )
            .await
            .is_err()
            {
                return;
            }
        }
    }
    // Installing the bot only records meeting context (above). Capture never
    // starts here: the Teams meeting pane's "Add meeting" is the only trigger,
    // and it is gated on recording/transcription running (see
    // `teams_ingest::capture_meeting_transcripts`).
    send_onboarding_card(state, a, &scope, INSTALL_CARD_TEXT, None).await;
}

/// Card posted when the bot is added to a meeting. It claims nothing is saved.
const INSTALL_CARD_TEXT: &str =
    "Turn on recording or transcription, then add this meeting from the CoralSwarm tab.";

async fn send_onboarding_card(
    state: &Arc<AppState>,
    a: &Activity,
    scope: &Scope,
    text: &str,
    action: Option<serde_json::Value>,
) {
    let Some(url) = a
        .service_url
        .as_deref()
        .filter(|u| is_trusted_service_url(u))
    else {
        return;
    };
    let Ok(token) = microsoft::bot_connector_token(&state.http_client, &scope.tenant).await else {
        return;
    };
    let card = branded_card(
        serde_json::json!([{"type":"TextBlock", "text":text, "wrap":true}]),
        action,
    );
    match microsoft::send_activity(
        &state.http_client,
        &token,
        url,
        &scope.conversation_id,
        &card,
    )
    .await
    {
        Ok(_) => tracing::info!("teams bot: mention onboarding card delivered"),
        Err(e) => tracing::warn!(error=%e, "teams bot: mention onboarding card failed"),
    }
}
async fn post_mention_install_hint(state: &Arc<AppState>, a: &Activity, scope: &Scope) {
    send_onboarding_card(state,a,scope,
        "CoralSwarm is ready. Mention @CoralSwarm in the channel or group chat you want to save to your private ocean. Installing the app alone doesn't save any messages.",None).await;
}
async fn post_mention_activation(
    state: &Arc<AppState>,
    a: &Activity,
    scope: &Scope,
    created: bool,
) {
    let text = if created {
        "✅ Saving this conversation from now. New messages go into your private ocean."
    } else {
        "✅ This conversation is already saved. Existing history settings are unchanged."
    };
    let url = format!(
        "{}/connectors/microsoft-teams",
        state.personal_web_base_url().trim_end_matches('/')
    );
    send_onboarding_card(
        state,
        a,
        scope,
        text,
        Some(serde_json::json!([
            {"type":"Action.OpenUrl","title":"Manage what's saved","url":url}
        ])),
    )
    .await;
}
async fn post_mention_setup(
    state: &Arc<AppState>,
    a: &Activity,
    scope: &Scope,
    target: crate::routes::teams_onboarding::Target,
    mentioned: bool,
) {
    if !mentioned {
        post_mention_install_hint(state, a, scope).await;
        return;
    }
    let Some(secret) = crate::oauth::state::state_secret() else {
        return;
    };
    let Ok(ticket) = crate::routes::teams_onboarding::mint(&secret, target, Utc::now().timestamp())
    else {
        return;
    };
    let url = format!(
        "{}/connectors/microsoft-teams?onboarding={}",
        state.personal_web_base_url().trim_end_matches('/'),
        urlencoding::encode(&ticket)
    );
    send_onboarding_card(state,a,scope,
        "Finish connecting your Microsoft account and choose your CoralSwarm workspace to start saving this conversation to your private ocean from now.",
        Some(serde_json::json!([{"type":"Action.OpenUrl","title":"Finish setup","url":url}]))).await;
}

/// Only a unique, live OAuth owner who still belongs to this account may
/// confirm a member watch. The routing index is checked against its live grant.
async fn resolve_org_bot_member(
    control: &sqlx::PgPool,
    account_id: Uuid,
    tenant: &str,
    aad: &str,
) -> sqlx::Result<Option<Uuid>> {
    Ok(unique_member(
        &resolve_org_bot_members(control, account_id, tenant, aad).await?,
    ))
}

async fn resolve_org_bot_members(
    control: &sqlx::PgPool,
    account_id: Uuid,
    tenant: &str,
    aad: &str,
) -> sqlx::Result<Vec<Uuid>> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let owners: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT ci.owner_user_id FROM connector_installs ci \
         JOIN oauth_tokens ot ON ot.account_id=ci.account_id \
           AND ot.provider=ci.provider AND ot.owner_user_id=ci.owner_user_id \
           AND ot.provider_workspace_id=ci.external_workspace_id \
           AND ot.provider_user_id=ci.external_user_id \
         JOIN account_members am ON am.account_id=ci.account_id AND am.user_id=ci.owner_user_id \
         WHERE ci.account_id=$1 AND ci.provider='microsoft' \
           AND ci.external_workspace_id=$2 AND ci.external_user_id=$3 \
           AND ci.owner_user_id IS NOT NULL AND ot.credential_invalid_at IS NULL",
    )
    .bind(account_id)
    .bind(tenant)
    .bind(aad)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(owners)
}

fn unique_member(owners: &[Uuid]) -> Option<Uuid> {
    match owners {
        [owner] => Some(*owner),
        _ => None,
    }
}

// Preserve the concurrent implementation's provider-ID normalization: retries
// of the same signed event use the same identity, whitespace is not an identity.
fn mention_enrollment_key(a: &Activity) -> Option<String> {
    a.id.as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn sender_aad(a: &Activity) -> Option<String> {
    let raw = a.from.as_ref()?.aad_object_id.as_deref()?;
    Uuid::parse_str(raw).ok().map(|id| id.to_string())
}

/// The watch is selected only by the Microsoft-authenticated conversation and
/// verified member. Legacy delegated rows have an empty tenant_id; their tenant
/// binding is checked against the exact doorbell watcher registry separately.
async fn own_delegated_watch(
    content: &sqlx::PgPool,
    account: Uuid,
    member: Uuid,
    scope: &Scope,
) -> sqlx::Result<Option<(String, String, String)>> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(content, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT team_id, channel_id, chat_id FROM teams_watched_resources \
         WHERE account_id=$1 AND user_id=$2 AND auth_mode='delegated' \
           AND kind=$3 AND channel_id=$4 AND chat_id=$5 \
           AND (tenant_id='' OR tenant_id=$6)",
    )
    .bind(account)
    .bind(member)
    .bind(scope.kind)
    .bind(&scope.channel_id)
    .bind(&scope.chat_id)
    .bind(&scope.tenant)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await?;
    Ok(if rows.len() == 1 {
        rows.into_iter().next()
    } else {
        None
    })
}

fn claim_enrolled_card(claimed: &mut bool, enrolled: bool) -> bool {
    enrolled && claim_card_delivery(claimed)
}

async fn post_org_config_hint(state: &Arc<AppState>, a: &Activity, scope: &Scope) {
    let Some(service_url) = a
        .service_url
        .as_deref()
        .filter(|u| is_trusted_service_url(u))
    else {
        return;
    };
    let Ok(token) = microsoft::bot_connector_token(&state.http_client, &scope.tenant).await else {
        return;
    };
    let url = format!(
        "{}/connectors/microsoft-teams",
        state.personal_web_base_url().trim_end_matches('/')
    );
    let card = branded_card(
        serde_json::json!([
            {"type":"TextBlock", "text":"Manage the Teams channels and chats you've saved to CoralSwarm.", "wrap":true}
        ]),
        Some(serde_json::json!([
            {"type":"Action.OpenUrl", "title":"Manage what's saved", "url":url}
        ])),
    );
    if let Err(e) = microsoft::send_activity(
        &state.http_client,
        &token,
        service_url,
        &scope.conversation_id,
        &card,
    )
    .await
    {
        tracing::warn!(error=%e, "teams bot: management hint failed");
    }
}

async fn registered_member_watch(
    control: &sqlx::PgPool,
    account: Uuid,
    member: Uuid,
    scope: &Scope,
    team: &str,
    channel: &str,
    chat: &str,
) -> sqlx::Result<bool> {
    // A supplied AAD team GUID must agree; Teams' opaque team thread ID on
    // card clicks may instead fall back to the matched watch's stored GUID.
    if Uuid::parse_str(&scope.team_id).is_ok() && scope.team_id != team {
        return Ok(false);
    }
    let Some(resource) = microsoft::teams_subscription_resource(scope.kind, team, channel, chat)
    else {
        return Ok(false);
    };
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM teams_container_watchers \
         WHERE ms_tenant_id=$1 AND resource=$2 AND account_id=$3 AND user_id=$4)",
    )
    .bind(&scope.tenant)
    .bind(resource)
    .bind(account)
    .bind(member)
    .fetch_one(control)
    .await
}

/// No app-mode org capture: this path reads only the sender's already selected
/// resource. "From now" confirms that existing stream; it does not reset or
/// broaden any stored history/backfill policy.
async fn handle_org_member_activity(
    state: &Arc<AppState>,
    control: &sqlx::PgPool,
    account_id: Uuid,
    a: &Activity,
    scope: &Scope,
    window: Option<&str>,
    claimed: &mut bool,
) {
    if window.is_some_and(|w| w != "none") {
        return;
    }
    let Some(aad) = sender_aad(a) else {
        return;
    };
    let org_id: Option<String> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
            .bind(account_id)
            .fetch_optional(control)
            .await
            .ok()
            .flatten()
            .flatten();
    let Some(org_id) = org_id else {
        return;
    };
    if !teams_ingest::org_microsoft_teams_enabled_for(state, &org_id).await {
        return;
    }
    let Some(member) = resolve_org_bot_member(control, account_id, &scope.tenant, &aad)
        .await
        .ok()
        .flatten()
    else {
        return;
    };
    if !matches!(crate::middleware::auth::live_org_membership(control, &org_id, member).await,
                 Ok(Some((live_account, _))) if live_account == account_id)
    {
        return;
    }
    let Some((content, _)) = resolve_teams_content_pool(state, control, account_id).await else {
        return;
    };
    let watch = own_delegated_watch(&content, account_id, member, scope)
        .await
        .ok()
        .flatten();
    let registered = if let Some((team, channel, chat)) = watch {
        registered_member_watch(control, account_id, member, scope, &team, &channel, &chat)
            .await
            .unwrap_or(false)
    } else {
        false
    };
    let enrolled = registered;
    if !claim_enrolled_card(claimed, enrolled) {
        return;
    }
    let Some(service_url) = a
        .service_url
        .as_deref()
        .filter(|u| is_trusted_service_url(u))
    else {
        return;
    };
    let Ok(token) = microsoft::bot_connector_token(&state.http_client, &scope.tenant).await else {
        return;
    };
    let manage_url = format!(
        "{}/connectors/microsoft-teams",
        state.personal_web_base_url().trim_end_matches('/')
    );
    let card = if enrolled {
        if window == Some("none") {
            status_card("✅ Saving is on for this conversation. New messages continue into your private ocean. Existing history settings are unchanged.")
        } else {
            branded_card(
                serde_json::json!([
                    {"type":"TextBlock", "text":"✅ Saving is on for this conversation.", "wrap":true},
                    {"type":"TextBlock", "text":"New messages go into your private ocean. Manage saved channels and history in CoralSwarm.", "wrap":true}
                ]),
                Some(serde_json::json!([
                    {"type":"Action.Submit", "title":"From now", "data":{"cs":"backfill", "window":"none"}},
                    {"type":"Action.OpenUrl", "title":"Manage what's saved", "url":manage_url}
                ])),
            )
        }
    } else {
        branded_card(
            serde_json::json!([
                {"type":"TextBlock", "text":"Choose this channel or chat in CoralSwarm to start saving it.", "wrap":true}
            ]),
            Some(serde_json::json!([
                {"type":"Action.OpenUrl", "title":"Manage what's saved", "url":manage_url}
            ])),
        )
    };
    // replyToId is caller-controlled: send a new acknowledgment instead of
    // allowing a click to overwrite another member's card in this channel.
    let result = microsoft::send_activity(
        &state.http_client,
        &token,
        service_url,
        &scope.conversation_id,
        &card,
    )
    .await
    .map(|_| ());
    match result {
        Ok(()) => {
            tracing::info!(%account_id, %member, enrolled, "teams bot: member capture card delivered")
        }
        Err(e) => tracing::warn!(error=%e, "teams bot: member capture card failed"),
    }
}

/// Single-conversation activation (a channel @mention in an already-seeded team,
/// or a group-chat install): start live capture and post the backfill prompt. The
/// watch is idempotent — re-@mentioning never duplicates the row (`install_watch`
/// reports `inserted = false`) — but the prompt is intentionally re-posted when
/// `force_prompt` is set (i.e. on an explicit @mention) so a prompt that never
/// landed self-heals. A silent install re-entry (`force_prompt = false`) only
/// prompts on first insert. CS-122.
///
/// `post_cards`: the multi-account fan-out (`process_activity`) calls this once
/// PER RESOLVED ACCOUNT so every account gets its own `install_watch` row, but
/// the backfill-prompt card is a property of the CONVERSATION, not of any one
/// account — posting it once per account would duplicate the visible message in
/// the same Teams channel/chat. `false` skips the card while still creating this
/// account's watch row; exactly one call in the fan-out passes `true`.
async fn activate_single(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    plane: crate::routes::slack::AccountPlane,
    account_id: Uuid,
    user_id: Uuid,
    scope: &Scope,
    a: &Activity,
    force_prompt: bool,
    post_cards: bool,
) {
    // `pool`/`plane` are resolved once by the caller (`resolve_teams_
    // content_pool`, m365 Train 2 PR 2.4) — Personal ⇒ the control/consumer
    // pool (byte-identical to pre-PR-2.4), Org ⇒ that account's own tenant
    // pool. See this module's own doc comment ("Plane-resolved content
    // pools").
    //
    // Non-attributable (member-private-oceans PR 5, plan §3): `user_id` here
    // is the representative member (`account_members ORDER BY joined_at LIMIT
    // 1`, see the caller above this function), not a verified acting user —
    // it must NOT be threaded into `resolve_primary_graph` as an attributed
    // caller, or an org account would silently pick an arbitrary member's
    // private ocean as the destination.
    match teams_ingest::install_watch(
        state,
        pool,
        account_id,
        user_id,
        plane,
        crate::routes::slack::IngestCaller::NonAttributable,
        scope.kind,
        &scope.team_id,
        &scope.channel_id,
        &scope.chat_id,
        &scope.tenant,
        "",
        Some(0),
    )
    .await
    {
        Ok((id, inserted)) => {
            tracing::info!(%account_id, watch_id = %id, kind = %scope.kind, inserted, "teams bot: added → live capture started");
            // Prompt on first activation, or whenever the user explicitly @mentioned
            // (force_prompt) so a re-@mention recovers a prompt that never landed.
            // Gated on `post_cards` — see this function's own doc comment.
            if post_cards && (inserted || force_prompt) {
                post_prompt(state, scope, a).await;
            }
            // Cold start is the first insert only. A later @mention re-posts
            // the backfill card and must not repeat the connect notice.
            if post_cards && inserted {
                post_teams_cold_start(state, account_id, scope, a).await;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, %account_id, kind = %scope.kind, "teams bot: install_watch failed")
        }
    }
}

/// Human-friendly label for the conversation in card copy.
fn scope_label(kind: &str) -> &'static str {
    if kind == "chat" {
        "this chat"
    } else {
        "this channel"
    }
}

/// Content-free connect notice, only when this account cannot email anyone
/// from a live roster grant. Refuses an untrusted `serviceUrl` before the
/// claim, so a bad activity cannot burn the daily slot.
async fn post_teams_cold_start(
    state: &Arc<AppState>,
    account_id: Uuid,
    scope: &Scope,
    a: &Activity,
) {
    let Some(control) = state.db.as_ref() else {
        return;
    };
    let Some(service_url) = a.service_url.as_deref() else {
        return;
    };
    if !is_trusted_service_url(service_url) {
        tracing::warn!(service_url = %service_url, "teams bot: refusing untrusted serviceUrl (cold start)");
        return;
    }
    crate::connectors::cold_start::maybe_post_teams_cold_start(
        state,
        control,
        account_id,
        &scope.tenant,
        service_url,
        &scope.conversation_id,
    )
    .await;
}

/// Post the initial confirmation + backfill prompt card when the bot is added.
async fn post_prompt(state: &Arc<AppState>, scope: &Scope, a: &Activity) {
    let Some(service_url) = a.service_url.clone() else {
        tracing::warn!("teams bot: install activity had no serviceUrl; cannot post prompt");
        return;
    };
    if !is_trusted_service_url(&service_url) {
        tracing::warn!(service_url = %service_url, "teams bot: refusing untrusted serviceUrl (prompt)");
        return;
    }
    let bot_token = match microsoft::bot_connector_token(&state.http_client, &scope.tenant).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "teams bot: connector token failed (prompt)");
            return;
        }
    };
    let card = prompt_card(scope_label(scope.kind));
    match microsoft::send_activity(
        &state.http_client,
        &bot_token,
        &service_url,
        &scope.conversation_id,
        &card,
    )
    .await
    {
        Ok(_) => tracing::info!(kind = %scope.kind, "teams bot: posted backfill prompt"),
        Err(e) => tracing::warn!(error = %e, "teams bot: failed to post prompt"),
    }
}

/// Fan out across a team on first contact: the channel that triggered this (the
/// one @mentioned / installed to) is activated directly with a backfill prompt —
/// that's explicit consent, and its watch also marks the team as onboarded so a
/// later @mention won't re-fan. Every *other* standard channel gets a consent
/// card and is **not** ingested until someone accepts it. Private/shared channels
/// are skipped (different RSC perms). CS-122.
///
/// `post_cards`: same gate as `activate_single`'s own doc comment — the
/// multi-account fan-out calls this once per resolved account so every account
/// gets its own entry-channel `install_watch` row, but the entry-channel prompt
/// AND the sibling consent-card fan-out are conversation-level outbound side
/// effects that must fire exactly once, not once per account.
async fn activate_team_channels(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    plane: crate::routes::slack::AccountPlane,
    account_id: Uuid,
    user_id: Uuid,
    scope: &Scope,
    a: &Activity,
    force_prompt: bool,
    post_cards: bool,
) {
    let Some(service_url) = a.service_url.clone() else {
        return;
    };
    if !is_trusted_service_url(&service_url) {
        tracing::warn!(service_url = %service_url, "teams bot: refusing untrusted serviceUrl (fan-out)");
        return;
    }
    let bot_token = match microsoft::bot_connector_token(&state.http_client, &scope.tenant).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "teams bot: fan-out connector token failed");
            return;
        }
    };

    // Activate the triggering channel first. `install_watch` is ON CONFLICT, so
    // `entry_inserted` tells us whether THIS call is the one that seeded the team.
    // Under a race (two concurrent @mentions) or a duplicate install event (Teams
    // fires both installationUpdate and conversationUpdate for one add), only the
    // first caller sees `inserted == true`; the rest re-prompt the entry channel
    // when explicitly @mentioned but MUST NOT re-fan consent cards. CS-122.
    // `pool`/`plane` are resolved once by the caller — see `activate_single`'s
    // own doc comment (m365 Train 2 PR 2.4).
    //
    // Non-attributable (member-private-oceans PR 5, plan §3) — same rationale
    // as `activate_single`: `user_id` is a representative member, not a
    // verified caller.
    let entry_inserted = match teams_ingest::install_watch(
        state,
        pool,
        account_id,
        user_id,
        plane,
        crate::routes::slack::IngestCaller::NonAttributable,
        "channel",
        &scope.team_id,
        &scope.channel_id,
        "",
        &scope.tenant,
        "",
        Some(0),
    )
    .await
    {
        Ok((_id, inserted)) => {
            // Gated on `post_cards` — see this function's own doc comment.
            if post_cards && (inserted || force_prompt) {
                let card = prompt_card(scope_label("channel"));
                // Reply into the *incoming* @mention conversation, not the bare
                // channel id — replying works without a team install, whereas a
                // proactive post to the channel needs the bot in its roster.
                if let Err(e) = microsoft::send_activity(
                    &state.http_client,
                    &bot_token,
                    &service_url,
                    &scope.conversation_id,
                    &card,
                )
                .await
                {
                    tracing::warn!(error = %e, channel = %scope.channel_id, "teams bot: entry-channel prompt post failed");
                }
            }
            inserted
        }
        Err(e) => {
            tracing::warn!(error = %e, channel = %scope.channel_id, "teams bot: entry-channel install_watch failed");
            false
        }
    };
    if post_cards && entry_inserted {
        post_teams_cold_start(state, account_id, scope, a).await;
    }

    // The sibling consent-card fan-out below is entirely outbound (no watch
    // row of its own — a sibling only gets `install_watch`ed later, from
    // `handle_offer`'s accept path, when a human clicks the card), so a
    // non-card-posting account has nothing left to do here.
    if !post_cards {
        return;
    }

    // Only the first seeding of a team fans consent cards to its other channels; a
    // re-fan (duplicate install event / lost race) stops here so siblings are never
    // double-carded. This also avoids the extra app_token + list_channels calls.
    if !entry_inserted {
        tracing::info!(%account_id, team = %scope.team_id, "teams bot: team already seeded — no sibling fan-out");
        return;
    }

    let token = match microsoft::app_token(&state.http_client, &scope.tenant).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "teams bot: fan-out app token failed");
            return;
        }
    };
    let channels = match microsoft::list_channels(&state.http_client, &token, &scope.team_id).await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, team = %scope.team_id, "teams bot: fan-out list channels failed");
            return;
        }
    };

    let mut offered = 0usize;
    for ch in channels {
        if ch.id == scope.channel_id {
            continue; // entry channel already activated above
        }
        // Standard channels only — private/shared need different RSC permissions.
        if ch
            .membership_type
            .as_deref()
            .is_some_and(|m| m != "standard")
        {
            continue;
        }
        // Ask first — nothing is ingested in a sibling until someone accepts.
        let card = offer_card(&scope.team_id, &ch.id, &ch.display_name);
        if let Err(e) =
            microsoft::send_activity(&state.http_client, &bot_token, &service_url, &ch.id, &card)
                .await
        {
            tracing::warn!(error = %e, channel = %ch.id, "teams bot: offer card post failed");
        } else {
            offered += 1;
        }
    }
    tracing::info!(%account_id, team = %scope.team_id, offered, "teams bot: fanned out — entry channel active, siblings offered consent");
}

/// Handle a backfill-window click: lock the choice (no-op if already chosen),
/// then update the card through "⏳ ingesting…" → final count.
async fn handle_choice(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    a: &Activity,
    scope: &Scope,
    window: &str,
) {
    // `pool` is resolved once by the caller (`resolve_teams_content_pool`,
    // m365 Train 2 PR 2.4) — see `activate_single`'s own doc comment.
    let Some(service_url) = a.service_url.clone() else {
        return;
    };
    if !is_trusted_service_url(&service_url) {
        tracing::warn!(service_url = %service_url, "teams bot: refusing untrusted serviceUrl (choice)");
        return;
    }
    // Absent replyToId means there is no card to update in place — skip rather than
    // issue an update against an empty activity id.
    let Some(reply_to_id) = a.reply_to_id.clone() else {
        return;
    };

    // Lock: claim the choice only if not already made (stale/double click → no-op).
    let claim = teams_ingest::claim_history_choice(
        pool,
        account_id,
        scope.kind,
        &scope.channel_id,
        &scope.chat_id,
        window,
    )
    .await;
    let Some((graph_id, user_id, tenant_id, w_team_id, w_channel_id, w_chat_id)) = claim else {
        tracing::info!(%account_id, kind = %scope.kind, window, "teams bot: backfill choice ignored (already chosen / unknown watch)");
        return;
    };

    let label = scope_label(scope.kind);
    let bot_token = match microsoft::bot_connector_token(&state.http_client, &tenant_id).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "teams bot: connector token failed (choice)");
            return;
        }
    };
    let update = |text: String| {
        let http = state.http_client.clone();
        let token = bot_token.clone();
        let su = service_url.clone();
        let cid = scope.conversation_id.clone();
        let rid = reply_to_id.clone();
        async move {
            if let Err(e) =
                microsoft::update_activity(&http, &token, &su, &cid, &rid, &status_card(&text))
                    .await
            {
                tracing::warn!(error = %e, "teams bot: card update failed");
            }
        }
    };

    if window == "none" {
        update(format!(
            "✅ Active in {label} — capturing new messages only."
        ))
        .await;
        return;
    }

    // `window` arrives in the (signed) Action.Submit payload but is still
    // client-shaped — accept only the known tokens. Anything unrecognized or
    // non-positive falls back to capture-forward, never a silent full-history
    // backfill mislabeled as "the last <garbage> days". CS-122.
    let days = match window {
        "all" => None, // explicit full history
        d => match d.parse::<i64>() {
            Ok(n) if n > 0 => Some(n),
            _ => {
                tracing::warn!(%account_id, window, "teams bot: unrecognized backfill window; capturing new messages only");
                update(format!(
                    "✅ Active in {label} — capturing new messages only."
                ))
                .await;
                return;
            }
        },
    };
    let since = days.map(|n| Utc::now() - chrono::Duration::days(n));
    let window_label = match days {
        None => "all history".to_string(),
        Some(n) => format!("the last {n} days"),
    };
    update(format!("⏳ Ingesting {window_label} in {label}…")).await;

    // Use the ids captured at activation (the install/@mention carried the team's
    // AAD group id; the click does not) so /teams/{aadGroupId} matches the RSC grant.
    let (n, complete) = teams_ingest::backfill_history(
        state.clone(),
        pool.clone(),
        account_id,
        user_id,
        graph_id,
        tenant_id,
        scope.kind.to_string(),
        w_team_id,
        w_channel_id,
        w_chat_id,
        since,
    )
    .await;

    let plural = if n == 1 { "" } else { "s" };
    let note = if window == "all" {
        " (most recent history)"
    } else {
        ""
    };
    let text = if complete {
        format!("✅ Active in {label} — ingested {n} message{plural} from {window_label}{note}.")
    } else {
        format!(
            "✅ Active in {label} — ingested {n} message{plural} so far from {window_label}; \
             the rest is still importing in the background."
        )
    };
    update(text).await;
}

/// Handle a per-channel consent ("offer") click from the fan-out. Accept → create
/// the watch and replace the card with the backfill prompt; decline → replace with
/// a dismissal. The team/channel ids ride in the action payload because the click
/// activity doesn't carry the team's AAD group id. CS-122.
#[allow(clippy::too_many_arguments)]
async fn handle_offer(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    plane: crate::routes::slack::AccountPlane,
    account_id: Uuid,
    user_id: Uuid,
    a: &Activity,
    scope: &Scope,
    accept: bool,
    team_id: &str,
    channel_id: &str,
    name: &str,
) {
    let Some(service_url) = a.service_url.clone() else {
        return;
    };
    if !is_trusted_service_url(&service_url) {
        tracing::warn!(service_url = %service_url, "teams bot: refusing untrusted serviceUrl (offer)");
        return;
    }
    let Some(reply_to_id) = a.reply_to_id.clone() else {
        return;
    };
    let bot_token = match microsoft::bot_connector_token(&state.http_client, &scope.tenant).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "teams bot: connector token failed (offer)");
            return;
        }
    };
    // Replace the consent card in place (same conversation + activity id).
    let replace = |card: serde_json::Value| {
        let http = state.http_client.clone();
        let token = bot_token.clone();
        let su = service_url.clone();
        let cid = scope.conversation_id.clone();
        let rid = reply_to_id.clone();
        async move {
            if let Err(e) = microsoft::update_activity(&http, &token, &su, &cid, &rid, &card).await
            {
                tracing::warn!(error = %e, "teams bot: offer card update failed");
            }
        }
    };

    if !accept {
        replace(status_card(
            "Okay — I won't ingest this channel. @mention me here anytime to start.",
        ))
        .await;
        tracing::info!(%account_id, channel = %channel_id, "teams bot: offer declined");
        return;
    }

    // The ids come from the Action.Submit payload; a malformed accept (missing
    // `team`/`chan`) must not create a watch keyed on empty ids or subscribe against
    // an empty Graph path. CS-122.
    if team_id.is_empty() || channel_id.is_empty() {
        tracing::warn!(%account_id, "teams bot: offer accept missing team/channel id; ignoring");
        replace(status_card(
            "Couldn't start ingestion — missing channel reference. @mention me in the channel to start.",
        ))
        .await;
        return;
    }

    // `pool`/`plane` are resolved once by the caller — see `activate_single`'s
    // own doc comment (m365 Train 2 PR 2.4).
    //
    // Non-attributable (member-private-oceans PR 5, plan §3) — same rationale
    // as `activate_single`: `user_id` is a representative member, not a
    // verified caller (a consent-card click carries no CoralSwarm session
    // either).
    match teams_ingest::install_watch(
        state,
        pool,
        account_id,
        user_id,
        plane,
        crate::routes::slack::IngestCaller::NonAttributable,
        "channel",
        team_id,
        channel_id,
        "",
        &scope.tenant,
        name,
        Some(0),
    )
    .await
    {
        Ok((id, inserted)) => {
            tracing::info!(%account_id, watch_id = %id, channel = %channel_id, inserted, "teams bot: offer accepted → live capture started");
            replace(prompt_card(scope_label("channel"))).await;
        }
        Err(e) => {
            tracing::warn!(error = %e, %account_id, channel = %channel_id, "teams bot: offer accept install_watch failed");
            replace(status_card(
                "Couldn't start ingestion just now — please @mention me here to retry.",
            ))
            .await;
        }
    }
}

/// Build the `message` + Adaptive Card attachment envelope shared by every card we
/// post. `body` is the AdaptiveCard body array; `actions` is the optional actions
/// array (None → a buttonless status card). Hoists the contentType / schema /
/// version that were previously duplicated across the three builders. CS-122.
fn adaptive_card(body: serde_json::Value, actions: Option<serde_json::Value>) -> serde_json::Value {
    let mut content = serde_json::json!({
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.4",
        "body": body,
    });
    if let Some(actions) = actions {
        content["actions"] = actions;
    }
    serde_json::json!({
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": content,
        }]
    })
}

/// The CoralSwarm mark shown at the top of every user-facing card — the same
/// hosted icon the Gmail add-on and the add-in panes use.
const CARD_LOGO_URL: &str = "https://app.coralswarm.example/addin/gmail-icon.png";

/// `adaptive_card` with the CoralSwarm logo + name header prepended to the body.
/// Branding only: the actions and every submit payload are the caller's, untouched.
fn branded_card(body: serde_json::Value, actions: Option<serde_json::Value>) -> serde_json::Value {
    let mut items = vec![serde_json::json!({
        "type": "ColumnSet",
        "columns": [
            {"type": "Column", "width": "auto", "items": [
                {"type": "Image", "url": CARD_LOGO_URL, "altText": "CoralSwarm", "width": "24px", "height": "24px"}
            ]},
            {"type": "Column", "width": "stretch", "verticalContentAlignment": "Center", "items": [
                {"type": "TextBlock", "text": "CoralSwarm", "weight": "Bolder", "wrap": true}
            ]}
        ]
    })];
    if let Some(rest) = body.as_array() {
        items.extend(rest.iter().cloned());
    }
    adaptive_card(serde_json::Value::Array(items), actions)
}

/// Per-channel **consent** card for fan-out siblings (Action.Submit). Accepting
/// carries the team's AAD group id, the channel id, and its name in the payload so
/// the click handler can create the watch without re-deriving them. CS-122.
fn offer_card(team_id: &str, channel_id: &str, name: &str) -> serde_json::Value {
    branded_card(
        serde_json::json!([
            {"type": "TextBlock", "text": "Should CoralSwarm save this channel?", "wrap": true, "weight": "Bolder"},
            {"type": "TextBlock", "text": "I won't save anything here until you say yes.", "wrap": true, "isSubtle": true}
        ]),
        Some(serde_json::json!([
            {"type": "Action.Submit", "title": "Yes, save this channel", "data": {"cs": "offer", "act": "accept", "team": team_id, "chan": channel_id, "name": name}},
            {"type": "Action.Submit", "title": "No thanks", "data": {"cs": "offer", "act": "decline"}}
        ])),
    )
}

/// The confirmation + backfill prompt Adaptive Card (Action.Submit buttons).
fn prompt_card(label: &str) -> serde_json::Value {
    branded_card(
        serde_json::json!([
            {"type": "TextBlock", "text": format!("✅ CoralSwarm is now saving {label}."), "wrap": true, "weight": "Bolder"},
            {"type": "TextBlock", "text": "How much past history should I bring in?", "wrap": true, "isSubtle": true}
        ]),
        Some(serde_json::json!([
            {"type": "Action.Submit", "title": "From now", "data": {"cs": "backfill", "window": "none"}},
            {"type": "Action.Submit", "title": "Last 7 days", "data": {"cs": "backfill", "window": "7"}},
            {"type": "Action.Submit", "title": "Last 30 days", "data": {"cs": "backfill", "window": "30"}},
            {"type": "Action.Submit", "title": "Last 90 days", "data": {"cs": "backfill", "window": "90"}},
            {"type": "Action.Submit", "title": "All history", "data": {"cs": "backfill", "window": "all"}}
        ])),
    )
}

/// A buttonless status card (progress / final), replacing the prompt in place.
fn status_card(text: &str) -> serde_json::Value {
    adaptive_card(
        serde_json::json!([
            {"type": "TextBlock", "text": text, "wrap": true}
        ]),
        None,
    )
}

// ── Bot Framework JWT validation (modeled on clerk.rs) ───────────────────────

#[derive(Debug, Deserialize)]
struct BotClaims {
    // `iss`/`aud`/`exp` are enforced declaratively via `Validation`.
    #[serde(default)]
    #[allow(dead_code)]
    iss: Option<String>,
    /// The Bot Framework binds the activity's destination into the token: this
    /// claim must equal the activity's `serviceUrl`. We read it so the caller can
    /// reject a validly-signed token replayed against a different serviceUrl (#134).
    // Bot Framework signs lowercase `serviceurl`; the activity uses `serviceUrl`.
    #[serde(default, rename = "serviceurl", alias = "serviceUrl")]
    service_url: Option<String>,
}

struct CachedKeys {
    keys: HashMap<String, DecodingKey>,
    fetched: Instant,
}

fn key_cache() -> &'static RwLock<Option<CachedKeys>> {
    static CACHE: OnceLock<RwLock<Option<CachedKeys>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(None))
}

const JWKS_TTL: Duration = Duration::from_secs(60 * 60 * 24); // keys rotate slowly

/// The bot/app id the inbound token must be addressed to (`aud`). Reuses the
/// app registration's client id only when no separate bot identity is configured.
fn bot_app_id() -> Result<String, String> {
    std::env::var("MICROSOFT_BOT_APP_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("MICROSOFT_CLIENT_ID")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .ok_or_else(|| "MICROSOFT_BOT_APP_ID / MICROSOFT_CLIENT_ID not set".to_string())
}

async fn bot_signing_keys(
    http: &reqwest::Client,
    force: bool,
) -> Result<HashMap<String, DecodingKey>, String> {
    if !force {
        if let Some(c) = key_cache().read().await.as_ref() {
            if c.fetched.elapsed() < JWKS_TTL {
                return Ok(c.keys.clone());
            }
        }
    }

    #[derive(Deserialize)]
    struct OpenIdConfig {
        jwks_uri: String,
    }
    #[derive(Deserialize)]
    struct Jwk {
        kid: String,
        n: String,
        e: String,
    }
    #[derive(Deserialize)]
    struct Jwks {
        keys: Vec<Jwk>,
    }

    let cfg: OpenIdConfig = http
        .get(BOT_OPENID_CONFIG)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    let jwks: Jwks = http
        .get(&cfg.jwks_uri)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;

    let mut out = HashMap::new();
    for k in jwks.keys {
        if let Ok(key) = DecodingKey::from_rsa_components(&k.n, &k.e) {
            out.insert(k.kid, key);
        }
    }
    if out.is_empty() {
        return Err("no usable RSA keys in bot JWKS".into());
    }
    *key_cache().write().await = Some(CachedKeys {
        keys: out.clone(),
        fetched: Instant::now(),
    });
    Ok(out)
}

/// Validates the inbound Bot Framework JWT (issuer, audience, expiry, RS256
/// signature against the rotating JWKS) and returns its `serviceUrl` claim so the
/// caller can bind it to the activity's serviceUrl.
async fn validate_bot_jwt(
    headers: &HeaderMap,
    http: &reqwest::Client,
) -> Result<Option<String>, String> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or("missing bearer token")?;

    let audience = bot_app_id()?;
    let kid = decode_header(token)
        .map_err(|e| e.to_string())?
        .kid
        .ok_or("missing kid")?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[BOT_ISSUER]);
    validation.set_audience(&[audience]);
    validation.validate_exp = true;

    // Resolve key by kid, refetching once on a miss (key rotation).
    for force in [false, true] {
        let keys = bot_signing_keys(http, force).await?;
        if let Some(key) = keys.get(&kid) {
            return decode::<BotClaims>(token, key, &validation)
                .map(|data| data.claims.service_url)
                .map_err(|e| e.to_string());
        }
    }
    Err("unknown signing key (kid)".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn org_member_actor_and_card_scope_are_never_taken_from_payload() {
        let aad = Uuid::new_v4();
        let a: Activity = serde_json::from_value(serde_json::json!({
            "type":"message", "from":{"aadObjectId":aad.to_string()},
            "conversation":{"id":"signed-channel", "conversationType":"channel"},
            "channelData":{"tenant":{"id":"signed-tenant"},
                "team":{"aadGroupId":"signed-team"}, "channel":{"id":"signed-channel"}},
            "value":{"user":"forged-member", "team":"forged-team", "chan":"forged-channel"}
        }))
        .unwrap();
        assert_eq!(sender_aad(&a), Some(aad.to_string()));
        let scope = extract_scope(&a).unwrap();
        assert_eq!(scope.channel_id, "signed-channel");
        assert_eq!(scope.team_id, "signed-team");
        let invalid: Activity = serde_json::from_value(serde_json::json!({
            "from":{"id":aad.to_string(), "aadObjectId":"invalid"},
            "value":{"aadObjectId":aad.to_string()}
        }))
        .unwrap();
        assert!(sender_aad(&invalid).is_none());
        let with_id: Activity = serde_json::from_value(serde_json::json!({
            "id":"  a:retry-stable  ", "type":"message",
            "from":{"aadObjectId":aad.to_string()}
        }))
        .unwrap();
        assert_eq!(
            mention_enrollment_key(&with_id).as_deref(),
            Some("a:retry-stable")
        );
        assert!(mention_enrollment_key(&invalid).is_none());
        assert_eq!(unique_member(&[aad]), Some(aad));
        assert!(unique_member(&[]).is_none());
        assert!(unique_member(&[aad, Uuid::new_v4()]).is_none());
        let mut claimed = false;
        assert!(!claim_enrolled_card(&mut claimed, false));
        assert!(
            !claimed,
            "an unwatched first account cannot starve the enrolled account"
        );
        assert!(claim_enrolled_card(&mut claimed, true));
        assert!(!claim_enrolled_card(&mut claimed, true));
    }

    #[tokio::test]
    async fn org_member_bot_live_grant_and_own_watch_security_pg() {
        let Ok(url) = std::env::var("TEAMS_BOT_MEMBER_TEST_DATABASE_URL") else {
            return;
        };
        // Dedicated disposable test database only; never the repo's default DB.
        let admin = sqlx::PgPool::connect(&url).await.unwrap();
        sqlx::raw_sql(r#"
            CREATE ROLE bot_member_app NOLOGIN;
            CREATE TABLE accounts(id uuid PRIMARY KEY, clerk_org_id text);
            CREATE TABLE account_members(account_id uuid, user_id uuid, role text);
            CREATE TABLE org_revocations(clerk_org_id text);
            CREATE TABLE connector_installs(account_id uuid, provider text,
                external_workspace_id text, external_user_id text, owner_user_id uuid);
            CREATE TABLE oauth_tokens(account_id uuid, provider text,
                provider_workspace_id text, provider_user_id text, owner_user_id uuid,
                credential_invalid_at timestamptz);
            CREATE TABLE teams_watched_resources(account_id uuid, user_id uuid,
                auth_mode text, kind text, team_id text, channel_id text, chat_id text, tenant_id text);
            CREATE TABLE teams_container_watchers(ms_tenant_id text, resource text,
                account_id uuid, user_id uuid);
            ALTER TABLE oauth_tokens ENABLE ROW LEVEL SECURITY;
            CREATE POLICY account_scope ON oauth_tokens USING
                (account_id = NULLIF(current_setting('app.account_id', true),'')::uuid);
            ALTER TABLE teams_watched_resources ENABLE ROW LEVEL SECURITY;
            CREATE POLICY member_scope ON teams_watched_resources USING
                (account_id = NULLIF(current_setting('app.account_id', true),'')::uuid
                 AND user_id = NULLIF(current_setting('app.user_id', true),'')::uuid);
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO bot_member_app;
        "#).execute(&admin).await.unwrap();
        let app = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|c, _| {
                Box::pin(async move {
                    sqlx::query("SET ROLE bot_member_app").execute(c).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        let account = Uuid::new_v4();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let aad = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO accounts VALUES($1,'org_bot_test')")
            .bind(account)
            .execute(&admin)
            .await
            .unwrap();
        for owner in [a, b] {
            sqlx::query("INSERT INTO account_members VALUES($1,$2,'member')")
                .bind(account)
                .bind(owner)
                .execute(&admin)
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO connector_installs VALUES($1,'microsoft','tenant',$2,$3)")
            .bind(account)
            .bind(&aad)
            .bind(a)
            .execute(&admin)
            .await
            .unwrap();
        assert!(
            resolve_org_bot_member(&app, account, "tenant", &aad)
                .await
                .unwrap()
                .is_none(),
            "stale index without grant"
        );
        sqlx::query("INSERT INTO oauth_tokens VALUES($1,'microsoft','tenant',$2,$3,NULL)")
            .bind(account)
            .bind(&aad)
            .bind(a)
            .execute(&admin)
            .await
            .unwrap();
        assert_eq!(
            resolve_org_bot_member(&app, account, "tenant", &aad)
                .await
                .unwrap(),
            Some(a)
        );
        assert!(resolve_org_bot_member(&app, account, "other-tenant", &aad)
            .await
            .unwrap()
            .is_none());
        assert!(
            resolve_org_bot_member(&app, account, "tenant", "unknown-aad")
                .await
                .unwrap()
                .is_none()
        );
        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=now()")
            .execute(&admin)
            .await
            .unwrap();
        assert!(resolve_org_bot_member(&app, account, "tenant", &aad)
            .await
            .unwrap()
            .is_none());
        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at=NULL")
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query("DELETE FROM account_members WHERE user_id=$1")
            .bind(a)
            .execute(&admin)
            .await
            .unwrap();
        assert!(resolve_org_bot_member(&app, account, "tenant", &aad)
            .await
            .unwrap()
            .is_none());
        sqlx::query("INSERT INTO account_members VALUES($1,$2,'member')")
            .bind(account)
            .bind(a)
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query("INSERT INTO connector_installs VALUES($1,'microsoft','tenant',$2,$3)")
            .bind(account)
            .bind(&aad)
            .bind(b)
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query("INSERT INTO oauth_tokens VALUES($1,'microsoft','tenant',$2,$3,NULL)")
            .bind(account)
            .bind(&aad)
            .bind(b)
            .execute(&admin)
            .await
            .unwrap();
        assert!(
            resolve_org_bot_member(&app, account, "tenant", &aad)
                .await
                .unwrap()
                .is_none(),
            "ambiguous owners refuse"
        );
        sqlx::query("DELETE FROM oauth_tokens WHERE owner_user_id=$1")
            .bind(b)
            .execute(&admin)
            .await
            .unwrap();
        assert!(
            crate::middleware::auth::live_org_membership(&app, "org_bot_test", a)
                .await
                .unwrap()
                .is_some()
        );
        sqlx::query("INSERT INTO org_revocations VALUES('org_bot_test')")
            .execute(&admin)
            .await
            .unwrap();
        assert!(
            crate::middleware::auth::live_org_membership(&app, "org_bot_test", a)
                .await
                .unwrap()
                .is_none()
        );
        let scope = Scope {
            tenant: "tenant".into(),
            kind: "channel",
            team_id: "opaque-team".into(),
            channel_id: "coral".into(),
            chat_id: String::new(),
            conversation_id: "coral".into(),
        };
        sqlx::query("INSERT INTO teams_watched_resources VALUES($1,$2,'delegated','channel','stored-team','coral','','')")
            .bind(account).bind(a).execute(&admin).await.unwrap();
        let own = own_delegated_watch(&app, account, a, &scope)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(own.0, "stored-team");
        assert!(
            own_delegated_watch(&app, account, b, &scope)
                .await
                .unwrap()
                .is_none(),
            "another member's watch invisible"
        );
        assert!(
            !registered_member_watch(&app, account, a, &scope, &own.0, &own.1, &own.2)
                .await
                .unwrap(),
            "watch alone does not prove tenant enrollment"
        );
        let resource =
            microsoft::teams_subscription_resource("channel", "stored-team", "coral", "").unwrap();
        sqlx::query("INSERT INTO teams_container_watchers VALUES('tenant',$1,$2,$3)")
            .bind(&resource)
            .bind(account)
            .bind(a)
            .execute(&admin)
            .await
            .unwrap();
        assert!(
            registered_member_watch(&app, account, a, &scope, &own.0, &own.1, &own.2)
                .await
                .unwrap()
        );
        let wrong = Scope {
            tenant: "other-tenant".into(),
            ..scope
        };
        assert!(
            !registered_member_watch(&app, account, a, &wrong, &own.0, &own.1, &own.2)
                .await
                .unwrap()
        );
        sqlx::query("UPDATE teams_watched_resources SET tenant_id='different-tenant'")
            .execute(&admin)
            .await
            .unwrap();
        assert!(own_delegated_watch(&app, account, a, &wrong)
            .await
            .unwrap()
            .is_none());
        app.close().await;
        admin.close().await;
    }

    #[test]
    fn claim_card_delivery_gives_cards_to_the_first_caller_only() {
        let mut claimed = false;
        assert!(
            claim_card_delivery(&mut claimed),
            "the first caller claims it"
        );
        assert!(
            !claim_card_delivery(&mut claimed),
            "a second caller does not"
        );
        assert!(!claim_card_delivery(&mut claimed), "nor does a third");
    }

    /// The regression this whole mechanism exists to prevent (CodeRabbit
    /// review, PR #667): `process_activity` never calls
    /// `claim_card_delivery` at all for an account skipped by the
    /// representative-user lookup or content-pool resolution — it
    /// `continue`s past those gates first. So "account 0 is unresolvable,
    /// account 1 is usable" is exactly "the first CALL to `claim_card_
    /// delivery` happens for account 1", proven here without needing a
    /// database: the flag must still be unclaimed when the first usable
    /// account reaches it, whatever happened to earlier accounts that never
    /// called this function at all.
    #[test]
    fn claim_card_delivery_gives_cards_to_the_first_account_that_actually_reaches_it() {
        let mut claimed = false;
        // Account 0 was unresolvable and `continue`d — no call here at all,
        // simulating the skip.
        // Account 1 IS usable and is the first to call this function this
        // activity — it must claim the cards, not be silently starved the
        // way a fixed `idx == 0` check would have starved it.
        assert!(
            claim_card_delivery(&mut claimed),
            "the first account to actually reach dispatch must claim card delivery, even \
             though it was not literally index 0 in the resolver's result set"
        );
    }

    #[test]
    fn prompt_card_offers_five_windows_as_submit_actions() {
        let card = prompt_card("this channel");
        let content = &card["attachments"][0]["content"];
        let actions = content["actions"].as_array().expect("actions");
        assert_eq!(actions.len(), 5);
        let windows: Vec<&str> = actions
            .iter()
            .map(|a| a["data"]["window"].as_str().unwrap())
            .collect();
        assert_eq!(windows, ["none", "7", "30", "90", "all"]);
        for a in actions {
            assert_eq!(a["type"].as_str(), Some("Action.Submit"));
            assert_eq!(a["data"]["cs"].as_str(), Some("backfill"));
        }
        assert!(content["body"][1]["text"]
            .as_str()
            .unwrap()
            .contains("this channel"));
    }

    #[test]
    fn branded_cards_lead_with_the_logo_and_keep_actions_untouched() {
        let plain = adaptive_card(
            serde_json::json!([{"type": "TextBlock", "text": "hi"}]),
            Some(serde_json::json!([{"type": "Action.OpenUrl", "title": "t", "url": "u"}])),
        );
        let branded = branded_card(
            serde_json::json!([{"type": "TextBlock", "text": "hi"}]),
            Some(serde_json::json!([{"type": "Action.OpenUrl", "title": "t", "url": "u"}])),
        );
        let (p, b) = (
            &plain["attachments"][0]["content"],
            &branded["attachments"][0]["content"],
        );
        assert_eq!(p["actions"], b["actions"]);
        assert_eq!(b["body"][0]["type"].as_str(), Some("ColumnSet"));
        assert_eq!(
            b["body"][0]["columns"][0]["items"][0]["url"].as_str(),
            Some("https://app.coralswarm.example/addin/gmail-icon.png")
        );
        assert_eq!(b["body"][1], p["body"][0]);
    }

    #[test]
    fn meeting_install_path_never_authorizes_capture() {
        // Source guard (no Teams/Graph mock exists for this handler): the
        // production half of this file must not reach the capture ledger.
        let src = include_str!("teams_bot.rs");
        let prod = &src[..src.find("#[cfg(test)]\nmod tests").expect("test module marker")];
        for needle in ["authorize_with_window", "ensure_auto_transcription", "\"meeting_bot\""] {
            assert!(!prod.contains(needle), "bot path must not call {needle}");
        }
        // Context rows are still written on install.
        assert!(prod.contains("store_context("));
    }

    #[test]
    fn install_card_does_not_claim_capture_is_on() {
        assert_eq!(
            INSTALL_CARD_TEXT,
            "Turn on recording or transcription, then add this meeting from the CoralSwarm tab."
        );
        let lower = INSTALL_CARD_TEXT.to_lowercase();
        assert!(!lower.contains("saving this meeting") && !lower.contains("automatically"));
    }

    #[test]
    fn card_copy_says_save_not_capture() {
        let cards = [prompt_card("this channel"), offer_card("g", "c", "n")];
        for card in cards {
            let text = card.to_string().to_lowercase();
            assert!(!text.contains("capture"), "{text}");
            assert!(!text.contains("ingest"), "{text}");
        }
    }

    #[test]
    fn offer_card_asks_consent_and_carries_ids() {
        let card = offer_card("group-123", "19:chan@thread.tacv2", "Coral");
        let content = &card["attachments"][0]["content"];
        assert!(content["body"][1]["text"]
            .as_str()
            .unwrap()
            .contains("save this channel"));
        let actions = content["actions"].as_array().expect("actions");
        assert_eq!(actions.len(), 2);
        // Accept carries the ids needed to create the watch on click.
        let accept = &actions[0]["data"];
        assert_eq!(accept["cs"].as_str(), Some("offer"));
        assert_eq!(accept["act"].as_str(), Some("accept"));
        assert_eq!(accept["team"].as_str(), Some("group-123"));
        assert_eq!(accept["chan"].as_str(), Some("19:chan@thread.tacv2"));
        assert_eq!(accept["name"].as_str(), Some("Coral"));
        // Decline is a bare marker.
        assert_eq!(actions[1]["data"]["act"].as_str(), Some("decline"));
    }

    #[test]
    fn service_url_allowlist_accepts_teams_hosts_only() {
        // Real Teams hosts.
        assert!(is_trusted_service_url(
            "https://smba.trafficmanager.net/amer/"
        ));
        assert!(is_trusted_service_url(
            "https://smba.trafficmanager.net/emea/abc/"
        ));
        assert!(is_trusted_service_url("https://eu.botframework.com/"));
        // Rejected: wrong scheme, attacker hosts, lookalikes, garbage.
        assert!(!is_trusted_service_url(
            "http://smba.trafficmanager.net/amer/"
        )); // not https
        assert!(!is_trusted_service_url("https://evil.example.com/"));
        assert!(!is_trusted_service_url(
            "https://smba.trafficmanager.net.evil.com/"
        ));
        assert!(!is_trusted_service_url(
            "https://botframework.com.evil.com/"
        ));
        // #134: self-service Traffic Manager subdomains are NOT Microsoft-operated.
        assert!(!is_trusted_service_url(
            "https://attacker.trafficmanager.net/"
        ));
        assert!(!is_trusted_service_url(
            "https://smba2.trafficmanager.net/amer/"
        ));
        assert!(!is_trusted_service_url("not a url"));
    }

    #[test]
    fn service_url_must_match_signed_claim() {
        // Exact match and trailing-slash variance both pass.
        assert!(service_url_matches_claim(
            Some("https://smba.trafficmanager.net/amer/"),
            Some("https://smba.trafficmanager.net/amer/"),
        ));
        assert!(service_url_matches_claim(
            Some("https://smba.trafficmanager.net/amer"),
            Some("https://smba.trafficmanager.net/amer/"),
        ));
        // #134: a signed token can't be replayed against a different serviceUrl.
        assert!(!service_url_matches_claim(
            Some("https://attacker.example.com/"),
            Some("https://smba.trafficmanager.net/amer/"),
        ));
        // Region mismatch is also rejected.
        assert!(!service_url_matches_claim(
            Some("https://smba.trafficmanager.net/emea/"),
            Some("https://smba.trafficmanager.net/amer/"),
        ));
        // Fails closed: activity wants to act but the token bound no serviceUrl.
        assert!(!service_url_matches_claim(
            Some("https://smba.trafficmanager.net/amer/"),
            None
        ));
        // Nothing to act on → vacuously satisfied (handlers bail anyway).
        assert!(service_url_matches_claim(
            None,
            Some("https://smba.trafficmanager.net/amer/")
        ));
        assert!(service_url_matches_claim(None, None));
    }

    #[test]
    fn lowercase_serviceurl_survives_verified_jwt_decode() {
        // Exercise the same JWT-to-BotClaims deserialization as production,
        // using a local signing key rather than Microsoft's rotating RSA keys.
        let url = "https://smba.trafficmanager.net/amer/84c4ead0-dfe0-4770-9d9f-b819c4cc0122/";
        let claims = serde_json::json!({
            "iss": BOT_ISSUER, "aud": "test-bot", "exp": 4_000_000_000u64,
            "serviceurl": url,
        });
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(b"local-test-key"),
        )
        .unwrap();
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[BOT_ISSUER]);
        validation.set_audience(&["test-bot"]);
        let verified = decode::<BotClaims>(
            &token,
            &DecodingKey::from_secret(b"local-test-key"),
            &validation,
        )
        .unwrap()
        .claims;
        assert_eq!(verified.service_url.as_deref(), Some(url));
        assert!(service_url_matches_claim(
            Some(url),
            verified.service_url.as_deref()
        ));
        assert!(!service_url_matches_claim(
            Some("https://smba.trafficmanager.net/emea/"),
            verified.service_url.as_deref(),
        ));
    }

    #[test]
    fn serviceurl_alias_missing_and_conflicting_claims_remain_safe() {
        let url = "https://smba.trafficmanager.net/amer/";
        let legacy: BotClaims =
            serde_json::from_value(serde_json::json!({"serviceUrl": url})).unwrap();
        assert!(service_url_matches_claim(
            Some(url),
            legacy.service_url.as_deref()
        ));
        let missing: BotClaims = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(!service_url_matches_claim(
            Some(url),
            missing.service_url.as_deref()
        ));
        assert!(
            serde_json::from_value::<BotClaims>(serde_json::json!({
                "serviceurl": url, "serviceUrl": "https://smba.trafficmanager.net/emea/",
            }))
            .is_err(),
            "ambiguous duplicated claim must fail closed"
        );
    }

    #[test]
    fn status_card_is_buttonless() {
        let card = status_card("done");
        let content = &card["attachments"][0]["content"];
        assert!(content.get("actions").is_none());
        assert_eq!(content["body"][0]["text"].as_str(), Some("done"));
    }

    #[test]
    fn channel_scope_prefers_aad_group_id() {
        let a: Activity = serde_json::from_value(serde_json::json!({
            "type": "installationUpdate",
            "action": "add",
            "conversation": {"id": "19:chan@thread.tacv2", "conversationType": "channel"},
            "channelData": {
                "team": {"id": "19:teamthread", "aadGroupId": "group-123"},
                "channel": {"id": "19:chan@thread.tacv2"},
                "tenant": {"id": "tenant-abc"}
            }
        }))
        .unwrap();
        let s = extract_scope(&a).expect("channel scope");
        assert_eq!(s.kind, "channel");
        assert_eq!(s.team_id, "group-123"); // aadGroupId, not the team thread id
        assert_eq!(s.channel_id, "19:chan@thread.tacv2");
        assert_eq!(s.tenant, "tenant-abc");
        assert!(s.chat_id.is_empty());
    }

    #[test]
    fn meeting_chat_message_mention_is_a_bot_mention_and_scopes_to_chat() {
        let a: Activity = serde_json::from_value(serde_json::json!({
            "type":"message",
            "recipient":{"id":"28:bot"},
            "from":{"aadObjectId":"11111111-1111-1111-1111-111111111111"},
            "conversation":{"id":"19:meeting_x@thread.v2","conversationType":"groupChat","tenantId":"tenant-abc"},
            "channelData":{"meeting":{"id":"mtg"},"tenant":{"id":"tenant-abc"}},
            "entities":[{"type":"mention","mentioned":{"id":"28:bot"}}]
        })).unwrap();
        assert!(a.channel_data.as_ref().unwrap().meeting.is_some());
        assert!(is_bot_mention(&a, "message", Some("28:bot")));
        assert!(!is_bot_mention(&a, "message", Some("28:other")));
        assert!(!is_bot_mention(&a, "message", None));
        assert!(!is_bot_mention(&a, "conversationUpdate", Some("28:bot")));
        let scope = extract_scope(&a).unwrap();
        assert_eq!(scope.kind, "chat");
        assert_eq!(scope.chat_id, "19:meeting_x@thread.v2");
        // The routing decision process_activity uses: a meeting-scoped mention
        // continues to chat handling; plain messages and installs do not.
        assert!(meeting_activity_continues_to_chat(&a));
        let plain: Activity = serde_json::from_value(serde_json::json!({
            "type":"message","recipient":{"id":"28:bot"},
            "conversation":{"id":"19:meeting_x@thread.v2","conversationType":"groupChat","tenantId":"tenant-abc"},
            "channelData":{"meeting":{"id":"mtg"}}
        })).unwrap();
        assert!(!meeting_activity_continues_to_chat(&plain));
        let install: Activity = serde_json::from_value(serde_json::json!({
            "type":"installationUpdate","action":"add","recipient":{"id":"28:bot"},
            "conversation":{"id":"19:meeting_x@thread.v2","conversationType":"groupChat","tenantId":"tenant-abc"},
            "channelData":{"meeting":{"id":"mtg"}}
        })).unwrap();
        assert!(!meeting_activity_continues_to_chat(&install));
    }

    #[test]
    fn group_chat_scope_uses_conversation_id() {
        let a: Activity = serde_json::from_value(serde_json::json!({
            "type": "conversationUpdate",
            "conversation": {"id": "19:grp@thread.v2", "conversationType": "groupChat", "tenantId": "tenant-abc"}
        }))
        .unwrap();
        let s = extract_scope(&a).expect("chat scope");
        assert_eq!(s.kind, "chat");
        assert_eq!(s.chat_id, "19:grp@thread.v2");
        assert_eq!(s.conversation_id, "19:grp@thread.v2");
        assert!(s.team_id.is_empty());
    }

    #[test]
    fn personal_and_tenantless_activities_have_no_scope() {
        let personal: Activity = serde_json::from_value(serde_json::json!({
            "type": "conversationUpdate",
            "conversation": {"id": "p1", "conversationType": "personal", "tenantId": "t"}
        }))
        .unwrap();
        assert!(extract_scope(&personal).is_none());

        let no_tenant: Activity = serde_json::from_value(serde_json::json!({
            "type": "conversationUpdate",
            "conversation": {"id": "19:grp@thread.v2", "conversationType": "groupChat"}
        }))
        .unwrap();
        assert!(extract_scope(&no_tenant).is_none());
    }
}
