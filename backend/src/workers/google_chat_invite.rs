//! Chat invite job (org-onboarding plan item 22).
//!
//! Enqueued when a member connects Google Chat. The worker walks the spaces
//! that member's own token can see and emails three audiences, and only
//! after the grant includes `directory.readonly` and the People API returns
//! a real directory:
//!
//! * nobody on CoralSwarm → create-your-account email (`google_chat`)
//! * a CoralSwarm user who is not an org member → join-this-org email
//!   (`google_chat_join`). The token's membership class is
//!   [`crate::connectors::slack::MembershipClass::Unavailable`].
//! * an org member who has not connected Chat → the connect nudge, claiming
//!   `member_connect` so the hourly nudge and this job share one window
//!
//! The connected-member token is loaded the same way capture loads it. This
//! worker does not elect a space reader and does not rewrite that path.
//! A personal account fetches the directory regardless of the space's graph
//! kind, and only when the invite does not already have addresses (item 41).

use std::collections::HashSet;
use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google_chat_invite::{
    chat_connect_notice_provider, chat_invite_directory_grant_is_live, chat_join_membership_class,
    classify_chat_invite_audience, personal_invite_fetches_directory, ChatInviteAudience,
    ChatInviteIdentity, CHAT_JOIN_PROVIDER, CHAT_UNMATCHED_PROVIDER,
};
use crate::connectors::{google_chat, GoogleConnector, TokenOwner};
use crate::jobs::{queue, JobKind};
use crate::routes::connectors::load_creds_bg;
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

const JOB_BATCH_SIZE: usize = 5;
const MAX_SPACES: usize = 25;
const MAX_NOTICES: usize = 200;
pub const EMPTY_POLL_SLEEP: std::time::Duration = std::time::Duration::from_secs(30);
const TENANT_ENQUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Queue the invite job on the plane whose worker will dequeue it.
/// Called from the Chat OAuth callback after the token row has committed.
pub async fn enqueue_on_connect(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    clerk_org_id: Option<&str>,
) {
    let Some(pool) = jobs_pool(state, control_pool, clerk_org_id).await else {
        return;
    };
    let kind = JobKind::ChatInvite {
        account_id,
        owner_user_id,
    };
    let key = format!("chat-invite:{account_id}:{owner_user_id}");
    if let Err(e) = queue::enqueue(&pool, &kind, Some(account_id), Some(&key), 0).await {
        tracing::warn!(
            error = %e, %account_id, %owner_user_id,
            "chat-invite: failed to enqueue"
        );
    }
}

async fn jobs_pool(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    clerk_org_id: Option<&str>,
) -> Option<PgPool> {
    let Some(org_id) = clerk_org_id else {
        return Some(control_pool.clone());
    };
    let revoked = match tokio::time::timeout(
        TENANT_ENQUEUE_TIMEOUT,
        crate::tenancy::routing::org_is_revoked(control_pool, org_id),
    )
    .await
    {
        Ok(Ok(revoked)) => revoked,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, org = %org_id, "chat-invite: org revocation lookup failed — not enqueuing");
            return None;
        }
        Err(_) => {
            tracing::warn!(org = %org_id, "chat-invite: org revocation lookup timed out — not enqueuing");
            return None;
        }
    };
    if revoked {
        return None;
    }
    let row = match tokio::time::timeout(
        TENANT_ENQUEUE_TIMEOUT,
        crate::tenancy::routing::lookup_by_org(control_pool, org_id),
    )
    .await
    {
        Ok(Ok(row)) => row,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, org = %org_id, "chat-invite: tenant routing lookup failed — not enqueuing");
            return None;
        }
        Err(_) => {
            tracing::warn!(org = %org_id, "chat-invite: tenant routing lookup timed out — not enqueuing");
            return None;
        }
    };
    let Some(row) = row else {
        return Some(control_pool.clone());
    };
    if !row.is_servable() {
        tracing::warn!(org = %org_id, status = %row.status, "chat-invite: tenant is not servable — not enqueuing");
        return None;
    }
    let Some(runtime) = state.tenant_runtime.as_ref() else {
        tracing::warn!(org = %org_id, "chat-invite: no tenant runtime — not enqueuing");
        return None;
    };
    match tokio::time::timeout(
        TENANT_ENQUEUE_TIMEOUT,
        runtime
            .registry
            .get_or_build(org_id, row.account_id, &row.db_secret_ref),
    )
    .await
    {
        Ok(Ok(pool)) => Some(pool),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, org = %org_id, "chat-invite: tenant pool build failed — not enqueuing");
            None
        }
        Err(_) => {
            tracing::warn!(org = %org_id, "chat-invite: tenant pool build timed out — not enqueuing");
            None
        }
    }
}

pub async fn tick(
    t: &WorkerTenant,
    state: &Arc<AppState>,
    control_pool: &PgPool,
    http: &reqwest::Client,
) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["ChatInvite"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;
    if jobs.is_empty() {
        return Ok(Tick::idle());
    }
    let n = jobs.len();
    for job in jobs {
        let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
        let (account_id, owner_user_id) = match serde_json::from_value::<JobKind>(tagged) {
            Ok(JobKind::ChatInvite {
                account_id,
                owner_user_id,
            }) => (account_id, owner_user_id),
            Ok(_) => {
                queue::fail(&t.pool, job.id, "wrong job kind").await.ok();
                continue;
            }
            Err(e) => {
                queue::fail(&t.pool, job.id, &format!("malformed payload: {e}"))
                    .await
                    .ok();
                continue;
            }
        };
        match process_job(state, control_pool, http, account_id, owner_user_id).await {
            Ok(sent) => {
                let outcome = if sent == 0 {
                    queue::JobOutcome::NothingToDo
                } else {
                    queue::JobOutcome::DidWork
                };
                if let Err(e) = queue::complete(&t.pool, job.id, outcome).await {
                    tracing::error!(error = %e, job_id = %job.id, "chat-invite: complete failed");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, job_id = %job.id, %account_id, "chat-invite: job failed");
                queue::fail(&t.pool, job.id, &e).await.ok();
            }
        }
    }
    Ok(Tick::worked(n))
}

async fn process_job(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<usize, String> {
    let scope = stored_chat_scope(control_pool, account_id, owner_user_id)
        .await
        .map_err(|_| "google_chat scope could not be read".to_string())?;
    let Some(scope) = scope else {
        tracing::info!(%account_id, %owner_user_id, "chat-invite: no live google_chat grant — skipping");
        return Ok(0);
    };
    if !chat_invite_directory_grant_is_live(&scope) {
        tracing::info!(
            %account_id, %owner_user_id,
            "chat-invite: directory.readonly is not on the grant — the invite stays gated"
        );
        return Ok(0);
    }

    let clerk_org_id = resolve_clerk_org_id(control_pool, account_id).await;
    let account_has_org = clerk_org_id.is_some();
    // Item 41. The Chat roster has no addresses of its own, so a personal
    // invite's known-address count is zero and the directory is fetched
    // regardless of graph kind. A personal invite that already had addresses
    // would not fetch. This does not lift seating or identity resolution.
    if !account_has_org && !personal_invite_fetches_directory("unspecified", 0) {
        return Ok(0);
    }

    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, control_pool, account_id)
        .await
        .map_err(|e| format!("resolve DEK plane: {e}"))?;
    let Some(mut creds) = load_creds_bg(
        control_pool,
        &dek_plane,
        account_id,
        "google_chat",
        TokenOwner::Member(owner_user_id),
    )
    .await
    .map_err(|e| format!("load google_chat credential: {e}"))?
    else {
        tracing::info!(%account_id, %owner_user_id, "chat-invite: member token is gone — skipping");
        return Ok(0);
    };
    let google = GoogleConnector;
    google
        .refresh_if_needed(http, &mut creds)
        .await
        .map_err(|e| format!("refresh google_chat token: {e}"))?;
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = crate::routes::oauth::store_token_bundle(
            control_pool,
            &dek_plane,
            account_id,
            "google_chat",
            TokenOwner::Member(owner_user_id),
            refreshed,
        )
        .await
        {
            let _ = e;
            tracing::warn!(%account_id, %owner_user_id, "chat-invite: failed to persist a refreshed token");
        }
    }

    let directory = match google_chat::fetch_domain_directory(http, &creds.access_token).await {
        Ok(directory) => directory,
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id, %owner_user_id,
                "chat-invite: directory is not live — inviting nobody"
            );
            return Ok(0);
        }
    };

    let spaces = google_chat::list_spaces(http, &creds.access_token)
        .await
        .map_err(|e| format!("spaces.list: {e}"))?;
    let anchor =
        google_chat::chat_invite_anchor_domain(control_pool, account_id, owner_user_id).await;
    let workspace_id = if creds.provider_workspace_id.is_empty() {
        "google_chat".to_string()
    } else {
        creds.provider_workspace_id.clone()
    };
    let web_base = resolve_web_base(state, control_pool, clerk_org_id.as_deref()).await;
    let join_secret = crate::services::unmatched_recipient_email::join_notice_secret();

    let mut seen_emails = HashSet::new();
    let mut sent = 0usize;
    let mut notices = 0usize;
    let mut spaces_seen = 0usize;
    for space in &spaces {
        if space.single_user_bot_dm {
            continue;
        }
        if spaces_seen >= MAX_SPACES || notices >= MAX_NOTICES {
            tracing::warn!(
                %account_id, spaces = spaces.len(), spaces_seen, notices,
                "chat-invite: hit the per-job cap — the remainder waits for a later walk"
            );
            break;
        }
        spaces_seen += 1;
        let roster = match google_chat::resolve_space_roster(http, &creds.access_token, &space.name)
            .await
        {
            Ok(roster) => roster,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, space = %space.name, "chat-invite: roster read failed — skipping this space");
                continue;
            }
        };
        for member in &roster.members {
            if notices >= MAX_NOTICES {
                break;
            }
            let directory_email = directory.get(&member.sub).map(|entry| entry.email.as_str());
            let audience = classify_chat_invite_audience(
                &member.user_type,
                &member.affiliation,
                &member.resource_name,
                Ok(directory_email.filter(|email| !email.trim().is_empty())),
                &anchor,
                true,
                account_has_org,
                ChatInviteIdentity {
                    user_id: None,
                    is_org_member: false,
                    chat_connected: false,
                },
            );
            // Classification above uses an empty identity so guests and
            // domain misses drop out before a users lookup. Re-classify
            // once we know who the address belongs to.
            let Some(ChatInviteAudience::Unmatched { email }) = audience else {
                continue;
            };
            if !seen_emails.insert(email.clone()) {
                continue;
            }
            let user_id = user_id_for_email(control_pool, &email).await;
            let (is_org_member, chat_connected) = match user_id {
                Some(user_id) => (
                    is_account_member(control_pool, account_id, user_id).await,
                    member_has_live_chat(control_pool, account_id, user_id).await,
                ),
                None => (false, false),
            };
            let audience = classify_chat_invite_audience(
                &member.user_type,
                &member.affiliation,
                &member.resource_name,
                Ok(Some(email.as_str())),
                &anchor,
                true,
                account_has_org,
                ChatInviteIdentity {
                    user_id,
                    is_org_member,
                    chat_connected,
                },
            );
            let Some(audience) = audience else {
                continue;
            };
            notices += 1;
            if notices > MAX_NOTICES {
                break;
            }
            if send_audience(
                state,
                control_pool,
                account_id,
                clerk_org_id.as_deref(),
                &workspace_id,
                &space.name,
                &web_base,
                join_secret.as_deref(),
                &audience,
            )
            .await
            {
                sent += 1;
            }
        }
    }
    tracing::info!(%account_id, %owner_user_id, sent, notices, "chat-invite: pass finished");
    Ok(sent)
}

#[allow(clippy::too_many_arguments)]
async fn send_audience(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    clerk_org_id: Option<&str>,
    workspace_id: &str,
    space_id: &str,
    web_base: &str,
    join_secret: Option<&str>,
    audience: &ChatInviteAudience,
) -> bool {
    let email = match audience {
        ChatInviteAudience::Unmatched { email }
        | ChatInviteAudience::JoinThisOrg { email, .. }
        | ChatInviteAudience::ConnectNudge { email, .. } => email.as_str(),
    };
    let user_id = match audience {
        ChatInviteAudience::Unmatched { .. } => None,
        ChatInviteAudience::JoinThisOrg { user_id, .. }
        | ChatInviteAudience::ConnectNudge { user_id, .. } => Some(*user_id),
    };
    match crate::clerk_provision::invite_suppressed_by_tombstone(
        control_pool,
        clerk_org_id,
        user_id,
        Some(email),
    )
    .await
    {
        Ok(true) => return false,
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat-invite: tombstone check failed — continuing");
        }
    }
    match crate::services::unmatched_recipient_email::is_suppressed_any_provider(
        control_pool,
        email,
    )
    .await
    {
        Ok(true) => return false,
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat-invite: suppression check failed — continuing to the claim");
        }
    }

    match audience {
        ChatInviteAudience::Unmatched { email } => {
            claim_unmatched(
                state,
                control_pool,
                account_id,
                workspace_id,
                space_id,
                web_base,
                email,
            )
            .await
        }
        ChatInviteAudience::JoinThisOrg { user_id, email } => {
            let Some(clerk_org_id) = clerk_org_id else {
                return false;
            };
            let Some(join_secret) = join_secret else {
                tracing::warn!(%account_id, "chat-invite: JOIN_NOTICE_SECRET is unset — skipping the join email");
                return false;
            };
            claim_join(
                state,
                control_pool,
                account_id,
                user_id,
                clerk_org_id,
                workspace_id,
                space_id,
                join_secret,
                email,
            )
            .await
        }
        ChatInviteAudience::ConnectNudge { email, .. } => {
            debug_assert_eq!(chat_connect_notice_provider(), "member_connect");
            claim_connect(state, control_pool, account_id, web_base, email).await
        }
    }
}

async fn claim_unmatched(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    space_id: &str,
    web_base: &str,
    email: &str,
) -> bool {
    let mut tx = match begin_tenant_system_scoped(control_pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat-invite: unmatched claim transaction failed to open");
            return false;
        }
    };
    let claimed = match crate::services::unmatched_recipient_email::claim_notice_slot(
        &mut *tx,
        account_id,
        CHAT_UNMATCHED_PROVIDER,
        workspace_id,
        space_id,
        email,
    )
    .await
    {
        Ok(claimed) => claimed,
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %account_id, "chat-invite: unmatched claim failed");
            return false;
        }
    };
    let Some(tokens) = claimed else {
        tx.rollback().await.ok();
        return false;
    };
    let sent = crate::services::unmatched_recipient_email::send_nudge_email(
        state,
        email,
        web_base,
        &tokens.unsubscribe_token,
        &tokens.prefill_token,
    )
    .await;
    finish_claim(tx, sent, account_id).await
}

#[allow(clippy::too_many_arguments)]
async fn claim_join(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    user_id: &Uuid,
    clerk_org_id: &str,
    workspace_id: &str,
    space_id: &str,
    join_secret: &str,
    email: &str,
) -> bool {
    if join_request_already_open_or_rejected(control_pool, account_id, user_id).await {
        return false;
    }
    let mut tx = match begin_tenant_system_scoped(control_pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat-invite: join claim transaction failed to open");
            return false;
        }
    };
    let claimed = match crate::services::unmatched_recipient_email::claim_join_notice_slot(
        &mut *tx,
        account_id,
        CHAT_JOIN_PROVIDER,
        workspace_id,
        space_id,
        email,
    )
    .await
    {
        Ok(claimed) => claimed,
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %account_id, "chat-invite: join claim failed");
            return false;
        }
    };
    let Some(tokens) = claimed else {
        tx.rollback().await.ok();
        return false;
    };
    let join_token = match crate::services::unmatched_recipient_email::mint_join_notice_token(
        join_secret,
        account_id,
        *user_id,
        clerk_org_id,
        email,
        Some(workspace_id),
        Some(space_id),
        None,
        chat_join_membership_class(),
    ) {
        Ok(token) => token,
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %account_id, "chat-invite: failed to mint the join token");
            return false;
        }
    };
    let join_web_base = state.personal_web_base_url();
    let join_url = format!(
        "{join_web_base}/join/{}?t={}",
        urlencoding::encode(clerk_org_id),
        urlencoding::encode(&join_token)
    );
    let sent = crate::services::unmatched_recipient_email::send_join_org_email(
        state,
        email,
        "your team",
        &join_url,
        &join_web_base,
        &tokens.unsubscribe_token,
    )
    .await;
    finish_claim(tx, sent, account_id).await
}

async fn claim_connect(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    web_base: &str,
    email: &str,
) -> bool {
    let mut tx = match begin_tenant_system_scoped(control_pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat-invite: connect claim transaction failed to open");
            return false;
        }
    };
    let claimed =
        match crate::services::unmatched_recipient_email::claim_member_connect_notice_slot(
            &mut *tx, account_id, email,
        )
        .await
        {
            Ok(claimed) => claimed,
            Err(e) => {
                tx.rollback().await.ok();
                tracing::warn!(error = %e, %account_id, "chat-invite: connect claim failed");
                return false;
            }
        };
    let Some(tokens) = claimed else {
        tx.rollback().await.ok();
        return false;
    };
    let sent = crate::services::unmatched_recipient_email::send_connect_nudge_email(
        state,
        email,
        web_base,
        &tokens.unsubscribe_token,
    )
    .await;
    finish_claim(tx, sent, account_id).await
}

async fn finish_claim(
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
    sent: bool,
    account_id: Uuid,
) -> bool {
    if sent {
        if let Err(e) = tx.commit().await {
            tracing::warn!(error = %e, %account_id, "chat-invite: sent the email but failed to commit the cooldown");
        }
        true
    } else {
        tx.rollback().await.ok();
        false
    }
}

async fn join_request_already_open_or_rejected(
    control_pool: &PgPool,
    account_id: Uuid,
    user_id: &Uuid,
) -> bool {
    let mut tx = match begin_tenant_system_scoped(control_pool, account_id).await {
        Ok(tx) => tx,
        Err(_) => return false,
    };
    let existing: Result<bool, sqlx::Error> = sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM org_join_requests \
              WHERE account_id = $1 AND user_id = $2 \
                AND (decision IS NULL OR decision = 'rejected') \
         )",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await;
    tx.rollback().await.ok();
    existing.unwrap_or(false)
}

async fn stored_chat_scope(
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Option<String>, ()> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|_| ())?;
    let scope: Option<String> = sqlx::query_scalar(
        "SELECT scope FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'google_chat' AND owner_user_id = $2 \
           AND credential_invalid_at IS NULL \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|_| ())?;
    tx.rollback().await.ok();
    Ok(scope)
}

async fn resolve_clerk_org_id(control_pool: &PgPool, account_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(control_pool)
        .await
        .ok()
        .flatten()
}

async fn resolve_web_base(
    state: &AppState,
    control_pool: &PgPool,
    clerk_org_id: Option<&str>,
) -> String {
    match clerk_org_id {
        Some(org_id) => state.org_web_base_url(control_pool, org_id).await,
        None => state.personal_web_base_url(),
    }
}

async fn user_id_for_email(control_pool: &PgPool, email: &str) -> Option<Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM users WHERE lower(email) = $1 AND deleted_at IS NULL LIMIT 1",
    )
    .bind(email)
    .fetch_optional(control_pool)
    .await
    .ok()
    .flatten()
}

async fn is_account_member(control_pool: &PgPool, account_id: Uuid, user_id: Uuid) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(control_pool)
    .await
    .unwrap_or(false)
}

async fn member_has_live_chat(control_pool: &PgPool, account_id: Uuid, user_id: Uuid) -> bool {
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control_pool, account_id).await {
        Ok(tx) => tx,
        Err(_) => return false,
    };
    let connected: Result<bool, sqlx::Error> = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'google_chat' \
               AND owner_user_id = $2 AND credential_invalid_at IS NULL \
         )",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await;
    tx.rollback().await.ok();
    connected.unwrap_or(false)
}
