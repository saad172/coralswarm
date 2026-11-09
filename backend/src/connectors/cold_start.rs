//! In-container connect notice (org-onboarding plan item 25).
//!
//! Email is the normal invite. This post is the fallback for a container
//! where nobody can be reached by email. It is content-free, once a day,
//! and at most five times in a rolling 30 days. The first member who can
//! be emailed retires it.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::AppState;

const DAY: Duration = Duration::hours(24);
const WINDOW: Duration = Duration::days(30);
const MAX_POSTS: i32 = 5;

/// Whether a cold-start post is allowed at `now`.
pub(crate) fn cold_start_may_post(
    someone_can_be_emailed: bool,
    last_posted_at: Option<DateTime<Utc>>,
    posts_in_window: i32,
    window_started_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    if someone_can_be_emailed {
        return false;
    }
    if last_posted_at.is_some_and(|posted| now < posted + DAY) {
        return false;
    }
    let in_window = window_started_at.is_some_and(|started| now < started + WINDOW);
    if in_window && posts_in_window >= MAX_POSTS {
        return false;
    }
    true
}

/// Scopes of `oauth_tokens` rows for `account_id`, read inside
/// `begin_account_scoped`. `oauth_tokens` is RLS-covered (0083), so an
/// unscoped read on the control pool hard-errors as `coralswarm_app`.
async fn scoped_token_scopes(
    control: &PgPool,
    account_id: Uuid,
    sql: &'static str,
) -> Result<Vec<String>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id).await?;
    let scopes: Vec<String> = sqlx::query_scalar(sql)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
    tx.rollback().await.ok();
    Ok(scopes)
}

async fn slack_someone_can_be_emailed(control: &PgPool, account_id: Uuid) -> bool {
    // `slack_user` rows are always member-owned. A token whose owner has left
    // the account must not keep the notice retired.
    let scopes = scoped_token_scopes(
        control,
        account_id,
        "SELECT ot.scope FROM oauth_tokens ot \
         JOIN account_members am \
           ON am.account_id = ot.account_id AND am.user_id = ot.owner_user_id \
         WHERE ot.account_id = $1 AND ot.provider = 'slack_user' \
           AND ot.credential_invalid_at IS NULL",
    )
    .await;
    match scopes {
        Ok(scopes) => scopes
            .iter()
            .any(|scope| crate::connectors::slack::scope_grants_users_read_email(scope)),
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "cold start: slack_user scope lookup failed");
            true
        }
    }
}

async fn teams_someone_can_be_emailed(control: &PgPool, account_id: Uuid) -> bool {
    // Org grants are member-owned. A personal account's grant is account-wide
    // (`owner_user_id` NULL) and still counts. A departed member's row does not.
    let scopes = scoped_token_scopes(
        control,
        account_id,
        "SELECT ot.scope FROM oauth_tokens ot \
         WHERE ot.account_id = $1 AND ot.provider = 'microsoft' \
           AND ot.credential_invalid_at IS NULL \
           AND (ot.owner_user_id IS NULL OR EXISTS ( \
             SELECT 1 FROM account_members am \
             WHERE am.account_id = ot.account_id AND am.user_id = ot.owner_user_id \
           ))",
    )
    .await;
    match scopes {
        Ok(scopes) => scopes
            .iter()
            .any(|scope| crate::connectors::microsoft::scope_grants_team_member_read_all(scope)),
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "cold start: microsoft scope lookup failed");
            true
        }
    }
}

/// Test-only seam for `backend/tests/unscoped_oauth_reads_rls_pg.rs`.
#[cfg(feature = "test-support")]
pub async fn test_support_slack_someone_can_be_emailed(control: &PgPool, account_id: Uuid) -> bool {
    slack_someone_can_be_emailed(control, account_id).await
}

/// Test-only seam for `backend/tests/unscoped_oauth_reads_rls_pg.rs`.
#[cfg(feature = "test-support")]
pub async fn test_support_teams_someone_can_be_emailed(control: &PgPool, account_id: Uuid) -> bool {
    teams_someone_can_be_emailed(control, account_id).await
}

/// Claim today's slot for `(provider, account, container)`. True only after
/// the claim commits. A failed post still burns the slot.
async fn claim_cold_start_slot(
    control: &PgPool,
    account_id: Uuid,
    provider: &str,
    container_id: &str,
) -> bool {
    let mut tx = match crate::tenancy::scope::begin_tenant_system_scoped(control, account_id).await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "cold start: claim transaction failed");
            return false;
        }
    };
    let row: Option<(Option<DateTime<Utc>>, i32, Option<DateTime<Utc>>)> = match sqlx::query_as(
        "SELECT last_posted_at, posts_in_window, window_started_at \
         FROM cold_start_notices \
         WHERE provider = $1 AND account_id = $2 AND container_id = $3 \
         FOR UPDATE",
    )
    .bind(provider)
    .bind(account_id)
    .bind(container_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %account_id, "cold start: claim read failed");
            return false;
        }
    };
    let now = Utc::now();
    let (last_posted_at, posts_in_window, window_started_at) = row.unwrap_or((None, 0, None));
    if !cold_start_may_post(
        false,
        last_posted_at,
        posts_in_window,
        window_started_at,
        now,
    ) {
        tx.rollback().await.ok();
        return false;
    }
    let in_window = window_started_at.is_some_and(|started| now < started + WINDOW);
    let posts = if in_window { posts_in_window + 1 } else { 1 };
    let window_start = if in_window {
        window_started_at.unwrap_or(now)
    } else {
        now
    };
    let claim = match sqlx::query(
        "INSERT INTO cold_start_notices \
            (provider, account_id, container_id, last_posted_at, posts_in_window, window_started_at) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (provider, account_id, container_id) DO UPDATE \
            SET last_posted_at = EXCLUDED.last_posted_at, \
                posts_in_window = EXCLUDED.posts_in_window, \
                window_started_at = EXCLUDED.window_started_at \
         WHERE (cold_start_notices.last_posted_at IS NULL \
                OR EXCLUDED.last_posted_at >= cold_start_notices.last_posted_at + INTERVAL '24 hours') \
           AND (cold_start_notices.window_started_at IS NULL \
                OR EXCLUDED.last_posted_at >= cold_start_notices.window_started_at + INTERVAL '30 days' \
                OR cold_start_notices.posts_in_window < 5)",
    )
    .bind(provider)
    .bind(account_id)
    .bind(container_id)
    .bind(now)
    .bind(posts)
    .bind(window_start)
    .execute(&mut *tx)
    .await
    {
        Ok(result) => result,
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %account_id, "cold start: claim write failed");
            return false;
        }
    };
    if claim.rows_affected() == 0 {
        tx.rollback().await.ok();
        return false;
    }
    match tx.commit().await {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "cold start: claim commit failed");
            false
        }
    }
}

/// Claim today's slot and post a content-free connect notice into the Slack
/// channel the bot was just added to. A member who can already be emailed
/// skips the post.
pub(crate) async fn maybe_post_slack_cold_start(
    state: &AppState,
    control: &PgPool,
    account_id: Uuid,
    clerk_org_id: Option<&str>,
    bot_token: &str,
    channel_id: &str,
) {
    if slack_someone_can_be_emailed(control, account_id).await {
        return;
    }
    if !claim_cold_start_slot(control, account_id, "slack", channel_id).await {
        return;
    }
    let text = connect_notice_text(state, control, clerk_org_id).await;
    let body = serde_json::json!({ "channel": channel_id, "text": text });
    let send = state
        .http_client
        .post(format!(
            "{}/chat.postMessage",
            crate::connectors::slack::slack_api_base()
        ))
        .timeout(std::time::Duration::from_secs(5))
        .bearer_auth(bot_token)
        .json(&body)
        .send()
        .await;
    match send {
        Ok(resp) => {
            if !resp.status().is_success() {
                tracing::info!(status = %resp.status(), %channel_id, "cold start: Slack post was not accepted");
            } else if let Ok(json) = resp.json::<serde_json::Value>().await {
                if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
                    tracing::info!(
                        error = json.get("error").and_then(serde_json::Value::as_str),
                        %channel_id,
                        "cold start: Slack post was not accepted"
                    );
                }
            }
        }
        Err(e) => tracing::info!(error = %e, %channel_id, "cold start: Slack post failed"),
    }
}

async fn connect_notice_text(
    state: &AppState,
    control: &PgPool,
    clerk_org_id: Option<&str>,
) -> String {
    let web_base = match clerk_org_id {
        Some(org_id) => state.org_web_base_url(control, org_id).await,
        None => state.personal_web_base_url(),
    };
    format!(
        "Connect your own CoralSwarm account to capture the channels you're in: {web_base}/connectors"
    )
}

/// Claim today's slot and post the same content-free connect notice into the
/// Teams conversation the bot was just added to. A live member grant that can
/// read the roster skips the post. Callers post only on the first insert.
pub(crate) async fn maybe_post_teams_cold_start(
    state: &AppState,
    control: &PgPool,
    account_id: Uuid,
    tenant_id: &str,
    service_url: &str,
    conversation_id: &str,
) {
    if teams_someone_can_be_emailed(control, account_id).await {
        return;
    }
    let clerk_org_id: Option<String> =
        match sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(control)
            .await
        {
            Ok(Some(id)) => id,
            Ok(None) => {
                tracing::warn!(%account_id, "cold start: account missing; skipping Teams post");
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "cold start: clerk_org_id lookup failed");
                return;
            }
        };
    if !claim_cold_start_slot(control, account_id, "microsoft", conversation_id).await {
        return;
    }
    let text = connect_notice_text(state, control, clerk_org_id.as_deref()).await;
    let bot_token = match crate::connectors::microsoft::bot_connector_token(
        &state.http_client,
        tenant_id,
    )
    .await
    {
        Ok(token) => token,
        Err(e) => {
            tracing::info!(error = %e, %conversation_id, "cold start: Teams connector token failed");
            return;
        }
    };
    let activity = serde_json::json!({ "type": "message", "text": text });
    if let Err(e) = crate::connectors::microsoft::send_activity(
        &state.http_client,
        &bot_token,
        service_url,
        conversation_id,
        &activity,
    )
    .await
    {
        tracing::info!(error = %e, %conversation_id, "cold start: Teams post failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-25T15:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn a_connected_member_retires_the_cold_start() {
        assert!(!cold_start_may_post(true, None, 0, None, t0()));
    }

    #[test]
    fn the_first_post_is_allowed_and_the_same_day_is_not() {
        let now = t0();
        assert!(cold_start_may_post(false, None, 0, None, now));
        assert!(!cold_start_may_post(
            false,
            Some(now),
            1,
            Some(now),
            now + Duration::hours(23)
        ));
        assert!(cold_start_may_post(
            false,
            Some(now),
            1,
            Some(now),
            now + Duration::hours(24)
        ));
    }

    #[test]
    fn the_fifth_post_in_thirty_days_is_the_last() {
        let now = t0();
        assert!(!cold_start_may_post(
            false,
            Some(now - DAY),
            5,
            Some(now - Duration::days(10)),
            now
        ));
        assert!(cold_start_may_post(
            false,
            Some(now - DAY),
            5,
            Some(now - Duration::days(30)),
            now
        ));
    }

    #[test]
    fn team_member_read_all_is_an_exact_token() {
        let grants = crate::connectors::microsoft::scope_grants_team_member_read_all;
        assert!(grants("TeamMember.Read.All"));
        assert!(grants(
            "https://graph.microsoft.com/User.Read https://graph.microsoft.com/TeamMember.Read.All"
        ));
        assert!(!grants("TeamMember.Read.All.Extra"));
        assert!(!grants("https://evil.example/TeamMember.Read.All"));
        assert!(!grants("ChannelMessage.Read.All"));
    }
}
