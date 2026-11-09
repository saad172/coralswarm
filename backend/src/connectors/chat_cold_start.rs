//! In-space connect notice for Google Chat (org-onboarding plan item 25).
//!
//! Email is the normal invite. This post is the fallback for a space where
//! the app is present because a member added it, and nobody in the account
//! can be reached by email. It is content-free, at most once a day, and at
//! most five times in a rolling 30 days. A live `google_chat` grant that
//! includes `directory.readonly` retires it: that is the first connect that
//! can actually email. A content-free outbound notice is not an answer
//! (I11, item 28).

use chrono::{DateTime, Datelike, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::AppState;

use super::chat_app_identity::{self, AppPost};

const DAY: Duration = Duration::hours(24);
const WINDOW: Duration = Duration::days(30);
const MAX_POSTS: i32 = 5;
const PROVIDER: &str = "google_chat";

pub(crate) struct ColdStartGate {
    pub(crate) app_present: bool,
    pub(crate) someone_can_be_emailed: bool,
    pub(crate) last_posted_at: Option<DateTime<Utc>>,
    pub(crate) posts_in_window: i32,
    pub(crate) window_started_at: Option<DateTime<Utc>>,
    pub(crate) now: DateTime<Utc>,
}

/// Whether a cold-start post is allowed at `now`.
pub(crate) fn cold_start_may_post(gate: &ColdStartGate) -> bool {
    if !gate.app_present || gate.someone_can_be_emailed {
        return false;
    }
    if gate
        .last_posted_at
        .is_some_and(|posted| gate.now < posted + DAY)
    {
        return false;
    }
    let in_window = gate
        .window_started_at
        .is_some_and(|started| gate.now < started + WINDOW);
    if in_window && gate.posts_in_window >= MAX_POSTS {
        return false;
    }
    true
}

/// SPACE (and the legacy ROOM label) is the in-container channel. A direct
/// message is the admin-install path this notice does not open.
pub(crate) fn space_is_notice_target(space_type: &str) -> bool {
    matches!(space_type, "" | "SPACE" | "ROOM")
}

/// A content-free outbound notice is not an answer (I11, item 28). The
/// sentence is the same for every recipient and carries no captured text.
pub(crate) fn connect_notice_text(web_base: &str) -> String {
    let base = web_base.trim_end_matches('/');
    format!("Connect your own CoralSwarm account to capture this space: {base}/connectors")
}

/// Member added the app. Post when this account still cannot email anyone.
pub(crate) async fn on_added_to_space(
    state: &AppState,
    control: &PgPool,
    account_id: Uuid,
    space_name: &str,
    space_type: &str,
) {
    if !space_is_notice_target(space_type) || !chat_app_identity::valid_space_name(space_name) {
        return;
    }
    post_if_due(state, control, account_id, space_name, true).await;
}

/// The app left the space. Stop the series even when no watched-space row
/// exists yet (the cold path defers that row until a Chat token exists).
pub(crate) async fn retire_container(
    control: &PgPool,
    space_name: &str,
) -> Result<(), sqlx::Error> {
    if !chat_app_identity::valid_space_name(space_name) {
        return Ok(());
    }
    sqlx::query("SELECT retire_chat_cold_start_container($1)")
        .bind(space_name)
        .execute(control)
        .await?;
    Ok(())
}

pub(crate) async fn due_account_ids(control: &PgPool) -> Result<Vec<Uuid>, String> {
    sqlx::query_scalar("SELECT account_id FROM chat_cold_start_due_accounts()")
        .fetch_all(control)
        .await
        .map_err(|e| format!("chat cold start: due accounts failed: {e}"))
}

pub(crate) async fn repeat_due(state: &AppState, control: &PgPool, account_id: Uuid) -> usize {
    let spaces = match due_spaces(control, account_id).await {
        Ok(spaces) => spaces,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat cold start: due spaces failed");
            return 0;
        }
    };
    let mut posted = 0usize;
    for space in spaces.into_iter().take(20) {
        if post_if_due(state, control, account_id, &space, true).await {
            posted += 1;
        }
    }
    posted
}

async fn due_spaces(control: &PgPool, account_id: Uuid) -> Result<Vec<String>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("chat cold start scope: {e}")))?;
    let spaces = sqlx::query_scalar(
        "SELECT container_id FROM chat_cold_start_notices \
         WHERE provider = $1 AND account_id = $2 \
           AND last_posted_at IS NOT NULL \
           AND last_posted_at <= now() - interval '24 hours'",
    )
    .bind(PROVIDER)
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(spaces)
}

/// Returns true only when a notice was accepted.
async fn post_if_due(
    state: &AppState,
    control: &PgPool,
    account_id: Uuid,
    space_name: &str,
    app_present: bool,
) -> bool {
    if !app_present || !chat_app_identity::chat_bot_configured() {
        return false;
    }
    let text = connect_notice_text(&notice_web_base(state, control, account_id).await);
    let mut tx = match crate::tenancy::scope::begin_tenant_system_scoped(control, account_id).await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat cold start: claim transaction failed");
            return false;
        }
    };
    let can_email = match someone_can_be_emailed(&mut tx, account_id).await {
        Ok(can) => can,
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %account_id, "chat cold start: directory grant lookup failed");
            return false;
        }
    };
    if can_email {
        if let Err(e) = delete_notice(&mut tx, account_id, space_name).await {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %account_id, "chat cold start: retire failed");
            return false;
        }
        if let Err(e) = tx.commit().await {
            tracing::warn!(error = %e, %account_id, "chat cold start: retire commit failed");
        }
        return false;
    }
    if let Err(e) = sqlx::query(
        "INSERT INTO chat_cold_start_notices (provider, account_id, container_id, posts_in_window) \
         VALUES ($1, $2, $3, 0) \
         ON CONFLICT (provider, account_id, container_id) DO NOTHING",
    )
    .bind(PROVIDER)
    .bind(account_id)
    .bind(space_name)
    .execute(&mut *tx)
    .await
    {
        tx.rollback().await.ok();
        tracing::warn!(error = %e, %account_id, "chat cold start: placeholder insert failed");
        return false;
    }
    let row: Option<(Option<DateTime<Utc>>, i32, Option<DateTime<Utc>>)> = match sqlx::query_as(
        "SELECT last_posted_at, posts_in_window, window_started_at \
         FROM chat_cold_start_notices \
         WHERE provider = $1 AND account_id = $2 AND container_id = $3 \
         FOR UPDATE",
    )
    .bind(PROVIDER)
    .bind(account_id)
    .bind(space_name)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tx.rollback().await.ok();
            tracing::warn!(error = %e, %account_id, "chat cold start: claim read failed");
            return false;
        }
    };
    let Some((last_posted_at, posts_in_window, window_started_at)) = row else {
        tx.rollback().await.ok();
        return false;
    };
    let now = Utc::now();
    if !cold_start_may_post(&ColdStartGate {
        app_present: true,
        someone_can_be_emailed: false,
        last_posted_at,
        posts_in_window,
        window_started_at,
        now,
    }) {
        tx.rollback().await.ok();
        return false;
    }
    let (posts, window_start) = next_window(posts_in_window, window_started_at, now);
    let request_id =
        chat_app_identity::cold_start_request_id(account_id, space_name, yyyymmdd(now));
    let posted =
        chat_app_identity::post_app_message(&state.http_client, space_name, &text, &request_id)
            .await;
    match posted {
        Ok(AppPost::Posted) => {}
        Ok(AppPost::NotInSpace) => {
            if let Err(e) = delete_notice(&mut tx, account_id, space_name).await {
                tx.rollback().await.ok();
                tracing::warn!(error = %e, %account_id, "chat cold start: missing-space retire failed");
                return false;
            }
            tx.commit().await.ok();
            return false;
        }
        Err(e) => {
            tx.rollback().await.ok();
            tracing::info!(error = %e, %space_name, "chat cold start: post was not accepted");
            return false;
        }
    }
    if let Err(e) = sqlx::query(
        "UPDATE chat_cold_start_notices \
         SET last_posted_at = $4, posts_in_window = $5, window_started_at = $6 \
         WHERE provider = $1 AND account_id = $2 AND container_id = $3",
    )
    .bind(PROVIDER)
    .bind(account_id)
    .bind(space_name)
    .bind(now)
    .bind(posts)
    .bind(window_start)
    .execute(&mut *tx)
    .await
    {
        tx.rollback().await.ok();
        tracing::warn!(error = %e, %account_id, "chat cold start: claim write failed");
        return false;
    }
    match tx.commit().await {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat cold start: claim commit failed");
            false
        }
    }
}

fn next_window(
    posts_in_window: i32,
    window_started_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> (i32, DateTime<Utc>) {
    let in_window = window_started_at.is_some_and(|started| now < started + WINDOW);
    if in_window {
        (posts_in_window + 1, window_started_at.unwrap_or(now))
    } else {
        (1, now)
    }
}

fn yyyymmdd(now: DateTime<Utc>) -> u32 {
    let date = now.date_naive();
    (date.year() as u32) * 10_000 + (date.month() as u32) * 100 + date.day() as u32
}

async fn someone_can_be_emailed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let scopes: Vec<String> = sqlx::query_scalar(
        "SELECT ot.scope FROM oauth_tokens ot \
         JOIN account_members am \
           ON am.account_id = ot.account_id AND am.user_id = ot.owner_user_id \
         WHERE ot.account_id = $1 AND ot.provider = 'google_chat' \
           AND ot.credential_invalid_at IS NULL",
    )
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(scopes
        .iter()
        .any(|scope| super::google_chat_invite::chat_invite_directory_grant_is_live(scope)))
}

async fn delete_notice(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    space_name: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "DELETE FROM chat_cold_start_notices \
         WHERE provider = $1 AND account_id = $2 AND container_id = $3",
    )
    .bind(PROVIDER)
    .bind(account_id)
    .bind(space_name)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn notice_web_base(state: &AppState, control: &PgPool, account_id: Uuid) -> String {
    let clerk_org_id: Option<String> = match sqlx::query_scalar(
        "SELECT clerk_org_id FROM accounts WHERE id = $1",
    )
    .bind(account_id)
    .fetch_optional(control)
    .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "chat cold start: clerk_org_id lookup failed");
            None
        }
    };
    match clerk_org_id.as_deref() {
        Some(org_id) => state.org_web_base_url(control, org_id).await,
        None => state.personal_web_base_url(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::slack::AccountPlane;

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-25T15:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn gate(now: DateTime<Utc>) -> ColdStartGate {
        ColdStartGate {
            app_present: true,
            someone_can_be_emailed: false,
            last_posted_at: None,
            posts_in_window: 0,
            window_started_at: None,
            now,
        }
    }

    #[test]
    fn the_first_connect_retires_the_cold_start() {
        let mut open = gate(t0());
        assert!(cold_start_may_post(&open));
        open.someone_can_be_emailed = true;
        assert!(!cold_start_may_post(&open));
    }

    #[test]
    fn a_space_the_app_was_not_added_to_is_not_a_cold_start() {
        let mut closed = gate(t0());
        closed.app_present = false;
        assert!(!cold_start_may_post(&closed));
        assert!(!space_is_notice_target("DIRECT_MESSAGE"));
        assert!(space_is_notice_target("SPACE"));
    }

    #[test]
    fn the_daily_cap_blocks_a_second_post_the_same_day() {
        let now = t0();
        assert!(cold_start_may_post(&gate(now)));
        let mut later = gate(now + Duration::hours(23));
        later.last_posted_at = Some(now);
        later.posts_in_window = 1;
        later.window_started_at = Some(now);
        assert!(!cold_start_may_post(&later));
        later.now = now + Duration::hours(24);
        assert!(cold_start_may_post(&later));
    }

    #[test]
    fn the_fifth_post_in_thirty_days_is_the_last() {
        let now = t0();
        let mut capped = gate(now);
        capped.last_posted_at = Some(now - DAY);
        capped.posts_in_window = 5;
        capped.window_started_at = Some(now - Duration::days(10));
        assert!(!cold_start_may_post(&capped));
        capped.window_started_at = Some(now - Duration::days(30));
        assert!(cold_start_may_post(&capped));
    }

    #[test]
    fn the_notice_is_content_free_and_the_same_for_every_recipient() {
        let text = connect_notice_text("https://acme.example");
        assert_eq!(
            text,
            "Connect your own CoralSwarm account to capture this space: https://acme.example/connectors"
        );
        assert_eq!(text, connect_notice_text("https://acme.example/"));
    }

    #[test]
    fn added_to_space_lands_on_the_org_tenant_pool_not_only_state_db() {
        let org = super::super::google_chat::added_to_space_account_plane(true);
        assert_eq!(org, AccountPlane::Org);
        assert_ne!(org, AccountPlane::Personal);
        assert_eq!(
            super::super::google_chat::added_to_space_account_plane(false),
            AccountPlane::Personal
        );
    }
}
