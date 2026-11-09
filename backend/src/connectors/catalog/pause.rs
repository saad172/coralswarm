//! The pause gate (connectors redesign, layer B3): `PLAN.md`, "Backend data",
//! decision 10.
//!
//! A member can turn one source off or pause it. Either way nothing may be
//! captured for it afterwards, from any door: the gate is checked at every place
//! a provider's data is turned into atoms (the list is in the module docs of
//! each caller, and in the table at the bottom of this file). A missed gate is
//! the redesign's top risk, so the rule is deliberately tiny and shared:
//! [`is_paused`] answers one question for one member's one source, and every
//! entry point asks it before it fetches or writes.
//!
//! # What "paused" means
//!
//! The row in `connector_sources` (control plane, RLS by account) decides:
//!
//! | row | blocked? |
//! |---|---|
//! | none (a connection made before the catalog) | no |
//! | `enabled = false` (off, or not chosen yet in the wizard) | yes |
//! | `paused_at IS NOT NULL` | yes |
//! | otherwise | no |
//!
//! A source that is off captures nothing for the same reason a paused one does,
//! so one function answers both; the status derivation keeps them apart for the
//! member (`Off` beats `Paused`).
//!
//! # Where it reads
//!
//! Always the control plane (`AppState::control_db`), whatever pool the caller's
//! atoms go to: that is where the catalog writes the row, and a capture entry
//! point on an org tenant's pool still carries the control pool. The tenant twin
//! of the table (`migrations_tenant/0216`) is not read.
//!
//! # Failing closed
//!
//! A lookup that errors counts as paused. Letting a paused source capture
//! because the database blinked is the wrong way round for a privacy control;
//! the cost of the other way is a skipped pass or event, which cursor-driven
//! paths re-run and doorbells re-deliver. With no control pool at all (a process
//! with no database) there is nothing to read and nothing is blocked.
//!
//! # Cursor-driven paths resume from their cursor
//!
//! A paused pass leaves its cursor where it was, so when the member resumes, the
//! next pass reads what changed meanwhile (Gmail history, Calendar sync). Event
//! doorbells (Slack, Graph, GitHub) are not replayed: what arrived while paused
//! is not captured.
//!
//! # Entry points and their tests
//!
//! Every capture entry point and the test that proves a paused source writes
//! nothing there. Keep this table in step with the gates: it is the checklist.
//!
//! | source | entry point | gate | test |
//! |---|---|---|---|
//! | Slack channels | doorbell (`slack_member_capture::member_capture_targets`) | scope filter | `slack_gate_pg` tests in `catalog::pause_pg_tests` |
//! | | connect and worker backfill (`backfill_one_channel`, `due_backfill_rows`) | scope filter | same |
//! | Outlook, Teams chats, channels, meetings | `graph_webhook::ingest_message_with`, the one write boundary of mail, doorbell, resync and meeting sync | `microsoft_for_atom` | `microsoft_funnel_*` |
//! | | the org-plane job (`workers::graph_ingest`) | same | `microsoft_funnel_*` |
//! | Outlook, Gmail everything-mode workers | `sync_owner` | `MS_MAIL`, `GOOGLE_GMAIL` | `everything_mode_*` |
//! | Microsoft Calendar | `microsoft_calendar_sync::{member_pass, personal_pass}` | `MS_CALENDAR` | `calendar_*` |
//! | Gmail push | `google_push::run_gmail_sync_for_mailbox` | `GOOGLE_GMAIL` | `gmail_push_*` |
//! | Google Calendar | `google_calendar_sync::{personal_pass, org_member_pass}` | `GOOGLE_CALENDAR` | `calendar_*` |
//! | Google Chat | `google_chat_sweep::sync_mailbox_space_once` | `GOOGLE_CHAT` | `chat_*` |
//! | Meet | `google_meet::ingest_transcript_for_attendee` | `GOOGLE_MEET` | `meet_*` |
//! | GitHub | `github_event_ingest::handle_event_detailed` | `GITHUB_REPOS` | `github_*` |
//! | Composio | `routes::composio::poll` | toolkit | `composio_*` |

use sqlx::PgPool;
use uuid::Uuid;

use super::{AdapterCx, AdapterError, ConnectorAdapter};
use crate::AppState;

/// One source of one connector, as the gate and the writers name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceKey {
    pub provider: &'static str,
    pub source: &'static str,
}

pub const SLACK_CHANNELS: SourceKey = SourceKey::new("slack", "channels");
pub const MS_MAIL: SourceKey = SourceKey::new("microsoft", "mail");
pub const MS_CALENDAR: SourceKey = SourceKey::new("microsoft", "calendar");
pub const MS_TEAMS_CHATS: SourceKey = SourceKey::new("microsoft", "teams_chats");
pub const MS_TEAMS_CHANNELS: SourceKey = SourceKey::new("microsoft", "teams_channels");
pub const MS_TEAMS_MEETINGS: SourceKey = SourceKey::new("microsoft", "teams_meetings");
pub const GOOGLE_GMAIL: SourceKey = SourceKey::new("google", "gmail");
pub const GOOGLE_CALENDAR: SourceKey = SourceKey::new("google", "calendar");
pub const GOOGLE_MEET: SourceKey = SourceKey::new("google", "meet");
pub const GOOGLE_CHAT: SourceKey = SourceKey::new("google", "chat");
pub const GITHUB_REPOS: SourceKey = SourceKey::new("github", "repos");

impl SourceKey {
    pub const fn new(provider: &'static str, source: &'static str) -> Self {
        Self { provider, source }
    }

    /// The source a Microsoft atom `source_type` belongs to, for the messaging
    /// funnel that carries four of them. `None` for a type that funnel does not
    /// carry (it is then not gated by it).
    pub fn microsoft_for_atom(source_type: &str) -> Option<Self> {
        match source_type {
            "outlook" => Some(MS_MAIL),
            "teams_chat" => Some(MS_TEAMS_CHATS),
            "teams_channel" => Some(MS_TEAMS_CHANNELS),
            "teams_transcript" => Some(MS_TEAMS_MEETINGS),
            _ => None,
        }
    }
}

/// The pure rule: given the member's row (`enabled`, `paused`), is capture
/// blocked? No row is a connection from before the catalog, which captures.
pub fn blocks(row: Option<(bool, bool)>) -> bool {
    match row {
        None => false,
        Some((enabled, paused)) => !enabled || paused,
    }
}

/// Whether capture is blocked for one member's source, by provider and source
/// name. See the module docs for what blocks and why a lookup error does.
pub async fn is_paused_raw(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    provider: &str,
    source: &str,
) -> bool {
    match read_row(control, account_id, user_id, provider, source).await {
        Ok(row) => blocks(row),
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id, %user_id, provider, source,
                "pause gate: could not read connector_sources -- treating the source as paused"
            );
            true
        }
    }
}

/// [`is_paused_raw`] for a [`SourceKey`].
pub async fn is_paused(control: &PgPool, account_id: Uuid, user_id: Uuid, key: SourceKey) -> bool {
    is_paused_raw(control, account_id, user_id, key.provider, key.source).await
}

/// [`is_paused`] on the process's own control pool. With none configured there
/// is nothing to read, so nothing is blocked (the same posture every entry
/// point already has toward a missing database).
pub async fn is_paused_in(
    state: &AppState,
    account_id: Uuid,
    user_id: Uuid,
    key: SourceKey,
) -> bool {
    match state.control_db() {
        Some(control) => is_paused(control, account_id, user_id, key).await,
        None => false,
    }
}

/// [`is_paused_raw`] on the process's own control pool, for a Composio toolkit.
pub async fn is_paused_toolkit_in(
    state: &AppState,
    account_id: Uuid,
    user_id: Uuid,
    toolkit: &str,
) -> bool {
    match state.control_db() {
        Some(control) => is_paused_raw(control, account_id, user_id, toolkit, "workspace").await,
        None => false,
    }
}

async fn read_row(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    provider: &str,
    source: &str,
) -> Result<Option<(bool, bool)>, AdapterError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id).await?;
    let row: Option<(bool, bool)> = sqlx::query_as(
        "SELECT enabled, paused_at IS NOT NULL FROM connector_sources \
         WHERE account_id = $1 AND owner_user_id = $2 AND provider = $3 AND source = $4",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(provider)
    .bind(source)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(row)
}

// ---------------------------------------------------------------------------
// Writers: PATCH /v1/connectors/{p}/sources/{s}
// ---------------------------------------------------------------------------

/// Materialise the member's rows for a connection made before the catalog, so a
/// write below changes one source and leaves the others as they read today. A
/// connection with a row already, or none at all, is left alone.
async fn ensure_rows(
    cx: &AdapterCx<'_>,
    adapter: &dyn ConnectorAdapter,
) -> Result<(), AdapterError> {
    super::connect::keep_legacy_connection_set_up(cx, adapter).await
}

/// Set or clear `paused_at` for one source. Idempotent: pausing a paused source
/// keeps its original time.
pub async fn write_paused(
    cx: &AdapterCx<'_>,
    adapter: &dyn ConnectorAdapter,
    source: &str,
    paused: bool,
) -> Result<(), AdapterError> {
    ensure_rows(cx, adapter).await?;
    let pool = cx
        .state
        .control_db()
        .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, cx.account_id).await?;
    sqlx::query(
        "INSERT INTO connector_sources \
             (account_id, owner_user_id, provider, source, enabled, paused_at) \
         VALUES ($1, $2, $3, $4, false, CASE WHEN $5 THEN now() END) \
         ON CONFLICT (account_id, owner_user_id, provider, source) DO UPDATE \
             SET paused_at = CASE WHEN $5 THEN COALESCE(connector_sources.paused_at, now()) END, \
                 updated_at = now()",
    )
    .bind(cx.account_id)
    .bind(cx.user_id)
    .bind(adapter.key())
    .bind(source)
    .bind(paused)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    crate::read_models::dirty::connector_changed(cx.state, cx.tenant, cx.account_id, cx.user_id)
        .await
        .map_err(|e| AdapterError::Database(e.to_string()))?;
    Ok(())
}

/// Turn one source on or off.
pub async fn write_enabled(
    cx: &AdapterCx<'_>,
    adapter: &dyn ConnectorAdapter,
    source: &str,
    on: bool,
) -> Result<(), AdapterError> {
    ensure_rows(cx, adapter).await?;
    let pool = cx
        .state
        .control_db()
        .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, cx.account_id).await?;
    sqlx::query(
        "INSERT INTO connector_sources \
             (account_id, owner_user_id, provider, source, enabled) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (account_id, owner_user_id, provider, source) DO UPDATE \
             SET enabled = $5, updated_at = now()",
    )
    .bind(cx.account_id)
    .bind(cx.user_id)
    .bind(adapter.key())
    .bind(source)
    .bind(on)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    crate::read_models::dirty::connector_changed(cx.state, cx.tenant, cx.account_id, cx.user_id)
        .await
        .map_err(|e| AdapterError::Database(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rule_blocks_off_and_paused_and_passes_legacy_and_on() {
        assert!(
            !blocks(None),
            "a connection from before the catalog captures"
        );
        assert!(!blocks(Some((true, false))));
        assert!(blocks(Some((false, false))), "off");
        assert!(blocks(Some((true, true))), "paused");
        assert!(blocks(Some((false, true))), "off and paused");
    }

    #[test]
    fn microsoft_atom_types_map_to_their_source() {
        assert_eq!(SourceKey::microsoft_for_atom("outlook"), Some(MS_MAIL));
        assert_eq!(
            SourceKey::microsoft_for_atom("teams_chat"),
            Some(MS_TEAMS_CHATS)
        );
        assert_eq!(
            SourceKey::microsoft_for_atom("teams_channel"),
            Some(MS_TEAMS_CHANNELS)
        );
        assert_eq!(
            SourceKey::microsoft_for_atom("teams_transcript"),
            Some(MS_TEAMS_MEETINGS)
        );
        assert_eq!(SourceKey::microsoft_for_atom("microsoft_calendar"), None);
    }

    #[test]
    fn every_key_names_a_real_source() {
        use crate::connectors::catalog::registry;
        for k in [
            SLACK_CHANNELS,
            MS_MAIL,
            MS_CALENDAR,
            MS_TEAMS_CHATS,
            MS_TEAMS_CHANNELS,
            MS_TEAMS_MEETINGS,
            GOOGLE_GMAIL,
            GOOGLE_CALENDAR,
            GOOGLE_MEET,
            GOOGLE_CHAT,
            GITHUB_REPOS,
        ] {
            let a = registry::lookup(k.provider).unwrap_or_else(|| panic!("{}", k.provider));
            assert!(
                a.sources().iter().any(|s| s.key == k.source),
                "{}/{}",
                k.provider,
                k.source
            );
        }
    }
}
