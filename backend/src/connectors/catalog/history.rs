//! Import history (connectors redesign, layer B4): the last wizard step.
//!
//! `POST /v1/connectors/{provider}/sources/{source}/backfill` with `{days}`
//! records the member's choice on `connector_sources` and, for a window, asks the
//! source's adapter to import it. This module is the shared half: the pure
//! rules, the recording, the guard against a second run, and the progress every
//! source reports. Provider code lives in `history/<provider>.rs`; the adapters'
//! `start_backfill` only delegate.
//!
//! # The choice
//!
//! `days` is one of `0, 7, 30, 90`. `0` is "None": the choice is recorded, setup
//! is marked complete, and nothing runs. Anything else is a window, capped by the
//! source's own `max_days`. The cap applies to the member's choice only; the
//! automatic windows (Calendar's 365 days, GitHub's unbounded backfill) are
//! untouched. The order of refusals is: not an allowed value (422), source cannot
//! backfill (409 `history_unsupported`), over this source's `max_days` (422).
//!
//! # Per-source support
//!
//! | Source | Support | How |
//! |---|---|---|
//! | Slack channels | Backfill 90 | One `HistoryBackfill` job per channel the member's capture covers (at most 25 per request), `oldest = now - days` |
//! | GitHub repos | Backfill 90 | The member's active repos get a fresh cursor with a `window_floor` and a `GithubBackfill` job; the walk stops at the first item past it |
//! | Gmail, Outlook | Unsupported | Pick mode follows threads; everything-mode lists the whole mailbox and re-walks it every 24 hours, so no window could hold |
//! | Teams chats, channels | Backfill | Durable per-watcher bounds survive continuation |
//! | Google Chat | Backfill | Durable member-scoped cursor; effective window capped at 28 days |
//! | Calendars | Unsupported (`automatic_window`) | The 365-day window is automatic and not the member's to choose |
//! | Meet, Teams meetings | Unsupported (`per_meeting`) | Capture is per meeting |
//! | Jira, Linear, Confluence, HubSpot, Zendesk, Dart | Unsupported (`starts_at_connect`) | The toolkits capture from the moment of connect |
//!
//! # Progress and the running guard
//!
//! A run is "in flight" while `history_requested_at` is under 24 hours old, a
//! window was chosen, and the source still reports progress. Slack counts its
//! jobs (`done` or `dead` over all enqueued for that request, found by the
//! request's key prefix); GitHub reports the existing per-repo `backfill_progress`;
//! nothing else backfills, so `percent: null` is never reported today. While a run
//! is in flight the source reads `importing`; when it ends the status derivation
//! returns to `active` by itself, because nothing stores "importing".
//!
//! A second request while a run is in flight is refused with 409 `backfill_running`
//! and enqueues nothing. It does not replace the run: queued jobs cannot be
//! cancelled cleanly, and a replacement would double the work. The refusal is
//! atomic: the row is updated only if `history_requested_at` is still what the
//! request saw, so two racing requests cannot both enqueue.

pub mod github;
pub mod google_chat;
pub mod slack;
pub mod teams;

#[cfg(test)]
mod pg_tests;

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use super::facts::SourceRow;
use super::{
    connect, AdapterCx, AdapterError, ConnectorAdapter, Facts, GrantHealth, HistorySupport,
    SourceRuntime, SourceSpec,
};
use crate::middleware::auth::UserContext;
use crate::tenancy::extractor::Tenant;
use crate::AppState;

/// The only values a request may carry. `0` is "None".
pub const ALLOWED_DAYS: [u32; 4] = [0, 7, 30, 90];

/// How long after a request its run still counts as in flight. A run that is
/// quiet for longer is treated as over rather than shown as importing forever.
const IN_FLIGHT_SECS: i64 = 24 * 60 * 60;

/// What the member chose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    /// Record the choice, run nothing.
    None,
    Window(u32),
}

/// Why a request did not start a run.
#[derive(Debug)]
pub enum HistoryError {
    /// 409 `history_unsupported`, with the source's reason code.
    Unsupported(&'static str),
    /// 422: not one of [`ALLOWED_DAYS`].
    InvalidDays,
    /// 422: more than this source offers.
    ExceedsMax { max_days: u32 },
    /// 409: the source is not on.
    SourceOff,
    /// 409: the source is paused.
    SourcePaused,
    /// 409: a run for this source is still going.
    Running,
    Adapter(AdapterError),
}

impl From<AdapterError> for HistoryError {
    fn from(e: AdapterError) -> Self {
        Self::Adapter(e)
    }
}

/// The pure rule: may `days` be asked of a source with this support?
pub fn choose(support: HistorySupport, days: u32) -> Result<Choice, HistoryError> {
    if !ALLOWED_DAYS.contains(&days) {
        return Err(HistoryError::InvalidDays);
    }
    match support {
        HistorySupport::Unsupported { reason } => Err(HistoryError::Unsupported(reason)),
        HistorySupport::Backfill { max_days } if days > max_days => {
            Err(HistoryError::ExceedsMax { max_days })
        }
        HistorySupport::Backfill { .. } if days == 0 => Ok(Choice::None),
        HistorySupport::Backfill { .. } => Ok(Choice::Window(days)),
    }
}

/// The oldest moment a window of `days` reaches back to.
pub fn window_floor(now: DateTime<Utc>, days: u32) -> DateTime<Utc> {
    now - Duration::days(i64::from(days))
}

fn recent(at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now - at < Duration::seconds(IN_FLIGHT_SECS)
}

/// A run the member asked for and that has not finished. The automatic backfill
/// of a source (GitHub's, started when repos are picked) is not one.
pub fn user_run_in_flight(
    rt: &SourceRuntime,
    row: Option<&SourceRow>,
    now: DateTime<Utc>,
) -> bool {
    rt.importing.is_some()
        && row.is_some_and(|r| {
            r.history_days.is_some_and(|d| d > 0) && r.history_requested_at.is_some_and(|at| recent(at, now))
        })
}

/// Fill `rt.importing` for the sources whose progress lives outside the
/// provider tables the adapter already read. Called once per source by
/// `summary::build`. GitHub's progress is already on `rt` from its own adapter.
pub async fn progress(
    cx: &AdapterCx<'_>,
    provider: &str,
    spec: &SourceSpec,
    rt: &mut SourceRuntime,
) -> Result<(), AdapterError> {
    if provider == "slack" && spec.key == "channels" {
        rt.importing = slack::importing(cx, cx.facts.source(provider, spec.key)).await?;
    }
    if provider == "microsoft" && matches!(spec.key, "teams_chats" | "teams_channels") {
        rt.importing = teams::importing(cx, spec.key).await?;
    }
    if provider == "google" && spec.key == "chat" {
        rt.importing = google_chat::importing(cx).await?;
    }
    Ok(())
}

/// Record a choice. Returns `false` when `history_requested_at` is no longer
/// `seen`: another request got there first.
async fn record(
    state: &AppState,
    account_id: Uuid,
    user_id: Uuid,
    provider: &str,
    source: &str,
    days: u32,
    seen: Option<DateTime<Utc>>,
) -> Result<bool, AdapterError> {
    let pool = state
        .control_db()
        .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id).await?;
    // The last wizard step is this one, so any valid choice, "None" included,
    // finishes setup for the source. `COALESCE` keeps the first completion time.
    let done = sqlx::query(
        "UPDATE connector_sources \
         SET history_days = $5, history_requested_at = now(), \
             setup_completed_at = COALESCE(setup_completed_at, now()), updated_at = now() \
         WHERE account_id = $1 AND owner_user_id = $2 AND provider = $3 AND source = $4 \
           AND history_requested_at IS NOT DISTINCT FROM $6",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(provider)
    .bind(source)
    .bind(days as i32)
    .bind(seen)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(done.rows_affected() == 1)
}

/// When the member's current request for this source was recorded.
pub(crate) async fn requested_at(
    cx: &AdapterCx<'_>,
    provider: &str,
    source: &str,
) -> Result<Option<DateTime<Utc>>, AdapterError> {
    let pool = cx
        .state
        .control_db()
        .ok_or_else(|| AdapterError::Database("control plane unavailable".into()))?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, cx.account_id).await?;
    let at: Option<Option<DateTime<Utc>>> = sqlx::query_scalar(
        "SELECT history_requested_at FROM connector_sources \
         WHERE account_id = $1 AND owner_user_id = $2 AND provider = $3 AND source = $4",
    )
    .bind(cx.account_id)
    .bind(cx.user_id)
    .bind(provider)
    .bind(source)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(at.flatten())
}

/// Handle one backfill request end to end. The route has already resolved the
/// adapter and the source; this validates, guards, records and starts.
pub async fn request(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user: &UserContext,
    adapter: &dyn ConnectorAdapter,
    spec: &SourceSpec,
    days: u32,
) -> Result<(), HistoryError> {
    let choice = choose(spec.history, days)?;

    // Reserve the entire initialization interval before reading progress. A
    // competing request must not observe a recorded choice with no jobs yet.
    // Transaction ownership releases the reservation on cancellation as well
    // as errors; never hold a source row lock across provider initialization.
    let pool = state.control_db().ok_or_else(||
        AdapterError::Database("control plane unavailable".into()))?;
    let mut startup = crate::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await.map_err(AdapterError::from)?;
    let reserved: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("history-start:{}:{}:{}:{}",
            user.account_id, user.user_id, adapter.key(), spec.key))
        .fetch_one(&mut *startup).await.map_err(AdapterError::from)?;
    if !reserved {
        return Err(HistoryError::Running);
    }

    let facts = Facts::load(state, tenant, user.account_id, user.user_id).await?;
    let cx = AdapterCx::new(&facts, state, tenant, user);
    match adapter.grant(&cx).await? {
        None => return Err(AdapterError::NotConnected.into()),
        Some(g) if matches!(g.health, GrantHealth::ReconnectNeeded(_)) => {
            return Err(AdapterError::Refused {
                status: 409,
                code: "reconnect_needed",
            }
            .into())
        }
        Some(_) => {}
    }
    // A connection made before the catalog has no rows yet; write them as they
    // read today, so recording the choice does not turn its sources off.
    connect::keep_legacy_connection_set_up(&cx, adapter).await?;

    let facts = Facts::load(state, tenant, user.account_id, user.user_id).await?;
    let cx = AdapterCx::new(&facts, state, tenant, user);
    let mut rt = adapter.source_runtime(&cx, spec).await?;
    progress(&cx, adapter.key(), spec, &mut rt).await?;
    if !rt.enabled {
        return Err(HistoryError::SourceOff);
    }
    if rt.paused {
        return Err(HistoryError::SourcePaused);
    }
    let row = facts.source(adapter.key(), spec.key);
    if user_run_in_flight(&rt, row, Utc::now()) {
        return Err(HistoryError::Running);
    }

    let recorded = record(
        state,
        user.account_id,
        user.user_id,
        adapter.key(),
        spec.key,
        days,
        row.and_then(|r| r.history_requested_at),
    )
    .await?;
    if !recorded {
        return Err(HistoryError::Running);
    }

    if choice == Choice::None && adapter.key() == "google" && spec.key == "chat" {
        google_chat::start(&cx, 0).await?;
    }
    if let Choice::Window(days) = choice {
        adapter.start_backfill(&cx, spec, days).await?;
    }
    startup.rollback().await.map_err(AdapterError::from)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::catalog::{registry, Importing};

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    const BACKFILL_90: HistorySupport = HistorySupport::Backfill { max_days: 90 };

    #[test]
    fn only_the_four_values_are_accepted() {
        for days in [0, 7, 30, 90] {
            assert!(choose(BACKFILL_90, days).is_ok(), "{days}");
        }
        for days in [1, 6, 8, 28, 29, 31, 89, 91, 120, 365, u32::MAX] {
            assert!(
                matches!(choose(BACKFILL_90, days), Err(HistoryError::InvalidDays)),
                "{days}"
            );
        }
    }

    #[test]
    fn zero_is_none_and_the_rest_are_windows() {
        assert_eq!(choose(BACKFILL_90, 0).unwrap(), Choice::None);
        assert_eq!(choose(BACKFILL_90, 7).unwrap(), Choice::Window(7));
        assert_eq!(choose(BACKFILL_90, 90).unwrap(), Choice::Window(90));
    }

    #[test]
    fn a_source_caps_only_the_members_choice() {
        let chat = HistorySupport::Backfill { max_days: 28 };
        assert_eq!(choose(chat, 7).unwrap(), Choice::Window(7));
        assert_eq!(choose(chat, 0).unwrap(), Choice::None);
        for days in [30, 90] {
            assert!(matches!(
                choose(chat, days),
                Err(HistoryError::ExceedsMax { max_days: 28 })
            ));
        }
    }

    #[test]
    fn an_unsupported_source_refuses_with_its_reason_even_for_none() {
        let s = HistorySupport::Unsupported {
            reason: "starts_at_connect",
        };
        for days in [0, 7, 90] {
            assert!(matches!(
                choose(s, days),
                Err(HistoryError::Unsupported("starts_at_connect"))
            ));
        }
        // A bad value is still a bad value, whatever the source.
        assert!(matches!(choose(s, 120), Err(HistoryError::InvalidDays)));
    }

    #[test]
    fn the_floor_is_whole_days_back() {
        let now = at("2026-10-30T12:00:00Z");
        assert_eq!(window_floor(now, 7), at("2026-10-23T12:00:00Z"));
        assert_eq!(window_floor(now, 30), at("2026-09-30T12:00:00Z"));
        assert_eq!(window_floor(now, 90), at("2026-08-01T12:00:00Z"));
    }

    #[test]
    fn a_run_is_in_flight_only_while_requested_recently_and_progressing() {
        let now = at("2026-10-30T12:00:00Z");
        let importing = SourceRuntime {
            importing: Some(Importing {
                percent: Some(10),
                window_days: 7,
            }),
            ..Default::default()
        };
        let row = |days: Option<i32>, requested: Option<&str>| SourceRow {
            provider: "slack".into(),
            source: "channels".into(),
            enabled: true,
            paused_at: None,
            scope_mode: None,
            history_days: days,
            history_requested_at: requested.map(at),
            setup_completed_at: None,
        };
        let fresh = row(Some(7), Some("2026-10-30T11:00:00Z"));
        assert!(user_run_in_flight(&importing, Some(&fresh), now));
        // Not importing: nothing is in flight whatever the row says.
        assert!(!user_run_in_flight(
            &SourceRuntime::default(),
            Some(&fresh),
            now
        ));
        // GitHub's automatic backfill reports progress with no request on file.
        assert!(!user_run_in_flight(&importing, Some(&row(None, None)), now));
        assert!(!user_run_in_flight(&importing, None, now));
        // "None" runs nothing.
        assert!(!user_run_in_flight(
            &importing,
            Some(&row(Some(0), Some("2026-10-30T11:00:00Z"))),
            now
        ));
        // A request a day old is over, not importing for ever.
        assert!(!user_run_in_flight(
            &importing,
            Some(&row(Some(7), Some("2026-10-29T11:00:00Z"))),
            now
        ));
    }

    /// The support table, as built. A change to any row is a decision, so it
    /// fails here first.
    #[test]
    fn the_support_table() {
        use HistorySupport::*;
        let support = |p: &str, s: &str| {
            registry::lookup(p)
                .unwrap_or_else(|| panic!("no {p}"))
                .sources()
                .iter()
                .find(|x| x.key == s)
                .unwrap_or_else(|| panic!("no {p}/{s}"))
                .history
        };
        assert_eq!(support("slack", "channels"), Backfill { max_days: 90 });
        assert_eq!(support("github", "repos"), Backfill { max_days: 90 });

        let no = |reason| Unsupported { reason };
        assert_eq!(support("microsoft", "mail"), no("followed_threads_only"));
        assert_eq!(support("google", "gmail"), no("followed_threads_only"));
        assert_eq!(support("microsoft", "teams_chats"), BACKFILL_90);
        assert_eq!(support("microsoft", "teams_channels"), BACKFILL_90);
        assert_eq!(support("google", "chat"), BACKFILL_90);
        assert_eq!(support("microsoft", "calendar"), no("automatic_window"));
        assert_eq!(support("google", "calendar"), no("automatic_window"));
        assert_eq!(support("microsoft", "teams_meetings"), no("per_meeting"));
        assert_eq!(support("google", "meet"), no("per_meeting"));
        for toolkit in ["jira", "linear", "confluence", "hubspot", "zendesk", "dart"] {
            assert_eq!(support(toolkit, "workspace"), no("starts_at_connect"), "{toolkit}");
        }
    }

    /// A source that can backfill must be able to start one: its adapter's
    /// `start_backfill` is the only thing the route calls, and the trait default
    /// answers 501.
    #[test]
    fn every_backfillable_source_is_one_with_an_implementation() {
        let mut backfillable = Vec::new();
        for a in registry::all() {
            for s in a.sources() {
                if matches!(s.history, HistorySupport::Backfill { .. }) {
                    backfillable.push(format!("{}/{}", a.key(), s.key));
                }
            }
        }
        backfillable.sort();
        // Adding a row here means writing its `history/<provider>.rs` first.
        assert_eq!(backfillable, ["github/repos", "google/chat", "microsoft/teams_channels", "microsoft/teams_chats", "slack/channels"]);
    }
}
