//! Phase G item 48 — the 30-day grace on org-install capture.
//!
//! Founder, 2026-09-24: the org-install paths keep running for 30 days after
//! the connect-your-own emails begin (items 49 and 50), then they cannot be
//! reached. The instant lives in `org_install_capture_grace` (control
//! migration 0257). This module reads it. Callers do not add "now plus 30
//! days" themselves.
//!
//! What closes when the window closes, on `main`:
//!
//! * Slack `resolve_org_fanout_destinations` — copying one bot-captured
//!   message into every mapped member's ocean.
//! * Teams RSC app-mode watches (`auth_mode = 'app'`): install, live
//!   subscription, history backfill, renewal, backstop re-poll, and the
//!   webhook that fetches with the app token.
//! * The tenant-wide `appCatalogs/teamsApps/{id}/installedToOnlineMeetings/
//!   getAllTranscripts` subscription. Not the delegated organiser resource
//!   `users/{id}/onlineMeetings/getAllTranscripts` (item 51), and not the
//!   attendee calendar pull.
//!
//! `resolve_teams_fanout_destinations` / `graphs_for_matched_members` are
//! not on `main` (held PR #1000). Nothing here deletes them.
//!
//! A missing row or a failed read stays OPEN. Closing capture because the
//! clock could not be read would drop existing orgs on a blip. The closed
//! state is a successful read of an instant that is already 30 days old.
//!
//! Lib tests pin the migration's seed instant (item 50's merge, written
//! when the notices table has no `member_connect` row) as still open, so
//! the rest of the suite keeps exercising fan-out after the calendar day
//! the production binary turns it off. Tests that need a closed window
//! write a different instant. The production binary does not have that pin.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;

/// Thirty calendar days of 24 hours. The window is open while `now` is
/// strictly before `began + GRACE_DAYS`.
pub(crate) const GRACE_DAYS: i64 = 30;

/// Stable prefix on the error `teams_ingest::install_watch` returns once
/// the window is closed, so a test can tell this refusal from a
/// cross-member conflict.
pub(crate) const GRACE_CLOSED_INSTALL: &str = "org-install grace closed";

/// Item 50 squash-merged `aab538a1` at 2026-09-25 10:05:10 -0500.
#[cfg(test)]
pub(crate) fn item_50_merge_instant() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-09-25T15:05:10Z")
        .expect("item 50 merge instant")
        .with_timezone(&Utc)
}

pub(crate) fn capture_open(began: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now < began + Duration::days(GRACE_DAYS)
}

/// RSC app-mode (`auth_mode = 'app'`) is the org-install watch. Delegated
/// watches are the per-member path and stay reachable either way.
pub(crate) fn rsc_app_capture_blocked(capture_open: bool, auth_mode: &str) -> bool {
    !capture_open && auth_mode == "app"
}

/// Tenant-wide RSC transcript stream. The delegated organiser subscription
/// (`users/{id}/onlineMeetings/getAllTranscripts`) does not contain this
/// segment.
pub(crate) fn is_tenant_wide_transcript_resource(resource: &str) -> bool {
    resource.contains("installedToOnlineMeetings/getAllTranscripts")
}

/// `None` when the window is closed, so callers do not create or renew the
/// tenant-wide transcript subscription. `resource` is whatever
/// `microsoft::transcript_subscription_resource` computed (itself `None`
/// when `TEAMS_APP_ID` is unset).
pub(crate) fn tenant_wide_transcript_to_subscribe(
    capture_open: bool,
    resource: Option<String>,
) -> Option<String> {
    if capture_open {
        resource
    } else {
        None
    }
}

/// Read the control-plane clock. Fail open — see the module doc.
pub(crate) async fn org_install_capture_open(pool: &PgPool) -> bool {
    match connect_emails_began_at(pool).await {
        Ok(Some(began)) => {
            if test_seed_stays_open(began) {
                return true;
            }
            capture_open(began, Utc::now())
        }
        Ok(None) => {
            tracing::warn!("org install grace row missing — capture stays on");
            true
        }
        Err(e) => {
            tracing::warn!(error = %e, "org install grace read failed — capture stays on");
            true
        }
    }
}

async fn connect_emails_began_at(pool: &PgPool) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar("SELECT connect_emails_began_at FROM org_install_capture_grace")
        .fetch_optional(pool)
        .await
}

/// The migration seed is the item 50 merge when no `member_connect` row
/// exists. Lib tests keep that exact instant open so unrelated suites do
/// not start failing on the calendar day production closes the window.
/// A test that writes any other instant gets the real comparison.
fn test_seed_stays_open(began: DateTime<Utc>) -> bool {
    #[cfg(test)]
    {
        began.timestamp() == item_50_merge_instant().timestamp()
    }
    #[cfg(not(test))]
    {
        let _ = began;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn org_install_grace_capture_is_open_until_thirty_days() {
        let began = item_50_merge_instant();
        let last_open = began + Duration::days(GRACE_DAYS) - Duration::seconds(1);
        assert!(capture_open(began, began));
        assert!(capture_open(began, last_open));
    }

    #[test]
    fn org_install_grace_capture_is_closed_at_thirty_days() {
        let began = item_50_merge_instant();
        let closed = began + Duration::days(GRACE_DAYS);
        assert!(!capture_open(began, closed));
        assert!(!capture_open(began, closed + Duration::days(1)));
    }

    #[test]
    fn org_install_grace_rsc_app_watch_still_runs_before_the_window_closes() {
        assert!(
            !rsc_app_capture_blocked(true, "app"),
            "an open window must not block an RSC app-mode watch"
        );
        assert!(
            !rsc_app_capture_blocked(true, "delegated"),
            "delegated watches are not this gate"
        );
    }

    #[test]
    fn org_install_grace_rsc_app_watch_is_unreachable_after_the_window_closes() {
        assert!(
            rsc_app_capture_blocked(false, "app"),
            "a closed window must block an RSC app-mode watch"
        );
        assert!(
            !rsc_app_capture_blocked(false, "delegated"),
            "closing the org-install window must not block a member's own watch"
        );
    }

    #[test]
    fn org_install_grace_tenant_wide_transcript_subscription_still_runs_before_the_window_closes() {
        let resource =
            "appCatalogs/teamsApps/APP/installedToOnlineMeetings/getAllTranscripts".to_string();
        assert!(is_tenant_wide_transcript_resource(&resource));
        assert_eq!(
            tenant_wide_transcript_to_subscribe(true, Some(resource.clone())).as_deref(),
            Some(resource.as_str()),
            "an open window still creates the tenant-wide transcript subscription"
        );
    }

    #[test]
    fn org_install_grace_tenant_wide_transcript_subscription_is_unreachable_after_the_window_closes(
    ) {
        let resource =
            "appCatalogs/teamsApps/APP/installedToOnlineMeetings/getAllTranscripts".to_string();
        assert!(
            tenant_wide_transcript_to_subscribe(false, Some(resource)).is_none(),
            "a closed window must not hand the tenant-wide resource to ensure_subscription"
        );
    }

    #[test]
    fn org_install_grace_delegated_organiser_transcript_is_not_the_tenant_wide_stream() {
        let delegated = "users/aad-user/onlineMeetings/getAllTranscripts";
        assert!(
            !is_tenant_wide_transcript_resource(delegated),
            "item 51's per-member push is a different resource and stays"
        );
        // The attendee pull is not a subscription resource at all. It must
        // not match the tenant-wide stream this gate turns off.
        assert!(!delegated.contains("installedToOnlineMeetings"));
    }
}
