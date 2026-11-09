-- Migration: 0069_calendar_reminder_prefs
-- Description: Per-account preference gating the desktop pre-meeting reminder
--   nudge (cs-uxp4a-calendar). When a calendar is connected the desktop app
--   polls `/v1/calendar/upcoming` and fires an OS notification ~1 minute before
--   an event with 2+ attendees ("<title> starts in 1 minute"). This flag lets a
--   user turn those reminders off from web/desktop Settings without
--   disconnecting the calendar; the value follows the user across devices.
--
--   Keyed to `accounts`, not `users`: a calendar connection lives in
--   `oauth_tokens` scoped by `account_id` (provider='google'), so the reminder
--   preference is scoped the same way — the same identity the poll runs as.
--
--   A dedicated boolean column (rather than the `account_prefs` JSONB blob) so
--   the desktop poll can read it with a single indexed key lookup, matching the
--   precedent set by `user_prefs.mcp_write_enabled` (0060). Default TRUE: once a
--   user connects a calendar, reminders are on unless they opt out — the whole
--   point of the feature is the pre-meeting nudge. Absence of a row is treated
--   as the default (TRUE) by the read path, so connecting a calendar needs no
--   write here.
--
-- Depends on: 0001_init (accounts)
--
-- Down:
--   DROP TABLE IF EXISTS calendar_prefs;

CREATE TABLE IF NOT EXISTS calendar_prefs (
    account_id            UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    premeeting_reminders  BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
