-- Migration: 0114_microsoft_calendar_delta_window_start (tenant twin of control 0273)
-- Description: Microsoft Calendar history window (cs-mscal-body-window).
--   The `startDateTime` an org member's current `calendarView/delta` round
--   was anchored at, next to 0113's `delta_window_end` — see
--   `migrations_control/0273_microsoft_calendar_delta_window_start.sql`'s
--   header for the gap (events older than 30 days were never synced), the
--   new 365-day window, and why recording the start makes a cursor frozen
--   on a narrower window re-anchor on its next pass instead of waiting for
--   the scheduled re-anchor. Same column, same semantics; the
--   `member_ocean_isolation` policy is untouched.
--
-- Depends on: 0113_microsoft_calendar_delta_cursor.
-- Down:
--   ALTER TABLE microsoft_calendar_sync_state
--       DROP COLUMN IF EXISTS delta_window_start;

ALTER TABLE microsoft_calendar_sync_state
    ADD COLUMN IF NOT EXISTS delta_window_start TIMESTAMPTZ;
