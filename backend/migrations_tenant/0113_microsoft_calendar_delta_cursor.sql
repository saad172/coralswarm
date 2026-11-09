-- Migration: 0113_microsoft_calendar_delta_cursor (tenant twin of control 0272)
-- Description: Microsoft Calendar deletions (fix/cal-deletions). The Graph
--   `calendarView/delta` cursor columns for one org member's Microsoft
--   Calendar connection — see `migrations_control/0272_microsoft_calendar_
--   delta_cursor.sql`'s header for the bug (deleted Outlook events never
--   left the ocean: `fetch_events` was a cursorless `$top=20` list that only
--   ever saw what still exists), the design (`connectors::microsoft_calendar`
--   syncs through `calendarView/delta`, persisting the `@odata.deltaLink`
--   here exactly as Google persists its `syncToken`), and what each column
--   means. Same four columns, same semantics.
--
--   The `member_ocean_isolation` policy from 0067 is NOT touched: on the
--   tenant plane every row is an org member's (the personal-plane widening
--   control 0272 performs has no counterpart here — a personal account has
--   no tenant database).
--
-- Depends on: 0067_microsoft_calendar_sync_state (the table), 0112_atoms_
--             content_hash (the edit path the delta items feed).
-- Down:
--   ALTER TABLE microsoft_calendar_sync_state
--       DROP COLUMN IF EXISTS delta_link,
--       DROP COLUMN IF EXISTS delta_resume_link,
--       DROP COLUMN IF EXISTS delta_window_end,
--       DROP COLUMN IF EXISTS delta_reconciled_at;

ALTER TABLE microsoft_calendar_sync_state
    ADD COLUMN IF NOT EXISTS delta_link          TEXT,
    ADD COLUMN IF NOT EXISTS delta_resume_link   TEXT,
    ADD COLUMN IF NOT EXISTS delta_window_end    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS delta_reconciled_at TIMESTAMPTZ;
