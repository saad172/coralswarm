-- Migration: 0115_microsoft_calendar_verify_sweep (tenant twin of control 0276)
-- Description: Microsoft Calendar reconcile sweep cursor (cs-mscal-body-window).
--   Where an org member's verification of stored corals against Graph
--   stands — see `migrations_control/0276_microsoft_calendar_verify_sweep.sql`'s
--   header for the gap (reconcile only ever verified the same 200
--   lowest-sorting ids after a multi-pass full round, so a deleted series
--   whose master id sorted later was never retracted) and the circular,
--   resumable sweep these two columns make possible. Same columns, same
--   semantics; the `member_ocean_isolation` policy is untouched.
--
-- Depends on: 0114_microsoft_calendar_delta_window_start.
-- Down:
--   ALTER TABLE microsoft_calendar_sync_state
--       DROP COLUMN IF EXISTS delta_verify_cursor,
--       DROP COLUMN IF EXISTS delta_verify_lap_end;

ALTER TABLE microsoft_calendar_sync_state
    ADD COLUMN IF NOT EXISTS delta_verify_cursor  TEXT,
    ADD COLUMN IF NOT EXISTS delta_verify_lap_end TEXT;
