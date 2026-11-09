-- Drop the temporary huddle-event diagnostic table (`slack_huddle_probe`,
-- migration 0100). Its own header said "delete this once the trigger is
-- confirmed and the rewrite lands" — both have happened: the trigger is
-- pinned by the committed fixtures at `backend/tests/fixtures/slack_huddle/`,
-- and `huddle_capture_trigger` (`backend/src/routes/slack_events.rs`) is the
-- shipped rewrite that reads them. Production held exactly 4 rows, all
-- written during the measurement run itself. Founder-approved deletion.
--
-- 0100 is ALREADY DEPLOYED, so migrations stay append-only: this undoes its
-- effect with a new migration rather than editing 0100 in place.
--
-- No paired tenant-tree migration: 0100's own header established
-- `slack_huddle_probe` as CONTROL-plane only (diagnostic exhaust, no
-- account_id, no RLS, no retention policy, never read by application code),
-- and it was registered in `CONTROL_TABLES` in `scripts/check_schema_parity.sh`
-- for exactly that reason — dropped from the control-vs-tenant comparison
-- rather than requiring a tenant-tree twin. That registration is removed in
-- this same change now that the table itself is gone.

DROP TABLE IF EXISTS slack_huddle_probe;
