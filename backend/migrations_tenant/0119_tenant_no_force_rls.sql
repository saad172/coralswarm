-- Migration: 0119_tenant_no_force_rls
-- Description: Restore the tenant plane's "no FORCE ROW LEVEL SECURITY"
--   invariant (0009_tenant_rls.sql §7) on five per-meeting-capture tables.
--
--   0009 §7 deliberately leaves FORCE off every tenant table:
--     1. The tenant app connects as the table owner, and the owner is exempt
--        from ENABLE-only RLS. FORCE removes that exemption, which changes
--        behaviour for every owner-connected code path.
--     3. FORCE would break migrations and ops tooling (backfills,
--        `export_member_ocean`, purge) that run as the owner and need the
--        unfiltered view.
--   `tenant_rls_pg` asserts the invariant.
--
--   Four tenant migrations copied FORCE over from their control-plane twins
--   by mistake:
--     0105_teams_meeting_captures   -> teams_meeting_installations,
--                                      teams_meeting_captures
--     0107_outlook_capture_receipts -> outlook_capture_receipts
--     0109_outlook_schedule_intents -> outlook_schedule_intents
--     0111_google_meet_captures     -> google_meet_captures
--   Merged migrations are immutable, so this one removes it instead.
--
--   RLS stays ENABLED and every policy is untouched. The non-owner
--   `coralswarm_tenant_app` role is still subject to each policy.
--
--   Tenant-only. Control keeps FORCE on the twins (0260/0263/0266/0269).
--   pg_dump emits the FORCE flag, so after this migration the two planes
--   differ on it. `scripts/check_schema_parity.sh` strips FORCE from these
--   five tables on the control side (`CONTROL_ONLY_FORCE_RLS_TABLES`), and
--   `consumer_rls_pg` asserts that control still FORCEs them.
--
--   Idempotent: NO FORCE on a table that is already NO FORCE is a no-op.
--
-- Down (not recommended; violates 0009 §7):
--   ALTER TABLE <each table below> FORCE ROW LEVEL SECURITY;

ALTER TABLE teams_meeting_installations NO FORCE ROW LEVEL SECURITY;
ALTER TABLE teams_meeting_captures NO FORCE ROW LEVEL SECURITY;
ALTER TABLE outlook_capture_receipts NO FORCE ROW LEVEL SECURITY;
ALTER TABLE outlook_schedule_intents NO FORCE ROW LEVEL SECURITY;
ALTER TABLE google_meet_captures NO FORCE ROW LEVEL SECURITY;
