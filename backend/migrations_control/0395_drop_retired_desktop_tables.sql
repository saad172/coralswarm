-- Migration: 0395_drop_retired_desktop_tables
-- PAIRED WITH: tenant 0210 (drop_retired_desktop_tables).
-- contract: safe-after server-2026.10.29.5
--
-- The desktop app is retired (ADR 0015, docs/decisions/0015-retire-desktop-app.md).
-- Drops the four control tables only it used:
--   desktop_tokens           (0224) the desktop sync bearer token (`csdt_`)
--   capture_policies         (0194) per-account desktop capture policy
--   org_capture_attestations (0194) org-admin desktop capture attestation
--   sync_cursors             (0023) desktop<->cloud oplog sync cursor
-- None carries RLS. Their indexes go with them. No other table references
-- them by foreign key; org_capture_attestations' own RESTRICT FK to users
-- goes with the table.
--
-- account_deletion_user_keyed_tables() (0370) lists desktop_tokens, and
-- account_deletion_purge_user_rows() deletes from every table it returns, so
-- it is redefined first, verbatim minus that entry. CREATE OR REPLACE keeps
-- its owner and the EXECUTE grants 0370 made.
--
-- Contract migration: the release named above is the first one whose binary
-- no longer reads or writes these tables. The placeholder is replaced with
-- that release's tag once a later release exists
-- (scripts/check_migration_contract.sh).

CREATE OR REPLACE FUNCTION public.account_deletion_user_keyed_tables()
RETURNS TABLE (tbl TEXT, col TEXT)
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, public, pg_temp AS $$
    VALUES
    ('account_members','user_id'), ('addin_microsoft_links','user_id'),
    ('addin_microsoft_sessions','user_id'), ('addin_pairing_codes','owner_user_id'),
    ('addin_tokens','owner_user_id'), ('conversations','user_id'),
    ('escrow_recovery_log','recovered_user_id'),
    ('escrow_shares','admin_user_id'), ('escrow_wrapped_keys','user_id'),
    ('gmail_everything_mode','owner_user_id'), ('gmail_hidden_messages','owner_user_id'),
    ('gmail_watched_threads','ingested_by'), ('google_chat_watched_spaces','owner_user_id'),
    ('google_chat_watched_spaces','reader_user_id'),
    ('google_meet_ingested_conferences','owner_user_id'), ('google_meet_sync_state','owner_user_id'),
    ('google_watch_channels','owner_user_id'), ('journal_rollups','user_id'),
    ('mcp_tasks','owner_user_id'), ('mcp_tenant_selection','user_id'),
    ('microsoft_calendar_sync_state','owner_user_id'), ('org_auto_adds','user_id'),
    ('org_join_request_digests','admin_user_id'), ('org_join_requests','user_id'),
    ('org_membership_tombstones','user_id'), ('outlook_everything_mode','owner_user_id'),
    ('outlook_watched_conversations','ingested_by'), ('outlook_workflows','user_id'),
    ('queries','user_id'), ('query_history','user_id'), ('sessions','user_id'),
    ('slack_member_channels','user_id'), ('slack_watched_channels','user_id'),
    ('slot_reservations','user_id'), ('team_member_key_envelopes','user_id'),
    ('team_members','user_id'), ('team_seat_grants','user_id'),
    ('teams_container_watchers','user_id'), ('teams_watched_resources','user_id'),
    ('user_access_log','user_id'), ('user_account_prefs','user_id'),
    ('user_active_graph','user_id'), ('user_prefs','user_id'),
    ('user_private_keys','user_id'), ('connector_identities','user_id'),
    ('platform_active_users','user_id'), ('mcp_tool_calls','user_id');
$$;

DROP TABLE IF EXISTS desktop_tokens, capture_policies, org_capture_attestations, sync_cursors;
