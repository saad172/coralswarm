-- Migration: 0093_member_ocean_lifecycle_log_table_counts_key
-- Description: member-private-oceans PR 8 (docs/plans/member-private-oceans.md
--              §2.6 "Lifecycle" / §7) — the purge arm. Widens
--              `member_ocean_lifecycle_log`'s metadata-only manifest allowlist
--              (0092) by exactly ONE new key: `table_counts`.
--
--              ── Why a new key rather than one per table ──
--              Purge's audit manifest needs a per-table row count (plan
--              §2.6 step 5: "flip the log row to completed with per-table
--              counts") across the FULL `graph_id`-bearing content roster
--              enumerated in `0083_consumer_rls.sql`'s §(a)/(b)/(c)/(d)
--              (`atoms`, `nodes`, `node_atoms`, `relationship_edge`,
--              `meeting_transcripts`, `transcript_corrections`,
--              `coding_sessions`, `coding_session_shares`, `meeting_shares`,
--              `conversations`, `conversation_messages`,
--              `conversation_surfaced_edges`, `conversation_oceans`, `teams`,
--              `team_members`, `team_keys`, `team_member_key_envelopes`,
--              `projects`, `node_merges`, `node_promotions`,
--              `supersession_edge`, `non_supersession_pair`, `op_log`,
--              `activity_events`, `query_history`,
--              `outlook_watched_conversations`, `outlook_workflows`,
--              `teams_watched_resources`, plus the `graphs` row itself — see
--              `workers::member_ocean_reconciler::GRAPH_CONTENT_TABLES`, the
--              Rust-side roster this migration's key exists to hold counts
--              for). 0092's allowlist already has individual keys for a few
--              of these (`atoms`, `nodes`, `node_atoms`, `relationship_edges`,
--              `transcripts`, `conversations`) predating this PR, but adding
--              a dedicated top-level key per remaining table would mean
--              relitigating this CHECK on every future schema addition to the
--              roster. One `table_counts` key holding a `{"table_name":
--              count, ...}` object is exactly as metadata-only as the
--              existing per-table keys (identifiers as object keys, integers
--              as values — never row content) and absorbs the rest of the
--              roster without a running list of individual key names baked
--              into a CHECK constraint.
--
--              This migration does NOT touch `member_ocean_lifecycle_log`'s
--              other CHECK (`..._finished_at_matches_status`, unchanged) and
--              adds no new table — `member_ocean_lifecycle_log` is already in
--              `CONTROL_TABLES` (`scripts/check_schema_parity.sh`, added in
--              0092) so no parity-script change is needed here either.
--
-- Depends on: 0092_member_ocean_control_tables (creates the table and the
--             CHECK this migration widens).
--
-- Down:
--   ALTER TABLE member_ocean_lifecycle_log
--     DROP CONSTRAINT IF EXISTS member_ocean_lifecycle_log_manifest_metadata_only;
--   ALTER TABLE member_ocean_lifecycle_log
--     ADD CONSTRAINT member_ocean_lifecycle_log_manifest_metadata_only CHECK (
--       manifest - ARRAY[
--         'atoms', 'nodes', 'node_atoms', 'blobs', 'bytes',
--         'relationship_edges', 'transcripts', 'conversations',
--         'manifest_sha256', 'export_id', 'schema_version',
--         'offboarded_at', 'archived_at', 'drift_seconds', 'error'
--       ] = '{}'::jsonb
--     );

ALTER TABLE member_ocean_lifecycle_log
  DROP CONSTRAINT IF EXISTS member_ocean_lifecycle_log_manifest_metadata_only;

ALTER TABLE member_ocean_lifecycle_log
  ADD CONSTRAINT member_ocean_lifecycle_log_manifest_metadata_only CHECK (
    manifest - ARRAY[
      'atoms', 'nodes', 'node_atoms', 'blobs', 'bytes',
      'relationship_edges', 'transcripts', 'conversations',
      'manifest_sha256', 'export_id', 'schema_version',
      'offboarded_at', 'archived_at', 'drift_seconds', 'error',
      'table_counts'
    ] = '{}'::jsonb
  );
