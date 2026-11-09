-- Migration: 0061_graph_backfill_cursors
-- Description: Schema-parity twin of
--   `migrations_control/0179_graph_backfill_cursors.sql` (m365 Train 2 PR
--   2.6). `outlook_watched_conversations`, `teams_watched_resources`, and
--   `outlook_workflows` are all SHARED tables (`scripts/check_schema_parity.
--   sh`'s `CONTROL_ONLY_RLS_TABLES` roster) — a tenant DB holds its own copy
--   of each from the baseline dump (`migrations_tenant/0001_tenant_baseline.
--   sql`), so any schema change must land identically on both trees or a
--   fresh tenant DB silently diverges from control. Same discipline as
--   tenant migration `0059`/`0060`'s pairing with control's `0175`/`0177`.
--
--   Byte-identical column additions (see `0179`'s own header for the full
--   rationale). NOT inert here the way `0059`/`0060` were for `oauth_tokens`:
--   `teams_watched_resources` is already populated on the org tenant plane
--   (m365 Train 2 PR 2.4's roster auto-seating), and PR 2.6's resumable
--   backfill continuation tick reads/writes `backfill_cursor`/
--   `backfill_complete` on `t.pool` for org tenants exactly like it does for
--   the consumer plane (two-pool split; see `connectors::microsoft`'s
--   RLS-scoping module doc and `workers::graph_subscriptions`'s own doc for
--   the pattern this rides).
--
-- Down:
--   ALTER TABLE outlook_watched_conversations DROP COLUMN IF EXISTS backfill_cursor, DROP COLUMN IF EXISTS backfill_complete;
--   ALTER TABLE teams_watched_resources DROP COLUMN IF EXISTS backfill_cursor, DROP COLUMN IF EXISTS backfill_complete;
--   ALTER TABLE outlook_workflows DROP COLUMN IF EXISTS backfill_cursor, DROP COLUMN IF EXISTS backfill_complete;

ALTER TABLE outlook_watched_conversations
    ADD COLUMN IF NOT EXISTS backfill_cursor   TEXT,
    ADD COLUMN IF NOT EXISTS backfill_complete BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE teams_watched_resources
    ADD COLUMN IF NOT EXISTS backfill_cursor   TEXT,
    ADD COLUMN IF NOT EXISTS backfill_complete BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE outlook_workflows
    ADD COLUMN IF NOT EXISTS backfill_cursor   TEXT,
    ADD COLUMN IF NOT EXISTS backfill_complete BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_outlook_watched_conversations_backfill_pending
    ON outlook_watched_conversations (account_id)
    WHERE NOT backfill_complete;

CREATE INDEX IF NOT EXISTS idx_teams_watched_resources_backfill_pending
    ON teams_watched_resources (account_id)
    WHERE NOT backfill_complete;

CREATE INDEX IF NOT EXISTS idx_outlook_workflows_backfill_pending
    ON outlook_workflows (account_id)
    WHERE NOT backfill_complete;
