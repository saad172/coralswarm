-- Migration 0063: Google Calendar sync-token/backfill columns on
--   google_watch_channels — tenant twin of
--   migrations_control/0182_google_calendar_sync_state.sql (gsuite Train G3
--   PR 1, schema-parity twin pattern per m365 Train 2 PR 2.2/#661's
--   precedent, same as 0180/0062).
--
--   Byte-identical column set to the control-tree migration; see that
--   migration's header for the full column-by-column rationale.
--
--   No RLS change: this table has no `graph_id`/ocean anchor
--   (`migrations_tenant/0062_google_watch_channels.sql`'s own header already
--   explains why it is deliberately NOT covered by `member_ocean_isolation`,
--   same call as `graph_subscriptions`), and adding three columns to an
--   existing table doesn't change that.
--
--   Partial unique index on `channel_id` (CodeRabbit review finding on PR
--   #677, round 2 — see the control migration's header for the full
--   rationale): added here purely for schema-parity (`scripts/
--   check_schema_parity.sh` diffs the two trees structurally) — an org
--   tenant database holds exactly one account (ADR-0007), so the
--   cross-account collision this index prevents on the control plane
--   cannot occur here at all; the index is a harmless no-op guarantee on
--   this tree, not a behavior change.
--
-- Depends on: 0062_google_watch_channels.sql
-- Down:
--   DROP INDEX IF EXISTS idx_google_watch_channels_channel_id_unique;
--   ALTER TABLE google_watch_channels
--       DROP COLUMN IF EXISTS sync_token,
--       DROP COLUMN IF EXISTS backfill_page_token,
--       DROP COLUMN IF EXISTS backfill_complete;

ALTER TABLE google_watch_channels
    ADD COLUMN IF NOT EXISTS sync_token           TEXT        NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS backfill_page_token   TEXT,
    ADD COLUMN IF NOT EXISTS backfill_complete     BOOLEAN     NOT NULL DEFAULT FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_google_watch_channels_channel_id_unique
    ON google_watch_channels (channel_id)
    WHERE channel_id <> '';
