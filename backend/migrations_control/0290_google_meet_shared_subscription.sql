-- Migration: 0290_google_meet_shared_subscription
-- Description: let one Workspace Events subscription back a Meet watch row in
--   more than one account.
--
--   Google allows one Meet subscription per (authorizing user, target resource)
--   per app; a second `subscriptions.create` for the same Google user returns
--   409 ALREADY_EXISTS. The same Google user connected in two CoralSwarm
--   accounts (a personal account and an org) therefore has to share one
--   subscription name. Two unique indexes on `channel_id` made the second row
--   impossible: 0286's Meet-only `uq_google_meet_subscription` and 0182's
--   all-surface `idx_google_watch_channels_channel_id_unique`. The first is
--   dropped; the second now excludes Meet rows and keeps guarding Calendar,
--   Gmail and Chat channel ids exactly as before. Tenant twin: 0125.
--
--   Per-account identity is unchanged: `UNIQUE (account_id, surface,
--   resource_id)` (0062) still allows one row per account per Google user.
--   Routing by name stays indexed, now non-unique, so
--   `resolve_google_meet_subscription` fans out to every account's row.
--
-- Depends on: 0182_google_calendar_sync_state.sql, 0286_google_meet_push.sql.
-- Down:
--   DROP INDEX IF EXISTS idx_google_meet_subscription;
--   CREATE UNIQUE INDEX uq_google_meet_subscription ON google_watch_channels(channel_id) WHERE surface='meet' AND channel_id <> '';
--   DROP INDEX IF EXISTS idx_google_watch_channels_channel_id_unique;
--   CREATE UNIQUE INDEX idx_google_watch_channels_channel_id_unique ON google_watch_channels (channel_id) WHERE channel_id <> '';

DROP INDEX IF EXISTS uq_google_meet_subscription;
CREATE INDEX IF NOT EXISTS idx_google_meet_subscription ON google_watch_channels(channel_id) WHERE surface='meet' AND channel_id <> '';

DROP INDEX IF EXISTS idx_google_watch_channels_channel_id_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_google_watch_channels_channel_id_unique
    ON google_watch_channels (channel_id)
    WHERE channel_id <> '' AND surface <> 'meet';
