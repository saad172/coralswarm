-- Migration 0125: google_meet_shared_subscription (tenant twin of
--   migrations_control/0290_google_meet_shared_subscription.sql).
--
--   0063's all-surface unique index on `channel_id` now excludes Meet rows,
--   byte-identical to the control tree so `scripts/check_schema_parity.sh`
--   keeps agreeing. One Google user connected in several accounts shares one
--   Meet subscription name; see the control migration's header. The control
--   only routing index is not twinned (same call as 0286/0123).
--
-- Depends on: 0063_google_calendar_sync_state.sql.
-- Down:
--   DROP INDEX IF EXISTS idx_google_watch_channels_channel_id_unique;
--   CREATE UNIQUE INDEX idx_google_watch_channels_channel_id_unique ON google_watch_channels (channel_id) WHERE channel_id <> '';

DROP INDEX IF EXISTS idx_google_watch_channels_channel_id_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_google_watch_channels_channel_id_unique
    ON google_watch_channels (channel_id)
    WHERE channel_id <> '' AND surface <> 'meet';
