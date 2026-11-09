-- Migration: 0117_gmail_everything_mode_high_water (tenant twin of control 0279)
-- Description: Gmail everything mode high-water mark for an org member's
--   mailbox — see `migrations_control/0279_gmail_everything_mode_high_water.sql`'s
--   header for what the columns mean and why. Same columns.
--
-- Depends on: 0074_gmail_everything_mode.
-- Down:
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS page_failures;
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS last_full_walk_at;
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS walk_after;
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS walk_started_at;
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS high_water_at;

ALTER TABLE gmail_everything_mode
    ADD COLUMN IF NOT EXISTS high_water_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS walk_started_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS walk_after        TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_full_walk_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS page_failures     INTEGER NOT NULL DEFAULT 0;
