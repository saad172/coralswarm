-- Migration: 0118_gmail_everything_mode_given_up_ids (tenant twin of control 0280)
-- Description: Gmail everything mode — messages an org member's mailbox walk
--   skips, the message a page keeps failing on, and what the walk in
--   progress already fetched. See
--   `migrations_control/0280_gmail_everything_mode_given_up_ids.sql`'s header
--   for what the columns mean and why. Same columns.
--
-- Depends on: 0117_gmail_everything_mode_high_water.
-- Down:
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS walk_skip_ids;
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS page_failing_id;
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS given_up_ids;
--   ALTER TABLE gmail_everything_mode DROP COLUMN IF EXISTS unreadable_ids;

ALTER TABLE gmail_everything_mode
    ADD COLUMN IF NOT EXISTS unreadable_ids  TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS given_up_ids    TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS page_failing_id TEXT,
    ADD COLUMN IF NOT EXISTS walk_skip_ids   TEXT[] NOT NULL DEFAULT '{}';
