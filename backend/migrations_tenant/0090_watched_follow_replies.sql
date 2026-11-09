-- Migration 0090: `follow_replies` on the two opt-in watched-thread tables
--   — tenant twin of `migrations_control/0222_watched_follow_replies.sql`
--   (gmail-options backend train, PR 1 of 4). See that migration's header
--   for the full column-by-column rationale (byte-identical here — no
--   tenant-tree structural differences apply, both columns are plain
--   booleans with no cross-DB reference).
--
-- Depends on: 0066_google_gmail_watch_owner_and_threads.sql, 0001_tenant_baseline.sql.
-- Down:
--   ALTER TABLE gmail_watched_threads DROP COLUMN IF EXISTS follow_replies;
--   ALTER TABLE outlook_watched_conversations DROP COLUMN IF EXISTS follow_replies;

ALTER TABLE gmail_watched_threads
    ADD COLUMN IF NOT EXISTS follow_replies BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE outlook_watched_conversations
    ADD COLUMN IF NOT EXISTS follow_replies BOOLEAN NOT NULL DEFAULT TRUE;
