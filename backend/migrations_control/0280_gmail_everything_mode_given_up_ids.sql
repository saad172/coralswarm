-- Migration: 0280_gmail_everything_mode_given_up_ids
-- Description: Gmail everything mode — remember the messages the walk skips,
--   so a later walk does not stall on them again, and track WHICH message a
--   page keeps failing on.
--
--   ── Why ──
--   Migration 0279's walk gives up on a message that keeps failing (after
--   a few failed ticks on its page) and moves on. But the daily FULL walk
--   (no `after:` floor) lists that message again, fails on it again, and
--   spends another ~1.5–3 hours of failed ticks before giving up again —
--   holding up new mail for that mailbox every day.
--
--   `unreadable_ids`: Gmail message ids that can never be ingested: the
--   body does not parse, or has nothing ingestable once normalized. A Gmail
--   message's content never changes, so the walk never fetches them again.
--   A body-parser regression that wrongly empties messages needs this
--   column cleared by hand to recover them.
--
--   `given_up_ids`: ids the walk gave up on after they kept failing (a
--   Gmail 5xx on that one message, or our own ingest failing), or that
--   ingest rejected as invalid (422: a validation rule we wrote, which a bad
--   deploy can get wrong). That may
--   clear up (an ingest outage ends), so a bounded walk skips them but each
--   FULL walk tries each once more: stored → dropped from the list; still
--   failing → kept, and the walk moves on without stalling.
--
--   Both are bounded by the worker (most recent 5,000 unreadable and 1,000
--   given-up ids kept). One row
--   per mailbox already, so columns rather than a new table (no new RLS
--   surface). Re-enabling into a DIFFERENT ocean clears them, like the rest
--   of the walk state.
--
--   `page_failing_id`: the message the consecutive failed ticks counted by
--   `page_failures` failed on. The walk gives up only on THAT message, so a
--   message that fails once right after another recovers is never given up
--   on with someone else's count.
--
--   `walk_skip_ids`: ids the walk IN PROGRESS already fetched and will not
--   fetch again: gone (404) or hidden (Trash/Spam) by the time of the get,
--   or a given-up message still failing on its full-walk retry. Without it,
--   a tick that pauses at its get budget re-fetches them on the next tick
--   and can spend every tick's budget on them. Cleared when the walk ends;
--   a hidden message restored later is listed and fetched by a later walk.
--
--   No backfill: existing rows start empty.
--
--   Tenant twin: `migrations_tenant/0118_gmail_everything_mode_given_up_ids.sql`.
--
-- Depends on: 0279_gmail_everything_mode_high_water.
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
