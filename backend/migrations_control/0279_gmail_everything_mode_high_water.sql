-- Migration: 0279_gmail_everything_mode_high_water
-- Description: Gmail everything mode — a high-water mark, so a walk after
--   the first one lists only recent mail instead of the whole mailbox.
--
--   ── Why ──
--   Until now a completed walk cleared `resume_cursor` and the next tick
--   walked the whole mailbox again from the newest message, calling
--   `messages.get` (5 Gmail quota units) on every id. In prod (2026-09-24
--   onward) that blew Gmail's per-user 'Total Query Cost' quota partway
--   through every tick, the tick kept none of its progress, and personal
--   Gmail corals went stale. The worker now skips ids that already have
--   corals and lists only mail after the last completed walk's start.
--
--   `walk_started_at`: when the walk now in progress started (NULL = no
--   walk in progress, unless `resume_cursor` is set by a walk begun before
--   this migration, whose start is unknown). `high_water_at`: when the last
--   COMPLETED walk started — a walk completing copies its `walk_started_at`
--   here, and the next walk lists `after:` it (minus a day of slack).
--
--   `walk_after`: the `after:` floor of the walk in progress (NULL = a
--   full walk; the pageToken is only valid for the query it came from, so
--   a resumed walk must reuse it). `last_full_walk_at`: start of the last
--   completed FULL walk. Mail can join the listing with an old date (moved
--   out of Spam days later, restored from Trash, imported), below any
--   `after:` floor, so a full walk still runs once a day. It costs list
--   calls only. `page_failures`: consecutive ticks that failed on the page
--   at `resume_cursor`. After a few, that page's failing messages are
--   skipped and named on `last_error`, so one bad message cannot pin the
--   walk forever. Advancing a page resets it.
--
--   No backfill: existing rows start with all NULL (0), so each owner's next
--   walk lists the whole mailbox once. That walk fetches only the ids with
--   no corals yet (the backlog since 2026-09-24), then sets the mark.
--
--   Tenant twin: `migrations_tenant/0117_gmail_everything_mode_high_water.sql`.
--
-- Depends on: 0197_gmail_everything_mode.
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
