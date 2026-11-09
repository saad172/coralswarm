-- Migration: capture the v3 contact form
--
-- The v3 marketing site replaced the email-only waitlist box with a
-- form that also carries a name, a scope (personal | enterprise) and an
-- optional free-text message, and promises in copy that it "lands in my
-- inbox at the same time" with "a reply within one working day".
--
-- Two shapes are needed, not one:
--
--   waitlist_signups     — still one row per person. Deduped on email
--                          (ON CONFLICT DO NOTHING), so it answers
--                          "who is on the list".
--   contact_submissions  — APPEND-ONLY, one row per submission. It
--                          answers "what did they say, and when".
--
-- Why both: the waitlist table's ON CONFLICT DO NOTHING silently
-- discards everything about a repeat submission. Under the old
-- email-only form that was correct — a duplicate carried no new
-- information. With a message field it is a data-loss bug aimed
-- squarely at the best lead on the page: an enterprise evaluator who
-- already joined the waitlist months ago, comes back, writes a
-- paragraph about their team, and is shown "I'll write back within one
-- working day" while their message is dropped on the floor. Every
-- submission lands in contact_submissions regardless.

-- ---------------------------------------------------------------
-- 1. Additive columns on the existing table.
-- ---------------------------------------------------------------
-- Nullable so every existing row stays valid; no backfill. These hold
-- FIRST-touch values, matching how the attribution columns in 0002
-- behave — a repeat submission does not overwrite them.
--
-- SQLite/D1 has no ADD COLUMN IF NOT EXISTS, so (like 0002) this
-- migration relies on `wrangler d1 migrations apply` tracking what has
-- already run rather than embedding its own guards.

ALTER TABLE waitlist_signups ADD COLUMN name TEXT;
ALTER TABLE waitlist_signups ADD COLUMN scope TEXT;
ALTER TABLE waitlist_signups ADD COLUMN message TEXT;

-- ---------------------------------------------------------------
-- 2. The append-only submission log.
-- ---------------------------------------------------------------
-- No UNIQUE on email, by design: the whole point is that resubmitting
-- records a new row. `country` is derived server-side from Cloudflare's
-- CF-IPCountry header, never from the request body, and no IP address
-- is stored.

CREATE TABLE IF NOT EXISTS contact_submissions (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  email         TEXT NOT NULL,
  name          TEXT,
  -- 'personal' | 'enterprise'. CHECK rather than a lookup table: the
  -- set is closed, tiny, and mirrors a two-button toggle in the UI.
  scope         TEXT NOT NULL CHECK (scope IN ('personal', 'enterprise')),
  message       TEXT,
  utm_source    TEXT,
  utm_medium    TEXT,
  utm_campaign  TEXT,
  referrer      TEXT,
  landing_path  TEXT,
  country       TEXT,
  first_seen_at TEXT,
  created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- The two reads this table actually gets: "what came in recently"
-- (the daily triage) and "everything from this person" (before
-- replying). Both are point/range lookups, so both are worth an index.
CREATE INDEX IF NOT EXISTS idx_contact_submissions_created_at
  ON contact_submissions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_contact_submissions_email
  ON contact_submissions (email);
