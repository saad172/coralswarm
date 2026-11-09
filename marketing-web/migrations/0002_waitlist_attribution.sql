-- Migration: add first-touch acquisition attribution + country to waitlist_signups
--
-- All columns are nullable TEXT so existing rows (and any signup that omits
-- them, e.g. a client with JS/localStorage disabled) remain valid. Additive
-- only — no backfill, no NOT NULL, no rename of existing columns.
--
-- Column notes:
--   utm_source / utm_medium / utm_campaign — standard UTM params, taken
--     verbatim from the landing URL's query string on first visit.
--   referrer     — document.referrer at first visit, captured client-side;
--     null when there was no referrer or it was same-origin (see
--     components/firstTouch.ts).
--   landing_path — the path (no origin/query) the visitor first landed on.
--   country      — derived server-side from the CF-IPCountry header at
--     signup time (not client-supplied, not an IP address).
--   first_seen_at — client-side timestamp (ISO 8601) of the first page view
--     that established the stashed first-touch record. Distinct from
--     created_at, which is when the signup row itself was written.
--
-- IF NOT EXISTS-style guards aren't available for ALTER TABLE ADD COLUMN in
-- SQLite/D1, so this migration (like D1 migrations generally) is meant to be
-- applied exactly once per environment via `wrangler d1 migrations apply`,
-- which tracks applied migrations itself — mirroring 0001's approach of
-- relying on the migrations runner rather than embedding its own guards.

ALTER TABLE waitlist_signups ADD COLUMN utm_source TEXT;
ALTER TABLE waitlist_signups ADD COLUMN utm_medium TEXT;
ALTER TABLE waitlist_signups ADD COLUMN utm_campaign TEXT;
ALTER TABLE waitlist_signups ADD COLUMN referrer TEXT;
ALTER TABLE waitlist_signups ADD COLUMN landing_path TEXT;
ALTER TABLE waitlist_signups ADD COLUMN country TEXT;
ALTER TABLE waitlist_signups ADD COLUMN first_seen_at TEXT;

-- utm_source is the field the admin dashboard will group/filter by first
-- ("which channel is driving signups") — index it. Skipping an index on
-- country: with waitlist-scale row counts (low thousands, not millions) a
-- full scan for a coarse ~250-value column is cheap, and D1/SQLite gains
-- little from indexing a low-cardinality column that's typically used for
-- rollups (GROUP BY over the whole table) rather than point lookups.
CREATE INDEX IF NOT EXISTS idx_waitlist_signups_utm_source ON waitlist_signups (utm_source);
