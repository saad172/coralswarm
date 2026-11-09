-- Migration: first-party, cookieless pageview log
--
-- PostHog is on hold and Cloudflare Web Analytics (the beacon in
-- app/layout.tsx) can't break traffic down by UTM param — it only reports
-- aggregate site-wide numbers. This table exists to answer one question the
-- founder actually needs answered: "which channel/campaign is driving
-- visits", not just "who filled out the form" (contact_submissions already
-- answers that, but only for the small fraction of visitors who convert).
--
-- Privacy, by construction — no IP address, no user identifier, and no
-- cookie is stored anywhere in this table or set on the visitor at all:
--
--   path            — pathname only (no query string, no fragment), from
--                      window.location.pathname at ping time.
--   utm_source/medium/campaign — standard UTM params read from the CURRENT
--                      landing URL's query string at ping time. Distinct
--                      from waitlist_signups/contact_submissions' UTM
--                      columns, which record *first*-touch (read once, at
--                      signup, from a localStorage stash) — this table
--                      records *this visit's* attribution, which is the
--                      whole point of a channel-level pageview log.
--   referrer        — document.referrer reduced to origin+pathname only (no
--                      query string, no fragment — see firstTouch.ts's
--                      sanitizeReferrer, shared verbatim by the ping
--                      component), or null when absent/same-origin.
--   country         — derived server-side from the CF-IPCountry header at
--                      request time (never client-supplied, never an IP
--                      address — see worker/index.ts's sanitizeCountry).
--   is_first_visit  — 0/1, whether this browser's first-touch localStorage
--                      stash existed BEFORE this page load. Not a durable
--                      per-visitor identity: it degrades to "0" the moment
--                      localStorage is cleared, a different device is used,
--                      or storage is unavailable, and there is nothing in
--                      this table that could re-identify the same visitor
--                      across two pings either way.
--
-- No email, no name, no session token, no fingerprint, nothing that could
-- link a row here back to a specific person or device across requests.

CREATE TABLE IF NOT EXISTS page_views (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at     TEXT NOT NULL DEFAULT (datetime('now')),
  path           TEXT,
  utm_source     TEXT,
  utm_medium     TEXT,
  utm_campaign   TEXT,
  referrer       TEXT,
  country        TEXT,
  is_first_visit INTEGER NOT NULL DEFAULT 0 CHECK (is_first_visit IN (0, 1))
);

-- Mirrors 0002/0003's indexing rationale: utm_source is the column the
-- "visits by source" rollup groups on first, and created_at is what a
-- day/week range query filters on — both worth an index. country is
-- skipped for the same reason 0002 skips it: low-cardinality, rollup-only,
-- cheap to full-scan at this table's expected row count.
CREATE INDEX IF NOT EXISTS idx_page_views_utm_source ON page_views (utm_source);
CREATE INDEX IF NOT EXISTS idx_page_views_created_at ON page_views (created_at DESC);
