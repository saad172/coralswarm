-- Migration: 0117_journal_tier_rollups
-- Description: extends `journal_rollups` (migration `0113_journal_rollups`)
--   with the day/week/month summary tiers — Today-journal ticket 15, built
--   atop the resolved architecture in
--   `.scratch/today-zoomable-journal/issues/04-grilling-rollup-architecture.md`
--   (read that file's Answer for the full rationale; not repeated here beyond
--   what the DDL itself needs).
--
-- NUMBERING NOTE (historical): this branch was originally built against a
-- main whose highest pair was control-0114/tenant-0025, contested at the
-- time by #469 and #472. Both #469 and #472 have since merged to main
-- (control-0115/tenant-0026 and control-0116/tenant-0027 respectively, in
-- that order — two separate merge commits in this PR's history), so this
-- pair's own control-0117/tenant-0028 numbers are now gapless and final;
-- `scripts/check_migrations.sh` passes clean with no allowlisted gap.
--
-- PAIRED WITH: migrations_tenant/0028_journal_tier_rollups.sql — same ALTERs
-- (`scripts/check_schema_parity.sh`), same convention as 0113/0024's own
-- pairing.
--
-- Depends on: 0113_journal_rollups (the table this extends).
--
-- Down:
--   ALTER TABLE journal_rollups DROP CONSTRAINT IF EXISTS journal_rollups_tz_by_granularity;
--   ALTER TABLE journal_rollups DROP CONSTRAINT IF EXISTS journal_rollups_natural_key;
--   ALTER TABLE journal_rollups ADD CONSTRAINT journal_rollups_user_id_graph_id_hour_start_key UNIQUE (user_id, graph_id, hour_start);
--   DROP INDEX IF EXISTS journal_rollups_tier_read_idx;
--   ALTER TABLE journal_rollups DROP COLUMN IF EXISTS tz;
--   ALTER TABLE journal_rollups DROP COLUMN IF EXISTS period_start;
--   ALTER TABLE journal_rollups DROP COLUMN IF EXISTS granularity;
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY REUSE `hour_start` AS THE GENERAL TIER BOUNDARY, NOT JUST `period_start`
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Ticket 15 asks for `period_start` as "the tier boundary; equals hour_start
-- for hour rows." Rather than leave `hour_start` meaningless for day/week/
-- month rows (or make it nullable, weakening its existing NOT NULL contract
-- for the hour tier this table was built for), every row — regardless of
-- granularity — stores its OWN tier boundary in BOTH `hour_start` (the
-- original, byte-compatible column every existing hour-tier query already
-- reads) AND the new `period_start` (a same-valued, tier-agnostic alias new
-- code can read without caring whether a row happens to be an hour row).
-- `PostgresStore::upsert_journal_rollup` (the existing, UNCHANGED hour-compile
-- write path) keeps writing `hour_start` exactly as before; the new
-- day/week/month write path sets both columns to the same instant. This is
-- the cheapest way to satisfy "keep hour behavior byte-compatible" literally
-- (zero changes to any existing hour-tier query or index) while giving every
-- row a always-populated, tier-agnostic column to sort/filter on generically.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY THE UNIQUE CONSTRAINT NOW INCLUDES `granularity`
-- ─────────────────────────────────────────────────────────────────────────────
--
-- The old `UNIQUE (user_id, graph_id, hour_start)` is not safe once
-- `hour_start` is reused as every tier's boundary: a whole-hour-offset IANA
-- zone's local midnight always lands exactly on a UTC hour boundary, so a
-- day row's `hour_start` CAN legitimately collide with an actual hour row's
-- `hour_start` for the same (user, graph) — e.g. America/Chicago's local
-- midnight is always `HH:00:00Z` for some HH. Replacing the natural key with
-- `UNIQUE (user_id, graph_id, granularity, hour_start)` removes that
-- collision risk while leaving every EXISTING hour row's uniqueness
-- unaffected (`granularity` defaults to `'hour'` for all of them, so the old
-- 3-column key's behavior is exactly reproduced by the new 4-column one).
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `tz` IS NULL ON HOUR ROWS AND REQUIRED ON EVERYTHING ELSE
-- ─────────────────────────────────────────────────────────────────────────────
--
-- An hour row's boundary is always a plain UTC hour truncation — no IANA zone
-- is ever consulted to produce it (migration 0113's own header). A day/week/
-- month row's boundary, by contrast, is inherently tz-dependent (local
-- midnight, Monday-start weeks, local calendar months — see the grilling
-- ticket's Answer, decision 2). Storing `tz` only where it was actually used
-- to compute the boundary — rather than a constant column value everywhere,
-- or omitting it and letting a future reader guess which default was in
-- effect at compile time — makes every row self-describing and lets a future
-- per-user timezone (today there is no such setting anywhere in this
-- codebase; see `workers::journal_sweep`'s tier-boundary module doc for that
-- decision) show up as a per-row fact without a migration.
--
-- `input_fingerprint` is reused unchanged for every tier's chained fingerprint
-- (md5 over the ordered child `period_key‖fingerprint` pairs + child count —
-- see `store::journal_tier_fingerprint`); no new fingerprint column is added.

ALTER TABLE journal_rollups
    ADD COLUMN granularity  TEXT NOT NULL DEFAULT 'hour'
        CHECK (granularity IN ('hour', 'day', 'week', 'month')),
    ADD COLUMN period_start TIMESTAMPTZ,
    ADD COLUMN tz           TEXT;

-- Backfill every existing (hour-only, pre-this-migration) row: its tier
-- boundary is exactly its own `hour_start`, by definition.
UPDATE journal_rollups SET period_start = hour_start WHERE period_start IS NULL;

ALTER TABLE journal_rollups ALTER COLUMN period_start SET NOT NULL;

ALTER TABLE journal_rollups
    ADD CONSTRAINT journal_rollups_tz_by_granularity
        CHECK (
            (granularity = 'hour' AND tz IS NULL)
            OR (granularity <> 'hour' AND tz IS NOT NULL AND tz <> '')
        );

ALTER TABLE journal_rollups
    DROP CONSTRAINT IF EXISTS journal_rollups_user_id_graph_id_hour_start_key;
ALTER TABLE journal_rollups
    ADD CONSTRAINT journal_rollups_natural_key
        UNIQUE (user_id, graph_id, granularity, hour_start);

-- Tier-window discovery scans (`Store::journal_tier_window_groups`/
-- `_scoped`, `workers::journal_sweep::sweep_tier`'s ONE-query-per-tier-
-- per-pass window fetch): `WHERE granularity = $1 AND period_start >= $2
-- AND period_start < $3` (unscoped), plus an `account_id = $` residual
-- predicate on the scoped sibling. CodeRabbit round 1 caught that the
-- original cut of this index led with `account_id, user_id, ... hour_start`
-- — a shape `Store::list_journal_rollups` (the tier READ path) never
-- actually filters on (it binds `user_id`/`graph_id`/`granularity`/
-- `hour_start`, already served by `journal_rollups_natural_key` above,
-- confirmed a B-tree match by that unique index's own column order) and
-- that additionally used `hour_start`, not `period_start` — the column the
-- window-discovery scans actually filter. Reshaped to the predicate real
-- queries issue instead of dropping it outright, since the window scan is
-- unscoped-per-org / scoped-per-account and runs every sweep pass.
CREATE INDEX IF NOT EXISTS journal_rollups_tier_read_idx
    ON journal_rollups (granularity, period_start);
