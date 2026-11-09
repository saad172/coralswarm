-- Migration: 0028_journal_tier_rollups
-- Description: the TENANT half of control's `0117_journal_tier_rollups`. See
--   that migration's header for the full design rationale (why `hour_start`
--   is reused as every tier's boundary, why the unique constraint now
--   includes `granularity`, why `tz` is NULL only on hour rows) — not
--   repeated here to avoid the two copies drifting out of sync in prose
--   while the DDL itself is kept byte-equivalent, same convention as
--   0113/0024's own pairing.
--
-- PAIRED WITH: migrations_control/0117_journal_tier_rollups.sql — same ALTERs.
--
-- NUMBERING NOTE (historical): see 0117's header — #469 and #472 have both
-- since merged, taking 0115/0026 and 0116/0027 respectively; this pair's
-- 0117/0028 is now gapless and final.
--
-- Depends on: 0024_journal_rollups (the table this extends).
--
-- Down:
--   ALTER TABLE journal_rollups DROP CONSTRAINT IF EXISTS journal_rollups_tz_by_granularity;
--   ALTER TABLE journal_rollups DROP CONSTRAINT IF EXISTS journal_rollups_natural_key;
--   ALTER TABLE journal_rollups ADD CONSTRAINT journal_rollups_user_id_graph_id_hour_start_key UNIQUE (user_id, graph_id, hour_start);
--   DROP INDEX IF EXISTS journal_rollups_tier_read_idx;
--   ALTER TABLE journal_rollups DROP COLUMN IF EXISTS tz;
--   ALTER TABLE journal_rollups DROP COLUMN IF EXISTS period_start;
--   ALTER TABLE journal_rollups DROP COLUMN IF EXISTS granularity;

ALTER TABLE journal_rollups
    ADD COLUMN granularity  TEXT NOT NULL DEFAULT 'hour'
        CHECK (granularity IN ('hour', 'day', 'week', 'month')),
    ADD COLUMN period_start TIMESTAMPTZ,
    ADD COLUMN tz           TEXT;

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

-- See control-0117's header for why this indexes `(granularity,
-- period_start)` rather than the read-path shape (CodeRabbit round 1).
CREATE INDEX IF NOT EXISTS journal_rollups_tier_read_idx
    ON journal_rollups (granularity, period_start);
