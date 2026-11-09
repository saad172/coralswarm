-- Migration: 0026_query_history_created_at_index
-- Description: TENANT half of control's
--   `0115_query_history_created_at_index`. See that migration's header for
--   the full rationale — not repeated here to avoid the two copies drifting
--   out of sync in prose while the DDL itself is kept byte-equivalent.
--
-- PAIRED WITH: migrations_control/0115_query_history_created_at_index.sql
--   — identical index, same table shape (`query_history` is byte-equivalent
--   across both trees, `0001_tenant_baseline` / `0019_query_history`).
--
-- NUMBERING NOTE: claimed as the next free tenant number at write time
-- (0025 is claimed by open PR #468, `fix-journal-events-supersession-
-- promote`). If #468 (or another PR) merges first and this number
-- collides, this file must be renumbered before merge — see
-- `scripts/check_migrations.sh`.
--
-- Down:
--   DROP INDEX IF EXISTS idx_query_history_created_at;

CREATE INDEX IF NOT EXISTS idx_query_history_created_at
    ON query_history (created_at)
    WHERE user_id IS NOT NULL;
