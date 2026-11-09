-- Migration: 0115_query_history_created_at_index
-- Description: Support `Store::journal_dirty_hours`'s (org-tenant-plane,
--   UNSCOPED) `ask_inputs` CTE (PR #469, hourly-completeness audit gaps
--   #4/#5) — `WHERE user_id IS NOT NULL AND created_at >= $1 AND created_at
--   < $2`, run over the WHOLE lookback window on every sweep tick, with no
--   `account_id` predicate to lean on. The existing
--   `idx_query_history_account_created (account_id, created_at DESC)`
--   (`0019_query_history`) covers the SCOPED sibling
--   (`journal_dirty_hours_scoped`'s `account_id = $3` predicate) but cannot
--   serve this account-less scan. A partial index excluding NULL `user_id`
--   rows mirrors `idx_activity_events_graph_created`'s identical partial-
--   index shape (`0013_activity_feed`) for the same "unscoped background
--   scan, NULLs never matter" reason.
--
-- NUMBERING NOTE: claimed as the next free control number at write time
-- (0114 is claimed by open PR #468, `fix-journal-events-supersession-
-- promote`). If #468 (or another PR) merges first and this number
-- collides, this file must be renumbered before merge — see
-- `scripts/check_migrations.sh`.
--
-- Down:
--   DROP INDEX IF EXISTS idx_query_history_created_at;

CREATE INDEX IF NOT EXISTS idx_query_history_created_at
    ON query_history (created_at)
    WHERE user_id IS NOT NULL;
