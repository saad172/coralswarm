-- no-transaction
-- 0051_llm_usage_source_journal_period_index_concurrently.sql — paired half
-- of `migrations_control/0144_llm_usage_source_journal_period_index_concurrently.sql`.
-- See that migration's header for why `CREATE INDEX CONCURRENTLY` must be
-- alone in a `-- no-transaction` file, why this index is partial, the
-- `graph_id`-leading / granularity-before-period column ordering, and the
-- INVALID-index caveat if the build fails.
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only the header
-- prose differs) — `llm_usage` is a SHARED table, present in both trees and
-- NOT in `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its
-- schema-only `pg_dump` diff fails on any divergence here.
--
-- PAIRED WITH: backend/migrations_control/0144_llm_usage_source_journal_period_index_concurrently.sql
-- Depends on: 0048_llm_usage_provider_cost.sql (creates source_journal_period
--   and source_journal_granularity).
-- Down: see control 0144.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_llm_usage_source_journal_period
    ON llm_usage (graph_id, source_journal_granularity, source_journal_period)
    WHERE source_journal_period IS NOT NULL;
