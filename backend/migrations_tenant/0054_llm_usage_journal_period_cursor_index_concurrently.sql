-- no-transaction
-- 0054_llm_usage_journal_period_cursor_index_concurrently.sql — paired half
-- of `migrations_control/0147_llm_usage_journal_period_cursor_index_concurrently.sql`.
-- See that migration's header for the full rationale (why this THIRD index
-- was added only after `EXPLAIN` showed the merged PR2/#605 `(graph_id,
-- source_journal_granularity, source_journal_period)` index cannot serve a
-- CROSS-graph cursor over `source_journal_period`, the `(source_journal_period,
-- id)` column order, the EXPLAIN before/after evidence, and the
-- INVALID-index caveat if the build fails).
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only the header
-- prose differs) — `llm_usage` is a SHARED table, present in both trees and
-- NOT in `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its
-- schema-only `pg_dump` diff fails on any divergence here.
--
-- PAIRED WITH: backend/migrations_control/0147_llm_usage_journal_period_cursor_index_concurrently.sql
-- Depends on: 0048_llm_usage_provider_cost.sql (creates
--   `source_journal_period` on the tenant plane);
--   0051_llm_usage_source_journal_period_index_concurrently.sql (the sibling
--   per-graph index this one deliberately complements rather than replaces).
-- Down: see control 0147.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_llm_usage_journal_period_cursor
    ON llm_usage (source_journal_period, id)
    WHERE source_journal_period IS NOT NULL;
