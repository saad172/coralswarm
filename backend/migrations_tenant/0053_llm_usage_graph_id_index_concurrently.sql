-- no-transaction
-- 0053_llm_usage_graph_id_index_concurrently.sql — paired half of
-- `migrations_control/0146_llm_usage_graph_id_index_concurrently.sql`. See
-- that migration's header for the full rationale (why this index is a plain,
-- non-partial btree on `graph_id`, why it is needed alongside PR2's three
-- `source_*` partial indexes, the EXPLAIN before/after evidence, and the
-- INVALID-index caveat if the build fails).
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only the header
-- prose differs) — `llm_usage` is a SHARED table, present in both trees and
-- NOT in `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its
-- schema-only `pg_dump` diff fails on any divergence here.
--
-- PAIRED WITH: backend/migrations_control/0146_llm_usage_graph_id_index_concurrently.sql
-- Depends on: 0001_tenant_baseline.sql (creates `llm_usage` on the tenant
--   plane, including `graph_id`).
-- Down: see control 0146.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_llm_usage_graph_id
    ON llm_usage (graph_id);
