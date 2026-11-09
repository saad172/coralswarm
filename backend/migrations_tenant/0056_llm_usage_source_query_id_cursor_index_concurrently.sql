-- no-transaction
-- Migration: 0056_llm_usage_source_query_id_cursor_index_concurrently
-- Description: paired copy of
--   migrations_control/0152_llm_usage_source_query_id_cursor_index_concurrently.sql
--   — see that file's header for the full rationale. `llm_usage` is a SHARED
--   table (present in both trees, NOT in `CONTROL_TABLES`), so the DDL below
--   must stay byte-for-byte identical to the control file's.
--
-- PAIRED WITH: backend/migrations_control/0152_llm_usage_source_query_id_cursor_index_concurrently.sql
-- Depends on: 0048_llm_usage_provider_cost.sql (creates `source_query_id`);
--   0050_llm_usage_source_query_id_index_concurrently.sql.
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_llm_usage_source_query_id_cursor;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_llm_usage_source_query_id_cursor
    ON llm_usage (created_at, id)
    WHERE source_query_id IS NOT NULL;
