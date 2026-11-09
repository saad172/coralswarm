-- no-transaction
-- Migration: 0143_llm_usage_source_query_id_index_concurrently
-- Description: Build the partial index backing PR9's per-query cost lookup
--   (`SELECT ... FROM llm_usage WHERE source_query_id = $1`, the request-path
--   debit twin of `0142`'s per-job lookup) WITHOUT blocking writes on
--   `llm_usage` while it builds. Same split-file / `CONCURRENTLY` /
--   `-- no-transaction` mechanics as `0142`; see that file's header (and its
--   own pointer to `0059`/`0074`) for the full rationale, including why a
--   plain `CREATE INDEX` would still take a blocking `SHARE` lock here even
--   though the column is 100% NULL at build time, and why this could not be
--   combined with `0142` into one file (two `CONCURRENTLY` statements in one
--   migration file fails — confirmed against CI, see 0074's header).
--
--   Partial (`WHERE source_query_id IS NOT NULL`): NULL on every row until
--   PR4 starts populating it on request-path (synthesis/ask) calls; worker
--   rows carry `source_job_id` instead and are never matched by this index's
--   predicate.
--
--   `IF NOT EXISTS` + `CONCURRENTLY`: same INVALID-index-on-failed-build
--   caveat as `0142` — if this migration errors, drop
--   `idx_llm_usage_source_query_id` before re-running.
--
-- PAIRED WITH: backend/migrations_tenant/0050_llm_usage_source_query_id_index_concurrently.sql
-- Depends on: 0141_llm_usage_provider_cost.sql (creates source_query_id).
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_llm_usage_source_query_id;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_llm_usage_source_query_id
    ON llm_usage (source_query_id)
    WHERE source_query_id IS NOT NULL;
