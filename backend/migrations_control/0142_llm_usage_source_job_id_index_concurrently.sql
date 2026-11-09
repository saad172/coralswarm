-- no-transaction
-- Migration: 0142_llm_usage_source_job_id_index_concurrently
-- Description: Build the partial index backing PR9's per-job cost lookup
--   (`SELECT ... FROM llm_usage WHERE source_job_id = $1`, run on every ~5-min
--   derivation-worker tick) WITHOUT blocking writes on `llm_usage` while it
--   builds. Split out of `0141_llm_usage_provider_cost.sql` (which adds the
--   `source_job_id` column) for the same reason `0059` was split out of
--   `0058`, and the mechanics are unchanged from that precedent — see 0059's
--   header for the full explanation of why `CREATE INDEX CONCURRENTLY` must
--   be alone in a `-- no-transaction` file (sqlx sends a migration's SQL as
--   one wire-protocol Query message, and Postgres implicitly wraps multiple
--   statements in such a message in a transaction, which `CONCURRENTLY`
--   refuses to run inside — confirmed against CI by 0074's header, which also
--   documents that even pairing `CONCURRENTLY` with a preceding `DROP INDEX
--   CONCURRENTLY IF EXISTS` in the same file fails for the identical reason).
--
--   Partial (`WHERE source_job_id IS NOT NULL`): the column is NULL on every
--   row until PR4 starts populating it and stays NULL forever on pre-epoch
--   rows and on every request-path row (those carry `source_query_id`
--   instead) — indexing the NULL majority would waste space for a predicate
--   PR9 never issues.
--
--   `IF NOT EXISTS` + `CONCURRENTLY`: a failed concurrent build leaves an
--   INVALID index behind, and `IF NOT EXISTS` will then happily skip it
--   forever. If this migration errors, drop `idx_llm_usage_source_job_id`
--   before re-running rather than assuming the skip means success — the same
--   operational caveat every `CONCURRENTLY` migration in this tree carries
--   (0059/0074/0075/0099).
--
-- PAIRED WITH: backend/migrations_tenant/0049_llm_usage_source_job_id_index_concurrently.sql
-- Depends on: 0141_llm_usage_provider_cost.sql (creates source_job_id).
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_llm_usage_source_job_id;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_llm_usage_source_job_id
    ON llm_usage (source_job_id)
    WHERE source_job_id IS NOT NULL;
