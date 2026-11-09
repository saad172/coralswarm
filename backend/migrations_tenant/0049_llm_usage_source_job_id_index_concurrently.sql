-- no-transaction
-- 0049_llm_usage_source_job_id_index_concurrently.sql — paired half of
-- `migrations_control/0142_llm_usage_source_job_id_index_concurrently.sql`.
-- See that migration's header for why `CREATE INDEX CONCURRENTLY` must be
-- alone in a `-- no-transaction` file, why this index is partial, and the
-- INVALID-index caveat if the build fails.
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only the header
-- prose differs) — `llm_usage` is a SHARED table, present in both trees and
-- NOT in `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its
-- schema-only `pg_dump` diff fails on any divergence here.
--
-- PAIRED WITH: backend/migrations_control/0142_llm_usage_source_job_id_index_concurrently.sql
-- Depends on: 0048_llm_usage_provider_cost.sql (creates source_job_id).
-- Down: see control 0142.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_llm_usage_source_job_id
    ON llm_usage (source_job_id)
    WHERE source_job_id IS NOT NULL;
