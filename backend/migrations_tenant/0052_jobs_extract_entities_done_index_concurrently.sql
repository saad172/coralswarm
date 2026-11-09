-- no-transaction
-- 0052_jobs_extract_entities_done_index_concurrently.sql — paired half of
-- `migrations_control/0145_jobs_extract_entities_done_index_concurrently.sql`.
-- See that migration's header for the full rationale (why `CREATE INDEX
-- CONCURRENTLY` must be alone in a `-- no-transaction` file, why the index is
-- partial on `status='done' AND kind='ExtractEntities'`, why `(finished_at,
-- id)` is the column order, the EXPLAIN before/after evidence, and the
-- INVALID-index caveat if the build fails).
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only the header
-- prose differs) — `jobs` is a SHARED tenant-tree table, present in both
-- trees and NOT in `scripts/check_schema_parity.sh`'s `CONTROL_TABLES`
-- roster, so its schema-only `pg_dump` diff fails on any divergence here.
--
-- PAIRED WITH: backend/migrations_control/0145_jobs_extract_entities_done_index_concurrently.sql
-- Depends on: 0001_tenant_baseline.sql (creates `jobs` on the tenant plane,
--   including `finished_at`, `status`, `kind`, and the `job_status` enum).
-- Down: see control 0145.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_extract_entities_done
    ON jobs (finished_at, id)
    WHERE status = 'done' AND kind = 'ExtractEntities';
