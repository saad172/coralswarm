-- 0041_job_outcomes.sql — paired half of
-- `migrations_control/0134_job_outcomes.sql` (typed job-completion outcomes,
-- Track B step 1). See that migration's header for the full rationale and the
-- 2026-08-04 `ExtractRelationships` incident it exists to make impossible to
-- repeat. `jobs` is a SHARED tenant-tree table (0083's header: one of the 3
-- content tables deliberately left off the consumer-plane RLS roster —
-- `jobs`, `connector_identities`, `sync_cursors`), so this migration must
-- land byte-equivalent to the control-tree one for
-- `scripts/check_schema_parity.sh`'s `pg_dump --schema-only` diff to agree.
--
-- Depends on: 0001_tenant_baseline (creates `jobs`).
-- Down:
--   ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_outcome_check;
--   ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_failure_class_check;
--   ALTER TABLE jobs DROP COLUMN IF EXISTS outcome;
--   ALTER TABLE jobs DROP COLUMN IF EXISTS failure_class;

SET LOCAL lock_timeout = '3s';

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS outcome TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS failure_class TEXT;

ALTER TABLE jobs ADD CONSTRAINT jobs_outcome_check
    CHECK (outcome IS NULL OR outcome IN ('did_work', 'nothing_to_do'));

ALTER TABLE jobs ADD CONSTRAINT jobs_failure_class_check
    CHECK (failure_class IS NULL OR failure_class IN ('transient', 'structural'));
