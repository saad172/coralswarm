-- Migration 0079: job_progress (tenant twin of
--   migrations_control/0209_job_progress.sql — async-promote PR, same
--   schema-parity twin pattern as every other `jobs`-table migration in
--   this tree, e.g. 0004_jobs_account_id / 0041_job_outcomes).
--
--   Byte-identical column set and constraints; see the control migration's
--   header for the full rationale (pollable progress for `PromoteNode` jobs
--   — a tenant-plane promote, member ocean -> team/org ocean within the
--   same tenant database, is the MORE common case this feature exists for,
--   not an edge case: `cross_database_promotion_denied` means every
--   promotion into an org's shared ocean either originates on this plane
--   already or is refused outright).
--
-- Depends on: 0004_jobs_account_id (tenant's own `jobs` baseline).
-- Down:
--   ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_progress_current_nonnegative;
--   ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_progress_total_nonnegative;
--   ALTER TABLE jobs DROP COLUMN IF EXISTS progress_current;
--   ALTER TABLE jobs DROP COLUMN IF EXISTS progress_total;

SET LOCAL lock_timeout = '3s';

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS progress_current INTEGER;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS progress_total INTEGER;

ALTER TABLE jobs ADD CONSTRAINT jobs_progress_current_nonnegative
    CHECK (progress_current IS NULL OR progress_current >= 0);
ALTER TABLE jobs ADD CONSTRAINT jobs_progress_total_nonnegative
    CHECK (progress_total IS NULL OR progress_total >= 0);
