-- PAIRED WITH: migrations_control/0361_jobs_hold_streak.sql (byte-identical
-- body; `jobs` is a shared table, see scripts/check_schema_parity.sh).
--
-- `jobs.hold_streak`: how many managed holds in a row this job has taken
-- (COR-114). A held job now goes back to `pending` in place
-- (`jobs::queue::hold`) instead of completing and enqueueing a fresh row,
-- and its streak sets the delay: a hold that can clear backs off to a cap,
-- a hold that cannot clear dead-letters after a few tries. It was a fixed
-- 15 minutes forever, with a new row each time: `RegenerateDigest` alone
-- wrote about 43,000 `done` rows a day for one personal-org account whose
-- every digest was refused `requester_access_lost` (prod, 2026-10-21..24).
--
-- A constant default is metadata-only on PostgreSQL 11+: no table rewrite,
-- and `lock_timeout` bounds the brief ACCESS EXCLUSIVE lock.
--
-- Down:
--   ALTER TABLE jobs DROP COLUMN IF EXISTS hold_streak;
SET LOCAL lock_timeout = '3s';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS hold_streak INTEGER NOT NULL DEFAULT 0;
