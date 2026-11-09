-- Migration: 0004_jobs_account_id (tenant tree)
-- Description: paired copy of migrations_control/0085_jobs_account_id.sql —
--              `jobs` is a shared tenant-tree table (it is NOT in
--              `check_schema_parity.sh`'s CONTROL_TABLES roster), so the
--              org-tenancy Phase W.1 column must land in BOTH trees or new
--              tenant DBs silently diverge from the consumer plane (the exact
--              pairing gap PR #284 hit). See the control migration's header
--              for the full rationale (nullable on purpose; deliberately no
--              FK to `accounts`, which does not exist in this tree at all).
--
-- Down:
--   ALTER TABLE jobs DROP COLUMN IF EXISTS account_id;

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS account_id UUID;
