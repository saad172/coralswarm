-- 0005_member_ocean_columns.sql — paired half of
-- `migrations_control/0090_member_ocean_columns.sql` (member-private-oceans
-- PR 1, docs/plans/member-private-oceans.md §2.1/§7). See that migration's
-- header for the full rationale: why `owner_user_id` carries no FK (`users`
-- is control-tree-only and does not exist in this database at all), why
-- `archived_at`/the new indexes all carry `archived_at IS NULL`, why
-- `graphs_kind_check` widens by exactly ONE value (`'member'`, not
-- `'channel'` — that is Stage 2), why `graphs_member_requires_owner` is
-- trivially satisfied by every row that exists today, why the `is_primary`
-- cardinality split into an account-wide index and a new per-owner index is a
-- no-op for existing data, and the flagged, NOT silently resolved, apparent
-- resolution of the one-member-ocean-per-owner drafting error (see control 0090)
-- "secondary member oceans allowed" decision.
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only comments/
-- header prose differ) — `graphs` is a SHARED table (present in both trees;
-- NOT in `check_schema_parity.sh`'s `CONTROL_TABLES` roster), so
-- `check_schema_parity.sh`'s schema-only `pg_dump` diff would fail on any
-- divergence here.
--
-- This migration is SCHEMA ONLY and deliberately INERT, same as its control
-- counterpart: no Rust in this repo reads or writes `owner_user_id`/
-- `archived_at`, or creates a `kind='member'` row, until PR 2 and later.
--
-- Live-tenant safety: same `SET LOCAL lock_timeout = '3s'` discipline as
-- 0083_consumer_rls (control tree) for the same reason — `graphs` is tiny
-- (one row per ocean; this tenant's live `testorg` database has exactly one
-- row today), so a brief lock is acceptable, but a migration must never be
-- able to block indefinitely behind a long-running transaction on a live
-- tenant. Plain `CREATE INDEX` (not `CONCURRENTLY`) for the same reason as
-- the control migration: `graphs`' size makes the extra `CONCURRENTLY`
-- ceremony (separate no-transaction migration file, INVALID-index cleanup
-- risk) overhead with no corresponding benefit here.
--
-- Depends on: 0001_tenant_baseline (graphs, graphs.is_primary,
--             graphs_kind_check, uq_graphs_one_primary_per_account,
--             uq_graphs_one_org_per_account — the baseline already carries the
--             post-0026 shape of `graphs`, so there is no separate
--             tenant-tree pairing needed for that migration).
--
-- Down:
--   DROP INDEX IF EXISTS graphs_archived_at_idx;
--   DROP INDEX IF EXISTS uq_graphs_one_primary_per_owner;
--   DROP INDEX IF EXISTS uq_graphs_one_primary_account_wide;
--   CREATE UNIQUE INDEX IF NOT EXISTS uq_graphs_one_primary_per_account
--       ON graphs (account_id) WHERE is_primary;
--   ALTER TABLE graphs DROP CONSTRAINT IF EXISTS graphs_member_requires_owner;
--   ALTER TABLE graphs DROP CONSTRAINT IF EXISTS graphs_kind_check;
--   ALTER TABLE graphs
--       ADD CONSTRAINT graphs_kind_check CHECK (kind IN ('personal', 'team', 'org'));
--   ALTER TABLE graphs DROP COLUMN IF EXISTS archived_at;
--   ALTER TABLE graphs DROP COLUMN IF EXISTS owner_user_id;

SET LOCAL lock_timeout = '3s';

-- ── Columns ──────────────────────────────────────────────────────────────────
ALTER TABLE graphs ADD COLUMN IF NOT EXISTS owner_user_id UUID;   -- bare uuid, no FK
ALTER TABLE graphs ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;

-- ── kind CHECK: widen to include 'member' only ──────────────────────────────
ALTER TABLE graphs DROP CONSTRAINT IF EXISTS graphs_kind_check;
ALTER TABLE graphs ADD CONSTRAINT graphs_kind_check
  CHECK (kind IN ('personal','team','org','member'));

-- ── Owner-scoping invariant ──────────────────────────────────────────────────
ALTER TABLE graphs ADD CONSTRAINT graphs_member_requires_owner
  CHECK ((kind = 'member') = (owner_user_id IS NOT NULL));

-- ── Cardinality ──────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS uq_graphs_one_primary_per_account;
CREATE UNIQUE INDEX uq_graphs_one_primary_account_wide
  ON graphs (account_id) WHERE is_primary AND owner_user_id IS NULL AND archived_at IS NULL;
CREATE UNIQUE INDEX uq_graphs_one_primary_per_owner
  ON graphs (account_id, owner_user_id) WHERE is_primary AND owner_user_id IS NOT NULL AND archived_at IS NULL;
CREATE INDEX graphs_archived_at_idx ON graphs (archived_at) WHERE archived_at IS NOT NULL;
