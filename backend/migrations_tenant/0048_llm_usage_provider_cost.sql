-- Migration: 0048_llm_usage_provider_cost — tenant twin of
--   migrations_control/0141_llm_usage_provider_cost.sql. See that file's
--   header for the full rationale (why these six columns exist — the original
--   `provider`/`cost_usd`/`source_job_id`/`source_query_id` plus the
--   2026-08-13-in-review-found `source_journal_period`/
--   `source_journal_granularity` pair for the journal compile — why every
--   source-id/period/granularity column is nullable and deliberately
--   unbackfilled, why there is no FK to `jobs`/`query_history`, why nothing
--   writes them until PR4/PR5, and why the three supporting indexes live in
--   separate `CONCURRENTLY` files).
--
-- The DDL below (below this header) is BYTE-IDENTICAL to the control
-- migration — `llm_usage` is a SHARED table, present in both trees and NOT in
-- `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its
-- schema-only `pg_dump` diff fails on any divergence here. Same convention as
-- `migrations_tenant/0012_coding_session_atoms_index_concurrently.sql`'s
-- pairing with control `0099`.
--
-- `llm_usage` is squashed into this tree's `0001_tenant_baseline.sql` (it
-- already carries 0044's original columns plus 0073's cache-token columns),
-- so there is no separate tenant-tree `0044`/`0073` pair to depend on here —
-- the baseline snapshot IS that dependency. Likewise `journal_rollups`'
-- `granularity IN ('hour','day','week','month')` vocabulary (control `0117`)
-- is squashed into `0001_tenant_baseline.sql` too — this migration reuses
-- that CHECK's literal set for `source_journal_granularity`, not a live
-- tenant-tree `0117`.
--
-- PAIRED WITH: backend/migrations_control/0141_llm_usage_provider_cost.sql
-- Depends on: 0001_tenant_baseline.sql (creates llm_usage on the tenant plane).
-- Down: see control 0141.

SET LOCAL lock_timeout = '3s';

ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'anthropic';
ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS cost_usd NUMERIC;
ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS source_job_id UUID;
ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS source_query_id UUID;
ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS source_journal_period TIMESTAMPTZ;
ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS source_journal_granularity TEXT
    CHECK (source_journal_granularity IS NULL OR source_journal_granularity IN ('hour', 'day', 'week', 'month'));
