-- Migration: 0075_llm_usage_upstream_provider — tenant twin of
--   migrations_control/0202_llm_usage_upstream_provider.sql. See that file's
--   header for the full rationale (the measured 16% `z-ai/glm-5.3-flash`
--   failure rate, why `provider` itself can't be repurposed, why this column
--   is nullable and unbackfilled, and the deliberate gaps that leave it NULL
--   for the raw-stream/batch shapes).
--
-- The DDL below (below this header) is BYTE-IDENTICAL to the control
-- migration — `llm_usage` is a SHARED table, present in both trees and NOT in
-- `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its
-- schema-only `pg_dump` diff fails on any divergence here. Same convention as
-- `migrations_tenant/0048_llm_usage_provider_cost.sql`'s pairing with
-- control `0141`.
--
-- PAIRED WITH: backend/migrations_control/0202_llm_usage_upstream_provider.sql
-- Depends on: 0048_llm_usage_provider_cost.sql (most recent prior ALTER
--   adding a column to this table on the tenant tree).
-- Down: see control 0202.

SET LOCAL lock_timeout = '3s';

ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS upstream_provider TEXT;
