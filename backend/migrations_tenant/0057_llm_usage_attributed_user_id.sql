-- Migration: 0057_llm_usage_attributed_user_id — tenant twin of
--   migrations_control/0157_llm_usage_attributed_user_id.sql. See that file's
--   header for the full rationale — why `attributed_user_id` exists, and
--   critically, why it is NOT the same thing as `llm_usage_pending.user_id`
--   (that column is the tenant-plane RLS scope selector, gate-verified, never
--   stored; this column is stored-only attribution that never influences
--   transaction scoping or which `begin_*` scope function a call site uses —
--   read the control migration's header in full before touching either
--   column near this table again), why it is nullable/no-default/no-FK, and
--   why the supporting index lives in a separate `CONCURRENTLY` file.
--
-- The DDL below (below this header) is BYTE-IDENTICAL to the control
-- migration — `llm_usage` is a SHARED table, present in both trees and NOT in
-- `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its
-- schema-only `pg_dump` diff fails on any divergence here. Same convention as
-- `migrations_tenant/0048_llm_usage_provider_cost.sql`'s pairing with control
-- `0141`.
--
-- `llm_usage` is squashed into this tree's `0001_tenant_baseline.sql` (it
-- already carries 0044's original columns, 0073's cache-token columns, and
-- 0141/0048's provider/cost/source-id columns), so there is no separate
-- tenant-tree `0044`/`0141` pair to depend on here — the baseline snapshot IS
-- that dependency.
--
-- PAIRED WITH: backend/migrations_control/0157_llm_usage_attributed_user_id.sql
-- Depends on: 0001_tenant_baseline.sql (creates llm_usage on the tenant plane).
-- Down: see control 0157.

SET LOCAL lock_timeout = '3s';

ALTER TABLE llm_usage ADD COLUMN IF NOT EXISTS attributed_user_id UUID;
