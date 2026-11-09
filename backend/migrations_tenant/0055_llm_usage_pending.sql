-- Migration: 0055_llm_usage_pending — tenant twin of
--   migrations_control/0149_llm_usage_pending.sql. See that file's header for
--   the full rationale (the reserve half of PR4's reserve/finalize choke
--   point, why it is a SEPARATE transaction from finalize, the `kind` column
--   PR6's sweeper needs, why `estimated_input_tokens` is never billed, and why
--   there is no FK on any column here).
--
-- The `CREATE TABLE` + `CREATE INDEX` DDL below is BYTE-IDENTICAL to the
-- control migration — no column here has an FK to a control-tree-only table
-- (unlike `0047_mcp_tasks`'s `account_id`/`owner_user_id` divergence), so
-- unlike that pairing there is nothing to carve out on the table-shape side.
-- Only the RLS ENABLE/POLICY block differs, same as `0047_mcp_tasks`'s own
-- pairing with `0140_mcp_tasks`.
--
-- ── RLS: tenant-plane `member_ocean_isolation`, added here ──
-- Anchored DIRECTLY on `graph_id` via `tenant_rls_visible_graph_ids()`, the
-- SAME predicate `llm_usage`'s own tenant-plane policy uses
-- (`migrations_tenant/0009_tenant_rls` §6) — a pending row for a call made on
-- behalf of one ocean is visible under exactly the same member-ocean rule the
-- eventual `llm_usage` row it finalizes into will be. `account_id`/`user_id`
-- are NOT part of this predicate (same as `llm_usage`: `graph_id` is the one
-- tenant-plane anchor; `user_id` is the RLS scope SELECTOR reserve/finalize
-- use to open the transaction, not a second predicate column).
--
-- Registered in `scripts/check_schema_parity.sh`'s `TENANT_ONLY_RLS_TABLES` in
-- this same PR, per that list's own instruction.
--
-- PAIRED WITH: migrations_control/0149_llm_usage_pending.sql
-- Depends on: 0001_tenant_baseline.sql (llm_usage, the table this is the
--   reserve twin of), 0009_tenant_rls.sql (tenant_rls_visible_graph_ids(), the
--   RLS pattern this follows).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON llm_usage_pending;
--   ALTER TABLE llm_usage_pending DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS llm_usage_pending;

SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS llm_usage_pending (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id             UUID,
    user_id                UUID,
    graph_id               UUID,
    task                   TEXT        NOT NULL,
    provider               TEXT        NOT NULL,
    model                  TEXT        NOT NULL,
    estimated_input_tokens INTEGER     NOT NULL,
    started_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind                   TEXT        NOT NULL DEFAULT 'sync'
                                        CHECK (kind IN ('sync', 'batch'))
);

CREATE INDEX IF NOT EXISTS idx_llm_usage_pending_kind_started_at
    ON llm_usage_pending (kind, started_at);

ALTER TABLE llm_usage_pending ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON llm_usage_pending;
CREATE POLICY member_ocean_isolation ON llm_usage_pending
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
