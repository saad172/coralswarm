-- Migration: 0047_mcp_tasks — tenant twin of migrations_control/0140_mcp_tasks.sql.
-- See that file's header for the full rationale (MCP spec 2026-07-28 PR 2,
-- task-based `ask_ocean`, why only the LEGACY era writes rows here, the
-- `query_history`-precedent column choices, and the 24h TTL / sweep contract).
--
-- The `CREATE TABLE` + index DDL below is byte-identical to the control
-- migration EXCEPT `account_id`/`owner_user_id`, which are bare `uuid` (no
-- FK) here — `accounts`/`users` are control-tree-only tables, same
-- constraint `0139_reef_judgement`'s `created_by`/`annulled_by` and
-- `0125_membership_events`'s `actor_user_id`/`target_user_id` already
-- document. `graph_id` keeps its FK to `graphs(id)` — that table exists on
-- both trees.
--
-- ── RLS: tenant-plane `member_ocean_isolation`, added here ──
-- Same precedent as `0046_reef_judgement`/`0032_membership_events`: 0009 is
-- already closed (append-only), so this new table's policy is ENABLEd in its
-- own migration. Anchored DIRECTLY on `graph_id` via
-- `tenant_rls_visible_graph_ids()` (the SECURITY DEFINER member-ocean
-- predicate every 0009-shaped policy resolves through) — a task's `graph_id`
-- is the ocean `ask_ocean` actually queried, so a private-ocean task is
-- visible only to that ocean's owning member (or the system principal), and a
-- shared-commons-ocean task is visible to any member who can reach that
-- ocean, exactly as `query_history`'s own tenant policy already behaves for
-- the identical multi-ocean tool.
--
-- Registered in `scripts/check_schema_parity.sh`'s `TENANT_ONLY_RLS_TABLES`
-- in this same PR, per that list's own instruction.
--
-- PAIRED WITH: migrations_control/0140_mcp_tasks.sql
-- Depends on: 0001_tenant_baseline (graphs), 0009_tenant_rls
--   (tenant_rls_visible_graph_ids(), the RLS pattern this follows).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON mcp_tasks;
--   ALTER TABLE mcp_tasks DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS mcp_tasks;

CREATE TABLE IF NOT EXISTS mcp_tasks (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id     UUID        NOT NULL,  -- bare uuid, no FK -- accounts is control-tree-only.
    graph_id       UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    owner_user_id  UUID        NOT NULL,  -- bare uuid, no FK -- users is control-tree-only.

    tool_name      TEXT        NOT NULL CHECK (tool_name <> ''),
    status         TEXT        NOT NULL DEFAULT 'working'
                                CHECK (status IN ('working', 'completed', 'failed', 'cancelled')),

    result         JSONB,
    error          JSONB,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '24 hours'),

    CONSTRAINT mcp_tasks_completed_has_result CHECK (status <> 'completed' OR result IS NOT NULL),
    CONSTRAINT mcp_tasks_failed_has_error     CHECK (status <> 'failed'    OR error  IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_mcp_tasks_expires_at ON mcp_tasks (expires_at);
CREATE INDEX IF NOT EXISTS idx_mcp_tasks_owner ON mcp_tasks (owner_user_id, created_at DESC);

ALTER TABLE mcp_tasks ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON mcp_tasks;
CREATE POLICY member_ocean_isolation ON mcp_tasks
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
