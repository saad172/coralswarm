-- Migration: 0140_mcp_tasks
-- Description: MCP spec 2026-07-28 adoption, PR 2 ("task-based ask_ocean").
--
--   `ask_ocean` becomes task-based for every caller (founder-locked spec,
--   capability-gating explicitly rejected): the modern era (2026-07-28+
--   clients that declare the `io.modelcontextprotocol/tasks` SEP-2663
--   capability) gets an unsolicited protocol task handle via rmcp 3.1.2's
--   in-process `TaskManager` (`mcp::server::CoralSwarmMcp::get_task`/
--   `update_task`/`cancel_task`, `AppState::mcp_task_manager`); the legacy
--   era (every Claude client today) instead gets an ordinary tool result
--   carrying a task id, backed by THIS table, plus a new `get_task_result`
--   poll tool (`mcp::tools::read`).
--
--   Only the LEGACY path writes rows here — the modern path's task state is
--   owned entirely by rmcp's in-memory `TaskManager` (see that struct's own
--   doc: no persistence, TTL-swept in-process). This table is therefore the
--   durable half of the split, not a mirror of the in-memory half; a
--   `get_task_result` call against a modern-era task id (one the caller
--   should have polled via `tasks/get` instead) simply misses here and
--   answers `not_found`, which is the honest answer — no such row was ever
--   written.
--
--   ── Column choices, mirroring `query_history` (0019/0083) ──
--   `account_id`/`graph_id`/`owner_user_id` follow that table's precedent
--   exactly (see 0083_consumer_rls's `query_history` policy and
--   0009_tenant_rls's tenant twin): `graph_id` is the ONE ocean this ask
--   resolved against (the first entry of `ask_ocean`'s possibly-multi-ocean
--   `ocean_ids`, matching `spawn_query_history`'s existing single-graph_id
--   convention for the very same multi-ocean tool), `owner_user_id` is the
--   caller who created the task (also re-checked in Rust on every
--   `get_task_result` call — RLS is the outer boundary, the WHERE clause is
--   the inner one, same "id-anchored tools scope by the caller" convention
--   `mcp/tools/read.rs`'s module doc already documents for `get_reef`).
--
--   `tool_name` is future-proofing: only `ask_ocean` writes tasks today, but
--   nothing here is `ask_ocean`-specific, so a later task-eligible tool needs
--   no schema change.
--
--   `status` mirrors SEP-2663's `TaskStatus` vocabulary (`working` /
--   `completed` / `failed` / `cancelled`) minus `input_required`, which
--   `ask_ocean` never produces (it is a plain request/response call, same
--   "none of our tools return MRTR's InputRequired" note `mcp/server.rs`
--   already carries) — kept anyway for vocabulary symmetry with the modern
--   protocol path rather than inventing a divergent enum.
--
--   `expires_at` defaults to a 24h TTL from creation (locked spec). Read
--   paths (`get_task_result`) treat `expires_at < now()` as `not_found`
--   even before the periodic sweep (`workers::mcp_task_sweep`, hourly) has
--   physically reaped the row — the sweep reclaims storage, it is not what
--   makes an expired task inaccessible.
--
--   ── RLS: consumer-plane `account_isolation`, added here ──
--   Same precedent as 0125_membership_events/0139_reef_judgement: 0083 is
--   already closed (append-only), so a new table's policy is ENABLEd in its
--   own migration rather than retrofitted into 0083. Anchored DIRECTLY on
--   `account_id` (not `graph_id`) — the majority shape 0083 uses for tables
--   with a bare `account_id` column (`queries`, `query_history`,
--   `outlook_watched_conversations`, ...) — because the consumer plane's
--   boundary IS the account, and every row here always carries one.
--   Registered in `scripts/check_schema_parity.sh`'s `CONTROL_ONLY_RLS_TABLES`
--   in this same PR, per that list's own instruction.
--
-- PAIRED WITH: migrations_tenant/0047_mcp_tasks.sql — table shape is
--   IDENTICAL except `account_id`/`owner_user_id`, which are bare `uuid` (no
--   FK) on the tenant tree — `accounts`/`users` live only in the control DB.
-- Depends on: 0011_nodes (graphs), 0083_consumer_rls (RLS pattern this
--   follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON mcp_tasks;
--   ALTER TABLE mcp_tasks DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS mcp_tasks;

CREATE TABLE IF NOT EXISTS mcp_tasks (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    graph_id       UUID        NOT NULL REFERENCES graphs(id)   ON DELETE CASCADE,
    owner_user_id  UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,

    tool_name      TEXT        NOT NULL CHECK (tool_name <> ''),
    status         TEXT        NOT NULL DEFAULT 'working'
                                CHECK (status IN ('working', 'completed', 'failed', 'cancelled')),

    -- Populated only on the matching terminal status; NULL otherwise (see the
    -- two CHECK constraints below).
    result         JSONB,
    error          JSONB,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '24 hours'),

    CONSTRAINT mcp_tasks_completed_has_result CHECK (status <> 'completed' OR result IS NOT NULL),
    CONSTRAINT mcp_tasks_failed_has_error     CHECK (status <> 'failed'    OR error  IS NOT NULL)
);

-- Sweep predicate (`workers::mcp_task_sweep`, hourly, well inside the 24h TTL).
CREATE INDEX IF NOT EXISTS idx_mcp_tasks_expires_at ON mcp_tasks (expires_at);
-- `get_task_result` lookup pattern: caller's own tasks, newest first.
CREATE INDEX IF NOT EXISTS idx_mcp_tasks_owner ON mcp_tasks (owner_user_id, created_at DESC);

ALTER TABLE mcp_tasks ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON mcp_tasks;
CREATE POLICY account_isolation ON mcp_tasks
    USING (account_id = current_setting('app.account_id')::uuid);
