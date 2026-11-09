-- Migration 0064: coding_sessions.session_kind (tenant twin of
--   migrations_control/0185_coding_sessions_session_kind.sql — capture
--   blocker B1, the schema-parity twin pattern).
--
--   `coding_sessions` is a shared tenant table (it carries `graph_id` and is
--   covered by `member_ocean_isolation` — see
--   `migrations_tenant/0009_tenant_rls.sql`), so a column added on one plane
--   MUST land on the other in the same PR or `scripts/
--   check_schema_parity.sh` fails and newly-provisioned tenant databases
--   silently diverge from the control/consumer plane.
--
--   Column-for-column identical to the control twin. See that migration's
--   header for the full rationale: why the label is still DERIVED rather
--   than trusted, why `DEFAULT 'coding'` is what keeps every existing caller
--   working, why the column is INSERT-only in practice, and why there is
--   deliberately no `CHECK` constraint pinning the closed set into the
--   schema.
--
--   ── RLS: nothing to add ──────────────────────────────────────────────
--   `coding_sessions` already has its `member_ocean_isolation` policy from
--   `0009_tenant_rls.sql`; a policy is written against the table, not its
--   column list, so an added column inherits the existing predicate with no
--   change here.
--
-- Depends on: 0001_tenant_baseline (the `coding_sessions` table).
-- Down:
--   ALTER TABLE coding_sessions DROP COLUMN IF EXISTS session_kind;

ALTER TABLE coding_sessions
    ADD COLUMN IF NOT EXISTS session_kind TEXT NOT NULL DEFAULT 'coding';
