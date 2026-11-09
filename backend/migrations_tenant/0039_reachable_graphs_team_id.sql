-- 0039_reachable_graphs_team_id.sql — paired half of
-- `migrations_control/0132_reachable_graphs_team_id.sql`
-- (workspace-members team-id fix). See that migration's header for the full
-- rationale: the Members panel sent `graphs.id` to `/v1/teams/{team_id}/...`
-- routes that expect `teams.id`, so `store::Graph` grows a `team_id`
-- (`Option<Uuid>`) field populated via a correlated subquery joining
-- `teams.graph_id = graphs.id`.
--
-- `resolve_tenant_reachable_graphs` is the org-tenant-plane half of
-- `Store::list_reachable_graphs` (`tenancy::reachability::
-- tenant_list_reachable_graphs`) and must grow the same `team_id` column the
-- control-tree function does, for the same reason 0006 paired
-- `resolve_reachable_graphs`'s body change: `graphs`/`teams`/`team_members`
-- are SHARED tenant-tree tables (present in both trees, not in
-- `check_schema_parity.sh`'s `CONTROL_TABLES` roster), and this function is
-- landed BYTE-IDENTICAL on both sides purely so the schema-parity job's
-- `pg_dump --schema-only` diff sees matching definitions — see 0006's header
-- for the full "why this exists on the control tree too" explanation.
--
-- `resolve_tenant_graph_reachable_by_user` returns a plain `boolean` (no
-- `Graph` payload) and is untouched here, exactly as
-- `resolve_graph_reachable_by_user` is untouched on the control side.
--
-- ── Also folded in here: `my_team_role` ──
-- Same PR, same DROP+recreate (workspace-manage design doc's "Making role
-- cheap" section) as its control-side twin (`0132_reachable_graphs_team_id`
-- — see that migration's header for the full rationale on why this MUST land
-- alongside `team_id` rather than in a follow-up migration): the function
-- also now returns `my_team_role` (`text`, `'admin'` | `'member'` | `NULL`)
-- via a correlated subquery scoped to `p_user_id`, mirroring `team_id`'s own
-- determinism choice:
--
--   (SELECT tm.role FROM team_members tm
--    JOIN teams t ON t.id = tm.team_id
--    WHERE t.graph_id = g.id AND tm.user_id = p_user_id
--      AND tm.departing_at IS NULL
--    ORDER BY t.created_at ASC, t.id ASC LIMIT 1)
--
-- Depends on: 0001_tenant_baseline (graphs, teams, team_members),
--             0006_member_ocean_reachability_functions (the 7-column body
--             this migration extends to 8).
--
-- Down:
--   DROP FUNCTION IF EXISTS resolve_tenant_reachable_graphs(uuid, uuid);
--   CREATE OR REPLACE FUNCTION resolve_tenant_reachable_graphs(p_account_id uuid, p_user_id uuid)
--   RETURNS TABLE(
--       id uuid, name text, kind text, is_primary boolean, created_at timestamptz,
--       owner_user_id uuid, archived_at timestamptz
--   )
--   LANGUAGE sql SECURITY DEFINER STABLE SET search_path = public, pg_temp
--   AS $$
--       SELECT g.id, g.name, g.kind, g.is_primary, g.created_at, g.owner_user_id, g.archived_at
--       FROM graphs g
--       WHERE g.account_id = p_account_id
--         AND g.archived_at IS NULL
--         AND (g.kind <> 'team' OR g.id IN
--           (SELECT t.graph_id FROM teams t
--            JOIN team_members tm ON tm.team_id = t.id
--            WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
--         AND (g.kind <> 'member' OR g.owner_user_id = p_user_id)
--       ORDER BY g.is_primary DESC, g.created_at DESC
--       LIMIT 500
--   $$;
--   REVOKE EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) FROM PUBLIC;

SET LOCAL lock_timeout = '3s';

-- Output columns changed (7 -> 9): DROP + recreate, same hazard 0132
-- documents for the control-tree twin (team_id AND my_team_role both fold
-- into this one DROP+recreate — see this file's header).
DROP FUNCTION IF EXISTS resolve_tenant_reachable_graphs(uuid, uuid);

CREATE FUNCTION resolve_tenant_reachable_graphs(p_account_id uuid, p_user_id uuid)
RETURNS TABLE(
    id uuid, name text, kind text, is_primary boolean, created_at timestamptz,
    owner_user_id uuid, archived_at timestamptz, team_id uuid, my_team_role text
)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.id, g.name, g.kind, g.is_primary, g.created_at, g.owner_user_id, g.archived_at,
        (SELECT t.id FROM teams t WHERE t.graph_id = g.id ORDER BY t.created_at ASC, t.id ASC LIMIT 1),
        (SELECT tm.role FROM team_members tm
         JOIN teams t ON t.id = tm.team_id
         WHERE t.graph_id = g.id AND tm.user_id = p_user_id AND tm.departing_at IS NULL
         ORDER BY t.created_at ASC, t.id ASC LIMIT 1)
    FROM graphs g
    WHERE g.account_id = p_account_id
      AND g.archived_at IS NULL
      AND (g.kind <> 'team' OR g.id IN
        (SELECT t.graph_id FROM teams t
         JOIN team_members tm ON tm.team_id = t.id
         WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
      AND (g.kind <> 'member' OR g.owner_user_id = p_user_id)
    ORDER BY g.is_primary DESC, g.created_at DESC
    LIMIT 500
$$;

-- ── Grants ────────────────────────────────────────────────────────────────
-- Same defense-in-depth reasoning as 0006's Grants section — DROPped above,
-- so re-applied unconditionally.
REVOKE EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) FROM PUBLIC;

-- ── ...and RE-GRANT to the least-privilege app role ───────────────────────
-- LOAD-BEARING, and the reason this block exists at all: `DROP FUNCTION`
-- discards every grant on the function along with it. 0009's §"execute is an
-- allowlist" granted EXECUTE on this exact signature to
-- `coralswarm_tenant_app`; dropping and recreating it here silently revoked
-- that. Without this re-grant, every live org request path that resolves
-- reachable graphs starts failing with 42501 (insufficient_privilege) the
-- moment OPS-4 points the app at the non-superuser role — not at deploy time,
-- which is what makes it the nastiest possible shape of regression.
--
-- Caught by `tenant_rls_pg::execute_is_an_allowlist_not_a_schema_wide_grant`,
-- whose assertion message says exactly this. Anyone adding another
-- DROP+CREATE of a resolver here owes it the same re-grant.
--
-- Role-guarded like 0009's own block: `coralswarm_tenant_app` is created by
-- 0009 and may legitimately not exist on a database that has not reached it
-- yet, and a bare GRANT against a missing role aborts the migration.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
        GRANT EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid)
            TO coralswarm_tenant_app;
    END IF;
END
$$;
