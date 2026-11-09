-- Migration: 0132_reachable_graphs_team_id
-- Description: PRODUCTION BUG FIX — the workspace Members panel
--              (`/workspaces?id=<graph_id>`, `WorkspaceMembers.tsx`) showed
--              "team not found" for every shared workspace. The UI only
--              ever had `graphs.id` (an "ocean") to send, and every
--              `/v1/teams/{team_id}/...` route (`routes::teams`) is keyed on
--              `teams.id` — a SEPARATE primary key from `graphs.id`
--              (`0022_teams.sql:35-42`). Same root cause as, but unrelated
--              to, the member-private-oceans reachability work in 0090/0091.
--
--              The fix: publish `teams.id` on the `Graph` payload every
--              reachability/listing query returns (`store::Graph::team_id`,
--              `Option<Uuid>`, `Some` iff `kind = 'team'`), joined via a
--              correlated subquery (`store/mod.rs`'s `TEAM_ID_SUBQUERY`):
--
--                (SELECT t.id FROM teams t WHERE t.graph_id = g.id
--                 ORDER BY t.created_at ASC, t.id ASC LIMIT 1)
--
--              Every DIRECT `graphs` SELECT in `store/mod.rs` (`list_graphs`,
--              `get_graph`, and their `_scoped` twins) splices that subquery
--              text straight into its own query — no migration needed there.
--              This migration exists ONLY for the two `SECURITY DEFINER`
--              functions that stand between `store::mod`'s
--              `list_reachable_graphs` and the same `graphs` table
--              (`resolve_reachable_graphs`, control plane) and its
--              org-tenant-plane sibling (`resolve_tenant_reachable_graphs`)
--              — a SQL function's `RETURNS TABLE` can't be joined into from
--              the caller's side, so the SAME correlated subquery has to be
--              baked into each function body instead.
--
--              Determinism, not uniqueness (see `TEAM_ID_SUBQUERY`'s own doc
--              comment in `store/mod.rs` for the full rationale):
--              `teams.graph_id` has no unique constraint, so the subquery
--              picks the OLDEST `teams` row per `graphs` row
--              (`ORDER BY created_at ASC, id ASC LIMIT 1`) — a stable,
--              defensive tie-break for a case `create_team_tx` never
--              actually produces (it inserts exactly one `teams` row per
--              `graphs` row), not evidence that a duplicate exists today.
--
--              ── Also folded in here: `my_team_role` ──
--              Same PR, same DROP+recreate (workspace-manage design doc's
--              "Making role cheap" section): both functions ALSO now return
--              `my_team_role` (`text`, `'admin'` | `'member'` | `NULL`) — the
--              CALLER's role in the SAME team `team_id` resolves to, via a
--              correlated subquery scoped to this function's own `p_user_id`
--              argument (no extra parameter needed):
--
--                (SELECT tm.role FROM team_members tm
--                 JOIN teams t ON t.id = tm.team_id
--                 WHERE t.graph_id = g.id AND tm.user_id = p_user_id
--                   AND tm.departing_at IS NULL
--                 ORDER BY t.created_at ASC, t.id ASC LIMIT 1)
--
--              Same determinism rationale as `team_id`'s own subquery (oldest
--              `teams` row wins on the defensive multi-team-per-graph case),
--              additionally scoped to the caller and gated on active
--              membership so a departed member reads `NULL`, identically to
--              never having joined. Landed in the SAME migration as `team_id`
--              rather than a follow-up, deliberately: Postgres forbids
--              `CREATE OR REPLACE FUNCTION` from changing output columns, so
--              adding this after `team_id`'s column set had already shipped
--              would force a SECOND DROP+recreate of both functions, a
--              second migration pair, and a second chance to miss the
--              `EXECUTE` re-grant below — which, missed once already in an
--              earlier draft of THIS migration, would have broken every org
--              request path with 42501 after the OPS-4 cutover.
--
--              ── The 7 -> 9 column hazard, again ──
--              Same DROP-and-recreate requirement 0091's header already
--              documented for `resolve_reachable_graphs`: Postgres does not
--              allow `CREATE OR REPLACE FUNCTION` to change a function's
--              output column set. `resolve_graph_reachable_by_user` and
--              `resolve_tenant_graph_reachable_by_user` return a plain
--              `boolean` (no `Graph` payload involved) and are untouched by
--              this migration.
--
--              Every Rust-side row shape that reads either function grows
--              the same 7 -> 9 tuple in this PR: `store/mod`'s `GraphRow`/
--              `graph_row_to_graph`, and `tenancy::reachability`'s
--              `GraphRow`/`row_to_graph`. `backend/tests/
--              reachable_graphs_parity_pg.rs` and `reachability_pg.rs`
--              (both env-gated on a live Postgres, `ORGT_TEST_BASE_URL`) are
--              the guards that a mismatch here is caught in a live query.
--
-- Depends on: 0022_teams (teams.graph_id, the join target),
--             0091_member_ocean_reachability_functions (the 7-column bodies
--             this migration extends to 8).
--
-- Down:
--   DROP FUNCTION IF EXISTS resolve_reachable_graphs(uuid);
--   CREATE FUNCTION resolve_reachable_graphs(p_user_id uuid)
--   RETURNS TABLE(
--       id uuid, name text, kind text, is_primary boolean, created_at timestamptz,
--       owner_user_id uuid, archived_at timestamptz
--   )
--   LANGUAGE sql SECURITY DEFINER STABLE SET search_path = public, pg_temp
--   AS $$
--       SELECT g.id, g.name, g.kind, g.is_primary, g.created_at, g.owner_user_id, g.archived_at
--       FROM graphs g
--       WHERE g.account_id IN
--           (SELECT account_id FROM account_members WHERE user_id = p_user_id)
--         AND g.archived_at IS NULL
--         AND (g.kind <> 'team' OR g.id IN
--           (SELECT t.graph_id FROM teams t
--            JOIN team_members tm ON tm.team_id = t.id
--            WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
--         AND (g.kind <> 'member' OR g.owner_user_id = p_user_id)
--       ORDER BY g.is_primary DESC, g.created_at DESC
--       LIMIT 500
--   $$;
--   GRANT EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) TO coralswarm_app, CURRENT_USER;
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
--   GRANT EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) TO coralswarm_app, CURRENT_USER;

SET LOCAL lock_timeout = '3s';

-- ── 1. resolve_reachable_graphs — DROP + recreate (output columns changed) ──
DROP FUNCTION IF EXISTS resolve_reachable_graphs(uuid);

CREATE FUNCTION resolve_reachable_graphs(p_user_id uuid)
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
    WHERE g.account_id IN
        (SELECT account_id FROM account_members WHERE user_id = p_user_id)
      AND g.archived_at IS NULL
      AND (g.kind <> 'team' OR g.id IN
        (SELECT t.graph_id FROM teams t
         JOIN team_members tm ON tm.team_id = t.id
         WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
      AND (g.kind <> 'member' OR g.owner_user_id = p_user_id)
    ORDER BY g.is_primary DESC, g.created_at DESC
    LIMIT 500
$$;

-- ── 2. resolve_tenant_reachable_graphs — same output-column hazard ──────────
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
-- Same defense-in-depth reasoning as 0084/0091's Grants sections: Postgres
-- grants EXECUTE to PUBLIC by default on a newly created/replaced function,
-- and both functions here are `SECURITY DEFINER`. Both were DROPped above,
-- so both lost their prior grants and need them unconditionally re-applied.
REVOKE EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) TO coralswarm_app, CURRENT_USER;
