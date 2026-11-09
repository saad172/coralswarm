-- Migration: 0091_member_ocean_reachability_functions
-- Description: member-private-oceans PR 2
--              (docs/plans/member-private-oceans.md §2.2/§7) — teach the
--              ocean-reachability predicate about `kind = 'member'` and
--              `archived_at`, on BOTH planes, and move the org-tenant arm's
--              predicate out of hand-rolled Rust (`reachability.rs`) into a
--              paired SQL function. Still INERT: no `kind = 'member'` row can
--              exist yet (nothing creates one before PR 4, behind
--              `MEMBER_OCEANS_ENABLED`), and no row has `archived_at` set
--              (nothing writes it before PR 7) — so every predicate change
--              below is a behavioral no-op against every row that exists
--              today. See each function's comment for why.
--
--              New predicate (identical on every arm, plan §2.2):
--
--                AND g.archived_at IS NULL
--                AND (g.kind <> 'team'   OR g.id IN (SELECT t.graph_id FROM teams t
--                                                    JOIN team_members tm ON tm.team_id = t.id
--                                                    WHERE tm.user_id = $u AND tm.departing_at IS NULL))
--                AND (g.kind <> 'member' OR g.owner_user_id = $u)
--
--              The `'member'` arm is a PURE EQUALITY — no OR-arm, no join —
--              deliberately, per the plan's §4: shared-channel visibility
--              (Stage 2) is atom-level and lives on a DIFFERENT graph kind
--              (`'channel'`, not yet in the `graphs_kind_check` CHECK), so it
--              can never touch this predicate. Do not "simplify" this arm
--              back into something that joins another table — that is
--              precisely the shape change the plan rules out.
--
--              ── Why this MUST be SQL, not Rust (the P0 this PR closes) ──
--              Rolling-deploy safety is the whole reason this migration
--              exists rather than a Rust-only change (plan §2.2, §7 row for
--              PR 2). During a rolling deploy, OLD and NEW pods serve traffic
--              concurrently against the SAME database:
--
--                * The CONSUMER arm already delegates to the two
--                  `SECURITY DEFINER` functions this migration replaces the
--                  BODIES of (`resolve_reachable_graphs` /
--                  `resolve_graph_reachable_by_user`, migration
--                  0084_bootstrap_scope_functions:183-226). Replacing a
--                  function's body is a single atomic DDL statement that both
--                  an old pod's Rust (which still just calls
--                  `resolve_reachable_graphs($1)` by name) and a new pod's
--                  Rust see identically the instant this migration commits —
--                  there is no window where the two pods disagree about what
--                  the function does.
--                * The ORG-TENANT arm, BEFORE this PR, is hand-rolled SQL
--                  string-built inside `backend/src/tenancy/reachability.rs`
--                  (`tenant_list_reachable_graphs` /
--                  `tenant_graph_reachable_by_user`). An OLD pod's binary
--                  embeds the OLD Rust string verbatim — it does not
--                  "pick up" a SQL migration at all, because there is no SQL
--                  for it to pick up; the predicate live inside compiled Rust.
--                  If member-ocean gating shipped as a Rust-only change (add
--                  the `kind <> 'member' OR owner_user_id = $u` clause to the
--                  Rust string), an old pod mid-rollout would keep running its
--                  compiled-in predicate, which special-cases ONLY `'team'` —
--                  so it would return EVERY member ocean in the account to
--                  EVERY member of that account for as long as any old pod
--                  is still serving traffic. That is exactly the exposure
--                  this migration exists to make structurally impossible.
--
--              The fix: give the tenant arm its OWN paired SQL functions
--              (`resolve_tenant_reachable_graphs` /
--              `resolve_tenant_graph_reachable_by_user`, new in this
--              migration) and make `reachability.rs` a thin call into them —
--              see that file's PR-2 diff. Once the predicate lives in the
--              database, replacing its body fixes old AND new pods
--              simultaneously, on both arms, the same way 0084's carve-out
--              already does for the consumer arm. Row creation (the actual
--              observable behavior change) stays behind
--              `MEMBER_OCEANS_ENABLED`, flipped only after the fleet is fully
--              on this migration — so this SQL-vs-Rust distinction is what
--              makes that flag flip safe, not merely tidy.
--
--              ── Pairing is mandatory, not stylistic ──
--              `resolve_tenant_reachable_graphs` /
--              `resolve_tenant_graph_reachable_by_user` are landed
--              BYTE-IDENTICAL in this migration and in the paired
--              `migrations_tenant/0006_member_ocean_reachability_functions.sql`,
--              even though the control/consumer plane never calls them today
--              (they exist here purely so `check_schema_parity.sh`'s
--              `pg_dump --schema-only` diff — which is NOT told about a
--              tenant-only function the way it is told about a
--              control-only one via `CONTROL_FUNCTIONS` — sees the identical
--              definition on both sides and stays green). `graphs` /
--              `teams` / `team_members` are shared tenant-tree tables present
--              on both trees, so the function bodies are syntactically valid
--              (and, if ever called, correct) on either database.
--
--              `resolve_reachable_graphs` / `resolve_graph_reachable_by_user`
--              stay control-plane-only, exactly as before (already listed in
--              `scripts/check_schema_parity.sh`'s `CONTROL_FUNCTIONS`
--              ignore list by migration 0084 — verified during this PR that
--              replacing their BODIES, with the same name/argument types,
--              needs no change to that list; only ADDING or RENAMING a
--              control-only function would).
--
--              ── The 5 -> 7 column hazard ──
--              `resolve_reachable_graphs`'s `RETURNS TABLE` grows from 5 to 7
--              columns (`owner_user_id`, `archived_at` — migration 0090's new
--              `graphs` columns). Postgres does NOT allow `CREATE OR REPLACE
--              FUNCTION` to change a function's output column set — it
--              errors "cannot change return type of existing function" — so
--              that one function is DROPped and recreated (losing its
--              grants, which this migration re-applies below);
--              `resolve_graph_reachable_by_user` keeps its `boolean` return
--              type and stays a straight `CREATE OR REPLACE`. The two new
--              `resolve_tenant_*` functions are brand new, so no DROP is
--              needed for them.
--
--              Every Rust-side row shape that reads either family of
--              functions (`store/mod.rs`'s `GraphRow`,
--              `tenancy::reachability::GraphRow`) grows the same 5 -> 7
--              tuple in the SAME PR — see those files' comments for the
--              "must stay in lockstep" hazard this header is warning about.
--              `backend/tests/reachable_graphs_parity_pg.rs` and
--              `backend/tests/reachability_pg.rs` are the guards.
--
-- Depends on: 0084_bootstrap_scope_functions (the two functions replaced
--             here), 0090_member_ocean_columns (graphs.owner_user_id,
--             graphs.archived_at, graphs.graphs_kind_check widened to admit
--             'member'), 0022_teams (teams, team_members), 0083_consumer_rls
--             (creates the coralswarm_app role these functions are granted
--             to).
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid, uuid, uuid) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_tenant_reachable_graphs(uuid, uuid);
--   DROP FUNCTION IF EXISTS resolve_tenant_graph_reachable_by_user(uuid, uuid, uuid);
--   -- Restore the pre-0091 (5-column, no member/archived gating) bodies:
--   DROP FUNCTION IF EXISTS resolve_reachable_graphs(uuid);
--   CREATE FUNCTION resolve_reachable_graphs(p_user_id uuid)
--   RETURNS TABLE(id uuid, name text, kind text, is_primary boolean, created_at timestamptz)
--   LANGUAGE sql SECURITY DEFINER STABLE SET search_path = public, pg_temp
--   AS $$
--       SELECT g.id, g.name, g.kind, g.is_primary, g.created_at
--       FROM graphs g
--       WHERE g.account_id IN (SELECT account_id FROM account_members WHERE user_id = p_user_id)
--         AND (g.kind <> 'team' OR g.id IN
--           (SELECT t.graph_id FROM teams t JOIN team_members tm ON tm.team_id = t.id
--            WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
--       ORDER BY g.is_primary DESC, g.created_at DESC
--       LIMIT 500
--   $$;
--   GRANT EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) TO coralswarm_app, CURRENT_USER;
--   CREATE OR REPLACE FUNCTION resolve_graph_reachable_by_user(p_graph_id uuid, p_user_id uuid)
--   RETURNS boolean LANGUAGE sql SECURITY DEFINER STABLE SET search_path = public, pg_temp
--   AS $$
--       SELECT EXISTS (
--           SELECT 1 FROM graphs g
--           WHERE g.id = p_graph_id
--             AND g.account_id IN (SELECT account_id FROM account_members WHERE user_id = p_user_id)
--             AND (g.kind <> 'team' OR g.id IN
--                 (SELECT t.graph_id FROM teams t JOIN team_members tm ON tm.team_id = t.id
--                  WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
--       )
--   $$;

SET LOCAL lock_timeout = '3s';

-- ── 1. resolve_reachable_graphs — DROP + recreate (output columns changed) ──
DROP FUNCTION IF EXISTS resolve_reachable_graphs(uuid);

CREATE FUNCTION resolve_reachable_graphs(p_user_id uuid)
RETURNS TABLE(
    id uuid, name text, kind text, is_primary boolean, created_at timestamptz,
    owner_user_id uuid, archived_at timestamptz
)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.id, g.name, g.kind, g.is_primary, g.created_at, g.owner_user_id, g.archived_at
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

-- ── 2. resolve_graph_reachable_by_user — same return type, plain OR REPLACE ─
CREATE OR REPLACE FUNCTION resolve_graph_reachable_by_user(p_graph_id uuid, p_user_id uuid)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT EXISTS (
        SELECT 1 FROM graphs g
        WHERE g.id = p_graph_id
          AND g.account_id IN
              (SELECT account_id FROM account_members WHERE user_id = p_user_id)
          AND g.archived_at IS NULL
          AND (g.kind <> 'team' OR g.id IN
              (SELECT t.graph_id FROM teams t
               JOIN team_members tm ON tm.team_id = t.id
               WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
          AND (g.kind <> 'member' OR g.owner_user_id = p_user_id)
    )
$$;

-- ── 3. resolve_tenant_reachable_graphs — NEW, paired byte-identical into ────
--    migrations_tenant/0006. Backs `reachability.rs`'s
--    `tenant_list_reachable_graphs` (the org-tenant arm of
--    `ReachabilityResolver::list_reachable_graphs`). Structurally different
--    from (1) above: no `account_members` subquery (that table does not
--    exist on the tenant plane — `reachability.rs`'s module doc explains why
--    the tenant arm scopes by an explicit `account_id = $1` equality
--    instead), otherwise the identical team/member/archived gating.
CREATE OR REPLACE FUNCTION resolve_tenant_reachable_graphs(p_account_id uuid, p_user_id uuid)
RETURNS TABLE(
    id uuid, name text, kind text, is_primary boolean, created_at timestamptz,
    owner_user_id uuid, archived_at timestamptz
)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.id, g.name, g.kind, g.is_primary, g.created_at, g.owner_user_id, g.archived_at
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

-- ── 4. resolve_tenant_graph_reachable_by_user — NEW, paired byte-identical ──
--    into migrations_tenant/0006. Backs `reachability.rs`'s
--    `tenant_graph_reachable_by_user` (the org-tenant arm of
--    `ReachabilityResolver::graph_reachable_by_user`, the IDOR re-check).
CREATE OR REPLACE FUNCTION resolve_tenant_graph_reachable_by_user(
    p_graph_id uuid, p_account_id uuid, p_user_id uuid
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT EXISTS (
        SELECT 1 FROM graphs g
        WHERE g.id = p_graph_id
          AND g.account_id = p_account_id
          AND g.archived_at IS NULL
          AND (g.kind <> 'team' OR g.id IN
              (SELECT t.graph_id FROM teams t
               JOIN team_members tm ON tm.team_id = t.id
               WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
          AND (g.kind <> 'member' OR g.owner_user_id = p_user_id)
    )
$$;

-- ── Grants ────────────────────────────────────────────────────────────────
-- Same defense-in-depth reasoning as 0084's Grants section (CodeRabbit
-- review, MAJOR, on that migration): Postgres grants EXECUTE to PUBLIC by
-- default on a newly created/replaced function, and every function here is
-- `SECURITY DEFINER`. `resolve_reachable_graphs` lost its prior grants when
-- it was DROPped above, so it needs them re-applied unconditionally, not
-- only "if missing"; the other three are `CREATE OR REPLACE`, which
-- preserves existing grants, but the REVOKE+GRANT pair is repeated for all
-- four anyway so this migration's own idempotent re-run (or a future
-- ALTER) can never leave one of them on the PUBLIC default by accident.
REVOKE EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_graph_reachable_by_user(uuid, uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid, uuid, uuid) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_graph_reachable_by_user(uuid, uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid, uuid, uuid) TO coralswarm_app, CURRENT_USER;
