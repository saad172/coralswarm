-- 0006_member_ocean_reachability_functions.sql — paired half of
-- `migrations_control/0091_member_ocean_reachability_functions.sql`
-- (member-private-oceans PR 2, docs/plans/member-private-oceans.md
-- §2.2/§7). See that migration's header for the full rationale: the new
-- reachability predicate (`archived_at IS NULL`, team gating unchanged, and
-- the new `kind <> 'member' OR owner_user_id = $u` PURE-EQUALITY member
-- arm), and — the reason this migration exists at all — the rolling-deploy
-- hazard that requires the org-tenant arm's predicate to live in the
-- database as a `SECURITY DEFINER` function rather than hand-rolled Rust
-- (`backend/src/tenancy/reachability.rs`), so replacing the function BODY
-- fixes old and new pods simultaneously during a deploy instead of leaving
-- an old pod serving a stale, member-oblivious predicate that would widen
-- every member ocean to every account member.
--
-- Only these 2 functions are paired here (unlike the control migration's 4):
-- `resolve_reachable_graphs` / `resolve_graph_reachable_by_user` stay
-- control-plane-only, exactly as migration 0084/0003 already established —
-- their predicate's `account_members` subquery cannot even be expressed
-- here (`account_members` is a control-tree table, absent from this
-- baseline entirely). `resolve_tenant_reachable_graphs` /
-- `resolve_tenant_graph_reachable_by_user` are the two genuinely NEW
-- functions this PR adds, and they land BYTE-IDENTICAL in both trees —
-- `graphs` is a SHARED table (present in both; NOT in
-- `check_schema_parity.sh`'s `CONTROL_TABLES` roster) and `teams`/
-- `team_members` are shared tenant-tree tables too, so both function bodies
-- are syntactically valid — and, since the org-tenant arm's queries only
-- ever run against a real tenant pool in production, only MEANINGFULLY
-- exercised — on this tree. They exist on the control tree purely so
-- `check_schema_parity.sh`'s `pg_dump --schema-only` diff (which has no
-- "tenant-only function" ignore list, unlike `CONTROL_FUNCTIONS` for the
-- opposite asymmetry) sees an identical definition on both sides.
--
-- The bodies below are kept BYTE-IDENTICAL to their control-tree
-- counterparts (only comments/header prose differ) — `check_schema_parity.sh`
-- diffs `pg_dump --schema-only` output (with `--no-privileges`, so the GRANT
-- statements below are deliberately NOT required to match; only the function
-- bodies are), which reproduces a function's stored body text verbatim, so
-- any accidental divergence here would fail that job.
--
-- Unlike the control tree, this database has no `coralswarm_app`/RLS
-- privilege-separation model at all (same reasoning as
-- `migrations_tenant/0003_bootstrap_scope_functions.sql`) — org-tenancy
-- isolates tenants at the WHOLE-DATABASE level, not via row-level security
-- within a shared database, so there is no lower-privilege role here to
-- `GRANT EXECUTE` to. Still `SECURITY DEFINER` for exact parity with the
-- control tree (harmless here: whatever role the app connects to a tenant
-- DB with already has full CRUD on every table in it).
--
-- Depends on: 0001_tenant_baseline (graphs, teams, team_members),
--             0005_member_ocean_columns (graphs.owner_user_id,
--             graphs.archived_at, graphs_kind_check widened to admit
--             'member').
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) FROM PUBLIC;
--   REVOKE EXECUTE ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid, uuid, uuid) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_tenant_reachable_graphs(uuid, uuid);
--   DROP FUNCTION IF EXISTS resolve_tenant_graph_reachable_by_user(uuid, uuid, uuid);

SET LOCAL lock_timeout = '3s';

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
-- Same defense-in-depth reasoning as 0003's Grants section: Postgres grants
-- EXECUTE to PUBLIC by default on new functions, and both of these are
-- `SECURITY DEFINER`. There is no lower-privilege role in this tenant DB
-- today, but revoking the PUBLIC default costs nothing (the object OWNER
-- always retains implicit EXECUTE regardless of any REVOKE) and closes this
-- exposure in advance of any future lower-privilege tenant-DB role.
REVOKE EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid, uuid, uuid) FROM PUBLIC;
