-- 0003_bootstrap_scope_functions.sql — paired half of
-- `migrations_control/0084_bootstrap_scope_functions.sql` (org-tenancy Phase
-- S). See that migration's header for the full rationale.
--
-- Only 2 of that migration's 5 functions are paired here.
-- `Store::graph_account_id` and `workers::embed::resolve_dek_for_atoms` are
-- called against EITHER plane's pool (`graphs`/`atoms` are shared
-- tenant-tree tables, present in this baseline too), so both functions must
-- exist here too or a tenant-pool call to either Rust function breaks the
-- moment its SQL body is swapped to call them. The other 3 control-tree
-- functions (`resolve_oauth_account_id`, `resolve_reachable_graphs`,
-- `resolve_graph_reachable_by_user`) are control-plane-only in PURPOSE (their
-- Rust callers only ever run against the shared consumer pool) and, for the
-- latter two, control-plane-only by NECESSITY (`account_members` is not a
-- tenant-tree table at all) — they are deliberately NOT paired here, and are
-- registered instead in `scripts/check_schema_parity.sh`'s
-- `CONTROL_FUNCTIONS` ignore list so the parity job doesn't flag the
-- asymmetry as drift.
--
-- The two bodies below are kept BYTE-IDENTICAL to their control-tree
-- counterparts (only comments/header prose differ) — `check_schema_parity.sh`
-- diffs `pg_dump --schema-only` output, which reproduces a function's stored
-- body text verbatim, so any accidental divergence here would fail that job.
--
-- Unlike the control tree, this database has no `coralswarm_app`/RLS
-- privilege-separation model at all — the org-tenancy design isolates
-- tenants at the WHOLE-DATABASE level (one Postgres database per org),
-- not via row-level security within a shared database, so there is no
-- lower-privilege role here to `GRANT EXECUTE` to, and no scoping deadlock
-- these functions need to "break" the way their control-tree counterparts
-- do. They exist here purely so `Store::graph_account_id`/
-- `resolve_dek_for_atoms`'s SQL body is IDENTICAL regardless of which plane's
-- pool it runs against — no per-plane branching needed in Rust. Still
-- `SECURITY DEFINER` for exact parity with the control tree (harmless here:
-- whatever role the app connects to a tenant DB with already has full CRUD
-- on every table in it, so `SECURITY DEFINER` changes nothing observable).
--
-- Depends on: 0001_tenant_baseline (graphs, atoms)
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_graph_account_id(uuid) FROM PUBLIC;
--   REVOKE EXECUTE ON FUNCTION resolve_atoms_account_id(uuid[]) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_graph_account_id(uuid);
--   DROP FUNCTION IF EXISTS resolve_atoms_account_id(uuid[]);

CREATE OR REPLACE FUNCTION resolve_graph_account_id(p_graph_id uuid)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id FROM graphs WHERE id = p_graph_id
$$;

CREATE OR REPLACE FUNCTION resolve_atoms_account_id(p_atom_ids uuid[])
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT DISTINCT g.account_id
    FROM graphs g
    JOIN atoms at ON at.graph_id = g.id
    WHERE at.atom_id = ANY(p_atom_ids)
$$;

-- ── Grants ────────────────────────────────────────────────────────────────
-- Same defense-in-depth reasoning as the control tree's paired functions
-- (CodeRabbit review of this PR, MAJOR — see 0084's Grants section for the
-- full rationale): Postgres grants EXECUTE to PUBLIC by default on new
-- functions, and both of these are `SECURITY DEFINER` bypassing whatever
-- table-level privileges the CALLER has. There is no lower-privilege role in
-- this tenant DB TODAY (the org-tenancy design isolates tenants at the
-- whole-database level, not via a restricted in-database role the way
-- `coralswarm_app` restricts the control/consumer plane) — but revoking the
-- PUBLIC default costs nothing (the object OWNER always retains implicit
-- EXECUTE regardless of any REVOKE, so today's single tenant-DB connection
-- role is completely unaffected) and closes this exposure in advance of any
-- future lower-privilege role ever being added to a tenant DB, rather than
-- relying on "no such role exists yet" as the only protection.
REVOKE EXECUTE ON FUNCTION resolve_graph_account_id(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_atoms_account_id(uuid[]) FROM PUBLIC;
