-- Migration 0092: pin search_path to the Postgres default on every tenant
-- role and on the database itself.
--
-- 2026-09-19 prod: Neon transaction-pooler backends on the coralswarm
-- tenant served an empty search_path. Unqualified `atoms` / `jobs` /
-- `resolve_tenant_reachable_graphs` then failed with "does not exist"
-- against a fully intact public schema. Control-plane schema_version was
-- already 91, so migrate_tenants and the tenant_harness version gate
-- both treated the DB as healthy. Re-running the tenant tree or restoring
-- a backup would have been destructive.
--
-- The DSN cannot carry this. Neon PgBouncer rejects
-- `options=-c search_path=...` as an unsupported startup parameter
-- (same class as `app.tenant_actor`, PR #388). Role / database defaults
-- are the only setting that new pooled server backends inherit. Existing
-- backends pick it up when PgBouncer recycles them (~90s in the incident).
--
-- `"$user", public` is Postgres's own default, not a privilege change.
-- `$user` is skipped when that schema does not exist (it does not, on
-- either current tenant). SECURITY DEFINER functions keep their own
-- `SET search_path = public, pg_temp` pins.
--
-- Role-guarded: `neondb_owner` is Neon-specific; local / parity DBs may
-- use a different owner. Always pin CURRENT_USER (the migrator) and the
-- database. Pin `coralswarm_tenant_app` / `neondb_owner` when that ALTER
-- is allowed. A third-party-created app role cannot be ALTER'd by a
-- deprivileged owner (0009 gate 2 / `tenant_rls_role_provisioning_pg`);
-- `ALTER DATABASE SET` still pins the default every role inherits unless
-- it has its own search_path setting.
--
-- Down:
--   ALTER ROLE <owner> IN DATABASE <db> RESET search_path;
--   ALTER ROLE coralswarm_tenant_app IN DATABASE <db> RESET search_path;
--   ALTER DATABASE <db> RESET search_path;

DO $$
DECLARE
    db_name text := current_database();
BEGIN
    EXECUTE format(
        'ALTER ROLE %I IN DATABASE %I SET search_path = %s',
        current_user,
        db_name,
        '"$user", public'
    );

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
        BEGIN
            EXECUTE format(
                'ALTER ROLE coralswarm_tenant_app IN DATABASE %I SET search_path = %s',
                db_name,
                '"$user", public'
            );
        EXCEPTION WHEN insufficient_privilege THEN
            NULL;
        END;
    END IF;

    -- Neon DSNs connect as neondb_owner even when CURRENT_USER during
    -- migrate_tenants is that same role; name it explicitly so a future
    -- migrator role still pins the live app DSN user.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'neondb_owner')
       AND current_user <> 'neondb_owner' THEN
        BEGIN
            EXECUTE format(
                'ALTER ROLE neondb_owner IN DATABASE %I SET search_path = %s',
                db_name,
                '"$user", public'
            );
        EXCEPTION WHEN insufficient_privilege THEN
            NULL;
        END;
    END IF;

    EXECUTE format(
        'ALTER DATABASE %I SET search_path = %s',
        db_name,
        '"$user", public'
    );
END
$$;
