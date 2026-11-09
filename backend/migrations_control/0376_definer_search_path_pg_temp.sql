-- PAIRED WITH: tenant 0198_definer_search_path_pg_temp (byte-identical body).
--
-- Every SECURITY DEFINER routine in schema public ends its search_path with
-- pg_temp.
--
-- Why. When pg_temp is not named in a search_path, PostgreSQL searches the
-- session's temporary schema FIRST for relation and type names, ahead of
-- pg_catalog. A session holding app-role SQL (an injection, a compromised
-- worker) can create temporary objects, so it can make a definer resolve an
-- unqualified name to an object the session controls while the definer runs
-- with its owner's privileges. Functions are never looked up in pg_temp, but
-- types are: a temporary DOMAIN named `text` or `uuid` with a CHECK that calls
-- an explicitly-qualified pg_temp function runs that function as the definer's
-- owner the moment a PL/pgSQL variable of that type is initialised. Measured
-- before this migration: `coralswarm_app` reached `current_user = postgres`
-- through `library_stage_unattached_upload(uuid)`'s `status TEXT` variable.
-- backend/tests/definer_search_path_pg.rs reproduces it on both planes.
--
-- What. Definers whose search_path already ends in pg_temp are left alone
-- (most of the tree: `public, pg_temp`). Every other definer keeps its
-- existing schema list in its existing order, with any earlier pg_temp entry
-- removed and pg_temp appended last. A definer with no search_path at all gets
-- `pg_catalog, public, pg_temp`. Catalog-driven rather than hand-listed, so the
-- result does not depend on which CREATE OR REPLACE last ran on a given
-- database, and re-running it changes nothing.
--
-- Ownership. ALTER ROUTINE needs the owner's privileges. Some definers are
-- owned by dedicated NOLOGIN roles (coralswarm_managed_clock on both planes,
-- coralswarm_billing_entry_owner on control). A migrator that holds the
-- owner's privileges alters directly; one that only holds ADMIN on the owner
-- (a non-superuser tenant owner on Neon) takes the same temporary SET grant
-- 0346/tenant 0178 take, acts as the owner, and restores its prior grant. A
-- migrator with neither fails the migration: leaving a hijackable definer in
-- place silently is the outcome this file exists to prevent.
--
-- New definers. A later CREATE [OR REPLACE] FUNCTION replaces the SET clause,
-- so this migration does not protect a definer created or redefined after it.
-- scripts/check_definer_search_path.sh applies both trees to a fresh database
-- and fails the deploy (and nightly CI) if any definer lacks a trailing
-- pg_temp. Write `SET search_path = pg_catalog, public, pg_temp` on every new
-- SECURITY DEFINER function.
DO $harden$
DECLARE
 f record;
 elems text[];
 new_path text;
 alter_sql text;
 migrator oid := (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user);
 prior pg_catalog.pg_auth_members;
 temporary_set boolean;
 hardened integer := 0;
 already integer := 0;
 remaining text;
BEGIN
 FOR f IN
  SELECT p.oid,
         p.oid::pg_catalog.regprocedure::text AS sig,
         p.proowner,
         pg_catalog.pg_get_userbyid(p.proowner) AS owner_name,
         (SELECT pg_catalog.substr(c, 13) FROM pg_catalog.unnest(p.proconfig) c
           WHERE c LIKE 'search_path=%') AS sp
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.prosecdef
     AND n.nspname = 'public'
     AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                      WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                        AND d.objid = p.oid AND d.deptype = 'e')
   ORDER BY p.oid
 LOOP
  IF f.sp IS NULL THEN
   elems := ARRAY['pg_catalog', 'public'];
  ELSE
   -- proconfig stores the list flattened, e.g. `pg_catalog, public` or
   -- `"$user", public`. Split, trim, unquote; drop empty entries.
   SELECT COALESCE(pg_catalog.array_agg(e ORDER BY ord), '{}'::text[]) INTO elems
     FROM (SELECT pg_catalog.replace(pg_catalog.btrim(pg_catalog.btrim(x), '"'), '""', '"') AS e, ord
             FROM pg_catalog.unnest(pg_catalog.string_to_array(f.sp, ',')) WITH ORDINALITY AS u(x, ord)) s
    WHERE e <> '';
   IF pg_catalog.cardinality(elems) > 0 AND elems[pg_catalog.cardinality(elems)] = 'pg_temp' THEN
    already := already + 1;
    CONTINUE;
   END IF;
  END IF;
  elems := pg_catalog.array_remove(elems, 'pg_temp') || 'pg_temp'::text;
  SELECT pg_catalog.string_agg(pg_catalog.quote_ident(e), ', ' ORDER BY ord) INTO new_path
    FROM pg_catalog.unnest(elems) WITH ORDINALITY AS u(e, ord);
  alter_sql := pg_catalog.format('ALTER ROUTINE %s SET search_path = %s', f.sig, new_path);

  IF pg_catalog.pg_has_role(current_user, f.proowner, 'USAGE') THEN
   EXECUTE alter_sql;
  ELSE
   temporary_set := NOT pg_catalog.pg_has_role(current_user, f.proowner, 'SET');
   IF temporary_set THEN
    IF NOT pg_catalog.pg_has_role(current_user, f.proowner, 'MEMBER WITH ADMIN OPTION') THEN
     RAISE EXCEPTION USING ERRCODE = '42501',
      MESSAGE = pg_catalog.format('cannot pin search_path on %s: the migrator holds neither the privileges of, SET on, nor ADMIN on its owner %s', f.sig, f.owner_name);
    END IF;
    prior := NULL;
    SELECT * INTO prior FROM pg_catalog.pg_auth_members
     WHERE roleid = f.proowner AND member = migrator AND grantor = migrator;
    EXECUTE pg_catalog.format('GRANT %I TO %I WITH SET TRUE, INHERIT FALSE', f.owner_name, current_user);
   END IF;
   EXECUTE pg_catalog.format('SET LOCAL ROLE %I', f.owner_name);
   EXECUTE alter_sql;
   SET LOCAL ROLE NONE;
   IF temporary_set THEN
    IF prior.member IS NULL THEN
     EXECUTE pg_catalog.format('REVOKE %I FROM %I GRANTED BY %I', f.owner_name, current_user, current_user);
    ELSE
     EXECUTE pg_catalog.format('GRANT %I TO %I WITH ADMIN %s, INHERIT %s, SET %s',
      f.owner_name, current_user,
      CASE WHEN prior.admin_option THEN 'TRUE' ELSE 'FALSE' END,
      CASE WHEN prior.inherit_option THEN 'TRUE' ELSE 'FALSE' END,
      CASE WHEN prior.set_option THEN 'TRUE' ELSE 'FALSE' END);
    END IF;
   END IF;
  END IF;
  IF (SELECT proowner FROM pg_catalog.pg_proc WHERE oid = f.oid) <> f.proowner THEN
   RAISE EXCEPTION 'pinning search_path changed the owner of %', f.sig;
  END IF;
  hardened := hardened + 1;
 END LOOP;

 -- Postcondition: no definer in public is left without a trailing pg_temp.
 SELECT pg_catalog.string_agg(p.oid::pg_catalog.regprocedure::text, ', ') INTO remaining
   FROM pg_catalog.pg_proc p
   JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
  WHERE p.prosecdef AND n.nspname = 'public'
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                     WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                       AND d.objid = p.oid AND d.deptype = 'e')
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(p.proconfig) c
                     WHERE c ~ '^search_path=(.*,)?\s*"?pg_temp"?\s*$');
 IF remaining IS NOT NULL THEN
  RAISE EXCEPTION 'definers still lack a trailing pg_temp: %', remaining;
 END IF;
 RAISE NOTICE 'definer search_path: % pinned with trailing pg_temp, % already pinned', hardened, already;
END
$harden$;
