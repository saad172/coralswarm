-- Bridge until OPS-4-T: production tenant DSNs still connect as the tables'
-- OWNER (tenancy::registry), and migrate_tenants uses that same DSN. Every
-- tenant privilege is granted to coralswarm_tenant_app, so any object owned by
-- someone else (0155's coralswarm_managed_clock function) is unusable by the
-- owner runtime: production standing adoption failed with 42501 on 2026-10-18.
-- Make the owner runtime an INHERIT member of coralswarm_tenant_app so every
-- current and future app-role grant applies to it. INHERIT never changes
-- current_user, so the owner keeps its table-owner/BYPASSRLS exemption from
-- RLS. SET FALSE: the runtime gains no SET ROLE path. Nothing is granted to
-- PUBLIC and no app role gains a path to the owner or the clock owner.
-- Fallback when this migrator holds no ADMIN on coralswarm_tenant_app (it
-- created the role in 0009, so it should): grant the one known gap directly,
-- acting as the clock owner exactly as 0155 transferred ownership.
DO $$
DECLARE
 migrator oid := (SELECT oid FROM pg_roles WHERE rolname=current_user);
 table_owner name := (SELECT pg_get_userbyid(relowner) FROM pg_class
   WHERE oid='public.corpus_capture_admissions'::regclass);
 clock regprocedure := 'public.corpus_standing_receipt_event(uuid,uuid,text,text)'::regprocedure;
 runtime name;
 prior_self_grant pg_auth_members;
 temporary_set boolean;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
  RAISE EXCEPTION USING ERRCODE='42704',MESSAGE='coralswarm_tenant_app is mandatory on the tenant plane (0009)';
 END IF;
 FOREACH runtime IN ARRAY ARRAY(SELECT DISTINCT r FROM unnest(ARRAY[pg_get_userbyid(migrator),table_owner]) r) LOOP
  CONTINUE WHEN runtime IN ('coralswarm_tenant_app','coralswarm_app','coralswarm_managed_clock')
   OR (SELECT rolsuper FROM pg_roles WHERE rolname=runtime)
   OR pg_has_role(runtime,'coralswarm_tenant_app','USAGE');
  IF pg_has_role(current_user,'coralswarm_tenant_app','MEMBER WITH ADMIN OPTION') THEN
   EXECUTE format('GRANT coralswarm_tenant_app TO %I WITH INHERIT TRUE, SET FALSE',runtime);
   RAISE NOTICE 'owner runtime % inherits coralswarm_tenant_app',runtime;
  ELSE
   temporary_set := NOT pg_has_role(current_user,'coralswarm_managed_clock','SET');
   IF temporary_set THEN
    IF NOT pg_has_role(current_user,'coralswarm_managed_clock','MEMBER WITH ADMIN OPTION') THEN
     RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='owner runtime needs ADMIN on coralswarm_tenant_app or coralswarm_managed_clock; provision that owner edge out of band, never grant either role to an app role';
    END IF;
    SELECT * INTO prior_self_grant FROM pg_auth_members
     WHERE roleid='coralswarm_managed_clock'::regrole AND member=migrator AND grantor=migrator;
    EXECUTE format('GRANT coralswarm_managed_clock TO %I WITH SET TRUE, INHERIT FALSE',current_user);
   END IF;
   SET LOCAL ROLE coralswarm_managed_clock;
   EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %I',clock,runtime);
   SET LOCAL ROLE NONE;
   IF temporary_set THEN
    IF prior_self_grant.member IS NULL THEN
     EXECUTE format('REVOKE coralswarm_managed_clock FROM %I GRANTED BY %I',current_user,current_user);
    ELSE
     EXECUTE format('GRANT coralswarm_managed_clock TO %I WITH ADMIN %s, INHERIT %s, SET %s',
      current_user,CASE WHEN prior_self_grant.admin_option THEN 'TRUE' ELSE 'FALSE' END,
      CASE WHEN prior_self_grant.inherit_option THEN 'TRUE' ELSE 'FALSE' END,
      CASE WHEN prior_self_grant.set_option THEN 'TRUE' ELSE 'FALSE' END);
    END IF;
   END IF;
   RAISE WARNING 'owner runtime % lacks ADMIN on coralswarm_tenant_app; granted only the managed clock directly',runtime;
  END IF;
  IF NOT has_function_privilege(runtime,clock,'EXECUTE') THEN
   RAISE EXCEPTION 'owner runtime % still cannot execute the managed clock',runtime;
  END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a WHERE p.oid=clock AND a.grantee=0) THEN
  RAISE EXCEPTION 'managed clock must not be executable by PUBLIC';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname IN ('coralswarm_app','coralswarm_tenant_app')
   AND (pg_has_role(r.oid,'coralswarm_managed_clock','MEMBER') OR pg_has_role(r.oid,table_owner,'MEMBER'))) THEN
  RAISE EXCEPTION 'app roles must not reach the clock owner or the table owner';
 END IF;
END $$;
