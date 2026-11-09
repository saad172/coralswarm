-- PAIRED WITH: tenant 0206 (library_access_class).
-- Library reader access class (#1524 blocker 1). Additive: two functions, no
-- table, policy, grant-of-role or data change. Read tenant 0206's header
-- first; this twin differs from it in two places only:
--
-- - `library_evidence_class(g)`: `history_floor` is a literal NULL. The
--   control plane has no history windows (0184's header) and must not call a
--   tenant-only function (requirement C4); `library_evidence_shareable`
--   (0354) has no floor term here either.
-- - `correction_visible` is 0354's control expression, which also needs the
--   graph to be reachable by `app.user_id`.
--
-- The digest body is byte-identical to tenant 0206. It hashes this plane's
-- own catalog, so each plane has its own reviewed constant
-- (`read_access::CONTROL_CONTRACT_DIGEST`).

CREATE FUNCTION public.library_access_contract_digest() RETURNS text
 LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
 WITH tables AS MATERIALIZED (
  SELECT c.oid,c.relname::text relname,c.relkind,c.relrowsecurity,c.relforcerowsecurity
  FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f')
   AND (c.relname IN ('graphs','nodes','relationship_edge','node_atoms','atoms','source_items','source_versions',
     'source_version_chunks','semantic_atoms','semantic_atom_passages','node_semantic_atoms','reef_identity_corrections','jobs')
    OR c.relname LIKE 'library\_evidence\_%' OR c.relname LIKE 'library\_listing\_%' OR c.relname LIKE 'library\_node\_%')
 ), functions AS MATERIALIZED (
  SELECT p.oid::regprocedure::text signature,p.prosecdef,p.provolatile,
   COALESCE(array_to_string(p.proconfig,','),'-') config,
   encode(sha256(convert_to(p.prosrc,'UTF8')),'hex') source
  FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
  WHERE n.nspname='public'
   AND (p.proname IN ('library_permitted_atoms','reef_identity_support_valid','library_evidence_shareable','library_evidence_class',
     'library_access_contract_digest','reader_history_floors','history_floor_admits','resolve_graph_reachable_by_user')
    OR p.proname LIKE 'tenant\_rls\_%' OR p.proname LIKE 'library\_node\_title%')
 ), inventory(line) AS (
  SELECT 'library-access-contract-v1'
  UNION ALL
  SELECT format('T|%s|%s|%s|%s',t.relname,t.relkind,t.relrowsecurity,t.relforcerowsecurity) FROM tables t
  UNION ALL
  SELECT format('P|%s|%s|%s|%s|%s|%s|%s',t.relname,p.polname,p.polcmd,p.polpermissive,
   (SELECT string_agg(r.name,',' ORDER BY r.name COLLATE "C") FROM (
     SELECT CASE WHEN x=0 THEN 'public' ELSE pg_catalog.pg_get_userbyid(x)::text END name FROM unnest(p.polroles) x) r),
   COALESCE(pg_catalog.pg_get_expr(p.polqual,p.polrelid),'-'),COALESCE(pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid),'-'))
  FROM pg_catalog.pg_policy p JOIN tables t ON t.oid=p.polrelid
  UNION ALL
  SELECT format('F|%s|%s|%s|%s|%s',f.signature,f.prosecdef,f.provolatile,f.config,f.source) FROM functions f
 )
 SELECT encode(sha256(convert_to(string_agg(line,E'\n' ORDER BY line COLLATE "C"),'UTF8')),'hex') FROM inventory
$$;
REVOKE ALL ON FUNCTION public.library_access_contract_digest() FROM PUBLIC;

CREATE FUNCTION public.library_evidence_class(g UUID) RETURNS JSONB
 LANGUAGE sql STABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT jsonb_build_object('schema_version',2,
  'account_id',NULLIF(current_setting('app.account_id',true),''),
  'history_floor',NULL::timestamp,
  'correction_visible',CASE WHEN row_security_active('public.reef_identity_corrections'::regclass)
   THEN EXISTS(SELECT 1 FROM public.graphs x WHERE x.id=$1 AND x.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid
    AND COALESCE(public.resolve_graph_reachable_by_user($1,NULLIF(current_setting('app.user_id',true),'')::uuid),false))
   ELSE true END,
  'policy_digest',public.library_access_contract_digest())
$$;
REVOKE ALL ON FUNCTION public.library_evidence_class(UUID) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION public.library_access_contract_digest(),public.library_evidence_class(UUID) TO coralswarm_app;
GRANT EXECUTE ON FUNCTION public.library_access_contract_digest(),public.library_evidence_class(UUID) TO CURRENT_USER;
