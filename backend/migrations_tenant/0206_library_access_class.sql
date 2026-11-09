-- PAIRED WITH: control 0391 (library_access_class).
-- Library reader access class (#1524 blocker 1). Additive: two functions, no
-- table, policy, grant-of-role or data change.
--
-- The accelerated Library reader used to admit a cached graph only when the
-- reader's `library_evidence_context` (0185) was byte-equal to the worker's.
-- That fingerprint carries `current_user` and eight `row_security_active`
-- flags, so any reader whose role differs from the worker's was refused,
-- whatever it could actually see. The owner connection stays (founder
-- decision on #1524, 2026-10-28): member access is enforced in code, and the
-- cache compares an access CLASS, the facts that decide what a reader sees
-- of one graph:
--
--   {schema_version: 2, account_id, history_floor, correction_visible,
--    policy_digest}
--
-- 1. `library_access_contract_digest()`: sha256 over the RLS contract the
--    equivalence argument rests on: `relkind`/`relrowsecurity`/
--    `relforcerowsecurity` and every `pg_policy` row of the Library input
--    tables and every `library_evidence_*`/`library_listing_*`/
--    `library_node_*` table, and the definition (`prosrc`, `prosecdef`,
--    `provolatile`, `proconfig`) of the visibility, floor, permission and
--    correction functions. Names, not OIDs, and C-collated ordering, so a
--    migrated database gives the same text on every cluster. The binary
--    carries the reviewed digest per plane (`read_access::TENANT_CONTRACT_DIGEST`,
--    `CONTROL_CONTRACT_DIGEST`, checked by
--    `scripts/check_library_access_contract.sh`); a
--    migration that changes any of these objects changes the digest, and
--    the reader falls back to Live (`access_contract_changed`) until a human
--    re-reviews the equivalence and updates that constant. SECURITY DEFINER
--    with a pinned search path, so the deparsed policy text does not depend
--    on the caller's path.
--
-- 2. `library_evidence_class(g)`: the calling reader's class for graph g.
--    `history_floor` is the reader's floor from
--    `tenant_rls_visible_graph_windows()` (0064), the same source as
--    `library_evidence_shareable` (0185), rendered in UTC so the session
--    time zone cannot change the bytes. `correction_visible` is 0185's own
--    expression. SECURITY INVOKER on purpose: `row_security_active` must be
--    evaluated for the reader, not for the function owner.
--
-- No `SET ROLE` path and no role grant change (0159 stays SET FALSE). Both
-- functions are granted to the app role and explicitly to the migrating
-- role; production tenant requests and workers connect as that owner.

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
  'history_floor',(SELECT w.history_floor AT TIME ZONE 'UTC' FROM public.tenant_rls_visible_graph_windows() w WHERE w.graph_id=$1),
  'correction_visible',CASE WHEN row_security_active('public.reef_identity_corrections'::regclass)
   THEN EXISTS(SELECT 1 FROM public.graphs x WHERE x.id=$1 AND x.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid)
   ELSE true END,
  'policy_digest',public.library_access_contract_digest())
$$;
REVOKE ALL ON FUNCTION public.library_evidence_class(UUID) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION public.library_access_contract_digest(),public.library_evidence_class(UUID) TO coralswarm_tenant_app;
GRANT EXECUTE ON FUNCTION public.library_access_contract_digest(),public.library_evidence_class(UUID) TO CURRENT_USER;
