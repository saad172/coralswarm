-- Keep the canonical permission predicate and its indexed per-atom probes.
-- The bounded evidence statement hashes its materialized keyed input sets;
-- those planner settings must not propagate into the canonical function's
-- correlated mapping lookups. Function-local settings restore on return.
CREATE OR REPLACE FUNCTION public.library_evidence_permitted_atoms(g UUID,atom_ids UUID[])
RETURNS TABLE(atom_id UUID) LANGUAGE SQL STABLE SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp
SET enable_nestloop=on SET enable_mergejoin=on SET enable_sort=on AS $$
 SELECT p.atom_id FROM public.library_permitted_atoms(g,atom_ids) p
$$;
-- The workspace-card extraction-generation view remains authoritative;
-- only its keyed lookup plan is isolated from the outer hash policy.
CREATE OR REPLACE FUNCTION public.library_evidence_served_outcomes(g UUID,outcome_ids UUID[])
RETURNS TABLE(semantic_atom_id UUID) LANGUAGE SQL STABLE SECURITY INVOKER
SET search_path=pg_catalog,public,pg_temp
SET enable_nestloop=on SET enable_mergejoin=on SET enable_sort=on AS $$
 SELECT a.semantic_atom_id FROM public.served_semantic_atoms a
 WHERE a.graph_id=g AND a.semantic_atom_id=ANY(outcome_ids)
$$;
REVOKE ALL ON FUNCTION public.library_evidence_permitted_atoms(UUID,UUID[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.library_evidence_served_outcomes(UUID,UUID[]) FROM PUBLIC;
DO $$DECLARE r TEXT; BEGIN
 FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN
   EXECUTE format('GRANT EXECUTE ON FUNCTION public.library_evidence_permitted_atoms(UUID,UUID[]),public.library_evidence_served_outcomes(UUID,UUID[]) TO %I',r);
  END IF;
 END LOOP;
END$$;

-- Dirty resolution shares the refresh/read transaction after a hash-planned
-- evidence statement. Fence the visibility check as an indexed per-candidate
-- EXISTS: pulling it up can scan/join every hub edge before ORDER/LIMIT.
-- The unique nodes key makes EXISTS equivalent to the original inner join;
-- RLS, graph filtering, cursor order and exact consumed keys are unchanged.
-- Stale small-table statistics must not replace the ordered inverse probe with
-- a bitmap/heap scan plus sort after a hub grows. This indexed planner policy
-- is function-local and restores every caller setting on return. RLS can still
-- inspect its visible-node set or reject hidden physical links before LIMIT.
CREATE OR REPLACE FUNCTION public.library_evidence_change_nodes(g UUID,change_id BIGINT,after_node UUID DEFAULT NULL,node_limit INTEGER DEFAULT 64)
RETURNS TABLE(node_id UUID) LANGUAGE sql STABLE SECURITY INVOKER SET search_path=public,pg_temp
SET enable_nestloop=on SET enable_mergejoin=on SET enable_seqscan=off SET enable_bitmapscan=off
SET enable_indexscan=on SET enable_indexonlyscan=on SET enable_sort=on AS $$
 WITH c AS MATERIALIZED (SELECT * FROM library_evidence_changes WHERE graph_id=g AND id=change_id),
 keys AS (
  (SELECT c.node_id FROM c WHERE c.node_id IS NOT NULL AND (after_node IS NULL OR c.node_id>after_node))
  UNION
  (SELECT l.node_id FROM c CROSS JOIN LATERAL (
   SELECT a.node_id FROM node_atoms a
   WHERE a.atom_id=c.atom_id AND a.node_id>=COALESCE(after_node,'00000000-0000-0000-0000-000000000000'::uuid) AND (after_node IS NULL OR a.node_id>after_node)
   AND EXISTS(SELECT 1 FROM nodes n WHERE n.node_id=a.node_id AND n.graph_id=c.graph_id OFFSET 0)
   ORDER BY a.node_id LIMIT LEAST(GREATEST(node_limit,0),4096)) l)
  UNION
  (SELECT l.node_id FROM c CROSS JOIN LATERAL (
   SELECT a.node_id FROM node_semantic_atoms a WHERE a.semantic_atom_id=c.semantic_atom_id AND a.graph_id=c.graph_id
   AND a.node_id>=COALESCE(after_node,'00000000-0000-0000-0000-000000000000'::uuid) AND (after_node IS NULL OR a.node_id>after_node) ORDER BY a.node_id LIMIT LEAST(GREATEST(node_limit,0),4096)) l)
  UNION
  (SELECT l.node_id FROM c CROSS JOIN LATERAL (
   SELECT a.node_id FROM library_node_item_evidence a WHERE a.source_item_id=c.source_item_id AND a.graph_id=c.graph_id
   AND a.node_id>=COALESCE(after_node,'00000000-0000-0000-0000-000000000000'::uuid) AND (after_node IS NULL OR a.node_id>after_node) ORDER BY a.node_id LIMIT LEAST(GREATEST(node_limit,0),4096)) l)
 ) SELECT keys.node_id FROM keys ORDER BY keys.node_id LIMIT LEAST(GREATEST(node_limit,0),4096)
$$;
