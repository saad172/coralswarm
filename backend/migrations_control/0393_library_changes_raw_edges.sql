-- PAIRED WITH: tenant 0208 (library_changes_raw_edges).
-- Cost admission for the busy-graph overlay (#1524 blocker 2, design
-- docs/plans/library-members-busy-design/design.md section 3.3). The same
-- guard as library_evidence_raw_edges_safe (tenant 0195 / control 0365),
-- sized by the caller's distinct-key budgets instead of 0195's fixed 128
-- change rows, 64 nodes and 512 raw edges: the reader now admits up to 512
-- distinct object keys and 256 distinct nodes (crate::library_changes).
-- Additive: 0195 is unchanged; the Library reader no longer calls it.
-- Never returns evidence, identifiers or raw counts, only one boolean.
-- Canonical evidence/permission readers remain SECURITY INVOKER and unchanged.
-- Raw links are counted with row security off: links from nodes of a graph
-- the reader cannot see are still physical work for the reader's inverse
-- probe. Ordered existing indexes plus function-local planner settings keep
-- the physical early stop; all caller planner/RLS settings restore on return.
CREATE FUNCTION public.library_changes_raw_edges_safe(graph_ids UUID[],change_ids BIGINT[],node_ids UUID[],edge_limit INTEGER)
RETURNS BOOLEAN LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp SET row_security=off
SET enable_nestloop=on SET enable_mergejoin=on
SET enable_seqscan=off SET enable_bitmapscan=off
SET enable_indexscan=on SET enable_indexonlyscan=on SET jit=off AS $$
DECLARE graph UUID; safe BOOLEAN;
BEGIN
 IF graph_ids IS NULL OR change_ids IS NULL OR node_ids IS NULL OR edge_limit IS NULL
  OR cardinality(graph_ids) NOT BETWEEN 1 AND 32 OR cardinality(change_ids)>4096 OR cardinality(node_ids)>4096
  OR edge_limit NOT BETWEEN 1 AND 65536
  OR (cardinality(change_ids)>0 AND cardinality(node_ids)>0)
  OR array_position(graph_ids,NULL) IS NOT NULL OR array_position(change_ids,NULL) IS NOT NULL OR array_position(node_ids,NULL) IS NOT NULL THEN
  RETURN false;
 END IF;
 -- Delegate graph/account/member/history-window admission. This function
 -- deliberately uses the caller's transaction-local account/user/actor GUCs;
 -- its elevated current_user never substitutes for a content identity.
 FOREACH graph IN ARRAY graph_ids LOOP
  IF NOT COALESCE(public.library_evidence_shareable(graph),false) THEN RETURN false; END IF;
 END LOOP;
 -- An OLD graph key can outlive a moved/deleted object. Do not call its
 -- inverse fanout safe merely because the current object is absent here.
 -- Missing/mismatched historical keys conservatively take the live path.
 IF EXISTS(SELECT 1 FROM public.library_evidence_changes c WHERE c.id=ANY(change_ids) AND c.graph_id=ANY(graph_ids)
  AND ((c.atom_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.atoms a WHERE a.atom_id=c.atom_id AND a.graph_id=c.graph_id))
    OR (c.semantic_atom_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.semantic_atoms a WHERE a.semantic_atom_id=c.semantic_atom_id AND a.graph_id=c.graph_id)))) THEN RETURN false; END IF;
 IF cardinality(change_ids)>0 THEN
  WITH keys AS MATERIALIZED (
   SELECT c.* FROM public.library_evidence_changes c WHERE c.id=ANY(change_ids) AND c.graph_id=ANY(graph_ids)
  ), raw AS MATERIALIZED (
   SELECT 1 FROM keys c CROSS JOIN LATERAL (
    (SELECT 1 FROM public.node_atoms l WHERE l.atom_id=c.atom_id
      AND EXISTS(SELECT 1 FROM public.atoms a WHERE a.atom_id=c.atom_id AND a.graph_id=c.graph_id) ORDER BY l.node_id LIMIT edge_limit+1)
    UNION ALL
    (SELECT 1 FROM public.node_semantic_atoms l WHERE l.semantic_atom_id=c.semantic_atom_id AND l.graph_id=c.graph_id
      AND EXISTS(SELECT 1 FROM public.semantic_atoms a WHERE a.semantic_atom_id=c.semantic_atom_id AND a.graph_id=c.graph_id) ORDER BY l.node_id LIMIT edge_limit+1)
    UNION ALL
    (SELECT 1 FROM public.library_node_item_evidence l WHERE l.graph_id=c.graph_id AND l.source_item_id=c.source_item_id ORDER BY l.node_id LIMIT edge_limit+1)
   ) edges LIMIT edge_limit+1
  ) SELECT count(*)<=edge_limit INTO safe FROM raw;
 ELSE
  WITH owned_nodes AS MATERIALIZED (
   SELECT n.node_id,n.graph_id FROM public.nodes n WHERE n.node_id=ANY(node_ids) AND n.graph_id=ANY(graph_ids)
  ), raw AS MATERIALIZED (
   SELECT 1 FROM owned_nodes n CROSS JOIN LATERAL (
    (SELECT 1 FROM public.node_atoms l WHERE l.node_id=n.node_id ORDER BY l.atom_id LIMIT edge_limit+1)
    UNION ALL
    (SELECT 1 FROM public.node_semantic_atoms l WHERE l.node_id=n.node_id AND l.graph_id=n.graph_id ORDER BY l.semantic_atom_id LIMIT edge_limit+1)
   ) edges LIMIT edge_limit+1
  ) SELECT count(*)<=edge_limit INTO safe FROM raw;
 END IF;
 RETURN safe;
END $$;
REVOKE ALL ON FUNCTION public.library_changes_raw_edges_safe(UUID[],BIGINT[],UUID[],INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.library_changes_raw_edges_safe(UUID[],BIGINT[],UUID[],INTEGER) TO coralswarm_app;
