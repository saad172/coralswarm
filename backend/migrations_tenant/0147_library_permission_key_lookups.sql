-- Library permission checks scanned whole tables per call (COR-83).
-- library_edge_permitted joined provenance ids as text, so the planner ran
-- library_atom_permitted on every atom in the graph, and hashed every edge in
-- the graph to resolve source_edge_ids. It now parses ids once and looks each
-- one up by key. The id-list branch of library_permitted_atoms aggregates each
-- atom's source access through the chunk index instead of joining the graph's
-- chunks. Results are unchanged: uuid::text is lowercase canonical, so only
-- that spelling ever matched.
-- Callers map a NULL id list to '{}' because library_permitted_atoms reads
-- NULL as every atom in the graph.
CREATE OR REPLACE FUNCTION library_provenance_id(id TEXT) RETURNS UUID LANGUAGE SQL IMMUTABLE AS $$
 SELECT CASE WHEN id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN id::uuid END
$$;
CREATE OR REPLACE FUNCTION library_provenance_ids(ids JSONB) RETURNS UUID[] LANGUAGE SQL IMMUTABLE AS $$
 SELECT ARRAY(SELECT library_provenance_id(i) FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(ids)='array' THEN ids ELSE '[]'::jsonb END) i)
$$;
CREATE OR REPLACE FUNCTION library_permitted_atoms(g UUID,atom_ids UUID[] DEFAULT NULL) RETURNS TABLE(atom_id UUID)
 LANGUAGE SQL STABLE AS $$
 WITH mapped AS MATERIALIZED (
  SELECT c.atom_id,bool_or(s.access_state='permitted') permitted,bool_or(s.access_state<>'permitted') denied
  FROM source_version_chunks c JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id
  JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
  WHERE c.graph_id=g AND atom_ids IS NULL GROUP BY c.atom_id)
 SELECT a.atom_id FROM atoms a LEFT JOIN mapped m ON m.atom_id=a.atom_id
 WHERE a.graph_id=g AND atom_ids IS NULL
  AND NOT a.evidence_retracted AND NOT a.derived_copy_bound
  AND (NOT a.source_evidence_bound OR COALESCE(m.permitted,false)) AND NOT COALESCE(m.denied,false)
 UNION ALL
 SELECT a.atom_id FROM atoms a CROSS JOIN LATERAL (
  SELECT bool_or(s.access_state='permitted') permitted,bool_or(s.access_state<>'permitted') denied
  FROM source_version_chunks c JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id
  JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
  WHERE c.graph_id=g AND c.atom_id=a.atom_id) m
 WHERE a.graph_id=g AND a.atom_id=ANY(atom_ids)
  AND NOT a.evidence_retracted AND NOT a.derived_copy_bound
  AND (NOT a.source_evidence_bound OR COALESCE(m.permitted,false)) AND NOT COALESCE(m.denied,false)
$$;
CREATE OR REPLACE FUNCTION library_edge_permitted(g UUID,e_id UUID,seen UUID[] DEFAULT '{}',depth INTEGER DEFAULT 0)
 RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $$
DECLARE e relationship_edge%ROWTYPE;parent relationship_edge%ROWTYPE;parent_id UUID;
BEGIN
 IF depth<0 OR depth>8 OR cardinality(seen)>8 OR e_id=ANY(seen) THEN RETURN false; END IF;
 SELECT * INTO e FROM relationship_edge WHERE graph_id=g AND edge_id=e_id;
 IF NOT FOUND OR e.status='rejected' OR e.provenance->>'lifecycle'='unsupported' THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM library_permitted_atoms(g,CASE WHEN library_provenance_ids(e.provenance->'atom_ids') IS NULL THEN '{}' ELSE library_provenance_ids(e.provenance->'atom_ids') END)) THEN RETURN true; END IF;
 IF jsonb_typeof(e.provenance->'source_edge_ids') IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 IF jsonb_array_length(e.provenance->'source_edge_ids')=0 OR jsonb_array_length(e.provenance->'source_edge_ids')>32 THEN RETURN false; END IF;
 FOREACH parent_id IN ARRAY library_provenance_ids(e.provenance->'source_edge_ids') LOOP
  SELECT * INTO parent FROM relationship_edge WHERE graph_id=g AND edge_id=parent_id;
  IF NOT FOUND OR parent.status<>'confirmed' OR NOT library_edge_permitted(g,parent_id,seen||e_id,depth+1) THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;
REVOKE ALL ON FUNCTION library_provenance_id(TEXT),library_provenance_ids(JSONB) FROM PUBLIC;
DO $$ DECLARE r TEXT;BEGIN FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN EXECUTE format('GRANT EXECUTE ON FUNCTION library_provenance_id(TEXT),library_provenance_ids(JSONB) TO %I',r);END IF;END LOOP;END $$;
