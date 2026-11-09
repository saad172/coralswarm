-- Invoker-only shared read predicates. These do not confer graph access and
-- never reconstruct metadata or original source text from legacy chunks.
ALTER TABLE atoms ADD COLUMN source_evidence_bound BOOLEAN NOT NULL DEFAULT false;
UPDATE atoms a SET source_evidence_bound=true WHERE EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id);
CREATE FUNCTION library_mark_chunk_bound() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 UPDATE atoms SET source_evidence_bound=true WHERE graph_id=NEW.graph_id AND atom_id=NEW.atom_id AND NOT source_evidence_bound;
 RETURN NULL;
END $$;
CREATE TRIGGER library_chunk_bound AFTER INSERT OR UPDATE OF graph_id,atom_id ON source_version_chunks FOR EACH ROW EXECUTE FUNCTION library_mark_chunk_bound();
CREATE FUNCTION library_preserve_chunk_bound() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.source_evidence_bound THEN NEW.source_evidence_bound=true; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER library_chunk_bound_monotonic BEFORE UPDATE OF source_evidence_bound ON atoms FOR EACH ROW EXECUTE FUNCTION library_preserve_chunk_bound();
CREATE FUNCTION library_atom_permitted(g UUID,a_id UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM atoms a WHERE a.graph_id=g AND a.atom_id=a_id
  AND (NOT a.source_evidence_bound OR EXISTS(SELECT 1 FROM source_version_chunks c
   JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id
   JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
   WHERE c.graph_id=g AND c.atom_id=a.atom_id AND s.access_state='permitted'))
  AND NOT EXISTS(SELECT 1 FROM source_version_chunks c
   JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id
   JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
   WHERE c.graph_id=g AND c.atom_id=a.atom_id AND s.access_state<>'permitted'))
$$;
CREATE FUNCTION library_node_permitted(g UUID,n_id UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nodes n WHERE n.graph_id=g AND n.node_id=n_id AND (
  EXISTS(SELECT 1 FROM node_semantic_atoms l
   JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
   JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id
   JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
   WHERE l.graph_id=g AND l.node_id=n_id AND s.access_state='permitted') OR
  EXISTS(SELECT 1 FROM node_atoms l WHERE l.node_id=n_id AND library_atom_permitted(g,l.atom_id))))
$$;
CREATE FUNCTION library_node_title(g UUID,n_id UUID) RETURNS TEXT LANGUAGE SQL STABLE AS $$
 SELECT substring(n.title FOR 256) FROM nodes n WHERE n.graph_id=g AND n.node_id=n_id AND (
  EXISTS(SELECT 1 FROM node_semantic_atoms l
   JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
   JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id
   JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
   CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.entities)='array' THEN a.entities ELSE '[]'::jsonb END) e
   WHERE l.graph_id=g AND l.node_id=n_id AND s.access_state='permitted'
    AND lower(e->>'mention')=lower(n.title)) OR
  EXISTS(SELECT 1 FROM node_atoms l JOIN atoms a ON a.atom_id=l.atom_id AND a.graph_id=g
   WHERE l.node_id=n_id AND library_atom_permitted(g,a.atom_id)
    AND n.title<>'' AND (position(lower(n.title) IN lower(COALESCE(a.content_inline,'')))>0
     OR EXISTS(SELECT 1 FROM unnest(a.participants) p WHERE lower(p)=lower(n.title)))))
$$;
-- A rejected parent cannot become support merely because an endpoint remains
-- readable. Direct independent support wins; dependency chains are confirmed,
-- graph-qualified and bounded, with cycles conservatively unavailable.
CREATE FUNCTION library_edge_permitted(g UUID,e_id UUID,seen UUID[] DEFAULT '{}',depth INTEGER DEFAULT 0)
 RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS $$
DECLARE e relationship_edge%ROWTYPE;
BEGIN
 IF depth<0 OR depth>8 OR cardinality(seen)>8 OR e_id=ANY(seen) THEN RETURN false; END IF;
 SELECT * INTO e FROM relationship_edge WHERE graph_id=g AND edge_id=e_id;
 IF NOT FOUND OR e.status='rejected' OR e.provenance->>'lifecycle'='unsupported' THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(e.provenance->'atom_ids')='array' THEN e.provenance->'atom_ids' ELSE '[]'::jsonb END) ids(id)
  JOIN atoms a ON a.graph_id=g AND a.atom_id::text=ids.id WHERE library_atom_permitted(g,a.atom_id)) THEN RETURN true; END IF;
 IF jsonb_typeof(e.provenance->'source_edge_ids') IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 IF jsonb_array_length(e.provenance->'source_edge_ids')=0 OR jsonb_array_length(e.provenance->'source_edge_ids')>32 THEN RETURN false; END IF;
 RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(e.provenance->'source_edge_ids') ids(id)
  LEFT JOIN relationship_edge p ON p.graph_id=g AND p.edge_id::text=ids.id
  WHERE p.edge_id IS NULL OR p.status<>'confirmed' OR NOT library_edge_permitted(g,p.edge_id,seen||e_id,depth+1));
END $$;
REVOKE ALL ON FUNCTION library_atom_permitted(UUID,UUID),library_node_permitted(UUID,UUID),library_node_title(UUID,UUID),library_edge_permitted(UUID,UUID,UUID[],INTEGER) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
  GRANT EXECUTE ON FUNCTION library_atom_permitted(UUID,UUID),library_node_permitted(UUID,UUID),library_node_title(UUID,UUID),library_edge_permitted(UUID,UUID,UUID[],INTEGER) TO coralswarm_app;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
  GRANT EXECUTE ON FUNCTION library_atom_permitted(UUID,UUID),library_node_permitted(UUID,UUID),library_node_title(UUID,UUID),library_edge_permitted(UUID,UUID,UUID[],INTEGER) TO coralswarm_tenant_app;
 END IF;
END $$;
