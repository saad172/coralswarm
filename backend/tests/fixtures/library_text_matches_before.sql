
, matched_sources AS MATERIALIZED (
 SELECT s.* FROM scope_sources s WHERE $7::text IS NULL OR s.title ILIKE $7 OR s.source_id ILIKE $7
 OR EXISTS(SELECT 1 FROM scope_atoms a WHERE a.source_item_id=s.source_item_id AND
  (a.statement ILIKE $7 OR EXISTS(SELECT 1 FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id AND p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.quote ILIKE $7)))
), matched_atoms AS MATERIALIZED (
 SELECT a.* FROM scope_atoms a WHERE $7::text IS NULL OR a.statement ILIKE $7
 OR EXISTS(SELECT 1 FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id AND p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.quote ILIKE $7)
 OR EXISTS(SELECT 1 FROM scope_sources s WHERE s.source_item_id=a.source_item_id AND s.title ILIKE $7)
)