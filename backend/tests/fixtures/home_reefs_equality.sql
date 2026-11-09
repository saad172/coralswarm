-- Pure equality query from dc03578e perf-scratch; served semantic admission retains current history floors.
WITH win AS MATERIALIZED (
  SELECT n.graph_id, n.node_id, n.created_at FROM nodes n
  WHERE n.graph_id = ANY($1) AND n.created_at >= $2 AND n.created_at < $3
    AND (n.created_at < $4 OR n.created_at >= $5)
), sem AS MATERIALIZED (
  SELECT DISTINCT w.node_id FROM win w
  JOIN node_semantic_atoms l ON l.graph_id = w.graph_id AND l.node_id = w.node_id
  JOIN served_semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
), linked AS MATERIALIZED (
  SELECT w.graph_id, w.node_id, l.atom_id FROM win w JOIN node_atoms l ON l.node_id = w.node_id
  WHERE w.node_id NOT IN (SELECT node_id FROM sem)
), per_graph AS (
  SELECT graph_id, array_agg(DISTINCT atom_id) AS ids FROM linked GROUP BY graph_id
), leg AS MATERIALIZED (
  SELECT DISTINCT l.node_id FROM per_graph pg
  CROSS JOIN LATERAL public.library_permitted_atoms(pg.graph_id, CASE WHEN cardinality(pg.ids) > 10000 THEN NULL ELSE pg.ids END) p
  JOIN linked l ON l.graph_id = pg.graph_id AND l.atom_id = p.atom_id
), setbased AS (
  SELECT w.node_id, (w.node_id IN (SELECT node_id FROM sem) OR w.node_id IN (SELECT node_id FROM leg)) AS ok FROM win w
)
SELECT count(*) FILTER (WHERE s.ok <> public.library_node_permitted(w.graph_id, w.node_id)) AS mismatches,
       count(*) FILTER (WHERE s.ok) AS admitted, count(*) AS total
FROM win w JOIN setbased s USING (node_id)
