//! Distinct-key extraction over the two change logs.
//!
//! * Listing log: a loose index scan (recursive CTE) over
//!   `library_listing_changes(graph_id,source_item_id,id)` (tenant 0199 /
//!   control 0378) finds each distinct pending item with one index probe,
//!   however many rows name it. Rows with a NULL item are the unresolved
//!   parent hints the worker resolves; their presence is reported, exactly as
//!   the reader treats them today (`dirty_item_unresolved`).
//! * Node log, node keys: the same loose scan over
//!   `library_evidence_changes_node(graph_id,node_id) WHERE node_id IS NOT
//!   NULL` (tenant 0207 / control 0392).
//! * Node log, object keys (an item, atom or semantic atom whose node fan-out
//!   is resolved at read time): a bounded walk of the object rows in log
//!   order, deduplicated, and each distinct key resolved to its nodes once by
//!   `library_evidence_change_nodes` with no cursor (a read ignores the
//!   worker's progress cursor, as the overlay always has). The object rows
//!   carry no key index of their own; the walk is bounded by
//!   `Budgets::object_rows` and the distinct keys by `Budgets::object_keys`.
//!
//! Before any object key is resolved, `change_object_guard` bounds the keys'
//! physical inverse fan-out (`library_changes_raw_edges_safe`, row security
//! off, tenant 0208 / control 0393), as the reader's raw-edge guard did over
//! raw log rows: resolving the OLD key of an object since moved to another
//! graph would otherwise walk that graph's links.
//!
//! Every relation is capped at its budget plus one, so a count above the
//! budget is an exact "over budget" signal and below it an exact count.
use super::{Binds, Budgets};

pub(super) fn ctes(b: Binds, budgets: Budgets) -> String {
    format!(
        r#"change_graphs AS MATERIALIZED (
 SELECT DISTINCT unnest({graphs}) graph_id
), change_listing_keys AS MATERIALIZED (
 SELECT k.graph_id,k.source_item_id FROM change_graphs g CROSS JOIN LATERAL (
  WITH RECURSIVE walk(graph_id,source_item_id) AS (
   (SELECT c.graph_id,c.source_item_id FROM library_listing_changes c
    WHERE c.graph_id=g.graph_id AND c.source_item_id IS NOT NULL ORDER BY c.source_item_id LIMIT 1)
   UNION ALL
   SELECT w.graph_id,(SELECT c.source_item_id FROM library_listing_changes c
    WHERE c.graph_id=w.graph_id AND c.source_item_id>w.source_item_id ORDER BY c.source_item_id LIMIT 1)
   FROM walk w WHERE w.source_item_id IS NOT NULL
  ) SELECT walk.graph_id,walk.source_item_id FROM walk WHERE walk.source_item_id IS NOT NULL LIMIT {items_plus}
 ) k LIMIT {items_plus}
), change_listing_unresolved AS MATERIALIZED (
 SELECT EXISTS(SELECT 1 FROM change_graphs g JOIN library_listing_changes c
  ON c.graph_id=g.graph_id AND c.source_item_id IS NULL) unresolved
), change_direct_nodes AS MATERIALIZED (
 SELECT k.graph_id,k.node_id FROM change_graphs g CROSS JOIN LATERAL (
  WITH RECURSIVE walk(graph_id,node_id) AS (
   (SELECT c.graph_id,c.node_id FROM library_evidence_changes c
    WHERE c.graph_id=g.graph_id AND c.node_id IS NOT NULL ORDER BY c.node_id LIMIT 1)
   UNION ALL
   SELECT w.graph_id,(SELECT c.node_id FROM library_evidence_changes c
    WHERE c.graph_id=w.graph_id AND c.node_id IS NOT NULL AND c.node_id>w.node_id ORDER BY c.node_id LIMIT 1)
   FROM walk w WHERE w.node_id IS NOT NULL
  ) SELECT walk.graph_id,walk.node_id FROM walk WHERE walk.node_id IS NOT NULL LIMIT {nodes_plus}
 ) k LIMIT {nodes_plus}
), change_object_rows AS MATERIALIZED (
 SELECT r.* FROM change_graphs g CROSS JOIN LATERAL (
  SELECT c.id,c.graph_id,c.source_item_id,c.atom_id,c.semantic_atom_id FROM library_evidence_changes c
  WHERE c.graph_id=g.graph_id AND c.node_id IS NULL ORDER BY c.id LIMIT {rows_plus}
 ) r LIMIT {rows_plus}
), change_object_keys AS MATERIALIZED (
 SELECT k.id,k.graph_id FROM (
  SELECT DISTINCT ON (r.graph_id,r.source_item_id,r.atom_id,r.semantic_atom_id) r.id,r.graph_id,r.source_item_id,r.atom_id,r.semantic_atom_id
  FROM change_object_rows r WHERE (SELECT count(*) FROM change_object_rows)<={rows}
  ORDER BY r.graph_id,r.source_item_id,r.atom_id,r.semantic_atom_id,r.id
 ) k ORDER BY k.id LIMIT {objects_plus}
), change_object_guard AS MATERIALIZED (
 -- Physical inverse fan-out first, row security off (tenant 0208 / control
 -- 0393): an OLD key of a moved or deleted object, or a fan-out past the
 -- edge budget, is never walked; the read goes Live instead.
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM change_object_keys) OR (SELECT count(*) FROM change_object_keys)>{objects} THEN true
  ELSE public.library_changes_raw_edges_safe(ARRAY(SELECT graph_id FROM change_graphs),ARRAY(SELECT id FROM change_object_keys),ARRAY[]::uuid[],{edges}) END safe
), change_nodes AS MATERIALIZED (
 SELECT DISTINCT n.graph_id,n.node_id FROM (
  SELECT d.graph_id,d.node_id FROM change_direct_nodes d
  UNION ALL
  SELECT k.graph_id,r.node_id FROM change_object_keys k
  CROSS JOIN LATERAL library_evidence_change_nodes(k.graph_id,k.id,NULL,{nodes_plus}) r
  WHERE (SELECT count(*) FROM change_object_keys)<={objects} AND (SELECT count(*) FROM change_direct_nodes)<={nodes}
   AND (SELECT safe FROM change_object_guard)
 ) n
)"#,
        graphs = b.graphs,
        items_plus = budgets.items + 1,
        nodes_plus = budgets.nodes + 1,
        rows = budgets.object_rows,
        rows_plus = budgets.object_rows + 1,
        objects = budgets.object_keys,
        objects_plus = budgets.object_keys + 1,
        nodes = budgets.nodes,
        edges = budgets.object_edges,
    )
}
