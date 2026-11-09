-- Indexes backing the case-insensitive + alias reef resolution added to
-- Store::upsert_node_and_link_atom (code-review M2). That resolve step runs on
-- the extraction hot path (once per extracted entity per coral) and filters:
--   WHERE graph_id = $1 AND type = $2
--     AND (lower(title) = lower($3) OR jsonb_exists(body->'aliases', lower($3)))
-- Without these, the predicate degrades to a residual filter over every node of
-- the (graph_id, type) as a graph accumulates reefs.

-- Case-insensitive title lookups (folds "Casey" / "Casey" without a scan).
CREATE INDEX IF NOT EXISTS idx_nodes_graph_type_lower_title
    ON nodes (graph_id, type, lower(title));

-- Alias membership lookups: GIN over body->'aliases' supports the `?` /
-- jsonb_exists() containment test used by the resolver and merge-teaches-alias.
CREATE INDEX IF NOT EXISTS idx_nodes_aliases_gin
    ON nodes USING GIN ((body -> 'aliases'));
