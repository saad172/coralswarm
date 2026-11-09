-- Migration: 0017_nodes_unique_constraint
-- Description: Add UNIQUE constraint on (graph_id, type, title) for the nodes
--              table so that the extract worker's ON CONFLICT clause works correctly.
--
-- Down:
--   ALTER TABLE nodes DROP CONSTRAINT nodes_graph_type_title_unique;

ALTER TABLE nodes
    ADD CONSTRAINT nodes_graph_type_title_unique
    UNIQUE (graph_id, type, title);
