-- no-transaction
-- PAIRED WITH: tenant 0187 (library_node_semantic_inverse).
-- Ordered inverse links bound Library dirty-key pagination at large hubs.
-- Keep this concurrent statement alone: a multi-statement simple query runs
-- in an implicit transaction even in a no-transaction migration.
-- Retry after an interrupted build: DROP INDEX CONCURRENTLY IF EXISTS library_node_semantic_inverse;
CREATE INDEX CONCURRENTLY IF NOT EXISTS library_node_semantic_inverse ON node_semantic_atoms(semantic_atom_id,graph_id,node_id);
