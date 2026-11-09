-- no-transaction
-- PAIRED WITH: tenant 0186 (library_node_atoms_inverse).
-- Ordered inverse links bound Library dirty-key pagination at large hubs.
-- Keep this concurrent statement alone: a multi-statement simple query runs
-- in an implicit transaction even in a no-transaction migration.
-- Retry after an interrupted build: DROP INDEX CONCURRENTLY IF EXISTS library_node_atoms_inverse;
CREATE INDEX CONCURRENTLY IF NOT EXISTS library_node_atoms_inverse ON node_atoms(atom_id,node_id);
