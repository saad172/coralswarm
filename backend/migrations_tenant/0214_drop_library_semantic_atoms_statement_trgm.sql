-- no-transaction
-- contract: safe-after server-2026.10.29.1
-- No binary names this index; the planner picks the graph-scoped one, so
-- rollback to any release still works without it.
-- Library text search: idx_library_semantic_atoms_graph_statement_trgm
-- serves every lookup idx_library_semantic_atoms_statement_trgm
-- served, with or without a graph_id predicate. This index is now only
-- write cost. Paired control/tenant DDL is identical.
-- SQLx sends this file as one wire Query, so keep exactly ONE statement here.
-- Down: rerun the CREATE INDEX CONCURRENTLY of the migration that built it.

DROP INDEX CONCURRENTLY IF EXISTS idx_library_semantic_atoms_statement_trgm;
