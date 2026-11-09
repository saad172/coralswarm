-- no-transaction
-- Library text search: the outcome-text candidates of the scope graphs only.
-- The single-column trigram index matched every graph in the database, so a
-- multi-graph tenant rechecked each match against the heap and only then
-- dropped other graphs' rows: about 12.8k heap rows read for 1.5k kept, per
-- table, on the 2026-10-27 org tenant (issue #1524 blocker 5). Keying
-- graph_id beside the trigram drops them inside the index. Paired control/tenant DDL is identical.
-- SQLx sends this file as one wire Query, so keep exactly ONE statement here:
-- multiple concurrent builds in one file acquire an implicit transaction.
--
-- A failed build can leave an INVALID index. IF NOT EXISTS does not repair
-- it: inspect pg_index.indisvalid and DROP INDEX CONCURRENTLY the named index
-- before retrying. Do not mark an invalid index as a successful migration.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS idx_library_semantic_atoms_graph_statement_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_library_semantic_atoms_graph_statement_trgm
    ON semantic_atoms USING gin (graph_id, statement gin_trgm_ops);
