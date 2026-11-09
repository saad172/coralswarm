-- no-transaction
-- Library read model PR 5: outcome text candidates for substring search.
-- Paired control/tenant DDL is identical. pg_trgm is installed by the existing
-- source-path trigram migration. A concurrent build avoids blocking ingestion.
-- SQLx sends this file as one wire Query, so keep exactly ONE statement here:
-- multiple concurrent builds in one file acquire an implicit transaction.
--
-- A failed build can leave an INVALID index. IF NOT EXISTS does not repair
-- it: inspect pg_index.indisvalid and DROP INDEX CONCURRENTLY the named index
-- before retrying. Do not mark an invalid index as a successful migration.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS idx_library_semantic_atom_passages_quote_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_library_semantic_atom_passages_quote_trgm
    ON semantic_atom_passages USING gin (quote gin_trgm_ops);
