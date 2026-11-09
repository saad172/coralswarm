-- no-transaction
-- Migration: 0101_atoms_symbols_tsv_index
-- Description: GIN index over `atoms.symbols`'s tsvector, for the atom
--              keyword leg's symbol candidate (`store::keyword_search_pg`,
--              fix 3b + this fix).
--
-- Byte-identical DDL to
-- `migrations_control/0249_atoms_symbols_tsv_index.sql`. `atoms` is shared
-- across both trees; see that file for the full rationale — why a third
-- index rather than widening `idx_atoms_excerpt_tsv`, the `'simple'`/
-- `coalesce` choices (matching `content_tsv`/`idx_atoms_title_tsv`/
-- `idx_atoms_excerpt_tsv`/`idx_atoms_source_path_tsv`), and the expected
-- index size.
--
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_atoms_symbols_tsv;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_atoms_symbols_tsv
    ON atoms USING GIN (to_tsvector('simple', coalesce(symbols, '')));
