-- no-transaction
-- Migration: 0097_atoms_title_tsv_index
-- Description: GIN index over `atoms.title`'s tsvector, for the atom keyword
--              leg (`store::keyword_search_pg`).
--
-- Byte-identical DDL to migrations_control/0242_atoms_title_tsv_index.sql.
-- `atoms` is shared across both trees; see that file for why this is an
-- EXPRESSION index rather than a generated column, for the measured plans
-- that decided it, and for why the configuration is 'simple'.
--
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_atoms_title_tsv;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_atoms_title_tsv
    ON atoms USING GIN (to_tsvector('simple', coalesce(title, '')));
