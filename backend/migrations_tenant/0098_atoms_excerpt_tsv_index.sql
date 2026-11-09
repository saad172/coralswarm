-- no-transaction
-- Migration: 0098_atoms_excerpt_tsv_index
-- Description: GIN index over `atoms.excerpt`'s tsvector, for the atom keyword
--              leg's SYMBOL candidate half (`store::keyword_search_pg`, fix 3b).
--
-- Byte-identical DDL to migrations_control/0245_atoms_excerpt_tsv_index.sql.
-- `atoms` is shared across both trees; see that file for the measured plans
-- that rule out running an identifier phrase against `content_tsv` at all
-- (`user_id`: 3,925 rows, seq scan, 241ms warm / 3.8s cold), for why
-- `atoms.excerpt` is bounded where `content_tsv` is not, and for why this is an
-- EXPRESSION index rather than a generated column.
--
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_atoms_excerpt_tsv;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_atoms_excerpt_tsv
    ON atoms USING GIN (to_tsvector('simple', coalesce(excerpt, '')));
