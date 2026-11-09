-- no-transaction
-- Migration: 0099_atoms_source_path_tsv_index
-- Description: GIN index over the PATH SEGMENTS of `atoms.source_id`, for the
--              atom keyword leg's path half (`store::keyword_search_pg`, fix 3b).
--
-- Byte-identical DDL to migrations_control/0246_atoms_source_path_tsv_index.sql.
-- `atoms` is shared across both trees; see that file for the suffix-vs-prefix
-- gap this closes, and for the three-way EXPLAIN comparison (no index 40.2ms /
-- pg_trgm 0.89ms at 3,816 kB with a substring false hit / this 0.48ms at
-- 976 kB, whole-segment) that chose a segment tsvector over pg_trgm and over a
-- reverse() index.
--
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_atoms_source_path_tsv;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_atoms_source_path_tsv
    ON atoms USING GIN (to_tsvector('simple', translate(source_id, '/', ' ')));
