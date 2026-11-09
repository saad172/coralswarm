-- Migration: 0019_nodes_search_tsv
-- Description: §5.6 reef matching — a weighted, GIN-indexed `tsvector` over
--   `nodes` so a reef can be found by name/alias/summary via an index lookup
--   instead of `Store::keyword_search_node_atoms`'s full `nodes JOIN
--   node_atoms` scan (D8).
-- Depends on: 0001_tenant_baseline (nodes)
-- PAIRED WITH: migrations_control/0108_nodes_search_tsv.sql — the DDL in the
--              two files is IDENTICAL and must stay that way
--              (`scripts/check_schema_parity.sh` diffs a normalized pg_dump of
--              both trees and fails on any difference). See that file's
--              header for the full rationale (weight-class mapping, the
--              'simple' vs 'english' choice, why aliases index usefully as
--              raw JSON text, why the column is generated rather than
--              triggered, and why the index is not built CONCURRENTLY) and
--              for the populated-table verification this was run against
--              before being written.
-- Down:
--   DROP INDEX IF EXISTS idx_nodes_search_tsv;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS search_tsv;

ALTER TABLE nodes ADD COLUMN search_tsv tsvector
  GENERATED ALWAYS AS (
      setweight(to_tsvector('simple',
        CASE WHEN type = 'entity' THEN title ELSE '' END), 'A')
   || setweight(to_tsvector('simple',
        coalesce(body->>'aliases', '')), 'B')
   || setweight(to_tsvector('simple',
        CASE WHEN type <> 'entity' THEN title ELSE '' END), 'C')
   || setweight(to_tsvector('english',
        coalesce(body->'digest'->>'summary', '')), 'D')
  ) STORED;

CREATE INDEX idx_nodes_search_tsv ON nodes USING GIN (search_tsv);
