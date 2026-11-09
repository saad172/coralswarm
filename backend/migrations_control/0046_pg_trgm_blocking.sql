-- Migration: 0046_pg_trgm_blocking
-- Description: Scalable entity-resolution candidate blocking (audit H5). The
--              entity-resolution worker's default blocking is an O(n²) in-Rust
--              scan capped at 500 entity reefs per graph, so duplicates past the
--              cap are never deduped. This migration enables the pg_trgm
--              extension and adds a GIN trigram index on the entity-node title so
--              an OPT-IN DB-side blocking path (ER_BLOCKING=trgm) can fetch
--              candidate pairs above a similarity threshold with `%` /
--              `similarity()` instead of materializing every node in Rust.
--
--              Purely additive: the default code path (ER_BLOCKING unset/`rust`)
--              is unchanged and does not use this index. The index is partial
--              (type = 'entity') since only entity reefs are blocked.
-- Depends on: 0011_nodes (nodes table + title column)
-- Down:
--   DROP INDEX IF EXISTS idx_nodes_entity_title_trgm;
--   -- (the pg_trgm extension is left installed; dropping it is destructive to any
--   --  other trigram indexes and is intentionally not reversed here)

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- GIN trigram index over entity-reef titles. Backs the `%` / similarity() join
-- used by the trgm blocking path, scoped to entity nodes (the only type blocked).
CREATE INDEX IF NOT EXISTS idx_nodes_entity_title_trgm
    ON nodes USING GIN (title gin_trgm_ops)
    WHERE type = 'entity';
