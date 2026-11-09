-- no-transaction
-- Migration: 0099_coding_session_atoms_index_concurrently
-- Description: Build the partial index backing the coding-session slice of the
--   Sources view and the `corals by source` breakdown, WITHOUT blocking writes
--   on `atoms` while it builds.
--
--   Split out of 0098 (which does the `source_type` relabel) for exactly the
--   reason 0059 was split out of 0058, and the mechanics are unchanged from
--   that precedent — see 0059's header for the full explanation of why
--   `CREATE INDEX CONCURRENTLY` must be alone in a `-- no-transaction` file
--   (sqlx sends a migration's SQL as one wire-protocol Query message, and
--   Postgres implicitly wraps multiple statements in such a message in a
--   transaction, which `CONCURRENTLY` refuses to run inside).
--
--   Why the index at all. The (graph_id, source_type) index from 0007 already
--   serves plain equality lookups; this narrower partial index covers the
--   `ORDER BY timestamp DESC` those listings use without carrying every other
--   source type's rows, so the coding-session slice stays cheap on oceans
--   where captures come to dominate the atom count. That is a forward-looking
--   claim, not a current one: the largest ocean today holds 3,894 atoms in
--   total (measured 2026-07-26 via `get_ocean_stats`), so nothing here is
--   index-bound yet. The index is cheap insurance, not a fix for observed
--   slowness — sized to where capture volume is heading, not where it is.
--
--   `IF NOT EXISTS` + `CONCURRENTLY`: a failed concurrent build leaves an
--   INVALID index behind, and `IF NOT EXISTS` will then happily skip it
--   forever. If this migration errors, drop `idx_atoms_graph_coding_session`
--   before re-running rather than assuming the skip means success — the same
--   operational caveat every `CONCURRENTLY` migration in this tree carries.
--
-- Depends on: 0098_coding_session_source_type (the values this indexes).
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_atoms_graph_coding_session;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_atoms_graph_coding_session
    ON atoms (graph_id, timestamp DESC)
    WHERE source_type = 'coding_session';
