-- no-transaction
-- 0012_coding_session_atoms_index_concurrently.sql — paired half of
-- `migrations_control/0099_coding_session_atoms_index_concurrently.sql`. See
-- that migration's header for why `CREATE INDEX CONCURRENTLY` must be alone in
-- a `-- no-transaction` file, why this index exists, and the INVALID-index
-- caveat if the build fails.
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only the header
-- prose differs) — `atoms` is a SHARED table, present in both trees and NOT in
-- `check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its schema-only
-- `pg_dump` diff fails on any divergence here.
--
-- Depends on: 0011_coding_session_source_type (the values this indexes).
-- Down: see control 0099.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_atoms_graph_coding_session
    ON atoms (graph_id, timestamp DESC)
    WHERE source_type = 'coding_session';
