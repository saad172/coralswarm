-- no-transaction
-- 0207_library_evidence_changes_node_index.sql: paired half of
-- `migrations_control/0392_library_evidence_changes_node_index.sql`. See that
-- migration's header for the reason (the busy-graph distinct-key scan,
-- #1524) and the CONCURRENTLY mechanics.
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only the header
-- prose differs): `library_evidence_changes` exists on both planes (tenant
-- 0185 / control 0354) and the schema-parity check diffs it.
--
-- PAIRED WITH: backend/migrations_control/0392_library_evidence_changes_node_index.sql
-- Depends on: 0185_library_evidence.sql (creates library_evidence_changes).
-- Down: see control 0392.

CREATE INDEX CONCURRENTLY IF NOT EXISTS library_evidence_changes_node
    ON library_evidence_changes (graph_id, node_id)
    WHERE node_id IS NOT NULL;
