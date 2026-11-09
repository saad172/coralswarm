-- no-transaction
-- Migration: 0392_library_evidence_changes_node_index
-- Description: The partial index the busy-graph change set (#1524 blocker 2,
--   `backend/src/library_changes/keys.rs`) walks to find the DISTINCT node
--   keys pending in `library_evidence_changes`: a loose index scan, one probe
--   per distinct node however many log rows name it. Until now the log had
--   only `(graph_id, id)` (control 0354 / tenant 0185), so counting distinct
--   pending nodes meant reading every row. Additive; nothing reads it until
--   the change set is wired into the Library reader's admission.
--
--   Partial (`WHERE node_id IS NOT NULL`): exactly one key column is set per
--   row (the table's CHECK), and the item/atom/semantic-atom rows are found
--   by the log-order walk instead; indexing their NULL node_id would only add
--   write cost on the capture triggers' hot path.
--
--   `CONCURRENTLY` in a `-- no-transaction` file, alone, for the reasons in
--   0059/0142's headers (sqlx sends a migration as one Query message, which
--   PostgreSQL wraps in a transaction CONCURRENTLY refuses): the log takes
--   inserts from every capture statement on a busy graph, and a plain build
--   would block them for its duration.
--
--   `IF NOT EXISTS` + `CONCURRENTLY`: a failed concurrent build leaves an
--   INVALID index behind, and `IF NOT EXISTS` then skips it forever. If this
--   migration errors, drop `library_evidence_changes_node` before re-running
--   rather than assuming the skip means success (the same caveat as
--   0059/0074/0075/0099/0142).
--
-- PAIRED WITH: backend/migrations_tenant/0207_library_evidence_changes_node_index.sql
-- Depends on: 0354_library_evidence.sql (creates library_evidence_changes).
-- Down:
--   DROP INDEX CONCURRENTLY IF EXISTS library_evidence_changes_node;

CREATE INDEX CONCURRENTLY IF NOT EXISTS library_evidence_changes_node
    ON library_evidence_changes (graph_id, node_id)
    WHERE node_id IS NOT NULL;
