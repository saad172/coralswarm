-- no-transaction
-- One statement: SQLx must not wrap a concurrent build in a transaction.
-- If a failed build leaves an INVALID index, DROP INDEX CONCURRENTLY before retrying.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS idx_atoms_connector_activity;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_atoms_connector_activity
 ON atoms (ingested_by, source_type, created_at);
