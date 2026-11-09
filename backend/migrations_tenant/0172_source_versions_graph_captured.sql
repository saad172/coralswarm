-- no-transaction
-- Pipeline stage health: index for the capture cohort (`VERSION_STAGES_SQL`). COR-71's `clock_coverage` filters on `captured_at` too; until now both read the whole table.
-- `pipeline_stage_health` bounds every query by a time window on an indexed
-- column; this was the missing one for `source_versions`.
--
-- `CREATE INDEX CONCURRENTLY` cannot run in a transaction, so it is alone in
-- a `-- no-transaction` file. If the build fails it leaves an INVALID index;
-- `IF NOT EXISTS` would then skip it on retry, so drop it first:
--   DROP INDEX CONCURRENTLY IF EXISTS source_versions_graph_captured;
-- The DDL is identical in both trees (control 0339 is the twin): `source_versions` is a
-- shared table and `check_schema_parity.sh` diffs its schema.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS source_versions_graph_captured;

CREATE INDEX CONCURRENTLY IF NOT EXISTS source_versions_graph_captured
    ON source_versions (graph_id, captured_at);
