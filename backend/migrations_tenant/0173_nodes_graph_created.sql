-- no-transaction
-- Pipeline stage health: index for reefs minted in a window (`MINTED_REEFS_SQL`), the duplicate-rate denominator.
-- `pipeline_stage_health` bounds every query by a time window on an indexed
-- column; this was the missing one for `nodes`.
--
-- `CREATE INDEX CONCURRENTLY` cannot run in a transaction, so it is alone in
-- a `-- no-transaction` file. If the build fails it leaves an INVALID index;
-- `IF NOT EXISTS` would then skip it on retry, so drop it first:
--   DROP INDEX CONCURRENTLY IF EXISTS nodes_graph_created;
-- The DDL is identical in both trees (control 0340 is the twin): `nodes` is a
-- shared table and `check_schema_parity.sh` diffs its schema.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS nodes_graph_created;

CREATE INDEX CONCURRENTLY IF NOT EXISTS nodes_graph_created
    ON nodes (graph_id, created_at);
