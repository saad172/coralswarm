-- no-transaction
-- Pipeline stage health: index for relationship edges created in a window (`GRAPH_STAGES_SQL`).
-- `pipeline_stage_health` bounds every query by a time window on an indexed
-- column; this was the missing one for `relationship_edge`.
--
-- `CREATE INDEX CONCURRENTLY` cannot run in a transaction, so it is alone in
-- a `-- no-transaction` file. If the build fails it leaves an INVALID index;
-- `IF NOT EXISTS` would then skip it on retry, so drop it first:
--   DROP INDEX CONCURRENTLY IF EXISTS relationship_edge_graph_created;
-- The DDL is identical in both trees (tenant 0174 is the twin): `relationship_edge` is a
-- shared table and `check_schema_parity.sh` diffs its schema.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS relationship_edge_graph_created;

CREATE INDEX CONCURRENTLY IF NOT EXISTS relationship_edge_graph_created
    ON relationship_edge (graph_id, created_at);
