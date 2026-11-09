-- no-transaction
-- Pipeline stage health: index for EntityResolution judgements in a window, every verdict (`GRAPH_STAGES_SQL`); the existing partial indexes cover only `positive` and `unsure`.
-- `pipeline_stage_health` bounds every query by a time window on an indexed
-- column; this was the missing one for `reef_judgement`.
--
-- `CREATE INDEX CONCURRENTLY` cannot run in a transaction, so it is alone in
-- a `-- no-transaction` file. If the build fails it leaves an INVALID index;
-- `IF NOT EXISTS` would then skip it on retry, so drop it first:
--   DROP INDEX CONCURRENTLY IF EXISTS reef_judgement_graph_created;
-- The DDL is identical in both trees (tenant 0175 is the twin): `reef_judgement` is a
-- shared table and `check_schema_parity.sh` diffs its schema.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS reef_judgement_graph_created;

CREATE INDEX CONCURRENTLY IF NOT EXISTS reef_judgement_graph_created
    ON reef_judgement (graph_id, created_at);
