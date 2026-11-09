-- no-transaction
-- Pipeline stage health: index for dead letters per kind in a window (`DEAD_JOBS_SQL`). Partial: only dead rows, which the queue never rewrites.
-- `pipeline_stage_health` bounds every query by a time window on an indexed
-- column; this was the missing one for `jobs`.
--
-- `CREATE INDEX CONCURRENTLY` cannot run in a transaction, so it is alone in
-- a `-- no-transaction` file. If the build fails it leaves an INVALID index;
-- `IF NOT EXISTS` would then skip it on retry, so drop it first:
--   DROP INDEX CONCURRENTLY IF EXISTS jobs_dead_finished;
-- The DDL is identical in both trees (control 0343 is the twin): `jobs` is a
-- shared table and `check_schema_parity.sh` diffs its schema.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS jobs_dead_finished;

CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_dead_finished
    ON jobs (finished_at) WHERE status = 'dead';
