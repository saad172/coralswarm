-- no-transaction
-- Home "What's new" (GET /v1/home/whats-new): the newest reefs of one kind in
-- one ocean. The read asks, per (ocean, kind), for the newest reefs formed in
-- a fixed 7-day window, decisions and threads first. With only
-- nodes_graph_created (graph_id, created_at), a kind that is rare or absent
-- in an ocean walks that ocean's whole window and filters on type (17k
-- buffers on a 195k-reef / 7.8M-link seed); this index makes every
-- (ocean, kind) probe an index range no longer than its LIMIT.
--
-- CREATE INDEX CONCURRENTLY cannot run in a transaction, so it is alone in a
-- `-- no-transaction` file. A failed build leaves an INVALID index that
-- IF NOT EXISTS would then skip on retry, so drop it first:
--   DROP INDEX CONCURRENTLY IF EXISTS nodes_graph_type_created;
-- The DDL is identical in both trees (control 0423 is the twin): `nodes` is a
-- shared table and `check_schema_parity.sh` diffs its schema.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS nodes_graph_type_created;

CREATE INDEX CONCURRENTLY IF NOT EXISTS nodes_graph_type_created
    ON nodes (graph_id, type, created_at, node_id);
