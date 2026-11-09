-- no-transaction
-- PAIRED WITH: tenant 0216 (DDL byte-identical).
-- GET /v1/home/metrics counts reefs formed per day with `graph_id = ANY(..)` and a
-- `created_at` range. Only (graph_id, type) and (graph_id, updated_at) existed, so
-- every dashboard load scanned the reachable oceans' nodes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nodes_graph_created_at
 ON nodes (graph_id, created_at DESC);
