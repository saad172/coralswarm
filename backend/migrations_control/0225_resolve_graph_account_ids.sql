-- Migration: 0225_resolve_graph_account_ids
-- Description: Batched sibling of `resolve_graph_account_id` (migration
--              0084_bootstrap_scope_functions) — perf fix (PR D,
--              `GET /v1/library`). `routes/library.rs::query_library_consumer`
--              was calling `Store::graph_account_id` ONCE PER REACHABLE GRAPH,
--              sequentially awaited, to group graphs by owning account before
--              it can open a `begin_account_scoped` transaction per account
--              (same bootstrap chicken-and-egg 0084's header describes: you
--              cannot scope a connection to an account_id you don't know yet).
--              Measured against production (`EXPLAIN (ANALYZE, BUFFERS)`, see
--              PR D's report): each such round trip pays highly variable
--              intra-region network latency (single-digit ms up to ~80-160ms
--              observed on the `coralswarm-backend` -> `coralswarm-db.flycast`
--              path), while the query itself is sub-millisecond. For a
--              4-reachable-graph account this was 4 sequential round trips
--              doing the work one query could do. This function collapses
--              them into ONE: `WHERE id = ANY($1)` returning `(graph_id,
--              account_id)` pairs, same SECURITY DEFINER bootstrap carve-out
--              as `resolve_graph_account_id` — do NOT wrap this in
--              `begin_account_scoped`/RLS, and do not add any column beyond
--              the two bare uuids (no graph name, no content) so it stays
--              exactly as narrow a bootstrap surface as its non-batched
--              sibling.
--
--              Control-plane only, same reasoning as 0084's
--              `resolve_reachable_graphs`/`resolve_graph_reachable_by_user`:
--              its one Rust caller (`Store::graph_account_ids`, backing
--              `routes/library.rs`'s consumer-plane merge path) only ever
--              runs against the shared consumer pool — the org-tenant path
--              (`tenant.org_context().is_some()`) never fans out across
--              accounts, so it never needs this. NOT paired into
--              `migrations_tenant` for that reason; registered in
--              `scripts/check_schema_parity.sh`'s `CONTROL_FUNCTIONS` instead
--              of a byte-identical tenant twin.
--
-- Depends on: 0001_init (graphs), 0083_consumer_rls (coralswarm_app role),
--             0084_bootstrap_scope_functions (resolve_graph_account_id, the
--             non-batched sibling this mirrors)
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_graph_account_ids(uuid[]) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_graph_account_ids(uuid[]);

CREATE OR REPLACE FUNCTION resolve_graph_account_ids(p_graph_ids uuid[])
RETURNS TABLE(graph_id uuid, account_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT id, account_id FROM graphs WHERE id = ANY(p_graph_ids)
$$;

-- Same PUBLIC-default closing 0084 flags as MAJOR (CodeRabbit review of that
-- PR): Postgres grants EXECUTE on a newly-created function to PUBLIC unless
-- revoked, and this is SECURITY DEFINER / bypasses RLS by design, so leaving
-- the default would let any role with mere CONNECT enumerate graph->account
-- ownership.
REVOKE EXECUTE ON FUNCTION resolve_graph_account_ids(uuid[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_graph_account_ids(uuid[]) TO coralswarm_app, CURRENT_USER;
