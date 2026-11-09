-- Migration: 0216_due_source_connection_accounts
-- Description: `resolve_due_source_connection_accounts` — which accounts have a
--   source connection whose next check is due (CodeRabbit round 1 on #871).
--
--   The problem this solves. `0213`'s `source_connection_scope` policy is
--   anchored on `current_setting('app.account_id', true)::uuid`, so the
--   publication sweep MUST run inside an account-scoped transaction: under the
--   production `coralswarm_app` role (non-`BYPASSRLS` since OPS-4) an ambient
--   `SELECT * FROM source_connections` compares `account_id = NULL`, matches
--   nothing, and succeeds — the sweep was not merely unscoped, it was inert,
--   and inert in the shape that looks healthiest (no error, no rows, a green
--   tick).
--
--   But scoping means the worker needs an account BEFORE it can read. The
--   obvious fix — enumerate `accounts` and open one scoped transaction per
--   account — makes the sweep's cost scale with the size of the consumer plane
--   instead of with the amount of work there is to do, on a worker that polls
--   every 500ms (`workers::promote::EMPTY_POLL_SLEEP`). This function is the
--   index lookup that makes the scoped sweep O(due), not O(accounts): one
--   query per tick answers "which accounts, if any", and only those accounts
--   then get a scoped transaction.
--
--   Why `SECURITY DEFINER` is not a widening. It returns ACCOUNT IDS ONLY —
--   never a connection, a graph, a source identity, or any content — for rows
--   that are due right now. `accounts` is already deliberately outside the RLS
--   roster (an identity table; `workers::tombstone_purge`'s consumer tick
--   enumerates it unscoped by design), so every id this can return was already
--   enumerable by the same roles. It answers "does this account have work",
--   not "what is that work"; reading the work still requires the scoped
--   transaction the policy demands. Same shape and same `REVOKE FROM PUBLIC` +
--   explicit-grant discipline as `0084_bootstrap_scope_functions` /
--   `0091_member_ocean_reachability_functions`.
--
--   `p_limit` is mandatory, not defaulted: a sweep that fans out over every
--   due account in one tick is the unbounded-fan-out shape
--   `scripts/check_no_unbounded_fanout.sh` exists to refuse. The caller passes
--   its own bound and the remainder is picked up on the next tick (rows keep
--   their `next_check_at`, so nothing is skipped, only deferred).
--
--   Ordered by `next_check_at` so a bounded call takes the LONGEST-waiting
--   accounts first and cannot starve one account behind another.
--
--   Paired byte-identically into
--   `migrations_tenant/0084_due_source_connection_accounts.sql`.
--
-- Depends on: 0213_source_connections (creates the table and its due index).
-- Down:
--   DROP FUNCTION IF EXISTS resolve_due_source_connection_accounts(int);

SET LOCAL lock_timeout = '3s';

CREATE OR REPLACE FUNCTION resolve_due_source_connection_accounts(p_limit int)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id FROM (
        SELECT account_id, min(next_check_at) AS due_at
        FROM source_connections
        WHERE NOT paused AND completed_at IS NULL AND next_check_at <= now()
        GROUP BY account_id
    ) d
    ORDER BY d.due_at
    LIMIT p_limit
$$;

REVOKE EXECUTE ON FUNCTION resolve_due_source_connection_accounts(int) FROM PUBLIC;

DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') THEN
    GRANT EXECUTE ON FUNCTION resolve_due_source_connection_accounts(int) TO coralswarm_app;
  END IF;
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
    GRANT EXECUTE ON FUNCTION resolve_due_source_connection_accounts(int) TO coralswarm_tenant_app;
  END IF;
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_consumer_app') THEN
    GRANT EXECUTE ON FUNCTION resolve_due_source_connection_accounts(int) TO coralswarm_consumer_app;
  END IF;
END $$;

GRANT EXECUTE ON FUNCTION resolve_due_source_connection_accounts(int) TO CURRENT_USER;
