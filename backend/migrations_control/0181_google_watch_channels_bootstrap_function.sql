-- Migration 0181: google_watch_channels sweep work-set discovery
-- (SECURITY DEFINER bootstrap function; CodeRabbit review finding on gsuite
-- Train G1 PR 2, #674).
--
--   `workers::google_watch_renewal`'s CONSUMER-plane ticks sweep every
--   account with a channel due for renewal — cross-account by definition,
--   so there is no single account to scope a plain `SELECT ... FROM
--   google_watch_channels` against. Left as a raw table read under a
--   `PgPool` that is not the table owner, `0180`'s `account_isolation`
--   policy (`USING (account_id = current_setting('app.account_id')::uuid)`)
--   would make that query return ZERO ROWS whenever `app.account_id` is
--   unset — which is exactly the shared consumer/control pool's ambient
--   state outside an opened scope — and SILENTLY stop every renewal. This
--   is the identical incident class `0086_webhook_share_bootstrap_
--   functions.sql`'s header describes at length for the Microsoft analog
--   (`resolve_expiring_subscriptions`), and this function is that same
--   pattern applied to `google_watch_channels`: bare ids only (`id`,
--   `account_id` — never `resource_id`, `channel_token`, or any other
--   column), consumed purely to discover WHICH accounts have work. Each id
--   is then re-read inside its own `begin_tenant_system_scoped` transaction
--   (`workers::google_watch_renewal::channels_due`'s cross-account branch),
--   so the only thing crossing an account boundary here is the identifier
--   the sweep needs to open a scope with.
--
--   Control/consumer-plane only, no tenant-tree twin: an org tenant
--   database holds exactly one account (ADR-0007), so there is no cross-
--   account discovery problem to solve there at all — the org-tenant tick
--   already knows its `account_id` (resolved once from the control-plane
--   `tenant_databases` row, same as `workers::graph_subscriptions`'
--   `resolve_tenant_account`) and opens a scoped transaction directly,
--   with no SECURITY DEFINER hop. Registered in `scripts/
--   check_schema_parity.sh`'s `CONTROL_FUNCTIONS` list, same as every other
--   0086-style bootstrap function.
--
-- Depends on: 0180_google_watch_channels.sql (the table this reads).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_google_watch_channels_due(timestamptz) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_google_watch_channels_due(timestamptz) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_google_watch_channels_due(timestamptz);

CREATE OR REPLACE FUNCTION resolve_google_watch_channels_due(p_cutoff timestamptz)
RETURNS TABLE (id uuid, account_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT gwc.id, gwc.account_id
    FROM google_watch_channels gwc
    WHERE gwc.expires_at <= p_cutoff
$$;

REVOKE EXECUTE ON FUNCTION resolve_google_watch_channels_due(timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_google_watch_channels_due(timestamptz) TO coralswarm_app, CURRENT_USER;
