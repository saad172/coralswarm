-- Tenant twin of migrations_control/0406_connector_sources.sql; see that file for
-- what the table holds and why. The CREATE TABLE and the policy must stay the
-- same as the control side, or scripts/check_schema_parity.sh fails.
--
-- `account_id` and `owner_user_id` carry no foreign key here: `accounts` and
-- `users` are control-only tables that do not exist in a tenant database. The
-- policy is account-keyed like `slack_huddle_consents` (tenant 0124/0158), and
-- the tenant system scope (`begin_tenant_system_scoped`) sets `app.account_id`.
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON connector_sources;
--   DROP TABLE IF EXISTS connector_sources;

SET LOCAL lock_timeout = '3s';

CREATE TABLE connector_sources (
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id           UUID        NOT NULL,
    owner_user_id        UUID        NOT NULL,
    provider             TEXT        NOT NULL CHECK (provider ~ '^[a-z0-9_]{1,64}$'),
    source               TEXT        NOT NULL CHECK (source ~ '^[a-z0-9_]{1,64}$'),
    enabled              BOOLEAN     NOT NULL DEFAULT false,
    paused_at            TIMESTAMPTZ,
    scope_mode           TEXT        CHECK (scope_mode IN ('pick', 'everything')),
    history_days         INTEGER     CHECK (history_days IN (0, 7, 30, 90)),
    history_requested_at TIMESTAMPTZ,
    setup_completed_at   TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, owner_user_id, provider, source)
);

ALTER TABLE connector_sources ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON connector_sources
    USING (account_id = NULLIF(current_setting('app.account_id', true), '')::uuid)
    WITH CHECK (account_id = NULLIF(current_setting('app.account_id', true), '')::uuid);
