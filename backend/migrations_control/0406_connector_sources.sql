-- Connector catalog (connectors redesign, layer B1): one row per member, provider
-- and source, holding what the generic connector API needs and the provider
-- tables do not: whether the source is on, whether the member paused it, the
-- scope mode and history window they chose, and whether setup finished.
-- docs/design/connectors-redesign/PLAN.md, "Backend data".
--
-- Absence is meaningful. A connection made before this table existed has no
-- rows, and the catalog reads that as "set up already"; the connect flow (B2)
-- writes the member's rows at connect time, so a new connection has rows and
-- none of them has `setup_completed_at` until the wizard finishes.
--
-- `owner_user_id` is the member the row belongs to. No foreign key to `users`,
-- the same shape as `composio_connections.owner_user_id`: the account FK removes
-- every row when the account goes, and the tenant twin has no `users` table.
-- `scope_mode` NULL = not chosen yet. `history_days` 0 = the member chose
-- "None"; NULL = not chosen yet.
--
-- Paired with migrations_tenant/0220_connector_sources.sql. The CREATE TABLE and
-- the policy are the same on both planes, so scripts/check_schema_parity.sh needs
-- no entry for this table. Capture entry points that run on a tenant pool read
-- the pause state there (layer B3).
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON connector_sources;
--   DROP TABLE IF EXISTS connector_sources;

SET LOCAL lock_timeout = '3s';

CREATE TABLE connector_sources (
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id           UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
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
-- Missing and empty scope both deny (see 0326): a transaction-local GUC reads as
-- '' after COMMIT or ROLLBACK, and that must not fail the uuid cast.
CREATE POLICY account_isolation ON connector_sources
    USING (account_id = NULLIF(current_setting('app.account_id', true), '')::uuid)
    WITH CHECK (account_id = NULLIF(current_setting('app.account_id', true), '')::uuid);
