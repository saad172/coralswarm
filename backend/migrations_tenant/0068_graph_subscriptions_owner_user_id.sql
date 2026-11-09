-- Migration: 0068_graph_subscriptions_owner_user_id
-- Description: m365 enterprise train PR 3.3 — schema-parity twin of
--   `migrations_control/0190_graph_subscriptions_owner_user_id.sql`. See that
--   migration's header for the full design rationale (per-member mail
--   subscription coexistence on an org account, closing the gap
--   `connectors::microsoft_disconnect::purge_microsoft_connection`'s own doc
--   comment names). Byte-identical schema change; no FK to restate here
--   (this tree's `graph_subscriptions.account_id` carries no `REFERENCES
--   accounts(id)` — `accounts` does not exist on a tenant pool at all, same
--   as every other shared table in this tree).
--
-- Down:
--   DROP INDEX IF EXISTS uq_graph_subscriptions_per_owner;
--   DROP INDEX IF EXISTS uq_graph_subscriptions_account_wide;
--   ALTER TABLE graph_subscriptions
--     ADD CONSTRAINT graph_subscriptions_account_id_resource_key
--       UNIQUE (account_id, resource);
--   ALTER TABLE graph_subscriptions DROP COLUMN IF EXISTS owner_user_id;

--
--   MIXED-VERSION CUTOVER NOTE (CodeRabbit round 1): old binaries write
--   `ON CONFLICT (account_id, resource)`, which stops matching once the
--   named UNIQUE constraint below is dropped (neither partial index is a
--   valid arbiter for that spec). Postgres then ERRORS those writes rather
--   than corrupting anything — fail-closed, but noisy. This codebase's
--   release process already makes the window trivial: migrations are
--   applied manually per-org AT deploy time by the single deployer (see
--   plan release checklist), the only writers are this app's own workers,
--   and there is no rolling fleet of old binaries. Apply this migration
--   together with the deploy of the binary that contains PR 3.3 —
--   documented in the plan's release checklist rather than engineered
--   around with a multi-phase constraint dance this deployment shape
--   doesn't need.

ALTER TABLE graph_subscriptions ADD COLUMN IF NOT EXISTS owner_user_id UUID;

ALTER TABLE graph_subscriptions
    DROP CONSTRAINT IF EXISTS graph_subscriptions_account_id_resource_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_graph_subscriptions_account_wide
    ON graph_subscriptions (account_id, resource)
    WHERE owner_user_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_graph_subscriptions_per_owner
    ON graph_subscriptions (account_id, resource, owner_user_id)
    WHERE owner_user_id IS NOT NULL;
