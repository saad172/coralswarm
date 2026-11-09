-- Migration: 0254_connector_health_instance_key
-- Description: CS-456 / Phase G wave 2 item (S6) — capability-backing
--   registry. Extends `connector_health` (0243) so one account can hold
--   multiple instance-scoped degradations of the same capability (e.g. one
--   Chat space reader orphan vs another, one Teams container push vs another)
--   without collapsing them into a single row. `instance_key` defaults to ''
--   so every existing `backing_member_departed` / domain_directory row keeps
--   working under the widened primary key.
--
--   Also widens the `condition` CHECK to admit:
--     * `backing_member_departed` (0243 — unchanged)
--     * `backing_credential_lost` — last eligible backer's grant died and
--       nobody else can take over (shared capability; not a member's own
--       ReconsentNotice tile — founder Phase G decision 4 / CS-448)
--     * `no_eligible_backer` — re-election ran and found nobody
--
--   Control-plane only — 0243 has no tenant twin (connector-health is
--   bookkeeping about an org and a connector, never content).
--
-- Depends on: 0243_connector_health.
-- Down:
--   ALTER TABLE connector_health DROP CONSTRAINT IF EXISTS connector_health_condition_check;
--   ALTER TABLE connector_health ADD CONSTRAINT connector_health_condition_check
--     CHECK (condition IN ('backing_member_departed'));
--   ALTER TABLE connector_health DROP CONSTRAINT connector_health_pkey;
--   ALTER TABLE connector_health ADD PRIMARY KEY (account_id, provider, capability, condition);
--   ALTER TABLE connector_health DROP COLUMN IF EXISTS instance_key;

ALTER TABLE connector_health
    ADD COLUMN IF NOT EXISTS instance_key TEXT NOT NULL DEFAULT '';

-- Rebuild PK to include instance_key. Existing rows keep instance_key = ''.
ALTER TABLE connector_health DROP CONSTRAINT IF EXISTS connector_health_pkey;
ALTER TABLE connector_health
    ADD PRIMARY KEY (account_id, provider, capability, condition, instance_key);

-- Widen the condition CHECK. Postgres names an inline CHECK
-- `{table}_{column}_check` when no name is given at CREATE TABLE time.
ALTER TABLE connector_health DROP CONSTRAINT IF EXISTS connector_health_condition_check;
ALTER TABLE connector_health
    ADD CONSTRAINT connector_health_condition_check
    CHECK (condition IN (
        'backing_member_departed',
        'backing_credential_lost',
        'no_eligible_backer'
    ));
