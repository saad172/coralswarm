-- Migration: 0255_teams_container_doorbell
-- Description: CS-459 / Phase G wave 3 item 56 (S5a) — the Teams channel/chat
--   change-notification subscription is a doorbell. Graph allows one
--   chatMessage subscription per app-and-channel (or app-and-chat), with no
--   user or account dimension. These two control-plane tables are that
--   registry: one subscription row per (Microsoft tenant, resource), and one
--   watcher row per connected member. Message content never lands here —
--   only resource ids, so the webhook can ring every watcher and each
--   watcher fetches on their own token.
--
--   Control-plane only. The Graph webhook already looks subscriptions up on
--   the control pool; a twin on the tenant pool would split one Graph slot
--   across databases (the bug this table exists to avoid). No RLS: a single
--   row is shared by every account watching that container, the same
--   posture as `connector_installs` (a routing index, not one account's
--   content). `coralswarm_app` sees the rows because the tables are not
--   RLS-enabled; there is no account_isolation policy that could hide one
--   account's watcher from the doorbell fan-out.
--
--   Failover (reauthorizationRequired, subscriptionRemoved, credential
--   death, departure, admin health rows) is S5b and is not in this migration.
--
-- Depends on: 0254_connector_health_instance_key (numbering only).
-- Down:
--   DROP TABLE IF EXISTS teams_container_watchers;
--   DROP TABLE IF EXISTS teams_container_subscriptions;

CREATE TABLE teams_container_subscriptions (
    ms_tenant_id         TEXT        NOT NULL,
    resource             TEXT        NOT NULL,
    subscription_id      TEXT,
    client_state         TEXT        NOT NULL,
    expires_at           TIMESTAMPTZ,
    owner_account_id     UUID        REFERENCES accounts(id) ON DELETE SET NULL,
    owner_user_id        UUID,
    state                TEXT        NOT NULL DEFAULT 'electing'
        CHECK (state IN ('live', 'electing', 'polling')),
    polling_reason       TEXT,
    blocked_until        TIMESTAMPTZ,
    last_owner_change_at TIMESTAMPTZ,
    last_error           TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ms_tenant_id, resource)
);

-- NULL subscription_id (row not yet created upstream) is allowed many times;
-- a live Graph id is not.
CREATE UNIQUE INDEX teams_container_subscriptions_subscription_id_uq
    ON teams_container_subscriptions (subscription_id)
    WHERE subscription_id IS NOT NULL;

CREATE TABLE teams_container_watchers (
    ms_tenant_id     TEXT        NOT NULL,
    resource         TEXT        NOT NULL,
    account_id       UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id          UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Election order after the sticky owner: most recent successful fetch.
    -- Bookkeeping only — never message content.
    last_fetch_ok_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ms_tenant_id, resource, account_id, user_id)
);

CREATE INDEX teams_container_watchers_member_idx
    ON teams_container_watchers (account_id, user_id, resource);
