-- Migration 0015: slack_integrations table
-- Stores Slack workspace OAuth tokens for each connected account.

CREATE TABLE IF NOT EXISTS slack_integrations (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    workspace_id   TEXT        NOT NULL,   -- Slack team ID, e.g. "T1234567"
    workspace_name TEXT        NOT NULL,
    bot_token      TEXT        NOT NULL,   -- xoxb-... bot token (plaintext for dev)
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, workspace_id)
);

CREATE INDEX IF NOT EXISTS idx_slack_integrations_account
    ON slack_integrations (account_id);
