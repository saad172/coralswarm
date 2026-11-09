-- Migration 0006: oauth_tokens table
-- Stores OAuth access/refresh tokens encrypted at rest per account DEK.
--
-- Down:
--   DROP INDEX IF EXISTS idx_oauth_tokens_account;
--   DROP TABLE IF EXISTS oauth_tokens;

CREATE TABLE IF NOT EXISTS oauth_tokens (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id            UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    provider              TEXT        NOT NULL,   -- 'slack' | 'google' | 'notion' | 'linear'
    scope                 TEXT        NOT NULL,   -- space-separated OAuth scopes granted
    access_token          BYTEA       NOT NULL,   -- AES-256-GCM encrypted with account DEK
    refresh_token         BYTEA,                  -- encrypted; NULL for providers without refresh
    token_type            TEXT        NOT NULL DEFAULT 'Bearer',
    expires_at            TIMESTAMPTZ,            -- NULL = non-expiring
    provider_user_id      TEXT,                   -- provider's user/workspace ID for dedup
    provider_workspace_id TEXT,                   -- e.g. Slack workspace ID
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, provider, provider_workspace_id)
);

CREATE INDEX IF NOT EXISTS idx_oauth_tokens_account
    ON oauth_tokens (account_id, provider);
