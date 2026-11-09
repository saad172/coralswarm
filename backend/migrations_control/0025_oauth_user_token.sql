-- Migration 0025: add user_token_encrypted column to oauth_tokens
-- Stores the Slack user-scoped token (xoxp-…) encrypted with the same
-- AES-256-GCM / derive_account_dek scheme as the bot access_token.
-- NULL when a user_scope was not requested or not granted during the OAuth flow.
--
-- Down:
--   ALTER TABLE oauth_tokens DROP COLUMN IF EXISTS user_token_encrypted;

ALTER TABLE oauth_tokens
    ADD COLUMN IF NOT EXISTS user_token_encrypted BYTEA;
