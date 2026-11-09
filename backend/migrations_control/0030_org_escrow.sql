-- Migration: 0030_org_escrow
-- Description: B2B M-of-N org-escrow recovery storage (ADR-0007 D6). The server
--              stores ONLY opaque, client-encrypted blobs and stays blind:
--              - escrow_wrapped_keys: each member's private-ocean key wrapped
--                under the org recovery key.
--              - escrow_shares: each admin's Shamir share of the org recovery
--                key (M-of-N).
--              - escrow_recovery_log: an audit trail of recovery events.
--
-- Depends on: 0001_init (accounts, users)
--
-- Down:
--   DROP TABLE IF EXISTS escrow_recovery_log;
--   DROP TABLE IF EXISTS escrow_shares;
--   DROP TABLE IF EXISTS escrow_wrapped_keys;

CREATE TABLE IF NOT EXISTS escrow_wrapped_keys (
    account_id  UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id     UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    wrapped_key BYTEA       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, user_id)
);

CREATE TABLE IF NOT EXISTS escrow_shares (
    account_id    UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    admin_user_id UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    share         BYTEA       NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, admin_user_id)
);

CREATE TABLE IF NOT EXISTS escrow_recovery_log (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    recovered_user_id UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    initiated_by      UUID        REFERENCES users(id) ON DELETE SET NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_escrow_recovery_log_account
    ON escrow_recovery_log (account_id, created_at DESC);
