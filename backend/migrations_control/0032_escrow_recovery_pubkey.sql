-- Migration: 0032_escrow_recovery_pubkey
-- Description: Org recovery PUBLIC key storage for asymmetric M-of-N escrow
--              (ADR-0007 D6). The recovery key is now an RSA keypair: the PUBLIC
--              half is published here so every org member can wrap their own
--              private-ocean key to it at enrollment (per-user escrow, no shared
--              secret handed around). The PRIVATE half is Shamir-split and held
--              by admins out-of-band — it is NEVER stored server-side, so the
--              server cannot reconstruct it and stays blind. `threshold` /
--              `admin_count` are stored as ceremony metadata (display only).
--
-- Depends on: 0030_org_escrow (accounts, users), 0001_init
--
-- Down:
--   DROP TABLE IF EXISTS escrow_recovery_pubkey;

CREATE TABLE IF NOT EXISTS escrow_recovery_pubkey (
    account_id  UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    pubkey      BYTEA       NOT NULL,
    threshold   INT         NOT NULL,
    admin_count INT         NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
