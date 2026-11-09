-- Migration: 0006_dek
-- Description: Add per-account data encryption key (DEK) for envelope encryption.
--
-- Each account has a unique DEK wrapped (encrypted) with the master KEK that
-- lives in Fly.io secrets as MASTER_KEK_HEX.  The wrapped bytes are stored
-- here; the plaintext DEK never touches the database.
--
-- Down:
--   ALTER TABLE accounts DROP COLUMN IF EXISTS dek_wrapped;

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS dek_wrapped BYTEA;
-- Will be populated on account creation (application enforces NOT NULL in practice
-- when MASTER_KEK_HEX is present; NULL is allowed in dev/test without the env var).
