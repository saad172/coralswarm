-- Migration: 0031_user_private_keys
-- Description: Cross-device enrollment for the server-blind private ocean
--              (ADR-0007 D5). Stores the user's private-ocean key wrapped under
--              their PASSPHRASE (distinct from the org-escrow blob, which is
--              wrapped under the org recovery key). A new device fetches this
--              opaque blob and the user enters their passphrase once to enroll;
--              the server never sees the passphrase or the unwrapped key.
--
-- Depends on: 0001_init (users)
--
-- Down:
--   DROP TABLE IF EXISTS user_private_keys;

CREATE TABLE IF NOT EXISTS user_private_keys (
    user_id    UUID        PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    blob       TEXT        NOT NULL, -- opaque: { salt, wrapped } (base64), server-blind
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
