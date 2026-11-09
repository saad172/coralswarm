-- Migration: 0103_oauth_tokens_credential_invalid
-- Description: Schema-parity twin of
--   `migrations_control/0251_oauth_tokens_credential_invalid.sql` (CS-453,
--   Phase G wave 1, credential-death classification). `oauth_tokens` is a
--   SHARED table (`scripts/check_schema_parity.sh`'s `CONTROL_ONLY_RLS_TABLES`
--   roster), so any schema change to it must land identically on both trees
--   or new tenant DBs silently diverge — same discipline as tenant migration
--   `0060`'s pairing with control's `0177`.
--
--   Byte-identical column addition. No behavior change: nothing reads or
--   writes `oauth_tokens` on a tenant pool today — see control migration
--   `0251`'s own header for the full reasoning.
--
-- Down:
--   ALTER TABLE oauth_tokens DROP COLUMN IF EXISTS credential_invalid_reason;
--   ALTER TABLE oauth_tokens DROP COLUMN IF EXISTS credential_invalid_at;

ALTER TABLE oauth_tokens ADD COLUMN IF NOT EXISTS credential_invalid_at TIMESTAMPTZ;
ALTER TABLE oauth_tokens ADD COLUMN IF NOT EXISTS credential_invalid_reason TEXT;
