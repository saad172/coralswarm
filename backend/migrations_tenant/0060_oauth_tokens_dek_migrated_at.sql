-- Migration: 0060_oauth_tokens_dek_migrated_at
-- Description: Schema-parity twin of
--   `migrations_control/0177_oauth_tokens_dek_migrated_at.sql` (Foundation
--   train PR 1.2, CS-031 wrapped-DEK cutover). `oauth_tokens` is a SHARED
--   table (`scripts/check_schema_parity.sh`'s `CONTROL_ONLY_RLS_TABLES`
--   roster), so any schema change to it must land identically on both trees
--   or new tenant DBs silently diverge — same discipline as tenant migration
--   `0059`'s pairing with control's `0175`.
--
--   Byte-identical column addition. No behavior change: nothing reads or
--   writes `oauth_tokens` on a tenant pool today (Microsoft/per-member OAuth
--   is Train 3, not built) — see control migration `0177`'s own header for
--   the full justification.
--
-- Down:
--   ALTER TABLE oauth_tokens DROP COLUMN IF EXISTS dek_migrated_at;

ALTER TABLE oauth_tokens ADD COLUMN IF NOT EXISTS dek_migrated_at TIMESTAMPTZ;
