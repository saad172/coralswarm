-- Migration: 0010_auth_verify_attempts
-- Description: Track per-code verify attempt count for brute-force protection
--
-- Down:
--   ALTER TABLE auth_codes DROP COLUMN IF EXISTS verify_attempts;

ALTER TABLE auth_codes ADD COLUMN IF NOT EXISTS verify_attempts INT NOT NULL DEFAULT 0;
