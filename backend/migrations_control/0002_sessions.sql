-- Migration: 0002_sessions
-- Description: Session tokens and email-based auth codes for magic-link / 6-digit code auth
--
-- Down:
--   DROP INDEX  IF EXISTS idx_auth_codes_email;
--   DROP TABLE  IF EXISTS auth_codes;
--   DROP TABLE  IF EXISTS sessions;

-- ── Sessions ──────────────────────────────────────────────────────────────────
-- One row per active device session.  Raw token is never stored — only its
-- SHA-256 hex digest so a DB breach cannot replay sessions.

CREATE TABLE IF NOT EXISTS sessions (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id    TEXT        NOT NULL,
    token_hash   TEXT        NOT NULL UNIQUE,   -- SHA-256 of the raw token (hex)
    expires_at   TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Auth codes ────────────────────────────────────────────────────────────────
-- Short-lived 6-digit one-time codes sent by email.  The raw code is never
-- stored — only its SHA-256 hex digest.

CREATE TABLE IF NOT EXISTS auth_codes (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email      TEXT        NOT NULL,
    code_hash  TEXT        NOT NULL,           -- SHA-256 of the 6-digit code
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_auth_codes_email ON auth_codes(email, expires_at);
