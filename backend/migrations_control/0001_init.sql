-- Migration: 0001_init
-- Description: Initial schema — users, accounts, account_members
--
-- Rollback path (sqlx does not auto-run "Down" sections, but keeping the
-- reversal SQL here makes manual rollbacks or future tooling straightforward):
--
-- Down:
--   DROP TABLE IF EXISTS account_members;
--   DROP TABLE IF EXISTS accounts;
--   DROP TABLE IF EXISTS users;

-- ── Users ─────────────────────────────────────────────────────────────────────
-- One row per authenticated identity (Clerk user_id maps to this id via the
-- auth layer added in a later story).

CREATE TABLE IF NOT EXISTS users (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email      TEXT        UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

-- ── Accounts ──────────────────────────────────────────────────────────────────
-- An account is the billing/team unit.  The user who creates it becomes the
-- first owner (enforced by account_members).

CREATE TABLE IF NOT EXISTS accounts (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    plan_tier     TEXT        NOT NULL DEFAULT 'free',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Account members ───────────────────────────────────────────────────────────
-- Many-to-many join between accounts and users with an RBAC role column.

CREATE TABLE IF NOT EXISTS account_members (
    account_id UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id    UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    role       TEXT        NOT NULL CHECK (role IN ('owner', 'admin', 'member')),
    joined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, user_id)
);
