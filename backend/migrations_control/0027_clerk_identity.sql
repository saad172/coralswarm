-- Migration: 0027_clerk_identity
-- Description: Map Clerk identities onto the internal tenancy model so the
--              backend can adopt Clerk as the auth provider (ADR-0007 D3/D4,
--              Model B). A Clerk user maps to a `users` row; a Clerk
--              Organization maps 1:1 to an org `accounts` row. Personal
--              accounts-of-one keep `clerk_org_id = NULL`.
--
-- Depends on: 0001_init (users, accounts)
--
-- Down:
--   DROP INDEX IF EXISTS uq_accounts_clerk_org_id;
--   ALTER TABLE accounts DROP COLUMN IF EXISTS clerk_org_id;
--   DROP INDEX IF EXISTS uq_users_clerk_user_id;
--   ALTER TABLE users DROP COLUMN IF EXISTS clerk_user_id;

-- ── users.clerk_user_id ────────────────────────────────────────────────────────
-- The Clerk user id (`user_...`, the JWT `sub`). NULL for legacy email-code users
-- until they sign in via Clerk. Unique among non-NULL values.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS clerk_user_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_clerk_user_id
    ON users (clerk_user_id)
    WHERE clerk_user_id IS NOT NULL;

-- ── accounts.clerk_org_id ──────────────────────────────────────────────────────
-- The Clerk Organization id (`org_...`) this account mirrors. NULL ⇒ a personal
-- account-of-one (the user's private home, ADR-0007 D2-impl). Unique among
-- non-NULL values: one account per Clerk org.
ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS clerk_org_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_clerk_org_id
    ON accounts (clerk_org_id)
    WHERE clerk_org_id IS NOT NULL;
