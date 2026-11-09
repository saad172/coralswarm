-- Migration: 0080_user_account_prefs
-- Description: Per-(user, account) MCP write governance (org-tenancy PR 2.2,
--              docs/plans/org-tenancy-implementation-plan.md Phase 2). Replaces
--              the single per-user `user_prefs.mcp_write_enabled` flag (0060)
--              as the AUTHORITATIVE write-grant record: MCP write to an account
--              is allowed only when the (user_id, account_id) row here grants
--              it. `mcp::auth::effective_scopes` reads this row on every `/mcp`
--              request, so a grant/revoke takes effect on the caller's very
--              next call (no token re-mint required).
--
--              Precedence (documented here because this table defines it;
--              enforced in `mcp::auth::mcp_write_enabled_for_account`):
--                1. A `user_account_prefs` row for (user, active account) is
--                   authoritative when present — grant or deny.
--                2. No row + ORG account (`accounts.clerk_org_id IS NOT NULL`):
--                   DENY. Org write is granted explicitly at onboarding, never
--                   inherited from the legacy per-user flag.
--                3. No row + PERSONAL account: fall back to the legacy
--                   `user_prefs.mcp_write_enabled` flag (rollout back-compat).
--                   NOTE: the backfill below gives every user who HAS a
--                   `user_prefs` row an authoritative per-account row, so
--                   flipping the legacy flag after this migration is a NO-OP
--                   for all of them — grants and revocations must target
--                   `user_account_prefs`. This fallback only matters for a
--                   user with NO per-account row at all, i.e. one whose
--                   first-ever `user_prefs` row is written after this
--                   migration has already run.
--
--              The backfill is RESTRICTED to each user's personal account-of-one
--              (`clerk_org_id IS NULL`; at most one per user via 0028's partial
--              unique index). Deliberately NOT extended to org accounts the
--              user belongs to: the old flag meant "this user's client may
--              write to this user's own data" and must not silently widen into
--              "may write to the org's shared plane". This is also what makes
--              pre/post-migration behavior byte-identical for existing
--              personal-account MCP users (proved by
--              backend/tests/mcp_governance_pg.rs).
--
-- Depends on: 0001_init (users, accounts), 0027_clerk_identity
--             (accounts.clerk_org_id), 0028_accounts_owner_partial_unique
--             (single personal account per user), 0060_mcp_user_prefs
--             (user_prefs backfill source)
--
-- Down:
--   DROP TABLE IF EXISTS user_account_prefs;

CREATE TABLE IF NOT EXISTS user_account_prefs (
    user_id           UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    mcp_write_enabled BOOLEAN     NOT NULL DEFAULT false,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, account_id)
);

-- The PK covers the hot-path lookup (user_id, account_id). This index covers
-- the FK's delete side (account offboarding cascades) plus "who has a grant on
-- this account" admin queries — same rationale as 0007's
-- account_members(user_id) index.
CREATE INDEX IF NOT EXISTS user_account_prefs_account_id_idx
    ON user_account_prefs (account_id);

-- Backfill: personal accounts ONLY (see precedence note above). Copies the
-- legacy flag's value (true AND false — an explicit opt-out stays an explicit,
-- authoritative deny) onto the user's single personal account. Org accounts
-- get no row: deny-by-default until an explicit grant at org onboarding.
-- ON CONFLICT DO NOTHING keeps this idempotent if a per-account row was
-- somehow created before this migration ran.
INSERT INTO user_account_prefs (user_id, account_id, mcp_write_enabled, updated_at)
SELECT up.user_id, a.id, up.mcp_write_enabled, up.updated_at
FROM user_prefs up
JOIN accounts a
  ON a.owner_user_id = up.user_id
 AND a.clerk_org_id IS NULL
ON CONFLICT (user_id, account_id) DO NOTHING;
