-- Migration: 0081_org_membership_tombstones
-- Description: Webhook-authoritative org-membership removal (org-tenancy PR
--              2.2 security fix — offboarding-resurrection P0). When the Clerk
--              `organizationMembership.deleted` webhook removes a member
--              (`clerk_provision::remove_org_membership`), it now records a
--              tombstone here in the same transaction as the `account_members`
--              DELETE (and the `user_account_prefs` write-grant DELETE). The
--              request-path claims refresher
--              (`clerk_provision::refresh_org_membership_from_claims`, used by
--              `middleware::auth`'s ProvisionFromClaims policy) may INSERT an
--              org membership only when no tombstone exists — and never
--              updates an existing row at all. Only the webhook upsert
--              (`ensure_org_membership`, `organizationMembership.created`/
--              `.updated`) clears a tombstone: Clerk webhook events are the
--              only code path able to (re)create an org membership after a
--              removal. (Precisely: the legacy `/teams` account-membership
--              endpoints — the one other `account_members` writer — refuse
--              org accounts outright, and migration 0082 adds a deferred
--              constraint trigger that hard-blocks any tombstone- or
--              revocation-violating INSERT at the database level, so the
--              invariant holds even against writers that skip the guarded
--              provisioning functions.)
--
--              Why this table exists: without it, ANY `require_auth` request
--              carrying a still-valid (long-TTL) OAuth access token with
--              org_id/org_role claims re-upserted the very `account_members`
--              row the offboarding webhook had just deleted — durably
--              re-arming `/mcp`'s live membership re-check (PR 2.2's headline
--              revocation guarantee) for the token's whole TTL.
--
--              Keyed by (clerk_org_id TEXT, user_id) — deliberately NOT an
--              accounts(id) FK: if it FK'd the account row, deleting the org
--              account would cascade the tombstones away, after which a stale
--              token could resurrect both the account (`ensure_org_account`)
--              and its membership. The users(id) FK is safe — deleting the
--              user deletes the only principal the tombstone guards.
--
-- Depends on: 0001_init (users, account_members), 0027_clerk_identity
--             (accounts.clerk_org_id), 0080_user_account_prefs (the write
--             grant cleared alongside the membership on removal)
--
-- Down:
--   DROP TABLE IF EXISTS org_membership_tombstones;

CREATE TABLE IF NOT EXISTS org_membership_tombstones (
    clerk_org_id TEXT        NOT NULL,
    user_id      UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    removed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (clerk_org_id, user_id)
);
