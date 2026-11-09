-- Migration: 0082_org_revocations_and_membership_guard
-- Description: Org-LEVEL revocation + a database-enforced membership guard
--              (org-tenancy PR 2.2 re-review P1). The per-member tombstones
--              (0081) cannot express "nobody may join this deleted org": a
--              user added to the Clerk org who NEVER signed into this backend
--              has no account_members row at `organization.deleted` time, so
--              per-member enumeration gives them no tombstone — replaying
--              their still-valid org token against any require_auth route
--              would then INSERT a fresh membership on the deleted org and
--              re-arm /mcp read access to its retained data.
--
--              Two pieces:
--
--              1. `org_revocations` — one row per revoked Clerk org, written
--                 by `clerk_provision::revoke_org_access` (the
--                 `organization.deleted` webhook). Consulted by BOTH the
--                 request-path membership refresher
--                 (`refresh_org_membership_from_claims`) and /mcp's
--                 RequireExisting membership lookup. Keyed by clerk_org_id
--                 TEXT (same rationale as 0081: survives org-account row
--                 deletion/recreation). Never cleared: a deleted Clerk org
--                 never comes back under the same id (Clerk org ids are
--                 unique per creation).
--
--              2. `account_members_block_revoked` — a DEFERRABLE INITIALLY
--                 DEFERRED constraint trigger that hard-blocks ANY
--                 account_members INSERT whose target org account has an
--                 org-level revocation or a matching per-member tombstone.
--                 Defense in depth the application cannot bypass: it also
--                 backstops every current and future app-side writer (e.g.
--                 the legacy /teams endpoints, now gated off for org
--                 accounts) AND closes the read-committed TOCTOU on the
--                 app-side NOT EXISTS guards — being DEFERRED, it re-checks
--                 at COMMIT with a fresh snapshot, so an insert racing a
--                 concurrent offboarding transaction aborts instead of
--                 committing a resurrected row. The webhook re-add path
--                 (`ensure_org_membership`) deletes the tombstone in the
--                 SAME transaction as its insert, so its own commit-time
--                 check passes. Personal accounts (clerk_org_id IS NULL)
--                 return immediately — one PK lookup of overhead per
--                 (infrequent) membership insert.
--
-- Depends on: 0001_init (accounts, account_members), 0027_clerk_identity
--             (accounts.clerk_org_id), 0081_org_membership_tombstones
--
-- Down:
--   DROP TRIGGER IF EXISTS account_members_block_revoked ON account_members;
--   DROP FUNCTION IF EXISTS account_members_block_revoked();
--   DROP TABLE IF EXISTS org_revocations;

CREATE TABLE IF NOT EXISTS org_revocations (
    clerk_org_id TEXT        PRIMARY KEY,
    revoked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION account_members_block_revoked() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_org_id TEXT;
BEGIN
    SELECT clerk_org_id INTO v_org_id FROM accounts WHERE id = NEW.account_id;
    -- Personal account (or account row already gone by commit time): the
    -- revocation model does not govern it.
    IF v_org_id IS NULL THEN
        RETURN NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = v_org_id) THEN
        RAISE EXCEPTION
            'account_members insert blocked: org % is revoked (organization.deleted)', v_org_id
            USING ERRCODE = 'check_violation';
    END IF;
    IF EXISTS (SELECT 1 FROM org_membership_tombstones t
                WHERE t.clerk_org_id = v_org_id AND t.user_id = NEW.user_id) THEN
        RAISE EXCEPTION
            'account_members insert blocked: membership (org %, user %) is tombstoned', v_org_id, NEW.user_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL; -- AFTER trigger: return value is ignored
END $$;

DROP TRIGGER IF EXISTS account_members_block_revoked ON account_members;
CREATE CONSTRAINT TRIGGER account_members_block_revoked
    AFTER INSERT ON account_members
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION account_members_block_revoked();
