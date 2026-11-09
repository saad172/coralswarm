-- Migration: 0028_accounts_owner_partial_unique
-- Description: Model B (ADR-0007 D2-impl) requires one user to own BOTH their
--              personal account-of-one AND any number of org accounts they
--              create. The original global UNIQUE(owner_user_id) (migration
--              0008) forbids that — a second account owned by the same user
--              (the org account) violates it. Relax it to a PARTIAL unique
--              index that enforces "at most one personal account per owner"
--              (clerk_org_id IS NULL) while leaving org accounts unconstrained
--              on owner.
--
-- Depends on: 0008_accounts_owner_unique, 0027_clerk_identity (clerk_org_id)
--
-- Down:
--   DROP INDEX IF EXISTS uq_accounts_one_personal_per_owner;
--   ALTER TABLE accounts ADD CONSTRAINT accounts_owner_user_id_key UNIQUE (owner_user_id);

ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_owner_user_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_one_personal_per_owner
    ON accounts (owner_user_id)
    WHERE clerk_org_id IS NULL;
