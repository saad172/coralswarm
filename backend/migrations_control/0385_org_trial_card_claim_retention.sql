-- Migration: 0385_org_trial_card_claim_retention
-- Description: Card-fingerprint trial claims are kept for 24 months, then
--   deleted (#1540 review: retention for the anti-abuse record 0382 made).
--
--   `org_trial_claims` (0382) records who has already had a trial: one row
--   per user (`users.id`) and one per card (the Stripe card fingerprint).
--   It is kept on account deletion on purpose (founder decision #9: one
--   trial per card), but a card fingerprint is still data about a person,
--   and a card claim older than two years no longer earns its keep for
--   fraud prevention. So the self-serve cleanup sweep
--   (`tenancy::org_selfserve_cleanup`, `sweep_retention`) calls this
--   definer on every pass.
--
--   The app role holds no privilege on `org_trial_claims` (0382 asserts
--   it), so the delete is a SECURITY DEFINER with the retention fixed in
--   its body: the caller chooses only how many rows a call may delete, never
--   which, so a compromised app role can expire nothing newer than 24
--   months. User claims are not touched (they hold a `users.id`, which after
--   a deletion points at an anonymised row).
--
--   The partial index serves the scan; the table is small, but the sweep
--   runs every few minutes.

CREATE INDEX IF NOT EXISTS org_trial_claims_card_claimed_idx
    ON public.org_trial_claims (claimed_at)
    WHERE claim_kind = 'card';

-- Deletes up to `p_limit` card claims older than 24 months, oldest first.
-- Returns how many were deleted.
CREATE OR REPLACE FUNCTION public.org_trial_claims_expire_cards(p_limit INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    n INTEGER;
BEGIN
    DELETE FROM public.org_trial_claims c
     WHERE c.claim_kind = 'card'
       AND (c.claim_kind, c.claim_value) IN (
             SELECT o.claim_kind, o.claim_value FROM public.org_trial_claims o
              WHERE o.claim_kind = 'card' AND o.claimed_at < now() - interval '24 months'
              ORDER BY o.claimed_at
              LIMIT greatest(coalesce(p_limit, 0), 0));
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END $$;

REVOKE ALL ON FUNCTION public.org_trial_claims_expire_cards(INTEGER) FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
        GRANT EXECUTE ON FUNCTION public.org_trial_claims_expire_cards(INTEGER) TO coralswarm_app;
        IF has_table_privilege('coralswarm_app', 'public.org_trial_claims', 'SELECT,INSERT,UPDATE,DELETE') THEN
            RAISE EXCEPTION 'coralswarm_app must not hold any privilege on org_trial_claims';
        END IF;
    END IF;
END $$;
