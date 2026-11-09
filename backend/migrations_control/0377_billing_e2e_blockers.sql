-- Billing e2e blockers found by the staging Stripe test-mode run.
--
-- ── A. An abandoned or failed Checkout left the account unable to retry ─────
-- `ensure_stripe_customer` writes the customer-mapping row BEFORE Checkout
-- exists (so a racing webhook can resolve the customer). `status` defaulted to
-- 'active' and the CHECK offered no other "not subscribed yet" value, so that
-- mapping row read as a live subscription and `POST /v1/billing/checkout/
-- subscription` answered 409 "already has an active subscription" on every
-- retry.
--
-- 1. `status` gains 'pending': a customer exists, no subscription has been
--    confirmed. The checkout route inserts it explicitly; the
--    `checkout.session.completed` mirror UPDATE overwrites it with Stripe's
--    status. 'pending' is NOT frozen (`enforcement::status_is_frozen` is
--    past_due/canceled only), exactly like the old 'active' default for a row
--    with no subscription.
-- 2. `pending_checkout_session_id`: the open subscription Checkout Session, so
--    a new Checkout can expire the abandoned one (best effort) and
--    `checkout.session.expired` can clear it.
--
-- Existing mapping rows that already say 'active' with a NULL
-- `stripe_subscription_id` are NOT rewritten here: the table is behind the
-- 0321 financial entry fence, which a migration cannot enter. The route treats
-- "no subscription id" as "no subscription" regardless of `status`, so those
-- rows retry correctly without a backfill.
--
-- Rolling deploy: the pre-deploy backend never writes 'pending' (it inserts
-- the default), so the widened CHECK is safe in either order.
--
--
-- ── B. Replaying a used account-deletion cancel link ───────────────────────
-- `account_deletion_cancel` answered 'too_late' for a deletion that was
-- already 'cancelled', so a second click on the emailed link was a 409. It now
-- answers 'already_cancelled' (the route maps it to 200 {status:"cancelled"});
-- 'too_late' stays for running / completed. Body identical to 0370 otherwise
-- (same SECURITY DEFINER, same search_path with pg_temp last); CREATE OR
-- REPLACE keeps the owner and the EXECUTE grants. Rolling deploy: an older
-- backend maps any unknown word to too_late, i.e. today's behaviour.
--
-- Down:
--   -- B: re-run the account_deletion_cancel body from 0370.
--   ALTER TABLE stripe_subscriptions DROP COLUMN IF EXISTS pending_checkout_session_id;
--   -- only after no row says 'pending':
--   ALTER TABLE stripe_subscriptions DROP CONSTRAINT IF EXISTS stripe_subscriptions_status_check;
--   ALTER TABLE stripe_subscriptions ADD CONSTRAINT stripe_subscriptions_status_check
--       CHECK (status IN ('active', 'past_due', 'canceled'));
SET LOCAL lock_timeout = '3s';

ALTER TABLE stripe_subscriptions DROP CONSTRAINT IF EXISTS stripe_subscriptions_status_check;
ALTER TABLE stripe_subscriptions ADD CONSTRAINT stripe_subscriptions_status_check
    CHECK (status IN ('active', 'past_due', 'canceled', 'pending'));

ALTER TABLE stripe_subscriptions
    ADD COLUMN IF NOT EXISTS pending_checkout_session_id TEXT
        CHECK (pending_checkout_session_id IS NULL OR pending_checkout_session_id <> '');

CREATE OR REPLACE FUNCTION public.account_deletion_cancel(p_id UUID)
RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
DECLARE st TEXT;
BEGIN
    UPDATE public.account_deletions
       SET state = 'cancelled', cancelled_at = now(), lease_until = NULL, updated_at = now()
     WHERE id = p_id AND state = 'grace' AND initiated_by = 'user';
    IF FOUND THEN RETURN 'cancelled'; END IF;
    SELECT state INTO st FROM public.account_deletions WHERE id = p_id;
    IF st IS NULL THEN RETURN 'not_found'; END IF;
    IF st = 'cancelled' THEN RETURN 'already_cancelled'; END IF;
    RETURN 'too_late';
END $$;
