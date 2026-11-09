-- Migration: 0371_managed_reservation_extra_basis
-- Description: Settings launch plan PR 3.2. A managed reservation can now be
--   funded by extra usage (billing::funding_policy), so `funding_basis` gains
--   a third value, 'extra': the bound did not fit the plan allowance and was
--   admitted against the extra-usage balance and limits instead.
--
--   An 'extra' reservation holds its whole bound against extra usage until it
--   settles or is released; the settlement split itself is recomputed from the
--   live state under the account's financial lock, so the stored basis is an
--   admission hint, never the amount charged. The binding-immutability trigger
--   (0321) still pins the value once written. 'allowance' keeps requiring a
--   member period (CHECK from 0321); 'extra' does not, because a payer with no
--   period can still be funded by a personal account's extra usage.
--
--   ── extra_credits ──
--   An 'extra' reservation records HOW MUCH of its bound extra usage funds
--   (`extra_credits`, set at admission). Outstanding extra usage is then the
--   sum of `extra_credits`, and the plan-allowance holds count only
--   `bound_credits - extra_credits`. Counting the whole bound against both
--   (the first version) refused reservations the balance could cover. The
--   binding-immutability trigger now pins the column too.
--
--   Widening a CHECK only: every existing row ('allowance' / 'pool' / NULL for
--   released-unsent) already satisfies the new one. Added NOT VALID then
--   validated so the table is not held under an exclusive lock for a scan.
--
-- Depends on: 0321_managed_funded_dispatch.
-- Down:
--   ALTER TABLE managed_credit_reservations
--       DROP CONSTRAINT managed_credit_reservations_funding_basis_check;
--   ALTER TABLE managed_credit_reservations
--       ADD CONSTRAINT managed_credit_reservations_funding_basis_check
--       CHECK (funding_basis IN ('allowance','pool'));
--   (only after no 'extra' reservation exists)
--   ALTER TABLE managed_credit_reservations DROP CONSTRAINT
--       managed_credit_reservations_extra_basis_has_credits,
--       DROP CONSTRAINT managed_credit_reservations_extra_credits_check,
--       DROP COLUMN extra_credits;  (and restore 0321's trigger function)

SET LOCAL lock_timeout = '3s';

ALTER TABLE managed_credit_reservations
    DROP CONSTRAINT managed_credit_reservations_funding_basis_check;
ALTER TABLE managed_credit_reservations
    ADD CONSTRAINT managed_credit_reservations_funding_basis_check
    CHECK (funding_basis IN ('allowance', 'pool', 'extra')) NOT VALID;
ALTER TABLE managed_credit_reservations
    VALIDATE CONSTRAINT managed_credit_reservations_funding_basis_check;

ALTER TABLE managed_credit_reservations
    ADD COLUMN extra_credits NUMERIC;
-- Nullable, no default: metadata-only. Every existing row is allowance/pool.
ALTER TABLE managed_credit_reservations
    ADD CONSTRAINT managed_credit_reservations_extra_credits_check
    CHECK (extra_credits IS NULL OR (extra_credits > 0 AND bound_credits IS NOT NULL
        AND extra_credits <= bound_credits)) NOT VALID;
ALTER TABLE managed_credit_reservations
    VALIDATE CONSTRAINT managed_credit_reservations_extra_credits_check;
ALTER TABLE managed_credit_reservations
    ADD CONSTRAINT managed_credit_reservations_extra_basis_has_credits
    CHECK (funding_basis IS DISTINCT FROM 'extra' OR extra_credits IS NOT NULL) NOT VALID;
ALTER TABLE managed_credit_reservations
    VALIDATE CONSTRAINT managed_credit_reservations_extra_basis_has_credits;

CREATE OR REPLACE FUNCTION managed_credit_binding_immutable() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.plane,NEW.receipt_id,NEW.account_id,NEW.payer_id,NEW.graph_id,NEW.logical_call,
     NEW.request_hash,NEW.bound_micro_usd,NEW.bound_credits,NEW.member_period_id,
     NEW.conversion_effective_from,NEW.reference_cost_per_credit,NEW.funding_basis,NEW.created_at,
     NEW.extra_credits)
   IS DISTINCT FROM
    (OLD.plane,OLD.receipt_id,OLD.account_id,OLD.payer_id,OLD.graph_id,OLD.logical_call,
     OLD.request_hash,OLD.bound_micro_usd,OLD.bound_credits,OLD.member_period_id,
     OLD.conversion_effective_from,OLD.reference_cost_per_credit,OLD.funding_basis,OLD.created_at,
     OLD.extra_credits)
   OR (OLD.state<>'held' AND NEW IS DISTINCT FROM OLD)
 THEN RAISE EXCEPTION 'managed financial binding is immutable'; END IF;
 RETURN NEW;
END $$;
