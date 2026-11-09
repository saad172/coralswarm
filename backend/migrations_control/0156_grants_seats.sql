-- Migration: 0156_grants_seats
-- Description: gives `grants` (migration `0150_credit_ledger.sql`) a SEAT
--   COUNT, closing the gap migration `0155_billing_plans_pricing.sql`
--   documented against itself in its own header:
--
--     "**Nothing in this change computes that multiplication** — there is no
--      seat COUNT anywhere in this schema yet (`grants` has no `seats`
--      column, nothing tracks how many seats an account bought)."
--
--   Until this migration, a `team_standard` grant issued 3,000 credits
--   TOTAL rather than 3,000 PER SEAT — so a 10-seat customer paying
--   $400/month received the same allowance as a 3-seat customer paying
--   $120/month. That is not a rounding error; it is the difference between
--   a sellable Team plan and an unsellable one, which is why 0155's header
--   calls it out by name as "a real gap for whichever change wires
--   seat-priced self-serve purchasing end to end". This is that change's
--   schema half.
--
--   ── Entitlement formula, stated once, here ──
--   An account's credit allowance from one grant is:
--
--       COALESCE(grants.credits_override, plans.credits * COALESCE(grants.seats, 1))
--
--   `credits_override` keeps its pre-existing meaning of an ABSOLUTE total
--   that wins over everything — it is NOT a per-seat override. That
--   distinction is load-bearing: 0155's header told operators to reach for
--   `credits_override` precisely BECAUSE seats did not exist ("would need
--   `credits_override` set by hand ... to reflect the actual purchased seat
--   count"). Any grant written that way before this migration already holds
--   a correct TOTAL, so leaving `credits_override` as a total is what keeps
--   those rows right; re-reading it as per-seat would silently multiply
--   them. `COALESCE(seats, 1)` makes flat plans (`pro`, `free`,
--   `founder_comp` — all `seats IS NULL`) arithmetically unchanged.
--
--   The two call sites that compute this are
--   `backend/src/routes/usage.rs` (the plan summary the billing UI reads)
--   and `backend/src/bin/grant_plan.rs` (which decides the positive
--   `credit_ledger` entry a grant writes). Both are updated in this same PR.
--
--   ── Why a denormalized `plan_is_seat_priced` column ──
--   The invariant that actually matters is "a grant for a seat-priced plan
--   MUST carry a seat count, and a grant for a flat plan MUST NOT". Getting
--   that wrong in the permissive direction is the exact bug this migration
--   exists to kill: a `team_standard` grant with `seats IS NULL` would fall
--   through `COALESCE(seats, 1)` and quietly issue the ONE-SEAT allowance
--   to a customer who bought ten — reintroducing the pre-0156 bug through a
--   NULL rather than a missing column, and reintroducing it silently.
--
--   A plain CHECK cannot express this: the deciding fact
--   (`plans.is_seat_priced`) lives in ANOTHER TABLE, and Postgres CHECK
--   constraints may not reference one. The three ways out are a trigger
--   (procedural, invisible to a schema reader, and this tree uses none), an
--   application-level assertion (fine until the SECOND writer — the Stripe
--   webhook now being built — forgets it), or the standard relational
--   move used here: denormalize the deciding column and let a composite
--   FOREIGN KEY keep the copy honest.
--
--     * `plans` gains `UNIQUE (id, is_seat_priced)` — redundant given `id`
--       is already the primary key, and existing purely to be a legal
--       composite FK target.
--     * `grants.plan_is_seat_priced BOOL NOT NULL` mirrors the referenced
--       plan's flag, held in agreement by
--       `grants_plan_seat_pricing_fk (plan_id, plan_is_seat_priced)
--        REFERENCES plans (id, is_seat_priced)`. The copy CANNOT drift: a
--       row claiming a pricing shape its plan does not have has no matching
--       parent key and is rejected.
--     * `grants_seats_iff_seat_priced CHECK (plan_is_seat_priced = (seats IS
--       NOT NULL))` then states the real invariant using only columns of
--       THIS table, which is what makes it expressible at all.
--
--   Net effect: "seat-priced grant with no seats" and "flat-plan grant
--   carrying a seat count" are both structurally impossible, in exactly the
--   spirit 0155 invoked for
--   `plans_seat_minimum_requires_seat_pricing` — "a contradictory row
--   should fail loudly at INSERT, not silently exist waiting for a reader
--   to notice". The cost is one redundant boolean per grant, paid to make
--   the invariant hold for every future writer rather than only for the
--   writers that remember it.
--
--   Deliberately NO `ON UPDATE CASCADE`: flipping a live plan's
--   `is_seat_priced` while grants reference it now fails loudly instead of
--   silently rewriting what existing customers were sold. Changing a plan's
--   pricing SHAPE should mean introducing a new plan `code`, not mutating
--   one people already hold — and if it genuinely must be mutated, that is
--   a deliberate migration that also decides what happens to every
--   outstanding grant, not a cascade nobody reviews.
--
--   The pre-existing single-column `grants.plan_id -> plans(id)` FK from
--   `0150` is INTENTIONALLY left in place alongside the new composite one.
--   It is strictly redundant (the composite FK already implies it) but
--   dropping it would be a silent widening if the composite constraint were
--   ever removed later, and it costs nothing to keep.
--
--   ── `min_seats` is enforced in CODE, not here, and that is deliberate ──
--   `plans.min_seats` (0155 — `team_standard` carries 3) is a POLICY FLOOR
--   on what may be SOLD, not an arithmetic invariant of the ledger. It is
--   cross-table for the same reason as above, and unlike the seats/pricing
--   pairing, violating it cannot corrupt any number: a 1-seat
--   `team_standard` grant computes a perfectly correct 3,000-credit
--   allowance for a perfectly correct $40 charge. It is a revenue-policy
--   leak, not a wrong balance. So it is checked where the sale happens
--   (`grant_plan.rs` today, the Stripe checkout/webhook path next) and left
--   out of the schema rather than pushed into a trigger for symmetry's
--   sake.
--
--   ── Pre-flight guard on existing rows ──
--   The `DO` block below runs BEFORE the constraints and aborts with an
--   explicit message if any live grant already references a seat-priced
--   plan, because such a row's true seat count is UNKNOWABLE from the
--   schema and this migration must not invent one. Defaulting it to 1 (or
--   to `min_seats`) would be precisely the quiet wrong number the rest of
--   this migration exists to prevent. In practice this block is expected to
--   find nothing: `team_standard`/`team_premium` were seeded by 0155 one
--   commit ago, nothing can purchase them (there is no Stripe integration),
--   and `grant_plan.rs` had no way to specify seats — so no such grant can
--   exist yet. The guard is here for the case where that reasoning is wrong
--   on some database, where a clear abort beats a bare constraint
--   violation.
--
-- Depends on: 0150_credit_ledger.sql (`grants`, `plans`, and `grants`'
--   `account_isolation` RLS policy, which this migration does not alter —
--   adding columns does not change row visibility),
--   0155_billing_plans_pricing.sql (`plans.is_seat_priced`, without which
--   the composite FK target column does not exist).
--
-- CI note: `grants` and `plans` are BOTH already registered in
--   `scripts/check_schema_parity.sh`'s CONTROL_TABLES list, so they are
--   control-only by construction and this migration correctly has NO paired
--   tenant migration. Verified against that script while authoring: the
--   list reads "... credit_ledger credit_balances plans grants
--   enforcement_events ...". This migration adds COLUMNS to
--   already-registered control-only tables and introduces no new table.
--
-- Down:
--   ALTER TABLE grants DROP CONSTRAINT IF EXISTS grants_seats_iff_seat_priced;
--   ALTER TABLE grants DROP CONSTRAINT IF EXISTS grants_plan_seat_pricing_fk;
--   ALTER TABLE grants DROP COLUMN IF EXISTS plan_is_seat_priced;
--   ALTER TABLE grants DROP COLUMN IF EXISTS seats;
--   ALTER TABLE plans DROP CONSTRAINT IF EXISTS plans_id_is_seat_priced_key;

-- ── Pre-flight: refuse to guess a seat count for an existing grant ────────
DO $$
DECLARE
    offending INT;
BEGIN
    SELECT count(*) INTO offending
    FROM grants g
    JOIN plans p ON p.id = g.plan_id
    WHERE p.is_seat_priced;

    IF offending > 0 THEN
        RAISE EXCEPTION
            'migration 0156 cannot proceed: % grant row(s) already reference a '
            'seat-priced plan, and this migration will not invent a seat count '
            'for them. RECOVERY (grants.seats does NOT exist yet, so it cannot '
            'be set, and revoking does not help — revoked rows are constrained '
            'too): either DELETE the offending rows, listed by  SELECT g.id, '
            'g.account_id, p.code FROM grants g JOIN plans p ON p.id = g.plan_id '
            'WHERE p.is_seat_priced;  then re-run this migration — or, to keep '
            'them, run this migration''s ALTER statements by hand, populate '
            'grants.seats for those rows, and only then record 0156 as applied.',
            offending;
    END IF;
END
$$;

-- ── The composite FK target on `plans` ───────────────────────────────────
-- Redundant against the primary key on `id` alone; exists solely so
-- `grants (plan_id, plan_is_seat_priced)` has something legal to reference.
ALTER TABLE plans
    ADD CONSTRAINT plans_id_is_seat_priced_key UNIQUE (id, is_seat_priced);

-- ── Seats on `grants` ────────────────────────────────────────────────────
ALTER TABLE grants
    ADD COLUMN IF NOT EXISTS seats INT
        CHECK (seats IS NULL OR seats >= 1);

ALTER TABLE grants
    ADD COLUMN IF NOT EXISTS plan_is_seat_priced BOOL;

-- Backfill the mirror for every pre-existing grant. Safe by construction:
-- the pre-flight guard above proved every existing grant references a
-- NON-seat-priced plan, so every value written here is `false` and every
-- row therefore satisfies `grants_seats_iff_seat_priced` (`false = (NULL IS
-- NOT NULL)` -> `false = false`) without needing a seat count.
UPDATE grants g
   SET plan_is_seat_priced = p.is_seat_priced
  FROM plans p
 WHERE p.id = g.plan_id
   AND g.plan_is_seat_priced IS NULL;

ALTER TABLE grants
    ALTER COLUMN plan_is_seat_priced SET NOT NULL;

-- The copy cannot drift from the plan it mirrors.
ALTER TABLE grants
    ADD CONSTRAINT grants_plan_seat_pricing_fk
        FOREIGN KEY (plan_id, plan_is_seat_priced)
        REFERENCES plans (id, is_seat_priced);

-- The invariant this whole migration exists for, now expressible because
-- both columns live on this table.
ALTER TABLE grants
    ADD CONSTRAINT grants_seats_iff_seat_priced
        CHECK (plan_is_seat_priced = (seats IS NOT NULL));
