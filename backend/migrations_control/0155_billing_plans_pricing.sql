-- Migration: 0155_billing_plans_pricing
-- Description: real-pricing-and-limits PR — gives `plans` (migration
--   `0150_credit_ledger.sql`) a price, so a self-serve tier ladder can
--   actually be expressed, then seeds that ladder. `free` and `founder_comp`
--   (seeded in `0153_billing_plans_seed.sql`) are UNTOUCHED by this
--   migration — neither is self-serve purchasable, so neither needs a price.
--
--   ── Why `plans` had no price column until now ──
--   `0150`'s own header describes `plans` as "GLOBAL reference data" but
--   never claimed every plan would be PURCHASABLE — `0153` only ever seeded
--   two non-purchasable rows (a lifetime free tier, a founder/dogfood grant).
--   Without a price column there was no way to represent "here is a tier a
--   real customer can actually buy", which is the gap this migration closes.
--
--   ── Correction, mid-authoring: derived numbers superseded by the register
--   ── ──
--   This migration was FIRST drafted with three self-derived tiers
--   (`starter`/`pro`/`team`, no seat concept). That draft was wrong: it was
--   derived from unit economics alone, without checking the existing
--   decision register, and a founder-decided tier set already exists
--   ("Credit model carried from Billing Plan v2"). Registered numbers beat
--   derived ones. The `starter`/`team` codes below never shipped — do not
--   resurrect them; the three rows this migration actually seeds are `pro`,
--   `team_standard`, `team_premium` (see the seed block at the bottom).
--
--   ── price_usd_cents ──
--   NULL means "not self-serve purchasable" — `free` and `founder_comp` both
--   stay NULL after this migration runs (nothing below touches them). A
--   non-NULL value on a NON-seat-priced plan is the flat monthly price in
--   US cents. On a SEAT-PRICED plan (see `is_seat_priced` below) the exact
--   same column instead holds the PER-SEAT price in US cents — one column,
--   two meanings gated by `is_seat_priced`, rather than two mutually
--   exclusive nullable price columns, because no plan is ever both at once
--   (there is nothing to reconcile between a "flat price" and a "per-seat
--   price" on the same row).
--
--   ── Seat pricing — a second schema gap the register surfaced ──
--   The register prices `team_standard`/`team_premium` PER SEAT, with
--   `team_standard` additionally carrying a 3-seat minimum. `plans` had no
--   seat concept at all before this migration. Two new columns close that
--   gap:
--     * `is_seat_priced BOOL NOT NULL DEFAULT false` — whether `credits` and
--       `price_usd_cents` on this row are PER SEAT rather than flat-per-plan.
--     * `min_seats INT CHECK (min_seats IS NULL OR min_seats >= 1)` — the
--       minimum seat count a purchase of this plan must specify. NULL means
--       no enforced minimum (a seat-priced plan may still have no floor, as
--       `team_premium` does below; a NON-seat-priced plan always has NULL
--       here, enforced by the CHECK immediately below).
--   `plans_seat_minimum_requires_seat_pricing` CHECK (`min_seats IS NULL OR
--   is_seat_priced`) makes "a seat minimum on a flat-rate plan" structurally
--   impossible, in the same spirit as the pre-existing
--   `plans_period_length_matches_refresh_kind` CHECK two migrations up: a
--   contradictory row should fail loudly at INSERT, not silently exist
--   waiting for a reader to notice the minimum makes no sense.
--
--   ── `credits` semantics for a seat-priced row — READ BEFORE WIRING
--   ── ANYTHING TO THIS ──
--   For a seat-priced plan, `credits` is the PER-SEAT allowance: an
--   account's actual credit allowance is `credits * seats`, where `seats` is
--   however many seats that account purchased. **Nothing in this change
--   computes that multiplication** — there is no seat COUNT anywhere in this
--   schema yet (`grants` has no `seats` column, nothing tracks how many
--   seats an account bought). Stating this plainly rather than implying it
--   works: a `grants` row referencing `team_standard`/`team_premium` today
--   would need `credits_override` set by hand (per-account, at grant time)
--   to reflect the actual purchased seat count — the seat-count feature
--   itself is out of scope for this migration and is a real gap for
--   whichever change wires seat-priced self-serve purchasing end to end.
--
--   ── The three seeded tiers — REGISTERED (Billing Plan v2), not derived ──
--   Cost basis is $0.0044/credit (`credit_conversion.reference_cost_per_
--   credit`, seeded in `0148_model_rates.sql`) — how much real provider
--   spend one credit represents. Locked retail top-up price is $0.022/credit
--   (packs 500/$11, 2k/$44, 5k/$110, never-expire; memory
--   coralswarm-topup-pricing-decisions). Locked constraints that also govern
--   these three rows, restated here because they are exactly what a plans
--   table with a price column now makes possible to violate by accident:
--     * PLAN-FIRST — a plan is required before an account may buy a top-up
--       pack. (Not enforced by this migration; a future purchase-flow
--       change owns that gate. Recorded here so the next reader of `plans`
--       knows the assumption a self-serve price column exists to serve.)
--     * ROLLOVER REFUSED — an `anniversary` refresh RESETS the credit
--       allowance to the plan's `credits` value; it does NOT accumulate
--       unused credits from the prior period. This is the pre-existing
--       `refresh_kind = 'anniversary'` semantics `0150` already defined
--       (this migration adds no new refresh behavior), restated here
--       because it is directly relevant to reading the margin numbers below
--       (a customer who under-uses a period does not get to bank the
--       difference).
--     * NO FREE-TIER PACKS — top-up packs are only ever purchasable by an
--       account already on a PAID plan; the seeded `free` plan (`0153`,
--       untouched here) can never buy one. Also not enforced by this
--       migration (no top-up-purchase code exists yet); recorded for the
--       same reason as PLAN-FIRST above.
--
--   Registered rows and margin (an OBSERVATION derived from the registered
--   numbers below, not itself a decision this migration makes):
--     * `pro` — 1,500 credits/mo, $20.00/mo flat. $0.01333/credit ->
--       ~67% gross margin over the $0.0044 cost basis.
--     * `team_standard` — 3,000 credits/seat/mo, $40.00/seat/mo, 3-seat
--       minimum. $0.01333/credit/seat -> ~67% gross margin (identical
--       per-credit rate to `pro`; the "team" difference here is the
--       per-seat/minimum-seat purchase shape, not a different per-credit
--       price).
--     * `team_premium` — 15,000 credits/seat/mo, $175.00/seat/mo, no
--       enforced seat minimum. $0.01167/credit/seat -> ~62% gross margin.
--   Every one of these per-credit rates undercuts the $0.022/credit top-up
--   rate, same "subscription is cheaper per credit than a top-up" property
--   the earlier (now-superseded) derived draft also aimed for — this is a
--   property of the register's own numbers, not something this migration
--   had to engineer.
--
--   ── Known reconciliation gap, DEFERRED on purpose, not an oversight ──
--   `marketing-web` currently shows `$25`/`$100` query-quota tiers that
--   match NONE of the codes/prices/credit amounts in this migration. That
--   mismatch is real and is explicitly left unresolved here — do not "fix"
--   one side to match the other as a drive-by; reconciling marketing copy
--   with the register's actual `plans` rows is its own change, out of scope
--   for a migration whose job is only to make the registered tiers
--   representable and queryable.
--
--   `refresh_kind = 'anniversary'` REQUIRES `period_length IS NOT NULL`
--   (pre-existing `plans_period_length_matches_refresh_kind` CHECK) — all
--   three rows below use `INTERVAL '1 month'`, anniversary-from-grant-start
--   per `0150`'s own semantics (never a calendar-month boundary).
--
--   Idempotent (`ON CONFLICT (code) DO NOTHING`, same style as `0153`).
--
--   ── CI note ──
--   `plans` is ALREADY in `CONTROL_TABLES` in `scripts/check_schema_parity.sh`
--   (added in `0148`/`0150`'s own PRs, for `plans` itself) — verified while
--   authoring this migration; no change needed there. This migration adds
--   COLUMNS to an already-registered control-only table, not a new table.
--
-- Depends on: 0150_credit_ledger.sql (`plans` table + the
--   `plans_period_length_matches_refresh_kind` CHECK this migration's new
--   `plans_seat_minimum_requires_seat_pricing` CHECK is modeled on),
--   0153_billing_plans_seed.sql (`free`/`founder_comp`, untouched here),
--   0148_model_rates.sql (`credit_conversion.reference_cost_per_credit`, the
--   $0.0044 cost basis every margin figure above is computed against).
-- Down:
--   DELETE FROM plans WHERE code IN ('pro', 'team_standard', 'team_premium');
--   ALTER TABLE plans DROP CONSTRAINT IF EXISTS plans_seat_minimum_requires_seat_pricing;
--   ALTER TABLE plans DROP COLUMN IF EXISTS min_seats;
--   ALTER TABLE plans DROP COLUMN IF EXISTS is_seat_priced;
--   ALTER TABLE plans DROP COLUMN IF EXISTS price_usd_cents;

ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS price_usd_cents INT
        CHECK (price_usd_cents IS NULL OR price_usd_cents >= 0);

ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS is_seat_priced BOOL NOT NULL DEFAULT false;

ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS min_seats INT
        CHECK (min_seats IS NULL OR min_seats >= 1);

ALTER TABLE plans
    ADD CONSTRAINT plans_seat_minimum_requires_seat_pricing
        CHECK (min_seats IS NULL OR is_seat_priced);

-- ── Seed: registered self-serve tiers (Billing Plan v2) ────────────────────
-- `free`/`founder_comp` (0153) are not touched — both keep price_usd_cents
-- NULL (not self-serve purchasable), is_seat_priced false, min_seats NULL,
-- their existing `lifetime`/NULL refresh shape.
INSERT INTO plans
    (code, name, credits, price_usd_cents, is_seat_priced, min_seats, refresh_kind, period_length, is_active)
VALUES
    ('pro',            'Pro',            1500,  2000,  false, NULL, 'anniversary', INTERVAL '1 month', true),
    ('team_standard',  'Team Standard',  3000,  4000,  true,  3,    'anniversary', INTERVAL '1 month', true),
    ('team_premium',   'Team Premium',   15000, 17500, true,  NULL, 'anniversary', INTERVAL '1 month', true)
ON CONFLICT (code) DO NOTHING;
