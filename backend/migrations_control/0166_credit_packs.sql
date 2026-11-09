-- Migration: 0166_credit_packs
-- Description: billing self-serve launch PR B (2/5) — the top-up-pack
--   catalog (plan §1.10: "unified prepaid credits" overage model). Registered
--   packs (memory coralswarm-topup-pricing-decisions, restated in
--   `.scratch/billing-self-serve/plan.md` §1.10 and echoed in 0155's own
--   margin note): 500 credits/$11, 2,000 credits/$44, 5,000 credits/$110 —
--   all $0.022/credit, never-expire.
--
--   This migration creates the catalog and seeds those three rows only. It
--   does not implement PLAN-FIRST or NO-FREE-TIER-PACKS (0155's header
--   already named both as future purchase-flow gates, not schema
--   constraints) and it does not touch the ledger — crediting a purchased
--   pack onto `credit_balances` is PR D's `checkout.session.completed`
--   (payment mode) handler, wired against the price ids this table's
--   `stripe_price_id` column will hold once PR C's provisioning script runs.
--
--   ── Same shape as `seat_tiers` (0165), same reasoning ──
--   A pack is catalog/reference data exactly like a plan or a seat tier: one
--   row per purchasable price point, a nullable `stripe_price_id` PR C fills
--   in, no `account_id`, no RLS (mirrors `plans`/`seat_tiers` — see 0165's
--   header for the fuller "why no RLS" argument, identical here). Kept as its
--   own migration rather than folded into 0165 because packs and seat tiers
--   are conceptually unrelated catalogs (one prices SEATS under a
--   subscription, the other prices ONE-OFF top-ups) that happen to share a
--   shape — same granularity discipline as keeping 0161/0162 as separate
--   migrations despite both being Stripe-linkage tables.
--
--   ── `credits`/`price_usd_cents` are the WHOLE-PACK totals, not per-credit
--   ── or per-seat ──
--   Unlike `seat_tiers` (always per-seat) these are per-PACK: buying the
--   2,000-credit pack costs `price_usd_cents = 4400` once, not per credit.
--   The implied per-credit rate ($0.022 for all three) is a property of the
--   registered numbers, not a column this table stores — computing it from
--   `price_usd_cents / credits` at read time avoids a second number that
--   could drift from the two that actually define it.
--
--   ── Never-expire is a LEDGER property, not a column here ──
--   The catalog does not need a `credits_expire` flag: every pack this table
--   will ever sell is never-expire by the plan's own registration (§1.10),
--   and how a purchased pack's credits are written to `credit_ledger` (an
--   entry with no expiry, simply added to the account's standing balance) is
--   PR D's concern, not a fact about the price list. Adding a column that is
--   `false` on every row today and has no reader would be dead schema —
--   exactly the kind of speculative column this table's sibling migrations
--   (0165's header, "Seats are credits-only, no feature gating") already
--   argue against.
--
--   ── Why `code` values are `pack_500`/`pack_2000`/`pack_5000`, not the
--   ── credit amount alone ──
--   `plans`/`seat_tiers` both key on a short human-legible slug (`pro`,
--   `standard`); packs follow the identical convention rather than inventing
--   a bare-integer key, keeping every catalog table's `code` column the same
--   shape for any future code that joins across them generically.
--
-- CONTROL-PLANE ONLY, no tenant twin — same reasoning as `seat_tiers` (0165)
--   and `plans` before it: reference data has nothing tenant-specific to
--   isolate. Added to `scripts/check_schema_parity.sh`'s `CONTROL_TABLES`
--   roster in this same PR, grouped with 0165's `seat_tiers` and 0168's
--   `grant_seat_tiers`.
--
-- Depends on: 0165_seat_tiers.sql (the catalog-table shape and RLS posture
--   this migration mirrors exactly), 0155_billing_plans_pricing.sql (the
--   NO-FREE-TIER-PACKS/PLAN-FIRST gates this catalog exists to eventually
--   serve, both still unenforced — see that migration's header and this
--   one's own note above).
-- Down:
--   DELETE FROM credit_packs WHERE code IN ('pack_500', 'pack_2000', 'pack_5000');
--   DROP TABLE IF EXISTS credit_packs;

SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS credit_packs (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    code              TEXT        NOT NULL UNIQUE CHECK (code <> ''),
    name              TEXT        NOT NULL,
    -- Whole-pack totals — see header. NOT per-credit, NOT per-seat.
    credits           NUMERIC     NOT NULL CHECK (credits > 0),
    price_usd_cents   INT         NOT NULL CHECK (price_usd_cents > 0),
    -- NULL until PR C's provisioning script fills it in (same convention as
    -- `seat_tiers.stripe_price_id` / `plans.stripe_price_id`).
    stripe_price_id   TEXT        UNIQUE CHECK (stripe_price_id IS NULL OR stripe_price_id <> ''),
    is_active         BOOL        NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Seed: the three registered top-up packs (plan §1.10) ────────────────────
INSERT INTO credit_packs (code, name, credits, price_usd_cents)
VALUES
    ('pack_500',  '500 Credits',   500,  1100),
    ('pack_2000', '2,000 Credits', 2000, 4400),
    ('pack_5000', '5,000 Credits', 5000, 11000)
ON CONFLICT (code) DO NOTHING;
