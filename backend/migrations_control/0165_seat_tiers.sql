-- Migration: 0165_seat_tiers
-- Description: billing self-serve launch PR B (schema remodel) — the first of
--   five migrations (0165-0169) that replace 0155's two-tier team ladder
--   (`team_standard`/`team_premium`, PER-SEAT price+credits baked directly
--   into `plans`) with a proper `seat_tiers` catalog, so ONE `team` plan can
--   sell a MIX of tiers to the same account (a founder-decided requirement —
--   `.scratch/billing-self-serve/plan.md` §1.4 — that a single per-seat
--   `plans.credits`/`price_usd_cents` pair can never express: it can hold
--   exactly one price and one credit amount, never two). See that plan's §1
--   items 1.2-1.5, 1.7, 1.10 for the full registered decision set this train
--   of migrations implements; this file and 0166-0169 do not re-litigate any
--   of it.
--
--   This migration is deliberately narrow: it creates the catalog table and
--   seeds its two rows. It does NOT touch `plans`, `grants`, or
--   `account_members` — those are 0167/0168/0169's jobs respectively, kept
--   separate so each migration is reviewable as one self-contained unit, the
--   same granularity 0155/0156/0161/0162/0163/0164 already established in
--   this tree.
--
--   ── Why a catalog table rather than widening `plans` further ──
--   `plans` (0150) already conflates two different kinds of row: FLAT plans
--   (`pro`, `free`, `founder_comp`, one price/credit amount per account) and,
--   until this train, SEAT-PRICED plans that also carried one price/credit
--   amount — just interpreted per-seat rather than per-account (0155/0156's
--   own headers). That worked as long as a seat-priced plan needed only ONE
--   price point. It stops working the moment an account can buy DIFFERENT
--   seats at DIFFERENT price points under one umbrella plan (`team` — see
--   0167), because `plans` has exactly one `credits`/`price_usd_cents` pair
--   per row. `seat_tiers` is the normalized fix: a separate table, one row
--   per PRICE POINT, that a `team` grant can reference many of at once (via
--   0168's `grant_seat_tiers`) — the same "pull the varying part out into its
--   own table" move `credit_ledger`/`credit_balances` already made for
--   ledger-vs-cache, just applied to price points instead of balances.
--
--   ── Why NOT reuse `plans` rows for `standard`/`premium` themselves ──
--   A `seat_tiers` row is not a plan: nothing can be granted directly against
--   it (there is no `grants.seat_tier_id` — a grant references the `team`
--   PLAN, same as always, and `grant_seat_tiers` records which tiers/how many
--   seats within that one grant), and it carries no `refresh_kind`/
--   `period_length` of its own (a seat tier's renewal cadence is the
--   account's ONE subscription cadence, inherited from `team`, not a
--   per-tier cadence — nothing in the founder's model lets one account run
--   two different billing periods for two different seat tiers
--   simultaneously). Modeling it as a `plans` row would either fabricate
--   those columns with meaningless values or force `plans`' shape to bend
--   around a concept (per-tier catalog pricing under one subscription) it was
--   never designed to hold.
--
--   ── `credits`/`price_usd_cents` here are PER SEAT, always ──
--   Unlike `plans.credits`/`price_usd_cents` (whose meaning is gated by
--   `is_seat_priced` — see 0155's header), every `seat_tiers` row exists
--   SOLELY to price one seat, so there is no flat/per-seat ambiguity to gate:
--   `standard` is 3,000 credits/seat/mo at $40.00/seat/mo, `premium` is
--   15,000 credits/seat/mo at $175.00/seat/mo — registered figures (plan §1.3),
--   not derived. Both undercut the $0.022/credit top-up rate (plan §1.10;
--   registered packs 500/$11, 2,000/$44, 5,000/$110), the same
--   "subscription is cheaper per credit than a top-up" property 0155's
--   margin note already established for the plans this table now supersedes.
--
--   ── `stripe_price_id`, nullable ──
--   PR C's provisioning script (plan §1.13/§2.C) creates the actual Stripe
--   Price objects and writes their ids back here — this migration cannot
--   populate it (no Stripe calls happen in a schema-only PR) and must not
--   invent a placeholder value that would look like a real price id. NULL
--   here means "not yet wired to Stripe", the identical meaning 0155 gave
--   `plans.price_usd_cents = NULL` for a non-purchasable plan, applied to a
--   different column. UNIQUE (nullable-safe: Postgres UNIQUE permits any
--   number of NULLs) for the same reason `stripe_subscriptions.
--   stripe_customer_id` is UNIQUE (0162) — once populated, two tiers must
--   never be able to claim the same Stripe Price.
--
--   ── Seats are CREDITS-ONLY, no feature gating (plan §1.3) ──
--   Restated here because it is the reason this table carries no
--   feature-flag columns of any kind: `standard` vs `premium` differ ONLY in
--   credits/price, nothing else about the product changes with tier. A
--   future feature-gating requirement would need a real, reviewed design —
--   not a column bolted on here speculatively.
--
--   ── No seat minimum (plan §1.5, supersedes 0155's `team_standard`
--   ── 3-seat minimum) ──
--   `plans.min_seats` (0155) is therefore NOT mirrored onto this table — the
--   floor it enforced never carries forward. Any seat count from 1 is valid
--   for either tier; 0168's `grant_seat_tiers.seats` gets a bare `>= 1` CHECK,
--   nothing tighter.
--
--   ── No RLS: global reference data, same posture as `plans` ──
--   0150's header states `plans` "gets NO RLS policy" because it has no
--   `account_id` at all — it is catalog data every account reads the same
--   copy of, not a per-account-scoped row. `seat_tiers` is the identical
--   shape for the identical reason: a price list, not account data. Mirrors
--   `plans` exactly rather than inventing a new posture for what is
--   structurally the same kind of table.
--
-- CONTROL-PLANE ONLY, no tenant twin: catalog/pricing data belongs on the
--   control plane by construction (same as `plans`/`model_rates`/
--   `credit_conversion`) — there is nothing tenant-specific about a price
--   list. Added to `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster
--   in this same PR (grouped with 0166's `credit_packs` and 0168's
--   `grant_seat_tiers` in that script's own comment).
--
-- Depends on: 0150_credit_ledger.sql (the `plans`/RLS conventions this table
--   mirrors), 0155_billing_plans_pricing.sql (`is_seat_priced`/
--   `price_usd_cents` semantics this table's own pricing columns are modeled
--   on, and the `team_standard`/`team_premium` rows 0167 deactivates in favor
--   of these two seed rows).
-- Down:
--   DELETE FROM seat_tiers WHERE code IN ('standard', 'premium');
--   DROP TABLE IF EXISTS seat_tiers;

SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS seat_tiers (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    code              TEXT        NOT NULL UNIQUE CHECK (code <> ''),
    name              TEXT        NOT NULL,
    -- Per-seat, per-month. Always per-seat — see header, no flat/per-seat
    -- ambiguity to gate here the way `plans.credits` needs `is_seat_priced`.
    credits           NUMERIC     NOT NULL CHECK (credits >= 0),
    price_usd_cents   INT         NOT NULL CHECK (price_usd_cents >= 0),
    -- NULL until PR C's provisioning script fills it in. See header.
    stripe_price_id   TEXT        UNIQUE CHECK (stripe_price_id IS NULL OR stripe_price_id <> ''),
    is_active         BOOL        NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Seed: the two registered seat tiers (plan §1.3) ─────────────────────────
INSERT INTO seat_tiers (code, name, credits, price_usd_cents)
VALUES
    ('standard', 'Standard', 3000,  4000),
    ('premium',  'Premium',  15000, 17500)
ON CONFLICT (code) DO NOTHING;
