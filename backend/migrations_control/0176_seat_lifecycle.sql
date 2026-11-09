-- Migration: 0176_seat_lifecycle
-- Description: billing self-serve launch PR F (seat lifecycle) — the schema
--   half of ".scratch/billing-self-serve/pr-f-notes.md": (a) a new
--   `account_members.seat_purchased_at TIMESTAMPTZ` column, the marker that
--   makes `acquire_seat`/`release_seat` (`billing::seat_lifecycle`,
--   application code, not this migration) idempotent and makes
--   roster-vs-purchased drift queryable (NULL = this seat is not yet backed
--   by a Stripe purchase — every personal-account row and every org member
--   who joined before their org ever subscribed); and (b) the SUNSET this
--   PR's own predecessor scheduled on itself — `0169`'s
--   `account_members_default_seat_tier()` trigger function, finding 6 there:
--   "PR F (seat lifecycle) must make every call site that adds a team-org
--   member pass `seat_tier_id` explicitly... once that lands, the `IF ...
--   seat_tier_id IS NULL` branch below should become `RAISE EXCEPTION`".
--   This migration is that flip. The four Rust call sites 0169 could not
--   touch (`clerk_provision::ensure_personal_account`,
--   `clerk_provision::ensure_org_membership`,
--   `clerk_provision::refresh_org_membership_from_claims`,
--   `Store::insert_account_member`) are updated in the SAME PR (not this
--   migration) to pass `seat_tier_id` explicitly on every insert, so the
--   RAISE EXCEPTION arm below should never actually fire in steady state —
--   see that trigger function's own doc comment (rewritten here) for what it
--   means if it does.
--
--   ── Why `seat_purchased_at` lives on `account_members`, not a satellite
--   ── table ──
--   It is a per-member, per-account fact with no independent lifecycle of its
--   own (it is cleared implicitly the moment the member row itself is
--   deleted by `remove_org_membership` — there is nothing to cascade), the
--   same shape `account_members.offboarded_at`-adjacent bookkeeping already
--   takes on this table via other columns. A satellite table keyed on
--   `(account_id, user_id)` would only duplicate this table's own primary
--   key for a single nullable timestamp — the "pull the varying part into
--   its own table" argument `0165`'s header makes for `seat_tiers` does not
--   apply here: there is nothing MULTI-VALUED about "when was this member's
--   seat last (re)purchased", it is exactly one fact per member row.
--
--   Deliberately NOT reset to NULL by a tier CHANGE — a seat-tier
--   upgrade/downgrade (the tier-change route, `routes::billing_seats`, this
--   same PR) moves an ALREADY-purchased seat between tiers; it does not
--   un-purchase and re-purchase it, so `seat_purchased_at` is untouched by
--   that path and only ever cleared by the member row's own deletion.
--
--   ── Why NOT NULL/CHECK against `account_is_org_account` (deliberately
--   ── absent) ──
--   Unlike `seat_tier_id` (0169's `account_members_seat_tier_iff_org`,
--   IFF-shaped because EVERY org member must carry a tier), a personal
--   account member is ALWAYS `seat_purchased_at IS NULL` (personal accounts
--   are never seat-priced — 0169's own header) but an ORG member is not
--   always purchased either: plan §1.4's mixed-seat model, restated by this
--   PR's design notes, deliberately leaves pre-existing org members
--   unpurchased until Stripe actually bills for their seat (FOUNDER-CONFIRM,
--   `pr-f-notes.md`: "members who existed BEFORE the org subscribed are
--   never auto-purchased"). So the real invariant is one-directional
--   ("personal implies NULL"), not IFF, and a one-directional invariant
--   referencing another table's column is exactly the shape `0156`'s header
--   says a plain CHECK cannot express without denormalizing a mirror column
--   for it (the `account_is_org_account` treatment). Denormalizing a THIRD
--   mirror purely to gate a nullable bookkeeping timestamp — one Stripe
--   webhook/route writer can simply never set it wrong, since neither new
--   writer this PR adds (`acquire_seat`, the checkout-completed ledger seed)
--   ever touches a personal account's row in the first place — is not worth
--   the schema weight `is_org_account`/`account_is_org_account` already pay
--   for a REQUIRED (IFF) invariant; this one is enforced by construction at
--   the two call sites instead, the same trust boundary this schema already
--   extends to `credits_override`/`seats` pairing on `grants` before `0156`
--   existed at all.
--
-- CONTROL-PLANE ONLY — `account_members` is already registered in
--   `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster (0169's
--   header) and carries no RLS policy of its own; adding a column changes
--   neither fact, so neither roster needs a new entry.
--
--   ── Deploy note (rolling deploy window) ──
--   Between this migration applying and the new backend build (which passes
--   `seat_tier_id` explicitly everywhere) actually rolling out, an
--   old-binary org-member insert that relied on the silent default would
--   fail loudly instead — a brief window where `organizationMembership.
--   created`/`.updated` webhooks for a NEW org member on OLD code 5xx and
--   Clerk redelivers, or a first-sight auth request for a new member on OLD
--   code fails once and succeeds on the next request once the new binary is
--   live. Both are self-healing (Clerk's at-least-once redelivery; the next
--   request simply retries), and no data is written wrong in either case —
--   the alternative (leaving the silent default live indefinitely) is a
--   billing correctness bug forever, which is strictly worse than a
--   redelivered webhook for the width of one rolling deploy.
--
-- Depends on: 0169_account_members_seat_tier.sql (`account_members.
--   seat_tier_id`, `account_members_default_seat_tier()`, the trigger this
--   migration replaces the body of).
-- Down:
--   CREATE OR REPLACE FUNCTION account_members_default_seat_tier()
--   RETURNS TRIGGER LANGUAGE plpgsql AS $$
--   BEGIN
--       IF NEW.account_is_org_account IS NULL THEN
--           SELECT is_org_account INTO NEW.account_is_org_account
--             FROM accounts WHERE id = NEW.account_id;
--       END IF;
--       IF NEW.account_is_org_account AND NEW.seat_tier_id IS NULL THEN
--           SELECT id INTO NEW.seat_tier_id
--             FROM seat_tiers WHERE code = 'standard' AND is_active = true;
--           RAISE WARNING 'account_members_default_seat_tier: defaulting account_id=%, user_id=% to the standard seat tier',
--               NEW.account_id, NEW.user_id;
--       END IF;
--       RETURN NEW;
--   END;
--   $$;
--
--   -- Billing self-serve PR F review, MAJOR finding: `seat_purchased_at`
--   -- carries LIVE purchase-idempotency state the moment `acquire_seat`/
--   -- `release_seat` (`billing::seat_lifecycle`, application code) run
--   -- against production — a bare `DROP COLUMN` here discards every
--   -- member's purchase marker with no way to restore it on a later
--   -- forward re-migration, and `acquire_seat` would then treat
--   -- already-purchased seats as unpurchased and attempt a DUPLICATE
--   -- Stripe push. This migration is NOT safely revertible via a blind
--   -- DROP once any row has a non-NULL `seat_purchased_at` — snapshot the
--   -- column FIRST:
--   --   CREATE TABLE account_members_seat_purchased_at_backup AS
--   --     SELECT account_id, user_id, seat_purchased_at
--   --       FROM account_members WHERE seat_purchased_at IS NOT NULL;
--   -- and, if this migration is later re-applied going FORWARD, restore it
--   -- with:
--   --   UPDATE account_members am SET seat_purchased_at = b.seat_purchased_at
--   --     FROM account_members_seat_purchased_at_backup b
--   --    WHERE am.account_id = b.account_id AND am.user_id = b.user_id;
--   -- before dropping the backup table. Do NOT run the DROP COLUMN below
--   -- against a database with any purchased seats without taking this
--   -- snapshot first.
--   ALTER TABLE account_members DROP COLUMN IF EXISTS seat_purchased_at;

SET LOCAL lock_timeout = '3s';

-- ── 1. `seat_purchased_at` ───────────────────────────────────────────────
-- Plain, metadata-only ADD COLUMN (PG11+, no DEFAULT to backfill) — same
-- "no table rewrite" posture 0164/0169 already established for this table.
ALTER TABLE account_members
    ADD COLUMN IF NOT EXISTS seat_purchased_at TIMESTAMPTZ;

-- ── 2. The 0169 sunset: default-to-standard becomes RAISE EXCEPTION ────────
-- The `account_is_org_account` fill arm (mirrors `accounts.is_org_account`
-- onto every inserted row so `account_members_account_org_fk`/
-- `account_members_seat_tier_iff_org` have something to check against) is an
-- FK-mirror filler, not a billing default, and is UNCHANGED — see this
-- migration's own header and 0169's header ("Why NOT extend this same trick
-- to enforce...") for why that arm was never the thing PR F's sunset note
-- targeted.
--
-- The `seat_tier_id` default arm is what changes: a caller that leaves it
-- NULL for an org member no longer gets silently defaulted to `standard`
-- (0169's `RAISE WARNING` branch) — it is now rejected outright, matching
-- `0156`'s "every caller passes `plan_is_seat_priced` explicitly, no default
-- at all" precedent for the isomorphic problem on `grants`. Every existing
-- Rust writer of `account_members` is updated in this same PR (not this
-- migration) to pass `seat_tier_id` explicitly on every org-member insert, so
-- this branch should never fire in steady state; if it does, it means a
-- caller regressed to the old silent-default assumption, and that must be a
-- loud 500 (surfaced as a Clerk webhook 5xx / redelivery, or a request-path
-- error), not a quietly-wrong `standard` seat assignment.
CREATE OR REPLACE FUNCTION account_members_default_seat_tier()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_is_org_account BOOLEAN;
BEGIN
    IF NEW.account_is_org_account IS NULL THEN
        SELECT is_org_account INTO v_is_org_account
          FROM accounts WHERE id = NEW.account_id;
        -- Unchanged from 0169: left as-is, no COALESCE — either the
        -- NOT NULL constraint on `account_is_org_account` or the
        -- pre-existing `account_id` FK rejects a row referencing no such
        -- account, whichever fires first.
        NEW.account_is_org_account := v_is_org_account;
    END IF;

    IF NEW.account_is_org_account AND NEW.seat_tier_id IS NULL THEN
        -- Billing self-serve PR F review, trivial finding: `USING ERRCODE =
        -- 'check_violation'` (23514) — the same convention `0082`'s own
        -- `account_members_block_revoked` trigger already established for a
        -- trigger-enforced integrity rule on this table — so a caller can
        -- assert the stable SQLSTATE instead of coupling to this message's
        -- exact wording (`backend/tests/billing_schema_remodel_pg.rs`'s own
        -- `a_bare_insert_for_a_team_org_account_is_rejected_without_an_explicit_tier`).
        RAISE EXCEPTION 'account_members_default_seat_tier: org member insert for account_id=%, user_id=% supplied no seat_tier_id — PR F (seat lifecycle) requires every writer of account_members to pass one explicitly (the active ''standard'' tier for a plain org join, or the caller''s chosen tier for a real tier picker). This is no longer defaulted silently; fix the calling code.',
            NEW.account_id, NEW.user_id
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END;
$$;

-- No DROP/CREATE TRIGGER needed — CREATE OR REPLACE FUNCTION above changes
-- the existing `account_members_default_seat_tier_trg` trigger's behavior in
-- place; the trigger itself (0169) already points at this function by name.
