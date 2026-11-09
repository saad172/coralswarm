-- Migration: 0167_plans_team_remodel
-- Description: billing self-serve launch PR B (3/5) — the ladder remodel
--   itself (plan §1.3: "supersedes 0155's two team plans"). Three things,
--   all on `plans`:
--
--     1. `stripe_price_id` (nullable) for FLAT plans (`pro`) — the linkage PR
--        C's provisioning script fills in, same convention as 0165/0166's
--        catalog tables.
--     2. `team_standard`/`team_premium` (0155) get `is_active = false`. KEPT,
--        NOT DELETED — every existing `grants` row referencing either code
--        stays valid (`plan_id` FK, `grants_plan_seat_pricing_fk` composite
--        FK) and readable; `is_active = false` only stops NEW grants
--        (`billing::issuance::resolve_active_plan` filters on it) and self-
--        serve purchase flows (not yet built) from offering either code
--        again. Exactly the deactivation precedent this schema already has
--        for `is_active` (0150's own column, `plans.is_active`) — no new
--        mechanism invented.
--     3. One `team` plan row, the umbrella every seat-tier grant (0168) will
--        reference going forward. `is_seat_priced = true` (a `team` grant
--        DOES carry seats — see 0156/0168 — the flag's meaning as "this
--        plan's grants participate in the seats invariant" is unchanged),
--        `min_seats = NULL` (plan §1.5: no seat minimum, supersedes
--        `team_standard`'s registered 3-seat floor).
--
--   ── Why `team.stripe_price_id` is NOT set (stays NULL, and always will
--   ── be) ──
--   `stripe_price_id` on `plans` is documented above as being for FLAT
--   plans. `team` is not flat — it has no single price, only the per-tier
--   prices `seat_tiers.stripe_price_id` (0165) carries one per row. Setting
--   ANY value here — even once PR C runs — would be meaningless (which
--   Price? standard's? premium's?) and an operator reading it later would
--   have no way to know it was never wired on purpose versus simply
--   forgotten. Left NULL permanently, not "NULL for now".
--
--   ── Why `team.credits` is `0` rather than one of the old per-seat figures
--   ── ──
--   `plans.credits` is `NOT NULL` (0150), so some value is required, but for
--   a MIXED-seat grant (plan §1.4: an account can hold e.g. 3 `standard` +
--   2 `premium` seats under one `team` grant) there is no single per-seat
--   number this column could hold that would be correct — the true
--   entitlement is `Σ(grant_seat_tiers.seats × seat_tiers.credits)` (0168),
--   computed by joining THROUGH `grant_seat_tiers`, never by reading
--   `plans.credits` directly. Seeding this column with either tier's real
--   figure (3,000 or 15,000) would look like a meaningful number to a future
--   reader and risks being multiplied by `COALESCE(grants.seats, 1)` under
--   0156's pre-existing entitlement formula, silently reintroducing a wrong
--   total for the account's SECOND tier the moment someone reaches for the
--   familiar column instead of the new join. `0` is the loud, unmistakably
--   "read from `grant_seat_tiers` instead" placeholder — a real total from
--   this column would be `0`-per-seat only by an actual coincidence no
--   `team` account will ever hit. `PLAN_ENTITLEMENT_SQL` (`billing::
--   entitlement`) and `renewal::roll_period`'s per-tier computation are PR
--   E's job to update to read `grant_seat_tiers` for `is_seat_priced` grants
--   whose plan is `team` specifically — RESTATED HERE, NOT FIXED HERE: this
--   is schema-only (PR B scope), so nothing in this migration changes either
--   query, and until PR E lands, `plans.credits = 0` is exactly what the
--   PRE-EXISTING (0156) entitlement formula would compute for any `team`
--   grant that somehow got issued before PR E ships — a safe, visibly-wrong
--   zero rather than a plausible-looking but incorrect nonzero figure. No
--   route or worker in this tree can issue a `team` grant yet (PR D/F have
--   not landed), so this gap is inert today, not a live bug.
--
--   ── Why `team.price_usd_cents` is `NULL` ──
--   Same reasoning as `stripe_price_id` above: 0155 defined this column as
--   holding a single flat-or-per-seat price, and `team` has neither — its
--   real prices live on `seat_tiers.price_usd_cents`, one row per tier.
--   `NULL` already means "not applicable to self-serve at this column"
--   (0155's own header, for `free`/`founder_comp`) — reused here for a
--   distinct-but-analogous reason (not "free", but "priced elsewhere").
--
--   ── Why `team.is_active` is seeded `false` (review-round fix — this row
--   ── was originally seeded `true`; that was wrong) ──
--   `plans` has no per-member pricing shape today — `bin/grant_plan.rs::
--   resolve_plan` accepts ANY row with `is_active = true` and issues a grant
--   straight off `plans.credits`, which this migration's own header
--   documents as `0` for `team` (see above: the real total only exists via
--   `grant_seat_tiers`, a join no code in this schema-only PR performs yet).
--   An active `team` row is therefore a live footgun the moment it exists:
--   `--plan team` would issue a 0-credit grant, `renewal`'s `CURRENT_GRANT_SQL`
--   (newest unrevoked grant wins) would pick it as the account's active grant
--   over any real `team_standard`/`team_premium` grant, and `issue_grant`'s
--   own TOCTOU re-check treats an existing active grant as `AlreadyGranted` —
--   making the org-wide lockout sticky, not just a one-time mistake. `false`
--   here is the exact same "not wired yet" convention this migration already
--   uses for `stripe_price_id`/`price_usd_cents` (both NULL), applied to the
--   one remaining column (`is_active`) that can actually cause harm if left
--   in the wrong state, rather than merely being unpopulated. PR E is what
--   makes `team` grants price correctly (reading `grant_seat_tiers` instead
--   of `plans.credits`) — flipping `is_active = true` for `team` belongs in
--   that PR, once granting it is actually safe, not here.
--
-- Depends on: 0155_billing_plans_pricing.sql (`team_standard`/`team_premium`,
--   `is_seat_priced`, the `price_usd_cents`/`min_seats` this migration
--   partially supersedes), 0165_seat_tiers.sql (`seat_tiers`, the table that
--   now actually prices a `team` seat), 0156_grants_seats.sql (`plans`'s
--   `UNIQUE (id, is_seat_priced)` — unaffected by this migration, `team`
--   satisfies it with `is_seat_priced = true` same as `team_standard`/
--   `team_premium` did).
-- Down:
--   DELETE FROM plans WHERE code = 'team';
--   UPDATE plans SET is_active = true WHERE code IN ('team_standard', 'team_premium');
--   ALTER TABLE plans DROP COLUMN IF EXISTS stripe_price_id;
--   ── Why the reactivation above is unconditional, not "restore prior
--   ── state" ──
--   `team_standard`/`team_premium` have exactly one writer of `is_active`
--   before this migration: `0155`'s seed, which sets both `true` and is
--   never touched again anywhere else in this tree — verified by grepping
--   every migration between `0155` and this one for either code. "Restore to
--   true" and "restore to whatever it was" are therefore the same operation
--   today. This Down block is operator-run runbook text in this tree (no
--   migration here ships a separate executable `.down.sql`), so the caller
--   is expected to confirm current state before running it on any DB where
--   that invariant might no longer hold (e.g. a future migration that
--   deactivates one of these two codes for an unrelated reason) rather than
--   have this comment silently encode a conditional it cannot enforce.

SET LOCAL lock_timeout = '3s';

ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS stripe_price_id TEXT
        UNIQUE CHECK (stripe_price_id IS NULL OR stripe_price_id <> '');

-- ── Deactivate, do not delete (plan §1.3/§1.5) ───────────────────────────────
UPDATE plans
   SET is_active = false
 WHERE code IN ('team_standard', 'team_premium')
   AND is_active = true;

-- ── The new umbrella plan ────────────────────────────────────────────────────
-- `is_active = false` — see header "Why team.is_active is seeded false".
-- PR E flips this to true once `team` grants price correctly.
INSERT INTO plans
    (code, name, credits, price_usd_cents, is_seat_priced, min_seats, refresh_kind, period_length, is_active)
VALUES
    ('team', 'Team', 0, NULL, true, NULL, 'anniversary', INTERVAL '1 month', false)
ON CONFLICT (code) DO NOTHING;
