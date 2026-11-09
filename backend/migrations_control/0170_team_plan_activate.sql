-- Migration: 0170_team_plan_activate
-- Description: billing self-serve launch PR E (renewal rewrite) — flips the
--   `team` umbrella plan (0167) from `is_active = false` to `true`, now that
--   the renewal/entitlement code paths that read `grant_seat_tiers` instead
--   of `plans.credits` for a `team` grant have actually landed (this same
--   PR, `billing::renewal::roll_period`/`billing::entitlement::
--   PLAN_ENTITLEMENT_SQL`).
--
--   0167's header named this exact moment: "PR E is what makes `team`
--   grants price correctly (reading `grant_seat_tiers` instead of
--   `plans.credits`) — flipping `is_active = true` for `team` belongs in
--   that PR, once granting it is actually safe, not here." This migration is
--   that flip, and nothing else — nothing about `team`'s shape (`credits =
--   0`, `price_usd_cents = NULL`, `stripe_price_id` permanently NULL)
--   changes; see 0167's header for why each of those stays exactly as it
--   is even once the plan is active. `team.credits = 0` remains a
--   deliberate "read from `grant_seat_tiers` instead" placeholder — it was
--   never a real total to begin with, and PR E's whole point is that no
--   code path reads it as one any more for a `team` grant specifically.
--
--   ── Why this is safe NOW, when it was not safe at 0167 ──
--   0167's danger scenario was concrete: an active `team` row with no PR-E
--   code in place would let `bin/grant_plan --plan team` (or any future
--   caller of `billing::issuance::issue_grant`/`resolve_active_plan`) issue
--   a grant priced straight off `plans.credits = 0`, and `renewal::
--   CURRENT_GRANT_SQL`'s "most recently granted, non-revoked" selection
--   would then let that zero-credit grant win over a real one — a sticky,
--   org-wide lockout (0167's own header, "Why team.is_active is seeded
--   false"). This PR closes that specific gap: `renewal::roll_period`,
--   `renewal::seed_member_period`, and `billing::entitlement::
--   PLAN_ENTITLEMENT_SQL` all now detect `plans.code = 'team'` and compute
--   from `grant_seat_tiers`/`seat_tiers`/`account_members.seat_tier_id`
--   instead of `plans.credits`/`plans.price_usd_cents` — see this PR's own
--   module docs in `backend/src/billing/renewal.rs` and
--   `backend/src/billing/entitlement.rs` for the exact formulas. A `team`
--   grant issued from this point forward prices correctly.
--
--   `bin/grant_plan.rs` is deliberately NOT taught to issue a `team` grant
--   in this PR — see that binary's own updated doc comment (this same PR)
--   for why: it can express only ONE seat count against ONE plan, which can
--   never state a MIXED tier composition (`grant_seat_tiers` needs
--   per-tier counts, not a single `--seats <n>`), and no route in this tree
--   creates a `team` grant yet either (checkout/seat-lifecycle are PR D/F,
--   not landed). Flipping `is_active = true` here therefore does not, by
--   itself, make it possible for ANY caller in this tree to actually issue
--   a `team` grant today — `grant_plan` now explicitly refuses `--plan
--   team` with a pointer to this reasoning rather than silently producing a
--   single-tier grant with no `grant_seat_tiers` rows at all (which would
--   entitle the account to `Σ() = 0` credits, the exact zero-credit trap
--   0167 was written to prevent, just reached by a different door). Once PR
--   F's seat-lifecycle routes exist, they will write `grants` +
--   `grant_seat_tiers` directly, bypassing `grant_plan`/`issue_grant`'s
--   single-seats-column shape entirely (the same "this call path was never
--   the only writer once self-serve exists" reasoning `issuance.rs`'s own
--   docs already anticipate for Stripe-driven grants).
--
-- Depends on: 0167_plans_team_remodel.sql (the `team` row this migration
--   flips), this PR's own renewal/entitlement code changes (not a migration
--   dependency in the SQL sense, but the reason this flip is safe now and
--   was not at 0167 — see header above).
-- Down:
--   UPDATE plans SET is_active = false WHERE code = 'team';

SET LOCAL lock_timeout = '3s';

UPDATE plans
   SET is_active = true
 WHERE code = 'team'
   AND is_active = false;
