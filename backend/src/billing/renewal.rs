//! The period roll — the one thing that puts rows in
//! `member_allowance_periods`.
//!
//! # Why this module exists at all
//!
//! Migration `0161` built `member_allowance_periods` and
//! [`crate::billing::enforcement::is_blocked`] reads it, but until this module
//! ran there was **nothing that ever wrote a row**. Every `is_blocked` call
//! therefore fell through to the account-level `org_pool_exhausted` check and
//! per-seat limits were inert. That fallback is deliberate — it exists so
//! re-graining the gate could not make it weaker — and "no period rows exist"
//! was never a bug in Phase 2b. This module is what ends it.
//!
//! # Why the whole grant is ONE `INSERT ... SELECT`
//!
//! The alternative is to read the member roster, then loop and insert. That
//! shape has a race the single statement does not: a member added between the
//! read and the loop is silently skipped for the whole period, and a member
//! removed mid-loop makes the insert fail against a roster that no longer
//! matches. One statement takes its roster from the same snapshot it writes in,
//! so the set of members granted is exactly the set that existed at the instant
//! the period rolled.
//!
//! It is also what makes redelivery free. `0161`'s UNIQUE index on
//! `(account_id, user_id, period_start)` turns the whole thing into
//! `ON CONFLICT DO NOTHING`, so a redelivered `invoice.paid` inserts zero rows
//! rather than doubling anyone's allowance. Stripe's delivery contract is
//! at-least-once, so this is the normal path, not an edge case.
//!
//! # The per-member allowance formula, and why it is defined here ONCE
//!
//! [`crate::billing::entitlement`] carries the ACCOUNT-total formula, stated
//! once in SQL and called by both the handler and its tests, specifically
//! because an earlier version re-typed it into the test file and the tests then
//! proved nothing. This module is the per-MEMBER counterpart and follows the
//! same discipline: the formula lives in [`PER_MEMBER_GRANT_SQL`] alone, and
//! the tests that claim to prove it call [`roll_period`] rather than restating
//! it.
//!
//! The two formulas are deliberately consistent, and it is worth writing out
//! why, because "per-member × seats == the account total" is the invariant that
//! makes the split-bucket model add up:
//!
//! ```text
//! entitlement (total):  COALESCE(g.credits_override, p.credits * COALESCE(g.seats, 1))
//! this module (member): COALESCE(g.credits_override / COALESCE(g.seats, 1), p.credits)
//!
//! member × seats  =  COALESCE(override/seats, p.credits) × seats
//!                 =  COALESCE(override, p.credits × seats)     ✓ the total
//! ```
//!
//! `p.credits` is already the PER-SEAT figure for a seat-priced plan (migration
//! `0156`), which is why it is NOT divided. `credits_override` is an absolute
//! ACCOUNT total that wins over the multiplication entirely (`0156`'s header),
//! which is why it IS divided. A flat plan has `seats IS NULL`, and
//! `COALESCE(g.seats, 1)` collapses both branches onto one expression —
//! `grants_seats_iff_seat_priced` guarantees flat plans never carry a seat
//! count, so that branch is reached only where dividing by 1 is a no-op.
//!
//! One honest caveat: when an override does NOT divide evenly, NUMERIC division
//! leaves a sub-credit shortfall — measured, `1000 / 3` times three seats is
//! `999.9999999999999999`, i.e. `1e-16` unallocated. That is roughly 2e-18
//! dollars at the current credit rate, so it is recorded here rather than
//! engineered around. It is stated because a reconciliation that expects
//! per-member totals to sum EXACTLY to the override will see it, and finding it
//! in a ledger is much worse than reading it here.
//!
//! # `cap_credits` is deliberately left NULL
//!
//! `0161` resolves NULL as "the policy default", `COALESCE(cap_credits,
//! granted_credits)` — the founder's 1× rule. Writing the literal here would
//! freeze today's policy into every historical row and make a later change
//! invisible to old periods. A non-NULL value means an admin deliberately moved
//! ONE member's cap for ONE period, and the renewal must never manufacture
//! that signal.
//!
//! # Members and seats cannot legitimately disagree
//!
//! The founder's model is that **adding a member IS purchasing a seat** — the
//! two happen in the same act, so `COUNT(account_members)` and `grants.seats`
//! are the same number by construction. This module therefore does NOT arbitrate
//! between them: it grants every current member a full allowance, and reports
//! both counts in [`RenewalOutcome`] so a divergence is recorded and alerted on
//! rather than silently absorbed.
//!
//! That is a deliberate choice about which failure is worse. Granting only
//! `seats` members would silently starve whoever sorted last, with no surface
//! anywhere explaining why their allowance never arrived — the same
//! silent-wrong-number class as the seat bug fixed in #630, where a ten-seat
//! customer quietly received one seat's credits. A divergence here means the
//! member-add path failed to purchase a seat, which is a bug in THAT path; the
//! renewal's job is to make it loud, not to quietly bill around it.
//!
//! # Per-tier allowances for `team` grants (billing self-serve launch PR E)
//!
//! Everything above describes the SINGLE-TIER model: one `per_member_credits`
//! figure, computed from `plans.credits` (or `credits_override / seats`), the
//! same number written to every member's row. That model is exactly right for
//! a flat plan (`pro`, `free`) and for the retiring `team_standard`/
//! `team_premium` rows (0155/0167, `is_active = false` but still able to
//! renew an existing grant) — both are genuinely single-price-point plans.
//!
//! It is NOT right for the `team` umbrella plan (0167): a `team` account can
//! hold a MIX of `standard` and `premium` seats (plan §1.4), so there is no
//! single `per_member_credits` number correct for every member — a
//! `standard` member's allowance is `seat_tiers.credits` for `standard`
//! (3,000), a `premium` member's is `seat_tiers.credits` for `premium`
//! (15,000), read via that member's OWN `account_members.seat_tier_id`
//! (0169), not from `plans.credits` (which 0167's header documents is `0` for
//! `team` on purpose — a loud "read the join instead" placeholder, not a real
//! total).
//!
//! [`CURRENT_GRANT_SQL`] resolves a `use_seat_tier_credits` flag (`plans.code
//! = 'team' AND grants.credits_override IS NULL`) alongside the legacy
//! `per_member_credits` scalar, and [`PER_MEMBER_GRANT_SQL`] is a SINGLE
//! `INSERT ... SELECT` (the shape the module doc above already argues for)
//! extended with one `LEFT JOIN seat_tiers` and a `CASE` on that flag: when
//! it is true, each row's `granted_credits` comes from THAT ROW's own
//! `am.seat_tier_id` join; when false, every row gets the same legacy scalar,
//! byte-for-byte the query this module shipped with. A `team` grant that
//! DOES carry `credits_override` deliberately falls back to the legacy
//! divide-evenly-across-seats formula rather than the tier join — an
//! override is an admin's absolute total, and dividing it evenly across
//! seats (ignoring tier) is the same choice already made for
//! `team_standard`/`team_premium`, extended to `team` rather than
//! special-cased away from it.
//!
//! ## Pending downgrades, applied at roll (plan §1.7)
//!
//! [`roll_period`]'s FIRST statement — before it even counts members, let
//! alone computes an allowance — applies every pending seat-tier downgrade
//! for the account: `account_members.seat_tier_id = pending_seat_tier_id,
//! pending_seat_tier_id = NULL` for every row where the latter is not NULL.
//! This is what makes "downgrade takes effect at the NEXT period roll, never
//! before" (the founder's registered decision, §1.7) actually true: an
//! upgrade is applied immediately by the route that accepts it (PR F, not
//! this module's concern), but a downgrade is deliberately deferred, and
//! [`roll_period`] is the ONE place "the next period rolled" is decided. Only
//! `roll_period` applies pending downgrades — [`seed_member_period`]'s
//! mid-period join path does not, and must not: a member joining mid-period
//! is not a period roll, and applying someone ELSE's pending downgrade as a
//! side effect of an unrelated member's join would apply it early, exactly
//! the thing this ordering exists to prevent.
//!
//! Applying it unconditionally REGARDLESS OF WHETHER THE ACCOUNT HAS AN
//! ACTIVE GRANT (the early-return branch below), is deliberate rather than
//! wasted work: a downgrade recorded while a grant was active but consumed by
//! the time the next Stripe event lands must still land the moment SOME roll
//! eventually runs for this account, and gating the apply on "only if there's
//! a grant" would leave it stranded pending a grant state this function has
//! no other reason to re-check.
//!
//! **Gated on "is this the FIRST roll of THIS `period_start`?" (CodeRabbit,
//! PR #655 review).** A `customer.subscription.updated` redelivery — or any
//! other Stripe event that lands mid-period and happens to report the SAME
//! `period_start` `stripe_webhook` already rolled — re-enters [`roll_period`]
//! without a NEW period ever starting. `0161`'s UNIQUE index makes the
//! allowance-row INSERT further down an idempotent no-op for that case (see
//! the module doc above), but before this fix nothing stopped the downgrade
//! apply from running anyway: a downgrade scheduled AFTER the first roll
//! (mid-period, via the route that sets `pending_seat_tier_id`) would flip
//! `seat_tier_id` and clear `pending_seat_tier_id` on this SECOND event,
//! days before the period it was supposed to wait for. `roll_period` now
//! checks [`PERIOD_ALREADY_ROLLED_SQL`] — whether `member_allowance_periods`
//! already holds a row for this exact `(account_id, period_start)` — and
//! only applies pending downgrades when it does not. This check-then-apply
//! is race-safe because every production caller takes the per-account
//! [`lock_period_rotation`] advisory lock BEFORE calling `roll_period` (see
//! that function's own doc comment and the three call sites in
//! `billing::issuance` / `routes::stripe_webhook`): no concurrent
//! transaction can insert this period's first row between the SELECT and
//! this function's own INSERT, so "no rows yet" observed here cannot go
//! stale before this call finishes.
//!
//! The pending-downgrade TIER MOVE described just below (see the next
//! section) IS gated on `!already_rolled`, unlike the flip's sibling
//! statement it sits next to — see that section for why: unlike the old
//! roster reconciliation this replaces, the move is a DELTA (decrement the
//! old tier, increment the new one), not an idempotent from-scratch
//! recompute, so re-applying it on a repeat event would double-move the same
//! seats.
//!
//! ## `grant_seat_tiers` is a purchase-driven ledger (billing self-serve
//! ## launch PR F) — this module only MOVES it, never recomputes it
//!
//! **Superseded here (PR F, seat lifecycle): earlier revisions of this
//! module (PR E) recomputed `grant_seat_tiers`/`grants.seats` from the LIVE
//! ROSTER on every roll and every mid-period join, via a
//! `reconcile_team_grant_seat_tiers` helper (DELETE-then-INSERT-then-sync)
//! this module no longer has.** That was a deliberate stopgap PR E's own
//! header called out explicitly: "the seat-lifecycle routes that would
//! maintain [`grant_seat_tiers`] incrementally... are PR F, not landed" — so
//! until PR F shipped, `grant_seat_tiers` had no other writer and trusting
//! the roster was the only way to keep it from drifting out of step with
//! reality.
//!
//! PR F is that maintenance: `billing::seat_lifecycle::acquire_seat`
//! (wired from `clerk_provision::ensure_org_membership`), the tier-change
//! route (`routes::billing_seats`) and the admin buy/reduce actions
//! (`routes::org_slots`) maintain `grant_seat_tiers` and `grants.seats`
//! INCREMENTALLY, in step with actual Stripe purchases, not with
//! `account_members` membership. (CS-444: `release_seat` used to be on that
//! list and no longer is — a departing member's slot returns to the pool
//! instead of being un-purchased, so a REMOVAL writes nothing here at all.) `grant_seat_tiers` is therefore now a
//! **persistent purchase ledger** for the account's active `team` grant —
//! this module's job shrinks to ONE thing: moving counts between tiers when
//! a PENDING DOWNGRADE finally applies (a tier CHANGE of an already-purchased
//! seat, not a purchase or a release, so [`billing::seat_lifecycle`] is not
//! the right place for it — the move belongs exactly where the downgrade
//! itself is applied, this module's own `roll_period`). Neither
//! [`roll_period`] nor [`seed_member_period`] reads `account_members` to
//! decide `grant_seat_tiers`/`grants.seats` any more; both read `grants.seats`
//! and the per-row `seat_tiers` join purely to compute ALLOWANCES, exactly as
//! the rest of this module doc already describes.
//!
//! **The pending-downgrade move, step by step.** Immediately before
//! [`APPLY_PENDING_DOWNGRADES_SQL`] flips `account_members.seat_tier_id` (see
//! the "Pending downgrades, applied at roll" section above), [`roll_period`]
//! derives, from the SAME set of rows that flip is about to touch
//! (`pending_seat_tier_id IS NOT NULL`, grouped by `(seat_tier_id,
//! pending_seat_tier_id)`), how many seats move from each OLD tier to each
//! NEW tier — then decrements (or deletes, at zero — 0168's "a tier with
//! zero seats simply has no row here" convention, which a plain `UPDATE ...
//! SET seats = 0` cannot satisfy against `grant_seat_tiers_seats_check`) the
//! old tier's row and upserts (increments, or inserts fresh) the new tier's
//! row, for THIS grant only. `grants.seats` — the Σ across tiers — is then
//! resynced via [`RECONCILE_GRANTS_SEATS_SQL`] (unchanged SQL from PR E, but
//! now reached only from this move, never from a roster recompute). The move
//! and the flip touch DIFFERENT tables (`grant_seat_tiers`/`grants` vs
//! `account_members`) but must agree on which rows counted as "pending" —
//! both read that same predicate in the same transaction, before either
//! writes, so there is no window where they could see different rosters.
//!
//! **Gated on the identical `!already_rolled` check as the flip itself**,
//! and for the identical reason (see "Pending downgrades, applied at roll"
//! above): a redelivered event for an already-rolled period must not move
//! seats a SECOND time for a downgrade that was scheduled AFTER the first
//! roll — `a_repeat_roll_of_the_same_period_does_not_apply_a_downgrade_scheduled_after_it`
//! (this module's `pg_tests`) proves the ledger, not just the flip, stays
//! untouched on that repeat call. This is a real behavior change from PR E's
//! posture on its own (now-removed) roster reconciliation, which ran
//! UNCONDITIONALLY every call because a from-scratch recompute of the SAME
//! roster is a true no-op under redelivery. A tier MOVE is not a recompute —
//! it is `seats = seats - n` / `seats = seats + n`, and applying that twice
//! to the same redelivered event would silently double-move real purchased
//! seats. Only a genuinely NEW `period_start` may trigger the move, exactly
//! once, matching the flip it is paired with.
//!
//! **`grants.seats` for a `team` grant means the TOTAL across every tier**
//! (0168's header left this unstated on purpose, deferring to renewal
//! logic — this is that decision, carried over unchanged from PR E). Unlike
//! PR E's version, this module no longer skips syncing it on an "empty
//! roster" — there is no roster involved in the sync at all any more; the
//! sync runs exactly when a pending-downgrade move actually ran, from
//! whatever `grant_seat_tiers` now holds, which can never be zero for a row
//! that still exists (`grant_seat_tiers_seats_check`, 0168) and is simply
//! left untouched when the grant holds no `grant_seat_tiers` rows at all (a
//! `team` grant nobody has purchased a seat on yet — reachable the same way
//! a flat plan can be granted against zero members).
//!
//! ## The `team` seat_mismatch signal is un-suppressed (PR F)
//!
//! PR E's [`RenewalOutcome::seat_mismatch`] unconditionally suppressed itself
//! for every `team` roll, reasoning that `grants.seats` was DERIVED from the
//! same roster `members` counts every roll (via the now-removed roster
//! reconciliation), so an empty-roster account left `grants.seats` stale
//! (the `EXISTS` guard could not write `0`) purely as an artifact of that
//! recompute's own limits — a false alarm, not a real signal, and reporting
//! it would have buried the genuine one for every other plan shape.
//!
//! That reasoning no longer applies, because the thing it was protecting
//! against no longer exists: `grants.seats` for a `team` grant is not
//! derived from the roster at all any more — it is Σ of a purchase ledger
//! maintained by [`billing::seat_lifecycle`] and this module's own
//! pending-downgrade move. A `team` roll where `seats` (purchased) and
//! `members` (roster) disagree is now EXACTLY the drift this signal exists
//! to catch: a member without a purchased seat (the FOUNDER-CONFIRM case,
//! `.scratch/billing-self-serve/pr-f-notes.md` — "members who existed BEFORE
//! the org subscribed are never auto-purchased", so this is an EXPECTED,
//! non-alarming steady state for such accounts, not a bug — the signal is
//! informational, not an error), or a purchased seat with nobody on it (an
//! `acquire_seat` that ran but whose membership write somehow never landed —
//! genuinely worth a warn). [`RenewalOutcome::seat_mismatch`] therefore no
//! longer special-cases `is_team_plan` at all; see that method's own doc
//! comment below.

use sqlx::types::Decimal;
use sqlx::PgConnection;
use uuid::Uuid;

/// Resolves the account's current grant into the numbers the roll needs.
///
/// Same "most recently granted, non-revoked" selection as
/// `entitlement::PLAN_ENTITLEMENT_SQL` — deliberately, so the
/// allowance a member is granted and the entitlement the usage screen displays
/// can never be computed from two different grants.
///
/// `g.plan_id` is named alongside the two numbers `roll_period` needs so that
/// [`seed_member_period`]'s first-joiner branch can hand it straight to
/// [`crate::billing::issuance::resolve_initial_period`] rather than re-running
/// this same "most recently granted, non-revoked" lookup a second time under
/// a second, hand-duplicated query — see that function's own doc comment for
/// why the two must never drift (finding 9, review round 2). `roll_period`
/// itself has no use for the plan id and discards it.
///
/// `g.id` (PR E, billing self-serve launch): the grant id itself. **PR F:**
/// [`roll_period`] needs it to key [`move_pending_seat_tier_counts`]'s
/// pending-downgrade ledger move — see the module doc's "`grant_seat_tiers`
/// is a purchase-driven ledger" section. [`seed_member_period`] no longer
/// has any use for it (PR F removed its own reconciliation call; see that
/// function's own comment) and discards it, the same way `roll_period`
/// already discarded `plan_id`.
///
/// `is_team_plan`/`use_seat_tier_credits` (PR E): two flags derived from
/// `plans.code = 'team'`, kept separate because they gate two DIFFERENT
/// decisions. **PR F:** `is_team_plan` alone now gates whether
/// [`roll_period`] runs [`move_pending_seat_tier_counts`] at all (a
/// non-`team` grant can never carry `pending_seat_tier_id` rows in
/// practice — the tier-change route that sets them 409s outside a `team`
/// grant — but the gate is kept explicit rather than relying on that
/// invariant silently, the same "loud over quiet" posture this module uses
/// throughout). `use_seat_tier_credits` additionally requires
/// `credits_override IS NULL` and is what actually switches
/// [`PER_MEMBER_GRANT_SQL`]'s per-row formula and
/// [`seed_member_period`]'s single-member lookup onto the tier join — see the
/// module doc's "Per-tier allowances for `team` grants" section for why an
/// override deliberately keeps the legacy divide-evenly formula instead.
///
/// **PR E review round 2, finding 5:** this now returns the RAW components
/// (`credits_override`, `seats`, `plans.credits`) rather than a pre-divided
/// `per_member_credits` scalar. See [`legacy_per_member_credits`]'s own doc
/// comment for why the division must happen in Rust, AFTER
/// [`roll_period`] has had a chance to correct `seats` from a
/// pending-downgrade move (PR F; formerly a roster reconciliation, PR E) —
/// computing it inline in THIS query, before that correction runs, was the
/// ordering bug finding 5 fixed and remains the reason [`roll_period`]
/// re-reads `seats` from [`move_pending_seat_tier_counts`]'s return value
/// before dividing.
///
/// `$2` is bound to [`crate::billing::MIXED_TIER_PLAN_CODE`] — see that
/// constant's own doc comment (finding 8) for why the literal `'team'` used
/// to be spelled out independently at three sites.
///
/// `ORDER BY g.granted_at DESC, g.plan_is_seat_priced DESC, g.id DESC`: the
/// tie-break matters because `granted_at` uses tx-stable `now()`, so a flat
/// and a seat-priced grant issued in the same transaction can tie exactly,
/// leaving Postgres free to pick either as "current". `g.id DESC` breaks any
/// further tie deterministically — `id` is a random `gen_random_uuid()`, not
/// time-ordered, so it doesn't mean "newest", it just guarantees this query
/// and `enforcement::read_member_period_scoped` / `entitlement`'s
/// `PLAN_ENTITLEMENT_SQL` resolve the SAME row from the same snapshot.
const CURRENT_GRANT_SQL: &str = "\
    SELECT g.id AS grant_id, \
           g.credits_override, \
           COALESCE(g.seats, 1)::bigint AS seats, \
           p.credits AS plan_credits, \
           g.plan_id, \
           (p.code = $2) AS is_team_plan, \
           (p.code = $2 AND g.credits_override IS NULL) AS use_seat_tier_credits \
      FROM grants g JOIN plans p ON p.id = g.plan_id \
     WHERE g.account_id = $1 AND g.revoked_at IS NULL \
     ORDER BY g.granted_at DESC, g.plan_is_seat_priced DESC, g.id DESC LIMIT 1";

/// `CURRENT_GRANT_SQL`'s row shape — `(grant_id, credits_override, seats,
/// plan_credits, plan_id, is_team_plan, use_seat_tier_credits)` — named
/// because clippy's `type_complexity` lint flags the bare 7-element tuple
/// inline at both call sites ([`roll_period`], [`seed_member_period`]), and
/// a name is clearer at a destructuring site than counting tuple positions
/// against the column list above.
type CurrentGrantRow = (Uuid, Option<Decimal>, i64, Decimal, Uuid, bool, bool);

/// The per-member allowance for a NON-tiered grant — a flat plan,
/// `team_standard`/`team_premium`, or a `team` grant that carries
/// `credits_override` (which deliberately falls back to this formula rather
/// than the seat-tier join; see the module doc's "Per-tier allowances for
/// `team` grants" section) — `COALESCE(credits_override / seats,
/// plan_credits)`, stated once so [`roll_period`] and [`seed_member_period`]
/// can never let the two copies drift (this module's own discipline,
/// applied to itself).
///
/// **Takes `divisor` as a plain argument rather than reading it from the
/// grant row itself (PR E review round 2, finding 5/A):** the caller MUST
/// pass the POST-reconciliation count for a `team` grant — see
/// [`CURRENT_GRANT_SQL`]'s own doc comment for the bug this ordering fixes,
/// and [`seed_member_period`]'s call site for why its mid-period path needed
/// the identical fix (a `credits_override` divided by yesterday's count on
/// every mid-period join, not just at roll time).
///
/// **`divisor` is the RECEIVING MEMBER COUNT, not `grants.seats` (billing
/// self-serve PR F review, finding 3).** An earlier version of
/// [`roll_period`] passed `seats` (the PURCHASED total) here, which is wrong
/// the moment a roster outgrows its purchased seats — the FOUNDER-CONFIRM
/// steady state for a `team` account whose pre-existing members were never
/// auto-purchased (`billing::seat_lifecycle`'s own module doc). `override /
/// seats`, applied to EVERY roster row by [`PER_MEMBER_GRANT_SQL`]'s
/// `INSERT ... SELECT FROM account_members`, mints `override × (members /
/// seats)` in total instead of `override` — a roster of 4 against 1
/// purchased seat turned an 8,000-credit override into 32,000 actually
/// granted. `roll_period` now passes `members` (`MEMBER_COUNT_SQL`, read
/// over the SAME `account_members` predicate `PER_MEMBER_GRANT_SQL`'s own
/// `SELECT` uses, in the same transaction, so the two counts can never
/// disagree) instead — `override / members`, applied to every one of those
/// same `members` rows, sums back to exactly `override` regardless of how
/// purchased seats and roster size compare. Identical to the old figure
/// exactly when `members == seats` (the ordinary, non-drifted case), which is
/// why the aligned-case test below is unchanged. [`seed_member_period`]'s own
/// call site is NOT part of this fix — it still passes `seats`, a narrower,
/// pre-existing imprecision on the mid-period single-member path that this
/// finding did not target (see that call site's own comment).
///
/// **Precondition: `divisor >= 1` is NOT guaranteed by the caller (billing
/// self-serve PR F review, CRITICAL finding).** `members`
/// (`MEMBER_COUNT_SQL`, a bare `COUNT(*)`) can legitimately be `0` — a
/// granted `team` account with nobody on the roster yet, exactly the state
/// `ensure_org_account` leaves a brand-new org in before its first member
/// joins. Unlike the OLD `seats` argument (`COALESCE(g.seats, 1)`, always
/// `>= 1`), a bare roster count has no such floor. This function clamps with
/// `.max(1)` below rather than trusting every call site to pre-guard the
/// zero case: with zero receiving members, [`PER_MEMBER_GRANT_SQL`] writes
/// no rows at all, so the divided value is computed but never actually used
/// — clamping instead of guarding upstream is what keeps this arithmetically
/// total rather than relying on every current and future caller to remember
/// the guard.
fn legacy_per_member_credits(credits_override: Option<Decimal>, divisor: i64, plan_credits: Decimal) -> Decimal {
    match credits_override {
        // `divisor` is a roster COUNT(*) at every call site and can
        // legitimately be 0 (a granted account with nobody on the roster
        // yet). Clamped to 1: with no receiving members the caller's own
        // INSERT ... SELECT writes no rows at all, so this value is unused —
        // but dividing by 0 would panic inside the caller's transaction.
        Some(over) => over / Decimal::from(divisor.max(1)),
        None => plan_credits,
    }
}

/// The grant itself. One row per current member, at the per-member allowance,
/// idempotent under redelivery via `0161`'s UNIQUE index.
///
/// `cap_credits` is not named at all, so it takes the column default of NULL —
/// see the module doc for why the renewal must not write it.
///
/// **PR E (billing self-serve launch, per-tier allowances):** the `LEFT JOIN
/// seat_tiers` and `CASE` are the "extend it with the member→tier join"
/// shape the module doc's "Per-tier allowances for `team` grants" section
/// describes — one statement, still. `$5` (`use_seat_tier_credits`) is
/// `false` for every plan except `team` (and even for `team` when a
/// `credits_override` is set), in which case the `CASE` always takes the
/// `$4` branch and this is byte-for-byte the pre-PR-E query with `$4` bound
/// in `$4`'s old position — no behavior change for any plan this query
/// already served correctly. When `$5` is `true`, `st.credits` is NEVER NULL
/// for a row this INSERT reaches: `account_members_seat_tier_iff_org`
/// (`0169`) guarantees every ORG member carries a non-NULL `seat_tier_id`,
/// `team` grants exist only for org accounts, and `seat_tier_id` is a real FK
/// to `seat_tiers` — so the `LEFT JOIN` behaves as an INNER JOIN in practice
/// for this branch, and if that invariant were ever violated, the resulting
/// NULL `granted_credits` would fail `member_allowance_periods`' own NOT NULL
/// constraint rather than silently write a zero-credit row — loud, not
/// quiet, matching this module's "does not fail open" contract.
/// **PR E review round 2, finding 1:** wrapped in a `WITH ... RETURNING` CTE
/// so the caller gets BOTH the row count and the actual SUM of what was
/// written in the same round trip, rather than only `rows_affected()`. See
/// [`RenewalOutcome::summary`] for why: the legacy `per_member={credits}`
/// format is meaningless for a tiered `team` roll (every row's
/// `granted_credits` is different), and the LEGACY fallback scalar happens
/// to be `0` for every such roll (`plans.credits = 0` for `team`, 0167's own
/// header) — which reads identically to a genuine zero-credit bug in the
/// `stripe_webhook_events.outcome` audit trail. `COUNT(*)`/`SUM(...)` over
/// the `RETURNING` clause naturally answers "how many, how much" using
/// exactly the rows this statement itself inserted — `ON CONFLICT DO
/// NOTHING` rows are (by definition) not returned, so this remains the same
/// idempotent-under-redelivery count `rows_affected()` used to report.
const PER_MEMBER_GRANT_SQL: &str = "\
    WITH ins AS ( \
        INSERT INTO member_allowance_periods \
            (account_id, user_id, period_start, period_end, granted_credits) \
        SELECT $1, am.user_id, $2, $3, \
               CASE WHEN $5 THEN st.credits ELSE $4 END \
          FROM account_members am \
          LEFT JOIN seat_tiers st ON st.id = am.seat_tier_id \
         WHERE am.account_id = $1 \
           AND (NOT $6 OR NOT EXISTS ( \
                SELECT 1 FROM member_allowance_periods p \
                 WHERE p.account_id = $1 AND p.user_id = am.user_id \
                   AND p.period_start < $3 AND p.period_end > $2)) \
        ON CONFLICT (account_id, user_id, period_start) DO NOTHING \
        RETURNING granted_credits) \
    SELECT COUNT(*)::bigint AS rows_granted, \
           COALESCE(SUM(granted_credits), 0) AS total_credits_granted \
      FROM ins";

const MEMBER_COUNT_SQL: &str = "SELECT COUNT(*) FROM account_members WHERE account_id = $1";

/// **CodeRabbit, PR #655 review.** Whether `member_allowance_periods` already
/// holds AT LEAST ONE row for this exact `(account_id, period_start)` —
/// i.e. whether some earlier call already rolled this period, as opposed to
/// this being the FIRST roll that will seed it. [`roll_period`] reads this
/// BEFORE [`APPLY_PENDING_DOWNGRADES_SQL`] and gates that apply on the
/// result — see the module doc's "Pending downgrades, applied at roll"
/// section for the double-delivery bug this closes and why the check is
/// race-safe under the caller's already-held [`lock_period_rotation`].
///
/// A plain `SELECT EXISTS`, not a `COUNT`: this only ever needs a boolean,
/// and Postgres can stop at the first matching row rather than counting
/// every member's row for the period.
const PERIOD_ALREADY_ROLLED_SQL: &str = "\
    SELECT EXISTS(SELECT 1 FROM member_allowance_periods \
                   WHERE account_id = $1 AND period_start = $2)";

/// **PR E (billing self-serve launch): pending seat-tier downgrades, applied
/// atomically near the top of [`roll_period`].** See the module doc's
/// "Pending downgrades, applied at roll" section for why this must run
/// before [`roll_period`] counts members or computes an allowance, why
/// [`seed_member_period`]'s mid-period join path deliberately does NOT run
/// this, and (CodeRabbit, PR #655 review) why the caller gates this on
/// [`PERIOD_ALREADY_ROLLED_SQL`] rather than running it unconditionally.
const APPLY_PENDING_DOWNGRADES_SQL: &str = "\
    UPDATE account_members \
       SET seat_tier_id = pending_seat_tier_id, \
           pending_seat_tier_id = NULL \
     WHERE account_id = $1 AND pending_seat_tier_id IS NOT NULL";

/// **PR F review, finding 9: pending-downgrade move PAIRS, `(old_tier,
/// new_tier, cnt)` — grouped by BOTH endpoints, not just one side.** The
/// retired version of this move ran three blanket statements: the
/// delete/decrement pair grouped `moves` by OLD tier alone, and the upsert
/// grouped a SEPARATE `moves` CTE by NEW tier alone. That asymmetry is what
/// let the old side silently no-op (a missing `grant_seat_tiers` row, or one
/// holding FEWER seats than the pending count, matched neither the `<=` delete
/// predicate meaningfully nor the `>` decrement one) while the new side still
/// added the FULL pending count regardless — minting seats the old side never
/// actually held. Grouping by the pair instead is what lets
/// [`move_pending_seat_tier_counts`] measure exactly how many seats the old
/// side ACTUALLY gave up (via `RETURNING`/`rows_affected`) before deciding how
/// many the new side receives — see that function's own doc comment for the
/// per-pair transfer loop this drives.
const PENDING_DOWNGRADE_MOVE_PAIRS_SQL: &str = "\
    SELECT seat_tier_id AS old_tier, pending_seat_tier_id AS new_tier, COUNT(*)::bigint AS cnt \
      FROM account_members \
     WHERE account_id = $1 AND pending_seat_tier_id IS NOT NULL \
     GROUP BY seat_tier_id, pending_seat_tier_id";

/// The OLD tier's currently-ledgered seat count for one pending-downgrade
/// pair — read before deciding whether this pair's move is a full transfer,
/// a partial (drifted) one, or nothing at all. `FOR UPDATE`: this function's
/// only caller already holds [`lock_period_rotation`] for the whole account,
/// which serializes every OTHER writer of this table, but a plain row lock
/// costs nothing extra and documents the intent inline rather than relying
/// solely on the advisory lock a reader of this one query cannot see.
const GRANT_SEAT_TIER_CURRENT_SEATS_SQL: &str =
    "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2 FOR UPDATE";

const DELETE_GRANT_SEAT_TIER_SQL: &str = "DELETE FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2";

const DECREMENT_GRANT_SEAT_TIER_SQL: &str =
    "UPDATE grant_seat_tiers SET seats = seats - $3 WHERE grant_id = $1 AND seat_tier_id = $2";

const UPSERT_GRANT_SEAT_TIER_SQL: &str = "\
    INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3) \
    ON CONFLICT (grant_id, seat_tier_id) DO UPDATE SET seats = grant_seat_tiers.seats + excluded.seats";

/// The specific member(s) a shortfall left stranded — named in the shortfall
/// log line so an operator reading it does not have to reconstruct who is
/// affected from `(old_tier, new_tier)` alone.
const STRANDED_PENDING_DOWNGRADE_MEMBERS_SQL: &str = "\
    SELECT user_id FROM account_members \
     WHERE account_id = $1 AND seat_tier_id = $2 AND pending_seat_tier_id = $3";

/// **Sync `grants.seats` to the current `grant_seat_tiers` total.** Carried
/// over byte-for-byte from PR E, but reached from a different place now: PR
/// E called this after a from-scratch ROSTER recompute; PR F
/// ([`resync_grant_seats`]'s callers) call it after a pending-downgrade MOVE
/// or an `acquire_seat`/tier-change/`org_slots` ledger write, to resync
/// `grants.seats` — the Σ across tiers, 0168's header left this meaning
/// unstated on purpose, deferring to renewal logic — once the write above has
/// changed what that Σ is. The `EXISTS` guard is required for the identical
/// reason PR E needed it: a seat-priced grant's `seats` can be neither NULL
/// (`grants_seats_iff_seat_priced`) nor `0` (`0156`'s `CHECK (seats IS NULL
/// OR seats >= 1)`), so a grant with NO `grant_seat_tiers` rows at all (nobody
/// has purchased a seat on it yet) must leave `grants.seats` UNTOUCHED rather
/// than have this UPDATE attempt to write a `0` the schema would reject
/// outright. `RETURNING seats::bigint` hands the post-sync value back in the
/// same round trip every caller needs for its own "seats" report, rather than
/// a second SELECT.
///
/// **No `COALESCE(..., 0)` around the `SUM`** — every `grant_seat_tiers` row
/// this `SUM` reaches has `seats >= 1` (`0168`'s own CHECK) by construction,
/// so once the `EXISTS` guard is satisfied the sum can never actually be
/// NULL or zero; a `COALESCE` would only suggest a fallback this statement
/// cannot reach.
const RECONCILE_GRANTS_SEATS_SQL: &str = "\
    UPDATE grants \
       SET seats = (SELECT SUM(gst.seats) \
                       FROM grant_seat_tiers gst WHERE gst.grant_id = $1) \
     WHERE id = $1 AND EXISTS (SELECT 1 FROM grant_seat_tiers WHERE grant_id = $1) \
    RETURNING seats::bigint";

/// **The SHARED `grants.seats` resync helper (billing self-serve PR F
/// review, minor finding).** Every writer of `grant_seat_tiers` in this crate
/// used to hand-copy [`RECONCILE_GRANTS_SEATS_SQL`] independently, and two of
/// the five copies were missing the zero-guard `EXISTS` that constant's own
/// doc comment explains is required (a seat-priced grant's `seats` can be
/// neither NULL nor `0`). One function, called from every site, is what makes
/// that guard impossible to drop by accident at the next one.
///
/// The current callers are this module's own pending-downgrade move,
/// `billing::seat_lifecycle::acquire_seat`, `routes::billing_seats`'
/// tier-change route, `routes::stripe_webhook`'s checkout-completion
/// seat-ledger seed, and `routes::org_slots`' admin buy AND reduce actions.
/// **`release_seat` is deliberately NOT among them as of CS-444** — a member
/// leaving frees their slot back into the pool rather than un-purchasing it,
/// so a release writes no `grant_seat_tiers` row and has nothing to resync.
pub(crate) async fn resync_grant_seats(
    conn: &mut PgConnection,
    grant_id: Uuid,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(RECONCILE_GRANTS_SEATS_SQL)
        .bind(grant_id)
        .fetch_optional(&mut *conn)
        .await
}

/// **PR F.** Moves `grant_seat_tiers` counts old-tier → new-tier for every
/// PENDING seat-tier downgrade about to be applied to `account_id`'s `team`
/// grant `grant_id`, then resyncs `grants.seats` — see the module doc's
/// "`grant_seat_tiers` is a purchase-driven ledger" section for the full
/// rationale. Formerly `reconcile_team_grant_seat_tiers` (PR E), which
/// recomputed the WHOLE breakdown from the roster on every call; this
/// function instead moves only what a pending downgrade is about to change,
/// and — unlike its predecessor — is NOT safe to call twice for the same
/// pending rows (see [`roll_period`]'s own `!already_rolled` gate, the
/// module doc's "Gated on the identical `!already_rolled` check" paragraph).
///
/// # Transfer-exact, per pair (review, finding 9)
///
/// For each `(old_tier, new_tier, cnt)` pair ([`PENDING_DOWNGRADE_MOVE_PAIRS_SQL`]),
/// this reads the OLD tier's CURRENT ledgered seat count and computes
/// `actual_removed = min(current, cnt)` — never more than the old side
/// genuinely holds. The old side is decremented (or deleted, at the same
/// "zero seats has no row" convention 0168 documents) by EXACTLY
/// `actual_removed`, and the new side receives EXACTLY `actual_removed`, not
/// the raw pending `cnt`. In the ordinary case (the ledger already agrees with
/// the roster) `actual_removed == cnt` and this is byte-for-byte the old
/// behavior. When it does not — a `grant_seat_tiers` row missing entirely, or
/// holding fewer seats than the number of members whose `pending_seat_tier_id`
/// claims to be moving off it (both symptoms of a pre-existing ledger drift
/// this move did not cause) — the shortfall is logged LOUDLY, naming the
/// stranded member(s) via [`STRANDED_PENDING_DOWNGRADE_MEMBERS_SQL`], and
/// NOTHING is minted on the new side to cover it. The account's ledger stays
/// drifted (as it already was), rather than this move quietly manufacturing
/// seats nobody purchased to paper over a discrepancy it did not create — the
/// same "loud over quiet" posture this module uses throughout. This is a log,
/// not a propagated error: a genuinely pre-existing drift must not fail the
/// whole roll (and, transitively, the Stripe webhook transaction it runs
/// inside) for every OTHER, unaffected member's allowance.
///
/// # Ordering: caller MUST call this BEFORE [`APPLY_PENDING_DOWNGRADES_SQL`]
///
/// [`PENDING_DOWNGRADE_MOVE_PAIRS_SQL`] keys off `pending_seat_tier_id IS NOT
/// NULL` — exactly the set of rows [`APPLY_PENDING_DOWNGRADES_SQL`] is about
/// to clear. Calling this AFTER that flip would move zero seats: nothing would
/// still read as pending by the time this ran.
///
/// Returns the POST-move seat total when [`resync_grant_seats`]'s `EXISTS`
/// guard did not skip the write (at least one `grant_seat_tiers` row exists
/// for this grant after the move); `None` otherwise — a grant with no
/// purchased seats at all has nothing to move and nothing to sync.
///
/// # Caller MUST hold an open transaction with `lock_period_rotation` taken
///
/// [`roll_period`] is this function's only caller, and it always holds both
/// — the per-account advisory lock serializes this move against a concurrent
/// `acquire_seat`/tier-change/`org_slots` buy-or-reduce write to the SAME
/// `grant_seat_tiers` rows (`billing::seat_lifecycle`,
/// `routes::billing_seats`, `routes::org_slots`), all of which take the
/// identical lock before touching this table. `release_seat` takes it too
/// even though (CS-444) it no longer writes this table — it changes
/// `billing::slot_pool::SlotPool::used`, which the reduce path's own refusal
/// gate reads under the same lock.
async fn move_pending_seat_tier_counts(
    conn: &mut PgConnection,
    grant_id: Uuid,
    account_id: Uuid,
) -> Result<Option<i64>, sqlx::Error> {
    let pairs: Vec<(Uuid, Uuid, i64)> = sqlx::query_as(PENDING_DOWNGRADE_MOVE_PAIRS_SQL)
        .bind(account_id)
        .fetch_all(&mut *conn)
        .await?;
    if pairs.is_empty() {
        // Billing self-serve PR F review, MAJOR finding: nothing moved, so
        // there is nothing for `grant_seat_tiers`' Σ to have changed —
        // matching this function's own doc comment, "Returns the POST-move
        // seat total... a grant with no purchased seats at all has nothing
        // to move and nothing to sync." An unconditional `resync_grant_seats`
        // call here would otherwise silently CORRECT `grants.seats` from the
        // ledger on every roll, even one with zero pending downgrades —
        // rewriting an account whose `grants.seats` and ledger Σ genuinely
        // disagree for some OTHER, pre-existing reason, on a roll that moved
        // nothing at all.
        return Ok(None);
    }

    for (old_tier, new_tier, cnt) in pairs {
        let old_seats: i64 = sqlx::query_scalar::<_, i32>(GRANT_SEAT_TIER_CURRENT_SEATS_SQL)
            .bind(grant_id)
            .bind(old_tier)
            .fetch_optional(&mut *conn)
            .await?
            .unwrap_or(0)
            .into();
        let actual_removed = old_seats.min(cnt);

        if actual_removed > 0 {
            if actual_removed >= old_seats {
                sqlx::query(DELETE_GRANT_SEAT_TIER_SQL)
                    .bind(grant_id)
                    .bind(old_tier)
                    .execute(&mut *conn)
                    .await?;
            } else {
                sqlx::query(DECREMENT_GRANT_SEAT_TIER_SQL)
                    .bind(grant_id)
                    .bind(old_tier)
                    .bind(actual_removed as i32)
                    .execute(&mut *conn)
                    .await?;
            }
            sqlx::query(UPSERT_GRANT_SEAT_TIER_SQL)
                .bind(grant_id)
                .bind(new_tier)
                .bind(actual_removed as i32)
                .execute(&mut *conn)
                .await?;
        }

        if actual_removed < cnt {
            let stranded: Vec<Uuid> = sqlx::query_scalar(STRANDED_PENDING_DOWNGRADE_MEMBERS_SQL)
                .bind(account_id)
                .bind(old_tier)
                .bind(new_tier)
                .fetch_all(&mut *conn)
                .await?;
            tracing::error!(
                %account_id, %grant_id, %old_tier, %new_tier,
                pending = cnt, actually_removed = actual_removed, members = ?stranded,
                "billing::renewal::move_pending_seat_tier_counts: the old tier's purchase-ledger \
                 row could not supply enough seats for this pending downgrade — moved only what \
                 actually existed there rather than minting seats the old side never held. This \
                 account's grant_seat_tiers is already drifted (pre-existing, not caused by this \
                 move); the member(s) named above did not receive a corresponding move to the \
                 new tier."
            );
        }
    }

    resync_grant_seats(conn, grant_id).await
}

/// A single member's own seat-tier allowance — [`seed_member_period`]'s
/// counterpart to [`PER_MEMBER_GRANT_SQL`]'s per-row `LEFT JOIN seat_tiers`,
/// narrowed to the ONE member being seeded rather than the whole roster. See
/// the module doc's "Per-tier allowances for `team` grants" section.
const MEMBER_SEAT_TIER_CREDITS_SQL: &str = "\
    SELECT st.credits FROM account_members am \
      JOIN seat_tiers st ON st.id = am.seat_tier_id \
     WHERE am.account_id = $1 AND am.user_id = $2";

/// The ordinary case: a period that started before `boundary` and is still
/// open at it (`period_start < boundary AND period_end > boundary`) gets its
/// `period_end` pulled back to `boundary`, and nothing else.
///
/// `period_end > $2` is what makes this SHRINK-ONLY. A row already ending at
/// or before `boundary` does not match and is left byte-for-byte alone, so a
/// caller that passes a `boundary` earlier than intended can never WIDEN a
/// period that has already closed — the worst this predicate can do wrong is
/// a no-op, never a corruption in the other direction.
const CLOSE_OPEN_PERIOD_SQL: &str = "\
    UPDATE member_allowance_periods \
       SET period_end = $2, updated_at = now() \
     WHERE account_id = $1 AND period_start < $2 AND period_end > $2";

/// The degenerate case [`CLOSE_OPEN_PERIOD_SQL`] cannot truncate: a row whose
/// `period_start` is strictly AFTER `boundary`. Setting `period_end =
/// boundary` there would leave `period_end <= period_start`, which `0161`'s
/// `member_allowance_period_end_after_start` CHECK refuses outright — the
/// only way to keep such a row alive would be to ALSO move `period_start`
/// backward past `boundary`, which fabricates a start date nothing ever
/// granted.
///
/// This is deleted instead. A period whose window has not actually begun as
/// of `boundary` — whether because it was seeded ahead of schedule
/// (`grant_plan`) or because a later-dated Stripe invoice landed before an
/// earlier one (`stripe_webhook`, out-of-order delivery) — has no elapsed
/// history to preserve: nobody has drawn against it yet, because `boundary`
/// has not reached it. Rewriting its `period_start` to make truncation
/// possible would be strictly worse: it would silently falsify WHEN the
/// period began, which is exactly the kind of fabricated fact `0161`'s own
/// header (`WHY NO FK ON user_id`, `WHY cap_credits IS NULLABLE`) already
/// argues this table must never manufacture. Deleting a row that never
/// applied is not a fabrication; it is the truth.
///
/// **Deliberately `>`, not `>=`.** A row whose `period_start` EQUALS
/// `boundary` exactly matches NEITHER this statement NOR
/// [`CLOSE_OPEN_PERIOD_SQL`] — on purpose. That row is not a stale period to
/// close; either it is exactly the period a caller is about to (re-)seed
/// (`grant_plan` re-run, or `stripe_webhook` claiming a NEW `event_id` for an
/// ALREADY-granted period — `stripe_webhook::pg_tests::
/// a_new_event_for_an_already_granted_period_grants_nothing_extra` exercises
/// this), in which case `0161`'s UNIQUE index / the INSERT's own conflict
/// resolution is what must decide, not this function — or it is a different
/// member's row that has already been correctly seeded to start exactly
/// here, which is simply not this function's business. Using `>=` was tried
/// and measured wrong: it deletes that exact-match row out from under a
/// still-valid `ON CONFLICT DO NOTHING`, so the INSERT that follows no
/// longer conflicts with anything and silently grants a SECOND time — the
/// exact double-grant this whole migration exists to prevent, reintroduced
/// by the "fix" for it.
const DELETE_UNSTARTED_OPEN_PERIOD_SQL: &str = "\
    DELETE FROM member_allowance_periods \
     WHERE account_id = $1 AND period_start > $2";

/// What one [`roll_period`] call did. Every field is reported rather than
/// interpreted here, so the caller can record the numbers verbatim and the
/// tests can assert on them without re-deriving anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewalOutcome {
    /// Rows actually inserted. **Zero on a redelivered event** — that is
    /// success, not failure, and the caller must not treat it as an error.
    pub rows_granted: u64,
    /// Members on the roster at the instant the period rolled.
    pub members: i64,
    /// Purchased seats, `COALESCE(grants.seats, 1)`. `None` when the account
    /// has no active grant.
    ///
    /// `i64` rather than `grants.seats`'s own `INT4`, so it is directly
    /// comparable with [`Self::members`] (a `COUNT(*)`, which Postgres returns
    /// as `INT8`) without a cast at every comparison. The SQL casts to
    /// `::bigint` to match — decoding an `INT4` as `i64` is a hard
    /// `ColumnDecode` error in sqlx, not a silent widening, which is how the
    /// live suite caught the missing cast.
    pub seats: Option<i64>,
    /// The per-member allowance written to each row. `None` when the account
    /// has no active grant, in which case nothing was written at all.
    ///
    /// **PR E caveat, mixed-tier `team` grants:** for a `team` grant that
    /// used the per-tier join (`CURRENT_GRANT_SQL`'s `use_seat_tier_credits`
    /// — no `credits_override`), the rows actually written are NOT uniform
    /// — a `standard` member gets 3,000, a `premium` member gets 15,000 — so
    /// this single scalar cannot describe them. It still reports the
    /// LEGACY fallback figure (`COALESCE(credits_override / seats,
    /// plans.credits)`, which is `0` for `team` per 0167's header) rather
    /// than being widened to `None`, specifically so [`Self::no_active_plan`]
    /// (which tests THIS field for `None`) keeps meaning "no grant" and
    /// nothing else — a caller reading this field for a `team` roll must not
    /// treat it as "the" allowance; read `member_allowance_periods` directly
    /// for the real per-member number, or read [`Self::total_credits_granted`]
    /// for the aggregate [`Self::summary`] itself now uses (PR E review
    /// round 2, finding 1) instead of this misleading scalar.
    pub per_member_credits: Option<Decimal>,
    /// **PR E review round 2, finding 1.** The actual SUM of
    /// `granted_credits` written by this call's `PER_MEMBER_GRANT_SQL`
    /// `INSERT`, read back via its own `RETURNING` clause rather than
    /// re-derived. Correct whether the roll was uniform
    /// (`per_member_credits * rows_granted`, always, by construction — flat
    /// plans and `team_standard`/`team_premium`) or tiered (`team`, no
    /// single per-member figure to multiply). `None` in the same case
    /// [`Self::per_member_credits`] is `None` — no active grant, nothing was
    /// written.
    pub total_credits_granted: Option<Decimal>,
    /// **PR E review round 2, finding 1.** Whether this roll used the
    /// per-tier `seat_tiers` join (`CURRENT_GRANT_SQL`'s
    /// `use_seat_tier_credits`) rather than a single uniform figure — i.e.
    /// whether [`Self::per_member_credits`] is the real per-row allowance
    /// (`false`) or merely the LEGACY fallback scalar this struct's own doc
    /// comment already warns about (`true`). Drives [`Self::summary`]'s
    /// `per_member=tiered` branch. Always `false` when
    /// [`Self::no_active_plan`] holds.
    pub use_seat_tier_credits: bool,
    /// Whether the account's current grant is for the mixed-tier `team`
    /// plan at all — regardless of `credits_override` (unlike
    /// [`Self::use_seat_tier_credits`], which is specifically `false` when
    /// an override is set). Drives [`Self::summary`]'s formatting only as of
    /// PR F — [`Self::seat_mismatch`] no longer reads it at all; see that
    /// method's own doc comment for why the `team`-specific suppression it
    /// used to gate on this field was retired. Always `false` when
    /// [`Self::no_active_plan`] holds.
    pub is_team_plan: bool,
}

impl RenewalOutcome {
    /// The account is paying Stripe but holds no active plan grant, so there is
    /// no allowance to compute and NOTHING WAS WRITTEN.
    ///
    /// This is a real provisioning failure, not a quiet no-op: a customer whose
    /// invoice was paid has an unmet expectation from the moment it happens.
    /// The caller must surface it rather than answering "renewed".
    pub fn no_active_plan(&self) -> bool {
        self.per_member_credits.is_none()
    }

    /// The roster and the purchased seat count disagree, which the founder's
    /// model says cannot legitimately happen — adding a member IS purchasing a
    /// seat. See the module doc: every member was still granted, and this
    /// reports that the member-add path failed to buy a seat.
    ///
    /// Always `false` when [`Self::no_active_plan`] holds — there is no seat
    /// count to disagree with, and reporting a mismatch there would be a second
    /// alert about the same single fact.
    ///
    /// **No longer suppressed for a `team` grant (billing self-serve launch
    /// PR F — supersedes PR E review round 2, finding 7, whose suppression
    /// is retired here, not merely narrowed).** PR E's version of this
    /// method returned `false` unconditionally whenever `is_team_plan` held,
    /// because `grants.seats` for a `team` grant used to be DERIVED from the
    /// same roster `members` counts every roll (PR E's now-removed
    /// `reconcile_team_grant_seat_tiers`), which made a zero-member roster's
    /// stale, un-reconcilable-to-`0` `seats` value a structural false alarm
    /// rather than a real signal — see this module's doc comment, "The
    /// `team` seat_mismatch signal is un-suppressed (PR F)", for the full
    /// argument. `grants.seats` for a `team` grant is now Σ of a purchase
    /// ledger (`grant_seat_tiers`) maintained independently of the roster by
    /// `billing::seat_lifecycle::acquire_seat`, `routes::org_slots`' admin
    /// buy/reduce actions, and this module's own pending-downgrade move.
    ///
    /// **One-directional as of CS-435 (D1/D7/D9, org-onboarding plan §3 item
    /// 7) — `seats > members` no longer fires.** `billing::seat_lifecycle::
    /// acquire_seat` now consumes a pre-purchased slot before it ever calls
    /// Stripe (`billing::slot_pool`'s own module doc), which makes
    /// `purchased > members` — an admin's `routes::org_slots` "buy N slots"
    /// ahead of who has actually joined — the NORMAL, INTENDED steady state
    /// a healthy pool sits in, not a drift signal. Flagging it here would
    /// mean every account that ever pre-buys slots reports a permanent
    /// "mismatch", drowning the signal this method exists to surface. Only
    /// `seats < members` — the roster has outgrown what was purchased,
    /// which cannot happen through this crate's own writers and means
    /// something external drifted (a Stripe Dashboard edit, a crash between
    /// a Stripe push and this ledger's own commit — see `billing::
    /// seat_lifecycle`'s "Stripe push BEFORE the DB write, always" section)
    /// — is still reported. `members` here is the roster (`MEMBER_COUNT_SQL`,
    /// every current `account_members` row), a superset of `billing::
    /// slot_pool::SlotPool::used` (members whose seat is ALSO purchased): a
    /// pre-existing member never auto-purchased (this module's own
    /// FOUNDER-CONFIRM case) already makes `members > used`, so gating on
    /// the wider `members` count here is the MORE conservative of the two —
    /// it fires in every case `used` alone would, plus that already-expected
    /// case, exactly as it did before this change.
    pub fn seat_mismatch(&self) -> bool {
        self.seats.is_some_and(|seats| seats < self.members)
    }

    /// A short machine-greppable summary for `stripe_webhook_events.outcome`,
    /// written in the same transaction as the work it describes.
    ///
    /// **PR E review round 2, finding 1.** A tiered `team` roll (
    /// [`Self::use_seat_tier_credits`]) no longer prints `per_member=` the
    /// LEGACY fallback scalar, which is `0` for every such roll (`team`'s
    /// `plans.credits = 0` by design, 0167's header) and used to be
    /// indistinguishable in this exact audit trail from the real
    /// zero-credit-grant bug the "per_member=0" signal was previously relied
    /// on to catch. It instead prints `per_member=tiered` plus
    /// [`Self::total_credits_granted`], the real aggregate this call
    /// actually wrote. The flat-plan / `team_standard` / `team_premium`
    /// format is UNCHANGED, byte-for-byte, so existing tests and log
    /// consumers of that shape are unaffected.
    pub fn summary(&self) -> String {
        match (self.per_member_credits.as_ref(), self.seats) {
            (Some(_), Some(seats)) if self.use_seat_tier_credits => format!(
                "renewed rows={} members={} seats={} per_member=tiered total_credits={}",
                self.rows_granted,
                self.members,
                seats,
                self.total_credits_granted.unwrap_or(Decimal::ZERO)
            ),
            (Some(credits), Some(seats)) => format!(
                "renewed rows={} members={} seats={} per_member={}",
                self.rows_granted, self.members, seats, credits
            ),
            _ => format!("no_active_plan members={} rows=0", self.members),
        }
    }
}

/// Roll one account's allowance period: grant every current member their
/// per-seat allowance for `[period_start, period_end)`.
///
/// Takes a connection rather than a pool because the caller MUST run this in
/// the same transaction as its idempotency claim — see
/// [`crate::routes::stripe_webhook`]. A commit boundary between the two would
/// reintroduce exactly the double-grant this is built to prevent.
///
/// The connection must already be account-scoped
/// (`tenancy::scope::begin_account_scoped`): `grants` and
/// `member_allowance_periods` are both RLS-covered on the control plane, so an
/// unscoped call hard-errors under `coralswarm_app` rather than silently
/// reading another account's rows.
///
/// **Does not fail open.** Unlike the read-side billing helpers, whose failure
/// mode is "never take down asks/search", a renewal that quietly failed would
/// leave every member of a paying account without an allowance for a whole
/// period. Errors propagate so the caller can refuse to record the event as
/// processed and let Stripe redeliver.
pub async fn roll_period(
    conn: &mut PgConnection,
    account_id: Uuid,
    period_start: chrono::DateTime<chrono::Utc>,
    period_end: chrono::DateTime<chrono::Utc>,
) -> Result<RenewalOutcome, sqlx::Error> {
    roll_period_inner(conn, account_id, period_start, period_end, false).await
}

/// [`roll_period`] for a Stripe event that reports the SAME billing period the
/// account is already in (a seat-quantity change, a status flip): grants an
/// allowance only to members who hold no period overlapping
/// `[period_start, period_end)` yet, and leaves every period a member already
/// has exactly as it is. The caller must NOT run [`close_open_period`] first
/// (that would delete or truncate the very rows this keeps). Everything else
/// (pending downgrades, per-member credits, the outcome) is `roll_period`'s.
pub async fn roll_period_keeping_open(
    conn: &mut PgConnection,
    account_id: Uuid,
    period_start: chrono::DateTime<chrono::Utc>,
    period_end: chrono::DateTime<chrono::Utc>,
) -> Result<RenewalOutcome, sqlx::Error> {
    roll_period_inner(conn, account_id, period_start, period_end, true).await
}

async fn roll_period_inner(
    conn: &mut PgConnection,
    account_id: Uuid,
    period_start: chrono::DateTime<chrono::Utc>,
    period_end: chrono::DateTime<chrono::Utc>,
    keep_open_periods: bool,
) -> Result<RenewalOutcome, sqlx::Error> {
    // Step -1 (CodeRabbit, PR #655 review): is this the FIRST roll of THIS
    // `period_start`, or a repeat call for a period this account already
    // rolled (a redelivered / second Stripe event reporting the SAME
    // period)? Must be answered before Step 0 below touches anything — see
    // the module doc's "Pending downgrades, applied at roll" section for the
    // bug this closes and why it is safe to check-then-apply here: every
    // production caller holds the per-account `lock_period_rotation`
    // advisory lock before calling `roll_period`, so no concurrent
    // transaction can insert this period's first `member_allowance_periods`
    // row between this SELECT and this function's own INSERT further down.
    let already_rolled: bool = sqlx::query_scalar(PERIOD_ALREADY_ROLLED_SQL)
        .bind(account_id)
        .bind(period_start)
        .fetch_one(&mut *conn)
        .await?;

    let members: i64 = sqlx::query_scalar(MEMBER_COUNT_SQL)
        .bind(account_id)
        .fetch_one(&mut *conn)
        .await?;

    // Fetched BEFORE the pending-downgrade step below (PR F reorder — PR E
    // fetched this AFTER) because that step now needs `grant_id`/
    // `is_team_plan` to know whether there is a purchase LEDGER to move
    // counts in, not just an `account_members` flip to apply.
    let grant: Option<CurrentGrantRow> =
        sqlx::query_as(CURRENT_GRANT_SQL)
            .bind(account_id)
            .bind(crate::billing::MIXED_TIER_PLAN_CODE)
            .fetch_optional(&mut *conn)
            .await?;

    // Step 0 (PR E, billing self-serve launch; ledger move added PR F):
    // apply every pending seat-tier downgrade for this account BEFORE
    // anything below reads `account_members` or computes an allowance — see
    // the module doc's "Pending downgrades, applied at roll" section for why
    // this must be first WITHIN a genuinely new period.
    //
    // **Gated on `!already_rolled` (CodeRabbit, PR #655 review — this used to
    // run unconditionally).** Without the gate, a second Stripe event for a
    // period this account already rolled would apply a downgrade scheduled
    // AFTER that first roll — flipping `seat_tier_id` and clearing
    // `pending_seat_tier_id` mid-period, before the period boundary the
    // founder's decision (module doc, §1.7) says a downgrade must wait for.
    // The existing `member_allowance_periods` row for this period keeps its
    // correct PRE-downgrade `granted_credits` either way (the INSERT below is
    // `ON CONFLICT DO NOTHING`), so skipping the apply here is what keeps
    // `account_members`/entitlement reads and the allowance row agreeing
    // until the period actually rolls over.
    //
    // **PR F: [`move_pending_seat_tier_counts`] runs FIRST, still under the
    // SAME `!already_rolled` gate** — see the module doc's "Gated on the
    // identical `!already_rolled` check" paragraph for why this move, unlike
    // PR E's from-scratch roster reconciliation it replaces, is NOT safe to
    // run unconditionally: it is a DELTA against whatever is currently
    // `pending_seat_tier_id IS NOT NULL`, and running it twice for the same
    // pending rows would double-move real purchased seats. It must run
    // BEFORE [`APPLY_PENDING_DOWNGRADES_SQL`] clears those same rows' pending
    // markers — see [`move_pending_seat_tier_counts`]'s own doc comment.
    // Gated on `is_team_plan` (only a `team` grant's ledger can hold
    // per-tier rows to move at all) and folded into the SAME `if
    // !already_rolled` as the flip, rather than a second independent check,
    // so the two can never disagree about whether this is a genuinely new
    // period.
    let mut moved_seats: Option<i64> = None;
    if !already_rolled {
        if let Some((grant_id, _, _, _, _, is_team_plan, _)) = &grant {
            if *is_team_plan {
                moved_seats =
                    move_pending_seat_tier_counts(&mut *conn, *grant_id, account_id).await?;
            }
        }
        sqlx::query(APPLY_PENDING_DOWNGRADES_SQL)
            .bind(account_id)
            .execute(&mut *conn)
            .await?;
    }

    let Some((_grant_id, credits_override, mut seats, plan_credits, _plan_id, is_team_plan, use_seat_tier_credits)) =
        grant
    else {
        return Ok(RenewalOutcome {
            rows_granted: 0,
            members,
            seats: None,
            per_member_credits: None,
            total_credits_granted: None,
            use_seat_tier_credits: false,
            is_team_plan: false,
        });
    };

    // PR F: if the pending-downgrade move above actually ran and changed the
    // ledger, use the POST-move seat total — not the value `CURRENT_GRANT_SQL`
    // read moments ago, before the move. `RenewalOutcome::seats` (reported
    // below) still needs this, even though `legacy_per_member_credits` no
    // longer divides by it (see the next comment) — a stale pre-move figure
    // would misreport the purchased total exactly like PR E's finding 5 bug
    // this guards against in the same spirit (see that fix's own history in
    // this module doc), just against a MOVE now instead of a roster recompute.
    if let Some(reconciled) = moved_seats {
        seats = reconciled;
    }

    // billing self-serve PR F review, finding 3: divide `credits_override`
    // by `members` (the roster this SAME transaction's `PER_MEMBER_GRANT_SQL`
    // is about to grant), not `seats` (the purchased total) — see
    // `legacy_per_member_credits`'s own doc comment for the over-grant bug
    // this closes and why the two counts can never disagree here.
    let per_member_credits = legacy_per_member_credits(credits_override, members, plan_credits);

    let (inserted, total_credits_granted): (i64, Decimal) = sqlx::query_as(PER_MEMBER_GRANT_SQL)
        .bind(account_id)
        .bind(period_start)
        .bind(period_end)
        .bind(per_member_credits)
        .bind(use_seat_tier_credits)
        .bind(keep_open_periods)
        .fetch_one(&mut *conn)
        .await?;

    Ok(RenewalOutcome {
        rows_granted: inserted as u64,
        members,
        seats: Some(seats),
        per_member_credits: Some(per_member_credits),
        total_credits_granted: Some(total_credits_granted),
        use_seat_tier_credits,
        is_team_plan,
    })
}

// ── Member-join seeding (enforcement-completion PR, plan §1.9/§2.A) ─────────
//
// [`roll_period`] above seeds every member's allowance ONCE, at the instant
// a period rolls, from a single snapshot of `account_members`. That is
// exactly wrong for a member who joins mid-period: they are not on the
// roster at roll time, so `roll_period` never grants them, and they would
// sit with no `member_allowance_periods` row until the NEXT period rolls —
// which, for an org account under the enforcement-completion PR (enforcement
// flips at org creation, before any member exists — see
// `clerk_provision::provision_billing`'s doc comment), means a brand-new
// member of a brand-new org has nothing to draw on the moment they can first
// make a request. [`seed_member_period`] is the mid-period counterpart to
// `roll_period`: it grants exactly ONE member, for the account's CURRENT
// period, the instant they join.

/// The `(account_id, user_id, period_start)` conflict target this shares
/// with [`PER_MEMBER_GRANT_SQL`] — restricted to exactly the ONE user being
/// seeded via a plain `VALUES` row rather than `PER_MEMBER_GRANT_SQL`'s
/// `INSERT ... SELECT FROM account_members`. Nobody else is even named in
/// this statement, which is what makes "never touch another member's row"
/// true by construction rather than by care: there is no roster read here to
/// get wrong.
const SEED_ONE_MEMBER_PERIOD_SQL: &str = "\
    INSERT INTO member_allowance_periods \
        (account_id, user_id, period_start, period_end, granted_credits) \
    VALUES ($1, $2, $3, $4, $5) \
    ON CONFLICT (account_id, user_id, period_start) DO NOTHING";

/// The window EVERY current member of this account already shares for "now",
/// read from whichever member's row happens to already exist. All of an
/// account's members agree on this window by construction — `roll_period`
/// always applies exactly ONE `(period_start, period_end)` pair to every
/// member it grants in a single call — so reading it back from any ONE
/// existing row is as authoritative as reading it from all of them.
///
/// **`statement_timestamp()`, not `now()`** (CodeRabbit re-review, finding
/// 1's audit: every time-based predicate that runs AFTER
/// [`lock_period_rotation`] must see time that reflects the moment it
/// actually executes, not the moment its enclosing transaction began). Inside
/// a transaction, `now()` is fixed at `BEGIN` and does NOT advance even while
/// a statement blocks waiting on the advisory lock — so a caller that waited
/// on contention would otherwise decide "is a period open right now" against
/// a timestamp from BEFORE it ever got to look, exactly the staleness that
/// let a concurrent roller's newer period get deleted by
/// [`DELETE_UNSTARTED_OPEN_PERIOD_SQL`] in `issuance::issue_grant` before
/// this fix. `statement_timestamp()` is the start time of THIS statement, so
/// it always reflects post-lock-acquisition reality. This runs after
/// [`lock_period_rotation`] at every call site ([`seed_member_period`]), so
/// there is no case where the transaction-start semantics of `now()` would
/// have been correct here.
const CURRENT_OPEN_PERIOD_SQL: &str = "\
    SELECT period_start, period_end FROM member_allowance_periods \
     WHERE account_id = $1 \
       AND period_start <= statement_timestamp() AND period_end > statement_timestamp() \
     ORDER BY period_start DESC LIMIT 1";

/// Whether ANY `member_allowance_periods` row — open, closed, whatever its
/// window — has EVER existed for this account. Deliberately not "is there an
/// OPEN one": see [`seed_member_period`]'s doc comment (finding 4, review
/// round 2) for why the fresh-window branch must gate on this broader
/// question instead.
const ANY_PERIOD_EVER_EXISTED_SQL: &str =
    "SELECT EXISTS(SELECT 1 FROM member_allowance_periods WHERE account_id = $1)";

/// The account's `stripe_subscriptions.status`, read inside
/// [`seed_member_period`]'s own scoped transaction — [`None`] means no row at
/// all (an account that has never gone through Stripe checkout: a free-tier
/// personal account, or a hand-comp'd org). See [`seed_member_period`]'s own
/// comment at its call site for why this must be consulted before minting a
/// full allowance for a mid-period joiner.
const ACCOUNT_SUBSCRIPTION_STATUS_SQL: &str =
    "SELECT status FROM stripe_subscriptions WHERE account_id = $1";

/// Per-account serialization for [`seed_member_period`]'s decision of which
/// window to seed into (finding 5, review round 2). Taken as the FIRST
/// statement in the scoped transaction, before any read of `grants` or
/// `member_allowance_periods` — same "lock first, read second" discipline as
/// `store::write_judgement_tx`'s own `take_judgement_pair_lock` call.
///
/// # Why this has to exist at all
///
/// Two members of the SAME brand-new org joining at once both run
/// [`seed_member_period`] concurrently. Under READ COMMITTED, both can
/// observe "no open period" via [`CURRENT_OPEN_PERIOD_SQL`] before either has
/// committed, so both take the fresh-window branch and both compute their OWN
/// `now()`-anchored window — two DIFFERENT windows, a heartbeat apart. `0163`'s
/// exclusion constraint is keyed per `(account_id, user_id)`, so this is not
/// caught at either INSERT: they are for two different members, and each
/// commits cleanly. The account now permanently disagrees with itself about
/// when its first period began. A LATER seed attempt for whichever member's
/// window lost the race (a re-invite replay, a backfill re-run) then
/// collides with the OTHER window at the exclusion constraint — not a
/// friendly `ON CONFLICT DO NOTHING`, because the conflicting row belongs to
/// a different `period_start`, so the natural key never matches — and fails
/// with `23P01` forever, not just once.
///
/// Locking here closes the window this race needs: the SECOND concurrent
/// caller for the same account blocks until the first commits, then re-reads
/// [`CURRENT_OPEN_PERIOD_SQL`] and finds the window the first caller just
/// established, taking the "reuse" branch instead of "fresh" — exactly the
/// same outcome as if the two joins had been serialized by the caller.
///
/// # Key shape
///
/// `pg_advisory_xact_lock(hashtextextended($1, 0))`, keyed on a
/// `"billing_period_rotation:{account_id}"` string — the same
/// `hashtextextended`-over-a-formatted-key shape `store::
/// take_judgement_pair_lock` uses (an explicit seed of `0`, not the
/// server-wide default `hashtext` relies on). Namespaced by a literal prefix
/// so this lock's keyspace can never collide with a DIFFERENT advisory lock
/// that happens to hash the same account id for an unrelated purpose.
/// Transaction-scoped (`_xact_`): released automatically at COMMIT or
/// ROLLBACK, no separate unlock call, and safe to take repeatedly within one
/// transaction (Postgres reference-counts it). Two DIFFERENT accounts never
/// contend.
///
/// The key text says "rotation", not "seed", because this lock is no longer
/// [`seed_member_period`]'s alone (finding 1, enforcement-completion PR
/// review round 2) — see [`lock_period_rotation`]'s own doc comment for why
/// [`roll_period`]/[`close_open_period`]'s callers now take it too, and why
/// they MUST use this exact key, not a lookalike.
const PERIOD_ROTATION_LOCK_SQL: &str = "SELECT public.billing_enter_account($1)";

/// Take the per-account advisory lock that serializes EVERY caller which
/// decides "reuse the open period or start a fresh one" and/or performs the
/// close-then-roll sequence — [`seed_member_period`] on the seed side,
/// [`crate::billing::issuance::issue_grant`] and both
/// `routes::stripe_webhook` handlers (`apply_invoice_paid`,
/// `apply_subscription_updated`) on the roller side.
///
/// # Why the seed lock alone was not enough (finding 1, review round 2)
///
/// [`seed_member_period`] used to take a lock keyed the same way, but ONLY
/// around its own reuse-vs-fresh decision — the period rollers
/// (`issue_grant`, and the two stripe_webhook sites) called
/// [`close_open_period`]/[`roll_period`] without taking it at all. That left
/// a window: a seed resolves the currently-open window W1 for a member just
/// as a concurrent roller closes W1 and rolls a brand-new W2 — inserting a
/// row for that SAME member into W2 as part of its account-wide
/// `INSERT ... SELECT`. The seed's own INSERT of W1 then still succeeds,
/// because by the time it runs, W1 (closed) and W2 (fresh) merely ABUT
/// rather than overlap, so `0163`'s exclusion constraint never fires and the
/// `23P01` defense-in-depth fallback (see [`seed_member_period`]'s
/// "Idempotent" section) never triggers either. The member ends up holding
/// TWO allowance rows for what should be one continuous membership — a
/// double grant, not merely a divergent window. This is reachable in
/// production: `bin/backfill_billing_enforcement` can call `issue_grant` and
/// `seed_member_period` for the same account within one pass, and
/// `routes::teams::invite_member` seeds outside of its own membership
/// transaction, so nothing but this lock stood between "membership row
/// exists" and "the account's period rotated underneath it".
///
/// # Why one shared key, not one lock per call site
///
/// Two advisory locks with different keys do not serialize against each
/// other at all — they would just be two independent locks, each
/// uselessly serializing a call site against ITSELF the way
/// `seed_member_period` alone used to. The fix is not "add a second lock",
/// it is "make every caller that can rotate or seed this account's periods
/// take the SAME lock" — see [`PERIOD_ROTATION_LOCK_SQL`]'s own doc comment
/// for the exact key string every call site must reproduce byte-for-byte.
///
/// # Where it must be taken
///
/// Always the FIRST statement inside the account-scoped transaction that
/// will go on to read [`CURRENT_OPEN_PERIOD_SQL`] (seed side) or call
/// [`close_open_period`]/[`roll_period`] (roller side) — before either reads
/// or writes `member_allowance_periods`, so the lock genuinely guards the
/// decision rather than being taken after the race window already closed on
/// its own.
///
/// # `pub`, not `pub(crate)` (CodeRabbit re-review, finding 3)
///
/// `bin/backfill_billing_enforcement.rs` is a SEPARATE binary crate that
/// links against this library crate rather than a module compiled into it,
/// so `pub(crate)` visibility — correct for every other caller, which all
/// live inside this crate's own `src/` tree — is invisible to it. The
/// backfill's final coverage-check-then-flip must take this exact lock too
/// (see that binary's own module doc for the in-flight-seed race it closes),
/// so this has to be crate-external `pub` rather than narrowed further.
pub async fn lock_period_rotation(
    conn: &mut PgConnection,
    account_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(PERIOD_ROTATION_LOCK_SQL)
        .bind(account_id)
        .execute(conn)
        .await?;
    Ok(())
}

/// Seed exactly ONE member's allowance for the account's CURRENT period, the
/// moment they join — the mid-period counterpart to [`roll_period`], called
/// from `clerk_provision::ensure_org_membership` on every membership upsert
/// (invite, re-invite, role change).
///
/// # What "current period" means here, and the three ways it resolves
///
/// 1. **An open period already exists** (some other member of this account
///    already holds a `member_allowance_periods` row covering `now()`): its
///    exact `(period_start, period_end)` is reused verbatim, via
///    [`CURRENT_OPEN_PERIOD_SQL`], so every member of an account always
///    shares one window — the same invariant [`roll_period`] already
///    guarantees for a same-call cohort.
/// 2. **No period is open, and NO `member_allowance_periods` row has EVER
///    existed for this account** ([`ANY_PERIOD_EVER_EXISTED_SQL`] is
///    `false`) — this is the account's very FIRST joining member. This is
///    precisely the state an org account is left in by `clerk_provision::
///    provision_billing` at creation: `auto_grant_free_plan` issues the
///    grant against zero members, so `roll_period`'s own `INSERT ... SELECT`
///    wrote no rows at all (see `issuance::issue_grant`'s doc comment), and
///    this branch is reached on exactly the first member ever to join such
///    an org. The window is computed fresh via
///    [`crate::billing::issuance::resolve_initial_period`] — the SAME
///    `now() .. now() + plan.period_length` formula (100-year fallback for a
///    lifetime plan like `free`) a brand-new grant's own first period uses,
///    reused rather than re-typed here (finding 9, review round 2) so the
///    two can never drift apart. Recomputing `now()` here rather than trying
///    to recover the original grant's timestamp is deliberate, not an
///    approximation: this member's join genuinely is the first moment anyone
///    could have consumed anything against this org's grant, so "the period
///    starts now" is the correct answer for when the org's first real
///    allowance period begins.
/// 3. **No period is open, but SOME `member_allowance_periods` row for this
///    account exists** — a LAPSED account: every period this account ever
///    held has already ended (Stripe `past_due`/cancellation, say) and
///    nothing has re-granted it since. This is deliberately NOT case 2:
///    minting an off-cycle `now()`-anchored window for just the one member
///    who happens to join at this moment would hand them a window the
///    eventual real re-grant (whenever it comes) does not know about — the
///    next roll would close/replace it via `close_open_period`/a fresh
///    `roll_period` call, but until then this ONE member would hold a
///    window nobody else on the account shares, and if the re-grant's own
///    period happens to overlap it, the exclusion constraint rejects the
///    legitimate roll instead. Seeding nothing here is the correct
///    default-deny: `Ok(false)`, precisely as the no-active-grant case below
///    already does. The next real roll (`roll_period`, when the account is
///    re-granted) covers this member exactly as it covers every other
///    current member, with no special case needed.
///
/// # Concurrency
///
/// The three-way decision above is taken under a per-account
/// `pg_advisory_xact_lock` ([`lock_period_rotation`]) so two members joining
/// a brand-new account at the same instant cannot both take branch 2 with
/// two different `now()`-anchored windows — see that function's own doc
/// comment for the exact race this closes. As of finding 1 (review round 2
/// of the enforcement-completion PR), the SAME lock is also taken by
/// [`roll_period`]/[`close_open_period`]'s callers (`issue_grant`, both
/// `stripe_webhook` roller handlers), so this decision is now serialized
/// against a concurrent period ROLL too, not merely against another seed —
/// see [`lock_period_rotation`]'s doc comment for the abutting-windows
/// double-grant race that widened scope closes. As defense in depth for a
/// window that diverged BEFORE this lock existed (or through some future
/// path that bypasses it), the final INSERT also treats Postgres's `23P01`
/// exclusion-violation error as a benign already-seeded no-op rather than
/// propagating it — see the "Idempotent" section below.
///
/// # Full allowance, never prorated
///
/// A mid-period joiner is granted the FULL per-member allowance for
/// whatever remains of the current period — never a slice proportional to
/// the days remaining. This is not a simplification; it is the founder's
/// locked decision, already proven at the roll level by
/// `roll_period::pg_tests::a_member_added_mid_period_is_granted_in_full_on_the_next_roll`
/// and restated in the registered plan (§1.7: "mid-period seat add → full
/// allowance immediately... prorating credits too produces numbers nobody
/// can explain"). This function is the OTHER trigger for that same rule —
/// where that test's trigger is `roll_period` re-run after a roster change,
/// this one is triggered by the membership event itself, so the member never
/// has to wait for a period roll to receive anything at all.
///
/// # Idempotent, and never touches another member's row
///
/// The conflict target is `(account_id, user_id, period_start)` — the SAME
/// natural key [`roll_period`] relies on — so a re-invite or role-change
/// upsert for a member who already holds a row for the CURRENT period is a
/// safe no-op: `Ok(false)`, and their `consumed_credits` is untouched.
/// [`SEED_ONE_MEMBER_PERIOD_SQL`] names only the one `user_id` being seeded,
/// so there is no roster read here that could ever reach another member's
/// row, unlike [`roll_period`]'s account-wide `INSERT ... SELECT`.
///
/// **A pre-existing divergent window degrades to noise, not a permanent
/// error.** If this member already holds a row whose window OVERLAPS but
/// does not EQUAL `(period_start, period_end)` — reachable only from state
/// that predates [`lock_period_rotation`]'s serialization, or from any
/// future path that writes this table without going through it — the INSERT hits
/// `0163`'s exclusion constraint (`23P01`) instead of the natural-key
/// `ON CONFLICT`. That is caught here, logged at `tracing::warn!`, and
/// treated as `Ok(false)`: the member already has SOME allowance for this
/// account, which is what actually matters for `enforcement::is_blocked`,
/// and a caller (a webhook handler, the backfill binary) must not be made to
/// fail forever on an already-seeded member just because the two windows
/// don't byte-for-byte agree.
///
/// **Removed-then-re-added within the SAME period is also a safe no-op, and
/// here is why.** Nothing in this codebase ever deletes a
/// `member_allowance_periods` row — `clerk_provision::remove_org_membership`
/// deletes the `account_members` row and tombstones the membership, but
/// leaves every billing row it ever wrote untouched (mirroring this table's
/// own "no FK on `user_id`" design: `0161`'s header already documents that a
/// removed member's spend history must survive and their unused allowance is
/// neither refunded nor redistributed). So a member removed and re-added
/// within one period still has THEIR OLD ROW for that period's
/// `period_start`, and the same conflict target above makes their re-invite
/// a no-op: they come back with whatever they had already consumed, not a
/// fresh allowance — which is the correct anti-abuse direction (leave-and-
/// rejoin must not be a way to refill a spent allowance). A removal and
/// re-add spanning a period ROLL, by contrast, is not a special case at all:
/// the new period has a new `period_start`, nothing conflicts, and this
/// function simply grants them fresh for the new period like any other
/// current member — exactly what would happen to a member who never left.
///
/// # No grant, or no way to resolve a period → `Ok(false)`, silently
///
/// Two situations collapse to the same harmless no-op, and neither is an
/// error: an account with no active grant at all (a personal account, which
/// never calls this function in the first place, or an org whose grant was
/// revoked), and — the case this function exists to future-proof against —
/// a legacy org account that was granted but never flipped before this PR
/// shipped (pre-#651/pre-enforcement-completion) and so still holds zero
/// period rows for anyone. Both read as "nothing to seed" rather than "this
/// broke", because a membership upsert must never fail on account of
/// billing state it does not control — see the call site's own comment for
/// why this is called best-effort.
///
/// # Scoping
///
/// Runs entirely inside ONE account-scoped transaction
/// (`tenancy::scope::begin_account_scoped`) — `grants` and
/// `member_allowance_periods` are both RLS-covered on the control plane
/// (migrations `0150`, `0161`), the same requirement [`roll_period`] and
/// [`close_open_period`] document on their own doc comments.
pub async fn seed_member_period(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    // Step 1 (finding 5, review round 2; widened to cover the rollers too in
    // finding 1, review round 2 of the enforcement-completion PR): serialize
    // the whole reuse-vs-fresh-window decision below, per account, BEFORE
    // reading either `grants` or `member_allowance_periods` — see
    // [`lock_period_rotation`]'s own doc comment both for the
    // concurrent-first-joiner race this originally closed and for the
    // abutting-windows double-grant race against `roll_period`/
    // `close_open_period` it now ALSO closes.
    lock_period_rotation(&mut tx, account_id).await?;

    // ── Frozen-subscription guard (finding 4, billing self-serve PR D review
    // round) ──
    //
    // WHY: this function mints a FULL, immediately-spendable allowance for a
    // member the instant they join (see "Full allowance, never prorated"
    // above) — before `billing::enforcement::is_blocked`'s
    // `subscription_frozen` read-side gate is ever consulted, because that
    // gate only runs on the REQUEST path, not this WRITE path. Without this
    // check, a member joining an account whose subscription is
    // `past_due`/`canceled` (freeze-not-forfeit, plan §1.11) would receive a
    // brand-new `member_allowance_periods` row carrying real, spendable
    // credits — exactly the "purchased pool survives cancellation,
    // UNSPENDABLE until resubscribe" invariant `subscription_frozen` exists
    // to enforce, defeated at the one call site that never asks it. This is
    // a best-effort, silent no-op (`Ok(false)`, `tracing::warn!`) rather than
    // a propagated error, matching this function's own "No grant, or no way
    // to resolve a period -> Ok(false), silently" posture just below: a
    // membership upsert must never fail on account of billing state it does
    // not control.
    //
    // `None` (no `stripe_subscriptions` row at all) is deliberately NOT
    // frozen — the same "missing state is not an ON state" convention
    // `billing::enforcement::subscription_frozen` documents on its own doc
    // comment: an account that never went through Stripe checkout (a
    // `free`-tier personal account, a hand-granted `founder_comp`) has
    // nothing to be frozen FROM, so a mid-period joiner there seeds
    // normally, exactly as before this check existed.
    //
    // `enforcement::status_is_frozen`, not a local `matches!` (billing
    // self-serve PR D review round 1, CodeRabbit finding on
    // `enforcement.rs:450`/`renewal.rs:691-704`): this call already holds
    // the scoped transaction and the per-account advisory lock
    // (`lock_period_rotation`, taken just above), so a second pool
    // connection is not on the table here — only the duplicated predicate
    // is. Deferring to the shared function keeps the "what counts as
    // frozen" decision in exactly one place; see that function's own doc
    // comment for why its two-value denylist is complete over
    // `stripe_subscriptions.status`'s closed three-value domain.
    let subscription_status: Option<String> = sqlx::query_scalar(ACCOUNT_SUBSCRIPTION_STATUS_SQL)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;
    if crate::billing::enforcement::status_is_frozen(subscription_status.as_deref()) {
        tracing::warn!(
            %account_id, %user_id, status = subscription_status.as_deref().unwrap_or("?"),
            "seed_member_period: account's subscription is frozen — skipping the best-effort \
             mid-period seed for this newly-joined member; their allowance will be seeded \
             normally by the next real period roll once the account resubscribes"
        );
        tx.commit().await?;
        return Ok(false);
    }

    let grant: Option<CurrentGrantRow> =
        sqlx::query_as(CURRENT_GRANT_SQL)
            .bind(account_id)
            .bind(crate::billing::MIXED_TIER_PLAN_CODE)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((_grant_id, credits_override, seats, plan_credits, plan_id, _is_team_plan, use_seat_tier_credits)) =
        grant
    else {
        // No active grant at all — nothing to seed. Committing an empty,
        // read-only transaction is harmless and keeps this function's scope
        // handling symmetric with every branch below.
        tx.commit().await?;
        return Ok(false);
    };

    // PR E review round 2, finding 9: the window-resolution reads below used
    // to run AFTER the per-member tier lookup / legacy division, paying that
    // round trip even on the two no-op branches immediately below (no
    // resolvable window at all). Resolving the window FIRST means both
    // no-op paths return before doing any of that work.
    let existing_window: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as(CURRENT_OPEN_PERIOD_SQL)
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await?;

    let window = match existing_window {
        Some(w) => Some(w),
        None => {
            // No OPEN period — but that alone does not mean this is a
            // brand-new account (finding 4, review round 2). Distinguish
            // "never had one" (fresh-window branch, case 2) from "had one,
            // it lapsed" (seed nothing, case 3) — see the doc comment above
            // for why minting an off-cycle window in the lapsed case would
            // double-grant once the account is eventually re-granted.
            let ever_existed: bool = sqlx::query_scalar(ANY_PERIOD_EVER_EXISTED_SQL)
                .bind(account_id)
                .fetch_one(&mut *tx)
                .await?;
            if ever_existed {
                None
            } else {
                Some(crate::billing::issuance::resolve_initial_period(&mut tx, plan_id).await?)
            }
        }
    };

    let Some((period_start, period_end)) = window else {
        // Either a lapsed account (case 3 above) or nothing left to resolve
        // a window from at all — both are "nothing to seed", not an error.
        tx.commit().await?;
        return Ok(false);
    };

    // **PR F: no `grant_seat_tiers`/`grants.seats` reconciliation here any
    // more** — PR E's version (findings 5(A)/9/B) recomputed both from the
    // roster on every mid-period join, for the same "nothing else maintains
    // this table yet" reason the module doc's "`grant_seat_tiers` is a
    // purchase-driven ledger" section explains for [`roll_period`]. That
    // reason no longer holds: `grant_seat_tiers`/`grants.seats` are now
    // maintained by `billing::seat_lifecycle::acquire_seat` (called from
    // `clerk_provision::ensure_org_membership` right after the membership tx
    // this function's own caller already committed) — a mid-period joiner's
    // OWN allowance below still comes from their own `seat_tier_id` join
    // ([`MEMBER_SEAT_TIER_CREDITS_SQL`]), which needs no `grant_seat_tiers`
    // read at all, and the account-level entitlement total
    // (`billing::entitlement::current_plan`) is correct the moment
    // `acquire_seat` commits, independently of this function ever running.
    // A member seeded here whose seat was never purchased (the
    // FOUNDER-CONFIRM case: an org member who existed before the org ever
    // subscribed) correctly shows up as roster-vs-purchased drift via
    // [`RenewalOutcome::seat_mismatch`] at the next roll, rather than this
    // function silently manufacturing a ledger entry nobody actually bought.

    // PR E (billing self-serve launch): a `team` grant has no single
    // per-member figure — see the module doc's "Per-tier allowances for
    // `team` grants" section and `PER_MEMBER_GRANT_SQL`'s own doc comment.
    // [`roll_period`] expresses this as a per-row `LEFT JOIN`; this function
    // seeds exactly ONE member, so it resolves that one member's own tier
    // directly instead.
    let per_member_credits = if use_seat_tier_credits {
        match sqlx::query_scalar::<_, Decimal>(MEMBER_SEAT_TIER_CREDITS_SQL)
            .bind(account_id)
            .bind(user_id)
            .fetch_optional(&mut *tx)
            .await?
        {
            Some(c) => c,
            None => {
                // Should not happen: `account_members_seat_tier_iff_org`
                // (0169) guarantees every org member carries a non-NULL
                // `seat_tier_id`, and a `team` grant exists only for an org
                // account. Reachable only from a genuinely inconsistent row
                // (e.g. a caller that inserted `account_members` and this
                // seed call raced ahead of the tier-assigning trigger's own
                // transaction somehow observing a different snapshot) — this
                // function's caller (a membership upsert) must not fail just
                // because BILLING state is unresolvable, so this degrades to
                // "nothing to seed" rather than propagating an error, exactly
                // like every other branch in this function's "No grant, or
                // no way to resolve a period" section.
                tracing::warn!(
                    account_id = %account_id, user_id = %user_id,
                    "seed_member_period: team grant active but this member has no resolvable \
                     seat_tier_id — skipping the seed rather than granting a wrong (zero) amount"
                );
                tx.commit().await?;
                return Ok(false);
            }
        }
    } else {
        legacy_per_member_credits(credits_override, seats, plan_credits)
    };

    match sqlx::query(SEED_ONE_MEMBER_PERIOD_SQL)
        .bind(account_id)
        .bind(user_id)
        .bind(period_start)
        .bind(period_end)
        .bind(per_member_credits)
        .execute(&mut *tx)
        .await
    {
        Ok(res) => {
            tx.commit().await?;
            Ok(res.rows_affected() > 0)
        }
        Err(e)
            if e.as_database_error()
                .and_then(|d| d.code())
                .map(|c| c == "23P01")
                .unwrap_or(false) =>
        {
            // Defense-in-depth (finding 5, review round 2) — see this
            // function's own "Idempotent" section for the exact scenario.
            tracing::warn!(
                error = %e, account_id = %account_id, user_id = %user_id,
                "seed_member_period: hit 0163's exclusion constraint instead of the natural-key \
                 ON CONFLICT no-op — a pre-existing divergent allowance window for this member \
                 already covers this instant; degrading to a benign no-op rather than a \
                 permanent per-call error"
            );
            tx.rollback().await.ok();
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// What one [`close_open_period`] call did, reported rather than interpreted
/// here — same discipline as [`RenewalOutcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClosePeriodOutcome {
    /// Rows whose `period_end` was pulled back to `boundary` — see
    /// [`CLOSE_OPEN_PERIOD_SQL`]. Zero is the normal case: most calls happen
    /// when nothing was still open.
    pub rows_shrunk: u64,
    /// Rows deleted outright because their `period_start` was strictly AFTER
    /// `boundary` and could not be truncated without moving it — see
    /// [`DELETE_UNSTARTED_OPEN_PERIOD_SQL`].
    pub rows_deleted: u64,
}

impl ClosePeriodOutcome {
    /// Whether this call actually touched anything, for a caller that only
    /// wants to log the interesting case.
    pub fn changed(&self) -> bool {
        self.rows_shrunk > 0 || self.rows_deleted > 0
    }

    /// A short machine-greppable summary, matching [`RenewalOutcome::summary`]'s
    /// style for whatever ledger the caller writes it into
    /// (`stripe_webhook_events.outcome`, an operator's stderr, ...).
    pub fn summary(&self) -> String {
        format!(
            "closed shrunk={} deleted={}",
            self.rows_shrunk, self.rows_deleted
        )
    }
}

/// Closes any of an account's allowance periods that are still open as of
/// `boundary`, so a subsequent [`roll_period`] can seed a new period starting
/// at `boundary` without colliding with `0163`'s
/// `member_allowance_periods_no_overlap` exclusion constraint.
///
/// # Why this has to exist at all
///
/// `0163` turned an overlap between two allowance periods from a silently
/// tolerated data-quality bug into a hard INSERT error — see that
/// migration's header, and the CodeRabbit Major on PR #639 it closes. That is
/// strictly better than the status quo (loud beats silent), but it is not
/// itself a fix for either caller that seeds a new period: `grant_plan`
/// re-granting an account that already holds an open period, and
/// `stripe_webhook::apply_invoice_paid` receiving a SECOND subscription
/// invoice — a mid-period proration, say — whose period does not start where
/// the first one's ends. Both used to double-insert silently when an old
/// period was still open; after `0163` both instead fail with a constraint
/// violation, which for the webhook means a redelivered-forever 500. This
/// function is what makes "period rolls -> allowances reset" (`0161`'s
/// header) true rather than aspirational: it closes the old period FIRST, in
/// the same transaction the new one is seeded in, so the two writes can never
/// be separated by a commit boundary and the exclusion constraint never has
/// anything left to reject.
///
/// # Never extends, only shrinks
///
/// A row whose `period_end` is already at or before `boundary` matches
/// neither of this function's two statements and is left byte-for-byte
/// alone. This can only ever pull a `period_end` backward or delete a row
/// outright; it can never push one forward, even when called with a
/// `boundary` earlier than the caller intended.
///
/// # The two cases — and the one case that is neither
///
/// See [`CLOSE_OPEN_PERIOD_SQL`] for the ordinary case (`period_start <
/// boundary`, a period that started before `boundary` and is still open at
/// it — truncated in place) and [`DELETE_UNSTARTED_OPEN_PERIOD_SQL`] for the
/// degenerate one (`period_start > boundary`, strictly — a period that has
/// not started yet as of `boundary`, deleted because `0161`'s CHECK makes
/// truncating it in place impossible without also rewriting when it
/// started).
///
/// A row whose `period_start` EQUALS `boundary` exactly matches NEITHER
/// statement and is left completely alone — see
/// [`DELETE_UNSTARTED_OPEN_PERIOD_SQL`]'s doc comment for why that boundary
/// is deliberately open rather than closed: it is the one case measured to
/// be a correctness bug rather than a matter of taste.
///
/// # Same contract as [`roll_period`]
///
/// Takes a connection rather than a pool for the identical reason: the caller
/// MUST run this in the SAME transaction as the new period's INSERT, or a
/// commit boundary between the two reopens the exact race `0163` closes. The
/// connection must already be account-scoped
/// (`tenancy::scope::begin_account_scoped`) — `member_allowance_periods`
/// carries the same `account_isolation` RLS policy `roll_period` already
/// depends on. **Does not fail open**: an error here must abort the whole
/// seed rather than let the caller insert a new period over one this
/// function failed to close, which is exactly the overlap `0163` exists to
/// prevent.
pub async fn close_open_period(
    conn: &mut PgConnection,
    account_id: Uuid,
    boundary: chrono::DateTime<chrono::Utc>,
) -> Result<ClosePeriodOutcome, sqlx::Error> {
    // Deleted before shrunk, though the two are mutually exclusive by
    // `WHERE` clause (`period_start > $2` vs `period_start < $2`, with `= $2`
    // deliberately matching neither) and so the order genuinely does not
    // matter — done in this order only because "remove what cannot be
    // truncated, then truncate what remains" reads naturally.
    let rows_deleted = sqlx::query(DELETE_UNSTARTED_OPEN_PERIOD_SQL)
        .bind(account_id)
        .bind(boundary)
        .execute(&mut *conn)
        .await?
        .rows_affected();

    let rows_shrunk = sqlx::query(CLOSE_OPEN_PERIOD_SQL)
        .bind(account_id)
        .bind(boundary)
        .execute(&mut *conn)
        .await?
        .rows_affected();

    Ok(ClosePeriodOutcome {
        rows_shrunk,
        rows_deleted,
    })
}

// ── Pure unit tests (no Postgres) ────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(members: i64, seats: Option<i64>, credits: Option<u32>) -> RenewalOutcome {
        RenewalOutcome {
            rows_granted: members as u64,
            members,
            seats,
            per_member_credits: credits.map(Decimal::from),
            total_credits_granted: credits.map(|c| Decimal::from(c) * Decimal::from(members.max(0))),
            use_seat_tier_credits: false,
            is_team_plan: false,
        }
    }

    /// **CS-435 (D1/D7, org-onboarding plan §3 item 7) — one-directional.**
    /// Before CS-435 the founder's model said members and seats could never
    /// diverge, so BOTH directions were violations. `billing::seat_lifecycle::
    /// acquire_seat` now consumes a pre-purchased slot before it ever calls
    /// Stripe, which makes "more seats (purchased) than members (roster)"
    /// the pool's own intended, healthy steady state — an admin's
    /// `routes::org_slots` "buy N slots" ahead of anyone joining. Only "more
    /// members than seats" — the roster outgrew what was purchased, which
    /// cannot happen through this crate's own writers — remains a real
    /// signal. See [`RenewalOutcome::seat_mismatch`]'s own doc comment for
    /// the full argument.
    #[test]
    fn seat_mismatch_fires_only_when_members_outgrow_seats() {
        assert!(
            !outcome(4, Some(4), Some(3000)).seat_mismatch(),
            "equal is a healthy state"
        );
        assert!(
            outcome(5, Some(4), Some(3000)).seat_mismatch(),
            "more members than seats paid for — a real drift signal"
        );
        assert!(
            !outcome(3, Some(4), Some(3000)).seat_mismatch(),
            "more seats than members — a healthy pre-bought pool (CS-435), not drift"
        );
    }

    /// With no grant there is no seat count to disagree with, so reporting a
    /// mismatch would be a second alert about one fact — and would fire on
    /// EVERY unprovisioned account, burying the real signal.
    #[test]
    fn no_active_plan_suppresses_the_seat_mismatch_signal() {
        let none = outcome(3, None, None);
        assert!(none.no_active_plan());
        assert!(!none.seat_mismatch());
        assert!(
            none.summary().starts_with("no_active_plan"),
            "{}",
            none.summary()
        );
    }

    /// The summary is what lands in `stripe_webhook_events.outcome`, so it has
    /// to carry the numbers a support question would actually ask for.
    #[test]
    fn summary_carries_the_counts_it_claims_to() {
        let s = outcome(4, Some(4), Some(3000)).summary();
        for expected in [
            "renewed",
            "rows=4",
            "members=4",
            "seats=4",
            "per_member=3000",
        ] {
            assert!(s.contains(expected), "{expected:?} missing from {s:?}");
        }
    }

    /// **PR E review round 2, finding 1.** A tiered `team` roll must never
    /// print `per_member=0` — the LEGACY fallback scalar is `0` for every
    /// such roll (`team`'s `plans.credits = 0` by design), and that string
    /// used to be a reliable zero-credit-BUG signal in
    /// `stripe_webhook_events.outcome`. A real tiered roll now reads
    /// `per_member=tiered` plus the actual aggregate, so the two cases stay
    /// distinguishable in the audit trail.
    #[test]
    fn team_roll_summary_reports_tiered_not_a_misleading_zero() {
        let out = RenewalOutcome {
            rows_granted: 3,
            members: 3,
            seats: Some(3),
            per_member_credits: Some(Decimal::ZERO),
            total_credits_granted: Some(Decimal::from(21_000)),
            use_seat_tier_credits: true,
            is_team_plan: true,
        };
        let s = out.summary();
        assert!(s.contains("per_member=tiered"), "{s:?}");
        assert!(s.contains("total_credits=21000"), "{s:?}");
        assert!(
            !s.contains("per_member=0"),
            "must never resemble the old zero-credit-bug signal: {s:?}"
        );
        assert!(s.contains("members=3"), "{s:?}");
    }

    /// The flat-plan/`team_standard`/`team_premium` summary shape must stay
    /// byte-for-byte the same as before finding 1's fix — existing log
    /// consumers and tests depend on it.
    #[test]
    fn non_tiered_roll_summary_format_is_unchanged() {
        let s = outcome(4, Some(4), Some(3000)).summary();
        assert_eq!(s, "renewed rows=4 members=4 seats=4 per_member=3000");
    }

    /// **PR F (supersedes PR E review round 2, finding 7's suppression —
    /// see the module doc's "The `team` seat_mismatch signal is
    /// un-suppressed" section) — direction flipped by CS-435.**
    /// `grants.seats` for a `team` grant is a purchase ledger total,
    /// independent of the roster, so a `team` roll where they disagree can
    /// still be real, actionable drift — but ONLY in the "more members than
    /// seats" direction (`billing::seat_lifecycle::acquire_seat` cannot
    /// stamp a member purchased without either consuming a pre-bought slot
    /// or pushing Stripe first). A team grant with 3 purchased seats and
    /// ZERO members is CS-435's own headline case — `routes::org_slots`
    /// "buy N slots" landed before anyone joined — and must NOT fire.
    #[test]
    fn seat_mismatch_does_not_fire_for_a_team_plan_pre_bought_pool_with_no_members_yet() {
        let out = RenewalOutcome {
            rows_granted: 0,
            members: 0,
            seats: Some(3),
            per_member_credits: Some(Decimal::ZERO),
            total_credits_granted: Some(Decimal::ZERO),
            use_seat_tier_credits: true,
            is_team_plan: true,
        };
        assert!(
            !out.seat_mismatch(),
            "a team grant's purchased seat count (3) with nobody on the roster yet (0) is CS-435's \
             own healthy pre-bought-pool state, not drift"
        );
    }

    /// The direction CS-435 leaves untouched: a team grant whose roster has
    /// somehow outgrown what was purchased is still real drift — this
    /// cannot happen through this crate's own writers (every acquire either
    /// consumes pool capacity or pushes Stripe first), so it means
    /// something external moved (a Stripe Dashboard edit, or the crash
    /// window `billing::seat_lifecycle`'s own "Stripe push BEFORE the DB
    /// write, always" section names).
    #[test]
    fn seat_mismatch_still_fires_when_members_outgrow_a_team_plans_purchased_seats() {
        let out = RenewalOutcome {
            rows_granted: 5,
            members: 5,
            seats: Some(3),
            per_member_credits: Some(Decimal::ZERO),
            total_credits_granted: Some(Decimal::ZERO),
            use_seat_tier_credits: true,
            is_team_plan: true,
        };
        assert!(
            out.seat_mismatch(),
            "5 members against 3 purchased seats is genuine drift, not a healthy pool"
        );
    }
}

/// Execute guarded CONTROL fixture SQL using a real account/financial entry.
/// Kept outside converted route modules so their scope-regression guard stays strict.
#[cfg(test)]
pub(crate) async fn execute_financial_fixture<'q>(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    lock_period_rotation(&mut tx, account_id).await?;
    match query.execute(&mut *tx).await {
        Ok(result) => {
            tx.commit().await?;
            Ok(result)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

// ── Live-Postgres tests ──────────────────────────────────────────────────────

/// The period roll, proven against real Postgres.
///
/// # Why every one of these is a live-DB test rather than a unit test
///
/// There is nothing to unit-test here. The entire behaviour is SQL: which
/// rows the `INSERT ... SELECT` produces, what `ON CONFLICT DO NOTHING`
/// swallows, and what `COALESCE(credits_override / seats, credits)` evaluates
/// to in NUMERIC. A mock would be a re-statement of the code under test, which
/// this repo has already been bitten by once — see `billing::entitlement`'s
/// module doc for the review finding that produced that rule.
///
/// # Gated on `RELINFER_TEST_DATABASE_URL`
///
/// A run with no DSN SKIPS, and a skipped suite reports `ok. 0 passed` — which
/// is indistinguishable from a passing one on a green check mark. The CI step
/// therefore asserts `test -n "$RELINFER_TEST_DATABASE_URL"` AND greps for the
/// zero-test result, per `scripts/check_pg_suite_coverage.sh`. Neither guard is
/// optional: this suite is arithmetic that decides what customers are allowed
/// to spend.
///
/// Run with:
///   RELINFER_TEST_DATABASE_URL=postgres://localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib "billing::renewal::" -- --test-threads=1
#[cfg(test)]
mod pg_tests {
    use super::*;
    use sqlx::PgPool;

    async fn owner_fixture_transaction(
        conn: &mut PgConnection,
        account_id: Uuid,
    ) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, sqlx::Error> {
        use sqlx::Connection;
        let mut tx = conn.begin().await?;
        sqlx::query(r#"SELECT set_config('search_path', '"$user", public', true), set_config('app.account_id', $1, true)"#)
            .bind(account_id.to_string()).execute(&mut *tx).await?;
        lock_period_rotation(&mut tx, account_id).await?;
        Ok(tx)
    }

    async fn roll_period_fixture(
        conn: &mut PgConnection,
        account_id: Uuid,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Result<RenewalOutcome, sqlx::Error> {
        let mut tx = owner_fixture_transaction(conn, account_id).await?;
        match super::roll_period(&mut tx, account_id, start, end).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    async fn close_open_period_fixture(
        conn: &mut PgConnection,
        account_id: Uuid,
        boundary: chrono::DateTime<chrono::Utc>,
    ) -> Result<ClosePeriodOutcome, sqlx::Error> {
        let mut tx = owner_fixture_transaction(conn, account_id).await?;
        match super::close_open_period(&mut tx, account_id, boundary).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    /// Any period, as long as it is stable across a test. `now()`-relative
    /// bounds are deliberately avoided EXCEPT where a test needs the row to be
    /// the "current" one for `is_blocked` — see `enforcement`'s own gate test.
    fn period() -> (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) {
        (
            chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            chrono::DateTime::from_timestamp(1_702_678_400, 0).unwrap(),
        )
    }

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    /// An account plus `members` `account_members` rows. Returns the account id
    /// and every `users` row this fixture created — which, for `members >= 1`,
    /// is exactly the roster that holds an `account_members` row (the owner
    /// is one OF the members rather than an extra person: the roll grants
    /// per `account_members` row, and an owner who held no membership row
    /// would silently receive no allowance — a distinction worth encoding in
    /// the fixture rather than discovering in production). For `members ==
    /// 0`, `owner_user_id` still needs a `users` row to reference even
    /// though nobody is a member yet, so the returned list contains that one
    /// unmembered owner and nothing else — included specifically so
    /// [`cleanup`] can still find and delete it.
    async fn seed_account(pool: &PgPool, members: usize) -> (Uuid, Vec<Uuid>) {
        let mut users = Vec::new();
        for i in 0..members.max(1) {
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
                .bind(id)
                .bind(format!("renewal-{}-{i}@test.example", id.simple()))
                .execute(pool)
                .await
                .expect("create test user");
            users.push(id);
        }
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(users[0])
            .execute(pool)
            .await
            .expect("create test account");

        let mut member_ids = Vec::new();
        for (i, user_id) in users.iter().take(members).enumerate() {
            sqlx::query(
                "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)",
            )
            .bind(account_id)
            .bind(user_id)
            .bind(if i == 0 { "owner" } else { "member" })
            .execute(pool)
            .await
            .expect("create account_members row");
            member_ids.push(*user_id);
        }

        // `members.max(1)` above always creates one `users` row to serve as
        // `accounts.owner_user_id`, even when `members == 0` (a caller
        // simulating `ensure_org_account`'s zero-member state at creation).
        // The `.take(members)` loop above only pushes a user into
        // `member_ids` when it also got an `account_members` row, so for
        // `members == 0` that owner is created but never returned — and
        // `cleanup` only ever deletes the ids it is handed, so it was never
        // reachable at cleanup time either. That is the exact leak
        // `cleanup`'s own doc comment (below) says corrupted an unrelated
        // suite's anomaly count once. Every `users` row this fixture creates
        // must come back out here, membership row or not, so cleanup can
        // always find it.
        for user_id in &users {
            if !member_ids.contains(user_id) {
                member_ids.push(*user_id);
            }
        }
        (account_id, member_ids)
    }

    /// Grants `plan_code` to the account. `seats` must be `Some` exactly when
    /// the plan is seat-priced — `0156`'s `grants_seats_iff_seat_priced` CHECK
    /// enforces that, so this helper reads the real flag rather than assuming.
    async fn grant(
        pool: &PgPool,
        account_id: Uuid,
        plan_code: &str,
        seats: Option<i32>,
        credits_override: Option<Decimal>,
    ) {
        let (plan_id, is_seat_priced): (Uuid, bool) =
            sqlx::query_as("SELECT id, is_seat_priced FROM plans WHERE code = $1")
                .bind(plan_code)
                .fetch_one(pool)
                .await
                .unwrap_or_else(|e| panic!("plan {plan_code:?} must be seeded (0153/0155): {e}"));

        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced, credits_override) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(seats)
        .bind(is_seat_priced)
        .bind(credits_override))
        .await
        .expect("insert grant");
    }

    /// Every allowance row for one period, as `(user_id, granted, cap)`.
    async fn rows(
        pool: &PgPool,
        account_id: Uuid,
        period_start: chrono::DateTime<chrono::Utc>,
    ) -> Vec<(Uuid, Decimal, Option<Decimal>)> {
        sqlx::query_as(
            "SELECT user_id, granted_credits, cap_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND period_start = $2 ORDER BY user_id",
        )
        .bind(account_id)
        .bind(period_start)
        .fetch_all(pool)
        .await
        .expect("read member_allowance_periods")
    }

    async fn total_rows(pool: &PgPool, account_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .expect("count member_allowance_periods")
    }

    /// `accounts` cascades to `account_members`, `grants` and
    /// `member_allowance_periods`, so deleting it leaves the shared test
    /// database as it was found. That matters more than usual here: these
    /// suites run against ONE database in CI order, and a leaked row in a
    /// billing table has already corrupted an unrelated suite's anomaly count
    /// once in this build.
    async fn cleanup(pool: &PgPool, account_id: Uuid, users: &[Uuid]) {
        let _ = execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id),
        )
            .await;
        for u in users {
            let _ = sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(u)
                .execute(pool)
                .await;
        }
    }

    // ── PR E fixtures: `team`-plan ORG accounts with per-member seat tiers ──

    /// A `team`-plan ORG account (`clerk_org_id` set, so `0169`'s trigger
    /// flips `accounts.is_org_account`) with one member per entry of `tiers`
    /// (each `"standard"` or `"premium"`), each member's `account_members`
    /// row carrying that tier's `seat_tier_id` — the mixed-seat counterpart
    /// to [`seed_account`]'s flat-plan shape.
    ///
    /// Every `users` row this creates is returned, membership row or not —
    /// same "cleanup must always be able to find it" discipline
    /// [`seed_account`]'s own doc comment explains for its `members == 0`
    /// case (an empty `tiers` slice hits that identical case here: the owner
    /// gets a `users`/`accounts` row but no `account_members` row).
    async fn seed_team_org(pool: &PgPool, tiers: &[&str]) -> (Uuid, Vec<Uuid>) {
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("team-owner-{}@test.example", owner.simple()))
            .execute(pool)
            .await
            .expect("create owner user");
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
            .bind(account_id)
            .bind(owner)
            .bind(format!("org_{}", account_id.simple()))
            .execute(pool)
            .await
            .expect("create org account");

        let mut all_users = vec![owner];
        for (i, tier_code) in tiers.iter().enumerate() {
            let user_id = if i == 0 {
                owner
            } else {
                let user_id = Uuid::new_v4();
                sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
                    .bind(user_id)
                    .bind(format!("team-member-{}@test.example", user_id.simple()))
                    .execute(pool)
                    .await
                    .expect("create member user");
                all_users.push(user_id);
                user_id
            };
            let seat_tier_id: Uuid =
                sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active = true")
                    .bind(tier_code)
                    .fetch_one(pool)
                    .await
                    .unwrap_or_else(|e| panic!("seat_tiers {tier_code:?} must be seeded (0165): {e}"));
            sqlx::query(
                "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(account_id)
            .bind(user_id)
            .bind(if i == 0 { "owner" } else { "member" })
            .bind(seat_tier_id)
            .execute(pool)
            .await
            .expect("create tiered account_members row");
        }
        (account_id, all_users)
    }

    /// Grants the `team` plan to `account_id` directly (bypassing
    /// `billing::issuance::issue_grant`, matching this module's own [`grant`]
    /// helper's shape), returning the new grant's id. `seats` is whatever the
    /// caller wants the PRE-roll value to be — [`roll_period`]'s
    /// `grant_seat_tiers` reconciliation is exactly what several of these
    /// tests exist to prove overwrites a stale value, so this deliberately
    /// does NOT try to compute a "correct" one.
    async fn grant_team(
        pool: &PgPool,
        account_id: Uuid,
        seats: i32,
        credits_override: Option<Decimal>,
    ) -> Uuid {
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
            .fetch_one(pool)
            .await
            .expect("team plan must be seeded (0167)");
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .expect("begin team grant fixture");
        lock_period_rotation(&mut tx, account_id)
            .await
            .expect("enter team grant fixture");
        let grant_id = sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced, credits_override) \
             VALUES ($1, $2, $3, true, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(seats)
        .bind(credits_override)
        .fetch_one(&mut *tx)
        .await
        .expect("insert team grant");
        tx.commit().await.expect("commit team grant fixture");
        grant_id
    }

    /// **PR F.** Directly seeds `grant_seat_tiers` rows for `grant_id` — the
    /// purchase ledger `billing::seat_lifecycle::acquire_seat`,
    /// `routes::billing_seats` and `routes::org_slots` maintain in
    /// production — bypassing all of them, matching this file's own [`grant`]/
    /// [`grant_team`] helpers' shape ("insert the row a real writer would
    /// eventually produce, don't call the real writer"). Does NOT touch
    /// `grants.seats`; callers that need it to agree with the sum seeded
    /// here pass it to [`grant_team`] themselves.
    async fn seed_purchased_tiers(pool: &PgPool,
        account_id: Uuid, grant_id: Uuid, tiers: &[(&str, i32)]) {
        for (code, seats) in tiers {
            let seat_tier_id: Uuid =
                sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active = true")
                    .bind(code)
                    .fetch_one(pool)
                    .await
                    .unwrap_or_else(|e| panic!("seat_tiers {code:?} must be seeded (0165): {e}"));
            execute_financial_fixture(pool, account_id, sqlx::query(
                "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)",
            )
            .bind(grant_id)
            .bind(seat_tier_id)
            .bind(seats))
            .await
            .expect("seed grant_seat_tiers");
        }
    }

    /// The base case: four seats of `team_standard` is four rows of 3,000 —
    /// NOT one row of 12,000, and not four rows of 12,000.
    #[tokio::test]
    async fn grants_one_row_per_member_at_the_per_seat_allowance() {
        let Some(pool) = pg_test_pool("grants_one_row_per_member_at_the_per_seat_allowance").await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 4).await;
        grant(&pool, account_id, "team_standard", Some(4), None).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        assert_eq!(outcome.rows_granted, 4, "one row per member");
        assert_eq!(outcome.members, 4);
        assert_eq!(outcome.seats, Some(4));
        assert_eq!(outcome.per_member_credits, Some(Decimal::from(3000)));
        assert!(!outcome.no_active_plan());
        assert!(!outcome.seat_mismatch());

        let got = rows(&pool, account_id, start).await;
        assert_eq!(got.len(), 4);
        for (user_id, granted, cap) in &got {
            assert_eq!(*granted, Decimal::from(3000), "member {user_id} allowance");
            assert!(
                cap.is_none(),
                "cap_credits must stay NULL so 0161's 1x policy default applies; a literal \
                 here would freeze today's policy into this row forever"
            );
        }
        let mut granted_users: Vec<Uuid> = got.iter().map(|(u, _, _)| *u).collect();
        granted_users.sort();
        let mut expected = members.clone();
        expected.sort();
        assert_eq!(granted_users, expected, "exactly the roster, nobody else");

        cleanup(&pool, account_id, &members).await;
    }

    /// Stripe's delivery contract is at-least-once, so this is the NORMAL
    /// path. A second delivery of the same invoice must add nothing and change
    /// nothing.
    #[tokio::test]
    async fn a_redelivered_invoice_grants_nothing_a_second_time() {
        let Some(pool) = pg_test_pool("a_redelivered_invoice_grants_nothing_a_second_time").await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 3).await;
        grant(&pool, account_id, "team_standard", Some(3), None).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let first = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("first roll");
        let second = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("second roll");
        drop(conn);

        assert_eq!(first.rows_granted, 3);
        assert_eq!(second.rows_granted, 0, "redelivery must insert nothing");
        assert_eq!(
            total_rows(&pool, account_id).await,
            3,
            "no duplicate allowances"
        );
        for (_, granted, _) in rows(&pool, account_id, start).await {
            assert_eq!(
                granted,
                Decimal::from(3000),
                "allowance must not be doubled"
            );
        }

        cleanup(&pool, account_id, &members).await;
    }

    /// The other half of idempotency: a genuinely NEW period must still grant.
    /// A too-broad conflict target would make month two a silent no-op — the
    /// same "renewal that never renews" this whole phase exists to fix.
    #[tokio::test]
    async fn the_next_period_grants_again() {
        let Some(pool) = pg_test_pool("the_next_period_grants_again").await else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 2).await;
        grant(&pool, account_id, "team_standard", Some(2), None).await;
        let (start, end) = period();
        let next_start = end;
        let next_end = end + chrono::Duration::days(31);

        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("month one");
        let month_two = roll_period_fixture(&mut conn, account_id, next_start, next_end)
            .await
            .expect("month two");
        drop(conn);

        assert_eq!(month_two.rows_granted, 2, "a new period is a new grant");
        assert_eq!(
            total_rows(&pool, account_id).await,
            4,
            "both periods retained"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// `credits_override` is an absolute ACCOUNT total, so the per-member share
    /// is the override DIVIDED by seats — never the override itself, which
    /// would hand every member the whole account's allowance.
    ///
    /// The second assertion is the one that matters most: it checks this
    /// module's per-member formula against the PRODUCTION account-total query
    /// in `billing::entitlement`, so the two can never drift apart. That
    /// invariant — `per_member x seats == the account total` — is what makes
    /// the split-bucket model add up.
    #[tokio::test]
    async fn an_override_is_divided_across_seats_and_still_sums_to_the_account_total() {
        let Some(pool) =
            pg_test_pool("an_override_is_divided_across_seats_and_still_sums_to_the_account_total")
                .await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 4).await;
        grant(
            &pool,
            account_id,
            "team_standard",
            Some(4),
            Some(Decimal::from(7000)),
        )
        .await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        assert_eq!(
            outcome.per_member_credits,
            Some(Decimal::from(1750)),
            "7,000 across 4 seats is 1,750 each, not 7,000 each"
        );

        let entitlement = crate::billing::entitlement::current_plan(&pool, account_id)
            .await
            .expect("entitlement query")
            .expect("a non-revoked grant exists");
        let per_member: f64 = outcome
            .per_member_credits
            .unwrap()
            .to_string()
            .parse()
            .unwrap();
        assert_eq!(
            per_member * outcome.seats.unwrap() as f64,
            entitlement.credits,
            "per-member x seats must equal the account total the usage screen renders"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// A flat plan carries no seat count, and `COALESCE(g.seats, 1)` must make
    /// the single code path correct for it — 1,500 to the one member, not
    /// 1,500 divided by anything.
    #[tokio::test]
    async fn a_flat_plan_grants_its_full_allowance() {
        let Some(pool) = pg_test_pool("a_flat_plan_grants_its_full_allowance").await else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 1).await;
        grant(&pool, account_id, "pro", None, None).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        assert_eq!(outcome.per_member_credits, Some(Decimal::from(1500)));
        assert_eq!(outcome.seats, Some(1), "a flat plan reads as one seat");
        assert_eq!(outcome.rows_granted, 1);
        assert!(!outcome.seat_mismatch(), "one member, one implied seat");

        cleanup(&pool, account_id, &members).await;
    }

    /// A paid invoice against an account holding no plan grant. Nothing can be
    /// computed, so nothing is written — and the caller is told, because a
    /// customer whose invoice was paid has an unmet expectation from that
    /// moment and no amount of redelivery fixes a provisioning gap.
    #[tokio::test]
    async fn an_account_with_no_active_grant_writes_nothing_and_says_so() {
        let Some(pool) =
            pg_test_pool("an_account_with_no_active_grant_writes_nothing_and_says_so").await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 2).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        assert!(outcome.no_active_plan(), "must report the provisioning gap");
        assert_eq!(outcome.rows_granted, 0);
        assert_eq!(outcome.seats, None);
        assert_eq!(total_rows(&pool, account_id).await, 0, "nothing written");
        assert!(
            !outcome.seat_mismatch(),
            "no seat count exists to disagree with — reporting a mismatch here would be a \
             second alert about one fact"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// A REVOKED grant is not an active one. Without the `revoked_at IS NULL`
    /// filter a cancelled plan would keep renewing forever, which is the
    /// costliest possible direction for this bug to fail in.
    #[tokio::test]
    async fn a_revoked_grant_does_not_renew() {
        let Some(pool) = pg_test_pool("a_revoked_grant_does_not_renew").await else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 2).await;
        grant(&pool, account_id, "team_standard", Some(2), None).await;
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query("UPDATE grants SET revoked_at = now() WHERE account_id = $1")
            .bind(account_id),
        )
            .await
            .expect("revoke");
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        assert!(outcome.no_active_plan(), "a revoked grant must not renew");
        assert_eq!(total_rows(&pool, account_id).await, 0);

        cleanup(&pool, account_id, &members).await;
    }

    /// More members than purchased seats cannot legitimately happen through
    /// this crate's own writers (CS-435: `acquire_seat` either consumes pool
    /// capacity or pushes Stripe first) — a real drift signal, in the
    /// direction [`RenewalOutcome::seat_mismatch`] still reports. EVERY
    /// member is still granted regardless (nobody is silently starved), and
    /// the divergence is reported so the caller can alert on it.
    #[tokio::test]
    async fn a_seat_mismatch_still_grants_every_member_and_is_reported() {
        let Some(pool) =
            pg_test_pool("a_seat_mismatch_still_grants_every_member_and_is_reported").await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 5).await;
        // Five people on the roster, four seats paid for.
        grant(&pool, account_id, "team_standard", Some(4), None).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        assert!(outcome.seat_mismatch(), "the divergence must be reported");
        assert_eq!(outcome.members, 5);
        assert_eq!(outcome.seats, Some(4));
        assert_eq!(
            outcome.rows_granted, 5,
            "every member is granted — starving whoever sorted last would be the same \
             silent-wrong-number failure as the seat bug fixed in #630"
        );
        assert!(
            outcome.summary().contains("members=5"),
            "summary carries the counts"
        );
        assert!(outcome.summary().contains("seats=4"));

        cleanup(&pool, account_id, &members).await;
    }

    /// A member added AFTER the period rolled receives the FULL current-period
    /// allowance the next time the roll runs — never a prorated slice.
    ///
    /// That is the founder's locked decision ("mid-period seat add → full
    /// allowance immediately": Stripe already prorates the money, and prorating
    /// credits too produces numbers nobody can explain). It falls out of the
    /// conflict target being `(account, member, period_start)`: the members
    /// already granted conflict and are skipped, the new one does not.
    ///
    /// Asserted here so the property is a recorded decision rather than an
    /// accident of the query. The DELIBERATE trigger for this is Phase 4b's
    /// `customer.subscription.updated`; this test proves the underlying roll
    /// already behaves correctly when it arrives.
    #[tokio::test]
    async fn a_member_added_mid_period_is_granted_in_full_on_the_next_roll() {
        let Some(pool) =
            pg_test_pool("a_member_added_mid_period_is_granted_in_full_on_the_next_roll").await
        else {
            return;
        };
        let (account_id, mut members) = seed_account(&pool, 2).await;
        grant(&pool, account_id, "team_standard", Some(2), None).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");

        let latecomer = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(latecomer)
            .bind(format!("late-{}@test.example", latecomer.simple()))
            .execute(&pool)
            .await
            .expect("create latecomer");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(latecomer)
        .execute(&pool)
        .await
        .expect("add latecomer");
        members.push(latecomer);

        let again = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("re-roll");
        drop(conn);

        assert_eq!(
            again.rows_granted, 1,
            "only the new member is granted — the two already holding an allowance for this \
             period conflict and are skipped"
        );
        assert_eq!(again.members, 3);

        let granted = rows(&pool, account_id, start).await;
        assert_eq!(granted.len(), 3);
        let late = granted
            .iter()
            .find(|(u, _, _)| *u == latecomer)
            .expect("the new member holds an allowance row");
        assert_eq!(
            late.1,
            Decimal::from(3000),
            "the FULL per-seat allowance, never a prorated slice"
        );

        cleanup(&pool, account_id, &members).await;
    }

    // ── `0163`'s exclusion constraint, proven directly ──────────────────────

    /// The whole point of migration `0163`, proven at the database, not at
    /// application code: an overlapping second period for the same member is
    /// now REJECTED by Postgres itself.
    ///
    /// **Negative control.** Run this against a database still at `v162`
    /// (0163 not yet applied) and the second `INSERT` SUCCEEDS — the
    /// `.expect_err(...)` panics with "called `Result::unwrap_err()` on an
    /// `Ok` value". Watched fail exactly that way before applying `0163` to
    /// `cs_phase4b_test`.
    #[tokio::test]
    async fn an_overlapping_insert_is_rejected_by_the_database() {
        let Some(pool) = pg_test_pool("an_overlapping_insert_is_rejected_by_the_database").await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 1).await;
        let member = members[0];
        let base = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member)
        .bind(base)
        .bind(base + chrono::Duration::days(30))
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed the first period");

        // Starts 10 days into the first period, well before it ends: a real
        // overlap, not an abutment.
        let overlapping_start = base + chrono::Duration::days(10);
        let err = execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member)
        .bind(overlapping_start)
        .bind(overlapping_start + chrono::Duration::days(30))
        .bind(Decimal::from(3000)),
        )
        .await
        .expect_err(
            "an overlapping period must be rejected by the exclusion constraint, not merely \
             discouraged by application code",
        );

        let db_err = err
            .as_database_error()
            .expect("a database-level error, not a driver error");
        assert_eq!(
            db_err.code().as_deref(),
            Some("23P01"),
            "must be Postgres's exclusion_violation (23P01), not some other error: {db_err}"
        );
        assert_eq!(
            db_err.constraint(),
            Some("member_allowance_periods_no_overlap"),
            "must be 0163's named exclusion constraint specifically: {db_err}"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// `tstzrange`'s default `[)` bound makes an ABUTTING pair — one period's
    /// `period_end` exactly equal to the next one's `period_start` — NOT an
    /// overlap, so it must be ACCEPTED. `the_next_period_grants_again` (above)
    /// already proves this at the `roll_period` level; this proves it one
    /// layer down, directly against the constraint, so a future change to
    /// that test's shape can't quietly stop covering the boundary case.
    ///
    /// **Negative control.** Change `0163`'s `tstzrange(period_start,
    /// period_end)` to the inclusive-inclusive form
    /// (`tstzrange(period_start, period_end, '[]')`) and this INSERT starts
    /// failing with `23P01 exclusion_violation` on `member_allowance_periods_no_overlap`,
    /// which is what `.expect(...)` below would report as a panic. Watched
    /// fail that way with `'[]'` substituted in before restoring the default.
    #[tokio::test]
    async fn an_abutting_period_is_accepted_by_the_database() {
        let Some(pool) = pg_test_pool("an_abutting_period_is_accepted_by_the_database").await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 1).await;
        let member = members[0];
        let base = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let end = base + chrono::Duration::days(30);

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member)
        .bind(base)
        .bind(end)
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed the first period");

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member)
        .bind(end) // next_start == prev_end
        .bind(end + chrono::Duration::days(30))
        .bind(Decimal::from(3000)),
        )
        .await
        .expect(
            "an abutting period (next_start == prev_end) must be ACCEPTED — the boundary instant \
             belongs to the NEW period only, under [)'s default bounds, so `&&` is false",
        );

        cleanup(&pool, account_id, &members).await;
    }

    // ── `close_open_period` ──────────────────────────────────────────────────

    /// `close_open_period` must SHRINK a period that is still open at the
    /// boundary, and leave a period that already ended at or before the
    /// boundary completely untouched.
    ///
    /// **Negative control.** Drop the `period_end > $2` half of
    /// `CLOSE_OPEN_PERIOD_SQL`'s `WHERE` clause (leaving only `period_start <
    /// $2`) and the FIRST assertion below fails — the already-closed row now
    /// matches too, since its `period_start` is also before `boundary`, so
    /// BOTH rows get shrunk: `assertion \`left == right\` failed: exactly the
    /// open period was shrunk / left: 2 / right: 1`. Watched fail with
    /// exactly that message with the predicate removed, before restoring it.
    #[tokio::test]
    async fn close_open_period_shrinks_open_periods_and_leaves_closed_ones_alone() {
        let Some(pool) =
            pg_test_pool("close_open_period_shrinks_open_periods_and_leaves_closed_ones_alone")
                .await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 1).await;
        let member = members[0];
        let base = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let boundary = base + chrono::Duration::days(15);

        // An OPEN period spanning the boundary: starts before, ends after.
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member)
        .bind(base)
        .bind(base + chrono::Duration::days(30))
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed the open period");

        // An ALREADY-CLOSED period, entirely before the boundary — a
        // different member so it cannot overlap the row above.
        let other_member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(other_member)
            .bind(format!("closed-{}@test.example", other_member.simple()))
            .execute(&pool)
            .await
            .expect("create other member's user row");
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(other_member)
        .bind(base - chrono::Duration::days(60))
        .bind(base - chrono::Duration::days(30))
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed the already-closed period");

        let closed_before: (Uuid, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
            "SELECT user_id, period_end FROM member_allowance_periods \
              WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(other_member)
        .fetch_one(&pool)
        .await
        .expect("read the already-closed row before");

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = close_open_period_fixture(&mut conn, account_id, boundary)
            .await
            .expect("close");
        drop(conn);

        assert_eq!(outcome.rows_shrunk, 1, "exactly the open period was shrunk");
        assert_eq!(outcome.rows_deleted, 0, "nothing here needed deleting");

        let open_after: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT period_end FROM member_allowance_periods WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member)
        .fetch_one(&pool)
        .await
        .expect("read the open row after");
        assert_eq!(
            open_after, boundary,
            "the open period now ends exactly at the boundary"
        );

        let closed_after: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT period_end FROM member_allowance_periods WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(other_member)
        .fetch_one(&pool)
        .await
        .expect("read the already-closed row after");
        assert_eq!(
            closed_after, closed_before.1,
            "an already-closed period must be left byte-for-byte alone"
        );

        cleanup(&pool, account_id, &[members, vec![other_member]].concat()).await;
    }

    /// The degenerate branch: a period whose `period_start` is strictly AFTER
    /// the boundary cannot be truncated in place (that would leave `period_end
    /// <= period_start`), so it is deleted outright.
    ///
    /// **Negative control.** Stub `rows_deleted` to `0` inside
    /// `close_open_period` without running `DELETE_UNSTARTED_OPEN_PERIOD_SQL`
    /// at all, and the FIRST assertion fails: `assertion \`left == right\`
    /// failed: the unstarted period was deleted / left: 0 / right: 1`. Watched
    /// fail with that exact message with the delete statement removed, before
    /// restoring it.
    #[tokio::test]
    async fn close_open_period_deletes_a_period_that_has_not_started_by_the_boundary() {
        let Some(pool) =
            pg_test_pool("close_open_period_deletes_a_period_that_has_not_started_by_the_boundary")
                .await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 1).await;
        let member = members[0];
        let base = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let boundary = base; // the period below starts strictly AFTER this.

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member)
        .bind(base + chrono::Duration::days(5))
        .bind(base + chrono::Duration::days(35))
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed a period that has not started as of the boundary");

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = close_open_period_fixture(&mut conn, account_id, boundary)
            .await
            .expect("close");
        drop(conn);

        assert_eq!(outcome.rows_deleted, 1, "the unstarted period was deleted");
        assert_eq!(outcome.rows_shrunk, 0);
        assert_eq!(
            total_rows(&pool, account_id).await,
            0,
            "nothing left for this account"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// A row whose `period_start` EQUALS the boundary exactly must be left
    /// completely alone — it is not a stale period to close, it is either the
    /// SAME period a caller is about to (re-)seed or another member's row
    /// that already starts exactly there.
    ///
    /// **Negative control.** Change `DELETE_UNSTARTED_OPEN_PERIOD_SQL`'s
    /// predicate from `period_start > $2` back to `period_start >= $2` and
    /// this test's assertion fails: `rows_deleted` is `1` instead of `0`, and
    /// `total_rows` after is `0` instead of `1` — the exact-match row is
    /// gone. That was a real bug caught while wiring this into
    /// `stripe_webhook::apply_invoice_paid`: it deletes the row `ON CONFLICT
    /// DO NOTHING` needs intact for `a_new_event_for_an_already_granted_period_grants_nothing_extra`
    /// to stay idempotent.
    #[tokio::test]
    async fn close_open_period_leaves_a_period_starting_exactly_at_the_boundary_alone() {
        let Some(pool) = pg_test_pool(
            "close_open_period_leaves_a_period_starting_exactly_at_the_boundary_alone",
        )
        .await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 1).await;
        let member = members[0];
        let boundary = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member)
        .bind(boundary) // period_start == boundary, exactly.
        .bind(boundary + chrono::Duration::days(30))
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed a period starting exactly at the boundary");

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = close_open_period_fixture(&mut conn, account_id, boundary)
            .await
            .expect("close");
        drop(conn);

        assert_eq!(
            outcome.rows_deleted, 0,
            "a period starting exactly at the boundary is not deleted"
        );
        assert_eq!(
            outcome.rows_shrunk, 0,
            "nor is it shrunk — it already ends after the boundary"
        );
        assert_eq!(
            total_rows(&pool, account_id).await,
            1,
            "the exact-match row must survive untouched"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// The point of `close_open_period` existing: without it, seeding a new
    /// period while the previous one is still open now hits `0163`'s
    /// exclusion constraint as a hard `sqlx::Error` instead of the silent
    /// double-insert it used to be. With `close_open_period` called first, in
    /// the same connection, the old period is truncated and the new INSERT
    /// never collides.
    ///
    /// **Negative control.** Delete the `close_open_period` call from this
    /// test (leaving only the second `roll_period`) and the LAST assertion
    /// panics: `roll must now succeed once the old period is closed first:
    /// Database(PgDatabaseError { code: "23P01", ... constraint:
    /// Some("member_allowance_periods_no_overlap") ... })`. That is exactly
    /// the failure `grant_plan`'s and `stripe_webhook`'s wiring exist to
    /// prevent. Watched fail with that message before adding the call back.
    #[tokio::test]
    async fn close_then_roll_succeeds_where_roll_alone_would_violate_the_constraint() {
        let Some(pool) =
            pg_test_pool("close_then_roll_succeeds_where_roll_alone_would_violate_the_constraint")
                .await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 2).await;
        grant(&pool, account_id, "team_standard", Some(2), None).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("month one");

        // A NEW period starting mid-way through month one's still-open
        // window — the shape a mid-period proration invoice (or a hand-sale
        // re-grant) actually produces.
        let new_start = start + chrono::Duration::days(10);
        let new_end = new_start + chrono::Duration::days(31);

        // Proven directly, not merely asserted: WITHOUT closing first, this
        // now fails at the exclusion constraint rather than double-inserting
        // silently.
        let err = roll_period_fixture(&mut conn, account_id, new_start, new_end)
            .await
            .expect_err(
                "seeding a new period while the old one is still open must now be rejected by \
                 0163's exclusion constraint — this is the exact failure close_open_period \
                 exists to prevent",
            );
        let db_err = err.as_database_error().expect("a database-level error");
        assert_eq!(db_err.code().as_deref(), Some("23P01"));

        // NOW close the old period first, in the SAME connection the caller
        // (`grant_plan::write_grant`, `stripe_webhook::apply_invoice_paid`)
        // uses, and retry.
        let closed = close_open_period_fixture(&mut conn, account_id, new_start)
            .await
            .expect("close");
        assert_eq!(
            closed.rows_shrunk, 2,
            "both members' still-open month-one periods are truncated"
        );

        let outcome = roll_period_fixture(&mut conn, account_id, new_start, new_end)
            .await
            .expect("roll must now succeed once the old period is closed first");
        assert_eq!(outcome.rows_granted, 2);

        drop(conn);
        cleanup(&pool, account_id, &members).await;
    }

    // ── CodeRabbit re-review, finding 1: frozen-`now()`-under-lock ──────────

    /// **Regression test for finding 1 (CRITICAL).** Proves the exact
    /// mechanism a real cross-connection race would exploit, without a real
    /// concurrent connection.
    ///
    /// # Why this is a sequential simulation, not a true two-connection race
    ///
    /// The bug this closes needs three things to line up: (1) a caller's
    /// transaction begins, freezing Postgres's `now()`; (2) it then WAITS —
    /// on lock contention, in production — during which (3) a DIFFERENT
    /// actor commits a genuinely newer `member_allowance_periods` row. A
    /// true reproduction would hold `pg_advisory_xact_lock` for the account
    /// on one connection, spawn `issuance::issue_grant` on a second (it must
    /// block inside `lock_period_rotation`), commit the newer period from the
    /// first connection, release, and assert the second connection's
    /// eventual `close_open_period` left it intact. That was attempted and
    /// set aside: reliably observing "the second connection has genuinely
    /// blocked inside the advisory-lock wait, not merely not-yet-started" from
    /// the test driver requires polling Postgres's own lock/wait-event views
    /// (`pg_locks`/`pg_stat_activity`) keyed to that specific session, which
    /// this build has no existing harness for and which this environment's
    /// sandboxed, non-`RELINFER_TEST_DATABASE_URL`-configured Postgres
    /// instance cannot be used to develop and verify against safely. Per this
    /// finding's own instructions, this is the documented fallback: the
    /// closest deterministic test of the underlying MECHANISM, phrased
    /// exactly as suggested there — a `statement_timestamp()`-derived
    /// boundary taken AFTER a simulated wait must not delete a period
    /// committed during that wait, while the STALE, frozen-at-`BEGIN`
    /// boundary a pre-fix `now()` would have produced DOES delete it (the
    /// negative control below, which is the bug itself, reproduced directly).
    ///
    /// The `tokio::time::sleep` here is not a flaky synchronization guess —
    /// it manufactures a real, monotonic gap between `stale_boundary` and
    /// `newer_period_start` so the two are provably ordered; the assertions
    /// below do not depend on the sleep's exact duration, only on it being
    /// nonzero, so this cannot flake under load the way a "sleep and hope
    /// the other task finished" pattern would.
    #[tokio::test]
    async fn a_boundary_resolved_after_a_simulated_wait_does_not_delete_a_period_committed_during_it(
    ) {
        let Some(pool) = pg_test_pool(
            "a_boundary_resolved_after_a_simulated_wait_does_not_delete_a_period_committed_during_it",
        )
        .await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 1).await;

        // What a STALE, transaction-start-frozen `now()` would have captured
        // had it been read here — exactly the boundary the PRE-FIX
        // `resolve_initial_period` (called before `lock_period_rotation`,
        // hence before any wait) would have produced.
        let stale_boundary = chrono::Utc::now();

        // Simulate "a concurrent roller committed a newer period while we
        // were blocked on the lock" — precisely what a racing
        // `seed_member_period` call does in production between the moment a
        // stale boundary would have been captured and the moment
        // `close_open_period` actually runs.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let newer_period_start = chrono::Utc::now();
        let newer_period_end = newer_period_start + chrono::Duration::days(30);
        assert!(
            newer_period_start > stale_boundary,
            "sanity: the simulated wait must produce a strictly later timestamp"
        );
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(members[0])
        .bind(newer_period_start)
        .bind(newer_period_end)
        .bind(Decimal::from(1000)),
        )
        .await
        .expect("simulate the concurrently-committed newer period");

        // The boundary `statement_timestamp()` resolves to when actually
        // evaluated AFTER the wait — what the FIXED `resolve_initial_period`
        // produces once `lock_period_rotation` has released it.
        let live_boundary = chrono::Utc::now();
        assert!(
            live_boundary > newer_period_start,
            "sanity: the live boundary must be at or after the newer period's start"
        );

        let mut conn = pool.acquire().await.expect("acquire");

        // ── The fix: closing with the LIVE (post-wait) boundary must leave
        // the newer, concurrently-committed period untouched.
        let outcome = close_open_period_fixture(&mut conn, account_id, live_boundary)
            .await
            .expect("close with live boundary");
        assert_eq!(
            outcome.rows_deleted, 0,
            "a boundary that reflects post-wait time must not delete a period that started \
             during the wait"
        );
        assert_eq!(
            total_rows(&pool, account_id).await,
            1,
            "the concurrently-committed period must survive"
        );

        // ── Negative control: this is what proves the test would actually
        // have caught the bug. Closing with the STALE, frozen-before-the-wait
        // boundary DELETES the newer period, because
        // `period_start (newer_period_start) > boundary (stale_boundary)`
        // trips `DELETE_UNSTARTED_OPEN_PERIOD_SQL` — exactly the destructive
        // path Finding 1 identified inside `issuance::issue_grant` before
        // `lock_period_rotation` was moved ahead of `resolve_initial_period`
        // and `resolve_initial_period`/`CURRENT_OPEN_PERIOD_SQL` switched
        // from `now()` to `statement_timestamp()`.
        let outcome = close_open_period_fixture(&mut conn, account_id, stale_boundary)
            .await
            .expect("close with stale boundary");
        assert_eq!(
            outcome.rows_deleted, 1,
            "negative control: a STALE, transaction-start-frozen boundary deletes the newer \
             period — reproducing the exact bug Finding 1 fixes. If this assertion fails, the \
             test above is not actually distinguishing stale from live boundaries."
        );
        assert_eq!(
            total_rows(&pool, account_id).await,
            0,
            "negative control: the newer period is gone under the stale boundary"
        );

        drop(conn);
        cleanup(&pool, account_id, &members).await;
    }

    // ── `seed_member_period` (enforcement-completion PR, plan §1.9/§2.A) ────

    /// A window spanning "now", unlike [`period`] above (a fixed 2023 range)
    /// — `seed_member_period`'s `CURRENT_OPEN_PERIOD_SQL`/`is_blocked`'s own
    /// lookup both require `period_start <= now() AND period_end > now()`.
    ///
    /// Truncated to whole seconds for the same reason [`period`] is built
    /// from whole-second epochs: Postgres `timestamptz` is MICROsecond
    /// precision while `chrono::Utc::now()` carries nanoseconds (100ns steps
    /// on Windows), so a sub-microsecond remainder is silently dropped on
    /// INSERT and an exact-equality assertion against the re-read value
    /// fails (observed: `…146387800Z` written, `…146387Z` read back).
    fn current_window() -> (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) {
        let now = chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp(), 0)
            .expect("current epoch seconds are in range");
        (
            now - chrono::Duration::days(1),
            now + chrono::Duration::days(30),
        )
    }

    /// The base case: an account with an established open period (two
    /// existing members already granted) gets a THIRD, mid-period member
    /// seeded at the full per-member allowance, reusing the SAME window.
    #[tokio::test]
    async fn seed_member_period_grants_the_full_allowance_to_a_mid_period_joiner() {
        let Some(pool) =
            pg_test_pool("seed_member_period_grants_the_full_allowance_to_a_mid_period_joiner")
                .await
        else {
            return;
        };
        let (account_id, mut members) = seed_account(&pool, 2).await;
        grant(&pool, account_id, "team_standard", Some(2), None).await;
        let (start, end) = current_window();

        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("establish the open period");
        drop(conn);

        let latecomer = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(latecomer)
            .bind(format!("seed-late-{}@test.example", latecomer.simple()))
            .execute(&pool)
            .await
            .expect("create latecomer");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(latecomer)
        .execute(&pool)
        .await
        .expect("add latecomer");
        members.push(latecomer);

        let inserted = seed_member_period(&pool, account_id, latecomer)
            .await
            .expect("seed_member_period must not error");
        assert!(
            inserted,
            "a mid-period joiner with an open period and an active grant is seeded"
        );

        let row: (
            Decimal,
            Decimal,
            Option<Decimal>,
            chrono::DateTime<chrono::Utc>,
        ) = sqlx::query_as(
            "SELECT granted_credits, consumed_credits, cap_credits, period_start \
             FROM member_allowance_periods WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(latecomer)
        .fetch_one(&pool)
        .await
        .expect("read the seeded row");
        assert_eq!(
            row.0,
            Decimal::from(3000),
            "the FULL per-member allowance, never prorated"
        );
        assert_eq!(
            row.1,
            Decimal::ZERO,
            "a freshly seeded member has consumed nothing"
        );
        assert!(
            row.2.is_none(),
            "cap_credits stays NULL, same policy-default posture as roll_period"
        );
        assert_eq!(
            row.3, start,
            "reuses the account's EXISTING open window, not a fresh one"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// Re-running for the SAME member in the SAME period is a safe no-op:
    /// `Ok(false)`, and the existing row is left byte-for-byte alone — the
    /// shape a re-invite/role-change upsert of `ensure_org_membership`
    /// produces on every call, not just the first.
    #[tokio::test]
    async fn seed_member_period_is_idempotent_for_the_same_member_and_period() {
        let Some(pool) =
            pg_test_pool("seed_member_period_is_idempotent_for_the_same_member_and_period").await
        else {
            return;
        };
        let (account_id, mut members) = seed_account(&pool, 1).await;
        grant(&pool, account_id, "team_standard", Some(1), None).await;
        let (start, end) = current_window();
        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("establish the open period");
        drop(conn);

        let joiner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(joiner)
            .bind(format!("seed-idem-{}@test.example", joiner.simple()))
            .execute(&pool)
            .await
            .expect("create joiner");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(joiner)
        .execute(&pool)
        .await
        .expect("add joiner");
        members.push(joiner);

        let first = seed_member_period(&pool, account_id, joiner)
            .await
            .expect("first seed");
        assert!(first, "the first call actually inserts");

        // Simulate spend before the (re-)invite replays.
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "UPDATE member_allowance_periods SET consumed_credits = $3 \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(joiner)
        .bind(Decimal::from(500)),
        )
        .await
        .expect("simulate spend");

        let second = seed_member_period(&pool, account_id, joiner)
            .await
            .expect("second seed");
        assert!(
            !second,
            "re-seeding the same member for the same period must be a no-op"
        );

        let consumed: Decimal = sqlx::query_scalar(
            "SELECT consumed_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(joiner)
        .fetch_one(&pool)
        .await
        .expect("read consumed_credits");
        assert_eq!(
            consumed,
            Decimal::from(500),
            "the re-seed attempt must not reset (or otherwise touch) consumed_credits"
        );
        assert_eq!(
            total_rows(&pool, account_id).await,
            2,
            "still exactly one row per member (the pre-existing member plus the joiner) — no duplicate"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// Seeding member B must never touch member A's row — proven directly by
    /// giving A a non-zero `consumed_credits` BEFORE seeding B and asserting
    /// it is unchanged after. [`SEED_ONE_MEMBER_PERIOD_SQL`] names only the
    /// one `user_id` being seeded, so there is no roster read here that
    /// could ever reach another member's row — this test is what proves
    /// that structural claim rather than merely asserting it in a comment.
    #[tokio::test]
    async fn seed_member_period_never_touches_another_members_row() {
        let Some(pool) = pg_test_pool("seed_member_period_never_touches_another_members_row").await
        else {
            return;
        };
        let (account_id, mut members) = seed_account(&pool, 1).await;
        let member_a = members[0];
        grant(&pool, account_id, "team_standard", Some(1), None).await;
        let (start, end) = current_window();
        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("establish the open period");
        drop(conn);

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "UPDATE member_allowance_periods SET consumed_credits = $3 \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_a)
        .bind(Decimal::from(1234)),
        )
        .await
        .expect("give A some spend");

        let member_b = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(member_b)
            .bind(format!("seed-b-{}@test.example", member_b.simple()))
            .execute(&pool)
            .await
            .expect("create B");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(member_b)
        .execute(&pool)
        .await
        .expect("add B");
        members.push(member_b);

        let inserted = seed_member_period(&pool, account_id, member_b)
            .await
            .expect("seed B");
        assert!(inserted);

        let a_consumed: Decimal = sqlx::query_scalar(
            "SELECT consumed_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_a)
        .fetch_one(&pool)
        .await
        .expect("read A's row");
        assert_eq!(
            a_consumed,
            Decimal::from(1234),
            "seeding B must not touch A's consumed_credits"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// No active grant at all: `Ok(false)`, and nothing is written — the
    /// shape a still-unbilled personal account or a revoked org's stray
    /// membership event produces.
    #[tokio::test]
    async fn seed_member_period_with_no_active_grant_writes_nothing() {
        let Some(pool) =
            pg_test_pool("seed_member_period_with_no_active_grant_writes_nothing").await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 1).await;

        let inserted = seed_member_period(&pool, account_id, members[0])
            .await
            .expect("seed_member_period must not error with no grant");
        assert!(!inserted);
        assert_eq!(total_rows(&pool, account_id).await, 0, "nothing written");

        cleanup(&pool, account_id, &members).await;
    }

    /// The FIRST member ever to join an account with zero prior period rows
    /// (a brand-new org, granted at creation with nobody on the roster —
    /// `clerk_provision::provision_billing`'s exact starting state) still
    /// gets seeded via the fresh-window branch (case 2 of
    /// [`seed_member_period`]'s doc comment) rather than
    /// [`CURRENT_OPEN_PERIOD_SQL`] (there is no existing row to read a window
    /// from). This is the case that makes the org-enforcement flip safe.
    #[tokio::test]
    async fn seed_member_period_establishes_a_fresh_window_for_a_brand_new_orgs_first_member() {
        let Some(pool) = pg_test_pool(
            "seed_member_period_establishes_a_fresh_window_for_a_brand_new_orgs_first_member",
        )
        .await
        else {
            return;
        };
        // Zero members, mirroring `ensure_org_account`'s state at creation.
        let (account_id, mut members) = seed_account(&pool, 0).await;
        grant(&pool, account_id, "free", None, None).await;
        assert_eq!(
            total_rows(&pool, account_id).await,
            0,
            "sanity: a grant against zero members seeds no period rows (roll_period's own INSERT \
             ... SELECT reads an empty account_members)"
        );

        let first_member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(first_member)
            .bind(format!("seed-first-{}@test.example", first_member.simple()))
            .execute(&pool)
            .await
            .expect("create the first member");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(account_id)
        .bind(first_member)
        .execute(&pool)
        .await
        .expect("add the first member");
        members.push(first_member);

        // Server-side bounds, not `chrono::Utc::now()` host-clock reads: the
        // assertion below compares against `period_start`, which this
        // production path derives from Postgres's `statement_timestamp()`
        // (see `seed_member_period`'s own doc comment). Bounding with the
        // HOST clock instead would compare two different clocks and could
        // spuriously fail under real clock skew between the test process and
        // the database server; reading the bounds from the server itself
        // removes the skew entirely.
        let before: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT statement_timestamp()")
                .fetch_one(&pool)
                .await
                .expect("read statement_timestamp() before the call");
        let inserted = seed_member_period(&pool, account_id, first_member)
            .await
            .expect("seed_member_period must not error");
        let after: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT statement_timestamp()")
                .fetch_one(&pool)
                .await
                .expect("read statement_timestamp() after the call");
        assert!(
            inserted,
            "the account's very first joining member must still be seeded"
        );

        let row: (
            Decimal,
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
        ) = sqlx::query_as(
            "SELECT granted_credits, period_start, period_end \
                 FROM member_allowance_periods WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(first_member)
        .fetch_one(&pool)
        .await
        .expect("read the seeded row");
        assert_eq!(
            row.0,
            Decimal::from(1000),
            "the free plan's flat allowance (migration 0153)"
        );
        assert!(
            row.1 >= before && row.1 <= after,
            "the fresh window starts at seed-time 'now', not some other timestamp: {}",
            row.1
        );
        assert!(
            row.2 > chrono::Utc::now() + chrono::Duration::days(365 * 50),
            "a lifetime plan's period_length is NULL, resolving to the ~100-year window \
             `issuance::resolve_initial_period` also uses: {}",
            row.2
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// A LAPSED account (finding 4, review round 2): every period this
    /// account ever held has already ended, and none is open. A newcomer
    /// joining must be seeded NOTHING — minting an off-cycle `now()`-anchored
    /// window here is exactly what an eventual real re-grant's period would
    /// either collide with (the exclusion constraint) or silently
    /// double-allow (if it happens not to overlap). The next real
    /// `roll_period` call, whenever the account is re-granted, covers this
    /// member like any other current member with no special case needed.
    #[tokio::test]
    async fn seed_member_period_on_a_lapsed_account_seeds_nothing() {
        let Some(pool) = pg_test_pool("seed_member_period_on_a_lapsed_account_seeds_nothing").await
        else {
            return;
        };
        let (account_id, mut members) = seed_account(&pool, 1).await;
        grant(&pool, account_id, "team_standard", Some(1), None).await;

        // A period that has ALREADY ENDED — the account once held an
        // allowance but nothing has renewed it since (e.g. Stripe past_due).
        let lapsed_start = chrono::Utc::now() - chrono::Duration::days(60);
        let lapsed_end = chrono::Utc::now() - chrono::Duration::days(30);
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(members[0])
        .bind(lapsed_start)
        .bind(lapsed_end)
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed the lapsed period");

        let newcomer = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(newcomer)
            .bind(format!("lapsed-{}@test.example", newcomer.simple()))
            .execute(&pool)
            .await
            .expect("create newcomer");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(newcomer)
        .execute(&pool)
        .await
        .expect("add newcomer");
        members.push(newcomer);

        let inserted = seed_member_period(&pool, account_id, newcomer)
            .await
            .expect("seed_member_period must not error on a lapsed account");
        assert!(
            !inserted,
            "a lapsed account (a period existed, but none is open) must seed NOTHING — minting \
             an off-cycle now()-anchored window here would double-grant once the real re-grant \
             rolls"
        );
        assert_eq!(
            total_rows(&pool, account_id).await,
            1,
            "only the original lapsed row remains; the newcomer got no off-cycle window"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// The sequential proof finding 5 asks for in place of a true concurrency
    /// harness: a member who already holds a window that DIVERGES from what
    /// [`CURRENT_OPEN_PERIOD_SQL`] would resolve for "now" (the exact state
    /// two racing first-joiners — closed off by [`lock_period_rotation`]
    /// going forward — used to leave behind permanently) must degrade to `Ok(false)`
    /// rather than propagate `0163`'s exclusion-violation error forever.
    #[tokio::test]
    async fn seed_member_period_degrades_a_divergent_window_to_a_noop_instead_of_erroring() {
        let Some(pool) = pg_test_pool(
            "seed_member_period_degrades_a_divergent_window_to_a_noop_instead_of_erroring",
        )
        .await
        else {
            return;
        };
        let (account_id, members) = seed_account(&pool, 2).await;
        let (member_a, member_b) = (members[0], members[1]);
        grant(&pool, account_id, "team_standard", Some(2), None).await;

        // Two DIFFERENT, overlapping "current" windows for the two members —
        // hand-constructed rather than produced by an actual race, since a
        // sequential proof suffices (finding 5's own note). Whole seconds,
        // same as `current_window`: A's window is asserted byte-identical
        // after the round-trip, and Postgres truncates sub-microsecond bits.
        let now = chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp(), 0)
            .expect("current epoch seconds are in range");
        let window_a = (
            now - chrono::Duration::hours(2),
            now + chrono::Duration::days(30),
        );
        let window_b = (
            now - chrono::Duration::hours(1),
            now + chrono::Duration::days(31),
        );

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member_a)
        .bind(window_a.0)
        .bind(window_a.1)
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed A's window");

        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "INSERT INTO member_allowance_periods \
                 (account_id, user_id, period_start, period_end, granted_credits) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(member_b)
        .bind(window_b.0)
        .bind(window_b.1)
        .bind(Decimal::from(3000)),
        )
        .await
        .expect("seed B's DIFFERENT window — same account, same instant, no shared window");

        // A re-seed attempt for A: `CURRENT_OPEN_PERIOD_SQL` resolves the
        // "current" window via `ORDER BY period_start DESC LIMIT 1`, which
        // picks B's LATER-starting window, not A's own. The INSERT for A
        // therefore targets a window that does not match A's existing row's
        // natural key (so `ON CONFLICT` never engages) but DOES overlap it,
        // hitting 0163's exclusion constraint instead.
        let result = seed_member_period(&pool, account_id, member_a)
            .await
            .expect("must degrade to Ok(false), never propagate the exclusion violation");
        assert!(
            !result,
            "a divergent pre-existing window must read as an already-seeded no-op, not a fresh \
             grant attempt"
        );

        assert_eq!(
            total_rows(&pool, account_id).await,
            2,
            "still exactly the two original rows — no new row from the failed attempt, and \
             neither original row was touched or duplicated"
        );
        let a_row: (chrono::DateTime<chrono::Utc>, Decimal) = sqlx::query_as(
            "SELECT period_start, granted_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_a)
        .fetch_one(&pool)
        .await
        .expect("read A's row");
        assert_eq!(a_row.0, window_a.0, "A's original window is untouched");

        cleanup(&pool, account_id, &members).await;
    }

    // ── The org-flip invariant (`clerk_provision::provision_billing`) ───────

    /// Exercises `provision_billing` directly against a scaffolded ORG
    /// account with zero members, the exact state `ensure_org_account`
    /// leaves a brand-new org in. Proves the two halves of the
    /// enforcement-completion invariant end to end:
    ///
    ///   1. granting + flipping a zero-member account is itself harmless
    ///      (no panics, no rows for a nonexistent roster), and
    ///   2. once flipped, the FIRST member to actually join gets a real,
    ///      spendable allowance via [`seed_member_period`] — proving there
    ///      is no window where the org is enforced but nobody can be
    ///      granted anything.
    ///
    /// Deliberately does NOT go through `ensure_org_account` itself — that
    /// needs the full `AppState`/router harness (`ORGT_TEST_BASE_URL`) this
    /// crate's pg_tests do not stand up. `provision_billing` is the exact
    /// function `ensure_org_account` calls at the point in question, so
    /// exercising it directly against a hand-scaffolded account (same
    /// scaffold style as `seed_account` above, built inline since it must
    /// deliberately hold zero `account_members` rows) covers the same
    /// invariant without the harness.
    #[tokio::test]
    async fn provision_billing_grants_and_flips_a_zero_member_org_then_seeds_its_first_joiner() {
        let Some(pool) = pg_test_pool(
            "provision_billing_grants_and_flips_a_zero_member_org_then_seeds_its_first_joiner",
        )
        .await
        else {
            return;
        };

        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("org-flip-owner-{}@test.example", owner.simple()))
            .execute(&pool)
            .await
            .expect("create the org's nominal owner");
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
            .bind(account_id)
            .bind(owner)
            .bind(format!("org_{}", account_id.simple()))
            .execute(&pool)
            .await
            .expect("create the zero-member org account");

        crate::clerk_provision::provision_billing(&pool, account_id).await;

        let (grant_exists, enforced): (bool, bool) = {
            let g: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM grants WHERE account_id = $1 AND revoked_at IS NULL)",
            )
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .expect("check grants");
            let e: bool = sqlx::query_scalar(
                "SELECT billing_enforcement_enabled FROM accounts WHERE id = $1",
            )
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .expect("check the enforcement flag");
            (g, e)
        };
        assert!(
            grant_exists,
            "a zero-member org must still receive a real grant"
        );
        assert!(
            enforced,
            "the whole point of this PR: enforcement flips at org creation now, not just at \
             signup"
        );
        assert_eq!(
            total_rows(&pool, account_id).await,
            0,
            "nobody is on the roster yet, so nothing was seeded — this is the state \
             `seed_member_period`'s first-joiner path exists to close"
        );

        // The org's first real member joins. `seat_tier_id` explicit
        // (billing self-serve PR F, migration 0176: this account carries a
        // real `clerk_org_id`, so the trigger's org branch requires one).
        let standard_tier_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(&pool)
        .await
        .expect("active standard seat tier must exist (0165's seed)");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(standard_tier_id)
        .execute(&pool)
        .await
        .expect("the owner joins as the first member");
        let seeded = seed_member_period(&pool, account_id, owner)
            .await
            .expect("seed_member_period must not error");
        assert!(
            seeded,
            "the first member to join an enforced-but-empty org must be seeded"
        );

        let granted: Decimal = sqlx::query_scalar(
            "SELECT granted_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .expect("read the first member's granted_credits");
        assert_eq!(
            granted,
            Decimal::from(1000),
            "the free plan's flat allowance (migration 0153)"
        );

        cleanup(&pool, account_id, &[owner]).await;
    }

    // ── PR E: per-tier allowances for `team` grants ──────────────────────────

    /// **The headline per-tier case.** A `team` grant with a MIXED roster —
    /// 2 `standard` seats, 1 `premium` — must grant each member THEIR OWN
    /// tier's allowance (3,000 / 3,000 / 15,000), not a single averaged or
    /// zeroed figure. `grant_seat_tiers` is seeded here to match the roster
    /// exactly (as if `acquire_seat` had already purchased all three seats,
    /// PR F) — the per-member allowance computed by [`roll_period`] never
    /// reads `grant_seat_tiers` at all (it joins each member's OWN
    /// `seat_tier_id` directly), so this test seeds the ledger purely to
    /// prove the SEPARATE assertion below it: `roll_period` reports
    /// whatever ledger total it found, untouched.
    #[tokio::test]
    async fn team_grant_renewal_grants_each_member_their_own_tiers_allowance() {
        let Some(pool) =
            pg_test_pool("team_grant_renewal_grants_each_member_their_own_tiers_allowance").await
        else {
            return;
        };
        let (account_id, members) = seed_team_org(&pool, &["standard", "standard", "premium"]).await;
        let grant_id = grant_team(&pool, account_id, 3, None).await;
        seed_purchased_tiers(&pool,
            account_id,
            grant_id, &[("standard", 2), ("premium", 1)]).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        assert_eq!(outcome.rows_granted, 3, "one row per member");
        assert_eq!(outcome.members, 3);
        assert_eq!(
            outcome.seats,
            Some(3),
            "grants.seats is whatever the purchase ledger already held — PR F: roll_period does \
             not recompute it from the roster"
        );
        assert_eq!(
            outcome.total_credits_granted,
            Some(Decimal::from(21_000)),
            "the aggregate across all three rows: 2x3,000 + 1x15,000"
        );
        assert!(
            outcome.use_seat_tier_credits,
            "a team grant must report that per-member credits came from the tiered join, not a \
             single uniform figure"
        );

        let got = rows(&pool, account_id, start).await;
        assert_eq!(got.len(), 3);
        let (mut standard_count, mut premium_count) = (0, 0);
        for (user_id, granted, cap) in &got {
            assert!(cap.is_none(), "member {user_id}'s cap_credits must stay NULL");
            if *granted == Decimal::from(3000) {
                standard_count += 1;
            } else if *granted == Decimal::from(15000) {
                premium_count += 1;
            } else {
                panic!("member {user_id} got an unexpected granted_credits: {granted}");
            }
        }
        assert_eq!(standard_count, 2, "both standard members got 3,000");
        assert_eq!(premium_count, 1, "the premium member got 15,000, not 3,000 or 0");

        // PR F: the ledger this test seeded is exactly what it still reads
        // after the roll — roll_period never recomputes grant_seat_tiers.
        let tier_rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT st.code, gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1 ORDER BY st.code",
        )
        .bind(grant_id)
        .fetch_all(&pool)
        .await
        .expect("read grant_seat_tiers");
        assert_eq!(
            tier_rows,
            vec![("premium".to_string(), 1), ("standard".to_string(), 2)],
            "grant_seat_tiers must be exactly what this test seeded — untouched by the roll"
        );

        let seats_column: i32 = sqlx::query_scalar("SELECT seats FROM grants WHERE id = $1")
            .bind(grant_id)
            .fetch_one(&pool)
            .await
            .expect("read grants.seats");
        assert_eq!(seats_column, 3, "grants.seats must also be untouched by the roll");

        // The entitlement total (billing::entitlement::PLAN_ENTITLEMENT_SQL)
        // must agree with the SAME grant_seat_tiers rows — 2*3,000 + 1*15,000.
        let entitlement = crate::billing::entitlement::current_plan(&pool, account_id)
            .await
            .expect("entitlement query")
            .expect("the team grant exists");
        assert_eq!(
            entitlement.credits, 21_000.0,
            "Σ(seats x tier.credits): 2x3,000 + 1x15,000"
        );
        assert!(
            entitlement.credits_per_seat.is_none(),
            "a mixed-tier team grant has no single per-seat figure to render"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// **PR F required coverage: roll no longer recounts the roster —
    /// purchased counts survive a roster-only change.** An out-of-band move
    /// of a member's `seat_tier_id` (NOT through the tier-change route, so
    /// NOT via `pending_seat_tier_id` — simulating drift, a bug, or manual
    /// intervention) must NOT be picked up by a subsequent roll: the
    /// purchase ledger (`grant_seat_tiers`/`grants.seats`) stays exactly
    /// what it was, because nothing in `roll_period` reads
    /// `account_members.seat_tier_id` to decide it any more (superseding
    /// PR E's `team_grant_reconciliation_drops_a_tier_that_lost_its_last_member`,
    /// which proved the OPPOSITE — that roster changes WERE picked up —
    /// under the reconciliation model PR F removes). The member's OWN
    /// per-member allowance, by contrast, DOES follow their live
    /// `seat_tier_id` on the next roll — that join is independent of the
    /// ledger, proving the two are genuinely decoupled, not merely
    /// unobserved together.
    #[tokio::test]
    async fn roll_no_longer_recounts_the_roster_purchased_counts_survive_a_roster_only_change() {
        let Some(pool) = pg_test_pool(
            "roll_no_longer_recounts_the_roster_purchased_counts_survive_a_roster_only_change",
        )
        .await
        else {
            return;
        };
        let (account_id, members) = seed_team_org(&pool, &["premium"]).await;
        let user_id = members[0];
        let grant_id = grant_team(&pool, account_id, 1, None).await;
        seed_purchased_tiers(&pool, account_id, grant_id, &[("premium", 1)]).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("first roll");
        drop(conn);

        // Out-of-band roster change: move the member to `standard` directly,
        // NOT via the tier-change route (no `pending_seat_tier_id` involved
        // — that mechanism has its own dedicated tests below).
        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier");
        sqlx::query("UPDATE account_members SET seat_tier_id = $2 WHERE account_id = $1")
            .bind(account_id)
            .bind(standard_id)
            .execute(&pool)
            .await
            .expect("move member to standard, out of band");

        let next_start = end;
        let next_end = end + chrono::Duration::days(31);
        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, next_start, next_end)
            .await
            .expect("second roll");
        drop(conn);

        // The ledger is UNCHANGED — still premium:1 — because roll_period
        // never reads the roster to decide grant_seat_tiers any more.
        let tier_rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT st.code, gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1",
        )
        .bind(grant_id)
        .fetch_all(&pool)
        .await
        .expect("read grant_seat_tiers after the second roll");
        assert_eq!(
            tier_rows,
            vec![("premium".to_string(), 1)],
            "the purchased ledger must survive a roster-only change untouched"
        );
        assert_eq!(
            outcome.seats,
            Some(1),
            "grants.seats must also stay at the purchased total, not the (now different) roster"
        );

        // But the member's OWN allowance for the new period DOES follow
        // their live tier — this join is independent of the ledger.
        let next_rows = rows(&pool, account_id, next_start).await;
        assert_eq!(next_rows.len(), 1);
        assert_eq!(next_rows[0].0, user_id);
        assert_eq!(
            next_rows[0].1,
            Decimal::from(3000),
            "the member's own allowance follows their live (standard) seat_tier_id, decoupled \
             from the untouched purchase ledger"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// **Pending downgrade, applied exactly at roll — plan §1.7.** A member on
    /// `premium` with a `pending_seat_tier_id` of `standard` must keep drawing
    /// PREMIUM credits right up until a roll actually happens, and the SAME
    /// roll that applies the downgrade must grant the member their NEW
    /// (standard) allowance for the period it is seeding — proving the apply
    /// happens BEFORE `granted_credits` is computed, not merely before the
    /// function returns. **PR F required coverage:** also proves the
    /// purchase ledger MOVES with it — `grant_seat_tiers` loses its premium
    /// row (0168's "zero seats, no row" convention) and gains a standard
    /// one, `grants.seats` staying at the same total throughout (a MOVE, not
    /// a net change).
    #[tokio::test]
    async fn pending_downgrade_is_applied_exactly_at_roll_not_before() {
        let Some(pool) =
            pg_test_pool("pending_downgrade_is_applied_exactly_at_roll_not_before").await
        else {
            return;
        };
        let (account_id, members) = seed_team_org(&pool, &["premium"]).await;
        let user_id = members[0];
        let grant_id = grant_team(&pool, account_id, 1, None).await;
        seed_purchased_tiers(&pool, account_id, grant_id, &[("premium", 1)]).await;

        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier");
        sqlx::query(
            "UPDATE account_members SET pending_seat_tier_id = $3 \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("record a pending downgrade");

        // BEFORE any roll: still premium, the downgrade still only pending.
        let (tier_before, pending_before): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT st.code, am.pending_seat_tier_id FROM account_members am \
             JOIN seat_tiers st ON st.id = am.seat_tier_id \
             WHERE am.account_id = $1 AND am.user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("read pre-roll tier");
        assert_eq!(tier_before, "premium", "NOT applied before a roll runs");
        assert_eq!(pending_before, Some(standard_id));

        let (start, end) = period();
        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        // AFTER the roll: applied, and cleared.
        let (tier_after, pending_after): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT st.code, am.pending_seat_tier_id FROM account_members am \
             JOIN seat_tiers st ON st.id = am.seat_tier_id \
             WHERE am.account_id = $1 AND am.user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("read post-roll tier");
        assert_eq!(tier_after, "standard", "applied exactly at roll");
        assert_eq!(pending_after, None, "the pending marker must be cleared");

        // The credits granted THIS SAME roll already reflect the NEW tier —
        // the apply happened BEFORE granted_credits was computed, not after.
        assert_eq!(outcome.rows_granted, 1);
        let got = rows(&pool, account_id, start).await;
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].1,
            Decimal::from(3000),
            "this period's allowance must already be the POST-downgrade (standard) figure"
        );

        // PR F: the purchase ledger moved WITH the flip, in the same roll.
        let tier_rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT st.code, gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1",
        )
        .bind(grant_id)
        .fetch_all(&pool)
        .await
        .expect("read grant_seat_tiers after the roll");
        assert_eq!(
            tier_rows,
            vec![("standard".to_string(), 1)],
            "premium must have NO row left (0168's zero-seats convention) and standard must hold \
             the moved seat"
        );
        assert_eq!(
            outcome.seats,
            Some(1),
            "grants.seats is the SAME total after a move, not incremented or decremented"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// **CodeRabbit, PR #655 review.** A SECOND `roll_period` call for a
    /// period this account already rolled (the redelivered-event shape —
    /// same `period_start` as the first roll) must NOT apply a downgrade
    /// scheduled in between: the downgrade must stay pending, and
    /// `seat_tier_id` must stay at its PRE-downgrade value, until a roll for
    /// a genuinely NEW `period_start` actually happens. Companion to
    /// [`pending_downgrade_is_applied_exactly_at_roll_not_before`] above,
    /// which proves the opposite half of the same invariant (it DOES apply
    /// on the first roll of a new period) — this proves the repeat-call case
    /// that invariant's naive "apply unconditionally" implementation used to
    /// get wrong. **PR F required coverage: redelivery-safe.** The pending
    /// downgrade's ledger MOVE (`move_pending_seat_tier_counts`) must not
    /// double-move on this repeat call either — proving the move, not just
    /// the flip, is gated on `!already_rolled`.
    #[tokio::test]
    async fn a_repeat_roll_of_the_same_period_does_not_apply_a_downgrade_scheduled_after_it() {
        let Some(pool) = pg_test_pool(
            "a_repeat_roll_of_the_same_period_does_not_apply_a_downgrade_scheduled_after_it",
        )
        .await
        else {
            return;
        };
        let (account_id, members) = seed_team_org(&pool, &["premium"]).await;
        let user_id = members[0];
        let grant_id = grant_team(&pool, account_id, 1, None).await;
        seed_purchased_tiers(&pool, account_id, grant_id, &[("premium", 1)]).await;

        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier");

        let (start, end) = period();

        // Initial roll: seeds this period's allowance row at the member's
        // CURRENT (premium) tier. No downgrade is pending yet.
        let mut conn = pool.acquire().await.expect("acquire");
        let first = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("initial roll");
        drop(conn);
        assert_eq!(first.rows_granted, 1, "initial roll seeds the one member");
        let first_rows = rows(&pool, account_id, start).await;
        assert_eq!(first_rows.len(), 1);
        assert_eq!(
            first_rows[0].1,
            Decimal::from(15000),
            "premium allowance for the first roll"
        );

        // A downgrade is scheduled AFTER the first roll, still mid-period.
        sqlx::query(
            "UPDATE account_members SET pending_seat_tier_id = $3 \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("record a pending downgrade");

        // A SECOND event reports the SAME period_start (a redelivery, or any
        // other `customer.subscription.updated` that lands mid-period without
        // Stripe having actually rolled the period). `roll_period` re-enters
        // with the identical `(period_start, period_end)`.
        let mut conn = pool.acquire().await.expect("acquire");
        let second = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("repeat roll for the same period");
        drop(conn);
        assert_eq!(
            second.rows_granted, 0,
            "the allowance row already exists for this period — ON CONFLICT DO NOTHING"
        );

        // The downgrade must REMAIN PENDING and seat_tier_id must be
        // UNCHANGED — the whole point of this test.
        let (tier_after_second, pending_after_second): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT st.code, am.pending_seat_tier_id FROM account_members am \
             JOIN seat_tiers st ON st.id = am.seat_tier_id \
             WHERE am.account_id = $1 AND am.user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("read tier after the repeat roll");
        assert_eq!(
            tier_after_second, "premium",
            "a repeat roll of an ALREADY-rolled period must not apply a downgrade early"
        );
        assert_eq!(
            pending_after_second,
            Some(standard_id),
            "the pending downgrade must survive a repeat roll of the same period"
        );
        // And the existing allowance row for this period is untouched too —
        // still the pre-downgrade (premium) figure, not silently corrected.
        let rows_after_second = rows(&pool, account_id, start).await;
        assert_eq!(rows_after_second.len(), 1);
        assert_eq!(rows_after_second[0].1, Decimal::from(15000));

        // PR F: the ledger MOVE is gated on `!already_rolled` exactly like
        // the flip — a repeat call for an already-rolled period must move
        // NOTHING, or a redelivered event would double-move real purchased
        // seats. Still premium:1, byte-for-byte what this test seeded.
        let tier_rows_after_second: Vec<(String, i32)> = sqlx::query_as(
            "SELECT st.code, gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1 ORDER BY st.code",
        )
        .bind(grant_id)
        .fetch_all(&pool)
        .await
        .expect("read grant_seat_tiers after the repeat roll");
        assert_eq!(
            tier_rows_after_second,
            vec![("premium".to_string(), 1)],
            "the ledger must NOT move on a repeat roll of an already-rolled period — redelivery \
             safety for the move, same as the flip it is paired with"
        );
        assert_eq!(
            second.seats,
            Some(1),
            "grants.seats must stay at the untouched purchased total on a repeat roll"
        );

        // A roll for a NEW period_start, however, must apply it — same
        // invariant `pending_downgrade_is_applied_exactly_at_roll_not_before`
        // proves for a first roll, exercised here for a period that follows
        // an already-rolled one.
        let next_start = end;
        let next_end = end + chrono::Duration::days(31);
        let mut conn = pool.acquire().await.expect("acquire");
        let third = roll_period_fixture(&mut conn, account_id, next_start, next_end)
            .await
            .expect("roll into the new period");
        drop(conn);
        assert_eq!(third.rows_granted, 1);

        let (tier_after_third, pending_after_third): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT st.code, am.pending_seat_tier_id FROM account_members am \
             JOIN seat_tiers st ON st.id = am.seat_tier_id \
             WHERE am.account_id = $1 AND am.user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("read tier after the new-period roll");
        assert_eq!(
            tier_after_third, "standard",
            "the downgrade applies once a roll for a NEW period actually happens"
        );
        assert_eq!(pending_after_third, None);

        let next_rows = rows(&pool, account_id, next_start).await;
        assert_eq!(next_rows.len(), 1);
        assert_eq!(
            next_rows[0].1,
            Decimal::from(3000),
            "the new period's allowance already reflects the now-applied (standard) tier"
        );

        // PR F: the ledger move that was suppressed on the repeat (second)
        // roll now DOES happen, exactly once, on this genuinely new period.
        let tier_rows_after_third: Vec<(String, i32)> = sqlx::query_as(
            "SELECT st.code, gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1",
        )
        .bind(grant_id)
        .fetch_all(&pool)
        .await
        .expect("read grant_seat_tiers after the new-period roll");
        assert_eq!(
            tier_rows_after_third,
            vec![("standard".to_string(), 1)],
            "the ledger moves exactly once, on the roll for the genuinely new period"
        );
        assert_eq!(
            third.seats,
            Some(1),
            "grants.seats stays at the same total (1) — a move, not a net change"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// **PR F review, finding 9.** A pending downgrade whose OLD tier has NO
    /// `grant_seat_tiers` row at all (a pre-existing ledger drift — the
    /// scenario finding 6's Stripe-release-failure posture and finding 7's
    /// overhang sweep both name, "the ledger seat left in place" reclaimed
    /// LATER, not by this move) must move NOTHING: the retired blanket-UPDATE
    /// implementation would still have minted the pending count onto the NEW
    /// tier regardless (the old side silently no-op'd, the new side added
    /// unconditionally) — this proves the fixed per-pair transfer instead
    /// moves `min(actual_old_seats, pending_count) == 0`, leaves
    /// `grant_seat_tiers` with NO row for either tier, and leaves
    /// `grants.seats` at whatever it already was (nothing to resync against).
    /// The `account_members` flip and `pending_seat_tier_id` clear still
    /// happen regardless — that half is unconditional and independent of the
    /// ledger's own state (see [`APPLY_PENDING_DOWNGRADES_SQL`]'s own doc
    /// comment).
    #[tokio::test]
    async fn a_pending_downgrade_with_no_old_tier_ledger_row_moves_nothing() {
        let Some(pool) = pg_test_pool(
            "a_pending_downgrade_with_no_old_tier_ledger_row_moves_nothing")
        .await
        else {
            return;
        };
        let (account_id, members) = seed_team_org(&pool, &["premium"]).await;
        let user_id = members[0];
        // Deliberately NOT `seed_purchased_tiers` — the ledger holds NO row
        // for premium (or any tier) at all, simulating a drift this move did
        // not cause (e.g. an earlier Stripe-release failure that left the
        // member removed without a corresponding ledger decrement — findings
        // 6/7's own scenario).
        let grant_id = grant_team(&pool, account_id, 1, None).await;

        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier");
        sqlx::query(
            "UPDATE account_members SET pending_seat_tier_id = $3 \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("record a pending downgrade with nothing behind it on the ledger");

        let (start, end) = period();
        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll must not fail on a pre-existing ledger drift — it logs, not errors");
        drop(conn);

        // The flip itself is unconditional — independent of the ledger.
        let (tier_after, pending_after): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT st.code, am.pending_seat_tier_id FROM account_members am \
             JOIN seat_tiers st ON st.id = am.seat_tier_id \
             WHERE am.account_id = $1 AND am.user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("read post-roll tier");
        assert_eq!(tier_after, "standard", "the member's own tier assignment still flips");
        assert_eq!(pending_after, None);

        // Nothing was minted on either side of the ledger.
        let tier_rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT st.code, gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1",
        )
        .bind(grant_id)
        .fetch_all(&pool)
        .await
        .expect("read grant_seat_tiers after the roll");
        assert!(
            tier_rows.is_empty(),
            "no seats existed on the old tier to move, so NOTHING may appear on the new tier — \
             minting {tier_rows:?} from nothing is exactly finding 9's bug"
        );
        assert_eq!(
            outcome.seats,
            Some(1),
            "grants.seats is untouched — no grant_seat_tiers row exists to resync against \
             (resync_grant_seats' own EXISTS guard skips the write)"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// [`seed_member_period`] must resolve a team-org member's OWN tier
    /// allowance (billing self-serve launch PR E) — not `plans.credits`,
    /// which is `0` for `team` by design (0167's header). Mirrors the
    /// pre-existing `seed_member_period_establishes_a_fresh_window_for_a_brand_new_orgs_first_member`
    /// fixture shape (a zero-member org, a grant issued against nobody yet,
    /// then the first joiner), but for a `team` grant specifically.
    #[tokio::test]
    async fn seed_member_period_grants_a_team_members_own_tier_allowance() {
        let Some(pool) =
            pg_test_pool("seed_member_period_grants_a_team_members_own_tier_allowance").await
        else {
            return;
        };
        let (account_id, mut users) = seed_team_org(&pool, &[]).await;
        grant_team(&pool, account_id, 1, None).await;

        let user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("late-{}@test.example", user_id.simple()))
            .execute(&pool)
            .await
            .expect("create the org's first joiner");
        let premium_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("premium seat tier");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(premium_id)
        .execute(&pool)
        .await
        .expect("add the joiner");
        users.push(user_id);

        let seeded = seed_member_period(&pool, account_id, user_id)
            .await
            .expect("seed_member_period must not error");
        assert!(seeded, "the org's first joiner must be seeded");

        let granted: Decimal = sqlx::query_scalar(
            "SELECT granted_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("read the seeded row");
        assert_eq!(
            granted,
            Decimal::from(15000),
            "must come from the member's OWN tier (premium), not plans.credits (0 for team)"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// A `team` grant carrying `credits_override` deliberately falls back to
    /// the LEGACY divide-evenly-across-RECEIVING-MEMBERS formula rather than
    /// the tier join — an override is an admin's absolute total, and it must
    /// win regardless of which tiers the members actually hold. Roster (2)
    /// and purchased seats (2) are equal here, so this cannot by itself
    /// distinguish "divides by `grants.seats`" from "divides by the roster
    /// count" — see the sibling test below, which deliberately makes them
    /// differ, for that.
    #[tokio::test]
    async fn a_team_grants_credits_override_divides_evenly_ignoring_tiers() {
        let Some(pool) =
            pg_test_pool("a_team_grants_credits_override_divides_evenly_ignoring_tiers").await
        else {
            return;
        };
        let (account_id, members) = seed_team_org(&pool, &["standard", "premium"]).await;
        grant_team(&pool, account_id, 2, Some(Decimal::from(8000))).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        let got = rows(&pool, account_id, start).await;
        assert_eq!(got.len(), 2);
        for (user_id, granted, _) in &got {
            assert_eq!(
                *granted,
                Decimal::from(4000),
                "member {user_id}: 8,000 override / 2 receiving members = 4,000 each, ignoring \
                 that one member is on premium"
            );
        }
        assert_eq!(outcome.rows_granted, 2);

        cleanup(&pool, account_id, &members).await;
    }

    /// **PR F review, finding 3 (supersedes this test's own PR F version,
    /// which asserted the BUG: dividing by purchased seats rather than the
    /// receiving roster, which let a drifted roster mint credits past the
    /// override — a roster of 4 against 1 purchased seat turned an
    /// 8,000-credit override into 32,000 actually granted, `4 × 8,000`).**
    /// The sibling test above seeds `grant_team`'s `seats` (2) equal to the
    /// roster it also seeds (2 members), which cannot distinguish "divides by
    /// `grants.seats`" from "divides by the roster count", because the two
    /// happen to be the same number. This test deliberately makes them
    /// DIFFER — a roster of 4 but only 1 PURCHASED seat (the FOUNDER-CONFIRM
    /// steady state: pre-existing members are never auto-purchased) — to
    /// prove the override divides by the RECEIVING ROSTER (4), not the
    /// purchased total (1): every member still gets granted in full (the
    /// founder's "members == seats, always" roster invariant this module's
    /// own doc describes is about GRANTING, not about what gets divided), Σ
    /// granted stays exactly the override regardless of the purchased/roster
    /// split, and the resulting divergence between `seats` (1) and `members`
    /// (4) is still a real, reportable [`RenewalOutcome::seat_mismatch`] (PR
    /// F un-suppresses it for `team`, see the module doc) — the fix changes
    /// what gets DIVIDED, not whether the drift itself is still surfaced.
    #[tokio::test]
    async fn a_team_grants_credits_override_divides_by_receiving_members_not_purchased_seats() {
        let Some(pool) = pg_test_pool(
            "a_team_grants_credits_override_divides_by_receiving_members_not_purchased_seats",
        )
        .await
        else {
            return;
        };
        let (account_id, members) =
            seed_team_org(&pool, &["standard", "standard", "standard", "premium"]).await;
        // Only 1 seat has actually been PURCHASED; the roster this same call
        // already seeded holds 4 (pre-existing members, never auto-purchased).
        grant_team(&pool, account_id, 1, Some(Decimal::from(8000))).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        let got = rows(&pool, account_id, start).await;
        assert_eq!(got.len(), 4, "every roster member is still granted in full");
        let total: Decimal = got
            .iter()
            .fold(Decimal::ZERO, |acc, (_, granted, _)| acc + *granted);
        assert_eq!(
            total,
            Decimal::from(8000),
            "Σ granted must equal the override EXACTLY (8,000), regardless of the drift between \
             1 purchased seat and 4 roster members — dividing by the purchased total instead \
             would have minted 32,000 (4 x 8,000)"
        );
        for (user_id, granted, _) in &got {
            assert_eq!(
                *granted,
                Decimal::from(2000),
                "member {user_id}: must divide by the receiving roster (4), not grants.seats \
                 (1, purchased) — 8,000 / 4 = 2,000"
            );
        }
        assert_eq!(
            outcome.seats,
            Some(1),
            "grants.seats is left exactly as purchased — roll_period does not touch it absent a \
             pending-downgrade move"
        );
        assert!(
            outcome.seat_mismatch(),
            "seats (1, purchased) and members (4, roster) genuinely disagree, and PR F reports \
             that for a team grant instead of suppressing it — the finding-3 fix changes what \
             gets divided, not whether this drift is still surfaced"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// **PR F (supersedes PR E review round 2, finding 7 — that suppression
    /// is retired, not merely narrowed; see the module doc's "The `team`
    /// seat_mismatch signal is un-suppressed" section) — direction flipped
    /// by CS-435 (D1/D7, org-onboarding plan §3 item 7).** A `team` roll
    /// against a roster of ZERO current members, but a purchase ledger that
    /// already shows purchased seats (a `grant_team`-seeded `grants.seats`
    /// with nobody actually on the roster yet) is now `billing::slot_pool::
    /// SlotPool`'s own headline healthy state — `routes::org_slots`' "buy N
    /// slots" landing before anyone joined — and must NOT report a mismatch.
    /// `roll_period` still never touches `grants.seats` here at all (no
    /// pending-downgrade rows exist for this account), so it stays exactly
    /// at the value `grant_team` seeded — this test also proves that a
    /// `team` roll with no pending downgrades leaves the purchase ledger's
    /// seat total untouched, unrelated to the CS-435 change.
    #[tokio::test]
    async fn a_zero_member_team_roll_with_purchased_seats_is_a_healthy_pre_bought_pool() {
        let Some(pool) =
            pg_test_pool("a_zero_member_team_roll_with_purchased_seats_is_a_healthy_pre_bought_pool")
                .await
        else {
            return;
        };
        let (account_id, members) = seed_team_org(&pool, &[]).await;
        grant_team(&pool, account_id, 3, None).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll");
        drop(conn);

        assert_eq!(outcome.members, 0);
        assert_eq!(
            outcome.seats,
            Some(3),
            "grants.seats is a purchase-ledger figure now — roll_period must leave it exactly \
             as grant_team seeded it when there are no pending downgrades to move"
        );
        assert!(
            !outcome.seat_mismatch(),
            "3 purchased seats against 0 roster members is CS-435's own healthy pre-bought-pool \
             state, not drift — reclaiming or warning on it would cancel slots an admin just bought \
             ahead of anyone joining"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// **Billing self-serve PR F review, CRITICAL finding (outside-diff) —
    /// sibling to [`a_team_grants_credits_override_divides_by_receiving_members_not_purchased_seats`]
    /// (4 roster members) and
    /// [`a_zero_member_team_roll_with_purchased_seats_is_a_healthy_pre_bought_pool`]
    /// (0 roster members, no override) — this is the exact gap between the
    /// two: a ZERO-member roster carrying a `credits_override`.** Before this
    /// finding's fix, `legacy_per_member_credits` divided `credits_override`
    /// by `members` (a bare `COUNT(*)`, `0` here) with no floor, and
    /// `over / Decimal::from(0)` panics — inside `roll_period`'s transaction,
    /// which `apply_invoice_paid` runs inside, so the panic would abort the
    /// webhook task and Stripe would redeliver forever against an input that
    /// can never succeed. `legacy_per_member_credits` now clamps its divisor
    /// to `.max(1)`; this proves the clamp is reached AND that the resulting
    /// (unused, since nothing is granted) value never surfaces as an error —
    /// `roll_period` returns `Ok`, and `PER_MEMBER_GRANT_SQL`'s `INSERT ...
    /// SELECT FROM account_members` writes zero rows for a zero-member
    /// roster regardless of what the override's ARITHMETIC would have been.
    #[tokio::test]
    async fn a_zero_member_team_grant_with_credits_override_rolls_without_dividing_by_zero() {
        let Some(pool) = pg_test_pool(
            "a_zero_member_team_grant_with_credits_override_rolls_without_dividing_by_zero",
        )
        .await
        else {
            return;
        };
        let (account_id, members) = seed_team_org(&pool, &[]).await;
        grant_team(&pool, account_id, 3, Some(Decimal::from(8000))).await;
        let (start, end) = period();

        let mut conn = pool.acquire().await.expect("acquire");
        let outcome = roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("roll must succeed, not panic on a zero-member override divisor");
        drop(conn);

        let got = rows(&pool, account_id, start).await;
        assert_eq!(got.len(), 0, "a zero-member roster receives no member_allowance_periods rows");
        assert_eq!(outcome.members, 0);
        assert_eq!(
            outcome.seats,
            Some(3),
            "grants.seats is left exactly as grant_team seeded it — no pending downgrades to move"
        );
        assert!(
            !outcome.seat_mismatch(),
            "3 purchased seats against 0 roster members is CS-435's own healthy pre-bought-pool \
             state, not drift (see billing::slot_pool's own module doc)"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// **PR F (supersedes PR E review round 2, finding B, whose test proved
    /// `seed_member_period` ITSELF reconciled `grant_seat_tiers` to make
    /// entitlement correct — that mechanism no longer exists).** The
    /// account-level entitlement total is now correct the moment a seat is
    /// actually PURCHASED (`billing::seat_lifecycle::acquire_seat`,
    /// simulated here via [`seed_purchased_tiers`] the way this file's other
    /// ledger-aware tests do), independent of whether/when
    /// `seed_member_period` happens to run for that same member —
    /// proving the two are decoupled, not that one causes the other. A
    /// member's OWN mid-period allowance (via [`MEMBER_SEAT_TIER_CREDITS_SQL`])
    /// still seeds correctly regardless, since that join never touched
    /// `grant_seat_tiers` even under PR E.
    #[tokio::test]
    async fn seed_member_period_reads_the_purchased_ledger_independent_of_the_roster() {
        let Some(pool) = pg_test_pool(
            "seed_member_period_reads_the_purchased_ledger_independent_of_the_roster")
        .await
        else {
            return;
        };
        let (account_id, mut users) = seed_team_org(&pool, &[]).await;
        let grant_id = grant_team(&pool, account_id, 1, None).await;
        // The seat is already PURCHASED (as `acquire_seat` would have left
        // it) BEFORE the member row or the seed call below ever runs.
        seed_purchased_tiers(&pool, account_id, grant_id, &[("standard", 1)]).await;

        let user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("first-joiner-{}@test.example", user_id.simple()))
            .execute(&pool)
            .await
            .expect("create the org's first joiner");
        let standard_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(&pool)
        .await
        .expect("standard seat tier");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("add the joiner");
        users.push(user_id);

        // Entitlement is ALREADY correct — the purchase, not this seed call,
        // is what made it so.
        let entitlement_before = crate::billing::entitlement::current_plan(&pool, account_id)
            .await
            .expect("entitlement query")
            .expect("the team grant exists");
        assert_eq!(
            entitlement_before.credits, 3000.0,
            "entitlement reflects the purchased ledger already, before seed_member_period ever \
             runs for this member"
        );

        let seeded = seed_member_period(&pool, account_id, user_id)
            .await
            .expect("seed_member_period must not error");
        assert!(seeded, "the org's first joiner must be seeded");

        // Still correct afterward, and grant_seat_tiers/grants.seats are
        // BYTE-FOR-BYTE what this test seeded — seed_member_period touched
        // neither.
        let entitlement_after = crate::billing::entitlement::current_plan(&pool, account_id)
            .await
            .expect("entitlement query")
            .expect("the team grant exists");
        assert_eq!(entitlement_after.credits, 3000.0);
        assert!(
            entitlement_after.credits_per_seat.is_none(),
            "a team grant has no single per-seat figure to render"
        );

        let tier_rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT st.code, gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1",
        )
        .bind(grant_id)
        .fetch_all(&pool)
        .await
        .expect("read grant_seat_tiers");
        assert_eq!(
            tier_rows,
            vec![("standard".to_string(), 1)],
            "seed_member_period must not touch grant_seat_tiers at all (PR F)"
        );

        let seats_column: i32 = sqlx::query_scalar(
            "SELECT seats FROM grants WHERE account_id = $1 AND revoked_at IS NULL",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("read grants.seats");
        assert_eq!(
            seats_column, 1,
            "grants.seats must stay exactly at the purchased total seed_member_period found it \
             at — not reconciled to the (also 1-member, coincidentally equal) roster"
        );

        // And this member's OWN mid-period allowance still seeded correctly
        // — that join never depended on grant_seat_tiers, even under PR E.
        let granted: Decimal = sqlx::query_scalar(
            "SELECT granted_credits FROM member_allowance_periods \
             WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("read the seeded row");
        assert_eq!(granted, Decimal::from(3000));

        cleanup(&pool, account_id, &users).await;
    }

    /// Seeds/updates the account's `stripe_subscriptions` row status — same
    /// helper shape as `billing::enforcement::pg_tests::set_subscription_status`.
    async fn set_subscription_status(pool: &PgPool, account_id: Uuid, status: &str) {
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, status) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (account_id) DO UPDATE SET status = EXCLUDED.status",
        )
        .bind(account_id)
        .bind(format!("cus_renewal_seed_{}", Uuid::new_v4().simple()))
        .bind(status),
        )
        .await
        .expect("seed stripe_subscriptions status");
    }

    /// **Regression test for finding 4 (billing self-serve PR D review
    /// round).** A member joining an account whose subscription is
    /// `canceled` must NOT receive a spendable mid-period allowance — that
    /// would defeat freeze-not-forfeit (plan §1.11) at the one call site that
    /// never consults `billing::enforcement::subscription_frozen`.
    #[tokio::test]
    async fn seed_member_period_skips_a_canceled_subscription() {
        let Some(pool) = pg_test_pool("seed_member_period_skips_a_canceled_subscription").await
        else {
            return;
        };
        let (account_id, mut members) = seed_account(&pool, 1).await;
        grant(&pool, account_id, "team_standard", Some(1), None).await;
        let (start, end) = current_window();
        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("establish the open period");
        drop(conn);
        set_subscription_status(&pool, account_id, "canceled").await;

        let latecomer = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(latecomer)
            .bind(format!("seed-frozen-{}@test.example", latecomer.simple()))
            .execute(&pool)
            .await
            .expect("create latecomer");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(latecomer)
        .execute(&pool)
        .await
        .expect("add latecomer");
        members.push(latecomer);

        let inserted = seed_member_period(&pool, account_id, latecomer)
            .await
            .expect("seed_member_period must not error even when it skips");
        assert!(
            !inserted,
            "a canceled subscription must skip the mid-period seed entirely"
        );

        let row_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM member_allowance_periods WHERE account_id = $1 AND user_id = $2)",
        )
        .bind(account_id)
        .bind(latecomer)
        .fetch_one(&pool)
        .await
        .expect("check for a seeded row");
        assert!(
            !row_exists,
            "no allowance row must exist for the latecomer while the subscription is frozen"
        );

        cleanup(&pool, account_id, &members).await;
    }

    /// Same scenario, `past_due` (an involuntary lapse, subscription still
    /// alive) rather than `canceled` — both freeze identically, matching
    /// `billing::enforcement::subscription_frozen`'s own two-value gate.
    #[tokio::test]
    async fn seed_member_period_skips_a_past_due_subscription() {
        let Some(pool) = pg_test_pool("seed_member_period_skips_a_past_due_subscription").await
        else {
            return;
        };
        let (account_id, mut members) = seed_account(&pool, 1).await;
        grant(&pool, account_id, "team_standard", Some(1), None).await;
        let (start, end) = current_window();
        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("establish the open period");
        drop(conn);
        set_subscription_status(&pool, account_id, "past_due").await;

        let latecomer = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(latecomer)
            .bind(format!("seed-pastdue-{}@test.example", latecomer.simple()))
            .execute(&pool)
            .await
            .expect("create latecomer");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(latecomer)
        .execute(&pool)
        .await
        .expect("add latecomer");
        members.push(latecomer);

        let inserted = seed_member_period(&pool, account_id, latecomer)
            .await
            .expect("seed_member_period must not error even when it skips");
        assert!(!inserted, "a past_due subscription must also skip the mid-period seed");

        cleanup(&pool, account_id, &members).await;
    }

    /// **Positive control** — an ACTIVE subscription (or none linked at all)
    /// must keep seeding exactly as before this check existed. Without this
    /// alongside the two freeze tests above, a bug that skipped EVERY
    /// account regardless of status would also pass.
    #[tokio::test]
    async fn seed_member_period_still_seeds_an_active_subscription() {
        let Some(pool) = pg_test_pool("seed_member_period_still_seeds_an_active_subscription").await
        else {
            return;
        };
        let (account_id, mut members) = seed_account(&pool, 1).await;
        grant(&pool, account_id, "team_standard", Some(1), None).await;
        let (start, end) = current_window();
        let mut conn = pool.acquire().await.expect("acquire");
        roll_period_fixture(&mut conn, account_id, start, end)
            .await
            .expect("establish the open period");
        drop(conn);
        set_subscription_status(&pool, account_id, "active").await;

        let latecomer = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(latecomer)
            .bind(format!("seed-active-{}@test.example", latecomer.simple()))
            .execute(&pool)
            .await
            .expect("create latecomer");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(latecomer)
        .execute(&pool)
        .await
        .expect("add latecomer");
        members.push(latecomer);

        let inserted = seed_member_period(&pool, account_id, latecomer)
            .await
            .expect("seed_member_period must not error");
        assert!(inserted, "an active subscription must not be gated by this check at all");

        cleanup(&pool, account_id, &members).await;
    }
}
