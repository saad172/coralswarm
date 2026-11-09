//! The plan-entitlement query — ONE definition, used by the production
//! handler and by the tests that claim to prove it.
//!
//! # Why this module exists
//!
//! Migration `0156_grants_seats.sql` states the entitlement formula once:
//!
//! ```text
//! COALESCE(grants.credits_override, plans.credits * COALESCE(grants.seats, 1))
//! ```
//!
//! The first version of this change implemented that formula inline in
//! [`crate::routes::usage`] and then RE-TYPED IT as a `const` inside
//! `tests/billing_grant_seats_pg.rs`, with a comment inviting the reader to
//! diff the two by eye. That was a real hole, and review caught it: the
//! tests exercised the COPY, never the query the handler actually runs, so
//! a regression in the handler would have left all six tests green. Eyeball
//! diffing is not a test.
//!
//! Extracting the query here fixes that by construction. There is now
//! nothing to keep in sync: [`current_plan`] IS the production code path,
//! and the tests call this same function. A regression in the formula fails
//! the suite because the suite runs the formula.
//!
//! # A note on what this deliberately does NOT decide
//!
//! [`PlanEntitlement::credits`] is the account's TOTAL allowance under the
//! pooled reading of a seat-priced plan (`credits * seats`). The founder has
//! since chosen HARD PER-SEAT CAPS — each member limited individually, no
//! borrowing another member's headroom — which makes this figure a
//! DISPLAY total rather than the enforcement boundary. Enforcing it per
//! member requires per-user usage attribution that does not exist yet
//! (`llm_usage` has no `user_id`; `credit_balances` is keyed by
//! `account_id` alone), so that work lands separately. Nothing here should
//! be read as "the pool a member may spend against".
//!
//! # `team` grants: the total comes from `grant_seat_tiers`, not `plans.credits`
//! (billing self-serve launch PR E)
//!
//! `plans.credits * COALESCE(g.seats, 1)` is exactly right for a flat plan
//! and for the single-price-point `team_standard`/`team_premium` rows
//! (0155/0167, retiring but still able to renew an existing grant). It is
//! wrong by construction for the `team` umbrella plan: `team.credits = 0`
//! always (0167's header — a deliberate "read the join instead" placeholder,
//! never a real total), and a `team` grant can mix `standard`/`premium`
//! seats, so there is no single per-seat figure to multiply in the first
//! place. [`PLAN_ENTITLEMENT_SQL`] detects `p.code = 'team'` and instead sums
//! `grant_seat_tiers.seats * seat_tiers.credits` for this grant — the exact
//! formula 0168's header names as the reason that table exists ("Entitlement
//! total = Σ(seats × tier.credits)"), and the same rows
//! `billing::renewal::roll_period` keeps reconciled from the live
//! `account_members` roster at every period roll (see that module's doc
//! comment). `credits_override` still short-circuits the whole computation
//! for `team` exactly as it does for every other plan — an override is an
//! admin's absolute total regardless of how the underlying seats are priced.
//!
//! `credits_per_seat` is `NULL` for a `team` grant unconditionally (not just
//! when `credits_override` is set) — a MIXED-tier grant has no single
//! per-seat figure to render ("3,000 × 4 seats" is meaningless when some of
//! those 4 seats are actually 15,000/seat), the identical reasoning
//! [`PlanEntitlement::credits_per_seat`]'s own doc comment already gives for
//! the override case, extended to cover the other way a single figure can be
//! wrong.

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// The account's current (most recently granted, non-revoked) plan, with
/// the entitlement already computed in SQL.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PlanEntitlement {
    pub code: String,
    pub name: String,
    /// TOTAL allowance — already multiplied by [`Self::seats`]. A caller
    /// must NOT multiply again.
    pub credits: f64,
    /// Purchased seats for a seat-priced plan, `None` for a flat one.
    /// Migration `0156`'s `grants_seats_iff_seat_priced` makes those the
    /// only two possibilities, so `None` means "flat plan", never "unknown".
    pub seats: Option<i32>,
    /// The plan's PER-SEAT allowance, for rendering "3,000 × 4 seats".
    ///
    /// `None` in two distinct cases, both deliberate:
    ///   * a flat plan (no seats to break down), and
    ///   * **a grant carrying `credits_override`** — an override is an
    ///     ABSOLUTE total that wins over the multiplication entirely, so a
    ///     per-seat figure alongside it is not just redundant but
    ///     CONTRADICTORY: a client would render "4 seats × 3,000" next to a
    ///     total of 7,000. Suppressing it here is what keeps the API
    ///     self-consistent, rather than leaving each client to notice the
    ///     trap independently.
    pub credits_per_seat: Option<f64>,
    pub refresh_kind: String,
    pub granted_at: DateTime<Utc>,
}

/// `COALESCE(g.seats, 1)` is what makes one expression correct for both
/// plan shapes: seat-priced grants always carry a seat count and flat ones
/// never do (`grants_seats_iff_seat_priced`), so the COALESCE branch is
/// reached only for flat plans, where multiplying by 1 is a no-op. An
/// override short-circuits the multiplication because it is an absolute
/// total — see `0156`'s header.
///
/// **PR E (billing self-serve launch):** the `CASE WHEN p.code = $2`
/// arm inside the `credits` expression is the Σ(seats × tier.credits) total
/// this module's own doc comment describes — a correlated subquery over
/// `grant_seat_tiers JOIN seat_tiers`, scoped to THIS grant by `gst.grant_id
/// = g.id`. `COALESCE(..., 0)` inside that subquery matters: a `team`
/// account with no `grant_seat_tiers` rows yet (zero current members — see
/// `billing::renewal::roll_period`'s reconciliation, which only ever writes
/// rows for tiers that currently have members) must read as `0`, the same
/// loud "nothing to grant yet" value `plans.credits = 0` already reads as
/// for every `team` grant before PR E — a bare `SUM()` over zero rows would
/// instead produce SQL NULL, which `COALESCE(g.credits_override, NULL)`
/// would then propagate as the OUTER `credits` value being NULL whenever
/// `credits_override` is also NULL, turning "priced correctly at zero" into
/// "failed to resolve a price" for the exact account this arm exists to
/// serve.
///
/// `$2` is bound to [`crate::billing::MIXED_TIER_PLAN_CODE`] rather than the
/// literal `'team'` — see that constant's own doc comment (review round 2,
/// finding 8) for why this and two other otherwise-uncoupled sites must
/// agree on exactly one string.
///
/// `ORDER BY g.granted_at DESC, g.plan_is_seat_priced DESC, g.id DESC`: the
/// tie-break matters because `granted_at` uses tx-stable `now()`, so a flat
/// and a seat-priced grant issued in the same transaction can tie exactly,
/// leaving Postgres free to pick either as "current". `g.id DESC` breaks any
/// further tie deterministically — `id` is a random `gen_random_uuid()`, not
/// time-ordered, so it doesn't mean "newest", it just guarantees this query
/// and `enforcement::read_member_period_scoped` / `renewal::CURRENT_GRANT_SQL`
/// resolve the SAME row from the same snapshot.
const PLAN_ENTITLEMENT_SQL: &str = "\
    SELECT p.code, p.name, \
           COALESCE(g.credits_override, \
                     CASE WHEN p.code = $2 THEN ( \
                         SELECT COALESCE(SUM(gst.seats * st.credits), 0) \
                           FROM grant_seat_tiers gst JOIN seat_tiers st ON st.id = gst.seat_tier_id \
                          WHERE gst.grant_id = g.id \
                     ) ELSE p.credits * COALESCE(g.seats, 1) END \
                    )::float8 AS credits, \
           g.seats, \
           CASE WHEN p.code = $2 OR g.seats IS NULL OR g.credits_override IS NOT NULL \
                THEN NULL ELSE p.credits::float8 END AS credits_per_seat, \
           p.refresh_kind, g.granted_at \
      FROM grants g JOIN plans p ON p.id = g.plan_id \
     WHERE g.account_id = $1 AND g.revoked_at IS NULL \
     ORDER BY g.granted_at DESC, g.plan_is_seat_priced DESC, g.id DESC LIMIT 1";

/// Resolves the account's current plan entitlement.
///
/// Generic over the executor so the handler can pass its open
/// account-scoped transaction (`&mut *tx`) while tests pass a pool — the
/// same query either way, which is the entire point of this module.
pub async fn current_plan<'e, E>(
    executor: E,
    account_id: Uuid,
) -> Result<Option<PlanEntitlement>, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query_as::<_, PlanEntitlement>(PLAN_ENTITLEMENT_SQL)
        .bind(account_id)
        .bind(crate::billing::MIXED_TIER_PLAN_CODE)
        .fetch_optional(executor)
        .await
}
