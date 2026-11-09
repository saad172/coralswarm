//! Billing / usage-metering support (usage-metering PR1: DB-versioned rate
//! sheet). See [`rates`] for the `PriceSheet` loader that replaced the
//! hardcoded rate constants previously in `routes::admin`, and
//! [`credit_conversion`] for the credit-to-USD reference rate PR8/PR9 read.
//! [`enforcement`] (PR10) is the balance-enforcement observe-only flag, and
//! [`unknown_model_alert`] (PR11) is the once-per-outage dedup layered on top
//! of `rates::PriceSheet::rate`'s raw unknown-model signal. [`stripe_client`]
//! (billing self-serve launch, PR C) is the OUTBOUND half of the Stripe
//! integration — the hand-rolled REST client PR D/F/G/H's checkout, seat,
//! refill, and billing-management routes consume; the inbound half stays at
//! `routes::stripe_webhook`. [`seat_lifecycle`] (PR F) is the seat
//! acquire/release purchase-ledger writer wired from
//! `clerk_provision::ensure_org_membership`/`remove_org_membership`; the
//! tier-CHANGE surface lives at `routes::billing_seats` instead (a real
//! in-app route, not a Clerk-event side effect — see that module's own doc
//! comment for why). [`slot_pool`] (CS-435, org-onboarding plan §3 items
//! 7/15) is the derived purchased/used/reserved arithmetic and the
//! `slot_reservations` reader/writer pair `acquire_seat`'s consume-before-buy
//! branch and `routes::org_slots`/`workers::slot_reservations` build on.
//! [`org_trial`] (self-serve signup PR C′) is a new team org's free-credit
//! trial: its lifecycle state machine, pool, and conversion to paid.

pub mod cancel;
pub mod credit_conversion;
pub mod entitlement;
pub mod enforcement;
pub(crate) mod entry_queue;
pub mod funding_policy;
pub mod extra_usage;
pub mod issuance;
pub mod me;
pub(crate) mod managed_reservation;
pub mod notices;
pub mod org_trial;
pub mod rates;
pub mod renewal;
pub mod seat_lifecycle;
pub mod slot_pool;
pub mod stripe_client;
#[cfg(test)]
pub(crate) mod test_support;
pub mod unknown_model_alert;

/// The `plans.code` value for the one MIXED-TIER plan this schema defines —
/// `team`, which unlike every other plan holds a mix of `standard`/`premium`
/// seats per account rather than one single price point (migration `0167`).
/// Three otherwise-uncoupled sites need to agree on exactly this string:
/// [`entitlement::PLAN_ENTITLEMENT_SQL`] (which grant's total comes from
/// `grant_seat_tiers` instead of `plans.credits`), [`renewal`]'s
/// `CURRENT_GRANT_SQL` (which grant's per-member allowance comes from the
/// seat-tier join instead of the legacy scalar), and `bin/grant_plan.rs`
/// (which refuses to issue this plan by hand, since its single `--seats <n>`
/// cannot express a per-tier breakdown). A typo in any one of the three used
/// to be silently possible — each site independently spelled the literal
/// `"team"` — and this constant turns that into a compile error instead
/// (billing self-serve launch PR E, code review round 2, finding 8).
///
/// A schema-level signal (e.g. a `plans.is_mixed_tier` column, denormalized
/// onto `grants` the same way `0156` did for `plan_is_seat_priced`) is the
/// eventual right home for this fact — worth revisiting once PR F's
/// seat-lifecycle routes give a second WRITER a reason to ask the same
/// question. Today all three sites above are read-only, so a shared Rust
/// constant is the cheapest fix that actually closes the drift risk.
pub const MIXED_TIER_PLAN_CODE: &str = "team";
