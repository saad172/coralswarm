//! The free-credit trial a self-serve team org gets when its creator
//! completes the card-up-front Stripe Checkout (self-serve signup PR C′;
//! founder decisions 2026-10-27, #1, #6 and #9).
//!
//! # The product
//!
//! * The creator picks a seat count (at least 2) and goes through Stripe
//!   Checkout, which takes a card and charges $0.
//! * Checkout completion grants **3,000 credits as one flat org pool**,
//!   whatever the seat count, and starts tenant provisioning.
//! * Extra usage and auto-reload are refused until the first successful
//!   charge.
//! * The owner is emailed at 80% and 100% used. At 100% the trial converts:
//!   the subscription starts, the card is charged for the seats, and the
//!   normal first-month allowance is granted at once.
//! * 90-day backstop: an unconverted trial converts anyway, with an email
//!   7 days before.
//! * One trial per creator user AND per card fingerprint. A repeat creator
//!   gets a Checkout with no trial (pay the seats on day 1); a repeat card is
//!   only known once Checkout completes, so that trial is ended at once.
//!
//! # The Stripe mechanism
//!
//! A `mode=subscription` Checkout for `quantity = seats` of the `standard`
//! seat price, with `subscription_data[trial_period_days]=90`,
//! `payment_method_collection=always` and
//! `trial_settings[end_behavior][missing_payment_method]=cancel`. The
//! subscription exists (status `trialing`) from Checkout completion, with
//! the card saved as its default payment method. Converting is ONE call,
//! `POST /v1/subscriptions/{id}` with `trial_end=now`: Stripe invoices the
//! seats and charges the saved card immediately. Stripe's own `trial_end`
//! (90 days) is the backstop of the backstop: if this worker never runs,
//! Stripe still ends the trial and charges on day 90, and `invoice.paid`
//! finalizes the conversion here. See the PR body for the comparison with a
//! setup-mode Checkout.
//!
//! # How the pool is spent
//!
//! During the trial the org holds its real `team` grant (so slots, invites
//! and `seat_lifecycle` work exactly as for a paid org) with
//! `credits_override = 0`: every member's period is granted 0 credits. The
//! 3,000 credits are a `credit_ledger` row (`entry_kind = 'trial_credit'`)
//! that raises `credit_balances.balance`, the account pool every plan debit
//! already draws down. [`crate::billing::funding_policy::db::load_period`]
//! leaves the pool UNCAPPED per member while the account is `trialing`, so
//! any member can spend any of it. Conversion clears the override, closes
//! the trial periods at the new billing period's start, rolls the normal
//! per-tier allowance, and expires any credits left in the pool.
//!
//! # The lifecycle
//!
//! One explicit state per org (`org_trials.state`), see [`TrialState`]. The
//! decisions below are pure functions so the rules are tested without a
//! database; [`db`] applies them under the account's financial lock
//! (`renewal::lock_period_rotation`, the same lock every usage debit takes),
//! which is what makes conversion safe under concurrent debits: the
//! `trialing -> converting` step and the finalize step are each a guarded
//! `UPDATE ... WHERE state = <expected>` inside that lock, so two workers,
//! or a worker and a webhook, cannot both convert.

pub(crate) mod db;
pub(crate) mod lifecycle;
#[cfg(test)]
mod pg_tests;
#[cfg(test)]
mod tests;

use chrono::{DateTime, Duration, Utc};
use sqlx::types::Decimal;

/// Credits in the trial pool, per org, whatever the seat count.
pub const TRIAL_CREDITS: i64 = 3_000;
/// Stripe's `trial_period_days`: the 90-day backstop.
pub const TRIAL_DAYS: i64 = 90;
/// The backstop warning goes out this many days before the trial ends.
pub const ENDING_SOON_DAYS: i64 = 7;
/// An abandoned Checkout keeps its pending org (and "resume payment") this long.
pub const CHECKOUT_RESUME_HOURS: i64 = 24;
/// Team orgs start at two seats (decision #5).
pub const MIN_TRIAL_SEATS: i64 = 2;
/// Sanity ceiling, well under Stripe's line-item quantity limit.
pub const MAX_TRIAL_SEATS: i64 = 1_000;
/// The seat tier a self-serve org buys at Checkout (`team_standard`).
pub const TRIAL_SEAT_TIER: &str = "standard";
/// `credit_ledger.entry_kind` of the trial pool credit.
pub const ENTRY_KIND_TRIAL_CREDIT: &str = "trial_credit";
/// `credit_ledger.entry_kind` that expires credits left at conversion.
pub const ENTRY_KIND_TRIAL_CREDIT_EXPIRED: &str = "trial_credit_expired";

/// `org_trials.state`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TrialState {
    /// The org exists and a trial Checkout was issued; nothing paid yet.
    CheckoutOpen,
    /// Checkout completed with a trial: the pool is live, nothing charged.
    Trialing,
    /// The trial is ending (credits used, backstop, or a repeat card): the
    /// seats are being charged. No allowance until the charge succeeds.
    Converting,
    /// The first charge succeeded and the normal allowance was granted.
    Converted,
    /// The Checkout was not completed within 24 hours.
    Abandoned,
    /// The subscription ended (or a cancel was scheduled) before the
    /// conversion charge was paid. A conversion invoice paid anyway after
    /// this (the customer settled it before the void landed) still converts
    /// the org: see [`TrialState::can_become`].
    Canceled,
}

impl TrialState {
    pub const ALL: [TrialState; 6] = [
        TrialState::CheckoutOpen,
        TrialState::Trialing,
        TrialState::Converting,
        TrialState::Converted,
        TrialState::Abandoned,
        TrialState::Canceled,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            TrialState::CheckoutOpen => "checkout_open",
            TrialState::Trialing => "trialing",
            TrialState::Converting => "converting",
            TrialState::Converted => "converted",
            TrialState::Abandoned => "abandoned",
            TrialState::Canceled => "canceled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.as_str() == s)
    }

    /// The transition table. Every write in [`db`] checks it.
    pub const fn can_become(self, to: TrialState) -> bool {
        use TrialState::*;
        matches!(
            (self, to),
            (CheckoutOpen | Abandoned, Trialing | Converting | Converted)
                | (CheckoutOpen, Abandoned)
                | (Trialing, Converting | Canceled)
                | (Converting, Converted | Canceled)
                // A trial canceled while `converting` whose conversion
                // invoice is paid anyway: the customer paid, so the
                // allowance is granted. `db::finalize_conversion` only takes
                // this edge for a row that was converting (it carries a
                // `convert_reason`); a trial canceled from `trialing` never
                // had a conversion charge to pay.
                | (Canceled, Converted)
        )
    }

    /// No further transition is possible.
    pub const fn is_terminal(self) -> bool {
        matches!(self, TrialState::Converted)
    }

    /// Decision #6: extra usage and auto-reload stay off until the first
    /// successful charge. `first_charge_seen` is `org_trials.first_charge_at
    /// IS NOT NULL`: a trial that ended `canceled` (or an abandoned Checkout)
    /// never charged the card, so the org stays refused until a later
    /// subscription's first paid invoice stamps it.
    pub const fn refuses_extra_usage(self, first_charge_seen: bool) -> bool {
        match self {
            TrialState::CheckoutOpen | TrialState::Trialing | TrialState::Converting => true,
            TrialState::Canceled | TrialState::Abandoned => !first_charge_seen,
            TrialState::Converted => false,
        }
    }
}

/// `org_trials.convert_reason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConvertReason {
    CreditsUsed,
    Backstop,
    RepeatCard,
    NoTrial,
    /// The card's fingerprint could not be read at completion, so the
    /// one-trial-per-card rule could not be checked: fail closed, charge now.
    CardUnverified,
}

impl ConvertReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            ConvertReason::CreditsUsed => "credits_used",
            ConvertReason::Backstop => "backstop",
            ConvertReason::RepeatCard => "repeat_card",
            ConvertReason::NoTrial => "no_trial",
            ConvertReason::CardUnverified => "card_unverified",
        }
    }
}

/// What `checkout.session.completed` does for a self-serve org.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionPlan {
    /// First trial: team grant at 0 credits, trial pool, `trialing`.
    StartTrial,
    /// Trialing subscription, but the user or card already had a trial:
    /// team grant at 0 credits, no pool, `converting` (ended at Stripe now).
    EndTrialNow,
    /// The subscription is already charged: the normal team grant,
    /// `converted`.
    PaidNow,
    /// The lifecycle already moved past checkout (a second event for the
    /// same session): nothing to do.
    AlreadyApplied,
}

impl CompletionPlan {
    pub fn next_state(self) -> TrialState {
        match self {
            CompletionPlan::StartTrial => TrialState::Trialing,
            CompletionPlan::EndTrialNow => TrialState::Converting,
            CompletionPlan::PaidNow => TrialState::Converted,
            CompletionPlan::AlreadyApplied => {
                unreachable!("AlreadyApplied writes nothing; callers must check it first")
            }
        }
    }

    pub fn grants_trial_pool(self) -> bool {
        self == CompletionPlan::StartTrial
    }

    pub fn convert_reason(self) -> Option<ConvertReason> {
        match self {
            CompletionPlan::EndTrialNow => Some(ConvertReason::RepeatCard),
            CompletionPlan::PaidNow => Some(ConvertReason::NoTrial),
            CompletionPlan::StartTrial | CompletionPlan::AlreadyApplied => None,
        }
    }

    /// The team grant's `credits_override`: 0 until the seats are paid.
    pub fn credits_override(self) -> Option<Decimal> {
        match self {
            CompletionPlan::StartTrial | CompletionPlan::EndTrialNow => Some(Decimal::ZERO),
            CompletionPlan::PaidNow | CompletionPlan::AlreadyApplied => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TransitionError {
    pub from: TrialState,
    pub event: &'static str,
}

impl std::fmt::Display for TransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "trial in state {} cannot take {}", self.from.as_str(), self.event)
    }
}

/// Checkout completed.
///
/// * `eligible_at_open`: the creator had no trial when the session was made.
/// * `subscription_trialing`: Stripe put the subscription in trial.
/// * `claim_won`: `org_trial_claim` recorded the user and card claims (false
///   when either already had a trial). Only consulted when trialing.
pub fn on_checkout_completed(
    state: TrialState,
    eligible_at_open: bool,
    subscription_trialing: bool,
    claim_won: bool,
) -> Result<CompletionPlan, TransitionError> {
    match state {
        TrialState::CheckoutOpen | TrialState::Abandoned => {}
        TrialState::Trialing | TrialState::Converting | TrialState::Converted => {
            return Ok(CompletionPlan::AlreadyApplied)
        }
        TrialState::Canceled => {
            return Err(TransitionError { from: state, event: "checkout_completed" })
        }
    }
    Ok(if !subscription_trialing {
        CompletionPlan::PaidNow
    } else if eligible_at_open && claim_won {
        CompletionPlan::StartTrial
    } else {
        CompletionPlan::EndTrialNow
    })
}

/// A `trialing` org's usage, as the worker reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct UsageInput {
    pub credits: Decimal,
    pub used: Decimal,
    pub now: DateTime<Utc>,
    pub trial_ends_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UsageDecision {
    pub notify_80: bool,
    pub notify_used: bool,
    pub notify_ending_soon: bool,
    pub convert: Option<ConvertReason>,
}

/// What a `trialing` org's usage and clock call for. Notices are idempotent
/// claims, so "still true" is the right test, not "just crossed".
pub fn on_usage(u: &UsageInput) -> UsageDecision {
    let used_up = u.used >= u.credits;
    let eighty = u.used * Decimal::from(5) >= u.credits * Decimal::from(4);
    let backstop_due = u.now >= u.trial_ends_at;
    let ending_soon = u.now >= u.trial_ends_at - Duration::days(ENDING_SOON_DAYS);
    UsageDecision {
        notify_80: eighty,
        notify_used: used_up,
        notify_ending_soon: ending_soon && !used_up,
        convert: if used_up {
            Some(ConvertReason::CreditsUsed)
        } else if backstop_due {
            Some(ConvertReason::Backstop)
        } else {
            None
        },
    }
}

/// The Stripe subscription as the conversion step needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionFacts {
    pub status: String,
    /// `cancel_at_period_end` or a `cancel_at` is set.
    pub cancel_scheduled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversionStep {
    /// Still trialing at Stripe: end the trial there (charges the seats).
    EndTrialAtStripe,
    /// Stripe charged the seats: grant the normal allowance.
    Finalize,
    /// The charge has not succeeded yet (dunning). Nothing to do but wait.
    AwaitPayment,
    /// The customer canceled (or scheduled a cancel) before paying: never
    /// charge, end the trial.
    Cancel,
}

/// `active` is checked BEFORE a scheduled cancel: an `active` subscription
/// has paid for its period, and a cancel scheduled on it takes effect at
/// that period's end. Finalizing grants the allowance the customer paid
/// for; cancelling here would charge them and grant nothing.
pub fn conversion_step(s: &SubscriptionFacts) -> ConversionStep {
    match s.status.as_str() {
        "canceled" | "incomplete_expired" => ConversionStep::Cancel,
        "active" => ConversionStep::Finalize,
        _ if s.cancel_scheduled => ConversionStep::Cancel,
        "trialing" => ConversionStep::EndTrialAtStripe,
        _ => ConversionStep::AwaitPayment,
    }
}

/// Whether the conversion charge was paid, as far as Stripe can tell us.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChargeOutcome {
    /// The seats were paid: finalize.
    Paid,
    /// Nothing was collected and nothing will be collected automatically
    /// (the invoice is `void`/`uncollectible`, or the subscription never
    /// left its trial or is already ended): cancel.
    NotPaid,
    /// The invoice is still collectable (`open`, `draft`, or a subscription
    /// in dunning with no invoice read): wait.
    Pending,
}

/// #1528 S1: decide whether a conversion was paid.
///
/// When the conversion invoice was read, its status ALONE decides:
/// `paid` means paid, `void`/`uncollectible` means not paid, anything else
/// is pending. The subscription's status is used only when no invoice was
/// read. Stripe can move a `past_due` subscription back to `active` when
/// its latest invoice is voided or marked uncollectible, so `active` next
/// to an unpaid invoice is not a payment, and trusting it would grant a
/// full first-month allowance on a void invoice.
pub fn charge_outcome(invoice_status: Option<&str>, subscription_status: &str) -> ChargeOutcome {
    match invoice_status {
        Some("paid") => ChargeOutcome::Paid,
        Some("void" | "uncollectible") => ChargeOutcome::NotPaid,
        Some(_) => ChargeOutcome::Pending,
        None => match subscription_status {
            "active" => ChargeOutcome::Paid,
            "trialing" | "canceled" | "incomplete_expired" => ChargeOutcome::NotPaid,
            _ => ChargeOutcome::Pending,
        },
    }
}

/// A trial whose worker pass keeps failing this long after it asked for its
/// conversion charge raises an operator alert (#1528 S3).
pub const CONVERTING_ALERT_HOURS: i64 = 24;

/// #1528 S3: a failing pass on a trial `converting` since `requested_at`
/// escalates to an alert once that is more than [`CONVERTING_ALERT_HOURS`]
/// ago. Until then a failure is a warning: one Stripe blip is not news.
pub fn converting_overdue(requested_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now - requested_at > Duration::hours(CONVERTING_ALERT_HOURS)
}

/// Past the 24-hour "resume payment" window.
pub fn checkout_abandoned(opened_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now >= opened_at + Duration::hours(CHECKOUT_RESUME_HOURS)
}

pub fn validate_seats(seats: i64) -> Result<i64, String> {
    if (MIN_TRIAL_SEATS..=MAX_TRIAL_SEATS).contains(&seats) {
        Ok(seats)
    } else {
        Err(format!("seats must be between {MIN_TRIAL_SEATS} and {MAX_TRIAL_SEATS}"))
    }
}
