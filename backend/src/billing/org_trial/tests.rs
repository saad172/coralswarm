//! The trial lifecycle as a pure state machine. Written before the
//! implementation: each test names one rule from the founder's decisions
//! (2026-10-27, #1 and #9).

use chrono::{DateTime, Duration, Utc};
use sqlx::types::Decimal;

use super::*;

fn d(n: i64) -> Decimal {
    Decimal::from(n)
}

fn t0() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}

fn trialing_at(used: i64) -> UsageInput {
    UsageInput {
        credits: d(3000),
        used: d(used),
        now: t0(),
        trial_ends_at: t0() + Duration::days(60),
    }
}

// ── The transition table ────────────────────────────────────────────────

#[test]
fn every_state_round_trips_through_its_column_value() {
    for s in TrialState::ALL {
        assert_eq!(TrialState::parse(s.as_str()), Some(s));
    }
    assert_eq!(TrialState::parse("trial"), None);
}

#[test]
fn the_legal_transitions_are_exactly_the_documented_ones() {
    use TrialState::*;
    let legal = [
        (CheckoutOpen, Trialing),
        (CheckoutOpen, Converting),
        (CheckoutOpen, Converted),
        (CheckoutOpen, Abandoned),
        // A Checkout completed after the 24 h window marked it abandoned
        // still took the customer's card: the payment is honoured.
        (Abandoned, Trialing),
        (Abandoned, Converting),
        (Abandoned, Converted),
        (Trialing, Converting),
        (Trialing, Canceled),
        (Converting, Converted),
        (Converting, Canceled),
        // A conversion invoice paid after a cancel from `converting`.
        (Canceled, Converted),
    ];
    for from in TrialState::ALL {
        for to in TrialState::ALL {
            assert_eq!(
                from.can_become(to),
                legal.contains(&(from, to)),
                "{from:?} -> {to:?}"
            );
        }
    }
}

#[test]
fn converted_is_terminal_and_canceled_only_reopens_to_converted() {
    for to in TrialState::ALL {
        assert!(!TrialState::Converted.can_become(to));
        assert_eq!(TrialState::Canceled.can_become(to), to == TrialState::Converted, "{to:?}");
    }
    assert!(TrialState::Converted.is_terminal());
    assert!(!TrialState::Canceled.is_terminal(), "a late-paid conversion invoice converts it");
    assert!(!TrialState::Abandoned.is_terminal(), "a late Checkout completion revives it");
}

#[test]
fn extra_usage_stays_off_until_the_first_successful_charge() {
    use TrialState::*;
    for s in [CheckoutOpen, Trialing, Converting] {
        assert!(s.refuses_extra_usage(false), "{s:?}");
        assert!(s.refuses_extra_usage(true), "{s:?}");
    }
    // Decision #6: a trial that ended without a charge stays refused until
    // a later subscription's first paid invoice.
    for s in [Abandoned, Canceled] {
        assert!(s.refuses_extra_usage(false), "{s:?}");
        assert!(!s.refuses_extra_usage(true), "{s:?}");
    }
    assert!(!Converted.refuses_extra_usage(false));
    assert!(!Converted.refuses_extra_usage(true));
}

// ── Checkout completion ─────────────────────────────────────────────────

#[test]
fn a_first_trial_starts_the_trial() {
    let plan = on_checkout_completed(TrialState::CheckoutOpen, true, true, true).unwrap();
    assert_eq!(plan, CompletionPlan::StartTrial);
    assert_eq!(plan.next_state(), TrialState::Trialing);
    assert!(plan.grants_trial_pool());
    assert_eq!(plan.convert_reason(), None);
}

#[test]
fn a_repeat_card_ends_the_trial_at_once() {
    // The creator was eligible when the Checkout was created; the card's
    // fingerprint is only known now, and it already had a trial.
    let plan = on_checkout_completed(TrialState::CheckoutOpen, true, true, false).unwrap();
    assert_eq!(plan, CompletionPlan::EndTrialNow);
    assert_eq!(plan.next_state(), TrialState::Converting);
    assert!(!plan.grants_trial_pool(), "no free credits for a repeat card");
    assert_eq!(plan.convert_reason(), Some(ConvertReason::RepeatCard));
}

#[test]
fn a_repeat_creator_got_a_checkout_with_no_trial_and_pays_now() {
    // `trial_eligible = false` was decided before the session existed, so
    // the subscription is already active (not trialing).
    let plan = on_checkout_completed(TrialState::CheckoutOpen, false, false, false).unwrap();
    assert_eq!(plan, CompletionPlan::PaidNow);
    assert_eq!(plan.next_state(), TrialState::Converted);
    assert!(!plan.grants_trial_pool());
    assert_eq!(plan.convert_reason(), Some(ConvertReason::NoTrial));
}

#[test]
fn a_subscription_stripe_did_not_put_in_trial_is_paid_now_whatever_we_expected() {
    // Stripe is the authority on whether money was taken: an active
    // subscription means the seats were charged.
    let plan = on_checkout_completed(TrialState::CheckoutOpen, true, false, true).unwrap();
    assert_eq!(plan, CompletionPlan::PaidNow);
}

#[test]
fn a_trialing_subscription_for_an_ineligible_creator_is_ended_now() {
    let plan = on_checkout_completed(TrialState::CheckoutOpen, false, true, true).unwrap();
    assert_eq!(plan, CompletionPlan::EndTrialNow);
}

#[test]
fn a_late_completion_after_abandonment_is_honoured() {
    assert_eq!(
        on_checkout_completed(TrialState::Abandoned, true, true, true).unwrap(),
        CompletionPlan::StartTrial
    );
}

#[test]
fn completion_after_the_lifecycle_moved_on_is_a_no_op_not_an_error() {
    for s in [TrialState::Trialing, TrialState::Converting, TrialState::Converted] {
        assert_eq!(on_checkout_completed(s, true, true, true).unwrap(), CompletionPlan::AlreadyApplied);
    }
}

#[test]
fn completion_for_a_canceled_trial_is_refused() {
    assert!(on_checkout_completed(TrialState::Canceled, true, true, true).is_err());
}

// ── Usage thresholds ────────────────────────────────────────────────────

#[test]
fn below_eighty_percent_nothing_happens() {
    let u = on_usage(&trialing_at(2399));
    assert_eq!(u, UsageDecision::default());
}

#[test]
fn eighty_percent_used_warns_the_owner() {
    let u = on_usage(&trialing_at(2400));
    assert!(u.notify_80);
    assert!(!u.notify_used);
    assert_eq!(u.convert, None);
}

#[test]
fn all_credits_used_converts_and_emails() {
    for used in [3000, 3001, 9000] {
        let u = on_usage(&trialing_at(used));
        assert!(u.notify_80, "the 80% email still goes if it was skipped past");
        assert!(u.notify_used);
        assert_eq!(u.convert, Some(ConvertReason::CreditsUsed), "used={used}");
    }
}

#[test]
fn a_fractional_overshoot_below_the_pool_does_not_convert() {
    let mut u = trialing_at(0);
    u.used = Decimal::new(299_999, 2); // 2,999.99
    assert_eq!(on_usage(&u).convert, None);
}

#[test]
fn seven_days_before_the_backstop_warns_once_the_window_opens() {
    let mut u = trialing_at(10);
    u.trial_ends_at = u.now + Duration::days(7) + Duration::seconds(1);
    assert!(!on_usage(&u).notify_ending_soon);
    u.trial_ends_at = u.now + Duration::days(7);
    assert!(on_usage(&u).notify_ending_soon);
    assert_eq!(on_usage(&u).convert, None);
}

#[test]
fn the_ninety_day_backstop_converts_with_credits_left() {
    let mut u = trialing_at(10);
    u.trial_ends_at = u.now;
    let d = on_usage(&u);
    assert_eq!(d.convert, Some(ConvertReason::Backstop));
    assert!(!d.notify_used, "credits were not used up");
    assert!(d.notify_ending_soon, "a missed 7-day warning still goes out");
}

#[test]
fn credits_used_wins_over_the_backstop_when_both_hold() {
    let mut u = trialing_at(3000);
    u.trial_ends_at = u.now - Duration::days(1);
    assert_eq!(on_usage(&u).convert, Some(ConvertReason::CreditsUsed));
}

// ── Conversion progress ─────────────────────────────────────────────────

#[test]
fn a_trialing_subscription_is_ended_to_charge_the_seats() {
    assert_eq!(
        conversion_step(&SubscriptionFacts { status: "trialing".into(), cancel_scheduled: false }),
        ConversionStep::EndTrialAtStripe
    );
}

#[test]
fn an_active_subscription_means_the_charge_went_through() {
    assert_eq!(
        conversion_step(&SubscriptionFacts { status: "active".into(), cancel_scheduled: false }),
        ConversionStep::Finalize
    );
}

#[test]
fn an_unpaid_conversion_waits_for_dunning() {
    for status in ["past_due", "incomplete", "unpaid"] {
        assert_eq!(
            conversion_step(&SubscriptionFacts { status: status.into(), cancel_scheduled: false }),
            ConversionStep::AwaitPayment,
            "{status}"
        );
    }
}

#[test]
fn a_cancel_scheduled_during_the_trial_is_never_charged() {
    for status in ["trialing", "past_due", "incomplete", "unpaid"] {
        assert_eq!(
            conversion_step(&SubscriptionFacts { status: status.into(), cancel_scheduled: true }),
            ConversionStep::Cancel,
            "{status}"
        );
    }
}

/// Review finding 1: a PAID conversion with a cancel scheduled at period end
/// is still paid for that period. Cancelling it would charge the customer
/// and grant nothing.
#[test]
fn a_paid_conversion_with_a_cancel_at_period_end_still_finalizes() {
    assert_eq!(
        conversion_step(&SubscriptionFacts { status: "active".into(), cancel_scheduled: true }),
        ConversionStep::Finalize
    );
}

#[test]
fn a_canceled_subscription_cancels_the_trial() {
    for status in ["canceled", "incomplete_expired"] {
        assert_eq!(
            conversion_step(&SubscriptionFacts { status: status.into(), cancel_scheduled: false }),
            ConversionStep::Cancel
        );
    }
}

#[test]
fn the_abandoned_checkout_window_is_twenty_four_hours() {
    let opened = t0();
    assert!(!checkout_abandoned(opened, opened + Duration::hours(24) - Duration::seconds(1)));
    assert!(checkout_abandoned(opened, opened + Duration::hours(24)));
}

#[test]
fn seat_count_starts_at_two() {
    assert!(validate_seats(1).is_err());
    assert!(validate_seats(0).is_err());
    assert!(validate_seats(-3).is_err());
    assert_eq!(validate_seats(2), Ok(2));
    assert_eq!(validate_seats(MAX_TRIAL_SEATS), Ok(MAX_TRIAL_SEATS));
    assert!(validate_seats(MAX_TRIAL_SEATS + 1).is_err());
}

/// #1528 S1: an invoice that was read decides on its own status; the
/// subscription's status decides only when no invoice was read.
#[test]
fn the_conversion_invoice_decides_over_the_subscription_status() {
    use ChargeOutcome::*;
    assert_eq!(charge_outcome(Some("paid"), "past_due"), Paid);
    assert_eq!(charge_outcome(Some("paid"), "canceled"), Paid);
    assert_eq!(charge_outcome(Some("void"), "active"), NotPaid, "Stripe can flip past_due to active on a void");
    assert_eq!(charge_outcome(Some("uncollectible"), "active"), NotPaid);
    assert_eq!(charge_outcome(Some("open"), "active"), Pending);
    assert_eq!(charge_outcome(Some("draft"), "past_due"), Pending);
    assert_eq!(charge_outcome(None, "active"), Paid);
    assert_eq!(charge_outcome(None, "trialing"), NotPaid);
    assert_eq!(charge_outcome(None, "canceled"), NotPaid);
    assert_eq!(charge_outcome(None, "incomplete_expired"), NotPaid);
    assert_eq!(charge_outcome(None, "past_due"), Pending);
    assert_eq!(charge_outcome(None, "unpaid"), Pending);
}

/// #1528 S3: more than 24 hours in `converting` is overdue.
#[test]
fn converting_is_overdue_after_twenty_four_hours() {
    assert!(!converting_overdue(t0(), t0()));
    assert!(!converting_overdue(t0(), t0() + Duration::hours(CONVERTING_ALERT_HOURS)));
    assert!(converting_overdue(t0(), t0() + Duration::hours(CONVERTING_ALERT_HOURS) + Duration::seconds(1)));
}

/// #1528 N2: the void key is stable within an hour (retries and racing
/// workers share it) and changes at the next, so a cached 5xx is not
/// replayed for Stripe's whole 24-hour key lifetime.
#[test]
fn the_void_key_rolls_over_every_hour() {
    let account = uuid::Uuid::nil();
    let hour = DateTime::from_timestamp(1_800_000_000 - 1_800_000_000 % 3600, 0).unwrap();
    let k = |t| lifecycle::void_invoice_key(account, "in_1", t);
    assert_eq!(k(hour), k(hour + Duration::minutes(59)));
    assert_ne!(k(hour), k(hour + Duration::hours(1)));
    assert!(k(hour).starts_with(&format!("org-trial-void:{account}:in_1:")));
}
