//! Pure tests: sizing, backoff, the exact PaymentIntent form, and how a Stripe
//! object is read. No database.

use std::time::Duration;

use serde_json::json;
use uuid::Uuid;

use super::stripe::{payment_intent_params, PaymentIntentFacts};
use super::*;

const M: i64 = 1_000_000;

fn row(amount: i64) -> ReloadRow {
    ReloadRow {
        id: Uuid::new_v4(),
        account_id: Uuid::new_v4(),
        amount_micro_usd: amount,
        state: "created".into(),
        idempotency_key: "reload:x".into(),
        payment_intent_id: None,
        customer_id: Some("cus_1".into()),
        payment_method_id: Some("pm_1".into()),
        created_at: chrono::Utc::now(),
    }
}

// ── sizing ─────────────────────────────────────────────────────────────────

#[test]
fn sizes_to_the_configured_amount_when_there_is_room() {
    assert_eq!(size_reload(25 * M, 100 * M, 0, 2 * M, 0), 25 * M);
}

#[test]
fn never_buys_more_than_can_still_be_spent_this_month() {
    // limit 100, spent 90, balance 4: only 6 of room remains.
    assert_eq!(size_reload(25 * M, 100 * M, 90 * M, 4 * M, 0), 6 * M);
}

#[test]
fn a_balance_that_already_covers_the_room_buys_nothing() {
    assert_eq!(size_reload(25 * M, 100 * M, 90 * M, 10 * M, 0), 0);
    assert_eq!(size_reload(25 * M, 100 * M, 90 * M, 15 * M, 0), 0);
}

#[test]
fn the_limit_reached_buys_nothing() {
    assert_eq!(size_reload(25 * M, 100 * M, 100 * M, 0, 0), 0);
    assert_eq!(size_reload(25 * M, 100 * M, 120 * M, 0, 0), 0);
}

#[test]
fn what_was_already_charged_this_month_counts_even_if_the_ledger_forgot_it() {
    // An adjustment emptied the balance after a $100 reload. The room by
    // spend says 100, but 100 was already charged against a 100 limit.
    assert_eq!(size_reload(25 * M, 100 * M, 0, 0, 100 * M), 0);
    assert_eq!(size_reload(25 * M, 100 * M, 0, 0, 90 * M), 10 * M);
}

#[test]
fn rounds_down_to_whole_cents_and_skips_dust() {
    assert_eq!(size_reload(25 * M, 10 * M + 5_000, 0, 0, 0), 10 * M);
    // Less than the $1 minimum is not worth a card charge.
    assert_eq!(size_reload(25 * M, 100 * M, 99 * M + 500_000, 0, 0), 0);
}

#[test]
fn overflow_and_nonsense_inputs_size_to_zero() {
    assert_eq!(size_reload(i64::MAX, i64::MAX, i64::MIN, i64::MIN, i64::MIN), 0);
    assert_eq!(size_reload(-5, 100 * M, 0, 0, 0), 0);
    assert_eq!(size_reload(25 * M, -1, 0, 0, 0), 0);
}

// ── backoff ────────────────────────────────────────────────────────────────

#[test]
fn backoff_grows_with_consecutive_failures() {
    assert_eq!(failure_backoff(0), Duration::ZERO);
    assert_eq!(failure_backoff(1), Duration::from_secs(3_600));
    assert_eq!(failure_backoff(2), Duration::from_secs(6 * 3_600));
    assert!(failure_backoff(9) >= failure_backoff(2));
}

// ── the Stripe form ────────────────────────────────────────────────────────

fn find<'a>(p: &'a crate::billing::stripe_client::Params, k: &str) -> Option<&'a str> {
    p.as_pairs().iter().find(|(a, _)| a == k).map(|(_, v)| v.as_str())
}

#[test]
fn the_payment_intent_is_an_off_session_confirmed_charge_in_cents() {
    let r = row(25 * M);
    let p = payment_intent_params(&r).unwrap();
    assert_eq!(find(&p, "amount"), Some("2500"));
    assert_eq!(find(&p, "currency"), Some("usd"));
    assert_eq!(find(&p, "customer"), Some("cus_1"));
    assert_eq!(find(&p, "payment_method"), Some("pm_1"));
    assert_eq!(find(&p, "confirm"), Some("true"));
    assert_eq!(find(&p, "off_session"), Some("true"));
    assert_eq!(find(&p, "error_on_requires_action"), Some("true"));
    assert_eq!(find(&p, "metadata[reload_id]"), Some(r.id.to_string().as_str()));
    assert_eq!(find(&p, "metadata[account_id]"), Some(r.account_id.to_string().as_str()));
    assert_eq!(find(&p, "metadata[purpose]"), Some(PURPOSE));
}

#[test]
fn the_form_is_identical_on_every_call_for_the_same_row() {
    let r = row(25 * M);
    assert_eq!(payment_intent_params(&r).unwrap(), payment_intent_params(&r).unwrap());
}

#[test]
fn a_row_without_a_card_or_with_a_fractional_cent_cannot_be_charged() {
    let mut r = row(25 * M);
    r.payment_method_id = None;
    assert!(payment_intent_params(&r).is_err());
    let mut r = row(25 * M);
    r.customer_id = None;
    assert!(payment_intent_params(&r).is_err());
    assert!(payment_intent_params(&row(25 * M + 1)).is_err());
    assert!(payment_intent_params(&row(0)).is_err());
}

// ── reading a PaymentIntent ────────────────────────────────────────────────

#[test]
fn reads_the_facts_we_act_on() {
    let id = Uuid::new_v4();
    let v = json!({
        "id": "pi_1", "status": "requires_payment_method", "amount": 2500, "currency": "usd",
        "customer": "cus_1",
        "metadata": {"purpose": PURPOSE, "reload_id": id.to_string(), "account_id": "acc"},
        "last_payment_error": {"code": "card_declined", "decline_code": "insufficient_funds", "message": "no"}
    });
    let f = PaymentIntentFacts::from_value(&v).unwrap();
    assert_eq!(f.id, "pi_1");
    assert_eq!(f.amount_cents, Some(2500));
    assert_eq!(f.reload_id, Some(id));
    assert!(f.is_reload());
    assert_eq!(f.error_code.as_deref(), Some("insufficient_funds"));
}

#[test]
fn an_expanded_customer_object_is_read_and_a_missing_id_is_refused() {
    let f = PaymentIntentFacts::from_value(&json!({"id":"pi","status":"succeeded","customer":{"id":"cus_9"}})).unwrap();
    assert_eq!(f.customer.as_deref(), Some("cus_9"));
    assert!(!f.is_reload());
    assert!(PaymentIntentFacts::from_value(&json!({"status":"succeeded"})).is_err());
}

#[test]
fn only_reload_payment_intents_are_routed_here() {
    assert!(is_reload_event(
        "payment_intent.succeeded",
        &json!({"metadata": {"purpose": PURPOSE}})
    ));
    assert!(!is_reload_event("payment_intent.succeeded", &json!({"metadata": {"purpose": "extra_usage_topup"}})));
    assert!(!is_reload_event("payment_intent.succeeded", &json!({})));
    assert!(!is_reload_event("invoice.paid", &json!({"metadata": {"purpose": PURPOSE}})));
    for t in ["succeeded", "payment_failed", "requires_action", "canceled"] {
        assert!(is_reload_event(&format!("payment_intent.{t}"), &json!({"metadata": {"purpose": PURPOSE}})));
    }
}
