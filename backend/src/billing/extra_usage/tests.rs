//! Pure tests: the settings validation matrix, top-up bounds, and the exact
//! Stripe form a top-up creates. No database.

use super::*;
use sqlx::types::Decimal;

const M: i64 = MICRO_PER_USD;

fn enable() -> UpdateRequest {
    UpdateRequest { enabled: true, ..Default::default() }
}

fn reload(enabled: bool, threshold: Option<i64>, amount: Option<i64>) -> Option<AutoReloadUpdate> {
    Some(AutoReloadUpdate { enabled, threshold_micro_usd: threshold, amount_micro_usd: amount })
}

fn stored(limit: Option<i64>) -> StoredSettings {
    StoredSettings {
        enabled: true,
        monthly_limit_micro_usd: limit,
        member_cap_seat_multiple: None,
        auto_reload_enabled: false,
        auto_reload_threshold_micro_usd: None,
        auto_reload_amount_micro_usd: None,
    }
}

fn code(r: Result<StoredSettings, Invalid>) -> &'static str {
    r.expect_err("expected a rejection").code
}

// ── defaults on first enable ───────────────────────────────────────────────

#[test]
fn first_enable_on_a_team_org_defaults_to_100_dollars_and_1x_cap() {
    let s = resolve_update(AccountKind::TeamOrg, None, &enable()).unwrap();
    assert!(s.enabled);
    assert_eq!(s.monthly_limit_micro_usd, Some(100 * M));
    assert_eq!(s.member_cap_seat_multiple, Some(Decimal::ONE));
    assert!(!s.auto_reload_enabled, "auto-reload is off by default");
    assert_eq!(s.auto_reload_threshold_micro_usd, None);
    assert_eq!(s.auto_reload_amount_micro_usd, None);
}

#[test]
fn first_enable_on_a_personal_account_defaults_to_50_dollars_and_no_cap() {
    let s = resolve_update(AccountKind::Personal, None, &enable()).unwrap();
    assert_eq!(s.monthly_limit_micro_usd, Some(50 * M));
    assert_eq!(s.member_cap_seat_multiple, None);
    assert!(!s.auto_reload_enabled);
}

#[test]
fn an_explicit_limit_beats_the_default() {
    let req = UpdateRequest { monthly_limit_micro_usd: Some(250 * M), ..enable() };
    let s = resolve_update(AccountKind::TeamOrg, None, &req).unwrap();
    assert_eq!(s.monthly_limit_micro_usd, Some(250 * M));
}

#[test]
fn disabling_with_nothing_stored_stores_a_disabled_row_with_no_limit() {
    let req = UpdateRequest { enabled: false, ..Default::default() };
    let s = resolve_update(AccountKind::Personal, None, &req).unwrap();
    assert!(!s.enabled);
    assert_eq!(s.monthly_limit_micro_usd, None, "a limit is only required while enabled");
}

#[test]
fn omitted_fields_keep_their_stored_values() {
    let mut cur = stored(Some(75 * M));
    cur.member_cap_seat_multiple = Some(Decimal::new(250, 2));
    cur.auto_reload_enabled = true;
    cur.auto_reload_threshold_micro_usd = Some(5 * M);
    cur.auto_reload_amount_micro_usd = Some(20 * M);
    let s = resolve_update(AccountKind::TeamOrg, Some(&cur), &enable()).unwrap();
    assert_eq!(s, cur);
}

#[test]
fn re_enabling_keeps_the_stored_limit_instead_of_the_default() {
    let mut cur = stored(Some(30 * M));
    cur.enabled = false;
    let s = resolve_update(AccountKind::Personal, Some(&cur), &enable()).unwrap();
    assert_eq!(s.monthly_limit_micro_usd, Some(30 * M));
}

// ── limit bounds ───────────────────────────────────────────────────────────

#[test]
fn the_limit_is_bounds_checked() {
    for bad in [0, -1, M - 10_000, 100_000 * M + 10_000, i64::MAX, i64::MIN] {
        let req = UpdateRequest { monthly_limit_micro_usd: Some(bad), ..enable() };
        assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "limit_out_of_range", "{bad}");
    }
    for ok in [M, 100_000 * M] {
        let req = UpdateRequest { monthly_limit_micro_usd: Some(ok), ..enable() };
        assert!(resolve_update(AccountKind::Personal, None, &req).is_ok(), "{ok}");
    }
}

#[test]
fn a_limit_is_validated_even_when_disabling() {
    let req = UpdateRequest { enabled: false, monthly_limit_micro_usd: Some(-5), ..Default::default() };
    assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "limit_out_of_range");
}

#[test]
fn a_limit_must_be_whole_cents() {
    let req = UpdateRequest { monthly_limit_micro_usd: Some(10 * M + 1), ..enable() };
    assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "amount_not_whole_cents");
}

// ── member cap ─────────────────────────────────────────────────────────────

#[test]
fn a_personal_account_cannot_set_a_member_cap() {
    let req = UpdateRequest { member_cap_seat_multiple: Some(1.0), ..enable() };
    assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "member_cap_not_applicable");
}

#[test]
fn a_team_org_cap_is_bounds_checked_and_two_decimals() {
    for bad in [0.0, -1.0, 0.09, 100.01, f64::NAN, f64::INFINITY] {
        let req = UpdateRequest { member_cap_seat_multiple: Some(bad), ..enable() };
        assert_eq!(code(resolve_update(AccountKind::TeamOrg, None, &req)), "member_cap_out_of_range", "{bad}");
    }
    let req = UpdateRequest { member_cap_seat_multiple: Some(1.234), ..enable() };
    assert_eq!(code(resolve_update(AccountKind::TeamOrg, None, &req)), "member_cap_out_of_range");
    let req = UpdateRequest { member_cap_seat_multiple: Some(2.5), ..enable() };
    let s = resolve_update(AccountKind::TeamOrg, None, &req).unwrap();
    assert_eq!(s.member_cap_seat_multiple, Some(Decimal::new(250, 2)));
    let req = UpdateRequest { member_cap_seat_multiple: Some(0.1), ..enable() };
    assert!(resolve_update(AccountKind::TeamOrg, None, &req).is_ok());
    let req = UpdateRequest { member_cap_seat_multiple: Some(100.0), ..enable() };
    assert!(resolve_update(AccountKind::TeamOrg, None, &req).is_ok());
}

// ── auto-reload ────────────────────────────────────────────────────────────

#[test]
fn auto_reload_requires_extra_usage_to_be_enabled() {
    let req = UpdateRequest {
        enabled: false,
        auto_reload: reload(true, Some(5 * M), Some(20 * M)),
        ..Default::default()
    };
    assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "auto_reload_requires_extra_usage");
}

#[test]
fn enabling_auto_reload_needs_a_threshold_and_an_amount() {
    for (t, a) in [(None, None), (Some(5 * M), None), (None, Some(20 * M))] {
        let req = UpdateRequest { auto_reload: reload(true, t, a), ..enable() };
        assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "auto_reload_incomplete");
    }
}

#[test]
fn auto_reload_can_use_values_already_stored() {
    let mut cur = stored(Some(100 * M));
    cur.auto_reload_threshold_micro_usd = Some(5 * M);
    cur.auto_reload_amount_micro_usd = Some(20 * M);
    let req = UpdateRequest { auto_reload: reload(true, None, None), ..enable() };
    let s = resolve_update(AccountKind::Personal, Some(&cur), &req).unwrap();
    assert!(s.auto_reload_enabled);
    assert_eq!(s.auto_reload_amount_micro_usd, Some(20 * M));
}

#[test]
fn a_valid_auto_reload_is_stored() {
    let req = UpdateRequest { auto_reload: reload(true, Some(5 * M), Some(20 * M)), ..enable() };
    let s = resolve_update(AccountKind::Personal, None, &req).unwrap();
    assert!(s.auto_reload_enabled);
    assert_eq!(s.auto_reload_threshold_micro_usd, Some(5 * M));
    assert_eq!(s.auto_reload_amount_micro_usd, Some(20 * M));
}

#[test]
fn the_reload_amount_may_equal_the_limit_but_never_exceed_it() {
    let req = UpdateRequest {
        monthly_limit_micro_usd: Some(20 * M),
        auto_reload: reload(true, Some(5 * M), Some(20 * M)),
        ..enable()
    };
    assert!(resolve_update(AccountKind::Personal, None, &req).is_ok());
    let req = UpdateRequest {
        monthly_limit_micro_usd: Some(20 * M),
        auto_reload: reload(true, Some(5 * M), Some(20 * M + 10_000)),
        ..enable()
    };
    assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "auto_reload_exceeds_limit");
}

#[test]
fn the_default_limit_also_bounds_the_reload_amount() {
    // Pro default limit is $50: a $60 reload would let one reload pass it.
    let req = UpdateRequest { auto_reload: reload(true, Some(5 * M), Some(60 * M)), ..enable() };
    assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "auto_reload_exceeds_limit");
}

#[test]
fn lowering_the_limit_below_the_stored_reload_amount_is_refused() {
    let mut cur = stored(Some(100 * M));
    cur.auto_reload_enabled = true;
    cur.auto_reload_threshold_micro_usd = Some(5 * M);
    cur.auto_reload_amount_micro_usd = Some(40 * M);
    let req = UpdateRequest { monthly_limit_micro_usd: Some(30 * M), ..enable() };
    assert_eq!(code(resolve_update(AccountKind::TeamOrg, Some(&cur), &req)), "auto_reload_exceeds_limit");
    // ... even with the reload switched off, because the stored amount still
    // has to satisfy the schema's reload-within-limit CHECK.
    cur.auto_reload_enabled = false;
    assert_eq!(code(resolve_update(AccountKind::TeamOrg, Some(&cur), &req)), "auto_reload_exceeds_limit");
}

#[test]
fn reload_amount_and_threshold_are_bounds_checked() {
    for bad in [0, -1, 5 * M - 10_000, 1_000 * M + 10_000, 20 * M + 1] {
        let req = UpdateRequest {
            monthly_limit_micro_usd: Some(5_000 * M),
            auto_reload: reload(true, Some(5 * M), Some(bad)),
            ..enable()
        };
        let c = code(resolve_update(AccountKind::TeamOrg, None, &req));
        assert!(
            c == "auto_reload_amount_out_of_range" || c == "amount_not_whole_cents",
            "{bad} -> {c}"
        );
    }
    for bad in [0, -1, M - 10_000, M + 1] {
        let req = UpdateRequest { auto_reload: reload(true, Some(bad), Some(20 * M)), ..enable() };
        let c = code(resolve_update(AccountKind::Personal, None, &req));
        assert!(
            c == "auto_reload_threshold_out_of_range" || c == "amount_not_whole_cents",
            "{bad} -> {c}"
        );
    }
}

#[test]
fn a_reload_must_lift_the_balance_back_over_its_threshold() {
    let req = UpdateRequest { auto_reload: reload(true, Some(30 * M), Some(20 * M)), ..enable() };
    assert_eq!(code(resolve_update(AccountKind::Personal, None, &req)), "auto_reload_threshold_exceeds_amount");
}

#[test]
fn disabling_extra_usage_switches_auto_reload_off_but_keeps_its_values() {
    let mut cur = stored(Some(100 * M));
    cur.auto_reload_enabled = true;
    cur.auto_reload_threshold_micro_usd = Some(5 * M);
    cur.auto_reload_amount_micro_usd = Some(20 * M);
    let req = UpdateRequest { enabled: false, ..Default::default() };
    let s = resolve_update(AccountKind::TeamOrg, Some(&cur), &req).unwrap();
    assert!(!s.enabled);
    assert!(!s.auto_reload_enabled, "the schema forbids reload without enabled");
    assert_eq!(s.auto_reload_amount_micro_usd, Some(20 * M));
}

#[test]
fn switching_auto_reload_off_keeps_the_amount_for_later() {
    let mut cur = stored(Some(100 * M));
    cur.auto_reload_enabled = true;
    cur.auto_reload_threshold_micro_usd = Some(5 * M);
    cur.auto_reload_amount_micro_usd = Some(20 * M);
    let req = UpdateRequest { auto_reload: reload(false, None, None), ..enable() };
    let s = resolve_update(AccountKind::TeamOrg, Some(&cur), &req).unwrap();
    assert!(!s.auto_reload_enabled);
    assert_eq!(s.auto_reload_threshold_micro_usd, Some(5 * M));
}

#[test]
fn unknown_request_fields_are_rejected() {
    let bad: Result<UpdateRequest, _> = serde_json::from_str(r#"{"enabled":true,"surprise":1}"#);
    assert!(bad.is_err());
    let ok: UpdateRequest = serde_json::from_str(
        r#"{"enabled":true,"monthly_limit_micro_usd":1000000,"auto_reload":{"enabled":false}}"#,
    )
    .unwrap();
    assert!(ok.enabled);
}

// ── top-up amounts ─────────────────────────────────────────────────────────

#[test]
fn a_topup_amount_is_bounds_checked_to_whole_cents() {
    assert!(validate_topup_amount(5 * M).is_ok());
    assert!(validate_topup_amount(1_000 * M).is_ok());
    assert!(validate_topup_amount(12_340_000).is_ok());
    for bad in [i64::MIN, -5 * M, 0, 1, 5 * M - 10_000, 1_000 * M + 10_000, i64::MAX] {
        assert_eq!(validate_topup_amount(bad).unwrap_err().code, "topup_amount_out_of_range", "{bad}");
    }
    assert_eq!(validate_topup_amount(5 * M + 1).unwrap_err().code, "amount_not_whole_cents");
}

// ── Stripe form ────────────────────────────────────────────────────────────

fn get<'a>(p: &'a Params, key: &str) -> Option<&'a str> {
    p.as_pairs().iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

#[test]
fn the_topup_session_is_a_card_payment_that_saves_the_card_for_off_session_use() {
    let acct = "11111111-1111-1111-1111-111111111111";
    let p = topup_checkout_params("cus_1", acct, 25 * M + 500_000, "https://app/ok", "https://app/no");
    assert_eq!(get(&p, "mode"), Some("payment"));
    assert_eq!(get(&p, "customer"), Some("cus_1"));
    assert_eq!(get(&p, "client_reference_id"), Some(acct));
    assert_eq!(get(&p, "success_url"), Some("https://app/ok"));
    assert_eq!(get(&p, "cancel_url"), Some("https://app/no"));
    assert_eq!(get(&p, "line_items[0][quantity]"), Some("1"));
    assert_eq!(get(&p, "line_items[0][price_data][currency]"), Some("usd"));
    assert_eq!(get(&p, "line_items[0][price_data][unit_amount]"), Some("2550"), "cents, not micro-USD");
    assert_eq!(get(&p, "payment_intent_data[setup_future_usage]"), Some("off_session"));
    assert_eq!(get(&p, "payment_method_types[0]"), Some("card"));
    for prefix in ["metadata", "payment_intent_data[metadata]"] {
        assert_eq!(get(&p, &format!("{prefix}[account_id]")), Some(acct));
        assert_eq!(get(&p, &format!("{prefix}[purpose]")), Some(TOPUP_PURPOSE));
    }
    assert_eq!(get(&p, "metadata[amount_micro_usd]"), Some("25500000"));
    assert!(get(&p, "allow_promotion_codes").is_none(), "a discount would credit more than was paid");
}

#[test]
fn the_idempotency_key_is_stable_inside_a_window_and_varies_with_what_it_covers() {
    let a = Uuid::new_v4();
    let k = |acct, amt, s: &str, c: &str, now| topup_idempotency_key(acct, amt, s, c, now);
    let base = k(a, 10 * M, "https://a/ok", "https://a/no", 1_000_000);
    assert_eq!(base, k(a, 10 * M, "https://a/ok", "https://a/no", 1_000_100), "same window");
    assert_ne!(base, k(a, 10 * M, "https://a/ok", "https://a/no", 1_001_000), "next window");
    assert_ne!(base, k(a, 11 * M, "https://a/ok", "https://a/no", 1_000_000), "amount");
    assert_ne!(base, k(Uuid::new_v4(), 10 * M, "https://a/ok", "https://a/no", 1_000_000), "account");
    assert_ne!(base, k(a, 10 * M, "https://b/ok", "https://a/no", 1_000_000), "urls: Stripe rejects a reused key with new params");
    assert!(base.len() < 255);
}

// ── what the webhook reads off a session ───────────────────────────────────

#[test]
fn payment_facts_are_read_from_the_retrieved_session() {
    use super::webhook::PaymentFacts;
    let s = serde_json::json!({
        "id": "cs_1", "mode": "payment", "payment_status": "paid",
        "payment_intent": "pi_1", "amount_total": 2550, "amount_subtotal": 2400, "currency": "usd",
        "metadata": {"purpose": "extra_usage_topup", "account_id": "acct-1"},
    });
    let f = PaymentFacts::from_session(&s);
    assert_eq!(f.payment_intent.as_deref(), Some("pi_1"));
    assert_eq!(f.amount_total_cents, Some(2550));
    assert_eq!(f.amount_subtotal_cents, Some(2400), "the pre-tax amount is what gets credited");
    assert_eq!(f.currency.as_deref(), Some("usd"));
    assert_eq!(f.payment_status.as_deref(), Some("paid"));
    assert_eq!(f.metadata_account_id.as_deref(), Some("acct-1"));
    assert!(f.is_topup());

    // An expanded payment_intent object reads as its id.
    let s = serde_json::json!({"payment_intent": {"id": "pi_2"}, "metadata": {}});
    let f = PaymentFacts::from_session(&s);
    assert_eq!(f.payment_intent.as_deref(), Some("pi_2"));
    assert!(!f.is_topup(), "no purpose metadata: a legacy credit-pack session");

    let f = PaymentFacts::from_session(&serde_json::json!({}));
    assert_eq!(f, PaymentFacts::default());
    assert!(!f.is_topup());
}
