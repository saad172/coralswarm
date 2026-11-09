//! Unit tests for [`FundingPolicy::decide`]: every branch and boundary of the
//! decision, with no database.

use super::*;
use RefusalReason::*;

const RATE: i64 = 22_000; // micro-USD per credit ($0.022)

fn d(n: i64) -> Decimal {
    Decimal::from(n)
}

fn dd(s: &str) -> Decimal {
    s.parse().unwrap()
}

/// A paid, non-org account whose 100-credit allowance is used up, with extra
/// usage on: $50 limit, $10 balance, nothing spent. Tests change one thing.
fn base() -> FundingSnapshot {
    FundingSnapshot {
        allowance: AllowanceState {
            member: Some(MemberAllowance { granted: d(100), consumed: d(100), held: d(0), pool_cap: None }),
            pool_available: Some(d(0)),
            missing_pool_is_unlimited: false,
        },
        credits_frozen: false,
        extra: ExtraState {
            plan_is_free: false,
            is_team_org: false,
            settings: Some(ExtraSettings {
                enabled: true,
                monthly_limit_micro_usd: Some(50_000_000),
                member_cap_seat_multiple: None,
            }),
            seat_allowance_credits: d(100),
            payer_known: true,
            period_spent_micro_usd: 0,
            member_spent_credits: d(0),
            held_account_credits: d(0),
            held_member_credits: d(0),
            balance_micro_usd: 10_000_000,
            rate_micro_usd_per_credit: RATE,
        },
    }
}

fn team(mut s: FundingSnapshot) -> FundingSnapshot {
    s.extra.is_team_org = true;
    s.extra.settings.as_mut().unwrap().member_cap_seat_multiple = Some(d(1));
    s
}

fn settings(s: &mut FundingSnapshot) -> &mut ExtraSettings {
    s.extra.settings.as_mut().unwrap()
}

fn member(s: &mut FundingSnapshot) -> &mut MemberAllowance {
    s.allowance.member.as_mut().unwrap()
}

fn funded(allowance: Decimal, credits: Decimal, micro: i64) -> FundingOutcome {
    FundingOutcome::Funded { allowance_credits: allowance, extra: ExtraFunding { credits, micro_usd: micro } }
}

// ── plan allowance first ────────────────────────────────────────────────────

#[test]
fn inside_the_allowance_is_funded_by_the_allowance_alone() {
    let mut s = base();
    member(&mut s).consumed = d(40);
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(10))), funded(d(10), d(0), 0));
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(10))), funded(d(10), d(0), 0));
    assert!(FundingPolicy::decide(&s, Intent::Probe).is_allowance_only());
}

#[test]
fn allowance_never_touches_extra_usage_even_when_it_is_off() {
    let mut s = base();
    member(&mut s).consumed = d(0);
    s.extra.settings = None;
    s.extra.plan_is_free = true;
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(100))), funded(d(100), d(0), 0));
}

#[test]
fn exactly_at_the_allowance_boundary_the_charge_is_still_allowance() {
    let mut s = base();
    member(&mut s).consumed = d(90);
    // 10 credits left, charge exactly 10: all allowance, no extra debit.
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(10))), funded(d(10), d(0), 0));
}

#[test]
fn one_hair_past_the_allowance_spills_into_extra_usage() {
    let mut s = base();
    member(&mut s).consumed = d(90);
    // 10.0001 credits: 10 from the allowance, 0.0001 from extra usage.
    // ceil(0.0001 * 22000) = ceil(2.2) = 3 micro-USD.
    assert_eq!(
        FundingPolicy::decide(&s, Intent::Settle(dd("10.0001"))),
        funded(d(10), dd("0.0001"), 3)
    );
}

#[test]
fn a_straddling_charge_splits_allowance_then_extra() {
    let mut s = base();
    member(&mut s).consumed = d(95);
    // 5 credits left; charge 8 -> 5 allowance + 3 extra = 66_000 micro-USD.
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(8))), funded(d(5), d(3), 66_000));
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(8))), funded(d(5), d(3), 66_000));
}

#[test]
fn exactly_exhausted_allowance_sends_the_whole_charge_to_extra_usage() {
    let s = base();
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(2))), funded(d(0), d(2), 44_000));
}

#[test]
fn pool_credits_are_plan_allowance_and_come_before_extra_usage() {
    let mut s = base();
    s.allowance.pool_available = Some(d(500));
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(30))), funded(d(30), d(0), 0));
}

#[test]
fn pool_part_is_bounded_by_the_members_pool_cap() {
    let mut s = base();
    s.allowance.pool_available = Some(d(500));
    member(&mut s).pool_cap = Some(d(20));
    // 20 credits of pool room, charge 30: 20 allowance + 10 extra.
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(30))), funded(d(20), d(10), 220_000));
    // Cap fully used: the pool is closed, everything goes to extra usage.
    member(&mut s).consumed = d(120);
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(5))), funded(d(0), d(5), 110_000));
}

#[test]
fn frozen_credits_close_the_pool_but_not_the_members_own_allowance() {
    let mut s = base();
    s.credits_frozen = true;
    s.allowance.pool_available = Some(d(500));
    // Own allowance exhausted: pool closed, extra closed -> paywall.
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(CreditsFrozen));
    // Own allowance left: still served from it.
    member(&mut s).consumed = d(60);
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(40))), funded(d(40), d(0), 0));
    // Straddling a frozen account: allowance part funded, remainder unfunded.
    assert_eq!(
        FundingPolicy::decide(&s, Intent::Settle(d(50))),
        FundingOutcome::Partial {
            allowance_credits: d(40),
            extra: ExtraFunding::NONE,
            unfunded_credits: d(10),
            reason: CreditsFrozen
        }
    );
}

#[test]
fn a_missing_pool_row_is_unlimited_for_preflight_and_zero_otherwise() {
    let mut s = base();
    s.allowance.pool_available = None;
    s.allowance.missing_pool_is_unlimited = true;
    assert!(FundingPolicy::decide(&s, Intent::Probe).is_allowance_only());
    // ... but the member's pool cap still binds.
    member(&mut s).pool_cap = Some(d(10));
    member(&mut s).consumed = d(110);
    // Pool cap fully used: the unlimited missing pool is closed, so the
    // charge falls through to extra usage (funded in `base`).
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), funded(d(0), d(1), 22_000));
    s.allowance.missing_pool_is_unlimited = false;
    member(&mut s).pool_cap = None;
    member(&mut s).consumed = d(100);
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), funded(d(0), d(1), 22_000));
}

#[test]
fn no_period_row_uses_the_pool_only() {
    let mut s = base();
    s.allowance.member = None;
    s.allowance.pool_available = Some(d(7));
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(7))), funded(d(7), d(0), 0));
    s.allowance.pool_available = Some(d(0));
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(1))), funded(d(0), d(1), 22_000));
}

#[test]
fn holds_reduce_the_member_allowance() {
    let mut s = base();
    member(&mut s).consumed = d(50);
    member(&mut s).held = d(45);
    // 5 credits left after holds.
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(6))), funded(d(5), d(1), 22_000));
}

// ── extra usage gates ───────────────────────────────────────────────────────

#[test]
fn free_plan_is_refused_with_the_free_plan_reason_even_if_settings_exist() {
    let mut s = base();
    s.extra.plan_is_free = true;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(FreePlan));
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(FreePlan));
}

#[test]
fn extra_usage_off_or_never_configured_is_refused() {
    let mut s = base();
    settings(&mut s).enabled = false;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(ExtraUsageOff));
    s.extra.settings = None; // a missing row is off
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(ExtraUsageOff));
}

#[test]
fn frozen_beats_free_beats_off() {
    let mut s = base();
    s.credits_frozen = true;
    s.extra.plan_is_free = true;
    s.extra.settings = None;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(CreditsFrozen));
    s.credits_frozen = false;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(FreePlan));
    s.extra.plan_is_free = false;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(ExtraUsageOff));
}

#[test]
fn settlement_with_a_closed_gate_leaves_the_whole_remainder_unfunded() {
    let mut s = base();
    s.extra.settings = None;
    member(&mut s).consumed = d(98);
    assert_eq!(
        FundingPolicy::decide(&s, Intent::Settle(d(5))),
        FundingOutcome::Partial {
            allowance_credits: d(2),
            extra: ExtraFunding::NONE,
            unfunded_credits: d(3),
            reason: ExtraUsageOff
        }
    );
}

// ── per-member cap (team orgs only) ─────────────────────────────────────────

#[test]
fn member_at_the_cap_is_refused_and_one_credit_under_is_funded() {
    let mut s = team(base());
    s.extra.member_spent_credits = d(100); // cap = 1x 100
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(MemberCapReached));
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(MemberCapReached));
    s.extra.member_spent_credits = d(99);
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), funded(d(0), d(1), 22_000));
    // Exactly the room left is fine; a hair more is refused.
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))).refusal(), None);
    assert_eq!(
        FundingPolicy::decide(&s, Intent::Admit(dd("1.0001"))),
        FundingOutcome::Refused(MemberCapReached)
    );
}

#[test]
fn settlement_clamps_to_the_cap_room() {
    let mut s = team(base());
    s.extra.member_spent_credits = d(99);
    assert_eq!(
        FundingPolicy::decide(&s, Intent::Settle(d(3))),
        FundingOutcome::Partial {
            allowance_credits: d(0),
            extra: ExtraFunding { credits: d(1), micro_usd: 22_000 },
            unfunded_credits: d(2),
            reason: MemberCapReached
        }
    );
}

#[test]
fn the_cap_defaults_to_one_times_the_seat_allowance_when_unset_on_a_team_org() {
    let mut s = team(base());
    settings(&mut s).member_cap_seat_multiple = None;
    s.extra.member_spent_credits = d(100);
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(MemberCapReached));
}

#[test]
fn the_cap_scales_with_the_seat_multiple() {
    let mut s = team(base());
    settings(&mut s).member_cap_seat_multiple = Some(dd("2.5"));
    s.extra.member_spent_credits = d(249);
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), funded(d(0), d(1), 22_000));
    s.extra.member_spent_credits = d(250);
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(MemberCapReached));
}

#[test]
fn a_team_member_without_an_allowance_period_has_no_cap_room() {
    let mut s = team(base());
    s.extra.seat_allowance_credits = d(0);
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(MemberCapReached));
}

#[test]
fn held_member_reservations_count_against_the_cap() {
    let mut s = team(base());
    s.extra.member_spent_credits = d(90);
    s.extra.held_member_credits = d(10);
    s.extra.held_account_credits = d(10);
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(MemberCapReached));
}

#[test]
fn pro_and_personal_accounts_have_no_member_cap() {
    let mut s = base(); // is_team_org = false
    s.extra.member_spent_credits = d(1_000_000);
    s.extra.seat_allowance_credits = d(0);
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(5))), funded(d(0), d(5), 110_000));
}

// ── monthly limit ───────────────────────────────────────────────────────────

#[test]
fn at_the_monthly_limit_is_refused() {
    let mut s = base();
    s.extra.period_spent_micro_usd = 50_000_000;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(MonthlyLimitReached));
}

#[test]
fn one_micro_dollar_under_the_limit_still_probes_funded_but_cannot_admit_a_credit() {
    let mut s = base();
    s.extra.period_spent_micro_usd = 49_999_999;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), funded(d(0), d(0), 0));
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(MonthlyLimitReached));
}

#[test]
fn settlement_never_overshoots_the_limit() {
    let mut s = base();
    s.extra.period_spent_micro_usd = 49_990_000; // 10_000 micro-USD of room
    let FundingOutcome::Partial { extra, unfunded_credits, reason, .. } =
        FundingPolicy::decide(&s, Intent::Settle(d(1)))
    else {
        panic!("a charge over the limit room is partial");
    };
    assert_eq!(extra.micro_usd, 10_000);
    assert_eq!(reason, MonthlyLimitReached);
    assert_eq!(extra.credits + unfunded_credits, d(1));
    assert!(s.extra.period_spent_micro_usd + extra.micro_usd <= 50_000_000);
}

#[test]
fn a_charge_exactly_filling_the_limit_is_funded() {
    let mut s = base();
    s.extra.period_spent_micro_usd = 49_978_000; // exactly 22_000 left = 1 credit
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(1))), funded(d(0), d(1), 22_000));
}

#[test]
fn an_enabled_row_without_a_limit_fails_closed() {
    let mut s = base();
    settings(&mut s).monthly_limit_micro_usd = None;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(MonthlyLimitReached));
}

#[test]
fn the_member_cap_is_reported_before_the_limit_when_both_are_exhausted() {
    let mut s = team(base());
    s.extra.member_spent_credits = d(100);
    s.extra.period_spent_micro_usd = 50_000_000;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(MemberCapReached));
}

// ── balance ─────────────────────────────────────────────────────────────────

#[test]
fn an_empty_balance_is_the_paywall() {
    let mut s = base();
    s.extra.balance_micro_usd = 0;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(ExtraBalanceExhausted));
}

#[test]
fn the_limit_is_reported_before_the_balance_when_both_are_exhausted() {
    let mut s = base();
    s.extra.balance_micro_usd = 0;
    s.extra.period_spent_micro_usd = 50_000_000;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(MonthlyLimitReached));
}

#[test]
fn a_balance_smaller_than_the_charge_is_clamped_and_the_rest_is_unfunded() {
    let mut s = base();
    s.extra.balance_micro_usd = 10_000;
    let FundingOutcome::Partial { allowance_credits, extra, unfunded_credits, reason } =
        FundingPolicy::decide(&s, Intent::Settle(d(1)))
    else {
        panic!("clamped, not refused");
    };
    assert_eq!(allowance_credits, d(0));
    assert_eq!(extra.micro_usd, 10_000, "spends exactly the balance, never more");
    assert_eq!(reason, ExtraBalanceExhausted);
    assert_eq!(extra.credits + unfunded_credits, d(1));
    assert!(unfunded_credits > d(0));
    assert!(extra.micro_usd <= s.extra.balance_micro_usd);
}

#[test]
fn a_charge_exactly_equal_to_the_balance_is_funded_in_full() {
    let mut s = base();
    s.extra.balance_micro_usd = 22_000;
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(1))), funded(d(0), d(1), 22_000));
    s.extra.balance_micro_usd = 21_999;
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(ExtraBalanceExhausted));
}

#[test]
fn reservations_hold_balance_and_limit_room() {
    let mut s = base();
    s.extra.balance_micro_usd = 100_000;
    s.extra.held_account_credits = dd("4.5"); // 99_000 micro-USD held
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(ExtraBalanceExhausted));
    s.extra.balance_micro_usd = 10_000_000;
    s.extra.period_spent_micro_usd = 49_950_000;
    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), FundingOutcome::Refused(MonthlyLimitReached));
}

#[test]
fn credits_to_micro_usd_rounds_up_so_the_balance_is_never_undercharged() {
    let s = base();
    // 0.00001 credit = 0.22 micro-USD -> 1 micro-USD.
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(dd("0.00001"))), funded(d(0), dd("0.00001"), 1));
}

#[test]
fn rate_changes_reprice_the_debit_not_the_credits() {
    let mut s = base();
    s.extra.rate_micro_usd_per_credit = 30_000;
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(2))), funded(d(0), d(2), 60_000));
}

// ── Probe ───────────────────────────────────────────────────────────────────

#[test]
fn probe_is_funded_when_extra_usage_has_room_and_amounts_are_zero() {
    let s = base();
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), funded(d(0), d(0), 0));
}

#[test]
fn probe_matches_admit_of_the_smallest_charge_on_every_reason() {
    let mut cases: Vec<FundingSnapshot> = Vec::new();
    cases.push(base());
    let mut a = base(); a.credits_frozen = true; cases.push(a);
    let mut a = base(); a.extra.plan_is_free = true; cases.push(a);
    let mut a = base(); a.extra.settings = None; cases.push(a);
    let mut a = team(base()); a.extra.member_spent_credits = d(100); cases.push(a);
    let mut a = base(); a.extra.period_spent_micro_usd = 50_000_000; cases.push(a);
    let mut a = base(); a.extra.balance_micro_usd = 0; cases.push(a);
    for s in cases {
        let probe = FundingPolicy::decide(&s, Intent::Probe).refusal();
        let tiny = FundingPolicy::decide(&s, Intent::Admit(dd("0.00001"))).refusal();
        assert_eq!(probe, tiny);
    }
}

// ── invariants over a grid ──────────────────────────────────────────────────

#[test]
fn settlement_conserves_the_charge_and_respects_every_ceiling() {
    let charges = ["0.00001", "0.5", "1", "7", "100", "1000"];
    let balances = [0, 1, 21_999, 22_000, 500_000, 50_000_000];
    let spent = [0, 49_999_999, 50_000_000];
    let member_spent = [0, 99, 100];
    let consumed = [0, 99, 100, 150];
    let pools = [0, 5];
    for c in charges {
        for b in balances {
            for sp in spent {
                for ms in member_spent {
                    for co in consumed {
                        for pool in pools {
                            for team_org in [false, true] {
                                let mut s = base();
                                s.extra.balance_micro_usd = b;
                                s.extra.period_spent_micro_usd = sp;
                                s.extra.member_spent_credits = d(ms);
                                s.extra.is_team_org = team_org;
                                s.allowance.pool_available = Some(d(pool));
                                member(&mut s).consumed = d(co);
                                let charge = dd(c);
                                let out = FundingPolicy::decide(&s, Intent::Settle(charge));
                                let (allow, extra, unfunded) = match out {
                                    FundingOutcome::Funded { allowance_credits, extra } => {
                                        (allowance_credits, extra, d(0))
                                    }
                                    FundingOutcome::Partial { allowance_credits, extra, unfunded_credits, .. } => {
                                        (allowance_credits, extra, unfunded_credits)
                                    }
                                    FundingOutcome::Refused(r) => panic!("settlement refused: {r:?}"),
                                };
                                assert_eq!(allow + extra.credits + unfunded, charge, "conservation");
                                assert!(extra.micro_usd >= 0 && extra.micro_usd <= b, "never past the balance");
                                assert!(sp + extra.micro_usd <= 50_000_000 || extra.micro_usd == 0, "never past the limit");
                                if team_org {
                                    assert!(d(ms) + extra.credits <= d(100) || extra.micro_usd == 0, "never past the cap");
                                }
                                assert!(allow >= d(0) && unfunded >= d(0) && extra.credits >= d(0));
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn admit_is_all_or_nothing() {
    let s = base();
    for c in ["0.5", "1", "300", "5000"] {
        match FundingPolicy::decide(&s, Intent::Admit(dd(c))) {
            FundingOutcome::Partial { .. } => panic!("admission returned a partial"),
            _ => {}
        }
    }
}

#[test]
fn refusal_codes_are_stable_and_unique() {
    let codes: Vec<&str> = RefusalReason::ALL.iter().map(|r| r.code()).collect();
    assert_eq!(
        codes,
        [
            "free_plan",
            "credits_frozen",
            "extra_usage_off",
            "member_cap_reached",
            "monthly_limit_reached",
            "extra_balance_exhausted"
        ]
    );
    let mut sorted = codes.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), codes.len());
}

// ── headroom equals the pre-3.2 enforcement answers ─────────────────────────

fn allowance(granted: i64, consumed: i64, cap: Option<i64>, pool: Option<i64>, unlimited: bool) -> AllowanceState {
    AllowanceState {
        member: Some(MemberAllowance {
            granted: d(granted),
            consumed: d(consumed),
            held: d(0),
            pool_cap: cap.map(d),
        }),
        pool_available: pool.map(d),
        missing_pool_is_unlimited: unlimited,
    }
}

#[test]
fn headroom_matches_the_legacy_is_blocked_truth_table() {
    // (granted, consumed, cap, pool, frozen, expect_open)
    let cases = [
        (100, 99, None, Some(-5), false, true),  // inside own allowance
        (100, 100, None, Some(1), false, true),  // at allowance, pool left
        (100, 100, None, Some(0), false, false), // pool empty
        (100, 100, None, Some(-3), false, false),
        (100, 100, Some(50), Some(900), false, true),
        (100, 150, Some(50), Some(900), false, false), // cap reached
        (100, 149, Some(50), Some(900), false, true),
        (100, 100, None, Some(900), true, false), // frozen closes the pool
        (100, 10, None, Some(0), true, true),     // frozen, own allowance left
    ];
    for (g, c, cap, pool, frozen, open) in cases {
        let h = allowance(g, c, cap, pool, false).headroom(frozen);
        assert_eq!(h.is_positive(), open, "granted={g} consumed={c} cap={cap:?} pool={pool:?} frozen={frozen}");
    }
}

#[test]
fn own_allowance_covering_the_intent_ignores_everything_else() {
    // The SQL loader returns early when the member's own remaining allowance
    // covers the intent, without reading the pool, the subscription or extra
    // usage. That is only valid if `decide` gives the same answer whatever
    // those say.
    for pool in [None, Some(d(-50)), Some(d(0)), Some(d(5_000))] {
        for frozen in [false, true] {
            for unlimited in [false, true] {
                for cap in [None, Some(d(0)), Some(d(10))] {
                    let mut s = base();
                    s.credits_frozen = frozen;
                    s.allowance.pool_available = pool;
                    s.allowance.missing_pool_is_unlimited = unlimited;
                    member(&mut s).consumed = d(60);
                    member(&mut s).pool_cap = cap;
                    s.extra.plan_is_free = true;
                    s.extra.settings = None;
                    // 40 credits of own allowance remain.
                    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(40))), funded(d(40), d(0), 0));
                    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(40))), funded(d(40), d(0), 0));
                    assert_eq!(FundingPolicy::decide(&s, Intent::Admit(d(1))), funded(d(1), d(0), 0));
                    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), funded(d(0), d(0), 0));
                }
            }
        }
    }
}

#[test]
fn unattributed_team_spend_has_no_member_cap_but_still_the_limit_and_balance() {
    let mut s = team(base());
    s.extra.payer_known = false;
    s.extra.seat_allowance_credits = d(0);
    s.extra.member_spent_credits = d(0);
    // A known payer with no allowance has no cap room; an unknown payer is
    // charged to the org.
    assert_eq!(FundingPolicy::decide(&s, Intent::Settle(d(5))), funded(d(0), d(5), 110_000));
    s.extra.period_spent_micro_usd = 50_000_000;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(MonthlyLimitReached));
    s.extra.period_spent_micro_usd = 0;
    s.extra.balance_micro_usd = 0;
    assert_eq!(FundingPolicy::decide(&s, Intent::Probe), FundingOutcome::Refused(ExtraBalanceExhausted));
}

#[test]
fn the_rate_is_required_only_when_extra_usage_is_enabled() {
    assert_eq!(rate_when_needed(true, Some(22_000)), Ok(22_000));
    assert!(rate_when_needed(true, None).is_err());
    assert!(rate_when_needed(true, Some(0)).is_err());
    assert_eq!(rate_when_needed(false, None), Ok(1));
}
