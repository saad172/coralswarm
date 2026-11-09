//! The one funding decision (settings launch plan PR 3.2, decisions 11-13, 19
//! and 20).
//!
//! # What this module is
//!
//! [`FundingPolicy::decide`] is a PURE function: a snapshot of an account's
//! funding state plus an [`Intent`] in, a typed [`FundingOutcome`] out. It
//! reads nothing and writes nothing. Three callers share it, so the pre-flight
//! gate, the reservation gate and settlement can never disagree:
//!
//! * pre-flight (`enforcement::check_blocked` / `interactive_admission`):
//!   [`Intent::Probe`], "could the next unit of spend be funded at all?";
//! * reservation (`enforcement::managed_funding_tx`): [`Intent::Admit`], "can
//!   this whole bound be funded?";
//! * settlement (`funding_policy::db::SettlementPlanner`, used by
//!   `workers::billing_derivation` and `managed_reservation::settle_tx`):
//!   [`Intent::Settle`], "the work already happened; split this charge".
//!
//! The SQL that builds the snapshot lives in [`db`]; this file has no I/O.
//!
//! # Order of the decision
//!
//! 1. **Plan allowance first.** The member's own seat allowance, then the
//!    shared pool (other seats' unused credits, bounded by the member's pool
//!    cap, and unavailable while credits are frozen). This is the existing
//!    policy, unchanged; see [`AllowanceState::headroom`].
//! 2. If the allowance cannot fund the whole charge, the remainder needs
//!    **extra usage**, which is refused in this order:
//!    credits frozen by a cancellation (decision 19) -> Free plan -> extra
//!    usage off (a missing `extra_usage_settings` row is off) -> per-member
//!    cap (team orgs only; default 1x the member's seat allowance; personal
//!    accounts, including Pro, have none) -> org/Pro monthly limit -> extra
//!    balance.
//!
//! # Money rules
//!
//! * Extra usage is spent from a prepaid balance held in integer micro-USD.
//!   A charge in credits becomes `ceil(credits * rate)` micro-USD at the rate
//!   in force; the rate is snapshotted onto every ledger row.
//! * The balance never goes negative (decision 20). At settlement a charge
//!   larger than the balance, the limit room or the cap room is CLAMPED: extra
//!   usage funds what it can and the remainder is reported as
//!   [`FundingOutcome::Partial::unfunded_credits`]. The existing code records
//!   overshoot by letting `credit_ledger` / `credit_balances` take the whole
//!   uncovered amount (so the plan balance goes below zero and the gate stays
//!   closed); settlement reuses exactly that for the unfunded remainder.
//! * Rounding: each debit is `ceil(credits * rate)` micro-USD on its own, so
//!   the balance is never undercharged and a row can be over by less than one
//!   micro-USD (a hundred 0.00001-credit debits cost 100 micro-USD, not the
//!   exact 22). The credits recorded on the row are exact; only the money is
//!   rounded, and always up. This is accepted: it is bounded by 1 micro-USD per
//!   ledger row and is never in the customer's favour at the cost of a
//!   negative balance.
//! * The monthly limit and the per-member cap are counted per UTC calendar
//!   month, deliberately independent of the plan allowance period. Allowance
//!   periods are cut, shortened and re-created by renewal and seat changes
//!   mid-month; tying the limit to them restarted it from zero (found in
//!   review). A calendar-month window cannot move under a running total.
//! * Unattributed spend (no payer) on a team org has no member to cap. By
//!   default (`db::UNATTRIBUTED_SPEND`) it draws the org's limit and balance
//!   with no member cap; `UnattributedSpend::Owner` instead attributes it to
//!   the account owner. Either way it can no longer be free overshoot.
//! * Reservations ([`Intent::Admit`]) are all-or-nothing: a bound that
//!   cannot be funded in full is refused with the reason of the first
//!   exhausted gate.

use sqlx::types::Decimal;

pub(crate) mod db;
#[cfg(test)]
mod pg_tests;
#[cfg(test)]
mod regression_tests;

/// Why a spend that the plan allowance cannot cover was refused (or, at
/// settlement, left unfunded). The wire `code` is stable: the web and MCP
/// clients pick paywall copy from it (settings launch plan PR 3.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RefusalReason {
    /// The plan is Free (or the account has no plan) and its allowance is
    /// used up. Extra usage is not offered on Free. Paywall: upgrade.
    FreePlan,
    /// The subscription was cancelled (or lapsed): purchased credits are
    /// frozen until the account resubscribes (decision 19).
    CreditsFrozen,
    /// A paid plan whose extra usage is switched off (or never configured).
    ExtraUsageOff,
    /// A team-org member reached their per-member extra-usage cap.
    MemberCapReached,
    /// The org (or Pro account) reached its monthly extra-usage limit.
    MonthlyLimitReached,
    /// Extra usage is on and within its limits, but the prepaid balance is
    /// used up.
    ExtraBalanceExhausted,
}

impl RefusalReason {
    pub const ALL: [RefusalReason; 6] = [
        RefusalReason::FreePlan,
        RefusalReason::CreditsFrozen,
        RefusalReason::ExtraUsageOff,
        RefusalReason::MemberCapReached,
        RefusalReason::MonthlyLimitReached,
        RefusalReason::ExtraBalanceExhausted,
    ];

    /// The stable wire code carried in the 402 body and the MCP error.
    pub const fn code(self) -> &'static str {
        match self {
            RefusalReason::FreePlan => "free_plan",
            RefusalReason::CreditsFrozen => "credits_frozen",
            RefusalReason::ExtraUsageOff => "extra_usage_off",
            RefusalReason::MemberCapReached => "member_cap_reached",
            RefusalReason::MonthlyLimitReached => "monthly_limit_reached",
            RefusalReason::ExtraBalanceExhausted => "extra_balance_exhausted",
        }
    }
}

/// Plan-allowance headroom, in credits.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Headroom {
    /// No pool row exists and the caller's policy is to let the spend
    /// through (pre-flight's long-standing fail-open for a missing
    /// `credit_balances` row), bounded only by the member's pool cap.
    Unlimited,
    Credits(Decimal),
}

impl Headroom {
    fn min_with(self, cap: Option<Decimal>) -> Headroom {
        match (self, cap) {
            (h, None) => h,
            (Headroom::Unlimited, Some(c)) => Headroom::Credits(c),
            (Headroom::Credits(h), Some(c)) => Headroom::Credits(h.min(c)),
        }
    }

    fn plus(self, extra: Decimal) -> Headroom {
        match self {
            Headroom::Unlimited => Headroom::Unlimited,
            Headroom::Credits(h) => Headroom::Credits(h.saturating_add_dec(extra)),
        }
    }

    fn is_positive(self) -> bool {
        match self {
            Headroom::Unlimited => true,
            Headroom::Credits(h) => h > Decimal::ZERO,
        }
    }
}

/// `Decimal` addition that saturates instead of panicking on overflow.
trait SaturatingDec {
    fn saturating_add_dec(self, other: Decimal) -> Decimal;
}
impl SaturatingDec for Decimal {
    fn saturating_add_dec(self, other: Decimal) -> Decimal {
        self.checked_add(other).unwrap_or(Decimal::MAX)
    }
}

/// One member's allowance for the current period. `held` is the credits other
/// reservations of this member already hold (admission only; zero for
/// pre-flight and settlement).
#[derive(Clone, Debug, PartialEq)]
pub struct MemberAllowance {
    pub granted: Decimal,
    pub consumed: Decimal,
    pub held: Decimal,
    /// The member's cap on drawing the shared pool, already resolved: an
    /// explicit `cap_credits`, else the seat allowance for a seat-priced
    /// account, else `None` (uncapped).
    pub pool_cap: Option<Decimal>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AllowanceState {
    /// `None`: the payer has no current allowance period.
    pub member: Option<MemberAllowance>,
    /// Pool balance net of holds. `None`: no `credit_balances` row.
    pub pool_available: Option<Decimal>,
    /// What a missing pool row means. Pre-flight lets the spend through
    /// ([`Headroom::Unlimited`]); admission and settlement read it as zero.
    pub missing_pool_is_unlimited: bool,
}

impl AllowanceState {
    /// How many credits the plan allowance can still fund. With `frozen`, only
    /// the member's own allowance counts; the shared pool is closed.
    ///
    /// * own remaining = `granted - consumed - held`, floored at 0;
    /// * pool part (only past the own allowance) = pool net of the own part,
    ///   bounded by the cap's remaining room, and 0 while frozen.
    pub fn headroom(&self, frozen: bool) -> Headroom {
        let zero = Decimal::ZERO;
        let (own, cap_left) = match &self.member {
            Some(m) => {
                let used = m.consumed.saturating_add_dec(m.held);
                let own = (m.granted - used).max(zero);
                let overage = (used - m.granted).max(zero);
                let cap_left = m.pool_cap.map(|cap| (cap - overage).max(zero));
                (own, cap_left)
            }
            None => (zero, None),
        };
        let pool = if frozen {
            Headroom::Credits(zero)
        } else {
            match self.pool_available {
                Some(p) => Headroom::Credits((p - own).max(zero)),
                None if self.missing_pool_is_unlimited => Headroom::Unlimited,
                None => Headroom::Credits(zero),
            }
        };
        pool.min_with(cap_left).plus(own)
    }
}

/// `extra_usage_settings`, as far as the decision needs it. A missing row is
/// `ExtraState::settings == None` and reads as off.
#[derive(Clone, Debug, PartialEq)]
pub struct ExtraSettings {
    pub enabled: bool,
    pub monthly_limit_micro_usd: Option<i64>,
    pub member_cap_seat_multiple: Option<Decimal>,
}

/// Everything the extra-usage stage reads. Loaded only when the plan
/// allowance cannot cover the charge.
#[derive(Clone, Debug, PartialEq)]
pub struct ExtraState {
    /// Free plan, or no plan at all.
    pub plan_is_free: bool,
    /// A team org (not a personal account, not a personal org): the only
    /// account kind that has a per-member cap.
    pub is_team_org: bool,
    pub settings: Option<ExtraSettings>,
    /// The payer's seat allowance this period (credits); the cap is a
    /// multiple of it. Zero when the payer has no allowance period, which
    /// closes the cap for a team org (fail closed).
    pub seat_allowance_credits: Decimal,
    /// False for spend with no payer: there is no member to cap, so the
    /// per-member cap is not applied (the org limit and balance still are).
    pub payer_known: bool,
    /// Extra usage spent by the whole account this period (micro-USD).
    pub period_spent_micro_usd: i64,
    /// Extra usage spent by the payer this period (credits).
    pub member_spent_credits: Decimal,
    /// Credits held by outstanding reservations funded from extra usage:
    /// the whole account, and the payer (admission only).
    pub held_account_credits: Decimal,
    pub held_member_credits: Decimal,
    pub balance_micro_usd: i64,
    pub rate_micro_usd_per_credit: i64,
}

impl ExtraState {
    /// Placeholder for a decision the plan allowance fully covers: the extra
    /// stage is never read, so it is not loaded.
    pub fn unloaded() -> Self {
        ExtraState {
            plan_is_free: true,
            is_team_org: false,
            settings: None,
            seat_allowance_credits: Decimal::ZERO,
            payer_known: true,
            period_spent_micro_usd: 0,
            member_spent_credits: Decimal::ZERO,
            held_account_credits: Decimal::ZERO,
            held_member_credits: Decimal::ZERO,
            balance_micro_usd: 0,
            rate_micro_usd_per_credit: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FundingSnapshot {
    pub allowance: AllowanceState,
    /// The subscription is cancelled or lapsed (decision 19): the shared pool
    /// and purchased extra-usage credits are closed.
    pub credits_frozen: bool,
    pub extra: ExtraState,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Intent {
    /// Pre-flight: could any further spend be funded?
    Probe,
    /// Reservation: is this whole bound (credits) fundable? All or nothing.
    Admit(Decimal),
    /// Settlement: split this charge (credits). Never refuses; the part
    /// nothing can fund is returned as unfunded.
    Settle(Decimal),
}

/// What extra usage funds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtraFunding {
    pub credits: Decimal,
    pub micro_usd: i64,
}

impl ExtraFunding {
    pub const NONE: ExtraFunding = ExtraFunding { credits: Decimal::ZERO, micro_usd: 0 };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FundingOutcome {
    /// The whole charge is funded. `extra.micro_usd == 0` means the plan
    /// allowance alone covers it. For [`Intent::Probe`] the amounts are zero:
    /// only the verdict is meaningful.
    Funded { allowance_credits: Decimal, extra: ExtraFunding },
    /// Settlement only. `unfunded_credits` could not be funded by anything and
    /// `reason` names the gate that stopped extra usage.
    Partial {
        allowance_credits: Decimal,
        extra: ExtraFunding,
        unfunded_credits: Decimal,
        reason: RefusalReason,
    },
    /// Probe / Admit only.
    Refused(RefusalReason),
}

impl FundingOutcome {
    pub fn refusal(&self) -> Option<RefusalReason> {
        match self {
            FundingOutcome::Refused(r) => Some(*r),
            _ => None,
        }
    }

    /// True when the charge was funded without an extra-usage debit.
    pub fn is_allowance_only(&self) -> bool {
        matches!(self, FundingOutcome::Funded { extra, .. } if extra.micro_usd == 0)
    }
}

pub struct FundingPolicy;

impl FundingPolicy {
    /// See the module doc. Pure and total: it never panics on extreme inputs
    /// (decimal arithmetic saturates).
    pub fn decide(snapshot: &FundingSnapshot, intent: Intent) -> FundingOutcome {
        let probe = matches!(intent, Intent::Probe);
        let charge = match intent {
            Intent::Probe => Decimal::ZERO,
            Intent::Admit(c) | Intent::Settle(c) => c.max(Decimal::ZERO),
        };
        let headroom = snapshot.allowance.headroom(snapshot.credits_frozen);

        // 1. Plan allowance first.
        let from_allowance = match headroom {
            Headroom::Unlimited => charge,
            Headroom::Credits(h) => charge.min(h),
        };
        let covered = if probe { headroom.is_positive() } else { from_allowance >= charge };
        if covered {
            return FundingOutcome::Funded {
                allowance_credits: if probe { Decimal::ZERO } else { from_allowance },
                extra: ExtraFunding::NONE,
            };
        }

        // 2. Extra usage for the remainder.
        let need_credits = charge - from_allowance;
        let stage = Self::extra_stage(snapshot, need_credits, probe);
        match (stage, intent) {
            (Ok(ExtraStage { funded_micro, need_micro, .. }), _) if funded_micro >= need_micro => {
                FundingOutcome::Funded {
                    allowance_credits: from_allowance,
                    extra: if probe {
                        ExtraFunding::NONE
                    } else {
                        ExtraFunding { credits: need_credits, micro_usd: need_micro }
                    },
                }
            }
            (Ok(ExtraStage { funded_micro, reason, .. }), Intent::Settle(_)) => {
                let rate = Decimal::from(snapshot.extra.rate_micro_usd_per_credit.max(1));
                let credits = if funded_micro == 0 {
                    Decimal::ZERO
                } else {
                    (Decimal::from(funded_micro) / rate).min(need_credits)
                };
                FundingOutcome::Partial {
                    allowance_credits: from_allowance,
                    extra: ExtraFunding { credits, micro_usd: funded_micro },
                    unfunded_credits: need_credits - credits,
                    reason: reason.unwrap_or(RefusalReason::ExtraBalanceExhausted),
                }
            }
            (Ok(ExtraStage { reason, .. }), _) => {
                FundingOutcome::Refused(reason.unwrap_or(RefusalReason::ExtraBalanceExhausted))
            }
            (Err(reason), Intent::Settle(_)) => FundingOutcome::Partial {
                allowance_credits: from_allowance,
                extra: ExtraFunding::NONE,
                unfunded_credits: need_credits,
                reason,
            },
            (Err(reason), _) => FundingOutcome::Refused(reason),
        }
    }

    /// Frozen -> Free -> off are hard gates (`Err`). Past them, the three
    /// rooms (member cap, monthly limit, balance) bound how much extra usage
    /// can fund; `reason` names the first room smaller than the need.
    fn extra_stage(s: &FundingSnapshot, need_credits: Decimal, probe: bool) -> Result<ExtraStage, RefusalReason> {
        let e = &s.extra;
        if s.credits_frozen {
            return Err(RefusalReason::CreditsFrozen);
        }
        if e.plan_is_free {
            return Err(RefusalReason::FreePlan);
        }
        let Some(settings) = e.settings.as_ref().filter(|x| x.enabled) else {
            return Err(RefusalReason::ExtraUsageOff);
        };
        let rate = Decimal::from(e.rate_micro_usd_per_credit.max(1));
        let need_micro = if probe { 1 } else { ceil_micro(need_credits, rate) };

        let held_micro = ceil_micro(e.held_account_credits, rate);
        let cap_room = if e.is_team_org && e.payer_known {
            // A missing multiple on a team org reads as the 1x default: the
            // cap must not silently disappear.
            let multiple = settings.member_cap_seat_multiple.unwrap_or(Decimal::ONE);
            let cap_credits = multiple.checked_mul(e.seat_allowance_credits).unwrap_or(Decimal::MAX);
            let room = (cap_credits - e.member_spent_credits.saturating_add_dec(e.held_member_credits))
                .max(Decimal::ZERO);
            floor_micro(room, rate)
        } else {
            i64::MAX
        };
        // `enabled` requires a limit (CHECK). If it is somehow absent, fail
        // closed rather than treat it as unlimited.
        let limit = settings.monthly_limit_micro_usd.unwrap_or(0);
        let limit_room = limit
            .saturating_sub(e.period_spent_micro_usd)
            .saturating_sub(held_micro)
            .max(0);
        let balance_room = e.balance_micro_usd.saturating_sub(held_micro).max(0);

        let funded_micro = need_micro.min(cap_room).min(limit_room).min(balance_room);
        let reason = if cap_room < need_micro {
            Some(RefusalReason::MemberCapReached)
        } else if limit_room < need_micro {
            Some(RefusalReason::MonthlyLimitReached)
        } else if balance_room < need_micro {
            Some(RefusalReason::ExtraBalanceExhausted)
        } else {
            None
        };
        Ok(ExtraStage { funded_micro, need_micro, reason })
    }
}

struct ExtraStage {
    funded_micro: i64,
    need_micro: i64,
    reason: Option<RefusalReason>,
}

/// The rate extra usage is priced at. Only needed once the gates before it
/// have passed (extra usage enabled): an account with extra usage off must not
/// fail because no rate is configured.
pub(crate) fn rate_when_needed(enabled: bool, rate: Option<i64>) -> Result<i64, &'static str> {
    match rate.filter(|r| *r > 0) {
        Some(r) => Ok(r),
        None if enabled => Err("no positive extra-usage rate in force"),
        None => Ok(1),
    }
}

/// `ceil(credits * rate)` micro-USD, saturating.
pub(crate) fn ceil_micro(credits: Decimal, rate: Decimal) -> i64 {
    if credits <= Decimal::ZERO {
        return 0;
    }
    let micro = credits.checked_mul(rate).unwrap_or(Decimal::MAX).ceil();
    i64::try_from(micro).unwrap_or(i64::MAX)
}

/// `floor(credits * rate)` micro-USD, saturating.
fn floor_micro(credits: Decimal, rate: Decimal) -> i64 {
    if credits <= Decimal::ZERO {
        return 0;
    }
    let micro = credits.checked_mul(rate).unwrap_or(Decimal::MAX).floor();
    i64::try_from(micro).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;
