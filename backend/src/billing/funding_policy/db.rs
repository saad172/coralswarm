//! The SQL half of [`super::FundingPolicy`]: building a [`FundingSnapshot`]
//! from the control database, and writing an extra-usage debit.
//!
//! # Transactions and locking
//!
//! Every function takes the caller's `PgConnection`, which must be inside an
//! account-scoped transaction ([`crate::tenancy::scope::begin_account_scoped`]):
//! `member_allowance_periods`, `credit_balances`, `stripe_subscriptions` and
//! the `extra_usage_*` tables all carry `account_isolation` policies.
//!
//! * Reads for pre-flight need no lock.
//! * Admission (`managed_funding_tx`) and settlement run under
//!   [`crate::billing::renewal::lock_period_rotation`], which is also the
//!   `billing_enter_account` entry fence (0321). The caller takes it; this
//!   module only writes after it. Because that lock serialises every
//!   financial writer of the account, a snapshot read after it cannot change
//!   before the transaction commits, which is what lets concurrent
//!   settlements neither overshoot the limit nor drive the balance negative.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::{types::Decimal, PgConnection};
use uuid::Uuid;

use super::{
    rate_when_needed, AllowanceState, ExtraFunding, ExtraSettings, ExtraState, FundingOutcome, FundingPolicy,
    FundingSnapshot, Intent, MemberAllowance, RefusalReason,
};
use crate::billing::enforcement::status_is_frozen;

/// How spend with no payer is treated on a team org (settings launch plan
/// review of #1474; product decision pending, so both are implemented).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnattributedSpend {
    /// Charge the org's monthly limit and extra balance with no member cap.
    OrgLimit,
    /// Attribute it to the account owner (their cap and seat allowance apply).
    Owner,
}

/// The policy in force. A one-line change selects the other option.
pub(crate) const UNATTRIBUTED_SPEND: UnattributedSpend = UnattributedSpend::OrgLimit;

fn missing(what: &str) -> sqlx::Error {
    sqlx::Error::Protocol(format!("extra usage: {what}"))
}

/// Which period row is "the member's period".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PeriodSel {
    /// The period covering now.
    Current,
    /// The period a reservation was made against (`None`: it had none).
    Pinned(Option<Uuid>),
}

/// Which caller is asking; the three differ in clock, hold accounting, and in
/// what a missing pool row means. These differences are the pre-3.2 behaviour
/// of `check_blocked` and `managed_funding_tx`, preserved exactly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Request-path gate. `interactive` hides managed (background) spend.
    Preflight { interactive: bool },
    /// Reservation: holds count, the clock is `clock_timestamp()`.
    Admission,
    /// Settlement: no holds, nothing hidden.
    Settlement,
}

/// A member's allowance period as loaded, plus the facts extra usage needs.
#[derive(Clone, Debug)]
pub(crate) struct LoadedPeriod {
    pub id: Uuid,
    pub member: MemberAllowance,
}

pub(crate) async fn load_period(
    conn: &mut PgConnection,
    account: Uuid,
    payer: Uuid,
    mode: Mode,
    sel: PeriodSel,
) -> Result<Option<LoadedPeriod>, sqlx::Error> {
    let pinned = matches!(sel, PeriodSel::Pinned(_));
    let pinned_id = match sel {
        PeriodSel::Pinned(id) => id,
        PeriodSel::Current => None,
    };
    if pinned && pinned_id.is_none() {
        return Ok(None);
    }
    let hide_managed = matches!(mode, Mode::Preflight { interactive: true });
    let use_clock = matches!(mode, Mode::Admission);
    // `ORDER BY period_start DESC LIMIT 1` and the `cur_grant` lateral are the
    // exact selections the pre-3.2 gate used (see `enforcement`'s notes on why
    // both sites must pick the same row).
    let row: Option<(Uuid, Decimal, Decimal, Option<Decimal>, bool)> =
        sqlx::query_as(
            "SELECT p.id, p.granted_credits, \
                    p.consumed_credits - CASE WHEN $5 THEN p.managed_consumed_credits ELSE 0 END, \
                    p.cap_credits, \
                    COALESCE((SELECT g.plan_is_seat_priced FROM grants g \
                               WHERE g.account_id=$1 AND g.revoked_at IS NULL \
                               ORDER BY g.granted_at DESC, g.plan_is_seat_priced DESC, g.id DESC \
                               LIMIT 1), false) \
                    AND NOT EXISTS (SELECT 1 FROM org_trials t \
                                     WHERE t.account_id=$1 AND t.state='trialing') \
               FROM member_allowance_periods p \
              WHERE p.account_id=$1 AND p.user_id=$2 \
                AND (($3 AND p.id=$4) OR (NOT $3 \
                     AND p.period_start <= (CASE WHEN $6 THEN clock_timestamp() ELSE now() END) \
                     AND p.period_end > (CASE WHEN $6 THEN clock_timestamp() ELSE now() END))) \
              ORDER BY p.period_start DESC LIMIT 1",
        )
        .bind(account)
        .bind(payer)
        .bind(pinned)
        .bind(pinned_id)
        .bind(hide_managed)
        .bind(use_clock)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(id, granted, consumed, cap, seat_priced)| LoadedPeriod {
        id,
        member: MemberAllowance {
            granted,
            consumed,
            held: Decimal::ZERO,
            // An explicit admin cap wins; otherwise a seat-priced account
            // defaults to 1x the seat allowance and a flat plan is uncapped.
            // A trialing self-serve org reads as uncapped too: its members'
            // seat allowance is 0 and the trial's credits are ONE org pool
            // any member may spend (`billing::org_trial`, decision #1).
            pool_cap: cap.or_else(|| seat_priced.then_some(granted)),
        },
    }))
}

/// `status_is_frozen` over `stripe_subscriptions` (decision 19).
pub(crate) async fn load_frozen(conn: &mut PgConnection, account: Uuid) -> Result<bool, sqlx::Error> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account)
            .fetch_optional(&mut *conn)
            .await?;
    Ok(status_is_frozen(status.as_deref()))
}

/// The raw plan pool balance, `None` when the account has no row.
pub(crate) async fn load_pool(
    conn: &mut PgConnection,
    account: Uuid,
    mode: Mode,
) -> Result<Option<Decimal>, sqlx::Error> {
    let add_managed = matches!(mode, Mode::Preflight { interactive: true });
    sqlx::query_scalar(
        "SELECT balance + CASE WHEN $2 THEN managed_debited_credits ELSE 0 END \
           FROM credit_balances WHERE account_id = $1",
    )
    .bind(account)
    .bind(add_managed)
    .fetch_optional(&mut *conn)
    .await
}

/// Credits held against the PLAN allowance by outstanding reservations for one
/// member's period. A reservation funded partly by extra usage holds only its
/// allowance share here; its extra share is held by [`load_extra_holds`], so
/// no part of a bound is counted twice.
async fn load_member_held(
    conn: &mut PgConnection,
    account: Uuid,
    period: Uuid,
) -> Result<Decimal, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COALESCE(sum(bound_credits - COALESCE(extra_credits,0)),0) \
           FROM managed_credit_reservations \
          WHERE account_id=$1 AND member_period_id=$2 AND state='held'",
    )
    .bind(account)
    .bind(period)
    .fetch_one(&mut *conn)
    .await
}

/// Credits held by outstanding reservations across the whole account.
async fn load_account_held(conn: &mut PgConnection, account: Uuid) -> Result<Decimal, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COALESCE(sum(bound_credits - COALESCE(extra_credits,0)),0) \
           FROM managed_credit_reservations \
          WHERE account_id=$1 AND state='held'",
    )
    .bind(account)
    .fetch_one(&mut *conn)
    .await
}

/// The window extra usage is counted in: the UTC calendar month. It is
/// independent of the plan allowance period on purpose (see the module doc of
/// `funding_policy`): renewal and seat changes cut and recreate allowance
/// periods mid-month, and a limit tied to them restarted from zero.
pub(crate) async fn extra_window(
    conn: &mut PgConnection,
) -> Result<(DateTime<Utc>, DateTime<Utc>), sqlx::Error> {
    sqlx::query_as(
        "SELECT date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC', \
                (date_trunc('month', now() AT TIME ZONE 'UTC') + interval '1 month') AT TIME ZONE 'UTC'",
    )
    .fetch_one(&mut *conn)
    .await
}

/// Account-level facts the extra-usage stage reads.
#[derive(Clone, Debug)]
pub(crate) struct ExtraShared {
    pub plan_is_free: bool,
    pub is_team_org: bool,
    pub settings: Option<ExtraSettings>,
    pub balance_micro_usd: i64,
    pub rate_micro_usd_per_credit: i64,
    /// A self-serve org that has not been charged yet (`billing::org_trial`,
    /// decision #6): extra usage reads as off whatever the settings row says.
    pub trial_refuses_extra: bool,
}

pub(crate) async fn load_extra_shared(
    conn: &mut PgConnection,
    account: Uuid,
) -> Result<ExtraShared, sqlx::Error> {
    // No current grant reads as Free. A team org is an org account that is not
    // a personal org (a personal org is one person's Pro account: no cap).
    let (plan_is_free, is_team_org): (bool, bool) = sqlx::query_as(
        "SELECT COALESCE((SELECT p.code = 'free' FROM grants g JOIN plans p ON p.id = g.plan_id \
                           WHERE g.account_id=$1 AND g.revoked_at IS NULL \
                           ORDER BY g.granted_at DESC, g.plan_is_seat_priced DESC, g.id DESC LIMIT 1), true), \
                COALESCE(a.is_org_account AND NOT EXISTS( \
                    SELECT 1 FROM personal_orgs po WHERE po.clerk_org_id = a.clerk_org_id), false) \
           FROM accounts a WHERE a.id=$1",
    )
    .bind(account)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or((true, false));
    let settings: Option<(bool, Option<i64>, Option<Decimal>)> = sqlx::query_as(
        "SELECT enabled, monthly_limit_micro_usd, member_cap_seat_multiple \
           FROM extra_usage_settings WHERE account_id=$1",
    )
    .bind(account)
    .fetch_optional(&mut *conn)
    .await?;
    let balance: Option<i64> =
        sqlx::query_scalar("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1")
            .bind(account)
            .fetch_optional(&mut *conn)
            .await?;
    let rate: Option<i64> = sqlx::query_scalar(
        "SELECT micro_usd_per_credit FROM extra_usage_rates \
          WHERE effective_from <= clock_timestamp() ORDER BY effective_from DESC LIMIT 1",
    )
    .fetch_optional(&mut *conn)
    .await?;
    let trial_refuses_extra = crate::billing::org_trial::db::extra_usage_refused(conn, account).await?;
    let enabled = settings.as_ref().is_some_and(|s| s.0) && !trial_refuses_extra;
    let rate = rate_when_needed(enabled, rate).map_err(missing)?;
    Ok(ExtraShared {
        plan_is_free,
        is_team_org,
        settings: settings.map(|(enabled, limit, multiple)| ExtraSettings {
            enabled: enabled && !trial_refuses_extra,
            monthly_limit_micro_usd: limit,
            member_cap_seat_multiple: multiple,
        }),
        balance_micro_usd: balance.unwrap_or(0),
        rate_micro_usd_per_credit: rate,
        trial_refuses_extra,
    })
}

pub(crate) async fn load_period_spent(
    conn: &mut PgConnection,
    account: Uuid,
    period_start: DateTime<Utc>,
) -> Result<i64, sqlx::Error> {
    Ok(sqlx::query_scalar(
        "SELECT spent_micro_usd FROM extra_usage_periods WHERE account_id=$1 AND period_start=$2",
    )
    .bind(account)
    .bind(period_start)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or(0))
}

/// Only `entry_kind = 'debit'` rows count toward the member cap.
pub(crate) async fn load_member_spent(
    conn: &mut PgConnection,
    account: Uuid,
    payer: Uuid,
    period_start: DateTime<Utc>,
) -> Result<Decimal, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COALESCE(sum(credits),0) FROM extra_usage_ledger \
          WHERE account_id=$1 AND user_id=$2 AND period_start=$3 AND entry_kind='debit'",
    )
    .bind(account)
    .bind(payer)
    .bind(period_start)
    .fetch_one(&mut *conn)
    .await
}

async fn load_extra_holds(
    conn: &mut PgConnection,
    account: Uuid,
    payer: Uuid,
) -> Result<(Decimal, Decimal), sqlx::Error> {
    sqlx::query_as(
        "SELECT COALESCE(sum(extra_credits),0), \
                COALESCE(sum(extra_credits) FILTER (WHERE payer_id=$2),0) \
           FROM managed_credit_reservations \
          WHERE account_id=$1 AND state='held' AND extra_credits IS NOT NULL",
    )
    .bind(account)
    .bind(payer)
    .fetch_one(&mut *conn)
    .await
}

/// A one-shot evaluation for pre-flight and admission.
pub(crate) struct Evaluation {
    pub outcome: FundingOutcome,
    /// The member period the allowance was read from, and its terms.
    pub period: Option<LoadedPeriod>,
    /// `Pinned(Some(id))` was requested and the row is gone.
    pub pinned_period_missing: bool,
}

/// Load a snapshot for `payer` and run [`FundingPolicy::decide`].
///
/// Phase 1 reads only the plan allowance. When it covers the intent, the
/// extra-usage tables are never touched, so an account without extra usage
/// pays no new queries on its hot path.
pub(crate) async fn evaluate(
    conn: &mut PgConnection,
    account: Uuid,
    payer: Uuid,
    mode: Mode,
    sel: PeriodSel,
    intent: Intent,
) -> Result<Evaluation, sqlx::Error> {
    let mut period = load_period(conn, account, payer, mode, sel).await?;
    if matches!(sel, PeriodSel::Pinned(Some(_))) && period.is_none() {
        return Ok(Evaluation {
            outcome: FundingOutcome::Refused(RefusalReason::ExtraUsageOff),
            period: None,
            pinned_period_missing: true,
        });
    }
    let admission = mode == Mode::Admission;
    if let (true, Some(p)) = (admission, period.as_mut()) {
        p.member.held = load_member_held(conn, account, p.id).await?;
    }

    // Fast path, and the common one: the member's OWN remaining allowance
    // covers the intent, so the pool, the subscription and extra usage cannot
    // change the answer (`AllowanceState::headroom` adds to the own part, it
    // never subtracts from it; pinned by
    // `own_allowance_covering_the_intent_ignores_everything_else`). One query
    // for pre-flight, as before this PR.
    if let Some(p) = period.as_ref() {
        let m = &p.member;
        let own = (m.granted - m.consumed.saturating_add(m.held)).max(Decimal::ZERO);
        let charge = match intent {
            Intent::Probe => None,
            Intent::Admit(c) | Intent::Settle(c) => Some(c.max(Decimal::ZERO)),
        };
        let covered = charge.map_or(own > Decimal::ZERO, |c| own >= c);
        if covered {
            return Ok(Evaluation {
                outcome: FundingOutcome::Funded {
                    allowance_credits: charge.unwrap_or(Decimal::ZERO),
                    extra: ExtraFunding::NONE,
                },
                period,
                pinned_period_missing: false,
            });
        }
    }

    let held_account = if admission { load_account_held(conn, account).await? } else { Decimal::ZERO };
    let frozen = load_frozen(conn, account).await?;
    let balance = load_pool(conn, account, mode).await?;
    let allowance = AllowanceState {
        member: period.as_ref().map(|p| p.member.clone()),
        pool_available: balance.map(|b| b - held_account),
        // Pre-flight has always let a missing `credit_balances` row through.
        missing_pool_is_unlimited: matches!(mode, Mode::Preflight { .. }),
    };

    let mut snapshot = FundingSnapshot { allowance, credits_frozen: frozen, extra: ExtraState::unloaded() };
    let first = FundingPolicy::decide(&snapshot, intent);
    if matches!(first, FundingOutcome::Funded { .. }) {
        return Ok(Evaluation { outcome: first, period, pinned_period_missing: false });
    }

    let shared = load_extra_shared(conn, account).await?;
    let window = extra_window(conn).await?;
    let (extra_held_account, extra_held_member) = if admission {
        load_extra_holds(conn, account, payer).await?
    } else {
        (Decimal::ZERO, Decimal::ZERO)
    };
    snapshot.extra = ExtraState {
        plan_is_free: shared.plan_is_free,
        is_team_org: shared.is_team_org,
        settings: shared.settings,
        seat_allowance_credits: period.as_ref().map(|p| p.member.granted).unwrap_or(Decimal::ZERO),
        payer_known: true,
        period_spent_micro_usd: load_period_spent(conn, account, window.0).await?,
        member_spent_credits: load_member_spent(conn, account, payer, window.0).await?,
        held_account_credits: extra_held_account,
        held_member_credits: extra_held_member,
        balance_micro_usd: shared.balance_micro_usd,
        rate_micro_usd_per_credit: shared.rate_micro_usd_per_credit,
    };
    let outcome = FundingPolicy::decide(&snapshot, intent);
    Ok(Evaluation { outcome, period, pinned_period_missing: false })
}

// ── settlement ─────────────────────────────────────────────────────────────

/// How one charge is split. `allowance_credits + unfunded_credits` is what the
/// plan ledger (`credit_ledger` / `credit_balances` / member consumption)
/// bears; `extra` is what `extra_usage_ledger` bears.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Split {
    pub allowance_credits: Decimal,
    pub extra: ExtraFunding,
    pub unfunded_credits: Decimal,
    /// Why part of the charge is unfunded, when any is.
    pub reason: Option<RefusalReason>,
    pub rate_micro_usd_per_credit: i64,
    /// The calendar month the extra part counts in.
    pub window: (DateTime<Utc>, DateTime<Utc>),
    /// The payer and period lookup the ALLOWANCE part was decided with.
    pub payer: Option<Uuid>,
    pub sel: PeriodSel,
    /// Who the EXTRA part is attributed to (the member cap and the ledger
    /// `user_id`). Differs from `payer` only for unattributed spend under
    /// [`UnattributedSpend::Owner`]; `None` means charged to the org.
    pub extra_payer: Option<Uuid>,
}

impl Split {
    /// The credits the plan ledger records for this charge.
    pub fn plan_credits(&self) -> Decimal {
        self.allowance_credits + self.unfunded_credits
    }
}

#[derive(Clone, Debug)]
struct SharedState {
    pool: Option<Decimal>,
    frozen: bool,
    extra: Option<ExtraShared>,
    spent_by_window: HashMap<DateTime<Utc>, i64>,
}

/// Splits charges for one account inside one financial transaction.
///
/// Every `plan`/`record` pair must run under the account's
/// `lock_period_rotation`. The planner caches what it reads and applies every
/// recorded split to its own copy, so a batch of debits is decided against the
/// state the previous debits left, even though the plan ledger's totals are
/// only flushed at the end of the batch. The cache is valid because nothing
/// else can write the account's money while the lock is held.
///
/// Member state is cached by period row id, not by lookup key: a debit
/// resolving the payer's "current" period and a settlement pinned to that same
/// period are one row and must share one running `consumed`.
pub(crate) struct SettlementPlanner {
    account: Uuid,
    unattributed: UnattributedSpend,
    owner: Option<Option<Uuid>>,
    shared: Option<SharedState>,
    /// Lookup key -> the period row it resolved to (`None`: no such period).
    keys: HashMap<(Option<Uuid>, PeriodSel), Option<Uuid>>,
    periods: HashMap<Uuid, LoadedPeriod>,
    /// The payer's extra-usage spend (credits) in a window.
    member_spent: HashMap<(Uuid, DateTime<Utc>), Decimal>,
}

impl SettlementPlanner {
    pub fn new(account: Uuid) -> Self {
        Self::with_policy(account, UNATTRIBUTED_SPEND)
    }

    pub fn with_policy(account: Uuid, unattributed: UnattributedSpend) -> Self {
        Self {
            account,
            unattributed,
            owner: None,
            shared: None,
            keys: HashMap::new(),
            periods: HashMap::new(),
            member_spent: HashMap::new(),
        }
    }

    async fn ensure_shared(&mut self, conn: &mut PgConnection) -> Result<(), sqlx::Error> {
        if self.shared.is_none() {
            let pool = load_pool(conn, self.account, Mode::Settlement).await?;
            let frozen = load_frozen(conn, self.account).await?;
            self.shared = Some(SharedState { pool, frozen, extra: None, spent_by_window: HashMap::new() });
        }
        Ok(())
    }

    async fn ensure_payer(
        &mut self,
        conn: &mut PgConnection,
        payer: Option<Uuid>,
        sel: PeriodSel,
    ) -> Result<(), sqlx::Error> {
        if self.keys.contains_key(&(payer, sel)) {
            return Ok(());
        }
        let loaded = match payer {
            Some(payer) => load_period(conn, self.account, payer, Mode::Settlement, sel).await?,
            None => None,
        };
        let id = loaded.map(|p| {
            let id = p.id;
            // Keep the cached copy if another key already resolved this row:
            // it carries consumption the database does not have yet.
            self.periods.entry(id).or_insert(p);
            id
        });
        self.keys.insert((payer, sel), id);
        Ok(())
    }

    fn period(&self, payer: Option<Uuid>, sel: PeriodSel) -> Option<&LoadedPeriod> {
        self.keys.get(&(payer, sel)).copied().flatten().and_then(|id| self.periods.get(&id))
    }

    async fn owner_user(&mut self, conn: &mut PgConnection) -> Result<Option<Uuid>, sqlx::Error> {
        if self.owner.is_none() {
            let owner: Option<Uuid> = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
                .bind(self.account)
                .fetch_optional(&mut *conn)
                .await?;
            self.owner = Some(owner);
        }
        Ok(self.owner.flatten())
    }

    /// Decide how to split `charge` credits for `payer`. Reads only; call
    /// [`Self::record`] once the plan-ledger row has actually been inserted.
    pub async fn plan(
        &mut self,
        conn: &mut PgConnection,
        payer: Option<Uuid>,
        sel: PeriodSel,
        charge: Decimal,
    ) -> Result<Split, sqlx::Error> {
        self.ensure_shared(conn).await?;
        self.ensure_payer(conn, payer, sel).await?;
        let member = self.period(payer, sel).map(|p| p.member.clone());
        let shared = self.shared.as_ref().expect("loaded");
        let mut snapshot = FundingSnapshot {
            allowance: AllowanceState { member, pool_available: shared.pool, missing_pool_is_unlimited: false },
            credits_frozen: shared.frozen,
            extra: ExtraState::unloaded(),
        };
        let first = FundingPolicy::decide(&snapshot, Intent::Settle(charge));
        if matches!(first, FundingOutcome::Funded { .. }) {
            // The plan allowance covers it: no extra-usage read, and `window`
            // and `rate` are unused because nothing is debited from extra.
            let unused_window = (DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC);
            return Ok(Self::split_of(first, payer, sel, payer, unused_window, 1));
        }

        // Who the extra part is attributed to.
        let (extra_payer, extra_sel) = match (payer, self.unattributed) {
            (Some(p), _) => (Some(p), sel),
            (None, UnattributedSpend::Owner) => (self.owner_user(conn).await?, PeriodSel::Current),
            (None, UnattributedSpend::OrgLimit) => (None, sel),
        };
        self.ensure_payer(conn, extra_payer, extra_sel).await?;

        if self.shared.as_ref().expect("loaded").extra.is_none() {
            let extra = load_extra_shared(conn, self.account).await?;
            self.shared.as_mut().expect("loaded").extra = Some(extra);
        }
        let window = extra_window(conn).await?;
        let start = window.0;
        if !self.shared.as_ref().expect("loaded").spent_by_window.contains_key(&start) {
            let spent = load_period_spent(conn, self.account, start).await?;
            self.shared.as_mut().expect("loaded").spent_by_window.insert(start, spent);
        }
        let mut member_spent = Decimal::ZERO;
        if let Some(who) = extra_payer {
            if !self.member_spent.contains_key(&(who, start)) {
                let spent = load_member_spent(conn, self.account, who, start).await?;
                self.member_spent.insert((who, start), spent);
            }
            member_spent = self.member_spent[&(who, start)];
        }
        let seat_allowance = self
            .period(extra_payer, extra_sel)
            .map(|p| p.member.granted)
            .unwrap_or(Decimal::ZERO);
        let shared = self.shared.as_ref().expect("loaded");
        let extra = shared.extra.as_ref().expect("loaded");
        snapshot.extra = ExtraState {
            plan_is_free: extra.plan_is_free,
            is_team_org: extra.is_team_org,
            settings: extra.settings.clone(),
            seat_allowance_credits: seat_allowance,
            payer_known: extra_payer.is_some(),
            period_spent_micro_usd: shared.spent_by_window.get(&start).copied().unwrap_or(0),
            member_spent_credits: member_spent,
            held_account_credits: Decimal::ZERO,
            held_member_credits: Decimal::ZERO,
            balance_micro_usd: extra.balance_micro_usd,
            rate_micro_usd_per_credit: extra.rate_micro_usd_per_credit,
        };
        let outcome = FundingPolicy::decide(&snapshot, Intent::Settle(charge));
        Ok(Self::split_of(outcome, payer, sel, extra_payer, window, extra.rate_micro_usd_per_credit))
    }

    fn split_of(
        outcome: FundingOutcome,
        payer: Option<Uuid>,
        sel: PeriodSel,
        extra_payer: Option<Uuid>,
        window: (DateTime<Utc>, DateTime<Utc>),
        rate: i64,
    ) -> Split {
        match outcome {
            FundingOutcome::Funded { allowance_credits, extra } => Split {
                allowance_credits,
                extra,
                unfunded_credits: Decimal::ZERO,
                reason: None,
                rate_micro_usd_per_credit: rate,
                window,
                payer,
                sel,
                extra_payer,
            },
            FundingOutcome::Partial { allowance_credits, extra, unfunded_credits, reason } => Split {
                allowance_credits,
                extra,
                unfunded_credits,
                reason: Some(reason),
                rate_micro_usd_per_credit: rate,
                window,
                payer,
                sel,
                extra_payer,
            },
            // `decide` never refuses a settlement; if that ever changes, stop
            // loudly rather than invent a split.
            FundingOutcome::Refused(r) => {
                unreachable!("FundingPolicy::decide refused a Settle intent: {r:?}")
            }
        }
    }

    /// Apply a split to the cache after its plan-ledger row was inserted.
    pub fn record(&mut self, split: &Split) {
        let written = split.plan_credits();
        if let Some(shared) = self.shared.as_mut() {
            shared.pool = Some(shared.pool.unwrap_or(Decimal::ZERO) - written);
            if split.extra.micro_usd > 0 {
                if let Some(extra) = shared.extra.as_mut() {
                    extra.balance_micro_usd -= split.extra.micro_usd;
                }
                *shared.spent_by_window.entry(split.window.0).or_insert(0) += split.extra.micro_usd;
            }
        }
        if let Some(id) = self.keys.get(&(split.payer, split.sel)).copied().flatten() {
            if let Some(period) = self.periods.get_mut(&id) {
                period.member.consumed += written;
            }
        }
        if let Some(who) = split.extra_payer {
            if let Some(spent) = self.member_spent.get_mut(&(who, split.window.0)) {
                *spent += split.extra.credits;
            }
        }
    }
}

/// One extra-usage debit to write.
pub(crate) struct ExtraDebit<'a> {
    pub account: Uuid,
    pub split: &'a Split,
    /// Every key starts `debit:<kind>:<account>:`: `idempotency_key` is
    /// globally unique, so a key without the account collides across accounts
    /// (a journal debit for the same graph/period did, in review).
    pub idempotency_key: String,
    pub source_job_id: Option<Uuid>,
    pub note: Option<&'a str>,
}

/// The idempotency key for a debit from `kind` (`job`, `receipt`, `query`,
/// `journal`) of `account`; `source` identifies the unit of work.
pub(crate) fn debit_key(kind: &str, account: Uuid, source: impl std::fmt::Display) -> String {
    format!("debit:{kind}:{account}:{source}")
}

/// Write the extra-usage side of a split: the ledger row, the balance and the
/// period running total. The caller holds `lock_period_rotation` (which is the
/// `billing_enter_account` entry fence, so the guard triggers admit the writes).
///
/// A conflicting idempotency key is an error, not a no-op: the caller only
/// reaches here after inserting a NEW plan-ledger row for the same source, so
/// an existing extra row means the two ledgers disagree about that source.
pub(crate) async fn write_extra_debit(
    conn: &mut PgConnection,
    debit: &ExtraDebit<'_>,
) -> Result<(), sqlx::Error> {
    let split = debit.split;
    let micro = split.extra.micro_usd;
    if micro <= 0 {
        return Ok(());
    }
    let (start, end) = split.window;

    // One row per account per UTC calendar month: no overlap is possible.
    sqlx::query(
        "INSERT INTO extra_usage_periods (account_id, period_start, period_end) \
         VALUES ($1, $2, $3) ON CONFLICT (account_id, period_start) DO NOTHING",
    )
    .bind(debit.account)
    .bind(start)
    .bind(end)
    .execute(&mut *conn)
    .await?;

    let inserted = sqlx::query(
        "INSERT INTO extra_usage_ledger \
            (account_id, entry_kind, amount_micro_usd, credits, rate_micro_usd_per_credit, \
             user_id, period_start, idempotency_key, source_job_id, note) \
         VALUES ($1, 'debit', $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind(debit.account)
    .bind(-micro)
    .bind(split.extra.credits)
    .bind(split.rate_micro_usd_per_credit)
    .bind(split.extra_payer)
    .bind(start)
    .bind(&debit.idempotency_key)
    .bind(debit.source_job_id)
    .bind(debit.note)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if inserted != 1 {
        return Err(missing("a debit with this idempotency key already exists for a new plan-ledger row"));
    }

    // `balance_micro_usd >= 0` is also a CHECK; the guard keeps the failure a
    // clear error instead of a constraint violation.
    let updated = sqlx::query(
        "UPDATE extra_usage_balances SET balance_micro_usd = balance_micro_usd - $2, updated_at=now() \
          WHERE account_id=$1 AND balance_micro_usd >= $2",
    )
    .bind(debit.account)
    .bind(micro)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if updated != 1 {
        return Err(missing("balance too small for a clamped debit (planner and ledger disagree)"));
    }

    sqlx::query(
        "UPDATE extra_usage_periods p SET spent_micro_usd = p.spent_micro_usd + $3, \
                spent_credits = p.spent_credits + $4, updated_at=now(), \
                limit_reached_at = CASE WHEN p.limit_reached_at IS NULL AND p.spent_micro_usd + $3 >= \
                    COALESCE((SELECT s.monthly_limit_micro_usd FROM extra_usage_settings s \
                               WHERE s.account_id=p.account_id AND s.enabled), 9223372036854775807) \
                    THEN now() ELSE p.limit_reached_at END \
          WHERE p.account_id=$1 AND p.period_start=$2",
    )
    .bind(debit.account)
    .bind(start)
    .bind(micro)
    .bind(split.extra.credits)
    .execute(&mut *conn)
    .await?;
    Ok(())
}
