//! Extra-usage auto-reload (settings launch plan PR 3.5, decisions 11-13, 19,
//! 22, 23): when the prepaid balance drops under the customer's threshold,
//! charge their saved card off-session for the configured amount and credit
//! the extra-usage ledger.
//!
//! This charges a customer's card with nobody present. The two ways to get it
//! wrong are charging twice and charging past the monthly limit; everything
//! below is arranged around those.
//!
//! # One charge, ever (the double-charge rules)
//!
//! 1. **One reload in flight per account.** The decision is a row in
//!    `extra_usage_reloads`; a partial unique index (0367) allows one row in
//!    `created`/`submitted` per account. Two workers (or two processes) racing
//!    for the same account serialise on the account's financial lock, and the
//!    loser either sees the row or hits the index. Nothing is sent to Stripe
//!    before the row is committed.
//! 2. **A stable Stripe idempotency key.** The PaymentIntent is created under
//!    `reload:<row id>` (a generated column, so it cannot be set to anything
//!    else), with parameters stored on the row at decision time (customer and
//!    card, migration 0374) so every retry sends byte-identical parameters.
//!    A crash after Stripe created the charge but before we recorded it is
//!    therefore recovered by re-sending the same request, which Stripe answers
//!    with the same PaymentIntent. There is no code path that creates a
//!    PaymentIntent for a reload under any other key, or twice under one.
//! 3. **Credit once.** The ledger credit is keyed `reload:<row id>` and the
//!    payment intent id and reload id are unique on the ledger too. The sync
//!    answer, the `payment_intent.succeeded` webhook (replayed, resent, out of
//!    order) and the reaper all go through [`settle`], and only the first one
//!    to move the row to `succeeded` credits.
//! 4. **Money received is credited, whatever the row says.** A success for a
//!    row we had marked `failed` or `abandoned` is credited: the customer was
//!    charged. Only an event that does not match our row (amount, currency,
//!    customer, account, payment intent) is refused, loudly.
//! 5. **Never guess.** Any outcome we cannot classify (timeout, 5xx, 429,
//!    unparseable answer) leaves the row `created` and is retried with the same
//!    key, or reconciled by READING Stripe ([`reconcile_one`]). A row is moved
//!    to `failed` only on an answer that proves no charge happened (a card
//!    error, or a 4xx rejection). It is moved to `abandoned` only after Stripe
//!    was asked and has no PaymentIntent for it.
//!
//! # Never past the monthly limit (sizing)
//!
//! The limit (decision 12, 22) counts debits per UTC calendar month in
//! `extra_usage_periods.spent_micro_usd`. A reload is sized by [`size_reload`]:
//!
//! ```text
//! reload = min( configured amount,
//!               limit - spent this month - balance,        // room to spend it
//!               limit - already charged by reload this month )  // belt and braces
//! ```
//!
//! rounded DOWN to whole cents, and no reload at all when that is under $1.
//!
//! Why this cannot overshoot: `spent + balance` is the most that can ever have
//! been spent this month if the whole balance were spent. Every debit moves
//! `spent` up and `balance` down by the same amount, so the sum is unchanged by
//! debits. A reload sized so that `spent + balance + reload <= limit` therefore
//! keeps the sum under the limit no matter how debits interleave between the
//! decision and the credit. Once the sum equals the limit nothing more is ever
//! bought this month, and the next month starts the count over. The third term
//! covers the one thing the first cannot see: money taken that the balance no
//! longer shows (a manual adjustment or refund), which would otherwise buy the
//! same room twice.
//!
//! The decision and every debit run under the same per-account lock
//! (`billing_enter_account`, 0321), so sizing sees a consistent snapshot. A
//! customer lowering their own limit in the seconds between the decision and
//! the charge is not re-checked once the charge is sent: the money is theirs
//! and is credited, it just waits for next month.
//!
//! # When a reload does not happen
//!
//! [`Skip`]: auto-reload off, Free plan, credits frozen (cancelled or
//! `past_due`, decisions 19, 23), a cancellation scheduled (`cancel_at_period_end`
//! or `cancel_at`: the credits would freeze at period end, so the customer would
//! pay for a balance they cannot spend), a reload already in flight, still backing off
//! after a failure, balance not under the threshold, no room under the limit,
//! no card on file. Refunds and disputes are manual (decision 18).
//!
//! # Failures
//!
//! A decline, an authentication-required card (3DS cannot be completed with the
//! customer absent) or a rejected request marks the row `failed`, emails the
//! admins ([`crate::billing::notices`]) and starts a backoff: 1 hour after the
//! first consecutive failure, 6 hours after the second. After
//! [`MAX_CONSECUTIVE_FAILURES`] in a row auto-reload is turned off and the
//! email says so. "In a row" counts failed reloads since the last success and
//! since the customer last turned auto-reload ON (`auto_reload_reset_at`,
//! set only on an off -> on change), so turning it back on gives a fresh count
//! but an unrelated settings save does not. A missing card is not a strike: one notice per month.
//!
//! # Reaper
//!
//! [`run_pass`] first reconciles in-flight rows older than a bound
//! ([`DRIVE_AFTER_SECS`] for `created`, [`RECONCILE_SUBMITTED_AFTER_SECS`] for
//! `submitted`). See [`reconcile_one`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::webhook::{proto, Tx};
use crate::billing::entry_queue::EntryTurn;
use super::{CENT_MICRO_USD, MICRO_PER_USD};
use crate::billing::funding_policy::db::{load_extra_shared, load_frozen};
use crate::billing::notices::{self, NoticeKind};
use crate::billing::stripe_client::{StripeClient, StripeError};

pub mod settle;
pub mod stripe;

#[cfg(test)]
mod pg_tests;
#[cfg(test)]
mod tests;

use self::settle::{apply_observation, Observation};
use self::stripe::{payment_intent_params, PaymentIntentFacts};

/// `metadata[purpose]` on a reload's PaymentIntent. The webhook routes on it:
/// every other PaymentIntent (subscription invoices, top-up Checkout) is none of
/// this module's business.
pub const PURPOSE: &str = "extra_usage_auto_reload";

/// Failed reloads in a row that turn auto-reload off.
pub const MAX_CONSECUTIVE_FAILURES: i64 = 3;
/// A reload smaller than this is not worth a card charge.
pub const MIN_RELOAD_MICRO_USD: i64 = MICRO_PER_USD;
/// A `created` row older than this is picked up by the reaper (a live driver
/// finishes inside Stripe's client timeout budget, well under this).
pub const DRIVE_AFTER_SECS: i64 = 60;
/// A `submitted` row still unsettled after this is read back from Stripe.
pub const RECONCILE_SUBMITTED_AFTER_SECS: i64 = 300;
/// Within this age a `created` row may be re-sent under its original key
/// (Stripe keeps keys for 24 hours; this stays far inside that). Older rows are
/// only reconciled by looking at Stripe, never re-sent.
pub const REDRIVE_MAX_AGE_SECS: i64 = 3_600;
/// A `created` row with no PaymentIntent at Stripe is abandoned only once it is
/// at least this old, so a request still in flight cannot be raced.
pub const ABANDON_AFTER_SECS: i64 = 120;
/// How long a no-card account is left alone before Stripe is asked again.
const NO_CARD_RECHECK: Duration = Duration::from_secs(600);

/// One `extra_usage_reloads` row, as the driver sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct ReloadRow {
    pub id: Uuid,
    pub account_id: Uuid,
    pub amount_micro_usd: i64,
    pub state: String,
    pub idempotency_key: String,
    pub payment_intent_id: Option<String>,
    pub customer_id: Option<String>,
    pub payment_method_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl ReloadRow {
    const COLUMNS: &'static str = "id, account_id, amount_micro_usd, state, stripe_idempotency_key, \
        stripe_payment_intent_id, stripe_customer_id, stripe_payment_method_id, created_at";
}

type RowTuple = (Uuid, Uuid, i64, String, String, Option<String>, Option<String>, Option<String>, DateTime<Utc>);

impl From<RowTuple> for ReloadRow {
    fn from(t: RowTuple) -> Self {
        ReloadRow {
            id: t.0,
            account_id: t.1,
            amount_micro_usd: t.2,
            state: t.3,
            idempotency_key: t.4,
            payment_intent_id: t.5,
            customer_id: t.6,
            payment_method_id: t.7,
            created_at: t.8,
        }
    }
}

/// Why an account was not reloaded on this pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Skip {
    Off,
    Free,
    Frozen,
    /// The subscription is set to cancel (at period end or on a date): credits
    /// bought now would freeze.
    CancelScheduled,
    InFlight,
    Backoff,
    AboveThreshold,
    NoRoom,
    NoPaymentMethod,
    /// The account has an `account_deletions` row (grace, running or
    /// completed): never charged. A cancelled deletion does not count.
    Deleting,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RunOutcome {
    Skipped(Skip),
    /// The row is now in this state (`succeeded`, `failed`, `submitted`,
    /// `abandoned`, or still `created`).
    Reloaded { reload_id: Uuid, state: String },
    /// Stripe could not be reached or answered something unclassifiable.
    /// Nothing was lost: the row (if any) stays and is retried or reconciled.
    Deferred(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PassStats {
    pub reloaded: usize,
    pub reconciled: usize,
    pub errors: usize,
}

// ── pure rules ─────────────────────────────────────────────────────────────

/// How many micro-USD to charge. See the module doc, "Never past the monthly
/// limit". Returns 0 for "do not reload". Inputs that cannot be real (a
/// negative spend, balance or amount) size to 0 rather than being trusted.
pub fn size_reload(
    configured: i64,
    limit: i64,
    spent_this_month: i64,
    balance: i64,
    reloaded_this_month: i64,
) -> i64 {
    if configured <= 0 || limit <= 0 || spent_this_month < 0 || balance < 0 || reloaded_this_month < 0 {
        return 0;
    }
    let room_to_spend = i128::from(limit) - i128::from(spent_this_month) - i128::from(balance);
    let room_to_charge = i128::from(limit) - i128::from(reloaded_this_month);
    let amount = i128::from(configured).min(room_to_spend).min(room_to_charge);
    if amount < i128::from(MIN_RELOAD_MICRO_USD) {
        return 0;
    }
    let cents = i128::from(CENT_MICRO_USD);
    i64::try_from(amount - amount % cents).unwrap_or(0)
}

/// How long to wait after `n` failed reloads in a row before trying again.
pub fn failure_backoff(consecutive_failures: i64) -> Duration {
    Duration::from_secs(match consecutive_failures {
        i64::MIN..=0 => 0,
        1 => 3_600,
        2 => 6 * 3_600,
        _ => 24 * 3_600,
    })
}

/// Whether a signed Stripe event is about an auto-reload PaymentIntent.
pub fn is_reload_event(event_type: &str, object: &serde_json::Value) -> bool {
    matches!(
        event_type,
        "payment_intent.succeeded"
            | "payment_intent.payment_failed"
            | "payment_intent.requires_action"
            | "payment_intent.canceled"
    ) && object
        .get("metadata")
        .and_then(|m| m.get("purpose"))
        .and_then(serde_json::Value::as_str)
        == Some(PURPOSE)
}

// ── the decision ───────────────────────────────────────────────────────────

enum Decision {
    Skip(Skip),
    Go { amount: i64, period_start: DateTime<Utc>, customer: String },
}

/// Open the account's financial transaction: scoped, and past the entry fence
/// (`billing_enter_account`, which also takes the per-account advisory lock).
///
/// This worker runs at worker rate, so it goes through the in-process entry
/// queue (#1497): the turn is taken BEFORE the connection and the lock wait
/// is bounded. Drop the returned turn right after the transaction commits,
/// and before calling anything that opens another financial transaction for
/// the same account (`note_no_card`, `drive`, `apply_observation`): the
/// account's turn is not re-entrant.
///
/// A skip (queue or lock timeout) is returned before any write; the pass
/// counts it as an error for this account and moves on, and the next pass
/// retries. No reload row is created, marked or struck by a skip.
async fn open(control: &PgPool, account: Uuid) -> Result<(EntryTurn, Tx), sqlx::Error> {
    crate::billing::entry_queue::begin_entered(account, "auto_reload", || {
        crate::tenancy::scope::begin_account_scoped(control, account)
    })
    .await
}

type SettingsRow = (bool, Option<i64>, bool, Option<i64>, Option<i64>, Option<DateTime<Utc>>);

/// Is the subscription scheduled to cancel? Columns from 0367, written by the
/// subscription webhook.
async fn cancel_scheduled(tx: &mut Tx, account: Uuid) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT cancel_at_period_end OR cancel_at IS NOT NULL FROM stripe_subscriptions WHERE account_id = $1",
    )
    .bind(account)
    .fetch_optional(&mut **tx)
    .await?
    .unwrap_or(false))
}

/// This UTC calendar month's start, what was spent in it and what reloads
/// already bought in it. The window the limit counts in (#1474). Periods
/// OVERLAPPING it are summed, which is the month's row when periods are
/// monthly and a safe over-count (a smaller reload) if one straddles it.
async fn month_totals(tx: &mut Tx, account: Uuid) -> Result<(DateTime<Utc>, i64, i64), sqlx::Error> {
    sqlx::query_as(
        "WITH m AS (SELECT date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS s, \
                           (date_trunc('month', now() AT TIME ZONE 'UTC') + interval '1 month') AT TIME ZONE 'UTC' AS e) \
         SELECT m.s, \
                COALESCE((SELECT sum(p.spent_micro_usd) FROM extra_usage_periods p \
                           WHERE p.account_id = $1 AND p.period_start < m.e AND p.period_end > m.s), 0)::bigint, \
                COALESCE((SELECT sum(r.amount_micro_usd) FROM extra_usage_reloads r \
                           WHERE r.account_id = $1 AND r.state = 'succeeded' AND r.settled_at >= m.s), 0)::bigint \
           FROM m",
    )
    .bind(account)
    .fetch_one(&mut **tx)
    .await
}

/// Does this account have a non-cancelled `account_deletions` row (state
/// `grace`, `running` or `completed`)? Asked through 0370's SECURITY DEFINER
/// `account_deletion_state_for_account`, which answers only for an account that
/// is the deleted user's own (they own it and nobody else is a member); an
/// account they merely co-owned is not theirs to refuse billing for.
async fn being_deleted(tx: &mut Tx, account: Uuid) -> Result<bool, sqlx::Error> {
    let state: Option<String> = sqlx::query_scalar("SELECT account_deletion_state_for_account($1)")
        .bind(account)
        .fetch_one(&mut **tx)
        .await?;
    Ok(state.is_some())
}

/// Decide, under the account lock, whether to reload and for how much.
async fn decide(tx: &mut Tx, account: Uuid) -> Result<Decision, sqlx::Error> {
    let settings: Option<SettingsRow> = sqlx::query_as(
        "SELECT enabled, monthly_limit_micro_usd, auto_reload_enabled, \
                auto_reload_threshold_micro_usd, auto_reload_amount_micro_usd, auto_reload_reset_at \
           FROM extra_usage_settings WHERE account_id = $1",
    )
    .bind(account)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((true, Some(limit), true, Some(threshold), Some(amount), reset_at)) = settings else {
        return Ok(Decision::Skip(Skip::Off));
    };
    // Settings 2.4: an account being deleted (or already deleted) is never
    // charged, whatever its settings say.
    if being_deleted(tx, account).await? {
        return Ok(Decision::Skip(Skip::Deleting));
    }
    let shared = load_extra_shared(tx, account).await?;
    if shared.plan_is_free {
        return Ok(Decision::Skip(Skip::Free));
    }
    if load_frozen(tx, account).await? {
        return Ok(Decision::Skip(Skip::Frozen));
    }
    if cancel_scheduled(tx, account).await? {
        return Ok(Decision::Skip(Skip::CancelScheduled));
    }
    let in_flight: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM extra_usage_reloads WHERE account_id = $1 AND state IN ('created','submitted'))",
    )
    .bind(account)
    .fetch_one(&mut **tx)
    .await?;
    if in_flight {
        return Ok(Decision::Skip(Skip::InFlight));
    }

    // Backoff: failures since the last success and since the customer last
    // turned auto-reload on. NOT since the last settings save: any save would
    // otherwise restart the count and an unattended card could be charged
    // without the three-strikes switch-off ever firing.
    let (streak, last_failed_at, now): (i64, Option<DateTime<Utc>>, DateTime<Utc>) = sqlx::query_as(
        "SELECT count(*), max(settled_at), now() FROM extra_usage_reloads \
          WHERE account_id = $1 AND state = 'failed' \
            AND created_at > GREATEST( \
                  COALESCE((SELECT max(settled_at) FROM extra_usage_reloads \
                             WHERE account_id = $1 AND state = 'succeeded'), '-infinity'::timestamptz), \
                  COALESCE($2, '-infinity'::timestamptz))",
    )
    .bind(account)
    .bind(reset_at)
    .fetch_one(&mut **tx)
    .await?;
    if let Some(at) = last_failed_at {
        let wait = chrono::Duration::from_std(failure_backoff(streak)).unwrap_or(chrono::Duration::zero());
        if now < at + wait {
            return Ok(Decision::Skip(Skip::Backoff));
        }
    }

    if shared.balance_micro_usd >= threshold {
        return Ok(Decision::Skip(Skip::AboveThreshold));
    }

    let (month_start, spent, reloaded) = month_totals(tx, account).await?;
    let size = size_reload(amount, limit, spent, shared.balance_micro_usd, reloaded);
    if size == 0 {
        return Ok(Decision::Skip(Skip::NoRoom));
    }

    let customer: Option<String> =
        sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account)
            .fetch_optional(&mut **tx)
            .await?;
    match customer.filter(|c| !c.is_empty()) {
        Some(customer) => Ok(Decision::Go { amount: size, period_start: month_start, customer }),
        None => Ok(Decision::Skip(Skip::NoPaymentMethod)),
    }
}

static NO_CARD_UNTIL: Mutex<Option<HashMap<Uuid, Instant>>> = Mutex::new(None);

fn no_card_recently(account: Uuid) -> bool {
    let mut g = NO_CARD_UNTIL.lock().unwrap_or_else(|e| e.into_inner());
    let map = g.get_or_insert_with(HashMap::new);
    map.retain(|_, until| *until > Instant::now());
    map.contains_key(&account)
}

fn remember_no_card(account: Uuid) {
    let mut g = NO_CARD_UNTIL.lock().unwrap_or_else(|e| e.into_inner());
    g.get_or_insert_with(HashMap::new).insert(account, Instant::now() + NO_CARD_RECHECK);
}

/// Tell the customer once per UTC month that a reload was due and there is no
/// card to charge. Not a failed reload: no row, no strike.
async fn note_no_card(control: &PgPool, account: Uuid) -> Result<(), sqlx::Error> {
    let (turn, mut tx) = open(control, account).await?;
    let key: String = sqlx::query_scalar("SELECT 'no_payment_method:' || to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM')")
        .fetch_one(&mut *tx)
        .await?;
    let claimed = notices::claim(&mut tx, account, NoticeKind::AutoReloadFailed, &key, None).await?;
    tx.commit().await?;
    drop(turn);
    if claimed {
        tracing::warn!(%account, "auto-reload due but the customer has no default payment method; notice claimed");
    }
    Ok(())
}

// ── the worker's entry points ──────────────────────────────────────────────

/// Decide for one account and, if a reload is due, claim it and drive it.
pub async fn run_account(control: &PgPool, stripe: &StripeClient, account: Uuid) -> Result<RunOutcome, sqlx::Error> {
    // Cheap first look. Nothing is sent anywhere for an account that does not
    // need a reload, and the common answer costs one short transaction.
    let customer = {
        let (turn, mut tx) = open(control, account).await?;
        let d = decide(&mut tx, account).await?;
        tx.commit().await?;
        // `note_no_card` below opens its own financial transaction.
        drop(turn);
        match d {
            Decision::Skip(Skip::NoPaymentMethod) => {
                note_no_card(control, account).await?;
                return Ok(RunOutcome::Skipped(Skip::NoPaymentMethod));
            }
            Decision::Skip(s) => return Ok(RunOutcome::Skipped(s)),
            Decision::Go { customer, .. } => customer,
        }
    };
    if no_card_recently(account) {
        return Ok(RunOutcome::Skipped(Skip::NoPaymentMethod));
    }
    // The card is read from Stripe OUTSIDE any transaction, then stored on the
    // row with the decision.
    let pm = match stripe.customer_default_payment_method(&customer).await {
        Ok(Some(pm)) => pm,
        Ok(None) => {
            remember_no_card(account);
            note_no_card(control, account).await?;
            return Ok(RunOutcome::Skipped(Skip::NoPaymentMethod));
        }
        Err(e) => {
            tracing::warn!(%account, error = %e, "auto-reload: could not read the customer's default card; will retry");
            return Ok(RunOutcome::Deferred(e.to_string()));
        }
    };

    // Decide again under the lock and claim the row in the same transaction.
    let (turn, mut tx) = open(control, account).await?;
    let (amount, period_start, customer) = match decide(&mut tx, account).await? {
        Decision::Go { amount, period_start, customer } => (amount, period_start, customer),
        Decision::Skip(s) => {
            tx.commit().await?;
            return Ok(RunOutcome::Skipped(s));
        }
    };
    let claimed: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO extra_usage_reloads \
            (account_id, amount_micro_usd, period_start, stripe_customer_id, stripe_payment_method_id) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (account_id) WHERE state IN ('created','submitted') DO NOTHING \
         RETURNING id",
    )
    .bind(account)
    .bind(amount)
    .bind(period_start)
    .bind(&customer)
    .bind(&pm)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    // `drive` settles the answer in its own financial transaction.
    drop(turn);
    let Some(reload_id) = claimed else {
        return Ok(RunOutcome::Skipped(Skip::InFlight));
    };
    tracing::info!(%account, %reload_id, amount_micro_usd = amount, "auto-reload: claimed");
    drive(control, stripe, account, reload_id).await
}

async fn load_row(control: &PgPool, account: Uuid, id: Uuid) -> Result<Option<ReloadRow>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account).await.map_err(proto)?;
    let row: Option<RowTuple> = sqlx::query_as(&format!(
        "SELECT {} FROM extra_usage_reloads WHERE id = $1 AND account_id = $2",
        ReloadRow::COLUMNS
    ))
    .bind(id)
    .bind(account)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row.map(Into::into))
}

fn reloaded(row_id: Uuid, state: impl Into<String>) -> RunOutcome {
    RunOutcome::Reloaded { reload_id: row_id, state: state.into() }
}

async fn state_of(control: &PgPool, account: Uuid, id: Uuid) -> Result<String, sqlx::Error> {
    Ok(load_row(control, account, id).await?.map(|r| r.state).unwrap_or_else(|| "missing".into()))
}

/// Send a `created` row's PaymentIntent request and record the answer. Safe to
/// call any number of times: the request is byte-identical and keyed by the row.
async fn drive(control: &PgPool, stripe: &StripeClient, account: Uuid, id: Uuid) -> Result<RunOutcome, sqlx::Error> {
    let Some(row) = load_row(control, account, id).await? else {
        return Ok(reloaded(id, "missing"));
    };
    if row.state != "created" {
        return Ok(reloaded(id, row.state));
    }
    let params = match payment_intent_params(&row) {
        Ok(p) => p,
        Err(why) => {
            // A row we cannot even describe to Stripe: nothing was sent.
            tracing::error!(%account, reload_id = %id, %why, "auto-reload: row cannot be charged");
            let obs = Observation::Failed { code: "invalid_reload".into(), message: Some(why) };
            apply_observation(control, Some(stripe), account, id, &obs, None).await?;
            return Ok(reloaded(id, state_of(control, account, id).await?));
        }
    };
    match stripe.create_reload_payment_intent(&params, &row.idempotency_key).await {
        Ok(pi) => {
            let obs = Observation::from_status(&pi);
            apply_observation(control, Some(stripe), account, id, &obs, Some(&pi)).await?;
        }
        Err(StripeError::CardDeclined { code, decline_code, message, .. }) => {
            let obs = Observation::Failed { code: decline_code.filter(|c| !c.is_empty()).unwrap_or(code), message: Some(message) };
            apply_observation(control, Some(stripe), account, id, &obs, None).await?;
        }
        // A 4xx rejection means Stripe refused the request: no PaymentIntent.
        // 409 (idempotency in progress / conflict) and 429 are NOT that.
        Err(StripeError::Api { status, code, message, .. }) if matches!(status, 400 | 403 | 404) => {
            let obs = Observation::Failed { code: code.unwrap_or_else(|| "request_rejected".into()), message: Some(message) };
            apply_observation(control, Some(stripe), account, id, &obs, None).await?;
        }
        Err(e) => {
            tracing::warn!(%account, reload_id = %id, error = %e, "auto-reload: PaymentIntent request not confirmed; row stays created and is retried with the same key");
            return Ok(RunOutcome::Deferred(e.to_string()));
        }
    }
    Ok(reloaded(id, state_of(control, account, id).await?))
}

/// Settle a stuck in-flight row from what Stripe says, never by starting a new
/// charge:
///
/// * `submitted`: retrieve the PaymentIntent by id and apply its status.
/// * `created`, recent and still wanted: re-send the original request under the
///   original key. Stripe returns the existing PaymentIntent if the first
///   attempt reached it, or creates the one charge if it did not.
/// * `created`, otherwise (old, or auto-reload since turned off): look for the
///   PaymentIntent at Stripe by the `reload_id` we put in its metadata. Found:
///   adopt it. Not found and old enough that no request can still be in
///   flight: `abandoned`. A key is never re-sent past [`REDRIVE_MAX_AGE_SECS`].
pub async fn reconcile_one(
    control: &PgPool,
    stripe: &StripeClient,
    account: Uuid,
    reload: Uuid,
) -> Result<RunOutcome, sqlx::Error> {
    reconcile_inner(control, stripe, account, reload, true).await
}

async fn reconcile_inner(
    control: &PgPool,
    stripe: &StripeClient,
    account: Uuid,
    reload: Uuid,
    allow_create: bool,
) -> Result<RunOutcome, sqlx::Error> {
    let Some(row) = load_row(control, account, reload).await? else {
        return Ok(reloaded(reload, "missing"));
    };
    match row.state.as_str() {
        "submitted" => {
            let Some(pi_id) = row.payment_intent_id.as_deref() else {
                return Ok(RunOutcome::Deferred("submitted row without a payment intent id".into()));
            };
            match stripe.retrieve_payment_intent(pi_id).await {
                Ok(pi) => {
                    let obs = Observation::from_status(&pi);
                    apply_observation(control, Some(stripe), account, reload, &obs, Some(&pi)).await?;
                    touch(control, account, reload).await?;
                    Ok(reloaded(reload, state_of(control, account, reload).await?))
                }
                Err(e) => {
                    tracing::warn!(%account, %reload, error = %e, "auto-reload reaper: could not read the payment intent");
                    touch(control, account, reload).await?;
                    Ok(RunOutcome::Deferred(e.to_string()))
                }
            }
        }
        "created" => {
            let age = (Utc::now() - row.created_at).num_seconds();
            if allow_create && age <= REDRIVE_MAX_AGE_SECS && still_wanted(control, account, row.amount_micro_usd).await? {
                let out = drive(control, stripe, account, reload).await?;
                touch(control, account, reload).await?;
                return Ok(out);
            }
            let customer = row.customer_id.clone().unwrap_or_default();
            let found = if customer.is_empty() {
                Ok(None)
            } else {
                stripe
                    .list_customer_payment_intents(&customer, row.created_at.timestamp() - 300)
                    .await
                    .map(|v| v.into_iter().find(|p: &PaymentIntentFacts| p.reload_id == Some(reload)))
            };
            match found {
                Ok(Some(pi)) => {
                    let obs = Observation::from_status(&pi);
                    apply_observation(control, Some(stripe), account, reload, &obs, Some(&pi)).await?;
                    Ok(reloaded(reload, state_of(control, account, reload).await?))
                }
                Ok(None) if age >= ABANDON_AFTER_SECS => {
                    apply_observation(control, Some(stripe), account, reload, &Observation::Canceled, None).await?;
                    tracing::warn!(%account, %reload, age_secs = age, "auto-reload reaper: no payment intent at Stripe for a stale reload; abandoned without charging");
                    Ok(reloaded(reload, state_of(control, account, reload).await?))
                }
                Ok(None) => Ok(RunOutcome::Deferred("too young to abandon".into())),
                Err(e) => {
                    tracing::warn!(%account, %reload, error = %e, "auto-reload reaper: could not list payment intents");
                    touch(control, account, reload).await?;
                    Ok(RunOutcome::Deferred(e.to_string()))
                }
            }
        }
        other => Ok(reloaded(reload, other)),
    }
}

/// Is the decision behind a `created` row still one we would make? Re-checks
/// everything [`decide`] would (auto-reload off, Free, frozen, a scheduled
/// cancellation, an account deletion) AND re-runs [`size_reload`] on today's numbers: a row that was
/// sized an hour ago against a limit that has since been lowered, or a balance
/// that has since been topped up above the threshold, is not re-sent. The
/// caller then looks the row up at Stripe and abandons it if nothing was
/// charged (see [`reconcile_inner`]).
async fn still_wanted(control: &PgPool, account: Uuid, row_amount_micro_usd: i64) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account).await.map_err(proto)?;
    let settings: Option<(bool, Option<i64>, bool, Option<i64>)> = sqlx::query_as(
        "SELECT enabled, monthly_limit_micro_usd, auto_reload_enabled, auto_reload_threshold_micro_usd \
           FROM extra_usage_settings WHERE account_id = $1",
    )
    .bind(account)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((true, Some(limit), true, Some(threshold))) = settings else {
        tx.commit().await?;
        return Ok(false);
    };
    let shared = load_extra_shared(&mut tx, account).await?;
    let frozen = load_frozen(&mut tx, account).await?;
    let cancelling = cancel_scheduled(&mut tx, account).await?;
    let deleting = being_deleted(&mut tx, account).await?;
    let (_month, spent, reloaded) = month_totals(&mut tx, account).await?;
    tx.commit().await?;
    if shared.plan_is_free || frozen || cancelling || deleting || shared.balance_micro_usd >= threshold {
        return Ok(false);
    }
    // The row's amount is the configured amount as far as sizing goes: it fits
    // only if today's room still allows all of it.
    Ok(size_reload(row_amount_micro_usd, limit, spent, shared.balance_micro_usd, reloaded) == row_amount_micro_usd)
}

/// Move a row to the back of the reaper's queue after an attempt that changed
/// nothing, so one unresolvable row cannot starve the rest.
async fn touch(control: &PgPool, account: Uuid, id: Uuid) -> Result<(), sqlx::Error> {
    let (turn, mut tx) = open(control, account).await?;
    sqlx::query("UPDATE extra_usage_reloads SET updated_at = now() WHERE id = $1 AND state IN ('created','submitted')")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    drop(turn);
    Ok(())
}

/// One worker pass: reconcile stale in-flight rows, then reload accounts that
/// are due. `new_charges = false` is the kill switch: it still reconciles what
/// Stripe already did (and credits money received) but sends no new request.
pub async fn run_pass(control: &PgPool, stripe: &StripeClient, new_charges: bool) -> Result<PassStats, sqlx::Error> {
    run_pass_filtered(control, stripe, new_charges, None).await
}

/// [`run_pass`] restricted to one account (tests share a database with other
/// accounts they must not touch).
pub async fn run_pass_filtered(
    control: &PgPool,
    stripe: &StripeClient,
    new_charges: bool,
    only: Option<Uuid>,
) -> Result<PassStats, sqlx::Error> {
    let mut stats = PassStats::default();
    let scan_limit: i32 = if only.is_some() { 100_000 } else { 100 };

    let stale: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT account_id, reload_id FROM extra_usage_stale_reloads(make_interval(secs => $1), make_interval(secs => $2), $3)")
            .bind(DRIVE_AFTER_SECS as f64)
            .bind(RECONCILE_SUBMITTED_AFTER_SECS as f64)
            .bind(scan_limit)
            .fetch_all(control)
            .await?;
    for (account, reload) in stale.into_iter().filter(|(a, _)| only.is_none_or(|o| o == *a)) {
        match reconcile_inner(control, stripe, account, reload, new_charges).await {
            Ok(RunOutcome::Reloaded { .. }) => stats.reconciled += 1,
            Ok(_) => {}
            Err(e) => {
                stats.errors += 1;
                tracing::error!(%account, %reload, error = %e, "auto-reload reaper failed for this reload; continuing");
            }
        }
    }

    if new_charges {
        let limit: i32 = if only.is_some() { 100_000 } else { 200 };
        let candidates: Vec<Uuid> = sqlx::query_scalar("SELECT extra_usage_reload_candidates($1)")
            .bind(limit)
            .fetch_all(control)
            .await?;
        for account in candidates.into_iter().filter(|a| only.is_none_or(|o| o == *a)) {
            match run_account(control, stripe, account).await {
                Ok(RunOutcome::Reloaded { .. }) => stats.reloaded += 1,
                Ok(_) => {}
                Err(e) => {
                    stats.errors += 1;
                    tracing::error!(%account, error = %e, "auto-reload failed for this account; continuing");
                }
            }
        }
    }
    Ok(stats)
}
