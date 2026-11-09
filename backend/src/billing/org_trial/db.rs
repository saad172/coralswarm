//! The SQL half of [`super`]: every read and write of `org_trials`, the
//! trial pool, and the conversion's grant change.
//!
//! Every function takes a connection that is already inside an
//! account-scoped transaction (`tenancy::scope::begin_account_scoped`) AND
//! holds the account's financial lock (`renewal::lock_period_rotation`, the
//! 0321 `billing_enter_account` fence): `org_trials`, `grants`,
//! `credit_ledger`, `credit_balances` and `member_allowance_periods` are all
//! RLS-covered and fenced, so a write without both is refused by the schema.
//! Holding the lock is also what serializes these state changes against
//! every usage debit (`workers::billing_derivation` takes the same lock).

use chrono::{DateTime, Utc};
use sqlx::types::Decimal;
use sqlx::PgConnection;
use uuid::Uuid;

use super::{CompletionPlan, ConvertReason, TrialState};

/// One `org_trials` row.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TrialRow {
    pub account_id: Uuid,
    pub state: TrialState,
    pub creator_user_id: Option<Uuid>,
    pub seats: i32,
    pub trial_eligible: bool,
    pub checkout_session_id: Option<String>,
    pub checkout_opened_at: DateTime<Utc>,
    pub stripe_subscription_id: Option<String>,
    pub trial_credits: Decimal,
    pub pool_baseline: Option<Decimal>,
    pub trial_started_at: Option<DateTime<Utc>>,
    pub trial_ends_at: Option<DateTime<Utc>>,
    pub convert_reason: Option<String>,
    pub first_charge_at: Option<DateTime<Utc>>,
}

type RawRow = (
    Uuid,
    String,
    Option<Uuid>,
    i32,
    bool,
    Option<String>,
    DateTime<Utc>,
    Option<String>,
    Decimal,
    Option<Decimal>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<String>,
    Option<DateTime<Utc>>,
);

const SELECT_ROW: &str = "SELECT account_id, state, creator_user_id, seats, trial_eligible, \
        checkout_session_id, checkout_opened_at, stripe_subscription_id, trial_credits, \
        pool_baseline, trial_started_at, trial_ends_at, convert_reason, first_charge_at \
   FROM org_trials WHERE account_id = $1";

fn from_raw(r: RawRow) -> Result<TrialRow, sqlx::Error> {
    let state = TrialState::parse(&r.1)
        .ok_or_else(|| sqlx::Error::Protocol(format!("org_trials: unknown state {:?}", r.1)))?;
    Ok(TrialRow {
        account_id: r.0,
        state,
        creator_user_id: r.2,
        seats: r.3,
        trial_eligible: r.4,
        checkout_session_id: r.5,
        checkout_opened_at: r.6,
        stripe_subscription_id: r.7,
        trial_credits: r.8,
        pool_baseline: r.9,
        trial_started_at: r.10,
        trial_ends_at: r.11,
        convert_reason: r.12,
        first_charge_at: r.13,
    })
}

pub(crate) async fn load(conn: &mut PgConnection, account: Uuid) -> Result<Option<TrialRow>, sqlx::Error> {
    let raw: Option<RawRow> = sqlx::query_as(SELECT_ROW).bind(account).fetch_optional(&mut *conn).await?;
    raw.map(from_raw).transpose()
}

/// [`load`] with `FOR UPDATE`, for a caller about to change the state.
pub(crate) async fn load_for_update(conn: &mut PgConnection, account: Uuid) -> Result<Option<TrialRow>, sqlx::Error> {
    let raw: Option<RawRow> = sqlx::query_as(&format!("{SELECT_ROW} FOR UPDATE"))
        .bind(account)
        .fetch_optional(&mut *conn)
        .await?;
    raw.map(from_raw).transpose()
}

/// Just the state; `None` for an org that never had a self-serve trial
/// (every concierge org, every personal account).
pub(crate) async fn state_of(conn: &mut PgConnection, account: Uuid) -> Result<Option<TrialState>, sqlx::Error> {
    let s: Option<String> = sqlx::query_scalar("SELECT state FROM org_trials WHERE account_id = $1")
        .bind(account)
        .fetch_optional(&mut *conn)
        .await?;
    s.map(|s| TrialState::parse(&s).ok_or_else(|| sqlx::Error::Protocol(format!("org_trials: unknown state {s:?}"))))
        .transpose()
}

/// When a `converting` trial asked for its conversion charge
/// (`convert_requested_at`); `None` for a trial in any other state, or an
/// account with none.
pub(crate) async fn converting_since(conn: &mut PgConnection, account: Uuid) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    let at: Option<Option<DateTime<Utc>>> = sqlx::query_scalar(
        "SELECT convert_requested_at FROM org_trials WHERE account_id = $1 AND state = 'converting'",
    )
    .bind(account)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(at.flatten())
}

/// Decision #6 for the funding snapshot and the extra-usage routes: `true`
/// while the org's trial refuses extra usage and auto-reload (see
/// [`TrialState::refuses_extra_usage`]). `false` for an account with no
/// self-serve trial.
pub(crate) async fn extra_usage_refused(conn: &mut PgConnection, account: Uuid) -> Result<bool, sqlx::Error> {
    let row: Option<(String, bool)> =
        sqlx::query_as("SELECT state, first_charge_at IS NOT NULL FROM org_trials WHERE account_id = $1")
            .bind(account)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((state, charged)) = row else { return Ok(false) };
    let state = TrialState::parse(&state)
        .ok_or_else(|| sqlx::Error::Protocol(format!("org_trials: unknown state {state:?}")))?;
    Ok(state.refuses_extra_usage(charged))
}

/// Record the org's first successful charge, once. Called for a later
/// subscription's first paid invoice after a trial ended unpaid, and by
/// `checkout.session.completed` when that later subscription is created
/// already `active` (#1528 S2: its `invoice.paid` may have arrived first,
/// before the mirror named it).
pub(crate) async fn stamp_first_charge(conn: &mut PgConnection, account: Uuid) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(
        "UPDATE org_trials SET first_charge_at = statement_timestamp(), updated_at = now() \
          WHERE account_id = $1 AND first_charge_at IS NULL",
    )
    .bind(account)
    .execute(&mut *conn)
    .await?
    .rows_affected()
        == 1)
}

/// Has this user (or card) already had a trial? Through the 0382 definer:
/// the app role cannot read `org_trial_claims` itself.
pub(crate) async fn already_claimed(
    conn: &mut PgConnection,
    user: Option<Uuid>,
    card_fingerprint: Option<&str>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT public.org_trial_already_claimed($1, $2)")
        .bind(user)
        .bind(card_fingerprint)
        .fetch_one(&mut *conn)
        .await
}

/// Take the trial for `account` (user and card together, or neither).
pub(crate) async fn claim(
    conn: &mut PgConnection,
    account: Uuid,
    user: Option<Uuid>,
    card_fingerprint: Option<&str>,
) -> Result<bool, sqlx::Error> {
    if user.is_none() && card_fingerprint.is_none() {
        // Nothing identifies who is trialing: refuse the trial rather than
        // hand out an unbounded number of them.
        return Ok(false);
    }
    sqlx::query_scalar("SELECT public.org_trial_claim($1, $2, $3)")
        .bind(account)
        .bind(user)
        .bind(card_fingerprint)
        .fetch_one(&mut *conn)
        .await
}

/// What [`open_checkout`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpenOutcome {
    /// A new row, or an open one whose session was replaced (resume).
    Open,
    /// The org's trial already moved past checkout; no session may replace it.
    NotOpen(TrialState),
}

/// Record the Checkout session a creator was sent to. First call inserts
/// the row; a resume within the 24 h window replaces the session (and may
/// change the seat count). The window's anchor, `checkout_opened_at`, is
/// never moved, so resuming cannot extend it.
pub(crate) async fn open_checkout(
    conn: &mut PgConnection,
    account: Uuid,
    creator: Uuid,
    seats: i32,
    trial_eligible: bool,
    session_id: &str,
) -> Result<OpenOutcome, sqlx::Error> {
    let state: Option<String> = sqlx::query_scalar(
        "INSERT INTO org_trials (account_id, state, creator_user_id, seats, trial_eligible, checkout_session_id, trial_credits) \
         VALUES ($1, 'checkout_open', $2, $3, $4, $5, $6) \
         ON CONFLICT (account_id) DO UPDATE SET \
             seats = EXCLUDED.seats, trial_eligible = EXCLUDED.trial_eligible, \
             checkout_session_id = EXCLUDED.checkout_session_id, updated_at = now() \
           WHERE org_trials.state = 'checkout_open' \
         RETURNING state",
    )
    .bind(account)
    .bind(creator)
    .bind(seats)
    .bind(trial_eligible)
    .bind(session_id)
    .bind(Decimal::from(super::TRIAL_CREDITS))
    .fetch_optional(&mut *conn)
    .await?;
    match state {
        Some(_) => Ok(OpenOutcome::Open),
        None => Ok(OpenOutcome::NotOpen(state_of(conn, account).await?.unwrap_or(TrialState::CheckoutOpen))),
    }
}

/// The facts `checkout.session.completed` brings, read from Stripe before
/// the webhook's transaction opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompletionFacts {
    pub subscription_id: String,
    pub subscription_trialing: bool,
    pub trial_ends_at: Option<DateTime<Utc>>,
    pub card_fingerprint: Option<String>,
}

/// Decide what this completion does, taking the trial claim when the
/// creator was eligible and Stripe put the subscription in trial. Runs in
/// the webhook's transaction after the event claim: if anything later fails
/// the claim rolls back with it, so a redelivery decides again.
///
/// **An unreadable card (`card_unverified`).** The trial Checkout is card
/// only (`payment_method_types[0]=card`, `stripe_client::
/// org_trial_checkout_params`), so the saved method is normally a `card`
/// PaymentMethod with a `card.fingerprint`. Stripe can still offer Link on a
/// card-only session, depending on the account's Link setting; a card saved
/// through Link may come back as a `card` method (fingerprint readable, the
/// normal rule applies) or as a `link` method, which has no
/// `card.fingerprint`. Every method without a readable fingerprint takes the
/// same path: no trial pool, the seats are charged now, an error-level alert
/// is raised, and the creator's USER claim is still recorded (review round
/// 2), so the same person cannot trial a later org. No card claim is
/// written, because there is no fingerprint to claim.
pub(crate) async fn plan_completion(
    conn: &mut PgConnection,
    row: &TrialRow,
    facts: &CompletionFacts,
) -> Result<CompletionPlan, sqlx::Error> {
    // Peek first: a non-claim path must not write a claim.
    let precheck = super::on_checkout_completed(row.state, row.trial_eligible, facts.subscription_trialing, true)
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    if precheck != CompletionPlan::StartTrial {
        return Ok(precheck);
    }
    if facts.card_fingerprint.is_none() {
        // The one-trial-per-card rule cannot be checked. Fail closed: no
        // free credits, the seats are charged now. The creator's user claim
        // is taken anyway (their trial is spent on this org, even though it
        // was refused); `false` (already claimed) changes nothing here.
        if row.creator_user_id.is_some() {
            claim(conn, row.account_id, row.creator_user_id, None).await?;
        }
        tracing::error!(
            alert = true, account = %row.account_id, subscription = %facts.subscription_id,
            "org trial: card fingerprint unreadable at Checkout completion; trial refused, charging now"
        );
        return Ok(CompletionPlan::EndTrialNow);
    }
    let won = claim(conn, row.account_id, row.creator_user_id, facts.card_fingerprint.as_deref()).await?;
    super::on_checkout_completed(row.state, row.trial_eligible, facts.subscription_trialing, won)
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))
}

/// Raise the account pool by the trial credits: a `credit_ledger` row plus
/// the matching `credit_balances` change (the ledger is the pool's journal,
/// the balance its cache; the two must move together). Returns the balance
/// after the credit, which is the trial's `pool_baseline`.
pub(crate) async fn credit_trial_pool(
    conn: &mut PgConnection,
    account: Uuid,
    credits: Decimal,
) -> Result<Decimal, sqlx::Error> {
    sqlx::query("INSERT INTO credit_ledger (account_id, entry_kind, credits) VALUES ($1, $2, $3)")
        .bind(account)
        .bind(super::ENTRY_KIND_TRIAL_CREDIT)
        .bind(credits)
        .execute(&mut *conn)
        .await?;
    sqlx::query_scalar(
        "INSERT INTO credit_balances (account_id, balance, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (account_id) DO UPDATE SET balance = credit_balances.balance + EXCLUDED.balance, \
             updated_at = now() \
         RETURNING balance",
    )
    .bind(account)
    .bind(credits)
    .fetch_one(&mut *conn)
    .await
}

/// Apply a completion plan's state change (the grant was already written
/// by the caller). `now` is the trial's start.
pub(crate) async fn record_completion(
    conn: &mut PgConnection,
    row: &TrialRow,
    plan: CompletionPlan,
    facts: &CompletionFacts,
) -> Result<(), sqlx::Error> {
    if plan == CompletionPlan::AlreadyApplied {
        return Ok(());
    }
    let next = plan.next_state();
    debug_assert!(row.state.can_become(next));
    let baseline = if plan.grants_trial_pool() {
        Some(credit_trial_pool(conn, row.account_id, row.trial_credits).await?)
    } else {
        None
    };
    // `trial_ends_at` is Stripe's own trial_end; a trialing subscription
    // without one cannot happen, but a missing value must not leave the
    // trial without a backstop, so default to the documented 90 days.
    let ends_at = facts
        .trial_ends_at
        .unwrap_or_else(|| Utc::now() + chrono::Duration::days(super::TRIAL_DAYS));
    let reason = match plan {
        CompletionPlan::EndTrialNow if facts.card_fingerprint.is_none() => Some(ConvertReason::CardUnverified),
        _ => plan.convert_reason(),
    };
    let n = sqlx::query(
        "UPDATE org_trials SET state = $2, stripe_subscription_id = $3, card_fingerprint = $4, \
                pool_baseline = $5, \
                first_charge_at = CASE WHEN $2 = 'converted' THEN statement_timestamp() ELSE first_charge_at END, \
                trial_started_at = CASE WHEN $6 THEN statement_timestamp() END, \
                trial_ends_at = CASE WHEN $6 THEN $7 END, \
                convert_reason = $8, \
                convert_requested_at = CASE WHEN $8::text IS NULL THEN NULL ELSE statement_timestamp() END, \
                converted_at = CASE WHEN $2 = 'converted' THEN statement_timestamp() END, \
                ended_at = NULL, end_reason = NULL, updated_at = now() \
          WHERE account_id = $1 AND state = $9",
    )
    .bind(row.account_id)
    .bind(next.as_str())
    .bind(&facts.subscription_id)
    .bind(facts.card_fingerprint.as_deref())
    .bind(baseline)
    .bind(plan.grants_trial_pool())
    .bind(ends_at)
    .bind(reason.map(ConvertReason::as_str))
    .bind(row.state.as_str())
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if n != 1 {
        return Err(sqlx::Error::Protocol(format!(
            "org_trials: completion expected state {} for {}",
            row.state.as_str(),
            row.account_id
        )));
    }
    Ok(())
}

/// The trial's share of the account pool, read from the ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TrialPool {
    /// What the trial grant put in: the sum of the `trial_credit` ledger
    /// rows (0 for an org that never got a pool, e.g. a repeat card).
    pub granted: Decimal,
    /// Credits spent since the trial grant landed.
    pub used: Decimal,
    /// Trial credits still in the pool: `granted - used`, never more than
    /// the balance. Only these are expired when the trial ends.
    pub remaining: Decimal,
    pub balance: Decimal,
}

/// Where the trial pool stands (review round 2, finding E).
///
/// The pool is `credit_balances.balance`, which the trial credit shares with
/// anything else credited to the account (an operator grant, a promo).
/// Every plan debit lowers the balance, so spending is the drop since the
/// trial credit landed (`pool_baseline`, the balance right after it),
/// corrected for credits added or removed since then by anything that is
/// not usage: the NET of every ledger row other than the trial's own two
/// kinds with no job, query, receipt or journal source. An operator grant
/// (positive) does not hide spending, and an operator clawback (negative,
/// #1528 N1) is not counted as spending. A usage row with positive credits
/// is a debit being revised down, which the balance already reflects as
/// spending undone.
///
/// The trial's credits are spent first: they are the ones that expire. So
/// `remaining = granted - used`, and an operator or promo credit, whether it
/// was there before the trial or arrived during it, is never counted as
/// trial credit and survives the trial's end.
pub(crate) async fn trial_pool(conn: &mut PgConnection, row: &TrialRow) -> Result<TrialPool, sqlx::Error> {
    let (granted, since): (Option<Decimal>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT SUM(credits), MIN(created_at) FROM credit_ledger WHERE account_id = $1 AND entry_kind = $2",
    )
    .bind(row.account_id)
    .bind(super::ENTRY_KIND_TRIAL_CREDIT)
    .fetch_one(&mut *conn)
    .await?;
    let balance: Option<Decimal> = sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id = $1")
        .bind(row.account_id)
        .fetch_optional(&mut *conn)
        .await?;
    let balance = balance.unwrap_or(Decimal::ZERO);
    let Some(since) = since else {
        return Ok(TrialPool { granted: Decimal::ZERO, used: Decimal::ZERO, remaining: Decimal::ZERO, balance });
    };
    let granted = granted.unwrap_or(Decimal::ZERO).max(Decimal::ZERO);
    // #1528 N1: net, not only positive. A negative non-usage row (an
    // operator clawback) lowers the balance without anyone spending; left
    // out, it was counted as trial spend and could convert the trial early.
    let added: Option<Decimal> = sqlx::query_scalar(
        "SELECT SUM(credits) FROM credit_ledger \
          WHERE account_id = $1 AND created_at >= $2 \
            AND entry_kind NOT IN ($3, $4) \
            AND source_job_id IS NULL AND source_query_id IS NULL \
            AND source_receipt_id IS NULL AND journal_period IS NULL",
    )
    .bind(row.account_id)
    .bind(since)
    .bind(super::ENTRY_KIND_TRIAL_CREDIT)
    .bind(super::ENTRY_KIND_TRIAL_CREDIT_EXPIRED)
    .fetch_one(&mut *conn)
    .await?;
    let baseline = row.pool_baseline.unwrap_or(granted);
    let used = (baseline + added.unwrap_or(Decimal::ZERO) - balance).max(Decimal::ZERO);
    let remaining = (granted - used).max(Decimal::ZERO).min(balance.max(Decimal::ZERO));
    Ok(TrialPool { granted, used, remaining, balance })
}

/// Credits used from the trial pool so far (see [`trial_pool`]).
pub(crate) async fn trial_used(conn: &mut PgConnection, row: &TrialRow) -> Result<Decimal, sqlx::Error> {
    Ok(trial_pool(conn, row).await?.used)
}

/// `trialing -> converting`. `false` when the row was not `trialing` (a
/// concurrent worker or webhook got there first): the caller then does
/// nothing, which is what makes conversion happen once.
pub(crate) async fn begin_conversion(
    conn: &mut PgConnection,
    account: Uuid,
    reason: ConvertReason,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(
        "UPDATE org_trials SET state = 'converting', convert_reason = $2, \
                convert_requested_at = statement_timestamp(), updated_at = now() \
          WHERE account_id = $1 AND state = 'trialing'",
    )
    .bind(account)
    .bind(reason.as_str())
    .execute(&mut *conn)
    .await?
    .rows_affected()
        == 1)
}

/// End a live trial without converting it (`abandoned` from
/// `checkout_open`, `canceled` from `trialing`/`converting`).
///
/// A cancel also undoes what the trial changed on the team grant, the same
/// way [`finalize_conversion`] does: the 0-credit override is cleared (so a
/// later subscription on the same grant gets the per-tier allowance, not 0)
/// and credits left in the pool are expired. The grant itself stays; the
/// subscription's own cancellation is what freezes the org. An abandoned
/// Checkout never got a grant or a pool, so it has nothing to undo.
pub(crate) async fn end(
    conn: &mut PgConnection,
    account: Uuid,
    to: TrialState,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    let from: &[&str] = match to {
        TrialState::Abandoned => &["checkout_open"],
        TrialState::Canceled => &["trialing", "converting"],
        other => {
            return Err(sqlx::Error::Protocol(format!("org_trials: end() cannot move to {}", other.as_str())))
        }
    };
    let trial_credits: Option<Decimal> = sqlx::query_scalar(
        "UPDATE org_trials SET state = $2, ended_at = statement_timestamp(), end_reason = $3, updated_at = now() \
          WHERE account_id = $1 AND state = ANY($4) \
          RETURNING trial_credits",
    )
    .bind(account)
    .bind(to.as_str())
    .bind(reason)
    .bind(from)
    .fetch_optional(&mut *conn)
    .await?;
    if trial_credits.is_none() {
        return Ok(false);
    }
    if to == TrialState::Canceled {
        clear_trial_override(conn, account, "trial canceled").await?;
        expire_trial_pool(conn, account).await?;
    }
    Ok(true)
}

/// Clear the trial's 0-credit `credits_override` on the account's active
/// team grant. Only a `0` override is the trial's; any other value was set
/// by an operator and is left alone. Returns whether one was cleared.
pub(crate) async fn clear_trial_override(conn: &mut PgConnection, account: Uuid, note: &str) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(
        "UPDATE grants SET credits_override = NULL, \
                note = COALESCE(note || '; ', '') || $3 \
          WHERE id = (SELECT g.id FROM grants g JOIN plans p ON p.id = g.plan_id \
                       WHERE g.account_id = $1 AND p.code = $2 AND g.revoked_at IS NULL \
                       ORDER BY g.granted_at DESC, g.id DESC LIMIT 1) \
            AND credits_override = 0",
    )
    .bind(account)
    .bind(crate::billing::MIXED_TIER_PLAN_CODE)
    .bind(note)
    .execute(&mut *conn)
    .await?
    .rows_affected()
        == 1)
}

/// Expire the trial credits the pool still holds ([`trial_pool`]'s
/// `remaining`), once: a second call, e.g. a late-paid conversion after a
/// cancel already expired the pool, finds the expiry row and writes
/// nothing. Credits that did not come from the trial grant (an operator
/// grant, a promo) are not touched (review round 2, finding E).
async fn expire_trial_pool(conn: &mut PgConnection, account: Uuid) -> Result<Decimal, sqlx::Error> {
    let already: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM credit_ledger WHERE account_id = $1 AND entry_kind = $2)")
            .bind(account)
            .bind(super::ENTRY_KIND_TRIAL_CREDIT_EXPIRED)
            .fetch_one(&mut *conn)
            .await?;
    if already {
        return Ok(Decimal::ZERO);
    }
    let Some(row) = load(conn, account).await? else { return Ok(Decimal::ZERO) };
    let expired = trial_pool(conn, &row).await?.remaining;
    if expired > Decimal::ZERO {
        sqlx::query("INSERT INTO credit_ledger (account_id, entry_kind, credits) VALUES ($1, $2, $3)")
            .bind(account)
            .bind(super::ENTRY_KIND_TRIAL_CREDIT_EXPIRED)
            .bind(-expired)
            .execute(&mut *conn)
            .await?;
        sqlx::query("UPDATE credit_balances SET balance = balance - $2, updated_at = now() WHERE account_id = $1")
            .bind(account)
            .bind(expired)
            .execute(&mut *conn)
            .await?;
    }
    Ok(expired)
}

/// The conversion invoice is unpaid (Stripe dunning). Recorded for support;
/// the state stays `converting`.
pub(crate) async fn mark_payment_failed(conn: &mut PgConnection, account: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE org_trials SET payment_failed_at = statement_timestamp(), updated_at = now() \
          WHERE account_id = $1 AND state = 'converting'",
    )
    .bind(account)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// What [`finalize_conversion`] did.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FinalizeOutcome {
    /// The normal allowance was granted; `members` periods were rolled and
    /// `expired` trial credits were left in the pool and expired.
    ///
    /// `refund_required` (#1528 N4): the payment landed on a subscription
    /// that is already canceled (a late-paid conversion, whose subscription
    /// `lifecycle::stop_charging_at_stripe` canceled at Stripe, or a mirror
    /// that already says `canceled`). The org cannot use what was paid for,
    /// so an operator must refund it; the caller records `refund_required`.
    /// Such a payment is not the org's first charge: `first_charge_at` is
    /// not stamped.
    Converted { members: u64, expired: Decimal, refund_required: bool },
    /// The trial was not `converting` (already finalized, or never began):
    /// nothing written.
    NotConverting(Option<TrialState>),
    /// A late-paid conversion (the trial was canceled from `converting`)
    /// for a subscription the org no longer runs on: it has since
    /// resubscribed on `live`. Nothing written; the payment needs a manual
    /// refund (review round 2, finding B).
    Superseded { live: Option<String> },
    /// The org was deleted in Clerk (#1396 review): a conversion paid after
    /// that is never granted, the mirror is not touched and no tenant
    /// database is queued. Nothing written; the payment needs a manual
    /// refund.
    OrgDeleted,
}

const ACTIVE_TEAM_GRANT_SQL: &str = "\
    SELECT g.id FROM grants g JOIN plans p ON p.id = g.plan_id \
     WHERE g.account_id = $1 AND p.code = $2 AND g.revoked_at IS NULL \
     ORDER BY g.granted_at DESC, g.id DESC LIMIT 1";

/// The seats were paid: grant the normal first-month allowance for the
/// billing period Stripe just opened, `[period_start, period_end)`.
///
/// Called from the conversion worker (after Stripe reports the subscription
/// `active`) and from `invoice.paid` (the same charge, observed by webhook,
/// including Stripe's own day-90 trial end). Whichever runs first converts;
/// the other finds the row no longer `converting` and writes nothing.
///
/// 1. The team grant's `credits_override` (0 during the trial) is cleared,
///    so the per-tier allowance applies from now on.
/// 2. Credits still in the pool are expired with a ledger row (the trial
///    ended; they are not carried into the paid plan's pool).
/// 3. Zero-credit, unspent rows a racing roll may have written at exactly
///    `period_start` are removed, then the trial periods are closed at
///    `period_start` and the new period is rolled at the paid allowance.
/// 4. `stripe_subscriptions` mirrors the paid period; the trial is `converted`.
pub(crate) async fn finalize_conversion(
    conn: &mut PgConnection,
    account: Uuid,
    subscription_id: &str,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
) -> Result<FinalizeOutcome, sqlx::Error> {
    let Some(row) = load_for_update(conn, account).await? else {
        return Ok(FinalizeOutcome::NotConverting(None));
    };
    // A trial canceled while `converting` (it carries a convert_reason)
    // whose conversion invoice was paid anyway: the customer paid, so the
    // allowance is granted. A trial canceled from `trialing` never asked for
    // a charge and is not converted by one.
    let late_paid = row.state == TrialState::Canceled && row.convert_reason.is_some();
    if row.state != TrialState::Converting && !late_paid {
        return Ok(FinalizeOutcome::NotConverting(Some(row.state)));
    }
    // #1396 review: once Clerk deleted the org, no payment revives it. The
    // `organization.deleted` arm cancels the trial and the subscription, and
    // a conversion invoice paid after that (late, or from its hosted page)
    // must not grant the allowance, re-activate the mirror or queue a tenant
    // database. Checked under the caller's financial lock.
    if crate::billing::cancel::org_deleted_in_clerk(&mut *conn, account).await? {
        return Ok(FinalizeOutcome::OrgDeleted);
    }
    if late_paid {
        // Finding B: only while the mirror still names this subscription.
        // An org that resubscribed since runs on another subscription, and
        // this payment must not overwrite its mirror, periods or allowance.
        let live: Option<Option<String>> =
            sqlx::query_scalar("SELECT stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1 FOR UPDATE")
                .bind(account)
                .fetch_optional(&mut *conn)
                .await?;
        let live = live.flatten();
        if live.as_deref() != Some(subscription_id) {
            return Ok(FinalizeOutcome::Superseded { live });
        }
        // The trial was canceled because nothing was meant to charge, yet
        // the customer paid. The operator alert (with the refund ask) is
        // raised below, once, as `refund_required`.
        tracing::warn!(
            %account, subscription_id,
            "org trial: conversion invoice paid after the trial was canceled; granting the paid allowance"
        );
    }
    let grant: Option<Uuid> = sqlx::query_scalar(ACTIVE_TEAM_GRANT_SQL)
        .bind(account)
        .bind(crate::billing::MIXED_TIER_PLAN_CODE)
        .fetch_optional(&mut *conn)
        .await?;
    if grant.is_none() {
        return Err(sqlx::Error::Protocol(format!(
            "org_trials: {account} is converting but holds no active team grant"
        )));
    }
    clear_trial_override(conn, account, "trial converted").await?;
    let expired = expire_trial_pool(conn, account).await?;

    sqlx::query(
        "DELETE FROM member_allowance_periods \
          WHERE account_id = $1 AND period_start = $2 AND granted_credits = 0 AND consumed_credits = 0",
    )
    .bind(account)
    .bind(period_start)
    .execute(&mut *conn)
    .await?;
    crate::billing::renewal::close_open_period(&mut *conn, account, period_start).await?;
    let rolled = crate::billing::renewal::roll_period(&mut *conn, account, period_start, period_end).await?;

    // A subscription already deleted at Stripe stays `canceled` in the
    // mirror (a late-paid invoice does not revive it); its periods are
    // still recorded above. Finding B: only the mirror of THIS subscription
    // is written; a mirror on another one (a resubscribe) is never
    // overwritten by an older subscription's payment.
    sqlx::query(
        "UPDATE stripe_subscriptions SET status = 'active', stripe_subscription_id = $2, \
                current_period_start = $3, current_period_end = $4, updated_at = now() \
          WHERE account_id = $1 AND status <> 'canceled' AND stripe_subscription_id = $2",
    )
    .bind(account)
    .bind(subscription_id)
    .bind(period_start)
    .bind(period_end)
    .execute(&mut *conn)
    .await?;
    // The mirror's status after the write above (which never touches a
    // `canceled` mirror). A late-paid conversion, or one whose mirror
    // already says `canceled`, was paid on a subscription that is already
    // canceled: it needs a refund.
    let mirror_status: Option<String> =
        sqlx::query_scalar("SELECT status FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account)
            .fetch_optional(&mut *conn)
            .await?;
    let refund_required = late_paid || mirror_status.as_deref() == Some("canceled");
    // #1528 review: a payment owed back (`refund_required`) is not the
    // org's first charge, so `first_charge_at` is left as it was.
    sqlx::query(
        "UPDATE org_trials SET state = 'converted', converted_at = statement_timestamp(), \
                first_charge_at = CASE WHEN $3 THEN first_charge_at \
                                       ELSE COALESCE(first_charge_at, statement_timestamp()) END, \
                ended_at = NULL, end_reason = NULL, \
                stripe_subscription_id = $2, updated_at = now() \
          WHERE account_id = $1 AND (state = 'converting' \
                OR (state = 'canceled' AND convert_reason IS NOT NULL))",
    )
    .bind(account)
    .bind(subscription_id)
    .bind(refund_required)
    .execute(&mut *conn)
    .await?;
    // A repeat-card org was not queued at Checkout (it had not paid yet);
    // every other org already was, and a second enqueue is a no-op.
    //
    // Not for a late-paid conversion: that trial was canceled, and its
    // subscription with it (`lifecycle::stop_charging_at_stripe` cancels at
    // Stripe before the row says `canceled`), so there is no live
    // subscription to serve. Nor when the mirror already says `canceled`.
    if refund_required {
        tracing::info!(%account, late_paid, "org trial: subscription canceled; no tenant database queued at conversion");
    } else {
        enqueue_provisioning(conn, account).await?;
    }
    if refund_required {
        tracing::error!(
            alert = true, %account, subscription_id, late_paid,
            "org trial: conversion paid on a subscription that is already canceled; refund it manually"
        );
    }
    Ok(FinalizeOutcome::Converted { members: rolled.rows_granted, expired, refund_required })
}

/// What [`enqueue_provisioning`] did, for the caller's recorded outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Provisioning {
    Enqueued,
    /// Not enqueued, and retrying the event would not change that: the
    /// signup flag is off, a job or routing row already exists (a
    /// redelivery), or the org's control rows are not in the shape the job
    /// needs. Logged.
    Skipped(String),
}

impl Provisioning {
    pub fn summary(&self) -> String {
        match self {
            Provisioning::Enqueued => "provisioning=enqueued".into(),
            Provisioning::Skipped(why) => format!("provisioning=skipped({why})"),
        }
    }
}

/// Queue the org's tenant database on the caller's transaction
/// (`tenancy::provisioning::enqueue_in`), so the job row commits or rolls
/// back with the Checkout or conversion that paid for it.
///
/// Only a database error is returned as `Err` (the caller rolls back and the
/// event is retried). Every refusal is `Ok(Skipped)`: answering a Stripe
/// webhook with a 500 for "the flag is off" or "already enqueued" would make
/// Stripe redeliver for days with the same answer.
pub(crate) async fn enqueue_provisioning(
    conn: &mut PgConnection,
    account: Uuid,
) -> Result<Provisioning, sqlx::Error> {
    use crate::tenancy::provisioning::{enqueue_in, EnqueueError, EnqueueRequest};
    let clerk_org_id: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account)
        .fetch_optional(&mut *conn)
        .await?
        .flatten();
    let Some(clerk_org_id) = clerk_org_id else {
        tracing::error!(%account, "org trial: self-serve account has no clerk_org_id; no tenant database queued");
        return Ok(Provisioning::Skipped("no clerk_org_id".into()));
    };
    match enqueue_in(conn, EnqueueRequest::selfserve(clerk_org_id.clone(), account)).await {
        Ok(outcome) => {
            tracing::info!(%account, org = %clerk_org_id, ?outcome, "org trial: tenant provisioning queued");
            Ok(Provisioning::Enqueued)
        }
        Err(EnqueueError::Db(e)) => Err(e),
        Err(EnqueueError::Disabled) => {
            tracing::info!(%account, org = %clerk_org_id, "org trial: self-serve signup flag is off; no tenant database queued");
            Ok(Provisioning::Skipped("signup flag off".into()))
        }
        Err(EnqueueError::Conflict { org, detail }) => {
            // A job or routing row for this org raced the pre-checks. Not a
            // reason to retry the payment event, but someone should look.
            tracing::error!(
                alert = true, %account, %org, %detail,
                "org trial: tenant provisioning enqueue conflicted; the Checkout is recorded, the job needs a look"
            );
            Ok(Provisioning::Skipped(format!("conflict: {detail}")))
        }
        Err(other) => {
            tracing::warn!(%account, org = %clerk_org_id, error = %other, "org trial: tenant provisioning not queued");
            Ok(Provisioning::Skipped(other.to_string()))
        }
    }
}
