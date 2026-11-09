//! Driving a trial forward: the worker's per-account pass and the
//! conversion that charges the seats.
//!
//! Every Stripe call runs OUTSIDE a database transaction (the codebase rule:
//! no pooled connection held across an external round trip). Each database
//! step opens its own account-scoped transaction, takes the financial lock,
//! re-reads the row, and changes state only from the state it expects, so a
//! step that lost a race to a webhook or another worker writes nothing.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use super::db::{self, FinalizeOutcome, TrialRow};
use super::{conversion_step, on_usage, ChargeOutcome, ConversionStep, SubscriptionFacts, TrialState, UsageInput};
use crate::billing::notices::{self, NoticeKind};
use crate::billing::stripe_client::{StripeClient, SubscriptionView};

/// The dedupe key of every trial notice: an account has one trial, ever.
pub(crate) const TRIAL_NOTICE_KEY: &str = "trial";

/// What one pass over one account did, for the worker's log.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PassReport {
    pub actions: Vec<&'static str>,
}

impl PassReport {
    fn did(&mut self, what: &'static str) {
        self.actions.push(what);
    }
    pub fn worked(&self) -> bool {
        !self.actions.is_empty()
    }
}

async fn scoped_locked(
    control: &PgPool,
    account: Uuid,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, String> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| format!("org trial scope: {e}"))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account)
        .await
        .map_err(|e| format!("org trial lock: {e}"))?;
    Ok(tx)
}

fn sql(what: &'static str) -> impl Fn(sqlx::Error) -> String {
    move |e| format!("org trial {what}: {e}")
}

/// One worker pass for one account: abandon a stale Checkout, notice and
/// convert a trialing org, or push a converting one forward.
pub(crate) async fn process_account(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account: Uuid,
    now: DateTime<Utc>,
) -> Result<PassReport, String> {
    let mut report = PassReport::default();
    let mut tx = scoped_locked(control, account).await?;
    let Some(row) = db::load_for_update(&mut tx, account).await.map_err(sql("load"))? else {
        tx.rollback().await.ok();
        return Ok(report);
    };
    // Self-serve signup A2 (security review S7): a paid org whose
    // provisioning is `held` has no workspace. It is never converted (no
    // charge for the seats) or driven through a conversion (whose cancel
    // path would end its subscription) until an operator, or the owner's
    // retry, gets it built. The cleanup sweep pauses collection at Stripe for
    // the same org, so Stripe's own trial end cannot charge it either.
    let held = provisioning_held(&mut tx, account).await.map_err(sql("held read"))?;
    if held && matches!(row.state, TrialState::Trialing | TrialState::Converting) {
        tx.rollback().await.ok();
        tracing::error!(alert = true, %account, state = row.state.as_str(),
            "org trial: provisioning is held for an operator, or collection is still paused at Stripe; conversion paused \
             (no charge) until the workspace is ready and the cleanup sweep has resumed collection");
        return Ok(report);
    }
    let mut drive = row.state == TrialState::Converting;
    let mut expire_session: Option<String> = None;
    match row.state {
        TrialState::CheckoutOpen if super::checkout_abandoned(row.checkout_opened_at, now) => {
            if db::end(&mut tx, account, TrialState::Abandoned, "checkout not completed within 24 hours")
                .await
                .map_err(sql("abandon"))?
            {
                report.did("abandoned");
                expire_session = row.checkout_session_id.clone();
            }
        }
        TrialState::Trialing => {
            if subscription_canceled(&mut tx, account).await.map_err(sql("subscription read"))? {
                db::end(&mut tx, account, TrialState::Canceled, "subscription canceled during the trial")
                    .await
                    .map_err(sql("cancel"))?;
                report.did("canceled");
            } else {
                drive = trialing_pass(&mut tx, &row, now, &mut report).await?;
            }
        }
        _ => {}
    }
    tx.commit().await.map_err(sql("commit"))?;
    if let Some(session) = expire_session {
        // Best effort: an abandoned org's Checkout must not be completable
        // later from an old tab. A failure (already expired or completed is
        // a 400) is logged; a completion that slips through is still
        // honoured (`abandoned -> trialing`).
        match stripe {
            Some(stripe) => {
                match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.expire_checkout_session(&session)).await {
                    Ok(()) => report.did("checkout_expired"),
                    Err(e) => tracing::warn!(%account, session, error = %e, "org trial: could not expire the abandoned Checkout session"),
                }
            }
            None => tracing::warn!(%account, session, "org trial: abandoned Checkout session not expired (no Stripe client)"),
        }
    }
    if drive {
        drive_conversion(control, stripe, account, &mut report).await?;
    }
    Ok(report)
}

/// End a Clerk-deleted org's trial through [`db::end`] (#1396). Returns the
/// state the row was moved to, or `None` when there was nothing to end (no
/// trial, or one already `converted`/`canceled`/`abandoned`), which is what
/// a redelivery of the same event sees.
///
/// * `trialing`/`converting` -> `canceled`. `db::end` clears the 0-credit
///   override and expires the pool. A conversion the worker is part-way
///   through is not charged for: `drive_conversion` re-reads the row under
///   the financial lock before it writes, and writes nothing for a row that
///   is no longer `converting`; and `db::finalize_conversion` refuses any
///   org deleted in Clerk. What this cannot undo is a Stripe call the worker
///   already made (ending the trial at Stripe); the subscription cancel
///   that follows voids an `open` conversion invoice first.
/// * `checkout_open` -> `abandoned`, the only end the transition table
///   allows from there. Its Checkout session is expired FIRST, outside any
///   transaction, and the row is ended only once Stripe confirms the session
///   can no longer start a subscription: the expire succeeded, or a re-read
///   says `expired` or `complete`. A session that is still `open` (or cannot
///   be read) is an `Err`, so the webhook answers 5xx, the row stays
///   `checkout_open`, and Clerk's retry tries again. A session that
///   `complete`d is handled by `checkout.session.completed`: for an org with
///   an `org_revocations` row it ends the new subscription instead of
///   activating it (`routes::stripe_webhook::refuse_for_deleting_org`, #1543).
///
/// Runs BEFORE the subscription is cancelled at Stripe, so no worker pass
/// can begin charging the seats of an org that is being deleted.
pub(crate) async fn end_for_deleted_org(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account: Uuid,
) -> Result<Option<TrialState>, String> {
    const REASON: &str = "organization deleted in Clerk";
    let peek = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
            .await
            .map_err(|e| format!("org trial scope: {e}"))?;
        let row = db::load(&mut tx, account).await.map_err(sql("load"))?;
        tx.commit().await.map_err(sql("commit"))?;
        row
    };
    if let Some(TrialRow { state: TrialState::CheckoutOpen, checkout_session_id: Some(session), .. }) = &peek {
        close_checkout_for_deleted_org(stripe, account, session).await?;
    }

    let mut tx = scoped_locked(control, account).await?;
    let Some(row) = db::load_for_update(&mut tx, account).await.map_err(sql("load"))? else {
        tx.rollback().await.ok();
        return Ok(None);
    };
    let to = match row.state {
        TrialState::Trialing | TrialState::Converting => TrialState::Canceled,
        // Only reached with the session confirmed closed above, or with no
        // session at all (the row changed to `checkout_open` since the peek
        // is not a transition the table allows).
        TrialState::CheckoutOpen => TrialState::Abandoned,
        TrialState::Converted | TrialState::Canceled | TrialState::Abandoned => {
            tx.rollback().await.ok();
            return Ok(None);
        }
    };
    let ended = db::end(&mut tx, account, to, REASON).await.map_err(sql("end on org deletion"))?;
    tx.commit().await.map_err(sql("commit"))?;
    if !ended {
        return Ok(None);
    }
    tracing::info!(%account, from = row.state.as_str(), to = to.as_str(), "org trial: ended because the organization was deleted");
    Ok(Some(to))
}

/// Make sure a deleted org's trial Checkout can no longer start a
/// subscription (#1396 review). `Ok` once the expire succeeded or Stripe
/// reports the session `expired` or `complete`; an `Err` otherwise.
async fn close_checkout_for_deleted_org(stripe: Option<&StripeClient>, account: Uuid, session: &str) -> Result<(), String> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let Some(stripe) = stripe else {
        // No Stripe client, no Checkout could have been created by this
        // process either; nothing can be expired or read.
        tracing::warn!(%account, session, "org trial: deleted org's Checkout session not expired (no Stripe client)");
        return Ok(());
    };
    let expire_error = match with_stripe_deadline(stripe.expire_checkout_session(session)).await {
        Ok(()) => {
            tracing::info!(%account, session, "org trial: deleted org's Checkout session expired");
            return Ok(());
        }
        Err(e) => e,
    };
    let current = with_stripe_deadline(stripe.retrieve_checkout_session(session))
        .await
        .map_err(|e| format!("org trial {account}: expire Checkout {session}: {expire_error}; re-read: {e}"))?;
    match current.get("status").and_then(serde_json::Value::as_str) {
        Some("expired") => Ok(()),
        Some("complete") => {
            tracing::warn!(%account, session, "org trial: deleted org's Checkout completed; checkout.session.completed ends its subscription");
            Ok(())
        }
        other => Err(format!(
            "org trial {account}: Checkout {session} is still {} after a failed expire ({expire_error}); retrying",
            other.unwrap_or("unknown")
        )),
    }
}

/// How a failed worker pass was reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureReport {
    /// A warning: the trial is not converting, or began converting within
    /// the last [`super::CONVERTING_ALERT_HOURS`].
    Warned,
    /// An error with `alert = true`: the trial has been stuck in
    /// `converting` for more than [`super::CONVERTING_ALERT_HOURS`]. The org
    /// has no allowance and no extra usage while it waits, and the charge
    /// may be in an unknown state at Stripe, so a human must look.
    Alerted,
}

/// Log a worker pass that failed for `account` (#1528 S3). A trial stuck in
/// `converting` for more than 24 hours escalates to an operator alert; any
/// other failure is a warning, and the next pass retries. A failure to read
/// the row is itself only a warning (the pass error is still logged).
pub(crate) async fn report_pass_failure(
    control: &PgPool,
    account: Uuid,
    now: DateTime<Utc>,
    error: &str,
) -> FailureReport {
    let since = async {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
            .await
            .map_err(|e| format!("org trial scope: {e}"))?;
        let at = db::converting_since(&mut tx, account).await.map_err(sql("converting read"))?;
        tx.commit().await.map_err(sql("commit"))?;
        Ok::<_, String>(at)
    }
    .await;
    match since {
        Ok(Some(requested_at)) if super::converting_overdue(requested_at, now) => {
            tracing::error!(
                alert = true, %account, %requested_at, error,
                "workers::org_trials: trial stuck in converting for more than 24 hours; the org has no paid allowance \
                 until this is resolved"
            );
            FailureReport::Alerted
        }
        Ok(_) => {
            tracing::warn!(%account, error, "workers::org_trials: pass failed for this account; continuing with the rest");
            FailureReport::Warned
        }
        Err(read) => {
            tracing::warn!(
                %account, error, read_error = %read,
                "workers::org_trials: pass failed for this account (and its conversion age could not be read); continuing"
            );
            FailureReport::Warned
        }
    }
}

/// Whether this account's trial must not be converted or driven right now
/// (control 0384, A2 S7, #1540 SF1/SF5):
/// * its provisioning job is `held` and the org is NOT being served (no
///   `active` routing row; a `held_routed` org whose routing row is active
///   has a workspace and is billed normally, SF5); or
/// * the cleanup sweep paused collection at Stripe and has not yet resumed
///   it (SF1). Between the job reaching `ready` and the sweep's resume, an
///   invoice Stripe voided during the pause is still the subscription's
///   latest, and converting then would read it as "not paid" and cancel a
///   paying customer. The sweep's resume re-anchors such a subscription so
///   it is charged afresh, and only then clears the pause.
pub(crate) async fn provisioning_held(conn: &mut sqlx::PgConnection, account: Uuid) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM tenant_provisioning_jobs j \
                          LEFT JOIN tenant_databases td ON td.clerk_org_id = j.clerk_org_id \
                         WHERE j.account_id = $1 AND j.status = 'failed' AND j.clerk_org_cleanup = 'held' \
                           AND td.status IS DISTINCT FROM 'active') \
             OR EXISTS (SELECT 1 FROM org_selfserve_creations c \
                         WHERE c.account_id = $1 AND c.collection_paused_at IS NOT NULL)",
    )
    .bind(account)
    .fetch_one(&mut *conn)
    .await
}

async fn subscription_canceled(conn: &mut sqlx::PgConnection, account: Uuid) -> Result<bool, sqlx::Error> {
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM stripe_subscriptions WHERE account_id = $1")
        .bind(account)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(status.as_deref() == Some("canceled"))
}

/// The usage half: claim the due notices and, at 100% (or day 90), move the
/// row to `converting` under the financial lock. Returns whether the caller
/// should now charge the seats.
async fn trialing_pass(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    row: &TrialRow,
    now: DateTime<Utc>,
    report: &mut PassReport,
) -> Result<bool, String> {
    let account = row.account_id;
    let used = db::trial_used(tx, row).await.map_err(sql("usage"))?;
    let Some(trial_ends_at) = row.trial_ends_at else {
        return Err(format!("org trial {account}: trialing without trial_ends_at (CHECK should prevent this)"));
    };
    let decision = on_usage(&UsageInput { credits: row.trial_credits, used, now, trial_ends_at });
    for (due, kind) in [
        (decision.notify_80, NoticeKind::TrialCredits80),
        (decision.notify_used, NoticeKind::TrialCreditsUsed),
        (decision.notify_ending_soon, NoticeKind::TrialEndingSoon),
    ] {
        if due && notices::claim(tx, account, kind, TRIAL_NOTICE_KEY, None).await.map_err(sql("notice"))? {
            report.did(kind.as_str());
        }
    }
    let Some(reason) = decision.convert else {
        return Ok(false);
    };
    let began = db::begin_conversion(tx, account, reason).await.map_err(sql("begin conversion"))?;
    if began {
        tracing::info!(%account, reason = reason.as_str(), %used, "org trial: converting to paid");
        report.did("converting");
    }
    Ok(began)
}

fn facts_of(sub: &SubscriptionView) -> SubscriptionFacts {
    SubscriptionFacts {
        status: sub.status.clone(),
        cancel_scheduled: sub.cancel_at_period_end || sub.cancel_at.is_some(),
    }
}

/// The idempotency key that ends one subscription's trial: the same for
/// every attempt, so two workers (or a retry) produce one Stripe update.
pub(crate) fn end_trial_key(account: Uuid, subscription_id: &str) -> String {
    format!("org-trial-end:{account}:{subscription_id}")
}

/// The idempotency key that cancels one trial subscription at Stripe.
pub(crate) fn cancel_trial_key(account: Uuid, subscription_id: &str) -> String {
    format!("org-trial-cancel:{account}:{subscription_id}")
}

/// The idempotency key that keeps the owner's cancel when a conversion
/// turned out to be paid: the subscription ends at the paid period's end.
pub(crate) fn cancel_at_period_end_key(account: Uuid, subscription_id: &str) -> String {
    format!("org-trial-cancel-at-period-end:{account}:{subscription_id}")
}

/// The idempotency key that voids one conversion invoice.
///
/// #1528 N2: Stripe stores the first response under an idempotency key for
/// 24 hours, a 5xx included, and replays it for every later request with
/// that key. A fixed key would turn one transient Stripe failure into a day
/// of replayed failures, with the trial stuck in `converting`. The key is
/// therefore fixed only within one UTC hour: retries inside the hour (and
/// two racing workers) still share it, and a cached failure stops being
/// replayed at the next hour. A second void of an already-void invoice is
/// harmless: Stripe answers 400 and the caller re-reads the invoice anyway.
pub(crate) fn void_invoice_key(account: Uuid, invoice_id: &str, now: DateTime<Utc>) -> String {
    format!("org-trial-void:{account}:{invoice_id}:{}", now.timestamp().div_euclid(3600))
}

/// What [`stop_charging_at_stripe`] established.
#[derive(Debug)]
enum Stopped {
    /// Nothing can charge for this conversion any more: the subscription is
    /// canceled at Stripe, and the conversion invoice (if there was one) is
    /// confirmed `void` or `uncollectible`.
    Canceled,
    /// The conversion invoice is `paid` (or, with no invoice to read, the
    /// subscription is `active`). The caller finalizes with this re-read
    /// subscription; the owner's cancel takes effect at the end of the paid
    /// period.
    Paid(SubscriptionView),
}

/// The subscription's conversion invoice: its latest invoice once the
/// trial has ended at Stripe. A `trialing` subscription has none (its
/// latest invoice is the $0 opening one, which pays for nothing).
fn conversion_invoice_of(sub: &SubscriptionView) -> Option<String> {
    if sub.status == "trialing" {
        None
    } else {
        sub.latest_invoice.clone()
    }
}

/// A `converting` trial is being canceled (a cancel was scheduled, or the
/// charge is in dunning with a cancel pending). Review round 2, finding A:
/// a customer who has just paid must never be canceled on the spot.
///
/// 1. An outstanding conversion invoice (`past_due`, `incomplete`,
///    `unpaid`) is voided, keyed ([`void_invoice_key`]). A failure is
///    logged, not fatal.
/// 2. The subscription AND the invoice are re-read, whatever the void
///    answered: the customer may have paid between the first read and the
///    void (a paid invoice cannot be voided).
/// 3. #1528 S1: when the conversion invoice was read, ITS status alone
///    decides ([`super::charge_outcome`]). Voiding a `past_due`
///    subscription's latest invoice can move the subscription back to
///    `active`, so `active` next to a `void` invoice is not a payment.
///    The subscription's status decides only when there is no invoice.
/// 4. Paid: no cancel. The caller finalizes the conversion, and the owner's
///    cancel is kept as `cancel_at_period_end` (set, keyed, if Stripe no
///    longer shows one).
/// 5. Not paid (`void`/`uncollectible`, or a subscription still `trialing`
///    with nothing charged): the subscription is canceled now (no
///    proration, no final invoice, keyed), unless Stripe already ended it.
/// 6. Pending (the void failed and the invoice is still `open`): an `Err`.
///    The row stays `converting` and the next pass decides again.
///
/// A subscription Stripe already reports `canceled`/`incomplete_expired`
/// needs none of this. Should a conversion invoice be paid after the cancel
/// anyway (an `uncollectible` invoice can still be paid), `invoice.paid`
/// grants the allowance (`db::finalize_conversion`'s late-paid edge) and
/// raises an operator alert asking for a refund.
async fn stop_charging_at_stripe(stripe: &StripeClient, account: Uuid, sub: &SubscriptionView) -> Result<Stopped, String> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    if matches!(sub.status.as_str(), "canceled" | "incomplete_expired") {
        return Ok(Stopped::Canceled);
    }
    let conversion_invoice = conversion_invoice_of(sub);
    if matches!(sub.status.as_str(), "past_due" | "incomplete" | "unpaid") {
        if let Some(invoice) = conversion_invoice.as_deref() {
            let key = void_invoice_key(account, invoice, Utc::now());
            match with_stripe_deadline(stripe.void_invoice(invoice, &key)).await {
                Ok(()) => tracing::info!(%account, invoice, "org trial: open conversion invoice voided"),
                Err(e) => tracing::warn!(%account, invoice, error = %e, "org trial: could not void the conversion invoice; re-reading before any cancel"),
            }
        }
    }

    let now = with_stripe_deadline(stripe.retrieve_subscription(&sub.id))
        .await
        .map_err(|e| format!("org trial {account}: re-read subscription before cancel: {e}"))?;
    // The conversion invoice of the RE-READ subscription (#1528 review):
    // Stripe may have ended the trial itself (day 90) between the two reads.
    // The stale `trialing` view names no conversion invoice, and deciding on
    // the re-read `active` status alone would finalize a charge that is
    // still a `draft`.
    let conversion_invoice = conversion_invoice_of(&now);
    let invoice = match conversion_invoice.as_deref() {
        Some(id) => Some(
            with_stripe_deadline(stripe.retrieve_invoice(id))
                .await
                .map_err(|e| format!("org trial {account}: re-read conversion invoice {id} before cancel: {e}"))?,
        ),
        None => None,
    };
    let ended_at_stripe = matches!(now.status.as_str(), "canceled" | "incomplete_expired");

    match super::charge_outcome(invoice.as_ref().map(|i| i.status.as_str()), &now.status) {
        ChargeOutcome::Paid => {
            tracing::warn!(
                %account, subscription = %now.id, status = %now.status,
                invoice_status = invoice.as_ref().map(|i| i.status.as_str()).unwrap_or(""),
                "org trial: the conversion was paid before the cancel could stop it; finalizing, the cancel takes effect at period end"
            );
            if !ended_at_stripe && !now.cancel_at_period_end && now.cancel_at.is_none() {
                let key = cancel_at_period_end_key(account, &now.id);
                if let Err(e) = with_stripe_deadline(stripe.update_subscription(
                    &now.id,
                    true,
                    None,
                    crate::billing::stripe_client::Idem::Key(&key),
                ))
                .await
                {
                    // The customer paid; the allowance is still granted. The
                    // owner's cancel needs a human if Stripe would not keep it.
                    tracing::error!(
                        alert = true, %account, subscription = %now.id, error = %e,
                        "org trial: could not schedule the owner's cancel at period end on a paid conversion"
                    );
                }
            }
            return Ok(Stopped::Paid(now));
        }
        _ if ended_at_stripe => return Ok(Stopped::Canceled),
        ChargeOutcome::NotPaid => {}
        ChargeOutcome::Pending => {
            return Err(match &invoice {
                Some(i) => format!(
                    "org trial {account}: conversion invoice {} is {} (subscription {}); not canceling until it is void or uncollectible",
                    i.id, i.status, now.status
                ),
                None => format!(
                    "org trial {account}: subscription is {} with no conversion invoice read; not canceling this pass",
                    now.status
                ),
            });
        }
    }
    if now.status == "active" {
        // S1's case, logged so an operator can match it to Stripe's view.
        tracing::warn!(
            %account, subscription = %now.id,
            invoice_status = invoice.as_ref().map(|i| i.status.as_str()).unwrap_or(""),
            "org trial: subscription reports active but its conversion invoice was not paid; canceling, no allowance"
        );
    }

    let key = cancel_trial_key(account, &sub.id);
    match with_stripe_deadline(stripe.cancel_subscription_now_keyed(&sub.id, &key)).await {
        Ok(_) => {
            tracing::info!(%account, subscription = %sub.id, "org trial: subscription canceled at Stripe before any conversion charge");
            Ok(Stopped::Canceled)
        }
        Err(e) => {
            // Already canceled (by the customer, or an earlier pass whose
            // key expired) is the outcome we want.
            let after = with_stripe_deadline(stripe.retrieve_subscription(&sub.id))
                .await
                .map_err(|e2| format!("org trial {account}: cancel at Stripe: {e}; re-read: {e2}"))?;
            if matches!(after.status.as_str(), "canceled" | "incomplete_expired") {
                Ok(Stopped::Canceled)
            } else {
                Err(format!("org trial {account}: cancel at Stripe: {e}"))
            }
        }
    }
}

/// Charge the seats of a `converting` trial and, once Stripe reports the
/// subscription paid (`active`), grant the normal allowance.
///
/// Safe to call at any time and from anywhere (the worker, or right after
/// the webhook that started a repeat-card conversion): it reads the row,
/// asks Stripe where the subscription is, and takes at most one step.
pub(crate) async fn drive_conversion(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account: Uuid,
    report: &mut PassReport,
) -> Result<(), String> {
    let row = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
            .await
            .map_err(|e| format!("org trial scope: {e}"))?;
        let row = db::load(&mut tx, account).await.map_err(sql("load"))?;
        // S7: never drive a held org's conversion (see `process_account`).
        let held = provisioning_held(&mut tx, account).await.map_err(sql("held read"))?;
        tx.commit().await.map_err(sql("commit"))?;
        if held {
            return Ok(());
        }
        row
    };
    let Some(row) = row.filter(|r| r.state == TrialState::Converting) else {
        return Ok(());
    };
    let Some(subscription_id) = row.stripe_subscription_id.clone() else {
        return Err(format!("org trial {account}: converting with no subscription id"));
    };
    let Some(stripe) = stripe else {
        return Err(format!("org trial {account}: converting but STRIPE_SECRET_KEY is not configured"));
    };

    let mut sub = crate::billing::seat_lifecycle::with_stripe_deadline(stripe.retrieve_subscription(&subscription_id))
        .await
        .map_err(|e| format!("org trial {account}: retrieve subscription: {e}"))?;
    if conversion_step(&facts_of(&sub)) == ConversionStep::EndTrialAtStripe {
        let key = end_trial_key(account, &subscription_id);
        sub = crate::billing::seat_lifecycle::with_stripe_deadline(stripe.end_trial_now(&subscription_id, &key))
            .await
            .map_err(|e| format!("org trial {account}: end trial at Stripe: {e}"))?;
        report.did("trial_ended_at_stripe");
    }

    let mut step = conversion_step(&facts_of(&sub));
    // #1528 review: the conversion invoice is still a `draft` (Stripe holds
    // it for about an hour after the trial ends before finalizing and
    // charging it). Nothing has failed yet, so the wait is silent: no
    // `payment_failed_at`, which billing would show as "payment failed".
    let mut invoice_draft = false;
    if step == ConversionStep::Finalize {
        // #1528 S1: `active` alone is not a payment. When the subscription
        // names a conversion invoice, its status decides; `active` decides
        // only when there is no invoice to read.
        if let Some(invoice_id) = conversion_invoice_of(&sub) {
            let invoice = crate::billing::seat_lifecycle::with_stripe_deadline(stripe.retrieve_invoice(&invoice_id))
                .await
                .map_err(|e| format!("org trial {account}: read conversion invoice {invoice_id}: {e}"))?;
            step = match super::charge_outcome(Some(&invoice.status), &sub.status) {
                ChargeOutcome::Paid => ConversionStep::Finalize,
                ChargeOutcome::NotPaid => {
                    tracing::warn!(
                        %account, subscription = %sub.id, invoice = %invoice.id, invoice_status = %invoice.status,
                        "org trial: subscription reports active but its conversion invoice was not paid; canceling, no allowance"
                    );
                    ConversionStep::Cancel
                }
                ChargeOutcome::Pending => {
                    invoice_draft = invoice.status == "draft";
                    ConversionStep::AwaitPayment
                }
            };
        }
    }
    if step == ConversionStep::Cancel {
        // Before the row says `canceled`: nothing may charge the card for
        // this conversion afterwards. If this fails the row stays
        // `converting` and the next pass tries again. A conversion found
        // paid on the way is finalized instead (finding A).
        match stop_charging_at_stripe(stripe, account, &sub).await? {
            Stopped::Canceled => report.did("stopped_at_stripe"),
            Stopped::Paid(paid) => {
                sub = paid;
                step = ConversionStep::Finalize;
                report.did("paid_before_cancel");
            }
        }
    }

    let mut tx = scoped_locked(control, account).await?;
    // The row was read without the lock, and Stripe was called since. Re-read
    // it under the lock: a row that is no longer `converting` (the org was
    // deleted in Clerk and its trial canceled, say, #1396) is written to by
    // nobody here. The webhook's late-paid edge, not the worker, owns a
    // conversion paid after a cancel.
    let current = db::load_for_update(&mut tx, account).await.map_err(sql("re-read under lock"))?;
    if current.map(|r| r.state) != Some(TrialState::Converting) {
        tx.rollback().await.ok();
        tracing::info!(%account, "org trial: the row left converting during the Stripe calls; this pass writes nothing");
        return Ok(());
    }
    match step {
        ConversionStep::Finalize => {
            let Some(item) = sub.items.first() else {
                return Err(format!("org trial {account}: active subscription has no items"));
            };
            let (Some(start), Some(end)) = (
                DateTime::from_timestamp(item.current_period_start, 0),
                DateTime::from_timestamp(item.current_period_end, 0),
            ) else {
                return Err(format!("org trial {account}: unrepresentable subscription period"));
            };
            match db::finalize_conversion(&mut tx, account, &sub.id, start, end).await.map_err(sql("finalize"))? {
                FinalizeOutcome::Converted { members, expired, refund_required } => {
                    tracing::info!(%account, members, %expired, refund_required, "org trial: converted, paid allowance granted");
                    report.did("converted");
                    if refund_required {
                        report.did("refund_required");
                    }
                }
                FinalizeOutcome::NotConverting(_) => {}
                FinalizeOutcome::OrgDeleted => {
                    tracing::error!(alert = true, %account, subscription = %sub.id,
                        "org trial: the conversion was paid for an organization deleted in Clerk; not converted, refund it manually");
                    report.did("refused_org_deleted");
                }
                FinalizeOutcome::Superseded { live } => {
                    // Only a late-paid row (already `canceled`) can be
                    // superseded, and the worker finalizes only `converting`.
                    tracing::error!(alert = true, %account, subscription = %sub.id, ?live, "org trial: worker conversion found the org on another subscription; nothing written");
                }
            }
        }
        ConversionStep::AwaitPayment if invoice_draft => {
            tracing::info!(%account, status = %sub.status, "org trial: conversion invoice is still a draft; waiting for Stripe to finalize and charge it");
            report.did("awaiting_invoice");
        }
        ConversionStep::AwaitPayment => {
            db::mark_payment_failed(&mut tx, account).await.map_err(sql("payment failed"))?;
            tracing::warn!(%account, status = %sub.status, "org trial: conversion charge not paid yet; Stripe is retrying");
            report.did("awaiting_payment");
        }
        ConversionStep::Cancel => {
            if db::end(&mut tx, account, TrialState::Canceled, "canceled before the first charge")
                .await
                .map_err(sql("cancel"))?
            {
                report.did("canceled");
            }
        }
        ConversionStep::EndTrialAtStripe => {
            // Stripe answered the trial_end=now update with a subscription
            // still trialing: nothing changed. The next pass retries.
            tracing::warn!(%account, "org trial: Stripe still reports trialing after ending the trial");
        }
    }
    tx.commit().await.map_err(sql("commit"))?;
    Ok(())
}
