//! Settling a reload from what Stripe says: the one place a reload row changes
//! state and the one place it credits money.
//!
//! The same code runs for the answer to our own PaymentIntent request, for a
//! signed `payment_intent.*` webhook, and for the reaper reading Stripe back
//! (the three differ only in how they hear, see [`Observation`]), so the
//! "credit once" and "never lose a charge" rules are written once.
//!
//! # State machine
//!
//! ```text
//! created ──► submitted ──► succeeded            (credited, notice)
//!    │            │
//!    └────────────┴───────► failed               (notice, maybe auto-off)
//!    └────────────┴───────► abandoned            (cancelled; nothing charged)
//! failed | abandoned ─────► succeeded            (money arrived after all)
//! ```
//!
//! `succeeded` is final. A late failure or cancellation event never moves a
//! `succeeded` row, and an old event never un-fails anything. A success always
//! wins, from any state, because it means the customer was charged.
//!
//! # Trust
//!
//! The webhook is signature-verified before it gets here. Beyond that, an event
//! only acts on the row it names (`metadata.reload_id`, for THIS account) and
//! only if the payment matches that row: same account, same customer, USD, the
//! row's amount, and (once recorded) the row's PaymentIntent. The amount
//! credited is the row's, which that check pinned to what Stripe says was
//! charged, never a number from an event on its own.

use sqlx::{types::Decimal, PgPool};
use uuid::Uuid;

use super::stripe::PaymentIntentFacts;
use super::{ReloadRow, MAX_CONSECUTIVE_FAILURES, PURPOSE};
use crate::billing::extra_usage::webhook::{begin_claimed, record, Tx};
use crate::billing::extra_usage::{credit_in_tx, rate_in_force, CreditEntry, CreditKind, CreditOutcome, CENT_MICRO_USD};
use crate::billing::notices::{self, NoticeKind};
use crate::billing::stripe_client::StripeClient;

/// What we learned about a reload's payment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    Succeeded,
    /// The payment was refused. `code` is the decline code (or a coarser
    /// code); `message` is Stripe's own text.
    Failed { code: String, message: Option<String> },
    /// The card wants the customer present (3DS). Off-session that can never
    /// complete, so it is a failure, and the PaymentIntent is cancelled.
    RequiresAction,
    Canceled,
    /// Still in progress at Stripe.
    Processing,
}

impl Observation {
    /// From a PaymentIntent's status (the API answer, or the reaper's read).
    pub fn from_status(pi: &PaymentIntentFacts) -> Self {
        match pi.status.as_str() {
            "succeeded" => Observation::Succeeded,
            "requires_action" => Observation::RequiresAction,
            "requires_payment_method" => Observation::Failed {
                code: pi.error_code.clone().unwrap_or_else(|| "payment_failed".into()),
                message: pi.error_message.clone(),
            },
            "canceled" => Observation::Canceled,
            _ => Observation::Processing,
        }
    }

    /// From a webhook event type. The event type is what Stripe is telling us
    /// happened, so it decides, not the object's (possibly newer) status.
    pub fn from_event(event_type: &str, pi: &PaymentIntentFacts) -> Option<Self> {
        Some(match event_type {
            "payment_intent.succeeded" => Observation::Succeeded,
            "payment_intent.payment_failed" => Observation::Failed {
                code: pi.error_code.clone().unwrap_or_else(|| "payment_failed".into()),
                message: pi.error_message.clone(),
            },
            "payment_intent.requires_action" => Observation::RequiresAction,
            "payment_intent.canceled" => Observation::Canceled,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventOutcome {
    Credited { micro_usd: i64 },
    Failed { auto_reload_turned_off: bool },
    Abandoned,
    Submitted,
    /// Nothing to do: already settled, or an older or duplicate observation.
    Noop,
    /// The same Stripe event was delivered before.
    Replay,
    /// Claimed and recorded; needs a human. Never retried.
    Unprocessable(String),
}

struct Applied {
    outcome: EventOutcome,
    /// A PaymentIntent to cancel once the transaction has committed.
    cancel: Option<(String, Uuid)>,
}

fn applied(outcome: EventOutcome) -> Applied {
    Applied { outcome, cancel: None }
}

fn clip(s: &str) -> String {
    s.chars().take(300).collect()
}

/// Does this payment match the row it names? `Err` is why not.
fn matches_row(row: &ReloadRow, pi: &PaymentIntentFacts) -> Result<(), String> {
    if !pi.is_reload() {
        return Err(format!("purpose is {:?}, not {PURPOSE}", pi.purpose));
    }
    if pi.reload_id != Some(row.id) {
        return Err("payment intent names a different reload".into());
    }
    if pi.account_id.as_deref() != Some(row.account_id.to_string().as_str()) {
        return Err(format!("payment intent account {:?} is not the reload's account", pi.account_id));
    }
    if let (Some(want), Some(got)) = (row.customer_id.as_deref(), pi.customer.as_deref()) {
        if want != got {
            return Err(format!("payment intent customer {got} is not the reload's customer {want}"));
        }
    }
    if !pi.currency.as_deref().is_some_and(|c| c.eq_ignore_ascii_case("usd")) {
        return Err(format!("payment intent currency is {:?}, not usd", pi.currency));
    }
    match pi.amount_cents.and_then(|c| c.checked_mul(CENT_MICRO_USD)) {
        Some(m) if m == row.amount_micro_usd => {}
        other => {
            return Err(format!(
                "payment intent amount is {:?} micro-USD, the reload is {}",
                other, row.amount_micro_usd
            ))
        }
    }
    if let Some(have) = row.payment_intent_id.as_deref() {
        if have != pi.id {
            return Err(format!("reload is recorded against {have}, not {}", pi.id));
        }
    }
    Ok(())
}

/// Apply one observation to one row, inside the caller's fenced transaction.
async fn apply_in_tx(
    tx: &mut Tx,
    account: Uuid,
    reload: Uuid,
    obs: &Observation,
    pi: Option<&PaymentIntentFacts>,
    event_id: Option<&str>,
) -> Result<Applied, sqlx::Error> {
    let row: Option<super::RowTuple> = sqlx::query_as(&format!(
        "SELECT {} FROM extra_usage_reloads WHERE id = $1 AND account_id = $2 FOR UPDATE",
        ReloadRow::COLUMNS
    ))
    .bind(reload)
    .bind(account)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row.map(ReloadRow::from) else {
        return Ok(applied(EventOutcome::Unprocessable(format!("no reload {reload} for this account"))));
    };
    if let Some(pi) = pi {
        if let Err(why) = matches_row(&row, pi) {
            if *obs == Observation::Succeeded {
                tracing::error!(
                    %account, %reload, payment_intent = %pi.id, %why,
                    "auto-reload: a SUCCEEDED payment does not match its reload row; nothing credited; needs a human"
                );
            }
            return Ok(applied(EventOutcome::Unprocessable(why)));
        }
    }
    let in_flight = matches!(row.state.as_str(), "created" | "submitted");
    let pi_id = pi.map(|p| p.id.as_str());

    match obs {
        Observation::Succeeded => {
            let Some(pi) = pi else {
                return Ok(applied(EventOutcome::Unprocessable("success without a payment intent".into())));
            };
            if row.state == "succeeded" {
                return Ok(applied(EventOutcome::Noop));
            }
            let rate = rate_in_force(tx).await?;
            let entry = CreditEntry {
                kind: CreditKind::AutoReload,
                micro_usd: row.amount_micro_usd,
                credits: Some(Decimal::from(row.amount_micro_usd) / Decimal::from(rate)),
                rate_micro_usd_per_credit: rate,
                idempotency_key: format!("reload:{}", row.id),
                stripe_event_id: event_id,
                stripe_payment_intent_id: Some(&pi.id),
                stripe_checkout_session_id: None,
                reload_id: Some(row.id),
                note: None,
            };
            let credited = credit_in_tx(tx, account, &entry).await?;
            sqlx::query(
                "UPDATE extra_usage_reloads SET state = 'succeeded', \
                        stripe_payment_intent_id = COALESCE(stripe_payment_intent_id, $2), \
                        submitted_at = COALESCE(submitted_at, now()), settled_at = now(), \
                        failure_code = NULL, failure_message = NULL, attempts = attempts + 1, updated_at = now() \
                  WHERE id = $1",
            )
            .bind(row.id)
            .bind(&pi.id)
            .execute(&mut **tx)
            .await?;
            notices::claim(tx, account, NoticeKind::AutoReloadSucceeded, &row.id.to_string(), None).await?;
            if matches!(credited, CreditOutcome::Credited { .. }) {
                alert_if_over_limit(tx, account, &row, in_flight).await?;
            }
            Ok(applied(match credited {
                CreditOutcome::Credited { .. } => EventOutcome::Credited { micro_usd: row.amount_micro_usd },
                CreditOutcome::Duplicate => EventOutcome::Noop,
            }))
        }

        Observation::Failed { .. } | Observation::RequiresAction => {
            if !in_flight {
                return Ok(applied(EventOutcome::Noop));
            }
            let (code, message) = match obs {
                Observation::Failed { code, message } => (code.clone(), message.clone()),
                _ => ("authentication_required".to_string(), Some("The card requires authentication.".to_string())),
            };
            sqlx::query(
                "UPDATE extra_usage_reloads SET state = 'failed', failure_code = $2, failure_message = $3, \
                        stripe_payment_intent_id = COALESCE(stripe_payment_intent_id, $4), \
                        settled_at = now(), attempts = attempts + 1, updated_at = now() \
                  WHERE id = $1",
            )
            .bind(row.id)
            .bind(clip(&code))
            .bind(message.as_deref().map(clip))
            .bind(pi_id)
            .execute(&mut **tx)
            .await?;

            // Consecutive failures since the last success and since the customer
            // last turned auto-reload on (not since any settings save); this row
            // is the newest.
            let streak: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM extra_usage_reloads r WHERE r.account_id = $1 AND r.state = 'failed' \
                    AND r.created_at > GREATEST( \
                          COALESCE((SELECT max(settled_at) FROM extra_usage_reloads \
                                     WHERE account_id = $1 AND state = 'succeeded'), '-infinity'::timestamptz), \
                          COALESCE((SELECT auto_reload_reset_at FROM extra_usage_settings WHERE account_id = $1), '-infinity'::timestamptz))",
            )
            .bind(account)
            .fetch_one(&mut **tx)
            .await?;
            let turned_off = streak >= MAX_CONSECUTIVE_FAILURES;
            if turned_off {
                sqlx::query(
                    "UPDATE extra_usage_settings SET auto_reload_enabled = false, updated_by_user_id = NULL, \
                            updated_at = now() WHERE account_id = $1",
                )
                .bind(account)
                .execute(&mut **tx)
                .await?;
                sqlx::query("UPDATE extra_usage_reloads SET disabled_auto_reload = true WHERE id = $1")
                    .bind(row.id)
                    .execute(&mut **tx)
                    .await?;
                tracing::warn!(%account, reload_id = %row.id, streak, "auto-reload: turned off after consecutive failures");
            }
            notices::claim(tx, account, NoticeKind::AutoReloadFailed, &row.id.to_string(), None).await?;
            let cancel = (matches!(obs, Observation::RequiresAction))
                .then(|| pi_id.map(|p| (p.to_string(), row.id)))
                .flatten();
            Ok(Applied { outcome: EventOutcome::Failed { auto_reload_turned_off: turned_off }, cancel })
        }

        Observation::Canceled => {
            if !in_flight {
                return Ok(applied(EventOutcome::Noop));
            }
            sqlx::query(
                "UPDATE extra_usage_reloads SET state = 'abandoned', \
                        stripe_payment_intent_id = COALESCE(stripe_payment_intent_id, $2), \
                        settled_at = now(), updated_at = now() WHERE id = $1",
            )
            .bind(row.id)
            .bind(pi_id)
            .execute(&mut **tx)
            .await?;
            Ok(applied(EventOutcome::Abandoned))
        }

        Observation::Processing => {
            if row.state != "created" {
                return Ok(applied(EventOutcome::Noop));
            }
            let Some(pi_id) = pi_id else {
                return Ok(applied(EventOutcome::Noop));
            };
            sqlx::query(
                "UPDATE extra_usage_reloads SET state = 'submitted', stripe_payment_intent_id = $2, \
                        submitted_at = now(), attempts = attempts + 1, updated_at = now() \
                  WHERE id = $1 AND state = 'created'",
            )
            .bind(row.id)
            .bind(pi_id)
            .execute(&mut **tx)
            .await?;
            Ok(applied(EventOutcome::Submitted))
        }
    }
}

/// A success we credit anyway (the money was taken) can push this month's
/// reloads past the monthly limit: a row we had given up on (`abandoned`, or
/// `failed`) whose charge went through after all, while a newer reload was
/// sized against a month that did not include it. Credit it and say so loudly;
/// the customer is owed the credits, and an operator may want to look.
async fn alert_if_over_limit(tx: &mut Tx, account: Uuid, row: &ReloadRow, was_in_flight: bool) -> Result<(), sqlx::Error> {
    let (reloaded, limit): (i64, Option<i64>) = sqlx::query_as(
        "SELECT COALESCE((SELECT sum(r.amount_micro_usd) FROM extra_usage_reloads r \
                           WHERE r.account_id = $1 AND r.state = 'succeeded' \
                             AND r.settled_at >= date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'), 0)::bigint, \
                (SELECT monthly_limit_micro_usd FROM extra_usage_settings WHERE account_id = $1)",
    )
    .bind(account)
    .fetch_one(&mut **tx)
    .await?;
    if let Some(limit) = limit.filter(|l| reloaded > *l) {
        tracing::error!(
            alert = true, %account, reload_id = %row.id, late = !was_in_flight,
            reloaded_this_month_micro_usd = reloaded, monthly_limit_micro_usd = limit,
            "auto-reload: crediting a payment took this month's reloads past the monthly limit; \
             credited anyway because the money was taken"
        );
    }
    Ok(())
}

/// Best effort, after commit: a PaymentIntent that needs the customer present
/// will never be completed, so cancel it. A failure only leaves an inert intent
/// (and would still be credited if somehow paid).
async fn cancel_after_commit(stripe: Option<&StripeClient>, cancel: Option<(String, Uuid)>) {
    let (Some(stripe), Some((pi, reload))) = (stripe, cancel) else { return };
    if let Err(e) = stripe.cancel_payment_intent(&pi, &format!("reload-cancel:{reload}")).await {
        tracing::warn!(payment_intent = %pi, %reload, error = %e, "auto-reload: could not cancel an unusable payment intent");
    }
}

/// Apply what we heard outside a webhook (our own request's answer, or the
/// reaper's read) in its own fenced transaction.
///
/// Worker-rate, so it goes through the in-process entry queue (#1497) with a
/// bounded lock wait; the webhook path (`apply_event`, via `begin_claimed`)
/// does not. The turn is released before the post-commit Stripe call.
pub async fn apply_observation(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account: Uuid,
    reload: Uuid,
    obs: &Observation,
    pi: Option<&PaymentIntentFacts>,
) -> Result<EventOutcome, sqlx::Error> {
    let (turn, mut tx) = crate::billing::entry_queue::begin_entered(account, "auto_reload_settle", || {
        crate::tenancy::scope::begin_account_scoped(control, account)
    })
    .await?;
    let a = apply_in_tx(&mut tx, account, reload, obs, pi, None).await?;
    tx.commit().await?;
    drop(turn);
    cancel_after_commit(stripe, a.cancel).await;
    Ok(a.outcome)
}

/// A signed `payment_intent.*` event for an auto-reload PaymentIntent. One
/// account-scoped transaction: the fence, the event claim, the settlement. Any
/// error rolls back the claim too, so Stripe's redelivery retries cleanly.
pub async fn apply_event(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account: Uuid,
    event_id: &str,
    event_type: &str,
    pi: &PaymentIntentFacts,
) -> Result<EventOutcome, sqlx::Error> {
    let Some(mut tx) = begin_claimed(control, account, event_id, event_type).await? else {
        return Ok(EventOutcome::Replay);
    };
    let (obs, reload) = match (Observation::from_event(event_type, pi), pi.reload_id) {
        (Some(o), Some(r)) => (o, r),
        _ => {
            let why = format!("{event_type} for payment intent {} carries no usable reload", pi.id);
            tracing::error!(event_id, %account, %why, "auto-reload webhook: claimed and recorded, not retried");
            record(&mut tx, event_id, &format!("unprocessable: {why}")).await?;
            tx.commit().await?;
            return Ok(EventOutcome::Unprocessable(why));
        }
    };
    let a = apply_in_tx(&mut tx, account, reload, &obs, Some(pi), Some(event_id)).await?;
    let label = match &a.outcome {
        EventOutcome::Credited { micro_usd } => format!("auto_reload credited micro_usd={micro_usd}"),
        EventOutcome::Failed { auto_reload_turned_off } => format!("auto_reload failed auto_off={auto_reload_turned_off}"),
        EventOutcome::Unprocessable(why) => {
            tracing::error!(event_id, %account, %reload, %why, "auto-reload webhook: not applicable; claimed and recorded; needs a human");
            format!("unprocessable: {why}")
        }
        other => format!("auto_reload {}", format!("{other:?}").to_lowercase()),
    };
    record(&mut tx, event_id, &label).await?;
    tx.commit().await?;
    tracing::info!(event_id, %account, %reload, outcome = ?a.outcome, "auto-reload webhook applied");
    cancel_after_commit(stripe, a.cancel).await;
    Ok(a.outcome)
}
