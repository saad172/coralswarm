//! Payment-mode `checkout.session.completed`: credits the extra-usage ledger
//! (settings launch plan PR 3.4, decision 11).
//!
//! Two kinds of payment-mode session reach here, told apart by
//! `metadata[purpose]` on the RE-FETCHED session ([`PaymentFacts`]):
//!
//! * a top-up ([`apply_topup`]): created by `POST /v1/billing/extra-usage/topup`;
//! * a credit pack ([`apply_pack_purchase`]): created by the older
//!   `POST /v1/billing/checkout/pack`, and any pack session created before this
//!   PR (no metadata). Packs used to credit `credit_balances`, the plan pool.
//!   That balance is the allowance every debit subtracts from, so credits
//!   bought after the allowance ran out could never be spent. They now credit
//!   the extra-usage balance too. Prod had zero pack purchases when this
//!   shipped, so there is nothing to migrate; a session the OLD code already
//!   credited is recognised by `credit_ledger.stripe_checkout_session_id` and
//!   not credited a second time.
//!
//! # Discipline (same as every other billing webhook writer)
//!
//! One account-scoped transaction: the account's financial entry fence first
//! (`lock_period_rotation`, before even the event claim), then the
//! `stripe_webhook_events` claim, then the credit. Any error rolls the whole
//! thing back INCLUDING the claim, so Stripe's redelivery retries cleanly.
//! A session that is not yet paid (a delayed method) is recorded and answered
//! 200; `checkout.session.async_payment_succeeded` credits it once paid and
//! `async_payment_failed` records the failure. Pack and top-up sessions are
//! also created card-only, so this is a second line of defence.
//! A payment that can never be processed (unpaid, wrong account, wrong
//! currency) keeps its claim and records the reason, and answers 200:
//! retrying cannot fix it, and it needs a human.
//!
//! # What is credited
//!
//! What Stripe says was paid BEFORE TAX (`amount_subtotal`, USD), never a
//! number from the request or from metadata. `amount_total` includes the tax
//! Stripe collects for the government, which is not a balance the customer
//! bought: a $5.00 top-up that totals $5.44 credits $5.00. A session without
//! an `amount_subtotal` is unprocessable rather than falling back to the
//! taxed total. A pack is additionally capped at its list price,
//! so a pack can never credit more than the pack is worth. The credit does
//! not depend on the account's plan, extra-usage setting or subscription
//! state: an out-of-order or late event for money already taken is still
//! credited, and whether it can be spent is the funding policy's decision
//! (frozen, Free and off all refuse spending).

use serde_json::Value;
use sqlx::types::Decimal;
use uuid::Uuid;

use super::{credit_in_tx, rate_in_force, CreditEntry, CreditKind, CreditOutcome, CENT_MICRO_USD, TOPUP_PURPOSE};
use crate::billing::stripe_client::SessionLineItem;

/// What the (re-fetched) Checkout session says about the payment.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PaymentFacts {
    pub payment_intent: Option<String>,
    pub amount_total_cents: Option<i64>,
    /// Pre-tax amount: what is credited (`amount_total` includes tax).
    pub amount_subtotal_cents: Option<i64>,
    pub currency: Option<String>,
    pub payment_status: Option<String>,
    pub purpose: Option<String>,
    pub metadata_account_id: Option<String>,
}

impl PaymentFacts {
    pub fn from_session(session: &Value) -> Self {
        let text = |v: Option<&Value>| v.and_then(Value::as_str).map(str::to_string);
        let metadata = session.get("metadata");
        let payment_intent = match session.get("payment_intent") {
            Some(Value::String(s)) => Some(s.clone()),
            Some(obj @ Value::Object(_)) => text(obj.get("id")),
            _ => None,
        };
        PaymentFacts {
            payment_intent,
            amount_total_cents: session.get("amount_total").and_then(Value::as_i64),
            amount_subtotal_cents: session.get("amount_subtotal").and_then(Value::as_i64),
            currency: text(session.get("currency")),
            payment_status: text(session.get("payment_status")),
            purpose: text(metadata.and_then(|m| m.get("purpose"))),
            metadata_account_id: text(metadata.and_then(|m| m.get("account_id"))),
        }
    }

    pub fn is_topup(&self) -> bool {
        self.purpose.as_deref() == Some(TOPUP_PURPOSE)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaymentOutcome {
    Credited { micro_usd: i64 },
    /// The same event was delivered before.
    Replay,
    /// A different event already credited this session or payment.
    DuplicateSession,
    /// Claimed and recorded; needs a human. Never retried.
    Unprocessable(String),
}

pub(super) type Tx = sqlx::Transaction<'static, sqlx::Postgres>;

pub(super) fn proto(e: impl std::fmt::Display) -> sqlx::Error {
    sqlx::Error::Protocol(e.to_string())
}

/// Open the account's transaction, enter the fence, claim the event.
/// `None`: the event was already claimed (a replay); the transaction is gone.
pub(super) async fn begin_claimed(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
) -> Result<Option<Tx>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id).await.map_err(proto)?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    let claimed = sqlx::query(
        "INSERT INTO stripe_webhook_events (event_id, event_type, account_id) \
         VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(account_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if claimed == 0 {
        let _ = tx.rollback().await;
        tracing::info!(event_id, %account_id, "Stripe webhook: replay, already processed");
        return Ok(None);
    }
    Ok(Some(tx))
}

pub(super) async fn record(tx: &mut Tx, event_id: &str, outcome: &str) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
        .bind(event_id)
        .bind(outcome)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn unprocessable(
    mut tx: Tx,
    account_id: Uuid,
    event_id: &str,
    session_id: &str,
    reason: String,
) -> Result<PaymentOutcome, sqlx::Error> {
    tracing::error!(
        event_id, %account_id, session_id, %reason,
        "Stripe webhook: payment checkout not creditable; claimed and recorded, not retried; needs a human"
    );
    record(&mut tx, event_id, &format!("unprocessable: {reason}")).await?;
    tx.commit().await?;
    Ok(PaymentOutcome::Unprocessable(reason))
}

/// Credit `micro_usd` once and finish the transaction.
#[allow(clippy::too_many_arguments)]
async fn credit_and_finish(
    mut tx: Tx,
    account_id: Uuid,
    event_id: &str,
    session_id: &str,
    facts: &PaymentFacts,
    kind: CreditKind,
    micro_usd: i64,
    label: &str,
) -> Result<PaymentOutcome, sqlx::Error> {
    let rate = rate_in_force(&mut tx).await?;
    let entry = CreditEntry {
        kind,
        micro_usd,
        credits: Some(Decimal::from(micro_usd) / Decimal::from(rate)),
        rate_micro_usd_per_credit: rate,
        idempotency_key: format!("checkout_session:{session_id}"),
        stripe_event_id: Some(event_id),
        stripe_payment_intent_id: facts.payment_intent.as_deref(),
        stripe_checkout_session_id: Some(session_id),
        reload_id: None,
        note: None,
    };
    let outcome = match credit_in_tx(&mut tx, account_id, &entry).await? {
        CreditOutcome::Credited { balance_after_micro_usd } => {
            record(
                &mut tx,
                event_id,
                &format!("{label} micro_usd={micro_usd} balance_after={balance_after_micro_usd}"),
            )
            .await?;
            PaymentOutcome::Credited { micro_usd }
        }
        CreditOutcome::Duplicate => {
            // A DIFFERENT event already credited this session or payment (a
            // dashboard resend). Keep this event's claim, credit nothing.
            record(&mut tx, event_id, &format!("{label} duplicate_session session_id={session_id}")).await?;
            PaymentOutcome::DuplicateSession
        }
    };
    tx.commit().await?;
    tracing::info!(event_id, %account_id, ?outcome, "Stripe webhook: payment checkout completed");
    Ok(outcome)
}

/// An extra-usage top-up session completed.
pub async fn apply_topup(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    session_id: &str,
    facts: &PaymentFacts,
) -> Result<PaymentOutcome, sqlx::Error> {
    let Some(tx) = begin_claimed(control, account_id, event_id, event_type).await? else {
        return Ok(PaymentOutcome::Replay);
    };
    let bad = |why: String| why;
    // Cheap structural checks first; each keeps the claim and records why.
    let reason = if facts.payment_status.as_deref() != Some("paid") {
        bad(format!("top-up session payment_status is {:?}, not paid", facts.payment_status))
    } else if facts.metadata_account_id.as_deref() != Some(account_id.to_string().as_str()) {
        bad(format!(
            "top-up session metadata account {:?} does not match the customer's account",
            facts.metadata_account_id
        ))
    } else if !facts.currency.as_deref().is_some_and(|c| c.eq_ignore_ascii_case("usd")) {
        bad(format!("top-up session currency is {:?}, not usd", facts.currency))
    } else {
        match facts.amount_subtotal_cents {
            Some(c) if c > 0 && c.checked_mul(CENT_MICRO_USD).is_some() => String::new(),
            other => bad(format!("top-up session amount_subtotal is {other:?}")),
        }
    };
    if !reason.is_empty() {
        return unprocessable(tx, account_id, event_id, session_id, reason).await;
    }
    let micro = facts.amount_subtotal_cents.unwrap_or(0) * CENT_MICRO_USD;
    credit_and_finish(tx, account_id, event_id, session_id, facts, CreditKind::Topup, micro, "checkout_completed_topup")
        .await
}

/// A credit-pack session completed (module doc: now credits the extra ledger).
pub async fn apply_pack_purchase(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    session_id: &str,
    facts: &PaymentFacts,
    line_items: &[SessionLineItem],
) -> Result<PaymentOutcome, sqlx::Error> {
    let Some(mut tx) = begin_claimed(control, account_id, event_id, event_type).await? else {
        return Ok(PaymentOutcome::Replay);
    };

    // A delayed payment method completes the session unpaid. Credit only once
    // Stripe says paid: either here, or on the later
    // `checkout.session.async_payment_succeeded` (same session id, so the
    // session idempotency key keeps it to one credit).
    if facts.payment_status.as_deref() != Some("paid") {
        return unprocessable(
            tx,
            account_id,
            event_id,
            session_id,
            format!("pack session payment_status is {:?}, not paid", facts.payment_status),
        )
        .await;
    }

    let mut list_cents: i64 = 0;
    for item in line_items {
        let row: Option<(Decimal, i32)> = sqlx::query_as(
            "SELECT credits, price_usd_cents FROM credit_packs WHERE stripe_price_id = $1 AND is_active = true",
        )
        .bind(&item.price_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((_credits, price_cents)) = row else {
            return unprocessable(tx, account_id, event_id, session_id, format!("unrecognized pack price {}", item.price_id))
                .await;
        };
        let line = i64::from(price_cents).checked_mul(item.quantity);
        match line.and_then(|l| list_cents.checked_add(l)) {
            Some(total) => list_cents = total,
            None => {
                return unprocessable(tx, account_id, event_id, session_id, "pack total overflows".into()).await;
            }
        }
    }
    if let Some(c) = facts.currency.as_deref() {
        if !c.eq_ignore_ascii_case("usd") {
            return unprocessable(tx, account_id, event_id, session_id, format!("pack session currency is {c:?}"))
                .await;
        }
    }
    // Never more than was paid pre-tax, never more than list. The taxed
    // `amount_total` is not used: tax is not a purchased balance.
    let cents = match facts.amount_subtotal_cents {
        Some(paid) if paid > 0 => paid.min(list_cents),
        Some(paid) => {
            return unprocessable(tx, account_id, event_id, session_id, format!("pack session amount_subtotal is {paid}"))
                .await;
        }
        // Never fall back to list price: credit only what Stripe says was paid.
        None => {
            return unprocessable(tx, account_id, event_id, session_id, "pack session carries no amount_subtotal".into())
                .await;
        }
    };
    if cents <= 0 {
        return unprocessable(tx, account_id, event_id, session_id, "pack purchase totals zero".into()).await;
    }

    // A session the pre-3.4 code already credited to the plan pool.
    let legacy: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM credit_ledger WHERE account_id = $1 AND stripe_checkout_session_id = $2)",
    )
    .bind(account_id)
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await?;
    if legacy {
        record(&mut tx, event_id, &format!("checkout_completed_payment duplicate_session legacy session_id={session_id}"))
            .await?;
        tx.commit().await?;
        return Ok(PaymentOutcome::DuplicateSession);
    }

    credit_and_finish(
        tx,
        account_id,
        event_id,
        session_id,
        facts,
        CreditKind::PackPurchase,
        cents * CENT_MICRO_USD,
        "checkout_completed_payment",
    )
    .await
}
