//! `cancel_account_subscription`: schedule an account's Stripe subscription
//! to end at the close of the paid period. Settings launch PR 3.3; the
//! delete-account flow (PR 2.4) is the caller that needs it.
//!
//! # Semantics
//!
//! * Cancel AT PERIOD END, never immediately: the account keeps what it paid
//!   for until the period closes. When Stripe then sends
//!   `customer.subscription.deleted`, `routes::stripe_webhook` freezes the
//!   account (decision 19: purchased extra-usage credits are frozen, not
//!   forfeited, on every cancellation; only delete-account forfeits them, and
//!   that is the caller's job, not this function's).
//! * Idempotent. The Stripe write (`cancel_at_period_end=true`) has no
//!   cumulative effect, so it is sent as `Idem::NaturallyIdempotent` rather
//!   than under a replay key. A key would be wrong here: Stripe caches a
//!   keyed response for 24 hours, so a cancel, un-cancel in the portal, and
//!   cancel again inside a day would replay the first answer and silently not
//!   cancel. Before writing, the live subscription is read; if it is already
//!   scheduled (or ended) nothing is sent.
//! * No subscription (never subscribed, or customer-only row) is a normal
//!   outcome, [`CancelOutcome::NoSubscription`], not an error: a free account
//!   being deleted has nothing to cancel.
//!
//! The local `stripe_subscriptions.cancel_at_period_end/cancel_at` mirror is
//! written here too, so the caller and the Billing page see the new state
//! without waiting for the webhook. The webhook writes the same fields and
//! converges on Stripe's truth if the two ever differ.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::billing::stripe_client::{parse_cancel_state, Idem, StripeClient, StripeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The account has no Stripe subscription. Nothing to cancel.
    NoSubscription,
    /// The subscription has already ended (`canceled`).
    AlreadyEnded,
    /// It was already scheduled to end; no Stripe write was made.
    AlreadyScheduled { cancel_at: Option<DateTime<Utc>> },
    /// This call scheduled the cancellation.
    Scheduled { cancel_at: Option<DateTime<Utc>> },
    /// Ended immediately (delete-account only).
    EndedNow,
}

#[derive(Debug, thiserror::Error)]
pub enum CancelError {
    /// A subscription exists but Stripe is not configured on this process.
    #[error("stripe is not configured")]
    NotConfigured,
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("stripe error: {0}")]
    Stripe(#[from] StripeError),
}

fn to_utc(secs: Option<i64>) -> Option<DateTime<Utc>> {
    secs.and_then(|s| DateTime::from_timestamp(s, 0))
}

/// Persist the cancellation mirror for the account's CURRENT subscription.
/// Matches on `stripe_subscription_id` so a late write about a superseded
/// subscription cannot mark a resubscribed account as cancelling. Returns
/// whether a row was updated.
pub(crate) async fn mirror_cancel_state(
    control: &PgPool,
    account_id: Uuid,
    subscription_id: &str,
    cancel_at_period_end: bool,
    cancel_at: Option<DateTime<Utc>>,
) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    let updated = sqlx::query(
        "UPDATE stripe_subscriptions \
            SET cancel_at_period_end = $3, cancel_at = $4, updated_at = now() \
          WHERE account_id = $1 AND stripe_subscription_id = $2",
    )
    .bind(account_id)
    .bind(subscription_id)
    .bind(cancel_at_period_end)
    .bind(cancel_at)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(updated > 0)
}

pub async fn cancel_account_subscription(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account_id: Uuid,
) -> Result<CancelOutcome, CancelError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;

    let Some((Some(subscription_id), status)) = row else {
        return Ok(CancelOutcome::NoSubscription);
    };
    if status == "canceled" {
        return Ok(CancelOutcome::AlreadyEnded);
    }
    let Some(stripe) = stripe else {
        tracing::error!(%account_id, "cancel_account_subscription: stripe not configured but a subscription exists");
        return Err(CancelError::NotConfigured);
    };

    let live = crate::billing::seat_lifecycle::with_stripe_deadline(
        stripe.retrieve_subscription(&subscription_id),
    )
    .await?;
    if live.status == "canceled" {
        return Ok(CancelOutcome::AlreadyEnded);
    }
    if live.cancel_at_period_end {
        let cancel_at = to_utc(live.cancel_at);
        mirror_cancel_state(control, account_id, &subscription_id, true, cancel_at).await?;
        return Ok(CancelOutcome::AlreadyScheduled { cancel_at });
    }

    // `cancellation_details[reason]` is read-only on Stripe's side, so no
    // reason is sent.
    let updated = crate::billing::seat_lifecycle::with_stripe_deadline(stripe.update_subscription(
        &subscription_id,
        true,
        None,
        Idem::NaturallyIdempotent,
    ))
    .await?;
    let (scheduled, cancel_at_secs) = parse_cancel_state(&updated);
    if !scheduled {
        return Err(CancelError::Stripe(StripeError::Parse(
            "subscription update did not report cancel_at_period_end=true".into(),
        )));
    }
    let cancel_at = to_utc(cancel_at_secs);
    mirror_cancel_state(control, account_id, &subscription_id, true, cancel_at).await?;
    tracing::info!(%account_id, subscription_id, ?cancel_at, "subscription scheduled to cancel at period end");
    Ok(CancelOutcome::Scheduled { cancel_at })
}

/// End the account's subscription IMMEDIATELY (delete-account, settings 2.4).
///
/// Unlike [`cancel_account_subscription`] this does not wait for the period to
/// close: the customer is being deleted, so nothing may bill them again, and
/// "keeps what they paid for until period end" has no meaning for an account
/// nobody can sign in to. It sends `prorate=false, invoice_now=false`, so no
/// proration credit or final invoice is created either; remaining paid time is
/// forfeited. Refunds are manual at launch (decision 18).
///
/// Idempotent and crash-safe, which is what the deletion job relies on: the
/// live subscription is read first and a `canceled` one is never written to,
/// so a crash between Stripe's answer and the job's checkpoint re-runs as
/// [`CancelOutcome::AlreadyEnded`] with no second Stripe write. The local
/// `customer.subscription.deleted` webhook owns the semantic status change
/// (freeze etc.); this function only clears the scheduled-cancel mirror.
pub async fn cancel_account_subscription_now(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account_id: Uuid,
) -> Result<CancelOutcome, CancelError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;

    let Some((Some(subscription_id), status)) = row else {
        return Ok(CancelOutcome::NoSubscription);
    };
    if status == "canceled" {
        return Ok(CancelOutcome::AlreadyEnded);
    }
    let Some(stripe) = stripe else {
        tracing::error!(%account_id, "cancel_account_subscription_now: stripe not configured but a subscription exists");
        return Err(CancelError::NotConfigured);
    };
    let live = crate::billing::seat_lifecycle::with_stripe_deadline(
        stripe.retrieve_subscription(&subscription_id),
    )
    .await?;
    if live.status == "canceled" {
        return Ok(CancelOutcome::AlreadyEnded);
    }
    crate::billing::seat_lifecycle::with_stripe_deadline(
        stripe.cancel_subscription_now_without_proration(&subscription_id),
    )
    .await?;
    mirror_cancel_state(control, account_id, &subscription_id, false, None).await?;
    tracing::info!(%account_id, subscription_id, "subscription ended immediately (account deletion)");
    Ok(CancelOutcome::EndedNow)
}

/// The `Idempotency-Key` that ends a deleted org's subscription: one per
/// subscription id, the same for every redelivery of the Clerk event.
///
/// The same key as in-app org deletion's own cancel and as
/// `routes::stripe_webhook::refuse_for_deleting_org` (one definition, in
/// `services::org_deletion`), so whichever of the three runs second replays
/// Stripe's first answer instead of cancelling twice.
pub fn deleted_org_cancel_key(subscription_id: &str) -> String {
    crate::services::org_deletion::deleted_org_cancel_key(subscription_id)
}

/// The `Idempotency-Key` that voids a deleted org's open invoice: one per
/// invoice, so a redelivery replays Stripe's first answer.
pub fn deleted_org_void_key(invoice_id: &str) -> String {
    format!("org-deleted-void:{invoice_id}")
}

/// Is `account_id` an org that Clerk deleted, whose billing must end?
///
/// True for an org account (`accounts.clerk_org_id` set) with an
/// `org_revocations` row, unless that Clerk org is a PERSONAL org
/// (`personal_orgs`, control 0208). After `bin/adopt_personal_org_account`
/// a person's own account carries their personal org's id, and deleting
/// that Clerk org must never cancel the person's own subscription.
///
/// Read by `org_trial::db::finalize_conversion`: a conversion paid after the
/// deletion (late, or from the invoice's hosted page) never revives the org.
pub(crate) async fn org_deleted_in_clerk<'e, E>(conn: E, account_id: Uuid) -> Result<bool, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM accounts a \
                          JOIN org_revocations r ON r.clerk_org_id = a.clerk_org_id \
                         WHERE a.id = $1 \
                           AND NOT EXISTS (SELECT 1 FROM personal_orgs p WHERE p.clerk_org_id = a.clerk_org_id))",
    )
    .bind(account_id)
    .fetch_one(conn)
    .await
}

fn subscription_ended(status: &str) -> bool {
    matches!(status, "canceled" | "incomplete_expired")
}

/// Void the subscription's latest invoice when it is `open` (#1396 review):
/// cancelling a subscription stops Stripe's automatic collection, but an
/// open invoice can still be paid by hand from its hosted page, and a paid
/// conversion invoice would otherwise reach `invoice.paid`'s late-paid edge.
///
/// * not `open` (`paid`, `void`, `uncollectible`, `draft`, or no invoice):
///   nothing to void. A `draft` cannot be voided; once the subscription is
///   cancelled Stripe does not collect it automatically, and should it be
///   paid anyway `finalize_conversion` refuses to convert a deleted org
///   (it raises a refund alert instead).
/// * the void fails: the invoice is re-read. Anything but still `open` (the
///   customer paid it, or another writer voided it) is accepted; still
///   `open` is the error, and the webhook answers 5xx so Clerk retries.
async fn void_open_latest_invoice(
    stripe: &StripeClient,
    sub: &crate::billing::stripe_client::SubscriptionView,
) -> Result<(), CancelError> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let Some(invoice_id) = sub.latest_invoice.as_deref() else {
        return Ok(());
    };
    let invoice = with_stripe_deadline(stripe.retrieve_invoice(invoice_id)).await?;
    if invoice.status != "open" {
        return Ok(());
    }
    if let Err(e) = with_stripe_deadline(stripe.void_invoice(invoice_id, &deleted_org_void_key(invoice_id))).await {
        let after = with_stripe_deadline(stripe.retrieve_invoice(invoice_id)).await?;
        if after.status == "open" {
            return Err(e.into());
        }
        tracing::warn!(invoice = invoice_id, status = %after.status, "deleted org: open invoice was not voided; it is no longer open");
        return Ok(());
    }
    tracing::info!(subscription = %sub.id, invoice = invoice_id, "deleted org: open invoice voided before the cancel");
    Ok(())
}

/// End ONE subscription of a deleted org at Stripe, now: void its open
/// invoice, then one keyed `DELETE` (`prorate=false`, `invoice_now=false`).
/// No write for a subscription Stripe already reports ended; a `DELETE`
/// that errors is followed by a re-read, and "ended anyway" is success.
async fn end_deleted_org_subscription(stripe: &StripeClient, subscription_id: &str) -> Result<CancelOutcome, CancelError> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let live = with_stripe_deadline(stripe.retrieve_subscription(subscription_id)).await?;
    if subscription_ended(&live.status) {
        return Ok(CancelOutcome::AlreadyEnded);
    }
    void_open_latest_invoice(stripe, &live).await?;
    let key = deleted_org_cancel_key(subscription_id);
    if let Err(e) = with_stripe_deadline(stripe.cancel_subscription_now_keyed(subscription_id, &key)).await {
        let after = with_stripe_deadline(stripe.retrieve_subscription(subscription_id)).await?;
        if subscription_ended(&after.status) {
            return Ok(CancelOutcome::AlreadyEnded);
        }
        return Err(e.into());
    }
    Ok(CancelOutcome::EndedNow)
}

/// End a Clerk-deleted org's subscription NOW (#1396, the
/// `organization.deleted` webhook).
///
/// Same terms as [`cancel_account_subscription_now`] (no proration, no final
/// invoice): the org is gone and its revocation is permanent, so nobody can
/// use what is left of the period. The local `customer.subscription.deleted`
/// webhook still owns the status change and the freeze.
///
/// Idempotent, keyed on the subscription id, so a Clerk redelivery (or a
/// sweep that already cancelled before deleting the org, #1533) never
/// writes twice:
///
/// * no mirror row, no subscription id, or a mirror that says `canceled`:
///   no Stripe call at all;
/// * the live subscription is read first; `canceled`/`incomplete_expired`
///   is [`CancelOutcome::AlreadyEnded`] with no write;
/// * otherwise its open latest invoice is voided (see
///   [`void_open_latest_invoice`]) and ONE `DELETE` is sent, under
///   [`deleted_org_cancel_key`];
/// * a `DELETE` that errors is followed by a re-read: a subscription that is
///   ended anyway (another writer won) is `AlreadyEnded`, anything else is
///   the error, so the webhook answers 5xx and Svix retries.
///
/// Only the subscription the mirror names is ended. Another subscription
/// on the same Stripe customer is not looked for (#1548).
pub async fn cancel_deleted_org_subscription(
    control: &PgPool,
    stripe: Option<&StripeClient>,
    account_id: Uuid,
) -> Result<CancelOutcome, CancelError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;

    let Some((Some(subscription_id), status)) = row else {
        return Ok(CancelOutcome::NoSubscription);
    };
    if subscription_ended(&status) {
        return Ok(CancelOutcome::AlreadyEnded);
    }
    let Some(stripe) = stripe else {
        tracing::error!(%account_id, "cancel_deleted_org_subscription: stripe not configured but a subscription exists");
        return Err(CancelError::NotConfigured);
    };
    let outcome = end_deleted_org_subscription(stripe, &subscription_id).await?;
    if outcome == CancelOutcome::EndedNow {
        mirror_cancel_state(control, account_id, &subscription_id, false, None).await?;
        tracing::info!(%account_id, subscription_id, "subscription ended immediately (Clerk organization deleted)");
    }
    Ok(outcome)
}

#[cfg(test)]
mod pg_tests {
    use super::*;
    use crate::billing::test_support::*;
    use serde_json::json;

    async fn read_mirror(pool: &PgPool, account_id: Uuid) -> (bool, Option<DateTime<Utc>>) {
        sqlx::query_as("SELECT cancel_at_period_end, cancel_at FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn no_subscription_is_a_normal_outcome_and_needs_no_stripe() {
        let Some(pool) = pg_test_pool("cancel::no_subscription").await else { return };
        let (account, user) = seed_personal_account(&pool).await;
        let none = cancel_account_subscription(&pool, None, account).await;
        // customer-only row (no subscription id) is the same outcome
        seed_subscription(&pool, account, &format!("cus_{}", Uuid::new_v4().simple()), None, "active").await;
        let customer_only = cancel_account_subscription(&pool, None, account).await;
        cleanup_account(&pool, account, &[user]).await.unwrap();
        assert_eq!(none.unwrap(), CancelOutcome::NoSubscription);
        assert_eq!(customer_only.unwrap(), CancelOutcome::NoSubscription);
    }

    #[tokio::test]
    async fn schedules_at_period_end_mirrors_and_is_idempotent() {
        let Some(pool) = pg_test_pool("cancel::schedules").await else { return };
        let (account, user) = seed_personal_account(&pool).await;
        let sub = format!("sub_{}", Uuid::new_v4().simple());
        seed_subscription(&pool, account, &format!("cus_{}", Uuid::new_v4().simple()), Some(&sub), "active").await;
        let mock = MockStripe::spawn().await;
        let path = format!("/v1/subscriptions/{sub}");
        // First read: not cancelling. The POST reports it scheduled.
        mock.respond("GET", &path, 200, subscription_json(&sub, "active", &[("si_1", "price_pro", 1)], false, None, None));
        mock.respond("POST", &path, 200, subscription_json(&sub, "active", &[("si_1", "price_pro", 1)], true, Some(1_702_592_000), None));

        let first = cancel_account_subscription(&pool, Some(&*mock.client), account).await.unwrap();
        let mirrored = read_mirror(&pool, account).await;
        let posts_after_first = mock.calls("POST", &path);

        // Second call: Stripe now reports it scheduled, so no second write.
        mock.respond("GET", &path, 200, subscription_json(&sub, "active", &[("si_1", "price_pro", 1)], true, Some(1_702_592_000), None));
        let second = cancel_account_subscription(&pool, Some(&*mock.client), account).await.unwrap();
        let posts_after_second = mock.calls("POST", &path);
        cleanup_account(&pool, account, &[user]).await.unwrap();

        let at = DateTime::from_timestamp(1_702_592_000, 0);
        assert_eq!(first, CancelOutcome::Scheduled { cancel_at: at });
        assert_eq!(mirrored, (true, at));
        assert_eq!(posts_after_first.len(), 1);
        assert_eq!(posts_after_first[0].form.get("cancel_at_period_end").map(String::as_str), Some("true"));
        assert!(posts_after_first[0].idempotency_key.is_none(), "natural idempotency: no replay-cache key");
        assert_eq!(second, CancelOutcome::AlreadyScheduled { cancel_at: at });
        assert_eq!(posts_after_second.len(), 1, "no second Stripe write");
    }

    #[tokio::test]
    async fn an_ended_subscription_is_not_touched_and_missing_stripe_errors_loudly() {
        let Some(pool) = pg_test_pool("cancel::ended").await else { return };
        let (account, user) = seed_personal_account(&pool).await;
        let sub = format!("sub_{}", Uuid::new_v4().simple());
        seed_subscription(&pool, account, &format!("cus_{}", Uuid::new_v4().simple()), Some(&sub), "active").await;
        let no_stripe = cancel_account_subscription(&pool, None, account).await;
        let mock = MockStripe::spawn().await;
        mock.respond("GET", &format!("/v1/subscriptions/{sub}"), 200, subscription_json(&sub, "canceled", &[("si_1", "price_pro", 1)], false, None, None));
        let live_ended = cancel_account_subscription(&pool, Some(&*mock.client), account).await.unwrap();
        seed_subscription(&pool, account, &format!("cus_{}", Uuid::new_v4().simple()), Some(&sub), "canceled").await;
        let local_ended = cancel_account_subscription(&pool, None, account).await.unwrap();
        cleanup_account(&pool, account, &[user]).await.unwrap();
        assert!(matches!(no_stripe, Err(CancelError::NotConfigured)));
        assert_eq!(live_ended, CancelOutcome::AlreadyEnded);
        assert_eq!(local_ended, CancelOutcome::AlreadyEnded);
        assert!(mock.calls("POST", &format!("/v1/subscriptions/{sub}")).is_empty());
        let _ = json!(null);
    }

    /// #1396: a Clerk-deleted org's subscription is ended with ONE keyed
    /// `DELETE`, however often the event is delivered.
    #[tokio::test]
    async fn deleted_org_cancel_ends_the_subscription_once_across_redelivery() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let Some(pool) = pg_test_pool("cancel::deleted_org_once").await else { return };
        let (account, user) = seed_org_account(&pool, "owner").await;
        let sub = format!("sub_{}", Uuid::new_v4().simple());
        seed_subscription(&pool, account, &format!("cus_{}", Uuid::new_v4().simple()), Some(&sub), "active").await;
        let mock = MockStripe::spawn().await;
        let path = format!("/v1/subscriptions/{sub}");
        let canceled = Arc::new(AtomicBool::new(false));
        let (g, d) = (canceled.clone(), canceled.clone());
        let (gs, ds) = (sub.clone(), sub.clone());
        mock.respond_with("GET", &path, move |_| {
            let status = if g.load(Ordering::SeqCst) { "canceled" } else { "active" };
            (200, subscription_json(&gs, status, &[("si_1", "price_pro", 1)], false, None, None))
        });
        mock.respond_with("DELETE", &path, move |_| {
            d.store(true, Ordering::SeqCst);
            (200, subscription_json(&ds, "canceled", &[("si_1", "price_pro", 1)], false, None, None))
        });

        let first = cancel_deleted_org_subscription(&pool, Some(&*mock.client), account).await.unwrap();
        // Redelivery: the local mirror still says `active` (Stripe's
        // `customer.subscription.deleted` has not arrived yet).
        let second = cancel_deleted_org_subscription(&pool, Some(&*mock.client), account).await.unwrap();
        let deletes = mock.calls("DELETE", &path);
        cleanup_account(&pool, account, &[user]).await.unwrap();

        assert_eq!(first, CancelOutcome::EndedNow);
        assert_eq!(second, CancelOutcome::AlreadyEnded);
        assert_eq!(deletes.len(), 1, "exactly one Stripe cancel across two deliveries");
        assert_eq!(deletes[0].idempotency_key.as_deref(), Some(deleted_org_cancel_key(&sub).as_str()));
        assert_eq!(deletes[0].form.get("prorate").map(String::as_str), Some("false"));
    }

    /// #1396 / #1533: a subscription that already ended (the A2 sweep
    /// cancels before it deletes the Clerk org) is a no-op: no Stripe write,
    /// and no Stripe call at all when the mirror already says so.
    #[tokio::test]
    async fn deleted_org_cancel_is_a_noop_for_an_already_cancelled_subscription() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        let Some(pool) = pg_test_pool("cancel::deleted_org_noop").await else { return };
        let (account, user) = seed_org_account(&pool, "owner").await;
        let sub = format!("sub_{}", Uuid::new_v4().simple());
        let path = format!("/v1/subscriptions/{sub}");
        let customer = format!("cus_{}", Uuid::new_v4().simple());

        // Mirror says canceled: no Stripe client needed.
        seed_subscription(&pool, account, &customer, Some(&sub), "canceled").await;
        let local = cancel_deleted_org_subscription(&pool, None, account).await;

        // Mirror lags, Stripe says canceled: one read, no write.
        seed_subscription(&pool, account, &customer, Some(&sub), "active").await;
        let mock = MockStripe::spawn().await;
        mock.respond("GET", &path, 200, subscription_json(&sub, "canceled", &[("si_1", "price_pro", 1)], false, None, None));
        let live = cancel_deleted_org_subscription(&pool, Some(&*mock.client), account).await.unwrap();
        let deletes_live = mock.calls("DELETE", &path).len();

        // Another writer ends it between our read and our DELETE: Stripe
        // refuses the DELETE, the re-read shows it ended.
        let raced = MockStripe::spawn().await;
        let reads = Arc::new(AtomicU32::new(0));
        let (r, rs) = (reads.clone(), sub.clone());
        raced.respond_with("GET", &path, move |_| {
            let status = if r.fetch_add(1, Ordering::SeqCst) == 0 { "active" } else { "canceled" };
            (200, subscription_json(&rs, status, &[("si_1", "price_pro", 1)], false, None, None))
        });
        raced.respond("DELETE", &path, 400, json!({"error": {"type": "invalid_request_error", "message": "No such subscription"}}));
        let lost_race = cancel_deleted_org_subscription(&pool, Some(&*raced.client), account).await.unwrap();
        let mirror = read_mirror(&pool, account).await;
        cleanup_account(&pool, account, &[user]).await.unwrap();

        assert_eq!(local.unwrap(), CancelOutcome::AlreadyEnded);
        assert_eq!(live, CancelOutcome::AlreadyEnded);
        assert_eq!(deletes_live, 0, "an ended subscription is never written to");
        assert_eq!(lost_race, CancelOutcome::AlreadyEnded);
        assert_eq!(mirror, (false, None));
    }
}
