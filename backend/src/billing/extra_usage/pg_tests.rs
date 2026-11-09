//! Live-Postgres tests for crediting the extra-usage ledger: the writer, the
//! Checkout webhook for top-ups, and the credit-pack reroute.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL`; without it each test prints a skip
//! line (a vacuous pass), which CI's zero-match / `skipping ` greps turn into
//! a failure. The pool is the database owner, so RLS is bypassed, but the
//! 0321 financial entry fence is a trigger and fires for every role.

use sqlx::{types::Decimal, PgPool};
use uuid::Uuid;

use super::webhook::{apply_pack_purchase, apply_topup, PaymentFacts, PaymentOutcome};
use super::*;
use crate::billing::stripe_client::SessionLineItem;

const M: i64 = MICRO_PER_USD;

async fn pg(test: &str) -> Option<PgPool> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("skipping {test}: set RELINFER_TEST_DATABASE_URL to run it");
        return None;
    };
    Some(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .expect("connect to test Postgres"),
    )
}

async fn account(pool: &PgPool) -> Uuid {
    let user = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(user)
        .bind(format!("{user}@extra-usage.test"))
        .execute(pool)
        .await
        .expect("user");
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
        .bind(id)
        .bind(user)
        .execute(pool)
        .await
        .expect("account");
    id
}

async fn fenced(pool: &PgPool, account: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await.expect("scope");
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.expect("enter");
    tx
}

fn entry<'a>(key: &str, micro: i64) -> CreditEntry<'a> {
    CreditEntry {
        kind: CreditKind::Topup,
        micro_usd: micro,
        credits: Some(Decimal::from(micro) / Decimal::from(22_000)),
        rate_micro_usd_per_credit: 22_000,
        idempotency_key: key.to_string(),
        stripe_event_id: None,
        stripe_payment_intent_id: None,
        stripe_checkout_session_id: None,
        reload_id: None,
        note: None,
    }
}

async fn credit(pool: &PgPool, a: Uuid, e: &CreditEntry<'_>) -> Result<CreditOutcome, sqlx::Error> {
    let mut tx = fenced(pool, a).await;
    let out = credit_in_tx(&mut tx, a, e).await?;
    tx.commit().await?;
    Ok(out)
}

async fn balance(pool: &PgPool, a: Uuid) -> Option<i64> {
    sqlx::query_scalar("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1")
        .bind(a)
        .fetch_optional(pool)
        .await
        .expect("balance")
}

async fn ledger_rows(pool: &PgPool, a: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM extra_usage_ledger WHERE account_id=$1")
        .bind(a)
        .fetch_one(pool)
        .await
        .expect("rows")
}

async fn pool_balance(pool: &PgPool, a: Uuid) -> Option<Decimal> {
    sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id=$1")
        .bind(a)
        .fetch_optional(pool)
        .await
        .expect("credit_balances")
}

async fn event_outcome(pool: &PgPool, event: &str) -> Option<String> {
    sqlx::query_scalar("SELECT outcome FROM stripe_webhook_events WHERE event_id=$1")
        .bind(event)
        .fetch_optional(pool)
        .await
        .expect("event")
}

fn evt() -> String {
    format!("evt_{}", Uuid::new_v4().simple())
}

fn cs() -> String {
    format!("cs_{}", Uuid::new_v4().simple())
}

fn paid(a: Uuid, cents: i64) -> PaymentFacts {
    PaymentFacts {
        payment_intent: Some(format!("pi_{}", Uuid::new_v4().simple())),
        amount_total_cents: Some(cents),
        amount_subtotal_cents: Some(cents),
        currency: Some("usd".into()),
        payment_status: Some("paid".into()),
        purpose: Some(TOPUP_PURPOSE.into()),
        metadata_account_id: Some(a.to_string()),
    }
}

const CE: &str = "checkout.session.completed";

/// Test ids must be unique per run: the database persists between runs and the
/// ledger's Stripe ids are globally unique.
fn uniq(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

// ── credit_in_tx ───────────────────────────────────────────────────────────

#[tokio::test]
async fn credit_writes_the_ledger_row_and_the_balance_together() {
    let Some(pool) = pg("credit_writes_the_ledger_row_and_the_balance_together").await else { return };
    let a = account(&pool).await;
    assert_eq!(balance(&pool, a).await, None, "a missing balance row reads as zero");

    let (k1, k2, ev1, ev2) = (uniq("k1"), uniq("k2"), uniq("evt"), uniq("evt"));
    let mut e1 = entry(&k1, 25 * M);
    e1.stripe_event_id = Some(&ev1);
    assert_eq!(credit(&pool, a, &e1).await.unwrap(), CreditOutcome::Credited { balance_after_micro_usd: 25 * M });
    let mut e2 = entry(&k2, 10 * M);
    e2.stripe_event_id = Some(&ev2);
    assert_eq!(credit(&pool, a, &e2).await.unwrap(), CreditOutcome::Credited {
        balance_after_micro_usd: 35 * M
    });
    assert_eq!(balance(&pool, a).await, Some(35 * M));
    let sum: i64 = sqlx::query_scalar("SELECT sum(amount_micro_usd)::bigint FROM extra_usage_ledger WHERE account_id=$1")
        .bind(a)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sum, 35 * M, "the balance always equals the ledger sum");
    let (kind, rate): (String, Option<i64>) =
        sqlx::query_as("SELECT entry_kind, rate_micro_usd_per_credit FROM extra_usage_ledger WHERE idempotency_key=$1")
            .bind(&k1)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((kind.as_str(), rate), ("topup", Some(22_000)));
}

#[tokio::test]
async fn each_idempotency_key_makes_a_second_credit_a_noop() {
    let Some(pool) = pg("each_idempotency_key_makes_a_second_credit_a_noop").await else { return };
    let a = account(&pool).await;
    let (key, ev, pi, sess) = (uniq("dup"), uniq("evt"), uniq("pi"), uniq("cs"));
    let mut first = entry(&key, 20 * M);
    first.stripe_event_id = Some(&ev);
    first.stripe_payment_intent_id = Some(&pi);
    first.stripe_checkout_session_id = Some(&sess);
    assert!(matches!(credit(&pool, a, &first).await.unwrap(), CreditOutcome::Credited { .. }));

    let other_ev = uniq("evt");
    // Same idempotency key.
    let mut e = entry(&key, 20 * M);
    e.stripe_event_id = Some(&other_ev);
    assert_eq!(credit(&pool, a, &e).await.unwrap(), CreditOutcome::Duplicate);
    // Same checkout session, new everything else.
    let mut e = entry("k-session", 20 * M);
    e.stripe_checkout_session_id = Some(&sess);
    assert_eq!(credit(&pool, a, &e).await.unwrap(), CreditOutcome::Duplicate);
    // Same payment intent.
    let mut e = entry("k-pi", 20 * M);
    e.stripe_payment_intent_id = Some(&pi);
    assert_eq!(credit(&pool, a, &e).await.unwrap(), CreditOutcome::Duplicate);
    // Same Stripe event.
    let mut e = entry("k-evt", 20 * M);
    e.stripe_event_id = Some(&ev);
    assert_eq!(credit(&pool, a, &e).await.unwrap(), CreditOutcome::Duplicate);

    assert_eq!(balance(&pool, a).await, Some(20 * M), "only the first credit counted");
    assert_eq!(ledger_rows(&pool, a).await, 1);
}

#[tokio::test]
async fn a_credit_without_a_stripe_reference_is_refused_by_the_schema() {
    let Some(pool) = pg("a_credit_without_a_stripe_reference_is_refused_by_the_schema").await else { return };
    let a = account(&pool).await;
    // Money in must be traceable to Stripe (ledger CHECK); a bare entry has none.
    let mut tx = fenced(&pool, a).await;
    let bare = entry("bare", M);
    let res = credit_in_tx(&mut tx, a, &bare).await;
    assert!(res.is_err(), "{res:?}");
    drop(tx);
    assert_eq!(balance(&pool, a).await, None);
}

#[tokio::test]
async fn a_credit_outside_the_entry_fence_is_refused() {
    let Some(pool) = pg("a_credit_outside_the_entry_fence_is_refused").await else { return };
    let a = account(&pool).await;
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a).await.expect("scope");
    let mut e = entry("unfenced", M);
    e.stripe_event_id = Some("evt_unfenced");
    let res = credit_in_tx(&mut tx, a, &e).await;
    assert!(res.is_err(), "no billing_enter_account, no write: {res:?}");
    drop(tx);
    assert_eq!(balance(&pool, a).await, None);
}

#[tokio::test]
async fn a_non_positive_credit_is_refused_before_touching_the_database() {
    let Some(pool) = pg("a_non_positive_credit_is_refused_before_touching_the_database").await else { return };
    let a = account(&pool).await;
    for bad in [0, -M] {
        let mut e = entry("neg", bad);
        e.stripe_event_id = Some("evt_neg");
        assert!(credit(&pool, a, &e).await.is_err(), "{bad}");
    }
    assert_eq!(balance(&pool, a).await, None);
}

#[tokio::test]
async fn the_rate_in_force_is_the_seeded_22000() {
    let Some(pool) = pg("the_rate_in_force_is_the_seeded_22000").await else { return };
    let mut conn = pool.acquire().await.unwrap();
    assert_eq!(rate_in_force(&mut conn).await.unwrap(), 22_000);
}

// ── top-up webhook ─────────────────────────────────────────────────────────

#[tokio::test]
async fn a_paid_topup_credits_what_stripe_says_was_paid() {
    let Some(pool) = pg("a_paid_topup_credits_what_stripe_says_was_paid").await else { return };
    let a = account(&pool).await;
    let (event, session) = (evt(), cs());
    let facts = paid(a, 2550);
    let out = apply_topup(&pool, a, &event, CE, &session, &facts).await.unwrap();
    assert_eq!(out, PaymentOutcome::Credited { micro_usd: 25_500_000 });
    assert_eq!(balance(&pool, a).await, Some(25_500_000));

    let (kind, amount, session_id, pi, ev, rate, credits): (
        String, i64, Option<String>, Option<String>, Option<String>, Option<i64>, Option<Decimal>,
    ) = sqlx::query_as(
        "SELECT entry_kind, amount_micro_usd, stripe_checkout_session_id, stripe_payment_intent_id, \
                stripe_event_id, rate_micro_usd_per_credit, credits \
           FROM extra_usage_ledger WHERE account_id=$1",
    )
    .bind(a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kind, "topup");
    assert_eq!(amount, 25_500_000);
    assert_eq!(session_id.as_deref(), Some(session.as_str()));
    assert_eq!(pi, facts.payment_intent);
    assert_eq!(ev.as_deref(), Some(event.as_str()));
    assert_eq!(rate, Some(22_000));
    assert_eq!(credits.unwrap().round_dp(4), (Decimal::from(25_500_000) / Decimal::from(22_000)).round_dp(4));
    assert_eq!(pool_balance(&pool, a).await, None, "a top-up never touches the plan pool");
    assert!(event_outcome(&pool, &event).await.unwrap().contains("topup"));
}

#[tokio::test]
async fn a_redelivered_topup_event_is_a_replay() {
    let Some(pool) = pg("a_redelivered_topup_event_is_a_replay").await else { return };
    let a = account(&pool).await;
    let (event, session) = (evt(), cs());
    let facts = paid(a, 1000);
    assert!(matches!(apply_topup(&pool, a, &event, CE, &session, &facts).await.unwrap(), PaymentOutcome::Credited { .. }));
    for _ in 0..3 {
        assert_eq!(apply_topup(&pool, a, &event, CE, &session, &facts).await.unwrap(), PaymentOutcome::Replay);
    }
    assert_eq!(balance(&pool, a).await, Some(10 * M));
    assert_eq!(ledger_rows(&pool, a).await, 1);
}

#[tokio::test]
async fn a_second_event_for_the_same_session_does_not_credit_again() {
    let Some(pool) = pg("a_second_event_for_the_same_session_does_not_credit_again").await else { return };
    let a = account(&pool).await;
    let session = cs();
    let facts = paid(a, 1000);
    assert!(matches!(
        apply_topup(&pool, a, &evt(), CE, &session, &facts).await.unwrap(),
        PaymentOutcome::Credited { .. }
    ));
    // A dashboard resend mints a new event id for the same session.
    let second = evt();
    assert_eq!(
        apply_topup(&pool, a, &second, CE, &session, &facts).await.unwrap(),
        PaymentOutcome::DuplicateSession
    );
    assert_eq!(balance(&pool, a).await, Some(10 * M));
    assert!(event_outcome(&pool, &second).await.unwrap().contains("duplicate_session"));
}

#[tokio::test]
async fn the_same_payment_under_a_different_session_id_does_not_credit_again() {
    let Some(pool) = pg("the_same_payment_under_a_different_session_id_does_not_credit_again").await else { return };
    let a = account(&pool).await;
    let facts = paid(a, 1000);
    assert!(matches!(
        apply_topup(&pool, a, &evt(), CE, &cs(), &facts).await.unwrap(),
        PaymentOutcome::Credited { .. }
    ));
    assert_eq!(
        apply_topup(&pool, a, &evt(), CE, &cs(), &facts).await.unwrap(),
        PaymentOutcome::DuplicateSession,
        "the payment intent is unique too"
    );
    assert_eq!(balance(&pool, a).await, Some(10 * M));
}

#[tokio::test]
async fn concurrent_deliveries_for_one_session_credit_exactly_once() {
    let Some(pool) = pg("concurrent_deliveries_for_one_session_credit_exactly_once").await else { return };
    let a = account(&pool).await;
    let session = cs();
    let facts = paid(a, 1000);
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let (pool, session, facts) = (pool.clone(), session.clone(), facts.clone());
        tasks.push(tokio::spawn(async move {
            apply_topup(&pool, a, &evt(), CE, &session, &facts).await.unwrap()
        }));
    }
    let mut credited = 0;
    for t in tasks {
        if matches!(t.await.unwrap(), PaymentOutcome::Credited { .. }) {
            credited += 1;
        }
    }
    assert_eq!(credited, 1);
    assert_eq!(balance(&pool, a).await, Some(10 * M));
    assert_eq!(ledger_rows(&pool, a).await, 1);
}

#[tokio::test]
async fn a_topup_is_credited_whatever_state_the_account_is_in() {
    // Out of order: the webhook can arrive before, or long after, any other
    // billing event. The money was paid, so it is credited; whether it can be
    // SPENT is the funding policy's call (frozen / off / Free all refuse).
    let Some(pool) = pg("a_topup_is_credited_whatever_state_the_account_is_in").await else { return };
    let a = account(&pool).await; // no grant, no settings, no subscription
    assert!(matches!(
        apply_topup(&pool, a, &evt(), CE, &cs(), &paid(a, 500)).await.unwrap(),
        PaymentOutcome::Credited { .. }
    ));
    {
        let mut tx = fenced(&pool, a).await;
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, status) VALUES ($1,$2,'canceled')",
        )
        .bind(a)
        .bind(format!("cus_{}", Uuid::new_v4().simple()))
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    assert!(matches!(
        apply_topup(&pool, a, &evt(), CE, &cs(), &paid(a, 500)).await.unwrap(),
        PaymentOutcome::Credited { .. }
    ));
    assert_eq!(balance(&pool, a).await, Some(10 * M));
}

/// Staging Stripe test-mode run: a $5.00 top-up that Stripe totalled at $5.44
/// (tax) credited $5.44. Tax is not a purchased balance: credit the subtotal.
#[tokio::test]
async fn a_taxed_topup_credits_the_pre_tax_subtotal_not_the_total() {
    let Some(pool) = pg("a_taxed_topup_credits_the_pre_tax_subtotal_not_the_total").await else { return };
    let a = account(&pool).await;
    let facts = PaymentFacts { amount_total_cents: Some(544), amount_subtotal_cents: Some(500), ..paid(a, 500) };
    let out = apply_topup(&pool, a, &evt(), CE, &cs(), &facts).await.unwrap();
    assert_eq!(out, PaymentOutcome::Credited { micro_usd: 5 * M });
    assert_eq!(balance(&pool, a).await, Some(5 * M));
}

/// Same for a pack: tax on top of list price must not raise the credit, and a
/// discounted subtotal still credits only what was paid pre-tax.
#[tokio::test]
async fn a_taxed_pack_credits_the_pre_tax_subtotal_not_the_total() {
    let Some(pool) = pg("a_taxed_pack_credits_the_pre_tax_subtotal_not_the_total").await else { return };
    let _guard = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
    let a = account(&pool).await;
    set_pack_price(&pool, "pack_500", "price_pack_500_tax34").await;
    let facts = PaymentFacts { amount_total_cents: Some(1198), amount_subtotal_cents: Some(800), ..paid_pack(800) };
    let out = apply_pack_purchase(&pool, a, &evt(), CE, &cs(), &facts, &[line("price_pack_500_tax34", 1)])
        .await
        .unwrap();
    assert_eq!(out, PaymentOutcome::Credited { micro_usd: 8 * M });
    assert_eq!(balance(&pool, a).await, Some(8 * M));
}

#[tokio::test]
async fn an_unpaid_or_malformed_topup_is_recorded_not_credited() {
    let Some(pool) = pg("an_unpaid_or_malformed_topup_is_recorded_not_credited").await else { return };
    let a = account(&pool).await;
    let other = Uuid::new_v4();
    let cases: Vec<(&str, PaymentFacts)> = vec![
        ("unpaid", PaymentFacts { payment_status: Some("unpaid".into()), ..paid(a, 1000) }),
        ("missing status", PaymentFacts { payment_status: None, ..paid(a, 1000) }),
        ("wrong account", PaymentFacts { metadata_account_id: Some(other.to_string()), ..paid(a, 1000) }),
        ("no account", PaymentFacts { metadata_account_id: None, ..paid(a, 1000) }),
        ("eur", PaymentFacts { currency: Some("eur".into()), ..paid(a, 1000) }),
        ("zero", paid(a, 0)),
        ("negative", paid(a, -100)),
        ("no amount", PaymentFacts { amount_subtotal_cents: None, ..paid(a, 1000) }),
        // A total with no pre-tax subtotal must not fall back to the taxed total.
        ("total but no subtotal", PaymentFacts { amount_subtotal_cents: None, amount_total_cents: Some(1000), ..paid(a, 1000) }),
    ];
    for (name, facts) in cases {
        let event = evt();
        let out = apply_topup(&pool, a, &event, CE, &cs(), &facts).await.unwrap();
        assert!(matches!(out, PaymentOutcome::Unprocessable(_)), "{name}: {out:?}");
        assert!(
            event_outcome(&pool, &event).await.unwrap().starts_with("unprocessable:"),
            "{name}: the claim is committed with the reason, so Stripe is not retried forever"
        );
    }
    assert_eq!(balance(&pool, a).await, None);
    assert_eq!(ledger_rows(&pool, a).await, 0);
}

#[tokio::test]
async fn a_failure_after_the_claim_rolls_the_claim_back_so_redelivery_can_retry() {
    let Some(pool) = pg("a_failure_after_the_claim_rolls_the_claim_back_so_redelivery_can_retry").await else {
        return;
    };
    let ghost = Uuid::new_v4(); // no accounts row: the ledger FK fails after the claim
    let event = evt();
    let res = apply_topup(&pool, ghost, &event, CE, &cs(), &paid(ghost, 1000)).await;
    assert!(res.is_err(), "{res:?}");
    assert_eq!(event_outcome(&pool, &event).await, None, "no claim may survive a failed credit");
}

// ── credit packs: rerouted to the extra ledger ─────────────────────────────

async fn set_pack_price(pool: &PgPool, code: &str, price: &str) {
    sqlx::query("UPDATE credit_packs SET stripe_price_id=$2 WHERE code=$1")
        .bind(code)
        .bind(price)
        .execute(pool)
        .await
        .expect("pack price");
}

/// A fully paid pack session as the re-fetched Checkout session reports it.
fn paid_pack(cents: i64) -> PaymentFacts {
    PaymentFacts {
        amount_total_cents: Some(cents),
        amount_subtotal_cents: Some(cents),
        currency: Some("usd".into()),
        payment_status: Some("paid".into()),
        ..Default::default()
    }
}

fn line(price: &str, quantity: i64) -> SessionLineItem {
    SessionLineItem { price_id: price.to_string(), quantity }
}

#[tokio::test]
async fn a_pack_purchase_feeds_the_extra_ledger_not_the_plan_pool() {
    let Some(pool) = pg("a_pack_purchase_feeds_the_extra_ledger_not_the_plan_pool").await else { return };
    let _guard = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
    let a = account(&pool).await;
    set_pack_price(&pool, "pack_500", "price_pack_500_x34").await;
    set_pack_price(&pool, "pack_2000", "price_pack_2000_x34").await;
    let (event, session) = (evt(), cs());
    let lines = [line("price_pack_500_x34", 1), line("price_pack_2000_x34", 2)];

    let out = apply_pack_purchase(&pool, a, &event, CE, &session, &paid_pack(9900), &lines)
        .await
        .unwrap();
    // $11 + 2 x $44 = $99 at list price.
    assert_eq!(out, PaymentOutcome::Credited { micro_usd: 99 * M });
    assert_eq!(balance(&pool, a).await, Some(99 * M));
    assert_eq!(pool_balance(&pool, a).await, None, "credit_balances is no longer fed by packs");
    let legacy: i64 = sqlx::query_scalar("SELECT count(*) FROM credit_ledger WHERE account_id=$1")
        .bind(a)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(legacy, 0);
    let (kind, credits): (String, Option<Decimal>) = sqlx::query_as(
        "SELECT entry_kind, credits FROM extra_usage_ledger WHERE account_id=$1",
    )
    .bind(a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kind, "pack_purchase");
    assert_eq!(credits, Some(Decimal::from(4500)), "500 + 2 x 2000 credits");

    // Replay and a second event for the same session are no-ops.
    assert_eq!(
        apply_pack_purchase(&pool, a, &event, CE, &session, &paid_pack(9900), &lines).await.unwrap(),
        PaymentOutcome::Replay
    );
    assert_eq!(
        apply_pack_purchase(&pool, a, &evt(), CE, &session, &paid_pack(9900), &lines).await.unwrap(),
        PaymentOutcome::DuplicateSession
    );
    assert_eq!(balance(&pool, a).await, Some(99 * M));
}

#[tokio::test]
async fn a_pack_never_credits_more_than_was_paid() {
    let Some(pool) = pg("a_pack_never_credits_more_than_was_paid").await else { return };
    let _guard = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
    let a = account(&pool).await;
    set_pack_price(&pool, "pack_500", "price_pack_500_y34").await;
    // Stripe says $8 was paid for an $11 pack (a discount): credit $8.
    let facts = paid_pack(800);
    let out = apply_pack_purchase(&pool, a, &evt(), CE, &cs(), &facts, &[line("price_pack_500_y34", 1)])
        .await
        .unwrap();
    assert_eq!(out, PaymentOutcome::Credited { micro_usd: 8 * M });
    // Stripe says MORE than list was paid: credit list, not more than the pack is worth.
    let b = account(&pool).await;
    let facts = paid_pack(5000);
    let out = apply_pack_purchase(&pool, b, &evt(), CE, &cs(), &facts, &[line("price_pack_500_y34", 1)])
        .await
        .unwrap();
    assert_eq!(out, PaymentOutcome::Credited { micro_usd: 11 * M });
}

#[tokio::test]
async fn an_unrecognized_pack_price_credits_nothing_and_is_recorded() {
    let Some(pool) = pg("an_unrecognized_pack_price_credits_nothing_and_is_recorded").await else { return };
    let _guard = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
    let a = account(&pool).await;
    set_pack_price(&pool, "pack_500", "price_pack_500_z34").await;
    let event = evt();
    let out = apply_pack_purchase(
        &pool,
        a,
        &event,
        CE,
        &cs(),
        &paid_pack(100_000),
        &[line("price_pack_500_z34", 1), line("price_nobody_knows", 1)],
    )
    .await
    .unwrap();
    assert!(matches!(out, PaymentOutcome::Unprocessable(_)), "{out:?}");
    assert_eq!(balance(&pool, a).await, None, "no partial credit");
    assert!(event_outcome(&pool, &event).await.unwrap().starts_with("unprocessable:"));
}

#[tokio::test]
async fn a_session_already_credited_under_the_old_scheme_is_not_credited_again() {
    // Old webhook payloads: a session whose purchase the previous code already
    // credited to the plan pool must not ALSO credit the extra ledger when
    // Stripe redelivers it under a new event id.
    let Some(pool) = pg("a_session_already_credited_under_the_old_scheme_is_not_credited_again").await else {
        return;
    };
    let _guard = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
    let a = account(&pool).await;
    set_pack_price(&pool, "pack_500", "price_pack_500_w34").await;
    let session = cs();
    {
        let mut tx = fenced(&pool, a).await;
        sqlx::query(
            "INSERT INTO credit_ledger (account_id, entry_kind, credits, stripe_checkout_session_id) \
             VALUES ($1,'credit_pack_purchase',500,$2)",
        )
        .bind(a)
        .bind(&session)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let out = apply_pack_purchase(
        &pool, a, &evt(), CE, &session, &paid_pack(1100), &[line("price_pack_500_w34", 1)],
    )
    .await
    .unwrap();
    assert_eq!(out, PaymentOutcome::DuplicateSession);
    assert_eq!(balance(&pool, a).await, None);
}

const ASYNC_OK: &str = "checkout.session.async_payment_succeeded";

#[tokio::test]
async fn an_unpaid_pack_is_not_credited_until_the_async_payment_succeeds() {
    let Some(pool) = pg("an_unpaid_pack_is_not_credited_until_the_async_payment_succeeds").await else { return };
    let _guard = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
    let a = account(&pool).await;
    set_pack_price(&pool, "pack_500", "price_pack_500_u34").await;
    let (session, lines) = (cs(), [line("price_pack_500_u34", 1)]);

    // Completed but the delayed method has not settled: recorded, not credited.
    let unpaid = PaymentFacts { payment_status: Some("unpaid".into()), ..paid_pack(1100) };
    let (e1, e2) = (evt(), evt());
    let out = apply_pack_purchase(&pool, a, &e1, CE, &session, &unpaid, &lines).await.unwrap();
    assert!(matches!(out, PaymentOutcome::Unprocessable(_)), "{out:?}");
    assert_eq!(balance(&pool, a).await, None, "unpaid session must not credit");
    assert!(event_outcome(&pool, &e1).await.unwrap().starts_with("unprocessable:"));

    // The async success event (new event id, same session) credits once.
    let out = apply_pack_purchase(&pool, a, &e2, ASYNC_OK, &session, &paid_pack(1100), &lines).await.unwrap();
    assert_eq!(out, PaymentOutcome::Credited { micro_usd: 11 * M });
    assert_eq!(balance(&pool, a).await, Some(11 * M));

    // A resend of the success under yet another event id credits nothing more.
    let out = apply_pack_purchase(&pool, a, &evt(), ASYNC_OK, &session, &paid_pack(1100), &lines).await.unwrap();
    assert_eq!(out, PaymentOutcome::DuplicateSession);
    assert_eq!(balance(&pool, a).await, Some(11 * M));
}

#[tokio::test]
async fn a_pack_session_without_an_amount_is_unprocessable_not_list_price() {
    let Some(pool) = pg("a_pack_session_without_an_amount_is_unprocessable_not_list_price").await else { return };
    let _guard = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
    let a = account(&pool).await;
    set_pack_price(&pool, "pack_500", "price_pack_500_v34").await;
    let facts = PaymentFacts { amount_subtotal_cents: None, ..paid_pack(0) };
    let event = evt();
    let out = apply_pack_purchase(&pool, a, &event, CE, &cs(), &facts, &[line("price_pack_500_v34", 1)])
        .await
        .unwrap();
    assert!(matches!(out, PaymentOutcome::Unprocessable(_)), "{out:?}");
    assert_eq!(balance(&pool, a).await, None);
    assert!(event_outcome(&pool, &event).await.unwrap().contains("no amount_subtotal"));
}
