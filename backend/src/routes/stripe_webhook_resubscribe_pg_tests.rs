//! The resubscribe seat-ledger bug (self-serve PR C′, from the triage of
//! #1389): a team org whose subscription was deleted and who subscribes
//! again kept its OLD purchased seat count while Stripe billed the new
//! quantity, because the surviving team grant made issuance report
//! `AlreadyGranted` and nothing else was recorded. Runs as `coralswarm_app`.
//!
//! A child module of `routes::stripe_webhook` (attached with `#[path]`), so
//! it drives the same private handlers the endpoint runs.

use super::*;
use crate::pg_catalog_test_support::{set_seat_tier_price, CATALOG_PRICE_LOCK};
use sqlx::PgPool;

async fn pools(test: &str) -> Option<(PgPool, PgPool)> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "{test}: REQUIRE_PG_SUITE is set but RELINFER_TEST_DATABASE_URL is not"
        );
        eprintln!("skipping {test}: set RELINFER_TEST_DATABASE_URL to run it");
        return None;
    };
    let owner = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await.expect("connect");
    let app = crate::app_role_test_support::app_role_pool(&owner, &url).await;
    Some((owner, app))
}

async fn fin(pool: &PgPool, account: Uuid, q: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>) {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await.expect("scope");
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.expect("lock");
    q.execute(&mut *tx).await.expect("fenced write");
    tx.commit().await.expect("commit");
}

/// A team org (owner only), linked to a Stripe customer, no grant yet.
async fn seed(owner: &PgPool) -> (Uuid, Uuid) {
    let user = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(user)
        .bind(format!("resub-{}@test.example", user.simple()))
        .execute(owner)
        .await
        .unwrap();
    let account = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
        .bind(account)
        .bind(user)
        .bind(format!("org_{}", account.simple()))
        .execute(owner)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         SELECT $1, $2, 'owner', id FROM seat_tiers WHERE code = 'standard' AND is_active",
    )
    .bind(account)
    .bind(user)
    .execute(owner)
    .await
    .unwrap();
    fin(
        owner,
        account,
        sqlx::query("INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, status) VALUES ($1, $2, 'pending')")
            .bind(account)
            .bind(format!("cus_resub_{}", account.simple())),
    )
    .await;
    (account, user)
}

async fn stripe_for(price: &str, quantity: i64, start: i64) -> Arc<crate::billing::stripe_client::StripeClient> {
    let body = json!({
        "id": "sub_resub_mock", "object": "subscription", "status": "active",
        "items": {"data": [{"id": "si_resub", "quantity": quantity, "current_period_start": start,
                            "current_period_end": start + 2_592_000, "price": {"id": price}}]},
    });
    let app = axum::Router::new().fallback(axum::routing::any(move |req: axum::http::Request<axum::body::Body>| {
        let body = body.clone();
        async move {
            let reply = if req.uri().path().starts_with("/v1/subscriptions/") { body } else { json!({}) };
            (StatusCode::OK, [("content-type", "application/json")], reply.to_string())
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    Arc::new(
        crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}")),
    )
}

async fn checkout(app: &PgPool, account: Uuid, price: &str, sub: &str, quantity: i64, start: i64) -> StatusCode {
    let mut state = crate::test_app_state_owned_with_db(app.clone(), None);
    state.stripe = Some(stripe_for(price, quantity, start).await);
    apply_checkout_completed_subscription(
        &Arc::new(state),
        app,
        account,
        &format!("evt_{}", Uuid::new_v4().simple()),
        &format!("cs_{}", Uuid::new_v4().simple()),
        sub,
        &crate::billing::stripe_client::SessionLineItem { price_id: price.to_string(), quantity },
    )
    .await
    .status()
}

/// `(purchased seats in the ledger, grants.seats, members holding a seat)`.
async fn ledger(owner: &PgPool, account: Uuid) -> (i64, Option<i32>, i64) {
    let purchased: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(gst.seats), 0)::bigint FROM grant_seat_tiers gst \
           JOIN grants g ON g.id = gst.grant_id WHERE g.account_id = $1 AND g.revoked_at IS NULL",
    )
    .bind(account)
    .fetch_one(owner)
    .await
    .unwrap();
    let seats: Option<i32> = sqlx::query_scalar(
        "SELECT g.seats FROM grants g JOIN plans p ON p.id = g.plan_id \
          WHERE g.account_id = $1 AND p.code = 'team' AND g.revoked_at IS NULL",
    )
    .bind(account)
    .fetch_one(owner)
    .await
    .unwrap();
    let stamped: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND seat_purchased_at IS NOT NULL")
            .bind(account)
            .fetch_one(owner)
            .await
            .unwrap();
    (purchased, seats, stamped)
}

async fn cleanup(owner: &PgPool, account: Uuid, user: Uuid) {
    fin(owner, account, sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account)).await;
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user).execute(owner).await.unwrap();
}

/// Buy 3 seats, the subscription is deleted, subscribe again for 5: the
/// ledger is 5 (what Stripe now bills), with the purchaser holding one.
#[tokio::test]
async fn a_new_subscription_after_a_deleted_one_records_its_own_seat_quantity() {
    const NAME: &str = "a_new_subscription_after_a_deleted_one_records_its_own_seat_quantity";
    let Some((owner, app)) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let price = format!("price_std_resub_{}", Uuid::new_v4().simple());
    let previous = set_seat_tier_price(&owner, "standard", Some(&price)).await;
    let (account, user) = seed(&owner).await;
    let sub_a = format!("sub_resub_a_{}", Uuid::new_v4().simple());
    let sub_b = format!("sub_resub_b_{}", Uuid::new_v4().simple());

    let bought_three = checkout(&app, account, &price, &sub_a, 3, 1_700_000_000).await;
    let after_three = ledger(&owner, account).await;
    apply_subscription_deleted(
        &app,
        account,
        &format!("evt_{}", Uuid::new_v4().simple()),
        "customer.subscription.deleted",
        &sub_a,
        Some("payment_failed"),
    )
    .await
    .expect("delete");
    let bought_five = checkout(&app, account, &price, &sub_b, 5, 1_705_000_000).await;
    let after_five = ledger(&owner, account).await;
    // A second, distinct event for the same new subscription: no re-replace.
    let again = checkout(&app, account, &price, &sub_b, 5, 1_705_000_000).await;
    let after_again = ledger(&owner, account).await;
    cleanup(&owner, account, user).await;
    set_seat_tier_price(&owner, "standard", previous.as_deref()).await;

    assert_eq!((bought_three, bought_five, again), (StatusCode::OK, StatusCode::OK, StatusCode::OK));
    assert_eq!(after_three, (3, Some(3), 1), "the first checkout records its own quantity");
    assert_eq!(after_five, (5, Some(5), 1), "the resubscription replaces the stale ledger");
    assert_eq!(after_again, (5, Some(5), 1), "a second event for the live subscription changes nothing");
}

/// A completion for a different subscription while the linked one is still
/// live (a double Checkout) must not replace the live ledger.
#[tokio::test]
async fn a_second_subscription_while_the_first_is_live_does_not_replace_the_ledger() {
    const NAME: &str = "a_second_subscription_while_the_first_is_live_does_not_replace_the_ledger";
    let Some((owner, app)) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let price = format!("price_std_resub_{}", Uuid::new_v4().simple());
    let previous = set_seat_tier_price(&owner, "standard", Some(&price)).await;
    let (account, user) = seed(&owner).await;
    let sub_a = format!("sub_resub_a_{}", Uuid::new_v4().simple());
    checkout(&app, account, &price, &sub_a, 3, 1_700_000_000).await;
    let other_sub = format!("sub_resub_x_{}", Uuid::new_v4().simple());
    let other = checkout(&app, account, &price, &other_sub, 7, 1_700_000_000).await;
    let after = ledger(&owner, account).await;
    cleanup(&owner, account, user).await;
    set_seat_tier_price(&owner, "standard", previous.as_deref()).await;

    assert_eq!(other, StatusCode::OK);
    assert_eq!(after, (3, Some(3), 1));
}

/// Review nit: resubscribing for FEWER seats than members currently hold a
/// slot must not leave more members seated than were bought
/// (`slot_pool`: used <= purchased). The owner keeps theirs, then the
/// longest-seated; the rest are released the way `release_seat` releases
/// one (stamp cleared), to re-acquire through `acquire_seat`.
#[tokio::test]
async fn a_resubscribe_for_fewer_seats_releases_the_overflow() {
    const NAME: &str = "a_resubscribe_for_fewer_seats_releases_the_overflow";
    let Some((owner, app)) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let price = format!("price_std_resub_{}", Uuid::new_v4().simple());
    let previous = set_seat_tier_price(&owner, "standard", Some(&price)).await;
    let (account, user) = seed(&owner).await;
    let mut members = Vec::new();
    for _ in 0..3 {
        let m = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(m)
            .bind(format!("resub-{}@test.example", m.simple()))
            .execute(&owner)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             SELECT $1, $2, 'member', id FROM seat_tiers WHERE code = 'standard' AND is_active",
        )
        .bind(account)
        .bind(m)
        .execute(&owner)
        .await
        .unwrap();
        members.push(m);
    }
    let sub_a = format!("sub_resub_a_{}", Uuid::new_v4().simple());
    let sub_b = format!("sub_resub_b_{}", Uuid::new_v4().simple());
    checkout(&app, account, &price, &sub_a, 4, 1_700_000_000).await;
    // Every member took a slot while the first subscription was live, the
    // owner first, then members[0], [1], [2] in that order.
    for (i, m) in members.iter().enumerate() {
        fin(
            &owner,
            account,
            sqlx::query(
                "UPDATE account_members SET seat_purchased_at = now() + make_interval(secs => $3) \
                  WHERE account_id = $1 AND user_id = $2",
            )
            .bind(account)
            .bind(m)
            .bind((i + 1) as f64),
        )
        .await;
    }
    let before = ledger(&owner, account).await;
    apply_subscription_deleted(
        &app,
        account,
        &format!("evt_{}", Uuid::new_v4().simple()),
        "customer.subscription.deleted",
        &sub_a,
        Some("cancellation_requested"),
    )
    .await
    .expect("delete");
    let status = checkout(&app, account, &price, &sub_b, 2, 1_705_000_000).await;
    let after = ledger(&owner, account).await;
    let seated: Vec<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM account_members WHERE account_id = $1 AND seat_purchased_at IS NOT NULL ORDER BY user_id",
    )
    .bind(account)
    .fetch_all(&owner)
    .await
    .unwrap();
    cleanup(&owner, account, user).await;
    for m in &members {
        sqlx::query("DELETE FROM users WHERE id = $1").bind(m).execute(&owner).await.unwrap();
    }
    set_seat_tier_price(&owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(before, (4, Some(4), 4));
    assert_eq!(after, (2, Some(2), 2), "no more members seated than bought");
    let mut expected = vec![user, members[0]];
    expected.sort();
    assert_eq!(seated, expected, "the owner, then the longest-seated member, keep their slots");
}

/// Review round 2, finding D: members released past a smaller resubscribe
/// still move onto the purchased tier. `roll_period` grants every member the
/// allowance of their `seat_tier_id`, slot or not, so a released member left
/// on the old subscription's premium tier would be granted a premium
/// allowance nobody pays for.
#[tokio::test]
async fn a_resubscribe_moves_released_members_off_the_old_tier() {
    const NAME: &str = "a_resubscribe_moves_released_members_off_the_old_tier";
    let Some((owner, app)) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let price = format!("price_std_resub_{}", Uuid::new_v4().simple());
    let previous = set_seat_tier_price(&owner, "standard", Some(&price)).await;
    let (account, user) = seed(&owner).await;
    let (standard, standard_credits): (Uuid, sqlx::types::Decimal) =
        sqlx::query_as("SELECT id, credits FROM seat_tiers WHERE code = 'standard' AND is_active")
            .fetch_one(&owner)
            .await
            .unwrap();
    let premium: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium' AND is_active")
        .fetch_one(&owner)
        .await
        .unwrap();
    let mut members = Vec::new();
    for _ in 0..2 {
        let m = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(m)
            .bind(format!("resub-{}@test.example", m.simple()))
            .execute(&owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)")
            .bind(account)
            .bind(m)
            .bind(premium)
            .execute(&owner)
            .await
            .unwrap();
        members.push(m);
    }
    let sub_a = format!("sub_resub_a_{}", Uuid::new_v4().simple());
    let sub_b = format!("sub_resub_b_{}", Uuid::new_v4().simple());
    checkout(&app, account, &price, &sub_a, 3, 1_700_000_000).await;
    // Both members held premium slots on the first subscription.
    for (i, m) in members.iter().enumerate() {
        fin(
            &owner,
            account,
            sqlx::query(
                "UPDATE account_members SET seat_tier_id = $3, seat_purchased_at = now() + make_interval(secs => $4) \
                  WHERE account_id = $1 AND user_id = $2",
            )
            .bind(account)
            .bind(m)
            .bind(premium)
            .bind((i + 1) as f64),
        )
        .await;
    }
    apply_subscription_deleted(
        &app,
        account,
        &format!("evt_{}", Uuid::new_v4().simple()),
        "customer.subscription.deleted",
        &sub_a,
        Some("cancellation_requested"),
    )
    .await
    .expect("delete");
    // Resubscribe on ONE standard seat: the owner keeps it, both members are
    // released.
    let start = 1_705_000_000_i64;
    let status = checkout(&app, account, &price, &sub_b, 1, start).await;
    let tiers: Vec<(Uuid, Uuid, bool)> = sqlx::query_as(
        "SELECT user_id, seat_tier_id, seat_purchased_at IS NOT NULL FROM account_members \
          WHERE account_id = $1 ORDER BY user_id",
    )
    .bind(account)
    .fetch_all(&owner)
    .await
    .unwrap();
    // The new subscription's first invoice rolls the period.
    let invoice = json!({
        "id": format!("in_{}", Uuid::new_v4().simple()), "object": "invoice", "customer": "cus_resub",
        "billing_reason": "subscription_create", "amount_paid": 4000, "subscription": sub_b,
        "parent": {"subscription_details": {"subscription": sub_b}},
        "lines": {"data": [{"period": {"start": start, "end": start + 2_592_000}}]},
    });
    let rolled = handle_invoice_paid(
        &app,
        None,
        account,
        &format!("evt_{}", Uuid::new_v4().simple()),
        "invoice.paid",
        "cus_resub",
        &invoice,
    )
    .await
    .status();
    let granted: Vec<sqlx::types::Decimal> = sqlx::query_scalar(
        "SELECT granted_credits FROM member_allowance_periods WHERE account_id = $1 AND period_start = to_timestamp($2) \
          ORDER BY user_id",
    )
    .bind(account)
    .bind(start as f64)
    .fetch_all(&owner)
    .await
    .unwrap();
    cleanup(&owner, account, user).await;
    for m in &members {
        sqlx::query("DELETE FROM users WHERE id = $1").bind(m).execute(&owner).await.unwrap();
    }
    set_seat_tier_price(&owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(tiers.len(), 3);
    assert_eq!(tiers.iter().filter(|(_, _, seated)| *seated).count(), 1, "only the owner keeps a slot");
    assert!(
        tiers.iter().all(|(_, tier, _)| *tier == standard),
        "every member, released or not, is on the purchased tier: {tiers:?}"
    );
    assert_eq!(rolled, StatusCode::OK);
    assert_eq!(granted, vec![standard_credits; 3], "no premium allowance is granted after the resubscribe");
}
