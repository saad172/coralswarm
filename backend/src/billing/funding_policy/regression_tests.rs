//! Regression tests for the money review of PR #1474. Each test failed on the
//! reviewed code (the failing behaviour is named in its doc comment) and passes
//! after the fix.

use chrono::{Duration, Utc};
use sqlx::types::Decimal;
use uuid::Uuid;

use super::db::{PeriodSel, SettlementPlanner};
use super::pg_tests::*;
use super::RefusalReason;
use crate::workers::billing_derivation::{native_job_debit, native_journal_debit};

/// An `extra` reservation's whole bound was counted against the plan
/// allowance AND against extra usage, so a second reservation was refused
/// (`extra_balance_exhausted`) although the balance covered it. Now only the
/// extra share (`extra_credits`) is held against extra usage and only the
/// allowance share against the allowance.
#[tokio::test]
async fn an_extra_reservation_holds_each_credit_once() {
    let Some(pool) = pg("an_extra_reservation_holds_each_credit_once").await else { return };
    // 100 credits of own allowance; $50 limit; $5 balance (227 credits).
    let a = seed(&pool, Plan::Pro, 100, 0).await;
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 5_000_000).await;

    // 150 = 100 allowance + 50 extra (1.1M micro-USD).
    let first = admit(&pool, &a, d(150)).await.expect("funded");
    assert_eq!(first.1, "extra");
    let r1 = request(&a, 150_000_000);
    hold(&pool, &a, &r1, d(150), "extra", Some(d(50))).await;

    // Outstanding extra is 50 credits, not 150. The allowance is fully held,
    // so another 150 needs 150 extra: 50 + 150 = 200 credits = 4.4M <= 5M.
    let second = admit(&pool, &a, d(150)).await;
    assert_eq!(second.map(|f| f.1).as_deref(), Some("extra"), "false extra_balance_exhausted");
    let r2 = request(&a, 150_000_000);
    hold(&pool, &a, &r2, d(150), "extra", Some(d(150))).await;

    // 200 credits are now held against extra (4.4M of 5M): 80 more (1.76M) do not fit.
    assert_eq!(admit(&pool, &a, d(80)).await, None);
    // 20 (0.44M) still do.
    assert_eq!(admit(&pool, &a, d(20)).await.map(|f| f.1).as_deref(), Some("extra"));
}

/// Releasing or settling an `extra` reservation frees exactly its extra share.
#[tokio::test]
async fn releasing_an_extra_reservation_frees_its_extra_share() {
    let Some(pool) = pg("releasing_an_extra_reservation_frees_its_extra_share").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 100 * RATE).await;
    let r = request(&a, 90_000_000);
    hold(&pool, &a, &r, d(90), "extra", Some(d(90))).await;
    assert_eq!(admit(&pool, &a, d(20)).await, None, "90 held of 100");
    let mut tx = fenced(&pool, a.id).await;
    crate::billing::managed_reservation::release_unsent_tx(&mut tx, &r).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(admit(&pool, &a, d(20)).await.map(|f| f.1).as_deref(), Some("extra"));
}

/// A reservation admitted by `managed_funding_tx` is stored with its extra
/// share (the column the holds read) and the share is immutable once stored.
#[tokio::test]
async fn the_extra_share_is_stored_and_immutable() {
    let Some(pool) = pg("the_extra_share_is_stored_and_immutable").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 10_000_000).await;
    let r = request(&a, 5_000_000);
    hold(&pool, &a, &r, d(5), "extra", Some(d(5))).await;
    let stored: Option<Decimal> =
        sqlx::query_scalar("SELECT extra_credits FROM managed_credit_reservations WHERE receipt_id=$1")
            .bind(r.receipt)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, Some(d(5)));
    let mut tx = fenced(&pool, a.id).await;
    let tamper = sqlx::query("UPDATE managed_credit_reservations SET extra_credits=1 WHERE receipt_id=$1")
        .bind(r.receipt)
        .execute(&mut *tx)
        .await;
    assert!(tamper.is_err(), "the binding trigger pins extra_credits");
}

/// The journal idempotency key had no account id and `idempotency_key` is
/// globally unique, so a second account's journal debit for the same
/// (graph, granularity, period) failed its whole derivation batch forever.
#[tokio::test]
async fn journal_debits_of_two_accounts_do_not_collide() {
    let Some(pool) = pg("journal_debits_of_two_accounts_do_not_collide").await else { return };
    let a = pro_with_extra(&pool, 10_000_000).await;
    let b = pro_with_extra(&pool, 10_000_000).await;
    let graph = Uuid::new_v4();
    let period = Utc::now() - Duration::hours(3);
    native_journal_debit(&pool, a.id, Some(a.user), graph, period, d(2)).await.expect("first account");
    native_journal_debit(&pool, b.id, Some(b.user), graph, period, d(2)).await.expect("second account");
    assert_eq!(extra_debits(&pool, &a).await, (1, 2 * RATE));
    assert_eq!(extra_debits(&pool, &b).await, (1, 2 * RATE));
    // And each account's own replay is still a no-op.
    assert_eq!(native_journal_debit(&pool, a.id, Some(a.user), graph, period, d(2)).await.unwrap(), 0);
    assert_eq!(extra_debits(&pool, &a).await, (1, 2 * RATE));
}

/// Every key carries the account: job, receipt, query and journal.
#[tokio::test]
async fn every_debit_key_names_its_account() {
    let Some(pool) = pg("every_debit_key_names_its_account").await else { return };
    let a = pro_with_extra(&pool, 10_000_000).await;
    debit(&pool, &a, Some(a.user), 1).await;
    native_journal_debit(&pool, a.id, Some(a.user), Uuid::new_v4(), Utc::now() - Duration::hours(1), d(1))
        .await
        .unwrap();
    let keys: Vec<String> =
        sqlx::query_scalar("SELECT idempotency_key FROM extra_usage_ledger WHERE account_id=$1")
            .bind(a.id)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(keys.len(), 2);
    for k in keys {
        assert!(k.starts_with(&format!("debit:")) && k.contains(&a.id.to_string()), "{k}");
    }
}

/// A payer with no allowance period was counted in the calendar month and a
/// payer WITH one in the allowance period, so the limit restarted from zero
/// when a period appeared mid-month (spend 440000 against a 220000 limit).
#[tokio::test]
async fn the_monthly_limit_survives_an_allowance_period_appearing_mid_month() {
    let Some(pool) = pg("the_monthly_limit_survives_an_allowance_period_appearing_mid_month").await else {
        return;
    };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    let mut tx = fenced(&pool, a.id).await;
    sqlx::query("DELETE FROM member_allowance_periods WHERE account_id=$1")
        .bind(a.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    set_extra(&pool, &a, true, Some(10 * RATE), None).await; // limit: 10 credits
    set_extra_balance(&pool, &a, 10_000_000).await;
    native_job_debit(&pool, a.id, Some(a.user), Uuid::new_v4(), d(10)).await.unwrap();
    native_job_debit(&pool, a.id, Some(a.user), Uuid::new_v4(), d(5)).await.unwrap();
    assert_eq!(extra_debits(&pool, &a).await, (1, 10 * RATE), "limit reached");

    let mut tx = fenced(&pool, a.id).await;
    sqlx::query(
        "INSERT INTO member_allowance_periods (account_id,user_id,period_start,period_end,granted_credits,consumed_credits) \
         VALUES ($1,$2,now()-interval '1 hour',now()+interval '30 days',0,0)",
    )
    .bind(a.id)
    .bind(a.user)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(preflight(&pool, &a, a.user).await, Some(RefusalReason::MonthlyLimitReached));
    native_job_debit(&pool, a.id, Some(a.user), Uuid::new_v4(), d(10)).await.unwrap();
    assert_eq!(extra_debits(&pool, &a).await, (1, 10 * RATE), "still bounded by the same limit");
}

/// Rounding is ceil per debit: at most one micro-USD per ledger row over the
/// exact price, never under (documented in the module doc and the PR).
#[tokio::test]
async fn rounding_is_ceil_per_debit_and_bounded_by_one_micro_usd_per_row() {
    let Some(pool) = pg("rounding_is_ceil_per_debit_and_bounded_by_one_micro_usd_per_row").await else { return };
    let a = pro_with_extra(&pool, 10_000_000).await;
    let tiny = Decimal::new(1, 5); // 0.00001 credits = 0.22 micro-USD exactly
    for _ in 0..100 {
        native_job_debit(&pool, a.id, Some(a.user), Uuid::new_v4(), tiny).await.unwrap();
    }
    let (rows, charged) = extra_debits(&pool, &a).await;
    assert_eq!((rows, charged), (100, 100));
    let exact_micro = 100.0 * 0.22;
    assert!(charged as f64 >= exact_micro && (charged as f64) < exact_micro + rows as f64);
}

/// Frozen accounts never spend extra usage at settlement, deliberate or
/// lapsed (decision 19; `past_due` freezing is pending a founder decision).
#[tokio::test]
async fn a_frozen_account_never_spends_extra_usage() {
    let Some(pool) = pg("a_frozen_account_never_spends_extra_usage").await else { return };
    for status in ["canceled", "past_due"] {
        let a = pro_with_extra(&pool, 10_000_000).await;
        set_subscription(&pool, &a, status).await;
        native_job_debit(&pool, a.id, Some(a.user), Uuid::new_v4(), d(5)).await.unwrap();
        assert_eq!(extra_debits(&pool, &a).await, (0, 0), "{status}");
        assert_eq!(extra_balance(&pool, &a).await, 10_000_000);
        assert_eq!(plan_ledger_sum(&pool, &a).await, d(-5));
        assert_eq!(preflight(&pool, &a, a.user).await, Some(RefusalReason::CreditsFrozen));
    }
}

/// A reservation pinned to the previous allowance period settles after the
/// period rolled: no overlap error, and its spend counts in the same calendar
/// month as the rest.
#[tokio::test]
async fn a_pinned_settlement_after_an_allowance_rollover_counts_in_the_month() {
    let Some(pool) = pg("a_pinned_settlement_after_an_allowance_rollover_counts_in_the_month").await else {
        return;
    };
    let a = pro_with_extra(&pool, 10_000_000).await;
    let r = request(&a, 5_000_000);
    hold(&pool, &a, &r, d(5), "extra", Some(d(5))).await;
    let boundary = Utc::now() - Duration::seconds(1);
    let mut tx = fenced(&pool, a.id).await;
    sqlx::query("UPDATE member_allowance_periods SET period_end=$2 WHERE account_id=$1")
        .bind(a.id)
        .bind(boundary)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO member_allowance_periods (account_id,user_id,period_start,period_end,granted_credits,consumed_credits) \
         VALUES ($1,$2,$3,$4,0,0)",
    )
    .bind(a.id)
    .bind(a.user)
    .bind(boundary)
    .bind(boundary + Duration::days(30))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    native_job_debit(&pool, a.id, Some(a.user), Uuid::new_v4(), d(2)).await.unwrap();
    let mut tx = fenced(&pool, a.id).await;
    crate::billing::managed_reservation::settle_tx(&mut tx, &r, 3_000_000).await.expect("pinned settle");
    tx.commit().await.unwrap();
    assert_eq!(extra_debits(&pool, &a).await, (2, 5 * RATE));
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM extra_usage_periods WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

/// Unattributed team-org spend was never extra-funded (cap room 0 for "no
/// payer") and never closed the gate: free overshoot. It is now charged to
/// the org limit and balance, so the gate closes when they run out.
#[tokio::test]
async fn unattributed_team_spend_is_no_longer_free() {
    let Some(pool) = pg("unattributed_team_spend_is_no_longer_free").await else { return };
    let org = seed(&pool, Plan::Team, 100, 100).await;
    set_extra(&pool, &org, true, Some(50_000_000), Some(1)).await;
    set_extra_balance(&pool, &org, 4 * 40 * RATE).await; // 160 credits
    assert_eq!(preflight(&pool, &org, org.user).await, None);
    for _ in 0..5 {
        native_job_debit(&pool, org.id, None, Uuid::new_v4(), d(40)).await.unwrap();
    }
    let (rows, charged) = extra_debits(&pool, &org).await;
    assert_eq!((rows, charged), (4, 160 * RATE), "funded until the balance is gone");
    assert_eq!(plan_ledger_sum(&pool, &org).await, d(-40), "only the last 40 are unfunded");
    assert_eq!(
        preflight(&pool, &org, org.user).await,
        Some(RefusalReason::ExtraBalanceExhausted),
        "the gate closes instead of staying open"
    );
}

/// Sanity for the planner pin used by the settlement path.
#[tokio::test]
async fn planner_defaults_to_the_org_limit_policy() {
    let Some(pool) = pg("planner_defaults_to_the_org_limit_policy").await else { return };
    let org = seed(&pool, Plan::Team, 100, 100).await;
    set_extra(&pool, &org, true, Some(50_000_000), Some(1)).await;
    set_extra_balance(&pool, &org, 10_000_000).await;
    let mut tx = fenced(&pool, org.id).await;
    let mut planner = SettlementPlanner::new(org.id);
    let split = planner.plan(&mut tx, None, PeriodSel::Current, d(5)).await.unwrap();
    assert_eq!(split.extra_payer, None);
    assert_eq!(split.extra.micro_usd, 5 * RATE);
    tx.rollback().await.unwrap();
}
