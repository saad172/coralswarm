//! Live-Postgres tests for `GET /v1/billing/me`: the member, admin, Pro, Free
//! and frozen shapes, the blocked reason, and the rule that no dollar figure
//! ever appears in the response.

use serde_json::Value;

use super::*;
use crate::billing::notices::pg_tests::kit::*;

/// Every key in the JSON, recursively.
fn keys(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            for (k, v) in m {
                out.push(k.clone());
                keys(v, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|v| keys(v, out)),
        _ => {}
    }
}

fn assert_no_dollars(me: &BillingMe) {
    let v = serde_json::to_value(me).unwrap();
    let mut ks = Vec::new();
    keys(&v, &mut ks);
    for k in &ks {
        for banned in ["usd", "dollar", "micro", "cents", "price", "amount", "cost"] {
            assert!(!k.contains(banned), "field {k} looks like money: {v}");
        }
    }
    assert!(!v.to_string().contains('$'), "{v}");
}

#[tokio::test]
async fn a_team_member_sees_their_allowance_and_their_cap_and_nothing_in_dollars() {
    let Some(pool) = pg("a_team_member_sees_their_allowance_and_their_cap_and_nothing_in_dollars").await else { return };
    let a = seed(&pool, Plan::Team, 100, 0).await;
    let (member, _) = add_member(&pool, &a, "member", 100, 30).await;
    set_extra(&pool, &a, true, Some(50_000_000), Some(1)).await;
    set_extra_balance(&pool, &a, 10_000_000).await;

    let me = build(&pool, a.id, member).await.unwrap();
    assert_no_dollars(&me);
    assert_eq!(me.account_kind, AccountKind::TeamOrg);
    assert_eq!(me.plan.code, "team_standard");
    let credits = me.plan_credits.clone().unwrap();
    assert_eq!((credits.used, credits.total, credits.remaining), (30.0, 100.0, 70.0));
    assert!(credits.resets_at > credits.period_start);
    assert_eq!(me.extra_usage.status, ExtraUsageStatus::On);
    assert_eq!(me.extra_usage.scope, Some(ExtraUsageScope::MemberCap));
    assert_eq!((me.extra_usage.used_credits, me.extra_usage.limit_credits), (0.0, Some(100.0)));
    assert_eq!(me.extra_usage.percent_used, Some(0.0));
    assert_eq!(me.blocked, BlockedMe { blocked: false, reason: None });

    // Spend past the allowance into extra usage: 70 left + 40 extra.
    debit(&pool, &a, Some(member), 110).await;
    let me = build(&pool, a.id, member).await.unwrap();
    assert_no_dollars(&me);
    assert_eq!(me.extra_usage.used_credits, 40.0);
    assert_eq!(me.extra_usage.percent_used, Some(40.0));
    assert!(!me.blocked.blocked);

    // Reach the cap.
    debit(&pool, &a, Some(member), 60).await;
    let me = build(&pool, a.id, member).await.unwrap();
    assert_eq!(me.extra_usage.percent_used, Some(100.0));
    assert_eq!(
        me.blocked,
        BlockedMe { blocked: true, reason: Some("member_cap_reached".into()) }
    );
    // Reading it again is a pure read: it claimed no notice.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM billing_notices WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 0, "looking at your usage must never send mail");
}

#[tokio::test]
async fn an_admin_gets_the_same_member_shape_for_themselves() {
    let Some(pool) = pg("an_admin_gets_the_same_member_shape_for_themselves").await else { return };
    let a = seed(&pool, Plan::Team, 100, 10).await;
    let (admin, _) = add_member(&pool, &a, "admin", 100, 5).await;
    let (other, _) = add_member(&pool, &a, "member", 100, 100).await;
    set_extra(&pool, &a, true, Some(50_000_000), Some(1)).await;
    set_extra_balance(&pool, &a, 10_000_000).await;
    debit(&pool, &a, Some(other), 150).await; // another member's extra spend

    let me = build(&pool, a.id, admin).await.unwrap();
    assert_no_dollars(&me);
    assert_eq!(me.account_kind, AccountKind::TeamOrg);
    assert_eq!(me.plan_credits.unwrap().used, 5.0, "their own usage, not the org's");
    assert_eq!(me.extra_usage.scope, Some(ExtraUsageScope::MemberCap));
    assert_eq!(me.extra_usage.used_credits, 0.0, "another member's extra spend is not theirs");
}

#[tokio::test]
async fn a_team_member_with_extra_usage_off_has_no_limit_to_show() {
    let Some(pool) = pg("a_team_member_with_extra_usage_off_has_no_limit_to_show").await else { return };
    let a = seed(&pool, Plan::Team, 100, 100).await;
    let me = build(&pool, a.id, a.user).await.unwrap();
    assert_eq!(me.extra_usage.status, ExtraUsageStatus::Off);
    assert_eq!((me.extra_usage.scope, me.extra_usage.limit_credits, me.extra_usage.percent_used), (None, None, None));
    assert_eq!(me.blocked.reason.as_deref(), Some("extra_usage_off"));
}

#[tokio::test]
async fn a_pro_account_sees_its_monthly_limit_in_credits() {
    let Some(pool) = pg("a_pro_account_sees_its_monthly_limit_in_credits").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    // $50 at $0.022 per credit is 2272 whole credits.
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 100_000_000).await;
    debit(&pool, &a, Some(a.user), 568).await;

    let me = build(&pool, a.id, a.user).await.unwrap();
    assert_no_dollars(&me);
    assert_eq!(me.account_kind, AccountKind::Personal);
    assert_eq!(me.plan.code, "pro");
    assert_eq!(me.extra_usage.scope, Some(ExtraUsageScope::MonthlyLimit));
    assert_eq!(me.extra_usage.used_credits, 568.0);
    assert_eq!(me.extra_usage.limit_credits, Some(2272.0));
    assert_eq!(me.extra_usage.percent_used, Some(25.0));
    assert!(!me.blocked.blocked);

    // Out of balance: blocked with the balance reason.
    set_extra_balance(&pool, &a, 0).await;
    let me = build(&pool, a.id, a.user).await.unwrap();
    assert_eq!(me.blocked.reason.as_deref(), Some("extra_balance_exhausted"));
}

#[tokio::test]
async fn a_free_account_has_no_extra_usage_and_is_blocked_when_out_of_credits() {
    let Some(pool) = pg("a_free_account_has_no_extra_usage_and_is_blocked_when_out_of_credits").await else { return };
    let a = seed(&pool, Plan::Free, 50, 20).await;
    let me = build(&pool, a.id, a.user).await.unwrap();
    assert_no_dollars(&me);
    assert!(me.plan.is_free);
    assert_eq!(me.extra_usage.status, ExtraUsageStatus::Unavailable);
    assert_eq!(me.extra_usage.limit_credits, None);
    assert_eq!(me.plan_credits.as_ref().unwrap().remaining, 30.0);
    assert!(!me.blocked.blocked);

    let spent = seed(&pool, Plan::Free, 50, 50).await;
    let me = build(&pool, spent.id, spent.user).await.unwrap();
    assert_eq!(me.blocked, BlockedMe { blocked: true, reason: Some("free_plan".into()) });
}

#[tokio::test]
async fn a_frozen_subscription_reports_credits_frozen() {
    let Some(pool) = pg("a_frozen_subscription_reports_credits_frozen").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 10_000_000).await;
    set_subscription(&pool, &a, "canceled").await;
    let me = build(&pool, a.id, a.user).await.unwrap();
    assert_no_dollars(&me);
    assert_eq!(me.blocked, BlockedMe { blocked: true, reason: Some("credits_frozen".into()) });
    assert_eq!(me.extra_usage.status, ExtraUsageStatus::On);
}

#[tokio::test]
async fn a_member_with_no_allowance_period_yet_gets_null_credits_not_zero() {
    let Some(pool) = pg("a_member_with_no_allowance_period_yet_gets_null_credits_not_zero").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 0).await;
    let (stranger, _) = new_user(&pool, "x").await;
    let me = build(&pool, a.id, stranger).await.unwrap();
    assert!(me.plan_credits.is_none());
}

#[tokio::test]
async fn the_route_returns_the_contract_for_a_signed_in_member() {
    let Some(pool) = pg("the_route_returns_the_contract_for_a_signed_in_member").await else { return };
    let a = seed(&pool, Plan::Team, 100, 25).await;
    let (member, _) = add_member(&pool, &a, "member", 100, 25).await;
    set_extra(&pool, &a, true, Some(50_000_000), Some(1)).await;
    let state = std::sync::Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
    let user = crate::middleware::auth::UserContext {
        user_id: member,
        account_id: a.id,
        role: "member".into(),
        device_id: "d".into(),
    };
    let resp = crate::routes::billing_me::get_billing_me(
        axum::extract::State(state),
        axum::extract::Extension(user),
    )
    .await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["account_kind"], "team_org");
    assert_eq!(v["plan_credits"]["used"], 25.0);
    assert_eq!(v["extra_usage"]["scope"], "member_cap");
    assert_eq!(v["blocked"]["blocked"], false);
    assert!(!v.to_string().contains('$'));
}
