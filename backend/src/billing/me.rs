//! `GET /v1/billing/me`: one signed-in member's own billing position, for the
//! "Your usage" card on Account (Settings launch plan PR 3.6, decision 15).
//!
//! # No dollars, anywhere
//!
//! Every amount is credits or a percentage, for every account kind. A member
//! of a team organization must not see money (decision 15), and the Pro owner
//! gets the same shape so the web renders one card, not two. Money lives
//! behind the admin/owner Billing page (`/v1/billing/summary`, 3.3), which is
//! where someone who can act on it goes. A Pro account's monthly limit is
//! stored in dollars; it is converted to credits at the rate in force
//! (`extra_usage_rates`), which is the same conversion the funding policy
//! applies when it spends.
//!
//! # What "extra usage" means for each caller
//!
//! * **Team organization member (admins included):** `scope = "member_cap"`.
//!   `used_credits` is the caller's own extra-usage spend this window and
//!   `limit_credits` is their cap (cap multiple x their seat allowance, the
//!   same product the funding policy enforces). The organization-wide limit
//!   and balance are not exposed: they are admin settings, not a member's.
//! * **Personal (Pro) account:** `scope = "monthly_limit"`. `used_credits` is
//!   the account's extra-usage spend this window, `limit_credits` is the
//!   monthly limit in credits. No member cap exists.
//!
//! # Blocked
//!
//! `blocked` is the answer the pre-flight gate would give this caller right
//! now ([`check_funding`]), with the refusal code the paywall copy keys on
//! (`free_plan`, `credits_frozen`, `extra_usage_off`, `member_cap_reached`,
//! `monthly_limit_reached`, `extra_balance_exhausted`). Reading it sends no
//! notice: only the gates that actually refuse work do.
//!
//! # Reading, not fabricating
//!
//! A failed read is an error, never a zero. A missing allowance period (a
//! brand-new member the renewal has not reached) is `plan_credits: null`, not
//! `0 of 0`.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{types::Decimal, PgPool};
use utoipa::ToSchema;
use uuid::Uuid;

use super::enforcement::{check_funding, Spend};
use super::funding_policy::db::{
    extra_window, load_extra_shared, load_member_spent, load_period, Mode, PeriodSel,
};

#[derive(Debug, Clone, Copy, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccountKind {
    TeamOrg,
    Personal,
}

#[derive(Debug, Clone, Serialize, ToSchema, PartialEq)]
pub struct PlanMe {
    /// `free`, `pro`, `team`, ...
    pub code: String,
    pub is_free: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema, PartialEq)]
pub struct PlanCreditsMe {
    /// Credits the caller has used from their own allowance this period.
    pub used: f64,
    /// The caller's allowance for the period.
    pub total: f64,
    /// `max(total - used, 0)`.
    pub remaining: f64,
    /// The period start (inclusive).
    #[schema(value_type = String, format = DateTime)]
    pub period_start: DateTime<Utc>,
    /// When the allowance resets (the period end).
    #[schema(value_type = String, format = DateTime)]
    pub resets_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtraUsageStatus {
    /// The plan has no extra usage (Free).
    Unavailable,
    /// Available but not switched on.
    Off,
    On,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtraUsageScope {
    /// A team member's own cap.
    MemberCap,
    /// A personal account's monthly limit.
    MonthlyLimit,
}

#[derive(Debug, Clone, Serialize, ToSchema, PartialEq)]
pub struct ExtraUsageMe {
    pub status: ExtraUsageStatus,
    /// What `limit_credits` is the limit of; `null` unless `status = "on"`.
    pub scope: Option<ExtraUsageScope>,
    /// Extra-usage credits the caller (team) or the account (personal) has
    /// used in the current window.
    pub used_credits: f64,
    /// The cap or limit in credits; `null` unless `status = "on"`.
    pub limit_credits: Option<f64>,
    /// `used / limit` as 0 to 100, capped; `null` without a limit.
    pub percent_used: Option<f64>,
}

#[derive(Debug, Clone, Serialize, ToSchema, PartialEq)]
pub struct BlockedMe {
    pub blocked: bool,
    /// The refusal code when blocked, else `null`.
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema, PartialEq)]
pub struct BillingMe {
    pub account_kind: AccountKind,
    pub plan: PlanMe,
    /// `null` when the caller has no allowance period yet.
    pub plan_credits: Option<PlanCreditsMe>,
    pub extra_usage: ExtraUsageMe,
    pub blocked: BlockedMe,
}

fn f(d: Decimal) -> f64 {
    d.to_string().parse().unwrap_or(0.0)
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Build the response for `user` on `account`.
pub async fn build(control: &PgPool, account: Uuid, user: Uuid) -> Result<BillingMe, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let shared = load_extra_shared(&mut tx, account).await?;
    let plan_code: Option<String> = sqlx::query_scalar(
        "SELECT p.code FROM grants g JOIN plans p ON p.id = g.plan_id \
          WHERE g.account_id = $1 AND g.revoked_at IS NULL \
          ORDER BY g.granted_at DESC, g.plan_is_seat_priced DESC, g.id DESC LIMIT 1",
    )
    .bind(account)
    .fetch_optional(&mut *tx)
    .await?;
    let period = load_period(&mut tx, account, user, Mode::Settlement, PeriodSel::Current).await?;
    let window = extra_window(&mut tx).await?;
    // The plan allowance period is its own window, independent of the
    // calendar month extra usage is counted in.
    let plan_window: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)> = match period.as_ref() {
        Some(p) => {
            sqlx::query_as("SELECT period_start, period_end FROM member_allowance_periods WHERE id = $1")
                .bind(p.id)
                .fetch_optional(&mut *tx)
                .await?
        }
        None => None,
    };

    let enabled = shared.settings.as_ref().filter(|s| s.enabled);
    let (used_credits, limit_credits, scope) = if shared.plan_is_free {
        (Decimal::ZERO, None, None)
    } else if shared.is_team_org {
        let used = load_member_spent(&mut tx, account, user, window.0).await?;
        // The funding policy's cap: multiple (default 1) x the seat allowance.
        let limit = enabled.map(|s| {
            let multiple = s.member_cap_seat_multiple.unwrap_or(Decimal::ONE);
            let seat = period.as_ref().map(|p| p.member.granted).unwrap_or(Decimal::ZERO);
            multiple.checked_mul(seat).unwrap_or(Decimal::MAX)
        });
        (used, limit, enabled.map(|_| ExtraUsageScope::MemberCap))
    } else {
        let used: Decimal = sqlx::query_scalar(
            "SELECT spent_credits FROM extra_usage_periods WHERE account_id = $1 AND period_start = $2",
        )
        .bind(account)
        .bind(window.0)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(Decimal::ZERO);
        let limit = enabled.and_then(|s| s.monthly_limit_micro_usd).map(|micro| {
            (Decimal::from(micro) / Decimal::from(shared.rate_micro_usd_per_credit.max(1))).floor()
        });
        (used, limit, enabled.map(|_| ExtraUsageScope::MonthlyLimit))
    };
    tx.commit().await?;

    let status = if shared.plan_is_free {
        ExtraUsageStatus::Unavailable
    } else if enabled.is_some() {
        ExtraUsageStatus::On
    } else {
        ExtraUsageStatus::Off
    };
    let used_f = f(used_credits);
    let limit_f = limit_credits.map(f);
    let percent_used = limit_f.map(|l| if l > 0.0 { round2((used_f / l * 100.0).clamp(0.0, 100.0)) } else { 100.0 });

    let refusal = check_funding(control, account, user, Spend::Interactive).await?;

    let code = plan_code.unwrap_or_else(|| "free".to_string());
    Ok(BillingMe {
        account_kind: if shared.is_team_org { AccountKind::TeamOrg } else { AccountKind::Personal },
        plan: PlanMe { is_free: code == "free", code },
        plan_credits: period.zip(plan_window).map(|(p, plan_window)| {
            let total = f(p.member.granted);
            let used = f(p.member.consumed);
            PlanCreditsMe {
                used,
                total,
                remaining: (total - used).max(0.0),
                period_start: plan_window.0,
                resets_at: plan_window.1,
            }
        }),
        extra_usage: ExtraUsageMe {
            status,
            scope,
            used_credits: used_f,
            limit_credits: limit_f,
            percent_used,
        },
        blocked: BlockedMe { blocked: refusal.is_some(), reason: refusal.map(|r| r.code().to_string()) },
    })
}

#[cfg(test)]
mod pg_tests;
