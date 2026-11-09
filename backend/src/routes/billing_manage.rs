//! Billing management endpoints for the Settings Billing page (settings
//! launch PR 3.3):
//!
//! * `POST /v1/billing/portal`: a Stripe Billing Portal session URL.
//! * `GET  /v1/billing/summary`: plan, seats, credits, extra usage, renewal,
//!   cancellation state and card on file, in one read.
//! * `GET  /v1/billing/seats/quote`: what Stripe would charge for adding or
//!   removing seats, so the web can show a price before the admin confirms.
//!
//! # Who may call
//!
//! All three are money surfaces, so they use the same authority bar as
//! checkout, [`require_purchase_authority`]: an org account requires
//! `owner`/`admin`; a personal account (plain or personal org) is always its
//! own user. A plain org member gets **403**. Members will read their own
//! slice from `GET /v1/billing/me` (PR 3.6). The seat quote is org-only
//! (`org_only = true`), so a personal account gets 400.
//!
//! # The portal does not change seats
//!
//! The Billing Portal configuration (`stripe_client::portal_configuration_params`,
//! provisioned by `provision_stripe`) allows payment-method update, invoice
//! history and cancel-at-period-end only. Seat quantity moves through
//! `POST /v1/orgs/slots` (quote, then confirm), which keeps the seat ledger
//! in step with Stripe. A portal session is only ever created against the
//! configuration this system owns. If it is not provisioned the route fails
//! (503) rather than falling back to the account's default configuration,
//! which could expose subscription updates.
//!
//! # Amounts are never client-supplied
//!
//! The quote takes a seat COUNT, never a price or total. Every figure in the
//! response comes from Stripe's preview invoice for the live subscription.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Extension, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::billing::seat_lifecycle::with_stripe_deadline;
use crate::billing::stripe_client::{PreviewChange, StripeError};
use crate::middleware::auth::UserContext;
use crate::routes::checkout::{require_purchase_authority, validate_return_urls};
use crate::AppState;

/// Env override for the portal configuration id (e.g. to pin one in staging).
/// Unset in normal operation: the configuration is found by its metadata
/// marker.
const PORTAL_CONFIGURATION_ENV: &str = "STRIPE_PORTAL_CONFIGURATION_ID";
/// The card lookup on the summary is decoration; never let Stripe latency
/// hold the page.
const CARD_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
const MIN_QUOTE_QUANTITY: i64 = 1;
const MAX_QUOTE_QUANTITY: i64 = 500;

// ── helpers ─────────────────────────────────────────────────────────────

fn err(status: StatusCode, code: &str, msg: &str) -> Response {
    (status, Json(json!({"error": msg, "code": code}))).into_response()
}
fn bad_request(msg: &str) -> Response {
    err(StatusCode::BAD_REQUEST, "bad_request", msg)
}
fn conflict(code: &str, msg: &str) -> Response {
    err(StatusCode::CONFLICT, code, msg)
}
fn service_unavailable() -> Response {
    err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable")
}
fn internal_error(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "billing_manage: failed");
    err(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", "internal error")
}
fn stripe_error(what: &str, e: StripeError) -> Response {
    tracing::error!(error = %e, step = what, "billing_manage: stripe call failed");
    match e {
        StripeError::NotConfigured | StripeError::Auth { .. } => service_unavailable(),
        _ => err(StatusCode::BAD_GATEWAY, "billing_unavailable", "billing provider unavailable"),
    }
}

// ── POST /v1/billing/portal ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PortalRequest {
    /// Where Stripe sends the browser when the admin leaves the portal. Must
    /// be on this deployment's own origin (same rule as checkout).
    pub return_url: String,
}

#[derive(Debug, Serialize)]
pub struct PortalResponse {
    /// Stripe-hosted, short-lived, single-purpose link.
    pub url: String,
}

pub async fn create_portal_session(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PortalRequest>,
) -> Response {
    if body.return_url.trim().is_empty() {
        return bad_request("return_url is required");
    }
    if let Err(resp) = require_purchase_authority(&state, &user, false).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    if let Err(resp) =
        validate_return_urls(&state, control, user.account_id, &body.return_url, &body.return_url).await
    {
        return resp;
    }
    let Some(stripe) = state.stripe.as_ref() else {
        return service_unavailable();
    };

    let customer_id: Option<String> = match read_customer_id(control, user.account_id).await {
        Ok(c) => c,
        Err(e) => return internal_error("customer lookup", e),
    };
    let Some(customer_id) = customer_id else {
        return conflict(
            "no_billing_account",
            "this account has no billing account yet, subscribe first",
        );
    };

    let configuration_id = match std::env::var(PORTAL_CONFIGURATION_ENV) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => match with_stripe_deadline(stripe.find_portal_configuration()).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                tracing::error!(
                    alert = true,
                    "billing portal: no active portal configuration with marker {}={}; run provision_stripe --execute",
                    crate::billing::stripe_client::PORTAL_CONFIG_METADATA_KEY,
                    crate::billing::stripe_client::PORTAL_CONFIG_VERSION,
                );
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "portal_not_configured",
                    "billing portal is not configured",
                );
            }
            Err(e) => return stripe_error("portal configuration lookup", e),
        },
    };

    match with_stripe_deadline(stripe.create_billing_portal_session(
        &customer_id,
        &body.return_url,
        Some(&configuration_id),
    ))
    .await
    {
        Ok(session) => (StatusCode::OK, Json(PortalResponse { url: session.url })).into_response(),
        Err(e) => stripe_error("create portal session", e),
    }
}

async fn read_customer_id(control: &sqlx::PgPool, account_id: Uuid) -> Result<Option<String>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row = sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id = $1")
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(row)
}

// ── GET /v1/billing/summary ─────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct BillingSummary {
    /// `"org"` or `"personal"`.
    pub account_kind: String,
    pub plan: Option<SummaryPlan>,
    pub seats: SummarySeats,
    pub credits: SummaryCredits,
    pub extra_usage: SummaryExtraUsage,
    pub subscription: SummarySubscription,
    /// Default payment method on file, of any type (best effort: `null` when
    /// none, or when Stripe did not answer within 3 seconds).
    pub payment_method: Option<SummaryCard>,
    /// Self-serve PR C′: the org's free-credit trial, when it has one.
    /// `null` for every other account.
    pub trial: Option<SummaryTrial>,
}

/// Credits for display (the summary reports credits as JSON numbers).
fn decimal_f64(d: sqlx::types::Decimal) -> f64 {
    d.to_string().parse().unwrap_or(0.0)
}

/// A self-serve org's trial, for the billing page and the trial banner.
#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryTrial {
    /// `checkout_open`, `trialing`, `converting`, `converted`, `abandoned`
    /// or `canceled` (`billing::org_trial::TrialState`).
    pub state: String,
    pub seats: i32,
    /// The trial pool, and how much of it is used (credits).
    pub credits_total: f64,
    pub credits_used: f64,
    /// When the 90-day backstop converts the trial.
    pub ends_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The charge is waiting on Stripe's retries (card declined).
    pub payment_failed: bool,
    /// What one seat costs per month once the trial converts (the
    /// `standard` seat tier the Checkout bought), in US cents. `null` when
    /// the tier is not readable; the page then omits the price.
    pub seat_price_usd_cents: Option<i32>,
    /// The monthly allowance one seat gets on the first charge.
    pub seat_monthly_credits: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryPlan {
    pub code: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SummarySeats {
    /// Members holding a purchased seat (org). 1 for a personal account.
    pub used: i64,
    /// Seats purchased (org). 1 for a personal account.
    pub total: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryCredits {
    /// Plan allowance consumed in the current period.
    pub allowance_used: f64,
    /// Plan allowance for the current period (all seats).
    pub allowance_total: f64,
    /// When the allowance resets (end of the current period), if known.
    pub resets_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryAutoReload {
    pub enabled: bool,
    pub threshold_micro_usd: Option<i64>,
    pub amount_micro_usd: Option<i64>,
}

/// All money is integer micro-USD (1 USD = 1_000_000).
#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryExtraUsage {
    /// No settings row means off.
    pub enabled: bool,
    pub balance_micro_usd: i64,
    pub monthly_limit_micro_usd: Option<i64>,
    /// Extra usage spent in the current period, against the limit.
    pub spent_this_period_micro_usd: i64,
    /// Purchased extra-usage credits are frozen (not forfeited): the
    /// subscription is `canceled` or `past_due`. Same predicate as
    /// `enforcement::status_is_frozen`, which the funding decision uses, so
    /// the page and the paywall cannot disagree. The balance is kept and
    /// returns on resubscribe or when payment recovers.
    pub frozen: bool,
    pub member_cap_seat_multiple: Option<f64>,
    pub auto_reload: SummaryAutoReload,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SummarySubscription {
    /// `active` | `past_due` | `canceled`, or `null` with no subscription.
    /// This is the MIRROR's closed domain (0162): Stripe's `trialing` is
    /// stored as `active` on purpose (every paid-up reader treats `active`
    /// as entitled). A trial is told apart by the `trial` block, never by
    /// this field.
    pub status: Option<String>,
    /// Whether a Stripe customer exists (the portal needs one).
    pub has_billing_account: bool,
    /// End of the current billing period (the renewal date).
    pub renews_at: Option<DateTime<Utc>>,
    pub cancel_at_period_end: bool,
    pub cancel_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryCard {
    /// Stripe's payment method type: `card`, `us_bank_account`, `link`, ...
    #[serde(rename = "type")]
    pub kind: String,
    /// Card fields, present for `card` only (unchanged for existing clients).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brand: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last4: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp_month: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp_year: Option<i64>,
    /// `us_bank_account` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bank_name: Option<String>,
    /// `link` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// Banner-only data. This is never a BillingSummary: payment decoration and
/// the other billing-page fields have not been read.
#[derive(Debug, Serialize)]
pub struct TrialBanner {
    pub kind: &'static str,
    pub trial: Option<SummaryTrial>,
}

/// Shared trial derivation keeps every state, credit and payment-failed rule
/// identical in the full summary and the banner's read-only snapshot.
async fn load_summary_trial(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
) -> Result<Option<SummaryTrial>, Response> {
    let trial = match crate::billing::org_trial::db::load(&mut **tx, account_id).await {
        Ok(Some(row)) => {
            let used = match crate::billing::org_trial::db::trial_used(&mut **tx, &row).await {
                Ok(u) => u,
                Err(e) => return Err(internal_error("trial usage", e)),
            };
            let payment_failed: bool = match sqlx::query_scalar(
                "SELECT payment_failed_at IS NOT NULL FROM org_trials WHERE account_id = $1",
            )
            .bind(account_id)
            .fetch_one(&mut **tx)
            .await
            {
                Ok(v) => v,
                Err(e) => return Err(internal_error("trial payment", e)),
            };
            let tier: Option<(i32, f64)> = match sqlx::query_as(
                "SELECT price_usd_cents, credits::float8 FROM seat_tiers WHERE code = $1",
            )
            .bind(crate::billing::org_trial::TRIAL_SEAT_TIER)
            .fetch_optional(&mut **tx)
            .await
            {
                Ok(t) => t,
                Err(e) => return Err(internal_error("trial seat tier", e)),
            };
            Some(SummaryTrial {
                seat_price_usd_cents: tier.map(|t| t.0),
                seat_monthly_credits: tier.map(|t| t.1),
                state: row.state.as_str().to_string(),
                seats: row.seats,
                credits_total: decimal_f64(row.trial_credits),
                // Before the pool exists (checkout) nothing is used.
                credits_used: if row.pool_baseline.is_some() { decimal_f64(used) } else { 0.0 },
                ends_at: row.trial_ends_at,
                payment_failed: payment_failed && row.state == crate::billing::org_trial::TrialState::Converting,
            })
        }
        Ok(None) => None,
        Err(e) => return Err(internal_error("trial", e)),
    };
    Ok(trial)
}

pub async fn get_trial_banner(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> Response {
    use crate::routes::bootstrap::measurement::{Guard, Outcome, Phase};
    let mut timing = Guard::start(Phase::BillingDb);
    let response = async {
        if let Err(response) = require_purchase_authority(&state, &user, false).await {
            return response;
        }
        let Some(control) = state.control_db() else { return service_unavailable(); };
        let mut tx = match crate::tenancy::scope::begin_request_account_read(control, user.account_id).await {
            Ok(tx) => tx,
            Err(e) => return internal_error("scope", e),
        };
        let trial = match load_summary_trial(&mut tx, user.account_id).await {
            Ok(trial) => trial,
            Err(response) => return response,
        };
        if let Err(e) = tx.commit().await { return internal_error("commit", e); }
        crate::routes::bootstrap::measurement::skipped(Phase::BillingProvider);
        (StatusCode::OK, Json(TrialBanner { kind: "trial_banner_v1", trial })).into_response()
    }.await;
    timing.finish(Outcome::status(response.status()));
    response
}

pub async fn get_billing_summary(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
) -> Response {
    use crate::routes::bootstrap::measurement::{Guard, Outcome, Phase};
    // Only records inside bootstrap's task-local scope. Includes the existing
    // purchase-authority read and scoped DB work, excluding the provider await.
    let mut db_timing = Guard::start(Phase::BillingDb);
    let response = async {
    if let Err(resp) = require_purchase_authority(&state, &user, false).await {
        return resp;
    }
    let (Some(control), Some(store)) = (state.control_db(), state.control_store()) else {
        return service_unavailable();
    };
    let account_id = user.account_id;
    let is_org = match store.account_is_real_org(account_id, user.user_id).await {
        Ok(v) => v,
        Err(e) => return internal_error("account lookup", e),
    };

    let mut tx = match crate::tenancy::scope::begin_request_account_read(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("scope", e),
    };

    let plan = match crate::billing::entitlement::current_plan(&mut *tx, account_id).await {
        Ok(p) => p,
        Err(e) => return internal_error("plan", e),
    };

    let seats = if is_org {
        match crate::billing::slot_pool::read_slot_pool(&mut tx, account_id).await {
            Ok(pool) => SummarySeats { used: pool.used, total: pool.purchased },
            Err(e) => return internal_error("seats", e),
        }
    } else {
        SummarySeats { used: 1, total: 1 }
    };

    type SubRow = (String, Option<String>, String, Option<DateTime<Utc>>, bool, Option<DateTime<Utc>>);
    let sub_row: Option<SubRow> =
        match sqlx::query_as(
            "SELECT stripe_customer_id, stripe_subscription_id, status, current_period_end, \
                    cancel_at_period_end, cancel_at \
               FROM stripe_subscriptions WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(r) => r,
            Err(e) => return internal_error("subscription", e),
        };

    let (used, granted, period_end): (f64, f64, Option<DateTime<Utc>>) = match sqlx::query_as(
        "SELECT COALESCE(SUM(consumed_credits), 0)::float8, COALESCE(SUM(granted_credits), 0)::float8, \
                MAX(period_end) \
           FROM member_allowance_periods \
          WHERE account_id = $1 AND period_start <= now() AND period_end > now()",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal_error("allowance", e),
    };

    type SettingsRow = (bool, Option<i64>, Option<f64>, bool, Option<i64>, Option<i64>);
    let settings: Option<SettingsRow> = match sqlx::query_as(
        "SELECT enabled, monthly_limit_micro_usd, member_cap_seat_multiple::float8, \
                auto_reload_enabled, auto_reload_threshold_micro_usd, auto_reload_amount_micro_usd \
           FROM extra_usage_settings WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal_error("extra usage settings", e),
    };
    let balance: i64 = match sqlx::query_scalar(
        "SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(r) => r.unwrap_or(0),
        Err(e) => return internal_error("extra usage balance", e),
    };
    let spent: i64 = match sqlx::query_scalar(
        "SELECT spent_micro_usd FROM extra_usage_periods \
          WHERE account_id = $1 AND period_start <= now() AND period_end > now() LIMIT 1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(r) => r.unwrap_or(0),
        Err(e) => return internal_error("extra usage period", e),
    };
    let trial = match load_summary_trial(&mut tx, account_id).await {
        Ok(trial) => trial,
        Err(response) => return response,
    };
    if let Err(e) = tx.commit().await {
        return internal_error("commit", e);
    }

    db_timing.finish(Outcome::Ok);

    let has_billing_account = sub_row.is_some();
    let customer_id = sub_row.as_ref().map(|r| r.0.clone());
    let subscription = match &sub_row {
        Some((_, sub_id, status, period_end, cancel_flag, cancel_at)) => SummarySubscription {
            // A customer row with no subscription yet is "no subscription".
            status: sub_id.as_ref().map(|_| status.clone()),
            has_billing_account,
            renews_at: *period_end,
            cancel_at_period_end: *cancel_flag,
            cancel_at: *cancel_at,
        },
        None => SummarySubscription {
            status: None,
            has_billing_account,
            renews_at: None,
            cancel_at_period_end: false,
            cancel_at: None,
        },
    };

    let allowance_total = plan.as_ref().map(|p| p.credits).unwrap_or(granted);
    let credits = SummaryCredits {
        allowance_used: used,
        allowance_total,
        resets_at: period_end.or(subscription.renews_at),
    };

    let frozen = crate::billing::enforcement::status_is_frozen(subscription.status.as_deref());
    let extra_usage = match settings {
        Some((enabled, limit, cap, reload, threshold, amount)) => SummaryExtraUsage {
            enabled,
            balance_micro_usd: balance,
            monthly_limit_micro_usd: limit,
            spent_this_period_micro_usd: spent,
            frozen,
            member_cap_seat_multiple: cap,
            auto_reload: SummaryAutoReload {
                enabled: reload,
                threshold_micro_usd: threshold,
                amount_micro_usd: amount,
            },
        },
        None => SummaryExtraUsage {
            enabled: false,
            balance_micro_usd: balance,
            monthly_limit_micro_usd: None,
            spent_this_period_micro_usd: spent,
            frozen,
            member_cap_seat_multiple: None,
            auto_reload: SummaryAutoReload { enabled: false, threshold_micro_usd: None, amount_micro_usd: None },
        },
    };

    let payment_method = match (state.stripe.as_ref(), customer_id) {
        (Some(stripe), Some(customer_id)) => {
            let mut provider_timing = Guard::start(Phase::BillingProvider);
            let result = tokio::time::timeout(CARD_LOOKUP_TIMEOUT, stripe.retrieve_customer_default_payment_method_summary(&customer_id)).await;
            provider_timing.finish(match &result {
                Ok(Ok(_)) => Outcome::Ok,
                Ok(Err(_)) => Outcome::Error,
                Err(_) => Outcome::Timeout,
            });
            match result {
                Ok(Ok(pm)) => pm.map(|c| SummaryCard {
                    kind: c.kind,
                    brand: c.brand,
                    last4: c.last4,
                    exp_month: c.exp_month,
                    exp_year: c.exp_year,
                    bank_name: c.bank_name,
                    email: c.email,
                }),
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, %account_id, "billing summary: payment method lookup failed, omitting it");
                    None
                }
                Err(_) => {
                    tracing::warn!(%account_id, "billing summary: payment method lookup timed out, omitting it");
                    None
                }
            }
        }
        _ => {
            crate::routes::bootstrap::measurement::skipped(Phase::BillingProvider);
            None
        },
    };

    let body = BillingSummary {
        account_kind: if is_org { "org" } else { "personal" }.to_string(),
        plan: plan.map(|p| SummaryPlan { code: p.code, name: p.name }),
        seats,
        credits,
        extra_usage,
        subscription,
        payment_method,
        trial,
    };
    (StatusCode::OK, Json(body)).into_response()
    }.await;
    // Early DB/authority returns retain their real status. If the bootstrap
    // deadline cancels this future, the guard instead records cancellation.
    db_timing.finish(Outcome::status(response.status()));
    response
}

// ── GET /v1/billing/seats/quote ─────────────────────────────────────────

fn default_tier() -> String {
    "standard".to_string()
}
fn default_action() -> String {
    "add".to_string()
}

#[derive(Debug, Deserialize)]
pub struct SeatQuoteQuery {
    /// Number of seats to add or remove, 1..=500.
    pub quantity: i64,
    /// `seat_tiers.code`. Default `standard`.
    #[serde(default = "default_tier")]
    pub tier: String,
    /// `add` (default) or `reduce`.
    #[serde(default = "default_action")]
    pub action: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SeatQuote {
    pub tier: String,
    pub action: String,
    pub quantity: i64,
    /// Seats of this tier on the subscription today.
    pub current_quantity: i64,
    pub new_quantity: i64,
    pub currency: String,
    /// What this change adds to the next invoice, in cents: the difference
    /// between the preview with the change and the preview without it.
    /// Already-pending charges are not included. Always 0 for `reduce`:
    /// reductions are not prorated or credited, the lower amount starts next
    /// period (`recurring_cents_after`).
    pub proration_cents: i64,
    /// Total of the next invoice if the change is made.
    pub next_invoice_total_cents: i64,
    /// What each full cycle costs after the change, in cents (excludes
    /// prorations).
    pub recurring_cents_after: i64,
    /// Next renewal (end of the current period).
    pub next_invoice_at: Option<DateTime<Utc>>,
}

pub async fn get_seat_quote(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Query(q): Query<SeatQuoteQuery>,
) -> Response {
    if !(MIN_QUOTE_QUANTITY..=MAX_QUOTE_QUANTITY).contains(&q.quantity) {
        return bad_request("quantity must be between 1 and 500");
    }
    let reduce = match q.action.as_str() {
        "add" => false,
        "reduce" => true,
        _ => return bad_request("action must be add or reduce"),
    };
    // org_only: a personal account has no seats to quote (400).
    if let Err(resp) = require_purchase_authority(&state, &user, true).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let Some(stripe) = state.stripe.as_ref() else {
        return service_unavailable();
    };
    let account_id = user.account_id;

    let tier: Option<(Uuid, String, Option<String>)> = match sqlx::query_as(
        "SELECT id, code, stripe_price_id FROM seat_tiers WHERE code = $1 AND is_active = true",
    )
    .bind(&q.tier)
    .fetch_optional(control)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal_error("seat tier lookup", e),
    };
    let Some((tier_id, tier_code, price_id)) = tier else {
        return bad_request("unknown or inactive seat tier");
    };
    let Some(price_id) = price_id else {
        return internal_error("tier price", format!("seat tier {tier_code} has no stripe_price_id"));
    };

    if crate::billing::enforcement::subscription_frozen(control, account_id).await {
        return conflict("subscription_inactive", "subscription inactive, seats cannot be changed while frozen");
    }

    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("scope", e),
    };
    let sub_row: Option<(String, Option<String>)> = match sqlx::query_as(
        "SELECT stripe_customer_id, stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal_error("subscription lookup", e),
    };
    let available = if reduce {
        match crate::billing::slot_pool::read_slot_pool_for_tier_ledger(&mut tx, account_id, tier_id).await {
            Ok(pool) => Some(pool.available()),
            Err(e) => return internal_error("slot pool", e),
        }
    } else {
        None
    };
    if let Err(e) = tx.commit().await {
        return internal_error("commit", e);
    }
    let Some((customer_id, Some(subscription_id))) = sub_row else {
        return conflict("no_subscription", "this account has no active team subscription");
    };
    if let Some(available) = available {
        if q.quantity > available {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "not enough free seats of this tier to remove",
                    "code": "seats_in_use",
                    "available": available,
                })),
            )
                .into_response();
        }
    }

    let sub = match with_stripe_deadline(stripe.retrieve_subscription(&subscription_id)).await {
        Ok(s) => s,
        Err(e) => return stripe_error("retrieve subscription", e),
    };
    let item = sub.items.iter().find(|it| it.price_id == price_id);
    let current_quantity = item.map(|i| i.quantity).unwrap_or(0);
    let new_quantity = if reduce { current_quantity - q.quantity } else { current_quantity + q.quantity };
    if new_quantity < 0 {
        return conflict("seats_in_use", "cannot remove more seats than the subscription has");
    }
    let change = match (item, reduce, new_quantity) {
        (Some(it), true, 0) => PreviewChange::RemoveItem { item_id: &it.id },
        (Some(it), _, n) => PreviewChange::SetQuantity { item_id: &it.id, quantity: n },
        (None, false, n) => PreviewChange::AddItem { price_id: &price_id, quantity: n },
        (None, true, _) => {
            return conflict("seats_in_use", "this subscription has no seats of that tier");
        }
    };
    let next_invoice_at = sub
        .items
        .iter()
        .map(|i| i.current_period_end)
        .min()
        .and_then(|s| DateTime::from_timestamp(s, 0));

    let proration_date = Utc::now().timestamp();
    let (baseline, changed) = match with_stripe_deadline(async {
        tokio::try_join!(
            stripe.create_preview_invoice(&customer_id, &subscription_id, None, proration_date, !reduce),
            stripe.create_preview_invoice(&customer_id, &subscription_id, Some(change), proration_date, !reduce),
        )
    })
    .await
    {
        Ok(pair) => pair,
        Err(e) => return stripe_error("preview invoice", e),
    };
    let recurring_cents_after: i64 = changed.lines.iter().filter(|l| !l.proration).map(|l| l.amount).sum();

    (
        StatusCode::OK,
        Json(SeatQuote {
            tier: tier_code,
            action: q.action,
            quantity: q.quantity,
            current_quantity,
            new_quantity,
            currency: changed.currency,
            proration_cents: changed.total - baseline.total,
            next_invoice_total_cents: changed.total,
            recurring_cents_after,
            next_invoice_at,
        }),
    )
        .into_response()
}

// ── Live-Postgres tests ─────────────────────────────────────────────────

#[cfg(test)]
mod pg_tests {
    use super::*;
    use crate::billing::test_support::*;
    use serde_json::{json, Value};

    const RETURN_URL: &str = "http://localhost:3000/settings/billing";

    fn state_with(pool: &sqlx::PgPool, mock: Option<&MockStripe>) -> Arc<AppState> {
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        if let Some(m) = mock {
            state.stripe = Some(m.client.clone());
        }
        Arc::new(state)
    }

    fn cust() -> String {
        format!("cus_{}", Uuid::new_v4().simple())
    }

    async fn portal(state: &Arc<AppState>, a: Uuid, u: Uuid, url: &str) -> (StatusCode, Value) {
        let resp = create_portal_session(
            State(state.clone()),
            Extension(user_context(a, u)),
            Json(PortalRequest { return_url: url.to_string() }),
        )
        .await;
        (resp.status(), body_json(resp).await)
    }

    fn portal_config_json() -> Value {
        json!({"data": [
            {"id": "bpc_other", "metadata": {}},
            {"id": "bpc_owned", "metadata": {"cs_portal_config": "v1"}},
        ], "has_more": false})
    }

    #[tokio::test]
    async fn portal_for_an_org_admin_uses_our_configuration_and_return_url() {
        let Some(pool) = pg_test_pool("portal_admin").await else { return };
        let (a, u) = seed_org_account(&pool, "admin").await;
        let customer = cust();
        seed_subscription(&pool, a, &customer, Some("sub_x"), "active").await;
        let mock = MockStripe::spawn().await;
        mock.respond("GET", "/v1/billing_portal/configurations", 200, portal_config_json());
        mock.respond("POST", "/v1/billing_portal/sessions", 200, json!({"id": "bps_1", "url": "https://billing.stripe.com/p/session/x"}));
        let state = state_with(&pool, Some(&mock));
        let (status, body) = portal(&state, a, u, RETURN_URL).await;
        let calls = mock.calls("POST", "/v1/billing_portal/sessions");
        cleanup_account(&pool, a, &[u]).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["url"], "https://billing.stripe.com/p/session/x");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].form["customer"], customer);
        assert_eq!(calls[0].form["configuration"], "bpc_owned");
        assert_eq!(calls[0].form["return_url"], RETURN_URL);
    }

    #[tokio::test]
    async fn portal_refuses_a_plain_member_and_creates_nothing() {
        let Some(pool) = pg_test_pool("portal_member").await else { return };
        let (a, owner) = seed_org_account(&pool, "owner").await;
        let member = add_org_member(&pool, a, "member").await;
        seed_subscription(&pool, a, &cust(), Some("sub_x"), "active").await;
        let mock = MockStripe::spawn().await;
        let state = state_with(&pool, Some(&mock));
        let (status, _) = portal(&state, a, member, RETURN_URL).await;
        let n = mock.requests().len();
        cleanup_account(&pool, a, &[owner, member]).await.unwrap();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn portal_works_for_a_personal_account_and_a_personal_org() {
        let Some(pool) = pg_test_pool("portal_personal").await else { return };
        let (a, u) = seed_personal_account(&pool).await;
        seed_subscription(&pool, a, &cust(), Some("sub_p"), "active").await;
        let (pa, pu, clerk_org) = seed_personal_org_account(&pool).await;
        seed_subscription(&pool, pa, &cust(), Some("sub_po"), "active").await;
        let mock = MockStripe::spawn().await;
        mock.respond("GET", "/v1/billing_portal/configurations", 200, portal_config_json());
        mock.respond("POST", "/v1/billing_portal/sessions", 200, json!({"id": "bps_1", "url": "https://billing.stripe.com/p/s"}));
        let state = state_with(&pool, Some(&mock));
        let (s1, _) = portal(&state, a, u, RETURN_URL).await;
        let (s2, _) = portal(&state, pa, pu, RETURN_URL).await;
        cleanup_account(&pool, a, &[u]).await.unwrap();
        cleanup_account(&pool, pa, &[pu]).await.unwrap();
        cleanup_personal_org(&pool, &clerk_org).await;
        assert_eq!((s1, s2), (StatusCode::OK, StatusCode::OK));
    }

    #[tokio::test]
    async fn portal_error_contract() {
        let Some(pool) = pg_test_pool("portal_errors").await else { return };
        let (a, u) = seed_personal_account(&pool).await;
        let mock = MockStripe::spawn().await;
        mock.respond("GET", "/v1/billing_portal/configurations", 200, json!({"data": [], "has_more": false}));
        let state = state_with(&pool, Some(&mock));

        // No customer yet: 409 no_billing_account.
        let (s_nocust, b_nocust) = portal(&state, a, u, RETURN_URL).await;
        seed_subscription(&pool, a, &cust(), Some("sub_p"), "active").await;
        // Foreign return URL: 400.
        let (s_foreign, _) = portal(&state, a, u, "https://evil.example.com/x").await;
        // Config not provisioned: 503 portal_not_configured, no session made.
        let (s_noconf, b_noconf) = portal(&state, a, u, RETURN_URL).await;
        let sessions = mock.calls("POST", "/v1/billing_portal/sessions").len();
        // Stripe not configured at all: 503.
        let (s_nostripe, _) = portal(&state_with(&pool, None), a, u, RETURN_URL).await;
        cleanup_account(&pool, a, &[u]).await.unwrap();
        assert_eq!(s_nocust, StatusCode::CONFLICT);
        assert_eq!(b_nocust["code"], "no_billing_account");
        assert_eq!(s_foreign, StatusCode::BAD_REQUEST);
        assert_eq!(s_noconf, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(b_noconf["code"], "portal_not_configured");
        assert_eq!(sessions, 0);
        assert_eq!(s_nostripe, StatusCode::SERVICE_UNAVAILABLE);
    }

    async fn summary(state: &Arc<AppState>, a: Uuid, u: Uuid) -> (StatusCode, Value) {
        let resp = get_billing_summary(State(state.clone()), Extension(user_context(a, u))).await;
        (resp.status(), body_json(resp).await)
    }

    #[tokio::test]
    async fn summary_for_a_personal_account_without_settings_reads_extra_usage_off() {
        let Some(pool) = pg_test_pool("summary_personal").await else { return };
        let (a, u) = seed_personal_account(&pool).await;
        let state = state_with(&pool, None);
        let (status, body) = summary(&state, a, u).await;
        cleanup_account(&pool, a, &[u]).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["account_kind"], "personal");
        assert_eq!(body["seats"], json!({"used": 1, "total": 1}));
        assert_eq!(body["extra_usage"]["enabled"], false);
        assert_eq!(body["extra_usage"]["balance_micro_usd"], 0);
        assert!(body["extra_usage"]["monthly_limit_micro_usd"].is_null());
        assert_eq!(body["subscription"]["has_billing_account"], false);
        assert!(body["subscription"]["status"].is_null());
        assert!(body["payment_method"].is_null());
    }

    #[tokio::test]
    async fn summary_for_an_org_admin_reports_plan_seats_credits_extra_usage_cancel_and_card() {
        let Some(pool) = pg_test_pool("summary_org").await else { return };
        let (a, u) = seed_org_account(&pool, "owner").await;
        let customer = cust();
        seed_subscription(&pool, a, &customer, Some("sub_o"), "active").await;
        let cancel_at = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        exec_fixture(
            &pool,
            a,
            sqlx::query("UPDATE stripe_subscriptions SET cancel_at_period_end = true, cancel_at = $2 WHERE account_id = $1")
                .bind(a)
                .bind(cancel_at),
        )
        .await
        .unwrap();
        // Allowance: 100 granted, 40 consumed in the current period.
        exec_fixture(
            &pool,
            a,
            sqlx::query(
                "INSERT INTO member_allowance_periods (account_id, user_id, period_start, period_end, granted_credits, consumed_credits) \
                 VALUES ($1, $2, now() - interval '2 days', now() + interval '28 days', 100, 40)",
            )
            .bind(a)
            .bind(u),
        )
        .await
        .unwrap();
        // Extra usage on, $100 limit, $12.50 balance, $3 spent.
        sqlx::query(
            "INSERT INTO extra_usage_settings (account_id, enabled, monthly_limit_micro_usd, member_cap_seat_multiple) \
             VALUES ($1, true, 100000000, 1)",
        )
        .bind(a)
        .execute(&pool)
        .await
        .unwrap();
        exec_fixture(
            &pool,
            a,
            sqlx::query("INSERT INTO extra_usage_balances (account_id, balance_micro_usd) VALUES ($1, 12500000)").bind(a),
        )
        .await
        .unwrap();
        exec_fixture(
            &pool,
            a,
            sqlx::query(
                "INSERT INTO extra_usage_periods (account_id, period_start, period_end, spent_micro_usd) \
                 VALUES ($1, now() - interval '2 days', now() + interval '28 days', 3000000)",
            )
            .bind(a),
        )
        .await
        .unwrap();

        let mock = MockStripe::spawn().await;
        mock.respond(
            "GET",
            &format!("/v1/customers/{customer}"),
            200,
            json!({"id": customer, "invoice_settings": {"default_payment_method": {
                "id": "pm_1", "card": {"brand": "visa", "last4": "4242", "exp_month": 12, "exp_year": 2030}}}}),
        );
        let state = state_with(&pool, Some(&mock));
        let (status, body) = summary(&state, a, u).await;
        cleanup_account(&pool, a, &[u]).await.unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["account_kind"], "org");
        assert_eq!(body["seats"]["total"], 0, "no seats purchased in this fixture");
        assert_eq!(body["credits"]["allowance_used"], 40.0);
        assert_eq!(body["credits"]["allowance_total"], 100.0, "no plan grant: falls back to the granted sum");
        assert!(body["credits"]["resets_at"].is_string());
        assert_eq!(body["extra_usage"]["enabled"], true);
        assert_eq!(body["extra_usage"]["balance_micro_usd"], 12_500_000);
        assert_eq!(body["extra_usage"]["monthly_limit_micro_usd"], 100_000_000);
        assert_eq!(body["extra_usage"]["spent_this_period_micro_usd"], 3_000_000);
        assert_eq!(body["extra_usage"]["frozen"], false);
        assert_eq!(body["subscription"]["status"], "active");
        assert_eq!(body["subscription"]["cancel_at_period_end"], true);
        assert_eq!(body["subscription"]["cancel_at"], "2027-01-15T08:00:00Z");
        assert_eq!(body["payment_method"]["brand"], "visa");
        assert_eq!(body["payment_method"]["last4"], "4242");
    }

    /// #1557: a trialing org. The mirror says `active` (Stripe's `trialing`
    /// is stored as `active`), and the trial facts come from `org_trials`:
    /// 1,200 of 3,000 credits used, the backstop date, the seat count and the
    /// price the card is charged afterwards.
    #[tokio::test]
    async fn summary_for_a_trialing_org_reports_the_trial_facts() {
        let Some(pool) = pg_test_pool("summary_trialing_org").await else { return };
        let (a, u) = seed_org_account(&pool, "owner").await;
        let sub = format!("sub_{}", Uuid::new_v4().simple());
        seed_subscription(
            &pool,
            a,
            &cust(),
            Some(&sub),
            crate::routes::stripe_webhook::map_subscription_status("trialing"),
        )
        .await;
        let ends = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        exec_fixture(
            &pool,
            a,
            sqlx::query(
                "INSERT INTO org_trials (account_id, state, creator_user_id, seats, trial_eligible, \
                        stripe_subscription_id, trial_credits, pool_baseline, trial_started_at, trial_ends_at) \
                 VALUES ($1, 'trialing', $2, 2, true, $4, 3000, 3000, now(), $3)",
            )
            .bind(a)
            .bind(u)
            .bind(ends)
            .bind(&sub),
        )
        .await
        .unwrap();
        exec_fixture(
            &pool,
            a,
            sqlx::query("INSERT INTO credit_ledger (account_id, entry_kind, credits) VALUES ($1, 'trial_credit', 3000)").bind(a),
        )
        .await
        .unwrap();
        exec_fixture(
            &pool,
            a,
            sqlx::query("INSERT INTO credit_balances (account_id, balance, updated_at) VALUES ($1, 1800, now())").bind(a),
        )
        .await
        .unwrap();
        let state = state_with(&pool, None);
        let (status, body) = summary(&state, a, u).await;
        let tier_cents: i32 = sqlx::query_scalar("SELECT price_usd_cents FROM seat_tiers WHERE code = 'standard'")
            .fetch_one(&pool)
            .await
            .unwrap();
        cleanup_account(&pool, a, &[u]).await.unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["subscription"]["status"], "active", "the mirror holds Stripe's trialing as active");
        let t = &body["trial"];
        assert_eq!(t["state"], "trialing");
        assert_eq!(t["seats"], 2);
        assert_eq!(t["credits_total"], 3000.0);
        assert_eq!(t["credits_used"], 1200.0);
        assert_eq!(t["ends_at"], "2027-01-15T08:00:00Z");
        assert_eq!(t["payment_failed"], false);
        assert_eq!(t["seat_price_usd_cents"], tier_cents);
        assert!(t["seat_monthly_credits"].as_f64().unwrap() > 0.0);
    }

    /// Mock the customer GET with `default` as the expanded default method
    /// (or none), and `GET /v1/payment_methods` answering per `type`.
    fn mock_payment_methods(mock: &MockStripe, customer: &str, default: Option<Value>, by_type: &[(&str, Value)]) {
        mock.respond(
            "GET",
            &format!("/v1/customers/{customer}"),
            200,
            json!({"id": customer, "invoice_settings": {"default_payment_method": default}}),
        );
        let by_type: Vec<(String, Value)> = by_type.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
        mock.respond_with("GET", "/v1/payment_methods", move |r| {
            let t = r.query.get("type").cloned().unwrap_or_default();
            let data: Vec<Value> = by_type.iter().filter(|(k, _)| *k == t).map(|(_, v)| v.clone()).collect();
            (200, json!({"data": data, "has_more": false}))
        });
    }

    async fn summary_payment_method(default: Option<Value>, by_type: &[(&str, Value)], tag: &str) -> Value {
        let Some(pool) = pg_test_pool(tag).await else { return Value::Null };
        let (a, u) = seed_personal_account(&pool).await;
        let customer = cust();
        seed_subscription(&pool, a, &customer, Some(&format!("sub_{}", Uuid::new_v4().simple())), "active").await;
        let mock = MockStripe::spawn().await;
        mock_payment_methods(&mock, &customer, default, by_type);
        let state = state_with(&pool, Some(&mock));
        let (status, body) = summary(&state, a, u).await;
        cleanup_account(&pool, a, &[u]).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        body["payment_method"].clone()
    }

    #[tokio::test]
    async fn summary_reports_a_us_bank_account_default_payment_method() {
        let bank = json!({"id": "pm_bank", "type": "us_bank_account",
            "us_bank_account": {"bank_name": "Example Bank", "last4": "4242"}});
        let pm = summary_payment_method(Some(bank), &[], "summary_pm_bank").await;
        if pm.is_null() {
            return; // no test database
        }
        assert_eq!(pm["type"], "us_bank_account");
        assert_eq!(pm["bank_name"], "Example Bank");
        assert_eq!(pm["last4"], "4242");
        assert!(pm.get("brand").is_none() && pm.get("exp_month").is_none());
    }

    #[tokio::test]
    async fn summary_reports_a_card_default_with_the_legacy_fields_and_a_type() {
        let card = json!({"id": "pm_card", "type": "card",
            "card": {"brand": "visa", "last4": "4242", "exp_month": 12, "exp_year": 2030}});
        let pm = summary_payment_method(Some(card), &[], "summary_pm_card").await;
        if pm.is_null() {
            return;
        }
        assert_eq!(pm["type"], "card");
        assert_eq!(pm["brand"], "visa");
        assert_eq!(pm["last4"], "4242");
        assert_eq!(pm["exp_month"], 12);
        assert_eq!(pm["exp_year"], 2030);
        assert!(pm.get("bank_name").is_none());
    }

    #[tokio::test]
    async fn summary_reports_a_link_default_without_a_last4() {
        let link = json!({"id": "pm_link", "type": "link", "link": {"email": "a@example.com"}});
        let pm = summary_payment_method(Some(link), &[], "summary_pm_link").await;
        if pm.is_null() {
            return;
        }
        assert_eq!(pm["type"], "link");
        assert_eq!(pm["email"], "a@example.com");
        assert!(pm.get("last4").is_none());
    }

    /// The real purchase: the customer has no invoice default, and the only
    /// method on file is a bank account. A card-only listing found nothing.
    #[tokio::test]
    async fn summary_falls_back_to_a_bank_account_when_no_default_is_set() {
        let bank = json!({"id": "pm_bank", "type": "us_bank_account",
            "us_bank_account": {"bank_name": "Example Bank", "last4": "4242"}});
        let pm = summary_payment_method(None, &[("us_bank_account", bank)], "summary_pm_fallback_bank").await;
        if pm.is_null() {
            return;
        }
        assert_eq!(pm["type"], "us_bank_account");
        assert_eq!(pm["last4"], "4242");
    }

    #[tokio::test]
    async fn summary_prefers_a_listed_card_over_a_listed_bank_when_no_default_is_set() {
        let bank = json!({"id": "pm_bank", "type": "us_bank_account", "us_bank_account": {"bank_name": "Example Bank", "last4": "4242"}});
        let card = json!({"id": "pm_card", "type": "card", "card": {"brand": "visa", "last4": "4242", "exp_month": 1, "exp_year": 2031}});
        let pm = summary_payment_method(None, &[("us_bank_account", bank), ("card", card)], "summary_pm_prefers_card").await;
        if pm.is_null() {
            return;
        }
        assert_eq!(pm["type"], "card");
        assert_eq!(pm["last4"], "4242");
    }

    #[tokio::test]
    async fn summary_reports_extra_usage_frozen_exactly_when_canceled_or_past_due() {
        let Some(pool) = pg_test_pool("summary_frozen").await else { return };
        let (a, u) = seed_personal_account(&pool).await;
        let state = state_with(&pool, None);
        let mut seen = Vec::new();
        // No subscription at all: nothing to be frozen from.
        seen.push(("none", summary(&state, a, u).await.1["extra_usage"]["frozen"].clone()));
        for status in ["active", "past_due", "canceled", "active"] {
            seed_subscription(&pool, a, &format!("cus_frz_{}", a.simple()), Some("sub_frz"), status).await;
            seen.push((status, summary(&state, a, u).await.1["extra_usage"]["frozen"].clone()));
        }
        cleanup_account(&pool, a, &[u]).await.unwrap();
        assert_eq!(
            seen,
            vec![
                ("none", json!(false)),
                ("active", json!(false)),
                ("past_due", json!(true)),
                ("canceled", json!(true)),
                ("active", json!(false)),
            ]
        );
    }

    #[tokio::test]
    async fn summary_refuses_a_member_and_survives_a_stripe_outage() {
        let Some(pool) = pg_test_pool("summary_member").await else { return };
        let (a, owner) = seed_org_account(&pool, "owner").await;
        let member = add_org_member(&pool, a, "member").await;
        seed_subscription(&pool, a, &cust(), Some("sub_o"), "active").await;
        let mock = MockStripe::spawn().await; // no routes: every Stripe call 404s
        let state = state_with(&pool, Some(&mock));
        let (s_member, _) = summary(&state, a, member).await;
        let (s_admin, body) = summary(&state, a, owner).await;
        cleanup_account(&pool, a, &[owner, member]).await.unwrap();
        assert_eq!(s_member, StatusCode::FORBIDDEN);
        assert_eq!(s_admin, StatusCode::OK, "a card lookup failure must not fail the summary");
        assert!(body["payment_method"].is_null());
    }

    async fn quote(state: &Arc<AppState>, a: Uuid, u: Uuid, query: SeatQuoteQuery) -> (StatusCode, Value) {
        let resp = get_seat_quote(State(state.clone()), Extension(user_context(a, u)), Query(query)).await;
        (resp.status(), body_json(resp).await)
    }
    fn qq(quantity: i64, tier: &str, action: &str) -> SeatQuoteQuery {
        SeatQuoteQuery { quantity, tier: tier.into(), action: action.into() }
    }
    fn preview_json(total: i64, lines: &[(i64, bool)]) -> Value {
        let data: Vec<Value> = lines
            .iter()
            .map(|(amount, proration)| json!({"amount": amount, "parent": {"subscription_item_details": {"proration": proration}}}))
            .collect();
        json!({"total": total, "amount_due": total, "currency": "usd", "lines": {"data": data}})
    }

    #[tokio::test]
    async fn seat_quote_add_diffs_the_preview_against_the_baseline() {
        let Some(pool) = pg_test_pool("quote_add").await else { return };
        let _g = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
        let prev = crate::pg_catalog_test_support::set_seat_tier_price(&pool, "standard", Some("price_std_q")).await;
        let (a, u) = seed_org_account(&pool, "admin").await;
        let customer = cust();
        let sub = format!("sub_{}", Uuid::new_v4().simple());
        seed_subscription(&pool, a, &customer, Some(&sub), "active").await;
        let mock = MockStripe::spawn().await;
        mock.respond("GET", &format!("/v1/subscriptions/{sub}"), 200,
            subscription_json(&sub, "active", &[("si_std", "price_std_q", 4)], false, None, None));
        // Baseline (no change) $40.00; with the change $50.00, of which $10.00 is proration.
        mock.respond_with("POST", "/v1/invoices/create_preview", |r| {
            if r.form.contains_key("subscription_details[items][0][id]") {
                (200, preview_json(5000, &[(4000, false), (1000, true)]))
            } else {
                (200, preview_json(4000, &[(4000, false)]))
            }
        });
        let state = state_with(&pool, Some(&mock));
        let (status, body) = quote(&state, a, u, qq(3, "standard", "add")).await;
        let previews = mock.calls("POST", "/v1/invoices/create_preview");
        crate::pg_catalog_test_support::set_seat_tier_price(&pool, "standard", prev.as_deref()).await;
        cleanup_account(&pool, a, &[u]).await.unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["current_quantity"], 4);
        assert_eq!(body["new_quantity"], 7);
        assert_eq!(body["currency"], "usd");
        assert_eq!(body["next_invoice_total_cents"], 5000);
        assert_eq!(body["proration_cents"], 1000);
        assert_eq!(body["recurring_cents_after"], 4000);
        assert_eq!(previews.len(), 2);
        let changed: Vec<_> = previews.iter().filter(|r| r.form.contains_key("subscription_details[items][0][id]")).collect();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].form["subscription_details[items][0][id]"], "si_std");
        assert_eq!(changed[0].form["subscription_details[items][0][quantity]"], "7");
        assert_eq!(changed[0].form["customer"], customer);
        assert_eq!(changed[0].form["subscription_details[proration_behavior]"], "create_prorations");
    }

    #[tokio::test]
    async fn seat_quote_reduce_previews_without_proration_so_no_credit_is_promised() {
        let Some(pool) = pg_test_pool("quote_reduce").await else { return };
        let _g = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
        let prev = crate::pg_catalog_test_support::set_seat_tier_price(&pool, "standard", Some("price_std_q")).await;
        let (a, u) = seed_org_account(&pool, "owner").await;
        let sub = format!("sub_{}", Uuid::new_v4().simple());
        seed_subscription(&pool, a, &cust(), Some(&sub), "active").await;
        // 10 purchased standard seats in the ledger, none in use.
        let team_plan: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'").fetch_one(&pool).await.unwrap();
        let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard'").fetch_one(&pool).await.unwrap();
        exec_fixture(&pool, a, sqlx::query("INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) VALUES ($1, $2, 10, true)").bind(a).bind(team_plan)).await.unwrap();
        let grant_id: Uuid = sqlx::query_scalar("SELECT id FROM grants WHERE account_id = $1").bind(a).fetch_one(&pool).await.unwrap();
        exec_fixture(&pool, a, sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 10)").bind(grant_id).bind(tier_id)).await.unwrap();

        let mock = MockStripe::spawn().await;
        mock.respond("GET", &format!("/v1/subscriptions/{sub}"), 200,
            subscription_json(&sub, "active", &[("si_std", "price_std_q", 10)], false, None, None));
        // With `none` Stripe shows the same next invoice (no credit line) but
        // the lower recurring amount; with create_prorations it would show a
        // credit, which the real reduction never gives.
        mock.respond_with("POST", "/v1/invoices/create_preview", |r| {
            let none = r.form.get("subscription_details[proration_behavior]").map(String::as_str) == Some("none");
            if r.form.contains_key("subscription_details[items][0][id]") {
                if none {
                    (200, preview_json(8000, &[(8000, false)]))
                } else {
                    (200, preview_json(6000, &[(8000, false), (-2000, true)]))
                }
            } else {
                (200, preview_json(8000, &[(8000, false)]))
            }
        });
        let state = state_with(&pool, Some(&mock));
        let (status, body) = quote(&state, a, u, qq(2, "standard", "reduce")).await;
        let previews = mock.calls("POST", "/v1/invoices/create_preview");
        crate::pg_catalog_test_support::set_seat_tier_price(&pool, "standard", prev.as_deref()).await;
        cleanup_account(&pool, a, &[u]).await.unwrap();

        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["current_quantity"], 10);
        assert_eq!(body["new_quantity"], 8);
        assert_eq!(body["proration_cents"], 0, "no credit for a reduction");
        assert_eq!(body["recurring_cents_after"], 8000);
        assert!(previews.iter().all(|r| r.form.get("subscription_details[proration_behavior]").map(String::as_str).unwrap_or("none") == "none"));
        assert!(previews.iter().all(|r| !r.form.contains_key("subscription_details[proration_date]")));
    }

    #[tokio::test]
    async fn seat_quote_error_contract() {
        let Some(pool) = pg_test_pool("quote_errors").await else { return };
        let _g = crate::pg_catalog_test_support::CATALOG_PRICE_LOCK.lock().await;
        let prev = crate::pg_catalog_test_support::set_seat_tier_price(&pool, "standard", Some("price_std_q")).await;
        let (a, owner) = seed_org_account(&pool, "owner").await;
        let member = add_org_member(&pool, a, "member").await;
        let mock = MockStripe::spawn().await;
        let state = state_with(&pool, Some(&mock));

        let (s_member, _) = quote(&state, a, member, qq(1, "standard", "add")).await;
        let (s_zero, _) = quote(&state, a, owner, qq(0, "standard", "add")).await;
        let (s_big, _) = quote(&state, a, owner, qq(501, "standard", "add")).await;
        let (s_action, _) = quote(&state, a, owner, qq(1, "standard", "swap")).await;
        let (s_tier, _) = quote(&state, a, owner, qq(1, "nope", "add")).await;
        let (s_nosub, b_nosub) = quote(&state, a, owner, qq(1, "standard", "add")).await;

        // Reduce more than the free seats: 409 seats_in_use.
        let sub = format!("sub_{}", Uuid::new_v4().simple());
        seed_subscription(&pool, a, &cust(), Some(&sub), "active").await;
        let (s_reduce, b_reduce) = quote(&state, a, owner, qq(1, "standard", "reduce")).await;

        // Personal account: 400.
        let (pa, pu) = seed_personal_account(&pool).await;
        let (s_personal, _) = quote(&state, pa, pu, qq(1, "standard", "add")).await;
        let n = mock.requests().len();

        crate::pg_catalog_test_support::set_seat_tier_price(&pool, "standard", prev.as_deref()).await;
        cleanup_account(&pool, a, &[owner, member]).await.unwrap();
        cleanup_account(&pool, pa, &[pu]).await.unwrap();

        assert_eq!(s_member, StatusCode::FORBIDDEN);
        assert_eq!(s_zero, StatusCode::BAD_REQUEST);
        assert_eq!(s_big, StatusCode::BAD_REQUEST);
        assert_eq!(s_action, StatusCode::BAD_REQUEST);
        assert_eq!(s_tier, StatusCode::BAD_REQUEST);
        assert_eq!(s_nosub, StatusCode::CONFLICT);
        assert_eq!(b_nosub["code"], "no_subscription");
        assert_eq!(s_reduce, StatusCode::CONFLICT);
        assert_eq!(b_reduce["code"], "seats_in_use");
        assert_eq!(s_personal, StatusCode::BAD_REQUEST);
        assert_eq!(n, 0, "no Stripe call is made for a refused quote");
    }
}
