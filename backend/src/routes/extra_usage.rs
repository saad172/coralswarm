//! Extra-usage settings and top-up (settings launch plan PR 3.4, decisions
//! 11-13 and 19):
//!
//! * `PUT  /v1/billing/extra-usage`: turn extra usage on or off, set the
//!   monthly limit, the org per-member cap and auto-reload.
//! * `POST /v1/billing/extra-usage/topup`: a Stripe Checkout URL that adds to
//!   the prepaid balance. The balance is credited by the Checkout webhook
//!   (`billing::extra_usage::webhook`), never by this route.
//!
//! Reads come from `GET /v1/billing/summary` (PR 3.3), whose `extra_usage`
//! block already carries the same fields; the PUT response repeats the stored
//! settings so the page can update without a second round trip.
//!
//! # Who may call
//!
//! The same bar as checkout: an org account needs `owner`/`admin` (403
//! `admin_required` otherwise); a personal account (plain or personal org) is
//! always its own user. Free accounts get 403 `plan_required`: extra usage is
//! a paid-plan feature (decision 13).
//!
//! # Errors
//!
//! Every refusal is `{"error": <message>, "code": <stable code>}`. Codes:
//! `admin_required`, `plan_required`, `credits_frozen`, `extra_usage_off`,
//! and the 400 validation codes of [`crate::billing::extra_usage::Invalid`].
//!
//! # Top-up refusals (decision 19, and not taking money for nothing)
//!
//! A top-up is refused, rather than accepted-but-frozen, when the balance
//! could not be spent: credits frozen by a cancellation (409
//! `credits_frozen`; they return on resubscribe), a Free plan (403
//! `plan_required`), or extra usage off (409 `extra_usage_off`). Same order
//! as `FundingPolicy`. The refusal is checked again by the funding policy at
//! spend time, so a top-up that raced a cancellation is credited but stays
//! frozen.
//!
//! # Amounts
//!
//! The client sends an amount in micro-USD inside server-side bounds
//! ($5..$1,000, whole cents); it never sends a price. The session is an
//! ad-hoc card payment that saves the card for off-session use (auto-reload).

use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::types::Decimal;

use crate::billing::extra_usage::{
    resolve_update, topup_checkout_params, topup_idempotency_key, validate_topup_amount, AccountKind, Invalid,
    StoredSettings, UpdateRequest, CENT_MICRO_USD, LIMIT_MAX_MICRO_USD, LIMIT_MIN_MICRO_USD,
    RELOAD_THRESHOLD_MIN_MICRO_USD, TOPUP_MAX_MICRO_USD, TOPUP_MIN_MICRO_USD,
};
use crate::billing::funding_policy::db::{load_extra_shared, load_frozen};
use crate::middleware::auth::UserContext;
use crate::AppState;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopupRequest {
    /// Micro-USD (1 USD = 1_000_000), $5..$1,000, whole cents.
    pub amount_micro_usd: i64,
    pub success_url: String,
    pub cancel_url: String,
}

fn refuse(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({"error": message, "code": code}))).into_response()
}

fn invalid(i: Invalid) -> Response {
    refuse(StatusCode::BAD_REQUEST, i.code, &i.message)
}

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

fn internal(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "extra usage: failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

/// Decision #6 (self-serve PR C′): a new team org's trial runs on its free
/// credits only. Extra usage, top-ups, packs and auto-reload unlock with the
/// first successful charge.
fn trial_active() -> Response {
    refuse(
        StatusCode::CONFLICT,
        "trial_active",
        "Extra usage turns on after your free trial ends and your first payment goes through.",
    )
}

fn plan_required() -> Response {
    refuse(
        StatusCode::FORBIDDEN,
        "plan_required",
        "Extra usage is available on paid plans. Upgrade to turn it on.",
    )
}

/// Org: `owner`/`admin`. Personal (plain or personal org): its own user.
async fn authorize(state: &AppState, user: &UserContext) -> Result<(), Response> {
    let Some(store) = state.control_store() else {
        return Err(unavailable());
    };
    let is_real_org = store
        .account_is_real_org(user.account_id, user.user_id)
        .await
        .map_err(|e| internal("account lookup", e))?;
    if !is_real_org {
        return Ok(());
    }
    match store.get_account_member_role(user.account_id, user.user_id).await {
        Ok(role) if matches!(role.as_deref(), Some("owner") | Some("admin")) => Ok(()),
        Ok(_) => Err(refuse(
            StatusCode::FORBIDDEN,
            "admin_required",
            "Only an organization admin can change extra usage.",
        )),
        Err(e) => Err(internal("role check", e)),
    }
}

fn limits() -> Value {
    json!({
        "topup_min_micro_usd": TOPUP_MIN_MICRO_USD,
        "topup_max_micro_usd": TOPUP_MAX_MICRO_USD,
        "monthly_limit_min_micro_usd": LIMIT_MIN_MICRO_USD,
        "monthly_limit_max_micro_usd": LIMIT_MAX_MICRO_USD,
        "auto_reload_threshold_min_micro_usd": RELOAD_THRESHOLD_MIN_MICRO_USD,
        "auto_reload_amount_min_micro_usd": TOPUP_MIN_MICRO_USD,
        "auto_reload_amount_max_micro_usd": TOPUP_MAX_MICRO_USD,
        "amount_step_micro_usd": CENT_MICRO_USD,
    })
}

fn cap_json(c: Option<Decimal>) -> Value {
    use std::str::FromStr;
    c.and_then(|d| f64::from_str(&d.to_string()).ok()).map(Value::from).unwrap_or(Value::Null)
}

/// Whether the account's default payment method can be charged by auto-reload.
/// `true` when it is a card, and also when there is nothing to judge (no
/// Stripe client configured, no Stripe customer yet, no method on file): the
/// reload run already skips an account with no method, and the refusal is for
/// the one case it can never succeed. A Stripe failure is a 503, never a
/// guess, because this gates a money-affecting setting.
async fn default_payment_method_is_card(state: &AppState, account: uuid::Uuid) -> Result<bool, Response> {
    let Some(stripe) = state.stripe.as_ref() else {
        return Ok(true);
    };
    let Some(control) = state.control_db() else {
        return Err(unavailable());
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| internal("scope", e))?;
    let customer: Option<String> =
        sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| internal("customer read", e))?;
    tx.commit().await.map_err(|e| internal("commit", e))?;
    let Some(customer) = customer else {
        return Ok(true);
    };
    match stripe.retrieve_customer_default_payment_method_summary(&customer).await {
        Ok(pm) => Ok(pm.map_or(true, |pm| pm.is_card())),
        Err(e) => {
            tracing::error!(error = %e, %account, "extra usage: default payment method lookup failed");
            Err(unavailable())
        }
    }
}

type SettingsRow = (bool, Option<i64>, Option<Decimal>, bool, Option<i64>, Option<i64>);

/// `PUT /v1/billing/extra-usage`. Body: [`UpdateRequest`]. 200 with
/// `{extra_usage: {...}, limits: {...}}`.
pub async fn put_extra_usage(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<UpdateRequest>,
) -> Response {
    if let Err(resp) = authorize(&state, &user).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let account = user.account_id;
    // Auto-reload charges the default payment method off-session as a card
    // PaymentIntent. Looked up BEFORE the transaction: it is a network call and
    // the transaction holds the account's financial lock. Only needed when the
    // request asks for it on; whether that is a real off -> on change is
    // decided below, once the stored settings are read.
    let default_is_card = if body.auto_reload.as_ref().is_some_and(|r| r.enabled) {
        match default_payment_method_is_card(&state, account).await {
            Ok(ok) => ok,
            Err(resp) => return resp,
        }
    } else {
        true
    };
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account).await {
        Ok(tx) => tx,
        Err(e) => return internal("scope", e),
    };
    // The account's financial lock serialises concurrent PUTs (the merge below
    // is read-modify-write) and settlement against the settings they read.
    if let Err(e) = crate::billing::renewal::lock_period_rotation(&mut tx, account).await {
        return internal("lock", e);
    }
    let shared = match load_extra_shared(&mut tx, account).await {
        Ok(s) => s,
        Err(e) => return internal("account facts", e),
    };
    let current: Option<SettingsRow> = match sqlx::query_as(
        "SELECT enabled, monthly_limit_micro_usd, member_cap_seat_multiple, auto_reload_enabled, \
                auto_reload_threshold_micro_usd, auto_reload_amount_micro_usd \
           FROM extra_usage_settings WHERE account_id = $1",
    )
    .bind(account)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal("settings read", e),
    };
    // A Free account cannot turn extra usage on. One that downgraded with it
    // on can still turn it off.
    if shared.plan_is_free && !(current.is_some() && !body.enabled) {
        return plan_required();
    }
    // Turning it off (or saving it off) stays allowed; nothing may switch
    // extra usage or auto-reload on before the trial's first charge.
    if shared.trial_refuses_extra && (body.enabled || body.auto_reload.as_ref().is_some_and(|r| r.enabled)) {
        return trial_active();
    }
    let current = current.map(|(enabled, limit, cap, reload, threshold, amount)| StoredSettings {
        enabled,
        monthly_limit_micro_usd: limit,
        member_cap_seat_multiple: cap,
        auto_reload_enabled: reload,
        auto_reload_threshold_micro_usd: threshold,
        auto_reload_amount_micro_usd: amount,
    });
    let kind = if shared.is_team_org { AccountKind::TeamOrg } else { AccountKind::Personal };
    let next = match resolve_update(kind, current.as_ref(), &body) {
        Ok(n) => n,
        Err(i) => return invalid(i),
    };
    // Turning auto-reload ON needs a card as the default payment method. A
    // bank account or Link method cannot be charged off-session by the card
    // PaymentIntent the reload makes, so the setting would be on and never
    // fire. An already-enabled setting is left alone (existing subscribers
    // keep working and can still edit the other fields).
    if next.auto_reload_enabled && !current.as_ref().is_some_and(|c| c.auto_reload_enabled) && !default_is_card {
        return refuse(
            StatusCode::CONFLICT,
            "auto_reload_requires_card",
            "Auto-reload can only charge a card, and your default payment method is not one. \
             Add a card in Manage billing, then turn auto-reload on.",
        );
    }
    // A save that changes nothing must not touch the row: `updated_at` means
    // "last real change".
    if current.as_ref() != Some(&next) {
        if let Err(e) = sqlx::query(
            "INSERT INTO extra_usage_settings \
                (account_id, enabled, monthly_limit_micro_usd, member_cap_seat_multiple, auto_reload_enabled, \
                 auto_reload_threshold_micro_usd, auto_reload_amount_micro_usd, updated_by_user_id, \
                 auto_reload_reset_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8, CASE WHEN $5 THEN now() END) \
             ON CONFLICT (account_id) DO UPDATE SET \
                enabled = EXCLUDED.enabled, \
                monthly_limit_micro_usd = EXCLUDED.monthly_limit_micro_usd, \
                member_cap_seat_multiple = EXCLUDED.member_cap_seat_multiple, \
                auto_reload_enabled = EXCLUDED.auto_reload_enabled, \
                auto_reload_threshold_micro_usd = EXCLUDED.auto_reload_threshold_micro_usd, \
                auto_reload_amount_micro_usd = EXCLUDED.auto_reload_amount_micro_usd, \
                updated_by_user_id = EXCLUDED.updated_by_user_id, \
                auto_reload_reset_at = CASE \
                    WHEN EXCLUDED.auto_reload_enabled AND NOT extra_usage_settings.auto_reload_enabled THEN now() \
                    ELSE extra_usage_settings.auto_reload_reset_at END, \
                updated_at = now()",
        )
        .bind(account)
        .bind(next.enabled)
        .bind(next.monthly_limit_micro_usd)
        .bind(next.member_cap_seat_multiple)
        .bind(next.auto_reload_enabled)
        .bind(next.auto_reload_threshold_micro_usd)
        .bind(next.auto_reload_amount_micro_usd)
        .bind(user.user_id)
        .execute(&mut *tx)
        .await
        {
            return internal("settings write", e);
        }
    }
    if let Err(e) = tx.commit().await {
        return internal("commit", e);
    }
    // Audit trail for a money-affecting setting.
    tracing::info!(
        %account, actor = %user.user_id,
        was = ?current, now = ?next,
        "extra usage: settings changed"
    );
    let body = json!({
        "extra_usage": {
            "enabled": next.enabled,
            "balance_micro_usd": shared.balance_micro_usd,
            "monthly_limit_micro_usd": next.monthly_limit_micro_usd,
            "member_cap_seat_multiple": cap_json(next.member_cap_seat_multiple),
            "auto_reload": {
                "enabled": next.auto_reload_enabled,
                "threshold_micro_usd": next.auto_reload_threshold_micro_usd,
                "amount_micro_usd": next.auto_reload_amount_micro_usd,
            },
        },
        "limits": limits(),
    });
    (StatusCode::OK, Json(body)).into_response()
}

/// Refuse a purchase whose balance could not be spent: credits frozen by a
/// cancellation (409 `credits_frozen`), a Free plan (403 `plan_required`) or
/// extra usage off (409 `extra_usage_off`). Same order as `FundingPolicy`.
/// Shared by the top-up and the credit-pack checkout, which both land in the
/// extra-usage balance.
pub(crate) async fn refuse_if_balance_unspendable(
    state: &AppState,
    account: uuid::Uuid,
) -> Result<(), Response> {
    let Some(control) = state.control_db() else {
        return Err(unavailable());
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| internal("scope", e))?;
    let facts = async {
        let shared = load_extra_shared(&mut tx, account).await?;
        let frozen = load_frozen(&mut tx, account).await?;
        Ok::<_, sqlx::Error>((shared, frozen))
    }
    .await;
    let (shared, frozen) = facts.map_err(|e| internal("account facts", e))?;
    tx.commit().await.map_err(|e| internal("commit", e))?;
    if frozen {
        return Err(refuse(
            StatusCode::CONFLICT,
            "credits_frozen",
            "Your subscription is inactive, so extra-usage credits are frozen. Resubscribe to add funds.",
        ));
    }
    if shared.plan_is_free {
        return Err(plan_required());
    }
    if shared.trial_refuses_extra {
        return Err(trial_active());
    }
    if !shared.settings.as_ref().is_some_and(|s| s.enabled) {
        return Err(refuse(
            StatusCode::CONFLICT,
            "extra_usage_off",
            "Turn on extra usage before adding funds.",
        ));
    }
    Ok(())
}

/// `POST /v1/billing/extra-usage/topup`. Body: [`TopupRequest`]. 200 with
/// `{checkout_url, session_id}`; redirect the browser to `checkout_url`.
pub async fn create_topup(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<TopupRequest>,
) -> Response {
    if body.success_url.trim().is_empty() || body.cancel_url.trim().is_empty() {
        return refuse(StatusCode::BAD_REQUEST, "return_urls_required", "success_url and cancel_url are required");
    }
    if let Err(resp) = authorize(&state, &user).await {
        return resp;
    }
    if let Err(i) = validate_topup_amount(body.amount_micro_usd) {
        return invalid(i);
    }
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let account = user.account_id;

    if let Err(resp) = refuse_if_balance_unspendable(&state, account).await {
        return resp;
    }

    if let Err(resp) = crate::routes::checkout::validate_return_urls(
        &state,
        control,
        account,
        &body.success_url,
        &body.cancel_url,
    )
    .await
    {
        return resp;
    }
    let customer_id = match crate::routes::checkout::ensure_stripe_customer(&state, account).await {
        Ok(id) => id,
        Err(resp) => return resp,
    };
    let Some(stripe) = state.stripe.as_ref() else {
        return unavailable();
    };
    let params = topup_checkout_params(
        &customer_id,
        &account.to_string(),
        body.amount_micro_usd,
        &body.success_url,
        &body.cancel_url,
    );
    let key = topup_idempotency_key(
        account,
        body.amount_micro_usd,
        &body.success_url,
        &body.cancel_url,
        chrono::Utc::now().timestamp(),
    );
    match stripe.create_checkout_session_topup(&params, &key).await {
        Ok(session) => (
            StatusCode::OK,
            Json(json!({"checkout_url": session.url, "session_id": session.id})),
        )
            .into_response(),
        Err(e) => internal("stripe create checkout session", e),
    }
}

#[cfg(test)]
pub(crate) mod pg_tests;
