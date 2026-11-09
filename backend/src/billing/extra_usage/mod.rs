//! Extra usage: settings validation, top-up Checkout, and crediting the
//! extra-usage ledger (settings launch plan PR 3.4, decisions 11-13, 18-20).
//!
//! # What lives here
//!
//! * [`resolve_update`]: the PURE rules for `PUT /v1/billing/extra-usage`.
//!   Current settings plus a request in, the settings to store (or a typed
//!   [`Invalid`]) out. No I/O, so the whole validation matrix is unit tested.
//! * [`validate_topup_amount`] and [`topup_checkout_params`]: what a top-up
//!   may cost and the exact Stripe form it creates.
//! * [`credit_in_tx`]: the ONE writer that adds money to
//!   `extra_usage_ledger` + `extra_usage_balances`. The Checkout webhook
//!   ([`webhook`]) uses it; the auto-reload worker (3.5) will too.
//!
//! # Money rules
//!
//! * Every amount is integer micro-USD (1 USD = 1_000_000). Anything the
//!   customer is CHARGED (top-up, reload amount) must be a whole number of
//!   cents, so it converts to Stripe's `unit_amount` exactly.
//! * The client never names a price, only an amount inside server-side
//!   bounds ([`TOPUP_MIN_MICRO_USD`]..=[`TOPUP_MAX_MICRO_USD`]). What gets
//!   CREDITED is what Stripe says was paid (`amount_total`), never a number
//!   from the request or from metadata.
//! * Crediting is idempotent on three independent keys (checkout session id,
//!   payment intent id, Stripe event id), all unique on the ledger.

use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{types::Decimal, PgConnection};
use uuid::Uuid;

use crate::billing::stripe_client::{CheckoutSession, Idem, Params, StripeClient, StripeError};

pub(crate) mod reload;
pub(crate) mod webhook;

#[cfg(test)]
mod pg_tests;
#[cfg(test)]
mod tests;

pub const MICRO_PER_USD: i64 = 1_000_000;
/// One cent in micro-USD. Charged amounts are multiples of this.
pub const CENT_MICRO_USD: i64 = 10_000;

/// A single top-up: $5 to $1,000.
pub const TOPUP_MIN_MICRO_USD: i64 = 5 * MICRO_PER_USD;
pub const TOPUP_MAX_MICRO_USD: i64 = 1_000 * MICRO_PER_USD;
/// The monthly limit: $1 to $100,000.
pub const LIMIT_MIN_MICRO_USD: i64 = MICRO_PER_USD;
pub const LIMIT_MAX_MICRO_USD: i64 = 100_000 * MICRO_PER_USD;
/// Org default limit (decision 12) and Pro default limit (decision 13).
pub const DEFAULT_ORG_LIMIT_MICRO_USD: i64 = 100 * MICRO_PER_USD;
pub const DEFAULT_PRO_LIMIT_MICRO_USD: i64 = 50 * MICRO_PER_USD;
/// Auto-reload fires below this balance; at least $1.
pub const RELOAD_THRESHOLD_MIN_MICRO_USD: i64 = MICRO_PER_USD;
/// Per-member cap as a multiple of the seat allowance (decision 12: default 1x).
pub const DEFAULT_MEMBER_CAP_MULTIPLE: i64 = 1;

/// `metadata[purpose]` on a top-up Checkout session (and its PaymentIntent).
/// The webhook routes on this: a payment-mode session without it is a legacy
/// credit-pack purchase.
pub const TOPUP_PURPOSE: &str = "extra_usage_topup";

/// A rejected request: a stable machine `code` (the web picks copy from it)
/// and a human `message`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invalid {
    pub code: &'static str,
    pub message: String,
}

impl Invalid {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Invalid { code, message: message.into() }
    }
}

/// Which kind of account the settings belong to. Only a team org has a
/// per-member cap and an org-sized default limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountKind {
    TeamOrg,
    /// A personal account, including a personal org: one person's Pro account.
    Personal,
}

/// `extra_usage_settings` as stored.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredSettings {
    pub enabled: bool,
    pub monthly_limit_micro_usd: Option<i64>,
    pub member_cap_seat_multiple: Option<Decimal>,
    pub auto_reload_enabled: bool,
    pub auto_reload_threshold_micro_usd: Option<i64>,
    pub auto_reload_amount_micro_usd: Option<i64>,
}

/// The `PUT /v1/billing/extra-usage` body. `enabled` is required; every other
/// field that is omitted keeps its stored value (or its default on first
/// enable).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateRequest {
    pub enabled: bool,
    pub monthly_limit_micro_usd: Option<i64>,
    /// Team orgs only.
    pub member_cap_seat_multiple: Option<f64>,
    pub auto_reload: Option<AutoReloadUpdate>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoReloadUpdate {
    pub enabled: bool,
    pub threshold_micro_usd: Option<i64>,
    pub amount_micro_usd: Option<i64>,
}

fn whole_cents(v: i64) -> bool {
    v % CENT_MICRO_USD == 0
}

fn check_limit(v: i64) -> Result<(), Invalid> {
    if !(LIMIT_MIN_MICRO_USD..=LIMIT_MAX_MICRO_USD).contains(&v) {
        return Err(Invalid::new(
            "limit_out_of_range",
            "The monthly limit must be between $1 and $100,000.",
        ));
    }
    if !whole_cents(v) {
        return Err(Invalid::new("amount_not_whole_cents", "The monthly limit must be a whole number of cents."));
    }
    Ok(())
}

fn check_reload_amount(v: i64) -> Result<(), Invalid> {
    if !(TOPUP_MIN_MICRO_USD..=TOPUP_MAX_MICRO_USD).contains(&v) {
        return Err(Invalid::new(
            "auto_reload_amount_out_of_range",
            "The auto-reload amount must be between $5 and $1,000.",
        ));
    }
    if !whole_cents(v) {
        return Err(Invalid::new("amount_not_whole_cents", "The auto-reload amount must be a whole number of cents."));
    }
    Ok(())
}

fn check_reload_threshold(v: i64) -> Result<(), Invalid> {
    if !(RELOAD_THRESHOLD_MIN_MICRO_USD..=TOPUP_MAX_MICRO_USD).contains(&v) {
        return Err(Invalid::new(
            "auto_reload_threshold_out_of_range",
            "The auto-reload threshold must be between $1 and $1,000.",
        ));
    }
    if !whole_cents(v) {
        return Err(Invalid::new("amount_not_whole_cents", "The auto-reload threshold must be a whole number of cents."));
    }
    Ok(())
}

/// 0.10x to 100x, at most two decimals (the column is NUMERIC(8,2)).
fn parse_cap(x: f64) -> Result<Decimal, Invalid> {
    let bad = || Invalid::new("member_cap_out_of_range", "The member cap must be between 0.1x and 100x, in steps of 0.01.");
    if !x.is_finite() || !(0.1..=100.0).contains(&x) {
        return Err(bad());
    }
    let scaled = (x * 100.0).round();
    if (scaled / 100.0 - x).abs() > 1e-9 {
        return Err(bad());
    }
    Ok(Decimal::new(scaled as i64, 2))
}

/// The pure rules for `PUT /v1/billing/extra-usage`. See the module doc.
///
/// The result is validated as a WHOLE (stored values merged under the
/// request), so no request can leave the row in a state the schema's CHECKs
/// would reject or that lets an auto-reload pass the monthly limit.
pub fn resolve_update(
    kind: AccountKind,
    current: Option<&StoredSettings>,
    req: &UpdateRequest,
) -> Result<StoredSettings, Invalid> {
    if let Some(l) = req.monthly_limit_micro_usd {
        check_limit(l)?;
    }
    let default_limit = match kind {
        AccountKind::TeamOrg => DEFAULT_ORG_LIMIT_MICRO_USD,
        AccountKind::Personal => DEFAULT_PRO_LIMIT_MICRO_USD,
    };
    let limit = req
        .monthly_limit_micro_usd
        .or(current.and_then(|c| c.monthly_limit_micro_usd))
        .or(req.enabled.then_some(default_limit));

    let cap = match kind {
        AccountKind::Personal => {
            if req.member_cap_seat_multiple.is_some() {
                return Err(Invalid::new(
                    "member_cap_not_applicable",
                    "A per-member cap only applies to team organizations.",
                ));
            }
            None
        }
        AccountKind::TeamOrg => match req.member_cap_seat_multiple {
            Some(x) => Some(parse_cap(x)?),
            None => current
                .and_then(|c| c.member_cap_seat_multiple)
                .or(Some(Decimal::from(DEFAULT_MEMBER_CAP_MULTIPLE))),
        },
    };

    let cur_enabled = current.map(|c| c.auto_reload_enabled).unwrap_or(false);
    let cur_threshold = current.and_then(|c| c.auto_reload_threshold_micro_usd);
    let cur_amount = current.and_then(|c| c.auto_reload_amount_micro_usd);
    let (mut reload_on, threshold, amount) = match &req.auto_reload {
        None => (cur_enabled, cur_threshold, cur_amount),
        Some(a) => {
            if let Some(t) = a.threshold_micro_usd {
                check_reload_threshold(t)?;
            }
            if let Some(m) = a.amount_micro_usd {
                check_reload_amount(m)?;
            }
            if a.enabled && !req.enabled {
                return Err(Invalid::new(
                    "auto_reload_requires_extra_usage",
                    "Auto-reload needs extra usage to be turned on.",
                ));
            }
            (a.enabled, a.threshold_micro_usd.or(cur_threshold), a.amount_micro_usd.or(cur_amount))
        }
    };
    if !req.enabled {
        // The schema forbids auto-reload without extra usage. The configured
        // values are kept for when it is turned back on.
        reload_on = false;
    }
    if reload_on && (threshold.is_none() || amount.is_none()) {
        return Err(Invalid::new(
            "auto_reload_incomplete",
            "Auto-reload needs both a threshold and an amount.",
        ));
    }
    if let (Some(t), Some(m)) = (threshold, amount) {
        if t > m {
            return Err(Invalid::new(
                "auto_reload_threshold_exceeds_amount",
                "The auto-reload amount must be at least the threshold, so a reload lifts the balance back above it.",
            ));
        }
    }
    if let (Some(m), Some(l)) = (amount, limit) {
        if m > l {
            return Err(Invalid::new(
                "auto_reload_exceeds_limit",
                "The auto-reload amount cannot exceed the monthly limit.",
            ));
        }
    }

    Ok(StoredSettings {
        enabled: req.enabled,
        monthly_limit_micro_usd: limit,
        member_cap_seat_multiple: cap,
        auto_reload_enabled: reload_on,
        auto_reload_threshold_micro_usd: threshold,
        auto_reload_amount_micro_usd: amount,
    })
}

/// A top-up amount the server will create a Checkout session for: $5 to
/// $1,000, whole cents.
pub fn validate_topup_amount(amount_micro_usd: i64) -> Result<(), Invalid> {
    if !(TOPUP_MIN_MICRO_USD..=TOPUP_MAX_MICRO_USD).contains(&amount_micro_usd) {
        return Err(Invalid::new("topup_amount_out_of_range", "A top-up must be between $5 and $1,000."));
    }
    if !whole_cents(amount_micro_usd) {
        return Err(Invalid::new("amount_not_whole_cents", "A top-up must be a whole number of cents."));
    }
    Ok(())
}

/// The form body for a top-up Checkout session. The amount is already
/// validated; it is sent as an ad-hoc `price_data` line (no catalog price), in
/// cents. `setup_future_usage=off_session` saves the card on the customer so
/// auto-reload (3.5) can charge it later. Card only, so the session is paid
/// when it completes. No promotion codes: a discount would credit more than
/// was paid.
pub fn topup_checkout_params(
    customer_id: &str,
    account_id: &str,
    amount_micro_usd: i64,
    success_url: &str,
    cancel_url: &str,
) -> Params {
    let cents = amount_micro_usd / CENT_MICRO_USD;
    let mut p = Params::new();
    p.push("mode", "payment")
        .push("customer", customer_id)
        .push("client_reference_id", account_id)
        .push("success_url", success_url)
        .push("cancel_url", cancel_url)
        .push("payment_method_types[0]", "card")
        .push("line_items[0][quantity]", "1")
        .push("line_items[0][price_data][currency]", "usd")
        .push("line_items[0][price_data][unit_amount]", cents.to_string())
        .push("line_items[0][price_data][product_data][name]", "CoralSwarm extra usage top-up")
        .push("payment_intent_data[setup_future_usage]", "off_session")
        .push("payment_intent_data[metadata][account_id]", account_id)
        .push("payment_intent_data[metadata][purpose]", TOPUP_PURPOSE)
        .push("metadata[account_id]", account_id)
        .push("metadata[purpose]", TOPUP_PURPOSE)
        .push("metadata[amount_micro_usd]", amount_micro_usd.to_string());
    p
}

/// The Stripe idempotency key for a top-up session creation. A session is not
/// a charge, so this only dedupes a double click: the same account, amount
/// and return URLs inside a 10-minute window resolve to one session. The URLs
/// are part of the key because Stripe rejects a reused key sent with
/// different parameters. Crediting is idempotent separately, on the session id.
pub fn topup_idempotency_key(
    account_id: Uuid,
    amount_micro_usd: i64,
    success_url: &str,
    cancel_url: &str,
    now_unix: i64,
) -> String {
    let mut h = Sha256::new();
    h.update(success_url.as_bytes());
    h.update([0]);
    h.update(cancel_url.as_bytes());
    let digest = hex::encode(h.finalize());
    format!("topup:{account_id}:{amount_micro_usd}:{}:{}", &digest[..16], now_unix.div_euclid(600))
}

impl StripeClient {
    /// `POST /v1/checkout/sessions` for an extra-usage top-up, keyed by
    /// [`topup_idempotency_key`].
    pub async fn create_checkout_session_topup(
        &self,
        params: &Params,
        idempotency_key: &str,
    ) -> Result<CheckoutSession, StripeError> {
        let v = self.post_form("/v1/checkout/sessions", params, Idem::Key(idempotency_key)).await?;
        let field = |k: &str| {
            v.get(k)
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .ok_or_else(|| StripeError::Parse(format!("missing/invalid {k:?} field")))
        };
        Ok(CheckoutSession { id: field("id")?, url: field("url")? })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreditKind {
    Topup,
    PackPurchase,
    /// An auto-reload charge (settings 3.5). The ledger allows `reload_id`
    /// only on this kind.
    AutoReload,
}

impl CreditKind {
    fn as_str(self) -> &'static str {
        match self {
            CreditKind::Topup => "topup",
            CreditKind::PackPurchase => "pack_purchase",
            CreditKind::AutoReload => "auto_reload",
        }
    }
}

pub struct CreditEntry<'a> {
    pub kind: CreditKind,
    pub micro_usd: i64,
    pub credits: Option<Decimal>,
    pub rate_micro_usd_per_credit: i64,
    pub idempotency_key: String,
    pub stripe_event_id: Option<&'a str>,
    pub stripe_payment_intent_id: Option<&'a str>,
    pub stripe_checkout_session_id: Option<&'a str>,
    /// The `extra_usage_reloads` row this credit settles ([`CreditKind::AutoReload`]).
    pub reload_id: Option<Uuid>,
    pub note: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreditOutcome {
    Credited { balance_after_micro_usd: i64 },
    /// A ledger row with the same idempotency key, session, payment intent or
    /// event already exists. Nothing was written.
    Duplicate,
}

/// Credit the extra-usage ledger and balance, once.
///
/// `conn` must be inside `begin_account_scoped` and past
/// `billing::renewal::lock_period_rotation` (the 0321 entry fence): without
/// it the schema refuses the write. The ledger row and the balance move in
/// the caller's transaction, so they commit or roll back together. The insert
/// is `ON CONFLICT DO NOTHING` across EVERY unique key (idempotency key,
/// event, payment intent, checkout session), so a replay, or a second event
/// for the same payment, writes nothing and reports [`CreditOutcome::Duplicate`].
pub async fn credit_in_tx(
    conn: &mut PgConnection,
    account: Uuid,
    entry: &CreditEntry<'_>,
) -> Result<CreditOutcome, sqlx::Error> {
    if entry.micro_usd <= 0 {
        return Err(sqlx::Error::Protocol(format!(
            "extra usage credit must be positive, got {}",
            entry.micro_usd
        )));
    }
    let inserted: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO extra_usage_ledger \
            (account_id, entry_kind, amount_micro_usd, credits, rate_micro_usd_per_credit, \
             idempotency_key, stripe_event_id, stripe_payment_intent_id, stripe_checkout_session_id, \
             reload_id, note) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
         ON CONFLICT DO NOTHING RETURNING id",
    )
    .bind(account)
    .bind(entry.kind.as_str())
    .bind(entry.micro_usd)
    .bind(entry.credits)
    .bind(entry.rate_micro_usd_per_credit)
    .bind(&entry.idempotency_key)
    .bind(entry.stripe_event_id)
    .bind(entry.stripe_payment_intent_id)
    .bind(entry.stripe_checkout_session_id)
    .bind(entry.reload_id)
    .bind(entry.note)
    .fetch_optional(&mut *conn)
    .await?;
    if inserted.is_none() {
        return Ok(CreditOutcome::Duplicate);
    }
    let balance: i64 = sqlx::query_scalar(
        "INSERT INTO extra_usage_balances (account_id, balance_micro_usd) VALUES ($1, $2) \
         ON CONFLICT (account_id) DO UPDATE SET \
            balance_micro_usd = extra_usage_balances.balance_micro_usd + EXCLUDED.balance_micro_usd, \
            updated_at = now() \
         RETURNING balance_micro_usd",
    )
    .bind(account)
    .bind(entry.micro_usd)
    .fetch_one(&mut *conn)
    .await?;
    Ok(CreditOutcome::Credited { balance_after_micro_usd: balance })
}

/// The micro-USD per credit in force now.
pub async fn rate_in_force(conn: &mut PgConnection) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT micro_usd_per_credit FROM extra_usage_rates \
          WHERE effective_from <= clock_timestamp() ORDER BY effective_from DESC LIMIT 1",
    )
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| sqlx::Error::Protocol("extra usage: no rate in force".into()))
}
