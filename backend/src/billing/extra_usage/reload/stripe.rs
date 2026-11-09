//! The Stripe calls auto-reload makes, and the one parser for what Stripe says
//! about a PaymentIntent.
//!
//! Every mutating call carries an idempotency key derived from the reload row
//! (`reload:<id>` to create, `reload-cancel:<id>` to cancel). The reads
//! (customer, retrieve, list) are safe to repeat.

use serde_json::Value;
use uuid::Uuid;

use super::{ReloadRow, PURPOSE};
use crate::billing::extra_usage::CENT_MICRO_USD;
use crate::billing::stripe_client::{Idem, Params, StripeClient, StripeError};

/// What a PaymentIntent object says, narrowed to the fields we act on. Built by
/// [`PaymentIntentFacts::from_value`] from a webhook payload or an API response
/// alike, so both paths judge the same facts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PaymentIntentFacts {
    pub id: String,
    pub status: String,
    /// `amount` in cents (the amount we asked to charge).
    pub amount_cents: Option<i64>,
    pub currency: Option<String>,
    pub customer: Option<String>,
    pub purpose: Option<String>,
    pub reload_id: Option<Uuid>,
    pub account_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl PaymentIntentFacts {
    pub fn from_value(v: &Value) -> Result<Self, String> {
        let text = |x: Option<&Value>| x.and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string);
        let id = text(v.get("id")).ok_or("payment intent has no id")?;
        let status = text(v.get("status")).ok_or("payment intent has no status")?;
        let customer = match v.get("customer") {
            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
            Some(o @ Value::Object(_)) => text(o.get("id")),
            _ => None,
        };
        let md = v.get("metadata");
        let err = v.get("last_payment_error");
        Ok(PaymentIntentFacts {
            id,
            status,
            amount_cents: v.get("amount").and_then(Value::as_i64),
            currency: text(v.get("currency")),
            customer,
            purpose: text(md.and_then(|m| m.get("purpose"))),
            reload_id: text(md.and_then(|m| m.get("reload_id"))).and_then(|s| s.parse().ok()),
            account_id: text(md.and_then(|m| m.get("account_id"))),
            error_code: text(err.and_then(|e| e.get("decline_code")).or_else(|| err.and_then(|e| e.get("code")))),
            error_message: text(err.and_then(|e| e.get("message"))),
        })
    }

    pub fn is_reload(&self) -> bool {
        self.purpose.as_deref() == Some(PURPOSE)
    }
}

/// The exact form for a reload's PaymentIntent. Pure and deterministic in the
/// row, so every retry sends the same bytes under the same idempotency key.
///
/// `confirm` + `off_session`: charge the saved card now with no customer
/// present. `error_on_requires_action`: a card that wants 3DS answers with an
/// error instead of parking the PaymentIntent waiting for a customer who is not
/// there. The amount is the row's, in whole cents.
pub fn payment_intent_params(row: &ReloadRow) -> Result<Params, String> {
    let customer = row.customer_id.as_deref().ok_or("reload row has no customer")?;
    let pm = row.payment_method_id.as_deref().ok_or("reload row has no payment method")?;
    if row.amount_micro_usd <= 0 || row.amount_micro_usd % CENT_MICRO_USD != 0 {
        return Err(format!("reload amount {} is not a positive whole number of cents", row.amount_micro_usd));
    }
    let mut p = Params::new();
    p.push("amount", (row.amount_micro_usd / CENT_MICRO_USD).to_string())
        .push("currency", "usd")
        .push("customer", customer)
        .push("payment_method", pm)
        .push("payment_method_types[0]", "card")
        .bool("confirm", true)
        .bool("off_session", true)
        .bool("error_on_requires_action", true)
        .push("description", "CoralSwarm extra usage auto-reload")
        .push("metadata[purpose]", PURPOSE)
        .push("metadata[account_id]", row.account_id.to_string())
        .push("metadata[reload_id]", row.id.to_string());
    Ok(p)
}

impl StripeClient {
    /// `POST /v1/payment_intents` under the reload's stable key. The same key
    /// returns the same PaymentIntent, which is what makes crash recovery safe.
    pub async fn create_reload_payment_intent(
        &self,
        params: &Params,
        idempotency_key: &str,
    ) -> Result<PaymentIntentFacts, StripeError> {
        let v = self.post_form("/v1/payment_intents", params, Idem::Key(idempotency_key)).await?;
        PaymentIntentFacts::from_value(&v).map_err(StripeError::Parse)
    }

    pub async fn retrieve_payment_intent(&self, id: &str) -> Result<PaymentIntentFacts, StripeError> {
        let v = self.get(&format!("/v1/payment_intents/{id}"), &Params::new()).await?;
        PaymentIntentFacts::from_value(&v).map_err(StripeError::Parse)
    }

    /// The customer's recent PaymentIntents (strongly consistent, unlike
    /// search). `created_gte` is a unix time.
    pub async fn list_customer_payment_intents(
        &self,
        customer: &str,
        created_gte: i64,
    ) -> Result<Vec<PaymentIntentFacts>, StripeError> {
        let mut q = Params::new();
        q.push("customer", customer).push("created[gte]", created_gte.to_string()).push("limit", "100");
        let v = self.get("/v1/payment_intents", &q).await?;
        let data = v
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| StripeError::Parse("payment intent list has no data array".into()))?;
        data.iter().map(|x| PaymentIntentFacts::from_value(x).map_err(StripeError::Parse)).collect()
    }

    pub async fn cancel_payment_intent(&self, id: &str, idempotency_key: &str) -> Result<(), StripeError> {
        self.post_form(&format!("/v1/payment_intents/{id}/cancel"), &Params::new(), Idem::Key(idempotency_key))
            .await
            .map(|_| ())
    }

    /// The customer's default card (`invoice_settings.default_payment_method`),
    /// the one the subscription's card was copied to (settings 3.3). `None`
    /// when the customer has none: there is no card to charge.
    pub async fn customer_default_payment_method(&self, customer: &str) -> Result<Option<String>, StripeError> {
        let v = self.get(&format!("/v1/customers/{customer}"), &Params::new()).await?;
        Ok(match v.get("invoice_settings").and_then(|s| s.get("default_payment_method")) {
            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
            Some(o @ Value::Object(_)) => o.get("id").and_then(Value::as_str).map(str::to_string),
            _ => None,
        })
    }
}
