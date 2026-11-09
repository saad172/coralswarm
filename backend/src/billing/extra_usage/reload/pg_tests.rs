//! Live-Postgres tests for auto-reload: the one-charge guarantees (concurrent
//! workers, crash and retry, replayed and out-of-order webhooks, the reaper),
//! the decline / 3DS / auto-off paths, and the monthly-limit arithmetic under a
//! debit race. Stripe is a stateful local mock that honours idempotency keys
//! the way the real one does (same key, same answer).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL`; without it each test prints a skip
//! line (a vacuous pass, which CI's `skipping ` grep turns into a failure).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode, Uri};
use axum::response::IntoResponse;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use super::settle::{apply_event, EventOutcome};
use super::stripe::PaymentIntentFacts;
use super::*;
use crate::billing::notices::pg_tests::kit::{self, Acct, Plan};
use crate::billing::stripe_client::StripeClient;

const M: i64 = 1_000_000;

// ── Stripe mock ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum Mode {
    Succeed,
    /// 402 card_error; a PaymentIntent exists in `requires_payment_method`.
    Decline(&'static str),
    /// 200 with the PaymentIntent parked in `requires_action`.
    RequiresAction,
    /// 200 with the PaymentIntent `processing` (the webhook decides).
    Processing,
    /// Create and store the PaymentIntent, then answer 500 this many times
    /// (the process "crashed" before it saw the answer).
    ServerErrorAfterStore(usize),
}

struct Mocked {
    mode: Mode,
    default_pm: Option<String>,
    /// idempotency key -> (status, body)
    by_key: HashMap<String, (u16, Value)>,
    pis: HashMap<String, Value>,
    /// Every create request: (key, form).
    creates: Vec<(String, HashMap<String, String>)>,
    cancels: Vec<String>,
    customer_gets: usize,
    lists: usize,
    n: u64,
    remaining_500: usize,
}

#[derive(Clone)]
struct Mock {
    st: Arc<Mutex<Mocked>>,
}

fn pairs(s: &str) -> HashMap<String, String> {
    s.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            let d = |x: &str| urlencoding::decode(&x.replace('+', " ")).map(|c| c.into_owned()).unwrap_or_default();
            (d(k), d(v))
        })
        .collect()
}

fn pi_json(id: &str, status: &str, form: &HashMap<String, String>, err: Option<(&str, &str)>) -> Value {
    let mut v = json!({
        "id": id,
        "status": status,
        "amount": form.get("amount").and_then(|a| a.parse::<i64>().ok()).unwrap_or(0),
        "currency": form.get("currency").cloned().unwrap_or_default(),
        "customer": form.get("customer").cloned().unwrap_or_default(),
        "metadata": {
            "purpose": form.get("metadata[purpose]").cloned().unwrap_or_default(),
            "account_id": form.get("metadata[account_id]").cloned().unwrap_or_default(),
            "reload_id": form.get("metadata[reload_id]").cloned().unwrap_or_default(),
        },
    });
    if let Some((code, msg)) = err {
        v["last_payment_error"] = json!({"code": "card_declined", "decline_code": code, "message": msg});
    }
    v
}

fn respond(status: u16, body: Value) -> axum::response::Response {
    (
        StatusCode::from_u16(status).unwrap(),
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

async fn handler(State(m): State<Mock>, method: Method, uri: Uri, headers: axum::http::HeaderMap, body: Bytes) -> axum::response::Response {
    let path = uri.path().to_string();
    let query = pairs(uri.query().unwrap_or(""));
    let form = pairs(std::str::from_utf8(&body).unwrap_or(""));
    let mut s = m.st.lock().unwrap();
    if method == Method::POST && path == "/v1/payment_intents" {
        let key = headers.get("idempotency-key").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
        s.creates.push((key.clone(), form.clone()));
        if let Some((st, b)) = s.by_key.get(&key).cloned() {
            if s.remaining_500 > 0 {
                s.remaining_500 -= 1;
                return respond(500, json!({"error": {"type": "api_error", "message": "boom"}}));
            }
            return respond(st, b);
        }
        s.n += 1;
        let id = format!("pi_mock_{}_{}", s.n, Uuid::new_v4().simple());
        let (status, body) = match s.mode.clone() {
            Mode::Succeed => (200, pi_json(&id, "succeeded", &form, None)),
            Mode::Processing => (200, pi_json(&id, "processing", &form, None)),
            Mode::RequiresAction => (200, pi_json(&id, "requires_action", &form, None)),
            Mode::ServerErrorAfterStore(n) => {
                s.remaining_500 = n;
                (200, pi_json(&id, "succeeded", &form, None))
            }
            Mode::Decline(code) => {
                let pi = pi_json(&id, "requires_payment_method", &form, Some((code, "Your card was declined.")));
                s.pis.insert(id.clone(), pi);
                let err = json!({"error": {"type": "card_error", "code": "card_declined", "decline_code": code,
                                           "message": "Your card was declined."}});
                s.by_key.insert(key, (402, err.clone()));
                return respond(402, err);
            }
        };
        s.pis.insert(id, body.clone());
        s.by_key.insert(key, (status, body.clone()));
        if s.remaining_500 > 0 {
            s.remaining_500 -= 1;
            return respond(500, json!({"error": {"type": "api_error", "message": "boom"}}));
        }
        return respond(status, body);
    }
    if method == Method::GET && path == "/v1/payment_intents" {
        s.lists += 1;
        let cus = query.get("customer").cloned().unwrap_or_default();
        let data: Vec<Value> = s.pis.values().filter(|p| p["customer"] == cus.as_str()).cloned().collect();
        return respond(200, json!({"object": "list", "data": data}));
    }
    if let Some(id) = path.strip_prefix("/v1/payment_intents/") {
        if method == Method::POST && id.ends_with("/cancel") {
            let id = id.trim_end_matches("/cancel").to_string();
            s.cancels.push(id.clone());
            if let Some(p) = s.pis.get_mut(&id) {
                p["status"] = json!("canceled");
            }
            return respond(200, json!({"id": id, "status": "canceled"}));
        }
        if method == Method::GET {
            return match s.pis.get(id) {
                Some(p) => respond(200, p.clone()),
                None => respond(404, json!({"error": {"type": "invalid_request_error", "message": "No such payment_intent"}})),
            };
        }
    }
    if method == Method::GET && path.starts_with("/v1/customers/") {
        s.customer_gets += 1;
        let pm = s.default_pm.clone();
        return respond(200, json!({"id": "cus", "invoice_settings": {"default_payment_method": pm}}));
    }
    respond(404, json!({"error": {"type": "invalid_request_error", "message": format!("unmocked {method} {path}")}}))
}

impl Mock {
    async fn start() -> (StripeClient, Mock) {
        let m = Mock {
            st: Arc::new(Mutex::new(Mocked {
                mode: Mode::Succeed,
                default_pm: Some("pm_card".into()),
                by_key: HashMap::new(),
                pis: HashMap::new(),
                creates: vec![],
                cancels: vec![],
                customer_gets: 0,
                lists: 0,
                n: 0,
                remaining_500: 0,
            })),
        };
        let app = axum::Router::new().fallback(handler).with_state(m.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}")),
            m,
        )
    }
    fn mode(&self, m: Mode) {
        self.st.lock().unwrap().mode = m;
    }
    fn no_card(&self) {
        self.st.lock().unwrap().default_pm = None;
    }
    /// How many DISTINCT PaymentIntents (charges) exist.
    fn charges(&self) -> usize {
        self.st.lock().unwrap().pis.len()
    }
    fn create_requests(&self) -> usize {
        self.st.lock().unwrap().creates.len()
    }
    fn create_keys(&self) -> Vec<String> {
        self.st.lock().unwrap().creates.iter().map(|(k, _)| k.clone()).collect()
    }
    fn cancels(&self) -> Vec<String> {
        self.st.lock().unwrap().cancels.clone()
    }
    fn pi_ids(&self) -> Vec<String> {
        self.st.lock().unwrap().pis.keys().cloned().collect()
    }
    fn pi(&self, id: &str) -> Value {
        self.st.lock().unwrap().pis[id].clone()
    }
    fn set_pi_status(&self, id: &str, status: &str) {
        self.st.lock().unwrap().pis.get_mut(id).unwrap()["status"] = json!(status);
    }
}

// ── fixtures ───────────────────────────────────────────────────────────────

struct Fx {
    pool: PgPool,
    a: Acct,
    customer: String,
    stripe: StripeClient,
    mock: Mock,
}

async fn fx(test: &str, limit: i64, threshold: i64, amount: i64, balance: i64) -> Option<Fx> {
    let pool = kit::pg(test).await?;
    let a = kit::seed(&pool, Plan::Pro, 100, 0).await;
    kit::set_subscription(&pool, &a, "active").await;
    let customer: String = sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    kit::set_extra(&pool, &a, true, Some(limit), None).await;
    sqlx::query(
        "UPDATE extra_usage_settings SET auto_reload_enabled=true, auto_reload_threshold_micro_usd=$2, \
                auto_reload_amount_micro_usd=$3 WHERE account_id=$1",
    )
    .bind(a.id)
    .bind(threshold)
    .bind(amount)
    .execute(&pool)
    .await
    .unwrap();
    kit::set_extra_balance(&pool, &a, balance).await;
    let (stripe, mock) = Mock::start().await;
    Some(Fx { pool, a, customer, stripe, mock })
}

impl Fx {
    async fn run(&self) -> RunOutcome {
        run_account(&self.pool, &self.stripe, self.a.id).await.expect("run_account")
    }
    async fn reconcile(&self, reload: Uuid) -> RunOutcome {
        reconcile_one(&self.pool, &self.stripe, self.a.id, reload).await.expect("reconcile")
    }
    async fn balance(&self) -> i64 {
        sqlx::query_scalar("SELECT COALESCE((SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1),0)")
            .bind(self.a.id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
    async fn reloads(&self) -> Vec<(Uuid, String, Option<String>, bool, Option<String>)> {
        sqlx::query_as(
            "SELECT id, state, failure_code, disabled_auto_reload, stripe_payment_intent_id \
               FROM extra_usage_reloads WHERE account_id=$1 ORDER BY created_at",
        )
        .bind(self.a.id)
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }
    async fn only_reload(&self) -> (Uuid, String, Option<String>, bool, Option<String>) {
        let mut r = self.reloads().await;
        assert_eq!(r.len(), 1, "expected exactly one reload row: {r:?}");
        r.remove(0)
    }
    async fn ledger_rows(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM extra_usage_ledger WHERE account_id=$1 AND entry_kind='auto_reload'")
            .bind(self.a.id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
    async fn notices(&self, kind: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM billing_notices WHERE account_id=$1 AND notice_kind=$2")
            .bind(self.a.id)
            .bind(kind)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
    async fn auto_reload_on(&self) -> bool {
        sqlx::query_scalar("SELECT auto_reload_enabled FROM extra_usage_settings WHERE account_id=$1")
            .bind(self.a.id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
    /// Make every reload row look `secs` older (settled and created), so
    /// backoff and the staleness bounds can be crossed without sleeping.
    async fn age(&self, secs: i64) {
        let mut tx = kit::fenced(&self.pool, self.a.id).await;
        sqlx::query(
            "UPDATE extra_usage_reloads SET created_at = created_at - make_interval(secs => $2), \
                    submitted_at = submitted_at - make_interval(secs => $2), \
                    settled_at = settled_at - make_interval(secs => $2) WHERE account_id=$1",
        )
        .bind(self.a.id)
        .bind(secs as f64)
        .execute(&mut *tx)
        .await
        .unwrap();
        // The customer's last settings change moves back with them, or every
        // aged failure would predate it and stop counting.
        sqlx::query("UPDATE extra_usage_settings SET updated_at = updated_at - make_interval(secs => $2) WHERE account_id=$1")
            .bind(self.a.id)
            .bind(secs as f64)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    /// A settlement-style debit: balance down, this month's spend up, in one
    /// fenced transaction (what `write_extra_debit` does).
    async fn debit(&self, micro: i64) -> bool {
        let mut tx = kit::fenced(&self.pool, self.a.id).await;
        let bal: i64 = sqlx::query_scalar("SELECT COALESCE((SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1),0)")
            .bind(self.a.id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let take = micro.min(bal);
        if take <= 0 {
            tx.rollback().await.unwrap();
            return false;
        }
        sqlx::query("UPDATE extra_usage_balances SET balance_micro_usd = balance_micro_usd - $2 WHERE account_id=$1")
            .bind(self.a.id)
            .bind(take)
            .execute(&mut *tx)
            .await
            .unwrap();
        self.add_spent(&mut tx, take).await;
        tx.commit().await.unwrap();
        true
    }
    async fn add_spent(&self, tx: &mut sqlx::Transaction<'static, sqlx::Postgres>, micro: i64) {
        sqlx::query(
            "INSERT INTO extra_usage_periods (account_id, period_start, period_end, spent_micro_usd) \
             SELECT $1, date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC', \
                    (date_trunc('month', now() AT TIME ZONE 'UTC') + interval '1 month') AT TIME ZONE 'UTC', $2 \
             ON CONFLICT (account_id, period_start) DO UPDATE SET spent_micro_usd = extra_usage_periods.spent_micro_usd + $2",
        )
        .bind(self.a.id)
        .bind(micro)
        .execute(&mut **tx)
        .await
        .unwrap();
    }
    async fn set_spent(&self, micro: i64) {
        let mut tx = kit::fenced(&self.pool, self.a.id).await;
        self.add_spent(&mut tx, micro).await;
        tx.commit().await.unwrap();
    }
    async fn spent(&self) -> i64 {
        sqlx::query_scalar("SELECT COALESCE(sum(spent_micro_usd),0)::bigint FROM extra_usage_periods WHERE account_id=$1")
            .bind(self.a.id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
    async fn event(&self, event_id: &str, event_type: &str, pi: &Value) -> EventOutcome {
        let facts = PaymentIntentFacts::from_value(pi).unwrap();
        apply_event(&self.pool, Some(&self.stripe), self.a.id, event_id, event_type, &facts)
            .await
            .expect("apply_event")
    }
}

fn evt() -> String {
    format!("evt_{}", Uuid::new_v4().simple())
}

/// A webhook-shaped PaymentIntent for the account's only reload.
async fn event_pi(fx: &Fx, id: &str, status: &str, cents: i64) -> Value {
    let (rid, ..) = fx.only_reload().await;
    json!({
        "id": id, "status": status, "amount": cents, "currency": "usd", "customer": fx.customer,
        "metadata": {"purpose": PURPOSE, "account_id": fx.a.id.to_string(), "reload_id": rid.to_string()},
    })
}

// ── one in flight, one charge ──────────────────────────────────────────────

#[tokio::test]
async fn concurrent_workers_create_one_reload_and_charge_once() {
    let Some(fx) = fx("concurrent_workers_create_one_reload_and_charge_once", 100 * M, 5 * M, 25 * M, 2 * M).await else {
        return;
    };
    let fx = Arc::new(fx);
    let mut tasks = vec![];
    for _ in 0..10 {
        let fx = fx.clone();
        tasks.push(tokio::spawn(async move { fx.run().await }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let (rid, state, ..) = fx.only_reload().await;
    assert_eq!(state, "succeeded");
    assert_eq!(fx.mock.charges(), 1, "exactly one PaymentIntent exists at Stripe");
    assert!(fx.mock.create_keys().iter().all(|k| *k == format!("reload:{rid}")));
    assert_eq!(fx.balance().await, 27 * M);
    assert_eq!(fx.ledger_rows().await, 1);
    assert_eq!(fx.notices("auto_reload_succeeded").await, 1);
}

#[tokio::test]
async fn the_schema_refuses_a_second_in_flight_reload() {
    let Some(fx) = fx("the_schema_refuses_a_second_in_flight_reload", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Processing);
    fx.run().await;
    let mut tx = kit::fenced(&fx.pool, fx.a.id).await;
    let second = sqlx::query(
        "INSERT INTO extra_usage_reloads (account_id, amount_micro_usd, period_start) VALUES ($1, $2, now())",
    )
    .bind(fx.a.id)
    .bind(5 * M)
    .execute(&mut *tx)
    .await;
    assert!(second.is_err(), "the partial unique index must hold");
}

#[tokio::test]
async fn a_crash_after_the_payment_intent_exists_reuses_the_key_and_charges_once() {
    let Some(fx) = fx("a_crash_after_the_payment_intent_exists_reuses_the_key_and_charges_once", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    // Stripe creates the charge, then every answer we get back is a 500.
    fx.mock.mode(Mode::ServerErrorAfterStore(3));
    let first = fx.run().await;
    assert!(matches!(first, RunOutcome::Deferred(_)), "got {first:?}");
    let (rid, state, ..) = fx.only_reload().await;
    assert_eq!(state, "created", "we never saw the answer, so the row is still created");
    assert_eq!(fx.balance().await, 0);
    assert_eq!(fx.mock.charges(), 1);

    // The retry (next tick, or the reaper) re-sends the SAME key.
    let second = fx.reconcile(rid).await;
    assert!(matches!(second, RunOutcome::Reloaded { ref state, .. } if state == "succeeded"), "got {second:?}");
    assert_eq!(fx.mock.charges(), 1, "still one charge");
    let keys = fx.mock.create_keys();
    assert!(keys.len() >= 2 && keys.iter().all(|k| *k == format!("reload:{rid}")), "{keys:?}");
    assert_eq!(fx.balance().await, 25 * M);
    assert_eq!(fx.ledger_rows().await, 1);

    // And again: nothing more happens.
    fx.reconcile(rid).await;
    assert_eq!(fx.balance().await, 25 * M);
    assert_eq!(fx.mock.charges(), 1);
}

// ── webhooks ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_replayed_success_webhook_credits_once() {
    let Some(fx) = fx("a_replayed_success_webhook_credits_once", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Processing);
    fx.run().await;
    let (_, state, _, _, pi_id) = fx.only_reload().await;
    assert_eq!(state, "submitted");
    let pi_id = pi_id.expect("submitted rows carry the payment intent");
    fx.mock.set_pi_status(&pi_id, "succeeded");
    let pi = fx.mock.pi(&pi_id);

    let e1 = evt();
    assert_eq!(fx.event(&e1, "payment_intent.succeeded", &pi).await, EventOutcome::Credited { micro_usd: 25 * M });
    assert_eq!(fx.event(&e1, "payment_intent.succeeded", &pi).await, EventOutcome::Replay);
    // A dashboard resend is a new event id for the same payment.
    assert_eq!(fx.event(&evt(), "payment_intent.succeeded", &pi).await, EventOutcome::Noop);

    assert_eq!(fx.balance().await, 25 * M);
    assert_eq!(fx.ledger_rows().await, 1);
    assert_eq!(fx.notices("auto_reload_succeeded").await, 1);
    assert_eq!(fx.only_reload().await.1, "succeeded");
}

/// #1497: the settlement after a successful charge is skipped (the account's
/// lock is held by a connection outside this process's entry queue), so the
/// row stays `created` while Stripe holds one succeeded PaymentIntent. The
/// skip marks nothing; the webhook then credits exactly once; the reaper and
/// a replayed webhook add nothing and never charge again.
#[tokio::test]
async fn a_settlement_skipped_after_the_charge_is_credited_once_by_the_webhook() {
    let Some(fx) = fx("a_settlement_skipped_after_the_charge_is_credited_once_by_the_webhook", 100 * M, 5 * M, 25 * M, 0).await else {
        return;
    };
    // The charge reaches Stripe but every answer is lost (the client retries
    // a 5xx, so one is not enough): the row stays `created`.
    fx.mock.mode(Mode::ServerErrorAfterStore(3));
    let first = fx.run().await;
    assert!(matches!(first, RunOutcome::Deferred(_)), "the lost answer defers the row: {first:?}");
    let (rid, state, _, _, pi_id) = fx.only_reload().await;
    assert_eq!((state.as_str(), pi_id), ("created", None));
    assert_eq!(fx.mock.charges(), 1, "Stripe holds the one charge");

    // The re-drive gets the stored answer back under the same idempotency
    // key, then its settlement is skipped behind the held lock.
    let holder = kit::fenced(&fx.pool, fx.a.id).await;
    let skipped = reconcile_one(&fx.pool, &fx.stripe, fx.a.id, rid).await;
    let Err(error) = skipped else {
        panic!("the settlement behind a held lock must be skipped, not applied");
    };
    assert!(error.to_string().contains("lock timeout"), "the skip is the bounded lock wait: {error}");
    assert_eq!(fx.mock.charges(), 1, "the re-drive re-used the key: no second charge");
    assert_eq!(fx.only_reload().await.1, "created", "the skip marked nothing");
    assert_eq!((fx.balance().await, fx.ledger_rows().await), (0, 0), "and credited nothing");
    holder.rollback().await.unwrap();

    let pi_id = fx.mock.pi_ids().remove(0);
    let pi = fx.mock.pi(&pi_id);
    assert_eq!(
        fx.event(&evt(), "payment_intent.succeeded", &pi).await,
        EventOutcome::Credited { micro_usd: 25 * M },
        "the webhook credits the charge once"
    );
    assert_eq!(fx.only_reload().await.1, "succeeded");
    fx.reconcile(rid).await;
    assert_eq!(fx.event(&evt(), "payment_intent.succeeded", &pi).await, EventOutcome::Noop, "a replay adds nothing");
    assert_eq!(fx.mock.charges(), 1, "never charged again");
    assert_eq!(fx.balance().await, 25 * M);
    assert_eq!(fx.ledger_rows().await, 1, "exactly one credit");
}

#[tokio::test]
async fn concurrent_deliveries_of_the_success_event_credit_once() {
    let Some(fx) = fx("concurrent_deliveries_of_the_success_event_credit_once", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Processing);
    fx.run().await;
    let pi_id = fx.only_reload().await.4.unwrap();
    fx.mock.set_pi_status(&pi_id, "succeeded");
    let pi = fx.mock.pi(&pi_id);
    let fx = Arc::new(fx);
    let mut tasks = vec![];
    for _ in 0..8 {
        let (fx, pi) = (fx.clone(), pi.clone());
        tasks.push(tokio::spawn(async move { fx.event(&evt(), "payment_intent.succeeded", &pi).await }));
    }
    let mut credited = 0;
    for t in tasks {
        if matches!(t.await.unwrap(), EventOutcome::Credited { .. }) {
            credited += 1;
        }
    }
    assert_eq!(credited, 1);
    assert_eq!(fx.balance().await, 25 * M);
    assert_eq!(fx.ledger_rows().await, 1);
}

#[tokio::test]
async fn a_success_event_before_the_row_is_submitted_is_credited_and_the_late_worker_adds_nothing() {
    let Some(fx) = fx(
        "a_success_event_before_the_row_is_submitted_is_credited_and_the_late_worker_adds_nothing",
        100 * M,
        5 * M,
        25 * M,
        0,
    )
    .await
    else {
        return;
    };
    // The worker created the charge but has not recorded it (row still `created`).
    fx.mock.mode(Mode::ServerErrorAfterStore(3));
    fx.run().await;
    let (rid, state, ..) = fx.only_reload().await;
    assert_eq!(state, "created");
    let pi_id = fx.mock.pi_ids().remove(0);
    let pi = fx.mock.pi(&pi_id);

    // Stripe's webhook gets here first.
    assert_eq!(fx.event(&evt(), "payment_intent.succeeded", &pi).await, EventOutcome::Credited { micro_usd: 25 * M });
    let (_, state, _, _, recorded) = fx.only_reload().await;
    assert_eq!((state.as_str(), recorded.as_deref()), ("succeeded", Some(pi_id.as_str())));

    // The slow worker finishes afterwards.
    fx.reconcile(rid).await;
    assert_eq!(fx.balance().await, 25 * M);
    assert_eq!(fx.ledger_rows().await, 1);
    assert_eq!(fx.mock.charges(), 1);

    // An older failure event arriving last changes nothing.
    let stale = event_pi(&fx, &pi_id, "requires_payment_method", 2500).await;
    assert_eq!(fx.event(&evt(), "payment_intent.payment_failed", &stale).await, EventOutcome::Noop);
    assert_eq!(fx.only_reload().await.1, "succeeded");
    assert_eq!(fx.notices("auto_reload_failed").await, 0);
}

#[tokio::test]
async fn a_late_success_for_a_row_already_marked_failed_still_credits_the_money() {
    let Some(fx) = fx("a_late_success_for_a_row_already_marked_failed_still_credits_the_money", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    fx.mock.mode(Mode::Decline("insufficient_funds"));
    fx.run().await;
    assert_eq!(fx.only_reload().await.1, "failed");
    let pi = event_pi(&fx, &format!("pi_late_{}", Uuid::new_v4().simple()), "succeeded", 2500).await;
    assert_eq!(fx.event(&evt(), "payment_intent.succeeded", &pi).await, EventOutcome::Credited { micro_usd: 25 * M });
    assert_eq!(fx.balance().await, 25 * M);
    assert_eq!(fx.only_reload().await.1, "succeeded");
}

#[tokio::test]
async fn an_event_that_does_not_match_our_row_credits_nothing() {
    let Some(fx) = fx("an_event_that_does_not_match_our_row_credits_nothing", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Processing);
    fx.run().await;
    let pi_id = fx.only_reload().await.4.unwrap();
    // Wrong amount.
    let mut pi = fx.mock.pi(&pi_id);
    pi["status"] = json!("succeeded");
    pi["amount"] = json!(9_999_900);
    assert!(matches!(fx.event(&evt(), "payment_intent.succeeded", &pi).await, EventOutcome::Unprocessable(_)));
    // Wrong currency.
    let mut pi = fx.mock.pi(&pi_id);
    pi["currency"] = json!("eur");
    assert!(matches!(fx.event(&evt(), "payment_intent.succeeded", &pi).await, EventOutcome::Unprocessable(_)));
    // Another account's reload id on our customer's payment.
    let mut pi = fx.mock.pi(&pi_id);
    pi["metadata"]["reload_id"] = json!(Uuid::new_v4().to_string());
    assert!(matches!(fx.event(&evt(), "payment_intent.succeeded", &pi).await, EventOutcome::Unprocessable(_)));
    // A different payment intent than the one recorded.
    let mut pi = fx.mock.pi(&pi_id);
    pi["id"] = json!("pi_someone_else");
    assert!(matches!(fx.event(&evt(), "payment_intent.succeeded", &pi).await, EventOutcome::Unprocessable(_)));
    assert_eq!(fx.balance().await, 0);
    assert_eq!(fx.ledger_rows().await, 0);
    assert_eq!(fx.only_reload().await.1, "submitted");
}

// ── decline, 3DS, auto-off ─────────────────────────────────────────────────

#[tokio::test]
async fn a_decline_fails_the_reload_tells_the_customer_and_backs_off() {
    let Some(fx) = fx("a_decline_fails_the_reload_tells_the_customer_and_backs_off", 100 * M, 5 * M, 25 * M, 0).await else {
        return;
    };
    fx.mock.mode(Mode::Decline("insufficient_funds"));
    let out = fx.run().await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "failed"), "got {out:?}");
    let (rid, state, code, disabled, _) = fx.only_reload().await;
    assert_eq!((state.as_str(), code.as_deref(), disabled), ("failed", Some("insufficient_funds"), false));
    assert_eq!(fx.balance().await, 0);
    assert_eq!(fx.notices("auto_reload_failed").await, 1);
    let key: String = sqlx::query_scalar("SELECT dedupe_key FROM billing_notices WHERE account_id=$1")
        .bind(fx.a.id)
        .fetch_one(&fx.pool)
        .await
        .unwrap();
    assert_eq!(key, rid.to_string());

    // No hammering the card: the next tick waits.
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::Backoff));
    assert_eq!(fx.mock.create_requests(), 1);

    // Stripe's own payment_failed event for the same attempt adds nothing.
    let pi_id = fx.mock.pi_ids().remove(0);
    let pi = event_pi(&fx, &pi_id, "requires_payment_method", 2500).await;
    assert_eq!(fx.event(&evt(), "payment_intent.payment_failed", &pi).await, EventOutcome::Noop);
    assert_eq!(fx.notices("auto_reload_failed").await, 1);
    assert!(fx.auto_reload_on().await);
}

#[tokio::test]
async fn a_failure_event_that_arrives_first_marks_the_row_failed_once() {
    let Some(fx) = fx("a_failure_event_that_arrives_first_marks_the_row_failed_once", 100 * M, 5 * M, 25 * M, 0).await else {
        return;
    };
    fx.mock.mode(Mode::ServerErrorAfterStore(3));
    fx.run().await;
    assert_eq!(fx.only_reload().await.1, "created");
    let pi = event_pi(&fx, &format!("pi_failed_first_{}", Uuid::new_v4().simple()), "requires_payment_method", 2500).await;
    let out = fx.event(&evt(), "payment_intent.payment_failed", &pi).await;
    assert_eq!(out, EventOutcome::Failed { auto_reload_turned_off: false });
    assert_eq!(fx.only_reload().await.1, "failed");
    assert_eq!(fx.notices("auto_reload_failed").await, 1);
    assert_eq!(fx.balance().await, 0);
}

#[tokio::test]
async fn three_d_secure_cannot_complete_off_session_so_it_fails_and_the_intent_is_cancelled() {
    let Some(fx) = fx(
        "three_d_secure_cannot_complete_off_session_so_it_fails_and_the_intent_is_cancelled",
        100 * M,
        5 * M,
        25 * M,
        0,
    )
    .await
    else {
        return;
    };
    fx.mock.mode(Mode::RequiresAction);
    let out = fx.run().await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "failed"), "got {out:?}");
    let (_, state, code, ..) = fx.only_reload().await;
    assert_eq!((state.as_str(), code.as_deref()), ("failed", Some("authentication_required")));
    assert_eq!(fx.mock.cancels(), fx.mock.pi_ids());
    assert_eq!(fx.notices("auto_reload_failed").await, 1);
    assert_eq!(fx.balance().await, 0);
}

#[tokio::test]
async fn a_requires_action_event_fails_a_submitted_row() {
    let Some(fx) = fx("a_requires_action_event_fails_a_submitted_row", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Processing);
    fx.run().await;
    let pi_id = fx.only_reload().await.4.unwrap();
    let mut pi = fx.mock.pi(&pi_id);
    pi["status"] = json!("requires_action");
    assert_eq!(
        fx.event(&evt(), "payment_intent.requires_action", &pi).await,
        EventOutcome::Failed { auto_reload_turned_off: false }
    );
    assert_eq!(fx.only_reload().await.1, "failed");
    assert_eq!(fx.mock.cancels(), vec![pi_id]);
}

#[tokio::test]
async fn a_cancelled_intent_abandons_the_reload_without_a_failure_email_or_strike() {
    let Some(fx) = fx("a_cancelled_intent_abandons_the_reload_without_a_failure_email_or_strike", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    fx.mock.mode(Mode::Processing);
    fx.run().await;
    let pi_id = fx.only_reload().await.4.unwrap();
    let mut pi = fx.mock.pi(&pi_id);
    pi["status"] = json!("canceled");
    assert_eq!(fx.event(&evt(), "payment_intent.canceled", &pi).await, EventOutcome::Abandoned);
    assert_eq!(fx.only_reload().await.1, "abandoned");
    assert_eq!(fx.notices("auto_reload_failed").await, 0);
    // Not a failure: no backoff either.
    fx.mock.mode(Mode::Succeed);
    assert!(matches!(fx.run().await, RunOutcome::Reloaded { .. }));
}

#[tokio::test]
async fn three_failures_in_a_row_turn_auto_reload_off_and_say_so() {
    let Some(fx) = fx("three_failures_in_a_row_turn_auto_reload_off_and_say_so", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Decline("generic_decline"));
    for i in 1..=MAX_CONSECUTIVE_FAILURES {
        let out = fx.run().await;
        assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "failed"), "attempt {i}: {out:?}");
        assert_eq!(fx.auto_reload_on().await, i < MAX_CONSECUTIVE_FAILURES, "after failure {i}");
        fx.age(48 * 3_600).await;
    }
    let rows = fx.reloads().await;
    assert_eq!(rows.len() as i64, MAX_CONSECUTIVE_FAILURES);
    let flags: Vec<bool> = rows.iter().map(|r| r.3).collect();
    assert_eq!(flags, vec![false, false, true], "only the reload that tripped it carries the flag");
    assert_eq!(fx.notices("auto_reload_failed").await, MAX_CONSECUTIVE_FAILURES);
    // Off means off.
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::Off));
    assert_eq!(fx.mock.charges() as i64, MAX_CONSECUTIVE_FAILURES);
}

#[tokio::test]
async fn a_success_resets_the_failure_count() {
    let Some(fx) = fx("a_success_resets_the_failure_count", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Decline("generic_decline"));
    for _ in 0..2 {
        fx.run().await;
        fx.age(48 * 3_600).await;
    }
    fx.mock.mode(Mode::Succeed);
    fx.run().await;
    assert_eq!(fx.balance().await, 25 * M);
    fx.age(48 * 3_600).await;
    // Spend it down so the threshold trips again, then two more declines.
    fx.debit(25 * M).await;
    fx.mock.mode(Mode::Decline("generic_decline"));
    for _ in 0..2 {
        fx.run().await;
        fx.age(48 * 3_600).await;
    }
    assert!(fx.auto_reload_on().await, "2 + success + 2 is not 3 in a row");
}

// ── the monthly limit ──────────────────────────────────────────────────────

#[tokio::test]
async fn the_reload_is_cut_to_the_room_left_under_the_limit() {
    let Some(fx) = fx("the_reload_is_cut_to_the_room_left_under_the_limit", 20 * M, 5 * M, 15 * M, 2 * M).await else { return };
    fx.set_spent(12 * M).await;
    fx.run().await;
    // room = 20 - 12 spent - 2 balance = 6, not the configured 15.
    assert_eq!(fx.balance().await, 8 * M);
    let charged: i64 = sqlx::query_scalar("SELECT amount_micro_usd FROM extra_usage_reloads WHERE account_id=$1")
        .bind(fx.a.id)
        .fetch_one(&fx.pool)
        .await
        .unwrap();
    assert_eq!(charged, 6 * M);
    let cents = fx.mock.st.lock().unwrap().creates[0].1["amount"].clone();
    assert_eq!(cents, "600");
    // spent + balance is now exactly the limit: no further reload, ever, this month.
    fx.debit(7 * M).await;
    fx.age(48 * 3_600).await;
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::NoRoom));
    assert_eq!(fx.mock.charges(), 1);
}

#[tokio::test]
async fn at_the_limit_nothing_is_charged() {
    let Some(fx) = fx("at_the_limit_nothing_is_charged", 20 * M, 5 * M, 15 * M, 0).await else { return };
    fx.set_spent(20 * M).await;
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::NoRoom));
    assert_eq!(fx.mock.create_requests(), 0);
    assert!(fx.reloads().await.is_empty());
}

#[tokio::test]
async fn a_reload_already_charged_this_month_is_not_charged_again_when_the_ledger_is_adjusted() {
    let Some(fx) = fx(
        "a_reload_already_charged_this_month_is_not_charged_again_when_the_ledger_is_adjusted",
        20 * M,
        5 * M,
        15 * M,
        0,
    )
    .await
    else {
        return;
    };
    // $20 was charged by auto-reload this month, then an operator emptied the balance.
    let mut tx = kit::fenced(&fx.pool, fx.a.id).await;
    sqlx::query(
        "INSERT INTO extra_usage_reloads (account_id, amount_micro_usd, state, stripe_payment_intent_id, period_start, settled_at, submitted_at) \
         VALUES ($1, $2, 'succeeded', 'pi_old_' || gen_random_uuid()::text, date_trunc('month', now()), now(), now())",
    )
    .bind(fx.a.id)
    .bind(20 * M)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::NoRoom));
    assert_eq!(fx.mock.create_requests(), 0);
}

#[tokio::test]
async fn debits_racing_reloads_never_leave_more_bought_than_can_be_spent() {
    let Some(fx) = fx("debits_racing_reloads_never_leave_more_bought_than_can_be_spent", 40 * M, 5 * M, 15 * M, 3 * M).await
    else {
        return;
    };
    let limit = 40 * M;
    let fx = Arc::new(fx);
    let debiter = {
        let fx = fx.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                fx.debit(2 * M + 250_000).await;
                // The invariant holds at every commit, not just at the end.
                let (spent, bal) = (fx.spent().await, fx.balance().await);
                assert!(spent + bal <= limit, "spent {spent} + balance {bal} passed the limit {limit}");
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
    };
    let mut runners = vec![];
    for _ in 0..4 {
        let fx = fx.clone();
        runners.push(tokio::spawn(async move {
            for _ in 0..25 {
                fx.run().await;
                tokio::time::sleep(std::time::Duration::from_millis(3)).await;
            }
        }));
    }
    debiter.await.unwrap();
    for r in runners {
        r.await.unwrap();
    }
    let charged: i64 = fx
        .reloads()
        .await
        .iter()
        .filter(|r| r.1 == "succeeded")
        .count() as i64;
    assert!(charged >= 1);
    let total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(amount_micro_usd),0)::bigint FROM extra_usage_reloads WHERE account_id=$1 AND state='succeeded'",
    )
    .bind(fx.a.id)
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    // Everything charged was either spent or is still the balance, and the sum
    // of both never passed the limit: the opening 3 plus what was charged.
    assert!(total <= limit - 3 * M, "reloads charged {total} against a limit of {limit}");
    let (spent, bal) = (fx.spent().await, fx.balance().await);
    assert_eq!(spent + bal, 3 * M + total);
    assert!(spent + bal <= limit);
    assert!(spent <= limit);
    assert_eq!(fx.mock.charges() as i64, fx.reloads().await.iter().filter(|r| r.4.is_some()).count() as i64);
}

// ── refusals ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn frozen_credits_are_never_reloaded() {
    for status in ["past_due", "canceled"] {
        let Some(fx) = fx("frozen_credits_are_never_reloaded", 100 * M, 5 * M, 25 * M, 0).await else { return };
        kit::set_subscription(&fx.pool, &fx.a, status).await;
        assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::Frozen), "{status}");
        assert_eq!(fx.mock.create_requests(), 0);
        assert!(fx.reloads().await.is_empty());
    }
}

#[tokio::test]
async fn off_and_free_accounts_are_never_reloaded() {
    let Some(fx) = fx("off_and_free_accounts_are_never_reloaded", 100 * M, 5 * M, 25 * M, 0).await else { return };
    sqlx::query("UPDATE extra_usage_settings SET auto_reload_enabled=false WHERE account_id=$1")
        .bind(fx.a.id)
        .execute(&fx.pool)
        .await
        .unwrap();
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::Off));
    sqlx::query("UPDATE extra_usage_settings SET auto_reload_enabled=true WHERE account_id=$1")
        .bind(fx.a.id)
        .execute(&fx.pool)
        .await
        .unwrap();
    let mut tx = kit::fenced(&fx.pool, fx.a.id).await;
    sqlx::query("UPDATE grants SET plan_id=(SELECT id FROM plans WHERE code='free'), plan_is_seat_priced=false, seats=NULL WHERE account_id=$1")
        .bind(fx.a.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::Free));
    assert_eq!(fx.mock.create_requests(), 0);
}

#[tokio::test]
async fn above_the_threshold_nothing_happens() {
    let Some(fx) = fx("above_the_threshold_nothing_happens", 100 * M, 5 * M, 25 * M, 5 * M).await else { return };
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::AboveThreshold));
    assert_eq!(fx.mock.create_requests(), 0);
    assert_eq!(fx.mock.st.lock().unwrap().customer_gets, 0, "no Stripe call for an account that does not need one");
}

#[tokio::test]
async fn no_card_means_no_charge_and_one_failed_notice() {
    let Some(fx) = fx("no_card_means_no_charge_and_one_failed_notice", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.no_card();
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::NoPaymentMethod));
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::NoPaymentMethod));
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::NoPaymentMethod));
    assert_eq!(fx.mock.create_requests(), 0);
    assert!(fx.reloads().await.is_empty(), "no card, no reload row");
    assert_eq!(fx.notices("auto_reload_failed").await, 1, "told once, not on every tick");
    assert!(fx.auto_reload_on().await, "a missing card is not a strike");
}

#[tokio::test]
async fn an_account_with_no_stripe_customer_cannot_be_reloaded() {
    let Some(fx) = fx("an_account_with_no_stripe_customer_cannot_be_reloaded", 100 * M, 5 * M, 25 * M, 0).await else { return };
    let mut tx = kit::fenced(&fx.pool, fx.a.id).await;
    sqlx::query("DELETE FROM stripe_subscriptions WHERE account_id=$1").bind(fx.a.id).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::NoPaymentMethod));
    assert_eq!(fx.mock.create_requests(), 0);
}

// ── the reaper ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_reaper_settles_a_submitted_reload_from_stripe_without_a_new_charge() {
    let Some(fx) = fx("the_reaper_settles_a_submitted_reload_from_stripe_without_a_new_charge", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    fx.mock.mode(Mode::Processing);
    fx.run().await;
    let (rid, state, _, _, pi_id) = fx.only_reload().await;
    assert_eq!(state, "submitted");
    // The success webhook never arrived.
    fx.mock.set_pi_status(&pi_id.unwrap(), "succeeded");
    fx.age(3_600).await;
    let stats = run_pass_filtered(&fx.pool, &fx.stripe, true, Some(fx.a.id)).await.unwrap();
    assert!(stats.reconciled >= 1, "{stats:?}");
    assert_eq!(fx.only_reload().await.1, "succeeded");
    assert_eq!(fx.balance().await, 25 * M);
    assert_eq!(fx.ledger_rows().await, 1);
    assert_eq!(fx.mock.charges(), 1);
    assert_eq!(fx.mock.create_requests(), 1, "reconciling reads; it never creates");
    let _ = rid;
}

#[tokio::test]
async fn the_reaper_adopts_a_charge_it_finds_at_stripe_for_a_stale_created_row() {
    let Some(fx) = fx("the_reaper_adopts_a_charge_it_finds_at_stripe_for_a_stale_created_row", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    fx.mock.mode(Mode::ServerErrorAfterStore(3));
    fx.run().await;
    let (rid, state, ..) = fx.only_reload().await;
    assert_eq!(state, "created");
    let before = fx.mock.create_requests();
    // Too old to re-send the original request: find it instead.
    fx.age(REDRIVE_MAX_AGE_SECS + 600).await;
    let out = fx.reconcile(rid).await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "succeeded"), "got {out:?}");
    assert_eq!(fx.mock.create_requests(), before, "no create request at all");
    assert_eq!(fx.mock.charges(), 1);
    assert_eq!(fx.balance().await, 25 * M);
    assert!(fx.mock.st.lock().unwrap().lists >= 1);
}

#[tokio::test]
async fn the_reaper_abandons_a_stale_created_row_that_never_reached_stripe() {
    let Some(fx) = fx("the_reaper_abandons_a_stale_created_row_that_never_reached_stripe", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    // A row claimed, then the process died before any request.
    let mut tx = kit::fenced(&fx.pool, fx.a.id).await;
    let rid: Uuid = sqlx::query_scalar(
        "INSERT INTO extra_usage_reloads (account_id, amount_micro_usd, period_start, stripe_customer_id, stripe_payment_method_id, created_at) \
         VALUES ($1, $2, date_trunc('month', now()), $3, 'pm_card', now() - interval '3 hours') RETURNING id",
    )
    .bind(fx.a.id)
    .bind(25 * M)
    .bind(&fx.customer)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let out = fx.reconcile(rid).await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "abandoned"), "got {out:?}");
    assert_eq!(fx.mock.create_requests(), 0, "a three-hour-old decision is not charged");
    assert_eq!(fx.balance().await, 0);
    // The account is free to decide again.
    assert!(matches!(fx.run().await, RunOutcome::Reloaded { ref state, .. } if state == "succeeded"));
    assert_eq!(fx.mock.charges(), 1);
}

#[tokio::test]
async fn the_reaper_resends_a_recent_created_row_with_its_original_key() {
    let Some(fx) = fx("the_reaper_resends_a_recent_created_row_with_its_original_key", 100 * M, 5 * M, 25 * M, 0).await else {
        return;
    };
    fx.mock.mode(Mode::ServerErrorAfterStore(3));
    fx.run().await;
    fx.age(DRIVE_AFTER_SECS + 5).await;
    let stats = run_pass_filtered(&fx.pool, &fx.stripe, true, Some(fx.a.id)).await.unwrap();
    assert!(stats.reconciled >= 1, "{stats:?}");
    assert_eq!(fx.only_reload().await.1, "succeeded");
    assert_eq!(fx.mock.charges(), 1);
    assert_eq!(fx.balance().await, 25 * M);
}

#[tokio::test]
async fn with_new_charges_switched_off_the_pass_never_creates_a_payment_intent() {
    let Some(fx) = fx("with_new_charges_switched_off_the_pass_never_creates_a_payment_intent", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    let stats = run_pass_filtered(&fx.pool, &fx.stripe, false, Some(fx.a.id)).await.unwrap();
    assert_eq!(stats.reloaded, 0);
    assert_eq!(fx.mock.create_requests(), 0);
    assert!(fx.reloads().await.is_empty());
    // The same pass with charges on does reload (the account is in the candidate scan).
    let stats = run_pass_filtered(&fx.pool, &fx.stripe, true, Some(fx.a.id)).await.unwrap();
    assert!(stats.reloaded >= 1, "{stats:?}");
    assert_eq!(fx.balance().await, 25 * M);
}

// ── money review of #1482 ──────────────────────────────────────────────────

/// Tracing capture, same `MakeWriter` technique as `workers::billing_derivation`.
#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogBuf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> LogBuf {
        self.clone()
    }
}

/// Run `fut` with ERROR-level tracing captured (the guard stays on this
/// single-threaded test task across awaits).
async fn with_error_logs<T>(fut: impl std::future::Future<Output = T>) -> (T, String) {
    let buf = LogBuf::default();
    let sub = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(tracing::Level::ERROR)
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(sub);
    let out = fut.await;
    drop(guard);
    let logged = String::from_utf8(buf.0.lock().unwrap().clone()).expect("utf-8 log");
    (out, logged)
}

async fn set_cancel(fx: &Fx, at_period_end: bool, cancel_at: bool) {
    let mut tx = kit::fenced(&fx.pool, fx.a.id).await;
    sqlx::query(
        "UPDATE stripe_subscriptions SET cancel_at_period_end = $2, \
                cancel_at = CASE WHEN $3 THEN now() + interval '10 days' END WHERE account_id = $1",
    )
    .bind(fx.a.id)
    .bind(at_period_end)
    .bind(cancel_at)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn an_account_set_to_cancel_is_not_reloaded() {
    let Some(fx) = fx("an_account_set_to_cancel_is_not_reloaded", 100 * M, 5 * M, 25 * M, 0).await else { return };
    // Either form of scheduled cancellation: the credits would freeze at period end.
    for (end, date) in [(true, false), (false, true), (true, true)] {
        set_cancel(&fx, end, date).await;
        assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::CancelScheduled), "period_end={end} cancel_at={date}");
        assert_eq!(fx.mock.create_requests(), 0);
        assert!(fx.reloads().await.is_empty());
    }
    // Positive control: the same account reloads once the cancellation is withdrawn.
    set_cancel(&fx, false, false).await;
    assert!(matches!(fx.run().await, RunOutcome::Reloaded { ref state, .. } if state == "succeeded"));
    assert_eq!(fx.balance().await, 25 * M);
}

#[tokio::test]
async fn settings_saves_do_not_reset_the_failure_count() {
    let Some(fx) = fx("settings_saves_do_not_reset_the_failure_count", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Decline("generic_decline"));
    for i in 1..=MAX_CONSECUTIVE_FAILURES {
        let out = fx.run().await;
        assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "failed"), "attempt {i}: {out:?}");
        assert_eq!(fx.auto_reload_on().await, i < MAX_CONSECUTIVE_FAILURES, "after failure {i}");
        fx.age(48 * 3_600).await;
        // The customer saves their settings (any save bumps updated_at) between declines.
        sqlx::query("UPDATE extra_usage_settings SET updated_at = now() WHERE account_id = $1")
            .bind(fx.a.id)
            .execute(&fx.pool)
            .await
            .unwrap();
    }
    assert_eq!(fx.mock.charges() as i64, MAX_CONSECUTIVE_FAILURES, "three declines, then off: not six charges");
    assert!(!fx.auto_reload_on().await);
    assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::Off));
}

#[tokio::test]
async fn turning_auto_reload_back_on_starts_a_fresh_count() {
    let Some(fx) = fx("turning_auto_reload_back_on_starts_a_fresh_count", 100 * M, 5 * M, 25 * M, 0).await else { return };
    fx.mock.mode(Mode::Decline("generic_decline"));
    for _ in 0..MAX_CONSECUTIVE_FAILURES {
        fx.run().await;
        fx.age(48 * 3_600).await;
    }
    assert!(!fx.auto_reload_on().await);
    // What the settings route does on off -> on.
    sqlx::query("UPDATE extra_usage_settings SET auto_reload_enabled = true, auto_reload_reset_at = now() WHERE account_id = $1")
        .bind(fx.a.id)
        .execute(&fx.pool)
        .await
        .unwrap();
    fx.run().await; // one more decline: strike one of a new count, not a fourth
    assert!(fx.auto_reload_on().await, "a fresh count starts at zero");
}

async fn insert_stale_created(fx: &Fx, age_secs: i64) -> Uuid {
    let mut tx = kit::fenced(&fx.pool, fx.a.id).await;
    let rid: Uuid = sqlx::query_scalar(
        "INSERT INTO extra_usage_reloads (account_id, amount_micro_usd, period_start, stripe_customer_id, \
                                          stripe_payment_method_id, created_at) \
         VALUES ($1, $2, date_trunc('month', now()), $3, 'pm_card', now() - make_interval(secs => $4)) RETURNING id",
    )
    .bind(fx.a.id)
    .bind(25 * M)
    .bind(&fx.customer)
    .bind(age_secs as f64)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    rid
}

#[tokio::test]
async fn a_stale_created_row_that_no_longer_fits_is_abandoned_not_resent() {
    // Limit lowered after the decision: 25 no longer fits under 20.
    let Some(fx) = fx("a_stale_created_row_that_no_longer_fits_is_abandoned_not_resent", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    let rid = insert_stale_created(&fx, 600).await; // recent enough to be re-sent, old enough to abandon
    sqlx::query(
        "UPDATE extra_usage_settings SET monthly_limit_micro_usd = $2, auto_reload_amount_micro_usd = $2 WHERE account_id = $1",
    )
    .bind(fx.a.id)
    .bind(20 * M)
    .execute(&fx.pool)
    .await
    .unwrap();
    let out = fx.reconcile(rid).await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "abandoned"), "got {out:?}");
    assert_eq!(fx.mock.create_requests(), 0, "the stale row must not be charged");
    assert_eq!(fx.balance().await, 0);
}

#[tokio::test]
async fn a_stale_created_row_is_abandoned_when_the_balance_no_longer_needs_it() {
    let Some(fx) = fx("a_stale_created_row_is_abandoned_when_the_balance_no_longer_needs_it", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    let rid = insert_stale_created(&fx, 600).await;
    kit::set_extra_balance(&fx.pool, &fx.a, 50 * M).await; // topped up above the threshold since
    let out = fx.reconcile(rid).await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "abandoned"), "got {out:?}");
    assert_eq!(fx.mock.create_requests(), 0);
}

#[tokio::test]
async fn a_stale_created_row_that_still_fits_is_resent() {
    // Positive control for the two tests above.
    let Some(fx) = fx("a_stale_created_row_that_still_fits_is_resent", 100 * M, 5 * M, 25 * M, 0).await else { return };
    let rid = insert_stale_created(&fx, 600).await;
    let out = fx.reconcile(rid).await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "succeeded"), "got {out:?}");
    assert_eq!(fx.mock.create_requests(), 1);
}

#[tokio::test]
async fn a_late_success_that_overshoots_the_month_is_credited_and_alerts() {
    // Limit $30, reload $25. Row A is given up on (abandoned) and a second
    // reload B is sized against a month that does not include it; then A's
    // charge turns out to have gone through.
    let Some(fx) = fx("a_late_success_that_overshoots_the_month_is_credited_and_alerts", 30 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    let a_row = insert_stale_created(&fx, 3 * 3_600).await;
    let out = fx.reconcile(a_row).await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "abandoned"), "got {out:?}");
    // B, an ordinary in-limit success: no alert.
    let (b_out, b_logs) = with_error_logs(fx.run()).await;
    assert!(matches!(b_out, RunOutcome::Reloaded { ref state, .. } if state == "succeeded"));
    assert!(!b_logs.contains("alert"), "{b_logs}");
    assert_eq!(fx.balance().await, 25 * M);

    let late = json!({
        "id": format!("pi_late_a_{}", Uuid::new_v4().simple()), "status": "succeeded", "amount": 2500,
        "currency": "usd", "customer": fx.customer,
        "metadata": {"purpose": PURPOSE, "account_id": fx.a.id.to_string(), "reload_id": a_row.to_string()},
    });
    let (outcome, logged) = with_error_logs(fx.event(&evt(), "payment_intent.succeeded", &late)).await;

    assert_eq!(outcome, EventOutcome::Credited { micro_usd: 25 * M }, "the money was taken: credit it");
    assert_eq!(fx.balance().await, 50 * M);
    assert!(logged.contains("alert=true") || logged.contains("alert = true"), "no alert logged: {logged}");
    assert!(logged.contains("past the monthly limit"), "{logged}");
}

// ── account deletion (settings 2.4) ────────────────────────────────────────

/// Replace the account's deletion row with one in `state` (owner pool: the
/// table is fixture data here, as in `services::account_deletion` tests).
async fn set_deletion(fx: &Fx, state: &str) {
    sqlx::query("DELETE FROM account_deletions WHERE account_id = $1").bind(fx.a.id).execute(&fx.pool).await.unwrap();
    let sql = match state {
        "grace" => "INSERT INTO account_deletions (user_id, account_id, initiated_by, state) VALUES ($1, $2, 'user', 'grace')",
        "running" => {
            "INSERT INTO account_deletions (user_id, account_id, initiated_by, state, running_since) \
             VALUES ($1, $2, 'user', 'running', now())"
        }
        "completed" => {
            "INSERT INTO account_deletions (user_id, account_id, initiated_by, state, running_since, completed_at, completed_steps) \
             VALUES ($1, $2, 'user', 'completed', now(), now(), ARRAY['tombstone'])"
        }
        "cancelled" => {
            "INSERT INTO account_deletions (user_id, account_id, initiated_by, state, cancelled_at) \
             VALUES ($1, $2, 'user', 'cancelled', now())"
        }
        other => panic!("unknown deletion state {other}"),
    };
    sqlx::query(sql).bind(fx.a.user).bind(fx.a.id).execute(&fx.pool).await.unwrap();
}

#[tokio::test]
async fn an_account_being_deleted_is_never_reloaded() {
    let Some(fx) = fx("an_account_being_deleted_is_never_reloaded", 100 * M, 5 * M, 25 * M, 0).await else { return };
    for state in ["grace", "running", "completed"] {
        set_deletion(&fx, state).await;
        assert_eq!(fx.run().await, RunOutcome::Skipped(Skip::Deleting), "state={state}");
        assert_eq!(fx.mock.create_requests(), 0, "state={state}");
        assert!(fx.reloads().await.is_empty(), "state={state}");
    }
    // Positive control: a cancelled deletion is not a deletion; the same
    // account, with the same due reload, is charged.
    set_deletion(&fx, "cancelled").await;
    assert!(matches!(fx.run().await, RunOutcome::Reloaded { ref state, .. } if state == "succeeded"));
    assert_eq!(fx.mock.create_requests(), 1);
    assert_eq!(fx.balance().await, 25 * M);
}

#[tokio::test]
async fn a_stale_created_row_for_an_account_being_deleted_is_abandoned_not_resent() {
    // Same setup as `a_stale_created_row_that_still_fits_is_resent`, which is the
    // positive control: with no deletion that row IS re-sent.
    let Some(fx) =
        fx("a_stale_created_row_for_an_account_being_deleted_is_abandoned_not_resent", 100 * M, 5 * M, 25 * M, 0).await
    else {
        return;
    };
    let rid = insert_stale_created(&fx, 600).await;
    set_deletion(&fx, "grace").await;
    let out = fx.reconcile(rid).await;
    assert!(matches!(out, RunOutcome::Reloaded { ref state, .. } if state == "abandoned"), "got {out:?}");
    assert_eq!(fx.mock.create_requests(), 0, "a deleting account must not be charged by the reaper");
    assert_eq!(fx.balance().await, 0);
}
