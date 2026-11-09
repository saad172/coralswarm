//! `interactive_ask`: Jev's Ask-time calls on their own managed purpose,
//! through the real Jev client, the real gated OpenRouter transport (against a
//! local stand-in) and the real funded gate, on both planes. The org plane
//! also runs as a Neon-shaped table-owner runtime.
//!
//! An Ask Jev call produces one receipt and is billed once (by the receipt
//! axis, never the query axis). A hold, a kill switch or a lost member fails
//! the phase open with nothing sent and nothing billed.
use super::*;
use crate::decision::openrouter::JevTimeouts;
use crate::decision::{AskBudget, DecisionError, DigestCard, OpenRouterDecisionClient};
use crate::workers::billing_derivation;
use crate::workers::tenant_harness::WorkerTenant;
use sqlx::types::Decimal;
use std::ffi::OsString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Process env for one test; restored on drop (the suite runs with
/// `--test-threads=1`).
struct Env(Vec<(&'static str, Option<OsString>)>);

impl Env {
    fn set(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let saved = pairs.iter().map(|(key, _)| (*key, std::env::var_os(key))).collect();
        for (key, value) in pairs {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        Self(saved)
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

const ON: &[(&str, Option<&str>)] = &[
    ("MANAGED_PROCESSING_DISABLED", None),
    ("MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK", None),
    ("MANAGED_PER_ATTEMPT_CAP_USD", None),
    ("JEV_MODEL", None),
    ("JEV_MODEL_ASK", None),
];

/// Reported provider cost of one stand-in reply, in micro-USD.
const REPLY_COST_MICRO: i64 = 210;

/// A local OpenRouter stand-in: every request gets one forced-tool reply that
/// scores every pack question, with a reported cost.
async fn openrouter_stand_in(questions: usize) -> (String, Arc<AtomicUsize>) {
    use axum::response::IntoResponse;
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let answers: serde_json::Map<String, serde_json::Value> =
        (0..questions).map(|i| (format!("q{i}"), serde_json::json!(0))).collect();
    let arguments = serde_json::Value::Object(answers).to_string();
    let app = axum::Router::new().fallback(move || {
        let counter = counter.clone();
        let arguments = arguments.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            axum::Json(serde_json::json!({
                "id": "gen-interactive-ask",
                "model": "deepseek/deepseek-v4-flash-0731",
                "provider": "MockHost",
                "choices": [{
                    "message": {"content": null, "tool_calls": [{"function": {"arguments": arguments}}]},
                    "finish_reason": "tool_calls"
                }],
                "usage": {"prompt_tokens": 900, "completion_tokens": 40, "cost": REPLY_COST_MICRO as f64 / 1_000_000.0}
            }))
            .into_response()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.ok() });
    (url, hits)
}

fn jev_client(url: &str) -> OpenRouterDecisionClient {
    let http = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
    let llm = crate::llm::openai::OpenAiProvider::openrouter(http, "test-key").with_base_url(url.to_string());
    OpenRouterDecisionClient::new(Arc::new(llm), JevTimeouts::uniform(Duration::from_secs(8)))
}

fn cards(n: usize) -> Vec<DigestCard> {
    (0..n)
        .map(|i| DigestCard {
            atom_id: Uuid::new_v4(),
            reef_title: format!("Billing reef {i}"),
            reef_type: "topic".into(),
            digest: format!("Retained evidence {i}: Dana owns billing."),
            reef_id: Some(Uuid::new_v4()),
        })
        .collect()
}

fn budget(f: &Fixture, content: &PgPool, asker: Uuid, query: Uuid) -> AskBudget {
    AskBudget::new(content.clone(), f.control.app.clone(), f.tenant.clone(), f.account, asker, f.graph, query)
}

fn worker(f: &Fixture, pool: &PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: f.tenant.clone(),
        pool: pool.clone(),
        store: Arc::new(crate::store::PostgresStore::new(pool.clone())),
    }
}

async fn funded(org: bool) -> Fixture {
    let f = Fixture::new(org).await;
    f.balance(Decimal::from(100)).await;
    f
}

/// One pack phase through the managed budget: `Ok` with every card judged,
/// or the phase's fail-open error.
async fn pack(
    client: &OpenRouterDecisionClient,
    budget: Option<AskBudget>,
    cards: &[DigestCard],
) -> Result<usize, DecisionError> {
    crate::decision::with_ask_budget(
        budget,
        crate::decision::evaluate_pack_with_retry(client, "who owns billing?", "billing owner", cards),
    )
    .await
    .map(|judged| judged.len())
}

async fn count(pool: &PgPool, sql: &str, account: Uuid) -> i64 {
    sqlx::query_scalar(sql).bind(account).fetch_one(pool).await.unwrap()
}

/// An Ask Jev call reserves before the send, settles after, and is billed
/// exactly once: one `interactive_ask` receipt, one managed debit, no query
/// axis debit, and an `llm_usage` row the query axis cannot select.
async fn ask_call_is_receipted_and_billed_once(org: bool, owner: bool) {
    let _env = Env::set(ON);
    let f = funded(org).await;
    let runtime = if owner { super::entity_resolution::owner_runtime(&f).await } else { f.app().clone() };
    let cards = cards(4);
    let (url, hits) = openrouter_stand_in(cards.len()).await;
    let client = jev_client(&url);
    let query = Uuid::new_v4();

    let judged = pack(&client, Some(budget(&f, &runtime, f.payer, query)), &cards).await;
    assert_eq!(judged.unwrap(), cards.len(), "the Ask phase gets Jev's judgements");
    assert_eq!(hits.load(Ordering::SeqCst), 1, "one provider attempt");

    let receipts: Vec<(Uuid, String, String, Uuid, Uuid, Option<Uuid>, Option<Uuid>, i64, i64)> = sqlx::query_as(
        "SELECT receipt_id,purpose,state,graph_id,attributed_user_id,job_id,unit_id,reserved_micro_usd,settled_micro_usd \
           FROM managed_attempt_receipts WHERE account_id=$1",
    )
    .bind(f.account)
    .fetch_all(f.owner())
    .await
    .unwrap();
    assert_eq!(receipts.len(), 1, "{receipts:?}");
    let (receipt, purpose, state, graph, payer, job, unit, reserved, settled) = receipts[0].clone();
    assert_eq!((purpose.as_str(), state.as_str()), ("interactive_ask", "billed"));
    assert_eq!((graph, payer, job, unit), (f.graph, f.payer, None, None), "charged to the asker, no job or unit");
    assert!(reserved >= settled && settled == REPLY_COST_MICRO, "reserved {reserved} before, settled {settled} after");

    let period: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT limit_micro_usd,reserved_micro_usd,charged_micro_usd,attempts FROM managed_budget_periods \
          WHERE account_id=$1 AND purpose='interactive_ask'",
    )
    .bind(f.account)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(period, (1_000_000, 0, REPLY_COST_MICRO, 1), "default $1/day cap, reservation released into the charge");

    let usage: Vec<(String, Option<Uuid>, Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
        "SELECT task,source_query_id,source_job_id,attributed_user_id FROM llm_usage WHERE account_id=$1",
    )
    .bind(f.account)
    .fetch_all(f.owner())
    .await
    .unwrap();
    assert_eq!(usage, vec![("jev_ask".to_string(), None, None, Some(f.payer))],
        "visible usage row, with no query key for the query axis to bill");
    assert_eq!(count(f.owner(), "SELECT count(*) FROM llm_usage_pending WHERE account_id=$1", f.account).await, 0);

    // No double billing: the receipt is debited once across settle and two
    // derivation passes, and the query axis never bills the Jev call.
    for _ in 0..2 {
        billing_derivation::tick(&worker(&f, f.app()), &f.control.app).await.unwrap();
    }
    let debits: Vec<(String, Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
        "SELECT entry_kind,source_receipt_id,source_query_id FROM credit_ledger \
          WHERE account_id=$1 AND entry_kind LIKE 'usage_%' ORDER BY entry_kind",
    )
    .bind(f.account)
    .fetch_all(&f.control.owner)
    .await
    .unwrap();
    assert_eq!(debits, vec![("usage_managed".to_string(), Some(receipt), None)], "one managed debit, nothing else");
    assert_eq!(
        count(&f.control.owner, "SELECT count(*) FROM managed_credit_reservations WHERE account_id=$1 AND state='held'", f.account).await,
        0,
        "the credit reservation settled"
    );

    // The receipt is sealed history even for the table owner (whose RLS
    // bypass would otherwise reach it).
    let owner_pool = if owner { &runtime } else { f.owner() };
    let error = sqlx::query("DELETE FROM managed_attempt_receipts WHERE receipt_id=$1")
        .bind(receipt)
        .execute(owner_pool)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot be deleted"), "{error}");
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn interactive_ask_call_is_receipted_and_billed_once_consumer() {
    ask_call_is_receipted_and_billed_once(false, false).await;
}

#[tokio::test]
async fn interactive_ask_call_is_receipted_and_billed_once_tenant_app_role() {
    ask_call_is_receipted_and_billed_once(true, false).await;
}

#[tokio::test]
async fn interactive_ask_call_is_receipted_and_billed_once_tenant_owner_runtime() {
    ask_call_is_receipted_and_billed_once(true, true).await;
}

fn held(result: &Result<usize, DecisionError>, code: &str) {
    match result {
        Err(error @ DecisionError::Budget(reason)) => {
            assert!(reason.contains(code), "expected {code}: {reason}");
            assert!(error.is_fail_open() && !error.is_context_overflow(), "fails open, never shrink-retried");
        }
        other => panic!("expected a {code} hold, got {other:?}"),
    }
}

/// A budget hold, the global kill switch and a lost member each fail the
/// phase open: nothing sent, nothing billed, no
/// credit left held. The same fixture then answers, so the holds are real.
#[tokio::test]
async fn interactive_ask_holds_fail_open_unsent_and_unbilled_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let cards = cards(3);
        let (url, hits) = openrouter_stand_in(cards.len()).await;
        let client = jev_client(&url);

        {
            let _cap = Env::set(&[("MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK", Some("0"))]);
            held(&pack(&client, Some(budget(&f, f.app(), f.payer, Uuid::new_v4())), &cards).await, "daily_budget_exhausted");
        }
        {
            let _kill = Env::set(&[("MANAGED_PROCESSING_DISABLED", Some("true"))]);
            held(&pack(&client, Some(budget(&f, f.app(), f.payer, Uuid::new_v4())), &cards).await, "managed_processing_disabled");
        }
        let stranger = Uuid::new_v4();
        held(&pack(&client, Some(budget(&f, f.app(), stranger, Uuid::new_v4())), &cards).await, "requester_access_lost");
        let no_budget = pack(&client, None, &cards).await;
        assert!(matches!(no_budget, Err(DecisionError::Budget(_))), "{no_budget:?}");

        assert_eq!(hits.load(Ordering::SeqCst), 0, "no hold reached the provider");
        assert_eq!(
            count(f.owner(), "SELECT count(*) FROM managed_attempt_receipts WHERE account_id=$1 AND state<>'not_generated' AND dispatch_phase<>'cancelled_unsent'", f.account).await,
            0,
            "no armed or billed receipt"
        );
        assert_eq!(
            count(&f.control.owner, "SELECT count(*) FROM managed_credit_reservations WHERE account_id=$1 AND state='held'", f.account).await,
            0,
            "no credit left held"
        );
        assert_eq!(count(f.owner(), "SELECT count(*) FROM llm_usage WHERE account_id=$1 AND task='jev_ask'", f.account).await, 0);
        billing_derivation::tick(&worker(&f, f.app()), &f.control.app).await.unwrap();
        assert_eq!(
            count(&f.control.owner, "SELECT count(*) FROM credit_ledger WHERE account_id=$1 AND entry_kind LIKE 'usage_%'", f.account).await,
            0,
            "nothing billed"
        );

        // Positive control: the same fixture answers once nothing holds it.
        assert_eq!(pack(&client, Some(budget(&f, f.app(), f.payer, Uuid::new_v4())), &cards).await.unwrap(), cards.len());
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        f.finish().await;
    }
}

/// Ask is interactive, so the longer background bounds never reach it: its
/// gate keeps the transport default (a blocking read, no provider sort, no
/// generation lookup) and its own client, sized to the Ask budget, ends a
/// hung call. The phase fails open on time and the cut attempt is settled
/// once, inside the transport, never left for the reaper.
#[tokio::test]
async fn interactive_ask_stays_bounded_against_a_hung_provider_both_planes() {
    use axum::response::IntoResponse;
    let mut pairs = ON.to_vec();
    pairs.extend([("JEV_TIMEOUT_SECS", Some("1")), ("LLM_MAX_RETRIES", Some("1"))]);
    let _env = Env::set(&pairs);
    for org in [false, true] {
        let f = funded(org).await;
        let bodies = Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
        let lookups = Arc::new(AtomicUsize::new(0));
        let (seen, looked) = (bodies.clone(), lookups.clone());
        let app = axum::Router::new()
            .route(
                "/v1/chat/completions",
                axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let seen = seen.clone();
                    async move {
                        seen.lock().unwrap().push(body);
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        "never"
                    }
                }),
            )
            .route(
                "/v1/generation",
                axum::routing::get(move || {
                    let looked = looked.clone();
                    async move {
                        looked.fetch_add(1, Ordering::SeqCst);
                        axum::http::StatusCode::NOT_FOUND.into_response()
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        // Production sizes the Ask transport at the Ask budget + 1 s.
        let http = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
        let llm = crate::llm::openai::OpenAiProvider::openrouter(http, "test-key").with_base_url(url);
        let client = OpenRouterDecisionClient::new(Arc::new(llm), JevTimeouts::uniform(Duration::from_secs(1)));

        let started = std::time::Instant::now();
        let judged = pack(&client, Some(budget(&f, f.app(), f.payer, Uuid::new_v4())), &cards(3)).await;
        assert!(judged.is_err(), "fails open");
        assert!(started.elapsed() < Duration::from_secs(3), "the Ask phase stays bounded: {:?}", started.elapsed());

        // The spawned metering task settles the cut attempt shortly after.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let open = count(f.owner(), "SELECT count(*) FROM managed_attempt_receipts WHERE account_id=$1 AND state='reserved'", f.account).await;
            if open == 0 || std::time::Instant::now() > deadline {
                assert_eq!(open, 0, "settled live, not by the reaper");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let states: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT state,reserved_micro_usd,settled_micro_usd FROM managed_attempt_receipts WHERE account_id=$1",
        )
        .bind(f.account)
        .fetch_all(f.owner())
        .await
        .unwrap();
        assert_eq!(states.len(), 1, "one attempt, settled once: {states:?}");
        assert_eq!(states[0].0, "ambiguous");
        assert_eq!(states[0].1, states[0].2);
        let body = bodies.lock().unwrap()[0].clone();
        assert!(body.get("stream").is_none(), "Ask keeps its blocking read");
        assert!(body["provider"].get("sort").is_none(), "Ask keeps OpenRouter's default routing");
        assert_eq!(lookups.load(Ordering::SeqCst), 0, "no generation lookup on the Ask path");
        f.finish().await;
    }
}
