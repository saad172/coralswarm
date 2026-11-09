//! Call timeouts on the real funded gate, against a local OpenRouter
//! stand-in that streams slowly or hangs.
//!
//! Production (coralswarm tenant, 24 h to 2026-10-20): 446 extraction-window
//! receipts settled ambiguous, 385 of them 60 to 70 s after creation — the
//! shared client's 60 s total deadline cutting answers that were still
//! arriving. A slow answer must be used and billed once; a hung one must
//! still end, settled once inside the transport, at the provider's own figure
//! when its generation record says so.
use super::*;
use crate::llm::{openai::OpenAiProvider, GatedLlm};
use crate::workers::billing_derivation;
use crate::workers::tenant_harness::WorkerTenant;
use sqlx::types::Decimal;
use std::ffi::OsString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Process env for one test; restored on drop (`--test-threads=1`).
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

const DEFAULTS: &[(&str, Option<&str>)] = &[
    ("MANAGED_PROCESSING_DISABLED", None),
    ("MANAGED_STREAMING_DISABLED", None),
    ("MANAGED_STREAM_STALL_SECS", None),
    ("MANAGED_PROVIDER_SORT", None),
    ("MANAGED_ATTEMPT_TIMEOUT_SECS_SOURCE_EXTRACTION", None),
    ("OPENROUTER_GENERATION_LOOKUP_DISABLED", None),
    ("LLM_MAX_RETRIES", Some("4")),
    ("LLM_RETRY_BASE_MS", Some("1")),
    ("LLM_RETRY_MAX_DELAY_MS", Some("5")),
];

/// Reported cost of the slow stand-in answer, in micro-USD.
const SLOW_COST_MICRO: i64 = 210;
/// What the stand-in's generation record says a cut generation cost.
const CUT_COST_MICRO: i64 = 90;

struct StandIn {
    url: String,
    bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    lookups: Arc<AtomicUsize>,
}

fn data(value: serde_json::Value) -> String {
    format!("data: {value}\n\n")
}

/// Streams `events` (delay in ms, raw text) for every completion and serves
/// `record` (or a 404) as the generation record.
async fn stand_in(events: Vec<(u64, String)>, record: Option<serde_json::Value>) -> StandIn {
    use axum::response::IntoResponse;
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let lookups = Arc::new(AtomicUsize::new(0));
    let (seen, looked) = (bodies.clone(), lookups.clone());
    let app = axum::Router::new()
        .route(
            "/v1/chat/completions",
            axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let (seen, events) = (seen.clone(), events.clone());
                async move {
                    seen.lock().unwrap().push(body);
                    let stream = async_stream::stream! {
                        for (ms, text) in events {
                            tokio::time::sleep(Duration::from_millis(ms)).await;
                            yield Ok::<_, std::convert::Infallible>(bytes::Bytes::from(text));
                        }
                    };
                    axum::response::Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(axum::body::Body::from_stream(stream))
                        .unwrap()
                }
            }),
        )
        .route(
            "/v1/generation",
            axum::routing::get(move || {
                let (looked, record) = (looked.clone(), record.clone());
                async move {
                    looked.fetch_add(1, Ordering::SeqCst);
                    match record {
                        Some(record) => axum::Json(record).into_response(),
                        None => axum::http::StatusCode::NOT_FOUND.into_response(),
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.ok() });
    StandIn { url, bodies, lookups }
}

/// A complete answer that takes ~2.4 s to arrive, past the 1 s client.
fn slow_answer() -> Vec<(u64, String)> {
    let chunk = |text: &str| {
        data(serde_json::json!({"id": "gen-slow", "model": "deepseek/deepseek-v4-flash-0731", "provider": "Parasail",
            "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": null}]}))
    };
    vec![
        (0, ": OPENROUTER PROCESSING\n\n".into()),
        (600, chunk("fixture ")),
        (600, chunk("answer")),
        (600, data(serde_json::json!({"id": "gen-slow", "model": "deepseek/deepseek-v4-flash-0731",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}))),
        (600, data(serde_json::json!({"id": "gen-slow", "model": "deepseek/deepseek-v4-flash-0731", "provider": "Parasail",
            "choices": [], "usage": {"prompt_tokens": 900, "completion_tokens": 40,
            "cost": SLOW_COST_MICRO as f64 / 1_000_000.0}}))),
        (0, "data: [DONE]\n\n".into()),
    ]
}

/// One event naming the generation, then keep-alives only.
fn hung_answer() -> Vec<(u64, String)> {
    vec![
        (0, data(serde_json::json!({"id": "gen-hung", "model": "deepseek/deepseek-v4-flash-0731", "provider": "Near AI",
            "choices": [{"index": 0, "delta": {"content": "{\"atoms\":["}}]}))),
        (300, ": OPENROUTER PROCESSING\n\n".into()),
        (300, ": OPENROUTER PROCESSING\n\n".into()),
        (120_000, "data: [DONE]\n\n".into()),
    ]
}

/// The production client shape (a total deadline, `LLM_TIMEOUT_SECS`),
/// scaled from 60 s to 1 s.
fn provider(url: &str) -> OpenAiProvider {
    let http = reqwest::Client::builder().timeout(Duration::from_secs(1)).build().unwrap();
    OpenAiProvider::openrouter(http, "local-fixture").with_base_url(url.to_string())
}

/// An extraction window's shape: the production output cap.
fn window_request() -> CompletionRequest {
    CompletionRequest { max_tokens: 8_000, ..completion_request() }
}

fn plane<'a>(f: &'a Fixture, runtime: &'a PgPool) -> JobScope<'a> {
    match f.tenant {
        TenantId::Consumer => JobScope::Consumer { pool: runtime, account_id: f.account },
        TenantId::Org(_) => JobScope::Org(runtime),
    }
}

async fn receipts(f: &Fixture) -> Vec<(String, i64, Option<i64>, Option<String>, i16)> {
    sqlx::query_as(
        "SELECT state,reserved_micro_usd,settled_micro_usd,generation_id,attempt_no \
           FROM managed_attempt_receipts WHERE account_id=$1 ORDER BY created_at",
    )
    .bind(f.account)
    .fetch_all(f.owner())
    .await
    .unwrap()
}

async fn derive_twice(f: &Fixture) -> Vec<(String, Option<Uuid>)> {
    let worker = WorkerTenant {
        tenant: f.tenant.clone(),
        pool: f.app().clone(),
        store: Arc::new(crate::store::PostgresStore::new(f.app().clone())),
    };
    for _ in 0..2 {
        billing_derivation::tick(&worker, &f.control.app).await.unwrap();
    }
    sqlx::query_as(
        "SELECT entry_kind,source_receipt_id FROM credit_ledger \
          WHERE account_id=$1 AND entry_kind LIKE 'usage_%' ORDER BY 1,2",
    )
    .bind(f.account)
    .fetch_all(&f.control.owner)
    .await
    .unwrap()
}

/// A window whose answer arrives over 2.4 s, past the 1 s total deadline the
/// shared client carries, is read whole: one receipt, billed at the
/// provider's figure, debited once.
async fn slow_answer_is_used_and_billed_once(org: bool, owner: bool) {
    let _env = Env::set(DEFAULTS);
    let f = Fixture::new(org).await;
    f.allowance(Decimal::from(10)).await;
    let runtime = if owner { super::entity_resolution::owner_runtime(&f).await } else { f.app().clone() };
    let server = stand_in(slow_answer(), None).await;
    let provider = provider(&server.url);
    let plane = plane(&f, &runtime);
    let req = window_request();
    let gate = ManagedGate::open(f.call(&plane), &req).await.unwrap();

    let response = provider.complete_gated(req, &gate).await.expect("a slow answer is used, not abandoned");
    assert_eq!(response.content, "fixture answer");
    assert_eq!(response.cost_usd, Some(SLOW_COST_MICRO as f64 / 1_000_000.0));

    let rows = receipts(&f).await;
    assert_eq!(rows.len(), 1, "one attempt: {rows:?}");
    let (state, reserved, settled, generation, attempt) = rows[0].clone();
    assert_eq!((state.as_str(), settled, generation.as_deref(), attempt), ("billed", Some(SLOW_COST_MICRO), Some("gen-slow"), 1));
    assert!(reserved > SLOW_COST_MICRO, "billed below the reservation, not at it");

    let body = server.bodies.lock().unwrap()[0].clone();
    assert_eq!(body["stream"], true);
    assert_eq!(body["provider"]["sort"], "throughput");
    assert_eq!((body["provider"]["zdr"].clone(), body["provider"]["data_collection"].clone()),
        (serde_json::json!(true), serde_json::json!("deny")), "sorting keeps the data policy");
    assert!(body["provider"]["max_price"]["completion"].as_f64().unwrap() > 0.0, "the priced terms still bound the pool");
    assert_eq!(body["model"], "deepseek/deepseek-v4-flash-0731", "model pinning is unchanged");
    assert_eq!(server.lookups.load(Ordering::SeqCst), 0, "a billed answer needs no lookup");

    let receipt: Uuid = sqlx::query_scalar("SELECT receipt_id FROM managed_attempt_receipts WHERE account_id=$1")
        .bind(f.account).fetch_one(f.owner()).await.unwrap();
    assert_eq!(derive_twice(&f).await, vec![("usage_managed".to_string(), Some(receipt))], "debited exactly once");

    drop(gate);
    drop(plane);
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn slow_extraction_answer_is_billed_once_consumer() {
    slow_answer_is_used_and_billed_once(false, false).await;
}

#[tokio::test]
async fn slow_extraction_answer_is_billed_once_tenant_app_role() {
    slow_answer_is_used_and_billed_once(true, false).await;
}

#[tokio::test]
async fn slow_extraction_answer_is_billed_once_tenant_owner_runtime() {
    slow_answer_is_used_and_billed_once(true, true).await;
}

/// A generation that stops producing events still ends, on the stall bound,
/// inside the transport: one receipt, never re-sent, never left for the
/// reaper. With a priced generation record it settles at that figure; with
/// none it settles ambiguous at the reservation, carrying the generation id.
#[tokio::test]
async fn hung_extraction_ends_on_stall_and_settles_once_both_planes() {
    let _env = Env::set(DEFAULTS);
    let _stall = Env::set(&[("MANAGED_STREAM_STALL_SECS", Some("1"))]);
    let record = serde_json::json!({"data": {"id": "gen-hung", "model": "deepseek/deepseek-v4-flash-20260731",
        "provider_name": "Near AI", "total_cost": CUT_COST_MICRO as f64 / 1_000_000.0, "cancelled": true,
        "native_tokens_prompt": 900, "native_tokens_completion": 12}});
    for org in [false, true] {
        for priced in [true, false] {
            let f = Fixture::new(org).await;
            f.allowance(Decimal::from(10)).await;
            let server = stand_in(hung_answer(), priced.then(|| record.clone())).await;
            let provider = provider(&server.url);
            let plane = f.plane();
            let req = window_request();
            let gate = ManagedGate::open(f.call(&plane), &req).await.unwrap();
            let started = std::time::Instant::now();
            let err = provider.complete_gated(req, &gate).await.unwrap_err();
            assert!(matches!(err, crate::llm::LlmError::Timeout), "{err:?}");
            assert!(started.elapsed() < Duration::from_secs(10), "bounded by the stall, not the 300 s attempt");
            assert_eq!(server.bodies.lock().unwrap().len(), 1, "a cut attempt is never re-sent");
            assert!(server.lookups.load(Ordering::SeqCst) >= 1);

            let rows = receipts(&f).await;
            assert_eq!(rows.len(), 1, "{rows:?}");
            let (state, reserved, settled, generation, _) = rows[0].clone();
            assert_eq!(generation.as_deref(), Some("gen-hung"), "the cut attempt names its generation");
            if priced {
                assert_eq!((state.as_str(), settled), ("billed", Some(CUT_COST_MICRO)), "the provider's figure, not the bound");
            } else {
                assert_eq!((state.as_str(), settled), ("ambiguous", Some(reserved)), "no record: the full reservation");
            }
            // Settled live: nothing left `reserved` for the receipt reaper.
            let open: i64 = sqlx::query_scalar("SELECT count(*) FROM managed_attempt_receipts WHERE account_id=$1 AND state='reserved'")
                .bind(f.account).fetch_one(f.owner()).await.unwrap();
            assert_eq!(open, 0);
            assert_eq!(derive_twice(&f).await.len(), 1, "debited exactly once");
            drop(gate);
            drop(plane);
            f.finish().await;
        }
    }
}
