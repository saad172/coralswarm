//! OpenRouter embeddings for the server corpus.
//!
//! One model, one width, no fallback provider, no ONNX substitute. A failed
//! call leaves `atoms.embedding_next` empty so the backfill can retry it.
//! The live `bge-small-en-v1.5` vector is a different writer and is not
//! filled from this response.
//!
//! Request policy matches the chat client: `provider.zdr` and
//! `provider.data_collection: "deny"`. `allow_fallbacks` is false here
//! because a different host's embedding would not share this coordinate space.

use serde_json::{json, Value};

use crate::embed::{
    EmbedBatchFuture, EmbedError, EmbedInput, EmbedProvider, GatedEmbed, ProviderPermit, Sealed,
    EMBED_DIM, EMBED_MODEL_CLOUD,
};
use crate::send_gate::{self, SendGate, SendOutcome, Unmanaged};

/// OpenRouter model id. The stored stamp is [`EMBED_MODEL_CLOUD`], which also
/// records the requested width.
pub const CLOUD_EMBED_MODEL: &str = "openai/text-embedding-3-small";

/// OpenRouter answers embeddings with the bare upstream id
/// (`text-embedding-3-small`), not the `openai/` slug that was requested.
fn is_served_cloud_model(model: Option<&str>) -> bool {
    model.is_some_and(|model| {
        model == CLOUD_EMBED_MODEL || Some(model) == CLOUD_EMBED_MODEL.strip_prefix("openai/")
    })
}

/// POST `{base}/v1/embeddings`.
pub struct OpenRouterEmbedder {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl OpenRouterEmbedder {
    pub fn new(client: reqwest::Client, api_key: impl Into<String>) -> Self {
        Self {
            client,
            api_key: api_key.into().trim().to_string(),
            base_url: "https://openrouter.ai/api".to_string(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        let trimmed = base_url.into().trim().trim_end_matches('/').to_string();
        let trimmed = trimmed.strip_suffix("/v1").unwrap_or(&trimmed);
        self.base_url = trimmed.trim_end_matches('/').to_string();
        self
    }

    fn embeddings_url(&self) -> String {
        format!("{}/v1/embeddings", self.base_url)
    }
}

/// Body sent to OpenRouter. `allow_fallbacks` is false: a second provider
/// would be a different coordinate space wearing this stamp.
pub fn embeddings_request_body(texts: &[&str]) -> Value {
    json!({
        "model": CLOUD_EMBED_MODEL,
        "input": texts,
        "dimensions": EMBED_DIM,
        "provider": {
            "zdr": true,
            "data_collection": "deny",
            "allow_fallbacks": false,
        }
    })
}

fn embeddings_request_body_with_terms(
    texts: &[&str],
    terms: Option<&send_gate::SendTerms>,
) -> Result<Value, EmbedError> {
    let mut body = embeddings_request_body(texts);
    let Some(terms) = terms else {
        return Ok(body);
    };
    let invalid = || {
        EmbedError::Denied(send_gate::Deny::Refuse(
            send_gate::RefuseCode::UnpricedModel,
        ))
    };
    if terms.models.len() != 1 || terms.models[0] != CLOUD_EMBED_MODEL {
        return Err(invalid());
    }
    let price = terms
        .max_price
        .filter(|price| price.prompt.is_finite() && price.prompt >= 0.0)
        .ok_or_else(invalid)?;
    body["provider"]["max_price"] = json!({ "prompt": price.prompt, "request": 0 });
    Ok(body)
}

/// Parse an embeddings response into one vector per input, in input order.
/// Rejects a width other than [`EMBED_DIM`], an all-zero vector, a short
/// `data` array, or a missing `embedding`.
pub fn parse_embeddings_response(
    body: &Value,
    expected: usize,
) -> Result<Vec<Vec<f32>>, EmbedError> {
    parse_embeddings_response_with_indexes(body, expected, false)
}

fn parse_embeddings_response_with_indexes(
    body: &Value,
    expected: usize,
    require_indexes: bool,
) -> Result<Vec<Vec<f32>>, EmbedError> {
    let data = body.get("data").and_then(|d| d.as_array()).ok_or_else(|| {
        EmbedError::InferenceFailed("embeddings response has no data array".into())
    })?;
    if data.len() != expected {
        return Err(EmbedError::InferenceFailed(format!(
            "embeddings response has {} vectors, expected {expected}",
            data.len()
        )));
    }

    let mut indexed: Vec<(usize, Vec<f32>)> = Vec::with_capacity(expected);
    for (fallback_index, item) in data.iter().enumerate() {
        let index = match item.get("index").and_then(Value::as_u64) {
            Some(index) => usize::try_from(index).map_err(|_| {
                EmbedError::InferenceFailed("embeddings index exceeds the supported range".into())
            })?,
            None if !require_indexes => fallback_index,
            None => {
                return Err(EmbedError::InferenceFailed(
                    "embeddings item has no unsigned integer index".into(),
                ))
            }
        };
        let values = item
            .get("embedding")
            .and_then(|e| e.as_array())
            .ok_or_else(|| {
                EmbedError::InferenceFailed(format!("embeddings item {index} has no embedding"))
            })?;
        if values.len() != EMBED_DIM {
            return Err(EmbedError::InferenceFailed(format!(
                "embeddings item {index} has width {}, expected {EMBED_DIM} — widen the column before flipping search",
                values.len()
            )));
        }
        let vector: Vec<f32> = values
            .iter()
            .map(|v| {
                v.as_f64()
                    .map(|n| n as f32)
                    .filter(|n| n.is_finite())
                    .ok_or_else(|| {
                        EmbedError::InferenceFailed(format!(
                            "embeddings item {index} has a non-numeric or nonfinite component"
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
        if vector.iter().all(|n| *n == 0.0) {
            return Err(EmbedError::InferenceFailed(format!(
                "embeddings item {index} is all zeros"
            )));
        }
        indexed.push((index, vector));
    }
    indexed.sort_by_key(|(index, _)| *index);
    if indexed
        .iter()
        .enumerate()
        .any(|(i, (index, _))| *index != i)
    {
        return Err(EmbedError::InferenceFailed(
            "embeddings response indexes are not a permutation of the input".into(),
        ));
    }
    Ok(indexed.into_iter().map(|(_, vector)| vector).collect())
}

impl EmbedProvider for OpenRouterEmbedder {
    fn embed_batch<'a>(
        &'a self,
        inputs: &'a [EmbedInput],
        _: ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        self.embed_batch_with_gate(inputs, &Unmanaged, false)
    }

    fn gated(&self) -> Option<&dyn GatedEmbed> {
        Some(self)
    }
}

enum Observed<'a> {
    NotWritten,
    SendFailed,
    Status,
    BodyUnreadable,
    Body(&'a Value),
}

fn embed_outcome(observed: Observed<'_>) -> SendOutcome {
    match observed {
        Observed::NotWritten | Observed::Status => SendOutcome::NotGenerated,
        Observed::SendFailed | Observed::BodyUnreadable => SendOutcome::Ambiguous {
            generation_id: None,
        },
        Observed::Body(body) => {
            let generation_id = body.get("id").and_then(Value::as_str).map(str::to_string);
            let usage = body.get("usage");
            let reported_cost_usd = usage
                .and_then(|usage| usage.get("cost"))
                .and_then(Value::as_f64)
                .filter(|cost| {
                    cost.is_finite()
                        && *cost >= 0.0
                        && (*cost * 1_000_000.0).ceil() < i64::MAX as f64
                });
            let input_tokens = usage
                .and_then(|usage| usage.get("prompt_tokens"))
                .and_then(Value::as_u64);
            let served_model = body.get("model").and_then(Value::as_str);
            if reported_cost_usd.is_none()
                && (input_tokens.is_none() || !is_served_cloud_model(served_model))
            {
                return SendOutcome::Ambiguous { generation_id };
            }
            SendOutcome::Billed {
                reported_cost_usd,
                input_tokens: input_tokens.unwrap_or(0),
                output_tokens: 0,
                served_model: served_model.unwrap_or(CLOUD_EMBED_MODEL).to_string(),
                upstream: body
                    .get("provider")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                generation_id,
            }
        }
    }
}

impl Sealed for OpenRouterEmbedder {}

impl GatedEmbed for OpenRouterEmbedder {
    fn embed_batch_gated<'a>(
        &'a self,
        inputs: &'a [EmbedInput],
        gate: &'a dyn SendGate,
        _: ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        self.embed_batch_with_gate(inputs, gate, true)
    }
}

impl OpenRouterEmbedder {
    fn embed_batch_with_gate<'a>(
        &'a self,
        inputs: &'a [EmbedInput],
        gate: &'a dyn SendGate,
        managed: bool,
    ) -> EmbedBatchFuture<'a> {
        Box::pin(async move {
            if inputs.is_empty() {
                return Ok(Vec::new());
            }
            let texts: Vec<&str> = inputs.iter().map(EmbedInput::as_str).collect();
            let terms = gate.terms();
            let request_body = embeddings_request_body_with_terms(&texts, terms)?;
            let permit = send_gate::acquire(gate, 1)
                .await
                .map_err(EmbedError::Denied)?;
            let sent = permit.into_sent();
            let response = match self
                .client
                .post(self.embeddings_url())
                .bearer_auth(&self.api_key)
                .header("HTTP-Referer", "https://coralswarm.ai")
                .header("X-Title", "CoralSwarm")
                .json(&request_body)
                .send()
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    let observed = if e.is_connect() {
                        Observed::NotWritten
                    } else {
                        Observed::SendFailed
                    };
                    send_gate::settle(gate, sent, embed_outcome(observed)).await;
                    return Err(EmbedError::Transport(format!(
                        "embeddings request failed: {e}"
                    )));
                }
            };
            let status = response.status();
            // Read the body as text first: an error status often carries no
            // JSON (a proxy's HTML 502), and its status, not its body, is
            // what decides whether it is worth a retry.
            let text = response.text().await;
            let body: Option<Value> = text
                .as_ref()
                .ok()
                .and_then(|text| serde_json::from_str(text).ok());
            let observed = match (&body, status.is_success()) {
                (_, false) => Observed::Status,
                (Some(body), true) => Observed::Body(body),
                (None, true) => Observed::BodyUnreadable,
            };
            send_gate::settle(gate, sent, embed_outcome(observed)).await;
            let text = text.map_err(|e| {
                EmbedError::Transport(format!(
                    "embeddings response ({status}) was not read: {e}"
                ))
            })?;
            if !status.is_success() {
                if status == reqwest::StatusCode::BAD_REQUEST && is_input_too_long(&text) {
                    return Err(EmbedError::InputTooLong(format!(
                        "embeddings HTTP {status}: {text}"
                    )));
                }
                return Err(EmbedError::Http {
                    status: status.as_u16(),
                    body: text,
                });
            }
            let body = body.ok_or_else(|| {
                EmbedError::InferenceFailed(format!("embeddings response was not JSON ({status})"))
            })?;
            if managed && !is_served_cloud_model(body.get("model").and_then(Value::as_str)) {
                return Err(EmbedError::InferenceFailed(
                    "embeddings response model does not match the requested model".into(),
                ));
            }
            parse_embeddings_response_with_indexes(&body, texts.len(), managed)
        })
    }
}

/// The provider's "input over the model's limit" refusal. OpenRouter relays
/// OpenAI's message (`Invalid 'input[0]': maximum input length is 8192
/// tokens.`); "maximum context length" is the same refusal's older wording.
fn is_input_too_long(body: &str) -> bool {
    let text = body.to_ascii_lowercase();
    text.contains("maximum input length") || text.contains("maximum context length")
}

/// Stamp the cloud writer stores. Kept next to the client so a caller cannot
/// persist a vector from this provider under the bge name.
pub fn cloud_stamp() -> &'static str {
    EMBED_MODEL_CLOUD
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(texts: &[&str]) -> Vec<EmbedInput> {
        texts.iter().flat_map(|t| EmbedInput::split(t)).collect()
    }

    fn unit_vector() -> Vec<Value> {
        let mut values = vec![json!(0.0); EMBED_DIM];
        values[0] = json!(1.0);
        values
    }

    #[test]
    fn request_pins_width_zdr_and_no_fallback() {
        let body = embeddings_request_body(&["a reef"]);
        assert_eq!(body["model"], CLOUD_EMBED_MODEL);
        assert_eq!(body["dimensions"], EMBED_DIM);
        assert_eq!(body["provider"]["zdr"], true);
        assert_eq!(body["provider"]["data_collection"], "deny");
        assert_eq!(body["provider"]["allow_fallbacks"], false);
        assert!(body.get("models").is_none(), "no backup model list");
    }

    #[test]
    fn parse_orders_by_index_and_rejects_the_wrong_width() {
        let ok = json!({
            "data": [
                { "index": 1, "embedding": unit_vector() },
                { "index": 0, "embedding": unit_vector() },
            ]
        });
        let parsed = parse_embeddings_response(&ok, 2).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0][0], 1.0);

        let wide = json!({
            "data": [{ "index": 0, "embedding": [1.0, 0.0] }]
        });
        assert!(parse_embeddings_response(&wide, 1).is_err());
    }

    #[test]
    fn parse_rejects_an_all_zero_vector() {
        let zeros = json!({
            "data": [{ "index": 0, "embedding": vec![json!(0.0); EMBED_DIM] }]
        });
        assert!(parse_embeddings_response(&zeros, 1).is_err());
    }

    #[test]
    fn outcome_by_observation() {
        assert_eq!(
            embed_outcome(Observed::NotWritten),
            SendOutcome::NotGenerated
        );
        assert_eq!(embed_outcome(Observed::Status), SendOutcome::NotGenerated);
        assert_eq!(
            embed_outcome(Observed::SendFailed),
            SendOutcome::Ambiguous {
                generation_id: None
            }
        );
        assert_eq!(
            embed_outcome(Observed::BodyUnreadable),
            SendOutcome::Ambiguous {
                generation_id: None
            }
        );
        assert_eq!(
            embed_outcome(Observed::Body(&json!({"id":"g","data":[]}))),
            SendOutcome::Ambiguous {
                generation_id: Some("g".into())
            }
        );
    }

    struct Recording(std::sync::Mutex<Vec<SendOutcome>>);
    impl SendGate for Recording {
        fn terms(&self) -> Option<&send_gate::SendTerms> {
            None
        }
        fn reserve<'a>(
            &'a self,
            _: u32,
        ) -> send_gate::GateFuture<'a, Result<send_gate::Reservation, send_gate::Deny>> {
            Box::pin(async { Ok(send_gate::Reservation::default()) })
        }
        fn settle<'a>(
            &'a self,
            _: send_gate::Sent,
            outcome: &'a SendOutcome,
        ) -> send_gate::GateFuture<'a, ()> {
            Box::pin(async move { self.0.lock().unwrap().push(outcome.clone()) })
        }
    }

    #[tokio::test]
    async fn gated_embed_settles_reported_cost() {
        let body = json!({
            "id": "gen-emb-1",
            "model": CLOUD_EMBED_MODEL,
            "provider": "OpenAI",
            "data": [{ "index": 0, "embedding": unit_vector() }],
            "usage": { "prompt_tokens": 12, "total_tokens": 12, "cost": 0.00000024 }
        });
        let app = axum::Router::new().route(
            "/v1/embeddings",
            axum::routing::post(move || {
                let body = body.clone();
                async move { axum::Json(body) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        let embedder = OpenRouterEmbedder::new(reqwest::Client::new(), "k")
            .with_base_url(format!("http://{addr}"));
        let gate = Recording(std::sync::Mutex::new(Vec::new()));
        let gated = embedder.gated().expect("the cloud embedder is gated");
        let vectors = gated.embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new()).await.unwrap();
        assert_eq!(vectors.len(), 1);
        assert_eq!(
            *gate.0.lock().unwrap(),
            vec![SendOutcome::Billed {
                reported_cost_usd: Some(0.00000024),
                input_tokens: 12,
                output_tokens: 0,
                served_model: CLOUD_EMBED_MODEL.into(),
                upstream: Some("OpenAI".into()),
                generation_id: Some("gen-emb-1".into()),
            }]
        );
        let ((), log) = send_gate::with_attempt_log(async {
            embedder.embed_batch(&inputs(&["a reef"]), ProviderPermit::new()).await.unwrap();
        })
        .await;
        assert_eq!(log.len(), 1, "the unmanaged path reaches the attempt log");
        assert_eq!(log[0].outcome.reported_cost_usd(), Some(0.00000024));
    }

    struct MeteredGate {
        terms: Option<send_gate::SendTerms>,
        reserves: std::sync::atomic::AtomicUsize,
        outcomes: std::sync::Mutex<Vec<SendOutcome>>,
        deny: bool,
    }

    impl MeteredGate {
        fn new(terms: Option<send_gate::SendTerms>) -> Self {
            Self {
                terms,
                reserves: std::sync::atomic::AtomicUsize::new(0),
                outcomes: std::sync::Mutex::new(Vec::new()),
                deny: false,
            }
        }
    }

    impl SendGate for MeteredGate {
        fn terms(&self) -> Option<&send_gate::SendTerms> {
            self.terms.as_ref()
        }

        fn reserve<'a>(
            &'a self,
            attempt_no: u32,
        ) -> send_gate::GateFuture<'a, Result<send_gate::Reservation, send_gate::Deny>> {
            Box::pin(async move {
                assert_eq!(attempt_no, 1);
                self.reserves
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if self.deny {
                    Err(send_gate::Deny::Unavailable)
                } else {
                    Ok(send_gate::Reservation::default())
                }
            })
        }

        fn settle<'a>(
            &'a self,
            sent: send_gate::Sent,
            outcome: &'a SendOutcome,
        ) -> send_gate::GateFuture<'a, ()> {
            Box::pin(async move {
                assert_eq!(sent.attempt_no(), 1);
                self.outcomes.lock().unwrap().push(outcome.clone());
            })
        }
    }

    fn priced_terms() -> send_gate::SendTerms {
        send_gate::SendTerms {
            models: vec![CLOUD_EMBED_MODEL.into()],
            max_price: Some(send_gate::MaxPrice {
                prompt: 0.02,
                completion: 0.0,
            }),
        }
    }

    struct Loopback {
        embedder: OpenRouterEmbedder,
        requests: std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Loopback {
        async fn new(status: u16, body: String) -> Self {
            let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let captured = requests.clone();
            let app = axum::Router::new().route(
                "/v1/embeddings",
                axum::routing::post(move |axum::Json(request): axum::Json<Value>| {
                    captured.lock().unwrap().push(request);
                    let body = body.clone();
                    async move { (axum::http::StatusCode::from_u16(status).unwrap(), body) }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self {
                embedder: OpenRouterEmbedder::new(reqwest::Client::new(), "loopback")
                    .with_base_url(format!("http://{addr}")),
                requests,
                task,
            }
        }
    }

    impl Drop for Loopback {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn embedding_reply() -> Value {
        json!({
            "id": "embedding-test",
            "model": CLOUD_EMBED_MODEL,
            "usage": { "cost": 0.00000024 },
            "data": [{ "index": 0, "embedding": unit_vector() }],
        })
    }

    #[tokio::test]
    async fn managed_wire_carries_exact_terms_and_defaults_remain_compatible() {
        let server = Loopback::new(200, embedding_reply().to_string()).await;
        let managed = MeteredGate::new(Some(priced_terms()));
        let defaults = MeteredGate::new(None);
        for gate in [&managed, &defaults] {
            server
                .embedder
                .embed_batch_gated(&inputs(&["a reef"]), gate, ProviderPermit::new())
                .await
                .unwrap();
            assert_eq!(gate.reserves.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(gate.outcomes.lock().unwrap().len(), 1);
        }
        server.embedder.embed_batch(&inputs(&["a reef"]), ProviderPermit::new()).await.unwrap();
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let mut expected = embeddings_request_body(&["a reef"]);
        expected["provider"]["max_price"] = json!({ "prompt": 0.02, "request": 0 });
        assert_eq!(requests[0], expected);
        assert_eq!(requests[1], embeddings_request_body(&["a reef"]));
        assert_eq!(requests[2], embeddings_request_body(&["a reef"]));
    }

    #[tokio::test]
    async fn invalid_managed_terms_refuse_before_reservation_or_post() {
        let server = Loopback::new(200, embedding_reply().to_string()).await;
        let mut cases = Vec::new();
        for models in [
            vec![],
            vec!["other-model".into()],
            vec![CLOUD_EMBED_MODEL.into(), "fallback".into()],
        ] {
            cases.push(send_gate::SendTerms {
                models,
                ..priced_terms()
            });
        }
        cases.push(send_gate::SendTerms {
            max_price: None,
            ..priced_terms()
        });
        for prompt in [-0.01, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            cases.push(send_gate::SendTerms {
                max_price: Some(send_gate::MaxPrice {
                    prompt,
                    completion: 0.0,
                }),
                ..priced_terms()
            });
        }
        for terms in cases {
            let gate = MeteredGate::new(Some(terms));
            assert!(server
                .embedder
                .embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new())
                .await
                .is_err());
            assert_eq!(gate.reserves.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert!(gate.outcomes.lock().unwrap().is_empty());
        }
        assert!(server.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unusable_usage_remains_ambiguous_after_real_post() {
        for usage in [
            Value::Null,
            json!({}),
            json!({ "total_tokens": 4 }),
            json!({ "prompt_tokens": -1 }),
            json!({ "prompt_tokens": 1.5 }),
            json!({ "prompt_tokens": "2" }),
            json!({ "cost": -0.1 }),
            json!({ "cost": 1e308 }),
            json!({ "cost": "NaN" }),
        ] {
            let mut body = embedding_reply();
            body["usage"] = usage.clone();
            let server = Loopback::new(200, body.to_string()).await;
            let gate = MeteredGate::new(Some(priced_terms()));
            assert!(server
                .embedder
                .embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new())
                .await
                .is_ok());
            assert_eq!(server.requests.lock().unwrap().len(), 1);
            assert_eq!(gate.reserves.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(
                *gate.outcomes.lock().unwrap(),
                vec![SendOutcome::Ambiguous {
                    generation_id: Some("embedding-test".into()),
                }],
                "usage {usage}"
            );
        }
    }

    #[tokio::test]
    async fn token_usage_requires_explicit_fixed_served_model() {
        for model in [
            None,
            Some("unknown"),
            Some("anthropic/text-embedding-3-small"),
            Some(CLOUD_EMBED_MODEL),
            Some("text-embedding-3-small"),
        ] {
            let mut body = embedding_reply();
            body["usage"] = json!({ "prompt_tokens": 12 });
            body.as_object_mut().unwrap().remove("model");
            if let Some(model) = model {
                body["model"] = json!(model);
            }
            let served = matches!(model, Some(CLOUD_EMBED_MODEL | "text-embedding-3-small"));
            let server = Loopback::new(200, body.to_string()).await;
            let gate = MeteredGate::new(Some(priced_terms()));
            let result = server.embedder.embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new()).await;
            assert_eq!(result.is_ok(), served, "model {model:?}");
            let outcome = gate.outcomes.lock().unwrap()[0].clone();
            if served {
                assert!(matches!(
                    outcome,
                    SendOutcome::Billed {
                        reported_cost_usd: None,
                        input_tokens: 12,
                        ..
                    }
                ));
            } else {
                assert!(
                    matches!(outcome, SendOutcome::Ambiguous { .. }),
                    "model {model:?}: {outcome:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn managed_response_model_binding_preserves_accounting_and_unmanaged_defaults() {
        let mut accepted_invalid_models = Vec::new();
        for model in [None, Some("unknown")] {
            for usage in [
                json!({ "prompt_tokens": 12 }),
                json!({ "cost": 0.00000024 }),
            ] {
                let mut body = embedding_reply();
                body["usage"] = usage.clone();
                body.as_object_mut().unwrap().remove("model");
                if let Some(model) = model {
                    body["model"] = json!(model);
                }
                let server = Loopback::new(200, body.to_string()).await;
                for terms in [Some(priced_terms()), None] {
                    let description = format!("model={model:?}, usage={usage}, terms={terms:?}");
                    let gate = MeteredGate::new(terms);
                    if server
                        .embedder
                        .embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new())
                        .await
                        .is_ok()
                    {
                        accepted_invalid_models.push(description);
                    }
                    assert_eq!(gate.reserves.load(std::sync::atomic::Ordering::SeqCst), 1);
                    let outcomes = gate.outcomes.lock().unwrap();
                    assert_eq!(outcomes.len(), 1);
                    if usage.get("cost").is_some() {
                        assert_eq!(outcomes[0].reported_cost_usd(), Some(0.00000024));
                    } else {
                        assert_eq!(
                            outcomes[0],
                            SendOutcome::Ambiguous {
                                generation_id: Some("embedding-test".into()),
                            }
                        );
                    }
                }
                assert_eq!(server.requests.lock().unwrap().len(), 2);
                assert_eq!(
                    server.requests.lock().unwrap()[1],
                    embeddings_request_body(&["a reef"])
                );
                let vectors = server.embedder.embed_batch(&inputs(&["a reef"]), ProviderPermit::new()).await.unwrap();
                assert_eq!(vectors.len(), 1);
                assert_eq!(server.requests.lock().unwrap().len(), 3);
            }
        }
        assert!(
            accepted_invalid_models.is_empty(),
            "managed transport returned vectors: {accepted_invalid_models:?}"
        );
    }

    #[tokio::test]
    async fn malformed_managed_vectors_preserve_observed_charge() {
        let mut missing = json!({ "embedding": unit_vector() });
        let mut cases = vec![missing.clone()];
        for index in [json!(-1), json!(0.5), json!("0"), json!(1), json!(u64::MAX)] {
            missing["index"] = index;
            cases.push(missing.clone());
        }
        let mut overflow = unit_vector();
        overflow[0] = json!(1e100);
        cases.push(json!({ "index": 0, "embedding": overflow }));
        cases.push(json!({ "index": 0, "embedding": [1.0] }));
        for item in cases {
            let mut body = embedding_reply();
            body["data"] = json!([item]);
            let server = Loopback::new(200, body.to_string()).await;
            let gate = MeteredGate::new(Some(priced_terms()));
            assert!(
                server
                    .embedder
                    .embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new())
                    .await
                    .is_err(),
                "item {item}"
            );
            assert_eq!(server.requests.lock().unwrap().len(), 1);
            assert_eq!(gate.reserves.load(std::sync::atomic::Ordering::SeqCst), 1);
            let outcomes = gate.outcomes.lock().unwrap();
            assert_eq!(outcomes.len(), 1);
            assert_eq!(outcomes[0].reported_cost_usd(), Some(0.00000024));
        }
    }

    #[tokio::test]
    async fn managed_indexes_must_form_a_complete_permutation() {
        for indexes in [[2, 0, 1], [0, 0, 2], [0, 1, 3]] {
            let mut body = embedding_reply();
            body["data"] = Value::Array(
                indexes
                    .iter()
                    .map(|index| {
                        let mut vector = unit_vector();
                        vector[0] = json!(*index + 1);
                        json!({ "index": index, "embedding": vector })
                    })
                    .collect(),
            );
            let server = Loopback::new(200, body.to_string()).await;
            let gate = MeteredGate::new(Some(priced_terms()));
            let result = server
                .embedder
                .embed_batch_gated(&inputs(&["reef", "coral", "ocean"]), &gate, ProviderPermit::new())
                .await;
            if indexes == [2, 0, 1] {
                assert_eq!(
                    result.unwrap().iter().map(|v| v[0]).collect::<Vec<_>>(),
                    vec![1.0, 2.0, 3.0]
                );
            } else {
                assert!(result.is_err());
            }
            assert_eq!(server.requests.lock().unwrap().len(), 1);
            assert_eq!(
                server.requests.lock().unwrap()[0]["input"],
                json!(["reef", "coral", "ocean"])
            );
            assert_eq!(gate.outcomes.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn every_gated_response_requires_explicit_indexes() {
        let mut accepted_invalid_indexes = Vec::new();
        for (name, indexes, unmanaged_accepts) in [
            ("missing", [None, None], true),
            ("negative", [Some(json!(-1)), Some(json!(-2))], true),
            ("fractional", [Some(json!(0.5)), Some(json!(1.5))], true),
            ("duplicate", [Some(json!(0)), Some(json!(0))], false),
            ("out of range", [Some(json!(0)), Some(json!(2))], false),
        ] {
            let mut body = embedding_reply();
            body["data"] = Value::Array(
                indexes
                    .into_iter()
                    .map(|index| {
                        let mut item = json!({ "embedding": unit_vector() });
                        if let Some(index) = index {
                            item["index"] = index;
                        }
                        item
                    })
                    .collect(),
            );
            let server = Loopback::new(200, body.to_string()).await;
            for terms in [Some(priced_terms()), None] {
                let description = format!("indexes={name}, terms={terms:?}");
                let gate = MeteredGate::new(terms);
                if server
                    .embedder
                    .embed_batch_gated(&inputs(&["reef", "coral"]), &gate, ProviderPermit::new())
                    .await
                    .is_ok()
                {
                    accepted_invalid_indexes.push(description);
                }
                assert_eq!(gate.reserves.load(std::sync::atomic::Ordering::SeqCst), 1);
                let outcomes = gate.outcomes.lock().unwrap();
                assert_eq!(outcomes.len(), 1);
                assert_eq!(outcomes[0].reported_cost_usd(), Some(0.00000024));
            }
            assert_eq!(server.requests.lock().unwrap().len(), 2);
            assert_eq!(
                server.requests.lock().unwrap()[1],
                embeddings_request_body(&["reef", "coral"])
            );
            let result = server.embedder.embed_batch(&inputs(&["reef", "coral"]), ProviderPermit::new()).await;
            assert_eq!(
                result.is_ok(),
                unmanaged_accepts,
                "unmanaged indexes={name}"
            );
            assert_eq!(server.requests.lock().unwrap().len(), 3);
        }
        assert!(
            accepted_invalid_indexes.is_empty(),
            "gated transport accepted invalid indexes: {accepted_invalid_indexes:?}"
        );
    }

    #[test]
    fn usable_zero_cost_or_tokens_are_billed_and_invalid_cost_can_use_tokens() {
        for usage in [
            json!({ "cost": 0 }),
            json!({ "prompt_tokens": 0 }),
            json!({ "cost": -1, "prompt_tokens": 12 }),
            json!({ "cost": 1e308, "prompt_tokens": 12 }),
        ] {
            let body = json!({ "model": CLOUD_EMBED_MODEL, "usage": usage });
            let outcome = embed_outcome(Observed::Body(&body));
            assert!(matches!(outcome, SendOutcome::Billed { .. }), "{outcome:?}");
            if usage["cost"].as_f64().is_some_and(|cost| cost != 0.0) {
                assert_eq!(outcome.reported_cost_usd(), None);
            }
        }
    }

    #[tokio::test]
    async fn timed_out_written_request_is_ambiguous_without_retry() {
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let captured = requests.clone();
        let app = axum::Router::new().route(
            "/v1/embeddings",
            axum::routing::post(move |axum::Json(_): axum::Json<Value>| {
                captured.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move { std::future::pending::<String>().await }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(250))
            .build()
            .unwrap();
        let embedder =
            OpenRouterEmbedder::new(client, "loopback").with_base_url(format!("http://{addr}"));
        let gate = MeteredGate::new(Some(priced_terms()));
        let result = embedder.embed_batch_gated(&inputs(&["reef"]), &gate, ProviderPermit::new()).await;
        task.abort();
        assert!(result.is_err());
        assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(gate.reserves.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            *gate.outcomes.lock().unwrap(),
            vec![SendOutcome::Ambiguous {
                generation_id: None
            }]
        );
    }

    #[test]
    fn finite_f64_components_must_fit_f32() {
        let mut values = unit_vector();
        values[0] = json!(1e100);
        let body = json!({ "data": [{ "index": 0, "embedding": values }] });
        assert!(parse_embeddings_response(&body, 1).is_err());
    }

    #[tokio::test]
    async fn an_over_limit_input_is_its_own_permanent_error() {
        // The body OpenRouter relayed in prod on 2026-10-23.
        let over = r#"{"error":{"message":"Invalid 'input[0]': maximum input length is 8192 tokens.","code":400}}"#;
        let server = Loopback::new(400, over.into()).await;
        let gate = MeteredGate::new(Some(priced_terms()));
        let error = server.embedder.embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new()).await.unwrap_err();
        assert!(matches!(error, EmbedError::InputTooLong(_)), "{error:?}");
        assert_eq!(*gate.outcomes.lock().unwrap(), vec![SendOutcome::NotGenerated]);
        // Any other 400 is reported as the status it was.
        let server = Loopback::new(400, r#"{"error":{"message":"bad request"}}"#.into()).await;
        let gate = MeteredGate::new(Some(priced_terms()));
        let error = server.embedder.embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new()).await.unwrap_err();
        assert!(matches!(error, EmbedError::Http { status: 400, .. }), "{error:?}");
    }

    /// The gateway's verdict on what this transport actually reports, over
    /// a real HTTP exchange: only 429, 5xx and a dead connection are worth a
    /// retry. A proxy's non-JSON 502 is still a 502.
    #[tokio::test]
    async fn the_gateway_classifies_the_wire_status() {
        use crate::embed_gateway::{plan, send, Disposition, Purpose};
        let over = r#"{"error":{"message":"maximum input length is 8192 tokens"}}"#;
        for (status, body, expected) in [
            (400, r#"{"error":{"message":"bad request"}}"#, Disposition::Permanent),
            (400, over, Disposition::Permanent),
            (401, r#"{"error":"no auth"}"#, Disposition::Permanent),
            (402, r#"{"error":"insufficient credits"}"#, Disposition::Permanent),
            (404, "not found", Disposition::Permanent),
            (422, r#"{"error":"unprocessable"}"#, Disposition::Permanent),
            (429, r#"{"error":"rate limited"}"#, Disposition::Transient),
            (500, r#"{"error":"internal"}"#, Disposition::Transient),
            (502, "<html>bad gateway</html>", Disposition::Transient),
            (503, "", Disposition::Transient),
            (200, "not json", Disposition::Permanent),
        ] {
            let server = Loopback::new(status, body.into()).await;
            let gate = MeteredGate::new(Some(priced_terms()));
            let request = &plan(&["a reef"], 8).requests[0];
            let failure = send(&server.embedder, Purpose::Managed(&gate), request)
                .await
                .unwrap_err();
            assert_eq!(failure.disposition, expected, "{status} {body}: {failure}");
            assert_eq!(server.requests.lock().unwrap().len(), 1, "one send, no retry");
        }
        // Nothing listening: a transport failure, transient.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let dead = OpenRouterEmbedder::new(reqwest::Client::new(), "k")
            .with_base_url(format!("http://{addr}"));
        let gate = MeteredGate::new(Some(priced_terms()));
        let request = &plan(&["a reef"], 8).requests[0];
        let failure = send(&dead, Purpose::Managed(&gate), request).await.unwrap_err();
        assert!(matches!(failure.error, EmbedError::Transport(_)), "{failure}");
        assert_eq!(failure.disposition, Disposition::Transient);
    }

    #[tokio::test]
    async fn refusals_and_unreadable_answers_settle_without_retry() {
        for (status, body, expected) in [
            (
                429,
                "{\"error\":\"rate limited\"}",
                SendOutcome::NotGenerated,
            ),
            (
                200,
                "not json",
                SendOutcome::Ambiguous {
                    generation_id: None,
                },
            ),
        ] {
            let server = Loopback::new(status, body.into()).await;
            let gate = MeteredGate::new(Some(priced_terms()));
            assert!(server
                .embedder
                .embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new())
                .await
                .is_err());
            assert_eq!(server.requests.lock().unwrap().len(), 1);
            assert_eq!(gate.reserves.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(*gate.outcomes.lock().unwrap(), vec![expected]);
        }
        let server = Loopback::new(200, embedding_reply().to_string()).await;
        let mut gate = MeteredGate::new(Some(priced_terms()));
        gate.deny = true;
        assert!(server
            .embedder
            .embed_batch_gated(&inputs(&["a reef"]), &gate, ProviderPermit::new())
            .await
            .is_err());
        assert!(server.requests.lock().unwrap().is_empty());
        assert!(gate.outcomes.lock().unwrap().is_empty());
    }

    #[test]
    fn base_url_strips_a_trailing_v1() {
        let client = reqwest::Client::new();
        let embedder = OpenRouterEmbedder::new(client, "sk-or-test")
            .with_base_url("https://openrouter.ai/api/v1/");
        assert_eq!(
            embedder.embeddings_url(),
            "https://openrouter.ai/api/v1/embeddings"
        );
    }
}
