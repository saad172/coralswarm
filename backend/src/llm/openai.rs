use super::retry::RetryPolicy;
use super::{
    CompletionRequest, CompletionResponse, GatedLlm, LlmError, LlmProvider, ReasoningSetting,
    Role, StreamDelta, StreamEvent, StreamFinalEvent,
};
use coralswarm_core::send_gate::{
    self, SendGate, SendOutcome, SendPermit, SendTerms, Sent, TransportPolicy, Unmanaged,
};
use bytes::Bytes;
use futures::StreamExt as _;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;

/// Talks to any OpenAI-compatible `/v1/chat/completions` endpoint —
/// `api.openai.com` (the historical, still-supported default) or
/// OpenRouter's OpenAI-compatible endpoint (via [`Self::openrouter`], the
/// OpenRouter provider port, PR1). The two modes differ only in
/// `base_url`/attribution headers and whether the mandatory OpenRouter
/// no-training `provider` policy object (plus the optional `models`
/// fallback array) is sent — every other request/response field is
/// identical.
pub struct OpenAiProvider {
    client: reqwest::Client,
    /// Client used for the SSE path only. Mirrors
    /// Client used for the SSE path only. See [`Self::with_stream_client`]
    /// for the full "why" (a total-deadline client silently truncates a
    /// long-but-healthy stream; production regression, 2026-08).
    stream_client: reqwest::Client,
    api_key: String,
    base_url: String,
    /// [`LlmProvider::name`]'s return value for this instance.
    provider_name: &'static str,
    /// Whether this instance speaks to OpenRouter specifically — gates the
    /// mandatory no-training `provider` policy object, the optional
    /// `models` fallback array ([`resolve_fallback_model`]), and the
    /// OpenRouter attribution headers. `false` for a plain [`Self::new`]
    /// (real `api.openai.com` or an OpenAI-compatible local proxy), which
    /// must never receive OpenRouter-only request fields.
    is_openrouter: bool,
}

/// Cap a provider response body before it goes into a log line.
///
/// A log line is shipped whole: oversized ones are dropped, not truncated.
/// During the 2026-09-04 outage every failing call logged ~125 KB of HTML in
/// a single field and not one of those lines reached `fly logs`, so a total
/// outage presented as total silence — the worst possible failure mode for
/// the one channel an operator watches during a cutover.
///
/// `jobs.error_message` deliberately still stores the body in full: the
/// database has no line-size limit, and that is where the complete evidence
/// belongs (it is what identified this bug).
const LOG_BODY_MAX_BYTES: usize = 1024;

fn truncate_for_log(body: &str) -> String {
    if body.len() <= LOG_BODY_MAX_BYTES {
        return body.to_string();
    }
    let cut = (0..=LOG_BODY_MAX_BYTES)
        .rev()
        .find(|i| body.is_char_boundary(*i))
        .unwrap_or(0);
    format!("{}… [{} more bytes truncated]", &body[..cut], body.len() - cut)
}

/// Normalizes an OpenAI-compatible API base so that appending
/// `/v1/chat/completions` yields exactly one `/v1` segment.
///
/// The `base_url` field means "everything before `/v1`". That contract is
/// easy to break in the one direction no mock-server test can see: every
/// wire test here points the provider at a bare `http://{addr}`, so a base
/// that wrongly ends in `/v1` still produces a matching request path in the
/// test and a doubled one in production. That is exactly how
/// `https://openrouter.ai/api/v1/v1/chat/completions` reached production on
/// 2026-09-04 — OpenRouter served its HTML 404 page, no request ever reached
/// a model, and its usage counter never moved.
///
/// So rather than rely on callers getting it right, absorb every spelling
/// operators actually use: `https://openrouter.ai/api`, `.../api/v1` and
/// `.../api/v1/` all normalize to the same base. Surrounding whitespace goes
/// too — a `fly secrets set` value piped from a heredoc or a file arrives
/// with a trailing newline, which would otherwise be sent inside the URL.
fn normalize_base_url(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    url.strip_suffix("/v1").unwrap_or(url).trim_end_matches('/').to_string()
}

impl OpenAiProvider {
    /// The key is trimmed on the way in. A trailing newline — what a piped
    /// `fly secrets set OPENROUTER_API_KEY < file` produces — is not a valid
    /// `Authorization: Bearer` header byte, so reqwest poisons the request
    /// builder and `.send()` fails WITHOUT anything leaving the process. That
    /// looks identical to a network outage and, like the doubled-`/v1` bug
    /// above, leaves the provider's usage counter untouched. `lib.rs` already
    /// learned this on `STRIPE_SECRET_KEY`; trimming here rather than at each
    /// `env::var` site covers every caller at once, dev binaries included.
    pub fn new(client: reqwest::Client, api_key: impl Into<String>) -> Self {
        Self {
            stream_client: client.clone(),
            client,
            api_key: api_key.into().trim().to_string(),
            base_url: normalize_base_url("https://api.openai.com"),
            provider_name: "openai",
            is_openrouter: false,
        }
    }

    /// Configure this provider instance for OpenRouter's OpenAI-compatible
    /// endpoint, `https://openrouter.ai/api/v1/chat/completions` by default
    /// — override the base with [`Self::with_base_url`], e.g. from
    /// `OPENROUTER_BASE_URL`.
    ///
    /// Note what `base_url` stores: everything BEFORE `/v1`, because
    /// [`Self::completions_url`] appends `/v1/chat/completions` itself. This
    /// constructor stored `".../api/v1"` between PR1 and 2026-09-04 and so
    /// posted to `.../api/v1/v1/chat/completions`, which OpenRouter answers
    /// with its HTML 404 page. That took production down for the length of a
    /// cutover and no test saw it — see [`normalize_base_url`].
    ///
    /// Turns on the mandatory no-training `provider` policy object and the
    /// optional `models` fallback array on every request (see
    /// [`OpenAiProvider::build_request_body`]) and the OpenRouter
    /// attribution headers, and makes [`LlmProvider::name`] report
    /// `"openrouter"` (rather than `"openai"`) for `llm_usage.provider`.
    pub fn openrouter(client: reqwest::Client, api_key: impl Into<String>) -> Self {
        Self {
            stream_client: client.clone(),
            client,
            api_key: api_key.into().trim().to_string(),
            base_url: normalize_base_url("https://openrouter.ai/api"),
            provider_name: "openrouter",
            is_openrouter: true,
        }
    }

    /// Supply a separate client for [`LlmProvider::stream_complete`] — the
    /// total-deadline-vs-stall-timeout split. A total-deadline client silently
    /// truncates a long-but-healthy synthesis stream (production regression,
    /// 2026-08).
    pub fn with_stream_client(mut self, client: reqwest::Client) -> Self {
        self.stream_client = client;
        self
    }

    /// Override the API base URL (e.g. `OPENROUTER_BASE_URL`).
    ///
    /// The value is run through [`normalize_base_url`] first, so an operator
    /// may set `OPENROUTER_BASE_URL` to any of `https://openrouter.ai/api`,
    /// `https://openrouter.ai/api/v1` or `https://openrouter.ai/api/v1/`
    /// and get the same working endpoint. Normalization also trims
    /// surrounding whitespace, which is what a `fly secrets set` value fed
    /// from a heredoc or a file arrives with.
    ///
    /// In non-test builds only `https://` URLs are accepted — an SSRF
    /// guard: a misconfigured non-https base would send the API key to
    /// whatever that URL resolves to. The guard runs on the NORMALIZED value
    /// so that leading whitespace fails closed rather than sneaking past it.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        let url = normalize_base_url(&url.into());
        // The `e2e` build (debug-only, see `lib.rs`) talks to an in-runner
        // fake over http.
        #[cfg(not(any(test, feature = "test-support", feature = "e2e")))]
        assert!(
            url.starts_with("https://"),
            "with_base_url: only https:// URLs are accepted in production; got {:?}",
            url
        );
        self.base_url = url;
        self
    }

    /// The ONE place the request URL is composed, for both the blocking and
    /// the SSE path. `base_url` never carries `/v1` ([`normalize_base_url`]),
    /// so this is the only site allowed to add it.
    fn completions_url(&self) -> String {
        format!("{}/v1/chat/completions", self.base_url)
    }

    /// `HTTP-Referer`/`X-Title` OpenRouter attribution headers — sent only
    /// when this instance is actually pointed at OpenRouter (harmless
    /// either way, but real `api.openai.com` has no use for them).
    fn attribution_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        if self.is_openrouter {
            headers.insert(
                "HTTP-Referer",
                reqwest::header::HeaderValue::from_static("https://coralswarm.ai"),
            );
            headers.insert("X-Title", reqwest::header::HeaderValue::from_static("CoralSwarm"));
        }
        headers
    }

    /// Build the JSON request body shared by [`LlmProvider::complete`] and
    /// [`LlmProvider::stream_complete`] — messages, forced-tool `tools`/
    /// `tool_choice` (mirrors `anthropic::build_complete_body`'s
    /// `tool_schema` handling), the mandatory `reasoning` field
    /// ([`reasoning_json`]), and — ONLY when [`Self::is_openrouter`] — two
    /// OpenRouter-only fields:
    ///
    /// - `provider`: a POLICY object, not a host allowlist. Before
    ///   2026-09-05 this pinned a hardcoded per-model inference-host
    ///   allowlist (`order` + `allow_fallbacks: false`). OpenRouter's
    ///   provider-side quantization offerings drifted under that pin and
    ///   eliminated every allowlisted host for `z-ai/glm-5.3-flash`
    ///   (measured: DeepInfra now serves it at fp4, Fireworks/Cloudflare
    ///   report quantization "unknown" — all three fail the
    ///   `quantizations: ["fp8","bf16"]` filter), 404ing every capable-tier
    ///   call with "No endpoints found". The founder's actual requirement
    ///   was never "these specific hosts" — it is only "no provider may
    ///   train on our data", which OpenRouter enforces server-side via
    ///   `data_collection: "deny"` + `zdr: true` regardless of which host
    ///   ends up serving the request. So `order` is gone and
    ///   `allow_fallbacks` is `true`: OpenRouter is free to route to ANY
    ///   ZDR-compliant, non-training host at fp8/bf16 quality or better.
    ///   `quantizations` stays as a QUALITY floor (never fp4), not a
    ///   privacy control — kept unconditionally, same as
    ///   `require_parameters`, which is verified (live smoke test,
    ///   2026-09-05) not to shrink the eligible pool.
    /// - `models`: an optional single-element backup-model array — see
    ///   [`resolve_fallback_model`] for the default mapping and the
    ///   quality-for-availability rationale. **Known limitation** (measured,
    ///   not something to try to solve here): `models` rescues a primary
    ///   that resolves to zero routable endpoints (this pin's own outage
    ///   case), and the `provider` policy object above still applies to
    ///   whichever model actually serves the fallback — but it does NOT
    ///   rescue an invalid/retired model id. That case 400s on request
    ///   validation before routing is ever attempted, same as it would with
    ///   no `models` key at all.
    ///
    /// `streaming` adds `stream`/`stream_options.include_usage`.
    ///
    /// Infallible: unlike the pinned-allowlist design this replaces (which
    /// could fail closed with [`LlmError::ModelNotFound`] on an unrecognized
    /// model), nothing in this function can fail, so it returns a bare
    /// `serde_json::Value` rather than a `Result`. If a future addition here
    /// becomes fallible again, give this a `Result<_, LlmError>` return type
    /// THEN — don't carry one pre-emptively with no reachable `Err` arm.
    ///
    /// `terms` (COR-21) are what a send gate priced. When present they replace
    /// the model and the fallback list, and carry `provider.max_price`, so the
    /// request can only be served by a model the gate priced, at or under the
    /// price it reserved for.
    fn build_request_body(
        &self,
        req: &CompletionRequest,
        streaming: bool,
        terms: Option<&SendTerms>,
    ) -> serde_json::Value {
        let mut messages: Vec<serde_json::Value> = Vec::new();

        if let Some(sys) = &req.system {
            messages.push(json!({
                "role": "system",
                "content": sys,
            }));
        }

        for m in &req.messages {
            messages.push(json!({
                "role": role_str(&m.role),
                "content": m.content,
            }));
        }

        let model = terms
            .and_then(|t| t.models.first())
            .map(String::as_str)
            .unwrap_or(&req.model);
        let mut body = json!({
            "model": model,
            "max_tokens": req.max_tokens,
            "messages": messages,
        });

        // Audit H3 parity: force schema-valid JSON via a single tool +
        // forced tool_choice, mirroring `anthropic::build_complete_body`.
        if let Some(ts) = &req.tool_schema {
            body["tools"] = json!([{
                "type": "function",
                "function": {
                    "name": ts.name,
                    "parameters": ts.schema,
                }
            }]);
            if ts.strict {
                body["tools"][0]["function"]["strict"] = json!(true);
            }
            body["tool_choice"] = json!({
                "type": "function",
                "function": { "name": ts.name },
            });
        }

        // Reasoning must be set explicitly on every request — see
        // `CompletionRequest::reasoning`'s own doc. Priced terms may be served
        // by any candidate, so the one `reasoning` field must suit them all.
        body["reasoning"] = match terms {
            Some(terms) => reasoning_json_for(&terms.models, model, req.reasoning),
            None => reasoning_json(model, req.reasoning),
        };

        if self.is_openrouter {
            body["provider"] = json!({
                "zdr": true,
                "data_collection": "deny",
                "quantizations": ["fp8", "bf16"],
                "allow_fallbacks": true,
                "require_parameters": true,
            });

            match terms {
                Some(terms) => {
                    if terms.models.len() > 1 {
                        body["models"] = json!(terms.models[1..]);
                    }
                    if let Some(price) = terms.max_price {
                        // The gate prices tokens only, so no per-request fee.
                        body["provider"]["max_price"] = json!({
                            "prompt": price.prompt,
                            "completion": price.completion,
                            "request": 0,
                        });
                    }
                }
                None => {
                    if let Some(backup) = resolve_fallback_model(&req.model) {
                        body["models"] = json!([backup]);
                    }
                }
            }
        }

        if streaming {
            body["stream"] = json!(true);
            body["stream_options"] = json!({ "include_usage": true });
        }

        body
    }

    /// Shared prefix of [`LlmProvider::complete`]'s and
    /// [`LlmProvider::stream_complete`]'s STREAM-OPEN retry loop:
    /// POSTs `body` and
    /// checks the response status, but does NOT consume the body — the
    /// non-streaming caller reads it as JSON, the streaming caller reads it
    /// as an SSE byte stream. Returns the still-unread `reqwest::Response`
    /// on a 200, or `(error, retry_after_hint)` otherwise so callers can
    /// honour `Retry-After`.
    ///
    /// `client` is passed in rather than read off `self` because the two
    /// callers need different timeout semantics — see
    /// [`Self::with_stream_client`]. A thin wrapper around [`post_and_check`]
    /// (a free function, not a method) — see that function's own doc for why
    /// the mid-stream reopen-and-retry loop needs the OWNED-parameters shape
    /// it has, rather than borrowing `&self`.
    async fn send_and_check(
        &self,
        permit: SendPermit,
        client: &reqwest::Client,
        body: &serde_json::Value,
        timeout: Option<Duration>,
    ) -> (Sent, SendResult) {
        post_and_check(
            permit,
            client,
            &self.completions_url(),
            &self.api_key,
            self.attribution_headers(),
            body,
            timeout,
        )
        .await
    }
}

/// Every model an OpenRouter request for `primary` may be served by: the
/// primary, then its `models` fallback. Shared by the request body and the
/// managed worst-case bound, so the two cannot price different sets.
///
/// `fallback`, when given, replaces the global [`resolve_fallback_model`] map
/// for this one request (source extraction's
/// `SOURCE_EXTRACTION_FALLBACK_MODEL`). A blank fallback, or one naming the
/// primary itself, is ignored: a model is never its own fallback.
pub(crate) fn candidate_models(primary: &str, fallback: Option<&str>) -> Vec<String> {
    let mut models = vec![primary.to_string()];
    match fallback.map(str::trim).filter(|f| !f.is_empty() && *f != primary) {
        Some(fallback) => models.push(fallback.to_string()),
        None => models.extend(resolve_fallback_model(primary)),
    }
    models
}

type SendResult = Result<reqwest::Response, (LlmError, Option<Duration>, SendOutcome)>;

/// What one attempt showed the transport, in the order it can be observed.
enum Observed<'a> {
    /// `reqwest` never wrote the request (connect failure).
    NotWritten,
    /// Written, then a timeout or a reset before any status.
    SendFailed,
    /// An HTTP answer other than 200.
    Status,
    /// 200 with a decoded body. The str is the requested model.
    Body(&'a OpenAiResponse, &'a str),
}

/// The one outcome classification for a completion attempt (COR-21). A body
/// that carries usage was generated and billed, whatever else it says; one
/// that was written and never answered usefully is ambiguous; an HTTP refusal
/// generated nothing.
fn classify_attempt(observed: Observed<'_>) -> SendOutcome {
    match observed {
        Observed::NotWritten | Observed::Status => SendOutcome::NotGenerated,
        Observed::SendFailed => {
            SendOutcome::Ambiguous { generation_id: None }
        }
        Observed::Body(body, requested) => match &body.usage {
            Some(usage) => SendOutcome::Billed {
                reported_cost_usd: usage.cost,
                input_tokens: u64::from(usage.prompt_tokens),
                output_tokens: u64::from(usage.completion_tokens),
                served_model: body.model.clone().unwrap_or_else(|| requested.to_string()),
                upstream: body.provider.clone(),
                generation_id: body.id.clone(),
            },
            None => SendOutcome::Ambiguous {
                generation_id: body.id.clone(),
            },
        },
    }
}

/// The actual body of what used to be [`OpenAiProvider::send_and_check`] —
/// factored into a free function taking OWNED/borrowed primitives instead of
/// `&self` so it can be called from BOTH: (1) [`OpenAiProvider::send_and_check`]
/// (used by [`OpenAiProvider::complete_once`] and the initial STREAM-OPEN
/// loop), and (2) directly, from INSIDE [`LlmProvider::stream_complete`]'s
/// `async_stream::stream!` generator, to re-open a fresh connection on a
/// mid-stream retry (item 1, streaming path). That generator cannot borrow
/// `&self` — the returned `Pin<Box<dyn Stream + Send>>` has no lifetime tied
/// to the `&self` the trait method was called with, so this needs to move in
/// owned copies of everything `send_and_check` used to read off `self`
/// (`client`, the composed `url`, `api_key`, and `headers`) instead.
///
/// COR-21: it consumes a [`SendPermit`], so no completion is sent without a
/// gate's admission, and hands back the [`Sent`] for the caller to settle.
///
/// `timeout`, when set, replaces the client's total timeout for this one
/// request (a [`TransportPolicy`] attempt bound).
async fn post_and_check(
    permit: SendPermit,
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    headers: reqwest::header::HeaderMap,
    body: &serde_json::Value,
    timeout: Option<Duration>,
) -> (Sent, SendResult) {
    let sent = permit.into_sent();
    let result = post_and_check_inner(client, url, api_key, headers, body, timeout).await;
    (sent, result)
}

async fn post_and_check_inner(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    headers: reqwest::header::HeaderMap,
    body: &serde_json::Value,
    timeout: Option<Duration>,
) -> SendResult {
    let mut request = client
        .post(url)
        .bearer_auth(api_key)
        .header("content-type", "application/json")
        .headers(headers)
        .json(body);
    if let Some(timeout) = timeout {
        request = request.timeout(timeout);
    }
    let response = request
        .send()
        .await
        .map_err(|e| {
            let outcome = classify_attempt(if e.is_connect() {
                Observed::NotWritten
            } else {
                Observed::SendFailed
            });
            let err = if e.is_timeout() {
                LlmError::Timeout
            } else {
                LlmError::Upstream {
                    status: 0,
                    body: e.to_string(),
                }
            };
            (err, None, outcome)
        })?;

    let status = response.status().as_u16();
    if status != 200 {
        let retry_after = super::retry::parse_retry_after(response.headers());
        let body_text = response.text().await.unwrap_or_default();
        if status == 404 {
            // A 404 is one of two very different things, and the log line
            // has to say which. Either a permanent provider/model
            // constraint mismatch (every allowlisted backend refuses this
            // request), or — the 2026-09-04 production outage — a request
            // that never reached the API at all and was answered by
            // OpenRouter's marketing site. Logging the URL separates them
            // instantly. Deliberately NOT remapped to transient: only the
            // documented 503 ("no available provider") is transient, and
            // `map_status`'s `>= 500` branch already covers that.
            let looks_like_html = body_text.trim_start().starts_with('<');
            tracing::error!(
                url = %url,
                body_bytes = body_text.len(),
                body_prefix = %truncate_for_log(&body_text),
                "OpenRouter: 404 — {}",
                if looks_like_html {
                    "the body is HTML, not JSON, so this request never reached the API. \
                     Check `url`: it must contain exactly one /v1 segment"
                } else {
                    "likely a permanent provider/model constraint mismatch (e.g. every \
                     allowlisted backend refuses this request); see `body_prefix` for the \
                     constraint that failed"
                }
            );
        }
        return Err((map_status(status, body_text), retry_after, classify_attempt(Observed::Status)));
    }

    Ok(response)
}

// ── Reasoning ──────────────────────────────────────────────────────────────────

/// Model ids whose OpenRouter deployment REFUSES to disable reasoning at
/// all — sending the otherwise-universal `Off` encoding (`{"enabled":
/// false}`) to one of these gets a hard `400` back, not a degraded-but-
/// accepted response.
///
/// **Live smoke-test finding (2026-09-03):** `z-ai/glm-5.3-flash` answers
/// `{"enabled": false}` with
/// `HTTP 400 {"error":{"message":"Reasoning is mandatory for this endpoint \
/// and cannot be disabled.","code":400}}`. Every capable-tier background job
/// (journal compile, expertise inference, summary-reduce, extraction
/// escalation) and the Ask Smart tier's "off" dial rung send exactly that
/// for GLM, so — unpatched — all of them fail against OpenRouter.
///
/// **Fix:** for a model in this set, `Off` is instead sent as
/// `{"effort": "low"}` — see [`reasoning_json`]. This is not a cosmetic
/// substitute; it was chosen from measured behaviour, on GLM via DeepInfra,
/// of the smallest prompt actually reasoning under each candidate encoding
/// (`completion_tokens_details.reasoning_tokens`, cost for a trivial
/// prompt in parens):
///   - `{"effort": "low"}` → **200**, `reasoning_tokens = 0` (cost $2.2e-6) — used here
///   - `{"effort": "minimal"}` → 200, `reasoning_tokens = 64` (worse than `low`)
///   - `reasoning` omitted entirely → 200, `reasoning_tokens = 4`
///   - `{"effort": "low", "exclude": true}` → 200, `reasoning_tokens = 34`
///
/// `{"effort": "low"}` is therefore not merely accepted where `{"enabled":
/// false}` is rejected — it is also the encoding that empirically produces
/// the FEWEST reasoning tokens on this model, i.e. zero. That is why the web
/// dial, `model_config.rs`, and the Smart-tier "off" rung are deliberately
/// left untouched by this fix: from the caller's perspective "off" on GLM
/// still behaves like reasoning is off (zero reasoning tokens billed), it is
/// just spelled differently on the wire for this one model.
///
/// `deepseek/deepseek-v4-flash-0731` (the cheap tier's only model) accepts
/// `{"enabled": false}` fine and is NOT in this set.
///
/// Matched the same way `billing::rates::PriceSheet::rate` matches a served
/// model id against its `:nitro`-style OpenRouter routing-variant suffix
/// (see that function's own doc comment): CONTAINMENT, not exact string
/// equality, via [`requires_mandatory_reasoning`] — so
/// `z-ai/glm-5.3-flash:nitro` still resolves to the same floor as
/// `z-ai/glm-5.3-flash`.
const MANDATORY_REASONING_MODELS: &[&str] = &["z-ai/glm-5.3-flash"];

/// `true` when `model` cannot accept `{"enabled": false}` and must instead
/// receive `{"effort": "low"}` for [`ReasoningSetting::Off`] — see
/// [`MANDATORY_REASONING_MODELS`]'s doc comment for the full "why" and the
/// measured evidence.
fn requires_mandatory_reasoning(model: &str) -> bool {
    MANDATORY_REASONING_MODELS.iter().any(|pattern| model.contains(pattern))
}

/// Whether a request any of `models` may serve gets a reasoning floor for
/// [`ReasoningSetting::Off`] (see [`reasoning_json_for`]).
#[cfg(test)]
pub(crate) fn any_requires_mandatory_reasoning(models: &[String]) -> bool {
    models.iter().any(|m| requires_mandatory_reasoning(m))
}

/// Render a [`ReasoningSetting`] as the OpenAI/OpenRouter `reasoning`
/// request field — `Off` as `{"enabled": false}` for every model EXCEPT the
/// ones in [`MANDATORY_REASONING_MODELS`] (which get `{"effort": "low"}`
/// instead, since they 400 on `{"enabled": false}`), and the other three
/// levels as `{"effort": "low"|"medium"|"high"}` regardless of model.
fn reasoning_json(model: &str, setting: ReasoningSetting) -> serde_json::Value {
    match setting {
        ReasoningSetting::Off => {
            if requires_mandatory_reasoning(model) {
                json!({ "effort": "low" })
            } else {
                json!({ "enabled": false })
            }
        }
        ReasoningSetting::Low => json!({ "effort": "low" }),
        ReasoningSetting::Medium => json!({ "effort": "medium" }),
        ReasoningSetting::High => json!({ "effort": "high" }),
    }
}

/// [`reasoning_json`] for a request any of `models` may serve. A candidate
/// that cannot turn reasoning off sets the encoding for all of them, so a
/// fallback is never sent an `Off` it rejects.
fn reasoning_json_for(models: &[String], primary: &str, setting: ReasoningSetting) -> serde_json::Value {
    let model = models
        .iter()
        .map(String::as_str)
        .find(|m| requires_mandatory_reasoning(m))
        .unwrap_or(primary);
    reasoning_json(model, setting)
}

// ── OpenRouter model-level fallback (availability over quality) ────────────────

/// Default primary -> backup model mapping for the `models` array
/// [`OpenAiProvider::build_request_body`] adds to every OpenRouter request —
/// see that function's own doc comment for the full mechanism and the
/// 2026-09-05 outage this whole module is being fixed for.
///
/// Deliberately a CROSS-fallback between the two already-approved production
/// models, and deliberately trading QUALITY for AVAILABILITY: if
/// `z-ai/glm-5.3-flash` has zero routable endpoints (exactly the measured
/// 2026-09-05 quantization-collapse case), OpenRouter falls through to
/// `deepseek/deepseek-v4-flash-0731` instead — a weaker model for that call,
/// but one that keeps synthesis running rather than 404ing outright. That is
/// the founder's explicit "never down" preference. Symmetric the other way:
/// `deepseek/deepseek-v4-flash-0731` falls back to `z-ai/glm-5.3-flash`.
///
/// **This is the knob to change** if that availability-over-quality trade is
/// ever unwanted for one direction or both — either edit this map directly,
/// or set `OPENROUTER_FALLBACK_MODEL_JSON` (see [`resolve_fallback_model`])
/// without a deploy.
///
/// Any model not named here gets no `models` key at all (never an empty
/// array) — see [`resolve_fallback_model`].
///
/// Standing source extraction (v2 and v3) does not use this map: it names its own
/// fallback per managed call (`SOURCE_EXTRACTION_FALLBACK_MODEL`, see
/// [`candidate_models`]) so its request can run with reasoning off.
fn default_fallback_model(model: &str) -> Option<&'static str> {
    // Suffix-tolerant, deliberately matching `requires_mandatory_reasoning`
    // (and `billing::rates::PriceSheet::rate`): a `:nitro`/`:floor` routing
    // variant is the SAME underlying model and must keep its availability
    // fallback. An exact match here would hand a variant id the GLM reasoning
    // floor but no `models` key — so the one request shape most likely to be
    // tuned for throughput would be the one left unprotected against exactly
    // the zero-endpoint collapse this fallback exists to survive.
    if model.contains("deepseek/deepseek-v4-flash-0731") {
        Some("z-ai/glm-5.3-flash")
    } else if model.contains("z-ai/glm-5.3-flash") {
        Some("deepseek/deepseek-v4-flash-0731")
    } else {
        None
    }
}

/// Resolve `model`'s OpenRouter fallback model id, if any — the hardcoded
/// [`default_fallback_model`] map, overridable via
/// `OPENROUTER_FALLBACK_MODEL_JSON` (a JSON object mapping primary model id
/// -> backup model id) so the founder can retune the backup without a
/// deploy. Unlike the removed provider-allowlist override this replaces
/// (which could only ADD coverage, never touch a production pin), this
/// override CAN replace the hardcoded mapping for a production model id too
/// — availability tuning is exactly the founder-facing knob this env var is
/// for.
///
/// Lookup is per-model: if the parsed override JSON contains a (trimmed) key
/// equal to `model`, that entry wins; otherwise this falls through to
/// [`default_fallback_model`] — an override that only names one of the two
/// production models leaves the other on its hardcoded default. A
/// completely unset or unparseable override behaves the same way (falls
/// through to the hardcoded default for every model).
///
/// Every key and value read from the override is trimmed before use — the
/// same `fly secrets set OPENROUTER_FALLBACK_MODEL_JSON < file` trailing-
/// newline gotcha [`crate::llm::model_config::model_from_env`]'s own doc
/// comment documents for a single-value env var applies here too, just one
/// level down (inside the JSON object's own string values) rather than on
/// the raw env string. An override entry is skipped — with a
/// `tracing::warn!`, falling through to the hardcoded default for that
/// model rather than silently sending something broken — when its (trimmed)
/// value is blank, or when it is self-referential (a model must never list
/// itself as its own `models` fallback: OpenRouter would just be asked to
/// retry the same dead primary).
fn resolve_fallback_model(model: &str) -> Option<String> {
    if let Ok(raw) = std::env::var("OPENROUTER_FALLBACK_MODEL_JSON") {
        match serde_json::from_str::<std::collections::HashMap<String, String>>(raw.trim()) {
            Ok(map) => {
                for (raw_key, raw_value) in &map {
                    let key = raw_key.trim();
                    if key != model {
                        continue;
                    }
                    let value = raw_value.trim();
                    if value.is_empty() {
                        tracing::warn!(
                            model = %model,
                            "OPENROUTER_FALLBACK_MODEL_JSON supplied a blank fallback for this \
                             model — ignoring it and falling back to the hardcoded default \
                             rather than sending a broken `models` entry"
                        );
                        break;
                    }
                    if value == key {
                        tracing::warn!(
                            model = %model,
                            "OPENROUTER_FALLBACK_MODEL_JSON maps this model to itself — ignoring \
                             it and falling back to the hardcoded default; a model can never be \
                             its own fallback"
                        );
                        break;
                    }
                    return Some(value.to_string());
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "OPENROUTER_FALLBACK_MODEL_JSON is set but failed to parse as a JSON object \
                     of model id -> backup model id; ignoring it for this request"
                );
            }
        }
    }

    default_fallback_model(model).map(|s| s.to_string())
}

// ── Response shapes ───────────────────────────────────────────────────────────

/// The `error` object OpenAI/OpenRouter embed in an otherwise-200 response
/// or SSE chunk when something goes wrong AFTER the request is accepted.
///
/// Two shapes this exists for (audit: cutover-blocking #452-class bug —
/// without this, a mid-stream failure parsed "successfully" as an empty/
/// partial answer that then got PERSISTED to `query_history` as complete):
///
/// 1. Non-streaming: a `200 OK` whose body is ONLY `{"error": {...}}` — no
///    `choices`/`usage`/`model` at all. See [`OpenAiResponse`], where those
///    three fields are `Option` for exactly this reason.
/// 2. Streaming: once an SSE stream has started, OpenRouter never reverts
///    the HTTP status away from 200 — a mid-stream failure arrives as a
///    normal `data: {...}` chunk carrying this same `error` field (commonly
///    alongside `"choices":[{"finish_reason":"error"}]`), with NO `[DONE]`
///    line after it. See [`StreamChunk::error`].
///
/// Both call sites check this field FIRST, before reading anything else off
/// the parsed value — see [`openai_error_to_llm_error`].
#[derive(Debug, Deserialize)]
struct OpenAiErrorBody {
    /// Usually a numeric HTTP-equivalent status code (`429`, `500`, ...),
    /// tolerated as a numeric STRING too (`"429"`) since neither OpenAI's
    /// nor OpenRouter's error-shape docs guarantee the wire type. Anything
    /// else (a symbolic code, or the field absent) is `None` — routed to a
    /// generic transient `Upstream` error by [`openai_error_to_llm_error`]
    /// rather than failing to parse the whole response.
    #[serde(default, deserialize_with = "deserialize_flexible_error_code")]
    code: Option<u16>,
    message: Option<String>,
}

fn deserialize_flexible_error_code<'de, D>(deserializer: D) -> Result<Option<u16>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    Ok(value.and_then(|v| match v {
        serde_json::Value::Number(n) => n.as_u64().map(|x| x as u16),
        serde_json::Value::String(s) => s.trim().parse::<u16>().ok(),
        _ => None,
    }))
}

/// Map an embedded [`OpenAiErrorBody`] to an [`LlmError`] — reuses
/// [`map_status`] when `code` parses as a status-like number (so 429/401/
/// 403/404/408/5xx all get their usual classification, including
/// transience), else a generic transient `Upstream { status: 0, .. }`
/// (unknown shape; the safe default is to let a worker retry rather than
/// assume the failure is permanent).
fn openai_error_to_llm_error(error: OpenAiErrorBody) -> LlmError {
    let body = error
        .message
        .unwrap_or_else(|| "OpenAI/OpenRouter reported an error with no message".to_string());
    match error.code {
        Some(code) => map_status(code, body),
        None => LlmError::Upstream { status: 0, body },
    }
}

/// A forced tool call's `function.arguments` — a JSON-encoded STRING per the
/// OpenAI/OpenRouter wire format (unlike Anthropic's `tool_use.input`, which
/// is an embedded JSON object) — parsed with `serde_json`, never
/// string-matched (audit H3 parity).
#[derive(Debug, Deserialize)]
struct OpenAiFunctionCall {
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiToolCall {
    function: OpenAiFunctionCall,
}

#[derive(Debug, Deserialize)]
struct OpenAiMessage {
    content: Option<String>,
    /// Present when the model actually called the forced tool. `None`/empty
    /// is the lenient-fallback case: a model that answered in plain text
    /// despite forced tool_choice (mirrors `anthropic::extract_content`'s
    /// identical fallback).
    #[serde(default)]
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    message: OpenAiMessage,
    /// `"stop"`/`"length"`/`"tool_calls"`/`"error"`/... — mapped to
    /// Anthropic's `stop_reason` vocabulary by [`map_finish_reason`] so
    /// `is_truncated`/the truncation-aware retries in `synthesis.rs` and
    /// `workers::digest` work identically regardless of provider.
    /// `"error"` is handled specially (as an `Err`, never a successful
    /// `CompletionResponse`) — see `complete()`'s own handling.
    #[serde(default)]
    finish_reason: Option<String>,
}

/// OpenAI's `usage.prompt_tokens_details` (present on cache-enabled requests —
/// prompt caching is automatic on the OpenAI side, no opt-in request field
/// needed, unlike Anthropic's `cache_control`). `cached_tokens` is the
/// documented field (tokens served FROM cache, a cache hit); `cache_write_tokens`
/// is speculative — some newer models reportedly report a cache-write count
/// here too — so it's treated the same as `cached_tokens`: optional, defaulting
/// to 0 when the provider doesn't send it. Absent entirely on older models /
/// calls that didn't hit or write cache, so the whole struct is optional on its
/// parent.
#[derive(Debug, Deserialize)]
struct OpenAiPromptTokensDetails {
    #[serde(default)]
    cached_tokens: Option<u32>,
    #[serde(default)]
    cache_write_tokens: Option<u32>,
}

/// Maps an optional `prompt_tokens_details` to `(cache_read, cache_creation)`,
/// defaulting both to `0` when absent — shared by the non-streaming and
/// streaming (SSE) paths so the two can't drift.
fn cache_tokens_from_details(details: &Option<OpenAiPromptTokensDetails>) -> (u32, u32) {
    match details {
        Some(d) => (d.cached_tokens.unwrap_or(0), d.cache_write_tokens.unwrap_or(0)),
        None => (0, 0),
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    /// See [`OpenAiPromptTokensDetails`]. Absent on non-cached responses / older
    /// models, in which case cache stats default to 0.
    #[serde(default)]
    prompt_tokens_details: Option<OpenAiPromptTokensDetails>,
    /// Provider-reported cost in USD for this exact call (audit: PR3,
    /// usage-metering plan §2 item 7). This is NOT a standard OpenAI field —
    /// real `api.openai.com` never sends it. OpenRouter's OpenAI-compatible
    /// endpoint reports it here automatically (the request-side
    /// `usage.include` opt-in is deprecated/a no-op). Absent (the default,
    /// for every provider this client isn't pointed at OpenRouter for) ⇒
    /// `None`, which [`CompletionResponse::cost_usd`] preserves as `None` —
    /// never defaulted to `0.0`.
    #[serde(default)]
    cost: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponse {
    /// `#[serde(default)]`: an error-only body (see [`OpenAiErrorBody`])
    /// has no `choices` at all.
    #[serde(default)]
    choices: Vec<OpenAiChoice>,
    /// `Option`, not required: an error-only body has no `usage` either —
    /// see [`OpenAiErrorBody`]'s doc. Checked (and turned into a
    /// [`LlmError::Parse`]) only AFTER confirming `error` is absent.
    #[serde(default)]
    usage: Option<OpenAiUsage>,
    /// `Option` for the same reason as `usage`.
    #[serde(default)]
    model: Option<String>,
    /// See [`OpenAiErrorBody`] — checked FIRST, before `choices`/`usage`/
    /// `model` are ever read.
    #[serde(default)]
    error: Option<OpenAiErrorBody>,
    /// The ACTUAL upstream inference host OpenRouter routed this request to
    /// (e.g. `"DeepInfra"`, `"Fireworks"`) — a top-level field on EVERY
    /// OpenRouter response, success or error (OpenRouter retry fix,
    /// 2026-09-07). Absent on real `api.openai.com` (`#[serde(default)]`
    /// covers that, and every non-OpenRouter provider's response). Reported
    /// into [`super::UPSTREAM_PROVIDER`] via [`super::report_upstream_provider`]
    /// so `llm_usage.upstream_provider` can attribute a failure — the whole
    /// reason this fix exists is "we cannot see which host fails" (16% of
    /// `z-ai/glm-5.3-flash` calls, measured over 7 days). Never confused with
    /// the pre-existing `llm_usage.provider` column, which stays the
    /// constant `"openrouter"`/`"anthropic"`/etc. this crate's own
    /// [`LlmProvider::name`] returns — that column is what rate lookups key
    /// on and must never change value.
    #[serde(default)]
    provider: Option<String>,
    /// OpenRouter's generation id, recorded on the attempt's receipt.
    #[serde(default)]
    id: Option<String>,
}

// ── SSE / streaming shapes ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct StreamDeltaRaw {
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    delta: Option<StreamDeltaRaw>,
    /// See [`OpenAiChoice::finish_reason`] — same field, same mapping via
    /// [`map_finish_reason`], carried on whichever chunk names it (usually
    /// the chunk immediately before the terminal usage-only chunk).
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamUsage {
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
    /// See [`OpenAiPromptTokensDetails`]. On the streaming path this arrives on
    /// the same final usage chunk as `prompt_tokens`/`completion_tokens`
    /// (`stream_options.include_usage`).
    #[serde(default)]
    prompt_tokens_details: Option<OpenAiPromptTokensDetails>,
    /// See [`OpenAiUsage::cost`] — same OpenRouter-only, opt-in field, on the
    /// same final usage chunk.
    #[serde(default)]
    cost: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StreamChunk {
    #[serde(default)]
    choices: Option<Vec<StreamChoice>>,
    usage: Option<StreamUsage>,
    /// The served model, carried on every `chat.completion.chunk` (audit:
    /// PR3, served-model capture). `None` only on a malformed/nonstandard
    /// chunk; the caller falls back to the requested model in that case —
    /// see `StreamFinalEvent::model`'s doc comment.
    #[serde(default)]
    model: Option<String>,
    /// See [`OpenAiErrorBody`] — a mid-stream failure chunk. Checked FIRST,
    /// before `model`/`choices`/`usage` on this same chunk are read.
    #[serde(default)]
    error: Option<OpenAiErrorBody>,
    /// See [`OpenAiResponse::provider`] — the SAME top-level field, carried
    /// on every SSE chunk rather than once on a single body. Tracked
    /// "running" (last-chunk-wins) by [`LlmProvider::stream_complete`],
    /// mirroring how `model` above is tracked.
    #[serde(default)]
    provider: Option<String>,
}

// ── HTTP error mapping ────────────────────────────────────────────────────────

fn map_status(status: u16, body: String) -> LlmError {
    match status {
        // OpenRouter/OpenAI use 408 for a request timeout — transient
        // (`LlmError::Timeout` is retried, see `LlmError::is_transient`).
        408 => LlmError::Timeout,
        429 => LlmError::RateLimit(body),
        401 | 403 => LlmError::Auth(body),
        404 => LlmError::ModelNotFound(body),
        _ => LlmError::Upstream { status, body },
    }
}

// ── Role helper ───────────────────────────────────────────────────────────────

fn role_str(role: &Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

// ── Tool-call / finish-reason parity with the Anthropic path (audit H3) ────────

/// Collapse a response choice into the string callers consume — mirrors
/// `anthropic::extract_content` exactly (same "parse then re-serialize the
/// structured value, lenient plain-text fallback" shape) so no consumer of
/// either provider needs to change.
///
/// Forced-tool path (`forced_tool: true`, i.e. `req.tool_schema` was set):
/// prefer the first `tool_calls[0].function.arguments` — a JSON-encoded
/// STRING on this wire format, parsed with `serde_json` (never
/// string-matched) into a `serde_json::Value`, then re-serialized so callers
/// get canonical JSON text regardless of the model's exact formatting. If
/// `arguments` fails to parse (a malformed tool call — should not happen but
/// is not fatal), the raw string is passed through as-is: still something
/// for the caller's own lenient brace-slice fallback to work with. If NO
/// tool call is present at all (the model answered in plain text despite
/// forced `tool_choice` — the same "shouldn't happen" case
/// `anthropic::extract_content` tolerates), falls back to `message.content`.
fn extract_openai_content(choice: OpenAiChoice, forced_tool: bool) -> String {
    if forced_tool {
        if let Some(tool_call) = choice
            .message
            .tool_calls
            .and_then(|mut calls| if calls.is_empty() { None } else { Some(calls.remove(0)) })
        {
            let raw = tool_call.function.arguments;
            return match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(value) => serde_json::to_string(&value).unwrap_or(raw),
                Err(_) => raw,
            };
        }
    }
    choice.message.content.unwrap_or_default()
}

/// Map OpenAI/OpenRouter's `finish_reason` onto the `stop_reason`
/// vocabulary `is_truncated` expects
/// (`llm::STOP_REASON_MAX_TOKENS`) and every truncation-aware retry
/// (`synthesis::synthesize`, `workers::digest::regenerate`) work
/// identically regardless of which provider actually served the call.
/// `"length"` -> the `max_tokens` sentinel, `"stop"` ->
/// `"end_turn"`, `"tool_calls"` -> `"tool_use"`. Anything else (e.g.
/// `"content_filter"`) passes through verbatim rather than being discarded —
/// still informative, and `is_truncated`'s exact-match comparison only cares
/// about the `max_tokens` case. `None` stays `None` (a chunk/response that
/// never named one). `"error"` is NEVER passed to this function — both
/// callers intercept it before mapping (see `complete()`/`stream_complete`).
fn map_finish_reason(reason: Option<&str>) -> Option<String> {
    match reason {
        None => None,
        Some("length") => Some(crate::llm::STOP_REASON_MAX_TOKENS.to_string()),
        Some("stop") => Some("end_turn".to_string()),
        Some("tool_calls") => Some("tool_use".to_string()),
        Some(other) => Some(other.to_string()),
    }
}

impl OpenAiProvider {
    /// ONE attempt of [`LlmProvider::complete`]: send, then parse the 200
    /// body and apply the body-level checks that, before this fix, ran
    /// exactly once with no way back into the retry loop (measured
    /// 2026-09-07: `z-ai/glm-5.3-flash` failed 16% of calls this way over 7
    /// days — every failure an HTTP 200 whose body carried
    /// `choices[0].finish_reason == "error"` with no top-level `error`
    /// object, which [`LlmError::is_transient`] already classified as
    /// retryable but nothing retried). Restructuring "one attempt" to mean
    /// "send + parse + body-check" (rather than "send" alone) is the entire
    /// fix — [`LlmProvider::complete`]'s loop below is otherwise unchanged
    /// from before this fix.
    ///
    /// The third element of the old `Err` tuple, now
    /// [`AttemptError::upstream`], is [`OpenAiResponse::provider`] — the
    /// upstream host this attempt's body named, when it got far enough to
    /// parse one (`None` for a raw network failure, which never produces a
    /// body at all). The caller reports it via
    /// [`crate::llm::report_upstream_provider`] so it survives into the warn
    /// log and, on the FINAL exhausted attempt, into
    /// `llm_usage.upstream_provider` — see that function's own doc.
    ///
    /// COR-21: the attempt acquires its own permit from `gate` (a deny is
    /// returned as [`LlmError::Managed`], which the loop never retries) and
    /// settles it before returning, whatever the result. Every bound in
    /// `bound` ends the attempt here, never by dropping this future, so the
    /// attempt is always settled by its own transport.
    async fn complete_once(
        &self,
        gate: &dyn SendGate,
        attempt_no: u32,
        body: &serde_json::Value,
        forced_tool: bool,
        bound: &AttemptBound,
    ) -> Result<CompletionResponse, AttemptError> {
        let attempt_end = bound.attempt_end();
        if attempt_end.is_some_and(|end| end <= tokio::time::Instant::now()) {
            super::report_completion_reason("deadline");
            return Err(AttemptError::final_(LlmError::Timeout, None));
        }
        let permit = send_gate::acquire(gate, attempt_no)
            .await
            .map_err(|deny| AttemptError::new(LlmError::Managed(deny), None, None))?;
        let now = tokio::time::Instant::now();
        if attempt_end.is_some_and(|end| end <= now) {
            // Admission used the whole budget. Nothing was written, so
            // nothing was generated.
            send_gate::settle(gate, permit.into_sent(), SendOutcome::NotGenerated).await;
            super::report_completion_reason("deadline");
            return Err(AttemptError::final_(LlmError::Timeout, None));
        }
        // The request's own deadline sits just past the attempt's, so the
        // attempt bound below fires first and names the reason.
        let request_timeout =
            attempt_end.map(|end| end.saturating_duration_since(now) + REQUEST_TIMEOUT_MARGIN);
        let (sent, result) = self
            .send_and_check(permit, &self.client, body, request_timeout)
            .await;
        let response = match result {
            Ok(response) => response,
            Err((err, retry_after, outcome)) => {
                // A send cut by a policy bound reached the provider (or may
                // have): charged in full, and the same request against the
                // same bound fails the same way.
                let cut = bound.bounded
                    && matches!(err, LlmError::Timeout)
                    && matches!(outcome, SendOutcome::Ambiguous { .. });
                send_gate::settle(gate, sent, outcome).await;
                if cut {
                    super::report_completion_reason("deadline");
                    return Err(AttemptError::final_(err, None));
                }
                return Err(AttemptError::new(err, retry_after, None));
            }
        };

        let requested = body["model"].as_str().unwrap_or_default();
        let read = match (bound.stream_stall, attempt_end) {
            (Some(stall), Some(end)) if is_event_stream(&response) => {
                read_event_stream(response, end, stall).await
            }
            _ => read_body(response, attempt_end).await,
        };
        let parsed = match read {
            Ok(parsed) => parsed,
            Err(failure) => {
                let outcome = self
                    .resolve_ambiguous(
                        bound,
                        SendOutcome::Ambiguous {
                            generation_id: failure.generation_id,
                        },
                    )
                    .await;
                send_gate::settle(gate, sent, outcome).await;
                if let Some(reason) = failure.reason {
                    super::report_completion_reason(reason);
                }
                return Err(AttemptError {
                    err: failure.err,
                    retry_after: None,
                    upstream: failure.upstream,
                    no_retry: failure.no_retry,
                });
            }
        };
        let outcome = self
            .resolve_ambiguous(bound, classify_attempt(Observed::Body(&parsed, requested)))
            .await;
        send_gate::settle(gate, sent, outcome).await;

        let upstream_provider = parsed.provider;

        // A 200 whose body is ONLY `{"error": {...}}` — see
        // `OpenAiErrorBody`'s doc. Checked BEFORE anything else on `parsed`
        // is read.
        if let Some(error) = parsed.error {
            return Err(AttemptError::new(openai_error_to_llm_error(error), None, upstream_provider));
        }

        let usage = parsed.usage.ok_or_else(|| {
            AttemptError::new(
                LlmError::Parse(
                    "OpenAI/OpenRouter response has no usage field and no error field"
                        .to_string(),
                ),
                None,
                upstream_provider.clone(),
            )
        })?;
        let model = parsed.model.ok_or_else(|| {
            AttemptError::new(
                LlmError::Parse(
                    "OpenAI/OpenRouter response has no model field and no error field"
                        .to_string(),
                ),
                None,
                upstream_provider.clone(),
            )
        })?;

        let mut choices = parsed.choices.into_iter();
        let first = choices.next();

        // `finish_reason == "error"` (mid-generation failure surfaced via
        // the choice rather than — or alongside — a top-level `error`
        // object): treat as a genuine failure, never a successful (partial)
        // `CompletionResponse` that a caller might persist as complete
        // (the #452 bug class). This IS the 16% shape.
        if first.as_ref().and_then(|c| c.finish_reason.as_deref()) == Some("error") {
            return Err(AttemptError::new(
                LlmError::Upstream {
                    status: 0,
                    body: "OpenAI/OpenRouter response finish_reason=\"error\" with no top-level \
                           error field"
                        .to_string(),
                },
                None,
                upstream_provider,
            ));
        }

        let stop_reason =
            map_finish_reason(first.as_ref().and_then(|c| c.finish_reason.as_deref()));
        let content = first
            .map(|c| extract_openai_content(c, forced_tool))
            .unwrap_or_default();

        let (cache_read_input_tokens, cache_creation_input_tokens) =
            cache_tokens_from_details(&usage.prompt_tokens_details);

        // Success: still report the upstream host — `llm_usage.
        // upstream_provider` wants it on a clean completion too (item 3:
        // "write it on every usage row, success and errored"), not just on
        // a failure.
        super::report_upstream_provider(upstream_provider.as_deref());

        Ok(CompletionResponse {
            content,
            input_tokens: usage.prompt_tokens,
            output_tokens: usage.completion_tokens,
            model,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            stop_reason,
            // See `OpenAiUsage::cost` — `None` for real OpenAI, populated
            // only when the upstream response actually reports it.
            cost_usd: usage.cost,
        })
    }

    /// An attempt that would settle ambiguous at its full reservation, but
    /// whose generation id is known, is settled at what the provider actually
    /// charged when OpenRouter's generation record says so. The record is
    /// written once the generation ends (a cut stream is cancelled upstream),
    /// so a record that carries a cost is final. The answer itself is not in
    /// the record and is never used: the caller still gets the error.
    ///
    /// Only for a gate whose policy bounds attempts (managed spend), and only
    /// on OpenRouter. Anything short of a priced record stays ambiguous.
    async fn resolve_ambiguous(&self, bound: &AttemptBound, outcome: SendOutcome) -> SendOutcome {
        let SendOutcome::Ambiguous {
            generation_id: Some(id),
        } = &outcome
        else {
            return outcome;
        };
        if !bound.bounded || !self.is_openrouter || generation_lookup_disabled() {
            return outcome;
        }
        let url = format!("{}/v1/generation", self.base_url);
        // The whole lookup is bounded, not only each request: a caller's
        // backstop (`managed::transport::BACKSTOP_GRACE`) must still leave
        // room for the settle after it, or the attempt is dropped unsettled.
        let lookup = async {
            for delay in GENERATION_LOOKUP_BACKOFF {
                tokio::time::sleep(delay).await;
                let response = self
                    .client
                    .get(&url)
                    .query(&[("id", id.as_str())])
                    .bearer_auth(&self.api_key)
                    .timeout(GENERATION_LOOKUP_TIMEOUT)
                    .send()
                    .await;
                let Ok(response) = response else { continue };
                if response.status() != reqwest::StatusCode::OK {
                    continue;
                }
                let Ok(record) = response.json::<GenerationRecord>().await else {
                    continue;
                };
                if let Some(billed) = record.data.billed(id) {
                    return Some(billed);
                }
            }
            None
        };
        if let Ok(Some(billed)) = tokio::time::timeout(GENERATION_LOOKUP_BUDGET, lookup).await {
            tracing::info!(
                generation_id = %id,
                "openrouter: a cut attempt settled at the provider's actual cost"
            );
            super::report_completion_reason("cut_settled_actual_cost");
            return billed;
        }
        tracing::warn!(
            generation_id = %id,
            "openrouter: no priced generation record for a cut attempt; settling ambiguous"
        );
        outcome
    }
}

/// Slack on a request's own timeout past its attempt's bound.
const REQUEST_TIMEOUT_MARGIN: Duration = Duration::from_secs(2);
/// A streamed attempt with no attempt timeout of its own is still bounded:
/// the stream client has no total deadline.
const DEFAULT_STREAM_ATTEMPT: Duration = Duration::from_secs(300);
/// Waits before each generation-record lookup (13.5 s in all, inside
/// [`GENERATION_LOOKUP_BUDGET`]).
#[cfg(not(test))]
const GENERATION_LOOKUP_BACKOFF: [Duration; 5] = [
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(6),
];
#[cfg(test)]
const GENERATION_LOOKUP_BACKOFF: [Duration; 3] = [Duration::from_millis(10); 3];
const GENERATION_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on the whole lookup, waits and requests together. Without it, five
/// slow requests ran to 15.5 s + 5 x 5 s = 40.5 s, past the 30 s backstop
/// Jev's binding and the enrichment judge keep for lookup AND settle.
#[cfg(not(test))]
const GENERATION_LOOKUP_BUDGET: Duration = Duration::from_secs(15);
#[cfg(test)]
const GENERATION_LOOKUP_BUDGET: Duration = Duration::from_millis(800);

fn generation_lookup_disabled() -> bool {
    std::env::var("OPENROUTER_GENERATION_LOOKUP_DISABLED")
        .is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

/// The bounds a gate's [`TransportPolicy`] puts on each attempt, resolved
/// once per logical call.
pub(crate) struct AttemptBound {
    attempt_timeout: Option<Duration>,
    deadline: Option<tokio::time::Instant>,
    stream_stall: Option<Duration>,
    /// The policy, not the client, bounds attempts.
    bounded: bool,
}

impl AttemptBound {
    pub(crate) fn from_policy(policy: &TransportPolicy) -> Self {
        let attempt_timeout = match (policy.attempt_timeout, policy.stream_stall) {
            (None, Some(_)) => Some(DEFAULT_STREAM_ATTEMPT),
            (timeout, _) => timeout,
        };
        Self {
            attempt_timeout,
            deadline: policy.deadline.map(tokio::time::Instant::from_std),
            stream_stall: policy.stream_stall,
            bounded: policy.bounds_attempts(),
        }
    }

    /// When an attempt started now must end.
    fn attempt_end(&self) -> Option<tokio::time::Instant> {
        let by_timeout = self.attempt_timeout.map(|t| tokio::time::Instant::now() + t);
        match (by_timeout, self.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

/// One failed attempt, as the retry loop sees it.
struct AttemptError {
    err: LlmError,
    retry_after: Option<Duration>,
    upstream: Option<String>,
    /// The attempt was sent and then cut (or its answer lost): charged in
    /// full, so the transport never repeats it.
    no_retry: bool,
}

impl AttemptError {
    fn new(err: LlmError, retry_after: Option<Duration>, upstream: Option<String>) -> Self {
        Self { err, retry_after, upstream, no_retry: false }
    }

    fn final_(err: LlmError, upstream: Option<String>) -> Self {
        Self { err, retry_after: None, upstream, no_retry: true }
    }
}

/// An attempt whose 200 answer could not be read to the end.
struct ReadFailure {
    err: LlmError,
    generation_id: Option<String>,
    upstream: Option<String>,
    /// `llm_usage.diagnostics.completion_reason` for this cut.
    reason: Option<&'static str>,
    no_retry: bool,
}

fn is_event_stream(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim_start().starts_with("text/event-stream"))
}

/// A blocking read of one JSON body. A body read past the deadline (the
/// policy's, or the client's own total timeout) is a timeout, not a parse
/// error: the provider accepted the request and was still answering.
async fn read_body(
    response: reqwest::Response,
    attempt_end: Option<tokio::time::Instant>,
) -> Result<OpenAiResponse, ReadFailure> {
    let cut = || ReadFailure {
        err: LlmError::Timeout,
        generation_id: None,
        upstream: None,
        reason: Some("deadline"),
        no_retry: true,
    };
    let bytes = match attempt_end {
        Some(end) => match tokio::time::timeout_at(end, response.bytes()).await {
            Ok(bytes) => bytes,
            Err(_) => return Err(cut()),
        },
        None => response.bytes().await,
    };
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(e) if e.is_timeout() => return Err(cut()),
        Err(e) => {
            return Err(ReadFailure {
                err: LlmError::Parse(e.to_string()),
                generation_id: None,
                upstream: None,
                reason: None,
                no_retry: false,
            })
        }
    };
    serde_json::from_slice::<OpenAiResponse>(&bytes).map_err(|e| ReadFailure {
        err: LlmError::Parse(e.to_string()),
        generation_id: None,
        upstream: None,
        reason: None,
        no_retry: false,
    })
}

/// Reads a streamed completion into the same [`OpenAiResponse`] a blocking
/// read decodes. The stream is cut when no event arrives for `stall`, or at
/// `attempt_end`, whichever is first. Comment lines (OpenRouter's
/// `: OPENROUTER PROCESSING` keep-alive) are not progress: a provider that
/// never produces a token still stalls.
async fn read_event_stream(
    response: reqwest::Response,
    attempt_end: tokio::time::Instant,
    stall: Duration,
) -> Result<OpenAiResponse, ReadFailure> {
    let mut acc = SseAccumulator::default();
    let mut bytes = response.bytes_stream();
    let mut last_event = tokio::time::Instant::now();
    loop {
        let stall_end = last_event + stall;
        let until = stall_end.min(attempt_end);
        let next = match tokio::time::timeout_at(until, bytes.next()).await {
            Ok(next) => next,
            Err(_) => {
                let reason = if attempt_end <= stall_end { "deadline" } else { "stall" };
                return Err(acc.failure(LlmError::Timeout, Some(reason)));
            }
        };
        match next {
            None => break,
            Some(Err(e)) if e.is_timeout() => {
                return Err(acc.failure(LlmError::Timeout, Some("deadline")))
            }
            Some(Err(e)) => {
                return Err(acc.failure(LlmError::Upstream { status: 0, body: e.to_string() }, None))
            }
            Some(Ok(chunk)) => {
                if acc.feed(&chunk)? {
                    last_event = tokio::time::Instant::now();
                }
                if acc.done {
                    break;
                }
            }
        }
    }
    Ok(acc.finish())
}

/// The running state of one streamed completion.
#[derive(Default)]
struct SseAccumulator {
    buf: Vec<u8>,
    id: Option<String>,
    model: Option<String>,
    provider: Option<String>,
    content: String,
    /// Forced-tool argument fragments, by tool-call index.
    tool_args: std::collections::BTreeMap<u32, String>,
    finish_reason: Option<String>,
    usage: Option<OpenAiUsage>,
    error: Option<OpenAiErrorBody>,
    done: bool,
}

impl SseAccumulator {
    /// A mid-stream failure. Whatever arrived is discarded; only the
    /// generation id and host survive, for settlement and attribution.
    fn failure(&self, err: LlmError, reason: Option<&'static str>) -> ReadFailure {
        ReadFailure {
            err,
            generation_id: self.id.clone(),
            upstream: self.provider.clone(),
            reason,
            no_retry: true,
        }
    }

    /// Feeds raw bytes; `true` when at least one event arrived.
    fn feed(&mut self, chunk: &[u8]) -> Result<bool, ReadFailure> {
        self.buf.extend_from_slice(chunk);
        let mut progressed = false;
        while let Some(pos) = self.buf.iter().position(|b| *b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=pos).collect();
            let line = match std::str::from_utf8(&raw) {
                Ok(line) => line.trim_end_matches(['\n', '\r']),
                Err(e) => return Err(self.failure(LlmError::Parse(format!("SSE utf-8: {e}")), None)),
            };
            // Blank separators, `: comment` keep-alives, `event:`/`id:` fields.
            let Some(data) = line.strip_prefix("data:") else { continue };
            let data = data.trim_start();
            progressed = true;
            if data == "[DONE]" {
                self.done = true;
                return Ok(true);
            }
            let chunk: SseChunk = match serde_json::from_str(data) {
                Ok(chunk) => chunk,
                Err(e) => return Err(self.failure(LlmError::Parse(format!("SSE parse: {e}")), None)),
            };
            self.absorb(chunk);
            if self.error.is_some() {
                // OpenRouter ends a failed stream after its error event.
                self.done = true;
                return Ok(true);
            }
        }
        Ok(progressed)
    }

    fn absorb(&mut self, chunk: SseChunk) {
        if self.id.is_none() {
            self.id = chunk.id;
        }
        if chunk.model.is_some() {
            self.model = chunk.model;
        }
        if chunk.provider.is_some() {
            self.provider = chunk.provider;
        }
        if chunk.error.is_some() {
            self.error = chunk.error;
        }
        if let Some(usage) = chunk.usage {
            if let (Some(prompt_tokens), Some(completion_tokens)) =
                (usage.prompt_tokens, usage.completion_tokens)
            {
                self.usage = Some(OpenAiUsage {
                    prompt_tokens,
                    completion_tokens,
                    prompt_tokens_details: usage.prompt_tokens_details,
                    cost: usage.cost,
                });
            }
        }
        for choice in chunk.choices {
            if choice.finish_reason.is_some() {
                self.finish_reason = choice.finish_reason;
            }
            let Some(delta) = choice.delta else { continue };
            if let Some(content) = delta.content {
                self.content.push_str(&content);
            }
            for call in delta.tool_calls.unwrap_or_default() {
                let args = self.tool_args.entry(call.index.unwrap_or(0)).or_default();
                if let Some(fragment) = call.function.and_then(|f| f.arguments) {
                    args.push_str(&fragment);
                }
            }
        }
    }

    /// The stream as one decoded body. An empty stream yields a body with no
    /// usage, which the caller treats exactly like such a blocking body.
    fn finish(self) -> OpenAiResponse {
        let tool_calls = self.tool_args.into_values().next().map(|arguments| {
            vec![OpenAiToolCall {
                function: OpenAiFunctionCall { arguments },
            }]
        });
        let has_choice =
            tool_calls.is_some() || !self.content.is_empty() || self.finish_reason.is_some();
        OpenAiResponse {
            choices: if has_choice {
                vec![OpenAiChoice {
                    message: OpenAiMessage {
                        content: (!self.content.is_empty()).then_some(self.content),
                        tool_calls,
                    },
                    finish_reason: self.finish_reason,
                }]
            } else {
                Vec::new()
            },
            usage: self.usage,
            model: self.model,
            error: self.error,
            provider: self.provider,
            id: self.id,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SseChunk {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    error: Option<OpenAiErrorBody>,
    #[serde(default, deserialize_with = "null_as_empty")]
    choices: Vec<SseChoice>,
    /// Lenient: a chunk's usage only counts once it carries both counts.
    #[serde(default)]
    usage: Option<StreamUsage>,
}

fn null_as_empty<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Deserialize)]
struct SseChoice {
    #[serde(default)]
    delta: Option<SseDelta>,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SseDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<SseToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct SseToolCallDelta {
    #[serde(default)]
    index: Option<u32>,
    #[serde(default)]
    function: Option<SseFunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct SseFunctionDelta {
    #[serde(default)]
    arguments: Option<String>,
}

/// OpenRouter's `GET /v1/generation?id=` record: what one generation cost.
#[derive(Debug, Deserialize)]
struct GenerationRecord {
    data: GenerationData,
}

#[derive(Debug, Deserialize)]
struct GenerationData {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    provider_name: Option<String>,
    #[serde(default)]
    total_cost: Option<f64>,
    #[serde(default)]
    tokens_prompt: Option<u64>,
    #[serde(default)]
    tokens_completion: Option<u64>,
    #[serde(default)]
    native_tokens_prompt: Option<u64>,
    #[serde(default)]
    native_tokens_completion: Option<u64>,
}

impl GenerationData {
    /// A record with a finite, positive cost is the provider's bill. A cut
    /// attempt was sent and its prompt read (it named its generation), so a
    /// zero cost is a record not yet priced, never a free attempt: it stays
    /// ambiguous rather than settling a charged generation at $0.
    fn billed(self, id: &str) -> Option<SendOutcome> {
        let cost = self.total_cost.filter(|c| c.is_finite() && *c > 0.0)?;
        Some(SendOutcome::Billed {
            reported_cost_usd: Some(cost),
            input_tokens: self.native_tokens_prompt.or(self.tokens_prompt).unwrap_or(0),
            output_tokens: self.native_tokens_completion.or(self.tokens_completion).unwrap_or(0),
            served_model: self.model?,
            upstream: self.provider_name,
            generation_id: Some(id.to_string()),
        })
    }
}

// ── LlmProvider impl ──────────────────────────────────────────────────────────

impl super::private::Sealed for OpenAiProvider {}

#[async_trait::async_trait]
impl GatedLlm for OpenAiProvider {
    async fn complete_gated(
        &self,
        req: CompletionRequest,
        gate: &dyn SendGate,
    ) -> Result<CompletionResponse, LlmError> {
        let forced_tool = req.tool_schema.is_some();
        let policy = gate.transport();
        let bound = AttemptBound::from_policy(&policy);
        let mut body = self.build_request_body(&req, bound.stream_stall.is_some(), gate.terms());
        if self.is_openrouter {
            if let Some(sort) = policy.provider_sort.or_else(super::provider_sort_hint) {
                // An ordering only: `zdr`, `data_collection`, the
                // quantization floor and `max_price` still bound the pool.
                body["provider"]["sort"] = json!(sort);
            }
        }

        // Per-call retry (audit S2): exponential-backoff-with-jitter +
        // Retry-After. Without this, a single 429/5xx from an
        // allowlisted backend is a user-facing synthesis failure.
        //
        // OpenRouter retry fix (2026-09-07): ONE ATTEMPT is now
        // [`Self::complete_once`] — send + parse + the body-level
        // `finish_reason == "error"` / error-only-200 check — not just the
        // network `send`, so a mid-generation upstream failure goes back
        // through this SAME loop instead of returning immediately. On
        // exhaustion (or a non-transient error) this returns EXACTLY the
        // error `complete_once` produced.
        //
        // An attempt that was sent and then cut (or whose answer was lost)
        // is never repeated here: it is charged in full, and the caller
        // owns any retry (extraction retries once on a smaller window). No
        // retry starts past the policy's deadline.
        let retry = RetryPolicy::from_env();
        let mut attempt: u32 = 0;
        loop {
            super::report_attempt(attempt + 1);
            match self.complete_once(gate, attempt + 1, &body, forced_tool, &bound).await {
                Ok(resp) => return Ok(resp),
                Err(failed) => {
                    let AttemptError { err, retry_after, upstream, no_retry } = failed;
                    super::report_upstream_provider(upstream.as_deref());
                    let last = attempt + 1 >= retry.max_attempts;
                    if last || no_retry || !err.is_transient() {
                        return Err(err);
                    }
                    let delay = retry.backoff(attempt, retry_after);
                    if bound
                        .deadline
                        .is_some_and(|deadline| tokio::time::Instant::now() + delay >= deadline)
                    {
                        return Err(err);
                    }
                    let (error_class, status) = super::error_metadata(&err);
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = retry.max_attempts,
                        delay_ms = delay.as_millis() as u64,
                        model = %req.model,
                        upstream_provider = upstream.as_deref().unwrap_or("unknown"),
                        error_class,
                        status,
                        "openai/openrouter complete: transient failure — backing off and retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl LlmProvider for OpenAiProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.complete_gated(req, &Unmanaged).await
    }

    fn gated(&self) -> Option<&dyn GatedLlm> {
        Some(self)
    }

    async fn stream_complete(
        &self,
        req: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        let body = self.build_request_body(&req, true, None);
        let model = req.model.clone();

        // Owned copies for the generator below — `post_and_check` (not
        // `self.send_and_check`) is what the generator calls to re-open a
        // fresh connection on a mid-stream retry, and it cannot borrow
        // `&self` — see that function's own doc.
        let client = self.stream_client.clone();
        let url = self.completions_url();
        let api_key = self.api_key.clone();
        let headers = self.attribution_headers();

        // Stream-OPEN retry: a failure before even a 200 status is
        // seen. `attempt` is NOT reset once the generator below starts — a
        // mid-stream retry (see that generator's own doc) shares this same
        // counter and the same `policy.max_attempts` ceiling, so the TOTAL
        // number of connection attempts for one logical call is bounded
        // exactly like the non-streaming path's is.
        let policy = RetryPolicy::from_env();
        let mut attempt: u32 = 0;
        let (response, open_sent) = loop {
            let permit = send_gate::acquire(&Unmanaged, attempt + 1)
                .await
                .map_err(LlmError::Managed)?;
            let (sent, result) =
                post_and_check(permit, &client, &url, &api_key, headers.clone(), &body, None).await;
            match result {
                Ok(resp) => break (resp, sent),
                Err((err, retry_after, outcome)) => {
                    send_gate::settle(&Unmanaged, sent, outcome).await;
                    let last = attempt + 1 >= policy.max_attempts;
                    if last || !err.is_transient() {
                        return Err(err);
                    }
                    let delay = policy.backoff(attempt, retry_after);
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = policy.max_attempts,
                        delay_ms = delay.as_millis() as u64,
                        model = %model,
                        "openai/openrouter stream_complete: transient failure opening stream — \
                         backing off and retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        };

        macro_rules! settle_open {
            ($open:ident, $outcome:expr) => {
                if let Some(sent) = $open.take() {
                    send_gate::settle(&Unmanaged, sent, $outcome).await;
                }
            };
        }

        let stream = async_stream::stream! {
            let mut response = response;
            let mut open: Option<Sent> = Some(open_sent);
            let mut byte_stream: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, reqwest::Error>> + Send>> =
                Box::pin(response.bytes_stream());
            let mut buf = String::new();
            // Served model, tracked across chunks (audit: PR3, served-model
            // capture) — every `chat.completion.chunk` carries it, so the
            // LAST chunk seen wins (a provider that changed model mid-stream
            // would be a real anomaly; this simply reflects whatever the
            // terminal chunk reports, same as the token counts below).
            let mut served_model: Option<String> = None;
            // The upstream host OpenRouter actually routed to — see
            // `OpenAiResponse::provider`'s doc; same "last chunk wins"
            // tracking as `served_model`. Reported into `llm_usage.
            // upstream_provider` on every terminal outcome below (Final,
            // and an exhausted/non-retried Err).
            let mut served_provider: Option<String> = None;
            // `finish_reason`, tracked the same way — it typically arrives on
            // the chunk immediately BEFORE the terminal usage-only chunk
            // (whose own `choices` array is empty), so this must be captured
            // running rather than read off the same chunk the Final event is
            // yielded from. Updated from `choices[]` BEFORE `usage` is
            // processed on any given chunk (see the ordering below) so a
            // chunk that happens to carry BOTH never reads a stale value.
            let mut finish_reason: Option<String> = None;
            // OpenRouter retry fix (2026-09-07), streaming clause: `true`
            // the instant the FIRST `StreamEvent::Delta` is yielded to the
            // consumer, across every reopened attempt for this logical
            // call — never reset. This is the retry gate: a mid-stream
            // failure that arrives before any content ever reached the
            // consumer is retried (a fresh connection, same `RetryPolicy`,
            // same shared `attempt` counter as the STREAM-OPEN loop above);
            // one that arrives AFTER is surfaced exactly as before this fix
            // — retrying post-content would either duplicate a token the
            // consumer already has or silently discard one, and this
            // codebase has no way to tell the consumer "rewind".
            let mut any_content_yielded = false;

            'consume: loop {
                while let Some(chunk_result) = byte_stream.next().await {
                    let chunk: Bytes = match chunk_result {
                        Ok(b) => b,
                        Err(e) => {
                            settle_open!(open, SendOutcome::Ambiguous { generation_id: None });
                            yield Err(LlmError::Upstream { status: 0, body: e.to_string() });
                            return;
                        }
                    };

                    let text = match std::str::from_utf8(&chunk) {
                        Ok(s) => s,
                        Err(e) => {
                            settle_open!(open, SendOutcome::Ambiguous { generation_id: None });
                            yield Err(LlmError::Parse(e.to_string()));
                            return;
                        }
                    };

                    buf.push_str(text);

                    loop {
                        match buf.find('\n') {
                            None => break,
                            Some(pos) => {
                                let line = buf[..pos].trim_end_matches('\r').to_owned();
                                buf.drain(..=pos);

                                if line.is_empty() {
                                    continue;
                                }

                                if let Some(data) = line.strip_prefix("data: ") {
                                    if data.trim() == "[DONE]" {
                                        settle_open!(open, SendOutcome::Ambiguous { generation_id: None });
                                        return;
                                    }

                                    let parsed_chunk: StreamChunk = match serde_json::from_str(data) {
                                        Ok(c) => c,
                                        Err(e) => {
                                            settle_open!(open, SendOutcome::Ambiguous { generation_id: None });
                                            yield Err(LlmError::Parse(format!("SSE parse: {e}")));
                                            return;
                                        }
                                    };

                                    if parsed_chunk.provider.is_some() {
                                        served_provider = parsed_chunk.provider.clone();
                                    }

                                    // A mid-stream failure chunk (audit:
                                    // cutover-blocking — the HTTP status stays
                                    // 200 once streaming has started; this is
                                    // the ONLY signal). No `[DONE]` follows a
                                    // chunk like this on the real wire, but this
                                    // doesn't rely on that.
                                    if let Some(error) = parsed_chunk.error {
                                        let mapped = openai_error_to_llm_error(error);
                                        super::report_upstream_provider(served_provider.as_deref());
                                        settle_open!(open, SendOutcome::Ambiguous { generation_id: None });
                                        if !any_content_yielded
                                            && attempt + 1 < policy.max_attempts
                                            && mapped.is_transient()
                                        {
                                            let delay = policy.backoff(attempt, None);
                                            tracing::warn!(
                                                attempt = attempt + 1,
                                                max_attempts = policy.max_attempts,
                                                delay_ms = delay.as_millis() as u64,
                                                model = %model,
                                                upstream_provider =
                                                    served_provider.as_deref().unwrap_or("unknown"),
                                                error = %mapped,
                                                "openai/openrouter stream_complete: transient \
                                                 failure before any token was yielded — backing \
                                                 off and retrying"
                                            );
                                            tokio::time::sleep(delay).await;
                                            attempt += 1;
                                            let permit = match send_gate::acquire(&Unmanaged, attempt + 1).await {
                                                Ok(permit) => permit,
                                                Err(deny) => {
                                                    yield Err(LlmError::Managed(deny));
                                                    return;
                                                }
                                            };
                                            let (sent, reopened) =
                                                post_and_check(permit, &client, &url, &api_key, headers.clone(), &body, None).await;
                                            match reopened {
                                                Ok(new_resp) => {
                                                    open = Some(sent);
                                                    response = new_resp;
                                                    byte_stream = Box::pin(response.bytes_stream());
                                                    buf.clear();
                                                    served_model = None;
                                                    finish_reason = None;
                                                    continue 'consume;
                                                }
                                                Err((open_err, _retry_after, outcome)) => {
                                                    send_gate::settle(&Unmanaged, sent, outcome).await;
                                                    yield Err(open_err);
                                                    return;
                                                }
                                            }
                                        } else {
                                            yield Err(mapped);
                                            return;
                                        }
                                    }

                                    if parsed_chunk.model.is_some() {
                                        served_model = parsed_chunk.model.clone();
                                    }

                                    // Choices — including `finish_reason` — are
                                    // handled BEFORE `usage` on this SAME chunk
                                    // (a chunk that carries both must not yield
                                    // a Final event reading a stale
                                    // `finish_reason` from an earlier chunk).
                                    let mut stream_errored = false;
                                    if let Some(choices) = parsed_chunk.choices {
                                        for choice in choices {
                                            if choice.finish_reason.as_deref() == Some("error") {
                                                // A terminal failure surfaced via
                                                // the choice rather than (or
                                                // alongside) a top-level `error`
                                                // object — still a failure, never
                                                // a successful partial answer.
                                                stream_errored = true;
                                                continue;
                                            }
                                            if let Some(fr) = choice.finish_reason.as_deref() {
                                                finish_reason = map_finish_reason(Some(fr));
                                            }
                                            if let Some(delta) = choice.delta {
                                                if let Some(content) = delta.content {
                                                    if !content.is_empty() {
                                                        any_content_yielded = true;
                                                        yield Ok(StreamEvent::Delta(StreamDelta {
                                                            text: content,
                                                        }));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if stream_errored {
                                        super::report_upstream_provider(served_provider.as_deref());
                                        settle_open!(open, SendOutcome::Ambiguous { generation_id: None });
                                        let mapped = LlmError::Upstream {
                                            status: 0,
                                            body: "OpenRouter stream finish_reason=\"error\" with \
                                                   no top-level error field"
                                                .to_string(),
                                        };
                                        if !any_content_yielded && attempt + 1 < policy.max_attempts {
                                            let delay = policy.backoff(attempt, None);
                                            tracing::warn!(
                                                attempt = attempt + 1,
                                                max_attempts = policy.max_attempts,
                                                delay_ms = delay.as_millis() as u64,
                                                model = %model,
                                                upstream_provider =
                                                    served_provider.as_deref().unwrap_or("unknown"),
                                                error = %mapped,
                                                "openai/openrouter stream_complete: \
                                                 finish_reason=\"error\" before any token was \
                                                 yielded — backing off and retrying"
                                            );
                                            tokio::time::sleep(delay).await;
                                            attempt += 1;
                                            let permit = match send_gate::acquire(&Unmanaged, attempt + 1).await {
                                                Ok(permit) => permit,
                                                Err(deny) => {
                                                    yield Err(LlmError::Managed(deny));
                                                    return;
                                                }
                                            };
                                            let (sent, reopened) =
                                                post_and_check(permit, &client, &url, &api_key, headers.clone(), &body, None).await;
                                            match reopened {
                                                Ok(new_resp) => {
                                                    open = Some(sent);
                                                    response = new_resp;
                                                    byte_stream = Box::pin(response.bytes_stream());
                                                    buf.clear();
                                                    served_model = None;
                                                    finish_reason = None;
                                                    continue 'consume;
                                                }
                                                Err((open_err, _retry_after, outcome)) => {
                                                    send_gate::settle(&Unmanaged, sent, outcome).await;
                                                    yield Err(open_err);
                                                    return;
                                                }
                                            }
                                        } else {
                                            yield Err(mapped);
                                            return;
                                        }
                                    }

                                    // Usage chunk (last chunk when include_usage=true).
                                    if let Some(usage) = parsed_chunk.usage {
                                        let input_tokens = usage.prompt_tokens.unwrap_or(0);
                                        let output_tokens = usage.completion_tokens.unwrap_or(0);
                                        let (cache_read_input_tokens, cache_creation_input_tokens) =
                                            cache_tokens_from_details(&usage.prompt_tokens_details);
                                        let cost_usd = usage.cost;
                                        // Only yield final event if we have actual token counts.
                                        if input_tokens > 0 || output_tokens > 0 {
                                            super::report_upstream_provider(served_provider.as_deref());
                                            settle_open!(open, SendOutcome::Billed {
                                                reported_cost_usd: cost_usd,
                                                input_tokens: u64::from(input_tokens),
                                                output_tokens: u64::from(output_tokens),
                                                served_model: served_model.clone().unwrap_or_else(|| model.clone()),
                                                upstream: served_provider.clone(),
                                                generation_id: None,
                                            });
                                            yield Ok(StreamEvent::Final(StreamFinalEvent {
                                                input_tokens,
                                                output_tokens,
                                                cache_creation_input_tokens,
                                                cache_read_input_tokens,
                                                stop_reason: finish_reason.clone(),
                                                model: served_model.clone(),
                                                cost_usd,
                                            }));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                // `byte_stream` ended (no more chunks) without `[DONE]`, an
                // error chunk, or a terminal usage chunk — unchanged from
                // before this fix: nothing more to do, end the generator.
                settle_open!(open, SendOutcome::Ambiguous { generation_id: None });
                return;
            }
        };

        Ok(Box::pin(stream))
    }

    fn name(&self) -> &'static str {
        self.provider_name
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::{Message, ToolSchema};

    fn req(model: &str, tool_schema: Option<ToolSchema>, reasoning: ReasoningSetting) -> CompletionRequest {
        CompletionRequest {
            messages: vec![Message { role: Role::User, content: "hello".into() }],
            model: model.to_string(),
            max_tokens: 256,
            system: Some("you are a helpful assistant".into()),
            tool_schema,
            effort: None,
            reasoning,
        }
    }

    fn tool_schema() -> ToolSchema {
        ToolSchema {
            strict: false,
            name: "emit_entities".into(),
            schema: serde_json::json!({
                "type": "object",
                "properties": { "entities": { "type": "array" } },
                "required": ["entities"],
            }),
        }
    }

    /// Bind a local mock server and return its address. Shared by every
    /// end-to-end test below.
    async fn spawn_mock(app: axum::Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local mock server");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        addr
    }

    #[test]
    fn openai_provider_default_base_url() {
        let client = reqwest::Client::new();
        let provider = OpenAiProvider::new(client, "test-key");
        assert_eq!(provider.base_url, "https://api.openai.com");
        assert_eq!(provider.name(), "openai");
        assert!(!provider.is_openrouter);
    }

    #[test]
    fn openai_provider_with_base_url() {
        let client = reqwest::Client::new();
        let provider =
            OpenAiProvider::new(client, "test-key").with_base_url("http://localhost:11434");
        assert_eq!(provider.base_url, "http://localhost:11434");
    }

    #[test]
    fn openrouter_constructor_sets_base_url_name_and_flag() {
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "test-key");
        assert_eq!(provider.base_url, "https://openrouter.ai/api");
        assert_eq!(provider.name(), "openrouter");
        assert!(provider.is_openrouter);
    }

    /// The 2026-09-04 production outage, as an assertion.
    ///
    /// Asserting the `base_url` FIELD (the test above) is what let the bug
    /// through: the field was "correct-looking" and wrong. Only the composed
    /// URL is the thing the network sees, so pin that.
    #[test]
    fn openrouter_completions_url_has_exactly_one_v1_segment() {
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "test-key");
        assert_eq!(
            provider.completions_url(),
            "https://openrouter.ai/api/v1/chat/completions",
            "a doubled /v1 hits OpenRouter's HTML 404 page, not its API"
        );
        assert!(!provider.completions_url().contains("/v1/v1"));
    }

    #[test]
    fn openai_completions_url_has_exactly_one_v1_segment() {
        let provider = OpenAiProvider::new(reqwest::Client::new(), "test-key");
        assert_eq!(
            provider.completions_url(),
            "https://api.openai.com/v1/chat/completions"
        );
    }

    /// `OPENROUTER_BASE_URL` is the operator-facing trap: OpenRouter's own
    /// docs name `https://openrouter.ai/api/v1`, so that is what gets pasted.
    /// Every spelling must land on the same endpoint.
    #[test]
    fn base_url_override_absorbs_every_spelling_operators_paste() {
        for candidate in [
            "https://openrouter.ai/api",
            "https://openrouter.ai/api/",
            "https://openrouter.ai/api/v1",
            "https://openrouter.ai/api/v1/",
            "  https://openrouter.ai/api/v1\n",
        ] {
            let provider =
                OpenAiProvider::openrouter(reqwest::Client::new(), "test-key").with_base_url(candidate);
            assert_eq!(
                provider.completions_url(),
                "https://openrouter.ai/api/v1/chat/completions",
                "base url {candidate:?} must resolve to the one real endpoint"
            );
        }
    }

    /// Proves the composed URL is the path that actually reaches the wire —
    /// i.e. that [`OpenAiProvider::completions_url`] is the only path builder
    /// and `send_and_check` uses it. The mock answers on a FALLBACK so a
    /// wrong path is recorded and reported, instead of 404-ing into an
    /// unrelated deserialization failure.
    #[tokio::test]
    async fn the_composed_url_is_the_path_that_reaches_the_wire() {
        use std::sync::{Arc, Mutex};

        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
            let sink = Arc::clone(&sink);
            async move {
                sink.lock().unwrap().push(uri.path().to_string());
                axum::Json(serde_json::json!({
                    "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
                    "usage": { "prompt_tokens": 1, "completion_tokens": 1 },
                    "model": "deepseek/deepseek-v4-flash-0731"
                }))
            }
        });
        let addr = spawn_mock(app).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "test-key")
            .with_base_url(format!("http://{addr}/api"));

        provider
            .complete(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off))
            .await
            .expect("the mock returns a well-formed completion");

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["/api/v1/chat/completions"],
            "the request path must carry exactly one /v1 segment"
        );
    }

    /// A trailing newline in the key is not a legal `Authorization` header
    /// byte: reqwest poisons the request builder and `.send()` fails without
    /// anything leaving the process — indistinguishable from a network
    /// outage, and it leaves the provider's usage counter untouched exactly
    /// like the doubled-`/v1` bug did.
    #[test]
    fn api_key_is_trimmed_so_a_piped_fly_secret_cannot_poison_the_auth_header() {
        for raw in ["sk-or-v1-abc\n", "  sk-or-v1-abc  ", "sk-or-v1-abc\r\n"] {
            assert_eq!(
                OpenAiProvider::openrouter(reqwest::Client::new(), raw).api_key,
                "sk-or-v1-abc"
            );
            assert_eq!(
                OpenAiProvider::new(reqwest::Client::new(), raw).api_key,
                "sk-or-v1-abc"
            );
        }
    }

    #[test]
    fn truncate_for_log_bounds_an_oversized_body() {
        let short = "{\"error\":\"nope\"}";
        assert_eq!(truncate_for_log(short), short, "a small body is logged whole");

        // OpenRouter's HTML 404 page was ~125 KB; that is what silenced the logs.
        let html = "<!DOCTYPE html>".to_string() + &"x".repeat(125_000);
        let out = truncate_for_log(&html);
        assert!(out.len() < LOG_BODY_MAX_BYTES + 64, "bounded, got {} bytes", out.len());
        assert!(out.starts_with("<!DOCTYPE html>"), "the diagnostic prefix survives");
        assert!(out.contains("more bytes truncated"));
    }

    #[test]
    fn truncate_for_log_never_splits_a_utf8_char() {
        // Multi-byte chars straddling the cut must not panic.
        let body = "é".repeat(LOG_BODY_MAX_BYTES);
        let out = truncate_for_log(&body);
        assert!(out.starts_with('é'));
        assert!(out.contains("more bytes truncated"));
    }

    #[test]
    fn normalize_base_url_leaves_a_bare_origin_alone() {
        assert_eq!(normalize_base_url("https://api.openai.com"), "https://api.openai.com");
        assert_eq!(normalize_base_url("http://localhost:11434"), "http://localhost:11434");
        // Only a TRAILING /v1 is a duplicate; a host named v1 is not.
        assert_eq!(normalize_base_url("https://v1.example.com"), "https://v1.example.com");
    }

    // ── Reasoning serialization ──────────────────────────────────────────────

    #[test]
    fn reasoning_json_off_disables_reasoning() {
        assert_eq!(reasoning_json("gpt-4o-mini", ReasoningSetting::Off), json!({ "enabled": false }));
    }

    #[test]
    fn reasoning_json_maps_effort_levels() {
        assert_eq!(reasoning_json("gpt-4o-mini", ReasoningSetting::Low), json!({ "effort": "low" }));
        assert_eq!(reasoning_json("gpt-4o-mini", ReasoningSetting::Medium), json!({ "effort": "medium" }));
        assert_eq!(reasoning_json("gpt-4o-mini", ReasoningSetting::High), json!({ "effort": "high" }));
    }

    #[test]
    fn build_request_body_always_carries_reasoning() {
        let provider = OpenAiProvider::new(reqwest::Client::new(), "k");
        for setting in [
            ReasoningSetting::Off,
            ReasoningSetting::Low,
            ReasoningSetting::Medium,
            ReasoningSetting::High,
        ] {
            let body = provider.build_request_body(&req("gpt-4o-mini", None, setting), false, None);
            assert_eq!(body["reasoning"], reasoning_json("gpt-4o-mini", setting));
        }
    }

    // ── GLM reasoning floor: `Off` cannot be `{"enabled": false}` ──────────────
    //
    // Live smoke-test finding: OpenRouter answers `{"enabled": false}` for
    // `z-ai/glm-5.3-flash` with HTTP 400 "Reasoning is mandatory for this
    // endpoint and cannot be disabled." — see `MANDATORY_REASONING_MODELS`'s
    // doc comment for the measured token-count evidence behind picking
    // `{"effort": "low"}` as the replacement encoding.

    #[test]
    fn reasoning_json_off_sends_effort_low_floor_for_glm() {
        assert_eq!(
            reasoning_json("z-ai/glm-5.3-flash", ReasoningSetting::Off),
            json!({ "effort": "low" }),
            "GLM 400s on {{\"enabled\": false}} — Off must be sent as effort low instead"
        );
    }

    #[test]
    fn reasoning_json_off_still_disables_reasoning_for_deepseek() {
        assert_eq!(
            reasoning_json("deepseek/deepseek-v4-flash-0731", ReasoningSetting::Off),
            json!({ "enabled": false }),
            "DeepSeek accepts {{\"enabled\": false}} fine and must be unaffected by the GLM floor"
        );
    }

    #[test]
    fn reasoning_json_glm_low_medium_high_unaffected_by_the_off_floor() {
        for (setting, expected) in [
            (ReasoningSetting::Low, json!({ "effort": "low" })),
            (ReasoningSetting::Medium, json!({ "effort": "medium" })),
            (ReasoningSetting::High, json!({ "effort": "high" })),
        ] {
            assert_eq!(reasoning_json("z-ai/glm-5.3-flash", setting), expected);
        }
    }

    #[test]
    fn reasoning_json_off_applies_the_glm_floor_through_a_nitro_style_suffix() {
        // Matches billing::rates::PriceSheet::rate's `:nitro`-suffix
        // handling: a routing-variant suffix must still resolve to the same
        // underlying model, not fall through to the "no floor" default.
        assert_eq!(
            reasoning_json("z-ai/glm-5.3-flash:nitro", ReasoningSetting::Off),
            json!({ "effort": "low" })
        );
    }

    #[test]
    fn build_request_body_sends_effort_low_for_glm_off_and_omits_enabled() {
        let provider = OpenAiProvider::new(reqwest::Client::new(), "k");
        let body = provider
            .build_request_body(&req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off), false, None);
        assert_eq!(body["reasoning"], json!({ "effort": "low" }));
        assert!(
            body["reasoning"].get("enabled").is_none(),
            "the GLM floor must not also carry an `enabled` key"
        );
    }

    #[test]
    fn build_request_body_sends_enabled_false_for_deepseek_off_unchanged() {
        let provider = OpenAiProvider::new(reqwest::Client::new(), "k");
        let body = provider.build_request_body(
            &req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off),
            false,
            None,
        );
        assert_eq!(body["reasoning"], json!({ "enabled": false }));
    }

    // ── Forced tool use — request serialization (audit H3 parity) ──────────────

    #[test]
    fn build_request_body_has_no_tools_when_schema_none() {
        let provider = OpenAiProvider::new(reqwest::Client::new(), "k");
        let body =
            provider.build_request_body(&req("gpt-4o-mini", None, ReasoningSetting::Off), false, None);
        assert!(body.get("tools").is_none());
        assert!(body.get("tool_choice").is_none());
    }

    #[test]
    fn build_request_body_sends_forced_function_tool_and_tool_choice() {
        let provider = OpenAiProvider::new(reqwest::Client::new(), "k");
        let body = provider.build_request_body(
            &req("gpt-4o-mini", Some(tool_schema()), ReasoningSetting::Off),
            false,
            None,
        );

        let tools = body["tools"].as_array().expect("tools must be an array");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["function"]["name"], "emit_entities");
        assert_eq!(tools[0]["function"]["parameters"]["required"][0], "entities");
        assert!(tools[0]["function"].get("strict").is_none());
        let mut strict = tool_schema();
        strict.strict = true;
        let body = provider.build_request_body(&req("gpt-4o-mini", Some(strict), ReasoningSetting::Off), false, None);
        assert_eq!(body["tools"][0]["function"]["strict"], true);

        assert_eq!(body["tool_choice"]["type"], "function");
        assert_eq!(body["tool_choice"]["function"]["name"], "emit_entities");
    }

    // ── OpenRouter `provider` policy object (mandatory) ─────────────────────────

    #[test]
    fn build_request_body_omits_provider_routing_and_models_when_not_openrouter() {
        let provider = OpenAiProvider::new(reqwest::Client::new(), "k");
        let body = provider.build_request_body(
            &req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off),
            false,
            None,
        );
        assert!(
            body.get("provider").is_none(),
            "a plain (non-OpenRouter) OpenAiProvider must never send the OpenRouter-only \
             provider policy object"
        );
        assert!(
            body.get("models").is_none(),
            "a plain (non-OpenRouter) OpenAiProvider must never send the OpenRouter-only \
             models fallback array — deepseek/glm ids would be meaningless to real \
             api.openai.com anyway"
        );
    }

    /// **The regression this whole PR exists to prevent.** The 2026-09-05
    /// outage shipped because every prior version of this test asserted a
    /// hardcoded per-model `order` array instead of the composed policy
    /// object — this asserts the exact five keys the fix sends and, most
    /// importantly, that `order` is GONE. A test that re-adds `order` here
    /// must fail; see this PR's own negative-control run.
    #[test]
    fn build_request_body_sends_the_exact_policy_object_on_openrouter_with_no_order_key() {
        // `build_request_body` reads OPENROUTER_FALLBACK_MODEL_JSON on every
        // OpenRouter build, so this test is an env READER and must serialize
        // against the `set_var` calls in the fallback tests — an unsynchronized
        // getenv/setenv pair is a data race, not just a flaky assertion.
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k");
        let body = provider.build_request_body(
            &req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off),
            false,
            None,
        );
        assert_eq!(
            body["provider"],
            json!({
                "zdr": true,
                "data_collection": "deny",
                "quantizations": ["fp8", "bf16"],
                "allow_fallbacks": true,
                "require_parameters": true,
            }),
            "the provider object must be EXACTLY these five keys — no more, no less"
        );
        assert!(
            body["provider"].get("order").is_none(),
            "a per-model host allowlist must never reappear on the wire — this is the exact \
             2026-09-05 outage: DeepInfra/Fireworks/Cloudflare all dropped to a quantization \
             that failed the allowlisted `order`, 404ing every capable-tier call, even though \
             plenty of ZDR-compliant fp8/bf16 hosts remained routable"
        );
        assert_eq!(body["provider"]["allow_fallbacks"], true);
    }

    #[test]
    fn build_request_body_sends_the_same_policy_object_regardless_of_model() {
        // `build_request_body` reads OPENROUTER_FALLBACK_MODEL_JSON on every
        // OpenRouter build, so this test is an env READER and must serialize
        // against the `set_var` calls in the fallback tests — an unsynchronized
        // getenv/setenv pair is a data race, not just a flaky assertion.
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        // The policy object is model-agnostic on purpose — it is a privacy
        // policy, not a per-model pin — so GLM must get byte-identical
        // `provider` JSON to DeepSeek's.
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k");
        let body = provider
            .build_request_body(&req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off), false, None);
        assert_eq!(
            body["provider"],
            json!({
                "zdr": true,
                "data_collection": "deny",
                "quantizations": ["fp8", "bf16"],
                "allow_fallbacks": true,
                "require_parameters": true,
            })
        );
    }

    #[test]
    fn build_request_body_sends_the_policy_object_even_for_an_unrecognized_model() {
        // `build_request_body` reads OPENROUTER_FALLBACK_MODEL_JSON on every
        // OpenRouter build, so this test is an env READER and must serialize
        // against the `set_var` calls in the fallback tests — an unsynchronized
        // getenv/setenv pair is a data race, not just a flaky assertion.
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        // The old allowlist design FAILED CLOSED (`Err(ModelNotFound)`) for
        // any model it didn't have a pinned allowlist for. The policy object
        // has no such per-model gate — any model id gets the same
        // no-training policy, since routing itself (not this function) is
        // what decides whether the model id is valid.
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k");
        let body = provider
            .build_request_body(&req("some/unpinned-model", None, ReasoningSetting::Off), false, None);
        assert_eq!(body["provider"]["data_collection"], "deny");
        assert!(body["provider"].get("order").is_none());
    }

    // ── OpenRouter `models` fallback array ───────────────────────────────────────

    /// Serializes `OPENROUTER_FALLBACK_MODEL_JSON` env-var tests — they
    /// mutate a process-global env var, so they can't run concurrently with
    /// each other or with themselves. Mirrors `anthropic.rs`'s
    /// `CACHE_ENV_LOCK` / this file's own (now-removed) `ALLOWLIST_ENV_LOCK`.
    static FALLBACK_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn build_request_body_sets_glm_as_the_deepseek_fallback() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k");
        let body = provider.build_request_body(
            &req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off),
            false,
            None,
        );
        assert_eq!(body["models"], json!(["z-ai/glm-5.3-flash"]));
    }

    #[test]
    fn build_request_body_sets_deepseek_as_the_glm_fallback() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k");
        let body = provider
            .build_request_body(&req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off), false, None);
        assert_eq!(body["models"], json!(["deepseek/deepseek-v4-flash-0731"]));
    }

    #[test]
    fn build_request_body_omits_models_key_entirely_for_an_unmapped_model() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k");
        let body = provider
            .build_request_body(&req("some/unpinned-model", None, ReasoningSetting::Off), false, None);
        assert!(
            body.get("models").is_none(),
            "an unmapped model must get NO `models` key at all, never an empty array"
        );
    }

    /// A `:nitro`/`:floor` routing variant is the same underlying model, so it
    /// must keep its fallback. `reasoning_json` already treats it that way
    /// (`reasoning_json_off_applies_the_glm_floor_through_a_nitro_style_suffix`);
    /// an exact-match lookup here would have silently disagreed.
    #[test]
    fn resolve_fallback_model_survives_a_nitro_style_routing_suffix() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");

        assert_eq!(
            resolve_fallback_model("z-ai/glm-5.3-flash:nitro").as_deref(),
            Some("deepseek/deepseek-v4-flash-0731")
        );
        assert_eq!(
            resolve_fallback_model("deepseek/deepseek-v4-flash-0731:floor").as_deref(),
            Some("z-ai/glm-5.3-flash")
        );
        // An unrelated model still gets no fallback.
        assert_eq!(resolve_fallback_model("acme/other-model").as_deref(), None);
    }

    #[test]
    fn resolve_fallback_model_env_override_is_honoured() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "OPENROUTER_FALLBACK_MODEL_JSON",
            r#"{"deepseek/deepseek-v4-flash-0731": "some/other-backup-model"}"#,
        );
        let result = resolve_fallback_model("deepseek/deepseek-v4-flash-0731");
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        assert_eq!(result.as_deref(), Some("some/other-backup-model"));
    }

    #[test]
    fn resolve_fallback_model_env_override_leaves_an_unmentioned_model_on_its_default() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "OPENROUTER_FALLBACK_MODEL_JSON",
            r#"{"deepseek/deepseek-v4-flash-0731": "some/other-backup-model"}"#,
        );
        // GLM isn't named in the override, so it keeps its hardcoded default.
        let result = resolve_fallback_model("z-ai/glm-5.3-flash");
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        assert_eq!(result.as_deref(), Some("deepseek/deepseek-v4-flash-0731"));
    }

    #[test]
    fn resolve_fallback_model_rejects_a_self_referential_mapping() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "OPENROUTER_FALLBACK_MODEL_JSON",
            r#"{"deepseek/deepseek-v4-flash-0731": "deepseek/deepseek-v4-flash-0731"}"#,
        );
        let result = resolve_fallback_model("deepseek/deepseek-v4-flash-0731");
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        assert_eq!(
            result.as_deref(),
            Some("z-ai/glm-5.3-flash"),
            "a self-referential override must be rejected and fall back to the hardcoded default"
        );
    }

    #[test]
    fn resolve_fallback_model_rejects_a_blank_value() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "OPENROUTER_FALLBACK_MODEL_JSON",
            r#"{"deepseek/deepseek-v4-flash-0731": "   "}"#,
        );
        let result = resolve_fallback_model("deepseek/deepseek-v4-flash-0731");
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        assert_eq!(
            result.as_deref(),
            Some("z-ai/glm-5.3-flash"),
            "a blank/whitespace-only override value must be rejected and fall back to the \
             hardcoded default"
        );
    }

    #[test]
    fn resolve_fallback_model_trims_the_override_value() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "OPENROUTER_FALLBACK_MODEL_JSON",
            "{\"deepseek/deepseek-v4-flash-0731\": \"  some/other-backup-model\\n\"}",
        );
        let result = resolve_fallback_model("deepseek/deepseek-v4-flash-0731");
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        assert_eq!(
            result.as_deref(),
            Some("some/other-backup-model"),
            "a `fly secrets set ... < file`-style trailing newline/whitespace in the override \
             value must be trimmed before use"
        );
    }

    #[test]
    fn resolve_fallback_model_malformed_env_override_falls_back_to_defaults() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::set_var("OPENROUTER_FALLBACK_MODEL_JSON", "not valid json");
        let result = resolve_fallback_model("z-ai/glm-5.3-flash");
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        assert_eq!(result.as_deref(), Some("deepseek/deepseek-v4-flash-0731"));
    }

    #[test]
    fn resolve_fallback_model_unmapped_model_stays_none_without_env_override() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        assert_eq!(resolve_fallback_model("totally/unknown-model"), None);
    }

    // ── finish_reason -> stop_reason mapping ────────────────────────────────────

    #[test]
    fn map_finish_reason_maps_length_to_max_tokens_sentinel() {
        assert_eq!(
            map_finish_reason(Some("length")).as_deref(),
            Some(crate::llm::STOP_REASON_MAX_TOKENS)
        );
    }

    #[test]
    fn map_finish_reason_maps_stop_to_end_turn() {
        assert_eq!(map_finish_reason(Some("stop")).as_deref(), Some("end_turn"));
    }

    #[test]
    fn map_finish_reason_maps_tool_calls_to_tool_use() {
        assert_eq!(map_finish_reason(Some("tool_calls")).as_deref(), Some("tool_use"));
    }

    #[test]
    fn map_finish_reason_passes_through_unrecognized_values() {
        assert_eq!(map_finish_reason(Some("content_filter")).as_deref(), Some("content_filter"));
    }

    #[test]
    fn map_finish_reason_none_stays_none() {
        assert_eq!(map_finish_reason(None), None);
    }

    // ── map_status ───────────────────────────────────────────────────────────

    #[test]
    fn map_status_maps_408_to_timeout_transient() {
        let err = map_status(408, "timed out".into());
        assert!(matches!(err, LlmError::Timeout));
        assert!(err.is_transient(), "408 must be treated as transient/retryable");
    }

    #[test]
    fn map_status_maps_429_to_rate_limit() {
        assert!(matches!(map_status(429, "x".into()), LlmError::RateLimit(_)));
    }

    #[test]
    fn map_status_maps_401_and_403_to_auth() {
        assert!(matches!(map_status(401, "x".into()), LlmError::Auth(_)));
        assert!(matches!(map_status(403, "x".into()), LlmError::Auth(_)));
    }

    #[test]
    fn map_status_maps_404_to_model_not_found_not_transient() {
        let err = map_status(404, "no allowed providers".into());
        assert!(matches!(err, LlmError::ModelNotFound(_)));
        assert!(
            !err.is_transient(),
            "a 404 provider/model constraint mismatch is permanent, not transient — only the \
             documented 503 case is"
        );
    }

    #[test]
    fn map_status_maps_503_to_transient_upstream() {
        let err = map_status(503, "no available provider".into());
        assert!(matches!(err, LlmError::Upstream { status: 503, .. }));
        assert!(err.is_transient(), "503 must be transient — >= 500");
    }

    // ── Embedded `error` object (cutover-blocking) ──────────────────────────────

    #[test]
    fn openai_error_to_llm_error_maps_numeric_code_via_map_status() {
        let error = OpenAiErrorBody { code: Some(429), message: Some("rate limited".into()) };
        assert!(matches!(openai_error_to_llm_error(error), LlmError::RateLimit(_)));
    }

    #[test]
    fn openai_error_to_llm_error_parses_a_numeric_string_code() {
        let raw = r#"{ "code": "500", "message": "boom" }"#;
        let error: OpenAiErrorBody = serde_json::from_str(raw).unwrap();
        assert_eq!(error.code, Some(500));
    }

    #[test]
    fn openai_error_to_llm_error_falls_back_to_transient_upstream_when_code_absent() {
        let error = OpenAiErrorBody { code: None, message: Some("mystery failure".into()) };
        let err = openai_error_to_llm_error(error);
        assert!(matches!(err, LlmError::Upstream { status: 0, .. }));
        assert!(err.is_transient(), "an unrecognized error shape must default to retryable");
    }

    #[tokio::test]
    async fn complete_returns_err_for_a_200_body_that_is_error_only() {
        use axum::response::IntoResponse;
        use axum::Json;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                Json(serde_json::json!({
                    "error": { "code": 429, "message": "rate limited by upstream backend" }
                }))
                .into_response()
            }),
        );
        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let err = provider
            .complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect_err("an error-only 200 body must surface as Err, never a partial success");
        assert!(matches!(err, LlmError::RateLimit(_)));
    }

    #[tokio::test]
    async fn complete_returns_err_when_finish_reason_is_error_with_no_top_level_error() {
        use axum::response::IntoResponse;
        use axum::Json;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                Json(serde_json::json!({
                    "choices": [{ "message": { "content": "" }, "finish_reason": "error" }],
                    "usage": { "prompt_tokens": 5, "completion_tokens": 0 },
                    "model": "gpt-4o-mini"
                }))
                .into_response()
            }),
        );
        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let err = provider
            .complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect_err("finish_reason=\"error\" must surface as Err");
        assert!(matches!(err, LlmError::Upstream { .. }));
    }

    #[tokio::test]
    async fn stream_complete_yields_err_for_a_mid_stream_error_chunk_with_no_done() {
        use axum::response::IntoResponse;
        use futures::StreamExt;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                // No [DONE] after the error chunk — matches the real wire
                // behavior this test guards against.
                let sse_body = concat!(
                    "data: {\"model\":\"deepseek/deepseek-v4-flash-0731\",\"choices\":[{\"delta\":{\"content\":\"partial \"}}]}\n\n",
                    "data: {\"error\":{\"code\":500,\"message\":\"upstream backend failed mid-generation\"},\"choices\":[{\"finish_reason\":\"error\"}]}\n\n",
                );
                ([("content-type", "text/event-stream")], sse_body).into_response()
            }),
        );
        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let mut stream = provider
            .stream_complete(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off))
            .await
            .expect("stream should open");

        let mut saw_delta = false;
        let mut err = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(StreamEvent::Delta(_)) => saw_delta = true,
                Ok(StreamEvent::Final(_)) => panic!("must never yield a Final event for an errored stream"),
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert!(saw_delta, "the partial delta before the error chunk should still have streamed");
        let err = err.expect("the stream must yield an Err for the mid-stream error chunk");
        assert!(matches!(err, LlmError::Upstream { status: 500, .. }));
    }

    /// OpenRouter retry fix (item 4e, streaming): `finish_reason: "error"`
    /// with NO content ever yielded first is now RETRIED (a fresh
    /// connection, same `RetryPolicy`) rather than surfaced immediately. This
    /// mock ALWAYS answers the same error chunk, so every attempt fails and
    /// the stream still ends in an `Err` once `LLM_MAX_RETRIES` is exhausted
    /// — same observable outcome this test always asserted, now reached
    /// after `max_attempts` real requests instead of one.
    #[tokio::test]
    async fn stream_complete_yields_err_when_finish_reason_error_has_no_top_level_error() {
        use axum::response::IntoResponse;
        use futures::StreamExt;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_MAX_RETRIES", "3");
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let sse_body = concat!(
                        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"error\"}]}\n\n",
                        "data: [DONE]\n\n",
                    );
                    ([("content-type", "text/event-stream")], sse_body).into_response()
                }
            }),
        );
        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let mut stream = provider
            .stream_complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect("stream should open");

        let mut err = None;
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                err = Some(e);
                break;
            }
        }

        std::env::remove_var("LLM_MAX_RETRIES");
        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert!(err.is_some(), "finish_reason=\"error\" alone must still yield an Err after exhaustion");
        assert!(
            matches!(err, Some(LlmError::Upstream { status: 0, .. })),
            "must be the SAME error shape as before this fix"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "must have retried up to LLM_MAX_RETRIES=3 real requests, never yielding Err on the \
             first"
        );
    }

    // ── Stream ordering: finish_reason before usage on the SAME chunk ──────────

    /// The actual regression proof for item 7: `finish_reason: "length"`
    /// arrives on the SAME chunk as the terminal usage payload — the OLD
    /// code processed `usage` first, so `Final.stop_reason` would have read
    /// a stale (`None`) value instead of the max_tokens sentinel from THIS
    /// chunk.
    #[tokio::test]
    async fn stream_complete_finish_reason_on_the_usage_chunk_itself_is_not_lost() {
        use axum::response::IntoResponse;
        use futures::StreamExt;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                let sse_body = concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
                    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
                    "data: [DONE]\n\n",
                );
                ([("content-type", "text/event-stream")], sse_body).into_response()
            }),
        );
        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let mut stream = provider
            .stream_complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect("stream should open");

        let mut final_event = None;
        while let Some(item) = stream.next().await {
            if let StreamEvent::Final(f) = item.expect("stream item should be Ok") {
                final_event = Some(f);
            }
        }
        let f = final_event.expect("final event should be yielded");
        assert_eq!(
            f.stop_reason.as_deref(),
            Some(crate::llm::STOP_REASON_MAX_TOKENS),
            "finish_reason on the SAME chunk as usage must not be lost"
        );
    }

    // ── Forced tool use — response parsing (audit H3 parity) ───────────────────

    #[test]
    fn extract_openai_content_parses_tool_call_arguments_into_canonical_json() {
        let raw = r#"{
            "message": {
                "content": null,
                "tool_calls": [{
                    "function": { "arguments": "{\"entities\": [\"Alice\", \"Bob\"]}" }
                }]
            },
            "finish_reason": "tool_calls"
        }"#;
        let choice: OpenAiChoice = serde_json::from_str(raw).unwrap();
        let content = extract_openai_content(choice, true);
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["entities"], json!(["Alice", "Bob"]));
    }

    #[test]
    fn extract_openai_content_falls_back_to_plain_text_when_no_tool_call_present() {
        let raw = r#"{
            "message": { "content": "I cannot comply with that tool schema." },
            "finish_reason": "stop"
        }"#;
        let choice: OpenAiChoice = serde_json::from_str(raw).unwrap();
        let content = extract_openai_content(choice, true);
        assert_eq!(content, "I cannot comply with that tool schema.");
    }

    #[test]
    fn extract_openai_content_passes_through_malformed_tool_arguments_leniently() {
        let raw = r#"{
            "message": {
                "content": null,
                "tool_calls": [{ "function": { "arguments": "not valid json" } }]
            },
            "finish_reason": "tool_calls"
        }"#;
        let choice: OpenAiChoice = serde_json::from_str(raw).unwrap();
        let content = extract_openai_content(choice, true);
        assert_eq!(content, "not valid json");
    }

    #[test]
    fn extract_openai_content_ignores_tool_calls_when_not_forced() {
        // forced_tool: false ⇒ always plain text, even if the model
        // gratuitously included tool_calls.
        let raw = r#"{
            "message": {
                "content": "plain answer",
                "tool_calls": [{ "function": { "arguments": "{}" } }]
            },
            "finish_reason": "stop"
        }"#;
        let choice: OpenAiChoice = serde_json::from_str(raw).unwrap();
        let content = extract_openai_content(choice, false);
        assert_eq!(content, "plain answer");
    }

    // ── Prompt-cache token parsing (response side) ───────────────────────────────

    #[test]
    fn openai_response_parses_cache_tokens_when_present() {
        let raw = r#"{
            "choices": [{ "message": { "content": "hi" }, "finish_reason": "stop" }],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 20,
                "prompt_tokens_details": {
                    "cached_tokens": 3000,
                    "cache_write_tokens": 1500
                }
            },
            "model": "gpt-4o"
        }"#;
        let parsed: OpenAiResponse = serde_json::from_str(raw).unwrap();
        let usage = parsed.usage.expect("usage present");
        assert_eq!(usage.prompt_tokens, 100);
        assert_eq!(usage.completion_tokens, 20);
        let (cache_read, cache_creation) = cache_tokens_from_details(&usage.prompt_tokens_details);
        assert_eq!(cache_read, 3000);
        assert_eq!(cache_creation, 1500);
    }

    #[test]
    fn openai_response_cache_tokens_default_to_zero_when_absent() {
        // Older models / non-cached responses omit `prompt_tokens_details` entirely.
        let raw = r#"{
            "choices": [{ "message": { "content": "hi" } }],
            "usage": { "prompt_tokens": 50, "completion_tokens": 10 },
            "model": "gpt-4o-mini"
        }"#;
        let parsed: OpenAiResponse = serde_json::from_str(raw).unwrap();
        let usage = parsed.usage.expect("usage present");
        assert!(usage.prompt_tokens_details.is_none());
        let (cache_read, cache_creation) = cache_tokens_from_details(&usage.prompt_tokens_details);
        assert_eq!(cache_read, 0);
        assert_eq!(cache_creation, 0);
    }

    #[test]
    fn openai_response_cache_tokens_default_to_zero_when_details_present_but_fields_missing() {
        // `prompt_tokens_details` present but without the cache sub-fields (e.g. a
        // model that reports the object but not (yet) cache stats).
        let raw = r#"{
            "choices": [{ "message": { "content": "hi" } }],
            "usage": {
                "prompt_tokens": 50,
                "completion_tokens": 10,
                "prompt_tokens_details": {}
            },
            "model": "gpt-4o-mini"
        }"#;
        let parsed: OpenAiResponse = serde_json::from_str(raw).unwrap();
        let usage = parsed.usage.expect("usage present");
        let (cache_read, cache_creation) = cache_tokens_from_details(&usage.prompt_tokens_details);
        assert_eq!(cache_read, 0);
        assert_eq!(cache_creation, 0);
    }

    #[test]
    fn openai_response_missing_choices_and_usage_without_an_error_field_is_a_parse_error() {
        // Genuinely malformed — no `error`, but also no `usage`. Must not
        // silently succeed with fabricated zero token counts.
        let raw = r#"{ "model": "gpt-4o-mini" }"#;
        let parsed: OpenAiResponse = serde_json::from_str(raw).unwrap();
        assert!(parsed.error.is_none());
        assert!(parsed.usage.is_none());
    }

    #[test]
    fn openai_stream_usage_parses_cache_tokens_when_present() {
        let raw = r#"{
            "prompt_tokens": 200,
            "completion_tokens": 40,
            "prompt_tokens_details": { "cached_tokens": 128 }
        }"#;
        let parsed: StreamUsage = serde_json::from_str(raw).unwrap();
        let (cache_read, cache_creation) =
            cache_tokens_from_details(&parsed.prompt_tokens_details);
        assert_eq!(cache_read, 128);
        assert_eq!(cache_creation, 0, "cache_write_tokens absent ⇒ 0");
    }

    #[test]
    fn openai_stream_usage_cache_tokens_default_to_zero_when_absent() {
        let raw = r#"{ "prompt_tokens": 200, "completion_tokens": 40 }"#;
        let parsed: StreamUsage = serde_json::from_str(raw).unwrap();
        let (cache_read, cache_creation) =
            cache_tokens_from_details(&parsed.prompt_tokens_details);
        assert_eq!(cache_read, 0);
        assert_eq!(cache_creation, 0);
    }

    // ── PR3: provider-reported cost (OpenRouter's `usage.include` shape) ────────
    //
    // Real `api.openai.com` never sends a `cost` field on `usage` — these tests
    // exercise the OPTIONAL parse: present ⇒ surfaced, absent ⇒ `None` (never
    // `0.0`).

    #[test]
    fn openai_response_parses_cost_when_present() {
        let raw = r#"{
            "choices": [{ "message": { "content": "hi" } }],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 20,
                "cost": 0.00234
            },
            "model": "anthropic/claude-sonnet-5"
        }"#;
        let parsed: OpenAiResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.usage.expect("usage present").cost, Some(0.00234));
    }

    #[test]
    fn openai_response_cost_defaults_to_none_when_absent() {
        // The real OpenAI wire shape — no `cost` field at all.
        let raw = r#"{
            "choices": [{ "message": { "content": "hi" } }],
            "usage": { "prompt_tokens": 50, "completion_tokens": 10 },
            "model": "gpt-4o-mini"
        }"#;
        let parsed: OpenAiResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(
            parsed.usage.expect("usage present").cost, None,
            "absent cost must parse as None, never 0.0"
        );
    }

    #[test]
    fn openai_stream_usage_parses_cost_when_present() {
        let raw = r#"{
            "prompt_tokens": 200,
            "completion_tokens": 40,
            "cost": 0.0091
        }"#;
        let parsed: StreamUsage = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.cost, Some(0.0091));
    }

    #[test]
    fn openai_stream_usage_cost_defaults_to_none_when_absent() {
        let raw = r#"{ "prompt_tokens": 200, "completion_tokens": 40 }"#;
        let parsed: StreamUsage = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.cost, None);
    }

    #[test]
    fn openai_stream_chunk_parses_model_when_present() {
        let raw = r#"{
            "model": "anthropic/claude-sonnet-5",
            "choices": [{ "delta": { "content": "hi" } }]
        }"#;
        let parsed: StreamChunk = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.model.as_deref(), Some("anthropic/claude-sonnet-5"));
    }

    #[test]
    fn openai_stream_chunk_model_defaults_to_none_when_absent() {
        let raw = r#"{ "choices": [{ "delta": { "content": "hi" } }] }"#;
        let parsed: StreamChunk = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.model, None);
    }

    // ── End-to-end: complete() / stream_complete() actually construct the
    //    fields above, not just parse the wire shape in isolation ──────────────

    /// PR3 negative control: a provider (the common, real-OpenAI case) that
    /// reports no cost must leave `CompletionResponse.cost_usd` as `None` —
    /// never `0.0`, which downstream would read as "this call was free."
    #[tokio::test]
    async fn complete_cost_usd_none_when_upstream_omits_it() {
        use axum::response::IntoResponse;
        use axum::Json;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                Json(serde_json::json!({
                    "choices": [{ "message": { "content": "hi" }, "finish_reason": "stop" }],
                    "usage": { "prompt_tokens": 50, "completion_tokens": 10 },
                    "model": "gpt-4o-mini"
                }))
                .into_response()
            }),
        );

        let addr = spawn_mock(app).await;

        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let resp = provider
            .complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect("complete should succeed");
        assert_eq!(resp.cost_usd, None);
        assert_eq!(resp.stop_reason.as_deref(), Some("end_turn"));
    }

    /// The positive counterpart: when the upstream DOES report cost (the
    /// OpenRouter shape), it passes straight through.
    #[tokio::test]
    async fn complete_cost_usd_passes_through_when_upstream_reports_it() {
        use axum::response::IntoResponse;
        use axum::Json;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                Json(serde_json::json!({
                    "choices": [{ "message": { "content": "hi" }, "finish_reason": "stop" }],
                    "usage": { "prompt_tokens": 50, "completion_tokens": 10, "cost": 0.00456 },
                    "model": "anthropic/claude-sonnet-5"
                }))
                .into_response()
            }),
        );

        let addr = spawn_mock(app).await;

        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let resp = provider
            .complete(req("anthropic/claude-sonnet-5", None, ReasoningSetting::Off))
            .await
            .expect("complete should succeed");
        assert_eq!(resp.cost_usd, Some(0.00456));
    }

    /// A `finish_reason: "length"` response must surface as the
    /// `max_tokens` truncation sentinel so `is_truncated`/the
    /// truncation-aware retries fire for OpenAI/OpenRouter the same as for
    /// Anthropic.
    #[tokio::test]
    async fn complete_maps_length_finish_reason_to_max_tokens_sentinel() {
        use axum::response::IntoResponse;
        use axum::Json;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                Json(serde_json::json!({
                    "choices": [{ "message": { "content": "cut off mid" }, "finish_reason": "length" }],
                    "usage": { "prompt_tokens": 50, "completion_tokens": 10 },
                    "model": "gpt-4o-mini"
                }))
                .into_response()
            }),
        );

        let addr = spawn_mock(app).await;

        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let resp = provider
            .complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect("complete should succeed");
        assert!(crate::llm::is_truncated(&resp));
    }

    /// End-to-end forced tool use via a real (mock) HTTP round trip: the
    /// request carries `tools`/`tool_choice`, and the response's
    /// `tool_calls[0].function.arguments` JSON string lands in
    /// `CompletionResponse.content` as canonical JSON.
    #[tokio::test]
    async fn complete_round_trips_forced_tool_use() {
        use axum::response::IntoResponse;
        use axum::Json;
        for strict in [false, true] {
            let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
            let capture = captured.clone();
            let app = axum::Router::new().route(
                "/v1/chat/completions",
                axum::routing::post(move |Json(body): Json<serde_json::Value>| {
                    *capture.lock().unwrap() = Some(body);
                    async {
                        Json(serde_json::json!({
                            "choices": [{"message": {"content": null, "tool_calls": [{
                                "function": { "arguments": "{\"entities\":[\"Alice\"]}" }
                            }]}, "finish_reason": "tool_calls"}],
                            "usage": { "prompt_tokens": 50, "completion_tokens": 10 },
                            "model": "deepseek/deepseek-v4-flash-0731"
                        })).into_response()
                    }
                }),
            );
            let addr = spawn_mock(app).await;
            let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "test-key")
                .with_base_url(format!("http://{addr}"));
            let mut tool = tool_schema();
            tool.strict = strict;
            let resp = provider.complete(req("deepseek/deepseek-v4-flash-0731", Some(tool), ReasoningSetting::Off))
                .await.expect("complete should succeed");
            let body = captured.lock().unwrap().take().unwrap();
            if strict {
                assert_eq!(body["tools"][0]["function"]["strict"], true);
            } else {
                assert!(body["tools"][0]["function"].get("strict").is_none());
            }
            assert_eq!(serde_json::from_str::<serde_json::Value>(&resp.content).unwrap()["entities"], json!(["Alice"]));
            assert_eq!(resp.stop_reason.as_deref(), Some("tool_use"));
        }
    }

    /// End-to-end streaming: the served model (tracked across chunks) and
    /// provider-reported cost (from the terminal usage chunk) both reach the
    /// `StreamEvent::Final` frame, and `finish_reason` from the
    /// second-to-last chunk survives onto that same Final frame.
    #[tokio::test]
    async fn stream_complete_carries_served_model_cost_and_stop_reason_from_final_chunk() {
        use axum::response::IntoResponse;
        use futures::StreamExt;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                let sse_body = concat!(
                    "data: {\"model\":\"anthropic/claude-sonnet-5\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
                    "data: {\"model\":\"anthropic/claude-sonnet-5\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: {\"model\":\"anthropic/claude-sonnet-5\",\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"cost\":0.00077},\"choices\":[]}\n\n",
                    "data: [DONE]\n\n",
                );
                ([("content-type", "text/event-stream")], sse_body).into_response()
            }),
        );

        let addr = spawn_mock(app).await;

        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let mut stream = provider
            .stream_complete(req("openrouter/auto", None, ReasoningSetting::Off))
            .await
            .expect("stream should open");

        let mut final_event = None;
        while let Some(item) = stream.next().await {
            if let StreamEvent::Final(f) = item.expect("stream item should be Ok") {
                final_event = Some(f);
            }
        }
        let f = final_event.expect("final event should be yielded");
        assert_eq!(f.model.as_deref(), Some("anthropic/claude-sonnet-5"));
        assert_eq!(f.cost_usd, Some(0.00077));
        assert_eq!(f.stop_reason.as_deref(), Some("end_turn"));
    }

    /// Negative control on the streaming path: no `cost` on the usage chunk
    /// and no `model` on any chunk ⇒ both `None`, never a default.
    #[tokio::test]
    async fn stream_complete_model_and_cost_none_when_upstream_omits_them() {
        use axum::response::IntoResponse;
        use futures::StreamExt;

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                let sse_body = concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
                    "data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2},\"choices\":[]}\n\n",
                    "data: [DONE]\n\n",
                );
                ([("content-type", "text/event-stream")], sse_body).into_response()
            }),
        );

        let addr = spawn_mock(app).await;

        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let mut stream = provider
            .stream_complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect("stream should open");

        let mut final_event = None;
        while let Some(item) = stream.next().await {
            if let StreamEvent::Final(f) = item.expect("stream item should be Ok") {
                final_event = Some(f);
            }
        }
        let f = final_event.expect("final event should be yielded");
        assert_eq!(f.model, None, "no chunk named a model ⇒ None, not empty string");
        assert_eq!(f.cost_usd, None, "no cost reported ⇒ None, never 0.0");
        assert_eq!(f.stop_reason, None, "no chunk named a finish_reason ⇒ None");
    }

    // ── Per-call retry (item 3) ──────────────────────────────────────────────

    #[tokio::test]
    async fn complete_retries_a_transient_429_then_succeeds() {
        use axum::response::IntoResponse;
        use axum::Json;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        (axum::http::StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response()
                    } else {
                        Json(serde_json::json!({
                            "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
                            "usage": { "prompt_tokens": 5, "completion_tokens": 2 },
                            "model": "gpt-4o-mini"
                        }))
                        .into_response()
                    }
                }
            }),
        );

        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let (resp, diagnostic_attempts) = super::super::CALL_DIAGNOSTICS.scope(
            std::cell::RefCell::new(Default::default()), async {
                let resp = provider.complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
                    .await.expect("should retry past the transient 429 and succeed");
                (resp, super::super::current_diagnostics().unwrap().attempt_count)
            }).await;
        assert_eq!(diagnostic_attempts, 2, "metadata must count the recovered attempt");

        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert_eq!(attempts.load(Ordering::SeqCst), 2, "expected exactly one retry");
        assert_eq!(resp.content, "ok");
    }

    #[tokio::test]
    async fn complete_does_not_retry_a_permanent_auth_error() {
        use axum::response::IntoResponse;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    (axum::http::StatusCode::UNAUTHORIZED, "invalid api key").into_response()
                }
            }),
        );

        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let err = provider
            .complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect_err("an auth error must fail fast, never retry");

        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert!(matches!(err, LlmError::Auth(_)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "must not retry a permanent auth error");
    }

    #[tokio::test]
    async fn stream_complete_retries_a_transient_5xx_stream_open_then_succeeds() {
        use axum::response::IntoResponse;
        use futures::StreamExt;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "no available provider")
                            .into_response()
                    } else {
                        let sse_body = concat!(
                            "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
                            "data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2},\"choices\":[]}\n\n",
                            "data: [DONE]\n\n",
                        );
                        ([("content-type", "text/event-stream")], sse_body).into_response()
                    }
                }
            }),
        );

        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let mut stream = provider
            .stream_complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect("should retry past the transient 503 and open the stream");

        let mut deltas = Vec::new();
        while let Some(item) = stream.next().await {
            if let StreamEvent::Delta(d) = item.expect("stream item should be Ok") {
                deltas.push(d.text);
            }
        }

        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert_eq!(attempts.load(Ordering::SeqCst), 2, "expected exactly one stream-open retry");
        assert_eq!(deltas.join(""), "hi");
    }

    // ── OpenRouter retry fix (2026-09-07): retry the BODY-level failure ────────
    //
    // Measured in production: `z-ai/glm-5.3-flash` via OpenRouter failed 16%
    // of calls, every one an HTTP 200 whose body carried
    // `choices[0].finish_reason == "error"` with no top-level `error` object
    // — a shape the retry loop above never saw because it wrapped only the
    // network `send`. These tests pin the fix: one ATTEMPT is now send +
    // parse + body-check.

    /// Item 4(a): first response `finish_reason: "error"`, second succeeds —
    /// the call succeeds and made exactly two requests.
    #[tokio::test]
    async fn complete_retries_a_finish_reason_error_body_then_succeeds() {
        use axum::response::IntoResponse;
        use axum::Json;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        Json(serde_json::json!({
                            "choices": [{ "message": { "content": "" }, "finish_reason": "error" }],
                            "usage": { "prompt_tokens": 5, "completion_tokens": 0 },
                            "model": "z-ai/glm-5.3-flash",
                            "provider": "DeepInfra"
                        }))
                        .into_response()
                    } else {
                        Json(serde_json::json!({
                            "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
                            "usage": { "prompt_tokens": 5, "completion_tokens": 2 },
                            "model": "z-ai/glm-5.3-flash",
                            "provider": "Fireworks"
                        }))
                        .into_response()
                    }
                }
            }),
        );

        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let resp = provider
            .complete(req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off))
            .await
            .expect("should retry past the finish_reason=\"error\" body and succeed");

        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert_eq!(attempts.load(Ordering::SeqCst), 2, "expected exactly two requests");
        assert_eq!(resp.content, "ok");
    }

    /// Item 4(b): every attempt returns `finish_reason: "error"` — the call
    /// fails after `LLM_MAX_RETRIES` attempts with the SAME `LlmError::
    /// Upstream { status: 0, .. }` message a caller saw before this fix (on
    /// the very first, unretried attempt).
    #[tokio::test]
    async fn complete_fails_after_max_attempts_with_the_same_error_as_before_the_fix() {
        use axum::response::IntoResponse;
        use axum::Json;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_MAX_RETRIES", "3");
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "choices": [{ "message": { "content": "" }, "finish_reason": "error" }],
                        "usage": { "prompt_tokens": 5, "completion_tokens": 0 },
                        "model": "z-ai/glm-5.3-flash"
                    }))
                    .into_response()
                }
            }),
        );

        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let err = provider
            .complete(req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off))
            .await
            .expect_err("every attempt failing must still end in an Err");

        std::env::remove_var("LLM_MAX_RETRIES");
        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert_eq!(attempts.load(Ordering::SeqCst), 3, "must make exactly LLM_MAX_RETRIES attempts");
        match &err {
            LlmError::Upstream { status, body } => {
                assert_eq!(*status, 0);
                assert_eq!(
                    body,
                    "OpenAI/OpenRouter response finish_reason=\"error\" with no top-level error field"
                );
            }
            other => panic!("expected the SAME Upstream{{status:0}} shape as before this fix, got {other:?}"),
        }
    }

    /// Item 4(c): a NON-transient body-level error (mapped from a `code`
    /// that isn't retryable, e.g. 400) must NOT be retried — same "fail fast
    /// on a permanent error" contract the network-level retry loop already
    /// had.
    #[tokio::test]
    async fn complete_does_not_retry_a_non_transient_body_level_error() {
        use axum::response::IntoResponse;
        use axum::Json;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "error": { "code": 400, "message": "bad request body" }
                    }))
                    .into_response()
                }
            }),
        );

        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let err = provider
            .complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect_err("a non-transient error-only 200 body must fail, not succeed");

        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert!(!err.is_transient(), "a mapped 400 must not be classified transient");
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "must not retry a non-transient body error");
    }

    /// Item 4(e), streaming half: a mid-stream `finish_reason: "error"`
    /// chunk arriving BEFORE any content token has been yielded to the
    /// consumer is retried (a fresh connection) and the second attempt
    /// succeeds — the consumer only ever sees the clean result.
    #[tokio::test]
    async fn stream_complete_retries_a_finish_reason_error_before_any_token_then_succeeds() {
        use axum::response::IntoResponse;
        use futures::StreamExt;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        // No content delta at all before the error chunk.
                        let sse_body = concat!(
                            "data: {\"provider\":\"DeepInfra\",\"choices\":[{\"delta\":{},\"finish_reason\":\"error\"}]}\n\n",
                        );
                        ([("content-type", "text/event-stream")], sse_body).into_response()
                    } else {
                        let sse_body = concat!(
                            "data: {\"provider\":\"Fireworks\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
                            "data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2},\"choices\":[]}\n\n",
                            "data: [DONE]\n\n",
                        );
                        ([("content-type", "text/event-stream")], sse_body).into_response()
                    }
                }
            }),
        );

        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let mut stream = provider
            .stream_complete(req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off))
            .await
            .expect("stream should open");

        let mut deltas = Vec::new();
        let mut saw_err = false;
        while let Some(item) = stream.next().await {
            match item {
                Ok(StreamEvent::Delta(d)) => deltas.push(d.text),
                Ok(StreamEvent::Final(_)) => {}
                Err(_) => {
                    saw_err = true;
                    break;
                }
            }
        }

        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert!(!saw_err, "the retried-and-succeeded call must never surface an Err to the consumer");
        assert_eq!(deltas.join(""), "hi");
        assert_eq!(attempts.load(Ordering::SeqCst), 2, "expected exactly one mid-stream retry");
    }

    /// Item 4(e), the other half: content already reached the consumer, so a
    /// SUBSEQUENT mid-stream error must be surfaced exactly as before this
    /// fix — never retried. This is
    /// `stream_complete_yields_err_for_a_mid_stream_error_chunk_with_no_done`
    /// above, restated as an explicit attempt-count proof that no retry (and
    /// therefore no second request) ever happens once content has streamed.
    #[tokio::test]
    async fn stream_complete_does_not_retry_after_content_was_already_yielded() {
        use axum::response::IntoResponse;
        use futures::StreamExt;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_handler = attempts.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let attempts = attempts_for_handler.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    let sse_body = concat!(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"partial \"}}]}\n\n",
                        "data: {\"error\":{\"code\":500,\"message\":\"upstream backend failed mid-generation\"},\"choices\":[{\"finish_reason\":\"error\"}]}\n\n",
                    );
                    ([("content-type", "text/event-stream")], sse_body).into_response()
                }
            }),
        );
        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let mut stream = provider
            .stream_complete(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off))
            .await
            .expect("stream should open");

        let mut saw_delta = false;
        let mut err = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(StreamEvent::Delta(_)) => saw_delta = true,
                Ok(StreamEvent::Final(_)) => panic!("must never yield a Final event for an errored stream"),
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }

        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert!(saw_delta);
        assert!(matches!(err, Some(LlmError::Upstream { status: 500, .. })));
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "must not reopen a connection once content was yielded");
    }

    // ── OpenRouter `provider` field (item 3: record the upstream host) ─────────

    #[test]
    fn openai_response_parses_top_level_provider_field() {
        let raw = r#"{
            "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1 },
            "model": "z-ai/glm-5.3-flash",
            "provider": "DeepInfra"
        }"#;
        let parsed: OpenAiResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.provider.as_deref(), Some("DeepInfra"));
    }

    #[test]
    fn openai_response_provider_absent_on_a_real_openai_body() {
        let raw = r#"{
            "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1 },
            "model": "gpt-4o-mini"
        }"#;
        let parsed: OpenAiResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.provider, None);
    }

    #[test]
    fn stream_chunk_parses_top_level_provider_field() {
        let raw = r#"{"provider":"Fireworks","choices":[{"delta":{"content":"hi"}}]}"#;
        let parsed: StreamChunk = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.provider.as_deref(), Some("Fireworks"));
    }

    /// End-to-end proof that a SUCCESSFUL `complete()` reports the upstream
    /// host into `crate::llm::UPSTREAM_PROVIDER` — the exact mechanism
    /// `usage::metered_complete` uses to persist `llm_usage.
    /// upstream_provider` on a clean completion (item 3).
    #[tokio::test]
    async fn complete_reports_upstream_provider_via_task_local_on_success() {
        let response = serde_json::json!({
            "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1 },
            "model": "z-ai/glm-5.3-flash",
            "provider": "DeepInfra"
        });
        let (addr, _captured) = spawn_capturing_mock(response).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let (result, upstream_provider) = crate::llm::UPSTREAM_PROVIDER
            .scope(std::cell::RefCell::new(None), async {
                let r = provider.complete(req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off)).await;
                (r, crate::llm::take_upstream_provider())
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(upstream_provider.as_deref(), Some("DeepInfra"));
    }

    /// Same mechanism, on the EXHAUSTED-failure path — this is what lets
    /// `metered_complete`'s Err arm attribute an `_errored` row to a host
    /// (item 3's whole point: "we cannot see which host fails").
    #[tokio::test]
    async fn complete_reports_upstream_provider_via_task_local_on_exhausted_failure() {
        use axum::response::IntoResponse;
        use axum::Json;

        let _guard = crate::llm::retry::tests::RETRY_ENV_LOCK.lock().unwrap();
        std::env::set_var("LLM_MAX_RETRIES", "2");
        std::env::set_var("LLM_RETRY_BASE_MS", "1");
        std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                Json(serde_json::json!({
                    "choices": [{ "message": { "content": "" }, "finish_reason": "error" }],
                    "usage": { "prompt_tokens": 5, "completion_tokens": 0 },
                    "model": "z-ai/glm-5.3-flash",
                    "provider": "DeepInfra"
                }))
                .into_response()
            }),
        );
        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let (result, upstream_provider) = crate::llm::UPSTREAM_PROVIDER
            .scope(std::cell::RefCell::new(None), async {
                let r = provider.complete(req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off)).await;
                (r, crate::llm::take_upstream_provider())
            })
            .await;

        std::env::remove_var("LLM_MAX_RETRIES");
        std::env::remove_var("LLM_RETRY_BASE_MS");
        std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");

        assert!(result.is_err());
        assert_eq!(upstream_provider.as_deref(), Some("DeepInfra"));
    }

    // ── Request-capture: prove the wire, not just the parse (item 8) ───────────
    //
    // Negative control this whole section exists for: every test above mocks
    // the RESPONSE and never inspects what was actually SENT — they would
    // all still pass even if `complete()` sent `{}` as the request body.
    // These capture the request itself.

    type CapturedRequest = (serde_json::Value, axum::http::HeaderMap);

    async fn spawn_capturing_mock(
        response_body: serde_json::Value,
    ) -> (std::net::SocketAddr, std::sync::Arc<std::sync::Mutex<Option<CapturedRequest>>>) {
        use axum::extract::Json as JsonExtractor;
        use axum::response::IntoResponse;
        use axum::Json;

        let captured: std::sync::Arc<std::sync::Mutex<Option<CapturedRequest>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let captured_for_handler = captured.clone();

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(
                move |headers: axum::http::HeaderMap, JsonExtractor(body): JsonExtractor<serde_json::Value>| {
                    let captured = captured_for_handler.clone();
                    let response_body = response_body.clone();
                    async move {
                        *captured.lock().unwrap() = Some((body, headers));
                        Json(response_body).into_response()
                    }
                },
            ),
        );

        let addr = spawn_mock(app).await;
        (addr, captured)
    }

    fn ok_response(model: &str) -> serde_json::Value {
        serde_json::json!({
            "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1 },
            "model": model
        })
    }

    /// Scoped Ask's unmanaged answer call orders hosts through the task-local
    /// hint; outside it nothing is sorted, and the data policy never loosens.
    #[tokio::test]
    async fn the_provider_sort_hint_orders_an_unmanaged_call() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        let (addr, captured) = spawn_capturing_mock(ok_response("z-ai/glm-5.3-flash")).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let call = || provider.complete(req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off));
        call().await.unwrap();
        let (plain, _) = captured.lock().unwrap().take().unwrap();
        assert!(plain["provider"].get("sort").is_none());
        crate::llm::with_provider_sort(Some("throughput"), call()).await.unwrap();
        let (sorted, _) = captured.lock().unwrap().take().unwrap();
        assert_eq!(sorted["provider"]["sort"], "throughput");
        assert_eq!(sorted["provider"]["zdr"], true);
        assert_eq!(sorted["provider"]["data_collection"], "deny");
        assert_eq!(sorted["provider"]["quantizations"], plain["provider"]["quantizations"]);
        assert_eq!(sorted["models"], plain["models"]);
        assert_eq!(sorted["model"], "z-ai/glm-5.3-flash");
        crate::llm::with_provider_sort(None, call()).await.unwrap();
        let (off, _) = captured.lock().unwrap().take().unwrap();
        assert!(off["provider"].get("sort").is_none());
    }

    #[tokio::test]
    async fn openrouter_request_wire_pairs_for_deepseek_reasoning_tools_and_headers() {
        // `build_request_body` reads OPENROUTER_FALLBACK_MODEL_JSON on every
        // OpenRouter build, so this test is an env READER and must serialize
        // against the `set_var` calls in the fallback tests — an unsynchronized
        // getenv/setenv pair is a data race, not just a flaky assertion.
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        let (addr, captured) = spawn_capturing_mock(ok_response("deepseek/deepseek-v4-flash-0731")).await;

        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "sk-or-test-key")
            .with_base_url(format!("http://{addr}"));

        provider
            .complete(req(
                "deepseek/deepseek-v4-flash-0731",
                Some(tool_schema()),
                ReasoningSetting::High,
            ))
            .await
            .expect("complete should succeed");

        let (body, headers) = captured.lock().unwrap().take().expect("request must have been captured");

        assert_eq!(body["provider"]["zdr"], true);
        assert_eq!(body["provider"]["data_collection"], "deny");
        assert_eq!(body["provider"]["allow_fallbacks"], true);
        assert!(
            body["provider"].get("order").is_none(),
            "the ACTUAL request over the wire must NOT carry a per-model host allowlist — this \
             is the exact 2026-09-05 regression"
        );
        assert_eq!(body["reasoning"], json!({ "effort": "high" }));
        assert_eq!(body["tools"][0]["function"]["name"], "emit_entities");
        assert_eq!(body["tool_choice"]["function"]["name"], "emit_entities");
        assert_eq!(body["model"], "deepseek/deepseek-v4-flash-0731");

        assert_eq!(
            headers.get("authorization").and_then(|v| v.to_str().ok()),
            Some("Bearer sk-or-test-key")
        );
        assert_eq!(
            headers.get("http-referer").and_then(|v| v.to_str().ok()),
            Some("https://coralswarm.ai")
        );
        assert_eq!(headers.get("x-title").and_then(|v| v.to_str().ok()), Some("CoralSwarm"));
    }

    #[tokio::test]
    async fn openrouter_request_wire_pairs_for_glm_and_reasoning_off() {
        // `build_request_body` reads OPENROUTER_FALLBACK_MODEL_JSON on every
        // OpenRouter build, so this test is an env READER and must serialize
        // against the `set_var` calls in the fallback tests — an unsynchronized
        // getenv/setenv pair is a data race, not just a flaky assertion.
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        // Live smoke-test finding: OpenRouter 400s `z-ai/glm-5.3-flash` on
        // `{"enabled": false}` ("Reasoning is mandatory for this endpoint
        // and cannot be disabled.") — the ACTUAL request over the wire must
        // carry the `{"effort": "low"}` floor instead, not the universal
        // `Off` encoding. See `MANDATORY_REASONING_MODELS`'s doc comment.
        let (addr, captured) = spawn_capturing_mock(ok_response("z-ai/glm-5.3-flash")).await;

        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "sk-or-test-key")
            .with_base_url(format!("http://{addr}"));

        provider
            .complete(req("z-ai/glm-5.3-flash", None, ReasoningSetting::Off))
            .await
            .expect("complete should succeed");

        let (body, _headers) = captured.lock().unwrap().take().expect("request must have been captured");

        assert!(
            body["provider"].get("order").is_none(),
            "the ACTUAL request over the wire must NOT carry a per-model host allowlist"
        );
        assert_eq!(
            body["reasoning"],
            json!({ "effort": "low" }),
            "GLM 400s on {{\"enabled\": false}} — the wire body must carry the effort-low floor"
        );
        assert!(
            body["reasoning"].get("enabled").is_none(),
            "the GLM floor must not also carry an `enabled` key"
        );
        assert!(body.get("tools").is_none(), "no tool_schema ⇒ no tools field on the wire");
    }

    #[tokio::test]
    async fn openrouter_request_wire_pairs_for_deepseek_and_reasoning_off_unchanged() {
        // `build_request_body` reads OPENROUTER_FALLBACK_MODEL_JSON on every
        // OpenRouter build, so this test is an env READER and must serialize
        // against the `set_var` calls in the fallback tests — an unsynchronized
        // getenv/setenv pair is a data race, not just a flaky assertion.
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        // Counterpart to the GLM case above: DeepSeek accepts
        // `{"enabled": false}` fine and must be completely unaffected by the
        // GLM-only reasoning floor.
        let (addr, captured) = spawn_capturing_mock(ok_response("deepseek/deepseek-v4-flash-0731")).await;

        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "sk-or-test-key")
            .with_base_url(format!("http://{addr}"));

        provider
            .complete(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off))
            .await
            .expect("complete should succeed");

        let (body, _headers) = captured.lock().unwrap().take().expect("request must have been captured");

        assert_eq!(body["reasoning"], json!({ "enabled": false }));
    }

    #[tokio::test]
    async fn plain_openai_request_never_carries_the_provider_routing_object_or_attribution_headers() {
        let (addr, captured) = spawn_capturing_mock(ok_response("gpt-4o-mini")).await;

        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "sk-test-key").with_base_url(format!("http://{addr}"));

        provider
            .complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
            .await
            .expect("complete should succeed");

        let (body, headers) = captured.lock().unwrap().take().expect("request must have been captured");
        assert!(body.get("provider").is_none());
        assert!(body.get("models").is_none());
        assert!(headers.get("http-referer").is_none());
        assert!(headers.get("x-title").is_none());
    }

    /// The wire-level counterpart of item 4/5's builder-only assertions
    /// (`build_request_body_sets_glm_as_the_deepseek_fallback` etc.) — proves
    /// `models` reaches the actual socket, not just what the builder
    /// returned in-process. Guarded by `FALLBACK_ENV_LOCK` since, unlike the
    /// removed provider allowlist, `OPENROUTER_FALLBACK_MODEL_JSON` CAN
    /// override the production default this test pins.
    #[tokio::test]
    async fn openrouter_request_wire_carries_the_provider_policy_and_models_fallback() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");

        let (addr, captured) = spawn_capturing_mock(ok_response("deepseek/deepseek-v4-flash-0731")).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "sk-or-test-key")
            .with_base_url(format!("http://{addr}"));

        provider
            .complete(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off))
            .await
            .expect("complete should succeed");

        let (body, _headers) = captured.lock().unwrap().take().expect("request must have been captured");

        assert_eq!(
            body["provider"],
            json!({
                "zdr": true,
                "data_collection": "deny",
                "quantizations": ["fp8", "bf16"],
                "allow_fallbacks": true,
                "require_parameters": true,
            }),
            "the ACTUAL request over the wire must carry exactly the five-key policy object"
        );
        assert_eq!(
            body["models"],
            json!(["z-ai/glm-5.3-flash"]),
            "the ACTUAL request over the wire must carry the GLM cross-fallback for a DeepSeek \
             primary"
        );
    }

    #[tokio::test]
    async fn complete_maps_cache_tokens_through_a_real_http_round_trip() {
        let response = serde_json::json!({
            "choices": [{ "message": { "content": "hi" }, "finish_reason": "stop" }],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 20,
                "prompt_tokens_details": { "cached_tokens": 3000, "cache_write_tokens": 1500 }
            },
            "model": "gpt-4o"
        });
        let (addr, _captured) = spawn_capturing_mock(response).await;

        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "test-key").with_base_url(format!("http://{addr}"));

        let resp = provider
            .complete(req("gpt-4o", None, ReasoningSetting::Off))
            .await
            .expect("complete should succeed");

        assert_eq!(resp.cache_read_input_tokens, 3000, "cached_tokens -> cache_read_input_tokens");
        assert_eq!(
            resp.cache_creation_input_tokens, 1500,
            "cache_write_tokens -> cache_creation_input_tokens"
        );
    }


    struct RecordingGate {
        terms: Option<SendTerms>,
        policy: TransportPolicy,
        deny_on: Option<u32>,
        reserves: std::sync::Mutex<Vec<u32>>,
        settled: std::sync::Mutex<Vec<(u32, SendOutcome)>>,
    }

    impl RecordingGate {
        fn new() -> Self {
            Self {
                terms: None,
                policy: TransportPolicy::default(),
                deny_on: None,
                reserves: std::sync::Mutex::new(Vec::new()),
                settled: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn outcomes(&self) -> Vec<SendOutcome> {
            self.settled.lock().unwrap().iter().map(|(_, o)| o.clone()).collect()
        }
    }

    impl SendGate for RecordingGate {
        fn terms(&self) -> Option<&SendTerms> {
            self.terms.as_ref()
        }
        fn transport(&self) -> TransportPolicy {
            self.policy.clone()
        }
        fn reserve<'a>(
            &'a self,
            attempt_no: u32,
        ) -> send_gate::GateFuture<'a, Result<send_gate::Reservation, send_gate::Deny>> {
            Box::pin(async move {
                self.reserves.lock().unwrap().push(attempt_no);
                if self.deny_on == Some(attempt_no) {
                    return Err(send_gate::Deny::Hold(send_gate::HoldCode::DailyBudgetExhausted));
                }
                Ok(send_gate::Reservation::default())
            })
        }
        fn settle<'a>(&'a self, sent: Sent, outcome: &'a SendOutcome) -> send_gate::GateFuture<'a, ()> {
            Box::pin(async move {
                self.settled.lock().unwrap().push((sent.attempt_no(), outcome.clone()));
            })
        }
    }

    async fn scripted_mock(
        script: Vec<(u16, serde_json::Value)>,
    ) -> (std::net::SocketAddr, std::sync::Arc<std::sync::atomic::AtomicU32>) {
        use axum::response::IntoResponse;
        use std::sync::atomic::{AtomicU32, Ordering};
        let hits = std::sync::Arc::new(AtomicU32::new(0));
        let counter = hits.clone();
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let counter = counter.clone();
                let script = script.clone();
                async move {
                    let n = counter.fetch_add(1, Ordering::SeqCst) as usize;
                    let (status, body) = script[n.min(script.len() - 1)].clone();
                    let status = axum::http::StatusCode::from_u16(status).unwrap();
                    if body.is_string() {
                        (status, body.as_str().unwrap().to_string()).into_response()
                    } else {
                        (status, axum::Json(body)).into_response()
                    }
                }
            }),
        );
        (spawn_mock(app).await, hits)
    }

    fn billed_body(model: &str, cost: f64) -> serde_json::Value {
        serde_json::json!({
            "id": "gen-1",
            "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 7, "completion_tokens": 3, "cost": cost },
            "model": model,
            "provider": "DeepInfra"
        })
    }

    struct RetryEnv(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);
    impl RetryEnv {
        fn set(max_attempts: &str) -> Self {
            let guard = crate::llm::retry::tests::RETRY_ENV_LOCK
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            std::env::set_var("LLM_MAX_RETRIES", max_attempts);
            std::env::set_var("LLM_RETRY_BASE_MS", "1");
            std::env::set_var("LLM_RETRY_MAX_DELAY_MS", "5");
            Self(guard)
        }
    }
    impl Drop for RetryEnv {
        fn drop(&mut self) {
            std::env::remove_var("LLM_MAX_RETRIES");
            std::env::remove_var("LLM_RETRY_BASE_MS");
            std::env::remove_var("LLM_RETRY_MAX_DELAY_MS");
        }
    }

    #[tokio::test]
    async fn one_acquire_per_http_attempt() {
        let _env = RetryEnv::set("4");
        let (addr, hits) = scripted_mock(vec![
            (500, serde_json::json!("down")),
            (500, serde_json::json!("down")),
            (500, serde_json::json!("down")),
            (200, billed_body("gpt-4o-mini", 0.00002)),
        ])
        .await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let gate = RecordingGate::new();
        let resp = provider
            .complete_gated(req("gpt-4o-mini", None, ReasoningSetting::Off), &gate)
            .await
            .expect("fourth attempt succeeds");
        assert_eq!(resp.content, "ok");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(*gate.reserves.lock().unwrap(), vec![1, 2, 3, 4]);
        let settled: Vec<u32> = gate.settled.lock().unwrap().iter().map(|(n, _)| *n).collect();
        assert_eq!(settled, vec![1, 2, 3, 4]);
        let outcomes = gate.outcomes();
        assert!(outcomes[..3].iter().all(|o| *o == SendOutcome::NotGenerated));
        assert!(matches!(
            &outcomes[3],
            SendOutcome::Billed { reported_cost_usd: Some(c), generation_id: Some(g), .. }
                if (*c - 0.00002).abs() < 1e-12 && g == "gen-1"
        ));
    }

    #[tokio::test]
    async fn denied_reserve_sends_nothing_and_stops() {
        let _env = RetryEnv::set("4");
        let (addr, hits) = scripted_mock(vec![(500, serde_json::json!("down"))]).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let mut gate = RecordingGate::new();
        gate.deny_on = Some(2);
        let err = provider
            .complete_gated(req("gpt-4o-mini", None, ReasoningSetting::Off), &gate)
            .await
            .expect_err("the deny ends the call");
        assert!(matches!(
            err,
            LlmError::Managed(send_gate::Deny::Hold(send_gate::HoldCode::DailyBudgetExhausted))
        ));
        assert!(!err.is_transient());
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(*gate.reserves.lock().unwrap(), vec![1, 2]);
        assert_eq!(gate.outcomes(), vec![SendOutcome::NotGenerated]);
    }

    async fn one_attempt(provider: &OpenAiProvider) -> (Result<CompletionResponse, LlmError>, Vec<SendOutcome>) {
        let gate = RecordingGate::new();
        let result = provider
            .complete_gated(req("gpt-4o-mini", None, ReasoningSetting::Off), &gate)
            .await;
        (result, gate.outcomes())
    }

    #[tokio::test]
    async fn outcome_classification_table() {
        let _env = RetryEnv::set("1");
        let ambiguous = |g: Option<&str>| SendOutcome::Ambiguous { generation_id: g.map(str::to_string) };

        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_addr = closed.local_addr().unwrap();
        drop(closed);
        let provider = OpenAiProvider::new(reqwest::Client::new(), "k")
            .with_base_url(format!("http://{closed_addr}"));
        let (result, outcomes) = one_attempt(&provider).await;
        assert!(matches!(result, Err(LlmError::Upstream { status: 0, .. })));
        assert_eq!(outcomes, vec![SendOutcome::NotGenerated], "connect failure");

        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                "late"
            }),
        );
        let addr = spawn_mock(app).await;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(100))
            .build()
            .unwrap();
        let provider = OpenAiProvider::new(client, "k").with_base_url(format!("http://{addr}"));
        let (result, outcomes) = one_attempt(&provider).await;
        assert!(matches!(result, Err(LlmError::Timeout)));
        assert_eq!(outcomes, vec![ambiguous(None)], "timeout before headers");

        let cases: Vec<(u16, serde_json::Value, SendOutcome, &str)> = vec![
            (408, serde_json::json!("slow"), SendOutcome::NotGenerated, "408"),
            (429, serde_json::json!("busy"), SendOutcome::NotGenerated, "429"),
            (401, serde_json::json!("key"), SendOutcome::NotGenerated, "401"),
            (403, serde_json::json!("key"), SendOutcome::NotGenerated, "403"),
            (
                404,
                serde_json::json!({"error":{"message":"No endpoints found that satisfy the max price for this request","code":404}}),
                SendOutcome::NotGenerated,
                "404 max_price",
            ),
            (502, serde_json::json!("bad gateway"), SendOutcome::NotGenerated, "5xx"),
            (200, serde_json::json!("{not json"), ambiguous(None), "200 undecodable"),
            (
                200,
                serde_json::json!({"id":"gen-err","error":{"message":"upstream died","code":502}}),
                ambiguous(Some("gen-err")),
                "200 error-only",
            ),
            (
                200,
                serde_json::json!({"id":"gen-none","choices":[{"message":{"content":"x"},"finish_reason":"stop"}],"model":"gpt-4o-mini"}),
                ambiguous(Some("gen-none")),
                "200 without usage",
            ),
        ];
        for (status, body, expected, label) in cases {
            let (addr, hits) = scripted_mock(vec![(status, body)]).await;
            let provider =
                OpenAiProvider::new(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
            let (result, outcomes) = one_attempt(&provider).await;
            assert!(result.is_err(), "{label}");
            assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1, "{label}");
            assert_eq!(outcomes, vec![expected], "{label}");
        }

        let billed_cases = vec![
            (billed_body("gpt-4o-mini", 0.0001), true, "ok"),
            (
                serde_json::json!({"choices":[{"message":{"content":""},"finish_reason":"error"}],"usage":{"prompt_tokens":7,"completion_tokens":0,"cost":0.0001},"model":"z-ai/glm-5.3-flash"}),
                false,
                "finish_reason error",
            ),
            (
                serde_json::json!({"error":{"message":"cut off","code":500},"usage":{"prompt_tokens":7,"completion_tokens":2,"cost":0.0001},"model":"gpt-4o-mini"}),
                false,
                "error object with usage",
            ),
            (
                serde_json::json!({"choices":[{"message":{"content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2,"cost":0.0001}}),
                false,
                "missing model",
            ),
        ];
        for (body, ok, label) in billed_cases {
            let (addr, _) = scripted_mock(vec![(200, body)]).await;
            let provider =
                OpenAiProvider::new(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
            let (result, outcomes) = one_attempt(&provider).await;
            assert_eq!(result.is_ok(), ok, "{label}");
            assert_eq!(outcomes.len(), 1, "{label}");
            match &outcomes[0] {
                SendOutcome::Billed { reported_cost_usd, served_model, .. } => {
                    assert_eq!(*reported_cost_usd, Some(0.0001), "{label}");
                    assert!(!served_model.is_empty(), "{label}");
                }
                other => panic!("{label}: expected Billed, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn body_carries_terms_models_and_max_price() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        let _env = RetryEnv::set("1");
        let (addr, captured) = spawn_capturing_mock(billed_body("deepseek/deepseek-v4-flash-0731", 0.0001)).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k")
            .with_base_url(format!("http://{addr}"));

        let mut gate = RecordingGate::new();
        gate.terms = Some(SendTerms {
            models: vec!["deepseek/deepseek-v4-flash-0731".into(), "z-ai/glm-5.3-flash".into()],
            max_price: Some(send_gate::MaxPrice { prompt: 0.16, completion: 0.5 }),
        });
        provider
            .complete_gated(req("ignored/by-terms", None, ReasoningSetting::Off), &gate)
            .await
            .unwrap();
        let (body, _) = captured.lock().unwrap().take().unwrap();
        assert_eq!(body["model"], "deepseek/deepseek-v4-flash-0731");
        assert_eq!(body["models"], serde_json::json!(["z-ai/glm-5.3-flash"]));
        assert_eq!(
            body["provider"]["max_price"],
            serde_json::json!({ "prompt": 0.16, "completion": 0.5, "request": 0 })
        );
        assert_eq!(body["provider"]["data_collection"], "deny");
        assert_eq!(
            body["reasoning"],
            serde_json::json!({ "effort": "low" }),
            "the GLM fallback cannot turn reasoning off, so the priced body uses its encoding"
        );

        provider
            .complete(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off))
            .await
            .unwrap();
        let (body, _) = captured.lock().unwrap().take().unwrap();
        assert!(body["provider"].get("max_price").is_none(), "unmanaged sends no price ceiling");
        assert_eq!(body["reasoning"], serde_json::json!({ "enabled": false }), "unmanaged keeps the primary's encoding");
        assert_eq!(body["models"], serde_json::json!(["z-ai/glm-5.3-flash"]));
        assert_eq!(
            candidate_models("deepseek/deepseek-v4-flash-0731", None),
            vec!["deepseek/deepseek-v4-flash-0731".to_string(), "z-ai/glm-5.3-flash".to_string()]
        );
    }

    #[tokio::test]
    async fn extraction_terms_with_v41_fallback_turn_reasoning_off() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        let _env = RetryEnv::set("1");
        let (addr, captured) = spawn_capturing_mock(billed_body("deepseek/deepseek-v4-flash-0731", 0.0001)).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k")
            .with_base_url(format!("http://{addr}"));
        let models = candidate_models(
            "deepseek/deepseek-v4-flash-0731",
            Some("deepseek/deepseek-v4.1-flash"),
        );
        assert_eq!(
            models,
            vec!["deepseek/deepseek-v4-flash-0731".to_string(), "deepseek/deepseek-v4.1-flash".to_string()]
        );
        let mut gate = RecordingGate::new();
        gate.terms = Some(SendTerms {
            models,
            max_price: Some(send_gate::MaxPrice { prompt: 0.22, completion: 0.68 }),
        });
        provider
            .complete_gated(req("ignored/by-terms", None, ReasoningSetting::Off), &gate)
            .await
            .unwrap();
        let (body, _) = captured.lock().unwrap().take().unwrap();
        assert_eq!(body["model"], "deepseek/deepseek-v4-flash-0731");
        assert_eq!(body["models"], serde_json::json!(["deepseek/deepseek-v4.1-flash"]));
        assert_eq!(
            body["reasoning"],
            serde_json::json!({ "enabled": false }),
            "no candidate needs mandatory reasoning, so Off is really off"
        );
    }

    #[test]
    fn v41_flash_can_turn_reasoning_off_as_primary_or_fallback() {
        assert!(!requires_mandatory_reasoning("deepseek/deepseek-v4.1-flash"));
        assert_eq!(
            reasoning_json("deepseek/deepseek-v4.1-flash", ReasoningSetting::Off),
            serde_json::json!({ "enabled": false })
        );
        let v41_primary = vec!["deepseek/deepseek-v4.1-flash".to_string(), "deepseek/deepseek-v4-flash-0731".to_string()];
        assert_eq!(
            reasoning_json_for(&v41_primary, "deepseek/deepseek-v4.1-flash", ReasoningSetting::Off),
            serde_json::json!({ "enabled": false })
        );
        // The kill switch's GLM value restores today's floor.
        let glm = candidate_models("deepseek/deepseek-v4-flash-0731", Some("z-ai/glm-5.3-flash"));
        assert_eq!(
            reasoning_json_for(&glm, "deepseek/deepseek-v4-flash-0731", ReasoningSetting::Off),
            serde_json::json!({ "effort": "low" })
        );
    }

    #[test]
    fn candidate_override_never_names_the_primary_or_a_blank() {
        let _guard = FALLBACK_ENV_LOCK.lock().unwrap();
        std::env::remove_var("OPENROUTER_FALLBACK_MODEL_JSON");
        let default = candidate_models("deepseek/deepseek-v4-flash-0731", None);
        assert_eq!(candidate_models("deepseek/deepseek-v4-flash-0731", None), default);
        assert_eq!(candidate_models("deepseek/deepseek-v4-flash-0731", Some("  ")), default);
        assert_eq!(
            candidate_models("deepseek/deepseek-v4-flash-0731", Some("deepseek/deepseek-v4-flash-0731")),
            default
        );
        assert_eq!(
            candidate_models("deepseek/deepseek-v4.1-flash", Some(" deepseek/deepseek-v4.1-flash ")),
            vec!["deepseek/deepseek-v4.1-flash".to_string()],
            "v4.1 as primary has no global fallback, so naming itself leaves it alone"
        );
    }

    #[tokio::test]
    async fn stream_attempts_reach_the_attempt_log() {
        use axum::response::IntoResponse;
        use futures::StreamExt;
        let _env = RetryEnv::set("4");
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter = hits.clone();
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move || {
                let counter = counter.clone();
                async move {
                    if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "busy").into_response();
                    }
                    let sse = concat!(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"model\":\"gpt-4o-mini\"}\n\n",
                        "data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"cost\":0.00003},\"choices\":[]}\n\n",
                        "data: [DONE]\n\n",
                    );
                    ([("content-type", "text/event-stream")], sse).into_response()
                }
            }),
        );
        let addr = spawn_mock(app).await;
        let provider =
            OpenAiProvider::new(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let ((), log) = send_gate::with_attempt_log(async {
            let mut stream = provider
                .stream_complete(req("gpt-4o-mini", None, ReasoningSetting::Off))
                .await
                .unwrap();
            while let Some(item) = stream.next().await {
                item.unwrap();
            }
        })
        .await;
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].outcome, SendOutcome::NotGenerated);
        assert_eq!(log[1].outcome.reported_cost_usd(), Some(0.00003));
    }

    // ── Transport policy: streamed reads, attempt bounds, actual-cost settle ──

    /// One SSE event line as OpenRouter frames it.
    fn sse_data(value: serde_json::Value) -> String {
        format!("data: {value}\n\n")
    }

    /// A stand-in that answers every completion with `events` (delay in ms,
    /// raw text) as an event stream, records each request body, and serves
    /// `generation` (or a 404) at `/v1/generation`.
    async fn streaming_mock(
        events: Vec<(u64, String)>,
        generation: Option<serde_json::Value>,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        std::sync::Arc<std::sync::atomic::AtomicU32>,
    ) {
        use axum::response::IntoResponse;
        let bodies = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let lookups = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let seen = bodies.clone();
        let looked = lookups.clone();
        let app = axum::Router::new()
            .route(
                "/v1/chat/completions",
                axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let seen = seen.clone();
                    let events = events.clone();
                    async move {
                        seen.lock().unwrap().push(body);
                        let stream = async_stream::stream! {
                            for (ms, text) in events {
                                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                                yield Ok::<_, std::convert::Infallible>(Bytes::from(text));
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
                    let looked = looked.clone();
                    let generation = generation.clone();
                    async move {
                        looked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        match generation {
                            Some(record) => axum::Json(record).into_response(),
                            None => axum::http::StatusCode::NOT_FOUND.into_response(),
                        }
                    }
                }),
            );
        (spawn_mock(app).await, bodies, lookups)
    }

    fn bounded(attempt_ms: u64, stall_ms: u64) -> TransportPolicy {
        TransportPolicy {
            attempt_timeout: Some(std::time::Duration::from_millis(attempt_ms)),
            stream_stall: Some(std::time::Duration::from_millis(stall_ms)),
            deadline: None,
            provider_sort: Some("throughput"),
        }
    }

    /// A forced-tool answer streamed in fragments over ~800 ms.
    fn slow_tool_stream() -> Vec<(u64, String)> {
        let chunk = |delta: serde_json::Value| {
            sse_data(serde_json::json!({
                "id": "gen-slow", "model": "deepseek/deepseek-v4-flash-0731", "provider": "Parasail",
                "choices": [{"index": 0, "delta": delta, "finish_reason": null}]
            }))
        };
        vec![
            (0, ": OPENROUTER PROCESSING\n\n".to_string()),
            (200, chunk(serde_json::json!({"tool_calls": [{"index": 0, "id": "call_1", "type": "function",
                "function": {"name": "emit", "arguments": "{\"a\":"}}]}))),
            (200, chunk(serde_json::json!({"tool_calls": [{"index": 0, "function": {"arguments": "1}"}}]}))),
            (200, sse_data(serde_json::json!({"id": "gen-slow", "model": "deepseek/deepseek-v4-flash-0731",
                "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}))),
            (100, sse_data(serde_json::json!({"id": "gen-slow", "model": "deepseek/deepseek-v4-flash-0731",
                "provider": "Parasail", "choices": [],
                "usage": {"prompt_tokens": 900, "completion_tokens": 40, "cost": 0.00021}}))),
            (0, "data: [DONE]\n\n".to_string()),
        ]
    }

    fn short_client() -> reqwest::Client {
        // The production default is a 60 s TOTAL deadline; scaled down here.
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(300))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn a_slow_streamed_answer_outlives_the_client_deadline_and_is_billed_once() {
        let _env = RetryEnv::set("4");
        let (addr, bodies, _) = streaming_mock(slow_tool_stream(), None).await;
        let provider = OpenAiProvider::openrouter(short_client(), "k").with_base_url(format!("http://{addr}"));

        // Before: the client's total deadline cuts the answer mid-generation.
        // It was sent, so it is charged in full, and it is not repeated.
        let gate = RecordingGate::new();
        let err = provider
            .complete_gated(req("deepseek/deepseek-v4-flash-0731", Some(tool_schema()), ReasoningSetting::Off), &gate)
            .await
            .unwrap_err();
        assert!(matches!(err, LlmError::Timeout), "a body read past the deadline is a timeout, not a parse error: {err:?}");
        assert_eq!(gate.outcomes(), vec![SendOutcome::Ambiguous { generation_id: None }]);
        assert_eq!(*gate.reserves.lock().unwrap(), vec![1], "a cut answer is never re-sent by the transport");

        // After: a bounded, streamed attempt reads the whole answer.
        let mut gate = RecordingGate::new();
        gate.policy = bounded(5_000, 2_000);
        let resp = provider
            .complete_gated(req("deepseek/deepseek-v4-flash-0731", Some(tool_schema()), ReasoningSetting::Off), &gate)
            .await
            .expect("slow but healthy answer is used");
        assert_eq!(resp.content, r#"{"a":1}"#);
        assert_eq!(resp.stop_reason.as_deref(), Some("tool_use"));
        assert_eq!((resp.input_tokens, resp.output_tokens), (900, 40));
        assert_eq!(resp.cost_usd, Some(0.00021));
        assert_eq!(*gate.reserves.lock().unwrap(), vec![1]);
        let outcomes = gate.outcomes();
        assert_eq!(outcomes.len(), 1, "billed exactly once");
        assert!(matches!(&outcomes[0],
            SendOutcome::Billed { reported_cost_usd: Some(c), generation_id: Some(g), upstream: Some(u), .. }
                if (*c - 0.00021).abs() < 1e-12 && g == "gen-slow" && u == "Parasail"));

        let body = bodies.lock().unwrap().last().cloned().unwrap();
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
        assert_eq!(body["provider"]["sort"], "throughput");
        // Sorting never loosens the data policy.
        assert_eq!(body["provider"]["zdr"], true);
        assert_eq!(body["provider"]["data_collection"], "deny");
        assert_eq!(body["provider"]["allow_fallbacks"], true);
        assert_eq!(body["model"], "deepseek/deepseek-v4-flash-0731");
        // The unbounded call sent no sort and no stream.
        let first = bodies.lock().unwrap()[0].clone();
        assert!(first.get("stream").is_none() && first["provider"].get("sort").is_none());
    }

    fn hung_stream() -> Vec<(u64, String)> {
        vec![
            (0, sse_data(serde_json::json!({"id": "gen-hung", "model": "deepseek/deepseek-v4-flash-0731",
                "provider": "Near AI", "choices": [{"index": 0, "delta": {"content": "{\"atoms\":["}}]}))),
            // Keep-alives only from here on: not progress.
            (100, ": OPENROUTER PROCESSING\n\n".to_string()),
            (100, ": OPENROUTER PROCESSING\n\n".to_string()),
            (30_000, "data: [DONE]\n\n".to_string()),
        ]
    }

    #[tokio::test]
    async fn a_hung_stream_is_cut_on_stall_and_settled_at_the_providers_cost() {
        let _env = RetryEnv::set("4");
        let record = serde_json::json!({"data": {"id": "gen-hung", "model": "deepseek/deepseek-v4-flash-20260731",
            "provider_name": "Near AI", "total_cost": 0.00009, "cancelled": true,
            "native_tokens_prompt": 900, "native_tokens_completion": 12}});
        let (addr, bodies, lookups) = streaming_mock(hung_stream(), Some(record)).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let mut gate = RecordingGate::new();
        gate.policy = bounded(10_000, 400);
        let started = std::time::Instant::now();
        let err = provider
            .complete_gated(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off), &gate)
            .await
            .unwrap_err();
        assert!(matches!(err, LlmError::Timeout), "{err:?}");
        assert!(started.elapsed() < std::time::Duration::from_secs(3), "stall bound, not the attempt bound");
        assert_eq!(bodies.lock().unwrap().len(), 1, "a cut attempt is never repeated");
        assert!(lookups.load(std::sync::atomic::Ordering::SeqCst) >= 1);
        assert_eq!(gate.outcomes(), vec![SendOutcome::Billed {
            reported_cost_usd: Some(0.00009),
            input_tokens: 900,
            output_tokens: 12,
            served_model: "deepseek/deepseek-v4-flash-20260731".into(),
            upstream: Some("Near AI".into()),
            generation_id: Some("gen-hung".into()),
        }], "settled once, at the provider's figure, not the reservation");
    }

    #[tokio::test]
    async fn a_cut_without_a_priced_record_stays_ambiguous_with_its_generation_id() {
        let _env = RetryEnv::set("4");
        let (addr, _, lookups) = streaming_mock(hung_stream(), None).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let mut gate = RecordingGate::new();
        gate.policy = bounded(600, 5_000);
        let err = provider
            .complete_gated(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off), &gate)
            .await
            .unwrap_err();
        assert!(matches!(err, LlmError::Timeout));
        assert_eq!(lookups.load(std::sync::atomic::Ordering::SeqCst) as usize, GENERATION_LOOKUP_BACKOFF.len());
        assert_eq!(gate.outcomes(), vec![SendOutcome::Ambiguous { generation_id: Some("gen-hung".into()) }]);

        // An unmanaged caller never pays for a lookup.
        let (addr, _, lookups) = streaming_mock(hung_stream(), None).await;
        let provider = OpenAiProvider::openrouter(short_client(), "k").with_base_url(format!("http://{addr}"));
        let gate = RecordingGate::new();
        let _ = provider.complete_gated(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off), &gate).await;
        assert_eq!(lookups.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_passed_deadline_sends_nothing_and_reserves_nothing() {
        let _env = RetryEnv::set("4");
        let (addr, bodies, _) = streaming_mock(slow_tool_stream(), None).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let mut gate = RecordingGate::new();
        gate.policy = TransportPolicy {
            deadline: Some(std::time::Instant::now()),
            ..bounded(5_000, 2_000)
        };
        let err = provider
            .complete_gated(req("deepseek/deepseek-v4-flash-0731", None, ReasoningSetting::Off), &gate)
            .await
            .unwrap_err();
        assert!(matches!(err, LlmError::Timeout));
        assert!(gate.reserves.lock().unwrap().is_empty());
        assert!(gate.outcomes().is_empty());
        assert!(bodies.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_bounded_call_still_retries_a_fast_refusal_but_not_past_its_deadline() {
        let _env = RetryEnv::set("4");
        let (addr, hits) = scripted_mock(vec![
            (502, serde_json::json!("bad gateway")),
            (200, billed_body("gpt-4o-mini", 0.00002)),
        ])
        .await;
        let provider = OpenAiProvider::new(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let mut gate = RecordingGate::new();
        gate.policy = bounded(5_000, 2_000);
        let resp = provider
            .complete_gated(req("gpt-4o-mini", None, ReasoningSetting::Off), &gate)
            .await
            .expect("a JSON answer to a streamed request is read as a body");
        assert_eq!(resp.content, "ok");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(gate.outcomes()[0], SendOutcome::NotGenerated);
    }

    #[test]
    fn the_event_reader_joins_fragments_across_chunk_boundaries() {
        let mut acc = SseAccumulator::default();
        let text = concat!(
            ": OPENROUTER PROCESSING\n\n",
            "data: {\"id\":\"gen-x\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"k\\\":\"}}]}}]}\n\n",
            "data: {\"id\":\"gen-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"v\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: {\"id\":\"gen-x\",\"choices\":null,\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"cost\":0.1}}\n\n",
            "data: [DONE]\n\n",
        )
        .as_bytes();
        // A keep-alive alone is not progress.
        assert!(!acc.feed(&text[..25]).ok().unwrap());
        // Split mid-line, and mid-UTF-8 would be inside a line too.
        for piece in text[25..].chunks(7) {
            acc.feed(piece).ok().unwrap();
        }
        assert!(acc.done);
        let body = acc.finish();
        assert_eq!(body.id.as_deref(), Some("gen-x"));
        assert_eq!(body.usage.as_ref().map(|u| u.completion_tokens), Some(2));
        let choice = body.choices.into_iter().next().unwrap();
        assert_eq!(choice.finish_reason.as_deref(), Some("tool_calls"));
        assert_eq!(extract_openai_content(choice, true), r#"{"k":"v"}"#);

        let mut failed = SseAccumulator::default();
        failed
            .feed(b"data: {\"id\":\"gen-e\",\"error\":{\"message\":\"upstream died\",\"code\":502}}\n\n")
            .ok()
            .unwrap();
        assert!(failed.done, "an error event ends the stream");
        let body = failed.finish();
        assert!(body.error.is_some() && body.usage.is_none());
        assert_eq!(body.id.as_deref(), Some("gen-e"));
    }

    #[test]
    fn a_generation_record_without_a_cost_is_not_a_bill() {
        let data = |v: serde_json::Value| serde_json::from_value::<GenerationData>(v).unwrap();
        assert!(data(serde_json::json!({"model": "m"})).billed("g").is_none());
        assert!(data(serde_json::json!({"model": "m", "total_cost": -1.0})).billed("g").is_none());
        assert!(data(serde_json::json!({"total_cost": 0.1})).billed("g").is_none(), "no served model");
        assert!(
            data(serde_json::json!({"model": "m", "total_cost": 0.0, "tokens_prompt": 4})).billed("g").is_none(),
            "a zero-cost record of a sent, cut attempt is unpriced, not a $0 bill"
        );
        assert!(matches!(
            data(serde_json::json!({"model": "m", "total_cost": 0.00009, "tokens_prompt": 4})).billed("g"),
            Some(SendOutcome::Billed { reported_cost_usd: Some(c), input_tokens: 4, .. }) if c == 0.00009
        ));
    }

    #[tokio::test]
    async fn a_slow_generation_lookup_is_bounded_and_settles_ambiguous() {
        let _env = RetryEnv::set("4");
        // A `/generation` endpoint that answers each lookup only after the
        // per-request timeout: unbounded, the lookup ran every request to it.
        let app = axum::Router::new().route(
            "/v1/generation",
            axum::routing::get(|| async {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                "late"
            }),
        );
        let addr = spawn_mock(app).await;
        let provider = OpenAiProvider::openrouter(reqwest::Client::new(), "k").with_base_url(format!("http://{addr}"));
        let bound = AttemptBound::from_policy(&bounded(5_000, 2_000));
        let started = std::time::Instant::now();
        let outcome = provider
            .resolve_ambiguous(&bound, SendOutcome::Ambiguous { generation_id: Some("gen-slow-record".into()) })
            .await;
        assert_eq!(outcome, SendOutcome::Ambiguous { generation_id: Some("gen-slow-record".into()) });
        assert!(
            // Fixed, not derived from the budget: unbounded, three 5 s
            // request timeouts ran 15 s here.
            started.elapsed() < std::time::Duration::from_secs(3),
            "the whole lookup is bounded: {:?}",
            started.elapsed()
        );
    }
}
