pub mod model_config;
pub mod openai;
mod retry;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
pub mod usage;

pub use coralswarm_core::send_gate::{self, Deny, HoldCode, RefuseCode, SendGate};

use futures::StreamExt as _;
use serde::{Deserialize, Serialize};

// ── Upstream-host side channel (OpenRouter retry fix, 2026-09-07) ───────────
//
// OpenRouter names the ACTUAL inference host that served (or attempted to
// serve) a request in a top-level `provider` field, on both the
// non-streaming body and every SSE chunk — see `openai::OpenAiResponse`/
// `openai::StreamChunk`. Measured 2026-09-07: `z-ai/glm-5.3-flash` failed
// 16% of calls over 7 days, every one shaped as an HTTP 200 whose body
// carried `choices[0].finish_reason == "error"` with no top-level `error`
// object — and until this fix, nothing recorded WHICH host produced that
// failure, so "which upstream host is unreliable" was unanswerable from
// `llm_usage` alone.
//
// This can't ride on [`LlmError`] itself: `LlmError::Upstream` is
// struct-literal-constructed and exhaustively pattern-matched at ~20 sites
// across this codebase (`llm::openai`,
// `workers::summary`, and every one of their test modules), and this fix's
// own remit requires the error a caller sees on final exhaustion to stay
// BYTE-FOR-BYTE identical to what shipped before it (same variant, same
// message) — adding a field there would break that guarantee at every one
// of those sites for no benefit to callers that never touch OpenRouter.
//
// It also can't be a plain field on `OpenAiProvider`: that struct is a
// single, `Arc`-shared, long-lived instance reused across many CONCURRENT
// requests (built once at startup for connection-pool reuse — see its own
// doc), so "the last provider this instance observed" would race across
// unrelated in-flight calls and could hand one request another's answer.
//
// A `tokio::task_local!` is the correct shape instead: it is scoped to the
// ONE async task driving ONE logical call end-to-end, so concurrent calls
// can never see each other's value regardless of how many retries either
// one is doing internally. [`usage::metered_complete`]/
// [`usage::metered_complete_via_stream`] open a fresh `.scope()` around each
// `llm.complete()`/`complete_via_stream()` call and read the value back
// (via [`take_upstream_provider`]) before that scope's future resolves, so
// it is available for BOTH the success and the exhausted-error `finalize()`
// call — see each function's own doc. Outside a scope (every non-OpenRouter
// provider, and every call site that predates this fix), both
// [`report_upstream_provider`] and [`take_upstream_provider`] are silent,
// harmless no-ops — `try_with` simply finds no active scope.
tokio::task_local! {
    pub(crate) static UPSTREAM_PROVIDER: std::cell::RefCell<Option<String>>;
}

/// Record `provider` into the current [`UPSTREAM_PROVIDER`] scope, if this
/// task is running inside one. Called by [`openai::OpenAiProvider::complete`]/
/// `stream_complete` on every attempt (successful or failed) that got far
/// enough to parse a body, so the value always reflects the MOST RECENT
/// attempt — exactly the one whose outcome the caller is about to observe.
/// A silent no-op when `provider` is `None` (nothing observed this attempt,
/// e.g. a raw network failure with no body at all) or when there is no
/// active scope (every non-`OpenAiProvider` call path).
pub(crate) fn report_upstream_provider(provider: Option<&str>) {
    if let Some(p) = provider {
        let _ = UPSTREAM_PROVIDER.try_with(|cell| *cell.borrow_mut() = Some(p.to_string()));
    }
}

/// Take (and clear) whatever [`report_upstream_provider`] most recently
/// recorded in the CURRENT scope. `None` outside a scope, or if nothing was
/// ever reported inside it (e.g. every attempt failed before a body was
/// ever parsed).
pub(crate) fn take_upstream_provider() -> Option<String> {
    UPSTREAM_PROVIDER
        .try_with(|cell| cell.borrow_mut().take())
        .unwrap_or(None)
}

tokio::task_local! {
    /// OpenRouter `provider.sort` for calls made inside [`with_provider_sort`]
    /// whose send gate sets none (scoped Ask's unmanaged answer call, see
    /// [`ask_answer_provider_sort`]).
    static PROVIDER_SORT: Option<&'static str>;
}

/// Runs `fut` with `sort` as the host order of every OpenRouter call it makes
/// whose gate sets no order. Same-task only, like the other call task-locals.
pub async fn with_provider_sort<F: std::future::Future>(sort: Option<&'static str>, fut: F) -> F::Output {
    PROVIDER_SORT.scope(sort, fut).await
}

pub(crate) fn provider_sort_hint() -> Option<&'static str> {
    PROVIDER_SORT.try_with(|sort| *sort).ok().flatten()
}

/// `ASK_PROVIDER_SORT`: the OpenRouter host order for scoped Ask's answer
/// call. `throughput` (default), `latency` or `price`; anything else turns
/// sorting off. An ordering only: `zdr`, `data_collection: deny`, the
/// quantization floor and the model pins still bound the pool.
///
/// Replayed 2026-10-20 on quality-eval Ask prompts (GLM 5.3 Flash, 60 per
/// arm): unsorted routing put a fifth of calls on hosts with a 10-15 s
/// median and gave p50 3.4 s / p95 19.4 s; `throughput` gave 2.6 s / 16.7 s
/// and `latency` 2.2 s / 20.8 s. Jev's DeepSeek calls showed no gain from
/// sorting, so they stay unsorted.
pub fn ask_answer_provider_sort() -> Option<&'static str> {
    parse_ask_provider_sort(std::env::var("ASK_PROVIDER_SORT").ok().as_deref())
}

pub(crate) fn parse_ask_provider_sort(raw: Option<&str>) -> Option<&'static str> {
    let Some(raw) = raw else { return Some("throughput") };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "throughput" => Some("throughput"),
        "latency" => Some("latency"),
        "price" => Some("price"),
        _ => None,
    }
}

// Per-logical-call metadata, isolated from concurrent provider calls. Deliberately
// contains no prompts, provider bodies, or error messages.
#[derive(Default, Clone, Serialize)]
pub(crate) struct CallDiagnostics {
    pub phase: Option<&'static str>,
    pub attempt_count: u32,
    pub completion_reason: Option<&'static str>,
    pub error_class: Option<&'static str>,
    pub http_status: Option<u16>,
    pub elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extraction: Option<ExtractionDiagnostics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AtomRejection {
    Schema,
    Text,
    QuoteBounds,
    QuoteMissing,
    QuoteAmbiguous,
    EntityLimit,
    EntityUnsupported,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExtractionEnvelopeError {
    Schema,
    Truncated,
    /// The response had atom candidates and every one was rejected.
    NoValidAtoms,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct ExtractionDiagnostics {
    pub accepted_atoms: u32,
    pub duplicate_atoms: u32,
    pub whitespace_matches: u32,
    pub rejected_atoms: std::collections::BTreeMap<AtomRejection, u32>,
    pub envelope_error: Option<ExtractionEnvelopeError>,
    /// Entity mentions dropped from atoms that were kept, by reason. An
    /// unsupported mention costs only itself, never its atom.
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub dropped_entities: std::collections::BTreeMap<AtomRejection, u32>,
    /// Atom candidates past the per-window cap, not kept.
    #[serde(skip_serializing_if = "is_zero")]
    pub capped_atoms: u32,
    /// Set on the second call of a window: the category that made the worker
    /// retry it on a smaller window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_retry: Option<&'static str>,
    /// The `atoms` field came as a JSON string holding the array (parsed).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub stringified_atoms: bool,
    /// Where a schema error was found: the field path of the envelope
    /// (`atoms[3].entities[0].type`), field names and indices only, never
    /// content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_error_path: Option<String>,
    /// The response used its whole output-token budget while its stop
    /// reason said otherwise: read as truncated.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub silent_truncation: bool,
    /// v3, on a window's retry: every atom was rejected for a quote the
    /// window does not contain (or contains twice), so the window finished
    /// with no atoms instead of failing its version.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub unanchorable_window: bool,
    /// Source-atoms v2 only: accepted/dropped relations and dropped validity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relations: Option<crate::workers::source_atoms::v2::RelationDiagnostics>,
}

pub(crate) fn report_extraction_diagnostics(diagnostics: ExtractionDiagnostics) {
    let _ = CALL_DIAGNOSTICS.try_with(|cell| cell.borrow_mut().extraction = Some(diagnostics));
}

tokio::task_local! {
    pub(crate) static CALL_DIAGNOSTICS: std::cell::RefCell<CallDiagnostics>;
}

pub(crate) fn report_attempt(attempt: u32) {
    let _ = CALL_DIAGNOSTICS.try_with(|cell| cell.borrow_mut().attempt_count = attempt);
}

pub(crate) fn report_completion_reason(reason: &'static str) {
    let _ = CALL_DIAGNOSTICS.try_with(|cell| cell.borrow_mut().completion_reason = Some(reason));
}

pub(crate) fn current_diagnostics() -> Option<CallDiagnostics> {
    CALL_DIAGNOSTICS.try_with(|cell| cell.borrow().clone()).ok()
}

pub(crate) fn error_metadata(error: &LlmError) -> (&'static str, Option<u16>) {
    match error {
        LlmError::RateLimit(_) => ("rate_limit", Some(429)),
        LlmError::Auth(_) => ("auth", None),
        LlmError::ModelNotFound(_) => ("model_not_found", None),
        LlmError::Timeout => ("timeout", None),
        LlmError::Upstream { status, .. } => ("upstream", (*status != 0).then_some(*status)),
        LlmError::Parse(_) => ("parse", None),
        LlmError::Managed(_) => ("managed_denied", None),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
}

/// An optional JSON-Schema-typed tool the provider can be forced to call so its
/// reply is guaranteed to be schema-valid JSON (audit H3). When set on a
/// `CompletionRequest`, the Anthropic provider sends a single `tools` entry with
/// this `schema` as its `input_schema` and a `tool_choice` forcing that tool,
/// then surfaces the tool call's `input` (serialized) as the response `content`
/// so callers `serde_json::from_str` it exactly as before. Providers that do not
/// support forced tool use ignore this field and behave unchanged — callers keep
/// their lenient free-text JSON parse as a fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSchema {
    #[serde(default)]
    pub strict: bool,
    /// Tool name advertised to the model (e.g. `"emit_entities"`).
    pub name: String,
    /// The JSON Schema for the tool's input — the shape the model must return.
    pub schema: serde_json::Value,
}

/// Explicit reasoning-mode request, threaded on every [`CompletionRequest`]
/// (OpenRouter provider port, PR1). Every background/auxiliary call site
/// sets this EXPLICITLY (see each consumer's own comment) rather than
/// relying on the `#[default]` below, so a future default change can never
/// silently flip a cost-sensitive extraction call onto a paid reasoning
/// tier.
///
/// [`crate::llm::openai::OpenAiProvider`] serializes this on every request
/// (see that module's `reasoning_json`). Distinct from
/// [`CompletionRequest::effort`], a leftover ladder from the Anthropic
/// client that OpenRouter does not send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningSetting {
    /// No reasoning/thinking tokens — the default for every background job.
    /// `OpenAiProvider` serializes this as `"reasoning": {"enabled": false}`.
    #[default]
    Off,
    Low,
    Medium,
    High,
}

impl ReasoningSetting {
    /// Derive a [`ReasoningSetting`] from the four-rung effort string
    /// (`None`/`"off"`/`"low"`/`"medium"`/`"high"`/`"xhigh"`/`"max"`)
    /// used by the two synthesis prose call sites
    /// ([`crate::synthesis::synthesize`], [`crate::synthesis::synthesize_stream`])
    /// so a caller who already resolved an effort level doesn't have to
    /// separately decide a reasoning level for OpenRouter.
    /// `xhigh`/`max` both collapse to [`ReasoningSetting::High`]. Matching is
    /// case-insensitive and trims whitespace, mirroring
    /// [`model_config::resolve_effort`]'s own normalization;
    /// anything else unrecognized falls back to [`ReasoningSetting::Off`]
    /// (fail closed on cost, never on the strongest/most expensive tier).
    pub fn from_effort_str(effort: Option<&str>) -> Self {
        match effort.map(|e| e.trim().to_ascii_lowercase()).as_deref() {
            None | Some("off") | Some("") => ReasoningSetting::Off,
            Some("low") => ReasoningSetting::Low,
            Some("medium") => ReasoningSetting::Medium,
            Some("high") | Some("xhigh") | Some("max") => ReasoningSetting::High,
            Some(_) => ReasoningSetting::Off,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompletionRequest {
    pub messages: Vec<Message>,
    pub model: String,
    pub max_tokens: u32,
    pub system: Option<String>,
    /// Optional forced-tool-use schema (audit H3). `None` (the default) ⇒
    /// existing free-text behavior, byte-for-byte unchanged for every provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_schema: Option<ToolSchema>,
    /// Optional reasoning-effort level, already validated by
    /// [`model_config::resolve_effort`] — never a raw, unvalidated client
    /// string, and always one of `low`/`medium`/`high` when `Some` (two-model-
    /// world amendment, 2026-09-03: that resolver's own client-facing dial is
    /// four rungs, `off`/`low`/`medium`/`high`, but `off` collapses to `None`
    /// before it ever reaches this field, and the still-accepted legacy
    /// `xhigh`/`max` requests collapse to `"high"`). OpenRouter reads
    /// [`CompletionRequest::reasoning`] instead; this field is kept so in-flight
    /// callers that already resolved an effort string do not have to change.
    /// `None` (the default) ⇒ existing behavior, byte-for-byte unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// Explicit reasoning-mode request — see [`ReasoningSetting`]'s own doc.
    /// `#[default] Off` covers every pre-existing call site this struct's
    /// literal-with-`..Default::default()` pattern already used, but every
    /// listed background/auxiliary call site (OpenRouter provider port,
    /// PR1) still sets it explicitly rather than relying on that default.
    #[serde(default)]
    pub reasoning: ReasoningSetting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionResponse {
    pub content: String,
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub model: String,
    /// Tokens written to the provider's prompt cache on this call (a cache MISS
    /// that primed the cache for a subsequent hit). `0` when the provider
    /// doesn't populate cache stats for this response — an Anthropic/OpenAI
    /// model or request that simply didn't write to cache this call.
    pub cache_creation_input_tokens: u32,
    /// Tokens served FROM the provider's prompt cache on this call (a cache
    /// HIT, typically billed at a fraction of the normal input-token rate).
    /// `0` under the same 0-default semantics as
    /// [`cache_creation_input_tokens`](Self::cache_creation_input_tokens): no
    /// caching concept for this provider, or this call simply didn't hit cache.
    pub cache_read_input_tokens: u32,
    /// Why the model stopped generating — Anthropic's `stop_reason`
    /// (`"end_turn"`, `"max_tokens"`, `"stop_sequence"`, `"tool_use"`, ...).
    /// `None` for providers that don't surface it (OpenAI as wired
    /// today) or for a response parsed before this field existed.
    ///
    /// Production regression (2026-08): synthesis and the digest worker never
    /// read this, so a response truncated at `max_tokens` surfaced only as a
    /// downstream "JSON parse error: EOF while parsing..." — indistinguishable
    /// from the model simply emitting malformed JSON. Callers use this field
    /// to tell the two apart and retry accordingly (see
    /// `synthesis::synthesize`'s truncation-aware retry).
    pub stop_reason: Option<String>,
    /// Provider-reported cost in USD for this exact call, when the provider
    /// actually reports one (today: nothing — Anthropic never
    /// reports cost; a future OpenRouter integration via its `usage.include`
    /// request flag would populate this on the OpenAI-compatible provider).
    /// `None` means "unknown," NOT "free" — deliberately never defaulted to
    /// `0.0`. A `0.0` here would read as "this call cost nothing" to any
    /// downstream billing consumer (PR9's derivation worker prefers this
    /// field over `model_rates` table math when it IS present), silently
    /// zeroing out real spend for every call a provider doesn't itself price.
    /// See the usage-metering plan, PR3 negative control.
    pub cost_usd: Option<f64>,
}

/// Anthropic's `stop_reason` value for a response cut off by the `max_tokens`
/// cap. The one string literal every truncation check compares against —
/// factored out (CodeRabbit, PR #452) so `synthesis::synthesize`, the digest
/// worker, and the `create_query_stream` SSE path (which compares a bare
/// `Option<&str>` pulled off [`StreamFinalEvent::stop_reason`] rather than a
/// whole [`CompletionResponse`]) can't drift on the exact sentinel value.
pub const STOP_REASON_MAX_TOKENS: &str = "max_tokens";

/// Whether `resp` was cut off by the provider's `max_tokens` cap (Anthropic's
/// `stop_reason: "max_tokens"`) rather than a normal stop. Centralizes the
/// comparison (CodeRabbit, PR #452) so the truncation-aware retry in
/// `synthesis::synthesize` and the digest worker (`workers::digest::regenerate`)
/// share one definition of "truncated" instead of two independent string
/// comparisons.
pub fn is_truncated(resp: &CompletionResponse) -> bool {
    resp.stop_reason.as_deref() == Some(STOP_REASON_MAX_TOKENS)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDelta {
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamFinalEvent {
    pub input_tokens: u32,
    pub output_tokens: u32,
    /// See [`CompletionResponse::cache_creation_input_tokens`].
    pub cache_creation_input_tokens: u32,
    /// See [`CompletionResponse::cache_read_input_tokens`].
    pub cache_read_input_tokens: u32,
    /// See [`CompletionResponse::stop_reason`]. Carried on the SSE
    /// `message_delta` event's `delta.stop_reason` — see
    /// `anthropic::SseEvent`.
    pub stop_reason: Option<String>,
    /// The model that actually SERVED this streamed call, when the
    /// provider's terminal frame names one (Anthropic's `message_start`
    /// carries `message.model`; OpenAI/OpenRouter chunks carry `model`).
    /// `None` when the provider's stream protocol doesn't surface a served
    /// model at all — callers MUST fall back to the requested model at the
    /// point they record it (see [`complete_via_stream`]), never paper over
    /// a `None` with an empty string. Before this field existed,
    /// `complete_via_stream` (and every direct `StreamEvent::Final`
    /// consumer) had no way to tell a served model apart from the requested
    /// one, so a provider serving a different model than asked for would be
    /// mispriced under the wrong rate for every such row — see the
    /// usage-metering plan, PR3.
    pub model: Option<String>,
    /// See [`CompletionResponse::cost_usd`] — same "`None` means unknown,
    /// never `0.0`" contract on the streaming path.
    pub cost_usd: Option<f64>,
}

#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    #[error("rate limited: {0}")]
    RateLimit(String),
    #[error("auth error: {0}")]
    Auth(String),
    #[error("model not found: {0}")]
    ModelNotFound(String),
    #[error("timeout")]
    Timeout,
    #[error("upstream error {status}: {body}")]
    Upstream { status: u16, body: String },
    #[error("parse error: {0}")]
    Parse(String),
    /// The managed send gate refused an attempt before anything was sent.
    #[error("managed send denied: {0}")]
    Managed(Deny),
}

impl LlmError {
    /// Whether this error is *transient* — a retry (here, by re-queuing the job)
    /// has a reasonable chance of succeeding. Audit S2.
    ///
    /// Transient: rate limits (429), request timeouts, network-level failures
    /// (`Upstream { status: 0 }`), and upstream 5xx including Anthropic's `529
    /// overloaded`. NOT transient: auth failures, model-not-found, and parse
    /// errors of a 200 body — those are genuine bad input/config that a retry
    /// would just repeat.
    pub fn is_transient(&self) -> bool {
        match self {
            LlmError::RateLimit(_) => true,
            LlmError::Timeout => true,
            LlmError::Upstream { status, .. } => *status == 0 || *status >= 500,
            LlmError::Auth(_)
            | LlmError::ModelNotFound(_)
            | LlmError::Parse(_)
            | LlmError::Managed(_) => false,
        }
    }
}

/// Per-account LLM configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    pub mode: LlmMode,
    pub provider: Option<LlmProviderKind>,
    /// Reference to a stored API key (not the key itself).
    pub api_key_ref: Option<String>,
    pub model_override: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LlmMode {
    Managed,
    Byok,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LlmProviderKind {
    Anthropic,
    OpenAI,
}

/// Trait for LLM providers.
#[async_trait::async_trait]
pub trait LlmProvider: Send + Sync {
    /// Non-streaming completion.
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError>;

    /// Streaming completion. Yields deltas, then a final usage event.
    /// The stream yields `Ok(StreamEvent)` items.
    async fn stream_complete(
        &self,
        req: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    >;

    /// Human-readable provider identifier persisted to `llm_usage.provider`
    /// for cost attribution (OpenRouter provider port, PR1) — real values
    /// are `"anthropic"`, `"openrouter"`, `"openai"`. Defaults to
    /// `"unknown"` so the many test-only mock `LlmProvider` implementations
    /// scattered across this codebase (and `backend/tests/*.rs`) don't all
    /// need a matching update just to keep compiling — every REAL provider
    /// (`OpenAiProvider`) overrides this.
    fn name(&self) -> &'static str {
        "unknown"
    }

    /// The gated transport behind this provider. A provider or wrapper that
    /// does not expose one cannot serve a managed call: it is refused, never
    /// silently sent ungated.
    fn gated(&self) -> Option<&dyn GatedLlm> {
        None
    }
}

mod private {
    pub trait Sealed {}
}

/// A transport that takes one `send_gate::acquire` per HTTP attempt, consumed
/// by that attempt's send. Sealed to `crate::llm`, so no wrapper outside it
/// can rebind or drop the gate.
#[async_trait::async_trait]
pub trait GatedLlm: private::Sealed + Send + Sync {
    async fn complete_gated(
        &self,
        req: CompletionRequest,
        gate: &dyn SendGate,
    ) -> Result<CompletionResponse, LlmError>;
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Delta(StreamDelta),
    Final(StreamFinalEvent),
}

// ── Escaping the non-streaming total deadline (production regression, 2026-08) ─
//
// `LlmProvider::complete()`'s HTTP client carries a *total* deadline
// (`LLM_TIMEOUT_SECS`, default 60s) — the right bound for a genuinely short
// call, but wrong for a long synthesis or digest generation on a Sonnet-class
// model, which routinely outruns it. PR #418 already fixed this for the
// STREAMING path: `stream_complete()` runs on a stall-bounded client
// (`LLM_STREAM_STALL_SECS`, `crate::llm_stream_client`) with no total
// deadline at all, so a slow-but-healthy generation is never cut off. This
// helper reuses that same stream for a call site that is logically
// non-streaming (it wants one `CompletionResponse`, not incremental deltas),
// escaping the total deadline without weakening the stall protection.
//
// Deliberately NOT fixed by adding `read_timeout` to the *non-streaming*
// client instead: a non-streaming Anthropic response is silent — zero
// bytes — until the entire generation finishes, so a per-read stall timeout
// on that client would fire on every slow-but-healthy request, not just
// genuinely dead ones. The streaming client only works as a stall bound
// because bytes trickle in continuously; that property doesn't hold here.

/// Perform `req` via the provider's [`LlmProvider::stream_complete`],
/// accumulate the text deltas, and return the same [`CompletionResponse`]
/// shape [`LlmProvider::complete`] would have — including [`stop_reason`]
/// from the terminal [`StreamEvent::Final`] event — so callers can swap one
/// for the other without changing anything downstream.
///
/// [`stop_reason`]: CompletionResponse::stop_reason
///
/// `model` records the SERVED model when the terminal [`StreamFinalEvent`]
/// names one ([`StreamFinalEvent::model`]), falling back to `req.model` (the
/// REQUESTED model) only when the frame carries none. Before PR3 this was
/// unconditionally `req.model` — the streaming protocol's `Final` event had
/// no model field at all, so `synthesis::synthesize`,
/// `synthesis::synthesize_stream`, and `workers::digest` all recorded the
/// requested model even when a provider actually served a different one,
/// mispricing every such row (usage-metering plan, PR3 / §4 item 4). The
/// fallback is resolved HERE, at the one place both values are in scope, so
/// no downstream caller has to remember to do it itself.
///
/// [`CompletionResponse::cost_usd`] is threaded straight through from the
/// terminal frame's [`StreamFinalEvent::cost_usd`] — `None` unless the
/// provider actually reported one; never coerced to `0.0`.
///
/// Returns [`LlmError::Parse`] if the stream ends without ever yielding a
/// `Final` event (a malformed or prematurely closed stream) — the same
/// "unusable response" classification a JSON parse failure on the
/// non-streaming path gets.
pub async fn complete_via_stream(
    llm: &dyn LlmProvider,
    req: CompletionRequest,
) -> Result<CompletionResponse, LlmError> {
    complete_via_stream_with_delta(llm, req, &mut |_| {}).await
}

/// [`complete_via_stream`] plus a per-delta hook. Used by MCP `ask_ocean` to
/// forward synthesis tokens while the tool call is still open. The
/// accumulated `CompletionResponse` is identical to [`complete_via_stream`].
pub async fn complete_via_stream_with_delta<F>(
    llm: &dyn LlmProvider,
    req: CompletionRequest,
    on_delta: &mut F,
) -> Result<CompletionResponse, LlmError>
where
    F: FnMut(&str) + Send,
{
    let requested_model = req.model.clone();
    let mut stream = llm.stream_complete(req).await?;

    let mut content = String::new();
    let mut final_event: Option<StreamFinalEvent> = None;
    while let Some(item) = stream.next().await {
        match item? {
            StreamEvent::Delta(d) => {
                on_delta(&d.text);
                content.push_str(&d.text);
            }
            StreamEvent::Final(f) => final_event = Some(f),
        }
    }

    let f = final_event.ok_or_else(|| {
        LlmError::Parse("stream ended without a final usage event".to_string())
    })?;

    Ok(CompletionResponse {
        content,
        input_tokens: f.input_tokens,
        output_tokens: f.output_tokens,
        // Served model preferred; requested model is the fallback, never an
        // empty string — see this function's doc comment.
        model: f.model.unwrap_or(requested_model),
        cache_creation_input_tokens: f.cache_creation_input_tokens,
        cache_read_input_tokens: f.cache_read_input_tokens,
        stop_reason: f.stop_reason,
        cost_usd: f.cost_usd,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ask_answer_sort_defaults_to_throughput_and_can_be_turned_off() {
        assert_eq!(parse_ask_provider_sort(None), Some("throughput"));
        assert_eq!(parse_ask_provider_sort(Some(" ")), Some("throughput"));
        assert_eq!(parse_ask_provider_sort(Some("Latency")), Some("latency"));
        assert_eq!(parse_ask_provider_sort(Some("price")), Some("price"));
        assert_eq!(parse_ask_provider_sort(Some("off")), None);
        assert_eq!(parse_ask_provider_sort(Some("nitro")), None, "an unknown value is never sent");
    }

    #[test]
    fn llm_mode_serializes_correctly() {
        assert_eq!(
            serde_json::to_string(&LlmMode::Managed).unwrap(),
            r#""managed""#
        );
        assert_eq!(
            serde_json::to_string(&LlmMode::Byok).unwrap(),
            r#""byok""#
        );
        assert_eq!(
            serde_json::to_string(&LlmMode::Local).unwrap(),
            r#""local""#
        );
    }

    #[test]
    fn llm_provider_kind_serializes_correctly() {
        assert_eq!(
            serde_json::to_string(&LlmProviderKind::Anthropic).unwrap(),
            r#""anthropic""#
        );
        assert_eq!(
            serde_json::to_string(&LlmProviderKind::OpenAI).unwrap(),
            r#""openai""#
        );
    }

    #[test]
    fn llm_error_display_variants() {
        let e = LlmError::RateLimit("retry after 60s".to_string());
        assert!(e.to_string().contains("rate limited"));

        let e = LlmError::Auth("invalid key".to_string());
        assert!(e.to_string().contains("auth error"));

        let e = LlmError::ModelNotFound("gpt-99".to_string());
        assert!(e.to_string().contains("model not found"));

        let e = LlmError::Timeout;
        assert_eq!(e.to_string(), "timeout");

        let e = LlmError::Upstream {
            status: 500,
            body: "internal server error".to_string(),
        };
        assert!(e.to_string().contains("500"));

        let e = LlmError::Parse("unexpected token".to_string());
        assert!(e.to_string().contains("parse error"));
    }

    #[test]
    fn is_transient_classifies_errors() {
        assert!(LlmError::RateLimit("429".into()).is_transient());
        assert!(LlmError::Timeout.is_transient());
        assert!(LlmError::Upstream { status: 0, body: "network".into() }.is_transient());
        assert!(LlmError::Upstream { status: 500, body: "x".into() }.is_transient());
        assert!(LlmError::Upstream { status: 529, body: "overloaded".into() }.is_transient());
        // Permanent / bad-input errors are not retried.
        assert!(!LlmError::Auth("bad key".into()).is_transient());
        assert!(!LlmError::ModelNotFound("gpt-99".into()).is_transient());
        assert!(!LlmError::Parse("bad json".into()).is_transient());
        assert!(!LlmError::Upstream { status: 400, body: "bad req".into() }.is_transient());
    }

    // ── is_truncated (CodeRabbit, PR #452) ──────────────────────────────────────

    fn resp_with_stop_reason(stop_reason: Option<&str>) -> CompletionResponse {
        CompletionResponse {
            content: String::new(),
            input_tokens: 0,
            output_tokens: 0,
            model: "mock".to_string(),
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: stop_reason.map(str::to_string),
            cost_usd: None,
        }
    }

    #[test]
    fn is_truncated_true_only_for_max_tokens() {
        assert!(is_truncated(&resp_with_stop_reason(Some("max_tokens"))));
        assert!(!is_truncated(&resp_with_stop_reason(Some("end_turn"))));
        assert!(!is_truncated(&resp_with_stop_reason(Some("stop_sequence"))));
        assert!(!is_truncated(&resp_with_stop_reason(Some("tool_use"))));
        assert!(!is_truncated(&resp_with_stop_reason(None)));
    }

    // ── complete_via_stream (production regression 2026-08) ────────────────────

    fn req() -> CompletionRequest {
        CompletionRequest {
            messages: vec![Message {
                role: Role::User,
                content: "hi".into(),
            }],
            model: "mock-model".into(),
            max_tokens: 64,
            system: None,
            tool_schema: None,
            effort: None,
            reasoning: ReasoningSetting::Off,
        }
    }

    // ── ReasoningSetting::from_effort_str (OpenRouter provider port, PR1) ──────

    #[test]
    fn reasoning_setting_defaults_to_off() {
        assert_eq!(ReasoningSetting::default(), ReasoningSetting::Off);
    }

    #[test]
    fn from_effort_str_maps_known_levels_case_insensitively_and_trims() {
        assert_eq!(ReasoningSetting::from_effort_str(None), ReasoningSetting::Off);
        assert_eq!(ReasoningSetting::from_effort_str(Some("off")), ReasoningSetting::Off);
        assert_eq!(ReasoningSetting::from_effort_str(Some("OFF")), ReasoningSetting::Off);
        assert_eq!(ReasoningSetting::from_effort_str(Some("")), ReasoningSetting::Off);
        assert_eq!(ReasoningSetting::from_effort_str(Some("low")), ReasoningSetting::Low);
        assert_eq!(ReasoningSetting::from_effort_str(Some("  LOW  ")), ReasoningSetting::Low);
        assert_eq!(ReasoningSetting::from_effort_str(Some("medium")), ReasoningSetting::Medium);
        assert_eq!(ReasoningSetting::from_effort_str(Some("high")), ReasoningSetting::High);
    }

    #[test]
    fn from_effort_str_collapses_xhigh_and_max_to_high() {
        assert_eq!(ReasoningSetting::from_effort_str(Some("xhigh")), ReasoningSetting::High);
        assert_eq!(ReasoningSetting::from_effort_str(Some("max")), ReasoningSetting::High);
    }

    #[test]
    fn from_effort_str_falls_back_to_off_for_unrecognized_values() {
        assert_eq!(
            ReasoningSetting::from_effort_str(Some("extreme")),
            ReasoningSetting::Off,
            "an unrecognized value must fail closed to Off, never silently escalate cost"
        );
        assert_eq!(ReasoningSetting::from_effort_str(Some("gpt-4o; DROP TABLE")), ReasoningSetting::Off);
    }

    #[tokio::test]
    async fn complete_via_stream_accumulates_deltas_and_carries_stop_reason() {
        struct Provider;
        #[async_trait::async_trait]
        impl LlmProvider for Provider {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, LlmError> {
                unimplemented!("not exercised here")
            }
            async fn stream_complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<
                std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>,
                >,
                LlmError,
            > {
                let events: Vec<Result<StreamEvent, LlmError>> = vec![
                    Ok(StreamEvent::Delta(StreamDelta { text: "Hello, ".into() })),
                    Ok(StreamEvent::Delta(StreamDelta { text: "world.".into() })),
                    Ok(StreamEvent::Final(StreamFinalEvent {
                        input_tokens: 12,
                        output_tokens: 34,
                        cache_creation_input_tokens: 1,
                        cache_read_input_tokens: 2,
                        stop_reason: Some("end_turn".to_string()),
                        // No served model on this frame ⇒ falls back to the
                        // requested model (see the dedicated fallback test
                        // below).
                        model: None,
                        cost_usd: None,
                    })),
                ];
                Ok(Box::pin(futures::stream::iter(events)))
            }
        }

        let mut seen = String::new();
        let resp = complete_via_stream_with_delta(&Provider, req(), &mut |d| seen.push_str(d))
            .await
            .expect("should assemble a CompletionResponse from the stream");
        assert_eq!(seen, "Hello, world.");
        assert_eq!(resp.content, "Hello, world.");
        assert_eq!(resp.input_tokens, 12);
        assert_eq!(resp.output_tokens, 34);
        assert_eq!(resp.model, "mock-model");
        assert_eq!(resp.cache_creation_input_tokens, 1);
        assert_eq!(resp.cache_read_input_tokens, 2);
        assert_eq!(resp.stop_reason.as_deref(), Some("end_turn"));
        assert_eq!(resp.cost_usd, None);
    }

    // ── PR3: served model + provider cost on the streaming path ────────────────

    /// THE headline `Done + verified` assertion for PR3: a stream whose
    /// terminal frame names a DIFFERENT model than the one requested must
    /// record the SERVED model, not the requested one. Before PR3,
    /// `StreamFinalEvent` had no `model` field at all, so `complete_via_stream`
    /// unconditionally echoed back `req.model` — a provider silently serving a
    /// different model than requested would have every such row mispriced
    /// under the wrong rate (usage-metering plan §4 item 4).
    #[tokio::test]
    async fn complete_via_stream_records_served_model_when_different_from_requested() {
        struct Provider;
        #[async_trait::async_trait]
        impl LlmProvider for Provider {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, LlmError> {
                unimplemented!("not exercised here")
            }
            async fn stream_complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<
                std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>,
                >,
                LlmError,
            > {
                let events: Vec<Result<StreamEvent, LlmError>> = vec![
                    Ok(StreamEvent::Delta(StreamDelta { text: "hi".into() })),
                    Ok(StreamEvent::Final(StreamFinalEvent {
                        input_tokens: 5,
                        output_tokens: 2,
                        cache_creation_input_tokens: 0,
                        cache_read_input_tokens: 0,
                        stop_reason: Some("end_turn".to_string()),
                        // The provider served a DIFFERENT model than the one
                        // this request asked for (`req().model ==
                        // "mock-model"`).
                        model: Some("mock-model-served-variant".to_string()),
                        cost_usd: None,
                    })),
                ];
                Ok(Box::pin(futures::stream::iter(events)))
            }
        }

        let resp = complete_via_stream(&Provider, req())
            .await
            .expect("should assemble a CompletionResponse from the stream");
        assert_eq!(
            resp.model, "mock-model-served-variant",
            "must record the SERVED model, not the requested one"
        );
    }

    /// The other half: when the terminal frame carries NO served model at
    /// all (every provider today except where explicitly wired), the
    /// requested model is the fallback — never an empty string.
    #[tokio::test]
    async fn complete_via_stream_falls_back_to_requested_model_when_frame_carries_none() {
        struct Provider;
        #[async_trait::async_trait]
        impl LlmProvider for Provider {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, LlmError> {
                unimplemented!("not exercised here")
            }
            async fn stream_complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<
                std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>,
                >,
                LlmError,
            > {
                let events: Vec<Result<StreamEvent, LlmError>> = vec![Ok(StreamEvent::Final(
                    StreamFinalEvent {
                        input_tokens: 5,
                        output_tokens: 2,
                        cache_creation_input_tokens: 0,
                        cache_read_input_tokens: 0,
                        stop_reason: Some("end_turn".to_string()),
                        model: None,
                        cost_usd: None,
                    },
                ))];
                Ok(Box::pin(futures::stream::iter(events)))
            }
        }

        let resp = complete_via_stream(&Provider, req())
            .await
            .expect("should assemble a CompletionResponse from the stream");
        assert_eq!(resp.model, "mock-model", "must fall back to req.model, not empty");
    }

    /// PR3 negative control: a provider that reports no cost leaves
    /// `cost_usd` as `None` on the assembled `CompletionResponse` — it must
    /// NEVER default to `0.0`, which downstream would read as "this call was
    /// free" and silently zero out real spend. See this module's
    /// `CompletionResponse::cost_usd` doc comment.
    #[tokio::test]
    async fn complete_via_stream_leaves_cost_usd_none_when_provider_reports_nothing() {
        struct Provider;
        #[async_trait::async_trait]
        impl LlmProvider for Provider {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, LlmError> {
                unimplemented!("not exercised here")
            }
            async fn stream_complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<
                std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>,
                >,
                LlmError,
            > {
                let events: Vec<Result<StreamEvent, LlmError>> = vec![Ok(StreamEvent::Final(
                    StreamFinalEvent {
                        input_tokens: 5,
                        output_tokens: 2,
                        cache_creation_input_tokens: 0,
                        cache_read_input_tokens: 0,
                        stop_reason: Some("end_turn".to_string()),
                        model: None,
                        cost_usd: None,
                    },
                ))];
                Ok(Box::pin(futures::stream::iter(events)))
            }
        }

        let resp = complete_via_stream(&Provider, req())
            .await
            .expect("should assemble a CompletionResponse from the stream");
        assert_eq!(
            resp.cost_usd, None,
            "absent provider cost must surface as None, never 0.0"
        );
    }

    /// The positive counterpart: when a provider DOES report cost, it passes
    /// straight through untouched.
    #[tokio::test]
    async fn complete_via_stream_passes_through_provider_reported_cost() {
        struct Provider;
        #[async_trait::async_trait]
        impl LlmProvider for Provider {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, LlmError> {
                unimplemented!("not exercised here")
            }
            async fn stream_complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<
                std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>,
                >,
                LlmError,
            > {
                let events: Vec<Result<StreamEvent, LlmError>> = vec![Ok(StreamEvent::Final(
                    StreamFinalEvent {
                        input_tokens: 5,
                        output_tokens: 2,
                        cache_creation_input_tokens: 0,
                        cache_read_input_tokens: 0,
                        stop_reason: Some("end_turn".to_string()),
                        model: None,
                        cost_usd: Some(0.00123),
                    },
                ))];
                Ok(Box::pin(futures::stream::iter(events)))
            }
        }

        let resp = complete_via_stream(&Provider, req())
            .await
            .expect("should assemble a CompletionResponse from the stream");
        assert_eq!(resp.cost_usd, Some(0.00123));
    }

    #[tokio::test]
    async fn complete_via_stream_errors_when_no_final_event_arrives() {
        struct NoFinalProvider;
        #[async_trait::async_trait]
        impl LlmProvider for NoFinalProvider {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, LlmError> {
                unimplemented!("not exercised here")
            }
            async fn stream_complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<
                std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>,
                >,
                LlmError,
            > {
                let events: Vec<Result<StreamEvent, LlmError>> =
                    vec![Ok(StreamEvent::Delta(StreamDelta { text: "orphaned".into() }))];
                Ok(Box::pin(futures::stream::iter(events)))
            }
        }

        let err = complete_via_stream(&NoFinalProvider, req())
            .await
            .expect_err("a stream with no Final event must not silently succeed");
        assert!(matches!(err, LlmError::Parse(_)));
    }

    #[tokio::test]
    async fn complete_via_stream_propagates_mid_stream_errors() {
        struct FailingProvider;
        #[async_trait::async_trait]
        impl LlmProvider for FailingProvider {
            async fn complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<CompletionResponse, LlmError> {
                unimplemented!("not exercised here")
            }
            async fn stream_complete(
                &self,
                _req: CompletionRequest,
            ) -> Result<
                std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>,
                >,
                LlmError,
            > {
                let events: Vec<Result<StreamEvent, LlmError>> = vec![
                    Ok(StreamEvent::Delta(StreamDelta { text: "partial".into() })),
                    Err(LlmError::Timeout),
                ];
                Ok(Box::pin(futures::stream::iter(events)))
            }
        }

        let err = complete_via_stream(&FailingProvider, req())
            .await
            .expect_err("a mid-stream error must propagate, not be swallowed");
        assert!(matches!(err, LlmError::Timeout));
    }
}
