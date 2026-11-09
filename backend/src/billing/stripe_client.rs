//! Outbound Stripe REST client — the transport PR D (checkout), PR F (seat
//! lifecycle), PR G (auto-refill), and PR H (billing management) consume.
//! `.scratch/billing-self-serve/plan.md` §1.8/§1.13, §2.C.
//!
//! **This PR (C) ships no routes.** Every function below is `pub` and
//! genuinely unreachable from any request path until its consuming PR lands
//! — a reviewer should not "clean up" an apparently-dead function here; it
//! is dead on purpose, the same way a library's public API has functions no
//! *current* caller in this crate uses yet. The provisioning binary
//! (`bin/provision_stripe.rs`) is the one caller PR C itself ships.
//!
//! # Posture: hand-rolled, no SDK, mirrors the inbound webhook
//!
//! [`routes::stripe_webhook`](crate::routes::stripe_webhook) hand-rolls HMAC
//! verification rather than pulling in a Stripe SDK; this client hand-rolls
//! form-encoded POSTs for the same reason (plan §1.8) — one dependency,
//! reviewable in full, instead of trusting a large generated surface for a
//! handful of endpoints. The two never call each other; they share only a
//! vocabulary and a posture:
//!
//! | Inbound (`routes::stripe_webhook`) | Outbound (here) |
//! |---|---|
//! | Hand-rolled HMAC, no SDK | Hand-rolled form POST, no SDK |
//! | Secret from env, absence is a distinct state | Key from env at boot, absence is `NotConfigured` |
//! | Missing secret ⇒ 503, nothing processed | Missing key ⇒ 503, nothing charged |
//! | At-least-once delivery ⇒ event-claim table | At-least-once retries ⇒ `Idempotency-Key` |
//! | Absence renders LOUD, never as success | `Parse` on any missing required field; no silent `Option` fallback |
//!
//! # Secret source
//!
//! `STRIPE_SECRET_KEY` is read ONCE at boot in `run()` and stored on
//! `AppState.stripe: Option<Arc<StripeClient>>` — the `llm` pattern
//! `AppState.stripe: Option<Arc<StripeClient>>` — the `llm` pattern
//! (`lib.rs`, `OPENROUTER_API_KEY` read once into `AppState.llm`), not the
//! `clerk.rs` per-call `std::env::var` pattern. The key sits on a
//! user-facing request path (checkout POST, seat quantity push); a per-call
//! read buys nothing and makes "is billing configured?" answerable only at
//! request time. Cost: a key rotation needs a process restart — acceptable,
//! and `fly secrets set` restarts the machine anyway. `AppState.stripe ==
//! None` ⇒ every call here would return [`StripeError::NotConfigured`] (in
//! practice, the *consuming route* checks `state.stripe.is_some()` before
//! ever reaching this module) ⇒ the route answers 503 and logs once. Never a
//! 200 that pretends a charge happened.
//!
//! # Idempotency — the one invariant enforced by the type system, not memory
//!
//! Stripe honours an `Idempotency-Key` header on every POST for 24h. The
//! [`Idem`] enum exists so a call site cannot forget to think about replay
//! safety:
//!
//! * [`Idem::Key`] — a caller-supplied, DETERMINISTIC key derived from the
//!   business fact (never `Uuid::new_v4()` here — a random key turns a retry
//!   into a second charge, the exact failure the header exists to prevent).
//! * [`Idem::NotIdempotent`] — declares the POST is not safe to replay.
//!   **The retry loop enforces exactly one attempt for this case, no matter
//!   what the failure was** — retrying an unkeyed `POST
//!   /v1/payment_intents` would double-charge a customer. This is this
//!   module's single most important invariant; see
//!   `a_non_idempotent_post_is_never_retried_even_on_a_server_error` in the
//!   test module below.
//! * [`Idem::NaturallyIdempotent`] — the POST is idempotent by construction
//!   (no cumulative effect from repeating it), not via Stripe's key-replay
//!   contract. Retried freely, no `Idempotency-Key` header sent at all. See
//!   [`update_webhook_endpoint`](StripeClient::update_webhook_endpoint) for
//!   the one call site that uses this today.
//!
//! `GET`/`DELETE` need no `Idem` at all — both retry freely (HTTP already
//! treats them as safe to repeat, and Stripe's `DELETE` endpoints used here
//! are all naturally idempotent: canceling an already-canceled subscription
//! item is a no-op).
//!
//! Recipes actually used by a call site are documented on that call site,
//! not centrally — see e.g. [`StripeClient::create_customer`].
//!
//! # Why every response is `serde_json::Value` at the transport layer
//!
//! `post_form`/`get`/`delete` return `Value`; the typed wrappers below parse
//! only the fields they actually consume. This mirrors how the webhook reads
//! Stripe JSON (narrow accessors, absence is LOUD) rather than modelling
//! Stripe's full object graph with `serde` derives — a derived struct over
//! an API surface this large is a maintenance liability for the same reason
//! the webhook avoided one.
//!
//! # Retry posture
//!
//! Ported (not imported) from `llm::retry`'s `RetryPolicy` — full-jitter
//! exponential backoff, `Retry-After` as a floor, `STRIPE_*`-prefixed env
//! knobs with tighter defaults (these calls sit inside a synchronous user
//! request, not a background job). See [`RetryPolicy`].
//!
//! **Known divergence from the port source:** `llm::retry::RetryPolicy::
//! backoff` still clamps the `Retry-After`
//! floor down to `max_delay` via a trailing `.min(max_delay)` — the same bug
//! fixed in this module's `backoff()` (see [`MAX_RETRY_AFTER_HONOURED`]).
//! That means the LLM retry doc comment ("a `retry_after` hint...
//! takes precedence as a lower bound") is currently false for any
//! `Retry-After` above `LLM_RETRY_MAX_DELAY_MS`. Left unfixed here
//! deliberately — this PR's scope is the Stripe client only; fixing the LLM
//! client is separate work with its own blast radius (streaming completions,
//! background synthesis jobs) and its own review.

use reqwest::Method;
use serde_json::Value;
use std::time::Duration;

// ── Idempotency ─────────────────────────────────────────────────────────────

/// Whether a POST may be retried. See this module's doc comment,
/// "Idempotency — the one invariant enforced by the type system, not memory".
#[derive(Debug, Clone, Copy)]
pub enum Idem<'a> {
    /// A deterministic key derived from the business fact being requested.
    /// Resent unchanged on every retry attempt.
    Key(&'a str),
    /// This POST is not safe to replay. Forces exactly one attempt.
    NotIdempotent,
    /// This POST is idempotent BY CONSTRUCTION — not via Stripe's
    /// `Idempotency-Key` replay contract, but because the operation itself
    /// has no cumulative effect (e.g. `POST /v1/webhook_endpoints/{id}`
    /// reconciling `enabled_events` to a fixed list: sending it twice
    /// leaves the endpoint in the same state as sending it once). Retried
    /// freely to the policy budget, exactly like `GET`/`DELETE`, and
    /// WITHOUT an `Idempotency-Key` header — a key exists to make a
    /// non-idempotent operation safe to retry; an operation that is already
    /// idempotent doesn't need one, and inventing a key recipe for it would
    /// only add a stale-key expiry footgun with no corresponding benefit.
    NaturallyIdempotent,
}

// ── Wire encoding ───────────────────────────────────────────────────────────

/// A Stripe form body. Deliberately a flat `Vec<(String, String)>` rather
/// than a serde-derived struct per endpoint: Stripe's wire format IS
/// bracket-notated key strings (`line_items[0][price]`,
/// `metadata[account_id]`), and modelling it as one makes the exact bytes we
/// send directly assertable in a unit test (see the `tests` module below). A
/// serde struct would move that correctness behind a derive nobody reads.
///
/// `reqwest`'s `.form()`/`.query()` serialise flat pair lists — Stripe's
/// nesting is pre-flattened into the key strings by the callers below, not
/// by this type. No new Cargo dependency: `reqwest 0.12`'s `.form()` pulls
/// in `serde_urlencoded` transitively, unconditionally (unlike `.json()`,
/// gated behind the `json` feature this crate already enables).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Params(Vec<(String, String)>);

impl Params {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.0.push((key.into(), value.into()));
        self
    }

    /// `{prefix}[{key}]=value` — e.g. `metadata[account_id]=…`.
    pub fn nested(&mut self, prefix: &str, key: &str, value: impl Into<String>) -> &mut Self {
        self.push(format!("{prefix}[{key}]"), value)
    }

    /// `{prefix}[{i}][{key}]=value` — e.g. `line_items[0][price]=…`.
    pub fn indexed(
        &mut self,
        prefix: &str,
        i: usize,
        key: &str,
        value: impl Into<String>,
    ) -> &mut Self {
        self.push(format!("{prefix}[{i}][{key}]"), value)
    }

    /// `{prefix}[0]=v0`, `{prefix}[1]=v1`, … — e.g. `expand[0]=line_items`.
    pub fn list(&mut self, prefix: &str, values: &[&str]) -> &mut Self {
        for (i, v) in values.iter().enumerate() {
            self.push(format!("{prefix}[{i}]"), *v);
        }
        self
    }

    /// `"true"`/`"false"` — NEVER `"1"`/`"0"`. Stripe's form decoder accepts
    /// only the literal strings `true`/`false` for a boolean field.
    pub fn bool(&mut self, key: &str, v: bool) -> &mut Self {
        self.push(key, if v { "true" } else { "false" })
    }

    pub fn as_pairs(&self) -> &[(String, String)] {
        &self.0
    }
}

// ── Error taxonomy ───────────────────────────────────────────────────────────

/// Shaped after `LlmError` (`llm/mod.rs`, `is_transient` there) and
/// `SlackError` (`oauth/slack.rs`)'s transport-vs-API split.
#[derive(Debug, thiserror::Error)]
pub enum StripeError {
    /// `STRIPE_SECRET_KEY` unset/empty. The outbound mirror of
    /// `VerifyResult::SecretNotSet` (`routes::stripe_webhook`) — routes
    /// answer 503 and charge nothing.
    #[error("Stripe is not configured")]
    NotConfigured,
    /// Transport/TLS failure. `public` is a fixed, safe constant; the real
    /// `reqwest::Error` is logged via `tracing::warn!`, never returned
    /// (CWE-209 — see `connectors/slack.rs`'s identical redaction rule).
    #[error("{public}")]
    Transport {
        public: &'static str,
        request_id: Option<String>,
    },
    #[error("Stripe request timed out")]
    Timeout,
    /// 401 — a bad or rotated key. NEVER retried.
    #[error("Stripe rejected our API key")]
    Auth { request_id: Option<String> },
    /// 429.
    #[error("Stripe rate limited us")]
    RateLimited {
        retry_after: Option<Duration>,
        request_id: Option<String>,
    },
    /// A `409`-with-`type: idempotency_error` response: the same
    /// `Idempotency-Key` was reused with a materially different request
    /// body. A bug in a key recipe, not a transient failure — NEVER
    /// retried, and an unclassified 409 in a naive retry loop would spin
    /// until the budget ran out and report a network problem instead.
    #[error("idempotency key {key:?} was reused with a different request")]
    IdempotencyConflict {
        key: String,
        request_id: Option<String>,
    },
    /// 402 `card_error`. Terminal by design — PR G's ceiling/hard-block path
    /// depends on telling this apart from a transient failure. `message` is
    /// Stripe's own `error.message`, which IS safe to surface (copy Stripe
    /// writes for end users), unlike a raw transport/parse error string.
    #[error("card declined ({code})")]
    CardDeclined {
        code: String,
        decline_code: Option<String>,
        message: String,
        request_id: Option<String>,
    },
    /// Any other non-2xx. `message` is Stripe's own `error.message`.
    #[error("Stripe API error {status}: {message}")]
    Api {
        status: u16,
        kind: String,
        code: Option<String>,
        param: Option<String>,
        message: String,
        request_id: Option<String>,
    },
    /// A 2xx response whose body did not have the shape a caller requires.
    /// LOUD, never a silent `None` — see `routes::stripe_webhook`'s "Two
    /// facts" module-doc section for why absence must not render as
    /// success in this subsystem.
    #[error("unexpected Stripe response shape: {0}")]
    Parse(String),
    /// A `lookup_key` matched only an ARCHIVED (`active=false`) price.
    /// Stripe's unique-lookup-key constraint applies to active prices only —
    /// an archived price can still hold the key, which would otherwise dead-
    /// end the "not found ⇒ safe to create" path
    /// ([`StripeClient::find_price_by_lookup_key`]) in an undiagnosed 400 on
    /// the create call. NEVER retried, and this client never auto-transfers
    /// the key (verify-don't-mutate posture — see this module's
    /// provisioning-only section).
    #[error(
        "lookup_key {lookup_key:?} is held by an ARCHIVED price ({price_id}), not an active \
         one. Stripe prices are immutable and lookup keys are unique among ACTIVE prices only — \
         either bump the lookup key to a new version (e.g. `_v2`) and re-run, or pass \
         `transfer_lookup_key=true` when creating the replacement price to move the key off the \
         archived one. This client does not do either automatically."
    )]
    ArchivedLookupKey { lookup_key: String, price_id: String },
}

impl StripeError {
    /// Whether a retry has a reasonable chance of succeeding. Same shape and
    /// intent as `LlmError::is_transient` (`llm/mod.rs`).
    pub fn is_transient(&self) -> bool {
        match self {
            StripeError::Transport { .. } | StripeError::Timeout | StripeError::RateLimited { .. } => {
                true
            }
            StripeError::Api { status, .. } => *status >= 500,
            StripeError::NotConfigured
            | StripeError::Auth { .. }
            | StripeError::IdempotencyConflict { .. }
            | StripeError::CardDeclined { .. }
            | StripeError::Parse(_)
            | StripeError::ArchivedLookupKey { .. } => false,
        }
    }

    /// Whether this is a `404` from Stripe — the provisioning bin's
    /// existence check (`GET /v1/products/{id}`) reads this rather than a
    /// dedicated `NotFound` variant, since a 404 is otherwise an ordinary
    /// `Api` error everywhere else this client is used.
    pub fn is_not_found(&self) -> bool {
        matches!(self, StripeError::Api { status: 404, .. })
    }
}

/// The real `reqwest::Error`/parse-error strings never reach a caller —
/// only these two fixed, safe constants do. CWE-209, same rule
/// `connectors/slack.rs`'s `PUBLIC_TRANSPORT_ERROR`/`PUBLIC_PARSE_ERROR`
/// state for the identical reason.
const PUBLIC_TRANSPORT_ERROR: &str = "could not reach Stripe";

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let secs: u64 = raw.trim().parse().ok()?;
    Some(Duration::from_secs(secs))
}

/// Classify a non-2xx response body into a [`StripeError`] variant. `idem_key`
/// is threaded through only to populate [`StripeError::IdempotencyConflict`]'s
/// `key` field — this function has no other use for it.
fn parse_error_body(
    status: u16,
    body_text: &str,
    request_id: Option<String>,
    idem_key: Option<&str>,
    retry_after: Option<Duration>,
) -> StripeError {
    let parsed: Option<Value> = serde_json::from_str(body_text).ok();
    let err_obj = parsed.as_ref().and_then(|v| v.get("error"));
    let kind = err_obj
        .and_then(|e| e.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let code = err_obj
        .and_then(|e| e.get("code"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let param = err_obj
        .and_then(|e| e.get("param"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let message = err_obj
        .and_then(|e| e.get("message"))
        .and_then(|v| v.as_str())
        .unwrap_or("Stripe returned an error with no message")
        .to_string();
    let decline_code = err_obj
        .and_then(|e| e.get("decline_code"))
        .and_then(|v| v.as_str())
        .map(String::from);

    if kind == "idempotency_error" {
        return StripeError::IdempotencyConflict {
            key: idem_key.unwrap_or_default().to_string(),
            request_id,
        };
    }
    if status == 402 || kind == "card_error" {
        return StripeError::CardDeclined {
            code: code.unwrap_or_default(),
            decline_code,
            message,
            request_id,
        };
    }
    if status == 401 {
        return StripeError::Auth { request_id };
    }
    if status == 429 {
        return StripeError::RateLimited {
            retry_after,
            request_id,
        };
    }
    StripeError::Api {
        status,
        kind,
        code,
        param,
        message,
        request_id,
    }
}

// ── Retry policy (ported from llm::retry, STRIPE_*-prefixed) ────────────

const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_BASE_MS: u64 = 250;
const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 8_000;

/// Absolute ceiling on how long `backoff()` will ever sleep, even to honour
/// a server-supplied `Retry-After` floor that exceeds `max_delay`. WHY a
/// second cap on top of `max_delay`: these calls sit inside a synchronous
/// user request (checkout POST, seat quantity push) — `max_delay` bounds our
/// own jittered backoff, but `Retry-After` is Stripe's number, not ours, and
/// honouring it unbounded would let a hostile or misbehaving `Retry-After`
/// value (or a very conservative one on Stripe's side) stall a user-facing
/// request for an unbounded time. Past this ceiling we do not sleep at all —
/// `execute()` fails fast with the `RateLimited` error instead, which the
/// consuming route surfaces as a retryable failure rather than a hung
/// request. 60s is comfortably above anything Stripe is documented to send
/// and comfortably below "the request already timed out anyway."
const MAX_RETRY_AFTER_HONOURED: Duration = Duration::from_secs(60);

/// Resolved retry policy, read from env once per call — see
/// `llm::retry::RetryPolicy`, which this is a line-for-line port of
/// (STRIPE_*-prefixed env knobs, tighter defaults: these calls sit inside a
/// synchronous user request, not a background synthesis job).
#[derive(Debug, Clone, Copy)]
struct RetryPolicy {
    max_attempts: u32,
    base: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    fn from_env() -> Self {
        let max_attempts = std::env::var("STRIPE_MAX_RETRIES")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .map(|n| n.max(1))
            .unwrap_or(DEFAULT_MAX_ATTEMPTS);
        let base_ms = std::env::var("STRIPE_RETRY_BASE_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_BASE_MS);
        let max_delay_ms = std::env::var("STRIPE_RETRY_MAX_DELAY_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_MAX_DELAY_MS);
        Self {
            max_attempts,
            base: Duration::from_millis(base_ms),
            max_delay: Duration::from_millis(max_delay_ms),
        }
    }

    /// `base * 2^attempt` plus full jitter, clamped to `max_delay`, with a
    /// `Retry-After` hint acting as a LOWER BOUND, never an override — a
    /// server telling us to wait at least N seconds must not be undercut by
    /// our own jitter landing below it. Unlike an earlier version of this
    /// function, the floor is honoured EVEN ABOVE `max_delay` — `max_delay`
    /// bounds our own jittered backoff, not Stripe's explicit instruction.
    /// The floor is itself bounded only by [`MAX_RETRY_AFTER_HONOURED`]: a
    /// `Retry-After` past that absolute ceiling returns `None` rather than a
    /// `Duration`, telling `execute()` to fail fast instead of blocking a
    /// synchronous user request for an unbounded time.
    fn backoff(&self, attempt: u32, retry_after: Option<Duration>) -> Option<Duration> {
        let exp = self.base.saturating_mul(1u32 << attempt.min(16));
        let capped = exp.min(self.max_delay);
        let jittered = if capped.is_zero() {
            Duration::ZERO
        } else {
            let millis = capped.as_millis() as u64;
            Duration::from_millis(rand::random::<u64>() % (millis + 1))
        };
        match retry_after {
            Some(ra) if ra > MAX_RETRY_AFTER_HONOURED => None,
            Some(ra) => Some(ra.max(jittered)),
            None => Some(jittered),
        }
    }
}

/// Which attempts a call is allowed. Internal — callers express intent via
/// [`Idem`] on [`StripeClient::post_form`]; `get`/`delete` are always
/// [`RetryMode::Always`].
enum RetryMode<'a> {
    /// Exactly one attempt, no matter what. [`Idem::NotIdempotent`].
    Never,
    /// Retried to the policy's budget, resending the SAME key every attempt.
    WithIdemKey(&'a str),
    /// Retried to the policy's budget, no idempotency key at all (`GET`/`DELETE`).
    Always,
}

// ── Client ────────────────────────────────────────────────────────────────

/// Dedicated HTTP client for outbound Stripe calls — 15s total / 5s connect.
/// Deliberately NOT `AppState.http_client` (built with a *total* timeout
/// driven by `LLM_TIMEOUT_SECS`, default 60s — an LLM-sized deadline wrong
/// for a card charge sitting inside a synchronous user request) and NOT
/// `llm_stream_client()` (a per-read stall budget with no total deadline —
/// the wrong shape for a request/response REST call). Stripe's own p99 is
/// well under 1s, so 15s total leaves ample headroom.
///
/// With `STRIPE_MAX_RETRIES=3` (default) and `STRIPE_RETRY_MAX_DELAY_MS=8000`
/// (default) the worst case across the retry loop is roughly 3×15s plus
/// backoff. **The consuming routes (PR D/F/G/H) must apply their own outer
/// budget for anything on a synchronous user path** — that is not bounded
/// here.
pub fn stripe_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .expect("failed to build Stripe HTTP client")
}

/// Thin outbound Stripe REST client. See this module's doc comment.
pub struct StripeClient {
    http: reqwest::Client,
    secret_key: String,
    base_url: String,
}

impl StripeClient {
    pub fn new(http: reqwest::Client, secret_key: impl Into<String>) -> Self {
        Self {
            http,
            secret_key: secret_key.into(),
            base_url: "https://api.stripe.com".to_string(),
        }
    }

    /// Override the API base URL. In non-test builds only `https://` is
    /// accepted — a misconfiguration is caught at startup rather than
    /// leaking the secret key to an attacker-controlled host at request
    /// time. Mirrors `OpenAiProvider::with_base_url` exactly, including
    /// the `#[cfg(not(test))]` guard that makes the local-mock-server tests
    /// in this module possible. The `e2e` build (debug-only, see `lib.rs`)
    /// also takes an `http://` base: its Stripe is an in-runner fake.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        #[cfg(not(any(test, feature = "e2e")))]
        assert!(
            url.starts_with("https://"),
            "StripeClient::with_base_url: only https:// URLs are accepted in production; got {:?}",
            url
        );
        self.base_url = url;
        self
    }

    /// Whether this client is configured against Stripe's LIVE object graph
    /// (`sk_live_…` OR `rk_live_…`) rather than test mode. Both prefix
    /// families carry a `_live_` mode marker — checking only `sk_live_`
    /// would mis-tag a restricted `rk_live_…` key's objects as `env=test`
    /// metadata while it actually mutates the live account. The provisioning
    /// bin uses this to build its `prov:{object}:{code}:{livemode}`
    /// idempotency-key recipe and its metadata `env` tag — kept here as the
    /// one definition of "is this a live key" rather than re-parsed at each
    /// call site. (The provisioning bin separately refuses restricted keys
    /// outright — see `provision_stripe::classify_secret_key` — but this
    /// method stays correct for any other caller that might be handed one.)
    pub fn livemode(&self) -> bool {
        self.secret_key.starts_with("sk_live_") || self.secret_key.starts_with("rk_live_")
    }

    // ── Transport primitives ────────────────────────────────────────────

    async fn send_once(
        &self,
        method: &Method,
        path: &str,
        params: &Params,
        idem_key: Option<&str>,
    ) -> Result<Value, (StripeError, Option<Duration>)> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self
            .http
            .request(method.clone(), &url)
            .bearer_auth(&self.secret_key);
        req = if *method == Method::GET {
            req.query(params.as_pairs())
        } else {
            req.form(params.as_pairs())
        };
        if let Some(key) = idem_key {
            req = req.header("Idempotency-Key", key);
        }

        let response = req.send().await.map_err(|e| {
            tracing::warn!(error = %e, path, "stripe: transport error");
            let err = if e.is_timeout() {
                StripeError::Timeout
            } else {
                StripeError::Transport {
                    public: PUBLIC_TRANSPORT_ERROR,
                    request_id: None,
                }
            };
            (err, None)
        })?;

        let status = response.status().as_u16();
        let request_id = response
            .headers()
            .get("Request-Id")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let retry_after = parse_retry_after(response.headers());

        if (200..300).contains(&status) {
            return response.json::<Value>().await.map_err(|e| {
                tracing::warn!(error = %e, path, status, "stripe: 2xx response body was not valid JSON");
                (
                    StripeError::Parse(format!("2xx response body was not valid JSON: {e}")),
                    None,
                )
            });
        }

        let body_text = response.text().await.unwrap_or_default();
        if status >= 400 {
            tracing::warn!(status, path, request_id = ?request_id, "stripe: non-2xx response");
        }
        let err = parse_error_body(status, &body_text, request_id, idem_key, retry_after);
        Err((err, retry_after))
    }

    async fn execute(
        &self,
        method: Method,
        path: &str,
        params: &Params,
        mode: RetryMode<'_>,
    ) -> Result<Value, StripeError> {
        let policy = RetryPolicy::from_env();
        let idem_key = match mode {
            RetryMode::WithIdemKey(k) => Some(k),
            RetryMode::Never | RetryMode::Always => None,
        };
        // The headline invariant: `Never` gets exactly one attempt no matter
        // what the failure was — see this module's doc comment.
        let max_attempts = if matches!(mode, RetryMode::Never) {
            1
        } else {
            policy.max_attempts
        };

        let mut attempt: u32 = 0;
        loop {
            match self.send_once(&method, path, params, idem_key).await {
                Ok(v) => return Ok(v),
                Err((err, retry_after)) => {
                    let last = attempt + 1 >= max_attempts;
                    if last || !err.is_transient() {
                        return Err(err);
                    }
                    let delay = match policy.backoff(attempt, retry_after) {
                        Some(d) => d,
                        None => {
                            // Retry-After exceeded MAX_RETRY_AFTER_HONOURED —
                            // fail fast rather than block a synchronous user
                            // request for an unbounded time. See that
                            // constant's doc comment.
                            tracing::warn!(
                                path,
                                error = %err,
                                retry_after_secs = retry_after.map(|d| d.as_secs()),
                                "stripe: Retry-After exceeded the retry ceiling — failing fast instead of sleeping"
                            );
                            return Err(err);
                        }
                    };
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts,
                        delay_ms = delay.as_millis() as u64,
                        path,
                        error = %err,
                        "stripe: transient failure — backing off and retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    /// `POST {path}` with `params` as a form body. Retried per [`Idem`] —
    /// see this module's doc comment for the invariant this enforces.
    pub async fn post_form(
        &self,
        path: &str,
        params: &Params,
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let mode = match idem {
            Idem::Key(k) => RetryMode::WithIdemKey(k),
            Idem::NotIdempotent => RetryMode::Never,
            Idem::NaturallyIdempotent => RetryMode::Always,
        };
        self.execute(Method::POST, path, params, mode).await
    }

    /// `GET {path}?query`. Always safe to retry.
    pub async fn get(&self, path: &str, query: &Params) -> Result<Value, StripeError> {
        self.execute(Method::GET, path, query, RetryMode::Always)
            .await
    }

    /// `DELETE {path}` with `params` as a form body. Always safe to retry —
    /// every Stripe `DELETE` this client calls is naturally idempotent
    /// (canceling an already-canceled resource is a no-op, not a second
    /// side effect).
    pub async fn delete(&self, path: &str, params: &Params) -> Result<Value, StripeError> {
        self.execute(Method::DELETE, path, params, RetryMode::Always)
            .await
    }
}

// ── Response parsing helpers — narrow, LOUD on absence ──────────────────────

fn req_str(v: &Value, key: &str) -> Result<String, StripeError> {
    v.get(key)
        .and_then(|x| x.as_str())
        .map(str::to_string)
        .ok_or_else(|| StripeError::Parse(format!("missing/invalid {key:?} field")))
}

fn req_i64(v: &Value, key: &str) -> Result<i64, StripeError> {
    v.get(key)
        .and_then(|x| x.as_i64())
        .ok_or_else(|| StripeError::Parse(format!("missing/invalid {key:?} field")))
}

fn req_bool(v: &Value, key: &str) -> Result<bool, StripeError> {
    v.get(key)
        .and_then(|x| x.as_bool())
        .ok_or_else(|| StripeError::Parse(format!("missing/invalid {key:?} field")))
}

fn opt_str(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(str::to_string)
}

// ── Typed response shapes ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripeCustomer {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutSession {
    pub id: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionItemView {
    pub id: String,
    pub price_id: String,
    pub quantity: i64,
    /// Off `items.data[].current_period_start`, NOT the subscription object
    /// itself — `routes::stripe_webhook`'s module doc documents this from
    /// live probing (the 2025 flexible `billing_mode` moved these fields).
    /// A client that read the subscription-level field would silently get
    /// nothing.
    pub current_period_start: i64,
    pub current_period_end: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionView {
    pub id: String,
    pub status: String,
    /// One entry per subscription item. Mixed seat tiers (plan §1.4) make a
    /// TWO-item subscription the normal case for a `team` account — see
    /// `testdata/subscription_two_items.json` and the parsing test that
    /// exercises it, which is what kills any `items.data[0]`-only
    /// assumption.
    pub items: Vec<SubscriptionItemView>,
    /// Stripe's `cancel_at_period_end` mirror. Absent on a payload that
    /// predates the field is read as `false`: this is a scheduling flag
    /// ("will end at renewal"), and defaulting to "not cancelling" is the
    /// direction that cannot freeze a paying account.
    pub cancel_at_period_end: bool,
    /// Unix seconds Stripe will end the subscription, when scheduled.
    pub cancel_at: Option<i64>,
    /// The subscription's own default payment method id (Stripe returns it
    /// as a bare id unless expanded; both shapes are read).
    pub default_payment_method: Option<String>,
    /// The owning customer id (bare id or expanded object).
    pub customer_id: Option<String>,
    /// Unix seconds the trial ends, while the subscription has one.
    pub trial_end: Option<i64>,
    /// The subscription's most recent invoice id (bare id or expanded
    /// object). For a trial that was just ended, the conversion invoice.
    pub latest_invoice: Option<String>,
}

/// One invoice as the trial lifecycle needs it (signup PR C′, review round
/// 2): whether it was paid, how much it took, and the PaymentIntent that
/// took it (what a refund is issued against).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvoiceView {
    pub id: String,
    /// `draft`, `open`, `paid`, `uncollectible` or `void`.
    pub status: String,
    pub amount_paid: i64,
    /// The legacy top-level `payment_intent`, or (API `basil` and later,
    /// where that field is gone) the first PAID entry of the expanded
    /// `payments` list whose payment is a PaymentIntent. `None` for an
    /// invoice paid some other way (customer balance, out of band).
    pub payment_intent: Option<String>,
}

/// Parse a retrieved invoice into an [`InvoiceView`]. `id` and `status` are
/// required; a missing `amount_paid` reads as 0.
pub fn parse_invoice_view(v: &Value) -> Result<InvoiceView, StripeError> {
    let payment_intent = reference_id_value(v.get("payment_intent")).or_else(|| {
        v.get("payments")
            .and_then(|p| p.get("data"))
            .and_then(Value::as_array)?
            .iter()
            .filter(|row| row.get("status").and_then(Value::as_str).is_none_or(|s| s == "paid"))
            .find_map(|row| {
                let payment = row.get("payment")?;
                if payment.get("type").and_then(Value::as_str).is_some_and(|t| t != "payment_intent") {
                    return None;
                }
                reference_id_value(payment.get("payment_intent"))
            })
    });
    Ok(InvoiceView {
        id: req_str(v, "id")?,
        status: req_str(v, "status")?,
        amount_paid: v.get("amount_paid").and_then(Value::as_i64).unwrap_or(0),
        payment_intent,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupIntent {
    pub id: String,
    pub client_secret: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentMethodSummary {
    pub id: String,
    /// Stripe's payment method `type`: `card`, `us_bank_account`, `link`, ...
    pub kind: String,
    /// Cards only.
    pub brand: Option<String>,
    /// Cards, bank accounts and other methods that expose one; `link` has none.
    pub last4: Option<String>,
    /// Cards only.
    pub exp_month: Option<i64>,
    pub exp_year: Option<i64>,
    /// `us_bank_account` only (e.g. `Example Bank`).
    pub bank_name: Option<String>,
    /// `link` only: the Link account's email, the one thing Stripe exposes.
    pub email: Option<String>,
}

impl PaymentMethodSummary {
    pub fn is_card(&self) -> bool {
        self.kind == "card"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvoiceSummary {
    pub id: String,
    pub number: Option<String>,
    pub status: String,
    pub amount_paid: i64,
    pub currency: String,
    pub created: i64,
    /// Stripe-hosted link — plan §1.12: "PDFs remain Stripe-hosted links",
    /// we carry the URL, never proxy the bytes.
    pub hosted_invoice_url: Option<String>,
    pub invoice_pdf: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvoiceList {
    pub data: Vec<InvoiceSummary>,
    pub has_more: bool,
}

fn parse_subscription(v: &Value) -> Result<SubscriptionView, StripeError> {
    let id = req_str(v, "id")?;
    let status = req_str(v, "status")?;
    let items_data = v
        .get("items")
        .and_then(|i| i.get("data"))
        .and_then(|d| d.as_array())
        .ok_or_else(|| StripeError::Parse("missing/invalid \"items.data\" field".into()))?;
    let items = items_data
        .iter()
        .map(|it| {
            let price_id = it
                .get("price")
                .and_then(|p| p.get("id"))
                .and_then(|x| x.as_str())
                .ok_or_else(|| {
                    StripeError::Parse("missing/invalid \"items.data[].price.id\" field".into())
                })?
                .to_string();
            Ok(SubscriptionItemView {
                id: req_str(it, "id")?,
                price_id,
                quantity: req_i64(it, "quantity")?,
                current_period_start: req_i64(it, "current_period_start")?,
                current_period_end: req_i64(it, "current_period_end")?,
            })
        })
        .collect::<Result<Vec<_>, StripeError>>()?;
    let (cancel_at_period_end, cancel_at) = parse_cancel_state(v);
    let default_payment_method = reference_id_value(v.get("default_payment_method"));
    let customer_id = reference_id_value(v.get("customer"));
    let trial_end = v.get("trial_end").and_then(Value::as_i64);
    let latest_invoice = reference_id_value(v.get("latest_invoice"));
    Ok(SubscriptionView {
        id,
        status,
        items,
        cancel_at_period_end,
        cancel_at,
        default_payment_method,
        customer_id,
        trial_end,
        latest_invoice,
    })
}

/// `(cancel_at_period_end, cancel_at)` off a Stripe subscription object.
/// Missing fields read as `(false, None)`, see [`SubscriptionView`].
pub fn parse_cancel_state(v: &Value) -> (bool, Option<i64>) {
    (
        v.get("cancel_at_period_end").and_then(Value::as_bool).unwrap_or(false),
        v.get("cancel_at").and_then(Value::as_i64),
    )
}

/// A Stripe reference field is either a bare id string or an expanded
/// object carrying `id`. Returns the id for both shapes.
fn reference_id_value(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Object(o) => o.get("id").and_then(Value::as_str).map(str::to_string),
        _ => None,
    }
}

/// Reads any payment method type. The type-specific details live under a key
/// named after the type (`card`, `us_bank_account`, `link`, ...); everything
/// but `id` and `type` is best effort, because a method with no `last4` (Link)
/// is still a method on file. A card must still carry its four fields: a card
/// payload without them is a Stripe shape this code does not understand.
fn parse_payment_method(v: &Value) -> Result<PaymentMethodSummary, StripeError> {
    let id = req_str(v, "id")?;
    let kind = opt_str(v, "type")
        // A payload without `type` is read as a card when it carries a `card`
        // object.
        .or_else(|| v.get("card").map(|_| "card".to_string()))
        .ok_or_else(|| StripeError::Parse("missing/invalid \"type\" field".into()))?;
    let details = v.get(kind.as_str());
    if kind == "card" {
        let card = details.ok_or_else(|| StripeError::Parse("missing \"card\" field".into()))?;
        return Ok(PaymentMethodSummary {
            id,
            kind,
            brand: Some(req_str(card, "brand")?),
            last4: Some(req_str(card, "last4")?),
            exp_month: Some(req_i64(card, "exp_month")?),
            exp_year: Some(req_i64(card, "exp_year")?),
            bank_name: None,
            email: None,
        });
    }
    let last4 = details.and_then(|d| opt_str(d, "last4"));
    let bank_name = details.and_then(|d| opt_str(d, "bank_name"));
    let email = details.and_then(|d| opt_str(d, "email"));
    Ok(PaymentMethodSummary { id, kind, brand: None, last4, exp_month: None, exp_year: None, bank_name, email })
}

fn parse_invoice(v: &Value) -> Result<InvoiceSummary, StripeError> {
    Ok(InvoiceSummary {
        id: req_str(v, "id")?,
        number: opt_str(v, "number"),
        status: req_str(v, "status")?,
        amount_paid: req_i64(v, "amount_paid")?,
        currency: req_str(v, "currency")?,
        created: req_i64(v, "created")?,
        hosted_invoice_url: opt_str(v, "hosted_invoice_url"),
        invoice_pdf: opt_str(v, "invoice_pdf"),
    })
}

// ── Pure param builders (Layer 1 test surface — see the `tests` module) ────
//
// Factored out of their `StripeClient` methods the same way
// `llm::openai::OpenAiProvider::build_request_body` is factored out of `complete`:
// testable without an HTTP round-trip, and the method that actually sends
// the request calls the SAME function a test exercises, so a test proves
// something about production code rather than a hand-copied restatement of
// it.

fn customer_params(account_id: &str, email: &str, name: Option<&str>, env: &str) -> Params {
    let mut p = Params::new();
    p.push("email", email);
    if let Some(n) = name {
        p.push("name", n);
    }
    p.nested("metadata", "account_id", account_id);
    p.nested("metadata", "env", env);
    p
}

/// Checkout buys exactly ONE seat (plan §1.6) — `quantity` is hardcoded to
/// `"1"`, not a parameter, so a call site cannot accidentally request more.
fn checkout_subscription_params(
    customer_id: &str,
    price_id: &str,
    account_id: &str,
    success_url: &str,
    cancel_url: &str,
    allow_promotion_codes: bool,
) -> Params {
    let mut p = Params::new();
    p.push("mode", "subscription");
    p.push("customer", customer_id);
    p.indexed("line_items", 0, "price", price_id);
    p.indexed("line_items", 0, "quantity", "1");
    p.push("success_url", success_url);
    p.push("cancel_url", cancel_url);
    p.push("client_reference_id", account_id);
    p.push("subscription_data[metadata][account_id]", account_id);
    p.bool("allow_promotion_codes", allow_promotion_codes);
    // Card only, like the top-up and pack sessions: auto-reload charges the
    // saved method off-session as a card PaymentIntent, which a bank debit
    // or Link-funded method cannot satisfy.
    p.list("payment_method_types", &["card"]);
    p
}

/// `payment_intent_data[setup_future_usage]=off_session` is what makes PR
/// G's auto-refill possible at all — a pack bought without it leaves no
/// reusable payment method. Asserted BY NAME in the test module below.
fn checkout_payment_params(
    customer_id: &str,
    price_id: &str,
    quantity: i64,
    account_id: &str,
    success_url: &str,
    cancel_url: &str,
) -> Params {
    let mut p = Params::new();
    p.push("mode", "payment");
    p.push("customer", customer_id);
    p.indexed("line_items", 0, "price", price_id);
    p.indexed("line_items", 0, "quantity", quantity.to_string());
    p.push("success_url", success_url);
    p.push("cancel_url", cancel_url);
    p.push("client_reference_id", account_id);
    p.push("payment_intent_data[metadata][account_id]", account_id);
    p.push("payment_intent_data[setup_future_usage]", "off_session");
    // Card only: a delayed method (bank debit, voucher) completes the session
    // unpaid and can fail later, after the credits were spent.
    p.list("payment_method_types", &["card"]);
    p
}

/// The self-serve team org's first Checkout (signup PR C′): `seats` of the
/// `standard` seat price, card collected up front.
///
/// With `trial_days = Some(n)` the subscription starts `trialing` and
/// Checkout charges $0: `payment_method_collection=always` makes Checkout
/// take a card anyway, and `trial_settings[end_behavior][missing_payment_method]
/// =cancel` means a trial that somehow ends with no card is cancelled rather
/// than left `past_due`. `n` is the 90-day backstop; our own worker ends the
/// trial earlier (`trial_end=now`) when the trial credits run out. With
/// `None` (the creator already had a trial) the seats are charged at once.
///
/// Card only, like every other Checkout here: the one-trial-per-card rule
/// reads the card's fingerprint, and auto-reload charges a card off-session.
/// No promotion codes: a coupon would make the conversion invoice $0.
/// `metadata[selfserve_org]` is a support breadcrumb only; the webhook
/// resolves the org through the Stripe customer, never through metadata.
pub fn org_trial_checkout_params(
    customer_id: &str,
    price_id: &str,
    seats: i64,
    account_id: &str,
    trial_days: Option<i64>,
    success_url: &str,
    cancel_url: &str,
) -> Params {
    let mut p = Params::new();
    p.push("mode", "subscription");
    p.push("customer", customer_id);
    p.indexed("line_items", 0, "price", price_id);
    p.indexed("line_items", 0, "quantity", seats.to_string());
    if trial_days.is_some() {
        // The welcome offer, shown on the Checkout page next to the seats as
        // a $0 line. Inline `price_data` (one-time, no `recurring`), so no
        // catalog object exists for it. It is NOT a subscription item: Stripe
        // puts it on the first invoice only. The webhook skips it when it
        // counts what was bought ([`parse_checkout_session_priced_line_items`]).
        p.push("line_items[1][price_data][currency]", "usd");
        p.push("line_items[1][price_data][unit_amount]", "0");
        p.push("line_items[1][price_data][product_data][name]", welcome_offer_name());
        p.push("line_items[1][quantity]", "1");
    }
    p.push("success_url", success_url);
    p.push("cancel_url", cancel_url);
    p.push("client_reference_id", account_id);
    p.push("subscription_data[metadata][account_id]", account_id);
    p.push("subscription_data[metadata][selfserve_org]", "1");
    p.push("metadata[account_id]", account_id);
    p.push("metadata[selfserve_org]", "1");
    p.push("payment_method_collection", "always");
    if let Some(days) = trial_days {
        p.push("subscription_data[trial_period_days]", days.to_string());
        p.push("subscription_data[trial_settings][end_behavior][missing_payment_method]", "cancel");
        // Stripe's own page says "N days free". The trial usually ends
        // sooner, when the credits run out, and the founder's decision is
        // that the Checkout page says so plainly, next to the button.
        p.push("custom_text[submit][message]", org_trial_submit_message(seats, days));
    }
    p.bool("allow_promotion_codes", false);
    p.list("payment_method_types", &["card"]);
    p
}

/// The line Stripe Checkout shows above the button on a trial Checkout.
/// Plain words: when the trial ends, what is charged, how to avoid it.
pub fn org_trial_submit_message(seats: i64, trial_days: i64) -> String {
    let seat_word = if seats == 1 { "seat" } else { "seats" };
    format!(
        "You won't be charged today. Your free trial ends when your team's {credits} free credits run out, \
         or after {trial_days} days, whichever comes first. Then your subscription starts automatically and \
         this card is charged for {seats} {seat_word} every month. Cancel before then in Settings > Billing \
         and you won't be charged.",
        credits = format_credits(crate::billing::org_trial::TRIAL_CREDITS),
    )
}

/// `3000` -> `"3,000"`: the trial credit grant as the customer reads it.
fn format_credits(n: i64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// The name of the $0 welcome-offer line on the trial Checkout.
pub fn welcome_offer_name() -> String {
    format!("Welcome offer: {} free credits", format_credits(crate::billing::org_trial::TRIAL_CREDITS))
}

/// `POST /v1/subscriptions/{id}` ending the trial now. Stripe invoices the
/// subscription's items and charges the default payment method at once;
/// `payment_behavior=allow_incomplete` leaves a failed charge `past_due`
/// (Stripe's dunning retries it) instead of refusing the update.
fn end_trial_now_params() -> Params {
    let mut p = Params::new();
    p.push("trial_end", "now");
    p.push("payment_behavior", "allow_incomplete");
    p
}

fn session_retrieve_query() -> Params {
    let mut q = Params::new();
    q.list("expand", &["line_items"]);
    q
}

fn subscription_item_quantity_params(quantity: i64, proration_behavior: &str) -> Params {
    let mut p = Params::new();
    p.push("quantity", quantity.to_string());
    p.push("proration_behavior", proration_behavior);
    p
}

fn subscription_item_swap_params(new_price_id: &str, proration_behavior: &str) -> Params {
    let mut p = Params::new();
    p.push("price", new_price_id);
    p.push("proration_behavior", proration_behavior);
    p
}

fn payment_intent_off_session_params(
    customer_id: &str,
    amount_cents: i64,
    payment_method: &str,
    metadata: &[(&str, &str)],
) -> Params {
    let mut p = Params::new();
    p.push("customer", customer_id);
    p.push("amount", amount_cents.to_string());
    p.push("currency", "usd");
    p.push("payment_method", payment_method);
    p.bool("off_session", true);
    p.bool("confirm", true);
    for (k, v) in metadata {
        p.nested("metadata", k, *v);
    }
    p
}

// ── Typed surface — provisioning-only (PR C) ────────────────────────────────
//
// Reachable only from `bin/provision_stripe.rs`, never from a request path.

impl StripeClient {
    /// `GET /v1/products/{id}`. We choose deterministic ids (`cs_plan_pro`),
    /// so this is an exact existence check, not a search — a 404 means "not
    /// provisioned yet" (see [`StripeError::is_not_found`]).
    pub async fn retrieve_product(&self, id: &str) -> Result<Value, StripeError> {
        self.get(&format!("/v1/products/{id}"), &Params::new()).await
    }

    /// `POST /v1/products`. Stripe permits a caller-supplied `id`.
    pub async fn create_product(
        &self,
        id: &str,
        name: &str,
        metadata: &[(&str, &str)],
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.push("id", id);
        p.push("name", name);
        for (k, v) in metadata {
            p.nested("metadata", k, *v);
        }
        self.post_form("/v1/products", &p, idem).await
    }

    /// `GET /v1/prices?lookup_keys[]=…&active=true&limit=1`. `lookup_key`s
    /// are unique per account per livemode and read-your-write consistent —
    /// deliberately NOT the Search API, which is only eventually consistent
    /// and would risk a duplicate create on a re-run right after a create.
    ///
    /// If the active-only lookup misses, this does a SECOND lookup without
    /// the `active` filter before reporting "not found" — Stripe's unique-
    /// lookup-key constraint only applies to active prices, so an archived
    /// price can be squatting on the key. Reporting a plain `None` there
    /// would read to the caller as "safe to create," and the resulting
    /// `create_price` call would dead-end in an undiagnosed 400. See
    /// [`StripeError::ArchivedLookupKey`]. Verify-don't-mutate: this never
    /// transfers the key itself, only fails loudly with the recovery option.
    pub async fn find_price_by_lookup_key(&self, lookup_key: &str) -> Result<Option<Value>, StripeError> {
        let mut q = Params::new();
        q.list("lookup_keys", &[lookup_key]);
        q.bool("active", true);
        q.push("limit", "1");
        let resp = self.get("/v1/prices", &q).await?;
        let data = resp
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| StripeError::Parse("missing/invalid \"data\" field".into()))?;
        if let Some(found) = data.first() {
            return Ok(Some(found.clone()));
        }

        // Active lookup missed — check for an archived price holding the
        // key before reporting "not found".
        let mut q_any = Params::new();
        q_any.list("lookup_keys", &[lookup_key]);
        q_any.push("limit", "1");
        let resp_any = self.get("/v1/prices", &q_any).await?;
        let data_any = resp_any
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| StripeError::Parse("missing/invalid \"data\" field".into()))?;
        match data_any.first() {
            Some(archived) => {
                let price_id = archived
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("<unknown>")
                    .to_string();
                Err(StripeError::ArchivedLookupKey {
                    lookup_key: lookup_key.to_string(),
                    price_id,
                })
            }
            None => Ok(None),
        }
    }

    /// `POST /v1/prices`. `recurring_interval` of `None` creates a one-time
    /// (pack) price; `Some(interval)` creates a `usage_type=licensed`
    /// recurring (seat/plan) price — never `metered`, since seats are
    /// priced by subscription-item `quantity`, not usage.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_price(
        &self,
        product: &str,
        unit_amount_cents: i64,
        currency: &str,
        recurring_interval: Option<&str>,
        lookup_key: &str,
        tax_behavior: &str,
        metadata: &[(&str, &str)],
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.push("product", product);
        p.push("unit_amount", unit_amount_cents.to_string());
        p.push("currency", currency);
        p.push("lookup_key", lookup_key);
        p.push("tax_behavior", tax_behavior);
        if let Some(interval) = recurring_interval {
            p.nested("recurring", "interval", interval);
            p.nested("recurring", "usage_type", "licensed");
        }
        for (k, v) in metadata {
            p.nested("metadata", k, *v);
        }
        self.post_form("/v1/prices", &p, idem).await
    }

    /// `GET /v1/webhook_endpoints?limit=100`, paginated via `has_more` /
    /// `starting_after` until Stripe reports no more pages. Stripe caps
    /// every account at 16 webhook endpoints total — well under one page —
    /// so this loop is defense-in-depth rather than a fix for an
    /// observed bug: a single `limit=100` request already covers every
    /// account today, but reading only page 1 unconditionally would silently
    /// miss a URL on a later page if that cap ever changed, and cause
    /// `ensure_webhook` to create a duplicate endpoint instead of finding
    /// the existing one.
    pub async fn list_webhook_endpoints(&self) -> Result<Vec<Value>, StripeError> {
        let mut all = Vec::new();
        let mut starting_after: Option<String> = None;
        loop {
            let mut q = Params::new();
            q.push("limit", "100");
            if let Some(cursor) = &starting_after {
                q.push("starting_after", cursor.as_str());
            }
            let resp = self.get("/v1/webhook_endpoints", &q).await?;
            let page = resp
                .get("data")
                .and_then(|d| d.as_array())
                .ok_or_else(|| StripeError::Parse("missing/invalid \"data\" field".into()))?;
            let has_more = resp
                .get("has_more")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let last_id = page.last().and_then(|v| v.get("id")).and_then(|v| v.as_str()).map(str::to_string);
            all.extend(page.iter().cloned());
            if !has_more {
                break;
            }
            starting_after = match last_id {
                Some(id) => Some(id),
                // `has_more=true` with an empty/id-less page would loop
                // forever on the same cursor — treat it as the last page
                // rather than spin.
                None => break,
            };
        }
        Ok(all)
    }

    /// `POST /v1/webhook_endpoints`. Returns the full endpoint object,
    /// including `secret` — returned ONLY on create, never again. The
    /// caller (the provisioning bin) is responsible for printing it once
    /// and never persisting it to a file or log.
    pub async fn create_webhook_endpoint(
        &self,
        url: &str,
        enabled_events: &[&str],
        api_version: &str,
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.push("url", url);
        p.list("enabled_events", enabled_events);
        p.push("api_version", api_version);
        self.post_form("/v1/webhook_endpoints", &p, idem).await
    }

    /// `POST /v1/webhook_endpoints/{id}` — endpoints ARE mutable (unlike
    /// prices), used to reconcile `enabled_events` on a re-run rather than
    /// creating a duplicate endpoint. Always [`Idem::NaturallyIdempotent`],
    /// not a caller-supplied key: setting `enabled_events` to a fixed list
    /// has no cumulative effect no matter how many times it's sent, so
    /// there is nothing for an `Idempotency-Key` to protect against here —
    /// unlike the deterministic-but-mutable-body keys elsewhere in the
    /// provisioning bin (`prov:product:…`/`prov:price:…`), this call is
    /// naturally safe to replay unkeyed.
    pub async fn update_webhook_endpoint(
        &self,
        id: &str,
        enabled_events: &[&str],
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.list("enabled_events", enabled_events);
        self.post_form(&format!("/v1/webhook_endpoints/{id}"), &p, Idem::NaturallyIdempotent)
            .await
    }
}

// ── Typed surface — PR D (checkout, plan §2.D) ──────────────────────────────

impl StripeClient {
    /// `POST /v1/customers`. `metadata[env]` is derived from
    /// [`StripeClient::livemode`], not a caller-supplied string — one
    /// definition of "which mode is this", not two that could disagree.
    ///
    /// Idempotency recipe: `cust:{account_id}` — one customer per account,
    /// forever (`stripe_subscriptions.account_id` is a PRIMARY KEY,
    /// migration `0162`, saying the same thing in the schema). This call is
    /// §1.8's pre-creation: it must run BEFORE the checkout session so the
    /// `stripe_subscriptions` mapping row exists before any webhook can
    /// arrive.
    pub async fn create_customer(
        &self,
        account_id: &str,
        email: &str,
        name: Option<&str>,
        idem: Idem<'_>,
    ) -> Result<StripeCustomer, StripeError> {
        let env = if self.livemode() { "live" } else { "test" };
        let p = customer_params(account_id, email, name, env);
        let v = self.post_form("/v1/customers", &p, idem).await?;
        Ok(StripeCustomer { id: req_str(&v, "id")? })
    }

    /// `POST /v1/checkout/sessions`, `mode=subscription`. Deliberately
    /// [`Idem::NotIdempotent`] — a checkout session is not a charge, and a
    /// user clicking "Upgrade" twice legitimately wants a fresh session
    /// with a fresh expiry. This is the one deliberate exception to "every
    /// POST needs a key recipe"; see this module's Idempotency doc.
    pub async fn create_checkout_session_subscription(
        &self,
        customer_id: &str,
        price_id: &str,
        account_id: &str,
        success_url: &str,
        cancel_url: &str,
        allow_promotion_codes: bool,
    ) -> Result<CheckoutSession, StripeError> {
        let p = checkout_subscription_params(
            customer_id,
            price_id,
            account_id,
            success_url,
            cancel_url,
            allow_promotion_codes,
        );
        let v = self
            .post_form("/v1/checkout/sessions", &p, Idem::NotIdempotent)
            .await?;
        Ok(CheckoutSession {
            id: req_str(&v, "id")?,
            url: req_str(&v, "url")?,
        })
    }

    /// `POST /v1/checkout/sessions`, `mode=payment`. Same `NotIdempotent`
    /// reasoning as the subscription-mode sibling above.
    pub async fn create_checkout_session_payment(
        &self,
        customer_id: &str,
        price_id: &str,
        quantity: i64,
        account_id: &str,
        success_url: &str,
        cancel_url: &str,
    ) -> Result<CheckoutSession, StripeError> {
        let p = checkout_payment_params(
            customer_id,
            price_id,
            quantity,
            account_id,
            success_url,
            cancel_url,
        );
        let v = self
            .post_form("/v1/checkout/sessions", &p, Idem::NotIdempotent)
            .await?;
        Ok(CheckoutSession {
            id: req_str(&v, "id")?,
            url: req_str(&v, "url")?,
        })
    }

    /// `POST /v1/checkout/sessions` for a self-serve org's first Checkout;
    /// see [`org_trial_checkout_params`]. Not idempotency-keyed, for the
    /// same reason as [`Self::create_checkout_session_subscription`].
    #[allow(clippy::too_many_arguments)]
    pub async fn create_org_trial_checkout_session(
        &self,
        customer_id: &str,
        price_id: &str,
        seats: i64,
        account_id: &str,
        trial_days: Option<i64>,
        success_url: &str,
        cancel_url: &str,
    ) -> Result<CheckoutSession, StripeError> {
        let p = org_trial_checkout_params(
            customer_id,
            price_id,
            seats,
            account_id,
            trial_days,
            success_url,
            cancel_url,
        );
        let v = self.post_form("/v1/checkout/sessions", &p, Idem::NotIdempotent).await?;
        Ok(CheckoutSession { id: req_str(&v, "id")?, url: req_str(&v, "url")? })
    }

    /// End a subscription's trial now, charging its items. Keyed by the
    /// caller (one key per subscription): a retried or concurrent call
    /// replays Stripe's first answer instead of updating twice.
    pub async fn end_trial_now(&self, subscription_id: &str, idem_key: &str) -> Result<SubscriptionView, StripeError> {
        let v = self
            .post_form(&format!("/v1/subscriptions/{subscription_id}"), &end_trial_now_params(), Idem::Key(idem_key))
            .await?;
        parse_subscription(&v)
    }

    /// `GET /v1/payment_methods/{id}`, returning the card's `fingerprint`
    /// (the same card added twice has the same fingerprint). `None` for a
    /// method that is not a card.
    pub async fn retrieve_card_fingerprint(&self, payment_method_id: &str) -> Result<Option<String>, StripeError> {
        let v = self.get(&format!("/v1/payment_methods/{payment_method_id}"), &Params::new()).await?;
        Ok(v.get("card")
            .and_then(|c| c.get("fingerprint"))
            .and_then(Value::as_str)
            .filter(|f| !f.is_empty())
            .map(str::to_string))
    }

    /// `GET /v1/setup_intents/{id}`, returning its `payment_method` id. A
    /// trial Checkout saves the card through a SetupIntent, so this is where
    /// the card is when the subscription and customer carry no default.
    pub async fn retrieve_setup_intent_payment_method(&self, setup_intent_id: &str) -> Result<Option<String>, StripeError> {
        let v = self.get(&format!("/v1/setup_intents/{setup_intent_id}"), &Params::new()).await?;
        Ok(reference_id_value(v.get("payment_method")))
    }

    /// `POST /v1/invoices/{id}/void` under a caller key. Voiding an open
    /// invoice means no retry, and no hosted-page payment, can collect it.
    /// Voiding a paid or already-void invoice is a 400 from Stripe; callers
    /// treat the call as best effort.
    pub async fn void_invoice(&self, invoice_id: &str, idem_key: &str) -> Result<(), StripeError> {
        self.post_form(&format!("/v1/invoices/{invoice_id}/void"), &Params::new(), Idem::Key(idem_key))
            .await?;
        Ok(())
    }

    /// `GET /v1/invoices/{id}?expand[0]=payments`: status, amount paid, and
    /// the PaymentIntent that paid it. Read-only.
    pub async fn retrieve_invoice(&self, invoice_id: &str) -> Result<InvoiceView, StripeError> {
        let mut q = Params::new();
        q.push("expand[0]", "payments");
        let v = self.get(&format!("/v1/invoices/{invoice_id}"), &q).await?;
        parse_invoice_view(&v)
    }

    /// `POST /v1/checkout/sessions/{id}/expire`. Used to retire an abandoned
    /// subscription Checkout when a new one replaces it. Expiring an already
    /// expired or completed session is a 400 from Stripe; callers treat the
    /// whole call as best effort. No parameters, no cumulative effect:
    /// [`Idem::NaturallyIdempotent`].
    pub async fn expire_checkout_session(&self, id: &str) -> Result<(), StripeError> {
        self.post_form(
            &format!("/v1/checkout/sessions/{id}/expire"),
            &Params::new(),
            Idem::NaturallyIdempotent,
        )
        .await?;
        Ok(())
    }

    /// Ids of the customer's OPEN Checkout Sessions (`GET /v1/checkout/sessions?
    /// customer=…&status=open`), every page. Org deletion expires these so a
    /// Checkout opened before the request cannot start a new subscription.
    pub async fn list_open_checkout_sessions(&self, customer_id: &str) -> Result<Vec<String>, StripeError> {
        let mut out = Vec::new();
        let mut after: Option<String> = None;
        for _ in 0..20 {
            let mut q = Params::new();
            q.push("customer", customer_id);
            q.push("status", "open");
            q.push("limit", "100");
            if let Some(a) = after.as_deref() {
                q.push("starting_after", a);
            }
            let v = self.get("/v1/checkout/sessions", &q).await?;
            let data = v
                .get("data")
                .and_then(|d| d.as_array())
                .ok_or_else(|| StripeError::Parse("missing/invalid \"data\" field".into()))?;
            out.extend(data.iter().filter_map(|s| s.get("id").and_then(|i| i.as_str()).map(str::to_string)));
            let more = v.get("has_more").and_then(|m| m.as_bool()).unwrap_or(false);
            match (more, out.last()) {
                (true, Some(last)) => after = Some(last.clone()),
                _ => return Ok(out),
            }
        }
        Err(StripeError::Parse("too many open checkout sessions".into()))
    }

    /// `(id, status)` of every subscription the customer has at Stripe,
    /// whatever its status (`GET /v1/subscriptions?customer=…&status=all`),
    /// every page. This is Stripe's truth, never our mirror.
    pub async fn list_customer_subscriptions(&self, customer_id: &str) -> Result<Vec<(String, String)>, StripeError> {
        let mut out: Vec<(String, String)> = Vec::new();
        let mut after: Option<String> = None;
        for _ in 0..20 {
            let mut q = Params::new();
            q.push("customer", customer_id);
            q.push("status", "all");
            q.push("limit", "100");
            if let Some(a) = after.as_deref() {
                q.push("starting_after", a);
            }
            let v = self.get("/v1/subscriptions", &q).await?;
            let data = v
                .get("data")
                .and_then(|d| d.as_array())
                .ok_or_else(|| StripeError::Parse("missing/invalid \"data\" field".into()))?;
            for s in data {
                out.push((req_str(s, "id")?, req_str(s, "status")?));
            }
            let more = v.get("has_more").and_then(|m| m.as_bool()).unwrap_or(false);
            match (more, out.last()) {
                (true, Some(last)) => after = Some(last.0.clone()),
                _ => return Ok(out),
            }
        }
        Err(StripeError::Parse("too many subscriptions".into()))
    }

    /// `GET /v1/checkout/sessions/{id}?expand[0]=line_items`. **Required**:
    /// `checkout.session.completed` does NOT carry `line_items` in the
    /// event payload — the webhook handler must re-fetch with expansion.
    /// Returns the raw `Value`; [`parse_checkout_session_line_items`] is what
    /// actually parses the (possibly multi-line-item) `line_items.data`
    /// array.
    pub async fn retrieve_checkout_session(&self, id: &str) -> Result<Value, StripeError> {
        let q = session_retrieve_query();
        self.get(&format!("/v1/checkout/sessions/{id}"), &q).await
    }
}

/// One entry of an EXPANDED checkout session's `line_items.data` array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLineItem {
    pub price_id: String,
    pub quantity: i64,
}

/// Parses EVERY entry of `line_items.data` from an expanded checkout session
/// (the `Value` [`StripeClient::retrieve_checkout_session`] returns) — never
/// just the first. This is the concrete fix for plan §2.D's "the current
/// `items.data[0]` single-item assumption must die": PR D's own checkout
/// routes only ever create a single-price session today (subscription mode
/// hardcodes `line_items[0]`; a pack purchase is one pack code per session),
/// but the PARSER makes no such assumption — `testdata/
/// checkout_session_completed_expanded.json` (two pack line items) is
/// consumed by this exact function in this module's test below, not merely
/// shape-asserted, closing the gap the fixture's own README flagged.
///
/// Consumed by `routes::stripe_webhook`'s `checkout.session.completed`
/// handling for BOTH modes: subscription mode uses it to recover which
/// price (and therefore which plan/seat-tier) was purchased, and payment
/// mode sums `credits_packs.credits * quantity` over every entry to credit
/// the ledger — a pack purchase is never assumed to be exactly one line.
pub fn parse_checkout_session_line_items(
    session: &Value,
) -> Result<Vec<SessionLineItem>, StripeError> {
    parse_line_items_where(session, |_| true)
}

/// A free one-time line: `price.type = "one_time"`, `price.unit_amount = 0`
/// and `amount_total = 0`, all three present. The trial Checkout's welcome
/// offer ([`welcome_offer_name`]) is one. It is not a subscription item (it
/// lands on the first invoice only), so it is neither a seat nor a price to
/// look up. A line missing any of the three fields is NOT free, so a
/// fixture or a payload of an older shape is still counted.
fn is_free_one_time_line(it: &Value) -> bool {
    let price = it.get("price");
    price.and_then(|p| p.get("type")).and_then(Value::as_str) == Some("one_time")
        && price.and_then(|p| p.get("unit_amount")).and_then(Value::as_i64) == Some(0)
        && it.get("amount_total").and_then(Value::as_i64) == Some(0)
}

/// [`parse_checkout_session_line_items`] minus the free one-time lines
/// ([`is_free_one_time_line`]): what a subscription-mode session actually
/// SUBSCRIBES to. The webhook's "exactly one priced line" rule reads this, so
/// the trial Checkout's $0 welcome-offer line cannot make a trial session
/// look like a two-item purchase. A paid one-time line is still counted, so
/// any other extra item is still refused.
pub fn parse_checkout_session_priced_line_items(
    session: &Value,
) -> Result<Vec<SessionLineItem>, StripeError> {
    parse_line_items_where(session, |it| !is_free_one_time_line(it))
}

fn parse_line_items_where(
    session: &Value,
    keep: impl Fn(&Value) -> bool,
) -> Result<Vec<SessionLineItem>, StripeError> {
    let items = session
        .get("line_items")
        .and_then(|l| l.get("data"))
        .and_then(|d| d.as_array())
        .ok_or_else(|| StripeError::Parse("missing/invalid \"line_items.data\" field".into()))?;
    items
        .iter()
        .filter(|it| keep(it))
        .map(|it| {
            let price_id = it
                .get("price")
                .and_then(|p| p.get("id"))
                .and_then(|x| x.as_str())
                .ok_or_else(|| {
                    StripeError::Parse(
                        "missing/invalid \"line_items.data[].price.id\" field".into(),
                    )
                })?
                .to_string();
            Ok(SessionLineItem {
                price_id,
                quantity: req_i64(it, "quantity")?,
            })
        })
        .collect::<Result<Vec<_>, StripeError>>()
}

// ── Typed surface — PR F (seat lifecycle, plan §2.F) ────────────────────────

impl StripeClient {
    /// `GET /v1/subscriptions/{id}`. Mixed seat tiers (§1.4) make a
    /// two-item subscription the normal case — see [`SubscriptionView`]'s
    /// doc comment.
    pub async fn retrieve_subscription(&self, id: &str) -> Result<SubscriptionView, StripeError> {
        let v = self.get(&format!("/v1/subscriptions/{id}"), &Params::new()).await?;
        parse_subscription(&v)
    }

    /// `POST /v1/subscription_items/{id}` — seat add/remove within an
    /// existing tier. `proration_behavior` is the caller's decision (plan
    /// §1.7: `create_prorations` for most seat changes — Stripe prorates
    /// money).
    pub async fn update_subscription_item(
        &self,
        item_id: &str,
        quantity: i64,
        proration_behavior: &str,
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let p = subscription_item_quantity_params(quantity, proration_behavior);
        self.post_form(&format!("/v1/subscription_items/{item_id}"), &p, idem)
            .await
    }

    /// `POST /v1/subscription_items/{id}` — tier UPGRADE/DOWNGRADE.
    /// **Upgrade** ⇒ caller passes `proration_behavior="create_prorations"`
    /// (immediate). **Downgrade** ⇒ caller passes `"none"` (plan §1.7,
    /// verbatim: no refund, premium allowance kept to period end). Getting
    /// these backwards refunds money the plan says we keep — see the two
    /// named wire-pair tests in this module's `tests` submodule.
    pub async fn swap_subscription_item_price(
        &self,
        item_id: &str,
        new_price_id: &str,
        proration_behavior: &str,
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let p = subscription_item_swap_params(new_price_id, proration_behavior);
        self.post_form(&format!("/v1/subscription_items/{item_id}"), &p, idem)
            .await
    }

    /// `POST /v1/subscription_items` — first member on a second tier.
    pub async fn add_subscription_item(
        &self,
        subscription_id: &str,
        price_id: &str,
        quantity: i64,
        proration_behavior: &str,
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.push("subscription", subscription_id);
        p.push("price", price_id);
        p.push("quantity", quantity.to_string());
        p.push("proration_behavior", proration_behavior);
        self.post_form("/v1/subscription_items", &p, idem).await
    }

    /// `DELETE /v1/subscription_items/{id}` — last member left a tier.
    pub async fn delete_subscription_item(
        &self,
        item_id: &str,
        proration_behavior: &str,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.push("proration_behavior", proration_behavior);
        self.delete(&format!("/v1/subscription_items/{item_id}"), &p)
            .await
    }

    /// `POST /v1/subscriptions/{id}` — in-app cancel (§1.12):
    /// `cancel_at_period_end` + `cancellation_details[reason]` (the webhook
    /// already reads `cancellation_details.reason`,
    /// `routes::stripe_webhook`). A TYPED signature rather than a generic
    /// params blob — this is the only field pair this call ever sets.
    pub async fn update_subscription(
        &self,
        subscription_id: &str,
        cancel_at_period_end: bool,
        cancellation_reason: Option<&str>,
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.bool("cancel_at_period_end", cancel_at_period_end);
        if let Some(r) = cancellation_reason {
            p.push("cancellation_details[reason]", r);
        }
        self.post_form(&format!("/v1/subscriptions/{subscription_id}"), &p, idem)
            .await
    }

    /// `DELETE /v1/subscriptions/{id}` — immediate cancel path.
    pub async fn cancel_subscription(
        &self,
        subscription_id: &str,
        reason: Option<&str>,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        if let Some(r) = reason {
            p.push("cancellation_details[reason]", r);
        }
        self.delete(&format!("/v1/subscriptions/{subscription_id}"), &p)
            .await
    }

    /// `DELETE /v1/customers/{id}`: removes the customer, its saved payment
    /// methods and any subscription still on it. Invoices and charges stay in
    /// the Stripe dashboard for tax and dispute purposes. Used only by account
    /// deletion, after the subscription was ended. An already-deleted customer
    /// answers 404 or `deleted: true`; the caller treats both as done.
    pub async fn delete_customer(&self, customer_id: &str) -> Result<Value, StripeError> {
        self.delete(&format!("/v1/customers/{customer_id}"), &Params::new()).await
    }

    /// `POST /v1/refunds` for a payment intent, under a caller-supplied
    /// deterministic key so a webhook replay refunds once.
    pub async fn refund_payment_intent(
        &self,
        payment_intent: &str,
        idempotency_key: &str,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.push("payment_intent", payment_intent);
        p.push("reason", "requested_by_customer");
        self.post_form("/v1/refunds", &p, Idem::Key(idempotency_key)).await
    }

    /// `DELETE /v1/subscriptions/{id}` with `prorate=false` and
    /// `invoice_now=false`: end the subscription NOW and create neither a
    /// proration credit nor a final invoice. Used only when the customer is
    /// being deleted (settings launch 2.4): nothing may bill them again and
    /// remaining paid time is forfeited, not credited to a customer nobody
    /// can sign in as.
    pub async fn cancel_subscription_now_without_proration(
        &self,
        subscription_id: &str,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.bool("prorate", false);
        p.bool("invoice_now", false);
        self.delete(&format!("/v1/subscriptions/{subscription_id}"), &p)
            .await
    }

    /// [`Self::cancel_subscription_now_without_proration`] sent with an
    /// `Idempotency-Key` (callers key it on the subscription id). Stripe
    /// treats `DELETE` as idempotent by definition, so the key is a belt on
    /// top of that: a retry within the key's lifetime replays the first
    /// answer. Cancelling a subscription that is already canceled is still
    /// an error at Stripe; callers that need "canceled, however it got
    /// there" re-read the subscription on error.
    pub async fn cancel_subscription_now_keyed(&self, subscription_id: &str, idem_key: &str) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.bool("prorate", false);
        p.bool("invoice_now", false);
        self.execute(
            Method::DELETE,
            &format!("/v1/subscriptions/{subscription_id}"),
            &p,
            RetryMode::WithIdemKey(idem_key),
        )
        .await
    }
}

// ── Typed surface — PR G (auto-refill, plan §1.10/§2.G) ─────────────────────

impl StripeClient {
    /// `POST /v1/payment_intents`, `off_session=true` + `confirm=true` +
    /// `currency=usd`. `idem_key` is a **mandatory `&str`, not an
    /// [`Idem`]** — the signature itself makes it impossible to call this
    /// unkeyed (plan §1.10, verbatim: "idempotent per (account, period,
    /// counter)" — carry that wording into the caller's own key recipe).
    pub async fn create_payment_intent_off_session(
        &self,
        customer_id: &str,
        amount_cents: i64,
        payment_method: &str,
        metadata: &[(&str, &str)],
        idem_key: &str,
    ) -> Result<Value, StripeError> {
        let p = payment_intent_off_session_params(customer_id, amount_cents, payment_method, metadata);
        self.post_form("/v1/payment_intents", &p, Idem::Key(idem_key))
            .await
    }

    /// `GET /v1/customers/{id}`, reads `invoice_settings.default_payment_method`.
    pub async fn retrieve_customer_default_payment_method(
        &self,
        customer_id: &str,
    ) -> Result<Option<String>, StripeError> {
        let v = self
            .get(&format!("/v1/customers/{customer_id}"), &Params::new())
            .await?;
        Ok(v.get("invoice_settings")
            .and_then(|s| s.get("default_payment_method"))
            .and_then(|pm| pm.as_str())
            .map(str::to_string))
    }
}

// ── Typed surface — PR H (billing management, plan §1.12) ──────────────────

impl StripeClient {
    /// `POST /v1/setup_intents`, `usage=off_session`,
    /// `payment_method_types[0]=card`. `client_secret` is what web Elements
    /// consumes.
    pub async fn create_setup_intent(
        &self,
        customer_id: &str,
        idem: Idem<'_>,
    ) -> Result<SetupIntent, StripeError> {
        let mut p = Params::new();
        p.push("customer", customer_id);
        p.push("usage", "off_session");
        p.list("payment_method_types", &["card"]);
        let v = self.post_form("/v1/setup_intents", &p, idem).await?;
        Ok(SetupIntent {
            id: req_str(&v, "id")?,
            client_secret: req_str(&v, "client_secret")?,
        })
    }

    /// `POST /v1/customers/{id}`, `invoice_settings[default_payment_method]`.
    pub async fn set_default_payment_method(
        &self,
        customer_id: &str,
        payment_method_id: &str,
        idem: Idem<'_>,
    ) -> Result<Value, StripeError> {
        let mut p = Params::new();
        p.nested("invoice_settings", "default_payment_method", payment_method_id);
        self.post_form(&format!("/v1/customers/{customer_id}"), &p, idem)
            .await
    }

    /// `GET /v1/payment_methods?customer=…&type=card`.
    pub async fn list_payment_methods(
        &self,
        customer_id: &str,
    ) -> Result<Vec<PaymentMethodSummary>, StripeError> {
        self.list_payment_methods_of_type(customer_id, "card").await
    }

    /// `GET /v1/payment_methods?customer=…&type=…` (Stripe requires a type).
    pub async fn list_payment_methods_of_type(
        &self,
        customer_id: &str,
        kind: &str,
    ) -> Result<Vec<PaymentMethodSummary>, StripeError> {
        let mut q = Params::new();
        q.push("customer", customer_id);
        q.push("type", kind);
        let resp = self.get("/v1/payment_methods", &q).await?;
        let data = resp
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| StripeError::Parse("missing/invalid \"data\" field".into()))?;
        data.iter().map(parse_payment_method).collect()
    }

    /// `GET /v1/invoices?customer=…&limit=…&starting_after=…`. §1.12: "PDFs
    /// remain Stripe-hosted links" — see [`InvoiceSummary`].
    pub async fn list_invoices(
        &self,
        customer_id: &str,
        limit: i64,
        starting_after: Option<&str>,
    ) -> Result<InvoiceList, StripeError> {
        let mut q = Params::new();
        q.push("customer", customer_id);
        q.push("limit", limit.to_string());
        if let Some(sa) = starting_after {
            q.push("starting_after", sa);
        }
        let resp = self.get("/v1/invoices", &q).await?;
        let data = resp
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| StripeError::Parse("missing/invalid \"data\" field".into()))?
            .iter()
            .map(parse_invoice)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = req_bool(&resp, "has_more")?;
        Ok(InvoiceList { data, has_more })
    }
}

// ── Typed surface — settings launch PR 3.3 (portal, seat quote, card) ──────

/// Metadata marker on the one Billing Portal configuration this system owns.
/// Lookup is by this marker, never by a configuration id copied into env or
/// the database: the id changes per Stripe account/mode, the marker does not.
pub const PORTAL_CONFIG_METADATA_KEY: &str = "cs_portal_config";
/// Bump the version to provision a replacement configuration (the old one is
/// left in place and simply stops being selected).
pub const PORTAL_CONFIG_VERSION: &str = "v1";

/// The Billing Portal configuration CoralSwarm wants: payment-method update,
/// invoice history, and cancel at period end. Everything else is OFF:
///
/// * `subscription_update` is disabled, so seat quantity and tier changes
///   cannot happen in the portal. They go through our own quote + confirm
///   flow (`GET /v1/billing/seats/quote`, `POST /v1/orgs/slots`), which keeps
///   the seat ledger (`grant_seat_tiers`) in step with Stripe.
/// * `customer_update` is disabled (no email/address/tax edits that would
///   drift from our account record).
/// * Cancellation is `at_period_end` with `proration_behavior=none`: nothing
///   is refunded and access (and the purchased allowance) runs to the end of
///   the paid period. Immediate cancel is never offered.
///
/// Used verbatim for both create and update, so re-running provisioning
/// converges a drifted configuration back to this shape.
pub fn portal_configuration_params() -> Params {
    let mut p = Params::new();
    p.nested("business_profile", "headline", "Manage your CoralSwarm billing");
    p.bool("features[customer_update][enabled]", false);
    p.bool("features[invoice_history][enabled]", true);
    p.bool("features[payment_method_update][enabled]", true);
    p.bool("features[subscription_update][enabled]", false);
    p.bool("features[subscription_cancel][enabled]", true);
    p.push("features[subscription_cancel][mode]", "at_period_end");
    p.push("features[subscription_cancel][proration_behavior]", "none");
    // No `cancellation_reason` block: Stripe rejects `[enabled]=false` there
    // unless `[options]` is also sent, and we do not collect reasons.
    p.nested("metadata", PORTAL_CONFIG_METADATA_KEY, PORTAL_CONFIG_VERSION);
    p
}

/// Whether a `GET /v1/billing_portal/configurations` entry is the one this
/// system owns at the current version.
pub fn is_owned_portal_configuration(cfg: &Value) -> bool {
    cfg.get("metadata")
        .and_then(|m| m.get(PORTAL_CONFIG_METADATA_KEY))
        .and_then(Value::as_str)
        == Some(PORTAL_CONFIG_VERSION)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortalSession {
    pub id: String,
    pub url: String,
}

/// One change to preview against a live subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreviewChange<'a> {
    /// Set an existing subscription item to `quantity` (>= 1).
    SetQuantity { item_id: &'a str, quantity: i64 },
    /// Remove an existing subscription item (its quantity goes to zero).
    RemoveItem { item_id: &'a str },
    /// Add a new item at `price_id` x `quantity`.
    AddItem { price_id: &'a str, quantity: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewLine {
    pub amount: i64,
    pub proration: bool,
}

/// The parts of a preview invoice the seat quote reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvoicePreview {
    pub total: i64,
    pub amount_due: i64,
    pub currency: String,
    pub lines: Vec<PreviewLine>,
}

fn preview_params(
    customer_id: &str,
    subscription_id: &str,
    change: Option<PreviewChange<'_>>,
    proration_date: i64,
    prorate: bool,
) -> Params {
    let mut p = Params::new();
    p.push("customer", customer_id);
    p.push("subscription", subscription_id);
    if let Some(change) = change {
        // Must match what the real change will do: seat ADDS use
        // `create_prorations`; REDUCTIONS use `none` (no credit, the lower
        // amount starts next period), see `routes::org_slots::reduce_slots`.
        p.push(
            "subscription_details[proration_behavior]",
            if prorate { "create_prorations" } else { "none" },
        );
        if prorate {
            p.push("subscription_details[proration_date]", proration_date.to_string());
        }
        match change {
            PreviewChange::SetQuantity { item_id, quantity } => {
                p.push("subscription_details[items][0][id]", item_id);
                p.push("subscription_details[items][0][quantity]", quantity.to_string());
            }
            PreviewChange::RemoveItem { item_id } => {
                p.push("subscription_details[items][0][id]", item_id);
                p.bool("subscription_details[items][0][deleted]", true);
            }
            PreviewChange::AddItem { price_id, quantity } => {
                p.push("subscription_details[items][0][price]", price_id);
                p.push("subscription_details[items][0][quantity]", quantity.to_string());
            }
        }
    }
    p
}

fn parse_invoice_preview(v: &Value) -> Result<InvoicePreview, StripeError> {
    let lines = v
        .get("lines")
        .and_then(|l| l.get("data"))
        .and_then(Value::as_array)
        .ok_or_else(|| StripeError::Parse("missing/invalid \"lines.data\" field".into()))?
        .iter()
        .map(|line| {
            // `proration` is a top-level bool on older API versions and moved
            // under `parent.subscription_item_details` on `basil`. Absent in
            // both shapes reads as "not a proration line".
            let proration = line
                .get("proration")
                .and_then(Value::as_bool)
                .or_else(|| {
                    line.get("parent")?
                        .get("subscription_item_details")?
                        .get("proration")?
                        .as_bool()
                })
                .unwrap_or(false);
            Ok(PreviewLine { amount: req_i64(line, "amount")?, proration })
        })
        .collect::<Result<Vec<_>, StripeError>>()?;
    Ok(InvoicePreview {
        total: req_i64(v, "total")?,
        amount_due: req_i64(v, "amount_due")?,
        currency: req_str(v, "currency")?,
        lines,
    })
}

impl StripeClient {
    /// `POST /v1/billing_portal/sessions`. [`Idem::NotIdempotent`], the same
    /// deliberate exception as the checkout-session creators: a portal
    /// session is a short-lived link, not a charge, and every click wants a
    /// fresh one.
    pub async fn create_billing_portal_session(
        &self,
        customer_id: &str,
        return_url: &str,
        configuration_id: Option<&str>,
    ) -> Result<PortalSession, StripeError> {
        let mut p = Params::new();
        p.push("customer", customer_id);
        p.push("return_url", return_url);
        if let Some(c) = configuration_id {
            p.push("configuration", c);
        }
        let v = self
            .post_form("/v1/billing_portal/sessions", &p, Idem::NotIdempotent)
            .await?;
        Ok(PortalSession { id: req_str(&v, "id")?, url: req_str(&v, "url")? })
    }

    /// The id of this system's active Billing Portal configuration, if
    /// provisioned (`provision_stripe` creates it).
    pub async fn find_portal_configuration(&self) -> Result<Option<String>, StripeError> {
        let mut q = Params::new();
        // Stripe's list filter is `active` (there is no `is_active`; sending
        // it is a 400 `parameter_unknown`).
        q.push("active", "true");
        q.push("limit", "100");
        let resp = self.get("/v1/billing_portal/configurations", &q).await?;
        let data = resp
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| StripeError::Parse("missing/invalid \"data\" field".into()))?;
        Ok(data
            .iter()
            .find(|c| is_owned_portal_configuration(c))
            .and_then(|c| c.get("id").and_then(Value::as_str))
            .map(str::to_string))
    }

    /// `POST /v1/billing_portal/configurations`. Provisioning only.
    /// Idempotency recipe `portal-config:{version}`: a retried provisioning
    /// run inside Stripe's 24h window resolves the same configuration.
    pub async fn create_portal_configuration(&self) -> Result<String, StripeError> {
        let key = format!("portal-config:{PORTAL_CONFIG_VERSION}");
        let v = self
            .post_form(
                "/v1/billing_portal/configurations",
                &portal_configuration_params(),
                Idem::Key(&key),
            )
            .await?;
        req_str(&v, "id")
    }

    /// `POST /v1/billing_portal/configurations/{id}`. Reconciling to a fixed
    /// shape has no cumulative effect, so it is [`Idem::NaturallyIdempotent`]
    /// (same reasoning as `update_webhook_endpoint`).
    pub async fn update_portal_configuration(&self, id: &str) -> Result<(), StripeError> {
        let mut p = portal_configuration_params();
        p.bool("active", true);
        self.post_form(
            &format!("/v1/billing_portal/configurations/{id}"),
            &p,
            Idem::NaturallyIdempotent,
        )
        .await?;
        Ok(())
    }

    /// `POST /v1/invoices/create_preview` (the `basil` replacement for the
    /// removed upcoming-invoice GET). Read-only on Stripe's side: it creates
    /// nothing, so it is [`Idem::NaturallyIdempotent`]. `change = None`
    /// previews the next invoice as it stands today.
    pub async fn create_preview_invoice(
        &self,
        customer_id: &str,
        subscription_id: &str,
        change: Option<PreviewChange<'_>>,
        proration_date: i64,
        prorate: bool,
    ) -> Result<InvoicePreview, StripeError> {
        let p = preview_params(customer_id, subscription_id, change, proration_date, prorate);
        let v = self
            .post_form("/v1/invoices/create_preview", &p, Idem::NaturallyIdempotent)
            .await?;
        parse_invoice_preview(&v)
    }

    /// The customer's default payment method of ANY type, for display and for
    /// the auto-reload card check. One expanded GET; when no default is set,
    /// falls back to the first method on file (cards first, then bank
    /// accounts, then Link). `Ok(None)` is "nothing on file", not an error.
    pub async fn retrieve_customer_default_payment_method_summary(
        &self,
        customer_id: &str,
    ) -> Result<Option<PaymentMethodSummary>, StripeError> {
        let mut q = Params::new();
        q.list("expand", &["invoice_settings.default_payment_method"]);
        let v = self.get(&format!("/v1/customers/{customer_id}"), &q).await?;
        if let Some(pm) = v
            .get("invoice_settings")
            .and_then(|s| s.get("default_payment_method"))
            .filter(|pm| pm.is_object())
        {
            return parse_payment_method(pm).map(Some);
        }
        for kind in ["card", "us_bank_account", "link"] {
            if let Some(pm) = self.list_payment_methods_of_type(customer_id, kind).await?.into_iter().next() {
                return Ok(Some(pm));
            }
        }
        Ok(None)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes tests that mutate `STRIPE_*` process-global env vars.
    /// `tokio::sync::Mutex`, not `std::sync::Mutex`: several consumers below
    /// are `async fn` tests that hold the guard across an `.await` — a std
    /// `MutexGuard` held across an await point is a clippy
    /// `await_holding_lock` lint (and a real footgun on a multi-threaded
    /// runtime). Exact precedent: `llm::model_config::EXTRACT_ESCALATE_ENV_LOCK`.
    /// `.blocking_lock()` for this file's plain (non-async) `#[test]` usage,
    /// `.lock().await` for the async ones.
    static RETRY_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    // ── Layer 1: pure Params wire-pair assertions ───────────────────────

    fn pairs(p: &Params) -> Vec<(String, String)> {
        p.as_pairs().to_vec()
    }

    fn assert_pair(p: &Params, key: &str, value: &str) {
        let found = pairs(p).into_iter().find(|(k, _)| k == key);
        assert_eq!(
            found.as_ref().map(|(_, v)| v.as_str()),
            Some(value),
            "expected {key}={value:?} in {:?}",
            pairs(p)
        );
    }

    fn assert_absent(p: &Params, key: &str) {
        assert!(
            pairs(p).iter().all(|(k, _)| k != key),
            "expected {key} to be ABSENT from {:?}",
            pairs(p)
        );
    }

    #[test]
    fn params_nested_and_indexed_and_list_and_bool() {
        let mut p = Params::new();
        p.nested("metadata", "account_id", "acc_1");
        p.indexed("line_items", 0, "price", "price_1");
        p.list("expand", &["line_items", "customer"]);
        p.bool("allow_promotion_codes", true);
        p.bool("allow_promotion_codes_false", false);

        assert_pair(&p, "metadata[account_id]", "acc_1");
        assert_pair(&p, "line_items[0][price]", "price_1");
        assert_pair(&p, "expand[0]", "line_items");
        assert_pair(&p, "expand[1]", "customer");
        // Booleans serialise as "true"/"false", never "1"/"0".
        assert_pair(&p, "allow_promotion_codes", "true");
        assert_pair(&p, "allow_promotion_codes_false", "false");
    }

    #[test]
    fn customer_create_wire_pairs() {
        let p = customer_params("acc_123", "a@example.com", None, "test");
        assert_pair(&p, "email", "a@example.com");
        assert_pair(&p, "metadata[account_id]", "acc_123");
        assert_pair(&p, "metadata[env]", "test");
        assert_absent(&p, "name");
    }

    #[test]
    fn customer_create_includes_name_when_given() {
        let p = customer_params("acc_123", "a@example.com", Some("Acme Inc"), "live");
        assert_pair(&p, "name", "Acme Inc");
        assert_pair(&p, "metadata[env]", "live");
    }

    /// Auto-reload charges the saved method off-session as a card
    /// PaymentIntent, so a subscription paid by bank debit would leave the
    /// customer unable to ever turn it on.
    #[test]
    fn subscription_checkout_is_card_only() {
        let p = checkout_subscription_params("cus_1", "price_standard", "acc_123", "https://a/s", "https://a/c", false);
        assert_pair(&p, "payment_method_types[0]", "card");
        assert_absent(&p, "payment_method_types[1]");
    }

    /// Self-serve PR C′: the trial Checkout takes a card, charges $0, buys
    /// the chosen seat count, and ends itself on day 90 at the latest.
    #[test]
    fn org_trial_checkout_collects_a_card_for_a_ninety_day_trial_on_every_seat() {
        let p = org_trial_checkout_params("cus_1", "price_std", 4, "acc_1", Some(90), "https://a/s", "https://a/c");
        assert_pair(&p, "mode", "subscription");
        assert_pair(&p, "line_items[0][price]", "price_std");
        assert_pair(&p, "line_items[0][quantity]", "4");
        assert_absent(&p, "line_items[1][price]");
        assert_pair(&p, "payment_method_collection", "always");
        assert_pair(&p, "subscription_data[trial_period_days]", "90");
        assert_pair(&p, "subscription_data[trial_settings][end_behavior][missing_payment_method]", "cancel");
        assert_pair(&p, "subscription_data[metadata][account_id]", "acc_1");
        assert_pair(&p, "client_reference_id", "acc_1");
        assert_pair(&p, "allow_promotion_codes", "false");
        assert_pair(&p, "payment_method_types[0]", "card");
        assert_absent(&p, "payment_method_types[1]");
        let msg = org_trial_submit_message(4, 90);
        assert_pair(&p, "custom_text[submit][message]", &msg);
        assert!(msg.contains("won't be charged today"), "{msg}");
        assert!(msg.contains("credits run out"), "{msg}");
        assert!(msg.contains("4 seats"), "{msg}");
        assert!(msg.contains("90 days"), "{msg}");
        // The copy's credit figure is the real pool size.
        assert_eq!(crate::billing::org_trial::TRIAL_CREDITS, 3_000, "update org_trial_submit_message");
        assert!(msg.len() <= 1200, "Stripe caps custom_text at 1200 characters");
    }

    /// The welcome offer is a free, one-time, inline-priced second line. It
    /// is on a trial Checkout only, never recurring, and the seat line is
    /// untouched.
    #[test]
    fn org_trial_checkout_adds_a_free_one_time_welcome_offer_line() {
        let p = org_trial_checkout_params("cus_1", "price_std", 4, "acc_1", Some(90), "https://a/s", "https://a/c");
        assert_pair(&p, "line_items[1][price_data][currency]", "usd");
        assert_pair(&p, "line_items[1][price_data][unit_amount]", "0");
        assert_pair(&p, "line_items[1][price_data][product_data][name]", "Welcome offer: 3,000 free credits");
        assert_pair(&p, "line_items[1][quantity]", "1");
        assert_absent(&p, "line_items[1][price]");
        assert_absent(&p, "line_items[1][price_data][recurring][interval]");
        assert_absent(&p, "line_items[2][quantity]");
        assert_pair(&p, "line_items[0][price]", "price_std");
        assert_pair(&p, "line_items[0][quantity]", "4");
        assert_pair(&p, "subscription_data[trial_period_days]", "90");
        assert_eq!(welcome_offer_name(), "Welcome offer: 3,000 free credits");
        assert_eq!(format_credits(999), "999");
        assert_eq!(format_credits(1_000_000), "1,000,000");
    }

    /// No trial (the creator already had one): seats are charged at once, so
    /// there is nothing to welcome them with.
    #[test]
    fn org_trial_checkout_without_a_trial_has_no_welcome_offer() {
        let p = org_trial_checkout_params("cus_1", "price_std", 2, "acc_1", None, "https://a/s", "https://a/c");
        assert_absent(&p, "line_items[1][quantity]");
        assert_absent(&p, "line_items[1][price_data][unit_amount]");
    }

    /// The real shape Stripe returned in the sandbox (session
    /// `cs_test_b1sto...`): seat line recurring qty 2, welcome line one_time
    /// $0. The priced view keeps only the seat; the raw view keeps both.
    #[test]
    fn priced_line_items_skip_only_the_free_one_time_line() {
        let session = serde_json::json!({"line_items": {"data": [
            {"quantity": 2, "amount_total": 0, "price": {"id": "price_seat", "type": "recurring", "unit_amount": 3000}},
            {"quantity": 1, "amount_total": 0, "price": {"id": "price_welcome", "type": "one_time", "unit_amount": 0}},
        ]}});
        let priced = parse_checkout_session_priced_line_items(&session).unwrap();
        assert_eq!(priced, vec![SessionLineItem { price_id: "price_seat".into(), quantity: 2 }]);
        assert_eq!(parse_checkout_session_line_items(&session).unwrap().len(), 2);

        // A PAID one-time line, and a line with no type info, are still counted.
        let session = serde_json::json!({"line_items": {"data": [
            {"quantity": 1, "amount_total": 0, "price": {"id": "price_seat", "type": "recurring", "unit_amount": 3000}},
            {"quantity": 1, "amount_total": 500, "price": {"id": "price_fee", "type": "one_time", "unit_amount": 500}},
            {"quantity": 1, "price": {"id": "price_untyped"}},
        ]}});
        assert_eq!(parse_checkout_session_priced_line_items(&session).unwrap().len(), 3);
    }

    /// A creator who already had a trial pays the seats on day 1.
    #[test]
    fn org_checkout_without_a_trial_carries_no_trial_fields() {
        let p = org_trial_checkout_params("cus_1", "price_std", 2, "acc_1", None, "https://a/s", "https://a/c");
        assert_absent(&p, "subscription_data[trial_period_days]");
        assert_absent(&p, "subscription_data[trial_settings][end_behavior][missing_payment_method]");
        assert_absent(&p, "custom_text[submit][message]");
        assert_pair(&p, "line_items[0][quantity]", "2");
        assert_pair(&p, "payment_method_collection", "always");
    }

    #[test]
    fn ending_a_trial_charges_now_and_lets_dunning_handle_a_failure() {
        let p = end_trial_now_params();
        assert_pair(&p, "trial_end", "now");
        assert_pair(&p, "payment_behavior", "allow_incomplete");
    }

    #[test]
    fn parse_invoice_view_reads_the_paying_payment_intent_in_both_shapes() {
        let legacy = serde_json::json!({"id": "in_1", "status": "paid", "amount_paid": 8000, "payment_intent": "pi_legacy"});
        assert_eq!(
            parse_invoice_view(&legacy).unwrap(),
            InvoiceView { id: "in_1".into(), status: "paid".into(), amount_paid: 8000, payment_intent: Some("pi_legacy".into()) }
        );
        let basil = serde_json::json!({"id": "in_2", "status": "paid", "amount_paid": 8000, "payments": {"data": [
            {"status": "canceled", "payment": {"type": "payment_intent", "payment_intent": "pi_failed"}},
            {"status": "paid", "payment": {"type": "payment_intent", "payment_intent": {"id": "pi_paid"}}},
        ]}});
        assert_eq!(parse_invoice_view(&basil).unwrap().payment_intent.as_deref(), Some("pi_paid"));
        let zero = serde_json::json!({"id": "in_3", "status": "paid"});
        let z = parse_invoice_view(&zero).unwrap();
        assert_eq!((z.amount_paid, z.payment_intent), (0, None));
        assert!(parse_invoice_view(&serde_json::json!({"id": "in_4"})).is_err(), "status is required");
    }

    #[test]
    fn parse_subscription_reads_the_trial_end() {
        let v = serde_json::json!({
            "id": "sub_t", "status": "trialing", "trial_end": 1_810_000_000,
            "items": {"data": [{"id": "si", "quantity": 2, "current_period_start": 1, "current_period_end": 2,
                                "price": {"id": "price_std"}}]}
        });
        let s = parse_subscription(&v).unwrap();
        assert_eq!(s.trial_end, Some(1_810_000_000));
        let mut v2 = v.clone();
        v2.as_object_mut().unwrap().remove("trial_end");
        assert_eq!(parse_subscription(&v2).unwrap().trial_end, None);
    }

    #[test]
    fn parse_payment_method_reads_a_card() {
        let pm = parse_payment_method(&serde_json::json!({
            "id": "pm_c", "type": "card",
            "card": {"brand": "visa", "last4": "4242", "exp_month": 4, "exp_year": 2028}
        }))
        .unwrap();
        assert!(pm.is_card());
        assert_eq!(pm.brand.as_deref(), Some("visa"));
        assert_eq!(pm.last4.as_deref(), Some("4242"));
        assert_eq!((pm.exp_month, pm.exp_year), (Some(4), Some(2028)));
        assert_eq!(pm.bank_name, None);
    }

    #[test]
    fn parse_payment_method_reads_a_card_payload_without_a_type() {
        let pm = parse_payment_method(&serde_json::json!({
            "id": "pm_c", "card": {"brand": "visa", "last4": "4242", "exp_month": 4, "exp_year": 2028}
        }))
        .unwrap();
        assert_eq!(pm.kind, "card");
    }

    #[test]
    fn parse_payment_method_reads_a_us_bank_account() {
        let pm = parse_payment_method(&serde_json::json!({
            "id": "pm_b", "type": "us_bank_account",
            "us_bank_account": {"bank_name": "Example Bank", "last4": "4242", "account_type": "checking"}
        }))
        .unwrap();
        assert!(!pm.is_card());
        assert_eq!(pm.kind, "us_bank_account");
        assert_eq!(pm.bank_name.as_deref(), Some("Example Bank"));
        assert_eq!(pm.last4.as_deref(), Some("4242"));
        assert_eq!(pm.brand, None);
    }

    #[test]
    fn parse_payment_method_reads_link_without_a_last4() {
        let pm = parse_payment_method(&serde_json::json!({
            "id": "pm_l", "type": "link", "link": {"email": "a@example.com"}
        }))
        .unwrap();
        assert_eq!(pm.kind, "link");
        assert_eq!(pm.email.as_deref(), Some("a@example.com"));
        assert_eq!(pm.last4, None);
    }

    #[test]
    fn parse_payment_method_still_rejects_a_card_without_its_fields() {
        assert!(parse_payment_method(&serde_json::json!({"id": "pm_c", "type": "card", "card": {}})).is_err());
    }

    #[test]
    fn subscription_checkout_wire_pairs() {
        let p = checkout_subscription_params(
            "cus_1",
            "price_standard",
            "acc_123",
            "https://app/success",
            "https://app/cancel",
            true,
        );
        assert_pair(&p, "mode", "subscription");
        assert_pair(&p, "customer", "cus_1");
        assert_pair(&p, "line_items[0][price]", "price_standard");
        // Checkout buys exactly ONE seat — hardcoded, not a parameter.
        assert_pair(&p, "line_items[0][quantity]", "1");
        assert_pair(&p, "client_reference_id", "acc_123");
        assert_pair(&p, "subscription_data[metadata][account_id]", "acc_123");
        assert_pair(&p, "allow_promotion_codes", "true");
    }

    /// Assert BY NAME: PR G's auto-refill is silently impossible without
    /// this exact field — a pack bought without `setup_future_usage` leaves
    /// no reusable payment method.
    #[test]
    fn pack_checkout_sets_setup_future_usage_off_session() {
        let p = checkout_payment_params(
            "cus_1",
            "price_pack_500",
            1,
            "acc_123",
            "https://app/success",
            "https://app/cancel",
        );
        assert_pair(&p, "mode", "payment");
        assert_pair(&p, "payment_intent_data[setup_future_usage]", "off_session");
        assert_pair(&p, "payment_intent_data[metadata][account_id]", "acc_123");
        assert_pair(&p, "line_items[0][quantity]", "1");
    }

    /// Delayed payment methods complete a session unpaid; packs are card only.
    #[test]
    fn pack_checkout_is_card_only() {
        let p = checkout_payment_params("cus_1", "price_pack_500", 1, "acc_123", "https://a/s", "https://a/c");
        assert_pair(&p, "payment_method_types[0]", "card");
    }

    #[test]
    fn session_retrieve_expands_line_items() {
        let q = session_retrieve_query();
        assert_pair(&q, "expand[0]", "line_items");
    }

    /// Getting upgrade/downgrade proration backwards refunds money the plan
    /// says we keep (§1.7) — two separate named tests on purpose.
    #[test]
    fn tier_upgrade_uses_create_prorations() {
        let p = subscription_item_swap_params("price_premium", "create_prorations");
        assert_pair(&p, "proration_behavior", "create_prorations");
        assert_pair(&p, "price", "price_premium");
    }

    #[test]
    fn tier_downgrade_uses_proration_behavior_none() {
        let p = subscription_item_swap_params("price_standard", "none");
        assert_pair(&p, "proration_behavior", "none");
    }

    #[test]
    fn seat_quantity_update_wire_pairs() {
        let p = subscription_item_quantity_params(3, "create_prorations");
        assert_pair(&p, "quantity", "3");
        assert_pair(&p, "proration_behavior", "create_prorations");
    }

    #[test]
    fn off_session_payment_intent_wire_pairs() {
        let p = payment_intent_off_session_params(
            "cus_1",
            1100,
            "pm_1",
            &[("account_id", "acc_123"), ("period_start", "1700000000")],
        );
        assert_pair(&p, "off_session", "true");
        assert_pair(&p, "confirm", "true");
        assert_pair(&p, "currency", "usd");
        assert_pair(&p, "amount", "1100");
        assert_pair(&p, "payment_method", "pm_1");
        assert_pair(&p, "metadata[account_id]", "acc_123");
        assert_pair(&p, "metadata[period_start]", "1700000000");
    }

    // ── StripeError classification ──────────────────────────────────────

    #[test]
    fn is_transient_classifies_errors() {
        assert!(StripeError::Timeout.is_transient());
        assert!(StripeError::Transport {
            public: "x",
            request_id: None
        }
        .is_transient());
        assert!(StripeError::RateLimited {
            retry_after: None,
            request_id: None
        }
        .is_transient());
        assert!(StripeError::Api {
            status: 500,
            kind: String::new(),
            code: None,
            param: None,
            message: String::new(),
            request_id: None
        }
        .is_transient());
        assert!(!StripeError::Api {
            status: 400,
            kind: String::new(),
            code: None,
            param: None,
            message: String::new(),
            request_id: None
        }
        .is_transient());
        assert!(!StripeError::Auth { request_id: None }.is_transient());
        assert!(!StripeError::NotConfigured.is_transient());
        assert!(!StripeError::IdempotencyConflict {
            key: "k".into(),
            request_id: None
        }
        .is_transient());
        assert!(!StripeError::CardDeclined {
            code: "c".into(),
            decline_code: None,
            message: "m".into(),
            request_id: None
        }
        .is_transient());
        assert!(!StripeError::Parse("x".into()).is_transient());
    }

    #[test]
    fn parse_error_body_maps_card_error_regardless_of_status() {
        let body = r#"{"error":{"type":"card_error","code":"card_declined","decline_code":"insufficient_funds","message":"Your card has insufficient funds."}}"#;
        let err = parse_error_body(402, body, None, None, None);
        match err {
            StripeError::CardDeclined {
                code,
                decline_code,
                message,
                ..
            } => {
                assert_eq!(code, "card_declined");
                assert_eq!(decline_code.as_deref(), Some("insufficient_funds"));
                assert!(message.contains("insufficient funds"));
            }
            other => panic!("expected CardDeclined, got {other:?}"),
        }
    }

    #[test]
    fn parse_error_body_maps_idempotency_conflict_and_carries_the_key() {
        let body = r#"{"error":{"type":"idempotency_error","message":"Keys for idempotent requests can only be used with the same parameters they were first used with."}}"#;
        let err = parse_error_body(409, body, None, Some("refill:acc_1:1700000000:0"), None);
        match err {
            StripeError::IdempotencyConflict { key, .. } => {
                assert_eq!(key, "refill:acc_1:1700000000:0");
            }
            other => panic!("expected IdempotencyConflict, got {other:?}"),
        }
    }

    #[test]
    fn parse_error_body_maps_401_to_auth() {
        let body = r#"{"error":{"type":"invalid_request_error","message":"Invalid API Key provided"}}"#;
        assert!(matches!(
            parse_error_body(401, body, None, None, None),
            StripeError::Auth { .. }
        ));
    }

    #[test]
    fn parse_error_body_maps_429_to_rate_limited_carrying_retry_after() {
        let body = r#"{"error":{"type":"rate_limit_error","message":"Too many requests"}}"#;
        let err = parse_error_body(429, body, None, None, Some(Duration::from_secs(3)));
        match err {
            StripeError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_secs(3)));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn parse_error_body_falls_back_to_api_for_unmapped_status() {
        let body = r#"{"error":{"type":"invalid_request_error","message":"No such price"}}"#;
        match parse_error_body(400, body, None, None, None) {
            StripeError::Api { status, message, .. } => {
                assert_eq!(status, 400);
                assert!(message.contains("No such price"));
            }
            other => panic!("expected Api, got {other:?}"),
        }
    }

    #[test]
    fn is_not_found_matches_only_404_api_errors() {
        assert!(StripeError::Api {
            status: 404,
            kind: String::new(),
            code: None,
            param: None,
            message: String::new(),
            request_id: None
        }
        .is_not_found());
        assert!(!StripeError::Api {
            status: 400,
            kind: String::new(),
            code: None,
            param: None,
            message: String::new(),
            request_id: None
        }
        .is_not_found());
        assert!(!StripeError::Timeout.is_not_found());
    }

    // ── livemode / base URL ──────────────────────────────────────────────

    #[test]
    fn livemode_detected_from_key_prefix() {
        let live = StripeClient::new(reqwest::Client::new(), "sk_live_abc");
        let test = StripeClient::new(reqwest::Client::new(), "sk_test_abc");
        assert!(live.livemode());
        assert!(!test.livemode());
    }

    /// A restricted key carries the same `_live_`/`_test_` mode marker as an
    /// unrestricted one — `livemode()` must not special-case the `rk_`
    /// family, or a restricted live key's objects would be mis-tagged
    /// `env=test` while actually mutating the live account.
    #[test]
    fn livemode_detected_for_restricted_keys_too() {
        let live = StripeClient::new(reqwest::Client::new(), "rk_live_abc");
        let test = StripeClient::new(reqwest::Client::new(), "rk_test_abc");
        assert!(live.livemode());
        assert!(!test.livemode());
    }

    #[test]
    fn default_base_url_is_the_real_api() {
        let c = StripeClient::new(reqwest::Client::new(), "sk_test_x");
        assert_eq!(c.base_url, "https://api.stripe.com");
    }

    #[test]
    fn with_base_url_overrides_for_tests() {
        let c = StripeClient::new(reqwest::Client::new(), "sk_test_x")
            .with_base_url("http://localhost:9090");
        assert_eq!(c.base_url, "http://localhost:9090");
    }

    // ── Retry policy ──────────────────────────────────────────────────────

    #[test]
    fn retry_policy_defaults() {
        let _guard = RETRY_ENV_LOCK.blocking_lock();
        std::env::remove_var("STRIPE_MAX_RETRIES");
        std::env::remove_var("STRIPE_RETRY_BASE_MS");
        std::env::remove_var("STRIPE_RETRY_MAX_DELAY_MS");
        let p = RetryPolicy::from_env();
        assert_eq!(p.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(p.base, Duration::from_millis(DEFAULT_RETRY_BASE_MS));
        assert_eq!(p.max_delay, Duration::from_millis(DEFAULT_RETRY_MAX_DELAY_MS));
    }

    // ── Layer 2: recorded-fixture response parsing ──────────────────────

    #[test]
    fn parses_customer_create_fixture() {
        let raw = include_str!("testdata/customer_create.json");
        let v: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(req_str(&v, "id").unwrap(), "cus_TEST0000000001");
    }

    #[test]
    fn parses_checkout_session_create_fixture() {
        let raw = include_str!("testdata/checkout_session_create.json");
        let v: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(req_str(&v, "id").unwrap(), "cs_test_TEST0000000001");
        assert!(req_str(&v, "url").unwrap().starts_with("https://checkout.stripe.com/"));
    }

    /// PR D's real parser, not a raw-shape assertion — see this fixture's own
    /// README note, which this test satisfies. Proves
    /// [`parse_checkout_session_line_items`] returns BOTH entries in order,
    /// with the right `price_id`/`quantity` pairs, so a caller that only
    /// looked at index 0 would silently under-credit a two-pack purchase.
    #[test]
    fn parses_checkout_session_completed_expanded_fixture_multi_line_item() {
        let raw = include_str!("testdata/checkout_session_completed_expanded.json");
        let v: Value = serde_json::from_str(raw).unwrap();
        let items = parse_checkout_session_line_items(&v)
            .expect("must parse a real two-item checkout session");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].price_id, "price_pack_500_test");
        assert_eq!(items[0].quantity, 1);
        assert_eq!(items[1].price_id, "price_pack_2000_test");
        assert_eq!(items[1].quantity, 2);
    }

    /// THE important fixture: mixed seat tiers (§1.4) make a two-item
    /// subscription the NORMAL case, and this is what kills any
    /// `items.data[0]`-only assumption. Also pins the period fields to
    /// `items.data[].current_period_*`, per `routes::stripe_webhook`'s own
    /// documented live-probing finding.
    #[test]
    fn parses_subscription_two_items_fixture() {
        let raw = include_str!("testdata/subscription_two_items.json");
        let v: Value = serde_json::from_str(raw).unwrap();
        let sub = parse_subscription(&v).expect("must parse a real two-item subscription");
        assert_eq!(sub.items.len(), 2);
        assert_eq!(sub.items[0].price_id, "price_seat_standard_test");
        assert_eq!(sub.items[0].quantity, 3);
        assert_eq!(sub.items[1].price_id, "price_seat_premium_test");
        assert_eq!(sub.items[1].quantity, 2);
        // Non-zero, real epoch seconds — proves these came off items.data[],
        // not a (missing) subscription-level field silently defaulting.
        assert!(sub.items[0].current_period_start > 0);
        assert!(sub.items[0].current_period_end > sub.items[0].current_period_start);
    }

    #[test]
    fn parses_invoice_list_fixture_with_pagination() {
        let raw = include_str!("testdata/invoice_list.json");
        let v: Value = serde_json::from_str(raw).unwrap();
        let data = v.get("data").and_then(|d| d.as_array()).unwrap();
        let invoices: Vec<InvoiceSummary> = data.iter().map(|i| parse_invoice(i).unwrap()).collect();
        assert_eq!(invoices.len(), 2);
        assert_eq!(invoices[0].status, "paid");
        assert!(invoices[0].hosted_invoice_url.is_some());
        assert!(req_bool(&v, "has_more").unwrap());
    }

    #[test]
    fn fixture_error_card_declined_402_maps_to_card_declined() {
        let raw = include_str!("testdata/error_card_declined_402.json");
        let v: Value = serde_json::from_str(raw).unwrap();
        let err_obj = v.get("error").unwrap();
        assert_eq!(err_obj.get("type").unwrap().as_str(), Some("card_error"));
        let err = parse_error_body(402, raw, None, None, None);
        assert!(matches!(err, StripeError::CardDeclined { .. }));
    }

    #[test]
    fn fixture_error_idempotency_409_maps_to_idempotency_conflict_not_transient() {
        let raw = include_str!("testdata/error_idempotency_409.json");
        let err = parse_error_body(409, raw, None, Some("refill:acc_1:0:0"), None);
        assert!(matches!(err, StripeError::IdempotencyConflict { .. }));
        assert!(!err.is_transient());
    }

    #[test]
    fn fixture_error_rate_limited_429_maps_to_rate_limited() {
        let raw = include_str!("testdata/error_rate_limited_429.json");
        let err = parse_error_body(429, raw, None, None, Some(Duration::from_secs(2)));
        assert!(matches!(err, StripeError::RateLimited { .. }));
        assert!(err.is_transient());
    }

    // ── Layer 3: local axum mock server — transport/retry behavior ──────
    // The repo's established substitute for a mock-HTTP crate (no wiremock/
    // httpmock/mockito dependency) — mirrors llm::openai's own test
    // module, which uses this exact pattern ~10 times.

    use axum::response::IntoResponse;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    async fn spawn_mock<F, Fut>(handler: F) -> (StripeClient, std::net::SocketAddr)
    where
        F: Fn(axum::http::HeaderMap) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = axum::response::Response> + Send,
    {
        let app = axum::Router::new().fallback(axum::routing::any(
            move |headers: axum::http::HeaderMap| {
                let handler = handler.clone();
                async move { handler(headers).await }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local mock server");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let client = StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"));
        (client, addr)
    }

    fn json_response(status: u16, body: serde_json::Value) -> axum::response::Response {
        (
            axum::http::StatusCode::from_u16(status).unwrap(),
            [("content-type", "application/json")],
            body.to_string(),
        )
            .into_response()
    }

    async fn tiny_backoff_guard() -> tokio::sync::MutexGuard<'static, ()> {
        let guard = RETRY_ENV_LOCK.lock().await;
        std::env::set_var("STRIPE_RETRY_BASE_MS", "1");
        std::env::set_var("STRIPE_RETRY_MAX_DELAY_MS", "5");
        guard
    }

    fn clear_retry_env() {
        std::env::remove_var("STRIPE_MAX_RETRIES");
        std::env::remove_var("STRIPE_RETRY_BASE_MS");
        std::env::remove_var("STRIPE_RETRY_MAX_DELAY_MS");
    }

    /// Proves the `Retry-After` floor from `backoff()` is actually honoured,
    /// not silently discarded by a final `.min(max_delay)` clamp (the exact
    /// bug this test used to pass through — see this module's PR-review
    /// history). `STRIPE_RETRY_MAX_DELAY_MS` is set well BELOW the
    /// `Retry-After` the mock sends, so a passing test here is only possible
    /// if the floor is genuinely allowed to exceed `max_delay`: we assert
    /// the actual wall-clock elapsed time is at least the floor, not just
    /// that the call eventually succeeded. `Retry-After` is whole seconds
    /// (HTTP/Stripe convention, `parse_retry_after` above parses it as
    /// such), so `1` second is the smallest floor expressible here — still
    /// tiny next to STRIPE_RETRY_MAX_DELAY_MS=5ms, which is the point.
    #[tokio::test]
    async fn retries_429_honouring_retry_after_floor_then_succeeds() {
        let _guard = RETRY_ENV_LOCK.lock().await;
        // Deliberately max_delay << the Retry-After floor below: our own
        // jittered backoff alone would never sleep this long, so if the
        // assertion below passes, the floor — not the jitter — caused it.
        std::env::set_var("STRIPE_RETRY_BASE_MS", "1");
        std::env::set_var("STRIPE_RETRY_MAX_DELAY_MS", "5");
        const FLOOR_MS: u64 = 1000;

        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let (client, _addr) = spawn_mock(move |_headers| {
            let a = a.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    (
                        axum::http::StatusCode::TOO_MANY_REQUESTS,
                        [("retry-after", "1")],
                        json_response(429, serde_json::json!({"error":{"type":"rate_limit_error","message":"slow down"}})),
                    )
                        .into_response()
                } else {
                    json_response(200, serde_json::json!({"id": "cus_ok"}))
                }
            }
        })
        .await;

        let started = std::time::Instant::now();
        let v = client
            .post_form("/v1/customers", &Params::new(), Idem::Key("cust:acc_1"))
            .await
            .expect("should retry past the 429 and succeed");
        let elapsed = started.elapsed();

        assert_eq!(v["id"], "cus_ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        // Small tolerance below the exact floor for scheduler/timer slop —
        // still tight enough that only the floor being honoured (not the
        // 5ms jittered backoff) could satisfy it.
        assert!(
            elapsed >= Duration::from_millis(FLOOR_MS.saturating_sub(50)),
            "expected the Retry-After floor (~{FLOOR_MS}ms) to be honoured despite \
             STRIPE_RETRY_MAX_DELAY_MS=5, got elapsed={elapsed:?} — the floor was discarded"
        );
        clear_retry_env();
    }

    /// `backoff()` fails fast (returns `None`) rather than sleeping when
    /// `Retry-After` exceeds [`MAX_RETRY_AFTER_HONOURED`] — a hostile or
    /// overly conservative server value must not block a synchronous user
    /// request for an unbounded time. See that constant's doc comment.
    #[test]
    fn backoff_returns_none_when_retry_after_exceeds_the_absolute_ceiling() {
        let policy = RetryPolicy {
            max_attempts: 3,
            base: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        };
        let over_ceiling = MAX_RETRY_AFTER_HONOURED + Duration::from_secs(1);
        assert!(policy.backoff(0, Some(over_ceiling)).is_none());
        // Exactly at the ceiling is still honoured — only strictly over it
        // fails fast.
        assert!(policy.backoff(0, Some(MAX_RETRY_AFTER_HONOURED)).is_some());
    }

    /// A `Retry-After` past the ceiling makes `execute()` fail fast with the
    /// original `RateLimited` error instead of sleeping — proven end to end
    /// through a real (mocked) 429 response, not just the pure `backoff()`
    /// unit above.
    #[tokio::test]
    async fn retry_after_past_the_ceiling_fails_fast_instead_of_sleeping() {
        let _guard = tiny_backoff_guard().await;
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let over_ceiling_secs = MAX_RETRY_AFTER_HONOURED.as_secs() + 60;
        let (client, _addr) = spawn_mock(move |_headers| {
            let a = a.clone();
            let secs = over_ceiling_secs;
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                (
                    axum::http::StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", secs.to_string())],
                    json_response(429, serde_json::json!({"error":{"type":"rate_limit_error","message":"slow down"}})),
                )
                    .into_response()
            }
        })
        .await;

        let started = std::time::Instant::now();
        let err = client
            .post_form("/v1/customers", &Params::new(), Idem::Key("cust:acc_1"))
            .await
            .expect_err("must fail fast, not hang");
        assert!(matches!(err, StripeError::RateLimited { .. }));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "must not retry once Retry-After exceeds the ceiling"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "must fail fast rather than sleeping anywhere near the requested Retry-After"
        );
        clear_retry_env();
    }

    #[tokio::test]
    async fn does_not_retry_permanent_auth_error() {
        let _guard = tiny_backoff_guard().await;
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let (client, _addr) = spawn_mock(move |_headers| {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                json_response(401, serde_json::json!({"error":{"type":"invalid_request_error","message":"bad key"}}))
            }
        })
        .await;

        let err = client
            .post_form("/v1/customers", &Params::new(), Idem::Key("cust:acc_1"))
            .await
            .expect_err("401 must fail, not succeed");
        assert!(matches!(err, StripeError::Auth { .. }));
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "permanent errors must not be retried");
        clear_retry_env();
    }

    #[tokio::test]
    async fn card_declined_402_fails_after_exactly_one_attempt() {
        let _guard = tiny_backoff_guard().await;
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let (client, _addr) = spawn_mock(move |_headers| {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                json_response(
                    402,
                    serde_json::json!({"error":{"type":"card_error","code":"card_declined","message":"declined"}}),
                )
            }
        })
        .await;

        let err = client
            .post_form(
                "/v1/payment_intents",
                &Params::new(),
                Idem::Key("refill:acc_1:0:0"),
            )
            .await
            .expect_err("402 must fail");
        assert!(matches!(err, StripeError::CardDeclined { .. }));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        clear_retry_env();
    }

    #[tokio::test]
    async fn idempotency_conflict_409_fails_after_exactly_one_attempt() {
        let _guard = tiny_backoff_guard().await;
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let (client, _addr) = spawn_mock(move |_headers| {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                json_response(
                    409,
                    serde_json::json!({"error":{"type":"idempotency_error","message":"reused with a different request"}}),
                )
            }
        })
        .await;

        let err = client
            .post_form("/v1/customers", &Params::new(), Idem::Key("cust:acc_1"))
            .await
            .expect_err("409 idempotency conflict must fail");
        assert!(matches!(err, StripeError::IdempotencyConflict { .. }));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        clear_retry_env();
    }

    #[tokio::test]
    async fn a_keyed_post_is_retried_to_the_budget_on_server_errors() {
        let _guard = tiny_backoff_guard().await;
        std::env::set_var("STRIPE_MAX_RETRIES", "3");
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let (client, _addr) = spawn_mock(move |_headers| {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                json_response(500, serde_json::json!({"error":{"type":"api_error","message":"internal"}}))
            }
        })
        .await;

        let err = client
            .post_form("/v1/customers", &Params::new(), Idem::Key("cust:acc_1"))
            .await
            .expect_err("exhausted retries must still fail");
        assert!(matches!(err, StripeError::Api { status: 500, .. }));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "must retry to the configured budget (STRIPE_MAX_RETRIES=3)"
        );
        clear_retry_env();
    }

    /// THE headline test — this module's single most important invariant.
    /// Retrying an unkeyed POST on a transient-looking 500 would double-
    /// charge a customer; `Idem::NotIdempotent` must force exactly one
    /// attempt no matter what the failure was.
    #[tokio::test]
    async fn a_non_idempotent_post_is_never_retried_even_on_a_server_error() {
        let _guard = tiny_backoff_guard().await;
        std::env::set_var("STRIPE_MAX_RETRIES", "5");
        let attempts = Arc::new(AtomicU32::new(0));
        let a = attempts.clone();
        let (client, _addr) = spawn_mock(move |_headers| {
            let a = a.clone();
            async move {
                a.fetch_add(1, Ordering::SeqCst);
                json_response(500, serde_json::json!({"error":{"type":"api_error","message":"internal"}}))
            }
        })
        .await;

        let err = client
            .post_form("/v1/checkout/sessions", &Params::new(), Idem::NotIdempotent)
            .await
            .expect_err("500 must still fail");
        assert!(matches!(err, StripeError::Api { status: 500, .. }));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "Idem::NotIdempotent must be attempted EXACTLY ONCE, regardless of STRIPE_MAX_RETRIES \
             or how transient the failure looks — retrying would double-charge a customer"
        );
        clear_retry_env();
    }

    /// A retry that re-rolled the key would be the same bug as generating
    /// one internally — the SAME `Idempotency-Key` must be sent on every
    /// attempt of a retried call.
    #[tokio::test]
    async fn the_same_idempotency_key_is_sent_on_every_retry_attempt() {
        let _guard = tiny_backoff_guard().await;
        let seen_keys: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::new(AtomicU32::new(0));
        let sk = seen_keys.clone();
        let a = attempts.clone();
        let (client, _addr) = spawn_mock(move |headers: axum::http::HeaderMap| {
            let sk = sk.clone();
            let a = a.clone();
            async move {
                let key = headers
                    .get("idempotency-key")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                sk.lock().unwrap().push(key);
                let n = a.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    json_response(500, serde_json::json!({"error":{"type":"api_error","message":"internal"}}))
                } else {
                    json_response(200, serde_json::json!({"id": "cus_ok"}))
                }
            }
        })
        .await;

        client
            .post_form("/v1/customers", &Params::new(), Idem::Key("cust:acc_1"))
            .await
            .expect("should eventually succeed");

        let keys = seen_keys.lock().unwrap();
        assert_eq!(keys.len(), 3, "expected 2 failures then a success");
        assert!(
            keys.iter().all(|k| k == "cust:acc_1"),
            "every attempt must carry the SAME idempotency key: {keys:?}"
        );
        clear_retry_env();
    }

    /// `Idem::NaturallyIdempotent` retries to the policy budget like a
    /// keyed POST, but sends NO `Idempotency-Key` header on any attempt —
    /// proves both halves at once (retried, and unkeyed).
    #[tokio::test]
    async fn naturally_idempotent_post_retries_without_an_idempotency_key_header() {
        let _guard = tiny_backoff_guard().await;
        std::env::set_var("STRIPE_MAX_RETRIES", "3");
        let seen_keys: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::new(AtomicU32::new(0));
        let sk = seen_keys.clone();
        let a = attempts.clone();
        let (client, _addr) = spawn_mock(move |headers: axum::http::HeaderMap| {
            let sk = sk.clone();
            let a = a.clone();
            async move {
                sk.lock()
                    .unwrap()
                    .push(headers.get("idempotency-key").map(|_| "present".to_string()));
                let n = a.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    json_response(500, serde_json::json!({"error":{"type":"api_error","message":"internal"}}))
                } else {
                    json_response(200, serde_json::json!({"id": "we_ok"}))
                }
            }
        })
        .await;

        client
            .post_form("/v1/webhook_endpoints/we_1", &Params::new(), Idem::NaturallyIdempotent)
            .await
            .expect("should eventually succeed");

        let keys = seen_keys.lock().unwrap();
        assert_eq!(keys.len(), 3, "expected 2 failures then a success — retried like a keyed POST");
        assert!(
            keys.iter().all(Option::is_none),
            "NaturallyIdempotent must never send an Idempotency-Key header: {keys:?}"
        );
        clear_retry_env();
    }

    /// `Request-Id` surfaces into the error variant on a non-2xx.
    #[tokio::test]
    async fn request_id_response_header_surfaces_into_the_error() {
        let _guard = tiny_backoff_guard().await;
        let (client, _addr) = spawn_mock(move |_headers| async move {
            (
                axum::http::StatusCode::UNAUTHORIZED,
                [("request-id", "req_test_123")],
                serde_json::json!({"error":{"type":"invalid_request_error","message":"bad key"}}).to_string(),
            )
                .into_response()
        })
        .await;

        let err = client
            .post_form("/v1/customers", &Params::new(), Idem::Key("cust:acc_1"))
            .await
            .expect_err("401 must fail");
        match err {
            StripeError::Auth { request_id } => {
                assert_eq!(request_id.as_deref(), Some("req_test_123"));
            }
            other => panic!("expected Auth, got {other:?}"),
        }
        clear_retry_env();
    }

    #[tokio::test]
    async fn connect_read_timeout_surfaces_as_timeout_error() {
        // A handler that never responds inside the client's own short
        // timeout — proves StripeError::Timeout (not a generic Transport
        // error) is what a stalled connection produces.
        let app = axum::Router::new().route(
            "/v1/customers",
            axum::routing::post(|| async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                json_response(200, serde_json::json!({"id": "cus_never"}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        let short_timeout_http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(200))
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let client = StripeClient::new(short_timeout_http, "sk_test_mock")
            .with_base_url(format!("http://{addr}"));

        let _guard = tiny_backoff_guard().await;
        let err = client
            .post_form("/v1/customers", &Params::new(), Idem::NotIdempotent)
            .await
            .expect_err("must time out");
        assert!(matches!(err, StripeError::Timeout), "got {err:?}");
        clear_retry_env();
    }

    // ── find_price_by_lookup_key: archived-price fallback ────────────────

    /// The active-only lookup misses, but a SECOND unfiltered lookup finds
    /// an archived price still holding the key — this must fail loudly with
    /// [`StripeError::ArchivedLookupKey`], not report `Ok(None)` (which the
    /// caller reads as "safe to create").
    #[tokio::test]
    async fn find_price_by_lookup_key_falls_through_to_archived_when_active_lookup_misses() {
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let app = axum::Router::new().route(
            "/v1/prices",
            axum::routing::get(move |uri: axum::http::Uri| {
                let cc = cc.clone();
                async move {
                    let n = cc.fetch_add(1, Ordering::SeqCst);
                    let query = uri.query().unwrap_or("");
                    if query.contains("active") {
                        assert_eq!(n, 0, "the active-filtered lookup must go first");
                        json_response(200, serde_json::json!({"data": []}))
                    } else {
                        assert_eq!(n, 1, "the unfiltered fallback lookup must go second");
                        json_response(
                            200,
                            serde_json::json!({"data": [{"id": "price_archived_1", "active": false}]}),
                        )
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let client =
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"));

        let err = client
            .find_price_by_lookup_key("cs_pro_v1")
            .await
            .expect_err("an archived price holding the key must fail loudly");
        match err {
            StripeError::ArchivedLookupKey { lookup_key, price_id } => {
                assert_eq!(lookup_key, "cs_pro_v1");
                assert_eq!(price_id, "price_archived_1");
            }
            other => panic!("expected ArchivedLookupKey, got {other:?}"),
        }
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "must issue both the active and the fallback unfiltered lookup"
        );
    }

    /// Truly absent (both lookups empty) must still report `Ok(None)` — the
    /// fallback lookup must not turn a genuine "not found" into an error.
    #[tokio::test]
    async fn find_price_by_lookup_key_returns_none_when_truly_absent() {
        let app = axum::Router::new().route(
            "/v1/prices",
            axum::routing::get(|| async { json_response(200, serde_json::json!({"data": []})) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let client =
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"));

        let result = client
            .find_price_by_lookup_key("cs_pro_v1")
            .await
            .expect("truly-absent must not error");
        assert!(result.is_none());
    }

    /// An active hit must short-circuit — no fallback lookup fired at all.
    #[tokio::test]
    async fn find_price_by_lookup_key_returns_active_hit_without_a_fallback_lookup() {
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let app = axum::Router::new().route(
            "/v1/prices",
            axum::routing::get(move || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    json_response(200, serde_json::json!({"data": [{"id": "price_active_1", "active": true}]}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let client =
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"));

        let result = client
            .find_price_by_lookup_key("cs_pro_v1")
            .await
            .expect("must not error")
            .expect("must find the active price");
        assert_eq!(result["id"], "price_active_1");
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "an active hit must not trigger the fallback lookup"
        );
    }

    #[test]
    fn get_and_delete_need_no_idem_and_are_conceptually_always_retryable() {
        // Documentation-as-test: `get`/`delete` have no `Idem` parameter at
        // all in their signature — this is enforced by the type system, not
        // exercised at runtime here. See `StripeClient::get`/`::delete`.
        fn assert_no_idem_param<F>(_f: F)
        where
            F: for<'a> Fn(&'a StripeClient, &'a str, &'a Params) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Value, StripeError>> + Send + 'a>,
            >,
        {
        }
        assert_no_idem_param(|c, p, q| Box::pin(c.get(p, q)));
        assert_no_idem_param(|c, p, q| Box::pin(c.delete(p, q)));
    }
    // ── Settings launch PR 3.3 ───────────────────────────────────────────

    #[test]
    fn portal_configuration_allows_card_invoices_and_period_end_cancel_only() {
        let p = portal_configuration_params();
        assert_pair(&p, "features[payment_method_update][enabled]", "true");
        assert_pair(&p, "features[invoice_history][enabled]", "true");
        assert_pair(&p, "features[subscription_cancel][enabled]", "true");
        assert_pair(&p, "features[subscription_cancel][mode]", "at_period_end");
        assert_pair(&p, "features[subscription_cancel][proration_behavior]", "none");
        // Seat and plan changes must stay out of the portal.
        assert_pair(&p, "features[subscription_update][enabled]", "false");
        assert_pair(&p, "features[customer_update][enabled]", "false");
        assert_pair(&p, "metadata[cs_portal_config]", "v1");
    }

    /// Stripe rejects `cancellation_reason[enabled]=false` without
    /// `[options]`, and we do not collect reasons, so no such key is sent.
    #[test]
    fn portal_configuration_sends_no_cancellation_reason_key() {
        let p = portal_configuration_params();
        assert!(
            pairs(&p).iter().all(|(k, _)| !k.contains("cancellation_reason")),
            "got {:?}",
            pairs(&p)
        );
    }

    /// Records every request (method, path+query, body) and answers with a
    /// canned JSON body.
    async fn spawn_capture(
        reply: serde_json::Value,
    ) -> (StripeClient, Arc<Mutex<Vec<(String, String, String)>>>) {
        let seen: Arc<Mutex<Vec<(String, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let app = axum::Router::new().fallback(axum::routing::any(
            move |req: axum::http::Request<axum::body::Body>| {
                let seen = seen2.clone();
                let reply = reply.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
                    seen.lock().unwrap().push((
                        parts.method.to_string(),
                        parts.uri.to_string(),
                        String::from_utf8_lossy(&bytes).into_owned(),
                    ));
                    json_response(200, reply)
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let client = StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"));
        (client, seen)
    }

    /// `POST /v1/billing/portal` and `provision_stripe` look the configuration
    /// up with `GET ...?active=true`. `is_active` is not a Stripe parameter.
    #[tokio::test]
    async fn find_portal_configuration_filters_with_active_not_is_active() {
        let (client, seen) = spawn_capture(serde_json::json!({
            "data": [{"id": "bpc_1", "metadata": {"cs_portal_config": "v1"}}]
        }))
        .await;
        let id = client.find_portal_configuration().await.unwrap();
        assert_eq!(id.as_deref(), Some("bpc_1"));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "GET");
        assert_eq!(seen[0].1, "/v1/billing_portal/configurations?active=true&limit=100");
    }

    /// The create call carries no `is_active` and no `cancellation_reason`;
    /// the update call converges with `active=true`.
    #[tokio::test]
    async fn portal_configuration_create_and_update_send_valid_form_params() {
        let (client, seen) = spawn_capture(serde_json::json!({"id": "bpc_1"})).await;
        client.create_portal_configuration().await.unwrap();
        client.update_portal_configuration("bpc_1").await.unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].1, "/v1/billing_portal/configurations");
        assert_eq!(seen[1].1, "/v1/billing_portal/configurations/bpc_1");
        for (_, _, body) in seen.iter() {
            assert!(!body.contains("is_active"), "body: {body}");
            assert!(!body.contains("cancellation_reason"), "body: {body}");
            assert!(body.contains("features%5Bsubscription_cancel%5D%5Bmode%5D=at_period_end"), "body: {body}");
        }
        assert!(!seen[0].2.contains("active="), "create must not set active: {}", seen[0].2);
        assert!(seen[1].2.contains("active=true"), "update body: {}", seen[1].2);
    }

    #[test]
    fn owned_portal_configuration_matches_on_the_marker_only() {
        assert!(is_owned_portal_configuration(&serde_json::json!({"metadata": {"cs_portal_config": "v1"}})));
        assert!(!is_owned_portal_configuration(&serde_json::json!({"metadata": {"cs_portal_config": "v0"}})));
        assert!(!is_owned_portal_configuration(&serde_json::json!({"metadata": {}})));
        assert!(!is_owned_portal_configuration(&serde_json::json!({"id": "bpc_default"})));
    }

    #[test]
    fn preview_params_cover_set_remove_and_add() {
        let none = preview_params("cus_1", "sub_1", None, 99, true);
        assert_pair(&none, "customer", "cus_1");
        assert_pair(&none, "subscription", "sub_1");
        assert_absent(&none, "subscription_details[items][0][id]");

        let set = preview_params("cus_1", "sub_1", Some(PreviewChange::SetQuantity { item_id: "si_1", quantity: 7 }), 99, true);
        assert_pair(&set, "subscription_details[items][0][id]", "si_1");
        assert_pair(&set, "subscription_details[items][0][quantity]", "7");
        assert_pair(&set, "subscription_details[proration_behavior]", "create_prorations");
        assert_pair(&set, "subscription_details[proration_date]", "99");

        let rm = preview_params("cus_1", "sub_1", Some(PreviewChange::RemoveItem { item_id: "si_1" }), 99, false);
        assert_pair(&rm, "subscription_details[items][0][deleted]", "true");
        // A reduction previews with `none` and carries no proration date.
        assert_pair(&rm, "subscription_details[proration_behavior]", "none");
        assert_absent(&rm, "subscription_details[proration_date]");

        let add = preview_params("cus_1", "sub_1", Some(PreviewChange::AddItem { price_id: "price_x", quantity: 2 }), 99, true);
        assert_pair(&add, "subscription_details[items][0][price]", "price_x");
        assert_pair(&add, "subscription_details[items][0][quantity]", "2");
        assert_absent(&add, "subscription_details[items][0][id]");
    }

    #[test]
    fn invoice_preview_reads_proration_from_either_api_shape() {
        let v = serde_json::json!({
            "total": 1500, "amount_due": 1500, "currency": "usd",
            "lines": {"data": [
                {"amount": 1000, "proration": false},
                {"amount": 500, "parent": {"subscription_item_details": {"proration": true}}},
                {"amount": 7},
            ]}
        });
        let p = parse_invoice_preview(&v).unwrap();
        assert_eq!(p.total, 1500);
        assert_eq!(
            p.lines,
            vec![
                PreviewLine { amount: 1000, proration: false },
                PreviewLine { amount: 500, proration: true },
                PreviewLine { amount: 7, proration: false },
            ]
        );
        assert!(parse_invoice_preview(&serde_json::json!({"total": 1})).is_err());
    }

    #[test]
    fn cancel_state_and_reference_ids_parse_both_shapes() {
        assert_eq!(parse_cancel_state(&serde_json::json!({})), (false, None));
        assert_eq!(
            parse_cancel_state(&serde_json::json!({"cancel_at_period_end": true, "cancel_at": 5})),
            (true, Some(5))
        );
        assert_eq!(reference_id_value(Some(&serde_json::json!("pm_1"))).as_deref(), Some("pm_1"));
        assert_eq!(reference_id_value(Some(&serde_json::json!({"id": "pm_2"}))).as_deref(), Some("pm_2"));
        assert_eq!(reference_id_value(Some(&serde_json::json!(null))), None);
        assert_eq!(reference_id_value(None), None);
    }
}
