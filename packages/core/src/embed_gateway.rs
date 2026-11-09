//! The embedding gateway: the one door to an embedding provider.
//!
//! Every embedding goes through [`submit`], or through [`plan`] then [`send`]
//! when a caller must bind each provider request to its own receipt
//! (`managed::embedding` in the backend). The gateway owns, in one place:
//!
//! - **size**: every text becomes [`EmbedInput`]s of at most
//!   [`EMBED_INPUT_MAX_BYTES`]. A longer text is split, never truncated, and
//!   its pieces' vectors are pooled back into one vector for the text.
//! - **batching**: texts pack into requests bounded by count and bytes. A
//!   split text goes in a request of its own, so it can only fail itself.
//! - **the provider call**: [`ProviderPermit`] is made here and nowhere
//!   else, so no other code can call a provider.
//! - **money**: a paid provider is sent to only under [`Purpose::Managed`]
//!   (a receipted send gate) or [`Purpose::Metered`] (every attempt
//!   reported). [`Purpose::Unbilled`] on a paid provider is refused.
//! - **error classification**: [`classify`]. Only 429, 5xx and transport
//!   failures are transient. Anything else, every other 4xx included, is
//!   permanent for the inputs that got it.
//!
//! Attempt counting and backoff for stored rows belong to the backend's
//! `embedding` module, which keys them on [`Disposition`].

use std::fmt;
use std::ops::Range;

pub use crate::embed::EMBED_INPUT_MAX_BYTES;
use crate::embed::{EmbedError, EmbedInput, EmbedProvider, ProviderPermit};
use crate::send_gate::{self, AttemptRecord, Deny, GateFuture, RefuseCode, SendGate};

/// Most bytes one provider request carries. OpenAI caps one request at
/// 300,000 tokens summed over its inputs, and bytes bound tokens from above.
/// A single text longer than this is refused as permanently too long.
pub const REQUEST_MAX_BYTES: usize = 256 * 1024;

/// Most texts one request from [`submit`] carries. Bounds the in-process
/// model's attention memory as well as the HTTP body.
pub const SUBMIT_BATCH: usize = 32;

// ── Classification ────────────────────────────────────────────────────────────

/// Whether a failed input is worth sending again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// The provider or the path to it was unavailable; the same input may
    /// succeed later.
    Transient,
    /// The same input fails the same way every time.
    Permanent,
}

/// The one rule for whether an embedding failure is retried.
///
/// Transient: HTTP 429 and 5xx, a transport failure (connect, timeout, a
/// dropped response), and a send gate's hold or indecision, which clear on
/// their own. Everything else is permanent: every other 4xx, an over-limit
/// input, a gate's refusal, a missing model, and an answer that cannot be
/// used. Unknown means permanent, so no failure can loop forever.
pub fn classify(error: &EmbedError) -> Disposition {
    match error {
        EmbedError::Http { status, .. } if *status == 429 || (500..=599).contains(status) => {
            Disposition::Transient
        }
        EmbedError::Transport(_) => Disposition::Transient,
        EmbedError::Denied(Deny::Hold(_) | Deny::Unavailable) => Disposition::Transient,
        EmbedError::Http { .. }
        | EmbedError::InputTooLong(_)
        | EmbedError::Denied(Deny::Refuse(_))
        | EmbedError::ModelNotLoaded(_)
        | EmbedError::InferenceFailed(_) => Disposition::Permanent,
    }
}

/// A failed request or text, with its [`classify`] verdict.
#[derive(Clone, Debug)]
pub struct Failure {
    pub disposition: Disposition,
    pub error: EmbedError,
}

impl Failure {
    fn permanent(error: EmbedError) -> Self {
        Failure {
            disposition: Disposition::Permanent,
            error,
        }
    }

    pub fn is_permanent(&self) -> bool {
        self.disposition == Disposition::Permanent
    }
}

impl From<EmbedError> for Failure {
    fn from(error: EmbedError) -> Self {
        Failure {
            disposition: classify(&error),
            error,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.disposition {
            Disposition::Transient => "transient",
            Disposition::Permanent => "permanent",
        };
        write!(f, "{} ({kind})", self.error)
    }
}

impl std::error::Error for Failure {}

// ── Purpose ───────────────────────────────────────────────────────────────────

/// Why an embedding is not billed. Each is a model with no paid transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unbilled {
    /// The in-process ONNX model (the embed worker and the ingest route's
    /// local embed).
    OnDevice,
    /// The offline retrieval eval harness (hash or ONNX embedder).
    Eval,
}

/// Records one paid HTTP attempt, for a send that is metered but not
/// receipted. The backend's implementation writes an `llm_usage` row.
pub trait AttemptMeter: Send + Sync {
    fn record<'a>(&'a self, input_bytes: usize, attempt: &'a AttemptRecord) -> GateFuture<'a, ()>;
}

/// What an embedding is for, which decides how it may be paid for.
#[derive(Clone, Copy)]
pub enum Purpose<'a> {
    /// Managed corpus embedding. The gate reserves, receipts and settles
    /// exactly one provider request, so a managed caller sends each
    /// [`Request`] from [`plan`] under its own gate.
    Managed(&'a dyn SendGate),
    /// A paid send outside the managed purpose (Ask and search queries,
    /// digest titles, `/v1/embed`). Every HTTP attempt is reported to the
    /// meter.
    Metered(&'a dyn AttemptMeter),
    /// A model with no paid transport. Refused on a paid provider.
    Unbilled(Unbilled),
}

// ── Planning ──────────────────────────────────────────────────────────────────

/// One provider request: bounded inputs for one or more texts.
#[derive(Debug)]
pub struct Request {
    items: Vec<usize>,
    inputs: Vec<EmbedInput>,
    spans: Vec<Range<usize>>,
}

impl Request {
    fn new() -> Self {
        Request {
            items: Vec::new(),
            inputs: Vec::new(),
            spans: Vec::new(),
        }
    }

    fn push(&mut self, item: usize, pieces: Vec<EmbedInput>) {
        let start = self.inputs.len();
        self.inputs.extend(pieces);
        self.items.push(item);
        self.spans.push(start..self.inputs.len());
    }

    /// Indexes, into the texts given to [`plan`], of the texts this request
    /// carries, in the order [`send`] returns their vectors.
    pub fn items(&self) -> &[usize] {
        &self.items
    }

    /// The bounded inputs sent, a split text's pieces in a row.
    pub fn inputs(&self) -> &[EmbedInput] {
        &self.inputs
    }

    pub fn input_bytes(&self) -> usize {
        self.inputs.iter().map(|i| i.as_str().len()).sum()
    }
}

/// [`plan`]'s result: the requests to send, and the texts no request can
/// carry, each with its permanent failure.
#[derive(Debug)]
pub struct Plan {
    pub requests: Vec<Request>,
    pub refused: Vec<(usize, Failure)>,
}

/// Bound every text and group the texts into requests of at most
/// `max_texts` texts and [`REQUEST_MAX_BYTES`] bytes. Texts within the input
/// limit share requests, in order; each text that had to be split follows in
/// a request of its own. A whitespace-only text over the limit, or a text
/// over [`REQUEST_MAX_BYTES`], is refused, never cut down.
pub fn plan(texts: &[&str], max_texts: usize) -> Plan {
    let max_texts = max_texts.max(1);
    let mut bounded: Vec<Request> = Vec::new();
    let mut bounded_bytes = 0usize;
    let mut isolated = Vec::new();
    let mut refused = Vec::new();
    for (item, text) in texts.iter().enumerate() {
        let pieces = EmbedInput::split(text);
        let bytes: usize = pieces.iter().map(|p| p.as_str().len()).sum();
        if pieces.is_empty() {
            refused.push((
                item,
                Failure::permanent(EmbedError::InferenceFailed(
                    "nothing to embed: the text is only whitespace".into(),
                )),
            ));
            continue;
        }
        if bytes > REQUEST_MAX_BYTES {
            refused.push((
                item,
                Failure::permanent(EmbedError::InputTooLong(format!(
                    "{} bytes is over the {REQUEST_MAX_BYTES}-byte request limit",
                    text.len()
                ))),
            ));
            continue;
        }
        if pieces.len() > 1 {
            let mut request = Request::new();
            request.push(item, pieces);
            isolated.push(request);
            continue;
        }
        let full = bounded.last().is_none_or(|r| {
            r.items.len() >= max_texts || bounded_bytes + bytes > REQUEST_MAX_BYTES
        });
        if full {
            bounded.push(Request::new());
            bounded_bytes = 0;
        }
        bounded_bytes += bytes;
        bounded.last_mut().expect("pushed above").push(item, pieces);
    }
    bounded.extend(isolated);
    Plan {
        requests: bounded,
        refused,
    }
}

// ── Sending ───────────────────────────────────────────────────────────────────

/// Send one planned request and return one vector per text it carries, in
/// [`Request::items`] order. A split text's piece vectors are averaged,
/// weighted by piece length, and re-normalised.
pub async fn send(
    provider: &dyn EmbedProvider,
    purpose: Purpose<'_>,
    request: &Request,
) -> Result<Vec<Vec<f32>>, Failure> {
    let inputs = request.inputs.as_slice();
    let refused = || EmbedError::Denied(Deny::Refuse(RefuseCode::Ungated));
    let result = match purpose {
        Purpose::Managed(gate) => match provider.gated() {
            Some(gated) => gated.embed_batch_gated(inputs, gate, ProviderPermit::new()).await,
            None => Err(refused()),
        },
        Purpose::Metered(meter) => {
            let (result, attempts) = send_gate::with_attempt_log(
                provider.embed_batch(inputs, ProviderPermit::new()),
            )
            .await;
            let bytes = request.input_bytes();
            for attempt in &attempts {
                meter.record(bytes, attempt).await;
            }
            result
        }
        // A paid provider with no gate and no meter would be a send nobody
        // pays for or sees.
        Purpose::Unbilled(_) if provider.gated().is_some() => Err(refused()),
        Purpose::Unbilled(_) => provider.embed_batch(inputs, ProviderPermit::new()).await,
    };
    let vectors = result.map_err(Failure::from)?;
    if vectors.len() != inputs.len() {
        return Err(Failure::permanent(EmbedError::InferenceFailed(format!(
            "provider returned {} vectors for {} inputs",
            vectors.len(),
            inputs.len()
        ))));
    }
    Ok(request
        .spans
        .iter()
        .map(|span| pool(&vectors[span.clone()], &inputs[span.clone()]))
        .collect())
}

/// Embed `texts`, one vector each, in order. Plans with [`SUBMIT_BATCH`] and
/// sends every request; the first failure, or a text no request can carry,
/// fails the whole call before anything later is sent.
///
/// For [`Purpose::Managed`] use [`plan`] and [`send`]: a managed gate admits
/// one request, and a second request under it is refused.
pub async fn submit(
    provider: &dyn EmbedProvider,
    purpose: Purpose<'_>,
    texts: &[&str],
) -> Result<Vec<Vec<f32>>, Failure> {
    let plan = plan(texts, SUBMIT_BATCH);
    if let Some((_, failure)) = plan.refused.into_iter().next() {
        return Err(failure);
    }
    let mut out: Vec<Option<Vec<f32>>> = vec![None; texts.len()];
    for request in &plan.requests {
        let vectors = send(provider, purpose, request).await?;
        for (item, vector) in request.items.iter().zip(vectors) {
            out[*item] = Some(vector);
        }
    }
    out.into_iter()
        .map(|v| {
            v.ok_or_else(|| {
                Failure::permanent(EmbedError::InferenceFailed("a text was never sent".into()))
            })
        })
        .collect()
}

/// [`submit`] for one text.
pub async fn submit_one(
    provider: &dyn EmbedProvider,
    purpose: Purpose<'_>,
    text: &str,
) -> Result<Vec<f32>, Failure> {
    submit(provider, purpose, &[text])
        .await?
        .pop()
        .ok_or_else(|| Failure::permanent(EmbedError::InferenceFailed("empty embeddings batch".into())))
}

/// One vector for a text from its pieces' vectors: the length-weighted mean,
/// L2-normalised. A text of one piece keeps its vector unchanged.
fn pool(vectors: &[Vec<f32>], inputs: &[EmbedInput]) -> Vec<f32> {
    if vectors.len() == 1 {
        return vectors[0].clone();
    }
    let mut pooled = vec![0f32; vectors.first().map_or(0, Vec::len)];
    for (vector, input) in vectors.iter().zip(inputs) {
        let weight = input.as_str().len().max(1) as f32;
        for (out, x) in pooled.iter_mut().zip(vector) {
            *out += weight * x;
        }
    }
    let norm = pooled.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm.is_finite() && norm > 0.0 {
        for x in &mut pooled {
            *x /= norm;
        }
    }
    pooled
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::embed::{EmbedBatchFuture, GatedEmbed, Sealed, EMBED_DIM};
    use crate::send_gate::{SendOutcome, Unmanaged};

    /// Answers every input with a unit vector on an axis picked from its
    /// first byte, or with `fail`. Records the inputs of each call.
    struct Fake {
        paid: bool,
        fail: Option<EmbedError>,
        calls: Mutex<Vec<Vec<String>>>,
    }

    impl Fake {
        fn new(paid: bool) -> Self {
            Fake {
                paid,
                fail: None,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn failing(error: EmbedError) -> Self {
            Fake {
                fail: Some(error),
                ..Fake::new(false)
            }
        }

        fn answer(&self, inputs: &[EmbedInput]) -> Result<Vec<Vec<f32>>, EmbedError> {
            self.calls
                .lock()
                .unwrap()
                .push(inputs.iter().map(|i| i.as_str().to_string()).collect());
            if let Some(error) = &self.fail {
                return Err(error.clone());
            }
            Ok(inputs
                .iter()
                .map(|i| {
                    let mut v = vec![0f32; EMBED_DIM];
                    v[usize::from(i.as_str().bytes().next().unwrap_or(0)) % EMBED_DIM] = 1.0;
                    v
                })
                .collect())
        }
    }

    impl EmbedProvider for Fake {
        fn embed_batch<'a>(
            &'a self,
            inputs: &'a [EmbedInput],
            _: ProviderPermit,
        ) -> EmbedBatchFuture<'a> {
            Box::pin(async move {
                if self.paid {
                    // The unmanaged paid path settles at the attempt log.
                    let sent = send_gate::acquire(&Unmanaged, 1).await.unwrap().into_sent();
                    send_gate::settle(&Unmanaged, sent, SendOutcome::NotGenerated).await;
                }
                self.answer(inputs)
            })
        }

        fn gated(&self) -> Option<&dyn GatedEmbed> {
            self.paid.then_some(self as &dyn GatedEmbed)
        }
    }

    impl Sealed for Fake {}

    impl GatedEmbed for Fake {
        fn embed_batch_gated<'a>(
            &'a self,
            inputs: &'a [EmbedInput],
            _: &'a dyn SendGate,
            _: ProviderPermit,
        ) -> EmbedBatchFuture<'a> {
            Box::pin(async move { self.answer(inputs) })
        }
    }

    #[derive(Default)]
    struct Meter(Mutex<Vec<(usize, AttemptRecord)>>);

    impl AttemptMeter for Meter {
        fn record<'a>(&'a self, bytes: usize, attempt: &'a AttemptRecord) -> GateFuture<'a, ()> {
            Box::pin(async move { self.0.lock().unwrap().push((bytes, attempt.clone())) })
        }
    }

    const LOCAL: Purpose<'static> = Purpose::Unbilled(Unbilled::OnDevice);

    #[tokio::test]
    async fn an_oversized_text_is_split_alone_and_pooled_into_one_vector() {
        let fake = Fake::new(false);
        // "a…" then "b…" pieces: the split cuts at the paragraph break.
        let long = format!("{}\n\n{}", "a".repeat(6000), "b".repeat(6000));
        let vectors = submit(&fake, LOCAL, &["short", long.as_str(), "tiny"]).await.unwrap();
        assert_eq!(vectors.len(), 3);
        let calls = fake.calls.lock().unwrap();
        // The bounded texts share one request; the long one goes alone, as
        // two pieces each within the limit.
        assert_eq!(calls[0], vec!["short".to_string(), "tiny".to_string()]);
        assert_eq!(calls[1].len(), 2);
        assert!(calls[1].iter().all(|p| p.len() <= EMBED_INPUT_MAX_BYTES));
        assert_eq!(calls[1].concat().len(), 12000, "nothing truncated");
        // Equal-length pieces on axes a and b pool to the unit diagonal.
        let long_vec = &vectors[1];
        let (a, b) = (usize::from(b'a'), usize::from(b'b'));
        assert!((long_vec[a] - 0.5f32.sqrt()).abs() < 1e-6);
        assert!((long_vec[b] - 0.5f32.sqrt()).abs() < 1e-6);
        assert_eq!(vectors[0][usize::from(b's')], 1.0);
        assert_eq!(vectors[2][usize::from(b't')], 1.0);
    }

    #[test]
    fn plan_bounds_requests_by_count_and_bytes_and_refuses_what_cannot_fit() {
        let texts: Vec<String> = (0..5).map(|i| format!("t{i}")).collect();
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        let plan = plan(&refs, 2);
        let groups: Vec<_> = plan.requests.iter().map(|r| r.items().to_vec()).collect();
        assert_eq!(groups, vec![vec![0, 1], vec![2, 3], vec![4]]);

        let near = "x".repeat(EMBED_INPUT_MAX_BYTES);
        let many = vec![near.as_str(); REQUEST_MAX_BYTES / EMBED_INPUT_MAX_BYTES + 1];
        let plan = super::plan(&many, 1000);
        assert_eq!(plan.requests.len(), 2, "the byte cap closes the first request");
        assert!(plan.requests.iter().all(|r| r.input_bytes() <= REQUEST_MAX_BYTES));

        let huge = "word ".repeat(REQUEST_MAX_BYTES / 4);
        let blank = " ".repeat(EMBED_INPUT_MAX_BYTES + 1);
        let plan = super::plan(&["ok", huge.as_str(), blank.as_str()], 8);
        assert_eq!(plan.requests.len(), 1);
        let refused: Vec<_> = plan.refused.iter().map(|(i, f)| (*i, f.is_permanent())).collect();
        assert_eq!(refused, vec![(1, true), (2, true)]);
        assert!(matches!(plan.refused[0].1.error, EmbedError::InputTooLong(_)));
    }

    #[test]
    fn only_429_5xx_and_transport_failures_are_transient() {
        let http = |status| EmbedError::Http {
            status,
            body: String::new(),
        };
        for status in [429, 500, 502, 503, 504, 599] {
            assert_eq!(classify(&http(status)), Disposition::Transient, "{status}");
        }
        for status in [400, 401, 402, 403, 404, 413, 422, 451, 302, 200] {
            assert_eq!(classify(&http(status)), Disposition::Permanent, "{status}");
        }
        use crate::send_gate::HoldCode;
        for (error, expected) in [
            (EmbedError::Transport("timed out".into()), Disposition::Transient),
            (EmbedError::Denied(Deny::Hold(HoldCode::DailyBudgetExhausted)), Disposition::Transient),
            (EmbedError::Denied(Deny::Unavailable), Disposition::Transient),
            (EmbedError::Denied(Deny::Refuse(RefuseCode::CallCostCap)), Disposition::Permanent),
            (EmbedError::InputTooLong("8192".into()), Disposition::Permanent),
            (EmbedError::InferenceFailed("not json".into()), Disposition::Permanent),
            (EmbedError::ModelNotLoaded("missing".into()), Disposition::Permanent),
        ] {
            assert_eq!(classify(&error), expected, "{error}");
        }
    }

    #[tokio::test]
    async fn a_failed_request_carries_its_verdict() {
        let bad = Fake::failing(EmbedError::Http {
            status: 400,
            body: "bad request".into(),
        });
        let failure = submit(&bad, LOCAL, &["a"]).await.unwrap_err();
        assert!(failure.is_permanent());
        let down = Fake::failing(EmbedError::Http {
            status: 503,
            body: "busy".into(),
        });
        let failure = submit(&down, LOCAL, &["a"]).await.unwrap_err();
        assert_eq!(failure.disposition, Disposition::Transient);
    }

    #[tokio::test]
    async fn a_paid_provider_is_never_sent_unbilled_and_a_free_one_never_managed() {
        let paid = Fake::new(true);
        let failure = submit(&paid, LOCAL, &["a"]).await.unwrap_err();
        assert!(matches!(failure.error, EmbedError::Denied(Deny::Refuse(RefuseCode::Ungated))));
        assert!(paid.calls.lock().unwrap().is_empty(), "nothing sent");

        let free = Fake::new(false);
        let plan = plan(&["a"], 8);
        let failure = send(&free, Purpose::Managed(&Unmanaged), &plan.requests[0])
            .await
            .unwrap_err();
        assert!(failure.is_permanent());
        assert!(free.calls.lock().unwrap().is_empty(), "nothing sent");
    }

    #[tokio::test]
    async fn a_metered_send_reports_every_attempt_with_its_bytes() {
        let paid = Fake::new(true);
        let meter = Meter::default();
        submit_one(&paid, Purpose::Metered(&meter), "a reef").await.unwrap();
        let recorded = meter.0.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, "a reef".len());
    }

    #[tokio::test]
    async fn a_short_answer_is_a_permanent_failure() {
        struct Short;
        impl EmbedProvider for Short {
            fn embed_batch<'a>(&'a self, _: &'a [EmbedInput], _: ProviderPermit) -> EmbedBatchFuture<'a> {
                Box::pin(async { Ok(Vec::new()) })
            }
        }
        let failure = submit(&Short, LOCAL, &["a"]).await.unwrap_err();
        assert!(failure.is_permanent());
    }
}
