//! Embedding provider trait.
//!
//! Defines the interface for generating dense vector embeddings from text.
//! Concrete implementations (ONNX Runtime, stub, mock) implement [`EmbedProvider`]
//! and can be swapped without touching call-sites — useful for tests and for
//! future model upgrades.
//!
//! Nothing outside this crate can call a provider. Every provider method
//! takes a [`ProviderPermit`], which only [`crate::embed_gateway`] makes, and
//! every input is an [`EmbedInput`], whose only constructor bounds it to
//! [`EMBED_INPUT_MAX_BYTES`]. Callers go through the gateway, which owns size
//! bounding, batching, the call itself and error classification.
//!
//! The chosen model for CoralSwarm MVP is `BAAI/bge-small-en-v1.5` (384-dimensional,
//! ~33 MB ONNX).  See `docs/decisions/0002-vector-store-and-embedding.md`.

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;

// ── Active model identity ────────────────────────────────────────────────────

/// Identifier for the in-process ONNX bge model, stamped onto
/// `atoms.embed_model` by `Store::set_atom_embeddings`. Server search uses
/// [`EMBED_MODEL_CLOUD`] on `embed_model_next` instead.
pub const EMBED_MODEL_NAME: &str = "bge-small-en-v1.5";

/// Stamp written on `atoms.embed_model_next` for the cloud vector that sits
/// beside the live [`EMBED_MODEL_NAME`] vector during the hold.
///
/// Server search and the browser private engine use this stamp on
/// `atoms.embed_model_next`. [`EMBED_MODEL_NAME`] stays on `atoms.embedding`.
/// The `@384` suffix is the requested output width,
/// not a second model: OpenRouter `openai/text-embedding-3-small` is asked for
/// `dimensions: 384` so the existing `vector(384)` column can hold the result.
pub const EMBED_MODEL_CLOUD: &str = "openai/text-embedding-3-small@384";

/// Width of both the live column and the cloud side column.
pub const EMBED_DIM: usize = 384;

/// Hold-era helper: probe ok and no remaining bge rows without a cloud vector.
/// Server search now reads `embedding_next` regardless; this is still used
/// for boot logs and tests. Local-only rows are not in that count.
pub fn cloud_flip_ready(probe_ok: bool, rows_missing_cloud_vector: i64) -> bool {
    probe_ok && rows_missing_cloud_vector == 0
}

// ── Error type ────────────────────────────────────────────────────────────────

/// Errors that can arise during embedding.
///
/// A transport reports what it saw ([`Self::Http`], [`Self::Transport`]);
/// [`crate::embed_gateway::classify`] alone decides which are worth a retry.
#[derive(Clone, Debug, Error)]
pub enum EmbedError {
    /// The underlying model file could not be loaded or initialised.
    #[error("model not loaded: {0}")]
    ModelNotLoaded(String),

    /// The model was loaded but inference failed for the given input.
    #[error("inference failed: {0}")]
    InferenceFailed(String),

    /// A send gate refused the request before anything was sent (COR-21).
    #[error("embedding send denied: {0}")]
    Denied(crate::send_gate::Deny),

    /// The provider refused an input as longer than the model accepts
    /// (OpenAI: HTTP 400 "maximum input length is 8192 tokens"). Permanent
    /// for that text: re-sending it fails the same way, so a caller must
    /// not treat it as a transient outage.
    #[error("embedding input too long: {0}")]
    InputTooLong(String),

    /// The provider answered with a non-success HTTP status.
    #[error("embeddings HTTP {status}: {body}")]
    Http { status: u16, body: String },

    /// No HTTP answer: the connection failed, timed out, or dropped before
    /// the response was read.
    #[error("embeddings transport failed: {0}")]
    Transport(String),
}

// ── Bounded input ─────────────────────────────────────────────────────────────

/// Largest text one provider input may carry, in bytes.
///
/// The cloud model (`openai/text-embedding-3-small`) refuses an input over
/// 8192 tokens with an HTTP 400, and a byte-level BPE token is at least one
/// byte, so an input of at most this many bytes is within that limit for any
/// text. Chunks are bounded well below it
/// ([`crate::chunker::MAX_CHUNK_BYTES`] plus the contextual header).
pub const EMBED_INPUT_MAX_BYTES: usize = 8192;

/// One provider input, at most [`EMBED_INPUT_MAX_BYTES`] long.
///
/// The field is private and [`Self::split`] is the only constructor, so no
/// provider can be handed an over-limit input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbedInput(String);

impl EmbedInput {
    /// Bound `text` to [`EMBED_INPUT_MAX_BYTES`]. Text within the limit is
    /// one input, byte for byte. Longer text is split, never truncated, at
    /// the last paragraph, line, sentence or word break that fits (else at a
    /// character boundary); the pieces keep every word, in order.
    /// Whitespace-only text over the limit has nothing to embed and yields
    /// no input.
    pub fn split(text: &str) -> Vec<EmbedInput> {
        if text.len() <= EMBED_INPUT_MAX_BYTES {
            return vec![EmbedInput(text.to_string())];
        }
        crate::chunker::split_to_bytes(text, EMBED_INPUT_MAX_BYTES)
            .into_iter()
            .map(EmbedInput)
            .collect()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Proof that a provider call comes from [`crate::embed_gateway`]. Every
/// provider method takes one by value and only this crate can make one, so a
/// provider call anywhere else does not compile. An implementation outside
/// this crate receives it, and may hand it on to a provider it wraps.
#[derive(Debug)]
pub struct ProviderPermit(());

impl ProviderPermit {
    pub(crate) fn new() -> Self {
        ProviderPermit(())
    }
}

// ── Type aliases ──────────────────────────────────────────────────────────────

/// Boxed future returned by [`EmbedProvider::embed_batch`].
pub type EmbedBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>, EmbedError>> + Send + 'a>>;

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Embedding provider — pluggable for testing and future model upgrades.
///
/// The trait is object-safe: return types are boxed futures so
/// `Box<dyn EmbedProvider>` and `Arc<dyn EmbedProvider>` work without a
/// proc-macro or nightly feature.
///
/// The output vectors **must** have dimension 384 to match the `atoms.embedding`
/// column and its HNSW index.  Implementations are responsible for enforcing
/// this invariant.
///
/// Anyone may implement it; only [`crate::embed_gateway`] can call it (see
/// [`ProviderPermit`]).
pub trait EmbedProvider: Send + Sync {
    /// Embed a batch of bounded inputs, with no money gate.
    ///
    /// Each entry in the returned `Vec` corresponds to the input at the same
    /// index.  Implementations may process the batch in parallel or sequentially.
    fn embed_batch<'a>(
        &'a self,
        inputs: &'a [EmbedInput],
        permit: ProviderPermit,
    ) -> EmbedBatchFuture<'a>;

    /// The gated paid transport behind this provider, if it has one. A
    /// provider without one cannot serve a managed embedding: it is refused,
    /// never sent ungated. Local models have no paid transport.
    fn gated(&self) -> Option<&dyn GatedEmbed> {
        None
    }
}

mod private {
    pub trait Sealed {}
}
pub(crate) use private::Sealed;

/// An embedding transport that takes one send-gate permit per HTTP request.
/// Sealed to this crate, so no wrapper can rebind or drop the gate.
pub trait GatedEmbed: private::Sealed + Send + Sync {
    fn embed_batch_gated<'a>(
        &'a self,
        inputs: &'a [EmbedInput],
        gate: &'a dyn crate::send_gate::SendGate,
        permit: ProviderPermit,
    ) -> EmbedBatchFuture<'a>;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the trait is object-safe: `Box<dyn EmbedProvider>` must compile.
    #[allow(dead_code)]
    fn _assert_object_safe(_: Box<dyn EmbedProvider>) {}

    #[test]
    fn input_within_the_limit_is_kept_byte_for_byte() {
        let full = "a".repeat(EMBED_INPUT_MAX_BYTES);
        for text in ["", "  padded  ", full.as_str()] {
            let inputs = EmbedInput::split(text);
            assert_eq!(inputs.len(), 1);
            assert_eq!(inputs[0].as_str(), text);
        }
    }

    #[test]
    fn input_over_the_limit_is_split_never_truncated() {
        // Five times the limit, all words.
        let text = "reef ".repeat(EMBED_INPUT_MAX_BYTES);
        let inputs = EmbedInput::split(&text);
        assert!(inputs.len() >= 5, "{} pieces", inputs.len());
        assert!(inputs
            .iter()
            .all(|i| !i.as_str().is_empty() && i.as_str().len() <= EMBED_INPUT_MAX_BYTES));
        let words: usize = inputs.iter().map(|i| i.as_str().split_whitespace().count()).sum();
        assert_eq!(words, EMBED_INPUT_MAX_BYTES, "every word survives the split");
        // No break anywhere: cut at character boundaries, still whole.
        let blob = "é".repeat(EMBED_INPUT_MAX_BYTES);
        let inputs = EmbedInput::split(&blob);
        assert!(inputs.iter().all(|i| i.as_str().len() <= EMBED_INPUT_MAX_BYTES));
        assert_eq!(inputs.iter().map(EmbedInput::as_str).collect::<String>(), blob);
        assert!(EmbedInput::split(&" ".repeat(EMBED_INPUT_MAX_BYTES + 1)).is_empty());
    }

    #[test]
    fn flip_waits_for_a_passed_probe_and_an_empty_backfill() {
        assert!(!cloud_flip_ready(false, 0));
        assert!(!cloud_flip_ready(true, 1));
        assert!(!cloud_flip_ready(true, -1));
        assert!(cloud_flip_ready(true, 0));
    }
}
