//! ONNX Runtime embedding backend for `BAAI/bge-small-en-v1.5`.
//!
//! [`OrtEmbedder`] implements [`EmbedProvider`] using the `ort` crate (ONNX
//! Runtime v2) and HuggingFace `tokenizers`.  The model produces 384-dimensional
//! L2-normalised embeddings matching the `atoms.embedding` column schema.
//!
//! # Model files
//!
//! Download with `scripts/download_model.sh` before running in production.
//! The embedder falls back to a `ModelNotLoaded` error (rather than panicking)
//! when the files are absent — callers should handle `None` gracefully.
//!
//! ## Resolution order for `OrtEmbedder::load()`
//!
//! 1. `EMBED_MODEL_PATH` + `EMBED_TOKENIZER_PATH` environment variables
//! 2. `./models/bge-small-en-v1.5.onnx` / `./models/bge-small-en-v1.5-tokenizer.json`

use std::{path::Path, sync::Mutex};

use ort::{inputs, session::Session, value::Tensor};

// GPU execution providers are opt-in via the `gpu` feature. The default
// (CPU-only) build links no CUDA/DirectML binaries — keeping the Linux Docker
// image small and avoiding the CUDA binary download on the Fly build.
#[cfg(feature = "gpu")]
use ort::execution_providers::CUDAExecutionProvider;
#[cfg(all(feature = "gpu", windows))]
use ort::execution_providers::DirectMLExecutionProvider;
use tokenizers::Tokenizer;

use crate::embed::{EmbedBatchFuture, EmbedError, EmbedInput, EmbedProvider, ProviderPermit};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Default sequence length including [CLS] and [SEP].
///
/// This is a *cap* on truncation, not a fixed padding target — see
/// [`OrtEmbedder::tokenize_batch`] for why. Padding every batch to this
/// length regardless of actual content caused a confirmed production OOM:
/// at `EMBED_BATCH_SIZE = 64` (see `backend/src/workers/embed.rs`) with
/// bge-small's 12 attention heads, `64 * 12 * 512 * 512 * 4 bytes (f32) =
/// 805,306,368 bytes` for the attention-score tensor alone — matched
/// byte-for-byte in the prod OOM log ("Extending BFCArena ... num_bytes:
/// 805306368") on a 1024 MB Fly machine, which SIGKILLed the entire backend
/// process (exit_code=137, oom_killed=true) mid-job. Real atom content in
/// that incident averaged 140 chars (max 767), so ~95% of the padded tensor
/// was wasted memory.
const DEFAULT_MAX_LENGTH: usize = 512;

/// Default model file path (relative to process working directory).
const DEFAULT_MODEL_PATH: &str = "models/bge-small-en-v1.5.onnx";

/// Default tokenizer file path.
const DEFAULT_TOKENIZER_PATH: &str = "models/bge-small-en-v1.5-tokenizer.json";

/// Expected output dimension from bge-small-en-v1.5.
pub const EMBED_DIM: usize = 384;

// ── OrtEmbedder ───────────────────────────────────────────────────────────────

/// ONNX Runtime–backed embedder for `BAAI/bge-small-en-v1.5`.
///
/// `ort::Session::run` requires `&mut self`, so the session is wrapped in a
/// `Mutex` to permit shared-reference access from the `EmbedProvider` trait.
/// Inference calls serialise at the mutex; for higher throughput, use multiple
/// `OrtEmbedder` instances behind a pool.
pub struct OrtEmbedder {
    session: Mutex<Session>,
    tokenizer: Tokenizer,
    max_length: usize,
}

impl OrtEmbedder {
    /// Load model and tokenizer from explicit file paths using the default max length.
    pub fn load_from_path(model_path: &str, tokenizer_path: &str) -> Result<Self, EmbedError> {
        Self::load_from_path_with_max_length(model_path, tokenizer_path, DEFAULT_MAX_LENGTH)
    }

    /// Same as [`load_from_path`] but allows overriding the token sequence length.
    ///
    /// `max_length` must be at least 1. `tokenize_batch`'s `seq_len` floors at 1
    /// (to avoid a zero-width tensor for degenerate all-empty batches — see that
    /// method's doc comment) and asserts `seq_len <= self.max_length`; an
    /// unvalidated `max_length` of 0 would violate that invariant on every call —
    /// tripping the `debug_assert!` in a debug build, or silently producing a
    /// 1-column tensor that ignores the caller's "no tokens" request in release
    /// (CodeRabbit review, PR #194). Reject it here instead, at the one call site
    /// that owns the value, rather than at every inference call.
    pub fn load_from_path_with_max_length(
        model_path: &str,
        tokenizer_path: &str,
        max_length: usize,
    ) -> Result<Self, EmbedError> {
        if max_length < 1 {
            return Err(EmbedError::ModelNotLoaded(format!(
                "max_length must be at least 1, got {max_length}"
            )));
        }
        if !Path::new(model_path).exists() {
            return Err(EmbedError::ModelNotLoaded(format!(
                "ONNX model file not found: {model_path}. \
                 Run scripts/download_model.sh to download it."
            )));
        }
        if !Path::new(tokenizer_path).exists() {
            return Err(EmbedError::ModelNotLoaded(format!(
                "tokenizer file not found: {tokenizer_path}. \
                 Run scripts/download_model.sh to download it."
            )));
        }

        // Register GPU execution providers in priority order when the `gpu`
        // feature is enabled. ONNX Runtime skips any EP whose runtime libs are
        // absent and falls back to CPU. With the `gpu` feature disabled
        // (default), no GPU EP binaries are linked or downloaded — CPU only.
        // The binding is only mutated in the `gpu` cfg; `#[allow(unused_mut)]`
        // keeps the default (CPU) build warning-free.
        #[allow(unused_mut)]
        let mut session_builder = Session::builder()
            .map_err(|e| EmbedError::ModelNotLoaded(format!("ORT session builder: {e}")))?;

        #[cfg(feature = "gpu")]
        {
            // CUDA EP: highest throughput on NVIDIA; requires CUDA Toolkit DLLs at runtime.
            #[allow(unused_mut)]
            let mut eps: Vec<ort::execution_providers::ExecutionProviderDispatch> =
                vec![CUDAExecutionProvider::default().build()];
            // DirectML EP: Windows-native GPU via DirectX 12; zero extra install needed.
            #[cfg(windows)]
            eps.push(DirectMLExecutionProvider::default().build());
            session_builder = session_builder
                .with_execution_providers(eps)
                .map_err(|e| EmbedError::ModelNotLoaded(format!("ORT EP registration: {e}")))?;
        }

        let session = session_builder
            .commit_from_file(model_path)
            .map_err(|e| {
                EmbedError::ModelNotLoaded(format!("ORT session load from {model_path}: {e}"))
            })?;

        let tokenizer = Tokenizer::from_file(tokenizer_path).map_err(|e| {
            EmbedError::ModelNotLoaded(format!("tokenizer load from {tokenizer_path}: {e}"))
        })?;

        Ok(Self {
            session: Mutex::new(session),
            tokenizer,
            max_length,
        })
    }

    /// Load model using environment variables or default file paths.
    ///
    /// Resolution order:
    /// 1. `EMBED_MODEL_PATH` + `EMBED_TOKENIZER_PATH` env vars
    /// 2. `./models/bge-small-en-v1.5.onnx` + `./models/bge-small-en-v1.5-tokenizer.json`
    ///
    /// Returns `Err(EmbedError::ModelNotLoaded)` if the files don't exist —
    /// callers should log a warning and set the embedder to `None`.
    pub fn load() -> Result<Self, EmbedError> {
        let model_path = std::env::var("EMBED_MODEL_PATH")
            .unwrap_or_else(|_| DEFAULT_MODEL_PATH.to_string());
        let tokenizer_path = std::env::var("EMBED_TOKENIZER_PATH")
            .unwrap_or_else(|_| DEFAULT_TOKENIZER_PATH.to_string());

        Self::load_from_path(&model_path, &tokenizer_path)
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Tokenize a slice of texts and produce BERT-style input tensors.
    ///
    /// Returns flat `(input_ids, attention_mask, token_type_ids)` vecs each
    /// of length `batch * seq_len`, ready to be shaped into `[batch, seq_len]`
    /// tensors for ONNX Runtime — where `seq_len` is the **dynamic** longest
    /// encoded sequence actually present in this batch, capped at
    /// `self.max_length` (truncation), rather than always padding to the
    /// fixed `self.max_length`.
    ///
    /// # Why dynamic sizing (production OOM, see [`DEFAULT_MAX_LENGTH`])
    ///
    /// Self-attention scores scale as
    /// `batch * num_heads * seq_len^2 * 4 bytes` (f32). Always padding to a
    /// fixed 512 tokens regardless of content meant a batch of short atoms
    /// paid the same (huge) memory cost as a batch of maximally long ones —
    /// this is exactly what OOM-killed the backend process in production
    /// (see the doc comment on [`DEFAULT_MAX_LENGTH`] for the incident
    /// numbers). Sizing tensors to the batch's actual longest sequence keeps
    /// memory proportional to real content while still bounding worst-case
    /// memory via truncation at `self.max_length`.
    ///
    /// The HuggingFace `tokenizers` crate is not configured with its own
    /// fixed-length padding here — padding to `seq_len` is done manually
    /// below by writing into pre-sized, zero-filled vecs (pad token id 0,
    /// pad attention mask 0), so this function is the single place that
    /// controls tensor width; no separate "disable tokenizer padding" step
    /// was needed.
    fn tokenize_batch(&self, texts: &[&str]) -> Result<TokenBatch, EmbedError> {
        let batch = texts.len();

        // First pass: encode every text and find the longest sequence in
        // this batch, each individually capped at `self.max_length` so a
        // single over-long input can't blow up `seq_len` past the cap.
        let mut encodings = Vec::with_capacity(batch);
        let mut seq_len = 1usize; // floor of 1 avoids a zero-width tensor for degenerate all-empty batches
        for (idx, text) in texts.iter().enumerate() {
            let encoding = self
                .tokenizer
                .encode(*text, true) // add_special_tokens = true → [CLS]…[SEP]
                .map_err(|e| EmbedError::InferenceFailed(format!("tokenizer encode: {e}")))?;
            let real_len = encoding.get_ids().len();
            let capped_len = real_len.min(self.max_length);
            // The chunker (packages/core/src/chunker.rs) estimates token count with
            // a crude words*4/3 heuristic before we ever see real BPE ids here, so
            // a chunk it judged "under target" can still exceed `self.max_length`
            // once actually tokenized — silently truncated below with no signal
            // anywhere upstream. Log it so truncation is at least observable
            // (batch index only, not the text itself, to avoid leaking content
            // into logs); if this fires often in practice, TOKEN_TARGET (or the
            // heuristic itself) needs to come down further.
            if real_len > self.max_length {
                tracing::warn!(
                    batch_index = idx,
                    real_tokens = real_len,
                    max_length = self.max_length,
                    dropped_tokens = real_len - self.max_length,
                    "embed_ort: input truncated to max_length — chunker's token estimate undercounted"
                );
            }
            seq_len = seq_len.max(capped_len);
            encodings.push(encoding);
        }
        // Invariant relied on below: seq_len <= self.max_length, because it's
        // the max of per-text values that are each already capped at
        // self.max_length.
        debug_assert!(seq_len <= self.max_length);

        let total = batch * seq_len;
        let mut input_ids = vec![0i64; total];
        let mut attention_mask = vec![0i64; total];
        let token_type_ids = vec![0i64; total]; // all zeros for BERT single-sequence

        for (i, encoding) in encodings.iter().enumerate() {
            let ids = encoding.get_ids();
            let mask = encoding.get_attention_mask();

            // `seq_len <= self.max_length` (see above), so this also
            // enforces truncation to `self.max_length` for any text longer
            // than the cap.
            let len = ids.len().min(seq_len);
            let base = i * seq_len;

            for j in 0..len {
                input_ids[base + j] = ids[j] as i64;
                attention_mask[base + j] = mask[j] as i64;
            }
            // Positions beyond `len` remain 0 (pad token id, pad mask = 0).
        }

        Ok(TokenBatch {
            batch,
            max_length: seq_len,
            input_ids,
            attention_mask,
            token_type_ids,
        })
    }

    /// Run ONNX inference and return raw CLS-token embeddings.
    ///
    /// bge-small-en-v1.5 outputs `last_hidden_state` (index 0) shaped
    /// `[batch, seq_len, 384]`.  We take the [CLS] token at position 0.
    fn run_inference(&self, tb: TokenBatch) -> Result<Vec<Vec<f32>>, EmbedError> {
        let shape = vec![tb.batch as i64, tb.max_length as i64];

        let ids_tensor = Tensor::<i64>::from_array((shape.clone(), tb.input_ids))
            .map_err(|e| EmbedError::InferenceFailed(format!("input_ids tensor: {e}")))?;
        let mask_tensor = Tensor::<i64>::from_array((shape.clone(), tb.attention_mask))
            .map_err(|e| EmbedError::InferenceFailed(format!("attention_mask tensor: {e}")))?;
        let type_tensor = Tensor::<i64>::from_array((shape, tb.token_type_ids))
            .map_err(|e| EmbedError::InferenceFailed(format!("token_type_ids tensor: {e}")))?;

        let mut guard = self
            .session
            .lock()
            .map_err(|e| EmbedError::InferenceFailed(format!("session mutex poisoned: {e}")))?;
        let outputs = guard
            .run(inputs![
                "input_ids"      => ids_tensor,
                "attention_mask" => mask_tensor,
                "token_type_ids" => type_tensor,
            ])
            .map_err(|e| EmbedError::InferenceFailed(format!("ORT session run: {e}")))?;

        // Output 0 is `last_hidden_state` shaped [batch, seq_len, hidden_dim].
        let (_, output) = outputs.iter().next()
            .ok_or_else(|| EmbedError::InferenceFailed("ONNX returned no outputs".into()))?;
        let (shape, raw) = output
            .try_extract_tensor::<f32>()
            .map_err(|e| EmbedError::InferenceFailed(format!("extract output tensor: {e}")))?;

        if shape.len() != 3 {
            return Err(EmbedError::InferenceFailed(format!(
                "expected 3-D output [batch, seq_len, hidden], got {}-D",
                shape.len()
            )));
        }

        let batch = shape[0] as usize;
        let seq_len = shape[1] as usize;
        let hidden_dim = shape[2] as usize;

        if hidden_dim != EMBED_DIM {
            return Err(EmbedError::InferenceFailed(format!(
                "unexpected hidden_dim {hidden_dim}, expected {EMBED_DIM}"
            )));
        }

        // Extract [CLS] token (position 0) for each batch element.
        let mut embeddings = Vec::with_capacity(batch);
        for b in 0..batch {
            let start = b * seq_len * hidden_dim;
            if start + hidden_dim > raw.len() {
                return Err(EmbedError::InferenceFailed(format!(
                    "tensor size mismatch: expected {} floats, got {}",
                    start + hidden_dim,
                    raw.len()
                )));
            }
            embeddings.push(raw[start..start + hidden_dim].to_vec());
        }

        Ok(embeddings)
    }

    /// Tokenize, run inference, and L2-normalise — synchronous entry point.
    fn embed_batch_sync(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Ok(vec![]);
        }
        let tb = self.tokenize_batch(texts)?;
        let raw = self.run_inference(tb)?;
        if raw.len() != texts.len() {
            return Err(EmbedError::InferenceFailed(format!(
                "batch length mismatch: {} inputs produced {} embeddings",
                texts.len(),
                raw.len()
            )));
        }
        raw.into_iter().map(l2_normalize).collect()
    }
}

/// Intermediate struct holding flat tokenizer output for a batch.
struct TokenBatch {
    batch: usize,
    max_length: usize,
    input_ids: Vec<i64>,
    attention_mask: Vec<i64>,
    token_type_ids: Vec<i64>,
}

// ── Test-only fixed-length twin (pre-fix behaviour, for regression tests) ─────

/// Reproduces the pre-fix behaviour of always padding every batch to
/// `self.max_length`, kept only so tests can prove the dynamic-sizing fix
/// (see [`OrtEmbedder::tokenize_batch`]) does not change embedding *values*,
/// only tensor size / memory. Not used outside `#[cfg(test)]`.
#[cfg(test)]
impl OrtEmbedder {
    fn tokenize_batch_fixed_length(&self, texts: &[&str]) -> Result<TokenBatch, EmbedError> {
        let batch = texts.len();
        let total = batch * self.max_length;
        let mut input_ids = vec![0i64; total];
        let mut attention_mask = vec![0i64; total];
        let token_type_ids = vec![0i64; total];

        for (i, text) in texts.iter().enumerate() {
            let encoding = self
                .tokenizer
                .encode(*text, true)
                .map_err(|e| EmbedError::InferenceFailed(format!("tokenizer encode: {e}")))?;
            let ids = encoding.get_ids();
            let mask = encoding.get_attention_mask();
            let len = ids.len().min(self.max_length);
            let base = i * self.max_length;
            for j in 0..len {
                input_ids[base + j] = ids[j] as i64;
                attention_mask[base + j] = mask[j] as i64;
            }
        }

        Ok(TokenBatch {
            batch,
            max_length: self.max_length,
            input_ids,
            attention_mask,
            token_type_ids,
        })
    }

    fn embed_batch_sync_fixed_length(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Ok(vec![]);
        }
        let tb = self.tokenize_batch_fixed_length(texts)?;
        let raw = self.run_inference(tb)?;
        raw.into_iter().map(l2_normalize).collect()
    }
}

// ── EmbedProvider impl ────────────────────────────────────────────────────────

impl EmbedProvider for OrtEmbedder {
    fn embed_batch<'a>(
        &'a self,
        inputs: &'a [EmbedInput],
        _: ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        Box::pin(async move {
            let texts: Vec<&str> = inputs.iter().map(EmbedInput::as_str).collect();
            tokio::task::block_in_place(|| self.embed_batch_sync(&texts))
        })
    }
}

// ── L2 normalization ──────────────────────────────────────────────────────────

/// L2-normalise a vector in-place and return it.
///
/// If the vector has zero norm (all-zeros input) it is returned unchanged to
/// avoid NaN propagation.
pub fn l2_normalize(mut v: Vec<f32>) -> Result<Vec<f32>, EmbedError> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        for x in &mut v {
            *x /= norm;
        }
    }
    Ok(v)
}

/// L2-normalise every row in a batch of embedding vectors.
///
/// Each inner `Vec<f32>` is normalised independently.
pub fn l2_normalize_rows(batch: Vec<Vec<f32>>) -> Result<Vec<Vec<f32>>, EmbedError> {
    batch.into_iter().map(l2_normalize).collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// The gateway's in-process path, as every caller reaches the model.
    async fn one(embedder: &dyn EmbedProvider, text: &str) -> Result<Vec<f32>, EmbedError> {
        crate::embed_gateway::submit_one(embedder, ON_DEVICE, text)
            .await
            .map_err(|f| f.error)
    }

    async fn many(embedder: &dyn EmbedProvider, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        crate::embed_gateway::submit(embedder, ON_DEVICE, texts)
            .await
            .map_err(|f| f.error)
    }

    const ON_DEVICE: crate::embed_gateway::Purpose<'static> =
        crate::embed_gateway::Purpose::Unbilled(crate::embed_gateway::Unbilled::OnDevice);
    use crate::embed::EmbedProvider;

    // ── L2 normalization unit tests ───────────────────────────────────────────

    #[test]
    fn l2_normalize_unit_vector_unchanged() {
        let v = vec![1.0f32, 0.0, 0.0];
        let n = l2_normalize(v).unwrap();
        let norm: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6, "norm={norm}");
    }

    #[test]
    fn l2_normalize_scales_correctly() {
        // [3, 4] has norm 5; normalised → [0.6, 0.8]
        let v = vec![3.0f32, 4.0];
        let n = l2_normalize(v).unwrap();
        assert!((n[0] - 0.6).abs() < 1e-6, "n[0]={}", n[0]);
        assert!((n[1] - 0.8).abs() < 1e-6, "n[1]={}", n[1]);
    }

    #[test]
    fn l2_normalize_zero_vector_no_nan() {
        let v = vec![0.0f32, 0.0, 0.0];
        let n = l2_normalize(v.clone()).unwrap();
        assert!(n.iter().all(|x| !x.is_nan()), "zero vector produced NaN");
        assert_eq!(n, v, "zero vector should be unchanged");
    }

    #[test]
    fn l2_normalize_384d_is_unit() {
        let v: Vec<f32> = (0..EMBED_DIM).map(|i| i as f32 * 0.01 + 1.0).collect();
        let n = l2_normalize(v).unwrap();
        let norm: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5, "norm={norm}");
    }

    #[test]
    fn l2_normalize_rows_each_unit() {
        let batch = vec![
            vec![3.0f32, 4.0],      // norm 5
            vec![1.0f32, 0.0],      // already unit
            vec![0.0f32, 0.0],      // zero vector
        ];
        let out = l2_normalize_rows(batch).unwrap();
        // Row 0
        assert!((out[0][0] - 0.6).abs() < 1e-6);
        assert!((out[0][1] - 0.8).abs() < 1e-6);
        // Row 1
        assert!((out[1][0] - 1.0).abs() < 1e-6);
        // Row 2: zero → no NaN
        assert!(!out[2][0].is_nan());
        assert!(!out[2][1].is_nan());
    }

    // ── max_length validation ─────────────────────────────────────────────────

    /// CodeRabbit review, PR #194: `max_length = 0` must be rejected at load
    /// time rather than silently violating `tokenize_batch`'s
    /// `seq_len <= self.max_length` invariant on every call (debug-assert trip
    /// in a debug build, or a spurious 1-column tensor in release — see
    /// `load_from_path_with_max_length`'s doc comment). Checked before the
    /// file-existence checks, so this doesn't need real model/tokenizer files
    /// on disk to exercise.
    #[test]
    fn load_with_zero_max_length_is_rejected() {
        let result = OrtEmbedder::load_from_path_with_max_length(
            "/nonexistent/model.onnx",
            "/nonexistent/tokenizer.json",
            0,
        );
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("max_length = 0 must be rejected"),
        };
        assert!(
            matches!(err, EmbedError::ModelNotLoaded(_)),
            "expected ModelNotLoaded, got {err:?}"
        );
        assert!(
            err.to_string().contains("max_length"),
            "error message should mention max_length, got: {err}"
        );
    }

    // ── Token truncation / padding unit tests ─────────────────────────────────

    #[test]
    fn truncation_max_length_respected() {
        let max_length = 8usize;
        let ids: Vec<i64> = (0..20).map(|i| i as i64).collect(); // longer than max_length
        let mut out_ids = vec![0i64; max_length];
        let len = ids.len().min(max_length);
        out_ids[..len].copy_from_slice(&ids[..len]);
        assert_eq!(out_ids.len(), max_length);
        for (j, &id) in out_ids.iter().enumerate() {
            assert_eq!(id, j as i64, "position {j}");
        }
    }

    #[test]
    fn padding_fills_with_zeros() {
        let max_length = 16usize;
        let ids: Vec<i64> = vec![101, 2023, 102]; // [CLS] token [SEP]
        let mut out_ids = vec![0i64; max_length];
        let len = ids.len().min(max_length);
        out_ids[..len].copy_from_slice(&ids[..len]);
        assert_eq!(out_ids[0], 101);
        assert_eq!(out_ids[1], 2023);
        assert_eq!(out_ids[2], 102);
        for i in 3..max_length {
            assert_eq!(out_ids[i], 0, "pad position {i} must be 0");
        }
    }

    #[test]
    fn token_batch_truncates_and_pads() {
        // Simulate what tokenize_batch does with max_length = 4.
        let batch_size = 2;
        let max_length = 4;
        // "sentence" = [CLS, a, b, SEP] → len 4 (at boundary)
        // "hi" = [CLS, h, SEP] → len 3 (needs 1 pad)
        let sequences: Vec<Vec<i64>> = vec![vec![101, 7, 8, 102], vec![101, 5, 102]];
        let mut out = vec![0i64; batch_size * max_length];
        for (i, seq) in sequences.iter().enumerate() {
            let len = seq.len().min(max_length);
            let base = i * max_length;
            out[base..base + len].copy_from_slice(&seq[..len]);
        }
        // Row 0: no padding needed
        assert_eq!(&out[0..4], &[101, 7, 8, 102]);
        // Row 1: last position should be 0
        assert_eq!(&out[4..8], &[101, 5, 102, 0]);
    }

    // ── Trait compliance: mock EmbedProvider ─────────────────────────────────

    /// A minimal in-memory stub that satisfies `EmbedProvider` without ORT.
    struct StubEmbedder {
        dim: usize,
    }

    impl EmbedProvider for StubEmbedder {
        fn embed_batch<'a>(
            &'a self,
            inputs: &'a [EmbedInput],
            _: ProviderPermit,
        ) -> EmbedBatchFuture<'a> {
            let dim = self.dim;
            let n = inputs.len();
            Box::pin(async move {
                Ok((0..n)
                    .map(|_| {
                        std::iter::once(1.0f32)
                            .chain(std::iter::repeat(0.0f32).take(dim - 1))
                            .collect::<Vec<f32>>()
                    })
                    .collect())
            })
        }
    }

    #[tokio::test]
    async fn stub_embed_returns_384d_vector() {
        let embedder = StubEmbedder { dim: EMBED_DIM };
        let result = one(&embedder, "hello world").await.unwrap();
        assert_eq!(result.len(), EMBED_DIM);
    }

    #[tokio::test]
    async fn stub_embed_batch_returns_correct_count() {
        let embedder = StubEmbedder { dim: EMBED_DIM };
        let texts = ["hello", "world", "foo"];
        let results = many(&embedder, &texts).await.unwrap();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(r.len(), EMBED_DIM);
        }
    }

    #[tokio::test]
    async fn stub_is_dyn_object_safe() {
        use std::sync::Arc;
        let embedder: Arc<dyn EmbedProvider> = Arc::new(StubEmbedder { dim: EMBED_DIM });
        let v = one(embedder.as_ref(), "test").await.unwrap();
        assert_eq!(v.len(), EMBED_DIM);
    }

    // ── Integration tests (require model files — skipped in CI) ──────────────

    /// Resolve the repo-root `models/` dir from this crate's manifest dir.
    ///
    /// `cargo test` runs test binaries with cwd = the crate's manifest
    /// directory (`packages/core`), not the workspace root, so the relative
    /// `./models/...` path used by [`OrtEmbedder::load`]'s defaults does not
    /// resolve here even when `models/` exists at the repo root. Building an
    /// absolute path from `CARGO_MANIFEST_DIR` makes these tests work
    /// regardless of the directory `cargo test` is invoked from.
    fn model_paths() -> (String, String) {
        (
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../models/bge-small-en-v1.5.onnx").to_string(),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../models/bge-small-en-v1.5-tokenizer.json"
            )
            .to_string(),
        )
    }

    /// Load real model and embed a sentence.
    /// Run manually: `cargo test -p coralswarm-core -- --ignored`
    ///
    /// `flavor = "multi_thread"` is required: `OrtEmbedder::embed`/`embed_batch`
    /// call `tokio::task::block_in_place`, which panics ("can call blocking
    /// only when running on the multi-threaded runtime") under the default
    /// current-thread test runtime.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn integration_embed_single_sentence() {
        let (model, tokenizer) = model_paths();
        let embedder =
            OrtEmbedder::load_from_path(&model, &tokenizer).expect("model should load from repo models/");
        let result = one(&embedder, "Hello, CoralSwarm!").await.unwrap();
        assert_eq!(result.len(), EMBED_DIM, "embedding must be 384-dim");
        let norm: f32 = result.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4, "must be L2-normalised; norm={norm}");
    }

    /// See `flavor = "multi_thread"` note on `integration_embed_single_sentence`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn integration_embed_batch_unit_vectors() {
        let (model, tokenizer) = model_paths();
        let embedder =
            OrtEmbedder::load_from_path(&model, &tokenizer).expect("model should load from repo models/");
        let texts = ["First sentence.", "Second sentence.", "Third sentence."];
        let results = many(&embedder, &texts).await.unwrap();
        assert_eq!(results.len(), 3);
        for (i, v) in results.iter().enumerate() {
            assert_eq!(v.len(), EMBED_DIM, "row {i} dim");
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((norm - 1.0).abs() < 1e-4, "row {i} norm={norm}");
        }
    }

    #[test]
    #[ignore]
    fn integration_load_missing_file_returns_err() {
        let result =
            OrtEmbedder::load_from_path("/nonexistent/model.onnx", "/nonexistent/tok.json");
        assert!(matches!(result, Err(EmbedError::ModelNotLoaded(_))));
    }

    // ── Dynamic sequence-length correctness (require model files) ────────────
    //
    // These tests are the direct regression coverage for the OOM fix: they
    // prove that sizing tensors to the batch's actual content (instead of
    // always padding to `DEFAULT_MAX_LENGTH`) does not change embedding
    // *values*, only memory footprint, and that truncation for genuinely
    // long inputs still works.

    /// (a) Correctness of dynamic sizing: a batch of short strings must
    /// embed identically (within epsilon) whether tensors are sized to the
    /// batch's actual longest sequence (new behaviour, via `embed_batch_sync`)
    /// or padded to the full `DEFAULT_MAX_LENGTH` (old, pre-fix behaviour,
    /// via the test-only `embed_batch_sync_fixed_length` twin) — the
    /// attention mask should make the extra padding a no-op for the [CLS]
    /// output either way.
    #[tokio::test]
    #[ignore]
    async fn dynamic_padding_matches_fixed_512_padding() {
        let (model, tokenizer) = model_paths();
        let embedder =
            OrtEmbedder::load_from_path(&model, &tokenizer).expect("model should load from repo models/");
        let texts = ["Hello, CoralSwarm!", "A short atom.", "Reef graph."];

        let dynamic = embedder.embed_batch_sync(&texts).unwrap();
        let fixed = embedder.embed_batch_sync_fixed_length(&texts).unwrap();

        assert_eq!(dynamic.len(), fixed.len());
        for (i, (d, f)) in dynamic.iter().zip(fixed.iter()).enumerate() {
            assert_eq!(d.len(), f.len(), "row {i} dim mismatch");
            for (j, (dv, fv)) in d.iter().zip(f.iter()).enumerate() {
                assert!(
                    (dv - fv).abs() < 1e-4,
                    "row {i} dim {j}: dynamic={dv} fixed(512-padded)={fv}"
                );
            }
        }
    }

    /// (b) Mixed-length batch: padding/attention-mask correctness. Each
    /// element's embedding in a mixed-length batch must match its solo
    /// (batch-of-1) embedding within epsilon — proving that shorter
    /// sequences sharing a batch with longer ones aren't corrupted by the
    /// longer ones' padding under dynamic sizing.
    /// See `flavor = "multi_thread"` note on `integration_embed_single_sentence`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn mixed_length_batch_matches_solo_embeddings() {
        let (model, tokenizer) = model_paths();
        let embedder =
            OrtEmbedder::load_from_path(&model, &tokenizer).expect("model should load from repo models/");
        let texts = [
            "Hi.",
            "This is a somewhat longer sentence with more tokens in it.",
            "Medium length sentence here.",
        ];

        let batch = many(&embedder, &texts).await.unwrap();
        for (i, text) in texts.iter().enumerate() {
            let solo = one(&embedder, text).await.unwrap();
            assert_eq!(batch[i].len(), solo.len());
            for (j, (bv, sv)) in batch[i].iter().zip(solo.iter()).enumerate() {
                assert!(
                    (bv - sv).abs() < 1e-4,
                    "text {i} dim {j}: batch={bv} solo={sv}"
                );
            }
        }
    }

    /// (c) Truncation: an input longer than `DEFAULT_MAX_LENGTH` tokens must
    /// still be truncated to `DEFAULT_MAX_LENGTH`, i.e. the per-batch
    /// dynamic sizing introduced by the fix must not become unbounded.
    /// See `flavor = "multi_thread"` note on `integration_embed_single_sentence`
    /// (this test's final `embedder.embed(...)` call needs it too).
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn over_max_length_input_is_truncated() {
        let (model, tokenizer) = model_paths();
        let embedder =
            OrtEmbedder::load_from_path(&model, &tokenizer).expect("model should load from repo models/");

        // ~1000 repeated words tokenizes to well over 512 tokens.
        let long_text = "reef ".repeat(1000);
        let tb = embedder.tokenize_batch(&[long_text.as_str()]).unwrap();
        assert_eq!(
            tb.max_length, DEFAULT_MAX_LENGTH,
            "dynamic seq_len must be capped at DEFAULT_MAX_LENGTH for over-long input"
        );

        // Confirm inference still runs end-to-end without error at the
        // truncated length.
        let result = one(&embedder, &long_text).await.unwrap();
        assert_eq!(result.len(), EMBED_DIM);
    }

    /// A short input must NOT be padded all the way to `DEFAULT_MAX_LENGTH`
    /// — this is the crux of the OOM fix: dynamic seq_len should track
    /// actual content, not a fixed worst case.
    #[tokio::test]
    #[ignore]
    async fn short_input_does_not_pad_to_default_max_length() {
        let (model, tokenizer) = model_paths();
        let embedder =
            OrtEmbedder::load_from_path(&model, &tokenizer).expect("model should load from repo models/");
        let tb = embedder.tokenize_batch(&["Hi."]).unwrap();
        assert!(
            tb.max_length < DEFAULT_MAX_LENGTH,
            "short input should not be padded to the full {DEFAULT_MAX_LENGTH} — got {}",
            tb.max_length
        );
    }
}
