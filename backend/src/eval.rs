//! Retrieval evaluation harness (Phase 1 quality gate) — **measurement only**.
//!
//! This module does not touch the production retrieval path. It composes the
//! same public building blocks the `/v1/queries` handler uses
//! ([`Store::vector_search`] + [`Store::keyword_search`] +
//! [`Store::keyword_search_node_atoms`] + [`Store::expand`] +
//! [`crate::retrieval::rerank::rerank`]) so that Recall@20, nDCG@10 and
//! abstention accuracy are measurable and repeatable against a real Postgres
//! with the fixtures ingested through the real chunker + embedding + FTS path.
//!
//! The three tiny pieces that live *inside* `routes/queries.rs` and are not
//! `pub` (the candidate-K constants, `merge_keyword_hits`, and
//! `ask_relevance_floor`) are mirrored below verbatim, with provenance
//! comments, rather than made public — keeping this PR additive.
//!
//! See `backend/eval/README.md` for how to run the harness and re-baseline.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_core::chunker::{self, ChunkStrategy};
use coralswarm_core::embed::{
    EmbedBatchFuture, EmbedInput, EmbedProvider, ProviderPermit, EMBED_MODEL_NAME,
};
use coralswarm_core::embed_ort::{l2_normalize, EMBED_DIM};

use crate::retrieval::rerank::{rerank, RerankParams};
use crate::retrieval::RetrievalFilter;
use crate::store::{Store, StoreError};

// ── Errors ─────────────────────────────────────────────────────────────────────

/// Typed errors for the evaluation harness (library-style: callers can match on
/// the failing operation rather than parse strings). Binary edges wrap these in
/// `anyhow`.
#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    /// A fixture or golden row was malformed (bad date, etc.).
    #[error("fixture error: {0}")]
    Fixture(String),

    /// Embedding a fixture chunk or a query failed.
    #[error("{context}: {source}")]
    Embed {
        context: String,
        #[source]
        source: coralswarm_core::embed::EmbedError,
    },

    /// A `Store` (database) operation failed.
    #[error("{context}: {source}")]
    Store {
        context: String,
        #[source]
        source: StoreError,
    },

    /// A raw SQL statement in the ingest path failed.
    #[error("{context}: {source}")]
    Db {
        context: String,
        #[source]
        source: sqlx::Error,
    },
}

// ── Mirrored retrieval constants (source of truth: routes/queries.rs) ──────────
//
// Kept in lockstep by hand so the harness measures the same candidate depth /
// abstention floor production uses. If these change in `routes/queries.rs`,
// change them here too (a drift check lives in the module tests below).

/// `routes/queries.rs::RETRIEVAL_CANDIDATE_K` — candidates per retrieval path.
pub const RETRIEVAL_CANDIDATE_K: usize = 50;
/// `routes/queries.rs::NODE_CANDIDATE_K` — reef-lexical candidate reefs.
pub const NODE_CANDIDATE_K: usize = 8;
/// `routes/queries.rs`: `top_k = req.top_k.unwrap_or(20)` — default retrieval depth.
pub const DEFAULT_TOP_K: usize = 20;
/// `routes/queries.rs::ASK_RELEVANCE_FLOOR_DEFAULT` — honesty-guard floor.
pub const ASK_RELEVANCE_FLOOR_DEFAULT: f64 = 0.02;

/// Fixed reference timestamp stamped on every ingested fixture atom.
///
/// The production reranker applies a 90-day recency half-life keyed off
/// `atoms.timestamp` and the wall clock (`Utc::now()`). Stamping every fixture
/// with the SAME timestamp makes that recency factor identical across atoms, so
/// it no longer perturbs the *relative* ordering the metric measures. We keep
/// production's real `RerankParams::default()` (we do not stub the recency term
/// away); a small residual wall-clock sensitivity remains and is absorbed by
/// the baseline tolerance (see `backend/eval/README.md`). Overridable via
/// `EVAL_REFERENCE_TS` (RFC3339) for experiments.
pub const EVAL_REFERENCE_TS_DEFAULT: &str = "2026-07-01T00:00:00Z";

/// `routes/queries.rs::ask_relevance_floor()` — mirrored verbatim.
pub fn ask_relevance_floor() -> f64 {
    match std::env::var("ASK_RELEVANCE_FLOOR") {
        Ok(v) => match v.trim().parse::<f64>() {
            Ok(f) if (0.0..=1.0).contains(&f) => f,
            _ => ASK_RELEVANCE_FLOOR_DEFAULT,
        },
        Err(_) => ASK_RELEVANCE_FLOOR_DEFAULT,
    }
}

/// `routes/queries.rs::supersession_ranking_enabled()` — mirrored verbatim
/// (§3.9). **Defaults ON.** Needed here (rather than calling the original)
/// because `routes` is a private module — see this module's own doc comment
/// on why the handful of non-`pub` pieces this harness needs are mirrored
/// with a provenance comment instead of made `pub`.
pub fn supersession_ranking_enabled() -> bool {
    match std::env::var("SUPERSESSION_RANKING_ENABLED") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        Err(_) => true,
    }
}

/// `routes/queries.rs::merge_keyword_hits()` — mirrored verbatim (graph-temporal
/// hardening: including its `HashMap`-iteration-order fix; see that function's
/// doc comment for why the sort is required, not cosmetic): normalise each
/// keyword source to `[0,1]` by its own max, then combine per atom by max, then
/// sort by `atom_id` so the dedup's `HashMap` pass never leaks a
/// process-random order into the result.
fn merge_keyword_hits(
    keyword_hits: Vec<(Uuid, f64)>,
    node_atom_hits: Vec<(Uuid, f64)>,
) -> Vec<(Uuid, f64)> {
    fn normalize(hits: Vec<(Uuid, f64)>) -> Vec<(Uuid, f64)> {
        let max = hits.iter().map(|&(_, s)| s).fold(0.0_f64, f64::max);
        if max > 0.0 {
            hits.into_iter().map(|(id, s)| (id, s / max)).collect()
        } else {
            hits
        }
    }
    let mut best: HashMap<Uuid, f64> = HashMap::new();
    for (id, score) in normalize(keyword_hits)
        .into_iter()
        .chain(normalize(node_atom_hits))
    {
        let slot = best.entry(id).or_insert(f64::MIN);
        if score > *slot {
            *slot = score;
        }
    }
    let mut merged: Vec<(Uuid, f64)> = best.into_iter().collect();
    merged.sort_by_key(|&(id, _)| id);
    merged
}

// ── Fixture / golden types ─────────────────────────────────────────────────────

/// One fixture document (a meeting transcript, or a Slack-ish atom).
#[derive(Debug, Clone, Deserialize)]
pub struct FixtureDoc {
    pub doc_id: String,
    pub source_type: String,
    #[serde(default)]
    pub title: Option<String>,
    /// `YYYY-MM-DD` — embedded into the meeting header for transcript docs.
    pub date: String,
    #[serde(default)]
    pub participants: Vec<String>,
    pub content: String,
}

/// One golden query → expected-doc mapping.
#[derive(Debug, Clone, Deserialize)]
pub struct GoldenQuery {
    pub id: String,
    /// `"direct"` | `"temporal"` | `"abstention"` | `"staleness"` | `"distractor"`
    /// | `"reef"`.
    ///
    /// `"staleness"` queries probe fact-update pairs in the corpus (an older doc
    /// superseded by a newer one covering the same fact): positive cases expect
    /// only the newer doc in `relevant_doc_ids`, negative cases expect BOTH docs
    /// of a genuinely-divergent-but-non-superseding pair (see `golden/golden.jsonl`).
    ///
    /// `"distractor"` queries probe topically-adjacent-but-lexically-imprecise
    /// fixture pairs (see [`NON_GATED_CATEGORIES`] for why this category is
    /// excluded from the blocking hash-embedder gate).
    ///
    /// `"reef"` queries are entity-name lookups ("who is X") graded correct only
    /// when the reef-lexical scoring path (`retrieval::node_text_score` via
    /// `Store::keyword_search_node_atoms`) surfaces the answer — see
    /// [`ReefFixture`].
    pub kind: String,
    pub query: String,
    /// Empty ⇔ abstention negative (the corpus cannot answer it).
    #[serde(default)]
    pub relevant_doc_ids: Vec<String>,
}

impl GoldenQuery {
    pub fn is_abstention(&self) -> bool {
        self.relevant_doc_ids.is_empty()
    }
}

/// Golden-category `kind`s that are computed and reported like any other
/// category (visible in `by_category`, the printed summary, and the JSON
/// report / baseline snapshot for BOTH embedders) but are excluded from:
///
/// 1. The **pooled** `overall.recall_at_20` / `overall.ndcg_at_10` (see
///    [`run_eval`]'s `ans_n`/`ans_r`/`ans_d` accumulation).
/// 2. The **per-category** blocking-gate floor (`retrieval_eval.rs::evaluate_gate`).
///
/// Today this is just `"distractor"`. That category exists to catch a real,
/// previously-observed failure mode — a topically-adjacent-but-lexically-wrong
/// document out-scoring the lexically-precise correct one under REAL semantic
/// vector similarity (e.g. 0.726 vs. 0.646 in production) — which the
/// deterministic [`HashEmbedder`] used by the *blocking* CI gate cannot
/// reproduce **by construction**: its cosine similarity tracks lexical overlap
/// (see `hash_embedder_overlap_beats_disjoint`), so it can't ever prefer a
/// topically-similar-but-imprecise doc over a token-precise one the way a real
/// embedding model can. Two consequences follow from that, both handled by
/// this exclusion:
///
/// - Under `hash`, this category is expected to score consistently strong —
///   that's not a "regression" to gate on, it's the deterministic embedder
///   being unable to fail the way it's designed to be failed. Gating pooled/
///   per-category floors on it would make the blocking gate depend on a
///   number that can only ever look artificially good, never actually signal
///   anything.
/// - The fixture pairs are *intentionally* built from highly-overlapping
///   vocabulary (that's what makes them distractors) — exactly the shape most
///   likely to trip [`HashEmbedder`]'s feature-hashing bucket collisions in a
///   way that has nothing to do with retrieval quality. A blocking floor on
///   that number would be gating on hash-collision noise, not signal.
///
/// The real, meaningful signal for this category lives in the *non-blocking*
/// weekly `EVAL_EMBEDDER=ort` run (`backend/eval/baseline.ort.json` /
/// `.github/workflows/retrieval-eval-ort.yml`), where the category can
/// actually exercise (and catch a regression in) real semantic-vs-lexical
/// ranking — see `backend/eval/README.md` ("Distractor golden category").
pub const NON_GATED_CATEGORIES: &[&str] = &["distractor"];

/// Read a JSONL file into a `Vec<T>`, skipping blank lines.
pub fn load_jsonl<T: for<'de> Deserialize<'de>>(path: &std::path::Path) -> std::io::Result<Vec<T>> {
    let text = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: T = serde_json::from_str(line).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{}:{}: {e}", path.display(), i + 1),
            )
        })?;
        out.push(v);
    }
    Ok(out)
}

// ── Deterministic atom IDs ─────────────────────────────────────────────────────

/// Derive a stable atom UUID from `(graph_id, doc_id, chunk_position)`.
///
/// The corpus/golden files reference documents by `doc_id`; the harness assigns
/// each ingested chunk this deterministic id so the query→document mapping is
/// stable and reproducible within a run (SHA-256 of a fixed namespace string,
/// shaped into a v4-form UUID; no new crate features — `sha2` is already a dep).
/// The `graph_id` is folded in so repeated local runs against a persistent
/// database don't collide on `atoms_pkey` (CI runs against a fresh container);
/// the committed golden set references only `doc_id`, so this internal
/// namespacing does not affect the stable query→document contract.
pub fn eval_atom_id(graph_id: Uuid, doc_id: &str, position: usize) -> Uuid {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"coralswarm-eval-atom:");
    hasher.update(graph_id.as_bytes());
    hasher.update(b":");
    hasher.update(doc_id.as_bytes());
    hasher.update(b":");
    hasher.update(position.to_le_bytes());
    let digest = hasher.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&digest[..16]);
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    Uuid::from_bytes(b)
}

// ── Deterministic hashing embedder (CI / no-model path) ────────────────────────

/// A deterministic, model-free embedder: a signed feature-hashing (bag-of-words)
/// vectoriser into [`EMBED_DIM`] dimensions, L2-normalised.
///
/// It is NOT a semantic model — but it is deterministic (fixed FNV-1a hash, no
/// SipHash randomisation), needs no 133 MB ONNX download, and gives cosine
/// similarity that tracks lexical overlap, so the vector-search path carries
/// real (if shallow) signal in CI. Real-model eval uses [`coralswarm_core::
/// embed_ort::OrtEmbedder`] locally; see `backend/eval/README.md`. Output is a
/// non-zero unit vector so `Store::set_atom_embeddings` (which rejects all-zero
/// stub vectors) accepts it.
pub struct HashEmbedder;

impl HashEmbedder {
    fn tokens(text: &str) -> Vec<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
            .map(|t| t.to_lowercase())
            .collect()
    }

    fn fnv1a(s: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in s.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }

    pub fn embed_text(text: &str) -> Vec<f32> {
        let mut v = vec![0f32; EMBED_DIM];
        for tok in Self::tokens(text) {
            let h = Self::fnv1a(&tok);
            let bucket = (h % EMBED_DIM as u64) as usize;
            // Signed hashing (Weinberger et al.) reduces collision bias.
            let sign = if (h >> 33) & 1 == 0 { 1.0 } else { -1.0 };
            v[bucket] += sign;
        }
        if v.iter().all(|&x| x == 0.0) {
            v[0] = 1.0; // guarantee a non-zero vector for empty/symbol-only input
        }
        l2_normalize(v).unwrap_or_else(|_| {
            let mut z = vec![0f32; EMBED_DIM];
            z[0] = 1.0;
            z
        })
    }
}

impl EmbedProvider for HashEmbedder {
    fn embed_batch<'a>(&'a self, inputs: &'a [EmbedInput], _: ProviderPermit) -> EmbedBatchFuture<'a> {
        Box::pin(async move { Ok(inputs.iter().map(|i| Self::embed_text(i.as_str())).collect()) })
    }
}

/// The eval harness's embeddings go through the gateway like every other,
/// as an explicitly unbilled purpose (hash or local ONNX embedder).
const EVAL: crate::embedding::Purpose<'static> =
    crate::embedding::Purpose::Unbilled(crate::embedding::Unbilled::Eval);

// ── Ingest ─────────────────────────────────────────────────────────────────────

fn reference_ts() -> DateTime<Utc> {
    std::env::var("EVAL_REFERENCE_TS")
        .ok()
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|| {
            DateTime::parse_from_rfc3339(EVAL_REFERENCE_TS_DEFAULT)
                .expect("valid default reference ts")
                .with_timezone(&Utc)
        })
}

/// Ingest the fixture corpus through the real chunker + header + FTS + embedding
/// path, mirroring `ingest::submit` step-for-step against a
/// [`crate::store::PostgresStore`]. Returns `doc_id → [atom_id]`.
///
/// Steps mirrored: chunk with the source-type strategy; build the meeting header
/// for transcript sources (`build_meeting_header`) and prepend it to each chunk;
/// `insert_atoms`; `set_atoms_inline_content`; stamp `header_len`; embed; write
/// `atoms.embedding` + `embed_model`/`chunker_version` via `set_atom_embeddings`;
/// build `content_tsv` with `to_tsvector('simple', …)` exactly as
/// `workers/embed.rs::update_content_tsv_and_clear_failure` does.
pub async fn ingest_corpus(
    store: &dyn Store,
    pool: &PgPool,
    embedder: &dyn EmbedProvider,
    graph_id: Uuid,
    ingested_by: Uuid,
    docs: &[FixtureDoc],
) -> Result<HashMap<String, Vec<Uuid>>, EvalError> {
    let ts = reference_ts();
    let mut doc_atoms: HashMap<String, Vec<Uuid>> = HashMap::new();

    // RLS scoping (org-tenancy B.3): the two `UPDATE atoms ...` statements
    // below are this module's only direct SQL, and `atoms` carries a
    // graph-mediated 0083 policy. `ingest_corpus` takes a `graph_id` but no
    // `account_id` (it is an offline eval harness, not a request path), so
    // the scoping account is resolved ONCE up front from the fixture ocean
    // via `resolve_graph_account_id` — the audited `SECURITY DEFINER`
    // bootstrap from migration `0084_bootstrap_scope_functions`. Every
    // per-doc statement then runs in its own short scoped transaction:
    // `ingest_corpus` interleaves DB writes with embedding calls (which can
    // be a network round-trip to a remote embedder), so one long transaction
    // spanning the whole corpus would pin a pooled connection across them —
    // the same reason PR #291's workers use a per-statement `JobScope`.
    let scope_account: Option<Uuid> = sqlx::query_scalar("SELECT resolve_graph_account_id($1)")
        .bind(graph_id)
        .fetch_one(pool)
        .await
        .map_err(|e| EvalError::Db {
            context: "resolve fixture ocean account".to_string(),
            source: e,
        })?;
    let scope_account = scope_account.ok_or_else(|| {
        EvalError::Fixture(format!("fixture ocean {graph_id} has no owning account"))
    })?;

    for doc in docs {
        let strategy = ChunkStrategy::for_source_type(&doc.source_type);
        let is_transcript = matches!(strategy, ChunkStrategy::Transcript);
        let chunks = chunker::chunk(&doc.content, strategy);
        if chunks.is_empty() {
            continue;
        }

        // The document's own date, parsed from the fixture, is what goes in the
        // meeting header — so temporal queries ("June 1") match the header's real
        // date rather than the shared reference timestamp. `ts` is kept below for
        // atom recency (the reranker's recency half-life keys off `atoms.timestamp`
        // and stamping all atoms identically neutralises that term across the corpus).
        let document_ts = chrono::NaiveDate::parse_from_str(&doc.date, "%Y-%m-%d")
            .map_err(|e| EvalError::Fixture(format!("invalid date for {}: {e}", doc.doc_id)))?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| EvalError::Fixture(format!("invalid time for {}", doc.doc_id)))?
            .and_utc();

        // Contextual meeting header — only for transcript-shaped sources, matching
        // `routes/ingest.rs::build_contextual_header`.
        let header = if is_transcript {
            chunker::build_meeting_header(
                doc.title.as_deref(),
                Some(document_ts),
                &doc.participants,
            )
        } else {
            None
        };
        let header_len: i32 = header.as_ref().map(|h| (h.len() + 2) as i32).unwrap_or(0);

        let mut ids = Vec::with_capacity(chunks.len());
        let mut source_ids = Vec::with_capacity(chunks.len());
        let mut blob_keys = Vec::with_capacity(chunks.len());
        let mut texts = Vec::with_capacity(chunks.len());
        // `atoms.title` (migration 0212), stamped below with `content_tsv`.
        // Production writes a title for EVERY atom — `routes::ingest` derives
        // `fallback_title` synchronously and the async sweep upgrades it to an
        // LLM headline — and `Store::keyword_search` weights that column at
        // tsvector class `A`. Leaving it NULL here would make the blocking
        // `retrieval-eval` gate blind to the entire title half of the keyword
        // leg, which is the half that carries most of its recall. A fixture
        // doc's own `title` stands in for the LLM headline (`title_kind =
        // 'llm'`); docs without one get the same `fallback_title` production
        // would compute, off the chunk BODY (header excluded, as in ingest).
        let mut titles: Vec<Option<String>> = Vec::with_capacity(chunks.len());
        let mut title_kinds: Vec<Option<String>> = Vec::with_capacity(chunks.len());
        for ci in &chunks {
            let atom_id = eval_atom_id(graph_id, &doc.doc_id, ci.position);
            let body = ci.text_with_speaker();
            match doc.title.as_deref() {
                Some(t) => {
                    titles.push(Some(t.to_string()));
                    title_kinds.push(Some("llm".to_string()));
                }
                None => {
                    let fallback = crate::services::atom_title::fallback_title(&body);
                    title_kinds.push(fallback.as_ref().map(|_| "fallback".to_string()));
                    titles.push(fallback);
                }
            }
            let text = match &header {
                Some(h) => format!("{h}\n\n{body}"),
                None => body,
            };
            source_ids.push(format!("{}:chunk:{}", doc.doc_id, ci.position));
            // Eval fixtures live on the consumer plane — 2-segment personal
            // key (org-tenancy PR 2.4).
            blob_keys.push(coralswarm_core::r2::personal_blob_key(graph_id, atom_id));
            texts.push(text);
            ids.push(atom_id);
        }

        store
            .insert_atoms(
                &ids,
                graph_id,
                &doc.source_type,
                &source_ids,
                &blob_keys,
                &doc.participants,
                ts,
                ingested_by,
                "private",
                "english",
            )
            .await
            .map_err(|e| EvalError::Store {
                context: format!("insert_atoms({})", doc.doc_id),
                source: e,
            })?;

        store
            .set_atoms_inline_content(&ids, &texts)
            .await
            .map_err(|e| EvalError::Store {
                context: format!("set_atoms_inline_content({})", doc.doc_id),
                source: e,
            })?;

        if header.is_some() {
            let mut tx = crate::tenancy::scope::begin_account_scoped(pool, scope_account)
                .await
                .map_err(|e| EvalError::Fixture(format!("header_len scoped tx: {e}")))?;
            sqlx::query("UPDATE atoms SET header_len = $2 WHERE atom_id = ANY($1)")
                .bind(&ids)
                .bind(header_len)
                .execute(&mut *tx)
                .await
                .map_err(|e| EvalError::Db {
                    context: format!("stamp header_len({})", doc.doc_id),
                    source: e,
                })?;
            tx.commit().await.map_err(|e| EvalError::Db {
                context: format!("stamp header_len commit({})", doc.doc_id),
                source: e,
            })?;
        }

        let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        let embeddings = crate::embedding::submit(embedder, EVAL, &refs)
            .await
            .map_err(|e| EvalError::Embed {
                context: format!("embed_batch({})", doc.doc_id),
                source: e.error,
            })?;
        store
            .set_atom_embeddings(&ids, &embeddings)
            .await
            .map_err(|e| EvalError::Store {
                context: format!("set_atom_embeddings({})", doc.doc_id),
                source: e,
            })?;
        // `vector_search` compares `embedding_next` and requires
        // `embed_model_next = EMBED_MODEL_CLOUD` (#1032). The hash embedder
        // is the stand-in for that cloud vector — same width, and the query
        // vector comes from the same function — so both sides of the
        // comparison have to land in that column. `set_atom_embeddings`
        // only fills the live bge column, which server search does not read. Leaving `embedding_next` null drops the vector leg
        // and the gate scores keyword-only.
        store
            .set_cloud_embeddings(&ids, &embeddings)
            .await
            .map_err(|e| EvalError::Store {
                context: format!("set_cloud_embeddings({})", doc.doc_id),
                source: e,
            })?;

        // `atoms.excerpt` (migration 0116), stamped below alongside the title.
        // `routes::ingest` writes it for EVERY chunk at write time, from the
        // same `derive_journal_excerpt` used here, and `Store::keyword_search`
        // runs fix 3b's SYMBOL candidate leg against it. Leaving it NULL would
        // make the blocking `retrieval-eval` gate blind to that entire leg —
        // the same blindness this function had for `atoms.title` before
        // `#1035` fixed it, and for the same reason: the fixture ingest is a
        // hand-written mirror of the production write path, so anything
        // production writes and retrieval reads has to be written here too.
        let excerpts: Vec<Option<String>> = texts
            .iter()
            .map(|t| {
                crate::store::derive_journal_excerpt(
                    Some(t),
                    header.as_ref().map(|_| header_len),
                )
            })
            .collect();

        // content_tsv, built with 'simple' — identical to the embed worker —
        // and `title`/`title_kind`/`excerpt`, identical to `routes::ingest` +
        // `workers::atom_title_backfill`. All in one statement so an atom is
        // never left in a shape the real pipeline would not produce.
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, scope_account)
            .await
            .map_err(|e| EvalError::Fixture(format!("content_tsv scoped tx: {e}")))?;
        sqlx::query(
            r#"UPDATE atoms SET content_tsv = to_tsvector('simple', v.content),
                                title       = v.title,
                                title_kind  = v.title_kind,
                                excerpt     = v.excerpt
               FROM (SELECT unnest($1::uuid[]) AS atom_id,
                            unnest($2::text[]) AS content,
                            unnest($3::text[]) AS title,
                            unnest($4::text[]) AS title_kind,
                            unnest($5::text[]) AS excerpt) v
               WHERE atoms.atom_id = v.atom_id"#,
        )
        .bind(&ids)
        .bind(&texts)
        .bind(&titles)
        .bind(&title_kinds)
        .bind(&excerpts)
        .execute(&mut *tx)
        .await
        .map_err(|e| EvalError::Db {
            context: format!("content_tsv({})", doc.doc_id),
            source: e,
        })?;
        tx.commit().await.map_err(|e| EvalError::Db {
            context: format!("content_tsv commit({})", doc.doc_id),
            source: e,
        })?;

        doc_atoms.insert(doc.doc_id.clone(), ids);
    }

    Ok(doc_atoms)
}

// ── Reef (node) fixtures ─────────────────────────────────────────────────────

/// One curated reef (node) fixture: a named entity/decision linked to the
/// atoms of one or more already-ingested fixture docs (`FixtureDoc::doc_id`,
/// via [`ingest_corpus`]'s returned `doc_id → atom_ids` map).
///
/// Retrieval is atom-first — vector + keyword search run over corals, and
/// reefs are only ever reached by expanding *up* from an atom hit or via
/// `Store::keyword_search_node_atoms`'s reef-lexical scoring
/// (`retrieval::node_text_score`). `eval.rs`'s ingest path (`ingest_corpus`)
/// never inserted `nodes`/`node_atoms` rows before this, so a regression in
/// that reef-lexical path moved zero eval metrics. These fixtures close that
/// gap for the `"reef"` golden category (`golden/golden.jsonl`).
///
/// Mirrors the real dogfooded failure mode this exists to catch: a raw
/// diarized transcript often only has a generic placeholder speaker label
/// ("Speaker 1") until a human renames the auto-created reef to the person's
/// real name (see `workers/extract.rs::is_placeholder_entity_name`) — so the
/// atom's own content never contains the real name at all, and only the
/// reef's `title`/digest `summary` do. `backend/eval/fixtures/corpus.jsonl`'s
/// `r01`–`r06` docs deliberately use `"Speaker 1"`/`"Speaker 2"` (never the
/// real name) for exactly this reason: a query like "who is Dorian" can only
/// be answered by `keyword_search_node_atoms` reaching this reef's `title`,
/// not by plain atom-content keyword or (hash-embedder) vector search, which
/// is what makes the `"reef"` golden category a genuine test of this code
/// path rather than a redundant restatement of `"direct"`.
#[derive(Debug, Clone, Deserialize)]
pub struct ReefFixture {
    /// Stable fixture-local id (diagnostics only; not written to the DB).
    pub node_id: String,
    /// `nodes.type` — the reef-write path only accepts `"entity"` here:
    /// `Store::upsert_node` doesn't populate `body->>'decided_at'`, and
    /// `nodes`' own CHECK constraint (`0011_nodes.sql`) requires that field
    /// for `type = 'decision'`. Production collapses every extracted type to
    /// `"entity"` for the same reason (`workers::source_atoms::map_node_type`).
    pub node_type: String,
    pub title: String,
    /// Digest summary text — becomes `body->'digest'->>'summary'`, the second
    /// (lower-weighted) field `node_text_score` scores a query against.
    #[serde(default)]
    pub summary: String,
    /// `FixtureDoc::doc_id`s (already ingested via [`ingest_corpus`]) whose
    /// atoms this reef links to.
    pub atom_doc_ids: Vec<String>,
}

/// Insert `reefs` as real `nodes` rows, linked to the already-ingested atoms
/// of their `atom_doc_ids` (via `doc_atoms`, [`ingest_corpus`]'s return
/// value), through the real `Store::upsert_node` / `Store::set_node_digest` /
/// `Store::link_node_atoms` write path — mirroring how the extract worker
/// creates reefs in production. Returns `node_id (fixture) → node_id (DB)`.
///
/// Optional / additive: a corpus with no `reef.jsonl` (or an empty `reefs`
/// slice) simply skips this — every other eval category is unaffected.
pub async fn ingest_reef_fixtures(
    store: &dyn Store,
    graph_id: Uuid,
    doc_atoms: &HashMap<String, Vec<Uuid>>,
    reefs: &[ReefFixture],
) -> Result<HashMap<String, Uuid>, EvalError> {
    let mut node_ids: HashMap<String, Uuid> = HashMap::with_capacity(reefs.len());

    for r in reefs {
        let node_id = store
            .upsert_node(graph_id, &r.node_type, &r.title)
            .await
            .map_err(|e| EvalError::Store {
                context: format!("upsert_node({})", r.node_id),
                source: e,
            })?;

        if !r.summary.is_empty() {
            let digest_json = serde_json::json!({
                "summary": r.summary,
                "atoms_key": "",
                "excerpts": [],
            })
            .to_string();
            store
                .set_node_digest(node_id, &digest_json)
                .await
                .map_err(|e| EvalError::Store {
                    context: format!("set_node_digest({})", r.node_id),
                    source: e,
                })?;
        }

        let mut pairs: Vec<(Uuid, Uuid)> = Vec::new();
        for doc_id in &r.atom_doc_ids {
            let atoms = doc_atoms.get(doc_id).ok_or_else(|| {
                EvalError::Fixture(format!(
                    "reef fixture {} references unknown doc_id {doc_id} \
                     (must be a doc_id already ingested via ingest_corpus)",
                    r.node_id
                ))
            })?;
            pairs.extend(atoms.iter().map(|&aid| (node_id, aid)));
        }
        if !pairs.is_empty() {
            store
                .link_node_atoms(&pairs)
                .await
                .map_err(|e| EvalError::Store {
                    context: format!("link_node_atoms({})", r.node_id),
                    source: e,
                })?;
        }

        node_ids.insert(r.node_id.clone(), node_id);
    }

    Ok(node_ids)
}

/// Resolve the unambiguous member selected by each single-document fixture.
/// Multi-member fixtures must supply an explicit selection instead of guessing.
pub fn single_member_fixture_supports(
    doc_atoms: &HashMap<String, Vec<Uuid>>,
    reefs: &[ReefFixture],
) -> Result<HashMap<String, Uuid>, EvalError> {
    reefs.iter().map(|fixture| {
        if fixture.atom_doc_ids.len() != 1 {
            return Err(EvalError::Fixture("fixture needs an explicit support selection".into()));
        }
        let members = doc_atoms.get(&fixture.atom_doc_ids[0])
            .ok_or_else(|| EvalError::Fixture("fixture support unavailable".into()))?;
        if members.len() != 1 {
            return Err(EvalError::Fixture("fixture needs an explicit support selection".into()));
        }
        Ok((fixture.node_id.clone(), members[0]))
    }).collect()
}

/// Apply the committed human-curated identity labels through the production
/// writer. The fixture's sole declared document/member is its explicit support;
/// captured anonymous text and generated summaries are never made authority.
#[allow(clippy::too_many_arguments)]
pub async fn curate_reef_fixtures(
    store: &dyn Store,
    pool: &PgPool,
    graph_id: Uuid,
    account_id: Uuid,
    actor: Uuid,
    explicit_supports: &HashMap<String, Uuid>,
    reefs: &[ReefFixture],
    node_ids: &HashMap<String, Uuid>,
) -> Result<(), EvalError> {
    for fixture in reefs {
        let support = *explicit_supports.get(&fixture.node_id)
            .ok_or_else(|| EvalError::Fixture("curated reef needs explicit member support".into()))?;
        let node = *node_ids.get(&fixture.node_id)
            .ok_or_else(|| EvalError::Fixture("curated reef unavailable".into()))?;
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await.map_err(|source| EvalError::Store { context: "curated fixture scope".into(), source })?;
        let node_graph: Option<Uuid> = sqlx::query_scalar("SELECT graph_id FROM public.nodes WHERE node_id=$1")
            .bind(node).fetch_optional(&mut *tx).await
            .map_err(|source| EvalError::Db { context: "curated fixture graph".into(), source })?;
        if node_graph != Some(graph_id) {
            return Err(EvalError::Fixture("curated reef belongs to another graph".into()));
        }
        let expected = store.node_version_scoped(&mut tx, node).await
            .map_err(|source| EvalError::Store { context: "curated fixture version".into(), source })?;
        crate::services::reef_identity::patch_supported(store, &mut tx, account_id,
            actor, node, &fixture.title, None, expected, support).await
            .map_err(|source| EvalError::Store { context: "curated fixture identity".into(), source })?;
        tx.commit().await.map_err(|source| EvalError::Db { context: "curated fixture commit".into(), source })?;
    }
    Ok(())
}

// ── Retrieval ──────────────────────────────────────────────────────────────────

/// Outcome of running one query through the mirrored retrieval path.
pub struct RetrievalOutcome {
    /// Reranked atom IDs, best-first (up to `top_k`).
    pub ranked_ids: Vec<Uuid>,
    /// The same reranked set as `ranked_ids`, but carrying each atom's full
    /// [`crate::retrieval::rerank::RankedAtom`] (score/`vector_sim`/
    /// `keyword_norm`/`node_id`) rather than just its id. Added for
    /// `backend/tests/graph_temporal_eval_pg.rs` (§7.1's fixture-ocean
    /// harness, an external integration-test crate that can only see this
    /// module's `pub` surface): that suite's tie-break-robust assertions
    /// (`rank_best_case`/`rank_worst_case`) need every candidate's `score` to
    /// resolve exact ties consistently — something a bare `Vec<Uuid>` cannot
    /// provide — and the deterministic `HashEmbedder` it uses produces exact
    /// score ties routinely (see that file's own doc comments). Populated by
    /// both [`run_retrieval`] and [`run_multi_ocean_retrieval`]; `ranked_ids`
    /// is kept alongside it (rather than removed) so every existing caller of
    /// this struct's original field is unaffected.
    pub ranked: Vec<crate::retrieval::rerank::RankedAtom>,
    /// Whether the honesty guard would abstain (no atom cleared the floor).
    pub abstained: bool,
    /// Top reranked score (for diagnostics / floor recalibration).
    pub top_score: f64,
    /// §7.4 per-gate retrieval trace — `Some` only from
    /// [`run_multi_ocean_retrieval`], which actually calls
    /// `routes::queries::multi_graph_retrieve`. [`run_retrieval`] mirrors
    /// that path by hand (single ocean, no supersession/per-ocean-floor/
    /// context-cap-scaling machinery) and never produces a real trace, so it
    /// stays `None` there — never a fabricated trace standing in for a call
    /// that didn't happen. This is what lets a §7.1 fixture case (run via
    /// `--multi-ocean`) name WHICH gate dropped a `must_retrieve` atom
    /// instead of leaving it to be bisected by hand.
    pub trace: Option<crate::routes::queries::RetrievalTrace>,
}

/// Run one query through the same composition as `routes/queries.rs::create_query`:
/// vector + keyword + reef-lexical search → merge → expand → rerank → honesty guard.
///
/// `rerank_params` is threaded through (rather than hardcoded) so tooling like
/// `retrieval_eval --sweep` can vary alpha/beta without touching production
/// defaults — callers that want the shipped behavior pass
/// `&RerankParams::default()`.
pub async fn run_retrieval(
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    graph_id: Uuid,
    query: &str,
    top_k: usize,
    rerank_params: &RerankParams,
) -> Result<RetrievalOutcome, EvalError> {
    let embedding = crate::embedding::submit_one(embedder, EVAL, query).await.map_err(|e| EvalError::Embed {
        context: "embed query".to_string(),
        source: e.error,
    })?;
    let filter = RetrievalFilter::default();

    // Retrieval failures must fail the harness loudly — silently coercing a DB
    // error into an empty result set would score as poor recall (or a spurious
    // abstention) and corrupt the baseline. Propagate every store call.
    let vector_hits = store
        .vector_search(graph_id, &embedding, RETRIEVAL_CANDIDATE_K, &filter)
        .await
        .map_err(|e| EvalError::Store {
            context: "vector_search".to_string(),
            source: e,
        })?;
    let keyword_hits = store
        .keyword_search(graph_id, query, RETRIEVAL_CANDIDATE_K, &filter)
        .await
        .map_err(|e| EvalError::Store {
            context: "keyword_search".to_string(),
            source: e,
        })?;
    let node_atom_hits = store
        .keyword_search_node_atoms(graph_id, query, NODE_CANDIDATE_K, &filter)
        .await
        .map_err(|e| EvalError::Store {
            context: "keyword_search_node_atoms".to_string(),
            source: e,
        })?;

    let keyword_hits = merge_keyword_hits(keyword_hits, node_atom_hits);

    let mut seed_ids: Vec<Uuid> = vector_hits
        .iter()
        .map(|(id, _)| *id)
        .chain(keyword_hits.iter().map(|(id, _)| *id))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    seed_ids.sort();

    let nodes = store
        .expand(graph_id, &seed_ids, Some(embedding.as_slice()))
        .await
        .map_err(|e| EvalError::Store {
            context: "expand".to_string(),
            source: e,
        })?;
    let node_map: HashMap<Uuid, Uuid> = nodes
        .iter()
        .flat_map(|n| n.atom_ids.iter().map(move |&aid| (aid, n.node_id)))
        .collect();

    let ts_rows = store
        .atom_timestamps(&seed_ids)
        .await
        .map_err(|e| EvalError::Store {
            context: "atom_timestamps".to_string(),
            source: e,
        })?;
    let timestamps: HashMap<Uuid, DateTime<Utc>> = ts_rows
        .into_iter()
        .filter_map(|(id, ts)| ts.map(|t| (id, t)))
        .collect();

    let reranked = rerank(
        &vector_hits,
        &keyword_hits,
        &timestamps,
        &node_map,
        rerank_params,
        top_k,
    );

    let top_score = reranked.first().map(|a| a.score).unwrap_or(0.0);
    let abstained =
        reranked.is_empty() || !reranked.iter().any(|a| a.score >= ask_relevance_floor());

    Ok(RetrievalOutcome {
        ranked_ids: reranked.iter().map(|a| a.atom_id).collect(),
        ranked: reranked,
        abstained,
        top_score,
        // This function hand-mirrors `multi_graph_retrieve` rather than
        // calling it — see `RetrievalOutcome::trace`'s doc comment.
        trace: None,
    })
}

// ── Metrics ────────────────────────────────────────────────────────────────────

use std::hash::Hash;

/// Recall@k: fraction of the relevant items that appear in the top-`k`.
///
/// Generic over the retrieval unit. The harness scores at **document level**
/// (meeting / Slack message), because a transcript ingests as one atom per
/// speaker turn — so the user-meaningful question is "did we surface the
/// document that answers this?", and a relevant doc counts once regardless of
/// how many of its turns are retrieved. See [`ranked_docs_dedup`].
pub fn recall_at_k<T: Eq + Hash>(ranked: &[T], relevant: &HashSet<T>, k: usize) -> f64 {
    if relevant.is_empty() {
        return 0.0;
    }
    let hits = ranked
        .iter()
        .take(k)
        .filter(|id| relevant.contains(id))
        .count();
    hits as f64 / relevant.len() as f64
}

/// nDCG@k with binary relevance (IDCG assumes all relevant items at the top).
pub fn ndcg_at_k<T: Eq + Hash>(ranked: &[T], relevant: &HashSet<T>, k: usize) -> f64 {
    if relevant.is_empty() {
        return 0.0;
    }
    let mut dcg = 0.0;
    for (i, id) in ranked.iter().take(k).enumerate() {
        if relevant.contains(id) {
            dcg += 1.0 / ((i as f64) + 2.0).log2();
        }
    }
    let ideal_hits = relevant.len().min(k);
    let mut idcg = 0.0;
    for i in 0..ideal_hits {
        idcg += 1.0 / ((i as f64) + 2.0).log2();
    }
    if idcg == 0.0 {
        0.0
    } else {
        dcg / idcg
    }
}

/// Collapse a ranked list of atom IDs to distinct document IDs, keeping each
/// document at the position of its best (first-appearing) atom.
pub fn ranked_docs_dedup(ranked_atoms: &[Uuid], atom_to_doc: &HashMap<Uuid, String>) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out = Vec::new();
    for a in ranked_atoms {
        if let Some(d) = atom_to_doc.get(a) {
            if seen.insert(d.clone()) {
                out.push(d.clone());
            }
        }
    }
    out
}

// ── Report ─────────────────────────────────────────────────────────────────────

/// Aggregate metrics for one category (or overall).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CategoryMetrics {
    pub queries: usize,
    /// Mean Recall@20 over answerable (non-abstention) queries in this bucket.
    pub recall_at_20: f64,
    /// Mean nDCG@10 over answerable queries in this bucket.
    pub ndcg_at_10: f64,
    /// Abstention accuracy: fraction of queries whose abstain decision was
    /// correct (abstention negatives should abstain; answerable should not).
    pub abstention_accuracy: f64,
    /// Raw numerator of `abstention_accuracy` (`abstention_accuracy * queries`,
    /// pre-division) — kept alongside the fraction so the CI gate can apply a
    /// **count floor** (e.g. "no more than 1 regression") instead of a fractional
    /// tolerance, which is noisy at the small `n` the abstention category has.
    pub abstention_correct: usize,
}

/// Per-query detail (for the JSON artifact / debugging).
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub id: String,
    pub kind: String,
    pub abstained: bool,
    pub expected_abstention: bool,
    pub recall_at_20: f64,
    pub ndcg_at_10: f64,
    pub top_score: f64,
}

/// The full machine-readable eval report.
#[derive(Debug, Clone, Serialize)]
pub struct EvalReport {
    pub embedder: String,
    pub embed_model: String,
    pub top_k: usize,
    pub corpus_docs: usize,
    pub golden_queries: usize,
    pub overall: CategoryMetrics,
    pub by_category: HashMap<String, CategoryMetrics>,
    pub per_query: Vec<QueryResult>,
}

/// Per-category snapshot inside a [`Baseline`] — recall/nDCG plus the raw
/// abstention correct/total counts (not just the fraction), so the CI gate can
/// apply a count floor to the abstention category instead of a fractional
/// tolerance (see `retrieval_eval.rs::gate_against_baseline`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct CategoryBaseline {
    pub queries: usize,
    pub recall_at_20: f64,
    pub ndcg_at_10: f64,
    pub abstention_accuracy: f64,
    pub abstention_correct: usize,
}

impl From<&CategoryMetrics> for CategoryBaseline {
    fn from(m: &CategoryMetrics) -> Self {
        CategoryBaseline {
            queries: m.queries,
            recall_at_20: m.recall_at_20,
            ndcg_at_10: m.ndcg_at_10,
            abstention_accuracy: m.abstention_accuracy,
            abstention_correct: m.abstention_correct,
        }
    }
}

/// The committed baseline the CI gate compares against.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Baseline {
    pub embedder: String,
    pub recall_at_20: f64,
    pub ndcg_at_10: f64,
    /// Overall abstention accuracy (all categories pooled). Kept for
    /// diagnostics/back-compat; the CI gate's binding abstention check is the
    /// count floor on `by_category["abstention"]`, not this pooled fraction.
    #[serde(default)]
    pub abstention_accuracy: f64,
    /// Per-category snapshot (recall/nDCG/abstention), so a regression confined
    /// to one small category can't hide inside the pooled tolerance. `#[serde(default)]`
    /// so baselines recorded before this field existed still parse (the gate
    /// treats a category missing from an old baseline as "not comparable, skip"
    /// rather than a hard failure).
    #[serde(default)]
    pub by_category: HashMap<String, CategoryBaseline>,
    /// Reference timestamp fixtures were stamped with when this was recorded.
    #[serde(default)]
    pub reference_ts: String,
    #[serde(default)]
    pub note: String,
}

/// Run the full evaluation: for each golden query, retrieve and score.
///
/// `rerank_params` defaults to `RerankParams::default()` for the normal (gate)
/// path; `retrieval_eval --sweep` passes varied alpha/beta to build the tuning
/// grid — see `backend/eval/README.md`.
///
/// Single-graph retrieval only (`run_retrieval`, which mirrors the single-ocean
/// `/v1/queries` composition). See [`run_eval_multi_ocean`] for the sibling
/// that instead fans a query out across several graphs through
/// `multi_graph_retrieve` — the two share all scoring/aggregation via
/// [`aggregate_eval_report`], and differ only in how a single query's
/// [`RetrievalOutcome`] is obtained.
pub async fn run_eval(
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    graph_id: Uuid,
    doc_atoms: &HashMap<String, Vec<Uuid>>,
    golden: &[GoldenQuery],
    embedder_name: &str,
    top_k: usize,
    rerank_params: &RerankParams,
) -> Result<EvalReport, EvalError> {
    let atom_to_doc = atom_to_doc_map(doc_atoms);
    let mut graded = Vec::with_capacity(golden.len());
    for g in golden {
        let outcome =
            run_retrieval(store, embedder, graph_id, &g.query, top_k, rerank_params).await?;
        graded.push(grade_query(g, &outcome, &atom_to_doc));
    }
    Ok(aggregate_eval_report(
        graded,
        embedder_name,
        top_k,
        doc_atoms.len(),
    ))
}

/// `atom_id → doc_id`, built once and shared by every query's grading in a run
/// ([`run_eval`] / [`run_eval_multi_ocean`]) — retrieval scores at atom
/// granularity but the golden set (and therefore Recall@k/nDCG@k) is graded at
/// document level (see `recall_at_k`'s doc comment).
fn atom_to_doc_map(doc_atoms: &HashMap<String, Vec<Uuid>>) -> HashMap<Uuid, String> {
    doc_atoms
        .iter()
        .flat_map(|(doc, ids)| ids.iter().map(move |&id| (id, doc.clone())))
        .collect()
}

/// One golden query's graded outcome, retrieval-path-agnostic — produced by
/// either [`run_eval`] (single-graph `run_retrieval`) or
/// [`run_eval_multi_ocean`] (fanned-out `multi_graph_retrieve`), then folded
/// into an [`EvalReport`] by [`aggregate_eval_report`] identically regardless
/// of which retrieval path produced it.
struct GradedQuery<'g> {
    golden: &'g GoldenQuery,
    recall: f64,
    ndcg: f64,
    abstained: bool,
    top_score: f64,
}

/// Score one golden query's [`RetrievalOutcome`] against its expected
/// `relevant_doc_ids` — shared by [`run_eval`] and [`run_eval_multi_ocean`] so
/// both retrieval paths are graded identically.
fn grade_query<'g>(
    g: &'g GoldenQuery,
    outcome: &RetrievalOutcome,
    atom_to_doc: &HashMap<Uuid, String>,
) -> GradedQuery<'g> {
    let relevant: HashSet<String> = g.relevant_doc_ids.iter().cloned().collect();
    let ranked_docs = ranked_docs_dedup(&outcome.ranked_ids, atom_to_doc);
    let (recall, ndcg) = if g.is_abstention() {
        (0.0, 0.0)
    } else {
        (
            recall_at_k(&ranked_docs, &relevant, 20),
            ndcg_at_k(&ranked_docs, &relevant, 10),
        )
    };
    GradedQuery {
        golden: g,
        recall,
        ndcg,
        abstained: outcome.abstained,
        top_score: outcome.top_score,
    }
}

/// Fold every golden query's [`GradedQuery`] into the pooled + per-category
/// [`EvalReport`] — the aggregation math [`run_eval`] and
/// [`run_eval_multi_ocean`] share verbatim (see [`NON_GATED_CATEGORIES`] for
/// the pooled-recall/nDCG exclusion this applies).
fn aggregate_eval_report(
    graded: Vec<GradedQuery<'_>>,
    embedder_name: &str,
    top_k: usize,
    corpus_docs: usize,
) -> EvalReport {
    let mut per_query = Vec::with_capacity(graded.len());
    // Accumulators keyed by category.
    let mut cat_answerable: HashMap<String, (f64, f64, usize)> = HashMap::new(); // (recall_sum, ndcg_sum, n)
    let mut cat_abstain_correct: HashMap<String, (usize, usize)> = HashMap::new(); // (correct, total)
    let golden_queries = graded.len();

    for gq in graded {
        let g = gq.golden;
        let abstain_correct = gq.abstained == g.is_abstention();

        if !g.is_abstention() {
            let e = cat_answerable.entry(g.kind.clone()).or_default();
            e.0 += gq.recall;
            e.1 += gq.ndcg;
            e.2 += 1;
        }
        let a = cat_abstain_correct.entry(g.kind.clone()).or_default();
        a.1 += 1;
        if abstain_correct {
            a.0 += 1;
        }

        per_query.push(QueryResult {
            id: g.id.clone(),
            kind: g.kind.clone(),
            abstained: gq.abstained,
            expected_abstention: g.is_abstention(),
            recall_at_20: gq.recall,
            ndcg_at_10: gq.ndcg,
            top_score: gq.top_score,
        });
    }

    // Build per-category metrics.
    let mut by_category: HashMap<String, CategoryMetrics> = HashMap::new();
    let mut all_kinds: HashSet<String> = HashSet::new();
    all_kinds.extend(cat_answerable.keys().cloned());
    all_kinds.extend(cat_abstain_correct.keys().cloned());
    for kind in all_kinds {
        let (rsum, nsum, n) = cat_answerable.get(&kind).copied().unwrap_or((0.0, 0.0, 0));
        let (ac, at) = cat_abstain_correct.get(&kind).copied().unwrap_or((0, 0));
        by_category.insert(
            kind,
            CategoryMetrics {
                queries: at,
                recall_at_20: if n > 0 { rsum / n as f64 } else { 0.0 },
                ndcg_at_10: if n > 0 { nsum / n as f64 } else { 0.0 },
                abstention_accuracy: if at > 0 { ac as f64 / at as f64 } else { 0.0 },
                abstention_correct: ac,
            },
        );
    }

    // Overall: recall/ndcg averaged over answerable, GATE-RELEVANT queries (see
    // `NON_GATED_CATEGORIES` — a category listed there, e.g. "distractor", is
    // still fully reported per-category in `by_category` above, just excluded
    // from this pooled figure so it can't move the blocking gate's headline
    // numbers for reasons unrelated to a real regression); abstention accuracy
    // over all queries (unaffected — that pooled figure isn't part of the
    // per-category-noise concern `NON_GATED_CATEGORIES` exists for).
    let ans_n: usize = cat_answerable
        .iter()
        .filter(|(k, _)| !NON_GATED_CATEGORIES.contains(&k.as_str()))
        .map(|(_, (_, _, n))| *n)
        .sum();
    let ans_r: f64 = cat_answerable
        .iter()
        .filter(|(k, _)| !NON_GATED_CATEGORIES.contains(&k.as_str()))
        .map(|(_, (r, _, _))| *r)
        .sum();
    let ans_d: f64 = cat_answerable
        .iter()
        .filter(|(k, _)| !NON_GATED_CATEGORIES.contains(&k.as_str()))
        .map(|(_, (_, d, _))| *d)
        .sum();
    let abs_correct: usize = cat_abstain_correct.values().map(|(c, _)| *c).sum();
    let abs_total: usize = cat_abstain_correct.values().map(|(_, t)| *t).sum();

    let overall = CategoryMetrics {
        queries: golden_queries,
        recall_at_20: if ans_n > 0 { ans_r / ans_n as f64 } else { 0.0 },
        ndcg_at_10: if ans_n > 0 { ans_d / ans_n as f64 } else { 0.0 },
        abstention_accuracy: if abs_total > 0 {
            abs_correct as f64 / abs_total as f64
        } else {
            0.0
        },
        abstention_correct: abs_correct,
    };

    EvalReport {
        embedder: embedder_name.to_string(),
        embed_model: EMBED_MODEL_NAME.to_string(),
        top_k,
        corpus_docs,
        golden_queries,
        overall,
        by_category,
        per_query,
    }
}

/// Multi-ocean sibling of [`run_eval`]: every golden query is answered through
/// [`crate::routes::queries::multi_graph_retrieve`], fanned out across every
/// graph in `graph_ids`, instead of the single-graph `run_retrieval` path —
/// exercising the per-ocean floor guarantee and cross-ocean merge/rerank that
/// a single-graph eval run never reaches (`multi_graph_retrieve` is otherwise
/// unexercised by this harness — see `backend/eval/README.md`, "Multi-ocean
/// fan-out eval mode"). Uses `RerankParams::default()` (not threaded through,
/// unlike [`run_eval`]'s `--sweep` support) — this mode is opt-in diagnostic
/// tooling (`retrieval_eval --multi-ocean`), not part of the default/blocking
/// eval run, so it doesn't need the sweep hook.
///
/// `doc_atoms` must already reflect ALL oceans' ingested atoms (i.e. the
/// caller merges each ocean's own `ingest_corpus` return value before calling
/// this) — grading is still done at document level via `doc_id`, which stays
/// globally unique across oceans in the fixture corpus.
pub async fn run_eval_multi_ocean(
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    graph_ids: &[Uuid],
    doc_atoms: &HashMap<String, Vec<Uuid>>,
    golden: &[GoldenQuery],
    embedder_name: &str,
    top_k: usize,
) -> Result<EvalReport, EvalError> {
    let atom_to_doc = atom_to_doc_map(doc_atoms);
    let mut graded = Vec::with_capacity(golden.len());
    for g in golden {
        let outcome = run_multi_ocean_retrieval(store, embedder, graph_ids, &g.query, top_k).await?;
        graded.push(grade_query(g, &outcome, &atom_to_doc));
    }
    Ok(aggregate_eval_report(
        graded,
        embedder_name,
        top_k,
        doc_atoms.len(),
    ))
}

/// Run one query through [`crate::routes::queries::multi_graph_retrieve`] —
/// the multi-ocean sibling of [`run_retrieval`]. See that function's doc
/// comment for the shared honesty-guard / top-score semantics; the only
/// difference here is the retrieval call itself fans out across `graph_ids`
/// and applies the per-ocean floor guarantee / cross-ocean merge instead of
/// running a single graph.
pub async fn run_multi_ocean_retrieval(
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    graph_ids: &[Uuid],
    query: &str,
    top_k: usize,
) -> Result<RetrievalOutcome, EvalError> {
    let embedding = crate::embedding::submit_one(embedder, EVAL, query).await.map_err(|e| EvalError::Embed {
        context: "embed query".to_string(),
        source: e.error,
    })?;
    let filter = RetrievalFilter::default();

    // Offline eval harness: no live per-request pool/RLS concern here (same
    // reasoning as `bin/retrieval_eval.rs`), so this takes the pool-less
    // (unscoped) branch. `Uuid::nil()`:
    // `multi_graph_retrieve` never reads `user_id` on the `pool: None` branch
    // (no scoped transaction is ever opened), so this value is inert here —
    // there is no real acting-user concept in an offline eval run.
    let mgr = crate::routes::queries::multi_graph_retrieve(
        store,
        None,
        Uuid::nil(),
        graph_ids,
        &embedding,
        query,
        &filter,
        top_k,
    )
    .await;

    if !mgr.unreachable.is_empty() {
        // `multi_graph_retrieve` degrades per-ocean (G25) rather than failing
        // the whole call, so an eval fixture ocean that failed doesn't panic —
        // but it's not silent either: it would quietly understate recall for
        // any query whose relevant doc lived in that ocean, so surface it
        // loudly for whoever is reading `--multi-ocean` output.
        eprintln!(
            "retrieval_eval --multi-ocean: {} ocean(s) unreachable for query {query:?}: {:?}",
            mgr.unreachable.len(),
            mgr.unreachable
        );
    }

    let top_score = mgr.reranked.first().map(|a| a.score).unwrap_or(0.0);
    Ok(RetrievalOutcome {
        ranked_ids: mgr.reranked.iter().map(|a| a.atom_id).collect(),
        ranked: mgr.reranked,
        abstained: !mgr.any_cleared_floor,
        top_score,
        // §7.4: the real per-gate trace `multi_graph_retrieve` computed for
        // this call — so a `must_retrieve` failure on this path names WHICH
        // gate dropped the atom instead of leaving it to be bisected by hand.
        trace: Some(mgr.trace),
    })
}

/// A distinct throwaway graph name per run so repeated local runs against a
/// persistent DB don't collide on `insert_atoms`' `(graph_id, source_type,
/// source_id)` uniqueness.
pub fn unique_graph_name() -> String {
    let now = Utc::now();
    format!("eval-{}", now.format("%Y%m%d%H%M%S%3f"))
}

// ── Tests (pure, no DB) ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_atom_id_is_deterministic_and_valid_v4() {
        let g = Uuid::from_u128(1);
        let a = eval_atom_id(g, "m01", 0);
        let b = eval_atom_id(g, "m01", 0);
        assert_eq!(a, b);
        assert_ne!(eval_atom_id(g, "m01", 0), eval_atom_id(g, "m01", 1));
        assert_ne!(eval_atom_id(g, "m01", 0), eval_atom_id(g, "m02", 0));
        // Different graph ⇒ different id (avoids cross-run pkey collisions).
        assert_ne!(eval_atom_id(g, "m01", 0), eval_atom_id(Uuid::from_u128(2), "m01", 0));
        assert_eq!(a.get_version_num(), 4);
    }

    #[test]
    fn hash_embedder_is_unit_norm_and_nonzero() {
        let v = HashEmbedder::embed_text("Stripe payment go-live August 15");
        assert_eq!(v.len(), EMBED_DIM);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4, "norm={norm}");
        assert!(v.iter().any(|&x| x != 0.0));
        // empty / symbol-only input must still be a valid non-zero unit vector.
        let e = HashEmbedder::embed_text("!!! ???");
        let en: f32 = e.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((en - 1.0).abs() < 1e-4);
    }

    #[test]
    fn hash_embedder_overlap_beats_disjoint() {
        // Cosine similarity should be higher for lexically-overlapping text.
        let q = HashEmbedder::embed_text("which payment provider did we choose stripe");
        let rel = HashEmbedder::embed_text("decision we go with stripe for payments");
        let irr = HashEmbedder::embed_text("the office wifi password is unknown");
        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        assert!(
            cos(&q, &rel) > cos(&q, &irr),
            "overlap {} should beat disjoint {}",
            cos(&q, &rel),
            cos(&q, &irr)
        );
    }

    #[test]
    fn recall_and_ndcg_basic() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let ranked = vec![a, c, b];
        let rel: HashSet<Uuid> = [a, b].into_iter().collect();
        assert!((recall_at_k(&ranked, &rel, 20) - 1.0).abs() < 1e-9);
        assert!((recall_at_k(&ranked, &rel, 1) - 0.5).abs() < 1e-9);
        // nDCG@10: hits at rank 0 and 2 → dcg = 1/log2(2)+1/log2(4)=1+0.5=1.5;
        // idcg (2 rel) = 1/log2(2)+1/log2(3)=1+0.6309=1.6309 → ~0.9197.
        let n = ndcg_at_k(&ranked, &rel, 10);
        assert!((n - 0.9197).abs() < 1e-3, "ndcg={n}");
    }

    #[test]
    fn abstention_metrics_have_no_relevant() {
        let g = GoldenQuery {
            id: "a01".into(),
            kind: "abstention".into(),
            query: "wifi password?".into(),
            relevant_doc_ids: vec![],
        };
        assert!(g.is_abstention());
    }

    /// Guardrail: if these drift from `routes/queries.rs`, someone changed the
    /// production retrieval knobs without updating the harness. This test cannot
    /// import the private constants, so it just pins the values the harness was
    /// written against — update both together.
    #[test]
    fn mirrored_constants_pinned() {
        assert_eq!(RETRIEVAL_CANDIDATE_K, 50);
        assert_eq!(NODE_CANDIDATE_K, 8);
        assert_eq!(DEFAULT_TOP_K, 20);
        assert_eq!(ASK_RELEVANCE_FLOOR_DEFAULT, 0.02);
    }

    /// Same drift guardrail for the mirrored `supersession_ranking_enabled`
    /// (§3.9): if `routes/queries.rs`'s default or parsing ever changes
    /// without this mirror being updated too, this test — not
    /// `graph_temporal_eval_pg.rs`'s harness — is where that surfaces.
    #[test]
    fn supersession_ranking_enabled_mirror_defaults_on() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("SUPERSESSION_RANKING_ENABLED");
        assert!(supersession_ranking_enabled(), "must default ON (§3.9)");
        std::env::set_var("SUPERSESSION_RANKING_ENABLED", "off");
        assert!(!supersession_ranking_enabled());
        std::env::remove_var("SUPERSESSION_RANKING_ENABLED");
    }

    // ── atom_to_doc_map / grade_query / aggregate_eval_report ───────────────

    #[test]
    fn atom_to_doc_map_builds_reverse_index() {
        let a1 = Uuid::from_u128(1);
        let a2 = Uuid::from_u128(2);
        let a3 = Uuid::from_u128(3);
        let mut doc_atoms = HashMap::new();
        doc_atoms.insert("d1".to_string(), vec![a1, a2]);
        doc_atoms.insert("d2".to_string(), vec![a3]);
        let m = atom_to_doc_map(&doc_atoms);
        assert_eq!(m.get(&a1), Some(&"d1".to_string()));
        assert_eq!(m.get(&a2), Some(&"d1".to_string()));
        assert_eq!(m.get(&a3), Some(&"d2".to_string()));
        assert_eq!(m.len(), 3);
    }

    #[test]
    fn grade_query_abstention_negative_scores_zero_recall_and_ndcg() {
        let g = GoldenQuery {
            id: "a1".into(),
            kind: "abstention".into(),
            query: "wifi password?".into(),
            relevant_doc_ids: vec![],
        };
        let outcome = RetrievalOutcome {
            ranked_ids: vec![Uuid::from_u128(1)],
            ranked: vec![],
            abstained: true,
            top_score: 0.01,
            trace: None,
        };
        let atom_to_doc = HashMap::new();
        let graded = grade_query(&g, &outcome, &atom_to_doc);
        assert_eq!(graded.recall, 0.0);
        assert_eq!(graded.ndcg, 0.0);
        assert!(graded.abstained);
    }

    #[test]
    fn grade_query_answerable_scores_recall_at_document_level() {
        let g = GoldenQuery {
            id: "d1".into(),
            kind: "direct".into(),
            query: "stripe?".into(),
            relevant_doc_ids: vec!["docA".into()],
        };
        let atom = Uuid::from_u128(42);
        let outcome = RetrievalOutcome {
            ranked_ids: vec![atom],
            ranked: vec![],
            abstained: false,
            top_score: 0.9,
            trace: None,
        };
        let mut atom_to_doc = HashMap::new();
        atom_to_doc.insert(atom, "docA".to_string());
        let graded = grade_query(&g, &outcome, &atom_to_doc);
        assert_eq!(graded.recall, 1.0);
        assert!(graded.ndcg > 0.0);
        assert!(!graded.abstained);
    }

    /// The critical behavior [`NON_GATED_CATEGORIES`] exists for: a category
    /// listed there (e.g. "distractor") must not drag the pooled
    /// `overall.recall_at_20`/`ndcg_at_10` down (or up) even though it's fully
    /// reported in `by_category`.
    #[test]
    fn aggregate_eval_report_excludes_non_gated_categories_from_pooled_recall() {
        assert!(NON_GATED_CATEGORIES.contains(&"distractor"));

        let g_direct = GoldenQuery {
            id: "d1".into(),
            kind: "direct".into(),
            query: "q1".into(),
            relevant_doc_ids: vec!["docA".into()],
        };
        let g_distractor = GoldenQuery {
            id: "x1".into(),
            kind: "distractor".into(),
            query: "q2".into(),
            relevant_doc_ids: vec!["docB".into()],
        };
        let graded = vec![
            GradedQuery {
                golden: &g_direct,
                recall: 1.0,
                ndcg: 1.0,
                abstained: false,
                top_score: 0.9,
            },
            GradedQuery {
                golden: &g_distractor,
                recall: 0.0,
                ndcg: 0.0,
                abstained: false,
                top_score: 0.1,
            },
        ];
        let report = aggregate_eval_report(graded, "hash", 20, 2);

        // distractor's 0.0 must not drag the pooled figure down from direct's 1.0.
        assert_eq!(report.overall.recall_at_20, 1.0);
        assert_eq!(report.overall.ndcg_at_10, 1.0);
        // ...yet it's still fully visible per-category.
        assert_eq!(report.by_category["distractor"].recall_at_20, 0.0);
        assert_eq!(report.by_category["direct"].recall_at_20, 1.0);
        assert_eq!(report.golden_queries, 2);
    }
}
