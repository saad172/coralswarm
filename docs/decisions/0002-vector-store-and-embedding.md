# ADR-0002: Vector Store and Embedding Model

**Date:** 2026-05-13
**Status:** Accepted, amended 2026-09-21
**Deciders:** CoralSwarm core team
**Supersedes:** —
**Superseded by:** —

---

## Context

CoralSwarm needs to store and search semantic embeddings for "atoms" — discrete units of
knowledge ingested from sources such as Slack, Notion, Zoom, and local files.
The requirements that constrain the choice are:

- **Privacy-first, on-prem/backend deployment.** The original decision kept
  embeddings in-process. The 2026-09-21 amendment below replaces that rule for
  the server corpus only. Desktop and the browser private engine stay on-device.
- **Scale per account:** 10 k atoms on day one, potentially 1 M atoms per account at
  steady state. A single Fly Postgres instance must handle the vector workload.
- **Fixed 384-dim vectors.** CS-025 (`0003_atoms.sql`) already committed
  `embedding vector(384)` with an HNSW cosine index (`m=16, ef_construction=64`).
  Changing dimensions would require a destructive migration and a new HNSW rebuild.
- **Cosine similarity search** at query time (semantic "find atoms like this").
- **Model must be embeddable.** It ships inside or alongside the Rust backend binary.
  ONNX Runtime lets us run inference in the same process without a Python interpreter.
- **CPU-only inference.** Fly Machines are CPU-only; no CUDA/Metal available in
  production.

---

## Decision

### Vector Store: pgvector on Fly Postgres

We confirm **pgvector** as the vector store. It is already implemented (CS-025) and
requires no additional infrastructure.

### Embedding Model: `BAAI/bge-small-en-v1.5`

We select **`BAAI/bge-small-en-v1.5`** as the embedding model for the MVP.

---

## Rationale

### Vector Store Evaluation

Three options were considered:

| Option | Notes |
|---|---|
| **pgvector** (chosen) | Already provisioned; HNSW index; full Postgres ACID; handles millions of rows |
| **sqlite-vec** | Brute-force only (no ANN index as of 2026); single-writer bottleneck; would require abandoning the Fly Postgres stack entirely |
| **Qdrant / Weaviate** | Dedicated vector DBs with excellent performance at scale, but add an extra managed service, cost, and operational overhead; payload filtering requires keeping a separate relational DB in sync anyway |

**Why pgvector wins:** CoralSwarm already runs Fly Postgres 17.2 with pgvector
available. The atoms table and HNSW index are already in production migrations.
pgvector's HNSW implementation handles the 10 k–1 M atom range comfortably without
external services. Cosine-distance search with `vector_cosine_ops` is exactly what
semantic search requires. An external dedicated vector DB would give us marginally
better ANN throughput at 100 M+ vectors, but that is not a near-term concern, and
migrating later is straightforward (dump embeddings, bulk-insert into Qdrant).

**Why not sqlite-vec:** sqlite-vec has no approximate-nearest-neighbour (ANN) index —
all similarity search is full-scan, which is acceptable only below ~100 k vectors. More
critically, switching to SQLite would require replacing the entire Fly Postgres stack,
discarding `sqlx` migration infrastructure, and losing full-text and relational queries
in one store. The tradeoff is negative at every dimension relevant to CoralSwarm.

### Embedding Model Evaluation

All three candidates output 384-dimensional vectors and can run on CPU via ONNX Runtime.

#### MTEB Benchmark Scores (English retrieval, NDCG@10 unless noted)

| Model | MTEB avg | Retrieval (15 datasets) | Model size | Context |
|---|---|---|---|---|
| `sentence-transformers/all-MiniLM-L6-v2` | 56.3 | ~41–43 | ~22 MB / 23 M params | 256 tokens |
| `BAAI/bge-small-en-v1.5` | **62.17** | **51.68** | ~33 MB / 33 M params | 512 tokens |
| `nomic-ai/nomic-embed-text-v1.5` @ 768 d | 62.28 | ~53 | 274 MB / 137 M params | 8192 tokens |
| `nomic-ai/nomic-embed-text-v1.5` @ 384 d (MRL) | ~61.0 est. | ~51–52 est. | 274 MB / 137 M params | 8192 tokens |

Sources: BAAI/bge-small-en-v1.5 model card (Hugging Face); nomic-ai/nomic-embed-text-v1.5
model card (Hugging Face); MTEB Leaderboard; community benchmarks (supermemory.ai,
morphllm.com).

#### Analysis

**`all-MiniLM-L6-v2`** is disqualified as a meaningful baseline only. Its MTEB average
of 56.3 is 6 points behind bge-small and nomic-embed-v1.5. Its 256-token context window
means long atoms are truncated silently. The Hacker News engineering community consensus
(2025) is to avoid it for new production datasets. It is included here only to establish
what "widely used default" means numerically.

**`nomic-embed-text-v1.5`** achieves similar retrieval quality to bge-small-en-v1.5 when
truncated to 384 dimensions via Matryoshka Representation Learning (estimated MTEB
~61.0–61.5, interpolating between the published 512 d score of 61.96 and 256 d score of
61.04). Its 8 k-token context window is genuinely useful for long documents. However:

- At 274 MB on disk (137 M parameters), it is **~8× larger** than bge-small-en-v1.5
  (~33 MB). For a Rust backend that loads the model at startup and runs inference on
  every ingest event, this materially increases cold-start time and memory pressure on
  Fly Machines (256 MB default RAM for shared-cpu-1x).
- The 384 d operating point is not a first-class dimension for nomic-embed-text-v1.5.
  The model card lists 768, 512, 256, 128, and 64 as recommended sizes; 384 sits between
  published checkpoint sizes and requires a custom truncation + L2-norm step that is not
  exposed in the canonical inference pipeline. Using an unofficial operating point
  introduces implementation risk.
- The retrieval quality advantage over bge-small at 384 d is marginal (within noise on
  MTEB), because nomic-embed-v1.5 at 384 d falls to roughly the same score band as
  bge-small at its native 384 d.

**`BAAI/bge-small-en-v1.5`** is bge-small's native dimensionality. It scores 62.17 MTEB
average and 51.68 on retrieval (15-dataset average), outperforming MiniLM-L6-v2 by
~+10 points on retrieval and nomic-embed at 384 d by ~0–1 point — essentially equal
quality. Critically:

- **33 MB ONNX file.** Loads in under a second on any Fly Machine; fits in L3 cache on
  multi-core CPUs; near-zero impact on memory budget.
- **Native 384 d output.** No truncation step; no custom normalisation. The full MTEB
  retrieval score is achieved at the model's designed operating point.
- **512-token context.** Sufficient for the atom granularity CoralSwarm targets (a Slack
  message, a Notion paragraph, a meeting excerpt — rarely exceed 300 tokens).
- **ONNX-first ecosystem.** BAAI publishes a first-party ONNX export; the model is used
  in production by LlamaIndex, LangChain, and Qdrant's own examples.

#### Recall-per-MB tradeoff (the deciding criterion)

For a privacy-focused, on-prem backend where the model binary ships with the service:

| Model | MTEB retrieval | Size | Retrieval / MB |
|---|---|---|---|
| all-MiniLM-L6-v2 | ~41–43 | 22 MB | ~1.9 |
| **bge-small-en-v1.5** | **51.68** | **33 MB** | **~1.57** |
| nomic-embed-v1.5 @ 384 d | ~51–52 | 274 MB | ~0.19 |

bge-small-en-v1.5 delivers **~8× better retrieval quality per MB** than
nomic-embed-text-v1.5 at 384 d, with essentially identical absolute retrieval quality.
That is the decisive factor.

---

## Consequences

### Positive

- **Zero new infrastructure.** pgvector is already running. No Qdrant, Weaviate, or
  Redis instance to provision, pay for, or operate.
- **Small binary footprint.** 33 MB ONNX file added to the backend container. Total
  image size increase is modest.
- **Fast cold start.** Model loads in < 1 s on a 1 vCPU Fly Machine; no GPU warmup.
- **Consistent cosine space.** All atoms are embedded by the same model at the same
  dimensionality, indexed by the existing HNSW structure.
- **Upgrade path is clean.** If a better 384 d model emerges, re-embedding all atoms is
  a batch job. HNSW index is rebuilt automatically on next startup via pgvector's index
  maintenance.

### Negative / Risks

- **English-only.** bge-small-en-v1.5 is trained on English text. Non-English content
  will receive lower-quality embeddings. Mitigation: if multi-language support becomes
  a requirement, evaluate `bge-m3` (1024 d, multilingual) and add a migration to
  widen the embedding column.
- **512-token limit.** Atoms longer than 512 tokens (roughly 380 words) will be
  truncated. Mitigation: the ingest pipeline should chunk long content before embedding;
  each chunk becomes a separate atom.
- **HNSW index parameters are fixed.** `m=16, ef_construction=64` are solid defaults
  but not tuned for >500 k vectors. At large scale we may want `m=32` and higher
  `ef_construction`. Mitigation: pgvector allows `REINDEX` without downtime; this is
  an operational concern, not a schema change.
- **No streaming inference.** ONNX Runtime in Rust (via `ort` crate) loads the full
  model into memory. For batch ingest jobs this is fine; extremely high single-request
  concurrency may require a worker pool pattern.

### Deferred Decisions

- **Quantisation.** INT8-quantised bge-small-en-v1.5 reduces the ONNX file to ~9 MB
  with <0.5% MTEB degradation. Worth evaluating once the ingest pipeline is built.
- **Chunking strategy.** How to split atoms longer than 512 tokens is a separate ADR.
- **Re-embedding on model upgrade.** Background job design is out of scope here.

---

## Alternatives Considered (Summary)

| Option | Rejected because |
|---|---|
| sqlite-vec | No ANN index; full-scan only; requires abandoning Fly Postgres stack |
| Qdrant | Extra managed service; adds cost and ops overhead; not justified at current scale |
| Weaviate | Same as Qdrant |
| nomic-embed-text-v1.5 | 274 MB vs 33 MB; ~8× worse recall/MB at 384 d |
| all-MiniLM-L6-v2 | 6 pt lower MTEB avg; 256-token context; outdated training pipeline |
| bge-base-en-v1.5 | 109 MB, 63.55 MTEB avg — good, but 3× larger for +1.4 MTEB; not worth it at MVP |

---

## References

- BAAI/bge-small-en-v1.5 model card: https://huggingface.co/BAAI/bge-small-en-v1.5
- nomic-ai/nomic-embed-text-v1.5 model card: https://huggingface.co/nomic-ai/nomic-embed-text-v1.5
- MTEB Leaderboard: https://huggingface.co/spaces/mteb/leaderboard
- pgvector HNSW documentation: https://github.com/pgvector/pgvector
- sqlite-vec vs pgvector discussion: https://github.com/asg017/sqlite-vec/issues/94
- CS-025 migration: `backend/migrations/0003_atoms.sql`

---

## Amendment (2026-09-21): server embeddings move to OpenRouter

The "no cloud embedding API" rule applied when no coral text left the
backend. Server-custodied oceans already send a retrieval shortlist to
OpenRouter at synthesis. For those oceans, the server embedding call uses
the same vendor, with stricter retention terms than chat.

**Model.** `openai/text-embedding-3-small` via `POST /api/v1/embeddings`,
`dimensions: 384`, so the existing `vector(384)` column and HNSW index stay.
The stored stamp is `openai/text-embedding-3-small@384`. If a probe returns
any other width, do not flip search; change the column width or the model
and re-embed the whole corpus.

**Retention.** Every request sets `provider.zdr: true` and
`provider.data_collection: "deny"`, and `allow_fallbacks: false`. A failed
call is not retried against another provider and is not filled with the
ONNX vector. The cloud slot stays empty until a later attempt succeeds.

**Hold, then flip.** Cloud vectors are written to `atoms.embedding_next`
beside the live `bge-small-en-v1.5` vector. Asks keep embedding and
searching with bge until every server row that should have a cloud vector
has one, including rows created during the backfill and laptop-synced bge
vectors. Query embedding and the search stamp flip together. Until that
count is zero, search does not read the side column.

**What stays local.** The desktop embedder stays `bge-small-en-v1.5`. The
browser private engine stays MiniLM. An account set to Local only skips the
cloud call, gets no server vector from it, and remains keyword-searchable.
There are no Local only accounts today; ONNX is not kept as their fallback
after the flip.

**Text.** Email, notes, and documents gain a provenance line (title, date,
project; empty clauses omitted) in the same pass. Meetings, chat, and
coding sessions keep the headers they already have. Transcript chunks stay
cut on speaker turns with no overlap. Chunk size stays about 400 tokens.
