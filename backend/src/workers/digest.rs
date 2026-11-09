//! Reef digest regeneration (graph-temporal-memory plan, Workstream A, §2).
//!
//! `nodes.body.digest.{summary, excerpts}` is the warm summary + per-coral
//! excerpts rendered into the `ENTITIES` synthesis block (and, once a
//! digest exists, into `search_tsv`'s weight-D term — a warm digest doubles
//! the surface a reef can be found by, not just what a human sees on open).
//!
//! ## Two writers, one freshness signal
//!
//! Two paths can (re)generate a reef's digest:
//!   * **Background** — `RegenerateDigest` jobs, enqueued by the
//!     `nodes_maybe_enqueue_digest_regeneration` trigger (migration
//!     0109/0019) the instant `coral_count`/`title` change in a way that
//!     crosses [`should_regenerate_digest`]'s threshold. This module's
//!     `tick`/`process_job` drain that queue using the §2.4 relevance-ranked
//!     coral selection.
//!   * **On-demand** — `routes::nodes::node_digest`, hit on human open,
//!     using the older heap-order-truncate-30 selection (unchanged; the
//!     §2.4 rewrite only applies to the background path per this
//!     workstream's scope).
//!
//! Both paths, on a successful generation, call
//! `Store::set_node_digest_coral_count` to stamp `digest_coral_count =
//! coral_count` at that moment — so [`should_regenerate_digest`] (evaluated
//! both in SQL by the trigger above and here in Rust, kept in sync by hand
//! since Postgres cannot call into a Rust binary) stays the ONE freshness
//! definition either writer's output is judged by. Without this, a
//! background-warmed digest would be indistinguishable from stale the
//! moment the on-demand handler's own (unrelated) cache logic ran — the
//! exact defeat this workstream exists to avoid (§2.1's diagnosis: a digest
//! that is warmed but then immediately treated as cold on the next coral is
//! no fix at all).
//!
//! ## Coral content is not always inline (the 2026-08-04 R2 cutover)
//!
//! R2 blob storage went live in production at **2026-08-04 04:41 UTC**;
//! every atom written since carries `content_blob_key` with `content_inline`
//! NULL. This worker originally loaded coral text with a flat
//! `AND content_inline IS NOT NULL` and dropped the misses via `filter_map`,
//! which produced TWO distinct defects — the same D5 shape PR #569 fixed in
//! the relationship workers, never ported here:
//!
//!   * A reef whose corals are ALL post-cutover selected nothing, so the job
//!     died on "no inline coral content" and retried to `dead`. Loud, but
//!     only because it hit zero — 108 dead jobs across 8 reefs.
//!   * A reef with a MIX quietly summarised only its pre-cutover corals and
//!     still reported `done`. Invisible, and far wider: 77 reefs silently
//!     dropping 2,549 corals from their own digests.
//!
//! [`load_selected_coral_content`] now resolves R2-only corals from R2 —
//! the same DEK-resolve-then-`get_atom_blob` path
//! `extract::fetch_r2_only_content` runs for a single atom, batched (see
//! that function's doc) — and fails the job LOUDLY if any selected coral's
//! content cannot be loaded. A digest assembled from a subset is
//! exactly the silent degradation this worker exists to avoid — it becomes
//! the reef's warm summary AND its `search_tsv` weight-D term, so corals
//! missing from it are missing from what the reef can be found by, with
//! nothing anywhere reporting the loss. Retries absorb a transient R2/DEK
//! blip; a persistent failure dead-letters where an operator can see it.
//!
//! ## Constants (§2.7 / §10 tuning table)
//!
//! All estimates pending real production output, per the plan's own
//! admission (§11: "Every value in §10's tuning table. None is derived").

use std::collections::HashMap;

use coralswarm_core::embed::EmbedProvider;
use coralswarm_core::r2::R2Client;
use futures::future;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::jobs::queue::{self, JobRow};
use crate::llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, Message, Role};
use crate::relationships::{EdgeOrigin, EdgeStatus, RelationshipEdge};
use crate::store::{NodeDigestContext, Store, StoreError};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::embed::JobScope;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::workers::verify_job_scope;

// ── §2.3 — eligibility and cadence ────────────────────────────────────────────

/// Minimum coral count for a reef to ever be considered for a digest.
///
/// 3, not the plan's original 8 (COR-66): the quality bound is a digest on
/// at least 80% of entity reefs with 3 or more corals, because reef pages
/// and Jev packs read the digest. At 8, 49% of the org ocean's 3+-coral reefs
/// could never get one. Mirrored in the regeneration trigger (tenant 0181 /
/// control 0350).
pub const DIGEST_MIN_CORAL_COUNT: i64 = 3;
/// Absolute floor on the growth-cadence trigger (§2.3's `max(5, ...)`).
pub const DIGEST_GROWTH_FLOOR: i64 = 5;
/// Relative growth-cadence trigger: regenerate once corals added since the
/// last digest exceed this fraction of what the last digest covered.
pub const DIGEST_GROWTH_FRACTION: f64 = 0.20;

/// `eligible = type = 'entity' AND coral_count >= 3` (§2.3, lowered from 8).
///
/// Decision (and thread/reference) reefs are never eligible: the title IS
/// the statement and `body.rationale` IS the reasoning, so a digest would
/// only restate what is already there verbatim.
pub fn digest_eligible(node_type: &str, coral_count: i64) -> bool {
    node_type == "entity" && coral_count >= DIGEST_MIN_CORAL_COUNT
}

/// `regenerate = digest_coral_count = 0 OR (coral_count - digest_coral_count) >=
/// max(5, ceil(digest_coral_count * 0.20)) OR renamed` (§2.3).
///
/// `digest_coral_count = 0` means the reef has never had a digest (or its
/// digest was stripped by a deletion): always due. With the eligibility bar
/// at 3, growth alone (+5) would never fire a first digest for a reef of 3
/// to 4 corals.
///
/// Pure arithmetic twin of the SQL the `nodes_maybe_enqueue_digest_regeneration`
/// trigger evaluates (migration 0109/0019) — kept identical by hand and
/// exercised here with boundary-focused unit tests, since the two can never
/// be allowed to disagree about where the line falls. Does NOT check
/// [`digest_eligible`] itself; callers apply that gate separately (mirroring
/// the trigger's own early-return before this arithmetic runs).
pub fn should_regenerate_digest(coral_count: i64, digest_coral_count: i64, renamed: bool) -> bool {
    if renamed || digest_coral_count <= 0 {
        return true;
    }
    let growth = coral_count - digest_coral_count;
    let relative = (digest_coral_count as f64 * DIGEST_GROWTH_FRACTION).ceil() as i64;
    let threshold = DIGEST_GROWTH_FLOOR.max(relative);
    growth >= threshold
}

// ── §2.4 — coral selection ────────────────────────────────────────────────────

/// Keep-while-selecting per source (`base_source_id`-grouped: a meeting's
/// chunk atoms all share one source).
pub const DIGEST_PER_SOURCE_CAP: usize = 6;
/// Reject a candidate whose highest cosine similarity to anything already
/// selected meets or exceeds this — the chunk-run guard (§2.4's "novelty
/// stop"): a dozen near-duplicate slices of the same meeting stop crowding
/// out everything else once the reef has enough of that meeting represented.
pub const DIGEST_NOVELTY_MAX_SIM: f32 = 0.92;
/// `score >= DIGEST_RELATIVE_FLOOR * best_score` — self-normalises per reef,
/// so no absolute similarity threshold needs tuning across reefs of very
/// different topical tightness.
pub const DIGEST_RELATIVE_FLOOR: f32 = 0.6;
/// Hard cap on corals selected for one digest generation.
pub const DIGEST_SELECT_CAP: usize = 30;

/// Wall-clock deadline for one digest's whole R2 fetch batch (at most
/// [`DIGEST_SELECT_CAP`] concurrent GETs). `R2Client` sets no operation
/// timeout of its own, so without this a stalled GET holds a claimed job in
/// `running` past the worker's shutdown grace window. Generous relative to a
/// healthy batch (tens of milliseconds each, run concurrently) — this is a
/// hang guard, not a latency budget.
pub const R2_BATCH_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// One candidate coral for digest selection: its cosine similarity to the
/// reef TITLE's embedding (`score`, ranked descending by the caller — see
/// [`select_digest_corals`]'s precondition), its own embedding (for the
/// novelty check against already-selected candidates), and a `source_key`
/// (`workers::extract::base_source_id` — the chunk-suffix-stripped
/// `source_id`) grouping chunks of the same underlying document/meeting for
/// the per-source cap.
#[derive(Debug, Clone)]
pub struct DigestCandidate {
    pub atom_id: Uuid,
    pub source_key: String,
    pub score: f32,
    pub embedding: Vec<f32>,
}

/// Cosine similarity between two embedding vectors. `atoms.embedding` is
/// produced by `packages/core/src/embed_ort.rs`'s bge-small model, which is
/// ALREADY L2-normalised (that module's own doc comment), so a plain dot
/// product is the cosine similarity — no separate norm division needed. A
/// dimension mismatch (should never happen: one embed model, one column)
/// degrades to `0.0` rather than panicking.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// §2.4's coral-selection rule:
///
/// ```text
/// keep while
///     score >= 0.6 * best_score
/// and per_source_count < 6
/// and max_sim(candidate, selected) < 0.92
/// until 30
/// ```
///
/// **Precondition:** `candidates` is already ranked by `score` DESCENDING
/// (the caller's pgvector query does this — `ORDER BY embedding <=> $query`).
/// This lets the relative-floor check be a hard `break` rather than a
/// per-candidate `continue`: once one candidate falls below the floor, every
/// later one (lower-or-equal score) does too. The per-source cap and novelty
/// check are NOT monotonic in score, so they `continue` (skip) instead —
/// a later, lower-ranked candidate from an under-represented source, or one
/// that IS novel, can still be selected after a same-source or near-duplicate
/// candidate is skipped.
///
/// Returns the selected candidates' `atom_id`s in selection order (which is
/// also score order, since selection only ever skips forward).
pub fn select_digest_corals(candidates: &[DigestCandidate]) -> Vec<Uuid> {
    let Some(best) = candidates.first() else {
        return Vec::new();
    };
    let floor = DIGEST_RELATIVE_FLOOR * best.score;

    let mut selected: Vec<&DigestCandidate> = Vec::with_capacity(DIGEST_SELECT_CAP);
    let mut per_source_count: HashMap<&str, usize> = HashMap::new();

    for c in candidates {
        if selected.len() >= DIGEST_SELECT_CAP {
            break;
        }
        // Ranked descending: once below the floor, nothing later can clear it.
        if c.score < floor {
            break;
        }
        let count = per_source_count
            .get(c.source_key.as_str())
            .copied()
            .unwrap_or(0);
        if count >= DIGEST_PER_SOURCE_CAP {
            continue;
        }
        let max_sim = selected
            .iter()
            .map(|s| cosine_similarity(&s.embedding, &c.embedding))
            .fold(f32::NEG_INFINITY, f32::max);
        if max_sim >= DIGEST_NOVELTY_MAX_SIM {
            continue;
        }
        *per_source_count.entry(c.source_key.as_str()).or_insert(0) += 1;
        selected.push(c);
    }

    selected.into_iter().map(|c| c.atom_id).collect()
}

// ── §2.6 — digest context: established edges ──────────────────────────────────

/// `origin = stated OR (origin = inferred AND status = confirmed)` (§2.6) —
/// the same trust tier §5.3 of the plan defines for traversal, applied here
/// to what the digest job is allowed to treat as a currently-holding fact.
///
/// Deliberately does NOT additionally gate on confidence (§5.3's
/// `traversable` flag adds `confidence >= 0.70`) — that flag belongs to
/// Workstream D (retrieval traversal) and does not exist yet; §2.6 specifies
/// only the origin/status trust tier for digest context.
pub fn is_established_edge(edge: &RelationshipEdge) -> bool {
    matches!(edge.origin, EdgeOrigin::Stated)
        || (matches!(edge.origin, EdgeOrigin::Inferred)
            && matches!(edge.status, EdgeStatus::Confirmed))
}

// ── §2.5/§2.6 — job mechanics ───────────────────────────────────────────────────

/// Digest model default; overridable via `DIGEST_MODEL` (same env var the
/// on-demand handler, `routes::nodes::node_digest`, reads).
const DIGEST_MODEL_DEFAULT: &str = "deepseek/deepseek-v4-flash-0731";
/// System prompt — same compiled-in file as the on-demand handler (a
/// separate `include_str!`, not a shared `pub` constant, so this worker
/// doesn't reach into `routes::nodes`'s module-private items for a single
/// string).
const DIGEST_SYSTEM_PROMPT: &str = include_str!("../../prompts/digest_v1.txt");
/// `llm_usage.task` for the title embedding that ranks a digest's corals.
const DIGEST_TITLE_EMBED_TASK: &str = "digest_title_embed";
/// Candidate pool size fetched from pgvector BEFORE `select_digest_corals`
/// greedily filters it down to `DIGEST_SELECT_CAP` — generous headroom for
/// the relative floor / per-source cap / novelty stop to have real
/// candidates to work with on a large reef, bounded so this stays one
/// indexed query rather than a full per-reef atom scan.
const DIGEST_CANDIDATE_POOL: i64 = 200;

const JOB_BATCH_SIZE: usize = 4;
/// Idle poll interval when the queue is empty.
pub(crate) const EMPTY_POLL_SLEEP: std::time::Duration = std::time::Duration::from_millis(500);

#[derive(Deserialize)]
struct DigestJson {
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    excerpts: Vec<DigestExcerptJson>,
}

#[derive(Deserialize)]
struct DigestExcerptJson {
    index: usize,
    excerpt: String,
    #[serde(default)]
    contribution: Option<String>,
}

/// The pieces of the stored `body.digest` this worker decides freshness by
/// (see `routes::nodes::CachedDigest` for the full shape).
#[derive(Deserialize, Default)]
struct StoredDigest {
    #[serde(default)]
    digested_title: Option<String>,
    /// Absent on digests from before managed enrichment (#1352). The reader
    /// (`crate::reef_digest_read`) never serves those, so they are due.
    #[serde(default)]
    managed_receipt: Option<String>,
    /// [`selection_key`] of the inputs the stored digest was written from.
    #[serde(default)]
    selection_key: Option<String>,
}

/// One key over everything a digest pass would send the writer before Jev's
/// picker filters it: the model, the prompt, the reef title, each selected
/// coral in order (id, content revision and a hash of its text) and each
/// established edge line. Equal keys mean a regeneration would reread
/// exactly what the stored digest was written from, so the pass spends
/// nothing (the debounce on unchanged inputs).
pub(crate) fn selection_key(
    model: &str,
    title: &str,
    items: &[(Uuid, String)],
    revisions: &HashMap<Uuid, i64>,
    edges: &[EdgeFact],
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"digest-selection-v1\x1f");
    h.update(model.as_bytes());
    h.update(b"\x1f");
    h.update(Sha256::digest(DIGEST_SYSTEM_PROMPT.as_bytes()));
    h.update(b"\x1f");
    h.update(title.as_bytes());
    for (id, text) in items {
        h.update(b"\x1ea");
        h.update(id.as_bytes());
        h.update(revisions.get(id).copied().unwrap_or(0).max(0).to_be_bytes());
        h.update(Sha256::digest(text.as_bytes()));
    }
    for edge in edges {
        h.update(b"\x1ee");
        h.update(edge.edge_id.as_bytes());
        h.update(edge.line.as_bytes());
    }
    hex::encode(h.finalize())
}

/// Strip ```json fences and isolate the outer JSON object (LLMs often wrap
/// it) — same helper as `routes::nodes::extract_json_object`, duplicated
/// rather than imported (private to that module; trivial, three-line logic).
fn extract_json_object(s: &str) -> &str {
    let s = s.trim();
    let s = s
        .strip_prefix("```json")
        .or_else(|| s.strip_prefix("```"))
        .unwrap_or(s);
    let s = s.strip_suffix("```").unwrap_or(s).trim();
    match (s.find('{'), s.rfind('}')) {
        (Some(a), Some(b)) if b > a => &s[a..=b],
        _ => s,
    }
}

// ── Managed enrichment (source-agnostic plan PR 8) ───────────────────────────
//
// Every model call this worker makes (Jev's keep-picker when `JEV_JUDGE_MODE`
// is active, the digest writer, and its one truncation/parse retry) is its
// own `managed_enrichment` logical call, charged to the graph's executor
// under the shared daily cap and a per-graph daily call bound
// ([`DIGEST_DAILY_CALLS_ENV`]). Each call binds the corals it reads
// `(atom, id, content_revision, sha256(text))`, each established edge line
// the writer's prompt states `(digest_edge, edge_id, 0, sha256(line))`, plus
// one `digest_node` marker `(node, title)`. The digest is published in one
// transaction that re-reads the reef, the corals and the edges under the
// reef's row lock and writes only when they are unchanged;
// `body.digest.managed_receipt` names the receipt, which is what deletion
// invalidation (`managed_enrichment_invalidate_atoms`, tenant 0171 / control
// 0338) matches to strip a digest built from a deleted coral, and what the
// reader-side check (`crate::reef_digest_read`) re-verifies on every read.
// A hold goes back on the queue in place under the shared retry rule
// (`jobs::queue::hold`): a hold that can clear backs off to a cap, one that
// cannot (the requester lost access) dead-letters after a few tries, and
// the next coral change re-enqueues the reef. The daily cap and bound wait
// for the next UTC day.

/// `DIGESTS_ENABLED`: the digests' own kill switch. OFF unless exactly
/// `true`. Since #1225 (2026-10-12) the generic node reads
/// (`get_node_digest_scoped` / `node_digest_context_scoped`) fence a stored
/// digest as unattributed derived content and still do; the one read that
/// serves it is `crate::reef_digest_read` (`GET /v1/nodes/{id}/digest` and
/// MCP `get_reef`), which re-verifies the digest's receipt for the reader.
pub(crate) const DIGESTS_SWITCH_ENV: &str = "DIGESTS_ENABLED";
/// `DIGEST_DAILY_CALLS_PER_GRAPH`: digest calls (picker, writer, retry) one
/// graph may open per UTC day. A runaway guard only: the dollar bound below
/// is the throttle. It was 90 (about 45 digests per graph per day), which in
/// production (2026-10-20) held ~5,600 reefs a day at about $0.07 of digest
/// spend per graph while 22,000 reefs had no digest: months to cover them.
/// 1,500 calls is at least 500 digests, about $1 at the measured ~$0.0007
/// per call.
pub(crate) const DIGEST_DAILY_CALLS_ENV: &str = "DIGEST_DAILY_CALLS_PER_GRAPH";
pub(crate) const DEFAULT_DIGEST_DAILY_CALLS_PER_GRAPH: i64 = 1500;
/// `DIGEST_DAILY_SHARE_OF_ENRICHMENT`: the digests' share of the account's
/// shared `managed_enrichment` cap (`MANAGED_DAILY_CAP_USD_ENRICHMENT`), as a
/// fraction of it, across all of its graphs, counted from the receipts
/// (settled, else reserved) that bind a [`INPUT_NODE`] marker: the writer,
/// its retry and Jev's keep-picker. Was a fixed $1.40, which at the $2
/// default left $0.60 for everything else and did not move when the cap was
/// raised. 0.5 is about what digests spent at $2 (~$1.00 a day, 2026-10-27);
/// with entity resolution's 0.3 (`workers::entity_resolution`) it leaves a
/// fifth of the cap that only the journal, coral titles and the
/// supersession judge can spend (together $0.25 on 2026-10-20). The shared
/// cap still holds whatever this is set to.
pub(crate) const DIGEST_DAILY_SHARE_ENV: &str = "DIGEST_DAILY_SHARE_OF_ENRICHMENT";
pub(crate) const DEFAULT_DIGEST_DAILY_SHARE: f64 = 0.5;
/// `DIGEST_DAILY_USD_PER_ACCOUNT`: the earlier fixed-dollar knob. When set it
/// still wins over [`DIGEST_DAILY_SHARE_ENV`], so an operator's explicit
/// figure is never silently replaced; unset (production, 2026-10-27), the
/// share decides.
pub(crate) const DIGEST_DAILY_USD_ENV: &str = "DIGEST_DAILY_USD_PER_ACCOUNT";

/// Micro-USD the account's digests may spend per UTC day.
pub(crate) fn digest_daily_usd_bound() -> i64 {
    let explicit = std::env::var(DIGEST_DAILY_USD_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .is_some_and(|n| n.is_finite() && n >= 0.0);
    if explicit {
        crate::managed::enrichment::daily_usd_bound(DIGEST_DAILY_USD_ENV, 0.0)
    } else {
        crate::managed::enrichment::daily_share_micro_usd(DIGEST_DAILY_SHARE_ENV, DEFAULT_DIGEST_DAILY_SHARE)
    }
}
/// A pass starts only with at least this much of the dollar bound left:
/// three calls at the largest digest reservation seen in production
/// ($0.0062 for a doubled-`max_tokens` retry), so a pass never stops
/// half-way on the bound.
const DIGEST_PASS_RESERVE_MICRO_USD: i64 = 20_000;
/// The most calls one digest can make (picker + writer + retry). A pass only
/// starts with at least this many left, so it never stops half-way.
const MAX_CALLS_PER_DIGEST: i64 = 3;
pub(crate) const INPUT_ATOM: &str = "atom";
/// The marker every digest call binds: the reef and the title it was asked
/// about. Also the deletion trigger's join key.
pub(crate) const INPUT_NODE: &str = "digest_node";
/// One established relationship the writer's prompt states, bound over its
/// exact prompt line (`- {src} {predicate} {dst}`).
pub(crate) const INPUT_EDGE: &str = "digest_edge";

/// Whether the digest consumer may claim and spend (shared switch, global
/// kill switch, and [`DIGESTS_SWITCH_ENV`] exactly `true`).
pub(crate) fn digests_claim_allowed() -> bool {
    crate::managed::enrichment::claim_allowed()
        && std::env::var(DIGESTS_SWITCH_ENV).ok().as_deref() == Some("true")
}

/// Why a digest pass stopped before publishing (nothing written).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// The graph's daily call bound or the shared daily cap.
    NextDay,
    /// A managed hold or managed storage unavailable.
    Held(&'static str),
}

enum Failure {
    Stop(Stop),
    /// Fail the job so the queue retries with backoff.
    Retry(String),
}

impl From<String> for Failure {
    fn from(e: String) -> Self {
        Failure::Retry(e)
    }
}

/// The job's managed context.
struct Managed<'a> {
    control: &'a PgPool,
    tenant: &'a TenantId,
    account_id: Uuid,
    graph_id: Uuid,
    job: crate::managed::enrichment::EnrichmentJob,
    principal: Option<Uuid>,
    calls_left: i64,
}

impl Managed<'_> {
    fn work(&self, inputs: Vec<crate::managed::enrichment::EnrichmentInput>) -> Result<crate::managed::enrichment::EnrichmentWork, Failure> {
        crate::managed::enrichment::EnrichmentWork::new(self.account_id, self.graph_id, Some(self.job.clone()), None, inputs)
            .map_err(|e| Failure::Retry(e.to_string()))
    }

    async fn principal(&mut self, scope: &JobScope<'_>, work: &crate::managed::enrichment::EnrichmentWork) -> Result<Uuid, Failure> {
        if let Some(principal) = self.principal {
            return Ok(principal);
        }
        match crate::managed::enrichment::select_principal(scope, self.control, self.tenant, work).await {
            Ok(principal) => {
                self.principal = Some(principal);
                Ok(principal)
            }
            Err(coralswarm_core::send_gate::Deny::Hold(code)) => Err(Failure::Stop(Stop::Held(code.code()))),
            // Storage, not a missing principal: a hold that can clear.
            Err(_) => Err(Failure::Stop(Stop::Held(crate::jobs::queue::MANAGED_UNAVAILABLE))),
        }
    }

    fn take_call(&mut self) -> Result<(), Failure> {
        if self.calls_left <= 0 {
            return Err(Failure::Stop(Stop::NextDay));
        }
        self.calls_left -= 1;
        Ok(())
    }
}

fn managed_failure(e: LlmError) -> Failure {
    use coralswarm_core::send_gate::{Deny, HoldCode};
    match e {
        LlmError::Managed(Deny::Hold(HoldCode::DailyBudgetExhausted)) => Failure::Stop(Stop::NextDay),
        LlmError::Managed(Deny::Hold(code)) => Failure::Stop(Stop::Held(code.code())),
        LlmError::Managed(Deny::Refuse(code)) => Failure::Stop(Stop::Held(code.code())),
        LlmError::Managed(Deny::Unavailable) => Failure::Stop(Stop::Held(crate::jobs::queue::MANAGED_UNAVAILABLE)),
        e => Failure::Retry(format!("LLM call failed: {e}")),
    }
}

/// One digest-regeneration pass: dequeue up to [`JOB_BATCH_SIZE`]
/// `RegenerateDigest` jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant`, same shape as every other
/// W.2-converted queue worker. Claims nothing unless
/// [`digests_claim_allowed`]; `control` is the control-plane pool the managed
/// gate reserves on.
pub async fn tick(
    t: &WorkerTenant,
    control: &PgPool,
    embedder: &dyn EmbedProvider,
    llm: &dyn LlmProvider,
    r2: Option<&R2Client>,
) -> Result<Tick, String> {
    if !digests_claim_allowed() {
        return Ok(Tick::idle());
    }
    let jobs = claim(t).await?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(&t.tenant, &t.pool, control, t.store.as_ref(), embedder, llm, r2, job).await;
    }
    Ok(Tick::worked(n))
}

// ── Claim order: the priority lane ───────────────────────────────────────────
//
// The queue's own order is `(run_after, id)`. Every held reef comes back at
// the same minute after UTC midnight, so that order is effectively random
// across the backlog (2026-10-20: ~5,600 pending reefs, all due at 00:01).
// The lane claims instead, among due jobs: reefs a reader asked for first
// (`payload.requested`, set by [`queue_regeneration`] on a reader miss), then
// reefs with no digest yet, then regenerations; the reefs with the most
// corals first within each tier. Jobs the lane cannot see (a reef that is
// gone, or a consumer-plane job with no account stamp yet) are claimed
// oldest-first behind it, so nothing starves.
//
// The first-digest tier: on 2026-10-27 the org ocean re-digested the same
// ~240 hub reefs every day (each grows past the +20% bar daily; "CoralSwarm"
// 33 times in 7 days) inside the daily dollar bound, while 64,113 of 66,101
// eligible reefs had never had one. "No digest" is what the reader can
// serve: `digest_coral_count = 0` (never digested, or stripped by a
// deletion) or a digest with no managed receipt (written before managed
// enrichment, never served). The column is tested first, so a reef's body
// is only read when it has a counter.

const KINDS: &[&str] = &["RegenerateDigest"];
/// Consumer accounts read per tick for lane candidates.
const LANE_ACCOUNTS: i64 = 16;
/// Payload flag on a pass a reader asked for.
pub(crate) const REQUESTED: &str = "requested";

async fn claim(t: &WorkerTenant) -> Result<Vec<JobRow>, String> {
    // The lane only orders claims; if it cannot be read, claim oldest-first.
    let lane = priority_lane(t, JOB_BATCH_SIZE).await.unwrap_or_else(|error| {
        tracing::warn!(error = %error, "digest: priority lane unavailable; claiming oldest-first");
        Vec::new()
    });
    let mut jobs = queue::dequeue_ids(&t.pool, KINDS, &lane)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;
    if jobs.len() < JOB_BATCH_SIZE {
        jobs.extend(
            queue::dequeue(&t.pool, KINDS, JOB_BATCH_SIZE - jobs.len())
                .await
                .map_err(|e| format!("dequeue failed: {e}"))?,
        );
    }
    Ok(jobs)
}

/// Up to `limit` due `RegenerateDigest` job ids in lane order. Reading
/// `nodes` needs a scope: the org plane reads once as the tenant system
/// actor, the consumer plane once per account with due jobs.
pub(crate) async fn priority_lane(t: &WorkerTenant, limit: usize) -> Result<Vec<Uuid>, StoreError> {
    let mut lane: Vec<LaneRow> = Vec::new();
    match &t.tenant {
        TenantId::Org(_) => {
            let mut tx = begin_tenant_system_scoped_without_account(&t.pool).await?;
            lane = lane_rows(&mut tx, None, limit).await?;
            tx.rollback().await?;
        }
        TenantId::Consumer => {
            let accounts: Vec<Uuid> = sqlx::query_scalar(
                "SELECT account_id FROM public.jobs WHERE kind=ANY($1) AND status='pending' AND run_after<=now() \
                   AND account_id IS NOT NULL GROUP BY account_id ORDER BY min(run_after) LIMIT $2",
            )
            .bind(KINDS)
            .bind(LANE_ACCOUNTS)
            .fetch_all(&t.pool)
            .await?;
            for account in accounts {
                let mut tx = begin_account_scoped(&t.pool, account).await?;
                lane.extend(lane_rows(&mut tx, Some(account), limit).await?);
                tx.rollback().await?;
            }
            lane.sort_by_key(|(id, requested, first, corals)| {
                (std::cmp::Reverse(*requested), std::cmp::Reverse(*first), std::cmp::Reverse(*corals), *id)
            });
            lane.truncate(limit);
        }
    }
    Ok(lane.into_iter().map(|(id, ..)| id).collect())
}

/// `(job id, a reader asked for it, the reef has no digest yet, corals)`.
type LaneRow = (Uuid, bool, bool, i64);

async fn lane_rows(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account: Option<Uuid>,
    limit: usize,
) -> Result<Vec<LaneRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT j.id, COALESCE(j.payload->>'requested','')='true' AS requested, \
                (n.digest_coral_count <= 0 OR (n.body->'digest'->>'managed_receipt') IS NULL) AS first_digest, \
                n.coral_count::bigint \
           FROM public.jobs j JOIN public.nodes n ON n.node_id = CASE WHEN j.payload->>'node_id' ~* \
                '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (j.payload->>'node_id')::uuid END \
          WHERE j.kind=ANY($1) AND j.status='pending' AND j.run_after<=now() AND ($2::uuid IS NULL OR j.account_id=$2) \
          ORDER BY 2 DESC, 3 DESC, 4 DESC, j.run_after, j.id LIMIT $3",
    )
    .bind(KINDS)
    .bind(account)
    .bind(limit as i64)
    .fetch_all(&mut **tx)
    .await
}

/// Mark the pending pass for `node_id` as asked for by a reader, so the lane
/// claims it ahead of the coral-count order.
async fn mark_requested(pool: &PgPool, node_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE jobs SET payload = payload || jsonb_build_object($2::text, true) \
          WHERE idempotency_key=$1 AND status='pending' AND kind='RegenerateDigest' \
            AND COALESCE(payload->>$2,'') <> 'true'",
    )
    .bind(format!("digest:{node_id}"))
    .bind(REQUESTED)
    .execute(pool)
    .await
    .map(|_| ())
}

/// Queue one background regeneration for `node_id` (deduped on the same
/// `digest:{node}` key as the coral-count trigger) and mark it as asked for
/// by a reader. Returns whether a job is now pending. Called after the read
/// transaction has closed.
pub(crate) async fn queue_regeneration(
    pool: Option<&PgPool>,
    node_id: Uuid,
    graph_id: Uuid,
    account_id: Uuid,
) -> bool {
    let Some(pool) = pool else { return false };
    match crate::jobs::queue::enqueue(
        pool,
        &crate::jobs::JobKind::RegenerateDigest { node_id, graph_id },
        Some(account_id),
        Some(&format!("digest:{node_id}")),
        0,
    )
    .await
    {
        Ok(_) => {
            if let Err(e) = mark_requested(pool, node_id).await {
                tracing::warn!(error = %e, %node_id, "digest: failed to mark a reader's regeneration; it keeps its coral-count place");
            }
            true
        }
        Err(e) => {
            tracing::warn!(error = %e, %node_id, "digest: failed to queue a background regeneration");
            false
        }
    }
}

/// Hand a held reef back to the queue in place, under the shared retry rule
/// (`jobs::queue::hold`). Its payload, and with it a reader's request,
/// stays on the row.
async fn hold_reef(pool: &PgPool, job: &JobRow, node_id: Uuid, code: &str) -> queue::Held {
    match queue::hold(pool, job.id, code).await {
        Ok(held) => held,
        Err(e) => {
            tracing::warn!(error = %e, job_id = %job.id, %node_id, code, "digest: failed to hold the reef; the lease reaper retries it");
            queue::Held::NotRunning
        }
    }
}

/// A pass stopped on a daily bound or the shared cap: defer every other due
/// pass of the graph to the same next-day time in one statement, instead of
/// claiming, completing and re-inserting each of them (2026-10-20: ~5,600
/// such round trips a day). They come back together after midnight and the
/// lane orders them.
async fn defer_graph_to_next_day(pool: &PgPool, graph_id: Uuid, delay_secs: i64) {
    let deferred = sqlx::query(
        "UPDATE jobs SET run_after = now() + make_interval(secs => $2) \
          WHERE kind='RegenerateDigest' AND status='pending' AND payload->>'graph_id'=$1::text \
            AND run_after < now() + make_interval(secs => $2)",
    )
    .bind(graph_id)
    .bind(delay_secs as f64)
    .execute(pool)
    .await;
    match deferred {
        Ok(done) => tracing::info!(%graph_id, deferred = done.rows_affected(), delay_secs,
            "digest: daily bound reached; the graph's other due passes deferred to the next UTC day"),
        Err(e) => tracing::warn!(error = %e, %graph_id, "digest: failed to defer the graph's due passes; each is held when claimed"),
    }
}

/// Hold for the next UTC day: this reef and the rest of the graph's due
/// passes with it. A known clear time, so the hold streak is unchanged.
async fn hold_until_next_day(pool: &PgPool, job: &JobRow, node_id: Uuid, graph_id: Uuid) -> i64 {
    let delay = crate::managed::enrichment::secs_until_next_utc_day(chrono::Utc::now());
    if let Err(e) = queue::hold_until(pool, job.id, "daily_bound", delay).await {
        tracing::warn!(error = %e, job_id = %job.id, %node_id, "digest: failed to hold the reef for the next day; the lease reaper retries it");
    }
    defer_graph_to_next_day(pool, graph_id, delay).await;
    delay
}

/// `pub` so integration tests can drive one dequeued job end-to-end against
/// a real Postgres, mirroring `workers::summary::process_job`'s visibility.
#[allow(clippy::too_many_arguments)]
pub async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    control: &PgPool,
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    llm: &dyn LlmProvider,
    r2: Option<&R2Client>,
    job: JobRow,
) {
    let node_id = job
        .payload
        .get("node_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Uuid>().ok());
    let graph_id = job
        .payload
        .get("graph_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Uuid>().ok());
    let (Some(node_id), Some(graph_id)) = (node_id, graph_id) else {
        tracing::error!(job_id = %job.id, "digest: malformed payload (missing node_id/graph_id)");
        if let Err(e) =
            queue::fail(pool, job.id, "malformed payload: missing node_id/graph_id").await
        {
            tracing::error!(error = %e, job_id = %job.id, "failed to mark malformed digest job failed");
        }
        return;
    };

    // ── Per-job RLS scope (org-tenancy Phase W.2) ─────────────────────────────
    let Some(account_id) = verify_job_scope(pool, &job, graph_id, "digest").await else {
        return;
    };
    let scope = JobScope::for_job(tenant, pool, account_id);

    // Switched off between claim and processing: spend nothing, write
    // nothing, hand the reef back for a later pass.
    if !digests_claim_allowed() {
        hold_reef(pool, &job, node_id, "switched_off").await;
        return;
    }
    // Settle any receipt an earlier crashed attempt of this job left behind
    // before opening a new call (managed::enrichment step 1).
    if let Err(deny) = crate::managed::enrichment::recover_job(&scope, control, tenant, account_id, job.id).await {
        tracing::warn!(job_id = %job.id, error = %deny, "digest: managed receipt recovery unavailable — retrying the job");
        if let Err(e) = queue::fail(pool, job.id, "managed enrichment recovery unavailable").await {
            tracing::error!(error = %e, job_id = %job.id, "failed to mark digest job failed");
        }
        return;
    }
    let (calls_left, usd_left) = match daily_room(&scope, account_id, graph_id).await {
        Ok(left) => left,
        Err(e) => {
            tracing::error!(error = %e, job_id = %job.id, "digest: daily bounds unreadable — failing job");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark digest job failed");
            }
            return;
        }
    };
    if calls_left < MAX_CALLS_PER_DIGEST || usd_left < DIGEST_PASS_RESERVE_MICRO_USD {
        let delay = hold_until_next_day(pool, &job, node_id, graph_id).await;
        tracing::info!(job_id = %job.id, %node_id, %graph_id, calls_left, usd_left_micro = usd_left, delay_secs = delay,
            "digest: daily digest bound reached; reef rescheduled for the next UTC day");
        return;
    }
    let mut managed = Managed {
        control,
        tenant,
        account_id,
        graph_id,
        job: crate::managed::enrichment::EnrichmentJob { id: job.id, kind: job.kind.clone(), attempts: job.attempts },
        principal: None,
        calls_left,
    };

    match regenerate(&scope, store, embedder, llm, &mut managed, pool, r2, node_id, job.id).await {
        Ok(()) => {
            // `regenerate`'s `Ok(())` covers "digest regenerated", a stale
            // publish (nothing written, the next coral change re-enqueues),
            // and its documented no-op skips (reef gone, no longer eligible).
            if let Err(e) = queue::complete(pool, job.id, queue::JobOutcome::DidWork).await {
                tracing::error!(error = %e, job_id = %job.id, "digest: failed to complete job");
            }
        }
        Err(Failure::Stop(Stop::NextDay)) => {
            let delay = hold_until_next_day(pool, &job, node_id, graph_id).await;
            tracing::info!(job_id = %job.id, %node_id, delay_secs = delay,
                "digest: daily cap reached; reef held for the next UTC day");
        }
        Err(Failure::Stop(Stop::Held(code))) => {
            let held = hold_reef(pool, &job, node_id, code).await;
            tracing::info!(job_id = %job.id, %node_id, code, ?held,
                "digest: held by managed admission; reef back on the queue under the shared retry rule");
        }
        Err(Failure::Retry(e)) => {
            tracing::error!(error = %e, job_id = %job.id, %node_id, "digest: failing job");
            if let Err(qe) = queue::fail(pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark digest job failed");
            }
        }
    }
}

/// Digest calls the graph may still open today, and micro-USD of the
/// account's daily digest bound left (every graph of the account).
async fn daily_room(scope: &JobScope<'_>, account_id: Uuid, graph_id: Uuid) -> Result<(i64, i64), String> {
    let bound = crate::managed::enrichment::daily_bound(DIGEST_DAILY_CALLS_ENV, DEFAULT_DIGEST_DAILY_CALLS_PER_GRAPH);
    let usd_bound = digest_daily_usd_bound();
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let used = crate::managed::enrichment::calls_today(&mut tx, account_id, Some(graph_id), INPUT_NODE)
        .await
        .map_err(|e| e.to_string())?;
    let spent = crate::managed::enrichment::spend_today_micro_usd(&mut tx, account_id, None, INPUT_NODE)
        .await
        .map_err(|e| e.to_string())?;
    tx.rollback().await.ok();
    Ok(((bound - used).max(0), (usd_bound - spent).max(0)))
}

async fn begin_scope(scope: &JobScope<'_>) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => begin_account_scoped(pool, *account_id).await,
        JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool).await,
    }
}

fn digest_marker(node_id: Uuid, title: &str) -> crate::managed::enrichment::EnrichmentInput {
    crate::managed::enrichment::EnrichmentInput::new(INPUT_NODE, node_id, 0, title.as_bytes())
}

fn edge_input(edge_id: Uuid, line: &str) -> crate::managed::enrichment::EnrichmentInput {
    crate::managed::enrichment::EnrichmentInput::new(INPUT_EDGE, edge_id, 0, line.as_bytes())
}

/// The inputs one call binds: the corals in order, then each established
/// edge line the prompt carries, then the reef marker.
fn digest_inputs(
    node_id: Uuid,
    title: &str,
    items: &[(Uuid, String)],
    revisions: &HashMap<Uuid, i64>,
    edges: &[EdgeFact],
) -> Vec<crate::managed::enrichment::EnrichmentInput> {
    let mut inputs: Vec<crate::managed::enrichment::EnrichmentInput> = items
        .iter()
        .map(|(id, text)| {
            crate::managed::enrichment::EnrichmentInput::new(INPUT_ATOM, *id, revisions.get(id).copied().unwrap_or(0).max(0), text.as_bytes())
        })
        .collect();
    inputs.extend(edges.iter().map(|e| edge_input(e.edge_id, &e.line)));
    inputs.push(digest_marker(node_id, title));
    inputs
}

/// One established relationship as the digest prompt states it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EdgeFact {
    pub edge_id: Uuid,
    pub line: String,
}

/// `- {src} {predicate} {dst}\n`: the reef itself is named by `title`, any
/// other endpoint by its Library title, or by its id when that is hidden.
pub(crate) fn edge_line(
    node_id: Uuid,
    title: &str,
    title_of: &HashMap<Uuid, String>,
    src: Uuid,
    predicate: &str,
    dst: Uuid,
) -> String {
    let name = |id: Uuid| {
        if id == node_id {
            title.to_string()
        } else {
            title_of.get(&id).cloned().unwrap_or_else(|| id.to_string())
        }
    };
    format!("- {} {predicate} {}\n", name(src), name(dst))
}

fn render_edge_lines(edges: &[EdgeFact]) -> String {
    if edges.is_empty() {
        return String::new();
    }
    let mut lines = String::from("\nEstablished facts about this reef (confirmed relationships):\n");
    for e in edges {
        lines.push_str(&e.line);
    }
    lines
}

/// The CURRENT line of each of `edge_ids` that is still an established edge
/// of `node_id` in `graph_id`, as visible in `conn`'s scope. The publish
/// re-verify and the reader-side digest check (`crate::reef_digest_read`)
/// both use it, so a bound edge that was rejected, deleted, re-pointed, or
/// whose other endpoint's title changed or is now hidden reads as changed.
pub(crate) async fn current_edge_lines(
    conn: &mut sqlx::PgConnection,
    graph_id: Uuid,
    node_id: Uuid,
    title: &str,
    edge_ids: &[Uuid],
) -> Result<HashMap<Uuid, String>, sqlx::Error> {
    if edge_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(Uuid, Uuid, String, Uuid, String, String)> = sqlx::query_as(
        "SELECT edge_id, src_node_id, predicate, dst_node_id, origin, status FROM relationship_edge \
          WHERE graph_id = $1 AND edge_id = ANY($2) AND (src_node_id = $3 OR dst_node_id = $3)",
    )
    .bind(graph_id)
    .bind(edge_ids)
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut others: Vec<Uuid> = rows.iter().flat_map(|r| [r.1, r.3]).filter(|id| *id != node_id).collect();
    others.sort();
    others.dedup();
    let title_of: HashMap<Uuid, String> = if others.is_empty() {
        HashMap::new()
    } else {
        sqlx::query_as::<_, (Uuid, String)>(crate::store::verdict::NODE_TITLES_SQL)
        .bind(graph_id)
        .bind(&others)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .collect()
    };
    Ok(rows
        .into_iter()
        .filter(|(_, _, _, _, origin, status)| {
            // Same defaults as `store::relationship_edge_from_row`, so the
            // worker and this check classify one row identically.
            let origin = EdgeOrigin::parse(origin).unwrap_or(EdgeOrigin::Stated);
            let status = EdgeStatus::parse(status).unwrap_or(EdgeStatus::Suggested);
            matches!(origin, EdgeOrigin::Stated)
                || (matches!(origin, EdgeOrigin::Inferred) && matches!(status, EdgeStatus::Confirmed))
        })
        .map(|(edge_id, src, predicate, dst, _, _)| (edge_id, edge_line(node_id, title, &title_of, src, &predicate, dst)))
        .collect())
}

/// `atoms.content_revision` for `ids`, under the job's scope.
async fn scoped_atom_revisions(scope: &JobScope<'_>, ids: &[Uuid]) -> Result<HashMap<Uuid, i64>, String> {
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let rows: Vec<(Uuid, i64)> = sqlx::query_as("SELECT atom_id, content_revision FROM atoms WHERE atom_id = ANY($1)")
        .bind(ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().collect())
}

/// The regeneration pass itself. `Err(Retry)` fails (and, via `fail`'s
/// backoff, retries) the job — used for anything that might succeed on a
/// later attempt (DB error, transient LLM error, atoms not embedded YET).
/// `Err(Stop)` is a managed hold: nothing was written and the reef is
/// rescheduled. A reef that is simply no longer due for a digest is `Ok(())`.
#[allow(clippy::too_many_arguments)]
async fn regenerate(
    scope: &JobScope<'_>,
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    llm: &dyn LlmProvider,
    managed: &mut Managed<'_>,
    pool: &PgPool,
    r2: Option<&R2Client>,
    node_id: Uuid,
    job_id: Uuid,
) -> Result<(), Failure> {
    let (tenant, account_id, graph_id) = (managed.tenant, managed.account_id, managed.graph_id);
    let Some(ctx) = scoped_node_digest_context(scope, store, node_id)
        .await
        .map_err(|e| e.to_string())?
    else {
        tracing::info!(%node_id, "digest: reef no longer exists; skipping");
        return Ok(());
    };

    // Re-validate eligibility/cadence at CLAIM time (§2.3), not just at
    // enqueue time — a job can sit `pending` for a while, and the reef's
    // state (type, coral_count, whether it's already been digested since)
    // can have moved on. Mirrors the SAME arithmetic the SQL trigger
    // evaluated to enqueue this job in the first place.
    if !digest_eligible(&ctx.node_type, ctx.coral_count) {
        tracing::info!(%node_id, node_type = %ctx.node_type, coral_count = ctx.coral_count, "digest: no longer eligible; skipping");
        return Ok(());
    }
    // Freshness is decided below by the inputs themselves, not by the coral
    // counters: a pass is queued by growth or a rename (the trigger), a
    // reader whose stored digest no longer verifies, or the backfill of
    // reefs without an attributed digest. The counters cannot tell a digest
    // written before managed enrichment, or one whose coral was edited,
    // from a fresh one; the selection key can.
    //
    // Read straight from the row: the generic context read
    // (`node_digest_context_scoped`) fences a stored digest (it returns no
    // digest and a zero counter), so a gate built on it saw every reef as
    // never digested and regenerated every queued pass.
    let (stored, coral_count_now) = scoped_stored_digest(scope, node_id).await.map_err(|e| e.to_string())?;

    // ── §2.4: embed the title, rank corals, greedily select ──────────────────
    // The server embedder is the paid cloud model, so the title embed is
    // metered on this job like the digest's own completion.
    let title_usage = crate::llm::usage::UsageContext::new(pool.clone(), scope.usage_account_id(), Some(graph_id))
        .with_source_job(job_id);
    let title_vec = crate::embedding::submit_one(
        embedder,
        crate::embedding::Purpose::Metered { usage: Some(&title_usage), task: DIGEST_TITLE_EMBED_TASK },
        &ctx.title,
    )
    .await
    .map_err(|e| format!("embed title failed: {e}"))?;
    let candidates = scoped_node_digest_coral_candidates(
        scope,
        store,
        node_id,
        &title_vec,
        DIGEST_CANDIDATE_POOL,
    )
    .await
    .map_err(|e| e.to_string())?;
    let selected_ids = select_digest_corals(&candidates);
    if selected_ids.is_empty() {
        // Coral-count-eligible but nothing embedded yet (embedding lags
        // ingestion). Transient — fail so the bounded backoff retries rather
        // than silently producing an empty digest.
        return Err(Failure::Retry("no embedded corals available yet for selection".to_string()));
    }

    // ── §2.6: established edges (the SAME trust tier §5.3 defines) ───────────
    let edges = scoped_list_relationship_edges_for_nodes(scope, store, graph_id, &[node_id])
        .await
        .map_err(|e| e.to_string())?;
    let established: Vec<&RelationshipEdge> =
        edges.iter().filter(|e| is_established_edge(e)).collect();
    // Each established edge is bound as its own `digest_edge` input over the
    // exact line the prompt carries (both endpoint titles included), so a
    // reader-side check can refuse a digest whose edge facts or related reef
    // titles it may no longer see (`crate::reef_digest_read`).
    let edges: Vec<EdgeFact> = if established.is_empty() {
        Vec::new()
    } else {
        let mut other_ids: Vec<Uuid> = established
            .iter()
            .flat_map(|e| [e.src_node_id, e.dst_node_id])
            .filter(|id| *id != node_id)
            .collect();
        other_ids.sort();
        other_ids.dedup();
        let titles = scoped_nodes_by_ids(scope, store, graph_id, &other_ids)
            .await
            .map_err(|e| e.to_string())?;
        let title_of: HashMap<Uuid, String> = titles.into_iter().collect();
        established
            .iter()
            .map(|e| EdgeFact {
                edge_id: e.edge_id,
                line: edge_line(node_id, &ctx.title, &title_of, e.src_node_id, &e.predicate, e.dst_node_id),
            })
            .collect()
    };

    // ── Corals: selected atom_ids, content-loaded, in selection order ────────
    // Loads inline AND R2-only corals (see the module doc's R2-cutover
    // section); errors rather than quietly summarising a subset.
    let mut content_by_id =
        load_selected_coral_content(scope, tenant, pool, r2, account_id, &selected_ids).await?;
    // Preserve selection order (relevance-ranked, not insertion order of the
    // HashMap).
    let items: Vec<(Uuid, String)> = selected_ids
        .iter()
        .filter_map(|id| content_by_id.remove(id).map(|c| (*id, c)))
        .collect();
    if items.is_empty() {
        return Err(Failure::Retry("no coral content available for the selected corals".to_string()));
    }
    let revisions = scoped_atom_revisions(scope, &selected_ids).await?;

    let digest_model =
        crate::llm::model_config::model_from_env("DIGEST_MODEL", DIGEST_MODEL_DEFAULT);
    let key = selection_key(&digest_model, &ctx.title, &items, &revisions, &edges);
    if stored.managed_receipt.is_some()
        && stored.selection_key.as_deref() == Some(key.as_str())
        && stored.digested_title.as_deref() == Some(ctx.title.as_str())
    {
        // Same inputs as the stored digest: nothing to write but the
        // counter, so the growth rule measures from now.
        let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
        store
            .set_node_digest_coral_count_scoped(&mut tx, node_id, coral_count_now)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        tracing::info!(%node_id, coral_count = coral_count_now,
            "digest: inputs unchanged since the stored digest; not regenerated");
        return Ok(());
    }

    let usage = crate::llm::usage::UsageContext::new(pool.clone(), scope.usage_account_id(), Some(graph_id))
        .with_source_job(job_id);

    // Jev's keep-picker, metered as its own managed call. A hold stops the
    // pass (never a second paid call); any other failure falls open to the
    // unfiltered writer, as before.
    let judge_mode = crate::decision::jev_judge_mode();
    let mut items = items;
    let jev_seat = judge_mode.is_active()
        && crate::decision::jev_provider_kind() != Some(crate::decision::JevProviderKind::TypeSafe);
    if jev_seat {
        let work = managed.work(digest_inputs(node_id, &ctx.title, &items, &revisions, &[]))?;
        let principal = managed.principal(scope, &work).await?;
        managed.take_call()?;
        let req = crate::decision::judge::digest_keep_request(&ctx.title, &items);
        let call = crate::managed::ManagedCall {
            plane: scope,
            control: managed.control,
            tenant,
            account_id,
            principal,
            fallback_model: None,
            subject: crate::managed::Subject::Enrichment { work: &work },
        };
        match crate::decision::openrouter::evaluate_managed_enrichment(llm, &req, &usage, call).await {
            Ok(resp) => {
                let scores = crate::decision::judge::digest_keep_scores(&resp, &items);
                let keep: std::collections::HashSet<Uuid> = scores
                    .iter()
                    .filter(|(_, n)| *n >= crate::decision::NOUL_YES_THRESHOLD)
                    .map(|(id, _)| *id)
                    .collect();
                tracing::info!(mode = judge_mode.as_str(), considered = scores.len(), keep = keep.len(),
                    "digest: Jev picker (managed_enrichment)");
                if judge_mode.is_live() && !keep.is_empty() {
                    items.retain(|(id, _)| keep.contains(id));
                }
            }
            Err(e @ LlmError::Managed(_)) => return Err(managed_failure(e)),
            Err(e) => tracing::warn!(error = %e, "digest: Jev picker failed — unfiltered LLM writer"),
        }
    }

    let max_tokens = crate::llm::model_config::digest_max_tokens();

    let first = try_digest_completion(scope, llm, managed, &usage, &digest_model, node_id, &ctx.title, &edges, &items, &revisions, max_tokens).await?;
    let (parsed, resp, used_items, work) = match first {
        Ok((parsed, resp, work)) => (parsed, resp, items.clone(), work),
        Err(first_resp) => {
            // Truncation-aware retry (production regression, 2026-08 —
            // mirrors `synthesis::synthesize`'s). A response cut off at
            // `max_tokens` is systemic — retry with MORE room and the FULL
            // coral set; any other parse failure (the model ignored the
            // JSON-only instruction) gets the reduced-item retry. The retry
            // is its own managed call.
            let truncated = crate::llm::is_truncated(&first_resp);
            let (retry_items, retry_max_tokens): (Vec<(Uuid, String)>, u32) = if truncated {
                tracing::info!(%node_id, coral_count = items.len(), max_tokens,
                    retry_max_tokens = max_tokens.saturating_mul(2),
                    "digest: first attempt truncated at max_tokens — retrying with doubled max_tokens and the full coral set");
                (items.clone(), max_tokens.saturating_mul(2))
            } else {
                let reduced_count = ((items.len() * 4) / 5)
                    .max(1)
                    .min(items.len().saturating_sub(1).max(1));
                tracing::info!(%node_id, coral_count = items.len(), retry_coral_count = reduced_count.min(items.len()),
                    "digest: first attempt failed to parse (not a max_tokens truncation) — retrying with a reduced coral set");
                (items[..reduced_count.min(items.len())].to_vec(), max_tokens)
            };
            if retry_items.is_empty() {
                return Err(Failure::Retry("digest synthesis failed after retry: empty coral set".to_string()));
            }
            match try_digest_completion(scope, llm, managed, &usage, &digest_model, node_id, &ctx.title, &edges, &retry_items, &revisions, retry_max_tokens).await? {
                Ok((parsed, resp, work)) => (parsed, resp, retry_items, work),
                Err(_) => return Err(Failure::Retry("digest synthesis failed after retry: unparseable response".to_string())),
            }
        }
    };

    let excerpts: Vec<Value> = parsed
        .excerpts
        .into_iter()
        .filter_map(|e| {
            let idx = e.index.checked_sub(1)?;
            let (atom_id, _) = used_items.get(idx)?;
            Some(
                json!({ "atom_id": atom_id, "excerpt": e.excerpt, "contribution": e.contribution }),
            )
        })
        .collect();

    // ── Publish: re-read the reef and the corals under row locks, verify the
    //    call's inputs, write the digest and its counter in one transaction ──
    let mut tx = begin_scope(scope).await.map_err(|e| e.to_string())?;
    let (title_now, coral_count_at_publish): (Option<String>, i64) = sqlx::query_as::<_, (Option<String>, i64)>(
        "SELECT library_node_title(graph_id,node_id), coral_count::bigint FROM nodes WHERE node_id=$1 FOR UPDATE",
    )
    .bind(node_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| e.to_string())?
    .unwrap_or((None, 0));
    let used_ids: Vec<Uuid> = used_items.iter().map(|(id, _)| *id).collect();
    let revisions_now: HashMap<Uuid, i64> = sqlx::query_as::<_, (Uuid, i64)>(
        "SELECT atom_id, content_revision FROM atoms WHERE atom_id = ANY($1) ORDER BY atom_id FOR SHARE",
    )
    .bind(&used_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| e.to_string())?
    .into_iter()
    .collect();
    let edge_ids: Vec<Uuid> = work.inputs().iter().filter(|i| i.kind == INPUT_EDGE).map(|i| i.id).collect();
    let edges_now = match title_now.as_deref() {
        Some(title) => current_edge_lines(&mut *tx, graph_id, node_id, title, &edge_ids)
            .await
            .map_err(|e| e.to_string())?,
        None => HashMap::new(),
    };
    let current: Vec<crate::managed::enrichment::EnrichmentInput> = work
        .inputs()
        .iter()
        .filter_map(|input| match input.kind.as_str() {
            INPUT_ATOM => {
                let revision = *revisions_now.get(&input.id)?;
                let mut now = input.clone();
                now.content_revision = revision.max(0);
                Some(now)
            }
            INPUT_EDGE => edges_now.get(&input.id).map(|line| edge_input(input.id, line)),
            INPUT_NODE => title_now.as_deref().map(|title| digest_marker(node_id, title)),
            _ => None,
        })
        .collect();
    let verified = match crate::managed::enrichment::verify_inputs_locked(&mut tx, &work, &current).await {
        Ok(crate::managed::enrichment::InputCheck::Fresh(verified)) => verified,
        Ok(crate::managed::enrichment::InputCheck::Stale) => {
            tx.commit().await.map_err(|e| e.to_string())?;
            tracing::info!(%node_id, job_id = %job_id,
                "digest: the reef or a coral changed during the call — published nothing (discarded_stale)");
            return Ok(());
        }
        Ok(crate::managed::enrichment::InputCheck::AlreadyWritten) => {
            tx.rollback().await.ok();
            return Ok(());
        }
        Err(_) => {
            tx.rollback().await.ok();
            return Err(Failure::Stop(Stop::Held(crate::jobs::queue::MANAGED_UNAVAILABLE)));
        }
    };
    let digest_json = json!({
        "atoms_key": "",
        "digested_title": ctx.title,
        "summary": parsed.summary,
        "excerpts": excerpts,
        "managed_receipt": verified.receipt(),
        "selection_key": key,
    })
    .to_string();
    store
        .set_node_digest_scoped(&mut tx, node_id, &digest_json)
        .await
        .map_err(|e| e.to_string())?;
    // The row's own counter, the one the regeneration trigger measures growth
    // against (not the reader-permitted count in `ctx`, which can be lower
    // and would re-fire the growth rule on every later coral).
    store
        .set_node_digest_coral_count_scoped(&mut tx, node_id, coral_count_at_publish)
        .await
        .map_err(|e| e.to_string())?;
    verified.mark_written(&mut tx).await.map_err(|e| format!("managed publish unavailable: {e}"))?;
    tx.commit().await.map_err(|e| e.to_string())?;

    tracing::info!(
        %node_id,
        job_id = %job_id,
        coral_count = ctx.coral_count,
        selected = used_items.len(),
        stop_reason = ?resp.stop_reason,
        output_tokens = resp.output_tokens,
        model = %digest_model,
        "digest: regenerated (managed_enrichment)"
    );
    Ok(())
}

/// One digest-writer attempt as its own managed call: build the user message
/// from `items`, send it, and parse the JSON. `Ok(Err(response))` is a
/// response that did not parse, kept so `regenerate` can inspect
/// `stop_reason` for the truncation-aware retry.
#[allow(clippy::too_many_arguments)]
async fn try_digest_completion(
    scope: &JobScope<'_>,
    llm: &dyn LlmProvider,
    managed: &mut Managed<'_>,
    usage: &crate::llm::usage::UsageContext,
    digest_model: &str,
    node_id: Uuid,
    title: &str,
    edges: &[EdgeFact],
    items: &[(Uuid, String)],
    revisions: &HashMap<Uuid, i64>,
    max_tokens: u32,
) -> Result<Result<(DigestJson, CompletionResponse, crate::managed::enrichment::EnrichmentWork), CompletionResponse>, Failure> {
    let mut passage = String::new();
    for (i, (_, text)) in items.iter().enumerate() {
        passage.push_str(&format!("[{}] {}\n\n", i + 1, text));
    }
    let edge_lines = render_edge_lines(edges);
    let user_msg = format!("Reef title: {title}\n{edge_lines}\nLinked corals:\n{passage}");
    let work = managed.work(digest_inputs(node_id, title, items, revisions, edges))?;
    let principal = managed.principal(scope, &work).await?;
    managed.take_call()?;
    let request = CompletionRequest {
        messages: vec![Message { role: Role::User, content: user_msg }],
        model: digest_model.to_string(),
        max_tokens,
        system: Some(DIGEST_SYSTEM_PROMPT.to_string()),
        reasoning: crate::llm::ReasoningSetting::Off,
        ..Default::default()
    };
    let (resp, parsed) = crate::managed::enrichment::complete(
        llm, scope, managed.control, managed.tenant, &work, principal, request, usage, "digest",
        |response| serde_json::from_str::<DigestJson>(extract_json_object(&response.content)).ok(),
    )
    .await
    .map_err(managed_failure)?;
    Ok(match parsed {
        Some(parsed) => Ok((parsed, resp, work)),
        None => Err(resp),
    })
}

// ── Scope-aware Store wrappers (org-tenancy Phase W.2) ────────────────────────
//
// Same shape as `workers::entity_resolution`'s equivalents: each wrapper runs
// ONE RLS-covered `Store` call under the job's `JobScope` — consumer plane ⇒
// the `_scoped` variant inside a fresh `begin_account_scoped` transaction;
// org tenant ⇒ the same `_scoped` variant inside a fresh
// `begin_tenant_system_scoped_without_account` transaction. NEITHER arm is
// ambient. `migrations_tenant/0009_tenant_rls` puts member-ocean policies on
// these tables inside a tenant DB too, so a new wrapper that runs the plain
// method on the org arm would, after OPS-4-T, digest the org and personal
// oceans and skip every member and team ocean without reporting anything.

/// The reef's stored digest (raw `body.digest`) and its `coral_count`
/// counter, under the job's scope. Only this worker reads the raw digest:
/// readers go through `crate::reef_digest_read`.
async fn scoped_stored_digest(scope: &JobScope<'_>, node_id: Uuid) -> Result<(StoredDigest, i64), StoreError> {
    let mut tx = begin_scope(scope).await?;
    let row: Option<(Option<Value>, i64)> =
        sqlx::query_as("SELECT body->'digest', coral_count::bigint FROM nodes WHERE node_id=$1")
            .bind(node_id)
            .fetch_optional(&mut *tx)
            .await?;
    tx.commit().await?;
    let (digest, coral_count) = row.unwrap_or((None, 0));
    let stored = digest
        .filter(Value::is_object)
        .and_then(|d| serde_json::from_value::<StoredDigest>(d).ok())
        .unwrap_or_default();
    Ok((stored, coral_count))
}

async fn scoped_node_digest_context(
    scope: &JobScope<'_>,
    store: &dyn Store,
    node_id: Uuid,
) -> Result<Option<NodeDigestContext>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store.node_digest_context_scoped(&mut tx, node_id).await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.node_digest_context_scoped(&mut tx, node_id).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

async fn scoped_node_digest_coral_candidates(
    scope: &JobScope<'_>,
    store: &dyn Store,
    node_id: Uuid,
    query_vec: &[f32],
    limit: i64,
) -> Result<Vec<DigestCandidate>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store
                .node_digest_coral_candidates_scoped(&mut tx, node_id, query_vec, limit)
                .await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.node_digest_coral_candidates_scoped(&mut tx, node_id, query_vec, limit).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

async fn scoped_list_relationship_edges_for_nodes(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<RelationshipEdge>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store
                .list_relationship_edges_for_nodes_scoped(&mut tx, graph_id, node_ids)
                .await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.list_relationship_edges_for_nodes_scoped(&mut tx, graph_id, node_ids).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

async fn scoped_nodes_by_ids(
    scope: &JobScope<'_>,
    store: &dyn Store,
    graph_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<(Uuid, String)>, StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let out = store
                .nodes_by_ids_scoped(&mut tx, graph_id, node_ids)
                .await?;
            tx.commit().await?;
            Ok(out)
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.nodes_by_ids_scoped(&mut tx, graph_id, node_ids).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}

/// One selected coral's stored content, before R2 resolution.
///
/// `content_inline` is `None`/empty for every atom written after the
/// 2026-08-04 R2 cutover; `blob_key` (`atoms.content_blob_key`, `NOT NULL`
/// since migration `0003_atoms`) is how that content is then reached. Same
/// two-case split `extract::LoadedAtom` models for a single atom, batched —
/// this worker selects up to [`DIGEST_SELECT_CAP`] corals per digest.
struct SelectedCoralRow {
    atom_id: Uuid,
    content_inline: Option<String>,
    blob_key: String,
    header_len: i32,
}

/// Load the stored rows for the selected corals under the job's scope,
/// WITHOUT filtering on `content_inline` — the filter this query used to
/// carry is what made post-cutover corals vanish from digests (module doc).
///
/// Runs the same query on both planes rather than going through
/// `Store::atom_inline_content` for org tenants, because that trait method's
/// return shape has no `content_blob_key` and widening it would ripple
/// through every `Store` implementation for one caller. Mirrors
/// `extract::scoped_load_atom`, which reads `atoms` the same direct way for
/// exactly the same reason.
async fn scoped_selected_coral_rows(
    scope: &JobScope<'_>,
    atom_ids: &[Uuid],
) -> Result<Vec<SelectedCoralRow>, StoreError> {
    const SQL: &str = r#"
        SELECT atom_id, content_inline, content_blob_key, COALESCE(header_len, 0) AS header_len
        FROM   atoms
        WHERE  atom_id = ANY($1)
    "#;
    let map = |r: sqlx::postgres::PgRow| SelectedCoralRow {
        atom_id: r.get("atom_id"),
        content_inline: r.get("content_inline"),
        blob_key: r.get("content_blob_key"),
        header_len: r.get("header_len"),
    };
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            let rows = sqlx::query(SQL)
                .bind(atom_ids)
                .fetch_all(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(rows.into_iter().map(map).collect())
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let rows = sqlx::query(SQL).bind(atom_ids).fetch_all(&mut *tx).await?;
            tx.commit().await?;
            Ok(rows.into_iter().map(map).collect())
        }
    }
}

/// Resolve every selected coral to its plaintext, inline or R2-only.
///
/// Fails — never degrades — when any selected coral cannot be resolved. See
/// the module doc for why a partial digest is the worse outcome: it is
/// indistinguishable from a complete one at every layer that reads it, and
/// the 77 reefs currently shipping incomplete digests are the proof.
///
/// The DEK is resolved ONCE for the batch and the blob GETs run
/// concurrently, mirroring `workers::embed::process_job`'s fetch loop rather
/// than calling `extract::fetch_r2_only_content` per coral (which would
/// re-resolve the DEK up to [`DIGEST_SELECT_CAP`] times for one digest).
pub(crate) async fn load_selected_coral_content(
    scope: &JobScope<'_>,
    tenant: &TenantId,
    pool: &PgPool,
    r2: Option<&R2Client>,
    account_id: Uuid,
    selected_ids: &[Uuid],
) -> Result<HashMap<Uuid, String>, String> {
    let rows = scoped_selected_coral_rows(scope, selected_ids)
        .await
        .map_err(|e| e.to_string())?;

    // A selected id with no row at all: the atom was deleted between
    // selection and this read, or RLS filters it. Not fetchable and not a
    // fetch failure — let the caller's ordering pass drop it, and let the
    // `items.is_empty()` guard catch the case where that leaves nothing.
    let mut resolved: HashMap<Uuid, String> = HashMap::with_capacity(rows.len());
    let mut r2_only: Vec<(Uuid, String, i32)> = Vec::new();
    for row in rows {
        match row.content_inline {
            Some(t) if !t.is_empty() => {
                resolved.insert(
                    row.atom_id,
                    crate::store::strip_header_prefix(&t, row.header_len).to_string(),
                );
            }
            // `content_inline = ''` (present but empty) is NOT the R2-only
            // shape and must not trigger a fetch. Deliberate divergence from
            // `extract::load_atom`, which folds NULL and '' together via
            // `unwrap_or_default()`: that worker handles ONE atom, so a
            // pointless GET costs only that job. This one fails the whole
            // digest if any fetch fails, and a pre-cutover atom carries a
            // `content_blob_key` that was never uploaded (R2 was not
            // configured before the cutover — the same stale keys
            // `routes::queries` now logs "blob not found" against). Routing
            // an empty-content atom down the R2 path would let one such row
            // dead-letter a reef's digest permanently, on pre-cutover data,
            // from a fix aimed at post-cutover data. An atom with empty
            // content has nothing to contribute either way: skip it and let
            // the caller's ordering pass drop it, exactly like a missing row.
            Some(_) => {
                tracing::debug!(
                    atom_id = %row.atom_id,
                    "digest: coral has empty inline content; skipping (not an R2 fetch)"
                );
            }
            None => r2_only.push((row.atom_id, row.blob_key, row.header_len)),
        }
    }

    if r2_only.is_empty() {
        return Ok(resolved);
    }

    let Some(r2_client) = r2 else {
        return Err(format!(
            "R2 not configured — cannot load {} R2-only coral(s) for this digest",
            r2_only.len()
        ));
    };
    let dek = crate::crypto::resolve_dek_for_tenant(tenant, pool, account_id)
        .await
        .map_err(|e| format!("DEK resolution failed: {e}"))?
        .ok_or_else(|| "account has no DEK — run key rotation first".to_string())?;

    let fetches = r2_only.iter().map(|(atom_id, blob_key, header_len)| async move {
        match r2_client.get_atom_blob(blob_key, &dek).await {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes).into_owned();
                // The blob holds the SAME string `content_inline` would
                // (`routes::ingest` uploads the chunk text it also persists,
                // and `header_len` is stamped on that atom row), so the
                // contextual-header strip applies identically to both.
                Ok((
                    *atom_id,
                    crate::store::strip_header_prefix(&text, *header_len).to_string(),
                ))
            }
            Err(e) => Err(format!("{atom_id}: {e}")),
        }
    });

    // Deadline the batch. `R2Client` configures no operation timeout, so a
    // stalled GET would otherwise hold this job in `running` indefinitely —
    // outliving the worker's shutdown grace window and leaving recovery to
    // the stale-job reaper. A timeout returns `Err` like any other fetch
    // failure, so `process_job` applies the normal retry backoff instead.
    let fetched = match tokio::time::timeout(R2_BATCH_FETCH_TIMEOUT, future::join_all(fetches)).await
    {
        Ok(results) => results,
        Err(_) => {
            return Err(format!(
                "R2 fetch for {} R2-only coral(s) exceeded {}s; failing the job for retry rather \
                 than holding the claim",
                r2_only.len(),
                R2_BATCH_FETCH_TIMEOUT.as_secs()
            ))
        }
    };

    let mut failures: Vec<String> = Vec::new();
    for result in fetched {
        match result {
            Ok((atom_id, text)) => {
                resolved.insert(atom_id, text);
            }
            Err(e) => failures.push(e),
        }
    }

    if !failures.is_empty() {
        return Err(format!(
            "R2 fetch failed for {} of {} R2-only coral(s); refusing to build a digest from a \
             subset: {}",
            failures.len(),
            r2_only.len(),
            failures.join("; ")
        ));
    }

    Ok(resolved)
}

/// Test-only entry point onto the private [`load_selected_coral_content`],
/// exposed for the same reason `extract::test_support_fetch_r2_only_content`
/// is: an external `backend/tests/*.rs` binary links this crate as an
/// ordinary dependency and cannot see private items, but the R2-cutover
/// regression needs to drive THIS EXACT function — the real loader, not a
/// reimplementation of it — against real `atoms` rows and a real
/// `R2Client`.
#[cfg(feature = "test-support")]
pub async fn test_support_load_selected_coral_content(
    tenant: &TenantId,
    pool: &PgPool,
    r2: Option<&R2Client>,
    account_id: Uuid,
    selected_ids: &[Uuid],
) -> Result<HashMap<Uuid, String>, String> {
    let scope = JobScope::for_job(tenant, pool, account_id);
    load_selected_coral_content(&scope, tenant, pool, r2, account_id, selected_ids).await
}

#[cfg(test)]
mod established_edge_tests {
    use super::*;
    use chrono::Utc;

    fn edge(origin: EdgeOrigin, status: EdgeStatus) -> RelationshipEdge {
        RelationshipEdge {
            edge_id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            src_node_id: Uuid::new_v4(),
            predicate: "reports_to".to_string(),
            dst_node_id: Uuid::new_v4(),
            origin,
            status,
            confidence: 1.0,
            provenance: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version: 1,
        }
    }

    #[test]
    fn stated_is_established_regardless_of_status() {
        assert!(is_established_edge(&edge(
            EdgeOrigin::Stated,
            EdgeStatus::Suggested
        )));
        assert!(is_established_edge(&edge(
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed
        )));
        // Stated edges are written as Confirmed in practice (D6), but the
        // predicate itself must not depend on that — origin alone suffices.
        assert!(is_established_edge(&edge(
            EdgeOrigin::Stated,
            EdgeStatus::Rejected
        )));
    }

    #[test]
    fn inferred_confirmed_is_established() {
        assert!(is_established_edge(&edge(
            EdgeOrigin::Inferred,
            EdgeStatus::Confirmed
        )));
    }

    #[test]
    fn inferred_suggested_is_not_established() {
        assert!(!is_established_edge(&edge(
            EdgeOrigin::Inferred,
            EdgeStatus::Suggested
        )));
    }

    #[test]
    fn inferred_rejected_is_not_established() {
        assert!(!is_established_edge(&edge(
            EdgeOrigin::Inferred,
            EdgeStatus::Rejected
        )));
    }
}

#[cfg(test)]
mod cadence_tests {
    use super::*;

    // ── digest_eligible ────────────────────────────────────────────────────────

    #[test]
    fn entity_below_min_is_ineligible() {
        assert!(!digest_eligible("entity", 2));
    }

    #[test]
    fn entity_at_min_is_eligible() {
        assert!(digest_eligible("entity", 3));
    }

    #[test]
    fn entity_above_min_is_eligible() {
        assert!(digest_eligible("entity", 298));
    }

    #[test]
    fn decision_never_eligible_regardless_of_size() {
        assert!(!digest_eligible("decision", 10_000));
    }

    #[test]
    fn thread_and_reference_never_eligible() {
        assert!(!digest_eligible("thread", 50));
        assert!(!digest_eligible("reference", 50));
    }

    // ── should_regenerate_digest: growth cadence ──────────────────────────────

    #[test]
    fn first_ever_digest_fires_as_soon_as_eligible() {
        // Never digested: due at the eligibility bar, though growth (3) is
        // below the +5 floor.
        assert!(should_regenerate_digest(3, 0, false));
        assert!(should_regenerate_digest(8, 0, false));
    }

    #[test]
    fn growth_below_absolute_floor_does_not_fire() {
        // digest_coral_count=20: 20% = 4, but the floor is 5 -> threshold 5.
        // growth=4 < 5.
        assert!(!should_regenerate_digest(24, 20, false));
    }

    #[test]
    fn growth_at_absolute_floor_fires() {
        // Same digest_coral_count=20 (threshold still the floor, 5): growth=5.
        assert!(should_regenerate_digest(25, 20, false));
    }

    #[test]
    fn small_digest_coral_count_uses_the_floor_not_the_percentage() {
        // digest_coral_count=10: 20% = 2, but floor is 5 -> threshold is 5.
        assert!(!should_regenerate_digest(14, 10, false)); // growth 4 < 5
        assert!(should_regenerate_digest(15, 10, false)); // growth 5 >= 5
    }

    #[test]
    fn large_digest_coral_count_uses_the_percentage_not_the_floor() {
        // digest_coral_count=1000: 20% = 200 > floor of 5.
        assert!(!should_regenerate_digest(1199, 1000, false)); // growth 199 < 200
        assert!(should_regenerate_digest(1200, 1000, false)); // growth 200 >= 200
    }

    #[test]
    fn ceil_rounds_up_the_percentage_threshold() {
        // digest_coral_count=21: 20% = 4.2 -> ceil = 5 (still the floor here,
        // but exercises the ceil() call rather than the max() short-circuit
        // hitting a value where floor and percentage coincide by coincidence).
        // digest_coral_count=26: 20% = 5.2 -> ceil = 6 > floor(5).
        assert!(!should_regenerate_digest(31, 26, false)); // growth 5 < 6
        assert!(should_regenerate_digest(32, 26, false)); // growth 6 >= 6
    }

    #[test]
    fn negative_growth_never_fires() {
        // Corals were removed since the last digest (e.g. an unmerge moved
        // some back out) — never regenerate on shrinkage alone.
        assert!(!should_regenerate_digest(5, 20, false));
    }

    #[test]
    fn no_growth_and_no_rename_does_not_fire() {
        assert!(!should_regenerate_digest(50, 50, false));
    }

    // ── should_regenerate_digest: rename ───────────────────────────────────────

    #[test]
    fn rename_fires_even_with_zero_growth() {
        assert!(should_regenerate_digest(50, 50, true));
    }

    #[test]
    fn rename_fires_even_with_negative_growth() {
        assert!(should_regenerate_digest(5, 50, true));
    }
}

#[cfg(test)]
mod selection_tests {
    use super::*;

    /// A one-hot vector of length `dim` with a `1.0` at `pos` — pairwise
    /// orthogonal (cosine similarity 0.0) to every other one-hot vector of
    /// the same length, so tests can isolate the relative-floor / per-source
    /// / cap behaviour from the novelty check entirely.
    fn one_hot(pos: usize, dim: usize) -> Vec<f32> {
        let mut v = vec![0.0f32; dim];
        v[pos] = 1.0;
        v
    }

    fn candidate(
        atom_id: Uuid,
        source_key: &str,
        score: f32,
        embedding: Vec<f32>,
    ) -> DigestCandidate {
        DigestCandidate {
            atom_id,
            source_key: source_key.to_string(),
            score,
            embedding,
        }
    }

    // ── cosine_similarity ──────────────────────────────────────────────────────

    #[test]
    fn cosine_similarity_of_identical_normalised_vectors_is_one() {
        let v = vec![0.6, 0.8]; // already unit length
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_similarity_of_orthogonal_vectors_is_zero() {
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]), 0.0);
    }

    #[test]
    fn cosine_similarity_dimension_mismatch_degrades_to_zero() {
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0, 0.0]), 0.0);
    }

    #[test]
    fn cosine_similarity_of_empty_vectors_is_zero() {
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
    }

    // ── select_digest_corals: empty / trivial ─────────────────────────────────

    #[test]
    fn empty_candidates_selects_nothing() {
        assert!(select_digest_corals(&[]).is_empty());
    }

    #[test]
    fn single_candidate_is_always_selected() {
        let a = Uuid::new_v4();
        let cands = vec![candidate(a, "s1", 0.5, one_hot(0, 4))];
        assert_eq!(select_digest_corals(&cands), vec![a]);
    }

    // ── relative floor (0.6 * best_score) ─────────────────────────────────────

    #[test]
    fn relative_floor_keeps_scores_at_or_above_sixty_percent_of_best() {
        let (a, b, c, d) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        // best = 1.0 -> floor = 0.6. b (0.6) clears it exactly; c (0.59) does not.
        let cands = vec![
            candidate(a, "s1", 1.0, one_hot(0, 4)),
            candidate(b, "s2", 0.6, one_hot(1, 4)),
            candidate(c, "s3", 0.59, one_hot(2, 4)),
            candidate(d, "s4", 0.1, one_hot(3, 4)),
        ];
        assert_eq!(select_digest_corals(&cands), vec![a, b]);
    }

    #[test]
    fn floor_self_normalises_to_a_low_scoring_reef() {
        // Every candidate scores low in absolute terms, but the floor is
        // RELATIVE to this reef's own best score, so all three still clear it.
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let cands = vec![
            candidate(a, "s1", 0.10, one_hot(0, 4)),
            candidate(b, "s2", 0.09, one_hot(1, 4)),
            candidate(c, "s3", 0.061, one_hot(2, 4)), // 0.061 >= 0.6*0.10 = 0.06
        ];
        assert_eq!(select_digest_corals(&cands), vec![a, b, c]);
    }

    // ── per-source cap (< 6) ───────────────────────────────────────────────────

    #[test]
    fn per_source_cap_stops_a_single_source_from_dominating() {
        // 8 candidates, all from ONE meeting (`base_source_id`-grouped),
        // all clearing the floor, all mutually orthogonal (no novelty
        // interaction) — the "46 consecutive chunks of a single meeting"
        // failure mode this cap exists to prevent.
        let ids: Vec<Uuid> = (0..8).map(|_| Uuid::new_v4()).collect();
        let cands: Vec<DigestCandidate> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| candidate(*id, "one-meeting", 1.0 - (i as f32) * 0.01, one_hot(i, 8)))
            .collect();
        let selected = select_digest_corals(&cands);
        assert_eq!(selected.len(), DIGEST_PER_SOURCE_CAP);
        // The six HIGHEST-scoring (earliest-ranked) of the eight are kept.
        assert_eq!(selected, ids[..DIGEST_PER_SOURCE_CAP].to_vec());
    }

    #[test]
    fn per_source_cap_is_independent_per_source() {
        // 6 from source A (fills the cap) + 1 from source B: B is still kept,
        // because the cap is per-source, not global.
        let a_ids: Vec<Uuid> = (0..6).map(|_| Uuid::new_v4()).collect();
        let b_id = Uuid::new_v4();
        let mut cands: Vec<DigestCandidate> = a_ids
            .iter()
            .enumerate()
            .map(|(i, id)| candidate(*id, "source-a", 1.0 - (i as f32) * 0.01, one_hot(i, 8)))
            .collect();
        cands.push(candidate(b_id, "source-b", 0.9, one_hot(7, 8)));
        let selected = select_digest_corals(&cands);
        assert!(
            selected.contains(&b_id),
            "a different source must not be capped by A's count"
        );
        assert_eq!(selected.len(), 7);
    }

    // ── novelty stop (max_sim < 0.92) ──────────────────────────────────────────

    #[test]
    fn novelty_stop_rejects_a_near_duplicate_from_a_different_source() {
        // B is a near-duplicate of A's embedding (cosine 1.0 >= 0.92) even
        // though it comes from a DIFFERENT source — isolates novelty from the
        // per-source cap. C is orthogonal to both and must still be kept.
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let cands = vec![
            candidate(a, "s1", 1.0, vec![1.0, 0.0]),
            candidate(b, "s2", 0.9, vec![1.0, 0.0]), // identical embedding -> novelty-rejected
            candidate(c, "s3", 0.8, vec![0.0, 1.0]), // orthogonal -> kept
        ];
        assert_eq!(select_digest_corals(&cands), vec![a, c]);
    }

    #[test]
    fn novelty_threshold_boundary_just_under_rejects_just_over_accepts() {
        // Construct b at EXACTLY cosine 0.92 to a (rejected: `>=`), and c at a
        // hair below 0.92 (accepted).
        let a = candidate(Uuid::new_v4(), "s1", 1.0, vec![1.0, 0.0]);
        // cos = 0.92 exactly.
        let at_threshold = candidate(
            Uuid::new_v4(),
            "s2",
            0.9,
            vec![0.92, (1.0f32 - 0.92f32 * 0.92f32).sqrt()],
        );
        // cos = 0.90, clearly under.
        let below_threshold = candidate(
            Uuid::new_v4(),
            "s3",
            0.8,
            vec![0.90, (1.0f32 - 0.90f32 * 0.90f32).sqrt()],
        );
        let ids_at = select_digest_corals(&[a.clone(), at_threshold.clone()]);
        assert_eq!(
            ids_at,
            vec![a.atom_id],
            "cosine == 0.92 must be rejected (>=)"
        );

        let ids_below = select_digest_corals(&[a.clone(), below_threshold.clone()]);
        assert_eq!(ids_below, vec![a.atom_id, below_threshold.atom_id]);
    }

    // ── hard cap (30) ───────────────────────────────────────────────────────────

    #[test]
    fn hard_cap_stops_at_thirty_even_with_room_under_every_other_rule() {
        // 40 candidates, every one from a DIFFERENT source (no per-source
        // cap interaction) and mutually orthogonal (no novelty interaction),
        // all clearing the floor (scores 1.00 down to 0.61).
        let dim = 40;
        let ids: Vec<Uuid> = (0..dim).map(|_| Uuid::new_v4()).collect();
        let cands: Vec<DigestCandidate> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                candidate(
                    *id,
                    &format!("source-{i}"),
                    1.0 - (i as f32) * 0.01,
                    one_hot(i, dim),
                )
            })
            .collect();
        let selected = select_digest_corals(&cands);
        assert_eq!(selected.len(), DIGEST_SELECT_CAP);
        assert_eq!(selected, ids[..DIGEST_SELECT_CAP].to_vec());
    }

    // ── ranking precondition ────────────────────────────────────────────────────

    #[test]
    fn selection_order_matches_input_score_order() {
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let cands = vec![
            candidate(a, "s1", 1.0, one_hot(0, 4)),
            candidate(b, "s2", 0.9, one_hot(1, 4)),
            candidate(c, "s3", 0.8, one_hot(2, 4)),
        ];
        assert_eq!(select_digest_corals(&cands), vec![a, b, c]);
    }
}
