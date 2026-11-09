//! Handlers for the `/v1/queries` resource.
//!
//! `POST /v1/queries` embeds the query, runs retrieval (vector + keyword),
//! expands via the knowledge graph, re-ranks, fetches atom content, and calls
//! the LLM synthesizer. Returns a structured JSON answer with citations and
//! timing metadata.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use utoipa::path as utoipa_path;
use uuid::Uuid;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Number of candidate atoms fetched from each retrieval path before re-ranking.
/// Higher values improve recall at the cost of reranker latency.
const RETRIEVAL_CANDIDATE_K: usize = 50;

/// Number of top-matching reefs (nodes) whose atoms are pulled in as additional
/// keyword candidates by the reef lexical-search path. Kept small: reef matches
/// are a precision signal (a curated title/summary hit), not broad recall.
const NODE_CANDIDATE_K: usize = 8;

/// Cap on atoms pulled in by the deterministic event-time scan
/// ([`event_time_window`] / [`apply_event_window_merge`]). Applied TWICE:
/// first PER GRAPH, as the `limit` passed to each graph's
/// `Store::timestamp_window_atoms` call; then again as a COMBINED cap across
/// all graphs in `apply_event_window_merge` (most-recent-first truncation),
/// so a multi-ocean ask with N reachable graphs can't force-merge more than
/// this many total atoms into the synthesis set regardless of N. Matches
/// [`RETRIEVAL_CANDIDATE_K`] — same order of magnitude as the other
/// candidate paths this augments.

/// Max typed relationship edges surfaced to synthesis (ADR-0008).
const SYNTHESIS_RELATIONSHIP_CAP: usize = 40;

/// Maximum number of distilled assertions injected into the `CLAIMS` block
/// (graph-temporal-memory §3.6, completing D2's `ENTITIES` narrowing). §3.6's
/// own recommendation: "current generation plus immediate predecessor, ~6
/// claims" — its own words note the exact depth beyond that pair is
/// deliberately undecided (§11), so this is a flat cap on the SELECTED set,
/// not a per-topic depth limit. A superseded claim's superseder can still
/// push the rendered count past this cap (the never-orphan invariant beats
/// the cap, same precedence as the atom-level force-include below it).
const SYNTHESIS_CLAIMS_CAP: usize = 6;

// ── Ask v2: multi-ocean retrieval (ADR-0011) ────────────────────────────────────

/// Default conservative relevance floor for the honesty guard (ADR-0011 G24).
/// An answer is only synthesized when at least one reranked atom scores ≥ this.
/// Deliberately low: it suppresses confident prose ONLY when retrieval found
/// nothing meaningfully relevant (the "Casey's mom → fabricated Jordan" class),
/// not when results are merely thin. Env-tunable via `ASK_RELEVANCE_FLOOR`.
const ASK_RELEVANCE_FLOOR_DEFAULT: f64 = 0.02;

/// Ceiling on the number of atoms handed to synthesis after the ocean-count
/// scaling (ADR-0011 A6). Keeps the prompt bounded no matter how many oceans are
/// in scope.
const SYNTHESIS_CONTEXT_CAP_CEILING: usize = 60;

/// Conservative relevance floor, read from `ASK_RELEVANCE_FLOOR` (default
/// [`ASK_RELEVANCE_FLOOR_DEFAULT`]). Values outside `[0, 1]` fall back to the
/// default.
pub(crate) fn ask_relevance_floor() -> f64 {
    match std::env::var("ASK_RELEVANCE_FLOOR") {
        Ok(v) => match v.trim().parse::<f64>() {
            Ok(f) if (0.0..=1.0).contains(&f) => f,
            _ => ASK_RELEVANCE_FLOOR_DEFAULT,
        },
        Err(_) => ASK_RELEVANCE_FLOOR_DEFAULT,
    }
}

// ── Relevance-floor SHADOW MODE (searchfix relevance-floor-shadow) ─────────────
//
// **This is diagnostic-only and changes NO production behavior.**
// `ASK_RELEVANCE_FLOOR_DEFAULT`/[`ask_relevance_floor`] above — the LIVE floor
// that actually gates whether `ask_ocean`/`create_query`/`create_query_stream`
// synthesize or abstain — is untouched by this section. This just computes, at
// the exact point the live honesty guard already decides, what a STRICTER
// candidate floor would have decided instead, and logs it when the two
// disagree — so a founder reviewing production logs can see how a real floor
// change would have behaved before anyone flips the live default.
//
// Threshold provenance: run `EVAL_EMBEDDER=ort cargo run -p coralswarm-backend
// --bin retrieval_eval -- --dump-scores` against a real Postgres with the real
// ONNX embedder (`backend/eval/README.md`) to reproduce these numbers.
// Per-category top-1 (best reranked candidate) score distribution over the 81
// golden queries:
//   abstention (14):                 min 0.5447  max 0.5977  mean 0.5638
//   direct + temporal + staleness (67): min 0.5900  max 0.9485  mean 0.6408
// The distributions OVERLAP: the abstention category's highest scorer (`a04`,
// 0.5977) scores higher than six answerable queries (`d14` 0.5900, `d36`
// 0.5902, `d40` 0.5931, `t02` 0.5934, `d03` 0.5963, `t04` 0.5973) — so there is
// NO threshold that separates all 14 abstention queries from all 67 answerable
// ones with zero cost. `0.588` sits in the actual gap between the abstention
// category's 13th-highest scorer (`a13`, 0.5852) and the answerable category's
// lowest scorer (`d14`, 0.5900): at `0.588`, 13/14 abstention queries correctly
// fall below it (vs. 0/14 under today's live `0.02` floor) while ALL 67
// answerable queries stay above it (zero false abstentions). Raising the floor
// further to also catch `a04` (>= ~0.598) would cost 6-7 answerable queries a
// false "I don't know" — a materially worse tradeoff for one extra abstention
// catch. `0.588` is therefore the candidate value carried into shadow mode.
const ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT: f64 = 0.588;

/// Shadow (diagnostic-only) relevance floor, read from `ASK_RELEVANCE_FLOOR_SHADOW`
/// (default [`ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT`]), parsed the same defensively
/// as [`ask_relevance_floor`]. Never gates production behavior directly — only
/// feeds [`shadow_floor_disagreement`]'s diagnostic comparison.
pub(crate) fn ask_relevance_floor_shadow() -> f64 {
    match std::env::var("ASK_RELEVANCE_FLOOR_SHADOW") {
        Ok(v) => match v.trim().parse::<f64>() {
            Ok(f) if (0.0..=1.0).contains(&f) => f,
            _ => ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT,
        },
        Err(_) => ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT,
    }
}

/// Whether the honesty guard would synthesize (`true`) or abstain (`false`)
/// given a candidate set's rerank scores and a floor. Mirrors, verbatim, the
/// `reranked.is_empty() || !any_cleared_floor` gate every call site applies —
/// kept as one function so the live and shadow checks can never drift apart.
fn honesty_guard_would_answer(scores: &[f64], floor: f64) -> bool {
    !scores.is_empty() && scores.iter().any(|&s| s >= floor)
}

/// A disagreement between the LIVE floor's honesty-guard outcome and the
/// SHADOW floor's outcome for one query's candidate set — `None` when they
/// agree (the common case today, since the shadow floor is far stricter than
/// the near-inert live `0.02`, so most agreement is "both answer").
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ShadowFloorDisagreement {
    pub top_score: f64,
    pub live_floor: f64,
    pub shadow_floor: f64,
    /// The outcome under today's live floor — what the caller actually got.
    pub live_would_answer: bool,
    /// The outcome the shadow floor would have produced instead.
    pub shadow_would_answer: bool,
}

/// Compare the live and shadow floor outcomes for one candidate set; `None`
/// when they agree. Pure/sync and takes raw scores (not `RankedAtom`) so it is
/// directly unit-testable against synthetic/fixture scores without a live
/// Postgres or a full rerank pipeline.
pub(crate) fn shadow_floor_disagreement(
    scores: &[f64],
    live_floor: f64,
    shadow_floor: f64,
) -> Option<ShadowFloorDisagreement> {
    let live_would_answer = honesty_guard_would_answer(scores, live_floor);
    let shadow_would_answer = honesty_guard_would_answer(scores, shadow_floor);
    if live_would_answer == shadow_would_answer {
        return None;
    }
    let top_score = scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    Some(ShadowFloorDisagreement {
        top_score,
        live_floor,
        shadow_floor,
        live_would_answer,
        shadow_would_answer,
    })
}

/// Compute the shadow-vs-live disagreement for `scores` (against the current
/// [`ask_relevance_floor`] / [`ask_relevance_floor_shadow`] env-configured
/// values) and, if they disagree, emit a structured diagnostic log line with
/// enough context for a founder reviewing production logs to see how a floor
/// change would have behaved. No-op (and zero behavior change) when they
/// agree. `graph_ids` is logged for correlation, never the query text itself
/// (private-ocean content, matching this file's existing log conventions).
pub(crate) fn log_shadow_floor_disagreement(
    request_id: Option<&str>,
    graph_ids: &[Uuid],
    scores: &[f64],
) {
    let live_floor = ask_relevance_floor();
    let shadow_floor = ask_relevance_floor_shadow();
    if let Some(d) = shadow_floor_disagreement(scores, live_floor, shadow_floor) {
        tracing::info!(
            request_id = request_id.unwrap_or("n/a"),
            graph_ids = ?graph_ids,
            top_score = d.top_score,
            live_floor = d.live_floor,
            shadow_floor = d.shadow_floor,
            live_outcome = if d.live_would_answer { "answer" } else { "abstain" },
            shadow_outcome = if d.shadow_would_answer { "answer" } else { "abstain" },
            "ask_relevance_floor shadow mode: live/shadow floor disagreement (diagnostic only, no behavior change)"
        );
    }
}

// ── §3.11 absolute vsim gate ─────────────────────────────────────────────────
//
// See [`multi_graph_retrieve`]'s own "§3.11 absolute vsim gate" comment for
// WHERE this runs and WHY it's scoped to raw `Store::vector_search` output
// only (never `keyword_hits`). This section is just the threshold + the pure
// conversion helper.

/// Default absolute vsim gate (§3.11). A candidate from `Store::vector_search`
/// is dropped BEFORE reranking when its cosine similarity (`1.0 -
/// distance/2.0`, `[0, 1]`) falls below this — unblended, unnormalised,
/// independent of the relative keyword-normalisation path and of ocean size
/// (§3.11 explicitly rejects size scaling: cosine similarity means the same
/// thing at any corpus size).
///
/// **Threshold provenance — real data, not a guess, but not live production
/// traffic either (none exists yet — pre-launch product; this is the same
/// caveat [`ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT`] above already carries).**
/// Measured with the REAL ONNX embedder (`EVAL_EMBEDDER=ort`, `bge-small-en-
/// v1.5` — the model production actually runs), against
/// `backend/eval/{fixtures/corpus.jsonl,golden/golden.jsonl}` (the existing
/// 53-doc / 103-query retrieval-eval golden set), via the exact same
/// `Store::vector_search` call this gate wraps. Reproduce with:
/// `EVAL_EMBEDDER=ort cargo run -p coralswarm-backend --bin retrieval_eval`
/// plus a one-off dump of `Store::vector_search`'s raw top-1 cosine
/// similarity per golden query (mirrors `--dump-scores`'s method, just on
/// the unblended vsim rather than the blended rerank score).
///
/// Two numbers decided it, both restricted to queries where the correct
/// answer's evidence is genuinely reachable ONLY via the vector path (i.e.
/// excluding the `reef` golden category — entity-name lookups resolved by
/// `keyword_search_node_atoms`'s title/summary match, which this gate never
/// touches; confirmed empirically that several `reef` queries' correct atom
/// has a vsim of 0.0–0.77, having NEVER been a `vector_search` candidate at
/// all, which is exactly why the gate must stay scoped to `vector_hits`
/// only):
///   - The WORST real vector-path match across all 79 non-reef answerable
///     golden queries is `0.8239` (`d14`).
///   - The abstention category's (14 queries, no true answer) top-1 vsim
///     tops out at `0.8185` for 12 of them; the remaining 2 (`a04` 0.8538,
///     `a13` 0.8497) exceed even the worst real match, so no absolute vsim
///     value catches them without also risking `d14` — same shape overlap
///     already documented for the blended score.
///
/// `0.82` sits in the real gap between those two numbers (`0.8185` <
/// `0.82` < `0.8239`): it fully empties the candidate-vector-pool for 12 of
/// the 14 abstention negatives (every one of their vector hits, not just the
/// top one, since top-1 is each query's maximum) at ZERO measured cost to
/// every vector-path-dependent answerable query.
///
/// **What this threshold is NOT:** a general-purpose abstention
/// classifier — that is explicitly a SEPARATE, undecided follow-up
/// (§3.11's other listed option, "raise the floor using
/// `log_shadow_floor_disagreement`"). This gate exists to fix the
/// empty/small-ocean case (`Store::vector_search`'s `LIMIT top_k` returning
/// its k nearest neighbours regardless of whether any are actually close),
/// not to resolve every abstention miss.
///
/// **§7.1's fixture ocean (`graph_temporal_eval_pg`) cannot reproduce this
/// exact threshold's calibration** — that harness is hardcoded to the
/// deterministic, model-free `HashEmbedder` (never `ort`), whose coarse
/// bag-of-words cosine similarity has a different, corpus-and-embedder-
/// specific distribution than `bge-small-en-v1.5`'s. The gate mechanism
/// (this constant, env-tunable below) is exercised identically on both
/// embedders; the specific NUMBER `0.82` is real-embedder-calibrated and is
/// not expected to reproduce the same abstention/answerable separation
/// under `HashEmbedder`.
const ASK_VSIM_GATE_DEFAULT: f64 = 0.82;

/// Absolute vsim gate (§3.11), read from `ASK_VSIM_GATE` (default
/// [`ASK_VSIM_GATE_DEFAULT`]), parsed the same defensively as
/// [`ask_relevance_floor`]. Unlike [`supersession_ranking_enabled`] /
/// `graph_traversal_enabled`, this has no on/off flag — §3.11 makes the
/// DECISION to ship the gate mechanism for real (the open question was only
/// ever the threshold VALUE, which this env var lets an operator override
/// without a redeploy if production traffic shows `0.82` needs to move).
pub(crate) fn ask_vsim_gate() -> f64 {
    match std::env::var("ASK_VSIM_GATE") {
        Ok(v) => match v.trim().parse::<f64>() {
            Ok(f) if (0.0..=1.0).contains(&f) => f,
            _ => ASK_VSIM_GATE_DEFAULT,
        },
        Err(_) => ASK_VSIM_GATE_DEFAULT,
    }
}

/// Whether supersession-aware ranking (Phase 5b) is turned on for real —
/// read from `SUPERSESSION_RANKING_ENABLED`, parsed defensively the same way
/// [`crate::relationships::inference::infer_review_enabled`] and (per the
/// Phase 5a dry-run gate) `SUPERSESSION_AUTO_EDGES` are. **Defaults ON**
/// (§3.9, flipped from its original default-OFF): any of `0`/`false`/`no`/
/// `off` (case-insensitive) is a deliberate OFF override; unset, blank, or
/// any other value falls through to the default — ON — mirroring exactly the
/// single-check-against-the-non-default-tokens shape the flag used before the
/// flip (originally: recognized-truthy -> ON, everything else, including
/// unset/blank/garbage, folded into the OFF default; now the mirror image).
///
/// **Why the flip was safe now, when it wasn't at ship time:** the flag
/// originally shipped OFF specifically because `supersession_edge` rows could
/// already exist via the pre-existing manual `POST /v1/nodes/{id}/supersede`
/// endpoint, so flipping the DEFAULT (rather than merely having the
/// mechanism exist) was an immediate, uncontrolled production behavior
/// change. The flip is gated on D1 (`Store::expand`'s seed-exclusion fix):
/// with that bug in place, `[SUPERSEDED]` annotation and the ranking penalty
/// reached roughly 3% of atoms on a large reef, so flipping the default
/// before D1 landed would have turned on a penalty that mostly didn't fire.
/// D1 is in this branch's base (PR #420), so that precondition is met.
///
/// This is a REAL production behavior change, not a theoretical one gated
/// behind another flag — every existing `supersession_edge` row (manual
/// supersede + CS-050 auto-detection) now affects ranking, the
/// `[SUPERSEDED]` annotation, and force-included citations. An operator who
/// needs to roll it back can still set `SUPERSESSION_RANKING_ENABLED=off`
/// (or `false`/`no`/`0`) explicitly. While OFF, [`multi_graph_retrieve`]
/// still computes what the penalty/annotation/honesty-guard outcome WOULD
/// have been and logs it (shadow mode, same discipline as
/// `ask_relevance_floor`'s own rollout and the Phase 5a
/// `SUPERSESSION_AUTO_EDGES` dry-run gate) — that shadow-logging path is
/// unchanged by this flip, it now simply runs in the OFF-override case
/// rather than the common case.
pub(crate) fn supersession_ranking_enabled() -> bool {
    match std::env::var("SUPERSESSION_RANKING_ENABLED") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        Err(_) => true,
    }
}

// ── §7.4 retrieval trace ─────────────────────────────────────────────────────
//
// `multi_graph_retrieve` now has roughly six places an atom can be excluded
// (the vsim gate, the blended floor, the per-ocean floor, the context cap,
// the reef coverage floor, and the supersession penalty) — plus traversal
// additions still to come — and a miss was previously indistinguishable
// across all of them. This is the fix: a per-query trace of per-gate counts,
// always computed (no debug flag — it's roughly a dozen integers, no
// per-atom allocation), attached to every `MultiRetrieval` as `trace` so it
// is reachable programmatically (the "debug event" half of this — a test or
// the §7.1 eval harness can assert on it directly, not scrape a log string),
// and also emitted as a structured `tracing` log line via
// [`log_retrieval_trace`] (the "structured log line" half) for production
// reads. Both come from the SAME value, so they can never disagree.
//
// This serves the eval harness as much as production: when a §7.1 fixture
// case asserts `must_retrieve` and the atom is absent, the trace names WHICH
// gate dropped it instead of leaving the failure to be bisected by hand.
//
// Some gates in this trace are not implemented on `main` yet and land in
// other PRs. Those fields are `Option<_>` and left `None` — never a
// fabricated `0`, which would be indistinguishable from "this gate ran and
// excluded nothing":
//   - `reef_matched` — §5.6 coverage qualification (PR #424, not on `main`).
//
// `vsim_gate` (§3.11 absolute vsim gate) is no longer in that reserved list
// — it lands in the SAME PR that wires this field, so it is `Some(_)`
// unconditionally from here on: unlike `supersession_pen`/`traversal_added`
// below, there is no feature flag gating whether it RUNS (§3.11 decided the
// mechanism ships for real; see `ask_vsim_gate`'s doc comment for the
// threshold's provenance and the one open question it leaves — the exact
// VALUE, which is env-tunable via `ASK_VSIM_GATE` without code changes).
//
// `traversal_added` (§5.4 currents traversal) is ALSO no longer reserved —
// it lands in the SAME PR that wires this field, so it is `Some(_)`
// unconditionally from here on (the gate now exists), but see
// [`TraversalTrace`]'s doc comment for why its CONTENTS while
// `GRAPH_TRAVERSAL_ENABLED` is off are real zeros rather than the
// shadow-computed numbers — it follows `supersession_pen` below, not
// `reef_matched` above.
//
// `supersession_pen` is different in kind: the mechanism EXISTS today,
// gated by [`supersession_ranking_enabled`] (default OFF). While the flag is
// off, `demoted: 0` is a TRUE statement about current behavior (the penalty
// never fires), not an absent gate — so it stays a plain, non-`Option`
// field, with `ranking_enabled` carried alongside so a reader can tell "0
// because the flag is off" apart from "0 because nothing was superseded".

/// One gate's candidate-count transition: how many candidates entered vs.
/// survived.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GateTrace {
    pub before: usize,
    pub after: usize,
}

/// §5.6 reef coverage qualification (PR #424) — reserved, not on `main`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReefMatchedTrace {
    pub matched: usize,
    pub qualified: usize,
}

/// §5.4 currents traversal. Landed in the same PR that wires this field —
/// see the module-level note's `traversal_added` entry for why this is no
/// longer `None`-forever, and the field docs on [`RetrievalTrace::
/// traversal_added`] for the shadow-mode contract this struct now has to
/// honor (mirrors [`SupersessionPenTrace`]'s `demoted`/`ranking_enabled`
/// split exactly, for the identical reason: §5.5 currents traversal is ALSO
/// gated by a default-off flag, and a shadow-computed number reported as
/// though it were applied would make this trace lie).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TraversalTrace {
    /// Atoms currents traversal genuinely ADDED to the real candidate pool.
    /// `0` whenever `enabled` is `false` — matching `SupersessionPenTrace`'s
    /// `demoted`, this is a TRUE statement about applied behavior (§5.5:
    /// while the flag is off, `multi_graph_retrieve` never merges a single
    /// traversal-derived atom into `keyword_hits`), never a shadow-computed
    /// count reported as if it had landed in the pool.
    pub added: usize,
    /// Number of hopped-in reefs that actually CONTRIBUTED an atom counted
    /// in `added`. Also `0` when `enabled` is `false`, for the same reason —
    /// this is not "how many reefs the shadow walk found" (that number is
    /// diagnostic-only and lives in the separate `currents_traversal
    /// (SHADOW...)` log line emitted by `log_currents_traversal_shadow`,
    /// deliberately kept OUT of this always-on trace).
    pub reefs: usize,
    /// Max hop depth actually walked to produce `added` (§5.4: 1 or 2, `0`
    /// when nothing was added or `enabled` is `false`).
    pub hop: u32,
    /// Whether `GRAPH_TRAVERSAL_ENABLED` was on for THIS query — the
    /// disambiguator: `added: 0, enabled: false` means "the flag is off, so
    /// nothing was ever eligible to be added" (the overwhelmingly common
    /// case today); `added: 0, enabled: true` means "the flag is on, and
    /// this specific query's traversal genuinely found nothing to add".
    /// Without this flag those two `0`s would be indistinguishable, exactly
    /// the failure mode [`SupersessionPenTrace::ranking_enabled`] exists to
    /// prevent for the supersession gate.
    pub enabled: bool,
}

/// §3.11 honesty floor. NOT a per-atom filter — it's an all-or-nothing
/// null-retrieval guard (see §3.11's title), so there is no "before -> after
/// count" the way the other gates have; `candidates` is the pool size the
/// guard was evaluated against and `cleared` is its outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlendedFloorTrace {
    pub candidates: usize,
    pub cleared: bool,
}

/// Supersession penalty (Phase 5b). `demoted` counts candidates in the
/// merged pool whose score was actually discounted; see the module-level
/// note above on why `demoted: 0` with `ranking_enabled: false` is a true
/// count, not a missing gate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SupersessionPenTrace {
    pub demoted: usize,
    pub ranking_enabled: bool,
}

/// §7.4 — the full per-query trace. See the module-level note above for
/// which fields are live today vs. reserved for a gate landing elsewhere.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetrievalTrace {
    /// Raw vector-search hits, summed across every in-scope ocean.
    pub vector_hits: usize,
    /// Raw keyword-search hits — FTS (`keyword_search`) plus node/reef-title
    /// matches (`keyword_search_node_atoms`), merged via
    /// [`merge_keyword_hits`] — summed across every in-scope ocean.
    pub keyword_hits: usize,
    /// §5.6 reef coverage qualification — reserved, not on `main`.
    pub reef_matched: Option<ReefMatchedTrace>,
    /// Deduplicated candidate count entering [`rerank`] (vector ∪ keyword) —
    /// the vector side here is already POST `vsim_gate` (below).
    pub merged: usize,
    /// §3.11 absolute vsim gate. `Some(_)` unconditionally (no feature flag
    /// — see `ask_vsim_gate`'s doc comment): `before`/`after` are the raw
    /// `Store::vector_search` hit count immediately before/after dropping
    /// any candidate whose cosine similarity falls below [`ask_vsim_gate`].
    /// Applied ONLY to `vector_hits`, before they ever enter [`rerank`] —
    /// never to `keyword_hits` (a keyword-only match, including a reef-name
    /// hit via `keyword_search_node_atoms`, never had a `vsim` to gate on).
    pub vsim_gate: Option<GateTrace>,
    /// Supersession penalty (Phase 5b) applied to the merged pool.
    pub supersession_pen: SupersessionPenTrace,
    /// The relevance-floor honesty guard, evaluated on the (possibly
    /// supersession-demoted) merged pool.
    pub blended_floor: BlendedFloorTrace,
    /// The ocean-count-scaled synthesis context cap
    /// ([`synthesis_context_cap`]): pool size immediately before/after
    /// `ranked_all.iter().take(cap)`. The per-ocean floor guarantee below
    /// can push the FINAL selected set back past `after` again — this field
    /// captures only the cut itself, not the final size.
    pub context_cap: GateTrace,
    /// Extra atoms pulled back in by the per-ocean floor guarantee (A5) —
    /// only ever nonzero for a multi-ocean query; a single-ocean query has
    /// nothing to add here, which is a true `0`, not an absent gate.
    pub per_ocean_floor_added: usize,
    /// §5.4 currents traversal. `Some(_)` unconditionally now that the gate
    /// is implemented (see the module-level note above) — the `Option` no
    /// longer means "not built"; it is kept `Option` rather than collapsed
    /// to a bare `TraversalTrace` purely for API stability with the other
    /// still-reserved fields above. See [`TraversalTrace`]'s doc comment for
    /// the shadow-mode contract: while `GRAPH_TRAVERSAL_ENABLED` is off,
    /// `added`/`reefs`/`hop` are real zeros (nothing was ever merged into
    /// the pool) and `enabled: false` says why — never the shadow-computed
    /// "what traversal found" numbers, which stay confined to the separate
    /// `currents_traversal (SHADOW...)` log line.
    pub traversal_added: Option<TraversalTrace>,
}

/// Emit the §7.4 [`RetrievalTrace`] as a structured `tracing` log line —
/// modeled on [`log_shadow_floor_disagreement`] above (same
/// request-id/graph-id correlation fields, never the query text itself).
/// Unlike that function this is unconditional: the trace is always on, so
/// this always logs, once per `multi_graph_retrieve` call.
fn log_retrieval_trace(request_id: Option<&str>, graph_ids: &[Uuid], trace: &RetrievalTrace) {
    tracing::info!(
        request_id = request_id.unwrap_or("n/a"),
        graph_ids = ?graph_ids,
        vector_hits = trace.vector_hits,
        keyword_hits = trace.keyword_hits,
        reef_matched = ?trace.reef_matched,
        merged = trace.merged,
        vsim_gate = ?trace.vsim_gate,
        supersession_demoted = trace.supersession_pen.demoted,
        supersession_ranking_enabled = trace.supersession_pen.ranking_enabled,
        blended_floor_candidates = trace.blended_floor.candidates,
        blended_floor_cleared = trace.blended_floor.cleared,
        context_cap_before = trace.context_cap.before,
        context_cap_after = trace.context_cap.after,
        per_ocean_floor_added = trace.per_ocean_floor_added,
        traversal_added = ?trace.traversal_added,
        "retrieval_trace: per-gate candidate counts for this query (§7.4)"
    );
}

/// Whether §5.4/§5.5's currents traversal is turned on for real — read from
/// `GRAPH_TRAVERSAL_ENABLED`, parsed defensively the SAME way
/// [`supersession_ranking_enabled`] is (any of `1`/`true`/`yes`/`on`,
/// case-insensitive, is ON; unset/blank/malformed is OFF). **Defaults OFF.**
///
/// This gates ALL real behavior change, exactly like `SUPERSESSION_RANKING_
/// ENABLED`: while OFF, [`multi_graph_retrieve`] still WALKS the currents
/// graph and computes what the traversed candidate pool would have looked
/// like — on a throwaway clone, per §5.5 — and logs the delta (atoms added,
/// rank positions changed, whether a hopped atom would have been cited), but
/// never merges a single traversal-derived atom into the real candidate
/// pool, never changes `ranked_all`/`selected`, and never touches a
/// citation. Flip only once observed shadow deltas justify the three
/// tuning-table ESTIMATES this feature runs on (2 hops, ×0.4/×0.16 decay,
/// ~5 hopped reefs — none derived, per §10/§11).
///
/// Caveat preserved from §5.5 by design: the shadow log says what CHANGED,
/// not what was BETTER — that judgement stays human. No code path in this
/// module computes or implies an automated verdict on whether traversal
/// improved an answer.
pub(crate) fn graph_traversal_enabled() -> bool {
    match std::env::var("GRAPH_TRAVERSAL_ENABLED") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

/// §5.5 shadow-mode diagnostic emitter for currents traversal — the
/// `GRAPH_TRAVERSAL_ENABLED`-off counterpart to
/// [`log_shadow_floor_disagreement`] and the supersession-penalty shadow
/// block inside [`multi_graph_retrieve`]: "build the traversed candidate
/// pool on a throwaway clone, log the delta ... and apply nothing" (§5.5,
/// verbatim). Only ever called while the flag is OFF — see
/// [`multi_graph_retrieve`]'s call site — because once it's ON the merge
/// already happened for real and there is no "would have" left to report.
///
/// Reports exactly the three deltas §5.5 names:
///   - **atoms added** — traversal-derived atom ids not already reachable via
///     vector/keyword/reef-name search, i.e. genuinely NEW candidates.
///   - **rank positions changed** — among atoms present in BOTH the real
///     `ranked_all` and a shadow rerank with traversal atoms merged in, how
///     many moved to a different rank index.
///   - **would have been cited** — whether ANY traversal-derived atom lands
///     in the shadow pool's top `cap` (the same cap [`MultiRetrieval::reranked`]
///     itself is built from). This is a diagnostic approximation, not a
///     full re-run of the per-ocean-floor-guarantee / force-include passes
///     `selected` goes through — it answers "would this atom have been in
///     the running," not "would it have survived every downstream rule."
///
/// **Caveat preserved from §5.5, verbatim: this reports what CHANGED, not
/// what was BETTER.** No field here is a verdict — there is deliberately no
/// "traversal helped" / "traversal hurt" boolean. That judgement stays
/// human; a founder reading these logs decides whether the delta looks like
/// an improvement before ever flipping the flag.
#[allow(clippy::too_many_arguments)]
fn log_currents_traversal_shadow(
    graph_ids: &[Uuid],
    vector_hits: &[(Uuid, f32)],
    keyword_hits: &[(Uuid, f64)],
    timestamps: &HashMap<Uuid, DateTime<Utc>>,
    node_map: &HashMap<Uuid, Uuid>,
    ranked_all: &[RankedAtom],
    selected_len: usize,
    cap: usize,
    traversal_atoms: &[(Uuid, f64)],
    traversal_by_graph: &HashMap<Uuid, CurrentsTraversal>,
) {
    // ── Throwaway clone (§5.5) — `keyword_hits`/`ranked_all` are read-only here.
    let shadow_keyword_hits = merge_keyword_hits(keyword_hits.to_vec(), traversal_atoms.to_vec());
    let shadow_rerank_k = vector_hits.len() + shadow_keyword_hits.len();
    let shadow_ranked = rerank(
        vector_hits,
        &shadow_keyword_hits,
        timestamps,
        node_map,
        &RerankParams::default(),
        shadow_rerank_k,
    );

    let real_ids: std::collections::HashSet<Uuid> = ranked_all.iter().map(|a| a.atom_id).collect();
    let traversal_ids: std::collections::HashSet<Uuid> =
        traversal_atoms.iter().map(|(id, _)| *id).collect();

    let atoms_added = traversal_ids.difference(&real_ids).count();

    let real_rank: HashMap<Uuid, usize> = ranked_all
        .iter()
        .enumerate()
        .map(|(i, a)| (a.atom_id, i))
        .collect();
    let shadow_rank: HashMap<Uuid, usize> = shadow_ranked
        .iter()
        .enumerate()
        .map(|(i, a)| (a.atom_id, i))
        .collect();
    let mut rank_positions_changed = 0usize;
    for (&id, &real_i) in &real_rank {
        if let Some(&shadow_i) = shadow_rank.get(&id) {
            if shadow_i != real_i {
                rank_positions_changed += 1;
            }
        }
    }

    let would_be_cited = shadow_ranked
        .iter()
        .take(cap.max(selected_len))
        .any(|a| traversal_ids.contains(&a.atom_id));

    let hopped_reef_count: usize = traversal_by_graph
        .values()
        .map(|t| t.hopped_reefs.len())
        .sum();
    let seed_reef_count: usize = traversal_by_graph.values().map(|t| t.seed_count).sum();

    tracing::info!(
        graph_ids = ?graph_ids,
        seed_reef_count,
        hopped_reef_count,
        traversal_candidate_atoms = traversal_atoms.len(),
        atoms_added,
        rank_positions_changed,
        would_be_cited,
        "currents_traversal (SHADOW — GRAPH_TRAVERSAL_ENABLED is off): walked from {} seed reef(s) to {} \
         hopped reef(s), surfacing {} candidate atom(s) ({} genuinely new); no ranking/synthesis/citation \
         change applied. Reports what CHANGED, not what was better — that judgement stays human.",
        seed_reef_count,
        hopped_reef_count,
        traversal_atoms.len(),
        atoms_added,
    );
}

/// Per-ocean-count synthesis-context cap (ADR-0011 A6): grows with the number of
/// in-scope oceans so a cross-ocean answer can draw on each, bounded by
/// [`SYNTHESIS_CONTEXT_CAP_CEILING`]. For a single ocean this is exactly `top_k`,
/// preserving the pre-v2 behavior.
fn synthesis_context_cap(top_k: usize, ocean_count: usize) -> usize {
    let n = ocean_count.max(1);
    let scaled = top_k.saturating_add(n.saturating_sub(1).saturating_mul(top_k / 2));
    // `Ord::clamp(min, max)` panics if `min > max`; `top_k` is caller-controlled
    // (up to 100) and can exceed `SYNTHESIS_CONTEXT_CAP_CEILING`, so the lower
    // bound must itself be capped at the ceiling to keep the clamp well-formed.
    // For `top_k <= SYNTHESIS_CONTEXT_CAP_CEILING` this is exactly `top_k`,
    // preserving the pre-existing behavior.
    let lower = top_k.min(SYNTHESIS_CONTEXT_CAP_CEILING);
    scaled.clamp(lower, SYNTHESIS_CONTEXT_CAP_CEILING)
}

/// Merge BM25/ts_rank keyword hits with reef-derived keyword candidates into a
/// single deduped `(atom_id, score)` list for the reranker.
///
/// The two sources score on different, backend-specific scales (FTS5 bm25 vs
/// ts_rank_cd vs reef token-overlap), so each is first normalised to `[0, 1]`
/// by its own max — putting a reef-name match and a strong BM25 hit on equal
/// footing. Scores are then combined per atom by taking the max. Normalising
/// the keyword side here is behaviour-preserving for the no-reef-hit case: the
/// reranker already divides keyword scores by their max, so a pre-divided list
/// reranks identically.
///
/// # Determinism (graph-temporal-memory hardening)
/// The dedup step folds into a `HashMap`, whose iteration order is randomly
/// seeded per process (Rust's default `RandomState`) — collecting it directly
/// would hand the reranker a keyword-candidate list whose order is
/// process-dependent even for byte-identical input. `rerank`'s final sort only
/// tie-breaks on score + recency (§3.10's `rank_cmp`), so two atoms with equal
/// score, equal recency, and no vector hit (i.e. present only via this
/// function) would otherwise be ordered by nothing more meaningful than that
/// per-process accident — exactly the kind of tied-cohort-at-the-cap
/// nondeterminism this hardening pass targets in the retrieval store. Sorting
/// by `atom_id` here fixes that: same input, same output, on every run.
fn merge_keyword_hits(
    keyword_hits: Vec<(Uuid, f64)>,
    node_atom_hits: Vec<(Uuid, f64)>,
) -> Vec<(Uuid, f64)> {
    use std::collections::HashMap;

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

/// A relationship edge surfaced alongside an answer. Carries the `edge_id` so a
/// conversational client can confirm/reject an inferred edge inline (ADR-0008
/// decision 9), plus the tier fields used to build the synthesis context.
#[derive(Clone)]
pub(crate) struct AnswerRelationship {
    pub(crate) edge_id: Uuid,
    pub(crate) subject: String,
    pub(crate) predicate: String,
    pub(crate) object: String,
    pub(crate) src_node_id: Uuid,
    pub(crate) dst_node_id: Uuid,
    pub(crate) inferred: bool,
    pub(crate) confirmed: bool,
    pub(crate) confidence: f32,
}

/// Gather typed relationship edges touching the query-relevant reefs, resolved to
/// canonical titles, for the synthesis RELATIONSHIPS section (ADR-0008). One hop
/// from the expanded reefs is enough to surface e.g. an inferred
/// `parent_of(Harper, Casey)` edge when the query retrieved the "Casey" reef.
/// Rejected edges are excluded; inferred-suggested edges are kept (hedged in the
/// prompt). Best-effort: any store error yields an empty list.
/// `pool`/`user_id` (added with the OPS-4 unscoped-read fix): `relationship_edge`
/// and `nodes` are both RLS-covered by migration 0083, so on the cloud plane these
/// reads must carry `app.account_id` or they hard-error with 42704 under
/// `coralswarm_app` — and every caller here discards the error, so the failure
/// rendered as "this answer has no relationships" rather than as an error.
/// `pool: None` is the pool-less arm, where there is no RLS to satisfy.
///
/// Each read opens its OWN short scoped transaction rather than sharing one:
/// this runs on the ask path, and the rule at this module's conversation-scope
/// comment is per statement, never held across the surrounding stream.
pub(crate) async fn build_synthesis_relationships(
    store: &dyn Store,
    pool: Option<&sqlx::PgPool>,
    user_id: Uuid,
    graph_id: Uuid,
    nodes: &[NodeWithAtoms],
) -> Vec<AnswerRelationship> {
    if nodes.is_empty() {
        return Vec::new();
    }
    let frontier: Vec<Uuid> = nodes.iter().map(|n| n.node_id).collect();

    // The ocean's owning account, resolved once for both scoped reads below.
    // `graph_account_id` is the deliberately-unscoped 0084 bootstrap resolver —
    // it is how the account is discovered, so it cannot itself be scoped.
    let scope = match pool {
        Some(p) => match store.graph_account_id(graph_id).await {
            Ok(Some(account_id)) => Some((p, account_id)),
            _ => return Vec::new(),
        },
        None => None,
    };

    let edges = match scope {
        Some((p, account_id)) => {
            let Ok(mut tx) =
                crate::tenancy::scope::begin_tenant_scoped(p, account_id, user_id).await
            else {
                return Vec::new();
            };
            let r = store
                .list_relationship_edges_for_nodes_scoped(&mut tx, graph_id, &frontier)
                .await;
            tx.commit().await.ok();
            r
        }
        None => {
            store
                .list_relationship_edges_for_nodes(graph_id, &frontier)
                .await
        }
    };
    let edges = match edges {
        Ok(e) if !e.is_empty() => e,
        _ => return Vec::new(),
    };

    // Resolve endpoint node ids → canonical reef titles. Targeted lookup by
    // the actual edge-endpoint ids (dogfooding gap-sweep #2, item 7,
    // 2026-07-18) rather than a `list_nodes(..., 5000, 0)` scan of the
    // ocean's entire entity-reef population to resolve at most ~80 titles.
    let endpoint_ids: Vec<Uuid> = edges
        .iter()
        .flat_map(|e| [e.src_node_id, e.dst_node_id])
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let titles = match scope {
        Some((p, account_id)) => {
            let Ok(mut tx) =
                crate::tenancy::scope::begin_tenant_scoped(p, account_id, user_id).await
            else {
                return Vec::new();
            };
            let r = store
                .nodes_by_ids_scoped(&mut tx, graph_id, &endpoint_ids)
                .await;
            tx.commit().await.ok();
            r
        }
        None => store.nodes_by_ids(graph_id, &endpoint_ids).await,
    };
    let title_by_id: HashMap<Uuid, String> = match titles {
        Ok(ns) => ns.into_iter().collect(),
        Err(_) => return Vec::new(),
    };

    let mut out: Vec<AnswerRelationship> = Vec::new();
    let mut seen: std::collections::HashSet<(Uuid, String, Uuid)> =
        std::collections::HashSet::new();
    for e in edges {
        if e.status == EdgeStatus::Rejected {
            continue;
        }
        let (Some(subject), Some(object)) = (
            title_by_id.get(&e.src_node_id),
            title_by_id.get(&e.dst_node_id),
        ) else {
            continue;
        };
        if !seen.insert((e.src_node_id, e.predicate.clone(), e.dst_node_id)) {
            continue;
        }
        out.push(AnswerRelationship {
            edge_id: e.edge_id,
            subject: subject.clone(),
            predicate: e.predicate.clone(),
            object: object.clone(),
            src_node_id: e.src_node_id,
            dst_node_id: e.dst_node_id,
            inferred: e.origin == EdgeOrigin::Inferred,
            confirmed: e.status == EdgeStatus::Confirmed,
            confidence: e.confidence,
        });
        if out.len() >= SYNTHESIS_RELATIONSHIP_CAP {
            break;
        }
    }
    out
}

use crate::{
    billing::enforcement,
    middleware::auth::UserContext,
    relationships::{EdgeOrigin, EdgeStatus},
    retrieval::{
        rerank::{
            apply_supersession_penalty, rerank, RankedAtom, RerankParams,
            SUPERSESSION_PENALTY_MULTIPLIER,
        },
        traversal::{
            cap_frontier, cap_hopped_reefs, flatten_hopped_atoms, hop_scores, query_looks_kinship,
            HopPolicy, TraversalWalk, WalkReef, WalkedCurrent, ASK_CO_OCCUR_MIN_SHARED,
            MAX_HIT_SEED_REEFS, MAX_HOPS,
        },
        RetrievalFilter,
    },
    store::{NodeWithAtoms, Store, StoreError, SupersessionTarget},
    synthesis::{SupersededClaim, SynthesisClaim},
    tenancy::extractor::Tenant,
    AppState,
};

// ── Helpers ───────────────────────────────────────────────────────────────────
// These handlers return `impl IntoResponse`, so the 503 arm uses `.into_response()`.
// Called as `require_store!(tenant)` everywhere in this file (org-tenancy PR
// 1.4): `tenant.store` is the `Tenant` extractor's resolved content store
// (`queries`/`conversations`/etc. are tenant-tree tables), never ambient
// `state.store`.

macro_rules! require_store {
    ($state:expr) => {
        match &$state.store {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable: no database"})),
                )
                    .into_response()
            }
        }
    };
}

/// Shared 402 response for `create_query`/`create_query_stream` (real-
/// pricing-and-limits PR) — same `{"error", "request_id"}` envelope every
/// other error arm in this file already uses, so a caller parsing one error
/// shape from this endpoint doesn't need a special case for this one.
/// `PAYMENT_REQUIRED` (RFC 7231 §6.5.2, HTTP 402) is the closest standard
/// status for "this account has run out of credits" — distinct from 429
/// (rate limit: a transient, retry-shortly signal) and 403 (forbidden: an
/// authorization signal, not a billing one).
/// The HTTP form of an [`enforcement::InteractiveRefusal`]. An unreadable
/// balance is a retryable 503, never a 402 that tells a funded user they are
/// out of credits.
fn interactive_refusal_response(refusal: enforcement::InteractiveRefusal, request_id: &str) -> Response {
    match refusal {
        enforcement::InteractiveRefusal::OutOfCredits(reason) => out_of_credits_response(request_id, reason),
        enforcement::InteractiveRefusal::EligibilityUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "credit eligibility is temporarily unavailable",
                "reason": "credit_eligibility_unavailable",
                "resolution": "retry shortly",
                "request_id": request_id,
            })),
        )
            .into_response(),
    }
}

fn out_of_credits_response(
    request_id: &str,
    refusal: crate::billing::funding_policy::RefusalReason,
) -> Response {
    (StatusCode::PAYMENT_REQUIRED, Json(out_of_credits_body(request_id, refusal))).into_response()
}

/// The 402 body. `error`, `reason`, `resolution` and `request_id` are the
/// pre-extra-usage fields and keep their values; `refusal_reason` is new and
/// carries the funding policy's stable code (`free_plan`, `credits_frozen`,
/// `extra_usage_off`, `member_cap_reached`, `monthly_limit_reached`,
/// `extra_balance_exhausted`) so a client can pick per-reason paywall copy.
fn out_of_credits_body(
    request_id: &str,
    refusal: crate::billing::funding_policy::RefusalReason,
) -> serde_json::Value {
    json!({
        "error": "out of credits: this account's credit balance is at or below zero",
        "reason": "insufficient_credits",
        "refusal_reason": refusal.code(),
        "resolution": "purchase a plan or top-up credits to continue asking/searching",
        "request_id": request_id,
    })
}

// ── Request / response types ──────────────────────────────────────────────────

/// Query parameters accepted by `GET /v1/queries`.
#[derive(Deserialize)]
pub struct ListQueriesParams {
    /// Optional: restrict results to a single graph.
    pub graph_id: Option<Uuid>,
    /// 1-based page number (default: 1).
    pub page: Option<i64>,
    /// Records per page, capped at 50 (default: 20).
    pub page_size: Option<i64>,
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct CreateQueryRequest {
    pub text: String,
    #[schema(value_type = Option<Object>)]
    pub temporal: Option<crate::temporal::TemporalSelection>,
    #[schema(value_type = Option<Object>)]
    pub library_scope: Option<crate::library_read::LibraryScope>,
    pub conversation_id: Option<Uuid>,
    pub scope_action: Option<String>,
    pub style: Option<String>,
    pub model: Option<String>,
    pub candidate_limit: Option<i64>,
    pub recap_after: Option<String>,
    /// Re-answer this saved question (the thread's latest unanswered or failed one) instead of asking anew.
    pub retry_message_id: Option<Uuid>,
    pub graph_id: Option<Uuid>,
    /// Restrict retrieval to atoms with this scope value (e.g. `"private"` or `"account"`).
    pub scope: Option<String>,
    /// Restrict retrieval to atoms with this source type (e.g. `"slack"`, `"email"`).
    pub source_type: Option<String>,
    /// Restrict retrieval to atoms with ANY of these source types (GitHub
    /// connector eval, `docs/connectors/github.md` §5 — e.g.
    /// `["github_code", "github_code_summary"]` for both code tiers at once).
    /// When present and non-empty it wins over `source_type`; absent or
    /// empty, `source_type` applies unchanged. See
    /// `RetrievalFilter::source_type_list` for the precedence rule.
    #[serde(default)]
    pub source_types: Option<Vec<String>>,
    pub top_k: Option<usize>,
}

// ── Fire-and-forget query-history recorder (org-tenancy B.1) ─────────────────

// ── Conversation-surface RLS scope (org-tenancy OPS-4 phase B.6) ─────────────
//
// `create_query_stream` touches four RLS-covered consumer tables (migration
// `0083_consumer_rls`): `conversations`, `conversation_messages`,
// `conversation_oceans`, `conversation_surfaced_edges`. Every one of those
// calls used to run on the unscoped `&self.pool` path, so under the
// post-OPS-4 `coralswarm_app` role each of them hard-errors with
// `unrecognized configuration parameter "app.account_id"` — the whole chat
// stream, every turn.
//
// **Shape: per statement, never per stream.** This handler interleaves DB
// work with query condensation, embedding, retrieval fan-out, contradiction
// detection and a full token-streaming synthesis round trip. Holding ONE
// `begin_account_scoped` transaction across that would pin a pooled
// connection for the entire life of an LLM stream — exactly the pathology
// PR #291's per-statement `JobScope` rationale rules out, and the same
// reason `services::ask` and `services::transcripts` (PR #300) scope per
// statement. So each helper below opens a short scoped transaction around
// its ONE Store call and closes it before any network wait. No statement
// here depends on another's uncommitted state, so nothing is lost.
//
// **Which account.** Two different policy anchors are in play:
//
// * `conversations` carries `account_id` DIRECTLY, and
//   `conversation_messages` / `conversation_surfaced_edges` hop one step to
//   it. So all three scope to the CONVERSATION ROW'S OWN `account_id` —
//   which is `graph_account_id(primary ocean at creation time)`, stamped by
//   `create_conversation` below, NOT the caller's active account. That is
//   the same graph-anchored choice PRs #295/#300 made for reachability-gated
//   handlers, and it is load-bearing here: reachability is deliberately
//   multi-account (ADR-0007 Model B), so a legitimate reader of a team ocean
//   owned by another account would be filtered to "conversation not found"
//   if this scoped by `user_ctx.account_id`.
// * `conversation_oceans` anchors on `graph_id → graphs.account_id`
//   instead, and this turn's `graph_ids[]` is the D9 read union — genuinely
//   multi-account. It therefore scopes PER OCEAN, one short transaction per
//   distinct owning account. See
//   [`scoped_replace_conversation_oceans`].
//
// **Bootstrapping the account for an EXISTING conversation** is the one
// chicken-and-egg: reading `conversations` needs a scope, and the scope is a
// column of the row being read. There is deliberately no conversation→account
// SECURITY DEFINER resolver (see `routes::conversations`' module doc).
// [`scoped_get_conversation_bootstrap`] resolves it by probing this turn's
// primary-ocean account first and, only on a miss, every account the caller
// belongs to — see that function's doc for why that set is exactly right and
// no wider than ADR-0007 Model B's own read union.
//
// **Nothing here swallows a scoping failure.** Each helper returns the same
// `Result`/`Option` its unscoped twin did, and every call site keeps the
// exact error handling it already had (hard `error` SSE frame for the
// load-bearing reads/writes, `warn!` for the ones that were already
// best-effort before this change).
//
// With no pool (`tenant.pool == None`) `scope` is `None` and every
// helper falls back to the plain unscoped Store method. The ORG plane USED to also fall back to `None` here on the stale
// reasoning that "tenant databases get whole-database isolation and carry no
// policies" — no longer true since `migrations_tenant/0008_tenant_rls`, which
// covers every one of these conversation-surface tables. Tenant RLS Phase 2
// threads the acting user through instead (see `ConvScope`'s doc), so an Org
// decision now scopes exactly like Personal.

// `scoped_get_graph` lived here. Its only caller read `is_primary` to derive a
// new conversation's mode-locked-for-life `private`/`shared` value; with the
// private scope removed, every conversation is "shared" and there is nothing
// left to read. `Store::get_graph{,_scoped}` are unaffected — other modules
// still use them directly.

// ── Handlers ──────────────────────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/queries",
    tag = "queries",
    security(("bearer_token" = [])),
    params(
        ("graph_id"  = Option<Uuid>, Query, description = "Filter by graph ID"),
        ("page"      = Option<i64>,  Query, description = "Page number (default 1)"),
        ("page_size" = Option<i64>,  Query, description = "Records per page, max 50 (default 20)"),
    ),
    responses(
        (status = 200, description = "Paginated query history", body = Value),
        (status = 401, description = "Unauthorized"),
        (status = 503, description = "Database unavailable", body = Value),
    )
)]
pub async fn list_queries(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Query(params): Query<ListQueriesParams>,
) -> impl IntoResponse {
    // ── Require the store (org-tenancy PR 1.4: resolved by the `Tenant`
    // extractor — `query_history` is a tenant-tree table) ────────────────────
    let store = require_store!(tenant);

    // ── Pagination params ─────────────────────────────────────────────────────
    let page_size = params.page_size.unwrap_or(20).clamp(1, 50);
    let page = params.page.unwrap_or(1).max(1);
    let offset = (page - 1) * page_size;

    // ── Run the query (scoped to caller's account) ───────────────────────────
    // RLS scoping (org-tenancy B.1): `query_history` is RLS-covered (migration
    // 0083), so on any Postgres plane the read runs inside a
    // `begin_account_scoped` transaction; a pool-less `Tenant` keeps the plain
    // Store path.
    // `user_ctx.user_id` (Group-B security fix, PR 0 of the member-private-
    // oceans plan, item 6): query history is a record of what a SPECIFIC
    // person asked and the answer they were shown. Before this call carried
    // the caller's own `user_id`, this route returned every row for the whole
    // account — any org/account member could read every other member's
    // questions and answer summaries. This is a founder decision (per-user
    // everywhere), not merely a leak fix: it narrows the route's own contract,
    // not just this one call site, and MUST be called out in the PR body as a
    // deliberate behavior change (org members stop seeing each other's query
    // history).
    let rows = {
        match tenant.begin_member_scoped(user_ctx.account_id).await {
            Ok(mut tx) => {
                if crate::tenancy::scope::set_tenant_user(&mut tx,user_ctx.user_id).await.is_err() {
                    return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))).into_response();
                }
                let rows = store
                    .list_query_history_scoped(
                        &mut tx,
                        user_ctx.account_id,
                        user_ctx.user_id,
                        params.graph_id,
                        page_size,
                        offset,
                    )
                    .await;
                match rows {
                    Ok(rows) => tx.commit().await.map_err(StoreError::from).map(|()| rows),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    };

    match rows {
        Ok(mut records) => {
            if tenant.pool.is_some() && !records.is_empty() {
                let graphs:HashSet<Uuid>=records.iter().map(|r|r.graph_id).collect();
                if graphs.len()>32 {return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"narrow query history to one graph"}))).into_response();}
                for graph in graphs {
                    if !crate::source_evidence::still_admitted(&state,&tenant,&user_ctx,graph,user_ctx.account_id).await {
                        return (StatusCode::NOT_FOUND,Json(json!({"error":"query history not found or access denied"}))).into_response();
                    }
                }
                // Exact support is evaluated again after the fresh control
                // membership/org check, so a prior read is not an attestation.
                let mut tx=match tenant.begin_member_scoped(user_ctx.account_id).await {Ok(tx)=>tx,Err(_)=>return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))).into_response()};
                if crate::tenancy::scope::set_tenant_user(&mut tx,user_ctx.user_id).await.is_err() {return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))).into_response();}
                records=match store.list_query_history_scoped(&mut tx,user_ctx.account_id,user_ctx.user_id,params.graph_id,page_size,offset).await {Ok(rows)=>rows,Err(_)=>return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))).into_response()};
                if tx.commit().await.is_err() {return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"}))).into_response();}
            }
            let items: Vec<Value> = records
                .into_iter()
                .map(|r| {
                    json!({
                        "id":             r.id,
                        "graph_id":       r.graph_id,
                        "user_id":        r.user_id,
                        "query_text":     r.query_text,
                        "answer_status": if r.answer_summary.is_empty() { "unavailable" } else { "supported" },
                        "answer_summary": r.answer_summary,
                        "citation_count": r.citation_count,
                        "input_tokens":   r.input_tokens,
                        "output_tokens":  r.output_tokens,
                        "created_at":     r.created_at,
                    })
                })
                .collect();

            (
                StatusCode::OK,
                Json(json!({
                    "items":     items,
                    "page":      page,
                    "page_size": page_size,
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "list_queries: store error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

/// `llm_usage.task` for a query-time cloud embedding (COR-89).
pub(crate) const QUERY_EMBED_TASK: &str = "query_embed";

#[utoipa_path(
    post,
    path = "/v1/queries",
    tag = "queries",
    security(("bearer_token" = [])),
    request_body = CreateQueryRequest,
    responses(
        (status = 200, description = "Query answer with citations"),
        (status = 422, description = "Validation error", body = Value),
        (status = 429, description = "Rate limit exceeded", body = Value),
        (status = 503, description = "Service unavailable", body = Value),
    )
)]
/// RLS-scoping status (org-tenancy B.1 — FLAGGED PARTIAL CONVERSION): this
/// handler's `query_history` writes run scoped (see [`spawn_query_history`])
/// and its bootstrap/authorization reads (`graph_account_id`,
/// `graph_reachable_by_user`) are the PR #284 SECURITY DEFINER carve-outs.
/// The retrieval/synthesis core is NOT yet converted, deliberately:
/// * its hot-path Store methods (`vector_search`, `keyword_search`,
///   `atom_content_refs`, `atom_channels`, `atom_authors`, `get_node_digest`
///   fan-outs) have no `_scoped` twins yet (not covered by the B.0 chunks
///   #285/#289/#290 — a follow-up Store chunk, likely in
///   `ocean_stats_scoped`'s self-scoping shape since several run genuinely
///   concurrently via `tokio::join!`/`join_all` and cannot share one `&mut
///   Transaction` without serializing a latency-critical path);
/// * holding one request transaction across the multi-second LLM synthesis
///   call would pin a pooled connection for the whole call — the follow-up
///   conversion needs per-phase scoped transactions, not this PR's
///   one-tx-per-request pattern.
///
/// Until then those reads run unscoped and hard-error (never leak) under the
/// post-OPS-4 strict predicate. Tracked in the B.1 PR body.
pub async fn create_query(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Extension(request_id_ext): Extension<String>,
    Json(req): Json<CreateQueryRequest>,
) -> impl IntoResponse {
    let request_id = request_id_ext;

    tracing::info!(
        request_id = %request_id,
        user_id = %user_ctx.user_id,
        "create_query: start"
    );

    // ── 0. Billing gate (real-pricing-and-limits PR) ──────────────────────────
    // Checked FIRST, before any store/DB/embedding work below, so an
    // out-of-credits account is rejected as cheaply as possible — mirrors the
    // rate-limit check's own "reject before spending anything" placement
    // further down. `state.control_db()` (never ambient `state.db` — see that
    // accessor's own doc comment) is the CONTROL-plane pool `credit_balances`
    // lives on; `None` (no control DB configured) has nothing to gate
    // against. A failed eligibility read refuses
    // (COR-108) — see `enforcement::interactive_admission`, the gate MCP
    // `ask_ocean`/`search_atoms` share.
    if let Err(refusal) = enforcement::interactive_admission(state.control_db(), user_ctx.account_id, user_ctx.user_id).await {
        tracing::info!(
            request_id = %request_id,
            user_id = %user_ctx.user_id,
            account_id = %user_ctx.account_id,
            ?refusal,
            "create_query: refused by the interactive credit gate"
        );
        return interactive_refusal_response(refusal, &request_id);
    }

    // ── 1. Hand off to the scoped ask path ────────────────────────────────────
    // Legacy source filters are rejected; cloud source filtering goes through
    // `library_scope`. `scoped_ask::execute` owns retrieval and synthesis.
    if req.scope.is_some() || req.source_type.is_some() || req.source_types.as_ref().is_some_and(|v| !v.is_empty()) {
        return (StatusCode::BAD_REQUEST,Json(json!({"error":"use library_scope for cloud source filters","request_id":request_id}))).into_response();
    }
    let question=req.text.clone();
    let ask=crate::scoped_ask::AskRequest{text:req.text,temporal:req.temporal,library_scope:req.library_scope,conversation_id:req.conversation_id,
        scope_action:req.scope_action,style:req.style,model:req.model,candidate_limit:req.candidate_limit.or(req.top_k.map(|n| n as i64)),recap_after:req.recap_after,admitted_recap_window:None,retry_message_id:req.retry_message_id,request_id:Some(request_id.to_string()),
        #[cfg(feature="test-support")] recap_clock:None, #[cfg(feature="test-support")] fault:None,
        legacy_graph_ids:req.graph_id.into_iter().collect()};
    return match crate::scoped_ask::execute(&state,&tenant,&user_ctx,ask).await {
        Ok(mut answer)=>{answer.payload["request_id"]=json!(request_id);crate::services::find_results::attach_to_answer(&tenant,&user_ctx,&question,&mut answer.payload).await;Json(answer.payload).into_response()},Err(response)=>response,
    };
}

// ── Streaming conversational query (cs-ask-chat-a) ──────────────────────────────

/// Request body for `POST /v1/queries/stream`. Like [`CreateQueryRequest`] but
/// carries an optional `conversation_id`: when present the turn is appended to
/// that conversation (caller must own it) and prior turns give the LLM memory;
/// when absent a new conversation is created lazily and its id is returned in the
/// `meta` SSE event.
#[derive(Deserialize)]
pub struct CreateQueryStreamRequest {
    pub text: String,
    pub temporal: Option<crate::temporal::TemporalSelection>,
    pub library_scope: Option<crate::library_read::LibraryScope>,
    pub scope_action: Option<String>,
    pub candidate_limit: Option<i64>,
    pub recap_after: Option<String>,
    /// Re-answer this saved question instead of asking anew.
    pub retry_message_id: Option<Uuid>,
    /// Legacy single-ocean scope. Still accepted; coerced into a one-element
    /// `graph_ids` for back-compat (ADR-0011 story 1).
    pub graph_id: Option<Uuid>,
    /// Ask v2 multi-ocean scope: query one or several server-custodied oceans at
    /// once (ADR-0011 A1/A4). When both are present, `graph_ids` wins and any
    /// `graph_id` is folded in.
    #[serde(default)]
    pub graph_ids: Option<Vec<Uuid>>,
    pub conversation_id: Option<Uuid>,
    pub scope: Option<String>,
    pub source_type: Option<String>,
    /// Multi-valued `source_type` (GitHub connector eval §5): restrict
    /// retrieval to atoms whose `source_type` is ANY of these. Present and
    /// non-empty wins over `source_type`; absent or empty defers to it. Same
    /// field, same precedence as [`CreateQueryRequest::source_types`].
    #[serde(default)]
    pub source_types: Option<Vec<String>>,
    pub top_k: Option<usize>,
    /// Ask v2 model picker (ADR-0011 D15/D16). Capability label `fast`/`smart`/`max`
    /// (or an exact allowlisted id). Validated server-side; governs answer prose
    /// only. Unknown values fall back to the instance default.
    pub model: Option<String>,
    /// Answer style (ADR-0011 F23): `concise` | `detailed` | `sources-only`.
    /// `sources-only` returns ranked sources without synthesizing prose.
    pub style: Option<String>,
    /// Retrieval depth (ADR-0011 F23): `fast` | `balanced` | `thorough`. Maps to
    /// `top_k` + synthesis-context size.
    pub depth: Option<String>,
    /// Ask v2 conversation mode (ADR-0011 C11/C14). Accepted values: `private`
    /// | `shared`. `private` is kept only for older clients and is folded to
    /// `"shared"` (see [`normalize_mode`]); unset and unknown values also store
    /// `"shared"`, so this field has exactly one persistable value today.
    ///
    /// Recorded once on a NEW conversation as a HISTORICAL LABEL — it gates no
    /// behaviour anywhere, and is ignored for existing conversations. It used
    /// to be derived from whether the primary ocean was the user's private
    /// Primary; that derivation is gone along with the mutually-exclusive
    /// private ask scope.
    pub mode: Option<String>,
    /// Reasoning-effort level for the answer prose. Two-model-world amendment
    /// (2026-09-03): `off` | `low` | `medium` | `high` (the legacy
    /// Anthropic-only `xhigh`/`max` rungs are still accepted for
    /// backward-compatibility and collapse to `high`). Validated server-side,
    /// TIER-aware, by [`crate::llm::model_config::resolve_effort`] — never
    /// forwarded raw, and never the literal string `"off"` (Anthropic has no
    /// such value; it resolves to `None` instead):
    ///   * **fast** tier: always resolves to `None` regardless of this field
    ///     (no reasoning dial).
    ///   * **max** tier: always resolves to `Some("high")` regardless of this
    ///     field (dial hidden client-side, pinned).
    ///   * **smart** tier: this field, normalized as above; blank/unset
    ///     resolves to `Some("medium")` (the default rung); `off` resolves to
    ///     `None`, identically to fast.
    ///
    /// Independently of tier, also resolves to `None` whenever the resolved
    /// answer-prose model id is Haiku-class (`output_config.effort` 400s on
    /// that pairing regardless of tier) — see `resolve_effort`'s doc comment.
    ///
    /// Governs the answer prose only; auxiliary passes never receive it.
    pub effort: Option<String>,
}

/// Resolve the effective in-scope ocean set from the (legacy `graph_id`, new
/// `graph_ids[]`) pair: union, de-duplicated, order-preserving. The first element
/// is treated as the conversation's "primary" ocean (account, history, actions).
fn resolve_graph_ids(graph_id: Option<Uuid>, graph_ids: &Option<Vec<Uuid>>) -> Vec<Uuid> {
    let mut out: Vec<Uuid> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for &g in graph_ids.iter().flatten() {
        if seen.insert(g) {
            out.push(g);
        }
    }
    if let Some(g) = graph_id {
        if seen.insert(g) {
            out.push(g);
        }
    }
    out
}

/// `POST /v1/queries/stream` — the conversational answer path.
///
/// Validates + authorizes like [`create_query`] and saves the user turn before
/// the stream opens, so every refusal up to that point is the same HTTP JSON
/// response `POST /v1/queries` gives. The stream then sends a `progress` event
/// per answer stage, `meta`, and the buffered answer once in `final`, or one
/// `error` carrying the HTTP status and body `POST /v1/queries` would return.
/// See `scoped_ask::progress`.
///
/// RLS-scoping status (org-tenancy OPS-4): the CONVERSATION surface
/// (`conversations`, `conversation_messages`, `conversation_oceans`,
/// `conversation_surfaced_edges`) is converted — see the
/// "Conversation-surface RLS scope" block above [`ConvScope`] for the shape
/// (per statement, never one transaction across the stream) and the
/// scope-account choice. `query_history` writes run scoped through
/// [`spawn_query_history`]; the retrieval core (`multi_graph_retrieve`,
/// `fetch_atom_content`, `citation_details`) scopes per ocean/per statement
/// as of PR #309; and the bootstrap/authorization reads
/// (`Store::graph_account_id`, `ReachabilityResolver`) are the PR #284
/// SECURITY DEFINER carve-outs.
///
/// Still UNSCOPED on this path, each best-effort and each owned by the
/// retrieval-core lane rather than this one — a hard error degrades display
/// metadata or an action card, never the answer, and never leaks:
/// `resolve_ocean_names` (ocean display names — the remainder
/// `services::ask`'s module doc already records), `build_synthesis_reefs` /
/// `build_synthesis_relationships`, `gather_actions`, and
/// `save_correction_note`'s ingest path.
#[utoipa_path(
    post,
    path = "/v1/queries/stream",
    tag = "queries",
    security(("bearer_token" = [])),
    request_body(content = Value, description = "The `POST /v1/queries` body, plus an optional `conversation_id` and `retry_message_id`. `effort`, `depth`, `scope`, `source_type` and `source_types` are refused."),
    responses(
        (status = 200, content_type = "text/event-stream", description = "SSE. `progress` per answer stage: `{\"stage\":\"searching\"|\"retrieved\"|\"writing\"|\"checking\",\"attempt\":n}`, `retrieved` with `sources`; then `meta` `{conversation_id,new_conversation,request_id}` and one `final` (the `POST /v1/queries` answer), or one `error` `{status,error,code,retryable,failed_message_id,...}`. See docs/implementation/scoped-stream-progress-api.md."),
        (status = 400, description = "Invalid question, style, scope or refused control", body = Value),
        (status = 402, description = "Out of credits", body = Value),
        (status = 404, description = "Scope or conversation not found or not readable", body = Value),
        (status = 409, description = "Conversation or scope changed; reload", body = Value),
        (status = 429, description = "Rate limit exceeded", body = Value),
        (status = 503, description = "Service unavailable", body = Value),
    )
)]
pub async fn create_query_stream(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Extension(request_id_ext): Extension<String>,
    Json(req): Json<CreateQueryStreamRequest>,
) -> Response {
    let request_id = request_id_ext;

    // ── 0. Billing gate (real-pricing-and-limits PR) ──────────────────────────
    // Same check, same placement rationale, as `create_query`'s identical
    // block — see that handler's comment. Checked before `require_store!`
    // below so an out-of-credits account never even resolves the store.
    if let Err(refusal) = enforcement::interactive_admission(state.control_db(), user_ctx.account_id, user_ctx.user_id).await {
        tracing::info!(
            request_id = %request_id,
            user_id = %user_ctx.user_id,
            account_id = %user_ctx.account_id,
            ?refusal,
            "create_query_stream: refused by the interactive credit gate"
        );
        return interactive_refusal_response(refusal, &request_id);
    }

    if req.effort.is_some() || req.depth.is_some() {
        return (StatusCode::BAD_REQUEST,Json(json!({"error":"cloud Library Ask does not accept depth or effort controls"}))).into_response();
    }

    if req.scope.is_some() || req.source_type.is_some() || req.source_types.as_ref().is_some_and(|v| !v.is_empty()) {
        return (StatusCode::BAD_REQUEST,Json(json!({"error":"use library_scope for cloud source filters","request_id":request_id}))).into_response();
    }
    let ask=crate::scoped_ask::AskRequest{text:req.text,temporal:req.temporal,library_scope:req.library_scope,conversation_id:req.conversation_id,
        scope_action:req.scope_action,style:req.style,model:req.model,candidate_limit:req.candidate_limit.or(req.top_k.map(|n| n as i64)),recap_after:req.recap_after,admitted_recap_window:None,retry_message_id:req.retry_message_id,request_id:Some(request_id.to_string()),
        #[cfg(feature="test-support")] recap_clock:None, #[cfg(feature="test-support")] fault:None,
        legacy_graph_ids:resolve_graph_ids(req.graph_id,&req.graph_ids)};
    let prepared=match crate::scoped_ask::prepare(&state,&tenant,&user_ctx,ask).await {Ok(prepared)=>prepared,Err(response)=>return response};
    let frames=crate::scoped_ask::progress::frames(state,tenant,user_ctx,prepared,request_id);
    axum::response::Sse::new(frames.map(|frame| Ok::<_,std::convert::Infallible>(frame.into_event()))).into_response()
}

// ── Inline action cards (cs-ask-chat-a P2) ──────────────────────────────────

// ── Temporal intake intent (cs-ask-chat-a) ──────────────────────────────────

// ── Event-time-scope intent ─────────────────────────────────────────────────
//
// Deliberately a SEPARATE classifier/axis from `intake_window` above.
// `intake_window` answers "what was ingested?" (`atoms.created_at`) with a
// canned counts+highlights summary and never reaches the LLM. This one
// answers "what happened?" (`atoms.timestamp`, the event/occurred time) and
// is wired into the SAME synthesis pipeline as any other retrieval — it only
// changes how candidates are gathered (see
// `apply_event_window_merge`), not how the answer is written.
// Collapsing the two into one classifier would silently reinterpret "what's
// new" as an event-time question (or vice versa) for either's phrase set.

/// Detect event-recap phrasing — "what happened today", "summarize this
/// week", "recap yesterday", "what happened on Monday" — and resolve the
/// referenced period into an explicit `[since, until]` UTC range anchored to
/// `Utc::now()`. Returns `None` for anything else, including plain "what
/// happened" with no resolvable time reference (that's ordinary semantic
/// retrieval, not a scoped time window) and event verbs used topically
/// ("summarize the launch decision" has no time reference to resolve).
///
/// A cheap deterministic pre-pass (string match + `chrono` arithmetic), not
/// an LLM call — the phrase set is narrow enough that flexibility isn't the
/// bottleneck, and this must stay near-zero latency since it runs on every
/// question.
///
/// This is Tier 1 of the tiered hybrid classifier (see the "Event-time-scope:
/// tiered hybrid classifier" module docs below, near `has_date_token`). Both
/// production call sites now go through [`classify_event_window`] (which
/// calls `event_time_window_at` — this function's clock-injectable twin —
/// directly), so this `Utc::now()`-anchored convenience wrapper has no
/// production caller left; kept `#[allow(dead_code)]` as the direct,
/// non-tiered entry point the unit tests below exercise.
#[allow(dead_code)]
pub(crate) fn event_time_window(text: &str, tz: Tz) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    event_time_window_at(text, Utc::now(), tz)
}

/// Verbs/phrases that mark a question as asking for an event recap rather
/// than a topical lookup. Required alongside a resolvable time reference
/// below — "summarize" alone is far too broad ("summarize the launch
/// decision" must stay topical retrieval).
const EVENT_RECAP_PHRASES: &[&str] = &[
    "what happened",
    "what's happened",
    "whats happened",
    "what went on",
    "summarize",
    "summarise",
    "recap",
    "catch me up",
    "fill me in",
    "give me a rundown",
    "give me a recap",
];

/// [`event_time_window`] with an injected clock, for deterministic testing.
///
/// `tz` is the ASKING USER's timezone — every period below resolves to that
/// zone's calendar boundaries, not UTC's. See [`tz_midnight`] for why.
pub(crate) fn event_time_window_at(
    text: &str,
    now: DateTime<Utc>,
    tz: Tz,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let t = text.to_lowercase();

    if !EVENT_RECAP_PHRASES.iter().any(|p| t.contains(p)) {
        return None;
    }

    const WEEKDAYS: &[(&str, chrono::Weekday)] = &[
        ("monday", chrono::Weekday::Mon),
        ("tuesday", chrono::Weekday::Tue),
        ("wednesday", chrono::Weekday::Wed),
        ("thursday", chrono::Weekday::Thu),
        ("friday", chrono::Weekday::Fri),
        ("saturday", chrono::Weekday::Sat),
        ("sunday", chrono::Weekday::Sun),
    ];
    for (name, wd) in WEEKDAYS {
        if t.contains(&format!("on {name}")) {
            return Some(weekday_range(now, tz, *wd));
        }
    }

    // Both days named ("what happened today and yesterday") is a TWO-day span.
    // Checked before the single-day arms because they return on first match, so
    // the bare `today` arm below used to swallow this case and answer about
    // today alone — silently dropping yesterday even on the deterministic path.
    if t.contains("today") && t.contains("yesterday") {
        let (since, _) = day_range(now, tz, 1);
        let (_, until) = day_range(now, tz, 0);
        return Some((since, until));
    }
    if t.contains("today") {
        return Some(day_range(now, tz, 0));
    }
    if t.contains("yesterday") {
        return Some(day_range(now, tz, 1));
    }
    if t.contains("this week") {
        return Some(week_range(now, tz, 0));
    }
    if t.contains("last week") {
        return Some(week_range(now, tz, 1));
    }
    if t.contains("this month") {
        return Some(month_range(now, tz, 0));
    }
    None
}

/// The ASKING USER's IANA timezone, for resolving an event-recap window's
/// calendar boundaries — the single source of "whose today?" for the ask
/// path (`crate::scoped_ask`).
///
/// Reads `users.timezone` (migration `0118_users_timezone`), captured from the
/// browser by `POST /v1/me/timezone`. Falls back to
/// `journal_sweep::journal_tz()` (env `JOURNAL_TZ`, else UTC) when the user has
/// never captured one — a brand-new user, an API/MCP caller that never loaded
/// the web app, or a client build predating the capture endpoint. That is the
/// SAME fallback the journal uses, so an untimezoned user gets one consistent
/// notion of "today" across both features.
///
/// # Why this reads CONTROL, and why it is not `journal_sweep::resolve_user_tz`
///
/// `users` is a CONTROL-plane table — it does not exist on the tenant plane at
/// all (see `0118_users_timezone`'s header). `resolve_user_tz` therefore takes
/// a `TenantId` and returns the bare fallback for `TenantId::Org(_)`, because
/// the JOURNAL SWEEP is a background worker bound to one plane with no
/// connection back to control.
///
/// A REQUEST is not so constrained: `AppState::control_store` is the control
/// plane and is already used on this very path (the `account_prefs` egress
/// ceiling). So an org-tenant user gets their real timezone here rather than
/// being silently dropped to UTC — the org gap is a worker limitation, not an
/// inherent one, and there is no reason to import it into the ask path.
///
/// Never fails: a lookup error is logged and degrades to the fallback, exactly
/// as an unset timezone would. A recap answering in the wrong zone is a bug;
/// a recap that 500s because a timezone lookup hiccuped is a worse one.
pub(crate) async fn resolve_asking_user_tz(state: &AppState, user_id: Uuid) -> Tz {
    let fallback = crate::workers::journal_sweep::journal_tz();
    let Some(store) = state.control_store() else {
        return fallback;
    };
    match store.user_timezone(user_id).await {
        Ok(Some(raw)) => match raw.trim().parse::<Tz>() {
            Ok(tz) => tz,
            Err(_) => {
                // A stored value that no longer parses (renamed/retired zone,
                // or a client that sent something bogus past the route's own
                // validation) must not silently shift the window.
                tracing::warn!(
                    %user_id,
                    "stored user timezone is not a parseable IANA zone — \
                     falling back for this ask's event window"
                );
                fallback
            }
        },
        Ok(None) => fallback,
        Err(e) => {
            tracing::warn!(
                %user_id, error = %e,
                "user_timezone lookup failed — falling back for this ask's event window"
            );
            fallback
        }
    }
}

/// Midnight (00:00:00) on `day` **in `tz`**, as the UTC instant it corresponds
/// to — the primitive every calendar-boundary helper below is built from.
///
/// # Why this is not `day.and_hms_opt(0,0,0).and_utc()`
///
/// It used to be, and that was the "answered from 0 sources" bug: a calendar
/// day was resolved against the UTC date, so for any user west of UTC "today"
/// began at 19:00/18:00 the previous LOCAL evening. A US-Central founder
/// asking "what was worked on today" at 21:38 local got a window covering
/// 19:00–21:38 — the whole working day sat in what the server called
/// yesterday, the deterministic date scan found nothing, and synthesis fell
/// back to the (deliberately non-citable) CLAIMS block and reported zero
/// sources.
///
/// DST handling (a local midnight can be skipped or can happen twice) is
/// delegated to `journal_sweep::resolve_local_midnight` rather than
/// reimplemented, so a recap's day boundaries and the journal's day
/// boundaries can never disagree about a transition day.
fn tz_midnight(day: chrono::NaiveDate, tz: Tz) -> DateTime<Utc> {
    crate::workers::journal_sweep::resolve_local_midnight(
        tz,
        day.and_hms_opt(0, 0, 0).expect("00:00:00 is always valid"),
    )
}

/// `now`'s calendar date **in `tz`** — the anchor every "days ago" offset
/// below counts back from.
fn tz_today(now: DateTime<Utc>, tz: Tz) -> chrono::NaiveDate {
    now.with_timezone(&tz).date_naive()
}

/// `[start-of-day, min(end-of-day, now)]` for the day `days_ago` days before
/// `now`'s date **in `tz`**. `days_ago = 0` ⇒ today (end clamped to `now`, not
/// midnight tonight, so it never reaches into the future); `days_ago = 1` ⇒
/// yesterday (end is exactly local midnight today, which is always ≤ `now`).
///
/// The returned bounds are UTC instants, as the `atoms.timestamp` scan needs —
/// only the BOUNDARIES are computed in `tz`.
fn day_range(now: DateTime<Utc>, tz: Tz, days_ago: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let day = tz_today(now, tz) - chrono::Duration::days(days_ago);
    let start = tz_midnight(day, tz);
    // Next local midnight, not `start + 24h`: a DST transition inside the day
    // makes it 23 or 25 hours long, and a fixed offset would clip or overrun it.
    let end = tz_midnight(day + chrono::Duration::days(1), tz).min(now);
    (start, end.max(start))
}

/// `[start-of-week, min(end-of-week, now)]` (ISO week, Monday start) for the
/// week `weeks_ago` weeks before `now`'s current week, in `tz`.
fn week_range(now: DateTime<Utc>, tz: Tz, weeks_ago: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    use chrono::Datelike;
    let today = tz_today(now, tz);
    let days_since_monday = today.weekday().num_days_from_monday() as i64;
    let this_monday = today - chrono::Duration::days(days_since_monday);
    let start_day = this_monday - chrono::Duration::days(7 * weeks_ago);
    let start = tz_midnight(start_day, tz);
    let end = tz_midnight(start_day + chrono::Duration::days(7), tz).min(now);
    (start, end.max(start))
}

/// `[start-of-month, min(end-of-month, now)]` for the month `months_ago`
/// months before `now`'s current month, in `tz`.
fn month_range(now: DateTime<Utc>, tz: Tz, months_ago: u32) -> (DateTime<Utc>, DateTime<Utc>) {
    use chrono::Datelike;
    let today = tz_today(now, tz);
    let mut y = today.year();
    let mut m = today.month() as i64 - months_ago as i64;
    while m <= 0 {
        m += 12;
        y -= 1;
    }
    let m = m as u32;
    let start_day = chrono::NaiveDate::from_ymd_opt(y, m, 1).unwrap();
    let start = tz_midnight(start_day, tz);
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    let end_day = chrono::NaiveDate::from_ymd_opt(ny, nm, 1).unwrap();
    let end = tz_midnight(end_day, tz).min(now);
    (start, end.max(start))
}

/// `[start-of-day, min(end-of-day, now)]` for the most recent occurrence of
/// `weekday` at or before `now`'s date in `tz` (today counts if it IS that
/// weekday) — "what happened on Monday" is the user's Monday, not UTC's.
fn weekday_range(
    now: DateTime<Utc>,
    tz: Tz,
    weekday: chrono::Weekday,
) -> (DateTime<Utc>, DateTime<Utc>) {
    use chrono::Datelike;
    let today = tz_today(now, tz);
    let today_wd = today.weekday().num_days_from_monday() as i64;
    let target_wd = weekday.num_days_from_monday() as i64;
    let diff = (today_wd - target_wd).rem_euclid(7);
    day_range(now, tz, diff)
}

/// Collapse a body of text to a single-line snippet capped at `max_chars`
/// characters (char-safe): internal whitespace runs (incl. newlines) are
/// collapsed to single spaces, then the result is trimmed and truncated with
/// a trailing "…" when it overflows. Shared by every route that derives a
/// human preview from raw atom content (intake highlights, candidate corals).
pub(crate) fn collapse_snippet(body: &str, max_chars: usize) -> String {
    let one_line = body.split_whitespace().collect::<Vec<_>>().join(" ");
    let t = one_line.trim();
    if t.chars().count() > max_chars {
        let head: String = t.chars().take(max_chars).collect();
        format!("{}…", head.trim_end())
    } else {
        t.to_string()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn fetch_timestamps(
    store: &dyn Store,
    scope: Option<(&sqlx::PgPool, Uuid, Uuid)>,
    atom_ids: &[Uuid],
) -> HashMap<Uuid, DateTime<Utc>> {
    if atom_ids.is_empty() {
        return HashMap::new();
    }

    // Timestamps drive the reranker's `recency_decay` component and every
    // citation's `age_days`. Unscoped on a live-RLS consumer plane this
    // errored into an empty map, which silently zeroed the recency signal for
    // the whole candidate pool.
    let rows = match scope {
        Some((pool, account_id, user_id)) => {
            scoped_atom_timestamps(store, pool, account_id, user_id, atom_ids).await
        }
        None => store.atom_timestamps(atom_ids).await,
    };
    let rows = match rows {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(
                error = %e,
                "atom_timestamps failed — recency ranking and citation ages degrade for this query"
            );
            Vec::new()
        }
    };

    rows.into_iter()
        .filter_map(|(id, ts)| ts.map(|t| (id, t)))
        .collect()
}

/// Outcome of fanning retrieval out across one or more in-scope oceans
/// (ADR-0011 story 1). Candidate pools from every reachable ocean are merged,
/// reranked globally, then trimmed with a per-ocean floor + ocean-count-scaled
/// cap. `atom_graph` lets the caller tag every surviving atom (and therefore
/// every citation) with the ocean it came from.
pub(crate) struct MultiRetrieval {
    /// Final synthesis/contradiction set: globally reranked, floor-cleared,
    /// per-ocean-floor-guaranteed, capped. Empty ⇒ honesty guard should fire.
    pub(crate) reranked: Vec<RankedAtom>,
    /// Pre-cap reranked pool (after vsim + rerank + optional supersession
    /// penalty). Ask-path Jev packing (ADR-0014) judges this ~50-card set
    /// before `take(cap)`. Empty when retrieval found nothing.
    pub(crate) ranked_pre_cap: Vec<RankedAtom>,
    /// `atom_id → graph_id` for every candidate (used for per-ocean citation
    /// badges + contradiction grouping).
    pub(crate) atom_graph: HashMap<Uuid, Uuid>,
    /// Merged graph-expansion nodes across all oceans.
    pub(crate) nodes: Vec<NodeWithAtoms>,
    /// `node_id → graph_id` so per-ocean relationship lookups stay scoped.
    #[allow(dead_code)]
    pub(crate) node_graph: HashMap<Uuid, Uuid>,
    /// `atom_id → node_id` from expansion.
    pub(crate) node_map: HashMap<Uuid, Uuid>,
    /// `atom_id → timestamp` for the surviving set.
    pub(crate) timestamps: HashMap<Uuid, DateTime<Utc>>,
    /// Oceans whose retrieval errored — surfaced as `unreachable_oceans` (G25).
    pub(crate) unreachable: Vec<Uuid>,
    /// At least one candidate cleared the relevance floor (honesty guard input).
    pub(crate) any_cleared_floor: bool,
    /// `atom_id → every superseding reef (node)` (Phase 5b — supersession-aware
    /// ranking; widened to multi-valued by §3.6). Populated from
    /// `supersession_edge` regardless of `SUPERSESSION_RANKING_ENABLED` (a
    /// plain read has no behavior to gate), but callers MUST re-check
    /// [`supersession_ranking_enabled`] before acting on it (annotating
    /// `SynthesisAtom::superseded`, surfacing `superseded_by` on a citation,
    /// etc.) — the flag is what gates actual behavior change, not the
    /// presence of this map. Node-level, not atom-level: see the granularity
    /// note in `multi_graph_retrieve`.
    pub(crate) superseded_atoms: HashMap<Uuid, Vec<SupersededClaim>>,
    /// True only when supersession ranking is ON and EVERY floor-clearing
    /// atom in `reranked` is known-superseded (honesty-guard input, Phase
    /// 5b) — i.e. removing superseded atoms from consideration would leave
    /// zero above-floor results. Always `false` when the flag is off.
    pub(crate) all_surviving_superseded: bool,
    /// Distilled non-`entity` assertions for the `CLAIMS` block (§3.6,
    /// completing D2's `ENTITIES` narrowing) — capped at
    /// [`SYNTHESIS_CLAIMS_CAP`] plus any superseder pulled in past the cap by
    /// the never-orphan invariant. Unlike `superseded_atoms`, this is
    /// populated regardless of `SUPERSESSION_RANKING_ENABLED` (D2/§3.3's fix
    /// is unconditional); only each claim's OWN `superseded` field is
    /// flag-gated, mirroring `SynthesisAtom::superseded`.
    pub(crate) claims: Vec<SynthesisClaim>,
    /// `atom_id → title` for corals the DETERMINISTIC date scan contributed and
    /// normal semantic retrieval did not (migration 0097, step 3 of per-coral
    /// titles).
    ///
    /// These are represented to synthesis by their TITLE rather than their body.
    /// A date-range recap wants COVERAGE — every coral in the range — and bodies
    /// cannot pay for that: each one costs an `atoms` content read (R2 GET +
    /// decrypt when blobs are on) plus its full length in the prompt, and the
    /// window can be a whole month. Titles are ~10 words, so the same answer can
    /// name everything that happened instead of a sampled subset, and the
    /// truncation this replaces (which dropped whole days) stops being needed.
    ///
    /// Atoms that ALSO cleared semantic retrieval are deliberately absent here —
    /// they keep their full body, because those are the ones the question is
    /// actually about. So a recap reads as "full detail on what matters, plus a
    /// complete inventory of everything else in the range".
    ///
    /// Empty whenever there is no event window, which is the overwhelming
    /// majority of asks.
    pub(crate) window_titles: HashMap<Uuid, WindowCoral>,
    /// Catalog-scan yes-reefs for this turn. Empty when the kill switch is
    /// off, the Noul found nothing, or fetch failed open. Packing requires
    /// a non-empty set (Q14).
    pub(crate) catalog_yes_reefs: HashSet<Uuid>,
    /// §7.4 per-gate retrieval trace — always populated by
    /// `multi_graph_retrieve` itself (see the module-level note above
    /// [`RetrievalTrace`]); untouched by `apply_event_window_merge`, which
    /// has no gates of its own in scope for this trace.
    pub(crate) trace: RetrievalTrace,
    /// Subgraph this question's currents walk produced. Always populated
    /// (including shadow mode); `merged` is whether the flag folded hopped
    /// atoms into the real pool. Ask's Context drawer draws this.
    pub(crate) walk: TraversalWalk,
}

/// [`Store::expand`], post-filtered against `filter.project` when set
/// (coding-session capture Phase 3, audit P1-6): `Store::expand` deliberately
/// takes no filter of its own — it walks GRAPH STRUCTURE (which reef(s) a
/// seed atom belongs to, and that reef's OTHER member atoms, up to
/// [`crate::store::MAX_SIBLING_ATOMS_PER_NODE`] per node — plus any
/// supersession-injected copies), not atom CONTENT — so a project-filtered
/// `ask_ocean`/`search_atoms` call would otherwise still drag sibling atoms
/// from a DIFFERENT project into `nodes` via a matched reef's membership,
/// even though `vector_search`/`keyword_search`/`timestamp_window_atoms`
/// themselves already exclude those atoms from ever being a SEED. This
/// matters even though `mgr.reranked` (the actual citation/result set) is
/// built only from vector/keyword/event-window hits, never from `expand`'s
/// raw `atom_ids` directly — `nodes` still feeds `atom_reef_map`/
/// `node_title_map`/`build_synthesis_reefs`' reef-digest selection AND
/// `citation_details`' cross-tenant-only (not cross-project) backfill scope
/// check, so an out-of-project atom id surviving inside `nodes` is a real,
/// if narrow, boundary leak — not merely inert bookkeeping.
///
/// When `filter.project` is `None` this is a pure pass-through (zero extra
/// queries) — every existing HTTP/`create_query_stream` caller of
/// `multi_graph_retrieve`/`apply_event_window_merge` is therefore
/// byte-identical to before this function existed.
#[allow(clippy::too_many_arguments)]
async fn expand_filtered(
    store: &dyn Store,
    scope: Option<(&sqlx::PgPool, Uuid, Uuid)>,
    graph_id: Uuid,
    seed_atom_ids: &[Uuid],
    filter: &RetrievalFilter,
    query_embedding: Option<&[f32]>,
) -> Vec<NodeWithAtoms> {
    // Scoped whenever `scope` is `Some`. Unscoped on a consumer plane with
    // `0083_consumer_rls` live raises rather than returning rows, and this
    // used to be a silent `.unwrap_or_default()` — which is why every
    // citation came back with `reefs: []` against a reef-rich ocean.
    //
    // `scope` is `None` for TWO reasons, not one — this comment used to claim
    // "bare only on pool-less backends" and that was false (#561). It is
    // `None` with no pool (no RLS, and the bare call is correct), but ALSO
    // whenever the caller's account lookup missed despite
    // a live pool: `multi_graph_retrieve` builds it as
    // `pool.zip(account_by_graph.get(gid))`, and `account_by_graph` is only
    // populated for graphs whose account actually resolved
    // (`partition_event_window_scans`). In that second case the bare call
    // cannot succeed on the consumer plane — it is a second fault on a path
    // where account resolution has already failed, and the `Err` arm below
    // reports it loudly rather than silently returning an empty reef set.
    let expanded = match scope {
        Some((pool, account_id, user_id)) => {
            scoped_expand(
                store,
                pool,
                account_id,
                user_id,
                graph_id,
                seed_atom_ids,
                query_embedding,
            )
            .await
        }
        None => store.expand(graph_id, seed_atom_ids, query_embedding).await,
    };
    let nodes = match expanded {
        Ok(nodes) => nodes,
        Err(e) => {
            // Reef membership is ENRICHMENT (citations' `reefs`, synthesis
            // reef digests), not the answer set — `mgr.reranked` is built from
            // vector/keyword/event-window hits, never from `expand`. So a
            // failure here degrades enrichment rather than the ocean, exactly
            // as before; it is now LOUD instead of silent.
            tracing::error!(
                graph_id = %graph_id,
                error = %e,
                "expand failed — citations will carry no reef context for this ocean"
            );
            return Vec::new();
        }
    };
    let Some(project) = filter.project.as_deref() else {
        return nodes;
    };

    // One batch lookup for every atom id `expand` returned (genuine members
    // AND supersession-injected copies) — never N+1 per node.
    let mut all_atom_ids: Vec<Uuid> = nodes
        .iter()
        .flat_map(|n| n.atom_ids.iter().chain(n.injected_atom_ids.iter()).copied())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    all_atom_ids.sort();
    if all_atom_ids.is_empty() {
        return nodes;
    }
    let provenance = match scope {
        Some((pool, account_id, user_id)) => {
            scoped_atom_provenance(store, pool, account_id, user_id, &all_atom_ids).await
        }
        None => store.atom_provenance(&all_atom_ids).await,
    };
    let project_by_atom: HashMap<Uuid, Option<String>> = match provenance {
        Ok(rows) => rows
            .into_iter()
            .map(|(atom_id, project, _mcp_client_name)| (atom_id, project))
            .collect(),
        Err(e) => {
            // FAIL CLOSED. `.unwrap_or_default()` here used to yield an empty
            // map, which made `matches_project` false for EVERY atom and so
            // silently dropped every node — a false-empty. The alternative
            // (returning `nodes` unfiltered) is worse: it is precisely the
            // cross-project boundary leak this function exists to prevent. So
            // keep the narrowing behavior, but say so out loud.
            tracing::error!(
                graph_id = %graph_id,
                error = %e,
                "atom_provenance failed under a project filter — dropping reef \
                 context for this ocean rather than leaking out-of-project atoms"
            );
            return Vec::new();
        }
    };
    // An atom `atom_provenance` has no row for at all (no project AND no
    // mcp_client_name ever stamped) reads as `None` here too — correct,
    // since `None != Some(project)` either way, and this is a strict
    // narrowing filter (never widens what a non-project-filtered call would
    // have returned).
    let matches_project = |atom_id: &Uuid| -> bool {
        project_by_atom
            .get(atom_id)
            .and_then(|p| p.as_deref())
            .is_some_and(|p| p == project)
    };

    nodes
        .into_iter()
        .filter_map(|n| {
            let NodeWithAtoms {
                node_id,
                node_type,
                title,
                atom_ids,
                injected_atom_ids,
            } = n;
            let atom_ids: Vec<Uuid> = atom_ids.into_iter().filter(matches_project).collect();
            let injected_atom_ids: Vec<Uuid> = injected_atom_ids
                .into_iter()
                .filter(matches_project)
                .collect();
            // A node left with NO project-matching atoms (genuine or
            // injected) has nothing legitimate to contribute to a
            // project-scoped answer — drop it entirely so its title/digest
            // can never surface via `build_synthesis_reefs` either.
            if atom_ids.is_empty() && injected_atom_ids.is_empty() {
                None
            } else {
                Some(NodeWithAtoms {
                    node_id,
                    node_type,
                    title,
                    atom_ids,
                    injected_atom_ids,
                })
            }
        })
        .collect()
}

/// [`Store::vector_search`] under its own short `begin_tenant_scoped`
/// transaction — see `multi_graph_retrieve`'s RLS doc section for why each
/// of the three concurrent per-ocean calls gets its own transaction rather
/// than sharing one.
#[allow(clippy::too_many_arguments)]
async fn scoped_vector_search(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    embedding: &[f32],
    filter: &RetrievalFilter,
) -> Result<Vec<(Uuid, f32)>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .vector_search_scoped(&mut tx, graph_id, embedding, RETRIEVAL_CANDIDATE_K, filter)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::keyword_search`] under its own short `begin_tenant_scoped`
/// transaction — see `multi_graph_retrieve`'s RLS doc section.
#[allow(clippy::too_many_arguments)]
async fn scoped_keyword_search(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    search_text: &str,
    filter: &RetrievalFilter,
) -> Result<Vec<(Uuid, f64)>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .keyword_search_scoped(&mut tx, graph_id, search_text, RETRIEVAL_CANDIDATE_K, filter)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::expand`] under its own short `begin_tenant_scoped` transaction —
/// see `multi_graph_retrieve`'s RLS doc section.
#[allow(clippy::too_many_arguments)]
async fn scoped_expand(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    seed_atom_ids: &[Uuid],
    query_embedding: Option<&[f32]>,
) -> Result<Vec<NodeWithAtoms>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .expand_scoped(&mut tx, graph_id, seed_atom_ids, query_embedding)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::atom_timestamps`] under its own short `begin_tenant_scoped`
/// transaction — see `multi_graph_retrieve`'s RLS doc section.
async fn scoped_atom_timestamps(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    atom_ids: &[Uuid],
) -> Result<Vec<(Uuid, Option<DateTime<Utc>>)>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store.atom_timestamps_scoped(&mut tx, atom_ids).await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::list_supersession_targets`] under its own short
/// `begin_tenant_scoped` transaction — see `multi_graph_retrieve`'s RLS doc
/// section.
async fn scoped_list_supersession_targets(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<crate::store::SupersessionTarget>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .list_supersession_targets_scoped(&mut tx, node_ids)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::atom_provenance`] under its own short `begin_tenant_scoped`
/// transaction — see `multi_graph_retrieve`'s RLS doc section.
async fn scoped_atom_provenance(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    atom_ids: &[Uuid],
) -> Result<Vec<(Uuid, Option<String>, Option<String>)>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store.atom_provenance_scoped(&mut tx, atom_ids).await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::keyword_search_node_atoms`] under its own short
/// `begin_tenant_scoped` transaction — see `multi_graph_retrieve`'s RLS doc
/// section.
#[allow(clippy::too_many_arguments)]
async fn scoped_keyword_search_node_atoms(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    search_text: &str,
    filter: &RetrievalFilter,
) -> Result<Vec<(Uuid, f64)>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .keyword_search_node_atoms_scoped(&mut tx, graph_id, search_text, NODE_CANDIDATE_K, filter)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::matched_reef_ids`] under its own short `begin_tenant_scoped`
/// transaction (§5.4's traversal hop-0 seed discovery) — see
/// `multi_graph_retrieve`'s RLS doc section.
async fn scoped_matched_reef_ids(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    search_text: &str,
    filter: &RetrievalFilter,
) -> Result<Vec<(Uuid, f64)>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .matched_reef_ids_scoped(&mut tx, graph_id, search_text, NODE_CANDIDATE_K, filter)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::list_relationship_edges_for_nodes`] under its own short
/// `begin_tenant_scoped` transaction — §5.4's per-hop outbound edge fetch.
async fn scoped_list_relationship_edges_for_nodes(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<crate::relationships::RelationshipEdge>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .list_relationship_edges_for_nodes_scoped(&mut tx, graph_id, node_ids)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

/// [`Store::reef_atoms_ranked_by_similarity`] under its own short
/// `begin_tenant_scoped` transaction — §5.4's hopped-reef atom fetch (the D1
/// fix applied to hopped reefs).
async fn scoped_reef_atoms_ranked_by_similarity(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    node_ids: &[Uuid],
    query_embedding: Option<&[f32]>,
) -> Result<std::collections::HashMap<Uuid, Vec<Uuid>>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .reef_atoms_ranked_by_similarity_scoped(&mut tx, graph_id, node_ids, query_embedding)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

async fn scoped_node_ids_for_atoms(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    atom_ids: &[Uuid],
) -> Result<Vec<Uuid>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .node_ids_for_atoms_scoped(&mut tx, graph_id, atom_ids)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

async fn scoped_nodes_by_ids(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    node_ids: &[Uuid],
) -> Result<Vec<(Uuid, String)>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .nodes_by_ids_scoped(&mut tx, graph_id, node_ids)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

async fn scoped_list_cooccurrence_neighbors(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    node_ids: &[Uuid],
    min_shared: i64,
    exclude: Option<Uuid>,
) -> Result<Vec<(Uuid, Uuid, i32)>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let rows = store
        .list_cooccurrence_neighbors_scoped(&mut tx, graph_id, node_ids, min_shared, exclude)
        .await?;
    tx.commit().await?;
    Ok(rows)
}

async fn scoped_graph_self_node_id(
    store: &dyn Store,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
) -> Result<Option<Uuid>, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id).await?;
    let id = store.graph_self_node_id_scoped(&mut tx, graph_id).await?;
    tx.commit().await?;
    Ok(id)
}

/// Result of walking §5.4's currents traversal for ONE graph. ALWAYS
/// computed by [`traverse_currents`] regardless of
/// [`graph_traversal_enabled`] — §5.5's shadow mode needs the full result to
/// log the delta even while the flag is off; it is the CALLER
/// ([`multi_graph_retrieve`]) that decides whether to merge
/// `candidate_atoms` into the real pool or only log it.
#[derive(Debug, Clone, Default)]
pub(crate) struct CurrentsTraversal {
    /// Number of hop-0 seed reefs the query itself matched
    /// ([`Store::matched_reef_ids`]) — the frontier traversal started from.
    pub(crate) seed_count: usize,
    /// Every reef reached at hop 1 and/or hop 2, capped at
    /// [`HOPPED_REEF_CAP`], with its decayed score (highest first).
    pub(crate) hopped_reefs: Vec<(Uuid, f64)>,
    /// Atom-level candidates derived from `hopped_reefs`
    /// ([`flatten_hopped_atoms`]) — the SAME `(atom_id, score)` shape
    /// [`Store::keyword_search_node_atoms`] returns, so it slots into
    /// [`merge_keyword_hits`] unchanged (§5.2: no `rerank` contract change).
    pub(crate) candidate_atoms: Vec<(Uuid, f64)>,
    /// Seed reefs, hopped reefs, and the currents that nominated them — the
    /// subgraph Ask's side panel draws. Always filled when seeds exist,
    /// including shadow mode (`merged` is set later by the caller).
    pub(crate) walk: TraversalWalk,
}

/// Walk §5.4's two-hop currents traversal for a single graph.
///
/// Seeds are the union of:
/// - reefs the query text names ([`Store::matched_reef_ids`])
/// - entity reefs of already-retrieved corals (`hit_atom_ids`), except the
///   self reef (a hit on the owner must not start a walk through their star)
///
/// Neighbour pick is query-aware ([`HopPolicy`]): dest-title overlap, then
/// topical vs professional predicate, then derived co-occurrence weight,
/// then confidence. Kinship is skipped unless the question looks kinship.
/// `works_on` / `responsible_for` / `expert_in` walk both ways because
/// their reverse rows are not stored. Derived co-occurrence pairs (shared
/// corals ≥ [`ASK_CO_OCCUR_MIN_SHARED`]) are a second edge source; the self
/// reef is never a hop target.
///
/// ALWAYS computed: this function has no notion of
/// `GRAPH_TRAVERSAL_ENABLED` — see [`graph_traversal_enabled`]'s doc comment
/// for why the flag gate lives entirely in the caller, not here.
async fn traverse_currents(
    store: &dyn Store,
    pool: Option<&sqlx::PgPool>,
    user_id: Uuid,
    account_id: Option<Uuid>,
    graph_id: Uuid,
    query: &str,
    query_embedding: &[f32],
    filter: &RetrievalFilter,
    hit_atom_ids: &[Uuid],
) -> CurrentsTraversal {
    let scope = pool.zip(account_id);

    let mut seeds: Vec<(Uuid, f64)> = match scope {
        Some((p, acct)) => {
            scoped_matched_reef_ids(store, p, acct, user_id, graph_id, query, filter)
                .await
                .unwrap_or_default()
        }
        None => store
            .matched_reef_ids(graph_id, query, NODE_CANDIDATE_K, filter)
            .await
            .unwrap_or_default(),
    };

    let named_seeds: HashSet<Uuid> = seeds.iter().map(|(id, _)| *id).collect();

    let self_id = match scope {
        Some((p, acct)) => scoped_graph_self_node_id(store, p, acct, user_id, graph_id)
            .await
            .unwrap_or(None),
        None => store.graph_self_node_id(graph_id).await.unwrap_or(None),
    };

    if !hit_atom_ids.is_empty() {
        let hit_reefs = match scope {
            Some((p, acct)) => {
                scoped_node_ids_for_atoms(store, p, acct, user_id, graph_id, hit_atom_ids)
                    .await
                    .unwrap_or_default()
            }
            None => store
                .node_ids_for_atoms(graph_id, hit_atom_ids)
                .await
                .unwrap_or_default(),
        };
        let already: HashSet<Uuid> = seeds.iter().map(|(id, _)| *id).collect();
        let mut hit_seed_count = 0;
        for id in hit_reefs {
            if Some(id) == self_id || already.contains(&id) {
                continue;
            }
            if hit_seed_count >= MAX_HIT_SEED_REEFS {
                break;
            }
            seeds.push((id, 1.0));
            hit_seed_count += 1;
        }
    }

    if seeds.is_empty() {
        return CurrentsTraversal::default();
    }

    let query_tokens = crate::retrieval::node_query_tokens(query);
    let kinship_ok = query_looks_kinship(&query_tokens);
    let mut exclude = HashSet::new();
    if let Some(id) = self_id {
        exclude.insert(id);
    }

    let mut visited: HashSet<Uuid> = seeds.iter().map(|(id, _)| *id).collect();
    let mut frontier: HashMap<Uuid, f64> = seeds.iter().cloned().collect();
    let mut all_hopped: HashMap<Uuid, crate::retrieval::traversal::HopCandidate> = HashMap::new();
    let mut hop_of: HashMap<Uuid, u8> = HashMap::new();
    let mut walked_currents: Vec<WalkedCurrent> = Vec::new();
    let mut titles: HashMap<Uuid, String> = HashMap::new();

    for hop in 1..=MAX_HOPS {
        let frontier_ids: Vec<Uuid> = frontier.keys().copied().collect();
        let edges = match scope {
            Some((p, acct)) => scoped_list_relationship_edges_for_nodes(
                store,
                p,
                acct,
                user_id,
                graph_id,
                &frontier_ids,
            )
            .await
            .unwrap_or_default(),
            None => store
                .list_relationship_edges_for_nodes(graph_id, &frontier_ids)
                .await
                .unwrap_or_default(),
        };
        let derived = match scope {
            Some((p, acct)) => scoped_list_cooccurrence_neighbors(
                store,
                p,
                acct,
                user_id,
                graph_id,
                &frontier_ids,
                ASK_CO_OCCUR_MIN_SHARED,
                self_id,
            )
            .await
            .unwrap_or_default(),
            None => store
                .list_cooccurrence_neighbors(
                    graph_id,
                    &frontier_ids,
                    ASK_CO_OCCUR_MIN_SHARED,
                    self_id,
                )
                .await
                .unwrap_or_default(),
        };

        let mut title_ids: Vec<Uuid> = edges
            .iter()
            .flat_map(|e| [e.src_node_id, e.dst_node_id])
            .collect();
        title_ids.extend(derived.iter().map(|&(_, dst, _)| dst));
        title_ids.sort_unstable();
        title_ids.dedup();
        let dest_titles: HashMap<Uuid, String> = match scope {
            Some((p, acct)) => scoped_nodes_by_ids(store, p, acct, user_id, graph_id, &title_ids)
                .await
                .unwrap_or_default(),
            None => store
                .nodes_by_ids(graph_id, &title_ids)
                .await
                .unwrap_or_default(),
        }
        .into_iter()
        .collect();
        titles.extend(dest_titles.iter().map(|(id, t)| (*id, t.clone())));

        let policy = HopPolicy {
            query_tokens: query_tokens.clone(),
            dest_titles,
            exclude: exclude.clone(),
            kinship_ok,
            derived,
        };
        let outcome = hop_scores(&frontier, &edges, &visited, &policy);
        if outcome.next.is_empty() {
            break;
        }
        walked_currents.extend(outcome.currents);
        for (&id, &cand) in &outcome.next {
            hop_of.entry(id).or_insert(hop as u8);
            visited.insert(id);
            let slot = all_hopped
                .entry(id)
                .or_insert(crate::retrieval::traversal::HopCandidate {
                    score: f64::MIN,
                    best_confidence: f32::MIN,
                });
            if cand.score > slot.score
                || (cand.score == slot.score && cand.best_confidence > slot.best_confidence)
            {
                *slot = cand;
            }
        }
        frontier = cap_frontier(&outcome.next);
    }

    let hopped_reefs = cap_hopped_reefs(all_hopped);
    let missing: Vec<Uuid> = seeds
        .iter()
        .map(|(id, _)| *id)
        .chain(hopped_reefs.iter().map(|(id, _)| *id))
        .filter(|id| !titles.contains_key(id))
        .collect();
    if !missing.is_empty() {
        let extra: HashMap<Uuid, String> = match scope {
            Some((p, acct)) => scoped_nodes_by_ids(store, p, acct, user_id, graph_id, &missing)
                .await
                .unwrap_or_default(),
            None => store
                .nodes_by_ids(graph_id, &missing)
                .await
                .unwrap_or_default(),
        }
        .into_iter()
        .collect();
        titles.extend(extra);
    }
    let walk = assemble_walk(
        &seeds,
        &named_seeds,
        &hopped_reefs,
        &hop_of,
        walked_currents,
        &titles,
    );

    if hopped_reefs.is_empty() {
        return CurrentsTraversal {
            seed_count: seeds.len(),
            hopped_reefs,
            candidate_atoms: Vec::new(),
            walk,
        };
    }

    let hopped_ids: Vec<Uuid> = hopped_reefs.iter().map(|(id, _)| *id).collect();
    let atoms_by_node = match scope {
        Some((p, acct)) => scoped_reef_atoms_ranked_by_similarity(
            store,
            p,
            acct,
            user_id,
            graph_id,
            &hopped_ids,
            Some(query_embedding),
        )
        .await
        .unwrap_or_default(),
        None => store
            .reef_atoms_ranked_by_similarity(graph_id, &hopped_ids, Some(query_embedding))
            .await
            .unwrap_or_default(),
    };
    let candidate_atoms = flatten_hopped_atoms(&hopped_reefs, &atoms_by_node);

    CurrentsTraversal {
        seed_count: seeds.len(),
        hopped_reefs,
        candidate_atoms,
        walk,
    }
}

fn assemble_walk(
    seeds: &[(Uuid, f64)],
    named_seeds: &HashSet<Uuid>,
    hopped_reefs: &[(Uuid, f64)],
    hop_of: &HashMap<Uuid, u8>,
    walked: Vec<WalkedCurrent>,
    titles: &HashMap<Uuid, String>,
) -> TraversalWalk {
    let hopped_set: HashSet<Uuid> = hopped_reefs.iter().map(|(id, _)| *id).collect();
    let title = |id: Uuid| {
        titles
            .get(&id)
            .cloned()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Untitled".to_string())
    };
    // Prefer currents that leave a query-named seed; then cap so hit-reef
    // fan-out cannot dump hundreds of rows into Ask's side panel.
    let mut currents: Vec<WalkedCurrent> = walked
        .into_iter()
        .filter(|c| hopped_set.contains(&c.dst))
        .collect();
    currents.sort_by_key(|c| if named_seeds.contains(&c.src) { 0u8 } else { 1 });
    currents.truncate(16);

    let used_src: HashSet<Uuid> = currents.iter().map(|c| c.src).collect();
    let mut seed_reefs: Vec<WalkReef> = Vec::new();
    for (id, _) in seeds {
        if !(named_seeds.contains(id) || used_src.contains(id)) {
            continue;
        }
        if seed_reefs.iter().any(|r| r.id == *id) {
            continue;
        }
        seed_reefs.push(WalkReef {
            id: *id,
            title: title(*id),
            hop: 0,
        });
        if seed_reefs.len() >= 8 {
            break;
        }
    }

    let shown: HashSet<Uuid> = seed_reefs
        .iter()
        .map(|r| r.id)
        .chain(hopped_set.iter().copied())
        .collect();
    currents.retain(|c| shown.contains(&c.src) && hopped_set.contains(&c.dst));

    TraversalWalk {
        merged: false,
        seeds: seed_reefs,
        hops: hopped_reefs
            .iter()
            .map(|(id, _)| WalkReef {
                id: *id,
                title: title(*id),
                hop: hop_of.get(id).copied().unwrap_or(1),
            })
            .collect(),
        currents,
    }
}

fn merge_traversal_walks(
    by_graph: &HashMap<Uuid, CurrentsTraversal>,
    merged: bool,
) -> TraversalWalk {
    let mut walk = TraversalWalk {
        merged,
        ..TraversalWalk::default()
    };
    for t in by_graph.values() {
        walk.seeds.extend(t.walk.seeds.iter().cloned());
        walk.hops.extend(t.walk.hops.iter().cloned());
        walk.currents.extend(t.walk.currents.iter().cloned());
    }
    walk
}

/// Fan retrieval out across `graph_ids` in PARALLEL at full candidate K, merge
/// into one pool, rerank globally (no per-ocean quota), apply the per-ocean floor
/// guarantee, then the ocean-count-scaled context cap (ADR-0011 A4–A6, G25).
///
/// # RLS scoping, both planes (org-tenancy OPS-4 Store-surface follow-up;
/// # extended by tenant RLS Phase 2)
///
/// `pool` is `Some(&PgPool)` on any Postgres plane (consumer OR org tenant)
/// and `None` with no pool (no RLS),
/// matching every other `_scoped`-dispatch call site in this codebase
/// (`routes::atlas`'s `scoped_store!` macro, `routes::sessions`'s, etc.).
///
/// Each candidate ocean's `vector_search`/`keyword_search`/
/// `keyword_search_node_atoms` trio is the retrieval CORE's hot path — the
/// exact three calls PR #292 (B.1) flagged as blocked on `Store` twins that
/// didn't exist yet (now added). Scoped PER GRAPH (`graph_ids` is the D9
/// multi-account reachable set — ADR-0007 Model B — so there is no single
/// account this whole function can scope to) and PER STATEMENT: each of the
/// three concurrent calls opens its OWN short `begin_tenant_scoped`
/// transaction rather than sharing one, because `tokio::join!` needs to hold
/// all three in flight at once and a single `&mut Transaction` cannot be
/// borrowed by more than one in-flight query (same reasoning as
/// `Store::ocean_stats_scoped`). The account is resolved via
/// `Store::graph_account_id` — migration 0084's audited
/// `resolve_graph_account_id` SECURITY DEFINER bootstrap, deliberately left
/// unscoped because it's how that account is discovered in the first place.
/// An ocean whose account can't be resolved degrades exactly like an ocean
/// whose vector/keyword search errored today (G25) — never a hard failure of
/// the whole query.
///
/// `user_id` (tenant RLS Phase 2): the gate-verified acting caller —
/// `atoms`/`nodes` are `graph_id`-anchored, covered tables on
/// `migrations_tenant/0008_tenant_rls`. Before this parameter existed, this
/// function ran `begin_account_scoped` uniformly for BOTH planes, which meant
/// an Org-decision call set `app.account_id` only — a silent Phase-3 gap for
/// this exact retrieval hot path (a member ocean's atoms would have gone
/// invisible to retrieval the moment the tenant DSN cut over).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn multi_graph_retrieve(
    store: &dyn Store,
    pool: Option<&sqlx::PgPool>,
    user_id: Uuid,
    graph_ids: &[Uuid],
    embedding: &[f32],
    search_text: &str,
    filter: &RetrievalFilter,
    top_k: usize,
) -> MultiRetrieval {
    // ── Per-ocean fan-out (parallel) ──────────────────────────────────────────
    let mut tasks = FuturesUnordered::new();
    for &gid in graph_ids {
        let filter = filter.clone();
        tasks.push(async move {
            // Captured so the post-fan-out phases below (graph expansion,
            // timestamps, supersession) can scope to the SAME account without
            // re-issuing `graph_account_id` per graph a second time. `None` on
            // pool-less backends, where there is nothing to scope.
            let mut resolved_account: Option<Uuid> = None;
            // Retrieval first, then currents: hop-0 seeds include reefs of
            // the corals we already found. Traversal is ALWAYS computed
            // (shadow mode); merge vs log is decided after the fan-out.
            let (v, k, n) = match pool {
                Some(pool) => {
                    let account_id = match store.graph_account_id(gid).await {
                        Ok(Some(a)) => a,
                        _ => return Err(gid),
                    };
                    resolved_account = Some(account_id);
                    tokio::join!(
                        scoped_vector_search(store, pool, account_id, user_id, gid, embedding, &filter),
                        scoped_keyword_search(store, pool, account_id, user_id, gid, search_text, &filter),
                        scoped_keyword_search_node_atoms(
                            store, pool, account_id, user_id, gid, search_text, &filter
                        ),
                    )
                }
                None => tokio::join!(
                    store.vector_search(gid, embedding, RETRIEVAL_CANDIDATE_K, &filter),
                    store.keyword_search(gid, search_text, RETRIEVAL_CANDIDATE_K, &filter),
                    store.keyword_search_node_atoms(gid, search_text, NODE_CANDIDATE_K, &filter),
                ),
            };
            let mut hit_atom_ids = Vec::new();
            if let Ok(ref vv) = v {
                hit_atom_ids.extend(vv.iter().map(|(id, _)| *id));
            }
            if let Ok(ref kk) = k {
                hit_atom_ids.extend(kk.iter().map(|(id, _)| *id));
            }
            if let Ok(ref nn) = n {
                hit_atom_ids.extend(nn.iter().map(|(id, _)| *id));
            }
            hit_atom_ids.sort_unstable();
            hit_atom_ids.dedup();
            let t = traverse_currents(
                store,
                pool,
                user_id,
                resolved_account,
                gid,
                search_text,
                embedding,
                &filter,
                &hit_atom_ids,
            )
            .await;
            // An ocean counts as reachable only if BOTH primary paths succeeded;
            // a hard error on either degrades that ocean (G25) rather than the query.
            match (v, k) {
                (Ok(v), Ok(k)) => {
                    let n = n.unwrap_or_default();
                    Ok((gid, v, merge_keyword_hits(k, n), resolved_account, t))
                }
                _ => Err(gid),
            }
        });
    }

    let mut vector_hits: Vec<(Uuid, f32)> = Vec::new();
    let mut keyword_hits: Vec<(Uuid, f64)> = Vec::new();
    let mut atom_graph: HashMap<Uuid, Uuid> = HashMap::new();
    let mut unreachable: Vec<Uuid> = Vec::new();
    let mut account_by_graph: HashMap<Uuid, Uuid> = HashMap::new();
    // §5.5 shadow mode: every graph's currents-traversal result, ALWAYS
    // collected regardless of `graph_traversal_enabled()` — the flag decides
    // what happens to it below, never whether it gets computed.
    let mut traversal_by_graph: HashMap<Uuid, CurrentsTraversal> = HashMap::new();

    while let Some(res) = tasks.next().await {
        match res {
            Ok((gid, v, k, account_id, t)) => {
                if let Some(a) = account_id {
                    account_by_graph.insert(gid, a);
                }
                for (id, _) in &v {
                    atom_graph.entry(*id).or_insert(gid);
                }
                for (id, _) in &k {
                    atom_graph.entry(*id).or_insert(gid);
                }
                vector_hits.extend(v);
                keyword_hits.extend(k);
                traversal_by_graph.insert(gid, t);
            }
            Err(gid) => unreachable.push(gid),
        }
    }

    // §7.4 trace: raw hit counts, before dedup/rerank. Captured BEFORE the
    // currents-traversal merge just below — these two fields describe the
    // RAW vector/keyword pool (§7.4's own doc comment), and traversal-added
    // atoms get their own dedicated `trace.traversal_added` field rather
    // than inflating this one.
    let trace_vector_hits = vector_hits.len();
    let trace_keyword_hits = keyword_hits.len();

    // ── §3.11 absolute vsim gate ────────────────────────────────────────────
    //
    // The honesty floor (`ask_relevance_floor`, above) is RELATIVE: it fires
    // on the BLENDED score, whose keyword half (`knorm`) is normalised
    // against the best score IN THE RESULT SET, so a uniformly-bad candidate
    // set normalises its way past it. Starting from a small/empty ocean,
    // `Store::vector_search`'s `LIMIT top_k` returns its `k` nearest
    // neighbours REGARDLESS of whether any of them are actually close — a
    // 50-coral ocean at `top_k=20` returns 40% of the corpus with nothing
    // filtered. This gate is the second, independent, and ABSOLUTE half of
    // §3.11's fix: applied directly to the RAW, unblended, unnormalised
    // cosine similarity `Store::vector_search` returned, before anything
    // enters `rerank`/blending, and entirely independent of the relative
    // keyword-normalisation path — a strong keyword-only hit (e.g. a reef
    // name match via `keyword_search_node_atoms`) never had a `vsim` to gate
    // on in the first place, so this can never suppress one. §3.11
    // explicitly rejects any ocean-size scaling here: cosine similarity is
    // corpus-size-independent by construction, so one absolute regime
    // applies at every ocean size, matching what §7.1's fixture ocean
    // exercises.
    let vsim_gate = ask_vsim_gate();
    let vsim_gate_before = vector_hits.len();
    vector_hits.retain(|&(_, dist)| (1.0_f64 - dist as f64 / 2.0).clamp(0.0, 1.0) >= vsim_gate);
    let trace_vsim_gate = Some(GateTrace {
        before: vsim_gate_before,
        after: vector_hits.len(),
    });

    // ── Currents traversal: apply for real, or hold for shadow-only (§5.5) ────
    //
    // `GRAPH_TRAVERSAL_ENABLED` gates ALL real behavior change, exactly like
    // `SUPERSESSION_RANKING_ENABLED` (see `graph_traversal_enabled`'s doc
    // comment). OFF (the default): `all_traversal_atoms` is computed (needed
    // for the shadow-diff log below) but `keyword_hits`/`atom_graph` are
    // NEVER touched — every downstream computation (`seed_ids`, `expand()`,
    // supersession, `rerank`, the floor, the cap, `selected`) is therefore
    // byte-identical to a build with no currents-traversal code at all. ON:
    // merged into the REAL pool here, BEFORE `seed_ids` is computed below, so
    // a hopped-in atom participates in graph expansion / supersession
    // annotation / reranking exactly like any other candidate from this point
    // on — it is a genuine new entry to the candidate pool, not a bolt-on
    // appended after the fact.
    let all_traversal_atoms: Vec<(Uuid, f64)> = traversal_by_graph
        .values()
        .flat_map(|t| t.candidate_atoms.iter().cloned())
        .collect();
    let traversal_enabled = graph_traversal_enabled();
    if traversal_enabled && !all_traversal_atoms.is_empty() {
        for (&gid, t) in &traversal_by_graph {
            for (id, _) in &t.candidate_atoms {
                atom_graph.entry(*id).or_insert(gid);
            }
        }
        keyword_hits = merge_keyword_hits(keyword_hits, all_traversal_atoms.clone());
    }

    // ── Per-ocean graph expansion ─────────────────────────────────────────────
    let mut seed_ids: Vec<Uuid> = vector_hits
        .iter()
        .map(|(id, _)| *id)
        .chain(keyword_hits.iter().map(|(id, _)| *id))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    seed_ids.sort();

    // Group seeds by their ocean so each expand() stays correctly graph-scoped.
    let mut seeds_by_graph: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for &aid in &seed_ids {
        if let Some(&gid) = atom_graph.get(&aid) {
            seeds_by_graph.entry(gid).or_default().push(aid);
        }
    }
    let expand_results = futures::future::join_all(seeds_by_graph.iter().map(|(gid, seeds)| {
        // Reuse the account the fan-out already resolved for this graph.
        let scope = pool.zip(account_by_graph.get(gid).copied());
        async move {
            let scope = scope.map(|(p, a)| (p, a, user_id));
            (
                *gid,
                expand_filtered(store, scope, *gid, seeds, filter, Some(embedding)).await,
            )
        }
    }))
    .await;

    let mut nodes: Vec<NodeWithAtoms> = Vec::new();
    let mut node_graph: HashMap<Uuid, Uuid> = HashMap::new();
    for (gid, ns) in expand_results {
        for n in &ns {
            node_graph.insert(n.node_id, gid);
        }
        nodes.extend(ns);
    }
    let node_map: HashMap<Uuid, Uuid> = nodes
        .iter()
        .flat_map(|n| n.atom_ids.iter().map(move |&aid| (aid, n.node_id)))
        .collect();
    // Scoped PER ACCOUNT, not per call: `seed_ids` can span several oceans and
    // therefore several accounts (D9 Model B), and one `begin_tenant_scoped`
    // transaction only ever sees the account it set. Grouping keeps every atom
    // visible; the single-ocean case (the overwhelming majority) is exactly one
    // group, i.e. one query, as before.
    let timestamps = match pool {
        Some(_) => {
            let mut by_account: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
            let mut unscopable: Vec<Uuid> = Vec::new();
            for &aid in &seed_ids {
                match atom_graph
                    .get(&aid)
                    .and_then(|gid| account_by_graph.get(gid).copied())
                {
                    Some(account_id) => by_account.entry(account_id).or_default().push(aid),
                    None => unscopable.push(aid),
                }
            }
            if !unscopable.is_empty() {
                tracing::warn!(
                    count = unscopable.len(),
                    "fetch_timestamps: candidates with no resolvable account — \
                     their recency signal degrades"
                );
            }
            let groups = futures::future::join_all(by_account.iter().map(|(account_id, ids)| {
                let scope = pool.map(|p| (p, *account_id, user_id));
                async move { fetch_timestamps(store, scope, ids).await }
            }))
            .await;
            groups.into_iter().flatten().collect()
        }
        None => fetch_timestamps(store, None, &seed_ids).await,
    };

    // ── Supersession lookup (Phase 5b — supersession-aware ranking) ───────────
    //
    // Batched and scoped to just the candidate pool's owning nodes (the
    // reefs graph expansion actually surfaced), never the whole graph — a
    // large ocean must not pay for a full-table scan on every retrieval call.
    //
    // GRANULARITY LIMITATION (documented, not silently over-broad): the
    // audit that spawned this phase confirmed `supersession_edge` connects
    // REEFS (nodes), not individual corals (atoms) — see migration
    // `0011_nodes.sql`, `superseded_id`/`superseder_id` both reference
    // `nodes(node_id)`. A node can hold many atoms via the many-to-many
    // `node_atoms` join, and only SOME of them may actually be the stale
    // claim a human meant when they superseded that reef. There is no
    // reliable per-atom signal to narrow this further today, so marking a
    // node superseded marks EVERY atom currently linked to it (via
    // `NodeWithAtoms::atom_ids`) — a deliberately conservative choice in
    // line with this phase's "penalty + annotation, never hard-exclude"
    // design: a false-broad penalty only costs ranking on an atom that
    // happens to share a reef with a superseded claim, never recall.
    let mut candidate_node_ids: Vec<Uuid> = nodes.iter().map(|n| n.node_id).collect();
    candidate_node_ids.sort_unstable();
    candidate_node_ids.dedup();
    // Scoped per account for the same reason as `fetch_timestamps` above:
    // candidate reefs can span oceans. A failure degrades supersession
    // ANNOTATION only (penalty + `superseded_by` labelling, never recall or
    // hard-exclusion — see the phase's design note above), so it stays
    // non-fatal; it is now logged instead of vanishing into `unwrap_or_default`.
    let supersession_pairs: Vec<SupersessionTarget> = match pool {
        Some(pool) => {
            let mut by_account: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
            let mut unscopable = 0usize;
            for &nid in &candidate_node_ids {
                match node_graph
                    .get(&nid)
                    .and_then(|gid| account_by_graph.get(gid).copied())
                {
                    Some(account_id) => by_account.entry(account_id).or_default().push(nid),
                    None => unscopable += 1,
                }
            }
            if unscopable > 0 {
                tracing::warn!(
                    count = unscopable,
                    "supersession lookup: candidate reefs with no resolvable account — \
                     their supersession annotation degrades"
                );
            }
            let groups = futures::future::join_all(by_account.iter().map(|(account_id, ids)| {
                // `pool` is bound by this arm, so the scope is always present —
                // no unscoped Postgres fallback to leave lying around here.
                async move {
                    scoped_list_supersession_targets(store, pool, *account_id, user_id, ids).await
                }
            }))
            .await;
            groups
                .into_iter()
                .filter_map(|r| match r {
                    Ok(pairs) => Some(pairs),
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "list_supersession_targets failed — supersession annotation degrades"
                        );
                        None
                    }
                })
                .flatten()
                .collect()
        }
        None => store
            .list_supersession_targets(&candidate_node_ids)
            .await
            .unwrap_or_default(),
    };
    // ── Cross-ocean guard on the SUPERSEDER (member-private oceans) ───────────
    //
    // `list_supersession_targets` matches on `superseded_id` ONLY — it places
    // no constraint at all on which ocean the SUPERSEDER node lives in, and
    // `supersession_edge` carries no same-graph CHECK. `POST /v1/nodes/{id}/
    // supersede` authorizes both endpoints with `node_owned_by_account`, which
    // in an org tenant is satisfied by EVERY member-private ocean at once (they
    // all share the one org account), so an edge reading "<a reef I can reach>
    // is superseded by <another member's private reef>" is constructible
    // through the public API today — by that member herself, who needs no id
    // she doesn't already hold.
    //
    // Without this filter that superseder's node id travels straight back to
    // the caller: `search_atoms`' per-result `superseded_by` field, and (with
    // an LLM configured) `ask_ocean`'s `citation_details[].superseded_by` plus
    // the `[SUPERSEDED — see reef <id>]` prose annotation. That is an
    // identifier from an ocean the caller cannot reach, disclosed without a
    // single byte of that ocean's CONTENT appearing anywhere in the response —
    // so the marker-based leak assertions in
    // `backend/tests/member_ocean_probe_pg.rs` are structurally blind to it.
    // `supersession_never_discloses_a_superseder_reef_from_an_unreachable_ocean`
    // in that file is the regression probe, and it fails without these lines.
    //
    // Same predicate and same rationale as `citation_details`' tenant-guard
    // step: keep only ids whose home graph is in the caller's authorized scope
    // (`graph_ids` is the already-reachability-checked set the caller passes).
    // Fails CLOSED: a superseder whose graph can't be resolved is
    // dropped, costing at most a ranking annotation, never a disclosure.
    //
    // Scope of the fix, stated honestly: this closes the READ-side disclosure
    // only. `POST /v1/nodes/{id}/supersede` can still CREATE the cross-ocean
    // edge (its `node_owned_by_account` check remains account-level, not
    // reachability-level) — that write-side authorization gap is recorded in
    // this PR's body, not fixed here.
    //
    // RLS (org-tenancy OPS-4): `node_graph_id` runs UNSCOPED, matching the
    // `list_supersession_targets` call immediately above it that produced these
    // ids — both are part of the retrieval core's documented pool-less
    // remainder (`services::ask`'s module doc). It is also a pure id → graph_id
    // resolution, the same role `graph_account_id`/`atom_graph_id` play in the
    // neighbouring scoped helpers, and its result is only ever used to NARROW.
    let supersession_pairs = if supersession_pairs.is_empty() {
        supersession_pairs
    } else {
        let mut superseder_ids: Vec<Uuid> =
            supersession_pairs.iter().map(|t| t.superseder_id).collect();
        superseder_ids.sort_unstable();
        superseder_ids.dedup();
        // `node_graph` already maps every EXPANSION node to its graph, and was
        // built purely from `graph_ids`, so a superseder that graph expansion
        // already surfaced is resolvable for free. Only the remainder — a
        // superseder reef no in-scope expansion touched, which is exactly the
        // cross-ocean case this guard exists for — costs a round trip
        // (CodeRabbit, PR #348). The `graph_ids.contains` re-check on the
        // cached arm is trivially true today and kept deliberately: it means
        // the predicate is stated once, in one place, and stays correct if
        // `node_graph` ever gains an entry from outside `graph_ids`.
        let mut in_scope: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
        let mut unresolved: Vec<Uuid> = Vec::new();
        for nid in superseder_ids {
            match node_graph.get(&nid) {
                Some(gid) => {
                    if graph_ids.contains(gid) {
                        in_scope.insert(nid);
                    }
                }
                None => unresolved.push(nid),
            }
        }
        if !unresolved.is_empty() {
            let resolved: Vec<Uuid> =
                futures::stream::iter(unresolved.into_iter().map(|nid| async move {
                    match store.node_graph_id(nid).await {
                        Ok(Some(gid)) if graph_ids.contains(&gid) => Some(nid),
                        _ => None,
                    }
                }))
                .buffer_unordered(SUPERSEDER_SCOPE_LOOKUP_CONCURRENCY)
                .filter_map(|id| async move { id })
                .collect()
                .await;
            in_scope.extend(resolved);
        }
        supersession_pairs
            .into_iter()
            .filter(|t| in_scope.contains(&t.superseder_id))
            .collect()
    };
    // superseded_node_id -> every SupersessionTarget superseding it (§3.6:
    // widened from a plain `HashMap<Uuid, Uuid>` `.collect()`, which silently
    // kept "an arbitrary one" whenever a node was superseded by more than
    // one successor — real data loss that §3.4's auto-detection turns from
    // rare into common. This map still only drives a ranking
    // penalty/annotation, never the graph's own supersession-edge source of
    // truth.)
    let mut superseded_node_map: HashMap<Uuid, Vec<SupersessionTarget>> = HashMap::new();
    for t in supersession_pairs {
        superseded_node_map.entry(t.superseded_id).or_default().push(t);
    }
    // `Store::expand`'s own step 3 ("superseder atoms injected under the
    // superseded node" — see its doc comment) deliberately COPIES a
    // superseder node's atoms into its superseded sibling's `atom_ids`, so a
    // client browsing the outdated reef also sees the replacement content.
    // That means `NodeWithAtoms::atom_ids` is NOT a reliable "genuine
    // node_atoms membership" signal for the superseded side — naively
    // walking it would mark the SUPERSEDING atom as superseded too.
    //
    // Exclude those injected copies using `NodeWithAtoms::injected_atom_ids`
    // — a real provenance signal `Store::expand` stamps per node (see its
    // doc comment), not a derived/inferred one. Earlier this excluded atoms
    // by cross-referencing the superseder node's OWN entry in `nodes`
    // (i.e. treating "genuinely belongs to whatever node in `nodes` matches
    // `superseded_node_map`'s value" as the exclusion set) — that broke
    // whenever the superseder node itself wasn't independently a member of
    // `nodes` (e.g. retrieval only ever surfaced the superseded reef, never
    // a direct hit or expansion that pulled the superseder node in on its
    // own), silently letting the injected copy through as "genuine" and
    // marking the CURRENT/replacement atom as superseded. Per-node
    // `injected_atom_ids` needs no such cross-reference: it's authoritative
    // regardless of what else is or isn't present in `nodes`.
    // `reef_title` on each `SupersededClaim` names the SUPERSEDING reef — its
    // title IS the claim that replaced this one (§3.3). Built once here
    // rather than per-atom; per the plan's own note, this needs no new query
    // since `nodes` already carries every title `expand()` surfaced. A
    // superseder whose own node never separately appears in `nodes` (see the
    // doc comment on `Store::expand`'s step 3, just above) falls back to a
    // placeholder rather than panicking or silently dropping the claim — the
    // spec's "no new query needed" is the common case, not a guarantee.
    let node_titles_for_superseded = node_title_map(&nodes);
    let mut superseded_atoms: HashMap<Uuid, Vec<SupersededClaim>> = HashMap::new();
    if !superseded_node_map.is_empty() {
        for n in &nodes {
            if let Some(targets) = superseded_node_map.get(&n.node_id) {
                let claims: Vec<SupersededClaim> = targets
                    .iter()
                    .map(|t| SupersededClaim {
                        reef_title: node_titles_for_superseded
                            .get(&t.superseder_id)
                            .cloned()
                            .unwrap_or_else(|| format!("(untitled reef {})", t.superseder_id)),
                        superseder_id: t.superseder_id,
                        kind: t.kind,
                        until: t.superseded_on,
                    })
                    .collect();
                let injected: std::collections::HashSet<Uuid> =
                    n.injected_atom_ids.iter().copied().collect();
                for &aid in &n.atom_ids {
                    if injected.contains(&aid) {
                        continue; // an expand()-injected display copy, not a genuine member
                    }
                    superseded_atoms.entry(aid).or_insert_with(|| claims.clone());
                }
            }
        }
    }
    let ranking_enabled = supersession_ranking_enabled();
    // ADR-0014: when packing is live, demotion is the Score — do not also
    // apply the ×0.5 rerank penalty. Force-include + `[SUPERSEDED]` stay.
    let packing_live = crate::decision::JevAskPathMode::from_env().is_live();
    let apply_penalty = ranking_enabled && !packing_live;

    // ── Global rerank over the merged pool (no per-ocean quota) ────────────────
    // Rerank the WHOLE candidate pool (not just top_k) so the per-ocean floor can
    // reach below the global cut for a minority ocean's best source.
    let rerank_k = vector_hits.len() + keyword_hits.len();
    let mut ranked_all = rerank(
        &vector_hits,
        &keyword_hits,
        &timestamps,
        &node_map,
        &RerankParams::default(),
        rerank_k,
    );
    // §7.4 trace: deduplicated candidate count entering rerank.
    let trace_merged = ranked_all.len();

    // ── Supersession penalty (Phase 5b) ────────────────────────────────────────
    //
    // `SUPERSESSION_RANKING_ENABLED` gates ALL real behavior change — real
    // `supersession_edge` rows can already exist in production today (the
    // pre-existing manual `POST /v1/nodes/{id}/supersede` endpoint), so this
    // is a genuine, immediate production behavior change once flipped, not a
    // theoretical one behind another flag. **Default OFF.**
    //
    // OFF: compute what the penalty WOULD have done on a throwaway clone and
    // log it, but `ranked_all` itself is never touched — every downstream
    // computation (floor, cap, per-ocean guarantee, `selected`) is therefore
    // byte-identical to before this phase existed.
    // ON: penalize `ranked_all` in place (mutates + re-sorts), so every
    // downstream computation sees the discounted scores for real.
    //
    // §7.4 trace: count of candidates ACTUALLY IN the merged pool that carry
    // a superseded id — not `superseded_atoms.len()`, which is scoped to
    // every expanded node and can include ids `rerank()` never surfaced.
    // Counted before `apply_supersession_penalty` mutates/re-sorts
    // `ranked_all`; the count itself is unaffected by that (no removal).
    let mut trace_supersession_demoted = 0usize;
    if !superseded_atoms.is_empty() {
        let superseded_ids: std::collections::HashSet<Uuid> =
            superseded_atoms.keys().copied().collect();
        if apply_penalty {
            trace_supersession_demoted = ranked_all
                .iter()
                .filter(|a| superseded_ids.contains(&a.atom_id))
                .count();
            apply_supersession_penalty(
                &mut ranked_all,
                &superseded_ids,
                SUPERSESSION_PENALTY_MULTIPLIER,
            );
        } else if packing_live {
            tracing::info!(
                superseded_candidate_count = superseded_ids.len(),
                "supersession ×0.5 penalty skipped — JEV_ASK_PATH_MODE=live; demotion is the pack Score"
            );
        } else {
            let floor = ask_relevance_floor();
            let does_clear = ranked_all.iter().any(|a| a.score >= floor);
            let mut shadow = ranked_all.clone();
            apply_supersession_penalty(
                &mut shadow,
                &superseded_ids,
                SUPERSESSION_PENALTY_MULTIPLIER,
            );
            let would_clear = shadow.iter().any(|a| a.score >= floor);
            tracing::info!(
                superseded_candidate_count = superseded_ids.len(),
                penalty_multiplier = SUPERSESSION_PENALTY_MULTIPLIER,
                would_change_any_cleared_floor = would_clear != does_clear,
                "supersession_ranking (SHADOW — SUPERSESSION_RANKING_ENABLED is off): would have \
                 penalized {} candidate atom(s); no ranking/synthesis/citation change applied",
                superseded_ids.len(),
            );
        }
    }

    let floor = ask_relevance_floor();
    let any_cleared_floor = ranked_all.iter().any(|a| a.score >= floor);
    // §7.4 trace: the honesty guard is all-or-nothing (§3.11), not a per-atom
    // filter — `candidates` is the pool it was evaluated against, `cleared`
    // is the outcome. Evaluated on the (possibly supersession-demoted) pool,
    // matching `any_cleared_floor` above exactly.
    let trace_blended_floor = BlendedFloorTrace {
        candidates: ranked_all.len(),
        cleared: any_cleared_floor,
    };
    // Shadow mode (searchfix relevance-floor-shadow): log-only, no behavior
    // change. Shared by every `multi_graph_retrieve` caller.
    log_shadow_floor_disagreement(
        None,
        graph_ids,
        &ranked_all.iter().map(|a| a.score).collect::<Vec<f64>>(),
    );

    // ── Ocean-count-scaled context cap + per-ocean floor guarantee ─────────────
    let cap = synthesis_context_cap(top_k, graph_ids.len());
    let mut selected: Vec<RankedAtom> = ranked_all.iter().take(cap).cloned().collect();
    // §7.4 trace: the cut itself, before the per-ocean floor guarantee below
    // (and supersession force-include further down) can push `selected` back
    // past `cap` again.
    let trace_context_cap = GateTrace {
        before: ranked_all.len(),
        after: selected.len(),
    };
    let mut chosen: std::collections::HashSet<Uuid> = selected.iter().map(|a| a.atom_id).collect();

    // Per-ocean floor (A5): every in-scope ocean that has a floor-clearing hit
    // contributes at least its single best source to the synthesis set, even if
    // global reranking pushed it past the cap. This is what lets a minority ocean
    // holding the contradicting fact survive into contradiction detection.
    if graph_ids.len() > 1 {
        for &gid in graph_ids {
            let already = selected
                .iter()
                .any(|a| atom_graph.get(&a.atom_id) == Some(&gid));
            if already {
                continue;
            }
            if let Some(best) = ranked_all
                .iter()
                .find(|a| a.score >= floor && atom_graph.get(&a.atom_id) == Some(&gid))
            {
                if chosen.insert(best.atom_id) {
                    selected.push(best.clone());
                }
            }
        }
        // Keep the set in global relevance order (citation numbering follows rank).
        selected.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    // §7.4 trace: atoms the per-ocean floor guarantee pulled back in past the
    // cap. Always `0` for a single-ocean query — a true zero (the block
    // above is skipped entirely), not an absent gate.
    let trace_per_ocean_floor_added = selected.len().saturating_sub(trace_context_cap.after);

    // ── Force-include the superseder + honesty-guard input (Phase 5b, ON only) ─
    //
    // Mirrors the per-ocean floor guarantee just above: every superseded atom
    // that survived into `selected` pulls its superseding reef's best-ranked
    // candidate atom into the set too — even past the cap — so the
    // synthesizer always sees the replacement fact alongside the outdated
    // one it's citing. Best-effort only: if the superseding reef has no atom
    // anywhere in this candidate pool (it wasn't matched by vector/keyword
    // search at all), its id is still surfaced via the `[SUPERSEDED — see
    // reef <id>]` annotation / citation `superseded_by` field, just without
    // force-included content in THIS retrieval pass.
    let mut all_surviving_superseded = false;
    if ranking_enabled && !superseded_atoms.is_empty() {
        let atom_reefs_local = atom_reef_map(&nodes);
        // §3.6: an atom can now carry MULTIPLE superseders — flat_map every
        // one of them in, not just the first, so the force-include step
        // below pulls in every superseding reef's best atom, not an
        // arbitrary one.
        let mut superseder_nodes: Vec<Uuid> = selected
            .iter()
            .flat_map(|a| {
                superseded_atoms
                    .get(&a.atom_id)
                    .into_iter()
                    .flatten()
                    .map(|c| c.superseder_id)
            })
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        superseder_nodes.sort_unstable();
        for superseder_node in superseder_nodes {
            let already = selected.iter().any(|a| {
                atom_reefs_local
                    .get(&a.atom_id)
                    .is_some_and(|v| v.contains(&superseder_node))
            });
            if already {
                continue;
            }
            if let Some(best) = ranked_all.iter().find(|a| {
                atom_reefs_local
                    .get(&a.atom_id)
                    .is_some_and(|v| v.contains(&superseder_node))
            }) {
                if chosen.insert(best.atom_id) {
                    selected.push(best.clone());
                }
            }
        }
        selected.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Honesty-guard input: true only when the floor-clearing subset of
        // `selected` is non-empty AND every entry in it is known-superseded
        // — i.e. removing superseded atoms from consideration would leave
        // zero above-floor results to answer from.
        let floor_clearing: Vec<&RankedAtom> =
            selected.iter().filter(|a| a.score >= floor).collect();
        all_surviving_superseded = !floor_clearing.is_empty()
            && floor_clearing
                .iter()
                .all(|a| superseded_atoms.contains_key(&a.atom_id));
    }

    // ── §3.6 `CLAIMS` block: distilled non-entity assertions ───────────────────
    //
    // Completes D2's `ENTITIES` narrowing — `build_synthesis_reefs` restricts
    // `ENTITIES` to `type = "entity"`; everything else (decision/thread/
    // reference reefs, whose titles ARE assertions per §3.3) competes for this
    // block instead. Computed HERE, not alongside `build_synthesis_reefs`
    // (which runs AFTER this function returns, from `mgr.reranked` alone) —
    // citation integrity requires force-including a claim's own atom past the
    // CONTEXT cap, which needs `ranked_all`/`selected`/`chosen`, the same
    // uncapped state the supersession force-include just above is already
    // holding.
    //
    // Candidate selection is UNCONDITIONAL (D2/§3.3 ship regardless of
    // `SUPERSESSION_RANKING_ENABLED`); only the never-orphan expansion and the
    // `superseded` annotation on each claim are gated on `ranking_enabled`,
    // mirroring `SynthesisAtom::superseded`'s own contract.
    let node_titles = node_titles_for_superseded;
    let mut rank_of_atom: HashMap<Uuid, usize> = HashMap::new();
    for (i, a) in ranked_all.iter().enumerate() {
        rank_of_atom.entry(a.atom_id).or_insert(i);
    }
    let mut claim_scored: Vec<(usize, Uuid)> = nodes
        .iter()
        .filter(|n| n.node_type != "entity")
        .filter_map(|n| {
            let best_rank = n
                .atom_ids
                .iter()
                .filter_map(|a| rank_of_atom.get(a))
                .min()
                .copied()?;
            Some((best_rank, n.node_id))
        })
        .collect();
    claim_scored.sort_by_key(|(rank, _)| *rank);
    let mut claim_node_ids: Vec<Uuid> = claim_scored
        .into_iter()
        .take(SYNTHESIS_CLAIMS_CAP)
        .map(|(_, id)| id)
        .collect();

    // Never-orphan invariant (hard requirement, §3.6): a superseded claim
    // reaching the block must never appear without its superseder — pull any
    // missing superseder in now, past the cap if needed. Only meaningful (and
    // only run) while `ranking_enabled`, since that's what makes "superseded"
    // visible at all — with the flag off, `superseded_node_map` has no live
    // reader, matching `superseded_atoms`'s own contract above.
    if ranking_enabled {
        let mut missing_superseders: Vec<Uuid> = Vec::new();
        for &nid in &claim_node_ids {
            if let Some(targets) = superseded_node_map.get(&nid) {
                for t in targets {
                    if !claim_node_ids.contains(&t.superseder_id)
                        && !missing_superseders.contains(&t.superseder_id)
                    {
                        missing_superseders.push(t.superseder_id);
                    }
                }
            }
        }
        claim_node_ids.extend(missing_superseders);
    }

    // Citation integrity (§3.6): a claim is NOT citable itself — the output
    // contract requires an `atom_id` per `[n]` marker, so every claim reaching
    // the block force-includes its highest-ranked source coral into `selected`
    // (`CONTEXT`), past the cap if needed. Deliberately UNCONDITIONAL (not
    // gated on `ranking_enabled`) — this is basic block hygiene, not
    // supersession-specific. "Highest-ranked" is `ranked_all`'s own order
    // (the same global rerank the atom-level force-include above already
    // trusts) — §11 leaves "which coral is best" undecided beyond that.
    //
    // Watch: like the supersession force-include above, this can push
    // `selected` past `synthesis_context_cap`'s greedy token budget — the
    // trade this phase makes (an under-cited claim is worse than a slightly
    // larger prompt) is the same one already accepted for the superseder
    // force-include; `build_user_message`'s OWN token budget is the final,
    // separate backstop that truncates `CONTEXT` if the combined set is too
    // large for the model.
    if !claim_node_ids.is_empty() {
        let atom_reefs_for_claims = atom_reef_map(&nodes);
        for &claim_node in &claim_node_ids {
            let already = selected.iter().any(|a| {
                atom_reefs_for_claims
                    .get(&a.atom_id)
                    .is_some_and(|v| v.contains(&claim_node))
            });
            if already {
                continue;
            }
            // Best-effort: a claim reef with no atom anywhere in this
            // candidate pool still renders in `CLAIMS` (the title alone is
            // useful context) — just without a force-included source coral.
            // The block's own framing ("ground any citation on the CONTEXT
            // atom(s) below") is what keeps the model from citing it directly
            // in that rare case.
            if let Some(best) = ranked_all.iter().find(|a| {
                atom_reefs_for_claims
                    .get(&a.atom_id)
                    .is_some_and(|v| v.contains(&claim_node))
            }) {
                if chosen.insert(best.atom_id) {
                    selected.push(best.clone());
                }
            }
        }
        selected.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    let claims: Vec<SynthesisClaim> = claim_node_ids
        .iter()
        .filter_map(|nid| {
            let title = node_titles.get(nid)?.clone();
            let node = nodes.iter().find(|n| &n.node_id == nid);
            let asserted_at = node.and_then(|n| {
                n.atom_ids
                    .iter()
                    .filter_map(|a| rank_of_atom.get(a).map(|r| (*r, a)))
                    .min_by_key(|(r, _)| *r)
                    .and_then(|(_, aid)| timestamps.get(aid).copied())
            });
            let superseded = if ranking_enabled {
                superseded_node_map
                    .get(nid)
                    .map(|targets| {
                        targets
                            .iter()
                            .map(|t| SupersededClaim {
                                reef_title: node_titles
                                    .get(&t.superseder_id)
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        format!("(untitled reef {})", t.superseder_id)
                                    }),
                                superseder_id: t.superseder_id,
                                kind: t.kind,
                                until: t.superseded_on,
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            Some(SynthesisClaim {
                title,
                asserted_at,
                superseded,
            })
        })
        .collect();

    // §7.4 trace: §5.4 currents traversal gate. `enabled` mirrors
    // `SupersessionPenTrace::ranking_enabled` exactly, for the identical
    // reason — while `GRAPH_TRAVERSAL_ENABLED` is off, `added`/`reefs`/`hop`
    // are REAL zeros (the merge above never ran, so nothing was ever
    // eligible to be added), never the shadow-computed "what traversal
    // found" numbers logged separately below. `added` counts DISTINCT atom
    // ids actually merged into `keyword_hits` (dedup, since a hopped atom
    // could in principle be reachable from more than one graph's traversal);
    // `reefs` is the hopped-reef count that contributed them; `hop` is the
    // configured max depth ([`MAX_HOPS`]) the walk that produced them ran to.
    let trace_traversal_added = if traversal_enabled && !all_traversal_atoms.is_empty() {
        let added_ids: std::collections::HashSet<Uuid> =
            all_traversal_atoms.iter().map(|(id, _)| *id).collect();
        let reefs: usize = traversal_by_graph
            .values()
            .map(|t| t.hopped_reefs.len())
            .sum();
        TraversalTrace {
            added: added_ids.len(),
            reefs,
            hop: MAX_HOPS as u32,
            enabled: true,
        }
    } else {
        TraversalTrace {
            added: 0,
            reefs: 0,
            hop: 0,
            enabled: traversal_enabled,
        }
    };

    // ── §7.4 retrieval trace ────────────────────────────────────────────────
    // Assembled from the counts captured at each gate above. `reef_matched`
    // stays `None` — that gate (§5.6, PR #424) is not implemented on `main`
    // yet (see the module-level note on `RetrievalTrace`). `vsim_gate` is now
    // `Some(_)` unconditionally — §3.11's gate is implemented and always
    // computed (no feature flag; see `ask_vsim_gate`'s doc comment for why).
    // `traversal_added` is `Some(_)` unconditionally now that §5.4 is
    // implemented — see `trace_traversal_added` above for the shadow-mode
    // contract its contents follow.
    let trace = RetrievalTrace {
        vector_hits: trace_vector_hits,
        keyword_hits: trace_keyword_hits,
        reef_matched: None,
        merged: trace_merged,
        vsim_gate: trace_vsim_gate,
        supersession_pen: SupersessionPenTrace {
            demoted: trace_supersession_demoted,
            ranking_enabled,
        },
        blended_floor: trace_blended_floor,
        context_cap: trace_context_cap,
        per_ocean_floor_added: trace_per_ocean_floor_added,
        traversal_added: Some(trace_traversal_added),
    };
    log_retrieval_trace(None, graph_ids, &trace);

    // ── §5.5 shadow-mode diagnostic: currents traversal delta ─────────────────
    // Only while the flag is OFF — when it's ON the merge already happened
    // for real (above, before graph expansion), so there is no "would have"
    // left to report. See `log_currents_traversal_shadow`'s doc comment for
    // exactly what is (and, per §5.5's own caveat, is NOT) claimed. This is
    // deliberately a SEPARATE diagnostic from `trace.traversal_added` above
    // (which stays real-zeroed while off) — the numbers this logs are the
    // shadow "what traversal would have found" ones `TraversalTrace`'s doc
    // comment says must never leak into the always-on trace.
    if !traversal_enabled && !all_traversal_atoms.is_empty() {
        log_currents_traversal_shadow(
            graph_ids,
            &vector_hits,
            &keyword_hits,
            &timestamps,
            &node_map,
            &ranked_all,
            selected.len(),
            cap,
            &all_traversal_atoms,
            &traversal_by_graph,
        );
    }

    MultiRetrieval {
        reranked: selected,
        ranked_pre_cap: ranked_all,
        atom_graph,
        nodes,
        node_graph,
        node_map,
        timestamps,
        unreachable,
        any_cleared_floor,
        // Only ever returned non-empty when the flag is ON (`ranking_enabled`).
        // With the flag off this is unconditionally empty regardless of what
        // `supersession_edge` actually holds — every downstream consumer can
        // therefore treat "empty map" as "nothing to do" without re-checking
        // the flag itself, which is what makes the OFF path byte-identical:
        // there is no live data for it to accidentally act on.
        superseded_atoms: if ranking_enabled {
            superseded_atoms
        } else {
            HashMap::new()
        },
        all_surviving_superseded,
        // §3.6: unconditional — see the `CLAIMS` computation above for why
        // this is NOT gated on `ranking_enabled` the way `superseded_atoms`
        // is (only the `superseded` annotation WITHIN each claim is).
        claims,
        // `multi_graph_retrieve` has no event window of its own — only
        // `apply_event_window_merge` populates this.
        window_titles: HashMap::new(),
        catalog_yes_reefs: HashSet::new(),
        trace,
        walk: merge_traversal_walks(&traversal_by_graph, traversal_enabled),
    }
}

/// Deterministic-timestamp-scan augmentation for event-time-scope intent
/// ("what happened today?", see [`event_time_window`]) — applied ON TOP OF an
/// already-computed [`multi_graph_retrieve`] outcome.
///
/// Root cause this closes: candidate generation for `ask_ocean`/`search_atoms`
/// and `POST /v1/queries/stream` is ALWAYS vector-cosine + Postgres FTS top-N
/// (`multi_graph_retrieve` above) — both are similarity/lexical gated against
/// the QUERY TEXT, so a same-day atom with no textual tie to the word "today"
/// (or to anything else in the question) is silently absent from the
/// candidate pool before rerank ever sees it; no amount of reranking or floor
/// tuning can recover an atom that was never fetched. When `event_window` is
/// `Some`, this additionally runs [`Store::timestamp_window_atoms`] — a plain
/// `atoms.timestamp` range scan, no similarity gate — per graph, and
/// force-merges every atom it finds into the retrieval outcome: into
/// `atom_graph`/`timestamps` for citation plumbing, into `nodes`/`node_map`
/// via `expand()` so reef membership and synthesis-reef context are populated
/// exactly like a normal candidate, and finally into `reranked` (the capped,
/// floor-cleared, synthesis-bound set) — bypassing the relevance floor/cap for
/// these atoms specifically, mirroring the per-ocean-floor-guarantee and
/// superseder force-include patterns already used inside
/// `multi_graph_retrieve` itself. An atom that ALSO cleared normal retrieval
/// keeps its real (higher) semantic rank; only genuinely new atoms are
/// appended.
///
/// `event_window` is `None` for the overwhelming majority of calls (any
/// question that isn't event-recap phrasing) — in that case this is a
/// pass-through returning `mgr` untouched.
///
/// **P0 fix, honesty-guard gating by tier (scale-readiness review
/// 2026-07-19):** `any_cleared_floor` is forced `true` whenever the scan
/// finds anything ONLY for [`EventWindowSource::Tier1`] windows — a
/// deterministic phrase match (pure `chrono` math against a known,
/// high-precision recap phrase like "what happened today?"), where trusting
/// the scan result unconditionally is correct: the query genuinely IS a
/// recap request. For [`EventWindowSource::Tier2`] windows (LLM-inferred —
/// can be a false positive on a non-recap question, e.g. misclassifying "who
/// is on call today?" as event-recap intent), the force-merged atoms still
/// enter the candidate pool (they can still be cited if actually relevant),
/// but `any_cleared_floor` is deliberately left untouched here — this
/// function runs strictly AFTER `multi_graph_retrieve` has already computed
/// it from the normal candidate pool's relevance scores, so "leave it alone"
/// literally means "let the existing floor-check logic decide", exactly as
/// it would for any non-event-window ask. A Tier-2-triggered scan whose hits
/// are genuinely irrelevant therefore still lets the honesty guard abstain
/// rather than fabricate (ADR-0011 G24 — see the "Casey's mom → invented
/// Jordan" fabrication-class comment near the honesty-guard call sites).
/// A coral the deterministic date scan contributed, represented by its title
/// rather than its body. `source_type` is the coral's REAL one — a title-only
/// coral still has to report accurately to `search_atoms` consumers.
#[derive(Debug, Clone)]
pub(crate) struct WindowCoral {
    pub(crate) title: String,
    pub(crate) source_type: String,
}

/// Bounded concurrency for `multi_graph_retrieve`'s superseder cross-ocean
/// guard, for the same reason as [`CITATION_SCOPE_LOOKUP_CONCURRENCY`]: the
/// number of supersession edges hanging off a candidate set is not bounded by
/// anything the retrieval path controls, so the node→graph resolution must not
/// fan out one query per edge unbounded.
const SUPERSEDER_SCOPE_LOOKUP_CONCURRENCY: usize = 8;

/// Invert a set of expanded reefs into `atom_id -> [node_id, ...]`: the
/// genuine many-to-many `node_atoms` membership already carried by
/// `NodeWithAtoms::atom_ids` (`Store::expand`'s per-node atom list), just
/// keyed the other way round. Used by both `citation_details` (ask_ocean
/// parity, fix 2) and `search_atoms` (multi-reef fix, fix 3) — no additional
/// DB round trip, since `nodes` is already fetched by the retrieval pipeline
/// both callers run.
pub(crate) fn atom_reef_map(nodes: &[NodeWithAtoms]) -> HashMap<Uuid, Vec<Uuid>> {
    let mut map: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for n in nodes {
        for &aid in &n.atom_ids {
            map.entry(aid).or_default().push(n.node_id);
        }
    }
    map
}

/// `node_id -> title` for the same `nodes` list `atom_reef_map` reads —
/// resolves the ids that map produces into human reef names.
pub(crate) fn node_title_map(nodes: &[NodeWithAtoms]) -> HashMap<Uuid, String> {
    nodes.iter().map(|n| (n.node_id, n.title.clone())).collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {

    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::{get, post},
        Router,
    };
    use tower::ServiceExt;

    // ── `source_types` on the wire (GitHub connector eval §5) ────────────────
    //
    // The request field is additive and `#[serde(default)]`: an old client
    // body with only `source_type` (or neither) still deserializes, and the
    // multi-valued list — when present and non-empty — is what retrieval
    // filters on. The precedence rule itself lives in
    // `RetrievalFilter::source_type_list` (unit-tested in `retrieval`); these
    // pin that both request structs carry the field through to it.

    #[test]
    fn stream_request_source_types_deserializes_and_wins_over_source_type() {
        let req: CreateQueryStreamRequest = serde_json::from_value(serde_json::json!({
            "text": "how does the backfill resume?",
            "graph_ids": [Uuid::new_v4().to_string()],
            "source_type": "github_pr",
            "source_types": ["github_code", "github_code_summary"],
        }))
        .expect("deserialize");
        let filter = RetrievalFilter {
            source_type: req.source_type.clone(),
            source_types: req.source_types.clone(),
            ..Default::default()
        };
        assert_eq!(
            filter.source_type_list(),
            Some(vec!["github_code".to_string(), "github_code_summary".to_string()])
        );
    }

    #[test]
    fn stream_request_without_source_types_keeps_the_single_source_type_filter() {
        let legacy: CreateQueryStreamRequest = serde_json::from_value(serde_json::json!({
            "text": "q",
            "source_type": "slack",
        }))
        .expect("deserialize legacy body");
        assert!(legacy.source_types.is_none(), "absent field must read as None");
        let filter = RetrievalFilter {
            source_type: legacy.source_type.clone(),
            source_types: legacy.source_types.clone(),
            ..Default::default()
        };
        assert_eq!(filter.source_type_list(), Some(vec!["slack".to_string()]));

        // The non-streaming request carries the same field.
        let plain: CreateQueryRequest = serde_json::from_value(serde_json::json!({
            "text": "q",
            "source_types": ["github_issue", "github_pr"],
        }))
        .expect("deserialize CreateQueryRequest");
        assert_eq!(
            plain.source_types,
            Some(vec!["github_issue".to_string(), "github_pr".to_string()])
        );
    }

    // ── `expand_filtered` project-boundary post-filter (P1-6, audit) ─────────
    //
    // `Store::expand` itself takes no filter — these tests prove the WRAPPER
    // closes the gap: an out-of-project sibling atom brought in only via
    // shared reef membership must never survive `expand_filtered` when a
    // project filter is active, and a reef left with NO in-project atom must
    // be dropped entirely (so its title/digest can never reach
    // `build_synthesis_reefs`/`atom_reef_map` either).

    // ── Relevance-floor SHADOW MODE (searchfix relevance-floor-shadow) ─────────
    //
    // Unit-level coverage of the shadow-vs-live disagreement logic against
    // synthetic scores (no live Postgres needed — the real-embedder threshold
    // search itself is a `--dump-scores` run, documented above
    // `ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT`). Also exercises the actual
    // top-1 scores from Phase 1's golden-set abstention category (a real
    // `EVAL_EMBEDDER=ort --dump-scores` run against the golden set, see
    // `backend/eval/golden/golden.jsonl`) as should-abstain regression fixtures,
    // per the Step 4 brief.


    #[test]
    fn ask_relevance_floor_default_is_unchanged() {
        // The single most important invariant of this PR: the LIVE floor must
        // not move. If this fails, something in this PR touched production
        // behavior — see `ASK_RELEVANCE_FLOOR_DEFAULT`.
        assert_eq!(ASK_RELEVANCE_FLOOR_DEFAULT, 0.02);
        std::env::remove_var("ASK_RELEVANCE_FLOOR");
        assert_eq!(ask_relevance_floor(), 0.02);
    }

    #[test]
    fn ask_relevance_floor_is_unaffected_by_the_shadow_env_var() {
        // Companion to `ask_relevance_floor_default_is_unchanged`: proves the
        // shadow knob genuinely cannot leak into the live gate even when it's
        // configured to something wildly different, e.g. a Postgres
        // e2e/CI/production environment that happens to have
        // `ASK_RELEVANCE_FLOOR_SHADOW` set for the diagnostic comparison.
        // Guards against exactly the "shadow computation isn't actually
        // inert" regression class this module's diagnostic logging must
        // never become.
        std::env::remove_var("ASK_RELEVANCE_FLOOR");
        std::env::set_var("ASK_RELEVANCE_FLOOR_SHADOW", "0.9");
        assert_eq!(
            ask_relevance_floor(),
            ASK_RELEVANCE_FLOOR_DEFAULT,
            "the live floor must not move no matter what ASK_RELEVANCE_FLOOR_SHADOW is set to"
        );
        assert_eq!(ask_relevance_floor_shadow(), 0.9);
        std::env::remove_var("ASK_RELEVANCE_FLOOR_SHADOW");
        assert_eq!(
            ask_relevance_floor_shadow(),
            ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT
        );
    }

    #[test]
    fn shadow_floor_disagreement_none_when_outcomes_agree() {
        // Both floors say "answer" (a comfortably-relevant hit) — no disagreement.
        assert!(shadow_floor_disagreement(&[0.9, 0.7], 0.02, 0.588).is_none());
        // Both floors say "abstain" (empty candidate set).
        assert!(shadow_floor_disagreement(&[], 0.02, 0.588).is_none());
        // Both floors say "abstain" (every score below both floors).
        assert!(shadow_floor_disagreement(&[0.01, 0.005], 0.02, 0.588).is_none());
    }

    #[test]
    fn shadow_floor_disagreement_flags_live_answer_shadow_abstain() {
        // The exact class this PR exists to surface: a weak/tangential hit
        // (e.g. 0.3) clears the near-inert live floor (0.02) but would NOT
        // clear the data-derived shadow floor (0.588).
        let d = shadow_floor_disagreement(&[0.3, 0.1], 0.02, 0.588)
            .expect("live=answer, shadow=abstain must disagree");
        assert!(d.live_would_answer);
        assert!(!d.shadow_would_answer);
        assert!((d.top_score - 0.3).abs() < 1e-9);
        assert_eq!(d.live_floor, 0.02);
        assert_eq!(d.shadow_floor, 0.588);
    }

    #[test]
    fn shadow_floor_disagreement_flags_live_abstain_shadow_answer() {
        // The inverse direction (would only arise if the shadow floor were ever
        // configured below the live floor) — the comparison logic must not
        // assume a fixed direction.
        let d = shadow_floor_disagreement(&[0.05], 0.1, 0.02)
            .expect("live=abstain, shadow=answer must disagree");
        assert!(!d.live_would_answer);
        assert!(d.shadow_would_answer);
    }

    #[test]
    fn shadow_floor_disagreement_empty_scores_is_abstain_under_any_floor() {
        // `reranked.is_empty()` is itself part of the honesty guard's "abstain"
        // condition in both call sites, independent of any per-atom score.
        assert!(!honesty_guard_would_answer(&[], 0.0));
        assert!(shadow_floor_disagreement(&[], 0.0, 0.0).is_none());
    }

    #[test]
    fn log_shadow_floor_disagreement_does_not_panic_without_request_id() {
        // `multi_graph_retrieve`'s call site has no request_id in scope;
        // `None` must be accepted cleanly (this just checks it doesn't panic —
        // tracing output isn't asserted here).
        log_shadow_floor_disagreement(None, &[Uuid::new_v4()], &[0.3]);
        log_shadow_floor_disagreement(Some("req-1"), &[Uuid::new_v4()], &[0.9]);
    }

    // ── §3.11 absolute vsim gate ─────────────────────────────────────────────
    //
    // `ASK_VSIM_GATE` is process-global env state; serialize every test that
    // touches it behind one mutex (mirrors `SUPERSESSION_ENV_GUARD` above).
    static VSIM_GATE_ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn ask_vsim_gate_default_matches_documented_threshold() {
        let _g = VSIM_GATE_ENV_GUARD.lock().unwrap();
        std::env::remove_var("ASK_VSIM_GATE");
        assert_eq!(ASK_VSIM_GATE_DEFAULT, 0.82);
        assert_eq!(ask_vsim_gate(), 0.82);
    }

    #[test]
    fn ask_vsim_gate_parses_env_override_defensively() {
        let _g = VSIM_GATE_ENV_GUARD.lock().unwrap();
        std::env::set_var("ASK_VSIM_GATE", "0.5");
        assert_eq!(ask_vsim_gate(), 0.5);
        // Out-of-range and malformed values fall back to the default, exactly
        // like `ask_relevance_floor`.
        for bad in ["1.5", "-0.1", "not-a-number", ""] {
            std::env::set_var("ASK_VSIM_GATE", bad);
            assert_eq!(
                ask_vsim_gate(),
                ASK_VSIM_GATE_DEFAULT,
                "{bad:?} must fall back to the default"
            );
        }
        std::env::remove_var("ASK_VSIM_GATE");
        assert_eq!(ask_vsim_gate(), ASK_VSIM_GATE_DEFAULT);
    }

    /// Should-abstain regression fixtures: real top-1 scores from a genuine
    /// `EVAL_EMBEDDER=ort cargo run -p coralswarm-backend --bin retrieval_eval
    /// -- --dump-scores` run against `backend/eval/golden/golden.jsonl`'s
    /// abstention category (14 queries — the corpus genuinely cannot answer
    /// these) plus its answerable categories' lowest scorers (the closest
    /// near-misses, including PR #227's staleness negative fixtures' sibling
    /// category). Pins the shadow floor's real-world behavior at the unit
    /// level without needing a live Postgres for the test suite itself.
    #[test]
    fn shadow_floor_candidate_correctly_abstains_on_13_of_14_golden_abstention_queries() {
        // category, query_id, top1_score — abstention (all 14; corpus cannot
        // answer any of these).
        let abstention_top1: &[(&str, f64)] = &[
            ("a01", 0.564895),
            ("a02", 0.572972),
            ("a03", 0.561667),
            ("a04", 0.597669), // the one known miss — see the module-level doc comment
            ("a05", 0.549559),
            ("a06", 0.561871),
            ("a07", 0.563798),
            ("a08", 0.544676),
            ("a09", 0.557919),
            ("a10", 0.560557),
            ("a11", 0.560291),
            ("a12", 0.565717),
            ("a13", 0.585201),
            ("a14", 0.545769),
        ];
        let shadow = ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT;
        let correct = abstention_top1
            .iter()
            .filter(|(_, score)| !honesty_guard_would_answer(&[*score], shadow))
            .count();
        assert_eq!(
            correct, 13,
            "shadow floor {shadow} should abstain on exactly 13/14 golden abstention queries \
             (documented overlap: 'a04' at 0.597669 is the one known miss)"
        );
        // And under TODAY's live floor, none of them abstain (the audited bug).
        let live_correct = abstention_top1
            .iter()
            .filter(|(_, score)| {
                !honesty_guard_would_answer(&[*score], ASK_RELEVANCE_FLOOR_DEFAULT)
            })
            .count();
        assert_eq!(
            live_correct, 0,
            "sanity: the live 0.02 floor should abstain on none of these (the audited bug)"
        );
    }

    #[test]
    fn shadow_floor_candidate_costs_zero_answerable_golden_queries() {
        // Lowest top-1 scores across direct/temporal/staleness (answerable)
        // categories — the ones closest to the shadow floor, i.e. the ones
        // most likely to become a false abstention if the floor were raised.
        let answerable_low_top1: &[(&str, f64)] = &[
            ("d14", 0.590035),
            ("d36", 0.590228),
            ("d40", 0.593064),
            ("t02", 0.593384),
            ("d03", 0.596289),
            ("t04", 0.597330),
            ("t01", 0.597891),
            ("st09", 0.602091), // staleness category's lowest — its own negative
                                // (non-superseding) cases score comparably or higher.
        ];
        let shadow = ASK_RELEVANCE_FLOOR_SHADOW_DEFAULT;
        for (id, score) in answerable_low_top1 {
            assert!(
                honesty_guard_would_answer(&[*score], shadow),
                "answerable query {id} (top1={score}) must still clear the shadow floor \
                 {shadow} — a false abstention here would be a real user-visible regression"
            );
        }
    }

    // ── Ask v2 scope / depth / cap helpers (ADR-0011) ───────────────────────────

    #[test]
    fn resolve_graph_ids_unions_and_dedupes_preserving_order() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        // graph_ids[] wins; legacy graph_id is folded in if not already present.
        assert_eq!(resolve_graph_ids(Some(a), &Some(vec![a, b])), vec![a, b]);
        assert_eq!(resolve_graph_ids(Some(b), &Some(vec![a])), vec![a, b]);
        // Legacy-only still works (back-compat).
        assert_eq!(resolve_graph_ids(Some(a), &None), vec![a]);
        // Nothing in scope ⇒ empty (handler rejects with 422).
        assert!(resolve_graph_ids(None, &None).is_empty());
        // Duplicates within graph_ids[] collapse.
        assert_eq!(resolve_graph_ids(None, &Some(vec![a, a, b])), vec![a, b]);
    }

    #[test]
    fn synthesis_context_cap_is_top_k_for_one_ocean_and_scales_bounded() {
        // Single ocean: exactly top_k (pre-v2 behavior preserved).
        assert_eq!(synthesis_context_cap(20, 1), 20);
        // Grows with ocean count.
        assert!(synthesis_context_cap(20, 3) > 20);
        // Never below top_k, never above the ceiling.
        assert!(synthesis_context_cap(20, 100) <= SYNTHESIS_CONTEXT_CAP_CEILING);
        assert!(synthesis_context_cap(20, 100) >= 20);
    }

    #[test]
    fn synthesis_context_cap_never_panics_for_top_k_above_ceiling() {
        // `top_k` is caller-controlled up to 100 (HTTP route and `ask_ocean` MCP
        // tool both allow it), well past `SYNTHESIS_CONTEXT_CAP_CEILING` (60).
        // The old `scaled.clamp(top_k, CEILING)` panicked whenever `top_k > 60`
        // because `Ord::clamp` requires `min <= max`. Sweep the full caller-facing
        // range (with explicit coverage of the 61..=100 boundary) across a few
        // ocean counts and assert the invariant holds without panicking.
        for ocean_count in [1usize, 2, 6] {
            for top_k in 1..=100usize {
                let cap = synthesis_context_cap(top_k, ocean_count);
                assert!(
                    cap <= SYNTHESIS_CONTEXT_CAP_CEILING,
                    "cap {cap} exceeded ceiling for top_k={top_k}, ocean_count={ocean_count}"
                );
                assert!(
                    cap >= top_k.min(SYNTHESIS_CONTEXT_CAP_CEILING),
                    "cap {cap} below top_k.min(ceiling) for top_k={top_k}, ocean_count={ocean_count}"
                );
            }
        }
    }

    // ── Phase 5b: supersession-aware ranking ──────────────────────────────────
    //
    // `SUPERSESSION_RANKING_ENABLED` is process-global env state; serialize
    // every test in this group behind one mutex (mirrors `synthesis.rs`'s own
    // `ENV_GUARD` for its `SYNTHESIS_*` env tests) so they can't observe each
    // other's in-flight override when the crate's test binary runs them in
    // parallel threads.
    static SUPERSESSION_ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn supersession_ranking_enabled_defaults_on() {
        // §3.9: flipped now that D1 (the seed-exclusion fix the flip is
        // gated on) is in this branch's base.
        let _g = SUPERSESSION_ENV_GUARD.lock().unwrap();
        std::env::remove_var("SUPERSESSION_RANKING_ENABLED");
        assert!(
            supersession_ranking_enabled(),
            "must default ON — §3.9's precondition (D1) is met, and the flag shipped OFF only \
             because THAT precondition wasn't met yet at ship time"
        );
    }

    #[test]
    fn supersession_ranking_enabled_parses_defensively() {
        let _g = SUPERSESSION_ENV_GUARD.lock().unwrap();
        // Explicit truthy tokens, and anything unrecognized (blank/garbage),
        // both fall through to the ON default.
        for v in ["1", "true", "TRUE", "Yes", "on", "", "garbage"] {
            std::env::set_var("SUPERSESSION_RANKING_ENABLED", v);
            assert!(supersession_ranking_enabled(), "{v:?} should leave ranking on");
        }
        // Only an explicit, recognized falsy token is a real OFF override.
        for v in ["0", "false", "FALSE", "No", "off"] {
            std::env::set_var("SUPERSESSION_RANKING_ENABLED", v);
            assert!(
                !supersession_ranking_enabled(),
                "{v:?} should be a real OFF override"
            );
        }
        std::env::remove_var("SUPERSESSION_RANKING_ENABLED");
    }

    // ── §3.6: multi-valued `superseded_atoms` (repairs the "arbitrary one" bug) ─

    // ── §3.6: the `CLAIMS` block ─────────────────────────────────────────────────

    // ── §7.4 retrieval trace ───────────────────────────────────────────────────

    // ── §5.3–§5.5: currents traversal ─────────────────────────────────────────
    //
    // `GRAPH_TRAVERSAL_ENABLED` is process-global env state; serialize every
    // test in this group behind its own mutex, same rationale as
    // `SUPERSESSION_ENV_GUARD` above.
    static GRAPH_TRAVERSAL_ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn graph_traversal_enabled_defaults_off() {
        let _g = GRAPH_TRAVERSAL_ENV_GUARD.lock().unwrap();
        std::env::remove_var("GRAPH_TRAVERSAL_ENABLED");
        assert!(!graph_traversal_enabled(), "must default OFF per §5.5");
    }

    #[test]
    fn graph_traversal_enabled_parses_defensively() {
        let _g = GRAPH_TRAVERSAL_ENV_GUARD.lock().unwrap();
        for v in ["1", "true", "TRUE", "Yes", "on"] {
            std::env::set_var("GRAPH_TRAVERSAL_ENABLED", v);
            assert!(graph_traversal_enabled(), "{v} should enable traversal");
        }
        for v in ["0", "false", "no", "off", "", "garbage"] {
            std::env::set_var("GRAPH_TRAVERSAL_ENABLED", v);
            assert!(!graph_traversal_enabled(), "{v} should keep traversal off");
        }
        std::env::remove_var("GRAPH_TRAVERSAL_ENABLED");
    }

    // Fixed (not `Uuid::new_v4()`) so two independently-seeded stores can be
    // compared atom-for-atom — the whole point of the byte-identical test
    // below is a cross-store comparison, which random ids would defeat.

    // ── Temporal intake intent (cs-ask-chat-a) ─────────────────────────────────

    // ── Event-time-scope intent ────────────────────────────────────────────────

    #[test]
    fn event_time_window_detects_recap_phrasing_with_a_time_reference() {
        let now = "2026-07-19T15:30:00Z".parse::<DateTime<Utc>>().unwrap();
        // "today": start of today (00:00 UTC) through `now`.
        let (since, until) = event_time_window_at("what happened today?", now, Tz::UTC).unwrap();
        assert_eq!(
            since,
            "2026-07-19T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(until, now);

        // "yesterday": full previous calendar day.
        let (since, until) = event_time_window_at("recap yesterday", now, Tz::UTC).unwrap();
        assert_eq!(
            since,
            "2026-07-18T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            until,
            "2026-07-19T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );

        // "this week" (ISO week, Monday start) — 2026-07-19 is a Sunday, so the
        // week started Monday 2026-07-13.
        let (since, until) = event_time_window_at("summarize this week", now, Tz::UTC).unwrap();
        assert_eq!(
            since,
            "2026-07-13T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(until, now);

        // "last week": the full prior ISO week.
        let (since, until) = event_time_window_at("give me a recap of last week", now, Tz::UTC).unwrap();
        assert_eq!(
            since,
            "2026-07-06T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            until,
            "2026-07-13T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );

        // "this month".
        let (since, until) = event_time_window_at("catch me up this month", now, Tz::UTC).unwrap();
        assert_eq!(
            since,
            "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(until, now);

        // "on <weekday>" — most recent occurrence at/before `now`'s date.
        // `now` is a Sunday; "on Friday" resolves to 2026-07-17.
        let (since, until) = event_time_window_at("what happened on friday", now, Tz::UTC).unwrap();
        assert_eq!(
            since,
            "2026-07-17T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            until,
            "2026-07-18T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn event_time_window_requires_both_a_recap_verb_and_a_time_reference() {
        let now = Utc::now();
        // A recap verb with NO resolvable time reference stays plain semantic
        // retrieval — "summarize the launch decision" must not be hijacked into
        // an (undefined) time window.
        assert!(event_time_window_at("summarize the launch decision", now, Tz::UTC).is_none());
        assert!(event_time_window_at("recap the merger", now, Tz::UTC).is_none());
        // A time reference with no recap verb is an ordinary topical question,
        // not an event-recap — e.g. a question ABOUT something that happens to
        // mention "today" in passing.
        assert!(event_time_window_at("who is on call today?", now, Tz::UTC).is_none());
        assert!(event_time_window_at("what's the weather today", now, Tz::UTC).is_none());
    }

    // ── Event-time-scope: tiered hybrid classifier (Tier 0/1/2) ────────────────

    // ── Relative multi-day spans ───────────────────────────────────────────────
    //
    // The July-25 miss had two independent causes. The window collapsed to a
    // single day (covered here), and the scan was truncated newest-first so a
    // dense day evicted an older one — that second one is now structurally gone:
    // the scan is complete and window corals reach synthesis as ~10-word titles,
    // so there is no budget left to starve anyone with. The `select_day_fair`
    // helper and its cap that used to be tested here went with it.

    #[test]
    fn tier1_resolves_today_and_yesterday_as_a_two_day_span() {
        // Regression: `event_time_window_at` returns on first match and the
        // `today` arm came first, so naming both days answered about today only.
        let now = DateTime::parse_from_rfc3339("2026-07-26T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (since, until) =
            event_time_window_at("recap what happened today and yesterday", now, Tz::UTC).unwrap();
        assert_eq!(
            since.date_naive().to_string(),
            "2026-07-25",
            "the span must START on yesterday"
        );
        assert_eq!(until, now, "and end clamped at now, never in the future");

        // A single-day question must NOT widen into a span.
        let (since_today, _) = event_time_window_at("recap what happened today", now, Tz::UTC).unwrap();
        assert_eq!(since_today.date_naive().to_string(), "2026-07-26");
    }

    /// The exact production incident, as a test.
    ///
    /// A US-Central founder asked "what happened today" at 21:38 local on
    /// Aug 4. In UTC that instant is already 02:38 on Aug 5, so the old
    /// UTC-anchored `day_range` returned a window starting at Aug 5 00:00Z —
    /// 19:00 local — and the entire working day (roughly 06:51–16:01 local)
    /// fell OUTSIDE it. The deterministic date scan found nothing, synthesis
    /// fell back to the non-citable CLAIMS block, and the answer came back
    /// "answered from 0 sources" over an ocean holding ~99 corals from that
    /// very day.
    #[test]
    fn today_is_the_users_calendar_day_not_the_servers() {
        let chicago: Tz = "America/Chicago".parse().unwrap();
        // 2026-08-04 21:38 CDT == 2026-08-05 02:38 UTC.
        let now = DateTime::parse_from_rfc3339("2026-08-05T02:38:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let (since, until) = event_time_window_at("what happened today", now, chicago).unwrap();

        // CDT is UTC-5, so the user's Aug 4 begins at 05:00Z on Aug 4.
        assert_eq!(
            since.to_rfc3339(),
            "2026-08-04T05:00:00+00:00",
            "\"today\" must start at the user's local midnight"
        );
        assert_eq!(until, now, "and end clamped at now");

        // The whole working day is inside the window — this is the assertion
        // that actually failed in production.
        for workday_instant in [
            "2026-08-04T11:51:00Z", // first coral of the day
            "2026-08-04T16:02:00Z",
            "2026-08-04T21:01:00Z", // last coral of the day
        ] {
            let t = DateTime::parse_from_rfc3339(workday_instant)
                .unwrap()
                .with_timezone(&Utc);
            assert!(
                t >= since && t <= until,
                "{workday_instant} must fall inside the user's \"today\""
            );
        }

        // And the old behaviour, pinned so the parameter can't quietly become
        // decorative: under UTC the SAME question excludes that whole workday.
        let (utc_since, _) = event_time_window_at("what happened today", now, Tz::UTC).unwrap();
        assert_eq!(utc_since.to_rfc3339(), "2026-08-05T00:00:00+00:00");
        let first_coral = DateTime::parse_from_rfc3339("2026-08-04T11:51:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(
            first_coral < utc_since,
            "this is the bug: UTC-anchored \"today\" starts after the work happened"
        );
    }

    /// East of the meridian the error runs the other way — a user in Tokyo
    /// asking early in the morning would, under UTC anchoring, get a "today"
    /// that had not started yet.
    #[test]
    fn today_is_local_east_of_utc_too() {
        let tokyo: Tz = "Asia/Tokyo".parse().unwrap();
        // 2026-08-05 08:00 JST == 2026-08-04 23:00 UTC.
        let now = DateTime::parse_from_rfc3339("2026-08-04T23:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let (since, until) = event_time_window_at("what happened today", now, tokyo).unwrap();
        // JST is UTC+9, so the user's Aug 5 began at 2026-08-04T15:00Z.
        assert_eq!(since.to_rfc3339(), "2026-08-04T15:00:00+00:00");
        assert_eq!(until, now);

        // Under UTC the window would still be on Aug 4 — a different calendar
        // day than the one the user is living in.
        let (utc_since, _) = event_time_window_at("what happened today", now, Tz::UTC).unwrap();
        assert_eq!(utc_since.to_rfc3339(), "2026-08-04T00:00:00+00:00");
        assert!(utc_since < since);
    }

    /// "on Monday" means the user's Monday. At 08:00 JST on Tuesday Aug 4,
    /// UTC is still Monday Aug 3 — so a UTC-anchored `weekday_range` would
    /// hand back TODAY (Aug 3 UTC being a Monday) instead of the user's
    /// actual most recent Monday, Aug 3 local.
    #[test]
    fn weekday_range_uses_the_users_calendar() {
        let tokyo: Tz = "Asia/Tokyo".parse().unwrap();
        // 2026-08-04 08:00 JST (a Tuesday) == 2026-08-03 23:00 UTC (a Monday).
        let now = DateTime::parse_from_rfc3339("2026-08-03T23:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let (since, until) = weekday_range(now, tokyo, chrono::Weekday::Mon);
        // The user's Monday is Aug 3 local, which began at 2026-08-02T15:00Z.
        assert_eq!(since.to_rfc3339(), "2026-08-02T15:00:00+00:00");
        assert_eq!(
            until.to_rfc3339(),
            "2026-08-03T15:00:00+00:00",
            "a past local day ends at the NEXT local midnight, not at `now`"
        );
    }

    /// A local calendar day is 23 or 25 hours wide across a DST transition,
    /// and consecutive days must stay contiguous with no gap or overlap —
    /// the property `journal_sweep::resolve_local_midnight` exists to hold,
    /// re-asserted here because the ask path now depends on it too.
    #[test]
    fn day_range_handles_dst_transitions() {
        let chicago: Tz = "America/Chicago".parse().unwrap();

        // Spring forward: 2026-03-08 in US Central loses an hour ⇒ 23h day.
        let after_spring = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (spring_start, spring_end) = day_range(after_spring, chicago, 1);
        assert_eq!(
            (spring_end - spring_start).num_hours(),
            23,
            "the spring-forward day must be 23 hours, not a flat 24"
        );

        // Fall back: 2026-11-01 in US Central repeats an hour ⇒ 25h day.
        let after_fall = DateTime::parse_from_rfc3339("2026-11-02T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (fall_start, fall_end) = day_range(after_fall, chicago, 1);
        assert_eq!(
            (fall_end - fall_start).num_hours(),
            25,
            "the fall-back day must be 25 hours, not a flat 24"
        );

        // Contiguity across the transition: yesterday's end IS today's start.
        let (today_start, _) = day_range(after_spring, chicago, 0);
        assert_eq!(
            spring_end, today_start,
            "consecutive local days must not gap or overlap"
        );
    }

    // ── Event-window scan failures must never read as an empty window ──────────
    //
    // Regression cover for the RLS-scoping fix: `timestamp_window_atoms` was
    // called on the bare pool, so once `0083_consumer_rls` went live it raised
    // `unrecognized configuration parameter "app.account_id"` on every ask —
    // and `.unwrap_or_else(|_| Vec::new())` turned that into "no atoms in the
    // window", which made `apply_event_window_merge` return `mgr` untouched.
    // The temporal scope vanished silently and "what did we work on today?" was
    // answered from pure semantic retrieval as a confident "nothing from
    // today". These lock in the error/empty distinction.

    // ── P0 fixes (scale-readiness review 2026-07-19) ────────────────────────────

    // ── collapse_inverse_pairs (inverse-pair collapse + already-shown suppress) ──

    #[test]
    fn merge_keyword_hits_normalizes_each_source_and_takes_max() {
        let a = Uuid::new_v4(); // keyword-only
        let b = Uuid::new_v4(); // node-only
        let c = Uuid::new_v4(); // in both

        // Different raw scales: keyword maxes at 20, node maxes at 6.
        let keyword = vec![(a, 20.0), (c, 10.0)];
        let node = vec![(b, 6.0), (c, 3.0)];

        let merged: std::collections::HashMap<Uuid, f64> =
            merge_keyword_hits(keyword, node).into_iter().collect();

        // Each source normalized to its own max → [0, 1], decoupling scales.
        assert!(
            (merged[&a] - 1.0).abs() < 1e-9,
            "keyword max normalizes to 1.0"
        );
        assert!(
            (merged[&b] - 1.0).abs() < 1e-9,
            "node max normalizes to 1.0"
        );
        // c: keyword 10/20 = 0.5 vs node 3/6 = 0.5 → max = 0.5.
        assert!(
            (merged[&c] - 0.5).abs() < 1e-9,
            "shared atom keeps the max normalized score"
        );
    }

    #[test]
    fn merge_keyword_hits_preserves_keyword_only_path() {
        // With no node hits, normalizing by max is exactly what the reranker
        // already does, so relative ordering is unchanged.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let merged: std::collections::HashMap<Uuid, f64> =
            merge_keyword_hits(vec![(a, 8.0), (b, 2.0)], vec![])
                .into_iter()
                .collect();
        assert!((merged[&a] - 1.0).abs() < 1e-9);
        assert!((merged[&b] - 0.25).abs() < 1e-9);
    }

    fn test_state_no_db_no_llm() -> Arc<AppState> {
        crate::test_app_state()
    }

    fn queries_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/queries", post(create_query))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    fn list_queries_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/queries", get(list_queries))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    // ── POST /v1/queries tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let router = queries_router(test_state_no_db_no_llm());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/queries")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"text":"hello","graph_id":"00000000-0000-0000-0000-000000000001"}"#,
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_no_db_returns_503() {
        let router = queries_router(test_state_no_db_no_llm());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/queries")
            .header("Authorization", "Bearer sometoken")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"text":"hello","graph_id":"00000000-0000-0000-0000-000000000001"}"#,
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── PR3 (usage-metering plan): `StreamFinalActuals::resolve` ───────────────
    //
    // `create_query_stream`'s streaming loop previously captured only
    // `input_tokens`/`output_tokens`/`stop_reason` off the terminal `Final`
    // frame — the cache counters were silently dropped the moment the loop
    // moved on, and (once `StreamFinalEvent` grew a `model`/`cost_usd`
    // field) those would have been dropped too. These tests exercise the
    // extracted pure resolver directly, proving the fix without standing up
    // the whole SSE handler.

    // ── `citation_details` guaranteed-coverage tests ──────────────────────────

    // ── `age_days` (phase 4 citation-richness fix 1) ──────────────────────────

    // ── `atom_reef_map` / `node_title_map` (phase 4 citation-richness fix 2/3) ─

    #[test]
    fn atom_reef_map_inverts_the_node_to_atoms_multimap_per_atom() {
        let atom = Uuid::new_v4();
        let other_atom = Uuid::new_v4();
        let reef_a = Uuid::new_v4();
        let reef_b = Uuid::new_v4();
        let nodes = vec![
            NodeWithAtoms {
                node_id: reef_a,
                node_type: "entity".to_string(),
                title: "Reef A".to_string(),
                atom_ids: vec![atom, other_atom],
                injected_atom_ids: vec![],
            },
            NodeWithAtoms {
                node_id: reef_b,
                node_type: "entity".to_string(),
                title: "Reef B".to_string(),
                atom_ids: vec![atom],
                injected_atom_ids: vec![],
            },
        ];
        let map = atom_reef_map(&nodes);
        let mut atom_reefs = map.get(&atom).cloned().unwrap_or_default();
        atom_reefs.sort();
        let mut expected = vec![reef_a, reef_b];
        expected.sort();
        assert_eq!(
            atom_reefs, expected,
            "the shared atom must map to BOTH reefs"
        );
        assert_eq!(map.get(&other_atom).cloned(), Some(vec![reef_a]));

        let titles = node_title_map(&nodes);
        assert_eq!(titles.get(&reef_a).cloned(), Some("Reef A".to_string()));
        assert_eq!(titles.get(&reef_b).cloned(), Some("Reef B".to_string()));
    }

    // ── `citation_details` channel/author/reef parity (phase 4 fix 2) ────────

    // ── GET /v1/queries tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn list_queries_missing_auth_returns_401() {
        let router = list_queries_router(test_state_no_db_no_llm());
        let req = HttpRequest::builder()
            .method("GET")
            .uri("/v1/queries")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn list_queries_bearer_no_db_returns_503() {
        let router = list_queries_router(test_state_no_db_no_llm());
        let req = HttpRequest::builder()
            .method("GET")
            .uri("/v1/queries")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[cfg(test)]
mod payment_required_tests {
    use super::*;
    use crate::billing::funding_policy::RefusalReason;

    /// The 402 keeps every pre-extra-usage field with its old value and adds
    /// `refusal_reason`, the stable code a client picks paywall copy from.
    #[tokio::test]
    async fn the_402_body_keeps_its_old_fields_and_adds_the_refusal_reason() {
        for reason in RefusalReason::ALL {
            let response = out_of_credits_response("req-1", reason);
            assert_eq!(response.status(), StatusCode::PAYMENT_REQUIRED);
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                body,
                json!({
                    "error": "out of credits: this account's credit balance is at or below zero",
                    "reason": "insufficient_credits",
                    "resolution": "purchase a plan or top-up credits to continue asking/searching",
                    "request_id": "req-1",
                    "refusal_reason": reason.code(),
                })
            );
        }
    }

    #[test]
    fn the_interactive_refusal_maps_to_the_402_with_its_reason() {
        let response = interactive_refusal_response(
            enforcement::InteractiveRefusal::OutOfCredits(RefusalReason::MemberCapReached),
            "req-2",
        );
        assert_eq!(response.status(), StatusCode::PAYMENT_REQUIRED);
        // An unreadable balance is still a retryable 503, never a 402.
        let response = interactive_refusal_response(
            enforcement::InteractiveRefusal::EligibilityUnavailable,
            "req-3",
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
