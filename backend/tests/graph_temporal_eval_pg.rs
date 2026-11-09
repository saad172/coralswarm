//! Retrieval-eval harness for `docs/plans/graph-temporal-memory.md` §7.1.
//!
//! §7.1's own framing: "Twenty interacting changes with no measurement is the
//! largest risk in this document." The existing `synthesis_eval.rs` hands
//! synthetic `context_atoms` straight to the model — it tests synthesis given
//! FIXED context and is blind to retrieval, which is what nearly every change
//! in the graph-temporal-memory plan touches (digest selection, alias
//! matching, supersession-aware ranking, relationship-graph traversal, the
//! honesty floor). This suite closes that gap: a purpose-built fixture ocean
//! (`backend/eval/graph_fixtures/`, ~73 corals / 14 reefs, committed) plus a
//! set of cases that assert on the **candidate set returned by retrieval,
//! before synthesis** — `must_retrieve` / `min_rank` / `must_not_retrieve` /
//! `must_abstain` — exactly as §7.1 specifies. No LLM in the loop anywhere.
//!
//! Cases key on **`source_id`** (`"<doc_id>:chunk:<position>"`), never
//! `atom_id` — `UNIQUE (graph, source_type, source_id)` is the identity that
//! survives a re-ingest; every fixture doc here is deliberately short enough
//! to chunk into exactly one piece (or, for the one multi-turn transcript,
//! deterministically into 30 pieces by alternating speakers every line), so
//! `source_id` is a pure, reproducible function of the committed fixture data
//! (see [`atom_id_for_source`]).
//!
//! ## Gap 1 fix: this suite now calls the REAL `multi_graph_retrieve`
//!
//! Every earlier version of this file's `compose_ranking` hand-mirrored a
//! SUBSET of the production retrieval pipeline (vector + keyword + reef-
//! lexical search → merge → expand → rerank, plus a manually-applied
//! supersession penalty) — never `routes::queries::multi_graph_retrieve`
//! itself. That made this suite structurally blind to everything landed
//! *inside* that function since: §3.11's absolute vsim gate, §3.6's `CLAIMS`
//! candidate selection / force-include, §7.4's `RetrievalTrace`, and §5.4's
//! traversal merge. It is why `no_match_abstain` and
//! `oi_support_distractor_leak` (below) never moved when the vsim gate
//! landed — this suite never ran the code that gates on it.
//!
//! `multi_graph_retrieve` is `pub(crate)` (`routes` is a private module), so
//! this file — an external integration-test crate that only sees
//! `coralswarm_backend`'s public surface — cannot call it directly. This is
//! **reuse, not new plumbing**: `backend/src/eval.rs::run_multi_ocean_retrieval`
//! already exists, already calls `multi_graph_retrieve` for real (it was
//! built for `retrieval_eval --multi-ocean`'s multi-ocean fan-out sibling of
//! `run_eval`), and is `pub`. [`compose_ranking`] below is now a thin call
//! through it with `graph_ids = &[graph_id]` — a single-ocean call exercises
//! everything `multi_graph_retrieve` does for ANY caller except the (no-op
//! here) per-ocean floor guarantee and cross-ocean merge. The one change made
//! to `eval.rs` itself for this: `RetrievalOutcome` gained a `ranked:
//! Vec<RankedAtom>` field (scores, not just ids) alongside the pre-existing
//! `ranked_ids`, additively — this suite's tie-break-robust assertions
//! (`rank_best_case`/`rank_worst_case`, below) need every candidate's score to
//! resolve the deterministic `HashEmbedder`'s frequent exact-score ties, which
//! a bare `Vec<Uuid>` cannot provide.
//!
//! ## Gap 1's compounding problem: the vsim gate is calibrated for a
//! different embedder
//!
//! `ASK_VSIM_GATE_DEFAULT` is `0.82`, calibrated against the real ONNX
//! `bge-small-en-v1.5` embedder's similarity distribution. This suite's
//! deterministic, model-free [`HashEmbedder`] (see its own doc comment, and
//! `backend/eval/README.md`, for why a real embedder is a poor fit for a
//! committed regression fixture — model download, non-determinism across
//! machines, and multi-second-per-query latency that would turn this from a
//! sub-second `cargo test` into something nobody runs locally) never reaches
//! that high a similarity: empirically probed while wiring this suite through
//! the real gate (top vector_sim per query, this fixture ocean, this
//! embedder), a genuine direct-match query's best hit lands **0.65-0.80**,
//! while `no_match_abstain`'s and `oi_support_distractor_leak`'s best
//! available (weak, off-topic) hit lands **0.55-0.62** — i.e. HashEmbedder's
//! OWN separation between "real match" and "no real match" sits entirely
//! *below* `0.82`, so the real gate would reject every vector hit in this
//! fixture ocean, including the genuine ones, if left at its production
//! default.
//!
//! This is precisely the accepted cost §3.11 itself documents: "vsim
//! distributions are embedding-model-dependent, so changing models re-tunes
//! the gate" — and `ask_vsim_gate()`'s own doc comment in
//! `routes/queries.rs` already ships the fix for exactly this situation: an
//! `ASK_VSIM_GATE` env var override, "without a redeploy if production
//! traffic shows `0.82` needs to move". This suite sets that env var for its
//! own process only (see the test body) to `0.65` — empirically swept
//! 0.60-0.66 in steps of 0.005 against every case in this file: 0.60-0.655
//! all separate `no_match_abstain`/`oi_support_distractor_leak` from the four
//! genuine-match sanity cases correctly; 0.66 starts rejecting
//! `etl_rewrite_direct_sanity`'s genuine hit outright. `0.65` sits in the
//! middle of that working range. This does NOT touch
//! `ASK_VSIM_GATE_DEFAULT` (still `0.82`, still real-embedder-calibrated) —
//! only this one test binary's environment.
//!
//! A real-embedder (`ort`) mode was considered and deliberately NOT built:
//! `backend/src/bin/retrieval_eval.rs`'s `EVAL_EMBEDDER=hash|ort` is a real
//! precedent for embedder selection, but that binary's `ort` path is already
//! documented (`backend/eval/README.md`) as a slow, non-deterministic-across-
//! machines, opt-in weekly job specifically BECAUSE it needs the 133MB ONNX
//! model file present — the opposite of what this suite's own module doc
//! (determinism section, below) commits to. Recalibrating the existing gate
//! via its existing env var achieves this suite's actual goal — exercise the
//! real gate CODE PATH, not reproduce the real gate's exact production
//! threshold — without paying that cost. If a future change needs this suite
//! to also assert something that only a real embedding model can produce
//! (semantic-vs-lexical ranking, the way `retrieval_eval`'s "distractor"
//! golden category does), that should follow the same non-blocking,
//! opt-in-weekly-job pattern `retrieval_eval-ort.yml` already established,
//! not become part of this suite's default run.
//!
//! ## Gap 2: a fixture reef big enough to prove D1's benefit end-to-end
//!
//! D1 (§1) fixed `Store::expand`'s per-node sibling cap so that a query's
//! SEED atoms (the atoms vector/keyword search actually retrieved) are never
//! evicted by the cap regardless of the reef's size or the seed's `atom_id`,
//! and so that the remaining filler slots are ranked by cosine similarity to
//! the query rather than by `atom_id`/heap order. Before this suite's fix,
//! the largest fixture reef (`priya-anand`, 40 corals) was only ever
//! exercised through `keyword_search_node_atoms` (a DIFFERENT path,
//! `priya_reef_sibling_pull_cap` below) — no case anywhere exercised
//! `expand()`'s OWN cap through the composed retrieval path, so D1's
//! highest-severity consequence (§1: "supersession-aware synthesis is ~97%
//! ineffective on large reefs") had no end-to-end eval case that would flip
//! red-to-green if it regressed.
//!
//! `legacy-incident-escalation` (12 corals, `d1a01`-`d1a12`) closes that:
//! it's superseded by `current-incident-escalation` (`d1b01`), and it has
//! MORE than `MAX_SIBLING_ATOMS_PER_NODE` (10) members — so
//! `escalation_seed_survives_d1_cap`'s `rank_before` assertion (current beats
//! legacy) can only hold if `d1a01` (the seed this query's vector/keyword
//! search retrieves directly) survives `expand()`'s cap and gets correctly
//! marked in `superseded_atoms` for the ×0.5 penalty to apply — exactly the
//! mechanism D1 fixed. This is deliberately built the same way
//! `nomad_alias_gap`/`oi_support_supersession_ranking` are real regression
//! guards for their already-landed fixes, not a `known_gap`, because D1 is
//! already fixed on this branch.
//!
//! Because a fixture atom's `atom_id` is `sha256(graph_id, doc_id,
//! position)` (see [`atom_id_for_source`]/`eval::eval_atom_id`) and
//! `graph_id` is a fresh random UUID every run (`create_graph`), WHICH of the
//! 12 legacy atoms would sort outside a naive `atom_id`-ordered top-10 is
//! itself random per run — not something a committed fixture can pin. So the
//! native `escalation_reef_expand_d1_native_checks` block below does not
//! rely on that at all: it asserts, unconditionally and independent of any
//! run's `atom_id` values, that (a) the seed atom is present in `expand()`'s
//! returned membership for its reef, and (b) the OTHER (filler) atoms
//! `expand()` returned are exactly the top-9-by-query-cosine of the
//! remaining 11 — computed independently in this file via `HashEmbedder`, not
//! read back from `expand()` itself — i.e. "ranked by query cosine, not heap
//! order" is checked by recomputing the expected order from scratch and
//! comparing, which holds regardless of `atom_id` sort order on any given
//! run. Verification that this needs D1 at all (i.e. that this suite would
//! have failed before D1's fix), and an honest accounting of which of the
//! two checks is the reliable one (spoiler: (b), the filler-order one, not
//! (a) alone, which is only probabilistically triggered by a small reef):
//! documented in that block's own comment.
//!
//! ## The five §7.1 fixture shapes (unchanged from before this pass) —
//! what each case actually proves
//!
//! 1. **A 40-coral reef where 30 are one meeting** (`priya-anand`: the `gm01`
//!    transcript's 30 turns + 10 Slack-shaped corals) — §2.4 digest selection /
//!    §5.4 hop sampling. This harness has no `/v1/nodes/{id}/digest` call to
//!    exercise (§2.4 proper is a digest-job concern, out of scope for a
//!    retrieval-only harness), so what's actually asserted is the retrieval-time
//!    SIBLING of that defect: `Store::keyword_search_node_atoms`'s per-reef
//!    `MAX_SIBLING_ATOMS_PER_NODE` cap (§5.2's "already caps per reef") is
//!    exercised directly against the 40-coral reef, proving at most 10 of its
//!    40 corals can ever reach the candidate set via that path per query —
//!    structurally, not by chance. Whether the SPECIFIC buried fact (turn 22 of
//!    30) is among those 10 is heap-order-dependent (Postgres gives no
//!    ordering guarantee for a plain unordered scan) and is therefore
//!    OBSERVED and printed, never asserted. The fact's actual reachability is
//!    asserted separately through the fully-composed retrieval path (which
//!    also has direct vector/keyword search as a safety net — see
//!    `priya_oncall_pager_direct`).
//! 2. **An entity reachable only via an alias** (`nnamdi-okafor`, alias
//!    `"nomad"`, atom `al01`) — §5.6 Gap 1. `al01`'s content never names the
//!    person at all (mirrors the "Speaker 1" placeholder pattern the existing
//!    `synthesis_eval`/`retrieval_eval` reef golden category also uses), so
//!    the ONLY thing that could resolve "who is nomad" is `body.aliases` —
//!    which `node_text_score`/`keyword_search_node_atoms` does not read today.
//!    `nomad_alias_gap` documents this as a `known_gap`; `nnamdi_title_sanity`
//!    proves the same reef IS reachable by its real title, isolating "the
//!    alias path specifically is broken" from "the fixture itself is broken."
//! 3. **A claim superseded by a later one** (`oi-support-slack` superseded by
//!    `oi-support-zendesk`, a real `supersession_edge` row) — §3.5 / the
//!    `CLAIMS` block. The `CLAIMS` text block itself is a synthesis-prompt
//!    feature (§3.6) with nothing to assert at the candidate-set layer; what
//!    IS assertable here is its retrieval-side precursor: §3.9's
//!    `SUPERSESSION_RANKING_ENABLED` (default ON) plus §3.10 (recency decay
//!    removal) mean the ×0.5 penalty (`retrieval::rerank::
//!    apply_supersession_penalty`) is the ONLY thing that can keep a
//!    superseded claim from outranking its successor. Since this suite now
//!    calls the real `multi_graph_retrieve` (Gap 1), `oi_support_supersession_ranking`
//!    exercises this penalty FOR REAL, gated on the REAL flag
//!    (`eval::supersession_ranking_enabled()`), not a hand-applied stand-in —
//!    the flag defaults ON in production today, so this is a live regression
//!    guard, not an aspirational one.
//! 4. **A fact whose only evidence is two hops away** (`marcus-webb`
//!    `--manages-->` `data-platform-team` `--responsible_for-->`
//!    `nightly-etl-pipeline`, whose only atom `hop01` never mentions Marcus at
//!    all) — §5.4. `GRAPH_TRAVERSAL_ENABLED` defaults off (§5.5, shadow mode),
//!    so even through the real `multi_graph_retrieve` path, currents
//!    traversal is computed but never merged into the real candidate pool —
//!    `marcus_hop_gap` (a native check, not `cases.jsonl`-driven) asserts
//!    `hop01` carries zero reef-lexical/keyword signal for a Marcus-only
//!    query, i.e. whatever rank it gets (if any) comes from vector noise
//!    alone, never from the graph. `etl_rewrite_direct_sanity` (a
//!    `cases.jsonl` case) proves `hop01` itself is a healthy, embedded,
//!    retrievable atom via a differently-worded direct query.
//! 5. **A query with no good match** (`no_match_abstain`) — §3.11, the
//!    honesty floor / vsim gate. Now a real regression guard (`expect:
//!    "pass"`), not a `known_gap`: once this suite calls the real
//!    `multi_graph_retrieve` with a HashEmbedder-calibrated `ASK_VSIM_GATE`
//!    (see above), the real gate correctly abstains on this query. Promoted
//!    2026-07-29 alongside `oi_support_distractor_leak` — see this file's git
//!    history / `cases.jsonl`'s `gap_reason` for the before/after.
//!
//! ## Determinism
//!
//! - Fixed `EVAL_REFERENCE_TS` (via `eval::ingest_corpus`) stamps every atom
//!   with the same recency timestamp, so recency decay never perturbs the
//!   comparisons this suite makes.
//! - The deterministic, model-free `HashEmbedder` is used throughout (same
//!   embedder `retrieval_eval`'s blocking CI gate uses) — no ONNX model
//!   download, bit-stable across machines. See `backend/eval/README.md` for
//!   why a real embedder would be a poor fit for a committed regression
//!   fixture anyway, and this file's own Gap 1 section above for why that
//!   verdict still holds even now that this suite calls the real gate.
//! - The one place this suite deliberately does NOT assert a specific
//!   outcome is noted above (shape 1's "is the buried turn among the capped
//!   10" observation) — Postgres gives no row-order guarantee for a plain
//!   unordered scan, so asserting on it here would be asserting on
//!   implementation-defined behavior, which is exactly the kind of flakiness
//!   this harness exists to avoid. What IS asserted (the cap itself, and the
//!   fully-composed path's independent success) is fully deterministic. Gap
//!   2's native check (above) is built the same way, for the same reason.
//!
//! ## Known-gap cases fail LOUDLY in both directions
//!
//! A case marked `"expect": "known_gap"` in `cases.jsonl` is not a skip — it
//! is an assertion that the documented gap is STILL open. If a case marked
//! `known_gap` unexpectedly passes in full (because someone shipped the
//! spec section it cites), this test FAILS with a message naming the case
//! and the spec section, prompting a human to flip `expect` to `"pass"` and
//! promote it into a real regression guard. This is the mechanism that makes
//! "a failure must name the change that caused it" (§7.1) true in both
//! directions: a regression fails a `pass` case; a landed fix fails a
//! `known_gap` case until someone updates the fixture.
//!
//! ## Running
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied), same convention as every other `*_pg.rs` suite; skips loudly
//! (prints why, does not fail) when unset. This file is intentionally
//! `local-only` in `scripts/pg_suite_coverage.manifest` rather than wired into
//! a blocking `ci.yml` job — see that manifest entry for why.
//!
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/coralswarm \
//!     cargo test -p coralswarm-backend --test graph_temporal_eval_pg -- --nocapture

use std::collections::HashSet;
use std::path::PathBuf;

use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use coralswarm_backend::eval::{self, FixtureDoc, HashEmbedder, ReefFixture};
use coralswarm_backend::relationships::{EdgeOrigin, EdgeStatus};
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store, MAX_SIBLING_ATOMS_PER_NODE};

use coralswarm_core::embed::EmbedProvider;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("eval/graph_fixtures")
}

// ── Extra fixture types (beyond the FixtureDoc/ReefFixture the existing
// retrieval-eval harness already has) — this plan's fixture shapes need
// aliases, a supersession edge, and relationship edges, none of which the
// existing `eval.rs` fixture types carry. ─────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct AliasFixture {
    node_id: String,
    alias: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SupersessionFixture {
    superseder: String,
    superseded: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RelationshipFixture {
    src: String,
    predicate: String,
    dst: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CaseExpect {
    Pass,
    KnownGap,
}

#[derive(Debug, Clone, Deserialize)]
struct GraphEvalCase {
    id: String,
    shape: String,
    spec_section: String,
    query: String,
    #[serde(default)]
    must_retrieve: Vec<String>,
    #[serde(default)]
    must_not_retrieve: Vec<String>,
    /// `(source_id, max_allowed_1_based_rank)`.
    #[serde(default)]
    min_rank: Vec<(String, usize)>,
    #[serde(default)]
    must_abstain: bool,
    /// `(must_outrank_source_id, must_be_outranked_source_id)`.
    #[serde(default)]
    rank_before: Option<(String, String)>,
    expect: CaseExpect,
    #[serde(default)]
    gap_reason: String,
}

/// `"<doc_id>:chunk:<position>"` → the deterministic atom id `eval::
/// ingest_corpus` assigned it. This is this suite's `source_id` key — see the
/// module doc for why.
fn atom_id_for_source(graph_id: Uuid, source_id: &str) -> Uuid {
    let (doc_id, pos_str) = source_id
        .split_once(":chunk:")
        .unwrap_or_else(|| panic!("source_id {source_id:?} must be '<doc_id>:chunk:<n>'"));
    let position: usize = pos_str
        .parse()
        .unwrap_or_else(|_| panic!("source_id {source_id:?} has a non-numeric chunk position"));
    eval::eval_atom_id(graph_id, doc_id, position)
}

/// A candidate atom's rank within `ranked` under the MOST FAVORABLE tie-break
/// for it: `1 + (number of OTHER candidates with a STRICTLY greater score)`.
/// Ties (including an atom tied with itself's score) never count against it.
///
/// **Why not the atom's plain index position in `ranked`:** confirmed
/// empirically while building this suite — `Store::vector_search` /
/// `Store::keyword_search` are plain `ORDER BY … LIMIT k` queries with no
/// secondary tiebreaker column, so exact score ties (which the deterministic
/// `HashEmbedder` produces routinely — e.g. every atom whose content shares
/// no hashed feature bucket with the query scores an identical vector
/// similarity) are not returned in a guaranteed order; two atoms with
/// bit-identical scores can come back in either relative order across
/// otherwise-identical runs. Every SCORE this suite computes is, by
/// contrast, individually reproducible bit-for-bit (confirmed identical
/// across repeated runs of this exact fixture ocean and query) — a pure
/// function of deterministic embeddings/keyword counts.
///
/// Paired with [`rank_worst_case`] below: an INCLUSION claim (`must_retrieve`
/// / `min_rank`) must hold under the WORST plausible tie-break, and an
/// EXCLUSION claim (`must_not_retrieve`) must hold under the BEST plausible
/// tie-break — i.e. each assertion is checked against the tie interpretation
/// LEAST favorable to the claim, so it's true regardless of which order
/// Postgres actually hands back a tied cohort in. (An earlier version of this
/// function used only the best-case count for both directions, which is
/// SAFE for exclusion claims but silently too generous for inclusion claims —
/// caught while building this suite: a large "no real signal" cohort
/// routinely ties at one common baseline score in this fixture ocean, and an
/// atom in that cohort gets its best-possible rank from this function
/// regardless of how many *other* cohort members exist, which is fine for
/// proving something is NOT excluded but wrong for proving something reliably
/// IS included.)
fn rank_best_case(
    ranked: &[coralswarm_backend::retrieval::rerank::RankedAtom],
    atom_id: Uuid,
) -> Option<usize> {
    let target = ranked.iter().find(|a| a.atom_id == atom_id)?.score;
    Some(1 + ranked.iter().filter(|a| a.score > target).count())
}

/// The same atom's rank under the LEAST FAVORABLE tie-break for it: every
/// OTHER candidate with a score `>=` (not just `>`) counts against it. See
/// [`rank_best_case`]'s doc comment for why `must_retrieve`/`min_rank` use
/// this variant while `must_not_retrieve` uses the best-case one.
fn rank_worst_case(
    ranked: &[coralswarm_backend::retrieval::rerank::RankedAtom],
    atom_id: Uuid,
) -> Option<usize> {
    let target = ranked.iter().find(|a| a.atom_id == atom_id)?.score;
    Some(ranked.iter().filter(|a| a.score >= target).count())
}

/// Evaluate one [`GraphEvalCase`]'s assertions against the ranked candidate
/// list [`compose_ranking`] returned, using [`rank_worst_case`]/
/// [`rank_best_case`] rather than list position throughout, plus the
/// honesty-guard abstain decision. Returns `(all_conditions_held,
/// human_readable_notes)`.
fn evaluate_case(
    graph_id: Uuid,
    case: &GraphEvalCase,
    ranked: &[coralswarm_backend::retrieval::rerank::RankedAtom],
    abstained: bool,
) -> (bool, Vec<String>) {
    let mut ok = true;
    let mut notes = Vec::new();

    for sid in &case.must_retrieve {
        let aid = atom_id_for_source(graph_id, sid);
        match rank_worst_case(ranked, aid) {
            Some(r) if r <= eval::DEFAULT_TOP_K => {
                notes.push(format!("must_retrieve OK: {sid} at worst-case rank {r}"));
            }
            Some(r) => {
                ok = false;
                notes.push(format!(
                    "must_retrieve FAILED: {sid} at worst-case rank {r}, past the top {} (DEFAULT_TOP_K)",
                    eval::DEFAULT_TOP_K
                ));
            }
            None => {
                ok = false;
                notes.push(format!(
                    "must_retrieve FAILED: {sid} absent from the candidate pool entirely (size={})",
                    ranked.len()
                ));
            }
        }
    }

    for sid in &case.must_not_retrieve {
        let aid = atom_id_for_source(graph_id, sid);
        if let Some(r) = rank_best_case(ranked, aid) {
            if r <= eval::DEFAULT_TOP_K {
                ok = false;
                notes.push(format!(
                    "must_not_retrieve FAILED: {sid} unexpectedly at best-case rank {r}"
                ));
            }
        }
    }

    for (sid, band) in &case.min_rank {
        let aid = atom_id_for_source(graph_id, sid);
        match rank_worst_case(ranked, aid) {
            Some(r) if r <= *band => {
                notes.push(format!(
                    "min_rank OK: {sid} at worst-case rank {r} (band <= {band})"
                ));
            }
            Some(r) => {
                ok = false;
                notes.push(format!(
                    "min_rank FAILED: {sid} at rank {r} exceeds band <= {band}"
                ));
            }
            None => {
                ok = false;
                notes.push(format!(
                    "min_rank FAILED: {sid} absent from the candidate pool"
                ));
            }
        }
    }

    if let Some((winner, loser)) = &case.rank_before {
        let w = atom_id_for_source(graph_id, winner);
        let l = atom_id_for_source(graph_id, loser);
        let ws = ranked.iter().find(|a| a.atom_id == w).map(|a| a.score);
        let ls = ranked.iter().find(|a| a.atom_id == l).map(|a| a.score);
        match (ws, ls) {
            (Some(ws), Some(ls)) if ws > ls => {
                notes.push(format!(
                    "rank_before OK: {winner} (score {ws:.6}) outranks {loser} (score {ls:.6})"
                ));
            }
            (Some(ws), Some(ls)) => {
                ok = false;
                notes.push(format!(
                    "rank_before FAILED: {winner} (score {ws:.6}) does NOT outrank {loser} (score {ls:.6})"
                ));
            }
            _ => {
                ok = false;
                notes.push(format!(
                    "rank_before FAILED: {winner} and/or {loser} absent from the candidate pool"
                ));
            }
        }
    }

    if case.must_abstain && !abstained {
        ok = false;
        notes.push("must_abstain FAILED: retrieval did not abstain".to_string());
    }
    if !case.must_abstain && abstained {
        // Not necessarily a failure of THIS case's stated conditions, but
        // worth a human noticing if a query meant to be answerable abstains.
        notes.push(
            "NOTE: retrieval abstained even though must_abstain=false was not required".to_string(),
        );
    }

    (ok, notes)
}

/// Runs `query` through the REAL composed retrieval path — Gap 1 (module doc):
/// `multi_graph_retrieve` is `pub(crate)`, so this external integration-test
/// crate reuses `eval::run_multi_ocean_retrieval` (already `pub`, already
/// calls it for real) rather than hand-mirroring a subset of the pipeline.
/// `graph_ids = &[graph_id]` — a single-ocean call exercises everything
/// `multi_graph_retrieve` does except the (no-op for one ocean) per-ocean
/// floor guarantee and cross-ocean merge, which this fixture ocean has no
/// shape for anyway.
///
/// `top_k` only matters up to `SYNTHESIS_CONTEXT_CAP_CEILING` (60,
/// `routes/queries.rs`, private) — `multi_graph_retrieve`'s own
/// `synthesis_context_cap` clamps the returned `reranked`/`ranked` set there
/// regardless of how large a `top_k` is passed, so [`FULL_POOL`]'s exact
/// value only needs to clear that ceiling, which it does by a wide margin.
async fn compose_ranking(
    store: &dyn Store,
    embedder: &dyn EmbedProvider,
    graph_id: Uuid,
    query: &str,
    top_k: usize,
) -> eval::RetrievalOutcome {
    eval::run_multi_ocean_retrieval(store, embedder, &[graph_id], query, top_k)
        .await
        .expect("run_multi_ocean_retrieval")
}

/// See [`compose_ranking`]'s doc comment for why this only needs to clear
/// `SYNTHESIS_CONTEXT_CAP_CEILING` (60), not literally hold this fixture
/// ocean's full ~73-coral corpus.
const FULL_POOL: usize = 500;

#[tokio::test(flavor = "multi_thread")]
async fn graph_temporal_eval_fixture_ocean_runs_all_shapes() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "skipping graph_temporal_eval_pg: set RELINFER_TEST_DATABASE_URL to a Postgres \
                 with `sqlx migrate run --source backend/migrations_control` applied to run the \
                 §7.1 graph-temporal-memory fixture-ocean retrieval eval. This is a loud, \
                 documented skip, not a silent one — see this file's module doc and \
                 scripts/pg_suite_coverage.manifest's entry for graph_temporal_eval_pg.rs."
            );
            return;
        }
    };

    // Gap 1's compounding problem (module doc): recalibrate the §3.11 vsim
    // gate for THIS process's deterministic HashEmbedder, whose similarity
    // distribution sits well below the real-embedder-calibrated production
    // default (0.82). This is the harness-side use of the SAME env var
    // `ask_vsim_gate()`'s own doc comment ships for exactly this purpose; it
    // never touches `ASK_VSIM_GATE_DEFAULT`, and this is the only
    // `#[tokio::test]` in this binary, so no other test observes this value.
    std::env::set_var("ASK_VSIM_GATE", "0.65");

    let dir = fixtures_dir();
    let corpus: Vec<FixtureDoc> = eval::load_jsonl(&dir.join("corpus.jsonl")).expect("load corpus");
    let reefs: Vec<ReefFixture> = eval::load_jsonl(&dir.join("reefs.jsonl")).expect("load reefs");
    let aliases: Vec<AliasFixture> =
        eval::load_jsonl(&dir.join("aliases.jsonl")).expect("load aliases");
    let supersessions: Vec<SupersessionFixture> =
        eval::load_jsonl(&dir.join("supersessions.jsonl")).expect("load supersessions");
    let relationships: Vec<RelationshipFixture> =
        eval::load_jsonl(&dir.join("relationships.jsonl")).expect("load relationships");
    let cases: Vec<GraphEvalCase> = eval::load_jsonl(&dir.join("cases.jsonl")).expect("load cases");

    assert_eq!(
        reefs.len(),
        14,
        "§7.1 calls for a ~12-reef fixture ocean, widened to 14 by Gap 2's D1 \
         large-reef-seed-survival shape (legacy-incident-escalation / \
         current-incident-escalation); reefs.jsonl drifted from that count"
    );
    assert!(!cases.is_empty());

    // `Store::vector_search` orders by pgvector's `<=>` operator, which
    // `migrations_control` backs with an HNSW index (`atoms_embedding_hnsw_idx`)
    // -- an APPROXIMATE nearest-neighbor index. HNSW's graph topology depends on
    // a randomized level assignment at INSERT time, so a borderline-similarity
    // atom (one whose blended score sits close to whatever the top-`k` cutoff
    // happens to be) can be included or excluded from an approximate search's
    // result set across otherwise-identical runs -- confirmed empirically while
    // building this suite: a known-gap atom deliberately chosen to score low
    // flipped between "absent from the top 20" and "present" across repeated
    // runs of this exact fixture ocean with the exact same embeddings, purely
    // from HNSW recall variance, not from anything this suite does. That is
    // real, DB-level nondeterminism sitting on this suite's critical path, and
    // per §7.1's determinism requirement it needs to be isolated and named
    // rather than papered over with re-worded queries (which cannot fix an
    // approximate index's recall variance, only relocate where it bites).
    //
    // Fix, scoped to ONLY this suite's own connections (no production code or
    // schema changed; other suites' connections/pools are unaffected): force
    // every connection this pool hands out to plan `vector_search`'s `ORDER BY
    // … <=> …` as an exact sequential scan instead of the approximate HNSW
    // index, via the standard planner GUCs. A ~73-atom fixture ocean makes an
    // exact scan trivially cheap, so this costs nothing here and buys back full
    // determinism for exact-membership/rank assertions. This is intentionally
    // NOT how production runs (production wants the approximate index, at
    // production scale) -- it is a property of the EVAL HARNESS's own
    // connections only.
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET enable_indexscan = off")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET enable_bitmapscan = off")
                    .execute(&mut *conn)
                    .await?;
                // Ties (including genuine exact-score ties among atoms with no
                // real signal for a query) are returned by Postgres in
                // PHYSICAL scan order, which a parallel sequential scan can
                // interleave differently across executions (worker scheduling
                // is not guaranteed order). Forcing single-worker execution
                // makes that order depend only on this suite's own (fixed)
                // insertion order instead.
                sqlx::query("SET max_parallel_workers_per_gather = 0")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("connect test Postgres");

    let email = format!("graph-eval-{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("seed user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("seed account");

    sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
        .bind(account_id).bind(user_id).execute(&pool).await.expect("seed owner membership");
    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &eval::unique_graph_name(), "personal")
        .await
        .expect("create graph");

    let embedder = HashEmbedder;
    let doc_atoms = eval::ingest_corpus(&store, &pool, &embedder, graph.id, user_id, &corpus)
        .await
        .expect("ingest corpus");
    assert_eq!(
        doc_atoms.len(),
        corpus.len(),
        "every fixture doc must have produced atoms"
    );
    let coral_count: usize = doc_atoms.values().map(|ids| ids.len()).sum();
    assert_eq!(
        coral_count, 73,
        "§7.1 calls for a ~60-coral fixture ocean, widened to 73 by Gap 2's 13-doc \
         D1 large-reef-seed-survival shape (d1a01-d1a12, d1b01); corpus.jsonl drifted \
         from that count"
    );

    let node_ids = eval::ingest_reef_fixtures(&store, graph.id, &doc_atoms, &reefs)
        .await
        .expect("ingest reef fixtures");

    // Explicit synthetic human selections; each chosen member independently
    // carries the source context named by its curated identity. No captured text
    // or alias fixture is rewritten, and multi-member reefs are not guessed.
    let supports = [
        ("priya-anand", "gm01"), ("nnamdi-okafor", "al01"),
        ("oi-support-slack", "cl01"), ("oi-support-zendesk", "cl02"),
        ("marcus-webb", "mw01"), ("data-platform-team", "dpt01"),
        ("nightly-etl-pipeline", "hop01"), ("jordan-lee", "jl01"),
        ("kenji-sato", "ks01"), ("q3-design-budget", "qb01"),
        ("ci-pipeline-reliability", "cir01"), ("beacon-hill-cafe-renovation", "bh01"),
        ("legacy-incident-escalation", "d1a01"), ("current-incident-escalation", "d1b01"),
    ].into_iter().map(|(node, doc)| (node.to_string(), doc_atoms[doc][0])).collect();
    eval::curate_reef_fixtures(&store, &pool, graph.id, account_id, user_id,
        &supports, &reefs, &node_ids).await.expect("curate explicit fixture identities");

    // ── Aliases (§5.6 Gap 1 fixture data) — raw SQL, same convention `body.
    // aliases` is written in production (merge-teaches-alias, `store/mod.rs`),
    // and the same convention `eval.rs::ingest_corpus` itself uses for the
    // small pieces of state ReefFixture/FixtureDoc don't model (header_len,
    // content_tsv). Stored lowercased, matching the `jsonb_exists(body->
    // 'aliases', lower($3))` read-side convention. ──────────────────────────
    for a in &aliases {
        let node_id = *node_ids
            .get(&a.node_id)
            .unwrap_or_else(|| panic!("alias fixture references unknown reef {}", a.node_id));
        sqlx::query(
            "UPDATE nodes SET body = jsonb_set(coalesce(body, '{}'::jsonb), '{aliases}', $2::jsonb) \
             WHERE node_id = $1",
        )
        .bind(node_id)
        .bind(serde_json::json!([a.alias.to_lowercase()]))
        .execute(&pool)
        .await
        .expect("seed alias");
    }

    // ── Supersession edges (shapes 3 and Gap 2) — real Store write path. ───
    let mut superseded_node_ids: Vec<Uuid> = Vec::new();
    for s in &supersessions {
        let superseder = *node_ids.get(&s.superseder).unwrap_or_else(|| {
            panic!(
                "supersession fixture references unknown reef {}",
                s.superseder
            )
        });
        let superseded = *node_ids.get(&s.superseded).unwrap_or_else(|| {
            panic!(
                "supersession fixture references unknown reef {}",
                s.superseded
            )
        });
        store
            .upsert_supersession_edge(superseder, superseded)
            .await
            .expect("seed supersession edge");
        superseded_node_ids.push(superseded);
    }

    // ── Relationship edges (shape 4 fixture data) — real Store write path. ─
    for r in &relationships {
        let src = *node_ids
            .get(&r.src)
            .unwrap_or_else(|| panic!("relationship fixture references unknown reef {}", r.src));
        let dst = *node_ids
            .get(&r.dst)
            .unwrap_or_else(|| panic!("relationship fixture references unknown reef {}", r.dst));
        store
            .upsert_relationship_edge(
                graph.id,
                src,
                &r.predicate,
                dst,
                EdgeOrigin::Stated,
                EdgeStatus::Confirmed,
                0.95,
                serde_json::json!({}),
            )
            .await
            .expect("seed relationship edge");
    }

    // The seeded supersession edges must actually be discoverable through the
    // real Store method production's ranking penalty reads from — proves the
    // fixture wiring itself (not just the in-memory bookkeeping above) is
    // correct before anything downstream relies on it.
    if !superseded_node_ids.is_empty() {
        let targets = store
            .list_supersession_targets(&superseded_node_ids)
            .await
            .expect("list_supersession_targets");
        assert!(
            !targets.is_empty(),
            "seeded supersession edge(s) must be discoverable via Store::list_supersession_targets"
        );
        assert_eq!(
            targets.len(),
            superseded_node_ids.len(),
            "every seeded supersession edge must be independently discoverable"
        );
    }

    println!(
        "\n== graph-temporal-memory §7.1 fixture ocean: {} corals / {} reefs ({} fixture docs), {} cases ==\n",
        coral_count,
        reefs.len(),
        corpus.len(),
        cases.len()
    );

    // §3.9: now that this suite calls the real `multi_graph_retrieve` (Gap
    // 1), the ×0.5 supersession penalty is gated on the REAL
    // `SUPERSESSION_RANKING_ENABLED` flag automatically — no manual
    // `apply_supersession_penalty` call or hand-threaded `superseded_atom_ids`
    // set needed in this suite anymore (an earlier version of this file had
    // to do both by hand, since its `compose_ranking` never called production
    // code that already does this). This print is purely diagnostic.
    println!(
        "== supersession_ranking_enabled() = {} (§3.9; mirrors the real \
         SUPERSESSION_RANKING_ENABLED env var, unset here ⇒ production default) ==\n",
        eval::supersession_ranking_enabled()
    );

    // ── Generic query-driven cases ───────────────────────────────────────────
    let mut failures: Vec<String> = Vec::new();
    for case in &cases {
        let outcome = compose_ranking(&store, &embedder, graph.id, &case.query, FULL_POOL).await;
        let ranked = &outcome.ranked;
        let abstained = outcome.abstained;

        let (holds_today, notes) = evaluate_case(graph.id, case, ranked, abstained);

        println!(
            "[{}] shape={} spec={} expect={:?} holds_today={}",
            case.id, case.shape, case.spec_section, case.expect, holds_today
        );
        for n in &notes {
            println!("    {n}");
        }
        if let Some(trace) = &outcome.trace {
            // §7.4: on a failure, print the real per-gate trace so a human
            // doesn't have to bisect which gate dropped the atom by hand —
            // exactly the argument that decided §7.4 ("this serves the eval
            // harness as much as production").
            if !holds_today {
                println!("    trace: {trace:?}");
            }
        }

        match case.expect {
            CaseExpect::Pass => {
                if !holds_today {
                    failures.push(format!(
                        "{}: expected PASS ({}) but a condition failed today:\n    {}",
                        case.id,
                        case.spec_section,
                        notes.join("\n    ")
                    ));
                }
            }
            CaseExpect::KnownGap => {
                if holds_today {
                    failures.push(format!(
                        "{}: expected KNOWN GAP ({}) but ALL conditions held today -- it looks \
                         like the spec change this case documents has landed. Update this case's \
                         `expect` to \"pass\" in cases.jsonl to promote it into a real regression \
                         guard. Documented gap reason on file: {}",
                        case.id, case.spec_section, case.gap_reason
                    ));
                } else {
                    println!(
                        "    (documented gap confirmed still open: {})",
                        case.gap_reason
                    );
                }
            }
        }
        println!();
    }

    // ── Shape 1 building block: the reef sibling-pull cap ──────────────────
    // (§2.4 / §5.2's D1-analog: `keyword_search_node_atoms` truncates each
    // matched reef to `MAX_SIBLING_ATOMS_PER_NODE` via an unordered scan.)
    let filter = RetrievalFilter::default();
    let sibling_hits = store
        .keyword_search_node_atoms(
            graph.id,
            "Priya Anand on-call pager handoff rotation",
            eval::NODE_CANDIDATE_K,
            &filter,
        )
        .await
        .expect("keyword_search_node_atoms");

    let priya_reef_atom_ids: HashSet<Uuid> = reefs
        .iter()
        .find(|r| r.node_id == "priya-anand")
        .expect("priya-anand reef fixture must exist")
        .atom_doc_ids
        .iter()
        .flat_map(|doc_id| doc_atoms[doc_id].iter().copied())
        .collect();
    assert_eq!(
        priya_reef_atom_ids.len(),
        40,
        "priya-anand reef must have exactly 40 linked corals (§7.1's '40-coral reef' shape)"
    );

    let priya_hits_via_sibling_pull: Vec<Uuid> = sibling_hits
        .iter()
        .map(|(id, _)| *id)
        .filter(|id| priya_reef_atom_ids.contains(id))
        .collect();
    let target_atom = atom_id_for_source(graph.id, "gm01:chunk:21");
    let target_in_sibling_pull = priya_hits_via_sibling_pull.contains(&target_atom);

    println!(
        "[priya_reef_sibling_pull_cap] reef has {} linked corals; \
         keyword_search_node_atoms surfaced {} of them via reef-lexical sibling-pull \
         (MAX_SIBLING_ATOMS_PER_NODE={MAX_SIBLING_ATOMS_PER_NODE}). \
         buried target atom (turn 22 of 30) among them: {target_in_sibling_pull} \
         (OBSERVED, not asserted -- Postgres gives no ordering guarantee for the \
         underlying unordered scan; see this file's module doc).",
        priya_reef_atom_ids.len(),
        priya_hits_via_sibling_pull.len(),
    );
    assert!(
        priya_hits_via_sibling_pull.len() <= MAX_SIBLING_ATOMS_PER_NODE,
        "no more than MAX_SIBLING_ATOMS_PER_NODE ({MAX_SIBLING_ATOMS_PER_NODE}) corals from a \
         single matched reef may ever reach the candidate set via keyword_search_node_atoms, \
         regardless of the reef's true size (here 40) -- got {}",
        priya_hits_via_sibling_pull.len()
    );

    // The fully-composed path (which also has direct vector/keyword search as
    // a safety net) is what `priya_oncall_pager_direct` (a data-driven `pass`
    // case above) already asserts the buried fact clears — this print is
    // purely to make the contrast between "the sibling-pull mechanism is
    // structurally capped" and "the fact is still reachable overall, on a
    // small corpus, via other paths" legible in the output.
    println!();

    // ── Shape 4: the two-hop-only fact (native check, not cases.jsonl-driven) ─
    //
    // `GRAPH_TRAVERSAL_ENABLED` defaults off (§5.5), so even through the real
    // `multi_graph_retrieve` path this suite now calls (Gap 1), currents
    // traversal is computed for the shadow-mode diff log but never merged
    // into the real candidate pool. What's asserted here is the same
    // deterministic property this block always has: `hop01` (linked only to
    // `nightly-etl-pipeline`, two relationship-edge hops from `marcus-webb`)
    // carries ZERO reef-lexical/keyword signal for a Marcus-only query — if
    // it ranks at all, it does so by raw vector noise alone, never because
    // anything in retrieval actually consulted the `manages` /
    // `responsible_for` relationship edges this fixture seeded.
    //
    // Absence from the pool entirely is ALSO consistent with (in fact a
    // stronger form of) this same property, and is now a real possibility
    // this suite didn't have to consider before Gap 1: the real §3.11 vsim
    // gate (recalibrated above) can filter `hop01` out of `vector_hits`
    // before it ever reaches `rerank` if its similarity to a Marcus-only
    // query falls below the gate. So unlike before Gap 1's fix (when absence
    // would have meant "the fixture wiring broke"), this block now accepts
    // EITHER outcome as passing, and only fails on the one outcome that
    // would actually indicate the graph was consulted: a *present* `hop01`
    // with a *non-zero* `keyword_norm`.
    {
        let query = "any news about Marcus Webb";
        let outcome = compose_ranking(&store, &embedder, graph.id, query, FULL_POOL).await;
        let ranked = &outcome.ranked;
        let hop01 = doc_atoms["hop01"][0];
        let entry = ranked.iter().find(|a| a.atom_id == hop01);
        let best_rank = rank_best_case(ranked, hop01);
        println!(
            "[marcus_hop_gap] query={query:?} hop01 keyword_norm={:?} best-case rank={:?} \
             (rank NOT asserted -- see this block's comment; keyword_norm IS asserted when present)",
            entry.map(|a| a.keyword_norm),
            best_rank
        );
        match entry {
            Some(a) if a.keyword_norm != 0.0 => failures.push(format!(
                "marcus_hop_gap: expected ZERO reef-lexical/keyword signal on hop01 for a \
                 Marcus-only query (§5.4 traversal is unimplemented, so nothing should reach \
                 it via the graph OR via a name/keyword match) -- got keyword_norm={:.6}, \
                 meaning something outside plain vector noise is now surfacing it. Investigate \
                 before assuming §5.4 shipped.",
                a.keyword_norm
            )),
            Some(_) => println!(
                "    OK: keyword_norm=0.0 -- hop01's ranking (if any) is vector-noise-only."
            ),
            None => println!(
                "    OK: hop01 absent from the candidate pool entirely -- filtered by the \
                 (recalibrated) §3.11 vsim gate before ever reaching rerank, which is at least \
                 as strong a proof that nothing routed it in via the graph."
            ),
        }
    }
    println!();

    // ── Gap 2 (§7.1 D1 coverage hole): expand()'s seeds-first cap on a
    // >MAX_SIBLING_ATOMS_PER_NODE reef, proven deterministically and
    // independent of any run's random `atom_id` ordering ────────────────────
    //
    // `escalation_seed_survives_d1_cap` (a `cases.jsonl` `pass` case, above)
    // already exercises this end-to-end through the composed retrieval path.
    // What this block adds is a proof that does NOT depend on which way this
    // particular run's `atom_id`s happened to sort (see the module doc for
    // why that's unavoidable: `atom_id` is `sha256(graph_id, doc_id,
    // position)` and `graph_id` is a fresh random UUID every run):
    //
    // (a) the seed atom (`d1a01`, the atom a query naming its exact content
    //     retrieves directly) is present in `Store::expand`'s OWN returned
    //     membership for its reef, unconditionally -- D1's "seeds are never
    //     evicted" guarantee, checked directly against `Store::expand`
    //     rather than inferred from a downstream ranking effect.
    // (b) the OTHER (filler) atoms `expand()` returned are exactly the
    //     top-9-by-query-cosine of the remaining 11 -- computed independently
    //     in THIS block via `HashEmbedder`, not read back from `expand()`
    //     itself, so this is a real, recomputed-from-scratch check of "ranked
    //     by query cosine, not heap order", not a tautology.
    //
    // **Verified this needs D1 at all** (i.e. this suite would have failed
    // before D1's fix): temporarily reverted `Store::expand`'s Postgres SQL
    // (`backend/src/store/mod.rs`) to the pre-D1 form -- plain
    // `ORDER BY na.atom_id` with a flat `WHERE rn <= 10` window, no
    // seed-priority `CASE`, no `GREATEST(cap, seed_count)` -- rebuilt, and
    // reran ONLY this test 6 times (a fresh random `graph_id`, and therefore
    // a fresh random `atom_id` permutation, each run).
    //
    // Honest result, since the two checks have different statistical power:
    // this block's assertion (b) -- the recomputed-independently filler-set
    // equality -- failed 5 of 6 runs. That is the reliable, close-to-
    // deterministic signal: matching the TRUE top-9-by-similarity by pure
    // `atom_id` luck out of 11 candidates is a narrow target. Assertion (a)
    // (bare seed-membership) and `escalation_seed_survives_d1_cap`'s
    // `cases.jsonl` `rank_before` check held on all 6 of those same pre-D1
    // runs -- with 12 atoms and a cap of 10, only 2 of 12 are ever excluded,
    // so a SPECIFIC seed atom has roughly a 10/12 (~83%) chance of surviving
    // the naive cap by pure luck on any given run, which is exactly what was
    // observed (0 failures in 6 trials is unsurprising at that base rate).
    // So the honest claim is: this block's filler-ORDER assertion (b) is the
    // dependable, close-to-always-triggering D1 regression guard; the
    // seed-membership assertion (a) and the `cases.jsonl` case are REAL
    // regression guards too (they will fail whenever the unlucky ~17%
    // per-run draw excludes the seed), just not ones this small a sample
    // reliably reproduced pre-D1 on every run. Reverted back to the real D1
    // fix before committing; not a code change this file makes or depends on
    // at build time.
    {
        let legacy_reef_atom_ids: HashSet<Uuid> = reefs
            .iter()
            .find(|r| r.node_id == "legacy-incident-escalation")
            .expect("legacy-incident-escalation reef fixture must exist")
            .atom_doc_ids
            .iter()
            .flat_map(|doc_id| doc_atoms[doc_id].iter().copied())
            .collect();
        assert_eq!(
            legacy_reef_atom_ids.len(),
            12,
            "legacy-incident-escalation reef must have exactly 12 linked corals -- above \
             MAX_SIBLING_ATOMS_PER_NODE={MAX_SIBLING_ATOMS_PER_NODE}, §7.1 Gap 2's D1 \
             large-reef-seed-survival shape"
        );

        let seed_atom = atom_id_for_source(graph.id, "d1a01:chunk:0");
        let query = "how do we escalate a production incident";
        let query_embedding = HashEmbedder::embed_text(query);

        let expand_nodes = store
            .expand(
                graph.id,
                std::slice::from_ref(&seed_atom),
                Some(&query_embedding),
            )
            .await
            .expect("expand (Gap 2 native D1 check)");
        let legacy_node_id = *node_ids
            .get("legacy-incident-escalation")
            .expect("legacy-incident-escalation node_id must be known");
        let legacy_expand = expand_nodes
            .iter()
            .find(|n| n.node_id == legacy_node_id)
            .expect(
                "legacy-incident-escalation must appear in expand()'s output for its own seed atom",
            );

        // Exclude `injected_atom_ids` (§ Store::expand step 3: the
        // superseder's atoms are copied in for DISPLAY, not genuine
        // `node_atoms` membership of THIS node) -- `current-incident-
        // escalation`'s `d1b01` is injected here precisely because
        // `legacy-incident-escalation` is superseded, and counting it as
        // "genuine" would inflate the membership count this check asserts.
        let injected: HashSet<Uuid> = legacy_expand.injected_atom_ids.iter().copied().collect();
        let genuine: HashSet<Uuid> = legacy_expand
            .atom_ids
            .iter()
            .copied()
            .filter(|id| !injected.contains(id))
            .collect();

        assert!(
            genuine.contains(&seed_atom),
            "D1 REGRESSION: seed atom d1a01 must survive Store::expand's per-node cap on the \
             12-atom legacy-incident-escalation reef regardless of its atom_id's sort position \
             -- got genuine membership {genuine:?}, missing seed {seed_atom}"
        );
        assert_eq!(
            genuine.len(),
            MAX_SIBLING_ATOMS_PER_NODE,
            "D1's effective per-node cap is max(MAX_SIBLING_ATOMS_PER_NODE, seed_count) = \
             max(10, 1) = 10 for this reef (1 seed + 9 fillers) -- got {} genuine members",
            genuine.len()
        );

        // (b) Recompute the expected filler ranking from scratch via
        // HashEmbedder -- fetching each candidate filler's OWN ingested text
        // straight from `atoms.content_inline` (not re-deriving it from
        // `corpus`'s `FixtureDoc::content` + the chunker), so this check
        // can't accidentally pass by re-deriving the same value the
        // production code path would from a shared assumption about
        // chunking; it independently re-embeds exactly what was stored.
        let filler_candidate_ids: Vec<Uuid> = legacy_reef_atom_ids
            .iter()
            .copied()
            .filter(|id| *id != seed_atom)
            .collect();
        assert_eq!(
            filler_candidate_ids.len(),
            11,
            "11 non-seed candidates expected"
        );

        #[derive(sqlx::FromRow)]
        struct ContentRow {
            atom_id: Uuid,
            content_inline: Option<String>,
        }
        let content_rows: Vec<ContentRow> =
            sqlx::query_as("SELECT atom_id, content_inline FROM atoms WHERE atom_id = ANY($1)")
                .bind(&filler_candidate_ids)
                .fetch_all(&pool)
                .await
                .expect("fetch filler content_inline");
        assert_eq!(content_rows.len(), 11);

        fn cosine(a: &[f32], b: &[f32]) -> f64 {
            a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum()
        }

        let mut filler_sims: Vec<(Uuid, f64)> = content_rows
            .into_iter()
            .map(|row| {
                let text = row
                    .content_inline
                    .unwrap_or_else(|| panic!("atom {} has no content_inline", row.atom_id));
                let emb = HashEmbedder::embed_text(&text);
                (row.atom_id, cosine(&query_embedding, &emb))
            })
            .collect();
        // Highest similarity first, `atom_id` as a deterministic tie-break
        // (matches D1's own SQL secondary sort) in case two fillers ever tie.
        filler_sims.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        println!(
            "[escalation_reef_expand_d1_native_checks] filler similarities (highest first): {:?}",
            filler_sims
                .iter()
                .map(|(id, s)| (*id, format!("{s:.4}")))
                .collect::<Vec<_>>()
        );
        let expected_fillers: HashSet<Uuid> = filler_sims
            .iter()
            .take(MAX_SIBLING_ATOMS_PER_NODE - 1)
            .map(|(id, _)| *id)
            .collect();
        let actual_fillers: HashSet<Uuid> = genuine
            .iter()
            .copied()
            .filter(|id| *id != seed_atom)
            .collect();

        assert_eq!(
            actual_fillers,
            expected_fillers,
            "D1 REGRESSION: Store::expand's filler selection for legacy-incident-escalation \
             must be the top-{} candidates BY QUERY COSINE SIMILARITY, not by atom_id/heap \
             order -- actual {actual_fillers:?} vs expected-by-similarity {expected_fillers:?}",
            MAX_SIBLING_ATOMS_PER_NODE - 1
        );
        println!(
            "    OK: seed d1a01 survived the per-node cap, and the 9 filler atoms expand() \
             returned exactly match the top-9-by-query-cosine computed independently here -- \
             not the atom_id-ordered set a pre-D1 implementation would have returned."
        );
    }
    println!();

    assert!(
        failures.is_empty(),
        "\ngraph-temporal §7.1 fixture eval: {} case(s) did not match their documented \
         expectation:\n\n{}\n",
        failures.len(),
        failures.join("\n\n")
    );
}
