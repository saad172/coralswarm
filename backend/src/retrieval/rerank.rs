//! Reranker — α·relevance + β·keyword blend, top-K output.
//!
//! Combines candidates from vector search (cosine distance), keyword search
//! (ts_rank_cd), and graph expansion (atom→node mapping) into a single ranked
//! list of atoms.  This module is pure computation — no database access.
//!
//! ## §3.10 — recency left the blend
//!
//! Until this change, `score = alpha*vsim + beta*knorm*recency_decay(ts)`: a
//! 90-day-half-life multiplier on the keyword term only. That was a crude
//! proxy for supersession — a guess about staleness made because nothing
//! knew for certain whether an old fact had actually been superseded. Now
//! that `supersession_edge` + [`apply_supersession_penalty`] (Phase 5b) can
//! answer that question directly, the proxy isn't merely redundant, it's
//! *wrong*: it penalises facts that are old **and still true**, which is
//! most of what a knowledge base holds. Biographical facts, decisions and
//! architectural choices don't decay; they get reversed, and reversal **is**
//! supersession. `RerankParams.recency_half_life_days` no longer weights
//! `score` at all — [`apply_supersession_penalty`] is the ONLY temporal
//! modifier of relevance left in this module.
//!
//! It also fixed a live asymmetry: the old decay multiplied only the
//! keyword term, so a vector-matched coral got no age penalty while a
//! keyword-matched one decayed at a 90-day half-life — and
//! `keyword_search_node_atoms` emits reef-matched atoms (naming a person or
//! project) as keyword-style candidates, so a one-year-old coral in a named
//! reef used to contribute at most `0.3 * 1.0 * 0.06 = 0.018`, invisible
//! below the honesty floor. A reef match is an *identity* signal, not a
//! freshness one, and the defining corals for an entity are usually its
//! oldest.
//!
//! **Permitted hedge, exercised:** [`RankedAtom::recency_decay`] is still
//! computed and still surfaced on every atom, but it now feeds only the
//! final sort's tie-break ([`rank_cmp`]) — ordering atoms whose blended
//! `score` is already within [`SCORE_TIE_EPSILON`], never multiplying into
//! or suppressing a score the way the old decay could. Age breaks ties; it
//! never demotes evidence.
//!
//! **Accepted cost:** conflict detection has false negatives, and much drift
//! is never stated as a contradiction at all — nobody writes "we no longer
//! use Fly.io", they just stop mentioning it. With the decay gone, an
//! un-superseded stale coral now surfaces at full confidence. Judged the
//! right trade: a wrong answer from a *detected* contradiction is
//! recoverable and inspectable (the `[SUPERSEDED]` annotation, the `CLAIMS`
//! block); one from silent decay never surfaces at all.

use std::collections::{HashMap, HashSet};
use std::f64::consts::LN_2;

use chrono::{DateTime, Utc};
use uuid::Uuid;

// ── Public output type ────────────────────────────────────────────────────────

/// Combined relevance score for a single atom.
#[derive(Debug, Clone)]
pub struct RankedAtom {
    pub atom_id: Uuid,
    pub score: f64,
    /// The node this atom belongs to, if known from graph expansion.
    pub node_id: Option<Uuid>,
    /// Vector similarity component, `[0, 1]` (`1.0 - cosine_distance/2.0`),
    /// `0.0` when this atom had no vector hit. Surfaced (alongside
    /// `keyword_norm`/`recency_decay`) so a caller can tell WHY an atom
    /// scored the way it did — e.g. a high `score` driven entirely by
    /// `vector_sim` with `keyword_norm == 0.0` is pure topical adjacency, not
    /// a real keyword/entity match (dogfooded ranking-transparency gap,
    /// 2026-07).
    pub vector_sim: f64,
    /// Normalised keyword (ts_rank_cd) component, `[0, 1]`, `0.0` when this
    /// atom had no keyword hit.
    pub keyword_norm: f64,
    /// Recency decay factor, `(0, 1]` — `1.0` for a current/undated atom,
    /// decaying toward `0` as the atom ages past `recency_half_life_days`.
    /// **Not part of `score` (§3.10)** — kept only as the tie-break input for
    /// [`rank_cmp`]'s "permitted hedge" (atoms whose `score` is within
    /// [`SCORE_TIE_EPSILON`] are ordered newest-first) and as a diagnostic
    /// so a caller can still tell how old the atom is.
    pub recency_decay: f64,
}

// ── Configuration ─────────────────────────────────────────────────────────────

/// Reranking parameters.
#[derive(Debug, Clone)]
pub struct RerankParams {
    /// Weight for vector similarity score (default: 0.7).
    pub alpha: f64,
    /// Weight for keyword (BM25) score (default: 0.3).
    pub beta: f64,
    /// Recency decay half-life in days (default: 90.0). **No longer weights
    /// `score` (§3.10)** — the only consumer left is [`RankedAtom::
    /// recency_decay`]'s computation, which feeds [`rank_cmp`]'s same-score
    /// tie-break, never a multiplier on relevance.
    pub recency_half_life_days: f64,
}

impl Default for RerankParams {
    fn default() -> Self {
        Self {
            alpha: 0.7,
            beta: 0.3,
            recency_half_life_days: 90.0,
        }
    }
}

// ── Core function ─────────────────────────────────────────────────────────────

/// Combine vector + keyword candidates and rerank by:
///   `score = alpha * relevance + beta * keyword_score`
///
/// **§3.10: `score` has no recency term.** Recency is computed (see below)
/// and surfaced on every [`RankedAtom`], but only [`rank_cmp`]'s same-score
/// tie-break reads it — age alone never raises or lowers `score`. The only
/// thing in this crate that still discounts an atom for temporal reasons is
/// [`apply_supersession_penalty`], and only when something concrete
/// (a `supersession_edge`) says it was actually superseded.
///
/// # Arguments
/// - `vector_hits`: `(atom_id, cosine_distance)` — lower distance = more relevant.
///   pgvector `<=>` returns cosine distance ∈ [0, 2].
///   Converted to `similarity = 1.0 - distance/2.0` (maps [0,2] → [1.0, 0.0]).
/// - `keyword_hits`: `(atom_id, ts_rank_cd score)` — higher = more relevant.
///   Normalised to `[0, 1]` by dividing by the max score in the set.
/// - `timestamps`: map of `atom_id → UTC timestamp`, feeding `recency_decay`
///   (tie-break only — see above).
/// - `node_map`: map of `atom_id → node_id` from expand results (may be empty).
/// - `params`: blend weights (`recency_half_life_days` shapes the tie-break
///   input only, per above).
/// - `top_k`: maximum number of atoms to return.
///
/// # Recency decay (tie-break input only, §3.10)
/// `exp(-ln(2) * days_since / half_life)`
/// where `days_since = (now - timestamp).num_days().max(0)`.
/// Atoms without a timestamp get `recency = 1.0` (treated as current).
///
/// # Deduplication
/// If the same `atom_id` appears in both `vector_hits` and `keyword_hits`, the
/// function computes a single score combining both contributions (not two
/// separate entries).
///
/// # Return value
/// At most `top_k` [`RankedAtom`]s, ordered by [`rank_cmp`] (`score`
/// descending, recency-tie-broken).
pub fn rerank(
    vector_hits: &[(Uuid, f32)],
    keyword_hits: &[(Uuid, f64)],
    timestamps: &HashMap<Uuid, DateTime<Utc>>,
    node_map: &HashMap<Uuid, Uuid>,
    params: &RerankParams,
    top_k: usize,
) -> Vec<RankedAtom> {
    if top_k == 0 {
        return vec![];
    }

    // ── Step 1: Build vector similarity map ───────────────────────────────────
    // pgvector <=> returns cosine distance in [0, 2]: 0=identical, 2=opposite.
    // Map to similarity in [0, 1]: similarity = 1.0 - distance/2.0
    let vector_sim: HashMap<Uuid, f64> = vector_hits
        .iter()
        .map(|&(id, dist)| (id, (1.0_f64 - dist as f64 / 2.0).clamp(0.0, 1.0)))
        .collect();

    // ── Step 2: Normalise keyword scores to [0, 1] ────────────────────────────
    let max_kw = keyword_hits
        .iter()
        .map(|&(_, s)| s)
        .fold(0.0_f64, f64::max);

    let keyword_norm: HashMap<Uuid, f64> = if max_kw > 0.0 {
        keyword_hits
            .iter()
            .map(|&(id, s)| (id, s / max_kw))
            .collect()
    } else {
        keyword_hits.iter().map(|&(id, _)| (id, 0.0)).collect()
    };

    // ── Step 3: Collect unique atom IDs from both sources ─────────────────────
    let mut all_ids: Vec<Uuid> = Vec::with_capacity(vector_hits.len() + keyword_hits.len());
    for &(id, _) in vector_hits {
        all_ids.push(id);
    }
    for &(id, _) in keyword_hits {
        if !vector_sim.contains_key(&id) {
            all_ids.push(id);
        }
    }

    // ── Step 4: Score each atom ───────────────────────────────────────────────
    let now = Utc::now();
    let half_life = params.recency_half_life_days;

    let mut ranked: Vec<RankedAtom> = all_ids
        .into_iter()
        .map(|atom_id| {
            let vsim = vector_sim.get(&atom_id).copied().unwrap_or(0.0);
            let knorm = keyword_norm.get(&atom_id).copied().unwrap_or(0.0);

            let recency = if let Some(&ts) = timestamps.get(&atom_id) {
                if half_life <= 0.0 {
                    1.0 // misconfigured half_life — treat every atom as current
                } else {
                    let days_since = (now - ts).num_days().max(0) as f64;
                    (-LN_2 * days_since / half_life).exp()
                }
            } else {
                1.0 // no timestamp → treat as current
            };

            // §3.10: recency is NOT part of `score` — it is computed above
            // purely to populate `recency_decay`, which only [`rank_cmp`]'s
            // tie-break consults. A multiplier here would resurrect exactly
            // the bug §3.10 removed: penalising an atom that is old but
            // still true.
            let score = params.alpha * vsim + params.beta * knorm;
            let node_id = node_map.get(&atom_id).copied();

            RankedAtom {
                atom_id,
                score,
                node_id,
                vector_sim: vsim,
                keyword_norm: knorm,
                recency_decay: recency,
            }
        })
        .collect();

    // ── Step 5: Sort by score (recency-tie-broken), take top_k ────────────────
    ranked.sort_by(rank_cmp);
    ranked.truncate(top_k);
    ranked
}

// ── §3.10 permitted hedge: recency as a same-score tie-break only ──────────

/// Tolerance, in blended-`score` units, within which two atoms are treated
/// as tied by [`rank_cmp`] and ordered by recency instead. This is the
/// §3.10 "permitted hedge" — NOT a "these two are roughly as relevant" fuzz
/// factor (that would just be the forbidden multiplier moved into the
/// comparator). It exists to give a deterministic, meaningful order to
/// atoms whose `score` is bit-identical (or differs only by `f64` rounding
/// noise in `alpha*vsim + beta*knorm`), which happens routinely — e.g. any
/// two atoms with no keyword hit and the same vector-distance bucket, or two
/// atoms neither search path touched at all, land on the exact same score
/// and would otherwise be ordered by nothing more meaningful than the
/// accident of which order they entered this function's input slices.
/// `1e-9` is close to `f64` epsilon for values in `[0, 1]` — it resolves
/// those genuine ties without ever second-guessing a real (even slightly)
/// better score.
const SCORE_TIE_EPSILON: f64 = 1e-9;

/// Order two already-scored atoms: `score` descending, with `recency_decay`
/// (more-recent-first) as a tie-break ONLY when the two scores are within
/// [`SCORE_TIE_EPSILON`] of each other — the §3.10 "permitted hedge" ("Age
/// then breaks ties without ever suppressing evidence"). Because this only
/// ever activates on a near-exact score tie, it can never demote an atom
/// whose relevance is genuinely better, and can never be the reason a
/// relevant atom falls below the honesty floor (§3.11) — unlike the old
/// multiplicative decay, which could.
fn rank_cmp(a: &RankedAtom, b: &RankedAtom) -> std::cmp::Ordering {
    if (a.score - b.score).abs() < SCORE_TIE_EPSILON {
        b.recency_decay.total_cmp(&a.recency_decay)
    } else {
        b.score.total_cmp(&a.score)
    }
}

// ── Supersession-aware ranking (Phase 5b) ───────────────────────────────────
//
// A `supersession_edge` row can already exist in production today (created
// via the pre-existing manual `POST /v1/nodes/{id}/supersede` endpoint), but
// nothing in retrieval ever read it back — a superseded atom scored and
// ranked identically to a current one. This is the READ-side fix: a
// multiplicative penalty applied to a superseded atom's already-blended
// `score`, so it survives (recoverable degradation, still answers "what did
// we originally decide?" questions) rather than being hard-excluded (which
// would silently destroy recall on a false-positive edge with no way back).

/// Multiplicative penalty applied to a superseded atom's blended `score`
/// (Phase 5b — supersession-aware ranking). Targets the FINAL blended score
/// rather than an individual `vector_sim`/`keyword_norm`/`recency_decay`
/// component: a superseded atom isn't necessarily a worse topical or keyword
/// match — it's specifically less TRUSTWORTHY as of now, which is a property
/// of the whole score, not one input to it. `0.5` sits in the middle of the
/// recommended 0.4-0.6 range: strong enough that a superseded atom reliably
/// drops behind a non-superseded one with a similar raw match, gentle enough
/// that an overwhelmingly better superseded match can still outrank a barely
/// relevant current one (recoverable degradation, not exclusion).
pub const SUPERSESSION_PENALTY_MULTIPLIER: f64 = 0.5;

/// Apply [`SUPERSESSION_PENALTY_MULTIPLIER`] (or a caller-supplied
/// `multiplier`) to every atom in `ranked` whose id is in `superseded`, then
/// re-sort by [`rank_cmp`] (score descending, §3.10 recency tie-break) on
/// the now possibly-changed scores. Pure computation
/// — no DB access, consistent with this module's existing contract; the
/// caller resolves which atoms are superseded via a DB lookup
/// (`Store::list_supersession_targets`) and passes just the id set in.
///
/// A no-op (no mutation, no re-sort) when `superseded` is empty, so a caller
/// that always invokes this but only ever has a non-empty set when the
/// supersession-ranking flag is on gets byte-identical output when it's off
/// and there's nothing to penalize.
pub fn apply_supersession_penalty(
    ranked: &mut [RankedAtom],
    superseded: &HashSet<Uuid>,
    multiplier: f64,
) {
    if superseded.is_empty() {
        return;
    }
    for atom in ranked.iter_mut() {
        if superseded.contains(&atom.atom_id) {
            atom.score *= multiplier;
        }
    }
    // Same comparator as `rerank`'s own final sort (§3.10's tie-break), so a
    // supersession-penalized re-sort can't produce a different tie-break
    // rule than the original ranking did.
    ranked.sort_by(rank_cmp);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_params() -> RerankParams {
        RerankParams::default()
    }

    // ── 1. Empty inputs return empty result ───────────────────────────────────

    #[test]
    fn rerank_empty_inputs_returns_empty() {
        let result = rerank(
            &[],
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &make_params(),
            10,
        );
        assert!(result.is_empty());
    }

    // ── 2. Vector-only, no timestamps ─────────────────────────────────────────

    #[test]
    fn vector_only_no_timestamps_scores_correctly() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();

        // distances: a=0.1, b=0.3, c=0.5 → similarities (1-d/2): 0.95, 0.85, 0.75
        let vector_hits = [(a, 0.1_f32), (b, 0.3_f32), (c, 0.5_f32)];

        let result = rerank(
            &vector_hits,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &make_params(),
            10,
        );

        assert_eq!(result.len(), 3);
        // Result must be sorted descending — atom with distance 0.1 has highest score.
        assert_eq!(result[0].atom_id, a);
        assert!(result[0].score > result[1].score);
        assert!(result[1].score > result[2].score);
        // Score for atom a: alpha * (1 - 0.1/2) = 0.7 * 0.95 = 0.665
        // Use a slightly loose epsilon to account for f32→f64 cast imprecision.
        let expected_a = 0.7_f64 * (1.0_f64 - 0.1_f32 as f64 / 2.0);
        assert!(
            (result[0].score - expected_a).abs() < 1e-9,
            "score {} ≠ expected {}",
            result[0].score,
            expected_a
        );
    }

    // ── 3. §3.10 — age alone no longer lowers `score`; it only tie-breaks ─────

    #[test]
    fn recency_no_longer_affects_score_only_breaks_ties() {
        let recent = Uuid::new_v4();
        let old = Uuid::new_v4();

        // Both atoms have the same vector distance and the same keyword score.
        let vector_hits = [(recent, 0.2_f32), (old, 0.2_f32)];
        let keyword_hits = [(recent, 1.0_f64), (old, 1.0_f64)];

        let now = Utc::now();
        let mut timestamps = HashMap::new();
        timestamps.insert(recent, now - Duration::days(1));
        timestamps.insert(old, now - Duration::days(180)); // 2 half-lives old

        let result = rerank(
            &vector_hits,
            &keyword_hits,
            &timestamps,
            &HashMap::new(),
            &make_params(),
            10,
        );

        let recent_entry = result.iter().find(|r| r.atom_id == recent).unwrap();
        let old_entry = result.iter().find(|r| r.atom_id == old).unwrap();

        // §3.10: identical vsim/keyword_norm ⇒ identical `score` regardless of
        // age — the old atom is NOT penalized for being 180 days old.
        assert_eq!(
            recent_entry.score, old_entry.score,
            "age alone must not change `score` (§3.10)"
        );
        // `recency_decay` is still computed and still differs...
        assert!(recent_entry.recency_decay > old_entry.recency_decay);
        // ...and the permitted hedge (tie-break only) still orders the newer
        // atom first when scores are this close, so retrieval output stays
        // deterministic rather than depending on input-slice order.
        assert_eq!(
            result[0].atom_id, recent,
            "same-score tie-break must prefer the more recent atom"
        );
    }

    // ── 3b. An old-but-still-relevant atom beats a fresh-but-weak one ─────────
    //
    // Direct regression guard for the §3.10 principle itself ("Locked
    // principle: an atom's standing is reduced *only* when something
    // supersedes it"): a genuinely better match must never be outranked by a
    // worse match purely because the worse one is newer. Under the OLD
    // multiplicative decay this could flip (a strong keyword match old
    // enough to decay past a weak-but-fresh one); under §3.10 it cannot.

    #[test]
    fn old_strong_match_beats_recent_weak_match() {
        let old_strong = Uuid::new_v4();
        let recent_weak = Uuid::new_v4();

        let vector_hits = [(old_strong, 0.0_f32), (recent_weak, 1.8_f32)];
        let now = Utc::now();
        let mut timestamps = HashMap::new();
        timestamps.insert(old_strong, now - Duration::days(365)); // ~4 half-lives
        timestamps.insert(recent_weak, now - Duration::days(1));

        let result = rerank(
            &vector_hits,
            &[],
            &timestamps,
            &HashMap::new(),
            &make_params(),
            10,
        );

        assert_eq!(
            result[0].atom_id, old_strong,
            "a genuinely better match, however old, must not be outranked by a \
             fresher-but-weaker one (§3.10: age alone is not evidence of staleness)"
        );
    }

    // ── 3c. The old asymmetry is gone: a keyword/reef match ages exactly like
    // a vector match ────────────────────────────────────────────────────────
    //
    // §3.10's own example: under the old decay, a one-year-old coral reached
    // ONLY via a reef/keyword match contributed at most
    // `0.3 * 1.0 * 0.06 ≈ 0.018` — invisible below the honesty floor — while
    // an equally-old atom reached via vector search paid no age penalty at
    // all. Proves that asymmetry is gone: an old keyword-only hit now scores
    // identically to a fresh one with the same `keyword_norm`.

    #[test]
    fn old_keyword_only_hit_pays_no_age_penalty() {
        let old = Uuid::new_v4();
        let fresh = Uuid::new_v4();

        // Same keyword score, no vector hit at all (mirrors a reef/name match
        // surfaced only by `keyword_search_node_atoms`).
        let keyword_hits = [(old, 4.0_f64), (fresh, 4.0_f64)];
        let now = Utc::now();
        let mut timestamps = HashMap::new();
        timestamps.insert(old, now - Duration::days(365));
        timestamps.insert(fresh, now - Duration::days(1));

        let result = rerank(
            &[],
            &keyword_hits,
            &timestamps,
            &HashMap::new(),
            &make_params(),
            10,
        );

        let old_entry = result.iter().find(|r| r.atom_id == old).unwrap();
        let fresh_entry = result.iter().find(|r| r.atom_id == fresh).unwrap();
        assert_eq!(
            old_entry.score, fresh_entry.score,
            "a year-old keyword/reef-only match must score identically to a fresh \
             one with the same keyword_norm — the pre-§3.10 asymmetry is gone"
        );
        // Sanity: this is nowhere near the (removed) old decayed value of
        // ~0.018 — it's the full undiscounted keyword contribution.
        assert!((old_entry.score - 0.3_f64).abs() < 1e-9);
    }

    // ── 4. Default params values ──────────────────────────────────────────────

    #[test]
    fn default_params_values() {
        let p = RerankParams::default();
        assert_eq!(p.alpha, 0.7);
        assert_eq!(p.beta, 0.3);
        assert_eq!(p.recency_half_life_days, 90.0);
    }

    // ── 5. top_k caps output ──────────────────────────────────────────────────

    #[test]
    fn top_k_caps_output() {
        let ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
        let vector_hits: Vec<(Uuid, f32)> = ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i as f32 * 0.05))
            .collect();

        let result = rerank(
            &vector_hits,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &make_params(),
            3,
        );

        assert_eq!(result.len(), 3);
    }

    // ── 6. Score breakdown — vector-only hit ──────────────────────────────────
    //
    // Asserts the `vector_sim`/`keyword_norm`/`recency_decay` fields are
    // populated correctly AND that `vector_sim`/`keyword_norm` reconstruct the
    // blended `score` (`alpha*vector_sim + beta*keyword_norm` — §3.10:
    // `recency_decay` is deliberately NOT part of this reconstruction, it is
    // a tie-break-only diagnostic) — a regression guard so the breakdown and
    // the blend can never silently drift apart (dogfooded
    // ranking-transparency gap, 2026-07).

    #[test]
    fn score_breakdown_vector_only_hit() {
        let a = Uuid::new_v4();
        let vector_hits = [(a, 0.2_f32)]; // similarity = 1 - 0.2/2 = 0.9
        let params = make_params();

        let result = rerank(
            &vector_hits,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &params,
            10,
        );

        assert_eq!(result.len(), 1);
        let r = &result[0];
        // Loose epsilon: `vector_sim` is computed from an f32 distance cast to
        // f64 (see `vector_only_no_timestamps_scores_correctly` above).
        let expected_vsim = 1.0_f64 - 0.2_f32 as f64 / 2.0;
        assert!((r.vector_sim - expected_vsim).abs() < 1e-6);
        assert_eq!(r.keyword_norm, 0.0, "no keyword hit ⇒ keyword_norm == 0");
        assert_eq!(r.recency_decay, 1.0, "no timestamp ⇒ recency_decay == 1");
        let reconstructed = params.alpha * r.vector_sim + params.beta * r.keyword_norm;
        assert!(
            (r.score - reconstructed).abs() < 1e-9,
            "score {} ≠ reconstructed {}",
            r.score,
            reconstructed
        );
    }

    // ── 7. Score breakdown — keyword-only hit ─────────────────────────────────

    #[test]
    fn score_breakdown_keyword_only_hit() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        // b scores higher raw so a normalises to < 1.0.
        let keyword_hits = [(a, 4.0_f64), (b, 8.0_f64)];
        let params = make_params();

        let result = rerank(
            &[],
            &keyword_hits,
            &HashMap::new(),
            &HashMap::new(),
            &params,
            10,
        );

        assert_eq!(result.len(), 2);
        let ra = result.iter().find(|r| r.atom_id == a).unwrap();
        let rb = result.iter().find(|r| r.atom_id == b).unwrap();

        assert_eq!(ra.vector_sim, 0.0, "no vector hit ⇒ vector_sim == 0");
        assert!((ra.keyword_norm - 0.5).abs() < 1e-9);
        assert_eq!(ra.recency_decay, 1.0);
        let reconstructed_a = params.alpha * ra.vector_sim + params.beta * ra.keyword_norm;
        assert!((ra.score - reconstructed_a).abs() < 1e-9);

        assert_eq!(rb.vector_sim, 0.0);
        assert!((rb.keyword_norm - 1.0).abs() < 1e-9);
        let reconstructed_b = params.alpha * rb.vector_sim + params.beta * rb.keyword_norm;
        assert!((rb.score - reconstructed_b).abs() < 1e-9);
    }

    // ── 8. Score breakdown — vector + keyword hit; recency computed but NOT
    // part of `score` (§3.10) ─────────────────────────────────────────────────

    #[test]
    fn score_breakdown_vector_and_keyword_hit_recency_excluded_from_score() {
        let a = Uuid::new_v4();
        let vector_hits = [(a, 0.4_f32)]; // similarity = 1 - 0.4/2 = 0.8
        let keyword_hits = [(a, 5.0_f64)]; // sole keyword hit ⇒ normalises to 1.0

        let now = Utc::now();
        let mut timestamps = HashMap::new();
        timestamps.insert(a, now - Duration::days(90)); // exactly one half-life

        let params = make_params();

        let result = rerank(
            &vector_hits,
            &keyword_hits,
            &timestamps,
            &HashMap::new(),
            &params,
            10,
        );

        assert_eq!(result.len(), 1);
        let r = &result[0];
        // Loose epsilon: `vector_sim` is computed from an f32 distance cast to
        // f64 (see `vector_only_no_timestamps_scores_correctly` above).
        let expected_vsim = 1.0_f64 - 0.4_f32 as f64 / 2.0;
        assert!((r.vector_sim - expected_vsim).abs() < 1e-6);
        assert!((r.keyword_norm - 1.0).abs() < 1e-9);
        // One half-life ⇒ decay ≈ 0.5 — still COMPUTED (tie-break diagnostic)...
        assert!(
            (r.recency_decay - 0.5).abs() < 1e-6,
            "recency_decay {} ≠ ~0.5",
            r.recency_decay
        );
        // ...but §3.10: `score` reconstructs from `vector_sim`/`keyword_norm`
        // ALONE — `recency_decay` being ~0.5 here must NOT have halved
        // anything.
        let reconstructed = params.alpha * r.vector_sim + params.beta * r.keyword_norm;
        assert!(
            (r.score - reconstructed).abs() < 1e-9,
            "score {} ≠ reconstructed {} (recency must not factor into score, §3.10)",
            r.score,
            reconstructed
        );
    }

    // ── 9. `apply_supersession_penalty` — Phase 5b ────────────────────────────

    #[test]
    fn apply_supersession_penalty_discounts_only_superseded_atoms() {
        let a = Uuid::new_v4(); // superseded
        let b = Uuid::new_v4(); // not superseded

        let vector_hits = [(a, 0.0_f32), (b, 0.4_f32)]; // a starts strictly ahead of b
        let mut result = rerank(
            &vector_hits,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &make_params(),
            10,
        );
        let a_before = result.iter().find(|r| r.atom_id == a).unwrap().score;
        let b_before = result.iter().find(|r| r.atom_id == b).unwrap().score;
        assert!(a_before > b_before, "sanity: a should start ahead of b");

        let mut superseded = HashSet::new();
        superseded.insert(a);
        apply_supersession_penalty(&mut result, &superseded, 0.5);

        let a_after = result.iter().find(|r| r.atom_id == a).unwrap().score;
        let b_after = result.iter().find(|r| r.atom_id == b).unwrap().score;
        assert!((a_after - a_before * 0.5).abs() < 1e-9, "superseded atom's score must be halved");
        assert_eq!(b_after, b_before, "non-superseded atom's score must be untouched");
    }

    #[test]
    fn apply_supersession_penalty_can_flip_rank_order() {
        let strong_superseded = Uuid::new_v4();
        let weak_current = Uuid::new_v4();

        // strong_superseded scores much higher pre-penalty...
        let vector_hits = [(strong_superseded, 0.0_f32), (weak_current, 1.6_f32)];
        let mut result = rerank(
            &vector_hits,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &make_params(),
            10,
        );
        assert_eq!(result[0].atom_id, strong_superseded);

        let mut superseded = HashSet::new();
        superseded.insert(strong_superseded);
        // A large penalty (well below the 0.4-0.6 recommended range, but valid
        // input) should be enough to flip the order for this synthetic gap.
        apply_supersession_penalty(&mut result, &superseded, 0.1);

        assert_eq!(
            result[0].atom_id, weak_current,
            "a heavily-penalized superseded atom must fall behind a weaker current one"
        );
    }

    #[test]
    fn apply_supersession_penalty_empty_set_is_a_no_op() {
        let a = Uuid::new_v4();
        let vector_hits = [(a, 0.2_f32)];
        let mut result = rerank(
            &vector_hits,
            &[],
            &HashMap::new(),
            &HashMap::new(),
            &make_params(),
            10,
        );
        let before = result[0].score;
        apply_supersession_penalty(&mut result, &HashSet::new(), 0.5);
        assert_eq!(result[0].score, before, "empty superseded set must not change any score");
    }

    #[test]
    fn default_supersession_penalty_multiplier_is_in_recommended_range() {
        assert!(
            (0.4..=0.6).contains(&SUPERSESSION_PENALTY_MULTIPLIER),
            "SUPERSESSION_PENALTY_MULTIPLIER ({SUPERSESSION_PENALTY_MULTIPLIER}) must stay in the \
             documented 0.4-0.6 range"
        );
    }
}
