//! Ask-path packing: one TypeSafe Score over the catalog-seated shortlist,
//! then code drop / sort / cap / window-veto / floor / force-include.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::retrieval::rerank::RankedAtom;

use super::{
    jev_model_for, Answer, JevOp, DecisionError, DecisionProvider, DecisionRequest, DecisionResponse, Question,
};

/// Score legend. Tune wording in shadow, not cutoffs.
pub const PACK_SCORE_CRITERIA: [&str; 4] = [
    "Does not address the ask, is empty/duplicate of a better coral in this set, or has nothing usable to say about it.",
    "About the ask but superseded, older, or only correct as prior state. Use for history; do not treat as current.",
    "About the ask and states a competing account. Usable as dissent, not as the sole current answer.",
    "States something that can be used as a current answer.",
];

pub const PACK_SCORE_INSTRUCTIONS: &str = "\
How should this coral be used for the ask? Judge `candidates[i]` identified \
by this question's atom id. drop = off-topic/empty/duplicate; historical = \
about the ask but superseded or only prior state; conflict = competing \
account of the ask; evidence = usable as a current answer.";

pub const NOUL_YES_THRESHOLD: f64 = 0.5;

/// Soft 3/reef seating inside [`PACK_TABLE_CAP`].
pub const PACK_TABLE_CAP: usize = 48;
pub const PACK_REEF_FLOOR: usize = 3;
pub const PACK_DIGEST_CHARS: usize = 160;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PackRole {
    Drop = 0,
    Historical = 1,
    Conflict = 2,
    Evidence = 3,
}

impl PackRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Historical => "historical",
            Self::Conflict => "conflict",
            Self::Evidence => "evidence",
        }
    }

    pub fn from_index(i: i32) -> Self {
        match i {
            1 => Self::Historical,
            2 => Self::Conflict,
            3 => Self::Evidence,
            _ => Self::Drop,
        }
    }
}

/// Prompt block a kept atom lands in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PackBlock {
    Evidence,
    Conflict,
    Historical,
    FloorDrop,
}

impl PackBlock {
    pub fn heading(self) -> &'static str {
        match self {
            Self::Evidence => "EVIDENCE",
            Self::Conflict => "CONFLICT",
            Self::Historical => "HISTORICAL",
            Self::FloorDrop => {
                "FLOOR (kept for minority-ocean coverage; do not treat as an answer)"
            }
        }
    }

    pub fn sort_key(self) -> u8 {
        match self {
            Self::Evidence => 0,
            Self::Conflict => 1,
            Self::Historical => 2,
            Self::FloorDrop => 3,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReefSkinny {
    pub id: Uuid,
    pub title: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub identifiers: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coral_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supersession_kind: Option<String>,
}

/// Card Jev sees: atom id + reef title/type + first+last digest.
#[derive(Debug, Clone, Serialize)]
pub struct DigestCard {
    pub atom_id: Uuid,
    pub reef_title: String,
    #[serde(rename = "type")]
    pub reef_type: String,
    pub digest: String,
    #[serde(skip)]
    pub reef_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct AtomJudgment {
    pub atom_id: Uuid,
    pub role: PackRole,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct PackedAtom {
    pub atom_id: Uuid,
    pub block: PackBlock,
    pub role: PackRole,
    pub floor_kept: bool,
    pub jev_score: f64,
}

#[derive(Debug, Clone)]
pub struct PackCompose {
    pub selected: Vec<PackedAtom>,
    pub dropped: Vec<Uuid>,
    pub floor_kept: Vec<Uuid>,
}

/// First non-empty line plus last non-empty line, clipped to `max_chars`.
pub fn digest_first_last(body: &str, max_chars: usize) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let lines: Vec<&str> = trimmed
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let text = match lines.as_slice() {
        [] => trimmed.to_string(),
        [one] => (*one).to_string(),
        [first, .., last] => format!("{first}\n{last}"),
    };
    if text.chars().count() <= max_chars {
        text
    } else {
        text.chars().take(max_chars).collect()
    }
}

/// Legacy 3×excerpt split — kept for ingest-door cards that still use it.
pub fn digest_excerpts(body: &str, max_excerpts: usize, max_chars: usize) -> Vec<String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let chunks: Vec<&str> = trimmed
        .split('\n')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let src: Vec<&str> = if chunks.is_empty() {
        vec![trimmed]
    } else {
        chunks
    };
    src.into_iter()
        .take(max_excerpts)
        .map(|s| {
            let n = s.chars().count();
            if n <= max_chars {
                s.to_string()
            } else {
                s.chars().take(max_chars).collect()
            }
        })
        .collect()
}

pub fn nearest_pack_role(score: f64) -> PackRole {
    let rounded = score.round() as i32;
    PackRole::from_index(rounded.clamp(0, 3))
}

pub fn packing_honesty_no_answer(selected: &[PackedAtom]) -> bool {
    // Floor+drop-only (or empty) → no-answer. Historical-only still answers.
    !selected.iter().any(|a| {
        matches!(
            a.block,
            PackBlock::Evidence | PackBlock::Conflict | PackBlock::Historical
        )
    })
}

pub fn packing_historical_only(selected: &[PackedAtom]) -> bool {
    !selected.is_empty()
        && selected
            .iter()
            .all(|a| matches!(a.block, PackBlock::Historical | PackBlock::FloorDrop))
        && selected.iter().any(|a| a.block == PackBlock::Historical)
}

/// Names CONTEXT ranges so the synthesizer treats blocks differently without
/// a prompt-format change when packing is off.
pub fn pack_context_directive(selected: &[PackedAtom]) -> Option<String> {
    if selected.is_empty() {
        return None;
    }
    let mut lines = vec![
        "CONTEXT atoms are grouped by Jev packing. Cite only from these numbered atoms."
            .to_string(),
    ];
    let mut i = 1usize;
    let mut current: Option<PackBlock> = None;
    let mut start = 1usize;
    let flush = |lines: &mut Vec<String>, block: PackBlock, start: usize, end: usize| {
        if start > end {
            return;
        }
        let range = if start == end {
            format!("[{start}]")
        } else {
            format!("[{start}]–[{end}]")
        };
        lines.push(format!("{range}: {}", block.heading()));
    };
    for atom in selected {
        if current != Some(atom.block) {
            if let Some(prev) = current {
                flush(&mut lines, prev, start, i - 1);
            }
            current = Some(atom.block);
            start = i;
        }
        i += 1;
    }
    if let Some(prev) = current {
        flush(&mut lines, prev, start, i - 1);
    }
    if packing_historical_only(selected) {
        lines.push(super::HISTORICAL_ONLY_DIRECTIVE.to_string());
    }
    Some(lines.join("\n"))
}

pub fn pack_questions(cards: &[DigestCard]) -> BTreeMap<String, Question> {
    let mut questions = BTreeMap::new();
    for card in cards {
        let id = card.atom_id;
        questions.insert(
            format!("pack_{id}"),
            Question::Score {
                instructions: json!({
                    "task": PACK_SCORE_INSTRUCTIONS,
                    "atom_id": id.to_string(),
                }),
                criteria: PACK_SCORE_CRITERIA
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
            },
        );
    }
    questions
}

pub fn pack_state(ask: &str, retrieval_query: &str, cards: &[DigestCard]) -> Value {
    json!({
        "ask": ask,
        "retrieval_query": retrieval_query,
        "candidates": cards,
    })
}

/// Soft `floor` corals per reef from `pool` order (catalog fetch order), then
/// force-includes still in the pool. Cap is soft: force-includes may exceed it.
pub fn seat_pack_cards(
    pool: &[RankedAtom],
    force: &HashSet<Uuid>,
    reef_of: &HashMap<Uuid, Uuid>,
    floor: usize,
    cap: usize,
) -> Vec<RankedAtom> {
    if pool.is_empty() || (floor == 0 && force.is_empty()) {
        return Vec::new();
    }
    let mut per_reef: HashMap<Option<Uuid>, usize> = HashMap::new();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for a in pool {
        let reef = reef_of.get(&a.atom_id).copied();
        let n = per_reef.entry(reef).or_insert(0);
        if *n >= floor {
            continue;
        }
        if seen.insert(a.atom_id) {
            *n += 1;
            out.push(a.clone());
            if out.len() >= cap {
                break;
            }
        }
    }
    for a in pool {
        if !force.contains(&a.atom_id) || !seen.insert(a.atom_id) {
            continue;
        }
        out.push(a.clone());
    }
    out
}

/// Drop extras past the per-reef floor (400 shrink step).
pub fn shrink_pack_cards(cards: &[DigestCard], floor: usize) -> Vec<DigestCard> {
    let mut per_reef: HashMap<Option<Uuid>, usize> = HashMap::new();
    let mut out = Vec::new();
    for c in cards {
        let n = per_reef.entry(c.reef_id).or_insert(0);
        if *n >= floor {
            continue;
        }
        *n += 1;
        out.push(c.clone());
    }
    out
}

fn block_for(role: PackRole, floor_kept: bool) -> PackBlock {
    if floor_kept {
        return PackBlock::FloorDrop;
    }
    match role {
        PackRole::Conflict => PackBlock::Conflict,
        PackRole::Historical => PackBlock::Historical,
        PackRole::Evidence => PackBlock::Evidence,
        PackRole::Drop => PackBlock::FloorDrop,
    }
}

fn pack_sort(a: &PackedAtom, b: &PackedAtom, rank_index: &HashMap<Uuid, usize>) -> std::cmp::Ordering {
    a.block
        .sort_key()
        .cmp(&b.block.sort_key())
        .then_with(|| {
            b.jev_score
                .partial_cmp(&a.jev_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| rank_index.get(&a.atom_id).cmp(&rank_index.get(&b.atom_id)))
}

/// Drop off-topic (unless floor or window), sort evidence→conflict→historical
/// by Jev Score, take(cap), restore window / floor / in-pool superseders.
pub fn compose_pack(
    ranked: &[RankedAtom],
    judgments: &HashMap<Uuid, AtomJudgment>,
    floor_ids: &HashSet<Uuid>,
    window_ids: &HashSet<Uuid>,
    superseders_of: &HashMap<Uuid, Vec<Uuid>>,
    cap: usize,
) -> PackCompose {
    let rank_index: HashMap<Uuid, usize> = ranked
        .iter()
        .enumerate()
        .map(|(i, a)| (a.atom_id, i))
        .collect();

    let mut dropped = Vec::new();
    let mut kept: Vec<PackedAtom> = Vec::new();

    for a in ranked {
        let Some(j) = judgments.get(&a.atom_id) else {
            kept.push(PackedAtom {
                atom_id: a.atom_id,
                block: PackBlock::Evidence,
                role: PackRole::Evidence,
                floor_kept: false,
                jev_score: a.score,
            });
            continue;
        };
        let is_floor = floor_ids.contains(&a.atom_id);
        let is_window = window_ids.contains(&a.atom_id);
        if j.role == PackRole::Drop && !is_floor && !is_window {
            dropped.push(a.atom_id);
            continue;
        }
        let floor_kept = j.role == PackRole::Drop && is_floor && !is_window;
        let role = if j.role == PackRole::Drop && is_window {
            PackRole::Historical
        } else {
            j.role
        };
        kept.push(PackedAtom {
            atom_id: a.atom_id,
            block: block_for(role, floor_kept),
            role,
            floor_kept,
            jev_score: j.score,
        });
    }

    kept.sort_by(|a, b| pack_sort(a, b, &rank_index));

    let mut selected: Vec<PackedAtom> = kept.iter().take(cap).cloned().collect();
    let mut chosen: HashSet<Uuid> = selected.iter().map(|a| a.atom_id).collect();

    for &fid in floor_ids.iter().chain(window_ids.iter()) {
        if chosen.contains(&fid) {
            continue;
        }
        if let Some(atom) = kept.iter().find(|a| a.atom_id == fid) {
            selected.push(atom.clone());
            chosen.insert(fid);
        }
    }

    let hits: Vec<Uuid> = selected.iter().map(|a| a.atom_id).collect();
    for hit in hits {
        let Some(supers) = superseders_of.get(&hit) else {
            continue;
        };
        for &sid in supers {
            if chosen.contains(&sid) {
                continue;
            }
            let Some(_atom) = ranked.iter().find(|a| a.atom_id == sid) else {
                continue;
            };
            if judgments.get(&sid).is_some_and(|j| j.role == PackRole::Drop) {
                continue;
            }
            let j = judgments.get(&sid);
            selected.push(PackedAtom {
                atom_id: sid,
                block: j
                    .map(|j| block_for(j.role, false))
                    .unwrap_or(PackBlock::Evidence),
                role: j.map(|j| j.role).unwrap_or(PackRole::Evidence),
                floor_kept: false,
                jev_score: j.map(|j| j.score).unwrap_or(0.0),
            });
            chosen.insert(sid);
        }
    }

    selected.sort_by(|a, b| pack_sort(a, b, &rank_index));

    let floor_kept = selected
        .iter()
        .filter(|a| a.floor_kept)
        .map(|a| a.atom_id)
        .collect();

    PackCompose {
        selected,
        dropped,
        floor_kept,
    }
}

pub fn judgments_from_response(
    cards: &[DigestCard],
    resp: &DecisionResponse,
) -> Result<HashMap<Uuid, AtomJudgment>, DecisionError> {
    let mut out = HashMap::new();
    for card in cards {
        let id = card.atom_id;
        let score = resp
            .answers
            .get(&format!("pack_{id}"))
            .and_then(Answer::as_score)
            .ok_or_else(|| DecisionError::Parse(format!("missing pack_{id}")))?;
        out.insert(
            id,
            AtomJudgment {
                atom_id: id,
                role: nearest_pack_role(score),
                score,
            },
        );
    }
    Ok(out)
}

/// Group pack cards by reef id (unreefed cards share one group).
pub fn group_digest_cards_by_reef(cards: &[DigestCard]) -> Vec<Vec<DigestCard>> {
    let mut groups: HashMap<Option<Uuid>, Vec<DigestCard>> = HashMap::new();
    let mut order: Vec<Option<Uuid>> = Vec::new();
    for card in cards {
        let key = card.reef_id;
        if !groups.contains_key(&key) {
            order.push(key);
        }
        groups.entry(key).or_default().push(card.clone());
    }
    order
        .into_iter()
        .filter_map(|k| groups.remove(&k))
        .collect()
}

/// Keep the stronger pack role / higher Score when the same atom is judged twice.
pub fn merge_pack_judgments(
    into: &mut HashMap<Uuid, AtomJudgment>,
    from: HashMap<Uuid, AtomJudgment>,
) {
    for (id, j) in from {
        into.entry(id)
            .and_modify(|existing| {
                if j.role > existing.role || (j.role == existing.role && j.score > existing.score) {
                    *existing = j.clone();
                }
            })
            .or_insert(j);
    }
}

pub async fn evaluate_pack(
    provider: &dyn DecisionProvider,
    ask: &str,
    retrieval_query: &str,
    cards: &[DigestCard],
) -> Result<HashMap<Uuid, AtomJudgment>, DecisionError> {
    if cards.is_empty() {
        return Ok(HashMap::new());
    }
    let req = DecisionRequest {
        state: pack_state(ask, retrieval_query, cards),
        model: jev_model_for(JevOp::Ask),
        op: JevOp::Ask,
        questions: pack_questions(cards),
    };
    let resp = provider.evaluate(req).await?;
    judgments_from_response(cards, &resp)
}

/// One table. A 400 shrinks once (drop extras past 3/reef) then the caller
/// fail-opens to rerank+cap. Transient retries live in the provider. The
/// whole chain is bounded by [`super::within_ask_budget`].
pub async fn evaluate_pack_with_retry(
    provider: &dyn DecisionProvider,
    ask: &str,
    retrieval_query: &str,
    cards: &[DigestCard],
) -> Result<HashMap<Uuid, AtomJudgment>, DecisionError> {
    super::within_ask_phase("pack", pack_chain(provider, ask, retrieval_query, cards)).await
}

async fn pack_chain(
    provider: &dyn DecisionProvider,
    ask: &str,
    retrieval_query: &str,
    cards: &[DigestCard],
) -> Result<HashMap<Uuid, AtomJudgment>, DecisionError> {
    match evaluate_pack(provider, ask, retrieval_query, cards).await {
        Ok(j) => Ok(j),
        Err(e) if e.is_context_overflow() => {
            let shrink = shrink_pack_cards(cards, PACK_REEF_FLOOR);
            if shrink.len() == cards.len() {
                return Err(e);
            }
            evaluate_pack(provider, ask, retrieval_query, &shrink).await
        }
        other => other,
    }
}

/// One request for the whole table (not per-reef).
pub async fn evaluate_pack_by_reef(
    provider: &dyn DecisionProvider,
    ask: &str,
    retrieval_query: &str,
    cards: &[DigestCard],
) -> Result<HashMap<Uuid, AtomJudgment>, DecisionError> {
    evaluate_pack_with_retry(provider, ask, retrieval_query, cards).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn atom(id: Uuid, score: f64) -> RankedAtom {
        RankedAtom {
            atom_id: id,
            score,
            node_id: None,
            vector_sim: score,
            keyword_norm: 0.0,
            recency_decay: 1.0,
        }
    }

    fn j(id: Uuid, role: PackRole, score: f64) -> AtomJudgment {
        AtomJudgment {
            atom_id: id,
            role,
            score,
        }
    }

    fn ids() -> [Uuid; 5] {
        [
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
            Uuid::from_u128(5),
        ]
    }

    fn compose(
        ranked: &[RankedAtom],
        judgments: &HashMap<Uuid, AtomJudgment>,
        floor: &HashSet<Uuid>,
        superseders: &HashMap<Uuid, Vec<Uuid>>,
        cap: usize,
    ) -> PackCompose {
        compose_pack(ranked, judgments, floor, &HashSet::new(), superseders, cap)
    }

    #[test]
    fn nearest_role_rounds() {
        assert_eq!(nearest_pack_role(0.0), PackRole::Drop);
        assert_eq!(nearest_pack_role(0.4), PackRole::Drop);
        assert_eq!(nearest_pack_role(0.5), PackRole::Historical);
        assert_eq!(nearest_pack_role(1.4), PackRole::Historical);
        assert_eq!(nearest_pack_role(2.0), PackRole::Conflict);
        assert_eq!(nearest_pack_role(2.6), PackRole::Evidence);
        assert_eq!(nearest_pack_role(3.2), PackRole::Evidence);
        assert_eq!(nearest_pack_role(-1.0), PackRole::Drop);
    }

    #[test]
    fn first_last_digest_clips() {
        let body = "First paragraph.\n\nSecond paragraph that is quite long and should be trimmed if needed.\nThird.";
        let d = digest_first_last(body, 160);
        assert!(d.starts_with("First paragraph."));
        assert!(d.contains("Third."));
        assert!(!d.contains("Second paragraph"));
        let clipped = digest_first_last(body, 10);
        assert_eq!(clipped.chars().count(), 10);
    }

    #[test]
    fn drop_offtopic_keeps_floor() {
        let [a, b, c, ..] = ids();
        let ranked = vec![atom(a, 0.9), atom(b, 0.4), atom(c, 0.3)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Evidence, 2.9));
        judgments.insert(b, j(b, PackRole::Drop, 0.1));
        judgments.insert(c, j(c, PackRole::Drop, 0.0));
        let floor = HashSet::from([c]);
        let out = compose(&ranked, &judgments, &floor, &HashMap::new(), 2);
        assert!(out.dropped.contains(&b));
        assert!(!out.dropped.contains(&c));
        assert!(out.floor_kept.contains(&c));
        assert!(out.selected.iter().any(|x| x.atom_id == a));
        assert!(out.selected.iter().any(|x| x.atom_id == c && x.floor_kept));
    }

    #[test]
    fn window_drop_is_vetoed() {
        let [a, b, ..] = ids();
        let ranked = vec![atom(a, 0.9), atom(b, 0.8)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Evidence, 2.8));
        judgments.insert(b, j(b, PackRole::Drop, 0.1));
        let window = HashSet::from([b]);
        let out = compose_pack(
            &ranked,
            &judgments,
            &HashSet::new(),
            &window,
            &HashMap::new(),
            5,
        );
        assert!(!out.dropped.contains(&b));
        let kept = out.selected.iter().find(|x| x.atom_id == b).unwrap();
        assert_eq!(kept.block, PackBlock::Historical);
        assert!(!packing_honesty_no_answer(&out.selected));
    }

    #[test]
    fn score_drop_drops_non_floor() {
        let [a, b, ..] = ids();
        let ranked = vec![atom(a, 0.9), atom(b, 0.8)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Drop, 0.1));
        judgments.insert(b, j(b, PackRole::Evidence, 2.7));
        let out = compose(&ranked, &judgments, &HashSet::new(), &HashMap::new(), 5);
        assert_eq!(out.dropped, vec![a]);
        assert_eq!(out.selected.len(), 1);
        assert_eq!(out.selected[0].atom_id, b);
    }

    #[test]
    fn conflict_score_is_conflict_block() {
        let [a, ..] = ids();
        let ranked = vec![atom(a, 0.9)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Conflict, 2.1));
        let out = compose(&ranked, &judgments, &HashSet::new(), &HashMap::new(), 5);
        assert_eq!(out.selected[0].block, PackBlock::Conflict);
        assert!(!packing_honesty_no_answer(&out.selected));
    }

    #[test]
    fn sort_is_evidence_then_conflict_then_historical() {
        let [a, b, c, ..] = ids();
        let ranked = vec![atom(a, 0.5), atom(b, 0.9), atom(c, 0.8)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Historical, 1.2));
        judgments.insert(b, j(b, PackRole::Conflict, 1.9));
        judgments.insert(c, j(c, PackRole::Evidence, 2.8));
        let out = compose(&ranked, &judgments, &HashSet::new(), &HashMap::new(), 5);
        let order: Vec<_> = out.selected.iter().map(|x| x.block).collect();
        assert_eq!(
            order,
            vec![
                PackBlock::Evidence,
                PackBlock::Conflict,
                PackBlock::Historical
            ]
        );
    }

    #[test]
    fn sort_within_block_uses_jev_score_not_retrieve() {
        let [a, b, ..] = ids();
        // Retrieve ranks a above b; Jev ranks b higher evidence.
        let ranked = vec![atom(a, 0.99), atom(b, 0.10)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Evidence, 2.1));
        judgments.insert(b, j(b, PackRole::Evidence, 2.9));
        let out = compose(&ranked, &judgments, &HashSet::new(), &HashMap::new(), 5);
        assert_eq!(out.selected[0].atom_id, b);
        assert_eq!(out.selected[1].atom_id, a);
    }

    #[test]
    fn cap_then_restore_floor_and_superseder() {
        let [a, b, c, d, e] = ids();
        let ranked = vec![
            atom(a, 0.99),
            atom(b, 0.98),
            atom(c, 0.97),
            atom(d, 0.2),
            atom(e, 0.1),
        ];
        let mut judgments = HashMap::new();
        for (id, s) in [(a, 2.9), (b, 2.8), (c, 2.7), (d, 2.6), (e, 2.5)] {
            judgments.insert(id, j(id, PackRole::Evidence, s));
        }
        let floor = HashSet::from([d]);
        let superseders = HashMap::from([(a, vec![e])]);
        let out = compose(&ranked, &judgments, &floor, &superseders, 2);
        let ids: HashSet<_> = out.selected.iter().map(|x| x.atom_id).collect();
        assert!(ids.contains(&a));
        assert!(ids.contains(&b));
        assert!(ids.contains(&d), "floor atom restored past cap");
        assert!(ids.contains(&e), "superseder force-included from pool");
        assert!(!ids.contains(&c));
    }

    #[test]
    fn superseder_cannot_come_from_outside_pool() {
        let [a, ..] = ids();
        let outsider = Uuid::from_u128(99);
        let ranked = vec![atom(a, 0.9)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Evidence, 2.8));
        let superseders = HashMap::from([(a, vec![outsider])]);
        let out = compose(&ranked, &judgments, &HashSet::new(), &superseders, 5);
        assert_eq!(out.selected.len(), 1);
        assert_eq!(out.selected[0].atom_id, a);
    }

    #[test]
    fn honesty_floor_drop_only() {
        let [a, ..] = ids();
        let ranked = vec![atom(a, 0.1)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Drop, 0.0));
        let floor = HashSet::from([a]);
        let out = compose(&ranked, &judgments, &floor, &HashMap::new(), 5);
        assert!(packing_honesty_no_answer(&out.selected));
        assert_eq!(out.selected[0].block, PackBlock::FloorDrop);
    }

    #[test]
    fn honesty_historical_only_still_answers() {
        let [a, ..] = ids();
        let ranked = vec![atom(a, 0.4)];
        let mut judgments = HashMap::new();
        judgments.insert(a, j(a, PackRole::Historical, 1.1));
        let out = compose(&ranked, &judgments, &HashSet::new(), &HashMap::new(), 5);
        assert!(!packing_honesty_no_answer(&out.selected));
        assert_eq!(out.selected[0].block, PackBlock::Historical);
    }

    #[test]
    fn pack_questions_are_one_score() {
        let card = DigestCard {
            atom_id: Uuid::from_u128(7),
            reef_title: "Fly".into(),
            reef_type: "entity".into(),
            digest: "hello".into(),
            reef_id: Some(Uuid::from_u128(8)),
        };
        let q = pack_questions(&[card]);
        assert_eq!(q.len(), 1);
        assert!(q.keys().any(|k| k.starts_with("pack_")));
        assert!(!q.keys().any(|k| k.starts_with("injection_")));
    }

    fn card(atom: u128, reef: Option<Uuid>, title: &str) -> DigestCard {
        DigestCard {
            atom_id: Uuid::from_u128(atom),
            reef_title: title.into(),
            reef_type: "entity".into(),
            digest: "x".into(),
            reef_id: reef,
        }
    }

    #[test]
    fn group_digest_cards_by_reef_splits() {
        let fly = Some(Uuid::from_u128(1));
        let r2 = Some(Uuid::from_u128(2));
        let groups = group_digest_cards_by_reef(&[
            card(10, fly, "Fly"),
            card(11, fly, "Fly"),
            card(20, r2, "R2"),
            card(99, None, ""),
        ]);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 1);
        assert_eq!(groups[2].len(), 1);
        assert!(groups[2][0].reef_id.is_none());
    }

    #[test]
    fn merge_pack_judgments_keeps_stronger_role() {
        let a = Uuid::from_u128(1);
        let mut into = HashMap::new();
        into.insert(a, j(a, PackRole::Historical, 1.2));
        let mut from = HashMap::new();
        from.insert(a, j(a, PackRole::Evidence, 2.8));
        merge_pack_judgments(&mut into, from);
        assert_eq!(into[&a].role, PackRole::Evidence);
    }

    #[test]
    fn seat_takes_three_per_reef_in_pool_order() {
        let fly = Uuid::from_u128(10);
        let r2 = Uuid::from_u128(20);
        let pool: Vec<RankedAtom> = (1..=6)
            .map(|i| {
                let mut a = atom(Uuid::from_u128(i), 0.1);
                a.node_id = Some(if i <= 4 { fly } else { r2 });
                a
            })
            .collect();
        let reef_of = HashMap::from([
            (Uuid::from_u128(1), fly),
            (Uuid::from_u128(2), fly),
            (Uuid::from_u128(3), fly),
            (Uuid::from_u128(4), fly),
            (Uuid::from_u128(5), r2),
            (Uuid::from_u128(6), r2),
        ]);
        let seated = seat_pack_cards(&pool, &HashSet::new(), &reef_of, 3, 48);
        let ids: Vec<_> = seated.iter().map(|a| a.atom_id.as_u128()).collect();
        assert_eq!(ids, vec![1, 2, 3, 5, 6]);
    }

    #[test]
    fn seat_appends_force_includes_past_floor() {
        let fly = Uuid::from_u128(10);
        let a4 = Uuid::from_u128(4);
        let pool: Vec<RankedAtom> = (1..=4)
            .map(|i| atom(Uuid::from_u128(i), 0.1))
            .collect();
        let reef_of = HashMap::from([
            (Uuid::from_u128(1), fly),
            (Uuid::from_u128(2), fly),
            (Uuid::from_u128(3), fly),
            (a4, fly),
        ]);
        let seated = seat_pack_cards(&pool, &HashSet::from([a4]), &reef_of, 3, 48);
        assert_eq!(seated.len(), 4);
        assert_eq!(seated[3].atom_id, a4);
    }

    #[test]
    fn shrink_drops_extras_past_floor() {
        let fly = Some(Uuid::from_u128(10));
        let cards = vec![
            card(1, fly, "Fly"),
            card(2, fly, "Fly"),
            card(3, fly, "Fly"),
            card(4, fly, "Fly"),
        ];
        let shrink = shrink_pack_cards(&cards, 3);
        assert_eq!(shrink.len(), 3);
        assert_eq!(shrink[2].atom_id, Uuid::from_u128(3));
    }
}
