//! Closed-set Jev judges that replace cheap-tier `complete()` adjudicate
//! seats (kind, conflict, mention, resolve, capture, contradiction, window).
//!
//! Unset env is **off** (tests). Production `fly.toml` sets `JEV_JUDGE_MODE=live`.
//! `shadow` logs and still calls the LLM. `live` uses the Jev answer and skips
//! the LLM. Every call fail-opens.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{json, Value};
use uuid::Uuid;

use super::{
    jev_model_for, Answer, JevOp, DecisionError, DecisionProvider, DecisionRequest, NoulCriteria, Question,
};
use crate::retrieval::rerank::RankedAtom;
use crate::store::NearCandidate;

/// Pack's yes-bar, inlined so this module does not import `pack` internals.
const NOUL_YES: f64 = 0.5;

#[derive(Debug, Clone, PartialEq)]
pub enum MentionJudge {
    Same {
        matched_index: usize,
        confidence: f64,
        reason: String,
    },
    Different {
        confidence: f64,
        reason: String,
    },
    Unsure {
        confidence: f64,
        reason: String,
    },
}

/// `JEV_JUDGE_MODE` — worker/aux closed-set adjudicate seats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JevJudgeMode {
    Off,
    Shadow,
    Live,
}

impl JevJudgeMode {
    pub fn from_env() -> Self {
        match std::env::var("JEV_JUDGE_MODE") {
            Ok(v) => Self::from_env_str(&v),
            Err(_) => Self::Off,
        }
    }

    pub fn from_env_str(v: &str) -> Self {
        match v.trim().to_ascii_lowercase().as_str() {
            "shadow" => Self::Shadow,
            "live" | "on" | "1" | "true" => Self::Live,
            _ => Self::Off,
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Shadow | Self::Live)
    }

    pub fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::Live => "live",
        }
    }
}

pub fn jev_judge_mode() -> JevJudgeMode {
    JevJudgeMode::from_env()
}

pub fn jev_catalog_scan_mode() -> JevCatalogScanMode {
    JevCatalogScanMode::from_env()
}

/// `JEV_CATALOG_SCAN_MODE` — entity + decision Nouls over the code-seated
/// catalog before pack. Default off. `off` is the kill switch (today's
/// retrieve, no Jev catalog). Production `fly.toml` sets `live`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JevCatalogScanMode {
    Off,
    Shadow,
    Live,
}

impl JevCatalogScanMode {
    pub fn from_env() -> Self {
        match std::env::var("JEV_CATALOG_SCAN_MODE") {
            Ok(v) => Self::from_env_str(&v),
            Err(_) => Self::Off,
        }
    }

    pub fn from_env_str(v: &str) -> Self {
        match v.trim().to_ascii_lowercase().as_str() {
            "shadow" => Self::Shadow,
            "live" | "on" | "1" | "true" => Self::Live,
            _ => Self::Off,
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Shadow | Self::Live)
    }

    pub fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::Live => "live",
        }
    }
}

fn choice_criteria(pairs: &[(&str, &str)]) -> BTreeMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Some(v.to_string())))
        .collect()
}

async fn eval(
    provider: &dyn DecisionProvider,
    op: JevOp,
    state: Value,
    questions: BTreeMap<String, Question>,
) -> Result<super::DecisionResponse, DecisionError> {
    provider
        .evaluate(DecisionRequest {
            state,
            model: jev_model_for(op),
            questions,
            op,
        })
        .await
}

/// Library kind: person / org / topic / project.
pub async fn judge_kind(
    provider: &dyn DecisionProvider,
    title: &str,
    evidence: &str,
) -> Result<Option<&'static str>, DecisionError> {
    let mut questions = BTreeMap::new();
    questions.insert(
        "kind".into(),
        Question::Choice {
            instructions: json!({"task": "Assign a kind to this knowledge-graph topic."}),
            criteria: choice_criteria(&[
                ("person", "a person"),
                ("org", "an organization or vendor"),
                ("topic", "a topic"),
                ("project", "a named project"),
            ]),
        },
    );
    let resp = eval(
        provider,
        JevOp::Judge,
        json!({"title": title, "digest": evidence}),
        questions,
    )
    .await?;
    let choice = resp.answers.get("kind").and_then(Answer::as_choice);
    Ok(crate::store::library::parse_library_kind(
        choice.unwrap_or(""),
    ))
}

/// Conflict-detection: conflicts / different / unsure.
pub async fn judge_conflict(
    provider: &dyn DecisionProvider,
    a_title: &str,
    a_rationale: &str,
    b_title: &str,
    b_rationale: &str,
) -> Result<(String, f64), DecisionError> {
    let mut questions = BTreeMap::new();
    questions.insert(
        "verdict".into(),
        Question::Choice {
            instructions: json!({
                "task": "Do these two claims conflict? Precision-biased: only conflicts when they cannot both currently hold."
            }),
            criteria: choice_criteria(&[
                (
                    "conflicts",
                    "The two claims cannot both currently hold; one contradicts, reverses, or replaces the other.",
                ),
                (
                    "different",
                    "Both can be true at once — different facts, different scopes, or the same claim restated.",
                ),
                ("unsure", "The text is not enough to tell."),
            ]),
        },
    );
    let resp = eval(
        provider,
        JevOp::Judge,
        json!({
            "a": {"title": a_title, "rationale": a_rationale},
            "b": {"title": b_title, "rationale": b_rationale},
        }),
        questions,
    )
    .await?;
    let ans = resp.answers.get("verdict");
    let choice = ans
        .and_then(Answer::as_choice)
        .unwrap_or("unsure")
        .to_string();
    let confidence = ans.map(Answer::choice_confidence).unwrap_or(0.0);
    Ok((choice, confidence))
}

/// Entity-resolution pair: same / different / unsure.
pub async fn judge_resolve_pair(
    provider: &dyn DecisionProvider,
    prompt: &str,
) -> Result<(String, f64), DecisionError> {
    judge_resolve_pair_served(provider, prompt)
        .await
        .map(|(verdict, confidence, _)| (verdict, confidence))
}

/// [`judge_resolve_pair`] plus the model that actually ANSWERED
/// (`DecisionResponse::model` — on OpenRouter the served id, which may be
/// the `models:[backup]` fallback, not the configured one). Callers that
/// persist provenance must record this, not `jev_model_for`.
pub async fn judge_resolve_pair_served(
    provider: &dyn DecisionProvider,
    prompt: &str,
) -> Result<(String, f64, String), DecisionError> {
    let req = resolve_pair_request(prompt);
    let resp = provider.evaluate(req).await?;
    Ok(resolve_pair_verdict(&resp))
}

/// The entity-resolution pair question as one typed request: the same
/// question [`judge_resolve_pair_served`] asks, built once so the managed
/// entity-resolution worker can send it through the managed gate
/// (`decision::openrouter::evaluate_managed_enrichment`) instead of the
/// process-wide provider.
pub(crate) fn resolve_pair_request(prompt: &str) -> DecisionRequest {
    let mut questions = BTreeMap::new();
    questions.insert(
        "verdict".into(),
        Question::Choice {
            instructions: json!({
                "task": "Do these two reefs refer to the same real-world entity?"
            }),
            criteria: choice_criteria(&[
                ("same", "They refer to the same real-world entity."),
                ("different", "They refer to different entities."),
                ("unsure", "Not enough to decide; do not merge."),
            ]),
        },
    );
    DecisionRequest {
        state: json!({"prompt": prompt}),
        model: jev_model_for(JevOp::Judge),
        questions,
        op: JevOp::Judge,
    }
}

/// `(verdict, confidence, served model)` from a [`resolve_pair_request`]
/// answer. A missing answer is `unsure` at confidence 0, never a merge.
pub(crate) fn resolve_pair_verdict(resp: &super::DecisionResponse) -> (String, f64, String) {
    let ans = resp.answers.get("verdict");
    let choice = ans
        .and_then(Answer::as_choice)
        .unwrap_or("unsure")
        .to_string();
    let confidence = ans.map(Answer::choice_confidence).unwrap_or(0.0);
    (choice, confidence, resp.model.clone())
}

/// Mention adjudicate: type the mention, then bind only among same-kind
/// candidates (eval: one-shot bind attached “Sam Carter Consulting” to a person).
pub async fn judge_mention(
    provider: &dyn DecisionProvider,
    passage: &str,
    mention: &str,
    candidates: &[NearCandidate],
) -> Result<MentionJudge, DecisionError> {
    if candidates.is_empty() {
        return Ok(MentionJudge::Unsure {
            confidence: 0.0,
            reason: "no candidates to adjudicate against".into(),
        });
    }

    let mut kind_q = BTreeMap::new();
    kind_q.insert(
        "kind".into(),
        Question::Choice {
            instructions: json!({
                "task": format!("What kind of entity is the mention “{mention}” in this passage?"),
            }),
            criteria: choice_criteria(&[
                ("person", "a person"),
                ("org", "an organization, vendor, or consulting firm"),
                ("topic", "a topic"),
                ("project", "a named project"),
            ]),
        },
    );
    let kind_resp = eval(
        provider,
        JevOp::Judge,
        json!({"passage": passage, "mention": mention}),
        kind_q,
    )
    .await?;
    let mention_kind = kind_resp
        .answers
        .get("kind")
        .and_then(Answer::as_choice)
        .unwrap_or("topic")
        .to_string();

    let mut bind_criteria = BTreeMap::new();
    for (i, c) in candidates.iter().enumerate() {
        bind_criteria.insert(
            i.to_string(),
            Some(format!(
                "{} (coral_count={}, {})",
                c.title,
                c.coral_count,
                if c.is_self { "self" } else { "candidate" }
            )),
        );
    }
    bind_criteria.insert(
        "different".into(),
        Some(
            "Different entity — including a type mismatch (person vs org that shares a name)"
                .into(),
        ),
    );
    bind_criteria.insert(
        "unsure".into(),
        Some("Not enough to decide; mint rather than guess".into()),
    );

    let mut bind_q = BTreeMap::new();
    bind_q.insert(
        "bind".into(),
        Question::Choice {
            instructions: json!({
                "task": "Which candidate does this mention refer to? A clear TYPE mismatch is DIFFERENT even with a shared name.",
                "mention_kind": mention_kind,
            }),
            criteria: bind_criteria,
        },
    );
    let cand_json: Vec<Value> = candidates
        .iter()
        .enumerate()
        .map(|(i, c)| {
            json!({
                "index": i,
                "title": c.title,
                "aliases": c.aliases,
                "coral_count": c.coral_count,
                "is_self": c.is_self,
            })
        })
        .collect();
    let bind_resp = eval(
        provider,
        JevOp::Judge,
        json!({
            "passage": passage,
            "mention": mention,
            "mention_kind": mention_kind,
            "candidates": cand_json,
        }),
        bind_q,
    )
    .await?;
    let ans = bind_resp.answers.get("bind");
    let choice = ans.and_then(Answer::as_choice).unwrap_or("unsure");
    let confidence = ans.map(Answer::choice_confidence).unwrap_or(0.0);
    Ok(match choice {
        "different" => MentionJudge::Different {
            confidence,
            reason: format!("jev kind-first mention_kind={mention_kind}"),
        },
        "unsure" => MentionJudge::Unsure {
            confidence,
            reason: format!("jev kind-first mention_kind={mention_kind}"),
        },
        idx => match idx.parse::<usize>() {
            Ok(i) if i < candidates.len() => MentionJudge::Same {
                matched_index: i,
                confidence,
                reason: format!("jev kind-first mention_kind={mention_kind}"),
            },
            _ => MentionJudge::Unsure {
                confidence,
                reason: format!("jev bind {idx:?} out of range; treating as unsure"),
            },
        },
    })
}

/// Capture save + correction Nouls. Statement prose stays an LLM writer.
pub async fn judge_capture(
    provider: &dyn DecisionProvider,
    latest: &str,
    history: &[(String, String)],
) -> Result<(bool, bool), DecisionError> {
    let mut questions = BTreeMap::new();
    questions.insert(
        "save".into(),
        Question::Noul {
            instructions: json!({
                "task": "Does the LATEST message state a durable, save-worthy fact? Questions, greetings, and tentative opinions are false."
            }),
            criteria: Some(NoulCriteria {
                yes: "Durable asserted fact or confirmed decision worth remembering".into(),
                no: "Question, greeting, opinion, small talk, or request".into(),
            }),
        },
    );
    questions.insert(
        "correction".into(),
        Question::Noul {
            instructions: json!({
                "task": "If saved, is the latest message a correction of earlier conversation? False when not a save."
            }),
            criteria: Some(NoulCriteria {
                yes: "Fixes, overrides, or refines something stated earlier".into(),
                no: "Net-new, or not a save".into(),
            }),
        },
    );
    let resp = eval(
        provider,
        JevOp::Ask,
        json!({"latest": latest, "history": history}),
        questions,
    )
    .await?;
    let save = resp
        .answers
        .get("save")
        .and_then(Answer::as_noul)
        .unwrap_or(0.0)
        >= NOUL_YES;
    let correction = save
        && resp
            .answers
            .get("correction")
            .and_then(Answer::as_noul)
            .unwrap_or(0.0)
            >= NOUL_YES;
    Ok((save, correction))
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogCard {
    pub id: Uuid,
    pub title: String,
    pub excerpt: String,
    pub reef_type: String,
    pub coral_count: i64,
    /// Named on title/aliases or retrieve-bound. Popularity fill is `false`.
    pub bound: bool,
}

/// Which Noul the catalog judge asks. Entities and decisions are different
/// judgments and never share a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogNoul {
    /// "Is this reef a source for the ask?"
    Source,
    /// "Is this the current account of a fact the ask needs?"
    Current,
}

impl CatalogNoul {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Current => "current",
        }
    }
}

/// Skinny catalog table size after typed seating.
pub const CATALOG_TABLE_CAP: usize = 48;
/// Reserved seats per type (entity / decision) inside [`CATALOG_TABLE_CAP`].
pub const CATALOG_TYPE_FLOOR: usize = 24;
/// Digest prefix on a catalog card (not the old 400-char excerpt).
pub const CATALOG_DIGEST_CHARS: usize = 160;
/// Soft per-type floor inside [`MAX_YES_REEFS_FETCH`].
pub const YES_REEF_TYPE_FLOOR: usize = 8;

/// Corals fetched per catalog-yes reef (digest excerpt ∪ keyword ∪ vector).
pub const CORALS_PER_YES_REEF: usize = 8;
/// Bound on yes-reefs we fetch corals for (highest Noul first).
pub const MAX_YES_REEFS_FETCH: usize = 16;

/// Present-state tokens OR'd into within-reef keyword. Membership already
/// filters to the reef; ask+title alone is drowned by the reef's own name.
pub const KEYWORD_STATE_TOKENS: &[&str] = &["production", "current", "serving", "hosted", "running"];

const ASK_STOPWORDS: &[&str] = &[
    "a", "about", "an", "and", "any", "are", "as", "at", "be", "but", "by", "can",
    "could", "did", "do", "does", "explain", "for", "from", "give", "had", "has",
    "have", "he", "her", "him", "his", "how", "i", "in", "is", "it", "its", "me",
    "my", "of", "on", "or", "please", "she", "should", "show", "tell", "than",
    "that", "the", "their", "them", "they", "this", "to", "use", "used", "using",
    "vs", "was", "we", "were", "what", "when", "where", "which", "who", "whom",
    "why", "with", "would", "you", "your",
];

/// `to_tsquery('simple', …)` of stopword-stripped ask tokens, reef title
/// tokens, and [`KEYWORD_STATE_TOKENS`], joined with `|`.
pub fn reef_keyword_tsquery(ask: &str, title: &str) -> String {
    let stop: HashSet<&str> = ASK_STOPWORDS.iter().copied().collect();
    let mut seen: HashSet<String> = HashSet::new();
    let mut toks: Vec<String> = Vec::new();
    let extra = KEYWORD_STATE_TOKENS.join(" ");
    for raw in ask
        .split(|c: char| !c.is_ascii_alphanumeric())
        .chain(title.split(|c: char| !c.is_ascii_alphanumeric()))
        .chain(extra.split(|c: char| !c.is_ascii_alphanumeric()))
    {
        if raw.is_empty() {
            continue;
        }
        let key = raw.to_ascii_lowercase();
        if stop.contains(key.as_str()) || !seen.insert(key) {
            continue;
        }
        toks.push(raw.to_string());
    }
    toks.join(" | ")
}

pub fn clip_catalog_digest(summary: &str) -> String {
    summary.chars().take(CATALOG_DIGEST_CHARS).collect()
}

/// Named (title/tsv) hits take slots first so a low-`coral_count` reef the ask
/// names can enter the catalog. Prefer [`seat_catalog_cards`] for the live path.
pub fn merge_catalog_cards(
    named: &[CatalogCard],
    popular: &[CatalogCard],
    cap: usize,
) -> Vec<CatalogCard> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for c in named.iter().chain(popular) {
        if !seen.insert(c.id) {
            continue;
        }
        out.push(c.clone());
        if out.len() >= cap {
            break;
        }
    }
    out
}

fn take_unique<'a>(cards: impl IntoIterator<Item = &'a CatalogCard>, dest: &mut Vec<CatalogCard>) {
    let mut seen: HashSet<Uuid> = dest.iter().map(|c| c.id).collect();
    for c in cards {
        if seen.insert(c.id) {
            dest.push(c.clone());
        }
    }
}

fn mark_bound(cards: &[CatalogCard], reef_type: &str) -> Vec<CatalogCard> {
    let mut out = Vec::new();
    take_unique(cards, &mut out);
    for c in &mut out {
        c.bound = true;
        c.reef_type = reef_type.to_string();
    }
    out
}

/// Soft 24/24 inside 48: named ∪ retrieve take their type's floor first.
/// Overflow may take the other type's unused reserved seats. Popularity
/// fills only leftover seats of its own type.
pub fn seat_catalog_cards(
    entity_bound: &[CatalogCard],
    decision_bound: &[CatalogCard],
    entity_popular: &[CatalogCard],
    decision_popular: &[CatalogCard],
    cap: usize,
    floor: usize,
) -> Vec<CatalogCard> {
    if cap == 0 {
        return Vec::new();
    }
    let floor = floor.min(cap);
    let e_bound = mark_bound(entity_bound, "entity");
    let d_bound = mark_bound(decision_bound, "decision");

    let e_own = e_bound.len().min(floor);
    let d_own = d_bound.len().min(floor);
    let e_overflow = e_bound.len().saturating_sub(floor);
    let d_overflow = d_bound.len().saturating_sub(floor);
    let e_unused = floor.saturating_sub(e_own);
    let d_unused = floor.saturating_sub(d_own);
    let e_from_other = e_overflow.min(d_unused);
    let d_from_other = d_overflow.min(e_unused);

    let e_bound_take = e_own + e_from_other;
    let d_bound_take = d_own + d_from_other;
    let e_pop_budget = e_unused.saturating_sub(d_from_other);
    let d_pop_budget = d_unused.saturating_sub(e_from_other);

    let bound_ids: HashSet<Uuid> = e_bound
        .iter()
        .chain(d_bound.iter())
        .map(|c| c.id)
        .collect();
    let mut e_pop = Vec::new();
    let mut d_pop = Vec::new();
    for c in entity_popular {
        if bound_ids.contains(&c.id) {
            continue;
        }
        let mut x = c.clone();
        x.bound = false;
        x.reef_type = "entity".into();
        e_pop.push(x);
    }
    for c in decision_popular {
        if bound_ids.contains(&c.id) {
            continue;
        }
        let mut x = c.clone();
        x.bound = false;
        x.reef_type = "decision".into();
        d_pop.push(x);
    }

    let mut out = Vec::new();
    out.extend(e_bound.into_iter().take(e_bound_take));
    out.extend(d_bound.into_iter().take(d_bound_take));
    out.extend(e_pop.into_iter().take(e_pop_budget));
    out.extend(d_pop.into_iter().take(d_pop_budget));
    if out.len() > cap {
        out.truncate(cap);
    }
    out
}

/// Bound cards of a failed type become yes (score 1.0). Popularity never does.
pub fn catalog_fail_open_scores(cards: &[CatalogCard]) -> Vec<(Uuid, f64)> {
    cards
        .iter()
        .filter(|c| c.bound)
        .map(|c| (c.id, 1.0))
        .collect()
}

/// Yes-reefs for the ask, highest Noul first, capped.
pub fn select_yes_reefs(scores: &[(Uuid, f64)], threshold: f64, max: usize) -> Vec<Uuid> {
    let mut yes: Vec<(Uuid, f64)> = scores
        .iter()
        .copied()
        .filter(|(_, n)| *n >= threshold)
        .collect();
    yes.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    yes.into_iter().take(max).map(|(id, _)| id).collect()
}

/// Soft 8/8 inside 16: unused entity fetch seats go to extra yes-decisions
/// (and the reverse). Result is highest Noul first among the selected set.
pub fn select_yes_reefs_typed(
    scores: &[(Uuid, f64)],
    cards: &[CatalogCard],
    threshold: f64,
    max: usize,
    floor: usize,
) -> Vec<Uuid> {
    if max == 0 {
        return Vec::new();
    }
    let floor = floor.min(max);
    let types: HashMap<Uuid, &str> = cards.iter().map(|c| (c.id, c.reef_type.as_str())).collect();
    let mut entity = Vec::new();
    let mut decision = Vec::new();
    for &(id, n) in scores {
        if n < threshold {
            continue;
        }
        match types.get(&id).copied() {
            Some("decision") => decision.push((id, n)),
            Some("entity") => entity.push((id, n)),
            _ => {}
        }
    }
    let by_noul = |a: &(Uuid, f64), b: &(Uuid, f64)| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    };
    entity.sort_by(by_noul);
    decision.sort_by(by_noul);

    let e_own = entity.len().min(floor);
    let d_own = decision.len().min(floor);
    let e_take = e_own + entity.len().saturating_sub(floor).min(floor.saturating_sub(d_own));
    let d_take = d_own + decision.len().saturating_sub(floor).min(floor.saturating_sub(e_own));
    let mut picked: Vec<(Uuid, f64)> = entity.into_iter().take(e_take).collect();
    picked.extend(decision.into_iter().take(d_take));
    picked.sort_by(by_noul);
    picked.into_iter().take(max).map(|(id, _)| id).collect()
}

/// Digest-excerpt atoms first, then latest-atom fill, unique, at most `n` per reef.
pub fn pick_corals_per_reef(
    digest: &[(Uuid, Uuid)],
    latest: &[(Uuid, Uuid)],
    n: usize,
) -> Vec<(Uuid, Uuid)> {
    if n == 0 {
        return Vec::new();
    }
    let mut per: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    let mut seen_atoms = HashSet::new();
    for &(node, atom) in digest.iter().chain(latest.iter()) {
        if !seen_atoms.insert(atom) {
            continue;
        }
        let slot = per.entry(node).or_default();
        if slot.len() < n {
            slot.push(atom);
        }
    }
    let mut out = Vec::new();
    let mut emitted = HashSet::new();
    for &(node, _) in digest.iter().chain(latest.iter()) {
        if !emitted.insert(node) {
            continue;
        }
        if let Some(atoms) = per.get(&node) {
            for &atom in atoms {
                out.push((node, atom));
            }
        }
    }
    out
}

/// Within-reef hybrid: first digest excerpt, then keyword, then vector fill.
/// Unique, cap `n`. Keyword is the current-fact channel; vector fills with
/// same-reef diaries.
pub fn pick_hybrid_per_reef(
    digest: &[Uuid],
    vector: &[Uuid],
    keyword: &[Uuid],
    n: usize,
) -> Vec<Uuid> {
    if n == 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut push = |id: Uuid| {
        if out.len() >= n || !seen.insert(id) {
            return;
        }
        out.push(id);
    };
    for &id in digest.iter().take(1) {
        push(id);
    }
    for &id in keyword {
        push(id);
    }
    for &id in vector {
        push(id);
    }
    out
}

/// Same as [`pick_hybrid_per_reef`], then recent `node_atoms` membership when
/// digest ∪ keyword ∪ vector all missed (reefs with empty `digest.excerpts`
/// and no lexical/vector hit).
pub fn pick_hybrid_or_membership(
    digest: &[Uuid],
    vector: &[Uuid],
    keyword: &[Uuid],
    membership: &[Uuid],
    n: usize,
) -> Vec<Uuid> {
    let out = pick_hybrid_per_reef(digest, vector, keyword, n);
    if out.is_empty() {
        membership.iter().copied().take(n).collect()
    } else {
        out
    }
}

/// After Noul seats `yes`, apply the yes-reef coral fetch.
///
/// Packing (grill Q14) is gated on seated yes-reefs, not on whether those
/// reefs yielded corals. Empty fetch fail-opens the *pool* — leave retrieve
/// as-is (`None`) — and **must not unseat** `yes`. Nonempty fetch replaces
/// the pool with [`topic_first_pool`].
pub fn apply_catalog_fetch(
    yes: HashSet<Uuid>,
    fetched: Vec<RankedAtom>,
    retrieve: &[RankedAtom],
    keep: &HashSet<Uuid>,
) -> (HashSet<Uuid>, Option<Vec<RankedAtom>>) {
    if yes.is_empty() || fetched.is_empty() {
        return (yes, None);
    }
    (
        yes.clone(),
        Some(topic_first_pool(retrieve, &yes, fetched, keep)),
    )
}

/// Pack pool after catalog Noul: fetched N-per-yes-reef first, then
/// event-window atoms. Retrieve hits are not kept merely because they sit on
/// a yes-reef. Empty yes or empty result fail-opens to `retrieve`.
pub fn topic_first_pool(
    retrieve: &[RankedAtom],
    yes_reefs: &HashSet<Uuid>,
    fetched: Vec<RankedAtom>,
    keep: &HashSet<Uuid>,
) -> Vec<RankedAtom> {
    if yes_reefs.is_empty() {
        return retrieve.to_vec();
    }
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for a in fetched {
        if seen.insert(a.atom_id) {
            out.push(a);
        }
    }
    for a in retrieve {
        if keep.contains(&a.atom_id) && seen.insert(a.atom_id) {
            out.push(a.clone());
        }
    }
    if out.is_empty() {
        return retrieve.to_vec();
    }
    out
}

/// Noul per catalog reef. `Source` and `Current` are different questions;
/// callers must not mix entity and decision cards in one request.
pub async fn judge_catalog_scan(
    provider: &dyn DecisionProvider,
    ask: &str,
    catalog: &[CatalogCard],
    kind: CatalogNoul,
) -> Result<Vec<(Uuid, f64)>, DecisionError> {
    if catalog.is_empty() {
        return Ok(Vec::new());
    }
    let (task, yes, no) = match kind {
        CatalogNoul::Source => (
            "Should this reef be a source for the ask?",
            "This entity is a source the ask needs",
            "Off-topic, hub, trap, or not a source for the ask",
        ),
        CatalogNoul::Current => (
            "Is this the current account of a fact the ask needs?",
            "Current, still in force, a fact the ask needs",
            "Historical, superseded, not current, or not a fact the ask needs",
        ),
    };
    let mut questions = BTreeMap::new();
    for c in catalog {
        questions.insert(
            format!("rel_{}", c.id),
            Question::Noul {
                instructions: json!({
                    "task": task,
                    "reef_id": c.id.to_string(),
                }),
                criteria: Some(NoulCriteria {
                    yes: yes.into(),
                    no: no.into(),
                }),
            },
        );
    }
    let visible: Vec<Value> = catalog
        .iter()
        .map(|c| {
            json!({
                "id": c.id,
                "title": c.title,
                "type": c.reef_type,
                "digest": c.excerpt,
            })
        })
        .collect();
    let resp = eval(
        provider,
        JevOp::Ask,
        json!({
            "ask": ask,
            "task": task,
            "catalog": visible,
        }),
        questions,
    )
    .await?;
    Ok(catalog
        .iter()
        .map(|c| {
            let n = resp
                .answers
                .get(&format!("rel_{}", c.id))
                .and_then(Answer::as_noul)
                .unwrap_or(0.0);
            (c.id, n)
        })
        .collect())
}

/// Transient retries live in the provider. A 400 shrinks once (popularity
/// off, then one split of the bound remainder) and then the caller
/// fail-opens. The whole chain is bounded by [`super::within_ask_budget`].
pub async fn judge_catalog_scan_with_retry(
    provider: &dyn DecisionProvider,
    ask: &str,
    catalog: &[CatalogCard],
    kind: CatalogNoul,
) -> Result<Vec<(Uuid, f64)>, DecisionError> {
    super::within_ask_phase("catalog_scan", catalog_scan_chain(provider, ask, catalog, kind)).await
}

async fn catalog_scan_chain(
    provider: &dyn DecisionProvider,
    ask: &str,
    catalog: &[CatalogCard],
    kind: CatalogNoul,
) -> Result<Vec<(Uuid, f64)>, DecisionError> {
    match judge_catalog_scan(provider, ask, catalog, kind).await {
        Ok(s) => Ok(s),
        Err(e) if e.is_context_overflow() => {
            let bound: Vec<CatalogCard> = catalog.iter().filter(|c| c.bound).cloned().collect();
            let shrink = if bound.is_empty() || bound.len() == catalog.len() {
                catalog
            } else {
                bound.as_slice()
            };
            match judge_catalog_scan(provider, ask, shrink, kind).await {
                Ok(s) => Ok(s),
                Err(e2) if e2.is_context_overflow() => {
                    judge_catalog_scan_split(provider, ask, shrink, kind).await
                }
                other => other,
            }
        }
        Err(e) => Err(e),
    }
}

async fn judge_catalog_scan_split(
    provider: &dyn DecisionProvider,
    ask: &str,
    catalog: &[CatalogCard],
    kind: CatalogNoul,
) -> Result<Vec<(Uuid, f64)>, DecisionError> {
    if catalog.len() <= 1 {
        return judge_catalog_scan(provider, ask, catalog, kind).await;
    }
    let mid = catalog.len() / 2;
    let (left, right) = tokio::join!(
        judge_catalog_scan(provider, ask, &catalog[..mid], kind),
        judge_catalog_scan(provider, ask, &catalog[mid..], kind),
    );
    match (left, right) {
        (Ok(mut a), Ok(b)) => {
            a.extend(b);
            Ok(a)
        }
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}

/// Cross-ocean contradiction: Noul per pair. Same-ocean pairs must be skipped
/// in code before calling.
pub async fn judge_contradiction_pair(
    provider: &dyn DecisionProvider,
    a_ocean: &str,
    a_text: &str,
    b_ocean: &str,
    b_text: &str,
) -> Result<f64, DecisionError> {
    let mut questions = BTreeMap::new();
    questions.insert(
        "x".into(),
        Question::Noul {
            instructions: json!({
                "task": "Do these two sources make claims that cannot both be true, AND do they come from DIFFERENT oceans? Same-ocean disagreement is false. Wording-only difference is false."
            }),
            criteria: Some(NoulCriteria {
                yes: "Genuine factual contradiction across two oceans".into(),
                no: "Same ocean, agreement, wording difference, or unrelated facts".into(),
            }),
        },
    );
    let resp = eval(
        provider,
        JevOp::Ask,
        json!({
            "a": {"ocean": a_ocean, "text": a_text},
            "b": {"ocean": b_ocean, "text": b_text},
        }),
        questions,
    )
    .await?;
    Ok(resp
        .answers
        .get("x")
        .and_then(Answer::as_noul)
        .unwrap_or(0.0))
}

/// Event-window period enum. `day_range` / `explicit` mean the caller should
/// keep the LLM writer for integers/ISO.
pub async fn judge_event_period(
    provider: &dyn DecisionProvider,
    question: &str,
    now_rfc3339: &str,
    tz: &str,
) -> Result<String, DecisionError> {
    let mut questions = BTreeMap::new();
    questions.insert(
        "period".into(),
        Question::Choice {
            instructions: json!({
                "task": "Which event-time period does this question name?",
                "now": now_rfc3339,
                "tz": tz,
            }),
            criteria: choice_criteria(&[
                (
                    "none",
                    "No event-time window; the question is not about a calendar period.",
                ),
                ("today", "Today."),
                ("yesterday", "Yesterday."),
                ("this_week", "This calendar week."),
                ("last_week", "Last calendar week."),
                ("this_month", "This calendar month."),
                ("last_month", "Last calendar month."),
                ("weekday", "A named weekday (Monday … Sunday)."),
                (
                    "day_range",
                    "A relative day range that needs numeric edges.",
                ),
                ("explicit", "An explicit calendar date or ISO timestamp."),
            ]),
        },
    );
    let resp = eval(
        provider,
        JevOp::Ask,
        json!({"question": question, "now": now_rfc3339, "tz": tz}),
        questions,
    )
    .await?;
    Ok(resp
        .answers
        .get("period")
        .and_then(Answer::as_choice)
        .unwrap_or("none")
        .to_string())
}

pub async fn judge_weekday(
    provider: &dyn DecisionProvider,
    question: &str,
    now_rfc3339: &str,
    tz: &str,
) -> Result<String, DecisionError> {
    let mut questions = BTreeMap::new();
    questions.insert(
        "weekday".into(),
        Question::Choice {
            instructions: json!({
                "task": "The period is already weekday. Which weekday does this question name?",
                "now": now_rfc3339,
            }),
            criteria: choice_criteria(&[
                ("monday", "Monday"),
                ("tuesday", "Tuesday"),
                ("wednesday", "Wednesday"),
                ("thursday", "Thursday"),
                ("friday", "Friday"),
                ("saturday", "Saturday"),
                ("sunday", "Sunday"),
            ]),
        },
    );
    let resp = eval(
        provider,
        JevOp::Ask,
        json!({"question": question, "now": now_rfc3339, "tz": tz, "period": "weekday"}),
        questions,
    )
    .await?;
    Ok(resp
        .answers
        .get("weekday")
        .and_then(Answer::as_choice)
        .unwrap_or("monday")
        .to_string())
}

/// Digest picker: Noul per already-selected coral. Writer stays DIGEST_MODEL.
/// Empty keep-set means the caller should fail-open to the unfiltered list.
pub async fn judge_digest_keep(
    provider: &dyn DecisionProvider,
    reef_title: &str,
    items: &[(Uuid, String)],
) -> Result<Vec<(Uuid, f64)>, DecisionError> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let resp = provider.evaluate(digest_keep_request(reef_title, items)).await?;
    Ok(digest_keep_scores(&resp, items))
}

/// The digest picker's request, built without a provider so the digest
/// worker can send it through the managed gate
/// (`decision::openrouter::evaluate_managed_enrichment`).
pub(crate) fn digest_keep_request(reef_title: &str, items: &[(Uuid, String)]) -> DecisionRequest {
    let mut questions = BTreeMap::new();
    for (id, _) in items {
        questions.insert(
            format!("keep_{id}"),
            Question::Noul {
                instructions: json!({
                    "task": "Should this coral stay in the reef digest?",
                    "atom_id": id.to_string(),
                }),
                criteria: Some(NoulCriteria {
                    yes: "Current, on-topic evidence for this reef".into(),
                    no: "Stale, off-topic, duplicate, or trap".into(),
                }),
            },
        );
    }
    let visible: Vec<Value> = items
        .iter()
        .map(|(id, body)| {
            let excerpt: String = body.chars().take(400).collect();
            json!({"atom_id": id, "excerpt": excerpt})
        })
        .collect();
    DecisionRequest {
        state: json!({
            "reef_title": reef_title,
            "task": "Pick which corals belong in this reef's digest.",
            "corals": visible,
        }),
        model: jev_model_for(JevOp::Judge),
        questions,
        op: JevOp::Judge,
    }
}

/// `(atom, keep score)` per item from a [`digest_keep_request`] answer; a
/// missing answer scores 0.
pub(crate) fn digest_keep_scores(resp: &super::DecisionResponse, items: &[(Uuid, String)]) -> Vec<(Uuid, f64)> {
    items
        .iter()
        .map(|(id, _)| {
            let n = resp
                .answers
                .get(&format!("keep_{id}"))
                .and_then(Answer::as_noul)
                .unwrap_or(0.0);
            (*id, n)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{
        Answer, DecisionError, DecisionProvider, DecisionRequest, DecisionResponse,
    };

    #[test]
    fn judge_mode_parses() {
        assert_eq!(JevJudgeMode::from_env_str(""), JevJudgeMode::Off);
        assert_eq!(JevJudgeMode::from_env_str("SHADOW"), JevJudgeMode::Shadow);
        assert!(JevJudgeMode::from_env_str("live").is_live());
        assert!(!JevCatalogScanMode::from_env_str("off").is_active());
        assert!(JevCatalogScanMode::from_env_str("shadow").is_active());
    }

    fn card(id: u128, title: &str, n: i64) -> CatalogCard {
        CatalogCard {
            id: Uuid::from_u128(id),
            title: title.into(),
            excerpt: String::new(),
            reef_type: "entity".into(),
            coral_count: n,
            bound: true,
        }
    }

    fn typed(id: u128, reef_type: &str, bound: bool) -> CatalogCard {
        CatalogCard {
            id: Uuid::from_u128(id),
            title: format!("{reef_type}-{id}"),
            excerpt: String::new(),
            reef_type: reef_type.into(),
            coral_count: 1,
            bound,
        }
    }

    fn ra(atom: u128, node: Option<u128>) -> RankedAtom {
        RankedAtom {
            atom_id: Uuid::from_u128(atom),
            score: 1.0,
            node_id: node.map(Uuid::from_u128),
            vector_sim: 0.0,
            keyword_norm: 1.0,
            recency_decay: 1.0,
        }
    }

    #[test]
    fn merge_named_first_then_cap() {
        let popular: Vec<_> = (1..=3).map(|i| card(i, "hub", 100)).collect();
        let named = vec![card(99, "DeepSeek", 2), card(1, "hub", 100)];
        let out = merge_catalog_cards(&named, &popular, 3);
        let ids: Vec<_> = out.iter().map(|c| c.id).collect();
        assert_eq!(
            ids,
            vec![Uuid::from_u128(99), Uuid::from_u128(1), Uuid::from_u128(2)]
        );
    }

    #[test]
    fn select_yes_reefs_orders_and_caps() {
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let c = Uuid::from_u128(3);
        let yes = select_yes_reefs(&[(a, 0.4), (b, 0.9), (c, 0.7)], 0.5, 1);
        assert_eq!(yes, vec![b]);
    }

    #[test]
    fn pick_digest_then_latest_cap() {
        let reef = Uuid::from_u128(1);
        let d1 = Uuid::from_u128(10);
        let d2 = Uuid::from_u128(11);
        let l1 = Uuid::from_u128(12);
        let l2 = Uuid::from_u128(13);
        let out = pick_corals_per_reef(&[(reef, d1), (reef, d2)], &[(reef, l1), (reef, l2)], 3);
        assert_eq!(out, vec![(reef, d1), (reef, d2), (reef, l1)]);
    }

    #[test]
    fn pick_skips_duplicate_atom() {
        let reef = Uuid::from_u128(1);
        let a = Uuid::from_u128(10);
        let out = pick_corals_per_reef(&[(reef, a)], &[(reef, a)], 3);
        assert_eq!(out, vec![(reef, a)]);
    }

    #[test]
    fn topic_first_drops_retrieve_even_on_yes() {
        let yes = HashSet::from([Uuid::from_u128(1)]);
        let retrieve = vec![ra(10, Some(1)), ra(11, Some(2)), ra(12, None)];
        let fetched = vec![ra(99, Some(1))];
        let out = topic_first_pool(&retrieve, &yes, fetched, &HashSet::new());
        let ids: Vec<_> = out.iter().map(|a| a.atom_id).collect();
        assert_eq!(ids, vec![Uuid::from_u128(99)]);
    }

    #[test]
    fn topic_first_keeps_window_off_reef() {
        let yes = HashSet::from([Uuid::from_u128(1)]);
        let retrieve = vec![ra(11, Some(2))];
        let keep = HashSet::from([Uuid::from_u128(11)]);
        let out = topic_first_pool(&retrieve, &yes, vec![], &keep);
        assert_eq!(out[0].atom_id, Uuid::from_u128(11));
    }

    #[test]
    fn topic_first_fail_open_empty_yes() {
        let retrieve = vec![ra(10, Some(1))];
        let out = topic_first_pool(&retrieve, &HashSet::new(), vec![], &HashSet::new());
        assert_eq!(out[0].atom_id, Uuid::from_u128(10));
    }

    #[test]
    fn topic_first_fail_open_empty_pool() {
        let yes = HashSet::from([Uuid::from_u128(1)]);
        let retrieve = vec![ra(11, Some(2))];
        let out = topic_first_pool(&retrieve, &yes, vec![], &HashSet::new());
        assert_eq!(out[0].atom_id, Uuid::from_u128(11));
    }

    #[test]
    fn empty_coral_fetch_does_not_unseat_yes_reefs() {
        let yes = HashSet::from([Uuid::from_u128(1)]);
        let retrieve = vec![ra(11, Some(2))];
        let (seated, pool) = apply_catalog_fetch(yes.clone(), vec![], &retrieve, &HashSet::new());
        assert_eq!(
            seated, yes,
            "Q14 packing keys off seated yes-reefs, not fetch"
        );
        assert!(
            pool.is_none(),
            "empty fetch leaves retrieve in place rather than replacing it"
        );
    }

    #[test]
    fn nonempty_coral_fetch_replaces_pool_and_keeps_yes() {
        let yes = HashSet::from([Uuid::from_u128(1)]);
        let retrieve = vec![ra(11, Some(2))];
        let fetched = vec![ra(99, Some(1))];
        let (seated, pool) = apply_catalog_fetch(yes.clone(), fetched, &retrieve, &HashSet::new());
        assert_eq!(seated, yes);
        let ids: Vec<_> = pool
            .expect("pool replaced")
            .iter()
            .map(|a| a.atom_id)
            .collect();
        assert_eq!(ids, vec![Uuid::from_u128(99)]);
    }

    #[test]
    fn pick_hybrid_or_membership_fills_when_lanes_miss() {
        let m1 = Uuid::from_u128(7);
        let m2 = Uuid::from_u128(8);
        assert_eq!(
            pick_hybrid_or_membership(&[], &[], &[], &[m1, m2], 8),
            vec![m1, m2]
        );
        let d = Uuid::from_u128(1);
        assert_eq!(
            pick_hybrid_or_membership(&[d], &[], &[], &[m1], 8),
            vec![d],
            "membership must not override a hybrid hit"
        );
    }

    #[test]
    fn pick_hybrid_digest_then_keyword_then_vector_cap() {
        let d = Uuid::from_u128(1);
        let k1 = Uuid::from_u128(2);
        let k2 = Uuid::from_u128(3);
        let v1 = Uuid::from_u128(4);
        let v2 = Uuid::from_u128(2); // dup of k1
        let out = pick_hybrid_per_reef(&[d], &[v1, v2], &[k1, k2], 3);
        assert_eq!(out, vec![d, k1, k2]);
    }

    #[test]
    fn pick_hybrid_unique_cap_and_empty_n() {
        assert!(pick_hybrid_per_reef(&[Uuid::from_u128(1)], &[], &[], 0).is_empty());
        let a = Uuid::from_u128(9);
        assert_eq!(pick_hybrid_per_reef(&[a], &[a], &[a], 8), vec![a]);
    }

    #[test]
    fn pick_hybrid_vector_fills_remaining() {
        let d = Uuid::from_u128(1);
        let k = Uuid::from_u128(2);
        let v1 = Uuid::from_u128(3);
        let v2 = Uuid::from_u128(4);
        let out = pick_hybrid_per_reef(&[d], &[v1, v2], &[k], 4);
        assert_eq!(out, vec![d, k, v1, v2]);
    }

    #[test]
    fn reef_keyword_tsquery_ors_title_and_state() {
        let q = reef_keyword_tsquery("what is coralswarms infrastructure", "OpenRouter");
        assert!(q.contains("coralswarms"));
        assert!(q.contains("infrastructure"));
        assert!(q.contains("OpenRouter"));
        assert!(q.contains("production"));
        assert!(!q.to_ascii_lowercase().split(" | ").any(|t| t == "what" || t == "is"));
    }

    #[test]
    fn clip_digest_is_160() {
        let long = "x".repeat(400);
        assert_eq!(clip_catalog_digest(&long).chars().count(), CATALOG_DIGEST_CHARS);
        assert_eq!(clip_catalog_digest("short"), "short");
    }

    #[test]
    fn seat_balanced_bound_fills_own_popularity() {
        let e_bound: Vec<_> = (1..=8).map(|i| typed(i, "entity", true)).collect();
        let d_bound: Vec<_> = (101..=108).map(|i| typed(i, "decision", true)).collect();
        let e_pop: Vec<_> = (201..=240).map(|i| typed(i, "entity", false)).collect();
        let d_pop: Vec<_> = (301..=340).map(|i| typed(i, "decision", false)).collect();
        let out = seat_catalog_cards(&e_bound, &d_bound, &e_pop, &d_pop, 48, 24);
        let entities = out.iter().filter(|c| c.reef_type == "entity").count();
        let decisions = out.iter().filter(|c| c.reef_type == "decision").count();
        assert_eq!(entities, 24);
        assert_eq!(decisions, 24);
        assert_eq!(out.iter().filter(|c| c.bound && c.reef_type == "entity").count(), 8);
        assert_eq!(out.iter().filter(|c| !c.bound && c.reef_type == "entity").count(), 16);
    }

    #[test]
    fn seat_decision_overflow_takes_unused_entity_reserved() {
        let e_bound: Vec<_> = (1..=8).map(|i| typed(i, "entity", true)).collect();
        let d_bound: Vec<_> = (101..=130).map(|i| typed(i, "decision", true)).collect();
        let e_pop: Vec<_> = (201..=240).map(|i| typed(i, "entity", false)).collect();
        let out = seat_catalog_cards(&e_bound, &d_bound, &e_pop, &[], 48, 24);
        let entities = out.iter().filter(|c| c.reef_type == "entity").count();
        let decisions = out.iter().filter(|c| c.reef_type == "decision").count();
        assert_eq!(decisions, 30);
        assert_eq!(entities, 18);
        assert!(out.iter().filter(|c| c.reef_type == "decision").all(|c| c.bound));
        assert_eq!(out.iter().filter(|c| c.reef_type == "entity" && !c.bound).count(), 10);
    }

    #[test]
    fn seat_popularity_cannot_steal_other_type() {
        let e_bound: Vec<_> = (1..=8).map(|i| typed(i, "entity", true)).collect();
        let d_bound: Vec<_> = (101..=108).map(|i| typed(i, "decision", true)).collect();
        let d_pop: Vec<_> = (301..=400).map(|i| typed(i, "decision", false)).collect();
        let out = seat_catalog_cards(&e_bound, &d_bound, &[], &d_pop, 48, 24);
        assert_eq!(out.iter().filter(|c| c.reef_type == "entity").count(), 8);
        assert_eq!(out.iter().filter(|c| c.reef_type == "decision").count(), 24);
        assert!(!out.iter().any(|c| c.reef_type == "entity" && !c.bound));
    }

    #[test]
    fn seat_both_overflow_hard_floors() {
        let e_bound: Vec<_> = (1..=40).map(|i| typed(i, "entity", true)).collect();
        let d_bound: Vec<_> = (101..=140).map(|i| typed(i, "decision", true)).collect();
        let out = seat_catalog_cards(&e_bound, &d_bound, &[], &[], 48, 24);
        assert_eq!(out.iter().filter(|c| c.reef_type == "entity").count(), 24);
        assert_eq!(out.iter().filter(|c| c.reef_type == "decision").count(), 24);
        assert!(out.iter().all(|c| c.bound));
    }

    #[test]
    fn select_yes_typed_soft_8_8() {
        let cards: Vec<_> = (1..=16)
            .map(|i| typed(i, "entity", true))
            .chain((101..=102).map(|i| typed(i, "decision", true)))
            .collect();
        let mut scores: Vec<(Uuid, f64)> = (1..=16)
            .map(|i| (Uuid::from_u128(i), 0.9))
            .collect();
        scores.push((Uuid::from_u128(101), 0.8));
        scores.push((Uuid::from_u128(102), 0.7));
        let yes = select_yes_reefs_typed(&scores, &cards, 0.5, 16, 8);
        let e = yes.iter().filter(|id| id.as_u128() <= 16).count();
        let d = yes.iter().filter(|id| id.as_u128() >= 101).count();
        assert_eq!(d, 2);
        assert_eq!(e, 14);
        assert_eq!(yes.len(), 16);
    }

    #[test]
    fn select_yes_typed_balanced_floors() {
        let cards: Vec<_> = (1..=10)
            .map(|i| typed(i, "entity", true))
            .chain((101..=110).map(|i| typed(i, "decision", true)))
            .collect();
        let scores: Vec<_> = (1..=10)
            .map(|i| (Uuid::from_u128(i), 0.9))
            .chain((101..=110).map(|i| (Uuid::from_u128(i), 0.8)))
            .collect();
        let yes = select_yes_reefs_typed(&scores, &cards, 0.5, 16, 8);
        assert_eq!(yes.iter().filter(|id| id.as_u128() <= 10).count(), 8);
        assert_eq!(yes.iter().filter(|id| id.as_u128() >= 101).count(), 8);
    }

    #[test]
    fn fail_open_scores_skip_popularity() {
        let cards = vec![typed(1, "entity", true), typed(2, "entity", false)];
        let s = catalog_fail_open_scores(&cards);
        assert_eq!(s, vec![(Uuid::from_u128(1), 1.0)]);
    }

    struct OverflowOnce {
        hits: std::sync::atomic::AtomicU32,
    }

    #[async_trait::async_trait]
    impl DecisionProvider for OverflowOnce {
        async fn evaluate(
            &self,
            req: DecisionRequest,
        ) -> Result<DecisionResponse, DecisionError> {
            let n = self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let catalog = req.state.get("catalog").and_then(|v| v.as_array());
            let len = catalog.map(|c| c.len()).unwrap_or(0);
            if n == 0 {
                return Err(DecisionError::Upstream {
                    status: 400,
                    body: "max_tokens_exceeded".into(),
                });
            }
            let mut answers = BTreeMap::new();
            for c in catalog.into_iter().flatten() {
                if let Some(id) = c.get("id").and_then(|v| v.as_str()) {
                    answers.insert(format!("rel_{id}"), Answer::Noul { noul: 0.9 });
                }
            }
            assert!(len <= 2, "shrink should drop popularity");
            Ok(DecisionResponse {
                model: req.model,
                answers,
                usage: None,
            })
        }
    }

    #[tokio::test]
    async fn catalog_400_drops_popularity_then_succeeds() {
        let cards = vec![
            typed(1, "entity", true),
            typed(2, "entity", true),
            typed(3, "entity", false),
            typed(4, "entity", false),
        ];
        let provider = OverflowOnce {
            hits: std::sync::atomic::AtomicU32::new(0),
        };
        let scores = judge_catalog_scan_with_retry(
            &provider,
            "where is prod",
            &cards,
            CatalogNoul::Source,
        )
        .await
        .unwrap();
        assert_eq!(scores.len(), 2);
        assert!(scores.iter().all(|(id, n)| *n >= 0.9 && id.as_u128() <= 2));
    }

}
