//! Post-synthesis citation check (ADR-0014 / TypeSafe citation-check cookbook).

use std::collections::{BTreeMap, HashMap, HashSet};

use regex::Regex;
use serde_json::json;
use uuid::Uuid;

use super::{
    jev_model_for, Answer, JevOp, DecisionError, DecisionProvider, DecisionRequest, DecisionResponse, Question,
};

pub const CITE_CHOICE_INSTRUCTIONS: &str = "How does the coral relate to the claim?";

pub fn cite_choice_criteria() -> BTreeMap<String, Option<String>> {
    BTreeMap::from([
        (
            "supports".into(),
            Some("The coral states the claim or directly implies that it is true".into()),
        ),
        (
            "contradicts".into(),
            Some("The coral states the opposite of the claim or implies it is false".into()),
        ),
        (
            "says_nothing".into(),
            Some("The coral does not address what the claim asserts, either way".into()),
        ),
    ])
}

/// Lazy static-ish criteria for tests / docs.
pub const CITE_CHOICE_CRITERIA: &str = "supports | contradicts | says_nothing";

pub const HONESTY_ALL_CITES_FAILED: &str = "\
I couldn't verify any of the citations in that answer against the retrieved \
corals, so I shouldn't stand behind it as a cited result. Try rephrasing, or \
ask again after more of the source material is ingested.";

pub const CITE_WINDOW_MIN: usize = 160;
pub const CITE_WINDOW_MAX: usize = 400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CiteVerdict {
    Verified,
    Unsupported,
    Contradicted,
    Fabricated,
    /// Jev 400/error after shrink — keep prose, do not treat as a pass.
    Unverified,
}

impl CiteVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Unsupported => "unsupported",
            Self::Contradicted => "contradicted",
            Self::Fabricated => "fabricated",
            Self::Unverified => "unverified",
        }
    }

    /// Jev judged the cite and it does not support the claim.
    /// `Unverified` is not a failure — the check did not run.
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            Self::Unsupported | Self::Contradicted | Self::Fabricated
        )
    }

    pub fn from_choice(choice: &str) -> Self {
        match choice {
            "supports" => Self::Verified,
            "contradicts" => Self::Contradicted,
            _ => Self::Unsupported,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CiteClaim {
    pub n: Option<usize>,
    pub atom_id: Uuid,
    pub claim: String,
    pub quote: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CiteResult {
    pub atom_id: Uuid,
    pub n: Option<usize>,
    pub verdict: CiteVerdict,
    pub sent_to_jev: bool,
}

#[derive(Debug, Clone)]
pub struct CiteCompose {
    pub results: Vec<CiteResult>,
    pub all_failed: bool,
}

pub fn normalize_quote(text: &str) -> String {
    let table = [
        ('“', '"'),
        ('”', '"'),
        ('„', '"'),
        ('«', '"'),
        ('»', '"'),
        ('‘', '\''),
        ('’', '\''),
    ];
    let mut s = text.to_string();
    for (from, to) in table {
        s = s.replace(from, &to.to_string());
    }
    let collapsed = regex_whitespace().replace_all(&s, " ");
    collapsed.trim().to_string()
}

fn regex_whitespace() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\s+").expect("whitespace regex"))
}

fn regex_cite_marker() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[(\d{1,3})\]").expect("cite marker regex"))
}

/// A double-quoted span of 8+ characters (straight, curly or guillemet
/// marks). Single quotes are not quote marks here: in prose they are
/// apostrophes ("Alex's", "doesn't"), and pairing two of them marked real
/// cites fabricated (11 of 33 on the 2026-10-20 quality eval). Spans pair
/// across the whole answer, so a sentence that starts inside a quotation
/// still gets that quotation, not the gap between two of them.
fn regex_quoted() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"["“”„«»]([^"“”„«»]{8,})["“”„«»]"#).expect("quote regex"))
}

fn regex_marker_run() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^(?:\s*\[\d{1,3}\])+").expect("marker run regex"))
}

/// Closing marks that may sit between a sentence's final `.` and the space.
const CLOSERS: [char; 4] = ['"', '”', '’', ')'];

/// Byte offsets just past each sentence end: a newline, or `.`/`!`/`?`
/// (and any closing quote or parenthesis) followed by whitespace or the end
/// of the text. A `.` inside `1.5`, `fly.toml` or `CLAUDE.md` is not a
/// sentence end.
fn sentence_ends(text: &str) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            '\n' => ends.push(i + 1),
            '.' | '!' | '?' => {
                let mut end = i + 1;
                while let Some(&(j, closer)) = chars.peek() {
                    if !CLOSERS.contains(&closer) {
                        break;
                    }
                    end = j + closer.len_utf8();
                    chars.next();
                }
                if chars.peek().is_none_or(|&(_, next)| next.is_whitespace()) {
                    ends.push(end);
                }
            }
            _ => {}
        }
    }
    ends
}

/// Whether a quoted span appears verbatim in `body`. Punctuation the answer
/// set inside the closing mark (`titled "Fix the queue,"`) is prose, not
/// part of the quotation.
pub fn quote_in_source(quote: &str, body: &str) -> bool {
    let mut needle = normalize_quote(quote);
    needle = needle
        .trim_matches(|c: char| c == '"' || c == '\'')
        .trim()
        .trim_end_matches([',', '.', ';', ':', '!', '?'])
        .trim()
        .to_string();
    if needle.len() < 8 {
        return false;
    }
    normalize_quote(body).contains(&needle)
}

/// Sentence containing `[n]`. Out-of-range markers stay literal prose (not a cite).
/// A marker that trails a finished sentence (`Done. [1]`) cites that sentence.
pub fn claim_sentence(answer: &str, marker_start: usize) -> String {
    claim_span(answer, marker_start).map_or_else(String::new, |(from, to)| answer[from..to].trim().to_string())
}

/// Byte range of [`claim_sentence`].
fn claim_span(answer: &str, marker_start: usize) -> Option<(usize, usize)> {
    if marker_start >= answer.len() || !answer.is_char_boundary(marker_start) {
        return None;
    }
    let ends = sentence_ends(answer);
    let last_end_at_or_before = |pos: usize| ends.iter().rev().find(|&&e| e <= pos).copied().unwrap_or(0);
    let start = last_end_at_or_before(marker_start);
    let lead = regex_cite_marker().replace_all(&answer[start..marker_start], "");
    let lead = lead.trim();
    // `Done.[1]` or `Done. [1]`: the marker run closes the sentence before it.
    let closes_previous = if lead.is_empty() { start > 0 } else { lead.trim_end_matches(CLOSERS).ends_with(['.', '!', '?']) };
    if closes_previous {
        let run = regex_marker_run().find(&answer[marker_start..]).map_or(0, |m| m.end());
        let from = if lead.is_empty() { last_end_at_or_before(start - 1) } else { start };
        return Some((from, marker_start + run));
    }
    let end = ends.iter().find(|&&e| e > marker_start).copied().unwrap_or(answer.len());
    Some((start, end))
}

/// A claim's quoted span is real when it is in the claim's own passage or in
/// a passage cited in the same sentence: `"title" [2][4]` quotes one of them.
fn quote_found(claim: &CiteClaim, claims: &[CiteClaim], bodies: &HashMap<Uuid, String>) -> bool {
    let Some(quote) = claim.quote.as_deref() else { return true };
    claims
        .iter()
        .filter(|c| c.atom_id == claim.atom_id || c.claim == claim.claim)
        .any(|c| quote_in_source(quote, bodies.get(&c.atom_id).map(String::as_str).unwrap_or("")))
}

pub fn extract_numbered_cites(answer: &str, rendered: &[Uuid]) -> Vec<CiteClaim> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let quotes: Vec<(usize, usize, &str)> = regex_quoted()
        .captures_iter(answer)
        .filter_map(|c| {
            let (whole, inner) = (c.get(0)?, c.get(1)?);
            Some((whole.start(), whole.end(), inner.as_str()))
        })
        .collect();
    for cap in regex_cite_marker().captures_iter(answer) {
        let n: usize = match cap.get(1).and_then(|m| m.as_str().parse().ok()) {
            Some(n) => n,
            None => continue,
        };
        if n == 0 || n > rendered.len() {
            continue;
        }
        if !seen.insert(n) {
            continue;
        }
        let whole = cap.get(0).expect("match");
        let Some((from, to)) = claim_span(answer, whole.start()) else { continue };
        let sentence = answer[from..to].trim().to_string();
        // The first quotation that overlaps the sentence; compose_cite then
        // checks quote_in_source.
        let quote = quotes
            .iter()
            .find(|(start, end, _)| *start < to && *end > from)
            .map(|(_, _, inner)| normalize_quote(inner));
        out.push(CiteClaim {
            n: Some(n),
            atom_id: rendered[n - 1],
            claim: sentence,
            quote,
        });
    }
    out
}

/// MCP / non-stream: ids not in the packed/rendered set are fabricated and
/// never sent to Jev (no UUID backfill).
pub fn claims_from_atom_ids(ids: &[Uuid], packed: &HashSet<Uuid>, answer: &str) -> Vec<CiteClaim> {
    let mut seen = HashSet::new();
    ids.iter()
        .filter(|id| seen.insert(**id))
        .map(|&atom_id| CiteClaim {
            n: None,
            atom_id,
            claim: answer.trim().to_string(),
            quote: None,
        })
        .map(|c| {
            if packed.contains(&c.atom_id) {
                c
            } else {
                // Still returned so compose can mark fabricated without Jev.
                c
            }
        })
        .collect()
}

/// Keep prose; flags live on [`CiteResult`]. Do not strip `[n]` markers.
pub fn apply_cite_verdicts(answer: &str, _results: &[CiteResult]) -> String {
    answer.to_string()
}

/// Mark every on-desk cite unverified (off-desk stays fabricated). Used when
/// Jev 400/errors after the claim-window shrink.
pub fn unverified_compose(claims: &[CiteClaim], packed: &HashSet<Uuid>) -> CiteCompose {
    let results = claims
        .iter()
        .map(|claim| {
            if packed.contains(&claim.atom_id) {
                CiteResult {
                    atom_id: claim.atom_id,
                    n: claim.n,
                    verdict: CiteVerdict::Unverified,
                    sent_to_jev: false,
                }
            } else {
                CiteResult {
                    atom_id: claim.atom_id,
                    n: claim.n,
                    verdict: CiteVerdict::Fabricated,
                    sent_to_jev: false,
                }
            }
        })
        .collect::<Vec<_>>();
    CiteCompose {
        results,
        all_failed: false,
    }
}

/// Full body when it fits in [`CITE_WINDOW_MAX`]; otherwise a 160–400 char
/// window around the quoted claim (or the start of the coral).
pub fn claim_window(body: &str, quote: Option<&str>) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let chars: Vec<char> = trimmed.chars().collect();
    if chars.len() <= CITE_WINDOW_MAX {
        return trimmed.to_string();
    }
    let needle = quote
        .map(normalize_quote)
        .filter(|q| q.chars().count() >= 8);
    let start = needle
        .as_deref()
        .and_then(|q| quote_orig_start(trimmed, q))
        .unwrap_or(0);
    let q_len = needle.as_ref().map(|q| q.chars().count()).unwrap_or(0);
    let pad = CITE_WINDOW_MAX.saturating_sub(q_len.max(CITE_WINDOW_MIN)) / 2;
    let from = start.saturating_sub(pad);
    let to = (from + CITE_WINDOW_MAX).min(chars.len());
    let from = to.saturating_sub(CITE_WINDOW_MAX);
    chars[from..to].iter().collect()
}

/// Character index of `needle` in `body`, mapped back through whitespace
/// collapse so the window is cut from the source, not the normalized copy.
fn quote_orig_start(body: &str, needle: &str) -> Option<usize> {
    if let Some(byte) = body.find(needle) {
        return Some(body[..byte].chars().count());
    }
    let normalized = normalize_quote(body);
    let byte = normalized.find(needle)?;
    let norm_before = normalized[..byte].chars().count();
    Some(orig_index_after_normalized(body, norm_before))
}

/// How many source characters produce `norm_chars` of [`normalize_quote`] output.
fn orig_index_after_normalized(body: &str, norm_chars: usize) -> usize {
    let chars: Vec<char> = body.chars().collect();
    let mut orig = 0usize;
    let mut norm = 0usize;
    let mut prev_ws = false;
    while orig < chars.len() && chars[orig].is_whitespace() {
        orig += 1;
    }
    while orig < chars.len() && norm < norm_chars {
        if chars[orig].is_whitespace() {
            if !prev_ws {
                norm += 1;
                prev_ws = true;
            }
        } else {
            prev_ws = false;
            norm += 1;
        }
        orig += 1;
    }
    if prev_ws {
        while orig < chars.len() && chars[orig].is_whitespace() {
            orig += 1;
        }
    }
    orig
}

pub fn compose_cite(
    claims: &[CiteClaim],
    packed: &HashSet<Uuid>,
    bodies: &HashMap<Uuid, String>,
    jev_choices: &HashMap<Uuid, String>,
) -> CiteCompose {
    let mut results = Vec::new();
    for claim in claims {
        if !packed.contains(&claim.atom_id) {
            results.push(CiteResult {
                atom_id: claim.atom_id,
                n: claim.n,
                verdict: CiteVerdict::Fabricated,
                sent_to_jev: false,
            });
            continue;
        }
        if !quote_found(claim, claims, bodies) {
            results.push(CiteResult {
                atom_id: claim.atom_id,
                n: claim.n,
                verdict: CiteVerdict::Fabricated,
                sent_to_jev: false,
            });
            continue;
        }
        let verdict = jev_choices
            .get(&claim.atom_id)
            .map(|c| CiteVerdict::from_choice(c))
            .unwrap_or(CiteVerdict::Unsupported);
        results.push(CiteResult {
            atom_id: claim.atom_id,
            n: claim.n,
            verdict,
            sent_to_jev: jev_choices.contains_key(&claim.atom_id),
        });
    }
    let all_failed = !results.is_empty() && results.iter().all(|r| r.verdict.is_failure());
    CiteCompose { results, all_failed }
}

pub fn cite_questions(claims: &[CiteClaim]) -> BTreeMap<String, Question> {
    let mut q = BTreeMap::new();
    for (i, c) in claims.iter().enumerate() {
        q.insert(
            format!("cite_{i}_{}", c.atom_id),
            Question::Choice {
                instructions: json!({
                    "task": CITE_CHOICE_INSTRUCTIONS,
                    "claim": c.claim,
                    "atom_id": c.atom_id.to_string(),
                }),
                criteria: cite_choice_criteria(),
            },
        );
    }
    q
}

pub fn cite_state(answer: &str, claims: &[CiteClaim], bodies: &HashMap<Uuid, String>) -> serde_json::Value {
    json!({
        "answer": answer,
        "citations": claims.iter().map(|c| {
            json!({
                "n": c.n,
                "atom_id": c.atom_id,
                "claim": c.claim,
                "coral": bodies.get(&c.atom_id).cloned().unwrap_or_default(),
            })
        }).collect::<Vec<_>>(),
    })
}

/// Numbered stream cites, or MCP UUID cites. Ids outside `packed` are
/// fabricated locally and never sent to Jev.
pub async fn verify_citations(
    provider: &dyn DecisionProvider,
    answer: &str,
    rendered: &[Uuid],
    cited_ids: &[Uuid],
    packed: &HashSet<Uuid>,
    bodies: &HashMap<Uuid, String>,
    numbered: bool,
) -> Result<CiteCompose, DecisionError> {
    let claims = if numbered {
        extract_numbered_cites(answer, rendered)
    } else {
        claims_from_atom_ids(cited_ids, packed, answer)
    };
    let mut for_jev = Vec::new();
    for c in &claims {
        if !packed.contains(&c.atom_id) {
            continue;
        }
        if !quote_found(c, &claims, bodies) {
            continue;
        }
        for_jev.push(c.clone());
    }
    let choices = if for_jev.is_empty() {
        HashMap::new()
    } else {
        evaluate_cite_with_retry(provider, answer, &for_jev, bodies).await?
    };
    Ok(compose_cite(&claims, packed, bodies, &choices))
}

fn cite_bodies_windowed(claims: &[CiteClaim], bodies: &HashMap<Uuid, String>) -> HashMap<Uuid, String> {
    let mut out = HashMap::new();
    for c in claims {
        let raw = bodies.get(&c.atom_id).map(String::as_str).unwrap_or("");
        out.insert(c.atom_id, claim_window(raw, c.quote.as_deref()));
    }
    out
}

/// The whole chain (first call + 400 windowed retry) is bounded by
/// [`super::within_ask_budget`]; on expiry the caller marks every cite
/// unverified and keeps the prose.
pub async fn evaluate_cite_with_retry(
    provider: &dyn DecisionProvider,
    answer: &str,
    claims: &[CiteClaim],
    bodies: &HashMap<Uuid, String>,
) -> Result<HashMap<Uuid, String>, DecisionError> {
    super::within_ask_phase("cite", cite_chain(provider, answer, claims, bodies)).await
}

async fn cite_chain(
    provider: &dyn DecisionProvider,
    answer: &str,
    claims: &[CiteClaim],
    bodies: &HashMap<Uuid, String>,
) -> Result<HashMap<Uuid, String>, DecisionError> {
    match evaluate_cite(provider, answer, claims, bodies).await {
        Ok(c) => Ok(c),
        Err(e) if e.is_context_overflow() => {
            let windowed = cite_bodies_windowed(claims, bodies);
            evaluate_cite(provider, answer, claims, &windowed).await
        }
        other => other,
    }
}

pub async fn evaluate_cite(
    provider: &dyn DecisionProvider,
    answer: &str,
    claims: &[CiteClaim],
    bodies: &HashMap<Uuid, String>,
) -> Result<HashMap<Uuid, String>, DecisionError> {
    if claims.is_empty() {
        return Ok(HashMap::new());
    }
    let req = DecisionRequest {
        state: cite_state(answer, claims, bodies),
        model: jev_model_for(JevOp::Ask),
        op: JevOp::Ask,
        questions: cite_questions(claims),
    };
    let resp: DecisionResponse = provider.evaluate(req).await?;
    let mut out = HashMap::new();
    for (i, c) in claims.iter().enumerate() {
        let key = format!("cite_{i}_{}", c.atom_id);
        if let Some(choice) = resp.answers.get(&key).and_then(Answer::as_choice) {
            out.insert(c.atom_id, choice.to_string());
        }
    }
    if out.len() != claims.len() {
        return Err(DecisionError::Parse(format!(
            "citation answers missing: got {} of {}",
            out.len(),
            claims.len()
        )));
    }
    Ok(out)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn extract_in_range_only() {
        let rendered = vec![id(1), id(2)];
        let answer = "Alpha [1] is true. Bravo [2] too. Ignore [9] and [0]. Repeat [1].";
        let cites = extract_numbered_cites(answer, &rendered);
        assert_eq!(cites.len(), 2);
        assert_eq!(cites[0].n, Some(1));
        assert_eq!(cites[0].atom_id, id(1));
        assert!(cites[0].claim.contains("Alpha"));
        assert_eq!(cites[1].n, Some(2));
    }

    #[test]
    fn fabricated_quote_skips_jev() {
        let atom = id(1);
        let packed = HashSet::from([atom]);
        let mut bodies = HashMap::new();
        bodies.insert(atom, "The sky is blue on Tuesday.".into());
        let claims = vec![CiteClaim {
            n: Some(1),
            atom_id: atom,
            claim: "They said \"the moon is made of cheese\" [1].".into(),
            quote: Some("the moon is made of cheese".into()),
        }];
        let out = compose_cite(&claims, &packed, &bodies, &HashMap::new());
        assert_eq!(out.results[0].verdict, CiteVerdict::Fabricated);
        assert!(!out.results[0].sent_to_jev);
        assert!(out.all_failed);
    }

    #[test]
    fn matching_quote_uses_jev_choice() {
        let atom = id(1);
        let packed = HashSet::from([atom]);
        let mut bodies = HashMap::new();
        bodies.insert(atom, "The sky is blue on Tuesday.".into());
        let claims = vec![CiteClaim {
            n: Some(1),
            atom_id: atom,
            claim: "It said \"The sky is blue on Tuesday.\" [1]".into(),
            quote: Some("The sky is blue on Tuesday.".into()),
        }];
        let choices = HashMap::from([(atom, "supports".into())]);
        let out = compose_cite(&claims, &packed, &bodies, &choices);
        assert_eq!(out.results[0].verdict, CiteVerdict::Verified);
        assert!(out.results[0].sent_to_jev);
        assert!(!out.all_failed);
    }

    #[test]
    fn mcp_id_outside_packed_is_fabricated() {
        let packed = HashSet::from([id(1)]);
        let claims = claims_from_atom_ids(&[id(99)], &packed, "answer");
        let out = compose_cite(&claims, &packed, &HashMap::new(), &HashMap::new());
        assert_eq!(out.results[0].verdict, CiteVerdict::Fabricated);
        assert!(!out.results[0].sent_to_jev);
    }

    #[test]
    fn curly_quotes_normalize() {
        assert!(quote_in_source(
            "“hello world there”",
            "prefix hello world there suffix"
        ));
    }

    #[test]
    fn extract_curly_quoted_claim() {
        let rendered = vec![id(1)];
        let answer = "They said “the sky is blue on Tuesday” [1].";
        let cites = extract_numbered_cites(answer, &rendered);
        assert_eq!(cites.len(), 1);
        assert_eq!(
            cites[0].quote.as_deref(),
            Some("the sky is blue on Tuesday")
        );
        assert!(quote_in_source(
            cites[0].quote.as_deref().unwrap(),
            "prefix the sky is blue on Tuesday suffix"
        ));
    }

    #[test]
    fn apply_cite_verdicts_keeps_prose() {
        let answer = "A [1] and B [2] stay.";
        let results = vec![
            CiteResult {
                atom_id: id(1),
                n: Some(1),
                verdict: CiteVerdict::Fabricated,
                sent_to_jev: false,
            },
            CiteResult {
                atom_id: id(2),
                n: Some(2),
                verdict: CiteVerdict::Verified,
                sent_to_jev: true,
            },
        ];
        assert_eq!(apply_cite_verdicts(answer, &results), answer);
    }

    #[test]
    fn unverified_does_not_count_as_all_failed() {
        let atom = id(1);
        let claims = vec![CiteClaim {
            n: Some(1),
            atom_id: atom,
            claim: "x".into(),
            quote: None,
        }];
        let out = unverified_compose(&claims, &HashSet::from([atom]));
        assert_eq!(out.results[0].verdict, CiteVerdict::Unverified);
        assert!(!out.all_failed);
        assert!(!out.results[0].verdict.is_failure());
    }

    #[test]
    fn claim_window_keeps_short_body() {
        let body = "The sky is blue on Tuesday and that is enough.";
        assert_eq!(claim_window(body, Some("sky is blue")), body);
    }

    #[test]
    fn claim_window_clips_long_body_around_quote() {
        let prefix = "aa ".repeat(80);
        let quote = "the production URL is fly.dev";
        let suffix = " zz".repeat(80);
        let body = format!("{prefix}{quote}{suffix}");
        let window = claim_window(&body, Some(quote));
        assert!(window.contains(quote));
        assert!(window.chars().count() <= CITE_WINDOW_MAX);
        assert!(window.chars().count() >= CITE_WINDOW_MIN);
    }

    #[test]
    fn claim_window_maps_offset_past_collapsed_whitespace() {
        let quote = "the production URL is fly.dev";
        let body = format!("hello{}{quote}{}", " \n".repeat(250), "x".repeat(80));
        assert!(
            body.chars().count() > CITE_WINDOW_MAX,
            "fixture must force a window cut"
        );
        let window = claim_window(&body, Some(quote));
        assert!(
            window.contains(quote),
            "window must keep the quote after >400 collapsed whitespace chars: {window:?}"
        );
        assert!(window.chars().count() <= CITE_WINDOW_MAX);
    }

    #[test]
    fn claim_keeps_decimals_file_names_and_quoted_periods() {
        let rendered = vec![id(1), id(2), id(3)];
        let answer = "The outage lasted 7.5 minutes [1]. It omitted flags in `fly.manual.toml` and CLAUDE.md [2]. \
            It throws \"Capture history requires a selection.\" [3]";
        let cites = extract_numbered_cites(answer, &rendered);
        assert_eq!(cites[0].claim, "The outage lasted 7.5 minutes [1].");
        assert_eq!(cites[1].claim, "It omitted flags in `fly.manual.toml` and CLAUDE.md [2].");
        assert_eq!(cites[2].claim, "It throws \"Capture history requires a selection.\" [3]");
    }

    #[test]
    fn marker_after_a_finished_sentence_cites_that_sentence() {
        let rendered = vec![id(1), id(2)];
        let answer = "Intro line.\nThe deploy failed. [1][2] Then it was retried.";
        let cites = extract_numbered_cites(answer, &rendered);
        assert_eq!(cites[0].claim, "The deploy failed. [1][2]");
        assert_eq!(cites[1].claim, "The deploy failed. [1][2]");
        assert_eq!(claim_sentence("Done.[1] Next.", 5), "Done.[1]");
        assert_eq!(claim_sentence("[1] Leads the line.", 0), "[1] Leads the line.");
    }

    #[test]
    fn apostrophes_are_not_quotes() {
        let rendered = vec![id(1)];
        let answer = "Alex's fix doesn't regress the queue's drain [1].";
        let cites = extract_numbered_cites(answer, &rendered);
        assert_eq!(cites[0].quote, None, "{:?}", cites[0].quote);
        let packed = HashSet::from([id(1)]);
        let bodies = HashMap::from([(id(1), "The fix keeps the drain.".to_string())]);
        let out = compose_cite(&cites, &packed, &bodies, &HashMap::from([(id(1), "supports".to_string())]));
        assert_eq!(out.results[0].verdict, CiteVerdict::Verified);
    }

    #[test]
    fn a_quote_from_a_sibling_cite_is_not_fabricated() {
        let rendered = vec![id(1), id(2), id(3)];
        let answer = "PR \"fix(promote): copy embed_model\" landed [1][2]. Also \"never said this\" [3].";
        let cites = extract_numbered_cites(answer, &rendered);
        let packed = HashSet::from([id(1), id(2), id(3)]);
        let bodies = HashMap::from([
            (id(1), "The promote path now copies the model.".to_string()),
            (id(2), "PR #1: fix(promote): copy embed_model so oceans stay searchable".to_string()),
            (id(3), "Something else.".to_string()),
        ]);
        let choices = HashMap::from([(id(1), "supports".to_string()), (id(2), "supports".to_string())]);
        let out = compose_cite(&cites, &packed, &bodies, &choices);
        let verdicts: Vec<_> = out.results.iter().map(|r| r.verdict).collect();
        assert_eq!(verdicts, vec![CiteVerdict::Verified, CiteVerdict::Verified, CiteVerdict::Fabricated]);
    }

    #[test]
    fn a_sentence_inside_a_quotation_keeps_that_quotation() {
        let rendered = vec![id(1), id(2)];
        let answer = "The prompt says \"Pick one. Reply with exactly one label.\" [1] It is metered as \"kind_reeval calls\" [2].";
        let cites = extract_numbered_cites(answer, &rendered);
        assert_eq!(cites[0].quote.as_deref(), Some("Pick one. Reply with exactly one label."));
        assert_eq!(cites[1].quote.as_deref(), Some("kind_reeval calls"));
    }

    #[test]
    fn punctuation_inside_the_closing_mark_is_not_quoted() {
        assert!(quote_in_source("Build the namespace resolver,", "COR-6: Build the namespace resolver"));
        assert!(quote_in_source("“Fix the queue drain.”", "PR #1: Fix the queue drain"));
        assert!(!quote_in_source("Fix the queue drain now,", "PR #1: Fix the queue drain"));
    }

    #[test]
    fn empty_cites_are_not_all_failed() {
        let out = compose_cite(&[], &HashSet::new(), &HashMap::new(), &HashMap::new());
        assert!(!out.all_failed);
    }
}
