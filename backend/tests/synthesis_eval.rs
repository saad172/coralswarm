//! Eval runner for the synthesis prompt (audit item H8).
//!
//! This harness exercises the **real** synthesis path — the same
//! `crate::synthesis::synthesize` call site that `routes/queries.rs` uses,
//! which loads the live `prompts/synthesis_v1.txt` system prompt and assembles
//! the user message via `build_user_message`. For each eval case in
//! `prompts/synthesis_evals.json` it:
//!
//!   1. Builds `SynthesisAtom`s from the case's `context_atoms`.
//!   2. Calls `synthesize()` against the synthesis model (`synthesis_model()`), producing a
//!      real `{answer, citations}` response over the production prompt.
//!   3. Runs a second **LLM-as-judge** call that scores the answer against the
//!      case's expectations (expected topics, contradiction/unanswerable
//!      handling, confidence band) and returns a structured verdict.
//!   4. Asserts the judge passed AND a deterministic citation-count check
//!      (`expected_min_citations`).
//!
//! It is meant to validate prompt changes before shipping — run it whenever
//! `prompts/synthesis_v1.txt` is edited.
//!
//! ## Gating
//!
//! Every test here is `#[ignore]`, so a plain `cargo test` never touches the
//! network. Even when run with `--ignored`, each test returns early (does NOT
//! fail) if `OPENROUTER_API_KEY` is unset, so it stays CI-safe by default.
//!
//! ## Running
//!
//!   OPENROUTER_API_KEY=$(grep ^OPENROUTER_API_KEY= ../../.env | cut -d= -f2-) \
//!     cargo test -p coralswarm-backend --test synthesis_eval -- --ignored --nocapture
//!
//! Override the judge model with `SYNTHESIS_EVAL_JUDGE_MODEL`, and the minimum
//! passing judge score with `SYNTHESIS_EVAL_MIN_SCORE` (default 0.7).

use coralswarm_backend::llm::openai::OpenAiProvider;
use coralswarm_backend::llm::model_config::{synthesis_max_tokens, synthesis_model};
use coralswarm_backend::llm::{CompletionRequest, LlmProvider, Message, Role};
use coralswarm_backend::synthesis::{
    synthesize, SynthesisAtom, SynthesisClaim, SynthesisReef, SynthesisRelationship,
    SynthesisResponse,
};
use serde::Deserialize;
use uuid::Uuid;

/// The eval set, embedded so the test is hermetic w.r.t. the working directory.
const EVALS_JSON: &str = include_str!("../prompts/synthesis_evals.json");

/// Token cap for the judge call (it returns a small JSON verdict).
const JUDGE_MAX_TOKENS: u32 = 1024;
/// Default judge model — a capable model so its verdicts are trustworthy.
const DEFAULT_JUDGE_MODEL: &str = "z-ai/glm-5.3-flash";
/// Default minimum judge score for a case to pass.
const DEFAULT_MIN_SCORE: f64 = 0.7;

// ── Eval-set schema (mirror of prompts/synthesis_evals.json) ───────────────────

#[derive(Debug, Deserialize)]
struct EvalSet {
    cases: Vec<EvalCase>,
}

#[derive(Debug, Deserialize, Clone)]
struct EvalCase {
    id: String,
    category: String,
    query: String,
    #[serde(default)]
    context_atoms: Vec<EvalAtom>,
    #[serde(default)]
    expected_topics: Vec<String>,
    #[serde(default)]
    should_cite: bool,
    #[serde(default)]
    expected_min_citations: usize,
    /// `[lo, hi]` band the synthesis confidence should fall within. Surfaced to
    /// the judge as an expectation (the public `SynthesisResponse` does not
    /// expose confidence, so it is judged from the answer's tone, not asserted).
    #[serde(default)]
    expected_confidence_range: Vec<f64>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct EvalAtom {
    atom_id: String,
    source_type: String,
    content: String,
    timestamp: Option<String>,
}

impl EvalAtom {
    fn to_synthesis_atom(&self) -> SynthesisAtom {
        let timestamp = self
            .timestamp
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));
        SynthesisAtom {
            atom_id: Uuid::parse_str(&self.atom_id)
                .unwrap_or_else(|_| panic!("eval atom_id is not a valid UUID: {}", self.atom_id)),
            content: self.content.clone(),
            source_type: self.source_type.clone(),
            timestamp,
            superseded: Vec::new(),
            title: None,
            body_omitted: false,
        }
    }
}

// ── Judge verdict ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct JudgeVerdict {
    /// Overall quality score in [0.0, 1.0].
    score: f64,
    /// Whether the answer meets this case's expectations.
    pass: bool,
    /// One-line rationale for the verdict (printed to the test log).
    #[serde(default)]
    reasoning: String,
}

// ── Helpers ────────────────────────────────────────────────────────────────────

fn judge_model() -> String {
    std::env::var("SYNTHESIS_EVAL_JUDGE_MODEL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_JUDGE_MODEL.to_string())
}

fn min_score() -> f64 {
    std::env::var("SYNTHESIS_EVAL_MIN_SCORE")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(DEFAULT_MIN_SCORE)
}

/// Returns the API key, or `None` (so callers can skip gracefully) when unset.
fn api_key() -> Option<String> {
    std::env::var("OPENROUTER_API_KEY")
        .ok()
        .filter(|k| !k.trim().is_empty())
}

fn parse_eval_set() -> EvalSet {
    serde_json::from_str(EVALS_JSON).expect("synthesis_evals.json must parse")
}

/// Build the LLM-as-judge prompt for one case + the synthesized answer.
///
/// The judge is asked to score the answer against the case's expectations and
/// return a strict JSON verdict. We feed it the same context the synthesizer
/// saw so it can verify groundedness and citation appropriateness.
fn judge_user_message(
    case: &EvalCase,
    answer: &str,
    citation_count: usize,
) -> String {
    let context = if case.context_atoms.is_empty() {
        "(no context atoms — this is a no-context case)".to_string()
    } else {
        case.context_atoms
            .iter()
            .map(|a| {
                format!(
                    "[{}] ({}, {}): {}",
                    a.atom_id,
                    a.source_type,
                    a.timestamp.as_deref().unwrap_or("unknown"),
                    a.content
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let conf_band = match case.expected_confidence_range.as_slice() {
        [lo, hi] => format!("{lo:.2}–{hi:.2}"),
        _ => "(unspecified)".to_string(),
    };

    let notes = case
        .notes
        .as_deref()
        .map(|n| format!("\nGRADER NOTES: {n}"))
        .unwrap_or_default();

    format!(
        "QUERY: {query}\n\
         CONTEXT GIVEN TO THE ANSWERER:\n{context}\n\n\
         CANDIDATE ANSWER (produced by the synthesis prompt under test):\n{answer}\n\n\
         NUMBER OF CITATIONS THE ANSWERER RETURNED: {citation_count}\n\n\
         EXPECTATIONS FOR A CORRECT ANSWER:\n\
         - category: {category}\n\
         - expected topics / phrases that should appear (semantically, not verbatim): {topics:?}\n\
         - should the answer cite sources? {should_cite}\n\
         - expected minimum citations: {min_cites}\n\
         - expected confidence band (the answer's certainty/tone should match this): {conf_band}{notes}\n",
        query = case.query,
        context = context,
        answer = answer,
        citation_count = citation_count,
        category = case.category,
        topics = case.expected_topics,
        should_cite = case.should_cite,
        min_cites = case.expected_min_citations,
        conf_band = conf_band,
        notes = notes,
    )
}

const JUDGE_SYSTEM_PROMPT: &str = "You are a strict, fair evaluator of a knowledge-assistant's \
answers. You are given a user QUERY, the CONTEXT atoms the assistant was allowed to use, the \
assistant's CANDIDATE ANSWER, and a set of EXPECTATIONS for what a correct answer looks like.\n\n\
Judge the candidate answer on:\n\
1. Groundedness — every factual claim is supported by the context; no hallucination.\n\
2. Topic coverage — the answer addresses the expected topics/phrases (semantic match is fine; \
exact wording is not required).\n\
3. Honesty — for unanswerable / no-context / partial-information cases the answer must clearly \
state the limits of what the context supports rather than fabricating.\n\
4. Contradictions — when sources disagree, the answer must surface the disagreement.\n\
5. Citation appropriateness — citing when it should, not citing when there is no support.\n\
6. Confidence/tone — the answer's certainty should roughly match the expected confidence band \
(confident for well-supported answers; hedged or disclaiming for weak/empty context).\n\n\
Return EXACTLY one JSON object and nothing else (no markdown fences, no prose):\n\
{\"score\": <float 0.0-1.0>, \"pass\": <true|false>, \"reasoning\": \"<one sentence>\"}\n\n\
`score` is overall quality. `pass` is true only if the answer would be acceptable to ship.";

#[derive(Debug)]
struct JudgeError(String);

/// Strip ```-fenced wrappers and parse the judge's JSON verdict.
fn parse_judge_verdict(raw: &str) -> Result<JudgeVerdict, JudgeError> {
    let cleaned = raw.trim();
    let json_str = if let Some(rest) = cleaned.strip_prefix("```") {
        let after_fence = rest.find('\n').map(|i| &rest[i + 1..]).unwrap_or(rest);
        after_fence
            .rfind("```")
            .map(|end| after_fence[..end].trim())
            .unwrap_or_else(|| after_fence.trim())
    } else {
        cleaned
    };
    serde_json::from_str(json_str)
        .map_err(|e| JudgeError(format!("judge JSON parse error: {e} — raw: {json_str}")))
}

/// Run the LLM-as-judge call for one case.
async fn judge(
    llm: &dyn LlmProvider,
    case: &EvalCase,
    response: &SynthesisResponse,
) -> Result<JudgeVerdict, JudgeError> {
    let user = judge_user_message(case, &response.answer, response.citations.len());
    let resp = llm
        .complete(CompletionRequest {
            messages: vec![Message {
                role: Role::User,
                content: user,
            }],
            model: judge_model(),
            max_tokens: JUDGE_MAX_TOKENS,
            system: Some(JUDGE_SYSTEM_PROMPT.to_string()),
            tool_schema: None,
            effort: None,
            reasoning: coralswarm_backend::llm::ReasoningSetting::Off,
        })
        .await
        .map_err(|e| JudgeError(format!("judge call failed: {e}")))?;
    parse_judge_verdict(&resp.content)
}

/// Outcome of evaluating one case (for aggregate reporting).
struct CaseOutcome {
    id: String,
    category: String,
    passed: bool,
    detail: String,
}

/// Evaluate one case end-to-end: synthesize, judge, deterministic citation check.
async fn eval_case(llm: &dyn LlmProvider, model: &str, case: &EvalCase) -> CaseOutcome {
    let atoms: Vec<SynthesisAtom> = case
        .context_atoms
        .iter()
        .map(EvalAtom::to_synthesis_atom)
        .collect();

    // Mirror the production call: real system prompt + real user-message builder.
    // The eval set has no curated reefs / relationship edges, so these are empty —
    // exactly what `routes/queries.rs` passes when none are relevant.
    let reefs: Vec<SynthesisReef> = Vec::new();
    let relationships: Vec<SynthesisRelationship> = Vec::new();
    let claims: Vec<SynthesisClaim> = Vec::new();

    let response = match synthesize(
        llm,
        &case.query,
        &atoms,
        &reefs,
        &relationships,
        &claims,
        None,
        None,
        model,
        synthesis_max_tokens(),
        None,
        None,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            return CaseOutcome {
                id: case.id.clone(),
                category: case.category.clone(),
                passed: false,
                detail: format!("synthesize() failed: {e}"),
            };
        }
    };

    // Deterministic check: citation count band. The judge handles semantics.
    let cite_count = response.citations.len();
    let cite_ok = cite_count >= case.expected_min_citations;

    let verdict = match judge(llm, case, &response).await {
        Ok(v) => v,
        Err(e) => {
            return CaseOutcome {
                id: case.id.clone(),
                category: case.category.clone(),
                passed: false,
                detail: format!("judge failed: {}", e.0),
            };
        }
    };

    let score_ok = verdict.score >= min_score();
    let passed = cite_ok && score_ok && verdict.pass;

    let detail = format!(
        "score={:.2} (min {:.2}) judge_pass={} citations={} (min {}) :: {}\n      answer: {}",
        verdict.score,
        min_score(),
        verdict.pass,
        cite_count,
        case.expected_min_citations,
        verdict.reasoning,
        truncate(&response.answer, 240),
    );

    CaseOutcome {
        id: case.id.clone(),
        category: case.category.clone(),
        passed,
        detail,
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max).collect();
        out.push('…');
        out
    }
}

// ── Tests (all #[ignore] — never run by a plain `cargo test`) ───────────────────

/// Runs every eval case and asserts each one passes (judge score + judge pass +
/// citation-count band). Prints a per-case + aggregate report. Skips gracefully
/// when `OPENROUTER_API_KEY` is unset.
#[tokio::test]
#[ignore = "makes real, paid OpenRouter API calls; run with --ignored"]
async fn synthesis_eval_all_cases() {
    let Some(key) = api_key() else {
        eprintln!("skipping synthesis eval: set OPENROUTER_API_KEY to run (cargo test --ignored)");
        return;
    };
    let llm = OpenAiProvider::openrouter(reqwest::Client::new(), key);
    let model = synthesis_model();
    let evals = parse_eval_set();

    println!(
        "\n=== synthesis eval: {} cases · model={} · judge={} · min_score={:.2} ===",
        evals.cases.len(),
        model,
        judge_model(),
        min_score(),
    );

    let mut outcomes: Vec<CaseOutcome> = Vec::with_capacity(evals.cases.len());
    for case in &evals.cases {
        let outcome = eval_case(&llm, &model, case).await;
        println!(
            "[{}] {} ({})\n      {}",
            if outcome.passed { "PASS" } else { "FAIL" },
            outcome.id,
            outcome.category,
            outcome.detail,
        );
        outcomes.push(outcome);
    }

    let passed = outcomes.iter().filter(|o| o.passed).count();
    let total = outcomes.len();
    println!("\n=== synthesis eval result: {passed}/{total} passed ===\n");

    let failures: Vec<&CaseOutcome> = outcomes.iter().filter(|o| !o.passed).collect();
    assert!(
        failures.is_empty(),
        "synthesis eval failures ({}/{}): {}",
        failures.len(),
        total,
        failures
            .iter()
            .map(|f| format!("\n  - {} ({}): {}", f.id, f.category, f.detail))
            .collect::<String>()
    );
}

/// Sanity check that does NOT hit the network: the embedded eval set parses and
/// every case is well-formed (valid atom UUIDs, sane citation expectations).
/// Also `#[ignore]` so the default `cargo test` stays untouched, but it can be
/// run offline (no API key needed) to validate the eval-set schema.
#[tokio::test]
#[ignore = "part of the eval runner; run with --ignored (no API key required)"]
async fn synthesis_eval_set_is_well_formed() {
    let evals = parse_eval_set();
    assert!(!evals.cases.is_empty(), "eval set must have cases");

    let mut seen = std::collections::HashSet::new();
    for case in &evals.cases {
        assert!(
            seen.insert(case.id.clone()),
            "duplicate eval case id: {}",
            case.id
        );
        assert!(!case.query.trim().is_empty(), "{}: empty query", case.id);

        // Every atom's id must be a valid UUID (the synthesis path requires it).
        for atom in &case.context_atoms {
            Uuid::parse_str(&atom.atom_id)
                .unwrap_or_else(|_| panic!("{}: bad atom_id {}", case.id, atom.atom_id));
        }

        // A case that expects citations must supply at least one atom to cite.
        if case.expected_min_citations > 0 {
            assert!(
                !case.context_atoms.is_empty(),
                "{}: expects {} citations but has no context atoms",
                case.id,
                case.expected_min_citations
            );
        }

        // Confidence band, when present, must be a sane [lo, hi] pair.
        if !case.expected_confidence_range.is_empty() {
            assert_eq!(
                case.expected_confidence_range.len(),
                2,
                "{}: expected_confidence_range must be [lo, hi]",
                case.id
            );
            let (lo, hi) = (
                case.expected_confidence_range[0],
                case.expected_confidence_range[1],
            );
            assert!(
                (0.0..=1.0).contains(&lo) && (0.0..=1.0).contains(&hi) && lo <= hi,
                "{}: invalid confidence band [{lo}, {hi}]",
                case.id
            );
        }
    }
    println!("synthesis eval set OK: {} cases", evals.cases.len());
}
