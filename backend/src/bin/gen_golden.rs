//! One-time golden-set generator (Phase 1 eval harness).
//!
//! Reads the committed fixture corpus and asks GLM 5.3 Flash
//! (`z-ai/glm-5.3-flash`) to draft candidate `query → relevant_doc_ids` pairs of
//! three kinds — direct content, temporal (date-bounded), and abstention
//! negatives (queries the corpus cannot answer). Output is written for HUMAN
//! REVIEW to `backend/eval/golden/golden.candidates.jsonl`; a human spot-checks
//! and prunes it, then promotes the good rows into `golden.jsonl`. The eval
//! itself never calls this — the golden set is committed, so running the eval
//! needs no API key.
//!
//! Builds its provider through [`coralswarm_backend::build_cloud_llm_provider`]
//! — the same OpenRouter factory `run()` uses to wire `AppState::llm`.
//!
//! Usage:
//!   OPENROUTER_API_KEY=sk-or-… cargo run -p coralswarm-backend --bin gen_golden \
//!       [-- --per-kind 40]
//!   OPENROUTER_API_KEY=sk-or-… GOLDEN_GEN_MODEL=z-ai/glm-5.3-flash \
//!       cargo run -p coralswarm-backend --bin gen_golden [-- --per-kind 40]
//!
//! Drafting stays on the capable tier (`z-ai/glm-5.3-flash`). Override with
//! `GOLDEN_GEN_MODEL` only when you want a different OpenRouter id.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context};
use coralswarm_backend::eval::{self, FixtureDoc};
use coralswarm_backend::llm::{CompletionRequest, Message, ReasoningSetting, Role};
use serde_json::json;

/// Capable-tier drafter. Override via `GOLDEN_GEN_MODEL`.
const GOLDEN_GEN_MODEL: &str = "z-ai/glm-5.3-flash";

fn eval_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("eval")
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("gen_golden: {e:#}");
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let openrouter_api_key = std::env::var("OPENROUTER_API_KEY").ok();
    if openrouter_api_key.is_none() {
        bail!("set OPENROUTER_API_KEY to generate candidate golden pairs");
    }
    let model = std::env::var("GOLDEN_GEN_MODEL").unwrap_or_else(|_| GOLDEN_GEN_MODEL.to_string());

    // `--per-kind N` (default 40): how many queries to request per category.
    let per_kind: usize = {
        let args: Vec<String> = std::env::args().collect();
        args.iter()
            .position(|a| a == "--per-kind")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(40)
    };

    let dir = eval_dir();
    let corpus: Vec<FixtureDoc> =
        eval::load_jsonl(&dir.join("fixtures/corpus.jsonl")).context("load corpus")?;
    let valid_ids: std::collections::HashSet<String> =
        corpus.iter().map(|d| d.doc_id.clone()).collect();

    let corpus_block = corpus
        .iter()
        .map(|d| {
            format!(
                "[{}] type={} title={} date={}\n{}",
                d.doc_id,
                d.source_type,
                d.title.as_deref().unwrap_or("(none)"),
                d.date,
                d.content
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");

    let system = "You write evaluation queries for a meeting-transcript + memory \
        retrieval system. Given a fixed corpus of documents (each with a stable \
        doc_id), produce natural end-user questions with the doc_id(s) that \
        answer them. Answers must be UNAMBIGUOUS and grounded strictly in the \
        corpus. Output ONLY a JSON array, no prose.";

    let instructions = format!(
        "Corpus documents:\n\n{corpus_block}\n\n\
        Produce three kinds of queries:\n\
        1. \"direct\": {per_kind} content questions answered by one or more docs.\n\
        2. \"temporal\": {per_kind} questions that reference a date or period \
           (the meeting date is embedded in each transcript), answered by the \
           matching doc(s).\n\
        3. \"abstention\": {per_kind} plausible questions the corpus genuinely \
           CANNOT answer (relevant_doc_ids MUST be empty).\n\n\
        Return a JSON array of objects: \
        {{\"kind\":\"direct|temporal|abstention\",\"query\":\"...\",\
        \"relevant_doc_ids\":[\"m02\"]}}. \
        Use only doc_ids that exist above. For abstention, relevant_doc_ids=[]."
    );

    eprintln!("gen_golden: requesting ~{} pairs from {model}…", per_kind * 3);
    // The async reqwest client has NO default timeout, so a stalled request
    // would hang this one-shot command indefinitely. Bound it.
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("build HTTP client")?;
    let llm = coralswarm_backend::build_cloud_llm_provider(
        http_client,
        openrouter_api_key,
    )
    .context("no cloud LLM provider could be built from the resolved API key")?;

    let resp = llm
        .complete(CompletionRequest {
            messages: vec![Message { role: Role::User, content: instructions }],
            model,
            max_tokens: 8192,
            system: Some(system.to_string()),
            tool_schema: None,
            effort: None,
            reasoning: ReasoningSetting::Off,
        })
        .await
        .context("LLM request failed")?;

    // The model may wrap the array in prose/fences despite instructions; extract
    // the outermost JSON array.
    let arr_str = extract_json_array(&resp.content).context("no JSON array found in model output")?;
    let candidates: Vec<serde_json::Value> =
        serde_json::from_str(arr_str).context("parse candidate array")?;

    // Validate + assign ids; drop rows referencing unknown docs or violating the
    // abstention invariant.
    let mut kept = Vec::new();
    let mut dropped = 0usize;
    let (mut nd, mut nt, mut na) = (0usize, 0usize, 0usize);
    for c in &candidates {
        let kind = c["kind"].as_str().unwrap_or("");
        let query = c["query"].as_str().unwrap_or("").trim().to_string();
        let docs: Vec<String> = c["relevant_doc_ids"]
            .as_array()
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let known = docs.iter().all(|d| valid_ids.contains(d));
        let ok = !query.is_empty()
            && matches!(kind, "direct" | "temporal" | "abstention")
            && known
            && (kind == "abstention") == docs.is_empty();
        if !ok {
            dropped += 1;
            continue;
        }
        let idx = match kind {
            "direct" => { nd += 1; nd }
            "temporal" => { nt += 1; nt }
            _ => { na += 1; na }
        };
        let prefix = &kind[..1];
        kept.push(json!({
            "id": format!("{prefix}{idx:02}"),
            "kind": kind,
            "query": query,
            "relevant_doc_ids": docs,
        }));
    }

    let out = dir.join("golden/golden.candidates.jsonl");
    let mut s = String::new();
    for row in &kept {
        let encoded = serde_json::to_string(row).context("serialize candidate")?;
        s.push_str(&encoded);
        s.push('\n');
    }
    tokio::fs::write(&out, s)
        .await
        .with_context(|| format!("write {}", out.display()))?;

    eprintln!(
        "gen_golden: wrote {} candidates ({} direct, {} temporal, {} abstention); dropped {} invalid.\n\
         Review + prune → promote good rows into golden.jsonl:\n  {}",
        kept.len(), nd, nt, na, dropped, out.display()
    );
    Ok(())
}

/// Extract the outermost `[ … ]` slice from a string that may include prose or
/// ```json fences around the array.
fn extract_json_array(s: &str) -> Option<&str> {
    let start = s.find('[')?;
    let end = s.rfind(']')?;
    if end > start {
        Some(&s[start..=end])
    } else {
        None
    }
}
