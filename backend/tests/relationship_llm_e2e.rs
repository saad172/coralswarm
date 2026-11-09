//! Real-LLM end-to-end test for the relationship knowledge graph (ADR-0008).
//!
//! Unlike `relationship_inference_e2e.rs` (deterministic store + inference), this
//! exercises the **actual LLM calls** at both ends of the pipeline:
//!
//!   1. Extraction — feeds "Harper is my wife" / "Casey is my daughter" to the cheap-tier model
//!      with the real `extract_relationships_v1` prompt and asserts the typed
//!      triples (`spouse_of`, `parent_of`) come back, first-person resolved to
//!      the author.
//!   2. Inference — runs the real `relationships::inference::infer` over those
//!      stated edges (+ materialized inverses) and asserts `parent_of(Harper,Casey)`.
//!   3. Synthesis — calls the real `synthesize` over the inferred edge and asserts
//!      the answer names Harper AND hedges it (inferred/likely/unconfirmed).
//!
//! Gated on `OPENROUTER_API_KEY` (makes real, paid API calls); skips otherwise.
//! Run with:
//!   OPENROUTER_API_KEY=$(grep ^OPENROUTER_API_KEY= ../../.env | cut -d= -f2-) \
//!     cargo test -p coralswarm-backend --test relationship_llm_e2e -- --nocapture

use std::collections::HashMap;

use coralswarm_backend::llm::openai::OpenAiProvider;
use coralswarm_backend::llm::{CompletionRequest, LlmProvider, Message, Role};
use coralswarm_backend::relationships::inference::{infer, EdgeInput};
use coralswarm_backend::relationships::predicates;
use coralswarm_backend::synthesis::{synthesize, SynthesisAtom, SynthesisRelationship};
use uuid::Uuid;

const MODEL: &str = "deepseek/deepseek-v4-flash-0731";
const EXTRACT_PROMPT: &str = include_str!("../prompts/extract_relationships_v1.txt");

#[derive(serde::Deserialize)]
struct Triple {
    subject: String,
    predicate: String,
    object: String,
}
#[derive(serde::Deserialize)]
struct ExtractResp {
    #[serde(default)]
    relationships: Vec<Triple>,
}

fn slice_json(raw: &str) -> &str {
    match (raw.find('{'), raw.rfind('}')) {
        (Some(a), Some(b)) if b >= a => &raw[a..=b],
        _ => raw,
    }
}

async fn extract(llm: &dyn LlmProvider, author: &str, message: &str) -> Vec<Triple> {
    let resp = llm
        .complete(CompletionRequest {
            messages: vec![Message {
                role: Role::User,
                content: format!("## Author\n{author}\n\n## Message\n{message}"),
            }],
            model: MODEL.to_string(),
            max_tokens: 512,
            system: Some(EXTRACT_PROMPT.to_string()),
            tool_schema: None,
            effort: None,
            reasoning: coralswarm_backend::llm::ReasoningSetting::Off,
        })
        .await
        .expect("extraction LLM call");
    serde_json::from_str::<ExtractResp>(slice_json(&resp.content))
        .map(|r| r.relationships)
        .unwrap_or_default()
}

#[tokio::test]
async fn family_pipeline_with_real_llm() {
    let api_key = match std::env::var("OPENROUTER_API_KEY") {
        Ok(k) if !k.trim().is_empty() => k,
        _ => {
            eprintln!("skipping: set OPENROUTER_API_KEY to run the real-LLM e2e test");
            return;
        }
    };
    let llm = OpenAiProvider::openrouter(reqwest::Client::new(), api_key);

    // ── 1. EXTRACTION (real LLM) ──────────────────────────────────────────────
    let mut triples = Vec::new();
    triples.extend(extract(&llm, "Alex", "Harper is my wife").await);
    triples.extend(extract(&llm, "Alex", "Casey is my daughter").await);

    println!("\n--- extracted triples ---");
    for t in &triples {
        println!("  {} {} {}", t.subject, t.predicate, t.object);
    }

    let has = |subj: &str, pred: &str, obj: &str| {
        triples.iter().any(|t| {
            t.subject.eq_ignore_ascii_case(subj)
                && t.predicate == pred
                && t.object.eq_ignore_ascii_case(obj)
        })
    };
    assert!(
        has("Alex", predicates::SPOUSE_OF, "Harper"),
        "extractor must emit spouse_of(Alex, Harper) from 'Harper is my wife'"
    );
    assert!(
        has("Alex", predicates::PARENT_OF, "Casey"),
        "extractor must emit parent_of(Alex, Casey) from 'Casey is my daughter'"
    );

    // ── 2. INFERENCE (real engine, deterministic) ─────────────────────────────
    // Resolve names → ids and materialize inverse/symmetric edges as the worker does.
    let mut ids: HashMap<String, Uuid> = HashMap::new();
    let mut id_of = |name: &str| *ids.entry(name.to_lowercase()).or_insert_with(Uuid::new_v4);

    let mut edges: Vec<EdgeInput> = Vec::new();
    let mut push = |edges: &mut Vec<EdgeInput>, s: Uuid, p: &str, d: Uuid| {
        edges.push(EdgeInput {
            edge_id: Uuid::new_v4(),
            src: s,
            predicate: p.to_string(),
            dst: d,
            confidence: 0.95,
        });
    };
    for t in &triples {
        if !predicates::is_known(&t.predicate) {
            continue;
        }
        let s = id_of(&t.subject);
        let d = id_of(&t.object);
        push(&mut edges, s, &t.predicate, d);
        if predicates::is_symmetric(&t.predicate) {
            push(&mut edges, d, &t.predicate, s);
        } else if let Some(inv) = predicates::inverse(&t.predicate) {
            push(&mut edges, d, inv, s);
        }
    }

    let harper = id_of("Harper");
    let Casey = id_of("Casey");
    let inferred = infer(&edges);
    println!("--- inferred edges ---");
    for e in &inferred {
        println!("  {:?}->{:?} {} (conf {:.2}) via {}", e.src, e.dst, e.predicate, e.confidence, e.rule);
    }
    let mother = inferred
        .iter()
        .find(|e| e.predicate == predicates::PARENT_OF && e.src == harper && e.dst == Casey)
        .expect("engine must infer parent_of(Harper, Casey)");
    assert!(mother.defeasible && mother.confidence < 0.85);

    // ── 3. SYNTHESIS (real LLM) ───────────────────────────────────────────────
    let atoms = vec![
        SynthesisAtom {
            atom_id: Uuid::new_v4(),
            content: "Harper is my wife".to_string(),
            source_type: "slack".to_string(),
            timestamp: None,
            superseded: Vec::new(),
            title: None,
            body_omitted: false,
        },
        SynthesisAtom {
            atom_id: Uuid::new_v4(),
            content: "Casey is my daughter".to_string(),
            source_type: "slack".to_string(),
            timestamp: None,
            superseded: Vec::new(),
            title: None,
            body_omitted: false,
        },
    ];
    let rels = vec![SynthesisRelationship {
        subject: "Harper".to_string(),
        predicate: "parent_of".to_string(),
        object: "Casey".to_string(),
        inferred: true,
        confirmed: false,
        confidence: mother.confidence,
    }];

    let answer = synthesize(&llm, "who is Casey's mom?", &atoms, &[], &rels, &[], None, None, MODEL, 512, None, None)
        .await
        .expect("synthesis LLM call");
    println!("--- synthesized answer ---\n  {}\n", answer.answer);

    let lc = answer.answer.to_lowercase();
    assert!(lc.contains("harper"), "answer must name Harper; got: {}", answer.answer);
    assert!(
        ["likely", "appears", "inferred", "unconfirmed", "may ", "probably", "suggest"]
            .iter()
            .any(|h| lc.contains(h)),
        "inferred relationship must be hedged, not asserted as fact; got: {}",
        answer.answer
    );
}
