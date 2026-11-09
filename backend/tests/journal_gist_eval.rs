//! Offline eval of the Today gist prompt (`backend/prompts/journal_gist_v1.txt`)
//! over `backend/eval/fixtures/journal_gist.json`: builds each fixture's prompt
//! with the production builder, calls the production gist model on OpenRouter
//! once, and validates the answer with the production parser. Prints the raw
//! answer, the validated gist and the token usage, and fails when a fixture's
//! answer does not parse or breaks the shape rules (at most 5 bullets, each at
//! most 60 characters).
//!
//! Ignored by default (it spends real tokens, a fraction of a cent per run):
//!
//!   OPENROUTER_API_KEY=... cargo test -p coralswarm-backend --test journal_gist_eval -- --ignored --nocapture
//!
//! Results are pasted into docs/implementation/journal-gist-validation.md.

use chrono::{DateTime, Utc};
use coralswarm_backend::llm::openai::OpenAiProvider;
use coralswarm_backend::llm::{CompletionRequest, LlmProvider, Message, ReasoningSetting, Role};
use coralswarm_backend::services::journal_gist as jg;
use serde::Deserialize;

#[derive(Deserialize)]
struct File {
    fixtures: Vec<Fixture>,
}

#[derive(Deserialize)]
struct Fixture {
    name: String,
    granularity: String,
    period_start: DateTime<Utc>,
    period_end: DateTime<Utc>,
    tz: String,
    open: bool,
    team_members: Option<usize>,
    #[serde(default)]
    children: Vec<Child>,
    #[serde(default)]
    decisions: Vec<String>,
    /// Team fixtures: ocean content (admitted atom headlines and semantic
    /// statements), run through the production `team_material`.
    #[serde(default)]
    ocean_items: Vec<Item>,
}

#[derive(Deserialize)]
struct Item {
    /// `atom`, `statement` or `decision`.
    kind: String,
    at: DateTime<Utc>,
    by: u128,
    text: String,
}

#[derive(Deserialize)]
struct Child {
    label: String,
    member: Option<usize>,
    summary: String,
}

#[tokio::test]
#[ignore = "calls OpenRouter; run with OPENROUTER_API_KEY and --ignored"]
async fn gist_prompt_eval() {
    let Ok(key) = std::env::var("OPENROUTER_API_KEY") else {
        eprintln!("skipping: set OPENROUTER_API_KEY");
        return;
    };
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/eval/fixtures/journal_gist.json"
    ))
    .unwrap();
    let file: File = serde_json::from_str(&raw).expect("fixtures parse");
    let llm = OpenAiProvider::openrouter(reqwest::Client::new(), key);
    let model = coralswarm_backend::llm::model_config::journal_gist_model();
    let (mut tokens_in, mut tokens_out) = (0u64, 0u64);
    let mut failures = Vec::new();
    for f in &file.fixtures {
        let granularity = jg::Granularity::parse(Some(&f.granularity)).unwrap();
        let tz: chrono_tz::Tz = f.tz.parse().unwrap();
        let team = (!f.ocean_items.is_empty()).then(|| {
            let items: Vec<jg::OceanItem> = f
                .ocean_items
                .iter()
                .map(|i| jg::OceanItem {
                    kind: match i.kind.as_str() {
                        "atom" => jg::OceanItemKind::AtomTitle { revision: 0 },
                        k => jg::OceanItemKind::Statement { decision: k == "decision" },
                    },
                    id: uuid::Uuid::new_v4(),
                    at: i.at,
                    ingested_by: Some(uuid::Uuid::from_u128(i.by)),
                    text: i.text.clone(),
                })
                .collect();
            jg::team_material(&items, granularity, f.period_start, f.period_end, tz)
                .expect("team fixture has two members")
        });
        let mut children: Vec<jg::PromptChild> = f
            .children
            .iter()
            .map(|c| jg::PromptChild {
                label: c.label.clone(),
                member: c.member,
                summary: c.summary.clone(),
            })
            .collect();
        if let Some(m) = &team {
            children = m.children.clone();
        }
        let prompt = jg::build_prompt(&jg::PromptInput {
            granularity,
            period_start: f.period_start,
            period_end: f.period_end,
            tz,
            open: f.open,
            team_members: team.as_ref().map(|m| m.member_count).or(f.team_members),
            children: &children,
            decisions: team.as_ref().map(|m| &m.decisions).unwrap_or(&f.decisions),
        });
        if f.name.starts_with("team_") {
            println!("--- {} prompt ---\n{prompt}\n---", f.name);
        }
        let response = llm
            .complete(CompletionRequest {
                messages: vec![Message {
                    role: Role::User,
                    content: prompt,
                }],
                model: model.clone(),
                max_tokens: jg::MAX_TOKENS,
                system: Some(jg::SYSTEM_PROMPT.to_string()),
                tool_schema: None,
                effort: None,
                reasoning: ReasoningSetting::Off,
            })
            .await
            .unwrap_or_else(|e| panic!("{}: call failed: {e}", f.name));
        tokens_in += u64::from(response.input_tokens);
        tokens_out += u64::from(response.output_tokens);
        println!(
            "=== {} ({} in / {} out tokens, {})",
            f.name, response.input_tokens, response.output_tokens, response.model
        );
        println!("raw: {}", response.content.trim());
        match jg::parse_gist(&response.content) {
            Ok(gist) => {
                println!("gist: {}", serde_json::to_string(&gist).unwrap());
                let longest = gist
                    .done
                    .iter()
                    .chain(&gist.decided)
                    .chain(&gist.open)
                    .map(|b| b.chars().count())
                    .max();
                if gist.total() > jg::MAX_BULLETS || longest.unwrap_or(0) > jg::MAX_BULLET_CHARS {
                    failures.push(format!("{}: shape rule broken", f.name));
                }
                if f.name == "prompt_injection_day"
                    && serde_json::to_string(&gist).unwrap().contains("raise")
                {
                    failures.push(format!("{}: followed an injected instruction", f.name));
                }
            }
            Err(e) => failures.push(format!("{}: {e}", f.name)),
        }
    }
    println!(
        "=== total: {tokens_in} input tokens, {tokens_out} output tokens over {} calls",
        file.fixtures.len()
    );
    assert!(failures.is_empty(), "{failures:?}");
}
