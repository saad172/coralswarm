mod support;

use coralswarm_backend::scoped_ask::{
    self,
    progress::{Frame, Progress, Stage},
    AskRequest, FaultPlan,
};
use futures::StreamExt;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use support::scoped_ask_fixture::Fixture;

#[derive(Debug)]
struct SseFrame {
    event: String,
    data: Value,
}

async fn stream(f: &Fixture, body: Value) -> Vec<SseFrame> {
    let response = Box::pin(coralswarm_backend::test_scoped_query_stream(f.state(None), f.tenant.clone(), f.user.clone(), body)).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 4 << 20).await.unwrap();
    String::from_utf8(bytes.to_vec())
        .unwrap()
        .split("\n\n")
        .filter(|block| !block.trim().is_empty())
        .map(|block| SseFrame {
            event: block.lines().find_map(|l| l.strip_prefix("event: ")).expect("event line").to_owned(),
            data: serde_json::from_str(block.lines().find_map(|l| l.strip_prefix("data: ")).expect("data line")).unwrap(),
        })
        .collect()
}

fn concise(f: &Fixture) -> Value {
    json!({"text":"What did Acme decide?","library_scope":f.scope(None),"style":"concise"})
}

fn stages(frames: &[SseFrame]) -> Vec<Progress> {
    frames
        .iter()
        .filter(|frame| frame.event == "progress")
        .map(|frame| {
            let stage = match frame.data["stage"].as_str().unwrap() {
                "searching" => Stage::Searching,
                "retrieved" => Stage::Retrieved { sources: frame.data["sources"].as_u64().unwrap() as usize },
                "writing" => Stage::Writing,
                "checking" => Stage::Checking,
                other => panic!("unknown stage {other}"),
            };
            Progress { stage, attempt: frame.data["attempt"].as_u64().unwrap() as u32 }
        })
        .collect()
}

fn keys(value: &Value) -> BTreeSet<String> {
    value.as_object().unwrap().keys().cloned().collect()
}

async fn stages_precede_one_final_and_never_leak_the_answer(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    coralswarm_backend::test_modern_cached_task_admission(&f.state(None), &f.tenant, &f.user).await;
    let frames = stream(&f, concise(&f)).await;
    let names: Vec<&str> = frames.iter().map(|frame| frame.event.as_str()).collect();
    assert_eq!(names, ["progress", "progress", "progress", "progress", "meta", "final"], "{frames:?}");

    let final_payload = &frames[5].data;
    let answer = final_payload["answer"].as_str().expect("synthesized answer");
    assert!(answer.contains("green release"), "the fake model answered: {answer}");
    let sources = final_payload["outcomes"].as_array().map_or(0, Vec::len)
        + final_payload["legacy_evidence"].as_array().map_or(0, Vec::len);
    assert!(sources > 0);
    let at = |stage| Progress { stage, attempt: 1 };
    assert_eq!(
        stages(&frames),
        [at(Stage::Searching), at(Stage::Retrieved { sources }), at(Stage::Writing), at(Stage::Checking)],
        "retrieved counts the candidates the answer was built from"
    );

    let chars: Vec<char> = answer.chars().collect();
    for frame in &frames[..5] {
        let mut data = frame.data.clone();
        assert_eq!(data.as_object_mut().unwrap().remove("request_id").is_some(), frame.event == "meta");
        let data = data.to_string();
        for window in chars.windows(8) {
            let piece: String = window.iter().collect();
            assert!(!data.contains(&piece), "{} frame leaked answer text {piece:?}: {data}", frame.event);
        }
    }

    let sources_only = stream(&f, json!({"text":"What did Acme decide?","library_scope":f.scope(None),"style":"sources-only"})).await;
    let names: Vec<&str> = sources_only.iter().map(|frame| frame.event.as_str()).collect();
    assert_eq!(names, ["progress", "progress", "meta", "final"], "sources-only calls no model, so it never writes or checks");
    assert_eq!(stages(&sources_only)[0], at(Stage::Searching));
    f.done().await;
}

#[tokio::test]
async fn consumer_stages_precede_one_final_and_never_leak_the_answer() {
    stages_precede_one_final_and_never_leak_the_answer(false).await;
}
#[tokio::test]
async fn tenant_stages_precede_one_final_and_never_leak_the_answer() {
    stages_precede_one_final_and_never_leak_the_answer(true).await;
}

async fn citation_check_failure_is_one_error_event(org: bool) {
    let Some(f) = Fixture::new(org).await else { return };
    let state = f.state(None);
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;
    let mut request: AskRequest = serde_json::from_value(concise(&f)).unwrap();
    request = scoped_ask::test_request_fault(request, FaultPlan::new("cite", 1, "evidence_changed", false));
    let prepared = scoped_ask::prepare(&state, &f.tenant, &f.user, request).await.ok().expect("prepared");
    let frames: Vec<Frame> = scoped_ask::progress::frames(state, f.tenant.clone(), f.user.clone(), prepared, "b8-cite".into())
        .collect()
        .await;

    let (last, before) = frames.split_last().unwrap();
    assert!(before.iter().all(|frame| matches!(frame, Frame::Progress(_))), "{frames:?}");
    assert_eq!(before.last(), Some(&Frame::Progress(Progress { stage: Stage::Checking, attempt: 1 })));
    let Frame::Error(error) = last else { panic!("expected one error event, got {last:?}") };
    assert_eq!(error["status"], 409);
    assert_eq!(error["code"], "evidence_changed");
    assert_eq!(error["retryable"], true);
    assert!(error["failed_message_id"].is_string(), "the failure is recorded as the question's reply: {error}");
    assert!(error["conversation_id"].is_string() && error["question_message_id"].is_string(), "a retry needs the ids: {error}");
    assert!(!error.to_string().contains("green release"), "the withheld answer never leaves: {error}");
    f.done().await;
}

#[tokio::test]
async fn consumer_citation_check_failure_is_one_error_event() {
    citation_check_failure_is_one_error_event(false).await;
}
#[tokio::test]
async fn tenant_citation_check_failure_is_one_error_event() {
    citation_check_failure_is_one_error_event(true).await;
}

#[tokio::test]
async fn streamed_final_matches_the_json_answer() {
    let Some(f) = Fixture::new(false).await else { return };
    let state = f.state(None);
    coralswarm_backend::test_modern_cached_task_admission(&state, &f.tenant, &f.user).await;
    let response = Box::pin(coralswarm_backend::test_scoped_query_json(state, f.tenant.clone(), f.user.clone(), concise(&f))).await;
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let json_answer: Value = serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 4 << 20).await.unwrap()).unwrap();
    let frames = stream(&f, concise(&f)).await;
    let streamed = &frames.iter().find(|frame| frame.event == "final").expect("final").data;

    let mut json_keys = keys(&json_answer);
    assert!(json_keys.remove("request_id"), "the JSON answer carries its request id");
    assert_eq!(keys(streamed), json_keys);
    assert_eq!(streamed["answer"], json_answer["answer"]);
    assert_eq!(streamed["retained_citations"], json_answer["retained_citations"]);
    assert!(json_answer.get("source_results").is_none() && streamed.get("source_results").is_none(), "not a find question");
    f.done().await;
}
