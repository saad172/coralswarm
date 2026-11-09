//! Positive identity-curation and scoped cloud Ask under a real coralswarm_app
//! login. The cloud scoped Ask relay deliberately emits meta/final, not the
//! retired automatic lexical merge_suggestion SSE card. An empty/error-only
//! success cannot replace the useful action: this suite also lists a genuine
//! synthetic human-UNSURE review item and confirms it through the real router,
//! proving the human judgement, physical merge and queue removal under RLS.
//! This does not certify automatic managed semantic consumers or model output.
//! Env-gated on ORGT_TEST_BASE_URL; no model/provider or background job runs.

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::{
    JudgementSource, JudgementVerdict, NewJudgement, PostgresStore, Store,
};
use serde_json::{json, Value};
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

fn future_exp() -> usize {
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair. `test_jwks()` returns
/// the JWKS document as a STRING, so it must be served raw with an explicit
/// content-type — wrapping it in `axum::Json` would JSON-encode the string into
/// a quoted blob and every token would fail verification.
async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let jwks_app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks_body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local JWKS listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, jwks_app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

#[derive(Debug)]
struct SseFrame {
    event: String,
    data: Value,
}

/// Drain the full real cloud SSE relay and parse its meta/final payloads.
async fn read_sse(resp: axum::response::Response) -> Vec<SseFrame> {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("drain SSE body");
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let mut frames = Vec::new();
    for block in text.split("\n\n") {
        let mut event: Option<String> = None;
        let mut data: Option<String> = None;
        for line in block.lines() {
            if let Some(rest) = line.strip_prefix("event:") {
                event = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("data:") {
                data = Some(rest.trim().to_string());
            }
        }
        if let (Some(event), Some(data)) = (event, data) {
            frames.push(SseFrame {
                event,
                data: serde_json::from_str(&data).unwrap_or(Value::Null),
            });
        }
    }
    frames
}

fn render(frames: &[SseFrame]) -> String {
    frames
        .iter()
        .map(|f| format!("{}={}", f.event, f.data))
        .collect::<Vec<_>>()
        .join(" | ")
}

async fn post_stream(app: &axum::Router, token: &str, body: Value) -> (StatusCode, Vec<SseFrame>) {
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/queries/stream")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    (status, read_sse(resp).await)
}

// This binary has one test. The switch is process-local fixture configuration,
// restored on unwinding as well as success; no production activation occurs.
struct ReviewFlag(Option<std::ffi::OsString>);
impl ReviewFlag {
    fn enabled() -> Self {
        let old = std::env::var_os("JUDGEMENT_GRAPH_WRITES");
        unsafe {
            std::env::set_var("JUDGEMENT_GRAPH_WRITES", "on");
        }
        Self(old)
    }
}
impl Drop for ReviewFlag {
    fn drop(&mut self) {
        unsafe {
            match &self.0 {
                Some(old) => std::env::set_var("JUDGEMENT_GRAPH_WRITES", old),
                None => std::env::remove_var("JUDGEMENT_GRAPH_WRITES"),
            }
        }
    }
}

async fn review_request(
    app: &axum::Router,
    token: &str,
    method: &str,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let req = HttpRequest::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).expect("review response JSON"),
    )
}

#[tokio::test]
async fn manual_review_actions_and_cloud_ask_work_as_coralswarm_app() {
    let Some(probe) = RlsProbe::setup("orgt_action_cards_rls").await else {
        eprintln!("skipping manual_review_actions_and_cloud_ask_work_as_coralswarm_app: set ORGT_TEST_BASE_URL");
        return;
    };

    // ── Fixture, seeded through the OWNER pool ────────────────────────────────
    // Seeding must bypass the very policies under test.

    // Literal synthetic evidence supports both displayed endpoint names through
    // real Library admission. It is not a fabricated model/identity receipt.
    sqlx::query(
        "UPDATE atoms SET content_tsv = to_tsvector('simple', 'what needs review dorian pending'), \
                          content_inline = 'what needs review: Dorian and Dorian Rivera are pending identity review' \
         WHERE atom_id = $1",
    )
    .bind(probe.a.atom_id)
    .execute(&probe.owner)
    .await
    .expect("seed keyword-searchable atom");

    // The explicit human-UNSURE item below, not a speculative lexical sweep,
    // makes this pair actionable. The fixture itself does not decide identity.
    sqlx::query("UPDATE nodes SET type = 'entity', title = 'Dorian' WHERE node_id = $1")
        .bind(probe.a.node_id)
        .execute(&probe.owner)
        .await
        .expect("retype probe reef as an entity");
    let dup_node = Uuid::new_v4();
    sqlx::query("INSERT INTO nodes(node_id, graph_id, type, title) VALUES ($1, $2, 'entity', $3)")
        .bind(dup_node)
        .bind(probe.a.graph_id)
        .bind("Dorian Rivera")
        .execute(&probe.owner)
        .await
        .expect("seed duplicate entity reef");
    for node in [probe.a.node_id, dup_node] {
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(node)
            .bind(probe.a.atom_id)
            .execute(&probe.owner)
            .await
            .unwrap();
    }
    let _review_flag = ReviewFlag::enabled();
    let owner_store = PostgresStore::new(probe.owner.clone());
    let judgement = owner_store
        .write_judgement(NewJudgement {
            graph_id: probe.a.graph_id,
            node_a_id: probe.a.node_id,
            node_b_id: dup_node,
            verdict: JudgementVerdict::Unsure,
            source: JudgementSource::Human,
            confidence: None,
            model_id: None,
            prompt_version: None,
            evidence: json!({"synthetic_fixture":"explicit human question; not machine evidence"}),
            created_by: Some(probe.a.user_id),
            expires_at: Some(chrono::Utc::now() + chrono::Duration::days(1)),
        })
        .await
        .expect("seed explicit human review question");

    let jwks_url = spawn_local_jwks().await;
    // SAFETY: single-threaded with respect to env mutation — this is the only
    // test in this binary and the only thing here that touches CLERK_*.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let sub_a = "action_cards_probe_user_a";
    let sub_b = "action_cards_probe_user_b";
    for (sub, fixture) in [(sub_a, &probe.a), (sub_b, &probe.b)] {
        sqlx::query("UPDATE users SET clerk_user_id=$1 WHERE id=$2")
            .bind(sub)
            .bind(fixture.user_id)
            .execute(&probe.owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner')")
            .bind(fixture.account_id)
            .bind(fixture.user_id)
            .execute(&probe.owner)
            .await
            .unwrap();
    }

    // AppState served ENTIRELY off the coralswarm_app pool.
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));
    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));

    // Scoped cloud Ask remains a positive stream round trip over real evidence.
    let (status, frames) = post_stream(
        &app,
        &token_a,
        json!({ "text": "what needs review", "graph_id": probe.a.graph_id }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "stream must return 200 — frames: {}",
        render(&frames)
    );
    assert!(
        frames.iter().all(|f| f.event != "error"),
        "stream must not carry an error frame — frames: {}",
        render(&frames)
    );

    let meta = frames
        .iter()
        .find(|f| f.event == "meta")
        .expect("cloud Ask meta");
    assert!(meta.data["conversation_id"].is_string());
    let answer = frames
        .iter()
        .find(|f| f.event == "final")
        .expect("cloud Ask final");
    assert!(
        answer.data["message_id"].is_string(),
        "assistant persisted: {}",
        render(&frames)
    );
    assert!(
        answer.data["legacy_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["atom_id"] == json!(probe.a.atom_id)),
        "real authorized source: {}",
        render(&frames)
    );

    // The current positive action surface returns concrete IDs and supported
    // titles, then accepts the human's explicit confirm through the router.
    let queue = format!("/v1/graphs/{}/merge-review", probe.a.graph_id);
    let confirm = format!("{queue}/confirm");
    let (status, page) = review_request(&app, &token_a, "GET", &queue, json!(null)).await;
    assert_eq!(status, StatusCode::OK, "review page: {page}");
    let items = page["items"].as_array().expect("review items");
    assert_eq!(items.len(), 1, "the actual question must render: {page}");
    let card = &items[0];
    assert_eq!(card["judgement_id"], json!(judgement));
    let mut endpoints = vec![
        (card["a"]["node_id"].clone(), card["a"]["title"].clone()),
        (card["b"]["node_id"].clone(), card["b"]["title"].clone()),
    ];
    endpoints.sort_by_key(|e| e.0.to_string());
    let mut expected = vec![
        (json!(probe.a.node_id), json!("Dorian")),
        (json!(dup_node), json!("Dorian Rivera")),
    ];
    expected.sort_by_key(|e| e.0.to_string());
    assert_eq!(
        endpoints, expected,
        "actionable endpoint identities and literal-supported titles"
    );
    let token_b = mint(json!({"iss":ISSUER,"sub":sub_b,"exp":future_exp()}));
    let payload = json!({"judgement_id":judgement,"keep_id":probe.a.node_id,"dup_id":dup_node});
    for method in ["GET", "POST"] {
        let (status, body) = review_request(
            &app,
            &token_b,
            method,
            if method == "GET" { &queue } else { &confirm },
            payload.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "foreign {method}: {body}");
        assert_eq!(body["error"], "ocean not found");
    }
    let (status, body) = review_request(&app, &token_a, "POST", &confirm, payload.clone()).await;
    assert_eq!(status, StatusCode::OK, "human confirm: {body}");
    assert_eq!(body["ok"], json!(true));
    let (human,source,verdict):(Uuid,String,String)=sqlx::query_as(
        "SELECT next.judgement_id,next.source,next.verdict FROM reef_judgement old JOIN reef_judgement next ON next.judgement_id=old.superseded_by WHERE old.judgement_id=$1 AND old.annulled_at IS NOT NULL")
        .bind(judgement).fetch_one(&probe.owner).await.expect("superseded question points at real decision");
    assert_eq!(source, "human");
    assert_eq!(verdict, "positive");
    let backlink:Option<Uuid>=sqlx::query_scalar("SELECT judgement_id FROM node_merges WHERE keep_id=$1 AND dup_id=$2 ORDER BY created_at DESC LIMIT 1")
        .bind(probe.a.node_id).bind(dup_node).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(
        backlink,
        Some(human),
        "materialized merge has its human judgement backlink"
    );
    let duplicate_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)")
            .bind(dup_node)
            .fetch_one(&probe.owner)
            .await
            .unwrap();
    assert!(
        !duplicate_exists,
        "confirmed duplicate is physically merged"
    );
    let (status, page) = review_request(&app, &token_a, "GET", &queue, json!(null)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["items"], json!([]), "resolved question leaves queue");
    let (status, body) = review_request(&app, &token_a, "POST", &confirm, payload).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "stale card cannot merge twice: {body}"
    );
    probe.teardown().await;
}
