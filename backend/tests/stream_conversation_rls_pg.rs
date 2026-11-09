//! Scoped cloud conversation round trips served entirely off a real
//! coralswarm_app login through POST /v1/queries/stream and a local Clerk JWT.
//! Positive turns persist both roles, owning account and ocean descriptors;
//! inherited and explicitly replaced Library scopes remain server-owned.
//! Foreign conversation requests fail with non-leaking HTTP404 JSON before
//! SSE starts. Both direct SQL wrong-account/unscoped controls remain to
//! prove the four conversation tables actually enforce RLS.
//! The cloud handler now delegates to scoped_ask before constructing its
//! meta/final relay; these tests assert the current P7 scope contract rather
//! than the retired worker's 200/SSE-error or silent cross-account switch.
//! Env-gated on ORGT_TEST_BASE_URL and run with --test-threads=1 in CI.

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use serde_json::{json, Value};
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair — same fixture
/// `retrieval_scoped_pg.rs`/`routes_b2_rls_pg.rs` use, duplicated for the same
/// separate-compilation-unit reason.
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

/// One parsed SSE frame: `(event name, data payload)`.
#[derive(Debug)]
struct SseFrame {
    event: String,
    data: Value,
}

/// Drain and parse the complete real SSE body. Positive persistence is checked
/// separately below; early JSON refusals use post_refusal instead.
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

fn frame<'a>(frames: &'a [SseFrame], name: &str) -> Option<&'a SseFrame> {
    frames.iter().find(|f| f.event == name)
}

fn render(frames: &[SseFrame]) -> String {
    frames
        .iter()
        .map(|f| format!("{}={}", f.event, f.data))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// `POST /v1/queries/stream` through the real router, drained to completion.
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

/// Refusals happen before SSE starts on the scoped cloud path. Parse the real
/// JSON body so a 404/409 cannot accidentally pass as an empty stream.
async fn post_refusal(app: &axum::Router, token: &str, body: Value) -> (StatusCode, Value) {
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/queries/stream")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    assert_eq!(resp.headers()["content-type"], "application/json");
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).expect("structured refusal JSON"),
    )
}

#[tokio::test]
async fn create_query_stream_conversation_surface_is_account_scoped_as_coralswarm_app() {
    let Some(probe) = RlsProbe::setup("orgt_stream_conversation_rls").await else {
        eprintln!(
            "skipping create_query_stream_conversation_surface_is_account_scoped_as_coralswarm_app: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // Keyword-searchable content in each account's own ocean, so the
    // retrieval-only branch (no LLM in `test_app_state()`) has real sources to
    // return rather than tripping the honesty guard. Seeded through the OWNER
    // pool — fixture seeding must bypass the very policies under test.
    for (atom_id, inline) in [
        (
            probe.a.atom_id,
            "the narwhal orchestra rehearsal starts at noon",
        ),
        (probe.b.atom_id, "B account must never see this content"),
    ] {
        sqlx::query(
            "UPDATE atoms SET content_tsv = to_tsvector('simple', 'narwhal orchestra rehearsal'), \
                              content_inline = $2 \
             WHERE atom_id = $1",
        )
        .bind(atom_id)
        .bind(inline)
        .execute(&probe.owner)
        .await
        .expect("seed keyword-searchable atom");
    }

    let jwks_url = spawn_local_jwks().await;
    // SAFETY: CI runs this binary with --test-threads=1; both tests use
    // the same shared Clerk test keypair and a loopback JWKS listener.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    // Real Clerk identities for BOTH accounts — the cross-account assertion
    // needs B to be a genuinely authenticated caller, not an unauthenticated
    // one (an unauthenticated 401 would prove nothing about RLS).
    let sub_a = "stream_conv_probe_user_a";
    let sub_b = "stream_conv_probe_user_b";
    for (sub, fixture) in [(sub_a, &probe.a), (sub_b, &probe.b)] {
        sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
            .bind(sub)
            .bind(fixture.user_id)
            .execute(&probe.owner)
            .await
            .expect("stamp clerk_user_id");
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(fixture.account_id)
        .bind(fixture.user_id)
        .execute(&probe.owner)
        .await
        .expect("seed account_members");
    }

    // AppState served ENTIRELY off the coralswarm_app pool — no LLM, no R2,
    // matching `test_app_state()`'s defaults, so the stream takes its
    // retrieval-only branch and exercises the conversation surface end to end
    // with no network dependency.
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));
    let token_b = mint(json!({ "iss": ISSUER, "sub": sub_b, "exp": future_exp() }));

    // ── 1. Turn one: lazily create the conversation and persist both turns ──
    // A positive route round trip under the restricted login must create and
    // persist the conversation; direct negative controls below prove its RLS.
    let (status, frames) = post_stream(
        &app,
        &token_a,
        json!({ "text": "narwhal orchestra", "graph_id": probe.a.graph_id }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "POST /v1/queries/stream must return 200 + an SSE body — frames: {}",
        render(&frames)
    );
    assert!(
        frame(&frames, "error").is_none(),
        "turn 1 must not emit an SSE error frame (this is what an unscoped \
         conversation surface looks like under coralswarm_app) — frames: {}",
        render(&frames)
    );
    let meta = frame(&frames, "meta").unwrap_or_else(|| {
        panic!(
            "turn 1 must emit a meta frame — frames: {}",
            render(&frames)
        )
    });
    assert_eq!(
        meta.data["new_conversation"],
        json!(true),
        "turn 1 must have lazily created the conversation — meta: {}",
        meta.data
    );
    let conversation_id: Uuid = meta.data["conversation_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(|| panic!("meta must carry a conversation_id — meta: {}", meta.data));
    let final_frame = frame(&frames, "final").unwrap_or_else(|| {
        panic!(
            "turn 1 must emit a final frame — frames: {}",
            render(&frames)
        )
    });
    assert!(
        final_frame.data["message_id"].is_string(),
        "the assistant turn must have been PERSISTED (message_id present) — final: {}",
        final_frame.data
    );

    let evidence = final_frame.data["legacy_evidence"]
        .as_array()
        .expect("scoped evidence");
    assert!(
        evidence
            .iter()
            .any(|e| e["atom_id"] == json!(probe.a.atom_id)),
        "A's own source must be returned"
    );
    assert!(
        evidence
            .iter()
            .all(|e| e["atom_id"] != json!(probe.b.atom_id)),
        "B's private source must not be returned"
    );

    // The row is real, and stamped with the OCEAN'S owning account — the scope
    // every subsequent conversation statement in the turn keys on.
    let conv_account: Uuid =
        sqlx::query_scalar("SELECT account_id FROM conversations WHERE id = $1")
            .bind(conversation_id)
            .fetch_one(&probe.owner)
            .await
            .expect("the conversation row must exist");
    assert_eq!(
        conv_account, probe.a.account_id,
        "the conversation must be stamped with the ocean's owning account"
    );

    // Both turns persisted, in order, through `append_message_scoped`.
    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role FROM conversation_messages WHERE conversation_id = $1 ORDER BY seq",
    )
    .bind(conversation_id)
    .fetch_all(&probe.owner)
    .await
    .expect("read conversation_messages");
    assert_eq!(
        roles,
        vec!["user".to_string(), "assistant".to_string()],
        "turn 1 must persist the user turn AND the assistant turn"
    );

    // The per-turn ocean scope descriptor (`conversation_oceans`) — the one
    // converted table whose 0083 policy anchors on `graph_id`, not on the
    // conversation's account, and therefore scopes PER OCEAN.
    let oceans: Vec<Uuid> =
        sqlx::query_scalar("SELECT graph_id FROM conversation_oceans WHERE conversation_id = $1")
            .bind(conversation_id)
            .fetch_all(&probe.owner)
            .await
            .expect("read conversation_oceans");
    assert_eq!(
        oceans,
        vec![probe.a.graph_id],
        "the turn's ocean scope descriptor must have been written"
    );

    // ── 2. Turn two: same conversation, history read back ───────────────────
    let (status, frames) = post_stream(
        &app,
        &token_a,
        json!({
            "text": "and when does it finish?",
            "graph_id": probe.a.graph_id,
            "conversation_id": conversation_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "turn 2 status");
    assert!(
        frame(&frames, "error").is_none(),
        "turn 2 must not emit an SSE error frame — frames: {}",
        render(&frames)
    );
    let meta = frame(&frames, "meta").unwrap_or_else(|| {
        panic!(
            "turn 2 must emit a meta frame — frames: {}",
            render(&frames)
        )
    });
    assert_eq!(
        meta.data["conversation_id"].as_str(),
        Some(conversation_id.to_string().as_str()),
        "turn 2 must continue the SAME conversation — meta: {}",
        meta.data
    );
    assert_eq!(
        meta.data["new_conversation"],
        json!(false),
        "turn 2 must not create a new conversation — meta: {}",
        meta.data
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM conversation_messages WHERE conversation_id = $1")
            .bind(conversation_id)
            .fetch_one(&probe.owner)
            .await
            .expect("count messages");
    assert_eq!(
        count, 4,
        "turn 2 must append its own user + assistant rows to the existing thread"
    );

    // ── 3. Cross-account: B cannot reach A's conversation ───────────────────
    // B is a fully authenticated caller with an ocean of its own, aiming a
    // valid `conversation_id` belonging to A.
    let (status, refusal) = post_refusal(
        &app,
        &token_b,
        json!({
            "text": "narwhal orchestra",
            "graph_id": probe.b.graph_id,
            "conversation_id": conversation_id,
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "foreign conversation: {refusal}"
    );
    assert_eq!(refusal["error"], "conversation not found or access denied");
    assert!(
        refusal.get("conversation_id").is_none(),
        "refusal must not disclose metadata"
    );
    let count_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM conversation_messages WHERE conversation_id = $1")
            .bind(conversation_id)
            .fetch_one(&probe.owner)
            .await
            .expect("count messages after B's attempt");
    assert_eq!(
        count_after, 4,
        "B's attempt must not have appended anything to A's conversation"
    );

    // ── 4. The policies these statements run under really bite ──────────────
    // Without this the round trips above could pass for the wrong reason (e.g.
    // a role that quietly bypasses RLS). Same query SHAPES the handler issues.
    for (query, binds) in [
        (
            "SELECT id FROM conversations WHERE id = $1",
            vec![conversation_id],
        ),
        (
            "SELECT id FROM conversation_messages WHERE conversation_id = $1",
            vec![conversation_id],
        ),
        (
            "SELECT graph_id FROM conversation_oceans WHERE conversation_id = $1",
            vec![conversation_id],
        ),
        (
            "SELECT edge_id FROM conversation_surfaced_edges WHERE conversation_id = $1",
            vec![conversation_id],
        ),
    ] {
        probe.assert_unscoped_hard_errors(query, &binds).await;
        probe
            .assert_cross_account_returns_nothing(probe.b.account_id, query, &binds)
            .await;
    }
    // ...and scoped to A they resolve A's own rows.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM conversations WHERE id = $1",
            &[conversation_id],
            &[conversation_id],
        )
        .await;
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT graph_id FROM conversation_oceans WHERE conversation_id = $1",
            &[conversation_id],
            &[probe.a.graph_id],
        )
        .await;

    probe.teardown().await;
}

/// A member can start and continue a conversation in another account's ocean.
/// Its saved scope is server-owned: omission inherits it, an explicit same-
/// account replacement succeeds, and changing accounts cannot move the thread.
#[tokio::test]
async fn conversation_in_other_account_inherits_and_explicitly_replaces_scope() {
    let Some(probe) = RlsProbe::setup("orgt_stream_conversation_rls_multi").await else {
        eprintln!("skipping conversation_in_other_account_inherits_and_explicitly_replaces_scope: set ORGT_TEST_BASE_URL");
        return;
    };

    let jwks_url = spawn_local_jwks().await;
    // SAFETY: the CI step runs this binary with `--test-threads=1`, and both
    // tests here set the same CLERK_* values against the same shared test
    // keypair.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    // ONE user (A), TWO accounts: their own, plus membership of B's account —
    // the exact Model B shape. `account_members` is what
    // `resolve_graph_reachable_by_user` gates on, so this is also what makes
    // B's ocean reachable to A at all.
    let sub_a = "stream_conv_multi_user_a";
    sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
        .bind(sub_a)
        .bind(probe.a.user_id)
        .execute(&probe.owner)
        .await
        .expect("stamp A's clerk_user_id");
    for account in [probe.a.account_id, probe.b.account_id] {
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account)
        .bind(probe.a.user_id)
        .execute(&probe.owner)
        .await
        .expect("seed A's membership");
    }

    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));
    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));

    // Turn 1 over B's ocean ⇒ the conversation is stamped with B's ACCOUNT.
    let (status, frames) = post_stream(
        &app,
        &token_a,
        json!({ "text": "first turn", "graph_id": probe.b.graph_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "turn 1 status");
    assert!(
        frame(&frames, "error").is_none(),
        "turn 1 over the other account's ocean must succeed — frames: {}",
        render(&frames)
    );
    let conversation_id: Uuid = frame(&frames, "meta")
        .and_then(|m| m.data["conversation_id"].as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(|| {
            panic!(
                "turn 1 meta must carry a conversation_id — frames: {}",
                render(&frames)
            )
        });
    let conv_account: Uuid =
        sqlx::query_scalar("SELECT account_id FROM conversations WHERE id = $1")
            .bind(conversation_id)
            .fetch_one(&probe.owner)
            .await
            .expect("read the conversation's account");
    assert_eq!(
        conv_account, probe.b.account_id,
        "the conversation must carry the OCEAN's owning account, not the caller's active one"
    );

    // Omission inherits B's saved scope even though A's active account is A.
    let (status, frames) = post_stream(
        &app,
        &token_a,
        json!({"text":"second turn", "conversation_id":conversation_id}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "inherited turn status");
    assert!(frame(&frames, "error").is_none(), "{}", render(&frames));
    assert_eq!(
        frame(&frames, "meta").unwrap().data["conversation_id"],
        json!(conversation_id)
    );
    assert_eq!(
        frame(&frames, "meta").unwrap().data["new_conversation"],
        json!(false)
    );
    assert!(frame(&frames, "final").unwrap().data["message_id"].is_string());

    // This literal source and link make the exact-node selector genuinely
    // readable through Library admission; no consent/admission flag is forged.
    sqlx::query("UPDATE atoms SET content_inline='Probe Node is the synthetic review subject',content_tsv=to_tsvector('simple','Probe Node synthetic review subject') WHERE atom_id=$1")
        .bind(probe.b.atom_id).execute(&probe.owner).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(probe.b.node_id)
        .bind(probe.b.atom_id)
        .execute(&probe.owner)
        .await
        .unwrap();
    let narrow = json!({"graph_ids":[probe.b.graph_id],"node_id":probe.b.node_id});
    let (status, refusal) = post_refusal(
        &app,
        &token_a,
        json!({
            "text":"Probe Node", "conversation_id":conversation_id, "library_scope":narrow,
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "implicit scope change: {refusal}"
    );
    assert_eq!(
        refusal["error"],
        "scope change requires scope_action replace"
    );

    let (status, frames) = post_stream(
        &app,
        &token_a,
        json!({
            "text":"Probe Node", "conversation_id":conversation_id,
            "library_scope":narrow, "scope_action":"replace",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "explicit replacement status");
    assert!(frame(&frames, "error").is_none(), "{}", render(&frames));
    let final_frame = frame(&frames, "final").expect("replacement final");
    assert!(final_frame.data["message_id"].is_string());
    assert_eq!(final_frame.data["scope_changed"], json!(true));
    assert!(
        final_frame.data["legacy_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["atom_id"] == json!(probe.b.atom_id)),
        "replacement retains B's genuinely scoped source"
    );

    assert_eq!(
        final_frame.data["library_scope"]["node_id"],
        json!(probe.b.node_id)
    );
    assert_eq!(
        final_frame.data["previous_library_scope"]["graph_ids"],
        json!([probe.b.graph_id])
    );
    assert!(final_frame.data["previous_library_scope"]["node_id"].is_null());

    // Explicit replacement does not authorize a cross-account thread move,
    // even when the actor is a genuine member of both accounts.
    let (status, refusal) = post_refusal(
        &app,
        &token_a,
        json!({
            "text":"other account", "conversation_id":conversation_id,
            "library_scope":{"graph_ids":[probe.a.graph_id]}, "scope_action":"replace",
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "cross-account replacement: {refusal}"
    );
    assert_eq!(
        refusal["error"],
        "conversation scope spans account boundaries"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM conversation_messages WHERE conversation_id=$1")
            .bind(conversation_id)
            .fetch_one(&probe.owner)
            .await
            .unwrap();
    assert_eq!(
        count, 6,
        "three accepted turns persist both roles; both refusals append nothing"
    );
    let (account, scope): (Uuid, Value) =
        sqlx::query_as("SELECT account_id,library_scope FROM conversations WHERE id=$1")
            .bind(conversation_id)
            .fetch_one(&probe.owner)
            .await
            .unwrap();
    assert_eq!(
        account, probe.b.account_id,
        "refusals cannot move thread ownership"
    );
    assert_eq!(
        scope["node_id"],
        json!(probe.b.node_id),
        "refusal preserves last accepted scope"
    );

    probe.teardown().await;
}
