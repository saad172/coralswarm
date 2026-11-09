//! Real HTTP cloud scoped-Ask retrieval served entirely through a genuine
//! `coralswarm_app` login using the shared RlsProbe fixture. The cloud route
//! now delegates to Scoped Ask and Library read: genuinely unmapped chunks
//! appear in `legacy_evidence`, not compatibility `sources` or fabricated
//! retained citations. Exact own bytes/UUIDs and foreign-account denial are
//! the oracles. Configured but unreachable fake R2 must not hide inline rows.
//!
//! This covers current cloud admission/retrieval, not the separate poolless
//! legacy fetch_atom_content helper or availability of external storage.
//!
//! Env-gated, like every other `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test retrieval_scoped_pg

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use serde_json::{json, Value};
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tokio::sync::Mutex;
use tower::ServiceExt;

// Both cases initialize their own live JWKS listener. Serialize the shared
// process environment for ordinary whole-target runs as well as --exact runs.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair — same fixture
/// `routes_b5b_rls_pg.rs`/`routes_b2_rls_pg.rs` use, duplicated for the same
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

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

fn assert_own_legacy_evidence(body: &Value, probe: &RlsProbe) {
    assert_eq!(body["mode"], "retrieval_only");
    assert_eq!(body["library_scope"]["graph_ids"], json!([probe.a.graph_id]));
    assert_eq!(body["sources"], json!([]), "legacy sources compatibility field stays empty");
    assert_eq!(body["outcomes"], json!([]), "unmapped chunks are not semantic outcomes");
    assert_eq!(body["retained_citations"], json!([]), "no retained passage is fabricated");
    let evidence = body["legacy_evidence"].as_array().expect("legacy evidence array present");
    assert_eq!(evidence.len(), 1, "exact own result only: {body}");
    assert_eq!(evidence[0]["kind"], "legacy_chunk");
    assert_eq!(evidence[0]["representation"], "unmapped_legacy_chunk");
    assert_eq!(evidence[0]["graph_id"], json!(probe.a.graph_id));
    assert_eq!(evidence[0]["atom_id"], json!(probe.a.atom_id));
    assert_eq!(evidence[0]["content"], "the narwhal orchestra rehearsal starts at noon");
    assert_eq!(evidence[0]["content_truncated"], false);
    let serialized = body.to_string();
    for forbidden in [probe.b.graph_id.to_string(), probe.b.atom_id.to_string(), "B account must never see this content".to_string()] {
        assert!(!serialized.contains(&forbidden), "foreign identity/content must never be emitted: {body}");
    }
}

#[tokio::test]
async fn create_query_retrieval_round_trip_is_account_scoped_as_coralswarm_app() {
    let Some(probe) = RlsProbe::setup("orgt_retrieval_scoped_routes").await else {
        eprintln!(
            "skipping create_query_retrieval_round_trip_is_account_scoped_as_coralswarm_app: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;

    // Distinctive inline legacy content exercises the current cloud Library
    // read under the real application role, with no model or retained mapping.
    sqlx::query(
        "UPDATE atoms SET content_tsv = to_tsvector('simple', 'narwhal orchestra rehearsal'), \
                          content_inline = 'the narwhal orchestra rehearsal starts at noon' \
         WHERE atom_id = $1",
    )
    .bind(probe.a.atom_id)
    .execute(&probe.owner)
    .await
    .expect("seed A's keyword-searchable atom");
    // B gets a distinct, equally-findable atom in its OWN graph — this is
    // the cross-account probe below aiming at a REAL row that must not leak.
    sqlx::query(
        "UPDATE atoms SET content_tsv = to_tsvector('simple', 'narwhal orchestra rehearsal'), \
                          content_inline = 'B account must never see this content' \
         WHERE atom_id = $1",
    )
    .bind(probe.b.atom_id)
    .execute(&probe.owner)
    .await
    .expect("seed B's keyword-searchable atom");

    let jwks_url = spawn_local_jwks().await;
    // SAFETY: both cases hold ENV_LOCK across their env-sensitive HTTP calls.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let sub_a = "retrieval_scoped_probe_user_a";
    sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
        .bind(sub_a)
        .bind(probe.a.user_id)
        .execute(&probe.owner)
        .await
        .expect("stamp A's clerk_user_id");
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(probe.a.account_id)
        .bind(probe.a.user_id)
        .execute(&probe.owner)
        .await
        .expect("seed A's account_members");

    // The real application-role pool serves current cloud scoped Ask. No
    // model or R2 is configured, so raw unmapped inline evidence is returned.
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));

    // ── Positive path: A queries A's own ocean, finds A's own content ──────
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/queries")
        .header("authorization", format!("Bearer {token_a}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "text": "narwhal orchestra",
                "graph_id": probe.a.graph_id,
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "create_query must serve A's own ocean under the coralswarm_app role"
    );
    let body = body_json(resp).await;
    assert_own_legacy_evidence(&body, &probe);

    // Cross-account: the current cloud Library scope boundary conceals an
    // inaccessible graph with the same opaque404 as a missing source. No
    // foreign evidence or object identity may be disclosed in the response.
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/queries")
        .header("authorization", format!("Bearer {token_a}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "text": "narwhal orchestra",
                "graph_id": probe.b.graph_id,
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let denied = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "foreign graph must be concealed: {denied}");
    assert_eq!(
        denied,
        json!({"error": "source not found or access denied"}),
        "denial must expose no foreign graph/atom UUID, content or existence detail"
    );

    probe.teardown().await;
}

/// Current cloud Ask returns an authorized unmapped inline chunk even with
/// fake R2 configured at a closed loopback port. This preserves the inline
/// compatibility requirement after cloud Ask moved to Library read. It does
/// not claim to execute the older poolless fetch_atom_content implementation.
#[tokio::test]
async fn inline_content_reaches_the_answer_even_when_r2_is_configured_and_broken() {
    let Some(probe) = RlsProbe::setup("orgt_retrieval_inline_first").await else {
        eprintln!(
            "skipping inline_content_reaches_the_answer_even_when_r2_is_configured_and_broken: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;

    // Keep the genuine wrapped-key fixture and unreachable R2 client. The
    // current cloud Library read consumes inline evidence directly; this test
    // must not mistake an authentication503 for a storage/DEK regression.
    unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) };
    let kek = coralswarm_core::kek::load_kek().expect("test KEK loads");
    let dek = coralswarm_core::kek::generate_dek();
    let wrapped = coralswarm_core::kek::wrap_dek(&kek, &dek).expect("wrap test DEK");
    sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
        .bind(&wrapped)
        .bind(probe.a.account_id)
        .execute(&probe.owner)
        .await
        .expect("seed A's wrapped DEK");

    // Exactly the pre-cutover shape: real inline content, plus a blob key
    // that no object was ever written for.
    sqlx::query(
        "UPDATE atoms SET content_tsv = to_tsvector('simple', 'narwhal orchestra rehearsal'), \
                          content_inline = 'the narwhal orchestra rehearsal starts at noon', \
                          content_blob_key = 'pre-cutover/never-uploaded' \
         WHERE atom_id = $1",
    )
    .bind(probe.a.atom_id)
    .execute(&probe.owner)
    .await
    .expect("seed A's pre-cutover atom");

    // This test must authenticate independently of the other test's process
    // environment/cache, including when the binary runs this case with --exact.
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: ENV_LOCK is held until the request and response assertions finish.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let sub_a = "retrieval_inline_first_user_a";
    sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
        .bind(sub_a)
        .bind(probe.a.user_id)
        .execute(&probe.owner)
        .await
        .expect("stamp A's clerk_user_id");
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(probe.a.account_id)
        .bind(probe.a.user_id)
        .execute(&probe.owner)
        .await
        .expect("seed A's account_members");

    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    // R2 configured but pointed at a closed port — every GET must fail.
    state.r2 = Some(Arc::new(
        coralswarm_core::r2::R2Client::new_with_endpoint_override(
            "unused",
            "unused",
            "unused",
            "unused-bucket",
            "http://127.0.0.1:1",
        )
        .await,
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/queries")
        .header("authorization", format!("Bearer {token_a}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "text": "narwhal orchestra",
                "graph_id": probe.a.graph_id,
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    // Authentication must succeed before this200/content oracle can prove
    // that configured broken R2 did not hide the authorized inline evidence.
    let st = resp.status();
    let body = body_json(resp).await;
    assert_eq!(
        st,
        StatusCode::OK,
        "an all-inline batch must not depend on R2 or on DEK resolution — body: {body}"
    );
    assert_own_legacy_evidence(&body, &probe);

    probe.teardown().await;
}
