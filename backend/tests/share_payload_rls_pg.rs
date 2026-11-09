//! Current cloud meeting-share contract plus the retained scoped payload proof.
//!
//! Whole-original Postgres share links are refused with the typed selected-
//! evidence-consent conflict before payload access. These HTTP tests verify
//! that refusal and its lack of content/publication/job side effects. They do
//! not claim an anonymous selected-evidence share API exists.
//!
//! The real payload builder still has a separate ordinary-role regression
//! proof: all five content values resolve under each source owner's scope,
//! while the original all-null checker and unscoped RLS-error control remain.
//! This is a scoped builder proof, not permission to bypass the HTTP refusal.
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test share_payload_rls_pg

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::{store::PostgresStore, tenancy::extractor::Tenant};
use serde_json::{json, Value};
use std::sync::Arc;
use support::rls_probe::{AccountFixture, RlsProbe};
use tower::ServiceExt;
use uuid::Uuid;

/// Every value the share payload is supposed to surface, seeded through the
/// owner/BYPASSRLS pool so the seeding itself is never subject to the policies
/// under test.
struct ShareFixture {
    token: String,
    team_token: String,
    source_id: String,
    title: String,
    transcript: String,
    duration_ms: i64,
    overview: String,
}

/// The atom timestamp behind `meeting_occurred_at` (`MIN(atoms.timestamp)`).
const OCCURRED_AT_PREFIX: &str = "2026-03-04T05:06:07";

async fn seed_share(probe: &RlsProbe, f: &AccountFixture, tag: &str) -> ShareFixture {
    let owner = &probe.owner;
    let source_id = format!("meeting-{tag}");
    // `title` is derived from the FIRST non-empty line of the raw text
    // (`routes::meetings::meeting_title_from_raw`), so the transcript body
    // carries both expected values at once.
    let title = format!("Quarterly {tag} planning sync");
    let transcript = format!("{title}\nAlice: shipping on Friday.\nBob: agreed.");
    let duration_ms = 1_234_567i64;
    let overview = format!("Team {tag} agreed to ship on Friday.");

    sqlx::query(
        "INSERT INTO meeting_transcripts \
             (graph_id, source_id, raw_inline, duration_ms, summary_status, summary) \
         VALUES ($1, $2, $3, $4, 'done', $5)",
    )
    .bind(f.graph_id)
    .bind(&source_id)
    .bind(&transcript)
    .bind(duration_ms)
    .bind(json!({ "overview": overview, "decisions": [], "action_items": [] }))
    .execute(owner)
    .await
    .expect("seed meeting_transcripts");

    // `meeting_occurred_at` is `MIN(atoms.timestamp)` over
    // `source_id LIKE '<source_id>%'`, so the payload's `occurred_at` comes
    // from this row — a SECOND RLS-covered table (`atoms`), reached through a
    // different policy shape (graph-mediated) than `meeting_transcripts`.
    sqlx::query(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'slack', $2, 'blob-key', $3::timestamptz)",
    )
    .bind(f.graph_id)
    .bind(format!("{source_id}:chunk:0"))
    .bind(format!("{OCCURRED_AT_PREFIX}Z"))
    .execute(owner)
    .await
    .expect("seed meeting atom");

    let token = format!("share-link-{tag}");
    sqlx::query(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by) \
         VALUES ($1, $2, $3, 'link', $4)",
    )
    .bind(f.graph_id)
    .bind(&source_id)
    .bind(&token)
    .bind(f.user_id)
    .execute(owner)
    .await
    .expect("seed link meeting_share");

    let team_token = format!("share-team-{tag}");
    sqlx::query(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by) \
         VALUES ($1, $2, $3, 'team', $4)",
    )
    .bind(f.graph_id)
    .bind(&source_id)
    .bind(&team_token)
    .bind(f.user_id)
    .execute(owner)
    .await
    .expect("seed team meeting_share");

    ShareFixture { token, team_token, source_id, title, transcript, duration_ms, overview }
}

/// Local JWKS listener serving the shared test keypair — same fixture
/// `retrieval_scoped_pg.rs`/`routes_b5b_rls_pg.rs` use, duplicated for the same
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

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

/// State whose entire content plane is the real `coralswarm_app` login.
fn state_on(probe: &RlsProbe) -> Arc<coralswarm_backend::AppState> {
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    Arc::new(state)
}

fn app_on(probe: &RlsProbe) -> axum::Router {
    coralswarm_backend::test_support_app(state_on(probe))
}

/// Snapshot only the synthetic accounts' content and effects, never auth-cache
/// bookkeeping. The refusal must not mutate originals, mint consent or dispatch.
async fn share_state(probe: &RlsProbe) -> Value {
    let mut state = serde_json::Map::new();
    let graphs = vec![probe.a.graph_id, probe.b.graph_id];
    let accounts = vec![probe.a.account_id, probe.b.account_id];
    for table in ["meeting_transcripts", "atoms", "meeting_shares", "source_items", "source_versions"] {
        let rows: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) \
             FROM {table} r WHERE graph_id=ANY($1)"
        )).bind(&graphs).fetch_one(&probe.owner).await.unwrap();
        state.insert(table.into(), rows);
    }
    for table in [
        "selected_evidence_publications", "corpus_capture_admissions", "jobs",
        "credit_balances", "credit_ledger", "managed_budget_periods",
        "managed_attempt_receipts", "managed_credit_reservations",
        "managed_embedding_inputs", "managed_enrichment_inputs", "llm_usage", "llm_usage_pending",
    ] {
        let rows: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) \
             FROM {table} r WHERE account_id=ANY($1)"
        )).bind(&accounts).fetch_one(&probe.owner).await.unwrap();
        state.insert(table.into(), rows);
    }
    Value::Object(state)
}

async fn assert_whole_original_refused(resp: axum::response::Response) {
    let status = resp.status();
    let body = body_json(resp).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body, json!({
        "error": "Whole-original links require selected evidence consent",
        "code": "selected_evidence_consent_required",
    }), "the refusal must contain no transcript, summary, token or source metadata");
}

/// Every content field of the shared payload. `visibility` is excluded on
/// purpose: it is the one field the pre-fix code still filled in correctly,
/// which is exactly what made the failure look like a 200.
const CONTENT_FIELDS: [&str; 5] = ["title", "occurred_at", "duration_ms", "summary", "transcript"];

/// The checker, as a `Result` rather than a bare `assert!` cascade, so
/// [`all_null_payload_is_the_negative_control`] can assert that it REJECTS the
/// all-null payload without panic machinery. `Err(reason)` explains which
/// field failed and why.
fn check_payload_fully_resolved(v: &Value, f: &ShareFixture, visibility: &str) -> Result<(), String> {
    if CONTENT_FIELDS
        .iter()
        .all(|k| v.get(*k).map(Value::is_null).unwrap_or(true))
    {
        return Err(format!(
            "share payload came back ALL NULL — the exact signature of the pre-fix \
             unscoped reads (each RLS failure swallowed into a `warn!` arm and rendered \
             as `null` behind a 200): {v}"
        ));
    }
    if v["visibility"] != json!(visibility) {
        return Err(format!("visibility must be {visibility:?}: {v}"));
    }
    if v["title"].as_str() != Some(f.title.as_str()) {
        return Err(format!(
            "title must be derived from the REAL transcript text (scoped \
             meeting_transcripts read): {v}"
        ));
    }
    if v["transcript"].as_str() != Some(f.transcript.as_str()) {
        return Err(format!("transcript must be the REAL stored raw text: {v}"));
    }
    if v["duration_ms"].as_i64() != Some(f.duration_ms) {
        return Err(format!(
            "duration_ms must come off the REAL transcript row: {v}"
        ));
    }
    if v["summary"]["overview"].as_str() != Some(f.overview.as_str()) {
        return Err(format!(
            "summary must be the REAL `summary_status = 'done'` JSONB, read off the same \
             scoped transcript row: {v}"
        ));
    }
    match v["occurred_at"].as_str() {
        Some(t) if t.starts_with(OCCURRED_AT_PREFIX) => {}
        other => {
            return Err(format!(
                "occurred_at must be MIN(atoms.timestamp) for this meeting (the scoped \
                 `atoms` read), got {other:?}: {v}"
            ))
        }
    }
    Ok(())
}

fn assert_payload_is_fully_resolved(v: &Value, f: &ShareFixture, visibility: &str) {
    if let Err(reason) = check_payload_fully_resolved(v, f, visibility) {
        panic!("{reason}");
    }
}

// ── Explicit legacy whole-original HTTP refusals ─────────────────────────────

#[tokio::test]
async fn public_whole_original_share_is_refused_without_side_effects() {
    let Some(probe) = RlsProbe::setup("orgt_share_payload_public").await else {
        eprintln!("skipping public_whole_original_share_is_refused_without_side_effects: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = seed_share(&probe, &probe.a, "a").await;
    let b = seed_share(&probe, &probe.b, "b").await;
    let before = share_state(&probe).await;
    let app = app_on(&probe);
    // The cloud refusal precedes token lookup. Neither a valid legacy token,
    // a team token, nor an unknown token grants whole-original disclosure.
    for token in [a.token.as_str(), b.token.as_str(), b.team_token.as_str(), "token-that-never-existed"] {
        let resp = app.clone().oneshot(
            HttpRequest::builder().uri(format!("/v1/shared/{token}"))
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_whole_original_refused(resp).await;
    }
    assert_eq!(share_state(&probe).await, before,
        "refused public links must preserve originals/shares and create no publication, job or financial effects");
    probe.teardown().await;
}

#[tokio::test]
async fn authed_whole_original_share_is_refused_without_side_effects() {
    let Some(probe) = RlsProbe::setup("orgt_share_payload_authed").await else {
        eprintln!("skipping authed_whole_original_share_is_refused_without_side_effects: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = seed_share(&probe, &probe.a, "a").await;
    let b = seed_share(&probe, &probe.b, "b").await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: this is the only CLERK_* mutator; run this suite single-threaded.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }
    let sub_a = "share_payload_probe_user_a";
    sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
        .bind(sub_a).bind(probe.a.user_id).execute(&probe.owner).await
        .expect("stamp A's clerk_user_id");
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(probe.a.account_id).bind(probe.a.user_id).execute(&probe.owner).await
        .expect("seed A's account_members");
    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));
    let app = app_on(&probe);
    let before = share_state(&probe).await;
    // A real authenticated owner still cannot use either their own legacy
    // token or another account's token as selected-passage consent.
    for token in [a.token.as_str(), b.token.as_str(), b.team_token.as_str(), "token-that-never-existed"] {
        let resp = app.clone().oneshot(
            HttpRequest::builder().uri(format!("/v1/meetings/shares/view/{token}"))
                .header("authorization", format!("Bearer {token_a}"))
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_whole_original_refused(resp).await;
    }
    assert_eq!(share_state(&probe).await, before,
        "refused authenticated links must preserve originals/shares and create no publication, job or financial effects");
    probe.teardown().await;
}

// ── Real scoped builder values, separately from HTTP disclosure authority ─────

#[tokio::test]
async fn scoped_builder_resolves_each_owners_real_values_as_coralswarm_app() {
    let Some(probe) = RlsProbe::setup("orgt_share_payload_builder").await else {
        eprintln!("skipping scoped_builder_resolves_each_owners_real_values_as_coralswarm_app: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = seed_share(&probe, &probe.a, "a").await;
    let b = seed_share(&probe, &probe.b, "b").await;
    let state = state_on(&probe);
    let tenant = Tenant::legacy(&state);
    let before = share_state(&probe).await;
    // A → B → A on the SAME app pool: actual transcript_scope must derive the
    // requested graph owner's account each time, never an ambient/first owner.
    for (graph, fixture, other) in [
        (probe.a.graph_id, &a, &b),
        (probe.b.graph_id, &b, &a),
        (probe.a.graph_id, &a, &b),
    ] {
        let v = coralswarm_backend::test_scoped_shared_payload(
            &state, &tenant, graph, &fixture.source_id, "link",
        ).await;
        assert_payload_is_fully_resolved(&v, fixture, "link");
        assert!(!v.to_string().contains(&other.overview),
            "the scoped builder must never mix another account's content: {v}");
    }
    assert_eq!(share_state(&probe).await, before,
        "the direct scoped builder proof must remain read-only and cannot authorize publication");
    probe.teardown().await;
}

// ── The negative control, as a first-class test ──────────────────────────────

/// Pins the old all-null failure so the separately named scoped builder proof
/// cannot silently weaken into merely resolving without an error.
///
/// Three parts:
///   1. Each read `build_shared_payload` performs, run as `coralswarm_app`
///      WITHOUT `app.account_id` set — precisely what AMBIENT (pool-level)
///      execution does — HARD ERRORS, with the SQLSTATE an RLS scope failure
///      actually raises (`42704`/`22P02`, never `42703`/`42P01`, which is why
///      the deleted `is_undefined_column_or_table` classifier could not have
///      matched it).
///   2. [`check_payload_fully_resolved`] REJECTS the all-null payload those
///      swallowed errors produced. Otherwise the scoped builder proof would
///      accept the old bug despite the legacy HTTP routes now being refused.
///   3. Scoped to the owner, the very same reads return the real rows.
#[tokio::test]
async fn all_null_payload_is_the_negative_control() {
    let Some(probe) = RlsProbe::setup("orgt_share_payload_negctl").await else {
        eprintln!("skipping all_null_payload_is_the_negative_control: set ORGT_TEST_BASE_URL");
        return;
    };
    let a = seed_share(&probe, &probe.a, "a").await;
    let _b = seed_share(&probe, &probe.b, "b").await;

    // ── 1. Unscoped ⇒ hard error, with the RLS-scope SQLSTATEs ───────────────
    for (label, query) in [
        (
            "get_meeting_transcript",
            "SELECT graph_id FROM meeting_transcripts WHERE graph_id = $1 AND source_id = $2",
        ),
        (
            "best_effort_summary (the removed raw query)",
            "SELECT graph_id FROM meeting_transcripts \
             WHERE graph_id = $1 AND source_id = $2 AND summary_status = 'done'",
        ),
        (
            "meeting_occurred_at",
            "SELECT graph_id FROM atoms WHERE graph_id = $1 AND source_id LIKE $2 || '%'",
        ),
    ] {
        let mut tx = probe
            .app_pool
            .begin()
            .await
            .expect("begin unscoped tx as coralswarm_app");
        let err = sqlx::query_scalar::<_, Uuid>(query)
            .bind(probe.a.graph_id)
            .bind(&a.source_id)
            .fetch_all(&mut *tx)
            .await
            .err()
            .unwrap_or_else(|| {
                panic!(
                    "{label}: an UNSCOPED read must hard-error as coralswarm_app. If this ever \
                     passes, 0083's policy stopped failing closed and every assertion in this \
                     file is worthless."
                )
            });
        let sqlstate = err
            .as_database_error()
            .and_then(|d| d.code())
            .map(|c| c.to_string());
        assert!(
            matches!(sqlstate.as_deref(), Some("42704") | Some("22P02")),
            "{label}: an RLS scope failure raises 42704 (unrecognized configuration \
             parameter) or 22P02 (invalid uuid input syntax), got {sqlstate:?} ({err}) — \
             the removed classifier only ever matched 42703/42P01"
        );
        tx.rollback().await.ok();
    }

    // ── 2. The checker rejects the payload those errors produced ─────────────
    let broken = json!({
        "title": null, "occurred_at": null, "duration_ms": null,
        "summary": null, "transcript": null, "visibility": "link",
    });
    let rejection = check_payload_fully_resolved(&broken, &a, "link")
        .expect_err(
            "check_payload_fully_resolved MUST reject the all-null payload the pre-fix code \
             returned — otherwise the scoped builder proof would pass against the bug",
        );
    assert!(
        rejection.contains("ALL NULL"),
        "the rejection must name the all-null failure mode explicitly, got {rejection:?}"
    );

    // ── 3. Scoped to the owner, the same rows are there all along ────────────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT graph_id FROM meeting_transcripts \
             WHERE graph_id = $1 AND summary_status = 'done'",
            &[probe.a.graph_id],
            &[probe.a.graph_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT graph_id FROM meeting_transcripts WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;

    probe.teardown().await;
}
