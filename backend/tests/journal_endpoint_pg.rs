//! Route-level tests for `GET /v1/journal` (Today-journal PR C), against the
//! REAL production router (`test_support_app`) served entirely off a
//! `coralswarm_app` login pool — same harness shape as `route_rls_b1_pg.rs` /
//! `routes_b5b_rls_pg.rs`: real Clerk-shaped JWTs against a local JWKS
//! listener, the `Tenant` extractor resolving `begin_member_scoped` exactly
//! as it does in production, so an RLS gap or a still-unscoped query in
//! `routes::journal`/`Store::journal_range_aggregate` would hard-error or
//! leak here instead of only in a hand-rolled probe.
//!
//! Covers:
//!   1. Happy path — a rollup-only hour, a rollup+aggregate hour, and an
//!      aggregate-only hour, all in one range, in ascending order.
//!   2. Span validation — >48h, from>=to, malformed timestamps → 400.
//!   3. CROSS-ACCOUNT isolation — two independently-owned personal accounts
//!      (the only "two different humans hit `GET /v1/journal`" shape a
//!      non-org Clerk token can reach in this codebase — a personal token's
//!      `account_id` always resolves via `ensure_personal_account`'s
//!      owner-lookup, never via `account_members`, so there is no HTTP-
//!      reachable "two personal-token humans share one account" case to
//!      probe here). Both accounts' rollup/atom/ask data is identifiable and
//!      seeded via the BYPASSRLS owner pool (indisputably present), and the
//!      assertions check it is specifically ABSENT from the other caller's
//!      response — not just "the list happens to be short". The stricter,
//!      SAME-account per-MEMBER boundary (`journal_rollups`'s own product-
//!      level `user_id` filter, independent of the `account_id` RLS
//!      backstop) is proven at the `Store` layer in `journal_rollups_pg.rs`
//!      (test 5), the same shape its own `list_journal_rollups` RLS proof
//!      already uses.
//!   4. Graph filtering — an atom authored by the caller but sitting in a
//!      graph owned by a DIFFERENT account (unreachable) contributes nothing.
//!   5. `journal_hour_items` (the hour drill-down, ticket 16) SAME-ACCOUNT
//!      second-member isolation — a real `account_members` row (an invited-
//!      teammate shape) gives a second user reachability into the SAME
//!      account/ocean as the first, and the probe calls the REAL
//!      `journal_hour_items` handler directly (re-exported at the crate root
//!      for this purpose) with a hand-built `UserContext` for that second
//!      user, since — per test 3's comment above — a plain personal Clerk
//!      token can never itself produce "two different users, one
//!      `account_id`" for `test_support_app`'s JWT-minting harness to route
//!      through. Non-vacuous: asserts the second member sees their OWN item
//!      in the same hour (not merely that the first member's item is
//!      absent), so an accidentally-empty result can't fake a pass — the
//!      vacuous-probe lesson from the member-private-oceans work.
//!   6. Ticket 19 — the day-tier read path resolves the user's stored
//!      timezone when reading retained rollups. A retained Tokyo day row is
//!      seeded through the real Store after proving the automatic sweep is
//!      terminal at managed admission (idle, no model call, no writes).
//!      The real authenticated endpoint must return that exact Tokyo
//!      period_start/period_end, summary and timezone, with a closed period
//!      and a real compiled_at. This covers retained output reads; it does
//!      not claim the blocked automatic sweep compiled the fixture.
//!
//! Env-gated on `ORGT_TEST_BASE_URL`, like every other `*_pg.rs` test here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test journal_endpoint_pg

mod support;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use axum::Router;
use chrono::{TimeZone, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::llm::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::journal_sweep::sweep_pass;
use coralswarm_backend::workers::tenant_harness::{Tick, WorkerTenant};
use coralswarm_backend::{journal_hour_items, AppState, JournalHourItemsQueryParams, UserContext};
use support::rls_probe::RlsProbe;

/// Serializes the shared-process `CLERK_*` env mutations, same pattern as
/// `route_rls_b1_pg.rs`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

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

struct EnvGuard;

impl EnvGuard {
    fn set(jwks_url: &str) -> Self {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", jwks_url);
        EnvGuard
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

fn get(uri: &str, token: &str) -> HttpRequest<Body> {
    HttpRequest::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

/// The full production router over the harness's `coralswarm_app` pool —
/// byte-identical construction to `route_rls_b1_pg.rs::app_over`.
fn app_over(probe: &RlsProbe) -> Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// One authenticated caller: a fresh user + personal account + Clerk-minted
/// token, PLUS a graph. Deliberately does not reuse `RlsProbe`'s own
/// `AccountFixture`s (which have no `account_members` row — required by the
/// D9 `resolve_reachable_graphs` bootstrap this route depends on).
struct Caller {
    user_id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
    token: String,
}

async fn seed_caller(owner: &PgPool, tag: &str) -> Caller {
    let sub = format!("journal_ep_{tag}_{}", Uuid::new_v4().simple());
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(format!("{sub}@journal-ep.example"))
    .bind(&sub)
    .fetch_one(owner)
    .await
    .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .expect("insert account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(owner)
        .await
        .expect("insert membership");
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name) VALUES ($1, $2) RETURNING id",
    )
    .bind(account_id)
    .bind(format!("{tag}-ocean"))
    .fetch_one(owner)
    .await
    .expect("insert graph");
    let token = mint(json!({
        "iss": ISSUER,
        "sub": sub,
        "exp": future_exp(),
    }));
    Caller { user_id, account_id, graph_id, token }
}

fn hour(y: i32, mo: u32, d: u32, h: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, 0, 0).unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn seed_atom(
    owner: &PgPool,
    graph_id: Uuid,
    ingested_by: Uuid,
    source_type: &str,
    source_id: &str,
    timestamp: chrono::DateTime<Utc>,
    title: Option<&str>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \
                            ingested_by, \"timestamp\", title) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_type)
    .bind(source_id)
    .bind(format!("blob-{source_id}"))
    .bind(ingested_by)
    .bind(timestamp)
    .bind(title)
    .fetch_one(owner)
    .await
    .expect("insert atom")
}

async fn seed_coding_session(owner: &PgPool, graph_id: Uuid, session_id: &str, title: &str) {
    sqlx::query(
        "INSERT INTO coding_sessions(graph_id, session_id, title) VALUES ($1, $2, $3)",
    )
    .bind(graph_id)
    .bind(session_id)
    .bind(title)
    .execute(owner)
    .await
    .expect("insert coding session");
}

async fn seed_ask(
    owner: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    user_id: Uuid,
    query_text: &str,
    created_at: chrono::DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO query_history(account_id, graph_id, user_id, query_text, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(user_id)
    .bind(query_text)
    .bind(created_at)
    .execute(owner)
    .await
    .expect("insert query_history");
}

async fn seed_activity_event(
    owner: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    user_id: Uuid,
    event_type: &str,
    payload: Value,
    created_at: chrono::DateTime<Utc>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO activity_events(account_id, user_id, graph_id, event_type, payload, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(graph_id)
    .bind(event_type)
    .bind(payload)
    .bind(created_at)
    .fetch_one(owner)
    .await
    .expect("insert activity_events")
}

/// The `hours[]` entry whose `hour_start` matches `h`, or panics with the
/// full body for debuggability.
fn hour_entry<'a>(body: &'a Value, h: chrono::DateTime<Utc>) -> &'a Value {
    body["hours"]
        .as_array()
        .expect("hours must be an array")
        .iter()
        .find(|entry| entry["hour_start"] == json!(h))
        .unwrap_or_else(|| panic!("hour {h} missing from response: {body}"))
}

// ── 1. Happy path: rollup-only, rollup+aggregate, aggregate-only, ordering ──

#[tokio::test]
async fn happy_path_renders_rollup_only_mixed_and_aggregate_only_hours_in_order() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_ep_happy").await else {
        eprintln!("skipping happy_path_renders_rollup_only_mixed_and_aggregate_only_hours_in_order: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "happy").await;
    let store = PostgresStore::new(probe.owner.clone());

    let h10 = hour(2026, 8, 1, 10);
    let h11 = hour(2026, 8, 1, 11);
    let h12 = hour(2026, 8, 1, 12);

    // Hour 10: ROLLUP ONLY — compiled prose, no matching live atoms (e.g. the
    // atoms have since aged out, or this predates PR C).
    store
        .upsert_journal_rollup(
            a.account_id,
            a.user_id,
            a.graph_id,
            h10,
            "Reviewed the ingest pipeline PR.",
            json!({"atom_count": 4}),
            "fp-h10",
            "claude-haiku-4-5",
        )
        .await
        .expect("upsert h10 rollup");

    // Hour 11: BOTH — a rollup AND live inputs (a coding-session atom whose
    // block title comes from `coding_sessions.title`, an untitled note atom
    // that falls back to the source_type label, and one ask).
    store
        .upsert_journal_rollup(
            a.account_id,
            a.user_id,
            a.graph_id,
            h11,
            "Shipped the journal endpoint.",
            json!({"atom_count": 2}),
            "fp-h11",
            "claude-haiku-4-5",
        )
        .await
        .expect("upsert h11 rollup");
    seed_coding_session(&probe.owner, a.graph_id, "sess-h11", "My Coding Session").await;
    seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "coding_session",
        "cc:sess-h11:ckpt-1",
        h11 + chrono::Duration::minutes(5),
        None,
    )
    .await;
    seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "note",
        "note-h11",
        h11 + chrono::Duration::minutes(10),
        None,
    )
    .await;
    seed_ask(
        &probe.owner,
        a.account_id,
        a.graph_id,
        a.user_id,
        "what did I ship this hour?",
        h11 + chrono::Duration::minutes(15),
    )
    .await;

    // Hour 12: AGGREGATE ONLY — atoms landed but the sweep hasn't compiled
    // this hour yet (no journal_rollups row at all).
    seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "note",
        "note-h12",
        h12 + chrono::Duration::minutes(2),
        Some("A per-coral headline"),
    )
    .await;

    let app = app_over(&probe);
    let (status,body)=support::read_models::stored_call(&app,&probe.app_pool,"/v1/journal?from=2026-08-01T10:00:00Z&to=2026-08-01T13:00:00Z",&a.token).await;
    assert_eq!(status,StatusCode::OK);

    let hours = body["hours"].as_array().unwrap();
    assert_eq!(hours.len(), 3, "exactly the three hours with data: {body}");
    let starts: Vec<&str> = hours.iter().map(|h| h["hour_start"].as_str().unwrap()).collect();
    assert_eq!(
        starts,
        vec!["2026-08-01T10:00:00Z", "2026-08-01T11:00:00Z", "2026-08-01T12:00:00Z"],
        "hours must be ascending by hour_start: {body}"
    );

    // Hour 10: rollup-only — entries present, aggregate is the always-present
    // zero fallback.
    let e10 = hour_entry(&body, h10);
    assert_eq!(e10["entries"].as_array().unwrap().len(), 1);
    assert_eq!(e10["entries"][0]["summary"], json!("Reviewed the ingest pipeline PR."));
    assert_eq!(e10["entries"][0]["graph_id"], json!(a.graph_id));
    assert_eq!(e10["entries"][0]["graph_name"], json!("happy-ocean"));
    assert_eq!(e10["aggregate"]["atom_count"], json!(0));
    assert_eq!(e10["aggregate"]["asks"], json!(0));
    assert_eq!(e10["aggregate"]["blocks"], json!([]));

    // Hour 11: entries AND a real aggregate coexist.
    let e11 = hour_entry(&body, h11);
    assert_eq!(e11["entries"][0]["summary"], json!("Shipped the journal endpoint."));
    assert_eq!(e11["aggregate"]["atom_count"], json!(2));
    assert_eq!(e11["aggregate"]["asks"], json!(1));
    let blocks11 = e11["aggregate"]["blocks"].as_array().unwrap();
    assert_eq!(blocks11.len(), 2, "one coding_session block + one note block: {e11}");
    let coding_block = blocks11.iter().find(|b| b["source_type"] == "coding_session").unwrap();
    assert_eq!(coding_block["title"], json!("My Coding Session"));
    assert_eq!(coding_block["atom_count"], json!(1));
    let note_block = blocks11.iter().find(|b| b["source_type"] == "note").unwrap();
    assert_eq!(note_block["title"], json!("Note"), "untitled note falls back to the source_type label");
    assert_eq!(note_block["atom_count"], json!(1));

    // Hour 12: aggregate-only — no entries, a real aggregate whose block
    // title comes from the atom's own per-coral headline.
    let e12 = hour_entry(&body, h12);
    assert_eq!(e12["entries"], json!([]));
    assert_eq!(e12["aggregate"]["atom_count"], json!(1));
    let blocks12 = e12["aggregate"]["blocks"].as_array().unwrap();
    assert_eq!(blocks12.len(), 1);
    assert_eq!(blocks12[0]["title"], json!("A per-coral headline"));

    probe.teardown().await;
}

// ── 2. Span validation ──────────────────────────────────────────────────────

#[tokio::test]
async fn span_validation_rejects_over_48h_backwards_and_malformed_ranges() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_ep_validation").await else {
        eprintln!("skipping span_validation_rejects_over_48h_backwards_and_malformed_ranges: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "validation").await;
    let app = app_over(&probe);

    let cases = [
        ("/v1/journal?from=2026-08-01T00:00:00Z&to=2026-08-03T00:00:01Z", "over 48h"),
        ("/v1/journal?from=2026-08-01T12:00:00Z&to=2026-08-01T00:00:00Z", "from after to"),
        ("/v1/journal?from=2026-08-01T00:00:00Z&to=2026-08-01T00:00:00Z", "from equals to"),
        ("/v1/journal?from=not-a-timestamp&to=2026-08-01T12:00:00Z", "malformed from"),
        ("/v1/journal?from=2026-08-01T00:00:00Z&to=not-a-timestamp", "malformed to"),
        ("/v1/journal?to=2026-08-01T12:00:00Z", "missing from"),
        ("/v1/journal?from=2026-08-01T00:00:00Z", "missing to"),
    ];
    for (uri, label) in cases {
        let resp = app.clone().oneshot(get(uri, &a.token)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{label}: {uri}");
        let body = body_json(resp).await;
        assert!(body["error"].is_string(), "{label} must carry a clear error message: {body}");
    }

    // Sanity: a valid, in-range request from the same caller still succeeds.
    let resp = app
        .oneshot(get("/v1/journal?from=2026-08-01T00:00:00Z&to=2026-08-01T01:00:00Z", &a.token))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    probe.teardown().await;
}

// ── 3. Cross-account isolation ──────────────────────────────────────────────

#[tokio::test]
async fn cross_account_isolation_never_leaks_another_accounts_journal() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_ep_cross_account").await else {
        eprintln!("skipping cross_account_isolation_never_leaks_another_accounts_journal: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "cross_acct_a").await;
    let b = seed_caller(&probe.owner, "cross_acct_b").await; // a DIFFERENT, unrelated account
    let store = PostgresStore::new(probe.owner.clone());

    let h = hour(2026, 8, 1, 9);

    // A's own data, identifiable.
    store
        .upsert_journal_rollup(
            a.account_id,
            a.user_id,
            a.graph_id,
            h,
            "A's own private summary text.",
            json!({}),
            "fp-a",
            "m",
        )
        .await
        .unwrap();
    seed_atom(&probe.owner, a.graph_id, a.user_id, "note", "a-note", h + chrono::Duration::minutes(1), None).await;
    seed_ask(&probe.owner, a.account_id, a.graph_id, a.user_id, "a's own ask", h + chrono::Duration::minutes(2)).await;

    // B's data — a completely separate account/ocean, indisputably present
    // (seeded via the BYPASSRLS owner pool) — the negative probe's real
    // target.
    store
        .upsert_journal_rollup(
            b.account_id,
            b.user_id,
            b.graph_id,
            h,
            "B's own private summary text — must never reach A.",
            json!({}),
            "fp-b",
            "m",
        )
        .await
        .unwrap();
    seed_atom(&probe.owner, b.graph_id, b.user_id, "note", "b-note-1", h + chrono::Duration::minutes(3), None).await;
    seed_atom(&probe.owner, b.graph_id, b.user_id, "note", "b-note-2", h + chrono::Duration::minutes(4), None).await;
    seed_ask(&probe.owner, b.account_id, b.graph_id, b.user_id, "b's own ask", h + chrono::Duration::minutes(5)).await;

    let app = app_over(&probe);

    // ── As A: must see exactly A's own entry/aggregate, never B's.
    let (status,body)=support::read_models::stored_call(&app,&probe.app_pool,"/v1/journal?from=2026-08-01T09:00:00Z&to=2026-08-01T10:00:00Z",&a.token).await;
    assert_eq!(status,StatusCode::OK);
    let e = hour_entry(&body, h);
    let entries = e["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "A must see exactly one entry (their own), got: {e}");
    assert_eq!(entries[0]["summary"], json!("A's own private summary text."));
    assert!(
        entries.iter().all(|entry| entry["summary"] != json!("B's own private summary text — must never reach A.")),
        "B's rollup summary must never appear in A's response: {e}"
    );
    assert_eq!(e["aggregate"]["atom_count"], json!(1), "A's aggregate must count only A's own atom, not B's two: {e}");
    assert_eq!(e["aggregate"]["asks"], json!(1), "A's asks must count only A's own ask, not B's: {e}");

    // ── As B: must see exactly B's own entry/aggregate, never A's.
    let (status,body)=support::read_models::stored_call(&app,&probe.app_pool,"/v1/journal?from=2026-08-01T09:00:00Z&to=2026-08-01T10:00:00Z",&b.token).await;
    assert_eq!(status,StatusCode::OK);
    let e = hour_entry(&body, h);
    let entries = e["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "B must see exactly one entry (their own), got: {e}");
    assert_eq!(entries[0]["summary"], json!("B's own private summary text — must never reach A."));
    assert!(
        entries.iter().all(|entry| entry["summary"] != json!("A's own private summary text.")),
        "A's rollup summary must never appear in B's response: {e}"
    );
    assert_eq!(e["aggregate"]["atom_count"], json!(2), "B's aggregate must count only B's own two atoms, not A's one: {e}");
    assert_eq!(e["aggregate"]["asks"], json!(1), "B's asks must count only B's own ask, not A's: {e}");

    // Sanity: both rollups genuinely exist in the table (proves the
    // negative results above are RLS/scoping actually working, not an empty
    // table).
    let both_exist: i64 =
        sqlx::query_scalar("SELECT count(*) FROM journal_rollups WHERE graph_id = ANY($1)")
            .bind([a.graph_id, b.graph_id])
            .fetch_one(&probe.owner)
            .await
            .unwrap();
    assert_eq!(both_exist, 2, "both A's and B's rollups must genuinely exist in the table");

    probe.teardown().await;
}

// ── 4. Graph filtering: an unreachable graph contributes nothing ───────────

#[tokio::test]
async fn unreachable_graph_contributes_nothing_even_when_authored_by_the_caller() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_ep_graph_filter").await else {
        eprintln!("skipping unreachable_graph_contributes_nothing_even_when_authored_by_the_caller: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "graph_filter_a").await;
    let other = seed_caller(&probe.owner, "graph_filter_other").await; // a DIFFERENT account entirely

    let h = hour(2026, 8, 1, 14);

    // A's own reachable graph: one atom, must appear.
    seed_atom(&probe.owner, a.graph_id, a.user_id, "note", "reachable-note", h + chrono::Duration::minutes(1), None)
        .await;

    // A graph belonging to a DIFFERENT account, with an atom whose
    // `ingested_by` is nonetheless A's user_id (seeded directly via the
    // BYPASSRLS owner pool — this could only happen via a data anomaly, but
    // it is exactly the case this test must prove is still excluded): A does
    // not have an `account_members` row on `other`'s account, so this graph
    // is unreachable to A regardless of authorship.
    seed_atom(
        &probe.owner,
        other.graph_id,
        a.user_id,
        "note",
        "unreachable-note",
        h + chrono::Duration::minutes(2),
        None,
    )
    .await;

    let app = app_over(&probe);
    let (status,body)=support::read_models::stored_call(&app,&probe.app_pool,"/v1/journal?from=2026-08-01T14:00:00Z&to=2026-08-01T15:00:00Z",&a.token).await;
    assert_eq!(status,StatusCode::OK);
    let e = hour_entry(&body, h);
    assert_eq!(
        e["aggregate"]["atom_count"],
        json!(1),
        "must count only the atom in A's REACHABLE graph, not the one in the unreachable graph: {e}"
    );

    probe.teardown().await;
}

// ── GET /v1/journal/hour/{hour_start}/items (ticket 16) ────────────────────
//
// Route-level tests for the uncapped hour drill-down endpoint, same harness
// shape as the `GET /v1/journal` suite above.

/// Minimal query-value percent-encoding — the cursor's RFC3339 half can
/// contain `:`/`+` (a UTC offset renders as `+00:00`, not `Z`, from
/// `DateTime::to_rfc3339`), and axum's `Query` extractor decodes `+` in a
/// query VALUE as a literal space (`application/x-www-form-urlencoded`
/// semantics), which would silently corrupt an unencoded cursor's offset
/// before `decode_hour_items_cursor` ever sees it. Encodes every byte outside
/// the unreserved RFC 3986 set.
fn percent_encode_query_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn hour_items_uri(hour_start: chrono::DateTime<Utc>, query: &str) -> String {
    // Explicit `Z` formatting (not `DateTime::to_rfc3339`, which renders a
    // UTC offset as `+00:00`) so the path segment never contains a literal
    // `+` — irrelevant to URI path parsing itself, but keeps this helper
    // trivially unambiguous rather than depending on chrono's offset
    // rendering choice.
    let base = format!("/v1/journal/hour/{}/items", hour_start.format("%Y-%m-%dT%H:%M:%SZ"));
    if query.is_empty() {
        base
    } else {
        format!("{base}?{query}")
    }
}

/// Every `(kind, id)` pair currently present in one page's `atoms`/
/// `activity`/`asks` arrays — used to prove a paginated walk returns every
/// item exactly once, never a duplicate or a gap.
fn page_item_keys(body: &Value) -> Vec<(String, String)> {
    let mut keys = Vec::new();
    for kind in ["atoms", "activity", "asks"] {
        for item in body[kind].as_array().expect("array") {
            keys.push((kind.to_string(), item["id"].as_str().expect("id is a string").to_string()));
        }
    }
    keys
}

// ── 5. Pagination across a busy hour + per-kind totals ──────────────────────

#[tokio::test]
async fn hour_items_paginates_a_busy_hour_and_reports_stable_per_kind_totals() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_hi_busy").await else {
        eprintln!("skipping hour_items_paginates_a_busy_hour_and_reports_stable_per_kind_totals: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "busy").await;
    let h = hour(2026, 8, 1, 10);

    // 5 atoms, 2 activity_events, 3 asks — 10 items total, deliberately NOT
    // a multiple of the page size below so the last page is a genuine
    // partial page.
    for i in 0..5u32 {
        seed_atom(
            &probe.owner,
            a.graph_id,
            a.user_id,
            "note",
            &format!("busy-note-{i}"),
            h + chrono::Duration::minutes(i as i64),
            Some(&format!("Note {i}")),
        )
        .await;
    }
    for i in 0..2u32 {
        seed_activity_event(
            &probe.owner,
            a.account_id,
            a.graph_id,
            a.user_id,
            "atom_ingested",
            json!({"atom_count": 1, "source_type": "note"}),
            h + chrono::Duration::minutes(20 + i as i64),
        )
        .await;
    }
    for i in 0..3u32 {
        seed_ask(
            &probe.owner,
            a.account_id,
            a.graph_id,
            a.user_id,
            &format!("busy question {i}"),
            h + chrono::Duration::minutes(40 + i as i64),
        )
        .await;
    }

    let app = app_over(&probe);

    // Walk every page with a small limit, collecting every item's (kind, id).
    let mut all_keys: Vec<(String, String)> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= 20, "runaway pagination loop — has_more/next_cursor must terminate");
        let query = match &cursor {
            Some(c) => format!("limit=3&cursor={c}", c = percent_encode_query_value(c)),
            None => "limit=3".to_string(),
        };
        let resp = app.clone().oneshot(get(&hour_items_uri(h, &query), &a.token)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;

        // Totals are the FULL hour's counts on every page, not the page
        // slice size — the whole point of a separate totals query.
        assert_eq!(body["totals"]["atoms"], json!(5), "page {pages}: {body}");
        assert_eq!(body["totals"]["activity"], json!(2), "page {pages}: {body}");
        assert_eq!(body["totals"]["asks"], json!(3), "page {pages}: {body}");

        all_keys.extend(page_item_keys(&body));

        if body["has_more"] == json!(false) {
            assert_eq!(body["next_cursor"], Value::Null);
            break;
        }
        cursor = Some(body["next_cursor"].as_str().expect("next_cursor is a string when has_more").to_string());
    }

    assert_eq!(pages, 4, "10 items at 3/page must take exactly 4 pages (3+3+3+1)");
    assert_eq!(all_keys.len(), 10, "every item must appear exactly once across the full walk: {all_keys:?}");
    let unique: std::collections::HashSet<_> = all_keys.iter().collect();
    assert_eq!(unique.len(), 10, "no item may repeat across pages: {all_keys:?}");

    probe.teardown().await;
}

// ── 5b. Group label uses the FULL colon-bearing session_id, never a
// split_part-style first-colon truncation ──────────────────────────────────

#[tokio::test]
async fn hour_items_atom_group_label_resolves_a_colon_bearing_session_id_via_the_full_session_row() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_hi_colon_session").await else {
        eprintln!("skipping hour_items_atom_group_label_resolves_a_colon_bearing_session_id_via_the_full_session_row: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "colon_session").await;
    let h = hour(2026, 8, 1, 13);

    // A session_id that itself contains colons — `split_part(source_id, ':',
    // 2)` would truncate this to just "team" instead of the full
    // "team:alpha:project". The escaped-LIKE join
    // (`Store::journal_hour_items`, mirroring `journal_range_aggregate`)
    // must still match the atom to this exact session row.
    let session_id = "team:alpha:project";
    seed_coding_session(&probe.owner, a.graph_id, session_id, "Team Alpha Project").await;
    seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "coding_session",
        &format!("cc:{session_id}:ckpt-1"),
        h + chrono::Duration::minutes(5),
        None,
    )
    .await;

    let app = app_over(&probe);
    let resp = app.oneshot(get(&hour_items_uri(h, ""), &a.token)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    let atoms = body["atoms"].as_array().unwrap();
    assert_eq!(atoms.len(), 1, "{body}");
    assert_eq!(atoms[0]["source_type"], json!("coding_session"));
    assert_eq!(
        atoms[0]["group"],
        json!("Team Alpha Project"),
        "the group label must come from the session ROW matched via the full colon-bearing \
         session_id, not a split_part truncation: {body}"
    );
    // Titles-always-present (independent review finding M3): this atom was
    // seeded with `title: None` and no `excerpt`/`content_inline`, so
    // `display_title` must fall all the way to its last rung
    // (`"{source label} · {date}"`) rather than leave `title` null/absent —
    // `group` is a SEPARATE session-aware field and must never be the only
    // non-empty label on the wire.
    let title = atoms[0]["title"].as_str().unwrap_or_else(|| panic!("title must be a non-empty string: {body}"));
    assert!(!title.trim().is_empty(), "title must be non-empty: {body}");

    probe.teardown().await;
}

// ── 5c. Group label resolution is exact under LIKE metacharacters (% or _)
// in the session_id, never a wildcard-broadened match ───────────────────────

#[tokio::test]
async fn hour_items_atom_group_label_escapes_like_metacharacters_in_the_session_id() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_hi_like_meta").await else {
        eprintln!("skipping hour_items_atom_group_label_escapes_like_metacharacters_in_the_session_id: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "like_meta").await;
    let h = hour(2026, 8, 1, 15);

    // `_` is a "match any single character" LIKE wildcard when unescaped —
    // an atom whose source_id differs from this session_id by exactly one
    // character in that position (`teamXalpha` vs `team_alpha`) would
    // wrongly match if the join's escaping were broken.
    seed_coding_session(&probe.owner, a.graph_id, "team_alpha", "Team Underscore").await;
    let underscore_exact = seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "coding_session",
        "cc:team_alpha:ckpt-1",
        h + chrono::Duration::minutes(1),
        None,
    )
    .await;
    let underscore_decoy = seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "coding_session",
        "cc:teamXalpha:ckpt-1", // NOT a real session_id — must not match "team_alpha"
        h + chrono::Duration::minutes(2),
        None,
    )
    .await;

    // `%` is a "match zero or more characters" LIKE wildcard when unescaped
    // — an atom whose source_id extends past this session_id would wrongly
    // match if the join's escaping were broken.
    seed_coding_session(&probe.owner, a.graph_id, "sprint50%", "Sprint Percent").await;
    let percent_exact = seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "coding_session",
        "cc:sprint50%:ckpt-1",
        h + chrono::Duration::minutes(3),
        None,
    )
    .await;
    let percent_decoy = seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "coding_session",
        "cc:sprint50XYZ:ckpt-1", // NOT a real session_id — must not match "sprint50%"
        h + chrono::Duration::minutes(4),
        None,
    )
    .await;

    let app = app_over(&probe);
    let resp = app.oneshot(get(&hour_items_uri(h, ""), &a.token)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    let atoms = body["atoms"].as_array().unwrap();
    assert_eq!(atoms.len(), 4, "{body}");
    let group_of = |id: Uuid| -> Value {
        atoms
            .iter()
            .find(|a| a["id"] == json!(id))
            .unwrap_or_else(|| panic!("atom {id} missing from response: {body}"))["group"]
            .clone()
    };

    assert_eq!(
        group_of(underscore_exact),
        json!("Team Underscore"),
        "the exact underscore-bearing session_id must still resolve normally: {body}"
    );
    assert_ne!(
        group_of(underscore_decoy),
        json!("Team Underscore"),
        "an unescaped `_` wildcard must never let a one-character-different source_id \
         match \"team_alpha\": {body}"
    );

    assert_eq!(
        group_of(percent_exact),
        json!("Sprint Percent"),
        "the exact percent-bearing session_id must still resolve normally: {body}"
    );
    assert_ne!(
        group_of(percent_decoy),
        json!("Sprint Percent"),
        "an unescaped `%` wildcard must never let a longer source_id match \"sprint50%\": {body}"
    );

    probe.teardown().await;
}

// ── 5d. Pagination keyset tiebreak: identical occurred_at values split
// across a page boundary ────────────────────────────────────────────────────

#[tokio::test]
async fn hour_items_pagination_tiebreaks_identical_occurred_at_by_id() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_hi_tiebreak").await else {
        eprintln!("skipping hour_items_pagination_tiebreaks_identical_occurred_at_by_id: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "tiebreak").await;
    let h = hour(2026, 8, 1, 16);
    // Every atom shares the EXACT SAME `occurred_at` — a page boundary
    // landing inside this tied-timestamp group is the one shape a naive
    // `occurred_at > cursor` (no `id` tiebreak) pagination would either skip
    // or duplicate.
    let tied_at = h + chrono::Duration::minutes(30);

    let mut seeded_ids: Vec<Uuid> = Vec::new();
    for i in 0..4u32 {
        let id = seed_atom(
            &probe.owner,
            a.graph_id,
            a.user_id,
            "note",
            &format!("tied-note-{i}"),
            tied_at,
            Some(&format!("Tied note {i}")),
        )
        .await;
        seeded_ids.push(id);
    }

    let app = app_over(&probe);

    // limit=2 guarantees the boundary falls in the middle of the 4-way tie.
    let mut walked_ids: Vec<Uuid> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= 10, "runaway pagination loop");
        let query = match &cursor {
            Some(c) => format!("limit=2&cursor={c}", c = percent_encode_query_value(c)),
            None => "limit=2".to_string(),
        };
        let resp = app.clone().oneshot(get(&hour_items_uri(h, &query), &a.token)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;

        for item in body["atoms"].as_array().unwrap() {
            walked_ids.push(Uuid::parse_str(item["id"].as_str().unwrap()).unwrap());
        }

        if body["has_more"] == json!(false) {
            assert_eq!(body["next_cursor"], Value::Null);
            break;
        }
        cursor = Some(body["next_cursor"].as_str().expect("next_cursor is a string when has_more").to_string());
    }

    assert_eq!(pages, 2, "4 tied items at 2/page must take exactly 2 pages");
    assert_eq!(walked_ids.len(), 4, "every tied item must appear exactly once: {walked_ids:?}");
    let unique: std::collections::HashSet<_> = walked_ids.iter().collect();
    assert_eq!(unique.len(), 4, "no tied item may repeat across the page boundary: {walked_ids:?}");

    // The `(occurred_at, id)` keyset orders ties by `id` ascending — walking
    // the pages in order must reproduce that exact ascending-id sequence.
    let mut expected = seeded_ids.clone();
    expected.sort();
    assert_eq!(walked_ids, expected, "tied items must be walked in ascending-id order: {walked_ids:?}");

    probe.teardown().await;
}

// ── 6. RLS isolation: member A cannot read member B's items ────────────────

#[tokio::test]
async fn hour_items_cross_account_isolation_never_leaks_another_accounts_items() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_hi_cross_acct").await else {
        eprintln!("skipping hour_items_cross_account_isolation_never_leaks_another_accounts_items: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "hi_cross_a").await;
    let b = seed_caller(&probe.owner, "hi_cross_b").await; // a DIFFERENT, unrelated account
    let h = hour(2026, 8, 1, 11);

    let a_atom = seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "note",
        "a-note",
        h + chrono::Duration::minutes(1),
        Some("A's own note"),
    )
    .await;
    seed_activity_event(
        &probe.owner,
        a.account_id,
        a.graph_id,
        a.user_id,
        "atom_ingested",
        json!({"atom_count": 1, "source_type": "note"}),
        h + chrono::Duration::minutes(2),
    )
    .await;
    seed_ask(&probe.owner, a.account_id, a.graph_id, a.user_id, "a's own question", h + chrono::Duration::minutes(3))
        .await;

    // B's data — a completely separate account/ocean, indisputably present
    // (seeded via the BYPASSRLS owner pool) — the negative probe's real
    // target.
    seed_atom(
        &probe.owner,
        b.graph_id,
        b.user_id,
        "note",
        "b-note",
        h + chrono::Duration::minutes(4),
        Some("B's own note — must never reach A"),
    )
    .await;
    seed_activity_event(
        &probe.owner,
        b.account_id,
        b.graph_id,
        b.user_id,
        "atom_ingested",
        json!({"atom_count": 1, "source_type": "note"}),
        h + chrono::Duration::minutes(5),
    )
    .await;
    seed_ask(&probe.owner, b.account_id, b.graph_id, b.user_id, "b's own question", h + chrono::Duration::minutes(6))
        .await;

    let app = app_over(&probe);

    // ── As A: must see exactly A's own three items, never B's.
    let resp = app.clone().oneshot(get(&hour_items_uri(h, ""), &a.token)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["totals"]["atoms"], json!(1), "A must see only A's own atom count: {body}");
    assert_eq!(body["totals"]["activity"], json!(1), "A must see only A's own activity count: {body}");
    assert_eq!(body["totals"]["asks"], json!(1), "A must see only A's own ask count: {body}");
    assert_eq!(body["atoms"][0]["id"], json!(a_atom));
    assert_eq!(body["atoms"][0]["excerpt"].is_null(), true, "no content_inline was seeded for this atom");
    let asks_a = body["asks"].as_array().unwrap();
    assert_eq!(asks_a[0]["question"], json!("a's own question"));
    assert!(
        asks_a.iter().all(|ask| ask["question"] != json!("b's own question")),
        "B's ask must never appear in A's response: {body}"
    );

    // ── As B: must see exactly B's own three items, never A's.
    let resp = app.oneshot(get(&hour_items_uri(h, ""), &b.token)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["totals"]["atoms"], json!(1), "B must see only B's own atom count: {body}");
    assert_eq!(body["totals"]["activity"], json!(1), "B must see only B's own activity count: {body}");
    assert_eq!(body["totals"]["asks"], json!(1), "B must see only B's own ask count: {body}");
    let asks_b = body["asks"].as_array().unwrap();
    assert_eq!(asks_b[0]["question"], json!("b's own question"));
    assert!(
        asks_b.iter().all(|ask| ask["question"] != json!("a's own question")),
        "A's ask must never appear in B's response: {body}"
    );

    probe.teardown().await;
}

// ── 7. Empty hour ────────────────────────────────────────────────────────────

#[tokio::test]
async fn hour_items_empty_hour_returns_zero_totals_and_empty_arrays() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_hi_empty").await else {
        eprintln!("skipping hour_items_empty_hour_returns_zero_totals_and_empty_arrays: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "hi_empty").await;
    // A genuinely empty hour: no rollup, no atoms, no activity, no asks —
    // exactly the "aggregate-only-tier-never-reached" / pre-epoch case ticket
    // 04's Answer says this endpoint must still serve cheaply.
    let h = hour(2020, 1, 2, 3);

    let app = app_over(&probe);
    let resp = app.oneshot(get(&hour_items_uri(h, ""), &a.token)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["hour_start"], json!(h));
    assert_eq!(body["atoms"], json!([]));
    assert_eq!(body["activity"], json!([]));
    assert_eq!(body["asks"], json!([]));
    assert_eq!(body["totals"], json!({"atoms": 0, "activity": 0, "asks": 0}));
    assert_eq!(body["has_more"], json!(false));
    assert_eq!(body["next_cursor"], Value::Null);

    probe.teardown().await;
}

// ── 8. Invalid hour_start rejection ─────────────────────────────────────────

#[tokio::test]
async fn hour_items_rejects_invalid_hour_start() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_hi_invalid").await else {
        eprintln!("skipping hour_items_rejects_invalid_hour_start: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "hi_invalid").await;
    let app = app_over(&probe);

    let cases = [
        ("/v1/journal/hour/not-a-timestamp/items", "malformed timestamp"),
        ("/v1/journal/hour/2026-08-01T10:15:00Z/items", "non-boundary minute"),
        ("/v1/journal/hour/2026-08-01T10:00:01Z/items", "non-boundary second"),
        ("/v1/journal/hour/1970-01-01T00:00:00Z/items", "below the sanity floor"),
        ("/v1/journal/hour/2999-01-01T00:00:00Z/items", "far-future hour"),
    ];
    for (uri, label) in cases {
        let resp = app.clone().oneshot(get(uri, &a.token)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{label}: {uri}");
        let body = body_json(resp).await;
        assert!(body["error"].is_string(), "{label} must carry a clear error message: {body}");
    }

    // Malformed cursor on an otherwise-valid hour → 400.
    let resp = app
        .clone()
        .oneshot(get(&hour_items_uri(hour(2026, 8, 1, 10), "cursor=not-a-real-cursor"), &a.token))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["error"], json!("malformed cursor"));

    // Sanity: a valid, in-range, on-boundary hour still succeeds.
    let resp = app.oneshot(get(&hour_items_uri(hour(2026, 8, 1, 10), ""), &a.token)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    probe.teardown().await;
}

// ── 9. SAME-ACCOUNT second-member isolation (through the endpoint) ─────────
//
// Test 3's cross-account probe is real but narrower than the product
// boundary: `journal_hour_items` must isolate by MEMBER, not merely by
// account (`journal_range_aggregate_and_list_rollups_isolate_by_member_
// within_one_account_and_across_accounts` in `journal_rollups_pg.rs` already
// proves this at the `Store` layer for the sibling read endpoints). This
// probes the SAME shape one layer up, through the actual
// `routes::journal::journal_hour_items` handler — not a re-implementation of
// its scoping logic — by calling it directly with a hand-built
// `UserContext` for a second, real `account_members` row on A's own
// account/graph. Going through `test_support_app`'s normal Clerk-JWT router
// is not possible here: a plain personal token's `account_id` always
// resolves via `ensure_personal_account`'s owner-lookup, never via
// `account_members` (test 3's own comment), so there is no bearer token that
// makes a SECOND human's request land on A's `account_id`. Calling the
// handler directly sidesteps only that token-to-account resolution step —
// every other part of the real request path (`Tenant::begin_member_scoped`,
// the real `Store` calls, the real `coralswarm_app`-authenticated pool) is
// exercised exactly as production would run it.

#[tokio::test]
async fn hour_items_same_account_second_member_cannot_read_another_members_items() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_journal_hi_second_member").await else {
        eprintln!("skipping hour_items_same_account_second_member_cannot_read_another_members_items: set ORGT_TEST_BASE_URL");
        return;
    };

    let a = seed_caller(&probe.owner, "second_member_a").await;

    // A second, real member of A's account — an `account_members` row is
    // exactly the shape an invited teammate has (mirrors
    // `journal_range_aggregate_and_list_rollups_isolate_by_member_within_
    // one_account_and_across_accounts`'s own "A2" fixture in
    // `journal_rollups_pg.rs`, one layer down). `resolve_reachable_graphs`
    // (D9) grants every account member reachability into the account's own
    // (non-team, non-member-kind) oceans, so A2 can genuinely reach A's
    // `graph_id` — the isolation this test is actually about must therefore
    // come from the endpoint's own `user_id`/`ingested_by` predicates
    // (belt-and-suspenders over `begin_member_scoped`'s RLS), not from A2
    // being unable to see the ocean at all.
    let a2_user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("second_member_a2_{}@journal-ep.example", Uuid::new_v4().simple()))
        .fetch_one(&probe.owner)
        .await
        .expect("insert a2 user");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(a.account_id)
        .bind(a2_user_id)
        .execute(&probe.owner)
        .await
        .expect("insert a2 membership");

    let h = hour(2026, 8, 1, 17);

    // A's own item — identifiable, must never reach A2.
    seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "note",
        "second-member-a-note",
        h + chrono::Duration::minutes(1),
        Some("A's own note — must never reach A2"),
    )
    .await;
    // A2's own item — SAME account, SAME graph as A, different user_id.
    let a2_atom = seed_atom(
        &probe.owner,
        a.graph_id,
        a2_user_id,
        "note",
        "second-member-a2-note",
        h + chrono::Duration::minutes(2),
        Some("A2's own note"),
    )
    .await;

    let tenant = Tenant {
        store: Some(Arc::new(PostgresStore::new(probe.app_pool.clone()))),
        pool: Some(probe.app_pool.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let a2_ctx = UserContext {
        user_id: a2_user_id,
        account_id: a.account_id,
        role: "member".to_string(),
        device_id: "test-device".to_string(),
    };

    let (status, response) = journal_hour_items(
        tenant.clone(),
        axum::Extension(a2_ctx),
        axum::extract::Path(h.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        axum::extract::Query(JournalHourItemsQueryParams { graph_id: None, cursor: None, limit: None }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = response.0;

    // Non-vacuous: A2 must see THEIR OWN item (an accidentally-empty result
    // must not be able to pass this probe — the vacuous-probe lesson).
    assert_eq!(body["totals"]["atoms"], json!(1), "A2 must see exactly A2's own atom, not A's: {body}");
    let atoms = body["atoms"].as_array().unwrap();
    assert_eq!(atoms.len(), 1, "{body}");
    assert_eq!(atoms[0]["id"], json!(a2_atom));
    assert_eq!(atoms[0]["group"], json!("A2's own note"));
    assert!(
        atoms.iter().all(|item| item["group"] != json!("A's own note — must never reach A2")),
        "A's note must never appear in A2's response: {body}"
    );

    // Symmetric check: A (the account owner) must likewise see only their
    // own item, not A2's.
    let a_ctx = UserContext {
        user_id: a.user_id,
        account_id: a.account_id,
        role: "owner".to_string(),
        device_id: "test-device".to_string(),
    };
    let (status, response) = journal_hour_items(
        tenant,
        axum::Extension(a_ctx),
        axum::extract::Path(h.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        axum::extract::Query(JournalHourItemsQueryParams { graph_id: None, cursor: None, limit: None }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = response.0;
    assert_eq!(body["totals"]["atoms"], json!(1), "A must see exactly A's own atom, not A2's: {body}");
    let atoms = body["atoms"].as_array().unwrap();
    assert_eq!(atoms.len(), 1, "{body}");
    assert_eq!(atoms[0]["group"], json!("A's own note — must never reach A2"));

    probe.teardown().await;
}

// ── 10. Ticket 19 — retained day-tier reads use the user's timezone ──────────

/// An ungated provider: the managed journal sweep must never send through it
/// (the gate refuses it before any model call). Fails immediately if either
/// provider entry point is reached.
struct PanicLlm;

#[async_trait::async_trait]
impl LlmProvider for PanicLlm {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        panic!("the managed journal sweep must never call an ungated provider")
    }

    async fn stream_complete(
        &self,
        _req: CompletionRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError> {
        panic!("the managed journal sweep must never call an ungated provider")
    }
}

/// Keep the closed historical input inside the sweep's epoch so a removed
/// admission gate cannot pass idle merely because the fixture is too old.
struct EpochGuard {
    prev: Option<String>,
}

impl EpochGuard {
    fn pin(value: &str) -> Self {
        let prev = std::env::var("JOURNAL_EPOCH").ok();
        std::env::set_var("JOURNAL_EPOCH", value);
        EpochGuard { prev }
    }
}

impl Drop for EpochGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var("JOURNAL_EPOCH", v),
            None => std::env::remove_var("JOURNAL_EPOCH"),
        }
    }
}

/// The `periods[]` entry whose `period_start` matches `p`, or panics with the
/// full body for debuggability — the tier analogue of [`hour_entry`].
fn period_entry<'a>(body: &'a Value, p: chrono::DateTime<Utc>) -> &'a Value {
    body["periods"]
        .as_array()
        .expect("periods must be an array")
        .iter()
        .find(|entry| entry["period_start"] == json!(p))
        .unwrap_or_else(|| panic!("period {p} missing from response: {body}"))
}

#[tokio::test]
async fn day_tier_read_path_resolves_the_users_timezone_for_retained_rollups() {
    let _env = ENV_LOCK.lock().await;
    // A long-past day keeps closed-period assertions independent of the
    // wall clock. Keep it inside the sweep's epoch for the admission proof.
    let _epoch_guard = EpochGuard::pin("2020-01-01T00:00:00Z");
    let Some(probe) = RlsProbe::setup("orgt_journal_ep_day_tz").await else {
        eprintln!("skipping day_tier_read_path_resolves_the_users_timezone_for_retained_rollups: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "day_tz").await;

    // `Asia/Tokyo` (UTC+9, no DST) is chosen specifically because it
    // disagrees with `JOURNAL_TZ`'s UTC fallback on which CALENDAR DAY a
    // late-UTC-evening hour falls in — exactly the divergence ticket 19's
    // Question describes. `2020-06-15T20:00:00Z` is `2020-06-16 05:00` in
    // Tokyo: the Tokyo-local day containing it is
    // `[2020-06-15T15:00:00Z, 2020-06-16T15:00:00Z)` (Tokyo midnight June 16
    // -> Tokyo midnight June 17, in UTC), while the UTC-calendar day
    // containing the SAME instant is `[2020-06-15T00:00:00Z,
    // 2020-06-16T00:00:00Z)` — a different boundary entirely. A read path
    // still resolving `JOURNAL_TZ`/UTC here would query for a rollup at the
    // WRONG `period_start` and never find the retained row.
    sqlx::query("UPDATE users SET timezone = 'Asia/Tokyo' WHERE id = $1")
        .bind(a.user_id)
        .execute(&probe.owner)
        .await
        .expect("set user timezone");

    let atom_ts = Utc.with_ymd_and_hms(2020, 6, 15, 20, 0, 0).unwrap();
    let day_start = Utc.with_ymd_and_hms(2020, 6, 15, 15, 0, 0).unwrap();
    let day_end = Utc.with_ymd_and_hms(2020, 6, 16, 15, 0, 0).unwrap();

    seed_atom(
        &probe.owner,
        a.graph_id,
        a.user_id,
        "note",
        "day-tz-note",
        atom_ts,
        Some("Wrote the day-tier timezone fix."),
    )
    .await;

    let store = PostgresStore::new(probe.owner.clone());
    let dirty = store
        .journal_dirty_hours(atom_ts, atom_ts + chrono::Duration::hours(1))
        .await
        .expect("dirty scan");
    assert!(
        dirty.iter().any(|d| d.user_id == a.user_id && d.graph_id == a.graph_id),
        "the fixture must contain a real compile candidate: {dirty:?}"
    );

    // The sweep runs on managed_enrichment (source-agnostic PR 6). Whether its
    // switch is on or off, an ungated provider is refused by the managed gate
    // before any call, so real input yields no write; never restore an
    // ungated model call merely to manufacture a retained read fixture.
    let t = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: probe.owner.clone(),
        store: Arc::new(PostgresStore::new(probe.owner.clone())),
    };
    let now = Utc.with_ymd_and_hms(2020, 6, 20, 0, 0, 0).unwrap();
    let outcome = sweep_pass(&t, &t.pool, &PanicLlm, now).await.expect("sweep_pass must succeed");
    assert_eq!(outcome, Tick::idle(), "no managed call can be sent through an ungated provider");
    let rollups: i64 = sqlx::query_scalar("SELECT count(*) FROM journal_rollups")
        .fetch_one(&probe.owner)
        .await
        .expect("count journal rollups after the sweep");
    assert_eq!(rollups, 0, "a refused managed call writes nothing");

    // Retained rows remain readable whatever the sweep did.
    // Seed through the real Store at the Tokyo boundary, then exercise the
    // unchanged authenticated read path under the normal application role.
    store
        .upsert_journal_tier_rollup(
            a.account_id,
            a.user_id,
            a.graph_id,
            "day",
            day_start,
            "Asia/Tokyo",
            "Wrote the day-tier timezone fix.",
            json!({"atom_count": 1}),
            "retained-day-tz-fingerprint",
            "retained-journal-model",
            &[],
        )
        .await
        .expect("seed retained day-tier rollup");

    // Verify the stored fixture at the exact Tokyo boundary before the
    // endpoint reads it, so an incorrectly seeded row cannot fake coverage.
    type DayRow = (String, Option<String>);
    let (stored_summary, stored_tz): DayRow = sqlx::query_as(
        "SELECT summary, tz FROM journal_rollups \
         WHERE user_id = $1 AND graph_id = $2 AND granularity = 'day' AND period_start = $3",
    )
    .bind(a.user_id)
    .bind(a.graph_id)
    .bind(day_start)
    .fetch_one(&probe.owner)
    .await
    .expect("a retained day-tier journal_rollups row must exist at the Tokyo boundary");
    assert_eq!(stored_tz.as_deref(), Some("Asia/Tokyo"));
    assert_eq!(stored_summary, "Wrote the day-tier timezone fix.");

    let app = app_over(&probe);
    let (status,body)=support::read_models::stored_call(&app,&probe.app_pool,"/v1/journal?from=2020-06-15T00:00:00Z&to=2020-06-17T00:00:00Z&granularity=day",&a.token).await;
    assert_eq!(status,StatusCode::OK);
    assert_eq!(body["granularity"], json!("day"));

    // The read path must resolve the retained row's `Asia/Tokyo` boundary
    // from users.timezone — if it had instead fallen through to
    // `JOURNAL_TZ`/UTC, the response would never contain a period whose
    // `period_start` is `day_start` (Tokyo midnight) at all; it would either
    // omit the range's data entirely or split it under a UTC-anchored
    // `period_start` that doesn't match this stored row.
    let period = period_entry(&body, day_start);
    assert_eq!(period["period_end"], json!(day_end), "period_end must be the Tokyo boundary too: {body}");
    assert_eq!(period["open"], json!(false), "a retained closed period must never read as open: {body}");
    let entries = period["entries"].as_array().expect("entries must be an array");
    assert_eq!(entries.len(), 1, "exactly the one retained entry: {body}");
    assert_eq!(entries[0]["summary"], json!("Wrote the day-tier timezone fix."));
    assert_eq!(entries[0]["tz"], json!("Asia/Tokyo"));
    assert_eq!(entries[0]["open"], json!(false));
    assert!(entries[0]["compiled_at"].is_string(), "a closed period's entry must carry a real compiled_at: {body}");

    probe.teardown().await;
}
