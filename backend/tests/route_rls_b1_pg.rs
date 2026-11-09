//! Route-level RLS conversion proofs for group B.1 (org-tenancy):
//! `routes/{queries,atoms,graphs,ingest,activity}.rs`, which this PR wires
//! onto `tenancy::scope::begin_account_scoped`.
//!
//! Built on the shared [`support::rls_probe::RlsProbe`] harness (Phase P.1):
//! a scratch DB with the full control tree (so every 0083 policy and the
//! `coralswarm_app` role are real), and — unlike the store-level `_scoped`
//! suites — the FULL production router (`test_support_app`) with
//! `state.db`/`state.store` pointed at the harness's **`coralswarm_app`
//! login pool**. Every handler query therefore runs exactly the way OPS-4
//! will run it: as the non-BYPASSRLS role, where an unscoped RLS-table query
//! HARD-ERRORS and only a correctly `begin_account_scoped` path can return
//! rows. Three proofs per converted route file:
//!
//!   1. the scoped path returns/writes only the caller's own rows;
//!   2. a cross-account attempt (account A's token against account B's ids)
//!      yields empty/denied — never B's data;
//!   3. the handler's underlying query shape hard-errors when scope is
//!      dropped (`assert_unscoped_hard_errors`), so a future refactor that
//!      loses the scoping fails loudly instead of leaking.
//!
//! Auth is real: a local JWKS listener + `clerk::test_support::mint`, same as
//! `route_migration_org_pg.rs`. The two callers are PRE-SEEDED (users /
//! personal accounts / memberships via the owner pool) rather than
//! lazily provisioned, because `clerk_provision::ensure_personal_account`'s
//! first-sight path itself writes a `graphs` row through the still-unscoped
//! `Store::create_graph` — an unconverted provisioning path outside this
//! PR's file set (flagged in the PR body) that would hard-error as
//! `coralswarm_app`.
//!
//! Env-gated like every `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test route_rls_b1_pg

mod support;

use std::sync::Arc;

use axum::body::Body;
use axum::http::Request as HttpRequest;
use axum::Router;
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::AppState;
use support::rls_probe::RlsProbe;

/// Serializes the shared-process `CLERK_*`/`MASTER_KEK_HEX` env mutations —
/// every test in this binary sets them to its own per-test values and removes
/// them at the end (same pattern as `route_migration_org_pg.rs`).
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

/// Same fixed test-only KEK hex as `mcp_pg_e2e.rs` — never a real credential.
const TEST_MASTER_KEK_HEX: &str =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair (same fixture as
/// `route_migration_org_pg.rs`, duplicated for the separate-compilation-unit
/// reason its own copy documents).
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

/// One authenticated caller: a pre-seeded Clerk-linked user + personal
/// account (+ owner membership + a wrapped DEK so ingest works), and a
/// minted personal-session token.
struct Caller {
    user_id: Uuid,
    account_id: Uuid,
    token: String,
}

async fn seed_caller(owner: &PgPool, tag: &str) -> Caller {
    let sub = format!("route_rls_b1_{tag}_{}", Uuid::new_v4().simple());
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(format!("{sub}@rls-b1.example"))
    .bind(&sub)
    .fetch_one(owner)
    .await
    .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .expect("insert personal account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(owner)
        .await
        .expect("insert membership");
    // Server-custodied DEK (interim cloud model) so `/v1/ingest/atom` can
    // resolve encryption material — wrapped under the same fixed test KEK the
    // test sets in `MASTER_KEK_HEX`.
    let kek = coralswarm_core::kek::load_kek().expect("test KEK loads from MASTER_KEK_HEX");
    let wrapped = coralswarm_core::kek::wrap_dek(&kek, &coralswarm_core::kek::generate_dek())
        .expect("wrap test DEK");
    sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
        .bind(&wrapped)
        .bind(account_id)
        .execute(owner)
        .await
        .expect("seed dek_wrapped");
    let token = mint(json!({
        "iss": ISSUER,
        "sub": sub,
        "exp": future_exp(),
    }));
    Caller { user_id, account_id, token }
}

/// The full production router over the harness's `coralswarm_app` pool.
fn app_over(probe: &RlsProbe) -> Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    coralswarm_backend::test_support_app(Arc::new(state))
}

fn get(uri: &str, token: &str) -> HttpRequest<Body> {
    HttpRequest::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn post_json(uri: &str, token: &str, body: &Value) -> HttpRequest<Body> {
    HttpRequest::builder()
        .method("POST")
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(body).unwrap()))
        .unwrap()
}

struct EnvGuard;

impl EnvGuard {
    fn set(jwks_url: &str) -> Self {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", jwks_url);
        std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
        EnvGuard
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
        std::env::remove_var("MASTER_KEK_HEX");
    }
}

/// Seed a bare atom into `graph_id` via the owner (BYPASSRLS) pool.
async fn seed_atom(owner: &PgPool, graph_id: Uuid, source_id: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', $2, 'blob-key', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(owner)
    .await
    .expect("seed atom")
}

/// Create a personal ocean through the (converted) POST /v1/graphs route and
/// return its id — this is itself part of the graphs-route proof: the INSERT
/// runs on the scoped transaction as `coralswarm_app`, so it only succeeds
/// because the conversion is in place.
async fn create_graph_via_api(app: &Router, caller: &Caller, name: &str) -> Uuid {
    let resp = app
        .clone()
        .oneshot(post_json("/v1/graphs", &caller.token, &json!({"name": name})))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::CREATED,
        "scoped create_graph as coralswarm_app must succeed"
    );
    let body = body_json(resp).await;
    Uuid::parse_str(body["id"].as_str().expect("graph id")).unwrap()
}

// ── routes/graphs.rs ─────────────────────────────────────────────────────────

#[tokio::test]
async fn graphs_routes_scoped_reads_and_writes_as_app_role() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_rls_b1_graphs").await else {
        eprintln!("skipping graphs_routes_scoped_reads_and_writes_as_app_role: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "graphs_a").await;
    let b = seed_caller(&probe.owner, "graphs_b").await;
    let app = app_over(&probe);

    // Scoped WRITE: create A's and B's oceans through the route (would
    // hard-error as coralswarm_app without begin_account_scoped).
    let ga = create_graph_via_api(&app, &a, "Ocean A").await;
    let gb = create_graph_via_api(&app, &b, "Ocean B").await;
    let owner_of_ga: Uuid = sqlx::query_scalar("SELECT account_id FROM graphs WHERE id = $1")
        .bind(ga)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(owner_of_ga, a.account_id, "created ocean lands in the caller's account");

    // Scoped READ returns own row…
    let resp = app.clone().oneshot(get(&format!("/v1/graphs/{ga}"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    assert_eq!(body_json(resp).await["name"], json!("Ocean A"));

    // …and a cross-account id yields 404, never B's data.
    let resp = app.clone().oneshot(get(&format!("/v1/graphs/{gb}"), &a.token)).await.unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "A asking for B's graph id must 404"
    );

    // /atoms + /sources: own graph lists its corals; B's graph id 404s.
    let atom_a = seed_atom(&probe.owner, ga, "src-a").await;
    let resp = app.clone().oneshot(get(&format!("/v1/graphs/{ga}/atoms"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let atoms = body_json(resp).await;
    let ids: Vec<&str> = atoms
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["atom_id"].as_str())
        .collect();
    assert_eq!(ids, vec![atom_a.to_string()], "own graph lists exactly its own corals");

    let resp = app.clone().oneshot(get(&format!("/v1/graphs/{gb}/atoms"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);

    let resp = app.clone().oneshot(get(&format!("/v1/graphs/{ga}/sources"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let sources = body_json(resp).await;
    assert_eq!(sources.as_array().map(Vec::len), Some(1), "one source group from the seeded coral");

    let resp = app.clone().oneshot(get(&format!("/v1/graphs/{gb}/sources"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);

    // /stats: `ocean_stats_scoped` (self-scoping per aggregate — PR #290)
    // works as coralswarm_app for the owning account's reachable ocean…
    let resp = app.clone().oneshot(get(&format!("/v1/graphs/{ga}/stats"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let stats = body_json(resp).await;
    assert_eq!(stats["atom_count"], json!(1), "stats count exactly the seeded coral");
    // …and an unreachable ocean is refused before any aggregate runs.
    let resp = app.clone().oneshot(get(&format!("/v1/graphs/{gb}/stats"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);

    // Scope dropped ⇒ the handlers' query shapes hard-error (never silent-empty).
    probe
        .assert_unscoped_hard_errors(
            "SELECT id FROM graphs WHERE id = $1 AND account_id = $2",
            &[ga, a.account_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT atom_id FROM atoms WHERE graph_id = $1", &[ga])
        .await;

    probe.teardown().await;
}

// ── routes/atoms.rs ──────────────────────────────────────────────────────────

#[tokio::test]
async fn atom_detail_route_scoped_to_atoms_own_account_as_app_role() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_rls_b1_atoms").await else {
        eprintln!("skipping atom_detail_route_scoped_to_atoms_own_account_as_app_role: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "atoms_a").await;
    let b = seed_caller(&probe.owner, "atoms_b").await;
    let app = app_over(&probe);

    let ga = create_graph_via_api(&app, &a, "Ocean A").await;
    let gb = create_graph_via_api(&app, &b, "Ocean B").await;
    let atom_a = seed_atom(&probe.owner, ga, "detail-a").await;
    let atom_b = seed_atom(&probe.owner, gb, "detail-b").await;

    // Scoped read: the atom's own account (bootstrap via the SECURITY DEFINER
    // resolver, then atom_graph_id_scoped/atom_detail_scoped on the tx).
    let resp = app.clone().oneshot(get(&format!("/v1/atoms/{atom_a}"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["atom_id"], json!(atom_a.to_string()));
    assert_eq!(body["graph_id"], json!(ga.to_string()));

    // Cross-account: A probing B's atom id gets 404, never B's detail.
    let resp = app.clone().oneshot(get(&format!("/v1/atoms/{atom_b}"), &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);

    // Nonexistent id: same opaque 404 (the bootstrap resolver returns nothing).
    let resp = app
        .clone()
        .oneshot(get(&format!("/v1/atoms/{}", Uuid::new_v4()), &a.token))
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);

    // Scope dropped ⇒ the handler's atom lookups hard-error.
    probe
        .assert_unscoped_hard_errors("SELECT graph_id FROM atoms WHERE atom_id = $1", &[atom_a])
        .await;

    probe.teardown().await;
}

// ── routes/ingest.rs ─────────────────────────────────────────────────────────

#[tokio::test]
async fn ingest_route_writes_inside_scoped_transaction_as_app_role() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_rls_b1_ingest").await else {
        eprintln!("skipping ingest_route_writes_inside_scoped_transaction_as_app_role: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "ingest_a").await;
    let b = seed_caller(&probe.owner, "ingest_b").await;
    let app = app_over(&probe);

    let ga = create_graph_via_api(&app, &a, "Ocean A").await;
    let gb = create_graph_via_api(&app, &b, "Ocean B").await;

    let ingest_body = |graph: Uuid| {
        json!({
            "graph_id": graph,
            "source_type": "transcript",
            "source_id": "rls-b1-transcript",
            "content": "the RLS conversion for ingest works end to end",
            "participants": ["probe"],
            "timestamp": "2026-07-23T00:00:00Z",
            "raw_content": "verbatim transcript body",
        })
    };

    // Scoped WRITE path: atoms + inline content + meeting transcript all land
    // (every statement runs on the request's begin_account_scoped tx — as
    // coralswarm_app this whole request would hard-error unconverted).
    let resp = app
        .clone()
        .oneshot(post_json("/v1/ingest/atom", &a.token, &ingest_body(ga)))
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK, "scoped ingest succeeds");
    let body = body_json(resp).await;
    let atom_ids = body["atom_ids"].as_array().expect("atom_ids array");
    assert!(!atom_ids.is_empty(), "ingest created atoms");

    let (count, inline_ok): (i64, bool) = {
        let row: (i64, i64) = sqlx::query_as(
            "SELECT count(*), count(content_inline) FROM atoms WHERE graph_id = $1",
        )
        .bind(ga)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
        (row.0, row.0 == row.1)
    };
    assert_eq!(count as usize, atom_ids.len(), "committed rows match the response");
    assert!(inline_ok, "dev-mode inline content stored for every new atom");
    let transcripts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM meeting_transcripts WHERE graph_id = $1")
            .bind(ga)
            .fetch_one(&probe.owner)
            .await
            .unwrap();
    assert_eq!(transcripts, 1, "raw transcript upserted on the same scoped tx");
    let evidence: (Uuid, String, String, String) = sqlx::query_as(
        "SELECT v.version_id,v.raw_inline,v.source_type,v.representation FROM source_versions v \
         JOIN source_items s ON s.current_version_id=v.version_id AND s.graph_id=v.graph_id \
         JOIN meeting_transcripts m ON m.current_version_id=v.version_id AND m.graph_id=v.graph_id \
         WHERE v.graph_id=$1 AND v.source_id='rls-b1-transcript'",
    ).bind(ga).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(evidence.1, "verbatim transcript body", "the committed version retains exact admitted raw text");
    assert_eq!(evidence.2, "transcript");
    assert_eq!(evidence.3, "original_text");
    let linked_atoms: Vec<Uuid> = sqlx::query_scalar(
        "SELECT atom_id FROM source_version_chunks WHERE graph_id=$1 AND source_version_id=$2",
    ).bind(ga).bind(evidence.0).fetch_all(&probe.owner).await.unwrap();
    assert_eq!(linked_atoms.len(), atom_ids.len());
    for atom in atom_ids {
        assert!(linked_atoms.contains(&Uuid::parse_str(atom.as_str().unwrap()).unwrap()),
            "every committed retrieval chunk must link to its captured source version");
    }

    // Cross-account: A ingesting into B's graph is refused (403), writes nothing.
    let resp = app
        .clone()
        .oneshot(post_json("/v1/ingest/atom", &a.token, &ingest_body(gb)))
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    let b_atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(gb)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(b_atoms, 0, "the refused cross-account ingest wrote nothing");
    let b_versions: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
        .bind(gb).fetch_one(&probe.owner).await.unwrap();
    assert_eq!(b_versions, 0, "the refused cross-account ingest must not preserve source evidence either");

    // Scope dropped ⇒ the handler's INSERT shape hard-errors (WITH CHECK).
    probe
        .assert_unscoped_hard_errors(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'probe', 'unscoped', 'k', now()) RETURNING atom_id",
            &[ga],
        )
        .await;

    probe.teardown().await;
}

// ── routes/activity.rs ───────────────────────────────────────────────────────

#[tokio::test]
async fn activity_route_scoped_read_and_scoped_event_insert_as_app_role() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_rls_b1_activity").await else {
        eprintln!("skipping activity_route_scoped_read_and_scoped_event_insert_as_app_role: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "activity_a").await;
    let b = seed_caller(&probe.owner, "activity_b").await;
    let app = app_over(&probe);

    let ev_a: Uuid = sqlx::query_scalar(
        "INSERT INTO activity_events (account_id, user_id, event_type) \
         VALUES ($1, $2, 'atom_ingested') RETURNING id",
    )
    .bind(a.account_id)
    .bind(a.user_id)
    .fetch_one(&probe.owner)
    .await
    .unwrap();
    let ev_b: Uuid = sqlx::query_scalar(
        "INSERT INTO activity_events (account_id, user_id, event_type) \
         VALUES ($1, $2, 'atom_ingested') RETURNING id",
    )
    .bind(b.account_id)
    .bind(b.user_id)
    .fetch_one(&probe.owner)
    .await
    .unwrap();

    // Scoped read: A sees exactly A's event; B's never appears.
    let resp = app.clone().oneshot(get("/v1/activity", &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let events = body_json(resp).await;
    let ids: Vec<String> = events
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["id"].as_str().map(str::to_string))
        .collect();
    assert_eq!(ids, vec![ev_a.to_string()], "A's feed is exactly A's events");
    assert!(!ids.contains(&ev_b.to_string()));

    // The fire-and-forget insert shape (insert_activity_event): succeeds on a
    // scoped tx as coralswarm_app…
    let mut tx = probe.scoped_tx(a.account_id).await;
    let inserted: Uuid = sqlx::query_scalar(
        "INSERT INTO activity_events (account_id, user_id, event_type, payload) \
         VALUES ($1, $2, 'query_run', '{}'::jsonb) RETURNING id",
    )
    .bind(a.account_id)
    .bind(a.user_id)
    .fetch_one(&mut *tx)
    .await
    .expect("scoped activity insert succeeds as coralswarm_app");
    assert_ne!(inserted, Uuid::nil());
    tx.rollback().await.ok();

    // …and hard-errors (WITH CHECK) when scope is dropped. Reads too.
    probe
        .assert_unscoped_hard_errors(
            "INSERT INTO activity_events (account_id, event_type) \
             VALUES ($1, 'query_run') RETURNING id",
            &[a.account_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT id FROM activity_events WHERE account_id = $1",
            &[a.account_id],
        )
        .await;

    probe.teardown().await;
}

// ── routes/queries.rs ────────────────────────────────────────────────────────

#[tokio::test]
async fn query_history_route_scoped_read_and_insert_shape_as_app_role() {
    let _env = ENV_LOCK.lock().await;
    let Some(probe) = RlsProbe::setup("orgt_rls_b1_queries").await else {
        eprintln!("skipping query_history_route_scoped_read_and_insert_shape_as_app_role: set ORGT_TEST_BASE_URL");
        return;
    };
    let jwks = spawn_local_jwks().await;
    let _guard = EnvGuard::set(&jwks);

    let a = seed_caller(&probe.owner, "queries_a").await;
    let b = seed_caller(&probe.owner, "queries_b").await;
    let app = app_over(&probe);

    let ga = create_graph_via_api(&app, &a, "Ocean A").await;
    let gb = create_graph_via_api(&app, &b, "Ocean B").await;

    let hist_a: Uuid = sqlx::query_scalar(
        "INSERT INTO query_history (account_id, graph_id, user_id, query_text) \
         VALUES ($1, $2, $3, 'what changed?') RETURNING id",
    )
    .bind(a.account_id)
    .bind(ga)
    .bind(a.user_id)
    .fetch_one(&probe.owner)
    .await
    .unwrap();
    let hist_b: Uuid = sqlx::query_scalar(
        "INSERT INTO query_history (account_id, graph_id, user_id, query_text) \
         VALUES ($1, $2, $3, 'b only') RETURNING id",
    )
    .bind(b.account_id)
    .bind(gb)
    .bind(b.user_id)
    .fetch_one(&probe.owner)
    .await
    .unwrap();

    // Scoped read (GET /v1/queries → list_query_history_scoped): A's history
    // only, even when filtering by B's graph id explicitly.
    let resp = app.clone().oneshot(get("/v1/queries", &a.token)).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let items = body_json(resp).await["items"].as_array().unwrap().clone();
    let ids: Vec<String> = items
        .iter()
        .filter_map(|i| i["id"].as_str().map(str::to_string))
        .collect();
    assert_eq!(ids, vec![hist_a.to_string()], "A's history is exactly A's rows");

    let resp = app
        .clone()
        .oneshot(get(&format!("/v1/queries?graph_id={gb}"), &a.token))
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let items = body_json(resp).await["items"].as_array().unwrap().clone();
    assert!(
        items.is_empty(),
        "A filtering by B's graph id gets zero rows, never {hist_b}"
    );

    // The fire-and-forget history insert shape (spawn_query_history): succeeds
    // on a scoped tx as coralswarm_app…
    let mut tx = probe.scoped_tx(a.account_id).await;
    let inserted: Uuid = sqlx::query_scalar(
        "INSERT INTO query_history (account_id, graph_id, user_id, query_text) \
         VALUES ($1, $2, $3, 'scoped insert') RETURNING id",
    )
    .bind(a.account_id)
    .bind(ga)
    .bind(a.user_id)
    .fetch_one(&mut *tx)
    .await
    .expect("scoped query_history insert succeeds as coralswarm_app");
    assert_ne!(inserted, Uuid::nil());
    tx.rollback().await.ok();

    // …and both the read and insert shapes hard-error when scope is dropped.
    probe
        .assert_unscoped_hard_errors(
            "SELECT id FROM query_history WHERE account_id = $1",
            &[a.account_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "INSERT INTO query_history (account_id, graph_id, user_id, query_text) \
             VALUES ($1, $2, $3, 'unscoped') RETURNING id",
            &[a.account_id, ga, a.user_id],
        )
        .await;

    probe.teardown().await;
}
