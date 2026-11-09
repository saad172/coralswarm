//! Integration tests for org-tenancy PR 1.4 (route migration chunk A:
//! `routes/{graphs,activity,atoms,ingest,queries}.rs`), PR 1.5 (route
//! migration chunk B: `routes/{nodes,relationships,conversations,
//! supersession,atlas}.rs`), and PR 1.6 (route migration chunk C:
//! `routes/{meetings,transcript,sessions,session_shares,shares,connectors,
//! slack,teams,workflows,calendar,sync}.rs` + the org fail-closed connector
//! gates) — end-to-end, through the REAL public HTTP router, under a REAL
//! `TenancyDecision::Org` produced by the real binding gate against two real
//! Postgres databases (control + tenant).
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights). Follows the same scratch-DB pattern as
//! `binding_gate_pg.rs` / `reachability_pg.rs` / `dek_relocation_pg.rs`, and
//! the same "spin up a local JWKS listener + mint a real Clerk-shaped JWT"
//! pattern `mcp_pg_e2e.rs` already established for driving the real router
//! with real Clerk verification.
//!
//! What this proves, beyond the unit tests in `tenancy::extractor` (which use
//! a lazy/never-connected pool) and the pure `ReachabilityResolver`/`crypto`
//! Postgres tests (which call library functions directly, never through a
//! route handler): that the `Tenant` extractor + `ReachabilityResolver` +
//! `require_control_store!`/`state.db` control-plane pins this PR wired into
//! `routes/graphs.rs`, `routes/activity.rs`, `routes/atoms.rs`, and
//! `routes/ingest.rs` all compose correctly on a REAL request path — Host
//! header → binding gate → `TenancyDecision::Org` → route handler → tenant
//! pool — with ZERO ambient `state.db`/`state.store` content-plane access.

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

/// Serializes the shared-process `CLERK_ISSUER`/`CLERK_JWKS_URL` env mutations:
/// every test in this binary sets them (to its own per-test JWKS URL) and
/// removes them at the end, so without this lock parallel tests would clobber
/// each other's vars mid-request and make JWT verification fail
/// nondeterministically. Each test holds the guard for its whole body. Same
/// pattern as `mcp_governance_pg.rs`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

/// RAII env-var guard for this binary's shared [`ENV_LOCK`]: acquires the
/// lock for its whole lifetime and restores every var it touches to its
/// PRE-guard value on `Drop` — including on a panicking assertion's unwind,
/// unlike a bare `std::env::remove_var` call at the very end of a test body
/// (which a panic skips entirely, since it never reaches that line). That
/// exact gap is how `org_slack_oauth_gate_lift_targets_control_plane_and_
/// stays_slack_only_pg`'s own panic used to leak `ORG_SLACK_OAUTH_ENABLED=1`
/// into `org_decision_chunk_c_routes_read_and_write_only_the_tenant_db`
/// (which runs later in the same process and never itself touches that var)
/// — the second, derivative failure #731 fixed alongside the real cache bug.
///
/// Mirrors `member_ocean_lifecycle_pg.rs`'s own `EnvGuard(MutexGuard<'static,
/// ()>)` shape, generalized one level: that file's guard always sets the
/// same two vars, but each test in THIS file sets a different combination,
/// and the gate-lift test additionally flips `ORG_SLACK_OAUTH_ENABLED`
/// partway through its body (`set`/`unset` below support that — a guard
/// already acquired can keep touching new keys, each remembered from ITS
/// own pre-guard value, not re-captured on a second touch).
struct EnvGuard {
    _lock: tokio::sync::MutexGuard<'static, ()>,
    /// Each touched key's value from BEFORE this guard ever touched it,
    /// captured once on first touch (`remember`) — a later `set`/`unset`
    /// call for the SAME key never overwrites what `Drop` restores to.
    prior: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    /// Acquires [`ENV_LOCK`] and sets every `(key, value)` pair.
    async fn acquire(pairs: &[(&'static str, &str)]) -> Self {
        let lock = ENV_LOCK.lock().await;
        let mut guard = Self { _lock: lock, prior: Vec::new() };
        for (key, value) in pairs {
            // SAFETY: `guard._lock` above serializes every mutation of these
            // vars in this binary against every other test's.
            unsafe { guard.set(key, value) };
        }
        guard
    }

    /// Sets `key` to `value`, remembering its pre-guard value on first
    /// touch. Safe to call any time after `acquire` — including partway
    /// through a test body for a var the test only decides to set later
    /// (e.g. the gate-lift test's `ORG_SLACK_OAUTH_ENABLED` flip) — the same
    /// lock is still held, so the restore contract is unchanged.
    ///
    /// # Safety
    /// Caller must hold this guard's `ENV_LOCK` for the whole time any other
    /// thread could observe `key` (true for the guard's entire lifetime),
    /// so no concurrent test reads it mid-mutation.
    unsafe fn set(&mut self, key: &'static str, value: &str) {
        self.remember(key);
        // SAFETY: see this method's own doc comment.
        unsafe { std::env::set_var(key, value) };
    }

    /// Removes `key` outright, remembering its pre-guard value on first
    /// touch. Used for a var a test wants explicitly ABSENT at some point
    /// (e.g. the gate-lift test's `SLACK_CLIENT_ID`/`SLACK_CLIENT_SECRET`/
    /// `ORG_SLACK_OAUTH_ENABLED` starting state) rather than set to a value.
    ///
    /// # Safety
    /// Same contract as [`Self::set`].
    unsafe fn unset(&mut self, key: &'static str) {
        self.remember(key);
        // SAFETY: see this method's own doc comment.
        unsafe { std::env::remove_var(key) };
    }

    fn remember(&mut self, key: &'static str) {
        if !self.prior.iter().any(|(k, _)| *k == key) {
            self.prior.push((key, std::env::var(key).ok()));
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // Runs to completion BEFORE `self._lock` itself is dropped (a
        // type's own `drop` body always runs before its fields' drops), so
        // every restore below is still serialized against every other
        // test's mutations by the same lock `acquire`/`set`/`unset`
        // document — on the happy path AND on a panicking unwind.
        for (key, value) in self.prior.drain(..) {
            // SAFETY: see `set`/`unset`'s own doc comment — `self._lock` is
            // still held for the whole body of this `drop` call.
            unsafe {
                match value {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

const APEX: &str = "coralswarm.example";

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}
async fn recreate(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Spin up a local JWKS listener serving the shared test keypair — same
/// fixture `mcp_pg_e2e.rs`/`mcp_http.rs` use, duplicated here for the same
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

#[tokio::test]
async fn org_decision_routes_content_through_the_tenant_pool_never_control() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_decision_routes_content_through_the_tenant_pool_never_control: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // ── Env: local JWKS listener so `require_auth` verifies real Clerk-shaped
    //    tokens minted by `clerk::test_support::mint`. ─────────────────────────
    // `EnvGuard` holds `ENV_LOCK` for the whole test body (serializing the
    // CLERK_* mutations against sibling tests) AND restores them on Drop,
    // including on a panicking assertion's unwind — see `EnvGuard`'s own doc
    // comment.
    let jwks_url = spawn_local_jwks().await;
    let _env_guard = EnvGuard::acquire(&[("CLERK_ISSUER", ISSUER), ("CLERK_JWKS_URL", &jwks_url)]).await;

    let (ctl, tenant_db) = ("orgt_route_ctl", "orgt_route_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + a pre-seeded org account + routing row ─────
    // Pre-creating the `accounts` row with `clerk_org_id` already set (rather
    // than letting `ensure_org_account` lazily create it on first request)
    // sidesteps the chicken-and-egg problem of `tenant_databases.account_id`'s
    // NOT NULL FK to `accounts(id)` needing a real id before any request has
    // run — `ensure_org_account`'s early-return path finds this exact row
    // (`SELECT id FROM accounts WHERE clerk_org_id = $1`) rather than
    // inserting a second one, so it's indistinguishable from the lazy path
    // any real deployment takes.
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let dummy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'person@example.com') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, 'org_a') RETURNING id",
    )
    .bind(dummy_owner)
    .fetch_one(&control)
    .await
    .unwrap();

    // A CONTROL-plane decoy graph on an unrelated personal account — proves
    // the org-decision routes below never surface control-plane content.
    let decoy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'person@example.com') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let decoy_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(decoy_owner)
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query("INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Decoy personal ocean', 'personal')")
        .bind(decoy_account)
        .execute(&control)
        .await
        .unwrap();

    financial_fixture::execute(
        &control,
        org_account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_a', $1, 'acme', 'SECRET_A', 'active')",
        )
        .bind(org_account_id),
    )
    .await
    .unwrap();

    // ── Tenant DB: tenant tree + fingerprint (content seeded AFTER the
    //    create-graph call below, once we have a real, API-created graph_id
    //    to hang it off — see the "one org ocean per account" note there). ───
    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_a')")
        .execute(&tenant_pool)
        .await
        .unwrap();

    // ── AppState: control pool as the ambient/Personal-plane db+store, and a
    //    real `TenantRuntime` (enabled) whose registry resolves 'SECRET_A' to
    //    this test's tenant Postgres — exactly the fingerprint-verified pool
    //    the `Tenant` extractor's `Org` arm builds a fresh `PostgresStore`
    //    over. ────────────────────────────────────────────────────────────────
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_A", with_db(&base, tenant_db))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let member_sub = format!("route_migration_org_pg_member_{}", Uuid::new_v4());
    let token = mint(json!({
        "iss": ISSUER,
        "sub": member_sub,
        "exp": future_exp(),
        "org_id": "org_a",
        "org_role": "org:member",
    }));
    // Claim-based tenancy Phase 4 cutover step 3: the org API address
    // `<slug>.api.<apex>` is retired (410). The apex is now every tenant's
    // door and the `org_id` claim on the token below is what names the
    // tenant, so this file's question — "does an org decision route through
    // the TENANT pool and never the control plane?" — is unchanged; only the
    // door is. The tenant-vs-control assertions are what have teeth here,
    // and they are untouched.
    let org_host = APEX;

    macro_rules! req {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        ($method:expr, $uri:expr, $json:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&$json).unwrap()))
                .unwrap()
        };
    }

    // ── 1. GET /v1/graphs (routes/graphs.rs::list_graphs) — the FIRST
    //    authenticated request into this org, through the REAL binding gate.
    //
    //    Before this PR, this org's tenant would be genuinely empty here (the
    //    prod `testorg` bug: `clerk_provision::ensure_org_account` never
    //    created an org ocean under `MULTITENANT_ENABLED`, and nothing else
    //    did either). Now, `TenantRuntime::finish_binding` builds this org's
    //    tenant pool via `TenantRegistry::get_or_build` as part of AUTHING
    //    this very request — BEFORE `list_graphs`'s handler ever runs — and
    //    that build self-heals the missing commons ocean
    //    (`TenantRegistry::ensure_org_ocean`). So the very first response this
    //    org ever gets back is already non-empty, with zero setup/backfill
    //    step anywhere in this test. ────────────────────────────────────────
    let resp = app.clone().oneshot(req!("GET", "/v1/graphs")).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "list_graphs must succeed under Org decision, and must never itself require an \
         explicit create-ocean call first (the org-tenancy empty-tenant bug this PR fixes)"
    );
    let body = body_json(resp).await;
    let graphs = body.as_array().expect("bare array");
    assert_eq!(
        graphs.len(),
        1,
        "the self-healed org ocean must be the ONLY graph visible here — never empty (the bug), \
         never the control-DB decoy personal ocean, got {graphs:?}"
    );
    assert_eq!(graphs[0]["kind"], json!("org"));
    let org_graph_id: Uuid = graphs[0]["id"].as_str().unwrap().parse().unwrap();

    let in_tenant: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id = $1)")
        .bind(org_graph_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
    assert!(in_tenant, "the self-healed ocean must land in the TENANT database");
    let in_control: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id = $1)")
        .bind(org_graph_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert!(
        !in_control,
        "the self-healed ocean must NEVER land in the control/consumer database"
    );

    // A further explicit `POST /v1/graphs {kind: "org"}` must now correctly
    // 409 — `routes::graphs::create_graph`'s own "one organization ocean per
    // account" check (unrelated to this PR) sees the self-healed ocean above
    // exactly like any other org-kind graph and refuses a second one. This
    // pins the interaction: the self-heal produces a row indistinguishable
    // from one the API created itself.
    let resp = app
        .clone()
        .oneshot(req!(
            "POST",
            "/v1/graphs",
            json!({"name": "A second org ocean", "kind": "org"})
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "create_graph(kind=org) must refuse a second org ocean, recognizing the self-healed one"
    );

    // ── Seed further tenant-DB content off the just-created, API-real
    //    graph_id (an atom + an activity event), so the read routes below
    //    exercise real tenant-pool data. ───────────────────────────────────────
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
         participants, timestamp) \
         VALUES ($1, 'note', 'seed-1', 'unused', 'the quarterly revenue target is forty two', \
         '{}', now()) RETURNING atom_id",
    )
    .bind(org_graph_id)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO activity_events(account_id, user_id, graph_id, event_type, payload) \
         VALUES ($1, NULL, $2, 'atom_ingested', '{}'::jsonb)",
    )
    .bind(org_account_id)
    .bind(org_graph_id)
    .execute(&tenant_pool)
    .await
    .unwrap();

    // ── 2. GET /v1/graphs/{id}/stats (routes/graphs.rs::get_ocean_stats) —
    //    exercises `ReachabilityResolver::graph_reachable_by_user` + tenant
    //    `ocean_stats`. ──────────────────────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/graphs/{org_graph_id}/stats")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let stats = body_json(resp).await;
    assert_eq!(stats["atom_count"], json!(1), "the seeded tenant-DB atom must be counted");

    // ── 3. GET /v1/atoms/{id} (routes/atoms.rs::get_atom) — resolves through
    //    the Tenant extractor's store + ReachabilityResolver; content_inline
    //    means no DEK resolution is exercised on this specific call (that path
    //    is covered by `dek_relocation_pg.rs`'s dedicated tenant-DEK test), but
    //    the reachability + tenant-store plumbing is. ────────────────────────
    let resp = app.clone().oneshot(req!("GET", format!("/v1/atoms/{atom_id}"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "get_atom must succeed under Org decision");
    let atom = body_json(resp).await;
    assert_eq!(
        atom["content"], json!("the quarterly revenue target is forty two"),
        "get_atom must read the tenant-DB atom's inline content"
    );

    // ── 4. GET /v1/activity (routes/activity.rs::list_activity) — tenant-tree
    //    `activity_events` table. ───────────────────────────────────────────
    let resp = app.clone().oneshot(req!("GET", "/v1/activity")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "list_activity must succeed under Org decision");
    let events = body_json(resp).await;
    assert_eq!(
        events.as_array().unwrap().len(),
        1,
        "must see exactly the one tenant-DB activity_events row"
    );

    // ── 5. POST /v1/graphs (kind='personal') — `account_is_org` must read the
    //    CONTROL plane (`accounts.clerk_org_id IS NOT NULL`), not the tenant
    //    plane (which has no `accounts` table at all — a mistaken tenant-pool
    //    read would 500, not 400). The org account here DOES have
    //    `clerk_org_id` set, so this must be rejected with the
    //    org-can't-have-a-personal-ocean 400 — independent of the "one org
    //    ocean per account" check above (that branch only runs for
    //    `kind == "org"`). ──────────────────────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(req!(
            "POST",
            "/v1/graphs",
            json!({"name": "Should be rejected", "kind": "personal"})
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "create_graph(kind=personal) under an org account must 400 via the CONTROL-plane \
         account_is_org check, not 500 from a mistaken tenant-plane accounts query"
    );

    // ── 6. POST /v1/ingest/atom targeting the org's own (kind='org') graph —
    //    ADR-0007 D7 fail-closed, as amended by member-private-oceans PR 5:
    //    ingest only ever targets the caller's own PRIVATE ocean (personal or
    //    `kind='member'`), never a shared `kind='org'` ocean. Reachability
    //    succeeds (it's the org's own tenant graph), but
    //    `get_graph(...).kind` failing BOTH the `personal` and `member`
    //    (owned + non-archived) arms must reject with 400 — proving
    //    `ingest::submit`'s content-plane calls (`graph_account_id`,
    //    reachability, `get_graph`) all correctly ran against the TENANT pool
    //    (a control-plane misroute would 403 "not found", not this 400, since
    //    `org_graph_id` doesn't exist in the control database's `graphs` table
    //    at all). ────────────────────────────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(req!(
            "POST",
            "/v1/ingest/atom",
            json!({
                "graph_id": org_graph_id,
                "source_type": "note",
                "source_id": "ingest-attempt-1",
                "content": "this should never land anywhere",
                "participants": [],
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })
        ))
        .await
        .unwrap();
    let status = resp.status();
    let ingest_body = body_json(resp).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "ingest into a non-personal (org) tenant graph must 400, got {status} {ingest_body:?}"
    );
    assert!(
        ingest_body["error"]
            .as_str()
            .unwrap_or("")
            .contains("your own private ocean"),
        "must be the D7 own-private-ocean-only rejection (member-private-oceans PR 5 wording), \
         got {ingest_body:?}"
    );

    // Cleanup. Env vars restored automatically when `_env_guard` drops at
    // the end of this function — including on panic unwind, not just this
    // happy path (see `EnvGuard`'s own doc comment).
    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// Poll (bounded) for the fire-and-forget `insert_query_history` spawn
/// `routes/queries.rs::create_query`'s retrieval-only path kicks off — it
/// runs off the request's critical path, so the row may not exist the
/// instant the HTTP response returns.
async fn poll_query_history_count(pool: &PgPool, account_id: Uuid, graph_id: Uuid) -> i64 {
    for _ in 0..50 {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM query_history WHERE account_id = $1 AND graph_id = $2",
        )
        .bind(account_id)
        .bind(graph_id)
        .fetch_one(pool)
        .await
        .unwrap();
        if count > 0 {
            return count;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    0
}

/// org-tenancy PR 1.4 P2-3 (adversarial review follow-up): `queries.rs` is
/// this PR's largest and highest-risk file, and `route_migration_org_pg.rs`'s
/// first test never touched it. Proves `routes/queries.rs::create_query`
/// (the non-streaming `POST /v1/queries`) under a real `TenancyDecision::Org`
/// — reachability + IDOR + the fire-and-forget `insert_query_history` spawn —
/// writes `query_history` rows into the TENANT database, never the control
/// database, and that `routes/queries.rs::list_queries`
/// (`GET /v1/queries`) reads them back from the same tenant plane, never
/// surfacing a control-plane decoy row for an unrelated account.
#[tokio::test]
async fn org_decision_query_history_lands_in_tenant_db_never_control() {
    let Some(base) = base_url() else {
        eprintln!("skipping org_decision_query_history_lands_in_tenant_db_never_control: set ORGT_TEST_BASE_URL");
        return;
    };

    // `EnvGuard` holds `ENV_LOCK` for the whole test body (serializing the
    // CLERK_* mutations against sibling tests) AND restores them on Drop,
    // including on a panicking assertion's unwind — see `EnvGuard`'s own doc
    // comment.
    let jwks_url = spawn_local_jwks().await;
    let _env_guard = EnvGuard::acquire(&[("CLERK_ISSUER", ISSUER), ("CLERK_JWKS_URL", &jwks_url)]).await;

    let (ctl, tenant_db) = ("orgt_route_queries_ctl", "orgt_route_queries_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + pre-seeded org account + routing row, same
    //    pattern as the sibling test above. Plus a CONTROL-plane decoy
    //    `query_history` row for a totally unrelated account/graph, so
    //    `list_queries` under the Org decision has something real to leak IF
    //    it (wrongly) fell back to the ambient consumer store. ──────────────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let dummy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'org-owner@acme2.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, 'org_b') RETURNING id",
    )
    .bind(dummy_owner)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        org_account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_b', $1, 'beta', 'SECRET_B', 'active')",
        )
        .bind(org_account_id),
    )
    .await
    .unwrap();

    let decoy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'decoy@other.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let decoy_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(decoy_owner)
            .fetch_one(&control)
            .await
            .unwrap();
    let decoy_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Decoy', 'personal') RETURNING id",
    )
    .bind(decoy_account)
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO query_history(account_id, graph_id, user_id, query_text) \
         VALUES ($1, $2, NULL, 'control-plane decoy query — must never leak into an org response')",
    )
    .bind(decoy_account)
    .bind(decoy_graph)
    .execute(&control)
    .await
    .unwrap();

    // ── Tenant DB: tenant tree + fingerprint + one org graph. ─────────────────
    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_b')")
        .execute(&tenant_pool)
        .await
        .unwrap();
    let org_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Beta org ocean', 'org') RETURNING id",
    )
    .bind(org_account_id)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();

    // ── AppState + app (no LLM configured — `test_app_state()`'s default —
    //    so `create_query` always takes its retrieval-only path regardless of
    //    what retrieval finds, which is exactly the path that calls
    //    `insert_query_history`). ───────────────────────────────────────────
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_B", with_db(&base, tenant_db))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let member_sub = format!("route_migration_org_pg_queries_member_{}", Uuid::new_v4());
    let token = mint(json!({
        "iss": ISSUER,
        "sub": member_sub,
        "exp": future_exp(),
        "org_id": "org_b",
        "org_role": "org:member",
    }));
    // Claim-based tenancy Phase 4 cutover step 3: the org API address
    // `<slug>.api.<apex>` is retired (410). The apex is now every tenant's
    // door and the `org_id` claim on the token below is what names the
    // tenant, so this file's question — "does an org decision route through
    // the TENANT pool and never the control plane?" — is unchanged; only the
    // door is. The tenant-vs-control assertions are what have teeth here,
    // and they are untouched.
    let org_host = APEX;

    // ── 1. POST /v1/queries (routes/queries.rs::create_query) under Org
    //    decision — retrieval-only (no LLM configured), so this must 200 and
    //    fire the query-history insert regardless of whether retrieval found
    //    any atoms. ──────────────────────────────────────────────────────────
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/queries")
        .header("host", org_host)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"text": "revenue target", "graph_id": org_graph_id})).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "create_query must succeed under Org decision");
    let body = body_json(resp).await;
    assert_eq!(body["mode"], json!("retrieval_only"), "no LLM configured => retrieval-only mode");

    // ── 2. The fire-and-forget `insert_query_history` write must land in the
    //    TENANT database (never the control database). ───────────────────────
    let tenant_count = poll_query_history_count(&tenant_pool, org_account_id, org_graph_id).await;
    assert_eq!(
        tenant_count, 1,
        "create_query's query-history insert must land in the TENANT database"
    );
    let control_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM query_history WHERE account_id = $1 AND graph_id = $2",
    )
    .bind(org_account_id)
    .bind(org_graph_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        control_count, 0,
        "create_query's query-history insert must NEVER land in the control/consumer database"
    );

    // ── 3. GET /v1/queries (routes/queries.rs::list_queries) under Org
    //    decision — must read back the TENANT-DB row and never the
    //    control-plane decoy row for an unrelated account. ────────────────────
    let req = HttpRequest::builder()
        .method("GET")
        .uri("/v1/queries")
        .header("host", org_host)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "list_queries must succeed under Org decision");
    let listed = body_json(resp).await;
    let items = listed["items"].as_array().expect("bare items array");
    assert_eq!(items.len(), 1, "must see exactly the one tenant-DB query-history row: {items:?}");
    assert_eq!(items[0]["query_text"], json!("revenue target"));
    assert!(
        items.iter().all(|i| i["query_text"] != json!("control-plane decoy query — must never leak into an org response")),
        "the control-plane decoy row must never appear in an org-scoped list_queries response"
    );

    // Cleanup. Env vars restored automatically when `_env_guard` drops at
    // the end of this function — including on panic unwind, not just this
    // happy path (see `EnvGuard`'s own doc comment).
    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// Poll (bounded) for the fire-and-forget `insert_activity_event` spawn
/// `routes/supersession.rs::create_supersession` kicks off — the spawn owns a
/// pool clone and runs off the request's critical path, so the row may not
/// exist the instant the HTTP response returns.
async fn poll_activity_event_count(pool: &PgPool, account_id: Uuid, event_type: &str) -> i64 {
    for _ in 0..50 {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM activity_events WHERE account_id = $1 AND event_type = $2",
        )
        .bind(account_id)
        .bind(event_type)
        .fetch_one(pool)
        .await
        .unwrap();
        if count > 0 {
            return count;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    0
}

/// org-tenancy PR 1.5 (route migration chunk B): the highest-risk routes of
/// `routes/{nodes,supersession,conversations,relationships,atlas}.rs` under a
/// real `TenancyDecision::Org` through the real binding gate.
///
/// Decoy strategy: the CONTROL database carries a MIRROR graph with the SAME
/// `graph_id` as the tenant org graph (owned by an unrelated personal
/// account), a mirror `nodes` row with the SAME `node_id`, and a decoy
/// conversation under that mirror graph — so if any migrated handler had
/// regressed to the ambient `state.db`/`state.store`, the wrong-plane
/// read/write would find perfectly plausible rows to return/update, and the
/// assertions below would catch it (rather than the misroute just erroring
/// out on a missing row).
///
/// Specifically proves:
/// 1. `GET /v1/nodes` (list_nodes) lists the TENANT-DB reefs, never the
///    control-DB mirror.
/// 2. `PATCH /v1/nodes/{id}` (patch_node) writes the TENANT-DB row; the
///    control-DB mirror row with the same `node_id` stays untouched.
/// 3. `POST /v1/nodes/{id}/supersede` (create_supersession) writes the
///    `supersession_edge` row into the TENANT database only — and its
///    fire-and-forget `insert_activity_event` spawn (the one `tokio::spawn`
///    in this chunk, owning a `Tenant`-scoped pool clone) inserts its
///    `activity_events` row into the TENANT database, never the control DB.
/// 4. `GET /v1/nodes/{id}/supersessions` (list_supersessions) reads it back
///    from the tenant plane.
/// 5. `GET /v1/graphs/{id}/atlas` (get_atlas) resolves reachability through
///    `ReachabilityResolver::list_reachable_graphs`'s tenant path and renders
///    the tenant ocean + reefs.
/// 6. `GET /v1/conversations?graph_id=` (list_conversations) returns the
///    TENANT-DB conversation and never the control-DB decoy under the mirror
///    graph id.
/// 7. `GET /v1/graphs/{id}/relationships` (list_relationships) serves the
///    tenant plane (empty, 200 — not a wrong-plane error).
#[tokio::test]
async fn org_decision_chunk_b_routes_read_and_write_only_the_tenant_db() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_decision_chunk_b_routes_read_and_write_only_the_tenant_db: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // `EnvGuard` holds `ENV_LOCK` for the whole test body (serializing the
    // CLERK_* mutations against sibling tests) AND restores them on Drop,
    // including on a panicking assertion's unwind — see `EnvGuard`'s own doc
    // comment.
    let jwks_url = spawn_local_jwks().await;
    let _env_guard = EnvGuard::acquire(&[("CLERK_ISSUER", ISSUER), ("CLERK_JWKS_URL", &jwks_url)]).await;

    let (ctl, tenant_db) = ("orgt_route_b_ctl", "orgt_route_b_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + pre-seeded org account + routing row. ──────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let dummy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'org-owner@gamma.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, 'org_c') RETURNING id",
    )
    .bind(dummy_owner)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        org_account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_c', $1, 'gamma', 'SECRET_C', 'active')",
        )
        .bind(org_account_id),
    )
    .await
    .unwrap();

    // ── Tenant DB: tenant tree + fingerprint + one org graph + two reefs. ─────
    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_c')")
        .execute(&tenant_pool)
        .await
        .unwrap();
    let org_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Gamma org ocean', 'org') RETURNING id",
    )
    .bind(org_account_id)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();
    let node_a: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title) VALUES ($1, 'entity', 'Original reef title') RETURNING node_id",
    )
    .bind(org_graph_id)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();
    let node_b: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title) VALUES ($1, 'entity', 'Newer reef') RETURNING node_id",
    )
    .bind(org_graph_id)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();

    // Library reefs require real permitted coral support. The first note
    // explicitly supports both the original and subsequently PATCHed title.
    for (node, text) in [
        (node_a, "Synthetic fixture note: Original reef title is renamed Renamed by chunk B."),
        (node_b, "Synthetic fixture note: Newer reef."),
    ] {
        let source_id = Uuid::new_v4().to_string();
        let atom: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) \
             VALUES($1,'note',$2,'fixture:route-chunk-b',$3,now()) RETURNING atom_id",
        )
        .bind(org_graph_id).bind(&source_id).bind(text)
        .fetch_one(&tenant_pool).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
            .bind(node).bind(atom).execute(&tenant_pool).await.unwrap();
        let support: (Uuid, String, String) = sqlx::query_as(
            "SELECT a.graph_id,a.source_id,a.content_inline FROM node_atoms l \
             JOIN nodes n ON n.node_id=l.node_id JOIN atoms a ON a.atom_id=l.atom_id \
             WHERE l.node_id=$1 AND l.atom_id=$2 AND a.graph_id=n.graph_id",
        )
        .bind(node).bind(atom).fetch_one(&tenant_pool).await.unwrap();
        assert_eq!(support, (org_graph_id, source_id, text.to_string()));
    }

    // ── Control-DB decoys: a MIRROR graph with the SAME id as the tenant org
    //    graph (under an unrelated personal account) and a mirror node with
    //    the SAME node_id as `node_a` — plausible wrong-plane targets. ────────
    let decoy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'decoy@gamma.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let decoy_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(decoy_owner)
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query(
        "INSERT INTO graphs(id, account_id, name, kind) VALUES ($1, $2, 'Decoy mirror ocean', 'personal')",
    )
    .bind(org_graph_id)
    .bind(decoy_account)
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO nodes(node_id, graph_id, type, title) VALUES ($1, $2, 'entity', 'Decoy control title')",
    )
    .bind(node_a)
    .bind(org_graph_id)
    .execute(&control)
    .await
    .unwrap();

    // ── AppState + app: control pool as the ambient plane, real enabled
    //    TenantRuntime resolving SECRET_C to the tenant Postgres. ─────────────
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_C", with_db(&base, tenant_db))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let member_sub = format!("route_migration_org_pg_chunk_b_member_{}", Uuid::new_v4());
    // An org ADMIN: every write below targets the org commons, which plain
    // members may read but not rewrite (settings 2.10's reef write scope —
    // `routes::nodes::ReefWriter`). This test is about which PLANE the write
    // lands on, so it uses the role that may make it.
    let token = mint(json!({
        "iss": ISSUER,
        "sub": member_sub,
        "exp": future_exp(),
        "org_id": "org_c",
        "org_role": "org:admin",
        // A real session JWT carries `sid`; a `sid`-less token is a
        // non-interactive credential and gets no admin authority (settings 2.8).
        "sid": format!("sess_{member_sub}"),
    }));
    // Claim-based tenancy Phase 4 cutover step 3: the org API address
    // `<slug>.api.<apex>` is retired (410). The apex is now every tenant's
    // door and the `org_id` claim on the token below is what names the
    // tenant, so this file's question — "does an org decision route through
    // the TENANT pool and never the control plane?" — is unchanged; only the
    // door is. The tenant-vs-control assertions are what have teeth here,
    // and they are untouched.
    let org_host = APEX;

    macro_rules! req {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        ($method:expr, $uri:expr, $json:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&$json).unwrap()))
                .unwrap()
        };
    }

    // ── 1. GET /v1/nodes (routes/nodes.rs::list_nodes) — must list the two
    //    TENANT-DB reefs (never the control-DB mirror node alone / in
    //    addition). Also materializes the member's `users` row on the control
    //    plane for the conversation seeding below. ─────────────────────────────
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/nodes?graph_id={org_graph_id}")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "list_nodes must succeed under Org decision");
    let listed = body_json(resp).await;
    let mut ids: Vec<String> = listed
        .as_array()
        .expect("bare array")
        .iter()
        .map(|n| n["node_id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    let mut expect = vec![node_a.to_string(), node_b.to_string()];
    expect.sort();
    assert_eq!(ids, expect, "list_nodes must return exactly the tenant-DB reefs");
    let titles: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["title"].as_str().unwrap())
        .collect();
    assert!(
        !titles.contains(&"Decoy control title"),
        "the control-DB mirror node must never surface: {titles:?}"
    );

    // The org member's user row was materialized on the control plane by the
    // request above — needed to seed a tenant conversation it owns.
    let member_user_id: Uuid =
        sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
            .bind(&member_sub)
            .fetch_one(&control)
            .await
            .unwrap();

    // ── 2. PATCH /v1/nodes/{node_a} (routes/nodes.rs::patch_node) — the write
    //    must land on the TENANT row; the control-DB mirror row with the SAME
    //    node_id must stay byte-identical. ───────────────────────────────────
    let resp = app
        .clone()
        .oneshot(req!(
            "PATCH",
            format!("/v1/nodes/{node_a}"),
            json!({"title": "Renamed by chunk B"})
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "patch_node must succeed under Org decision");
    let tenant_title: String = sqlx::query_scalar("SELECT title FROM nodes WHERE node_id = $1")
        .bind(node_a)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
    assert_eq!(tenant_title, "Renamed by chunk B", "patch_node must update the TENANT row");
    let control_title: String = sqlx::query_scalar("SELECT title FROM nodes WHERE node_id = $1")
        .bind(node_a)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        control_title, "Decoy control title",
        "patch_node must NEVER touch the control-DB mirror row"
    );

    // ── 3. POST /v1/nodes/{node_a}/supersede (routes/supersession.rs::
    //    create_supersession) — edge row + the fire-and-forget activity-event
    //    spawn must both land in the TENANT database. ─────────────────────────
    let resp = app
        .clone()
        .oneshot(req!(
            "POST",
            format!("/v1/nodes/{node_a}/supersede"),
            json!({"superseded_by": node_b})
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "create_supersession must succeed under Org decision"
    );
    let edge_in_tenant: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM supersession_edge WHERE superseder_id = $1 AND superseded_id = $2)",
    )
    .bind(node_b)
    .bind(node_a)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();
    assert!(edge_in_tenant, "the supersession edge must land in the TENANT database");
    let edge_in_control: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM supersession_edge)")
            .fetch_one(&control)
            .await
            .unwrap();
    assert!(!edge_in_control, "no supersession edge may ever land in the control database");

    // The one `tokio::spawn` in chunk B: `insert_activity_event` owns a pool
    // clone handed to it by the handler — that clone must be the TENANT pool.
    let tenant_events =
        poll_activity_event_count(&tenant_pool, org_account_id, "supersession_created").await;
    assert_eq!(
        tenant_events, 1,
        "create_supersession's fire-and-forget activity event must land in the TENANT database"
    );
    let control_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM activity_events WHERE account_id = $1 AND event_type = 'supersession_created'",
    )
    .bind(org_account_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        control_events, 0,
        "the activity-event spawn must NEVER write to the control database"
    );

    // ── 4. GET /v1/nodes/{node_a}/supersessions (routes/supersession.rs::
    //    list_supersessions) — reads the edge back from the tenant plane. ─────
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/nodes/{node_a}/supersessions")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let sups = body_json(resp).await;
    assert_eq!(
        sups["as_superseded"].as_array().unwrap().len(),
        1,
        "list_supersessions must read the tenant-DB edge back: {sups:?}"
    );

    // ── 5. GET /v1/graphs/{id}/atlas (routes/atlas.rs::get_atlas) — the
    //    `ReachabilityResolver::list_reachable_graphs` tenant path must find
    //    the org ocean (name from the TENANT row, not the control mirror). ────
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/graphs/{org_graph_id}/atlas")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "get_atlas must succeed under Org decision");
    let atlas = body_json(resp).await;
    assert_eq!(
        atlas["ocean"]["name"],
        json!("Gamma org ocean"),
        "get_atlas must render the TENANT ocean, never the control-DB mirror: {atlas:?}"
    );

    // ── 6. Conversations: seed a TENANT conversation owned by the member and
    //    a CONTROL decoy under the mirror graph id — the listing must return
    //    exactly the tenant one. ───────────────────────────────────────────────
    let tenant_conversation: Uuid = sqlx::query_scalar(
        "INSERT INTO conversations(account_id, graph_id, user_id, title) \
         VALUES ($1, $2, $3, 'Tenant thread') RETURNING id",
    )
    .bind(org_account_id)
    .bind(org_graph_id)
    .bind(member_user_id)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();
    // Unreceipted model-generated titles are withheld; this exact title is
    // instead an explicit user-authored contribution, the supported fallback.
    sqlx::query(
        "INSERT INTO conversation_messages(conversation_id,seq,role,text) \
         VALUES($1,0,'user','Tenant thread')",
    )
    .bind(tenant_conversation).execute(&tenant_pool).await.unwrap();
    sqlx::query(
        "INSERT INTO conversations(account_id, graph_id, user_id, title) \
         VALUES ($1, $2, $3, 'Control decoy thread — must never leak')",
    )
    .bind(decoy_account)
    .bind(org_graph_id)
    .bind(member_user_id)
    .execute(&control)
    .await
    .unwrap();

    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/conversations?graph_id={org_graph_id}")))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "list_conversations must succeed under Org decision"
    );
    let convs = body_json(resp).await;
    let owned = convs["conversations"].as_array().expect("conversations array");
    assert_eq!(owned.len(), 1, "must see exactly the tenant-DB conversation: {convs:?}");
    assert_eq!(owned[0]["title"], json!("Tenant thread"));

    // ── 7. GET /v1/graphs/{id}/relationships (routes/relationships.rs::
    //    list_relationships) — tenant plane, no edges yet ⇒ 200 empty page
    //    (a wrong-plane misroute would 403/500 — the point here is the
    //    migrated reachability + store path serves). ──────────────────────────
    let resp = app
        .clone()
        .oneshot(req!(
            "GET",
            format!("/v1/graphs/{org_graph_id}/relationships?status=suggested")
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "list_relationships must succeed under Org decision"
    );
    let rels = body_json(resp).await;
    assert_eq!(
        rels,
        json!({"edges": [], "has_more": false, "next_cursor": null}),
        "no edges seeded ⇒ empty page"
    );

    // Cleanup. Env vars restored automatically when `_env_guard` drops at
    // the end of this function — including on panic unwind, not just this
    // happy path (see `EnvGuard`'s own doc comment).
    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// Retain a real, content-verified synthetic session capture before testing its
/// immutable Library projection. The fixture uses owner pools for setup; this
/// helper does not claim a separate ordinary-role RLS proof.
async fn retain_chunk_c_session(pool: &PgPool, graph: Uuid, account: Uuid, actor: Uuid) {
    let body = "Tenant session: synthetic retained route fixture body.";
    let (kind, project, title): (String, Option<String>, Option<String>) = sqlx::query_as(
        "UPDATE coding_sessions SET raw_inline=$2 WHERE graph_id=$1 AND session_id='sess-1' \
         RETURNING session_kind,project_key,title",
    )
    .bind(graph)
    .bind(body)
    .fetch_one(pool)
    .await
    .unwrap();
    let source_id = "cc:sess-1:1";
    let atom: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) \
         VALUES($1,'coding_session',$2,'fixture:route-chunk-c',$3,now()) RETURNING atom_id",
    )
    .bind(graph)
    .bind(source_id)
    .bind(body)
    .fetch_one(pool)
    .await
    .unwrap();
    let metadata = json!({"session_id":"sess-1","capture_context":{
        "revision":1,"session_id":"sess-1","session_kind":kind,
        "session_title":title,"project_key":project,"root_session_id":"sess-1"
    }});
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account)
        .await.unwrap();
    coralswarm_backend::tenancy::scope::set_tenant_user(&mut tx, actor).await.unwrap();
    coralswarm_backend::capture_context::lock_session_tx(&mut tx, graph, "sess-1")
        .await.unwrap();
    let captured = coralswarm_backend::source_evidence::capture_source_version_tx(
        &mut tx, None, graph, account,
        &coralswarm_backend::source_evidence::CaptureSource {
            source_type: "coding_session", provider_namespace: "fixture:route-chunk-c",
            source_id, normalized_text: body, representation: "original_text",
            metadata: &metadata, occurred_at: None, participants: &[],
            ingested_by: Some(actor), scope: "private", duration_ms: None, segments: None,
        }, None, &[7; 32],
    ).await.expect("retain exact session bytes and immutable reported context");
    coralswarm_backend::source_evidence::link_verified_chunks_tx(
        &mut tx, graph, captured.version_id, &[atom],
    ).await.expect("map only the genuinely matching session coral");
    let admitted: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM source_version_chunks c \
         JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id \
         JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id \
         WHERE c.graph_id=$1 AND c.atom_id=$2 AND v.version_id=$3 \
         AND s.current_version_id=v.version_id AND s.access_state='permitted' \
         AND v.raw_inline=$4 AND library_atom_permitted(c.graph_id,c.atom_id)), \
         (SELECT count(*) FROM library_sessions($1) WHERE session_id='sess-1' AND title='Tenant session')",
    )
    .bind(graph).bind(atom).bind(captured.version_id).bind(body)
    .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(admitted, (1, 1), "exact retained support must admit the original session title");
    tx.commit().await.unwrap();
}

/// org-tenancy PR 1.6 (route migration chunk C): the highest-risk routes of
/// `routes/{meetings,sessions,shares,sync}.rs` under a real
/// `TenancyDecision::Org` through the real binding gate, plus the chunk's
/// fail-closed org gates (`connectors`/`workflows`/`calendar`/`slack`
/// management and CS-094 named teams) and cloud whole-original refusal.
///
/// Decoy strategy (mirrors the chunk-B test): the CONTROL database carries a
/// MIRROR graph with the SAME `graph_id` as the tenant org graph — owned by
/// the SAME org account row, so account-scoped wrong-plane queries (sync
/// pull's `graphs.account_id` join) find perfectly plausible rows — plus a
/// mirror `meeting_transcripts` row with the same `(graph_id, source_id)`
/// but different text, a mirror `coding_sessions` row, and a decoy `op_log`
/// row. If any migrated handler regressed to ambient `state.db`/
/// `state.store`, the assertions below surface the decoy instead of just
/// erroring on a missing row.
///
/// Specifically proves:
/// 1.  `GET /v1/meetings/summary` withholds unverified cloud projections;
///     both planes' distinct private originals remain intact.
/// 2.  `POST /v1/transcript-corrections` (meetings.rs::create_speaker_relabel)
///     writes `transcript_corrections` into the TENANT database only.
/// 3.  `GET /v1/sessions` (sessions.rs) lists the TENANT coding session,
///     never the control decoy.
/// 4.  Cloud whole-original share POST and anonymous GET return the exact
///     selected-consent refusal, without changing an explicit historical
///     tenant share or exposing originals. This guard precedes token lookup;
///     the old share-mint / consumer-lookup positives are unavailable.
/// 5.  `GET /v1/sync/pull` (sync.rs) returns the TENANT op_log row, never the
///     control decoy op.
/// 6.  `POST /v1/sync/push` (sync.rs) refuses an unseen graph without
///     provisioning a graph or atom in either database.
/// 7.  The chunk's org fail-closed gates all 403: connectors sync, workflows
///     list, calendar status, slack integrations list, named-team create —
///     and the connector 403 writes nothing anywhere.
#[tokio::test]
async fn org_decision_chunk_c_routes_read_and_write_only_the_tenant_db() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_decision_chunk_c_routes_read_and_write_only_the_tenant_db: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // `EnvGuard` holds `ENV_LOCK` for the whole test body (serializing the
    // CLERK_* mutations against sibling tests) AND restores them on Drop,
    // including on a panicking assertion's unwind — see `EnvGuard`'s own doc
    // comment.
    let jwks_url = spawn_local_jwks().await;
    let _env_guard = EnvGuard::acquire(&[("CLERK_ISSUER", ISSUER), ("CLERK_JWKS_URL", &jwks_url)]).await;

    let (ctl, tenant_db) = ("orgt_route_c_ctl", "orgt_route_c_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_3 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + pre-seeded org account + routing row. ──────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let dummy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'org-owner@delta.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, 'org_d') RETURNING id",
    )
    .bind(dummy_owner)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        org_account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_d', $1, 'delta', 'SECRET_D', 'active')",
        )
        .bind(org_account_id),
    )
    .await
    .unwrap();

    // ── Tenant DB: tenant tree + fingerprint + org graph + chunk-C content. ───
    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_d')")
        .execute(&tenant_pool)
        .await
        .unwrap();
    let org_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Delta org ocean', 'org') RETURNING id",
    )
    .bind(org_account_id)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline) \
         VALUES ($1, 'meeting-1', 'Speaker 2: the tenant meeting raw text')",
    )
    .bind(org_graph_id)
    .execute(&tenant_pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO coding_sessions(graph_id, session_id, title) \
         VALUES ($1, 'sess-1', 'Tenant session')",
    )
    .bind(org_graph_id)
    .execute(&tenant_pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO op_log(op_id, entity, entity_id, op_type, payload, version, updated_at, device_id, graph_id) \
         VALUES (gen_random_uuid(), 'atom', gen_random_uuid()::text, 'upsert', \
                 '{\"marker\":\"tenant-op\"}'::jsonb, 1, now(), 'seed-device', $1)",
    )
    .bind(org_graph_id)
    .execute(&tenant_pool)
    .await
    .unwrap();

    // ── Control-DB decoys: a MIRROR graph with the SAME id, owned by the SAME
    //    org account (so account-scoped wrong-plane queries match), plus
    //    mirror transcript / session / op_log rows. ──────────────────────────
    sqlx::query(
        "INSERT INTO graphs(id, account_id, name, kind) VALUES ($1, $2, 'Decoy mirror ocean', 'personal')",
    )
    .bind(org_graph_id)
    .bind(org_account_id)
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline) \
         VALUES ($1, 'meeting-1', 'CONTROL DECOY transcript — must never leak')",
    )
    .bind(org_graph_id)
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO coding_sessions(graph_id, session_id, title) \
         VALUES ($1, 'sess-1', 'Control decoy session — must never leak')",
    )
    .bind(org_graph_id)
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO op_log(op_id, entity, entity_id, op_type, payload, version, updated_at, device_id, graph_id) \
         VALUES (gen_random_uuid(), 'atom', gen_random_uuid()::text, 'upsert', \
                 '{\"marker\":\"control-decoy-op\"}'::jsonb, 1, now(), 'seed-device', $1)",
    )
    .bind(org_graph_id)
    .execute(&control)
    .await
    .unwrap();

    // ── AppState + app: control pool as the ambient plane, real enabled
    //    TenantRuntime resolving SECRET_D to the tenant Postgres. ─────────────
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_D", with_db(&base, tenant_db))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let member_sub = format!("route_migration_org_pg_chunk_c_member_{}", Uuid::new_v4());
    let token = mint(json!({
        "iss": ISSUER,
        "sub": member_sub,
        "exp": future_exp(),
        "org_id": "org_d",
        "org_role": "org:member",
    }));
    // Claim-based tenancy Phase 4 cutover step 3: the org API address
    // `<slug>.api.<apex>` is retired (410). The apex is now every tenant's
    // door and the `org_id` claim on the token below is what names the
    // tenant, so this file's question — "does an org decision route through
    // the TENANT pool and never the control plane?" — is unchanged; only the
    // door is. The tenant-vs-control assertions are what have teeth here,
    // and they are untouched.
    let org_host = APEX;

    macro_rules! req {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        ($method:expr, $uri:expr, $json:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&$json).unwrap()))
                .unwrap()
        };
    }

    // ── 1. GET /v1/meetings/summary (routes/meetings.rs::meeting_summary) —
    //    withholds the unverified projection. This is a privacy check, not
    //    proof that the unavailable raw projection read the tenant plane. ───
    let resp = app
        .clone()
        .oneshot(req!(
            "GET",
            format!("/v1/meetings/summary?graph_id={org_graph_id}&source_id=meeting-1")
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "meeting_summary must succeed under Org decision");
    let summary = body_json(resp).await;
    assert_eq!(summary, json!({
        "source_id":"meeting-1","title":null,"occurred_at":null,
        "raw_text":null,"corrected_text":null,"corrections":[],"merges":[],"new_entities":[],
        "summary_status":"unavailable","summary":null,"summary_model":null,
        "summary_error":null,"segments":null,"source_items":[],
        "projection_status":"unverified_lineage","evidence_status":"unknown"
    }), "unverified cloud projections must not reveal either private original");
    let member_user_id: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id=$1")
        .bind(&member_sub).fetch_one(&control).await.unwrap();
    // The original session-list positive needs a genuine immutable capture,
    // not just a mutable coding_sessions descriptor or a cc: prefix.
    retain_chunk_c_session(&tenant_pool, org_graph_id, org_account_id, member_user_id).await;
    let tenant_original: String = sqlx::query_scalar(
        "SELECT raw_inline FROM meeting_transcripts WHERE graph_id=$1 AND source_id='meeting-1'",
    ).bind(org_graph_id).fetch_one(&tenant_pool).await.unwrap();
    let control_original: String = sqlx::query_scalar(
        "SELECT raw_inline FROM meeting_transcripts WHERE graph_id=$1 AND source_id='meeting-1'",
    ).bind(org_graph_id).fetch_one(&control).await.unwrap();
    assert_eq!(tenant_original, "Speaker 2: the tenant meeting raw text");
    assert_eq!(control_original, "CONTROL DECOY transcript — must never leak");

    // ── 2. POST /v1/transcript-corrections (routes/meetings.rs::
    //    create_speaker_relabel) — the correction row must land in the TENANT
    //    database only. ──────────────────────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(req!(
            "POST",
            "/v1/transcript-corrections",
            json!({
                "graph_id": org_graph_id,
                "source_id": "meeting-1",
                "surface_form": "Speaker 2",
                "canonical": "Alice",
            })
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "create_speaker_relabel must succeed under Org decision"
    );
    let tenant_corrections: i64 =
        sqlx::query_scalar("SELECT count(*) FROM transcript_corrections WHERE graph_id = $1")
            .bind(org_graph_id)
            .fetch_one(&tenant_pool)
            .await
            .unwrap();
    assert_eq!(tenant_corrections, 1, "the relabel must land in the TENANT database");
    let control_corrections: i64 =
        sqlx::query_scalar("SELECT count(*) FROM transcript_corrections WHERE graph_id = $1")
            .bind(org_graph_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        control_corrections, 0,
        "the relabel must NEVER land in the control/consumer database"
    );

    // ── 3. GET /v1/sessions (routes/sessions.rs::list_sessions) — must list
    //    the TENANT coding session, never the control decoy with the same
    //    (graph_id, session_id). ─────────────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/sessions?graph_id={org_graph_id}")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "list_sessions must succeed under Org decision");
    let sessions = body_json(resp).await;
    let items = sessions["sessions"].as_array().expect("sessions array");
    assert_eq!(items.len(), 1, "must see exactly the tenant-DB session: {sessions:?}");
    assert_eq!(
        items[0]["title"],
        json!("Tenant session"),
        "list_sessions must return the TENANT row, never the control decoy: {sessions:?}"
    );

    // ── 4. Whole-original cloud links require selected evidence consent.
    // Seed one explicitly historical tenant-only share: this is not successful
    // POST creation. Refusal must neither add/modify shares nor expose bytes.
    let share_token = Uuid::new_v4().to_string();
    let historical_share: Value = sqlx::query_scalar(
        "INSERT INTO meeting_shares(graph_id,source_id,token,visibility,created_by) \
         VALUES($1,'meeting-1',$2,'link',$3) RETURNING to_jsonb(meeting_shares)",
    )
    .bind(org_graph_id).bind(&share_token).bind(member_user_id)
    .fetch_one(&tenant_pool).await.unwrap();
    let mut publication_counts = Vec::new();
    for pool in [&tenant_pool, &control] {
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM atoms WHERE graph_id=$1), \
             (SELECT count(*) FROM source_items WHERE graph_id=$1), \
             (SELECT count(*) FROM source_versions WHERE graph_id=$1), \
             (SELECT count(*) FROM source_version_chunks WHERE graph_id=$1)",
        ).bind(org_graph_id).fetch_one(pool).await.unwrap();
        publication_counts.push(counts);
    }
    let refusal = json!({"error":"Whole-original links require selected evidence consent",
                         "code":"selected_evidence_consent_required"});
    let resp = app
        .clone()
        .oneshot(req!(
            "POST",
            "/v1/meetings/shares",
            json!({"graph_id": org_graph_id, "source_id": "meeting-1", "visibility": "link"})
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT, "cloud whole-original creation is refused");
    assert_eq!(body_json(resp).await, refusal);
    let tenant_shares: i64 =
        sqlx::query_scalar("SELECT count(*) FROM meeting_shares WHERE graph_id = $1")
            .bind(org_graph_id).fetch_one(&tenant_pool).await.unwrap();
    assert_eq!(tenant_shares, 1, "only the explicit historical tenant share may remain");
    let unchanged_share: Value = sqlx::query_scalar(
        "SELECT to_jsonb(s) FROM meeting_shares s WHERE graph_id=$1 AND token=$2",
    ).bind(org_graph_id).bind(&share_token).fetch_one(&tenant_pool).await.unwrap();
    assert_eq!(unchanged_share, historical_share, "refused POST must not mutate the historical share");
    let control_shares: i64 =
        sqlx::query_scalar("SELECT count(*) FROM meeting_shares WHERE graph_id = $1")
            .bind(org_graph_id).fetch_one(&control).await.unwrap();
    assert_eq!(control_shares, 0, "the share row must NEVER land in the control database");

    // The guard precedes token lookup. Known tenant and unknown tokens have
    // the exact same anonymous refusal, without a token-existence signal.
    let unknown_token = Uuid::new_v4().to_string();
    for public_token in [&share_token, &unknown_token] {
        let resp = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(format!("/v1/shared/{public_token}"))
                    .header("host", APEX)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(resp).await, refusal);
    }
    for (pool, before) in [&tenant_pool, &control].into_iter().zip(publication_counts) {
        let after: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM atoms WHERE graph_id=$1), \
             (SELECT count(*) FROM source_items WHERE graph_id=$1), \
             (SELECT count(*) FROM source_versions WHERE graph_id=$1), \
             (SELECT count(*) FROM source_version_chunks WHERE graph_id=$1)",
        ).bind(org_graph_id).fetch_one(pool).await.unwrap();
        assert_eq!(after, before, "refused whole-original links must not copy source content on either plane");
    }
    for (pool, original) in [(&tenant_pool, &tenant_original), (&control, &control_original)] {
        let retained: String = sqlx::query_scalar(
            "SELECT raw_inline FROM meeting_transcripts WHERE graph_id=$1 AND source_id='meeting-1'",
        ).bind(org_graph_id).fetch_one(pool).await.unwrap();
        assert_eq!(&retained, original, "refused projections/publication retain each private original");
    }

    // ── 5. GET /v1/sync/pull (routes/sync.rs::pull) — must serve the TENANT
    //    op_log row; the control decoy op (same graph_id, same-account mirror
    //    graph, a perfectly plausible wrong-plane match) must never appear. ──
    let resp = app.clone().oneshot(req!("GET", "/v1/sync/pull?after=0")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "sync pull must succeed under Org decision");
    let pulled = body_json(resp).await;
    let ops = pulled["ops"].as_array().expect("ops array");
    assert_eq!(ops.len(), 1, "must see exactly the tenant-DB op: {pulled:?}");
    assert_eq!(
        ops[0]["payload"]["marker"],
        json!("tenant-op"),
        "sync pull must serve the TENANT op, never the control decoy: {pulled:?}"
    );

    // ── 6. POST /v1/sync/push (routes/sync.rs::push) — an UNSEEN graph_id
    //    under an Org decision must be REFUSED, never auto-provisioned.
    //
    //    Group-B security fix (PR 0 of the member-private-oceans plan
    //    §1.4(c)/item 5): this step used to assert the OPPOSITE — that an
    //    unseen `graph_id` auto-provisions in the tenant DB — which is
    //    exactly the exposure the plan's item 5 closes: the auto-created row
    //    takes the `graphs` table's default `kind='personal'`, and a
    //    `kind='personal'` ocean inside an org account is returned to every
    //    org member by the reachable-ocean union (ADR-0007 D2). The
    //    intentional, founder-approved behavior now is a clean refusal — see
    //    `routes/sync.rs::push`'s org-account comment. This is a deliberate
    //    behavior change, not a regression in this test. ────────────────────
    let pushed_graph = Uuid::new_v4();
    let pushed_atom = Uuid::new_v4();
    let resp = app
        .clone()
        .oneshot(req!(
            "POST",
            "/v1/sync/push",
            json!({
                "ops": [{
                    "seq": 0,
                    "op_id": Uuid::new_v4(),
                    "entity": "atom",
                    "entity_id": pushed_atom.to_string(),
                    "op_type": "upsert",
                    "payload": {
                        "atom_id": pushed_atom,
                        "graph_id": pushed_graph,
                        "source_type": "note",
                        "source_id": "push-1",
                        "content_blob_key": "unused",
                        "content_inline": "pushed from an org device",
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    },
                    "version": 1,
                    "updated_at": chrono::Utc::now().to_rfc3339(),
                    "device_id": "desktop-1",
                    "graph_id": pushed_graph,
                }]
            })
        ))
        .await
        .unwrap();
    let status = resp.status();
    let push_body = body_json(resp).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "sync push must refuse to auto-provision an unseen graph_id under an Org decision: {push_body:?}"
    );

    let graph_in_tenant: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id = $1)")
            .bind(pushed_graph)
            .fetch_one(&tenant_pool)
            .await
            .unwrap();
    assert!(!graph_in_tenant, "the refused push must not auto-provision a graph in the TENANT database");
    let atom_in_tenant: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM atoms WHERE atom_id = $1)")
            .bind(pushed_atom)
            .fetch_one(&tenant_pool)
            .await
            .unwrap();
    assert!(!atom_in_tenant, "the refused push must not land the pushed atom in the TENANT database either");
    let graph_in_control: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id = $1)")
            .bind(pushed_graph)
            .fetch_one(&control)
            .await
            .unwrap();
    assert!(!graph_in_control, "push must NEVER provision a graph in the control database");
    let atom_in_control: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM atoms WHERE atom_id = $1)")
            .bind(pushed_atom)
            .fetch_one(&control)
            .await
            .unwrap();
    assert!(!atom_in_control, "the pushed atom must NEVER land in the control database");

    // ── 7. The chunk's fail-closed org gates: connector-family routes
    //    (including the Teams ingest connector — security-review P0: it is a
    //    Clerk-JWT CONTENT route, NOT a webhook, and must be gated like its
    //    siblings — and the JWT-authed OAuth-connection routes, whose
    //    mint_state would otherwise launder an org account id into a
    //    consumer-plane callback write) and named teams must 403 under an
    //    Org decision (never a silent consumer-plane read/write). ───────────
    for (method, uri, body) in [
        ("POST", "/v1/connectors/microsoft/sync", None),
        ("GET", "/v1/connectors/microsoft/workflows", None),
        ("GET", "/v1/calendar/status", None),
        ("GET", "/v1/integrations/slack", None),
        ("POST", "/v1/teams", Some(json!({"name": "Org team"}))),
        ("GET", "/v1/connectors/microsoft/teams", None),
        (
            "POST",
            "/v1/connectors/microsoft/teams/ingest",
            Some(json!({"kind": "channel", "team_id": "t-1", "channel_id": "c-1"})),
        ),
        ("GET", "/v1/connectors/microsoft/teams/watched", None),
        ("GET", "/v1/oauth/state", None),
        ("GET", "/v1/oauth/connections", None),
    ] {
        let request = match body {
            Some(b) => req!(method, uri, b),
            None => req!(method, uri),
        };
        let resp = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "{method} {uri} must fail closed (403) under an Org decision"
        );
    }
    // Broad transcript discovery is retired for every account, including orgs.
    // Its constant response performs no calendar/provider or database work;
    // live connector endpoints above must still deny this org request with 403.
    let resp = app.clone().oneshot(req!(
        "POST", "/v1/connectors/microsoft/teams/transcripts/sync"
    )).await.unwrap();
    assert_eq!(resp.status(), StatusCode::GONE,
        "retired broad transcript sync must return its exact 410 contract");
    assert_eq!(body_json(resp).await, json!({
        "error": "Select a specific meeting and explicitly enable capture. Calendar and recording do not authorize transcript capture."
    }), "the retired response must contain only the static retirement notice");

    // The live 403 denials and retired 410 wrote nothing anywhere: no watch rows, no oauth token
    // rows, and no graphs beyond the seed + push — on EITHER plane. (The
    // teams-ingest POST above carries a fully-valid body, so anything short
    // of the org gate would have written a teams_watched_resources row and
    // spawned a backfill.)
    for (pool, plane) in [(&tenant_pool, "tenant"), (&control, "control")] {
        let watches: i64 = sqlx::query_scalar("SELECT count(*) FROM teams_watched_resources")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(watches, 0, "org-gated teams ingest must write no watch rows ({plane})");
        let tokens: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(tokens, 0, "org-gated oauth routes must write no token rows ({plane})");
    }
    let tenant_graphs: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs")
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
    assert_eq!(
        tenant_graphs, 1,
        "org-gated routes AND the now-refused sync push must not have created any tenant graphs \
         beyond the original seed"
    );

    // Cleanup. Env vars restored automatically when `_env_guard` drops at
    // the end of this function — including on panic unwind, not just this
    // happy path (see `EnvGuard`'s own doc comment).
    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// org-slack step 5: the org fail-closed gate on Slack OAuth
/// (`routes::oauth::reject_org_oauth_management` / `routes::slack::
/// reject_org_slack_management`, plus the new org/provider backstop inside
/// `routes::oauth::callback`) is lifted for Slack ONLY, behind
/// `ORG_SLACK_OAUTH_ENABLED` — through the REAL public HTTP router, under a
/// REAL `TenancyDecision::Org`, against two real Postgres databases (control
/// + tenant), same harness shape as the chunk-C test above.
///
/// Decoy strategy (same as chunk-B/-C): the TENANT database carries
/// `oauth_tokens`/`slack_integrations` rows for the SAME org account and the
/// SAME provider, but a DIFFERENT (obviously fake) workspace — proving
/// `list_connections`/`list_integrations`/`delete_connection`/
/// `delete_integration` (post-fix: `state.control_db()`, never `tenant.pool`)
/// only ever read/write the CONTROL row, never the tenant one.
///
/// Specifically proves:
/// 1.  Flag OFF: `mint_state`, `list_connections`, `delete_connection`,
///     `list_integrations`, `delete_integration` all still 403 for an org
///     account — byte-identical to before this PR — and touch no row.
/// 2.  Flag ON: `mint_state` succeeds (200, a real signed state token) — "an
///     org account cannot even begin a Slack install" is no longer true.
/// 3.  Flag ON: `list_connections` / `list_integrations` return the CONTROL
///     row, never the tenant decoy.
/// 4.  Flag ON: `GET /v1/oauth/slack/callback` with that minted state gets
///     PAST the org gate — it fails at Slack's OWN "not configured" check
///     (503, `SLACK_CLIENT_ID` unset in this test process) rather than 403,
///     deterministically and with NO network call
///     (`SlackConnector::exchange_code` checks its env vars before ever
///     calling out to slack.com).
/// 5.  Flag ON: the SAME minted state against `GET /v1/oauth/google/
///     callback` still 403s — the org/provider backstop inside `callback`
///     itself — and writes nothing on EITHER plane (asserted on the actual
///     row counts, never on an empty response body).
/// 6.  Flag ON: `delete_connection` / `delete_integration` remove the
///     CONTROL rows only; the tenant decoys are never touched.
#[tokio::test]
async fn org_slack_oauth_gate_lift_targets_control_plane_and_stays_slack_only_pg() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_slack_oauth_gate_lift_targets_control_plane_and_stays_slack_only_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // `EnvGuard` holds `ENV_LOCK` for the whole test body (serializing the
    // CLERK_*/OAUTH_STATE_SECRET/SLACK_*/ORG_SLACK_OAUTH_ENABLED env
    // mutations against sibling tests) AND restores every one of them on
    // Drop — including on a panicking assertion's unwind. This is the exact
    // fix for how THIS test's own earlier panic (before #731's cache fix)
    // used to leak `ORG_SLACK_OAUTH_ENABLED=1` into
    // `org_decision_chunk_c_routes_read_and_write_only_the_tenant_db`, which
    // runs later in the same process and never itself touches that var —
    // see `EnvGuard`'s own doc comment.
    let jwks_url = spawn_local_jwks().await;
    let mut env_guard = EnvGuard::acquire(&[
        ("CLERK_ISSUER", ISSUER),
        ("CLERK_JWKS_URL", &jwks_url),
        ("OAUTH_STATE_SECRET", "orgt-route-slackgate-test-secret-not-real"),
    ])
    .await;
    // SAFETY: `env_guard` holds `ENV_LOCK` for the rest of this test body.
    unsafe {
        env_guard.unset("SLACK_CLIENT_ID");
        env_guard.unset("SLACK_CLIENT_SECRET");
        env_guard.unset("ORG_SLACK_OAUTH_ENABLED");
    }

    let (ctl, tenant_db) = ("orgt_route_slackgate_ctl", "orgt_route_slackgate_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_4 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + org account + routing row + the REAL
    //    install rows (oauth_tokens, connector_installs, slack_integrations)
    //    — exactly what a completed install would have written. ────────────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let dummy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'org-owner@slackgate.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, 'org_slackgate') RETURNING id",
    )
    .bind(dummy_owner)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        org_account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_slackgate', $1, 'slackgate', 'SECRET_SLACKGATE', 'active')",
        )
        .bind(org_account_id),
    )
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO oauth_tokens(account_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, 'slack', 'channels:history', '\\x00'::bytea, 'T-REAL')",
    )
    .bind(org_account_id)
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO connector_installs(provider, external_workspace_id, account_id, clerk_org_id) \
         VALUES ('slack', 'T-REAL', $1, 'org_slackgate')",
    )
    .bind(org_account_id)
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO slack_integrations(account_id, workspace_id, workspace_name, bot_token) \
         VALUES ($1, 'T-REAL', 'Real Workspace', 'xoxb-real')",
    )
    .bind(org_account_id)
    .execute(&control)
    .await
    .unwrap();

    // ── Tenant DB: tenant tree + fingerprint + DECOY rows — same account_id,
    //    same provider, a DIFFERENT workspace. Must never leak or be
    //    touched by anything below. ─────────────────────────────────────────
    let tenant_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, tenant_db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_slackgate')")
        .execute(&tenant_pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO oauth_tokens(account_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, 'slack', 'decoy-scope', '\\x01'::bytea, 'T-DECOY')",
    )
    .bind(org_account_id)
    .execute(&tenant_pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO slack_integrations(account_id, workspace_id, workspace_name, bot_token) \
         VALUES ($1, 'T-DECOY', 'TENANT DECOY workspace — must never leak', 'xoxb-decoy')",
    )
    .bind(org_account_id)
    .execute(&tenant_pool)
    .await
    .unwrap();

    // ── AppState + app: control pool as the ambient plane, real enabled
    //    TenantRuntime resolving SECRET_SLACKGATE to the tenant Postgres. ───
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_SLACKGATE", with_db(&base, tenant_db))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let member_sub = format!("route_migration_org_pg_slackgate_member_{}", Uuid::new_v4());
    // `sid`: these stand in for real browser sessions (every Clerk session
    // JWT carries one). Without it the token classifies as a non-interactive
    // OAuth credential, which the org-admin gate (settings PR 2.8) refuses on
    // the connect/disconnect routes this test exercises.
    let token = mint(json!({
        "iss": ISSUER,
        "sub": member_sub,
        "exp": future_exp(),
        "sid": format!("sess_{member_sub}"),
        "org_id": "org_slackgate",
        "org_role": "org:member",
    }));
    // The connect/disconnect half of the surface is admin-gated
    // (`reject_org_slack_non_admin`), so the flag-lift assertions below need
    // BOTH voices: the member proves reads stay open and writes 403, the
    // admin proves the lift actually opens the write path.
    let admin_sub = format!("route_migration_org_pg_slackgate_admin_{}", Uuid::new_v4());
    let admin_token = mint(json!({
        "iss": ISSUER,
        "sub": admin_sub,
        "exp": future_exp(),
        "sid": format!("sess_{admin_sub}"),
        "org_id": "org_slackgate",
        "org_role": "org:admin",
    }));
    // Claim-based tenancy Phase 4 cutover step 3: the org API address
    // `<slug>.api.<apex>` is retired (410). The apex is now every tenant's
    // door and the `org_id` claim on the token below is what names the
    // tenant, so this file's question — "does an org decision route through
    // the TENANT pool and never the control plane?" — is unchanged; only the
    // door is. The tenant-vs-control assertions are what have teeth here,
    // and they are untouched.
    let org_host = APEX;

    macro_rules! req {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
    }
    macro_rules! admin_req {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {admin_token}"))
                .body(Body::empty())
                .unwrap()
        };
    }

    // ── 1. Flag OFF: every gate 403s, byte-identical to before this PR. ──────
    for (method, uri) in [
        ("GET", "/v1/oauth/state"),
        ("GET", "/v1/oauth/connections"),
        ("DELETE", "/v1/oauth/connections/slack"),
        ("GET", "/v1/integrations/slack"),
        ("DELETE", "/v1/integrations/slack/T-REAL"),
    ] {
        let resp = app.clone().oneshot(req!(method, uri)).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "{method} {uri} must still 403 under an org account with ORG_SLACK_OAUTH_ENABLED unset"
        );
    }
    let control_tokens_before: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens")
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        control_tokens_before, 1,
        "the flag-off 403s above must not have touched the seeded control row"
    );

    // ── 2. Flag ON. ────────────────────────────────────────────────────────
    // SAFETY: `env_guard` still holds `ENV_LOCK` (acquired above, held for
    // this whole test body).
    unsafe {
        env_guard.set("ORG_SLACK_OAUTH_ENABLED", "1");
    }

    // 2a. mint_state now succeeds FOR AN ADMIN — "an org account cannot even
    //     begin a Slack install today" is no longer true.
    //
    //     A plain member ALSO succeeds at mint_state itself (fixed alongside
    //     #731, correcting this test's own stale expectation): m365 Train 3
    //     PR 3.3 (commit c120d485, #693 — dated 2026-08-24, AFTER this test
    //     was last touched by #658 on 2026-08-21) deliberately removed
    //     `reject_org_slack_non_admin` from `mint_state` — see that
    //     function's own doc comment and `reject_org_oauth_management`'s.
    //     `mint_state` is provider-blind by construction (Microsoft member
    //     connects are self-service, so it cannot gate on "is this an
    //     admin-only provider" before the provider is even chosen), and the
    //     admin-only rule for SLACK SPECIFICALLY was re-enforced one layer
    //     down instead: `callback`'s own org/provider gate re-checks the
    //     signed state's `uid` against a LIVE `account_members.role` lookup
    //     (fail-closed) before ever exchanging the code — see 2a-bis right
    //     below, which proves that re-enforcement directly rather than just
    //     asserting mint_state's (now intentionally permissive) status code.
    let resp = app.clone().oneshot(req!("GET", "/v1/oauth/state")).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "mint_state must succeed for a plain org member too once ORG_SLACK_OAUTH_ENABLED is set — \
         it is provider-blind by design (m365 PR 3.3, #693); the admin-only rule for Slack lives at \
         callback now, not here"
    );
    let member_minted = body_json(resp).await;
    let member_minted_state = member_minted["state"]
        .as_str()
        .expect("state token in member mint_state response")
        .to_string();
    let resp = app.clone().oneshot(admin_req!("GET", "/v1/oauth/state")).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "mint_state must succeed for an org ADMIN once ORG_SLACK_OAUTH_ENABLED is set"
    );
    let minted = body_json(resp).await;
    let minted_state = minted["state"].as_str().expect("state token in mint_state response").to_string();

    // 2a-bis. The admin-only rule PR 3.3 moved out of mint_state lives here
    //         instead: a plain member's own validly-minted state is still
    //         refused — 403, never the "SLACK_CLIENT_ID unset" 503 admin_req
    //         hits in 2d below — because `callback`'s org Slack arm re-checks
    //         `account_members.role` (auto-provisioned from this member's
    //         own JWT claims by `require_auth`'s first-sight
    //         `ProvisionFromClaims` policy on the very requests above) and
    //         fails closed on a non-admin. Writes nothing on either plane.
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!(
                    "/v1/oauth/slack/callback?code=dummy-code&state={member_minted_state}"
                ))
                .header("host", org_host)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "a non-admin org member's own minted state must still be refused at callback — this is \
         where PR 3.3 moved Slack's admin-only enforcement to, not mint_state"
    );
    // Body, not just status: `google_org_callback_allowed`'s org/provider
    // gate (a few lines earlier in `callback`) ALSO 403s — for an unrelated
    // reason (an org whose Slack flag is off, or a disallowed provider) —
    // so a bare status-code match here would pass even if this request were
    // wrongly refused by THAT check instead of the admin-only re-check this
    // step exists to prove. Exact string copied from `callback`'s own
    // admin-refusal body (`backend/src/routes/oauth.rs`, the `if !is_admin`
    // arm) so a wording drift there breaks this assertion loudly rather than
    // silently narrowing what it actually proves.
    let member_callback_body = body_json(resp).await;
    assert_eq!(
        member_callback_body["error"],
        json!("connecting Slack for an organization requires an organization admin"),
        "must be refused by callback's admin-only re-check SPECIFICALLY, not by google_org_\
         callback_allowed's differently-worded org/provider refusal (which also 403s): \
         {member_callback_body:?}"
    );
    let member_callback_tokens: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens")
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        member_callback_tokens, 1,
        "the member's refused callback attempt must not have touched the seeded control row"
    );

    // 2b. list_connections reads the CONTROL row, never the tenant decoy.
    let resp = app.clone().oneshot(req!("GET", "/v1/oauth/connections")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "list_connections must succeed once lifted");
    let conns = body_json(resp).await;
    let conns = conns.as_array().expect("connections array");
    assert_eq!(conns.len(), 1, "must see exactly the CONTROL row: {conns:?}");
    assert_eq!(
        conns[0]["provider_workspace_id"],
        json!("T-REAL"),
        "list_connections must read the CONTROL oauth_tokens row, never the tenant decoy: {conns:?}"
    );

    // 2c. list_integrations reads the CONTROL row, never the tenant decoy.
    let resp = app.clone().oneshot(req!("GET", "/v1/integrations/slack")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "list_integrations must succeed once lifted");
    let integrations = body_json(resp).await;
    let workspaces = integrations["workspaces"].as_array().expect("workspaces array");
    assert_eq!(workspaces.len(), 1, "must see exactly the CONTROL row: {workspaces:?}");
    assert_eq!(
        workspaces[0]["workspace_id"],
        json!("T-REAL"),
        "list_integrations must read the CONTROL slack_integrations row, never the tenant decoy: \
         {workspaces:?}"
    );

    // 2d. Slack callback gets PAST the org gate — refused only by Slack's OWN
    //     "not configured" check (503), never the org gate (403). No network
    //     call happens: `SlackConnector::exchange_code` checks
    //     SLACK_CLIENT_ID/SLACK_CLIENT_SECRET before ever calling out.
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!("/v1/oauth/slack/callback?code=dummy-code&state={minted_state}"))
                .header("host", org_host)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "an org account's Slack callback must get PAST the org gate (never 403) once lifted — the \
         only thing left to refuse it here is SLACK_CLIENT_ID being unset in this test process"
    );

    // 2e. A FRESH state against a DIFFERENT provider still 403s — the
    //     org/provider backstop inside `callback` itself — and writes
    //     nothing on either plane (asserted on real row counts, never on an
    //     empty response body). A fresh mint is required here (not the same
    //     `minted_state` reused from 2d): foundation train PR 1.4 gave
    //     `callback` a genuine single-use jti replay guard
    //     (`oauth::jti_store::consume`), so 2d's slack attempt already
    //     consumed `minted_state`'s jti — reusing it here would 400 as a
    //     replay and prove nothing about the org/provider gate this step
    //     exists to test.
    let resp = app
        .clone()
        .oneshot(admin_req!("GET", "/v1/oauth/state"))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "mint_state must succeed again for a second, independent state token"
    );
    let minted_2 = body_json(resp).await;
    let minted_state_2 = minted_2["state"]
        .as_str()
        .expect("state token in second mint_state response")
        .to_string();
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri(format!(
                    "/v1/oauth/google/callback?code=dummy-code&state={minted_state_2}"
                ))
                .header("host", org_host)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "a non-Slack provider must still refuse an org account even with ORG_SLACK_OAUTH_ENABLED set"
    );
    for (pool, plane) in [(&control, "control"), (&tenant_pool, "tenant")] {
        let google_tokens: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE provider = 'google'")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(google_tokens, 0, "the refused google callback must write no oauth_tokens row ({plane})");
    }
    let control_google_installs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM connector_installs WHERE provider = 'google'")
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        control_google_installs, 0,
        "the refused google callback must write no connector_installs row (control — the table \
         does not exist on the tenant tree at all)"
    );

    // 2f. delete_connection — the ONLY disconnect the UI exposes — must leave
    //     NOTHING behind that can keep ingesting: the `oauth_tokens` row, the
    //     legacy `slack_integrations` row (whose `bot_token` is what
    //     `ingest_slack_message` actually reads), and the `connector_installs`
    //     routing row that maps the workspace back to this account.
    //
    //     Before the fix this removed `oauth_tokens` alone. The other two
    //     survived — `connector_installs` precisely BECAUSE `slack_integrations`
    //     did — so a user who clicked "Disconnect" kept having their Slack
    //     messages ingested. This step used to assert only the token count and
    //     so was fully green while that was true.
    let resp = app.clone().oneshot(req!("DELETE", "/v1/oauth/connections/slack")).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "delete_connection must still refuse a plain org member — disconnect is an admin act"
    );
    let resp = app.clone().oneshot(admin_req!("DELETE", "/v1/oauth/connections/slack")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "delete_connection must succeed for an org ADMIN once lifted");
    let control_tokens_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE provider = 'slack'")
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(control_tokens_after, 0, "delete_connection must remove the CONTROL oauth_tokens row");
    let control_integrations_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM slack_integrations WHERE workspace_id = 'T-REAL'")
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        control_integrations_after, 0,
        "delete_connection must ALSO remove the legacy slack_integrations row — its bot_token is \
         what ingest_slack_message reads, so leaving it means messages keep flowing after the \
         user disconnected"
    );
    let control_installs_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM connector_installs WHERE provider = 'slack'")
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        control_installs_after, 0,
        "with both token rows gone, delete_connection's own guard no longer holds the \
         connector_installs routing row back — the workspace must no longer resolve to this \
         account at all"
    );

    // 2g. delete_integration now has nothing left to remove — 404, not 200.
    //     It keeps its own mirror-image guard for any direct API caller, but
    //     the UI path above already tore everything down. The admin gate
    //     outranks the emptiness: a member gets 403 before the 404 lookup.
    let resp = app.clone().oneshot(req!("DELETE", "/v1/integrations/slack/T-REAL")).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "delete_integration must refuse a plain org member before it ever looks for the row"
    );
    let resp = app.clone().oneshot(admin_req!("DELETE", "/v1/integrations/slack/T-REAL")).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "delete_connection already removed the slack_integrations row, so delete_integration has \
         nothing left to delete"
    );

    // 2h. The TENANT decoys were NEVER touched by anything above.
    let tenant_tokens: (String, String) =
        sqlx::query_as("SELECT scope, provider_workspace_id FROM oauth_tokens WHERE account_id = $1")
            .bind(org_account_id)
            .fetch_one(&tenant_pool)
            .await
            .unwrap();
    assert_eq!(
        tenant_tokens,
        ("decoy-scope".to_string(), "T-DECOY".to_string()),
        "the tenant decoy oauth_tokens row must be untouched by every op above"
    );
    let tenant_integrations: (String, String) =
        sqlx::query_as("SELECT workspace_id, bot_token FROM slack_integrations WHERE account_id = $1")
            .bind(org_account_id)
            .fetch_one(&tenant_pool)
            .await
            .unwrap();
    assert_eq!(
        tenant_integrations,
        ("T-DECOY".to_string(), "xoxb-decoy".to_string()),
        "the tenant decoy slack_integrations row must be untouched by every op above"
    );

    // Cleanup. Env vars restored automatically when `env_guard` drops at the
    // end of this function — including on panic unwind, not just this happy
    // path (see `EnvGuard`'s own doc comment).
    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// `AppState::personal_web_base_url` / `org_web_base_url` / `web_base_url` — the
/// one place `routes::oauth::callback` (including the Teams/Outlook
/// meeting-pane Microsoft-connect return, `/teams/meeting/connected` and
/// `/outlook/meeting/connected`), the digest/nudge emails and the
/// meeting/session share-link builders get a user-facing URL from. Exercised
/// directly against a real Postgres control database (no router, no tenant DB
/// needed — these methods never touch the tenant plane).
///
/// **Per-org web subdomains are retired** — everything is served from
/// `app.{web_apex}`, the org being named by the token's `org_id` claim. This
/// used to prove an org resolved to its OWN `https://{slug}.{web_apex}`; that
/// assertion was the bug (it sent a browser to a host that no longer serves a
/// UI). Specifically proves:
/// 1. Personal (no `clerk_org_id`) resolves to `https://app.{web_apex}` once
///    the runtime is enabled.
/// 2. An org WITH a healthy `active` routing row resolves to the SAME
///    `https://app.{web_apex}` — never `{slug}.{web_apex}`, never the API apex.
/// 3. A `suspended` org, 4. an org with NO routing row at all — identical: the
///    answer no longer depends on the routing row (or on there being one).
/// 5. With the runtime DISABLED (`MULTITENANT_ENABLED` off, matching
///    pre-cutover / today's default), BOTH personal and org collapse to the
///    same `FRONTEND_URL`-derived base — the legacy single-value behavior,
///    untouched.
#[tokio::test]
async fn web_base_url_lands_every_plane_on_the_apex_app_host_never_a_subdomain_or_the_api_apex_pg() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping web_base_url_lands_every_plane_on_the_apex_app_host_never_a_subdomain_or_the_api_apex_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // Held for the whole test body — guards the FRONTEND_URL mutation in
    // step 5 below against sibling tests in this binary (see ENV_LOCK).
    let _env_guard = ENV_LOCK.lock().await;

    let ctl = "orgt_route_webbase_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_5 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    // ── Seed three orgs: one healthy, one suspended, one with NO routing row
    //    at all (never seeded into tenant_databases). ─────────────────────────
    async fn seed_org(control: &PgPool, clerk_org_id: &str, slug: &str, status: &str) {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("{clerk_org_id}@webbase.example"))
        .fetch_one(control)
        .await
        .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(clerk_org_id)
        .fetch_one(control)
        .await
        .unwrap();
        financial_fixture::execute(
            control,
            account_id,
            sqlx::query(
                "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(clerk_org_id)
            .bind(account_id)
            .bind(slug)
            .bind(format!("SECRET_{}", slug.to_uppercase()))
            .bind(status),
        )
        .await
        .unwrap();
    }
    seed_org(&control, "org_webbase_healthy", "orgwebhealthy", "active").await;
    seed_org(&control, "org_webbase_suspended", "orgwebsuspended", "suspended").await;
    // "org_webbase_orphan" is deliberately never inserted into tenant_databases.

    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
        Vec::<(String, String)>::new(),
    ))));

    // ── 1/2/3/4: runtime ENABLED, api apex and web apex deliberately
    //    DIFFERENT so a test that accidentally used the wrong one is caught. ──
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.tenant_runtime = Some(TenantRuntime::new_with_web_apex(
        true,
        "api.webbase-test.example",
        "webbase-test.example",
        registry.clone(),
    ));

    // 1. Personal.
    assert_eq!(
        state.personal_web_base_url(),
        "https://app.webbase-test.example",
        "personal base must be app.{{web_apex}}, never the api apex"
    );

    // 2/3/4. Healthy, suspended and routing-less orgs ALL land on the apex app
    //    host — never `{slug}.{web_apex}` (retired), never the API apex.
    for org in ["org_webbase_healthy", "org_webbase_suspended", "org_webbase_orphan"] {
        assert_eq!(
            state.org_web_base_url(&control, org).await,
            "https://app.webbase-test.example",
            "{org}: an org link must land on app.{{web_apex}}, not an org subdomain"
        );
    }

    // Same assertions again through the single dispatcher every real call
    // site actually uses.
    assert_eq!(
        state.web_base_url(&control, None).await,
        "https://app.webbase-test.example"
    );
    assert_eq!(
        state.web_base_url(&control, Some("org_webbase_healthy")).await,
        "https://app.webbase-test.example"
    );

    // ── 5. Runtime DISABLED: both collapse to the legacy FRONTEND_URL value,
    //    never an apex-derived host — the pre-cutover / MULTITENANT_ENABLED-
    //    off byte-identical path. ──────────────────────────────────────────
    state.tenant_runtime = Some(TenantRuntime::new_with_web_apex(
        false,
        "api.webbase-test.example",
        "webbase-test.example",
        registry,
    ));
    // SAFETY: env mutation serialized process-wide by `_env_guard`.
    unsafe {
        std::env::set_var("FRONTEND_URL", "https://legacy-webbase-test.example");
    }
    assert_eq!(
        state.personal_web_base_url(),
        "https://legacy-webbase-test.example",
        "a disabled runtime must fall back to FRONTEND_URL, never app.{{web_apex}}"
    );
    assert_eq!(
        state.org_web_base_url(&control, "org_webbase_healthy").await,
        "https://legacy-webbase-test.example",
        "a disabled runtime must fall back to FRONTEND_URL for an org too, never {{slug}}.{{web_apex}}"
    );
    unsafe {
        std::env::remove_var("FRONTEND_URL");
    }

    // Cleanup.
    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {ctl} WITH (FORCE)").as_str())
        .await
        .ok();
}
