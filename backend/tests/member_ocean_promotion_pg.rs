//! Route-level cloud legacy-promotion refusal tests for member-private oceans.
//!
//! Uses the real public router, Clerk-shaped JWTs and separate control/tenant
//! Postgres databases. P8 requires exact retained-passage consent through
//! `/v1/sources/publications`; legacy reef/source promotion is refused in cloud
//! before copying private content or examining a target's kind/reachability.
//! The org source-owner/database boundary still runs first: foreign member
//! sources remain forbidden. Poolless/local promotion is a separate contract.
//! See `docs/implementation/library-selected-publication-api.md` and merged
//! P8 (#1236), with the boundary-order correction in #1243.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights); every test silently returns (not fails) when it is
//! unset, exactly like its sibling `_pg` suites. **This suite IS wired into
//! CI** — `.github/workflows/ci.yml`'s `cross-tenant-probe` job (which
//! already runs a CREATE DATABASE-capable Postgres service) runs it
//! explicitly via `--test member_ocean_promotion_pg`.
//!
//! Run locally with:
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test member_ocean_promotion_pg -- --nocapture

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

/// Serializes the shared-process `CLERK_ISSUER`/`CLERK_JWKS_URL` env
/// mutations across the tests in this binary (same convention as
/// `route_migration_org_pg.rs`'s own `ENV_LOCK`).
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

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

/// Spin up a local JWKS listener serving the shared test keypair (same
/// fixture `route_migration_org_pg.rs`/`mcp_pg_e2e.rs`/`mcp_http.rs` use,
/// duplicated here for the same separate-compilation-unit reason).
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

// ── Fixture ───────────────────────────────────────────────────────────────────
//
// Two org members (A, B), each with their own owner-scoped `kind='member'`
// tenant graph + one reef; and an org commons ocean. A consumer-plane personal
// ocean + reef for member A is still seeded: it is no longer a promotable
// source (that is now a 403, proven in `cross_tenant_probe_pg.rs`), but its
// presence keeps these tests honest — the same-pool assertions below hold with
// a consumer-plane ocean sitting right there, unreachable.
//
// Member users are pre-seeded directly (fixed `id` + `clerk_user_id`) rather
// than left to `ensure_user`'s first-sight auto-provisioning: the member
// ocean graphs below need a real `owner_user_id` to exist BEFORE the first
// HTTP request, and `ensure_user`'s early `SELECT id FROM users WHERE
// clerk_user_id = $1` finds this row immediately, so first-sight provisioning
// never runs (same effect, just with a known id in hand ahead of time) —
// `account_members` is still left to auto-provision on first request
// (`OrgMembershipPolicy::ProvisionFromClaims`), matching every other org
// route test's convention.
// Not every field is read by every test in this file (each test exercises a
// different slice of the shared fixture) — `#[allow(dead_code)]` rather than
// pruning fields that document the fixture's full shape for future tests.
#[allow(dead_code)]
struct Fx {
    control: PgPool,
    tenant: PgPool,
    org_graph: Uuid,
    member_a: Uuid,
    member_a_sub: String,
    member_a_graph: Uuid,
    member_a_node: Uuid,
    member_a_source_id: String,
    member_b: Uuid,
    member_b_sub: String,
    member_b_graph: Uuid,
    member_b_node: Uuid,
    member_b_source_id: String,
    personal_node: Uuid,
}

async fn setup(base: &str, ctl: &str, ten: &str, org_id: &str) -> Fx {
    let mut conn = admin(base).await;
    for db in [ctl, ten] {
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + org account + routing row + two member
    //    users + a consumer-plane personal ocean for member A. ────────────────
    let control: PgPool = PgPoolOptions::new()
        .connect(&with_db(base, ctl))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control")
        .run(&control)
        .await
        .unwrap();

    let org_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("org-owner-{}@test.local", Uuid::new_v4()))
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(org_owner)
    .bind(org_id)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        org_account,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ($1, $2, 'memberoceans', 'SECRET_MO', 'active')",
        )
        .bind(org_id)
        .bind(org_account),
    )
    .await
    .unwrap();

    let member_a = Uuid::new_v4();
    let member_a_sub = format!("member_ocean_pg_a_{member_a}");
    sqlx::query("INSERT INTO users(id, email, clerk_user_id) VALUES ($1, $2, $3)")
        .bind(member_a)
        .bind(format!("a-{member_a}@test.local"))
        .bind(&member_a_sub)
        .execute(&control)
        .await
        .unwrap();
    let member_b = Uuid::new_v4();
    let member_b_sub = format!("member_ocean_pg_b_{member_b}");
    sqlx::query("INSERT INTO users(id, email, clerk_user_id) VALUES ($1, $2, $3)")
        .bind(member_b)
        .bind(format!("b-{member_b}@test.local"))
        .bind(&member_b_sub)
        .execute(&control)
        .await
        .unwrap();

    // Member A's CONSUMER-plane personal ocean + reef — the ONLY source shape
    // an org-context promotion could serve before this PR.
    let personal_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(member_a)
            .fetch_one(&control)
            .await
            .unwrap();
    let personal_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary) \
         VALUES ($1, 'Personal Ocean', 'personal', true) RETURNING id",
    )
    .bind(personal_account)
    .fetch_one(&control)
    .await
    .unwrap();
    let personal_node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', 'Consumer-Plane Reef', '{}'::jsonb) \
         RETURNING node_id",
    )
    .bind(personal_graph)
    .fetch_one(&control)
    .await
    .unwrap();
    // §4.3 (docs/plans/graph-temporal-memory.md): reef-only promotion is
    // rejected into a shared ocean, so test 4 below (which drives this reef
    // through the cross-DB service) needs a real coral to promote — content
    // travels via `content_inline` (`state.r2` stays `None` in `build_app`),
    // so no blob transport is exercised, matching this file's own module doc.
    let personal_atom: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             \"timestamp\", content_inline) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3) \
         RETURNING atom_id",
    )
    .bind(personal_graph)
    .bind(format!("consumer-plane-source-{personal_node}"))
    .bind("consumer-plane coral")
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(personal_node)
        .bind(personal_atom)
        .execute(&control)
        .await
        .unwrap();

    // ── Tenant DB: tenant tree + org ocean + one owner-scoped member ocean
    //    per member, each with one reef. ───────────────────────────────────────
    let tenant: PgPool = PgPoolOptions::new()
        .connect(&with_db(base, ten))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant")
        .run(&tenant)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&tenant)
        .await
        .unwrap();

    let org_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (id, account_id, name, kind) \
         VALUES (gen_random_uuid(), $1, 'Org Ocean', 'org') RETURNING id",
    )
    .bind(org_account)
    .fetch_one(&tenant)
    .await
    .unwrap();

    // Also links a coral (atom) carrying a deterministic `source_id` to the
    // reef, so `promote_source`'s `node_ids_for_source` lookup — `SELECT
    // DISTINCT na.node_id FROM node_atoms na JOIN atoms a ON a.atom_id =
    // na.atom_id WHERE a.graph_id = $1 AND a.source_id = $2` — has something
    // to find. Without this, `POST /v1/graphs/{id}/promote-source` would
    // always see zero reefs for ANY source_id, member ocean or not, and could
    // never distinguish "the routing fork is broken" from "there was nothing
    // to promote".
    async fn member_graph_and_node(
        tenant: &PgPool,
        org_account: Uuid,
        owner: Uuid,
        label: &str,
    ) -> (Uuid, Uuid, String) {
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (id, account_id, name, kind, owner_user_id, is_primary) \
             VALUES (gen_random_uuid(), $1, $2, 'member', $3, true) RETURNING id",
        )
        .bind(org_account)
        .bind(format!("{label}'s private ocean"))
        .bind(owner)
        .fetch_one(tenant)
        .await
        .unwrap();
        let node: Uuid = sqlx::query_scalar(
            "INSERT INTO nodes (node_id, graph_id, type, title, body) \
             VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
        )
        .bind(graph)
        .bind(format!("{label}'s private reef"))
        .fetch_one(tenant)
        .await
        .unwrap();
        let source_id = format!("{label}-source");
        let atom: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                                 \"timestamp\", content_inline) \
             VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3) \
             RETURNING atom_id",
        )
        .bind(graph)
        .bind(&source_id)
        .bind(format!("{label}'s private coral"))
        .fetch_one(tenant)
        .await
        .unwrap();
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node)
            .bind(atom)
            .execute(tenant)
            .await
            .unwrap();
        (graph, node, source_id)
    }

    let (member_a_graph, member_a_node, member_a_source_id) =
        member_graph_and_node(&tenant, org_account, member_a, "A").await;
    let (member_b_graph, member_b_node, member_b_source_id) =
        member_graph_and_node(&tenant, org_account, member_b, "B").await;

    Fx {
        control,
        tenant,
        org_graph,
        member_a,
        member_a_sub,
        member_a_graph,
        member_a_node,
        member_a_source_id,
        member_b,
        member_b_sub,
        member_b_graph,
        member_b_node,
        member_b_source_id,
        personal_node,
    }
}

async fn teardown(base: &str, fx: Fx, dbs: [&str; 2]) {
    fx.control.close().await;
    fx.tenant.close().await;
    let mut conn = admin(base).await;
    for db in dbs {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// Build the `AppState` + full HTTP `app` this whole file drives requests
/// through: control pool as the ambient/Personal-plane db+store, and a
/// real `TenantRuntime` whose registry resolves this test's `db_secret_ref`
/// to the tenant Postgres above.
fn build_app(fx: &Fx, base: &str, ten: &str) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(fx.control.clone());
    state.store = Some(Arc::new(PostgresStore::new(fx.control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_MO", with_db(base, ten))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    coralswarm_backend::test_support_app(Arc::new(state))
}

fn mint_member_token(org_id: &str, sub: &str) -> String {
    mint(json!({
        "iss": ISSUER,
        "sub": sub,
        "exp": future_exp(),
        "org_id": org_id,
        "org_role": "org:member",
    }))
}

// Compare actual private bytes, identities, links and publication side effects.
// This is owner-side fixture inspection; authorization is exercised by the HTTP
// calls below. A refusal must leave both member originals and the org untouched.
async fn promotion_content_snapshot(fx: &Fx) -> Value {
    let mut snapshot: Value = sqlx::query_scalar(
        r#"SELECT jsonb_build_object(
          'nodes', COALESCE((SELECT jsonb_agg(to_jsonb(n) ORDER BY n.node_id) FROM nodes n), '[]'::jsonb),
          'atoms', COALESCE((SELECT jsonb_agg(to_jsonb(a) ORDER BY a.atom_id) FROM atoms a), '[]'::jsonb),
          'links', COALESCE((SELECT jsonb_agg(to_jsonb(na) ORDER BY na.node_id, na.atom_id) FROM node_atoms na), '[]'::jsonb),
          'promotions', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.source_node_id, p.target_graph_id) FROM node_promotions p), '[]'::jsonb),
          'connections', COALESCE((SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM source_connections c), '[]'::jsonb),
          'publications', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.publication_id) FROM selected_evidence_publications p), '[]'::jsonb),
          'copy_jobs', COALESCE((SELECT jsonb_agg(to_jsonb(j) ORDER BY j.id) FROM jobs j WHERE j.kind IN ('PromoteNode', 'PublishSource')), '[]'::jsonb)
        )"#,
    )
    .fetch_one(&fx.tenant)
    .await
    .unwrap();
    let account: Uuid = sqlx::query_scalar("SELECT account_id FROM graphs WHERE id=$1")
        .bind(fx.org_graph).fetch_one(&fx.tenant).await.unwrap();
    let financial: Value = sqlx::query_scalar(
        r#"SELECT jsonb_build_object(
          'ledger', COALESCE((SELECT jsonb_agg(to_jsonb(l) ORDER BY l.id) FROM credit_ledger l WHERE account_id=$1), '[]'::jsonb),
          'balances', COALESCE((SELECT jsonb_agg(to_jsonb(b) ORDER BY b.account_id) FROM credit_balances b WHERE account_id=$1), '[]'::jsonb),
          'reservations', COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.plane, r.receipt_id) FROM managed_credit_reservations r WHERE account_id=$1), '[]'::jsonb)
        )"#,
    ).bind(account).fetch_one(&fx.control).await.unwrap();
    snapshot["financial"] = financial;
    let nodes = snapshot["nodes"].as_array().unwrap();
    for (id, title) in [(fx.member_a_node, "A's private reef"), (fx.member_b_node, "B's private reef")] {
        assert!(nodes.iter().any(|n| n["node_id"] == json!(id) && n["title"] == title));
    }
    let atoms = snapshot["atoms"].as_array().unwrap();
    for (graph, text) in [(fx.member_a_graph, "A's private coral"), (fx.member_b_graph, "B's private coral")] {
        assert!(atoms.iter().any(|a| a["graph_id"] == json!(graph) && a["content_inline"] == text));
    }
    for key in ["promotions", "connections", "publications", "copy_jobs"] {
        assert_eq!(snapshot[key], json!([]), "fixture/refusal must have no {key}");
    }
    snapshot
}

async fn assert_selected_consent_refusal(resp: axum::response::Response) {
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(resp).await, json!({
        "error": "Select exact retained passages using /sources/publications",
        "code": "selected_evidence_consent_required",
    }));
}

async fn tenant_node_promotions_count(
    tenant: &PgPool,
    source_node_id: Uuid,
    target_graph_id: Uuid,
) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM node_promotions WHERE source_node_id = $1 AND target_graph_id = $2",
    )
    .bind(source_node_id)
    .bind(target_graph_id)
    .fetch_one(tenant)
    .await
    .unwrap()
}

// ── 1. owned same-tenant reef promotion requires selected-passage consent ────

#[tokio::test]
async fn legacy_member_reef_promotion_requires_selected_consent_without_copying() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping legacy_member_reef_promotion_requires_selected_consent_without_copying: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by `_env_guard`.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ten) = ("orgt_momo_happy_ctl", "orgt_momo_happy_ten");
    let org_id = "org_momo_happy";
    let fx = setup(&base, ctl, ten, org_id).await;
    let app = build_app(&fx, &base, ten);
    let token = mint_member_token(org_id, &fx.member_a_sub);
    // Transport only. Claim-based tenancy Phase 4 cutover step 3 retired the
    // org API address `<slug>.api.<apex>` (410); the org token's `org_id`
    // claim selects the tenant on the apex, and nothing this test asserts is
    // about the Host.
    let host = APEX;

    let before = promotion_content_snapshot(&fx).await;
    let resp = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(format!("/v1/nodes/{}/promote", fx.member_a_node))
                .header("host", host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"target_graph_id": fx.org_graph})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_selected_consent_refusal(resp).await;
    assert_eq!(promotion_content_snapshot(&fx).await, before);
    assert_eq!(
        tenant_node_promotions_count(&fx.tenant, fx.member_a_node, fx.org_graph).await,
        0,
        "a blocked legacy route cannot create same-pool provenance or a target copy"
    );

    teardown(&base, fx, [ctl, ten]).await;
}

// ── 2. member A cannot promote member B's reef ────────────────────────────────

#[tokio::test]
async fn member_a_cannot_promote_member_bs_reef() {
    let Some(base) = base_url() else {
        eprintln!("skipping member_a_cannot_promote_member_bs_reef: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by `_env_guard`.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ten) = ("orgt_momo_crossmember_ctl", "orgt_momo_crossmember_ten");
    let org_id = "org_momo_crossmember";
    let fx = setup(&base, ctl, ten, org_id).await;
    let app = build_app(&fx, &base, ten);
    // Member A's token, naming member B's private reef.
    let token = mint_member_token(org_id, &fx.member_a_sub);
    // Transport only. Claim-based tenancy Phase 4 cutover step 3 retired the
    // org API address `<slug>.api.<apex>` (410); the org token's `org_id`
    // claim selects the tenant on the apex, and nothing this test asserts is
    // about the Host.
    let host = APEX;

    let resp = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(format!("/v1/nodes/{}/promote", fx.member_b_node))
                .header("host", host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"target_graph_id": fx.org_graph})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "member A must never be able to promote member B's private reef: gate 1's kind='member' \
         AND owner_user_id=caller AND archived_at IS NULL check refuses this at the ROUTING step. \
         This asserted 404 before fix/promotion-same-db-only, which was the right outcome for the \
         WRONG reason — the request fell through to the cross-DB service and 404'd only because \
         member B's tenant-only node id happened not to resolve on the consumer plane. It is now \
         a decision, not an accident"
    );
    assert_eq!(
        tenant_node_promotions_count(&fx.tenant, fx.member_b_node, fx.org_graph).await,
        0,
        "nothing must have been published from member B's ocean"
    );
    let node_count: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id = $1")
        .bind(fx.member_b_graph)
        .fetch_one(&fx.tenant)
        .await
        .unwrap();
    assert_eq!(node_count, 1, "member B's ocean must be untouched (still just her own reef)");

    teardown(&base, fx, [ctl, ten]).await;
}

// ── 3. consent refusal precedes own/foreign member-target lookup ─────────────

#[tokio::test]
async fn legacy_reef_promotion_refuses_own_and_foreign_member_targets_before_copy() {
    let Some(base) = base_url() else {
        eprintln!("skipping legacy_reef_promotion_refuses_own_and_foreign_member_targets_before_copy: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by `_env_guard`.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ten) = ("orgt_momo_targetmember_ctl", "orgt_momo_targetmember_ten");
    let org_id = "org_momo_targetmember";
    let fx = setup(&base, ctl, ten, org_id).await;
    let app = build_app(&fx, &base, ten);
    let token = mint_member_token(org_id, &fx.member_a_sub);
    // Transport only. Claim-based tenancy Phase 4 cutover step 3 retired the
    // org API address `<slug>.api.<apex>` (410); the org token's `org_id`
    // claim selects the tenant on the apex, and nothing this test asserts is
    // about the Host.
    let host = APEX;

    // Owned source passes the routing boundary, then cloud consent refuses
    // before either own or foreign target metadata can be looked up.
    let before = promotion_content_snapshot(&fx).await;
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(format!("/v1/nodes/{}/promote", fx.member_a_node))
                .header("host", host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"target_graph_id": fx.member_a_graph})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_selected_consent_refusal(resp).await;
    assert_eq!(
        tenant_node_promotions_count(&fx.tenant, fx.member_a_node, fx.member_a_graph).await,
        0
    );

    // Same exact refusal for a foreign target; disclose no target content.
    let resp2 = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(format!("/v1/nodes/{}/promote", fx.member_a_node))
                .header("host", host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"target_graph_id": fx.member_b_graph})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_selected_consent_refusal(resp2).await;
    assert_eq!(
        tenant_node_promotions_count(&fx.tenant, fx.member_a_node, fx.member_b_graph).await,
        0
    );

    assert_eq!(promotion_content_snapshot(&fx).await, before);
    teardown(&base, fx, [ctl, ten]).await;
}

// ── 4. owned whole-source publication also requires selected consent ────────

#[tokio::test]
async fn legacy_member_source_promotion_requires_selected_consent_without_job() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping legacy_member_source_promotion_requires_selected_consent_without_job: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by `_env_guard`.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ten) = ("orgt_momo_src_happy_ctl", "orgt_momo_src_happy_ten");
    let org_id = "org_momo_src_happy";
    let fx = setup(&base, ctl, ten, org_id).await;
    let app = build_app(&fx, &base, ten);
    let token = mint_member_token(org_id, &fx.member_a_sub);
    // Transport only. Claim-based tenancy Phase 4 cutover step 3 retired the
    // org API address `<slug>.api.<apex>` (410); the org token's `org_id`
    // claim selects the tenant on the apex, and nothing this test asserts is
    // about the Host.
    let host = APEX;

    let before = promotion_content_snapshot(&fx).await;
    let resp = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(format!("/v1/graphs/{}/promote-source", fx.member_a_graph))
                .header("host", host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "source_id": fx.member_a_source_id,
                        "target_graph_id": fx.org_graph,
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_selected_consent_refusal(resp).await;
    assert_eq!(promotion_content_snapshot(&fx).await, before);
    assert_eq!(tenant_node_promotions_count(&fx.tenant, fx.member_a_node, fx.org_graph).await, 0);

    teardown(&base, fx, [ctl, ten]).await;
}

// ── 5. promote_source's half of the fork: member A cannot promote a source
//       out of member B's ocean ───────────────────────────────────────────────

#[tokio::test]
async fn promote_source_member_a_cannot_promote_from_member_bs_ocean() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping promote_source_member_a_cannot_promote_from_member_bs_ocean: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by `_env_guard`.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ten) = ("orgt_momo_src_crossmember_ctl", "orgt_momo_src_crossmember_ten");
    let org_id = "org_momo_src_crossmember";
    let fx = setup(&base, ctl, ten, org_id).await;
    let app = build_app(&fx, &base, ten);
    // Member A's token, naming member B's private OCEAN (not just her reef).
    let token = mint_member_token(org_id, &fx.member_a_sub);
    // Transport only. Claim-based tenancy Phase 4 cutover step 3 retired the
    // org API address `<slug>.api.<apex>` (410); the org token's `org_id`
    // claim selects the tenant on the apex, and nothing this test asserts is
    // about the Host.
    let host = APEX;

    let resp = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri(format!("/v1/graphs/{}/promote-source", fx.member_b_graph))
                .header("host", host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "source_id": fx.member_b_source_id,
                        "target_graph_id": fx.org_graph,
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "member A must never be able to promote a source out of member B's private ocean — \
         gate 1 fails (member_b_graph is not owned by member A), so the routing fork refuses it \
         outright. Was 404 before fix/promotion-same-db-only, for the same accidental reason as \
         the promote_node case above"
    );
    assert_eq!(
        tenant_node_promotions_count(&fx.tenant, fx.member_b_node, fx.org_graph).await,
        0,
        "nothing must have been published from member B's ocean"
    );
    let node_count: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id = $1")
        .bind(fx.member_b_graph)
        .fetch_one(&fx.tenant)
        .await
        .unwrap();
    assert_eq!(node_count, 1, "member B's ocean must be untouched (still just her own reef)");

    teardown(&base, fx, [ctl, ten]).await;
}
