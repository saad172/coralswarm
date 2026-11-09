//! RLS probes for the org-tenancy OPS-4 phase-B.2 route conversion
//! (`routes/{nodes,relationships,conversations,supersession,atlas}.rs`).
//!
//! Two layers of proof, both against a REAL `coralswarm_app` login (the
//! non-BYPASSRLS role OPS-4 cuts the app over to), via the shared
//! `tests/support/rls_probe.rs` harness:
//!
//! 1. **Query-shape probes** — for each converted file, the exact table/filter
//!    shapes its `_scoped` Store calls execute get the harness's three
//!    standard proofs: scoped-to-A returns exactly A's rows; scoped-to-A
//!    aimed at B's ids returns NOTHING; and the same query with
//!    `app.account_id` never set HARD-ERRORS (0083's strict predicate).
//!
//! 2. **Route-level probes** — the real public router (`test_support_app`,
//!    real Clerk-shaped JWTs against a local JWKS listener, the
//!    `route_migration_org_pg.rs` pattern) served entirely off the
//!    `coralswarm_app` pool. This proves the converted handlers themselves
//!    (a) still WORK once every RLS-table query must run inside
//!    `begin_account_scoped` — an unconverted query path would hard-error
//!    here, and (b) fail closed (403/404/empty, never B's data) when caller A
//!    aims at B's resources.
//!
//! Env-gated, like every `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test routes_b2_rls_pg

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

// ── Shared seeding helpers (owner/BYPASSRLS pool — seeding must bypass the
//    policies the tests then probe) ─────────────────────────────────────────

async fn seed_second_node(owner: &PgPool, graph_id: Uuid, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, body) VALUES ($1, 'entity', $2, '{}') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .fetch_one(owner)
    .await
    .expect("insert second node")
}

async fn seed_supersession_edge(owner: &PgPool, superseded: Uuid, superseder: Uuid) {
    sqlx::query(
        "INSERT INTO supersession_edge(superseded_id, superseder_id, source, graph_id) \
         VALUES ($1, $2, 'user', (SELECT graph_id FROM nodes WHERE node_id = $1))",
    )
    .bind(superseded)
    .bind(superseder)
    .execute(owner)
    .await
    .expect("insert supersession edge");
}

async fn seed_relationship_edge(owner: &PgPool, graph_id: Uuid, src: Uuid, dst: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO relationship_edge(graph_id, src_node_id, predicate, dst_node_id) \
         VALUES ($1, $2, 'works_on', $3) RETURNING edge_id",
    )
    .bind(graph_id)
    .bind(src)
    .bind(dst)
    .fetch_one(owner)
    .await
    .expect("insert relationship edge")
}

/// A shared ('org'-kind) ocean inside `account` — a legal promotion TARGET.
/// `kind = 'org'` (not `'team'`) keeps the reachability predicate to plain
/// account membership, so the probe doesn't have to seed `teams`/`team_members`
/// as well; `promote_node` accepts either kind identically.
async fn seed_shared_ocean(owner: &PgPool, account: Uuid, name: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO graphs(account_id, name, kind) VALUES ($1, $2, 'org') RETURNING id")
        .bind(account)
        .bind(name)
        .fetch_one(owner)
        .await
        .expect("insert shared ocean")
}

async fn seed_conversation(owner: &PgPool, account: Uuid, graph: Uuid, user: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO conversations(account_id, graph_id, user_id, title) \
         VALUES ($1, $2, $3, 'probe conversation') RETURNING id",
    )
    .bind(account)
    .bind(graph)
    .bind(user)
    .fetch_one(owner)
    .await
    .expect("insert conversation")
}

// ── 1. Query-shape probes, one section per converted route file ─────────────

#[tokio::test]
async fn b2_route_query_shapes_are_account_scoped() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b2_shapes").await else {
        eprintln!("skipping b2_route_query_shapes_are_account_scoped: set ORGT_TEST_BASE_URL");
        return;
    };

    // Extra fixture rows: a second node per account (edge endpoints), one
    // supersession edge, one relationship edge, one conversation per account.
    let a_node2 = seed_second_node(&probe.owner, probe.a.graph_id, "Probe Node A2").await;
    let b_node2 = seed_second_node(&probe.owner, probe.b.graph_id, "Probe Node B2").await;
    seed_supersession_edge(&probe.owner, probe.a.node_id, a_node2).await;
    seed_supersession_edge(&probe.owner, probe.b.node_id, b_node2).await;
    let a_edge = seed_relationship_edge(&probe.owner, probe.a.graph_id, probe.a.node_id, a_node2).await;
    let b_edge = seed_relationship_edge(&probe.owner, probe.b.graph_id, probe.b.node_id, b_node2).await;
    let a_conv = seed_conversation(&probe.owner, probe.a.account_id, probe.a.graph_id, probe.a.user_id).await;
    let b_conv = seed_conversation(&probe.owner, probe.b.account_id, probe.b.graph_id, probe.b.user_id).await;
    let _ = (a_conv, b_edge);

    // ── routes/nodes.rs — `list_nodes_scoped` / `node_graph_id_scoped` shape ──
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT node_id FROM nodes WHERE graph_id = $1",
            &[probe.a.graph_id],
            &[probe.a.node_id, a_node2],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT node_id FROM nodes WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    // The by-id anchor every node-anchored handler resolves first: B's node id
    // from A's scope is absent, never a leak.
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT graph_id FROM nodes WHERE node_id = $1",
            &[probe.b.node_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT node_id FROM nodes WHERE graph_id = $1", &[probe.a.graph_id])
        .await;

    // ── routes/relationships.rs — `list_relationship_edges_by_status_scoped`
    //    and the `get_relationship_edge_scoped` by-id anchor ──────────────────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT edge_id FROM relationship_edge WHERE graph_id = $1 AND status = 'suggested'",
            &[probe.a.graph_id],
            &[a_edge],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT edge_id FROM relationship_edge WHERE edge_id = $1",
            &[b_edge],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT edge_id FROM relationship_edge", &[])
        .await;

    // ── routes/supersession.rs — `list_supersession_edges_scoped` shape ──────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT superseder_id FROM supersession_edge \
             WHERE superseded_id = $1 OR superseder_id = $1",
            &[probe.a.node_id],
            &[a_node2],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT superseder_id FROM supersession_edge \
             WHERE superseded_id = $1 OR superseder_id = $1",
            &[probe.b.node_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT superseder_id FROM supersession_edge", &[])
        .await;

    // ── routes/conversations.rs — listing + the `get_conversation_scoped`
    //    by-id anchor (`conversations` carries a direct account_id column) ────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM conversations WHERE graph_id = $1 AND user_id = $2",
            &[probe.a.graph_id, probe.a.user_id],
            &[a_conv],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM conversations WHERE id = $1",
            &[b_conv],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT id FROM conversations", &[])
        .await;

    // ── routes/atlas.rs — `list_atoms_scoped` shape (nodes + relationship
    //    edges already covered above; atlas composes all three) ───────────────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE graph_id = $1",
            &[probe.a.graph_id],
            &[probe.a.atom_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT atom_id FROM atoms WHERE graph_id = $1", &[probe.a.graph_id])
        .await;

    // ── routes/nodes.rs promotion — `node_promotions`' split policies (0083
    //    §2d): READ is a permissive OR across both anchors, WRITE a strict AND.
    //    The strict AND is what makes the consumer-plane promote same-account
    //    by construction, which is the premise `promote_node`/`promote_source`
    //    scope on. ──────────────────────────────────────────────────────────────
    let a_shared = seed_shared_ocean(&probe.owner, probe.a.account_id, "A shared").await;
    let b_shared = seed_shared_ocean(&probe.owner, probe.b.account_id, "B shared").await;

    // Same-account INSERT (source ocean and target ocean both A's) is accepted.
    {
        let mut tx = probe.scoped_tx(probe.a.account_id).await;
        let copy: Uuid = sqlx::query_scalar(
            "INSERT INTO nodes(graph_id, type, title, body) \
             VALUES ($1, 'entity', 'Published Copy', '{}') RETURNING node_id",
        )
        .bind(a_shared)
        .fetch_one(&mut *tx)
        .await
        .expect("same-account publish INSERT must be accepted under RLS");
        sqlx::query(
            "INSERT INTO node_promotions(source_node_id, target_graph_id, target_node_id, \
             promoted_by, from_version) VALUES ($1, $2, $3, $4, 1)",
        )
        .bind(probe.a.node_id)
        .bind(a_shared)
        .bind(copy)
        .bind(probe.a.user_id)
        .execute(&mut *tx)
        .await
        .expect("same-account node_promotions INSERT must be accepted under RLS");
        tx.commit().await.expect("commit same-account promotion");
    }
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT source_node_id FROM node_promotions WHERE target_graph_id = $1",
            &[a_shared],
            &[probe.a.node_id],
        )
        .await;
    // A's scope cannot see B's ocean at all, so A can never name it as a
    // promotion target — the `get_graph_scoped` anchor the handler resolves
    // first is already filtered to absent (→ its non-leaking 404).
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT id FROM graphs WHERE id = $1",
            &[b_shared],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT source_node_id FROM node_promotions", &[])
        .await;

    probe.teardown().await;
}

// ── 2. Route-level probes through the real public router, as coralswarm_app ─

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair — same fixture
/// `route_migration_org_pg.rs`/`mcp_pg_e2e.rs` use, duplicated for the same
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
async fn b2_converted_handlers_work_and_isolate_as_coralswarm_app() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b2_routes").await else {
        eprintln!(
            "skipping b2_converted_handlers_work_and_isolate_as_coralswarm_app: set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // ── Env: local JWKS so `require_auth` verifies real Clerk-shaped tokens.
    // This binary's only env-mutating test, so no cross-test lock is needed
    // (each test binary is its own process). ─────────────────────────────────
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: single-threaded with respect to env mutation — this is the only
    // test in this binary that touches CLERK_* (see comment above).
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    // ── Wire the harness fixtures up to Clerk-shaped identities so
    // `require_auth`'s early-return provisioning resolves to EXACTLY the
    // probe's seeded users/accounts (no new rows): `ensure_user` finds the
    // `clerk_user_id`, `ensure_personal_account` finds the owned account. The
    // membership rows are what `resolve_graph_reachable_by_user` (the
    // SECURITY DEFINER reachability carve-out) consults. ─────────────────────
    let sub_a = "b2_rls_probe_user_a";
    let sub_b = "b2_rls_probe_user_b";
    for (sub, fx) in [(sub_a, &probe.a), (sub_b, &probe.b)] {
        sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
            .bind(sub)
            .bind(fx.user_id)
            .execute(&probe.owner)
            .await
            .expect("stamp clerk_user_id");
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(fx.account_id)
        .bind(fx.user_id)
        .execute(&probe.owner)
        .await
        .expect("seed account_members");
    }

    let a_node2 = seed_second_node(&probe.owner, probe.a.graph_id, "Route Node A2").await;
    let b_node2 = seed_second_node(&probe.owner, probe.b.graph_id, "Route Node B2").await;
    let a_edge = seed_relationship_edge(&probe.owner, probe.a.graph_id, probe.a.node_id, a_node2).await;
    sqlx::query("UPDATE relationship_edge SET provenance = $2 WHERE edge_id = $1")
        .bind(a_edge)
        .bind(json!({"atom_ids": [probe.a.atom_id]}))
        .execute(&probe.owner)
        .await
        .expect("support A's relationship with A's permitted coral");
    let b_conv = seed_conversation(&probe.owner, probe.b.account_id, probe.b.graph_id, probe.b.user_id).await;
    let a_shared = seed_shared_ocean(&probe.owner, probe.a.account_id, "A shared").await;
    let b_shared = seed_shared_ocean(&probe.owner, probe.b.account_id, "B shared").await;
    for (node_id, atom_id) in [
        (probe.a.node_id, probe.a.atom_id),
        (a_node2, probe.a.atom_id),
        (probe.b.node_id, probe.b.atom_id),
        (b_node2, probe.b.atom_id),
    ] {
        sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(&probe.owner)
            .await
            .expect("link each account's reefs to its own permitted coral");
    }

    // ── AppState served ENTIRELY off the coralswarm_app pool: every content
    // query a converted handler runs is now subject to 0083's policies — an
    // unconverted (unscoped) query path hard-errors instead of serving. ──────
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));

    macro_rules! req_a {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("authorization", format!("Bearer {token_a}"))
                .body(Body::empty())
                .unwrap()
        };
        ($method:expr, $uri:expr, $json:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("authorization", format!("Bearer {token_a}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&$json).unwrap()))
                .unwrap()
        };
    }

    // ── routes/nodes.rs: list_nodes — scoped-correct then cross-denied ───────
    let resp = app
        .clone()
        .oneshot(req_a!("GET", format!("/v1/nodes?graph_id={}", probe.a.graph_id)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "A listing A's own ocean must succeed under RLS");
    let nodes = body_json(resp).await;
    let listed: Vec<&str> = nodes
        .as_array()
        .expect("bare array body")
        .iter()
        .filter_map(|n| n["node_id"].as_str())
        .collect();
    assert!(
        listed.contains(&probe.a.node_id.to_string().as_str())
            && listed.contains(&a_node2.to_string().as_str()),
        "A's listing must contain exactly A's nodes: {listed:?}"
    );
    assert!(
        !listed.contains(&probe.b.node_id.to_string().as_str()),
        "A's listing must never contain B's nodes"
    );

    let resp = app
        .clone()
        .oneshot(req_a!("GET", format!("/v1/nodes?graph_id={}", probe.b.graph_id)))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "A listing B's ocean must be denied (reachability), never served"
    );

    // ── routes/nodes.rs: patch_node — a scoped WRITE must actually commit ────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "PATCH",
            format!("/v1/nodes/{}", probe.a.node_id),
            json!({"title": "Renamed Under RLS"})
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "A patching A's own node must succeed under RLS");
    let persisted: String =
        sqlx::query_scalar("SELECT title FROM nodes WHERE node_id = $1")
            .bind(probe.a.node_id)
            .fetch_one(&probe.owner)
            .await
            .unwrap();
    assert_eq!(persisted, "Renamed Under RLS", "the scoped transaction must COMMIT the write");

    let resp = app
        .clone()
        .oneshot(req_a!(
            "PATCH",
            format!("/v1/nodes/{}", probe.b.node_id),
            json!({"title": "Hijacked"})
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "A patching B's node must be denied (RLS filters the ownership check)"
    );

    let publication_snapshot_sql = r#"
        SELECT jsonb_build_object(
            'promotions', (SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY to_jsonb(p)::text), '[]'::jsonb) FROM node_promotions p),
            'connections', (SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY to_jsonb(c)::text), '[]'::jsonb) FROM source_connections c),
            'jobs', (SELECT COALESCE(jsonb_agg(to_jsonb(j) ORDER BY to_jsonb(j)::text), '[]'::jsonb) FROM jobs j),
            'usage', (SELECT COALESCE(jsonb_agg(to_jsonb(u) ORDER BY to_jsonb(u)::text), '[]'::jsonb) FROM llm_usage u),
            'pending_usage', (SELECT COALESCE(jsonb_agg(to_jsonb(u) ORDER BY to_jsonb(u)::text), '[]'::jsonb) FROM llm_usage_pending u)
        )
    "#;
    let publication_before: Value = sqlx::query_scalar(publication_snapshot_sql)
        .fetch_one(&probe.owner)
        .await
        .expect("snapshot publication and model ledgers");
    assert_eq!(
        publication_before,
        json!({"promotions": [], "connections": [], "jobs": [], "usage": [], "pending_usage": []})
    );
    for (uri, payload) in [
        (
            format!("/v1/nodes/{}/promote", probe.a.node_id),
            json!({"target_graph_id": a_shared}),
        ),
        (
            format!("/v1/nodes/{}/promote", probe.b.node_id),
            json!({"target_graph_id": a_shared}),
        ),
        (
            format!("/v1/nodes/{}/promote", probe.a.node_id),
            json!({"target_graph_id": b_shared}),
        ),
        (
            format!("/v1/graphs/{}/promote-source", probe.a.graph_id),
            json!({"source_id": "probe-1", "target_graph_id": a_shared, "with_corals": true}),
        ),
        (
            format!("/v1/graphs/{}/promote-source", probe.b.graph_id),
            json!({"source_id": "probe-1", "target_graph_id": a_shared}),
        ),
    ] {
        let resp = app
            .clone()
            .oneshot(req_a!("POST", &uri, payload))
            .await
            .unwrap();
        let status = resp.status();
        let body = body_json(resp).await;
        assert_eq!(status, StatusCode::CONFLICT, "legacy publication must require selected evidence at {uri}: {body}");
        assert_eq!(body, json!({
            "error": "Select exact retained passages using /sources/publications",
            "code": "selected_evidence_consent_required"
        }));
        let publication_after: Value = sqlx::query_scalar(publication_snapshot_sql)
            .fetch_one(&probe.owner)
            .await
            .expect("read publication and model ledgers after refusal");
        assert_eq!(publication_after, publication_before, "publication refusal at {uri} must preserve every ledger row");
    }

    // ── routes/relationships.rs: list — scoped-correct then cross-denied ─────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!("/v1/graphs/{}/relationships?status=suggested", probe.a.graph_id)
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "A listing A's edges must succeed under RLS");
    let page = body_json(resp).await;
    assert_eq!(
        page["edges"].as_array().map(|a| a.len()),
        Some(1),
        "exactly A's one seeded edge: {page}"
    );
    assert_eq!(page["has_more"], serde_json::json!(false), "one edge fits one page: {page}");

    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!("/v1/graphs/{}/relationships", probe.b.graph_id)
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "A listing B's edges must be denied");

    // ── routes/supersession.rs: create own (scoped write) + cross-denied ─────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            format!("/v1/nodes/{}/supersede", probe.a.node_id),
            json!({"superseded_by": a_node2})
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "A superseding within A's ocean must succeed under RLS");

    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!("/v1/nodes/{}/supersessions", probe.a.node_id)
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let sup = body_json(resp).await;
    assert_eq!(
        sup["as_superseded"].as_array().map(|a| a.len()),
        Some(1),
        "the committed supersession edge must read back: {sup}"
    );

    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!("/v1/nodes/{}/supersessions", probe.b.node_id)
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "A listing B's supersessions must 404 (ownership check RLS-filtered)"
    );

    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            format!("/v1/nodes/{}/supersede", probe.b.node_id),
            json!({"superseded_by": b_node2})
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "A superseding B's nodes must 404, never write"
    );

    // ── routes/conversations.rs: cross-account by-id anchor is RLS-filtered ──
    let resp = app
        .clone()
        .oneshot(req_a!("GET", format!("/v1/conversations/{b_conv}")))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "A reading B's conversation must 404 under RLS (filtered to absent, not 403-leaked)"
    );

    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!("/v1/conversations?graph_id={}", probe.a.graph_id)
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "A listing A's conversations must succeed under RLS");
    let convs = body_json(resp).await;
    assert_eq!(
        convs["conversations"].as_array().map(|a| a.len()),
        Some(0),
        "A has no conversations of their own yet: {convs}"
    );

    // ── routes/atlas.rs: composed read — scoped-correct then cross-denied ────
    let resp = app
        .clone()
        .oneshot(req_a!("GET", format!("/v1/graphs/{}/atlas", probe.a.graph_id)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "A's atlas must assemble under RLS");
    let atlas = body_json(resp).await;
    let atlas_ids: Vec<&str> = atlas["nodes"]
        .as_array()
        .expect("atlas nodes array")
        .iter()
        .filter_map(|n| n["id"].as_str())
        .collect();
    assert!(
        atlas_ids.contains(&probe.a.atom_id.to_string().as_str()),
        "A's atlas must include A's coral: {atlas_ids:?}"
    );
    assert!(
        !atlas_ids.contains(&probe.b.node_id.to_string().as_str())
            && !atlas_ids.contains(&probe.b.atom_id.to_string().as_str()),
        "A's atlas must never include B's content"
    );

    let resp = app
        .clone()
        .oneshot(req_a!("GET", format!("/v1/graphs/{}/atlas", probe.b.graph_id)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "A requesting B's atlas must be denied");

    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
    probe.teardown().await;
}
