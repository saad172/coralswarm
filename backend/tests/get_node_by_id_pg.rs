//! Route-level probe for `GET /v1/nodes/{id}` (the get-node-by-id fix): a
//! reef is id-addressable for every operation EXCEPT reading it — `PATCH
//! /nodes/{id}`, `GET /nodes/{id}/atoms`, `/promote`, `/digest`, `/merge`,
//! `/confirm`, `/reject` all already exist, so `list_nodes`'s per-row shape
//! plus `graph_id` and `can_edit` is the contract this route fills in.
//!
//! Three behaviors, the minimum this route needs proven:
//!
//! 1. A reachable reef returns 200 with the correct `graph_id`, and — for a
//!    reef the caller OWNS — `can_edit: true` that agrees with a REAL
//!    `patch_node` call actually succeeding for the identical caller/reef.
//! 2. An unreachable reef returns 404 with the SAME shared body
//!    `get_node_atoms` already returns (the web client string-matches "not
//!    found", per this PR's description) — proven both for a node that does
//!    not exist and one that exists but belongs to an account the caller
//!    cannot reach.
//! 3. A reef in a TEAM ocean the caller reaches via TEAM MEMBERSHIP, not
//!    ownership — the actual bug scenario ("Topic unavailable" on a promoted
//!    team reef) — returns 200 with `can_edit: false`, and that flag agrees
//!    with a REAL `patch_node` call 403ing for the identical caller/reef.
//!    The fixture deliberately makes the caller's ACTIVE account (their own
//!    personal account) differ from the team ocean's OWNING account (an org
//!    account they and the reef's team both belong to):
//!    `ReachabilityResolver::graph_reachable_by_user`'s D9 union is scoped
//!    to the raw `user_id` across every account/team they belong to
//!    (`resolve_graph_reachable_by_user`'s `account_id IN (SELECT
//!    account_id FROM account_members WHERE user_id = ...)`), NOT to
//!    whichever account happens to be "active" for this request — so GET's
//!    AUTHORIZATION DECISION succeeds here precisely because it is NOT an
//!    ownership check, while `patch_node`'s IDOR guard
//!    (`node_owned_by_account`) checks `g.account_id = user.account_id`
//!    (ownership, not reachability) and 403s in exactly this shape.
//!    **This test proves the decision, not the RLS-enforced wire path** —
//!    see the inline comment at the GET call below and `ReachabilityResolver
//!    ::resolve_reef_access`'s own doc comment for a documented, pre-
//!    existing gap in that path on the consumer plane specifically (the
//!    tenant plane, where every live org/team ocean actually lives today, is
//!    unaffected). `can_edit` (`node_editable_by_account` in
//!    `routes/nodes.rs`, the SAME function `patch_node` itself calls for its
//!    IDOR check — never a second, hand-rolled copy of that rule) is what
//!    lets the client know the ownership-check outcome WITHOUT guessing
//!    from `kind` or any other proxy. Both tests below assert the flag and
//!    the real `patch_node` outcome TOGETHER (`can_edit == (patch status ==
//!    200)`), not either alone, so the two can never silently drift apart.
//!
//! Uses the same scratch-DB-per-file pattern as `reachability_pg.rs`
//! (`sqlx::migrate!("./migrations_control")` against a fresh database,
//! connected as the plain owner/BYPASSRLS role — `reachability_pg.rs`'s own
//! team-ocean fixture is the precedent for testing this exact "team ocean
//! via team membership, not account ownership" shape this way), and the
//! "call the real handler directly with a hand-built `Tenant`/`UserContext`"
//! idiom `journal_endpoint_pg.rs`/`judgement_graph_worker_pg.rs` established
//! for `routes/nodes.rs` handlers (see `lib.rs`'s doc comment on the `pub
//! use` this file relies on) — a caller whose ACTIVE account differs from a
//! team ocean's OWNING account is not a shape a plain Clerk token can
//! produce through `test_support_app`'s JWT-minting harness.
//!
//! **RLS enforcement itself is deliberately out of scope for this file**,
//! same as `judgement_graph_worker_pg.rs`'s explicit framing: every query
//! here runs on the owner/BYPASSRLS role, so these tests prove the
//! AUTHORIZATION-DECISION logic (`resolve_reef_access`, `node_editable_by_
//! account`) is correct, not that the real, RLS-enforced consumer-plane
//! connection can actually reach the rows that decision says are reachable.
//! Do not read a 200/`can_edit` result here as proof of the wire path; see
//! `ReachabilityResolver::resolve_reef_access`'s doc comment for the known,
//! tracked gap and why closing it is a separate PR.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights):
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test get_node_by_id_pg

use std::sync::Arc;

use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::Json;
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::{get_node_by_id, patch_node, PatchNodeRequest, UserContext};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

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

// ── Fixture helpers (mirrors `reachability_pg.rs`'s own — separate test
//    binary/crate, so duplicated locally rather than shared) ────────────────

async fn new_user(pool: &PgPool, email: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn new_account(pool: &PgPool, owner_user_id: Uuid) -> Uuid {
    sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn add_member(pool: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)")
        .bind(account_id)
        .bind(user_id)
        .bind(role)
        .execute(pool)
        .await
        .unwrap();
}

async fn new_graph(pool: &PgPool, account_id: Uuid, name: &str, kind: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind) VALUES ($1, $2, $3) RETURNING id")
        .bind(account_id)
        .bind(name)
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn new_node(pool: &PgPool, graph_id: Uuid, title: &str) -> Uuid {
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title, body) VALUES ($1, 'entity', $2, '{}') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .fetch_one(pool)
    .await
    .unwrap();
    // The read DTO requires admitted evidence independently of reachability.
    // This synthetic legacy note literally supports the existing fixture title.
    let atom_id = Uuid::new_v4();
    sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp) VALUES($1,$2,'note',$3,'get-node-fixture',$4,now())")
        .bind(atom_id).bind(graph_id).bind(format!("get-node-fixture:{atom_id}"))
        .bind(format!("{title} is recorded in this synthetic fixture."))
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node_id).bind(atom_id).execute(pool).await.unwrap();
    node_id
}

async fn new_team(pool: &PgPool, account_id: Uuid, graph_id: Uuid, created_by: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) \
         VALUES ($1, $2, 'team', $3) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(created_by)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn add_team_member(pool: &PgPool, team_id: Uuid, user_id: Uuid) {
    sqlx::query(
        "INSERT INTO team_members (team_id, user_id, role, departing_at) \
         VALUES ($1, $2, 'member', NULL)",
    )
    .bind(team_id)
    .bind(user_id)
    .execute(pool)
    .await
    .unwrap();
}

fn tenant_for(pool: &PgPool) -> Tenant {
    // `decision: None` — the consumer/personal plane (or "tenancy runtime
    // off"), exactly what `reachability.rs`'s `ReachabilityResolver` treats
    // identically to `TenancyDecision::Personal`. This is also what lets one
    // shared scratch database stand in for "the caller's active account" and
    // "an org account they separately belong to" at once — see this module's
    // own doc comment and `reachability_pg.rs`'s, for the same reason.
    Tenant {
        store: Some(Arc::new(PostgresStore::new(pool.clone()))),
        pool: Some(pool.clone()),
        decision: None,
        class: coralswarm_backend::route_class::RouteClass::Other,
    }
}

fn user_ctx(user_id: Uuid, account_id: Uuid) -> UserContext {
    UserContext {
        user_id,
        account_id,
        role: "member".to_string(),
        device_id: "test-device".to_string(),
    }
}

async fn body_of(json: Json<Value>) -> Value {
    json.0
}

// ── 1 & 2. Reachable -> 200 with the right `graph_id`; unreachable (missing
//    or another account's node) -> the shared 404 body ─────────────────────

#[tokio::test]
async fn reachable_reef_returns_200_and_unreachable_returns_shared_404() {
    let Some(base) = base_url() else {
        eprintln!("skipping reachable_reef_returns_200_and_unreachable_returns_shared_404: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_get_node_by_id_basic";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;

    let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

    // Caller A: personal account + ocean + reef.
    let a_user = new_user(&pool, "a@example.com").await;
    let a_account = new_account(&pool, a_user).await;
    add_member(&pool, a_account, a_user, "owner").await;
    let a_graph = new_graph(&pool, a_account, "A's ocean", "personal").await;
    let a_node = new_node(&pool, a_graph, "A's reef").await;

    // An entirely separate account B, with its own reef A can never reach.
    let b_user = new_user(&pool, "b@example.com").await;
    let b_account = new_account(&pool, b_user).await;
    add_member(&pool, b_account, b_user, "owner").await;
    let b_graph = new_graph(&pool, b_account, "B's ocean", "personal").await;
    let b_node = new_node(&pool, b_graph, "B's reef").await;

    let tenant = tenant_for(&pool);
    let a_ctx = user_ctx(a_user, a_account);

    // ── (1) Reachable: A's own reef, 200 with the correct `graph_id`, and
    //    `can_edit: true` — A owns the account this reef's ocean belongs to,
    //    so `PATCH` must actually succeed too (the flag and the real outcome
    //    must agree). ───────────────────────────────────────────────────────
    let (status, resp) =
        get_node_by_id(tenant.clone(), Extension(a_ctx.clone()), Path(a_node)).await;
    assert_eq!(status, StatusCode::OK, "A must be able to GET their own reef");
    let body = body_of(resp).await;
    assert_eq!(body["node_id"], json!(a_node), "{body}");
    assert_eq!(body["title"], json!("A's reef"), "{body}");
    assert_eq!(
        body["graph_id"],
        json!(a_graph),
        "graph_id must be the reef's OWN ocean, not any other: {body}"
    );
    let can_edit = body["can_edit"].as_bool().expect("can_edit must be a bool");

    // `can_edit` must agree with the REAL `patch_node` outcome for this exact
    // caller/reef — asserted TOGETHER (not can_edit alone, not the patch
    // outcome alone) so the two can never silently drift apart.
    let (patch_status, patch_resp) = patch_node(
        tenant.clone(),
        Extension(a_ctx.clone()),
        Path(a_node),
        Json(PatchNodeRequest { title: Some("A's renamed reef".to_string()), node_type: None, entity_type: None, expected_version: None, support_atom_id: None }),
    )
    .await;
    assert_eq!(
        can_edit,
        patch_status == StatusCode::OK,
        "can_edit ({can_edit}) must exactly predict whether PATCH succeeds (got {patch_status}): {:?}",
        body_of(patch_resp).await
    );
    assert!(can_edit, "A owns this reef's account, so can_edit must be true: {body}");

    // ── (2a) Unreachable: B's reef, from A. Must be the SAME 404 body
    //    `get_node_atoms` already returns (the web client string-matches it).
    let (status, resp) =
        get_node_by_id(tenant.clone(), Extension(a_ctx.clone()), Path(b_node)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "A must never be able to GET B's reef");
    let body = body_of(resp).await;
    assert_eq!(
        body,
        json!({"error": "reef not found or access denied"}),
        "must be the exact shared not-found/denied body: {body}"
    );

    // ── (2b) Unreachable: a `node_id` that does not exist at all must
    //    produce the SAME body (never distinguishing "doesn't exist" from
    //    "exists but you can't see it" — the non-leaking posture every
    //    node-id-anchored read in this file already has).
    let (status, resp) = get_node_by_id(tenant, Extension(a_ctx), Path(Uuid::new_v4())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let body = body_of(resp).await;
    assert_eq!(body, json!({"error": "reef not found or access denied"}), "{body}");
}

// ── 3. Team-ocean-via-team-membership: the actual bug scenario, plus the
//    PR's "Follow-up" question about `patch_node` ───────────────────────────

#[tokio::test]
async fn team_ocean_reachable_via_membership_get_succeeds_patch_403s() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping team_ocean_reachable_via_membership_get_succeeds_patch_403s: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_get_node_by_id_team";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;

    let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

    // `member`'s OWN personal account/ocean — this is their ACTIVE account
    // for the request below (a plain, no-org-context call, exactly what the
    // web app makes today per this PR's description).
    let member = new_user(&pool, "member@example.com").await;
    let member_personal_account = new_account(&pool, member).await;
    add_member(&pool, member_personal_account, member, "owner").await;

    // A SEPARATE org account `member` also belongs to (an ordinary member,
    // not its owner), with a team ocean `member` is a `team_members` row
    // for — but NOT the graph's owning account.
    let org_owner = new_user(&pool, "person@example.com").await;
    let org_account = new_account(&pool, org_owner).await;
    add_member(&pool, org_account, org_owner, "owner").await;
    add_member(&pool, org_account, member, "member").await;
    let team_ocean = new_graph(&pool, org_account, "Acme Team One ocean", "team").await;
    let team = new_team(&pool, org_account, team_ocean, org_owner).await;
    add_team_member(&pool, team, member).await;
    let team_node = new_node(&pool, team_ocean, "Team reef").await;

    let tenant = tenant_for(&pool);
    // The load-bearing part of the fixture: `account_id` is `member`'s OWN
    // personal account, NOT `org_account` — reachability must still find the
    // team ocean via `account_members`+`team_members` on the raw `user_id`,
    // independent of which account this request happens to be scoped to.
    let member_ctx = user_ctx(member, member_personal_account);

    // ── GET succeeds: this proves the AUTHORIZATION DECISION only —
    //    `resolve_reef_access`/`graph_reachable_by_user`'s D9 union (scoped
    //    to the raw `user_id` across every account/team `member` belongs to)
    //    says `member` may see `team_node` regardless of which account this
    //    request happens to be scoped to. It does NOT prove the real,
    //    RLS-enforced consumer-plane wire path: this fixture connects as the
    //    plain owner/BYPASSRLS role (module doc above), under which every
    //    `nodes`/`graphs` row is visible regardless of `app.account_id` — so
    //    this assertion cannot, and does not claim to, observe whether
    //    `resolve_reef_access`'s own account-scoped bootstrap read
    //    (`node_graph_id_scoped`, run against the CALLER's active account
    //    before this test's cross-account fixture is even resolved) would
    //    also succeed under a REAL non-bypass `coralswarm_app` connection on
    //    the consumer plane. It would not: see the documented, PRE-EXISTING
    //    gap on `ReachabilityResolver::resolve_reef_access`'s doc comment
    //    (`backend/src/tenancy/reachability.rs`) for the full account and
    //    why it's out of scope for this route/PR to close. This test still
    //    pins the authorization DECISION (worth keeping on its own merits),
    //    just not the RLS-enforced read path. ─────────────────────────────
    let (status, resp) =
        get_node_by_id(tenant.clone(), Extension(member_ctx.clone()), Path(team_node)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a team member must be able to GET a reef in their team's ocean, even though their \
         ACTIVE account is a different, personal one — the authorization DECISION only; see \
         the comment above on why this fixture cannot observe the RLS-enforced wire path"
    );
    let body = body_of(resp).await;
    assert_eq!(body["node_id"], json!(team_node), "{body}");
    assert_eq!(
        body["graph_id"],
        json!(team_ocean),
        "graph_id must be the team ocean the reef actually lives in: {body}"
    );
    let can_edit = body["can_edit"].as_bool().expect("can_edit must be a bool");

    // ── PATCH, same caller/request shape: `patch_node`'s IDOR guard
    //    (`node_owned_by_account`) checks `g.account_id = user.account_id` —
    //    ownership, not reachability — so it 403s here where GET succeeded.
    //    This is the PR's "Follow-up" finding, made concrete: a team-ocean
    //    reef a member can VIEW is not necessarily one they can EDIT via this
    //    route today. `can_edit` must have already told the client that,
    //    without it having to guess — asserted TOGETHER with the real PATCH
    //    outcome (not either alone) so the flag and reality can never
    //    silently drift apart. ─────────────────────────────────────────────
    let (status, resp) = patch_node(
        tenant,
        Extension(member_ctx),
        Path(team_node),
        Json(PatchNodeRequest { title: Some("Renamed by a team member".to_string()), node_type: None, entity_type: None, expected_version: None, support_atom_id: None }),
    )
    .await;
    assert_eq!(
        can_edit,
        status == StatusCode::OK,
        "can_edit ({can_edit}) must exactly predict whether PATCH succeeds (got {status})"
    );
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "patch_node's ownership check must 403 a team member whose active account differs from \
         the team ocean's owning account, even though GET just authorized the same caller for \
         the same reef (and reported can_edit: {can_edit})"
    );
    let body = body_of(resp).await;
    assert_eq!(
        body,
        json!({"error": "node not found or access denied"}),
        "patch_node's own not-owned body (distinct from GET's \"reef not found...\" wording): {body}"
    );
}
