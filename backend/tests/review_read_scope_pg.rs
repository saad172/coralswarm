//! Review READ scope (settings 2.12): which pending review items a member is
//! shown.
//!
//! Before this suite, the review-queue readers filtered on `graphs.account_id
//! = caller's account` alone. In an org every ocean shares that account, so
//! `GET /v1/candidates`, `GET /v1/review/count` and the Slack App Home review
//! section showed every member the titles and rationales of every other
//! member's PRIVATE pending decisions (and, on App Home, their suggested
//! edges). The tenant `member_ocean_isolation` RLS policy does not help in
//! production, where the tenant connection is the table owner.
//!
//! The rule this suite pins: a member sees review items only from oceans they
//! can READ, the D9 union `ReachabilityResolver::list_reachable_graphs`
//! already serves search, Ask, Library and the journal — their own private
//! ocean, the org commons, and team oceans they are an active member of.
//! Never another member's private ocean, never a team they are not on.
//! Personal accounts are unchanged.
//!
//! Real router, real Clerk-shaped tokens, separate control and tenant
//! Postgres databases. Gated on `ORGT_TEST_BASE_URL` (a base connection with
//! CREATE DATABASE rights); every test returns early when it is unset. Wired
//! into CI's `cross-tenant-probe` job.
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test review_read_scope_pg -- --test-threads=1 --nocapture
//!
//! Each test prints `REVIEW_READ_SCOPE_PG: PROVED <name>` only after its last
//! assertion, and `REVIEW_READ_SCOPE_PG: SKIPPED <name>` when it found no
//! database; CI counts both, so a skipped authorization test cannot report
//! green.

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext, TenantRuntime};
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const TEST_MASTER_KEK_HEX: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";

/// Serializes the process-global env vars every test sets.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin_conn(base: &str) -> PgConnection {
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
    (Utc::now().timestamp() + 3600) as usize
}

async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let app = axum::Router::new().route(
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
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

struct Env {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl Env {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: every env mutation in this binary is serialized by ENV_LOCK,
        // held in `_guard` until Drop.
        unsafe {
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
        }
        Self { _guard: guard }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        // SAFETY: ENV_LOCK is still held by `_guard`.
        unsafe {
            for k in ["CLERK_ISSUER", "CLERK_JWKS_URL", "RATE_LIMIT_BACKEND", "MASTER_KEK_HEX"] {
                std::env::remove_var(k);
            }
        }
    }
}

/// Clerk session token for the REST API. `org_role` must agree with the
/// seeded `account_members.role`: membership is re-provisioned from claims.
fn rest_token(sub: &str, org: Option<(&str, &str)>) -> String {
    let mut claims = json!({"iss": ISSUER, "sub": sub, "exp": future_exp(), "sid": "sess_rrs"});
    if let Some((org, role)) = org {
        claims["org_id"] = json!(org);
        claims["org_role"] = json!(role);
    }
    mint(claims)
}

async fn get(app: &axum::Router, token: &str, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("host", APEX)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

// ── Seeding ──────────────────────────────────────────────────────────────────

async fn seed_user(pool: &PgPool, label: &str) -> (Uuid, String) {
    let sub = format!("rrs_{label}_{}", Uuid::new_v4().simple());
    let id: Uuid =
        sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{sub}@test.local"))
            .bind(&sub)
            .fetch_one(pool)
            .await
            .expect("seed user");
    (id, sub)
}

/// Org memberships carry a seat tier; personal ones must not
/// (`account_members_seat_tier_iff_org`).
async fn seed_membership(pool: &PgPool, account: Uuid, user: Uuid, role: &str) {
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         SELECT $1, $2, $3, CASE WHEN a.clerk_org_id IS NULL THEN NULL \
                ELSE (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active) END \
         FROM accounts a WHERE a.id = $1",
    )
    .bind(account)
    .bind(user)
    .bind(role)
    .execute(pool)
    .await
    .expect("seed membership");
}

async fn seed_graph(pool: &PgPool, account: Uuid, kind: &str, owner: Option<Uuid>, primary: bool) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(account)
    .bind(format!("{kind} ocean {}", Uuid::new_v4().simple()))
    .bind(kind)
    .bind(owner)
    .bind(primary)
    .fetch_one(pool)
    .await
    .expect("seed graph")
}

/// A team ocean whose only active member is `member`.
async fn seed_team(pool: &PgPool, account: Uuid, graph: Uuid, member: Uuid) {
    let team: Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, $3, $4) \
         RETURNING id",
    )
    .bind(account)
    .bind(graph)
    .bind(format!("team {}", Uuid::new_v4().simple()))
    .bind(member)
    .fetch_one(pool)
    .await
    .expect("seed team");
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(team)
        .bind(member)
        .execute(pool)
        .await
        .expect("seed team member");
}

async fn seed_entity(pool: &PgPool, graph: Uuid, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph)
    .bind(title)
    .fetch_one(pool)
    .await
    .expect("seed entity")
}

/// A pending decision `age_days` old (its `pending_since`), carrying a
/// rationale that must never reach another member.
async fn seed_pending_decision(pool: &PgPool, graph: Uuid, title: &str, age_days: i64) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body, created_at) \
         VALUES (gen_random_uuid(), $1, 'decision', $2, \
                 jsonb_build_object('review_status', 'pending', \
                                    'rationale', $2 || ' rationale', \
                                    'decided_at', now()::text, \
                                    'pending_since', (now() - make_interval(days => $3))::text), \
                 now() - make_interval(days => $3)) \
         RETURNING node_id",
    )
    .bind(graph)
    .bind(title)
    .bind(age_days as i32)
    .fetch_one(pool)
    .await
    .expect("seed pending decision")
}

async fn seed_suggested_edge(pool: &PgPool, graph: Uuid, label: &str, age_days: i64) -> Uuid {
    let src = seed_entity(pool, graph, &format!("{label} src")).await;
    let dst = seed_entity(pool, graph, &format!("{label} dst")).await;
    sqlx::query_scalar(
        "INSERT INTO relationship_edge (graph_id, src_node_id, predicate, dst_node_id, origin, \
                                        status, confidence, provenance, queued_at) \
         VALUES ($1, $2, 'works_on', $3, 'inferred', 'suggested', 0.6, '{}', \
                 now() - make_interval(days => $4)) RETURNING edge_id",
    )
    .bind(graph)
    .bind(src)
    .bind(dst)
    .bind(age_days as i32)
    .fetch_one(pool)
    .await
    .expect("seed suggested edge")
}

fn build_app(control: &PgPool, tenant_dsn: Option<(&str, String)>) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    let pairs: Vec<(&str, String)> = tenant_dsn.into_iter().collect();
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(pairs)))),
    ));
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// Every pending-decision title on every page of `GET /v1/candidates`,
/// walked one row at a time so the keyset cursor is exercised under the
/// readable-ocean filter too. Also returns the raw pages, so a test can check
/// that no rationale leaks through any field.
async fn candidate_titles(app: &axum::Router, token: &str) -> (BTreeSet<String>, String) {
    let mut titles = BTreeSet::new();
    let mut raw = String::new();
    let mut cursor: Option<String> = None;
    for _ in 0..50 {
        let uri = match &cursor {
            None => "/v1/candidates?limit=1".to_string(),
            Some(c) => format!("/v1/candidates?limit=1&cursor={}", urlencode(c)),
        };
        let (s, body) = get(app, token, &uri).await;
        assert_eq!(s, StatusCode::OK, "GET {uri}: {body:?}");
        raw.push_str(&body.to_string());
        for c in body["items"].as_array().expect("items array") {
            titles.insert(c["title"].as_str().expect("title").to_string());
        }
        match body["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => return (titles, raw),
        }
    }
    panic!("candidate pagination did not terminate");
}

fn urlencode(s: &str) -> String {
    s.replace('+', "%2B").replace(':', "%3A").replace('|', "%7C")
}

/// What the Slack App Home review section would render for `viewer`.
async fn app_home_titles(
    tenant: &PgPool,
    org_id: &str,
    account: Uuid,
    viewer: Uuid,
) -> BTreeSet<String> {
    let decision = TenancyDecision::Org(TenantContext {
        org_id: org_id.to_string(),
        account_id: account,
        user_id: viewer,
        pool: tenant.clone(),
        page_pool: None,
    });
    let items = coralswarm_backend::test_slack_review_items_for_viewer(
        Some(&decision),
        tenant,
        account,
        viewer,
    )
    .await
    .expect("app home review items");
    items.into_iter().collect()
}

// ── Org plane ────────────────────────────────────────────────────────────────

struct OrgFx {
    _env: Env,
    base: String,
    dbs: [&'static str; 2],
    org_id: &'static str,
    account: Uuid,
    control: PgPool,
    tenant: PgPool,
    app: axum::Router,
    user_a: Uuid,
    user_b: Uuid,
    a_ocean: Uuid,
    a_rest: String,
    b_rest: String,
}

/// Org fixture. Pending decisions (and suggested edges) in:
///
/// * the commons (`org`)            — readable by A and B
/// * A's private ocean (`member`)   — A only
/// * B's private ocean (`member`)   — B only
/// * team ocean "Alpha", A is on it — A only
/// * team ocean "Beta", B is on it  — B only
///
/// B's items are the OLDEST, so on App Home (top two by deadline) they would
/// fill every slot if they leaked to A.
async fn org_fixture(ctl: &'static str, ten: &'static str, org_id: &'static str) -> Option<OrgFx> {
    let base = base_url()?;
    let _env = Env::enable().await;
    let mut conn = admin_conn(&base).await;
    for db in [ctl, ten] {
        recreate(&mut conn, db).await;
    }
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    let (owner, _) = seed_user(&control, "owner").await;
    let account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(org_id)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        account,
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
             VALUES ($1, $2, $3, $4, 'active')",
        )
        .bind(org_id)
        .bind(account)
        .bind(org_id.replace('_', "-"))
        .bind(ten),
    )
    .await
    .unwrap();
    let (user_a, sub_a) = seed_user(&control, "a").await;
    let (user_b, sub_b) = seed_user(&control, "b").await;
    seed_membership(&control, account, user_a, "member").await;
    seed_membership(&control, account, user_b, "member").await;

    let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, ten)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&tenant)
        .await
        .unwrap();

    let commons = seed_graph(&tenant, account, "org", None, false).await;
    let a_ocean = seed_graph(&tenant, account, "member", Some(user_a), true).await;
    let b_ocean = seed_graph(&tenant, account, "member", Some(user_b), true).await;
    let alpha = seed_graph(&tenant, account, "team", None, false).await;
    seed_team(&tenant, account, alpha, user_a).await;
    let beta = seed_graph(&tenant, account, "team", None, false).await;
    seed_team(&tenant, account, beta, user_b).await;

    seed_pending_decision(&tenant, commons, "Commons decision", 1).await;
    seed_pending_decision(&tenant, a_ocean, "A private decision", 1).await;
    seed_pending_decision(&tenant, alpha, "Alpha team decision", 1).await;
    seed_pending_decision(&tenant, b_ocean, "B private decision", 6).await;
    seed_pending_decision(&tenant, beta, "Beta team decision", 6).await;
    seed_suggested_edge(&tenant, commons, "Commons", 2).await;
    seed_suggested_edge(&tenant, a_ocean, "A private", 2).await;
    seed_suggested_edge(&tenant, b_ocean, "B private", 6).await;
    seed_suggested_edge(&tenant, beta, "Beta team", 6).await;

    let app = build_app(&control, Some((ten, with_db(&base, ten))));
    let a_rest = rest_token(&sub_a, Some((org_id, "org:member")));
    let b_rest = rest_token(&sub_b, Some((org_id, "org:member")));
    Some(OrgFx {
        _env, base, dbs: [ctl, ten], org_id, account, control, tenant, app,
        user_a, user_b, a_ocean, a_rest, b_rest,
    })
}

impl OrgFx {
    async fn teardown(self) {
        self.control.close().await;
        self.tenant.close().await;
        let mut conn = admin_conn(&self.base).await;
        for db in self.dbs {
            conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
        }
    }
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn review_queue_and_count_show_only_oceans_the_member_can_read() {
    let Some(fx) = org_fixture("orgt_rrs_rest_ctl", "orgt_rrs_rest_ten", "org_rrs_rest").await else {
        println!("REVIEW_READ_SCOPE_PG: SKIPPED review_queue_and_count_show_only_oceans_the_member_can_read");
        eprintln!("skipping review_queue_and_count_show_only_oceans_the_member_can_read: set ORGT_TEST_BASE_URL");
        return;
    };
    let OrgFx { app, a_rest, b_rest, a_ocean, .. } = &fx;

    // ── GET /v1/candidates ───────────────────────────────────────────────────
    let (a_titles, a_raw) = candidate_titles(app, a_rest).await;
    assert_eq!(
        a_titles,
        set(&["A private decision", "Alpha team decision", "Commons decision"]),
        "A sees their own ocean, their team and the commons, and nothing else"
    );
    for secret in ["B private decision", "Beta team decision"] {
        assert!(!a_raw.contains(secret), "A's review queue leaks {secret:?} (title or rationale): {a_raw}");
    }
    let (b_titles, _) = candidate_titles(app, b_rest).await;
    assert_eq!(
        b_titles,
        set(&["B private decision", "Beta team decision", "Commons decision"]),
        "B's own view is symmetric"
    );

    // ── GET /v1/review/count ─────────────────────────────────────────────────
    let (s, body) = get(app, a_rest, &format!("/v1/review/count?graph_id={a_ocean}")).await;
    assert_eq!(s, StatusCode::OK, "{body:?}");
    assert_eq!(
        body["decisions"],
        json!(3),
        "the badge counts only decisions A can read (own, team, commons): {body:?}"
    );
    println!("REVIEW_READ_SCOPE_PG: PROVED review_queue_and_count");

    fx.teardown().await;
}

#[tokio::test]
async fn slack_app_home_review_section_shows_only_oceans_the_viewer_can_read() {
    let Some(fx) = org_fixture("orgt_rrs_slack_ctl", "orgt_rrs_slack_ten", "org_rrs_slack").await else {
        println!("REVIEW_READ_SCOPE_PG: SKIPPED slack_app_home_review_section_shows_only_oceans_the_viewer_can_read");
        eprintln!("skipping slack_app_home_review_section_shows_only_oceans_the_viewer_can_read: set ORGT_TEST_BASE_URL");
        return;
    };
    let OrgFx { tenant, org_id, account, user_a, user_b, .. } = &fx;

    let a_home = app_home_titles(tenant, org_id, *account, *user_a).await;
    for item in &a_home {
        assert!(
            !item.contains("B private") && !item.contains("Beta team"),
            "A's App Home shows another member's private or another team's item {item:?}: {a_home:?}"
        );
    }
    assert_eq!(a_home.len(), 2, "A still gets a full section from readable oceans: {a_home:?}");

    // B's own oldest items fill B's section, as before.
    let b_home = app_home_titles(tenant, org_id, *account, *user_b).await;
    assert!(
        b_home.iter().all(|t| t.contains("B private") || t.contains("Beta team")),
        "B's App Home leads with B's own (oldest) items: {b_home:?}"
    );
    println!("REVIEW_READ_SCOPE_PG: PROVED slack_app_home");

    fx.teardown().await;
}

// ── Personal plane ───────────────────────────────────────────────────────────

#[tokio::test]
async fn personal_account_review_queue_is_unchanged() {
    let Some(base) = base_url() else {
        println!("REVIEW_READ_SCOPE_PG: SKIPPED personal_account_review_queue_is_unchanged");
        eprintln!("skipping personal_account_review_queue_is_unchanged: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = Env::enable().await;
    let ctl = "orgt_rrs_personal_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin_conn(&base).await;
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let (user, sub) = seed_user(&control, "personal").await;
    let account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user)
            .fetch_one(&control)
            .await
            .unwrap();
    seed_membership(&control, account, user, "owner").await;
    let mine = seed_graph(&control, account, "personal", None, true).await;
    let mine2 = seed_graph(&control, account, "personal", None, false).await;
    seed_pending_decision(&control, mine, "My decision", 1).await;
    seed_pending_decision(&control, mine2, "My other decision", 2).await;

    let (stranger, _) = seed_user(&control, "stranger").await;
    let stranger_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(stranger)
            .fetch_one(&control)
            .await
            .unwrap();
    seed_membership(&control, stranger_account, stranger, "owner").await;
    let theirs = seed_graph(&control, stranger_account, "personal", None, true).await;
    seed_pending_decision(&control, theirs, "Stranger decision", 1).await;

    let app = build_app(&control, None);
    let token = rest_token(&sub, None);
    let (titles, _) = candidate_titles(&app, &token).await;
    assert_eq!(titles, set(&["My decision", "My other decision"]));
    let (s, body) = get(&app, &token, &format!("/v1/review/count?graph_id={mine}")).await;
    assert_eq!(s, StatusCode::OK, "{body:?}");
    assert_eq!(body["decisions"], json!(2), "{body:?}");
    println!("REVIEW_READ_SCOPE_PG: PROVED personal_unchanged");

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {ctl} WITH (FORCE)").as_str()).await.ok();
}
