//! `routes::org_join_requests` — CS-437 (org-onboarding plan §3 items 4 + 14,
//! `docs/plans/org-onboarding-from-the-invite.md`, D3/D9/D10/D19).
//!
//! `list`/`approve`/`reject` take a [`Tenant`] extractor, so — same rationale
//! as `judgement_graph_worker_pg.rs`/`get_node_by_id_pg.rs`'s own doc
//! comments — this suite calls the REAL handlers directly (re-exported at
//! the crate root, see `lib.rs`'s own doc comment on that export) with a
//! hand-built `Tenant`/`UserContext`, rather than standing up the full
//! `TenantRuntime`/multi-DB routing harness just to prove D3's approve
//! sequence lands correctly. `create`/`list_mine` need no `Tenant` at all —
//! called the same way for consistency, and because the ONE thing that does
//! need a real router (the Clerk webhook double-delivery race) is exercised
//! through `test_support_app` + a real Svix-signed POST, same pattern
//! `member_ocean_lifecycle_pg.rs` uses.
//!
//! **What is NOT covered here, and why**: "approve with a full pool"
//! (`slot_pool::reserve_slot` returns `None`, so `acquire_seat` falls
//! through to a genuine Stripe purchase) needs `StripeClient::with_base_url`
//! pointed at a local mock — guarded `#[cfg(not(test))]` (see that method's
//! own doc comment: "mirrors `OpenAiProvider::with_base_url` exactly,
//! including the `#[cfg(not(test))]` guard that makes the local-mock-server
//! tests in this module possible"), which only relaxes when
//! `coralswarm-backend` itself is compiled WITH `cfg(test)` — true for an
//! in-crate `#[cfg(test)] mod pg_tests` block, never true for an external
//! `backend/tests/*.rs` binary that merely links the crate (even as a
//! `test-support`-featured dev-dependency). `org_invites.rs`'s own module
//! doc names the identical constraint for its Stripe seat-purchase-failure
//! unwind test. That ONE scenario lives instead in
//! `routes::org_join_requests::pg_tests` (in-crate), wired into
//! `ci.yml` as its own `--lib` filter step, alongside
//! `routes::org_invites::pg_tests::`.
//!
//! Covers:
//!   1. Reject: decided row, requester's open list drops it, a SECOND
//!      self-request for the same org is durably suppressed (D10) — same
//!      202, no second row — while a DIFFERENT org is unaffected (per-org).
//!   2. A second open request for the SAME org: `AlreadyOpen`, 202, no
//!      second row (the partial unique index).
//!   3. Personal-org target: 404 for the requester (same body as an unknown
//!      org id); 403 for every admin route. A non-admin member on the real
//!      org's own admin routes: 403.
//!   4. A tombstoned returning member: queued with
//!      `source = 'tombstoned_returning'`, never auto-approved.
//!   5. Approve with a free slot: Stripe untouched (never even configured —
//!      `state.stripe: None`, proving the fulfilled-reservation branch of
//!      `acquire_seat` never reaches it), the reservation ends up
//!      `fulfilled`, `slot = "reserved"`.
//!   6. Double delivery: the SAME membership's `organizationMembership.created`
//!      webhook, replayed after a successful approve — still exactly one
//!      `account_members` row, `seat_purchased_at` stamped ONCE (unchanged
//!      timestamp), still exactly one (fulfilled) reservation row.
//!   7. A stubbed Clerk 500 on approve: no `account_members` row, the
//!      reservation is `released` (not fulfilled), the request is still
//!      open.
//!
//! Env-gated on `ORGT_TEST_BASE_URL`, like every other `*_pg.rs` file here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test org_join_requests_pg -- \
//!     --test-threads=1 --nocapture

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{Request as HttpRequest, StatusCode};
use axum::routing::{delete as axum_delete, post as axum_post};
use axum::{Extension, Json};
use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;
use uuid::Uuid;

use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext};
use coralswarm_backend::{
    approve_join_request, create_join_request, list_join_requests, list_my_join_requests,
    reject_join_request, AppState, ApproveJoinRequestRequest, CreateJoinRequestRequest,
    ListJoinRequestsQuery, UserContext,
};

const WEBHOOK_SECRET_B64: &str = "am9pbi1yZXF1ZXN0cy1wZy10ZXN0LXdlYmhvb2stc2VjcmV0"; // arbitrary fixed key

/// Serializes every test in this file (shared-process `CLERK_*` env
/// mutations, plus `CLERK_WEBHOOK_SECRET` for the double-delivery test) —
/// same convention as every other `*_pg.rs` file's `ENV_LOCK`.
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

// ── Scratch-DB plumbing (pattern shared by every `*_pg.rs` file — copied
//    from `member_ocean_lifecycle_pg.rs:87-150`) ─────────────────────────────

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

async fn execute_financial_fixture<'q>(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    match query.execute(&mut *tx).await {
        Ok(result) => {
            tx.commit().await?;
            Ok(result)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

async fn fetch_financial_fixture_id<'q>(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    query: sqlx::query::QueryScalar<'q, sqlx::Postgres, Uuid, sqlx::postgres::PgArguments>,
) -> Result<Uuid, sqlx::Error> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    match query.fetch_one(&mut *tx).await {
        Ok(result) => {
            tx.commit().await?;
            Ok(result)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
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
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new().max_connections(4).connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

#[allow(dead_code)]
struct EnvGuard(tokio::sync::MutexGuard<'static, ()>);
impl EnvGuard {
    async fn acquire(clerk_base: &str) -> Self {
        let guard = ENV_LOCK.lock().await;
        // SAFETY: serialized by `guard`, held for this struct's whole
        // lifetime (including on panic/unwind, via `Drop` below) — same
        // pattern `routes::org_invites::pg_tests::EnvGuard` uses.
        unsafe {
            std::env::set_var("CLERK_API_BASE_URL", clerk_base);
            std::env::set_var("CLERK_SECRET_KEY", "sk_test_mock_join_requests");
            std::env::set_var("CLERK_WEBHOOK_SECRET", format!("whsec_{WEBHOOK_SECRET_B64}"));
        }
        Self(guard)
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: see `acquire` — still serialized by `self.0` here.
        unsafe {
            std::env::remove_var("CLERK_API_BASE_URL");
            std::env::remove_var("CLERK_SECRET_KEY");
            std::env::remove_var("CLERK_WEBHOOK_SECRET");
        }
    }
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

// ── Clerk Backend API mock (POST create-membership, DELETE unwind) ─────────

#[derive(Default, Clone)]
struct ClerkMockState {
    membership_create_status: u16,
    created_memberships: Vec<(String, String)>, // (org_id, user_id)
    deleted_memberships: Vec<(String, String)>,
}

type SharedClerkMock = Arc<std::sync::Mutex<ClerkMockState>>;

async fn spawn_clerk_mock(status: u16) -> (String, SharedClerkMock) {
    let state: SharedClerkMock =
        Arc::new(std::sync::Mutex::new(ClerkMockState { membership_create_status: status, ..Default::default() }));
    let app = axum::Router::new()
        .route(
            "/v1/organizations/{org_id}/memberships",
            axum_post(
                |axum::extract::Path(org_id): axum::extract::Path<String>,
                 axum::extract::State(state): axum::extract::State<SharedClerkMock>,
                 Json(body): Json<Value>| async move {
                    let user_id = body.get("user_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let mut s = state.lock().unwrap();
                    s.created_memberships.push((org_id, user_id));
                    let status = axum::http::StatusCode::from_u16(s.membership_create_status).unwrap();
                    if status.is_success() {
                        (status, Json(json!({"id": "orgmem_mock", "role": "org:member"}))).into_response()
                    } else {
                        (status, Json(json!({"errors": [{"code": "internal_error", "message": "mock 500"}]})))
                            .into_response()
                    }
                },
            ),
        )
        .route(
            "/v1/organizations/{org_id}/memberships/{user_id}",
            axum_delete(
                |axum::extract::Path((org_id, user_id)): axum::extract::Path<(String, String)>,
                 axum::extract::State(state): axum::extract::State<SharedClerkMock>| async move {
                    state.lock().unwrap().deleted_memberships.push((org_id, user_id));
                    (axum::http::StatusCode::OK, Json(json!({"id": "orgmem_mock"})))
                },
            ),
        )
        // The membership webhooks ask Clerk for the member's CURRENT
        // membership (stale-delivery guard); report what this mock created.
        .route(
            "/v1/users/{user_id}/organization_memberships",
            axum::routing::get(
                |axum::extract::Path(user_id): axum::extract::Path<String>,
                 axum::extract::State(state): axum::extract::State<SharedClerkMock>| async move {
                    let s = state.lock().unwrap();
                    let data: Vec<Value> = s
                        .created_memberships
                        .iter()
                        .filter(|(o, u)| *u == user_id && !s.deleted_memberships.contains(&(o.clone(), u.clone())))
                        .map(|(o, _)| json!({"organization": {"id": o}, "role": "org:member"}))
                        .collect();
                    let total = data.len();
                    Json(json!({"data": data, "total_count": total}))
                },
            ),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind clerk mock");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://{addr}"), state)
}

use axum::response::IntoResponse;

// ── Seeding ──────────────────────────────────────────────────────────────

/// A `team`-plan org with one ADMIN member (role `"admin"`, a purchased
/// `standard` seat) and `purchased_seats` total `standard` slots on the
/// ledger (so `available() = purchased_seats - 1`) — mirrors
/// `clerk_provision::pg_tests::seed_team_org_with_member`'s shape.
async fn seed_org(pool: &PgPool, tag: &str, purchased_seats: i64) -> (Uuid, String, Uuid, String) {
    let admin_id = Uuid::new_v4();
    let admin_clerk = format!("clerk_admin_{tag}_{}", admin_id.simple());
    sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
        .bind(admin_id)
        .bind(format!("{tag}-admin@join-requests.example"))
        .bind(&admin_clerk)
        .execute(pool)
        .await
        .expect("insert admin user");

    let clerk_org_id = format!("org_jr_{tag}_{}", Uuid::new_v4().simple());
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(admin_id)
            .bind(&clerk_org_id)
            .fetch_one(pool)
            .await
            .expect("insert org account");

    let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
        .fetch_one(pool)
        .await
        .expect("standard seat tier must be seeded (0165)");

    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
         VALUES ($1, $2, 'admin', $3, now())",
    )
    .bind(account_id)
    .bind(admin_id)
    .bind(tier_id)
    .execute(pool)
    .await
    .expect("insert admin membership");

    let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
        .bind(coralswarm_backend::billing::MIXED_TIER_PLAN_CODE)
        .fetch_one(pool)
        .await
        .expect("team plan must be seeded (0167)");
    let grant_id: Uuid = fetch_financial_fixture_id(pool, account_id, sqlx::query_scalar(
        "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) VALUES ($1, $2, 1, true) RETURNING id",
    )
    .bind(account_id)
    .bind(plan_id))
    .await
    .expect("insert team grant");
    if purchased_seats > 0 {
        execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)")
            .bind(grant_id)
            .bind(tier_id)
            .bind(purchased_seats),
        )
            .await
            .expect("seed the purchased ledger");
    }

    execute_financial_fixture(pool, account_id, sqlx::query(
        "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
         VALUES ($1, $2, $3, 'active')",
    )
    .bind(account_id)
    .bind(format!("cus_{}", Uuid::new_v4().simple()))
    .bind(format!("sub_{}", Uuid::new_v4().simple())))
    .await
    .expect("link stripe subscription");

    (account_id, clerk_org_id, admin_id, admin_clerk)
}

/// A real, signed-in CoralSwarm user with a Clerk identity but NO org
/// membership — the requester.
async fn seed_requester(pool: &PgPool, tag: &str) -> (Uuid, String) {
    let user_id = Uuid::new_v4();
    let clerk_user_id = format!("clerk_req_{tag}_{}", user_id.simple());
    sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
        .bind(user_id)
        .bind(format!("{tag}-requester@join-requests.example"))
        .bind(&clerk_user_id)
        .execute(pool)
        .await
        .expect("insert requester user");
    (user_id, clerk_user_id)
}

/// A personal-org target — an `accounts` row with a `clerk_org_id` that
/// `personal_orgs` also claims (D19: never receives/lists a join request).
async fn seed_personal_org(pool: &PgPool, tag: &str) -> (String, Uuid, Uuid) {
    let owner_id = Uuid::new_v4();
    let clerk_user_id = format!("clerk_powner_{tag}_{}", owner_id.simple());
    sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
        .bind(owner_id)
        .bind(format!("{tag}-powner@join-requests.example"))
        .bind(&clerk_user_id)
        .execute(pool)
        .await
        .expect("insert personal-org owner");
    let clerk_org_id = format!("org_personal_{tag}_{}", Uuid::new_v4().simple());
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(owner_id)
            .bind(&clerk_org_id)
            .fetch_one(pool)
            .await
            .expect("insert personal-org account");
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
        .bind(&clerk_org_id)
        .bind(&clerk_user_id)
        .execute(pool)
        .await
        .expect("insert personal_orgs row");
    (clerk_org_id, account_id, owner_id)
}

fn requester_ctx(user_id: Uuid) -> UserContext {
    UserContext { user_id, account_id: Uuid::new_v4(), role: "owner".to_string(), device_id: "jr-pg-test".to_string() }
}
fn admin_ctx(account_id: Uuid, user_id: Uuid) -> UserContext {
    UserContext { user_id, account_id, role: "admin".to_string(), device_id: "jr-pg-test".to_string() }
}
fn member_ctx(account_id: Uuid, user_id: Uuid) -> UserContext {
    UserContext { user_id, account_id, role: "member".to_string(), device_id: "jr-pg-test".to_string() }
}
fn org_tenant(pool: PgPool, org_id: &str, account_id: Uuid, user_id: Uuid) -> Tenant {
    Tenant {
        store: None,
        pool: None,
        decision: Some(TenancyDecision::Org(TenantContext {
            org_id: org_id.to_string(),
            account_id,
            user_id,
            pool,
            page_pool: None,
        })),
        class: coralswarm_backend::route_class::RouteClass::Other,
    }
}
fn app_state(control: PgPool) -> Arc<AppState> {
    Arc::new(coralswarm_backend::test_app_state_owned_with_db_and_store(control, None))
}

async fn count_open(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(pool)
    .await
    .unwrap()
}
async fn count_total(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM org_join_requests WHERE account_id = $1 AND user_id = $2")
        .bind(account_id)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

// ── Test 1: reject — durable, per-org suppression ───────────────────────────

#[tokio::test]
async fn reject_is_durable_per_org_but_a_different_org_is_unaffected() {
    let Some(base) = base_url() else {
        eprintln!("skipping reject_is_durable_per_org_but_a_different_org_is_unaffected: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_jr1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let (account_a, org_a, admin_a, _) = seed_org(&control, "jr1a", 2).await;
    let (account_b, org_b, _admin_b, _) = seed_org(&control, "jr1b", 2).await;
    let (requester_id, _) = seed_requester(&control, "jr1").await;

    let state = app_state(control.clone());

    // Requester asks to join org A.
    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: org_a.clone(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    assert_eq!(body_json(resp).await["status"], json!("sent_to_admin"));
    assert_eq!(count_open(&control, account_a, requester_id).await, 1);

    let id: Uuid = sqlx::query_scalar(
        "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
    )
    .bind(account_a)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .unwrap();

    // Admin rejects.
    let resp = reject_join_request(
        State(state.clone()),
        org_tenant(control.clone(), &org_a, account_a, admin_a),
        Extension(admin_ctx(account_a, admin_a)),
        Path(id),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let (decision, decided_by): (Option<String>, Option<Uuid>) =
        sqlx::query_as("SELECT decision, decided_by FROM org_join_requests WHERE id = $1")
            .bind(id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(decision.as_deref(), Some("rejected"));
    assert_eq!(decided_by, Some(admin_a));

    // The requester's open list no longer shows it.
    let resp = list_my_join_requests(State(state.clone()), Extension(requester_ctx(requester_id))).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let items = body_json(resp).await;
    assert_eq!(items.as_array().unwrap().len(), 0, "rejected request must not appear as open");

    // A second self-request for the SAME org: same 202, no second row (D10).
    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: org_a.clone(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    assert_eq!(body_json(resp).await["status"], json!("sent_to_admin"));
    assert_eq!(count_total(&control, account_a, requester_id).await, 1, "no second row for the rejecting org");
    assert_eq!(count_open(&control, account_a, requester_id).await, 0, "still no OPEN row for the rejecting org");

    // The SAME user requesting a DIFFERENT org: queued (per-org, D10).
    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: org_b.clone(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    assert_eq!(count_open(&control, account_b, requester_id).await, 1, "a different org must still queue");
}

// ── Test 2: a second open request for the same org is AlreadyOpen ──────────

#[tokio::test]
async fn second_open_request_for_the_same_org_inserts_no_second_row() {
    let Some(base) = base_url() else {
        eprintln!("skipping second_open_request_for_the_same_org_inserts_no_second_row: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_jr2_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let (account_id, org_id, _admin_id, _) = seed_org(&control, "jr2", 2).await;
    let (requester_id, _) = seed_requester(&control, "jr2").await;
    let state = app_state(control.clone());

    for _ in 0..2 {
        let resp = create_join_request(
            State(state.clone()),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }
    assert_eq!(count_total(&control, account_id, requester_id).await, 1);
    assert_eq!(count_open(&control, account_id, requester_id).await, 1);
}

// ── Test 3: personal-org target, and non-admin on admin routes ─────────────

#[tokio::test]
async fn personal_org_and_non_admin_are_refused() {
    let Some(base) = base_url() else {
        eprintln!("skipping personal_org_and_non_admin_are_refused: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_jr3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let (personal_org, personal_account_id, personal_owner_id) = seed_personal_org(&control, "jr3").await;
    let (requester_id, _) = seed_requester(&control, "jr3").await;
    let (account_id, org_id, _admin_id, _) = seed_org(&control, "jr3o", 2).await;
    let member_id = {
        let (id, clerk) = seed_requester(&control, "jr3m").await;
        let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard'")
            .fetch_one(&control)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'member', $3, now())",
        )
        .bind(account_id)
        .bind(id)
        .bind(tier_id)
        .execute(&control)
        .await
        .unwrap();
        let _ = clerk;
        id
    };
    let state = app_state(control.clone());

    // Requester → personal-org target: 404.
    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: personal_org.clone(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let personal_org_body = body_json(resp).await;

    // Requester → unknown org: SAME 404 body shape (D19).
    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: "org_does_not_exist".to_string(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(resp).await, personal_org_body, "personal-org and unknown-org 404s must read identically");

    // Admin routes against a personal-org tenant: 403. Uses the personal
    // org's OWN account_id AND its OWN owner consistently on both the
    // tenant and the `UserContext` — production can never produce any
    // other combination (a personal org has exactly one member, its owner,
    // and Clerk never issues an org JWT for a different user against
    // someone else's personal-org id), and `refuse_personal_org`'s
    // `account_is_real_org` lookup is specifically "is this NOT the
    // CALLER's own personal org" — a mismatched (account, user) pair fails
    // CLOSED (500) rather than proving the D19 refusal this assertion is
    // actually after.
    let tenant = org_tenant(control.clone(), &personal_org, personal_account_id, personal_owner_id);
    let resp = list_join_requests(
        State(state.clone()),
        tenant,
        Extension(admin_ctx(personal_account_id, personal_owner_id)),
        Query(ListJoinRequestsQuery { include_decided: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "a personal org must never list join requests (D19)");

    // Non-admin member on the REAL org's own admin routes: 403.
    let resp = list_join_requests(
        State(state.clone()),
        org_tenant(control.clone(), &org_id, account_id, member_id),
        Extension(member_ctx(account_id, member_id)),
        Query(ListJoinRequestsQuery { include_decided: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "a non-admin member must not read the admin queue");
}

// ── Test 4: tombstoned returning member is queued, not auto-approved ───────

#[tokio::test]
async fn tombstoned_returning_member_is_queued_with_its_own_source() {
    let Some(base) = base_url() else {
        eprintln!("skipping tombstoned_returning_member_is_queued_with_its_own_source: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_jr4_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let (account_id, org_id, _admin_id, _) = seed_org(&control, "jr4", 2).await;
    let (requester_id, _) = seed_requester(&control, "jr4").await;
    sqlx::query("INSERT INTO org_membership_tombstones (clerk_org_id, user_id) VALUES ($1, $2)")
        .bind(&org_id)
        .bind(requester_id)
        .execute(&control)
        .await
        .unwrap();
    let state = app_state(control.clone());

    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let (source, decision, evidence): (String, Option<String>, Value) =
        sqlx::query_as("SELECT source, decision, evidence FROM org_join_requests WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(requester_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(source, "tombstoned_returning");
    assert_eq!(decision, None, "a tombstoned return must be queued, never auto-approved");
    assert_eq!(evidence["tombstoned"], json!(true));

    let members: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(members, 0, "must not be auto-added");
}

// ── Test 5: approve with a free slot — no Stripe needed ─────────────────────

#[tokio::test]
async fn approve_with_a_free_slot_reserves_and_fulfills_without_stripe() {
    let Some(base) = base_url() else {
        eprintln!("skipping approve_with_a_free_slot_reserves_and_fulfills_without_stripe: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_jr5_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    // 2 purchased slots, 1 used (the admin) ⇒ 1 available.
    let (account_id, org_id, admin_id, _) = seed_org(&control, "jr5", 2).await;
    let (requester_id, requester_clerk) = seed_requester(&control, "jr5").await;
    let state = app_state(control.clone());

    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let id: Uuid = sqlx::query_scalar(
        "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
    )
    .bind(account_id)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .unwrap();

    let (clerk_base, clerk_mock) = spawn_clerk_mock(200).await;
    let _env = EnvGuard::acquire(&clerk_base).await;

    let resp = approve_join_request(
        State(state.clone()),
        org_tenant(control.clone(), &org_id, account_id, admin_id),
        Extension(admin_ctx(account_id, admin_id)),
        Path(id),
        Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK, "approve must succeed with a free slot");
    let body = body_json(resp).await;
    assert_eq!(body["slot"], json!("reserved"));
    assert_eq!(body["user_id"], json!(requester_id));

    {
        let s = clerk_mock.lock().unwrap();
        assert_eq!(s.created_memberships.len(), 1);
        assert_eq!(s.created_memberships[0], (org_id.clone(), requester_clerk.clone()));
    }

    let (role, seat_purchased_at): (String, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT role, seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .expect("a real account_members row must exist");
    assert_eq!(role, "member");
    assert!(seat_purchased_at.is_some(), "the seat must be stamped");

    let (released_at, release_reason): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) = sqlx::query_as(
        "SELECT released_at, release_reason FROM slot_reservations WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .expect("exactly one reservation row must exist");
    assert!(released_at.is_some());
    assert_eq!(release_reason.as_deref(), Some("fulfilled"));

    let (decision, decided_by, evidence): (Option<String>, Option<Uuid>, Value) =
        sqlx::query_as("SELECT decision, decided_by, evidence FROM org_join_requests WHERE id = $1")
            .bind(id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(decision.as_deref(), Some("approved"));
    assert_eq!(decided_by, Some(admin_id));
    assert_eq!(evidence["slot"], json!("reserved"));
    assert_eq!(evidence["tier"], json!("standard"));
    assert!(evidence.get("pool_full").is_none() || evidence["pool_full"] == json!(false));
}

// ── Test 6: double delivery — the webhook replays after a successful approve

fn sign_svix(secret_b64: &str, id: &str, ts: &str, body: &str) -> String {
    let key = STANDARD.decode(secret_b64).unwrap();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
    mac.update(format!("{id}.{ts}.{body}").as_bytes());
    format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()))
}
fn signed_webhook_request(body_json: Value) -> HttpRequest<Body> {
    let body = body_json.to_string();
    let (id, ts) = (format!("msg_{}", Uuid::new_v4()), chrono::Utc::now().timestamp().to_string());
    let sig = sign_svix(WEBHOOK_SECRET_B64, &id, &ts, &body);
    HttpRequest::builder()
        .method("POST")
        .uri("/v1/webhooks/clerk")
        .header("content-type", "application/json")
        .header("svix-id", id)
        .header("svix-timestamp", ts)
        .header("svix-signature", sig)
        .body(Body::from(body))
        .unwrap()
}
fn membership_created_webhook(org_id: &str, clerk_user_id: &str, email: &str) -> HttpRequest<Body> {
    signed_webhook_request(json!({
        "type": "organizationMembership.created",
        "data": {
            "organization": {"id": org_id, "name": "Acme"},
            "public_user_data": {
                "user_id": clerk_user_id,
                "identifier": email,
                "first_name": "Re",
                "last_name": "Quester",
            },
            "role": "org:member",
        },
    }))
}
fn webhook_app(control: PgPool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control)));
    coralswarm_backend::test_support_app(Arc::new(state))
}

#[tokio::test]
async fn double_delivery_after_approve_stays_at_one_membership_and_one_reservation() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping double_delivery_after_approve_stays_at_one_membership_and_one_reservation: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_jr6_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let (account_id, org_id, admin_id, _) = seed_org(&control, "jr6", 2).await;
    let (requester_id, requester_clerk) = seed_requester(&control, "jr6").await;
    let requester_email: String =
        sqlx::query_scalar("SELECT email FROM users WHERE id = $1").bind(requester_id).fetch_one(&control).await.unwrap();
    let state = app_state(control.clone());

    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let id: Uuid = sqlx::query_scalar(
        "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
    )
    .bind(account_id)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .unwrap();

    let (clerk_base, _clerk_mock) = spawn_clerk_mock(200).await;
    let _env = EnvGuard::acquire(&clerk_base).await;

    let resp = approve_join_request(
        State(state.clone()),
        org_tenant(control.clone(), &org_id, account_id, admin_id),
        Extension(admin_ctx(account_id, admin_id)),
        Path(id),
        Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let seat_purchased_before: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .unwrap();

    // Replay `organizationMembership.created` for the SAME membership.
    let app = webhook_app(control.clone());
    let resp = app
        .oneshot(membership_created_webhook(&org_id, &requester_clerk, &requester_email))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "the webhook must ack, not reject, a redelivery");

    let member_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(member_rows, 1, "still exactly one membership row");

    let seat_purchased_after: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(seat_purchased_before, seat_purchased_after, "seat_purchased_at must be stamped exactly once");

    let reservation_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM slot_reservations WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(requester_id)
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(reservation_rows, 1, "still exactly one reservation row (fulfilled, not re-created)");
}

// ── Test 7: a stubbed Clerk 500 unwinds and leaves the request open ────────

#[tokio::test]
async fn clerk_500_on_approve_unwinds_and_leaves_the_request_open() {
    let Some(base) = base_url() else {
        eprintln!("skipping clerk_500_on_approve_unwinds_and_leaves_the_request_open: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_jr7_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let (account_id, org_id, admin_id, _) = seed_org(&control, "jr7", 2).await;
    let (requester_id, _requester_clerk) = seed_requester(&control, "jr7").await;
    let state = app_state(control.clone());

    let resp = create_join_request(
        State(state.clone()),
        Extension(requester_ctx(requester_id)),
        Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let id: Uuid = sqlx::query_scalar(
        "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
    )
    .bind(account_id)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .unwrap();

    let (clerk_base, _clerk_mock) = spawn_clerk_mock(500).await;
    let _env = EnvGuard::acquire(&clerk_base).await;

    let resp = approve_join_request(
        State(state.clone()),
        org_tenant(control.clone(), &org_id, account_id, admin_id),
        Extension(admin_ctx(account_id, admin_id)),
        Path(id),
        Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "a Clerk 5xx must map to 502, not silently succeed");

    let member_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(member_rows, 0, "no account_members row on a Clerk failure");

    let (released_at, release_reason): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) = sqlx::query_as(
        "SELECT released_at, release_reason FROM slot_reservations WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(requester_id)
    .fetch_one(&control)
    .await
    .expect("the reservation reserve_slot took must still exist");
    assert!(released_at.is_some(), "the reservation must be released, not left dangling");
    assert_eq!(release_reason.as_deref(), Some("released"));

    let live_reservations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM slot_reservations WHERE account_id = $1 AND released_at IS NULL",
    )
    .bind(account_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(live_reservations, 0, "the pool must show 0 live reservations after the unwind");

    let decision: Option<String> =
        sqlx::query_scalar("SELECT decision FROM org_join_requests WHERE id = $1").bind(id).fetch_one(&control).await.unwrap();
    assert_eq!(decision, None, "the request must stay OPEN so an admin can retry");
}
