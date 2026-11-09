//! `workers::org_join_request_digest` — CS-438, org-onboarding plan §3 item
//! 5, D13 (`docs/plans/org-onboarding-from-the-invite.md` ~line 514) — plus
//! the members-page badge's own read, `GET /v1/orgs/join-requests/count`.
//!
//! Drives the worker through its ONE public entry point, [`tick`], exactly
//! as `workers::tenant_harness::run_multi_tenant` would call it — every
//! lower-level function this worker exposes (`process_account`, `Outcome`)
//! is `pub(crate)`, so this EXTERNAL suite proves behavior the same way
//! production observes it: by watching the mock Resend server's captured
//! requests and by reading `org_join_request_digests` back with a direct
//! connection, same discipline `member_connect_nudge_pg.rs` already
//! established for its own sibling worker.
//!
//! `count` (the badge route) is driven the same way
//! `org_join_requests_pg.rs` drives `list`/`approve`/`reject`: the real
//! handler (re-exported at the crate root), with a hand-built
//! `Tenant`/`UserContext`.
//!
//! Properties covered:
//!
//! 1. One org, two admins, three open requests: exactly two emails, two
//!    digest rows, `request_count = 3`, subject says "3 people".
//! 2. A second tick the SAME day sends nothing more (`digests_skipped_
//!    already_today`, not `digests_skipped_no_new` — the two are checked in
//!    that order; see the worker's own module doc for why they must stay
//!    distinguishable).
//! 3. A LATER day (simulated by pre-seeding yesterday's digest row directly
//!    — no clock is mocked) with no NEW requests sends nothing
//!    (`digests_skipped_no_new`).
//! 4. A later day with exactly one NEW request sends one email per admin,
//!    subject says "1 person" — the two people already reported are never
//!    repeated.
//! 5. A personal org with an open row (inserted directly — never a
//!    production shape, D19) is never emailed: the admin-roster query's own
//!    `personal_orgs` anti-join excludes it.
//! 6. An admin with no verified email (`users.email = ''`) is counted
//!    (`admins_without_address`), never crashes the tick, and every OTHER
//!    admin still gets their digest.
//! 7. A Resend 500 leaves no digest row behind (`send_failed`, the claim
//!    rolls back) — the very next tick, against a healthy Resend, sends.
//! 8. `GET /v1/orgs/join-requests/count`: `{open: 3}` for an admin, 403 for
//!    an ordinary member, 403 for a personal org.
//!
//! Gated on `ORGT_TEST_BASE_URL` — same scratch-DB fixture pattern as every
//! other `*_pg.rs` file in this directory. Every table this worker touches
//! is control-plane-only, so no tenant database is created at all.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test org_join_request_digest_pg -- \
//!     --test-threads=1 --nocapture

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Extension;
use chrono::{DateTime, NaiveDate, Utc};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::extractor::Tenant;
use coralswarm_backend::tenancy::routing::TenantStatus;
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::org_join_request_digest::tick;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::{count_join_requests, AppState, UserContext};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

// ── Scratch-DB plumbing (pattern shared by every `*_pg.rs` file) ─────────────

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
    let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

/// Serializes every test in this file — `RESEND_API_BASE_URL` is a
/// process-global env var each test mutates to its own per-test mock
/// address, same convention as every other `*_pg.rs` file's `ENV_LOCK`.
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

#[allow(dead_code)]
struct EnvGuard(tokio::sync::MutexGuard<'static, ()>);
impl EnvGuard {
    async fn acquire(resend_base: &str) -> Self {
        let guard = ENV_LOCK.lock().await;
        // SAFETY: serialized by `guard`, held for this struct's whole
        // lifetime (including on panic/unwind, via `Drop` below).
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", resend_base);
        }
        Self(guard)
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: see `acquire` — still serialized by `self.0` here.
        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
        }
    }
}

// ── Resend mock — lifted from `routes::org_invites::pg_tests::spawn_resend_mock` /
//    `member_connect_nudge_pg.rs`'s own copy ──────────────────────────────────

#[derive(Default)]
struct ResendMock {
    status: u16,
    requests: Vec<Value>,
}
type SharedResendMock = Arc<StdMutex<ResendMock>>;

async fn spawn_resend_mock(status: u16) -> (String, SharedResendMock) {
    let state: SharedResendMock = Arc::new(StdMutex::new(ResendMock { status, requests: Vec::new() }));
    let app = axum::Router::new()
        .route("/emails", axum::routing::post(resend_send))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind resend mock");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://{addr}"), state)
}

async fn resend_send(
    axum::extract::State(state): axum::extract::State<SharedResendMock>,
    axum::Json(body): axum::Json<Value>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let mut s = state.lock().unwrap();
    s.requests.push(body);
    let status = axum::http::StatusCode::from_u16(s.status).unwrap();
    (status, axum::Json(json!({"id": "email_mock_1"}))).into_response()
}

fn set_mock_status(mock: &SharedResendMock, status: u16) {
    mock.lock().unwrap().status = status;
}
fn mock_request_count(mock: &SharedResendMock) -> usize {
    mock.lock().unwrap().requests.len()
}
fn mock_subjects(mock: &SharedResendMock) -> Vec<String> {
    mock.lock()
        .unwrap()
        .requests
        .iter()
        .map(|r| r["subject"].as_str().unwrap_or_default().to_string())
        .collect()
}

// ── Control-DB seeding ────────────────────────────────────────────────────────

/// An org account with a `tenant_databases` routing row — `resolve_org_
/// account_id` (reused by `org_join_request_digest::tick`) needs it to
/// resolve `clerk_org_id -> account_id`, same as `member_connect_nudge_pg.rs`'s
/// own `mint_org`.
async fn mint_org(control: &PgPool, org_id: &str, slug: &str) -> Uuid {
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("{org_id}-owner@join-digest-pg.test"))
        .fetch_one(control)
        .await
        .unwrap();
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(owner)
            .bind(org_id)
            .fetch_one(control)
            .await
            .unwrap();
    execute_financial_fixture(
        control,
        account_id,
    sqlx::query(
        "INSERT INTO tenant_databases \
             (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
         VALUES ($1, $2, $3, $4, 'neon', 1, $5)",
    )
    .bind(org_id)
    .bind(account_id)
    .bind(slug)
    .bind(format!("secret_ref_{org_id}"))
    .bind(TenantStatus::Active.as_str()),
    )
    .await
    .unwrap();
    account_id
}

/// An admin (`role IN ('owner','admin')`) with a verified email — `seat_
/// tier_id` passed explicitly, same "required since seat lifecycle PR F"
/// note `member_connect_nudge_pg.rs::add_member`'s own comment gives.
async fn add_admin(control: &PgPool, account_id: Uuid, email: &str, role: &str) -> Uuid {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(control)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO account_members(account_id, user_id, role, joined_at, seat_tier_id) \
         VALUES ($1, $2, $3, now(), (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(role)
    .execute(control)
    .await
    .unwrap();
    user_id
}

/// A signed-in requester with an OPEN `org_join_requests` row, inserted
/// directly (never through `POST /v1/join-requests`) — this suite tests the
/// digest worker's own read of the queue, not the queue's own write path
/// (already covered by `org_join_requests_pg.rs`).
async fn add_open_request(
    control: &PgPool,
    account_id: Uuid,
    email: &str,
    name: Option<&str>,
    requested_at: DateTime<Utc>,
) -> Uuid {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email, name) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind(name)
        .fetch_one(control)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO org_join_requests (account_id, user_id, source, evidence, requested_at) \
         VALUES ($1, $2, 'self_request', '{}', $3)",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(requested_at)
    .execute(control)
    .await
    .unwrap();
    user_id
}

/// Plants yesterday's digest row directly — the "advance to a later day"
/// trick this suite's own module doc names: no clock is mocked, the worker
/// always reads the REAL `Utc::now()`, so a pre-seeded `sent_on` of
/// yesterday is always `< today()` regardless of when this test happens to
/// run, and `sent_at` is what the worker's own `new_since` cutoff reads.
///
/// `auto_add_count` (CS-447) is the second section's own count. It is a
/// real parameter rather than a hardcoded `0` because migration 0247's
/// `CHECK (request_count + auto_add_count > 0)` means a planted row must
/// describe a digest that could actually have been SENT — and the case
/// this suite most needs to plant, "yesterday we reported one auto-add and
/// no requests", is exactly the one a hardcoded `0` cannot express. (The
/// constraint caught that on the first run of
/// `an_auto_add_already_reported_is_never_reported_again`, which is the
/// gate working: an empty digest is unrepresentable, in fixtures too.)
async fn plant_yesterdays_digest(
    control: &PgPool,
    account_id: Uuid,
    admin_user_id: Uuid,
    sent_at: DateTime<Utc>,
    request_count: i32,
    auto_add_count: i32,
) {
    let sent_on: NaiveDate = (sent_at - chrono::Duration::days(1)).date_naive();
    sqlx::query(
        "INSERT INTO org_join_request_digests \
             (account_id, admin_user_id, sent_on, request_count, auto_add_count, sent_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(account_id)
    .bind(admin_user_id)
    .bind(sent_on)
    .bind(request_count)
    .bind(auto_add_count)
    .bind(sent_at)
    .execute(control)
    .await
    .unwrap();
}

async fn digest_row_count(control: &PgPool, account_id: Uuid, admin_user_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM org_join_request_digests WHERE account_id = $1 AND admin_user_id = $2",
    )
    .bind(account_id)
    .bind(admin_user_id)
    .fetch_one(control)
    .await
    .unwrap()
}

fn app_state(control: PgPool) -> Arc<AppState> {
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control)));
    state.resend_api_key = Some("test_resend_key".to_string());
    Arc::new(state)
}

fn worker_tenant(org_id: &str, pool: PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: TenantId::Org(org_id.to_string()),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn coralswarm_backend::store::Store>,
        pool,
    }
}

fn admin_ctx(account_id: Uuid, user_id: Uuid) -> UserContext {
    UserContext { user_id, account_id, role: "admin".to_string(), device_id: "jrd-pg-test".to_string() }
}
fn member_ctx(account_id: Uuid, user_id: Uuid) -> UserContext {
    UserContext { user_id, account_id, role: "member".to_string(), device_id: "jrd-pg-test".to_string() }
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn three_open_requests_two_admins_sends_two_digests_with_full_count() {
    let Some(base) = base_url() else {
        eprintln!("skipping three_open_requests_two_admins_sends_two_digests_with_full_count: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd1").await;
    let admin_a = add_admin(&control, account_id, "admin-a@join-digest-pg.test", "admin").await;
    let admin_b = add_admin(&control, account_id, "admin-b@join-digest-pg.test", "owner").await;

    let now = Utc::now();
    add_open_request(&control, account_id, "dana@join-digest-pg.test", Some("Dana"), now).await;
    add_open_request(&control, account_id, "erik@join-digest-pg.test", Some("Erik"), now).await;
    add_open_request(&control, account_id, "fiona@join-digest-pg.test", None, now).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.expect("tick succeeds");

    assert_eq!(mock_request_count(&mock), 2, "one digest per admin");
    assert_eq!(digest_row_count(&control, account_id, admin_a).await, 1);
    assert_eq!(digest_row_count(&control, account_id, admin_b).await, 1);

    let request_counts: Vec<i32> = sqlx::query_scalar(
        "SELECT request_count FROM org_join_request_digests WHERE account_id = $1 ORDER BY admin_user_id",
    )
    .bind(account_id)
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(request_counts, vec![3, 3], "each admin's digest reports all three requests");

    for subject in mock_subjects(&mock) {
        assert!(subject.contains("3 people asked to join"), "subject was: {subject}");
        assert!(subject.ends_with("on CoralSwarm"), "subject was: {subject}");
    }
}

#[tokio::test]
async fn second_tick_same_day_sends_nothing_more() {
    let Some(base) = base_url() else {
        eprintln!("skipping second_tick_same_day_sends_nothing_more: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd2_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd2").await;
    add_admin(&control, account_id, "admin-a@join-digest-pg.test", "admin").await;
    add_admin(&control, account_id, "admin-b@join-digest-pg.test", "owner").await;
    add_open_request(&control, account_id, "dana@join-digest-pg.test", Some("Dana"), Utc::now()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.unwrap();
    assert_eq!(mock_request_count(&mock), 2, "first tick sends one digest per admin");

    tick(&t, &control, &state).await.unwrap();
    assert_eq!(mock_request_count(&mock), 2, "second tick the same day sends nothing more");
}

#[tokio::test]
async fn later_day_with_no_new_requests_sends_nothing() {
    let Some(base) = base_url() else {
        eprintln!("skipping later_day_with_no_new_requests_sends_nothing: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd3").await;
    let admin_a = add_admin(&control, account_id, "admin-a@join-digest-pg.test", "admin").await;
    let requested_at = Utc::now() - chrono::Duration::hours(30);
    add_open_request(&control, account_id, "dana@join-digest-pg.test", Some("Dana"), requested_at).await;

    // Yesterday's digest already reported this same request — sent_at AFTER
    // requested_at, so today's `new_since` cutoff finds nothing new.
    plant_yesterdays_digest(&control, account_id, admin_a, requested_at + chrono::Duration::hours(1), 1, 0).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.unwrap();

    assert_eq!(mock_request_count(&mock), 0, "no new requests since yesterday's digest");
    assert_eq!(digest_row_count(&control, account_id, admin_a).await, 1, "still only yesterday's row");
}

#[tokio::test]
async fn later_day_with_one_new_request_sends_one_person() {
    let Some(base) = base_url() else {
        eprintln!("skipping later_day_with_one_new_request_sends_one_person: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd4_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd4").await;
    let admin_a = add_admin(&control, account_id, "admin-a@join-digest-pg.test", "admin").await;

    let old_requested_at = Utc::now() - chrono::Duration::hours(30);
    add_open_request(&control, account_id, "dana@join-digest-pg.test", Some("Dana"), old_requested_at).await;
    plant_yesterdays_digest(&control, account_id, admin_a, old_requested_at + chrono::Duration::hours(1), 1, 0).await;

    // A genuinely new request, after yesterday's digest sent_at.
    add_open_request(&control, account_id, "gio@join-digest-pg.test", Some("Gio"), Utc::now()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.unwrap();

    assert_eq!(mock_request_count(&mock), 1);
    let subjects = mock_subjects(&mock);
    assert!(subjects[0].contains("1 person asked to join"), "subject was: {}", subjects[0]);
    assert_eq!(digest_row_count(&control, account_id, admin_a).await, 2, "yesterday's row plus today's");

    let today_count: i32 = sqlx::query_scalar(
        "SELECT request_count FROM org_join_request_digests \
          WHERE account_id = $1 AND admin_user_id = $2 ORDER BY sent_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(admin_a)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(today_count, 1, "only the ONE new person, not both");
}

#[tokio::test]
async fn personal_org_open_request_is_never_emailed() {
    let Some(base) = base_url() else {
        eprintln!("skipping personal_org_open_request_is_never_emailed: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd5_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    // Same "mint an ordinary org, then retrofit a personal_orgs row" trick
    // `member_connect_nudge_pg.rs::personal_org_member_is_never_a_candidate`
    // uses, for the same reason: production never routes a personal org
    // through this worker's tick at all (no `tenant_databases` row — see
    // `tenancy::personal_org`'s own module doc), so this exercises the
    // worker's OWN `personal_orgs` anti-join defense-in-depth directly.
    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd5").await;
    let owner_user_id: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    let owner_clerk_user_id = format!("user_{}", Uuid::new_v4());
    sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
        .bind(&owner_clerk_user_id)
        .bind(owner_user_id)
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
        .bind(&org_id)
        .bind(&owner_clerk_user_id)
        .execute(&control)
        .await
        .unwrap();
    // The owner is also, structurally, an "admin" row (0001's own default
    // membership shape) — still excluded by the `personal_orgs` anti-join.
    add_admin(&control, account_id, "solo@join-digest-pg.test", "owner").await;
    add_open_request(&control, account_id, "curious@join-digest-pg.test", Some("Curious"), Utc::now()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.unwrap();

    assert_eq!(mock_request_count(&mock), 0, "a personal org is never emailed");
}

#[tokio::test]
async fn admin_without_a_verified_email_is_counted_but_others_still_get_theirs() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping admin_without_a_verified_email_is_counted_but_others_still_get_theirs: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd6_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd6").await;
    // Defensive-only branch: `users.email` is `NOT NULL` (migration 0001)
    // but carries no non-empty CHECK, so this is reachable only by direct
    // insert — see `workers::member_connect_nudge`'s own identical
    // dead-but-defended branch precedent.
    add_admin(&control, account_id, "", "admin").await;
    add_admin(&control, account_id, "admin-with-email@join-digest-pg.test", "owner").await;
    add_open_request(&control, account_id, "dana@join-digest-pg.test", Some("Dana"), Utc::now()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.unwrap();

    assert_eq!(mock_request_count(&mock), 1, "the admin with a real address still gets their digest");
}

#[tokio::test]
async fn resend_failure_leaves_no_digest_row_and_the_next_tick_sends() {
    let Some(base) = base_url() else {
        eprintln!("skipping resend_failure_leaves_no_digest_row_and_the_next_tick_sends: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(500).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd7_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd7").await;
    let admin_a = add_admin(&control, account_id, "admin-a@join-digest-pg.test", "admin").await;
    add_open_request(&control, account_id, "dana@join-digest-pg.test", Some("Dana"), Utc::now()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());

    tick(&t, &control, &state).await.unwrap();
    assert_eq!(mock_request_count(&mock), 1, "one send attempt was made");
    assert_eq!(
        digest_row_count(&control, account_id, admin_a).await,
        0,
        "a failed send must not leave a claimed digest row behind"
    );

    set_mock_status(&mock, 200);
    tick(&t, &control, &state).await.unwrap();
    assert_eq!(mock_request_count(&mock), 2, "the retried send went out");
    assert_eq!(digest_row_count(&control, account_id, admin_a).await, 1, "now claimed, on the successful send");
}

// ── GET /v1/orgs/join-requests/count ────────────────────────────────────────

#[tokio::test]
async fn count_route_reports_open_count_for_an_admin_and_refuses_a_member() {
    let Some(base) = base_url() else {
        eprintln!("skipping count_route_reports_open_count_for_an_admin_and_refuses_a_member: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_jrd8_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd8").await;
    let admin_id = add_admin(&control, account_id, "admin@join-digest-pg.test", "admin").await;
    let member_id = add_admin(&control, account_id, "member@join-digest-pg.test", "member").await;

    add_open_request(&control, account_id, "dana@join-digest-pg.test", Some("Dana"), Utc::now()).await;
    add_open_request(&control, account_id, "erik@join-digest-pg.test", Some("Erik"), Utc::now()).await;
    add_open_request(&control, account_id, "fiona@join-digest-pg.test", Some("Fiona"), Utc::now()).await;

    let state = app_state(control.clone());

    let resp = count_join_requests(
        State(state.clone()),
        org_tenant(control.clone(), &org_id, account_id, admin_id),
        Extension(admin_ctx(account_id, admin_id)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["open"], 3);

    let resp = count_join_requests(
        State(state.clone()),
        org_tenant(control.clone(), &org_id, account_id, member_id),
        Extension(member_ctx(account_id, member_id)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "an ordinary member may not read the admin badge count");
}

#[tokio::test]
async fn count_route_refuses_a_personal_org() {
    let Some(base) = base_url() else {
        eprintln!("skipping count_route_refuses_a_personal_org: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_jrd9_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrd9").await;
    let owner_user_id: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    let owner_clerk_user_id = format!("user_{}", Uuid::new_v4());
    sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
        .bind(&owner_clerk_user_id)
        .bind(owner_user_id)
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
        .bind(&org_id)
        .bind(&owner_clerk_user_id)
        .execute(&control)
        .await
        .unwrap();

    let state = app_state(control.clone());
    let resp = count_join_requests(
        State(state.clone()),
        org_tenant(control.clone(), &org_id, account_id, owner_user_id),
        Extension(admin_ctx(account_id, owner_user_id)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "a personal org has no join-request badge count");
}

// ── CS-447 / D13 as amended 2026-09-22: the digest's second section ─────────
//
// An auto-added member never creates a join request, so before this
// amendment nothing in the product told an admin it had happened. These
// tests drive the SAME `tick` as everything above — no separate worker, no
// separate cadence, no separate claim.

/// Record an auto-add directly, the way `routes::org_join_requests::create`
/// does after a real membership exists. `added_at` is explicit so a test can
/// place one before or after a planted digest.
async fn add_auto_add(
    control: &PgPool,
    account_id: Uuid,
    email: &str,
    name: Option<&str>,
    rung: &str,
    added_at: DateTime<Utc>,
) -> Uuid {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email, name) VALUES ($1, $2) RETURNING id")
        .bind(email)
        .bind(name)
        .fetch_one(control)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO org_auto_adds (account_id, user_id, rung, evidence, added_at) \
         VALUES ($1, $2, $3, '{}', $4)",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(rung)
    .bind(added_at)
    .execute(control)
    .await
    .unwrap();
    user_id
}

fn mock_bodies(mock: &SharedResendMock) -> Vec<String> {
    mock.lock()
        .unwrap()
        .requests
        .iter()
        .map(|r| r["html"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// **CS-447 / D13's amendment.** Nobody asked; two people were auto-added.
///
/// Before this change the account-level early-out (`has_open_requests`)
/// returned immediately and no digest was ever sent — the auto-add section
/// would have been unreachable for exactly the orgs it matters most to, the
/// ones where auto-add is working and nobody has to ask. The assertion that
/// catches that regression is the FIRST one: a digest exists at all.
///
/// It also pins the two things migration 0247 had to change for this to be
/// representable: `request_count = 0` (0240's `CHECK (request_count > 0)`
/// would have refused this row outright) and `auto_add_count = 2`.
#[tokio::test]
async fn auto_adds_alone_send_a_digest_with_a_zero_request_count() {
    let Some(base) = base_url() else {
        eprintln!("skipping auto_adds_alone_send_a_digest_with_a_zero_request_count: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd_aa1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrdaa1").await;
    let admin = add_admin(&control, account_id, "admin-aa1@join-digest-pg.test", "admin").await;

    let now = Utc::now();
    add_auto_add(&control, account_id, "gita@join-digest-pg.test", Some("Gita"), "member_and_domain", now).await;
    add_auto_add(&control, account_id, "hugo@join-digest-pg.test", Some("Hugo"), "directory", now).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.expect("tick succeeds");

    assert_eq!(
        mock_request_count(&mock),
        1,
        "an org with an empty queue and two auto-adds still owes its admin a digest"
    );
    assert_eq!(digest_row_count(&control, account_id, admin).await, 1);

    let (request_count, auto_add_count): (i32, i32) = sqlx::query_as(
        "SELECT request_count, auto_add_count FROM org_join_request_digests WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(request_count, 0, "0247 re-stated 0240's CHECK over BOTH counts so this row can exist");
    assert_eq!(auto_add_count, 2);

    let subject = mock_subjects(&mock).remove(0);
    assert!(subject.contains("2 people were added to"), "subject was: {subject}");
    assert!(!subject.contains("asked to join"), "nobody asked — subject was: {subject}");

    // D13's amendment, literally: "…and which rung they qualified on."
    let body = mock_bodies(&mock).remove(0);
    assert!(body.contains("added automatically"), "the second section must be labelled as itself");
    assert!(body.contains("Gita"), "body was missing a name");
    assert!(body.contains("full member of your workspace, on your verified domain"), "rung 2 must be spelled out");
    assert!(body.contains("listed in your directory"), "rung 1 must be spelled out");
    assert!(
        !body.contains("Review requests"),
        "an auto-add-only digest asks for no decision, so it carries no call to action"
    );
    assert!(body.contains("join activity"), "eyebrow must not say join requests for an auto-add-only digest");
    assert!(
        body.contains("only when someone newly asks to join or is added automatically"),
        "footer must mention auto-add; body was: {body}"
    );
    assert!(!body.contains("only when someone new has asked"), "request-only footer must not ship");
}

/// Both sections at once, visually distinct and separately counted.
///
/// The subject names both facts rather than hiding the automatic one in the
/// body — the automatic one is the one that spent money without asking.
#[tokio::test]
async fn a_digest_with_both_sections_counts_and_names_them_separately() {
    let Some(base) = base_url() else {
        eprintln!("skipping a_digest_with_both_sections_counts_and_names_them_separately: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd_aa2_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrdaa2").await;
    let admin = add_admin(&control, account_id, "admin-aa2@join-digest-pg.test", "admin").await;

    let now = Utc::now();
    add_open_request(&control, account_id, "ines@join-digest-pg.test", Some("Ines"), now).await;
    add_auto_add(&control, account_id, "jonas@join-digest-pg.test", Some("Jonas"), "member_and_domain", now).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.expect("tick succeeds");

    assert_eq!(mock_request_count(&mock), 1, "ONE email — one admin, one day, both sections");
    let (request_count, auto_add_count): (i32, i32) = sqlx::query_as(
        "SELECT request_count, auto_add_count FROM org_join_request_digests \
          WHERE account_id = $1 AND admin_user_id = $2",
    )
    .bind(account_id)
    .bind(admin)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!((request_count, auto_add_count), (1, 1));

    let subject = mock_subjects(&mock).remove(0);
    assert!(subject.contains("1 person asked to join"), "subject was: {subject}");
    assert!(subject.contains("1 was added automatically"), "subject was: {subject}");

    let body = mock_bodies(&mock).remove(0);
    assert!(body.contains("Ines"));
    assert!(body.contains("Jonas"));
    assert!(body.contains("Review requests"), "the ASKING section still carries its call to action");
    assert!(body.contains("added automatically"), "and the reporting section is separately labelled");
    assert!(
        body.contains("no one is in yet until you approve them"),
        "the asking section keeps its own copy — the two must not blur"
    );
}

/// The idempotency did not fork: an auto-add reported yesterday is never
/// reported again, and it is the SAME `(account, admin, sent_on)` key and
/// the SAME per-admin `sent_at` cutoff that makes that true.
///
/// Also the regression proof for `has_unreported_auto_adds`' own bound: an
/// org whose every auto-add predates every admin's last digest must skip at
/// the ACCOUNT level, not merely produce an empty section.
#[tokio::test]
async fn an_auto_add_already_reported_is_never_reported_again() {
    let Some(base) = base_url() else {
        eprintln!("skipping an_auto_add_already_reported_is_never_reported_again: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd_aa3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrdaa3").await;
    let admin = add_admin(&control, account_id, "admin-aa3@join-digest-pg.test", "admin").await;

    let now = Utc::now();
    let two_days_ago = now - chrono::Duration::days(2);
    // Auto-added two days ago, and reported in yesterday's digest.
    add_auto_add(&control, account_id, "kira@join-digest-pg.test", Some("Kira"), "member_and_domain", two_days_ago)
        .await;
    // Yesterday's digest reported exactly one thing: Kira's auto-add.
    plant_yesterdays_digest(&control, account_id, admin, now - chrono::Duration::days(1), 0, 1).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.expect("tick succeeds");
    assert_eq!(mock_request_count(&mock), 0, "a fact already reported is never reported twice");
    assert_eq!(digest_row_count(&control, account_id, admin).await, 1, "no new digest row");

    // One NEW auto-add, and the same admin hears about exactly that one.
    add_auto_add(&control, account_id, "liam@join-digest-pg.test", Some("Liam"), "directory", now).await;
    tick(&t, &control, &state).await.expect("second tick succeeds");
    assert_eq!(mock_request_count(&mock), 1);
    let body = mock_bodies(&mock).remove(0);
    assert!(body.contains("Liam"), "the new one is reported");
    assert!(!body.contains("Kira"), "the old one is not repeated");
    let subject = mock_subjects(&mock).remove(0);
    assert!(subject.contains("1 person was added to"), "subject was: {subject}");
}

/// Claim-then-send-then-commit, for the new section too: a failed Resend
/// send must leave no digest row, so the next tick retries rather than
/// burning the admin's one notification for the day.
///
/// Deliberately an auto-add-ONLY digest. The existing sibling test proves
/// this for requests; a section that rode a different claim, or committed
/// before sending, would pass that one and fail this.
#[tokio::test]
async fn a_failed_send_of_an_auto_add_digest_leaves_no_row_and_retries() {
    let Some(base) = base_url() else {
        eprintln!("skipping a_failed_send_of_an_auto_add_digest_leaves_no_row_and_retries: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(500).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_jrd_aa4_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "jrdaa4").await;
    let admin = add_admin(&control, account_id, "admin-aa4@join-digest-pg.test", "admin").await;
    add_auto_add(&control, account_id, "mira@join-digest-pg.test", Some("Mira"), "member_and_domain", Utc::now())
        .await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.expect("tick succeeds even when Resend 500s");
    assert_eq!(mock_request_count(&mock), 1, "it tried");
    assert_eq!(
        digest_row_count(&control, account_id, admin).await,
        0,
        "a failed send must never consume the claim"
    );

    set_mock_status(&mock, 200);
    tick(&t, &control, &state).await.expect("second tick succeeds");
    assert_eq!(mock_request_count(&mock), 2, "the very next tick retries");
    assert_eq!(digest_row_count(&control, account_id, admin).await, 1);
}
