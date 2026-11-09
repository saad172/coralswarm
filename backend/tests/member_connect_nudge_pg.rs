//! `workers::member_connect_nudge` — CS-436, org-onboarding plan §3 item 49
//! (D21, `docs/plans/org-onboarding-from-the-invite.md` ~line 1117 for D21,
//! ~line 1329 for item 49's own row).
//!
//! Drives the worker through its ONE public entry point, [`tick`], exactly
//! as `workers::tenant_harness::run_multi_tenant` would call it — every
//! lower-level function this worker exposes (`process_account`,
//! `candidates_for_account`, `Outcome`) is `pub(crate)`, so this EXTERNAL
//! suite proves behavior the same way production observes it: by watching
//! the mock Resend server's captured requests and by reading
//! `slack_unmatched_email_notices` back with a direct connection, never by
//! reaching into the worker's own counters.
//!
//! Properties covered:
//!
//! 1. A member past grace with no owned `oauth_tokens` row gets exactly one
//!    email; the claimed row carries `provider = 'member_connect'`.
//! 2. A second tick inside the 7-day cooldown sends nothing more.
//! 3. An email already suppressed under a DIFFERENT provider (`'slack'`,
//!    the sibling nudge's own value) is never sent this one either — the
//!    "same suppression list" the story's brief requires.
//! 4. A member who owns an `oauth_tokens` row for this account is never a
//!    candidate.
//! 5. A member still inside the grace window is never a candidate.
//! 6. A personal-org member is never a candidate, even when (defense in
//!    depth, not a real production shape — see the test's own comment) its
//!    `clerk_org_id` is given a `tenant_databases` row so `tick` can reach
//!    the candidate query at all.
//! 7. A Resend 500 leaves the cooldown UNCONSUMED — a `500` tick sends
//!    nothing durable (no `member_connect` row survives), and the very next
//!    tick against a healthy Resend succeeds. This is the deliberate
//!    divergence from `workers::slack_unmatched_email`'s own claim-then-send
//!    order (see `workers::member_connect_nudge`'s module doc, "Claim vs
//!    send order" section).
//! 8. An already-cooled-down member (CodeRabbit round 1 starvation fix) is
//!    excluded from the candidate SELECT itself, not merely skipped after
//!    being selected — proven via `Tick::processed`, the observable proxy
//!    for the SELECT's own row count.
//!
//! Gated on `ORGT_TEST_BASE_URL` — same scratch-DB fixture pattern as every
//! other `*_pg.rs` file in this directory. Every table this worker touches
//! is control-plane-only, so no tenant database is created at all.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test member_connect_nudge_pg -- \
//!     --test-threads=1 --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::routing::TenantStatus;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::member_connect_nudge::tick;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::AppState;
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

// ── Resend mock — lifted from `routes::org_invites::pg_tests::spawn_resend_mock` ──

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

// ── Control-DB seeding ────────────────────────────────────────────────────────

/// An org account with a `tenant_databases` routing row — `resolve_org_
/// account_id` (`workers::billing_derivation`, reused by `member_connect_
/// nudge::tick`) needs the latter to resolve `clerk_org_id -> account_id` at
/// all, same as `member_ocean_lifecycle_pg.rs`'s own `mint_org`.
async fn mint_org(control: &PgPool, org_id: &str, slug: &str) -> Uuid {
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("{org_id}-owner@member-connect-pg.test"))
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
    financial_fixture::execute(
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

/// A member row with an explicit `joined_at` — `past_grace` picks a
/// timestamp comfortably outside the default 24h grace window; the caller
/// passes `Utc::now()` (or later) for an "inside grace" candidate.
async fn add_member(control: &PgPool, account_id: Uuid, email: &str, joined_at: DateTime<Utc>) -> Uuid {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(control)
        .await
        .unwrap();
    // `seat_tier_id` is required explicitly since PR F (seat lifecycle) —
    // `account_members_default_seat_tier()` no longer defaults it silently
    // (same pattern `routes::org_invites::pg_tests::seed_org` uses).
    sqlx::query(
        "INSERT INTO account_members(account_id, user_id, role, joined_at, seat_tier_id) \
         VALUES ($1, $2, 'member', $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(joined_at)
    .execute(control)
    .await
    .unwrap();
    user_id
}

fn past_grace() -> DateTime<Utc> {
    Utc::now() - chrono::Duration::hours(48)
}

async fn insert_owned_token(control: &PgPool, account_id: Uuid, user_id: Uuid, provider: &str) {
    sqlx::query(
        "INSERT INTO oauth_tokens (account_id, provider, scope, access_token, owner_user_id) \
         VALUES ($1, $2, 'scope', $3, $4)",
    )
    .bind(account_id)
    .bind(provider)
    .bind(b"ciphertext".to_vec())
    .bind(user_id)
    .execute(control)
    .await
    .unwrap();
}

async fn insert_suppressed_notice(control: &PgPool, account_id: Uuid, provider: &str, email: &str) {
    sqlx::query(
        "INSERT INTO slack_unmatched_email_notices \
             (account_id, provider, email, last_notified_at, suppressed_at, unsubscribe_token) \
         VALUES ($1, $2, $3, now() - INTERVAL '30 days', now(), $4)",
    )
    .bind(account_id)
    .bind(provider)
    .bind(email)
    .bind(format!("suppressed-{}", Uuid::new_v4()))
    .execute(control)
    .await
    .unwrap();
}

async fn member_connect_row_count(control: &PgPool, email: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM slack_unmatched_email_notices WHERE provider = 'member_connect' AND email = $1",
    )
    .bind(email)
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
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
        pool,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn member_past_grace_with_no_owned_token_gets_exactly_one_email() {
    let Some(base) = base_url() else {
        eprintln!("skipping member_past_grace_with_no_owned_token_gets_exactly_one_email: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_mcn1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "mcn1").await;
    let email = "dana@member-connect-pg.test";
    add_member(&control, account_id, email, past_grace()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    let outcome = tick(&t, &control, &state).await.expect("tick succeeds");
    assert_eq!(outcome.processed, 1, "one candidate looked at");

    assert_eq!(mock_request_count(&mock), 1, "exactly one email sent");
    assert_eq!(member_connect_row_count(&control, email).await, 1);

    let provider: String = sqlx::query_scalar(
        "SELECT provider FROM slack_unmatched_email_notices WHERE email = $1",
    )
    .bind(email)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(provider, "member_connect");
}

#[tokio::test]
async fn second_tick_inside_cooldown_sends_nothing_more() {
    let Some(base) = base_url() else {
        eprintln!("skipping second_tick_inside_cooldown_sends_nothing_more: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_mcn2_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "mcn2").await;
    let email = "cooldown@member-connect-pg.test";
    add_member(&control, account_id, email, past_grace()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    tick(&t, &control, &state).await.unwrap();
    assert_eq!(mock_request_count(&mock), 1, "first tick sends");

    tick(&t, &control, &state).await.unwrap();
    assert_eq!(mock_request_count(&mock), 1, "second tick inside the 7-day cooldown sends nothing more");
    assert_eq!(member_connect_row_count(&control, email).await, 1, "still exactly one row, not a duplicate");
}

#[tokio::test]
async fn any_provider_suppression_blocks_the_member_connect_send() {
    let Some(base) = base_url() else {
        eprintln!("skipping any_provider_suppression_blocks_the_member_connect_send: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_mcn3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "mcn3").await;
    let email = "suppressed@member-connect-pg.test";
    add_member(&control, account_id, email, past_grace()).await;
    // Suppressed under the SIBLING nudge's provider, not this one's — the
    // "same suppression list" property this story's brief requires.
    insert_suppressed_notice(&control, account_id, "slack", email).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    let outcome = tick(&t, &control, &state).await.unwrap();

    // CodeRabbit round 1 (starvation fix): an any-provider-suppressed email
    // is excluded from `candidates_for_account`'s own SELECT, not merely
    // skipped after being selected — `outcome.processed` (Tick's `processed`
    // field) is the observable proxy for that SELECT's row count.
    assert_eq!(outcome.processed, 0, "a suppressed email is never even a candidate");
    assert_eq!(mock_request_count(&mock), 0, "never sent — suppressed under a different provider");
    assert_eq!(member_connect_row_count(&control, email).await, 0, "no member_connect row is ever claimed");
}

#[tokio::test]
async fn already_cooled_down_members_do_not_occupy_the_candidate_window() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping already_cooled_down_members_do_not_occupy_the_candidate_window: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_mcn8_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "mcn8").await;

    // CodeRabbit round 1 (Functional Correctness, Major): with `LIMIT
    // MAX_CANDIDATES_PER_TICK` ordered by `joined_at ASC`, a candidate that
    // is ALREADY inside an active cooldown (a prior successful send) or
    // already suppressed must not keep occupying that ordered window on
    // every later tick — starving every candidate past the cap. Two
    // hundred rows is impractical to seed in a fast test; this proves the
    // underlying exclusion mechanism directly instead: an already-cooled-
    // down member is excluded from the candidate SELECT itself (`outcome.
    // processed` == 1, not 2), which is exactly the property that keeps
    // the LIMIT window advancing once real volume exceeds the cap.
    let cooled_down_email = "already-nudged@member-connect-pg.test";
    add_member(&control, account_id, cooled_down_email, past_grace()).await;
    sqlx::query(
        "INSERT INTO slack_unmatched_email_notices \
             (account_id, provider, email, last_notified_at, unsubscribe_token) \
         VALUES ($1, 'member_connect', $2, now() - INTERVAL '1 day', $3)",
    )
    .bind(account_id)
    .bind(cooled_down_email)
    .bind(format!("tok-{}", Uuid::new_v4()))
    .execute(&control)
    .await
    .unwrap();

    let fresh_email = "never-nudged@member-connect-pg.test";
    add_member(&control, account_id, fresh_email, past_grace()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    let outcome = tick(&t, &control, &state).await.unwrap();

    assert_eq!(
        outcome.processed, 1,
        "the already-cooled-down member must not appear in the candidate SELECT at all"
    );
    assert_eq!(mock_request_count(&mock), 1, "only the never-nudged member gets emailed");
    assert_eq!(member_connect_row_count(&control, fresh_email).await, 1);
    assert_eq!(
        member_connect_row_count(&control, cooled_down_email).await,
        1,
        "the pre-seeded row is untouched, not renewed early"
    );
}

#[tokio::test]
async fn member_with_an_owned_token_is_not_a_candidate() {
    let Some(base) = base_url() else {
        eprintln!("skipping member_with_an_owned_token_is_not_a_candidate: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_mcn4_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "mcn4").await;
    let email = "connected@member-connect-pg.test";
    let user_id = add_member(&control, account_id, email, past_grace()).await;
    insert_owned_token(&control, account_id, user_id, "google").await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    let outcome = tick(&t, &control, &state).await.unwrap();
    assert_eq!(outcome.processed, 0, "the connected member is filtered out of the candidate set entirely");
    assert_eq!(mock_request_count(&mock), 0);
}

#[tokio::test]
async fn member_inside_the_grace_window_is_not_a_candidate() {
    let Some(base) = base_url() else {
        eprintln!("skipping member_inside_the_grace_window_is_not_a_candidate: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_mcn5_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "mcn5").await;
    let email = "brand-new@member-connect-pg.test";
    // Joined a minute ago — comfortably inside the default 24h grace window.
    add_member(&control, account_id, email, Utc::now() - chrono::Duration::minutes(1)).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    let outcome = tick(&t, &control, &state).await.unwrap();
    assert_eq!(outcome.processed, 0);
    assert_eq!(mock_request_count(&mock), 0);
}

#[tokio::test]
async fn personal_org_member_is_never_a_candidate() {
    let Some(base) = base_url() else {
        eprintln!("skipping personal_org_member_is_never_a_candidate: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(200).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_mcn6_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    // A personal org NEVER gets a `tenant_databases` row in production
    // (`tenancy::personal_org`'s own module doc: "A personal org gets no
    // database and no routing row"), which already keeps it off the
    // registry `run_multi_tenant` ticks from. To exercise `candidates_for_
    // account`'s OWN `personal_orgs` anti-join (the defense-in-depth that
    // function's doc comment describes) rather than merely re-proving the
    // upstream routing fact, this test mints a `tenant_databases` row for
    // the personal org anyway — a deliberately unrealistic setup, kept
    // narrow to this one test — so `tick` can resolve an `account_id` and
    // reach the query at all.
    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "mcn6").await;
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

    let email = "personal-org-member@member-connect-pg.test";
    add_member(&control, account_id, email, past_grace()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());
    let outcome = tick(&t, &control, &state).await.unwrap();
    assert_eq!(outcome.processed, 0, "personal_orgs anti-join excludes every member of this account");
    assert_eq!(mock_request_count(&mock), 0);
}

#[tokio::test]
async fn resend_failure_does_not_consume_the_cooldown() {
    let Some(base) = base_url() else {
        eprintln!("skipping resend_failure_does_not_consume_the_cooldown: set ORGT_TEST_BASE_URL");
        return;
    };
    let (mock_base, mock) = spawn_resend_mock(500).await;
    let _env = EnvGuard::acquire(&mock_base).await;

    let db = "orgt_mcn7_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin_conn(&base).await;
    recreate(&mut conn, db).await;
    let control = control_pool(&base, db).await;

    let org_id = format!("org_{}", Uuid::new_v4());
    let account_id = mint_org(&control, &org_id, "mcn7").await;
    let email = "flaky@member-connect-pg.test";
    add_member(&control, account_id, email, past_grace()).await;

    let state = app_state(control.clone());
    let t = worker_tenant(&org_id, control.clone());

    // First tick: Resend 500s. The claim must roll back with it — no
    // `member_connect` row should survive a failed send.
    tick(&t, &control, &state).await.unwrap();
    assert_eq!(mock_request_count(&mock), 1, "one send attempt was made");
    assert_eq!(
        member_connect_row_count(&control, email).await,
        0,
        "a failed send must not leave a claimed row behind — the cooldown is not consumed"
    );

    // Second tick: Resend recovers. The SAME candidate must be eligible
    // again immediately (not still serving out a cooldown it never earned).
    set_mock_status(&mock, 200);
    tick(&t, &control, &state).await.unwrap();
    assert_eq!(mock_request_count(&mock), 2, "the retried send went out");
    assert_eq!(member_connect_row_count(&control, email).await, 1, "now claimed, on the successful send");
}
