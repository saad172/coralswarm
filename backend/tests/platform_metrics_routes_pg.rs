//! Integration test for the five `/internal/admin/*` platform-metrics
//! reporting endpoints (platform-admin-metrics PR 6 of 7, `routes/admin.rs`).
//!
//! Two families of assertion.
//!
//! **Router placement / auth**, needing no live database at all: every
//! endpoint 401s without a valid `X-Internal-Token` (`test_app_state()` +
//! `test_support_internal_app`), and every endpoint is a genuine 404 on the
//! PUBLIC router (`test_support_app`) — these are cross-account platform-
//! operator surfaces and must never be reachable from `/v1`.
//!
//! **Functional smoke test against real Postgres**: a scratch control DB —
//! migrated through `waitlist_snapshot` (migration `0096`, sibling Unit E;
//! this branch is now the linear stack a → e → f, so that table always
//! exists here, never a maybe-missing dependency) — seeded with a personal
//! account + an org (via `tenant_databases`), logins (`user_access_log`),
//! org rollup rows (`platform_metrics_daily` / `platform_active_users`),
//! `llm_usage`, and `waitlist_snapshot` rows with a KNOWN overlap against
//! `users` (one matched/converted signup, one waitlisted-but-unconverted
//! signup, and a user who never waitlisted at all) — driving the REAL
//! `/internal` HTTP router exactly as `rotate_dek_pg.rs` does. Proves the
//! user/org counts, the cumulative growth series shape, all four conversion
//! buckets against the real `waitlist_snapshot` schema (`email_normalized`
//! PK, `email_raw`, `signed_up_at`, `utm_source`, `country` — NOT `email`/
//! `created_at`, the schema mismatch review round 2 caught), the personal +
//! org spend rows, the fleet row's `expected_schema_version` (derived live
//! from `tenancy::migrate::head_version`, not hardcoded), and the raw
//! `waitlist` rows (display email = `email_raw`, `created_at` =
//! `signed_up_at`, including a row with a NULL `signed_up_at` to prove that
//! genuinely-nullable column is reported as JSON `null`, never fabricated).
//!
//! Gated on `ORGT_TEST_BASE_URL`; same scratch-DB pattern as
//! `rotate_dek_pg.rs` / `admin_backfill_rls_pg.rs`.
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test platform_metrics_routes_pg

#[path = "support/retained_database.rs"]
mod retained_database;

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use coralswarm_backend::AppState;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

const INTERNAL_TOKEN: &str = "orgt-platform-metrics-test-token";

const ENDPOINTS: &[&str] = &[
    "/internal/admin/overview",
    "/internal/admin/where-from",
    "/internal/admin/spend?month=2026-01",
    "/internal/admin/fleet",
    "/internal/admin/waitlist",
];

/// Serializes every test in this file against the process-global
/// `INTERNAL_TOKEN` env var — same rationale as `rotate_dek_pg.rs`'s own
/// `ENV_LOCK` (separate compilation units can't share `#[cfg(test)]`-only
/// guards).
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

struct TestEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}
impl TestEnv {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        unsafe {
            std::env::set_var("INTERNAL_TOKEN", INTERNAL_TOKEN);
        }
        Self { _guard: guard }
    }
}
impl Drop for TestEnv {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("INTERNAL_TOKEN");
        }
    }
}

async fn get(app: &axum::Router, uri: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(tok) = token {
        builder = builder.header("x-internal-token", tok);
    }
    let req = builder.body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body)
}

// ── 1. Router placement / auth — no live database needed ────────────────────

#[tokio::test]
async fn internal_admin_routes_401_without_a_valid_token() {
    let _env = TestEnv::enable().await;
    let app = coralswarm_backend::test_support_internal_app(coralswarm_backend::test_app_state());

    for path in ENDPOINTS {
        let (status, _) = get(&app, path, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}: missing token must reject");

        let (status, _) = get(&app, path, Some("wrong-token")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}: wrong token must reject");
    }
}

#[tokio::test]
async fn internal_admin_routes_are_absent_from_the_public_router() {
    // No INTERNAL_TOKEN needed here — these paths must not even be registered
    // on `app()`, so the request 404s before auth is ever considered.
    let app = coralswarm_backend::test_support_app(coralswarm_backend::test_app_state());

    for path in ENDPOINTS {
        let (status, _) = get(&app, path, Some(INTERNAL_TOKEN)).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{path}: platform-operator surface must never be reachable from the public /v1 router"
        );
    }
}

// ── 2. Functional smoke test against real Postgres ───────────────────────────

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
    retained_database::recreate(conn, db).await.unwrap();
}
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&with_db(base, db))
        .await
        .expect("connect control");
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn platform_metrics_endpoints_report_seeded_data_with_real_waitlist_snapshot() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping platform_metrics_endpoints_report_seeded_data_with_real_waitlist_snapshot: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = TestEnv::enable().await;

    let db = "orgt_pme1_ctl";
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    // `waitlist_snapshot` (sibling Unit E, migration 0096) now ALWAYS exists
    // on this branch — the stack is linear (a → e → f) specifically so this
    // is never a maybe-missing dependency again.
    let waitlist_exists: bool = sqlx::query_scalar("SELECT to_regclass('public.waitlist_snapshot') IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(waitlist_exists, "waitlist_snapshot must exist on the linear a -> e -> f stack");

    // ── Seed: two users, a personal account, and an org account ────────────
    // `user_a`'s email is a FIXED, deliberately mixed-case/whitespace value
    // (not a random UUID prefix like `user_b`'s) so the conversion join's
    // normalization (`lower(trim(u.email))`) is actually exercised, not just
    // coincidentally satisfied by an already-lowercase generated address.
    let user_a: Uuid = sqlx::query_scalar("INSERT INTO users (email, name) VALUES ($1, 'Alice Personal') RETURNING id")
        .bind("  Alice@Test.Local  ")
        .fetch_one(&pool)
        .await
        .unwrap();
    let user_b: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();

    // ── waitlist_snapshot: a KNOWN overlap against `users` ──────────────────
    // Row 1: matches user_a (converted). `email_normalized` is what
    // `workers::waitlist_sync` would have already computed
    // (lowercase+trimmed) — the join compares it DIRECTLY against
    // `lower(trim(users.email))`, never re-normalizing this side.
    sqlx::query(
        "INSERT INTO waitlist_snapshot (email_normalized, email_raw, signed_up_at, utm_source, country) \
         VALUES ('alice@test.local', '  Alice@Test.Local  ', '2026-01-05T00:00:00Z', 'google', 'US')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Row 2: waitlisted, but NO matching user at all — waitlisted_unconverted.
    sqlx::query(
        "INSERT INTO waitlist_snapshot (email_normalized, email_raw, signed_up_at, utm_source, country) \
         VALUES ('waitlisted-only@test.local', 'WaitlistedOnly@Test.Local', '2026-01-06T00:00:00Z', 'twitter', 'CA')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Row 3: a defensively-parsed sync that failed to parse D1's
    // `created_at` — `signed_up_at` genuinely NULL, per the migration's own
    // doc comment. Also unmatched, and has no utm_source/country.
    sqlx::query(
        "INSERT INTO waitlist_snapshot (email_normalized, email_raw, signed_up_at, utm_source, country) \
         VALUES ('null-date@test.local', 'Null-Date@Test.Local', NULL, NULL, NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    // `user_b` never waitlists at all — users_never_waitlisted.

    let personal_account: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
        .bind(user_a)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(personal_account)
        .bind(user_a)
        .execute(&pool)
        .await
        .unwrap();

    let org_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, 'org_pme_test') RETURNING id")
            .bind(user_b)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
        .bind(org_account)
        .bind(user_b)
        .execute(&pool)
        .await
        .unwrap();

    let expected_schema_version = coralswarm_backend::tenancy::migrate::head_version();
    financial_fixture::execute(&pool, org_account, sqlx::query(
        "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, region, status, schema_version) \
         VALUES ('org_pme_test', $1, 'pme-test-org', 'SECRET_PME_TEST', 'iad', 'active', $2)",
    )
    .bind(org_account)
    .bind(expected_schema_version))
    .await
    .unwrap();

    // ── Logins (user_access_log) — both users, within the last 7 days ──────
    sqlx::query("INSERT INTO user_access_log (user_id, clerk_session_id, client, country) VALUES ($1, 'sess-a', 'desktop', 'US')")
        .bind(user_a)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO user_access_log (user_id, clerk_session_id, client, country) VALUES ($1, 'sess-b', 'web', 'CA')")
        .bind(user_b)
        .execute(&pool)
        .await
        .unwrap();

    // ── platform_active_users (did_work signal) — one row per user/tenant ──
    sqlx::query("INSERT INTO platform_active_users (day, user_id, tenant, source) VALUES (current_date, $1, '__consumer__', 'activity')")
        .bind(user_a)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO platform_active_users (day, user_id, tenant, source) VALUES (current_date, $1, 'org_pme_test', 'activity')")
        .bind(user_b)
        .execute(&pool)
        .await
        .unwrap();

    // ── llm_usage (personal-plane spend) — inside the target month ─────────
    let target_month = "2026-01";
    sqlx::query(
        "INSERT INTO llm_usage (account_id, task, model, input_tokens, output_tokens, created_at) \
         VALUES ($1, 'synthesis', 'claude-3-5-sonnet-latest', 100000, 20000, '2026-01-15T12:00:00Z')",
    )
    .bind(personal_account)
    .execute(&pool)
    .await
    .unwrap();

    // ── platform_metrics_daily (org-plane rollup) — inside the target month ─
    sqlx::query(
        "INSERT INTO platform_metrics_daily (day, tenant, llm_input_tokens, llm_output_tokens) \
         VALUES ('2026-01-10', 'org_pme_test', 500000, 100000)",
    )
    .execute(&pool)
    .await
    .unwrap();

    // ── Drive the real internal router ──────────────────────────────────────
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    let app = coralswarm_backend::test_support_internal_app(Arc::new(state));

    // -- overview --
    let (status, body) = get(&app, "/internal/admin/overview", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "overview body: {body:?}");
    assert_eq!(body["users"]["total"], 2);
    assert_eq!(body["users"]["new_30d"], 2);
    assert!(body["users"]["series"].as_array().unwrap().iter().any(|p| p["cum"] == 2));
    assert_eq!(body["orgs"]["total"], 1);
    assert_eq!(body["orgs"]["by_status"][0]["status"], "active");
    assert_eq!(body["orgs"]["by_status"][0]["count"], 1);
    assert_eq!(body["active_7d"]["signed_in"], 2);
    assert_eq!(body["active_7d"]["did_work"], 2);
    // Conversion funnel against the real waitlist_snapshot schema: 3 rows
    // seeded (alice = matched, waitlisted-only + null-date = unconverted),
    // 1 user (bob) never waitlisted. `conversion` must be the real object,
    // NOT null — the query genuinely succeeds against this real schema.
    assert!(!body["conversion"].is_null(), "conversion must not be null when the query succeeds: {body:?}");
    assert_eq!(body["conversion"]["waitlist_total"], 3);
    assert_eq!(body["conversion"]["matched"], 1, "only alice's normalized email matches a user");
    assert_eq!(body["conversion"]["waitlisted_unconverted"], 2);
    assert_eq!(body["conversion"]["users_never_waitlisted"], 1, "bob never waitlisted");
    assert!(body["freshness"]["control_at"].is_string());
    // rollup_at reflects platform_metrics_daily.computed_at, independent of
    // waitlist_snapshot — we DID insert a platform_metrics_daily row, so it
    // must be present.
    assert!(body["freshness"]["rollup_at"].is_string(), "body: {body:?}");

    // -- where-from --
    let (status, body) = get(&app, "/internal/admin/where-from", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "where-from body: {body:?}");
    let clients = body["clients"].as_array().unwrap();
    assert!(!clients.is_empty());
    let countries = body["countries"].as_array().unwrap();
    assert!(countries.iter().any(|c| c["key"] == "US"));
    assert!(countries.iter().any(|c| c["key"] == "CA"));
    // channels: real utm_source breakdown from waitlist_snapshot (google,
    // twitter, and the null-date row's NULL utm_source shown as "unknown").
    let channels = body["channels"].as_array().unwrap();
    assert!(channels.iter().any(|c| c["key"] == "google" && c["count"] == 1));
    assert!(channels.iter().any(|c| c["key"] == "twitter" && c["count"] == 1));
    assert!(channels.iter().any(|c| c["key"] == "unknown" && c["count"] == 1));
    assert_eq!(body["freshness"]["kind"], "live");

    // -- spend (strict month validation) --
    let (status, _) = get(&app, "/internal/admin/spend?month=not-a-month", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "malformed month must be rejected");
    let (status, _) = get(&app, "/internal/admin/spend?month=2026-13", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "out-of-range month must be rejected");

    let (status, body) = get(&app, &format!("/internal/admin/spend?month={target_month}"), Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "spend body: {body:?}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "personal account row + org tenant row: {rows:?}");
    let personal_row = rows.iter().find(|r| r["plane"] == "personal").expect("personal row present");
    assert_eq!(personal_row["row_kind"], "account");
    assert_eq!(personal_row["account_id"], personal_account.to_string());
    assert_eq!(personal_row["input_tokens"], 100000);
    assert_eq!(personal_row["output_tokens"], 20000);
    assert!(personal_row["cost_usd"].as_f64().unwrap() > 0.0);
    assert!(personal_row["accounts_rolled_up"].is_null());
    assert_eq!(personal_row["plane"], "personal", "a real account row's plane is never null");
    let org_row = rows.iter().find(|r| r["plane"] == "org").expect("org row present");
    assert_eq!(org_row["row_kind"], "account");
    assert_eq!(org_row["account_id"], org_account.to_string());
    assert_eq!(org_row["input_tokens"], 500000);
    assert_eq!(org_row["output_tokens"], 100000);
    assert_eq!(body["total"]["accounts"], 2);
    assert_eq!(body["total"]["input_tokens"], 600000);
    assert_eq!(body["total"]["output_tokens"], 120000);
    assert_eq!(body["freshness"]["kind"], "nightly", "org data present this month");

    // -- fleet --
    let (status, body) = get(&app, "/internal/admin/fleet", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "fleet body: {body:?}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["slug"], "pme-test-org");
    assert_eq!(rows[0]["clerk_org_id"], "org_pme_test");
    assert_eq!(rows[0]["status"], "active");
    assert_eq!(rows[0]["schema_version"], expected_schema_version);
    assert_eq!(rows[0]["expected_schema_version"], expected_schema_version);
    assert_eq!(rows[0]["members"], 1);

    // -- waitlist: real rows, raw email display, correct converted flags --
    let (status, body) = get(&app, "/internal/admin/waitlist", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "waitlist body: {body:?}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3, "rows: {rows:?}");
    assert_eq!(body["total"], 3);
    assert_eq!(body["freshness"]["kind"], "nightly");

    // Display email is `email_raw` (as D1 had it typed), NOT the normalized
    // join key — and `converted` is exact-normalized-match against `users`.
    let alice_row = rows
        .iter()
        .find(|r| r["email"] == "  Alice@Test.Local  ")
        .expect("alice's raw email is displayed verbatim");
    assert_eq!(alice_row["converted"], true);
    assert_eq!(alice_row["utm_source"], "google");
    assert_eq!(alice_row["country"], "US");
    assert!(alice_row["created_at"].is_string());

    let unconverted_row = rows
        .iter()
        .find(|r| r["email"] == "WaitlistedOnly@Test.Local")
        .expect("waitlisted-only row present");
    assert_eq!(unconverted_row["converted"], false, "no user matches this email");
    assert_eq!(unconverted_row["utm_source"], "twitter");

    // The genuinely-NULL `signed_up_at` row: reported as JSON null, never a
    // fabricated timestamp, and never dropped from the result.
    let null_date_row = rows
        .iter()
        .find(|r| r["email"] == "Null-Date@Test.Local")
        .expect("null-signed_up_at row still present");
    assert_eq!(null_date_row["converted"], false);
    assert!(null_date_row["created_at"].is_null(), "row: {null_date_row:?}");
    assert!(null_date_row["utm_source"].is_null());
    assert!(null_date_row["country"].is_null());

    // -- waitlist: limit clamping --
    let (status, body) = get(&app, "/internal/admin/waitlist?limit=1", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().unwrap().len(), 1, "limit=1 must be honored");
    assert_eq!(body["total"], 3, "total reflects the full table, not the limited page");

    pool.close().await;
    retained_database::finish(&mut conn, db, true).await.ok();
}

/// `admin_spend`'s collapsed `other_accounts` row: `plane` must be JSON
/// `null` (never inferred toward `"personal"`/`"org"`, even though the tail
/// it summarizes may span both planes), and the row must still be
/// arithmetically complete — its `input_tokens`/`output_tokens`/`cost_usd`
/// cover the ENTIRE tail, so `total` reconciles against the sum of every row
/// including this one.
#[tokio::test]
async fn spend_other_accounts_row_is_planeless_and_totals_reconcile() {
    let Some(base) = base_url() else {
        eprintln!("skipping spend_other_accounts_row_is_planeless_and_totals_reconcile: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = TestEnv::enable().await;

    let db = "orgt_pme2_ctl";
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    // 25 personal accounts, each with distinct llm_usage in the target month
    // — more than `SPEND_TOP_N` (20), so the ranking genuinely produces a
    // non-empty tail to collapse.
    const NUM_ACCOUNTS: i64 = 25;
    for i in 0..NUM_ACCOUNTS {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@test.local", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .unwrap();
        let account_id: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let input_tokens = (i + 1) * 10_000;
        let output_tokens = (i + 1) * 2_000;
        sqlx::query(
            "INSERT INTO llm_usage (account_id, task, model, input_tokens, output_tokens, created_at) \
             VALUES ($1, 'synthesis', 'claude-3-5-sonnet-latest', $2, $3, '2026-01-15T12:00:00Z')",
        )
        .bind(account_id)
        .bind(input_tokens)
        .bind(output_tokens)
        .execute(&pool)
        .await
        .unwrap();
    }

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    let app = coralswarm_backend::test_support_internal_app(Arc::new(state));

    let (status, body) = get(&app, "/internal/admin/spend?month=2026-01", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "spend body: {body:?}");
    let rows = body["rows"].as_array().unwrap();

    // 20 individual top accounts + exactly one collapsed `other_accounts` row.
    assert_eq!(rows.len(), 21, "rows: {rows:?}");
    let other_rows: Vec<&Value> = rows.iter().filter(|r| r["row_kind"] == "other_accounts").collect();
    assert_eq!(other_rows.len(), 1, "exactly one collapsed row: {rows:?}");
    let other = other_rows[0];
    assert!(other["plane"].is_null(), "other_accounts row's plane must be null, not inferred: {other:?}");
    assert!(other["account_id"].is_null());
    assert_eq!(other["accounts_rolled_up"], NUM_ACCOUNTS - 20);

    // Every non-collapsed row is a real account with a non-null plane.
    for r in rows.iter().filter(|r| r["row_kind"] == "account") {
        assert!(r["plane"] == "personal" || r["plane"] == "org", "row: {r:?}");
    }

    // Reconciliation: token sums are exact; cost_usd is within a small
    // epsilon (each row independently rounds to cents, so summing rounded
    // per-row values can differ from the separately-rounded total by a
    // fraction of a cent).
    let sum_input: i64 = rows.iter().map(|r| r["input_tokens"].as_i64().unwrap()).sum();
    let sum_output: i64 = rows.iter().map(|r| r["output_tokens"].as_i64().unwrap()).sum();
    let sum_cost: f64 = rows.iter().map(|r| r["cost_usd"].as_f64().unwrap()).sum();

    let expected_input: i64 = (1..=NUM_ACCOUNTS).map(|i| i * 10_000).sum();
    let expected_output: i64 = (1..=NUM_ACCOUNTS).map(|i| i * 2_000).sum();
    assert_eq!(sum_input, expected_input);
    assert_eq!(sum_output, expected_output);
    assert_eq!(body["total"]["input_tokens"], expected_input);
    assert_eq!(body["total"]["output_tokens"], expected_output);
    assert_eq!(sum_input, body["total"]["input_tokens"].as_i64().unwrap());
    assert_eq!(sum_output, body["total"]["output_tokens"].as_i64().unwrap());

    let total_cost = body["total"]["cost_usd"].as_f64().unwrap();
    assert!(
        (sum_cost - total_cost).abs() < 0.05,
        "sum of per-row cost_usd ({sum_cost}) must reconcile against total.cost_usd ({total_cost})"
    );
    assert_eq!(body["total"]["accounts"], NUM_ACCOUNTS);

    // The collapsed row's own tokens must equal exactly the sum of the 5
    // smallest (lowest-cost) accounts' tokens — i.e. it genuinely covers the
    // WHOLE tail, not a partial or double-counted slice of it.
    let tail_expected_input: i64 = (1..=5).map(|i| i * 10_000).sum();
    let tail_expected_output: i64 = (1..=5).map(|i| i * 2_000).sum();
    assert_eq!(other["input_tokens"], tail_expected_input);
    assert_eq!(other["output_tokens"], tail_expected_output);

    pool.close().await;
    retained_database::finish(&mut conn, db, true).await.ok();
}

/// The exact distinction review round 2 asked to make explicit and
/// test-covered: a genuinely EMPTY `waitlist_snapshot` must report real
/// zeros (a true, honest answer), while a query FAILURE against it must
/// report `null` (never zeros) — the two are not the same thing, and
/// conflating them (via `.unwrap_or((0, 0, 0, 0))`) is the exact bug this
/// round of review caught in production.
#[tokio::test]
async fn conversion_reports_real_zeros_when_empty_and_null_only_on_genuine_query_failure() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping conversion_reports_real_zeros_when_empty_and_null_only_on_genuine_query_failure: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = TestEnv::enable().await;

    let db = "orgt_pme3_ctl";
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    let app = coralswarm_backend::test_support_internal_app(Arc::new(state));

    // ── 1. Genuinely empty `waitlist_snapshot` (migrated, zero rows) ────────
    // Must be real, honest zeros — NOT null. `conversion` is a fully-formed
    // object, not absent, when the query itself succeeds.
    let (status, body) = get(&app, "/internal/admin/overview", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert!(
        !body["conversion"].is_null(),
        "a genuinely empty waitlist_snapshot must report real zeros, not null: {body:?}"
    );
    assert_eq!(body["conversion"]["waitlist_total"], 0);
    assert_eq!(body["conversion"]["matched"], 0);
    assert_eq!(body["conversion"]["waitlisted_unconverted"], 0);
    assert_eq!(body["conversion"]["users_never_waitlisted"], 0);

    // ── 2. Deliberately break the schema — simulating the exact incident ───
    // (a schema mismatch making the conversion query genuinely fail) —
    // WITHOUT touching `users`/`accounts`/etc., so every OTHER headline
    // count in the same response keeps succeeding independently.
    sqlx::query("DROP TABLE waitlist_snapshot").execute(&pool).await.unwrap();

    let (status, body) = get(&app, "/internal/admin/overview", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "overview must not 500 the whole dashboard: {body:?}");
    assert!(
        body["conversion"].is_null(),
        "a genuine query failure must degrade to null, never a fabricated zero: {body:?}"
    );
    // Every other headline count is untouched by the missing table and must
    // still report its real (here: zero, since nothing else was seeded)
    // value rather than also collapsing to null/erroring.
    assert_eq!(body["users"]["total"], 0);
    assert_eq!(body["orgs"]["total"], 0);
    assert_eq!(body["active_7d"]["signed_in"], 0);
    assert_eq!(body["active_7d"]["did_work"], 0);

    pool.close().await;
    retained_database::finish(&mut conn, db, true).await.ok();
}

/// Live-Postgres proof for this fix: `admin_spend`'s org-plane branch now
/// prices real Anthropic prompt-cache tokens straight from
/// `platform_metrics_daily` (migration `0123_platform_metrics_daily_cache_
/// tokens.sql`), instead of the pre-fix hardcoded `0, 0`. This drives the
/// SAME `SUM(bigint) -> ::bigint` query shape `platform_rollup_pg.rs`'s
/// helper queries already prove decode correctly, but through the REAL
/// `/internal/admin/spend` HTTP round trip — the exact layer PR #509's
/// decode bug (compiled fine, 500'd live) slipped through on a `--no-run`
/// check alone.
///
/// Seeds an org tenant whose ENTIRE month is cache-aware rollup rows (no
/// NULLs at all): `cache_pricing.complete` must be `true`, and `cost_usd`
/// must reflect the cache-creation (1.25x input rate) / cache-read (0.1x
/// input rate) multipliers on top of the plain input/output pricing, not
/// just the plain input/output total.
#[tokio::test]
async fn spend_org_plane_prices_real_cache_tokens_when_every_row_is_cache_aware() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping spend_org_plane_prices_real_cache_tokens_when_every_row_is_cache_aware: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = TestEnv::enable().await;

    let db = "orgt_pme4_ctl";
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let owner: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
    let org_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, 'org_pme4_cache') RETURNING id")
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
    let expected_schema_version = coralswarm_backend::tenancy::migrate::head_version();
    financial_fixture::execute(&pool, org_account, sqlx::query(
        "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, region, status, schema_version) \
         VALUES ('org_pme4_cache', $1, 'pme4-cache-org', 'SECRET_PME4', 'iad', 'active', $2)",
    )
    .bind(org_account)
    .bind(expected_schema_version))
    .await
    .unwrap();

    // Every row this org has this month carries real, non-null cache sums —
    // exactly what `workers::platform_rollup` now always writes going
    // forward (never NULL on a row it (re)writes).
    sqlx::query(
        "INSERT INTO platform_metrics_daily \
           (day, tenant, llm_input_tokens, llm_output_tokens, llm_cache_creation_tokens, llm_cache_read_tokens) \
         VALUES ('2026-02-10', 'org_pme4_cache', 1000000, 0, 1000000, 1000000)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    let app = coralswarm_backend::test_support_internal_app(Arc::new(state));

    let (status, body) = get(&app, "/internal/admin/spend?month=2026-02", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "spend body: {body:?}");

    let rows = body["rows"].as_array().unwrap();
    let org_row = rows.iter().find(|r| r["plane"] == "org").expect("org row present");
    assert_eq!(org_row["input_tokens"], 1_000_000);
    assert_eq!(org_row["output_tokens"], 0);

    // `__unattributed__` (org rows have no per-model breakdown) resolves to
    // `DEFAULT_RATE_PER_MILLION` = (5.7, 27.5) per million tokens.
    // cost = 1,000,000/1e6*5.7 (plain input)
    //      + 1,000,000/1e6*5.7*1.25 (cache creation, 1.25x input rate)
    //      + 1,000,000/1e6*5.7*0.1  (cache read, 0.1x input rate)
    //      = 5.7 + 7.125 + 0.57 = 13.395 -> rounds to 13.4
    let cost = org_row["cost_usd"].as_f64().unwrap();
    assert!(
        (cost - 13.4).abs() < 0.01,
        "org-plane cost_usd must include cache-creation/cache-read pricing, not just plain input/output: got {cost}"
    );

    // A plain-input-only estimate (the pre-fix `0, 0` behavior) would have
    // landed on 5.7 — proves the fix actually changed the number, not just
    // the plumbing.
    assert!(cost > 5.7 + 0.01, "cost_usd must be strictly greater than the pre-fix cache-blind estimate: got {cost}");

    assert_eq!(body["cache_pricing"]["complete"], true, "every row this org has this month is cache-aware: {body:?}");
    assert!(
        body["cache_pricing"]["note"].as_str().unwrap().contains("cache-aware"),
        "note: {:?}",
        body["cache_pricing"]["note"]
    );

    pool.close().await;
    retained_database::finish(&mut conn, db, true).await.ok();
}

/// Companion to the "fully cache-aware" test above: an org tenant with a
/// pre-cache-aware-rollup `(day, tenant)` row (`llm_cache_creation_tokens
/// IS NULL`, migration `0123`) that ALSO had real token usage that day must
/// flip `cache_pricing.complete` to `false` and the note must name the
/// migration/rollup gap — the response must not silently present a
/// plausible-but-partial number as complete.
#[tokio::test]
async fn spend_org_plane_flags_incomplete_when_a_row_predates_the_cache_aware_rollup() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping spend_org_plane_flags_incomplete_when_a_row_predates_the_cache_aware_rollup: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = TestEnv::enable().await;

    let db = "orgt_pme5_ctl";
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let owner: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@test.local", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
    let org_account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, 'org_pme5_stale') RETURNING id",
    )
    .bind(owner)
    .fetch_one(&pool)
    .await
    .unwrap();
    let expected_schema_version = coralswarm_backend::tenancy::migrate::head_version();
    financial_fixture::execute(&pool, org_account, sqlx::query(
        "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, region, status, schema_version) \
         VALUES ('org_pme5_stale', $1, 'pme5-stale-org', 'SECRET_PME5', 'iad', 'active', $2)",
    )
    .bind(org_account)
    .bind(expected_schema_version))
    .await
    .unwrap();

    // Row 1: cache-aware (real, non-null sums).
    sqlx::query(
        "INSERT INTO platform_metrics_daily \
           (day, tenant, llm_input_tokens, llm_output_tokens, llm_cache_creation_tokens, llm_cache_read_tokens) \
         VALUES ('2026-03-05', 'org_pme5_stale', 100000, 50000, 100, 100)",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Row 2: predates the cache-aware rollup — NULL cache columns — but has
    // real (nonzero) token usage, so it must taint completeness. Cache
    // columns are simply omitted from the INSERT (column default is NULL,
    // no DEFAULT clause — migration 0123's whole point).
    sqlx::query(
        "INSERT INTO platform_metrics_daily (day, tenant, llm_input_tokens, llm_output_tokens) \
         VALUES ('2026-03-06', 'org_pme5_stale', 200000, 0)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    let app = coralswarm_backend::test_support_internal_app(Arc::new(state));

    let (status, body) = get(&app, "/internal/admin/spend?month=2026-03", Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "spend body: {body:?}");

    let rows = body["rows"].as_array().unwrap();
    let org_row = rows.iter().find(|r| r["plane"] == "org").expect("org row present");
    // Tokens themselves still sum correctly across both rows (never dropped
    // by the NULL cache columns).
    assert_eq!(org_row["input_tokens"], 300000, "100000 + 200000 across both rows");
    assert_eq!(org_row["output_tokens"], 50000);

    // Cache pricing on the NULL row is treated as 0 in the best-known sum —
    // NOT dropped, NOT tainting the plain input/output totals — while the
    // completeness flag (checked below) is what actually signals the gap.
    let cost = org_row["cost_usd"].as_f64().unwrap();
    assert!(cost > 0.0);

    assert_eq!(
        body["cache_pricing"]["complete"], false,
        "a row predating the cache-aware rollup with real usage must flag incomplete: {body:?}"
    );
    let note = body["cache_pricing"]["note"].as_str().unwrap();
    assert!(note.contains("0123"), "note must name the migration/rollup gap explicitly: {note}");

    pool.close().await;
    retained_database::finish(&mut conn, db, true).await.ok();
}
