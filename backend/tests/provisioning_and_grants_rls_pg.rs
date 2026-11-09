//! Two OPS-4 follow-up proofs, both against a REAL `coralswarm_app` login via
//! the shared `tests/support/rls_probe.rs` harness (PR #286):
//!
//! **F2 — first-sight provisioning must survive RLS enforcement.**
//! `clerk_provision::{ensure_personal_account, ensure_org_account}` each
//! created the new account's first ocean through the UNSCOPED
//! `Store::create_graph` (`INSERT INTO graphs ... .fetch_one(&self.pool)`).
//! `graphs` is one of migration `0083_consumer_rls`'s 15 direct-`account_id`
//! tables and its `account_isolation` policy is `USING`-only — no `FOR`
//! clause means `FOR ALL`, and Postgres reuses a missing `WITH CHECK` as the
//! INSERT check — so the moment OPS-4 pointed the app at the non-`BYPASSRLS`
//! `coralswarm_app` role, that INSERT started raising `unrecognized
//! configuration parameter "app.account_id"`. The failure lands squarely on
//! `require_auth` → `resolve_clerk_context` → `ensure_personal_account`, i.e.
//! a BRAND-NEW user's very first authenticated request, which 500'd and left
//! them with no account at all (existing users short-circuit before it and
//! were unaffected — which is exactly why this could sit live). The fix
//! routes both call sites through `create_graph_scoped` inside
//! `tenancy::scope::begin_account_scoped`.
//!
//! `fresh_user_first_authenticated_request_provisions_account_and_ocean`
//! drives the REAL router (`test_support_app`, a real Clerk-shaped JWT
//! verified against a local JWKS listener) served entirely off the
//! `coralswarm_app` pool, for a `sub` that has no `users`, no `accounts`, no
//! `account_members` and no `graphs` row anywhere — the true cold-start path
//! — and asserts the account, the membership and the Personal ocean all exist
//! afterward.
//!
//! `unscoped_graph_insert_hard_errors_as_app_role` is that test's permanent
//! NEGATIVE CONTROL, kept in CI rather than performed once by hand: it runs
//! the pre-fix statement shape (an `INSERT INTO graphs` on a bare
//! `coralswarm_app` connection with `app.account_id` never set) and requires
//! it to hard-error. Without it, the end-to-end test above would still pass
//! on a build where RLS silently was not being enforced, and would therefore
//! prove nothing about the fix.
//!
//! **F5 — migration 0087 dropped the SECURITY DEFINER grant standard.**
//! `0087_session_share_bootstrap_functions` `GRANT`s on its two SECURITY
//! DEFINER functions but never `REVOKE`s from `PUBLIC`, unlike 0084 (5
//! REVOKEs / 5 functions), 0086 (8/8) and the tenant tree's own
//! `0003_bootstrap_scope_functions`. Postgres grants `EXECUTE` to `PUBLIC` by
//! default, so any role with mere `CONNECT` could call
//! `resolve_session_share_account_id(text)` /
//! `resolve_session_share_graph_id(uuid)` and read straight through the 0083
//! policies as an ownership oracle. `0089_session_share_function_public_revoke`
//! closes it; `session_share_functions_are_not_executable_by_public` proves a
//! CONNECT-only role is refused with SQLSTATE 42501 while `coralswarm_app`
//! still resolves both — and carries its own in-test control (a throwaway
//! SECURITY DEFINER function created WITHOUT a REVOKE, which the same role
//! CAN call) so a false "cannot execute" caused by the probe role simply
//! lacking schema reach cannot pass for the property under test.
//!
//! Env-gated, like every `*_pg.rs` here — and therefore wired into
//! `.github/workflows/ci.yml`'s `RLS scope proofs` job, which is the only
//! place `ORGT_TEST_BASE_URL` is set (see that job's HAZARD banner: eleven
//! suites silently skipped their way to green before it existed):
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test provisioning_and_grants_rls_pg

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection};
use std::str::FromStr;
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

fn future_exp() -> usize {
    (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600) as usize
}

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

// ── F2 ──────────────────────────────────────────────────────────────────────

/// The cold start: a Clerk `sub` this deployment has NEVER seen makes its
/// first authenticated request, served entirely off `coralswarm_app`.
///
/// Pre-fix this returns 500 (`ProvisionError::Store` from the unscoped
/// `INSERT INTO graphs`) and writes an `accounts` row with no ocean — the
/// exact production symptom. Post-fix it must return 200 AND the account,
/// the owner membership and the `Personal` personal ocean must all exist.
///
/// Note what is deliberately NOT stubbed: the account row, the membership
/// row and the DEK all still land through their normal (control-plane,
/// unscoped) statements, so this also proves the fix did not disturb the
/// ordering around the one statement it moved into a transaction.
#[tokio::test]
async fn fresh_user_first_authenticated_request_provisions_account_and_ocean() {
    let Some(probe) = RlsProbe::setup("orgt_rls_provisioning_f2").await else {
        eprintln!(
            "skipping fresh_user_first_authenticated_request_provisions_account_and_ocean: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let jwks_url = spawn_local_jwks().await;
    // SAFETY: this binary's only env-mutating test (the other two touch no
    // process env), and the suite is run with `--test-threads=1` in CI.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
        // Exercise `provision_account_dek`'s real branch rather than its
        // "MASTER_KEK unavailable — skipping" warn path, so the whole
        // provisioning sequence runs end to end.
        std::env::set_var("MASTER_KEK_HEX", "cd".repeat(32));
    }

    // A `sub` with no `users` row — nothing about this identity exists yet.
    // (`RlsProbe::setup` seeded accounts a/b for OTHER users; this one is
    // unrelated to both, which also means a leaked cross-account read would
    // show up as extra oceans in the response below.)
    let fresh_sub = "orgt_f2_brand_new_user";
    let pre_existing: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE clerk_user_id = $1")
            .bind(fresh_sub)
            .fetch_one(&probe.owner)
            .await
            .expect("count pre-existing users");
    assert_eq!(
        pre_existing, 0,
        "fixture precondition: this Clerk sub must be genuinely unseen"
    );

    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token = mint(json!({ "iss": ISSUER, "sub": fresh_sub, "exp": future_exp() }));
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/v1/graphs")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = body_json(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a brand-new user's FIRST authenticated request must provision cleanly \
         under the coralswarm_app role — got {status} {body}"
    );

    // The response is the reachability union for this user, so the freshly
    // created Personal ocean must already be in it (and nothing else — a
    // cross-account leak would show up right here).
    let graphs = body.as_array().expect("array of oceans");
    assert_eq!(
        graphs.len(),
        1,
        "exactly the new user's own Personal ocean must be reachable: {body}"
    );
    assert_eq!(graphs[0]["name"], json!("Personal"));
    assert_eq!(graphs[0]["kind"], json!("personal"));
    assert_eq!(
        graphs[0]["is_primary"],
        json!(true),
        "the first personal ocean must still be marked primary — the is_primary \
         sub-select reads under the same app.account_id GUC: {body}"
    );

    // …and the rows are really on disk, read back through the owner pool.
    let (user_id, account_id): (Uuid, Uuid) = sqlx::query_as(
        "SELECT u.id, a.id FROM users u \
         JOIN accounts a ON a.owner_user_id = u.id AND a.clerk_org_id IS NULL \
         WHERE u.clerk_user_id = $1",
    )
    .bind(fresh_sub)
    .fetch_one(&probe.owner)
    .await
    .expect("the provisioned user + personal account must exist");

    let members: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = $2 \
         AND role = 'owner'",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(&probe.owner)
    .await
    .expect("count memberships");
    assert_eq!(members, 1, "the owner membership row must exist");

    let (graph_id, is_primary): (Uuid, bool) = sqlx::query_as(
        "SELECT id, is_primary FROM graphs WHERE account_id = $1 AND kind = 'personal'",
    )
    .bind(account_id)
    .fetch_one(&probe.owner)
    .await
    .expect("the Personal ocean must have been committed — THIS is the F2 break");
    assert!(is_primary);
    assert_eq!(
        graphs[0]["id"].as_str().unwrap().parse::<Uuid>().unwrap(),
        graph_id,
        "the ocean the API returned must be the row that was committed"
    );

    // The scoped transaction must have COMMITTED, not merely succeeded — a
    // rolled-back tx would leave the API response above intact (the handler
    // re-reads through a different path) while the row vanished. The owner
    // read above already proves the commit; assert the account's DEK landed
    // too, so a mid-sequence abort cannot pass.
    let dek_present: bool =
        sqlx::query_scalar("SELECT dek_wrapped IS NOT NULL FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_one(&probe.owner)
            .await
            .expect("read account dek");
    assert!(
        dek_present,
        "provisioning must have continued past the ocean creation to the DEK step"
    );

    // Idempotency: a SECOND request for the same sub takes the short-circuit
    // (existing personal account) and must not create a duplicate ocean.
    let resp = app
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/v1/graphs")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ocean_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM graphs WHERE account_id = $1")
        .bind(account_id)
        .fetch_one(&probe.owner)
        .await
        .expect("count oceans");
    assert_eq!(
        ocean_count, 1,
        "re-authenticating must not provision a second ocean"
    );

    probe.teardown().await;
}

/// Permanent negative control for the test above.
///
/// The pre-fix statement shape — `INSERT INTO graphs (...)` executed on a
/// bare `coralswarm_app` connection with `app.account_id` never set — must
/// HARD-ERROR, and specifically with the strict `current_setting` failure
/// (SQLSTATE `42704`, `unrecognized configuration parameter`) rather than a
/// permission or constraint error that could coincidentally look the same.
/// The identical INSERT inside `begin_account_scoped` must then succeed, so
/// the difference is attributable to the scoping and nothing else.
///
/// If someone ever disables the policy, drops the role swap, or "fixes" a
/// future failure by handing `coralswarm_app` BYPASSRLS, this test goes red
/// and the end-to-end test above stops being evidence of anything.
#[tokio::test]
async fn unscoped_graph_insert_hard_errors_as_app_role() {
    let Some(probe) = RlsProbe::setup("orgt_rls_provisioning_f2_control").await else {
        eprintln!("skipping unscoped_graph_insert_hard_errors_as_app_role: set ORGT_TEST_BASE_URL");
        return;
    };

    const INSERT: &str = "INSERT INTO graphs (account_id, name, kind, is_primary) \
         VALUES ($1, 'Primary', 'personal', \
                 NOT EXISTS (SELECT 1 FROM graphs WHERE account_id = $1 AND is_primary)) \
         RETURNING id";

    // 1. Unscoped (the pre-fix `Store::create_graph` shape): must hard-error.
    let err = sqlx::query_scalar::<_, Uuid>(INSERT)
        .bind(probe.a.account_id)
        .fetch_one(&probe.app_pool)
        .await
        .expect_err(
            "the pre-fix unscoped INSERT INTO graphs MUST fail as coralswarm_app — if this \
             succeeds, RLS is not being enforced and the F2 end-to-end test proves nothing",
        );
    let code = err
        .as_database_error()
        .and_then(|e| e.code())
        .map(|c| c.to_string());
    assert_eq!(
        code.as_deref(),
        Some("42704"),
        "expected the strict current_setting failure (42704 undefined_object, \
         'unrecognized configuration parameter \"app.account_id\"'), got {err}"
    );

    // 2. The SAME statement inside `begin_account_scoped`: must succeed.
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let created: Uuid = sqlx::query_scalar(INSERT)
        .bind(probe.a.account_id)
        .fetch_one(&mut *tx)
        .await
        .expect("the scoped INSERT (what create_graph_scoped runs) must succeed");
    tx.commit().await.expect("commit scoped insert");

    let account_of: Uuid = sqlx::query_scalar("SELECT account_id FROM graphs WHERE id = $1")
        .bind(created)
        .fetch_one(&probe.owner)
        .await
        .expect("read back the scoped insert");
    assert_eq!(account_of, probe.a.account_id);

    // 3. And the scope is a real fence, not decoration: scoped to A, an
    //    INSERT claiming B's account must be refused by the policy's WITH
    //    CHECK (42501), never silently written.
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let err = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO graphs (account_id, name, kind, is_primary) \
         VALUES ($1, 'Forged', 'personal', false) RETURNING id",
    )
    .bind(probe.b.account_id)
    .fetch_one(&mut *tx)
    .await
    .expect_err("a cross-account graph INSERT must be refused under scope A");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501"),
        "expected the RLS WITH CHECK refusal (42501), got {err}"
    );
    tx.rollback().await.ok();

    probe.teardown().await;
}

// ── F5 ──────────────────────────────────────────────────────────────────────

/// Migration `0089_session_share_function_public_revoke` must close the
/// `PUBLIC` EXECUTE default that `0087_session_share_bootstrap_functions`
/// left open on its two SECURITY DEFINER resolvers, WITHOUT disturbing
/// `coralswarm_app`.
///
/// The probe role is created with `LOGIN` and nothing else, then granted only
/// `CONNECT` on the scratch database — the shape of a future reporting or
/// migration-tooling role that is deliberately denied `SELECT` on the content
/// tables. Two assertions per function:
///
///   * it CANNOT call either 0087 function (SQLSTATE `42501`), and
///   * it genuinely cannot read the underlying tables either, so the
///     functions were the only way through.
///
/// Plus an in-test CONTROL: a throwaway SECURITY DEFINER function created in
/// the same schema WITHOUT a REVOKE, which this role CAN call. That is what
/// distinguishes "0089's REVOKE works" from "this role could never have
/// executed anything here anyway" (missing schema `USAGE`, a failed login,
/// etc.) — without it, a broken probe would look like a passing test.
#[tokio::test]
async fn session_share_functions_are_not_executable_by_public() {
    const DB_NAME: &str = "orgt_rls_0089_public_revoke";
    let Some(probe) = RlsProbe::setup(DB_NAME).await else {
        eprintln!(
            "skipping session_share_functions_are_not_executable_by_public: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let base = std::env::var("ORGT_TEST_BASE_URL").expect("checked by setup above");
    // Same base-URL → scratch-DB-URL rewrite the harness does internally
    // (`with_db`), repeated here rather than widening the harness's API for
    // one caller: swap the path segment, keep any query parameters.
    let scratch_url = {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{DB_NAME}{params}")
    };

    // Role name derived from this suite's scratch DB so concurrent suites on
    // the same (cluster-wide) role namespace never collide.
    const PROBE_ROLE: &str = "orgt_0089_connect_only";
    const PROBE_PW: &str = "orgt_0089_connect_only_test_only_pw";

    // Cluster-wide DDL — drop any leftover from a panicked previous run first.
    let mut admin = PgConnection::connect(&base).await.expect("connect admin");
    admin
        .execute(format!("DROP ROLE IF EXISTS {PROBE_ROLE}").as_str())
        .await
        .expect("drop stale probe role");
    admin
        .execute(format!("CREATE ROLE {PROBE_ROLE} LOGIN PASSWORD '{PROBE_PW}'").as_str())
        .await
        .expect("create connect-only probe role");

    // Seed a real share so a SUCCESSFUL call would return a non-NULL answer —
    // otherwise a permission failure and "resolved to NULL" would be hard to
    // tell apart in the coralswarm_app leg below.
    sqlx::query(
        "INSERT INTO projects(graph_id, project_key) VALUES ($1, 'p0') ON CONFLICT DO NOTHING",
    )
    .bind(probe.a.graph_id)
    .execute(&probe.owner)
    .await
    .expect("seed project");
    sqlx::query(
        "INSERT INTO coding_sessions(graph_id, session_id, project_key, title) \
         VALUES ($1, 'sess-0089', 'p0', 'grant probe')",
    )
    .bind(probe.a.graph_id)
    .execute(&probe.owner)
    .await
    .expect("seed coding session");
    let share_id: Uuid = sqlx::query_scalar(
        "INSERT INTO coding_session_shares(graph_id, session_id, token, visibility, created_by) \
         VALUES ($1, 'sess-0089', 'tok-0089', 'link', $2) RETURNING id",
    )
    .bind(probe.a.graph_id)
    .bind(probe.a.user_id)
    .fetch_one(&probe.owner)
    .await
    .expect("seed coding session share");

    // The in-test control: same schema, same SECURITY DEFINER shape, no REVOKE.
    probe
        .owner
        .execute(
            "CREATE FUNCTION orgt_0089_control_fn() RETURNS int \
             LANGUAGE sql SECURITY DEFINER STABLE SET search_path = public, pg_temp \
             AS $$ SELECT 42 $$",
        )
        .await
        .expect("create control function");
    probe
        .owner
        .execute(format!("GRANT CONNECT ON DATABASE {DB_NAME} TO {PROBE_ROLE}").as_str())
        .await
        .expect("grant CONNECT to the probe role");

    let opts = PgConnectOptions::from_str(&scratch_url)
        .expect("parse scratch db url")
        .username(PROBE_ROLE)
        .password(PROBE_PW);
    let connect_only = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .expect("connect AS the CONNECT-only probe role");

    // CONTROL — proves the role really can reach and execute functions in
    // `public`, so the refusals below are 0089's doing and not an artifact.
    let control: i32 = sqlx::query_scalar("SELECT orgt_0089_control_fn()")
        .fetch_one(&connect_only)
        .await
        .expect(
            "the CONNECT-only role must be able to execute a SECURITY DEFINER function that was \
             NOT revoked from PUBLIC — if this fails the rest of this test proves nothing",
        );
    assert_eq!(control, 42);

    // …and it genuinely has no other way to the same facts. `accounts` is a
    // CONTROL-plane table with no 0083 policy, so the refusal here is a pure
    // table-ACL decision and pins the exact code (42501): this role holds no
    // `SELECT` anywhere.
    let err = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM accounts")
        .fetch_one(&connect_only)
        .await
        .unwrap_err();
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501"),
        "the CONNECT-only role must hold no direct SELECT on accounts, got {err}"
    );
    // The two tables the 0087 functions read are RLS-covered, and Postgres
    // surfaces the policy's own strict `current_setting` failure (42704)
    // ahead of the table-ACL refusal on these — so assert only that they are
    // unreachable, not which of the two doors slams first. Either way the
    // SECURITY DEFINER functions were the only path through, which is the
    // point.
    for table in ["coding_session_shares", "graphs"] {
        let err = sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&connect_only)
            .await
            .unwrap_err();
        let code = err
            .as_database_error()
            .and_then(|e| e.code())
            .map(|c| c.to_string());
        assert!(
            matches!(code.as_deref(), Some("42501") | Some("42704")),
            "the CONNECT-only role must not be able to read {table} \
             (expected 42501 insufficient_privilege or 42704 from the RLS predicate), got {err}"
        );
    }

    // THE PROPERTY UNDER TEST — both 0087 functions refused.
    let err = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT resolve_session_share_account_id('tok-0089')",
    )
    .fetch_one(&connect_only)
    .await
    .expect_err(
        "resolve_session_share_account_id must NOT be executable by a CONNECT-only role \
         (migration 0089 revokes the PUBLIC default 0087 left open)",
    );
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501"),
        "expected insufficient_privilege (42501), got {err}"
    );

    let err = sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_session_share_graph_id($1)")
        .bind(share_id)
        .fetch_one(&connect_only)
        .await
        .expect_err(
            "resolve_session_share_graph_id must NOT be executable by a CONNECT-only role \
             (migration 0089 revokes the PUBLIC default 0087 left open)",
        );
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("42501"),
        "expected insufficient_privilege (42501), got {err}"
    );

    // AND the app role is untouched: both still resolve, unscoped, which is
    // the whole point of these bootstrap functions.
    let account_id: Option<Uuid> =
        sqlx::query_scalar("SELECT resolve_session_share_account_id('tok-0089')")
            .fetch_one(&probe.app_pool)
            .await
            .expect("coralswarm_app must still hold its explicit EXECUTE grant");
    assert_eq!(
        account_id,
        Some(probe.a.account_id),
        "the share token must still resolve to its owning account for the app role"
    );
    let graph_id: Option<Uuid> = sqlx::query_scalar("SELECT resolve_session_share_graph_id($1)")
        .bind(share_id)
        .fetch_one(&probe.app_pool)
        .await
        .expect("coralswarm_app must still hold its explicit EXECUTE grant");
    assert_eq!(graph_id, Some(probe.a.graph_id));

    connect_only.close().await;
    probe
        .owner
        .execute(format!("DROP OWNED BY {PROBE_ROLE}").as_str())
        .await
        .ok();
    probe.teardown().await;
    // The role is cluster-wide, so it outlives the scratch DB — drop it last,
    // after the database that carried its CONNECT grant is gone.
    admin
        .execute(format!("DROP ROLE IF EXISTS {PROBE_ROLE}").as_str())
        .await
        .ok();
}
