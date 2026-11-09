//! Integration test for the binding gate's runtime resolution (org-tenancy PR
//! 1.2, part 2): `TenantRuntime::resolve_binding` against real databases —
//! routing lookup + fingerprint-verified tenant pool + the full gate policy.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE DATABASE
//! rights). Sets up a control DB (accounts + tenant_databases) and one active
//! tenant DB, then drives the gate through the key verdicts.

#[path = "support/retained_database.rs"]
mod retained_database;

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::{GateReject, TenancyDecision, TenantRuntime};
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;
use uuid::Uuid;
use coralswarm_backend::tenancy::gate::TokenKind;

const APEX: &str = "coralswarm.example";

/// Serializes every test in this file — same pattern as
/// `cross_tenant_probe_pg.rs`'s `ENV_LOCK`, for two reasons:
///
/// 1. the HTTP-level tests mutate process-global env (`CLERK_ISSUER`,
///    `CLERK_JWKS_URL`), and
/// 2. **every** test here applies the control tree to its own scratch DB, and
///    migration 0083's `coralswarm_app` is a CLUSTER-WIDE role — two concurrent
///    applications race `CREATE ROLE` into a 23505 that escapes 0083's
///    XX000-only retry guard (observed for real in #286, and again on this
///    file the moment it gained a second test). The CI step also pins
///    `--test-threads=1`; this lock keeps a plain `cargo test --test
///    binding_gate_pg` correct without it.
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}
async fn recreate(conn: &mut PgConnection, db: &str) {
    retained_database::recreate(conn, db).await.unwrap();
}

#[tokio::test]
async fn binding_gate_resolves_and_rejects() {
    let Some(base) = base_url() else {
        eprintln!("skipping binding_gate_resolves_and_rejects: set ORGT_TEST_BASE_URL");
        return;
    };
    // See `ENV_LOCK`: the control tree's cluster-wide `CREATE ROLE` cannot run
    // concurrently with another test's.
    let _env_guard = ENV_LOCK.lock().await;
    let (ctl, ta) = ("orgt_gate_ctl", "orgt_gate_a");
    let mut conn = admin(&base).await;
    for db in [ctl, ta] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // Control DB: control tree + an account + an ACTIVE routing row for acme→org_a
    // and a SUSPENDED routing row for beta→org_b.
    let control: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, ctl))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control")
        .run(&control)
        .await
        .unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'t@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, 'org_a' FROM u RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    // The authenticated caller `resolve_binding` is resolving on behalf of —
    // stamped onto the resulting `TenantContext` (org-tenancy PR 1.7 security
    // fix) so `ReachabilityResolver` can enforce "this context is only valid
    // for the user it was built for". Distinct from `account_id` on purpose:
    // this test doesn't need a real `account_members` row for it, since
    // `resolve_binding` itself never touches membership (that's PR 1.2's
    // earlier control-plane `ensure_org_membership`, out of scope here).
    let user_id = Uuid::new_v4();
    let suspended_account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(email) VALUES('suspended@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, 'org_b' FROM u RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    for (org, account, slug, secret, status) in [
        ("org_a", account_id, "acme", "SECRET_A", "active"),
        ("org_b", suspended_account_id, "beta", "SECRET_B", "suspended"),
    ] {
        financial_fixture::execute(
            &control,
            account,
            sqlx::query(
                "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
                 VALUES($1,$2,$3,$4,$5)",
            )
            .bind(org)
            .bind(account)
            .bind(slug)
            .bind(secret)
            .bind(status),
        )
        .await
        .unwrap();
    }

    // Tenant DB for org_a: tenant tree + fingerprint.
    let tpool = PgPoolOptions::new().connect(&with_db(&base, ta)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tpool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_a')")
        .execute(&tpool)
        .await
        .unwrap();
    tpool.close().await;

    let resolver = StaticMapResolver::from_pairs([("SECRET_A", with_db(&base, ta))]);
    let rt = TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver))));

    let host = |sub: &str| format!("{sub}.{APEX}");

    // Apex + personal → Personal.
    assert!(matches!(
        rt.resolve_binding(&control, APEX, TokenKind::Personal, account_id, user_id, None).await,
        Ok(TenancyDecision::Personal)
    ));
    // Apex + org claim → that claim's OWN tenant.
    //
    // This expectation INVERTED with claim-based tenancy (`docs/plans/
    // claim-based-tenancy.md` PR 1.2). The apex used to refuse every org token
    // ("org work belongs on the subdomain"); it is now every tenant's door, and
    // the claim — never the Host — names the tenant. This is the
    // integration-level twin of `gate::tests::
    // apex_org_token_matrix_binds_only_on_an_active_row`, which PR 1.2 inverted
    // in lockstep; the plan names this arm as the one expectation that must
    // invert, and its neighbours below as the ones that must not.
    //
    // Asserted through the POOL, not just the verdict: "bound a tenant" is not
    // the property — "bound the database belonging to the org the claim named"
    // is, and only the fingerprint on the far side of the pool can say so.
    match rt
        .resolve_binding(&control, APEX, TokenKind::Org("org_a".into()), account_id, user_id, None)
        .await
    {
        Ok(TenancyDecision::Org(tc)) => {
            assert_eq!(tc.org_id, "org_a");
            assert_eq!(tc.user_id, user_id, "TenantContext must carry the resolving user's own id");
            let seen: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta")
                .fetch_one(&tc.pool)
                .await
                .unwrap();
            assert_eq!(seen, "org_a", "the apex must bind the CLAIM's own tenant DB");
        }
        other => panic!("expected the apex to bind org_a's tenant, got {other:?}"),
    }
    // ...and the arms that must NOT have inverted with it. The plan lists
    // `Missing`/`Provisioning`/`Suspended`/`Offboarded` as hard requirements
    // precisely because the realistic failure is someone correctly inverting
    // the `Active` arm above and then carrying the same reasoning across to its
    // neighbours. Two are reachable from this fixture — `org_b` is SUSPENDED,
    // and `org_c_no_routing_row` has no routing row at all — and neither may
    // fall through to the consumer plane, where reachability has no account
    // filter and would hand back the union of every account this user is in.
    for claim in ["org_b", "org_c_no_routing_row"] {
        assert!(
            matches!(
                rt.resolve_binding(&control, APEX, TokenKind::Org(claim.into()), account_id, user_id, None).await,
                Err(GateReject::Forbidden(_))
            ),
            "an org token on the apex must still be refused for {claim}"
        );
    }
    // ── The gate's `Subdomain` arm, UNCHANGED — but no longer reachable from
    //    ANY real request as of claim-based-tenancy §Phase 3 PR 3.3.
    //
    //    Phase 4 cutover step 3 stops SERVING `<slug>.api.<apex>` for REST;
    //    step 4 (separate, not yet authorized) deletes this arm outright. As
    //    of PR 3.3, `/mcp` no longer reaches it either: it used to be the one
    //    surviving caller (`RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`, since
    //    deleted), but PR 3.3 retired `/mcp`'s OWN, independent org-host
    //    classification (`mcp::McpConfig::resolve_resource`'s `Subdomain` arm
    //    now collapses to `personal_identity`), so nothing constructs the
    //    `ResourceTenancy::Org` that used to route into this arm via MCP
    //    either. This test still drives `resolve_binding` DIRECTLY — below
    //    the router, so below `retired_org_host_guard` — and its expectations
    //    are left UNCHANGED (this arm's logic is dead code now, not deleted
    //    code; deleting it is step 4's job, a separate, not-yet-authorized
    //    change) rather than softened to match nothing calling it in
    //    production anymore.
    //
    //    The retirement itself is asserted one layer up, through the real
    //    router, by
    //    `retired_and_foreign_hosts_reject_before_auth_with_zero_control_plane_writes`
    //    below — which also proves the 410 is identical for the ACTIVE slug
    //    and the imaginary one. ─────────────────────────────────────────────
    //
    // acme subdomain + matching org → Org, and the pool reaches org_a's tenant DB.
    match rt
        .resolve_binding(&control, &host("acme"), TokenKind::Org("org_a".into()), account_id, user_id, None)
        .await
    {
        Ok(TenancyDecision::Org(tc)) => {
            assert_eq!(tc.org_id, "org_a");
            assert_eq!(tc.user_id, user_id, "TenantContext must carry the resolving user's own id");
            let seen: String = sqlx::query_scalar("SELECT org_id FROM tenant_meta")
                .fetch_one(&tc.pool)
                .await
                .unwrap();
            assert_eq!(seen, "org_a", "tenant pool must reach org_a's DB");
        }
        other => panic!("expected Org, got {other:?}"),
    }
    // acme subdomain + WRONG org (org_b token) → Forbidden.
    assert!(matches!(
        rt.resolve_binding(&control, &host("acme"), TokenKind::Org("org_b".into()), account_id, user_id, None).await,
        Err(GateReject::Forbidden(_))
    ));
    // acme subdomain + personal token → Forbidden.
    assert!(matches!(
        rt.resolve_binding(&control, &host("acme"), TokenKind::Personal, account_id, user_id, None).await,
        Err(GateReject::Forbidden(_))
    ));
    // unknown subdomain → NotFound (fail closed).
    assert!(matches!(
        rt.resolve_binding(&control, &host("nope"), TokenKind::Org("org_a".into()), account_id, user_id, None).await,
        Err(GateReject::NotFound)
    ));
    // suspended tenant → Forbidden.
    assert!(matches!(
        rt.resolve_binding(&control, &host("beta"), TokenKind::Org("org_b".into()), account_id, user_id, None).await,
        Err(GateReject::Forbidden(_))
    ));
    // foreign host → NotFound.
    assert!(matches!(
        rt.resolve_binding(&control, "evil.com", TokenKind::Org("org_a".into()), account_id, user_id, None).await,
        Err(GateReject::NotFound)
    ));

    control.close().await;
    for db in [ctl, ta] {
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Gate ORDERING: the Host is classified before the request is authenticated
// ═══════════════════════════════════════════════════════════════════════════
//
// The tests above drive `resolve_binding` directly, which never writes and so
// cannot say anything about ORDER. These drive the real `require_auth`
// middleware through the real public router, where the ordering question
// actually lives: the plan (PR 1.2 step 2) specifies "unknown subdomain → 404
// fail-closed BEFORE auth", but the gate used to run after
// `resolve_clerk_context`, which WRITES to the control plane
// (`ensure_user`/`ensure_org_account`/`refresh_org_membership_from_claims`).

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

fn org_token(sub: &str, org_id: &str) -> String {
    mint(json!({
        "iss": ISSUER,
        "sub": sub,
        "exp": (Utc::now().timestamp() + 3600) as usize,
        "org_id": org_id,
        "org_role": "org:member",
    }))
}

/// The real public router over an ENABLED tenant runtime, with `control` as
/// the ambient consumer plane — same shape as `cross_tenant_probe_pg.rs`'s
/// `build_app`.
fn build_app(
    control: &PgPool,
    resolvers: impl IntoIterator<Item = (&'static str, String)>,
) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
            resolvers,
        )))),
    ));
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// Every control-plane table `resolve_clerk_context` can write on the request
/// path, so "zero writes" is asserted by counting rows rather than by trusting
/// a status code.
async fn control_write_counts(control: &PgPool) -> Vec<(&'static str, i64)> {
    let mut counts = Vec::new();
    for table in ["users", "accounts", "account_members", "graphs"] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(control)
            .await
            .unwrap();
        counts.push((table, n));
    }
    counts
}

/// A host the gate refuses must be refused **before** the token is verified
/// and before a single control-plane row is written — and refused identically
/// whatever token it carries, so a probe cannot tell "real subdomain, bad
/// token" (401) from "no such subdomain" and enumerate tenants.
///
/// The property is unchanged by Phase 4 cutover step 3; only the status is.
/// Retired org subdomains now answer 410 instead of 404/403, and the
/// enumeration half got STRICTER rather than weaker: the fixture holds a real
/// ACTIVE slug (`acme`) alongside one that has never existed (`nope`), and the
/// two responses are now asserted byte-identical. Before the cutover they were
/// genuinely different (403 vs 404) — that difference was the oracle.
///
/// The positive control at the end is what makes the zero-write assertion
/// meaningful: the SAME token on the apex DOES provision (rows appear), so the
/// rejected-host path demonstrably skipped a write path that is live for this
/// exact request, rather than a write path that was never going to run.
#[tokio::test]
async fn retired_and_foreign_hosts_reject_before_auth_with_zero_control_plane_writes() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping retired_and_foreign_hosts_reject_before_auth_with_zero_control_plane_writes: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: serialized by `_env_guard` for this test's whole body.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let ctl = "orgt_gate_order_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, ctl))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    // One real, ACTIVE tenant so `acme` exists and `nope` genuinely doesn't.
    let account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'owner@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, 'org_a' FROM u RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
         VALUES('org_a',$1,'acme','SECRET_A','active')",
        )
        .bind(account_id),
    )
    .await
    .unwrap();

    // No secret is registered for SECRET_A on purpose: a request that got far
    // enough to build a tenant pool would fail loudly instead of passing.
    let app = build_app(&control, []);
    // A `sub` nobody has seen: if `resolve_clerk_context` ran at all it would
    // insert a `users` row for it.
    let sub = format!("gate_order_{}", Uuid::new_v4());
    let token = org_token(&sub, "org_a");

    let get = |host: &str, auth: Option<&str>| {
        let mut b = HttpRequest::builder()
            .method("GET")
            .uri("/v1/graphs")
            .header("host", host);
        if let Some(t) = auth {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        b.body(Body::empty()).unwrap()
    };

    let before = control_write_counts(&control).await;

    // ── 1. The retired org API address, real slug and imaginary slug alike,
    //    under every token shape → 410, byte-identical, zero writes.
    //
    //    `acme` HAS an active routing row in this fixture and `nope` has none.
    //    The whole point of collapsing the subdomain arm is that the caller
    //    cannot tell — so the bodies are compared, not just the statuses, and
    //    they are compared across the token axis too (valid org token, no
    //    token at all, syntactic garbage). A 401 anywhere in here would mean
    //    the JWT was verified before the Host was classified. ────────────────
    let mut retired_responses: Vec<(String, StatusCode, Vec<u8>)> = Vec::new();
    for host in ["acme.coralswarm.example", "nope.coralswarm.example"] {
        for (label, auth) in [
            ("a valid org token", Some(token.as_str())),
            ("no token at all", None),
            ("a syntactically invalid token", Some("not-a-jwt")),
        ] {
            let resp = app.clone().oneshot(get(host, auth)).await.unwrap();
            let status = resp.status();
            assert_eq!(
                status,
                StatusCode::GONE,
                "{host} with {label} must be 410 Gone — the org API address is retired"
            );
            retired_responses.push((format!("{host} / {label}"), status, body_bytes(resp).await));
        }
    }
    let (ref first_label, _, ref first_body) = retired_responses[0];
    for (label, _, body) in &retired_responses[1..] {
        assert_eq!(
            body, first_body,
            "the retired-address response differed between {first_label} and {label} — any \
             difference between a slug that exists and one that does not is an \
             org-existence oracle"
        );
    }
    // The FULL base, not just the apex substring. `contains(APEX)` was the first
    // draft and a negative control caught it: with `APEX = "coralswarm.example"`, a
    // message that hardcoded `https://api.coralswarm.example` instead of the
    // configured apex still contained it, so the mutation stayed green here
    // (only the unit test in `lib.rs` went red). A deployment advertising a base
    // it does not serve is exactly what this assertion is for.
    let expected_base = format!("https://{APEX}");
    assert!(
        String::from_utf8_lossy(first_body).contains(&expected_base),
        "the 410 body must NAME the CONFIGURED base ({expected_base}) to use instead, or the \
         cutover is a dead end for whoever hits it: {}",
        String::from_utf8_lossy(first_body)
    );
    assert_eq!(
        control_write_counts(&control).await,
        before,
        "a retired address must not provision anything: the Host is classified \
         before the JWT is verified and before resolve_clerk_context runs"
    );

    // ── 3. A foreign host → 404, NOT 410, with no database work at all. A
    //    domain that was never ours is not a retired address of ours. ────────
    for host in ["evil.com", "coralswarm-backend.example", "a.b.coralswarm.example"] {
        let resp = app.clone().oneshot(get(host, Some(&token))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "host {host} must 404");
    }
    assert_eq!(
        control_write_counts(&control).await,
        before,
        "no rejected host may write to the control plane"
    );

    // ── 4. Positive control: the SAME token on the apex reaches the post-auth
    //    half of the gate, and provisioning DID run for it. That is what proves
    //    the assertions above are about ORDERING, not about a dead write path.
    //
    //    The status changed with claim-based tenancy (PR 1.2) and is now
    //    load-bearing for a different reason. The apex BINDS the tenant the
    //    claim names, so this request gets as far as `finish_binding`'s pool
    //    build — and this fixture registers no secret for `SECRET_A` on purpose
    //    (`build_app(&control, [])` above), so that build fails. The gate must
    //    turn that failure into a refusal and never into a fall-through to the
    //    consumer plane, whose reachability has no account filter at all.
    //
    //    That is the plan's "lookup-error stays a refusal" hard requirement,
    //    and it is the ONE hard requirement `gate.rs`'s pure matrices say they
    //    cannot reach ("A lookup *error* is not a state on this axis:
    //    `finish_binding` maps a failed lookup to `GateReject::Unavailable`
    //    without ever reaching `decide`"). It is reachable here, so it is
    //    asserted here rather than left untested everywhere. ────────────────
    let resp = app.clone().oneshot(get(APEX, Some(&token))).await.unwrap();
    let status = resp.status();
    assert!(
        !status.is_success(),
        "a tenant pool that cannot be built must never be served: this caller \
         carries an org claim, so a 2xx here means the consumer plane answered \
         for org content ({status})"
    );
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "an unresolvable tenant secret is a retryable fail-closed 503 from the \
         claim-binding half of the gate"
    );
    let after = control_write_counts(&control).await;
    assert_ne!(
        after, before,
        "the apex request must have provisioned (proving the write path this \
         test asserts was SKIPPED for unknown hosts is live for this very token)"
    );
    let users_before = before.iter().find(|(t, _)| *t == "users").unwrap().1;
    let users_after = after.iter().find(|(t, _)| *t == "users").unwrap().1;
    assert_eq!(
        users_after,
        users_before + 1,
        "resolve_clerk_context inserts this caller's users row once it runs"
    );

    control.close().await;
    retained_database::finish(&mut conn, ctl, true)
        .await
        .ok();
}

/// The whole response body, so the forwarded-host probe can assert a forged
/// header changed NOTHING — not merely that two statuses happened to match.
async fn body_bytes(resp: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read response body")
        .to_vec()
}

/// Plan lines 53 + 128: a spoofed `X-Forwarded-Host` must NEVER select a
/// tenant. The gate is correct today by omission — `middleware::auth` reads
/// `axum::http::header::HOST` and nothing else — and this is the regression
/// probe that keeps it that way, since nothing in the type system stops a
/// future edit from trusting a proxy header.
///
/// Claim-based tenancy (PR 1.2) raised the stakes rather than lowering them:
/// the apex now binds a tenant from the claim, so there IS a live routing
/// decision on the apex for a forged header to try to steer. The apex arm is
/// therefore asserted differentially (identical response with and without the
/// header) instead of as a status literal — see arm 2 below.
#[tokio::test]
async fn spoofed_forwarded_host_never_selects_a_tenant() {
    let Some(base) = base_url() else {
        eprintln!("skipping spoofed_forwarded_host_never_selects_a_tenant: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: serialized by `_env_guard` for this test's whole body.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ta) = ("orgt_fwd_host_ctl", "orgt_fwd_host_ta");
    let mut conn = admin(&base).await;
    for db in [ctl, ta] {
        let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, ctl))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'owner@y.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, 'org_a' FROM u RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
         VALUES('org_a',$1,'acme','SECRET_A','active')",
        )
        .bind(account_id),
    )
    .await
    .unwrap();

    let tpool = PgPoolOptions::new().connect(&with_db(&base, ta)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tpool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_a')")
        .execute(&tpool)
        .await
        .unwrap();
    tpool.close().await;

    let app = build_app(&control, [("SECRET_A", with_db(&base, ta))]);
    let sub = format!("fwd_host_{}", Uuid::new_v4());
    let token = org_token(&sub, "org_a");

    let get = |host: &str, forwarded: Option<&str>| {
        let mut b = HttpRequest::builder()
            .method("GET")
            .uri("/v1/graphs")
            .header("host", host)
            .header("authorization", format!("Bearer {token}"));
        if let Some(f) = forwarded {
            // Both spellings a proxy might use.
            b = b.header("x-forwarded-host", f).header("forwarded", format!("host={f}"));
        }
        b.body(Body::empty()).unwrap()
    };

    // Baseline: the real `acme` host is RETIRED (Phase 4 cutover step 3) and
    // answers 410 — it no longer serves org A, or anyone.
    let real = app.clone().oneshot(get("acme.coralswarm.example", None)).await.unwrap();
    let real_status = real.status();
    assert_eq!(
        real_status,
        StatusCode::GONE,
        "control: the retired org API address answers 410, whatever token it carries"
    );

    // 1. A forged forwarded-host pointing AT the (retired) tenant address
    //    cannot promote a foreign host onto it — still the Host's own 404,
    //    never the 410 that would mean the forged header selected the shape.
    for host in ["evil.com", "coralswarm-backend.example"] {
        let resp = app
            .clone()
            .oneshot(get(host, Some("acme.coralswarm.example")))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "X-Forwarded-Host must not make {host} classify as anything but foreign"
        );
    }

    // 2. On the apex, a forged forwarded-host cannot change what the request
    //    resolves to.
    //
    //    This arm used to assert a flat 403, and that assertion was cheap:
    //    before claim-based tenancy (PR 1.2) an org token on the apex was
    //    refused outright, so no header could steer a binding that never
    //    happened. The apex now binds the tenant the CLAIM names, which is
    //    exactly what turns this arm from a formality into the probe's real
    //    question — a binding genuinely happens here, so can a proxy header
    //    redirect it?
    //
    //    Stated DIFFERENTIALLY rather than as a status literal: the response
    //    must be identical — status and body — to the same request carrying no
    //    forged header at all. A literal can be satisfied by a refusal that
    //    happens for some unrelated reason; equality with the un-forged request
    //    is what actually says the header was never read. (Its twin in
    //    `cross_tenant_probe_pg` runs the same shape with a SECOND live tenant
    //    for the forged header to name.)
    let clean = app.clone().oneshot(get(APEX, None)).await.unwrap();
    let clean_status = clean.status();
    let clean_body = body_bytes(clean).await;
    assert_eq!(
        clean_status,
        StatusCode::OK,
        "control: on the apex, org A's own claim must bind org A's tenant — if \
         this is a refusal the equality assertions below compare two refusals \
         and prove nothing"
    );
    for spoof in [
        "acme.coralswarm.example",
        "beta.coralswarm.example",
        "nope.coralswarm.example",
        "evil.com",
        APEX,
    ] {
        let resp = app.clone().oneshot(get(APEX, Some(spoof))).await.unwrap();
        let status = resp.status();
        assert_eq!(
            status, clean_status,
            "X-Forwarded-Host: {spoof} changed an apex request's status \
             ({status} vs {clean_status} with no forged header)"
        );
        assert_eq!(
            body_bytes(resp).await,
            clean_body,
            "X-Forwarded-Host: {spoof} changed an apex request's body"
        );
    }

    // 3. A forged forwarded-host cannot RESURRECT the retired address either.
    //    `X-Forwarded-Host: <apex>` is the interesting one: if the header were
    //    ever read, that is exactly the value that would turn a 410 back into a
    //    served apex request, restoring the address this PR retires by way of a
    //    client-supplied header.
    //
    //    Two 410s being equal is a weak comparison on its own, which is why
    //    arm 2's clean apex request above is asserted to be a real 200 with
    //    real content first: this file is not in a state where everything
    //    refuses.
    for forged in ["evil.com", "nope.coralswarm.example", APEX] {
        let resp = app
            .clone()
            .oneshot(get("acme.coralswarm.example", Some(forged)))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            real_status,
            "X-Forwarded-Host: {forged} changed how the retired acme address is answered"
        );
    }

    control.close().await;
    for db in [ctl, ta] {
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }
}
