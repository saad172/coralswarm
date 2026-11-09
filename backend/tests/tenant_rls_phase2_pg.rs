//! Integration tests for tenant RLS **Phase 2** (this PR): proof that real
//! application code — not hand-rolled SQL — carries the right session GUC
//! when run through the actual code paths this PR converted, from a
//! connection authenticated AS the low-privilege `coralswarm_tenant_app` role
//! with `migrations_tenant/0008_tenant_rls`'s policies fully live.
//!
//! `backend/tests/tenant_rls_pg.rs` (Phase 1) already proves the POLICIES
//! themselves are correct, using hand-rolled `set_config('app.user_id', ...)`
//! calls. That is deliberately NOT what this suite re-proves. What Phase 1
//! could not prove — because the Rust primitives had no call sites yet — is
//! that the WIRING is correct: that real request-shaped code
//! ([`coralswarm_backend::store::PostgresStore::ocean_stats_scoped`], a
//! Category B conversion in this PR) and the real worker-pool primitive
//! ([`coralswarm_backend::tenancy::registry::TenantRegistry::
//! get_or_build_worker_pool`], this PR's Category C answer) actually set the
//! GUCs 0008's policies read, end to end, through the exact functions this
//! PR ships — not through a test-authored substitute for them.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every other
//! `*_pg.rs` suite in this tree:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test tenant_rls_phase2_pg

mod support;

use std::str::FromStr;
use std::sync::Arc;

use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::{TenantPoolError, TenantRegistry};
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use support::tenant_role::{set_app_role_password_with_retry, APP_ROLE_TEST_PASSWORD};
use uuid::Uuid;

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// Same base URL, but authenticated as `coralswarm_tenant_app` instead of
/// whatever role `ORGT_TEST_BASE_URL` names — this is the literal DSN string
/// [`TenantRegistry`] resolves via its [`StaticMapResolver`], exactly as it
/// would resolve a real `tenant_databases.db_secret_ref` in production.
fn as_app_role_dsn(db_url: &str) -> String {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_tenant_app")
        .password(APP_ROLE_TEST_PASSWORD);
    // sqlx's PgConnectOptions doesn't itself serialize back to a URL string,
    // so build one directly — same components recreate_db/with_db already
    // assume (host/port/db), with the role swapped.
    let host = opts.get_host();
    let port = opts.get_port();
    let db = db_url.rsplit('/').next().unwrap_or("postgres");
    format!("postgres://coralswarm_tenant_app:{APP_ROLE_TEST_PASSWORD}@{host}:{port}/{db}")
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

/// One org, two members, one commons ocean and one member-private ocean each
/// — enough to prove `ocean_stats_scoped` (this PR's `Store::ocean_stats_scoped`
/// conversion) isolates atom/node counts by acting user, through the real
/// method, not a hand probe.
struct Fx {
    account: Uuid,
    user_a: Uuid,
    user_b: Uuid,
    g_a: Uuid,
    g_b: Uuid,
}

async fn seed(owner: &PgPool, _org_id: &str) -> Fx {
    let fx = Fx {
        account: Uuid::new_v4(),
        user_a: Uuid::new_v4(),
        user_b: Uuid::new_v4(),
        g_a: Uuid::new_v4(),
        g_b: Uuid::new_v4(),
    };
    let atom_a = Uuid::new_v4();
    let node_a = Uuid::new_v4();
    let atom_b = Uuid::new_v4();
    let node_b = Uuid::new_v4();

    let sql = format!(
        r#"
INSERT INTO graphs(id, account_id, name, kind, is_primary)
  VALUES ('{g_commons}', '{account}', 'commons', 'org', true);
INSERT INTO graphs(id, account_id, name, kind, owner_user_id)
  VALUES ('{g_a}', '{account}', 'A private reef', 'member', '{user_a}');
INSERT INTO graphs(id, account_id, name, kind, owner_user_id)
  VALUES ('{g_b}', '{account}', 'B private reef', 'member', '{user_b}');

INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, "timestamp")
  VALUES ('{atom_a}', '{g_a}', 'test', 'a-secret-1', 'blob', now());
INSERT INTO nodes(node_id, graph_id, type, title, body)
  VALUES ('{node_a}', '{g_a}', 'entity', 'A secret node', '{{}}');
INSERT INTO node_atoms(node_id, atom_id) VALUES ('{node_a}', '{atom_a}');

INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, "timestamp")
  VALUES ('{atom_b}', '{g_b}', 'test', 'b-secret-1', 'blob', now());
INSERT INTO nodes(node_id, graph_id, type, title, body)
  VALUES ('{node_b}', '{g_b}', 'entity', 'B secret node', '{{}}');
INSERT INTO node_atoms(node_id, atom_id) VALUES ('{node_b}', '{atom_b}');
"#,
        g_commons = Uuid::new_v4(),
        account = fx.account,
        g_a = fx.g_a,
        user_a = fx.user_a,
        g_b = fx.g_b,
        user_b = fx.user_b,
        atom_a = atom_a,
        node_a = node_a,
        atom_b = atom_b,
        node_b = node_b,
    );
    owner
        .execute(sql.as_str())
        .await
        .expect("seed phase2 fixture");

    fx
}

/// **Phase-3 tripwire proof.** The worker pool used to carry
/// `app.tenant_actor='system'` at the SESSION level, set via the connection
/// STARTUP packet — which a connection pooler rejects outright, so it had to go
/// (see `TenantRegistry::connect_and_verify`). The GUC is now set per
/// transaction by `begin_tenant_system_scoped` instead.
///
/// Removing it is safe today ONLY because production connects as the table
/// owner and `0009` §7 deliberately does not FORCE row security, so no policy
/// applies. This test pins the other half of that bargain: under a role the
/// policies DO apply to — which is exactly what Phase 3 introduces — building a
/// worker pool must FAIL LOUDLY rather than hand back a connection whose
/// ambient queries would silently return nothing.
///
/// Without this, Phase 3 turns every tenant-loop worker into a no-op that
/// reports success against an empty result set.
#[tokio::test]
async fn worker_pool_refuses_a_role_that_rls_applies_to() {
    let Some(base) = base_url() else {
        eprintln!("skipping worker_pool_refuses_a_role_that_rls_applies_to: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_phase2_worker_pool";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0008/0009_tenant_rls");
    set_app_role_password_with_retry(&owner).await;

    let org_id = "org_phase2_worker_pool";
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&owner)
        .await
        .expect("seed tenant_meta fingerprint");

    let fx = seed(&owner, org_id).await;

    let resolver = StaticMapResolver::from_pairs([("SECRET", as_app_role_dsn(&db_url))]);
    let registry = TenantRegistry::new(Arc::new(resolver));

    // The DSN authenticates as `coralswarm_tenant_app` — NOBYPASSRLS, and not
    // the table owner. That is precisely the Phase-3 configuration, so the
    // tripwire must fire.
    let err = registry
        .get_or_build_worker_pool(org_id, fx.account, "SECRET")
        .await
        .expect_err(
            "building a worker pool under a role that tenant RLS applies to must fail — \
             otherwise every worker silently reads an empty database after Phase 3",
        );

    match err {
        TenantPoolError::WorkerPoolNotRlsExempt { org, role } => {
            assert_eq!(org, org_id);
            assert_eq!(
                role, "coralswarm_tenant_app",
                "the error must name the offending role so the fix is obvious from the log"
            );
        }
        other => panic!("expected WorkerPoolNotRlsExempt, got {other:?}"),
    }

    // A failed build must never be cached — the next caller has to retry the
    // whole thing, exactly as every other failure variant behaves.
    let again = registry
        .get_or_build_worker_pool(org_id, fx.account, "SECRET")
        .await;
    assert!(
        matches!(again, Err(TenantPoolError::WorkerPoolNotRlsExempt { .. })),
        "the refusal must be reproducible, not a one-shot poisoned cell"
    );

    // And the replacement mechanism works under that same low-privilege role:
    // a per-transaction system scope sees every ocean, which is what the
    // deleted session-level GUC used to buy. This is the path the tenant-loop
    // workers move to.
    let request_pool = registry
        .get_or_build(org_id, fx.account, "SECRET")
        .await
        .expect("request pool builds fine — it is not subject to the tripwire");
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(
        &request_pool,
        fx.account,
    )
    .await
    .expect("begin_tenant_system_scoped");
    let graph_count: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        graph_count, 3,
        "a system-scoped transaction must still see commons + both member reefs"
    );
    tx.rollback().await.ok();

    request_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// **Category B proof, real code path.** Calls the actual
/// [`coralswarm_backend::store::Store::ocean_stats_scoped`] method — this
/// PR's conversion of `store/mod.rs`'s `_scoped` Store surface — through a
/// [`PostgresStore`] built directly over a connection authenticated AS
/// `coralswarm_tenant_app`, and proves member A sees A's own reef's stats
/// while member B querying the SAME graph_id sees nothing. This is not a
/// hand-rolled SQL probe (`tenant_rls_pg.rs` already has those for the raw
/// predicate) — it is the literal application function
/// `routes::graphs::get_ocean_stats` / `mcp::tools::read::get_ocean_stats` /
/// `self_context::build_system_context` all call, exercised end to end
/// against the low-privilege role with 0008's policies live.
#[tokio::test]
async fn ocean_stats_scoped_isolates_by_acting_user_through_real_application_code() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping ocean_stats_scoped_isolates_by_acting_user_through_real_application_code: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_phase2_ocean_stats";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0008/0009_tenant_rls");
    set_app_role_password_with_retry(&owner).await;

    let fx = seed(&owner, "org_phase2_ocean_stats").await;

    // The REQUEST-style pool — same primitive `Tenant::begin_member_scoped`
    // resolves against in a real handler (registry::get_or_build, not the
    // worker pool), connected as the same low-privilege role.
    let app_pool: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&as_app_role_dsn(&db_url))
        .await
        .expect("connect as coralswarm_tenant_app");
    let store = PostgresStore::new(app_pool.clone());

    // Member A, querying A's own reef: real content, real counts.
    let stats_a = store
        .ocean_stats_scoped(fx.account, fx.user_a, fx.g_a)
        .await
        .expect("ocean_stats_scoped must succeed for A's own account/graph");
    assert_eq!(stats_a.atom_count, 1, "A must see A's own atom");
    assert!(
        stats_a
            .reef_counts
            .iter()
            .any(|(t, n)| t == "entity" && *n >= 1),
        "A must see A's own entity reef — got {:?}",
        stats_a.reef_counts
    );

    // Member B, querying A's graph_id directly (the IDOR-pivot shape): the
    // SAME real function, a DIFFERENT user_id — must show zero, not an error
    // and not A's real content. This is `ocean_stats_scoped`'s actual
    // production call shape (routes::graphs::get_ocean_stats passes the
    // GRAPH'S OWNING account plus the CALLER's own user_id, exactly like
    // this).
    let stats_b_on_a = store
        .ocean_stats_scoped(fx.account, fx.user_b, fx.g_a)
        .await
        .expect("ocean_stats_scoped must not error on a cross-member graph_id");
    assert_eq!(
        stats_b_on_a.atom_count, 0,
        "member B must see ZERO atoms querying member A's graph_id through the real \
         ocean_stats_scoped function — the member-ocean boundary leaked"
    );
    assert!(
        stats_b_on_a.reef_counts.is_empty(),
        "member B must see no reef counts for member A's graph_id — got {:?}",
        stats_b_on_a.reef_counts
    );

    // And B sees B's own reef fine — the boundary denies, it does not blank
    // out everything for B.
    let stats_b = store
        .ocean_stats_scoped(fx.account, fx.user_b, fx.g_b)
        .await
        .expect("ocean_stats_scoped must succeed for B's own account/graph");
    assert_eq!(stats_b.atom_count, 1, "B must see B's own atom");

    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}
