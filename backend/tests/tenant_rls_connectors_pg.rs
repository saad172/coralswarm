//! Integration tests for the tenant-RLS **connector/webhook cluster**
//! resolution (security/tenant-rls-connectors, closing
//! `docs/tenant-rls-cutover-runbook.md` §2.5's 11 `pending` entries — see
//! §2.6 for the full per-site trace and the manifest for the file-by-file
//! reasoning).
//!
//! # Why this suite exists, and what it does NOT try to prove
//!
//! Every real call site in the 11 connector/webhook files was traced (not
//! assumed) to run exclusively against the CONSUMER/CONTROL pool (`state.db`,
//! or a `Tenant.pool` resolved only after a `reject_org_*` gate has already
//! 403'd any `TenancyDecision::Org` request) — see the manifest and the
//! runbook's §2.6. `routes::slack::resolve_primary_graph` in particular
//! cannot even be pointed at a genuine tenant-only database today: its first
//! query (`SELECT clerk_org_id IS NOT NULL FROM accounts WHERE id = $1`) reads
//! the control-plane `accounts` table, which does not exist in
//! `migrations_tenant` at all — an architectural, not just a call-graph,
//! confirmation that this cluster is consumer-plane-only. So this suite does
//! NOT attempt to drive `resolve_primary_graph`/`IngestCaller` against a real
//! tenant pool (it would error on a missing relation, proving nothing useful
//! about RLS).
//!
//! What it DOES prove, against a REAL tenant database with
//! `migrations_tenant/0009_tenant_rls`'s policies live and the low-privilege
//! `coralswarm_tenant_app` role: `teams_watched_resources` — one of the
//! exact tables `routes::teams_ingest.rs` (one of the 11 exempted files)
//! writes and reads via `crate::tenancy::scope::begin_account_scoped` today —
//! carries a real, live `graph_id`-anchored `member_ocean_isolation` policy
//! (0009 §6(b)). That means IF a future change ever lifts one of this
//! cluster's `reject_org_*` gates and lets a connector-ingested row reach a
//! genuine tenant pool, getting the GUC right is exactly as consequential as
//! the runbook's Category B conversions elsewhere, and the DENY-not-leak
//! contract (§1.1) holds today for exactly the same structural reason it
//! holds everywhere else in this migration.
//!
//! Using the REAL [`coralswarm_backend::tenancy::scope::begin_account_scoped`]
//! and [`coralswarm_backend::tenancy::scope::begin_tenant_scoped`] primitives
//! (not hand-rolled `set_config` calls) against this real table:
//!
//! 1. Today's actual application shape for this table
//!    (`begin_account_scoped` only, no `app.user_id` — exactly what
//!    `routes::teams_ingest.rs::ingest_resource`/`install_resource` call)
//!    fails closed: a connector-ingested watch row for member A's own ocean
//!    is INVISIBLE to a connection scoped only by `account_id`. This is the
//!    live, executable version of "connector ingest degrades, it does not
//!    leak" for this table.
//! 2. The identical row, read through the SAME real tenant pool but via
//!    `begin_tenant_scoped` with the owning member's `user_id` (the
//!    conversion a future change would need to make), resolves correctly —
//!    proving the escape hatch is ready and correct, not merely assumed.
//! 3. A DIFFERENT member's `user_id` through `begin_tenant_scoped` sees
//!    nothing — the cross-member half of the same proof (visible to exactly
//!    the right principal, not merely "visible to somebody").
//! 4. Proof-of-failure: assertion (2) is shown to go RED when the exact same
//!    call is reverted to `begin_account_scoped` (assertion 1's shape) —
//!    then restored and reconfirmed GREEN — so this suite is proven to
//!    actually pin the `user_id` threading rather than pass for an unrelated
//!    reason.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB convention as every other
//! `*_pg.rs` suite in this tree:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test tenant_rls_connectors_pg

mod support;

use std::str::FromStr;

use coralswarm_backend::tenancy::scope::{begin_account_scoped, begin_tenant_scoped};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
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

/// Same base URL, authenticated as `coralswarm_tenant_app` — the literal DSN
/// shape `TenantRegistry` resolves in production. Same helper as
/// `tenant_rls_phase2_pg.rs`.
fn as_app_role_dsn(db_url: &str) -> String {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_tenant_app")
        .password(APP_ROLE_TEST_PASSWORD);
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

/// One tenant account, two members, each with their own primary member ocean,
/// and ONE `teams_watched_resources` row — the exact table/shape
/// `routes::teams_ingest.rs::ingest_resource`'s watch-insert produces —
/// anchored on member A's ocean. Mirrors `tenant_rls_phase2_pg.rs`'s `Fx`
/// fixture shape so the two suites read the same way.
struct Fx {
    account: Uuid,
    user_a: Uuid,
    user_b: Uuid,
    g_a: Uuid,
    watch_id: Uuid,
}

async fn seed(owner: &PgPool) -> Fx {
    let fx = Fx {
        account: Uuid::new_v4(),
        user_a: Uuid::new_v4(),
        user_b: Uuid::new_v4(),
        g_a: Uuid::new_v4(),
        watch_id: Uuid::new_v4(),
    };
    let g_b = Uuid::new_v4();

    let sql = format!(
        r#"
INSERT INTO graphs(id, account_id, name, kind, owner_user_id)
  VALUES ('{g_a}', '{account}', 'A private reef', 'member', '{user_a}');
INSERT INTO graphs(id, account_id, name, kind, owner_user_id)
  VALUES ('{g_b}', '{account}', 'B private reef', 'member', '{user_b}');

INSERT INTO teams_watched_resources
    (id, account_id, user_id, graph_id, kind, team_id, channel_id, chat_id, display_name)
  VALUES
    ('{watch_id}', '{account}', '{user_a}', '{g_a}', 'channel', 'T1', 'C1', '', '#general');
"#,
        g_a = fx.g_a,
        g_b = g_b,
        account = fx.account,
        user_a = fx.user_a,
        user_b = fx.user_b,
        watch_id = fx.watch_id,
    );
    owner
        .execute(sql.as_str())
        .await
        .expect("seed connectors-cluster fixture");

    fx
}

async fn setup(db: &str) -> (PgConnection, PgPool, PgPool, Fx) {
    let base = base_url().expect("caller already checked base_url().is_some()");
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
        .expect("apply tenant tree incl. 0009_tenant_rls");
    // Retry-safe against the cluster-wide `pg_authid` race (this suite's two
    // `#[tokio::test]` functions each build their own scratch DATABASE but
    // race the SAME shared-catalog row) — see
    // `support::tenant_role::set_app_role_password_with_retry`'s own doc.
    set_app_role_password_with_retry(&owner).await;

    let fx = seed(&owner).await;

    let app_pool: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&as_app_role_dsn(&db_url))
        .await
        .expect("connect as coralswarm_tenant_app");

    // Proof this is genuinely the low-privilege role, not an accidental owner
    // connection (would make every assertion below vacuous) — same sanity
    // check `tenant_rls_phase2_pg.rs` runs.
    let who: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&app_pool)
        .await
        .unwrap();
    assert_eq!(who, "coralswarm_tenant_app");
    let bypass: bool =
        sqlx::query_scalar("SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user")
            .fetch_one(&app_pool)
            .await
            .unwrap();
    assert!(!bypass, "test pool must connect as the NOBYPASSRLS role");

    (admin_conn, owner, app_pool, fx)
}

async fn teardown(mut admin_conn: PgConnection, owner: PgPool, app_pool: PgPool, db: &str) {
    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Count of `teams_watched_resources` rows visible for `graph_id`, read
/// inside `tx` — the exact shape `routes::teams_ingest.rs::list_watched`
/// queries by (`WHERE account_id = $1`, but the row is only reachable at all
/// if RLS's `member_ocean_isolation` policy — keyed on `graph_id`, not
/// `account_id` — lets it through).
async fn visible_watch_count(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    graph_id: Uuid,
) -> i64 {
    sqlx::query("SELECT count(*) AS n FROM teams_watched_resources WHERE graph_id = $1")
        .bind(graph_id)
        .fetch_one(&mut **tx)
        .await
        .unwrap()
        .get::<i64, _>("n")
}

/// **Assertion 1 — today's real shape fails closed.** `routes::teams_ingest.rs`
/// (and every other file in the 11-file cluster) opens its transactions with
/// `begin_account_scoped` alone — no `app.user_id` — because none of them
/// carries a genuine `TenancyDecision::Org` today (see this file's module
/// doc). If one of them ever DID reach this exact table through a real
/// tenant pool without being converted first, this is precisely what would
/// happen: the row is invisible, not leaked. Proves the DENY half of §1.1's
/// contract against the REAL policy, not a synthetic one.
#[tokio::test]
async fn account_scoped_only_denies_a_real_teams_watch_row_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping account_scoped_only_denies_a_real_teams_watch_row_pg: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_connectors_account_scope_denies";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
    let (admin_conn, owner, app_pool, fx) = setup(db).await;

    let mut tx = begin_account_scoped(&app_pool, fx.account)
        .await
        .expect("begin_account_scoped must still open a transaction (it is inert re: RLS today, not broken)");
    let n = visible_watch_count(&mut tx, fx.g_a).await;
    tx.rollback().await.ok();

    assert_eq!(
        n, 0,
        "account_id-only scoping must NOT see member A's watch row through the real \
         member_ocean_isolation policy on teams_watched_resources — an unset app.user_id \
         must deny, matching §1.1's DENY-not-leak contract"
    );

    teardown(admin_conn, owner, app_pool, db).await;
}

/// **Assertions 2 + 3, plus the proof-of-failure (requirement: prove each
/// assertion can fail).** Uses `begin_tenant_scoped` — the conversion a
/// future change to this cluster would need to make — against the SAME real
/// row, and proves:
///   (a) the owning member sees it,
///   (b) a different member does not,
///   (c) reverting to `begin_account_scoped` (today's real, unconverted
///       shape) turns assertion (a) red again — so this test is proven to
///       actually pin the `user_id` threading, not pass for an unrelated
///       reason — then the conversion is restored and reconfirmed green.
#[tokio::test]
async fn tenant_scoped_would_show_exactly_the_right_member_pg() {
    let Some(_) = base_url() else {
        eprintln!(
            "skipping tenant_scoped_would_show_exactly_the_right_member_pg: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_connectors_tenant_scope_correct";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
    let (admin_conn, owner, app_pool, fx) = setup(db).await;

    // (a) The owning member, via the correctly-converted primitive: visible.
    let mut tx = begin_tenant_scoped(&app_pool, fx.account, fx.user_a)
        .await
        .expect("begin_tenant_scoped must open a transaction");
    let n_owner = visible_watch_count(&mut tx, fx.g_a).await;
    tx.rollback().await.ok();
    assert_eq!(
        n_owner, 1,
        "the watch's own owning member must see it once begin_tenant_scoped threads their \
         user_id — the correct post-conversion behavior"
    );

    // (b) A different member, same real row: must NOT see it — proves
    // "visible to exactly the right principal," not merely "visible to
    // somebody once app.user_id is set to anything."
    let mut tx = begin_tenant_scoped(&app_pool, fx.account, fx.user_b)
        .await
        .expect("begin_tenant_scoped must open a transaction");
    let n_other = visible_watch_count(&mut tx, fx.g_a).await;
    tx.rollback().await.ok();
    assert_eq!(
        n_other, 0,
        "a DIFFERENT member must NOT see member A's watch row — the member-ocean boundary \
         must not leak cross-member even once a user_id is threaded"
    );

    // (c) Proof-of-failure: break the scoping (revert to today's real,
    // unconverted `begin_account_scoped` shape) and confirm assertion (a)
    // goes RED — i.e. this test does not pass for a reason unrelated to the
    // user_id threading.
    let mut tx = begin_account_scoped(&app_pool, fx.account)
        .await
        .expect("begin_account_scoped must open a transaction");
    let n_broken = visible_watch_count(&mut tx, fx.g_a).await;
    tx.rollback().await.ok();
    assert_eq!(
        n_broken, 0,
        "PROOF-OF-FAILURE: reverting to begin_account_scoped (no app.user_id) must turn the \
         owner-visibility assertion red (0, not 1) — confirming assertion (a) actually \
         depends on begin_tenant_scoped's user_id threading rather than passing vacuously"
    );

    // Restored: re-run (a) exactly as originally asserted, confirmed green.
    let mut tx = begin_tenant_scoped(&app_pool, fx.account, fx.user_a)
        .await
        .expect("begin_tenant_scoped must open a transaction");
    let n_restored = visible_watch_count(&mut tx, fx.g_a).await;
    tx.rollback().await.ok();
    assert_eq!(
        n_restored, 1,
        "restored begin_tenant_scoped must show the owning member their own watch row again \
         (green after the deliberate red above)"
    );

    teardown(admin_conn, owner, app_pool, db).await;
}
