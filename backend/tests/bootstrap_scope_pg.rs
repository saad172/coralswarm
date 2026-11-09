//! Integration tests for the RLS bootstrap primitives (org-tenancy Phase S,
//! migration `migrations_control/0084_bootstrap_scope_functions.sql` and its
//! paired `migrations_tenant/0003_bootstrap_scope_functions.sql`).
//!
//! These five `SECURITY DEFINER` functions exist to break a chicken-and-egg
//! problem: `tenancy::scope::begin_account_scoped` (`SET LOCAL
//! app.account_id`) has to run BEFORE any query against an RLS-protected
//! consumer-plane table, but several call sites need to DISCOVER an
//! account_id by querying one of those very tables first. This test proves
//! the load-bearing property directly: every function below returns the
//! correct result when called **as `coralswarm_app`** with `app.account_id`
//! **deliberately never set** for the whole test — the one deliberate,
//! narrow, audited exception to "every query needs a prior scope" in this
//! codebase. A contrasting negative control (an ordinary RLS-covered table
//! read on the SAME connection, SAME unset `app.account_id`) proves this
//! isn't just because RLS is inert in this test — RLS is very much active
//! and enforced here; these five functions are a deliberate carve-out from
//! it, not evidence it isn't running.
//!
//! Gated on `ORGT_TEST_BASE_URL`; same scratch-DB pattern as
//! `consumer_rls_pg.rs`.
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test bootstrap_scope_pg

use std::str::FromStr;

use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

const APP_ROLE_TEST_PASSWORD: &str = "orgt_bootstrap_scope_test_only_pw";

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

/// A pool connected AS `coralswarm_app` (real login, not `SET ROLE`).
async fn app_role_pool(db_url: &str) -> PgPool {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_app")
        .password(APP_ROLE_TEST_PASSWORD);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .expect("connect AS coralswarm_app")
}

/// One full (user, account, personal graph, atom, oauth_tokens, team graph)
/// fixture, seeded via the owner/BYPASSRLS pool. `slack_workspace` is a
/// distinct Slack `provider_workspace_id` for `resolve_oauth_account_id`;
/// `team_member` (if `Some`) is added as an ACTIVE member of the team graph,
/// so `resolve_reachable_graphs`'s team-gating branch has both an
/// included-member and excluded-non-member case across the two seeded
/// accounts in the actual test.
struct Seeded {
    user_id: Uuid,
    account_id: Uuid,
    personal_graph_id: Uuid,
    atom_id: Uuid,
    team_graph_id: Uuid,
}

async fn seed_account(owner: &PgPool, email: &str, slack_workspace: &str) -> Seeded {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(owner)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .expect("insert account");
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(owner)
        .await
        .expect("insert account_members");
    let personal_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary) \
         VALUES ($1, 'home', 'personal', true) RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert personal graph");
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', 'probe-1', 'blob-key', now()) RETURNING atom_id",
    )
    .bind(personal_graph_id)
    .fetch_one(owner)
    .await
    .expect("insert atom");
    sqlx::query(
        "INSERT INTO oauth_tokens(account_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, 'slack', 'chat:write', 'unused-ciphertext', $2)",
    )
    .bind(account_id)
    .bind(slack_workspace)
    .execute(owner)
    .await
    .expect("insert oauth_tokens");

    // A team graph under this account — used to prove `resolve_reachable_graphs`'
    // team-membership gating in the actual test (one account's user is an
    // active member, the other account's is not).
    let team_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'team ocean', 'team') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert team graph");
    sqlx::query(
        "INSERT INTO teams(account_id, graph_id, name, created_by) VALUES ($1, $2, 'eng', $3)",
    )
    .bind(account_id)
    .bind(team_graph_id)
    .bind(user_id)
    .execute(owner)
    .await
    .expect("insert team");

    Seeded { user_id, account_id, personal_graph_id, atom_id, team_graph_id }
}

#[tokio::test]
async fn bootstrap_functions_resolve_correctly_as_app_role_with_scope_never_set() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping bootstrap_functions_resolve_correctly_as_app_role_with_scope_never_set: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_bootstrap_scope";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083 + 0084");

    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    let a = seed_account(&owner, "bootstrap-a@x.co", "T-WORKSPACE-A").await;
    let b = seed_account(&owner, "bootstrap-b@x.co", "T-WORKSPACE-B").await;

    // Give the two seeded accounts opposite team-membership states on their
    // OWN team graph, so `resolve_reachable_graphs`'s team-gating branch
    // (`g.kind <> 'team' OR g.id IN (... active team_members ...)`) exercises
    // both outcomes: A departs (soft-leaves) A's own team — must be excluded
    // even though it's A's own account's graph — while B stays an active
    // member of B's own team — must be included.
    sqlx::query(
        "INSERT INTO team_members(team_id, user_id, role, departing_at) \
         SELECT id, $1, 'admin', now() FROM teams WHERE graph_id = $2",
    )
    .bind(a.user_id)
    .bind(a.team_graph_id)
    .execute(&owner)
    .await
    .expect("insert departed team_members row for A on A's own team");

    // B stays an ACTIVE member of B's own team graph (no departing_at).
    sqlx::query(
        "INSERT INTO team_members(team_id, user_id, role) \
         SELECT id, $1, 'admin' FROM teams WHERE graph_id = $2",
    )
    .bind(b.user_id)
    .bind(b.team_graph_id)
    .execute(&owner)
    .await
    .expect("insert active team_members row for B on B's own team");

    let app_pool = app_role_pool(&db_url).await;
    let mut conn = app_pool.acquire().await.expect("acquire app-role connection");

    // ── Negative control FIRST, on the SAME unscoped connection: an ordinary
    // RLS-covered table read must hard-error with `app.account_id` unset —
    // proving RLS is genuinely active here, not merely untested. Every
    // assertion below this point is the deliberate, narrow exception to it.
    {
        let result: Result<Vec<Uuid>, sqlx::Error> =
            sqlx::query_scalar("SELECT id FROM graphs").fetch_all(&mut *conn).await;
        assert!(
            result.is_err(),
            "sanity check: an ordinary RLS-covered table read with app.account_id unset must \
             error on this connection — got {result:?}"
        );
    }

    // ── 1. resolve_graph_account_id ─────────────────────────────────────────
    {
        let found: Option<Uuid> =
            sqlx::query_scalar("SELECT resolve_graph_account_id($1)")
                .bind(a.personal_graph_id)
                .fetch_one(&mut *conn)
                .await
                .expect("resolve_graph_account_id must succeed with app.account_id unset");
        assert_eq!(found, Some(a.account_id));

        let missing: Option<Uuid> = sqlx::query_scalar("SELECT resolve_graph_account_id($1)")
            .bind(Uuid::new_v4())
            .fetch_one(&mut *conn)
            .await
            .expect("resolve_graph_account_id on an unknown graph_id must still succeed");
        assert_eq!(missing, None, "an unknown graph_id must resolve to NULL, not error");
    }

    // ── 1b. resolve_graph_account_ids (migration
    // 0225_resolve_graph_account_ids, the batched sibling — PR D perf fix,
    // `GET /v1/library`) ─────────────────────────────────────────────────
    {
        let unknown = Uuid::new_v4();
        let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT * FROM resolve_graph_account_ids($1)",
        )
        .bind(vec![a.personal_graph_id, b.personal_graph_id, unknown])
        .fetch_all(&mut *conn)
        .await
        .expect("resolve_graph_account_ids must succeed with app.account_id unset");
        let mut got: Vec<(Uuid, Uuid)> = rows;
        got.sort();
        let mut expected = vec![
            (a.personal_graph_id, a.account_id),
            (b.personal_graph_id, b.account_id),
        ];
        expected.sort();
        assert_eq!(
            got, expected,
            "must return exactly the (graph_id, account_id) pairs that exist — one row per \
             known graph, spanning two different accounts in one call, and no row at all for \
             the unknown graph_id (not a NULL row)"
        );

        let empty: Vec<(Uuid, Uuid)> =
            sqlx::query_as("SELECT * FROM resolve_graph_account_ids($1)")
                .bind(Vec::<Uuid>::new())
                .fetch_all(&mut *conn)
                .await
                .expect("resolve_graph_account_ids must succeed on an empty input array");
        assert!(empty.is_empty());

        // Same property, through the Rust `Store::graph_account_ids` method
        // this SQL function backs — proves the end-to-end map assembly
        // (`routes/library.rs::query_library_consumer`'s actual call shape),
        // not just the raw SQL.
        let store = PostgresStore::new(app_pool.clone());
        let map = store
            .graph_account_ids(&[a.personal_graph_id, b.personal_graph_id, unknown])
            .await
            .expect("Store::graph_account_ids must succeed with app.account_id unset");
        assert_eq!(map.get(&a.personal_graph_id), Some(&a.account_id));
        assert_eq!(map.get(&b.personal_graph_id), Some(&b.account_id));
        assert_eq!(
            map.get(&unknown),
            None,
            "an unknown graph_id must be absent from the map, not present with a NULL/default"
        );
        assert_eq!(map.len(), 2);

        let empty_map = store
            .graph_account_ids(&[])
            .await
            .expect("Store::graph_account_ids must succeed on an empty slice (no round trip)");
        assert!(empty_map.is_empty());
    }

    // ── 2. resolve_atoms_account_id ─────────────────────────────────────────
    {
        let single: Vec<Uuid> = sqlx::query_scalar("SELECT * FROM resolve_atoms_account_id($1)")
            .bind(vec![a.atom_id])
            .fetch_all(&mut *conn)
            .await
            .expect("resolve_atoms_account_id must succeed with app.account_id unset");
        assert_eq!(single, vec![a.account_id]);

        // Atom ids spanning two accounts: the function itself just returns
        // the distinct set (the ambiguity CHECK is the Rust caller's job,
        // per `workers::embed::resolve_dek_for_atoms`) — proves it does not
        // silently collapse/filter to one account on its own.
        let mut spanning: Vec<Uuid> =
            sqlx::query_scalar("SELECT * FROM resolve_atoms_account_id($1)")
                .bind(vec![a.atom_id, b.atom_id])
                .fetch_all(&mut *conn)
                .await
                .expect("resolve_atoms_account_id must succeed across two accounts' atom_ids");
        spanning.sort();
        let mut expected = vec![a.account_id, b.account_id];
        expected.sort();
        assert_eq!(spanning, expected);
    }

    // ── 3. resolve_oauth_account_id ─────────────────────────────────────────
    {
        let found: Option<Uuid> =
            sqlx::query_scalar("SELECT resolve_oauth_account_id($1, $2)")
                .bind("slack")
                .bind("T-WORKSPACE-A")
                .fetch_one(&mut *conn)
                .await
                .expect("resolve_oauth_account_id must succeed with app.account_id unset");
        assert_eq!(found, Some(a.account_id));

        let missing: Option<Uuid> =
            sqlx::query_scalar("SELECT resolve_oauth_account_id($1, $2)")
                .bind("slack")
                .bind("T-UNKNOWN-WORKSPACE")
                .fetch_one(&mut *conn)
                .await
                .expect("resolve_oauth_account_id on an unknown workspace must still succeed");
        assert_eq!(missing, None, "an unknown workspace must resolve to NULL, not error");
    }

    // ── 4. resolve_reachable_graphs ──────────────────────────────────────────
    {
        let rows: Vec<(Uuid, String, String, bool, chrono::DateTime<chrono::Utc>)> =
            sqlx::query_as(
                "SELECT id, name, kind, is_primary, created_at FROM resolve_reachable_graphs($1)",
            )
            .bind(a.user_id)
            .fetch_all(&mut *conn)
            .await
            .expect("resolve_reachable_graphs must succeed with app.account_id unset");
        let ids: Vec<Uuid> = rows.iter().map(|(id, ..)| *id).collect();
        assert!(
            ids.contains(&a.personal_graph_id),
            "user A must see their own personal graph"
        );
        assert!(
            !ids.contains(&a.team_graph_id),
            "user A departed their own team (departing_at set) — must NOT see that team graph, \
             even though it belongs to A's own account"
        );
        assert!(
            !ids.contains(&b.personal_graph_id),
            "user A must never see account B's personal graph"
        );
        assert!(
            !ids.contains(&b.team_graph_id),
            "user A must never see account B's team graph"
        );

        let rows_b: Vec<(Uuid, String, String, bool, chrono::DateTime<chrono::Utc>)> =
            sqlx::query_as(
                "SELECT id, name, kind, is_primary, created_at FROM resolve_reachable_graphs($1)",
            )
            .bind(b.user_id)
            .fetch_all(&mut *conn)
            .await
            .expect("resolve_reachable_graphs must succeed for user B too");
        let ids_b: Vec<Uuid> = rows_b.iter().map(|(id, ..)| *id).collect();
        assert!(
            ids_b.contains(&b.team_graph_id),
            "user B is an ACTIVE member of B's own team graph — must see it"
        );
    }

    // ── 5. resolve_graph_reachable_by_user ──────────────────────────────────
    {
        let own: bool = sqlx::query_scalar("SELECT resolve_graph_reachable_by_user($1, $2)")
            .bind(a.personal_graph_id)
            .bind(a.user_id)
            .fetch_one(&mut *conn)
            .await
            .expect("resolve_graph_reachable_by_user must succeed with app.account_id unset");
        assert!(own, "user A must be able to reach their own personal graph");

        let cross: bool = sqlx::query_scalar("SELECT resolve_graph_reachable_by_user($1, $2)")
            .bind(b.personal_graph_id)
            .bind(a.user_id)
            .fetch_one(&mut *conn)
            .await
            .expect("resolve_graph_reachable_by_user must succeed even for a cross-account graph");
        assert!(!cross, "user A must NOT be able to reach account B's personal graph");

        let departed_team: bool =
            sqlx::query_scalar("SELECT resolve_graph_reachable_by_user($1, $2)")
                .bind(a.team_graph_id)
                .bind(a.user_id)
                .fetch_one(&mut *conn)
                .await
                .expect("resolve_graph_reachable_by_user must succeed for the departed-team case");
        assert!(
            !departed_team,
            "user A departed their own team — must NOT be reachable, even though it's A's own \
             account's graph"
        );
    }

    drop(conn);
    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// `resolve_connector_install_account_ids` (migration
/// `0121_connector_installs`) — the genuine multi-row replacement for
/// `resolve_oauth_account_id`'s arbitrary `LIMIT 1` when it comes to Slack.
/// Same bootstrap-carveout proof as the test above (called as `coralswarm_app`
/// with `app.account_id` NEVER set), plus the property that motivated this
/// migration in the first place: two DIFFERENT accounts installing the SAME
/// workspace must BOTH come back, deterministically ordered, not one
/// silently dropped.
#[tokio::test]
async fn connector_install_resolver_returns_every_matching_account() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping connector_install_resolver_returns_every_matching_account: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_bootstrap_scope_connector_installs";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0121_connector_installs");

    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    // Two accounts, both installing the SAME Slack workspace — the exact
    // "two customers, or a customer plus someone's personal account" case
    // `docs/plans/org-connectors-and-channel-ingest.md` §9c documents. A
    // third account installs a DIFFERENT workspace, to prove the resolver
    // filters rather than returning everything.
    async fn seed_minimal_account(owner: &PgPool, email: &str) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(owner)
            .await
            .expect("insert user");
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .expect("insert account")
    }
    async fn seed_install(
        owner: &PgPool,
        account_id: Uuid,
        provider: &str,
        workspace_id: &str,
        created_at_offset_secs: i64,
    ) {
        sqlx::query(
            "INSERT INTO connector_installs \
                (provider, external_workspace_id, account_id, created_at) \
             VALUES ($1, $2, $3, now() + make_interval(secs => $4))",
        )
        .bind(provider)
        .bind(workspace_id)
        .bind(account_id)
        .bind(created_at_offset_secs as f64)
        .execute(owner)
        .await
        .expect("insert connector_installs fixture row");
    }

    let account_first = seed_minimal_account(&owner, "conn-install-first@x.co").await;
    let account_second = seed_minimal_account(&owner, "conn-install-second@x.co").await;
    let account_other_workspace = seed_minimal_account(&owner, "conn-install-other@x.co").await;

    // `account_second` installed BEFORE `account_first` (earlier created_at),
    // so the ORDER BY created_at assertion below is genuinely proving
    // something, not passing by insertion-order coincidence.
    seed_install(&owner, account_second, "slack", "T-SHARED", -60).await;
    seed_install(&owner, account_first, "slack", "T-SHARED", 0).await;
    seed_install(&owner, account_other_workspace, "slack", "T-OTHER", 0).await;
    // A different PROVIDER on the exact same workspace string must not
    // leak into a "slack" lookup — proves the resolver filters on provider
    // too, not just workspace id.
    seed_install(&owner, account_other_workspace, "microsoft", "T-SHARED", 0).await;

    let app_pool = app_role_pool(&db_url).await;
    let mut conn = app_pool.acquire().await.expect("acquire app-role connection");

    // Negative control, same as the sibling test: an ordinary RLS-covered
    // table read must still hard-error on this unscoped connection.
    {
        let result: Result<Vec<Uuid>, sqlx::Error> =
            sqlx::query_scalar("SELECT id FROM graphs").fetch_all(&mut *conn).await;
        assert!(
            result.is_err(),
            "sanity check: RLS must still be enforced on this connection — got {result:?}"
        );
    }

    // ── Multi-row case: both accounts that installed T-SHARED come back,
    // earliest first. ─────────────────────────────────────────────────────
    {
        let found: Vec<Uuid> =
            sqlx::query_scalar("SELECT * FROM resolve_connector_install_account_ids($1, $2)")
                .bind("slack")
                .bind("T-SHARED")
                .fetch_all(&mut *conn)
                .await
                .expect("resolve_connector_install_account_ids must succeed with app.account_id unset");
        assert_eq!(
            found,
            vec![account_second, account_first],
            "both accounts that installed the SAME workspace must come back, ordered by \
             created_at (earliest install first) — the exact fan-out the old LIMIT-1 resolvers \
             could never express"
        );
        assert!(
            !found.contains(&account_other_workspace),
            "an account that installed a DIFFERENT workspace must not appear"
        );
    }

    // ── No match -> empty Vec, not an error ─────────────────────────────────
    {
        let missing: Vec<Uuid> =
            sqlx::query_scalar("SELECT * FROM resolve_connector_install_account_ids($1, $2)")
                .bind("slack")
                .bind("T-UNKNOWN-WORKSPACE")
                .fetch_all(&mut *conn)
                .await
                .expect("resolve_connector_install_account_ids on an unknown workspace must \
                         still succeed");
        assert!(missing.is_empty(), "an unknown workspace must resolve to an empty set, not error");
    }

    // ── Provider filter: the same workspace STRING under a different
    // provider must not appear in a "slack" lookup. ────────────────────────
    {
        let slack_only: Vec<Uuid> =
            sqlx::query_scalar("SELECT * FROM resolve_connector_install_account_ids($1, $2)")
                .bind("slack")
                .bind("T-SHARED")
                .fetch_all(&mut *conn)
                .await
                .expect("resolve_connector_install_account_ids must succeed");
        assert!(
            !slack_only.contains(&account_other_workspace),
            "a microsoft install of the same workspace STRING must not leak into a slack lookup"
        );
    }

    drop(conn);
    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Proves the two paired functions on `migrations_tenant`
/// (`resolve_graph_account_id`, `resolve_atoms_account_id`) exist and behave
/// identically to their control-tree counterparts — required for
/// `workers::embed::resolve_dek_for_atoms` and `Store::graph_account_id` to
/// work unmodified against an org tenant's own database (no `coralswarm_app`/
/// RLS model exists on this plane at all; whole-database isolation covers it
/// instead, so this test simply calls the functions against the tenant
/// tree's normal owner connection).
#[tokio::test]
async fn tenant_tree_paired_functions_resolve_correctly() {
    let Some(base) = base_url() else {
        eprintln!("skipping tenant_tree_paired_functions_resolve_correctly: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_bootstrap_scope_tenant";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let tenant: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect tenant");
    sqlx::migrate!("./migrations_tenant")
        .run(&tenant)
        .await
        .expect("apply tenant tree incl. 0003_bootstrap_scope_functions");

    // Tenant DB's account_id column is a bare uuid with no cross-DB FK (see
    // 0001_tenant_baseline's header), so a fresh random account_id needs no
    // users/accounts fixture rows here.
    let account_id = Uuid::new_v4();
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name) VALUES ($1, 'tenant home') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(&tenant)
    .await
    .expect("insert tenant graph");
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', 'probe-1', 'blob-key', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .fetch_one(&tenant)
    .await
    .expect("insert tenant atom");

    let found: Option<Uuid> = sqlx::query_scalar("SELECT resolve_graph_account_id($1)")
        .bind(graph_id)
        .fetch_one(&tenant)
        .await
        .expect("resolve_graph_account_id must exist and work on the tenant tree");
    assert_eq!(found, Some(account_id));

    let atoms: Vec<Uuid> = sqlx::query_scalar("SELECT * FROM resolve_atoms_account_id($1)")
        .bind(vec![atom_id])
        .fetch_all(&tenant)
        .await
        .expect("resolve_atoms_account_id must exist and work on the tenant tree");
    assert_eq!(atoms, vec![account_id]);

    tenant.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}
