//! Integration tests for the `_scoped` `Store` trait surface added for the
//! `atoms`/`nodes`/`graphs` table cluster (org-tenancy rollout, PR B.0
//! chunk 1). See `store::mod::Store`'s "RLS-scoped transaction surface"
//! doc comment for the full design and the list of methods deliberately
//! left out of this first chunk.
//!
//! Nothing calls these `_scoped` methods from a route/worker yet — this
//! file is the only thing exercising them. It proves, driving
//! `PostgresStore` directly as a REAL `coralswarm_app` login (not `SET
//! ROLE` inside the owner's session — the same connection path OPS-4 will
//! eventually point the app at):
//!
//! 1. **Positive path**: called inside a [`begin_account_scoped`]
//!    transaction, each tested `_scoped` method reads/writes correctly
//!    scoped to that transaction's account — including the classic
//!    graph_id-mediated IDOR pivot (`atoms`/`nodes` are read/written
//!    directly by `graph_id`/`node_id`, never joining `graphs`, exactly
//!    the shape `consumer_rls_pg.rs` already proves for the ORDINARY
//!    (non-`_scoped`) surface) and a write-side (`WITH CHECK`) proof that
//!    a cross-account write is rejected even when the caller passes the
//!    wrong account/graph as a plain argument.
//! 2. **Negative control**: called on a `coralswarm_app` transaction with
//!    `app.account_id` never set, every tested `_scoped` method — reads
//!    AND writes alike — hard-errors. This is the RLS backstop's whole
//!    point: proves the new transaction-accepting surface routes through
//!    the exact same policies as the ordinary surface, not around them.
//!
//! Follows the `consumer_rls_pg.rs` / `bootstrap_scope_pg.rs` scratch-DB
//! pattern. Gated on `ORGT_TEST_BASE_URL`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test store_scoped_atoms_nodes_graphs_pg

use std::str::FromStr;

use chrono::Utc;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

const APP_ROLE_TEST_PASSWORD: &str = "orgt_store_scoped_test_only_pw";

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

/// One (user, account, personal graph, atom, node) fixture, seeded via the
/// owner/BYPASSRLS pool. The node is linked to the atom via `node_atoms` so
/// `get_node_scoped`'s `atom_count` aggregate has something to count.
struct Seeded {
    user_id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
    atom_id: Uuid,
    node_id: Uuid,
}

async fn seed_account(owner: &PgPool, email: &str) -> Seeded {
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
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary) \
         VALUES ($1, 'home', 'personal', true) RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert graph");
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', 'probe-1', 'blob-key', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert atom");
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title) VALUES ($1, 'entity', 'Probe Reef') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert node");
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(owner)
        .await
        .expect("insert node_atoms link");

    Seeded { user_id, account_id, graph_id, atom_id, node_id }
}

/// Positive path: reads and writes through the `_scoped` surface, inside a
/// [`begin_account_scoped`] transaction, are correctly scoped to that
/// transaction's account — for `graphs` (direct `account_id` column),
/// `atoms`/`nodes` (`graph_id`-mediated, the classic IDOR-pivot shape), on
/// both the read and write side, plus a `WITH CHECK` proof that a
/// cross-account write is rejected even when the caller passes the wrong
/// account/graph as a plain argument.
#[tokio::test]
async fn scoped_methods_read_and_write_correctly_when_account_is_scoped() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping scoped_methods_read_and_write_correctly_when_account_is_scoped: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_store_scoped_rw";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083_consumer_rls");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    let a = seed_account(&owner, "store-scoped-a@x.co").await;
    let b = seed_account(&owner, "store-scoped-b@x.co").await;

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());

    let mut tx = begin_account_scoped(&app_pool, a.account_id)
        .await
        .expect("begin_account_scoped for account A");

    // ── graphs: direct account_id column ────────────────────────────────
    let graphs = store
        .list_graphs_scoped(&mut tx, a.account_id)
        .await
        .expect("list_graphs_scoped must succeed when scoped");
    assert_eq!(
        graphs.iter().map(|g| g.id).collect::<Vec<_>>(),
        vec![a.graph_id],
        "list_graphs_scoped must see exactly A's own graph, never B's"
    );

    let own_graph = store
        .get_graph_scoped(&mut tx, a.graph_id, a.account_id)
        .await
        .expect("get_graph_scoped must succeed for A's own graph");
    assert!(own_graph.is_some(), "A must be able to read its own graph");

    // ── atoms: graph_id-mediated (IDOR pivot) — list_atoms_scoped never
    // mentions account_id at all, so this is a genuine RLS proof, not just
    // an app-level WHERE clause repeating the check.
    let atoms_own = store
        .list_atoms_scoped(&mut tx, a.graph_id)
        .await
        .expect("list_atoms_scoped must succeed for A's own graph_id");
    assert_eq!(
        atoms_own.iter().map(|at| at.atom_id).collect::<Vec<_>>(),
        vec![a.atom_id],
        "list_atoms_scoped(A's graph_id) must return A's atom"
    );

    let atoms_cross = store
        .list_atoms_scoped(&mut tx, b.graph_id)
        .await
        .expect("list_atoms_scoped must not ERROR on a cross-account graph_id — RLS just \
                 filters it to empty, since the query itself has no per-row auth check");
    assert!(
        atoms_cross.is_empty(),
        "scoped to A, list_atoms_scoped(B's graph_id) must return ZERO rows — got {atoms_cross:?}"
    );

    // ── nodes: graph_id-mediated too, and get_node_scoped filters ONLY by
    // node_id (no account_id, no graph_id argument at all) — the tightest
    // possible IDOR-pivot proof for this cluster.
    let node_own = store
        .get_node_scoped(&mut tx, a.node_id)
        .await
        .expect("get_node_scoped must succeed for A's own node");
    assert!(node_own.is_some(), "A must be able to read its own node by node_id");
    assert_eq!(node_own.unwrap().atom_count, 1, "the seeded node_atoms link must be visible");

    let node_cross = store
        .get_node_scoped(&mut tx, b.node_id)
        .await
        .expect("get_node_scoped must not ERROR reading B's node_id while scoped to A");
    assert!(
        node_cross.is_none(),
        "scoped to A, get_node_scoped(B's node_id) must return None — got {node_cross:?}"
    );

    // ── writes: same-account succeeds ───────────────────────────────────
    let new_graph = store
        .create_graph_scoped(&mut tx, a.account_id, "scoped-write-ocean", "personal")
        .await
        .expect("create_graph_scoped must succeed writing to A's own account_id");
    assert_eq!(new_graph.name, "scoped-write-ocean");

    let new_atom_ids = store
        .insert_atoms_scoped(
            &mut tx,
            &[Uuid::new_v4()],
            a.graph_id,
            "test",
            &["probe-2".to_string()],
            &["blob-key-2".to_string()],
            &[],
            Utc::now(),
            a.user_id,
            "private",
            "en",
            &[None],
            &[None],
            &[None],
            &[None],
            None,
        )
        .await
        .expect("insert_atoms_scoped must succeed writing to A's own graph_id");
    assert_eq!(new_atom_ids.len(), 1, "insert_atoms_scoped must report the inserted atom");

    let new_node_id = store
        .upsert_node_scoped(&mut tx, a.graph_id, "entity", "Scoped Write Reef")
        .await
        .expect("upsert_node_scoped must succeed writing to A's own graph_id");

    tx.commit().await.expect("commit A's scoped transaction");

    // ── write-side (WITH CHECK) proof: A's scoped transaction attempting to
    // write a row claiming B's graph_id must be REJECTED by the SAME RLS
    // policy that gates reads — not merely filtered out of a SELECT, but a
    // hard write failure — even though the argument itself is just a plain
    // Uuid the caller could get wrong. This runs in its OWN fresh
    // transaction (not the one just committed above): a real Postgres error
    // aborts whatever transaction it happens in, so proving it must NOT
    // poison unrelated prior work by keeping it isolated to its own tx,
    // rolled back afterward rather than committed.
    {
        let mut cross_tx = begin_account_scoped(&app_pool, a.account_id)
            .await
            .expect("begin_account_scoped for the cross-account write probe");
        let cross_write =
            store.upsert_node_scoped(&mut cross_tx, b.graph_id, "entity", "Should Not Land").await;
        assert!(
            cross_write.is_err(),
            "scoped to A, upsert_node_scoped(B's graph_id, ...) must be REJECTED by the nodes \
             table's WITH CHECK — got {cross_write:?}"
        );
        cross_tx.rollback().await.ok();
    }

    // ── verify persistence via the owner/BYPASSRLS pool: the successful
    // writes landed, scoped correctly; the rejected cross-account write did
    // NOT land at all (not even a partial/garbage row).
    let node_owner: Uuid =
        sqlx::query_scalar("SELECT graph_id FROM nodes WHERE node_id = $1")
            .bind(new_node_id)
            .fetch_one(&owner)
            .await
            .expect("the successful upsert_node_scoped write must be visible via the owner pool");
    assert_eq!(node_owner, a.graph_id);

    let rejected_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM nodes WHERE title = 'Should Not Land'")
            .fetch_one(&owner)
            .await
            .expect("count query as owner");
    assert_eq!(
        rejected_count, 0,
        "the WITH CHECK-rejected cross-account node write must not exist at all"
    );

    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Negative control: every tested `_scoped` method — reads AND writes
/// alike — hard-errors on a `coralswarm_app` transaction where
/// `app.account_id` was NEVER set (i.e. NOT opened via
/// [`begin_account_scoped`]). Proves the `_scoped` surface routes through
/// the exact same strict, non-`missing_ok` RLS predicate as the ordinary
/// (non-`_scoped`) surface `consumer_rls_pg.rs` already covers — i.e. that
/// adding a transaction-accepting parameter didn't accidentally create a
/// path around the backstop.
///
/// Each method gets its OWN fresh transaction (mirroring
/// `consumer_rls_pg.rs`'s pattern): a real Postgres error aborts the whole
/// transaction, so reusing one `tx` across multiple assertions here would
/// only prove the SECOND call errors (trivially true once the tx is already
/// aborted), not that each method independently hits the RLS backstop.
#[tokio::test]
async fn scoped_methods_hard_error_when_account_id_is_never_set() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping scoped_methods_hard_error_when_account_id_is_never_set: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_store_scoped_backstop";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083_consumer_rls");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    let a = seed_account(&owner, "store-scoped-backstop-a@x.co").await;

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());

    // ── reads ────────────────────────────────────────────────────────────
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.list_graphs_scoped(&mut tx, a.account_id).await;
        assert!(
            result.is_err(),
            "list_graphs_scoped with app.account_id never set must hard-error — got {result:?}"
        );
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.list_atoms_scoped(&mut tx, a.graph_id).await;
        assert!(
            result.is_err(),
            "list_atoms_scoped with app.account_id never set must hard-error — got {result:?}"
        );
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.get_node_scoped(&mut tx, a.node_id).await;
        assert!(
            result.is_err(),
            "get_node_scoped with app.account_id never set must hard-error — got {result:?}"
        );
        tx.rollback().await.ok();
    }

    // ── writes ───────────────────────────────────────────────────────────
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store
            .create_graph_scoped(&mut tx, a.account_id, "should-not-be-writable", "personal")
            .await;
        assert!(
            result.is_err(),
            "create_graph_scoped with app.account_id never set must hard-error — got {result:?}"
        );
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store
            .insert_atoms_scoped(
                &mut tx,
                &[Uuid::new_v4()],
                a.graph_id,
                "test",
                &["should-not-land".to_string()],
                &["blob-key".to_string()],
                &[],
                Utc::now(),
                a.user_id,
                "private",
                "en",
                &[None],
                &[None],
                &[None],
                &[None],
                None,
            )
            .await;
        assert!(
            result.is_err(),
            "insert_atoms_scoped with app.account_id never set must hard-error — got {result:?}"
        );
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store
            .upsert_node_scoped(&mut tx, a.graph_id, "entity", "Should Not Be Writable")
            .await;
        assert!(
            result.is_err(),
            "upsert_node_scoped with app.account_id never set must hard-error — got {result:?}"
        );
        tx.rollback().await.ok();
    }

    // Confirm none of the rejected writes landed anyway (belt-and-suspenders
    // — every assertion above already rolled its own transaction back, but
    // this proves it via the owner/BYPASSRLS pool directly).
    let leaked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM nodes WHERE title = 'Should Not Be Writable'",
    )
    .fetch_one(&owner)
    .await
    .expect("count query as owner");
    assert_eq!(leaked, 0, "no unscoped write must ever land, even after rollback");

    app_pool.close().await;
    owner.close().await;
    admin_conn
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}
