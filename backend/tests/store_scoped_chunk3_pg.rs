//! Integration tests for the `_scoped` `Store` trait surface added for
//! chunk 3 (org-tenancy rollout, PR B.0 chunk 3): the six methods chunk 1
//! and chunk 2 deliberately deferred as too tangled for a mechanical port —
//! `merge_nodes`, `unmerge_nodes`, `batch_apply_remote_ops` (since
//! removed with the desktop sync relay), `link_node_atom` (SAVEPOINT), `promote_node` (single-phase redesign),
//! and `ocean_stats` (pool-concurrent, no caller-supplied transaction).
//!
//! Same rigor as chunks 1/2: drives `PostgresStore` directly as a real
//! `coralswarm_app` login. The `link_node_atom_scoped` test is the most
//! load-bearing one here: it proves the SAVEPOINT technique actually works
//! — a soft-skipped FK conflict must NOT poison the shared transaction, so
//! a FOLLOWING call in the SAME transaction must still succeed.
//!

use std::{collections::HashMap, str::FromStr, sync::Mutex};

use coralswarm_backend::services::blob_store::PromotionBlobStore;
use coralswarm_backend::store::{
    AtomBlobCopyCtx, BlobCopyMode, PostgresStore, Store,
};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::promote;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_core::r2::{blob_key_for, R2Error};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

const APP_ROLE_TEST_PASSWORD: &str = "orgt_store_scoped_chunk3_test_only_pw";

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
async fn create_database(conn: &mut PgConnection, db: &str) {
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

struct Seeded {
    user_id: Uuid,
    account_id: Uuid,
    graph_id: Uuid,
    team_graph_id: Uuid,
    keep_id: Uuid,
    dup_id: Uuid,
    atom_id: Uuid,
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
    // A second graph on the SAME account, used as promote_node_scoped's
    // target — a distinct graph avoids the `(graph_id, type, title)` upsert
    // key colliding with the source node in the same ocean.
    let team_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'team ocean', 'team') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert team graph");
    let keep_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title) VALUES ($1, 'entity', 'Keep Reef') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert keep node");
    let dup_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title) VALUES ($1, 'entity', 'Dup Reef') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert dup node");
    let atom_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, $2, 'test', 'probe-1', $3, now())",
    )
    .bind(atom_id)
    .bind(graph_id)
    .bind(format!("{graph_id}/{atom_id}"))
    .execute(owner)
    .await
    .expect("insert atom");
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
        .bind(dup_id)
        .bind(atom_id)
        .execute(owner)
        .await
        .expect("link atom to dup node");

    Seeded { user_id, account_id, graph_id, team_graph_id, keep_id, dup_id, atom_id }
}

/// Positive path: all six chunk-3 methods, correctly scoped to the account.
#[tokio::test]
async fn chunk3_scoped_methods_work_correctly_when_account_is_scoped() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping chunk3_scoped_methods_work_correctly_when_account_is_scoped: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = coralswarm_backend::test_db::TestDatabase::from_url(&base, "orgt_chunk3_rw");
    let mut admin_conn = admin(&base).await;
    create_database(&mut admin_conn, &db).await;
    let db_url = with_db(&base, &db);

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

    let a = seed_account(&owner, "chunk3-a@x.co").await;
    let b = seed_account(&owner, "chunk3-b@x.co").await;
    let source_before: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM atoms a WHERE atom_id=$1")
        .bind(a.atom_id).fetch_one(&owner).await.unwrap();

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());

    let mut tx = begin_account_scoped(&app_pool, a.account_id)
        .await
        .expect("begin_account_scoped for account A");

    // ── merge_nodes_scoped ────────────────────────────────────────────────
    let moved = store
        .merge_nodes_scoped(&mut tx, a.keep_id, a.dup_id)
        .await
        .expect("merge_nodes_scoped must succeed for A's own nodes");
    assert_eq!(moved, 1, "the one atom linked to dup must have moved to keep");

    let keep_atoms = store
        .node_atom_ids_scoped(&mut tx, a.keep_id)
        .await
        .expect("node_atom_ids_scoped must succeed");
    assert_eq!(keep_atoms, vec![a.atom_id]);

    let merge_id: Uuid = sqlx::query_scalar("SELECT merge_id FROM node_merges WHERE dup_id = $1")
        .bind(a.dup_id)
        .fetch_one(&mut *tx)
        .await
        .expect("look up the merge record inside A's scoped transaction");

    // ── unmerge_nodes_scoped ─────────────────────────────────────────────
    store
        .unmerge_nodes_scoped(&mut tx, merge_id)
        .await
        .expect("unmerge_nodes_scoped must succeed for A's own merge record");
    let dup_restored: Option<Uuid> = sqlx::query_scalar("SELECT node_id FROM nodes WHERE node_id = $1")
        .bind(a.dup_id)
        .fetch_optional(&mut *tx)
        .await
        .expect("look up restored dup node");
    assert_eq!(dup_restored, Some(a.dup_id), "unmerge_nodes_scoped must recreate the dup node");

    // ── link_node_atom_scoped: SAVEPOINT proof ──────────────────────────
    // First, a legitimate link succeeds.
    store
        .link_node_atom_scoped(&mut tx, a.keep_id, a.atom_id)
        .await
        .expect("link_node_atom_scoped must succeed linking A's own node/atom");

    // Then, an FK-conflict (a node_id that doesn't exist) must SOFT-SKIP —
    // return Ok(()), not an error — and critically must NOT poison the
    // shared transaction: the very next statement below must still work.
    let bogus_node_id = Uuid::new_v4();
    store
        .link_node_atom_scoped(&mut tx, bogus_node_id, a.atom_id)
        .await
        .expect(
            "link_node_atom_scoped must SOFT-SKIP an FK conflict (Ok, not Err) via its \
             SAVEPOINT, exactly like the unscoped twin",
        );

    // Proof the shared transaction survived: another read/write in the SAME
    // `tx` right after the soft-skip must still succeed (would fail with
    // \"current transaction is aborted\" if the SAVEPOINT technique didn't
    // actually contain the conflict).
    let still_alive = store
        .node_atom_ids_scoped(&mut tx, a.keep_id)
        .await
        .expect(
            "a read AFTER link_node_atom_scoped's soft-skip must still succeed — proves the \
             SAVEPOINT contained the conflict instead of poisoning the whole shared transaction",
        );
    assert_eq!(still_alive, vec![a.atom_id]);

    // ── promote_node_scoped ──────────────────────────────────────────────
    let source_node_before: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(n) FROM nodes n WHERE node_id=$1")
        .bind(a.keep_id).fetch_one(&mut *tx).await.unwrap();
    let promotion = store
        .promote_node_scoped(&mut tx, a.keep_id, a.team_graph_id, a.user_id, None)
        .await
        .expect("promote_node_scoped must succeed for A's own source node/target graph");
    assert!(promotion.created);
    assert_eq!(promotion.source_node_id, a.keep_id);
    assert_eq!(promotion.target_graph_id, a.team_graph_id);
    assert_ne!(promotion.target_node_id, a.keep_id);
    assert_eq!(promotion.corals_requested, 1, "the coral on keep_id must have been requested");
    let promote_job_id = promotion.job_id.expect("a non-empty promotion must enqueue a PromoteNode job");

    tx.commit().await.expect("commit A's scoped transaction");

    let worker_tenant = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: app_pool.clone(),
        store: std::sync::Arc::new(PostgresStore::new(app_pool.clone())) as std::sync::Arc<dyn Store>,
    };
    let queued: (String, Option<Uuid>, serde_json::Value) = sqlx::query_as(
        "SELECT kind, account_id, payload FROM jobs WHERE id=$1")
        .bind(promote_job_id).fetch_one(&app_pool).await.unwrap();
    assert_eq!(queued, ("PromoteNode".to_owned(), Some(a.account_id), serde_json::json!({
        "target_node_id": promotion.target_node_id, "target_graph_id": a.team_graph_id,
        "source_node_id": a.keep_id, "promoted_by": a.user_id, "atom_ids": [a.atom_id],
    })));
    let tick = promote::tick(&worker_tenant, None).await.expect("promote worker tick");
    assert_eq!(tick.processed, 1);
    let outcome: (String, Option<String>, String, i32, bool) = sqlx::query_as(
        "SELECT status::text, failure_class, CASE \
         WHEN error_message = 'selected evidence consent required' THEN 'selected_evidence_consent_required' \
         ELSE 'unexpected' END, attempts, finished_at IS NOT NULL FROM jobs WHERE id = $1",
    )
    .bind(promote_job_id)
    .fetch_one(&app_pool)
    .await
    .expect("promote job row must exist");
    assert_eq!(outcome, (
        "dead".to_owned(), Some("structural".to_owned()),
        "selected_evidence_consent_required".to_owned(), 1, true,
    ));
    let mut verify_tx = begin_account_scoped(&app_pool, a.account_id)
        .await.expect("begin_account_scoped to verify legacy refusal");
    let copied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
        .bind(a.team_graph_id).fetch_one(&mut *verify_tx).await.unwrap();
    assert_eq!(copied, 0, "refused legacy jobs must copy no atoms");
    let links: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM node_atoms na JOIN nodes n ON n.node_id=na.node_id WHERE n.graph_id = $1")
        .bind(a.team_graph_id).fetch_one(&mut *verify_tx).await.unwrap();
    assert_eq!(links, 0, "refused legacy jobs must link no atoms");
    let original: (Uuid, String) = sqlx::query_as("SELECT graph_id, content_blob_key FROM atoms WHERE atom_id=$1")
        .bind(a.atom_id).fetch_one(&mut *verify_tx).await.unwrap();
    assert_eq!(original, (a.graph_id, format!("{}/{}", a.graph_id, a.atom_id)));
    assert_eq!(store.node_atom_ids_scoped(&mut verify_tx, a.keep_id).await.unwrap(), vec![a.atom_id]);
    let destination: (Uuid, Option<Uuid>, Option<Uuid>, String) = sqlx::query_as(
        "SELECT graph_id, promoted_from_node_id, promoted_from_graph_id, title FROM nodes WHERE node_id=$1")
        .bind(promotion.target_node_id).fetch_one(&mut *verify_tx).await.unwrap();
    assert_eq!(destination, (a.team_graph_id, Some(a.keep_id), Some(a.graph_id), "Keep Reef".to_owned()));
    let bookkeeping: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM node_promotions WHERE source_node_id=$1 AND target_node_id=$2 AND promoted_by=$3")
        .bind(a.keep_id).bind(promotion.target_node_id).bind(a.user_id)
        .fetch_one(&mut *verify_tx).await.unwrap();
    assert_eq!(bookkeeping, 1, "Store records the destination before the worker refuses copying");
    let ops: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM op_log WHERE entity_id=$1 AND graph_id=$2")
        .bind(promotion.target_node_id.to_string()).bind(a.team_graph_id)
        .fetch_one(&mut *verify_tx).await.unwrap();
    assert_eq!(ops, 1, "Store emits the destination node op before worker refusal");
    let source_after: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM atoms a WHERE atom_id=$1")
        .bind(a.atom_id).fetch_one(&mut *verify_tx).await.unwrap();
    assert_eq!(source_after, source_before, "legacy refusal preserves the complete source atom");
    let source_node_after: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(n) FROM nodes n WHERE node_id=$1")
        .bind(a.keep_id).fetch_one(&mut *verify_tx).await.unwrap();
    assert_eq!(source_node_after, source_node_before, "legacy promotion leaves its source node unchanged");
    verify_tx.rollback().await.unwrap();

    // ── ocean_stats_scoped: manages its own scoping internally ──────────
    let stats = store
        .ocean_stats_scoped(a.account_id, a.user_id, a.graph_id)
        .await
        .expect("ocean_stats_scoped must succeed for A's own account/graph");
    assert_eq!(stats.atom_count, 1, "A's home graph has exactly one atom");
    assert!(
        stats.reef_counts.iter().any(|(t, n)| t == "entity" && *n >= 1),
        "A's home graph must show at least one entity reef — got {:?}",
        stats.reef_counts
    );

    // Cross-account isolation proof: A's account_id against B's graph_id
    // must show ZERO atoms, not B's real atom count — `ocean_stats_scoped`
    // manages its own per-query scoping, so this is the equivalent of the
    // IDOR-pivot proof the other chunks run against a shared transaction.
    let cross_stats = store
        .ocean_stats_scoped(a.account_id, a.user_id, b.graph_id)
        .await
        .expect("ocean_stats_scoped must not error on a cross-account graph_id");
    assert_eq!(
        cross_stats.atom_count, 0,
        "scoped to A, ocean_stats_scoped(B's graph_id) must show zero atoms, not B's real count"
    );

    app_pool.close().await;
    owner.close().await;
    admin_conn.close().await.unwrap();
}

/// Negative control: every tested chunk-3 method hard-errors (or, for
/// `ocean_stats_scoped`, correctly returns zeroed-out cross-account data
/// rather than erroring — it manages its own scoping so there is no
/// "unset" case to test the same way) when there's no valid scope for the
/// account in question.
#[tokio::test]
async fn chunk3_scoped_methods_hard_error_when_account_id_is_never_set() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping chunk3_scoped_methods_hard_error_when_account_id_is_never_set: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = coralswarm_backend::test_db::TestDatabase::from_url(&base, "orgt_chunk3_backstop");
    let mut admin_conn = admin(&base).await;
    create_database(&mut admin_conn, &db).await;
    let db_url = with_db(&base, &db);

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

    let a = seed_account(&owner, "chunk3-backstop-a@x.co").await;

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());

    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.merge_nodes_scoped(&mut tx, a.keep_id, a.dup_id).await;
        assert!(result.is_err(), "merge_nodes_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store.link_node_atom_scoped(&mut tx, a.keep_id, a.atom_id).await;
        assert!(result.is_err(), "link_node_atom_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin unscoped tx (app.account_id never set)");
        let result = store
            .promote_node_scoped(&mut tx, a.keep_id, a.team_graph_id, a.user_id, None)
            .await;
        assert!(result.is_err(), "promote_node_scoped must hard-error — got {result:?}");
        tx.rollback().await.ok();
    }
    app_pool.close().await;
    owner.close().await;
    admin_conn.close().await.unwrap();
}

#[derive(Default)]
struct BlobRecords {
    objects: HashMap<String, Vec<u8>>,
    reads: Vec<(String, [u8; 32])>,
    writes: Vec<(String, [u8; 32])>,
}

#[derive(Default)]
struct RecordingBlobs(Mutex<BlobRecords>);

#[async_trait::async_trait]
impl PromotionBlobStore for RecordingBlobs {
    async fn get_blob(&self, key: &str, dek: &[u8; 32]) -> Result<Vec<u8>, R2Error> {
        let mut records = self.0.lock().unwrap();
        records.reads.push((key.to_owned(), *dek));
        records.objects.get(key).cloned().ok_or_else(|| R2Error::NotFound(key.to_owned()))
    }
    async fn put_blob(&self, key: &str, bytes: &[u8], dek: &[u8; 32]) -> Result<(), R2Error> {
        let mut records = self.0.lock().unwrap();
        records.writes.push((key.to_owned(), *dek));
        records.objects.insert(key.to_owned(), bytes.to_vec());
        Ok(())
    }
    async fn delete_blob(&self, _key: &str) -> Result<(), R2Error> {
        panic!("scoped copying must not delete source or destination objects")
    }
}

#[tokio::test]
async fn scoped_copy_preserves_blob_bytes_provenance_and_account_isolation() {
    let Some(base) = base_url() else {
        eprintln!("skipping scoped_copy_preserves_blob_bytes_provenance_and_account_isolation: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = coralswarm_backend::test_db::TestDatabase::from_url(&base, "orgt_chunk3_copy");
    let mut admin_conn = admin(&base).await;
    create_database(&mut admin_conn, &db).await;
    let db_url = with_db(&base, &db);
    let owner = PgPoolOptions::new().connect(&db_url).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
    sqlx::query(&format!("ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"))
        .execute(&owner).await.unwrap();
    let a = seed_account(&owner, "chunk3-copy-a@x.co").await;
    let b = seed_account(&owner, "chunk3-copy-b@x.co").await;
    // Reach revision seven through real metadata edits; the identity trigger
    // advances from the stored revision instead of accepting a requested jump.
    for revision in 2_i64..=7 {
        let stored_revision: i64 = sqlx::query_scalar(
            "UPDATE atoms SET ingested_by=$2, timestamp=timestamp+interval '1 microsecond' \
             WHERE atom_id=$1 RETURNING content_revision")
            .bind(a.atom_id).bind(a.user_id).fetch_one(&owner).await.unwrap();
        assert_eq!(stored_revision, revision, "fixture revisions must be trigger-generated");
    }
    let source_before: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM atoms a WHERE atom_id=$1")
        .bind(a.atom_id).fetch_one(&owner).await.unwrap();
    let target_node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Scoped copy destination') RETURNING node_id")
        .bind(a.team_graph_id).fetch_one(&owner).await.unwrap();
    let app_pool = app_role_pool(&db_url).await;
    let role: (bool, bool) = sqlx::query_as("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname=current_user")
        .fetch_one(&app_pool).await.unwrap();
    assert_eq!(role, (false, false));
    let policies: Vec<(String, bool, bool)> = sqlx::query_as(
        "SELECT c.relname::text, c.relrowsecurity, EXISTS(SELECT 1 FROM pg_policy p WHERE p.polrelid=c.oid) \
         FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
         WHERE n.nspname='public' AND c.relname IN ('atoms','graphs','nodes','node_atoms') ORDER BY c.relname")
        .fetch_all(&app_pool).await.unwrap();
    assert_eq!(policies, ["atoms", "graphs", "node_atoms", "nodes"].into_iter()
        .map(|name| (name.to_owned(), true, true)).collect::<Vec<_>>());
    let store = PostgresStore::new(app_pool.clone());
    let blobs = RecordingBlobs::default();
    let source_key = format!("{}/{}", a.graph_id, a.atom_id);
    let bytes = b"synthetic scoped copy bytes";
    blobs.0.lock().unwrap().objects.insert(source_key.clone(), bytes.to_vec());
    let dek = [29; 32];
    let ctx = AtomBlobCopyCtx { blobs: &blobs, org_id: None, dek: &dek };
    let mut tx = begin_account_scoped(&app_pool, a.account_id).await.unwrap();
    let copies = store.copy_atoms_to_graph_scoped(&mut tx, &[a.atom_id], a.team_graph_id, BlobCopyMode::R2(&ctx))
        .await.expect("copy A's own source into A's target graph");
    assert_eq!(copies.len(), 1);
    let copied = copies[0];
    assert_ne!(copied, a.atom_id);
    let row: (Uuid, String, Option<Uuid>, Option<Uuid>, Option<i64>) = sqlx::query_as(
        "SELECT graph_id, content_blob_key, ingested_by, promoted_from_atom_id, promoted_from_revision FROM atoms WHERE atom_id=$1")
        .bind(copied).fetch_one(&mut *tx).await.unwrap();
    let target_key = blob_key_for(None, a.team_graph_id, copied).unwrap();
    assert_ne!(target_key, source_key);
    assert_eq!(row, (a.team_graph_id, target_key.clone(), Some(a.user_id), Some(a.atom_id), Some(7)));
    store.link_node_atom_scoped(&mut tx, target_node, copied).await.unwrap();
    let linked = store.node_atom_ids_scoped(&mut tx, target_node).await.unwrap();
    assert_eq!(linked, vec![copied]);
    let repeated = store.copy_atoms_to_graph_scoped(&mut tx, &[a.atom_id], a.team_graph_id, BlobCopyMode::R2(&ctx))
        .await.expect("repeat the same scoped copy");
    assert_eq!(repeated, copies);
    tx.commit().await.unwrap();
    {
        let records = blobs.0.lock().unwrap();
        assert_eq!(records.reads, vec![(source_key.clone(), dek)]);
        assert_eq!(records.writes, vec![(target_key.clone(), dek)]);
        assert_eq!(records.objects.get(&target_key).unwrap().as_slice(), bytes);
        assert_eq!(records.objects.get(&source_key).unwrap().as_slice(), bytes);
        assert_eq!(records.objects.len(), 2);
    }
    let mut unset = app_pool.begin().await.unwrap();
    assert!(store.copy_atoms_to_graph_scoped(&mut unset, &[a.atom_id], a.team_graph_id, BlobCopyMode::R2(&ctx))
        .await.is_err(), "copy requires an account scope");
    unset.rollback().await.unwrap();
    let mut foreign_source = begin_account_scoped(&app_pool, a.account_id).await.unwrap();
    let hidden = store.copy_atoms_to_graph_scoped(&mut foreign_source, &[b.atom_id], a.team_graph_id, BlobCopyMode::R2(&ctx))
        .await.expect("foreign source is invisible under account A");
    assert!(hidden.is_empty());
    foreign_source.rollback().await.unwrap();
    let mut foreign_target = begin_account_scoped(&app_pool, a.account_id).await.unwrap();
    assert!(store.copy_atoms_to_graph_scoped(&mut foreign_target, &[a.atom_id], b.team_graph_id, BlobCopyMode::R2(&ctx))
        .await.is_err(), "copy into B's target must be refused under A");
    foreign_target.rollback().await.unwrap();
    let mut visible = begin_account_scoped(&app_pool, a.account_id).await.unwrap();
    let own: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE atom_id=$1")
        .bind(copied).fetch_one(&mut *visible).await.unwrap();
    assert_eq!(own, 1);
    let source_after: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM atoms a WHERE atom_id=$1")
        .bind(a.atom_id).fetch_one(&mut *visible).await.unwrap();
    assert_eq!(source_after, source_before);
    visible.rollback().await.unwrap();
    let mut hidden = begin_account_scoped(&app_pool, b.account_id).await.unwrap();
    let foreign: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE atom_id=$1")
        .bind(copied).fetch_one(&mut *hidden).await.unwrap();
    assert_eq!(foreign, 0);
    let target_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id=$1")
        .bind(b.team_graph_id).fetch_one(&mut *hidden).await.unwrap();
    assert_eq!(target_count, 0);
    hidden.rollback().await.unwrap();
    {
        let records = blobs.0.lock().unwrap();
        assert_eq!(records.reads.len(), 1, "denied copies must not read blobs");
        assert_eq!(records.writes.len(), 1, "denied copies must not write blobs");
        assert_eq!(records.objects.len(), 2);
    }
    app_pool.close().await;
    owner.close().await;
    admin_conn.close().await.unwrap();
}
