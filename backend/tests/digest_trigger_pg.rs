//! The reef digest trigger's counters and cadence (graph-temporal-memory §2.3),
//! with RegenerateDigest admitted on `managed_enrichment`.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights).
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:55987/postgres \
//!     cargo test -p coralswarm-backend --test digest_trigger_pg -- --nocapture

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
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
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&with_db(base, db))
        .await
        .expect("connect control");
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .unwrap();
    pool
}

// ── Fixture plumbing ──────────────────────────────────────────────────────────

async fn seed_graph(pool: &PgPool) -> (Uuid, Uuid, Uuid) {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("digest-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .expect("insert user");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, plan_tier) VALUES ($1, 'free') RETURNING id",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
    .expect("insert account");
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name) VALUES ($1, 'test') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .expect("insert graph");
    (user_id, account_id, graph_id)
}

async fn insert_node(pool: &PgPool, graph_id: Uuid, node_type: &str, title: &str) -> Uuid {
    let body = if node_type == "decision" {
        serde_json::json!({"decided_at": "2026-01-01T00:00:00Z"})
    } else {
        serde_json::json!({})
    };
    sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title, body) VALUES ($1, $2, $3, $4) RETURNING node_id",
    )
    .bind(graph_id)
    .bind(node_type)
    .bind(title)
    .bind(body)
    .fetch_one(pool)
    .await
    .expect("insert node")
}

/// Insert `n` atoms and link each to `node_id` via `node_atoms` — the ONE
/// statement shape the counter trigger fires on. Returns the inserted atom
/// ids (so a caller can later delete a subset to prove the decrement side).
async fn link_n_atoms(
    pool: &PgPool,
    graph_id: Uuid,
    node_id: Uuid,
    n: usize,
    source_prefix: &str,
) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'slack', $2, $3, now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("{source_prefix}-{i}"))
        .bind(format!("blob-{source_prefix}-{i}"))
        .fetch_one(pool)
        .await
        .expect("insert atom");
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(pool)
            .await
            .expect("link node_atoms");
        ids.push(atom_id);
    }
    ids
}

async fn counters(pool: &PgPool, node_id: Uuid) -> (i32, i32) {
    let row = sqlx::query("SELECT coral_count, digest_coral_count FROM nodes WHERE node_id = $1")
        .bind(node_id)
        .fetch_one(pool)
        .await
        .expect("read counters");
    (row.get("coral_count"), row.get("digest_coral_count"))
}

async fn pending_digest_job_count(pool: &PgPool, node_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE idempotency_key = $1 AND status = 'pending'",
    )
    .bind(format!("digest:{node_id}"))
    .fetch_one(pool)
    .await
    .expect("count pending jobs")
}

/// Every digest job for this reef: `(pending, total)`. The kind is admitted
/// on `managed_enrichment` (tenant 0171 / control 0338), so the coral-count
/// trigger mints pending rows, never dead ones, and its
/// `ON CONFLICT (idempotency_key) WHERE status='pending'` keeps at most ONE
/// pending pass per reef however many corals arrive before it runs.
async fn digest_jobs(pool: &PgPool, graph_id: Uuid, node_id: Uuid) -> (usize, usize) {
    let key = format!("digest:{node_id}");
    let rows = sqlx::query(
        "SELECT kind, payload, idempotency_key, status::text AS status, error_message FROM jobs WHERE idempotency_key = $1",
    )
    .bind(&key)
    .fetch_all(pool)
    .await
    .expect("read digest jobs");
    let mut pending = 0;
    for row in &rows {
        assert_eq!(row.get::<String, _>("kind"), "RegenerateDigest");
        assert_eq!(row.get::<String, _>("idempotency_key"), key);
        assert_eq!(
            row.get::<serde_json::Value, _>("payload"),
            serde_json::json!({"node_id": node_id, "graph_id": graph_id})
        );
        let status = row.get::<String, _>("status");
        assert_ne!(status, "dead", "an admitted digest is never dead-lettered at enqueue");
        assert_eq!(row.get::<Option<String>, _>("error_message"), None);
        if status == "pending" {
            pending += 1;
        }
    }
    assert!(pending <= 1, "at most one pending pass per reef");
    assert_eq!(pending as i64, pending_digest_job_count(pool, node_id).await);
    (pending, rows.len())
}

/// What the worker does after a pass: the pending row completes and the
/// reef's counter is stamped.
async fn complete_pass(pool: &PgPool, node_id: Uuid) {
    sqlx::query("UPDATE jobs SET status='done', finished_at=now() WHERE idempotency_key=$1 AND status='pending'")
        .bind(format!("digest:{node_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE nodes SET digest_coral_count = coral_count WHERE node_id = $1")
        .bind(node_id)
        .execute(pool)
        .await
        .unwrap();
}

// ── Counter maintenance: link increments, cascade-delete decrements ─────────

#[tokio::test]
async fn coral_count_tracks_link_and_cascade_delete() {
    let Some(base) = base_url() else {
        eprintln!("skipping coral_count_tracks_link_and_cascade_delete: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_digest1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let (_, _, graph_id) = seed_graph(&pool).await;
    let node_id = insert_node(&pool, graph_id, "entity", "Chris Marchetti").await;

    let (before_count, before_digest) = counters(&pool, node_id).await;
    assert_eq!(
        (before_count, before_digest),
        (0, 0),
        "a fresh node starts at zero"
    );

    let atom_ids = link_n_atoms(&pool, graph_id, node_id, 5, "link").await;
    let (after_link, _) = counters(&pool, node_id).await;
    assert_eq!(
        after_link, 5,
        "coral_count must track ordinary INSERT INTO node_atoms"
    );

    // Cascade path: DELETE FROM atoms (not node_atoms) — proves the trigger
    // catches the FK-cascaded deletion, the exact class of writer this
    // migration's header calls out as missed by a plain grep for
    // "DELETE FROM node_atoms".
    sqlx::query("DELETE FROM atoms WHERE atom_id = $1")
        .bind(atom_ids[0])
        .execute(&pool)
        .await
        .expect("delete atom (cascades node_atoms)");
    let (after_delete, _) = counters(&pool, node_id).await;
    assert_eq!(
        after_delete, 4,
        "coral_count must track a CASCADE DELETE on node_atoms too"
    );
}

// ── Eligibility gate: type = 'entity' AND coral_count >= 8 ───────────────────

#[tokio::test]
async fn decision_reef_never_enqueues_a_digest_regardless_of_size() {
    let Some(base) = base_url() else {
        eprintln!("skipping decision_reef_never_enqueues_a_digest_regardless_of_size: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_digest2_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let (_, _, graph_id) = seed_graph(&pool).await;
    let node_id = insert_node(&pool, graph_id, "decision", "We decided X").await;
    link_n_atoms(&pool, graph_id, node_id, 20, "dec").await;
    sqlx::query("UPDATE nodes SET title = 'We decided X (renamed)' WHERE node_id = $1")
        .bind(node_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (0, 0));
}

#[tokio::test]
async fn small_entity_threshold_enqueues_one_admitted_pass_at_eight() {
    let Some(base) = base_url() else {
        eprintln!("skipping small_entity_threshold_enqueues_one_admitted_pass_at_eight: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_digest3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let (_, _, graph_id) = seed_graph(&pool).await;
    let node_id = insert_node(&pool, graph_id, "entity", "Small Reef").await;
    link_n_atoms(&pool, graph_id, node_id, 7, "sm").await;

    assert_eq!(counters(&pool, node_id).await, (7, 0));
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (0, 0), "coral_count=7 < 8 must not enqueue");

    link_n_atoms(&pool, graph_id, node_id, 1, "sm-cross").await;
    assert_eq!(counters(&pool, node_id).await, (8, 0));
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (1, 1), "crossing 8 admits one pending pass");
}

/// The stampede guard: a burst of corals (the node_atoms bridge, a backfill)
/// mints ONE pending pass per reef, and the growth cadence re-arms only after
/// the worker has run.
#[tokio::test]
async fn growth_cadence_is_deduped_to_one_pending_pass_per_reef() {
    let Some(base) = base_url() else {
        eprintln!("skipping growth_cadence_is_deduped_to_one_pending_pass_per_reef: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_digest4_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let (_, _, graph_id) = seed_graph(&pool).await;
    let node_id = insert_node(&pool, graph_id, "entity", "Growth Reef").await;

    link_n_atoms(&pool, graph_id, node_id, 10, "g").await;
    assert_eq!(counters(&pool, node_id).await, (10, 0));
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (1, 1), "three eligible changes, one pending pass");

    link_n_atoms(&pool, graph_id, node_id, 30, "burst").await;
    assert_eq!(counters(&pool, node_id).await, (40, 0));
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (1, 1), "a burst of 30 corals still holds one pending pass");

    complete_pass(&pool, node_id).await;
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (0, 1));

    link_n_atoms(&pool, graph_id, node_id, 7, "g3").await;
    assert_eq!(counters(&pool, node_id).await, (47, 40));
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (0, 1), "growth 7 < ceil(40 * 0.2) = 8 does not re-arm");

    link_n_atoms(&pool, graph_id, node_id, 1, "g4").await;
    assert_eq!(counters(&pool, node_id).await, (48, 40));
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (1, 2), "growth 8 re-arms one pass");
}

#[tokio::test]
async fn rename_with_no_growth_re_arms_one_pass() {
    let Some(base) = base_url() else {
        eprintln!("skipping rename_with_no_growth_re_arms_one_pass: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_digest5_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let (_, _, graph_id) = seed_graph(&pool).await;
    let node_id = insert_node(&pool, graph_id, "entity", "Rename Reef").await;
    link_n_atoms(&pool, graph_id, node_id, 10, "r").await;
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (1, 1));
    complete_pass(&pool, node_id).await;
    assert_eq!(counters(&pool, node_id).await, (10, 10));

    sqlx::query("UPDATE nodes SET title = 'Rename Reef (renamed)' WHERE node_id = $1")
        .bind(node_id)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(counters(&pool, node_id).await, (10, 10));
    assert_eq!(digest_jobs(&pool, graph_id, node_id).await, (1, 2), "a rename re-arms one pass");
}
