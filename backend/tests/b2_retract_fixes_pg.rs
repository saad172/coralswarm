//! Live tests for B2 (`docs/plans/capture/02-b2-retract-fixes.md`, unit 02)
//! against a REAL Postgres — FM-1, FM-2, FM-6, and FM-5's three own probes.
//! Each test is the unit file's own negative control, inverted: the probe
//! the source ran to PROVE the failure, run here to prove the fix.
//!
//! **Single-DSN by design (finding 7, independent review): `ORGT_TEST_BASE_URL`
//! only.** An earlier revision gated most tests on `RELINFER_TEST_DATABASE_URL`
//! (a PRE-migrated external DB) and only FM-5 probe (c) on
//! `ORGT_TEST_BASE_URL` (a CREATEDB-capable connection this suite migrates
//! itself) — two DSN conventions meant this suite could not be wired into
//! ANY single existing CI job, and shipped registered as `local-only`
//! (`scripts/pg_suite_coverage.manifest`), so NONE of these seven proof
//! tests ran in CI at all. Fixed by standardizing on ONE convention:
//! [`shared_scratch_pool`] below creates/migrates ONE throwaway database,
//! ONCE per test-binary run (this file already runs `--test-threads=1`,
//! same convention as every other scratch-db suite in this repo, so there
//! is no concurrent-creation race), for the six tests that only need a
//! migrated control tree; probe (c) still creates its OWN separate scratch
//! database (it additionally needs the `coralswarm_app` role/password,
//! which the other six don't) — same `retraction_deletion_rls_pg.rs`-style
//! pattern as before, now also tearing itself down (finding 13). Both need
//! only `ORGT_TEST_BASE_URL` — the SAME DSN already wired into CI for
//! `retraction_deletion_rls_pg.rs`'s job, so wiring this suite is one more
//! `cargo test` step in that EXISTING job, not a new Postgres-service lane.
//! A normal `cargo test` with no DB configured skips every test in this
//! file (each prints `skipping: set ORGT_TEST_BASE_URL` and returns —
//! never a silent, assertion-free "pass").
//!
//! Run with:
//!   ORGT_TEST_BASE_URL=postgres://$USER@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test b2_retract_fixes_pg -- --test-threads=1 --nocapture

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use coralswarm_backend::jobs::queue;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::source_blob_cleanup;
use coralswarm_backend::services::connector_audit::Actor;
use coralswarm_backend::services::deletion::{delete_coral, delete_subject, outcome as retract_outcome, SurfaceOutcome};
use coralswarm_backend::services::blob_store::PromotionBlobStore;
use coralswarm_backend::services::retraction_audit;
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_core::r2::R2Error;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::OnceCell;
use uuid::Uuid;
use coralswarm_backend::store::SessionListFilter;

/// Fixed name — recreated (dropped then created) on first use each run, via
/// the SAME `recreate` helper probe (c) uses further down this file.
const SHARED_SCRATCH_DB_NAME: &str = "b2_retract_fixes_shared_wt";

static SHARED_SCRATCH_DB_URL: OnceCell<Option<String>> = OnceCell::const_new();

/// Lazily create/migrate ONE shared scratch database for this whole
/// test-binary run — see this file's own module doc ("Single-DSN by
/// design") for why this replaced a second, external
/// `RELINFER_TEST_DATABASE_URL` convention. `None` when
/// `ORGT_TEST_BASE_URL` is unset (every caller treats that as "skip this
/// test", never as "run with no assertions").
///
/// **Caches the resolved URL, NOT a `PgPool`, deliberately.** Each
/// `#[tokio::test]` function gets its OWN fresh Tokio runtime by default —
/// a `sqlx::PgPool` spawns background housekeeping bound to whichever
/// runtime was ACTIVE when it was built, so a pool built inside test A's
/// runtime and later reused (via a cached `PgPool` clone) inside test B's
/// DIFFERENT, by-then-torn-down-if-A-finished-first runtime intermittently
/// deadlocks/times out acquiring a connection (`PoolTimedOut`) — observed
/// directly: an earlier revision of this function cached the `PgPool`
/// itself and 4 of 10 tests failed that way, non-deterministically, only
/// when run as part of the full suite (never alone). Doing the expensive
/// part (CREATE DATABASE + run every `migrations_control` migration) ONCE,
/// then having every caller build its OWN cheap `PgPool::connect` against
/// the SAME already-migrated database, keeps the one-time cost while giving
/// every test a pool that belongs to ITS OWN runtime.
async fn shared_scratch_pool() -> Option<PgPool> {
    let db_url = SHARED_SCRATCH_DB_URL
        .get_or_init(|| async {
            let base = base_url()?;
            let mut admin_conn = admin(&base).await;
            recreate(&mut admin_conn, SHARED_SCRATCH_DB_NAME).await;
            let db_url = with_db(&base, SHARED_SCRATCH_DB_NAME);
            let pool: PgPool = PgPoolOptions::new()
                .connect(&db_url)
                .await
                .expect("connect shared scratch db (one-shot, for migration only)");
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .expect("migrate control tree incl. 0196");
            pool.close().await;
            Some(db_url)
        })
        .await
        .clone()?;
    Some(
        PgPoolOptions::new()
            .max_connections(8)
            .connect(&db_url)
            .await
            .expect("connect to the already-migrated shared scratch db"),
    )
}

// Worker proofs need a private queue: unrelated tests also enqueue cleanup
// jobs, and must not consume this transport's injected second-call failure.
async fn cleanup_scratch_pool(db: &str) -> Option<PgPool> {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return None;
    };
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    Some(pool)
}

async fn drain_blob_cleanup(pool: &PgPool, blobs: &FlakyBlobs) -> usize {
    let jobs = queue::dequeue(pool, &["DeleteSourceBlob"], 20).await.unwrap();
    let count = jobs.len();
    for job in jobs {
        source_blob_cleanup::process_job(&TenantId::Consumer, pool, Some(blobs), job).await;
    }
    count
}

async fn drop_cleanup_scratch(pool: PgPool, db: &str) {
    pool.close().await;
    let mut conn = admin(&base_url().unwrap()).await;
    conn.execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str()).await.unwrap();
}

macro_rules! skip_without_db {
    () => {
        match shared_scratch_pool().await {
            Some(pool) => pool,
            None => {
                eprintln!("skipping: set ORGT_TEST_BASE_URL");
                return;
            }
        }
    };
}

// ── In-memory blob double, identical shape to deletion_pg.rs's ─────────────

#[derive(Default)]
struct FlakyBlobs {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    delete_attempts: AtomicUsize,
    /// 0 = never fail. Otherwise, the Nth call to `delete_blob` (1-indexed)
    /// fails — used by the findings 2/5 cleanup-retry proof to simulate
    /// an R2 error after a multi-atom SQL retract has committed.
    fail_delete_on_attempt: AtomicUsize,
}

impl FlakyBlobs {
    fn put(&self, key: &str, content: &[u8]) {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_owned(), content.to_vec());
    }
    fn contains(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }
    fn fail_delete_on_attempt(&self, n: usize) {
        self.fail_delete_on_attempt.store(n, Ordering::SeqCst);
    }
}

#[async_trait]
impl PromotionBlobStore for FlakyBlobs {
    async fn get_blob(&self, blob_key: &str, _dek: &[u8; 32]) -> Result<Vec<u8>, R2Error> {
        self.objects
            .lock()
            .unwrap()
            .get(blob_key)
            .cloned()
            .ok_or_else(|| R2Error::NotFound(blob_key.to_owned()))
    }
    async fn put_blob(&self, blob_key: &str, content: &[u8], _dek: &[u8; 32]) -> Result<(), R2Error> {
        self.put(blob_key, content);
        Ok(())
    }
    async fn delete_blob(&self, blob_key: &str) -> Result<(), R2Error> {
        let attempt = self.delete_attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let fail_on = self.fail_delete_on_attempt.load(Ordering::SeqCst);
        if fail_on != 0 && attempt == fail_on {
            return Err(R2Error::S3("injected delete failure (findings 2/5 proof)".into()));
        }
        self.objects.lock().unwrap().remove(blob_key);
        Ok(())
    }
}

// ── Seed helpers (mirror deletion_pg.rs's shape) ────────────────────────────

async fn seed_user(pool: &PgPool) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .expect("insert user")
}

async fn seed_account(pool: &PgPool) -> (Uuid, Uuid) {
    let user_id = seed_user(pool).await;
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("insert account_members");
    (account_id, user_id)
}

async fn seed_graph_in(pool: &PgPool, account_id: Uuid, kind: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Ocean', $2) RETURNING id")
        .bind(account_id)
        .bind(kind)
        .fetch_one(pool)
        .await
        .expect("insert graph")
}

async fn seed_atom(
    pool: &PgPool,
    blobs: &FlakyBlobs,
    graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    timestamp: chrono::DateTime<Utc>,
) -> (Uuid, String) {
    let atom_id = Uuid::new_v4();
    let blob_key = format!("{graph_id}/{atom_id}");
    blobs.put(&blob_key, b"sensitive ingested content");
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             content_inline, participants, \"timestamp\", scope, fts_language, header_len) \
         VALUES ($1, $2, $3, $4, $5, $6, '{}', $7, 'team', 'english', 0)",
    )
    .bind(atom_id)
    .bind(graph_id)
    .bind(source_type)
    .bind(source_id)
    .bind(&blob_key)
    .bind("the fee is $48,500 — plaintext body FM-1 must not duplicate into op_log")
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("insert atom");
    (atom_id, blob_key)
}

/// A permitted immutable checkpoint snapshot supplies session read identity;
/// mutable coding_sessions descriptors and cc-prefixed atoms alone do not.
async fn seed_session_evidence(pool: &PgPool, graph: Uuid, atom: Uuid, session: &str) {
    let mut tx = pool.begin().await.expect("begin session evidence fixture");
    let (kind, source): (String, String) = sqlx::query_as(
        "SELECT source_type,source_id FROM atoms WHERE graph_id=$1 AND atom_id=$2",
    ).bind(graph).bind(atom).fetch_one(&mut *tx).await.expect("read exact checkpoint identity");
    let item: Uuid = sqlx::query_scalar(
        "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) \
         VALUES($1,$2,'fixture:retraction-session',$3) RETURNING source_item_id",
    ).bind(graph).bind(&kind).bind(&source).fetch_one(&mut *tx).await.expect("seed permitted session item");
    let context = serde_json::json!({"revision":1,"session_id":session,"session_kind":"coding"});
    let version: Uuid = sqlx::query_scalar(
        "INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,source_metadata,capture_context) \
         VALUES($1,$2,$3,$4,'fixture:retraction-session','Retained checkpoint.',$5,$6) RETURNING version_id",
    ).bind(graph).bind(item).bind(&source).bind(&kind)
        .bind(serde_json::json!({"capture_context":context}))
        .bind(context).fetch_one(&mut *tx).await.expect("seed immutable session version");
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item).bind(version).execute(&mut *tx).await.expect("set current checkpoint version");
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
        .bind(graph).bind(version).bind(atom).execute(&mut *tx).await.expect("link exact checkpoint evidence");
    tx.commit().await.expect("commit session evidence fixture");
}

async fn atom_exists(pool: &PgPool, atom_id: Uuid) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(pool)
        .await
        .unwrap()
        > 0
}

async fn seed_node(pool: &PgPool, graph_id: Uuid, node_type: &str, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, $2, $3, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph_id)
    .bind(node_type)
    .bind(title)
    .fetch_one(pool)
    .await
    .expect("insert node")
}

async fn link(pool: &PgPool, node_id: Uuid, atom_id: Uuid) {
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("link atom to node");
}

// ── FM-1: retract's tombstone / op_log plaintext leak ───────────────────────

/// The unit file's own probe, inverted: after a retract AND a sweep run past
/// `TOMBSTONE_RETENTION_DAYS`, a direct query for the atom's plaintext in
/// `op_log` returns ZERO rows. The negative control ON the control: seeded
/// FIRST with a synthetic `upsert` op carrying the plaintext (the exact
/// shape `store::record_atom_content_op` produces, and the exact shape the
/// PRE-FIX sweep — `WHERE op_type = 'delete'` only — left behind forever),
/// proving the detection query finds real bytes before asserting it finds
/// none after the (fixed) sweep runs.
#[tokio::test]
async fn fm1_stale_content_upsert_and_tombstone_are_both_purged_past_retention() {
    let db = "b2_blob_retention_cleanup";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
    let Some(pool) = cleanup_scratch_pool(db).await else { return; };
    let (account_id, _) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let (atom_id, blob_key) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-1", Utc::now()).await;

    const PLAINTEXT: &str = "the fee is $48,500 — this exact string must never survive in op_log";

    // Simulate the pre-fix leak this test's own probe targets: an `upsert`
    // op carrying the atom's plaintext, exactly the shape
    // `store::sync::atom_content_op` produces on every inline-content write
    // — and, before this PR, NEVER purged at any age.
    let old_op_id = Uuid::new_v4();
    let old_updated_at = Utc::now() - ChronoDuration::days(40);
    let old_seq: i64 = sqlx::query_scalar(
        "INSERT INTO op_log (op_id, entity, entity_id, op_type, payload, updated_at, device_id, graph_id) \
         VALUES ($1, 'atom', $2, 'upsert', $3, $4, 'test-device', $5) RETURNING seq",
    )
    .bind(old_op_id)
    .bind(atom_id.to_string())
    .bind(serde_json::json!({
        "atom_id": atom_id.to_string(),
        "graph_id": graph_id.to_string(),
        "content_inline": PLAINTEXT,
    }))
    .bind(old_updated_at)
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .expect("seed stale content upsert op");

    // Negative control ON the control: prove the detection query can find
    // real bytes before asserting it finds none — the failure mode this
    // guards is "a sweep that exits 0 having matched nothing", per the unit
    // file's own warning.
    let found_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM op_log WHERE entity_id = $1 AND payload->>'content_inline' = $2",
    )
    .bind(atom_id.to_string())
    .bind(PLAINTEXT)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        found_before, 1,
        "the detection query must find the seeded plaintext BEFORE the sweep runs, \
         or the query is wrong rather than the data clean"
    );

    // Retract the atom. FM-1's tombstone half: the delete op this writes
    // must NOT carry content_inline at all.
    let outcome = delete_coral(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        atom_id,
        Actor::System,
        None,
    )
    .await
    .expect("delete_coral succeeds");
    assert_eq!(outcome.atoms_deleted, vec![(graph_id, atom_id)]);
    assert!(!atom_exists(&pool, atom_id).await);
    assert!(blobs.contains(&blob_key), "SQL deletion queues post-commit ciphertext erasure");
    let queued: (Option<Uuid>, String) = sqlx::query_as(
        "SELECT account_id,status::text FROM jobs WHERE kind='DeleteSourceBlob' AND payload->>'blob_key'=$1",
    ).bind(&blob_key).fetch_one(&pool).await.unwrap();
    assert_eq!(queued, (Some(account_id), "pending".to_string()));
    assert_eq!(drain_blob_cleanup(&pool, &blobs).await, 1);
    assert!(!blobs.contains(&blob_key), "the real cleanup worker must destroy the R2 blob too");
    let completed: bool = sqlx::query_scalar(
        "SELECT status='done' AND outcome='did_work' FROM jobs WHERE kind='DeleteSourceBlob' AND payload->>'blob_key'=$1",
    ).bind(&blob_key).fetch_one(&pool).await.unwrap();
    assert!(completed, "physical erasure must durably complete the cleanup job");

    let tombstone_payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM op_log WHERE entity_id = $1 AND op_type = 'delete' \
         ORDER BY seq DESC LIMIT 1",
    )
    .bind(atom_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("tombstone op exists");
    assert!(
        tombstone_payload.get("content_inline").is_none(),
        "FM-1: the delete tombstone must not carry content_inline at all — got {tombstone_payload:?}"
    );

    let immediate_plaintext: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM op_log WHERE graph_id=$1 AND entity_id=$2 AND payload->>'content_inline'=$3",
    ).bind(graph_id).bind(atom_id.to_string()).bind(PLAINTEXT).fetch_one(&pool).await.unwrap();
    assert_eq!(immediate_plaintext, 0, "modern deletion must erase historical plaintext synchronously");

    // Stage a pre-fix historical snapshot, preserving its ORIGINAL smaller
    // seq. This must not become a new post-delete upsert/recreated identity.
    let tombstone_seq: i64 = sqlx::query_scalar(
        "SELECT seq FROM op_log WHERE graph_id=$1 AND entity='atom' AND entity_id=$2 AND op_type='delete' ORDER BY seq DESC LIMIT 1",
    ).bind(graph_id).bind(atom_id.to_string()).fetch_one(&pool).await.unwrap();
    assert!(old_seq < tombstone_seq, "historical snapshot must precede the tombstone");
    sqlx::query(
        "INSERT INTO op_log(seq,op_id,entity,entity_id,op_type,payload,updated_at,device_id,graph_id) \
         VALUES($1,$2,'atom',$3,'upsert',$4,$5,'test-device',$6)",
    ).bind(old_seq).bind(old_op_id).bind(atom_id.to_string())
        .bind(serde_json::json!({"atom_id":atom_id,"graph_id":graph_id,"content_inline":PLAINTEXT}))
        .bind(old_updated_at).bind(graph_id).execute(&pool).await.expect("stage pre-fix historical snapshot");
    let staged: (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER(WHERE payload->>'content_inline'=$3),max(seq) FROM op_log WHERE graph_id=$1 AND entity='atom' AND entity_id=$2",
    ).bind(graph_id).bind(atom_id.to_string()).bind(PLAINTEXT).fetch_one(&pool).await.unwrap();
    assert_eq!(staged, (1, tombstone_seq), "the historical plaintext is present and the tombstone remains newest");

    // Backdate the tombstone itself past the retention window so the sweep
    // has something to purge (delete_coral just wrote it with updated_at =
    // now()).
    sqlx::query("UPDATE op_log SET updated_at = $1 WHERE entity_id = $2 AND op_type = 'delete'")
        .bind(Utc::now() - ChronoDuration::days(40))
        .bind(atom_id.to_string())
        .execute(&pool)
        .await
        .unwrap();

    // FM-1's sweep half: purge past TOMBSTONE_RETENTION_DAYS must reach the
    // stale upsert too, not just the tombstone.
    let store = PostgresStore::new(pool.clone());
    let cutoff = Utc::now() - ChronoDuration::days(30);
    let removed = store.purge_expired_tombstones(cutoff).await.unwrap();
    assert!(
        removed >= 2,
        "expected the tombstone AND the stale upsert to be purged, got {removed}"
    );

    let found_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM op_log WHERE entity_id = $1 AND payload->>'content_inline' = $2",
    )
    .bind(atom_id.to_string())
    .bind(PLAINTEXT)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        found_after, 0,
        "FM-1: the atom's plaintext must be gone from op_log after the sweep"
    );
    let remaining_any: i64 = sqlx::query_scalar("SELECT count(*) FROM op_log WHERE entity_id = $1")
        .bind(atom_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining_any, 0, "no op_log row for this atom should survive the sweep");
    drop_cleanup_scratch(pool, db).await;
}

/// Finding 1 (independent review): `op_log` `entity_id`s are DETERMINISTIC
/// (`node_atom`'s is `compose_id(node_id, atom_id)`), so a `merge_nodes`
/// delete followed, much later, by an `unmerge_nodes` relink reuses the
/// IDENTICAL `entity_id`. A purge scoped only by `(entity, entity_id)` —
/// ignoring which op is CURRENT — would destroy that fresh relink alongside
/// its ancient, unrelated sibling tombstone, permanently diverging any peer
/// whose cursor predates the relink's `seq`. Both the old delete and the
/// "relink" upsert here are seeded old enough to be raw-age candidates —
/// the survival must come from "nothing newer exists for this identity",
/// not merely from the relink being fresh.
#[tokio::test]
async fn fm1_a_newer_op_under_a_recycled_entity_id_survives_a_purge_pg() {
    let pool = skip_without_db!();
    let recycled_entity_id = Uuid::new_v4().to_string();
    let ancient = Utc::now() - ChronoDuration::days(90);
    let old = Utc::now() - ChronoDuration::days(60);

    sqlx::query(
        "INSERT INTO op_log (op_id, entity, entity_id, op_type, payload, updated_at, device_id, graph_id)          VALUES ($1, 'node_atom', $2, 'delete', '{}'::jsonb, $3, 'test-device', NULL)",
    )
    .bind(Uuid::new_v4())
    .bind(&recycled_entity_id)
    .bind(ancient)
    .execute(&pool)
    .await
    .expect("seed the old delete (e.g. merge_nodes dropping a link)");

    sqlx::query(
        "INSERT INTO op_log (op_id, entity, entity_id, op_type, payload, updated_at, device_id, graph_id)          VALUES ($1, 'node_atom', $2, 'upsert', $3, $4, 'test-device', NULL)",
    )
    .bind(Uuid::new_v4())
    .bind(&recycled_entity_id)
    .bind(serde_json::json!({"relinked": true}))
    .bind(old)
    .execute(&pool)
    .await
    .expect("seed the relink (e.g. unmerge_nodes restoring it) — same entity_id, higher seq");

    let store = PostgresStore::new(pool.clone());
    let cutoff = Utc::now() - ChronoDuration::days(30);
    let removed = store.purge_expired_tombstones(cutoff).await.unwrap();
    assert_eq!(
        removed, 0,
        "FM-1 correctness: an entity whose CURRENT op is the relink (not the delete) must not be          purged at all, however old its earlier tombstone is — got {removed} rows removed"
    );

    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM op_log WHERE entity_id = $1")
        .bind(&recycled_entity_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 2, "both the old delete AND the relink must survive");

    let relink_survives: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM op_log WHERE entity_id = $1 AND op_type = 'upsert'",
    )
    .bind(&recycled_entity_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        relink_survives, 1,
        "the relink op specifically must survive — a peer whose cursor predates it would          otherwise never learn the link was restored"
    );
}

// ── FM-2: meeting transcript / share link ───────────────────────────────────

/// The unit file's own probe, inverted: retract every coral of a meeting,
/// then EXERCISE a share link issued BEFORE the retraction (not merely
/// check the FK/column) — it must fail to resolve, via the exact production
/// read path (`Store::get_active_meeting_share_by_token`) the public/authed
/// share routes call.
#[tokio::test]
async fn fm2_retracting_every_chunk_redacts_transcript_and_stops_the_share_link_resolving() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let now = Utc::now();

    let (chunk0, _) = seed_atom(&pool, &blobs, graph_id, "zoom", "meet-1:chunk:0", now).await;
    let (chunk1, _) = seed_atom(&pool, &blobs, graph_id, "zoom", "meet-1:chunk:1", now).await;

    let transcript_id: Uuid = sqlx::query_scalar(
        "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline, duration_ms) \
         VALUES ($1, 'meet-1', 'verbatim transcript text', 60000) RETURNING id",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .expect("seed meeting_transcripts");

    sqlx::query(
        "INSERT INTO transcript_corrections (graph_id, atom_id, source_id, surface_form, canonical) \
         VALUES ($1, $2, 'meet-1', 'Jordan', 'Morgan')",
    )
    .bind(graph_id)
    .bind(chunk0)
    .execute(&pool)
    .await
    .expect("seed transcript_corrections");

    let token = format!("test-share-token-fm2-{}", Uuid::new_v4());
    let token = token.as_str();
    sqlx::query(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by) \
         VALUES ($1, 'meet-1', $2, 'link', $3)",
    )
    .bind(graph_id)
    .bind(token)
    .bind(user_id)
    .execute(&pool)
    .await
    .expect("seed meeting_shares");

    let store = PostgresStore::new(pool.clone());
    // Before retraction: the share link resolves.
    assert!(
        store.get_active_meeting_share_by_token(token).await.unwrap().is_some(),
        "sanity: the share link must resolve BEFORE retraction"
    );

    // Retract the FIRST chunk only — the meeting is not yet fully retracted.
    delete_coral(&pool, Some(&blobs), "test-device", graph_id, chunk0, Actor::System, None)
        .await
        .expect("delete_coral (chunk0) succeeds");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM meeting_transcripts WHERE id = $1")
            .bind(transcript_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "transcript must survive while a sibling chunk remains"
    );
    assert!(
        store.get_active_meeting_share_by_token(token).await.unwrap().is_some(),
        "share link must still resolve while a sibling chunk remains"
    );

    // Retract the LAST remaining chunk — the meeting is now fully retracted.
    let outcome = delete_coral(&pool, Some(&blobs), "test-device", graph_id, chunk1, Actor::System, None)
        .await
        .expect("delete_coral (chunk1) succeeds");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM meeting_transcripts WHERE id = $1")
            .bind(transcript_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "FM-2: meeting_transcripts row must be gone once every chunk is retracted"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM transcript_corrections WHERE graph_id = $1 AND source_id = 'meet-1'"
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "FM-2: transcript_corrections overlay must be gone too"
    );

    // The literal negative control: EXERCISE the link, don't just check the
    // column.
    assert!(
        store.get_active_meeting_share_by_token(token).await.unwrap().is_none(),
        "FM-2: a share link issued before retraction must fail to resolve after it"
    );

    assert!(
        outcome
            .surface_outcomes
            .iter()
            .any(|o| o.surface == "meeting_transcript" && o.outcome == retract_outcome::REMOVED),
        "the cascade outcome must record the meeting_transcript surface as removed"
    );
}

/// Finding 3 (independent review, CRITICAL/destructive) — a Slack atom
/// whose `source_id` collides with a REAL meeting's base id must NEVER
/// trigger that meeting's redaction: no R2 blob delete, no
/// `transcript_corrections`/`meeting_transcripts` delete, no
/// `meeting_shares` revoke. Before the source_type gate, this atom alone
/// (retracted by itself, no other chunks involved) destroyed a live
/// meeting nobody asked to retract.
#[tokio::test]
async fn fm3_review_unrelated_atom_sharing_a_meetings_source_id_does_not_destroy_it() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let now = Utc::now();

    // A REAL, live, unrelated meeting — no relationship to the Slack atom
    // below except an unlucky matching source_id.
    let transcript_id: Uuid = sqlx::query_scalar(
        "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline, duration_ms)          VALUES ($1, 'collision-1', 'a real, live, unrelated meeting transcript', 60000)          RETURNING id",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .expect("seed the real meeting_transcripts row");
    // Deliberately NO live `atoms` row for the real meeting's own chunk
    // pattern (e.g. its chunks were already consumed/embedded and evicted,
    // or ingestion wrote the transcript but the atom write raced/lagged) —
    // this is what makes the collision atom below the ONLY thing sharing
    // the base id at the moment it's deleted: with a real chunk still
    // present, the (buggy, unscoped) "remaining" count would never reach
    // zero from deleting the Slack atom alone, masking the bug rather than
    // exposing it.
    let token = format!("test-share-token-fm3collision-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by)          VALUES ($1, 'collision-1', $2, 'link', $3)",
    )
    .bind(graph_id)
    .bind(&token)
    .bind(user_id)
    .execute(&pool)
    .await
    .expect("seed meeting_shares for the real meeting");

    // The UNRELATED Slack atom whose source_id collides with the real
    // meeting's base id.
    let (slack_atom, _) = seed_atom(&pool, &blobs, graph_id, "slack", "collision-1", now).await;

    delete_coral(&pool, Some(&blobs), "test-device", graph_id, slack_atom, Actor::System, None)
        .await
        .expect("delete_coral (the unrelated Slack atom) succeeds");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM meeting_transcripts WHERE id = $1")
            .bind(transcript_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "finding 3: the REAL meeting's transcript must survive an unrelated Slack atom's retraction"
    );
    let store = PostgresStore::new(pool.clone());
    assert!(
        store.get_active_meeting_share_by_token(&token).await.unwrap().is_some(),
        "finding 3: the real meeting's share link must still resolve"
    );
}

/// Finding 3's mirror case — an unrelated non-transcript-shaped atom
/// sharing a meeting's base source_id must NOT block that meeting's
/// legitimate redaction once every REAL chunk is retracted: the sibling
/// count must be scoped to transcript-shaped source types, not to every
/// atom sharing the base id.
#[tokio::test]
async fn fm3_review_mirror_case_unrelated_atom_does_not_block_the_real_redaction() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let now = Utc::now();

    let transcript_id: Uuid = sqlx::query_scalar(
        "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline, duration_ms)          VALUES ($1, 'mirror-1', 'a real meeting transcript', 60000) RETURNING id",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .expect("seed meeting_transcripts");
    let token = format!("test-share-token-fm3mirror-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by)          VALUES ($1, 'mirror-1', $2, 'link', $3)",
    )
    .bind(graph_id)
    .bind(&token)
    .bind(user_id)
    .execute(&pool)
    .await
    .expect("seed meeting_shares");

    // The one REAL chunk of this meeting.
    let (real_chunk, _) = seed_atom(&pool, &blobs, graph_id, "zoom", "mirror-1:chunk:0", now).await;
    // An UNRELATED Slack atom that happens to share the exact base id —
    // deliberately never retracted, must not count as a "sibling".
    let (_unrelated, _) = seed_atom(&pool, &blobs, graph_id, "slack", "mirror-1", now).await;

    let outcome = delete_coral(&pool, Some(&blobs), "test-device", graph_id, real_chunk, Actor::System, None)
        .await
        .expect("delete_coral (the real, only, chunk) succeeds");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM meeting_transcripts WHERE id = $1")
            .bind(transcript_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "finding 3 mirror: the real meeting must be redacted once its only real chunk is gone,          even though an unrelated Slack atom still shares its base source_id"
    );
    let store = PostgresStore::new(pool.clone());
    assert!(
        store.get_active_meeting_share_by_token(&token).await.unwrap().is_none(),
        "finding 3 mirror: the share link must stop resolving"
    );
    assert!(
        outcome
            .surface_outcomes
            .iter()
            .any(|o| o.surface == "meeting_transcript" && o.outcome == retract_outcome::REMOVED),
        "the cascade outcome must record the meeting_transcript surface as removed"
    );
}

/// Finding 4 (independent review, HIGH) — an unescaped `LIKE` pattern built
/// from `base` lets a literal `_`/`%` in a meeting's own source_id act as a
/// SQL wildcard. `"msg_001"` (`routes/ingest.rs`'s own exemplar of a
/// caller-supplied id containing an underscore) unescaped would ALSO match
/// `"msgX001:chunk:0"` — a DIFFERENT meeting's real, unrelated chunk —
/// inflating the sibling count and silently leaving the FIRST meeting's
/// transcript/share un-redacted even once every one of ITS OWN real chunks
/// is gone.
#[tokio::test]
async fn fm4_review_underscore_in_source_id_does_not_over_match_via_like() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let now = Utc::now();

    // Meeting A — base source_id contains a literal underscore.
    let transcript_a: Uuid = sqlx::query_scalar(
        "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline, duration_ms)          VALUES ($1, 'msg_001', 'meeting A transcript', 60000) RETURNING id",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .expect("seed meeting A's transcript");
    let token_a = format!("test-share-token-fm4a-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by)          VALUES ($1, 'msg_001', $2, 'link', $3)",
    )
    .bind(graph_id)
    .bind(&token_a)
    .bind(user_id)
    .execute(&pool)
    .await
    .expect("seed meeting A's share");
    let (chunk_a, _) = seed_atom(&pool, &blobs, graph_id, "zoom", "msg_001:chunk:0", now).await;

    // Meeting B — a COMPLETELY UNRELATED meeting whose base id would match
    // meeting A's `_`-bearing pattern if `_` is left as a live SQL wildcard
    // (`_` matches any single character — 'X' included).
    let (chunk_b, _) = seed_atom(&pool, &blobs, graph_id, "zoom", "msgX001:chunk:0", now).await;

    // Retract meeting A's one and only real chunk.
    let outcome = delete_coral(&pool, Some(&blobs), "test-device", graph_id, chunk_a, Actor::System, None)
        .await
        .expect("delete_coral (meeting A's only chunk) succeeds");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM meeting_transcripts WHERE id = $1")
            .bind(transcript_a)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "finding 4: meeting A must be redacted once its own real chunk is gone — an unescaped          LIKE pattern would count meeting B's unrelated chunk as a false sibling and block this"
    );
    let store = PostgresStore::new(pool.clone());
    assert!(
        store.get_active_meeting_share_by_token(&token_a).await.unwrap().is_none(),
        "finding 4: meeting A's share link must stop resolving"
    );
    assert!(
        outcome
            .surface_outcomes
            .iter()
            .any(|o| o.surface == "meeting_transcript" && o.outcome == retract_outcome::REMOVED),
        "the cascade outcome must record the meeting_transcript surface as removed"
    );
    // Meeting B's own atom is untouched — it was never a real sibling.
    assert!(atom_exists(&pool, chunk_b).await, "meeting B's unrelated chunk atom must survive");
}

// ── FM-6: coding sessions ────────────────────────────────────────────────────

/// The unit file's own probe, inverted: after retracting every coral of a
/// session, `list_coding_sessions` (the `list_sessions` MCP tool's own
/// backing query) must return nothing for it, and none of
/// title/project_key/repo_path/hostname/model/started_at/last_activity_at is
/// readable — via the ACTUAL table, not merely a `coral_count = 0` label.
#[tokio::test]
async fn fm6_retracting_every_checkpoint_removes_the_session_and_its_share() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO coding_sessions \
            (graph_id, session_id, project_key, title, repo_path, hostname, model, \
             started_at, last_activity_at) \
         VALUES ($1, 'sess-1', 'coralswarm', 'Fix the login bug', '/home/me/coralswarm', \
                 'Alexs-mbp', 'claude-sonnet-5', now(), now())",
    )
    .bind(graph_id)
    .execute(&pool)
    .await
    .expect("seed coding_sessions");

    let (ckpt0, _) = seed_atom(&pool, &blobs, graph_id, "coding_session", "cc:sess-1:ckpt-0", now).await;
    let (ckpt1, _) = seed_atom(&pool, &blobs, graph_id, "coding_session", "cc:sess-1:ckpt-1", now).await;
    seed_session_evidence(&pool, graph_id, ckpt0, "sess-1").await;
    seed_session_evidence(&pool, graph_id, ckpt1, "sess-1").await;

    let session_share_token = format!("test-share-token-fm6-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO coding_session_shares (graph_id, session_id, token, visibility, created_by) \
         VALUES ($1, 'sess-1', $3, 'link', $2)",
    )
    .bind(graph_id)
    .bind(user_id)
    .bind(&session_share_token)
    .execute(&pool)
    .await
    .expect("seed coding_session_shares");

    let store = PostgresStore::new(pool.clone());
    let listed_before = store
        .list_coding_sessions(graph_id, None, None, None, None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    assert_eq!(listed_before.len(), 1, "sanity: the session lists BEFORE retraction");

    // Retract the first checkpoint only.
    delete_coral(&pool, Some(&blobs), "test-device", graph_id, ckpt0, Actor::System, None)
        .await
        .expect("delete_coral (ckpt0) succeeds");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM coding_sessions WHERE graph_id = $1 AND session_id = 'sess-1'"
        )
        .bind(graph_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
        "session must survive while a sibling checkpoint remains"
    );

    // Retract the LAST remaining checkpoint.
    let outcome = delete_coral(&pool, Some(&blobs), "test-device", graph_id, ckpt1, Actor::System, None)
        .await
        .expect("delete_coral (ckpt1) succeeds");

    let listed_after = store
        .list_coding_sessions(graph_id, None, None, None, None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    assert!(
        listed_after.is_empty(),
        "FM-6: list_sessions must return nothing for the retracted session, got {listed_after:?}"
    );
    let row_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM coding_sessions WHERE graph_id = $1 AND session_id = 'sess-1'",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row_count, 0,
        "FM-6: the coding_sessions row itself — title/project_key/repo_path/hostname/model/\
         started_at/last_activity_at — must not survive retraction"
    );
    let shares_remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM coding_session_shares WHERE graph_id = $1 AND session_id = 'sess-1'",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        shares_remaining, 0,
        "FM-6: coding_session_shares must cascade away with the session row"
    );

    assert!(
        outcome
            .surface_outcomes
            .iter()
            .any(|o| o.surface == "coding_session" && o.outcome == retract_outcome::REMOVED),
        "the cascade outcome must record the coding_session surface as removed"
    );
}

/// Finding 6 (independent review, HIGH) — a coding-session checkpoint atom
/// whose `source_type` is STILL `"note"` (never relabeled to
/// `"coding_session"`) must still be recognized by its `"cc:"` source_id
/// prefix, matching `Store::list_coding_sessions`' own canonical
/// definition. This reproduces a REAL, permanent case, not a hypothetical
/// one: `migrations_tenant/0011_coding_session_source_type.sql`'s backfill
/// skips relabeling any atom that collides with an existing
/// `coding_session`-typed row at the same `(graph_id, source_id)` — that
/// atom keeps its pre-backfill `source_type` forever, on an otherwise fully
/// migrated, functioning tenant.
#[tokio::test]
async fn fm6_review_un_relabeled_source_type_checkpoint_still_removes_the_husk() {
    let pool = skip_without_db!();
    let (account_id, _) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let now = Utc::now();

    sqlx::query(
        "INSERT INTO coding_sessions             (graph_id, session_id, project_key, title, repo_path, hostname, model,              started_at, last_activity_at)          VALUES ($1, 'sess-unrelabeled', 'coralswarm', 'A husk-prone session',                  '/home/me/coralswarm', 'Alexs-mbp', 'claude-sonnet-5', now(), now())",
    )
    .bind(graph_id)
    .execute(&pool)
    .await
    .expect("seed coding_sessions");

    // The ONLY checkpoint of this session — `source_id` carries the real
    // `"cc:"` convention, but `source_type` is still `"note"`, exactly the
    // shape a clash-skipped backfill row (or an un-migrated tenant) leaves
    // behind.
    let (ckpt, _) = seed_atom(&pool, &blobs, graph_id, "note", "cc:sess-unrelabeled:ckpt-0", now).await;
    seed_session_evidence(&pool, graph_id, ckpt, "sess-unrelabeled").await;
    let still_note: String = sqlx::query_scalar("SELECT source_type FROM atoms WHERE atom_id=$1")
        .bind(ckpt).fetch_one(&pool).await.unwrap();
    assert_eq!(still_note, "note", "canonical evidence must not relabel the legacy checkpoint");

    let store = PostgresStore::new(pool.clone());
    assert_eq!(
        store
            .list_coding_sessions(graph_id, None, None, None, None, 10, &SessionListFilter::default())
            .await
            .unwrap()
            .len(),
        1,
        "sanity: the session lists BEFORE retraction despite source_type = 'note'"
    );

    let outcome = delete_coral(&pool, Some(&blobs), "test-device", graph_id, ckpt, Actor::System, None)
        .await
        .expect("delete_coral (the un-relabeled checkpoint) succeeds");

    let listed_after = store
        .list_coding_sessions(graph_id, None, None, None, None, 10, &SessionListFilter::default())
        .await
        .unwrap();
    assert!(
        listed_after.is_empty(),
        "finding 6: list_sessions must return nothing even though the retracted checkpoint's          source_type was never relabeled to 'coding_session' — got {listed_after:?}"
    );
    let row_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM coding_sessions WHERE graph_id = $1 AND session_id = 'sess-unrelabeled'",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row_count, 0, "finding 6: the coding_sessions husk must not survive");
    assert!(
        outcome
            .surface_outcomes
            .iter()
            .any(|o| o.surface == "coding_session" && o.outcome == retract_outcome::REMOVED),
        "the cascade outcome must record the coding_session surface as removed"
    );
}

// ── Finding 9: record_not_a_member actually exercised ──────────────────────

/// Finding 9 (independent review): `retraction_audit::record_not_a_member`'s
/// own doc comment used to claim it was "provided and tested" — it was
/// provided, but NOTHING called it, anywhere, ever. This test makes that
/// claim true: writes a real `outcome = 'not_a_member'` row through the
/// actual function, against the actual table and its actual CHECK
/// constraints (not a hand-written INSERT bypassing the function), and
/// reads it back.
#[tokio::test]
async fn finding_9_record_not_a_member_is_actually_exercised() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let source_id = format!("msg-not-a-member-{}", Uuid::new_v4());

    let retraction_id = retraction_audit::record_not_a_member(
        &pool,
        account_id,
        user_id,
        graph_id,
        false,
        "slack",
        &source_id,
        None,
    )
    .await;
    assert!(
        retraction_id.is_some(),
        "finding 9: record_not_a_member must succeed against the real constraints"
    );

    let (outcome_written, surface_written): (String, String) = sqlx::query_as(
        "SELECT outcome, surface FROM retraction_audit_log WHERE retraction_id = $1",
    )
    .bind(retraction_id.unwrap())
    .fetch_one(&pool)
    .await
    .expect("the row record_not_a_member wrote must be readable back");
    assert_eq!(
        outcome_written, "not_a_member",
        "finding 9: the vocabulary's third value must be a real, exercised outcome — not a \
         string nobody ever inserts"
    );
    assert_eq!(surface_written, "content");
}

/// Finding 9's second half: `retraction_audit::record_best_effort` (the
/// `Actor::System`/`Actor::Webhook` tier) is likewise never reached by any
/// PRODUCTION call site today — `connectors::google_gmail`/
/// `google_calendar` both deliberately pass `control_pool: None` (see their
/// own code comments) — but that must not mean it is untested. This drives
/// it through the real `delete_coral` → `run_cascade` →
/// `record_retraction_audit` → `record_best_effort` path with a REAL
/// control pool, proving the System-actor write path itself works even
/// though nothing wires it up today.
#[tokio::test]
async fn finding_9_record_best_effort_is_actually_exercised() {
    let pool = skip_without_db!();
    let (account_id, _) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let (atom_id, _) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-best-effort", Utc::now()).await;

    let outcome = delete_coral(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        atom_id,
        Actor::System,
        Some(&pool),
    )
    .await
    .expect("delete_coral (System actor, real control_pool) succeeds");
    assert!(!outcome.atoms_deleted.is_empty());

    let rows: Vec<(Uuid, Uuid, Uuid, Uuid, String, String, String)> = sqlx::query_as(
        "SELECT account_id,graph_id,atom_id,retraction_id,actor_kind,surface,outcome \
         FROM retraction_audit_log WHERE atom_id=$1 ORDER BY surface",
    ).bind(atom_id).fetch_all(&pool).await.unwrap();
    assert_eq!(rows, vec![
        (account_id, graph_id, atom_id, outcome.retraction_id, "system".into(), "content".into(), "removed".into()),
        (account_id, graph_id, atom_id, outcome.retraction_id, "system".into(), "journal_rollup".into(), "removed".into()),
    ], "each distinct deletion surface must be audited exactly once with the correct scope and retraction");

}

// ── Findings 2/5: deletion audit survives asynchronous blob failure ──────

/// PR1210 makes ciphertext erasure durable and asynchronous after the SQL
/// delete. A cleanup failure must retain every committed atom's audit row,
/// keep the failed object queued for retry, and converge to physical erasure.
#[tokio::test]
async fn findings_2_5_blob_cleanup_failure_preserves_committed_audit_and_retries() {
    let db = "b2_blob_retry_audit";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
    let Some(pool) = cleanup_scratch_pool(db).await else { return; };
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let now = Utc::now();
    let subject = seed_node(&pool, graph_id, "entity", "Someone").await;
    let (a1, k1) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-1", now).await;
    let (a2, k2) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-2", now).await;
    let (a3, k3) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-3", now).await;
    for id in [a1, a2, a3] {
        link(&pool, subject, id).await;
    }
    blobs.fail_delete_on_attempt(2);

    let outcome = delete_subject(
        &pool, Some(&blobs), "test-device", graph_id, subject, "owner", user_id, Some(&pool),
    ).await.expect("SQL deletion must commit independently of later blob transport failures");
    assert_eq!(outcome.atoms_deleted.len(), 3);
    for id in [a1, a2, a3] {
        assert!(!atom_exists(&pool, id).await);
        let audit_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM retraction_audit_log WHERE atom_id=$1 AND surface='content' AND outcome='removed'",
        ).bind(id).fetch_one(&pool).await.unwrap();
        assert_eq!(audit_rows, 1, "every committed deletion must already be audited");
    }
    assert_eq!(blobs.delete_attempts.load(Ordering::SeqCst), 0);
    assert!(blobs.contains(&k1) && blobs.contains(&k2) && blobs.contains(&k3));
    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='DeleteSourceBlob' AND account_id=$1 AND status='pending'",
    ).bind(account_id).fetch_one(&pool).await.unwrap();
    assert_eq!(queued, 3, "every deleted atom must have durable ciphertext cleanup");

    assert_eq!(drain_blob_cleanup(&pool, &blobs).await, 3);
    let failed: (Uuid, String, i32, String, bool) = sqlx::query_as(
        "SELECT id,payload->>'blob_key',attempts,error_message,run_after>now() FROM jobs WHERE kind='DeleteSourceBlob' AND status='pending'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(failed.2, 1);
    assert_eq!(failed.3, "source blob deletion failed");
    assert!(failed.4, "transport failure must schedule backoff instead of reporting completion");
    assert!(blobs.contains(&failed.1));
    assert_eq!([&k1, &k2, &k3].iter().filter(|key| blobs.contains(key)).count(), 1);
    let completed: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='DeleteSourceBlob' AND status='done'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(completed, 2);
    let audits: i64 = sqlx::query_scalar("SELECT count(*) FROM retraction_audit_log WHERE graph_id=$1 AND surface='content' AND outcome='removed'")
        .bind(graph_id).fetch_one(&pool).await.unwrap();
    assert_eq!(audits, 3, "worker failure must not erase the committed deletion audit");
    assert_eq!(drain_blob_cleanup(&pool, &blobs).await, 0, "backoff prevents an immediate retry");

    // Advance only this test's due time; use the real queue claim/retry path.
    sqlx::query("UPDATE jobs SET run_after=now() WHERE id=$1").bind(failed.0).execute(&pool).await.unwrap();
    assert_eq!(drain_blob_cleanup(&pool, &blobs).await, 1);
    assert!(!blobs.contains(&k1) && !blobs.contains(&k2) && !blobs.contains(&k3));
    let final_job: (String, i32, Option<String>) = sqlx::query_as("SELECT status::text,attempts,error_message FROM jobs WHERE id=$1")
        .bind(failed.0).fetch_one(&pool).await.unwrap();
    assert_eq!(final_job, ("done".to_string(), 1, None));
    let audits: i64 = sqlx::query_scalar("SELECT count(*) FROM retraction_audit_log WHERE graph_id=$1 AND surface='content' AND outcome='removed'")
        .bind(graph_id).fetch_one(&pool).await.unwrap();
    assert_eq!(audits, 3, "retry must preserve the original audit without duplicates");
    drop_cleanup_scratch(pool, db).await;
}

// ── Journal rollups: synchronous all-age invalidation ─────────────────────

/// Lifecycle invalidation removes stale journal prose at every age. Prove
/// real rows vanish before returning, while another graph's prose survives.
#[tokio::test]
async fn journal_rollups_are_invalidated_synchronously_at_every_age() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let independent_graph = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    // 0317 invalidates only the deleted atom's user and canonical UTC period.
    let now_hour = chrono::DateTime::<Utc>::from_timestamp(
        Utc::now().timestamp().div_euclid(3600) * 3600, 0,
    ).expect("canonical UTC hour");
    let old_ts = now_hour - ChronoDuration::days(30);
    let (old_atom, _) = seed_atom(&pool, &blobs, graph_id, "slack", "old-1", old_ts).await;
    let fresh_ts = now_hour - ChronoDuration::days(1);
    let (fresh_atom, _) = seed_atom(&pool, &blobs, graph_id, "slack", "fresh-1", fresh_ts).await;
    let attributed = sqlx::query("UPDATE atoms SET ingested_by=$1 WHERE graph_id=$2 AND atom_id=ANY($3)")
        .bind(user_id).bind(graph_id).bind(vec![old_atom,fresh_atom])
        .execute(&pool).await.unwrap();
    assert_eq!(attributed.rows_affected(), 2, "attribute exactly the two journal source atoms");
    let owners: Vec<Uuid> = sqlx::query_scalar("SELECT ingested_by FROM atoms WHERE graph_id=$1 AND atom_id=ANY($2)")
        .bind(graph_id).bind(vec![old_atom,fresh_atom]).fetch_all(&pool).await.unwrap();
    assert_eq!(owners, vec![user_id,user_id], "both source atoms belong to the journal user");
    let store = PostgresStore::new(pool.clone());
    store.upsert_journal_rollup(account_id,user_id,independent_graph,old_ts,
        "Independent graph must survive.",serde_json::json!({}),"independent-fingerprint","fixture").await.unwrap();

    for (atom, at, age) in [(old_atom,old_ts,"old"),(fresh_atom,fresh_ts,"fresh")] {
        store.upsert_journal_rollup(account_id,user_id,graph_id,at,
            "Sensitive journal prose.",serde_json::json!({}),"sensitive-fingerprint","fixture").await.unwrap();
        let before: String = sqlx::query_scalar("SELECT summary FROM journal_rollups WHERE graph_id=$1 AND user_id=$2 AND hour_start=$3")
            .bind(graph_id).bind(user_id).bind(at).fetch_one(&pool).await.unwrap();
        assert_eq!(before, "Sensitive journal prose.", "{age}: the invalidation target must actually exist");
        let outcome = delete_coral(&pool,Some(&blobs),"test-device",graph_id,atom,Actor::System,None)
            .await.expect("delete_coral succeeds independently of age");
        assert_eq!(outcome.atoms_deleted, vec![(graph_id,atom)]);
        let journal: Vec<_> = outcome.surface_outcomes.iter().filter(|o| o.surface=="journal_rollup").collect();
        assert_eq!(journal.len(), 1, "{age}: exactly one journal surface outcome per deleted atom");
        assert_eq!(journal[0].outcome, retract_outcome::REMOVED, "{age}: no retired out_of_window exemption");
        assert_eq!((journal[0].graph_id,journal[0].atom_id), (graph_id,Some(atom)));
        let after: i64 = sqlx::query_scalar("SELECT count(*) FROM journal_rollups WHERE graph_id=$1")
            .bind(graph_id).fetch_one(&pool).await.unwrap();
        assert_eq!(after, 0, "{age}: synchronous invalidation must erase actual persisted prose");
        let independent: Vec<String> = sqlx::query_scalar("SELECT summary FROM journal_rollups WHERE graph_id=$1")
            .bind(independent_graph).fetch_all(&pool).await.unwrap();
        assert_eq!(independent, vec!["Independent graph must survive.".to_string()], "{age}: invalidation must stay in the deleted graph");
    }
}

// ── FM-5 (a): the reef_judgement/node_merges compounding case ──────────────

/// FM-5's probe (a): grep `unmerge|node_merges|reef_judgement|annul` across
/// both services finds zero hits (unchanged by this PR — not one of D5's
/// four fixes). This test is the WRITTEN DISPOSITION the unit file requires
/// in place of a fix: a live `node_merges` row survives retraction of the
/// corals it names, UNCHANGED — the compounding gap is real, and this pins
/// it so a future accidental "fix" is a visible diff, not a silent one.
#[tokio::test]
async fn fm5_probe_a_node_merges_compounding_case_is_untouched_written_disposition() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;
    let blobs = FlakyBlobs::default();
    let (atom_id, _) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-1", Utc::now()).await;
    let keep_id = Uuid::new_v4();
    let dup_id = Uuid::new_v4();
    let merge_id: Uuid = sqlx::query_scalar(
        "INSERT INTO node_merges (graph_id, keep_id, dup_id, dup_snapshot) \
         VALUES ($1, $2, $3, '{}'::jsonb) RETURNING merge_id",
    )
    .bind(graph_id)
    .bind(keep_id)
    .bind(dup_id)
    .fetch_one(&pool)
    .await
    .expect("seed node_merges");
    let _ = user_id;

    delete_coral(&pool, Some(&blobs), "test-device", graph_id, atom_id, Actor::System, None)
        .await
        .expect("delete_coral succeeds");

    let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM node_merges WHERE merge_id = $1")
        .bind(merge_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        still_there, 1,
        "written disposition: node_merges is NOT reached by retract (accepted gap, FM-5 probe (a) — \
         not fixed by D5's four fixes)"
    );
}

// ── FM-5 (b): the manifest CHECK rejects an atom body ───────────────────────

/// FM-5's probe (b): attempt to insert an atom body into `manifest` and have
/// the CHECK reject it.
#[tokio::test]
async fn fm5_probe_b_manifest_check_rejects_a_non_allowlisted_key() {
    let pool = skip_without_db!();
    let (account_id, user_id) = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "team").await;

    // The allowlisted shape succeeds — a real UUID array (finding 8: the
    // shape CHECK requires bare UUID strings, not just "an array"; "abc"
    // used to sit here and would now be rejected by
    // `retraction_audit_log_manifest_survivors_shape`).
    let ok = sqlx::query(
        "INSERT INTO retraction_audit_log \
            (retraction_id, account_id, actor_kind, actor_user_id, source_type, source_id, \
             atom_id, graph_id, is_personal_ocean, surface, outcome, manifest) \
         VALUES ($1, $2, 'user', $3, 'slack', 'msg-1', $4, $5, false, 'content', 'removed', \
                 jsonb_build_object('survivors', jsonb_build_array($6::text)))",
    )
    .bind(Uuid::new_v4())
    .bind(account_id)
    .bind(user_id)
    .bind(Uuid::new_v4())
    .bind(graph_id)
    .bind(Uuid::new_v4().to_string())
    .execute(&pool)
    .await;
    assert!(ok.is_ok(), "the allowlisted `survivors` key, shaped as a UUID array, must be accepted: {ok:?}");

    // An atom body under a non-allowlisted key must be REJECTED.
    let rejected = sqlx::query(
        "INSERT INTO retraction_audit_log \
            (retraction_id, account_id, actor_kind, actor_user_id, source_type, source_id, \
             atom_id, graph_id, is_personal_ocean, surface, outcome, manifest) \
         VALUES ($1, $2, 'user', $3, 'slack', 'msg-1', $4, $5, false, 'content', 'removed', \
                 '{\"body\": \"the fee is $48,500\"}'::jsonb)",
    )
    .bind(Uuid::new_v4())
    .bind(account_id)
    .bind(user_id)
    .bind(Uuid::new_v4())
    .bind(graph_id)
    .execute(&pool)
    .await;
    assert!(
        rejected.is_err(),
        "FM-5 probe (b): a manifest key outside the allowlist must be REJECTED by the CHECK, \
         not silently accepted"
    );
    let msg = rejected.unwrap_err().to_string();
    assert!(
        msg.contains("retraction_audit_log_manifest_metadata_only"),
        "expected the metadata-only CHECK to be the failure, got: {msg}"
    );

    // Finding 8 (CRITICAL): a content STRING under the ALLOWLISTED key
    // itself must ALSO be rejected — the key-name CHECK above only proves
    // `manifest` can't carry an unexpected KEY; it says nothing about the
    // SHAPE of the value under `survivors`, which is exactly where
    // `{"survivors": "<atom body>"}` slips through un-tested.
    let content_under_allowlisted_key = sqlx::query(
        "INSERT INTO retraction_audit_log             (retraction_id, account_id, actor_kind, actor_user_id, source_type, source_id,              atom_id, graph_id, is_personal_ocean, surface, outcome, manifest)          VALUES ($1, $2, 'user', $3, 'slack', 'msg-1', $4, $5, false, 'content', 'removed',                  jsonb_build_object('survivors', 'the fee is $48,500 — the retracted body'))",
    )
    .bind(Uuid::new_v4())
    .bind(account_id)
    .bind(user_id)
    .bind(Uuid::new_v4())
    .bind(graph_id)
    .execute(&pool)
    .await;
    assert!(
        content_under_allowlisted_key.is_err(),
        "finding 8: a content string under the ALLOWLISTED `survivors` key must be REJECTED —          the key-name CHECK alone is not enough"
    );
    let msg2 = content_under_allowlisted_key.unwrap_err().to_string();
    assert!(
        msg2.contains("retraction_audit_log_manifest_survivors_shape"),
        "expected the survivors-shape CHECK to be the failure, got: {msg2}"
    );

    // The mirror: a JSON ARRAY under `survivors`, but one whose element is
    // NOT a bare UUID (an obvious content string instead) — the array-typed
    // check alone would not catch this; the per-element UUID check must.
    let content_in_array_element = sqlx::query(
        "INSERT INTO retraction_audit_log             (retraction_id, account_id, actor_kind, actor_user_id, source_type, source_id,              atom_id, graph_id, is_personal_ocean, surface, outcome, manifest)          VALUES ($1, $2, 'user', $3, 'slack', 'msg-1', $4, $5, false, 'content', 'removed',                  jsonb_build_object('survivors', jsonb_build_array('the fee is $48,500')))",
    )
    .bind(Uuid::new_v4())
    .bind(account_id)
    .bind(user_id)
    .bind(Uuid::new_v4())
    .bind(graph_id)
    .execute(&pool)
    .await;
    assert!(
        content_in_array_element.is_err(),
        "finding 8: an array element that is not a bare UUID must be REJECTED too"
    );
}

// ── FM-5 (c): append-only via REVOKE, not RLS ───────────────────────────────

const APP_ROLE_TEST_PASSWORD: &str = "b2_retraction_audit_app_role_test_only_pw";
const SETUP_ADVISORY_LOCK_KEY: i64 = 0x0B2_5C10;

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

/// FM-5's probe (c): attempt a `DELETE` on `retraction_audit_log` AS THE APP
/// ROLE (`coralswarm_app`, which `0083_consumer_rls.sql:382` grants DELETE
/// on everything by default) and have the migration's explicit `REVOKE`
/// refuse it with a hard `permission denied` error — never a silent
/// zero-rows success. A real login as the role, not `SET ROLE` inside the
/// owner's session (`consumer_rls_pg.rs`/`retraction_deletion_rls_pg.rs`'s
/// own established pattern) — the same production connection path
/// `DATABASE_URL` uses.
#[tokio::test]
async fn fm5_probe_c_delete_as_app_role_is_refused_by_the_revoke() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "b2_retraction_audit_revoke_wt";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(SETUP_ADVISORY_LOCK_KEY)
        .execute(&mut admin_conn)
        .await
        .expect("take setup advisory lock");

    let owner: PgPool = PgPoolOptions::new().connect(&db_url).await.expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0196");
    sqlx::query(&format!("ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"))
        .execute(&owner)
        .await
        .expect("set test-only password on coralswarm_app");

    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(SETUP_ADVISORY_LOCK_KEY)
        .execute(&mut admin_conn)
        .await
        .expect("release setup advisory lock");

    let (account_id, user_id) = seed_account(&owner).await;
    let graph_id = seed_graph_in(&owner, account_id, "team").await;
    let row_id: Uuid = sqlx::query_scalar(
        "INSERT INTO retraction_audit_log \
            (retraction_id, account_id, actor_kind, actor_user_id, source_type, source_id, \
             atom_id, graph_id, is_personal_ocean, surface, outcome, manifest) \
         VALUES ($1, $2, 'user', $3, 'slack', 'msg-1', $4, $5, false, 'content', 'removed', '{}'::jsonb) \
         RETURNING id",
    )
    .bind(Uuid::new_v4())
    .bind(account_id)
    .bind(user_id)
    .bind(Uuid::new_v4())
    .bind(graph_id)
    .fetch_one(&owner)
    .await
    .expect("seed one row as owner");

    let app_pool = app_role_pool(&db_url).await;

    // The load-bearing check this whole test exists to cover: `record()`
    // must actually be able to WRITE a row as the real, non-BYPASSRLS
    // `coralswarm_app` role — a bare `pool.begin()` (no `app.account_id`/
    // `app.user_id` GUCs set) would either hard-error on the strict
    // `current_setting('app.account_id')` or be silently refused by the
    // INSERT policy's `actor_user_id = app.user_id` check. Also proves the
    // author-visibility half of D8's RLS SELECT policy end-to-end: the
    // SAME actor reads their own just-written row back, under the SAME
    // app-role connection.
    let outcomes = vec![SurfaceOutcome {
        graph_id,
        is_personal_ocean: false,
        source_type: "slack".to_string(),
        source_id: "msg-app-role".to_string(),
        atom_id: Some(Uuid::new_v4()),
        surface: "content",
        outcome: retract_outcome::REMOVED,
        survivors: vec![],
    }];
    let written_retraction_id = Uuid::new_v4();
    retraction_audit::record(&app_pool, account_id, Actor::User(user_id), written_retraction_id, &outcomes)
        .await
        .expect("record() must succeed as the real coralswarm_app role");
    // Finding 13: this binding is named for what it PROVES (default-deny),
    // not for what a careless reader might expect ("the author can read
    // their own row") — the assertion right below requires ZERO, on
    // purpose: an unscoped read (no `app.user_id` set) must see NOTHING.
    // The scoped positive counterpart is the very next block.
    let visible_unscoped_default_deny: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM retraction_audit_log WHERE retraction_id = $1",
    )
    .bind(written_retraction_id)
    .fetch_one(&app_pool)
    .await
    .expect("an unscoped read must succeed as a query — it must just see zero rows");
    assert_eq!(
        visible_unscoped_default_deny, 0,
        "RLS default-deny: an UNSCOPED read (no app.user_id set) must see NOTHING here, proving \
         the follow-up SCOPED read below is a real positive check, not a vacuous one. A future \
         maintainer who is tempted to \"fix\" this assertion to 1 would be deleting the \
         default-deny proof this test exists to make."
    );

    // D8's author-visibility clause, positively: the SAME author, properly
    // scoped (app.user_id set), DOES see the row.
    let mut author_tx =
        coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app_pool, account_id, user_id)
            .await
            .expect("begin_tenant_scoped as coralswarm_app");
    let visible_scoped: i64 =
        sqlx::query_scalar("SELECT count(*) FROM retraction_audit_log WHERE retraction_id = $1")
            .bind(written_retraction_id)
            .fetch_one(&mut *author_tx)
            .await
            .expect("scoped author read");
    author_tx.rollback().await.ok();
    assert_eq!(
        visible_scoped, 1,
        "D8: the author, properly scoped, must be able to read the row they just wrote"
    );

    let delete_result = sqlx::query("DELETE FROM retraction_audit_log WHERE id = $1")
        .bind(row_id)
        .execute(&app_pool)
        .await;
    assert!(
        delete_result.is_err(),
        "FM-5 probe (c): DELETE as coralswarm_app must be REFUSED by the REVOKE, not silently \
         succeed. A migration that runs is not evidence this holds."
    );
    let msg = delete_result.unwrap_err().to_string();
    assert!(
        msg.to_lowercase().contains("permission denied"),
        "expected a hard permission-denied error, got: {msg}"
    );

    // The row must genuinely still be there — the failure is real, not
    // merely an error string with the delete having gone through anyway.
    let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM retraction_audit_log WHERE id = $1")
        .bind(row_id)
        .fetch_one(&owner)
        .await
        .unwrap();
    assert_eq!(still_there, 1, "the row must survive the refused DELETE");

    // Same probe for UPDATE — append-only means neither command is granted.
    let update_result = sqlx::query("UPDATE retraction_audit_log SET outcome = 'out_of_window' WHERE id = $1")
        .bind(row_id)
        .execute(&app_pool)
        .await;
    assert!(
        update_result.is_err(),
        "append-only: UPDATE as coralswarm_app must also be refused"
    );

    // Finding 13: the module doc promises this test "creates/migrates/
    // tears down itself" — this is the teardown half, previously missing
    // (the DB was left behind for the NEXT run's `recreate()` to clean up,
    // same as `retraction_deletion_rls_pg.rs`'s own convention — but THIS
    // file's own header claims the stronger property, so make it true).
    app_pool.close().await;
    owner.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}
