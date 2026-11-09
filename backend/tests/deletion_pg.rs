//! Live tests for `services::deletion` (§7.3 of
//! docs/plans/graph-temporal-memory.md) against a REAL Postgres — each of the
//! three entry points, the coral-less-reef rule actually firing, idempotency
//! / retry convergence, cross-ocean propagation by `(source_type,
//! source_id)`, the account boundary propagation must NOT cross, the
//! `subject` entry point's stronger role gate, and the R2-vs-Postgres
//! rollback path (both with an injected in-memory failure and, when the env
//! vars are set, against a REAL local S3-compatible fake — MinIO).
//!
//! Mirrors `backend/tests/retraction_pg.rs`'s structure closely (same seed
//! helpers, same `FlakyBlobs` shape) — deletion reuses §4.9's exact
//! R2-vs-Postgres ordering, so the same proof shape applies.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied, including `0111_promotion_identity_and_source_publications`); a
//! normal `cargo test` with no DB configured skips it, same convention as
//! `retraction_pg.rs` / `promote_node_corals_pg.rs`.
//!
//! The real-MinIO test additionally requires `R2_TEST_ENDPOINT_OVERRIDE` /
//! `R2_TEST_ACCESS_KEY` / `R2_TEST_SECRET_KEY` / `R2_TEST_BUCKET` (see
//! `r2_extract_success_pg.rs`'s module doc for the docker commands) and skips
//! independently of the rest of this file when unset.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/graph_deletion_wt \
//!     cargo test -p coralswarm-backend --test deletion_pg -- --nocapture
//!
//!   # Plus the real-blob test:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/graph_deletion_wt \
//!   R2_TEST_ENDPOINT_OVERRIDE=http://localhost:19000 \
//!   R2_TEST_ACCESS_KEY=minioadmin R2_TEST_SECRET_KEY=minioadmin R2_TEST_BUCKET=cs-test-bucket \
//!     cargo test -p coralswarm-backend --test deletion_pg -- --nocapture

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use coralswarm_backend::jobs::queue::JobRow;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::source_blob_cleanup;
use coralswarm_backend::services::connector_audit::Actor;
use coralswarm_backend::services::deletion::{
    delete_coral, delete_source, delete_subject, DeletionError,
};
use coralswarm_backend::services::blob_store::PromotionBlobStore;
use coralswarm_core::r2::{R2Client, R2Error};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

fn test_db_url() -> Option<String> {
    std::env::var("RELINFER_TEST_DATABASE_URL").ok()
}

async fn connect() -> Option<PgPool> {
    let url = test_db_url()?;
    Some(
        PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .expect("connect"),
    )
}

// ── In-memory blob double, identical shape to retraction_pg.rs's ────────────

#[derive(Default)]
struct FlakyBlobs {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    fail_deletes: AtomicBool,
    delete_attempts: AtomicUsize,
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
    fn delete_attempt_count(&self) -> usize {
        self.delete_attempts.load(Ordering::SeqCst)
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
    async fn put_blob(
        &self,
        blob_key: &str,
        content: &[u8],
        _dek: &[u8; 32],
    ) -> Result<(), R2Error> {
        self.put(blob_key, content);
        Ok(())
    }
    async fn delete_blob(&self, blob_key: &str) -> Result<(), R2Error> {
        self.delete_attempts.fetch_add(1, Ordering::SeqCst);
        if self.fail_deletes.load(Ordering::SeqCst) {
            return Err(R2Error::S3("injected delete failure".into()));
        }
        self.objects.lock().unwrap().remove(blob_key);
        Ok(())
    }
}

// ── Seed helpers ──────────────────────────────────────────────────────────────

async fn seed_user(pool: &PgPool) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .expect("insert user")
}

async fn seed_account(pool: &PgPool) -> Uuid {
    let user_id = seed_user(pool).await;
    sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
        .bind(user_id)
        .fetch_one(pool)
        .await
        .expect("insert account")
}

async fn seed_graph_in(pool: &PgPool, account_id: Uuid, name: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, $2, 'team') RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("insert graph")
}

/// Seed one atom, with a blob object also seeded into `blobs`. Returns
/// `(atom_id, blob_key)`.
async fn seed_atom(
    pool: &PgPool,
    blobs: &FlakyBlobs,
    graph_id: Uuid,
    source_type: &str,
    source_id: &str,
) -> (Uuid, String) {
    let atom_id = Uuid::new_v4();
    let blob_key = format!("{graph_id}/{atom_id}");
    blobs.put(&blob_key, b"sensitive ingested content");
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, \"timestamp\", scope, fts_language, header_len) \
         VALUES ($1, $2, $3, $4, $5, '{}', now(), 'team', 'english', 0)",
    )
    .bind(atom_id)
    .bind(graph_id)
    .bind(source_type)
    .bind(source_id)
    .bind(&blob_key)
    .execute(pool)
    .await
    .expect("insert atom");
    (atom_id, blob_key)
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

async fn node_exists(pool: &PgPool, node_id: Uuid) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM nodes WHERE node_id = $1")
        .bind(node_id)
        .fetch_one(pool)
        .await
        .unwrap()
        > 0
}
async fn atom_exists(pool: &PgPool, atom_id: Uuid) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(pool)
        .await
        .unwrap()
        > 0
}

async fn assert_blob_cleanup_queued(pool:&PgPool,key:&str) {
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='DeleteSourceBlob' AND payload->>'blob_key'=$1 AND status='pending'")
        .bind(key).fetch_one(pool).await.unwrap();
    assert_eq!(count,1,"committed deletion must durably schedule ciphertext cleanup");
}

macro_rules! skip_without_db {
    () => {
        match connect().await {
            Some(pool) => pool,
            None => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
                return;
            }
        }
    };
}

// ── 1. Coral entry point: happy path, sibling survives ───────────────────────

#[tokio::test]
async fn delete_coral_removes_one_atom_and_leaves_its_sibling_and_reef_intact() {
    let pool = skip_without_db!();
    let account_id = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "Ocean").await;
    let blobs = FlakyBlobs::default();
    let node_id = seed_node(&pool, graph_id, "entity", "Two-Coral Reef").await;
    let (a1, k1) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-1").await;
    let (a2, k2) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-2").await;
    link(&pool, node_id, a1).await;
    link(&pool, node_id, a2).await;

    let outcome = delete_coral(&pool, Some(&blobs), "test-device", graph_id, a1, Actor::System, None)
        .await
        .expect("delete_coral succeeds");
    assert_eq!(outcome.atoms_deleted, vec![(graph_id, a1)]);
    assert!(
        outcome.deleted_reef_ids.is_empty(),
        "the reef still has one coral left"
    );

    assert!(!atom_exists(&pool, a1).await);
    assert_blob_cleanup_queued(&pool,&k1).await;
    assert!(blobs.contains(&k1),"ciphertext cleanup happens after commit");
    assert!(atom_exists(&pool, a2).await, "sibling coral must survive");
    assert!(blobs.contains(&k2));
    assert!(node_exists(&pool, node_id).await);
}

// ── 2. Coral-less-reef rule actually firing ──────────────────────────────────

#[tokio::test]
async fn delete_coral_on_the_last_coral_deletes_the_now_coral_less_reef() {
    let pool = skip_without_db!();
    let account_id = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "Ocean").await;
    let blobs = FlakyBlobs::default();
    let node_id = seed_node(&pool, graph_id, "entity", "One-Coral Reef").await;
    let (a1, k1) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-1").await;
    link(&pool, node_id, a1).await;

    let outcome = delete_coral(&pool, Some(&blobs), "test-device", graph_id, a1, Actor::System, None)
        .await
        .expect("delete_coral succeeds");
    assert_eq!(outcome.atoms_deleted, vec![(graph_id, a1)]);
    assert_eq!(
        outcome.deleted_reef_ids,
        vec![(graph_id, node_id)],
        "the reef's last coral is gone — the reef itself must be deleted too (nodes has no \
         FK to atoms, so it would otherwise survive its own evidence)"
    );
    assert!(!atom_exists(&pool, a1).await);
    assert_blob_cleanup_queued(&pool,&k1).await;
    assert!(blobs.contains(&k1),"ciphertext cleanup happens after commit");
    assert!(
        !node_exists(&pool, node_id).await,
        "a natively-derived reef left coral-less must actually be deleted"
    );
}

// ── 3. Idempotency / retry convergence ───────────────────────────────────────

#[tokio::test]
async fn delete_coral_is_idempotent_on_retry() {
    let pool = skip_without_db!();
    let account_id = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "Ocean").await;
    let blobs = FlakyBlobs::default();
    let node_id = seed_node(&pool, graph_id, "entity", "Reef").await;
    let (a1, _k1) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-1").await;
    link(&pool, node_id, a1).await;

    let first = delete_coral(&pool, Some(&blobs), "test-device", graph_id, a1, Actor::System, None)
        .await
        .expect("first delete succeeds");
    assert_eq!(first.atoms_deleted, vec![(graph_id, a1)]);
    assert_eq!(first.deleted_reef_ids, vec![(graph_id, node_id)]);

    let second = delete_coral(&pool, Some(&blobs), "test-device", graph_id, a1, Actor::System, None)
        .await
        .expect("retrying an already-deleted atom must succeed, not error");
    assert!(
        second.atoms_deleted.is_empty(),
        "nothing left to delete on the idempotent retry"
    );
    assert!(second.deleted_reef_ids.is_empty());

    // An id that was never ingested at all behaves identically.
    let never_existed = delete_coral(&pool, Some(&blobs), "test-device", graph_id, Uuid::new_v4(), Actor::System, None)
        .await
        .expect("deleting an unknown atom id must succeed as a no-op");
    assert!(never_existed.atoms_deleted.is_empty());
}

// ── 4/5. R2-vs-Postgres rollback + retry convergence with an injected failure ─

#[tokio::test]
async fn unavailable_blob_transport_does_not_lose_durable_cleanup() {
    let pool=skip_without_db!();
    let account=seed_account(&pool).await;
    let graph=seed_graph_in(&pool,account,"Ocean").await;
    let blobs=FlakyBlobs::default();
    let node=seed_node(&pool,graph,"entity","Flaky Reef").await;
    let (atom,key)=seed_atom(&pool,&blobs,graph,"slack","msg-1").await;
    link(&pool,node,atom).await;
    blobs.fail_deletes.store(true,Ordering::SeqCst);
    let outcome=delete_coral(&pool,Some(&blobs),"test-device",graph,atom,Actor::System,None)
        .await.expect("logical deletion durably schedules remote cleanup");
    assert_eq!(outcome.atoms_deleted,vec![(graph,atom)]);
    assert_eq!(blobs.delete_attempt_count(),0,"no irreversible external call before database commit");
    assert!(!atom_exists(&pool,atom).await);
    assert!(!node_exists(&pool,node).await);
    assert!(blobs.contains(&key));
    assert_blob_cleanup_queued(&pool,&key).await;
    let again=delete_coral(&pool,Some(&blobs),"test-device",graph,atom,Actor::System,None).await.unwrap();
    assert!(again.atoms_deleted.is_empty());
    assert_blob_cleanup_queued(&pool,&key).await;
}

// ── 6. Source entry point: every copy of one ingested event, one call ───────

#[tokio::test]
async fn delete_source_removes_every_atom_sharing_source_identity_in_the_account() {
    let pool = skip_without_db!();
    let account_id = seed_account(&pool).await;
    let origin = seed_graph_in(&pool, account_id, "Personal").await;
    let sibling = seed_graph_in(&pool, account_id, "Team").await;
    let blobs = FlakyBlobs::default();

    // The original ingest, plus a same-account "promoted copy" sharing the
    // identical (source_type, source_id) natural key (mirrors
    // services::promotion's own copy shape — new atom_id, same pair).
    let (original, k_orig) = seed_atom(&pool, &blobs, origin, "slack", "ts-123").await;
    let (copy, k_copy) = seed_atom(&pool, &blobs, sibling, "slack", "ts-123").await;
    // An unrelated atom in the sibling graph must NOT be touched.
    let (unrelated, k_unrelated) = seed_atom(&pool, &blobs, sibling, "slack", "ts-999").await;

    let outcome = delete_source(
        &pool,
        Some(&blobs),
        "test-device",
        origin,
        "slack",
        "ts-123",
        Actor::System,
        None,
    )
    .await
    .expect("delete_source succeeds");

    let mut deleted: Vec<Uuid> = outcome.atoms_deleted.iter().map(|(_, a)| *a).collect();
    deleted.sort();
    let mut expected = vec![original, copy];
    expected.sort();
    assert_eq!(
        deleted, expected,
        "both copies of the source event are gone"
    );
    assert_eq!(
        outcome.propagated_graph_ids,
        vec![sibling],
        "the sibling graph is reported as touched by propagation"
    );

    assert!(!atom_exists(&pool, original).await);
    assert!(!atom_exists(&pool, copy).await);
    assert_blob_cleanup_queued(&pool,&k_orig).await;
    assert!(blobs.contains(&k_orig),"ciphertext cleanup happens after commit");
    assert_blob_cleanup_queued(&pool,&k_copy).await;
    assert!(blobs.contains(&k_copy),"ciphertext cleanup happens after commit");
    assert!(
        atom_exists(&pool, unrelated).await,
        "an unrelated atom in the SAME sibling graph must survive"
    );
    assert!(blobs.contains(&k_unrelated));
}

// ── 7. Propagation crosses graphs but NEVER accounts ─────────────────────────

#[tokio::test]
async fn propagation_does_not_cross_into_a_different_account() {
    let pool = skip_without_db!();
    let account_a = seed_account(&pool).await;
    let account_b = seed_account(&pool).await;
    let graph_a = seed_graph_in(&pool, account_a, "Ocean A").await;
    let graph_b = seed_graph_in(&pool, account_b, "Ocean B").await;
    let blobs = FlakyBlobs::default();

    // Same (source_type, source_id) pair, but in TWO DIFFERENT ACCOUNTS —
    // this must never happen from real promotion (which is same-account or
    // explicitly cross-DB-org), but the propagation query itself must be
    // proven not to reach across the account boundary even if it did.
    let (atom_a, key_a) = seed_atom(&pool, &blobs, graph_a, "notion", "doc-1").await;
    let (atom_b, key_b) = seed_atom(&pool, &blobs, graph_b, "notion", "doc-1").await;

    let outcome = delete_coral(&pool, Some(&blobs), "test-device", graph_a, atom_a, Actor::System, None)
        .await
        .expect("delete_coral succeeds");
    assert_eq!(outcome.atoms_deleted, vec![(graph_a, atom_a)]);
    assert!(
        outcome.propagated_graph_ids.is_empty(),
        "a same-looking source_id in a DIFFERENT account must not be reached"
    );

    assert!(!atom_exists(&pool, atom_a).await);
    assert_blob_cleanup_queued(&pool,&key_a).await;
    assert!(blobs.contains(&key_a),"ciphertext cleanup happens after commit");
    assert!(
        atom_exists(&pool, atom_b).await,
        "the other account's atom must be completely untouched"
    );
    assert!(blobs.contains(&key_b));
}

// ── 8. Subject entry point: every linked coral, plus the reef itself ────────

#[tokio::test]
async fn delete_subject_removes_every_linked_coral_and_then_the_person_reef_itself() {
    let pool = skip_without_db!();
    let account_id = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "Ocean").await;
    let blobs = FlakyBlobs::default();
    let subject = seed_node(&pool, graph_id, "entity", "Chris Marchetti").await;
    let (a1, k1) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-1").await;
    let (a2, k2) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-2").await;
    link(&pool, subject, a1).await;
    link(&pool, subject, a2).await;
    // An atom in an unrelated reef must not be touched.
    let other_node = seed_node(&pool, graph_id, "entity", "Someone Else").await;
    let (a3, k3) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-3").await;
    link(&pool, other_node, a3).await;

    let outcome = delete_subject(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        subject,
        "owner",
        Uuid::new_v4(),
        None,
    )
    .await
    .expect("delete_subject succeeds for an owner caller");

    let mut deleted: Vec<Uuid> = outcome.atoms_deleted.iter().map(|(_, a)| *a).collect();
    deleted.sort();
    let mut expected = vec![a1, a2];
    expected.sort();
    assert_eq!(deleted, expected);
    assert_eq!(
        outcome.deleted_reef_ids,
        vec![(graph_id, subject)],
        "the subject reef itself, left coral-less, must be deleted too — same rule as \
         every other entry point, no special-casing needed"
    );

    assert!(!atom_exists(&pool, a1).await);
    assert!(!atom_exists(&pool, a2).await);
    assert_blob_cleanup_queued(&pool,&k1).await;
    assert!(blobs.contains(&k1),"ciphertext cleanup happens after commit");
    assert_blob_cleanup_queued(&pool,&k2).await;
    assert!(blobs.contains(&k2),"ciphertext cleanup happens after commit");
    assert!(!node_exists(&pool, subject).await);

    assert!(
        atom_exists(&pool, a3).await,
        "a coral in an unrelated reef must survive subject deletion"
    );
    assert!(blobs.contains(&k3));
    assert!(node_exists(&pool, other_node).await);
}

// ── 9. Subject entry point's stronger gate ──────────────────────────────────

#[tokio::test]
async fn delete_subject_refuses_a_plain_member_and_destroys_nothing() {
    let pool = skip_without_db!();
    let account_id = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "Ocean").await;
    let blobs = FlakyBlobs::default();
    let subject = seed_node(&pool, graph_id, "entity", "Someone").await;
    let (a1, k1) = seed_atom(&pool, &blobs, graph_id, "slack", "msg-1").await;
    link(&pool, subject, a1).await;

    let err = delete_subject(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        subject,
        "member",
        Uuid::new_v4(),
        None,
    )
    .await
    .expect_err("a plain member must be refused subject deletion");
    assert!(matches!(err, DeletionError::InsufficientRole));

    // Destroyed NOTHING — the gate runs before any resolution/mutation.
    assert!(atom_exists(&pool, a1).await);
    assert!(blobs.contains(&k1));
    assert!(node_exists(&pool, subject).await);

    // An admin (not just an owner) is sufficient.
    let outcome = delete_subject(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        subject,
        "admin",
        Uuid::new_v4(),
        None,
    )
    .await
    .expect("an admin caller is sufficient");
    assert_eq!(outcome.atoms_deleted, vec![(graph_id, a1)]);
}

#[tokio::test]
async fn delete_subject_refuses_a_non_entity_node() {
    let pool = skip_without_db!();
    let account_id = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "Ocean").await;
    let blobs = FlakyBlobs::default();
    let decision_node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'decision', 'We decided X', \
                 jsonb_build_object('decided_at', now())) RETURNING node_id",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .expect("insert decision node");

    let err = delete_subject(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        decision_node,
        "owner",
        Uuid::new_v4(),
        None,
    )
    .await
    .expect_err("a decision reef is not a valid subject");
    assert!(matches!(err, DeletionError::SubjectNotFound));
    assert!(node_exists(&pool, decision_node).await);
}

// ── 10. Real blob deletion against a local S3-compatible fake (MinIO) ───────
//
// Mirrors r2_extract_success_pg.rs's env-var convention exactly. Skips
// independently of RELINFER_TEST_DATABASE_URL when the R2_TEST_* vars are
// unset — this is the one test in this file that proves the blob delete is a
// REAL network round trip, not merely an in-memory double.

struct R2TestEnv {
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
}

fn r2_test_env() -> Option<R2TestEnv> {
    Some(R2TestEnv {
        endpoint: std::env::var("R2_TEST_ENDPOINT_OVERRIDE").ok()?,
        access_key: std::env::var("R2_TEST_ACCESS_KEY").ok()?,
        secret_key: std::env::var("R2_TEST_SECRET_KEY").ok()?,
        bucket: std::env::var("R2_TEST_BUCKET").ok()?,
    })
}

#[tokio::test]
async fn delete_coral_deletes_the_real_blob_against_a_local_r2_fake() {
    let Some(pool) = connect().await else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!(
            "skipping: set R2_TEST_ENDPOINT_OVERRIDE / R2_TEST_ACCESS_KEY / R2_TEST_SECRET_KEY \
             / R2_TEST_BUCKET (a local MinIO — see r2_extract_success_pg.rs's module doc)"
        );
        return;
    };

    let client = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    let account_id = seed_account(&pool).await;
    let graph_id = seed_graph_in(&pool, account_id, "Ocean").await;
    let atom_id = Uuid::new_v4();
    let data_key = [7u8; 32];

    // A REAL put against the fake, at the atom's authoritative key shape.
    let blob_key = client
        .put_atom_blob(
            None,
            graph_id,
            atom_id,
            b"real R2 payload for deletion_pg",
            &data_key,
        )
        .await
        .expect("put should succeed against the local fake");

    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, \"timestamp\", scope, fts_language, header_len) \
         VALUES ($1, $2, 'note', 'real-blob-test', $3, '{}', now(), 'team', 'english', 0)",
    )
    .bind(atom_id)
    .bind(graph_id)
    .bind(&blob_key)
    .execute(&pool)
    .await
    .expect("insert atom");

    // Confirm the object is really there before we delete it.
    client
        .get_atom_blob(&blob_key, &data_key)
        .await
        .expect("the object must actually exist against the real fake before deletion");

    let blobs: &dyn PromotionBlobStore = &client;
    let outcome = delete_coral(&pool, Some(blobs), "test-device", graph_id, atom_id, Actor::System, None)
        .await
        .expect("delete_coral against a real blob store succeeds");
    assert_eq!(outcome.atoms_deleted, vec![(graph_id, atom_id)]);

    assert!(!atom_exists(&pool, atom_id).await);

    // Physical erasure follows the committed cleanup job. Select this test's
    // persisted envelope so unrelated fixture jobs cannot consume the proof.
    let (id, kind, payload, attempts, owner): (Uuid, String, serde_json::Value, i32, Option<Uuid>) =
        sqlx::query_as("SELECT id,kind,payload,attempts,account_id FROM jobs WHERE kind='DeleteSourceBlob' AND payload->>'blob_key'=$1 AND status='pending'")
            .bind(&blob_key).fetch_one(&pool).await.unwrap();
    assert_eq!(owner, Some(account_id));
    source_blob_cleanup::process_job(&TenantId::Consumer, &pool, Some(blobs), JobRow {
        id, kind, payload, attempts, account_id: owner,
    }).await;
    let status: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id=$1")
        .bind(id).fetch_one(&pool).await.unwrap();
    assert_eq!(status, "done", "real transport erasure must durably finish the job");

    // The REAL network proof: a GET after worker erasure is NotFound.
    let err = client
        .get_atom_blob(&blob_key, &data_key)
        .await
        .expect_err("must be NotFound after a real delete");
    assert!(
        matches!(err, R2Error::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}
