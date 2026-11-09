//! Live tests for `services::retraction` (§4.9 of
//! docs/plans/graph-temporal-memory.md) against a REAL Postgres —
//! idempotency, partial-failure recovery (the R2-vs-Postgres ordering the
//! module doc argues for), the coral-less-reef deletion rule, and the
//! **promoter-only authorization** gate.
//!
//! The authorization cases are the reason this suite exists in its current
//! shape. §4.9 originally shipped with "can reach the graph" authority; it is
//! now "was the original promoter", and the properties that must hold are:
//! the promoter can retract; a non-promoter who CAN reach the ocean cannot;
//! content with no recorded promoter fails CLOSED and destroys nothing; the
//! same-pool and cross-DB org promotion shapes are judged identically; and
//! BOTH §4.9 verbs enforce it (an open second door makes the first pointless).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied, including `0111_promotion_identity_and_source_publications`); a
//! normal `cargo test` with no DB configured skips it, same convention as
//! `promote_node_corals_pg.rs`.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/graph_promo_wt \
//!     cargo test -p coralswarm-backend --test retraction_pg -- --nocapture

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::Utc;
use coralswarm_backend::services::blob_store::PromotionBlobStore;
use coralswarm_backend::services::retraction::{
    revoke_source_publication, unpublish_atom, RetractionError, RevokeOutcome,
};
use coralswarm_core::r2::R2Error;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

fn test_db_url() -> Option<String> {
    std::env::var("RELINFER_TEST_DATABASE_URL").ok()
}

/// In-memory blob store with an on/off failure toggle, so a single instance
/// can prove "fails, rolls back" and then "retried, succeeds" against the
/// SAME underlying object map — exactly the retry-convergence property the
/// module doc claims.
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
        // Mirrors R2Client::delete_atom_blob's real contract: succeeds even
        // if the object is already gone (S3 DELETE is idempotent).
        self.objects.lock().unwrap().remove(blob_key);
        Ok(())
    }
}

/// A standalone user row — used as a promoter identity, or as the
/// "can reach the ocean but did not publish this" adversary.
async fn seed_user(pool: &PgPool) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .expect("insert user")
}

/// Seed `account -> graph`, returning `(pool, graph_id)`.
async fn seed_graph(pool: &PgPool) -> Uuid {
    let user_id = seed_user(pool).await;
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Target Ocean', 'team') \
         RETURNING id",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .expect("insert graph")
}

/// How a target reef's promotion provenance was recorded — the two shapes
/// that actually exist in production, plus the one that does not exist at
/// all. Seeding all three explicitly is the point: #428's rename-orphans-a-
/// reef defect came from the same-pool and cross-DB paths quietly diverging,
/// so the tests must be able to say which shape they are asserting on.
#[derive(Debug, Clone, Copy)]
enum Provenance {
    /// Same-pool `Store::promote_node`: identity columns stamped AND a
    /// `node_promotions` row recorded.
    SamePool { promoter: Uuid },
    /// Cross-DB org `services::promotion::write_promotion_rows`: identity
    /// columns stamped, and deliberately NO `node_promotions` row (that
    /// table's `source_node_id` FK targets a `nodes` row in the other
    /// database — see that module's doc).
    CrossDbOrg { promoter: Uuid },
    /// The pre-migration-0111 cross-plane case, and any organically-derived
    /// reef: no identity columns, no `node_promotions` row, nobody recorded
    /// as having published it. This is the gap the missing-provenance
    /// decision is about.
    None,
}

/// Seed one reef with the given corals already linked, each with a blob
/// object also seeded into `blobs` (mirroring a promoted, blob-backed
/// coral). Returns `(node_id, atom_ids, blob_keys)`.
///
/// `prov` decides whether this reef looks like a same-pool promotion, a
/// cross-DB org promotion, or content that arrived with no provenance at all.
async fn seed_reef_with_corals(
    pool: &PgPool,
    blobs: &FlakyBlobs,
    graph_id: Uuid,
    title: &str,
    n: usize,
    prov: Provenance,
) -> (Uuid, Vec<Uuid>, Vec<String>) {
    // The SOURCE reef's identity, in whichever ocean it was published from.
    // `promoted_from_node_id`/`promoted_from_graph_id` are bare, no-FK UUID
    // columns precisely so a cross-plane source can be named here (migration
    // 0111's header), so a synthetic id is a faithful stand-in for the
    // cross-DB case and is fine for the same-pool case too.
    let (owner_id, promoted_from) = match prov {
        Provenance::SamePool { promoter } | Provenance::CrossDbOrg { promoter } => {
            (Some(promoter), Some(Uuid::new_v4()))
        }
        Provenance::None => (None, None),
    };

    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body, owner_id, \
                            promoted_from_node_id, promoted_from_graph_id) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb, $3, $4, $5) \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .bind(owner_id)
    .bind(promoted_from)
    // The source ocean id is also a bare column; reuse `graph_id` for the
    // same-pool shape and a synthetic id for the cross-plane shape. Neither
    // is read by the authorization check — asserted by the two shapes being
    // judged identically below.
    .bind(promoted_from.map(|_| match prov {
        Provenance::SamePool { .. } => graph_id,
        _ => Uuid::new_v4(),
    }))
    .fetch_one(pool)
    .await
    .expect("insert node");

    // Only the same-pool path writes provenance into `node_promotions`. The
    // authorization check deliberately does NOT read this table (it does not
    // exist on the cross-DB path, and its `promoted_by` is overwritten on
    // re-promote); seeding it anyway is what lets the tests below prove the
    // two paths are judged identically rather than assume it.
    if let Provenance::SamePool { promoter } = prov {
        let source_node_id: Uuid = sqlx::query_scalar(
            "INSERT INTO nodes (node_id, graph_id, type, title, body) \
             VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
        )
        .bind(graph_id)
        .bind(format!("{title} (source)"))
        .fetch_one(pool)
        .await
        .expect("insert source node");
        sqlx::query(
            "INSERT INTO node_promotions \
                 (source_node_id, target_graph_id, target_node_id, promoted_by, from_version) \
             VALUES ($1, $2, $3, $4, 1)",
        )
        .bind(source_node_id)
        .bind(graph_id)
        .bind(node_id)
        .bind(promoter)
        .execute(pool)
        .await
        .expect("insert node_promotions");
    }

    let mut atom_ids = Vec::new();
    let mut blob_keys = Vec::new();
    for i in 0..n {
        let atom_id = Uuid::new_v4();
        let blob_key = format!("org_x/{graph_id}/{atom_id}");
        blobs.put(&blob_key, b"sensitive retracted content");
        sqlx::query(
            "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                                 participants, \"timestamp\", scope, fts_language, header_len) \
             VALUES ($1, $2, 'note', $3, $4, '{}', now(), 'team', 'english', 0)",
        )
        .bind(atom_id)
        .bind(graph_id)
        .bind(format!("src-{title}-{i}"))
        .bind(&blob_key)
        .execute(pool)
        .await
        .expect("insert atom");
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(atom_id)
            .execute(pool)
            .await
            .expect("link atom");
        atom_ids.push(atom_id);
        blob_keys.push(blob_key);
    }
    (node_id, atom_ids, blob_keys)
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
async fn link_exists(pool: &PgPool, node_id: Uuid, atom_id: Uuid) -> bool {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM node_atoms WHERE node_id = $1 AND atom_id = $2",
    )
    .bind(node_id)
    .bind(atom_id)
    .fetch_one(pool)
    .await
    .unwrap()
        > 0
}

// ── 1. The happy path: one of two corals unpublished, reef survives ───────────

#[tokio::test]
async fn unpublish_one_of_two_corals_leaves_reef_and_sibling_intact() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let promoter = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();
    let (node_id, atoms, keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Two-Coral Reef",
        2,
        Provenance::SamePool { promoter },
    )
    .await;

    let outcome = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], promoter)
        .await
        .expect("unpublish succeeds");
    assert!(outcome.atom_was_present);
    assert!(
        outcome.deleted_reef_ids.is_empty(),
        "the reef still has one coral left — must not be deleted"
    );

    assert!(
        !atom_exists(&pool, atoms[0]).await,
        "unpublished atom must be gone"
    );
    assert!(
        !blobs.contains(&keys[0]),
        "unpublished atom's blob must be gone"
    );
    assert!(
        atom_exists(&pool, atoms[1]).await,
        "sibling coral must survive"
    );
    assert!(
        blobs.contains(&keys[1]),
        "sibling coral's blob must survive"
    );
    assert!(
        node_exists(&pool, node_id).await,
        "reef with a remaining coral must survive"
    );
    assert!(link_exists(&pool, node_id, atoms[1]).await);
}

// ── 2. The coral-less-reef rule: last coral gone => reef gone too ─────────────

#[tokio::test]
async fn unpublish_last_coral_deletes_the_now_coral_less_reef() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let promoter = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();
    let (node_id, atoms, keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "One-Coral Reef",
        1,
        Provenance::SamePool { promoter },
    )
    .await;

    let outcome = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], promoter)
        .await
        .expect("unpublish succeeds");
    assert!(outcome.atom_was_present);
    assert_eq!(
        outcome.deleted_reef_ids,
        vec![node_id],
        "the reef's last coral is gone — the reef itself must be deleted too, per §4.9 \
         (nodes has no FK to atoms, so it would otherwise survive its own evidence)"
    );

    assert!(!atom_exists(&pool, atoms[0]).await);
    assert!(!blobs.contains(&keys[0]));
    assert!(
        !node_exists(&pool, node_id).await,
        "a natively-derived reef left coral-less must actually be deleted"
    );
}

// ── 3. Idempotency: unpublishing twice is a safe no-op the second time ────────

#[tokio::test]
async fn unpublish_is_idempotent_on_retry() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let promoter = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();
    let (node_id, atoms, _keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Idempotency Reef",
        1,
        Provenance::SamePool { promoter },
    )
    .await;

    let first = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], promoter)
        .await
        .expect("first unpublish succeeds");
    assert!(first.atom_was_present);
    assert_eq!(first.deleted_reef_ids, vec![node_id]);

    let second = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], promoter)
        .await
        .expect("retrying an already-unpublished atom must succeed, not error");
    assert!(
        !second.atom_was_present,
        "the second call finds nothing left to retract — idempotent no-op"
    );
    assert!(
        second.deleted_reef_ids.is_empty(),
        "nothing new to delete on the idempotent retry"
    );

    // Unpublishing an id that was never published at all is identical.
    let never_published = unpublish_atom(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        Uuid::new_v4(),
        promoter,
    )
    .await
    .expect("unpublishing an unknown atom id must succeed as a no-op");
    assert!(!never_published.atom_was_present);
}

// ── 4/5. Partial-failure recovery: blob delete fails => full rollback,
//         then a retry with the SAME instance (now healthy) converges ────────

#[tokio::test]
async fn blob_delete_failure_rolls_back_completely_and_a_retry_then_converges() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let promoter = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();
    let (node_id, atoms, keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Flaky Reef",
        1,
        Provenance::SamePool { promoter },
    )
    .await;

    blobs.fail_deletes.store(true, Ordering::SeqCst);
    let attempts_before = blobs.delete_attempt_count();
    let err = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], promoter)
        .await
        .expect_err("a blob delete failure must surface as an error, not silently succeed");
    assert!(matches!(err, RetractionError::Blob(_)));
    assert_eq!(
        blobs.delete_attempt_count(),
        attempts_before + 1,
        "exactly one delete attempt was made"
    );

    // UNAMBIGUOUS rollback: nothing was touched — the row, its link, and the
    // blob (still present, since the delete failed) are all exactly as
    // before the call. This is the module doc's core claim: a blob-delete
    // failure never leaves a half-retracted state.
    assert!(
        atom_exists(&pool, atoms[0]).await,
        "atom row must survive an unambiguous rollback"
    );
    assert!(
        link_exists(&pool, node_id, atoms[0]).await,
        "node_atoms link must survive"
    );
    assert!(node_exists(&pool, node_id).await, "reef must survive");
    assert!(
        blobs.contains(&keys[0]),
        "blob must survive a failed delete"
    );

    // Retry with the SAME instance, now healthy — must fully converge.
    blobs.fail_deletes.store(false, Ordering::SeqCst);
    let outcome = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], promoter)
        .await
        .expect("retry after the transient failure clears must succeed");
    assert!(outcome.atom_was_present);
    assert_eq!(outcome.deleted_reef_ids, vec![node_id]);
    assert!(!atom_exists(&pool, atoms[0]).await);
    assert!(!node_exists(&pool, node_id).await);
    assert!(!blobs.contains(&keys[0]));
}

/// The R2-off deployment shape (`blobs: None`, content lives in
/// `content_inline`) must still fully retract the row-level state — the
/// module doc's ordering only gates on a blob delete when there IS blob
/// transport configured at all.
#[tokio::test]
async fn unpublish_without_blob_transport_still_deletes_rows() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let promoter = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();
    let (node_id, atoms, _keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "No-R2 Reef",
        1,
        Provenance::SamePool { promoter },
    )
    .await;

    let outcome = unpublish_atom(&pool, None, "test-device", graph_id, atoms[0], promoter)
        .await
        .expect("unpublish with no blob transport still succeeds");
    assert!(outcome.atom_was_present);
    assert_eq!(outcome.deleted_reef_ids, vec![node_id]);
    assert!(!atom_exists(&pool, atoms[0]).await);
    assert!(!node_exists(&pool, node_id).await);
}

// ── 6. Revoke: idempotent, history-preserving, source-level consent flip ──────

#[tokio::test]
async fn revoke_source_publication_is_idempotent_and_preserves_the_row() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let source_graph_id = seed_graph(&pool).await;
    let target_graph_id = seed_graph(&pool).await;
    let source_id = format!("slack:{}", Uuid::new_v4());

    // §4.5's consent record: `consented_by` names the publisher, and is now
    // also the authorization anchor for revoking.
    let publisher = seed_user(&pool).await;
    sqlx::query(
        "INSERT INTO source_publications \
             (source_id, source_graph_id, target_graph_id, consented_by) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .bind(publisher)
    .execute(&pool)
    .await
    .expect("seed a live publication");

    let first =
        revoke_source_publication(&pool, &source_id, source_graph_id, target_graph_id, publisher)
            .await
            .expect("revoke succeeds");
    assert_eq!(
        first,
        RevokeOutcome::Revoked,
        "the first revoke actually flips revoked_at"
    );

    let revoked_at_1: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "SELECT revoked_at FROM source_publications \
         WHERE source_id = $1 AND source_graph_id = $2 AND target_graph_id = $3",
    )
    .bind(&source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        revoked_at_1.is_some(),
        "history stays intact — the row is UPDATEd, never deleted"
    );

    // Idempotent: revoking again is a successful no-op, and does not bump
    // revoked_at to a later timestamp.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let second =
        revoke_source_publication(&pool, &source_id, source_graph_id, target_graph_id, publisher)
            .await
            .expect("re-revoke succeeds");
    assert_eq!(
        second,
        RevokeOutcome::AlreadyRevoked,
        "an already-revoked publication is a no-op, not a fresh revoke"
    );
    assert!(!second.revoked_now());

    let revoked_at_2: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "SELECT revoked_at FROM source_publications \
         WHERE source_id = $1 AND source_graph_id = $2 AND target_graph_id = $3",
    )
    .bind(&source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        revoked_at_1, revoked_at_2,
        "revoked_at must not move on a repeated revoke"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
//  AUTHORIZATION — "was the original promoter", not "can reach the graph"
// ═══════════════════════════════════════════════════════════════════════════

/// Assert that a denied call changed NOTHING. Every §4.9 denial must be a
/// total no-op: the atom, its link, its reef and its blob all exactly as
/// before. A gate that denies but has already deleted the blob would be worse
/// than no gate at all, because the content is destroyed AND the caller is
/// told it failed.
async fn assert_nothing_was_retracted(
    pool: &PgPool,
    blobs: &FlakyBlobs,
    node_id: Uuid,
    atom_id: Uuid,
    blob_key: &str,
) {
    assert!(
        atom_exists(pool, atom_id).await,
        "a denied retraction must not delete the atom row"
    );
    assert!(
        link_exists(pool, node_id, atom_id).await,
        "a denied retraction must not delete the node_atoms link"
    );
    assert!(
        node_exists(pool, node_id).await,
        "a denied retraction must not delete the reef"
    );
    assert!(
        blobs.contains(blob_key),
        "a denied retraction must not delete the R2 blob"
    );
}

// ── 7. The promoter can retract; a non-promoter who CAN reach it cannot ───────

/// The headline property. Both callers here are equally able to reach the
/// ocean — that is the ENTIRE point: reachability was the old gate, and it
/// must no longer be sufficient. The reef is seeded through the same-pool
/// shape, so `node_promotions.promoted_by` also names the promoter; test 9
/// proves the cross-DB shape (which has no such row) is judged identically.
#[tokio::test]
async fn only_the_original_promoter_may_unpublish() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let promoter = seed_user(&pool).await;
    // A fellow member of the same shared ocean: under the OLD "can reach the
    // graph" rule this user could retract the promoter's disclosure. That is
    // exactly what must now fail.
    let fellow_member = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();
    let (node_id, atoms, keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Promoter-Only Reef",
        1,
        Provenance::SamePool { promoter },
    )
    .await;

    let attempts_before = blobs.delete_attempt_count();
    let err = unpublish_atom(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        atoms[0],
        fellow_member,
    )
    .await
    .expect_err("a non-promoter must be refused, not silently allowed");
    assert!(
        matches!(err, RetractionError::NotPromoter),
        "a member who did not promote this coral must get NotPromoter, got {err:?}"
    );

    // The refusal happens BEFORE the destructive step, not after it. The blob
    // delete is the first irreversible action `unpublish_atom` takes, so zero
    // delete attempts is the load-bearing proof that authorization is
    // upstream of it rather than a post-hoc error.
    assert_eq!(
        blobs.delete_attempt_count(),
        attempts_before,
        "a denied call must not even ATTEMPT the R2 blob delete — the check must run \
         ahead of the first irreversible step"
    );
    assert_nothing_was_retracted(&pool, &blobs, node_id, atoms[0], &keys[0]).await;

    // ...and the actual promoter is unaffected by any of this: the gate is a
    // narrowing, not a lockout.
    let outcome = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], promoter)
        .await
        .expect("the original promoter must still be able to retract");
    assert!(outcome.atom_was_present);
    assert_eq!(outcome.deleted_reef_ids, vec![node_id]);
    assert!(!atom_exists(&pool, atoms[0]).await);
    assert!(!blobs.contains(&keys[0]));
}

// ── 8. Missing provenance fails CLOSED, and destroys nothing ──────────────────

/// The crux case, pinned to the decision that was actually made. Content with
/// no recorded promoter (cross-DB org promotions predating migration 0111 —
/// not backfillable by construction — and corals that never got linked to a
/// promoted reef copy) is REFUSED, with an error distinguishable from an
/// ordinary policy denial, and nothing is deleted.
///
/// If this ever flips to "allow", that is a decision to reinstate the old
/// "anyone in the ocean" model over precisely the legacy content, and it
/// should have to break this test to do it.
#[tokio::test]
async fn missing_provenance_fails_closed_for_everyone() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let blobs = FlakyBlobs::default();
    let (node_id, atoms, keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Pre-0111 Cross-Plane Reef",
        1,
        Provenance::None,
    )
    .await;

    // Nobody at all can retract it — not a random member, and not even the
    // ocean's own owner. There is deliberately no owner override, so "the
    // owner can always clean up" is NOT a property of this system and must
    // not be assumed by anything built on top of it.
    let ocean_owner: Uuid = sqlx::query_scalar(
        "SELECT a.owner_user_id FROM graphs g JOIN accounts a ON a.id = g.account_id \
         WHERE g.id = $1",
    )
    .bind(graph_id)
    .fetch_one(&pool)
    .await
    .expect("resolve the ocean owner");
    let some_member = seed_user(&pool).await;

    for (who, label) in [
        (some_member, "an ordinary member"),
        (ocean_owner, "the ocean owner"),
    ] {
        let attempts_before = blobs.delete_attempt_count();
        let err = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], who)
            .await
            .expect_err("unprovenanced content must be refused, never silently retracted");
        assert!(
            matches!(err, RetractionError::ProvenanceUnavailable),
            "{label}: missing provenance must surface as ProvenanceUnavailable — distinct \
             from NotPromoter, because a data gap and a policy denial need different \
             follow-up. Got {err:?}"
        );
        assert_eq!(
            blobs.delete_attempt_count(),
            attempts_before,
            "{label}: a denied call must not attempt the blob delete"
        );
        assert_nothing_was_retracted(&pool, &blobs, node_id, atoms[0], &keys[0]).await;
    }
}

// ── 9. Same-pool and cross-DB org paths are judged IDENTICALLY ────────────────

/// #428's rename-orphans-a-reef defect came from these two promotion paths
/// diverging, so an authorization rule that behaves differently on each would
/// be the same mistake in a worse place. The two reefs here are seeded to the
/// two REAL shapes: same-pool has identity columns AND a `node_promotions`
/// row; cross-DB org has identity columns and deliberately NO
/// `node_promotions` row (its `source_node_id` FK targets a `nodes` row in
/// the other database). Every assertion below is made against both.
#[tokio::test]
async fn same_pool_and_cross_db_org_promotions_are_judged_identically() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let blobs = FlakyBlobs::default();

    let same_pool_promoter = seed_user(&pool).await;
    let cross_db_promoter = seed_user(&pool).await;
    let outsider = seed_user(&pool).await;

    let (sp_node, sp_atoms, sp_keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Same-Pool Shape",
        1,
        Provenance::SamePool {
            promoter: same_pool_promoter,
        },
    )
    .await;
    let (cd_node, cd_atoms, cd_keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Cross-DB Org Shape",
        1,
        Provenance::CrossDbOrg {
            promoter: cross_db_promoter,
        },
    )
    .await;

    // Sanity: the two fixtures really do differ in the way production does,
    // otherwise this test would be asserting identical behaviour on identical
    // inputs and proving nothing.
    let sp_promotions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM node_promotions WHERE target_node_id = $1")
            .bind(sp_node)
            .fetch_one(&pool)
            .await
            .unwrap();
    let cd_promotions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM node_promotions WHERE target_node_id = $1")
            .bind(cd_node)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sp_promotions, 1, "same-pool fixture must have provenance in node_promotions");
    assert_eq!(
        cd_promotions, 0,
        "cross-DB org fixture must have NO node_promotions row — that is the whole \
         asymmetry this test exists to hold the authorization rule steady across"
    );

    // Property 1: an outsider is refused on BOTH, with the SAME error.
    for (label, atom, node, key) in [
        ("same-pool", sp_atoms[0], sp_node, &sp_keys[0]),
        ("cross-DB org", cd_atoms[0], cd_node, &cd_keys[0]),
    ] {
        let err = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atom, outsider)
            .await
            .expect_err("outsider must be refused");
        assert!(
            matches!(err, RetractionError::NotPromoter),
            "{label}: outsider must get NotPromoter, got {err:?}"
        );
        assert_nothing_was_retracted(&pool, &blobs, node, atom, key).await;
    }

    // Property 2: neither promoter has authority over the OTHER path's coral.
    // (A same-pool promoter is not globally privileged just because the
    // richer `node_promotions` provenance happens to exist for their reef.)
    let err = unpublish_atom(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        cd_atoms[0],
        same_pool_promoter,
    )
    .await
    .expect_err("cross-path authority must not leak");
    assert!(matches!(err, RetractionError::NotPromoter));

    // Property 3: each path's own promoter succeeds, with the same outcome
    // shape (reef deleted as its last coral goes).
    let sp_outcome = unpublish_atom(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        sp_atoms[0],
        same_pool_promoter,
    )
    .await
    .expect("same-pool promoter retracts");
    let cd_outcome = unpublish_atom(
        &pool,
        Some(&blobs),
        "test-device",
        graph_id,
        cd_atoms[0],
        cross_db_promoter,
    )
    .await
    .expect("cross-DB org promoter retracts — same rule, no node_promotions row needed");
    assert_eq!(sp_outcome.atom_was_present, cd_outcome.atom_was_present);
    assert_eq!(sp_outcome.deleted_reef_ids, vec![sp_node]);
    assert_eq!(cd_outcome.deleted_reef_ids, vec![cd_node]);
}

// ── 10. The SECOND entry point enforces it too ────────────────────────────────

/// A gate on `unpublish_atom` alone would be worthless while
/// `revoke_source_publication` stayed open — a reviewer's first question, and
/// the first thing an attacker tries. Revoke reads its promoter from a
/// different table (`source_publications.consented_by`) but runs it through
/// the same decision function, so the observable policy must match.
#[tokio::test]
async fn revoke_is_gated_on_the_consenting_publisher() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let source_graph_id = seed_graph(&pool).await;
    let target_graph_id = seed_graph(&pool).await;
    let source_id = format!("slack:{}", Uuid::new_v4());
    let publisher = seed_user(&pool).await;
    let fellow_member = seed_user(&pool).await;

    sqlx::query(
        "INSERT INTO source_publications \
             (source_id, source_graph_id, target_graph_id, consented_by) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .bind(publisher)
    .execute(&pool)
    .await
    .expect("seed a live publication");

    let err = revoke_source_publication(
        &pool,
        &source_id,
        source_graph_id,
        target_graph_id,
        fellow_member,
    )
    .await
    .expect_err("a non-consenter must be refused");
    assert!(
        matches!(err, RetractionError::NotPromoter),
        "revoke must enforce the same rule as unpublish, got {err:?}"
    );

    // And the refusal left the row alone — a denial must not half-apply.
    let still_live: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "SELECT revoked_at FROM source_publications \
         WHERE source_id = $1 AND source_graph_id = $2 AND target_graph_id = $3",
    )
    .bind(&source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        still_live.is_none(),
        "a denied revoke must not flip revoked_at"
    );

    // The consenting publisher still can.
    let ok =
        revoke_source_publication(&pool, &source_id, source_graph_id, target_graph_id, publisher)
            .await
            .expect("the consenting publisher may revoke");
    assert_eq!(ok, RevokeOutcome::Revoked);
}

// ── 11. Revoke: missing consent record fails closed too ───────────────────────

/// `consented_by` is nullable — the column carries `ON DELETE SET NULL`, and
/// a row written before anything populated it has it NULL anyway. That is the
/// same "provenance is simply absent" shape as unpublish's pre-0111 case, and
/// it must reach the same fail-closed answer rather than quietly falling back
/// to whoever can reach the source ocean.
#[tokio::test]
async fn revoke_with_no_recorded_consenter_fails_closed() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let source_graph_id = seed_graph(&pool).await;
    let target_graph_id = seed_graph(&pool).await;
    let source_id = format!("slack:{}", Uuid::new_v4());
    let anyone = seed_user(&pool).await;

    // No `consented_by` — exactly the shape every row written by today's code
    // would have, since nothing in the tree populates that column yet.
    sqlx::query(
        "INSERT INTO source_publications (source_id, source_graph_id, target_graph_id) \
         VALUES ($1, $2, $3)",
    )
    .bind(&source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .execute(&pool)
    .await
    .expect("seed a publication with no recorded consenter");

    let err =
        revoke_source_publication(&pool, &source_id, source_graph_id, target_graph_id, anyone)
            .await
            .expect_err("a publication with no recorded consenter must fail closed");
    assert!(
        matches!(err, RetractionError::ProvenanceUnavailable),
        "got {err:?}"
    );

    let still_live: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "SELECT revoked_at FROM source_publications \
         WHERE source_id = $1 AND source_graph_id = $2 AND target_graph_id = $3",
    )
    .bind(&source_id)
    .bind(source_graph_id)
    .bind(target_graph_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(still_live.is_none(), "a denied revoke must change nothing");

    // A publication that does not exist at all is NOT an authorization
    // outcome — there is no protected object — and stays the idempotent
    // no-op it always was. It is also byte-identical to what the legitimate
    // publisher gets, so it is not an existence oracle.
    let missing = revoke_source_publication(
        &pool,
        &format!("slack:{}", Uuid::new_v4()),
        source_graph_id,
        target_graph_id,
        anyone,
    )
    .await
    .expect("an unknown publication is a no-op, not an error");
    assert_eq!(missing, RevokeOutcome::NotPublished);
    assert!(!missing.revoked_now());
}

// ── 12. Provenance is read from the TARGET graph, not wherever it is found ────

/// The check scopes its `nodes` join to the target graph in its own SQL
/// rather than relying on RLS (which is not active on the raw pool these
/// functions own — see the module doc). This pins that scoping: a promoted
/// reef in a DIFFERENT ocean, owned by the caller, must not vouch for a coral
/// in this one.
#[tokio::test]
async fn a_promoted_reef_in_another_ocean_does_not_confer_authority() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let victim_graph = seed_graph(&pool).await;
    let other_graph = seed_graph(&pool).await;
    let promoter = seed_user(&pool).await;
    let attacker = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();

    let (victim_node, victim_atoms, victim_keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        victim_graph,
        "Victim Reef",
        1,
        Provenance::SamePool { promoter },
    )
    .await;
    // The attacker legitimately promoted something of their own — into a
    // different ocean.
    let (_other_node, _other_atoms, _other_keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        other_graph,
        "Attacker's Own Reef",
        1,
        Provenance::CrossDbOrg {
            promoter: attacker,
        },
    )
    .await;

    let err = unpublish_atom(
        &pool,
        Some(&blobs),
        "test-device",
        victim_graph,
        victim_atoms[0],
        attacker,
    )
    .await
    .expect_err("owning a promoted reef elsewhere must not authorize anything here");
    assert!(matches!(err, RetractionError::NotPromoter), "got {err:?}");
    assert_nothing_was_retracted(
        &pool,
        &blobs,
        victim_node,
        victim_atoms[0],
        &victim_keys[0],
    )
    .await;
}

// ── 13. Idempotency is preserved for the promoter, and unchanged for others ───

/// The "already gone" arm runs BEFORE the promoter check, on purpose: there
/// is no object left to protect, and answering differently by caller would
/// turn this endpoint into an existence oracle over a graph the caller can
/// already read. This pins that ordering so a later "tighten it up" refactor
/// has to argue with it rather than silently change it.
#[tokio::test]
async fn an_already_retracted_coral_is_a_no_op_for_any_caller() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let promoter = seed_user(&pool).await;
    let stranger = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();
    let (_node_id, atoms, _keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Gone Already Reef",
        1,
        Provenance::SamePool { promoter },
    )
    .await;

    unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], promoter)
        .await
        .expect("promoter retracts");

    for who in [promoter, stranger] {
        let outcome = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atoms[0], who)
            .await
            .expect("an already-gone coral is a no-op, not an authorization error");
        assert!(
            !outcome.atom_was_present,
            "identical answer for every caller — no existence oracle"
        );
    }
}

// ── 14. A blob key that names a DIFFERENT graph is refused, not deleted ───────

/// The regression test for the "unpublish must never touch the SOURCE
/// ocean's object" invariant (module doc, Step 2). `copy_atoms_to_graph` /
/// `copy_atoms_to_graph_scoped` always mint a fresh, target-namespaced key
/// for a promoted copy, so a row's `content_blob_key` graph segment matching
/// its own `graph_id` is an invariant that should hold for every real row —
/// this proves what happens if it is EVER violated (a forged/corrupted row,
/// or a future copy-path bug), rather than trusting it silently.
#[tokio::test]
async fn a_blob_key_naming_a_different_graph_is_refused_not_deleted() {
    let Some(url) = test_db_url() else {
        eprintln!("skipping: set RELINFER_TEST_DATABASE_URL");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let graph_id = seed_graph(&pool).await;
    let other_graph_id = seed_graph(&pool).await; // stands in for a DIFFERENT ocean
    let promoter = seed_user(&pool).await;
    let blobs = FlakyBlobs::default();

    // A normally-authorized reef (same-pool promotion, `promoter` recorded) —
    // authorization must NOT be what stops this call; the blob-key guard must.
    let (node_id, atoms, _keys) = seed_reef_with_corals(
        &pool,
        &blobs,
        graph_id,
        "Mismatched Key Reef",
        1,
        Provenance::SamePool { promoter },
    )
    .await;
    let atom_id = atoms[0];

    // Overwrite the row's content_blob_key to point into `other_graph_id`'s
    // namespace instead of its own `graph_id` — the shape this guard exists
    // to catch. The object actually LIVES at that foreign key in `blobs`, so
    // a wrongly-permitted delete would be directly observable.
    let foreign_key = format!("org_x/{other_graph_id}/{atom_id}");
    blobs.put(&foreign_key, b"belongs to a DIFFERENT ocean");
    sqlx::query("UPDATE atoms SET content_blob_key = $1 WHERE atom_id = $2")
        .bind(&foreign_key)
        .bind(atom_id)
        .execute(&pool)
        .await
        .expect("corrupt the blob key for the test fixture");

    let err = unpublish_atom(&pool, Some(&blobs), "test-device", graph_id, atom_id, promoter)
        .await
        .expect_err("a blob key naming a different graph must be refused, not honored");
    assert!(
        matches!(err, RetractionError::Blob(R2Error::InvalidKey)),
        "expected the blob-scope guard's InvalidKey refusal, got {err:?}"
    );

    // Nothing touched: not the row, and — the property that actually
    // matters — not the foreign-ocean object either.
    assert!(atom_exists(&pool, atom_id).await, "row must survive the refusal");
    assert!(node_exists(&pool, node_id).await, "reef must survive the refusal");
    assert!(
        blobs.contains(&foreign_key),
        "the OTHER ocean's object must be untouched by a refused unpublish"
    );
    assert_eq!(
        blobs.delete_attempt_count(),
        0,
        "the guard must reject BEFORE any delete_blob call is even attempted"
    );
}
