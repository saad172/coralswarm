//! Live tests for `services::corrections::propagate` (contract §8.4 / C15,
//! conformance test 12) against a REAL Postgres.
//!
//! Proves the exact properties the design (`.scratch/gsuite-enterprise/
//! g3-meet-design.md` §5/§6) and `docs/connectors/CONNECTOR_CONTRACT.md`
//! §8.5's test 12 name:
//!
//!   1. a correction written on ONE graph rewrites the stored atom content in
//!      EVERY graph in the account sharing that source identity (two member
//!      graphs, one account — the fan-out shape any connector's per-member
//!      copies produce, Meet's included once Train G3 PR A lands);
//!   2. an `EmbedAtoms` job is enqueued naming every rewritten atom;
//!   3. a reef whose ONLY linked coral is a rewritten atom is invalidated
//!      (deleted, so extraction rebuilds it) — while a reef that ALSO cites
//!      an atom outside the rewritten set survives untouched;
//!   4. the verbatim `meeting_transcripts` row this module never touches
//!      stays byte-identical, so the existing read-time `corrected_text`
//!      overlay (`routes::meetings::apply_corrections`) is unaffected;
//!   5. re-running `propagate` with the SAME correction set converges (no
//!      atom is rewritten twice, no duplicate reef-deletion attempt errors).
//!
//! Gated on `ORGT_TEST_BASE_URL`, same scratch-DB-per-file convention as
//! every other `*_pg.rs` suite here (`support::rls_probe::RlsProbe`).
//!
//!   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//!     cargo test -p coralswarm-backend --test corrections_propagate_pg -- --test-threads=1 --nocapture

mod support;

use coralswarm_backend::services::blob_store::PromotionBlobStore;
use coralswarm_backend::services::corrections::propagate;
use coralswarm_core::r2::R2Error;
use sqlx::{PgPool, Row};
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// A second graph in the SAME account as `probe.a` — the "two member
/// oceans, one account" fan-out shape (`services::deletion`'s own pg tests
/// use the identical extra-seeding pattern via `probe.owner`). `kind =
/// 'member'` mirrors the per-member private-ocean convention the design
/// this module fixes describes (§3 of the Meet design) — propagation itself
/// is graph-`kind`-agnostic, so any value would exercise the same code path,
/// but this is the shape a real fan-out actually produces.
async fn seed_second_graph(owner: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, is_primary, owner_user_id) \
         VALUES ($1, 'Second Member Ocean', 'member', false, $2) RETURNING id",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_one(owner)
    .await
    .expect("insert second graph")
}

/// One chunk atom, `source_type = 'transcript'` (`routes::ingest`'s shape),
/// content stored inline. Most of this suite's tests use inline atoms (no R2
/// round trip needed to prove the rewrite/reef/embed/overlay properties) —
/// the R2 (`content_blob_key`-only) path is covered separately by
/// [`propagate_rewrites_an_r2_backed_atom_via_the_blob_store`] below, via a
/// fake [`PromotionBlobStore`] (CodeRabbit round 1, finding 7 — an earlier
/// revision of this comment claimed that path was "exercised by unit
/// coverage of `persist_rewritten_content`'s dispatch", which was false: no
/// such test existed anywhere in the tree).
async fn seed_transcript_atom(
    owner: &PgPool,
    graph_id: Uuid,
    source_id: &str,
    content: &str,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                             participants, \"timestamp\", scope, fts_language) \
         VALUES ($1, 'transcript', $2, 'blob-unused', $3, '{}', now(), 'private', 'english') \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(content)
    .fetch_one(owner)
    .await
    .expect("seed transcript atom")
}

async fn seed_meeting_transcript(owner: &PgPool, graph_id: Uuid, source_id: &str, raw: &str) {
    sqlx::query(
        "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline) VALUES ($1, $2, $3)",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(raw)
    .execute(owner)
    .await
    .expect("seed meeting_transcripts");
}

/// An `applied` speaker relabel, `insert_speaker_relabel_scoped`'s shape
/// (0070): `atom_id` is the nil-UUID sentinel (no FK, nothing reads it).
async fn seed_applied_relabel(
    owner: &PgPool,
    graph_id: Uuid,
    source_id: &str,
    surface_form: &str,
    canonical: &str,
    occurrence: Option<i32>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO transcript_corrections \
            (graph_id, atom_id, source_id, surface_form, canonical, status, kind, origin, occurrence) \
         VALUES ($1, $2, $3, $4, $5, 'applied', 'speaker', 'user', $6) \
         RETURNING correction_id",
    )
    .bind(graph_id)
    .bind(Uuid::nil())
    .bind(source_id)
    .bind(surface_form)
    .bind(canonical)
    .bind(occurrence)
    .fetch_one(owner)
    .await
    .expect("seed applied relabel")
}

async fn seed_node(owner: &PgPool, graph_id: Uuid, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title, body) VALUES ($1, 'entity', $2, '{}') RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .fetch_one(owner)
    .await
    .expect("seed node")
}

async fn link(owner: &PgPool, node_id: Uuid, atom_id: Uuid) {
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(owner)
        .await
        .expect("link node_atoms");
}

async fn content_of(owner: &PgPool, atom_id: Uuid) -> Option<String> {
    sqlx::query("SELECT content_inline FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(owner)
        .await
        .expect("read content_inline")
        .get("content_inline")
}

async fn node_exists(owner: &PgPool, node_id: Uuid) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)")
        .bind(node_id)
        .fetch_one(owner)
        .await
        .expect("node exists check")
}

/// Every `atom_id` named by ANY `EmbedAtoms` job's payload for this account —
/// `jobs` carries no `account_id` FK filter needed here since the scratch DB
/// is per-test-file and this is the only account that ever enqueues.
async fn embed_atoms_job_atom_ids(owner: &PgPool) -> Vec<Uuid> {
    let rows = sqlx::query("SELECT payload FROM jobs WHERE kind = 'EmbedAtoms'")
        .fetch_all(owner)
        .await
        .expect("read EmbedAtoms jobs");
    let mut ids = Vec::new();
    for row in rows {
        let payload: serde_json::Value = row.get("payload");
        if let Some(arr) = payload.get("atom_ids").and_then(|v| v.as_array()) {
            for v in arr {
                if let Some(s) = v.as_str() {
                    ids.push(Uuid::parse_str(s).expect("atom_id is a uuid"));
                }
            }
        }
    }
    ids
}

#[tokio::test]
async fn propagate_rewrites_two_graphs_embeds_invalidates_reef_and_leaves_overlay_source_untouched()
{
    let Some(probe) = RlsProbe::setup("orgt_corrections_propagate").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let graph_a = probe.a.graph_id;
    let graph_b = seed_second_graph(&probe.owner, probe.a.account_id, probe.a.user_id).await;
    let base_source_id = "google-meet conf-42";
    let raw = "Speaker 2: Let's ship the plan. Speaker 2: Any objections?";

    // Two independent copies of the SAME meeting in the SAME account (the
    // per-member fan-out shape) — one chunk atom each, identical content.
    let atom_a = seed_transcript_atom(
        &probe.owner,
        graph_a,
        &format!("{base_source_id}:chunk:0"),
        raw,
    )
    .await;
    let atom_b = seed_transcript_atom(
        &probe.owner,
        graph_b,
        &format!("{base_source_id}:chunk:0"),
        raw,
    )
    .await;

    // Only graph A carries the first-class meeting row (§1.7/§3.5 of the
    // design this closes) — propagation must never touch it.
    seed_meeting_transcript(&probe.owner, graph_a, base_source_id, raw).await;

    // A single-coral reef (ONLY linked to atom_a) — must be invalidated.
    let single_coral_reef = seed_node(&probe.owner, graph_a, "Speaker 2 (unresolved)").await;
    link(&probe.owner, single_coral_reef, atom_a).await;

    // A reef that ALSO cites something outside the rewritten set — an
    // unrelated atom in graph A — must SURVIVE (module doc's "strictly
    // narrower than any reef touching a rewritten atom" property).
    let unrelated_atom = seed_transcript_atom(
        &probe.owner,
        graph_a,
        "other-source:chunk:0",
        "unrelated content",
    )
    .await;
    let mixed_reef = seed_node(&probe.owner, graph_a, "Cross-meeting thread").await;
    link(&probe.owner, mixed_reef, atom_a).await;
    link(&probe.owner, mixed_reef, unrelated_atom).await;

    // The relabel, written against graph A (mirrors `POST
    // /v1/transcript-corrections` — `create_speaker_relabel`).
    seed_applied_relabel(
        &probe.owner,
        graph_a,
        base_source_id,
        "Speaker 2",
        "Alice",
        None,
    )
    .await;

    let outcome = propagate(
        &probe.owner,
        None,
        None,
        "test-device",
        graph_a,
        base_source_id,
    )
    .await
    .expect("propagate");

    // ── 1. Rewrite spans BOTH graphs in the account ─────────────────────────
    assert_eq!(
        content_of(&probe.owner, atom_a).await.as_deref(),
        Some("Alice: Let's ship the plan. Alice: Any objections?"),
        "graph A's atom must be rewritten"
    );
    assert_eq!(
        content_of(&probe.owner, atom_b).await.as_deref(),
        Some("Alice: Let's ship the plan. Alice: Any objections?"),
        "graph B's copy — a DIFFERENT graph in the SAME account — must be rewritten too"
    );
    assert_eq!(
        content_of(&probe.owner, unrelated_atom).await.as_deref(),
        Some("unrelated content"),
        "an atom with a DIFFERENT source_id must never be touched"
    );
    let mut rewritten_ids: Vec<Uuid> = outcome.rewritten_atoms.iter().map(|(_, a)| *a).collect();
    rewritten_ids.sort();
    let mut expected = vec![atom_a, atom_b];
    expected.sort();
    assert_eq!(
        rewritten_ids, expected,
        "outcome must report exactly the two rewritten atoms"
    );

    // ── 2. EmbedAtoms enqueued per rewritten atom ───────────────────────────
    assert!(
        outcome.embed_job_enqueued,
        "an EmbedAtoms job must have been enqueued"
    );
    let mut embed_ids = embed_atoms_job_atom_ids(&probe.owner).await;
    embed_ids.sort();
    embed_ids.dedup();
    assert!(
        embed_ids.contains(&atom_a) && embed_ids.contains(&atom_b),
        "EmbedAtoms job(s) must name BOTH rewritten atoms — got {embed_ids:?}"
    );

    // ── 3. Single-coral reef invalidated; mixed reef survives ───────────────
    assert!(
        !node_exists(&probe.owner, single_coral_reef).await,
        "a reef whose ONLY coral was rewritten must be invalidated (deleted)"
    );
    assert!(
        node_exists(&probe.owner, mixed_reef).await,
        "a reef that ALSO cites an atom outside the rewritten set must survive untouched"
    );
    let invalidated: Vec<Uuid> = outcome
        .invalidated_reef_ids
        .iter()
        .map(|(_, n)| *n)
        .collect();
    assert_eq!(invalidated, vec![single_coral_reef]);

    // ── 4. meeting_transcripts (the overlay's source) is byte-identical ─────
    let stored_raw: Option<String> = sqlx::query(
        "SELECT raw_inline FROM meeting_transcripts WHERE graph_id = $1 AND source_id = $2",
    )
    .bind(graph_a)
    .bind(base_source_id)
    .fetch_one(&probe.owner)
    .await
    .expect("read meeting_transcripts")
    .get("raw_inline");
    assert_eq!(
        stored_raw.as_deref(),
        Some(raw),
        "propagation must NEVER mutate meeting_transcripts.raw_inline — the read-time overlay \
         is computed from this untouched raw text plus the (unchanged) correction row, so it \
         still returns the correct corrected text regardless of what this module did to atoms"
    );

    // ── 5. Idempotency: re-running converges ────────────────────────────────
    let second = propagate(
        &probe.owner,
        None,
        None,
        "test-device",
        graph_a,
        base_source_id,
    )
    .await
    .expect("second propagate");
    assert!(
        second.rewritten_atoms.is_empty(),
        "re-running propagate with the SAME applied correction set must rewrite NOTHING further \
         (content-based idempotency) — got {:?}",
        second.rewritten_atoms
    );
    assert!(second.invalidated_reef_ids.is_empty());
    assert_eq!(
        content_of(&probe.owner, atom_a).await.as_deref(),
        Some("Alice: Let's ship the plan. Alice: Any objections?"),
        "content must be stable across a second propagate call"
    );

    probe.teardown().await;
}

#[tokio::test]
async fn propagate_is_a_no_op_when_no_corrections_are_applied() {
    let Some(probe) = RlsProbe::setup("orgt_corrections_propagate_noop").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let graph_a = probe.a.graph_id;
    let base_source_id = "google-meet conf-noop";
    let content = "Speaker 2: nothing to correct here.";
    let atom_a = seed_transcript_atom(
        &probe.owner,
        graph_a,
        &format!("{base_source_id}:chunk:0"),
        content,
    )
    .await;

    // No transcript_corrections row at all for this (graph_id, source_id).
    let outcome = propagate(
        &probe.owner,
        None,
        None,
        "test-device",
        graph_a,
        base_source_id,
    )
    .await
    .expect("propagate");

    assert!(outcome.rewritten_atoms.is_empty());
    assert!(!outcome.embed_job_enqueued);
    assert_eq!(
        content_of(&probe.owner, atom_a).await.as_deref(),
        Some(content)
    );

    probe.teardown().await;
}

#[tokio::test]
async fn propagate_occurrence_scoped_relabel_rewrites_only_the_atom_it_actually_appears_in() {
    let Some(probe) = RlsProbe::setup("orgt_corrections_propagate_occurrence").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let graph_a = probe.a.graph_id;
    let base_source_id = "google-meet conf-occurrence";
    let content = "Speaker 2: first thing. Speaker 2: second thing.";
    let atom_a = seed_transcript_atom(
        &probe.owner,
        graph_a,
        &format!("{base_source_id}:chunk:0"),
        content,
    )
    .await;

    seed_applied_relabel(
        &probe.owner,
        graph_a,
        base_source_id,
        "Speaker 2",
        "Alice",
        Some(2),
    )
    .await;

    let outcome = propagate(
        &probe.owner,
        None,
        None,
        "test-device",
        graph_a,
        base_source_id,
    )
    .await
    .expect("propagate");

    assert_eq!(outcome.rewritten_atoms.len(), 1);
    assert_eq!(
        content_of(&probe.owner, atom_a).await.as_deref(),
        Some("Speaker 2: first thing. Alice: second thing."),
        "an occurrence-scoped (this-segment) relabel must still rewrite stored atom content, \
         counted per-atom (services::corrections' module doc)"
    );

    probe.teardown().await;
}

// ── CodeRabbit round 1, finding 7: R2 (content_blob_key-only) coverage ──────

/// In-memory fake `PromotionBlobStore` — same shape as `deletion_pg.rs`'s
/// `FlakyBlobs`, minus the fault-injection knobs this test doesn't need.
#[derive(Default)]
struct FakeBlobs {
    objects: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl FakeBlobs {
    fn seed(&self, key: &str, content: &[u8]) {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_owned(), content.to_vec());
    }
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.objects.lock().unwrap().get(key).cloned()
    }
}

#[async_trait::async_trait]
impl PromotionBlobStore for FakeBlobs {
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
        self.objects
            .lock()
            .unwrap()
            .insert(blob_key.to_owned(), content.to_vec());
        Ok(())
    }
    async fn delete_blob(&self, _blob_key: &str) -> Result<(), R2Error> {
        Ok(())
    }
}

#[tokio::test]
async fn propagate_rewrites_an_r2_backed_atom_via_the_blob_store() {
    let Some(probe) = RlsProbe::setup("orgt_corrections_propagate_r2").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let graph_a = probe.a.graph_id;
    let base_source_id = "google-meet conf-r2";
    let blob_key = format!("{graph_a}/{}", Uuid::new_v4());
    let raw = "Speaker 2: r2-backed content.";
    let fake = FakeBlobs::default();
    fake.seed(&blob_key, raw.as_bytes());

    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \
                             participants, \"timestamp\", scope, fts_language) \
         VALUES ($1, 'transcript', $2, $3, '{}', now(), 'private', 'english') \
         RETURNING atom_id",
    )
    .bind(graph_a)
    .bind(format!("{base_source_id}:chunk:0"))
    .bind(&blob_key)
    .fetch_one(&probe.owner)
    .await
    .expect("seed R2-backed atom (content_inline left NULL)");

    seed_applied_relabel(
        &probe.owner,
        graph_a,
        base_source_id,
        "Speaker 2",
        "Alice",
        None,
    )
    .await;

    let dek = [7u8; 32];
    let outcome = propagate(
        &probe.app_pool,
        Some(&fake as &dyn PromotionBlobStore),
        Some(dek),
        "test-device",
        graph_a,
        base_source_id,
    )
    .await
    .expect("propagate over an R2-backed atom");

    assert_eq!(outcome.rewritten_atoms, vec![(graph_a, atom_id)]);
    assert!(outcome.embed_job_enqueued);

    let new_key: String = sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id=$1")
        .bind(atom_id).fetch_one(&probe.owner).await.unwrap();
    assert_ne!(new_key, blob_key, "content replacement must publish a fresh object identity");
    assert_eq!(fake.get(&blob_key).unwrap(), raw.as_bytes(), "the original object remains immutable");
    assert_eq!(String::from_utf8(fake.get(&new_key).unwrap()).unwrap(), "Alice: r2-backed content.");

    probe.teardown().await;
}

// ── CodeRabbit round 1, finding 4: per-source advisory lock ─────────────────

/// Directly proves the concurrency-control mechanism [`propagate`] now uses:
/// holds the EXACT SAME advisory-lock key `propagate()` itself takes,
/// externally, on a separate connection (simulating another in-flight
/// `propagate()` call for this source), then asserts a concurrently-spawned
/// `propagate()` call BLOCKS (does not complete) while that lock is held,
/// and completes promptly once it is released — with the correct final
/// content. This is the mechanism that makes the "unconditional writes race
/// overlapping propagations" finding moot: by construction, only one
/// `propagate()` call for a given `(account, source)` is ever mutating atoms
/// at a time, and whichever runs second re-reads corrections/content AFTER
/// the first has fully released the lock.
#[tokio::test]
async fn propagate_serializes_concurrent_calls_for_the_same_source_via_advisory_lock() {
    let Some(probe) = RlsProbe::setup("orgt_corrections_propagate_lock").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    let graph_a = probe.a.graph_id;
    let base_source_id = "google-meet conf-lock";
    let atom = seed_transcript_atom(
        &probe.owner,
        graph_a,
        &format!("{base_source_id}:chunk:0"),
        "Speaker 2: hi.",
    )
    .await;
    seed_applied_relabel(
        &probe.owner,
        graph_a,
        base_source_id,
        "Speaker 2",
        "Alice",
        None,
    )
    .await;

    // Hold the SAME key `services::corrections::propagate` documents and
    // uses (`corrections_propagate:{account_id}:{source_id}` via
    // `hashtext`), externally, simulating another in-flight call.
    let mut external_lock_tx = probe.owner.begin().await.expect("begin external lock tx");
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtext('corrections_propagate:' || $1::text || ':' || $2))",
    )
    .bind(probe.a.account_id.to_string())
    .bind(base_source_id)
    .execute(&mut *external_lock_tx)
    .await
    .expect("acquire external lock");

    let pool = probe.owner.clone();
    let handle = tokio::spawn(async move {
        propagate(&pool, None, None, "test-device", graph_a, base_source_id).await
    });

    // `propagate()` must NOT complete while the external lock is held —
    // give it ample time to (wrongly) race ahead if the lock weren't real.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !handle.is_finished(),
        "propagate() must BLOCK while another call holds the per-source advisory lock, not \
         race ahead and mutate atoms concurrently"
    );

    // Release the external lock — propagate() should now proceed and finish.
    external_lock_tx.rollback().await.ok();

    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
        .await
        .expect("propagate() must complete promptly once the contending lock is released")
        .expect("propagate task must not panic")
        .expect("propagate must succeed");

    assert_eq!(outcome.rewritten_atoms, vec![(graph_a, atom)]);
    assert_eq!(
        content_of(&probe.owner, atom).await.as_deref(),
        Some("Alice: hi.")
    );

    probe.teardown().await;
}
