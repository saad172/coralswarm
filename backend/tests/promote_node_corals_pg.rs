//! Live test for `Store::promote_node`'s per-coral selection against a REAL
//! Postgres: the subset/all/widening coral-copy behavior of
//! `PostgresStore`.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied) AND `R2_TEST_ENDPOINT_OVERRIDE`/`R2_TEST_ACCESS_KEY`/
//! `R2_TEST_SECRET_KEY`/`R2_TEST_BUCKET` (a local S3-compatible fake, e.g.
//! MinIO — see `r2_extract_success_pg.rs`'s module doc for the docker
//! commands): `Store::copy_atoms_to_graph`'s R2-first re-upload gate
//! (`content_blob_key IS NOT NULL`, regardless of `content_inline` — see
//! that method's own doc comment) means every seeded atom here needs a real
//! blob context, even though this suite otherwise only cares about coral
//! SELECTION semantics (subset/empty/widening), never content fidelity.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5543/coralswarm_relinfer_test \
//!   R2_TEST_ENDPOINT_OVERRIDE=http://localhost:19100 \
//!   R2_TEST_ACCESS_KEY=minioadmin \
//!   R2_TEST_SECRET_KEY=minioadmin \
//!   R2_TEST_BUCKET=cs-test-bucket \
//!     cargo test -p coralswarm-backend --test promote_node_corals_pg -- --nocapture

use chrono::Utc;
use coralswarm_backend::store::{AtomBlobCopyCtx, BlobCopyMode, PostgresStore, Store};
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::r2::R2Client;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;
use coralswarm_backend::store::SessionIdentity;

// SAFETY: `std::env::set_var` is only sound when no other thread in this
// process reads or writes the environment concurrently. This binary runs
// with `--test-threads=1` (see this crate's `ci.yml` step for this suite),
// and every test below calls this helper — writing `MASTER_KEK_HEX` — before
// its own `load_kek()` call ever reads it back, with no thread spawned in
// between that could touch the environment itself.
fn set_test_kek() {
    unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) };
}

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

const SKIP_REASON: &str =
    "skipping: set RELINFER_TEST_DATABASE_URL (a migrated Postgres) AND R2_TEST_ENDPOINT_OVERRIDE \
     / R2_TEST_ACCESS_KEY / R2_TEST_SECRET_KEY / R2_TEST_BUCKET (a local S3-compatible fake, e.g. \
     MinIO — see r2_extract_success_pg.rs's module doc comment for the docker commands)";

/// Seed `user -> account(dek_wrapped) -> graph`, returning
/// `(account_id, user_id, graph_id)`.
async fn seed_tenant(pool: &sqlx::PgPool, wrapped_dek: Option<&[u8]>, name: &str) -> (Uuid, Uuid, Uuid) {
    let store = PostgresStore::new(pool.clone());
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("insert user");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(wrapped_dek)
    .fetch_one(pool)
    .await
    .expect("insert account with wrapped DEK");
    let graph = store
        .create_graph(account_id, name, "personal")
        .await
        .expect("create graph");
    (account_id, user_id, graph.id)
}

/// Seeds an atom with REAL R2 content at the key `insert_atoms` stamps —
/// this suite cares about coral SELECTION semantics, never content
/// fidelity, but `Store::copy_atoms_to_graph`'s R2-first re-upload gate
/// (`content_blob_key IS NOT NULL`, unconditionally — see that method's own
/// doc comment) means every atom with a `content_blob_key` needs a blob
/// context that can actually resolve it, real content or not.
async fn seed_atom(
    store: &PostgresStore,
    r2: &R2Client,
    dek: &[u8; 32],
    graph_id: Uuid,
    user_id: Uuid,
    source_id: &str,
) -> Uuid {
    let (atom_id, blob_key) = seed_atom_row_only(store, graph_id, user_id, source_id).await;
    r2.put_atom_blob_at(&blob_key, format!("content for {source_id}").as_bytes(), dek)
        .await
        .expect("seed real source content at the stamped key");
    atom_id
}

/// [`seed_atom`]'s row half, WITHOUT the R2 upload — an atom whose
/// `content_blob_key` names an object that was never written. The first
/// thing `copy_atoms_to_graph_core`'s re-upload loop does with such a row is
/// `get_blob` it, so a chunk containing one fails outright (and its whole
/// transaction rolls back): a real, in-band failure injection for
/// `promote_session_retry_after_partial_failure_converges_pg`, not a
/// simulated one. Returns `(atom_id, blob_key)` so the caller can later
/// write the missing object and retry.
async fn seed_atom_row_only(
    store: &PostgresStore,
    graph_id: Uuid,
    user_id: Uuid,
    source_id: &str,
) -> (Uuid, String) {
    let atom_id = Uuid::new_v4();
    let blob_key = format!("{graph_id}/{atom_id}");
    store
        .insert_atoms(
            &[atom_id],
            graph_id,
            "slack",
            &[source_id.to_string()],
            &[blob_key.clone()],
            &[],
            Utc::now(),
            user_id,
            "private",
            "english",
        )
        .await
        .expect("insert atom");
    (atom_id, blob_key)
}

#[tokio::test]
async fn promote_node_coral_selection_subset_empty_and_widening_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    set_test_kek();
    let kek = load_kek().expect("test KEK loads");
    let dek = generate_dek();
    let wrapped = wrap_dek(&kek, &dek).expect("wrap test DEK");
    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    let (account_id, user_id, src_graph) = seed_tenant(&pool, Some(&wrapped), "promote-corals-src").await;
    let store = PostgresStore::new(pool.clone());
    let team = store
        .create_graph(account_id, "promote-corals-team", "team")
        .await
        .expect("create team ocean");
    let blob_ctx = AtomBlobCopyCtx { blobs: &r2, org_id: None, dek: &dek };

    let node_id = store
        .upsert_node(src_graph, "entity", "Coral Selection Subject")
        .await
        .expect("upsert node");
    let atom_a = seed_atom(&store, &r2, &dek, src_graph, user_id, "a").await;
    let atom_b = seed_atom(&store, &r2, &dek, src_graph, user_id, "b").await;
    let atom_c = seed_atom(&store, &r2, &dek, src_graph, user_id, "c").await;
    for a in [atom_a, atom_b, atom_c] {
        store.link_node_atom(node_id, a).await.expect("link atom");
    }

    // ── Some([]) — empty selection copies no corals ───────────────────────────
    let p_none = store
        .promote_node(node_id, team.id, user_id, Some(&[]), BlobCopyMode::NoR2)
        .await
        .expect("promote with empty coral selection");
    assert!(p_none.created, "first promote publishes the reef");
    assert_eq!(p_none.corals_requested, 0, "empty selection copies nothing");
    let linked = store
        .node_atom_ids(p_none.target_node_id)
        .await
        .expect("target node_atom_ids");
    assert!(linked.is_empty(), "reef-only promote links no corals");

    // ── Some([a]) — a narrower subset copies exactly that one coral ──────────
    let p_subset = store
        .promote_node(node_id, team.id, user_id, Some(&[atom_a]), BlobCopyMode::R2(&blob_ctx))
        .await
        .expect("promote with a single-coral subset");
    assert!(!p_subset.created, "re-promote updates the existing copy");
    assert_eq!(p_subset.corals_requested, 1, "subset copies only the requested coral");
    let linked = store
        .node_atom_ids(p_subset.target_node_id)
        .await
        .expect("target node_atom_ids");
    assert_eq!(linked.len(), 1, "exactly one coral copied+linked so far");

    // ── None (omitted) = all — widening: a and b/c both end up linked, the
    // earlier narrower promote's coral is never dropped ───────────────────────
    let p_all = store
        .promote_node(node_id, team.id, user_id, None, BlobCopyMode::R2(&blob_ctx))
        .await
        .expect("promote with no coral selection (all)");
    assert!(!p_all.created);
    assert_eq!(p_all.corals_requested, 3, "omitted selection copies every source coral");
    let linked_all = store
        .node_atom_ids(p_all.target_node_id)
        .await
        .expect("target node_atom_ids");
    assert_eq!(
        linked_all.len(),
        3,
        "widening promote adds the rest without touching the earlier copy"
    );

    // ── A subsequent NARROWER re-promote must not remove the wider copy's
    // corals already on the target (additive/widening-only, ADR-0006) ────────
    let p_narrower_again = store
        .promote_node(node_id, team.id, user_id, Some(&[atom_b]), BlobCopyMode::R2(&blob_ctx))
        .await
        .expect("narrower re-promote after a wider one");
    assert!(!p_narrower_again.created);
    assert_eq!(
        p_narrower_again.corals_requested, 1,
        "this call's requested subset is still reported as 1"
    );
    let linked_after_narrowing = store
        .node_atom_ids(p_narrower_again.target_node_id)
        .await
        .expect("target node_atom_ids");
    assert_eq!(
        linked_after_narrowing.len(),
        3,
        "a narrower re-promote never removes corals an earlier, wider promote already copied"
    );

    // ── Idempotency: re-copying a coral already on the target doesn't create
    // a duplicate atom or link row ────────────────────────────────────────────
    let atoms_in_team: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
            .bind(team.id)
            .fetch_one(&pool)
            .await
            .expect("count target atoms");
    assert_eq!(atoms_in_team, 3, "no duplicate coral copies across repeated promotes");

    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
}

/// §4.5 (docs/plans/graph-temporal-memory.md) regression: renaming the
/// SOURCE reef and re-promoting used to `INSERT` a second, orphaned target
/// row (the old upsert was keyed by `(graph_id, type, lower(title))`, so a
/// new title never matched the existing target row's conflict target).
/// Migration `0111_promotion_identity_and_source_publications` re-keys the
/// upsert on `(graph_id, promoted_from_node_id)` instead — this proves a
/// rename-then-re-promote now updates the SAME target row (including its
/// title) rather than forking a duplicate, and that `node_promotions`'
/// provenance still points at exactly one live target row afterward.
#[tokio::test]
async fn promote_node_rename_then_repromote_updates_existing_copy_not_orphans() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "skipping: set RELINFER_TEST_DATABASE_URL to run the live rename/re-promote e2e"
            );
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    let (account_id, user_id, src_graph) = seed_tenant(&pool, None, "rename-repromote-src").await;
    let store = PostgresStore::new(pool.clone());
    let team = store
        .create_graph(account_id, "rename-repromote-team", "team")
        .await
        .expect("create team ocean");

    let node_id = store
        .upsert_node(src_graph, "entity", "Original Title")
        .await
        .expect("upsert node");

    // First promotion, under the original title.
    let p1 = store
        .promote_node(node_id, team.id, user_id, Some(&[]), BlobCopyMode::NoR2)
        .await
        .expect("first promote");
    assert!(p1.created, "first promote publishes a fresh target row");

    // Rename the SOURCE reef (no `update_node`/`rename_node` method exists on
    // `Store` today — a direct UPDATE is the only way to simulate this).
    sqlx::query("UPDATE nodes SET title = $1 WHERE node_id = $2")
        .bind("Renamed Title")
        .bind(node_id)
        .execute(&pool)
        .await
        .expect("rename source reef");

    // Re-promote after the rename.
    let p2 = store
        .promote_node(node_id, team.id, user_id, Some(&[]), BlobCopyMode::NoR2)
        .await
        .expect("re-promote after rename");

    assert!(
        !p2.created,
        "re-promote after a rename must update the EXISTING target row, not create a new one"
    );
    assert_eq!(
        p2.target_node_id, p1.target_node_id,
        "the target row's identity must survive a source-side rename"
    );

    // Exactly ONE row in the target graph — no orphaned duplicate under the
    // old title.
    let target_rows: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT node_id, title FROM nodes WHERE graph_id = $1")
            .bind(team.id)
            .fetch_all(&pool)
            .await
            .expect("list target rows");
    assert_eq!(
        target_rows.len(),
        1,
        "a rename-then-re-promote must not leave an orphaned duplicate reef in the target: {target_rows:?}"
    );
    assert_eq!(
        target_rows[0].1, "Renamed Title",
        "the surviving row's title reflects the re-publish, not the stale original"
    );

    // `node_promotions` provenance still names exactly one target, and it is
    // the surviving row.
    let promo_target: Uuid = sqlx::query_scalar(
        "SELECT target_node_id FROM node_promotions WHERE source_node_id = $1 AND target_graph_id = $2",
    )
    .bind(node_id)
    .bind(team.id)
    .fetch_one(&pool)
    .await
    .expect("node_promotions row");
    assert_eq!(promo_target, target_rows[0].0);

    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
}


/// Proves the retry-convergence property `routes::nodes::promote_session`'s
/// "Write phase 2" doc comment leans on to justify returning a "partially
/// applied; retry to complete" 500 on a partial coral landing INSTEAD OF
/// compensating (deleting) whatever already committed (CodeRabbit PR #838
/// review, item 6 — chosen option (b), not (a): every write on that route's
/// path is already retry-safe by construction, so a caller-level retry of
/// the same request converges to the full set with no duplicates, which is
/// strictly simpler and no less correct than hand-rolled compensation
/// logic).
///
/// `promote_session` itself is unreachable from this external test binary —
/// `mod routes;` is private, the same reason `promote_r2_reupload_pg.rs`'s
/// module doc gives for why IT reads back through `Store`/`crypto` instead
/// of `routes::atoms::get_atom` — so this drives the same `Store` calls
/// `chunked_copy_and_link_corals` makes, in the same order and with the
/// same per-chunk transaction boundary, once per chunk.
///
/// The mid-way failure is REAL, not simulated by copying a smaller slice:
/// the fourth coral (chunk 2's first) is seeded by [`seed_atom_row_only`]
/// with a `content_blob_key` naming an object that was never written, so
/// `copy_atoms_to_graph_core`'s R2-first re-upload loop genuinely fails its
/// `get_blob` and rolls the whole chunk back — which the assertions below
/// check directly (an `expect_err` naming that key, and a target graph
/// still holding only chunk 1's three corals). Only then is the missing
/// object written and the WHOLE request retried from scratch, which is
/// what actually happens on a retry: `chunked_copy_and_link_corals` always
/// re-walks every chunk of `src_atom_ids` from the beginning, never
/// resuming where the failed attempt stopped.
///
/// `promote_session` itself passes `link_target_node_id: None`, but its two
/// siblings on that same helper (`promote_source`, `promote_node`) pass
/// `Some(node_id)`, and `link_node_atom`'s `ON CONFLICT DO NOTHING` is the
/// other half of the convergence claim — so the corals are linked to a
/// target reef here too, and the `node_atoms` count is asserted alongside
/// the atom count.
#[tokio::test]
async fn promote_session_retry_after_partial_failure_converges_pg() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };
    let r2env = match r2_test_env() {
        Some(e) => e,
        None => {
            eprintln!("{SKIP_REASON}");
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");
    let store = PostgresStore::new(pool.clone());

    set_test_kek();
    let kek = load_kek().expect("load test kek");
    let dek = generate_dek();
    let wrapped = wrap_dek(&kek, &dek).expect("wrap test DEK");
    let (account_id, user_id, src_graph) =
        seed_tenant(&pool, Some(&wrapped), "session-retry-src").await;
    let team = store
        .create_graph(account_id, "session-retry-team", "team")
        .await
        .expect("create team ocean");

    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;
    let blob_ctx = AtomBlobCopyCtx { blobs: &r2, org_id: None, dek: &dek };

    // A 6-coral session in the source graph, walked below as two chunks of
    // three. `CORAL_COPY_CHUNK_SIZE` is a private const of `routes::nodes`,
    // so the chunk boundary is drawn explicitly here rather than borrowed.
    let session_id = format!("sess-{}", Uuid::new_v4());
    store
        .upsert_coding_session(
            src_graph,
            &session_id,
            "coding",
            None,
            Some("Retry Session"),
            None,
            None,
            None,
            None,
            None,
            None,
            &SessionIdentity::default(),
        )
        .await
        .expect("seed source coding_sessions row");
    let mut src_atom_ids = Vec::new();
    for i in 0..3 {
        src_atom_ids
            .push(seed_atom(&store, &r2, &dek, src_graph, user_id, &format!("cc:{session_id}:{i}")).await);
    }
    // Chunk 2's FIRST coral is the injected failure — its stamped key names
    // an object that does not exist in the bucket.
    let (broken_atom_id, broken_key) =
        seed_atom_row_only(&store, src_graph, user_id, &format!("cc:{session_id}:3")).await;
    src_atom_ids.push(broken_atom_id);
    for i in 4..6 {
        src_atom_ids
            .push(seed_atom(&store, &r2, &dek, src_graph, user_id, &format!("cc:{session_id}:{i}")).await);
    }
    assert_eq!(src_atom_ids.len(), 6);
    let target_node_id = store
        .upsert_node(team.id, "entity", "Retry Session Reef")
        .await
        .expect("upsert target reef");

    async fn target_atom_map(
        pool: &sqlx::PgPool,
        graph_id: Uuid,
    ) -> std::collections::HashMap<String, (Uuid, String)> {
        let rows: Vec<(String, Uuid, String)> = sqlx::query_as(
            "SELECT source_id, atom_id, content_blob_key FROM atoms WHERE graph_id = $1",
        )
        .bind(graph_id)
        .fetch_all(pool)
        .await
        .expect("list target atoms");
        rows.into_iter().map(|(sid, aid, key)| (sid, (aid, key))).collect()
    }
    async fn linked_coral_count(pool: &sqlx::PgPool, node_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM node_atoms WHERE node_id = $1")
            .bind(node_id)
            .fetch_one(pool)
            .await
            .expect("count node_atoms")
    }

    // ── Attempt 1, write phase 1: the session container row ──────────────────
    let copied = store
        .copy_coding_session_to_graph(src_graph, &session_id, team.id)
        .await
        .expect("copy session container");
    assert!(copied, "the seeded source session must be found and copied");

    // ── Attempt 1, write phase 2, chunk 1: lands and COMMITS ─────────────────
    let landed_first = store
        .copy_atoms_to_graph(&src_atom_ids[..3], team.id, BlobCopyMode::R2(&blob_ctx))
        .await
        .expect("chunk 1 copies cleanly");
    assert_eq!(landed_first.len(), 3, "chunk 1 lands exactly the 3 corals it was given");
    for atom_id in &landed_first {
        store.link_node_atom(target_node_id, *atom_id).await.expect("link chunk 1 coral");
    }
    let after_chunk1 = target_atom_map(&pool, team.id).await;
    assert_eq!(after_chunk1.len(), 3, "only chunk 1's 3 corals have landed so far");
    assert_eq!(linked_coral_count(&pool, target_node_id).await, 3);

    // ── Attempt 1, write phase 2, chunk 2: FAILS mid-way, for real ───────────
    let chunk2_err = store
        .copy_atoms_to_graph(&src_atom_ids[3..], team.id, BlobCopyMode::R2(&blob_ctx))
        .await
        .expect_err("chunk 2 must genuinely FAIL — the injected missing R2 object");
    let chunk2_msg = chunk2_err.to_string();
    assert!(
        chunk2_msg.contains(&broken_key),
        "chunk 2 must fail on the missing SOURCE object, not something incidental: {chunk2_msg}"
    );
    // The failed chunk's transaction rolled back whole: not one of its three
    // corals landed, not even the two whose own R2 objects were fine.
    let after_failure = target_atom_map(&pool, team.id).await;
    assert_eq!(
        after_failure.len(),
        3,
        "a failed chunk must land NOTHING (its tx rolls back), leaving exactly chunk 1's \
         corals: {after_failure:?}"
    );
    assert_eq!(linked_coral_count(&pool, target_node_id).await, 3);

    // At this point `chunked_copy_and_link_corals` would have returned 3 of 6
    // and `promote_session` would have returned its "partially applied; retry
    // to complete" 500. Everything below is what that retry does.

    // ── The operator/caller repairs the cause, then retries the SAME request ─
    r2.put_atom_blob_at(&broken_key, b"repaired source content", &dek)
        .await
        .expect("write the previously-missing source object");

    let copied_again = store
        .copy_coding_session_to_graph(src_graph, &session_id, team.id)
        .await
        .expect("re-copy session container on retry");
    assert!(copied_again, "a retried copy still reports the source session as found");
    let mut landed_retry = Vec::new();
    for chunk in src_atom_ids.chunks(3) {
        let landed = store
            .copy_atoms_to_graph(chunk, team.id, BlobCopyMode::R2(&blob_ctx))
            .await
            .expect("every chunk copies cleanly on the retry");
        assert_eq!(landed.len(), 3, "each retried chunk reports all 3 of its corals");
        for atom_id in &landed {
            store.link_node_atom(target_node_id, *atom_id).await.expect("link retried coral");
        }
        landed_retry.extend(landed);
    }
    assert_eq!(
        landed_retry.len(),
        6,
        "the retry must report every coral, including the 3 chunk 1 already committed"
    );

    // ── Convergence: the full set, no duplicates, nothing re-keyed ───────────
    let after_retry = target_atom_map(&pool, team.id).await;
    assert_eq!(
        after_retry.len(),
        6,
        "retry must not duplicate the 3 corals chunk 1 already landed: {after_retry:?}"
    );
    for (source_id, (atom_id, blob_key)) in &after_chunk1 {
        assert_eq!(
            after_retry.get(source_id),
            Some(&(*atom_id, blob_key.clone())),
            "a coral chunk 1 already landed must keep the SAME target atom_id AND the same \
             already-correct content_blob_key after a retry (never re-inserted, never \
             re-uploaded) — source_id {source_id}"
        );
    }
    assert_eq!(
        linked_coral_count(&pool, target_node_id).await,
        6,
        "re-linking every coral on the retry must not duplicate the 3 links chunk 1 made"
    );

    let session_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM coding_sessions WHERE graph_id = $1 AND session_id = $2",
    )
    .bind(team.id)
    .bind(&session_id)
    .fetch_one(&pool)
    .await
    .expect("count target session rows");
    assert_eq!(session_rows, 1, "retry must not duplicate the session container row");

    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .ok();
}
