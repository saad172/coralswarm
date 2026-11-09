//! Live end-to-end test for the entity-resolution merge fixes (CS-ER bugs #1/#2)
//! against a REAL Postgres.
//!
//! Proves, against the database:
//!   * BUG #1 — a relationship edge attached to the duplicate reef SURVIVES the
//!     merge, re-pointed onto the survivor (instead of being cascade-deleted by
//!     `relationship_edge.{src,dst}_node_id ON DELETE CASCADE`).
//!   * BUG #2 — the merge writes a `node_merges` undo-log row, and
//!     `unmerge_nodes` restores the duplicate reef + its edge, is idempotent, and
//!     stamps `undone_at`.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations applied);
//! a normal `cargo test` with no DB configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test merge_unmerge_pg -- --nocapture

use coralswarm_backend::relationships::{predicates, EdgeOrigin, EdgeStatus};
use coralswarm_backend::store::{PostgresStore, ReefIdentifier, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn merge_repoints_edges_and_unmerge_restores_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live merge/unmerge e2e");
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    // ── Seed tenancy chain: user → account → graph ────────────────────────────
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("insert account");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, "merge-e2e", "personal")
        .await
        .expect("create graph");
    let g = graph.id;

    // ── Three reefs; an edge hangs off the DUPLICATE (dup -knows-> other) ──────
    let keep = store.upsert_node(g, "entity", "KeepReef").await.unwrap();
    let dup = store.upsert_node(g, "entity", "DupReef").await.unwrap();
    let other = store.upsert_node(g, "entity", "OtherReef").await.unwrap();
    store
        .upsert_relationship_edge(
            g,
            dup,
            predicates::KNOWS,
            other,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            0.9,
            serde_json::json!({ "atom_ids": [] }),
        )
        .await
        .expect("seed dup->other edge");

    // ── MERGE dup → keep ──────────────────────────────────────────────────────
    store.merge_nodes(keep, dup).await.expect("merge_nodes");

    let dup_count: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE node_id = $1")
        .bind(dup)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(dup_count, 0, "merge should remove the duplicate node");

    // BUG #1: the edge must SURVIVE, re-pointed onto keep.
    let on_keep = store.list_relationship_edges_for_nodes(g, &[keep]).await.unwrap();
    assert!(
        on_keep.iter().any(|e| e.src_node_id == keep
            && e.dst_node_id == other
            && e.predicate == predicates::KNOWS),
        "BUG#1: dup->other edge must survive re-pointed to keep->other; got {on_keep:?}"
    );

    // BUG #2: an undo-log row was written.
    let merge_id: Uuid = sqlx::query_scalar(
        "SELECT merge_id FROM node_merges WHERE keep_id = $1 AND dup_id = $2 AND undone_at IS NULL",
    )
    .bind(keep)
    .bind(dup)
    .fetch_one(&pool)
    .await
    .expect("a node_merges undo-log row should exist");

    // ── UNMERGE: restore the duplicate + its edge ─────────────────────────────
    store.unmerge_nodes(merge_id).await.expect("unmerge_nodes");

    let dup_back: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE node_id = $1")
        .bind(dup)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(dup_back, 1, "unmerge should restore the duplicate node");

    let on_dup = store.list_relationship_edges_for_nodes(g, &[dup]).await.unwrap();
    assert!(
        on_dup.iter().any(|e| e.src_node_id == dup && e.dst_node_id == other),
        "unmerge should restore the edge onto the dup; got {on_dup:?}"
    );

    let undone: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT undone_at FROM node_merges WHERE merge_id = $1")
            .bind(merge_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(undone.is_some(), "unmerge should stamp undone_at");

    // Idempotent: a second unmerge is a clean no-op.
    store.unmerge_nodes(merge_id).await.expect("second unmerge is a no-op");

    // ── Cleanup (cascades through graph → nodes → edges → node_merges) ─────────
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();

    println!("PG merge/unmerge e2e passed: edge survived re-pointed, undo-log written, unmerge restored");
}

/// CS-ER bug #3 against real Postgres: the per-ocean self reef (Part C) and the
/// connector-identity directory (Part B).
#[tokio::test]
async fn self_node_and_connector_identity_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live author-identity e2e");
            return;
        }
    };
    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();

    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .unwrap();
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let store = PostgresStore::new(pool.clone());
    let g = store
        .create_graph(account_id, "selfnode-e2e", "personal")
        .await
        .unwrap()
        .id;

    // ── Part C: ensure_self_node creates ONE reef, points graphs.self_node_id,
    // and is idempotent with a sticky title (a later raw-id form must NOT rename).
    let (n1, t1) = store.ensure_self_node(g, user_id, "Alex Example").await.unwrap();
    assert_eq!(t1, "Alex Example");
    let self_ptr: Option<Uuid> = sqlx::query_scalar("SELECT self_node_id FROM graphs WHERE id = $1")
        .bind(g)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(self_ptr, Some(n1), "graphs.self_node_id points at the self reef");

    let (n2, t2) = store.ensure_self_node(g, user_id, "U57C3A9EF09").await.unwrap();
    assert_eq!(n2, n1, "idempotent: same self reef");
    assert_eq!(t2, "Alex Example", "title sticks; a later raw-id form does not rename it");

    // ── Part B: connector-identity directory round-trips; misses return None.
    store
        .upsert_connector_identity("slack", "T123", "U57C3A9EF09", "Alex Example")
        .await
        .unwrap();
    let hit = store.connector_identity_name("slack", "T123", "U57C3A9EF09").await.unwrap();
    assert_eq!(hit.as_deref(), Some("Alex Example"), "directory hit returns the canonical name");
    let miss = store.connector_identity_name("slack", "T123", "UNSEEN").await.unwrap();
    assert_eq!(miss, None, "unknown id misses");

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
    println!("PG author-identity e2e passed: self reef stable + connector directory round-trips");
}

/// #135 against real Postgres: merging the ocean's self reef as the duplicate
/// must NOT null `graphs.self_node_id` (the 0049 FK is ON DELETE SET NULL) and
/// must NOT cause `ensure_self_node` to mint a SECOND self reef. Unmerge must
/// restore the pointer to the recreated dup.
#[tokio::test]
async fn merging_self_reef_keeps_self_pointer_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live self-reef merge e2e");
            return;
        }
    };
    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();

    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .unwrap();
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let store = PostgresStore::new(pool.clone());
    let g = store
        .create_graph(account_id, "self-merge-e2e", "personal")
        .await
        .unwrap()
        .id;

    // Owner self reef S (the auto-merge DUP) and a surviving duplicate K (KEEP).
    let (self_reef, _t) = store.ensure_self_node(g, user_id, "Owner Person").await.unwrap();
    let keep = store.upsert_node(g, "entity", "Owner (dup spelling)").await.unwrap();

    // ENTITY RESOLUTION merges the self reef AS THE DUPLICATE into the survivor.
    store.merge_nodes(keep, self_reef).await.expect("merge_nodes");

    // The self reef row is gone, but the pointer must have followed to KEEP — not
    // been nulled by the ON DELETE SET NULL FK.
    let self_ptr: Option<Uuid> = sqlx::query_scalar("SELECT self_node_id FROM graphs WHERE id = $1")
        .bind(g)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(self_ptr, Some(keep), "#135: self_node_id must follow the merge to the survivor, not null");

    // ensure_self_node must take its fast path (pointer non-null) and NOT mint a
    // second self reef.
    let entity_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id = $1 AND type = 'entity'")
            .bind(g)
            .fetch_one(&pool)
            .await
            .unwrap();
    let (again, _t2) = store.ensure_self_node(g, user_id, "Owner Person").await.unwrap();
    assert_eq!(again, keep, "#135: ensure_self_node returns the survivor, not a fresh reef");
    let entity_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id = $1 AND type = 'entity'")
            .bind(g)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(entity_after, entity_before, "#135: no SECOND self reef created");

    // UNMERGE restores the dup and re-points self_node_id back to it.
    let merge_id: Uuid = sqlx::query_scalar(
        "SELECT merge_id FROM node_merges WHERE keep_id = $1 AND dup_id = $2 AND undone_at IS NULL",
    )
    .bind(keep)
    .bind(self_reef)
    .fetch_one(&pool)
    .await
    .unwrap();
    store.unmerge_nodes(merge_id).await.expect("unmerge_nodes");
    let self_ptr_back: Option<Uuid> =
        sqlx::query_scalar("SELECT self_node_id FROM graphs WHERE id = $1")
            .bind(g)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(self_ptr_back, Some(self_reef), "#135: unmerge restores self_node_id to the dup");

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
    println!("PG self-reef merge e2e passed: pointer follows merge, no second reef, unmerge restores");
}

/// T2 FIX 1 regression, against real Postgres: `merge_nodes` must union `dup`'s
/// `reef_identifiers` rows onto `keep` INSIDE the same transaction as the
/// `DELETE FROM nodes` that removes `dup` — `reef_identifiers.node_id` is
/// `ON DELETE CASCADE` (migrations 0138/0045), so a union that runs in any
/// LATER, separate transaction only ever observes zero rows (already
/// cascade-deleted at commit) and silently unions nothing. The `on_keep`
/// assertion below is the NEGATIVE CONTROL: it FAILS against the pre-fix code
/// (which called `union_reef_identifiers`/`_scoped` in a fresh transaction
/// AFTER `merge_nodes` had already committed and cascaded dup's rows away).
///
/// Covers, in one pass:
///   (a) an identifier unique to dup survives the merge, re-pointed onto keep
///       — this is the actual negative control.
///   (b) an identifier keep already owns is left exactly as it was — one row,
///       no error. `reef_identifiers`'s own `UNIQUE (graph_id, kind,
///       value_norm)` constraint makes it impossible for two DIFFERENT live
///       nodes to simultaneously each own a row for the identical tuple (any
///       attempt to give the second node the same identifier is a no-op, not
///       an error — proven below before the merge even runs), so this is the
///       closest constructible analog of "both sides carry the same
///       identifier": prove the attempt to double-assign is absorbed
///       cleanly, then prove the merge is equally clean about the row it
///       never got to duplicate.
///   (c) dup's `reef_identifiers` rows are gone after the merge (cascaded
///       away with the node itself).
#[tokio::test]
async fn merge_nodes_unions_reef_identifiers_onto_keep_against_real_postgres() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "skipping: set RELINFER_TEST_DATABASE_URL to run the live identifier-union e2e"
            );
            return;
        }
    };
    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await.expect("connect");

    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("insert account");

    let store = PostgresStore::new(pool.clone());
    let g = store
        .create_graph(account_id, "identifier-union-e2e", "personal")
        .await
        .expect("create graph")
        .id;

    let keep = store.upsert_node(g, "entity", "KeepReef").await.unwrap();
    let dup = store.upsert_node(g, "entity", "DupReef").await.unwrap();

    // (b) setup: keep owns a shared email FIRST; dup's attempt at the exact
    // same identifier must silently no-op (0 rows), never error — proving the
    // invariant that makes "both sides literally hold the same row" an
    // unreachable DB state, before the merge is ever called.
    let shared_email = "shared@example.com";
    store
        .record_reef_identifier(g, keep, "email", shared_email, "test")
        .await
        .expect("keep gets the shared email");
    store
        .record_reef_identifier(g, dup, "email", shared_email, "test")
        .await
        .expect("dup's attempt at the identical (kind, value_norm) keep already owns must no-op, not error");
    let dup_email_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM reef_identifiers WHERE node_id = $1 AND kind = 'email'")
            .bind(dup)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        dup_email_rows, 0,
        "sanity check: dup must not have won a row for the identifier keep already owns"
    );

    // (a) setup: an identifier UNIQUE to dup. `git_author` deliberately —
    // it's a non-decisive kind (never triggers entity-resolution's own
    // identifier auto-merge), and this test calls `Store::merge_nodes`
    // directly, bypassing that worker entirely, so the kind choice doesn't
    // matter for this test either way; picked for symmetry with the prompt
    // fix (FIX 3) above.
    let dup_only_value = "Dup Author <dup-only@example.com>";
    store
        .record_reef_identifier(g, dup, "git_author", dup_only_value, "test")
        .await
        .expect("dup gets its own unique identifier");

    // ── MERGE dup -> keep ───────────────────────────────────────────────────
    store.merge_nodes(keep, dup).await.expect("merge_nodes must not error");

    let on_keep = store.list_reef_identifiers_for_nodes(&[keep]).await.unwrap();

    // (a) THE NEGATIVE CONTROL — fails against the pre-fix code.
    assert!(
        on_keep.iter().any(|i| i.kind == "git_author" && i.value_norm == dup_only_value),
        "FIX 1 regression: dup's unique git_author identifier must survive the merge, \
         re-pointed onto keep in the SAME transaction as the delete; got {on_keep:?}"
    );

    // (b) keep retains exactly ONE row for the identifier it already owned —
    // no duplicate, no error surfaced from the union re-pointing dup (who,
    // per the sanity check above, never actually held a competing row).
    let keep_emails: Vec<&ReefIdentifier> = on_keep.iter().filter(|i| i.kind == "email").collect();
    assert_eq!(
        keep_emails.len(),
        1,
        "keep must retain exactly one row for the shared identifier, not a duplicate; got {keep_emails:?}"
    );
    assert_eq!(keep_emails[0].value_norm, shared_email);

    // (c) dup's reef_identifiers rows are gone (cascade-deleted with the node).
    let dup_rows_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM reef_identifiers WHERE node_id = $1")
            .bind(dup)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dup_rows_after, 0, "dup's reef_identifiers rows must be gone after merge");

    // Exactly the pre-existing email plus the unioned git_author — proves the
    // union did not ALSO leave a stray duplicate anywhere else.
    assert_eq!(
        on_keep.len(),
        2,
        "keep should hold exactly the pre-existing email row plus dup's unioned git_author row; got {on_keep:?}"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
    println!(
        "PG identifier-union e2e passed: dup's unique identifier survived onto keep, \
         the shared identifier stayed a single row, dup's rows cascade-gone"
    );
}
