//! `Store::copy_atoms_to_graph`/`copy_atoms_to_graph_scoped` must carry
//! `atoms.embed_model` and `atoms.chunker_version` through the copy, not
//! just the `embedding` vector itself.
//!
//! `vector_search` (and `vector_search_scoped`) hard-filters
//! `embed_model = EMBED_MODEL_NAME`. A copy that lands the vector but
//! writes NULL for the stamp is invisible to Ask — observed in prod as the
//! Coralswarm Team ocean's 3,696 promoted corals, every one embedded and
//! every one `embed_model IS NULL`.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (same convention as
//! `copy_atoms_title_kind_pg.rs`); a plain `cargo test` with no DB
//! configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5442/relinfer_test \
//!     cargo test -p coralswarm-backend --test copy_atoms_embed_model_pg -- --nocapture

use coralswarm_backend::store::{BlobCopyMode, PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use coralswarm_core::chunker::CHUNKER_VERSION;
use coralswarm_core::embed::EMBED_MODEL_NAME;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use uuid::Uuid;

/// Distinct from `EMBED_MODEL_NAME` so a refresh that forgets to SET
/// `embed_model`/`chunker_version` cannot pass by leaving the insert-time
/// stamps in place.
const REFRESH_EMBED_MODEL: &str = "refresh-probe-model";
const REFRESH_CHUNKER_VERSION: i16 = 99;

macro_rules! test_pool {
    () => {{
        match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => PgPoolOptions::new()
                .max_connections(4)
                .connect(&u)
                .await
                .expect("connect to test Postgres"),
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run copy_atoms_embed_model_pg"
                );
                return;
            }
        }
    }};
}

async fn seed_tenancy(pool: &sqlx::PgPool) -> (Uuid, Uuid) {
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
    (user_id, account_id)
}

async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid, user_id: Uuid) {
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .ok();
}

/// One source atom with a real-shaped embedding + the stamps `vector_search`
/// requires. `content_inline` is set so `BlobCopyMode::NoR2` is exact (this
/// suite is about the stamp columns, not R2 re-upload).
async fn seed_embedded_atom(pool: &sqlx::PgPool, graph_id: Uuid, source_id: &str) -> Uuid {
    let mut embedding = vec![0.0f32; 384];
    embedding[0] = 1.0;
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
                           \"timestamp\", embedding, embed_model, chunker_version) \
         VALUES ($1, 'note', $2, 'blob', 'Wayfinder ticket 08 resolved.', now(), \
                 $3, $4, $5) \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(pgvector::Vector::from(embedding))
    .bind(EMBED_MODEL_NAME)
    .bind(CHUNKER_VERSION)
    .fetch_one(pool)
    .await
    .expect("seed embedded atom")
}

/// Bump source stamps and change `content_inline` so
/// `atoms_content_revision` advances `content_revision` (it only fires on
/// content/blob/timestamp, not on the stamp columns). That revision bump is
/// what lets `refresh_published_atoms_scoped` take the
/// `ON CONFLICT … DO UPDATE` arm (`refresh=true` AND revision changed). A
/// second `copy_atoms_to_graph`/`_scoped` would not: both pass
/// `refresh=false`, so that WHERE clause never fires.
async fn bump_source_embed_stamps(pool: &sqlx::PgPool, atom_id: Uuid) {
    sqlx::query(
        "UPDATE atoms SET embed_model = $2, chunker_version = $3, \
         content_inline = content_inline || ' refreshed' \
         WHERE atom_id = $1",
    )
    .bind(atom_id)
    .bind(REFRESH_EMBED_MODEL)
    .bind(REFRESH_CHUNKER_VERSION)
    .execute(pool)
    .await
    .expect("bump source embed stamps");
}

/// Re-promote through the only path that executes the ON CONFLICT UPDATE
/// this PR added (`refresh_published_atoms_scoped` → `refresh=true`).
async fn refresh_and_assert_stamps(
    store: &PostgresStore,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    src_atom: Uuid,
    target_graph_id: Uuid,
    existing_target: Uuid,
) {
    bump_source_embed_stamps(pool, src_atom).await;
    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .expect("begin_account_scoped for refresh");
    let refreshed = store
        .refresh_published_atoms_scoped(&mut tx, &[src_atom], target_graph_id, BlobCopyMode::NoR2)
        .await
        .expect("refresh_published_atoms_scoped");
    tx.commit().await.expect("commit refresh");
    assert_eq!(
        refreshed.len(),
        1,
        "refresh returns the existing target atom"
    );
    assert_eq!(
        refreshed[0], existing_target,
        "refresh must update the existing copy, not mint a second atom"
    );
    let (has_emb, embed_model, chunker_version) = embed_stamps(pool, existing_target).await;
    assert!(has_emb, "refresh must keep the embedding");
    assert_eq!(
        embed_model.as_deref(),
        Some(REFRESH_EMBED_MODEL),
        "ON CONFLICT refresh must SET embed_model from the source — a stale \
         insert-time stamp here is the regression this assertion exists for"
    );
    assert_eq!(
        chunker_version,
        Some(REFRESH_CHUNKER_VERSION),
        "ON CONFLICT refresh must SET chunker_version from the source"
    );
}

async fn embed_stamps(pool: &sqlx::PgPool, atom_id: Uuid) -> (bool, Option<String>, Option<i16>) {
    let row = sqlx::query(
        "SELECT embedding IS NOT NULL AS has_embedding, embed_model, chunker_version \
         FROM atoms WHERE atom_id = $1",
    )
    .bind(atom_id)
    .fetch_one(pool)
    .await
    .expect("read embed stamps");
    (
        row.get("has_embedding"),
        row.get("embed_model"),
        row.get("chunker_version"),
    )
}

#[tokio::test]
async fn copy_atoms_to_graph_carries_embed_model_and_chunker_version() {
    let pool = test_pool!();
    let (user_id, account_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let src_graph = store
        .create_graph(
            account_id,
            &format!("copy-emb-src-{}", Uuid::new_v4()),
            "personal",
        )
        .await
        .expect("create source graph");
    let target_graph = store
        .create_graph(
            account_id,
            &format!("copy-emb-target-{}", Uuid::new_v4()),
            "personal",
        )
        .await
        .expect("create target graph");

    let src_atom = seed_embedded_atom(&pool, src_graph.id, "copy-emb-unscoped-1").await;
    let (src_has, src_model, src_chunker) = embed_stamps(&pool, src_atom).await;
    assert!(src_has, "seed must have an embedding");
    assert_eq!(src_model.as_deref(), Some(EMBED_MODEL_NAME));
    assert_eq!(src_chunker, Some(CHUNKER_VERSION));

    let copied_ids = store
        .copy_atoms_to_graph(&[src_atom], target_graph.id, BlobCopyMode::NoR2)
        .await
        .expect("copy_atoms_to_graph");
    assert_eq!(copied_ids.len(), 1, "exactly one atom copied");
    assert_ne!(copied_ids[0], src_atom, "copy mints a fresh atom_id");

    let (has_emb, embed_model, chunker_version) = embed_stamps(&pool, copied_ids[0]).await;
    assert!(
        has_emb,
        "embedding must carry through the copy (pre-existing contract)"
    );
    assert_eq!(
        embed_model.as_deref(),
        Some(EMBED_MODEL_NAME),
        "embed_model must ALSO carry through — a NULL here is the Team-ocean \
         0-sources bug: vector_search excludes embed_model IS NULL"
    );
    assert_eq!(
        chunker_version,
        Some(CHUNKER_VERSION),
        "chunker_version must travel with the embedding, same as embed_model"
    );

    refresh_and_assert_stamps(
        &store,
        &pool,
        account_id,
        src_atom,
        target_graph.id,
        copied_ids[0],
    )
    .await;

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn copy_atoms_to_graph_scoped_carries_embed_model_and_chunker_version() {
    let pool = test_pool!();
    let (user_id, account_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let src_graph = store
        .create_graph(
            account_id,
            &format!("copy-emb-src-scoped-{}", Uuid::new_v4()),
            "personal",
        )
        .await
        .expect("create source graph");
    let target_graph = store
        .create_graph(
            account_id,
            &format!("copy-emb-target-scoped-{}", Uuid::new_v4()),
            "personal",
        )
        .await
        .expect("create target graph");

    let src_atom = seed_embedded_atom(&pool, src_graph.id, "copy-emb-scoped-1").await;

    let mut tx = begin_account_scoped(&pool, account_id)
        .await
        .expect("begin_account_scoped");
    let copied_ids = store
        .copy_atoms_to_graph_scoped(&mut tx, &[src_atom], target_graph.id, BlobCopyMode::NoR2)
        .await
        .expect("copy_atoms_to_graph_scoped");
    tx.commit().await.expect("commit");
    assert_eq!(copied_ids.len(), 1, "exactly one atom copied");

    let (has_emb, embed_model, chunker_version) = embed_stamps(&pool, copied_ids[0]).await;
    assert!(has_emb);
    assert_eq!(
        embed_model.as_deref(),
        Some(EMBED_MODEL_NAME),
        "the SCOPED twin must carry embed_model too, not just the unscoped path"
    );
    assert_eq!(
        chunker_version,
        Some(CHUNKER_VERSION),
        "the SCOPED twin must carry chunker_version too"
    );

    refresh_and_assert_stamps(
        &store,
        &pool,
        account_id,
        src_atom,
        target_graph.id,
        copied_ids[0],
    )
    .await;

    cleanup(&pool, account_id, user_id).await;
}
