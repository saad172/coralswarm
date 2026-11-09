//! `Store::copy_atoms_to_graph`/`copy_atoms_to_graph_scoped` must carry
//! `atoms.title_kind` through the copy, not just `title`/`excerpt`
//! (CodeRabbit round 1 on #837, filed against the title-always-present PR).
//!
//! Both queries already carry `title`/`excerpt` (the PR's own fix for the
//! verified prod finding: a 3,129-row copy with 0 titled, 5 excerpted). But
//! omitting `title_kind` from the `INSERT` column list writes `NULL` for
//! every copied row — so a copied FALLBACK title lands `title SET,
//! title_kind NULL`, the exact pre-`0212`/`0080` shape
//! `atom_title_backfill::CANDIDATE_SQL`'s `(title IS NULL OR title_kind =
//! 'fallback')` predicate does NOT admit (independent review finding M1's
//! own truth table). Left unfixed, EVERY future promotion would freeze a
//! copied fallback title forever, permanently reintroducing the bug M1's
//! migration one-shot backfill only closes for HISTORICAL rows.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations
//! applied, same convention as `session_shares_pg.rs`); a plain `cargo test`
//! with no DB configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5442/relinfer_test \
//!     cargo test -p coralswarm-backend --test copy_atoms_title_kind_pg -- --nocapture

use coralswarm_backend::store::{BlobCopyMode, PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use uuid::Uuid;

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
                    "skipping: set RELINFER_TEST_DATABASE_URL to run copy_atoms_title_kind_pg"
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
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(pool).await.ok();
}

/// Seed one atom directly with an explicit `title`/`title_kind` — the
/// fallback-titled shape a real copy would encounter (`routes::ingest`'s
/// synchronous write, or an earlier sweep pass).
async fn seed_fallback_titled_atom(pool: &sqlx::PgPool, graph_id: Uuid, source_id: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
                           \"timestamp\", title, title_kind, excerpt) \
         VALUES ($1, 'note', $2, 'blob', 'Deployed the copy fix. Then verified prod.', now(), \
                 'Deployed the copy fix', 'fallback', 'Deployed the copy fix. Then verified prod.') \
         RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(pool)
    .await
    .expect("seed fallback-titled atom")
}

async fn title_and_kind(pool: &sqlx::PgPool, atom_id: Uuid) -> (Option<String>, Option<String>) {
    let row = sqlx::query("SELECT title, title_kind FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(pool)
        .await
        .expect("read title/title_kind");
    (row.get("title"), row.get("title_kind"))
}

#[tokio::test]
async fn copy_atoms_to_graph_carries_title_kind() {
    let pool = test_pool!();
    let (user_id, account_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let src_graph = store
        .create_graph(account_id, &format!("copy-src-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create source graph");
    let target_graph = store
        .create_graph(account_id, &format!("copy-target-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create target graph");

    let src_atom = seed_fallback_titled_atom(&pool, src_graph.id, "copy-unscoped-1").await;

    // The seeded atom carries a (fake, never-really-uploaded) `content_blob_key`
    // AND `content_inline`, so `BlobCopyMode::NoR2` is exact here: it never
    // attempts a real R2 call, and its own fail-closed check (every row must
    // have `content_inline` to fall back on) passes because the seed set it.
    // This test is about `title`/`title_kind` carrying through the copy, not
    // R2 re-upload — `promote_r2_reupload_pg.rs` owns that.
    let copied_ids = store
        .copy_atoms_to_graph(&[src_atom], target_graph.id, BlobCopyMode::NoR2)
        .await
        .expect("copy_atoms_to_graph");
    assert_eq!(copied_ids.len(), 1, "exactly one atom copied");

    let (title, title_kind) = title_and_kind(&pool, copied_ids[0]).await;
    assert_eq!(
        title.as_deref(),
        Some("Deployed the copy fix"),
        "title must carry through the copy (the PR's own fix for finding #3)"
    );
    assert_eq!(
        title_kind.as_deref(),
        Some("fallback"),
        "title_kind must ALSO carry through — a NULL here reintroduces the exact \
         `title SET, title_kind NULL` shape M1's migration fix exists to close, on every \
         future promotion (CodeRabbit round 1 on #837)"
    );

    cleanup(&pool, account_id, user_id).await;
}

#[tokio::test]
async fn copy_atoms_to_graph_scoped_carries_title_kind() {
    let pool = test_pool!();
    let (user_id, account_id) = seed_tenancy(&pool).await;
    let store = PostgresStore::new(pool.clone());

    let src_graph = store
        .create_graph(account_id, &format!("copy-src-scoped-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create source graph");
    let target_graph = store
        .create_graph(account_id, &format!("copy-target-scoped-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create target graph");

    let src_atom = seed_fallback_titled_atom(&pool, src_graph.id, "copy-scoped-1").await;

    let mut tx = pool.begin().await.expect("begin tx");
    // Same `BlobCopyMode::NoR2` rationale as the unscoped test above.
    let copied_ids = store
        .copy_atoms_to_graph_scoped(&mut tx, &[src_atom], target_graph.id, BlobCopyMode::NoR2)
        .await
        .expect("copy_atoms_to_graph_scoped");
    tx.commit().await.expect("commit");
    assert_eq!(copied_ids.len(), 1, "exactly one atom copied");

    let (title, title_kind) = title_and_kind(&pool, copied_ids[0]).await;
    assert_eq!(title.as_deref(), Some("Deployed the copy fix"));
    assert_eq!(
        title_kind.as_deref(),
        Some("fallback"),
        "the SCOPED twin must carry title_kind too, not just the unscoped path"
    );

    cleanup(&pool, account_id, user_id).await;
}
