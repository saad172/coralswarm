//! Current P8 worker refusal plus explicit transport-maintenance regressions.
//! Whole-source cloud consent is intentionally unavailable. The separate new
//! selected-publication case drives the actual supported API and reader using
//! three real encrypted originals and independent exact-version selections.
//!
//! Gated on migrated RELINFER_TEST_DATABASE_URL and the local R2_TEST_* MinIO
//! fixture. No provider or paid/embedding worker is activated. Direct scoped
//! copies preserve batching/link/title/excerpt/retry maintenance checks without
//! claiming worker completion, automatic derivation or cloud publication.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5543/coralswarm_relinfer_test \
//!   R2_TEST_ENDPOINT_OVERRIDE=http://localhost:19100 \
//!   R2_TEST_ACCESS_KEY=minioadmin \
//!   R2_TEST_SECRET_KEY=minioadmin \
//!   R2_TEST_BUCKET=cs-test-bucket \
//!     cargo test -p coralswarm-backend --test promote_node_async_pg -- --nocapture

use std::sync::Arc;

#[path = "support/selected_publication_fixture.rs"]
mod selected_publication_fixture;

use chrono::Utc;
use coralswarm_backend::store::{AtomBlobCopyCtx, BlobCopyMode, PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::promote;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::r2::R2Client;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

// SAFETY: CI runs this integration binary with --test-threads=1. Both cases
// set the same public synthetic KEK before reading it; no sibling races.
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

async fn seed_tenant(pool: &sqlx::PgPool, wrapped_dek: &[u8], name: &str) -> (Uuid, Uuid, Uuid) {
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
/// see this file's module doc for why every atom here needs one now.
async fn seed_atom(
    store: &PostgresStore,
    r2: &R2Client,
    dek: &[u8; 32],
    graph_id: Uuid,
    user_id: Uuid,
    source_id: &str,
) -> Uuid {
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
    r2.put_atom_blob_at(&blob_key, format!("content for {source_id}").as_bytes(), dek)
        .await
        .expect("seed real source content at the stamped key");
    atom_id
}

#[tokio::test]
async fn legacy_worker_refuses_consent_while_scoped_batch_copy_preserves_metadata_and_retry() {
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

    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await.expect("connect");
    let store = PostgresStore::new(pool.clone());

    let (account_id, user_id, source_graph_id) =
        seed_tenant(&pool, &wrapped, "async-promote-source").await;
    let target_graph = store
        .create_graph(account_id, "async-promote-target-team", "team")
        .await
        .expect("create target team graph");

    let mut atom_ids = Vec::new();
    for i in 0..3 {
        atom_ids.push(
            seed_atom(&store, &r2, &dek, source_graph_id, user_id, &format!("async-{i}")).await,
        );
    }
    // Title-always-present (#837) transport-maintenance regression: stamp
    // one source atom with a title/excerpt (raw SQL — `insert_atoms` on this
    // branch predates #837's `titles`/`title_kinds` params) and confirm
    // the explicit scoped copy (routed through the SAME
    // `copy_atoms_to_graph_core` the R2-reupload suite covers directly)
    // doesn't drop them.
    let async_source_title = "async-promote source headline";
    let async_source_excerpt = "async-promote source excerpt";
    sqlx::query("UPDATE atoms SET title = $1, excerpt = $2 WHERE atom_id = $3")
        .bind(async_source_title)
        .bind(async_source_excerpt)
        .bind(atom_ids[0])
        .execute(&pool)
        .await
        .expect("stamp title/excerpt on one source atom");
    let source_node_id = store
        .upsert_node(source_graph_id, "entity", "Async Promote Source")
        .await
        .expect("upsert source reef");
    for atom_id in &atom_ids {
        store.link_node_atom(source_node_id, *atom_id).await.expect("link source atom");
    }

    // ── Act 1: promote_node_scoped publishes the node and enqueues a job,
    // but does NOT copy any corals itself. ─────────────────────────────────
    let mut tx = begin_account_scoped(&pool, account_id).await.expect("begin scoped tx");
    let promotion = store
        .promote_node_scoped(&mut tx, source_node_id, target_graph.id, user_id, None)
        .await
        .expect("promote_node_scoped");
    tx.commit().await.expect("commit publish");

    assert_eq!(promotion.corals_requested, 3, "all 3 attached atoms were resolved");
    let job_id = promotion.job_id.expect("a non-empty promotion must enqueue a job");

    let atoms_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
        .bind(target_graph.id)
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(atoms_before, 0, "the publish must not copy corals synchronously");

    let job_status: String = sqlx::query_scalar("SELECT status::text FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("job row exists");
    assert_eq!(job_status, "pending");

    // ── Act 2: the worker drains the job. `WorkerTenant` gets its OWN
    // `PostgresStore` handle (same pool, cheap to construct) so `store`
    // above stays available for the re-promote call further down. ─────────
    let t = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
    };
    selected_publication_fixture::assert_legacy_refusal(
        &pool, &t, &r2, account_id, &[source_graph_id, target_graph.id], job_id,
    ).await;

    // Actual bounded Store transport maintenance, explicitly not legacy cloud
    // consent or worker execution. No fake queue completion/derivation is used.
    let blob_ctx = AtomBlobCopyCtx { blobs: &r2, org_id: None, dek: &dek };
    let mut copy_tx = begin_account_scoped(&pool, account_id).await.unwrap();
    let copied = store.copy_atoms_to_graph_scoped(&mut copy_tx, &atom_ids,
        target_graph.id, BlobCopyMode::R2(&blob_ctx)).await.unwrap();
    store.link_node_atoms_batch_scoped(&mut copy_tx, promotion.target_node_id, &copied).await.unwrap();
    copy_tx.commit().await.unwrap();


    // ── Assert: explicit maintenance copies preserve metadata and links. ───
    let atoms_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
        .bind(target_graph.id)
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(atoms_after, 3, "the explicit scoped maintenance copy must retain every requested coral");

    // Title-always-present (#837) carry-through, scoped maintenance path (see the
    // stamp above): the copy of `async-0` in the target graph must keep its
    // source's title/excerpt, not land NULL.
    let (copied_title, copied_excerpt): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT title, excerpt FROM atoms WHERE graph_id = $1 AND source_id = $2",
    )
    .bind(target_graph.id)
    .bind("async-0")
    .fetch_one(&pool)
    .await
    .expect("promoted copy of async-0 exists in the target graph");
    assert_eq!(
        copied_title.as_deref(),
        Some(async_source_title),
        "title-always-present (#837): the scoped maintenance copy must carry the source's title"
    );
    assert_eq!(
        copied_excerpt.as_deref(),
        Some(async_source_excerpt),
        "title-always-present (#837): the scoped maintenance copy must carry the source's excerpt"
    );

    let linked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM node_atoms na JOIN atoms a ON a.atom_id = na.atom_id \
         WHERE na.node_id = $1 AND a.graph_id = $2",
    )
    .bind(promotion.target_node_id)
    .bind(target_graph.id)
    .fetch_one(&pool)
    .await
    .expect("count linked");
    assert_eq!(linked, 3, "every copied coral must be linked to the published reef");

    // The legacy worker refused before downstream derivation. That unavailable
    // worker done/progress/automatic-six-jobs claim is intentionally retired.
    let derivation_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE account_id=$1 AND kind IN ('ExtractEntities','ExtractRelationships')",
    ).bind(account_id).fetch_one(&pool).await.unwrap();
    assert_eq!(derivation_jobs, 0, "a consent refusal never starts legacy derivation");

    // ── Idempotency: re-running the drained worker (nothing left to claim)
    // and an explicit second scoped maintenance copy must not error or
    // duplicate corals. ──────────────────────────────────────────────────────
    let idle = promote::tick(&t, Some(&r2)).await.expect("tick");
    assert_eq!(idle.processed, 0, "no PromoteNode jobs should remain");

    let mut tx2 = begin_account_scoped(&pool, account_id).await.expect("begin scoped tx 2");
    let repromotion = store
        .promote_node_scoped(&mut tx2, source_node_id, target_graph.id, user_id, None)
        .await
        .expect("re-promote");
    tx2.commit().await.expect("commit re-promote");
    assert!(!repromotion.created, "re-promoting the same reef must update, not duplicate");

    if let Some(retry_job) = repromotion.job_id {
        selected_publication_fixture::assert_legacy_refusal(
            &pool, &t, &r2, account_id, &[source_graph_id, target_graph.id], retry_job,
        ).await;
        let mut retry_tx = begin_account_scoped(&pool, account_id).await.unwrap();
        let retried = store.copy_atoms_to_graph_scoped(&mut retry_tx, &atom_ids,
            target_graph.id, BlobCopyMode::R2(&blob_ctx)).await.unwrap();
        store.link_node_atoms_batch_scoped(&mut retry_tx, repromotion.target_node_id, &retried).await.unwrap();
        retry_tx.commit().await.unwrap();
        let atoms_final: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
            .bind(target_graph.id)
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(atoms_final, 3, "re-promoting must not duplicate corals (ON CONFLICT DO NOTHING)");
    }
}

/// Three independent current retained originals each need their own exact
/// selected consent; retries preserve the same three readable destinations.
#[tokio::test]
async fn three_encrypted_originals_require_exact_consent_and_reader_roundtrip_each() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else { eprintln!("{SKIP_REASON}"); return; };
    let Some(env) = r2_test_env() else { eprintln!("{SKIP_REASON}"); return; };
    set_test_kek();
    let dek = generate_dek();
    let wrapped = wrap_dek(&load_kek().unwrap(), &dek).unwrap();
    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap();
    let (account,user,_) = seed_tenant(&pool,&wrapped,"three-selected-originals").await;
    let r2 = Arc::new(R2Client::new_with_endpoint_override("unused-account-id",&env.access_key,
        &env.secret_key,&env.bucket,&env.endpoint).await);
    selected_publication_fixture::selected_encrypted_roundtrip(&pool,r2,&dek,account,user,3,"three_encrypted_originals_require_exact_consent_and_reader_roundtrip_each").await;
    selected_publication_fixture::cleanup_account(&pool,account).await;
    sqlx::query("DELETE FROM users WHERE id=$1").bind(user).execute(&pool).await.unwrap();
}
