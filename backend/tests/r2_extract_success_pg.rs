//! Live regression proving a SUCCESSFUL R2 GET actually round-trips end to
//! end (graph-temporal-memory hardening, fix 2) — through the real D5
//! R2-only-atom fetch path (`workers::extract::fetch_r2_only_content`,
//! exposed for this test as `test_support_fetch_r2_only_content` under the
//! `test-support` feature), against a real S3-compatible endpoint (MinIO) via
//! `R2Client::new_with_endpoint_override`.
//!
//! # Why this test exists
//! Before this fix, `R2Client`'s endpoint was hardcoded to real Cloudflare R2
//! with no override anywhere, so nothing in this repo could exercise a
//! *successful* R2 GET: `packages/core/src/r2.rs`'s only real-network
//! coverage was `#[ignore]`-gated behind real `R2_*` credentials, and
//! `workers::extract.rs`'s own test module covered only the FAILURE paths
//! (`r2: None`, DEK missing) — D5's central claim ("R2-only storage is
//! implemented") rested on inspection, not a passing test. This closes that
//! gap using a local fake instead of live Cloudflare credentials.
//!
//! # Requirements (both must be set, or this test skips loudly)
//! - `RELINFER_TEST_DATABASE_URL` — a Postgres with `migrations_control`
//!   applied (same convention as `expand_d1_pg.rs` / `retrieval_eval_pg.rs`).
//! - `R2_TEST_ENDPOINT_OVERRIDE` — a local S3-compatible fake's endpoint
//!   (e.g. MinIO: `http://localhost:19000`), plus `R2_TEST_ACCESS_KEY`,
//!   `R2_TEST_SECRET_KEY`, and `R2_TEST_BUCKET` (the bucket must already
//!   exist — this test does not create buckets, only objects in one).
//!
//! # Standing up the local fake (MinIO)
//!   docker run -d -p 19000:9000 -p 19001:9001 \
//!     -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//!     minio/minio server /data --console-address ":9001"
//!   docker run --rm --network host --entrypoint sh minio/mc -c \
//!     "mc alias set local http://localhost:19000 minioadmin minioadmin && \
//!      mc mb local/cs-test-bucket --ignore-existing"
//!
//! # Run
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55987/graph_hardening_wt \
//!   R2_TEST_ENDPOINT_OVERRIDE=http://localhost:19000 \
//!   R2_TEST_ACCESS_KEY=minioadmin \
//!   R2_TEST_SECRET_KEY=minioadmin \
//!   R2_TEST_BUCKET=cs-test-bucket \
//!     cargo test -p coralswarm-backend --test r2_extract_success_pg -- --nocapture

use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::extract::test_support_fetch_r2_only_content;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::r2::R2Client;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Mirrors `dek_relocation_pg.rs`'s `set_test_kek` exactly — a fixed,
/// deterministic KEK so wrap/unwrap round-trips without touching the real
/// `MASTER_KEK_HEX`.
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
     MinIO — see this file's module doc comment for the docker commands) to run the live \
     successful-R2-GET regression";

#[tokio::test]
async fn fetch_r2_only_content_round_trips_through_a_real_r2_get_against_a_local_fake() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    set_test_kek();
    let kek = load_kek().expect("test KEK loads");
    let dek = generate_dek();
    let wrapped = wrap_dek(&kek, &dek).expect("wrap test DEK");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect to test Postgres");

    let email = format!("r2-success-{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(&wrapped)
    .fetch_one(&pool)
    .await
    .expect("insert account with wrapped DEK");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, &format!("r2-success-{}", Uuid::new_v4()), "personal")
        .await
        .expect("create graph");

    // A REAL R2Client pointed at the local fake via the new endpoint override
    // (graph-temporal-memory hardening, fix 2) — not the JSON-only
    // encrypt/decrypt round-trip `r2.rs`'s existing unit tests already cover,
    // an actual PUT then GET against a real S3-compatible HTTP server.
    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id", // account_id is only used to build the real R2 endpoint URL, irrelevant once overridden
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    let atom_id = Uuid::new_v4();
    let plaintext = b"graph-temporal-memory hardening: this content really round-tripped through R2";
    let blob_key = r2
        .put_atom_blob(None, graph.id, atom_id, plaintext, &dek)
        .await
        .expect("put_atom_blob against the local R2 fake must succeed");

    // The exact D5 R2-only-atom code path: `workers::extract::
    // fetch_r2_only_content` resolves the account's DEK from Postgres, then
    // calls `R2Client::get_atom_blob` — this is the REAL function, not a
    // reimplementation, exercised via its `test-support`-gated wrapper.
    let fetched = test_support_fetch_r2_only_content(
        &TenantId::Consumer,
        &pool,
        Some(&r2),
        account_id,
        &blob_key,
    )
    .await
    .expect("fetch_r2_only_content must succeed against a real, populated R2 blob")
    .expect("a populated blob must come back as Some — None is reserved for empty plaintext");

    assert_eq!(
        fetched.as_bytes(),
        plaintext,
        "the R2-only extraction path must recover the exact plaintext that was PUT"
    );

    // Cleanup: leave no object behind in the shared test bucket.
    r2.delete_atom_blob(&blob_key)
        .await
        .expect("cleanup: delete test blob");
}
