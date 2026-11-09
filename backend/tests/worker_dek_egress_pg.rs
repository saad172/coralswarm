//! Worker-path invariant tests for org-tenancy PR 2.1:
//!
//! 1. **DEK plane split in the embed worker**: an ORG tick resolves its DEK
//!    from the tenant DB's `account_keys` — and when that row is missing it
//!    FAILS, even if the control plane holds a DEK for the same account
//!    (no cross-plane fallback; wrong-key encryption is unrecoverable). A
//!    CONSUMER tick keeps resolving from control `accounts.dek_wrapped`.
//!
//! Gated on `ORGT_TEST_BASE_URL`; same scratch-DB pattern as
//! `dek_relocation_pg.rs` / `worker_harness_pg.rs`.

use std::sync::Arc;
use std::time::Instant;

use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::workers::embed;
use coralswarm_core::embed::{EmbedBatchFuture, EmbedProvider};
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

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

/// Fixed test KEK (same pattern as `dek_relocation_pg.rs`).
fn set_test_kek() {
    // SAFETY: `set_var` mutates process-global state with no synchronization.
    // Every test in this process sets `MASTER_KEK_HEX` to the SAME constant
    // value, so a concurrent write between parallel tests is benign (identical
    // bytes, no torn read of a differing value) — the same pattern used by
    // `dek_relocation_pg.rs`. The DEK-plane assertion under test does not
    // depend on the KEK value, only on which table the wrapped DEK is read from.
    unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) };
}

// ── Stub providers ────────────────────────────────────────────────────────────

/// 384-dim constant-vector embedder with no gated transport: the managed
/// embedding gate can never admit it, so every call would be an ungated cloud
/// dispatch, and tests assert this stays zero. The keyword index (written
/// only with the decrypted text) is what proves which DEK the worker used.
static CLOUD_EMBED_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
async fn embed_stamp(pool: &PgPool, atom_id: Uuid) -> (bool, Option<String>, bool) {
    sqlx::query_as("SELECT embedding_next IS NOT NULL, embed_model_next, content_tsv IS NOT NULL FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(pool)
        .await
        .unwrap()
}
struct StubEmbedder;
impl EmbedProvider for StubEmbedder {
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [coralswarm_core::embed::EmbedInput],
        _: coralswarm_core::embed::ProviderPermit,
    ) -> EmbedBatchFuture<'a> {
        CLOUD_EMBED_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let n = texts.len();
        Box::pin(async move { Ok(vec![vec![0.1_f32; 384]; n]) })
    }
}

// ── Fixture helpers ───────────────────────────────────────────────────────────

async fn graph_with_atom(pool: &PgPool, account_id: Uuid, content: &str) -> (Uuid, Uuid) {
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name) VALUES ($1, 'wh test') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, \
                            content_inline, \"timestamp\") \
         VALUES ($1, 'note', 's1', 'unused-inline-path', $2, now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(content)
    .fetch_one(pool)
    .await
    .unwrap();
    (graph_id, atom_id)
}

async fn enqueue_embed_job(pool: &PgPool, atom_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO jobs (kind, payload) \
         VALUES ('EmbedAtoms', jsonb_build_object('atom_ids', jsonb_build_array($1::uuid))) \
         RETURNING id",
    )
    .bind(atom_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn job_status(pool: &PgPool, job_id: Uuid) -> String {
    sqlx::query_scalar("SELECT status::text FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn atom_has_embedding(pool: &PgPool, atom_id: Uuid) -> bool {
    sqlx::query_scalar("SELECT embedding_next IS NOT NULL FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

fn worker_tenant(tenant: TenantId, pool: &PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn embed_org_tick_resolves_dek_from_tenant_account_keys_never_control() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping embed_org_tick_resolves_dek_from_tenant_account_keys_never_control: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    set_test_kek();
    let kek = load_kek().expect("test KEK loads");

    let (ctl, ten) = ("orgt_wde_ctl", "orgt_wde_ten");
    let mut conn = admin(&base).await;
    for db in [ctl, ten] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // Control plane: an org account WITH a (decoy) control-plane DEK — if the
    // worker ever fell back across planes this key would make the negative
    // case below silently succeed instead of failing.
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    let decoy_dek = generate_dek();
    let decoy_wrapped = wrap_dek(&kek, &decoy_dek).unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'wde@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) \
         SELECT id, 'org_wde', $1 FROM u RETURNING id",
    )
    .bind(&decoy_wrapped)
    .fetch_one(&control)
    .await
    .unwrap();

    // Tenant plane: fingerprint + the org's REAL DEK in account_keys.
    let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, ten)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_wde')")
        .execute(&tenant)
        .await
        .unwrap();
    let org_dek = generate_dek();
    let org_wrapped = wrap_dek(&kek, &org_dek).unwrap();
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(org_account_id)
        .bind(&org_wrapped)
        .execute(&tenant)
        .await
        .unwrap();

    let depth = tokio::sync::Mutex::new(None::<Instant>);
    let wt = worker_tenant(TenantId::Org("org_wde".into()), &tenant);

    // ── 1. Org tick embeds using the tenant account_keys DEK ─────────────────
    let (_g, atom_id) = graph_with_atom(&tenant, org_account_id, "org content").await;
    let job_id = enqueue_embed_job(&tenant, atom_id).await;
    let tick = embed::tick(&wt, &StubEmbedder, Some(&StubEmbedder), None, None, &depth).await.expect("tick ok");
    assert_eq!(tick.processed, 1);
    assert_ne!(job_status(&tenant, job_id).await, "done", "an ungated provider is never admitted");
    assert_eq!(
        embed_stamp(&tenant, atom_id).await,
        (false, None, true),
        "keyword-indexed via tenant DEK; no cloud vector without managed admission"
    );

    // ── 2. Missing tenant account_keys row ⇒ FAIL, never the control DEK ─────
    // A second account exists on the control plane WITH dek_wrapped, but has
    // no account_keys row in the tenant DB. The org tick must fail its job —
    // if it succeeded, it could only have used the control-plane (decoy) key.
    let orphan_account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'wde2@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id, dek_wrapped) SELECT id, $1 FROM u RETURNING id",
    )
    .bind(&decoy_wrapped)
    .fetch_one(&control)
    .await
    .unwrap();
    let (_g2, atom2) = graph_with_atom(&tenant, orphan_account_id, "no tenant key").await;
    let job2 = enqueue_embed_job(&tenant, atom2).await;
    let tick2 = embed::tick(&wt, &StubEmbedder, Some(&StubEmbedder), None, None, &depth).await.expect("tick ok");
    assert_eq!(tick2.processed, 1);
    // `queue::fail` re-queues with backoff below max_attempts, so the failed
    // attempt shows as `pending` with attempts=1 and a DEK error recorded —
    // the invariant is that it did NOT complete and did NOT embed (which it
    // would have, had the worker fallen back to the control-plane decoy DEK).
    let (status2, attempts2, err2): (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status::text, attempts, error_message FROM jobs WHERE id = $1",
    )
    .bind(job2)
    .fetch_one(&tenant)
    .await
    .unwrap();
    assert_ne!(status2, "done", "missing tenant account_keys row must not complete the job");
    assert_eq!(attempts2, 1, "the attempt was consumed by a DEK failure");
    assert!(
        err2.unwrap_or_default().contains("DEK"),
        "failure must be the DEK lookup, not something incidental"
    );
    assert!(
        !atom_has_embedding(&tenant, atom2).await,
        "no embedding may be written without the tenant-plane DEK (no cross-plane fallback)"
    );

    // ── 3. Consumer tick keeps resolving from control accounts.dek_wrapped ───
    let (_g3, atom3) = graph_with_atom(&control, org_account_id, "personal content").await;
    let job3 = enqueue_embed_job(&control, atom3).await;
    let wt_consumer = worker_tenant(TenantId::Consumer, &control);
    let tick3 = embed::tick(&wt_consumer, &StubEmbedder, Some(&StubEmbedder), None, None, &depth).await.expect("tick ok");
    assert_eq!(tick3.processed, 1);
    assert_ne!(job_status(&control, job3).await, "done", "an ungated provider is never admitted");
    assert_eq!(embed_stamp(&control, atom3).await, (false, None, true));
    assert_eq!(CLOUD_EMBED_CALLS.load(std::sync::atomic::Ordering::SeqCst), 0, "automatic embed job dispatched to an ungated cloud provider");

    for db in [ctl, ten] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
