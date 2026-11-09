//! Local encrypted-blob copy maintenance and current P8 publication tests.
//! Legacy whole-source workers intentionally refuse consent. Their refusal has
//! a real no-copy/no-child-state test; scoped Store maintenance still proves
//! re-uploaded keys, independent DEK resolution and exact R2 content fidelity.
//! A separate actual selected-publication + authorized evidence-reader case
//! proves current user value from a real encrypted original, without copying
//! private whole-source metadata or enabling any paid worker.
//!
//! # Requirements (all three must be set, or every test here skips loudly)
//! - `RELINFER_TEST_DATABASE_URL` — a Postgres with `migrations_control`
//!   applied (same convention as `promote_node_async_pg.rs` / `promote_node_
//!   corals_pg.rs`, this suite's closest precedent).
//! - `R2_TEST_ENDPOINT_OVERRIDE` — a local S3-compatible fake's endpoint
//!   (e.g. MinIO: `http://localhost:19000`), plus `R2_TEST_ACCESS_KEY`,
//!   `R2_TEST_SECRET_KEY`, and `R2_TEST_BUCKET` (the bucket must already
//!   exist — see `r2_extract_success_pg.rs`'s module doc for the exact
//!   docker/mc commands to stand one up).
//!
//! # Run
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5442/coralswarm_relinfer_test \
//!   R2_TEST_ENDPOINT_OVERRIDE=http://localhost:19000 \
//!   R2_TEST_ACCESS_KEY=minioadmin \
//!   R2_TEST_SECRET_KEY=minioadmin \
//!   R2_TEST_BUCKET=cs-test-bucket \
//!     cargo test -p coralswarm-backend --test promote_r2_reupload_pg -- --nocapture

use std::sync::Arc;

#[path = "support/selected_publication_fixture.rs"]
mod selected_publication_fixture;

use coralswarm_backend::store::{AtomBlobCopyCtx, BlobCopyMode, PostgresStore, Store};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::promote;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::r2::{assert_blob_key_org_scope, blob_key_for, R2Client};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Mirrors `r2_extract_success_pg.rs`'s `set_test_kek` exactly — a fixed,
/// deterministic KEK so wrap/unwrap round-trips without touching the real
/// `MASTER_KEK_HEX`.
// SAFETY: this binary runs with `--test-threads=1` (see this crate's
// `ci.yml` step for this suite), so no two of this file's tests ever call
// `set_var`/read `MASTER_KEK_HEX` concurrently; each of the two tests that
// needs a KEK (the two `NoR2`-mode tests below never unwrap one — `NoR2`
// never touches R2 or a DEK at all) calls this immediately before its own
// `load_kek()`, establishing the value before it is ever read.
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
     MinIO — see r2_extract_success_pg.rs's module doc comment for the docker commands) to run \
     this live promote-R2-reupload regression";

/// Seed `user -> account(dek_wrapped) -> graph`, returning
/// `(pool, account_id, user_id, graph_id)`. Every test in this file shares
/// one account (promotion never crosses accounts), so this is called once.
async fn seed_tenant(pool: &sqlx::PgPool, dek_wrapped: &[u8], name: &str) -> (Uuid, Uuid, Uuid) {
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
    .bind(dek_wrapped)
    .fetch_one(pool)
    .await
    .expect("insert account with wrapped DEK");
    let graph = store.create_graph(account_id, name, "personal").await.expect("create graph");
    (account_id, user_id, graph.id)
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Legacy worker refuses whole-source consent. The bounded scoped-copy
//    maintenance seam still re-uploads the coral's content under the
//    correct key, and a reader (the exact assert_blob_key_org_scope +
//    get_atom_blob sequence routes/atoms.rs::get_atom / mcp::tools::read
//    both run) gets the ORIGINAL content back — non-null, byte-identical.
// ═══════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn legacy_worker_refuses_consent_while_scoped_copy_retains_content_and_reader_fidelity() {
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

    let pool = PgPoolOptions::new().max_connections(4).connect(&db_url).await.expect("connect");
    let (account_id, user_id, source_graph_id) =
        seed_tenant(&pool, &wrapped, "promote-r2-source").await;
    let store = PostgresStore::new(pool.clone());
    let target_graph = store
        .create_graph(account_id, "promote-r2-target-team", "team")
        .await
        .expect("create target team graph");

    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    // ── Seed a REAL R2-backed source atom (user A's reef) ────────────────────
    let source_atom_id = Uuid::new_v4();
    let plaintext = format!("promote-r2-reupload regression content {}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, 'placeholder-never-read', '{}', now())",
    )
    .bind(source_atom_id)
    .bind(source_graph_id)
    .bind(source_atom_id.to_string())
    .execute(&pool)
    .await
    .expect("insert source atom row");
    let source_key = r2
        .put_atom_blob(None, source_graph_id, source_atom_id, plaintext.as_bytes(), &dek)
        .await
        .expect("seed source blob in R2");
    sqlx::query("UPDATE atoms SET content_blob_key = $1 WHERE atom_id = $2")
        .bind(&source_key)
        .bind(source_atom_id)
        .execute(&pool)
        .await
        .expect("stamp source atom's real blob key");

    let source_node_id = store
        .upsert_node(source_graph_id, "entity", "Promote R2 Source")
        .await
        .expect("upsert source reef");
    store.link_node_atom(source_node_id, source_atom_id).await.expect("link source atom");

    // ── Act 1: promote_node_scoped publishes the node and enqueues a job,
    // WITHOUT copying/uploading anything itself (async-promote PR). ─────────
    let mut tx = begin_account_scoped(&pool, account_id).await.expect("begin scoped tx");
    let promotion = store
        .promote_node_scoped(&mut tx, source_node_id, target_graph.id, user_id, None)
        .await
        .expect("promote_node_scoped");
    tx.commit().await.expect("commit publish");
    assert_eq!(promotion.corals_requested, 1);
    let job_id = promotion.job_id.expect("a non-empty promotion must enqueue a job");

    // ── Act 2: current P8 refuses this whole-source worker before copying. ──
    let t = WorkerTenant {
        tenant: TenantId::Consumer,
        pool: pool.clone(),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
    };
    selected_publication_fixture::assert_legacy_refusal(
        &pool, &t, &r2, account_id, &[source_graph_id, target_graph.id], job_id,
    ).await;

    // Explicit bounded transport maintenance seam, not cloud publication consent:
    // preserve the real new-key/copy/independent-DEK reader regression below.
    let mut copy_tx = begin_account_scoped(&pool, account_id).await.unwrap();
    let blob_ctx = AtomBlobCopyCtx { blobs: &r2, org_id: None, dek: &dek };
    let copied = store.copy_atoms_to_graph_scoped(
        &mut copy_tx, &[source_atom_id], target_graph.id, BlobCopyMode::R2(&blob_ctx),
    ).await.unwrap();
    assert_eq!(copied.len(), 1);
    store.link_node_atoms_batch_scoped(&mut copy_tx, promotion.target_node_id, &copied).await.unwrap();
    copy_tx.commit().await.unwrap();

    // ── Assert: the copied row's key is CORRECT (blob_key_for, consumer
    // plane => 2-segment, org_id=None) — never the source's key, never a
    // hand-built string. ──────────────────────────────────────────────────
    let (target_atom_id, target_blob_key): (Uuid, String) =
        sqlx::query_as("SELECT atom_id, content_blob_key FROM atoms WHERE graph_id = $1")
            .bind(target_graph.id)
            .fetch_one(&pool)
            .await
            .expect("exactly one promoted atom in the target graph");
    assert_ne!(target_atom_id, source_atom_id, "the promoted copy gets a FRESH atom id");
    assert_ne!(target_blob_key, source_key, "the promoted copy gets its OWN key, never the source's");
    let expected_key =
        blob_key_for(None, target_graph.id, target_atom_id).expect("valid consumer-plane key");
    assert_eq!(
        target_blob_key, expected_key,
        "the copy's key must be exactly blob_key_for(org_id, target_graph, new_atom_id)"
    );
    assert!(
        assert_blob_key_org_scope(&target_blob_key, None).is_ok(),
        "a consumer-plane key must pass assert_blob_key_org_scope(_, None)"
    );

    // ── The bug this fixes: before it, this GET 404'd (nothing was ever
    // uploaded at the copy's key) and every reader nulled the content. Now
    // it must round-trip byte-identically.
    //
    // `routes::atoms::get_atom` (the real HTTP handler) and `mcp::tools::
    // read::get_atom` both live in modules this crate declares `mod`, not
    // `pub mod` (`routes`/`mcp` in `lib.rs`) — genuinely unreachable from an
    // external `backend/tests/*.rs` binary (which links this crate as an
    // ordinary dependency), even though the handler fns themselves are
    // `pub`. This test therefore drives the DEEPEST *reachable* shared
    // pieces of that same sequence instead of reproducing it ad hoc:
    // `Store::graph_account_id` (the account-bootstrap SECURITY DEFINER
    // resolver `get_atom` itself calls, via `resolve_atoms_account_id`, to
    // learn WHOSE DEK decrypts this atom before it's ever readable) into a
    // FRESH `DekCache` (member B's own request-scoped cache — never the
    // promoting user's `dek` variable from earlier in this test), then the
    // same `assert_blob_key_org_scope` + `get_atom_blob` call both readers
    // make. `crypto::resolve_dek_cached_dispatch(None, ...)` is the exact
    // Personal/flag-off arm `get_atom` dispatches to when no
    // `TenancyDecision` is present (this test's `TenantId::Consumer`
    // scenario) — so this exercises the REAL "resolve DEK by the atom's
    // OWN graph's account" path, not an assumption that it equals the
    // promoter's.
    let member_b_atom_acct = store
        .graph_account_id(target_graph.id)
        .await
        .expect("graph_account_id must succeed")
        .expect("target graph must resolve to an account — this is the account-bootstrap step \
                 get_atom's own resolve_atoms_account_id call performs");
    assert_eq!(
        member_b_atom_acct, account_id,
        "the promoted copy's account-bootstrap resolution must land on the SAME account that owns \
         the target graph"
    );
    let member_b_dek_cache = coralswarm_backend::dek_cache::DekCache::new();
    let member_b_dek = coralswarm_backend::crypto::resolve_dek_cached_dispatch(
        None,
        &pool,
        member_b_atom_acct,
        &member_b_dek_cache,
    )
    .await
    .expect("member B's independent DEK resolution must succeed")
    .expect("member B's independent DEK resolution must find a DEK");
    assert_eq!(
        member_b_dek, dek,
        "member B's independently-resolved DEK must be the SAME key that encrypted the content \
         (same-pool promotion never crosses accounts)"
    );
    assert!(
        coralswarm_core::r2::assert_blob_key_org_scope(&target_blob_key, None).is_ok(),
        "member B's request-scope org check (None on the consumer plane) must accept this key"
    );
    let read_back = r2.get_atom_blob(&target_blob_key, &member_b_dek).await.expect(
        "user B reading the promoted coral must find real content at the copy's key — this is \
         the exact defect this fix closes",
    );
    assert_eq!(
        String::from_utf8_lossy(&read_back),
        plaintext,
        "the promoted copy's content must be byte-identical to the source's"
    );

    // ── Idempotency: draining again (nothing left to claim) must not touch
    // the already-correct key or duplicate the upload. ───────────────────────
    let idle = promote::tick(&t, Some(&r2)).await.expect("tick");
    assert_eq!(idle.processed, 0);
    let (still_target_atom_id, still_blob_key): (Uuid, String) =
        sqlx::query_as("SELECT atom_id, content_blob_key FROM atoms WHERE graph_id = $1")
            .bind(target_graph.id)
            .fetch_one(&pool)
            .await
            .expect("still exactly one promoted atom");
    assert_eq!(still_target_atom_id, target_atom_id);
    assert_eq!(still_blob_key, target_blob_key, "re-draining must not touch an already-correct key");

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Unit-shaped check (still against a real DB — `copy_atoms_to_graph_scoped`
//    is a SQL method): the copied key equals `blob_key_for(org, target_graph,
//    new_atom)` and passes `assert_blob_key_org_scope` under an ORG request
//    — simulated via `AtomBlobCopyCtx::org_id`, exactly the parameter
//    `workers::promote` derives from `TenantId::Org(_)` for a real org
//    tenant (a tenant DB has no notion of "org" baked into its schema; see
//    `promoted_blob_backfill::resolve_org_id`'s doc comment for where the
//    app really gets it — it is always an explicit caller-supplied value,
//    which is exactly what this test exercises).
// ═══════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn org_scoped_copy_key_matches_blob_key_for_and_passes_the_scope_assert() {
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

    let pool = PgPoolOptions::new().max_connections(4).connect(&db_url).await.expect("connect");
    let (account_id, _user_id, source_graph_id) =
        seed_tenant(&pool, &wrapped, "promote-r2-org-source").await;
    let store = PostgresStore::new(pool.clone());
    let target_graph = store
        .create_graph(account_id, "promote-r2-org-target", "org")
        .await
        .expect("create target org graph");

    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    let source_atom_id = Uuid::new_v4();
    let plaintext = b"org-scoped promote copy content";
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, 'placeholder-never-read', '{}', now())",
    )
    .bind(source_atom_id)
    .bind(source_graph_id)
    .bind(source_atom_id.to_string())
    .execute(&pool)
    .await
    .expect("insert source atom row");
    let source_key = r2
        .put_atom_blob(None, source_graph_id, source_atom_id, plaintext, &dek)
        .await
        .expect("seed source blob");
    sqlx::query("UPDATE atoms SET content_blob_key = $1 WHERE atom_id = $2")
        .bind(&source_key)
        .bind(source_atom_id)
        .execute(&pool)
        .await
        .expect("stamp source key");

    let org_id = "org_test0000000000000000000001";
    let ctx = AtomBlobCopyCtx { blobs: &r2, org_id: Some(org_id), dek: &dek };

    let mut tx = pool.begin().await.expect("begin tx");
    let copied = store
        .copy_atoms_to_graph_scoped(
            &mut tx,
            &[source_atom_id],
            target_graph.id,
            BlobCopyMode::R2(&ctx),
        )
        .await
        .expect("copy_atoms_to_graph_scoped with an org blob_ctx");
    tx.commit().await.expect("commit copy");
    assert_eq!(copied.len(), 1);
    let new_atom_id = copied[0];

    let stored_key: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(new_atom_id)
            .fetch_one(&pool)
            .await
            .expect("copied row exists");

    let expected =
        blob_key_for(Some(org_id), target_graph.id, new_atom_id).expect("valid org-scoped key");
    assert_eq!(stored_key, expected, "copied key must equal blob_key_for(org, target_graph, new_atom)");
    assert!(stored_key.starts_with(&format!("{org_id}/")), "org-scoped key must carry the org prefix");
    assert!(
        assert_blob_key_org_scope(&stored_key, Some(org_id)).is_ok(),
        "the copied key must pass assert_blob_key_org_scope under the SAME org"
    );
    assert!(
        assert_blob_key_org_scope(&stored_key, Some("org_someone_else000000000001")).is_err(),
        "the copied key must be REJECTED under a DIFFERENT org's request"
    );

    let read_back = r2.get_atom_blob(&stored_key, &dek).await.expect("read back the org-scoped copy");
    assert_eq!(&read_back[..], plaintext);

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Fail-closed: a source atom with no usable content under
//    `BlobCopyMode::NoR2` (no R2 client on this server AND no
//    `content_inline` fallback) must never produce a committed, contentless
//    copy — the exact shape of the original bug, and also the shape the old
//    `blob_ctx: Option<&AtomBlobCopyCtx>` signature's ambiguous `None` used
//    to cover (a caller that had a real ctx but passed `None` by mistake was
//    indistinguishable, at the type level, from a genuinely R2-less server —
//    seeing this test still fail closed under the new, unambiguous
//    `BlobCopyMode::NoR2` proves that collapse didn't just move somewhere
//    else). The copy must error and nothing must land.
// ═══════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn copy_under_no_r2_mode_with_no_inline_fallback_fails_closed_instead_of_committing_a_contentless_row()
 {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let pool = PgPoolOptions::new().max_connections(4).connect(&db_url).await.expect("connect");
    let store = PostgresStore::new(pool.clone());
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
    let source_graph = store
        .create_graph(account_id, "no-blob-ctx-source", "personal")
        .await
        .expect("create source graph");
    let target_graph =
        store.create_graph(account_id, "no-blob-ctx-target", "team").await.expect("create target");

    // A source atom that HAS a `content_blob_key` (every atom does — the
    // column is `NOT NULL`) but NO `content_inline` fallback — never
    // actually uploaded anywhere, which is fine: this test must fail before
    // ever attempting a real R2 call (`BlobCopyMode::NoR2` never attempts
    // one at all).
    let source_atom_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, $4, '{}', now())",
    )
    .bind(source_atom_id)
    .bind(source_graph.id)
    .bind(source_atom_id.to_string())
    .bind(format!("{}/{}", source_graph.id, source_atom_id))
    .execute(&pool)
    .await
    .expect("insert source atom row");

    let mut tx = pool.begin().await.expect("begin tx");
    let result = store
        .copy_atoms_to_graph_scoped(
            &mut tx,
            &[source_atom_id],
            target_graph.id,
            BlobCopyMode::NoR2,
        )
        .await;
    assert!(
        result.is_err(),
        "copying under NoR2 with no content_inline fallback must fail closed, never silently \
         succeed"
    );
    // The transaction was never committed by the method (it returned Err
    // before doing so) — drop it explicitly to make the rollback visible in
    // this test rather than relying on `Drop`.
    tx.rollback().await.ok();

    let landed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
        .bind(target_graph.id)
        .fetch_one(&pool)
        .await
        .expect("count target atoms");
    assert_eq!(landed, 0, "a failed copy must never leave a row behind — no contentless copy");

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. `BlobCopyMode::NoR2` (no R2 client anywhere on this server — the
//    routes_b2_rls_pg.rs / rls_probe.rs shape, and every R2-less dev stack):
//    a source atom whose real content lives in `content_inline` copies
//    successfully, carrying that inline content AND its placeholder
//    `content_blob_key` verbatim into the target — no re-upload attempted,
//    no error. This is the regression 0709e6e8 introduced on this branch:
//    every atom has SOME `content_blob_key` (the column is `NOT NULL`), so
//    a gate that only checked "does the source have a blob key" fired for
//    this atom too and refused the copy even though a real R2 client was
//    never needed.
// ═══════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn copy_under_no_r2_mode_carries_inline_content_and_placeholder_key_verbatim() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let pool = PgPoolOptions::new().max_connections(4).connect(&db_url).await.expect("connect");
    let store = PostgresStore::new(pool.clone());
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
    let source_graph = store
        .create_graph(account_id, "no-r2-inline-source", "personal")
        .await
        .expect("create source graph");
    let target_graph =
        store.create_graph(account_id, "no-r2-inline-target", "team").await.expect("create target");

    // Exactly the rls_probe.rs / R2-less-dev-stack shape: a placeholder
    // `content_blob_key` seeded ALONGSIDE real `content_inline`.
    let source_atom_id = Uuid::new_v4();
    let inline_body = "narwhal orchestra rehearsal notes, no R2 anywhere on this server";
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             content_inline, participants, timestamp) \
         VALUES ($1, $2, 'test', $3, $4, $5, '{}', now())",
    )
    .bind(source_atom_id)
    .bind(source_graph.id)
    .bind(source_atom_id.to_string())
    .bind(format!("{}/{}", source_graph.id, source_atom_id))
    .bind(inline_body)
    .execute(&pool)
    .await
    .expect("insert source atom row");

    let mut tx = pool.begin().await.expect("begin tx");
    let copied = store
        .copy_atoms_to_graph_scoped(&mut tx, &[source_atom_id], target_graph.id, BlobCopyMode::NoR2)
        .await
        .expect("copy_atoms_to_graph_scoped under NoR2 with inline content must succeed");
    tx.commit().await.expect("commit copy");
    assert_eq!(copied.len(), 1, "exactly one coral landed in the target");

    let (content_inline, content_blob_key): (Option<String>, String) = sqlx::query_as(
        "SELECT content_inline, content_blob_key FROM atoms WHERE atom_id = $1",
    )
    .bind(copied[0])
    .fetch_one(&pool)
    .await
    .expect("read back the NoR2 copy");
    assert_eq!(
        content_inline.as_deref(),
        Some(inline_body),
        "content_inline must be carried verbatim under NoR2"
    );
    assert_eq!(
        content_blob_key,
        format!("{}/{}", target_graph.id, copied[0]),
        "the placeholder key must be left exactly as the INSERT computed it — no re-upload, no \
         correction, under NoR2"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Title/excerpt carry-through (pre-staged for #837, "titles always
//    present": `atoms.title`/`atoms.excerpt` land on main before this PR
//    does). `copy_atoms_to_graph_core` is the ONE place both
//    `copy_atoms_to_graph`/`_scoped` route through, so this is the single
//    guard that keeps a future rebase from silently dropping #837's fix —
//    without it, `display_title` papering over a NULL `title` at read time
//    would hide the regression, and the async title-backfill sweep would
//    silently re-title (and re-bill) every promoted coral from scratch.
//    R2 mode: the source atom's CONTENT lives in R2 (the re-upload path
//    this whole file exists to cover) while `title`/`excerpt` are ordinary
//    columns, never R2-backed — this proves the two are carried
//    independently of that re-upload machinery.
// ═══════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn copy_scoped_carries_title_and_excerpt_through_in_r2_mode() {
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

    let pool = PgPoolOptions::new().max_connections(4).connect(&db_url).await.expect("connect");
    let (account_id, _user_id, source_graph_id) =
        seed_tenant(&pool, &wrapped, "promote-title-r2-source").await;
    let store = PostgresStore::new(pool.clone());
    let target_graph = store
        .create_graph(account_id, "promote-title-r2-target", "team")
        .await
        .expect("create target team graph");

    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    let source_atom_id = Uuid::new_v4();
    let plaintext = b"promote title/excerpt carry-through content, R2 mode";
    let source_title = "A distinct source headline";
    let source_excerpt = "A distinct source excerpt, independent of the headline";
    // `title_kind = 'llm'` (not 'fallback') here specifically: this proves
    // the copy carries whatever kind the source actually has, not just
    // whether `title` is set at all — see the `title_kind` assertion below
    // for why the distinction matters (CodeRabbit round 1 on #837).
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp, title, title_kind, excerpt) \
         VALUES ($1, $2, 'test', $3, 'placeholder-never-read', '{}', now(), $4, 'llm', $5)",
    )
    .bind(source_atom_id)
    .bind(source_graph_id)
    .bind(source_atom_id.to_string())
    .bind(source_title)
    .bind(source_excerpt)
    .execute(&pool)
    .await
    .expect("insert source atom row with title+excerpt");
    let source_key = r2
        .put_atom_blob(None, source_graph_id, source_atom_id, plaintext, &dek)
        .await
        .expect("seed source blob");
    sqlx::query("UPDATE atoms SET content_blob_key = $1 WHERE atom_id = $2")
        .bind(&source_key)
        .bind(source_atom_id)
        .execute(&pool)
        .await
        .expect("stamp source key");

    let ctx = AtomBlobCopyCtx { blobs: &r2, org_id: None, dek: &dek };
    let mut tx = pool.begin().await.expect("begin tx");
    let copied = store
        .copy_atoms_to_graph_scoped(
            &mut tx,
            &[source_atom_id],
            target_graph.id,
            BlobCopyMode::R2(&ctx),
        )
        .await
        .expect("copy_atoms_to_graph_scoped in R2 mode");
    tx.commit().await.expect("commit copy");
    assert_eq!(copied.len(), 1);

    let (title, title_kind, excerpt): (Option<String>, Option<String>, Option<String>) =
        sqlx::query_as("SELECT title, title_kind, excerpt FROM atoms WHERE atom_id = $1")
            .bind(copied[0])
            .fetch_one(&pool)
            .await
            .expect("read back the copy");
    assert_eq!(
        title.as_deref(),
        Some(source_title),
        "title-always-present (#837): the promoted copy must carry the source's title, not drop it"
    );
    assert_eq!(
        title_kind.as_deref(),
        Some("llm"),
        "title_kind must carry through the copy too, not just title/excerpt (CodeRabbit round 1 \
         on #837) -- a NULL here reintroduces the exact `title SET, title_kind NULL` shape M1's \
         migration fix exists to close, on every future promotion"
    );
    assert_eq!(
        excerpt.as_deref(),
        Some(source_excerpt),
        "title-always-present (#837): the promoted copy must carry the source's excerpt, not drop it"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Same carry-through under `BlobCopyMode::NoR2` (the inline-content path,
//    §4 above's fixture shape). `title`/`excerpt` are ordinary columns,
//    independent of the R2 on/off toggle, so this must pass identically to
//    the R2-mode test above — proving the fix isn't accidentally gated on
//    `blob_mode` at all.
// ═══════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn copy_scoped_carries_title_and_excerpt_through_in_no_r2_mode() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let pool = PgPoolOptions::new().max_connections(4).connect(&db_url).await.expect("connect");
    let store = PostgresStore::new(pool.clone());
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
    let source_graph = store
        .create_graph(account_id, "no-r2-title-source", "personal")
        .await
        .expect("create source graph");
    let target_graph =
        store.create_graph(account_id, "no-r2-title-target", "team").await.expect("create target");

    let source_atom_id = Uuid::new_v4();
    let inline_body = "narwhal orchestra rehearsal notes, title/excerpt carry-through, NoR2 mode";
    let source_title = "No-R2 source headline";
    let source_excerpt = "No-R2 source excerpt";
    // `title_kind = 'fallback'` here (the R2-mode test above uses 'llm') —
    // between the two, both values this column can take are exercised, and
    // this one specifically is the shape the sweep's candidate predicate
    // `(title IS NULL OR title_kind = 'fallback')` targets for an LLM
    // upgrade, so it must survive the copy unchanged (CodeRabbit round 1 on
    // #837).
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             content_inline, participants, timestamp, title, title_kind, excerpt) \
         VALUES ($1, $2, 'test', $3, $4, $5, '{}', now(), $6, 'fallback', $7)",
    )
    .bind(source_atom_id)
    .bind(source_graph.id)
    .bind(source_atom_id.to_string())
    .bind(format!("{}/{}", source_graph.id, source_atom_id))
    .bind(inline_body)
    .bind(source_title)
    .bind(source_excerpt)
    .execute(&pool)
    .await
    .expect("insert source atom row");

    let mut tx = pool.begin().await.expect("begin tx");
    let copied = store
        .copy_atoms_to_graph_scoped(&mut tx, &[source_atom_id], target_graph.id, BlobCopyMode::NoR2)
        .await
        .expect("copy_atoms_to_graph_scoped under NoR2 must succeed");
    tx.commit().await.expect("commit copy");
    assert_eq!(copied.len(), 1);

    let (title, title_kind, excerpt): (Option<String>, Option<String>, Option<String>) =
        sqlx::query_as("SELECT title, title_kind, excerpt FROM atoms WHERE atom_id = $1")
            .bind(copied[0])
            .fetch_one(&pool)
            .await
            .expect("read back the copy");
    assert_eq!(
        title.as_deref(),
        Some(source_title),
        "title-always-present (#837): NoR2 copy must carry the source's title, not drop it"
    );
    assert_eq!(
        title_kind.as_deref(),
        Some("fallback"),
        "the SCOPED twin must carry title_kind under NoR2 too, not just the R2-mode test above \
         (CodeRabbit round 1 on #837)"
    );
    assert_eq!(
        excerpt.as_deref(),
        Some(source_excerpt),
        "title-always-present (#837): NoR2 copy must carry the source's excerpt, not drop it"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

/// Current end-user path: exact consent from an encrypted retained original,
/// then a separate ordinary reader sees the selection and not its private source.
#[tokio::test]
async fn selected_encrypted_original_is_published_and_read_through_authorized_handlers() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else { eprintln!("{SKIP_REASON}"); return; };
    let Some(env) = r2_test_env() else { eprintln!("{SKIP_REASON}"); return; };
    set_test_kek();
    let dek = generate_dek();
    let wrapped = wrap_dek(&load_kek().unwrap(), &dek).unwrap();
    let pool = PgPoolOptions::new().max_connections(4).connect(&db_url).await.unwrap();
    let (account,user,_) = seed_tenant(&pool,&wrapped,"selected-encrypted-source").await;
    let r2 = Arc::new(R2Client::new_with_endpoint_override("unused-account-id",&env.access_key,
        &env.secret_key,&env.bucket,&env.endpoint).await);
    selected_publication_fixture::selected_encrypted_roundtrip(&pool,r2,&dek,account,user,1,"selected_encrypted_original_is_published_and_read_through_authorized_handlers").await;
    selected_publication_fixture::cleanup_account(&pool,account).await;
    sqlx::query("DELETE FROM users WHERE id=$1").bind(user).execute(&pool).await.unwrap();
}
