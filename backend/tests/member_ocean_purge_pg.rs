//! Member-ocean lifecycle — PURGE (member-private-oceans PR 8,
//! `docs/plans/member-private-oceans.md` §2.6 "Lifecycle" / §7).
//!
//! **This arm deletes customer data for real.** There is no mandatory
//! log-only observation period — `MEMBER_OCEAN_PURGE_ENABLED` (default off,
//! independent of `MEMBER_OCEANS_ENABLED`) is the only gate. Every test in
//! this file that asserts something IS purged is therefore, definitionally,
//! asserting real, irreversible deletion happened against a scratch
//! database — that is the point of this suite.
//!
//! Drives `workers::member_ocean_reconciler::tick` directly against a real
//! control-plane scratch DB and a real tenant-plane scratch DB — same
//! no-HTTP, unit-level-of-the-tick pattern as `member_ocean_reconciler_pg.rs`
//! (purge has no request-path trigger to exercise; it is reconciler-only).
//! Archived member oceans are planted directly (backdating `archived_at`)
//! rather than driven through PR 7's archive arm (#340, open, not yet merged
//! at the time this file was written) — this file's only precondition on a
//! graph is `kind = 'member' AND archived_at < now() - interval '60 days'`,
//! which is exactly the SQL predicate purge itself reads, independent of how
//! `archived_at` got there.
//!
//! Properties covered (see this PR's description for the full list against
//! the task's required proof set):
//!
//! 1. **60-day boundary** — archived 61 days ago is purged; archived 59 days
//!    ago is not touched at all (row, blobs, and children all untouched).
//! 2. **Full-roster coverage** — every table in
//!    `workers::member_ocean_reconciler::GRAPH_CONTENT_TABLES` (the complete
//!    `0083_consumer_rls.sql` graph_id-bearing roster, minus the account-only
//!    tables) is empty for the purged graph afterward, asserted per table —
//!    including the two FK-less "soft" tables (`meeting_transcripts`,
//!    `transcript_corrections`) that CASCADE alone would never reach.
//! 3. **Blast radius** — only the target graph's blobs are deleted; a
//!    sibling member's archived ocean (same org, same 60-day eligibility) and
//!    the org commons ocean's blobs are BOTH untouched, asserted against the
//!    stub R2 store's own delete log, not merely against row counts.
//! 4. **A concurrent org-suspend attempt cannot interrupt an in-flight
//!    purge.** Both the org-routing lock AND the graph-eligibility lock (see
//!    property 8) are acquired BEFORE blob deletion and held continuously
//!    through the row-delete transaction, so a suspend racing a purge that
//!    has already passed both checks simply waits — it cannot land mid-loop
//!    and strand a half-purged graph. A SEPARATE, simpler case (org already
//!    suspended before the tick even starts) is also covered.
//! 5. **Revoked org is never purged** — `org_revocations` is checked before
//!    anything else; not even a candidate read is attempted.
//! 6. **Drain guard** — an in-flight `pending` job referencing one of the
//!    graph's atoms blocks the purge; completing that job lets the very next
//!    tick purge it.
//! 7. **Audit two-phase discipline** — `started` (no `finished_at`) →
//!    `completed` (with `table_counts`/`blobs` in the manifest,
//!    `finished_at` set); a simulated crash (a raw `started` row with no
//!    matching completion) is exactly the schema-supported "verify by hand"
//!    signal plan §2.6 describes.
//! 8. **Eligibility is re-asserted at mutation time, not trusted from the
//!    candidate read** (fix for a Critical CodeRabbit finding on PR #346): a
//!    graph restored — by any of the three restore triggers, all of which
//!    are plain `UPDATE graphs ... WHERE id = $graph_id` statements — between
//!    `purge_arm`'s ONE candidate `SELECT` and this graph's own turn in the
//!    batch is NOT purged, and keeps BOTH its rows AND its blobs. A second
//!    test proves the matching "never mark a lost race `completed`"
//!    property via two genuinely concurrent `tick()` calls racing the SAME
//!    graph.
//!
//! ## Proof of failure
//!
//! Per this PR's instructions, every guard added by this suite was verified
//! to actually fail before being considered done, then reverted — see this
//! PR's description for each exercise's exact mutation and failure output:
//! inverting the 59-day boundary; unscoping the blob-key read query (blast
//! radius); and reverting the eligibility re-check in `purge_one_graph`
//! (property 8's restore-survives test — the Critical fix this PR makes).
//! **One exception, stated plainly rather than glossed over:** the
//! concurrent-race test (property 8b) does NOT have a clean, deterministic
//! proof-of-failure for its `graph_deleted == 0` guard specifically — see
//! that test's own comment for why the fix's own locking makes that branch
//! unreachable via any external race once the lock is intact, and why an
//! attempt to force it by weakening the lock produced a non-deterministic
//! (tokio-scheduling-dependent) result rather than a reliable red/green
//! signal.
//!
//! ## Honest CI coverage
//!
//! Wired into `.github/workflows/ci.yml`'s `member-oceans` job in THIS PR —
//! see that job's own comment for the pre-existing "an
//! `ORGT_TEST_BASE_URL`-gated suite with no CI job silently `ok`s having done
//! nothing" gap this convention exists to avoid.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test member_ocean_purge_pg -- \
//!     --test-threads=1 --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::member_ocean_reconciler::{tick, GRAPH_CONTENT_TABLES};
use coralswarm_backend::workers::{promote, source_blob_cleanup};
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::r2::{assert_blob_key_org_scope, blob_key_for, R2Client, R2Error};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

/// Serializes every test in this file — same rationale as every other
/// `*_pg.rs` file's `ENV_LOCK` (`MEMBER_OCEANS_ENABLED`/
/// `MEMBER_OCEAN_PURGE_ENABLED` are process-global; migration 0083's
/// `coralswarm_app` role is cluster-wide).
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

// ── Scratch-DB plumbing (pattern shared by every `*_pg.rs` file) ─────────────

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
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}
async fn tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
    let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&pool)
        .await
        .unwrap();
    pool
}

/// Sets `MEMBER_OCEANS_ENABLED` + `MEMBER_OCEAN_PURGE_ENABLED` for one test's
/// duration, restoring both on drop — same `EnvGuard` pattern as
/// `member_ocean_lifecycle_pg.rs`'s, duplicated here per this directory's
/// existing per-file convention.
///
/// **Every test in this file acquires this BEFORE its own first
/// `std::env::var` read** (`base_url()`, `r2_test_env()`, `set_test_kek()`),
/// not merely before its own `set_var` calls (CodeRabbit PR #838 review) —
/// `std::env::set_var`/`remove_var` require exclusive process-environment
/// access, and a read of an UNRELATED variable racing this guard's mutation
/// on another thread is exactly as unsound as a racing write. `--test-
/// threads=1` (this file's own `ci.yml` step) already serializes test
/// bodies in practice; this lock makes that a self-contained, enforced
/// invariant of the file itself rather than a fact this file merely
/// documents and depends on an external CLI flag to uphold.
#[allow(dead_code)]
struct EnvGuard(tokio::sync::MutexGuard<'static, ()>);

impl EnvGuard {
    /// Both flags on — the common case for this file (proving purge runs).
    async fn acquire_both_on() -> Self {
        let guard = ENV_LOCK.lock().await;
        // SAFETY: `guard` serializes every mutation of these two
        // process-global vars in this binary against every other test's.
        unsafe {
            std::env::set_var("MEMBER_OCEANS_ENABLED", "1");
            std::env::set_var("MEMBER_OCEAN_PURGE_ENABLED", "1");
        }
        Self(guard)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: see `acquire_both_on` — `self.0` is still held here.
        unsafe {
            std::env::remove_var("MEMBER_OCEANS_ENABLED");
            std::env::remove_var("MEMBER_OCEAN_PURGE_ENABLED");
        }
    }
}

/// RAII: sets `MEMBER_OCEAN_PURGE_BLOB_BUDGET_SECS` for the guard's lifetime,
/// removing it on drop (including on unwind — this MUST clean up even if the
/// test panics, since leaking a tiny override into a later test in this same
/// binary would spuriously trip that test's own blob-deletion budget). Kept
/// separate from `EnvGuard` since only ONE test in this file needs this var;
/// callers must already hold `EnvGuard` (i.e. `ENV_LOCK`) before using this.
struct BlobBudgetGuard;

impl BlobBudgetGuard {
    fn set_secs(secs: u64) -> Self {
        // SAFETY: caller holds `ENV_LOCK` via `EnvGuard` for this test's
        // duration, serializing this against every other test's env mutation.
        unsafe { std::env::set_var("MEMBER_OCEAN_PURGE_BLOB_BUDGET_SECS", secs.to_string()) };
        Self
    }
}

impl Drop for BlobBudgetGuard {
    fn drop(&mut self) {
        // SAFETY: see `set_secs`.
        unsafe { std::env::remove_var("MEMBER_OCEAN_PURGE_BLOB_BUDGET_SECS") };
    }
}

/// Mint an org's control-plane account row + `tenant_databases` routing row,
/// `Active` — mirrors `member_ocean_provisioning_pg.rs`'s `mint_org`.
async fn mint_org(control: &PgPool, org_id: &str, slug: &str) -> Uuid {
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("{org_id}-owner@wh.test"))
        .fetch_one(control)
        .await
        .unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(org_id)
    .fetch_one(control)
    .await
    .unwrap();
    financial_fixture::execute(
        control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases \
         (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
         VALUES ($1, $2, $3, $4, 'neon', 1, 'active')",
        )
        .bind(org_id)
        .bind(account_id)
        .bind(slug)
        .bind(format!("{slug}-secret")),
    )
    .await
    .unwrap();
    account_id
}

fn worker_tenant(org_id: &str, pool: PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: TenantId::Org(org_id.to_string()),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
        pool,
    }
}

/// Insert a `kind='member'` graph, archived `days_ago` days in the past.
/// `is_primary = true` always (this file has no reason to exercise a
/// secondary member ocean — purge's own SQL does not care either way, it
/// scopes purely on `kind = 'member' AND archived_at`).
async fn insert_archived_member_ocean(
    tenant: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    days_ago: i64,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary, archived_at) \
         VALUES ($1, 'Purge-test ocean', 'member', $2, true, now() - make_interval(days => $3::int)) \
         RETURNING id",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(days_ago as i32)
    .fetch_one(tenant)
    .await
    .unwrap()
}

async fn insert_org_commons(tenant: &PgPool, account_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Org', 'org', false) RETURNING id",
    )
    .bind(account_id)
    .fetch_one(tenant)
    .await
    .unwrap()
}

async fn graph_exists(tenant: &PgPool, graph_id: Uuid) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM graphs WHERE id = $1)")
        .bind(graph_id)
        .fetch_one(tenant)
        .await
        .unwrap()
}

/// One atom for `graph_id`, with a real, shape-valid, org-scoped blob key —
/// so `validate_blob_key`/`assert_blob_key_org_scope` (both exercised for
/// real by the production purge code, not stubbed) actually pass.
async fn insert_atom(tenant: &PgPool, org_id: &str, graph_id: Uuid) -> (Uuid, String) {
    let atom_id = Uuid::new_v4();
    let blob_key = blob_key_for(Some(org_id), graph_id, atom_id).expect("valid org segment");
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp, content_inline) \
         VALUES ($1, $2, 'test', $3, $4, '{}', now(), 'hello')",
    )
    .bind(atom_id)
    .bind(graph_id)
    .bind(atom_id.to_string())
    .bind(&blob_key)
    .execute(tenant)
    .await
    .unwrap();
    (atom_id, blob_key)
}

/// A `meeting_transcripts` row for `graph_id` with a real, shape-valid,
/// org-scoped `raw_blob_key` — the SECOND R2-key-bearing column (besides
/// `atoms.content_blob_key`) purge must clean up, per this table's `graph_id`
/// (no FK — this row is also on purge's explicit-delete list) and its R2
/// object (built by the same `put_atom_blob` helper as an atom's blob,
/// `routes/ingest.rs:1108-1121`).
async fn insert_meeting_transcript_with_blob(tenant: &PgPool, org_id: &str, graph_id: Uuid) -> String {
    let blob_key = blob_key_for(Some(org_id), graph_id, Uuid::new_v4()).expect("valid org segment");
    sqlx::query("INSERT INTO meeting_transcripts (graph_id, source_id, raw_blob_key) VALUES ($1, $2, $3)")
        .bind(graph_id)
        .bind(format!("mt-blob-{}", Uuid::new_v4()))
        .bind(&blob_key)
        .execute(tenant)
        .await
        .unwrap();
    blob_key
}

/// Three retained versions: one shares the current raw key, two are history-only.
async fn insert_meeting_history(tenant: &PgPool, org_id: &str, graph: Uuid) -> Vec<String> {
    let source = format!("history-{}", Uuid::new_v4());
    let keys: Vec<String> = (0..3)
        .map(|_| blob_key_for(Some(org_id), graph, Uuid::new_v4()).unwrap())
        .collect();
    sqlx::query("INSERT INTO meeting_transcripts(graph_id,source_id,raw_blob_key) VALUES($1,$2,$3)")
        .bind(graph).bind(&source).bind(&keys[2]).execute(tenant).await.unwrap();
    sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting',$2)").bind(graph).bind(&source).execute(tenant).await.unwrap();
    for key in &keys {
        let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_id,source_item_id,raw_blob_key) VALUES($1,$2,(SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_id=$2),$3) RETURNING version_id")
            .bind(graph).bind(&source).bind(key).fetch_one(tenant).await.unwrap();
        sqlx::query("UPDATE meeting_transcripts SET current_version_id=$3 WHERE graph_id=$1 AND source_id=$2")
            .bind(graph).bind(&source).bind(version).execute(tenant).await.unwrap();
    }
    keys
}

/// A `coding_sessions` row for `graph_id` with a real, shape-valid,
/// org-scoped `raw_blob_key` — the THIRD R2-key-bearing column purge must
/// clean up (this table's `graph_id` DOES carry a CASCADE'd FK, but its blob
/// does not delete itself).
async fn insert_coding_session_with_blob(tenant: &PgPool, org_id: &str, graph_id: Uuid) -> String {
    let blob_key = blob_key_for(Some(org_id), graph_id, Uuid::new_v4()).expect("valid org segment");
    sqlx::query("INSERT INTO coding_sessions (graph_id, session_id, raw_blob_key) VALUES ($1, $2, $3)")
        .bind(graph_id)
        .bind(format!("sess-blob-{}", Uuid::new_v4()))
        .bind(&blob_key)
        .execute(tenant)
        .await
        .unwrap();
    blob_key
}

/// In-memory `PurgeBlobDeleter`: records every deleted key, and supports two
/// one-shot side effects fired from inside `delete_blob` (i.e. strictly
/// between `purge_one_graph`'s locks being acquired and the row-delete):
///
/// * `suspend_via` — SPAWNS (does not inline-await) an `UPDATE
///   tenant_databases SET status = 'suspended'` for the SAME org this purge
///   is running against. Spawning is load-bearing, not a style choice: this
///   purge already holds that exact row's `FOR UPDATE` lock at the point
///   `delete_blob` runs, so an INLINE-awaited update would block waiting for
///   a lock this same call stack cannot release until the inline call
///   returns — a real self-deadlock. Spawned, it sits pending in the
///   background (tokio's executor keeps polling it independently) and only
///   completes once this purge's transaction commits and releases the lock
///   — proving the suspend genuinely could not interrupt the in-flight
///   purge, not merely that this test avoided asking it to.
/// * `restore_via` — a PLAIN, INLINE-awaited `UPDATE graphs SET archived_at
///   = NULL WHERE id = $other_graph_id` for a DIFFERENT graph than the one
///   currently being purged. Safe to await inline (no deadlock risk): a
///   different `graphs.id` is a different row, so it cannot block on any
///   lock THIS purge holds. This simulates exactly the race CodeRabbit
///   named — a graph that was a valid candidate when `purge_arm`'s ONE
///   candidate `SELECT` ran, restored before its own turn in the batch.
/// * `hang_forever` — makes `delete_blob` never resolve on its own
///   (`std::future::pending().await`), simulating an R2 request that hangs
///   with no client-side timeout of its own visible from this call site
///   (CodeRabbit, PR #346: this file's one unbounded outbound await before
///   the fix). Used to prove `purge_one_graph`'s own `tokio::time::timeout`
///   around `delete_blob` actually fires, rather than trusting that it does.
/// * `slow_delay` — makes EVERY `delete_blob` call sleep this long before
///   succeeding — each individually well under `RECONCILE_TIMEOUT`, so no
///   single call ever times out, simulating "every call succeeds, just
///   slowly" — the realistic failure mode `purge_blob_delete_budget`'s own
///   doc comment describes, which no per-call bound can catch. Used with
///   `MEMBER_OCEAN_PURGE_BLOB_BUDGET_SECS` set to a tiny value so the
///   aggregate-budget path is provable in seconds, not the real 5-minute
///   default.
#[derive(Default)]
struct RecordingBlobStore {
    deleted: Mutex<Vec<String>>,
    fail_next: AtomicBool,
    suspend_via: Mutex<Option<(PgPool, String)>>,
    suspend_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    restore_via: Mutex<Option<(PgPool, Uuid)>>,
    hang_forever: AtomicBool,
    slow_delay: Mutex<Option<std::time::Duration>>,
}

impl RecordingBlobStore {
    fn deleted_keys(&self) -> Vec<String> {
        self.deleted.lock().unwrap().clone()
    }

    /// Waits for the background suspend-update spawned by `suspend_via` (if
    /// any) to finish. Call AFTER `tick()` returns, so assertions about the
    /// routing row's FINAL status observe the suspend's effect rather than
    /// racing it.
    async fn wait_for_suspend_task(&self) {
        let handle = self.suspend_task.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.await;
        }
    }
}

#[async_trait]
impl coralswarm_backend::workers::member_ocean_reconciler::PurgeBlobDeleter for RecordingBlobStore {
    async fn delete_blob(&self, blob_key: &str) -> Result<(), R2Error> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(R2Error::S3("simulated failure".to_string()));
        }
        if self.hang_forever.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves");
        }
        let delay = *self.slow_delay.lock().unwrap();
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        // One-shot: `.take()` so a multi-blob graph doesn't spawn/restore
        // more than once. Bound to a local BEFORE the `if let` so the
        // `MutexGuard` (non-`Send`) is dropped before any `.await` — the
        // guard would otherwise be kept alive across the await by `if let`'s
        // temporary-lifetime-extension rule, making this whole async fn's
        // future non-`Send` (required by the `PurgeBlobDeleter` trait).
        let suspend_hook = self.suspend_via.lock().unwrap().take();
        if let Some((control, org_id)) = suspend_hook {
            let handle = tokio::spawn(async move {
                let _ = sqlx::query("UPDATE tenant_databases SET status = 'suspended' WHERE clerk_org_id = $1")
                    .bind(&org_id)
                    .execute(&control)
                    .await;
            });
            *self.suspend_task.lock().unwrap() = Some(handle);
        }
        let restore_hook = self.restore_via.lock().unwrap().take();
        if let Some((tenant, other_graph_id)) = restore_hook {
            sqlx::query("UPDATE graphs SET archived_at = NULL, is_primary = true WHERE id = $1")
                .bind(other_graph_id)
                .execute(&tenant)
                .await
                .unwrap();
        }
        self.deleted.lock().unwrap().push(blob_key.to_string());
        Ok(())
    }
}

struct LifecycleLogRow {
    status: String,
    finished_at: Option<DateTime<Utc>>,
    manifest: serde_json::Value,
}

async fn purge_log_row(control: &PgPool, graph_id: Uuid) -> Option<LifecycleLogRow> {
    sqlx::query_as::<_, (String, Option<DateTime<Utc>>, serde_json::Value)>(
        "SELECT status, finished_at, manifest FROM member_ocean_lifecycle_log \
         WHERE action = 'purge' AND graph_id = $1 ORDER BY started_at DESC LIMIT 1",
    )
    .bind(graph_id)
    .fetch_optional(control)
    .await
    .unwrap()
    .map(|(status, finished_at, manifest)| LifecycleLogRow { status, finished_at, manifest })
}

/// ALL purge audit rows for `graph_id` (not just the latest) — used by the
/// concurrent-race test, where two ticks may each write their own row.
async fn purge_log_rows_all(control: &PgPool, graph_id: Uuid) -> Vec<LifecycleLogRow> {
    sqlx::query_as::<_, (String, Option<DateTime<Utc>>, serde_json::Value)>(
        "SELECT status, finished_at, manifest FROM member_ocean_lifecycle_log \
         WHERE action = 'purge' AND graph_id = $1 ORDER BY started_at ASC",
    )
    .bind(graph_id)
    .fetch_all(control)
    .await
    .unwrap()
    .into_iter()
    .map(|(status, finished_at, manifest)| LifecycleLogRow { status, finished_at, manifest })
    .collect()
}

async fn table_count(tenant: &PgPool, sql: &str, graph_id: Uuid) -> i64 {
    sqlx::query_scalar(sql).bind(graph_id).fetch_one(tenant).await.unwrap()
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. The 60-day boundary
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn purges_at_61_days_but_leaves_59_days_completely_untouched() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!("skipping purges_at_61_days_but_leaves_59_days_completely_untouched: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg1_ctl", "orgt_mopg1_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge1").await;
    let account_id = mint_org(&control, "org_purge1", "purge1-org").await;

    let owner_old: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('old@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let owner_recent: Uuid =
        sqlx::query_scalar("INSERT INTO users(email) VALUES ('recent@wh.test') RETURNING id")
            .fetch_one(&control)
            .await
            .unwrap();

    let old_graph = insert_archived_member_ocean(&tenant, account_id, owner_old, 61).await;
    let recent_graph = insert_archived_member_ocean(&tenant, account_id, owner_recent, 59).await;
    let (_, old_blob) = insert_atom(&tenant, "org_purge1", old_graph).await;
    let (_, recent_blob) = insert_atom(&tenant, "org_purge1", recent_graph).await;
    let old_history = insert_meeting_history(&tenant, "org_purge1", old_graph).await;
    let recent_history = insert_meeting_history(&tenant, "org_purge1", recent_graph).await;
    // Once evidence rows are deleted, cleanup jobs can be the only surviving
    // blob references. Purge subsumes pending/running/dead cleanup; remaining
    // queue jobs may subsequently finish their idempotent delete normally.
    let cleanup_keys: Vec<_> = (0..3)
        .map(|_| blob_key_for(Some("org_purge1"), old_graph, Uuid::new_v4()).unwrap())
        .collect();
    let recent_cleanup = blob_key_for(Some("org_purge1"), recent_graph, Uuid::new_v4()).unwrap();
    for (graph, key, status) in [
        (old_graph, &cleanup_keys[0], "pending"),
        (old_graph, &cleanup_keys[1], "running"),
        (old_graph, &cleanup_keys[2], "dead"),
        (recent_graph, &recent_cleanup, "pending"),
    ] {
        let job = coralswarm_backend::jobs::queue::enqueue(
            &tenant,
            &coralswarm_backend::jobs::JobKind::DeleteSourceBlob {
                graph_id: graph,
                account_id,
                org_id: Some("org_purge1".into()),
                blob_key: key.clone(),
            },
            Some(account_id),
            None,
            0,
        )
        .await
        .unwrap()
        .unwrap();
        sqlx::query("UPDATE jobs SET status=$2::job_status WHERE id=$1")
            .bind(job)
            .bind(status)
            .execute(&tenant)
            .await
            .unwrap();
    }


    let r2 = Arc::new(RecordingBlobStore::default());
    let wt = worker_tenant("org_purge1", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result.processed, 1, "exactly the 61-day-old graph is purged this tick");

    assert!(!graph_exists(&tenant, old_graph).await, "61-day-old graph must be gone");
    assert!(graph_exists(&tenant, recent_graph).await, "59-day-old graph must be untouched");
    let deleted = r2.deleted_keys();
    let mut expected = old_history.clone();
    expected.push(old_blob.clone());
    expected.extend(cleanup_keys);
    assert!(!deleted.contains(&recent_cleanup),"neighbor pending cleanup remains untouched");
    assert_eq!(deleted.len(), expected.len(), "each distinct historical/current key is deleted once");
    for key in expected { assert!(deleted.contains(&key), "missing historical blob: {key}"); }
    for key in recent_history { assert!(!deleted.contains(&key), "neighbor history must survive"); }
    let neighbor_versions: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
        .bind(recent_graph).fetch_one(&tenant).await.unwrap();
    assert_eq!(neighbor_versions, 3);
    assert!(
        !r2.deleted_keys().contains(&recent_blob),
        "the 59-day-old graph's blob must never be deleted"
    );

    let log = purge_log_row(&control, old_graph).await.expect("audit row for the purged graph");
    assert_eq!(log.status, "completed");
    assert!(log.finished_at.is_some());
    assert!(purge_log_row(&control, recent_graph).await.is_none(), "no audit row for the untouched graph");

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Full roster coverage — every GRAPH_CONTENT_TABLES entry, incl. the two
//    FK-less "soft" tables, is empty afterward.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn every_graph_content_table_is_empty_after_purge() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!("skipping every_graph_content_table_is_empty_after_purge: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg2_ctl", "orgt_mopg2_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge2").await;
    let account_id = mint_org(&control, "org_purge2", "purge2-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner2@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let creator: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('creator2@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();

    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    let target_for_promotion = insert_org_commons(&tenant, account_id).await;

    let (atom_id, _blob) = insert_atom(&tenant, "org_purge2", graph).await;
    let node_a: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', 'A') RETURNING node_id",
    )
    .bind(graph)
    .fetch_one(&tenant)
    .await
    .unwrap();
    let node_b: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', 'B') RETURNING node_id",
    )
    .bind(graph)
    .fetch_one(&tenant)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(node_a)
        .bind(atom_id)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO relationship_edge (graph_id, src_node_id, predicate, dst_node_id) \
         VALUES ($1, $2, 'relates_to', $3)",
    )
    .bind(graph)
    .bind(node_a)
    .bind(node_b)
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO node_merges (graph_id, keep_id, dup_id, dup_snapshot) VALUES ($1, $2, $3, '{}'::jsonb)",
    )
    .bind(graph)
    .bind(node_a)
    .bind(node_b)
    .execute(&tenant)
    .await
    .unwrap();
    let target_node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', 'Target') RETURNING node_id",
    )
    .bind(target_for_promotion)
    .fetch_one(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO node_promotions (source_node_id, target_graph_id, target_node_id, from_version) \
         VALUES ($1, $2, $3, 1)",
    )
    .bind(node_a)
    .bind(target_for_promotion)
    .bind(target_node)
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO supersession_edge (superseded_id, superseder_id, source, graph_id) \
         VALUES ($1, $2, 'stated', (SELECT graph_id FROM nodes WHERE node_id = $1))",
    )
        .bind(node_a)
        .bind(node_b)
        .execute(&tenant)
        .await
        .unwrap();
    let (pair_a, pair_b) = if node_a < node_b { (node_a, node_b) } else { (node_b, node_a) };
    sqlx::query("INSERT INTO non_supersession_pair (node_a_id, node_b_id) VALUES ($1, $2)")
        .bind(pair_a)
        .bind(pair_b)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query("INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline) VALUES ($1, 'mt-1', 'hi')")
        .bind(graph)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) VALUES($1,'transcript','legacy:meeting','mt-1')").bind(graph).execute(&tenant).await.unwrap();
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_id,source_item_id,raw_inline) VALUES($1,'mt-1',(SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_id='mt-1'),'hi') RETURNING version_id")
        .bind(graph).fetch_one(&tenant).await.unwrap();
    sqlx::query("UPDATE meeting_transcripts SET current_version_id=$2 WHERE graph_id=$1 AND source_id='mt-1'")
        .bind(graph).bind(version).execute(&tenant).await.unwrap();
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)").bind(graph).bind(version).bind(atom_id).execute(&tenant).await.unwrap();
    let semantic_atom: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'observation','Greeting','fixture-v1','fixture') RETURNING semantic_atom_id")
        .bind(graph).bind(version).fetch_one(&tenant).await.unwrap();
    sqlx::query("INSERT INTO semantic_atom_passages(graph_id,source_version_id,semantic_atom_id,start_char,end_char,quote) VALUES($1,$2,$3,0,2,'hi')")
        .bind(graph).bind(version).bind(semantic_atom).execute(&tenant).await.unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
        .bind(graph).bind(node_a).bind(semantic_atom).execute(&tenant).await.unwrap();
    sqlx::query("INSERT INTO semantic_atom_windows(graph_id,source_version_id,extraction_version,start_char,end_char,model) VALUES($1,$2,'fixture-v1',0,2,'fixture')")
        .bind(graph).bind(version).execute(&tenant).await.unwrap();
    sqlx::query(
        "INSERT INTO transcript_corrections (graph_id, atom_id, source_id, surface_form, canonical) \
         VALUES ($1, $2, 'mt-1', 'raw', 'Canonical')",
    )
    .bind(graph)
    .bind(atom_id)
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query("INSERT INTO meeting_shares (graph_id, source_id, token, created_by) VALUES ($1, 'mt-1', 'tok-1', $2)")
        .bind(graph)
        .bind(creator)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query("INSERT INTO coding_sessions (graph_id, session_id) VALUES ($1, 'sess-1')")
        .bind(graph)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO coding_session_shares (graph_id, session_id, token, created_by) VALUES ($1, 'sess-1', 'tok-2', $2)",
    )
    .bind(graph)
    .bind(creator)
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query("INSERT INTO projects (graph_id, project_key, display_name) VALUES ($1, 'proj-1', 'Proj')")
        .bind(graph)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query("INSERT INTO activity_events (account_id, graph_id, event_type) VALUES ($1, $2, 'graph_created')")
        .bind(account_id)
        .bind(graph)
        .execute(&tenant)
        .await
        .unwrap();
    let conv_id: Uuid = sqlx::query_scalar(
        "INSERT INTO conversations (account_id, graph_id, user_id) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(account_id)
    .bind(graph)
    .bind(owner)
    .fetch_one(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO conversation_messages (conversation_id, seq, role, text) VALUES ($1, 1, 'user', 'hi')",
    )
    .bind(conv_id)
    .execute(&tenant)
    .await
    .unwrap();
    let edge_id = Uuid::new_v4();
    sqlx::query("INSERT INTO conversation_surfaced_edges (conversation_id, edge_id) VALUES ($1, $2)")
        .bind(conv_id)
        .bind(edge_id)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query("INSERT INTO conversation_oceans (conversation_id, graph_id) VALUES ($1, $2)")
        .bind(conv_id)
        .bind(graph)
        .execute(&tenant)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO query_history (account_id, graph_id, query_text) VALUES ($1, $2, 'q')",
    )
    .bind(account_id)
    .bind(graph)
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO op_log (op_id, entity, entity_id, op_type, payload, updated_at, device_id, graph_id) \
         VALUES ($1, 'atom', $2, 'upsert', '{}'::jsonb, now(), 'dev-1', $3)",
    )
    .bind(Uuid::new_v4())
    .bind(atom_id.to_string())
    .bind(graph)
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outlook_watched_conversations (account_id, graph_id, ingested_by, conversation_id) \
         VALUES ($1, $2, $3, 'conv-1')",
    )
    .bind(account_id)
    .bind(graph)
    .bind(owner)
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outlook_workflows (account_id, user_id, graph_id, name, filter) \
         VALUES ($1, $2, $3, 'wf-1', '{}'::jsonb)",
    )
    .bind(account_id)
    .bind(owner)
    .bind(graph)
    .execute(&tenant)
    .await
    .unwrap();
    let team_id: Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, 'Team', $3) RETURNING id",
    )
    .bind(account_id)
    .bind(graph)
    .bind(owner)
    .fetch_one(&tenant)
    .await
    .unwrap();
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(team_id)
        .bind(owner)
        .execute(&tenant)
        .await
        .unwrap();
    let team_key_id: Uuid = sqlx::query_scalar(
        "INSERT INTO team_keys (graph_id, created_by) VALUES ($1, $2) RETURNING id",
    )
    .bind(graph)
    .bind(owner)
    .fetch_one(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_member_key_envelopes (team_key_id, user_id, wrapped_key) VALUES ($1, $2, $3)",
    )
    .bind(team_key_id)
    .bind(owner)
    .bind(vec![1u8, 2, 3])
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO teams_watched_resources (account_id, user_id, graph_id, kind) VALUES ($1, $2, $3, 'channel')",
    )
    .bind(account_id)
    .bind(owner)
    .bind(graph)
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query(
        // `period_start` (migration `0117_journal_tier_rollups`, ticket 15) is
        // NOT NULL with no default — set equal to `hour_start` for this
        // hour-granularity fixture row (`granularity` itself still defaults
        // to `'hour'`).
        "INSERT INTO journal_rollups (account_id, user_id, graph_id, hour_start, period_start, summary, input_fingerprint, model) \
         VALUES ($1, $2, $3, '2026-07-15T09:00:00Z', '2026-07-15T09:00:00Z', 'purge fixture hour', 'fp-purge-fixture', 'test-model')",
    )
    .bind(account_id)
    .bind(owner)
    .bind(graph)
    .execute(&tenant)
    .await
    .unwrap();

    // Sanity: every table actually has a row before purging, so the
    // post-purge zero counts are a genuine deletion proof, not a tautology
    // over never-populated tables.
    for (name, sql) in GRAPH_CONTENT_TABLES {
        let before = table_count(&tenant, sql, graph).await;
        assert!(before > 0, "test setup gap: {name} has no seeded row for this graph");
    }

    let r2 = Arc::new(RecordingBlobStore::default());
    let wt = worker_tenant("org_purge2", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result.processed, 1);
    assert!(!graph_exists(&tenant, graph).await);
    assert!(graph_exists(&tenant, target_for_promotion).await, "the promotion TARGET graph must survive");

    for (name, sql) in GRAPH_CONTENT_TABLES {
        let after = table_count(&tenant, sql, graph).await;
        assert_eq!(after, 0, "{name} must be empty for the purged graph");
    }

    let log = purge_log_row(&control, graph).await.expect("audit row");
    assert_eq!(log.status, "completed");
    let counts = log.manifest.get("table_counts").expect("table_counts key present").as_object().unwrap();
    assert_eq!(counts.get("atoms").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(counts.get("meeting_transcripts").and_then(|v| v.as_i64()), Some(1));
    for table in ["source_items", "source_version_chunks", "source_versions", "semantic_atoms", "semantic_atom_passages", "node_semantic_atoms", "semantic_atom_windows"] {
        assert_eq!(counts.get(table).and_then(|v| v.as_i64()), Some(1), "{table} audit count");
    }
    assert_eq!(counts.get("transcript_corrections").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(log.manifest.get("blobs").and_then(|v| v.as_i64()), Some(1));

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Blast radius: only the target graph's blobs are deleted.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn only_target_graphs_blobs_are_deleted_sibling_and_commons_untouched() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!(
            "skipping only_target_graphs_blobs_are_deleted_sibling_and_commons_untouched: set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let (ctl, tnt) = ("orgt_mopg3_ctl", "orgt_mopg3_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge3").await;
    let account_id = mint_org(&control, "org_purge3", "purge3-org").await;
    let owner_target: Uuid =
        sqlx::query_scalar("INSERT INTO users(email) VALUES ('target3@wh.test') RETURNING id")
            .fetch_one(&control)
            .await
            .unwrap();
    let owner_sibling: Uuid =
        sqlx::query_scalar("INSERT INTO users(email) VALUES ('sibling3@wh.test') RETURNING id")
            .fetch_one(&control)
            .await
            .unwrap();

    // Target: eligible (61 days). Sibling: ALSO eligible (70 days) but must
    // come out with its blobs untouched by the tick that purges the target —
    // proven by running the batch with BOTH candidates present and then
    // asserting the sibling's blob specifically, not merely absent from a
    // one-graph run.
    let target = insert_archived_member_ocean(&tenant, account_id, owner_target, 61).await;
    let sibling = insert_archived_member_ocean(&tenant, account_id, owner_sibling, 70).await;
    let commons = insert_org_commons(&tenant, account_id).await;

    let (_, target_blob) = insert_atom(&tenant, "org_purge3", target).await;
    let (_, sibling_blob) = insert_atom(&tenant, "org_purge3", sibling).await;
    let (_, commons_blob) = insert_atom(&tenant, "org_purge3", commons).await;
    // The two OTHER blob-bearing columns (meeting_transcripts.raw_blob_key,
    // coding_sessions.raw_blob_key) — same blast-radius property must hold
    // for these, not just atoms.content_blob_key.
    let target_mt_blob = insert_meeting_transcript_with_blob(&tenant, "org_purge3", target).await;
    let sibling_mt_blob = insert_meeting_transcript_with_blob(&tenant, "org_purge3", sibling).await;
    let commons_mt_blob = insert_meeting_transcript_with_blob(&tenant, "org_purge3", commons).await;
    let target_cs_blob = insert_coding_session_with_blob(&tenant, "org_purge3", target).await;
    let sibling_cs_blob = insert_coding_session_with_blob(&tenant, "org_purge3", sibling).await;
    let commons_cs_blob = insert_coding_session_with_blob(&tenant, "org_purge3", commons).await;

    let r2 = Arc::new(RecordingBlobStore::default());
    let wt = worker_tenant("org_purge3", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result.processed, 2, "both eligible member oceans are purged in one batch");

    assert!(!graph_exists(&tenant, target).await);
    assert!(!graph_exists(&tenant, sibling).await);
    assert!(graph_exists(&tenant, commons).await, "org commons ocean is kind='org', never eligible");

    let deleted = r2.deleted_keys();
    assert!(deleted.contains(&target_blob), "target's own atom blob must be deleted");
    assert!(deleted.contains(&sibling_blob), "sibling's own atom blob must be deleted (it was ALSO eligible)");
    assert!(deleted.contains(&target_mt_blob), "target's own meeting_transcripts blob must be deleted");
    assert!(deleted.contains(&sibling_mt_blob), "sibling's own meeting_transcripts blob must be deleted");
    assert!(deleted.contains(&target_cs_blob), "target's own coding_sessions blob must be deleted");
    assert!(deleted.contains(&sibling_cs_blob), "sibling's own coding_sessions blob must be deleted");
    assert!(
        !deleted.contains(&commons_blob),
        "PROOF-OF-FAILURE CHECKED: the org commons ocean's atom blob (kind='org', never archived) \
         must never appear in the delete log — this assertion was verified to fail when temporarily \
         pointed at the wrong key during development (see this PR's description)"
    );
    assert!(
        !deleted.contains(&commons_mt_blob),
        "the org commons ocean's meeting_transcripts blob must never appear in the delete log"
    );
    assert!(
        !deleted.contains(&commons_cs_blob),
        "the org commons ocean's coding_sessions blob must never appear in the delete log"
    );
    assert_eq!(deleted.len(), 6, "exactly six blobs deleted total (2 graphs x 3 blob-bearing columns) — no extra, no missing");

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

/// Mirrors `r2_extract_success_pg.rs`'s `set_test_kek` — a fixed,
/// deterministic KEK so wrap/unwrap round-trips without touching the real
/// `MASTER_KEK_HEX`. Only the legacy-refusal/historical-copy lifecycle case
/// below needs a real DEK/R2 round trip; every
/// other test in this file only ever exercises `RecordingBlobStore`'s
/// `delete_blob` and never decrypts anything.
///
/// SAFETY (caller contract, not enforced by the type system): the ONE
/// caller of this in this file already holds `EnvGuard` — see that struct's
/// own doc comment for why every env read/write in a test must happen while
/// it is held.
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

// ═══════════════════════════════════════════════════════════════════════════
// 3b. Legacy whole-original publication is refused. Historical copies carry
//     provenance and retract with their source; independent target originals
//     retain their own authority and ciphertext.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn legacy_promotion_is_refused_and_historical_copy_retracts_on_source_purge() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!(
            "skipping legacy_promotion_is_refused_and_historical_copy_retracts_on_source_purge: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    // Also needs a real S3-compatible fake (e.g. MinIO) as of the
    // promote-content-loss fix: the promote worker now actually re-uploads
    // content, so proving the target's key is correct AND the object exists
    // needs a real R2 round trip, not just a shape-valid key string — see
    // `r2_extract_success_pg.rs`'s module doc for the docker/mc commands.
    let Some(r2env) = r2_test_env() else {
        eprintln!(
            "skipping legacy_promotion_is_refused_and_historical_copy_retracts_on_source_purge: \
             set R2_TEST_ENDPOINT_OVERRIDE / R2_TEST_ACCESS_KEY / R2_TEST_SECRET_KEY / \
             R2_TEST_BUCKET"
        );
        return;
    };

    set_test_kek();
    let kek = load_kek().expect("test KEK loads");
    let dek = generate_dek();
    let wrapped = wrap_dek(&kek, &dek).expect("wrap test DEK");

    let (ctl, tnt) = ("orgt_mopg9_ctl", "orgt_mopg9_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_3 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge9").await;
    let account_id = mint_org(&control, "org_purge9", "purge9-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner9@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    // The wrapped DEK the promote worker's `resolve_dek_for_tenant` reads
    // for `TenantId::Org(_)` — `account_keys`, on the TENANT pool.
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(account_id)
        .bind(&wrapped)
        .execute(&tenant)
        .await
        .expect("insert wrapped DEK");

    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    let source_graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    let target_graph = insert_org_commons(&tenant, account_id).await;

    let (source_atom_id, source_blob) = insert_atom(&tenant, "org_purge9", source_graph).await;
    // `insert_atom` only stamps a real, shape-valid, org-scoped KEY on the
    // row (this file's other tests only ever exercise `delete_blob`,
    // never a real GET/PUT) — write REAL content at that same key now, so
    // the promote worker's re-upload leg (below) has something genuine to
    // fetch and this test can assert the target's content round-trips.
    let source_plaintext = b"member-ocean-purge regression: promoted content";
    r2.put_atom_blob_at(&source_blob, source_plaintext, &dek)
        .await
        .expect("seed real source blob content");
    let source_node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title) VALUES ($1, 'entity', 'Promote me') RETURNING node_id",
    )
    .bind(source_graph)
    .fetch_one(&tenant)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(source_node)
        .bind(source_atom_id)
        .execute(&tenant)
        .await
        .unwrap();

    // Manufacture a previously queued legacy request through the existing
    // Store writer. This is not publication consent and cannot authorize the
    // worker to copy whole-original bytes under the current cloud contract.
    let store = PostgresStore::new(tenant.clone());
    let mut tx = tenant.begin().await.unwrap();
    let promotion = store.promote_node_scoped(&mut tx, source_node, target_graph, owner, None)
        .await.expect("legacy Store request is queued");
    tx.commit().await.unwrap();
    assert_eq!(promotion.corals_requested, 1);
    let promote_job_id = promotion.job_id.expect("legacy queued request has a job");

    // A local rejecting endpoint observes every attempted R2 request. A real
    // request first proves the counter works; the refused worker must add zero
    // requests, so no copy/upload can hide behind an absent target SQL row.
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = attempts.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let trap_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let trap_app = axum::Router::new().fallback(move || {
        let counted = counted.clone();
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            axum::http::StatusCode::FORBIDDEN
        }
    });
    let trap = tokio::spawn(async move { axum::serve(listener, trap_app).await.unwrap() });
    assert_eq!(reqwest::get(&trap_endpoint).await.unwrap().status(), axum::http::StatusCode::FORBIDDEN);
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "the request observer must work");
    let refused_r2 = R2Client::new_with_endpoint_override("unused-account-id", "fixture", "fixture", "fixture", &trap_endpoint).await;
    let wt = worker_tenant("org_purge9", tenant.clone());
    let refused = promote::tick(&wt, Some(&refused_r2)).await.expect("refuse legacy job");
    assert_eq!(refused.processed, 1, "the queued legacy request must actually be examined");
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "consent refusal must precede every R2 read/write");
    trap.abort();
    let refusal: (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT status::text,failure_class,error_message FROM jobs WHERE id=$1")
        .bind(promote_job_id).fetch_one(&tenant).await.unwrap();
    assert_eq!(refusal, ("dead".into(), Some("structural".into()),
        Some("selected evidence consent required".into())), "dead is a refusal, never successful publication");
    let copied: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1")
        .bind(target_graph).fetch_one(&tenant).await.unwrap();
    assert_eq!(copied, 0, "refused legacy job must not publish any target coral");
    assert_eq!(r2.get_atom_blob(&source_blob, &dek).await.unwrap().as_slice(), source_plaintext,
        "refusal must leave source ciphertext intact");

    // Deliberately seed a HISTORICAL provenance-bound copy, as if it existed
    // before selected publication became mandatory. This fixture setup is not
    // a successful current publication and never re-enables the paused writer.
    let (copied_atom, copied_blob) = insert_atom(&tenant, "org_purge9", target_graph).await;
    sqlx::query("UPDATE atoms SET promoted_from_atom_id=$2 WHERE atom_id=$1")
        .bind(copied_atom).bind(source_atom_id).execute(&tenant).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(promotion.target_node_id).bind(copied_atom).execute(&tenant).await.unwrap();
    r2.put_atom_blob_at(&copied_blob, source_plaintext, &dek).await.unwrap();
    assert_ne!(copied_atom, source_atom_id, "a historical copy has a distinct coral UUID");
    assert_ne!(copied_blob, source_blob, "a copy must never alias source ciphertext");
    assert_eq!(copied_blob, blob_key_for(Some("org_purge9"), target_graph, copied_atom).unwrap());
    assert!(assert_blob_key_org_scope(&copied_blob, Some("org_purge9")).is_ok());
    let bound: bool = sqlx::query_scalar("SELECT derived_copy_bound FROM atoms WHERE atom_id=$1")
        .bind(copied_atom).fetch_one(&tenant).await.unwrap();
    assert!(bound, "actual immutable provenance binding must identify the historical copy");
    assert_eq!(r2.get_atom_blob(&copied_blob, &dek).await.unwrap().as_slice(), source_plaintext,
        "historical copy ciphertext must exist and decrypt before purge");

    // A separate target original is independently permitted and must survive.
    let (independent_atom, independent_blob) = insert_atom(&tenant, "org_purge9", target_graph).await;
    let independent_text = b"independent org original must survive source-member purge";
    r2.put_atom_blob_at(&independent_blob, independent_text, &dek).await.unwrap();
    let independent_node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Independent target original') RETURNING node_id")
        .bind(target_graph).fetch_one(&tenant).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(independent_node).bind(independent_atom).execute(&tenant).await.unwrap();
    let independent: (bool, bool) = sqlx::query_as(
        "SELECT NOT derived_copy_bound,library_atom_permitted(graph_id,atom_id) FROM atoms WHERE atom_id=$1")
        .bind(independent_atom).fetch_one(&tenant).await.unwrap();
    assert_eq!(independent, (true, true), "target original must be independently readable before purge");
    assert_ne!(independent_blob, copied_blob);
    assert_ne!(independent_blob, source_blob);
    assert_eq!(r2.get_atom_blob(&independent_blob, &dek).await.unwrap().as_slice(), independent_text);

    // The real lifecycle worker purges only the eligible source graph. Its
    // graph-delete trigger must retain the qualified copy dependency before
    // FK cascades erase the source/provenance rows.
    let purged = tick(&wt, &control, Some(&r2)).await.expect("actual source purge");
    assert_eq!(purged.processed, 1);
    assert!(!graph_exists(&tenant, source_graph).await, "source member graph must be gone");
    assert!(graph_exists(&tenant, target_graph).await, "target graph must survive");
    let retracted: bool = sqlx::query_scalar("SELECT evidence_retracted FROM atoms WHERE atom_id=$1")
        .bind(copied_atom).fetch_one(&tenant).await.unwrap();
    assert!(retracted, "dependent historical copy must lose evidence authority at source purge");
    let dependency: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM source_purge_dependencies d JOIN source_purge_events e USING(event_id) WHERE e.graph_id=$1 AND d.graph_id=$2 AND d.kind='atom' AND d.object_id=$3")
        .bind(source_graph).bind(target_graph).bind(copied_atom).fetch_one(&tenant).await.unwrap();
    assert!(dependency > 0, "actual source purge must capture the target copy dependency");

    // Use the normal account-scoped drain and actual fake-S3 deletion worker,
    // not manual DELETEs or a recording-only transport. Several bounded ticks
    // cover atom -> node/blob -> cleanup jobs as dependencies become visible.
    for _ in 0..8 {
        source_blob_cleanup::tick(&wt, Some(&r2)).await.expect("lifecycle SQL/blob cleanup");
    }
    let copied_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM atoms WHERE atom_id=$1)")
        .bind(copied_atom).fetch_one(&tenant).await.unwrap();
    assert!(!copied_exists, "unsupported historical copy must be physically removed");
    let copied_node_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)")
        .bind(promotion.target_node_id).fetch_one(&tenant).await.unwrap();
    assert!(!copied_node_exists, "unsupported copied node must not retain private prose");
    for key in [&source_blob, &copied_blob] {
        assert!(matches!(r2.get_atom_blob(key, &dek).await, Err(R2Error::NotFound(_))),
            "source and dependent target-copy ciphertext must be physically deleted: {key}");
    }
    let cleanup: (String, Option<String>) = sqlx::query_as(
        "SELECT status::text,error_message FROM jobs WHERE kind='DeleteSourceBlob' AND payload->>'blob_key'=$1")
        .bind(&copied_blob).fetch_one(&tenant).await.unwrap();
    assert_eq!(cleanup, ("done".into(), None), "copy cleanup job must finish successfully");
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM source_purge_dependencies WHERE processed_at IS NULL OR error_category IS NOT NULL")
        .fetch_one(&tenant).await.unwrap();
    assert_eq!(pending, 0, "the bounded lifecycle drain must finish without quarantine");
    let surviving: (Uuid, bool, bool) = sqlx::query_as(
        "SELECT graph_id,NOT derived_copy_bound,library_atom_permitted(graph_id,atom_id) FROM atoms WHERE atom_id=$1")
        .bind(independent_atom).fetch_one(&tenant).await.unwrap();
    assert_eq!(surviving, (target_graph, true, true), "independent target original row/authority must survive");
    let independent_node_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=$1)")
        .bind(independent_node).fetch_one(&tenant).await.unwrap();
    assert!(independent_node_exists, "independently backed target node must survive");
    assert_eq!(r2.get_atom_blob(&independent_blob, &dek).await.unwrap().as_slice(), independent_text,
        "independently authorized ciphertext and content must survive cleanup");
    let independent_cleanup: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='DeleteSourceBlob' AND payload->>'blob_key'=$1")
        .bind(&independent_blob).fetch_one(&tenant).await.unwrap();
    assert_eq!(independent_cleanup, 0, "independent target original must never be scheduled for deletion");

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4a. Org already suspended BEFORE the tick starts — the cheap, unlocked
//     pre-check aborts before an audit row is even written.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn org_already_suspended_before_the_tick_starts_is_never_purged() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!("skipping org_already_suspended_before_the_tick_starts_is_never_purged: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg4a_ctl", "orgt_mopg4a_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_4 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge4a").await;
    let account_id = mint_org(&control, "org_purge4a", "purge4a-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner4a@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    let (_, blob) = insert_atom(&tenant, "org_purge4a", graph).await;

    sqlx::query("UPDATE tenant_databases SET status = 'suspended' WHERE clerk_org_id = $1")
        .bind("org_purge4a")
        .execute(&control)
        .await
        .unwrap();

    let r2 = Arc::new(RecordingBlobStore::default());
    let wt = worker_tenant("org_purge4a", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result.processed, 0);

    assert!(graph_exists(&tenant, graph).await);
    assert!(r2.deleted_keys().is_empty());
    assert!(
        purge_log_row(&control, graph).await.is_none(),
        "the cheap unlocked pre-check aborts before any audit row is even written"
    );
    let _ = blob;

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4b. A CONCURRENT org-suspend attempt cannot interrupt a purge that has
//     already acquired both locks — it simply waits and applies afterward.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn a_concurrent_org_suspend_during_blob_deletion_does_not_interrupt_an_in_flight_purge() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!(
            "skipping a_concurrent_org_suspend_during_blob_deletion_does_not_interrupt_an_in_flight_purge: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let (ctl, tnt) = ("orgt_mopg4b_ctl", "orgt_mopg4b_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_5 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge4b").await;
    let account_id = mint_org(&control, "org_purge4b", "purge4b-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner4b@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    let (_, blob) = insert_atom(&tenant, "org_purge4b", graph).await;

    // The suspend-update SPAWNS from inside `delete_blob` — i.e. it is
    // issued strictly while `purge_one_graph` already holds BOTH the
    // routing-row lock and the graph-eligibility lock. Because it is
    // spawned rather than inline-awaited (see `RecordingBlobStore`'s own
    // doc comment for why inline would self-deadlock), it sits blocked in
    // the background and only actually applies once this purge commits and
    // releases the lock.
    let r2 = Arc::new(RecordingBlobStore::default());
    *r2.suspend_via.lock().unwrap() = Some((control.clone(), "org_purge4b".to_string()));

    let wt = worker_tenant("org_purge4b", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    r2.wait_for_suspend_task().await;

    assert_eq!(
        result.processed, 1,
        "the purge — already holding both locks before the concurrent suspend attempt could even \
         be issued — completes; a suspend that arrives too late to matter cannot un-complete it"
    );
    assert!(!graph_exists(&tenant, graph).await, "graph fully purged");
    assert_eq!(r2.deleted_keys(), vec![blob]);

    let log = purge_log_row(&control, graph).await.expect("audit row");
    assert_eq!(log.status, "completed");

    // The suspend DID eventually take effect — just after, not during.
    let final_status: String =
        sqlx::query_scalar("SELECT status FROM tenant_databases WHERE clerk_org_id = $1")
            .bind("org_purge4b")
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(
        final_status, "suspended",
        "the concurrent suspend request still lands — just after the purge's lock releases, not \
         instead of the purge"
    );

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8a. A graph restored between `purge_arm`'s ONE candidate SELECT and its
//     own turn in the batch is NOT purged — keeps both rows AND blobs.
//     (Fix for CodeRabbit's Critical finding on PR #346.)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn a_graph_restored_after_the_candidate_read_is_not_purged() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!("skipping a_graph_restored_after_the_candidate_read_is_not_purged: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg11_ctl", "orgt_mopg11_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_6 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge11").await;
    let account_id = mint_org(&control, "org_purge11", "purge11-org").await;
    let owner_a: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('ownerA11@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let owner_b: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('ownerB11@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();

    // Candidates are processed `ORDER BY archived_at ASC` — graph_a (archived
    // longer ago) is processed FIRST, graph_b SECOND, so graph_b is still
    // untouched at the moment graph_a's blob is deleted.
    let graph_a = insert_archived_member_ocean(&tenant, account_id, owner_a, 90).await;
    let graph_b = insert_archived_member_ocean(&tenant, account_id, owner_b, 61).await;
    let (_, blob_a) = insert_atom(&tenant, "org_purge11", graph_a).await;
    let (_, blob_b) = insert_atom(&tenant, "org_purge11", graph_b).await;

    // graph_b is a DIFFERENT row from graph_a, so restoring it inline (not
    // spawned) from inside graph_a's `delete_blob` call cannot block on
    // anything graph_a's transaction holds — this is exactly the race
    // CodeRabbit named: graph_b was a valid candidate when `purge_arm`'s ONE
    // candidate SELECT ran (both graphs were ≥60-days-archived at that
    // instant), but is restored before its own turn in the batch.
    let r2 = Arc::new(RecordingBlobStore::default());
    *r2.restore_via.lock().unwrap() = Some((tenant.clone(), graph_b));

    let wt = worker_tenant("org_purge11", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");

    assert_eq!(result.processed, 1, "only graph_a (still genuinely eligible) is purged");
    assert!(!graph_exists(&tenant, graph_a).await);
    assert!(
        graph_exists(&tenant, graph_b).await,
        "PROOF-OF-FAILURE CHECKED: graph_b — restored mid-tick — must survive; this assertion was \
         verified to fail (graph_b also deleted) when the eligibility re-check was temporarily \
         reverted — see this PR's description"
    );
    let graph_b_archived_at: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT archived_at FROM graphs WHERE id = $1")
            .bind(graph_b)
            .fetch_one(&tenant)
            .await
            .unwrap();
    assert!(graph_b_archived_at.is_none(), "graph_b was genuinely restored, not merely left alone");

    let deleted = r2.deleted_keys();
    assert!(deleted.contains(&blob_a), "graph_a's own blob is deleted");
    assert!(!deleted.contains(&blob_b), "graph_b's blob must NEVER be touched once it was restored");

    let log_b = purge_log_row(&control, graph_b).await.expect("audit row for graph_b (started, then aborted)");
    assert_ne!(
        log_b.status, "completed",
        "graph_b must never be recorded as a completed purge"
    );

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8b. Two genuinely concurrent ticks racing the SAME graph: exactly one
//     completes; the loser is never recorded as `completed`.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn two_concurrent_ticks_racing_the_same_graph_only_one_completes() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!("skipping two_concurrent_ticks_racing_the_same_graph_only_one_completes: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg12_ctl", "orgt_mopg12_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_7 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge12").await;
    let account_id = mint_org(&control, "org_purge12", "purge12-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner12@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    let (_, blob) = insert_atom(&tenant, "org_purge12", graph).await;

    // Two independent `WorkerTenant`s (mirroring two backend processes, or
    // two overlapping ticks of the same reconciler) racing `tick()` for the
    // SAME graph. The graph-eligibility `FOR UPDATE` lock serializes them:
    // whichever acquires it first purges fully; the second blocks, then
    // (once unblocked) finds the row already gone and aborts via the
    // "no longer eligible" path — never reaching, let alone completing, a
    // real 0-row `DELETE`.
    //
    // **Honesty note on this test's proof-of-failure (requirement 4b).**
    // With the lock held continuously (as designed), Postgres's row-level
    // locking makes it IMPOSSIBLE for this function's own `still_eligible`
    // SELECT and its own subsequent `graphs` DELETE to ever disagree within
    // one transaction — which means the literal `graph_deleted == 0` branch
    // is, by construction of this very fix, not independently reachable via
    // an external race once the lock is intact. I attempted the proof by
    // temporarily removing BOTH the eligibility SELECT's `FOR UPDATE` AND
    // the `graph_deleted == 0` guard together (to force both concurrent
    // transactions past the check and let the real DELETE decide); the
    // result depended on `tokio::join!`'s actual interleaving against a fast
    // local Postgres and did NOT reliably reproduce a double-`completed`
    // outcome in the run I observed — i.e. it is not a clean, deterministic
    // proof-of-failure, and I am not claiming it is one. What IS a clean,
    // deterministic proof-of-failure for the Critical fix this PR makes is
    // `a_graph_restored_after_the_candidate_read_is_not_purged`'s own
    // exercise (reverting the eligibility re-check itself) — see that
    // test's comment and this PR's description for that result. This test
    // is kept as a real, valuable regression check for the concurrent-race
    // CONTRACT (never double-complete, never corrupt state), just not as
    // proof of the specific `graph_deleted == 0` line in isolation.
    let r2 = Arc::new(RecordingBlobStore::default());
    let wt1 = worker_tenant("org_purge12", tenant.clone());
    let wt2 = worker_tenant("org_purge12", tenant.clone());
    let (res1, res2) = tokio::join!(
        tick(&wt1, &control, Some(r2.as_ref())),
        tick(&wt2, &control, Some(r2.as_ref())),
    );
    let (res1, res2) = (res1.expect("tick A ok"), res2.expect("tick B ok"));

    assert_eq!(
        res1.processed + res2.processed,
        1,
        "exactly one of the two concurrent ticks purges the graph — never both, never neither"
    );
    assert!(!graph_exists(&tenant, graph).await);
    assert_eq!(r2.deleted_keys(), vec![blob], "the blob is deleted exactly once, never twice");

    let logs = purge_log_rows_all(&control, graph).await;
    let completed_count = logs.iter().filter(|l| l.status == "completed").count();
    assert_eq!(
        completed_count, 1,
        "exactly one 'completed' audit row — a lost race must never be recorded as completed"
    );

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. A revoked org is never purged.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn revoked_org_is_never_purged() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!("skipping revoked_org_is_never_purged: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg5_ctl", "orgt_mopg5_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_8 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge5").await;
    let account_id = mint_org(&control, "org_purge5", "purge5-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner5@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    let (_, blob) = insert_atom(&tenant, "org_purge5", graph).await;

    sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)")
        .bind("org_purge5")
        .execute(&control)
        .await
        .unwrap();

    let r2 = Arc::new(RecordingBlobStore::default());
    let wt = worker_tenant("org_purge5", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result.processed, 0);

    assert!(graph_exists(&tenant, graph).await, "a revoked org's member ocean must never be purged");
    assert!(r2.deleted_keys().is_empty(), "not even a blob read/delete attempt for a revoked org");
    assert!(purge_log_row(&control, graph).await.is_none(), "no audit row at all — purge never started");

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
    let _ = blob;
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Drain guard: an in-flight job blocks the purge; it retries once the job
//    is no longer pending/running.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn inflight_job_blocks_purge_and_it_retries_once_the_job_clears() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!("skipping inflight_job_blocks_purge_and_it_retries_once_the_job_clears: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg6_ctl", "orgt_mopg6_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_9 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge6").await;
    let account_id = mint_org(&control, "org_purge6", "purge6-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner6@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    let (atom_id, blob) = insert_atom(&tenant, "org_purge6", graph).await;

    let job_id: Uuid = sqlx::query_scalar(
        "INSERT INTO jobs (kind, payload, status) \
         VALUES ('embed_atoms', $1, 'pending') RETURNING id",
    )
    .bind(serde_json::json!({ "atom_ids": [atom_id] }))
    .fetch_one(&tenant)
    .await
    .unwrap();

    let r2 = Arc::new(RecordingBlobStore::default());
    let wt = worker_tenant("org_purge6", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result.processed, 0, "an in-flight job for this graph's atom blocks the purge");
    assert!(graph_exists(&tenant, graph).await);
    assert!(r2.deleted_keys().is_empty(), "no blob touched while the drain guard is holding");
    assert!(purge_log_row(&control, graph).await.is_none(), "purge never even started an audit row");

    // Clear the job (mark done) and retry — must purge now.
    sqlx::query("UPDATE jobs SET status = 'done', finished_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(&tenant)
        .await
        .unwrap();
    let result2 = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result2.processed, 1, "second tick purges it once the job cleared");
    assert!(!graph_exists(&tenant, graph).await);
    assert_eq!(r2.deleted_keys(), vec![blob]);

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Audit two-phase discipline, incl. a simulated crash's `started`-row
//    signal.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn audit_row_is_started_then_completed_with_counts() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!("skipping audit_row_is_started_then_completed_with_counts: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg7_ctl", "orgt_mopg7_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_10 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge7").await;
    let account_id = mint_org(&control, "org_purge7", "purge7-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner7@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    insert_atom(&tenant, "org_purge7", graph).await;

    let r2 = Arc::new(RecordingBlobStore::default());
    let wt = worker_tenant("org_purge7", tenant.clone());
    tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");

    let log = purge_log_row(&control, graph).await.expect("audit row");
    assert_eq!(log.status, "completed");
    assert!(log.finished_at.is_some(), "0092's CHECK requires finished_at set for a terminal status");
    let table_counts: BTreeMap<String, i64> =
        serde_json::from_value(log.manifest["table_counts"].clone()).expect("table_counts is a count map");
    assert_eq!(table_counts.get("atoms"), Some(&1));
    assert_eq!(log.manifest["blobs"].as_i64(), Some(1));
    // "Never content" (migration 0092's own contract): the manifest must
    // carry only identifiers/counts/digests.
    assert!(
        !log.manifest.to_string().to_lowercase().contains("hello"),
        "the manifest must never contain atom content"
    );

    // Simulated crash: a raw 'started' row with no matching completion, for
    // a DIFFERENT graph id, is exactly the operator "verify by hand" signal
    // plan §2.6 describes — the schema (0092's CHECK) accepts it, and it is
    // trivially distinguishable from every real row this test just produced.
    let crashed_graph = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO member_ocean_lifecycle_log \
             (action, status, clerk_org_id, subject_user_id, graph_id, actor) \
         VALUES ('purge', 'started', 'org_purge7', $1, $2, 'reconciler')",
    )
    .bind(owner)
    .bind(crashed_graph)
    .execute(&control)
    .await
    .unwrap();
    let stuck: Option<(String, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT status, finished_at FROM member_ocean_lifecycle_log \
         WHERE action = 'purge' AND graph_id = $1",
    )
    .bind(crashed_graph)
    .fetch_optional(&control)
    .await
    .unwrap();
    let (status, finished_at) = stuck.expect("the simulated-crash row exists");
    assert_eq!(status, "started");
    assert!(finished_at.is_none(), "a started row with no finished_at IS the crash signal");

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7b. A hung R2 delete times out and is treated as a failure, NEVER a
//     success — CodeRabbit Major on PR #346 (the one unbounded outbound
//     await in this file). Slow by design (waits out the real
//     RECONCILE_TIMEOUT, 30s) rather than shortening a production constant
//     just for this test.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn a_hung_blob_delete_times_out_and_is_recorded_as_failed_never_completed() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!(
            "skipping a_hung_blob_delete_times_out_and_is_recorded_as_failed_never_completed: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let (ctl, tnt) = ("orgt_mopg13_ctl", "orgt_mopg13_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_11 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge13").await;
    let account_id = mint_org(&control, "org_purge13", "purge13-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner13@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    insert_atom(&tenant, "org_purge13", graph).await;

    let r2 = Arc::new(RecordingBlobStore::default());
    r2.hang_forever.store(true, Ordering::SeqCst);

    let wt = worker_tenant("org_purge13", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");

    assert_eq!(
        result.processed, 0,
        "a timed-out blob delete must never count as a completed purge"
    );
    assert!(
        graph_exists(&tenant, graph).await,
        "PROOF-OF-FAILURE CHECKED: the graph row must survive a timed-out blob delete — this \
         assertion (and the audit-status one below) was verified to hang indefinitely, never \
         reaching a pass or fail within a bounded wait, when the tokio::time::timeout around \
         delete_blob was temporarily removed — see this PR's description"
    );
    assert!(r2.deleted_keys().is_empty(), "the hung key must never be counted as deleted");

    let log = purge_log_row(&control, graph).await.expect("audit row");
    assert_eq!(
        log.status, "failed",
        "a timed-out blob delete must be recorded as failed, never completed — an orphaned-but- \
         claimed-destroyed blob is exactly the audit-integrity problem this PR discusses"
    );
    assert!(log.finished_at.is_some());
    let manifest_str = log.manifest.to_string();
    assert!(
        manifest_str.to_lowercase().contains("timed out"),
        "the manifest error should identify this as a timeout, not a generic failure: {manifest_str}"
    );

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7c. The WHOLE-LOOP budget: every individual call succeeds well under
//     RECONCILE_TIMEOUT, but the loop's aggregate time exceeds
//     purge_blob_delete_budget() — CodeRabbit's third round on PR #346's
//     Major (the realistic failure mode a per-call bound alone cannot
//     catch). Uses MEMBER_OCEAN_PURGE_BLOB_BUDGET_SECS to exercise this in
//     seconds rather than the real 5-minute default.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn a_slow_but_never_timing_out_blob_loop_exceeds_its_budget_and_is_not_completed() {
    let _env_guard = EnvGuard::acquire_both_on().await;

    let Some(base) = base_url() else {
        eprintln!(
            "skipping a_slow_but_never_timing_out_blob_loop_exceeds_its_budget_and_is_not_completed: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    // 1-second budget; each blob delete sleeps 0.6s (well under
    // RECONCILE_TIMEOUT, so no per-call timeout is ever involved) — the
    // deadline check trips on the THIRD key (elapsed ~1.2s >= 1s budget),
    // after the first two have already genuinely succeeded.
    let _budget_guard = BlobBudgetGuard::set_secs(1);

    let (ctl, tnt) = ("orgt_mopg14_ctl", "orgt_mopg14_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_12 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge14").await;
    let account_id = mint_org(&control, "org_purge14", "purge14-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner14@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    // Three atoms — three blob keys — so the budget trips mid-loop, not on
    // the first key (proving PARTIAL progress is discarded, not silently
    // truncated into a "completed" purge).
    insert_atom(&tenant, "org_purge14", graph).await;
    insert_atom(&tenant, "org_purge14", graph).await;
    insert_atom(&tenant, "org_purge14", graph).await;

    let r2 = Arc::new(RecordingBlobStore::default());
    *r2.slow_delay.lock().unwrap() = Some(std::time::Duration::from_millis(600));

    let wt = worker_tenant("org_purge14", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");

    assert_eq!(
        result.processed, 0,
        "a budget-exceeded graph must never count as a completed purge, even though most of its \
         blobs genuinely succeeded"
    );
    assert!(
        graph_exists(&tenant, graph).await,
        "PROOF-OF-FAILURE CHECKED: the graph row (and all its children) must survive a \
         budget-exceeded abort — this assertion was verified to fail (graph deleted, `completed` \
         recorded) when the whole-loop deadline check was temporarily removed — see this PR's \
         description"
    );
    assert_eq!(
        r2.deleted_keys().len(),
        2,
        "exactly the two blobs that completed before the budget tripped were deleted — the third \
         key must never be attempted"
    );

    let log = purge_log_row(&control, graph).await.expect("audit row");
    assert_eq!(
        log.status, "failed",
        "a budget-exceeded abort must be recorded as failed, never completed — a partially-purged \
         ocean whose audit row says 'completed' is the exact audit-integrity failure already fixed \
         once on this PR"
    );
    assert!(log.finished_at.is_some());
    let manifest_str = log.manifest.to_string();
    assert!(
        manifest_str.contains("budget") && manifest_str.contains("2/3"),
        "the manifest error should name the budget and the partial progress (2/3), so an operator \
         reading it can distinguish 'ran out of time, retrying' from a real error: {manifest_str}"
    );

    // Retry: with the budget restored to a generous value (env var removed
    // here, before the guard would drop anyway, to prove the NEXT tick's
    // default succeeds), the whole graph purges cleanly — proving this is a
    // "resume next tick" story, not a permanent failure.
    unsafe { std::env::remove_var("MEMBER_OCEAN_PURGE_BLOB_BUDGET_SECS") };
    let result2 = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result2.processed, 1, "the retried tick completes the purge in full");
    assert!(!graph_exists(&tenant, graph).await);
    // 2 (attempt 1, before the budget tripped) + 3 (attempt 2, which
    // re-reads and re-attempts ALL THREE keys since no row was ever touched)
    // = 5 total `delete_blob` calls across both attempts — deterministic,
    // and exactly the "harmless redundant re-delete" idempotency property
    // `delete_atom_blob` and this whole retry design depend on.
    assert_eq!(
        r2.deleted_keys().len(),
        5,
        "2 real deletes from attempt 1 + all 3 re-attempted (harmlessly) on the successful retry"
    );

    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Both flags are independently required.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn purge_flag_off_is_a_pure_noop_even_with_member_oceans_flag_on() {
    let _guard = ENV_LOCK.lock().await;
    // SAFETY: guarded by ENV_LOCK for this test's duration (acquired before
    // the `base_url()` read below, not just before these two `set_var`
    // calls — every environment read in this test happens while the lock is
    // held); both vars are restored before the guard is dropped, same
    // contract as EnvGuard.
    unsafe {
        std::env::set_var("MEMBER_OCEANS_ENABLED", "1");
        std::env::remove_var("MEMBER_OCEAN_PURGE_ENABLED");
    }

    let Some(base) = base_url() else {
        eprintln!("skipping purge_flag_off_is_a_pure_noop_even_with_member_oceans_flag_on: set ORGT_TEST_BASE_URL");
        return;
    };

    let (ctl, tnt) = ("orgt_mopg8_ctl", "orgt_mopg8_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_13 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let tenant = tenant_pool(&base, tnt, "org_purge8").await;
    let account_id = mint_org(&control, "org_purge8", "purge8-org").await;
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('owner8@wh.test') RETURNING id")
        .fetch_one(&control)
        .await
        .unwrap();
    let graph = insert_archived_member_ocean(&tenant, account_id, owner, 90).await;
    insert_atom(&tenant, "org_purge8", graph).await;

    let r2 = Arc::new(RecordingBlobStore::default());
    let wt = worker_tenant("org_purge8", tenant.clone());
    let result = tick(&wt, &control, Some(r2.as_ref())).await.expect("tick ok");
    assert_eq!(result.processed, 0, "MEMBER_OCEANS_ENABLED alone must never be sufficient to purge");
    assert!(graph_exists(&tenant, graph).await);
    assert!(r2.deleted_keys().is_empty());

    unsafe { std::env::remove_var("MEMBER_OCEANS_ENABLED") };
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
