//! Integration tests for the job queue's `account_id` plumbing (org-tenancy
//! Phase W.1, migration `migrations_control/0085_jobs_account_id.sql` and its
//! paired `migrations_tenant/0004_jobs_account_id.sql`).
//!
//! Post-OPS-4, every worker must open its per-job
//! `tenancy::scope::begin_account_scoped` transaction before touching an
//! RLS-covered table — and the scoping `account_id` has to come from the job
//! row itself (a per-job bootstrap round-trip would be wasted work when every
//! enqueue site already knows it). This file proves the three load-bearing
//! properties end to end against a real migrated Postgres:
//!
//!   1. `enqueue` → `dequeue` round-trips the account_id;
//!   2. a pre-W.1 legacy row (NULL account_id) whose payload carries a
//!      resolvable graph/atom reference is transparently backfilled at
//!      dequeue via the 0084 SECURITY DEFINER resolvers — and the backfill is
//!      persisted so retries don't re-resolve;
//!   3. a legacy row whose reference is UNRESOLVABLE is routed through the
//!      queue's `fail` path with the greppable
//!      "job missing account_id (pre-W.1 row)" error and never handed to a
//!      worker — while the deliberately account-less unscoped sweeps still
//!      pass through untouched.
//!
//! Gated on `ORGT_TEST_BASE_URL`; same scratch-DB pattern as
//! `bootstrap_scope_pg.rs` / `consumer_rls_pg.rs`.
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test jobs_account_scope_pg
//!
//! All scenarios live in ONE test function on ONE scratch DB: separate
//! `#[tokio::test]`s would race each other recreating the database (and
//! `dequeue` sweeps every pending row of a kind, so parallel scenarios could
//! claim each other's fixtures — same reasoning as `jobs::queue`'s in-module
//! reaper test).

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
use uuid::Uuid;

use coralswarm_backend::jobs::queue::{self, MISSING_ACCOUNT_ID_ERR};
use coralswarm_backend::jobs::JobKind;

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// Seed one (user, account, graph, two atoms) fixture via the owner pool.
async fn seed(owner: &PgPool) -> (Uuid, Uuid, Vec<Uuid>) {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("jobs-w1-{}@x.co", Uuid::new_v4()))
        .fetch_one(owner)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .expect("insert account");
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary) \
         VALUES ($1, 'home', 'personal', true) RETURNING id",
    )
    .bind(account_id)
    .fetch_one(owner)
    .await
    .expect("insert graph");
    let mut atom_ids = Vec::new();
    for i in 0..2 {
        let atom_id: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'test', $2, 'blob-key', now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("w1-probe-{i}"))
        .fetch_one(owner)
        .await
        .expect("insert atom");
        atom_ids.push(atom_id);
    }
    (account_id, graph_id, atom_ids)
}

/// Insert a job row the way a PRE-W.1 binary would have: no `account_id`
/// column in the INSERT at all.
async fn insert_legacy_job(owner: &PgPool, kind: &str, payload: serde_json::Value) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO jobs (kind, payload) VALUES ($1, $2) RETURNING id",
    )
    .bind(kind)
    .bind(payload)
    .fetch_one(owner)
    .await
    .expect("insert legacy job row")
}

async fn job_row(owner: &PgPool, id: Uuid) -> (Option<Uuid>, String, i32, Option<String>) {
    let r = sqlx::query(
        "SELECT account_id, status::text AS status, attempts, error_message FROM jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(owner)
    .await
    .expect("fetch job row");
    (
        r.get("account_id"),
        r.get::<String, _>("status"),
        r.get("attempts"),
        r.get("error_message"),
    )
}

#[tokio::test]
async fn account_id_roundtrips_and_legacy_rows_backfill_or_fail_loud() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping account_id_roundtrips_and_legacy_rows_backfill_or_fail_loud: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_jobs_account_scope";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = PgConnection::connect(&base).await.expect("connect admin");
    admin
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    admin
        .execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0084 (resolvers) + 0085 (jobs.account_id)");

    let (account_id, graph_id, atom_ids) = seed(&owner).await;

    // ── 1. enqueue → dequeue round-trips account_id ────────────────────────────
    let j1 = queue::enqueue(
        &owner,
        &JobKind::EmbedAtoms { atom_ids: atom_ids.clone() },
        Some(account_id),
        None,
        0,
    )
    .await
    .expect("enqueue")
    .expect("job created");

    let claimed = queue::dequeue(&owner, &["EmbedAtoms"], 10).await.expect("dequeue");
    let job = claimed
        .iter()
        .find(|j| j.id == j1)
        .expect("the enqueued job must be claimable");
    assert_eq!(
        job.account_id,
        Some(account_id),
        "enqueue → dequeue must round-trip the account_id"
    );

    // ── 2a. legacy NULL row, graph reference → backfilled + persisted ─────────
    // `ComputeDrift` is a graph-keyed kind still admitted under managed corpus
    // admission (#1234); `EntityResolution`, which this scenario used before,
    // is terminal at enqueue and would never reach `dequeue`.
    let legacy_graph = insert_legacy_job(
        &owner,
        "ComputeDrift",
        serde_json::json!({ "graph_id": graph_id }),
    )
    .await;

    let claimed = queue::dequeue(&owner, &["ComputeDrift"], 10).await.expect("dequeue");
    let job = claimed
        .iter()
        .find(|j| j.id == legacy_graph)
        .expect("resolvable legacy job must still be handed to the worker");
    assert_eq!(
        job.account_id,
        Some(account_id),
        "legacy row's graph_id must backfill to the owning account via resolve_graph_account_id"
    );
    let (persisted, status, _, _) = job_row(&owner, legacy_graph).await;
    assert_eq!(status, "running", "backfilled job is claimed normally");
    assert_eq!(
        persisted,
        Some(account_id),
        "the backfilled account_id must be persisted on the jobs row (no re-resolve on retry)"
    );

    // ── 2b. legacy NULL row, atom-batch reference → backfilled ────────────────
    let legacy_atoms = insert_legacy_job(
        &owner,
        "EmbedAtoms",
        serde_json::json!({ "atom_ids": atom_ids }),
    )
    .await;

    let claimed = queue::dequeue(&owner, &["EmbedAtoms"], 10).await.expect("dequeue");
    let job = claimed
        .iter()
        .find(|j| j.id == legacy_atoms)
        .expect("resolvable legacy atom-batch job must still be handed to the worker");
    assert_eq!(
        job.account_id,
        Some(account_id),
        "legacy row's atom_ids must backfill to the owning account via resolve_atoms_account_id"
    );

    // ── 3a. legacy NULL row, UNRESOLVABLE reference → fail-pathed, not returned ─
    let orphan_graph = Uuid::new_v4(); // no graphs row — resolver returns NULL
    let unresolvable = insert_legacy_job(
        &owner,
        "ComputeDrift",
        serde_json::json!({ "graph_id": orphan_graph }),
    )
    .await;

    let claimed = queue::dequeue(&owner, &["ComputeDrift"], 10).await.expect("dequeue");
    assert!(
        claimed.iter().all(|j| j.id != unresolvable),
        "an unresolvable pre-W.1 job must NEVER be handed to a worker"
    );
    let (acct, status, attempts, err) = job_row(&owner, unresolvable).await;
    assert_eq!(acct, None, "no account was resolvable");
    assert_eq!(attempts, 1, "the fail path must burn an attempt (bounded retries, never forever)");
    assert_eq!(
        status, "pending",
        "first failure requeues with backoff (max_attempts=3 converges it to 'dead')"
    );
    let err = err.expect("fail path must record an error_message");
    assert!(
        err.contains(MISSING_ACCOUNT_ID_ERR),
        "error must carry the greppable marker {MISSING_ACCOUNT_ID_ERR:?} — got: {err}"
    );

    // ── 3b. single-account kind with NO bootstrap resolver → fail-pathed ──────
    // `RouteCopresence`/`SendOutreach` are dormant (no enqueue site, no
    // worker), but a legacy NULL row of a single-account kind must
    // dead-letter loudly rather than pass through unscoped (CodeRabbit
    // review, PR #287).
    let no_resolver = insert_legacy_job(
        &owner,
        "SendOutreach",
        serde_json::json!({ "user_id": Uuid::new_v4(), "template": "welcome" }),
    )
    .await;

    let claimed = queue::dequeue(&owner, &["SendOutreach"], 10).await.expect("dequeue");
    assert!(
        claimed.iter().all(|j| j.id != no_resolver),
        "a resolver-less single-account legacy job must NEVER be handed to a worker"
    );
    let (_, _, attempts, err) = job_row(&owner, no_resolver).await;
    assert_eq!(attempts, 1, "fail path burns an attempt");
    assert!(
        err.expect("error recorded").contains(MISSING_ACCOUNT_ID_ERR),
        "resolver-less kind must carry the greppable marker"
    );

    // ── 3c. deliberately account-less unscoped sweep → passes through as None ──
    let sweep = insert_legacy_job(
        &owner,
        "ContentTsvRegconfigRepair",
        serde_json::json!({ "after_atom_id": null }),
    )
    .await;

    let claimed = queue::dequeue(&owner, &["ContentTsvRegconfigRepair"], 10)
        .await
        .expect("dequeue");
    let job = claimed
        .iter()
        .find(|j| j.id == sweep)
        .expect("an unscoped sweep must be claimable with NULL account_id");
    assert_eq!(
        job.account_id, None,
        "unscoped sweeps are account-less by design — no backfill, no fail path"
    );

    eprintln!(
        "PG jobs account-scope e2e passed: round-trip, graph/atom legacy backfill (+persist), \
         fail-loud unresolvable, unscoped-sweep passthrough"
    );
}
