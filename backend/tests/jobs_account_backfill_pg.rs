//! Live test for the one-shot `backfill_unattributed_job_accounts`
//! (attributes historical `ExtractEntities` `jobs` rows so
//! `workers::billing_derivation`'s unattributed-jobs alert stops firing on
//! residue).
//!
//! Drives the REAL core (`coralswarm_backend::jobs_account_backfill::
//! backfill_database`) against real Postgres, plus the REAL compiled binary
//! (via `CARGO_BIN_EXE_…`) end to end — never a re-implementation of either.
//! Modeled on `judgement_backfill_pg.rs`: gated on
//! `RELINFER_TEST_DATABASE_URL`, mints fresh `Uuid::new_v4()` fixtures per
//! test, and a run with no DSN set SKIPS with an `eprintln!` marker rather
//! than failing.
//!
//! **Unlike most `_pg.rs` suites in this tree, every test here cleans up
//! after itself** (`with_cleanup`, below) rather than relying on "fresh
//! UUIDs never collide" — that convention holds for tables everything else
//! reads scoped by `account_id`/`graph_id`, but `jobs_account_backfill`'s
//! whole subject is a GLOBALLY unscoped `jobs` count (no `account_id`
//! filter, matching the alert it fixes), and `jobs.account_id` carries NO
//! foreign key to `accounts` (migration `0085`'s own header) — so deleting
//! `accounts` does not cascade away a leftover `jobs` row the way it
//! cascades away `graphs`/`atoms`. A single leftover unresolved job from one
//! test run is directly visible to, and inflates, every later run's (or
//! later SUITE's — this was caught by `workers::billing_derivation`'s own
//! alert tests) unscoped count. Confirmed live: before this cleanup existed,
//! `resolvable_job_is_attributed_and_unresolvable_job_is_left_alone`'s
//! deliberately-unresolvable fixture (by design, permanently
//! `account_id IS NULL`) leaked into a LATER run of `workers::
//! billing_derivation::pg_tests::unattributed_alert_fires_for_a_recent_
//! unattributed_job` and inflated its `count=1` expectation to `count=2`.
//!
//! Covers:
//!   * a resolvable job (payload's `atom_id` -> a live `atoms` row -> a
//!     `graphs.account_id`) is attributed, WHILE an unresolvable one
//!     (`atom_id` pointing at nothing — the "atom was since deleted" shape)
//!     is left completely alone, in the SAME run;
//!   * a re-run is a no-op for the row the first run already resolved — the
//!     `account_id IS NULL` predicate is this backfill's entire idempotency
//!     mechanism (see the core module's doc);
//!   * the compiled binary: `--dry-run` (the default) writes nothing but
//!     predicts the `--execute` run's tally exactly; `--execute` actually
//!     writes.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://…/cs_phase4b_test \
//!     cargo test -p coralswarm-backend --test jobs_account_backfill_pg -- --test-threads=1

#[cfg(not(feature = "binaries"))]
compile_error!("This suite runs utilities. Enable defaults or --features binaries.");

use coralswarm_backend::jobs_account_backfill::{backfill_database, BackfillOpts};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

async fn pool_or_skip(test_name: &str) -> Option<PgPool> {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run the live \
                 jobs_account_backfill suite"
            );
            return None;
        }
    };
    Some(
        PgPoolOptions::new()
            .max_connections(8)
            .connect(&url)
            .await
            .expect("connect to test Postgres"),
    )
}

fn dry() -> BackfillOpts {
    BackfillOpts { execute: false }
}
fn exec() -> BackfillOpts {
    BackfillOpts { execute: true }
}

/// A real `users` + `accounts` + `graphs` row trio, the minimum
/// `resolve_atoms_account_id` needs to resolve anything.
async fn seed_graph(pool: &PgPool, label: &str) -> (Uuid, Uuid, Uuid) {
    let user_id = Uuid::new_v4();
    let account_id = Uuid::new_v4();
    let graph_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(user_id)
        .bind(format!("{label}-{user_id}@jobs-backfill-test.local"))
        .execute(pool)
        .await
        .expect("insert user");
    sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("insert account");
    sqlx::query("INSERT INTO graphs (id, account_id, name) VALUES ($1, $2, $3)")
        .bind(graph_id)
        .bind(account_id)
        .bind(format!("{label} ocean"))
        .execute(pool)
        .await
        .expect("insert graph");
    (user_id, account_id, graph_id)
}

/// A real, live `atoms` row — `resolve_atoms_account_id` only resolves atoms
/// that actually exist.
async fn seed_atom(pool: &PgPool, graph_id: Uuid, label: &str) -> Uuid {
    let atom_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, $3, '{}', now())",
    )
    .bind(atom_id)
    .bind(graph_id)
    .bind(format!("jobs-backfill-{label}-{atom_id}"))
    .execute(pool)
    .await
    .expect("insert atom");
    atom_id
}

/// A `status='done', kind='ExtractEntities', account_id = NULL` job whose
/// payload carries the ONE key that kind's payload ever carries — exactly
/// the shape `jobs_account_backfill::backfill_database` scans for. `atom_id`
/// need not reference a live atom (that is precisely how the dead-atom
/// fixture is built): the backfill's job is to find out either way.
async fn insert_unattributed_extract_job(pool: &PgPool, id: Uuid, atom_id: Uuid) {
    sqlx::query(
        "INSERT INTO jobs (id, kind, payload, status, idempotency_key, account_id, finished_at, started_at) \
         VALUES ($1, 'ExtractEntities', jsonb_build_object('atom_id', $2::text), 'done', $3, NULL, \
                 now() - interval '10 minutes', now() - interval '10 minutes')",
    )
    .bind(id)
    .bind(atom_id)
    .bind(format!("extract:{}", Uuid::new_v4()))
    .execute(pool)
    .await
    .expect("insert unattributed ExtractEntities job");
}

async fn job_account_id(pool: &PgPool, job_id: Uuid) -> Option<Uuid> {
    sqlx::query_scalar("SELECT account_id FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .expect("read back job.account_id")
}

/// Runs `body` (spawned as its own task), then UNCONDITIONALLY cleans up —
/// every `job_ids` row explicitly (see this file's module doc: `jobs` has no
/// FK to `accounts`, so nothing cascades it away), then `user_id`/
/// `account_id` (which DOES cascade away any `graphs`/`atoms` this test
/// seeded, via their `ON DELETE CASCADE` to `accounts`) — then re-panics
/// with `body`'s original payload. Same panic-safe teardown discipline as
/// `judgement_backfill_pg.rs`'s `with_cleanup`.
async fn with_cleanup<Fut>(pool: PgPool, job_ids: Vec<Uuid>, user_id: Uuid, account_id: Uuid, body: Fut)
where
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let result = tokio::spawn(body).await;
    sqlx::query("DELETE FROM jobs WHERE id = ANY($1)")
        .bind(&job_ids)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
    if let Err(e) = result {
        std::panic::resume_unwind(e.into_panic());
    }
}

// ── 1. Resolvable job attributed, unresolvable job left alone (same run) ───

/// **Observed negative control** (dropping the `continue` on the
/// zero-accounts-resolved match arm in `jobs_account_backfill::
/// backfill_database`'s gate 2, so a dead-atom row falls through to the
/// write step instead of being skipped): this test FAILS at the DRY-RUN
/// assertion, before execute even runs —
/// ```text
/// assertion `left == right` failed: would-update count must match the one resolvable row
///   left: 2
///  right: 1
/// ```
/// (the dead-atom row starts reporting "would update" too). Restoring the
/// `continue` makes it PASS again. Observed by hand while developing this
/// test.
#[tokio::test]
async fn resolvable_job_is_attributed_and_unresolvable_job_is_left_alone() {
    let Some(pool) =
        pool_or_skip("resolvable_job_is_attributed_and_unresolvable_job_is_left_alone").await
    else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "jab-mixed").await;
    let live_atom = seed_atom(&pool, graph_id, "jab-mixed-live").await;
    let dead_atom = Uuid::new_v4(); // never inserted into `atoms` — the "deleted since" shape

    let resolvable_job = Uuid::new_v4();
    insert_unattributed_extract_job(&pool, resolvable_job, live_atom).await;
    let unresolvable_job = Uuid::new_v4();
    insert_unattributed_extract_job(&pool, unresolvable_job, dead_atom).await;

    let body_pool = pool.clone();
    with_cleanup(
        pool,
        vec![resolvable_job, unresolvable_job],
        user_id,
        account_id,
        async move {
            // The dry run must PREDICT the same tally the execute run produces.
            let forecast = backfill_database(&body_pool, &dry()).await.expect("dry run");
            assert_eq!(forecast.scanned, 2);
            assert_eq!(forecast.updated, 1, "would-update count must match the one resolvable row");
            assert_eq!(forecast.dead_atom, 1);
            assert_eq!(forecast.malformed_payload, 0);
            assert_eq!(forecast.ambiguous_atom, 0);
            assert_eq!(forecast.errors, 0);
            assert_eq!(forecast.accounted(), forecast.scanned);
            assert_eq!(
                job_account_id(&body_pool, resolvable_job).await,
                None,
                "a dry run must write nothing"
            );

            let real = backfill_database(&body_pool, &exec()).await.expect("execute");
            assert_eq!(
                real, forecast,
                "the dry run's tally must equal what the execute run actually did"
            );

            assert_eq!(
                job_account_id(&body_pool, resolvable_job).await,
                Some(account_id),
                "the resolvable job must be stamped with the atom's owning account"
            );
            assert_eq!(
                job_account_id(&body_pool, unresolvable_job).await,
                None,
                "the unresolvable job (dead atom) must be left completely alone"
            );
        },
    )
    .await;
}

// ── 2. Idempotency: a re-run finds nothing left to touch ───────────────────

/// **The `account_id IS NULL` predicate is this backfill's entire
/// idempotency mechanism** (see the core module's doc). Uses ONLY a
/// resolvable row (unlike test 1 above) so the second run's `scanned`
/// count demonstrates the clean case: once resolved, the row structurally
/// no longer matches its own candidate query, on this run or any other.
///
/// **Observed negative control** (dropping `AND account_id IS NULL` from
/// `backfill_database`'s candidate `SELECT`): this test FAILS — on a
/// database that already carries other `done`/`ExtractEntities` rows from
/// unrelated activity, it fails as early as the FIRST run's own `scanned`
/// assertion (this backfill is no longer scoped to unattributed rows at
/// all), e.g.
/// ```text
/// assertion `left == right` failed
///   left: 76
///  right: 1
/// ```
/// (76 = every `done`/`ExtractEntities` row present at the time, not just
/// the one seeded here). Restoring the predicate makes it PASS again.
/// Observed by hand while developing this test.
#[tokio::test]
async fn rerun_finds_nothing_left_to_resolve() {
    let Some(pool) = pool_or_skip("rerun_finds_nothing_left_to_resolve").await else {
        return;
    };
    let (user_id, account_id, graph_id) = seed_graph(&pool, "jab-rerun").await;
    let atom_id = seed_atom(&pool, graph_id, "jab-rerun").await;
    let job_id = Uuid::new_v4();
    insert_unattributed_extract_job(&pool, job_id, atom_id).await;

    let body_pool = pool.clone();
    with_cleanup(pool, vec![job_id], user_id, account_id, async move {
        let first = backfill_database(&body_pool, &exec()).await.expect("run 1");
        assert_eq!(first.scanned, 1);
        assert_eq!(first.updated, 1);
        assert_eq!(first.errors, 0);
        assert_eq!(job_account_id(&body_pool, job_id).await, Some(account_id));

        let second = backfill_database(&body_pool, &exec()).await.expect("run 2");
        assert_eq!(second.scanned, 0, "a re-run must find NO candidates for an already-resolved row");
        assert_eq!(second.updated, 0, "a re-run must write nothing");
        assert_eq!(second.errors, 0);
        assert_eq!(
            job_account_id(&body_pool, job_id).await,
            Some(account_id),
            "the account_id must be unchanged by the no-op re-run"
        );
    })
    .await;
}

// ── 3. The compiled binary, end to end ──────────────────────────────────────

/// `--control-only` (no tenant routing needed) `--dry-run` over the whole
/// database — argument parsing, DSN resolution, report and greppable
/// summary included. Must touch nothing.
#[tokio::test]
async fn bin_dry_run_writes_nothing() {
    let Some(pool) = pool_or_skip("bin_dry_run_writes_nothing").await else {
        return;
    };
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pool_or_skip");
    let (user_id, account_id, graph_id) = seed_graph(&pool, "jab-bin-dry").await;
    let atom_id = seed_atom(&pool, graph_id, "jab-bin-dry").await;
    let job_id = Uuid::new_v4();
    insert_unattributed_extract_job(&pool, job_id, atom_id).await;

    let body_pool = pool.clone();
    with_cleanup(pool, vec![job_id], user_id, account_id, async move {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_backfill_unattributed_job_accounts"))
            .arg("--control-only")
            .env("MIGRATOR_DATABASE_URL", &url)
            .env_remove("DATABASE_URL")
            .output()
            .expect("run the backfill binary");
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        assert!(out.status.success(), "binary failed:\n{stdout}\n{stderr}");
        assert!(
            stdout.contains("ONE-SHOT, RUN ONCE PER DATABASE (mode=dry-run)"),
            "missing the one-shot banner:\n{stdout}"
        );
        assert!(
            stdout.contains("DRY RUN -- nothing was written"),
            "missing the dry-run disclaimer:\n{stdout}"
        );
        assert!(
            stderr.contains("summary: backfill_unattributed_job_accounts db=control mode=dry-run"),
            "missing the greppable per-database summary:\n{stderr}"
        );
        assert!(
            stderr.contains("summary: backfill_unattributed_job_accounts db=TOTAL mode=dry-run"),
            "missing the greppable grand total:\n{stderr}"
        );
        assert!(stderr.contains("errors=0"), "summary reported errors:\n{stderr}");

        assert_eq!(
            job_account_id(&body_pool, job_id).await,
            None,
            "the binary's dry run wrote a jobs.account_id"
        );
    })
    .await;
}

/// The same binary with `--execute` actually writes.
#[tokio::test]
async fn bin_execute_writes_the_attribution() {
    let Some(pool) = pool_or_skip("bin_execute_writes_the_attribution").await else {
        return;
    };
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pool_or_skip");
    let (user_id, account_id, graph_id) = seed_graph(&pool, "jab-bin-exec").await;
    let atom_id = seed_atom(&pool, graph_id, "jab-bin-exec").await;
    let job_id = Uuid::new_v4();
    insert_unattributed_extract_job(&pool, job_id, atom_id).await;

    let body_pool = pool.clone();
    with_cleanup(pool, vec![job_id], user_id, account_id, async move {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_backfill_unattributed_job_accounts"))
            .args(["--execute", "--control-only"])
            .env("MIGRATOR_DATABASE_URL", &url)
            .env_remove("DATABASE_URL")
            .output()
            .expect("run the backfill binary");
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        assert!(out.status.success(), "binary failed:\n{stdout}\n{stderr}");
        assert!(stderr.contains("mode=execute"), "not an execute run:\n{stderr}");
        assert!(!stdout.contains("DRY RUN"), "execute run claimed to be a dry run:\n{stdout}");

        assert_eq!(
            job_account_id(&body_pool, job_id).await,
            Some(account_id),
            "the binary's execute run must have stamped the job"
        );
    })
    .await;
}

/// `--execute --dry-run` is refused rather than resolved by precedence, and
/// before any database is touched (exit 2, the usage code). No fixtures to
/// clean up — the refusal happens before any query runs.
#[tokio::test]
async fn bin_refuses_contradictory_mode_flags() {
    let Some(pool) = pool_or_skip("bin_refuses_contradictory_mode_flags").await else {
        return;
    };
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pool_or_skip");
    drop(pool);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_backfill_unattributed_job_accounts"))
        .args(["--execute", "--dry-run"])
        .env("MIGRATOR_DATABASE_URL", &url)
        .env_remove("DATABASE_URL")
        .output()
        .expect("run the backfill binary");
    assert_eq!(out.status.code(), Some(2), "expected the usage exit code");
    assert!(String::from_utf8_lossy(&out.stderr).contains("mutually exclusive"));
}
