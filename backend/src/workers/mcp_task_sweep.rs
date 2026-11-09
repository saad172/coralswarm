//! `mcp_tasks` TTL sweep + stale-`working` reconciliation worker (MCP spec
//! 2026-07-28 adoption, PR 2 — task-based `ask_ocean`; reconciliation added
//! by issue #593 item 5).
//!
//! `mcp_tasks` (migration `0140_mcp_tasks`/`0047_mcp_tasks`) rows default to a
//! 24h `expires_at` (locked spec). `mcp::tools::read::get_task_result` already
//! treats `expires_at < now()` as `not_found` at READ time, so the EXPIRY half
//! of this sweep is pure storage reclamation, not a correctness dependency —
//! it can (and does) run on a much shorter cadence than the 24h TTL itself;
//! see [`SWEEP_INTERVAL`].
//!
//! The RECONCILIATION half (#593 item 5) is not pure reclamation: no code
//! path today writes `mcp_tasks.status = 'failed'` on every failure path (a
//! process restart or panic mid-`ask_ocean` mid-completion, or a completion
//! write that itself failed and outran `mcp::tools::read::LegacyAskTaskGuard`'s
//! own best-effort drop-time fallback) other than this reconciliation, so a
//! stranded row would otherwise sit `working` — with no way for a poller to
//! tell "still genuinely running" from "silently dead" — for the entire 24h
//! TTL. Every tick also marks any `working` row older than
//! [`STALE_WORKING_THRESHOLD`] `failed`, well before that TTL. See
//! [`RECONCILE_STALE_WORKING_SQL`] for the threshold rationale.
//!
//! ## Consumer-plane RLS scoping (mirrors `workers::tombstone_purge` exactly)
//!
//! `mcp_tasks` is on migration `0140_mcp_tasks`'s RLS roster (a DIRECT
//! `account_id` policy, not graph-anchored — see that migration's header),
//! and RLS is ENFORCING in prod (the app connects as the non-`BYPASSRLS`
//! `coralswarm_app` role, OPS-4). An unscoped `DELETE`/`UPDATE ... mcp_tasks
//! ...` against the CONSUMER pool therefore hard-errors on the strict
//! `current_setting('app.account_id')` predicate (no `missing_ok`), exactly
//! the failure mode `tombstone_purge`'s own module doc documents for
//! `op_log`. The consumer-plane tick runs loop-per-account under
//! [`crate::tenancy::scope::begin_account_scoped`] for the same reason —
//! both the reconciliation `UPDATE` and the expiry `DELETE` run inside that
//! SAME per-account transaction.
//!
//! ## ORG TENANT pool: `begin_tenant_system_scoped`'s GUC, not
//! ## "whole-database isolation" (#593 item 6 correction)
//!
//! An EARLIER cut of this doc claimed a tenant database is "whole-database
//! isolation" for `mcp_tasks`, the same way it genuinely is for
//! `tombstone_purge`'s `op_log`. That was WRONG, and the error was proven
//! empirically (an unscoped `DELETE` under the real, non-`BYPASSRLS` tenant
//! app role affects ZERO rows), not just by re-reading the migration: unlike
//! `op_log`, `mcp_tasks` IS on `0009_tenant_rls`'s per-member policy roster —
//! migration `0047_mcp_tasks`'s `member_ocean_isolation` policy is
//! `USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))`, the
//! exact per-member-ocean predicate every other content table in a tenant DB
//! carries. A plain unscoped statement against that pool only ever "worked"
//! in prod because the tenant worker pool still connects as the table OWNER
//! (`WorkerTenant`'s own doc: "this pool's correctness rests on the owner
//! exemption alone" until the per-tick `begin_tenant_system_scoped`
//! conversion lands) — an owner/`BYPASSRLS` connection ignores `USING`
//! clauses entirely, ENABLE-only RLS or not. Once that connection is a real,
//! non-`BYPASSRLS` tenant app role, the exact same unscoped statement
//! resolves `tenant_rls_visible_graph_ids()` with NEITHER `app.user_id` NOR
//! `app.tenant_actor` set, which that function's own predicate denies
//! everything to (`0009_tenant_rls`) — zero graphs visible, zero rows
//! touched, silently, forever.
//!
//! The fix is the same GUC [`crate::tenancy::scope::begin_tenant_system_scoped`]
//! sets: sweeping every ocean in a tenant regardless of which member owns it
//! is exactly the "genuinely tenant-wide, no single owning user" case that
//! function's own doc names. This worker calls
//! [`crate::tenancy::scope::begin_tenant_system_scoped_without_account`]
//! rather than `begin_tenant_system_scoped` itself, though: that function
//! bundles `app.tenant_actor` with `app.account_id` (most of its callers need
//! both for OTHER tables' policies in the same transaction), and this sweep
//! has no `account_id` to give it — `WorkerTenant` carries a `clerk_org_id`
//! (`TenantId::Org`), never an `accounts.id` — nor any use for one:
//! `member_ocean_isolation`'s predicate above never reads `app.account_id` at
//! all (there is exactly one account's rows in a tenant database to begin
//! with — the `account_id` COLUMN here is informational, not a policy
//! anchor, same as `0047`'s own header notes). `begin_tenant_system_scoped_
//! without_account` lives in `tenancy::scope` (not here) specifically so
//! `grep -rn begin_tenant_system_scoped backend/src` — the audit invariant
//! that function's own doc comment names — still catches this call site; see
//! that function's own doc for the fuller rationale (adversarial review
//! round 1, LOW).
//!
//! This worker joins [`crate::workers::tenant_harness::run_multi_tenant`] as
//! a sweep worker (`HarnessConfig::sweep_worker`) — `mcp_tasks` rows live on
//! whichever plane `mcp::tools::content_tenant` resolved for the request that
//! created them (the consumer pool for personal/flag-off callers, that org's
//! own tenant pool for org callers), so every active tenant needs its own
//! sweep, exactly like the tombstone purge.

use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::scope::begin_account_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// Sweep cadence. Deliberately much shorter than the 24h TTL itself — see the
/// module doc's "pure storage reclamation" note. Hourly keeps an expired row
/// from lingering more than ~1h past its `expires_at` in steady state, at a
/// cost of one cheap indexed `DELETE`/`UPDATE` pair per tenant per hour.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Age (from `created_at`) at which a `working` `mcp_tasks` row is considered
/// stranded rather than merely slow (#593 item 5 reconciliation). A real
/// `ask_ocean` synthesis — embed + retrieve + rerank + one LLM call —
/// finishes in low single-digit seconds, at most a couple minutes on a large,
/// cold ocean; 30 minutes is generously past any legitimate in-progress call
/// (no false positives against a merely slow LLM) while still surfacing a
/// genuinely stranded task (a panic that outran even
/// `mcp::tools::read::LegacyAskTaskGuard`'s own best-effort fallback, or a
/// process restart mid-ask) hours before the 24h TTL would otherwise be the
/// only signal. Sweep-time, not boot-time: this worker already iterates
/// every tenant hourly, so folding reconciliation into the existing tick
/// needs no separate boot hook, and catches a stranded row within one more
/// sweep interval of whenever it actually stranded.
pub(crate) const STALE_WORKING_THRESHOLD: Duration = Duration::from_secs(30 * 60);

/// The reconciliation both planes' ticks run (#593 item 5.4), sharing this one
/// statement so the predicate and the error body can never drift apart
/// between the two arms. `$1` is [`STALE_WORKING_THRESHOLD`] in whole seconds.
/// Only touches rows that are STILL `working` (never overwrites a genuine
/// `completed`/`failed`/`cancelled` row a normal completion wrote in the
/// meantime — there is no race with a legitimately-finishing task, since that
/// task's own completion write is exactly the kind of write this predicate
/// stops applying to once it lands).
const RECONCILE_STALE_WORKING_SQL: &str = "UPDATE mcp_tasks \
     SET status = 'failed', \
         error = '{\"code\": \"service_unavailable\", \"message\": \"task did not reach a \
         terminal state in time; reconciled by the mcp_tasks sweep\"}'::jsonb, \
         updated_at = now() \
     WHERE status = 'working' AND created_at < now() - ($1 * INTERVAL '1 second')";

/// Per-sweep timeout on the DB call — same bound and rationale as
/// `tombstone_purge::PURGE_TIMEOUT`: a hung query degrades to a logged
/// failure and retries next cycle rather than parking the harness's
/// schedule forever.
const SWEEP_TIMEOUT: Duration = Duration::from_secs(60);

/// The org-tenant-plane sweep body: reconcile-then-expire, one transaction,
/// scoped via [`crate::tenancy::scope::begin_tenant_system_scoped_without_account`]
/// (#593 item 6) so it actually touches rows under real tenant RLS
/// enforcement, not only under today's owner-role exemption. Returns
/// `(reconciled, swept)` row counts.
async fn org_tick(pool: &PgPool) -> Result<(u64, u64), crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    let reconciled = sqlx::query(RECONCILE_STALE_WORKING_SQL)
        .bind(STALE_WORKING_THRESHOLD.as_secs() as i64)
        .execute(&mut *tx)
        .await
        .map_err(crate::store::StoreError::from)?
        .rows_affected();
    let swept = sqlx::query("DELETE FROM mcp_tasks WHERE expires_at < now()")
        .execute(&mut *tx)
        .await
        .map_err(crate::store::StoreError::from)?
        .rows_affected();
    tx.commit().await.map_err(crate::store::StoreError::from)?;
    Ok((reconciled, swept))
}

/// One `mcp_tasks` sweep (stale-`working` reconciliation, #593 item 5, THEN
/// TTL expiry) on one tenant's pool. Driven by
/// `workers::tenant_harness::run_multi_tenant` with cadence
/// [`SWEEP_INTERVAL`].
pub async fn tick(t: &WorkerTenant) -> Result<Tick, String> {
    match &t.tenant {
        // Org tenant DB: see module doc's "#593 item 6 correction" — scoped
        // via `tenancy::scope::begin_tenant_system_scoped_without_account`,
        // not a bare unscoped statement.
        TenantId::Org(_) => match tokio::time::timeout(SWEEP_TIMEOUT, org_tick(&t.pool)).await {
            Err(_) => {
                tracing::warn!(
                    timeout_secs = SWEEP_TIMEOUT.as_secs(),
                    "mcp_tasks sweep timed out; will retry next cycle"
                );
                Ok(Tick::idle())
            }
            Ok(Ok((reconciled, swept))) => {
                let total = reconciled + swept;
                if total > 0 {
                    tracing::info!(reconciled, swept, "mcp_tasks sweep complete");
                    Ok(Tick::worked(total as usize))
                } else {
                    Ok(Tick::idle())
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "mcp_tasks sweep failed; will retry next cycle");
                Ok(Tick::idle())
            }
        },
        // Consumer plane: loop-per-account under the RLS contract (module doc).
        TenantId::Consumer => consumer_tick(t).await,
    }
}

/// The consumer-plane sweep: one `begin_account_scoped` transaction per
/// account (reconcile-then-expire inside that SAME transaction), mirroring
/// `tombstone_purge::consumer_tick`'s per-account shape. A single account's
/// failure (or timeout) is logged and skipped — the rest of the sweep still
/// runs this cycle, and the next cycle retries the failed account.
async fn consumer_tick(t: &WorkerTenant) -> Result<Tick, String> {
    let account_ids: Vec<Uuid> = match tokio::time::timeout(
        SWEEP_TIMEOUT,
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts ORDER BY id").fetch_all(&t.pool),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                timeout_secs = SWEEP_TIMEOUT.as_secs(),
                "mcp_tasks sweep: account enumeration timed out; will retry next cycle"
            );
            return Ok(Tick::idle());
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "mcp_tasks sweep: account enumeration failed; will retry next cycle");
            return Ok(Tick::idle());
        }
        Ok(Ok(ids)) => ids,
    };

    let mut total_processed = 0u64;
    let mut failed_accounts = 0usize;
    for account_id in account_ids {
        let sweep_one = async {
            let mut tx = begin_account_scoped(&t.pool, account_id)
                .await
                .map_err(|e| e.to_string())?;
            let reconciled = sqlx::query(RECONCILE_STALE_WORKING_SQL)
                .bind(STALE_WORKING_THRESHOLD.as_secs() as i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?
                .rows_affected();
            let result = sqlx::query("DELETE FROM mcp_tasks WHERE expires_at < now()")
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok::<u64, String>(reconciled + result.rows_affected())
        };
        match tokio::time::timeout(SWEEP_TIMEOUT, sweep_one).await {
            Err(_) => {
                failed_accounts += 1;
                tracing::warn!(
                    %account_id,
                    timeout_secs = SWEEP_TIMEOUT.as_secs(),
                    "mcp_tasks sweep timed out for account; will retry next cycle"
                );
            }
            Ok(Err(e)) => {
                failed_accounts += 1;
                tracing::warn!(%account_id, error = %e, "mcp_tasks sweep failed for account; will retry next cycle");
            }
            Ok(Ok(n)) => total_processed += n,
        }
    }

    if total_processed > 0 {
        tracing::info!(processed = total_processed, failed_accounts, "mcp_tasks sweep complete");
        Ok(Tick::worked(total_processed as usize))
    } else {
        tracing::debug!(failed_accounts, "mcp_tasks sweep: nothing to do");
        Ok(Tick::idle())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as
// `workers::embed_retry`'s own live sweep test — a plain `cargo test` with no
// DB configured skips this (prints and returns rather than failing), so it
// runs clean under the standard workspace `--lib` job with zero behavior
// change there. Run directly against a scratch Postgres with the control
// tree applied:
//
//   RELINFER_TEST_DATABASE_URL=postgres://user@localhost/scratch_db \
//     cargo test -p coralswarm-backend --lib mcp_task_sweep -- --test-threads=1
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{PostgresStore, Store};
    use sqlx::PgPool;
    use std::sync::Arc;

    /// One (user, account, graph) fixture — mirrors `workers::embed_retry`'s
    /// own `seed_tenant` helper (duplicated rather than shared: these are
    /// separate compilation-unit-adjacent in-module test blocks, and each
    /// worker's live test already carries its own copy).
    async fn seed_tenant(pool: &PgPool, graph_name: &str) -> (Uuid, Uuid, Uuid) {
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
        let graph_id: Uuid =
            sqlx::query_scalar("INSERT INTO graphs (account_id, name) VALUES ($1, $2) RETURNING id")
                .bind(account_id)
                .bind(graph_name)
                .fetch_one(pool)
                .await
                .expect("insert graph");
        (user_id, account_id, graph_id)
    }

    /// Insert one `mcp_tasks` row, `expires_at` offset by `expires_in_secs`
    /// from now (negative = already expired).
    async fn insert_mcp_task(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        owner_user_id: Uuid,
        expires_in_secs: i64,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO mcp_tasks (account_id, graph_id, owner_user_id, tool_name, status, expires_at) \
             VALUES ($1, $2, $3, 'ask_ocean', 'working', now() + ($4 * INTERVAL '1 second')) \
             RETURNING id",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(owner_user_id)
        .bind(expires_in_secs)
        .fetch_one(pool)
        .await
        .expect("insert mcp_tasks row")
    }

    /// Insert one `mcp_tasks` row with an explicit `created_at` offset
    /// (negative = in the past) and a far-future `expires_at` — used by the
    /// reconciliation tests below to age a `working` row past
    /// [`STALE_WORKING_THRESHOLD`] WITHOUT also tripping the (independent)
    /// expiry predicate, so each test proves exactly one of the two sweep
    /// behaviors.
    async fn insert_mcp_task_with_created_at_offset(
        pool: &PgPool,
        account_id: Uuid,
        graph_id: Uuid,
        owner_user_id: Uuid,
        created_at_offset_secs: i64,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO mcp_tasks (account_id, graph_id, owner_user_id, tool_name, status, \
             created_at, expires_at) \
             VALUES ($1, $2, $3, 'ask_ocean', 'working', \
             now() + ($4 * INTERVAL '1 second'), now() + interval '24 hours') \
             RETURNING id",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(owner_user_id)
        .bind(created_at_offset_secs)
        .fetch_one(pool)
        .await
        .expect("insert mcp_tasks row")
    }

    async fn task_still_exists(pool: &PgPool, id: Uuid) -> bool {
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM mcp_tasks WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .expect("query mcp_tasks")
            .is_some()
    }

    /// `(status, error)` for one `mcp_tasks` row — used by the reconciliation
    /// tests to check the terminal shape the sweep wrote, not just that the
    /// row still exists.
    async fn task_status_and_error(pool: &PgPool, id: Uuid) -> (String, Option<serde_json::Value>) {
        sqlx::query_as::<_, (String, Option<serde_json::Value>)>(
            "SELECT status, error FROM mcp_tasks WHERE id = $1",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("query mcp_tasks")
    }

    /// Exercises the consumer-plane loop-per-account arm (module doc) against
    /// two independent accounts, each with one already-expired and one live
    /// task. What this proves: (a) the sweep's `expires_at < now()` predicate
    /// actually deletes past-TTL rows and (b) leaves live rows alone — and,
    /// per the "deliberately scoped" note below, that both hold regardless
    /// of what else is in the table, not just in an otherwise-empty DB.
    ///
    /// What this does NOT prove: real per-account RLS scoping. `consumer_
    /// tick`'s per-account loop calls `begin_account_scoped`, which only sets
    /// `app.account_id` and relies entirely on `0140_mcp_tasks`'s
    /// `account_isolation` policy to confine the unqualified `DELETE FROM
    /// mcp_tasks WHERE expires_at < now()` to that one account. But that
    /// migration only runs `ENABLE ROW LEVEL SECURITY` (not `FORCE`), and
    /// both this job's CI connection (`postgres:postgres@...`) and a local
    /// scratch-DB run connect as the table owner/superuser — a role non-
    /// FORCE RLS does not restrict at all. So in the environment this test
    /// actually runs in, every loop iteration's DELETE is unscoped and
    /// global: a mutation that dropped the per-account loop entirely, or
    /// that only ever processed the first account, would delete the exact
    /// same rows and pass this test unchanged. Proving the scoping itself
    /// needs a `coralswarm_app`-role pool instead, where non-FORCE RLS *does*
    /// apply — see
    /// [`consumer_tick_enforces_per_account_scoping_under_the_coralswarm_app_role`]
    /// below (#593 Deferred B).
    ///
    /// Deliberately scoped to what THIS test seeded, not to a global
    /// `result.processed` count: `consumer_tick` loops every account in the
    /// `accounts` table, so `processed` is a DB-wide total. Asserting it
    /// equalled exactly 2 made this test order-dependent on the shared
    /// scratch Postgres used across the CI job's other pg suites — any
    /// leftover expired `mcp_tasks` row under an unrelated account (from
    /// another suite's fixtures, or a prior run) inflates the count and
    /// fails this test despite the sweep behaving correctly. `account_c`
    /// below is exactly such a foreign row, seeded on purpose and left
    /// unswept-or-not by design: this test asserts nothing about its fate
    /// and passes either way, proving the two assertions below hold
    /// regardless of what else is in the table.
    #[tokio::test]
    async fn consumer_tick_sweeps_expired_tasks_per_account_and_leaves_live_ones() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the mcp_tasks sweep test");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        let (user_a, account_a, graph_a) = seed_tenant(&pool, "mcp task sweep test A").await;
        let (user_b, account_b, graph_b) = seed_tenant(&pool, "mcp task sweep test B").await;

        // -60s, not -3600s: comfortably past `expires_at < now()` given the
        // seed-to-sweep round trip is milliseconds, but tight enough that a
        // mutated TTL grace window under an hour still gets caught.
        let expired_a = insert_mcp_task(&pool, account_a, graph_a, user_a, -60).await;
        let live_a = insert_mcp_task(&pool, account_a, graph_a, user_a, 3600).await;
        let expired_b = insert_mcp_task(&pool, account_b, graph_b, user_b, -60).await;
        let live_b = insert_mcp_task(&pool, account_b, graph_b, user_b, 3600).await;

        // A THIRD, unrelated account with its own already-expired row —
        // stands in for a foreign leftover row elsewhere in the shared
        // scratch DB (see the doc comment above). Regression guard: without
        // this, a future re-introduction of a global `result.processed`
        // assertion would pass locally and only break once something else
        // shares the DB.
        let (user_c, account_c, graph_c) = seed_tenant(&pool, "mcp task sweep test C (noise)").await;
        let _noise_expired_c = insert_mcp_task(&pool, account_c, graph_c, user_c, -60).await;

        let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
        let wt = WorkerTenant { tenant: TenantId::Consumer, pool: pool.clone(), store };

        tick(&wt).await.expect("sweep tick");

        assert!(!task_still_exists(&pool, expired_a).await, "expired task (account A) must be swept");
        assert!(!task_still_exists(&pool, expired_b).await, "expired task (account B) must be swept");
        assert!(task_still_exists(&pool, live_a).await, "live task (account A) must NOT be swept");
        assert!(task_still_exists(&pool, live_b).await, "live task (account B) must NOT be swept");

        // Best-effort cleanup, same convention as `workers::embed_retry`'s
        // own sibling test: `accounts`/`graphs`/`mcp_tasks` are all FK'd
        // `ON DELETE CASCADE` off `accounts.id` (0001_init / 0003_atoms /
        // 0140_mcp_tasks), so deleting `accounts` then `users` per fixture
        // — in that order, since `accounts.owner_user_id` is `ON DELETE
        // RESTRICT` — takes the still-live tasks and account_c's remnants
        // with it. `.ok()`, not `.expect(...)`: harmless to skip in
        // ephemeral CI, but keeps a reused local scratch DB from
        // accumulating an account (and a live `mcp_tasks` row) per run.
        for (user_id, account_id) in [(user_a, account_a), (user_b, account_b), (user_c, account_c)] {
            let mut financial = crate::tenancy::scope::begin_account_scoped(&pool, account_id).await.unwrap();
            crate::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
            sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&mut *financial).await.ok();
            financial.commit().await.unwrap();
            sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
        }
    }

    /// #593 item 5.4: a `working` row far older than [`STALE_WORKING_THRESHOLD`]
    /// (created 2h ago here, comfortably past the 30m threshold) must be
    /// reconciled to `failed` (with a populated `error`) by the very next
    /// sweep tick, rather than sitting `working` until the 24h TTL. A
    /// freshly-created `working` row (seconds old) must be left alone —
    /// proves the threshold, not just "every working row gets clobbered".
    /// Deliberately scoped to what THIS test seeded (same "own rows only,
    /// noise account included" convention as the sibling test above), for the
    /// same order-independence reason.
    #[tokio::test]
    async fn consumer_tick_reconciles_stale_working_tasks_into_failed() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the mcp_tasks \
                     reconciliation test"
                );
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        let (user_a, account_a, graph_a) = seed_tenant(&pool, "mcp task sweep reconcile test A").await;

        // 2h old: comfortably past the 30-minute threshold.
        let stale_working =
            insert_mcp_task_with_created_at_offset(&pool, account_a, graph_a, user_a, -7200).await;
        // Freshly created: must survive reconciliation untouched.
        let fresh_working =
            insert_mcp_task_with_created_at_offset(&pool, account_a, graph_a, user_a, 0).await;

        // A second, unrelated account's own stale row — same noise-row
        // regression guard as the sibling test: proves the assertions below
        // hold regardless of what else in the shared scratch DB also gets
        // reconciled this tick, not just in an otherwise-empty table.
        let (user_b, account_b, graph_b) = seed_tenant(&pool, "mcp task sweep reconcile test B (noise)").await;
        let _noise_stale_b =
            insert_mcp_task_with_created_at_offset(&pool, account_b, graph_b, user_b, -7200).await;

        let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
        let wt = WorkerTenant { tenant: TenantId::Consumer, pool: pool.clone(), store };

        tick(&wt).await.expect("sweep tick");

        let (status, error) = task_status_and_error(&pool, stale_working).await;
        assert_eq!(status, "failed", "a working task 2h past the threshold must be reconciled to failed");
        assert!(error.is_some(), "a reconciled task must carry a populated error");

        let (fresh_status, fresh_error) = task_status_and_error(&pool, fresh_working).await;
        assert_eq!(fresh_status, "working", "a freshly-created working task must NOT be reconciled");
        assert!(fresh_error.is_none(), "an untouched working task must have no error");

        for (user_id, account_id) in [(user_a, account_a), (user_b, account_b)] {
            let mut financial = crate::tenancy::scope::begin_account_scoped(&pool, account_id).await.unwrap();
            crate::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
            sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&mut *financial).await.ok();
            financial.commit().await.unwrap();
            sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
        }
    }

    // ── #593 Deferred B: per-account scoping under real RLS enforcement ────────

    /// Test-only password for the shared, cluster-wide `coralswarm_app` role
    /// (migration `0083_consumer_rls` creates it with NO password — see that
    /// migration's own header). Not a secret: this scratch database is
    /// ephemeral, and every `*_pg.rs` app-role suite in this tree sets its own
    /// equally-throwaway value (`tests/support/app_role.rs`'s own doc comment
    /// lists the pre-existing copies); this in-module test is a small,
    /// deliberate duplicate of that same pattern, unreachable from
    /// `tests/support` (separate compilation unit — this lives inside
    /// `src/`, not `tests/`).
    const APP_ROLE_TEST_PASSWORD: &str = "mcp_task_sweep_test_only_pw";

    /// `ALTER ROLE coralswarm_app WITH PASSWORD ...`, retried against the same
    /// cluster-wide-catalog race `tests/support/app_role.rs` documents and
    /// catches (`23505`/`42710`/`XX000` — two sessions racing the same
    /// `pg_authid` write). Only meaningfully racy across CONCURRENT processes;
    /// this job's `mcp-e2e` steps run sequentially, so the retry here is
    /// belt-and-suspenders, not load-bearing — but a bare, unretried `ALTER
    /// ROLE` was exactly the CodeRabbit finding on an earlier PR's copy of
    /// this same statement (see that support module's doc comment), so this
    /// one doesn't repeat it.
    async fn set_app_role_password_with_retry(pool: &PgPool) {
        let mut attempt: u32 = 0;
        loop {
            let result = sqlx::query(&format!(
                "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
            ))
            .execute(pool)
            .await;
            match result {
                Ok(_) => return,
                Err(sqlx::Error::Database(e))
                    if matches!(e.code().as_deref(), Some("23505") | Some("42710") | Some("XX000")) =>
                {
                    attempt += 1;
                    if attempt >= 10 {
                        panic!(
                            "set coralswarm_app test password: exhausted retries on a \
                             shared-catalog race ({}): {e}",
                            e.code().as_deref().unwrap_or("?")
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20 * attempt as u64)).await;
                }
                Err(e) => panic!("set coralswarm_app test password failed: {e}"),
            }
        }
    }

    /// A pool connected AS `coralswarm_app` (a real login, not `SET ROLE`)
    /// against the same database `owner_pool`/`url` point at — same technique
    /// `tests/support/app_role.rs`'s sibling suites use (`PgConnectOptions`
    /// with an overridden username/password), duplicated here for the same
    /// separate-compilation-unit reason as every other helper in this module.
    async fn app_role_pool(owner_pool: &PgPool, url: &str) -> PgPool {
        set_app_role_password_with_retry(owner_pool).await;
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(url)
            .expect("parse test DB url")
            .username("coralswarm_app")
            .password(APP_ROLE_TEST_PASSWORD);
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .expect("connect AS coralswarm_app")
    }

    /// #593 Deferred B: the sibling test above (`consumer_tick_sweeps_
    /// expired_tasks_per_account_and_leaves_live_ones`) runs as the table
    /// owner/superuser, under which `0140_mcp_tasks`'s ENABLE-(not FORCE-)RLS
    /// `account_isolation` policy never restricts anything at all — so a
    /// mutation that dropped the per-account loop entirely (one bare
    /// unscoped statement, no `begin_account_scoped` transaction), or one
    /// that only ever processed the FIRST account, deletes/reconciles the
    /// exact same rows table-wide either way and passes that test unchanged
    /// (see its own doc comment). This test re-runs the identical two-account
    /// fixture and tick, but against a pool connected AS `coralswarm_app`
    /// (migration `0083_consumer_rls`, NOBYPASSRLS, not the table owner) —
    /// under that role `account_isolation` genuinely restricts each scoped
    /// transaction's statements to the one account whose id is in
    /// `app.account_id`, so:
    ///   * a "process only the first account" mutation would leave account
    ///     B's rows completely untouched (its iteration never runs, and NO
    ///     other iteration's statement can reach account B's rows either,
    ///     since each is confined to its own `app.account_id`) — caught by
    ///     the account-B assertions below.
    ///   * a "drop the per-account loop, run one bare unscoped statement
    ///     against `&t.pool`" mutation would run OUTSIDE any
    ///     `begin_account_scoped` transaction at all — `account_isolation`'s
    ///     predicate is `current_setting('app.account_id')::uuid` with NO
    ///     `missing_ok`, which hard-ERRORS on a connection that never set it,
    ///     rather than silently matching everything — so that statement
    ///     itself fails, and NEITHER account's rows are touched — caught by
    ///     every assertion below (nothing gets reconciled or swept at all).
    ///
    /// Fixture setup/teardown still goes through `owner_pool` (RLS-exempt,
    /// simpler); only the `tick()` call under test runs against the
    /// `coralswarm_app`-connected pool.
    #[tokio::test]
    async fn consumer_tick_enforces_per_account_scoping_under_the_coralswarm_app_role() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run the mcp_tasks \
                     app-role scoping test"
                );
                return;
            }
        };
        let owner_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres");

        let (user_a, account_a, graph_a) = seed_tenant(&owner_pool, "mcp task sweep app-role test A").await;
        let (user_b, account_b, graph_b) = seed_tenant(&owner_pool, "mcp task sweep app-role test B").await;

        let expired_a = insert_mcp_task(&owner_pool, account_a, graph_a, user_a, -60).await;
        let live_a = insert_mcp_task(&owner_pool, account_a, graph_a, user_a, 3600).await;
        let expired_b = insert_mcp_task(&owner_pool, account_b, graph_b, user_b, -60).await;
        let live_b = insert_mcp_task(&owner_pool, account_b, graph_b, user_b, 3600).await;

        // Both stale-working rows too, exercising the reconciliation half
        // under the same real-enforcement pool.
        let stale_a =
            insert_mcp_task_with_created_at_offset(&owner_pool, account_a, graph_a, user_a, -7200).await;
        let stale_b =
            insert_mcp_task_with_created_at_offset(&owner_pool, account_b, graph_b, user_b, -7200).await;

        let app_pool = app_role_pool(&owner_pool, &url).await;
        let store: Arc<dyn Store> = Arc::new(PostgresStore::new(app_pool.clone()));
        let wt = WorkerTenant { tenant: TenantId::Consumer, pool: app_pool.clone(), store };

        tick(&wt).await.expect("sweep tick under coralswarm_app");

        assert!(
            !task_still_exists(&owner_pool, expired_a).await,
            "expired task (account A) must be swept under real RLS enforcement"
        );
        assert!(
            !task_still_exists(&owner_pool, expired_b).await,
            "expired task (account B) must be swept under real RLS enforcement"
        );
        assert!(task_still_exists(&owner_pool, live_a).await, "live task (account A) must NOT be swept");
        assert!(task_still_exists(&owner_pool, live_b).await, "live task (account B) must NOT be swept");

        let (status_a, _) = task_status_and_error(&owner_pool, stale_a).await;
        assert_eq!(status_a, "failed", "stale working task (account A) must be reconciled under real RLS");
        let (status_b, _) = task_status_and_error(&owner_pool, stale_b).await;
        assert_eq!(status_b, "failed", "stale working task (account B) must be reconciled under real RLS");

        for (user_id, account_id) in [(user_a, account_a), (user_b, account_b)] {
            let mut financial = crate::tenancy::scope::begin_account_scoped(&owner_pool, account_id).await.unwrap();
            crate::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
            sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&mut *financial).await.ok();
            financial.commit().await.unwrap();
            sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&owner_pool).await.ok();
        }
    }
}
