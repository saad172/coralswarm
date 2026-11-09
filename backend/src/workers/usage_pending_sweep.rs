//! `llm_usage_pending` orphan sweeper (usage-metering plan PR6,
//! `.scratch/usage-metering/plan.md` §1 PR6).
//!
//! PR4 established the reserve/finalize choke point: a pending row is
//! inserted and committed BEFORE an LLM call, then a matching finalize
//! deletes it and writes the real `llm_usage` row once the call completes.
//! When finalize never runs, the pending row is deliberately left behind
//! rather than voided at the call site. This worker is the backstop that
//! reclaims those rows, exactly the role `workers::reaper` plays for
//! orphaned `jobs` rows.
//!
//! **Billing-correctness fix — this doc used to call a client disconnect
//! "the COMMON case, not an edge case." That is no longer true, and the
//! surface this worker backstops is now much smaller.** A provider `Err` now
//! finalizes too (`metered_complete`/`metered_complete_via_stream`'s own
//! `Err` arms, and the raw-stream shape's `StreamUsageGuard::finalize_err`),
//! and a genuinely abandoned/cancelled raw stream now finalizes via
//! `llm::usage::StreamUsageGuard`'s `Drop` impl with whatever tokens were
//! actually observed before the drop — see that struct's own doc. What
//! remains for THIS worker to reap is now only the cases where NEITHER of
//! those can run at all: a killed process (no destructor runs on `SIGKILL`),
//! or a `StreamUsageGuard` dropped with no Tokio runtime handle available to
//! spawn onto (see that struct's `Drop` doc) — genuinely rare, not the common
//! path. See `llm::usage`'s own module doc, "A pending row is deliberately
//! left behind" paragraph, for the fuller picture.
//!
//! ## Two orphan classes, two very different floors
//!
//! `llm_usage_pending.kind` (`'sync'` | `'batch'`) exists for exactly one
//! reason: a sync call and a batch call have wildly different legitimate
//! in-flight durations, and conflating them would either reap live batch work
//! or leave sync orphans around for a day.
//!
//! * **`kind='sync'`** — [`SYNC_ORPHAN_AFTER`] (~15 minutes). Every
//!   synchronous entry point (`metered_complete`, `metered_complete_via_
//!   stream`, the hand-wired `reserve_for_stream`/`finalize_for_stream` pair
//!   around the interactive Ask SSE stream) is backed by an HTTP round-trip
//!   bounded by `LLM_TIMEOUT_SECS` (60s default) plus at most a few retries —
//!   a `kind='sync'` pending row still alive 15 minutes after `started_at` is
//!   unambiguously orphaned, not merely slow.
//! * **`kind='batch'`** — [`BATCH_ORPHAN_AFTER`] (~25 hours). The retired
//!   Anthropic Message-Batches extraction worker waited up to a full 24h
//!   (Anthropic's documented batch processing window) between reserving and
//!   finalizing; historical `kind='batch'` rows still age out on this floor. A
//!   `kind='batch'` floor anywhere near the sync one would reap LIVE,
//!   still-processing (and still-billing) batches out from under a worker
//!   that is legitimately still waiting on them — this is the headline
//!   negative control this worker's own tests exist to prove: **a 20-hour-old
//!   `kind='batch'` pending must NOT be reaped.**
//!
//! ## What "reaped" means: void, never fabricate
//!
//! A reaped row is DELETED, not "finalized" with the estimate `reserve`
//! wrote. `llm_usage_pending.estimated_input_tokens` exists only for
//! in-flight visibility and is explicitly documented (`llm::usage`'s own doc)
//! as NEVER billed — writing it into `llm_usage` as though it were a real,
//! metered actual would misrepresent cost data for a call this worker has no
//! way to know actually happened, ran to completion, or used that many
//! tokens. The honest signal here is a `tracing::warn!` per reaped row (for
//! operational visibility) and nothing more. This is also why reaping a
//! `kind='sync'` row slightly EARLY (a legitimately slow call crossing the 15
//! minute floor) is harmless rather than a correctness bug: `finalize`'s own
//! pending-row DELETE is idempotent (`DELETE ... WHERE id = $1` matching zero
//! rows is not an error — see `llm::usage::finalize`'s SAVEPOINT-guarded
//! delete), so a finalize that eventually does run after this sweeper already
//! reaped its pending row still commits the real `llm_usage` insert; it just
//! finds nothing to delete. No double charge, no lost accounting — the only
//! cost is losing a few minutes of in-flight visibility on a call that turned
//! out to be legitimately slow.
//!
//! ## Reap shape: one bulk statement per scope, never a per-row loop with a
//! ## shared transaction
//!
//! This train's standing lesson (see `llm::usage::finalize`'s own SAVEPOINT
//! doc) is that a bare `if let Err(e) = ... { warn!(...) }` inside a shared
//! Postgres transaction does NOT skip-and-continue: ANY statement error
//! aborts the WHOLE transaction (25P02), silently poisoning every later
//! statement including the final COMMIT. This worker never does that. Each
//! reap is exactly ONE `DELETE ... WHERE id IN (SELECT ...) RETURNING ...`
//! statement (see [`REAP_SQL`]) — there is no second statement in the same
//! transaction that could poison, so there is nothing for a SAVEPOINT to
//! protect. The per-tenant/per-account FAN-OUT below (see "RLS scoping") is
//! the one place this worker DOES loop with a best-effort contract, and it
//! sidesteps the same hazard structurally rather than with SAVEPOINTs: each
//! account gets its OWN, INDEPENDENT transaction (`begin_account_scoped`
//! opens a fresh one every iteration), so one account's failure can only ever
//! abort THAT account's transaction — the next iteration's `begin_account_
//! scoped` call starts clean and is entirely unaffected. See
//! `consumer_tick_one_account_failure_does_not_abort_the_rest` for the proof.
//!
//! ## RLS scoping (mirrors `workers::mcp_task_sweep` exactly)
//!
//! `llm_usage_pending` carries the SAME two RLS policies as `llm_usage`
//! itself: control-plane `account_isolation` (`account_id =
//! current_setting('app.account_id')::uuid`, STRICT — no `missing_ok`, hard-
//! errors on an unscoped statement under the real `coralswarm_app` role) and
//! tenant-plane `member_ocean_isolation` (`graph_id IN (SELECT *
//! FROM tenant_rls_visible_graph_ids())`). A sweep is inherently CROSS-account
//! (consumer plane) / CROSS-ocean (an org tenant plane), so:
//!
//! * **Consumer plane** — loop one [`crate::tenancy::scope::begin_account_scoped`]
//!   transaction per account (`consumer_tick`), same shape as
//!   `workers::mcp_task_sweep::consumer_tick`/`workers::tombstone_purge`.
//! * **Org tenant plane** — one [`crate::tenancy::scope::begin_tenant_system_scoped_without_account`]
//!   transaction for the whole tick (`org_tick`) — a sweep is genuinely
//!   tenant-wide, not one member's ocean, which is exactly the case that
//!   function exists for (see `workers::mcp_task_sweep`'s own "#593 item 6
//!   correction" doc section for why a bare unscoped statement is NOT safe to
//!   rely on here, despite today's worker-pool owner exemption).
//!
//! ## Shutdown: no drain, and that is deliberate (usage-metering plan's own
//! ## call — the register said "hook `lib.rs:3644`", which is wrong)
//!
//! `lib.rs:3644` (as of the plan's authoring) is only a COMMENT inside
//! `drain_one`'s body, not the drain mechanism itself — the real "Drain
//! shutdown-aware job workers" step lives at `lib.rs:3608-3688` and is a
//! `tokio::join!` over a fixed list of `Option<JoinHandle<()>>`s captured at
//! spawn time. Per `crate::shutdown`'s own module doc, `workers::reaper` and
//! `workers::embed_retry` (both periodic sweepers, not claimed-job drainers)
//! deliberately get NO grace window there: "a sweep is one bounded DB round-
//! trip, not a multi-step job with an LLM call or R2 fetch in it, so there's
//! nothing worth draining." This worker's tick is the same shape — one (or,
//! on the consumer plane, a handful of per-account) bounded `DELETE`, each
//! wrapped in [`SWEEP_TIMEOUT`] — so it is spawned in `lib.rs` WITHOUT
//! capturing a `JoinHandle` at all (`tokio::spawn(...)` with no `let handle
//! =`), exactly like `reaper`/`tombstone_purge`/`mcp_task_sweep`/
//! `review_deadline`. **Do NOT copy `workers::platform_rollup`'s spawn site
//! as the "no drain" template** — it also discards its `JoinHandle`, but for
//! an unrelated reason (nightly cadence, not "bounded round-trip"), and
//! critically it is NOT in the `drain_one` list either way, so copying it
//! teaches nothing about why THIS worker is safe to leave undrained. Shutdown
//! IS still wired, the same way every other sweep worker gets it: passed to
//! `workers::tenant_harness::run_multi_tenant`, which stops scheduling new
//! ticks once `shutdown.is_triggered()` and caps its own inter-tick sleeps at
//! its shutdown-poll interval — see that module's doc for the scheduling
//! side of the contract.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::scope::begin_account_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// Sweep cadence. A fraction of [`SYNC_ORPHAN_AFTER`] (same "catch it
/// reasonably close to the threshold" reasoning as `workers::reaper::
/// REAP_INTERVAL` relative to its own stale-job threshold) — worst case, a
/// sync orphan is reaped within `SYNC_ORPHAN_AFTER + SWEEP_INTERVAL` of
/// crossing the floor, not `SYNC_ORPHAN_AFTER` exactly.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// `kind='sync'` orphan floor — see the module doc's "Two orphan classes"
/// section. `pub(crate)`: read by this module's own tests.
pub(crate) const SYNC_ORPHAN_AFTER: Duration = Duration::from_secs(15 * 60);

/// `kind='batch'` orphan floor — see the module doc's "Two orphan classes"
/// section. MUST stay comfortably above the ~24h `wait_until_ended` polls
/// for; reaping earlier deletes live, still-billing work.
pub(crate) const BATCH_ORPHAN_AFTER: Duration = Duration::from_secs(25 * 60 * 60);

/// Per-sweep (or, on the consumer plane, per-account) DB-call timeout —
/// same bound/rationale as `workers::reaper::REAP_TIMEOUT` /
/// `workers::mcp_task_sweep::SWEEP_TIMEOUT`: a hung query degrades to a
/// logged failure and retries next cycle rather than parking the harness's
/// schedule forever.
const SWEEP_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on how many rows one reap statement deletes — mirrors
/// `workers::reaper::REAP_BATCH_LIMIT`'s "leave the excess for the next
/// cycle" posture rather than rewriting an unbounded number of rows in one
/// transaction. `LIMIT`, not a hard ceiling: any excess is simply picked up
/// by the next [`SWEEP_INTERVAL`] tick (or, on the consumer plane, the next
/// account's own independent transaction).
const SWEEP_BATCH_LIMIT: i64 = 200;

/// The shared reap statement every plane/account transaction runs — kept in
/// ONE place so the two cutoffs can never drift apart between the org and
/// consumer arms. `$1`/`$2` are the two cutoff `TIMESTAMPTZ`s (sync, batch);
/// `$3` is [`SWEEP_BATCH_LIMIT`]. `FOR UPDATE SKIP LOCKED` on the inner
/// SELECT is defensive (never two sweeps racing the same row today, since
/// the harness runs one tick per tenant at a time) rather than load-bearing.
///
/// This is exactly ONE statement — a `DELETE` whose `WHERE` clause contains a
/// subquery expression, not multiple top-level SQL statements — so it goes
/// through sqlx's prepared-statement path cleanly (see the module doc's "Reap
/// shape" section for why that matters here).
const REAP_SQL: &str = "DELETE FROM llm_usage_pending \
     WHERE id IN ( \
         SELECT id FROM llm_usage_pending \
         WHERE (kind = 'sync' AND started_at < $1) OR (kind = 'batch' AND started_at < $2) \
         ORDER BY started_at \
         LIMIT $3 \
         FOR UPDATE SKIP LOCKED \
     ) \
     RETURNING id, account_id, graph_id, task, model, kind, started_at";

/// One reaped row's identifying detail, kept only long enough to log it —
/// see the module doc's "What 'reaped' means" section for why this worker
/// deliberately does NOT write anything derived from these fields into
/// `llm_usage`.
struct ReapedPending {
    id: Uuid,
    account_id: Option<Uuid>,
    graph_id: Option<Uuid>,
    task: String,
    model: String,
    kind: String,
    started_at: DateTime<Utc>,
}

/// Run [`REAP_SQL`] against an already-scoped connection. `conn` is `&mut
/// PgConnection` (not `&mut Transaction`) so both call sites below can pass
/// `&mut *tx` off their own OWNED `Transaction` — the standard sqlx idiom
/// used throughout this codebase (e.g. `llm::usage::finalize`'s own
/// `.execute(&mut *tx)` calls).
async fn reap(
    conn: &mut sqlx::postgres::PgConnection,
    sync_cutoff: DateTime<Utc>,
    batch_cutoff: DateTime<Utc>,
) -> Result<Vec<ReapedPending>, sqlx::Error> {
    let rows: Vec<(Uuid, Option<Uuid>, Option<Uuid>, String, String, String, DateTime<Utc>)> =
        sqlx::query_as(REAP_SQL)
            .bind(sync_cutoff)
            .bind(batch_cutoff)
            .bind(SWEEP_BATCH_LIMIT)
            .fetch_all(conn)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(id, account_id, graph_id, task, model, kind, started_at)| ReapedPending {
            id,
            account_id,
            graph_id,
            task,
            model,
            kind,
            started_at,
        })
        .collect())
}

/// The two cutoff instants every reap this tick runs against — computed ONCE
/// per tick (not per account/transaction) so every scoped transaction in a
/// single consumer-plane sweep agrees on exactly the same boundary.
fn cutoffs() -> (DateTime<Utc>, DateTime<Utc>) {
    let now = Utc::now();
    let sync_age = chrono::Duration::from_std(SYNC_ORPHAN_AFTER)
        .expect("SYNC_ORPHAN_AFTER is a small fixed constant, always representable");
    let batch_age = chrono::Duration::from_std(BATCH_ORPHAN_AFTER)
        .expect("BATCH_ORPHAN_AFTER is a small fixed constant, always representable");
    (now - sync_age, now - batch_age)
}

/// Best-effort operational visibility for every row this tick reaped — see
/// the module doc's "What 'reaped' means" section for why this is the ONLY
/// trace a reaped row leaves.
fn log_reaped(reaped: &[ReapedPending]) {
    let now = Utc::now();
    for row in reaped {
        let age_secs = (now - row.started_at).num_seconds().max(0);
        tracing::warn!(
            pending_id = %row.id,
            kind = %row.kind,
            task = %row.task,
            model = %row.model,
            account_id = ?row.account_id,
            graph_id = ?row.graph_id,
            age_secs,
            "usage_pending_sweep: reaped an orphaned llm_usage_pending row"
        );
    }
}

/// Org-tenant-plane arm — see the module doc's "RLS scoping" section for why
/// this is `begin_tenant_system_scoped_without_account`, not a bare unscoped
/// statement.
async fn org_tick(pool: &PgPool) -> Result<Vec<ReapedPending>, crate::store::StoreError> {
    let (sync_cutoff, batch_cutoff) = cutoffs();
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?;
    let reaped = reap(&mut tx, sync_cutoff, batch_cutoff)
        .await
        .map_err(crate::store::StoreError::from)?;
    tx.commit().await.map_err(crate::store::StoreError::from)?;
    Ok(reaped)
}

/// One `llm_usage_pending` sweep on one tenant's pool. Driven by
/// `workers::tenant_harness::run_multi_tenant` with cadence
/// [`SWEEP_INTERVAL`] — see the module doc's shutdown section for why this
/// worker is spawned without a `JoinHandle` (no drain).
pub async fn tick(t: &WorkerTenant) -> Result<Tick, String> {
    match &t.tenant {
        TenantId::Org(_) => match tokio::time::timeout(SWEEP_TIMEOUT, org_tick(&t.pool)).await {
            Err(_) => {
                tracing::warn!(
                    timeout_secs = SWEEP_TIMEOUT.as_secs(),
                    "usage_pending_sweep: org-tenant sweep timed out; will retry next cycle"
                );
                Ok(Tick::idle())
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "usage_pending_sweep: org-tenant sweep failed; will retry next cycle");
                Ok(Tick::idle())
            }
            Ok(Ok(reaped)) => {
                if reaped.is_empty() {
                    Ok(Tick::idle())
                } else {
                    let n = reaped.len();
                    log_reaped(&reaped);
                    Ok(Tick::worked(n))
                }
            }
        },
        TenantId::Consumer => consumer_tick(t).await,
    }
}

/// The consumer-plane sweep: one `begin_account_scoped` transaction per
/// account (mirrors `workers::mcp_task_sweep::consumer_tick` /
/// `workers::tombstone_purge::consumer_tick`'s identical shape). A single
/// account's failure (or timeout) is logged and skipped — every OTHER
/// account's own, independent transaction still runs this cycle; see the
/// module doc's "Reap shape" section for why this structurally cannot
/// poison anything, and `consumer_tick_one_account_failure_does_not_abort_
/// the_rest` for the proof.
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
                "usage_pending_sweep: account enumeration timed out; will retry next cycle"
            );
            return Ok(Tick::idle());
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "usage_pending_sweep: account enumeration failed; will retry next cycle");
            return Ok(Tick::idle());
        }
        Ok(Ok(ids)) => ids,
    };

    let (sync_cutoff, batch_cutoff) = cutoffs();
    let mut total_processed = 0usize;
    let mut failed_accounts = 0usize;
    for account_id in account_ids {
        let sweep_one = async {
            let mut tx = begin_account_scoped(&t.pool, account_id)
                .await
                .map_err(|e| e.to_string())?;
            let reaped = reap(&mut tx, sync_cutoff, batch_cutoff)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok::<Vec<ReapedPending>, String>(reaped)
        };
        match tokio::time::timeout(SWEEP_TIMEOUT, sweep_one).await {
            Err(_) => {
                failed_accounts += 1;
                tracing::warn!(
                    %account_id,
                    timeout_secs = SWEEP_TIMEOUT.as_secs(),
                    "usage_pending_sweep: sweep timed out for account; will retry next cycle"
                );
            }
            Ok(Err(e)) => {
                failed_accounts += 1;
                tracing::warn!(%account_id, error = %e, "usage_pending_sweep: sweep failed for account; will retry next cycle");
            }
            Ok(Ok(reaped)) => {
                total_processed += reaped.len();
                log_reaped(&reaped);
            }
        }
    }

    if total_processed > 0 {
        tracing::info!(processed = total_processed, failed_accounts, "usage_pending_sweep: consumer-plane sweep complete");
        Ok(Tick::worked(total_processed))
    } else {
        tracing::debug!(failed_accounts, "usage_pending_sweep: nothing to do");
        Ok(Tick::idle())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as `workers::
// mcp_task_sweep`'s own live tests — a plain `cargo test` with no DB
// configured skips these (prints and returns rather than failing).
//
// Standing rule: NEVER round-trip `Utc::now()` through Postgres and assert
// equality (chrono carries nanosecond precision on Linux; `TIMESTAMPTZ`
// stores microseconds — passes on macOS, flakes in CI). Every fixture row
// below ages itself via `now() - (interval '1 minute' * $N)`, computed
// entirely DB-side — no Rust-side timestamp ever crosses the wire.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{PostgresStore, Store};
    use std::sync::Arc;

    /// One (user, account) fixture. `graph_id` is `None` here — this
    /// worker's own reap statement never reads `graph_id` on the consumer
    /// plane (RLS confines it via `app.account_id`, not this column), so a
    /// pending row's `graph_id` is irrelevant to every test in this module.
    async fn seed_account(pool: &PgPool) -> (Uuid, Uuid) {
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

    /// Insert one `llm_usage_pending` row, aged `minutes_ago` minutes old —
    /// entirely via DB-side arithmetic (`now() - interval '1 minute' * $N`),
    /// never a Rust-side `Utc::now()` (see this module's own test-section
    /// doc comment above).
    async fn insert_pending(
        pool: &PgPool,
        account_id: Uuid,
        kind: &str,
        minutes_ago: f64,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO llm_usage_pending \
                (account_id, task, provider, model, estimated_input_tokens, kind, started_at) \
             VALUES ($1, 'sweep_test_task', 'anthropic', 'sweep-test-model', 1, $2, \
                     now() - (interval '1 minute' * $3)) \
             RETURNING id",
        )
        .bind(account_id)
        .bind(kind)
        .bind(minutes_ago)
        .fetch_one(pool)
        .await
        .expect("insert llm_usage_pending fixture row")
    }

    async fn pending_exists(pool: &PgPool, id: Uuid) -> bool {
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM llm_usage_pending WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .expect("query llm_usage_pending")
            .is_some()
    }

    async fn cleanup(pool: &PgPool, user_id: Uuid, account_id: Uuid) {
        sqlx::query("DELETE FROM llm_usage_pending WHERE account_id = $1").bind(account_id).execute(pool).await.ok();
        sqlx::query("DELETE FROM llm_usage WHERE account_id = $1").bind(account_id).execute(pool).await.ok();
        let mut financial = crate::tenancy::scope::begin_account_scoped(pool, account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
        sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&mut *financial).await.ok();
        financial.commit().await.unwrap();
        sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(pool).await.ok();
    }

    fn wt(pool: PgPool) -> WorkerTenant {
        let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
        WorkerTenant { tenant: TenantId::Consumer, pool, store }
    }

    /// **Negative control (headline, per the PR6 task spec): a 20-hour-old
    /// `kind='batch'` pending is NOT reaped.** `wait_until_ended` polls up to
    /// 24h; reaping this early would delete live, still-billing work.
    #[tokio::test]
    async fn batch_pending_20_hours_old_is_not_reaped() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run batch_pending_20_hours_old_is_not_reaped");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await
            .expect("connect to test Postgres");
        let (user_id, account_id) = seed_account(&pool).await;
        let id = insert_pending(&pool, account_id, "batch", 20.0 * 60.0).await;

        tick(&wt(pool.clone())).await.expect("sweep tick");

        assert!(
            pending_exists(&pool, id).await,
            "a 20h-old batch pending must survive — wait_until_ended can legitimately still be polling"
        );

        cleanup(&pool, user_id, account_id).await;
    }

    /// **Negative control (paired with the one above, per the PR6 task
    /// spec): a 16-minute-old `kind='sync'` pending IS reaped.** Without
    /// this, the batch control above could pass simply because the sweeper
    /// reaps nothing at all — both directions, or neither proves anything.
    #[tokio::test]
    async fn sync_pending_16_minutes_old_is_reaped() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run sync_pending_16_minutes_old_is_reaped");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await
            .expect("connect to test Postgres");
        let (user_id, account_id) = seed_account(&pool).await;
        let id = insert_pending(&pool, account_id, "sync", 16.0).await;

        tick(&wt(pool.clone())).await.expect("sweep tick");

        assert!(!pending_exists(&pool, id).await, "a 16-minute-old sync pending must be reaped");

        cleanup(&pool, user_id, account_id).await;
    }

    /// A sync pending well WITHIN the floor (5 minutes) must survive — a real
    /// in-flight call, not an orphan. Proves the sweeper isn't simply
    /// deleting every `kind='sync'` row regardless of age.
    #[tokio::test]
    async fn sync_pending_within_the_floor_is_not_reaped() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run sync_pending_within_the_floor_is_not_reaped");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await
            .expect("connect to test Postgres");
        let (user_id, account_id) = seed_account(&pool).await;
        let id = insert_pending(&pool, account_id, "sync", 5.0).await;

        tick(&wt(pool.clone())).await.expect("sweep tick");

        assert!(pending_exists(&pool, id).await, "a 5-minute-old sync pending is a real in-flight call, not an orphan");

        cleanup(&pool, user_id, account_id).await;
    }

    /// The `kind='batch'` floor's upper bound: a 26-hour-old batch pending
    /// (comfortably past the ~25h floor) IS eventually reaped — proves the
    /// floor is a real threshold, not an accidental permanent skip of every
    /// `kind='batch'` row.
    #[tokio::test]
    async fn batch_pending_26_hours_old_is_reaped() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run batch_pending_26_hours_old_is_reaped");
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await
            .expect("connect to test Postgres");
        let (user_id, account_id) = seed_account(&pool).await;
        let id = insert_pending(&pool, account_id, "batch", 26.0 * 60.0).await;

        tick(&wt(pool.clone())).await.expect("sweep tick");

        assert!(!pending_exists(&pool, id).await, "a 26h-old batch pending is a real orphan and must be reaped");

        cleanup(&pool, user_id, account_id).await;
    }

    // ── `coralswarm_app`-role plumbing for the negative control below ──────────
    //
    // `REAP_SQL` carries NO explicit `account_id = ...` predicate of its own
    // — on the consumer plane it relies ENTIRELY on `account_isolation`'s
    // `USING` clause (via `begin_account_scoped`'s `app.account_id` GUC) to
    // confine one account's transaction to that account's own rows. Under
    // the table OWNER/superuser role this test's OTHER fixtures connect as,
    // RLS restricts NOTHING (ENABLE-only, not FORCE), so a bare `DELETE ...
    // WHERE id IN (SELECT ...)` with no per-account filter would process
    // BOTH accounts' eligible rows inside ONE account's transaction — which
    // would make a delete-blocking trigger abort BOTH rows' deletion in the
    // SAME statement, silently proving nothing about per-account isolation.
    // Confirmed empirically before landing this: the literal-embedded-UUID
    // trigger version of this test failed with EXACTLY that symptom (neither
    // row reaped) when run as the table owner. So this ONE test — the only
    // one in this module that depends on real per-account confinement, not
    // merely on the reap predicate's own age/kind logic — runs `tick()`
    // against a pool connected AS `coralswarm_app` (migration
    // `0083_consumer_rls`, non-`BYPASSRLS`), same technique `workers::
    // mcp_task_sweep`'s own `consumer_tick_enforces_per_account_scoping_
    // under_the_coralswarm_app_role` test uses, duplicated here (small,
    // deliberate copy — unreachable from `backend/tests/support/app_role.rs`,
    // a separate compilation unit) rather than imported.

    /// Test-only password for the shared, cluster-wide `coralswarm_app` role
    /// (migration `0083_consumer_rls` creates it with NO password). Not a
    /// secret: this scratch database is ephemeral.
    const APP_ROLE_TEST_PASSWORD: &str = "usage_pending_sweep_test_only_pw";

    /// `ALTER ROLE coralswarm_app WITH PASSWORD ...`, retried against the
    /// shared `pg_authid` catalog race `backend/tests/support/app_role.rs`
    /// documents (`23505`/`42710`/`XX000`) — belt-and-suspenders here since
    /// this suite runs with `--test-threads=1`, but a bare unretried `ALTER
    /// ROLE` was exactly a prior CodeRabbit finding on a copy of this same
    /// statement elsewhere in this tree.
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
                        panic!("set coralswarm_app test password: exhausted retries: {e}");
                    }
                    tokio::time::sleep(Duration::from_millis(20 * attempt as u64)).await;
                }
                Err(e) => panic!("set coralswarm_app test password failed: {e}"),
            }
        }
    }

    /// A pool connected AS `coralswarm_app` (a real login, not `SET ROLE`)
    /// against the same database `owner_pool`/`url` point at.
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

    /// **Negative control (required by the PR6 task spec): the consumer-plane
    /// per-account loop's best-effort contract.** Account A's transaction is
    /// made to fail (a `BEFORE DELETE` trigger that raises, same induction
    /// technique as `llm::usage::pg_tests::failed_pending_delete_still_
    /// commits_the_llm_usage_row`) while account B has its own eligible
    /// orphan. Account B's row must STILL be reaped — one account's failure
    /// must not silently abort the sweep of every other account, and (see
    /// this module's "Reap shape" doc section) this worker's per-account
    /// independent-transaction shape is what makes that true structurally,
    /// not a SAVEPOINT. Runs `tick()` under the REAL `coralswarm_app` role —
    /// see this section's own doc comment for why the table-owner role this
    /// suite's other tests use cannot exercise this specific property.
    #[tokio::test]
    async fn consumer_tick_one_account_failure_does_not_abort_the_rest() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run \
                     consumer_tick_one_account_failure_does_not_abort_the_rest"
                );
                return;
            }
        };
        let owner_pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await
            .expect("connect to test Postgres");
        let (user_a, account_a) = seed_account(&owner_pool).await;
        let (user_b, account_b) = seed_account(&owner_pool).await;
        let id_a = insert_pending(&owner_pool, account_a, "sync", 16.0).await;
        let id_b = insert_pending(&owner_pool, account_b, "sync", 16.0).await;

        // Two separate statements: sqlx sends these through the
        // prepared-statement path, which rejects multiple commands in one
        // query (42601).
        sqlx::query(&format!(
            "CREATE OR REPLACE FUNCTION sweep_test_block_delete() RETURNS trigger AS $$ \
             BEGIN IF OLD.account_id = '{account_a}'::uuid THEN \
             RAISE EXCEPTION 'sweep test: delete blocked for this account'; END IF; \
             RETURN OLD; END; $$ LANGUAGE plpgsql"
        ))
        .execute(&owner_pool)
        .await
        .expect("create the delete-blocking function");
        sqlx::query(
            "CREATE TRIGGER sweep_test_block_delete BEFORE DELETE ON llm_usage_pending \
             FOR EACH ROW EXECUTE FUNCTION sweep_test_block_delete()",
        )
        .execute(&owner_pool)
        .await
        .expect("install the delete-blocking trigger");

        let app_pool = app_role_pool(&owner_pool, &url).await;
        tick(&wt(app_pool.clone())).await.expect("sweep tick under coralswarm_app");

        sqlx::query("DROP TRIGGER IF EXISTS sweep_test_block_delete ON llm_usage_pending;")
            .execute(&owner_pool)
            .await
            .expect("drop the trigger");

        assert!(
            pending_exists(&owner_pool, id_a).await,
            "account A's row survives — its own transaction failed and rolled back"
        );
        assert!(
            !pending_exists(&owner_pool, id_b).await,
            "account B's row must still be reaped — its independent transaction never touched account A's failure"
        );

        cleanup(&owner_pool, user_a, account_a).await;
        cleanup(&owner_pool, user_b, account_b).await;
    }

    /// **Done + verified (PR6 task spec): a simulated client disconnect
    /// mid-stream leaves a pending row the sweeper then VOIDS correctly.**
    /// Reserves through the REAL `llm::usage::reserve_for_stream` primitive
    /// (the same one `synthesis::synthesize_stream`'s interactive Ask SSE
    /// path uses) rather than a raw INSERT, so this exercises the actual
    /// integration, not just this module's own SQL. The stream is never
    /// finalized (no `StreamUsageGuard` is constructed here at all — see
    /// `llm::usage::pg_tests::reserve_for_stream_without_a_matching_
    /// finalize_leaves_one_pending_and_zero_usage_rows`, which proves the
    /// pending row survives up to this point on the BARE primitive); the row
    /// is then aged past the sync floor and swept. Asserts the VOID contract
    /// from this module's "What 'reaped' means" doc section: the pending row
    /// is gone AND no `llm_usage` row was fabricated from the estimate.
    ///
    /// **Billing-correctness fix — what this test does NOT cover any more:**
    /// the REAL production caller (`synthesis::synthesize_stream`) also
    /// constructs a `llm::usage::StreamUsageGuard` alongside this exact
    /// reserve, and that guard's `Drop` now recovers a genuinely
    /// abandoned/cancelled stream with the tokens actually observed — see
    /// `llm::usage::pg_tests::stream_usage_guard_dropped_without_a_terminal_
    /// finalize_writes_the_tokens_actually_streamed`. This test still proves
    /// a real, if narrower, scenario: a bare `reserve_for_stream` with no
    /// guard attached (a caller that bypasses it) or a process kill (no
    /// `Drop` runs at all) is still correctly voided here, never fabricated.
    #[tokio::test]
    async fn disconnect_mid_stream_pending_is_voided_by_the_sweeper() {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!(
                    "skipping: set RELINFER_TEST_DATABASE_URL to run \
                     disconnect_mid_stream_pending_is_voided_by_the_sweeper"
                );
                return;
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await
            .expect("connect to test Postgres");
        let (user_id, account_id) = seed_account(&pool).await;

        let ctx = crate::llm::usage::UsageContext::new(pool.clone(), Some(account_id), None);
        let req = crate::llm::CompletionRequest {
            messages: vec![crate::llm::Message {
                role: crate::llm::Role::User,
                content: "simulated interactive ask, then the client hangs up".into(),
            }],
            model: "mock-model".into(),
            max_tokens: 64,
            system: None,
            tool_schema: None,
            effort: None,
            reasoning: crate::llm::ReasoningSetting::Off,
        };
        let pending_id = crate::llm::usage::reserve_for_stream(
            Some(&ctx),
            "synthesize_prose",
            "anthropic",
            &req,
            crate::llm::usage::PENDING_KIND_SYNC,
        )
        .await
        .expect("reserve must commit before stream_complete is ever called");

        // The disconnect: no `finalize_for_stream` call. Age the row past
        // the sync floor entirely DB-side, per this module's own test-section
        // doc comment.
        sqlx::query("UPDATE llm_usage_pending SET started_at = now() - interval '20 minutes' WHERE id = $1")
            .bind(pending_id)
            .execute(&pool)
            .await
            .expect("age the pending row past the sync floor");

        tick(&wt(pool.clone())).await.expect("sweep tick");

        assert!(!pending_exists(&pool, pending_id).await, "the sweeper must reap the orphaned pending row");
        let n_usage: i64 = sqlx::query_scalar("SELECT count(*) FROM llm_usage WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .expect("count llm_usage rows");
        assert_eq!(
            n_usage, 0,
            "the sweeper must VOID, never fabricate an llm_usage row from the never-billed estimate"
        );

        cleanup(&pool, user_id, account_id).await;
    }
}
