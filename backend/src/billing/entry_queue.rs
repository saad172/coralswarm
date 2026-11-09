//! The in-process queue in front of an account's financial lock (#1497).
//!
//! # The problem
//!
//! `billing_enter_account(account)` (control migration 0321) takes
//! `pg_advisory_xact_lock` keyed per account, and every financial transaction
//! for that account takes it as its first statement
//! ([`crate::billing::renewal::lock_period_rotation`]). Callers that hit it
//! at a high rate are the managed admissions (`managed::gate::funded`): an
//! org admission enters the lock three times (fence+prepare, fund,
//! fence+confirm+arm), then once more at settlement, and the first and third
//! entries hold the lock across the tenant-plane round trips of the fence
//! and the receipt. With several workers admitting for the same account at
//! once, every waiter sat on a control-pool connection for up to the 2 s
//! `lock_timeout` doing nothing. The control pool is 20 connections and also
//! serves authentication, so five overlapping waits cost unrelated requests
//! hundreds of milliseconds (prod, 2026-10-26).
//!
//! # What this does
//!
//! Callers that are ABOUT to open a financial transaction take a turn here
//! BEFORE they acquire a pool connection:
//!
//! 1. the account's in-process mutex: within one process, at most one
//!    financial transaction per account is on a connection at a time; the
//!    rest wait in memory holding nothing;
//! 2. a process-wide slot: at most [`slots`] financial transactions across
//!    all accounts hold control connections at once, so even across many
//!    accounts (or across machines, where the advisory lock still has to be
//!    waited for on a connection) the pool keeps room for requests.
//!
//! The database lock is still taken by every caller exactly as before. This
//! queue only decides WHO gets to go and hold a connection while contending
//! for it; it never replaces the lock. Correctness therefore rests where it
//! did: on the advisory lock and the 0321 entry fence. A second process, a
//! webhook handler or a CLI that does not come through this queue still
//! serializes on the database lock, and a caller that comes through this
//! queue still cannot write a fenced table without having entered.
//!
//! # Why this cannot deadlock
//!
//! The turn is taken BEFORE the connection at every call site, never after,
//! and never inside `lock_period_rotation` (which runs on an already-open
//! transaction). So:
//!
//! * a turn holder either holds a connection and is making progress, or is
//!   waiting for one behind the pool's bounded `acquire_timeout`;
//! * a turn waiter holds no connection, so it can never be what exhausts the
//!   pool a turn holder is waiting on;
//! * the database lock holder always holds a connection, so a turn holder
//!   waiting on the database lock (held by a caller outside this queue) is
//!   waiting on something that progresses.
//!
//! The one way to break this is to take a turn while already holding a
//! control connection for the same account (the mutex is not re-entrant), or
//! to take a turn, then a connection, then another turn. No call site does
//! either: `managed::gate::funded::reserve_inner` drops each phase's turn
//! right after that phase's control transaction commits and before it takes
//! the next one.
//!
//! # Every await a slot holder makes is bounded
//!
//! The queue is per process. Two app machines and a workers group contend
//! on the same database lock, and a caller outside this queue (a webhook, a
//! CLI, another process's worker) can hold it. A slot holder must therefore
//! never wait without a bound, or one remote holder would pin this
//! process's slots:
//!
//! | wait | bound | set by |
//! |---|---|---|
//! | the turn itself | [`queue_timeout`]: `BILLING_ENTRY_QUEUE_SECS`, default 20 s | [`Queue::turn`] |
//! | a control-pool connection | `DB_ACQUIRE_TIMEOUT_SECS`, default 10 s, ceiling 60 s | `lib.rs` pool options |
//! | a tenant-pool connection | `tenancy::registry::DEFAULT_ACQUIRE_TIMEOUT` | the registry |
//! | the database lock, holding a slot | `lock_timeout` = [`LOCK_WAIT`] (2 s) | [`enter`], or `managed_reservation::enter_account_tx` (same bound, plus `statement_timeout` 5 s) |
//!
//! Every site that takes a turn enters the lock through one of those two
//! helpers, never through a bare `lock_period_rotation`. On any of these
//! timeouts the error propagates, the transaction drops (rollback), and the
//! turn drops with the frame, so the slot and the account mutex are
//! released at once. `managed/gate/pg_tests/admission_latency.rs` pins
//! this for an admission and for the billing worker behind a lock held by
//! a connection that did not come through the queue (the other-process
//! case).
//!
//! A turn that times out fails with the same `Unavailable` a `lock_timeout`
//! produced before; the caller's existing cleanup (an unsent cancellation,
//! a job retry, the next worker tick) runs unchanged. Slots default to a
//! fifth of `DB_MAX_CONNECTIONS` (4 of 20), clamped to `1..=8`;
//! `BILLING_ENTRY_SLOTS` overrides.
//!
//! # A skip is never terminal, and a starving account is visible
//!
//! The worker sites (`billing_derivation`'s ledger write and recovery pass,
//! the auto-reload pass and its settlement) enter through [`begin_entered`].
//! Either bounded wait giving up there returns before the transaction has
//! done anything: no job, receipt, recovery-queue row or reload row is
//! marked, no attempt counter moves; the callers log, count an error for
//! the tick, and the next tick retries (the harness only backs the tenant
//! off, it persists nothing). Each such skip is logged under
//! [`LOG_TARGET`] (`billing.lock_skip`) with the account, the `site` and the
//! `kind` (`queue_timeout` / `lock_timeout`), so it can be counted; the
//! account's consecutive-skip count is kept in memory and the log becomes
//! an `error` with `alert = true` from the [`ESCALATE_AFTER`]th skip in a
//! row until the account next enters. `managed/gate/pg_tests/
//! admission_latency.rs` pins "nothing written on a mid-pass skip, each
//! receipt settled exactly once on the next pass".
//!
//! # Who stays outside the queue, and why that is fine
//!
//! * `billing::funding_policy` never takes the lock itself (its module doc:
//!   the caller takes it); it only reads and writes inside a caller's
//!   entered transaction.
//! * `billing::notices` writes only `billing_notices`, which 0367 left
//!   unfenced; it takes no financial lock.
//! * The Stripe webhooks (`extra_usage::webhook::begin_claimed`,
//!   `routes::stripe_webhook`'s renewal and seat handlers): one request
//!   connection per Stripe event, event-rate not worker-rate, and they must
//!   not queue behind workers. They do not set `lock_timeout`, so they wait
//!   out the current holder; every holder that came through this queue is
//!   bounded as above, so that wait is bounded too. Any error inside them
//!   rolls back the claim with the credit and answers 500, so Stripe
//!   redelivers (`stripe_webhook::payment_response`, `handle_invoice_paid`
//!   and the `apply_event` branch all map `Err` to 500). Should they ever be
//!   routed through this queue, [`QueueTimeout`] converts to
//!   `sqlx::Error::Protocol` and lands in the same 500 arm.
//! * Account deletion's `disable_extra_usage`/`forfeit_extra_usage`: once
//!   per deletion, one to three statements, on the deletion worker's single
//!   connection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use sqlx::PgConnection;
use tokio::sync::{OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

/// How long a slot holder waits for the database lock. The same bound
/// `managed_reservation::enter_account_tx` sets for admissions.
pub(crate) const LOCK_WAIT: Duration = Duration::from_secs(2);
const LOCK_WAIT_SQL: &str = "SELECT set_config('lock_timeout','2s',true)";
/// Postgres `lock_not_available`: what `lock_timeout` raises.
const LOCK_NOT_AVAILABLE: &str = "55P03";

/// Enter the account's financial lock on `conn` with the wait bounded by
/// [`LOCK_WAIT`]. For every turn holder that does not go through
/// `managed_reservation::enter_account_tx`: a bare `lock_period_rotation`
/// would wait without bound while holding a slot. `set_config(..., true)`
/// is transaction-local (`SET LOCAL`), never a session `SET`.
pub(crate) async fn enter(conn: &mut PgConnection, account: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(LOCK_WAIT_SQL).execute(&mut *conn).await?;
    crate::billing::renewal::lock_period_rotation(conn, account).await
}

// ── Worker sites: one entry point, one log shape, one skip counter ────────

/// The `tracing` target every skipped worker entry is logged under, so the
/// skips can be counted and alerted on (`target = "billing.lock_skip"`).
pub(crate) const LOG_TARGET: &str = "billing.lock_skip";

/// Consecutive skips of one account after which the warn becomes an
/// `error` with `alert = true`. Every holder of the lock that came through
/// this queue is bounded in seconds, and a worker tick that lands on an
/// account mid-burst is expected to skip once or twice. Five in a row at the
/// workers' cadence (auto-reload every 60 s, billing derivation every 5 min)
/// is five minutes to twenty-five minutes of one account never getting its
/// lock from this process: well past any bounded holder, and past a burst.
pub(crate) const ESCALATE_AFTER: u32 = 5;

/// Why a worker entry was skipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SkipKind {
    /// The in-process turn did not come within [`queue_timeout`].
    Queue,
    /// The database lock did not come within [`LOCK_WAIT`]: a holder in
    /// another process, or outside this queue.
    Lock,
}

impl SkipKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SkipKind::Queue => "queue_timeout",
            SkipKind::Lock => "lock_timeout",
        }
    }
}

/// Is this error one of the two bounded waits giving up? Anything else is
/// an ordinary database error for the caller to report as it always has.
///
/// [`enter`]'s `lock_timeout` is transaction-local, so a `55P03` can also be
/// raised LATER in the same transaction, by a row lock the work itself waits
/// on. That is classified as a skip too, deliberately: the transaction rolls
/// back with nothing committed exactly as an entry skip does, and "retry
/// next tick" is the right response to both. Only [`begin_entered`] feeds
/// the per-account consecutive counter, so a mid-transaction `55P03` is
/// counted by the caller that classifies it (the derivation tick's partial
/// report) but not in that counter.
pub(crate) fn skip_kind(error: &sqlx::Error) -> Option<SkipKind> {
    match error {
        sqlx::Error::Protocol(message) if message == QUEUE_TIMEOUT_MESSAGE => Some(SkipKind::Queue),
        sqlx::Error::Database(db) if db.code().as_deref() == Some(LOCK_NOT_AVAILABLE) => Some(SkipKind::Lock),
        _ => None,
    }
}

/// Consecutive worker skips per account, in memory. Cleared by the next
/// successful entry for the account.
pub(crate) struct Skips(Mutex<HashMap<Uuid, u32>>);

impl Skips {
    pub(crate) fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    /// Record one more skip; returns the new consecutive count.
    pub(crate) fn record(&self, account: Uuid) -> u32 {
        let mut map = self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let n = map.entry(account).or_insert(0);
        *n = n.saturating_add(1);
        *n
    }

    pub(crate) fn clear(&self, account: Uuid) {
        let mut map = self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        map.remove(&account);
    }

    pub(crate) fn consecutive(&self, account: Uuid) -> u32 {
        let map = self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        map.get(&account).copied().unwrap_or(0)
    }
}

fn skips() -> &'static Skips {
    static SKIPS: OnceLock<Skips> = OnceLock::new();
    SKIPS.get_or_init(Skips::new)
}

/// Whether the `consecutive`th skip in a row is logged as an alerting error.
pub(crate) fn escalates(consecutive: u32) -> bool {
    consecutive >= ESCALATE_AFTER
}

fn log_skip(account: Uuid, site: &'static str, kind: SkipKind, consecutive: u32, error: &dyn std::fmt::Display) {
    if escalates(consecutive) {
        tracing::error!(
            target: LOG_TARGET, alert = true, %account, site, kind = kind.as_str(), consecutive,
            escalate_after = ESCALATE_AFTER, error = %error,
            "billing lock skipped: account starving"
        );
    } else {
        tracing::warn!(
            target: LOG_TARGET, %account, site, kind = kind.as_str(), consecutive, error = %error,
            "billing lock skipped; retried next tick"
        );
    }
}

/// The worker sites' one entry point: the turn, then the connection, then
/// the bounded lock, in that order. On either bounded wait giving up the
/// skip is logged under [`LOG_TARGET`] with the account, `site` and kind,
/// the account's consecutive-skip count is bumped (and escalated past
/// [`ESCALATE_AFTER`]), and the error is returned for the caller to treat
/// as "skip this account, retry next tick": nothing has been written. A
/// successful entry clears the account's count.
///
/// `site` is a stable label for the caller (`billing_recovery`,
/// `billing_derivation`, `auto_reload`, `auto_reload_settle`). `begin`
/// opens the account-scoped transaction (`tenancy::scope::
/// begin_account_scoped` on the control pool); it is called only once the
/// turn is held, which is the whole point of the ordering.
pub(crate) async fn begin_entered<F, Fut, E>(
    account: Uuid,
    site: &'static str,
    begin: F,
) -> Result<(EntryTurn, sqlx::Transaction<'static, sqlx::Postgres>), sqlx::Error>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<sqlx::Transaction<'static, sqlx::Postgres>, E>>,
    E: std::fmt::Display,
{
    let entered = async {
        let turn = wait_turn(account).await?;
        let mut tx = begin().await.map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        enter(&mut tx, account).await?;
        Ok::<_, sqlx::Error>((turn, tx))
    }
    .await;
    match &entered {
        Ok(_) => skips().clear(account),
        Err(error) => {
            if let Some(kind) = skip_kind(error) {
                let consecutive = skips().record(account);
                log_skip(account, site, kind, consecutive, error);
            }
        }
    }
    entered
}

/// The account's current consecutive worker-skip count (tests, operators).
pub(crate) fn consecutive_skips(account: Uuid) -> u32 {
    skips().consecutive(account)
}

pub(crate) const SLOTS_ENV: &str = "BILLING_ENTRY_SLOTS";
pub(crate) const QUEUE_TIMEOUT_ENV: &str = "BILLING_ENTRY_QUEUE_SECS";

const DEFAULT_QUEUE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_SLOTS: usize = 64;
/// Above this many live account entries the map is swept for dropped ones.
const SWEEP_ABOVE: usize = 256;

/// Proof the caller is next for this account and holds a connection slot.
/// Drop it as soon as the financial transaction has committed (or rolled
/// back): the advisory lock is gone at that point, and the next caller
/// should go.
#[must_use = "dropping the turn immediately lets the next caller go before this transaction has even begun"]
pub(crate) struct EntryTurn {
    _account: OwnedMutexGuard<()>,
    _slot: OwnedSemaphorePermit,
}

/// The queue timed out: nothing was acquired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueTimeout;

const QUEUE_TIMEOUT_MESSAGE: &str = "financial entry queue timeout";

impl std::fmt::Display for QueueTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(QUEUE_TIMEOUT_MESSAGE)
    }
}

impl std::error::Error for QueueTimeout {}

impl From<QueueTimeout> for sqlx::Error {
    fn from(e: QueueTimeout) -> Self {
        sqlx::Error::Protocol(e.to_string())
    }
}

pub(crate) struct Queue {
    accounts: Mutex<HashMap<Uuid, Weak<tokio::sync::Mutex<()>>>>,
    slots: Arc<Semaphore>,
    timeout: Duration,
}

impl Queue {
    pub(crate) fn new(slots: usize, timeout: Duration) -> Self {
        Self {
            accounts: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(slots.max(1))),
            timeout,
        }
    }

    fn account(&self, account: Uuid) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.accounts.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(live) = map.get(&account).and_then(Weak::upgrade) {
            return live;
        }
        if map.len() > SWEEP_ABOVE {
            map.retain(|_, weak| weak.strong_count() > 0);
        }
        let fresh = Arc::new(tokio::sync::Mutex::new(()));
        map.insert(account, Arc::downgrade(&fresh));
        fresh
    }

    /// Wait for this account's turn and a connection slot, in that order:
    /// a same-account waiter blocked on the mutex consumes no slot, so a
    /// burst of admissions for one account cannot starve other accounts.
    pub(crate) async fn turn(&self, account: Uuid) -> Result<EntryTurn, QueueTimeout> {
        tokio::time::timeout(self.timeout, async {
            let account = self.account(account).lock_owned().await;
            let slot = self
                .slots
                .clone()
                .acquire_owned()
                .await
                .expect("the entry queue semaphore is never closed");
            EntryTurn { _account: account, _slot: slot }
        })
        .await
        .map_err(|_| QueueTimeout)
    }

    #[cfg(test)]
    fn free_slots(&self) -> usize {
        self.slots.available_permits()
    }

    #[cfg(test)]
    fn live_accounts(&self) -> usize {
        let map = self.accounts.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        map.values().filter(|weak| weak.strong_count() > 0).count()
    }
}

/// Connection slots for a control pool of `max_connections`: a fifth of it,
/// at least one, at most eight. With the default 20 that is 4, leaving 16
/// for requests; `admission::in_flight_cap(20)` already admits 16.
pub(crate) fn slots_for_pool(max_connections: u32) -> usize {
    (max_connections / 5).clamp(1, 8) as usize
}

fn parse_slots(raw: Option<&str>, max_connections: u32) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| (1..=MAX_SLOTS).contains(n))
        .unwrap_or_else(|| slots_for_pool(max_connections))
}

fn parse_queue_timeout(raw: Option<&str>) -> Duration {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_QUEUE_TIMEOUT)
}

pub(crate) fn slots() -> usize {
    parse_slots(std::env::var(SLOTS_ENV).ok().as_deref(), crate::db_max_connections())
}

pub(crate) fn queue_timeout() -> Duration {
    parse_queue_timeout(std::env::var(QUEUE_TIMEOUT_ENV).ok().as_deref())
}

fn global() -> &'static Queue {
    static QUEUE: OnceLock<Queue> = OnceLock::new();
    QUEUE.get_or_init(|| Queue::new(slots(), queue_timeout()))
}

/// Take this process's turn for `account`'s financial lock. Call it BEFORE
/// acquiring the pool connection the financial transaction will run on, and
/// drop the returned turn right after that transaction commits.
pub(crate) async fn wait_turn(account: Uuid) -> Result<EntryTurn, sqlx::Error> {
    Ok(global().turn(account).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHORT: Duration = Duration::from_millis(150);

    #[test]
    fn slots_are_a_fifth_of_the_pool_within_bounds() {
        assert_eq!(slots_for_pool(20), 4, "default control pool");
        assert_eq!(slots_for_pool(4), 1, "never zero");
        assert_eq!(slots_for_pool(1), 1);
        assert_eq!(slots_for_pool(100), 8, "capped");
    }

    #[test]
    fn slots_env_overrides_within_bounds_else_falls_back() {
        assert_eq!(parse_slots(Some("6"), 20), 6);
        assert_eq!(parse_slots(Some(" 2 "), 20), 2);
        for bad in ["0", "-1", "abc", "", "65"] {
            assert_eq!(parse_slots(Some(bad), 20), 4, "fallback for {bad:?}");
        }
        assert_eq!(parse_slots(None, 20), 4);
    }

    #[test]
    fn queue_timeout_env_overrides_else_defaults() {
        assert_eq!(parse_queue_timeout(Some("5")), Duration::from_secs(5));
        for bad in ["0", "x", ""] {
            assert_eq!(parse_queue_timeout(Some(bad)), DEFAULT_QUEUE_TIMEOUT, "fallback for {bad:?}");
        }
        assert_eq!(parse_queue_timeout(None), DEFAULT_QUEUE_TIMEOUT);
    }

    #[tokio::test]
    async fn same_account_callers_take_turns() {
        let queue = Queue::new(4, SHORT);
        let account = Uuid::new_v4();
        let first = queue.turn(account).await.expect("uncontended");
        assert_eq!(queue.free_slots(), 3, "the holder has a slot");
        let second = queue.turn(account).await;
        assert_eq!(second.err(), Some(QueueTimeout), "same account waits behind the holder");
        assert_eq!(queue.free_slots(), 3, "a waiter never took a slot");
        drop(first);
        let second = queue.turn(account).await.expect("the holder is gone");
        assert_eq!(queue.free_slots(), 3);
        drop(second);
        assert_eq!(queue.free_slots(), 4, "slots return with the turn");
    }

    #[tokio::test]
    async fn different_accounts_do_not_wait_on_each_other() {
        let queue = Queue::new(4, SHORT);
        let a = queue.turn(Uuid::new_v4()).await.expect("a");
        let b = queue.turn(Uuid::new_v4()).await.expect("b runs alongside a");
        assert_eq!(queue.free_slots(), 2);
        drop((a, b));
        assert_eq!(queue.free_slots(), 4);
    }

    #[tokio::test]
    async fn slots_bound_concurrent_holders_across_accounts() {
        let queue = Queue::new(1, SHORT);
        let a = queue.turn(Uuid::new_v4()).await.expect("a");
        let b = queue.turn(Uuid::new_v4()).await;
        assert_eq!(b.err(), Some(QueueTimeout), "no slot left while a holds the only one");
        drop(a);
        let _c = queue.turn(Uuid::new_v4()).await.expect("the slot came back");
    }

    #[tokio::test]
    async fn a_same_account_waiter_holds_no_slot_while_it_waits() {
        // One slot. An account-A holder, an account-A waiter, then account B
        // must still get the slot the moment A's holder releases: the waiter
        // on A's mutex never consumed it.
        let queue = Arc::new(Queue::new(1, Duration::from_secs(5)));
        let a = Uuid::new_v4();
        let holder = queue.turn(a).await.expect("holder");
        let waiter = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.turn(a).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(queue.free_slots(), 0, "only the holder has the slot");
        drop(holder);
        let next = waiter.await.unwrap().expect("the same-account waiter went next");
        assert_eq!(queue.free_slots(), 0);
        drop(next);
        let _b = queue.turn(Uuid::new_v4()).await.expect("account B gets the slot once A is done");
    }

    #[tokio::test]
    async fn a_timed_out_waiter_leaves_nothing_behind() {
        let queue = Queue::new(1, SHORT);
        let account = Uuid::new_v4();
        let holder = queue.turn(account).await.expect("holder");
        assert_eq!(queue.turn(account).await.err(), Some(QueueTimeout));
        drop(holder);
        assert_eq!(queue.free_slots(), 1, "the timed-out waiter left no slot taken");
        assert_eq!(queue.live_accounts(), 0, "and no account entry pinned");
        let _again = queue.turn(account).await.expect("the account is usable again");
    }

    #[tokio::test]
    async fn account_entries_are_dropped_with_their_last_holder() {
        let queue = Queue::new(4, SHORT);
        let turn = queue.turn(Uuid::new_v4()).await.expect("turn");
        assert_eq!(queue.live_accounts(), 1);
        drop(turn);
        assert_eq!(queue.live_accounts(), 0);
    }

    #[tokio::test]
    async fn the_map_is_swept_once_it_grows_past_the_bound() {
        let queue = Queue::new(4, SHORT);
        for _ in 0..(SWEEP_ABOVE + 1) {
            drop(queue.turn(Uuid::new_v4()).await.expect("turn"));
        }
        let before = queue.accounts.lock().unwrap().len();
        assert!(before > SWEEP_ABOVE, "dead entries accumulate until the sweep: {before}");
        drop(queue.turn(Uuid::new_v4()).await.expect("turn"));
        let after = queue.accounts.lock().unwrap().len();
        assert!(after <= 1, "the sweep removed the dead entries: {after}");
    }

    #[test]
    fn the_lock_wait_sql_sets_the_documented_bound_transaction_locally() {
        assert_eq!(LOCK_WAIT, Duration::from_secs(2));
        assert!(LOCK_WAIT_SQL.contains("'lock_timeout','2s',true"), "{LOCK_WAIT_SQL}");
        assert!(!LOCK_WAIT_SQL.to_ascii_uppercase().starts_with("SET "), "never a session SET: {LOCK_WAIT_SQL}");
    }

    #[test]
    fn queue_timeout_converts_to_a_protocol_error() {
        let e: sqlx::Error = QueueTimeout.into();
        assert!(matches!(e, sqlx::Error::Protocol(ref m) if m == "financial entry queue timeout"));
    }

    #[test]
    fn a_queue_timeout_is_a_skip_and_other_errors_are_not() {
        assert_eq!(skip_kind(&QueueTimeout.into()), Some(SkipKind::Queue));
        assert_eq!(skip_kind(&sqlx::Error::Protocol("something else".into())), None);
        assert_eq!(skip_kind(&sqlx::Error::RowNotFound), None);
        assert_eq!(skip_kind(&sqlx::Error::PoolTimedOut), None, "a pool timeout is not a lock skip");
        assert_eq!(SkipKind::Queue.as_str(), "queue_timeout");
        assert_eq!(SkipKind::Lock.as_str(), "lock_timeout");
        assert_eq!(LOCK_NOT_AVAILABLE, "55P03", "Postgres lock_not_available, what lock_timeout raises");
    }

    #[test]
    fn consecutive_skips_count_per_account_and_clear_on_success() {
        let skips = Skips::new();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(skips.consecutive(a), 0);
        assert_eq!(skips.record(a), 1);
        assert_eq!(skips.record(a), 2);
        assert_eq!(skips.record(b), 1, "accounts count separately");
        assert_eq!(skips.consecutive(a), 2);
        skips.clear(a);
        assert_eq!(skips.consecutive(a), 0, "a successful entry resets the run");
        assert_eq!(skips.consecutive(b), 1, "and only for that account");
        assert_eq!(skips.record(a), 1, "the next skip starts a new run");
    }

    #[test]
    fn escalation_starts_at_the_fifth_consecutive_skip() {
        assert_eq!(ESCALATE_AFTER, 5);
        for n in 1..ESCALATE_AFTER {
            assert!(!escalates(n), "skip {n} is a warn");
        }
        assert!(escalates(ESCALATE_AFTER), "the fifth is an alerting error");
        assert!(escalates(ESCALATE_AFTER + 1), "and it stays escalated until the account enters");
        assert_eq!(LOG_TARGET, "billing.lock_skip");
    }
}
