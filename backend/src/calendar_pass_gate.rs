//! Process-wide bound on calendar sync passes started by a request — the
//! Google push webhook (`routes::google_calendar_push`) and the manual
//! "sync now" route (`routes::connectors::trigger_sync`, Google and
//! Microsoft arms).
//!
//! # Why a bound is needed
//!
//! Every pass (`workers::google_calendar_sync::sync_personal_account` /
//! `sync_org_member`, `workers::microsoft_calendar_sync::sync_member` /
//! `sync_personal_account`) takes its per-connection
//! `pg_try_advisory_xact_lock` on a pooled connection and HOLDS that
//! connection for the whole pass — the lock is transaction-scoped, so the
//! transaction must stay open across the token refresh, the Google/Graph
//! round trips and the ingest — while every other step of the same pass
//! acquires a second connection from the same pool. The pool is the one
//! shared with every HTTP request (`DEFAULT_DB_MAX_CONNECTIONS`, 20) and
//! fails an acquire after 10 s. The sweeps run one pass at a time per
//! worker, so they pin one connection each; the request-initiated paths did
//! not: the push webhook spawned one unbounded task per notification, so a
//! burst across N distinct channels pinned N connections at once and, past
//! the pool size, every pass (and every request) sat on the acquire timeout
//! — the pinned connections could not finish without a second one.
//!
//! # Two bounds: the shared pool and each org's tenant pool
//!
//! Which pool a pass pins depends on the plane. The lock lives on the same
//! database as the other contender for the same connection: the control
//! pool (20) for Google (both planes) and for personal Microsoft
//! connections, but the org's TENANT request pool for a Microsoft org
//! member (`tenancy::registry::DEFAULT_MAX_CONNECTIONS`, 5, with a 5 s
//! acquire timeout) — and a Google org member's pass, whose lock is on the
//! control pool, still takes that tenant pool transiently for its ingest.
//! A bound sized off the control pool alone let four members of one org
//! click "sync now" at once and pin four of that org's five connections,
//! starving every other request for the org. So a pass on an org plane
//! takes TWO permits ([`CalendarPassGate::acquire_scoped`]): one from the
//! process-wide bound and a weighted share of a per-org budget sized from
//! that org's own pool ([`org_budget_for_pool`]: 2 units for a pool of 5).
//! The per-org semaphores live in a map keyed on the Clerk org id, created
//! on first use and dropped again once no pass holds one.
//!
//! # Per-org slots are weighted by tenant connections
//!
//! What the per-org bound protects is the org's tenant connections, and
//! the two providers use different amounts of them ([`OrgPassWeight`]):
//!
//! * a Microsoft org member's pass pins one tenant connection on its lock
//!   for its whole run and takes a second one briefly: 2 units;
//! * a Google pass (Calendar or Gmail) holds its lock on the CONTROL pool
//!   and only takes one tenant connection briefly, for its ingest: 1 unit.
//!
//! With a budget of 2 units for the default tenant pool of 5, one
//! Microsoft pass takes the whole budget, and two Google passes run side by
//! side. Before the weighting every pass counted as one of a single slot,
//! which serialized an org's Google passes for no connection it was short
//! of. Either way at most two of the org's five connections go to gated
//! passes. Units are taken FIFO (`Semaphore::acquire_many_owned`): a
//! Microsoft pass queued behind a Google one is not overtaken by a later
//! Google pass that would fit into the one free unit, so a steady stream of
//! Google work cannot starve it.
//!
//! # Why a semaphore here, not a dedicated lock pool
//!
//! There is no spare pool to take the lock on for a tenant without a second
//! pool per tenant. A dedicated pool of N connections is also just a
//! semaphore of N that costs N more server connections per machine;
//! bounding the passes in process gives the same ceiling for free and
//! bounds the transient second connection too. The sweeps are NOT gated:
//! they are already sequential (one pass per org at a time), and a push
//! burst must never be able to starve the backstop that catches up for
//! every pass skipped here.
//!
//! # Sizing and overflow
//!
//! [`CalendarPassGate::shared`] holds one permit per [`POOL_SHARE`] pooled
//! connections (`crate::db_max_connections()`), at least one and at most
//! [`MAX_PERMITS`]: 4 for the default pool of 20. A gated pass pins one
//! connection and takes one more transiently, so gated passes use at most
//! 8 of 20 at once, the two sequential sweeps up to 4 more, leaving the
//! rest for requests. Per org the budget is 2 units per [`POOL_SHARE`]
//! tenant connections (2 for the default pool of 5): at most two of its
//! connections go to gated passes (plus one per sequential sweep), see
//! "weighted" above. Over either bound a caller waits
//! ([`PUSH_WAIT`], [`ROUTE_WAIT`] — one deadline across both permits) and
//! then skips: a skipped push is caught by the next push for that channel
//! or by the 30-minute sweep (the push only decides WHEN a pass happens;
//! the cursor is untouched); a skipped manual sync answers `429` with
//! `Retry-After`, the same "already being done, try again shortly" the
//! route gives a `Busy` lock.
//!
//! # Gmail push shares both bounds
//!
//! The Gmail push pass (`routes::google_push::run_gmail_sync_for_mailbox`)
//! has the same shape — its per-mailbox `pg_try_advisory_xact_lock` pins a
//! CONTROL connection for the whole pass and an org member's ingest goes
//! through that org's tenant request pool — so it takes the same shared
//! instance and the same per-org budget (1 unit, as a Google pass): a
//! separate bound would add to this
//! one and break the ceilings above. It differs only on overflow: nothing
//! sweeps Gmail history, so a skipped Gmail push would wait for the
//! mailbox's next change. It therefore queues with no deadline
//! ([`CalendarPassGate::wait_scoped`]) instead of skipping, and a push for
//! a mailbox whose pass is already queued or running marks that pass to
//! diff once more when it ends.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How long a push-initiated pass waits for a permit before skipping.
/// Passes are short (one Calendar page and its ingest) so a burst drains
/// through four permits in well under this; a long backfill pass holding
/// a permit is exactly the case where skipping and letting the sweep catch
/// up is right.
pub const PUSH_WAIT: Duration = Duration::from_secs(30);
/// How long a manual "sync now" waits before answering `429` — short,
/// because a user is watching.
pub const ROUTE_WAIT: Duration = Duration::from_secs(5);
/// `Retry-After` on that `429`, in seconds.
pub const ROUTE_RETRY_AFTER_SECS: u64 = 5;
/// One permit per this many pooled connections.
const POOL_SHARE: u32 = 5;
/// Ceiling on permits however large the pool is configured.
const MAX_PERMITS: usize = 8;

/// The gate itself. [`shared`](Self::shared) is the production instance;
/// `new` exists for tests that need a gate of a known size.
pub struct CalendarPassGate {
    permits: Arc<Semaphore>,
    size: usize,
    /// One semaphore per org plane, keyed on the Clerk org id — see the
    /// module doc, "Two bounds".
    tenants: Mutex<HashMap<String, Arc<Semaphore>>>,
}

/// Held for the duration of one gated pass; releases its slot(s) on drop.
#[derive(Debug)]
pub struct PassPermit {
    _permit: OwnedSemaphorePermit,
    _tenant: Option<OwnedSemaphorePermit>,
}

/// How much of its org's per-org budget a pass takes: the tenant request
/// pool connections it uses at once (module doc, "weighted").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrgPassWeight {
    /// A Microsoft org member's calendar pass: its lock pins a tenant
    /// connection for the whole run, plus one used briefly.
    Microsoft,
    /// A Google pass (Calendar or Gmail): its lock is on the control pool;
    /// the tenant pool is used briefly, one connection, for ingest.
    Google,
}

impl OrgPassWeight {
    /// Units of the per-org budget this pass holds.
    pub const fn units(self) -> u32 {
        match self {
            OrgPassWeight::Microsoft => 2,
            OrgPassWeight::Google => 1,
        }
    }

    /// The heaviest pass: what one [`POOL_SHARE`] of a tenant pool buys.
    const MAX_UNITS: u32 = OrgPassWeight::Microsoft.units();
}

/// The per-org slot an org-plane pass takes alongside its process-wide
/// permit: which org, that org's tenant request pool (it sizes the budget),
/// and how heavy the pass is on that pool.
#[derive(Debug, Clone, Copy)]
pub struct OrgSlot<'a> {
    pub org_id: &'a str,
    pub pool: &'a PgPool,
    pub weight: OrgPassWeight,
}

impl<'a> OrgSlot<'a> {
    pub fn microsoft(org_id: &'a str, pool: &'a PgPool) -> Self {
        OrgSlot { org_id, pool, weight: OrgPassWeight::Microsoft }
    }

    pub fn google(org_id: &'a str, pool: &'a PgPool) -> Self {
        OrgSlot { org_id, pool, weight: OrgPassWeight::Google }
    }
}

/// Permits for a pool of `max_connections` — see the module doc, "Sizing".
pub(crate) fn permits_for_pool(max_connections: u32) -> usize {
    ((max_connections / POOL_SHARE) as usize).clamp(1, MAX_PERMITS)
}

/// An org's per-org budget in [`OrgPassWeight`] units: one heaviest pass
/// per [`POOL_SHARE`] connections of its tenant pool, by the same rule as
/// [`permits_for_pool`]. Never below [`OrgPassWeight::MAX_UNITS`], so every
/// pass fits the budget it waits on (a weight over the budget would queue
/// forever).
pub(crate) fn org_budget_for_pool(max_connections: u32) -> usize {
    permits_for_pool(max_connections) * OrgPassWeight::MAX_UNITS as usize
}

impl CalendarPassGate {
    pub fn new(size: usize) -> Self {
        let size = size.max(1);
        CalendarPassGate { permits: Arc::new(Semaphore::new(size)), size, tenants: Mutex::new(HashMap::new()) }
    }

    /// The process-wide gate, sized from the configured pool on first use
    /// (same `OnceLock` shape as `graph_rate_limit::shared`).
    pub fn shared() -> &'static CalendarPassGate {
        static INSTANCE: OnceLock<CalendarPassGate> = OnceLock::new();
        INSTANCE.get_or_init(|| CalendarPassGate::new(permits_for_pool(crate::db_max_connections())))
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn available(&self) -> usize {
        self.permits.available_permits()
    }

    /// How many process-wide permits are held right now.
    pub fn in_flight(&self) -> usize {
        self.size - self.available()
    }

    /// A permit, waiting up to `wait` for one to free up (FIFO); `None`
    /// means the caller should skip — see the module doc, "overflow". For a
    /// pass that touches no org tenant pool (personal plane).
    pub async fn acquire(&self, wait: Duration) -> Option<PassPermit> {
        self.acquire_scoped(None, wait).await
    }

    /// [`acquire`](Self::acquire) for a pass that also uses an org's tenant
    /// pool: `tenant` names the org, its pool and the pass's weight, and the
    /// permit carries that many units of the org's own budget (sized from
    /// the pool's `max_connections`) as well as the process-wide slot. The
    /// org units are taken first and `wait` is one deadline across both, so
    /// a caller queued behind its own org never holds a process-wide slot
    /// while it waits.
    pub async fn acquire_scoped(&self, tenant: Option<OrgSlot<'_>>, wait: Duration) -> Option<PassPermit> {
        // Timing out drops the half-acquired future, releasing an org slot
        // it already took.
        tokio::time::timeout(wait, self.wait_scoped(tenant)).await.ok()
    }

    /// [`acquire_scoped`](Self::acquire_scoped) with no deadline: queue
    /// (FIFO) until both slots are free. For a pass nothing else catches up
    /// if it is skipped — the Gmail push (`routes::google_push`, "Pass
    /// bound"). A waiter holds no connection, only its place in the queue
    /// (and, once taken, the org units while it waits for the shared slot).
    pub async fn wait_scoped(&self, tenant: Option<OrgSlot<'_>>) -> PassPermit {
        // Org units BEFORE the process-wide slot, always: a caller holding a
        // shared slot while it queued on its org would let one busy org
        // starve every other org of shared slots.
        let tenant_permit = match tenant {
            Some(slot) => {
                let sem = self.tenant_semaphore(slot.org_id, slot.pool.options().get_max_connections());
                Some(
                    sem.acquire_many_owned(slot.weight.units())
                        .await
                        .expect("pass gate semaphores are never closed"),
                )
            }
            None => None,
        };
        let permit = self.permits.clone().acquire_owned().await.expect("pass gate semaphores are never closed");
        PassPermit { _permit: permit, _tenant: tenant_permit }
    }

    /// The org's semaphore, created on first use from its pool size. Entries
    /// no pass holds (the map's own `Arc` is the only reference — a held
    /// `OwnedSemaphorePermit` keeps a clone) are pruned on the way, so the
    /// map stays the size of the orgs currently syncing.
    fn tenant_semaphore(&self, org_id: &str, max_connections: u32) -> Arc<Semaphore> {
        let mut map = self.tenants.lock().unwrap_or_else(|p| p.into_inner());
        map.retain(|key, sem| key == org_id || Arc::strong_count(sem) > 1);
        map.entry(org_id.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(org_budget_for_pool(max_connections))))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permits_track_the_pool_size_within_bounds() {
        assert_eq!(permits_for_pool(20), 4, "the default pool");
        assert_eq!(permits_for_pool(10), 2, "sqlx's own default");
        assert_eq!(permits_for_pool(4), 1, "never zero");
        assert_eq!(permits_for_pool(1), 1);
        assert_eq!(permits_for_pool(100), 8, "the operator ceiling still leaves most of the pool to requests");
    }

    /// Over the bound a caller waits for a slot and, failing that, gets
    /// `None` — never a permit the gate did not have.
    #[tokio::test]
    async fn acquire_waits_then_skips_once_the_gate_is_full() {
        let gate = CalendarPassGate::new(2);
        let a = gate.acquire(Duration::ZERO).await.expect("first permit");
        let b = gate.acquire(Duration::ZERO).await.expect("second permit");
        assert_eq!(gate.available(), 0);
        assert!(gate.acquire(Duration::from_millis(20)).await.is_none(), "full: skip after the wait");

        drop(a);
        let c = gate.acquire(Duration::ZERO).await.expect("a released slot is reusable");
        assert_eq!(gate.available(), 0);

        // A waiter gets the slot the moment one frees up, within its wait.
        let gate2 = CalendarPassGate::new(1);
        let held = gate2.acquire(Duration::ZERO).await.unwrap();
        let waiter = tokio::spawn({
            let permits = gate2.permits.clone();
            async move { tokio::time::timeout(Duration::from_secs(2), permits.acquire_owned()).await.is_ok() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(held);
        assert!(waiter.await.unwrap(), "the waiter was handed the released slot");
        drop((b, c));
    }

    fn lazy_pool(max_connections: u32) -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_lazy("postgres://localhost/unused")
            .expect("a lazy pool never connects")
    }

    /// The per-org bound, Microsoft: a pass pins a tenant connection for
    /// its whole run, so a tenant pool of 5 gets one at a time. A second
    /// "sync now" in the same org is skipped while the process-wide gate
    /// still has room, and a different org is not held up by it.
    #[tokio::test]
    async fn an_org_plane_gets_one_microsoft_pass_per_tenant_pool_of_five() {
        let gate = CalendarPassGate::new(4);
        let org_a = lazy_pool(5);
        let org_b = lazy_pool(5);
        let first = gate.acquire_scoped(Some(OrgSlot::microsoft("org_a", &org_a)), Duration::ZERO).await.expect("first pass in org_a");
        assert!(
            gate.acquire_scoped(Some(OrgSlot::microsoft("org_a", &org_a)), Duration::from_millis(20)).await.is_none(),
            "a second org_a pass would pin a second of its five connections"
        );
        assert!(
            gate.acquire_scoped(Some(OrgSlot::google("org_a", &org_a)), Duration::from_millis(20)).await.is_none(),
            "a Microsoft pass takes the org's whole budget"
        );
        assert_eq!(gate.in_flight(), 1, "the skipped callers hold no process-wide slot");
        let other = gate.acquire_scoped(Some(OrgSlot::microsoft("org_b", &org_b)), Duration::ZERO).await.expect("org_b is independent");
        let personal = gate.acquire(Duration::ZERO).await.expect("personal passes only take the shared bound");
        assert_eq!(gate.in_flight(), 3);

        drop(first);
        let again = gate.acquire_scoped(Some(OrgSlot::microsoft("org_a", &org_a)), Duration::ZERO).await.expect("a freed org slot is reusable");
        drop((other, personal, again));
        assert_eq!(gate.in_flight(), 0);
        // Idle entries are pruned the next time any org takes a slot.
        let _c = gate.acquire_scoped(Some(OrgSlot::google("org_c", &lazy_pool(5))), Duration::ZERO).await.unwrap();
        assert_eq!(gate.tenants.lock().unwrap().len(), 1);
    }

    /// The per-org bound, Google: a pass only uses a tenant connection
    /// briefly, so two run side by side in a pool of 5 (2 units of 2), a
    /// third waits, and a Microsoft pass waits until both are done. Once a
    /// Microsoft pass holds the budget, a Google pass waits for it.
    #[tokio::test]
    async fn two_google_passes_share_an_orgs_budget_that_one_microsoft_pass_fills() {
        let gate = CalendarPassGate::new(4);
        let org = lazy_pool(5);
        let g1 = gate.acquire_scoped(Some(OrgSlot::google("org_g", &org)), Duration::ZERO).await.expect("first Google pass");
        let g2 = gate
            .acquire_scoped(Some(OrgSlot::google("org_g", &org)), Duration::ZERO)
            .await
            .expect("a second Google pass in the same org runs alongside the first");
        assert!(gate.acquire_scoped(Some(OrgSlot::google("org_g", &org)), Duration::from_millis(20)).await.is_none(), "a third would exceed the budget");
        assert!(
            gate.acquire_scoped(Some(OrgSlot::microsoft("org_g", &org)), Duration::from_millis(20)).await.is_none(),
            "a Microsoft pass needs the whole budget"
        );
        drop(g1);
        assert!(
            gate.acquire_scoped(Some(OrgSlot::microsoft("org_g", &org)), Duration::from_millis(20)).await.is_none(),
            "one unit free is not enough for a Microsoft pass"
        );
        drop(g2);
        let ms = gate.acquire_scoped(Some(OrgSlot::microsoft("org_g", &org)), Duration::ZERO).await.expect("both units free");
        assert!(gate.acquire_scoped(Some(OrgSlot::google("org_g", &org)), Duration::from_millis(20)).await.is_none(), "the Microsoft pass holds the budget");
        assert_eq!(gate.in_flight(), 1);
        drop(ms);
        assert_eq!(gate.in_flight(), 0);
    }

    #[tokio::test]
    async fn a_queued_microsoft_pass_is_not_overtaken_by_a_later_google_pass() {
        let gate = CalendarPassGate::new(4);
        let org = lazy_pool(5);
        let running = gate.acquire_scoped(Some(OrgSlot::google("org_f", &org)), Duration::ZERO).await.unwrap();
        let ms = gate.wait_scoped(Some(OrgSlot::microsoft("org_f", &org)));
        tokio::pin!(ms);
        tokio::select! {
            biased;
            _ = &mut ms => panic!("Microsoft requires both org units"),
            _ = std::future::ready(()) => {}
        }
        assert!(
            gate.acquire_scoped(Some(OrgSlot::google("org_f", &org)), Duration::from_millis(20)).await.is_none(),
            "the later Google pass queues behind the Microsoft one"
        );
        drop(running);
        let ms_permit = tokio::time::timeout(Duration::from_secs(2), ms).await.expect("handed the whole budget");
        assert_eq!(gate.in_flight(), 1);
        drop(ms_permit);
    }

    #[tokio::test]
    async fn canceling_a_weighted_waiter_releases_its_reserved_org_units() {
        let gate = CalendarPassGate::new(4);
        let org = lazy_pool(5);
        let running = gate.acquire_scoped(Some(OrgSlot::google("org_cancel", &org)), Duration::ZERO).await.unwrap();
        {
            let ms = gate.wait_scoped(Some(OrgSlot::microsoft("org_cancel", &org)));
            tokio::pin!(ms);
            tokio::select! {
                biased;
                _ = &mut ms => panic!("Microsoft requires both org units"),
                _ = std::future::ready(()) => {}
            }
            assert_eq!(gate.in_flight(), 1, "org waiters take no shared slot");
        }
        let next = gate.acquire_scoped(Some(OrgSlot::google("org_cancel", &org)), Duration::ZERO).await
            .expect("cancellation returns the unit reserved by the weighted waiter");
        drop((running, next));
        assert_eq!(gate.in_flight(), 0);
        let ms = gate.acquire_scoped(Some(OrgSlot::microsoft("org_cancel", &org)), Duration::ZERO).await
            .expect("both units are returned after pass drop");
        drop(ms);
    }

    #[tokio::test]
    async fn a_shared_gate_timeout_releases_the_whole_weighted_org_budget() {
        let gate = CalendarPassGate::new(1);
        let org = lazy_pool(5);
        let shared = gate.acquire(Duration::ZERO).await.unwrap();
        assert!(gate.acquire_scoped(Some(OrgSlot::microsoft("org_timeout", &org)), Duration::from_millis(20)).await.is_none());
        assert_eq!(gate.in_flight(), 1, "the shared holder is the only live pass");
        drop(shared);
        let ms = gate.acquire_scoped(Some(OrgSlot::microsoft("org_timeout", &org)), Duration::ZERO).await
            .expect("the timed-out future returned both org units");
        drop(ms);
        assert_eq!(gate.in_flight(), 0);
    }

    #[test]
    fn every_pass_fits_the_smallest_org_budget() {
        assert_eq!(org_budget_for_pool(5), 2, "the default tenant pool: one Microsoft pass or two Google passes");
        assert_eq!(org_budget_for_pool(10), 4);
        for max in [0, 1, 4] {
            assert!(org_budget_for_pool(max) >= OrgPassWeight::Microsoft.units() as usize, "pool {max}");
        }
        assert!(OrgPassWeight::Google.units() <= OrgPassWeight::MAX_UNITS);
    }

    /// The no-deadline form (Gmail push): it never gives up, holds no
    /// process-wide slot while queued behind its own org, and is handed the
    /// org slot the moment the holder releases it.
    #[tokio::test]
    async fn wait_scoped_queues_behind_the_org_bound_instead_of_skipping() {
        let gate = Arc::new(CalendarPassGate::new(4));
        let org = lazy_pool(5);
        let held = gate.acquire_scoped(Some(OrgSlot::microsoft("org_w", &org)), Duration::ZERO).await.unwrap();
        let waiter = tokio::spawn({
            let (gate, org) = (gate.clone(), org.clone());
            async move { gate.wait_scoped(Some(OrgSlot::google("org_w", &org))).await }
        });
        // Far past every skip deadline the request paths use.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "a full org bound queues the waiter, never skips it");
        assert_eq!(gate.in_flight(), 1, "the queued waiter holds no process-wide slot");
        drop(held);
        let permit = tokio::time::timeout(Duration::from_secs(2), waiter).await.expect("handed the freed slot").unwrap();
        assert_eq!(gate.in_flight(), 1);
        drop(permit);
        assert_eq!(gate.in_flight(), 0);
    }

    /// A larger tenant pool gets proportionally more (same rule as the
    /// shared pool).
    #[tokio::test]
    async fn the_org_bound_is_sized_from_that_orgs_pool() {
        let gate = CalendarPassGate::new(8);
        let big = lazy_pool(10);
        let a = gate.acquire_scoped(Some(OrgSlot::microsoft("org_big", &big)), Duration::ZERO).await.unwrap();
        let b = gate.acquire_scoped(Some(OrgSlot::microsoft("org_big", &big)), Duration::ZERO).await.unwrap();
        assert!(gate.acquire_scoped(Some(OrgSlot::microsoft("org_big", &big)), Duration::from_millis(10)).await.is_none());
        drop((a, b));
    }
}
