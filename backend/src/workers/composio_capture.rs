//! Deployment-gated polling of every connected Composio capture. Each tick
//! first finishes pending connections (the member may have completed sign-in
//! after the web page stopped watching), then polls due captures for
//! workspace-wide changes since their watermark; the per-capture interval
//! (5/15/60 minutes) lives in `next_poll_at`, so this tick only decides who is
//! due.
//!
//! The harness ticks every plane from one serial loop, so a tick never waits
//! on a broker call. It starts a round on its own task and returns. A round
//! keeps up to `concurrency` polls in flight for its plane and refills from
//! the due query until `round_budget` is spent or nothing is due. Every
//! broker call, on every plane, draws from one [`CallBudget`].
//!
//! A capture whose member left the account is parked and its connected
//! account revoked, as Disconnect does. A revocation that fails is retried by
//! later rounds with backoff, at most [`REVOKES_PER_ROUND`] per round in one
//! poll slot.
use crate::{
    connectors::composio::{budget::CallBudget, recipe, Client, Recipe},
    middleware::auth::UserContext,
    shutdown::Shutdown,
    tenancy::{
        extractor::Tenant,
        runtime::{TenancyDecision, TenantContext},
        TenantId,
    },
    workers::tenant_harness::{Tick, WorkerTenant},
    AppState,
};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};
use uuid::Uuid;
/// Discovery cadence only; well under the 5-minute fastest capture interval so
/// a capture runs close to its `next_poll_at`.
pub const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Accelerate discovery only in the debug e2e stack; production scheduling and
/// provider capture intervals retain their defaults.
pub fn poll_interval() -> Duration {
    #[cfg(feature = "e2e")]
    if let Some(ms) = std::env::var("E2E_COMPOSIO_POLL_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    {
        return Duration::from_millis(ms);
    }
    POLL_INTERVAL
}
/// Pending connections checked per tenant per tick. A still-unfinished sign-in
/// costs one Composio read; the resolver skips any checked in the last 30s.
pub const PENDING_PER_TICK: i32 = 5;
/// Departed members' revocations retried per plane per round, at most. The
/// first failure ends the round's retries: Composio is likely failing them all.
pub const REVOKES_PER_ROUND: i32 = 5;
/// Polls in flight per plane. Throughput is linear in this up to at least 32
/// (broker latency bound), and 8 polls about 120 a minute at the bench's
/// realistic mix: 1.8x the 1,000-connection target. See
/// `docs/connectors/composio-scale-and-cost.md`.
pub const DEFAULT_CONCURRENCY: usize = 8;
/// How long a round keeps starting polls. Equal to the tick cadence, so rounds
/// run back to back while a plane has a backlog.
pub const ROUND_BUDGET: Duration = POLL_INTERVAL;
const POLL_TIMEOUT: Duration = Duration::from_secs(210);
/// Recheck delay while this plane's previous round is still starting polls.
const BUSY_RECHECK: Duration = Duration::from_secs(1);
/// Due rows that are already in flight have not taken their lease yet; wait
/// this long for them before asking again.
const LEASE_SETTLE: Duration = Duration::from_millis(100);

type Due = (Uuid, Uuid, Uuid, String);
type Pending = (Uuid, Uuid, Uuid, String);
type Departed = (Uuid, Uuid, Uuid, DateTime<Utc>);

pub struct Worker {
    budget: Arc<CallBudget>,
    concurrency: usize,
    round_budget: Duration,
    shutdown: Shutdown,
    planes: Mutex<HashMap<TenantId, Arc<Plane>>>,
}
struct Plane {
    slots: Arc<Semaphore>,
    starting: AtomicBool,
    in_flight: Mutex<HashSet<Uuid>>,
}
/// Clears `starting` however the round ends, including a panic.
struct Starting(Arc<Plane>);
impl Drop for Starting {
    fn drop(&mut self) {
        self.0.starting.store(false, Ordering::Release);
    }
}
struct InFlight(Arc<Plane>, Uuid);
impl Drop for InFlight {
    fn drop(&mut self) {
        lock(&self.0.in_flight).remove(&self.1);
    }
}
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Worker {
    pub fn new(budget: Arc<CallBudget>, concurrency: usize, round_budget: Duration, shutdown: Shutdown) -> Arc<Self> {
        Arc::new(Self {
            budget,
            concurrency: concurrency.max(1),
            round_budget,
            shutdown,
            planes: Mutex::default(),
        })
    }
    /// `COMPOSIO_POLL_CONCURRENCY` and `COMPOSIO_CALLS_PER_MINUTE`, else the
    /// defaults.
    pub fn from_env(shutdown: Shutdown) -> Arc<Self> {
        let concurrency = std::env::var("COMPOSIO_POLL_CONCURRENCY")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_CONCURRENCY);
        Self::new(Arc::new(CallBudget::from_env()), concurrency, ROUND_BUDGET, shutdown)
    }
    pub async fn tick(
        self: &Arc<Self>,
        t: &WorkerTenant,
        control: &PgPool,
        state: &Arc<AppState>,
    ) -> Result<Tick, String> {
        if std::env::var("COMPOSIO_CAPTURE_POLLING_ENABLED").as_deref() != Ok("true") {
            return Ok(Tick::idle());
        }
        self.tick_with(t, control, state, Client::configured).await
    }
    /// One tick as `tick` runs it once polling is enabled, with the broker
    /// client built only when there is work.
    #[doc(hidden)]
    pub async fn tick_with(
        self: &Arc<Self>,
        t: &WorkerTenant,
        control: &PgPool,
        state: &Arc<AppState>,
        client: impl FnOnce() -> Result<Client, crate::connectors::composio::Error>,
    ) -> Result<Tick, String> {
        let plane = self.plane(&t.tenant);
        if plane.starting.load(Ordering::Acquire) {
            return Ok(Tick::until_next_work(0, BUSY_RECHECK));
        }
        let pending = pending_batch(t, control).await?;
        let departed = departed_batch(t, control).await?;
        let due = due_batch(&t.pool).await?;
        if pending.is_empty() && departed.is_empty() && due.is_empty() {
            return Ok(Tick::idle());
        }
        let client = client()
            .map_err(|_| "composio polling enabled without COMPOSIO_API_KEY".to_string())?
            .with_budget(self.budget.clone());
        if plane.starting.swap(true, Ordering::AcqRel) {
            return Ok(Tick::until_next_work(0, BUSY_RECHECK));
        }
        let found = pending.len() + departed.len() + due.len();
        let round = Round {
            worker: self.clone(),
            plane: plane.clone(),
            t: t.clone(),
            control: control.clone(),
            state: state.clone(),
            client,
            deadline: Instant::now() + self.round_budget,
        };
        tokio::spawn(round.run(pending, departed, due.into()));
        Ok(Tick::worked(found))
    }
    /// Waits until no round is starting polls and no poll is in flight.
    #[doc(hidden)]
    pub async fn settle(&self) {
        loop {
            let busy = lock(&self.planes).values().any(|p| {
                p.starting.load(Ordering::Acquire) || p.slots.available_permits() < self.concurrency
            });
            if !busy {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    fn plane(&self, tenant: &TenantId) -> Arc<Plane> {
        lock(&self.planes)
            .entry(tenant.clone())
            .or_insert_with(|| {
                Arc::new(Plane {
                    slots: Arc::new(Semaphore::new(self.concurrency)),
                    starting: AtomicBool::new(false),
                    in_flight: Mutex::default(),
                })
            })
            .clone()
    }
}

struct Round {
    worker: Arc<Worker>,
    plane: Arc<Plane>,
    t: WorkerTenant,
    control: PgPool,
    state: Arc<AppState>,
    client: Client,
    deadline: Instant,
}
impl Round {
    async fn run(self, pending: Vec<Pending>, departed: Vec<Departed>, mut due: VecDeque<Due>) {
        let _starting = Starting(self.plane.clone());
        // In a poll slot of its own, so retries never hold up this round's
        // polls; with every slot still busy they wait for the next round.
        if !departed.is_empty() {
            if let Ok(slot) = self.plane.slots.clone().try_acquire_owned() {
                let (t, control, client) = (self.t.clone(), self.control.clone(), self.client.clone());
                tokio::spawn(async move {
                    let _slot = slot;
                    retry_revocations(&t, &control, &client, departed).await;
                });
            }
        }
        if let Err(e) = complete_pending(&self.t, &self.control, &self.state, &self.client, pending).await {
            tracing::warn!(tenant = ?self.t.tenant, error = %e, "composio pending sweep failed");
        }
        // A capture is polled at most once per round, so one that never takes
        // its lease cannot be retried in a loop.
        let mut tried = HashSet::new();
        loop {
            if self.worker.shutdown.is_triggered() {
                return;
            }
            let Ok(Ok(slot)) = tokio::time::timeout_at(self.deadline, self.plane.slots.clone().acquire_owned()).await else {
                return;
            };
            let Some((id, account, user, recipe)) = self.next(&mut due, &mut tried).await else {
                return;
            };
            let flight = InFlight(self.plane.clone(), id);
            let (t, control, state, client) = (self.t.clone(), self.control.clone(), self.state.clone(), self.client.clone());
            tokio::spawn(async move {
                let _slot = slot;
                let _flight = flight;
                match member_scope(&t, &control, account, user).await {
                    Ok(Scope::Member(tenant, caller)) => {
                        let _ = tokio::time::timeout(
                            POLL_TIMEOUT,
                            crate::routes::composio::poll(&state, &tenant, &caller, recipe, id, &client),
                        )
                        .await;
                    }
                    Ok(gone) => {
                        if let Err(e) = park(&t, &control, &client, id, account, user, matches!(gone, Scope::Left)).await {
                            tracing::warn!(tenant = ?t.tenant, error = %e, "composio orphaned capture not parked");
                        }
                    }
                    Err(e) => tracing::warn!(tenant = ?t.tenant, error = %e, "composio poll scope unavailable"),
                }
            });
        }
    }
    /// The next due capture not already in flight, claimed for this round, or
    /// `None` once nothing else is due before the deadline.
    async fn next(&self, due: &mut VecDeque<Due>, tried: &mut HashSet<Uuid>) -> Option<(Uuid, Uuid, Uuid, &'static Recipe)> {
        loop {
            while let Some((id, account, user, service)) = due.pop_front() {
                if tried.contains(&id) {
                    continue;
                }
                let Some(recipe) = recipe::get(&service) else {
                    tried.insert(id);
                    if let Err(e) = defer_retired(&self.t.pool, id, account, user, &service).await {
                        tracing::warn!(tenant = ?self.t.tenant, error = %e, "composio capture without a recipe not deferred");
                    }
                    continue;
                };
                if lock(&self.plane.in_flight).insert(id) {
                    tried.insert(id);
                    return Some((id, account, user, recipe));
                }
            }
            if Instant::now() >= self.deadline || self.worker.shutdown.is_triggered() {
                return None;
            }
            let rows = match due_batch(&self.t.pool).await {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(tenant = ?self.t.tenant, error = %e, "composio due-scope discovery failed");
                    return None;
                }
            };
            let waiting = {
                let in_flight = lock(&self.plane.in_flight);
                let waiting = rows.iter().any(|r| in_flight.contains(&r.0));
                due.extend(rows.into_iter().filter(|r| !in_flight.contains(&r.0) && !tried.contains(&r.0)));
                waiting
            };
            if due.is_empty() {
                if !waiting {
                    return None;
                }
                tokio::time::sleep(LEASE_SETTLE).await;
            }
        }
    }
}

/// Parks a due capture that can never take its lease again, so it stops being
/// due and cannot crowd live captures out of the due query's `LIMIT 10`
/// (#1258). Its member left the account (`left`: the connection is
/// disconnected and its connected account revoked, as Disconnect does), or its
/// account is no longer on this plane (the connection may serve the new plane,
/// so it is left alone). Atoms and the row stay; connecting again starts
/// afresh. A failed revocation is retried by [`retry_revocations`].
async fn park(t: &WorkerTenant, control: &PgPool, client: &Client, id: Uuid, account: Uuid, user: Uuid, left: bool) -> Result<(), String> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(&t.pool, account, user)
        .await
        .map_err(|_| "composio park scope unavailable".to_string())?;
    let connection = sqlx::query_scalar::<_, Uuid>("SELECT connection_id FROM composio_capture_scopes WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND state IN ('active','retry')")
        .bind(id).bind(account).bind(user)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| "composio park lookup failed".to_string())?;
    tx.commit().await.map_err(|_| "composio park lookup commit failed".to_string())?;
    let Some(connection) = connection else {
        return Ok(());
    };
    // The connection first: if parking the capture then fails, it stays due
    // and the next round repeats both. Only the park that disconnects it
    // revokes; the others leave a failed revocation to the retries.
    let mut revoke = None;
    if left {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
            .await
            .map_err(|_| "composio park connection scope unavailable".to_string())?;
        revoke = sqlx::query_scalar::<_, Option<String>>("UPDATE composio_connections SET status='disconnected',disconnected_at=COALESCE(disconnected_at,now()),updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND status<>'disconnected' RETURNING CASE WHEN revoked_at IS NULL THEN connected_account_id END")
            .bind(connection).bind(account).bind(user)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| "composio park connection failed".to_string())?
            .flatten();
        tx.commit().await.map_err(|_| "composio park connection commit failed".to_string())?;
    }
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(&t.pool, account, user)
        .await
        .map_err(|_| "composio park scope unavailable".to_string())?;
    sqlx::query("UPDATE composio_capture_scopes SET state='disconnected',updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND state IN ('active','retry')")
        .bind(id).bind(account).bind(user)
        .execute(&mut *tx)
        .await
        .map_err(|_| "composio park failed".to_string())?;
    tx.commit().await.map_err(|_| "composio park commit failed".to_string())?;
    tracing::info!(tenant = ?t.tenant, capture_id = %id, account_id = %account, member_left = left, "composio capture parked as disconnected");
    if let Some(remote) = revoke {
        revoke_departed(t, control, client, connection, account, user, &remote).await;
    }
    Ok(())
}
pub(crate) enum RevokeFailed {
    /// Composio did not confirm it; nothing is recorded.
    Provider(crate::connectors::composio::Error),
    /// Composio revoked it but `revoked_at` was not recorded. The next attempt
    /// meets Composio's 404, which counts as revoked, and records it.
    Store,
}
/// Revokes a connection's Composio connected account (and its provider
/// credentials) and records `revoked_at`. Disconnect, and the capture worker
/// for a member who left, both revoke through here.
pub(crate) async fn revoke_connection(
    control: &PgPool,
    client: &Client,
    account: Uuid,
    user: Uuid,
    id: Uuid,
    connected_account: &str,
) -> Result<(), RevokeFailed> {
    client.revoke(connected_account).await.map_err(RevokeFailed::Provider)?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|_| RevokeFailed::Store)?;
    sqlx::query("UPDATE composio_connections SET revoked_at=COALESCE(revoked_at,now()),updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3")
        .bind(id).bind(account).bind(user)
        .execute(&mut *tx)
        .await
        .map_err(|_| RevokeFailed::Store)?;
    tx.commit().await.map_err(|_| RevokeFailed::Store)
}
/// Revokes a departed member's connected account the way Disconnect does.
/// Whether it worked, so a failure can end a round's retries.
async fn revoke_departed(t: &WorkerTenant, control: &PgPool, client: &Client, connection: Uuid, account: Uuid, user: Uuid, remote: &str) -> bool {
    match revoke_connection(control, client, account, user, connection, remote).await {
        Ok(()) => {
            tracing::info!(tenant = ?t.tenant, connection_id = %connection, account_id = %account, "composio departed member's connected account revoked");
            true
        }
        Err(_) => {
            tracing::warn!(tenant = ?t.tenant, connection_id = %connection, account_id = %account, "composio departed member's connected account not revoked; retrying later");
            false
        }
    }
}
/// Retries departed members' revocations that have not succeeded yet. Each
/// attempt first moves the connection's `updated_at` (the backoff clock) to
/// now, so a concurrent round or a member who came back skips it.
async fn retry_revocations(t: &WorkerTenant, control: &PgPool, client: &Client, departed: Vec<Departed>) {
    for (connection, account, user, seen) in departed {
        let claimed = async {
            let mut tx = crate::tenancy::scope::begin_account_scoped(control, account).await.ok()?;
            let remote = sqlx::query_scalar::<_, String>("UPDATE composio_connections SET updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND status='disconnected' AND revoked_at IS NULL AND updated_at=$4 AND NOT EXISTS(SELECT 1 FROM account_members WHERE account_id=$2 AND user_id=$3) RETURNING connected_account_id")
                .bind(connection).bind(account).bind(user).bind(seen)
                .fetch_optional(&mut *tx)
                .await
                .ok()?;
            tx.commit().await.ok()?;
            Some(remote)
        }
        .await;
        match claimed {
            Some(Some(remote)) => {
                if !revoke_departed(t, control, client, connection, account, user, &remote).await {
                    return;
                }
            }
            Some(None) => {}
            None => {
                tracing::warn!(tenant = ?t.tenant, connection_id = %connection, "composio revocation retry could not be claimed");
                return;
            }
        }
    }
}
/// Pushes a due capture whose recipe is no longer in this build an hour
/// forward. It keeps its state, so a restored recipe resumes it.
async fn defer_retired(pool: &PgPool, id: Uuid, account: Uuid, user: Uuid, service: &str) -> Result<(), String> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account, user)
        .await
        .map_err(|_| "composio defer scope unavailable".to_string())?;
    sqlx::query("UPDATE composio_capture_scopes SET next_poll_at=now()+interval '1 hour',updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND service=$4 AND state IN ('active','retry') AND next_poll_at<=now()")
        .bind(id).bind(account).bind(user).bind(service)
        .execute(&mut *tx)
        .await
        .map_err(|_| "composio defer failed".to_string())?;
    tx.commit().await.map_err(|_| "composio defer commit failed".to_string())
}

async fn due_batch(pool: &PgPool) -> Result<Vec<Due>, String> {
    sqlx::query_as("SELECT id,account_id,owner_user_id,service FROM resolve_composio_capture_due()")
        .fetch_all(pool)
        .await
        .map_err(|_| "composio due-scope discovery failed".to_string())
}
/// One bounded pass over this tenant's pending connections, as the worker
/// tick runs it. Returns how many became active.
#[doc(hidden)]
pub async fn sweep_pending(
    t: &WorkerTenant,
    control: &PgPool,
    state: &Arc<AppState>,
    client: &Client,
) -> Result<usize, String> {
    let pending = pending_batch(t, control).await?;
    complete_pending(t, control, state, client, pending).await
}

fn plane_org(t: &WorkerTenant) -> Option<&str> {
    match &t.tenant {
        TenantId::Consumer => None,
        TenantId::Org(org) => Some(org.as_str()),
    }
}
/// This plane's departed members' connections whose revocation is due a retry.
async fn departed_batch(t: &WorkerTenant, control: &PgPool) -> Result<Vec<Departed>, String> {
    sqlx::query_as("SELECT id,account_id,owner_user_id,updated_at FROM resolve_composio_departed_unrevoked($1,$2)")
        .bind(plane_org(t))
        .bind(REVOKES_PER_ROUND)
        .fetch_all(control)
        .await
        .map_err(|_| "composio departed-connection discovery failed".to_string())
}
async fn pending_batch(t: &WorkerTenant, control: &PgPool) -> Result<Vec<Pending>, String> {
    let org = plane_org(t);
    sqlx::query_as(
        "SELECT id,account_id,owner_user_id,service FROM resolve_composio_pending_connections($1,$2)",
    )
    .bind(org)
    .bind(PENDING_PER_TICK)
    .fetch_all(control)
    .await
    .map_err(|_| "composio pending-connection discovery failed".to_string())
}
/// Runs the verify route's logic for each pending connection, then settles it:
/// still pending after an hour (or its member gone) means the sign-in was
/// abandoned, so it moves to `reconnect_needed` and is never checked again.
async fn complete_pending(
    t: &WorkerTenant,
    control: &PgPool,
    state: &Arc<AppState>,
    client: &Client,
    pending: Vec<Pending>,
) -> Result<usize, String> {
    let mut activated = 0;
    for (id, account, user, service) in pending {
        let mut rate_limited = false;
        let scope = match recipe::get(&service) {
            Some(r) => match member_scope(t, control, account, user).await? {
                Scope::Member(tenant, caller) => Some((r, (tenant, caller))),
                Scope::Left | Scope::OffPlane => None,
            },
            None => None,
        };
        if let Some((recipe, (tenant, caller))) = scope {
            let response = tokio::time::timeout(
                Duration::from_secs(210),
                crate::routes::composio::verify_with(state, &tenant, &caller, recipe, id, client),
            )
            .await;
            match response {
                Ok(r) if r.status().is_success() => activated += 1,
                Ok(r) => rate_limited = r.status() == StatusCode::TOO_MANY_REQUESTS,
                Err(_) => {}
            }
        }
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
            .await
            .map_err(|_| "composio pending scope unavailable".to_string())?;
        sqlx::query("UPDATE composio_connections SET status=CASE WHEN created_at<now()-interval '1 hour' THEN 'reconnect_needed' ELSE status END,updated_at=now() WHERE id=$1 AND account_id=$2 AND status='pending'")
            .bind(id)
            .bind(account)
            .execute(&mut *tx)
            .await
            .map_err(|_| "composio pending settle failed".to_string())?;
        tx.commit()
            .await
            .map_err(|_| "composio pending settle commit failed".to_string())?;
        if rate_limited {
            break;
        }
    }
    Ok(activated)
}
enum Scope {
    Member(Tenant, UserContext),
    /// The member is no longer in the account.
    Left,
    /// The account is not on this tenant's plane.
    OffPlane,
}
/// The member's current role and request scope for work on their behalf, or
/// why there is none. Membership is checked on every use, including after
/// reconnect.
async fn member_scope(
    t: &WorkerTenant,
    control: &PgPool,
    account: Uuid,
    user: Uuid,
) -> Result<Scope, String> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|_| "composio membership scope unavailable".to_string())?;
    let role = sqlx::query_scalar::<_, String>(
        "SELECT role FROM account_members WHERE account_id=$1 AND user_id=$2",
    )
    .bind(account)
    .bind(user)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|_| "composio membership unavailable".to_string())?;
    let plane_ok = match &t.tenant {
        TenantId::Consumer => {
            sqlx::query_scalar::<_, bool>(
                "SELECT NOT EXISTS(SELECT 1 FROM tenant_databases WHERE account_id=$1)",
            )
            .bind(account)
            .fetch_one(&mut *tx)
            .await
        }
        TenantId::Org(org) => {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1 AND clerk_org_id=$2)",
            )
            .bind(account)
            .bind(org)
            .fetch_one(&mut *tx)
            .await
        }
    }
    .map_err(|_| "composio plane verification failed".to_string())?;
    tx.commit()
        .await
        .map_err(|_| "composio membership commit failed".to_string())?;
    let Some(role) = role else {
        return Ok(Scope::Left);
    };
    if !plane_ok {
        return Ok(Scope::OffPlane);
    }
    let decision = match &t.tenant {
        TenantId::Consumer => None,
        TenantId::Org(org) => Some(TenancyDecision::Org(TenantContext {
            org_id: org.clone(),
            account_id: account,
            user_id: user,
            pool: t.pool.clone(),
            page_pool: None,
        })),
    };
    let tenant = Tenant {
        store: Some(t.store.clone()),
        pool: Some(t.pool.clone()),
        decision,
        class: crate::route_class::RouteClass::Other,
    };
    let caller = UserContext {
        account_id: account,
        user_id: user,
        role,
        device_id: "composio-poll".into(),
    };
    Ok(Scope::Member(tenant, caller))
}
