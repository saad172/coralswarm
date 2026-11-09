//! The [`TenantRegistry`]: lazy, single-flight, idle-evicted per-tenant Postgres
//! connection pools for the shared backend fleet.
//!
//! Isolation-critical invariant: **before a tenant pool is ever published, the
//! registry asserts the tenant database's own `tenant_meta.org_id` equals the
//! `clerk_org_id` the caller routed to.** A swapped DSN, a two-orgs-one-DB
//! provisioning mistake, or a poisoned routing row therefore fails closed at
//! pool-build time — a failed request, not a cross-tenant read. A failed build
//! is never cached, so a transient error retries on the next request.
//!
//! **Also the one place that guarantees an org's tenant database has its
//! commons "org" ocean** (org-tenancy OPS-4 follow-up — the empty-tenant
//! prod bug: `testorg` had zero oceans and `GET /v1/graphs` came back `[]`
//! forever). See [`ensure_org_ocean`]'s doc for the full story of why this
//! belongs in [`TenantRegistry::build`] rather than in
//! `clerk_provision::ensure_org_account` (which still creates the ocean
//! directly, unaffected by this change, when `MULTITENANT_ENABLED` is off —
//! the only mode in which it has a safe, always-available pool to create it
//! in) or on any per-request hot path.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::OnceCell;
use uuid::Uuid;

use super::secrets::{SecretError, SecretResolver};

/// Default per-tenant REQUEST connection ceiling, summed over both lanes
/// (`page` + `work`, see [`TenantLanes`]). Small on purpose: many tenants
/// share one fleet, and the tenant DB's own pooler is the real ceiling.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 5;
/// Default size of the `page` lane: the request connections reserved for
/// `RouteClass::PageRead` (`docs/plans/page-load-standard/design.md` §4.3).
/// The `work` lane gets the remainder of [`DEFAULT_MAX_CONNECTIONS`]. Two,
/// because a page load issues at most six page reads of under 10 ms each,
/// which queue under ~30 ms over two connections, and every connection
/// reserved here is one the work lane cannot borrow (design §11). Env
/// `TENANT_PAGE_LANE_CONNECTIONS` overrides it; both lanes are sampled as
/// `db_pool` metrics so the number can be re-fitted from data.
pub const DEFAULT_PAGE_LANE_CONNECTIONS: u32 = 2;
/// Env override for [`DEFAULT_PAGE_LANE_CONNECTIONS`].
pub const PAGE_LANE_ENV: &str = "TENANT_PAGE_LANE_CONNECTIONS";

/// The two request-serving pools of one tenant, against the same DSN. A hard
/// partition: a `work` statement that runs for seconds cannot occupy a
/// `page` connection, so page reads keep answering while Library pages, Ask
/// and mutations saturate their own lane. Two sqlx pools rather than a
/// semaphore so every existing `begin_member_scoped` signature stays as is
/// (design §4.3).
#[derive(Clone)]
pub struct TenantLanes {
    /// `RouteClass::Other` and `Stream`: the lane every existing call site
    /// already runs on.
    pub work: PgPool,
    /// `RouteClass::PageRead` only.
    pub page: PgPool,
}

/// A point-in-time reading of one pool, for the `db_pool` metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStats {
    pub size: u32,
    pub idle: u32,
    pub max: u32,
}

impl PoolStats {
    pub fn of(pool: &PgPool) -> Self {
        Self {
            size: pool.size(),
            idle: pool.num_idle() as u32,
            max: pool.options().get_max_connections(),
        }
    }
}

/// `(page, work)` connection counts for a tenant whose request budget is
/// `max_connections`. The page lane is clamped to `1..=max_connections - 1`
/// so neither lane is ever empty; a budget of one connection has no page
/// lane at all (`page = 0`) and the single connection is the work lane.
pub fn lane_split(max_connections: u32, page_lane: u32) -> (u32, u32) {
    if max_connections < 2 {
        return (0, max_connections);
    }
    let page = page_lane.clamp(1, max_connections - 1);
    (page, max_connections - page)
}

/// [`PAGE_LANE_ENV`] if set to a positive integer, else the default.
pub fn page_lane_from_env() -> u32 {
    std::env::var(PAGE_LANE_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_PAGE_LANE_CONNECTIONS)
}
/// Default ceiling for the SEPARATE per-tenant WORKER pool (tenant RLS Phase
/// 2, Category C — see [`TenantRegistry::get_or_build_worker_pool`]).
///
/// **Derived, not guessed** (CodeRabbit Major on the first cut of this PR,
/// which sized this at `2` on the vague "a few concurrent ticks" — too small
/// once actually worked out against `workers::tenant_harness`'s scheduling):
///
/// `run_multi_tenant` runs each tenant-loop worker (`embed`, `embed_retry`,
/// `entity_resolution`, `inference_closure`, `member_ocean_reconciler`,
/// `reaper`, `summary`, `tombstone_purge`, …) as its OWN independent task, and
/// the dormant-tenant scheduling is explicitly DESIGNED to align them: once a
/// tenant's per-(worker, tenant) backoff saturates, `dormant_slot_delay`
/// snaps every worker's next poll onto the SAME absolute slot boundary (one
/// Neon wake-up instead of ~11 smeared ones — see the module doc's Neon-
/// arithmetic section), and the cross-worker wake (`org_idle_delay` +
/// `run_multi_tenant`'s pre-pass pull-forward) additionally snaps every OTHER
/// worker back to `base_poll` the instant any one of them finds real work.
/// The realistic ceiling is therefore **all 11**, not "a few": every
/// tenant-loop worker's own `run_multi_tenant` task can independently decide
/// this tenant is due within the same `SHUTDOWN_POLL_INTERVAL` (500ms)
/// window and attempt to acquire a connection.
///
/// `11` is chosen — matching that count exactly, not padded — because each
/// worker's OWN dequeue-and-mark-running step needs at most one connection at
/// a time; only `embed` additionally fans a single tick's OWN batch out
/// concurrently (`future::join_all` over up to `JOB_BATCH_SIZE = 8` jobs), a
/// SEPARATE, pre-existing, per-worker tuning knob this PR does not touch.
/// That internal batch concurrency is accepted to queue briefly behind the
/// shared 11 during the rare moment both align, rather than sizing the pool
/// to the full every-worker's-own-batch-saturated worst case (up to ~18):
/// this pool serves background, already-latency-tolerant work (workers
/// already accept `base_poll`/backoff delays far larger than any sub-second
/// connection queuing), each connection hold here is one short scoped
/// transaction — never held across an LLM or R2 network call — so even
/// `embed`'s own 8-way fan-out mostly overlaps in wall-clock time rather than
/// in ACTIVE connection hold time, and `DEFAULT_ACQUIRE_TIMEOUT` (5s) is
/// comfortably larger than the sub-second time it takes ~11-18 short
/// transactions to cycle through a pool this size — nothing here times out,
/// it queues briefly. The alternative CodeRabbit's finding raised — STAGGER
/// the aligned wake instead of paying for concurrency — was considered and
/// deliberately not taken: the alignment's entire purpose is minimizing Neon
/// wake-ups (a multi-minute-scale concern), and staggering the 11 workers'
/// connection attempts by even a full second would not cost anything against
/// that goal, but it touches the carefully-tuned, already-tested scheduling
/// algorithm (`org_idle_delay`, `dormant_slot_delay`) for a problem sizing
/// the pool already solves without behavior change — a bigger, riskier
/// change for no proven benefit over the simpler fix.
///
/// Additive to the request pool's budget: worst case
/// [`DEFAULT_MAX_CONNECTIONS`] (5) + `DEFAULT_WORKER_MAX_CONNECTIONS` (11) =
/// **16 connections per active tenant**, up from 5 today. Confirm this
/// against each tenant cluster's connection/pooler headroom before Phase 3 —
/// see [`TenantRegistry::get_or_build_worker_pool`]'s doc for the same
/// caveat, now updated with this derivation.
pub const DEFAULT_WORKER_MAX_CONNECTIONS: u32 = 11;

/// Default idle time-to-eviction: a tenant untouched this long has its pool
/// dropped so a churned/quiet org doesn't hold a live pool forever.
/// (`lib.rs::run` spawns the periodic `evict_idle` sweep that enforces this.)
pub const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
/// Connection-acquire (and initial-connect) timeout for tenant pools. sqlx's
/// default is 30s — long enough that a HANGING tenant DB (network partition /
/// SYN blackhole, as opposed to a fast `ECONNREFUSED`) would park callers for
/// half a minute per attempt. The worker tenant harness additionally bounds
/// whole pool builds with its own timeout; this shorter acquire timeout
/// protects the REQUEST path (`TenantRuntime::resolve_binding`) and any
/// cached-pool-with-dead-connections acquire the same way. 5s comfortably
/// covers a Neon cold resume handshake while keeping a hung tenant from
/// head-of-line-blocking its caller.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum TenantPoolError {
    #[error("resolving tenant secret: {0}")]
    Secret(#[from] SecretError),
    #[error("connecting to tenant database: {0}")]
    Connect(sqlx::Error),
    /// The tenant DB's fingerprint does not match the org we routed to. This is
    /// the swapped-DSN / two-orgs-one-DB defense firing — never serve.
    #[error("tenant DB fingerprint mismatch: routed org {expected:?} but tenant_meta says {found:?}")]
    FingerprintMismatch { expected: String, found: String },
    #[error("tenant DB has no tenant_meta fingerprint row (org {0:?})")]
    MissingFingerprint(String),
    /// [`ensure_org_ocean`] failed. Folded into the SAME "failed build is
    /// never cached" path as every other variant here (see
    /// [`TenantRegistry::get_or_build`]) — deliberately, so the very next
    /// caller (another request, the next tenant-harness worker tick, a retried
    /// onboarding step) retries the whole build, including this insert,
    /// rather than silently publishing a pool for an org that never got its
    /// ocean.
    #[error("ensuring org commons ocean: {0}")]
    EnsureOcean(sqlx::Error),
    /// The worker pool connected as a role that `0009_tenant_rls`'s policies
    /// actually apply to, while nothing is setting `app.tenant_actor`.
    ///
    /// This is the Phase-3 tripwire. Worker ticks currently run ambient queries
    /// with no actor GUC, which is safe ONLY because the app connects as the
    /// table owner and `0009` §7 deliberately does not use `FORCE ROW LEVEL
    /// SECURITY`, so the owner is exempt from every policy. The moment a tenant
    /// DSN is repointed at the `NOBYPASSRLS` `coralswarm_tenant_app` role
    /// (Phase 3), that exemption disappears and every worker would silently read
    /// an empty database — jobs "succeeding" against zero rows.
    ///
    /// Refusing to build the pool converts that silent, data-shaped failure into
    /// a loud one. Clearing it is the job of the per-transaction retrofit, not of
    /// relaxing this check.
    #[error(
        "worker pool for org {org:?} connected as role {role:?}, which tenant RLS policies apply \
         to, but no actor GUC is set — the per-transaction scoping retrofit must land before this \
         tenant's DSN moves off the owner role"
    )]
    WorkerPoolNotRlsExempt { org: String, role: String },
}

/// Returns `Some(role)` when the worker pool's connection is NOT exempt from
/// `0009_tenant_rls`'s policies — i.e. when the Phase-3 tripwire should fire.
/// `None` means exempt, which is the expected state today.
///
/// Three independent ways to be exempt, matching Postgres' own rules:
///
/// * `SUPERUSER` — bypasses all permission checks, RLS included, with no
///   further attribute required. Local dev and much of the test harness connect
///   this way, so omitting it would refuse pools that are in fact exempt;
/// * `BYPASSRLS` on the role — skips policy evaluation explicitly; or
/// * being the table OWNER while the table is not `FORCE ROW LEVEL SECURITY` —
///   which is precisely the state `0009` §7 chose and documented.
///
/// `atoms` stands in for the whole tree: `0009` applies the same
/// enable-without-force treatment across it, and this runs after the
/// fingerprint gate has already proved the tenant schema is present.
///
/// A missing `atoms` is treated as "not exempt" — fail closed. A tenant DB
/// without it is not one whose access rules we can reason about.
async fn worker_pool_rls_exemption_breach(pool: &PgPool) -> Result<Option<String>, TenantPoolError> {
    let (role, superuser, bypass_rls, owner_exempt): (String, bool, bool, Option<bool>) =
        sqlx::query_as(
            r#"
        SELECT
            current_user::text,
            COALESCE((SELECT r.rolsuper
                        FROM pg_roles r
                       WHERE r.rolname = current_user), false),
            COALESCE((SELECT r.rolbypassrls
                        FROM pg_roles r
                       WHERE r.rolname = current_user), false),
            (SELECT c.relowner = (SELECT r2.oid FROM pg_roles r2 WHERE r2.rolname = current_user)
                    AND NOT c.relforcerowsecurity
               FROM pg_class c
              WHERE c.relname = 'atoms'
                AND c.relnamespace = 'public'::regnamespace)
        "#,
        )
        .fetch_one(pool)
        .await
        .map_err(TenantPoolError::Connect)?;

    if superuser || bypass_rls || owner_exempt.unwrap_or(false) {
        Ok(None)
    } else {
        Ok(Some(role))
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

struct TenantPool {
    /// The `work` lane on a request-pool entry; the whole pool on a
    /// worker-pool entry.
    pool: PgPool,
    /// The `page` lane. `None` on worker-pool entries (workers never serve a
    /// `PageRead`) and on a one-connection budget (see [`lane_split`]).
    page: Option<PgPool>,
    /// The secret ref this pool's DSN was resolved from at build time. If the
    /// routing row is later re-pointed at a different ref (tenant DB moved),
    /// [`TenantRegistry::get_or_build`] notices the mismatch, retires this
    /// pool, and rebuilds against the new DSN — without this, a re-pointed
    /// tenant would silently keep using the old database until a restart.
    secret_ref: String,
    last_used_ms: AtomicI64,
}

impl TenantPool {
    fn touch(&self) {
        self.last_used_ms.store(now_ms(), Ordering::Relaxed);
    }

    fn lanes(&self) -> TenantLanes {
        TenantLanes {
            work: self.pool.clone(),
            // A budget too small for two lanes serves page reads from the
            // one lane it has.
            page: self.page.clone().unwrap_or_else(|| self.pool.clone()),
        }
    }

    async fn close(&self) {
        self.pool.close().await;
        if let Some(page) = &self.page {
            page.close().await;
        }
    }
}

/// Concurrent map of verified `clerk_org_id` → lazily-built tenant pool.
///
/// The pool for an org is built at most once even under concurrent first
/// requests (single-flight via a per-key [`OnceCell`]), and only after its
/// fingerprint is verified. Not `Clone`; share behind an `Arc`.
pub struct TenantRegistry {
    resolver: Arc<dyn SecretResolver>,
    // Per-key OnceCell gives single-flight: concurrent first requests for the
    // same org await one build. A failed build removes the cell so it retries.
    cells: DashMap<String, Arc<OnceCell<Arc<TenantPool>>>>,
    /// The SEPARATE worker-pool cache (tenant RLS Phase 2, Category C). Keyed
    /// identically to `cells` but never populated or read by request-serving
    /// code — only [`get_or_build_worker_pool`](Self::get_or_build_worker_pool),
    /// which only `workers::tenant_harness` calls. Kept as a second map rather
    /// than a field on `TenantPool` so the two pools have independent
    /// lifecycles (a request-pool rebuild on secret-ref change does not need
    /// to also tear down the worker pool, and vice versa).
    worker_cells: DashMap<String, Arc<OnceCell<Arc<TenantPool>>>>,
    max_connections: u32,
    /// Requested `page` lane size; [`lane_split`] clamps it against
    /// `max_connections` at build time.
    page_lane_connections: u32,
    worker_max_connections: u32,
    idle_ttl: Duration,
}

impl TenantRegistry {
    pub fn new(resolver: Arc<dyn SecretResolver>) -> Self {
        Self::with_config(resolver, DEFAULT_MAX_CONNECTIONS, DEFAULT_IDLE_TTL)
    }

    pub fn with_config(
        resolver: Arc<dyn SecretResolver>,
        max_connections: u32,
        idle_ttl: Duration,
    ) -> Self {
        TenantRegistry {
            resolver,
            cells: DashMap::new(),
            worker_cells: DashMap::new(),
            max_connections,
            page_lane_connections: page_lane_from_env(),
            worker_max_connections: DEFAULT_WORKER_MAX_CONNECTIONS,
            idle_ttl,
        }
    }

    /// Override the `page` lane size (default [`page_lane_from_env`]).
    /// Builder-style, like [`Self::with_worker_max_connections`].
    pub fn with_page_lane_connections(mut self, n: u32) -> Self {
        self.page_lane_connections = n;
        self
    }

    /// Override the worker-pool connection ceiling (default
    /// [`DEFAULT_WORKER_MAX_CONNECTIONS`]). Builder-style so existing
    /// `TenantRegistry::new`/`with_config` call sites (production and the ~40
    /// pg-gated test files that construct a registry) are unaffected.
    pub fn with_worker_max_connections(mut self, n: u32) -> Self {
        self.worker_max_connections = n;
        self
    }

    /// Number of currently-built (published) tenant REQUEST pools. In-flight
    /// or failed builds are not counted. Useful for the pool-map-size metric
    /// and tests.
    pub fn live_pool_count(&self) -> usize {
        self.cells
            .iter()
            .filter(|e| e.value().get().is_some())
            .count()
    }

    /// Number of currently-built (published) tenant WORKER pools (see
    /// [`get_or_build_worker_pool`](Self::get_or_build_worker_pool)). Separate
    /// from [`live_pool_count`](Self::live_pool_count) since the two pools
    /// have independent lifecycles.
    pub fn live_worker_pool_count(&self) -> usize {
        self.worker_cells
            .iter()
            .filter(|e| e.value().get().is_some())
            .count()
    }

    /// Resolve — building lazily and single-flight — the pool for `clerk_org_id`
    /// whose DSN lives behind `secret_ref`. Before returning a freshly-built
    /// pool it asserts the tenant DB's `tenant_meta.org_id` equals
    /// `clerk_org_id`, and guarantees the tenant has its commons "org" ocean
    /// (see [`ensure_org_ocean`]). A failed build is not cached.
    ///
    /// `account_id` is the org's OWN control-plane account id
    /// (`tenant_databases.account_id` — every caller already has the routing
    /// row in hand to get `secret_ref` from, so this costs nothing extra to
    /// thread through). It is used ONLY by [`ensure_org_ocean`] on a cold
    /// build; a cache hit never touches it. It is NOT part of the cache key
    /// (that remains `clerk_org_id`, matching `tenant_databases`' own
    /// one-row-per-org shape) — a caller that ever passed a different
    /// `account_id` for the same `clerk_org_id` would only affect the ocean
    /// row on the NEXT cold build (after eviction/rebuild), never retroactively
    /// rewrite one already created; in practice this can't happen, since
    /// `tenant_databases.account_id` is immutable once a routing row is
    /// inserted (org-tenancy has no "reassign an org to a different account"
    /// operation).
    ///
    /// If a cached pool was built from a DIFFERENT `secret_ref` than the one
    /// the caller's (fresh) routing row names, the cached pool is retired
    /// (closed) and rebuilt against the new ref — this is how a re-pointed
    /// tenant DSN takes effect without a fleet restart. Closing the old pool
    /// severs any in-flight queries on it deliberately: after a re-point the
    /// old DSN's database is being retired, and continuing to serve from it
    /// is worse than a handful of failed requests during the swap.
    pub async fn get_or_build(
        &self,
        clerk_org_id: &str,
        account_id: Uuid,
        secret_ref: &str,
    ) -> Result<PgPool, TenantPoolError> {
        self.get_or_build_lanes(clerk_org_id, account_id, secret_ref)
            .await
            .map(|lanes| lanes.work)
    }

    /// [`Self::get_or_build`], returning both request lanes. The binding gate
    /// (`TenantRuntime::finish_binding`) is the one caller that needs the
    /// `page` lane; every other caller is background or control-plane work
    /// that runs on `work` and keeps using `get_or_build`.
    pub async fn get_or_build_lanes(
        &self,
        clerk_org_id: &str,
        account_id: Uuid,
        secret_ref: &str,
    ) -> Result<TenantLanes, TenantPoolError> {
        loop {
            let cell = self
                .cells
                .entry(clerk_org_id.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone();

            match cell
                .get_or_try_init(|| self.build(clerk_org_id, account_id, secret_ref))
                .await
            {
                Ok(tp) if tp.secret_ref == secret_ref => {
                    tp.touch();
                    return Ok(tp.lanes());
                }
                Ok(tp) => {
                    // Routing row re-pointed at a new secret ref: retire the
                    // stale pool and loop once to rebuild. Remove only if the
                    // map still holds OUR cell so a racing rebuild that
                    // already replaced it isn't clobbered; the retry then
                    // re-resolves whatever is current.
                    tracing::warn!(
                        org = %clerk_org_id,
                        old_ref = %tp.secret_ref,
                        new_ref = %secret_ref,
                        "tenant secret ref changed — retiring cached pool and rebuilding"
                    );
                    let stale = tp.clone();
                    self.cells
                        .remove_if(clerk_org_id, |_, c| Arc::ptr_eq(c, &cell));
                    stale.close().await;
                    continue;
                }
                Err(e) => {
                    // Do not cache a failed build — drop the empty cell so the
                    // next request re-attempts (transient connect error,
                    // later-fixed routing row, etc.).
                    self.cells.remove(clerk_org_id);
                    return Err(e);
                }
            }
        }
    }

    async fn build(
        &self,
        clerk_org_id: &str,
        account_id: Uuid,
        secret_ref: &str,
    ) -> Result<Arc<TenantPool>, TenantPoolError> {
        let (page, work) = lane_split(self.max_connections, self.page_lane_connections);
        Self::connect_and_verify(
            &*self.resolver,
            clerk_org_id,
            account_id,
            secret_ref,
            work,
            Some(page).filter(|n| *n > 0),
            false,
        )
        .await
    }

    /// Gauge readings for both request lanes of every ALREADY-BUILT request
    /// pool, keyed `org:{clerk_org_id}:work` / `org:{clerk_org_id}:page`
    /// (the admission registry's keys), for the pool sampler
    /// (`telemetry::spawn_pool_sampler`). Never builds a pool: a dormant
    /// tenant is not woken to be measured. A one-connection budget reports
    /// its work lane only (see [`lane_split`]).
    pub fn live_lane_stats(&self) -> Vec<(String, PoolStats)> {
        let mut out = Vec::new();
        for e in self.cells.iter() {
            let Some(tp) = e.value().get() else { continue };
            out.push((format!("org:{}:work", e.key()), PoolStats::of(&tp.pool)));
            if let Some(page) = &tp.page {
                out.push((format!("org:{}:page", e.key()), PoolStats::of(page)));
            }
        }
        out
    }

    /// Resolve — building lazily and single-flight, same shape as
    /// [`get_or_build`](Self::get_or_build) — the SEPARATE, system-scoped
    /// worker pool for `clerk_org_id` (tenant RLS Phase 2, Category C).
    ///
    /// **Why a second pool rather than a transaction-scoped `SET LOCAL` on the
    /// request pool.** `migrations_tenant/0008_tenant_rls`'s policies read
    /// `app.tenant_actor`/`app.user_id`, both `SET LOCAL`-only (transaction-
    /// scoped), which is what makes them safe on the REQUEST pool: a value set
    /// for one member's transaction is guaranteed gone by the time the
    /// connection is handed to a different member's request. `workers::
    /// tenant_harness` ticks, by contrast, run largely AMBIENT queries directly
    /// against `WorkerTenant.pool` / `WorkerTenant.store` — not everything is
    /// already inside one transaction — so retrofitting every one of the 11
    /// tenant-loop workers to open a transaction before its first query, purely
    /// to carry a GUC, is both a wide surface (11 workers, many call sites each)
    /// and a NEW risk: a tick that streams or does slow per-row work
    /// (`entity_resolution`, `infer_expertise`) wrapped in one long transaction
    /// reintroduces the idle-in-transaction problem the member-ocean purge arm
    /// (PR 8) was fixed to avoid.
    ///
    /// This pool USED TO sidestep both by setting `app.tenant_actor=system`
    /// via the Postgres STARTUP `options` parameter
    /// (`-c app.tenant_actor=system`, [`PgConnectOptions::options`]) — a
    /// SESSION-level GUC on every physical connection the pool ever handed
    /// out, needing no transaction and no change inside any tick body.
    /// **That mechanism no longer exists.** PR #388 deleted it because a
    /// connection pooler rejects unknown startup parameters outright
    /// (`unsupported startup parameter in options: app.tenant_actor`), so the
    /// pool failed to build for every tenant on a pooled DSN and all org
    /// background work was dead. See [`connect_and_verify`](Self::connect_and_verify),
    /// which now builds this pool with a plain `PgPoolOptions::connect` and
    /// sets no startup option at all.
    ///
    /// So what makes an AMBIENT tick query correct on this pool today is not
    /// a GUC. It is the owner exemption alone: the app connects as the
    /// tables' owner and `0009` §7 deliberately does not FORCE row security,
    /// so no policy applies. That is a dependency, not a guarantee, and it is
    /// asserted rather than assumed — [`worker_pool_rls_exemption_breach`]
    /// runs on every build of this pool and refuses it with
    /// [`TenantPoolError::WorkerPoolNotRlsExempt`] for any role RLS would
    /// apply to. When OPS-4-T repoints tenant DSNs at the NOBYPASSRLS
    /// `coralswarm_tenant_app` role, this pool therefore FAILS TO BUILD and
    /// the tenant's background work stops with a logged error. It does not
    /// quietly return empty rows.
    ///
    /// The pool still earns its keep — a separate connection budget and its
    /// own idle eviction, neither of which depended on the GUC — and it is
    /// still never given to request-serving code. Ticks are being converted
    /// to open a short [`crate::tenancy::scope::begin_tenant_system_scoped`]
    /// transaction per unit of work instead; a session-level `SET` was never
    /// an option on the REQUEST pool, because it would leak one member's
    /// actor onto the very next request that reuses the same physical
    /// connection — that IS the reason 0008's own header (§4) requires
    /// `SET LOCAL` there.
    ///
    /// **Why `system` for every tenant-loop worker, not per-job `app.user_id`.**
    /// The Phase-2 runbook's Category C section prefers `begin_tenant_scoped`
    /// with the job's owning ocean wherever a payload carries a `graph_id`. In
    /// practice none of the 11 tenant-loop workers can honor that at the
    /// GRANULARITY this pool operates at: every one of them dequeues/sweeps a
    /// BATCH of rows in one query (`JOB_BATCH_SIZE` jobs, or an `op_log`/
    /// `atoms` sweep) that in general spans MULTIPLE members' oceans in a
    /// single round trip. There is no single honest `app.user_id` for a batch;
    /// splitting each worker into one transaction per row to get per-owner
    /// scoping would be a real behavior/throughput change, not a scoping
    /// change, and is out of scope for a behavior-preserving Phase 2 PR. Every
    /// tenant-loop worker's `system` usage is therefore reviewed and justified
    /// on this same basis — see `docs/tenant-rls-cutover-runbook.md` §2.3 for
    /// the per-worker table.
    ///
    /// **Cost.** A second pool per active tenant, ceiling
    /// [`DEFAULT_WORKER_MAX_CONNECTIONS`] — see that constant's own doc for
    /// the full derivation (all 11 tenant-loop workers can be simultaneously
    /// due for one tenant by the aligned-wake design, so the ceiling is sized
    /// to that count, not a guess). Additive to the request pool's
    /// [`DEFAULT_MAX_CONNECTIONS`] (5) — worst case 16 connections per active
    /// tenant instead of 5. This registry's own doc already treats "the
    /// tenant DB's own pooler is the real ceiling" as the load-bearing
    /// assumption for per-tenant connection counts; an 11-connection addition
    /// is a real, additive cost that must be confirmed against each tenant
    /// cluster's connection/pooler headroom before Phase 3, not assumed.
    pub async fn get_or_build_worker_pool(
        &self,
        clerk_org_id: &str,
        account_id: Uuid,
        secret_ref: &str,
    ) -> Result<PgPool, TenantPoolError> {
        loop {
            let cell = self
                .worker_cells
                .entry(clerk_org_id.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone();

            match cell
                .get_or_try_init(|| {
                    Self::connect_and_verify(
                        &*self.resolver,
                        clerk_org_id,
                        account_id,
                        secret_ref,
                        self.worker_max_connections,
                        None,
                        true,
                    )
                })
                .await
            {
                Ok(tp) if tp.secret_ref == secret_ref => {
                    tp.touch();
                    return Ok(tp.pool.clone());
                }
                Ok(tp) => {
                    // Same re-point handling as get_or_build's request pool.
                    tracing::warn!(
                        org = %clerk_org_id,
                        old_ref = %tp.secret_ref,
                        new_ref = %secret_ref,
                        "tenant secret ref changed — retiring cached WORKER pool and rebuilding"
                    );
                    let stale = tp.clone();
                    self.worker_cells
                        .remove_if(clerk_org_id, |_, c| Arc::ptr_eq(c, &cell));
                    stale.close().await;
                    continue;
                }
                Err(e) => {
                    self.worker_cells.remove(clerk_org_id);
                    return Err(e);
                }
            }
        }
    }

    /// Shared connect+verify body for both the request pool ([`build`]) and
    /// the worker pool ([`get_or_build_worker_pool`]). `is_worker_pool` selects
    /// which of the two; both now connect identically, and the flag only picks
    /// which post-connect assertion applies. `page_lane` is the request
    /// pool's second lane (`Some(n)` connections against the same DSN, built
    /// only once the first pool has passed every check below); `None` on the
    /// worker pool.
    ///
    /// **Why there is no longer a startup option here.** This used to append
    /// `-c app.tenant_actor=system` to the worker pool's STARTUP packet, so the
    /// GUC was session-level for the connection's whole life and no worker tick
    /// needed a transaction. That is unusable against a connection pooler:
    /// Neon's rejects unknown startup parameters outright
    /// (`unsupported startup parameter in options: app.tenant_actor —
    /// Please use unpooled connection`), so the worker pool failed to build for
    /// every tenant on a pooled DSN and ALL org background work was dead —
    /// silently, since the failure only ever surfaced as a log line.
    ///
    /// The GUC now gets set per transaction instead, by
    /// [`crate::tenancy::scope::begin_tenant_system_scoped`] — the same
    /// `SET LOCAL` mechanism the request path already uses, which a pooler
    /// handles correctly. Removing it here is safe today only because the app
    /// still connects as the table owner and `0009` §7 deliberately does not
    /// FORCE row security, so policies do not apply to us at all; the assertion
    /// below is what makes that dependency explicit rather than assumed.
    async fn connect_and_verify(
        resolver: &dyn SecretResolver,
        clerk_org_id: &str,
        account_id: Uuid,
        secret_ref: &str,
        max_connections: u32,
        page_lane: Option<u32>,
        is_worker_pool: bool,
    ) -> Result<Arc<TenantPool>, TenantPoolError> {
        let dsn = resolver.resolve_async(secret_ref).await?;
        let pool = connect_pool(&dsn, max_connections).await?;

        // Phase-3 tripwire — see `TenantPoolError::WorkerPoolNotRlsExempt`.
        // Only the worker pool needs it: the request path sets `app.user_id`
        // per transaction and is correct under enforced RLS either way.
        if is_worker_pool {
            // Close on the check's OWN failure too, not just on a breach — `?`
            // here would leak the pool, and every other error path in this
            // function closes before returning.
            let breach = match worker_pool_rls_exemption_breach(&pool).await {
                Ok(b) => b,
                Err(e) => {
                    pool.close().await;
                    return Err(e);
                }
            };
            if let Some(role) = breach {
                pool.close().await;
                return Err(TenantPoolError::WorkerPoolNotRlsExempt {
                    org: clerk_org_id.to_string(),
                    role,
                });
            }
        }

        // Fingerprint gate: the tenant DB self-reports which org it belongs to.
        let found: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta")
            .fetch_optional(crate::tenancy::scope::request_read(&pool))
            .await
            .map_err(TenantPoolError::Connect)?;

        match found {
            Some(f) if f == clerk_org_id => {
                // Ocean self-heal (see this module's doc + `ensure_org_ocean`'s
                // own doc for the full story of the gap this closes): runs here,
                // AFTER the fingerprint proves this is genuinely the right
                // tenant DB and BEFORE the pool is ever published/cached, so
                // every consumer of a freshly-built pool — the request path,
                // `/mcp`, the tenant-harness worker loop, onboarding's own
                // rotate-dek step — gets a tenant that is guaranteed to have
                // its org ocean, with zero extra cost on every subsequent
                // cache-hit call (see `get_or_build`'s single-flight `OnceCell`).
                // Run unconditionally here too (not only on the request pool's
                // build): the worker harness's periodic poll can be the FIRST
                // thing to ever resolve a dormant org's pool (see this
                // module's doc), and `ensure_org_ocean`'s `ON CONFLICT DO
                // NOTHING` makes a second, redundant call from the other
                // pool's build harmless.
                if let Err(e) = ensure_org_ocean(&pool, account_id).await {
                    pool.close().await;
                    return Err(TenantPoolError::EnsureOcean(e));
                }
                // The page lane connects to the DSN the fingerprint and
                // ocean checks above just proved; it needs no checks of its
                // own, and a connect failure here fails the whole build like
                // every other error path.
                let page = match page_lane {
                    Some(n) => match connect_pool(&dsn, n).await {
                        Ok(p) => Some(p),
                        Err(e) => {
                            pool.close().await;
                            return Err(e);
                        }
                    },
                    None => None,
                };
                Ok(Arc::new(TenantPool {
                    pool,
                    page,
                    secret_ref: secret_ref.to_string(),
                    last_used_ms: AtomicI64::new(now_ms()),
                }))
            }
            Some(f) => {
                pool.close().await;
                Err(TenantPoolError::FingerprintMismatch {
                    expected: clerk_org_id.to_string(),
                    found: f,
                })
            }
            None => {
                pool.close().await;
                Err(TenantPoolError::MissingFingerprint(clerk_org_id.to_string()))
            }
        }
    }

    /// Evict pools untouched for longer than the configured idle TTL, closing
    /// each. Intended to run periodically from a background task. Returns the
    /// number evicted (request pools only — see
    /// [`evict_idle_worker_pools`](Self::evict_idle_worker_pools) for the
    /// worker-pool sweep).
    pub async fn evict_idle(&self) -> usize {
        Self::evict_idle_map(&self.cells, self.idle_ttl).await
    }

    /// Same idle-eviction sweep as [`evict_idle`](Self::evict_idle), for the
    /// SEPARATE worker-pool cache (tenant RLS Phase 2, Category C). Without
    /// this, a tenant that left the worker harness's active list (suspended,
    /// offboarded, or simply not polled by any tenant-loop worker for a while)
    /// would leak its worker pool forever — `get_or_build_worker_pool` is only
    /// ever called by `workers::tenant_harness`, which has no eviction path of
    /// its own. Intended to run from the same periodic sweep as `evict_idle`
    /// (`lib.rs::run`).
    pub async fn evict_idle_worker_pools(&self) -> usize {
        Self::evict_idle_map(&self.worker_cells, self.idle_ttl).await
    }

    /// Drop and close BOTH of one org's cached pools, if any — used by the
    /// self-serve provisioning job's cleanup so a pool built during a failed
    /// attempt never outlives the database it pointed at. A missing entry is
    /// a no-op.
    pub async fn evict_org(&self, clerk_org_id: &str) {
        for cells in [&self.cells, &self.worker_cells] {
            if let Some((_, cell)) = cells.remove(clerk_org_id) {
                if let Some(tp) = cell.get() {
                    tp.close().await;
                }
            }
        }
    }

    async fn evict_idle_map(
        cells: &DashMap<String, Arc<OnceCell<Arc<TenantPool>>>>,
        idle_ttl: Duration,
    ) -> usize {
        let cutoff = now_ms() - idle_ttl.as_millis() as i64;
        let stale: Vec<String> = cells
            .iter()
            .filter_map(|e| match e.value().get() {
                Some(tp) if tp.last_used_ms.load(Ordering::Relaxed) < cutoff => {
                    Some(e.key().clone())
                }
                _ => None,
            })
            .collect();

        let mut evicted = 0;
        for key in stale {
            if let Some((_, cell)) = cells.remove(&key) {
                if let Some(tp) = cell.get() {
                    tp.close().await;
                }
                evicted += 1;
            }
        }
        evicted
    }
}

/// One sqlx pool against `dsn`, shaped the way every tenant pool is.
async fn connect_pool(dsn: &str, max_connections: u32) -> Result<PgPool, TenantPoolError> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        // See DEFAULT_ACQUIRE_TIMEOUT: sqlx's 30s default would let a
        // hanging (not refusing) tenant DB park callers for half a minute
        // per acquire — on the worker harness that head-of-line-blocks
        // the consumer plane, on the request path it pins gate requests.
        .acquire_timeout(DEFAULT_ACQUIRE_TIMEOUT)
        // Best-effort pin for ambient worker queries (no scoped tx).
        // Neon PgBouncer rejects startup `-c search_path=`; a regular
        // SET after connect is allowed. Transaction pooling may still
        // hand the next statement a different backend — migration 0092's
        // role/database default is what those backends inherit.
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query(r#"SELECT set_config('search_path', '"$user", public', false)"#)
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(dsn)
        .await
        .map_err(TenantPoolError::Connect)
}

/// Idempotently guarantee this org's tenant database carries its commons
/// "org" ocean — the retroactive, order-independent fix for the
/// database-per-org empty-tenant gap described below.
///
/// **The gap.** `clerk_provision::ensure_org_account`'s "shadow-provisioning
/// fence" (see that function's own doc comment) deliberately creates ONLY the
/// control `accounts` row when `state.tenant_runtime.enabled`
/// (`MULTITENANT_ENABLED`) is on — it explicitly skips creating the ocean (and
/// the DEK) on that call, because the `pool` it has in hand there is always
/// the shared CONTROL pool (every one of its three callers —
/// `middleware::auth`'s request-path provisioning, and both
/// `routes::clerk_webhook` call sites — passes `state.db`), never the org's
/// own tenant pool, and writing an org's content ocean onto the shared plane
/// is exactly the "duplicate graph + a competing DEK" the fence exists to
/// prevent. That reasoning is sound (the accounts-row-creation moment —
/// typically the Clerk `organization.created` webhook, firing the instant an
/// admin creates the org, long before any operator runs `onboard_tenant.sh` —
/// genuinely cannot assume the org's tenant database exists yet, so it has no
/// safe pool to create the ocean IN). But the DEK half of that punt has a real
/// destination (`onboard_tenant.sh` step 7 → `routes::admin::rotate_dek`,
/// which upserts the tenant's `account_keys` row) while the ocean half never
/// got one: no script step, no endpoint, nothing ever inserted into the
/// tenant's `graphs` table. So under `MULTITENANT_ENABLED`, EVERY org's
/// commons ocean was simply never created, by design, unconditionally — not
/// a race that sometimes loses, but a punt with no catcher on the other end.
/// Result: a real, `active`, fully-migrated tenant with a fingerprint-verified
/// `tenant_meta` row and ZERO oceans, forever — exactly what happened to
/// prod's `testorg` (`org_2ExampleTestOrg000000000000`): `GET /v1/graphs`
/// returned `[]` on its org host, and the web boot's empty-oceans fallback
/// (`web/lib/providers.tsx`) then tried to create a PERSONAL ocean under the
/// org account, which `routes::graphs.rs`'s ADR-0007 D2 guard correctly
/// refuses — a 403 loop on first login, with no code path that ever wrote the
/// missing ocean.
///
/// (When `MULTITENANT_ENABLED` is off, `ensure_org_account` still creates the
/// ocean directly and unconditionally on the shared pool, exactly as before —
/// there is no tenant runtime/registry in that mode at all, so this function
/// is never reached and nothing here changes that path's behavior.)
///
/// **Why this fix lives here, in [`TenantRegistry::build`], and not back in
/// `ensure_org_account`:**
///
/// 1. **Order-independent by construction.** `build()` never runs until a
///    tenant pool is actually being connected FOR REAL — i.e. the tenant
///    database provably exists and has been migrated far enough to carry a
///    `tenant_meta` fingerprint row (the check immediately above this call
///    site). There is no "tenant DB doesn't exist yet" case here to trip
///    over: by the time this function is reached, migration has already run
///    (`tenant_meta` and `graphs` land in the same tenant-tree baseline —
///    `migrations_tenant/0001_tenant_baseline.sql` — so the fingerprint
///    check passing guarantees `graphs` exists too). Whichever caller
///    reaches `build()` FIRST for a given org — the Clerk webhook's
///    request-adjacent traffic, a member's first JWT, onboarding's own
///    step 7 `rotate-dek` call (which builds the pool WHILE the routing row
///    is still `provisioning`, before step 9 flips it `active` — see
///    `routes::admin::rotate_dek`'s doc comment), or the tenant-harness
///    background worker loop discovering the org on its next poll — is the
///    one that lays the ocean down, and every later reach is a no-op (point
///    3 below). Nothing about `accounts`-row timing can defeat it, because
///    nothing here ever looks at the `accounts` table at all.
/// 2. **Cheap on the hot path, by construction rather than by caching
///    discipline someone has to remember.** `build()` runs behind
///    `get_or_build`'s per-org `OnceCell` — i.e. AT MOST ONCE per tenant pool
///    per process lifetime (bounded by `DEFAULT_IDLE_TTL`, 30 minutes of
///    inactivity). The overwhelming majority of `get_or_build` calls are
///    cache hits that never reach this function, so there is no per-request
///    round trip to add here, unlike a "check on every org-authenticated
///    request" design would require.
/// 3. **Retroactive and self-healing, with no backfill job or new endpoint.**
///    Every ALREADY-onboarded org — including one stuck in today's broken
///    empty-tenant state — converges the next time ANYTHING resolves its
///    pool. In particular `workers::tenant_harness::run_multi_tenant` polls
///    every `active` `tenant_databases` row on a timer (`refresh_tenants`)
///    completely independent of user traffic, so a dormant org with nobody
///    ever logging in still gets its ocean created by the next background
///    worker tick — true convergence, not merely "fixed the next time a user
///    happens to visit".
///
/// **Idempotent and race-safe without a `SELECT`-then-`INSERT` window.**
/// `graphs`' `uq_graphs_one_org_per_account` partial unique index
/// (`... WHERE kind = 'org'`, defined in `migrations_tenant/
/// 0001_tenant_baseline.sql`) already enforces "at most one org ocean per
/// account" at the database level, so the `INSERT ... ON CONFLICT ... DO
/// NOTHING` below is a single atomic statement, same convention as
/// `clerk_provision.rs`'s `INSERT ... ON CONFLICT ... DO NOTHING RETURNING
/// id` idioms. This makes it safe under THREE distinct kinds of concurrency
/// at once: `build()`'s own per-process single-flight (only one call per org
/// ever reaches this far per process), two different backend PROCESSES/
/// instances racing their first cold build for the same org (the DB-level
/// unique index is what makes that safe — the process-local `OnceCell`
/// cannot), and a re-run of this exact statement on a LATER cold build after
/// eviction (a no-op, since the row from the first build is already there).
///
/// **No RLS scoping needed, but for a narrower reason than it used to be**
/// (CORRECTED — tenant RLS Phase 1/2). This paragraph originally claimed
/// "tenant databases are not covered by RLS at all", which stopped being true
/// the moment `migrations_tenant/0008_tenant_rls` shipped: the tenant tree
/// DOES carry row-level security now, including on `graphs`. What is still
/// true, and is the actual reason no scoping is required here: (a) TODAY, the
/// pool this function receives is always either the owner-connected request
/// pool or [`get_or_build_worker_pool`](Self::get_or_build_worker_pool)'s
/// worker pool — and BOTH are owner-connected, so both are exempt from
/// `ENABLE`-only RLS per 0008 §7 and no policy applies to either. (An earlier
/// version of this sentence gave a second, different reason for the worker
/// pool: a connection-level `app.tenant_actor=system` satisfying
/// `tenant_rls_is_system()`. PR #388 deleted that startup-packet GUC — a
/// connection pooler rejects it — so the owner exemption is the only reason
/// left, for both pools alike. The worker pool asserts it at build time via
/// `worker_pool_rls_exemption_breach`.) So there is nothing to scope either
/// way, and
/// (b) even under the eventual non-owner Phase 3 role with NEITHER of those
/// covering it, this specific `INSERT ... kind = 'org'` would still pass
/// `graphs`' `WITH CHECK` unconditionally, because that check is `tenant_rls_
/// is_system() OR kind <> 'member' OR owner_user_id = tenant_rls_actor_
/// user_id()` — an `'org'`-kind row satisfies the middle arm regardless of any
/// GUC. A plain `INSERT` against the ambient pool is therefore correct on all
/// three counts, not because this table is unprotected.
///
/// **Naming.** This path has no access to the org's Clerk display name (only
/// `clerk_org_id` and `account_id` are known at pool-build time — the name
/// lives in Clerk webhook payloads/JWT claims that `ensure_org_account` sees,
/// not here), so a newly-created ocean is named the same generic
/// `"Organization"` fallback `ensure_org_account` itself uses when no name
/// hint is available. This only matters under `MULTITENANT_ENABLED` (where, as
/// described above, this is the ONLY thing that ever creates the org ocean):
/// under the flag-off path, `ensure_org_account` still names it from the real
/// Clerk org name when available, and this `ON CONFLICT DO NOTHING` never
/// touches (or renames) a row that already exists — an operator can always
/// rename the ocean afterward if the generic default is undesirable.
///
/// A failure here fails the WHOLE pool build — see [`TenantPoolError::EnsureOcean`].
///
/// **Deliberately NOT `INSERT ... ON CONFLICT ... DO NOTHING`** (tenant RLS
/// Phase 2 fix — a real, pre-existing Phase-3 landmine this PR's own proof
/// tests caught, not something introduced here). Reproduced directly against
/// Postgres: once `migrations_tenant/0008_tenant_rls`'s policies are live and
/// this runs as the NOBYPASSRLS `coralswarm_tenant_app` role, `INSERT ...
/// ON CONFLICT (account_id) WHERE kind = 'org' DO NOTHING` unconditionally
/// fails with "new row violates row-level security policy for table
/// `graphs`" — REGARDLESS of `app.tenant_actor`, and even when there is no
/// actual conflicting row at all. Isolated by elimination: a `WITH CHECK
/// (true)` policy does not trigger it; the ordinary `graphs` `WITH CHECK`
/// (`tenant_rls_is_system() OR kind <> 'member' OR ...`) alone does not
/// either; it reproduces only once the table's `USING` clause is the actual
/// `graphs` predicate, `id IN (SELECT * FROM tenant_rls_visible_graph_ids())`
/// — a `SECURITY DEFINER`-function-backed `USING` clause combined with an
/// `ON CONFLICT` arbiter targeting a PARTIAL unique index. This is a genuine
/// interaction in Postgres's speculative-insertion planning for `ON
/// CONFLICT`, not anything specific to this table's data — but the fix
/// applied here is local rather than filed upstream, because every tenant
/// pool cold-build (BOTH `get_or_build`'s request pool and
/// [`TenantRegistry::get_or_build_worker_pool`]'s worker pool — this is the
/// ONE function both call) runs this statement unconditionally, so an
/// unfixed `ensure_org_ocean` would hard-fail every cold pool build the
/// instant Phase 3 repoints a tenant DSN at the non-owner role.
///
/// The fix: a PLAIN `INSERT`, with idempotency/race-safety recovered by
/// catching `unique_violation` (23505 — the `uq_graphs_one_org_per_account`
/// constraint) and treating it as success, exactly the same idiom
/// `0008_tenant_rls`'s own migration DDL already uses for the analogous
/// `CREATE ROLE` shared-catalog race (see that migration's §1 comment). This
/// sidesteps the `ON CONFLICT`/RLS interaction entirely: a plain `INSERT`'s
/// `WITH CHECK` evaluation is the one already proven to pass (kind='org'
/// satisfies `kind <> 'member'` unconditionally), and a genuine duplicate
/// (lost the race, or already provisioned) surfaces as an ordinary
/// `unique_violation` this function absorbs, never a policy violation.
async fn ensure_org_ocean(pool: &PgPool, account_id: Uuid) -> Result<(), sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Organization', 'org')",
    )
    .bind(account_id)
    .execute(pool)
    .await;

    match result {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
            // Lost the race (a concurrent build already created it) or this
            // org already has its commons ocean — both are success.
            Ok(())
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod lane_tests {
    use super::*;

    #[test]
    fn the_default_budget_splits_two_and_three() {
        assert_eq!(
            lane_split(DEFAULT_MAX_CONNECTIONS, DEFAULT_PAGE_LANE_CONNECTIONS),
            (2, 3)
        );
    }

    #[test]
    fn neither_lane_is_ever_empty_on_a_budget_of_two_or_more() {
        assert_eq!(
            lane_split(5, 0),
            (1, 4),
            "a zero request still reserves one"
        );
        assert_eq!(lane_split(5, 5), (4, 1), "the work lane keeps at least one");
        assert_eq!(lane_split(5, 9), (4, 1));
        assert_eq!(lane_split(2, 2), (1, 1));
    }

    #[test]
    fn a_one_connection_budget_has_no_page_lane() {
        assert_eq!(lane_split(1, 2), (0, 1));
        assert_eq!(lane_split(0, 2), (0, 0));
    }

    #[test]
    fn env_override_rejects_nonsense_and_zero() {
        std::env::set_var(PAGE_LANE_ENV, "3");
        assert_eq!(page_lane_from_env(), 3);
        std::env::set_var(PAGE_LANE_ENV, "0");
        assert_eq!(page_lane_from_env(), DEFAULT_PAGE_LANE_CONNECTIONS);
        std::env::set_var(PAGE_LANE_ENV, "two");
        assert_eq!(page_lane_from_env(), DEFAULT_PAGE_LANE_CONNECTIONS);
        std::env::remove_var(PAGE_LANE_ENV);
        assert_eq!(page_lane_from_env(), DEFAULT_PAGE_LANE_CONNECTIONS);
    }
}
