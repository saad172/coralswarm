//! Multi-tenant worker harness (org-tenancy PR 2.1): one background-worker
//! loop, N databases.
//!
//! Pre-split, every job worker owned a `loop { dequeue → process → sleep }`
//! against the single shared pool. Database-per-org means the same worker
//! logic must now run against `[consumer] ++ every active tenant DB`, so the
//! loop moves HERE and each worker shrinks to a [`Tick`]-returning function
//! ("process at most one batch on this (pool, store) pair, then return").
//! [`run_multi_tenant`] owns scheduling, tenant discovery, fairness, idle
//! backoff, the schema-version gate, and per-tenant failure isolation.
//!
//! # Which workers join the tenant loop (decision table)
//!
//! | worker                  | decision       | why |
//! |-------------------------|----------------|-----|
//! | `embed`                 | tenant loop    | jobs/atoms/graphs are tenant tables; DEK re-pointed at tenant `account_keys` via `crypto::resolve_dek_for_tenant` |
//! | `embed_retry`           | tenant loop    | sweeps `atoms.embed_failed_at` + enqueues jobs — both tenant-side |
//! | `summary`               | tenant loop    | meeting_transcripts is tenant-side; DEK via `resolve_dek_for_tenant` |
//! | `inference_closure`     | tenant loop    | jobs/relationship_edge tenant-side |
//! | `infer_expertise`       | tenant loop    | jobs/nodes/edges tenant-side |
//! | `entity_resolution`     | tenant loop    | jobs/nodes tenant-side |
//! | `reaper`                | tenant loop    | each tenant DB has its own `jobs` queue that can orphan `running` rows |
//! | `tombstone_purge`       | tenant loop    | `op_log` is tenant-side (tenant Stores emit sync ops on writes) |
//! | `member_ocean_reconciler` | tenant loop  | member-private-oceans PR 3 (provision arm only): diffs control-plane `account_members` against tenant-side `graphs WHERE kind='member'`; a no-op on the consumer plane and behind `MEMBER_OCEANS_ENABLED` (default off) |
//! | `slack_ingest`           | tenant loop    | org-slack step 2: drains `JobKind::SlackMessageIngest` off WHICHEVER plane `routes::slack_events::enqueue_slack_message_ingest` resolved and enqueued onto — the consumer `jobs` table, or a specific org's own tenant `jobs` table; each plane's queue is invisible to a dequeuer scoped to any other one, same reasoning as `reaper`/`tombstone_purge` |
//! | `huddle`                | tenant loop    | org-slack step 4: `routes::slack_events::enqueue_huddle_capture` moved planes in the SAME commit — `JobKind::SlackHuddleCapture` jobs land on whichever plane the huddle-capture trigger resolved; content (`atoms`, `slack_huddle_captures`) writes to `t.pool`, the Slack bot token (`oauth_tokens`, control-only) reads from `control_pool` |
//! | `slack_name_backfill`   | tenant loop    | org-slack step 4: the natural-repair enqueue in `routes::slack_events::persist_identity_and_enqueue_backfill` moved planes in the SAME commit — `JobKind::SlackNameBackfill` jobs land on whichever plane the identity write resolved; `atoms`/`graphs`/`slack_integrations`/`connector_identities` are all content-plane, so this needs `control_pool` ONLY for the account-less seed job's account-roster resolution |
//! | `slack_seat_reconcile`  | tenant loop    | org-slack step 4: `JobKind::SlackSeatReconcile` has no live enqueue site (hand-inserted — see that variant's own doc comment), so this worker simply reads whichever plane's `jobs` table an operator targets; `control_pool` resolves the account roster and the Slack bot token, every seat read/write (`slack_watched_channels`/`teams`/`team_members`/`team_seat_grants`) runs on `t.pool` |
//! | `graph_subscriptions`   | tenant loop    | m365 Train 2 PR 2.2 (the last consumer-only straggler, un-blocked by PR 2.3): two registrations (`tick_renew`/`tick_backstop`, different native cadences) — renewal/ensure-recreate are fully tenant-aware (`graph_subscriptions`/`teams_watched_resources`/`outlook_watched_conversations` on `t.pool`, the Microsoft OAuth token on `control_pool`); the backstop tick's content-ingest step now routes through `routes::graph_webhook::ingest_*`'s plane-aware funnel (PR 2.3), so `TenantId::Org` runs the SAME resync the consumer arm does — see that worker's own module doc |
//! | `graph_ingest`          | tenant loop    | m365 Train 2 PR 2.3: drains `JobKind::GraphMessageIngest` off WHICHEVER plane `routes::graph_webhook::resolve_graph_ingest_target` resolved and enqueued onto — the consumer `jobs` table, or a specific org's own tenant `jobs` table; each plane's queue is invisible to a dequeuer scoped to any other one, same reasoning as `slack_ingest`. No `control_pool` needed: unlike `slack_ingest`, the plane decision already happened once at enqueue time — see that worker's own module doc |
//! | `teams_seat_reconcile`  | tenant loop    | m365 Train 2 PR 2.4: the Teams analog of `slack_seat_reconcile`, tenant-loop-native from birth rather than migrated onto it — `JobKind::TeamsSeatReconcile` has no live enqueue site (hand-inserted), `control_pool` resolves the account roster and the CONTROL-only halves of the roster-identity match, every seat read/write (`teams_watched_resources`/`teams`/`team_members`/`team_seat_grants`) runs on `t.pool`. Its discovery query is deliberately empty in production today — see that worker's own module doc |
//! | `google_watch_renewal`  | tenant loop    | gsuite Train G1 PR 2: the Google analog of `graph_subscriptions`, tenant-loop-native from birth (no legacy single-loop version to migrate away from) — two registrations (`tick_renew`/`tick_backstop`, different native cadences); `google_watch_channels` reads/writes run on `t.pool`, the account's Google OAuth token (`oauth_tokens`, control-only) reads/writes on `control_pool`. No per-surface ingest exists yet (G2/G3/G4 populate rows), so both ticks are structurally live but find nothing to do in production until then — see that worker's own module doc |
//! | `statement_embed_sweep` | tenant loop    | statement vectors (`semantic_atom_embeddings`) and the `EmbedStatements` jobs it enqueues live on each plane's own tables; consumer reads per account, an org tenant as its system principal |
//! | `reembed`               | tenant loop    | leftover missing-cloud rows live on each tenant's `atoms`/`jobs`; empty-queue seed enqueues `reembed_cloud_seed` on **that** pool so org leftovers are not stranded on the consumer plane |
//! | `ingest_receive`        | consumer-only  | ADR-0016's receive stage: `JobKind::IngestReceive` lands on the consumer `jobs` table only (a webhook cannot know the delivery's accounts before the worker resolves them), so org tenant entries idle without a query |
//! | `content_tsv_backfill`  | consumer-only  | one-shot repair of migration-0062-era rows; those rows exist only on the shared plane |
//!
//! # The consumer pool
//!
//! The *consumer pool* is the existing `AppState.db` pool — one physical
//! control-plane Postgres serving both the control plane (identity/routing) and
//! the personal-content plane. The [`TenantRegistry`] does not own it, so it
//! is passed explicitly. It doubles as the control pool the harness reads
//! `tenant_databases` from.
//!
//! # Flag off ⇒ today's behavior
//!
//! With `MULTITENANT_ENABLED` unset (`runtime` is `None` or
//! `runtime.enabled == false`) the tenant list is exactly `[consumer]` and the
//! harness never queries `tenant_databases` (or anything else beyond what the
//! worker tick itself does). The consumer schedule reproduces the old
//! per-worker loop: work found → poll again immediately (queue workers) /
//! after the native interval (sweeps); queue empty → sleep `base_poll`
//! (the worker's old `EMPTY_POLL_SLEEP`); error → sleep `error_sleep` (the
//! old `DB_ERROR_SLEEP`). Idle backoff NEVER applies to the consumer plane —
//! the control-plane Postgres does not scale to zero and personal-user job latency must not
//! regress.
//!
//! # Idle backoff × Neon scale-to-zero (REQUIRED reading before tuning)
//!
//! Unthrottled, every tenant-loop worker would empty-poll every tenant DB at
//! its native 100–500ms cadence — multiplied across N Neon tenants that keeps
//! every tenant compute awake 24/7 and multiplies steady-state DB load ~N×,
//! defeating Neon's scale-to-zero entirely. Instead, per (worker, tenant):
//!
//! * an org tenant whose queue polls empty gets **exponential backoff**
//!   (double per consecutive empty poll) from `base_poll` up to
//!   [`DORMANT_POLL_PERIOD`];
//! * once fully backed off, polls **align to shared slots**: the slot clock is
//!   the tenant's process-wide last-found-work instant ([`TenantActivity`],
//!   shared by every harness in the process), so all dormant-tenant polls
//!   land in one short burst every `DORMANT_POLL_PERIOD` instead of smearing
//!   ~10 unaligned polls across it (which would put a query on the tenant DB
//!   every 1–2 minutes — permanently above Neon's inactivity threshold);
//! * any worker finding work resets the shared clock, and every OTHER
//!   wake-enabled worker's scheduler **wakes escalated entries on that clock**: each pass
//!   pulls forward any org entry whose backoff is escalated but whose shared
//!   clock shows work within [`RECENT_ACTIVITY_WINDOW`], and the idle branch
//!   re-checks the clock before escalating — so when embed finds a dormant
//!   tenant's first job, extract/relationships/etc. snap back to `base_poll`
//!   within one scheduler pass instead of waiting out their own already-
//!   scheduled 20-minute slots (which would have made a k-stage pipeline
//!   k × `DORMANT_POLL_PERIOD` end-to-end).
//!
//! Neon arithmetic at defaults: sqlx closes idle connections after ~10 min
//! (`idle_timeout` default), so a dormant tenant sees one query burst, ~10 min
//! of idle-but-open connections, then ~10 min with zero connections — clearing
//! Neon's 5-minute autosuspend threshold for roughly the back half of every
//! [`DORMANT_POLL_PERIOD`] cycle. Each burst briefly resumes the compute.
//! True zero-poll dormancy needs an enqueue-side signal (e.g. a
//! `tenant_databases.last_enqueue_at` bumped on ingest so only tenants with
//! plausible work get polled) — that is a **control-plane migration**, and
//! migration numbers are being claimed by concurrent org-tenancy PRs under a
//! gapless-numbering CI guard, so it is deliberately deferred; this harness is
//! designed so it can be added later purely inside `refresh_tenants` (skip
//! dormant tenants whose `last_enqueue_at` predates their last poll).
//!
//! ## Enqueue-side wake (the `DORMANT_POLL_PERIOD` latency ceiling)
//!
//! This doc used to end the paragraph above with: worst-case latency for a
//! fully dormant tenant is one `DORMANT_POLL_PERIOD` for the FIRST pipeline
//! stage to pick up a job, downstream stages then follow at `base_poll` via
//! the cross-worker wake — "acceptable for concierge-scale partners, revisit
//! with the enqueue-side signal before that stops being true."
//!
//! **That stopped being true when Slack ingest moved onto the tenant loop**
//! (org-slack step 2). The accepted trade assumed the job's producer was
//! ANOTHER STAGE OF THIS HARNESS, where a k-stage pipeline costs one dormant
//! slot total and nothing user-visible is waiting. A Slack webhook is a
//! different shape: the producer is outside the harness entirely, and a person
//! is watching. Measured in production: a message took **11m04s** to ingest;
//! the ingest itself ran in 340ms.
//!
//! So the enqueue-side signal exists now — as [`TenantActivity::note_enqueue`],
//! in process rather than in the control plane. Producers that write a `jobs`
//! row for an org tenant bump a per-org generation counter; the pre-pass in
//! [`run_multi_tenant`] pulls wake-enabled entries whose generation moved
//! to due-now ([`wake_reason`]). Wake latency is one [`SHUTDOWN_POLL_INTERVAL`] (~500ms),
//! because the scheduler already re-evaluates every entry that often
//! regardless of backoff. Queue workers opt in by default; periodic sweeps
//! retain their cadence unless explicitly opted in. Failure backoff and hard
//! backpressure pauses take precedence over both enqueue and peer wakes.
//!
//! This does NOT weaken the Neon arithmetic above. The backoff, the dormant
//! slots and their alignment are all untouched; a woken tenant is one whose
//! compute has genuine work to do, which needs it resumed anyway. It is also
//! strictly cheaper than the deferred `last_enqueue_at` design — no migration
//! and no control-plane write per message — and composes with it: that column
//! would let `refresh_tenants` skip polling dormant tenants entirely, with
//! `note_enqueue` supplying the wake.
//!
//! **Limit worth knowing.** The signal is process-local. A job enqueued by a
//! producer in ANOTHER process (or on another Fly machine) does not wake this
//! one, and falls back to the poll ceiling. Fine today — the Slack webhook
//! handler and the workers are the same binary, so the machine that takes the
//! webhook is the machine that wakes — but a producer/worker split would need
//! the DB-backed signal instead.
//!
//! Note the interaction with [`TenantRegistry`]'s idle pool eviction
//! (30 min TTL): dormant-tenant polls every [`DORMANT_POLL_PERIOD`] < 30 min
//! touch the pool, so pools for listed-active tenants stay resident by design
//! (cold-start cost is paid once); the periodic `evict_idle` sweep spawned in
//! `lib.rs::run` reaps pools for tenants that LEAVE the active list
//! (suspended/offboarded — they stop being scheduled here, go untouched, and
//! age out within the TTL + sweep interval).
//!
//! # Hung-tenant containment
//!
//! A tenant DB that HANGS (network partition / SYN blackhole) rather than
//! refusing fast must not head-of-line-block this single-task scheduler —
//! the consumer plane and every healthy tenant share it. Two bounds: pool
//! builds are wrapped in [`HarnessConfig::pool_build_timeout`] (default
//! [`DEFAULT_POOL_BUILD_TIMEOUT`]), and registry pools carry a short
//! `acquire_timeout` (`registry::DEFAULT_ACQUIRE_TIMEOUT`, vs sqlx's 30s
//! default) so a cached pool whose connections died also fails fast. A hung
//! tenant therefore costs at most ~one `pool_build_timeout` per retry — and
//! its error backoff escalates toward `max_org_idle_backoff`, so retries
//! thin out. (A statement that hangs on an already-established, half-dead
//! socket mid-tick is NOT bounded here; workers keep their own per-call
//! timeouts where that matters.)
//!
//! # Schema-version gate
//!
//! Before a tenant is ever polled, its routing row must satisfy
//! `tenant_databases.schema_version >= tenancy::migrate::head_version()` (the
//! version this binary's embedded tenant tree — and therefore its queries —
//! assumes; `migrate_tenants` records it after each successful apply). A
//! lagging tenant is skipped with a warning — the worker-side equivalent of
//! the gate's per-tenant 503 — and NO queries run against it until the
//! orchestrator catches it up. (The plan doc says `tenant_meta.schema_version`;
//! the column actually lives on the control routing row, which is cheaper too:
//! the gate rides the same `list_active` read as tenant discovery.)
//!
//! # Fairness & failure isolation
//!
//! Each scheduler pass runs at most one tick — one bounded batch (the
//! worker's own `JOB_BATCH_SIZE`) — per due tenant, in sequence, so one
//! backlogged tenant cannot starve the others (round-robin by construction).
//! A tick error or panic is caught, logged, and backs off THAT tenant only;
//! every other tenant (and the consumer plane) keeps processing. A tenant
//! whose pool cannot be built (Neon down, secret missing) is likewise backed
//! off and retried — never fatal to the harness.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::FutureExt;
use sqlx::PgPool;

use crate::shutdown::Shutdown;
use crate::store::{PostgresStore, Store};
use crate::tenancy::read_only_poison::{self, ReadOnlyLedger};
use crate::tenancy::routing::{self, TenantDatabase};
use crate::tenancy::runtime::TenantRuntime;
use crate::tenancy::TenantId;

// ── Tuning constants ─────────────────────────────────────────────────────────

/// Ceiling of the per-(worker, tenant) idle backoff AND the aligned dormant
/// poll period. See the module doc's Neon-arithmetic section before changing:
/// it must comfortably exceed sqlx's idle connection timeout (~10 min) plus
/// Neon's autosuspend threshold (5 min default) or dormant tenants never
/// actually suspend.
pub const DORMANT_POLL_PERIOD: Duration = Duration::from_secs(20 * 60);

/// Consecutive pool-build failures for one org before the harness escalates
/// from `warn!` to an alerting `error!`.
///
/// Deliberately small. Because the backoff doubles, five failures is already
/// several minutes of an org getting no background work at all, and the failure
/// is invisible from the outside — nothing 500s, nothing is queued-and-stuck,
/// the org simply looks idle. That is exactly how a pooled-DSN incident went
/// unnoticed: every worker logged a `warn!` every few seconds and the volume
/// read as noise rather than as an outage.
pub const POOL_FAILURE_ALERT_THRESHOLD: u32 = 5;

/// A tick this long is worth a warning: if it was CPU-bound on the async
/// runtime, every request on the process stalled with it (COR-96).
pub const SLOW_TICK: Duration = Duration::from_secs(30);

/// How loudly the Nth consecutive pool-build failure for one org should be
/// reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PoolFailureSeverity {
    /// Below the threshold — a transient blip should still be visible.
    Warn,
    /// Crossing the threshold — alert, exactly once per outage.
    Alert,
    /// Past the threshold — already alerted, stay quiet.
    Quiet,
}

/// Single source of truth for the escalation shape, so the production logging
/// and its test cannot drift apart. A test that re-implemented this match would
/// keep passing while production regressed — which is exactly how the first cut
/// of this shipped a warn-on-every-attempt path that read as an alert.
pub(crate) fn pool_failure_severity(failures: u32) -> PoolFailureSeverity {
    match failures.cmp(&POOL_FAILURE_ALERT_THRESHOLD) {
        std::cmp::Ordering::Less => PoolFailureSeverity::Warn,
        std::cmp::Ordering::Equal => PoolFailureSeverity::Alert,
        std::cmp::Ordering::Greater => PoolFailureSeverity::Quiet,
    }
}

/// Drop failure counters for orgs no longer in the active set.
///
/// `refresh_tenants` removes suspended, offboarded and schema-gated orgs from
/// `entries`, but a counter left behind outlives them. If such an org comes back
/// — un-suspended, or its schema catches up — a stale count at or past the
/// threshold would put its NEXT outage straight into [`PoolFailureSeverity::
/// Quiet`], suppressing the alert for a genuinely new failure. Counters are
/// per-outage state, so they die with the entry.
fn prune_pool_failures(pool_failures: &mut HashMap<String, u32>, entries: &HashMap<TenantId, Entry>) {
    pool_failures.retain(|org, _| entries.contains_key(&TenantId::Org(org.clone())));
}

/// How often the active-tenant list (and each row's schema-version gate) is
/// re-read from the control plane. One cheap indexed read per worker per
/// interval; also the max staleness of suspend/offboard taking effect on
/// workers.
pub const DEFAULT_TENANT_REFRESH: Duration = Duration::from_secs(30);

/// Floor between discovery passes that a wake for an unscheduled org starts.
/// Bounds the extra `tenant_databases` reads when such an org keeps waking
/// without ever being schedulable (its schema lags this binary).
pub const MIN_WAKE_DISCOVERY: Duration = Duration::from_secs(1);

/// Default post-error sleep — matches the `DB_ERROR_SLEEP = 5s` every
/// converted worker used, so flag-off error behavior is unchanged.
pub const DEFAULT_ERROR_SLEEP: Duration = Duration::from_secs(5);

/// How recently (per the shared [`TenantActivity`] clock) ANY worker must have
/// found work for a tenant for this worker to treat it as active: an escalated
/// entry inside this window snaps back to `base_poll` instead of escalating
/// further or waiting out its dormant slot. Half the dormant period: long
/// enough that a multi-stage pipeline (embed → extract → relationships → …)
/// completes at fast cadence once its first stage wakes, short enough that a
/// genuinely-idle tenant re-enters dormancy well within one period. Side
/// effect (accepted): a process restart re-seeds every tenant's clock, so all
/// tenants poll fast for this window after a deploy before backing off.
pub const RECENT_ACTIVITY_WINDOW: Duration = Duration::from_secs(10 * 60);

/// Default bound on a tenant pool build (resolve secret → connect →
/// fingerprint query). A tenant DB that HANGS instead of refusing would
/// otherwise park the shared scheduler task for the full connect/acquire
/// timeout on every retry — see the module doc's hung-tenant section.
pub const DEFAULT_POOL_BUILD_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound on any single scheduler sleep so a shutdown signal is noticed
/// promptly even when every tenant is deep in backoff (same contract as the
/// old sweep workers' `SHUTDOWN_POLL_INTERVAL`).
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

// ── Tick surface (what a worker implements) ──────────────────────────────────

/// One tenant's (pool, store) pair, handed to a worker tick. `store` wraps the
/// SAME pool (`PostgresStore::new(pool)` for tenants; the shared consumer
/// store on the consumer plane), so a tick can never mix planes by
/// construction. `tenant` is the DEK dimension — tenant-loop workers that
/// decrypt must thread it into `crypto::resolve_dek_for_tenant` (org DEKs
/// live in tenant `account_keys`, NOT control `accounts.dek_wrapped`).
///
/// **Tenant RLS (Category C).** On an ORG tenant, `pool`/`store` are built
/// from [`crate::tenancy::registry::TenantRegistry::get_or_build_worker_pool`]
/// — still a pool separate from the request pool, for its own connection
/// budget and idle eviction, but no longer a connection-scoped
/// `app.tenant_actor=system` pool.
///
/// That session-level GUC was set through the connection STARTUP packet, which
/// a connection pooler rejects outright, so it broke every org tenant on a
/// pooled DSN. Ambient queries a tick runs against this `pool` (or through
/// `store`) therefore satisfy `tenant_rls_is_system()` **only** by virtue of
/// the app still connecting as the table owner, which `0009` §7 leaves exempt
/// by deliberately not FORCE-ing row security. That dependency is asserted at
/// pool-build time — see `TenantPoolError::WorkerPoolNotRlsExempt`.
///
/// Ticks are being converted to open a short
/// [`crate::tenancy::scope::begin_tenant_system_scoped`] transaction per unit
/// of work — never one per tick, which would hold a connection across the LLM
/// and R2 calls several of these workers make. Until that conversion lands,
/// this pool's correctness rests on the owner exemption alone.
///
/// On the consumer plane `pool` is unchanged (the existing `AppState.db` pool;
/// `0009_tenant_rls` does not apply there at all).
#[derive(Clone)]
pub struct WorkerTenant {
    pub tenant: TenantId,
    pub pool: PgPool,
    pub store: Arc<dyn Store>,
}

/// What a single tick accomplished — drives the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    /// Jobs (or sweep items) processed. `> 0` counts as tenant activity:
    /// resets idle backoff and (for queue workers) re-polls immediately.
    pub processed: usize,
    /// Explicit delay overriding the idle/hot schedule. Hard backpressure
    /// pauses resist wakes; known-work deadlines can be interrupted by new
    /// work when the worker opts in. Leaves idle backoff state untouched.
    pub defer: Option<TickDelay>,
}

/// Whether a tick delay can be interrupted by newly enqueued work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickDelay {
    /// Retrying sooner will hurt: wakes must wait out this pause.
    Backpressure(Duration),
    /// Earliest currently-known work; a new enqueue may be due sooner.
    UntilNextWork(Duration),
}

impl Tick {
    /// Queue was empty — idle path (consumer: `base_poll`; org: backoff).
    pub fn idle() -> Self {
        Tick { processed: 0, defer: None }
    }
    /// Processed `n` items.
    pub fn worked(n: usize) -> Self {
        Tick { processed: n, defer: None }
    }
    /// Backpressure/etc.: park this tenant for `d` regardless of outcome.
    pub fn deferred(d: Duration) -> Self {
        Tick { processed: 0, defer: Some(TickDelay::Backpressure(d)) }
    }
    /// Wait for known work, allowing an opted-in enqueue wake to interrupt.
    pub fn until_next_work(processed: usize, d: Duration) -> Self {
        Tick { processed, defer: Some(TickDelay::UntilNextWork(d)) }
    }
}

/// Per-worker harness configuration.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    /// Worker name for logs.
    pub name: &'static str,
    /// Idle poll interval on the consumer plane / backoff floor for tenants —
    /// the worker's old `EMPTY_POLL_SLEEP` (queue workers) or sweep interval.
    pub base_poll: Duration,
    /// Post-error sleep (the old `DB_ERROR_SLEEP`).
    pub error_sleep: Duration,
    /// `true` (queue workers): work found ⇒ poll again immediately, matching
    /// the old `loop` shape where a non-empty dequeue skipped the sleep.
    /// `false` (periodic sweeps): work found ⇒ still wait `base_poll` — a
    /// reaper that reaped something must not tight-loop.
    pub hot_loop_on_work: bool,
    /// Respond to enqueue and peer-activity wakes. Queue workers opt in;
    /// periodic sweeps retain their native cadence unless explicitly enabled.
    pub wake_on_enqueue: bool,
    /// Idle-backoff ceiling for org tenants (never applied to the consumer).
    pub max_org_idle_backoff: Duration,
    /// Active-tenant list refresh cadence.
    pub tenant_refresh: Duration,
    /// Bound on a tenant pool build inside the scheduler pass — see the
    /// module doc's hung-tenant section. Never applies to the consumer pool
    /// (it is passed in already built).
    pub pool_build_timeout: Duration,
    /// Also schedule orgs that Clerk deleted (an `org_revocations` row) while
    /// their tenant database is still `active`. `false` for every worker but
    /// the member-ocean reconciler, whose archive arm must still stamp
    /// `archived_at` and revoke share tokens for a deleted org's members
    /// (#1396 review). See [`list_scheduled_tenants`].
    pub include_revoked_orgs: bool,
}

impl HarnessConfig {
    /// Config for a `jobs`-queue worker with empty-poll interval `base_poll`.
    pub fn queue_worker(name: &'static str, base_poll: Duration) -> Self {
        HarnessConfig {
            name,
            base_poll,
            error_sleep: DEFAULT_ERROR_SLEEP,
            hot_loop_on_work: true,
            wake_on_enqueue: true,
            max_org_idle_backoff: DORMANT_POLL_PERIOD,
            tenant_refresh: DEFAULT_TENANT_REFRESH,
            pool_build_timeout: DEFAULT_POOL_BUILD_TIMEOUT,
            include_revoked_orgs: false,
        }
    }

    /// Config for a periodic sweep worker with native cadence `interval`.
    /// Org backoff still applies when the sweep interval is shorter than the
    /// dormant period (e.g. the reaper's 5 min), capped at whichever is
    /// larger, so sweeps don't keep a dormant Neon compute awake either.
    pub fn sweep_worker(name: &'static str, interval: Duration) -> Self {
        HarnessConfig {
            name,
            base_poll: interval,
            error_sleep: DEFAULT_ERROR_SLEEP,
            hot_loop_on_work: false,
            wake_on_enqueue: false,
            max_org_idle_backoff: interval.max(DORMANT_POLL_PERIOD),
            tenant_refresh: DEFAULT_TENANT_REFRESH,
            pool_build_timeout: DEFAULT_POOL_BUILD_TIMEOUT,
            include_revoked_orgs: false,
        }
    }

    /// Opt this worker into [`HarnessConfig::include_revoked_orgs`].
    pub fn including_revoked_orgs(mut self) -> Self {
        self.include_revoked_orgs = true;
        self
    }
}

// ── Shared per-tenant activity clock ─────────────────────────────────────────

/// Process-wide, cross-worker "when did ANY worker last find work for this
/// tenant" clock. One instance is created in `lib.rs::run` and shared by every
/// harness so (a) one worker finding work (e.g. embed) snaps the downstream
/// workers (extract, relationships…) back to fast polling — job stages are
/// causally chained, so this is signal, not noise — and (b) fully-dormant
/// polls align to a common slot boundary (see the module doc's Neon section).
///
/// It also carries the per-tenant [`ReadOnlyLedger`], because it is already
/// the one per-tenant signal every harness and `AppState` share.
#[derive(Default)]
pub struct TenantActivity {
    /// SQLSTATE 25006 rejections and the backends discarded in response,
    /// per tenant (COR-82). Read by `/internal/admin/chain-health`.
    pub read_only: ReadOnlyLedger,
    last_work: DashMap<String, Instant>,
    /// Monotonic per-org counter bumped by [`TenantActivity::note_enqueue`]
    /// every time work is enqueued for a tenant from OUTSIDE the harness.
    ///
    /// A generation counter rather than a consumable flag on purpose: every
    /// wake-enabled harness reads the same value and compares it against its own
    /// `Entry::seen_wake`, so one enqueue wakes all of a tenant's opted-in workers.
    /// A "take the flag" design would let whichever worker polled first
    /// swallow the wake and leave the other twelve asleep — and the worker
    /// that swallows it is usually the wrong one, since the enqueuer does not
    /// know (and should not know) which worker consumes which `JobKind`.
    wake_gen: DashMap<String, u64>,
    /// Cross-process forwarder for [`TenantActivity::note_enqueue`] (#1495).
    ///
    /// Empty in the default `BACKEND_ROLE=all` process, where the harness that
    /// reads `wake_gen` lives in this same process. A `BACKEND_ROLE=http`
    /// process has no harness, so a local bump would wake nothing; there
    /// `workers::wake_bridge::spawn_publisher` installs a sender here and
    /// relays each org id to the worker process over Postgres `NOTIFY`.
    remote_wake: std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<String>>,
}

/// `wake_gen` key for the consumer plane. A Clerk org id is `org_…`, so this
/// can never collide with a tenant; it is a plain string so the cross-process
/// relay in `wake_bridge` carries it exactly like an org id.
pub const CONSUMER_WAKE: &str = "~consumer";

impl TenantActivity {
    pub fn new() -> Arc<Self> {
        Arc::new(TenantActivity::default())
    }

    fn note_work(&self, org: &str) {
        self.last_work.insert(org.to_string(), Instant::now());
    }

    /// Signal that work was ENQUEUED for `org` by something outside the
    /// harness — a webhook handler, a request path, any producer that writes
    /// a `jobs` row and then returns.
    ///
    /// Without this the harness has no idea the row appeared. Its only other
    /// wake ([`TenantActivity::note_work`]) fires when a worker FINDS work,
    /// which is sound for harness-generated pipelines (embed's find snaps
    /// extract back to fast polling) but says nothing about work created from
    /// outside. An org tenant that has been idle long enough to escalate is
    /// asleep for up to [`DORMANT_POLL_PERIOD`], so an externally-enqueued job
    /// waits out whatever slot happens to be in flight — uniform 0-20 min,
    /// mean ~10. That is precisely how a Slack message took 11 minutes to
    /// ingest while the ingest itself took 340ms.
    ///
    /// Cheap by construction: the scheduler already re-evaluates every entry
    /// each [`SHUTDOWN_POLL_INTERVAL`], so this costs one `DashMap` bump and
    /// buys ~500ms wake latency. It does NOT defeat Neon autosuspend — the
    /// tenant DB is touched only because there is genuinely work for it, which
    /// needs the compute awake anyway.
    ///
    /// Consumer-plane producers call [`TenantActivity::note_consumer_enqueue`]
    /// instead. That plane never escalates, but a queue worker's idle cadence
    /// there is still its `base_poll` (5 s for `read_model_refresh`), and a
    /// request that just enqueued a refresh has a browser polling for it.
    pub fn note_enqueue(&self, org: &str) {
        self.wake_gen
            .entry(org.to_string())
            .and_modify(|g| *g = g.wrapping_add(1))
            .or_insert(1);
        if let Some(tx) = self.remote_wake.get() {
            // Unbounded + non-blocking on purpose: callers are request and
            // webhook handlers. A closed receiver (publisher gone at shutdown)
            // just drops the wake; the dormant poll is the backstop.
            let _ = tx.send(org.to_string());
        }
    }

    /// [`TenantActivity::note_enqueue`] for work that landed on the consumer
    /// (personal/control) pool.
    pub fn note_consumer_enqueue(&self) {
        self.note_enqueue(CONSUMER_WAKE);
    }

    /// Test-only view of the enqueue generation for `org` (#1495 wake bridge
    /// tests observe a cross-process wake through it).
    #[cfg(any(test, feature = "test-support"))]
    pub fn enqueue_generation(&self, org: &str) -> u64 {
        self.wake_generation(org)
    }

    /// Install the cross-process forwarder (see `remote_wake`). Returns
    /// `false` if one was already installed; the first stays in place.
    pub fn forward_enqueues_to(&self, tx: tokio::sync::mpsc::UnboundedSender<String>) -> bool {
        self.remote_wake.set(tx).is_ok()
    }

    /// Current enqueue generation for `org` (0 = never enqueued-for).
    fn wake_generation(&self, org: &str) -> u64 {
        self.wake_gen.get(org).map(|g| *g.value()).unwrap_or(0)
    }

    /// Wake generations of orgs that `scheduled` does not report. A tenant
    /// provisioned since the last discovery pass receives its first enqueue
    /// before any harness schedules it, and `apply_wake` only reaches entries.
    fn unscheduled_wakes(&self, scheduled: impl Fn(&str) -> bool) -> HashMap<String, u64> {
        self.wake_gen
            .iter()
            .filter(|g| g.key() != CONSUMER_WAKE && !scheduled(g.key()))
            .map(|g| (g.key().clone(), *g.value()))
            .collect()
    }

    /// Time since any worker last found work for `org`; first sight counts as
    /// "just worked" so brand-new tenants start on the fast schedule.
    fn since_last_work(&self, org: &str, now: Instant) -> Duration {
        let entry = self
            .last_work
            .entry(org.to_string())
            .or_insert_with(|| now);
        now.saturating_duration_since(*entry.value())
    }

    /// Drop clocks for tenants no longer active (bounds the map).
    ///
    /// `wake_gen` is pruned alongside `last_work` for the same reason
    /// [`prune_pool_failures`] drops its counters: a generation left behind
    /// outlives its tenant, and if that org comes back its fresh `Entry` would
    /// seed `seen_wake` from a stale count. Harmless either way (a mismatch
    /// only costs one early poll), but per-tenant state should die with the
    /// tenant rather than accumulate for the process's lifetime. The consumer
    /// plane is never in `live` and never leaves, so its key stays.
    fn retain(&self, live: &HashSet<String>) {
        self.last_work.retain(|k, _| live.contains(k));
        self.wake_gen
            .retain(|k, _| k == CONSUMER_WAKE || live.contains(k));
    }
}

// ── Idle backoff (unit-testable, no clock inside) ────────────────────────────

/// Per-(worker, tenant) exponential idle backoff: doubles per consecutive
/// empty poll from `base` to `max`; any work resets it.
#[derive(Debug, Clone)]
pub(crate) struct IdleBackoff {
    base: Duration,
    max: Duration,
    cur: Option<Duration>,
}

impl IdleBackoff {
    pub(crate) fn new(base: Duration, max: Duration) -> Self {
        IdleBackoff { base, max: max.max(base), cur: None }
    }

    /// Record an empty poll; returns the interval to wait before the next one.
    pub(crate) fn on_empty(&mut self) -> Duration {
        let next = match self.cur {
            None => self.base,
            Some(c) => c.saturating_mul(2).min(self.max),
        };
        self.cur = Some(next);
        next
    }

    /// Record that work was found — next empty poll starts back at `base`.
    pub(crate) fn on_work(&mut self) {
        self.cur = None;
    }

    /// Whether the backoff has hit its ceiling (⇒ use aligned dormant slots).
    pub(crate) fn saturated(&self) -> bool {
        self.cur == Some(self.max)
    }

    /// Whether the backoff has escalated beyond its floor at all — i.e. this
    /// worker has stopped fast-polling the tenant. Used by the cross-worker
    /// wake: an escalated entry whose shared activity clock turns recent is
    /// snapped back to `base` (see [`org_idle_delay`] and the pre-pass wake in
    /// [`run_multi_tenant`]).
    pub(crate) fn is_escalated(&self) -> bool {
        self.cur.is_some_and(|c| c > self.base)
    }
}

/// Compute an org tenant's next idle delay after an empty poll, honoring the
/// shared activity clock (the P1 cross-worker wake): work found ANYWHERE for
/// this tenant within [`RECENT_ACTIVITY_WINDOW`] keeps THIS worker at
/// `base_poll` (resetting its escalation) so a pipeline whose first stage
/// woke keeps flowing; otherwise escalate exponentially, and once saturated
/// switch to slot-aligned dormant polling.
pub(crate) fn org_idle_delay(
    backoff: &mut IdleBackoff,
    since_last_work: Duration,
    dormant_period: Duration,
) -> Duration {
    if since_last_work < RECENT_ACTIVITY_WINDOW {
        backoff.on_work();
        return backoff.on_empty(); // == base
    }
    let esc = backoff.on_empty();
    if backoff.saturated() {
        dormant_slot_delay(since_last_work, dormant_period)
    } else {
        esc
    }
}

/// Why an entry is being pulled forward to "due now" before the due-check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wake {
    /// Work was ENQUEUED for this tenant from outside the harness since this
    /// worker's last pass (a webhook, a request path). There is work waiting
    /// right now.
    Enqueued,
    /// An enqueue is outstanding but a failure or hard backpressure pause
    /// holds it.
    /// Leave the schedule alone and — critically — do NOT advance `seen_wake`,
    /// so the wake is re-evaluated next pass and applied the moment the
    /// hold expires. Re-checking costs one map read, no I/O.
    EnqueuedButHeld,
    /// Another worker found this tenant work recently, so a causally-chained
    /// pipeline should keep flowing at fast cadence.
    PeerActivity,
    /// Nothing to do — leave the entry on its existing schedule.
    None,
}

/// Decide whether an entry should be pulled forward, and why.
///
/// [`Wake::Enqueued`] is checked FIRST and is deliberately not gated on
/// `is_escalated`, unlike [`Wake::PeerActivity`]. That gate is an optimisation
/// for a signal the harness generates about itself; an external enqueue means
/// "there is work RIGHT NOW" and may interrupt idle or known-work delays.
/// The caller gates both wake paths on the worker policy. Order also
/// matters for correctness: the caller only advances `seen_wake` on
/// `Enqueued`, so returning `PeerActivity` in preference would leave the
/// generation unconsumed and re-fire the same wake every pass.
pub(crate) fn wake_reason(
    seen_wake: u64,
    generation: u64,
    is_escalated: bool,
    since_last_work: Duration,
    in_wake_hold: bool,
) -> Wake {
    if generation != seen_wake {
        if in_wake_hold {
            Wake::EnqueuedButHeld
        } else {
            Wake::Enqueued
        }
    } else if in_wake_hold {
        // Neither peer activity nor enqueue can shorten a failure backoff
        // or a hard backpressure pause.
        Wake::None
    } else if is_escalated && since_last_work < RECENT_ACTIVITY_WINDOW {
        Wake::PeerActivity
    } else {
        Wake::None
    }
}

/// Delay until the next aligned dormant-poll slot: slots sit at whole
/// multiples of `period` on a clock starting at the tenant's shared
/// last-found-work instant, so every worker's dormant polls for one tenant
/// land together (one DB wake per period, not ~10 smeared ones).
pub(crate) fn dormant_slot_delay(since_last_work: Duration, period: Duration) -> Duration {
    let p = period.as_millis().max(1);
    let rem = since_last_work.as_millis() % p;
    Duration::from_millis((p - rem) as u64)
}

// ── The harness ──────────────────────────────────────────────────────────────

struct Entry {
    next_due: Instant,
    backoff: IdleBackoff,
    /// Routing row (None for the consumer entry). Refreshed every
    /// `tenant_refresh`; status changes take effect via the list membership,
    /// and a changed `db_secret_ref` takes effect because
    /// `TenantRegistry::get_or_build` compares it against the cached pool's
    /// build-time ref and rebuilds on mismatch.
    row: Option<TenantDatabase>,
    /// Last [`TenantActivity`] enqueue generation this worker has already
    /// reacted to. A newer generation upstream means someone enqueued work for
    /// this tenant since our last pass ⇒ pull the entry forward to due-now.
    /// Per-entry (not shared) so one enqueue wakes every opted-in worker once.
    /// The [`TenantId::Consumer`] entry tracks the [`CONSUMER_WAKE`] generation.
    seen_wake: u64,
    /// When the currently-scheduled FAILURE backoff expires, if this entry is
    /// in one (pool build failed / tick returned `Err` / tick panicked).
    /// `None` once a tick succeeds.
    ///
    /// Distinct from `backoff` even though the failure paths borrow that same
    /// counter to size their delay, because the two mean different things:
    /// idle backoff says "there was nothing to do", failure backoff says
    /// "trying again immediately will hurt". An enqueue wake overrides the
    /// former by design — that is this PR's entire point — and must NOT
    /// override the latter, or a tenant whose Neon pool is refusing
    /// connections gets retried on every scheduler pass for as long as its
    /// Slack workspace keeps producing messages, which is exactly the hammer
    /// `error_sleep` and the doubling exist to prevent.
    failure_until: Option<Instant>,
    /// Hard backpressure floor established by `Tick::deferred`. Wakes must
    /// preserve it, leaving pending enqueue generations unconsumed.
    not_before: Option<Instant>,
}

impl Entry {
    /// Whether a failure backoff is still in flight at `now`.
    fn in_failure_backoff(&self, now: Instant) -> bool {
        self.failure_until.is_some_and(|until| until > now)
    }

    /// Apply the same wake policy to both external enqueues and peer activity.
    fn apply_wake(
        &mut self,
        cfg: &HarnessConfig,
        generation: u64,
        since_last_work: Duration,
        now: Instant,
    ) {
        if !cfg.wake_on_enqueue {
            return;
        }
        let in_hold =
            self.in_failure_backoff(now) || self.not_before.is_some_and(|until| until > now);
        match wake_reason(
            self.seen_wake,
            generation,
            self.backoff.is_escalated(),
            since_last_work,
            in_hold,
        ) {
            Wake::Enqueued => {
                self.seen_wake = generation;
                self.backoff.on_work();
                self.next_due = now;
            }
            Wake::PeerActivity => {
                self.backoff.on_work();
                self.next_due = now;
            }
            // Preserve the pending generation until the hold expires.
            Wake::EnqueuedButHeld | Wake::None => {}
        }
    }

    /// The pre-pass wake for `key`'s entry: an enqueue on either plane, plus
    /// peer activity for orgs.
    fn apply_tenant_wake(
        &mut self,
        key: &TenantId,
        cfg: &HarnessConfig,
        activity: &TenantActivity,
        now: Instant,
    ) {
        let (generation, since_last_work) = match key {
            // The consumer plane has no shared activity clock and never
            // escalates, so there is no peer-activity wake to apply; `MAX`
            // keeps `wake_reason` from ever reading it as recent.
            TenantId::Consumer => (activity.wake_generation(CONSUMER_WAKE), Duration::MAX),
            TenantId::Org(org) => (
                activity.wake_generation(org),
                activity.since_last_work(org, now),
            ),
        };
        self.apply_wake(cfg, generation, since_last_work, now);
    }

    /// Schedule a successful tick, preserving consumer and org idle behavior.
    fn schedule_success(
        &mut self,
        tick: Tick,
        key: &TenantId,
        cfg: &HarnessConfig,
        activity: &TenantActivity,
        now: Instant,
    ) {
        self.failure_until = None;
        self.not_before = None;
        if tick.processed > 0 {
            self.backoff.on_work();
            if let TenantId::Org(org) = key {
                activity.note_work(org);
            }
        }
        self.next_due = match tick.defer {
            Some(TickDelay::Backpressure(d)) => {
                let until = now + d;
                self.not_before = Some(until);
                until
            }
            Some(TickDelay::UntilNextWork(d)) => now + d,
            None if tick.processed > 0 => {
                if cfg.hot_loop_on_work {
                    now
                } else {
                    now + cfg.base_poll
                }
            }
            None => match key {
                TenantId::Consumer => now + cfg.base_poll,
                TenantId::Org(org) => {
                    now + org_idle_delay(
                        &mut self.backoff,
                        activity.since_last_work(org, now),
                        cfg.max_org_idle_backoff,
                    )
                }
            },
        };
    }
}

/// Run `tick_fn` forever across `[consumer] ++ active tenants`, until
/// `shutdown` — see the module doc for scheduling semantics. The in-flight
/// tick is never cancelled on shutdown (same drain contract as the old worker
/// loops: stop claiming new work, let the current batch finish, and the
/// caller's grace window bounds the rest).
///
/// `consumer_pool`/`consumer_store` are the existing `AppState.db`/`.store`
/// pair (the store is constructed from the pool unconditionally in
/// `lib.rs::run`, so requiring both here does not narrow any worker's
/// availability). `runtime` carries the feature flag and the
/// `TenantRegistry`; `None` or `enabled == false` means consumer-only.
pub async fn run_multi_tenant<F, Fut>(
    consumer_pool: PgPool,
    consumer_store: Arc<dyn Store>,
    runtime: Option<Arc<TenantRuntime>>,
    activity: Arc<TenantActivity>,
    cfg: HarnessConfig,
    shutdown: Shutdown,
    tick_fn: F,
) where
    F: Fn(WorkerTenant) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Tick, String>> + Send,
{
    let multitenant = runtime.as_ref().is_some_and(|r| r.enabled);
    let expected_schema = crate::tenancy::migrate::head_version();
    tracing::info!(
        worker = cfg.name,
        multitenant,
        base_poll_ms = cfg.base_poll.as_millis() as u64,
        "worker started (tenant harness)"
    );

    let mut entries: HashMap<TenantId, Entry> = HashMap::new();
    entries.insert(
        TenantId::Consumer,
        Entry {
            next_due: Instant::now(),
            backoff: IdleBackoff::new(cfg.base_poll, cfg.max_org_idle_backoff),
            row: None,
            // Seeded from the current generation for the same reason org
            // entries are in `refresh_tenants`: this entry is already due now.
            seen_wake: activity.wake_generation(CONSUMER_WAKE),
            failure_until: None,
            not_before: None,
        },
    );
    // Tenants currently skipped by the schema gate — kept to log only on
    // transition, not every refresh.
    let mut schema_skipped: HashSet<String> = HashSet::new();
    // Consecutive pool-build failures per org. A tenant whose pool never builds
    // is not a transient blip: no tick of this worker has run for that org since
    // the count started, and nothing surfaces that to a user — the org simply
    // has no background work. Warn-per-attempt buries it (the retry itself
    // becomes the loudest thing in the log), so escalate once on crossing the
    // threshold and stay quiet after.
    let mut pool_failures: HashMap<String, u32> = HashMap::new();
    let mut next_refresh = Instant::now();
    let mut last_refresh = Instant::now();
    let mut unscheduled_seen: HashMap<String, u64> = HashMap::new();
    let scheduled = |entries: &HashMap<TenantId, Entry>, org: &str| {
        entries.contains_key(&TenantId::Org(org.to_string()))
    };

    loop {
        if shutdown.is_triggered() {
            tracing::info!(worker = cfg.name, "shutdown signalled, stopping (no new work claimed)");
            return;
        }

        // ── Tenant discovery + schema gate (flag on only) ────────────────────
        // A wake for an org this harness does not schedule yet pulls discovery
        // forward: a new org's first reads would otherwise wait out the
        // regular refresh before any worker computes them. Harnesses that
        // ignore wakes (sweeps) keep their cadence for new orgs too.
        let woke_unscheduled = multitenant
            && cfg.wake_on_enqueue
            && Instant::now() >= last_refresh + MIN_WAKE_DISCOVERY
            && activity.unscheduled_wakes(|org| scheduled(&entries, org)) != unscheduled_seen;
        if multitenant && (Instant::now() >= next_refresh || woke_unscheduled) {
            next_refresh = Instant::now() + cfg.tenant_refresh;
            last_refresh = Instant::now();
            refresh_tenants(
                &consumer_pool,
                &cfg,
                expected_schema,
                &mut entries,
                &mut schema_skipped,
                &activity,
            )
            .await;
            prune_pool_failures(&mut pool_failures, &entries);
            unscheduled_seen = activity.unscheduled_wakes(|org| scheduled(&entries, org));
        }

        // ── Cross-worker wake (P1 fix — see the module doc) ──────────────────
        // A dormant tenant's entries were scheduled for a far-out slot BEFORE
        // another worker found it work; without this pull-forward, extract
        // would wait out its own 20-minute slot after embed went hot and a
        // k-stage pipeline would take k slots end-to-end. Pull any escalated
        // entry whose shared activity clock turned recent back to "due now"
        // and reset its escalation. The consumer entry takes enqueue wakes
        // too: it never escalates, but its idle cadence is still `base_poll`.
        {
            let now = Instant::now();
            for (key, entry) in entries.iter_mut() {
                entry.apply_tenant_wake(key, &cfg, &activity, now);
            }
        }

        // ── Run one tick per due tenant (round-robin fairness) ───────────────
        let now = Instant::now();
        let due: Vec<TenantId> = entries
            .iter()
            .filter(|(_, e)| e.next_due <= now)
            .map(|(k, _)| k.clone())
            .collect();

        for key in due {
            if shutdown.is_triggered() {
                tracing::info!(worker = cfg.name, "shutdown signalled mid-pass, stopping");
                return;
            }
            let Some(entry) = entries.get_mut(&key) else { continue };

            // Resolve this tenant's (pool, store). Failure to build a tenant
            // pool is isolated: back off this tenant, keep serving the rest.
            let wt = match &key {
                TenantId::Consumer => WorkerTenant {
                    tenant: TenantId::Consumer,
                    pool: consumer_pool.clone(),
                    store: consumer_store.clone(),
                },
                TenantId::Org(org) => {
                    let Some(row) = entry.row.as_ref() else { continue };
                    let registry = runtime
                        .as_ref()
                        .expect("org entries only exist when runtime is present")
                        .registry
                        .clone();
                    // Bounded build: a HANGING tenant DB (vs a fast refusal)
                    // must not park this shared scheduler task — see the
                    // module doc's hung-tenant section. Timeout ⇒ same
                    // isolated-backoff path as a build error.
                    // `row.account_id`: this periodic poll is, notably, ALSO
                    // what self-heals a dormant org's missing commons ocean
                    // (`TenantRegistry::ensure_org_ocean`) even when no user
                    // ever logs in — every `active` tenant gets its pool built
                    // here on a timer regardless of request traffic.
                    // Tenant RLS Phase 2 (Category C): the WORKER pool, not
                    // the request pool — see `WorkerTenant`'s doc and
                    // `TenantRegistry::get_or_build_worker_pool`'s doc for
                    // why a pool separate from `get_or_build`'s is used here.
                    // It is NOT a connection-scoped `app.tenant_actor=system`
                    // pool: PR #388 deleted that startup-packet GUC because a
                    // connection pooler rejects it. The pool is separate for
                    // its own connection budget and idle eviction, and what
                    // covers an ambient tick query on it is the owner
                    // exemption, asserted at build time by
                    // `worker_pool_rls_exemption_breach` — which is why the
                    // `Err` arm below is the OPS-4-T failure mode: the pool
                    // refuses to build, loudly, rather than serving empty
                    // reads.
                    let built = tokio::time::timeout(
                        cfg.pool_build_timeout,
                        registry.get_or_build_worker_pool(org, row.account_id, &row.db_secret_ref),
                    )
                    .await
                    .map_err(|_| format!(
                        "pool build timed out after {}ms",
                        cfg.pool_build_timeout.as_millis()
                    ))
                    .and_then(|r| r.map_err(|e| e.to_string()));
                    match built {
                        Ok(pool) => {
                            // Reset on recovery, so a tenant that breaks again
                            // later alerts again rather than staying silent
                            // because it once crossed the threshold.
                            if let Some(prev) = pool_failures.remove(org.as_str()) {
                                if prev >= POOL_FAILURE_ALERT_THRESHOLD {
                                    tracing::info!(
                                        worker = cfg.name,
                                        org = %org,
                                        recovered_after = prev,
                                        "tenant pool recovered"
                                    );
                                }
                            }
                            WorkerTenant {
                                tenant: key.clone(),
                                store: Arc::new(PostgresStore::new(pool.clone())),
                                pool,
                            }
                        }
                        Err(e) => {
                            let delay = entry.backoff.on_empty().max(cfg.error_sleep);
                            let retry_at = Instant::now() + delay;
                            entry.next_due = retry_at;
                            // Hold this window against enqueue wakes — see
                            // `Entry::failure_until`.
                            entry.failure_until = Some(retry_at);
                            let failures =
                                pool_failures.entry(org.to_string()).or_insert(0);
                            *failures += 1;
                            match pool_failure_severity(*failures) {
                                PoolFailureSeverity::Warn => {
                                    tracing::warn!(
                                        worker = cfg.name,
                                        org = %org,
                                        error = %e,
                                        consecutive_failures = *failures,
                                        retry_in_ms = delay.as_millis() as u64,
                                        "tenant pool unavailable — backing off this tenant only"
                                    );
                                }
                                PoolFailureSeverity::Alert => {
                                    tracing::error!(
                                        worker = cfg.name,
                                        org = %org,
                                        error = %e,
                                        consecutive_failures = *failures,
                                        alert = true,
                                        "tenant pool has failed to build repeatedly — this org's \
                                         background work is NOT running and no user-visible error \
                                         is produced"
                                    );
                                }
                                // Past the threshold: stay quiet. Warning on every
                                // subsequent attempt is precisely the noise the
                                // alert exists to replace — it is what buried the
                                // pooled-DSN incident in the first place.
                                PoolFailureSeverity::Quiet => {
                                    tracing::debug!(
                                        worker = cfg.name,
                                        org = %org,
                                        consecutive_failures = *failures,
                                        "tenant pool still unavailable (already alerted)"
                                    );
                                }
                            }
                            continue;
                        }
                    }
                }
            };

            // Panic isolation: a panicking tick poisons only this tenant's
            // schedule, never the harness (or the other tenants). Both panic
            // sites are covered: constructing the future (`tick_fn(wt)` is
            // arbitrary closure code and can panic synchronously) and
            // polling it.
            let pool = wt.pool.clone();
            let started = Instant::now();
            let result = match std::panic::catch_unwind(AssertUnwindSafe(|| tick_fn(wt))) {
                Ok(fut) => AssertUnwindSafe(fut).catch_unwind().await,
                Err(construction_panic) => Err(construction_panic),
            };
            let now = Instant::now();
            let elapsed = now - started;
            if elapsed > SLOW_TICK {
                tracing::warn!(
                    worker = cfg.name,
                    tenant = ?key,
                    elapsed_ms = elapsed.as_millis() as u64,
                    processed = result.as_ref().ok().and_then(|r| r.as_ref().ok()).map(|t| t.processed),
                    "slow worker tick"
                );
            }
            match result {
                Ok(Ok(tick)) => {
                    entry.schedule_success(tick, &key, &cfg, &activity, now);
                }
                Ok(Err(e)) => {
                    let delay = match &key {
                        TenantId::Consumer => cfg.error_sleep,
                        TenantId::Org(_) => entry.backoff.on_empty().max(cfg.error_sleep),
                    };
                    entry.next_due = now + delay;
                    entry.failure_until = Some(now + delay);
                    tracing::error!(
                        worker = cfg.name,
                        tenant = ?key,
                        error = %e,
                        retry_in_ms = delay.as_millis() as u64,
                        "worker tick failed — backing off this tenant only"
                    );
                    // Every worker's DB error passes through here, so this is
                    // the one place a 25006 can be turned into a heal (COR-82).
                    if read_only_poison::is_read_only_rejection(&e) {
                        read_only_poison::heal(&key, &pool, &activity.read_only).await;
                    }
                }
                Err(panic) => {
                    let delay = match &key {
                        TenantId::Consumer => cfg.error_sleep,
                        TenantId::Org(_) => entry.backoff.on_empty().max(cfg.error_sleep),
                    };
                    entry.next_due = now + delay;
                    entry.failure_until = Some(now + delay);
                    let msg = panic
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "non-string panic payload".to_string());
                    tracing::error!(
                        worker = cfg.name,
                        tenant = ?key,
                        panic = %msg,
                        "worker tick PANICKED — isolated to this tenant, harness continues"
                    );
                }
            }
        }

        // ── Sleep until the earliest deadline (capped for shutdown polling) ──
        let now = Instant::now();
        let mut earliest = entries
            .values()
            .map(|e| e.next_due)
            .min()
            .unwrap_or(now + SHUTDOWN_POLL_INTERVAL);
        if multitenant {
            earliest = earliest.min(next_refresh);
        }
        let wait = earliest
            .saturating_duration_since(now)
            .min(SHUTDOWN_POLL_INTERVAL);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// Re-read the active-tenant list and reconcile `entries`: add newly-active
/// tenants (immediately due), refresh routing rows (secret_ref changes),
/// apply the schema-version gate, and drop tenants that left the list.
///
/// A control-plane read error keeps the previous list (log + carry on): a
/// control blip must not stop in-progress tenant work, and the staleness it
/// can introduce is bounded by the blip + the next successful refresh. New
/// tenants remain fail-closed (they are simply not discovered yet).
///
/// ACCEPTED RISK (named): during a SUSTAINED control-plane outage, a tenant
/// suspended for offboarding keeps having its already-visible jobs processed
/// until the next successful refresh — the suspend "write freeze" is only as
/// fresh as the control plane. Mitigation lives in the offboarding runbook
/// (PR 3.2 `offboard_tenant.sh`): after setting `suspended`, verify worker
/// quiescence (no `running` jobs in the tenant DB) before taking the export.
/// A stronger in-process fix (drop org entries after N consecutive refresh
/// failures) is deliberately not taken here: during a control outage it would
/// also stop PERFECTLY HEALTHY tenants' job processing, trading a narrow
/// offboarding-window risk for a broad availability loss.
/// The routing rows a harness with `cfg` schedules: [`routing::list_active`],
/// plus [`routing::list_active_revoked`] when `cfg.include_revoked_orgs`.
pub async fn list_scheduled_tenants(
    control_pool: &PgPool,
    cfg: &HarnessConfig,
) -> Result<Vec<routing::TenantDatabase>, sqlx::Error> {
    let mut rows = routing::list_active(control_pool).await?;
    if cfg.include_revoked_orgs {
        rows.extend(routing::list_active_revoked(control_pool).await?);
    }
    Ok(rows)
}

async fn refresh_tenants(
    control_pool: &PgPool,
    cfg: &HarnessConfig,
    expected_schema: i64,
    entries: &mut HashMap<TenantId, Entry>,
    schema_skipped: &mut HashSet<String>,
    activity: &TenantActivity,
) {
    let rows = match list_scheduled_tenants(control_pool, cfg).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                worker = cfg.name,
                error = %e,
                "active-tenant refresh failed — keeping previous tenant list"
            );
            return;
        }
    };

    let mut live: HashSet<String> = HashSet::with_capacity(rows.len());
    for row in rows {
        let org = row.clerk_org_id.clone();
        // Schema-version gate: never run this binary's queries against a
        // tenant whose applied tenant-tree version lags what the binary
        // expects (the per-tenant 503-equivalent: log + skip, no queries).
        let have = row.schema_version.unwrap_or(0);
        if have < expected_schema {
            if schema_skipped.insert(org.clone()) {
                tracing::warn!(
                    worker = cfg.name,
                    org = %org,
                    have_schema = have,
                    expected_schema,
                    "tenant schema lags this binary — skipping until migrate_tenants catches it up"
                );
            }
            continue;
        }
        if schema_skipped.remove(&org) {
            tracing::info!(worker = cfg.name, org = %org, "tenant schema caught up — resuming");
        }
        live.insert(org.clone());
        // Bound before `org` is moved into the `TenantId` key below.
        let wake_seed = activity.wake_generation(&org);
        entries
            .entry(TenantId::Org(org))
            .and_modify(|e| e.row = Some(row.clone()))
            .or_insert_with(|| Entry {
                next_due: Instant::now(),
                backoff: IdleBackoff::new(cfg.base_poll, cfg.max_org_idle_backoff),
                row: Some(row),
                // Seed from the CURRENT generation, not 0: a new entry is
                // already `next_due: now`, so any enqueue that landed before
                // we discovered this tenant is covered by that first poll.
                // Seeding 0 against a nonzero generation would just burn one
                // redundant pull-forward on the very next pass.
                seen_wake: wake_seed,
                failure_until: None,
                not_before: None,
            });
    }

    // Drop tenants that left the active list (suspended/offboarded/deleted) —
    // their pools are the registry's to evict; we just stop scheduling them.
    entries.retain(|k, _| match k {
        TenantId::Consumer => true,
        TenantId::Org(org) => live.contains(org),
    });
    schema_skipped.retain(|org| !live.contains(org));
    activity.retain(&live);
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Exercise standing's real success scheduling with a deterministic instant.
#[cfg(feature = "test-support")]
pub fn test_standing_scheduled_delay(tick: Tick, tenant: &TenantId) -> Duration {
    let cfg = crate::managed::standing::execution::harness_config();
    let now = Instant::now();
    let mut entry = Entry {
        next_due: now,
        backoff: IdleBackoff::new(cfg.base_poll, cfg.max_org_idle_backoff),
        row: None,
        seen_wake: 0,
        failure_until: None,
        not_before: None,
    };
    entry.schedule_success(tick, tenant, &cfg, &TenantActivity::default(), now);
    entry.next_due.duration_since(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn scheduled_entry(cfg: &HarnessConfig, now: Instant) -> Entry {
        Entry {
            next_due: now + cfg.base_poll,
            backoff: IdleBackoff::new(cfg.base_poll, cfg.max_org_idle_backoff),
            row: None,
            seen_wake: 0,
            failure_until: None,
            not_before: None,
        }
    }

    #[test]
    fn standing_actual_scheduler_never_idles_beyond_sixty_seconds() {
        let cfg=crate::managed::standing::execution::harness_config();
        let activity=TenantActivity::default();
        let key=TenantId::Org("standing-controlled-clock".into());
        let mut now=Instant::now();
        let mut entry=scheduled_entry(&cfg,now);
        for _ in 0..128 {
            entry.schedule_success(Tick::idle(),&key,&cfg,&activity,now);
            let delay=entry.next_due.duration_since(now);
            assert!(delay>Duration::ZERO && delay<=Duration::from_secs(60),"actual scheduled delay {delay:?}");
            now=entry.next_due;
        }
        entry.schedule_success(Tick::worked(1),&key,&cfg,&activity,now);
        assert_eq!(entry.next_due.duration_since(now),Duration::from_secs(60));
    }

    #[test]
    fn sweeps_keep_their_cadence_under_enqueue_and_peer_wakes() {
        let cfg = HarnessConfig::sweep_worker("sweep", Duration::from_secs(300));
        let activity = TenantActivity::default();
        let key = TenantId::Org("acme".into());
        let now = Instant::now();
        for tick in [Tick::worked(1), Tick::idle()] {
            let mut entry = scheduled_entry(&cfg, now);
            entry.schedule_success(tick, &key, &cfg, &activity, now);
            let deadline = entry.next_due;
            // Both pre-pass wake paths used to shorten sweeps' native cadence.
            entry.backoff.on_empty();
            entry.backoff.on_empty();
            assert!(entry.backoff.is_escalated());
            entry.apply_wake(&cfg, 0, Duration::ZERO, now + MS);
            assert_eq!(entry.next_due, deadline, "peer work must not wake sweeps");
            for generation in 1..=500 {
                entry.apply_wake(&cfg, generation, Duration::ZERO, now + MS);
                assert_eq!(entry.next_due, deadline, "enqueue must not wake sweeps");
            }
            assert!(entry.backoff.is_escalated());
            assert!(entry.next_due > now + MS);
        }
    }

    #[test]
    fn queue_enqueue_and_peer_wakes_remain_prompt() {
        let cfg = HarnessConfig::queue_worker("queue", MS);
        let now = Instant::now();
        for generation in [0, 1] {
            let mut entry = scheduled_entry(&cfg, now);
            entry.next_due = now + DORMANT_POLL_PERIOD;
            for _ in 0..30 {
                entry.backoff.on_empty();
            }
            entry.apply_wake(&cfg, generation, Duration::ZERO, now);
            assert_eq!(entry.next_due, now);
            assert_eq!(entry.seen_wake, generation);
            assert!(!entry.backoff.is_escalated());
        }
    }

    #[test]
    fn hard_pause_preserves_pending_enqueues_and_blocks_peer_wakes_until_expiry() {
        let cfg = HarnessConfig::queue_worker("embed", MS);
        let activity = TenantActivity::default();
        let key = TenantId::Org("acme".into());
        let now = Instant::now();
        let wait = Duration::from_secs(30);
        let mut entry = scheduled_entry(&cfg, now);
        entry.backoff.on_empty();
        entry.backoff.on_empty();
        entry.schedule_success(Tick::deferred(wait), &key, &cfg, &activity, now);
        assert_eq!(entry.not_before, Some(now + wait));
        assert!(entry.backoff.is_escalated(), "defer preserves idle backoff");
        // Peer activity is present even before any external enqueue.
        entry.apply_wake(&cfg, 0, Duration::ZERO, now + MS);
        assert_eq!(entry.next_due, now + wait);
        for generation in 1..=500 {
            entry.apply_wake(&cfg, generation, Duration::ZERO, now + wait - MS);
            assert_eq!(entry.next_due, now + wait, "no due-now scheduler spin");
            assert_eq!(entry.seen_wake, 0, "pending enqueue must survive the pause");
        }
        entry.apply_wake(&cfg, 500, Duration::ZERO, now + wait);
        assert_eq!(entry.seen_wake, 500, "wake applies at the exact boundary");
        assert_eq!(entry.next_due, now + wait);
        entry.schedule_success(Tick::worked(1), &key, &cfg, &activity, now + wait);
        assert_eq!(
            entry.not_before, None,
            "successful unpaused tick clears the floor"
        );
        entry.apply_wake(&cfg, 501, Duration::ZERO, now + wait + MS);
        assert_eq!(entry.next_due, now + wait + MS);
    }

    #[test]
    fn failure_backoff_preserves_pending_enqueues_and_blocks_peer_wakes_until_expiry() {
        let cfg = HarnessConfig::queue_worker("queue", MS);
        let now = Instant::now();
        let deadline = now + Duration::from_secs(5);
        let mut entry = scheduled_entry(&cfg, now);
        entry.next_due = deadline;
        entry.failure_until = Some(deadline);
        entry.backoff.on_empty();
        entry.backoff.on_empty();
        entry.apply_wake(&cfg, 0, Duration::ZERO, now + MS);
        assert_eq!(entry.next_due, deadline);
        for generation in 1..=500 {
            entry.apply_wake(&cfg, generation, Duration::ZERO, deadline - MS);
            assert_eq!(entry.next_due, deadline);
            assert_eq!(entry.seen_wake, 0);
        }
        entry.apply_wake(&cfg, 500, Duration::ZERO, deadline);
        assert_eq!(entry.next_due, deadline);
        assert_eq!(entry.seen_wake, 500);
    }

    #[test]
    fn new_teams_work_interrupts_idle_and_processed_known_work_deadlines() {
        let mut cfg = HarnessConfig::sweep_worker("teams_meeting_sync", Duration::from_secs(300));
        cfg.wake_on_enqueue = true;
        let activity = TenantActivity::default();
        let key = TenantId::Org("acme".into());
        let now = Instant::now();
        for (processed, wait) in [(0, Duration::from_secs(1800)), (3, cfg.base_poll)] {
            let mut entry = scheduled_entry(&cfg, now);
            entry.schedule_success(
                Tick::until_next_work(processed, wait),
                &key,
                &cfg,
                &activity,
                now,
            );
            assert_eq!(
                entry.next_due,
                now + wait,
                "known deadline is honored without a wake"
            );
            assert_eq!(entry.not_before, None);
            // A new schedule intent/capture arrives after the tick chose an
            // existing later deadline. Its generation must remain observable.
            entry.apply_wake(&cfg, 1, Duration::ZERO, now + MS);
            assert_eq!(entry.next_due, now + MS);
            assert_eq!(entry.seen_wake, 1);
        }
    }

    #[test]
    fn consumer_tick_scheduling_keeps_idle_work_and_defer_behavior() {
        let now = Instant::now();
        let activity = TenantActivity::default();
        let queue = HarnessConfig::queue_worker("queue", MS);
        let sweep = HarnessConfig::sweep_worker("sweep", Duration::from_secs(300));
        for cfg in [&queue, &sweep] {
            let mut entry = scheduled_entry(cfg, now);
            entry.schedule_success(Tick::idle(), &TenantId::Consumer, cfg, &activity, now);
            assert_eq!(entry.next_due, now + cfg.base_poll);
            entry.schedule_success(Tick::worked(1), &TenantId::Consumer, cfg, &activity, now);
            assert_eq!(
                entry.next_due,
                if cfg.hot_loop_on_work {
                    now
                } else {
                    now + cfg.base_poll
                }
            );
            for tick in [
                Tick::deferred(Duration::from_secs(30)),
                Tick::until_next_work(3, Duration::from_secs(1800)),
            ] {
                entry.schedule_success(tick, &TenantId::Consumer, cfg, &activity, now);
                let wait = if tick.processed == 0 { 30 } else { 1800 };
                assert_eq!(entry.next_due, now + Duration::from_secs(wait));
            }
        }
    }

    // ── External-enqueue wake ────────────────────────────────────────────────

    /// The defect this fix exists for: an org quiet long enough to saturate
    /// its backoff is asleep for `DORMANT_POLL_PERIOD`, so a webhook-enqueued
    /// job waits out the slot. Measured in production before the fix: 11m04s
    /// for an ingest whose work took 340ms.
    #[test]
    fn enqueue_wakes_a_fully_dormant_tenant() {
        let a = TenantActivity::default();
        a.note_enqueue("acme");
        // Saturated backoff, and no peer worker has found work in ages — the
        // exact state that produced the 11-minute delay.
        assert_eq!(
            wake_reason(0, a.wake_generation("acme"), true, DORMANT_POLL_PERIOD * 3, false),
            Wake::Enqueued,
        );
    }

    /// Not gated on escalation: an enqueue must pull forward even from a
    /// base-poll delay, because it means work exists NOW.
    #[test]
    fn enqueue_wakes_even_when_not_escalated() {
        assert_eq!(wake_reason(0, 1, false, Duration::ZERO, false), Wake::Enqueued);
    }

    /// The property that makes this a generation counter rather than a
    /// consumable flag: ONE enqueue wakes EVERY worker, each tracking its own
    /// `seen_wake`. A take-the-flag design would let the first worker to poll
    /// swallow the wake and leave the rest asleep.
    #[test]
    fn one_enqueue_wakes_every_worker_independently() {
        let a = TenantActivity::default();
        a.note_enqueue("acme");
        let generation = a.wake_generation("acme");

        // Twelve harnesses, each with its own last-seen generation.
        let mut seen = [0u64; 12];
        for s in seen.iter_mut() {
            assert_eq!(
                wake_reason(*s, generation, true, DORMANT_POLL_PERIOD, false),
                Wake::Enqueued,
                "every worker must see this enqueue",
            );
            *s = generation; // caller advances it on Enqueued
        }

        // ...and none of them re-fires on the next pass.
        for s in seen.iter() {
            assert_eq!(
                wake_reason(*s, generation, true, DORMANT_POLL_PERIOD, false),
                Wake::None,
                "a consumed wake must not re-fire",
            );
        }
    }

    /// Enqueue outranks peer activity so the generation is actually consumed.
    /// If `PeerActivity` won, `seen_wake` would never advance and the same
    /// wake would re-fire on every pass forever.
    #[test]
    fn enqueue_outranks_peer_activity_so_the_generation_is_consumed() {
        // Both signals present: escalated + recent peer work + a new generation.
        assert_eq!(wake_reason(0, 1, true, Duration::ZERO, false), Wake::Enqueued);
    }

    /// The pre-existing cross-worker wake still behaves exactly as before when
    /// no enqueue is outstanding.
    #[test]
    fn peer_activity_wake_is_unchanged_without_an_enqueue() {
        assert_eq!(wake_reason(7, 7, true, Duration::ZERO, false), Wake::PeerActivity);
        // Not escalated ⇒ already fast, nothing to pull forward.
        assert_eq!(wake_reason(7, 7, false, Duration::ZERO, false), Wake::None);
        // Escalated but peer work is stale ⇒ stay in backoff.
        assert_eq!(
            wake_reason(7, 7, true, RECENT_ACTIVITY_WINDOW + MS, false),
            Wake::None,
        );
    }

    // ── Enqueue wake vs FAILURE backoff (CodeRabbit #498, Major) ─────────────

    /// The defect: an enqueue wake used to override failure backoff too. A
    /// tenant whose Neon pool is refusing connections, in a Slack workspace
    /// that keeps producing messages, would be retried on EVERY scheduler pass
    /// — each message bumps the generation, each bump forced `next_due = now`
    /// — hammering a database that is already down and blowing straight past
    /// `error_sleep` and the doubling that `POOL_FAILURE_ALERT_THRESHOLD`'s
    /// doc assumes ("five failures is already several minutes").
    #[test]
    fn enqueue_does_not_override_failure_backoff() {
        assert_eq!(
            wake_reason(0, 1, true, DORMANT_POLL_PERIOD, true),
            Wake::EnqueuedButHeld,
        );
    }

    /// Continuous enqueues during an outage must NOT each buy a retry. Every
    /// new generation still resolves to "leave the schedule alone".
    #[test]
    fn continuous_enqueues_during_an_outage_never_pull_the_retry_forward() {
        let a = TenantActivity::default();
        let seen = 0;
        for _ in 0..500 {
            a.note_enqueue("acme");
            assert_eq!(
                wake_reason(seen, a.wake_generation("acme"), true, DORMANT_POLL_PERIOD, true),
                Wake::EnqueuedButHeld,
                "a failing tenant must not be retried per-message",
            );
        }
    }

    /// ...and the wake is not LOST: `seen_wake` is never advanced while
    /// failing, so the first pass after the window expires fires it.
    #[test]
    fn a_wake_deferred_by_failure_fires_once_the_window_expires() {
        let a = TenantActivity::default();
        a.note_enqueue("acme");
        let generation = a.wake_generation("acme");

        // During the failure window: deferred, generation NOT consumed.
        assert_eq!(
            wake_reason(0, generation, true, DORMANT_POLL_PERIOD, true),
            Wake::EnqueuedButHeld,
        );
        // Window expires (pool rebuilt ⇒ `failure_until` cleared).
        assert_eq!(
            wake_reason(0, generation, true, DORMANT_POLL_PERIOD, false),
            Wake::Enqueued,
            "the deferred wake must survive the outage, not be swallowed",
        );
    }

    /// Failure backoff also outranks the peer-activity wake: another worker
    /// finding work says nothing about whether THIS tenant's pool recovered.
    #[test]
    fn failure_backoff_outranks_peer_activity() {
        assert_eq!(wake_reason(7, 7, true, Duration::ZERO, true), Wake::None);
    }

    /// `Entry::in_failure_backoff` is time-bounded, not sticky.
    #[test]
    fn failure_window_expires_on_its_own_clock() {
        let now = Instant::now();
        let mut e = Entry {
            next_due: now,
            backoff: IdleBackoff::new(MS, MS),
            row: None,
            seen_wake: 0,
            failure_until: Some(now + Duration::from_secs(5)),
            not_before: None,
        };
        assert!(e.in_failure_backoff(now), "inside the window");
        assert!(
            !e.in_failure_backoff(now + Duration::from_secs(6)),
            "past the window",
        );
        e.failure_until = None;
        assert!(!e.in_failure_backoff(now), "cleared on a successful tick");
    }

    /// Counters die with their tenant, like `prune_pool_failures`'.
    #[test]
    fn retain_prunes_wake_generations_with_their_tenant() {
        let a = TenantActivity::default();
        a.note_enqueue("acme");
        a.note_enqueue("gone");
        assert_eq!(a.wake_generation("gone"), 1);

        a.retain(&HashSet::from(["acme".to_string()]));

        assert_eq!(a.wake_generation("acme"), 1);
        assert_eq!(a.wake_generation("gone"), 0, "dead tenant's counter dropped");
    }

    // ── Consumer-plane enqueue wake ──────────────────────────────────────────

    /// The defect: a personal-account request that enqueued a read-model
    /// refresh had no way to wake the consumer entry, which idles at
    /// `base_poll` (5 s for `read_model_refresh`). Measured locally before the
    /// fix: 1.8-4.6 s from POST refresh to a fresh row, against ~0.5 s on the
    /// org plane. Without the consumer arm in `apply_tenant_wake`, the
    /// "pulled to due-now" assertion fails with `next_due` still one
    /// `base_poll` out.
    #[test]
    fn consumer_enqueue_pulls_the_consumer_entry_to_due_now() {
        let cfg = HarnessConfig::queue_worker("read_model_refresh", Duration::from_secs(5));
        let activity = TenantActivity::default();
        let key = TenantId::Consumer;
        let now = Instant::now();
        let mut entry = scheduled_entry(&cfg, now);

        entry.apply_tenant_wake(&key, &cfg, &activity, now + MS);
        assert_eq!(entry.next_due, now + cfg.base_poll, "no enqueue, no wake");

        activity.note_consumer_enqueue();
        entry.apply_tenant_wake(&key, &cfg, &activity, now + MS);
        assert_eq!(
            entry.next_due,
            now + MS,
            "consumer enqueue pulled to due-now"
        );
        assert_eq!(entry.seen_wake, 1, "generation consumed");

        // The tick that follows reschedules at base_poll; the consumed
        // generation must not pull it forward again.
        entry.schedule_success(Tick::idle(), &key, &cfg, &activity, now + MS);
        let deadline = entry.next_due;
        assert_eq!(deadline, now + MS + cfg.base_poll);
        entry.apply_tenant_wake(&key, &cfg, &activity, now + 2 * MS);
        assert_eq!(entry.next_due, deadline, "a consumed wake must not re-fire");
    }

    /// `refresh_tenants` prunes `wake_gen` to the live org set, which never
    /// contains the consumer key. Without the carve-out the consumer
    /// generation reads 0 here, and every tenant refresh would then cost the
    /// consumer entry one spurious pull-forward (`seen_wake` != 0).
    /// A tenant provisioned after the last discovery pass gets its first
    /// enqueue before it is scheduled. That wake must be visible as
    /// unscheduled so the harness discovers the org instead of leaving its
    /// reads to the regular refresh; once recorded it must not re-fire, and a
    /// later enqueue for the same unknown org must fire again.
    #[test]
    fn a_wake_for_an_unscheduled_org_is_reported_until_discovery_records_it() {
        let a = TenantActivity::default();
        let scheduled = |org: &str| org == "known";
        a.note_enqueue("known");
        a.note_consumer_enqueue();
        assert!(
            a.unscheduled_wakes(scheduled).is_empty(),
            "scheduled orgs and the consumer are never unscheduled"
        );
        a.note_enqueue("fresh");
        let seen = a.unscheduled_wakes(scheduled);
        assert_eq!(seen, HashMap::from([("fresh".to_string(), 1)]));
        assert_eq!(
            a.unscheduled_wakes(scheduled),
            seen,
            "a recorded wake does not re-fire"
        );
        a.note_enqueue("fresh");
        assert_ne!(
            a.unscheduled_wakes(scheduled),
            seen,
            "a later enqueue for the unknown org fires again"
        );
    }

    #[test]
    fn retain_keeps_the_consumer_wake_generation() {
        let a = TenantActivity::default();
        a.note_consumer_enqueue();
        a.note_enqueue("acme");

        a.retain(&HashSet::new());

        assert_eq!(
            a.wake_generation(CONSUMER_WAKE),
            1,
            "consumer key survives an empty live set"
        );
        assert_eq!(a.wake_generation("acme"), 0, "org keys are still pruned");
    }

    /// Org signals are not consumer signals: a peer org's recent work and its
    /// enqueue generation leave the consumer entry on its schedule. Fails if
    /// `apply_tenant_wake` hands the consumer a recent `since_last_work`
    /// (`wake_reason` would return `PeerActivity` for an escalated entry) or
    /// reads an org's generation for it.
    #[test]
    fn peer_org_activity_never_wakes_the_consumer() {
        let cfg = HarnessConfig::queue_worker("read_model_refresh", Duration::from_secs(5));
        let activity = TenantActivity::default();
        let now = Instant::now();
        activity.note_work("acme");
        activity.note_enqueue("acme");
        let mut entry = scheduled_entry(&cfg, now);
        // The loop never escalates the consumer entry; force it so the guard
        // is shown not to depend on that invariant.
        entry.backoff.on_empty();
        entry.backoff.on_empty();
        assert!(entry.backoff.is_escalated());

        entry.apply_tenant_wake(&TenantId::Consumer, &cfg, &activity, now + MS);

        assert_eq!(
            entry.next_due,
            now + cfg.base_poll,
            "org activity left the consumer alone"
        );
        assert_eq!(
            entry.seen_wake, 0,
            "org generation was not consumed as a consumer wake"
        );
    }

    /// Repeated enqueues before a worker gets a pass collapse into one wake —
    /// the worker polls once and `dequeue`'s batch drains them together.
    #[test]
    fn repeated_enqueues_collapse_into_one_wake() {
        let a = TenantActivity::default();
        for _ in 0..50 {
            a.note_enqueue("acme");
        }
        let generation = a.wake_generation("acme");
        assert_eq!(generation, 50);
        assert_eq!(wake_reason(0, generation, true, DORMANT_POLL_PERIOD, false), Wake::Enqueued);
        assert_eq!(
            wake_reason(generation, generation, true, DORMANT_POLL_PERIOD, false),
            Wake::None,
        );
    }

    #[test]
    fn idle_backoff_escalates_doubling_to_max_and_resets_on_work() {
        let mut b = IdleBackoff::new(100 * MS, 800 * MS);
        // Empty polls escalate the interval: base, 2x, 4x, then capped.
        assert_eq!(b.on_empty(), 100 * MS);
        assert_eq!(b.on_empty(), 200 * MS);
        assert_eq!(b.on_empty(), 400 * MS);
        assert_eq!(b.on_empty(), 800 * MS);
        assert!(b.saturated());
        assert_eq!(b.on_empty(), 800 * MS, "stays capped");
        // Activity resets it back to base.
        b.on_work();
        assert!(!b.saturated());
        assert_eq!(b.on_empty(), 100 * MS);
    }

    #[test]
    fn idle_backoff_max_is_at_least_base() {
        let mut b = IdleBackoff::new(500 * MS, 100 * MS);
        assert_eq!(b.on_empty(), 500 * MS);
        assert_eq!(b.on_empty(), 500 * MS, "max clamped up to base");
    }

    #[test]
    fn dormant_slots_align_on_the_shared_clock() {
        let period = Duration::from_secs(1200);
        // Two workers computing at different offsets into the same period
        // land on the SAME absolute slot boundary.
        let d1 = dormant_slot_delay(Duration::from_secs(100), period);
        let d2 = dormant_slot_delay(Duration::from_secs(700), period);
        assert_eq!(Duration::from_secs(100) + d1, Duration::from_secs(1200));
        assert_eq!(Duration::from_secs(700) + d2, Duration::from_secs(1200));
        // Next period: multiple of the period again.
        let d3 = dormant_slot_delay(Duration::from_secs(1300), period);
        assert_eq!(Duration::from_secs(1300) + d3, Duration::from_secs(2400));
    }

    #[test]
    fn tenant_activity_first_sight_counts_as_active() {
        let a = TenantActivity::new();
        let now = Instant::now();
        assert_eq!(a.since_last_work("org_x", now), Duration::ZERO);
    }

    #[test]
    fn recent_shared_activity_snaps_an_escalated_worker_back_to_base() {
        // The P1 cross-worker wake: extract's backoff is deep in escalation,
        // but embed just found work for the tenant (shared clock is recent) —
        // extract's next idle delay must be base_poll, not the dormant slot.
        let period = Duration::from_secs(1200);
        let mut b = IdleBackoff::new(200 * MS, period);
        for _ in 0..30 {
            b.on_empty(); // fully saturate
        }
        assert!(b.saturated());
        let d = org_idle_delay(&mut b, Duration::from_secs(5), period);
        assert_eq!(d, 200 * MS, "recent shared activity ⇒ base_poll");
        assert!(!b.is_escalated(), "escalation was reset by the shared clock");
    }

    #[test]
    fn stale_shared_activity_escalates_then_slot_aligns() {
        let period = Duration::from_secs(1200);
        let mut b = IdleBackoff::new(100 * MS, period);
        let stale = RECENT_ACTIVITY_WINDOW + Duration::from_secs(1);
        // Outside the window: normal exponential escalation from base…
        assert_eq!(org_idle_delay(&mut b, stale, period), 100 * MS);
        assert_eq!(org_idle_delay(&mut b, stale, period), 200 * MS);
        // …and once saturated, the delay comes from the aligned slot grid.
        for _ in 0..30 {
            b.on_empty();
        }
        assert!(b.saturated());
        let d = org_idle_delay(&mut b, Duration::from_secs(1300), period);
        assert_eq!(Duration::from_secs(1300) + d, Duration::from_secs(2400));
    }

    #[test]
    fn is_escalated_tracks_beyond_base_only() {
        let mut b = IdleBackoff::new(100 * MS, 800 * MS);
        assert!(!b.is_escalated(), "fresh backoff is not escalated");
        b.on_empty(); // = base
        assert!(!b.is_escalated(), "at base is not escalated");
        b.on_empty(); // 2× base
        assert!(b.is_escalated());
        b.on_work();
        assert!(!b.is_escalated());
    }

    #[test]
    fn tick_constructors() {
        assert_eq!(Tick::idle(), Tick { processed: 0, defer: None });
        assert_eq!(Tick::worked(3), Tick { processed: 3, defer: None });
        assert_eq!(
            Tick::deferred(Duration::from_secs(30)),
            Tick { processed: 0, defer: Some(TickDelay::Backpressure(Duration::from_secs(30))) }
        );
    }

    /// The alert must fire exactly ONCE per outage, then go quiet.
    ///
    /// The first cut of this used `if failures == THRESHOLD { error } else
    /// { warn }`, which reverted to a warning on every attempt past the
    /// threshold — reintroducing the per-attempt noise the alert exists to
    /// replace, and which buried the pooled-DSN incident. The bug was invisible
    /// to every existing test because nothing asserted on log severity.
    #[test]
    fn pool_failure_escalation_alerts_once_then_stays_quiet() {
        // Calls the SAME function the production Err arm matches on. An earlier
        // version of this test re-implemented the comparison, which would have
        // kept passing while production drifted — the very failure mode it was
        // written to catch.
        let observed: Vec<PoolFailureSeverity> = (1..=POOL_FAILURE_ALERT_THRESHOLD + 3)
            .map(pool_failure_severity)
            .collect();

        assert_eq!(
            observed
                .iter()
                .filter(|s| **s == PoolFailureSeverity::Alert)
                .count(),
            1,
            "exactly one alert per outage, not one per retry: {observed:?}"
        );
        assert_eq!(
            observed[POOL_FAILURE_ALERT_THRESHOLD as usize - 1],
            PoolFailureSeverity::Alert,
            "the alert fires on crossing the threshold"
        );
        assert!(
            observed[POOL_FAILURE_ALERT_THRESHOLD as usize..]
                .iter()
                .all(|s| *s == PoolFailureSeverity::Quiet),
            "everything past the threshold stays quiet: {observed:?}"
        );
        assert!(
            observed[..POOL_FAILURE_ALERT_THRESHOLD as usize - 1]
                .iter()
                .all(|s| *s == PoolFailureSeverity::Warn),
            "early failures still warn, so a transient blip is visible: {observed:?}"
        );
    }

    /// A departing org's failure counter must not outlive it.
    ///
    /// Otherwise an org that is suspended (or schema-gated) while failing, then
    /// returns, resumes at a count past the threshold — so its next genuine
    /// outage opens at `Quiet` and never alerts at all.
    #[test]
    fn pool_failure_counters_die_with_their_tenant_entry() {
        fn entry() -> Entry {
            Entry {
                seen_wake: 0,
                failure_until: None,
                not_before: None,
                next_due: Instant::now(),
                backoff: IdleBackoff::new(Duration::from_secs(1), Duration::from_secs(60)),
                row: None,
            }
        }

        let mut entries: HashMap<TenantId, Entry> = HashMap::new();
        entries.insert(TenantId::Consumer, entry());
        entries.insert(TenantId::Org("org_still_here".into()), entry());

        let mut pool_failures: HashMap<String, u32> = HashMap::new();
        pool_failures.insert("org_still_here".into(), 2);
        // Suspended/offboarded mid-outage, already past the threshold.
        pool_failures.insert("org_departed".into(), POOL_FAILURE_ALERT_THRESHOLD + 4);

        prune_pool_failures(&mut pool_failures, &entries);

        assert_eq!(
            pool_failures.get("org_still_here"),
            Some(&2),
            "an active org keeps its in-flight count"
        );
        assert!(
            !pool_failures.contains_key("org_departed"),
            "a departed org's counter is dropped, so it starts clean if it returns"
        );

        // The point of dropping it: the next failure alerts rather than being
        // swallowed as already-reported.
        assert_eq!(
            pool_failure_severity(*pool_failures.get("org_departed").unwrap_or(&0) + 1),
            PoolFailureSeverity::Warn,
            "a returning org's first failure is a warn, not a suppressed Quiet"
        );
    }
}
