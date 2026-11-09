//! Shared-source semantic atoms, separate from the existing retrieval chunks.
//!
//! Each job reads one immutable source version and processes one bounded,
//! deterministic window. Evidence offsets count Unicode scalar values in the
//! exact preserved text; the model supplies quotations, never offsets. A
//! durable completion row makes retries safe after the queue's dedup expires.

use std::collections::HashSet;
use std::time::Duration;

use coralswarm_core::r2::R2Client;
use coralswarm_core::send_gate::HoldCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::jobs::{
    queue::{self, JobRow},
    JobKind,
};
use crate::llm::{CompletionRequest, Deny, LlmError, LlmProvider, Message, Role, ToolSchema};
use crate::store::{ReefRefCtx, ReefResolution, Store};
use crate::tenancy::{
    scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
    TenantId,
};
use crate::workers::{
    embed::JobScope,
    tenant_harness::{Tick, WorkerTenant},
};

/// The retired first source profile. No version is pinned to it anymore and
/// no worker extracts it; the name survives only where historical rows are
/// read (pins, windows and atoms written before v2).
pub const EXTRACTION_VERSION: &str = "source-atoms-v1";
pub const LEGACY_EXTRACTION_VERSION: &str = "meeting-atoms-v1";
pub use v2::{active_source_profile, EXTRACTION_VERSION_V2};
pub use v3::EXTRACTION_VERSION_V3;
const LEGACY_SYSTEM_PROMPT: &str = include_str!("../../prompts/extract_meeting_atoms_v1.txt");

#[path = "source_atoms_v2.rs"]
pub mod v2;
#[path = "source_atoms_v3.rs"]
pub mod v3;

/// A version pins its extraction contract. Queue compatibility must never
/// silently change the prompt halfway through a multi-window source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExtractionProfile {
    MeetingV1,
    /// Relations, temporal validity and Jev-adjudicated binding
    /// (`source_atoms_v2.rs`).
    SourceV2,
    /// v2 plus stated changes and dates resolved against `occurred_at`
    /// (`source_atoms_v3.rs`). Same job kinds and write path as `SourceV2`.
    SourceV3,
}
impl ExtractionProfile {
    fn from_version(version: &str) -> Option<Self> {
        match version {
            LEGACY_EXTRACTION_VERSION => Some(Self::MeetingV1),
            EXTRACTION_VERSION_V2 => Some(Self::SourceV2),
            EXTRACTION_VERSION_V3 => Some(Self::SourceV3),
            _ => None,
        }
    }
    fn version(self) -> &'static str {
        match self {
            Self::MeetingV1 => LEGACY_EXTRACTION_VERSION,
            Self::SourceV2 => EXTRACTION_VERSION_V2,
            Self::SourceV3 => EXTRACTION_VERSION_V3,
        }
    }
    /// Output token budget per window call. v3's atoms are ~30% larger than
    /// v2's (changes, anchors, speakers; measured ~205 vs ~158 tokens per
    /// atom), so a dense 8,000-char window can need more than 8,000 tokens;
    /// a truncated reply is retried on a smaller window and loses atoms. Only
    /// tokens produced are billed; the worst-case reservation rises ~$0.005.
    fn max_tokens(self) -> u32 {
        match self {
            Self::SourceV3 => 16_000,
            Self::MeetingV1 | Self::SourceV2 => 8_000,
        }
    }
    /// Relations, validity and Jev binding: the v2 write path (v2 and v3).
    fn structured(self) -> bool {
        matches!(self, Self::SourceV2 | Self::SourceV3)
    }
    fn system(self) -> &'static str {
        match self {
            Self::MeetingV1 => LEGACY_SYSTEM_PROMPT,
            Self::SourceV2 => v2::SYSTEM_PROMPT_V2,
            Self::SourceV3 => v3::SYSTEM_PROMPT_V3,
        }
    }
    fn task(self) -> &'static str {
        match self {
            Self::MeetingV1 => "meeting_atoms",
            Self::SourceV2 => "source_atoms_v2",
            Self::SourceV3 => "source_atoms_v3",
        }
    }
    fn tool_name(self) -> &'static str {
        match self {
            Self::MeetingV1 => "emit_meeting_atoms",
            Self::SourceV2 => v2::TOOL_NAME_V2,
            Self::SourceV3 => v3::TOOL_NAME_V3,
        }
    }
    fn job(self, graph_id: Uuid, source_version_id: Uuid, start_char: i64) -> JobKind {
        match self {
            Self::MeetingV1 => JobKind::ExtractMeetingAtoms {
                graph_id,
                source_version_id,
                start_char,
            },
            Self::SourceV2 | Self::SourceV3 => JobKind::ExtractSourceAtoms {
                graph_id,
                source_version_id,
                start_char,
            },
        }
    }
    fn key(self, version: Uuid, start: usize) -> String {
        let prefix = match self {
            Self::MeetingV1 => "meeting-atoms",
            Self::SourceV2 | Self::SourceV3 => "source-atoms",
        };
        format!("{prefix}:{version}:{start}")
    }
    fn input(self, window: &str, source: &crate::source_evidence::SourceVersion) -> String {
        match self {
            Self::MeetingV1 => window.to_string(),
            Self::SourceV2 => v2::input_v2(
                window,
                &source.source_type,
                &source.representation,
                v2::author_label(&source.source_metadata).as_deref(),
                Some(source.occurred_at.unwrap_or(source.captured_at).date_naive()),
            ),
            Self::SourceV3 => v3::input_v3(
                window,
                &source.source_type,
                &source.representation,
                v2::author_label(&source.source_metadata).as_deref(),
                v3::source_date(source.occurred_at),
                v3::source_anchor(&source.source_metadata, &source.source_id),
            ),
        }
    }
    /// What the response may carry beyond atoms, and the date it was told.
    fn contract(self, source: &crate::source_evidence::SourceVersion) -> v2::Contract {
        match self {
            Self::SourceV3 => v2::Contract {
                changes: true,
                source_date: v3::source_date(source.occurred_at),
                code: v3::is_code_source(&source.source_type),
                self_names: v3::self_names(&v3::source_anchor(&source.source_metadata, &source.source_id)),
            },
            // v2 code captures (the backlog still pinned to v2) get the same
            // code rules as v3: no people, no kinship, only code-to-code
            // `mentions` (prod 2026-10-22: 30 kinship edges from v2-extracted
            // test files and `relationships/inference.rs`).
            Self::SourceV2 => v2::Contract { code: v3::is_code_source(&source.source_type), ..Default::default() },
            _ => v2::Contract::default(),
        }
    }
    fn author(self, source: &crate::source_evidence::SourceVersion) -> Option<String> {
        match self {
            Self::SourceV2 | Self::SourceV3 => v2::author_label(&source.source_metadata),
            Self::MeetingV1 => None,
        }
    }
}
pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);
const WINDOW_CHARS: usize = 8_000;
const MAX_ATOMS: usize = 40;
const MAX_TEXT_CHARS: usize = 800;
const MAX_QUOTE_CHARS: usize = 4_000;
const MAX_ENTITIES: usize = 8;
const MAX_MENTION_CHARS: usize = 200;
const MANAGED_HOLD: &str = "managed_hold";

/// Jobs claimed and processed concurrently per tick. Each job holds at most
/// one content and one control connection, and only between model calls, so
/// pool use stays bounded by this number (the org worker pool has 11
/// connections shared by every worker). `SOURCE_ATOMS_CONCURRENCY`, 1..=12.
pub const CONCURRENCY_ENV: &str = "SOURCE_ATOMS_CONCURRENCY";
const DEFAULT_CONCURRENCY: usize = 4;
const MAX_CONCURRENCY: usize = 12;

pub fn concurrency() -> usize {
    std::env::var(CONCURRENCY_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| (1..=MAX_CONCURRENCY).contains(n))
        .unwrap_or(DEFAULT_CONCURRENCY)
}

/// A capture this recent is fresh: it is claimed in the fresh lane and
/// adopted ahead of the standing backlog cursor.
pub(crate) const FRESH_CAPTURE_WINDOW: Duration = Duration::from_secs(24 * 3600);
const KINDS: &[&str] = &["ExtractMeetingAtoms", "ExtractSourceAtoms", "ExtractStandingMeetingAtoms", "ExtractStandingSourceAtoms"];
/// Consumer accounts read per tick for fresh-lane candidates.
const FRESH_LANE_ACCOUNTS: i64 = 16;

/// Pending windows of fresh captures, one per (graph, source type) per round,
/// the pair with the fewest pending windows first and the newest capture
/// first within a pair. A burst in one ocean or source type then waits behind
/// a single new capture elsewhere instead of in front of it. Newest-first
/// (by `captured_at`, not by enqueue time) keeps a capture that just arrived
/// ahead of windows that recovery or a repin re-enqueued hours after their
/// capture. A document's later windows carry its `captured_at`, so they keep
/// their document's place. Nothing starves: an item the lane passes over keeps
/// its `run_after` and ages in the oldest-first half of every tick. Reading
/// source versions needs a scope: the org plane reads once as the tenant
/// system actor, the consumer plane once per account with pending windows.
///
/// Only live captures ride the lane ([`crate::capture_admission_health::LIVE_CAPTURE`]).
/// A re-capture of history (2026-10-20: about 1,300 Slack messages more than
/// a week old, re-captured in one afternoon) has a new `captured_at` but an
/// old `occurred_at`; newest-first by `captured_at` put the whole burst ahead
/// of the live messages in its pair for hours. It drains oldest-first with
/// the rest of the backlog.
async fn fresh_lane(tenant: &WorkerTenant, limit: usize) -> Result<Vec<Uuid>, crate::store::StoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut lane = Vec::new();
    match &tenant.tenant {
        TenantId::Org(_) => {
            let mut tx = begin_tenant_system_scoped_without_account(&tenant.pool).await?;
            lane = fresh_lane_rows(&mut tx, None, limit).await?;
            tx.rollback().await?;
        }
        TenantId::Consumer => {
            // Accounts whose windows were enqueued most recently: a fresh
            // capture's window is among an account's newest jobs.
            let accounts: Vec<Uuid> = sqlx::query_scalar("SELECT account_id FROM public.jobs WHERE status='pending' AND run_after<=now() AND kind=ANY($1) AND account_id IS NOT NULL AND created_at>=now()-make_interval(secs=>$3) GROUP BY account_id ORDER BY max(created_at) DESC LIMIT $2")
                .bind(KINDS).bind(FRESH_LANE_ACCOUNTS).bind(FRESH_CAPTURE_WINDOW.as_secs_f64()).fetch_all(&tenant.pool).await?;
            for account in accounts {
                let mut tx = begin_account_scoped(&tenant.pool, account).await?;
                lane.extend(fresh_lane_rows(&mut tx, Some(account), limit).await?);
                tx.rollback().await?;
            }
            lane.sort_by_key(|(id, rank, pending, captured_at)| (*rank, *pending, std::cmp::Reverse(*captured_at), *id));
            lane.truncate(limit);
        }
    }
    Ok(lane.into_iter().map(|(id, ..)| id).collect())
}

async fn fresh_lane_rows(
    tx: &mut Transaction<'static, Postgres>,
    account: Option<Uuid>,
    limit: usize,
) -> Result<Vec<(Uuid, i64, i64, chrono::DateTime<chrono::Utc>)>, sqlx::Error> {
    sqlx::query_as(&format!("SELECT id,lane_rank,pair_pending,captured_at FROM (SELECT j.id,j.run_after,v.captured_at,row_number() OVER (PARTITION BY v.graph_id,v.source_type ORDER BY v.captured_at DESC,j.run_after,j.id) AS lane_rank,count(*) OVER (PARTITION BY v.graph_id,v.source_type) AS pair_pending \
        FROM public.jobs j JOIN public.source_versions v ON v.version_id=CASE WHEN j.payload->>'source_version_id' ~* '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$' THEN (j.payload->>'source_version_id')::uuid END AND v.graph_id::text=j.payload->>'graph_id' \
        WHERE j.status='pending' AND j.run_after<=now() AND j.kind=ANY($1) AND j.created_at>=now()-make_interval(secs=>$2) AND v.captured_at>=now()-make_interval(secs=>$2) AND {live} AND ($3::uuid IS NULL OR j.account_id=$3)) lane \
        ORDER BY lane_rank,pair_pending,captured_at DESC,run_after,id LIMIT $4", live = crate::capture_admission_health::LIVE_CAPTURE))
        .bind(KINDS).bind(FRESH_CAPTURE_WINDOW.as_secs_f64()).bind(account).bind(limit as i64)
        .fetch_all(&mut **tx).await
}

/// How long a refilling tick keeps claiming into freed slots. After it, the
/// tick claims nothing more and returns once its in-flight windows finish, so
/// the harness can serve the other tenants (it runs them one at a time).
pub const REFILL_BUDGET: Duration = Duration::from_secs(120);

/// Free slots by lane: the first `concurrency / 2` slots prefer the fresh
/// lane, the rest claim oldest-first. A fresh slot the lane cannot fill
/// claims oldest-first too.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FreeSlots {
    pub fresh: usize,
    pub backlog: usize,
}

impl FreeSlots {
    fn total(self) -> usize {
        self.fresh + self.backlog
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lane {
    Fresh,
    Backlog,
}

/// Claims into `free`: fresh-lane windows first (at most `free.fresh`), then
/// oldest-first for every slot still free. With every slot free this is the
/// old per-tick split: half from the fresh lane, the rest oldest-first.
async fn claim_slots(tenant: &WorkerTenant, free: FreeSlots) -> Result<Vec<JobRow>, String> {
    // The lane only reorders claims. If it cannot be read, the tick still
    // claims oldest-first rather than claiming nothing.
    let fresh = fresh_lane(tenant, free.fresh).await.unwrap_or_else(|error| {
        tracing::warn!(alert=true, error=%error, "source_atoms_fresh_lane_unavailable");
        Vec::new()
    });
    let kinds = crate::managed::standing::execution::claimable_kinds(KINDS);
    let mut jobs = queue::dequeue_ids(&tenant.pool, &kinds, &fresh)
        .await
        .map_err(|_| "source atom dequeue failed".to_string())?;
    let rest = free.total().saturating_sub(jobs.len());
    if rest > 0 {
        jobs.extend(
            queue::dequeue(&tenant.pool, &kinds, rest)
                .await
                .map_err(|_| "source atom dequeue failed".to_string())?,
        );
    }
    Ok(jobs)
}

/// Runs claimed windows in `limit` slots. The first claim fills every slot;
/// while `budget` has not elapsed and `stop` is false, each slot that frees
/// is claimed into again, instead of every slot waiting for the slowest
/// window of its batch. A claim that returns nothing ends the refilling.
/// Returns the number of windows claimed.
///
/// 2026-10-22 (prod, coralswarm tenant): with a batch barrier, every tick
/// waited for its slowest window (about 90 s) while the average window took
/// about 35 s, so 8 slots did the work of about 3 (290 windows an hour).
pub(crate) async fn run_slots<J, C, CF, P, PF>(
    limit: usize,
    budget: Duration,
    stop: impl Fn() -> bool,
    mut claim: C,
    process: P,
) -> Result<usize, String>
where
    C: FnMut(FreeSlots) -> CF,
    CF: std::future::Future<Output = Result<Vec<J>, String>>,
    P: Fn(J) -> PF,
    PF: std::future::Future<Output = ()>,
{
    use futures::stream::{FuturesUnordered, StreamExt};
    let started = tokio::time::Instant::now();
    let mut free = FreeSlots { fresh: limit / 2, backlog: limit - limit / 2 };
    let mut running = FuturesUnordered::new();
    let mut claimed = 0usize;
    let mut first = true;
    let mut refilling = true;
    let release = |free: &mut FreeSlots, lane: Lane| match lane {
        Lane::Fresh => free.fresh += 1,
        Lane::Backlog => free.backlog += 1,
    };
    loop {
        if refilling && free.total() > 0 && !first && (started.elapsed() >= budget || stop()) {
            refilling = false;
        }
        if refilling && free.total() > 0 {
            let asked = free;
            free = FreeSlots::default();
            // Running windows keep progressing while the claim is in flight.
            let claim_fut = claim(asked);
            tokio::pin!(claim_fut);
            let result = loop {
                tokio::select! {
                    result = &mut claim_fut => break result,
                    Some(lane) = running.next(), if !running.is_empty() => release(&mut free, lane),
                }
            };
            match result {
                Ok(jobs) => {
                    if jobs.is_empty() {
                        refilling = false;
                    }
                    let mut fresh_left = asked.fresh;
                    let returned = jobs.len();
                    for job in jobs {
                        let lane = if fresh_left > 0 {
                            fresh_left -= 1;
                            Lane::Fresh
                        } else {
                            Lane::Backlog
                        };
                        let work = process(job);
                        running.push(async move {
                            work.await;
                            lane
                        });
                    }
                    claimed += returned;
                    // Slots the claim could not fill stay free for a later round.
                    let unfilled = asked.total().saturating_sub(returned);
                    let fresh_unfilled = fresh_left.min(unfilled);
                    free.fresh += fresh_unfilled;
                    free.backlog += unfilled - fresh_unfilled;
                }
                Err(error) if first => return Err(error),
                Err(error) => {
                    tracing::warn!(error=%error, "source_atoms_refill_claim_failed");
                    free.fresh += asked.fresh;
                    free.backlog += asked.backlog;
                    refilling = false;
                }
            }
            first = false;
            continue;
        }
        match running.next().await {
            Some(lane) => release(&mut free, lane),
            None => return Ok(claimed),
        }
    }
}

/// One claim of up to [`concurrency`] windows, run to completion: no slot is
/// refilled. Tests drive the worker one batch at a time through this.
pub async fn tick(
    tenant: &WorkerTenant,
    control_pool: &PgPool,
    llm: &dyn LlmProvider,
    r2: Option<&R2Client>,
) -> Result<Tick, String> {
    run_tick(tenant, control_pool, llm, r2, Duration::ZERO, &|| false).await
}

/// The production tick: refills freed slots for up to [`REFILL_BUDGET`], and
/// stops claiming as soon as shutdown is signalled.
pub async fn tick_refilling(
    tenant: &WorkerTenant,
    control_pool: &PgPool,
    llm: &dyn LlmProvider,
    r2: Option<&R2Client>,
    shutdown: &crate::shutdown::Shutdown,
) -> Result<Tick, String> {
    run_tick(tenant, control_pool, llm, r2, REFILL_BUDGET, &|| shutdown.is_triggered()).await
}

async fn run_tick(
    tenant: &WorkerTenant,
    control_pool: &PgPool,
    llm: &dyn LlmProvider,
    r2: Option<&R2Client>,
    budget: Duration,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Tick, String> {
    // Windows of one version never run concurrently: a continuation is only
    // enqueued when its predecessor commits. Distinct versions are independent.
    let count = run_slots(
        concurrency(),
        budget,
        stop,
        move |free| claim_slots(tenant, free),
        move |job: JobRow| {
            crate::jobs::publication::scope(
                job.id,
                process_job(
                    &tenant.tenant,
                    &tenant.pool,
                    control_pool,
                    tenant.store.as_ref(),
                    llm,
                    r2,
                    job,
                ),
            )
        },
    )
    .await?;
    Ok(if count == 0 { Tick::idle() } else { Tick::worked(count) })
}

#[cfg(test)]
mod refill_tests {
    use super::{run_slots, FreeSlots};
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::time::Duration;

    /// Drives `run_slots` over windows that take the given seconds. Returns
    /// (claimed, each claim's free slots, elapsed seconds).
    async fn drive(limit: usize, budget: u64, windows: &[u64], stop: impl Fn() -> bool) -> (usize, Vec<FreeSlots>, u64) {
        let queue = RefCell::new(windows.iter().copied().collect::<VecDeque<u64>>());
        let asks = RefCell::new(Vec::new());
        let started = tokio::time::Instant::now();
        let claimed = run_slots(
            limit,
            Duration::from_secs(budget),
            stop,
            |free: FreeSlots| {
                asks.borrow_mut().push(free);
                let mut queue = queue.borrow_mut();
                let n = (free.fresh + free.backlog).min(queue.len());
                let jobs: Vec<u64> = queue.drain(..n).collect();
                async move { Ok(jobs) }
            },
            |secs: u64| tokio::time::sleep(Duration::from_secs(secs)),
        )
        .await
        .unwrap();
        (claimed, asks.into_inner(), started.elapsed().as_secs())
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_window_does_not_hold_the_other_slots() {
        // One 100 s window and a queue of 10 s windows, two slots, 60 s budget.
        let mut windows = vec![100];
        windows.extend([10; 20]);
        let (claimed, _, elapsed) = drive(2, 60, &windows, || false).await;
        // The free slot is refilled at 10, 20, 30, 40 and 50 s; at 60 s the
        // budget is spent and the tick drains. A batch barrier claims 2.
        assert_eq!(claimed, 7);
        assert_eq!(elapsed, 100);
    }

    #[tokio::test(start_paused = true)]
    async fn a_freed_fresh_slot_is_claimed_as_a_fresh_slot() {
        let (_, asks, _) = drive(2, 30, &[5, 50, 5, 5], || false).await;
        assert_eq!(asks[0], FreeSlots { fresh: 1, backlog: 1 });
        assert_eq!(asks[1], FreeSlots { fresh: 1, backlog: 0 });
    }

    #[tokio::test(start_paused = true)]
    async fn zero_budget_claims_once_and_drains() {
        let (claimed, asks, elapsed) = drive(4, 0, &[1, 2, 3, 4, 5, 6], || false).await;
        assert_eq!((claimed, asks.len(), elapsed), (4, 1, 4));
        assert_eq!(asks[0], FreeSlots { fresh: 2, backlog: 2 });
    }

    #[tokio::test(start_paused = true)]
    async fn stop_ends_refilling_but_finishes_claimed_windows() {
        let (claimed, asks, elapsed) = drive(2, 600, &[10, 20, 30, 40], || true).await;
        assert_eq!((claimed, asks.len(), elapsed), (2, 1, 20));
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_claim_ends_the_tick() {
        let (claimed, asks, elapsed) = drive(2, 600, &[10, 20, 30], || false).await;
        // 10 s: refill takes the last window; 20 s: the queue is empty.
        assert_eq!((claimed, asks.len(), elapsed), (3, 3, 40));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_refill_keeps_what_was_claimed() {
        let calls = RefCell::new(0);
        let claimed = run_slots(
            2,
            Duration::from_secs(600),
            || false,
            |free: FreeSlots| {
                *calls.borrow_mut() += 1;
                let first = *calls.borrow() == 1;
                let n = free.fresh + free.backlog;
                async move { if first { Ok(vec![5u64; n]) } else { Err("dequeue failed".to_string()) } }
            },
            |secs: u64| tokio::time::sleep(Duration::from_secs(secs)),
        )
        .await;
        assert_eq!(claimed, Ok(2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_first_claim_is_an_error() {
        let claimed = run_slots(
            2,
            Duration::from_secs(600),
            || false,
            |_free: FreeSlots| async { Err::<Vec<u64>, _>("dequeue failed".to_string()) },
            |secs: u64| tokio::time::sleep(Duration::from_secs(secs)),
        )
        .await;
        assert!(claimed.is_err());
    }
}

async fn begin_scope<'a>(
    scope: &JobScope<'a>,
) -> Result<Transaction<'a, Postgres>, crate::store::StoreError> {
    match scope {
        JobScope::Consumer { pool, account_id } => begin_account_scoped(pool, *account_id).await,
        JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool).await,
    }
}

/// Public for end-to-end tests with a real queue/database and a mock provider.
/// All errors persisted to jobs/status are fixed categories, never source text
/// or model output. Every semantic write uses the owning job's RLS scope.
#[allow(clippy::too_many_arguments)]
pub async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    control_pool: &PgPool,
    store: &dyn Store,
    llm: &dyn LlmProvider,
    r2: Option<&R2Client>,
    job: JobRow,
) {
    if matches!(job.kind.as_str(), "ExtractStandingSourceAtoms" | "ExtractStandingMeetingAtoms") {
        crate::managed::standing::execution::StandingExecutor::new(tenant, pool, control_pool, store, llm, r2)
            .process_job(job).await;
        return;
    }
    process_source_job(tenant, pool, control_pool, store, llm, r2, job, None).await;
}

pub(crate) async fn process_standing_job(
    tenant: &TenantId, pool: &PgPool, control_pool: &PgPool, store: &dyn Store,
    llm: &dyn LlmProvider, r2: Option<&R2Client>, job: JobRow,
    work: &crate::managed::standing::execution::StandingWork,
) {
    #[cfg(feature="test-support")]
    crate::managed::standing::probe::checkpoint(job.id, crate::managed::standing::probe::Stage::Claimed).await;
    process_source_job(tenant, pool, control_pool, store, llm, r2, job, Some(work)).await;
}

async fn process_source_job(
    tenant: &TenantId, pool: &PgPool, control_pool: &PgPool, store: &dyn Store,
    llm: &dyn LlmProvider, r2: Option<&R2Client>, job: JobRow,
    standing: Option<&crate::managed::standing::execution::StandingWork>,
) {
    let parsed = serde_json::from_value::<JobKind>(json!({"kind":job.kind,"payload":job.payload}));
    let (graph_id, source_version_id, start_char) = match parsed {
        Ok(JobKind::ExtractMeetingAtoms {
            graph_id,
            source_version_id,
            start_char,
        })
        | Ok(JobKind::ExtractSourceAtoms {
            graph_id,
            source_version_id,
            start_char,
        })
        | Ok(JobKind::ExtractStandingMeetingAtoms { graph_id, source_version_id, start_char })
        | Ok(JobKind::ExtractStandingSourceAtoms { graph_id, source_version_id, start_char }) => (graph_id, source_version_id, start_char),
        _ => {
            let _ = queue::could_not_do(
                pool,
                job.id,
                queue::FailureClass::Structural,
                "invalid source atom job",
            )
            .await;
            return;
        }
    };
    if start_char < 0 || start_char > i32::MAX as i64 {
        let _ = queue::could_not_do(
            pool,
            job.id,
            queue::FailureClass::Structural,
            "invalid window position",
        )
        .await;
        return;
    }
    let Some(account_id) = job.account_id else {
        let _ = queue::fail(pool, job.id, "source atom job missing account").await;
        return;
    };
    // Unlike general graph workers, unknown ownership must never inherit a
    // stamped account's permissive egress policy, even on a BYPASSRLS pool.
    let owner = sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_graph_account_id($1)")
        .bind(graph_id)
        .fetch_one(pool)
        .await;
    match owner {
        Ok(Some(owner)) if owner == account_id => {}
        Ok(None) => {
            let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
            return;
        }
        Ok(Some(_)) => {
            let _ = queue::could_not_do(
                pool,
                job.id,
                queue::FailureClass::Structural,
                "source atom job owner mismatch",
            )
            .await;
            return;
        }
        Err(_) => {
            let _ = queue::fail(pool, job.id, "source owner lookup failed").await;
            return;
        }
    }
    let scope = JobScope::for_job(tenant, pool, account_id);
    // Admission can inspect readiness, never raw text/blob locators/context.
    let version = async {
        let mut tx = begin_scope(&scope).await?;
        let version = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT v.extraction_version,v.extraction_status,v.next_start_char FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE v.graph_id=$1 AND v.version_id=$2 AND s.access_state='permitted'",
        ).bind(graph_id).bind(source_version_id).fetch_optional(&mut *tx).await?;
        tx.rollback().await?;
        Ok::<_, crate::store::StoreError>(version)
    }
    .await;
    let version = match version {
        Ok(Some(version)) => version,
        Ok(None) => {
            #[cfg(feature="test-support")]
            if standing.is_some() { crate::managed::standing::probe::checkpoint(job.id, crate::managed::standing::probe::Stage::MetadataMissing).await; }
            let ledger=async {
                let mut tx=begin_scope(&scope).await?;
                if let Some(work) = standing {
                    crate::managed::standing::execution::hold_work_tx(&mut tx,work,"source_removed_or_access_lost").await?;
                    tx.commit().await?;
                    return Ok::<_,crate::store::StoreError>(());
                }
                sqlx::query("UPDATE corpus_enrichment_units SET state='unavailable',reason='source_removed_or_access_lost',updated_at=now() WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND execution_owner")
                    .bind(graph_id).bind(source_version_id).execute(&mut *tx).await?;
                sqlx::query("UPDATE corpus_capture_admissions SET state='unavailable',reason='source_removed_or_access_lost',updated_at=now() WHERE graph_id=$1 AND source_version_id=$2 AND state<>'processed'").bind(graph_id).bind(source_version_id).execute(&mut *tx).await?;
                tx.commit().await?;
                Ok::<_,crate::store::StoreError>(())
            }.await;
            if ledger.is_err() {
                let _ = queue::fail(pool, job.id, "managed run storage error").await;
                return;
            }
            let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
            return;
        }
        Err(_) => {
            fail(standing,
                &scope,
                pool,
                &job,
                graph_id,
                source_version_id,
                start_char,
                "source storage error",
            )
            .await;
            return;
        }
    };
    let Some(profile) = ExtractionProfile::from_version(&version.0) else {
        fail(standing,
            &scope,
            pool,
            &job,
            graph_id,
            source_version_id,
            start_char,
            "unsupported extraction version",
        )
        .await;
        return;
    };
    if version.1 == "done" || version.2 > start_char {
        let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
        return;
    }
    if version.2 != start_char {
        fail(standing,
            &scope,
            pool,
            &job,
            graph_id,
            source_version_id,
            start_char,
            "out of order extraction window",
        )
        .await;
        return;
    }
    // Private text/locators are read only on the same held transaction that
    // just passed the fresh fence: no second pool wait separates admission
    // from the retained-original read.
    #[cfg(feature="test-support")]
    if standing.is_some() { crate::managed::standing::probe::checkpoint(job.id, crate::managed::standing::probe::Stage::BeforePrivateFence).await; }
    let initial=async {
        let mut tx=begin_scope(&scope).await?;
        let mut control = if standing.is_some() {
            crate::managed::standing::execution::begin_control(tenant, control_pool, account_id).await?
        } else { None };
        let allowed = if let Some(work) = standing {
            crate::managed::standing::execution::bound(&mut tx, control.as_mut()).await?;
            crate::managed::standing::execution::attempt_allowed_locked(&mut tx, control.as_mut(), tenant, work).await?
        } else {
            crate::corpus_enrichment::window_access_allowed_tx(&mut tx,control_pool,tenant,graph_id,source_version_id,profile.version()).await?
        };
        let version=if allowed {
            Some(crate::source_evidence::load_version_scoped(&mut tx, graph_id, source_version_id).await?)
        } else if let Some(work) = standing {
            let reason=crate::managed::standing::execution::denial_reason_locked(&mut tx,control.as_mut(),work).await?;
            crate::managed::standing::execution::hold_work_tx(&mut tx,work,reason.code()).await?;
            None
        } else {
            sqlx::query("UPDATE source_versions SET extraction_status='blocked',extraction_error='managed_admission_required' WHERE graph_id=$1 AND version_id=$2 AND extraction_status<>'done' AND NOT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND execution_owner)").bind(graph_id).bind(source_version_id).execute(&mut *tx).await?;
            None
        };
        if let Some(control) = control { control.rollback().await?; }
        tx.commit().await?;Ok::<_,crate::store::StoreError>(version)
    }.await;
    let version = match initial {
        Ok(Some(Some(version)))=>version,
        Ok(Some(None))=>{let _=queue::complete(pool,job.id,queue::JobOutcome::NothingToDo).await;return;},
        Ok(None)=>{let _=queue::complete(pool,job.id,queue::JobOutcome::NothingToDo).await;return;},
        Err(_)=>{let _=queue::fail(pool,job.id,"managed admission storage error").await;return;},
    };
    // Resolve only the captured version's artifact, never the mutable latest
    // transcript or correction overlay. Strict UTF-8 rejects damaged evidence.
    let mut version = version;
    let original_blob_key = version.raw_blob_key.clone();
    let original_declared_hash = version.content_sha256.clone();
    let text = if let Some(raw) = version.raw_inline.take() {
        Ok(raw)
    } else if let (Some(key), Some(r2)) = (version.raw_blob_key.clone(), r2) {
        let org = match tenant { TenantId::Consumer => None, TenantId::Org(org) => Some(org.as_str()) };
        if !crate::source_evidence::artifact_locator_admitted(graph_id, &key, org) {
            Err("source locator unavailable")
        } else {
        match crate::crypto::resolve_dek_for_tenant(tenant, pool, account_id).await {
            Ok(Some(dek)) => match r2.get_atom_blob(&key, &dek).await {
                Ok(bytes) => String::from_utf8(bytes).map_err(|_| "invalid source encoding"),
                Err(_) => Err("source text unavailable"),
            },
            _ => Err("source key unavailable"),
        }
        }
    } else {
        Err("source text unavailable")
    };
    let text = match text {
        Ok(text) => text,
        Err(category) => {
            fail(standing,
                &scope,
                pool,
                &job,
                graph_id,
                source_version_id,
                start_char,
                category,
            )
            .await;
            return;
        }
    };
    if version.content_sha256.as_deref().is_some_and(|expected| {
        expected != crate::services::source_reingest::content_hash_hex(&text)
    }) {
        fail(standing,
            &scope,
            pool,
            &job,
            graph_id,
            source_version_id,
            start_char,
            "source content changed",
        )
        .await;
        return;
    }
    // A queue retry of this window (attempt 2, 3) starts on a smaller span:
    // its earlier attempts ran out of time or tokens on the full one.
    let first_limit = retry_window_limit(WINDOW_CHARS, u32::try_from(job.attempts).unwrap_or(0));
    let mut selected = match window_sized(&text, start_char as usize, first_limit) {
        Ok(w) if w.end_char <= i32::MAX as usize => w,
        _ => {
            fail(standing,
                &scope,
                pool,
                &job,
                graph_id,
                source_version_id,
                start_char,
                "invalid window position",
            )
            .await;
            return;
        }
    };
    let gate = admit_window(standing, &scope, tenant, control_pool, account_id, graph_id,
        source_version_id, profile, window_need(&prompt_window(profile, &version, &text, &selected))).await;
    let mut model = match gate {
        Ok(crate::corpus_enrichment::WindowGate::Permit { model }) => model,
        Ok(crate::corpus_enrichment::WindowGate::Hold) => {
            let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
            return;
        }
        Err(_) => {
            fail(standing,
                &scope,
                pool,
                &job,
                graph_id,
                source_version_id,
                start_char,
                "managed run storage error",
            )
            .await;
            return;
        }
    };
    #[cfg(feature="test-support")]
    if standing.is_some() { crate::managed::standing::probe::checkpoint(job.id, crate::managed::standing::probe::Stage::BeforeRunningStatus).await; }
    let running = set_status(
        standing, &scope,
        graph_id,
        source_version_id,
        start_char,
        "running",
        None,
    )
    .await;
    if matches!(running, Ok(false)) {
        let _=queue::complete(pool,job.id,queue::JobOutcome::NothingToDo).await;
        return;
    }
    if running.is_err() {
        fail(standing,
            &scope,
            pool,
            &job,
            graph_id,
            source_version_id,
            start_char,
            "status storage error",
        )
        .await;
        return;
    }
    #[cfg(feature="test-support")]
    if standing.is_some() { crate::managed::standing::probe::checkpoint(job.id, crate::managed::standing::probe::Stage::BeforeChargePrincipal).await; }
    let principal = match charge_principal(
        &scope,
        control_pool,
        account_id,
        version.ingested_by,
        graph_id,
        source_version_id,
        profile.version(),
    )
    .await
    {
        Ok(principal) => principal,
        Err(_) => {
            fail(standing, &scope, pool, &job, graph_id, source_version_id, start_char, "managed run storage error").await;
            return;
        }
    };
    // Attribution is captured with this exact typed source version. Never infer
    // it from mutable legacy chunks or a provider-ambiguous native source ID.
    let usage = crate::llm::usage::UsageContext::new(
        pool.clone(),
        scope.usage_account_id(),
        Some(graph_id),
    )
    .with_source_job(job.id)
    .with_attributed_user_opt(version.ingested_by.or(principal));
    let author = profile.author(&version);
    let contract = profile.contract(&version);
    // At most one retry per window, on a smaller span, for a model-side
    // failure a smaller input can fix: truncation, a timeout, or a response
    // with no valid atom. The retry is a new logical call: re-admitted (run
    // budget, fences) and reserved/settled on its own receipts, so no attempt
    // is ever charged twice. A second failure ends as before.
    let mut retried: Option<&'static str> = None;
    let result = loop {
        let prompt = prompt_window(profile, &version, &text, &selected);
        let attempt = if selected.text.trim().is_empty() {
            WindowCall::Done(Ok((Vec::new(), model.clone())))
        } else if let v3::CodeWindow::Skip(reason) = prompt {
            // Test code states nothing about the product: no call, and the
            // window is done. Counted here, by reason.
            // A test file keeps one "test file for X" atom on its first
            // window, so it still counts as extracted and can be found.
            let kept: Vec<ValidatedAtom> = match reason {
                v3::CodeSkip::TestPath => test_file_atom(&version, &selected).into_iter().collect(),
                v3::CodeSkip::TestModule => Vec::new(),
            };
            tracing::info!(%graph_id, version = %source_version_id, profile = profile.version(),
                reason = reason.as_str(), chars = selected.end_char - selected.start_char,
                kept_atoms = kept.len(), "source_atoms_test_code_skipped");
            WindowCall::Done(Ok((kept, model.clone())))
        } else if let Some(principal) = principal {
            let call = crate::managed::ManagedCall {
                plane: &scope,
                control: control_pool,
                tenant,
                account_id,
                principal,
                // Source extraction's own fallback, so the whole request can
                // run with reasoning off. The Jev seat below keeps the global map.
                fallback_model: matches!(profile, ExtractionProfile::SourceV2 | ExtractionProfile::SourceV3)
                    .then(crate::llm::model_config::source_extraction_fallback_model),
                subject: match standing {
                    Some(work) => crate::managed::Subject::StandingWindow { work },
                    None => crate::managed::Subject::Window { graph: graph_id, version: source_version_id,
                        profile: profile.version(), job: job.id },
                },
            };
            let window_ref = &selected;
            let response = crate::llm::usage::metered_complete_managed(
                llm,
                CompletionRequest {
                    messages: vec![Message {
                        role: Role::User,
                        content: profile.input(match &prompt {
                            v3::CodeWindow::Send(text) => text,
                            v3::CodeWindow::Skip(_) => selected.text,
                        }, &version),
                    }],
                    model: model.clone(),
                    max_tokens: profile.max_tokens(),
                    system: Some(profile.system().to_string()),
                    tool_schema: Some(tool_schema(profile)),
                    effort: None,
                    reasoning: crate::llm::ReasoningSetting::Off,
                },
                &usage,
                profile.task(),
                call,
                standing.is_some() && retried.is_some(),
                |response| {
                    let mut diagnostics = crate::llm::ExtractionDiagnostics {
                        window_retry: retried,
                        ..Default::default()
                    };
                    let stated = matches!(response.stop_reason.as_deref(), Some("max_tokens" | "length"));
                    // A provider can stop at the token cap and still report
                    // a normal stop: a full budget is a truncation either way.
                    let silent = !stated && response.output_tokens >= profile.max_tokens();
                    let result = if stated || silent {
                        diagnostics.envelope_error = Some(crate::llm::ExtractionEnvelopeError::Truncated);
                        diagnostics.silent_truncation = silent;
                        Err(TRUNCATED)
                    } else if profile.structured() {
                        v2::validate_structured(&response.content, window_ref, author.as_deref(), contract.clone(), &mut diagnostics)
                    } else {
                        validate_output_with_diagnostics(&response.content, window_ref, &mut diagnostics)
                    };
                    // v3, on the retry: a window whose every atom quotes text
                    // it does not contain has nothing to anchor. It finishes
                    // with no atoms rather than failing the version, whose
                    // failure would hide the item's earlier facts.
                    let result = match result {
                        Err(NO_VALID_ATOMS) if profile == ExtractionProfile::SourceV3 && retried.is_some()
                            && unanchorable(&diagnostics) =>
                        {
                            diagnostics.unanchorable_window = true;
                            Ok(Vec::new())
                        }
                        other => other,
                    };
                    log_window_validation(profile, graph_id, source_version_id, window_ref, &diagnostics);
                    crate::llm::report_extraction_diagnostics(diagnostics);
                    result
                },
            )
            .await;
            match response {
                Ok((_, Err(category @ (TRUNCATED | NO_VALID_ATOMS | INVALID_SCHEMA)))) => WindowCall::Retry(category),
                Ok((response, validated)) => WindowCall::Done(validated.map(|atoms| (atoms, response.model))),
                Err(LlmError::Managed(Deny::Hold(code))) => WindowCall::Done(if let Some(work)=standing {
                    #[cfg(feature="test-support")]
                    if code==HoldCode::Fenced {
                        crate::managed::standing::probe::checkpoint(job.id, crate::managed::standing::probe::Stage::FundedFenceDenied).await;
                    }
                    let held = async {
                        let mut tx = begin_scope(&scope).await?;
                        let mut control = crate::managed::standing::execution::begin_control(tenant,control_pool,account_id).await?;
                        let reason = if code==HoldCode::Fenced {
                            crate::managed::standing::execution::denial_reason_locked(&mut tx,control.as_mut(),work).await?.code()
                        } else { code.code() };
                        crate::managed::standing::execution::hold_work_tx(&mut tx,work,reason).await?;
                        if let Some(control)=control { control.rollback().await?; }
                        tx.commit().await?;
                        Ok::<_,crate::store::StoreError>(())
                    }.await;
                    if held.is_ok() { Err(MANAGED_HOLD) } else { Err("infrastructure_unavailable") }
                } else { Err(MANAGED_HOLD) }),
                Err(LlmError::Managed(Deny::Refuse(code))) => WindowCall::Done(Err(code.code())),
                Err(LlmError::Managed(Deny::Unavailable)) if standing.is_some() => WindowCall::Done(Err("infrastructure_unavailable")),
                // #1506: a billing lock or entry-queue timeout, or managed
                // storage. Nothing was sent; the job is held, not failed.
                Err(LlmError::Managed(Deny::Unavailable)) => WindowCall::Done(Err(queue::MANAGED_UNAVAILABLE)),
                // The transport already retried this call with backoff. A
                // timeout is retried once more here, on a smaller window.
                Err(LlmError::Timeout | LlmError::Upstream { status: 0, .. }) => WindowCall::Retry(MODEL_TIMEOUT),
                Err(_) if standing.is_some() => WindowCall::Done(Err("infrastructure_unavailable")),
                Err(_) => WindowCall::Done(Err("model request failed")),
            }
        } else {
            #[cfg(feature="test-support")]
            if standing.is_some() { crate::managed::standing::probe::checkpoint(job.id, crate::managed::standing::probe::Stage::NoPayer).await; }
            let held = async {
                let mut tx = begin_scope(&scope).await?;
                let reason = HoldCode::PrincipalUnavailable.code();
                if let Some(work) = standing {
                    crate::managed::standing::execution::hold_work_tx(&mut tx,work,reason).await?;
                    tx.commit().await?;
                    return Ok::<_,crate::store::StoreError>(());
                }
                crate::corpus_enrichment::hold_window_tx(
                    &mut tx, graph_id, source_version_id, profile.version(), reason,
                ).await?;
                sqlx::query("UPDATE source_versions SET extraction_status='blocked',extraction_error=$3 WHERE graph_id=$1 AND version_id=$2 AND extraction_status NOT IN ('done','blocked')")
                    .bind(graph_id).bind(source_version_id).bind(reason).execute(&mut *tx).await?;
                tx.commit().await?;
                Ok::<_, crate::store::StoreError>(())
            }
            .await;
            WindowCall::Done(if held.is_ok() {
                Err(MANAGED_HOLD)
            } else {
                Err("managed run storage error")
            })
        };
        let category = match attempt {
            WindowCall::Done(result) => break result,
            WindowCall::Retry(category) if retried.is_some() => break Err(final_category(category, standing.is_some())),
            WindowCall::Retry(category) => category,
        };
        let length = selected.end_char - selected.start_char;
        let Ok(next) = window_sized(&text, start_char as usize, retry_window_limit(length, 1)) else {
            break Err(final_category(category, standing.is_some()));
        };
        tracing::info!(%graph_id, version = %source_version_id, profile = profile.version(), reason = category,
            from_chars = length, to_chars = next.end_char - next.start_char, "source_atoms_window_retry");
        tokio::time::sleep(WINDOW_RETRY_BACKOFF).await;
        match admit_window(standing, &scope, tenant, control_pool, account_id, graph_id,
            source_version_id, profile, window_need(&prompt_window(profile, &version, &text, &next))).await
        {
            Ok(crate::corpus_enrichment::WindowGate::Permit { model: admitted }) => {
                model = admitted;
                selected = next;
                retried = Some(category);
            }
            Ok(crate::corpus_enrichment::WindowGate::Hold) => break Err(MANAGED_HOLD),
            Err(_) => break Err(if standing.is_some() { "infrastructure_unavailable" } else { "managed run storage error" }),
        }
    };
    let (atoms, model) = match result {
        Ok(result) => result,
        Err(MANAGED_HOLD) => {
            let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
            return;
        }
        // A cohort window whose managed call was `Deny::Unavailable` (#1506).
        // Like every other managed worker, it goes back on the queue under
        // the shared hold rule: same row, no attempt charged, a clearable
        // backoff. The window is pending again, not failed, so the unit and
        // its run stay as they are. (A standing window never gets here: its
        // `Unavailable` holds the standing work as `infrastructure_unavailable`.)
        Err(queue::MANAGED_UNAVAILABLE) => {
            let _ = set_status(None, &scope, graph_id, source_version_id, start_char, "pending", None).await;
            match queue::hold(pool, job.id, queue::MANAGED_UNAVAILABLE).await {
                Ok(held) => tracing::info!(job_id = %job.id, %graph_id, version = %source_version_id, ?held,
                    "source_atoms_window_held_managed_unavailable"),
                Err(e) => tracing::warn!(job_id = %job.id, %graph_id, error = %e,
                    "source_atoms: failed to hold the window; the lease reaper retries it"),
            }
            return;
        }
        Err(category) => {
            if let Some(work) = standing.filter(|_| category=="infrastructure_unavailable") {
                if let Ok(mut tx) = begin_scope(&scope).await {
                    if crate::managed::standing::execution::hold_work_tx(&mut tx, work, category).await.is_ok() {
                        let _ = tx.commit().await;
                    }
                }
                let _ = queue::complete(pool, job.id, queue::JobOutcome::NothingToDo).await;
            } else {
                fail(standing, &scope,pool,&job,graph_id,source_version_id,start_char,category).await;
            }
            return;
        }
    };
    // Jev adjudicates the mentions the ladder cannot bind, under this window's
    // managed purpose. Fail-open: no plan means the fallback ladder.
    let plan = match (profile, standing, principal) {
        (profile, Some(work), Some(principal)) if profile.structured() && !atoms.is_empty() && v2::jev_binding_enabled() => {
            jev_binding_plan(JevSeat {
                scope: &scope, tenant, control: control_pool, store, llm, usage: &usage,
                account_id, principal, work, graph_id, version_id: source_version_id,
                source_type: &version.source_type, author: author.as_deref(), model: &model,
            }, &atoms).await
        }
        _ => v2::BindingPlan::default(),
    };
    #[cfg(feature="test-support")]
    if standing.is_some() { crate::managed::standing::probe::checkpoint(job.id, crate::managed::standing::probe::Stage::BeforePublication).await; }
    match persist_window(
        &scope,
        tenant,
        control_pool,
        store,
        account_id,
        graph_id,
        source_version_id,
        &selected,
        &atoms,
        &model,
        job.id,
        profile,
        standing,
        &crate::services::source_reingest::content_hash_hex(&text),
        original_blob_key.as_deref(),
        original_declared_hash.as_deref(),
        &plan,
    )
    .await
    {
        Ok(wrote) => {
            let completed = queue::complete(
                pool,
                job.id,
                if wrote {
                    queue::JobOutcome::DidWork
                } else {
                    queue::JobOutcome::NothingToDo
                },
            )
            .await;
            #[cfg(feature="test-support")]
            let completed = if standing.is_some() && completed.is_ok() &&
                crate::managed::standing::probe::lose_ack(job.id,crate::managed::standing::probe::Ack::QueueCompletion) {
                Err(sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::ConnectionAborted,"local queue completion acknowledgement lost")))
            } else { completed };
            let _=completed;
        }
        Err(_) => {
            fail(standing,
                &scope,
                pool,
                &job,
                graph_id,
                source_version_id,
                start_char,
                "atom storage error",
            )
            .await;
        }
    }
}

/// What `selected` sends the model: v3 cuts test code out of `github_code`
/// ([`v3::code_window`]); every other profile sends the window as it is.
fn prompt_window<'w>(
    profile: ExtractionProfile,
    version: &crate::source_evidence::SourceVersion,
    text: &str,
    selected: &Window<'w>,
) -> v3::CodeWindow<'w> {
    match profile {
        // Test files and Rust test modules never reach the model, under v2
        // as under v3.
        ExtractionProfile::SourceV2 | ExtractionProfile::SourceV3 => v3::code_window(&version.source_type, &version.source_metadata,
            &version.source_id, text, selected.text, selected.start_char),
        _ => v3::CodeWindow::Send(std::borrow::Cow::Borrowed(selected.text)),
    }
}

/// The single atom a test file keeps instead of a model call: on its first
/// window only, [`v3::test_file_statement`] quoting the file's first line
/// (at most 200 characters). Later windows keep nothing.
fn test_file_atom(version: &crate::source_evidence::SourceVersion, selected: &Window<'_>) -> Option<ValidatedAtom> {
    if selected.start_char != 0 {
        return None;
    }
    let path = v3::code_path(&version.source_metadata, &version.source_id)?;
    let mut offset = 0;
    let line = selected.text.split_inclusive('\n').find_map(|line| {
        let at = offset;
        offset += line.len();
        let trimmed = line.trim();
        (!trimmed.is_empty()).then(|| (at + (line.len() - line.trim_start().len()), trimmed))
    })?;
    let quote: String = line.1.chars().take(200).collect();
    let start_char = selected.start_char + selected.text[..line.0].chars().count();
    Some(ValidatedAtom {
        kind: AtomKind::Observation,
        text: v3::test_file_statement(&path),
        start_char: start_char as i64,
        end_char: (start_char + quote.chars().count()) as i64,
        evidence_quote: quote,
        entities: Vec::new(),
        relations: Vec::new(),
        validity: None,
        changes: Vec::new(),
    })
}

/// The characters a window sends the model, and whether it makes a call.
fn window_need(prompt: &v3::CodeWindow<'_>) -> (i64, bool) {
    match prompt {
        v3::CodeWindow::Send(text) => (text.chars().count() as i64, !text.trim().is_empty()),
        v3::CodeWindow::Skip(_) => (0, false),
    }
}

/// Every rejected atom was rejected for its quote: absent from the window,
/// or found more than once.
fn unanchorable(diagnostics: &crate::llm::ExtractionDiagnostics) -> bool {
    use crate::llm::AtomRejection::{QuoteAmbiguous, QuoteMissing};
    !diagnostics.rejected_atoms.is_empty()
        && diagnostics.rejected_atoms.keys().all(|reason| matches!(reason, QuoteMissing | QuoteAmbiguous))
}

/// Validator outcome: the provider stopped on its output-token limit.
const TRUNCATED: &str = "truncated model response";
/// Internal: the call timed out after the transport's own retries. Never
/// persisted; [`final_category`] maps it to the stored category.
const MODEL_TIMEOUT: &str = "model request timed out";
/// A retried window is at least this long, unless the window itself is
/// shorter (then the same span is tried again).
const MIN_RETRY_WINDOW_CHARS: usize = 1_000;
/// Pause before a window's retry call. The transport has already backed off
/// between its own attempts.
const WINDOW_RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// One window call's outcome inside the worker's bounded retry loop.
enum WindowCall {
    Done(Result<(Vec<ValidatedAtom>, String), &'static str>),
    /// A model-side failure a smaller window can fix.
    Retry(&'static str),
}

/// The stored category of a retryable failure that was not retried, or
/// failed again. Unchanged from before the retry existed: a standing
/// timeout holds as infrastructure and is requeued; a cohort timeout fails
/// the window and the queue's bounded backoff retries the job.
fn final_category(category: &'static str, standing: bool) -> &'static str {
    match category {
        MODEL_TIMEOUT if standing => "infrastructure_unavailable",
        MODEL_TIMEOUT => "model request failed",
        other => other,
    }
}

/// `current` halved `halvings` times (at most three), never below
/// [`MIN_RETRY_WINDOW_CHARS`] and never above `current`.
fn retry_window_limit(current: usize, halvings: u32) -> usize {
    let current = current.max(1);
    (current >> halvings.min(3)).max(MIN_RETRY_WINDOW_CHARS).min(current)
}

/// The per-window record of what validation kept and dropped. Counts and
/// fixed enum keys only: no source text, quote or model output. The same
/// counters are durable in the call's `llm_usage.diagnostics.extraction`.
fn log_window_validation(
    profile: ExtractionProfile,
    graph_id: Uuid,
    version_id: Uuid,
    window: &Window<'_>,
    diagnostics: &crate::llm::ExtractionDiagnostics,
) {
    let rejected: u32 = diagnostics.rejected_atoms.values().sum();
    let dropped_entities: u32 = diagnostics.dropped_entities.values().sum();
    let dropped_relations: u32 = diagnostics.relations.as_ref()
        .map(|r| r.rejected_relations.values().sum()).unwrap_or(0);
    tracing::info!(
        %graph_id,
        version = %version_id,
        profile = profile.version(),
        start_char = window.start_char,
        end_char = window.end_char,
        accepted = diagnostics.accepted_atoms,
        rejected_atoms = rejected,
        dropped_entities,
        dropped_relations,
        capped_atoms = diagnostics.capped_atoms,
        envelope_error = ?diagnostics.envelope_error,
        window_retry = diagnostics.window_retry,
        "source_atoms_window_validated"
    );
}

/// Admission for one model call on `selected`, immediately before it: the
/// standing fence, or the cohort's fresh authorization plus its per-call run
/// budget reservation. A window's retry is admitted again, so it consumes
/// run budget and passes every fence like any other call. `Hold` has already
/// been recorded.
#[allow(clippy::too_many_arguments)]
async fn admit_window(
    standing: Option<&crate::managed::standing::execution::StandingWork>,
    scope: &JobScope<'_>,
    tenant: &TenantId,
    control_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    source_version_id: Uuid,
    profile: ExtractionProfile,
    (characters, model_call): (i64, bool),
) -> Result<crate::corpus_enrichment::WindowGate, crate::store::StoreError> {
    if let Some(work) = standing {
        let mut tx = begin_scope(scope).await?;
        let mut control = crate::managed::standing::execution::begin_control(tenant, control_pool, account_id).await?;
        crate::managed::standing::execution::bound(&mut tx, control.as_mut()).await?;
        let allowed = crate::managed::standing::execution::attempt_allowed_locked(&mut tx, control.as_mut(), tenant, work).await?;
        let gate = if allowed {
            let model: String = sqlx::query_scalar("SELECT model FROM corpus_enrichment_runs WHERE run_id=$1")
                .bind(work.run_id).fetch_one(&mut *tx).await?;
            crate::corpus_enrichment::WindowGate::Permit { model }
        } else {
            let reason=crate::managed::standing::execution::denial_reason_locked(&mut tx,control.as_mut(),work).await?;
            crate::managed::standing::execution::hold_work_tx(&mut tx,work,reason.code()).await?;
            crate::corpus_enrichment::WindowGate::Hold
        };
        if let Some(control) = control { control.rollback().await?; }
        tx.commit().await?;
        return Ok(gate);
    }
    let mut tx = begin_scope(scope).await?;
    // Blob retrieval may have waited while policy/source permission changed.
    let allowed = crate::corpus_enrichment::requester_allowed_tx(
        &mut tx,
        control_pool,
        tenant,
        graph_id,
        source_version_id,
        profile.version(),
    )
    .await?;
    let gate = if allowed {
        crate::corpus_enrichment::reserve_window_tx(
            &mut tx,
            graph_id,
            source_version_id,
            profile.version(),
            characters,
            model_call,
        )
        .await?
    } else {
        crate::corpus_enrichment::WindowGate::Hold
    };
    if gate == crate::corpus_enrichment::WindowGate::Hold {
        let source_permitted = crate::corpus_enrichment::source_permitted_tx(
            &mut tx,
            graph_id,
            source_version_id,
            profile.version(),
        )
        .await?;
        crate::corpus_enrichment::hold_window_tx(
            &mut tx,
            graph_id,
            source_version_id,
            profile.version(),
            if !source_permitted {
                "source_removed_or_access_lost"
            } else if allowed {
                "managed_run_paused"
            } else {
                "requester_access_lost"
            },
        )
        .await?;
    }
    tx.commit().await?;
    Ok(gate)
}

/// Everything Jev's binding seat needs from the window being processed.
struct JevSeat<'a> {
    scope: &'a JobScope<'a>,
    tenant: &'a TenantId,
    control: &'a PgPool,
    store: &'a dyn Store,
    llm: &'a dyn LlmProvider,
    usage: &'a crate::llm::usage::UsageContext,
    account_id: Uuid,
    principal: Uuid,
    work: &'a crate::managed::standing::execution::StandingWork,
    graph_id: Uuid,
    version_id: Uuid,
    source_type: &'a str,
    author: Option<&'a str>,
    model: &'a str,
}

/// One managed Jev request per window (at most `v2::JEV_MAX_QUESTIONS`
/// mentions), billed to the window's `source_extraction` purpose and unit as
/// its own logical call. Any failure, hold or refusal returns an empty plan,
/// so binding falls back to the ladder.
async fn jev_binding_plan(seat: JevSeat<'_>, atoms: &[ValidatedAtom]) -> v2::BindingPlan {
    let gathered = async {
        let mut tx = begin_scope(seat.scope).await?;
        let questions = v2::pending_questions(
            seat.store, &mut tx, seat.graph_id, seat.version_id, atoms,
            ExtractionProfile::SourceV2.task(), seat.model,
        ).await?;
        let context = v2::candidate_context_tx(&mut tx, seat.graph_id, &questions).await?;
        tx.rollback().await?;
        Ok::<_, crate::store::StoreError>((questions, context))
    }
    .await;
    let (questions, context) = match gathered {
        Ok(gathered) => gathered,
        Err(error) => {
            tracing::warn!(graph = %seat.graph_id, version = %seat.version_id, %error, "jev_binding_candidates_unavailable");
            return v2::BindingPlan::default();
        }
    };
    if questions.is_empty() {
        return v2::BindingPlan::default();
    }
    let request = v2::jev_request(
        &questions,
        &context,
        seat.source_type,
        seat.author,
        crate::decision::jev_model_for(crate::decision::JevOp::Judge),
    );
    let call = crate::managed::ManagedCall {
        plane: seat.scope,
        control: seat.control,
        tenant: seat.tenant,
        account_id: seat.account_id,
        principal: seat.principal,
        fallback_model: None,
        subject: crate::managed::Subject::StandingWindow { work: seat.work },
    };
    match crate::decision::openrouter::evaluate_managed(seat.llm, &request, seat.usage, call).await {
        Ok(response) => {
            let plan = v2::plan_from_answers(&questions, &response);
            tracing::info!(graph = %seat.graph_id, version = %seat.version_id, asked = questions.len(),
                decided = plan.decisions.len(), "jev_binding_adjudicated");
            plan
        }
        Err(error) => {
            tracing::info!(graph = %seat.graph_id, version = %seat.version_id, asked = questions.len(),
                error = %error, fallback = true, "jev_binding_failed_open");
            v2::BindingPlan::default()
        }
    }
}

/// The one member a managed window charges and checks credits for: the
/// capture actor while they are still a member of the account, else the run's
/// requester. A departed or unknown capture actor never names the charge.
async fn charge_principal(
    scope: &JobScope<'_>,
    control: &sqlx::PgPool,
    account_id: Uuid,
    ingested_by: Option<Uuid>,
    graph_id: Uuid,
    version_id: Uuid,
    profile: &str,
) -> Result<Option<Uuid>, crate::store::StoreError> {
    if let Some(user) = ingested_by {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id).await?;
        let member: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2)",
        )
        .bind(account_id)
        .bind(user)
        .fetch_one(&mut *tx)
        .await?;
        tx.rollback().await?;
        if member {
            return Ok(Some(user));
        }
    }
    run_requester(scope, graph_id, version_id, profile).await
}

/// Who pays when the capture actor cannot: a cohort run's requester, or a
/// standing run's CURRENT execution principal. A standing run's `requested_by`
/// is the principal it was created with and may have left the account since;
/// workspace captures with no known actor (Slack `slack:workspace`) are billed
/// to the org account through whoever the run executes as now.
async fn run_requester(
    scope: &JobScope<'_>,
    graph_id: Uuid,
    version_id: Uuid,
    profile: &str,
) -> Result<Option<Uuid>, crate::store::StoreError> {
    let mut tx = begin_scope(scope).await?;
    let user = sqlx::query_scalar("SELECT CASE WHEN r.run_kind='standing' THEN COALESCE(r.execution_principal,r.requested_by) ELSE r.requested_by END FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u ON u.run_id=r.run_id AND u.graph_id=r.graph_id WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=$2 AND u.extraction_version=$3 AND u.execution_owner")
        .bind(graph_id).bind(version_id).bind(profile).fetch_optional(&mut *tx).await?;
    tx.rollback().await?;
    Ok(user)
}

async fn set_status(
    standing: Option<&crate::managed::standing::execution::StandingWork>,
    scope: &JobScope<'_>,
    graph_id: Uuid,
    version_id: Uuid,
    start: i64,
    status: &str,
    error: Option<&str>,
) -> Result<bool, crate::store::StoreError> {
    let mut tx = begin_scope(scope).await?;
    if let Some(work) = standing {
        if !crate::managed::standing::execution::current_work_tx(&mut tx,work).await? { return Ok(false); }
        if status == "blocked" {
            crate::managed::standing::execution::hold_work_tx(&mut tx,work,error.unwrap_or("cloud_processing_disabled")).await?;
            tx.commit().await?;
            return Ok(true);
        }
    }
    if status == "blocked" {
        let profile: Option<String> = sqlx::query_scalar(
            "SELECT extraction_version FROM source_versions WHERE graph_id=$1 AND version_id=$2",
        )
        .bind(graph_id)
        .bind(version_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(profile) = profile {
            crate::corpus_enrichment::hold_window_tx(
                &mut tx,
                graph_id,
                version_id,
                &profile,
                "cloud_processing_disabled",
            )
            .await?;
        }
    }
    sqlx::query("UPDATE source_versions SET extraction_status=$4, extraction_error=$5 WHERE graph_id=$1 AND version_id=$2 AND next_start_char=$3 AND extraction_status<>'done'")
        .bind(graph_id).bind(version_id).bind(start).bind(status).bind(error).execute(&mut *tx).await?;
    if status == "failed" {
        sqlx::query("UPDATE corpus_enrichment_units SET state='failed',reason='extraction_failed',updated_at=now() WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND execution_owner")
            .bind(graph_id).bind(version_id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn fail(
    standing: Option<&crate::managed::standing::execution::StandingWork>,
    scope: &JobScope<'_>,
    pool: &PgPool,
    job: &JobRow,
    graph_id: Uuid,
    version_id: Uuid,
    start: i64,
    category: &'static str,
) {
    if let Some(work) = standing {
        let result = async {
            let mut tx = begin_scope(scope).await?;
            if !crate::managed::standing::execution::current_work_tx(&mut tx, work).await? {
                return Ok::<_, crate::store::StoreError>(false);
            }
            if matches!(category, "source storage error" | "managed run storage error" | "status storage error" |
                "atom storage error" | "source text unavailable" | "source key unavailable" | "infrastructure_unavailable") {
                crate::managed::standing::execution::hold_work_tx(&mut tx, work, "infrastructure_unavailable").await?;
            } else {
                sqlx::query("UPDATE source_versions SET extraction_status='failed',extraction_error=$3 WHERE graph_id=$1 AND version_id=$2 AND extraction_status<>'done'")
                    .bind(graph_id).bind(version_id).bind(category).execute(&mut *tx).await?;
                sqlx::query("UPDATE corpus_enrichment_units SET state='failed',reason='extraction_failed',standing_claim_until=NULL,updated_at=now() WHERE unit_id=$1")
                    .bind(work.unit_id).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            Ok(true)
        }.await;
        if result.is_ok() { let _ = queue::complete(pool,job.id,queue::JobOutcome::NothingToDo).await; }
        else { let _ = queue::fail(pool,job.id,"standing infrastructure unavailable").await; }
        return;
    }
    // Status always offers explicit Retry, including reaper-exhausted jobs.
    // Automatic retry still uses the queue's bounded three-attempt backoff.
    let _ = set_status(None, scope, graph_id, version_id, start, "failed", Some(category)).await;
    let _ = queue::fail(pool, job.id, category).await;
}

#[allow(clippy::too_many_arguments)]
async fn persist_window(
    scope: &JobScope<'_>,
    tenant: &TenantId,
    control_pool: &PgPool,
    store: &dyn Store,
    account_id: Uuid,
    graph_id: Uuid,
    version_id: Uuid,
    selected: &Window<'_>,
    atoms: &[ValidatedAtom],
    model: &str,
    job_id: Uuid,
    profile: ExtractionProfile,
    standing: Option<&crate::managed::standing::execution::StandingWork>,
    original_hash: &str,
    original_blob_key: Option<&str>,
    original_declared_hash: Option<&str>,
    plan: &v2::BindingPlan,
) -> Result<bool, crate::store::StoreError> {
    // A standing window waits for its graph's publication turn instead of
    // queueing on the run row (`begin_publication`).
    let mut tx = if standing.is_some() {
        crate::managed::standing::execution::begin_publication(scope, graph_id).await?
    } else {
        begin_scope(scope).await?
    };
    let mut control = if standing.is_some() {
        crate::managed::standing::execution::begin_control(tenant, control_pool, account_id).await?
    } else { None };
    // Run lock precedes parent/version locks, matching controller admission.
    let allowed = if let Some(work) = standing {
        crate::managed::standing::execution::bound(&mut tx, control.as_mut()).await?;
        crate::managed::standing::execution::attempt_allowed_locked(&mut tx, control.as_mut(), tenant, work).await?
    } else {
        crate::corpus_enrichment::window_access_allowed_tx(&mut tx, control_pool, tenant,
            graph_id, version_id, profile.version()).await?
    };
    if !allowed {
        if let Some(work)=standing {
            let reason=crate::managed::standing::execution::denial_reason_locked(&mut tx,control.as_mut(),work).await?;
            crate::managed::standing::execution::hold_work_tx(&mut tx,work,reason.code()).await?;
        }
        if let Some(control) = control { control.rollback().await?; }
        tx.commit().await?;
        return Ok(false);
    }
    #[cfg(feature="test-support")]
    if standing.is_some() { crate::managed::standing::probe::checkpoint(job_id, crate::managed::standing::probe::Stage::PublicationFenced).await; }
    // Match capture/deletion lock order: parent source item, then version.
    // Deletion during the model call therefore cannot mint orphan nodes, and
    // opposite FK lock acquisition cannot deadlock a concurrent replacement.
    let parent: Option<Uuid> = sqlx::query_scalar(
        "SELECT source_item_id FROM source_items WHERE graph_id=$1 AND access_state='permitted' AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE",
    ).bind(graph_id).bind(version_id).fetch_optional(&mut *tx).await?;
    if parent.is_none() {
        if let Some(work)=standing {
            crate::managed::standing::execution::hold_work_tx(&mut tx,work,"source_removed_or_access_lost").await?;
            tx.commit().await?;
            return Ok(false);
        }
        crate::corpus_enrichment::window_outcome_tx(
            &mut tx,
            graph_id,
            version_id,
            profile.version(),
            "unavailable",
            Some("source_removed_or_access_lost"),
        )
        .await?;
        tx.commit().await?;
        return Ok(false);
    }
    let current: Option<(i64, String, String)> = sqlx::query_as(
        "SELECT next_start_char,extraction_status,extraction_version FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE",
    ).bind(graph_id).bind(version_id).fetch_optional(&mut *tx).await?;
    let Some((cursor, status, extraction_version)) = current else {
        return Ok(false);
    };
    if status == "done"
        || cursor != selected.start_char as i64
        || extraction_version != profile.version()
    {
        return Ok(false);
    }
    if standing.is_some() {
        let (declared_hash, inline, locator): (Option<String>, Option<String>, Option<String>) = sqlx::query_as("SELECT content_sha256,raw_inline,raw_blob_key FROM source_versions WHERE graph_id=$1 AND version_id=$2")
            .bind(graph_id).bind(version_id).fetch_one(&mut *tx).await?;
        if declared_hash.as_deref() != original_declared_hash { return Ok(false); }
        let same_input = if let Some(hash) = declared_hash { hash == original_hash }
            else if let Some(text) = inline { crate::services::source_reingest::content_hash_hex(&text) == original_hash }
            else { original_blob_key.is_some() && locator.as_deref() == original_blob_key };
        if !same_input { return Ok(false); }
    }
    let completed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM semantic_atom_windows WHERE source_version_id=$1 AND extraction_version=$2 AND start_char=$3)")
        .bind(version_id).bind(profile.version()).bind(selected.start_char as i32).fetch_one(&mut *tx).await?;
    if completed {
        return Ok(false);
    }
    let mut ordinal: i32 = sqlx::query_scalar("SELECT COALESCE(max(ordinal)+1,0) FROM semantic_atoms WHERE source_version_id=$1 AND extraction_version=$2")
        .bind(version_id).bind(profile.version()).fetch_one(&mut *tx).await?;
    // BRIDGE (pipeline plan PR 1, removed in PR 11): every reef linked below
    // also gets `node_atoms` rows for this version's chunk atoms, so the
    // `node_atoms` readers (reef corals, MCP get_reef, atlas, digests) see
    // managed-extraction output. See `node_atoms_bridge` for the mapping.
    let bridge_chunks = if crate::node_atoms_bridge::enabled() {
        crate::node_atoms_bridge::version_chunks_tx(&mut tx, graph_id, version_id).await?
    } else {
        Vec::new()
    };
    let mut bridge_nodes = std::collections::BTreeSet::new();
    let mut reefs_bound = 0usize;
    // v2: every mention of this window resolves to one reef, or to none when
    // two entity types bound it differently. Relations bind or drop on this.
    let mut window_bindings: std::collections::HashMap<String, Option<Uuid>> = std::collections::HashMap::new();
    let mut written_atoms: Vec<(Uuid, &ValidatedAtom)> = Vec::new();
    for atom in atoms {
        let id = Uuid::new_v4();
        let mut mentions = Vec::new();
        let mut links = HashSet::new();
        for entity in &atom.entities {
            let ctx = reef_ctx(version_id, entity, profile.task(), model);
            let node_type = map_node_type(entity.entity_type.as_str());
            let resolution = store
                .resolve_reef_ref_scoped(&mut tx, graph_id, node_type, &entity.mention, &ctx)
                .await?;
            let (node_id, reason, how) = if profile.structured() {
                bind_v2(store, &mut tx, graph_id, version_id, node_type, entity, &ctx, resolution, plan).await?
            } else {
                match resolution {
                    ReefResolution::Resolved { node_id, .. } => (Some(node_id), None, "ladder"),
                    ReefResolution::Unknown => (
                        Some(store.mint_reef_scoped(&mut tx, graph_id, node_type, &entity.mention, &ctx).await?),
                        None,
                        "minted",
                    ),
                    ReefResolution::NearMatch { .. } => (None, Some("ambiguous"), "ladder"),
                    ReefResolution::Rejected(_) => (None, Some("rejected"), "ladder"),
                }
            };
            if profile.structured() {
                window_bindings
                    .entry(entity.mention.clone())
                    .and_modify(|bound| if *bound != node_id { *bound = None })
                    .or_insert(node_id);
            }
            if let Some(node_id) = node_id {
                // The Library reads the typed column, not only body metadata.
                // This helper preserves explicit user-pinned classifications.
                store
                    .record_entity_type_hint_scoped(&mut tx, node_id, entity.entity_type.as_str())
                    .await?;
                links.insert(node_id);
            }
            let mut mention = json!({"mention":entity.mention,"type":entity.entity_type,
                "status":if node_id.is_some(){"linked"}else{"unresolved"},"reason":reason});
            if profile.structured() {
                mention["how"] = json!(how);
            }
            mentions.push(mention);
        }
        let validity = atom.validity.as_ref();
        sqlx::query("INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model,entities,valid_from,valid_until,validity_quote) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)")
            .bind(id).bind(graph_id).bind(version_id).bind(ordinal).bind(atom.kind.as_str()).bind(&atom.text)
            .bind(profile.version()).bind(model).bind(json!(mentions))
            .bind(validity.and_then(|v| v.from)).bind(validity.and_then(|v| v.until)).bind(validity.map(|v| v.quote.as_str()))
            .execute(&mut *tx).await?;
        written_atoms.push((id, atom));
        sqlx::query("INSERT INTO semantic_atom_passages(semantic_atom_id,source_version_id,graph_id,start_char,end_char,quote) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(id).bind(version_id).bind(graph_id).bind(atom.start_char as i32).bind(atom.end_char as i32).bind(&atom.evidence_quote).execute(&mut *tx).await?;
        reefs_bound += links.len();
        for node_id in links {
            sqlx::query("INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES($1,$2,$3)")
                .bind(node_id).bind(id).bind(graph_id).execute(&mut *tx).await?;
            if !bridge_chunks.is_empty() {
                bridge_nodes.insert(node_id);
            }
        }
        ordinal += 1;
    }
    let bridge_links: Vec<(Uuid, &[Uuid])> = bridge_nodes.iter().map(|node| (*node, bridge_chunks.as_slice())).collect();
    crate::node_atoms_bridge::link_reefs_tx(store, &mut tx, &bridge_links).await?;
    if profile.structured() {
        let mut edges = 0;
        let mut changes = 0;
        let mut unbound = 0;
        // Relation endpoints the text names but the model did not list as
        // mentions: bind each to an existing reef graph-wide, only when the
        // door resolves it outright (never a mint, never an ambiguous match)
        // and the reef is visible to this capture. Else it stays unbound.
        let unlisted: std::collections::BTreeSet<String> = written_atoms.iter()
            .flat_map(|(_, atom)| atom.relations.iter().flat_map(|r| [r.subject.clone(), r.object.clone()]))
            .filter(|endpoint| !window_bindings.contains_key(endpoint))
            .collect();
        for endpoint in unlisted {
            let ctx = ReefRefCtx {
                atom_id: Some(version_id),
                worker: profile.task(),
                model: Some(model.to_string()),
                surface_form: Some(endpoint.clone()),
                ..Default::default()
            };
            let node = match store.resolve_reef_ref_scoped(&mut tx, graph_id, "entity", &endpoint, &ctx).await? {
                ReefResolution::Resolved { node_id, .. }
                    if v2::jev_target_valid_tx(&mut tx, graph_id, "entity", node_id, version_id).await? => Some(node_id),
                _ => None,
            };
            window_bindings.insert(endpoint, node);
        }
        for (semantic_atom_id, atom) in &written_atoms {
            let (written, missed) = v2::write_relations_tx(store, &mut tx, graph_id, version_id, *semantic_atom_id,
                &atom.relations, &window_bindings, &bridge_chunks, profile.version()).await?;
            edges += written;
            unbound += missed;
            // v3: a change keys on the reefs its mentions bound to anywhere in
            // the window, so it is recorded once every atom has bound.
            let stated: Vec<serde_json::Value> = atom.changes.iter()
                .filter_map(|change| v3::change_json(change, &window_bindings)).collect();
            if !stated.is_empty() {
                changes += stated.len();
                sqlx::query("UPDATE semantic_atoms SET stated_changes=$3 WHERE graph_id=$1 AND semantic_atom_id=$2")
                    .bind(graph_id).bind(semantic_atom_id).bind(json!(stated)).execute(&mut *tx).await?;
            }
        }
        let retired = if selected.has_more { 0 } else { v2::retire_superseded_tx(&mut tx, graph_id, version_id, profile.version()).await? };
        tracing::info!(%graph_id, %version_id, profile = profile.version(), atoms = written_atoms.len(), edges, changes,
            unbound_relations = unbound, jev_decisions = plan.decisions.len(), retired_atoms = retired,
            "source_atoms_v2_window_persisted");
        // PR 9: temporal evidence and supersession for the finished version.
        // Fail-open in its own savepoint; FACT_SUPERSESSION_MODE gates it.
        if !selected.has_more {
            crate::fact_supersession::after_extraction_tx(&mut tx, graph_id, version_id).await;
        }
    }
    if selected.end_char > selected.start_char {
        sqlx::query("INSERT INTO semantic_atom_windows(source_version_id,graph_id,extraction_version,start_char,end_char,model,job_id) VALUES($1,$2,$3,$4,$5,$6,$7)")
            .bind(version_id).bind(graph_id).bind(profile.version()).bind(selected.start_char as i32).bind(selected.end_char as i32).bind(model).bind(job_id).execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE source_versions SET next_start_char=$3,extraction_status=$4,extraction_error=NULL WHERE graph_id=$1 AND version_id=$2")
        .bind(graph_id).bind(version_id).bind(selected.end_char as i64).bind(if selected.has_more {"pending"} else {"done"}).execute(&mut *tx).await?;
    crate::corpus_enrichment::window_outcome_tx(
        &mut tx,
        graph_id,
        version_id,
        profile.version(),
        if selected.has_more {
            "queued"
        } else {
            "processed"
        },
        None,
    )
    .await?;
    if let Some(work) = standing {
        sqlx::query("UPDATE corpus_enrichment_units SET standing_claim_until=NULL,standing_start_cursor=NULL,standing_logical_call=NULL WHERE unit_id=$1 AND standing_generation=$2 AND standing_logical_call=$3")
            .bind(work.unit_id).bind(work.generation).bind(work.logical_call).execute(&mut *tx).await?;
    }
    if selected.has_more {
        let kind = profile.job(graph_id, version_id, selected.end_char as i64);
        let key = profile.key(version_id, selected.end_char);
        queue::enqueue_tx(&mut tx, &kind, Some(account_id), Some(&key), 0).await?;
    }
    // Reefs were minted or bound: one debounced EntityResolution pass for the
    // graph (managed_enrichment; nothing is scheduled while it is off).
    if reefs_bound > 0 {
        crate::workers::entity_resolution::enqueue_after_binding(&mut tx, account_id, graph_id).await?;
    }
    // Statement vectors for scoped Ask's statement leg (managed
    // `cloud_embedding`, receipted; see `workers::statement_embed`).
    if !written_atoms.is_empty() {
        crate::workers::statement_embed::enqueue_after_persist(&mut tx, account_id, graph_id, version_id).await?;
    }
    if let Some(control) = control { control.rollback().await?; }
    tx.commit().await?;
    #[cfg(feature="test-support")]
    if standing.is_some() && crate::managed::standing::probe::lose_ack(job_id,crate::managed::standing::probe::Ack::Publication) {
        return Err(sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::ConnectionAborted,"local publication acknowledgement lost")).into());
    }
    Ok(true)
}

/// All outcomes from one retained version share an evidence context. A newly
/// minted semantic atom ID would fragment the same explicit entity mention
/// across facts/windows.
fn reef_ctx(version_id: Uuid, entity: &EntityMention, worker: &'static str, model: &str) -> ReefRefCtx {
    ReefRefCtx {
        atom_id: Some(version_id),
        entity_type: Some(entity.entity_type.as_str().to_string()),
        worker,
        model: Some(model.to_string()),
        surface_form: Some(entity.mention.clone()),
        ..Default::default()
    }
}

/// v2 binding: the ladder's answer when it has one; for `Unknown`, an
/// existing reef with the exact title and type (`v2::exact_title_tx`); then
/// Jev's answer; else a mint for `Unknown`.
///
/// A `NearMatch` is decided by [`crate::mention_sweep::decide_near_tx`], the
/// rule the deferred sweep uses: a person, org or self-reef rival keeps the
/// mention unresolved (COR-36); else the exact-title reef binds; else any
/// other rival (a broader or narrower name, a spelling variant) keeps it
/// unresolved unless Jev answers, and is never minted. Only a near match
/// with no rival is minted, unless same-title copies exist or the title is
/// generic ([`crate::mention_sweep::mint_refusal_tx`]).
#[allow(clippy::too_many_arguments)]
async fn bind_v2(
    store: &dyn Store,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    node_type: &str,
    entity: &EntityMention,
    ctx: &ReefRefCtx,
    resolution: ReefResolution,
    plan: &v2::BindingPlan,
) -> Result<(Option<Uuid>, Option<&'static str>, &'static str), crate::store::StoreError> {
    use crate::mention_sweep::NearDecision;
    let entity_type = entity.entity_type.as_str();
    // `None`: `Unknown`. `Some(decision)`: a near match.
    let near = match resolution {
        ReefResolution::Resolved { node_id, .. } => return Ok((Some(node_id), None, "ladder")),
        ReefResolution::Rejected(_) => return Ok((None, Some("rejected"), "ladder")),
        ReefResolution::Unknown => None,
        ReefResolution::NearMatch { candidates } => Some(
            crate::mention_sweep::decide_near_tx(tx, graph_id, version_id, node_type, &entity.mention, entity_type, &candidates).await?,
        ),
    };
    match near {
        Some(NearDecision::ExactTitle(node_id)) => return Ok((Some(node_id), None, "exact_title")),
        None => {
            if let Some(node_id) = v2::exact_title_tx(tx, graph_id, node_type, &entity.mention, entity_type, version_id).await? {
                return Ok((Some(node_id), None, "exact_title"));
            }
        }
        Some(_) => {}
    }
    let key = (entity.mention.clone(), entity_type);
    match plan.decisions.get(&key) {
        Some(v2::JevBinding::Existing(node_id))
            if v2::jev_target_valid_tx(tx, graph_id, node_type, *node_id, version_id).await? =>
        {
            return Ok((Some(*node_id), None, "jev"));
        }
        Some(v2::JevBinding::New) => {
            let node_id = store.mint_reef_scoped(tx, graph_id, node_type, &entity.mention, ctx).await?;
            return Ok((Some(node_id), None, "jev_new"));
        }
        _ => {}
    }
    match near {
        Some(NearDecision::Keep(_)) => return Ok((None, Some("ambiguous"), "ladder")),
        Some(NearDecision::Open)
            if crate::mention_sweep::mint_refusal_tx(tx, graph_id, node_type, &entity.mention, entity_type).await?.is_some() =>
        {
            return Ok((None, Some("ambiguous"), "ladder"));
        }
        _ => {}
    }
    let node_id = store.mint_reef_scoped(tx, graph_id, node_type, &entity.mention, ctx).await?;
    Ok((Some(node_id), None, "minted"))
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AtomKind {
    Fact,
    Decision,
    Action,
    Observation,
}

impl AtomKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Fact => "fact",
            Self::Decision => "decision",
            Self::Action => "action",
            Self::Observation => "observation",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Person,
    Org,
    Project,
    Topic,
}

impl EntityType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Person => "person",
            Self::Org => "org",
            Self::Project => "project",
            Self::Topic => "topic",
        }
    }
}

/// Unknown extra fields are ignored: the model's extras carry nothing the
/// validator would keep, and rejecting them cost whole atoms.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EntityMention {
    pub mention: String,
    #[serde(rename = "type")]
    pub entity_type: EntityType,
}

/// One entity of an atom, decoded on its own: a malformed entity (unknown
/// type, missing field) is dropped without rejecting its atom.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(super) enum EntityCandidate {
    Valid(EntityMention),
    Invalid(serde::de::IgnoredAny),
}

/// A list the model may send as `null` or omit: both mean "none".
pub(super) fn nullable_list<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Deserialize)]
struct ExtractedAtom {
    kind: AtomKind,
    text: String,
    evidence_quote: String,
    #[serde(default, deserialize_with = "nullable_list")]
    entities: Vec<EntityCandidate>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AtomCandidate {
    Valid(ExtractedAtom),
    Invalid(serde::de::IgnoredAny),
}

/// Extra envelope fields are ignored; a missing, null or duplicated `atoms`
/// is still a schema error, because there is nothing to salvage.
#[derive(Debug, Deserialize)]
struct Extraction {
    atoms: Vec<AtomCandidate>,
}

/// Atom candidates validated per response. Past it, and past [`MAX_ATOMS`]
/// accepted atoms, candidates are counted as capped and not read.
pub(super) const MAX_ATOM_CANDIDATES: usize = MAX_ATOMS * 4;

/// The body of a single fenced code block (```` ```json ````), if `output` is
/// exactly one. Nothing outside the fence is kept.
fn unfenced(output: &str) -> Option<&str> {
    let body = output.trim().strip_prefix("```")?.strip_suffix("```")?;
    let (language, rest) = body.split_once('\n')?;
    matches!(language.trim(), "" | "json" | "JSON").then_some(rest)
}

/// Parse a response envelope. A response wrapped in one Markdown fence is
/// read from inside the fence; anything else that is not the envelope is a
/// schema error.
pub(super) fn parse_extraction<T: serde::de::DeserializeOwned>(
    output: &str,
    diagnostics: &mut crate::llm::ExtractionDiagnostics,
) -> Result<T, &'static str> {
    let schema_error = |diagnostics: &mut crate::llm::ExtractionDiagnostics| {
        diagnostics.envelope_error = Some(crate::llm::ExtractionEnvelopeError::Schema);
        INVALID_SCHEMA
    };
    // The field path of the first error, for diagnostics: the outermost
    // body that still parses as JSON (a fenced answer's inside).
    diagnostics.schema_error_path = None;
    let record_path = |diagnostics: &mut crate::llm::ExtractionDiagnostics, body: &str| {
        if diagnostics.schema_error_path.is_none() {
            diagnostics.schema_error_path = schema_error_path::<T>(body);
        }
    };
    let body = match serde_json::from_str::<T>(output) {
        Ok(parsed) => return Ok(parsed),
        Err(_) => match unfenced(output) {
            Some(inner) => match serde_json::from_str::<T>(inner) {
                Ok(parsed) => return Ok(parsed),
                Err(_) => inner,
            },
            None => output,
        },
    };
    // A model sometimes returns `atoms` as a JSON string holding the array.
    // Only then, and only when that string is itself a JSON array, is it read
    // as the same answer (the array re-serialized, so the strict parse rules
    // still apply to it); anything else stays a schema error.
    let Ok(serde_json::Value::Object(mut envelope)) = serde_json::from_str::<serde_json::Value>(body) else {
        record_path(diagnostics, body);
        return Err(schema_error(diagnostics));
    };
    let Some(serde_json::Value::String(inner)) = envelope.get("atoms") else {
        record_path(diagnostics, body);
        return Err(schema_error(diagnostics));
    };
    let Ok(array @ serde_json::Value::Array(_)) = serde_json::from_str::<serde_json::Value>(inner) else {
        record_path(diagnostics, body);
        return Err(schema_error(diagnostics));
    };
    envelope.insert("atoms".into(), array);
    let repaired = serde_json::Value::Object(envelope).to_string();
    let parsed = match serde_json::from_str::<T>(&repaired) {
        Ok(parsed) => parsed,
        Err(_) => {
            record_path(diagnostics, &repaired);
            return Err(schema_error(diagnostics));
        }
    };
    diagnostics.stringified_atoms = true;
    Ok(parsed)
}

/// Window error: the response is not the extraction envelope.
pub const INVALID_SCHEMA: &str = "invalid extraction schema";
const MAX_SCHEMA_PATH_CHARS: usize = 120;

/// The path to the first field `body` fails to deserialize at, as field
/// names and indices (`atoms[3].kind`). Characters outside a field-name
/// alphabet are dropped, so no model text is ever recorded; `None` when the
/// body is not JSON at all (the path would be empty).
fn schema_error_path<T: serde::de::DeserializeOwned>(body: &str) -> Option<String> {
    let mut deserializer = serde_json::Deserializer::from_str(body);
    let error = serde_path_to_error::deserialize::<_, T>(&mut deserializer).err()?;
    let path: String = error.path().to_string().chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '[' | ']'))
        .take(MAX_SCHEMA_PATH_CHARS)
        .collect();
    (!path.is_empty() && path != ".").then_some(path)
}

/// A window fails only when the model offered atoms and none survived.
/// An honest empty answer (`atoms: []`) completes the window.
pub(super) fn require_survivor(
    candidates: usize,
    accepted: usize,
    diagnostics: &mut crate::llm::ExtractionDiagnostics,
) -> Result<(), &'static str> {
    if candidates > 0 && accepted == 0 {
        diagnostics.envelope_error = Some(crate::llm::ExtractionEnvelopeError::NoValidAtoms);
        return Err(NO_VALID_ATOMS);
    }
    Ok(())
}

/// Window error: candidates were offered, none was valid.
pub const NO_VALID_ATOMS: &str = "no valid atoms";

#[derive(Debug, Clone)]
pub struct ValidatedAtom {
    pub kind: AtomKind,
    pub text: String,
    pub evidence_quote: String,
    pub start_char: i64,
    pub end_char: i64,
    pub entities: Vec<EntityMention>,
    /// v2 only: stated relations between this window's mentions.
    pub relations: Vec<v2::ValidatedRelation>,
    /// v2 only: the time validity the evidence states for this atom.
    pub validity: Option<v2::Validity>,
    /// v3 only: changes the evidence states in one statement.
    pub changes: Vec<v3::ValidatedChange>,
}

#[derive(Debug)]
struct Window<'a> {
    text: &'a str,
    start_char: usize,
    end_char: usize,
    has_more: bool,
}

/// Prefer a whole line boundary in the latter half of the bounded window.
/// Extremely long lines are split at a Unicode scalar boundary. No trimming,
/// newline normalization, added headings, or speaker reconstruction occurs.
#[cfg(test)]
fn window(text: &str, start_char: usize) -> Result<Window<'_>, &'static str> {
    window_sized(text, start_char, WINDOW_CHARS)
}

/// [`window`] with an explicit bound. A retried window uses a smaller bound;
/// the cursor, the window record and the continuation all follow the
/// window actually extracted, so boundaries stay contiguous.
fn window_sized(text: &str, start_char: usize, limit: usize) -> Result<Window<'_>, &'static str> {
    let limit = limit.max(1);
    let start_byte = text
        .char_indices()
        .nth(start_char)
        .map(|(byte, _)| byte)
        .or_else(|| (text.chars().count() == start_char).then_some(text.len()))
        .ok_or("invalid window position")?;
    let remainder = &text[start_byte..];
    let hard_end = remainder
        .char_indices()
        .nth(limit)
        .map(|(byte, _)| byte)
        .unwrap_or(remainder.len());
    let mut end_byte = hard_end;
    if hard_end < remainder.len() {
        if let Some((index, _)) = remainder[..hard_end]
            .chars()
            .enumerate()
            .filter(|(index, c)| *index >= limit / 2 && *c == '\n')
            .last()
        {
            end_byte = remainder
                .char_indices()
                .nth(index + 1)
                .map(|(byte, _)| byte)
                .unwrap_or(hard_end);
        }
    }
    let text = &remainder[..end_byte];
    Ok(Window {
        text,
        start_char,
        end_char: start_char + text.chars().count(),
        has_more: end_byte < remainder.len(),
    })
}

fn tool_schema(profile: ExtractionProfile) -> ToolSchema {
    match profile {
        ExtractionProfile::SourceV2 => return v2::tool_schema_v2(),
        ExtractionProfile::SourceV3 => return v3::tool_schema_v3(),
        ExtractionProfile::MeetingV1 => {}
    }
    ToolSchema {
        strict: true,
        name: profile.tool_name().to_string(),
        schema: json!({
            "type": "object", "additionalProperties": false, "required": ["atoms"],
            "properties": { "atoms": {
                "type": "array",
                "items": {
                    "type": "object", "additionalProperties": false,
                    "required": ["kind", "text", "evidence_quote", "entities"],
                    "properties": {
                        "kind": {"type": "string", "enum": ["fact", "decision", "action", "observation"]},
                        "text": {"type": "string"},
                        "evidence_quote": {"type": "string"},
                        "entities": { "type": "array", "items": {
                            "type": "object", "additionalProperties": false,
                            "required": ["mention", "type"],
                            "properties": {
                                "mention": {"type": "string"},
                                "type": {"type": "string", "enum": ["person", "org", "project", "topic"]}
                            }
                        }}
                    }
                }
            }}
        }),
    }
}

fn validate_output_with_diagnostics(
    output: &str,
    window: &Window<'_>,
    diagnostics: &mut crate::llm::ExtractionDiagnostics,
) -> Result<Vec<ValidatedAtom>, &'static str> {
    let extraction: Extraction = parse_extraction(output, diagnostics)?;
    let candidates = extraction.atoms.len();
    let mut seen = HashSet::new();
    let mut atoms = Vec::with_capacity(candidates.min(MAX_ATOMS));
    for (index, value) in extraction.atoms.into_iter().enumerate() {
        if index >= MAX_ATOM_CANDIDATES || atoms.len() >= MAX_ATOMS {
            diagnostics.capped_atoms += 1;
            continue;
        }
        let validated = match value {
            AtomCandidate::Valid(atom) => validate_atom(atom, window),
            AtomCandidate::Invalid(_) => Err(crate::llm::AtomRejection::Schema),
        };
        let (atom, anchor) = match validated {
            Ok(atom) => atom,
            Err(reason) => {
                *diagnostics.rejected_atoms.entry(reason).or_default() += 1;
                continue;
            }
        };
        let identity = (atom.kind.as_str(), atom.text.clone(), atom.start_char, atom.end_char);
        if seen.insert(identity) {
            diagnostics.accepted_atoms += 1;
            if anchor.normalized {
                diagnostics.whitespace_matches += 1;
            }
            for (reason, count) in anchor.dropped_entities {
                *diagnostics.dropped_entities.entry(reason).or_default() += count;
            }
            atoms.push(atom);
        } else {
            diagnostics.duplicate_atoms += 1;
        }
    }
    require_survivor(candidates, atoms.len(), diagnostics)?;
    Ok(atoms)
}

fn validate_atom(
    atom: ExtractedAtom,
    window: &Window<'_>,
) -> Result<(ValidatedAtom, AnchorStats), crate::llm::AtomRejection> {
    validate_atom_parts(atom.kind, &atom.text, &atom.evidence_quote, atom.entities, window, None)
}

/// What validating one kept atom cost or normalized.
#[derive(Debug, Default)]
pub(super) struct AnchorStats {
    /// The quote anchored only after normalization (whitespace runs, quote
    /// characters, ellipsis). The stored quote is still the source span.
    pub normalized: bool,
    /// Entity mentions dropped from this atom, by reason.
    pub dropped_entities: std::collections::BTreeMap<crate::llm::AtomRejection, u32>,
}

/// The atom rules shared by every profile. `author` (v2 only) is the one
/// person mention admissible without appearing in the quote, so first-person
/// statements can name their author.
///
/// Statement and evidence decide the atom. Entities are judged one by one: an
/// unsupported, malformed or excess mention is dropped and the atom is kept.
fn validate_atom_parts(
    kind: AtomKind,
    text: &str,
    evidence_quote: &str,
    candidate_entities: Vec<EntityCandidate>,
    window: &Window<'_>,
    author: Option<&str>,
) -> Result<(ValidatedAtom, AnchorStats), crate::llm::AtomRejection> {
    use crate::llm::AtomRejection;
    let text = text.trim();
    if text.is_empty() || text.chars().count() > MAX_TEXT_CHARS {
        return Err(AtomRejection::Text);
    }
    if evidence_quote.trim().is_empty() || evidence_quote.chars().count() > MAX_QUOTE_CHARS {
        return Err(AtomRejection::QuoteBounds);
    }
    let (start_byte, end_byte, normalized) = anchor_quote(window.text, evidence_quote)?;
    let quote = &window.text[start_byte..end_byte];
    if quote.chars().count() > MAX_QUOTE_CHARS {
        return Err(AtomRejection::QuoteBounds);
    }
    let mut stats = AnchorStats { normalized, ..AnchorStats::default() };
    let mut dropped = |reason| *stats.dropped_entities.entry(reason).or_default() += 1;
    let mut entities: Vec<EntityMention> = Vec::with_capacity(candidate_entities.len().min(MAX_ENTITIES));
    for candidate in candidate_entities {
        let EntityCandidate::Valid(mut entity) = candidate else {
            dropped(AtomRejection::Schema);
            continue;
        };
        // Surrounding whitespace is formatting, not part of the name.
        let trimmed = entity.mention.trim();
        if trimmed.len() != entity.mention.len() {
            entity.mention = trimmed.to_owned();
        }
        let authored = author == Some(entity.mention.as_str()) && entity.entity_type == EntityType::Person;
        if entity.mention.is_empty()
            || entity.mention.chars().count() > MAX_MENTION_CHARS
            || !(quote.contains(&entity.mention) || authored)
        {
            dropped(AtomRejection::EntityUnsupported);
            continue;
        }
        if entities.contains(&entity) {
            continue;
        }
        if entities.len() >= MAX_ENTITIES {
            dropped(AtomRejection::EntityLimit);
            continue;
        }
        entities.push(entity);
    }
    let start_char = window.start_char + window.text[..start_byte].chars().count();
    let end_char = start_char + quote.chars().count();
    Ok((ValidatedAtom {
        kind,
        text: text.to_owned(),
        evidence_quote: quote.to_owned(),
        start_char: start_char as i64,
        end_char: end_char as i64,
        entities,
        relations: Vec::new(),
        validity: None,
        changes: Vec::new(),
    }, stats))
}

struct QuoteScalar {
    value: char,
    start_byte: usize,
    end_byte: usize,
}

/// Typographic characters a model rewrites without changing the words:
/// curly and prime quotes fold to ASCII quotes, the ellipsis character to
/// three periods. Applied identically to source and quote.
fn folded(value: char) -> Option<&'static str> {
    match value {
        '\u{2018}' | '\u{2019}' | '\u{201A}' | '\u{201B}' | '\u{2032}' => Some("'"),
        '\u{201C}' | '\u{201D}' | '\u{201E}' | '\u{201F}' | '\u{2033}' => Some("\""),
        '\u{2026}' => Some("..."),
        _ => None,
    }
}

fn quote_scalars(text: &str) -> Vec<QuoteScalar> {
    quote_scalars_with(text, false)
}

/// Comment markers a model drops when it quotes code: Rust doc and line
/// comments, shell/Python/YAML comments, block-comment gutters, and Markdown
/// blockquotes. Longest first.
const COMMENT_MARKERS: &[&str] = &["//!", "///", "//", "/**", "*/", "#", "*", ">"];

/// Markdown markup a model drops when it quotes prose: emphasis, code
/// spans and table pipes. None of them is part of a word. `_` is never one:
/// `slack_user` and `slackuser` are different words.
const INLINE_MARKUP: &[char] = &['*', '`', '|'];

/// [`quote_scalars`], and with `lenient` a comment marker at the start of a
/// line (after its indentation) and inline Markdown markup anywhere
/// ([`INLINE_MARKUP`]) are no scalars at all, so "//! Ends **here**" and
/// "Ends here" normalize alike. A `#` inside a line is text.
fn quote_scalars_with(text: &str, skip_comment_markers: bool) -> Vec<QuoteScalar> {
    let mut scalars: Vec<QuoteScalar> = Vec::with_capacity(text.chars().count());
    let mut line_start = true;
    let mut skip_until = 0usize;
    for (byte, value) in text.char_indices() {
        if byte < skip_until {
            continue;
        }
        if skip_comment_markers && line_start && !matches!(value, ' ' | '\t' | '\r' | '\n') {
            line_start = false;
            if let Some(marker) = COMMENT_MARKERS.iter().find(|m| text[byte..].starts_with(**m)) {
                skip_until = byte + marker.len();
                continue;
            }
        }
        if value == '\n' {
            line_start = true;
        } else if !matches!(value, ' ' | '\t' | '\r') {
            line_start = false;
        }
        if skip_comment_markers && INLINE_MARKUP.contains(&value) {
            continue;
        }
        let end_byte = byte + value.len_utf8();
        if matches!(value, ' ' | '\t' | '\r' | '\n') {
            if let Some(previous) = scalars.last_mut().filter(|scalar| scalar.value == ' ') {
                previous.end_byte = end_byte;
                continue;
            }
            scalars.push(QuoteScalar { value: ' ', start_byte: byte, end_byte });
        } else if let Some(replacement) = folded(value) {
            // Every folded scalar maps back to the whole source character.
            for value in replacement.chars() {
                scalars.push(QuoteScalar { value, start_byte: byte, end_byte });
            }
        } else {
            scalars.push(QuoteScalar { value, start_byte: byte, end_byte });
        }
    }
    scalars
}

/// A quote with a leading or trailing elision marker ("...", "…") and the
/// whitespace beside it removed. The rest must still match the source
/// verbatim (after normalization); nothing is ever matched inside the gap.
fn without_edge_ellipsis(quote: &str) -> &str {
    let mut rest = quote;
    for marker in ["...", "\u{2026}"] {
        if let Some(stripped) = rest.strip_prefix(marker) {
            rest = stripped.trim_start_matches([' ', '\t', '\r', '\n']);
        }
    }
    for marker in ["...", "\u{2026}"] {
        if let Some(stripped) = rest.strip_suffix(marker) {
            rest = stripped.trim_end_matches([' ', '\t', '\r', '\n']);
        }
    }
    rest
}

/// Anchor a model quote in the window. Exact first; then, only when exact
/// search finds nothing, one normalized search (ASCII whitespace runs,
/// quote characters, ellipsis); then the same with edge elision markers
/// removed; last, the same ignoring line-leading code comment markers and
/// inline Markdown markup ([`quote_scalars_with`]). Every stage needs exactly
/// one occurrence. The returned span is
/// always original source text: a quote the source does not contain is
/// never accepted.
fn anchor_quote(source: &str, quote: &str) -> Result<(usize, usize, bool), crate::llm::AtomRejection> {
    use crate::llm::AtomRejection;
    let mut exact = source.char_indices().filter(|(byte, _)| source[*byte..].starts_with(quote));
    if let Some((byte, _)) = exact.next() {
        if exact.next().is_some() {
            return Err(AtomRejection::QuoteAmbiguous);
        }
        return Ok((byte, byte + quote.len(), false));
    }
    let original = source;
    let source = quote_scalars(source);
    let normalized_source: String = source.iter().map(|scalar| scalar.value).collect();
    let trimmed = without_edge_ellipsis(quote);
    let attempts: &[&str] = if trimmed.len() == quote.len() { &[quote] } else { &[quote, trimmed] };
    for attempt in attempts {
        let quote = quote_scalars(attempt);
        let normalized_quote: String = quote.iter().map(|scalar| scalar.value).collect();
        if normalized_quote.trim().is_empty() {
            continue;
        }
        let mut candidates = normalized_source.char_indices().enumerate()
            .filter(|(_, (byte, _))| normalized_source[*byte..].starts_with(&normalized_quote));
        let Some((index, _)) = candidates.next() else { continue };
        if candidates.next().is_some() {
            return Err(AtomRejection::QuoteAmbiguous);
        }
        return Ok((source[index].start_byte, source[index + quote.len() - 1].end_byte, true));
    }
    // Last: code comments quoted without their markers ("//!", "///", "#")
    // and prose quoted without its Markdown markup ("**", "`", "|"). Same
    // rules: one occurrence, and the span returned is the source's own text,
    // markers and markup included.
    let source = quote_scalars_with(original, true);
    let normalized_source: String = source.iter().map(|scalar| scalar.value).collect();
    for attempt in attempts {
        let quote = quote_scalars_with(attempt, true);
        let normalized_quote: String = quote.iter().map(|scalar| scalar.value).collect();
        let normalized_quote = normalized_quote.trim();
        if normalized_quote.is_empty() {
            continue;
        }
        let mut candidates = normalized_source.char_indices().enumerate()
            .filter(|(_, (byte, _))| normalized_source[*byte..].starts_with(normalized_quote));
        let Some((index, _)) = candidates.next() else { continue };
        if candidates.next().is_some() {
            return Err(AtomRejection::QuoteAmbiguous);
        }
        let len = normalized_quote.chars().count();
        return Ok((source[index].start_byte, source[index + len - 1].end_byte, true));
    }
    Err(AtomRejection::QuoteMissing)
}

/// The prompt input `version` renders for `window` of `source`. Test seam so
/// capture-side tests can prove the metadata they record reaches the model.
#[cfg(test)]
pub(crate) fn input_for_version(version: &str, window: &str, source: &crate::source_evidence::SourceVersion) -> Option<String> {
    ExtractionProfile::from_version(version).map(|p| p.input(window, source))
}

/// Map a model's entity sub-type to an allowed `nodes.type`
/// (entity/thread/reference). `decision` collapses to `entity`: the
/// `decisions_require_decided_at` CHECK on `nodes` needs a `decided_at` that an
/// entity mention cannot supply.
pub(crate) fn map_node_type(llm_type: &str) -> &str {
    match llm_type {
        "thread" | "reference" => llm_type,
        _ => "entity",
    }
}

#[cfg(test)]
fn validate_output(output: &str, window: &Window<'_>) -> Result<Vec<ValidatedAtom>, &'static str> {
    validate_output_with_diagnostics(output, window, &mut crate::llm::ExtractionDiagnostics::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(quote: &str) -> String {
        json!({"atoms": [{"kind": "decision", "text": "Keep the decision.",
            "evidence_quote": quote, "entities": []}]})
        .to_string()
    }

    #[test]
    fn synthetic_replay_preserves_supported_atoms_and_canonical_spans() {
        let replay: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/source_atoms_replay_synthetic.json"
        )).unwrap();
        for case in replay["cases"].as_array().unwrap() {
            let source = case["source"].as_str().unwrap();
            let result = validate_output(&case["response"].to_string(), &window(source, 0).unwrap());
            if let Some(expected) = case["expected_error"].as_str() {
                assert_eq!(result.unwrap_err(), expected, "{}", case["name"]);
                continue;
            }
            let atoms = result.unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
            let expected: Vec<&str> = case["expected_quotes"].as_array().unwrap().iter()
                .map(|quote| quote.as_str().unwrap()).collect();
            assert_eq!(atoms.iter().map(|atom| atom.evidence_quote.as_str()).collect::<Vec<_>>(),
                expected, "{}", case["name"]);
            for atom in atoms {
                assert_eq!(source.chars().skip(atom.start_char as usize)
                    .take((atom.end_char - atom.start_char) as usize).collect::<String>(),
                    atom.evidence_quote, "{}", case["name"]);
            }
        }
    }

    /// A model quoting a doc comment drops its markers. The quote anchors in
    /// a last stage that ignores line-leading markers; the span kept is the
    /// source's own text, markers included, and it must still be unique.
    #[test]
    fn code_comment_quotes_anchor_without_their_markers() {
        let code = "//! Source-atoms v3.\n//!\n//! One funded call per window\n//! returns changes.\nfn main() {}\n    /// Keeps the atom.\n";
        let (s, e, normalized) = anchor_quote(code, "One funded call per window returns changes.").unwrap();
        assert!(normalized);
        assert_eq!(&code[s..e], "One funded call per window\n//! returns changes.");
        let (s, e, _) = anchor_quote(code, "/// Keeps the atom.").unwrap();
        assert_eq!(&code[s..e], "/// Keeps the atom.", "an exact quote is unchanged");
        let (s, e, _) = anchor_quote(code, "Keeps the atom.").unwrap();
        assert_eq!(&code[s..e], "Keeps the atom.");
        // A marker inside a line is text, never skipped.
        assert_eq!(anchor_quote("let x = a # b;", "let x = a b;").unwrap_err(), crate::llm::AtomRejection::QuoteMissing);
        // Still one occurrence: two comment lines with the same words are ambiguous.
        assert_eq!(anchor_quote("// retry once\n# retry once\n", "retry once").unwrap_err(), crate::llm::AtomRejection::QuoteAmbiguous);
        // Words the source does not have never anchor.
        assert_eq!(anchor_quote(code, "One funded call per batch").unwrap_err(), crate::llm::AtomRejection::QuoteMissing);
    }

    /// v3.2 (measured: 103 of 188 lost v3 quotes dropped Markdown markup):
    /// emphasis, code spans and table pipes are ignored in the last stage;
    /// `_` never is; the span is the source's own text and must be unique.
    #[test]
    fn prose_quotes_anchor_without_their_markdown_markup() {
        let doc = "**Tests (red first).** All six failed against `3355dd33` and pass now.\n| B1 | `retry_once_pg` | ok |\n> quoted line here\n";
        let (s, e, normalized) = anchor_quote(doc, "Tests (red first). All six failed against 3355dd33 and pass now.").unwrap();
        assert!(normalized);
        assert_eq!(&doc[s..e], "Tests (red first).** All six failed against `3355dd33` and pass now.");
        let (s, e, _) = anchor_quote(doc, "B1 retry_once_pg ok").unwrap();
        assert_eq!(&doc[s..e], "B1 | `retry_once_pg` | ok");
        let (s, e, _) = anchor_quote(doc, "quoted line here").unwrap();
        assert_eq!(&doc[s..e], "quoted line here");
        assert_eq!(anchor_quote(doc, "B1 retryoncepg ok").unwrap_err(), crate::llm::AtomRejection::QuoteMissing, "`_` is part of a word");
        assert_eq!(anchor_quote("a **x** b\na `x` b\n", "a x b").unwrap_err(), crate::llm::AtomRejection::QuoteAmbiguous);
    }

    /// v3.4 (a Linear comment window in prod): `atoms` as a JSON string
    /// holding the array is the same answer; anything else is still a schema
    /// error. Shared by every profile.
    #[test]
    fn a_stringified_atoms_array_is_parsed() {
        let inner = json!([{"kind":"fact","text":"Keep the decision.","evidence_quote":"keep it","entities":[]}]).to_string();
        let w = window("We keep it.", 0).unwrap();
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let atoms = validate_output_with_diagnostics(&json!({"atoms": inner}).to_string(), &w, &mut d).unwrap();
        assert_eq!((atoms.len(), d.stringified_atoms), (1, true));
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let v2 = v2::validate_structured(&json!({"atoms": inner}).to_string(), &w, None, v2::Contract::default(), &mut d).unwrap();
        assert_eq!((v2.len(), d.stringified_atoms), (1, true), "v2 has the same exposure and the same fix");
        for bad in [json!({"atoms":"not json"}), json!({"atoms":"{\"a\":1}"}), json!({"atoms":7})] {
            let mut d = crate::llm::ExtractionDiagnostics::default();
            assert!(validate_output_with_diagnostics(&bad.to_string(), &w, &mut d).is_err(), "{bad}");
            assert!(!d.stringified_atoms);
        }
    }

    #[test]
    fn v3_gets_a_larger_output_budget_than_v2() {
        assert_eq!(ExtractionProfile::SourceV3.max_tokens(), 16_000);
        for p in [ExtractionProfile::SourceV2, ExtractionProfile::MeetingV1] {
            assert_eq!(p.max_tokens(), 8_000);
        }
    }

    #[test]
    fn exact_evidence_uses_unicode_scalars_and_preserves_crlf() {
        let text = "🪸 Zoë:\r\n決定: keep café.\r\n";
        let selected = window(text, 2).unwrap();
        let quote = "決定: keep café.\r\n";
        let atoms = validate_output(&response(quote), &selected).unwrap();
        let atom = &atoms[0];
        assert_eq!(atom.start_char, 8);
        assert_eq!(
            text.chars()
                .skip(atom.start_char as usize)
                .take((atom.end_char - atom.start_char) as usize)
                .collect::<String>(),
            quote
        );
        assert_eq!(validate_output(&response("決定: keep café.\n"), &selected).unwrap()[0].evidence_quote, quote);
    }

    #[test]
    fn repeated_and_overlapping_quotes_are_rejected() {
        for (source, quote) in [("yes. yes.", "yes."), ("aaa", "aa")] {
            let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
            assert_eq!(validate_output_with_diagnostics(&response(quote), &window(source, 0).unwrap(), &mut diagnostics).unwrap_err(), NO_VALID_ATOMS);
            assert_eq!(diagnostics.rejected_atoms[&crate::llm::AtomRejection::QuoteAmbiguous], 1);
        }
        assert_eq!(validate_output(&response("absent"), &window("present", 0).unwrap()).unwrap_err(), NO_VALID_ATOMS);
    }

    /// v3.4.3: a schema error records where it is, as field names and
    /// indices, never the model's text.
    #[test]
    fn schema_errors_record_their_field_path_only() {
        let path = |output: &str| {
            let mut d = crate::llm::ExtractionDiagnostics::default();
            assert!(parse_extraction::<v2::ExtractionV2>(output, &mut d).is_err(), "{output}");
            d.schema_error_path
        };
        assert_eq!(path("{\"atoms\":null}").as_deref(), Some("atoms"));
        assert_eq!(path("{\"atoms\":\"Dana Smith's salary\"}").as_deref(), Some("atoms"), "the string's content is not the path");
        assert_eq!(path("{\"atoms\":5}").as_deref(), Some("atoms"));
        assert_eq!(path("{}"), None, "a missing envelope field has no path below the root");
        assert_eq!(path("not json at all"), None);
        let mut d = crate::llm::ExtractionDiagnostics::default();
        let _ = parse_extraction::<v2::ExtractionV2>("{\"atoms\":[]}", &mut d).unwrap();
        assert_eq!(d.schema_error_path, None);
        let mut d = crate::llm::ExtractionDiagnostics { rejected_atoms: [(crate::llm::AtomRejection::QuoteMissing, 2), (crate::llm::AtomRejection::QuoteAmbiguous, 1)].into(), ..Default::default() };
        assert!(unanchorable(&d));
        d.rejected_atoms.insert(crate::llm::AtomRejection::Schema, 1);
        assert!(!unanchorable(&d));
        assert!(!unanchorable(&crate::llm::ExtractionDiagnostics::default()));
    }

    #[test]
    fn malformed_is_not_empty_success() {
        let source = window("Alice will deliver.", 0).unwrap();
        for output in [
            "{}",
            "[]",
            "{\"atoms\":null}",
            "{\"atoms\":[],\"atoms\":[]}",
            "Here you go:\n```json\n{\"atoms\":[]}\n```",
            "```python\n{\"atoms\":[]}\n```",
        ] {
            assert_eq!(validate_output(output, &source).unwrap_err(), "invalid extraction schema", "{output}");
        }
        // Tolerated: extra envelope fields, and one fenced JSON block.
        for output in [
            "{\"atoms\":[]}",
            "{\"atoms\":[],\"extra\":true}",
            "```json\n{\"atoms\":[]}\n```",
            "```\n{\"atoms\":[]}\n```",
        ] {
            assert!(validate_output(output, &source).unwrap().is_empty(), "{output}");
        }
    }

    #[test]
    fn unknown_fields_and_malformed_items_are_dropped_individually() {
        let source = window("Alice will deliver Roadmap.", 0).unwrap();
        let output = json!({"atoms":[
            {"kind":"fact","text":"Alice delivers.","evidence_quote":"Alice will deliver Roadmap.","confidence":0.9,
             "entities":[{"mention":"Alice","type":"person","salience":1},{"mention":"Roadmap","type":"place"},
                         {"type":"project"},"Roadmap",{"mention":"Roadmap","type":"project"}]},
            {"kind":"fact","text":"Null entities mean none.","evidence_quote":"Alice will","entities":null},
            {"kind":"fact","text":"Missing entities mean none.","evidence_quote":"deliver Roadmap."}
        ]});
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        let atoms = validate_output_with_diagnostics(&output.to_string(), &source, &mut diagnostics).unwrap();
        assert_eq!(atoms.len(), 3);
        assert_eq!(atoms[0].entities.iter().map(|e| e.mention.as_str()).collect::<Vec<_>>(), ["Alice", "Roadmap"]);
        assert_eq!(atoms[0].entities[1].entity_type, EntityType::Project);
        assert!(atoms[1].entities.is_empty() && atoms[2].entities.is_empty());
        assert_eq!(diagnostics.dropped_entities[&crate::llm::AtomRejection::Schema], 3);
        assert!(diagnostics.rejected_atoms.is_empty());
    }

    #[test]
    fn too_many_atoms_keeps_the_first_valid_ones_and_records_the_cut() {
        let text: String = (0..60).map(|i| format!("Line {i:02} states a fact.\n")).collect();
        let source = window(&text, 0).unwrap();
        let mut atoms: Vec<serde_json::Value> = vec![json!({"kind":"fact","text":"Bad.","evidence_quote":"absent","entities":[]})];
        atoms.extend((0..60).map(|i| json!({"kind":"fact","text":format!("Fact {i}."),
            "evidence_quote":format!("Line {i:02} states a fact."),"entities":[]})));
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        let kept = validate_output_with_diagnostics(&json!({"atoms":atoms}).to_string(), &source, &mut diagnostics).unwrap();
        assert_eq!(kept.len(), MAX_ATOMS);
        assert_eq!(kept[0].evidence_quote, "Line 00 states a fact.", "an invalid candidate never takes a slot");
        assert_eq!(kept[MAX_ATOMS - 1].evidence_quote, format!("Line {:02} states a fact.", MAX_ATOMS - 1));
        assert_eq!(diagnostics.capped_atoms, 60 - MAX_ATOMS as u32);
        assert_eq!(diagnostics.rejected_atoms[&crate::llm::AtomRejection::QuoteMissing], 1);
        assert!(diagnostics.envelope_error.is_none());
        // A runaway response is read only up to the candidate bound.
        let flood = json!({"atoms":vec![json!({}); MAX_ATOM_CANDIDATES + 5]});
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        assert_eq!(validate_output_with_diagnostics(&flood.to_string(), &source, &mut diagnostics).unwrap_err(), NO_VALID_ATOMS);
        assert_eq!((diagnostics.rejected_atoms[&crate::llm::AtomRejection::Schema], diagnostics.capped_atoms), (MAX_ATOM_CANDIDATES as u32, 5));
    }

    #[test]
    fn quote_normalization_is_typographic_only_and_never_bridges_a_gap() {
        let text = "Bo said \"ship it\" and Ana's team agreed... on Friday.";
        let source = window(text, 0).unwrap();
        for (quote, expected) in [
            ("Bo said \u{201C}ship it\u{201D} and Ana\u{2019}s team", "Bo said \"ship it\" and Ana's team"),
            ("team agreed\u{2026} on Friday.", "team agreed... on Friday."),
            ("\u{2026}Ana's team agreed", "Ana's team agreed"),
            ("... Ana's team agreed ...", "Ana's team agreed"),
        ] {
            let atoms = validate_output(&response(quote), &source).unwrap_or_else(|e| panic!("{quote}: {e}"));
            assert_eq!(atoms[0].evidence_quote, expected, "{quote}");
            assert_eq!(text.chars().skip(atoms[0].start_char as usize).take((atoms[0].end_char - atoms[0].start_char) as usize).collect::<String>(), expected);
        }
        for fabricated in ["Bo said ... on Friday.", "Bo said 'ship it'", "Bo said ship it", "ship it now"] {
            assert_eq!(validate_output(&response(fabricated), &source).unwrap_err(), NO_VALID_ATOMS, "{fabricated}");
        }
    }

    #[test]
    fn retry_window_limit_halves_within_bounds() {
        assert_eq!(retry_window_limit(WINDOW_CHARS, 0), WINDOW_CHARS);
        assert_eq!(retry_window_limit(WINDOW_CHARS, 1), WINDOW_CHARS / 2);
        assert_eq!(retry_window_limit(WINDOW_CHARS, 2), WINDOW_CHARS / 4);
        assert_eq!(retry_window_limit(WINDOW_CHARS, 9), MIN_RETRY_WINDOW_CHARS);
        assert_eq!(retry_window_limit(1_500, 1), MIN_RETRY_WINDOW_CHARS);
        assert_eq!(retry_window_limit(15, 1), 15, "a short window is retried as is");
        let text = "x".repeat(WINDOW_CHARS * 2);
        let smaller = window_sized(&text, 0, retry_window_limit(WINDOW_CHARS, 1)).unwrap();
        assert_eq!((smaller.end_char, smaller.has_more), (WINDOW_CHARS / 2, true));
        assert_eq!(final_category(MODEL_TIMEOUT, true), "infrastructure_unavailable");
        assert_eq!(final_category(MODEL_TIMEOUT, false), "model request failed");
        assert_eq!(final_category(TRUNCATED, true), TRUNCATED);
    }

    #[test]
    fn entities_must_be_explicit_in_evidence() {
        let source = window("Alice will deliver.", 0).unwrap();
        let mut output: serde_json::Value =
            serde_json::from_str(&response("Alice will deliver.")).unwrap();
        output["atoms"][0]["entities"] = json!([{"mention": "Bob", "type": "person"}]);
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        let kept = validate_output_with_diagnostics(&output.to_string(), &source, &mut diagnostics).unwrap();
        assert_eq!(kept.len(), 1, "an unsupported mention never costs its atom");
        assert!(kept[0].entities.is_empty());
        assert_eq!(diagnostics.dropped_entities[&crate::llm::AtomRejection::EntityUnsupported], 1);
        output["atoms"][0]["entities"] = json!([{"mention": "Bob", "type": "person"}, {"mention": " Alice ", "type": "person"}]);
        assert_eq!(validate_output(&output.to_string(), &source).unwrap()[0].entities,
            vec![EntityMention { mention: "Alice".into(), entity_type: EntityType::Person }]);
        output["atoms"][0]["entities"] = json!([{"mention": "Alice", "type": "person"}]);
        assert_eq!(
            validate_output(&output.to_string(), &source).unwrap()[0].entities[0].entity_type,
            EntityType::Person
        );
    }

    #[test]
    fn duplicate_atom_fields_are_local_schema_rejections() {
        let output = r#"{"atoms":[{"kind":"fact","kind":"action","text":"Keep it.","evidence_quote":"Keep it.","entities":[]},{"kind":"fact","text":"Keep it.","evidence_quote":"Keep it.","entities":[]}]}"#;
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        let atoms = validate_output_with_diagnostics(output, &window("Keep it.", 0).unwrap(), &mut diagnostics).unwrap();
        assert_eq!(atoms.len(), 1);
        assert_eq!(atoms[0].kind, AtomKind::Fact);
        assert_eq!(diagnostics.rejected_atoms[&crate::llm::AtomRejection::Schema], 1);
    }

    #[test]
    fn atom_and_canonical_quote_bounds_remain_enforced() {
        use crate::llm::AtomRejection;
        let source = window("Alice will deliver.", 0).unwrap();
        let valid: serde_json::Value = serde_json::from_str(&response(source.text)).unwrap();
        let mut invalid = Vec::new();
        for text in [" ".to_string(), "x".repeat(MAX_TEXT_CHARS + 1)] {
            let mut atom = valid["atoms"][0].clone();
            atom["text"] = json!(text);
            invalid.push(atom);
        }
        for quote in [" ".to_string(), "x".repeat(MAX_QUOTE_CHARS + 1)] {
            let mut atom = valid["atoms"][0].clone();
            atom["evidence_quote"] = json!(quote);
            invalid.push(atom);
        }
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        assert_eq!(validate_output_with_diagnostics(&json!({"atoms": invalid}).to_string(), &source, &mut diagnostics).unwrap_err(), NO_VALID_ATOMS);
        assert_eq!(diagnostics.rejected_atoms[&AtomRejection::Text], 2);
        assert_eq!(diagnostics.rejected_atoms[&AtomRejection::QuoteBounds], 2);
        // Excess entities are dropped from the atom; the atom stays.
        let source_text = "Ann Bea Cat Dan Eve Fay Gus Hal Ivy";
        let names: Vec<&str> = source_text.split(' ').collect();
        let mut atom = json!({"kind":"fact","text":"Nine people.","evidence_quote":source_text,"entities":[]});
        atom["entities"] = json!(names.iter().map(|n| json!({"mention":n,"type":"person"})).collect::<Vec<_>>());
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        let kept = validate_output_with_diagnostics(&json!({"atoms":[atom]}).to_string(), &window(source_text, 0).unwrap(), &mut diagnostics).unwrap();
        assert_eq!(kept[0].entities.len(), MAX_ENTITIES);
        assert_eq!(diagnostics.dropped_entities[&AtomRejection::EntityLimit], 1);
        let source = format!("Alice{}will deliver.", " ".repeat(MAX_QUOTE_CHARS));
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        assert_eq!(validate_output_with_diagnostics(&response("Alice will deliver."), &window(&source, 0).unwrap(), &mut diagnostics).unwrap_err(), NO_VALID_ATOMS);
        assert_eq!(diagnostics.rejected_atoms[&AtomRejection::QuoteBounds], 1);
        for length in [MAX_MENTION_CHARS, MAX_MENTION_CHARS + 1] {
            let mention = "A".repeat(length);
            let output = json!({"atoms":[{"kind":"fact","text":"A supported mention.",
                "evidence_quote":mention,"entities":[{"mention":mention,"type":"person"}]}]});
            let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
            let atoms = validate_output_with_diagnostics(&output.to_string(), &window(&mention, 0).unwrap(), &mut diagnostics).unwrap();
            assert_eq!(atoms.len(), 1);
            assert_eq!(atoms[0].entities.len(), usize::from(length == MAX_MENTION_CHARS));
            assert_eq!(diagnostics.dropped_entities.get(&AtomRejection::EntityUnsupported).copied().unwrap_or(0), u32::from(length > MAX_MENTION_CHARS));
        }
    }

    #[test]
    fn formatting_match_preserves_whole_boundary_runs_and_literal_entity_support() {
        let source = window("🪸  Alice\twill  deliver.\r\n", 1).unwrap();
        let atoms = validate_output(&response(" Alice will deliver.\n"), &source).unwrap();
        assert_eq!(atoms[0].evidence_quote, "  Alice\twill  deliver.\r\n");
        assert_eq!(atoms[0].start_char, 1);
        let mut value: serde_json::Value = serde_json::from_str(&response("Alice will deliver.")).unwrap();
        value["atoms"][0]["entities"] = json!([{"mention":"Alice will", "type":"person"}]);
        assert!(validate_output(&value.to_string(), &source).unwrap()[0].entities.is_empty(),
            "entity support stays literal: a normalized match is not support");
        value["atoms"][0]["entities"] = json!([{"mention":"Alice", "type":"person"}]);
        assert_eq!(validate_output(&value.to_string(), &source).unwrap()[0].entities[0].mention, "Alice");
    }

    #[tokio::test]
    async fn atom_tools_send_strict_closed_schema_on_the_openrouter_wire() {
        use axum::Json;
        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let capture = captured.clone();
        let app = axum::Router::new().route("/v1/chat/completions", axum::routing::post(
            move |Json(body): Json<serde_json::Value>| {
                capture.lock().unwrap().push(body);
                async { Json(json!({"choices":[{"message":{"tool_calls":[{"function":{"arguments":"{\"atoms\":[]}"}}]},"finish_reason":"tool_calls"}],
                    "usage":{"prompt_tokens":10,"completion_tokens":10},"model":"deepseek/deepseek-v4-flash-0731"})) }
            }
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let provider = crate::llm::openai::OpenAiProvider::openrouter(reqwest::Client::new(), "synthetic-test-key")
            .with_base_url(format!("http://{address}"));
        for profile in [ExtractionProfile::MeetingV1] {
            let schema = tool_schema(profile);
            let response = provider.complete(CompletionRequest {
                messages: vec![Message {role: Role::User, content: "Synthetic retained source.".into()}],
                model: "deepseek/deepseek-v4-flash-0731".into(), max_tokens: 100,
                system: None, tool_schema: Some(schema.clone()), effort: None,
                reasoning: crate::llm::ReasoningSetting::Off,
            }).await.unwrap();
            assert_eq!(serde_json::from_str::<serde_json::Value>(&response.content).unwrap(), json!({"atoms":[]}));
            let body = captured.lock().unwrap().pop().unwrap();
            assert_eq!(body["tools"][0]["function"], json!({"name":profile.tool_name(), "strict":true,"parameters":schema.schema}));
            assert_eq!(body["tool_choice"]["function"]["name"], profile.tool_name());
        }
        server.abort();
    }

    #[test]
    fn atom_schemas_request_only_the_strict_provider_subset() {
        for profile in [ExtractionProfile::MeetingV1] {
            let schema = tool_schema(profile);
            assert!(schema.strict);
            fn check(value: &serde_json::Value) {
                if let Some(object) = value.as_object() {
                    for key in ["minLength", "maxLength", "minItems", "maxItems"] {
                        assert!(!object.contains_key(key));
                    }
                    if object.get("type") == Some(&json!("object")) {
                        assert_eq!(object["additionalProperties"], false);
                        assert_eq!(object["properties"].as_object().unwrap().len(), object["required"].as_array().unwrap().len());
                    }
                    for nested in object.values() { check(nested); }
                } else if let Some(array) = value.as_array() {
                    for nested in array { check(nested); }
                }
            }
            check(&schema.schema);
        }
    }

    #[test]
    fn windows_are_bounded_and_reconstruct_source_exactly() {
        let source = format!("{}\r\n{}", "🪸é".repeat(3_100), "決定".repeat(9_000));
        let mut start = 0;
        let mut joined = String::new();
        loop {
            let w = window(&source, start).unwrap();
            assert!(w.end_char - w.start_char <= WINDOW_CHARS);
            assert!(w.end_char > start);
            joined.push_str(w.text);
            start = w.end_char;
            if !w.has_more {
                break;
            }
        }
        assert_eq!(joined, source);
        assert!(window(&source, start + 1).is_err());
    }

    #[test]
    fn one_bad_atom_is_isolated_and_duplicates_are_coalesced() {
        let source = window("A decision.", 0).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&response("A decision.")).unwrap();
        let first = value["atoms"][0].clone();
        value["atoms"].as_array_mut().unwrap().push(first);
        assert_eq!(
            validate_output(&value.to_string(), &source).unwrap().len(),
            1
        );
        value["atoms"][1]["kind"] = json!("guess");
        let mut diagnostics = crate::llm::ExtractionDiagnostics::default();
        assert_eq!(validate_output_with_diagnostics(&value.to_string(), &source, &mut diagnostics).unwrap().len(), 1);
        assert_eq!(diagnostics.accepted_atoms, 1);
        assert_eq!(diagnostics.rejected_atoms[&crate::llm::AtomRejection::Schema], 1);
    }

    #[test]
    fn map_node_type_collapses_decision_to_entity() {
        assert_eq!(map_node_type("decision"), "entity");
        assert_eq!(map_node_type("thread"), "thread");
        assert_eq!(map_node_type("reference"), "reference");
        assert_eq!(map_node_type("person"), "entity");
        assert_eq!(map_node_type(""), "entity");
    }
}
