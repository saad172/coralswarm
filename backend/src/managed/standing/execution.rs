use std::time::Duration;

use coralswarm_core::r2::R2Client;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{principal, PrincipalRequest, StandingProfile};
use crate::{
    jobs::queue::JobRow,
    llm::LlmProvider,
    store::{Store, StoreError},
    tenancy::TenantId,
    workers::{
        embed::JobScope,
        tenant_harness::{Tick, WorkerTenant},
    },
};

pub(crate) const INTERVAL: Duration = Duration::from_secs(60);

pub(crate) fn harness_config() -> crate::workers::tenant_harness::HarnessConfig {
    let mut config=crate::workers::tenant_harness::HarnessConfig::sweep_worker("standing_extraction",INTERVAL);
    config.max_org_idle_backoff=INTERVAL;
    config
}

pub(crate) fn rollout_enabled() -> bool {
    std::env::var("STANDING_PROCESSING_ENABLED").is_ok_and(|value| value == "true")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HoldReason {
    Infrastructure,
    Principal,
    CloudDisabled,
    ProcessingDisabled,
    Operator,
    Lifecycle,
    DailyBudget,
    Credits,
    KillSwitch,
    SourceUnavailable,
}
impl HoldReason {
    pub(super) fn parse(reason: &str) -> Option<Self> {
        Some(match reason {
            "infrastructure_unavailable" | "fenced" | "managed run storage error" => {
                Self::Infrastructure
            }
            "principal_unavailable" | "requester_access_lost" => Self::Principal,
            "cloud_processing_disabled" => Self::CloudDisabled,
            "managed_processing_disabled" | "processing_off" => Self::ProcessingDisabled,
            "operator_paused" | "managed_run_paused" => Self::Operator,
            "lifecycle_contract_not_ready" => Self::Lifecycle,
            "daily_budget_exhausted" => Self::DailyBudget,
            "credits_exhausted" | "credit_unfunded" => Self::Credits,
            "managed_processing_killed" => Self::KillSwitch,
            "source_removed_or_access_lost" => Self::SourceUnavailable,
            _ => return None,
        })
    }
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Infrastructure => "infrastructure_unavailable",
            Self::Principal => "principal_unavailable",
            Self::CloudDisabled => "cloud_processing_disabled",
            Self::ProcessingDisabled => "managed_processing_disabled",
            Self::Operator => "operator_paused",
            Self::Lifecycle => "lifecycle_contract_not_ready",
            Self::DailyBudget => "daily_budget_exhausted",
            Self::Credits => "credits_exhausted",
            Self::KillSwitch => "managed_processing_killed",
            Self::SourceUnavailable => "source_removed_or_access_lost",
        }
    }
}

pub(crate) use crate::managed::StandingWork;

#[derive(Clone, Copy)]
enum Close {
    NothingToDo,
    Retry(&'static str),
}

fn payload_uuid(job: &JobRow, key: &str) -> Option<Uuid> {
    job.payload.get(key).and_then(|v| v.as_str()).and_then(|s| s.parse().ok())
}

/// The job kinds a worker may claim now. Standing kinds stay queued while
/// standing is paused.
pub(crate) fn claimable_kinds(kinds: &[&'static str]) -> Vec<&'static str> {
    kinds.iter().copied().filter(|kind| rollout_enabled() || !crate::jobs::STANDING_EXTRACTION_KINDS.contains(kind)).collect()
}

pub(crate) struct StandingExecutor<'a> {
    tenant: &'a TenantId,
    pool: &'a PgPool,
    control: &'a PgPool,
    store: &'a dyn Store,
    llm: &'a dyn LlmProvider,
    r2: Option<&'a R2Client>,
}
impl<'a> StandingExecutor<'a> {
    pub(crate) fn new(
        tenant: &'a TenantId,
        pool: &'a PgPool,
        control: &'a PgPool,
        store: &'a dyn Store,
        llm: &'a dyn LlmProvider,
        r2: Option<&'a R2Client>,
    ) -> Self {
        Self {
            tenant,
            pool,
            control,
            store,
            llm,
            r2,
        }
    }

    pub(crate) async fn process_job(&self, job: JobRow) {
        if !rollout_enabled() {
            // Paused standing keeps its queue: resuming needs no recovery (#1360).
            let _ = crate::jobs::queue::release(self.pool, job.id, INTERVAL).await;
            return;
        }
        let result = tokio::time::timeout(CLAIM_BUDGET, self.claim(&job))
            .await
            .unwrap_or_else(|_| Err(BudgetElapsed { stage: "claim", budget: CLAIM_BUDGET }.into()));
        match result {
            Ok(Some(work)) => {
                crate::workers::source_atoms::process_standing_job(
                    self.tenant,
                    self.pool,
                    self.control,
                    self.store,
                    self.llm,
                    self.r2,
                    job,
                    &work,
                )
                .await
            }
            Ok(None) => self.close(&job, Close::NothingToDo).await,
            // A queue retry never changes a retained source into a permanent extraction failure.
            Err(_) => self.close(&job, Close::Retry("standing infrastructure unavailable")).await,
        }
    }

    /// Ends an unclaimed attempt. When that leaves the unit with no job to
    /// deliver it, the same transaction hands the unit to requeue, so a
    /// terminal job never strands a queued unit (#1360). If the scoped
    /// transaction cannot run, the bare queue call still ends the attempt;
    /// that, or a lost commit, leaves the unit to requeue's undelivered scan.
    async fn close(&self, job: &JobRow, close: Close) {
        let target = job.account_id.zip(payload_uuid(job, "graph_id")).zip(payload_uuid(job, "source_version_id"));
        if let Some(((account, graph), version)) = target {
            let scope = JobScope::for_job(self.tenant, self.pool, account);
            let settled = async {
                let mut tx = match scope {
                    JobScope::Consumer { pool, account_id } => crate::tenancy::scope::begin_account_scoped(pool, account_id).await?,
                    JobScope::Org(pool) => crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await?,
                };
                match close {
                    Close::NothingToDo => crate::jobs::queue::complete_on(&mut *tx, job.id, crate::jobs::queue::JobOutcome::NothingToDo).await?,
                    Close::Retry(error) => crate::jobs::queue::fail_on(&mut *tx, job.id, error).await?,
                }
                super::release_undelivered_version_tx(&mut tx, graph, version).await?;
                Ok::<_, StoreError>(tx)
            }.await;
            if let Ok(tx) = settled {
                // A failed commit may still have landed; repeating the queue
                // call could charge a second attempt.
                let _ = tx.commit().await;
                return;
            }
        }
        let _ = match close {
            Close::NothingToDo => crate::jobs::queue::complete(self.pool, job.id, crate::jobs::queue::JobOutcome::NothingToDo).await,
            Close::Retry(error) => crate::jobs::queue::fail(self.pool, job.id, error).await,
        };
    }

    async fn claim(&self, job: &JobRow) -> Result<Option<StandingWork>, StoreError> {
        let Some(account) = job.account_id else {
            return Ok(None);
        };
        let Some(graph) = payload_uuid(job, "graph_id") else {
            return Ok(None);
        };
        let Some(version) = payload_uuid(job, "source_version_id") else {
            return Ok(None);
        };
        let Some(cursor) = job
            .payload
            .get("start_char")
            .and_then(|v| v.as_i64())
            .filter(|v| *v >= 0 && *v <= i32::MAX as i64)
        else {
            return Ok(None);
        };
        let scope = JobScope::for_job(self.tenant, self.pool, account);
        let mut content = begin_content(&scope).await?;
        let actual_job: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1 AND account_id=$2 AND status::text='running' AND kind=$3 AND payload=$4)")
            .bind(job.id).bind(account).bind(&job.kind).bind(&job.payload).fetch_one(&mut *content).await?;
        if !actual_job {
            return Ok(None);
        }
        let old: Option<(Uuid, String, Option<Uuid>, bool)> = sqlx::query_as(
            "SELECT r.run_id,u.extraction_version,u.standing_logical_call,COALESCE(u.standing_claim_until>now(),false) FROM corpus_enrichment_units u JOIN corpus_enrichment_runs r ON r.run_id=u.run_id JOIN graphs g ON g.id=r.graph_id WHERE u.graph_id=$1 AND u.holding_id=$2 AND u.execution_owner AND r.run_kind='standing' AND r.account_id=$3 AND g.account_id=$3 AND u.state='queued' AND u.reason IS NULL")
            .bind(graph).bind(version).bind(account).fetch_optional(&mut *content).await?;
        content.rollback().await?;
        let Some((run, profile, old_call, busy)) = old else {
            return Ok(None);
        };
        if busy {
            return Ok(None);
        }
        if let Some(old_call) = old_call {
            crate::managed::gate::recover_logical_call(
                &scope,
                self.control,
                self.tenant,
                account,
                old_call,
            )
            .await?;
        }
        let mut content = begin_content(&scope).await?;
        let mut control = begin_control(self.tenant, self.control, account).await?;
        bound(&mut content, control.as_mut()).await?;
        share_run_gate(&mut content, graph).await?;
        let principal: Option<Uuid> = sqlx::query_scalar("SELECT execution_principal FROM corpus_enrichment_runs WHERE run_id=$1 AND account_id=$2 AND graph_id=$3 AND run_kind='standing' FOR SHARE")
            .bind(run).bind(account).bind(graph).fetch_optional(&mut *content).await?.flatten();
        if principal.is_none() {
            return Ok(None);
        }
        let parent: Option<Uuid> = sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND access_state='permitted' AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE")
            .bind(graph).bind(version).fetch_optional(&mut *content).await?;
        if parent.is_none() {
            return Ok(None);
        }
        let current: Option<i64> = sqlx::query_scalar("SELECT next_start_char FROM source_versions WHERE graph_id=$1 AND version_id=$2 AND extraction_version=$3 AND extraction_status NOT IN ('done','failed') FOR UPDATE")
            .bind(graph).bind(version).bind(&profile).fetch_optional(&mut *content).await?;
        if current != Some(cursor) {
            return Ok(None);
        }
        let unit: Option<(Uuid, i64)> = sqlx::query_as(
            "UPDATE corpus_enrichment_units SET standing_generation=standing_generation+1,standing_logical_call=$5,standing_start_cursor=$6,standing_claim_until=now()+$8::double precision*interval '1 second',state='queued',reason=NULL,retry_after=NULL,updated_at=now() WHERE run_id=$1 AND graph_id=$2 AND holding_id=$3 AND execution_owner AND extraction_version=$4 AND state='queued' AND reason IS NULL AND (standing_claim_until IS NULL OR standing_claim_until<=now()) AND standing_logical_call IS NOT DISTINCT FROM $7 RETURNING unit_id,standing_generation")
            .bind(run).bind(graph).bind(version).bind(&profile).bind(Uuid::new_v4()).bind(cursor).bind(old_call).bind(crate::workers::reaper::stale_after().as_secs_f64())
            .fetch_optional(&mut *content).await?;
        let Some((unit, generation)) = unit else {
            return Ok(None);
        };
        let logical_call: Uuid = sqlx::query_scalar(
            "SELECT standing_logical_call FROM corpus_enrichment_units WHERE unit_id=$1",
        )
        .bind(unit)
        .fetch_one(&mut *content)
        .await?;
        let work = StandingWork {
            account_id: account,
            graph_id: graph,
            run_id: run,
            unit_id: unit,
            version_id: version,
            job_id: job.id,
            profile: StandingProfile::parse(&profile)
                .map_err(|_| sqlx::Error::Protocol("unsupported standing profile".into()))?,
            generation,
            logical_call,
            start_cursor: cursor,
        };
        if !attempt_allowed_locked(&mut content, control.as_mut(), self.tenant, &work).await? {
            if !current_work_tx(&mut content,&work).await? { return Ok(None); }
            let reason = denial_reason_locked(&mut content, control.as_mut(), &work).await?;
            hold_work_tx(&mut content, &work, reason.code()).await?;
            if let Some(control) = control {
                control.rollback().await?;
            }
            content.commit().await?;
            return Ok(None);
        }
        if let Some(control) = control {
            control.rollback().await?;
        }
        content.commit().await?;
        Ok(Some(work))
    }
}

pub(crate) async fn sweep_tick(worker: &WorkerTenant, control: &PgPool) -> Result<Tick, String> {
    if !rollout_enabled() { return Ok(Tick::worked(0)); }
    tokio::time::timeout(
        Duration::from_secs(120),
        super::admission::sweep_tick(worker, control),
    )
    .await
    .map_err(|_| "standing sweep timed out".to_owned())?
    .map_err(|error| format!("standing sweep unavailable: {error}"))
}

/// Wall-clock bound on opening a standing content or control transaction
/// (pool acquire, `BEGIN`, scope `set_config`). Past it the work is held as
/// `infrastructure_unavailable` and retried, so a stalled pool cannot stall
/// the worker.
const BEGIN_BUDGET: Duration = Duration::from_millis(500);

const CLAIM_BUDGET: Duration = Duration::from_secs(10);

/// A standing budget ran out. Distinct from sqlx's `PoolTimedOut`: the budget
/// covers the pool wait and every round trip, so a slow database reads as
/// elapsed time here, not as an exhausted pool.
#[derive(Debug)]
struct BudgetElapsed {
    stage: &'static str,
    budget: Duration,
}

impl std::fmt::Display for BudgetElapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "standing {} budget of {:?} elapsed (pool wait and round trips)", self.stage, self.budget)
    }
}

impl std::error::Error for BudgetElapsed {}

impl From<BudgetElapsed> for StoreError {
    fn from(elapsed: BudgetElapsed) -> Self {
        StoreError::Db(Box::new(elapsed))
    }
}

/// [`BEGIN_BUDGET`], or `STANDING_BEGIN_BUDGET_MS` in test-support builds.
/// On a loaded machine the PG fixtures' round-trips can take longer than
/// 500 ms of wall clock, and a sequential sweep then fails with
/// [`BudgetElapsed`]. That is a property of the machine, not of the code
/// under test. Production builds never read the variable.
fn begin_budget() -> Duration {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(ms) = std::env::var("STANDING_BEGIN_BUDGET_MS").ok().and_then(|v| v.parse::<u64>().ok()) {
        return Duration::from_millis(ms);
    }
    BEGIN_BUDGET
}

pub(super) async fn begin_content<'a>(
    scope: &JobScope<'a>,
) -> Result<Transaction<'a, Postgres>, StoreError> {
    tokio::time::timeout(begin_budget(), async {
        match scope {
            JobScope::Consumer { pool, account_id } => {
                crate::tenancy::scope::begin_account_scoped(pool, *account_id).await
            }
            JobScope::Org(pool) => {
                crate::tenancy::scope::begin_tenant_system_scoped_without_account(pool).await
            }
        }
    })
    .await
    .map_err(|_| StoreError::from(BudgetElapsed { stage: "begin", budget: begin_budget() }))?
}
pub(crate) async fn begin_control<'a>(
    tenant: &TenantId,
    control: &'a PgPool,
    account: Uuid,
) -> Result<Option<Transaction<'a, Postgres>>, StoreError> {
    match tenant {
        TenantId::Consumer => Ok(None),
        TenantId::Org(_) => Ok(Some(
            tokio::time::timeout(
                begin_budget(),
                crate::tenancy::scope::begin_account_scoped(control, account),
            )
            .await
            .map_err(|_| StoreError::from(BudgetElapsed { stage: "begin", budget: begin_budget() }))??,
        )),
    }
}
/// How long a finished window waits for its graph's publication turn. The
/// model call is already paid for, so waiting is cheaper than discarding.
const PUBLICATION_TURN_BUDGET: Duration = Duration::from_secs(60);
const PUBLICATION_TURN_POLL: Duration = Duration::from_millis(200);

/// Opens the content transaction a standing window publishes in, holding its
/// graph's publication turn: a transaction advisory lock, so publications of
/// one graph still run one at a time (reef minting, binding and supersession
/// read what the previous window wrote), as they did when the run row lock
/// serialized them. The run row itself is only share-locked now, so claims
/// and admissions never wait for a publication. Waiting happens between
/// short `pg_try_advisory_xact_lock` attempts, not inside a transaction, so a
/// queued publication holds no pooled connection. Lock order: turn, run,
/// parent, version; nothing that takes the run row exclusively takes a turn.
pub(crate) async fn begin_publication<'a>(
    scope: &JobScope<'a>,
    graph: Uuid,
) -> Result<Transaction<'a, Postgres>, StoreError> {
    let deadline = tokio::time::Instant::now() + PUBLICATION_TURN_BUDGET;
    loop {
        let mut tx = begin_content(scope).await?;
        let turn: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock(hashtextextended('standing-publication:'||$1::text,0))",
        )
        .bind(graph)
        .fetch_one(&mut *tx)
        .await?;
        if turn {
            return Ok(tx);
        }
        tx.rollback().await?;
        if tokio::time::Instant::now() >= deadline {
            return Err(BudgetElapsed { stage: "publication turn", budget: PUBLICATION_TURN_BUDGET }.into());
        }
        tokio::time::sleep(PUBLICATION_TURN_POLL).await;
    }
}

pub(crate) async fn bound(
    content: &mut Transaction<'_, Postgres>,
    control: Option<&mut Transaction<'_, Postgres>>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT set_config('lock_timeout','500ms',true),set_config('statement_timeout','2s',true)",
    )
    .execute(&mut **content)
    .await?;
    if let Some(control) = control {
        sqlx::query("SELECT set_config('lock_timeout','500ms',true),set_config('statement_timeout','2s',true)").execute(&mut **control).await?;
    }
    Ok(())
}

pub(super) async fn policy_locked(
    tx: &mut Transaction<'_, Postgres>,
    account: Uuid,
) -> Result<Option<HoldReason>, sqlx::Error> {
    let prefs: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT prefs FROM account_prefs WHERE account_id=$1 FOR SHARE")
            .bind(account)
            .fetch_optional(&mut **tx)
            .await?;
    if let Some(prefs) = prefs {
        if prefs.get("managed_processing").and_then(|v| v.as_str()) == Some("off") {
            return Ok(Some(HoldReason::ProcessingDisabled));
        }
    }
    if crate::managed::Caps::from_env().disabled {
        return Ok(Some(HoldReason::KillSwitch));
    }
    Ok(None)
}

/// The standing run gate: a transaction advisory lock per graph that every
/// standing fence takes shared, before it share-locks the run row, and every
/// writer of a standing run row (pause, principal change) takes exclusively,
/// before it touches the row. Row share locks alone are not fair: a waiting
/// `UPDATE` waits for every share holder, and a new `FOR SHARE` is granted
/// beside the existing ones, so overlapping fences kept an operator pause
/// waiting for as long as windows kept arriving (7.57 s measured on PG17).
/// Heavyweight locks queue: once a writer waits for the gate, new fences
/// wait behind it, so the pause lands as soon as the fences already open
/// commit. Order: gate, then run row, then parent and version.
pub(crate) async fn share_run_gate(tx: &mut sqlx::PgConnection, graph: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock_shared(hashtextextended('standing-run-gate:'||$1::text,0))")
        .bind(graph).execute(tx).await?;
    Ok(())
}

/// See [`share_run_gate`]. A transaction that already holds the gate shared
/// may take it exclusively; it then waits only for other transactions.
pub(crate) async fn exclusive_run_gate(tx: &mut sqlx::PgConnection, graph: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('standing-run-gate:'||$1::text,0))")
        .bind(graph).execute(tx).await?;
    Ok(())
}

/// The standing fence: the run is running under its current principal, the
/// graph, source, policy and lifecycle allow the attempt, and this work still
/// owns its unit and job. It holds the run row `FOR SHARE` until the caller
/// commits. A pause or principal change (`UPDATE`) still waits for every open
/// fence, and a fence opened after one sees it. Fences no longer exclude each
/// other: as `FOR UPDATE` they made every claim, admission and publication of
/// one graph queue on one row, and a wait past the 500 ms `lock_timeout`
/// discarded a window the model had already been paid for (2026-10-21: 611
/// of 2,233 standing windows in 6 h on one org tenant).
pub(crate) async fn attempt_allowed_locked(
    content: &mut Transaction<'_, Postgres>,
    mut control: Option<&mut Transaction<'_, Postgres>>,
    tenant: &TenantId,
    work: &StandingWork,
) -> Result<bool, sqlx::Error> {
    if !rollout_enabled() { return Ok(false); }
    share_run_gate(content, work.graph_id).await?;
    let row: Option<(Option<Uuid>, String, String)> = sqlx::query_as(
        "SELECT execution_principal,status,standing_pause_state FROM corpus_enrichment_runs WHERE run_id=$1 AND graph_id=$2 AND account_id=$3 AND extraction_version=$4 AND run_kind='standing' AND mode='execute' AND corpus_kind='real' FOR SHARE")
        .bind(work.run_id).bind(work.graph_id).bind(work.account_id).bind(work.profile.as_str()).fetch_optional(&mut **content).await?;
    let Some((Some(current), status, pause)) = row else {
        return Ok(false);
    };
    if status != "running" || pause != "none" {
        return Ok(false);
    }
    let graph_exists = sqlx::query(
        "SELECT id FROM graphs WHERE id=$1 AND account_id=$2 AND archived_at IS NULL FOR SHARE",
    )
    .bind(work.graph_id)
    .bind(work.account_id)
    .fetch_optional(&mut **content)
    .await?
    .is_some();
    if !graph_exists {
        return Ok(false);
    }
    let selected = principal::execution_principal_locked(
        content,
        control.as_deref_mut(),
        tenant,
        &PrincipalRequest {
            account_id: work.account_id,
            graph_id: work.graph_id,
            run_id: work.run_id,
            profile: work.profile,
            expected_principal: current,
        },
        work.unit_id,
        work.version_id,
    )
    .await
    .map_err(|_| sqlx::Error::Protocol("standing principal observation unavailable".into()))?;
    if selected.is_none() {
        return Ok(false);
    }
    let policy = if let Some(control) = control.as_deref_mut() {
        policy_locked(control, work.account_id).await?
    } else {
        policy_locked(content, work.account_id).await?
    };
    if policy.is_some() || !crate::corpus_enrichment::lifecycle_ready_tx(content).await? {
        return Ok(false);
    }
    sqlx::query(
        "SELECT id FROM graphs WHERE id=$1 AND account_id=$2 AND archived_at IS NULL FOR SHARE",
    )
    .bind(work.graph_id)
    .bind(work.account_id)
    .fetch_optional(&mut **content)
    .await?
    .ok_or(sqlx::Error::RowNotFound)?;
    let parent: Option<Uuid> = sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND access_state='permitted' AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE")
        .bind(work.graph_id).bind(work.version_id).fetch_optional(&mut **content).await?;
    if parent.is_none() {
        return Ok(false);
    }
    let valid: Option<bool> = sqlx::query_scalar(&format!(
        "SELECT v.next_start_char=$3 AND v.extraction_version=$4 AND v.extraction_status NOT IN ('done','failed') AND {} AND NOT COALESCE(v.origin->>'transport'='synthetic',false) AND NOT COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false) FROM source_versions v WHERE graph_id=$1 AND version_id=$2 FOR UPDATE",
        super::admission::STANDING_ELIGIBLE))
        .bind(work.graph_id).bind(work.version_id).bind(work.start_cursor).bind(work.profile.as_str()).fetch_optional(&mut **content).await?;
    if valid != Some(true) {
        return Ok(false);
    }
    let owner: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE unit_id=$1 AND run_id=$2 AND graph_id=$3 AND holding_id=$4 AND kind='retained_version' AND execution_owner AND extraction_version=$5 AND standing_generation=$6 AND standing_logical_call=$7 AND standing_start_cursor=$8 AND state='queued' AND standing_claim_until>now())")
        .bind(work.unit_id).bind(work.run_id).bind(work.graph_id).bind(work.version_id).bind(work.profile.as_str())
        .bind(work.generation).bind(work.logical_call).bind(work.start_cursor).fetch_one(&mut **content).await?;
    if !owner {
        return Ok(false);
    }
    let job: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1 AND account_id=$2 AND status::text='running' AND kind=$3 AND payload->>'graph_id'=$4 AND payload->>'source_version_id'=$5 AND (payload->>'start_char')::bigint=$6)")
        .bind(work.job_id).bind(work.account_id)
        .bind(work.profile.job_kind())
        .bind(work.graph_id.to_string()).bind(work.version_id.to_string()).bind(work.start_cursor)
        .fetch_one(&mut **content).await?;
    Ok(job)
}

pub(crate) async fn current_work_tx(
    content: &mut Transaction<'_, Postgres>,
    work: &StandingWork,
) -> Result<bool, sqlx::Error> {
    share_run_gate(content, work.graph_id).await?;
    let run = sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE run_id=$1 AND graph_id=$2 AND account_id=$3 AND extraction_version=$4 AND run_kind='standing' FOR SHARE")
        .bind(work.run_id).bind(work.graph_id).bind(work.account_id).bind(work.profile.as_str()).fetch_optional(&mut **content).await?;
    if run.is_none() {
        return Ok(false);
    }
    sqlx::query("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE")
        .bind(work.graph_id).bind(work.version_id).fetch_optional(&mut **content).await?;
    let version: Option<bool> = sqlx::query_scalar("SELECT next_start_char=$3 AND extraction_version=$4 AND extraction_status NOT IN ('done','failed') FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE")
        .bind(work.graph_id).bind(work.version_id).bind(work.start_cursor).bind(work.profile.as_str())
        .fetch_optional(&mut **content).await?;
    if version == Some(false) { return Ok(false); }
    let job: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1 AND account_id=$2 AND status::text='running' AND kind=$3 AND payload->>'graph_id'=$4 AND payload->>'source_version_id'=$5 AND (payload->>'start_char')::bigint=$6)")
        .bind(work.job_id).bind(work.account_id)
        .bind(work.profile.job_kind())
        .bind(work.graph_id.to_string()).bind(work.version_id.to_string()).bind(work.start_cursor)
        .fetch_one(&mut **content).await?;
    if !job { return Ok(false); }
    let current: Option<bool> = sqlx::query_scalar("SELECT standing_generation=$2 AND standing_logical_call=$3 AND standing_start_cursor=$4 AND standing_claim_until>now() AND state='queued' FROM corpus_enrichment_units WHERE unit_id=$1 AND run_id=$5 AND graph_id=$6 AND holding_id=$7 AND execution_owner AND extraction_version=$8 FOR UPDATE")
        .bind(work.unit_id).bind(work.generation).bind(work.logical_call).bind(work.start_cursor)
        .bind(work.run_id).bind(work.graph_id).bind(work.version_id).bind(work.profile.as_str()).fetch_optional(&mut **content).await?;
    Ok(current == Some(true))
}

pub(crate) async fn hold_work_tx(
    content: &mut Transaction<'_, Postgres>,
    work: &StandingWork,
    reason: &str,
) -> Result<(), sqlx::Error> {
    if current_work_tx(content, work).await? {
        super::hold_unit_tx(
            content,
            work.graph_id,
            work.version_id,
            work.profile.as_str(),
            reason,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn denial_reason_locked(
    content: &mut Transaction<'_, Postgres>,
    control: Option<&mut Transaction<'_, Postgres>>,
    work: &StandingWork,
) -> Result<HoldReason, sqlx::Error> {
    if !rollout_enabled() { return Ok(HoldReason::KillSwitch); }
    let pause: Option<String> = sqlx::query_scalar("SELECT standing_pause_state FROM corpus_enrichment_runs WHERE run_id=$1 AND graph_id=$2 AND account_id=$3")
        .bind(work.run_id).bind(work.graph_id).bind(work.account_id).fetch_optional(&mut **content).await?;
    if matches!(pause.as_deref(), Some("operator" | "both")) {
        return Ok(HoldReason::Operator);
    }
    if pause.as_deref() == Some("processing") {
        return Ok(HoldReason::ProcessingDisabled);
    }
    let policy = if let Some(control) = control {
        policy_locked(control, work.account_id).await?
    } else {
        policy_locked(content, work.account_id).await?
    };
    if let Some(policy) = policy {
        return Ok(policy);
    }
    if !crate::corpus_enrichment::source_permitted_tx(
        content,
        work.graph_id,
        work.version_id,
        work.profile.as_str(),
    )
    .await?
    {
        return Ok(HoldReason::SourceUnavailable);
    }
    if !crate::corpus_enrichment::lifecycle_ready_tx(content).await? {
        return Ok(HoldReason::Lifecycle);
    }
    Ok(HoldReason::Principal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_stalled_begin_reports_the_budget_not_an_exhausted_pool() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let _silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(30))
            .connect_lazy(&format!("postgres://fixture@127.0.0.1:{port}/fixture"))
            .unwrap();
        let error = begin_control(&TenantId::Org("org_fixture".into()), &pool, Uuid::nil())
            .await
            .err()
            .expect("a database that never answers cannot open a transaction");
        assert!(error.to_string().contains("begin budget"), "{error}");
        assert!(!format!("{error:?}").contains("PoolTimedOut"), "{error:?}");
    }
}
