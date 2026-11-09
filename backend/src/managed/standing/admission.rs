use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    execution::{self, HoldReason},
    principal, PauseAction,
};
use crate::{
    corpus_enrichment::{self, Run},
    store::StoreError,
    tenancy::TenantId,
    workers::{
        embed::JobScope,
        tenant_harness::{Tick, WorkerTenant},
    },
};

const ACCOUNT_LIMIT: usize = 24;
/// Resume an unfinished account round promptly, without tight-looping.
const SCAN_CONTINUATION: std::time::Duration = std::time::Duration::from_secs(1);
const GRAPH_LIMIT: usize = 16;
/// Ceiling for one (graph, slot). Adoption stops starting batches at
/// [`ADOPT_BUDGET`] and requeue at [`REQUEUE_BUDGET`], each committing what
/// it has, so this only fires on a stuck statement, never on a long backlog.
const SLOT_BUDGET: std::time::Duration = std::time::Duration::from_secs(8);
const ADOPT_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
const REQUEUE_BUDGET: std::time::Duration = std::time::Duration::from_secs(6);
/// Versions adopted per transaction. The run row stays locked for one batch,
/// which bounds how long a concurrent window's fence can wait on it.
const ADOPT_BATCH: usize = 25;
/// Units requeued per transaction, for the same reason.
const REQUEUE_BATCH: i64 = 25;
/// Candidate versions read per slot.
const ADOPT_PAGE: i64 = 200;
const SWEEP_BUDGET: std::time::Duration = std::time::Duration::from_secs(110);

/// Captures standing may process: its backlog floor, plus explicit forward
/// re-extraction of an earlier capture (a v2 pin over v1 windows, or a v3 pin
/// over v1 or v2 windows, which only a re-extraction request creates), plus an earlier capture an operator
/// listed in `standing_floor_overrides` for its current profile (only
/// `retry_failed_extractions_v2 --before-floor` writes one; see its migration).
pub(crate) const STANDING_ELIGIBLE: &str = "(v.captured_at>='2026-10-13T00:00:00Z' OR (v.extraction_version IN ('source-atoms-v2','source-atoms-v3') AND EXISTS(SELECT 1 FROM semantic_atom_windows w WHERE w.source_version_id=v.version_id AND w.graph_id=v.graph_id AND w.extraction_version IN ('source-atoms-v1','source-atoms-v2') AND w.extraction_version<>v.extraction_version)) OR EXISTS(SELECT 1 FROM standing_floor_overrides o WHERE o.source_version_id=v.version_id AND o.graph_id=v.graph_id AND o.extraction_version=v.extraction_version))";

/// Blocked reasons standing adoption admits.
const ADOPTABLE_ERRORS: &str = "('managed_admission_required','capture_actor_unknown_managed_admission_required','requester_access_lost','capture_actor_unknown','reextraction_requested')";

type AdoptCursor = (chrono::DateTime<chrono::Utc>, Uuid);
/// Per (plane, graph, profile): the last version adoption attempted. The next
/// page starts after it, so a version that keeps failing cannot hold every
/// later version back; the cursor wraps when a page comes back short.
fn adopt_cursors() -> &'static std::sync::Mutex<std::collections::HashMap<(String, Uuid, String), AdoptCursor>> {
    static CURSORS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<(String, Uuid, String), AdoptCursor>>> = std::sync::OnceLock::new();
    CURSORS.get_or_init(Default::default)
}

#[derive(Debug, thiserror::Error)]
enum RecoveryFailure {
    #[error("unknown standing hold reason: unit={unit} reason={reason:?}")]
    UnknownHoldReason { unit: Uuid, reason: String },
    #[error("standing sweep slot failed: {0}")]
    Slot(String),
}

#[derive(Default)]
struct SweepPosition {
    account: Option<Uuid>,
    /// Visits left in this finite round. Snapshot the count, not an endless
    /// "inventory > ACCOUNT_LIMIT" predicate: idle accounts and arrivals must
    /// still reach the normal maintenance poll rather than spin forever.
    remaining_accounts: usize,
    graph: std::collections::HashMap<Uuid, Uuid>,
    slot: std::collections::HashMap<Uuid, (Uuid, usize)>,
}
fn positions() -> &'static std::sync::Mutex<std::collections::HashMap<String, SweepPosition>> {
    static POSITIONS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, SweepPosition>>> = std::sync::OnceLock::new();
    POSITIONS.get_or_init(Default::default)
}
fn tenant_key(worker: &WorkerTenant) -> String {
    let plane=match &worker.tenant { TenantId::Consumer => "consumer".into(), TenantId::Org(org) => format!("org:{org}") };
    format!("{plane}:{}",worker.pool.connect_options().get_database().unwrap_or_default())
}
fn rotate_after(values: &mut [Uuid], last: Option<Uuid>) {
    if let Some(last) = last {
        let next = values.iter().position(|id| *id > last).unwrap_or(0);
        values.rotate_left(next);
    }
}

fn sweep_now(worker: &WorkerTenant) -> tokio::time::Instant {
    let now=tokio::time::Instant::now();
    #[cfg(feature="test-support")]
    return super::probe::sweep_instant(worker.pool.connect_options().get_database().unwrap_or_default(),now);
    #[cfg(not(feature="test-support"))]
    { let _=worker; now }
}

pub(super) async fn sweep_tick(
    worker: &WorkerTenant,
    control_pool: &PgPool,
) -> Result<Tick, StoreError> {
    let mut accounts: Vec<Uuid> = match &worker.tenant {
        // Personal orgs included: they live on the consumer plane (COR-114).
        TenantId::Consumer => {
            let mut connection = worker.pool.acquire().await?;
            crate::managed::plane::consumer_plane_accounts(&mut connection).await?
        }
        TenantId::Org(_) => {
            let mut tx = crate::tenancy::scope::begin_tenant_system_scoped_without_account(&worker.pool).await?;
            let accounts = sqlx::query_scalar("SELECT DISTINCT account_id FROM graphs ORDER BY account_id")
                .fetch_all(&mut *tx).await?;
            tx.rollback().await?;
            accounts
        }
    };
    let key = tenant_key(worker);
    #[cfg(feature="test-support")]
    if super::probe::begin_sweep_clock(worker.pool.connect_options().get_database().unwrap_or_default()) {
        positions().lock().expect("standing sweep position poisoned").remove(&key);
    }
    let previous = positions().lock().expect("standing sweep position poisoned")
        .get(&key).and_then(|position| position.account);
    rotate_after(&mut accounts, previous);
    let remaining = {
        let mut all = positions().lock().expect("standing sweep position poisoned");
        let position = all.entry(key.clone()).or_default();
        position.remaining_accounts = if position.remaining_accounts == 0 {
            accounts.len()
        } else {
            // Deletions can shorten a round. New accounts cannot extend one;
            // the retained cursor reaches them in subsequent rounds.
            position.remaining_accounts.min(accounts.len())
        };
        position.remaining_accounts
    };
    let deadline = sweep_now(worker) + SWEEP_BUDGET;
    let mut processed = 0;
    let mut failure = None;
    for account in accounts.into_iter().take(ACCOUNT_LIMIT.min(remaining)) {
        if sweep_now(worker) >= deadline { break; }
        {
            let mut all = positions().lock().expect("standing sweep position poisoned");
            let position = all.entry(key.clone()).or_default();
            position.account = Some(account);
            position.remaining_accounts = position.remaining_accounts.saturating_sub(1);
        }
        #[cfg(feature="test-support")]
        super::probe::visit_sweep_account(worker.pool.connect_options().get_database().unwrap_or_default(),account);
        let scope = JobScope::for_job(&worker.tenant, &worker.pool, account);
        let graphs = tokio::time::timeout(SLOT_BUDGET, async {
            let mut content = execution::begin_content(&scope).await?;
            let graphs: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs WHERE account_id=$1 AND archived_at IS NULL ORDER BY id")
                .bind(account).fetch_all(&mut *content).await?;
            content.rollback().await?;
            Ok::<_,StoreError>(graphs)
        }).await;
        let mut graphs = match graphs {
            Ok(Ok(graphs)) => graphs,
            other => {
                let message=format!("account={account} graph discovery: {other:?}");
                tracing::error!(alert=true, %message, "standing_sweep_slot_failed");
                failure=Some(message); continue;
            }
        };
        let (previous, continuation)={
            let all=positions().lock().expect("standing sweep position poisoned");
            let position=all.get(&key);
            (position.and_then(|position|position.graph.get(&account).copied()),
             position.and_then(|position|position.slot.get(&account).copied()))
        };
        let continuation=continuation.filter(|(graph,_)|graphs.contains(graph));
        if let Some((graph,_))=continuation {
            let index=graphs.iter().position(|candidate|*candidate==graph).expect("retained sweep graph");
            graphs.rotate_left(index);
        } else {
            rotate_after(&mut graphs,previous);
        }
        for graph in graphs.into_iter().take(GRAPH_LIMIT) {
            let first=continuation.filter(|(candidate,_)|*candidate==graph).map(|(_,slot)|slot).unwrap_or(0);
            for slot in first..4 {
                if sweep_now(worker) >= deadline { break; }
                positions().lock().expect("standing sweep position poisoned").entry(key.clone()).or_default().slot.insert(account,(graph,slot));
                // Slot 0 adopts under the active source profile; slot 1 drains
                // every source-profile run that no longer adopts (retired, or
                // the inactive live profile after an operator switch).
                let profile=if slot<2 {crate::workers::source_atoms::active_source_profile()} else {"meeting-atoms-v1"};
                let retired=slot%2==1;
                let requeue_deadline=tokio::time::Instant::now()+REQUEUE_BUDGET;
                let result=tokio::time::timeout(SLOT_BUDGET, async {
                    match slot {
                        1 => drain_source_profiles(&scope,control_pool,&worker.tenant,account,graph,profile,requeue_deadline).await,
                        3 => requeue_retired(&scope,control_pool,&worker.tenant,account,graph,profile,requeue_deadline).await,
                        _ => reconcile_graph(&scope,control_pool,&worker.tenant,account,graph,profile,&key).await,
                    }
                }).await;
                {
                    let mut all=positions().lock().expect("standing sweep position poisoned");
                    let position=all.entry(key.clone()).or_default();
                    if slot==3 {
                        position.slot.remove(&account);
                        position.graph.insert(account,graph);
                    } else {
                        position.slot.insert(account,(graph,slot+1));
                    }
                }
                #[cfg(feature="test-support")]
                super::probe::advance_sweep_clock(worker.pool.connect_options().get_database().unwrap_or_default(),graph,profile,retired);
                match result {
                    Ok(Ok(count)) => processed+=count,
                    other => {
                        let message=format!("account={account} graph={graph} profile={profile} retired={retired}: {other:?}");
                        tracing::error!(alert=true, %message, "standing_sweep_slot_failed");
                        if failure.is_none() { failure=Some(message); }
                    }
                }
            }
            if sweep_now(worker) >= deadline { break; }
        }
    }
    if let Some(failure)=failure { return Err(StoreError::Db(Box::new(RecoveryFailure::Slot(failure)))); }
    if positions().lock().expect("standing sweep position poisoned")[&key].remaining_accounts > 0 {
        return Ok(Tick::until_next_work(processed, SCAN_CONTINUATION));
    }
    Ok(Tick::worked(processed))
}

async fn reconcile_graph(
    scope: &JobScope<'_>,
    control_pool: &PgPool,
    tenant: &TenantId,
    account: Uuid,
    graph: Uuid,
    profile: &str,
    cursor_key: &str,
) -> Result<usize, StoreError> {
    let started = tokio::time::Instant::now();
    let mut content = execution::begin_content(scope).await?;
    let mut control = execution::begin_control(tenant, control_pool, account).await?;
    execution::bound(&mut content, control.as_mut()).await?;
    let policy = if let Some(control) = control.as_mut() {
        execution::policy_locked(control, account).await?
    } else {
        execution::policy_locked(&mut content, account).await?
    };
    execution::share_run_gate(&mut content, graph).await?;
    let mut run: Option<Run> = sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE graph_id=$1 AND account_id=$2 AND extraction_version=$3 AND run_kind='standing' AND retired_at IS NULL FOR SHARE")
        .bind(graph).bind(account).bind(profile).fetch_optional(&mut *content).await?;
    if run.is_none()
        && policy.is_none()
        && corpus_enrichment::lifecycle_ready_tx(&mut content).await?
    {
        let principal = principal::select_for_graph_locked(
            &mut content,
            control.as_mut(),
            tenant,
            account,
            graph,
        )
        .await
        .map_err(|_| sqlx::Error::Protocol("standing initial principal unavailable".into()))?;
        if let Some(principal) = principal {
            let id: Option<Uuid> = sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,execution_principal,mode,run_kind,corpus_kind,status,standing_pause_state,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,$3,'execute','standing','real','running','none',$4,$5,1,1,1) ON CONFLICT(graph_id,extraction_version,corpus_kind) WHERE run_kind='standing' AND retired_at IS NULL DO NOTHING RETURNING run_id")
                .bind(graph).bind(account).bind(principal).bind(profile).bind(crate::llm::model_config::extract_model())
                .fetch_optional(&mut *content).await?;
            if let Some(id) = id {
                run = sqlx::query_as(
                    "SELECT * FROM corpus_enrichment_runs WHERE run_id=$1 FOR UPDATE",
                )
                .bind(id)
                .fetch_optional(&mut *content)
                .await?;
            }
        } else {
            tracing::warn!(alert=true, %account, %graph, profile, "managed_principal_unavailable");
        }
    }
    let Some(run) = run else {
        return Ok(0);
    };
    if policy == Some(HoldReason::ProcessingDisabled) {
        super::change_pause_tx(&mut content, graph, run.run_id, PauseAction::ProcessingOff).await?;
    } else {
        super::change_pause_tx(&mut content, graph, run.run_id, PauseAction::ProcessingOn).await?;
    }
    let model: String =
        sqlx::query_scalar("SELECT model FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run.run_id)
            .fetch_one(&mut *content)
            .await?;
    if model != crate::llm::model_config::extract_model() {
        tracing::warn!(alert=true, %account, %graph, run=%run.run_id, "managed_model_drift");
    }
    let running: bool = sqlx::query_scalar("SELECT status='running' AND standing_pause_state='none' FROM corpus_enrichment_runs WHERE run_id=$1").bind(run.run_id).fetch_one(&mut *content).await?;
    if policy.is_some() || !running {
        let reason = policy.unwrap_or(HoldReason::Operator).code();
        let versions: Vec<Uuid> = sqlx::query_scalar("SELECT holding_id FROM corpus_enrichment_units WHERE run_id=$1 AND execution_owner AND state IN ('pending','queued') ORDER BY unit_id LIMIT 200")
            .bind(run.run_id).fetch_all(&mut *content).await?;
        for version in versions {
            super::hold_unit_tx(&mut content, graph, version, profile, reason).await?;
        }
        if let Some(control) = control {
            control.rollback().await?;
        }
        content.commit().await?;
        return Ok(0);
    }
    // Under v2, blocked v1-pinned captures are candidates too, and under v3
    // blocked v1- and v2-pinned ones: adoption repins each forward before it
    // takes ownership (`v2::repin_tx`).
    let pins: Vec<&str> = std::iter::once(profile).chain(crate::workers::source_atoms::v2::repinnable_into(profile).iter().copied()).collect();
    let candidates = format!(
        "SELECT v.version_id,v.captured_at FROM source_versions v JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id \
          WHERE v.graph_id=$1 AND v.extraction_version=ANY($2) AND {STANDING_ELIGIBLE} AND v.extraction_status='blocked' AND v.extraction_error IN {ADOPTABLE_ERRORS} \
            AND s.access_state='permitted' AND NOT COALESCE(v.origin->>'transport'='synthetic',false) AND NOT COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false) \
            AND NOT EXISTS(SELECT 1 FROM corpus_enrichment_units u WHERE u.graph_id=v.graph_id AND u.holding_id=v.version_id AND u.extraction_version=$3 AND (u.execution_owner OR u.state='failed'))");
    // Two cursor-paged lanes, fresh captures first: a backlog of earlier
    // candidates cannot hold a new capture back until its cursor wraps, and
    // each cursor steps past versions that keep failing.
    let mut lanes = Vec::new();
    for (lane, fresh, page) in [("fresh", Some(crate::workers::source_atoms::FRESH_CAPTURE_WINDOW.as_secs_f64()), ADOPT_BATCH as i64), ("backlog", None, ADOPT_PAGE)] {
        let cursor_id = (cursor_key.to_owned(), graph, format!("{profile}:{lane}"));
        let cursor = adopt_cursors().lock().expect("standing adopt cursor poisoned").get(&cursor_id).copied();
        let versions: Vec<(Uuid, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(&format!(
            "{candidates} AND ($7::float8 IS NULL OR v.captured_at>=now()-make_interval(secs=>$7)) AND ($4::timestamptz IS NULL OR (v.captured_at,v.version_id)>($4,$5)) ORDER BY v.captured_at,v.version_id LIMIT $6"))
            .bind(graph).bind(&pins).bind(profile).bind(cursor.map(|c| c.0)).bind(cursor.map(|c| c.1)).bind(page).bind(fresh)
            .fetch_all(&mut *content).await?;
        lanes.push((cursor_id, versions, page));
    }
    if let Some(control) = control {
        control.rollback().await?;
    }
    content.commit().await?;
    let mut count = 0;
    let mut failure=None;
    let mut seen = std::collections::HashSet::new();
    for (cursor_id, versions, page) in lanes {
        let mut attempted = 0;
        for batch in versions.chunks(ADOPT_BATCH) {
            if started.elapsed() >= ADOPT_BUDGET { break; }
            let ids: Vec<Uuid> = batch.iter().map(|(id, _)| *id).filter(|id| seen.insert(*id)).collect();
            if !ids.is_empty() {
                match adopt_batch(scope,control_pool,tenant,account,graph,profile,run.run_id,&ids).await {
                    Ok(adopted)=>count+=adopted,
                    Err(error)=>{
                        tracing::error!(alert=true,%account,%graph,versions=ids.len(),profile,error=%error,"standing_adoption_failed");
                        if failure.is_none() { failure=Some(error); }
                    }
                }
            }
            attempted += batch.len();
        }
        let mut cursors = adopt_cursors().lock().expect("standing adopt cursor poisoned");
        if attempted == versions.len() && versions.len() < page as usize {
            cursors.remove(&cursor_id);
        } else if let Some((id, at)) = versions.get(attempted.saturating_sub(1)).filter(|_| attempted > 0) {
            cursors.insert(cursor_id, (*at, *id));
        }
    }
    count += requeue_run(scope, control_pool, tenant, account, run.run_id, started + REQUEUE_BUDGET).await?;
    if let Some(failure)=failure { return Err(failure); }
    Ok(count)
}

/// Adopt up to [`ADOPT_BATCH`] versions in one transaction pair. The run,
/// policy, lifecycle and principal are checked once per batch, under the same
/// locks per-version adoption used to take for each version. Each version runs in its own
/// savepoint, so one bad version is skipped instead of aborting its batch.
#[allow(clippy::too_many_arguments)]
async fn adopt_batch(
    scope: &JobScope<'_>,
    control_pool: &PgPool,
    tenant: &TenantId,
    account: Uuid,
    graph: Uuid,
    profile: &str,
    run_id: Uuid,
    versions: &[Uuid],
) -> Result<usize, StoreError> {
    let mut content = execution::begin_content(scope).await?;
    let mut control = execution::begin_control(tenant, control_pool, account).await?;
    execution::bound(&mut content, control.as_mut()).await?;
    execution::share_run_gate(&mut content, graph).await?;
    let run: Option<Run> = sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE run_id=$1 AND graph_id=$2 AND account_id=$3 AND extraction_version=$4 AND run_kind='standing' AND retired_at IS NULL AND status='running' AND standing_pause_state='none' FOR SHARE")
        .bind(run_id).bind(graph).bind(account).bind(profile).fetch_optional(&mut *content).await?;
    let Some(run) = run else {
        return Ok(0);
    };
    let policy = if let Some(control) = control.as_mut() {
        execution::policy_locked(control, account).await?
    } else {
        execution::policy_locked(&mut content, account).await?
    };
    if policy.is_some() || !corpus_enrichment::lifecycle_ready_tx(&mut content).await? {
        return Ok(0);
    }
    let current: Uuid = sqlx::query_scalar(
        "SELECT execution_principal FROM corpus_enrichment_runs WHERE run_id=$1",
    )
    .bind(run_id)
    .fetch_one(&mut *content)
    .await?;
    let selected = principal::adoption_principal_locked(
        &mut content,
        control.as_mut(),
        tenant,
        &super::PrincipalRequest {
            account_id: account,
            graph_id: graph,
            run_id,
            profile: super::StandingProfile::parse(profile)
                .map_err(|_| sqlx::Error::Protocol("unsupported standing profile".into()))?,
            expected_principal: current,
        },
    )
    .await
    .map_err(|_| sqlx::Error::Protocol("standing adoption principal unavailable".into()))?;
    if selected.is_none() {
        return Ok(0);
    }
    let mut adopted = 0;
    for version in versions {
        let mut savepoint = sqlx::Connection::begin(&mut *content).await?;
        match adopt_version_tx(&mut savepoint, &run, graph, profile, *version).await {
            Ok(n) => {
                savepoint.commit().await?;
                adopted += n;
            }
            Err(error) => {
                savepoint.rollback().await?;
                tracing::error!(alert=true,%account,%graph,%version,profile,error=%error,"standing_adoption_failed");
            }
        }
    }
    if let Some(control) = control {
        control.rollback().await?;
    }
    content.commit().await?;
    Ok(adopted)
}

/// One version's adoption inside the batch transaction: version lock, parent
/// lock, v1 -> v2 repin when adopting under v2, eligibility, owner unit,
/// queue, receipt clock.
async fn adopt_version_tx(
    content: &mut Transaction<'_, Postgres>,
    run: &Run,
    graph: Uuid,
    profile: &str,
    version: Uuid,
) -> Result<usize, StoreError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('corpus-version:' || $1::text))")
        .bind(version.to_string())
        .execute(&mut **content)
        .await?;
    let parent: Option<Uuid> = sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND access_state='permitted' AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE")
            .bind(graph).bind(version).fetch_optional(&mut **content).await?;
    let Some(parent) = parent else {
        return Ok(0);
    };
    #[cfg(feature="test-support")]
    super::probe::checkpoint(version, super::probe::Stage::BeforeAdoptionRepin).await;
    let earlier = crate::workers::source_atoms::v2::repinnable_into(profile);
    if !earlier.is_empty() {
        let pin: Option<String> = sqlx::query_scalar("SELECT extraction_version FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE")
            .bind(graph).bind(version).fetch_optional(&mut **content).await?;
        if let Some(from) = pin.as_deref().filter(|p| earlier.contains(p)) {
            // Only a still-blocked version: never a done one (re-extraction
            // is the explicit operator request alone).
            if !crate::workers::source_atoms::v2::repin_blocked_tx(content, graph, version, from, profile, "managed_admission_required").await? {
                return Ok(0);
            }
        }
    }
    let valid: bool = sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM source_versions v WHERE v.graph_id=$1 AND v.version_id=$2 AND v.extraction_version=$3 AND v.extraction_status='blocked' AND {STANDING_ELIGIBLE} AND NOT COALESCE(v.origin->>'transport'='synthetic',false) AND NOT COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false))"))
            .bind(graph).bind(version).bind(profile).fetch_one(&mut **content).await?;
    if !valid {
        return Ok(0);
    }
    let unit = corpus_enrichment::insert_unit_tx(
        content,
        run,
        "retained_version",
        version,
        Some(parent),
        Some(profile),
        "pending",
        None,
        "available",
        "retained_version",
    )
    .await?;
    corpus_enrichment::admit_tx(content, run, unit, version).await?;
    sqlx::query("SELECT corpus_standing_receipt_event($1,$2,'adopt',NULL)")
        .bind(graph)
        .bind(version)
        .execute(&mut **content)
        .await?;
    Ok(1)
}

type RequeueCursor = (chrono::DateTime<chrono::Utc>, Uuid);

/// Requeue `run`'s held and undelivered units, one committed transaction per
/// [`REQUEUE_BATCH`], starting batches until `deadline` and always at least
/// one. A recovery set larger than one slot then drains across sweeps; as a
/// single transaction it rolled back at the slot ceiling on every sweep and
/// never drained at all.
async fn requeue_run(
    scope: &JobScope<'_>,
    control_pool: &PgPool,
    tenant: &TenantId,
    account: Uuid,
    run_id: Uuid,
    deadline: tokio::time::Instant,
) -> Result<usize, StoreError> {
    let mut count = 0;
    let mut after = None;
    loop {
        let mut content = execution::begin_content(scope).await?;
        let mut control = execution::begin_control(tenant, control_pool, account).await?;
        execution::bound(&mut content, control.as_mut()).await?;
        // The run row is share-locked like every standing fence, so two
        // sweeps (two machines during a deploy) would no longer exclude each
        // other through it. This transaction lock keeps one requeue per run.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('standing-requeue:'||$1::text,0))")
            .bind(run_id).execute(&mut *content).await?;
        // The gate is per graph and precedes the row lock (`share_run_gate`).
        let graph: Option<Uuid> = sqlx::query_scalar("SELECT graph_id FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run_id).fetch_optional(&mut *content).await?;
        if let Some(graph) = graph {
            execution::share_run_gate(&mut content, graph).await?;
        }
        let run: Option<Run> = sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE run_id=$1 AND run_kind='standing' AND status='running' AND standing_pause_state='none' FOR SHARE")
            .bind(run_id).fetch_optional(&mut *content).await?;
        let policy = if let Some(control) = control.as_mut() {
            execution::policy_locked(control, account).await?
        } else {
            execution::policy_locked(&mut content, account).await?
        };
        let batch = match (run, policy) {
            (Some(run), None) => Some(requeue_units(&mut content, &run, after).await?),
            _ => None,
        };
        if let Some(control) = control {
            control.rollback().await?;
        }
        content.commit().await?;
        let Some((requeued, next)) = batch else { break };
        count += requeued;
        if next.is_none() || tokio::time::Instant::now() >= deadline { break; }
        after = next;
    }
    Ok(count)
}

/// One batch of [`requeue_run`]. The keyset cursor steps past units this
/// batch leaves pending (source removed, version finished), so they cannot
/// fill every later batch. Returns the cursor when the page was full.
async fn requeue_units(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    after: Option<RequeueCursor>,
) -> Result<(usize, Option<RequeueCursor>), StoreError> {
    if after.is_none() {
        super::release_undelivered_run_tx(tx, run.run_id).await?;
    }
    let units: Vec<(Uuid, Uuid, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as("SELECT unit_id,holding_id,reason,updated_at FROM corpus_enrichment_units WHERE run_id=$1 AND execution_owner AND state='pending' AND reason IS NOT NULL AND reason<>'source_removed_or_access_lost' AND (retry_after IS NULL OR retry_after<=now()) AND ($2::timestamptz IS NULL OR (updated_at,unit_id)>($2,$3)) ORDER BY updated_at,unit_id LIMIT $4")
        .bind(run.run_id).bind(after.map(|c| c.0)).bind(after.map(|c| c.1)).bind(REQUEUE_BATCH).fetch_all(&mut **tx).await?;
    let next = (units.len() as i64 == REQUEUE_BATCH).then(|| units.last().map(|u| (u.3, u.0))).flatten();
    let mut count = 0;
    for (unit, version, stored_reason, _) in units {
        let reason = HoldReason::parse(&stored_reason).ok_or_else(|| StoreError::Db(Box::new(
            RecoveryFailure::UnknownHoldReason { unit, reason: stored_reason.chars().take(128).collect() }
        )))?;
        if reason == HoldReason::SourceUnavailable {
            continue;
        }
        let eligible: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM source_versions v JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id WHERE v.graph_id=$1 AND v.version_id=$2 AND v.extraction_status NOT IN ('done','failed') AND s.access_state='permitted')")
            .bind(run.graph_id).bind(version).fetch_one(&mut **tx).await?;
        if !eligible {
            continue;
        }
        corpus_enrichment::queue_unit_tx(tx, run, version).await?;
        sqlx::query("UPDATE corpus_enrichment_units SET state='queued',reason=NULL,retry_after=NULL,updated_at=now() WHERE run_id=$1 AND holding_id=$2 AND execution_owner AND state='pending'")
            .bind(run.run_id).bind(version).execute(&mut **tx).await?;
        sqlx::query("SELECT corpus_standing_receipt_event($1,$2,'requeue',$3)")
            .bind(run.graph_id)
            .bind(version)
            .bind(reason.code())
            .execute(&mut **tx)
            .await?;
        count += 1;
    }
    Ok((count, next))
}

/// Slot 1: requeue held units of every source-profile run that no longer
/// adopts: retired runs, and the live run of the profile the operator switched
/// away from. Its adopted versions finish under the profile they were pinned
/// to; it adopts nothing new.
async fn drain_source_profiles(
    scope: &JobScope<'_>,
    control_pool: &PgPool,
    tenant: &TenantId,
    account: Uuid,
    graph: Uuid,
    active: &str,
    deadline: tokio::time::Instant,
) -> Result<usize, StoreError> {
    let mut count = 0;
    for profile in [crate::workers::source_atoms::EXTRACTION_VERSION_V2, crate::workers::source_atoms::EXTRACTION_VERSION_V3] {
        count += requeue_retired(scope, control_pool, tenant, account, graph, profile, deadline).await?;
        if profile != active {
            count += requeue_inactive_live(scope, control_pool, tenant, account, graph, profile, deadline).await?;
        }
    }
    Ok(count)
}

async fn requeue_inactive_live(
    scope: &JobScope<'_>,
    control_pool: &PgPool,
    tenant: &TenantId,
    account: Uuid,
    graph: Uuid,
    profile: &str,
    deadline: tokio::time::Instant,
) -> Result<usize, StoreError> {
    let mut content = execution::begin_content(scope).await?;
    let mut control = execution::begin_control(tenant, control_pool, account).await?;
    execution::bound(&mut content, control.as_mut()).await?;
    let policy = if let Some(control) = control.as_mut() {
        execution::policy_locked(control, account).await?
    } else {
        execution::policy_locked(&mut content, account).await?
    };
    execution::share_run_gate(&mut content, graph).await?;
    let run: Option<Run> = sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE graph_id=$1 AND account_id=$2 AND extraction_version=$3 AND run_kind='standing' AND retired_at IS NULL FOR SHARE")
        .bind(graph).bind(account).bind(profile).fetch_optional(&mut *content).await?;
    let mut requeue = None;
    if let Some(run) = run {
        super::change_pause_tx(
            &mut content,
            graph,
            run.run_id,
            if policy == Some(HoldReason::ProcessingDisabled) { PauseAction::ProcessingOff } else { PauseAction::ProcessingOn },
        )
        .await?;
        let running: bool = sqlx::query_scalar("SELECT status='running' AND standing_pause_state='none' FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run.run_id).fetch_one(&mut *content).await?;
        if policy.is_none() && running {
            requeue = Some(run.run_id);
        }
    }
    if let Some(control) = control {
        control.rollback().await?;
    }
    content.commit().await?;
    match requeue {
        Some(run) => requeue_run(scope, control_pool, tenant, account, run, deadline).await,
        None => Ok(0),
    }
}

async fn requeue_retired(
    scope: &JobScope<'_>,
    control_pool: &PgPool,
    tenant: &TenantId,
    account: Uuid,
    graph: Uuid,
    profile: &str,
    deadline: tokio::time::Instant,
) -> Result<usize, StoreError> {
    let mut content = execution::begin_content(scope).await?;
    let mut control = execution::begin_control(tenant, control_pool, account).await?;
    execution::bound(&mut content, control.as_mut()).await?;
    let policy = if let Some(control) = control.as_mut() {
        execution::policy_locked(control, account).await?
    } else {
        execution::policy_locked(&mut content, account).await?
    };
    execution::share_run_gate(&mut content, graph).await?;
    let runs: Vec<Run> = sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE graph_id=$1 AND account_id=$2 AND extraction_version=$3 AND run_kind='standing' AND retired_at IS NOT NULL AND status IN ('running','paused') ORDER BY run_id FOR SHARE")
        .bind(graph).bind(account).bind(profile).fetch_all(&mut *content).await?;
    let mut requeue = Vec::new();
    for run in runs {
        super::change_pause_tx(
            &mut content,
            graph,
            run.run_id,
            if policy == Some(HoldReason::ProcessingDisabled) {
                PauseAction::ProcessingOff
            } else {
                PauseAction::ProcessingOn
            },
        )
        .await?;
        let running: bool = sqlx::query_scalar("SELECT status='running' AND standing_pause_state='none' FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run.run_id).fetch_one(&mut *content).await?;
        if policy.is_none() && running {
            requeue.push(run.run_id);
        }
    }
    if let Some(control) = control {
        control.rollback().await?;
    }
    content.commit().await?;
    let mut count = 0;
    for run in requeue {
        count += requeue_run(scope, control_pool, tenant, account, run, deadline).await?;
    }
    Ok(count)
}
