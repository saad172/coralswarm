use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use tokio::time::{timeout, timeout_at, Instant};
use uuid::Uuid;

use crate::{tenancy::TenantId, workers::tenant_harness::WorkerTenant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StandingProfile { SourceAtomsV2, SourceAtomsV3, MeetingAtoms }

impl StandingProfile {
    pub(crate) fn parse(value: &str) -> Result<Self, PrincipalError> {
        match value {
            "source-atoms-v2" => Ok(Self::SourceAtomsV2),
            "source-atoms-v3" => Ok(Self::SourceAtomsV3),
            "meeting-atoms-v1" => Ok(Self::MeetingAtoms),
            _ => Err(PrincipalError::InvalidBinding(InvalidPrincipalBinding::Profile)),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SourceAtomsV2 => "source-atoms-v2",
            Self::SourceAtomsV3 => "source-atoms-v3",
            Self::MeetingAtoms => "meeting-atoms-v1",
        }
    }

    /// The standing job kind that executes this profile.
    pub(crate) fn job_kind(self) -> &'static str {
        match self {
            Self::SourceAtomsV2 | Self::SourceAtomsV3 => "ExtractStandingSourceAtoms",
            Self::MeetingAtoms => "ExtractStandingMeetingAtoms",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PrincipalRequest {
    pub account_id: Uuid,
    pub graph_id: Uuid,
    pub run_id: Uuid,
    pub profile: StandingProfile,
    pub expected_principal: Uuid,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PrincipalDisposition {
    Unchanged { principal: Uuid },
    Changed { from: Uuid, to: Uuid },
    Superseded { current: Uuid },
    NoEligible(NoEligibleReason),
    Denied(PrincipalDenial),
    Skipped(RunIneligible),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NoEligibleReason {
    OwnerUserMissing, OwnerUserDeleted, OwnerNotAccountMember, OwnerCannotReach,
    NoCurrentPrivilegedMember, NoReachablePrivilegedMember,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PrincipalDenial { AccountPlaneMismatch, OrgRevoked, GraphArchived }
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RunIneligible { Cohort, Retired }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureStage { AcquireContent, AcquireControl, Scope, RunLock, ControlRead, Reachability, Update, Rollback, Commit }
#[derive(Debug)]
pub(crate) enum PrincipalError {
    MissingData(MissingPrincipalData),
    InvalidBinding(InvalidPrincipalBinding),
    LookupIncomplete,
    ObservationChanged,
    Unavailable { stage: FailureStage, cause: crate::store::StoreError },
    Deadline { stage: FailureStage },
    CommitOutcomeUnknown,
}

#[derive(Debug)]
pub(crate) enum MissingPrincipalData { Account, Run, Graph }

#[derive(Debug)]
pub(crate) enum InvalidPrincipalBinding {
    Profile, RunAccount, GraphRun, StandingRunShape, StandingPrincipal, GraphAccount, GraphOwner,
}

pub(crate) struct StandingOwner<'a> {
    worker: &'a WorkerTenant,
    control: &'a PgPool,
}

const ACQUIRE_LIMIT: Duration = Duration::from_millis(250);
const OPERATION_LIMIT: Duration = Duration::from_secs(2);
const CANDIDATE_LIMIT: usize = 128;

impl<'a> StandingOwner<'a> {
    pub(crate) fn for_worker(worker: &'a WorkerTenant, control: &'a PgPool) -> Self {
        Self { worker, control }
    }

    pub(crate) async fn reconcile(&self, request: PrincipalRequest) -> Result<PrincipalDisposition, PrincipalError> {
        let deadline = Instant::now() + OPERATION_LIMIT;
        let content_begin = async {
            match self.worker.tenant {
                TenantId::Consumer => crate::tenancy::scope::begin_account_scoped(&self.worker.pool, request.account_id).await,
                TenantId::Org(_) => crate::tenancy::scope::begin_tenant_system_scoped(&self.worker.pool, request.account_id).await,
            }
        };
        let mut content = timeout(ACQUIRE_LIMIT, content_begin).await
            .map_err(|_| PrincipalError::Deadline { stage: FailureStage::AcquireContent })?
            .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::AcquireContent, cause: cause.into() })?;
        let mut control = match &self.worker.tenant {
            TenantId::Consumer => None,
            TenantId::Org(_) => Some(timeout(ACQUIRE_LIMIT, crate::tenancy::scope::begin_account_scoped(self.control, request.account_id)).await
                .map_err(|_| PrincipalError::Deadline { stage: FailureStage::AcquireControl })?
                .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::AcquireControl, cause: cause.into() })?),
        };
        let mut stage = FailureStage::Scope;
        let observed = timeout_at(deadline - ACQUIRE_LIMIT, async {
            bound_statements(&mut content).await?;
            if let Some(tx) = control.as_mut() { bound_statements(tx).await?; }
            reconcile_locked(&mut content, control.as_mut(), &self.worker.tenant, &request, &mut stage, PrincipalMode::Metadata).await
        }).await.map_err(|_| PrincipalError::Deadline { stage }).and_then(|r| r);
        if let Some(tx) = control {
            timeout_at(deadline, tx.rollback()).await
                .map_err(|_| PrincipalError::Deadline { stage: FailureStage::Rollback })?
                .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::Rollback, cause: cause.into() })?;
        }
        let disposition = match observed {
            Ok(value) => value,
            Err(error) => {
                timeout(ACQUIRE_LIMIT, content.rollback()).await
                    .map_err(|_| PrincipalError::Deadline { stage: FailureStage::Rollback })?
                    .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::Rollback, cause: cause.into() })?;
                return Err(error);
            }
        };
        if !matches!(disposition, PrincipalDisposition::Changed { .. }) {
            timeout_at(deadline, content.rollback()).await
                .map_err(|_| PrincipalError::Deadline { stage: FailureStage::Rollback })?
                .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::Rollback, cause: cause.into() })?;
            return Ok(disposition);
        }
        match timeout_at(deadline, content.commit()).await {
            Ok(Ok(())) => {},
            Ok(Err(sqlx::Error::Database(cause))) if cause.code().is_some_and(|code| code.starts_with("23") || code == "40001" || code == "40P01") => {
                return Err(PrincipalError::Unavailable {
                    stage: FailureStage::Commit, cause: sqlx::Error::Database(cause).into(),
                });
            }
            _ => return Err(PrincipalError::CommitOutcomeUnknown),
        }
        if let PrincipalDisposition::Changed { from, to } = &disposition {
            tracing::warn!(alert = true, tenant = ?self.worker.tenant, account = %request.account_id,
                graph = %request.graph_id, run = %request.run_id, profile = request.profile.as_str(),
                old_principal = %from, new_principal = %to, "managed_principal_changed");
        }
        Ok(disposition)
    }
}

async fn bound_statements(tx: &mut Transaction<'_, Postgres>) -> Result<(), PrincipalError> {
    sqlx::query("SELECT set_config('lock_timeout','250ms',true),set_config('statement_timeout','500ms',true)")
        .execute(&mut **tx).await
        .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::Scope, cause: cause.into() })?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct Run {
    account_id: Uuid, graph_id: Uuid, extraction_version: String, run_kind: String,
    execution_principal: Option<Uuid>, retired_at: Option<DateTime<Utc>>, mode: String,
    corpus_kind: String, status: String,
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
struct Graph { account_id: Uuid, kind: String, owner_user_id: Option<Uuid>, archived_at: Option<DateTime<Utc>> }

#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
struct Subject { user_id: Uuid, deleted: bool, role: Option<String>, joined_at: Option<DateTime<Utc>> }

#[derive(serde::Deserialize, Debug, PartialEq, Eq)]
struct ControlObservation { revoked: bool, subjects: Vec<Subject>, admins: Vec<Subject> }

impl Subject {
    fn eligible(&self, member: bool) -> bool {
        !self.deleted && if member { self.role.is_some() } else { matches!(self.role.as_deref(), Some("owner" | "admin")) }
    }
}

async fn graph(connection: &mut PgConnection, graph_id: Uuid) -> Result<Graph, PrincipalError> {
    sqlx::query_as("SELECT account_id,kind,owner_user_id,archived_at FROM graphs WHERE id=$1")
        .bind(graph_id).fetch_optional(connection).await
        .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::Reachability, cause: cause.into() })?
        .ok_or(PrincipalError::MissingData(MissingPrincipalData::Graph))
}

async fn control_observation(connection: &mut PgConnection, request: &PrincipalRequest, owner: Option<Uuid>) -> Result<ControlObservation, PrincipalError> {
    let facts: Option<sqlx::types::Json<ControlObservation>> = sqlx::query_scalar(
        "WITH members AS MATERIALIZED (
           SELECT u.id AS user_id,u.deleted_at IS NOT NULL AS deleted,m.role,m.joined_at
           FROM users u LEFT JOIN account_members m ON m.user_id=u.id AND m.account_id=$1
           WHERE u.id=$2 OR u.id=$3 OR (m.role IN ('owner','admin') AND u.deleted_at IS NULL)
         ), admins AS MATERIALIZED (
           SELECT * FROM members WHERE role IN ('owner','admin') AND NOT deleted
           ORDER BY joined_at,user_id LIMIT $4
         )
         SELECT jsonb_build_object(
           'revoked',EXISTS(SELECT 1 FROM org_revocations r WHERE r.clerk_org_id=a.clerk_org_id),
           'subjects',COALESCE((SELECT jsonb_agg(to_jsonb(m) ORDER BY user_id) FROM members m WHERE user_id=$2 OR user_id=$3),'[]'::jsonb),
           'admins',COALESCE((SELECT jsonb_agg(to_jsonb(m) ORDER BY joined_at,user_id) FROM admins m),'[]'::jsonb))
         FROM accounts a WHERE a.id=$1")
        .bind(request.account_id).bind(request.expected_principal).bind(owner)
        .bind((CANDIDATE_LIMIT + 1) as i64).fetch_optional(connection).await
        .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::ControlRead, cause: cause.into() })?;
    facts.map(|facts| facts.0).ok_or(PrincipalError::MissingData(MissingPrincipalData::Account))
}

fn binding_denial(observed: &ControlObservation) -> Option<PrincipalDenial> {
    observed.revoked.then_some(PrincipalDenial::OrgRevoked)
}

/// The account must resolve (`managed::plane`) to the plane this standing
/// work runs on. A personal org resolves to the consumer plane (COR-114);
/// an unresolvable account is a mismatch, never a guess.
async fn plane_denial(connection: &mut PgConnection, account: Uuid, tenant: &TenantId) -> Result<Option<PrincipalDenial>, PrincipalError> {
    use crate::managed::plane::{resolve_liability_plane, PlaneError};
    match resolve_liability_plane(connection, account, false).await {
        Ok(plane) if plane.tenant() == *tenant => Ok(None),
        Ok(_) | Err(PlaneError::Unresolvable(_)) => Ok(Some(PrincipalDenial::AccountPlaneMismatch)),
        Err(PlaneError::UnknownAccount) => Err(PrincipalError::MissingData(MissingPrincipalData::Account)),
        Err(PlaneError::Db(cause)) => Err(PrincipalError::Unavailable { stage: FailureStage::ControlRead, cause: cause.into() }),
    }
}

/// `Metadata` is the standalone reconcile, which owns its transaction and
/// takes the run row `FOR UPDATE`. `Adoption` and `OwnedExecution` run inside
/// a standing fence that already holds the row `FOR SHARE`: re-locking it
/// `FOR UPDATE` there would make every fence of the graph exclusive again. A
/// principal change under a share lock still writes the row, so it waits for
/// the other open fences, as a pause does.
#[derive(Clone, Copy)]
enum PrincipalMode { Metadata, Adoption, OwnedExecution { unit: Uuid, version: Uuid } }

async fn reconcile_locked(content: &mut Transaction<'_, Postgres>, mut control: Option<&mut Transaction<'_, Postgres>>, tenant: &TenantId, request: &PrincipalRequest, stage: &mut FailureStage, mode: PrincipalMode) -> Result<PrincipalDisposition, PrincipalError> {
    *stage = FailureStage::RunLock;
    let lock = if matches!(mode, PrincipalMode::Metadata) { "FOR UPDATE" } else { "FOR SHARE" };
    let gate = if matches!(mode, PrincipalMode::Metadata) {
        super::execution::exclusive_run_gate(content, request.graph_id).await
    } else {
        super::execution::share_run_gate(content, request.graph_id).await
    };
    gate.map_err(|cause| PrincipalError::Unavailable { stage: *stage, cause: cause.into() })?;
    let run: Run = sqlx::query_as(&format!("SELECT account_id,graph_id,extraction_version,run_kind,execution_principal,retired_at,mode,corpus_kind,status FROM corpus_enrichment_runs WHERE run_id=$1 {lock}"))
        .bind(request.run_id).fetch_optional(&mut **content).await
        .map_err(|cause| PrincipalError::Unavailable { stage: *stage, cause: cause.into() })?
        .ok_or(PrincipalError::MissingData(MissingPrincipalData::Run))?;
    if run.account_id != request.account_id { return Err(PrincipalError::InvalidBinding(InvalidPrincipalBinding::RunAccount)); }
    if run.graph_id != request.graph_id { return Err(PrincipalError::InvalidBinding(InvalidPrincipalBinding::GraphRun)); }
    if run.extraction_version != request.profile.as_str() { return Err(PrincipalError::InvalidBinding(InvalidPrincipalBinding::Profile)); }
    if run.run_kind != "standing" { return Ok(PrincipalDisposition::Skipped(RunIneligible::Cohort)); }
    let owned_execution = match mode {
        PrincipalMode::Metadata | PrincipalMode::Adoption => false,
        PrincipalMode::OwnedExecution { unit, version } => sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE unit_id=$1 AND run_id=$2 AND graph_id=$3 AND holding_id=$4 AND kind='retained_version' AND execution_owner AND extraction_version=$5)")
            .bind(unit).bind(request.run_id).bind(request.graph_id).bind(version).bind(request.profile.as_str())
            .fetch_one(&mut **content).await.map_err(|cause| PrincipalError::Unavailable { stage: *stage, cause: cause.into() })?,
    };
    if matches!(mode, PrincipalMode::OwnedExecution { .. }) && !owned_execution { return Err(PrincipalError::ObservationChanged); }
    if run.retired_at.is_some() && !owned_execution { return Ok(PrincipalDisposition::Skipped(RunIneligible::Retired)); }
    if run.mode != "execute" || run.corpus_kind != "real" || !matches!(run.status.as_str(), "running" | "paused") {
        return Err(PrincipalError::InvalidBinding(InvalidPrincipalBinding::StandingRunShape));
    }
    let current = run.execution_principal.ok_or(PrincipalError::InvalidBinding(InvalidPrincipalBinding::StandingPrincipal))?;
    if current != request.expected_principal { return Ok(PrincipalDisposition::Superseded { current }); }
    for _ in 0..2 {
        *stage = FailureStage::Reachability;
        let shape = graph(content, request.graph_id).await?;
        if shape.account_id != request.account_id { return Err(PrincipalError::InvalidBinding(InvalidPrincipalBinding::GraphAccount)); }
        if shape.archived_at.is_some() { return Ok(PrincipalDisposition::Denied(PrincipalDenial::GraphArchived)); }
        if shape.kind == "member" && shape.owner_user_id.is_none() { return Err(PrincipalError::InvalidBinding(InvalidPrincipalBinding::GraphOwner)); }
        *stage = FailureStage::ControlRead;
        let decision = if let Some(control) = control.as_mut() {
            let facts = control_observation(control, request, shape.owner_user_id).await?;
            if let Some(reason) = binding_denial(&facts) { return Ok(PrincipalDisposition::Denied(reason)); }
            if let Some(reason) = plane_denial(control, request.account_id, tenant).await? { return Ok(PrincipalDisposition::Denied(reason)); }
            *stage = FailureStage::Reachability;
            let ids: Vec<Uuid> = facts.subjects.iter().chain(facts.admins.iter().take(CANDIDATE_LIMIT)).map(|s| s.user_id).collect();
            let local: Option<(Uuid, String, Option<Uuid>, Option<DateTime<Utc>>, Vec<Uuid>)> = sqlx::query_as(
                "SELECT g.account_id,g.kind,g.owner_user_id,g.archived_at,ARRAY(SELECT id FROM unnest($3::uuid[]) id WHERE resolve_tenant_graph_reachable_by_user(g.id,$2,id)) FROM graphs g WHERE g.id=$1")
                .bind(request.graph_id).bind(request.account_id).bind(&ids).fetch_optional(&mut **content).await
                .map_err(|cause| PrincipalError::Unavailable { stage: *stage, cause: cause.into() })?;
            let Some((account_id, kind, owner_user_id, archived_at, reachable)) = local else { return Err(PrincipalError::MissingData(MissingPrincipalData::Graph)); };
            if shape != (Graph { account_id, kind, owner_user_id, archived_at }) { continue; }
            *stage = FailureStage::ControlRead;
            if control_observation(control, request, shape.owner_user_id).await? != facts { continue; }
            select_org(&shape, &facts, &reachable, current)?
        } else {
            consumer_decision(content, request, &shape).await?
        };
        let principal = match decision {
            PrincipalDisposition::Changed { to, .. } => to,
            other => return Ok(other),
        };
        *stage = FailureStage::Update;
        // A writer of the run row: take the gate exclusively first, so open
        // fences drain and new ones queue behind this change.
        super::execution::exclusive_run_gate(content, request.graph_id).await
            .map_err(|cause| PrincipalError::Unavailable { stage: *stage, cause: cause.into() })?;
        let updated = sqlx::query("UPDATE corpus_enrichment_runs SET execution_principal=$6,updated_at=now() WHERE run_id=$1 AND graph_id=$2 AND account_id=$3 AND extraction_version=$4 AND execution_principal=$5 AND run_kind='standing' AND ($7 OR retired_at IS NULL) AND status IN ('running','paused')")
            .bind(request.run_id).bind(request.graph_id).bind(request.account_id).bind(request.profile.as_str()).bind(current).bind(principal).bind(owned_execution)
            .execute(&mut **content).await.map_err(|cause| PrincipalError::Unavailable { stage: *stage, cause: cause.into() })?;
        if updated.rows_affected() != 1 { return Err(PrincipalError::ObservationChanged); }
        return Ok(PrincipalDisposition::Changed { from: current, to: principal });
    }
    Err(PrincipalError::ObservationChanged)
}

fn select_org(shape: &Graph, facts: &ControlObservation, reachable: &[Uuid], current: Uuid) -> Result<PrincipalDisposition, PrincipalError> {
    let member = shape.kind == "member";
    if (!member || shape.owner_user_id == Some(current)) && facts.subjects.iter().any(|s| s.user_id == current && s.eligible(member)) && reachable.contains(&current) {
        return Ok(PrincipalDisposition::Unchanged { principal: current });
    }
    if member {
        let owner = shape.owner_user_id.ok_or(PrincipalError::InvalidBinding(InvalidPrincipalBinding::GraphOwner))?;
        let Some(subject) = facts.subjects.iter().find(|s| s.user_id == owner) else { return Ok(PrincipalDisposition::NoEligible(NoEligibleReason::OwnerUserMissing)); };
        let reason = if subject.deleted { Some(NoEligibleReason::OwnerUserDeleted) }
            else if subject.role.is_none() { Some(NoEligibleReason::OwnerNotAccountMember) }
            else if !reachable.contains(&owner) { Some(NoEligibleReason::OwnerCannotReach) } else { None };
        return Ok(match reason { Some(reason) => PrincipalDisposition::NoEligible(reason), None => PrincipalDisposition::Changed { from: current, to: owner } });
    }
    if let Some(candidate) = facts.admins.iter().take(CANDIDATE_LIMIT).find(|s| reachable.contains(&s.user_id)) {
        return Ok(PrincipalDisposition::Changed { from: current, to: candidate.user_id });
    }
    if facts.admins.len() > CANDIDATE_LIMIT { return Err(PrincipalError::LookupIncomplete); }
    Ok(PrincipalDisposition::NoEligible(if facts.admins.is_empty() { NoEligibleReason::NoCurrentPrivilegedMember } else { NoEligibleReason::NoReachablePrivilegedMember }))
}

async fn consumer_decision(content: &mut PgConnection, request: &PrincipalRequest, shape: &Graph) -> Result<PrincipalDisposition, PrincipalError> {
    let row: Option<(Uuid, String, Option<Uuid>, Option<DateTime<Utc>>, bool, bool, Option<Uuid>, bool, bool, bool, bool)> = sqlx::query_as(
        "WITH bound AS MATERIALIZED (
           SELECT g.*,a.id IS NOT NULL AS account_exists,
             EXISTS(SELECT 1 FROM account_members m JOIN users u ON u.id=m.user_id AND u.deleted_at IS NULL
               WHERE m.account_id=$2 AND m.user_id=$3
                 AND (CASE WHEN g.kind='member' THEN g.owner_user_id=m.user_id ELSE m.role IN ('owner','admin') END)
                 AND resolve_graph_reachable_by_user(g.id,m.user_id)) AS incumbent
           FROM graphs g LEFT JOIN accounts a ON a.id=$2 WHERE g.id=$1
         )
         SELECT g.account_id,g.kind,g.owner_user_id,g.archived_at,g.account_exists,g.incumbent,
         CASE WHEN g.incumbent THEN NULL ELSE
           (SELECT m.user_id FROM account_members m JOIN users u ON u.id=m.user_id AND u.deleted_at IS NULL
            WHERE m.account_id=$2 AND (CASE WHEN g.kind='member' THEN g.owner_user_id=m.user_id ELSE m.role IN ('owner','admin') END)
              AND resolve_graph_reachable_by_user(g.id,m.user_id) ORDER BY m.joined_at,m.user_id LIMIT 1) END,
         EXISTS(SELECT 1 FROM users WHERE id=g.owner_user_id),
         EXISTS(SELECT 1 FROM users WHERE id=g.owner_user_id AND deleted_at IS NOT NULL),
         EXISTS(SELECT 1 FROM account_members WHERE account_id=$2 AND user_id=g.owner_user_id),
         EXISTS(SELECT 1 FROM account_members m JOIN users u ON u.id=m.user_id AND u.deleted_at IS NULL WHERE m.account_id=$2 AND m.role IN ('owner','admin'))
         FROM bound g")
        .bind(request.graph_id).bind(request.account_id).bind(request.expected_principal).fetch_optional(&mut *content).await
        .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::Reachability, cause: cause.into() })?;
    let Some((account_id,kind,owner_user_id,archived_at,account_exists,incumbent,selected,owner_exists,owner_deleted,owner_member,admins)) = row else { return Err(PrincipalError::MissingData(MissingPrincipalData::Graph)); };
    if shape != &(Graph { account_id,kind,owner_user_id,archived_at }) { return Err(PrincipalError::ObservationChanged); }
    if !account_exists { return Err(PrincipalError::MissingData(MissingPrincipalData::Account)); }
    if let Some(denial) = plane_denial(content, request.account_id, &TenantId::Consumer).await? { return Ok(PrincipalDisposition::Denied(denial)); }
    if incumbent { return Ok(PrincipalDisposition::Unchanged { principal: request.expected_principal }); }
    if let Some(to) = selected { return Ok(PrincipalDisposition::Changed { from: request.expected_principal, to }); }
    let reason = if shape.kind == "member" {
        if !owner_exists { NoEligibleReason::OwnerUserMissing } else if owner_deleted { NoEligibleReason::OwnerUserDeleted }
        else if !owner_member { NoEligibleReason::OwnerNotAccountMember } else { NoEligibleReason::OwnerCannotReach }
    } else if !admins { NoEligibleReason::NoCurrentPrivilegedMember } else { NoEligibleReason::NoReachablePrivilegedMember };
    Ok(PrincipalDisposition::NoEligible(reason))
}


pub(super) async fn select_for_graph_locked(
    content: &mut Transaction<'_, Postgres>, mut control: Option<&mut Transaction<'_, Postgres>>,
    tenant: &TenantId, account_id: Uuid, graph_id: Uuid,
) -> Result<Option<Uuid>, PrincipalError> {
    let shape = graph(content, graph_id).await?;
    if shape.account_id != account_id || shape.archived_at.is_some() { return Ok(None); }
    let request = PrincipalRequest { account_id, graph_id, run_id: Uuid::nil(), profile: StandingProfile::SourceAtomsV2,
        expected_principal: shape.owner_user_id.unwrap_or(Uuid::nil()) };
    let decision = if let Some(control) = control.as_mut() {
        let facts = control_observation(control, &request, shape.owner_user_id).await?;
        if binding_denial(&facts).is_some() || plane_denial(control, account_id, tenant).await?.is_some() { return Ok(None); }
        let ids: Vec<Uuid> = facts.subjects.iter().chain(facts.admins.iter().take(CANDIDATE_LIMIT)).map(|s| s.user_id).collect();
        let reachable: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM unnest($3::uuid[]) id WHERE resolve_tenant_graph_reachable_by_user($1,$2,id)")
            .bind(graph_id).bind(account_id).bind(ids).fetch_all(&mut **content).await
            .map_err(|cause| PrincipalError::Unavailable { stage: FailureStage::Reachability, cause: cause.into() })?;
        if graph(content, graph_id).await? != shape || control_observation(control, &request, shape.owner_user_id).await? != facts {
            return Err(PrincipalError::ObservationChanged);
        }
        select_org(&shape, &facts, &reachable, request.expected_principal)?
    } else { consumer_decision(content, &request, &shape).await? };
    Ok(match decision {
        PrincipalDisposition::Unchanged { principal } | PrincipalDisposition::Changed { to: principal, .. } => Some(principal),
        _ => None,
    })
}

pub(super) async fn execution_principal_locked(
    content: &mut Transaction<'_, Postgres>, control: Option<&mut Transaction<'_, Postgres>>,
    tenant: &TenantId, request: &PrincipalRequest, unit: Uuid, version: Uuid,
) -> Result<Option<Uuid>, PrincipalError> {
    let mut stage = FailureStage::RunLock;
    match reconcile_locked(content, control, tenant, request, &mut stage, PrincipalMode::OwnedExecution { unit, version }).await? {
        PrincipalDisposition::Unchanged { principal } | PrincipalDisposition::Changed { to: principal, .. } => Ok(Some(principal)),
        _ => Ok(None),
    }
}


pub(super) async fn adoption_principal_locked(
    content: &mut Transaction<'_, Postgres>, control: Option<&mut Transaction<'_, Postgres>>,
    tenant: &TenantId, request: &PrincipalRequest,
) -> Result<Option<Uuid>, PrincipalError> {
    let mut stage = FailureStage::RunLock;
    match reconcile_locked(content, control, tenant, request, &mut stage, PrincipalMode::Adoption).await? {
        PrincipalDisposition::Unchanged { principal } | PrincipalDisposition::Changed { to: principal, .. } => Ok(Some(principal)),
        _ => Ok(None),
    }
}
