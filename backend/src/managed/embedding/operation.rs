use super::*;
use std::collections::BTreeSet;

use coralswarm_core::embed::{EmbedError, EmbedProvider};
use serde::Serialize;
use sqlx::{PgConnection, PgPool};

mod statements;
pub(crate) use statements::process_statements;

use crate::{
    jobs::queue::JobRow,
    services::{blob_store::PromotionBlobStore, content_replacement},
    store::Store,
    tenancy::TenantId,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub(in crate::managed) struct SourceBinding {
    source_item_id: Uuid,
    source_version_id: Uuid,
    current_version_id: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Serialize, sqlx::FromRow)]
pub(in crate::managed) struct PreparedInput {
    atom_id: Uuid,
    graph_id: Uuid,
    content_revision: i64,
    content_blob_key: String,
    content_inline: Option<String>,
    header_len: Option<i32>,
    timestamp: DateTime<Utc>,
    fts_language: String,
    has_current_cloud: bool,
    #[sqlx(skip)]
    source_bindings: Vec<SourceBinding>,
    #[sqlx(skip)]
    text: String,
}

pub(in crate::managed) struct PreparedBatch {
    pub(in crate::managed) account_id: Uuid,
    pub(in crate::managed) graph_id: Uuid,
    pub(in crate::managed) job_id: Uuid,
    pub(in crate::managed) principal: Uuid,
    job_kind: String,
    job_payload: serde_json::Value,
    job_attempts: i32,
    inputs: Vec<PreparedInput>,
    /// Statement batch (`EmbedStatements`): set instead of `inputs`.
    statements: Vec<statements::StatementInput>,
}

#[derive(Debug)]
pub(crate) struct EmbeddingReport {
    pub(crate) written: usize,
    pub(crate) discarded_stale: usize,
    pub(crate) blocked: usize,
    owner: JobOwner,
}

struct LockedSnapshot {
    operator_paused: bool,
}

enum Publication {
    Written(usize),
    Held,
    DiscardedStale,
}

tokio::task_local! {
    /// The first error behind a `Deny::Unavailable` in one job's run
    /// ([`with_failure_cause`]), so the job's failure records what failed
    /// (`jobs.error_message`) and not just that something did.
    static UNAVAILABLE_CAUSE: std::cell::RefCell<Option<String>>;
}

fn unavailable(error: impl std::fmt::Display) -> Deny {
    tracing::warn!(%error, "managed embedding operation unavailable");
    let _ = UNAVAILABLE_CAUSE.try_with(|cause| {
        cause.borrow_mut().get_or_insert_with(|| error.to_string());
    });
    Deny::Unavailable
}

/// Run one job's managed embedding (`process` and its settlement) with
/// [`failure_cause`] recording the first error behind a `Deny::Unavailable`.
pub(crate) async fn with_failure_cause<F: std::future::Future>(work: F) -> F::Output {
    UNAVAILABLE_CAUSE.scope(std::cell::RefCell::new(None), work).await
}

/// Inside [`with_failure_cause`], the first error behind a
/// `Deny::Unavailable` so far.
pub(crate) fn failure_cause() -> Option<String> {
    UNAVAILABLE_CAUSE.try_with(|cause| cause.borrow().clone()).ok().flatten()
}

pub(in crate::managed) async fn begin<'a>(
    scope: &JobScope<'a>,
) -> Result<Transaction<'a, Postgres>, Deny> {
    let mut tx = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        match scope {
            JobScope::Consumer { pool, account_id } => {
                begin_account_scoped(pool, *account_id).await
            }
            JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool).await,
        }
    })
    .await
    .map_err(unavailable)?
    .map_err(unavailable)?;
    sqlx::query(
        "SELECT set_config('lock_timeout','2s',true),set_config('statement_timeout','5s',true)",
    )
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(tx)
}

async fn control<'a>(
    tenant: &TenantId,
    pool: &'a PgPool,
    account: Uuid,
) -> Result<Option<Transaction<'a, Postgres>>, Deny> {
    match tenant {
        TenantId::Consumer => Ok(None),
        TenantId::Org(_) => Ok(Some(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                begin_account_scoped(pool, account),
            )
            .await
            .map_err(unavailable)?
            .map_err(unavailable)?,
        )),
    }
}

async fn source_bindings(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    atom: Uuid,
) -> Result<Vec<SourceBinding>, Deny> {
    sqlx::query_as("SELECT s.source_item_id,v.version_id AS source_version_id,s.current_version_id FROM source_version_chunks c JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id WHERE c.graph_id=$1 AND c.atom_id=$2 ORDER BY s.source_item_id,v.version_id")
        .bind(graph).bind(atom).fetch_all(&mut **tx).await.map_err(unavailable)
}

async fn load_inputs(
    tx: &mut Transaction<'_, Postgres>,
    account: Uuid,
    ids: &[Uuid],
    locked: bool,
) -> Result<Vec<PreparedInput>, Deny> {
    let query = if locked {
        "SELECT a.atom_id,a.graph_id,a.content_revision,a.content_blob_key,a.content_inline,a.header_len,a.timestamp,COALESCE(a.fts_language,'english') AS fts_language,COALESCE(a.embedding_next IS NOT NULL AND a.embed_model_next='openai/text-embedding-3-small@384',false) AS has_current_cloud FROM atoms a JOIN graphs g ON g.id=a.graph_id WHERE g.account_id=$1 AND a.atom_id=ANY($2) ORDER BY a.atom_id FOR UPDATE OF a"
    } else {
        "SELECT a.atom_id,a.graph_id,a.content_revision,a.content_blob_key,a.content_inline,a.header_len,a.timestamp,COALESCE(a.fts_language,'english') AS fts_language,COALESCE(a.embedding_next IS NOT NULL AND a.embed_model_next='openai/text-embedding-3-small@384',false) AS has_current_cloud FROM atoms a JOIN graphs g ON g.id=a.graph_id WHERE g.account_id=$1 AND a.atom_id=ANY($2) ORDER BY a.atom_id"
    };
    let mut rows: Vec<PreparedInput> = sqlx::query_as(query)
        .bind(account)
        .bind(ids)
        .fetch_all(&mut **tx)
        .await
        .map_err(unavailable)?;
    if rows.len() != ids.len() {
        return Err(Deny::Hold(HoldCode::SourceRemovedOrAccessLost));
    }
    for row in &mut rows {
        row.source_bindings = source_bindings(tx, row.graph_id, row.atom_id).await?;
    }
    Ok(rows)
}

pub(in crate::managed) async fn reachable(
    content: &mut PgConnection,
    tenant: &TenantId,
    account: Uuid,
    graph: Uuid,
    user: Uuid,
) -> Result<bool, Deny> {
    match tenant {
        TenantId::Consumer => sqlx::query_scalar("SELECT resolve_graph_reachable_by_user($1,$2)")
            .bind(graph)
            .bind(user)
            .fetch_one(content)
            .await
            .map_err(unavailable),
        TenantId::Org(_) => {
            sqlx::query_scalar("SELECT resolve_tenant_graph_reachable_by_user($1,$2,$3)")
                .bind(graph)
                .bind(account)
                .bind(user)
                .fetch_one(content)
                .await
                .map_err(unavailable)
        }
    }
}

/// Principal selection's plane check, before a gate exists: the permit is
/// minted in the selection's own transaction (control for an org, the
/// consumer plane otherwise), and the selection fences take it.
pub(in crate::managed) async fn admit_locked(
    content: &mut Transaction<'_, Postgres>,
    financial: Option<&mut Transaction<'_, Postgres>>,
    tenant: &TenantId,
    account: Uuid,
) -> Result<PlanePermit, Deny> {
    let db: &mut PgConnection = match financial {
        Some(tx) => &mut **tx,
        None => &mut **content,
    };
    PlanePermit::admit(db, account, tenant).await
}

/// Whether `user` may be charged for a call on `graph`: the account still
/// resolves to the permit's plane, its org is not revoked, `user` is a
/// current member, and the graph is reachable by them. The one member
/// check enrichment's attributed user and the interactive asker share; each
/// locks its own graph row first. `Ok(false)` is no access; the caller
/// names the hold.
pub(in crate::managed) async fn member_reaches_locked(
    content: &mut Transaction<'_, Postgres>,
    mut financial: Option<&mut Transaction<'_, Postgres>>,
    permit: &PlanePermit,
    graph: Uuid,
    user: Uuid,
) -> Result<bool, Deny> {
    let account = permit.account();
    let db: &mut PgConnection = match financial.as_mut() {
        Some(tx) => &mut ***tx,
        None => &mut **content,
    };
    permit.confirm_locked(&mut *db, account).await?;
    let member: Option<Uuid> = sqlx::query_scalar(
        "SELECT m.user_id FROM account_members m JOIN users u ON u.id=m.user_id AND u.deleted_at IS NULL \
          WHERE m.account_id=$1 AND m.user_id=$2 \
            AND NOT EXISTS(SELECT 1 FROM org_revocations r JOIN accounts a ON a.clerk_org_id=r.clerk_org_id WHERE a.id=$1) \
          FOR SHARE OF m,u",
    )
    .bind(account)
    .bind(user)
    .fetch_optional(&mut *db)
    .await
    .map_err(unavailable)?;
    Ok(member.is_some() && reachable(content, permit.tenant(), account, graph, user).await?)
}

/// The one member a graph-level managed call is charged to when no member
/// asked for it: the personal or member ocean's owner, else the earliest
/// owner/admin who can still reach the graph. Shared by cloud embedding and
/// `managed_enrichment`.
pub(in crate::managed) async fn executor(
    content: &mut Transaction<'_, Postgres>,
    mut financial: Option<&mut Transaction<'_, Postgres>>,
    permit: &PlanePermit,
    graph: Uuid,
) -> Result<Uuid, Deny> {
    let (tenant, account) = (permit.tenant(), permit.account());
    let shape: Option<(String,Option<Uuid>)> = sqlx::query_as("SELECT kind,owner_user_id FROM graphs WHERE id=$1 AND account_id=$2 AND archived_at IS NULL FOR SHARE")
        .bind(graph).bind(account).fetch_optional(&mut **content).await.map_err(unavailable)?;
    let Some((kind, owner)) = shape else {
        return Err(Deny::Hold(HoldCode::RequesterAccessLost));
    };
    let db: &mut PgConnection = match financial.as_mut() {
        Some(tx) => &mut ***tx,
        None => &mut **content,
    };
    // The plane, re-resolved under the account row's share lock.
    permit.confirm_locked(&mut *db, account).await?;
    let account_owner: Uuid = sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id=$1")
        .bind(account)
        .fetch_one(&mut *db)
        .await
        .map_err(unavailable)?;
    let revoked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM org_revocations r JOIN accounts a ON a.clerk_org_id=r.clerk_org_id WHERE a.id=$1)")
        .bind(account).fetch_one(&mut *db).await.map_err(unavailable)?;
    if revoked {
        return Err(Deny::Hold(HoldCode::RequesterAccessLost));
    }
    let personal_owner = if kind == "personal" {
        Some(account_owner)
    } else {
        owner
    };
    let candidates: Vec<Uuid> = sqlx::query_scalar("SELECT m.user_id FROM account_members m JOIN users u ON u.id=m.user_id AND u.deleted_at IS NULL WHERE m.account_id=$1 AND (CASE WHEN $2 IN ('personal','member') THEN m.user_id=$3 ELSE m.role IN ('owner','admin') END) ORDER BY m.joined_at,m.user_id LIMIT 33 FOR SHARE OF m,u")
        .bind(account).bind(&kind).bind(personal_owner).fetch_all(&mut *db).await.map_err(unavailable)?;
    let count = candidates.len();
    for candidate in candidates.into_iter().take(32) {
        if reachable(content, tenant, account, graph, candidate).await? {
            sqlx::query("SELECT tm.user_id FROM teams t JOIN team_members tm ON tm.team_id=t.id WHERE t.graph_id=$1 AND tm.user_id=$2 AND tm.departing_at IS NULL FOR SHARE OF t,tm")
                .bind(graph).bind(candidate).fetch_all(&mut **content).await.map_err(unavailable)?;
            if reachable(content, tenant, account, graph, candidate).await? {
                return Ok(candidate);
            }
        }
    }
    if count > 32 {
        Err(Deny::Unavailable)
    } else {
        Err(Deny::Hold(HoldCode::RequesterAccessLost))
    }
}

/// Account-level admission shared by every graph-level managed purpose: the
/// global kill switch and the lifecycle contract. `_financial` is kept so the
/// callers' shared signature does not change.
pub(in crate::managed) async fn policy(
    content: &mut Transaction<'_, Postgres>,
    _financial: Option<&mut Transaction<'_, Postgres>>,
    _account: Uuid,
) -> Result<(), Deny> {
    if Caps::from_env().disabled {
        return Err(Deny::Hold(HoldCode::ProcessingDisabled));
    }
    if !crate::corpus_enrichment::lifecycle_ready_tx(content)
        .await
        .map_err(unavailable)?
    {
        return Err(Deny::Hold(HoldCode::LifecycleContractNotReady));
    }
    Ok(())
}

impl PreparedBatch {
    pub(in crate::managed) fn worst_case(
        &self,
        sheet: &PriceSheet,
        as_of: DateTime<Utc>,
    ) -> Result<WorstCase, RefuseCode> {
        if !self.statements.is_empty() {
            return self.statement_worst_case(sheet, as_of);
        }
        let batch = ReceiptBatch::checked(
            self.account_id,
            self.graph_id,
            self.job_id,
            self.principal,
            self.inputs
                .iter()
                .map(|p| ReceiptInput {
                    atom_id: p.atom_id,
                    content_revision: p.content_revision,
                    text: p.text.clone(),
                })
                .collect(),
        )
        .map_err(|_| RefuseCode::CallCostCap)?;
        embedding_worst_case(&batch, sheet, as_of)
    }

    pub(in crate::managed) fn request_hash(
        &self,
        tenant: &TenantId,
        worst: &WorstCase,
    ) -> Result<String, Deny> {
        if !self.statements.is_empty() {
            return self.statement_request_hash(tenant, worst);
        }
        let inputs:Vec<_>=self.inputs.iter().map(|p|serde_json::json!({
            "atom":p.atom_id,"revision":p.content_revision,"blob":p.content_blob_key,
            "inline":p.content_inline,"header":p.header_len,"timestamp":p.timestamp,
            "sources":p.source_bindings,"sha256":hex::encode(Sha256::digest(p.text.as_bytes())),"bytes":p.text.len()
        })).collect();
        let plane = match tenant {
            TenantId::Consumer => "consumer".to_owned(),
            TenantId::Org(org) => format!("org:{org}"),
        };
        let bytes=serde_json::to_vec(&serde_json::json!({"purpose":"cloud_embedding","plane":plane,
            "account":self.account_id,"graph":self.graph_id,"job":self.job_id,"kind":self.job_kind,"payload":self.job_payload,
            "attempts":self.job_attempts,"principal":self.principal,"inputs":inputs,"model":worst.terms.models,"ceiling":worst.ceiling().prompt,"bound":worst.per_attempt.get()})).map_err(unavailable)?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }

    pub(in crate::managed) async fn insert_inputs(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        receipt: Uuid,
    ) -> Result<(), Deny> {
        if !self.statements.is_empty() {
            return self.insert_statement_inputs(tx, receipt).await;
        }
        for (ordinal, input) in self.inputs.iter().enumerate() {
            sqlx::query("INSERT INTO managed_embedding_inputs(receipt_id,account_id,graph_id,atom_id,ordinal,content_revision,content_sha256,input_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                .bind(receipt).bind(self.account_id).bind(self.graph_id).bind(input.atom_id).bind(ordinal as i16)
                .bind(input.content_revision).bind(Sha256::digest(input.text.as_bytes()).to_vec()).bind(input.text.len() as i64)
                .execute(&mut **tx).await.map_err(unavailable)?;
        }
        Ok(())
    }

    pub(in crate::managed) async fn fresh_locked(
        &self,
        content: &mut Transaction<'_, Postgres>,
        mut financial: Option<&mut Transaction<'_, Postgres>>,
        permit: &PlanePermit,
    ) -> Result<(), Deny> {
        self.authority_locked(content, financial.as_deref_mut(), permit)
            .await?;
        let snapshot = self.snapshot_locked(content).await?;
        self.current_authority_locked(content, financial, permit, &snapshot)
            .await
    }

    async fn authority_locked(
        &self,
        content: &mut Transaction<'_, Postgres>,
        mut financial: Option<&mut Transaction<'_, Postgres>>,
        permit: &PlanePermit,
    ) -> Result<(), Deny> {
        if permit.account() != self.account_id {
            return Err(Deny::Unavailable);
        }
        policy(content, financial.as_deref_mut(), self.account_id).await?;
        if executor(content, financial, permit, self.graph_id).await?
            != self.principal
        {
            return Err(Deny::Hold(HoldCode::RequesterAccessLost));
        }
        self.lock_job(content).await
    }

    async fn current_authority_locked(
        &self,
        content: &mut Transaction<'_, Postgres>,
        financial: Option<&mut Transaction<'_, Postgres>>,
        permit: &PlanePermit,
        snapshot: &LockedSnapshot,
    ) -> Result<(), Deny> {
        if snapshot.operator_paused {
            return Err(Deny::Hold(HoldCode::ManagedRunPaused));
        }
        if executor(content, financial, permit, self.graph_id).await?
            != self.principal
        {
            return Err(Deny::Hold(HoldCode::RequesterAccessLost));
        }
        Ok(())
    }

    async fn snapshot_locked(
        &self,
        content: &mut Transaction<'_, Postgres>,
    ) -> Result<LockedSnapshot, Deny> {
        if !self.statements.is_empty() {
            return self.statement_snapshot_locked(content).await;
        }
        snapshot_locked(content, self.account_id, self.graph_id, &self.inputs).await
    }

    async fn lock_job(&self, content: &mut Transaction<'_, Postgres>) -> Result<(), Deny> {
        let valid_job:Option<Uuid>=sqlx::query_scalar("SELECT id FROM jobs WHERE id=$1 AND account_id=$2 AND kind=$3 AND payload=$4 AND status::text='running' AND attempts=$5 FOR UPDATE")
            .bind(self.job_id).bind(self.account_id).bind(&self.job_kind).bind(&self.job_payload).bind(self.job_attempts).fetch_optional(&mut **content).await.map_err(unavailable)?;
        valid_job.map(|_| ()).ok_or(Deny::Hold(HoldCode::Fenced))
    }
}

async fn snapshot_locked(
    content: &mut Transaction<'_, Postgres>,
    account: Uuid,
    graph: Uuid,
    inputs: &[PreparedInput],
) -> Result<LockedSnapshot, Deny> {
    let items: BTreeSet<_> = inputs
        .iter()
        .flat_map(|p| p.source_bindings.iter().map(|s| s.source_item_id))
        .collect();
    let versions: BTreeSet<_> = inputs
        .iter()
        .flat_map(|p| p.source_bindings.iter().map(|s| s.source_version_id))
        .collect();
    let versions: Vec<_> = versions.into_iter().collect();
    let owners: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT r.run_id,u.unit_id FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u ON u.run_id=r.run_id AND u.graph_id=r.graph_id WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=ANY($2) AND u.execution_owner AND r.retired_at IS NULL ORDER BY r.run_id,u.unit_id FOR SHARE OF r,u")
        .bind(graph).bind(&versions).fetch_all(&mut **content).await.map_err(unavailable)?;
    for item in items {
        let present:Option<Uuid>=sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_item_id=$2 AND access_state='permitted' FOR UPDATE")
                .bind(graph).bind(item).fetch_optional(&mut **content).await.map_err(unavailable)?;
        if present.is_none() {
            return Err(Deny::Hold(HoldCode::SourceRemovedOrAccessLost));
        }
    }
    for version in &versions {
        let present: Option<Uuid> = sqlx::query_scalar(
            "SELECT version_id FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE",
        )
        .bind(graph)
        .bind(version)
        .fetch_optional(&mut **content)
        .await
        .map_err(unavailable)?;
        if present.is_none() {
            return Err(Deny::Hold(HoldCode::SourceRemovedOrAccessLost));
        }
    }
    let ids: Vec<_> = inputs.iter().map(|p| p.atom_id).collect();
    let current = load_inputs(content, account, &ids, true).await?;
    for (expected, actual) in inputs.iter().zip(&current) {
        let mut snapshot = expected.clone();
        snapshot.text.clear();
        if snapshot != *actual {
            return Err(Deny::Hold(HoldCode::Fenced));
        }
    }
    sqlx::query("SELECT c.atom_id FROM source_version_chunks c WHERE c.graph_id=$1 AND c.atom_id=ANY($2) ORDER BY c.atom_id,c.source_version_id FOR UPDATE")
            .bind(graph).bind(&ids).fetch_all(&mut **content).await.map_err(unavailable)?;
    for input in inputs {
        if source_bindings(content, graph, input.atom_id).await? != input.source_bindings {
            return Err(Deny::Hold(HoldCode::Fenced));
        }
    }
    let permitted: bool =
        sqlx::query_scalar("SELECT count(*)=$3 FROM library_permitted_atoms($1,$2)")
            .bind(graph)
            .bind(&ids)
            .bind(ids.len() as i64)
            .fetch_one(&mut **content)
            .await
            .map_err(unavailable)?;
    if !permitted {
        return Err(Deny::Hold(HoldCode::SourceRemovedOrAccessLost));
    }
    let current: Vec<(Uuid, Uuid, String, Option<String>)> = sqlx::query_as("SELECT r.run_id,u.unit_id,r.status,r.pause_reason FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u ON u.run_id=r.run_id AND u.graph_id=r.graph_id WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=ANY($2) AND u.execution_owner AND r.retired_at IS NULL ORDER BY r.run_id,u.unit_id")
        .bind(graph).bind(&versions).fetch_all(&mut **content).await.map_err(unavailable)?;
    if current.iter().map(|row| (row.0, row.1)).collect::<Vec<_>>() != owners {
        return Err(Deny::Unavailable);
    }
    Ok(LockedSnapshot {
        operator_paused: current
            .iter()
            .any(|row| row.2 == "paused" && row.3.as_deref() == Some("operator_paused")),
    })
}

const OWNER_FIELD: &str = "_managed_embedding_claim";

#[derive(Debug)]
struct JobOwner {
    account: Uuid,
    job: Uuid,
    attempts: i32,
    payload: serde_json::Value,
}

impl JobOwner {
    async fn acquire(scope: &JobScope<'_>, job: &JobRow) -> Result<Self, Deny> {
        if let Some(previous) = job.payload.get(OWNER_FIELD) {
            let attempt = previous
                .get("queue_attempt")
                .and_then(serde_json::Value::as_i64)
                .ok_or(Deny::Hold(HoldCode::Fenced))?;
            if attempt >= i64::from(job.attempts) || attempt < 0 {
                return Err(Deny::Hold(HoldCode::Fenced));
            }
        }
        let account = job.account_id.ok_or(Deny::Unavailable)?;
        let mut payload = job.payload.clone();
        let object = payload.as_object_mut().ok_or(Deny::Unavailable)?;
        object.insert(
            OWNER_FIELD.to_owned(),
            serde_json::json!({"queue_attempt":job.attempts,"owner":Uuid::new_v4()}),
        );
        let mut tx = begin(scope).await?;
        let claimed=sqlx::query("UPDATE jobs SET payload=$6 WHERE id=$1 AND account_id=$2 AND attempts=$3 AND kind=$4 AND payload=$5 AND status::text='running'")
            .bind(job.id).bind(account).bind(job.attempts).bind(&job.kind).bind(&job.payload).bind(&payload)
            .execute(&mut *tx).await.map_err(unavailable)?.rows_affected();
        if claimed != 1 {
            return Err(Deny::Hold(HoldCode::Fenced));
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Self {
            account,
            job: job.id,
            attempts: job.attempts,
            payload,
        })
    }

    async fn lock(&self, tx: &mut Transaction<'_, Postgres>) -> Result<(), Deny> {
        let actual:Option<Uuid>=sqlx::query_scalar("SELECT id FROM jobs WHERE id=$1 AND account_id=$2 AND attempts=$3 AND payload=$4 AND status::text='running' FOR UPDATE")
            .bind(self.job).bind(self.account).bind(self.attempts).bind(&self.payload)
            .fetch_optional(&mut **tx).await.map_err(unavailable)?;
        actual.map(|_| ()).ok_or(Deny::Hold(HoldCode::Fenced))
    }

    async fn check(&self, scope: &JobScope<'_>) -> Result<(), Deny> {
        let mut tx = begin(scope).await?;
        self.lock(&mut tx).await?;
        tx.commit().await.map_err(unavailable)
    }
}

impl EmbeddingReport {
    pub(crate) async fn finish(
        self,
        tenant: &TenantId,
        pool: &PgPool,
        next: Option<(&crate::jobs::JobKind, i64)>,
    ) -> Result<(), Deny> {
        let scope = JobScope::for_job(tenant, pool, self.owner.account);
        let mut tx = begin(&scope).await?;
        self.owner.lock(&mut tx).await?;
        if let Some((kind, delay)) = next {
            let key = format!(
                "managed_embedding:{}:{}",
                self.owner.job,
                serde_json::to_string(kind).map_err(unavailable)?
            );
            crate::jobs::queue::enqueue_tx(
                &mut tx,
                kind,
                Some(self.owner.account),
                Some(&key),
                delay,
            )
            .await
            .map_err(unavailable)?;
        }
        let reason = if self.blocked > 0 {
            Some("managed_embedding_held")
        } else if self.discarded_stale > 0 {
            Some("managed_embedding_discarded_stale")
        } else {
            None
        };
        sqlx::query("UPDATE jobs SET status='done',finished_at=now(),outcome=$2,error_message=$3,payload=payload-$4 WHERE id=$1")
            .bind(self.owner.job).bind(if self.written>0 {"did_work"} else {"nothing_to_do"}).bind(reason).bind(OWNER_FIELD)
            .execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
}

async fn recover_job(
    scope: &JobScope<'_>,
    control: &PgPool,
    tenant: &TenantId,
    owner: &JobOwner,
) -> Result<(), Deny> {
    let mut tx = begin(scope).await?;
    owner.lock(&mut tx).await?;
    let calls:Vec<Uuid>=sqlx::query_scalar("SELECT DISTINCT logical_call FROM managed_attempt_receipts WHERE account_id=$1 AND job_id=$2 AND purpose='cloud_embedding' AND protocol_version=1 ORDER BY logical_call")
        .bind(owner.account).bind(owner.job).fetch_all(&mut *tx).await.map_err(unavailable)?;
    tx.commit().await.map_err(unavailable)?;
    for call in calls {
        super::super::gate::recover_logical_call(scope, control, tenant, owner.account, call)
            .await
            .map_err(unavailable)?;
    }
    Ok(())
}

impl PreparedBatch {
    pub(in crate::managed) async fn hold(
        &self,
        scope: &JobScope<'_>,
        code: &str,
    ) -> Result<(), Deny> {
        if !self.statements.is_empty() {
            return self.hold_statements(scope, code).await;
        }
        hold_inputs(
            scope,
            self.account_id,
            self.graph_id,
            self.job_id,
            self.job_attempts,
            &self.job_payload,
            &self.inputs,
            code,
        )
        .await
    }

    async fn publish(
        &self,
        scope: &JobScope<'_>,
        control_pool: &PgPool,
        tenant: &TenantId,
        store: &dyn Store,
        logical_call: Uuid,
        request_hash: &str,
        vectors: &[Vec<f32>],
    ) -> Result<Publication, Deny> {
        let mut financial = control(tenant, control_pool, self.account_id).await?;
        let mut tx = begin(scope).await?;
        self.lock_job(&mut tx).await?;
        let receipt:Option<Uuid>=sqlx::query_scalar("SELECT receipt_id FROM managed_attempt_receipts WHERE account_id=$1 AND graph_id=$2 AND job_id=$3 AND purpose='cloud_embedding' AND protocol_version=1 AND logical_call=$4 AND request_hash=$5 AND dispatch_phase='armed' AND state IN ('billed','overrun','ambiguous')")
            .bind(self.account_id).bind(self.graph_id).bind(self.job_id).bind(logical_call).bind(request_hash).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        let Some(receipt) = receipt else {
            return Err(Deny::Unavailable);
        };
        let inputs:Vec<(Uuid,i16,i64,Vec<u8>,i64,String)>=sqlx::query_as("SELECT atom_id,ordinal,content_revision,content_sha256,input_bytes,outcome FROM managed_embedding_inputs WHERE receipt_id=$1 AND account_id=$2 AND graph_id=$3 AND purpose='cloud_embedding' ORDER BY ordinal FOR UPDATE")
            .bind(receipt).bind(self.account_id).bind(self.graph_id).fetch_all(&mut *tx).await.map_err(unavailable)?;
        if inputs.len() != self.inputs.len() || vectors.len() != self.inputs.len() {
            return Err(Deny::Unavailable);
        }
        for (ordinal, (actual, expected)) in inputs.iter().zip(&self.inputs).enumerate() {
            if actual.0 != expected.atom_id
                || actual.1 != ordinal as i16
                || actual.2 != expected.content_revision
                || actual.3 != Sha256::digest(expected.text.as_bytes()).to_vec()
                || actual.4 != expected.text.len() as i64
            {
                return Err(Deny::Unavailable);
            }
        }
        if inputs.iter().all(|p| p.5 == "written") {
            return Ok(Publication::Written(self.inputs.len()));
        }
        if inputs.iter().any(|p| p.5 != "sent") {
            return Ok(Publication::DiscardedStale);
        }
        // Publish-time authority: the plane re-resolved in this transaction.
        let authority = match admit_locked(&mut tx, financial.as_mut(), tenant, self.account_id).await {
            Ok(permit) => self.authority_locked(&mut tx, financial.as_mut(), &permit).await.map(|()| permit),
            Err(deny) => Err(deny),
        };
        if matches!(authority, Err(Deny::Unavailable)) {
            return Err(Deny::Unavailable);
        }
        let snapshot = match self.snapshot_locked(&mut tx).await {
            Ok(snapshot) => Some(snapshot),
            Err(Deny::Hold(_) | Deny::Refuse(_)) => None,
            Err(error) => return Err(error),
        };
        let fresh = match (&snapshot, authority) {
            (Some(snapshot), Ok(permit)) => {
                self.current_authority_locked(&mut tx, financial.as_mut(), &permit, snapshot)
                    .await
            }
            (_, Err(deny)) => Err(deny),
            (None, Ok(_)) => Err(Deny::Hold(HoldCode::Fenced)),
        };
        match fresh {
            Ok(()) => {}
            Err(Deny::Unavailable) => return Err(Deny::Unavailable),
            Err(deny) => {
                let held = if let Some(snapshot) = &snapshot {
                    write_hold_locked(&mut tx, &self.inputs, reason(deny), snapshot).await?
                } else {
                    Publication::DiscardedStale
                };
                sqlx::query("UPDATE managed_embedding_inputs SET outcome='discarded_stale' WHERE receipt_id=$1 AND outcome='sent'")
                    .bind(receipt).execute(&mut *tx).await.map_err(unavailable)?;
                if let Some(financial) = financial {
                    financial.commit().await.map_err(unavailable)?;
                }
                tx.commit().await.map_err(unavailable)?;
                return Ok(held);
            }
        }
        let ids: Vec<_> = self.inputs.iter().map(|p| p.atom_id).collect();
        let texts: Vec<_> = self.inputs.iter().map(|p| p.text.clone()).collect();
        let languages: Vec<_> = self.inputs.iter().map(|p| p.fts_language.clone()).collect();
        store
            .set_cloud_embeddings_scoped(&mut tx, &ids, vectors)
            .await
            .map_err(unavailable)?;
        crate::workers::embed::update_content_tsv_and_clear_failure(
            &mut *tx, &ids, &texts, &languages,
        )
        .await
        .map_err(unavailable)?;
        sqlx::query("UPDATE managed_embedding_inputs SET outcome='written' WHERE receipt_id=$1 AND outcome='sent'")
            .bind(receipt).execute(&mut *tx).await.map_err(unavailable)?;
        if let Some(financial) = financial {
            financial.commit().await.map_err(unavailable)?;
        }
        if let Err(error) = tx.commit().await {
            tracing::warn!(%error,%receipt,"embedding publication commit response uncertain; reading durable input outcomes");
            let mut read = begin(scope).await?;
            let outcomes:Vec<String>=sqlx::query_scalar("SELECT outcome FROM managed_embedding_inputs WHERE receipt_id=$1 AND account_id=$2 AND graph_id=$3 ORDER BY ordinal")
                .bind(receipt).bind(self.account_id).bind(self.graph_id).fetch_all(&mut *read).await.map_err(unavailable)?;
            read.commit().await.map_err(unavailable)?;
            if outcomes.len() != self.inputs.len()
                || outcomes.iter().any(|outcome| outcome != "written")
            {
                return Err(Deny::Unavailable);
            }
        }
        Ok(Publication::Written(ids.len()))
    }
}

async fn hold_inputs(
    scope: &JobScope<'_>,
    account: Uuid,
    graph: Uuid,
    job: Uuid,
    attempts: i32,
    payload: &serde_json::Value,
    inputs: &[PreparedInput],
    code: &str,
) -> Result<(), Deny> {
    let mut tx = begin(scope).await?;
    let actual:Option<Uuid>=sqlx::query_scalar("SELECT id FROM jobs WHERE id=$1 AND account_id=$2 AND attempts=$3 AND payload=$4 AND status::text='running' FOR UPDATE")
        .bind(job).bind(account).bind(attempts).bind(payload).fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if actual.is_none() {
        return Err(Deny::Hold(HoldCode::Fenced));
    }
    hold_inputs_locked(&mut tx, account, graph, inputs, code).await?;
    tx.commit().await.map_err(unavailable)
}

async fn hold_inputs_locked(
    tx: &mut Transaction<'_, Postgres>,
    account: Uuid,
    graph: Uuid,
    inputs: &[PreparedInput],
    code: &str,
) -> Result<Publication, Deny> {
    let snapshot = match snapshot_locked(tx, account, graph, inputs).await {
        Ok(snapshot) => snapshot,
        Err(Deny::Hold(_) | Deny::Refuse(_)) => return Ok(Publication::DiscardedStale),
        Err(error) => return Err(error),
    };
    write_hold_locked(tx, inputs, code, &snapshot).await
}

async fn write_hold_locked(
    tx: &mut Transaction<'_, Postgres>,
    inputs: &[PreparedInput],
    code: &str,
    _snapshot: &LockedSnapshot,
) -> Result<Publication, Deny> {
    let ids: Vec<_> = inputs.iter().map(|p| p.atom_id).collect();
    let texts: Vec<_> = inputs.iter().map(|p| p.text.clone()).collect();
    let languages: Vec<_> = inputs.iter().map(|p| p.fts_language.clone()).collect();
    crate::workers::embed::update_content_tsv_and_clear_failure(
        &mut **tx, &ids, &texts, &languages,
    )
    .await
    .map_err(unavailable)?;
    let stamp = if code == HoldCode::CloudProcessingDisabled.code() {
        crate::workers::embed::SKIPPED_LOCAL_ONLY.to_owned()
    } else {
        format!("blocked:{code}")
    };
    // A hold is an attempt, so the retry sweep's attempts cap ends a hold
    // that never clears. Holds used to leave `embed_attempts` alone, and an
    // atom the provider refused was re-sent every sweep, forever (prod,
    // 2026-10-23). A permanent failure (`embedding::PERMANENT_HOLDS`: an
    // input no request can carry, a 4xx other than 429) jumps straight to
    // the cap: no retry can help. Not counted, because each waits on a switch
    // that someone flips back and the ladder (about an hour) would strand the
    // atom first: the money holds (the next UTC midnight, against which the
    // timestamp below is backdated with the unchanged count, or a top-up),
    // the operator's run pause and kill switch, and the local-only stamp,
    // which the sweep never claims anyway.
    sqlx::query("UPDATE atoms SET embed_model_next=$2,embed_attempts=CASE WHEN $4=ANY($5) THEN GREATEST(embed_attempts,$6) WHEN $4 IN ('daily_budget_exhausted','credits_exhausted','managed_run_paused','managed_processing_disabled','cloud_processing_disabled') THEN embed_attempts ELSE embed_attempts+1 END,embed_failed_at=CASE WHEN $4='daily_budget_exhausted' THEN (date_trunc('day',clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')+interval '1 day'-(power(2,LEAST(embed_attempts,30))*interval '30 seconds') ELSE clock_timestamp() END WHERE atom_id=ANY($1) AND (embedding_next IS NULL OR embed_model_next IS DISTINCT FROM $3)")
        .bind(&ids).bind(stamp).bind(coralswarm_core::embed::EMBED_MODEL_CLOUD).bind(code).bind(crate::embedding::PERMANENT_HOLDS.to_vec()).bind(crate::embedding::MAX_EMBED_ATTEMPTS).execute(&mut **tx).await.map_err(unavailable)?;
    Ok(Publication::Held)
}

fn reason(deny: Deny) -> &'static str {
    match deny {
        Deny::Hold(code) => code.code(),
        Deny::Refuse(code) => code.code(),
        Deny::Unavailable => "unavailable",
    }
}

/// What [`settle_unprocessed`] did with a job [`process`] returned an error
/// for.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Unprocessed {
    /// Fenced: another run owns the job, or it is no longer this run's
    /// `running` row. Nothing written.
    LeftToOwner,
    /// A managed hold, under the shared hold rule (`queue::hold`).
    Held(crate::jobs::queue::Held),
    /// Failed on the job's attempts ladder with the cause in
    /// `error_message`. `dead`: that was its last attempt, and the atoms it
    /// carried were charged one embed attempt.
    Failed { dead: bool },
}

/// Settle a job [`process`] returned an error for, so it never waits on the
/// reaper.
///
/// It used to stay `running` ("unacknowledged for durable recovery"), and
/// the reaper reset it about 20 minutes later, three times, then dead-lettered
/// it with "worker died mid-job". Its atoms' `embed_attempts` never moved, so
/// the retry sweep re-enqueued them forever, and each run paid for its
/// provider call again (prod, 2026-10-25: a NUL in one chunk body failed the
/// publish after billing; 309 paid sends for that one atom).
///
/// Durable recovery does not need the row to stay `running`: every run
/// starts by recovering the job's earlier logical calls (`recover_job`), and
/// a job back on the ladder keeps its id and payload, so it is recovered the
/// same way the reaper's reset was.
///
/// - `Hold(Fenced)` is left alone: the job is not this run's.
/// - Any other hold or refusal goes through `queue::hold`.
/// - `Unavailable` (storage, a statement Postgres refused) fails the job on
///   its ladder, guarded on this run's `running` row, with
///   [`failure_cause`] as the error. When that was its last attempt, each of
///   `atoms` still unembedded is charged one embed attempt, so the sweep's
///   cap (`embedding::MAX_EMBED_ATTEMPTS`) ends a failure that repeats.
///   Charging only the last attempt keeps a short outage from using up the
///   atoms' attempts.
pub(crate) async fn settle_unprocessed(
    tenant: &TenantId,
    pool: &PgPool,
    job: &JobRow,
    deny: Deny,
    atoms: &[Uuid],
) -> Result<Unprocessed, String> {
    let code = match deny {
        Deny::Hold(HoldCode::Fenced) => return Ok(Unprocessed::LeftToOwner),
        Deny::Hold(code) => Some(code.code()),
        Deny::Refuse(code) => Some(code.code()),
        Deny::Unavailable => None,
    };
    if let Some(code) = code {
        return crate::jobs::queue::hold(pool, job.id, code)
            .await
            .map(Unprocessed::Held)
            .map_err(|e| e.to_string());
    }
    let cause = failure_cause().unwrap_or_else(|| "cause not recorded".to_owned());
    let message = format!("managed embedding unavailable: {cause}");
    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    let mine: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM jobs WHERE id=$1 AND attempts=$2 AND status::text='running' FOR UPDATE",
    )
    .bind(job.id)
    .bind(job.attempts)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    if mine.is_none() {
        return Ok(Unprocessed::LeftToOwner);
    }
    crate::jobs::queue::fail_on(&mut *tx, job.id, &message)
        .await
        .map_err(|e| e.to_string())?;
    let dead: bool = sqlx::query_scalar("SELECT status::text='dead' FROM jobs WHERE id=$1")
        .bind(job.id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    tx.commit().await.map_err(|e| e.to_string())?;
    if dead && !atoms.is_empty() {
        let account = job.account_id.ok_or("job has no account")?;
        let scope = JobScope::for_job(tenant, pool, account);
        let mut tx = begin(&scope).await.map_err(|deny| deny.to_string())?;
        crate::workers::embed::bulk_mark_atoms_failed(&mut *tx, atoms)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
    }
    Ok(Unprocessed::Failed { dead })
}

pub(crate) async fn process(
    tenant: &TenantId,
    pool: &PgPool,
    control_pool: &PgPool,
    job: &JobRow,
    ids: &[Uuid],
    cloud: &dyn EmbedProvider,
    blobs: Option<&dyn PromotionBlobStore>,
    store: &dyn Store,
) -> Result<EmbeddingReport, Deny> {
    let account = job.account_id.ok_or(Deny::Unavailable)?;
    if !matches!(
        job.kind.as_str(),
        "EmbedAtoms" | "EmbedAtomsRetry" | "ReembedOutdatedChunks"
    ) {
        return Err(Deny::Unavailable);
    }
    if job.kind != "ReembedOutdatedChunks" {
        let (payload_ids, _) = crate::workers::embed::parse_atom_ids(job).map_err(unavailable)?;
        if ids.iter().any(|id| !payload_ids.contains(id)) {
            return Err(Deny::Unavailable);
        }
    }
    let ids: Vec<_> = ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let scope = JobScope::for_job(tenant, pool, account);
    let owner = JobOwner::acquire(&scope, job).await?;
    recover_job(&scope, control_pool, tenant, &owner).await?;
    if ids.is_empty() {
        return Ok(EmbeddingReport {
            written: 0,
            discarded_stale: 0,
            blocked: 0,
            owner,
        });
    }
    let mut tx = begin(&scope).await?;
    let mut inputs = load_inputs(&mut tx, account, &ids, false).await?;
    inputs.retain(|input| !input.has_current_cloud);
    tx.commit().await.map_err(unavailable)?;
    if inputs.is_empty() {
        return Ok(EmbeddingReport {
            written: 0,
            discarded_stale: 0,
            blocked: 0,
            owner,
        });
    }
    let dek = crate::crypto::resolve_dek_for_tenant(tenant, pool, account)
        .await
        .map_err(unavailable)?
        .ok_or(Deny::Unavailable)?;
    let mut report = EmbeddingReport {
        written: 0,
        discarded_stale: 0,
        blocked: 0,
        owner,
    };
    let graphs: BTreeSet<_> = inputs.iter().map(|p| p.graph_id).collect();
    for graph in graphs {
        report.owner.check(&scope).await?;
        let graph_inputs: Vec<_> = inputs
            .iter()
            .filter(|p| p.graph_id == graph)
            .cloned()
            .collect();
        for chunk in graph_inputs.chunks(8) {
            let mut prepared = chunk.to_vec();
            for input in &mut prepared {
                match coralswarm_core::r2::validate_blob_key(&input.content_blob_key) {
                    Ok(()) => {
                        let graph = input
                            .content_blob_key
                            .rsplit('/')
                            .nth(1)
                            .and_then(|value| Uuid::parse_str(value).ok());
                        if graph != Some(input.graph_id) {
                            return Err(Deny::Hold(HoldCode::SourceRemovedOrAccessLost));
                        }
                    }
                    Err(error) if blobs.is_some() || input.content_inline.is_none() => {
                        return Err(unavailable(error))
                    }
                    Err(_) => {}
                }
                input.text = content_replacement::current_text(
                    &input.content_blob_key,
                    input.content_inline.as_deref(),
                    blobs,
                    Some(&dek),
                )
                .await
                .map_err(unavailable)?;
            }
            // The gateway bounds every text and groups them into provider
            // requests; a text it had to split goes alone, so it can only
            // fail itself. Planned here, before a receipt and request hash
            // bind a batch: each request is admitted, receipted and sent on
            // its own gate.
            let texts: Vec<&str> = prepared.iter().map(|p| p.text.as_str()).collect();
            let plan = crate::embedding::plan(&texts, 8);
            drop(texts);
            let mut slots: Vec<Option<PreparedInput>> = prepared.into_iter().map(Some).collect();
            // A text no provider request can carry is held for good, never
            // cut down.
            for (item, failure) in &plan.refused {
                let Some(input) = slots[*item].take() else { continue };
                tracing::warn!(%failure,job=%job.id,atom=%input.atom_id,"managed embedding input refused before sending");
                let code = crate::embedding::hold_code(failure);
                hold_inputs(&scope, account, graph, job.id, job.attempts, &report.owner.payload, std::slice::from_ref(&input), code).await?;
                report.blocked += 1;
            }
            let mut queue: std::collections::VecDeque<(crate::embedding::Request, Vec<PreparedInput>)> = plan
                .requests
                .into_iter()
                .map(|request| {
                    let inputs = request.items().iter().filter_map(|i| slots[*i].take()).collect();
                    (request, inputs)
                })
                .collect();
            while let Some((request, prepared)) = queue.pop_front() {
                report.owner.check(&scope).await?;
                let mut financial = control(tenant, control_pool, account).await?;
                let mut tx = begin(&scope).await?;
                let selected = match policy(&mut tx, financial.as_mut(), account).await {
                    Ok(()) => match admit_locked(&mut tx, financial.as_mut(), tenant, account).await {
                        Ok(permit) => executor(&mut tx, financial.as_mut(), &permit, graph).await,
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                };
                if let Some(financial) = financial {
                    financial.commit().await.map_err(unavailable)?;
                }
                tx.commit().await.map_err(unavailable)?;
                let principal = match selected {
                    Ok(principal) => principal,
                    Err(Deny::Unavailable) => return Err(Deny::Unavailable),
                    Err(deny) => {
                        hold_inputs(
                            &scope,
                            account,
                            graph,
                            job.id,
                            job.attempts,
                            &report.owner.payload,
                            &prepared,
                            reason(deny),
                        )
                        .await?;
                        report.blocked += prepared.len();
                        continue;
                    }
                };
                let batch = PreparedBatch {
                    account_id: account,
                    graph_id: graph,
                    job_id: job.id,
                    principal,
                    job_kind: job.kind.clone(),
                    job_payload: report.owner.payload.clone(),
                    job_attempts: job.attempts,
                    inputs: prepared,
                    statements: Vec::new(),
                };
                let call = super::super::ManagedCall {
                    plane: &scope,
                    control: control_pool,
                    tenant,
                    account_id: account,
                    principal,
                    fallback_model: None,
                    subject: super::super::Subject::EmbeddingBatch { batch: &batch },
                };
                let gate = match super::super::ManagedGate::embedding(call).await {
                    Ok(gate) => gate,
                    Err(deny) => {
                        let reason = match deny {
                            Deny::Hold(code) => code.code(),
                            Deny::Refuse(code) => code.code(),
                            Deny::Unavailable => "unavailable",
                        };
                        batch.hold(&scope, reason).await?;
                        report.blocked += batch.inputs.len();
                        continue;
                    }
                };
                let sent = crate::embedding::send(cloud, crate::embedding::Purpose::Managed(&gate), &request).await;
                match sent {
                    Ok(vectors) => {
                        report.owner.check(&scope).await?;
                        let (logical_call, request_hash) = gate.embedding_binding();
                        let publication = batch
                            .publish(
                                &scope,
                                control_pool,
                                tenant,
                                store,
                                logical_call,
                                request_hash,
                                &vectors,
                            )
                            .await?;
                        match publication {
                            Publication::Written(written) => report.written += written,
                            Publication::Held => report.blocked += batch.inputs.len(),
                            Publication::DiscardedStale => report.discarded_stale += batch.inputs.len(),
                        }
                    }
                    Err(failure) => match failure.error {
                        EmbedError::Denied(deny) => {
                            batch.hold(&scope, reason(deny)).await?;
                            report.blocked += batch.inputs.len();
                        }
                        // A permanent refusal of several texts cannot be
                        // pinned on one of them: send each again on its own
                        // receipt, so only the text at fault is held for
                        // good. A refused request generated nothing, so the
                        // re-sends cost no more than one success.
                        _ if crate::embedding::refused_unsent(&failure) && batch.inputs.len() > 1 => {
                            tracing::warn!(%failure,job=%job.id,inputs=batch.inputs.len(),"managed embedding request refused; re-sending each input alone");
                            drop(gate);
                            for input in batch.inputs {
                                let mut alone = crate::embedding::plan(&[input.text.as_str()], 1);
                                match alone.requests.pop() {
                                    Some(request) => queue.push_back((request, vec![input])),
                                    None => {
                                        hold_inputs(&scope, account, graph, job.id, job.attempts, &report.owner.payload, std::slice::from_ref(&input), crate::embedding::PROVIDER_REJECTED).await?;
                                        report.blocked += 1;
                                    }
                                }
                            }
                        }
                        _ => {
                            let code = crate::embedding::hold_code(&failure);
                            tracing::warn!(%failure,job=%job.id,inputs=batch.inputs.len(),code,"managed embedding failed after accounting");
                            batch.hold(&scope, code).await?;
                            report.blocked += batch.inputs.len();
                        }
                    },
                }
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
pub(crate) async fn recover_owned_job_for_test(
    tenant: &TenantId,
    pool: &PgPool,
    control: &PgPool,
    job: &JobRow,
) -> Result<(), Deny> {
    let account = job.account_id.ok_or(Deny::Unavailable)?;
    let scope = JobScope::for_job(tenant, pool, account);
    let owner = JobOwner {
        account,
        job: job.id,
        attempts: job.attempts,
        payload: job.payload.clone(),
    };
    recover_job(&scope, control, tenant, &owner).await
}
