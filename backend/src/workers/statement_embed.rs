//! Statement embeddings for scoped Ask's statement-vector leg.
//!
//! Each semantic atom's statement gets one cloud vector in
//! `semantic_atom_embeddings`, from the same model as chunk vectors
//! ([`EMBED_MODEL_CLOUD`]). Nothing here calls a provider: every send goes
//! through `managed::embedding::process_statements`, on the managed
//! `cloud_embedding` purpose (receipts, the per-account daily cap, the
//! account's egress policy). Statements are never embedded at Ask time.
//!
//! Producers:
//! - [`enqueue_after_persist`]: extraction persisted atoms for a version
//!   (`workers::source_atoms`, every profile including v2 and v3).
//! - [`sweep_tick`], the backfill: on an interval, per plane, the versions
//!   whose permitted statements lack a current vector (none yet, an older
//!   model tag, or a changed statement) and are not held past their retry
//!   time, current versions first. Bounded to [`SWEEP_VERSIONS`] versions a
//!   tick, skipping versions that already have a pending or running job, so
//!   it is idempotent and resumes from table state after any restart. A held
//!   statement (daily budget, cloud processing off, access lost) is retried
//!   after its `retry_after`; an exhausted daily cap waits for the next UTC
//!   day, so the backfill can never spend past the cap.
//!
//! Cost: `text-embedding-3-small` is $0.02 per million input tokens. A
//! statement is about 20-30 tokens, so 80,000 statements are about 2M tokens,
//! about $0.04.

use std::time::Duration;

use coralswarm_core::embed::{EmbedProvider, EMBED_MODEL_CLOUD};
use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::{queue, JobKind};
use crate::shutdown::Shutdown;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

pub(crate) const JOB_KIND: &str = "EmbedStatements";
/// Backfill cadence per plane.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_secs(300);
/// Versions one sweep enqueues per plane. With about 14 statements a
/// version this is about 2,800 statements every five minutes.
pub(crate) const SWEEP_VERSIONS: i64 = 200;
const SWEEP_DB_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay before a persist-triggered job runs (see [`enqueue_after_persist`]).
const PERSIST_DELAY_SECS: i64 = 30;

fn key(version: Uuid) -> String {
    format!("embed_statements:{version}")
}

/// Off switch for the statement pipeline (enqueue and sweep). On unless
/// `STATEMENT_EMBEDDINGS_DISABLED` is set; `MANAGED_PROCESSING_DISABLED`
/// and the account egress policy still hold every send.
pub(crate) fn enabled() -> bool {
    !crate::admission::env_flag("STATEMENT_EMBEDDINGS_DISABLED")
}

/// Extraction persisted semantic atoms for `version`: embed their
/// statements. Detached from the extraction's publication (like entity
/// resolution), because the embedding may wait on the daily cap. Delayed by
/// [`PERSIST_DELAY_SECS`] so the windows of one version coalesce into the one
/// pending job its key dedupes to.
pub(crate) async fn enqueue_after_persist(
    conn: &mut sqlx::PgConnection,
    account_id: Uuid,
    graph_id: Uuid,
    version: Uuid,
) -> Result<(), sqlx::Error> {
    if !enabled() {
        return Ok(());
    }
    queue::enqueue_detached(
        conn,
        &JobKind::EmbedStatements { graph_id, statement_version_id: version },
        Some(account_id),
        Some(&key(version)),
        PERSIST_DELAY_SECS,
    )
    .await
    .map(|_| ())
}

/// One dequeued `EmbedStatements` job. Without a control pool or a sealed
/// cloud transport nothing can be receipted, so nothing is sent and the job
/// ends `done` with `managed_embedding_unavailable`; the sweep retries later.
pub(crate) async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    cloud: Option<&(dyn EmbedProvider + Send + Sync)>,
    control: Option<&PgPool>,
    job: queue::JobRow,
) {
    let (Some(control), Some(cloud)) = (control, cloud.filter(|cloud| cloud.gated().is_some())) else {
        let _ = sqlx::query("UPDATE jobs SET status='done',finished_at=now(),outcome='nothing_to_do',error_message='managed_embedding_unavailable' WHERE id=$1 AND status::text='running'")
            .bind(job.id).execute(pool).await;
        return;
    };
    match crate::managed::embedding::process_statements(tenant, pool, control, &job, cloud).await {
        Ok((report, more)) => {
            if report.blocked > 0 {
                tracing::warn!(job = %job.id, written = report.written, held = report.blocked,
                    discarded_stale = report.discarded_stale,
                    "statement embedding held part of the job; statements retry after their hold");
            }
            if let Err(error) = report.finish(tenant, pool, None).await {
                tracing::warn!(%error, job = %job.id, "statement embedding queue decision unresolved");
                return;
            }
            // More than one job's worth pending: continue under the version
            // key (a lost continuation is picked up by the sweep).
            if more {
                if let Ok(kind @ JobKind::EmbedStatements { statement_version_id, .. }) =
                    serde_json::from_value::<JobKind>(serde_json::json!({"kind":job.kind,"payload":job.payload}))
                {
                    if let Ok(mut conn) = pool.acquire().await {
                        let _ = queue::enqueue_detached(&mut conn, &kind, job.account_id, Some(&key(statement_version_id)), 0).await;
                    }
                }
            }
        }
        Err(error) => {
            tracing::warn!(%error, job = %job.id, "statement embedding leaves job unacknowledged for durable recovery")
        }
    }
}

/// The versions a sweep enqueues: permitted statements without a current
/// vector and not held past now, in graphs that are not archived, without a
/// pending or running job. Current versions first, then oldest.
pub(crate) const CANDIDATES_SQL: &str = r#"
WITH active AS MATERIALIZED (
 SELECT DISTINCT payload->>'statement_version_id' version FROM jobs
 WHERE kind='EmbedStatements' AND status IN ('pending','running')
)
SELECT a.graph_id,a.source_version_id,g.account_id
FROM semantic_atoms a JOIN graphs g ON g.id=a.graph_id AND g.archived_at IS NULL
JOIN source_versions v ON v.graph_id=a.graph_id AND v.version_id=a.source_version_id
JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id AND s.access_state='permitted'
WHERE ($1::uuid IS NULL OR g.account_id=$1)
 AND NOT EXISTS(SELECT 1 FROM semantic_atom_embeddings e WHERE e.graph_id=a.graph_id AND e.semantic_atom_id=a.semantic_atom_id
  AND ((e.embedding IS NOT NULL AND e.embed_model=$2 AND e.statement_sha256=sha256(convert_to(a.statement,'UTF8')))
   OR e.retry_after>now()))
 AND a.source_version_id::text NOT IN (SELECT version FROM active WHERE version IS NOT NULL)
GROUP BY a.graph_id,a.source_version_id,g.account_id
ORDER BY bool_or(s.current_version_id=a.source_version_id) DESC,min(a.created_at),a.source_version_id
LIMIT $3
"#;

async fn candidates(
    conn: &mut sqlx::PgConnection,
    account: Option<Uuid>,
    limit: i64,
) -> Result<Vec<(Uuid, Uuid, Uuid)>, sqlx::Error> {
    sqlx::query_as(CANDIDATES_SQL)
        .bind(account)
        .bind(EMBED_MODEL_CLOUD)
        .bind(limit)
        .fetch_all(conn)
        .await
}

/// Enqueues one job per candidate version, in the same scoped transaction
/// that read them. Returns how many were enqueued.
async fn enqueue_candidates(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: Option<Uuid>,
    limit: i64,
) -> Result<usize, sqlx::Error> {
    let rows = candidates(&mut **tx, account, limit).await?;
    let mut enqueued = 0;
    for (graph_id, version, account_id) in rows {
        let id = queue::enqueue_detached(
            &mut **tx,
            &JobKind::EmbedStatements { graph_id, statement_version_id: version },
            Some(account_id),
            Some(&key(version)),
            0,
        )
        .await?;
        enqueued += usize::from(id.is_some());
    }
    Ok(enqueued)
}

/// The backfill sweep for one plane. The consumer plane reads per account
/// under its RLS scope; an org tenant reads as the tenant system principal so
/// member and team oceans are included.
pub async fn sweep_tick(t: &WorkerTenant, shutdown: &Shutdown) -> Result<Tick, String> {
    if !enabled() {
        return Ok(Tick::idle());
    }
    let mut enqueued = 0usize;
    match &t.tenant {
        TenantId::Org(_) => {
            let run = async {
                let mut tx = begin_tenant_system_scoped_without_account(&t.pool)
                    .await
                    .map_err(|e| e.to_string())?;
                let n = enqueue_candidates(&mut tx, None, SWEEP_VERSIONS).await.map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())?;
                Ok::<_, String>(n)
            };
            match tokio::time::timeout(SWEEP_DB_TIMEOUT, run).await {
                Ok(Ok(n)) => enqueued += n,
                Ok(Err(error)) => tracing::warn!(%error, "statement embedding sweep failed"),
                Err(_) => tracing::warn!("statement embedding sweep timed out; will retry next cycle"),
            }
        }
        TenantId::Consumer => {
            let accounts: Vec<Uuid> = match tokio::time::timeout(
                SWEEP_DB_TIMEOUT,
                sqlx::query_scalar("SELECT id FROM accounts ORDER BY id").fetch_all(&t.pool),
            )
            .await
            {
                Ok(Ok(accounts)) => accounts,
                Ok(Err(error)) => return Err(format!("statement embedding sweep: account roster: {error}")),
                Err(_) => return Ok(Tick::idle()),
            };
            let mut remaining = SWEEP_VERSIONS;
            for account in accounts {
                if remaining <= 0 || shutdown.is_triggered() {
                    break;
                }
                let run = async {
                    let mut tx = begin_account_scoped(&t.pool, account).await.map_err(|e| e.to_string())?;
                    let n = enqueue_candidates(&mut tx, Some(account), remaining).await.map_err(|e| e.to_string())?;
                    tx.commit().await.map_err(|e| e.to_string())?;
                    Ok::<_, String>(n)
                };
                match tokio::time::timeout(SWEEP_DB_TIMEOUT, run).await {
                    Ok(Ok(n)) => {
                        enqueued += n;
                        remaining -= n as i64;
                    }
                    Ok(Err(error)) => tracing::warn!(%error, %account, "statement embedding sweep failed for an account"),
                    Err(_) => tracing::warn!(%account, "statement embedding sweep timed out for an account"),
                }
            }
        }
    }
    if enqueued > 0 {
        tracing::info!(enqueued, "statement embedding sweep enqueued versions");
    }
    Ok(Tick::worked(enqueued))
}
