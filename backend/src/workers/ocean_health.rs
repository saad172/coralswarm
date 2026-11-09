//! OceanHealth queue worker (G3).
//!
//! Drains `OceanHealth { graph_id }` jobs, computes the rolling 7-day window,
//! logs paging alarms, and writes `HealthViolation` job rows (one per graph /
//! alarm / ISO week). On an idle tick, enqueues a check for each ocean that
//! has not been checked in [`CHECK_EVERY`].

use std::collections::HashSet;
use std::time::Duration;

use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::queue::JobOutcome;
use crate::jobs::queue::{self, JobRow};
use crate::jobs::JobKind;
use crate::ocean_health::{self, evaluate, record_violations, rolling_window, to_json};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::embed::JobScope;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::workers::verify_job_scope;

pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_secs(5);
const JOB_BATCH_SIZE: usize = 8;
const CHECK_EVERY: Duration = Duration::from_secs(6 * 3600);

pub async fn tick(t: &WorkerTenant) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["OceanHealth"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        let scheduled = schedule_due(t).await?;
        if scheduled == 0 {
            return Ok(Tick::idle());
        }
        return Ok(Tick::worked(scheduled));
    }

    let n = jobs.len();
    for job in jobs {
        process_job(&t.tenant, &t.pool, job).await;
    }
    Ok(Tick::worked(n))
}

/// The consumer plane's `graphs` policy reads `app.account_id` strictly, so
/// its graphs are listed one account scope at a time. `jobs` carries no RLS
/// and is read once for every plane.
async fn schedule_due(t: &WorkerTenant) -> Result<usize, String> {
    let pool = &t.pool;
    let graphs: Vec<(Uuid, Option<Uuid>)> = match &t.tenant {
        TenantId::Consumer => {
            let accounts: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
                .fetch_all(pool)
                .await
                .map_err(|e| format!("due-graph account list failed: {e}"))?;
            let mut graphs = Vec::new();
            for account in accounts {
                let mut tx = begin_account_scoped(pool, account)
                    .await
                    .map_err(|e| format!("due-graph scope open failed: {e}"))?;
                let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs")
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| format!("due-graph query failed: {e}"))?;
                let _ = tx.rollback().await;
                graphs.extend(ids.into_iter().map(|id| (id, Some(account))));
            }
            graphs
        }
        TenantId::Org(_) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool)
                .await
                .map_err(|e| format!("due-graph scope open failed: {e}"))?;
            let graphs = sqlx::query_as("SELECT id, resolve_graph_account_id(id) FROM graphs")
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| format!("due-graph query failed: {e}"))?;
            let _ = tx.rollback().await;
            graphs
        }
    };

    let checked: HashSet<String> = sqlx::query_scalar(
        "SELECT DISTINCT payload->>'graph_id' FROM jobs \
          WHERE kind = 'OceanHealth' \
            AND (status IN ('pending', 'running') \
                 OR (status = 'done' \
                     AND finished_at > now() - ($1::bigint * interval '1 second'))) \
            AND payload->>'graph_id' IS NOT NULL",
    )
    .bind(CHECK_EVERY.as_secs() as i64)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("due-graph job query failed: {e}"))?
    .into_iter()
    .collect();
    let due = graphs
        .into_iter()
        .filter(|(graph_id, _)| !checked.contains(&graph_id.to_string()));

    let mut n = 0usize;
    for (graph_id, account_id) in due {
        let key = format!("ocean-health:{graph_id}");
        let kind = JobKind::OceanHealth { graph_id };
        match queue::enqueue(pool, &kind, account_id, Some(&key), 0).await {
            Ok(Some(_)) => n += 1,
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(%graph_id, error = %e, "ocean-health: enqueue check failed");
            }
        }
    }
    Ok(n)
}

pub async fn process_job(tenant: &TenantId, pool: &PgPool, job: JobRow) {
    let graph_id = job
        .payload
        .get("graph_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Uuid>().ok());
    let Some(graph_id) = graph_id else {
        tracing::error!(job_id = %job.id, "ocean-health: malformed payload (missing graph_id)");
        let _ = queue::fail(pool, job.id, "malformed payload: missing graph_id").await;
        return;
    };

    let Some(account_id) = verify_job_scope(pool, &job, graph_id, "ocean-health").await else {
        return;
    };

    let (start, end) = rolling_window(Utc::now());
    let read = match read_scoped(tenant, pool, account_id, graph_id, start, end).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(job_id = %job.id, %graph_id, error = %e, "ocean-health: read failed");
            let _ = queue::fail(pool, job.id, &format!("read failed: {e}")).await;
            return;
        }
    };
    let metrics = ocean_health::finish(read).await;
    let paging = evaluate(&metrics);
    for alarm in &paging {
        tracing::error!(
            %graph_id,
            alarm = alarm.kind.as_str(),
            value = alarm.value,
            threshold = alarm.threshold,
            "ocean-health: paging alarm"
        );
    }
    if paging.is_empty() {
        tracing::info!(%graph_id, new_reefs = metrics.new_reefs, "ocean-health: window ok");
    }

    if let Err(e) = record_violations(pool, Some(account_id), &metrics, &paging).await {
        tracing::error!(job_id = %job.id, error = %e, "ocean-health: record_violations failed");
        let _ = queue::fail(pool, job.id, &e).await;
        return;
    }

    let outcome = if paging.is_empty() {
        JobOutcome::NothingToDo
    } else {
        JobOutcome::DidWork
    };
    if let Err(e) =
        queue::complete_with_payload(pool, job.id, outcome, to_json(&metrics, &paging)).await
    {
        tracing::error!(job_id = %job.id, error = %e, "ocean-health: complete failed");
    }
}

async fn read_scoped(
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
) -> Result<ocean_health::WindowRead, String> {
    let scope = JobScope::for_job(tenant, pool, account_id);
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, account_id)
                .await
                .map_err(|e| format!("scope open failed: {e}"))?;
            let read = ocean_health::read_window(&mut tx, graph_id, start, end).await;
            let _ = tx.rollback().await;
            read.map_err(|e| e.to_string())
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool)
                .await
                .map_err(|e| format!("scope open failed: {e}"))?;
            let read = ocean_health::read_window(&mut tx, graph_id, start, end).await;
            let _ = tx.rollback().await;
            read.map_err(|e| e.to_string())
        }
    }
}
