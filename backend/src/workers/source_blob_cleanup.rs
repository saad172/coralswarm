//! Durable deletion of source blobs after their owning SQL rows disappear.
//! Only server-side deletion transactions enqueue this job. The queue envelope
//! and payload retain ownership; neither source existence nor an LLM is needed.
use std::time::Duration;

use coralswarm_core::r2::{validate_blob_key, R2Error};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    jobs::{
        queue::{self, JobRow},
        JobKind,
    },
    services::blob_store::PromotionBlobStore,
    tenancy::TenantId,
    workers::tenant_harness::{Tick, WorkerTenant},
};

pub(crate) const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

pub async fn tick(
    tenant: &WorkerTenant,
    blobs: Option<&dyn PromotionBlobStore>,
) -> Result<Tick, String> {
    let accounts: Vec<Uuid> = sqlx::query_scalar("SELECT library_pending_purge_accounts(20)")
        .fetch_all(&tenant.pool)
        .await
        .map_err(|_| "evidence purge envelope discovery failed".to_string())?;
    let mut drained = 0i32;
    for account in accounts {
        let mut tx = match &tenant.tenant {
            TenantId::Consumer => {
                crate::tenancy::scope::begin_account_scoped(&tenant.pool, account).await
            }
            TenantId::Org(_) => {
                crate::tenancy::scope::begin_tenant_system_scoped(&tenant.pool, account).await
            }
        }
        .map_err(|_| "evidence purge scope failed".to_string())?;
        let count: i32 = sqlx::query_scalar("SELECT library_drain_purge(100)")
            .fetch_one(&mut *tx)
            .await
            // SQLSTATE only: never row values or locators in worker errors.
            .map_err(|e| format!("evidence purge delivery failed ({})", e.as_database_error().and_then(|d| d.code()).as_deref().unwrap_or("io")))?;
        tx.commit()
            .await
            .map_err(|_| "evidence purge commit failed".to_string())?;
        drained += count;
    }
    let upload_accounts:Vec<Uuid>=sqlx::query_scalar("SELECT library_pending_upload_cleanup_accounts(20)").fetch_all(&tenant.pool).await.map_err(|_|"capture cleanup envelope discovery failed".to_string())?;
    for account in upload_accounts {
        drained+=crate::capture_upload_cleanup::drain_account(&tenant.pool,&tenant.tenant,account,blobs,None).await? as i32;
    }
    let jobs = queue::dequeue(&tenant.pool, &["DeleteSourceBlob"], 20)
        .await
        .map_err(|_| "source blob cleanup dequeue failed".to_string())?;
    if jobs.is_empty() {
        return Ok(if drained > 0 {
            Tick::worked(drained as usize)
        } else {
            Tick::idle()
        });
    }
    let count = jobs.len();
    for job in jobs {
        process_job(&tenant.tenant, &tenant.pool, blobs, job).await;
    }
    Ok(Tick::worked(count))
}

fn key_matches_owner(tenant: &TenantId, graph_id: Uuid, org_id: Option<&str>, key: &str) -> bool {
    if validate_blob_key(key).is_err() {
        return false;
    }
    let parts: Vec<_> = key.split('/').collect();
    let graph = graph_id.to_string();
    match (tenant, org_id, parts.as_slice()) {
        (TenantId::Consumer, None, [key_graph, _]) => *key_graph == graph,
        (TenantId::Org(tenant_org), Some(org), [key_org, key_graph, _]) => {
            tenant_org == org && *key_org == org && *key_graph == graph
        }
        _ => false,
    }
}

/// Public for queue integration tests using a fake blob transport. Errors are
/// sanitized categories; encrypted keys and source text never enter logs.
pub async fn process_job(
    tenant: &TenantId,
    pool: &PgPool,
    blobs: Option<&dyn PromotionBlobStore>,
    job: JobRow,
) {
    let parsed = serde_json::from_value::<JobKind>(json!({"kind":job.kind,"payload":job.payload}));
    let (graph_id, account_id, org_id, blob_key) = match parsed {
        Ok(JobKind::DeleteSourceBlob {
            graph_id,
            account_id,
            org_id,
            blob_key,
        }) => (graph_id, account_id, org_id, blob_key),
        _ => {
            let _ = queue::could_not_do(
                pool,
                job.id,
                queue::FailureClass::Structural,
                "invalid source blob cleanup job",
            )
            .await;
            return;
        }
    };
    // No bootstrap fallback for unstamped jobs: the graph may already be gone.
    if job.account_id != Some(account_id)
        || !key_matches_owner(tenant, graph_id, org_id.as_deref(), &blob_key)
    {
        let _ = queue::could_not_do(
            pool,
            job.id,
            queue::FailureClass::Structural,
            "source blob cleanup ownership mismatch",
        )
        .await;
        return;
    }
    // The retained envelope authorizes deletion after graph removal. If it
    // still exists, independently reject a mismatched live owner as well.
    match sqlx::query_scalar::<_, Option<Uuid>>("SELECT resolve_graph_account_id($1)")
        .bind(graph_id)
        .fetch_one(pool)
        .await
    {
        Ok(Some(owner)) if owner != account_id => {
            let _ = queue::could_not_do(
                pool,
                job.id,
                queue::FailureClass::Structural,
                "source blob cleanup owner mismatch",
            )
            .await;
            return;
        }
        Ok(_) => {}
        Err(_) => {
            let _ = queue::fail(pool, job.id, "source blob cleanup owner lookup failed").await;
            return;
        }
    }
    let Some(blobs) = blobs else {
        let _ = queue::fail(pool, job.id, "source blob transport unavailable").await;
        return;
    };
    match blobs.delete_blob(&blob_key).await {
        Ok(()) | Err(R2Error::NotFound(_)) => {
            let _ = queue::complete(pool, job.id, queue::JobOutcome::DidWork).await;
        }
        Err(_) => {
            let _ = queue::fail(pool, job.id, "source blob deletion failed").await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cleanup_keys_must_match_the_exact_graph_and_tenant_plane() {
        let graph = Uuid::new_v4();
        let blob = Uuid::new_v4();
        let personal = format!("{graph}/{blob}");
        let org = format!("org_alpha/{graph}/{blob}");
        assert!(key_matches_owner(
            &TenantId::Consumer,
            graph,
            None,
            &personal
        ));
        assert!(!key_matches_owner(
            &TenantId::Consumer,
            Uuid::new_v4(),
            None,
            &personal
        ));
        assert!(!key_matches_owner(
            &TenantId::Consumer,
            graph,
            Some("org_alpha"),
            &org
        ));
        assert!(key_matches_owner(
            &TenantId::Org("org_alpha".into()),
            graph,
            Some("org_alpha"),
            &org
        ));
        assert!(!key_matches_owner(
            &TenantId::Org("org_beta".into()),
            graph,
            Some("org_alpha"),
            &org
        ));
        assert!(!key_matches_owner(
            &TenantId::Org("org_alpha".into()),
            graph,
            None,
            &personal
        ));
        assert!(!key_matches_owner(
            &TenantId::Consumer,
            graph,
            None,
            &format!("{graph}/../{blob}")
        ));
    }
}
