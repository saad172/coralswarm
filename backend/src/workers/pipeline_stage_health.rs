//! Measures every knowledge-pipeline stage on each plane, per source type, and
//! pages when a stage that should flow has stopped. See
//! [`crate::pipeline_stage_health`].
//!
//! Each tick reads one plane, writes its [`StageReport`] to the control table
//! `pipeline_stage_health`, and logs `alert = true` (event `pipeline_*`) for
//! every finding, at most once an hour per finding in this process. A tick
//! that cannot read the whole plane writes nothing and logs `alert = true`
//! itself: an unmeasured plane must not read as a healthy one.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::capture_admission_health::{accounts_processing_off, CONSUMER_TENANT};
use crate::pipeline_stage_health::{
    evaluate, harden, measure_credit_holds, measure_dead_jobs, measure_scope, Finding, PlaneAccumulator,
    StageReport, Switches, Thresholds,
};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

pub const INTERVAL: Duration = Duration::from_secs(15 * 60);
const ALERT_EVERY: Duration = Duration::from_secs(60 * 60);
const PLANE_TIMEOUT: Duration = Duration::from_secs(180);

static LAST_ALERT: LazyLock<Mutex<HashMap<String, Instant>>> = LazyLock::new(Default::default);

pub async fn tick(t: &WorkerTenant, control: &PgPool) -> Result<Tick, String> {
    let th = Thresholds::from_env();
    let measured = tokio::time::timeout(PLANE_TIMEOUT, measure(t, control, Utc::now(), &th))
        .await
        .map_err(|_| "pipeline stage health: plane read timed out".to_string())
        .and_then(|r| r);
    let report = match measured {
        Ok(report) => report,
        Err(error) => {
            tracing::error!(alert = true, event = "pipeline_stage_health_unmeasured", tenant = %tenant_key(&t.tenant),
                %error, "pipeline stage health could not measure this plane");
            return Err(error);
        }
    };
    store(control, &report).await?;
    page(&evaluate(&report, &th));
    Ok(Tick { processed: 0, defer: None })
}

fn tenant_key(t: &TenantId) -> String {
    match t {
        TenantId::Consumer => CONSUMER_TENANT.to_string(),
        TenantId::Org(org) => org.clone(),
    }
}

/// Reads one plane. The consumer plane is RLS-scoped per account; an org
/// tenant is read once as the tenant system actor. Accounts that switched
/// processing off are not measured: their stages are not expected to flow.
pub async fn measure(
    t: &WorkerTenant,
    control: &PgPool,
    now: DateTime<Utc>,
    th: &Thresholds,
) -> Result<StageReport, String> {
    let err = |e: crate::store::StoreError| format!("pipeline stage health: {e}");
    let sql = |e: sqlx::Error| format!("pipeline stage health: {e}");
    let mut acc = PlaneAccumulator::default();
    let windows = th.windows();
    let mut credit_scopes: Vec<Uuid> = Vec::new();
    let dead;
    let (tenant, plane, skipped) = match &t.tenant {
        TenantId::Consumer => {
            let accounts: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
                .fetch_all(&t.pool)
                .await
                .map_err(sql)?;
            let off = {
                let mut c = control.acquire().await.map_err(sql)?;
                accounts_processing_off(&mut c, &accounts).await.map_err(sql)?
            };
            for account in accounts.iter().copied().filter(|a| !off.contains(a)) {
                let mut tx = begin_account_scoped(&t.pool, account).await.map_err(err)?;
                harden(&mut tx).await.map_err(sql)?;
                let graph_ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs WHERE account_id = $1 ORDER BY id")
                    .bind(account)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(sql)?;
                // An account with nothing captured in the baseline window has
                // no stage that should be flowing; one indexed probe skips it.
                let active: bool = !graph_ids.is_empty()
                    && sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM atoms WHERE graph_id = ANY($1) AND created_at >= $2)")
                        .bind(&graph_ids)
                        .bind(now - chrono::Duration::hours(th.baseline_hours))
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(sql)?;
                if active {
                    acc.add(measure_scope(&mut tx, &graph_ids, &[account], now, &windows, th).await.map_err(sql)?);
                    credit_scopes.push(account);
                }
                tx.rollback().await.map_err(sql)?;
            }
            // `jobs` carries no RLS: one plane-wide read.
            let mut tx = t.pool.begin().await.map_err(sql)?;
            harden(&mut tx).await.map_err(sql)?;
            dead = measure_dead_jobs(&mut tx, now, &windows).await.map_err(sql)?;
            tx.rollback().await.map_err(sql)?;
            (CONSUMER_TENANT.to_string(), "consumer".to_string(), off.len() as i64)
        }
        TenantId::Org(org) => {
            let mut tx = begin_tenant_system_scoped_without_account(&t.pool).await.map_err(err)?;
            harden(&mut tx).await.map_err(sql)?;
            let rows: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT id, account_id FROM graphs ORDER BY id")
                .fetch_all(&mut *tx)
                .await
                .map_err(sql)?;
            let mut accounts: Vec<Uuid> = rows.iter().map(|(_, a)| *a).collect();
            accounts.sort_unstable();
            accounts.dedup();
            let off = {
                let mut c = control.acquire().await.map_err(sql)?;
                accounts_processing_off(&mut c, &accounts).await.map_err(sql)?
            };
            let graph_ids: Vec<Uuid> = rows.iter().filter(|(_, a)| !off.contains(a)).map(|(g, _)| *g).collect();
            let measured: Vec<Uuid> = accounts.iter().copied().filter(|a| !off.contains(a)).collect();
            if !graph_ids.is_empty() {
                acc.add(measure_scope(&mut tx, &graph_ids, &measured, now, &windows, th).await.map_err(sql)?);
            }
            // `jobs` carries no RLS; the whole tenant database is this plane.
            dead = measure_dead_jobs(&mut tx, now, &windows).await.map_err(sql)?;
            tx.rollback().await.map_err(sql)?;
            credit_scopes = measured;
            (org.clone(), format!("org:{org}"), off.len() as i64)
        }
    };
    let mut report = acc.finish(tenant, now, th, &windows, dead, skipped, Some(Switches::current()));
    // Control-plane credit holds, per account, for this plane only.
    for account in credit_scopes {
        let mut tx = begin_account_scoped(control, account).await.map_err(err)?;
        harden(&mut tx).await.map_err(sql)?;
        let hold = measure_credit_holds(&mut tx, account, &plane, now, th).await.map_err(sql)?;
        tx.rollback().await.map_err(sql)?;
        if let Some(hold) = hold {
            report.snapshot.add_hold(hold);
        }
    }
    Ok(report)
}

pub async fn store(control: &PgPool, report: &StageReport) -> Result<(), String> {
    let json = serde_json::to_value(report).map_err(|e| e.to_string())?;
    sqlx::query(
        "INSERT INTO pipeline_stage_health(tenant,measured_at,report) VALUES($1,$2,$3) \
         ON CONFLICT(tenant) DO UPDATE SET measured_at=EXCLUDED.measured_at,report=EXCLUDED.report",
    )
    .bind(&report.tenant)
    .bind(report.measured_at)
    .bind(json)
    .execute(control)
    .await
    .map(|_| ())
    .map_err(|e| format!("pipeline stage health: store failed: {e}"))
}

/// Logs every finding with `alert = true`, at most once an hour per finding.
pub fn page(findings: &[Finding]) {
    let now = Instant::now();
    let mut last = LAST_ALERT.lock().unwrap_or_else(|p| p.into_inner());
    for f in findings {
        let key = f.key();
        if last.get(&key).is_some_and(|at| now.duration_since(*at) < ALERT_EVERY) {
            continue;
        }
        last.insert(key, now);
        tracing::error!(
            alert = true,
            event = %f.event,
            tenant = %f.tenant,
            source_type = f.source_type.as_deref().unwrap_or("*"),
            window_hours = f.window_hours,
            detail = %f.detail,
            "{}",
            f.message
        );
    }
}
