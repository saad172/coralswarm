//! Measures capture-to-Atom health on each plane and pages on stalled
//! captures (COR-71). See [`crate::capture_admission_health`].
//!
//! Each tick reads one plane, writes its [`PlaneReport`] to the control table
//! `capture_admission_health`, and logs `alert = true` for every stalled
//! group, at most once an hour per group in this process. A tick that cannot
//! read the whole plane writes nothing: the report then goes stale and
//! `/internal/admin/chain-health` reports the plane as stale.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::capture_admission_health::{
    accounts_processing_off, classify, clock_coverage, lag, progress, waiting, PlaneReport, ClockClass, CONSUMER_TENANT,
};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

pub const INTERVAL: Duration = Duration::from_secs(5 * 60);
const ALERT_EVERY: Duration = Duration::from_secs(60 * 60);
const PLANE_TIMEOUT: Duration = Duration::from_secs(120);

static LAST_ALERT: LazyLock<Mutex<HashMap<String, Instant>>> = LazyLock::new(Default::default);

pub async fn tick(t: &WorkerTenant, control: &PgPool) -> Result<Tick, String> {
    let report = tokio::time::timeout(PLANE_TIMEOUT, measure(t, control, Utc::now()))
        .await
        .map_err(|_| "capture admission health: plane read timed out".to_string())??;
    store(control, &report).await?;
    page(&report);
    Ok(Tick { processed: 0, defer: None })
}

/// Reads one plane. The consumer plane is RLS-scoped per account; an org
/// tenant holds one account and is read as the tenant system actor.
pub async fn measure(
    t: &WorkerTenant,
    control: &PgPool,
    now: DateTime<Utc>,
) -> Result<PlaneReport, String> {
    let err = |e: crate::store::StoreError| format!("capture admission health: {e}");
    let sql = |e: sqlx::Error| format!("capture admission health: {e}");
    let mut rows = Vec::new();
    let mut lags = Vec::new();
    let mut coverage = Vec::new();
    let mut moved = Vec::new();
    let tenant = match &t.tenant {
        TenantId::Consumer => {
            let accounts: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
                .fetch_all(&t.pool)
                .await
                .map_err(sql)?;
            for account in accounts {
                let mut tx = begin_account_scoped(&t.pool, account).await.map_err(err)?;
                rows.extend(waiting(&mut tx, Some(account), now).await.map_err(sql)?);
                coverage.extend(clock_coverage(&mut tx,Some(account)).await.map_err(sql)?);
                lags.extend(lag(&mut tx, Some(account), now).await.map_err(sql)?);
                moved.extend(progress(&mut tx, Some(account), now).await.map_err(sql)?);
                tx.rollback().await.map_err(sql)?;
            }
            CONSUMER_TENANT.to_string()
        }
        TenantId::Org(org) => {
            let mut tx = begin_tenant_system_scoped_without_account(&t.pool).await.map_err(err)?;
            rows = waiting(&mut tx, None, now).await.map_err(sql)?;
            coverage=clock_coverage(&mut tx,None).await.map_err(sql)?;
            lags = lag(&mut tx, None, now).await.map_err(sql)?;
            moved = progress(&mut tx, None, now).await.map_err(sql)?;
            tx.rollback().await.map_err(sql)?;
            org.clone()
        }
    };
    let mut accounts: Vec<Uuid> = rows.iter().map(|r| r.account_id).collect();
    accounts.extend(coverage.iter().map(|row|row.account_id));
    accounts.sort_unstable();
    accounts.dedup();
    let mut control = control.acquire().await.map_err(sql)?;
    let off = accounts_processing_off(&mut control, &accounts).await.map_err(sql)?;
    for row in &mut coverage { row.processing_off=off.contains(&row.account_id); }
    lags.retain(|row|coverage.iter().any(|candidate|candidate.graph_id==row.graph_id && !candidate.processing_off));
    Ok(PlaneReport { tenant, measured_at: now, lag: lags, waiting: classify(rows, &off),clock_coverage:Some(coverage), progress: Some(moved) })
}

pub async fn store(control: &PgPool, report: &PlaneReport) -> Result<(), String> {
    let json = serde_json::to_value(report).map_err(|e| e.to_string())?;
    sqlx::query(
        "INSERT INTO capture_admission_health(tenant,measured_at,report) VALUES($1,$2,$3) \
         ON CONFLICT(tenant) DO UPDATE SET measured_at=EXCLUDED.measured_at,report=EXCLUDED.report",
    )
    .bind(&report.tenant)
    .bind(report.measured_at)
    .bind(json)
    .execute(control)
    .await
    .map(|_| ())
    .map_err(|e| format!("capture admission health: store failed: {e}"))
}

fn page(report: &PlaneReport) {
    let now = Instant::now();
    let mut last = LAST_ALERT.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(coverage)=&report.clock_coverage {
        for row in coverage.iter().filter(|row|row.class==ClockClass::Unknown && row.captures>0) {
            let key=format!("clock/{}/{}/{:?}",report.tenant,row.graph_id,row.hold_basis);
            if last.get(&key).is_some_and(|at|now.duration_since(*at)<ALERT_EVERY) { continue; }
            last.insert(key,now);
            tracing::warn!(alert=true,event="managed_clock_unknown",tenant=%report.tenant,graph_id=%row.graph_id,basis=?row.hold_basis,captures=row.captures,processed=row.processed,"capture candidate clock history is unknown; eligible SLO is not established");
        }
    }
    for row in report.lag.iter().filter(|row|!row.slo_met) {
        let key=format!("lag/{}/{}",report.tenant,row.graph_id);
        if last.get(&key).is_some_and(|at|now.duration_since(*at)<ALERT_EVERY) { continue; }
        last.insert(key,now);
        tracing::warn!(alert=true,event="managed_lag_slo",tenant=%report.tenant,graph_id=%row.graph_id,p95_secs=row.p95_secs,p99_secs=row.p99_secs,max_secs=row.max_secs,"never-held capture lag exceeds the accepted SLO");
    }

    for row in report.waiting.iter().filter(|row|row.disposition==crate::capture_admission_health::Disposition::Stalled && row.state=="queued" && row.never_held && row.live && row.over_lag_age>0) {
        let key=format!("queued-lag/{}/{}",report.tenant,row.graph_id);
        if last.get(&key).is_some_and(|at|now.duration_since(*at)<ALERT_EVERY) { continue; }
        last.insert(key,now);
        tracing::warn!(alert=true,event="managed_lag_slo",tenant=%report.tenant,graph_id=%row.graph_id,captures=row.over_lag_age,"never-held queued captures exceed sixty minutes");
    }
    for w in report.stalled() {
        let reason = w.reason.as_deref().unwrap_or("");
        let key = format!("{}/{}/{}/{reason}", report.tenant, w.graph_id, w.state);
        if last.get(&key).is_some_and(|at| now.duration_since(*at) < ALERT_EVERY) {
            continue;
        }
        last.insert(key, now);
        tracing::error!(
            alert = true,
            event = "capture_admission_stalled",
            tenant = %report.tenant,
            graph_id = %w.graph_id,
            state = %w.state,
            reason,
            captures = w.captures,
            over_alert_age = w.over_alert_age,
            oldest_wait_minutes = (report.measured_at - w.oldest_captured_at).num_minutes(),
            "captures have waited more than 15 minutes to become Atoms and nothing is moving them"
        );
    }
}
