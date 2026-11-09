use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use sqlx::postgres::{PgArguments, PgConnection};
use sqlx::query::Query;
use sqlx::{Connection, Postgres, Row};
use uuid::Uuid;

use super::tenant_harness::{HarnessConfig, Tick, WorkerTenant};

pub const SWEPT_KINDS: [&str; 8] = [
    "GraphMessageIngest",
    "RegenerateDigest",
    "IngestDoor",
    "EmbedStatements",
    "EmbedAtoms",
    "GithubEventIngest",
    "SlackMessageIngest",
    "OceanHealth",
];

pub const BATCH_SIZE: i64 = 5_000;
pub const TICK_BUDGET: Duration = Duration::from_secs(20);
pub const HANG_GRACE: Duration = Duration::from_secs(5);
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub const MIN_WINDOW_DAYS: i32 = 7;

pub const ENABLED_ENV: &str = "JOBS_RETENTION_ENABLED";
pub const DONE_DAYS_ENV: &str = "JOBS_RETENTION_DONE_DAYS";
pub const DEAD_DAYS_ENV: &str = "JOBS_RETENTION_DEAD_DAYS";

pub fn harness_config() -> HarnessConfig {
    HarnessConfig { error_sleep: SWEEP_INTERVAL, ..HarnessConfig::sweep_worker("jobs_retention", SWEEP_INTERVAL) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    pub done_days: i32,
    pub dead_days: i32,
}

impl Policy {
    pub const DEFAULT: Policy = Policy { done_days: 30, dead_days: 14 };

    pub fn from_env() -> Result<Policy, String> {
        Ok(Policy {
            done_days: window_days(DONE_DAYS_ENV, std::env::var(DONE_DAYS_ENV).ok(), Self::DEFAULT.done_days)?,
            dead_days: window_days(DEAD_DAYS_ENV, std::env::var(DEAD_DAYS_ENV).ok(), Self::DEFAULT.dead_days)?,
        })
    }

    fn days(self, status: Status) -> i32 {
        match status {
            Status::Done => self.done_days,
            Status::Dead => self.dead_days,
        }
    }
}

fn window_days(key: &str, raw: Option<String>, default: i32) -> Result<i32, String> {
    let Some(raw) = raw else { return Ok(default) };
    let days: i32 = raw.trim().parse().map_err(|_| format!("{key}={raw:?} is not a whole number of days"))?;
    if days < MIN_WINDOW_DAYS {
        return Err(format!("{key}={days} is below the {MIN_WINDOW_DAYS}-day minimum"));
    }
    Ok(days)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Status {
    Done,
    Dead,
}

impl Status {
    pub const ALL: [Status; 2] = [Status::Done, Status::Dead];

    pub fn as_str(self) -> &'static str {
        match self {
            Status::Done => "done",
            Status::Dead => "dead",
        }
    }

    fn statements(self) -> &'static Statements {
        match self {
            Status::Done => &DONE,
            Status::Dead => &DEAD,
        }
    }
}

pub type Target = (&'static str, Status);

pub fn targets() -> impl Iterator<Item = Target> {
    SWEPT_KINDS.into_iter().flat_map(|kind| Status::ALL.into_iter().map(move |status| (kind, status)))
}

struct Statements {
    count: &'static str,
    lock: &'static str,
    delete: &'static str,
}

macro_rules! aged {
    ($status:literal) => {
        concat!(
            "j.kind = $1 AND j.status = '",
            $status,
            "' AND j.finished_at < now() - make_interval(days => $2::int)"
        )
    };
}

macro_rules! older_than_the_newest_100_done {
    () => {
        " AND j.finished_at < ( \
             SELECT k.finished_at FROM jobs k \
             WHERE k.kind = $1 AND k.status = 'done' AND k.finished_at IS NOT NULL \
             ORDER BY k.finished_at DESC OFFSET 99 LIMIT 1)"
    };
}

macro_rules! not_the_newest_did_work {
    () => {
        " AND (j.outcome IS DISTINCT FROM 'did_work' OR j.finished_at < ( \
             SELECT max(w.finished_at) FROM jobs w \
             WHERE w.kind = $1 AND w.status = 'done' AND w.outcome = 'did_work'))"
    };
}

macro_rules! unlinked {
    () => {
        " AND NOT EXISTS (SELECT 1 FROM job_dependencies d WHERE d.parent_id = j.id) \
          AND NOT EXISTS (SELECT 1 FROM job_dependencies d WHERE d.child_id = j.id)"
    };
}

macro_rules! statements {
    ($($eligible:tt)+) => {
        Statements {
            count: concat!("SELECT count(*) FROM jobs j WHERE ", $($eligible)+, unlinked!()),
            lock: concat!(
                "SELECT j.id FROM jobs j WHERE ",
                $($eligible)+,
                unlinked!(),
                " ORDER BY j.finished_at LIMIT $3 FOR UPDATE OF j SKIP LOCKED"
            ),
            delete: concat!("DELETE FROM jobs j WHERE j.id = ANY($3) AND ", $($eligible)+, unlinked!()),
        }
    };
}

const DONE: Statements = statements!(aged!("done"), older_than_the_newest_100_done!(), not_the_newest_did_work!());
const DEAD: Statements = statements!(aged!("dead"));

fn bind_target(sql: &str, (kind, status): Target, policy: Policy) -> Query<'_, Postgres, PgArguments> {
    sqlx::query(sql).bind(kind).bind(policy.days(status))
}

pub async fn count_eligible(conn: &mut PgConnection, policy: Policy) -> Result<BTreeMap<Target, i64>, sqlx::Error> {
    let mut tx = conn.begin().await?;
    sqlx::raw_sql("SET TRANSACTION READ ONLY; SET LOCAL plan_cache_mode = force_generic_plan").execute(&mut *tx).await?;
    let mut counts = BTreeMap::new();
    for target in targets() {
        let row = bind_target(target.1.statements().count, target, policy).fetch_one(&mut *tx).await?;
        counts.insert(target, row.try_get(0)?);
    }
    tx.commit().await?;
    Ok(counts)
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub batch_size: i64,
    pub budget: Duration,
}

impl Limits {
    pub const TICK: Limits = Limits { batch_size: BATCH_SIZE, budget: TICK_BUDGET };
}

#[derive(Debug, Default)]
pub struct SweepReport {
    pub deleted: BTreeMap<Target, u64>,
    pub failed: Vec<(Target, String)>,
    pub budget_exhausted: bool,
    /// A batch outlived the budget plus [`HANG_GRACE`] with no answer from the server.
    pub hung: bool,
    pub batches: u32,
    pub slowest_batch: Duration,
}

impl SweepReport {
    pub fn total_deleted(&self) -> u64 {
        self.deleted.values().sum()
    }
}

pub async fn sweep_once(conn: &mut PgConnection, policy: Policy, limits: Limits) -> SweepReport {
    let deadline = Instant::now() + limits.budget;
    let hang_at = tokio::time::Instant::from_std(deadline + HANG_GRACE);
    let mut report = SweepReport::default();
    'targets: for (kind, status) in targets() {
        loop {
            if Instant::now() >= deadline {
                report.budget_exhausted = true;
                break 'targets;
            }
            let started = Instant::now();
            let result =
                tokio::time::timeout_at(hang_at, delete_batch(conn, (kind, status), policy, limits.batch_size, deadline))
                    .await;
            let elapsed = started.elapsed();
            report.batches += 1;
            report.slowest_batch = report.slowest_batch.max(elapsed);
            let ms = elapsed.as_millis() as u64;
            match result {
                Err(_) => {
                    tracing::warn!(kind, status = status.as_str(), ms, "jobs-retention: batch hung past the budget");
                    report.budget_exhausted = true;
                    report.hung = true;
                    break 'targets;
                }
                Ok(Ok(batch)) => {
                    tracing::debug!(kind, status = status.as_str(), locked = batch.locked, deleted = batch.deleted, ms, "jobs-retention: batch");
                    *report.deleted.entry((kind, status)).or_default() += batch.deleted;
                    if batch.locked < limits.batch_size as usize {
                        break;
                    }
                }
                Ok(Err(e)) if is_statement_timeout(&e) => {
                    tracing::debug!(kind, status = status.as_str(), ms, "jobs-retention: batch cancelled at the budget");
                    report.budget_exhausted = true;
                    break 'targets;
                }
                Ok(Err(e)) => {
                    report.failed.push(((kind, status), e.to_string()));
                    break;
                }
            }
        }
    }
    report
}

struct Batch {
    locked: usize,
    deleted: u64,
}

/// Locks up to `batch_size` eligible rows, then deletes those still eligible
/// under a snapshot taken after the locks.
async fn delete_batch(
    conn: &mut PgConnection,
    target: Target,
    policy: Policy,
    batch_size: i64,
    deadline: Instant,
) -> Result<Batch, sqlx::Error> {
    let statements = target.1.statements();
    let mut tx = conn.begin_with("BEGIN ISOLATION LEVEL READ COMMITTED").await?;
    sqlx::query("SELECT set_config('plan_cache_mode', 'force_generic_plan', true), set_config('statement_timeout', $1, true)")
        .bind(time_left(deadline))
        .execute(&mut *tx)
        .await?;
    let ids: Vec<Uuid> = bind_target(statements.lock, target, policy)
        .bind(batch_size)
        .fetch_all(&mut *tx)
        .await?
        .iter()
        .map(|row| row.try_get(0))
        .collect::<Result<_, _>>()?;
    if ids.is_empty() {
        tx.commit().await?;
        return Ok(Batch { locked: 0, deleted: 0 });
    }
    cap_statement(&mut tx, deadline).await?;
    let deleted = bind_target(statements.delete, target, policy)
        .bind(&ids)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(Batch { locked: ids.len(), deleted })
}

async fn cap_statement(tx: &mut sqlx::Transaction<'_, Postgres>, deadline: Instant) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('statement_timeout', $1, true)").bind(time_left(deadline)).execute(&mut **tx).await?;
    Ok(())
}

fn time_left(deadline: Instant) -> String {
    format!("{}ms", deadline.saturating_duration_since(Instant::now()).as_millis().max(1))
}

fn is_statement_timeout(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("57014"))
}

pub async fn tick(t: &WorkerTenant, policy: Policy) -> Result<Tick, String> {
    tick_with(t, policy, Limits::TICK).await
}

pub async fn tick_with(t: &WorkerTenant, policy: Policy, limits: Limits) -> Result<Tick, String> {
    let mut conn = t.pool.acquire().await.map_err(|e| format!("jobs-retention: no connection: {e}"))?;
    let report = sweep_once(&mut conn, policy, limits).await;
    if report.hung {
        drop(conn.detach());
    }
    tracing::info!(
        tenant = ?t.tenant,
        deleted = report.total_deleted(),
        batches = report.batches,
        slowest_batch_ms = report.slowest_batch.as_millis() as u64,
        budget_exhausted = report.budget_exhausted,
        hung = report.hung,
        failed = report.failed.len(),
        "jobs-retention: swept"
    );
    if report.failed.is_empty() {
        return Ok(Tick::idle());
    }
    let failures: Vec<String> =
        report.failed.iter().map(|((kind, status), e)| format!("{kind} {}: {e}", status.as_str())).collect();
    Err(format!("jobs-retention: batch failed: {}", failures.join("; ")))
}
