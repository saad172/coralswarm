//! Causal job tracking. A shared, debounced job can belong to many promotions.
use sqlx::{PgPool, Postgres, Transaction};
use std::cell::Cell;
use std::future::Future;
use uuid::Uuid;

tokio::task_local! { static PARENT: (Uuid, Cell<bool>); }
pub async fn scope<T>(id: Uuid, work: impl Future<Output = T>) -> T {
    PARENT.scope((id, Cell::new(false)), work).await
}
pub fn parent() -> Option<Uuid> {
    PARENT.try_with(|p| p.0).ok()
}
pub fn enqueue_failed() {
    let _ = PARENT.try_with(|p| p.1.set(true));
}
pub fn has_enqueue_failure(id: Uuid) -> bool {
    PARENT.try_with(|p| p.0 == id && p.1.get()).unwrap_or(false)
}
pub async fn link(tx: &mut Transaction<'_, Postgres>, child: Uuid) -> Result<(), sqlx::Error> {
    if let Some(parent) = parent() {
        sqlx::query("INSERT INTO job_dependencies(parent_id, child_id) SELECT p.id, c.id FROM jobs p, jobs c WHERE p.id=$1 AND c.id=$2 AND p.account_id=c.account_id AND p.id<>c.id ON CONFLICT DO NOTHING")
            .bind(parent).bind(child).execute(&mut **tx).await?;
    }
    Ok(())
}

/// The message [`crate::jobs::queue::complete`] records on a job that DID its
/// own work but could not schedule the downstream work it causes.
///
/// Stable text, not an error rendering: it is written to `jobs.error_message`,
/// which `routes::jobs::get_job` returns to the browser verbatim.
pub const ENQUEUE_FAILURE_MESSAGE: &str =
    "Downstream processing could not be scheduled; retry required";

/// How long a terminal publication's causal links are kept before
/// [`purge_terminal_dependencies`] retires them. See that function.
pub const DEPENDENCY_RETENTION_DAYS: i64 = 30;

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct Stage {
    pub stage: String,
    pub total: i64,
    pub completed: i64,
    pub failed: i64,
    pub skipped: i64,
}
#[derive(Debug, serde::Serialize)]
pub struct Processing {
    pub state: &'static str,
    pub stages: Vec<Stage>,
}
impl Processing {
    /// `stages` must be non-empty — [`status`] returns `None` rather than
    /// calling this with an empty set, because an empty set is not a finished
    /// publication (see that function's doc comment).
    pub fn from_stages(stages: Vec<Stage>) -> Self {
        debug_assert!(
            !stages.is_empty(),
            "Processing::from_stages on an empty stage set reports `ready` for a \
             publication that was never observed — call status(), which returns None"
        );
        let state = if stages.iter().any(|s| s.failed > 0) {
            "attention"
        } else if stages.iter().any(|s| s.completed < s.total) {
            "processing"
        } else {
            "ready"
        };
        Self { state, stages }
    }
}

/// Roll a publication root and everything it caused up into one progress
/// report — or `None` when the caller may not see that root.
///
/// **Why `Option`, and why that is a security property rather than
/// ergonomics** (CodeRabbit round 1 on #871). The recursive seed is
/// `SELECT id FROM jobs WHERE id=$1 AND account_id=$2`. It matches nothing
/// for an unknown job id, for a job owned by a DIFFERENT account, and for a
/// root whose own `account_id` is NULL (SQL equality against NULL is never
/// true). All three used to produce zero rows, both `any()` calls in
/// [`Processing::from_stages`] were then vacuously false, and the fallthrough
/// answered **`"ready"`** — so "you may not look at this job" and "this
/// publication has finished" were the same response, and the more
/// authorization failed the more finished the work looked. Returning `None`
/// makes the caller decide what absence means instead of inheriting the most
/// reassuring possible answer.
///
/// Zero rows is exactly "the seed matched nothing": the root itself is always
/// a member of `work` when the seed matches, and the final `SELECT` joins
/// `jobs` back onto `work`, so a matched root always contributes at least one
/// stage row.
pub async fn status(
    pool: &PgPool,
    root: Uuid,
    account: Uuid,
) -> Result<Option<Processing>, sqlx::Error> {
    let stages = sqlx::query_as::<_, Stage>(r#"
      WITH RECURSIVE work(id) AS (
        SELECT id FROM jobs WHERE id=$1 AND account_id=$2
        UNION
        SELECT d.child_id FROM job_dependencies d JOIN work w ON w.id=d.parent_id
        JOIN jobs j ON j.id=d.child_id AND j.account_id=$2
      )
      SELECT CASE WHEN j.kind IN ('PromoteNode','PublishSource') THEN 'copying'
                  WHEN j.kind IN ('ExtractEntities','ExtractRelationships','ExtractMeetingAtoms','ExtractSourceAtoms','ExtractStandingMeetingAtoms','ExtractStandingSourceAtoms') THEN 'building'
                  ELSE 'checking' END AS stage,
             count(*) AS total, count(*) FILTER(WHERE j.status='done') AS completed,
             count(*) FILTER(WHERE j.status IN ('failed','dead')
                                OR (j.status='done' AND j.error_message IS NOT NULL)) AS failed,
             count(*) FILTER(WHERE j.outcome='nothing_to_do') AS skipped
      FROM jobs j JOIN work w ON w.id=j.id GROUP BY 1 ORDER BY 1
    "#).bind(root).bind(account).fetch_all(pool).await?;
    if stages.is_empty() {
        return Ok(None);
    }
    Ok(Some(Processing::from_stages(stages)))
}

/// Retire the causal links of publications that finished long enough ago that
/// nobody is still polling their progress.
///
/// **Why this has to exist** (CodeRabbit round 1 on #871). [`scope`] sets the
/// task-local parent for every job it wraps, not only publication-derived
/// ones, and the (since retired) per-atom relationship worker enqueued three
/// follow-ups per processed atom. On a busy account that is up to three
/// `job_dependencies` rows per atom ingested, forever — and [`status`]'s
/// recursive CTE walks this table on every job poll and every connection-list
/// row, so unbounded growth is a read cost, not just disk.
///
/// Deletes only links whose PARENT is terminal (`done`/`failed`/`dead`) and
/// finished more than [`DEPENDENCY_RETENTION_DAYS`] ago, so a live or
/// recently-finished publication's progress is never rewritten underneath a
/// client that is still polling it. A publication older than the horizon
/// degrades to "the root job's own row", which is what every pre-#871 caller
/// saw anyway.
///
/// `limit`-bounded (and always `ORDER BY`-free) so one pass can never turn
/// into a long-running delete on a hot table; the sweep that calls this runs
/// every tick, so a backlog drains across passes rather than in one lock.
pub async fn purge_terminal_dependencies(pool: &PgPool, limit: i64) -> Result<u64, sqlx::Error> {
    let deleted = sqlx::query(
        r#"
        DELETE FROM job_dependencies d
        WHERE (d.parent_id, d.child_id) IN (
            SELECT x.parent_id, x.child_id
            FROM job_dependencies x JOIN jobs p ON p.id = x.parent_id
            WHERE p.status IN ('done','failed','dead')
              AND p.finished_at IS NOT NULL
              AND p.finished_at < now() - make_interval(days => $1::int)
            LIMIT $2
        )
        "#,
    )
    .bind(DEPENDENCY_RETENTION_DAYS as i32)
    .bind(limit)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(deleted)
}
