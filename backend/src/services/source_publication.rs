//! Standing source consent and finite, resumable evidence publication batches.
//! Never reads a topic's other evidence or copies its private relationship graph.
use crate::{
    jobs::{publication, queue, JobKind},
    store::{AtomBlobCopyCtx, BlobCopyMode, StoreError},
    tenancy::{
        scope::{begin_account_scoped, begin_tenant_scoped, begin_tenant_system_scoped},
        TenantId,
    },
    workers::tenant_harness::{Tick, WorkerTenant},
};
use chrono::{DateTime, Utc};
use coralswarm_core::r2::R2Client;
use dashmap::{mapref::entry::Entry, DashMap};
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct Connection {
    pub id: Uuid,
    pub account_id: Uuid,
    pub owner_id: Uuid,
    pub source_graph_id: Uuid,
    pub target_graph_id: Uuid,
    pub source_type: String,
    pub source_id: String,
    pub match_prefix: bool,
    pub workspace_id: Option<String>,
    pub label: String,
    pub mode: String,
    pub paused: bool,
    pub include_history: bool,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub next_check_at: DateTime<Utc>,
    pub last_checked_at: Option<DateTime<Utc>>,
    pub current_job_id: Option<Uuid>,
    pub error_message: Option<String>,
}

/// Channel scope is derived from an actual Slack item, never a caller-supplied wildcard.
pub fn selector(
    source_id: &str,
    source_type: &str,
    channel: bool,
) -> Result<(String, bool), &'static str> {
    if channel {
        let id = source_id.split('/').next().unwrap_or("");
        if source_type != "slack"
            || !source_id.contains('/')
            || !id.starts_with(['C', 'G'])
            || !id.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Err("Channel connections are supported for Slack channel sources only");
        }
        Ok((format!("{id}/"), true))
    } else {
        // Keep chunked documents/messages together using the explicit chunk separator.
        let base = source_id
            .rsplit_once(":chunk:")
            .filter(|(_, n)| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
            .map(|(base, _)| base)
            .unwrap_or(source_id);
        Ok((base.to_owned(), false))
    }
}

pub async fn candidates(
    tx: &mut Transaction<'_, Postgres>,
    c: &Connection,
) -> Result<Vec<Uuid>, sqlx::Error> {
    candidates_for(tx, c, None, 5000).await
}
pub async fn has_pending(
    tx: &mut Transaction<'_, Postgres>,
    c: &Connection,
) -> Result<bool, sqlx::Error> {
    Ok(!candidates_for(tx, c, None, 1).await?.is_empty())
}
async fn candidates_for(
    tx: &mut Transaction<'_, Postgres>,
    c: &Connection,
    ids: Option<&[Uuid]>,
    limit: i64,
) -> Result<Vec<Uuid>, sqlx::Error> {
    if c.mode == "once" && c.completed_at.is_some() {
        return Ok(vec![]);
    }
    sqlx::query_scalar(
        r#"
      SELECT a.atom_id FROM atoms a
      LEFT JOIN source_connection_atoms r ON r.connection_id=$1 AND r.atom_id=a.atom_id
      WHERE a.graph_id=$2 AND a.source_type=$3 AND a.workspace_id IS NOT DISTINCT FROM $4
        AND (a.source_id=$5 OR (NOT $6 AND starts_with(a.source_id,$5 || ':chunk:'))
             OR ($6 AND starts_with(a.source_id,$5)))
        AND ($7 OR a.created_at >= $8)
        AND ($9='live' OR a.created_at <= $8)
        AND (r.atom_id IS NULL OR r.revision <> a.content_revision)
        AND ($10::uuid[] IS NULL OR a.atom_id=ANY($10))
      ORDER BY a.created_at,a.atom_id LIMIT $11
    "#,
    )
    .bind(c.id)
    .bind(c.source_graph_id)
    .bind(&c.source_type)
    .bind(&c.workspace_id)
    .bind(&c.source_id)
    .bind(c.match_prefix)
    .bind(c.include_history)
    .bind(c.created_at)
    .bind(&c.mode)
    .bind(ids)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await
}

pub async fn enqueue_batch(
    tx: &mut Transaction<'_, Postgres>,
    c: &Connection,
    ids: Vec<Uuid>,
) -> Result<Option<Uuid>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(None);
    }
    let job = queue::enqueue_tx(
        tx,
        &JobKind::PublishSource {
            connection_id: c.id,
            atom_ids: ids,
        },
        Some(c.account_id),
        None,
        0,
    )
    .await?;
    sqlx::query("UPDATE source_connections SET current_job_id=$2,error_message=NULL WHERE id=$1")
        .bind(c.id)
        .bind(job)
        .execute(&mut **tx)
        .await?;
    Ok(job)
}

async fn scoped(
    t: &WorkerTenant,
    c: &Connection,
) -> Result<Transaction<'static, Postgres>, StoreError> {
    match &t.tenant {
        TenantId::Consumer => begin_account_scoped(&t.pool, c.account_id).await,
        TenantId::Org(_) => begin_tenant_scoped(&t.pool, c.account_id, c.owner_id).await,
    }
}

/// Recheck consent authority on every batch, including after a queued job waits.
async fn authorized(
    t: &WorkerTenant,
    control: &PgPool,
    c: &Connection,
) -> Result<bool, StoreError> {
    let member: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2)",
    )
    .bind(c.account_id)
    .bind(c.owner_id)
    .fetch_one(control)
    .await?;
    if !member {
        return Ok(false);
    }
    if let TenantId::Org(org) = &t.tenant {
        let revoked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM org_revocations WHERE clerk_org_id=$1)",
        )
        .bind(org)
        .fetch_one(control)
        .await?;
        if revoked {
            return Ok(false);
        }
        for id in [c.source_graph_id, c.target_graph_id] {
            let allowed: bool =
                sqlx::query_scalar("SELECT resolve_tenant_graph_reachable_by_user($1,$2,$3)")
                    .bind(id)
                    .bind(c.account_id)
                    .bind(c.owner_id)
                    .fetch_one(&t.pool)
                    .await?;
            if !allowed {
                return Ok(false);
            }
        }
    } else {
        for id in [c.source_graph_id, c.target_graph_id] {
            if !t.store.graph_reachable_by_user(id, c.owner_id).await? {
                return Ok(false);
            }
        }
    }
    let mut tx = scoped(t, c).await?;
    let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs s, graphs d WHERE s.id=$1 AND d.id=$2 AND s.account_id=$3 AND d.account_id=$3 AND s.archived_at IS NULL AND d.archived_at IS NULL AND d.kind IN ('team','org') AND (($4 AND s.kind='member' AND s.owner_user_id=$5) OR (NOT $4 AND s.kind='personal')))")
        .bind(c.source_graph_id).bind(c.target_graph_id).bind(c.account_id).bind(matches!(t.tenant,TenantId::Org(_))).bind(c.owner_id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(valid)
}

async fn copy_batch(
    t: &WorkerTenant,
    control: &PgPool,
    r2: Option<&R2Client>,
    job: &queue::JobRow,
    connection_id: Uuid,
    ids: &[Uuid],
) -> Result<(), StoreError> {
    // Old queue payloads authorize whole bodies, not exact retained spans.
    if !crate::selected_publication::legacy_whole_source_authorized() {
        return Err(StoreError::Conflict("Selected evidence consent is required".into()));
    }
    // Discovery is queue metadata; content is read only after authority is proved.
    let c: Connection =
        sqlx::query_as("SELECT * FROM source_connections WHERE id=$1 AND account_id=$2")
            .bind(connection_id)
            .bind(job.account_id)
            .fetch_one(&t.pool)
            .await?;
    if !authorized(t, control, &c).await? {
        return Err(StoreError::NotFound);
    }
    let dek = if r2.is_some() {
        crate::crypto::resolve_dek_for_tenant(&t.tenant, &t.pool, c.account_id)
            .await
            .map_err(|e| StoreError::Db(e.into()))?
    } else {
        None
    };
    if r2.is_some() && dek.is_none() {
        return Err(StoreError::Conflict(
            "Content encryption key unavailable".into(),
        ));
    }
    let blobs = match (r2, dek.as_ref()) {
        (Some(r2), Some(dek)) => Some(AtomBlobCopyCtx {
            blobs: r2,
            dek,
            org_id: match &t.tenant {
                TenantId::Org(id) => Some(id.as_str()),
                _ => None,
            },
        }),
        _ => None,
    };
    queue::update_progress(&t.pool, job.id, 0, ids.len() as i64).await?;
    let total = ids.len() as i64;
    let mut copied = 0;
    for ids in ids.chunks(100) {
        // A finite batch has at most 5,000 IDs and commits in 100-atom chunks.
        // Recheck authority and consent between chunks so Pause remains responsive.
        if !authorized(t, control, &c).await? {
            return Err(StoreError::NotFound);
        }
        let mut tx = scoped(t, &c).await?;
        let paused: bool =
            sqlx::query_scalar("SELECT paused FROM source_connections WHERE id=$1 FOR UPDATE")
                .bind(c.id)
                .fetch_one(&mut *tx)
                .await?;
        if paused {
            return Err(StoreError::Conflict("Source connection is paused".into()));
        }
        let allowed = candidates_for(&mut tx, &c, Some(ids), 100).await?;
        let selected: Vec<Uuid> = ids
            .iter()
            .copied()
            .filter(|id| allowed.contains(id))
            .collect();
        // Lock source revisions until their copies and receipts commit together.
        let revisions: Vec<(Uuid, i64)> = sqlx::query_as(
            "SELECT atom_id,content_revision FROM atoms WHERE atom_id=ANY($1) FOR SHARE",
        )
        .bind(&selected)
        .fetch_all(&mut *tx)
        .await?;
        t.store
            .refresh_published_atoms_scoped(
                &mut tx,
                &selected,
                c.target_graph_id,
                BlobCopyMode::from_ctx(blobs.as_ref()),
            )
            .await?;
        let targets:Vec<(Uuid,Uuid,i64)>=sqlx::query_as("SELECT s.atom_id,d.atom_id,d.promoted_from_revision FROM atoms s JOIN atoms d ON d.graph_id=$2 AND d.promoted_from_atom_id=s.atom_id WHERE s.atom_id=ANY($1)")
        .bind(&selected).bind(c.target_graph_id).fetch_all(&mut *tx).await?;
        if targets.len() != selected.len() {
            return Err(StoreError::Conflict("An existing workspace item has a different publication origin; no evidence was overwritten".into()));
        }
        for (source, target, revision) in &targets {
            if !revisions.contains(&(*source, *revision)) {
                return Err(StoreError::Conflict(
                    "Source changed while publishing; retry required".into(),
                ));
            }
            sqlx::query("INSERT INTO source_connection_atoms(connection_id,atom_id,revision,target_atom_id) VALUES($1,$2,$3,$4) ON CONFLICT(connection_id,atom_id) DO UPDATE SET revision=EXCLUDED.revision,target_atom_id=EXCLUDED.target_atom_id")
            .bind(c.id).bind(source).bind(revision).bind(target).execute(&mut *tx).await?;
        }
        let target_ids: Vec<Uuid> = targets.iter().map(|r| r.1).collect();
        tx.commit().await?;
        // Strictly post-commit — see [`reclaim_superseded_blobs`].
        reclaim_superseded_blobs(t, &c, r2, &target_ids).await;
        copied += ids.len() as i64;
        queue::update_progress(&t.pool, job.id, copied, total).await?;
    }
    Ok(())
}
/// Upper bound on accounts swept per tick. A sweep that fans out over every
/// due account in one pass is the unbounded-fan-out shape
/// `scripts/check_no_unbounded_fanout.sh` exists to refuse; the remainder keeps
/// its `next_check_at` and is picked up on the next tick, longest-waiting
/// first (see `resolve_due_source_connection_accounts`).
const DUE_ACCOUNT_LIMIT: i32 = 25;

/// Upper bound on `job_dependencies` rows retired per sweep. See
/// [`publication::purge_terminal_dependencies`].
const DEPENDENCY_PURGE_BATCH: i64 = 500;

/// Least time between two retention sweeps of one tenant's `job_dependencies`.
/// The sweep retires links whose parent finished over
/// [`publication::DEPENDENCY_RETENTION_DAYS`] ago, so a sweep per hour retires
/// them at most an hour late. Before this gate it ran after every poll of the
/// promote worker (about every 2 s per tenant on prod, 2026-10-26), where it
/// scanned the table for 290 to 510 ms of tenant-DB time and deleted nothing.
const DEPENDENCY_PURGE_INTERVAL: Duration = Duration::from_secs(60 * 60);

static LAST_DEPENDENCY_PURGE: LazyLock<DashMap<TenantId, Instant>> = LazyLock::new(DashMap::new);

/// True at most once per [`DEPENDENCY_PURGE_INTERVAL`] per tenant; records
/// `now` as the sweep time when it returns true.
fn dependency_purge_due(last: &DashMap<TenantId, Instant>, tenant: &TenantId, now: Instant) -> bool {
    match last.entry(tenant.clone()) {
        Entry::Occupied(mut e) => {
            if now.duration_since(*e.get()) < DEPENDENCY_PURGE_INTERVAL {
                return false;
            }
            e.insert(now);
            true
        }
        Entry::Vacant(e) => {
            e.insert(now);
            true
        }
    }
}

/// The stable, caller-facing reason for a publication failure.
///
/// **Why this exists** (CodeRabbit round 1 on #871). The failure paths below
/// used to bind `e.to_string()` straight into `source_connections.error_message`
/// and `jobs.error_message`. `Connection` derives `Serialize` with a public
/// `error_message`, `routes::source_connections::list` serialises the whole row
/// into its response, and `routes::jobs::get_job` returns `error_message`
/// verbatim — so a wrapped `sqlx::Error` put SQL text, table and column names,
/// constraint names and connection detail in front of the browser. The
/// `backend/**` path instructions are explicit: "API responses must not leak
/// internal error details."
///
/// The split is by VARIANT, not by matching message strings, so it cannot rot
/// as the messages are edited: [`StoreError::Conflict`] is documented as
/// "carries a caller-facing message" and this module raises its deliberate
/// refusals as that variant; [`StoreError::NotFound`] is this module's
/// authorization refusal; everything else is internal by construction and is
/// logged in full by the caller rather than shown.
fn safe_reason(e: &StoreError) -> String {
    match e {
        StoreError::Conflict(message) => message.clone(),
        StoreError::NotFound => "Publication access is no longer available".to_owned(),
        _ => "This source could not be published. It will retry automatically.".to_owned(),
    }
}

pub async fn tick(
    t: &WorkerTenant,
    control: &PgPool,
    r2: Option<&R2Client>,
) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["PublishSource"], 2)
        .await
        .map_err(|e| e.to_string())?;
    let n = jobs.len();
    for job in jobs {
        let parsed: Result<JobKind, _> =
            serde_json::from_value(serde_json::json!({"kind":job.kind,"payload":job.payload}));
        let result = match parsed {
            Ok(JobKind::PublishSource {
                connection_id,
                atom_ids,
            }) => {
                publication::scope(
                    job.id,
                    copy_batch(t, control, r2, &job, connection_id, &atom_ids),
                )
                .await
            }
            _ => Err(StoreError::Conflict("Invalid publication job".into())),
        };
        match result {
            Ok(()) => queue::complete(&t.pool, job.id, queue::JobOutcome::DidWork).await,
            Err(e) => {
                // The full error goes to the log; only the classified reason
                // reaches `jobs.error_message`, which `GET /v1/jobs/{id}`
                // returns to the browser verbatim. See [`safe_reason`].
                tracing::warn!(job_id = %job.id, error = %e, "source publication batch failed");
                queue::fail(&t.pool, job.id, &safe_reason(&e)).await
            }
        }
        .map_err(|e| e.to_string())?;
    }

    sweep_due(t, control).await?;

    // Retention for the causal links this module's own publications create —
    // bounded, and never touching a publication a client could still be
    // polling. See `publication::purge_terminal_dependencies`.
    if dependency_purge_due(&LAST_DEPENDENCY_PURGE, &t.tenant, Instant::now()) {
        if let Err(e) =
            publication::purge_terminal_dependencies(&t.pool, DEPENDENCY_PURGE_BATCH).await
        {
            tracing::warn!(error = %e, "job_dependencies retention sweep failed; will retry in an hour");
        }
    }

    Ok(if n == 0 {
        Tick::idle()
    } else {
        Tick::worked(n)
    })
}

/// Find the connections whose next check is due and sweep each one.
///
/// **Why this is two queries and not one** (CodeRabbit round 1 on #871).
/// `WorkerTenant::pool` on `TenantId::Consumer` is the shared consumer/control
/// plane, and migration `0213`'s `source_connection_scope` policy is anchored
/// on `current_setting('app.account_id', true)::uuid`. Under the production
/// `coralswarm_app` role (non-`BYPASSRLS` since OPS-4) the ambient
/// `SELECT * FROM source_connections` this used to run compares
/// `account_id = NULL`, which is NULL for every row — so it returned ZERO rows
/// and SUCCEEDED. The consumer-plane sweep was therefore not merely unscoped,
/// it was inert, and inert in the shape that looks healthiest: no error, no
/// rows, a green tick.
///
/// Scoping means knowing an account before reading, so
/// `resolve_due_source_connection_accounts` (migration `0216`/`0084`) answers
/// "which accounts have work" from the `source_connections_due` index — ids
/// only, never content — and each of those accounts then gets its own
/// `begin_account_scoped` transaction for the read. That keeps the sweep
/// O(accounts with due work) rather than O(every account on the plane), on a
/// worker that polls every 500ms.
///
/// **The ORG plane needs a scope too, and "a per-org pool cannot reach another
/// account" was never the question.** #871 left this arm reading ambiently off
/// `&t.pool` on the reasoning that a single-org pool is already narrow enough.
/// It is not: `migrations_tenant/0081`'s `source_connection_scope` is the same
/// policy as `0213`'s, anchored on the same `app.account_id` GUC, so the
/// ambient org read failed in exactly the consumer shape described above —
/// `account_id = NULL`, zero rows, `Ok`. An org pool being unable to reach
/// another ORG does not give it the GUC its own rows' policy reads.
///
/// It also has a second conjunct the consumer plane does not:
/// `source_graph_id IN (SELECT id FROM graphs) AND target_graph_id IN (...)`,
/// and tenant `graphs` carries `0009_tenant_rls`'s `member_ocean_isolation`.
/// So `begin_account_scoped` alone would not have been enough either — it
/// would have traded "sees nothing" for the worse failure this file's own
/// guard exists to catch: every connection whose source or target is a
/// `member` or `team` ocean silently skipped, every `org`/`personal` one
/// swept, and `Ok` returned either way. Publishing OUT of a member ocean is
/// the headline path (`promote-source` from a member ocean —
/// `member_ocean_promotion_pg`), so that partial sweep would have missed most
/// of the real traffic while looking healthy.
///
/// Hence [`begin_tenant_system_scoped`]: a reviewed stand-down of the
/// member-ocean backstop, WITH the `account_id` the policy's first conjunct
/// still requires (not the `_without_account` twin, which would leave
/// `app.account_id` unset and re-create the zero-row inertia). `system` is
/// honest here and a user id would not be: the sweep is discovering which
/// connections exist across every member of the tenant, so there is no single
/// acting user to present — the owner is a COLUMN ON THE ROW THIS QUERY IS
/// TRYING TO READ. Justified in docs/tenant-rls-cutover-runbook.md §2.
///
/// The per-connection WRITES below go through [`scoped`] on both planes, and
/// that one DOES have an honest user: `c.owner_id`, read off the connection.
async fn sweep_due(t: &WorkerTenant, control: &PgPool) -> Result<(), String> {
    const DUE_SQL: &str = "SELECT * FROM source_connections WHERE account_id=$1 AND NOT paused AND completed_at IS NULL AND next_check_at<=now() ORDER BY next_check_at LIMIT 2";
    let accounts: Vec<Uuid> =
        sqlx::query_scalar("SELECT * FROM resolve_due_source_connection_accounts($1)")
            .bind(DUE_ACCOUNT_LIMIT)
            .fetch_all(&t.pool)
            .await
            .map_err(|e| e.to_string())?;

    let mut due: Vec<Connection> = Vec::new();
    for account_id in accounts {
        let rows: Vec<Connection> = match &t.tenant {
            TenantId::Consumer => {
                let mut tx = begin_account_scoped(&t.pool, account_id)
                    .await
                    .map_err(|e| e.to_string())?;
                let rows = sqlx::query_as(DUE_SQL)
                    .bind(account_id)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())?;
                rows
            }
            TenantId::Org(_) => {
                let mut tx = begin_tenant_system_scoped(&t.pool, account_id)
                    .await
                    .map_err(|e| e.to_string())?;
                let rows = sqlx::query_as(DUE_SQL)
                    .bind(account_id)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| e.to_string())?;
                tx.commit().await.map_err(|e| e.to_string())?;
                rows
            }
        };
        due.extend(rows);
    }

    for c in due {
        if let Err(e) = sweep(t, control, &c).await {
            // The full error goes to the log; only the classified reason is
            // persisted, because `routes::source_connections::list` serialises
            // this column straight into its response. See [`safe_reason`].
            tracing::warn!(connection_id = %c.id, error = %e, "source publication sweep failed");
            // Metadata-only failure update, and account-scoped like every
            // other write this module makes.
            let mut tx = scoped(t, &c).await.map_err(|e| e.to_string())?;
            sqlx::query("UPDATE source_connections SET error_message=$2,next_check_at=now()+interval '60 seconds' WHERE id=$1")
                .bind(c.id)
                .bind(safe_reason(&e))
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

async fn sweep(t: &WorkerTenant, control: &PgPool, c: &Connection) -> Result<(), StoreError> {
    if !authorized(t, control, c).await? {
        return Err(StoreError::Conflict(
            "Publication access is no longer available".into(),
        ));
    }
    let mut tx = scoped(t, c).await?;
    let current:Option<Connection>=sqlx::query_as("SELECT * FROM source_connections WHERE id=$1 AND NOT paused AND next_check_at<=now() FOR UPDATE SKIP LOCKED")
        .bind(c.id).fetch_optional(&mut *tx).await?;
    let Some(c) = current else { return Ok(()) };
    let ready = match c.current_job_id {
        // `None` is "this publication root is not visible to this account" —
        // an unknown job id, another account's job, or a root with no
        // `account_id` at all. It is NOT "the work finished" (see
        // `publication::status`), so it must not open the gate on a new batch.
        Some(id) => match publication::status(&t.pool, id, c.account_id).await? {
            Some(processing) => processing.state == "ready",
            None => {
                tracing::warn!(
                    connection_id = %c.id, job_id = %id,
                    "publication root is not visible to its own connection's account; \
                     not enqueueing a new batch this pass"
                );
                false
            }
        },
        None => true,
    };
    if ready {
        let ids = candidates(&mut tx, &c).await?;
        if ids.is_empty() && c.mode == "once" {
            sqlx::query("UPDATE source_connections SET completed_at=now() WHERE id=$1")
                .bind(c.id)
                .execute(&mut *tx)
                .await?;
        }
        enqueue_batch(&mut tx, &c, ids).await?;
    }
    sqlx::query("UPDATE source_connections SET last_checked_at=now(),next_check_at=now()+interval '60 seconds',error_message=NULL WHERE id=$1")
        .bind(c.id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

/// Delete the R2 objects a just-committed refresh replaced.
///
/// **Ordering is the whole point** (CodeRabbit round 1 on #871): this runs
/// AFTER the transaction that replaced `content_blob_key` has committed, never
/// before, so a rolled-back refresh can never take a live object with it. The
/// keys are read from `atoms.superseded_blob_keys`, which
/// `Store::copy_atoms_to_graph_scoped`'s refresh arm appended inside that same
/// committed transaction (see migration `0215_atoms_superseded_blob_keys`), and
/// a key is cleared from the ledger only once R2 has accepted its delete.
///
/// Best-effort by design: a failure here leaks one object and leaves the key
/// RECORDED, so the next refresh of the same atom drains it. Nothing about the
/// publication depends on it, so it never fails the batch.
///
/// Reads and clears in two short transactions with the R2 calls BETWEEN them
/// rather than holding one open across the network round trips — the same
/// reason `routes::source_connections::list` no longer holds its transaction
/// across `publication::status`.
async fn reclaim_superseded_blobs(
    t: &WorkerTenant,
    c: &Connection,
    r2: Option<&R2Client>,
    target_ids: &[Uuid],
) {
    let Some(r2) = r2 else { return };
    if target_ids.is_empty() {
        return;
    }
    let pending: Vec<(Uuid, Vec<String>)> = match scoped(t, c).await {
        Ok(mut tx) => {
            let rows = sqlx::query_as::<_, (Uuid, Vec<String>)>(
                "SELECT atom_id, superseded_blob_keys FROM atoms \
                 WHERE atom_id = ANY($1) AND superseded_blob_keys <> '{}'",
            )
            .bind(target_ids)
            .fetch_all(&mut *tx)
            .await;
            let _ = tx.commit().await;
            match rows {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(error = %e, "could not read superseded blob keys; will retry on the next refresh");
                    return;
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "could not scope superseded-blob reclamation; will retry on the next refresh");
            return;
        }
    };
    for (atom_id, keys) in pending {
        let mut reclaimed: Vec<String> = Vec::new();
        for key in keys {
            match r2.delete_atom_blob(&key).await {
                Ok(()) => reclaimed.push(key),
                Err(e) => {
                    tracing::warn!(%atom_id, error = %e, "could not reclaim a superseded blob; key stays recorded")
                }
            }
        }
        if reclaimed.is_empty() {
            continue;
        }
        let cleared = async {
            let mut tx = scoped(t, c).await?;
            sqlx::query(
                "UPDATE atoms SET superseded_blob_keys = \
                 ARRAY(SELECT k FROM unnest(superseded_blob_keys) AS k WHERE k <> ALL($2::text[])) \
                 WHERE atom_id = $1",
            )
            .bind(atom_id)
            .bind(&reclaimed)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok::<(), StoreError>(())
        }
        .await;
        if let Err(e) = cleared {
            // The objects are already gone; the stale keys are harmless (a
            // repeat delete of a missing object is a no-op).
            tracing::warn!(%atom_id, error = %e, "reclaimed superseded blobs but could not clear their keys");
        }
    }
}

#[cfg(test)]
mod dependency_purge_tests {
    use super::*;

    #[test]
    fn purge_runs_once_per_interval_per_tenant() {
        let last = DashMap::new();
        let a = TenantId::Org("org_a".into());
        let b = TenantId::Org("org_b".into());
        let t0 = Instant::now();
        assert!(dependency_purge_due(&last, &a, t0), "first sweep runs");
        assert!(!dependency_purge_due(&last, &a, t0 + Duration::from_secs(2)), "next poll skips");
        assert!(dependency_purge_due(&last, &b, t0 + Duration::from_secs(2)), "other tenant is independent");
        assert!(!dependency_purge_due(&last, &a, t0 + DEPENDENCY_PURGE_INTERVAL - Duration::from_secs(1)));
        assert!(dependency_purge_due(&last, &a, t0 + DEPENDENCY_PURGE_INTERVAL), "runs again after the interval");
        assert!(!dependency_purge_due(&last, &a, t0 + DEPENDENCY_PURGE_INTERVAL + Duration::from_secs(2)), "the rerun restarts the interval");
    }
}
