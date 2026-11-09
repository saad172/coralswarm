//! Provider-agnostic history-backfill worker — the durable consumer of
//! `JobKind::HistoryBackfill` (chat-per-user-ingest-and-in-chat-ask plan §4,
//! PR 2). Replaces the bare `tokio::spawn(backfill_channel(..))` the Slack
//! interactivity buttons used to fire directly off their ~3-second
//! interaction-ack path (see `routes::slack_interactivity`'s backfill-window
//! dispatch for the enqueue side, and `connectors::history_backfill` for the
//! actual per-provider engine this worker drives).
//!
//! # Plane
//!
//! Tenant loop, same reasoning as `workers::slack_ingest`: a `JobKind::
//! HistoryBackfill` job lands on WHICHEVER plane the enqueuer already
//! resolved `target_graph_id`/`target_user_id` against — the consumer/
//! control pool for a Personal account, a specific org's own tenant pool for
//! an Org account — and each plane's `jobs` table is invisible to a
//! dequeuer scoped to any other one. This is also what makes org support
//! real rather than theoretical: the job sits on the SAME tenant pool the
//! target ocean lives on, so this worker's per-tenant loop reaches it
//! without any extra plumbing.
//!
//! # Durability
//!
//! `connectors::history_backfill::run_slack`'s own doc comment states the
//! mandatory rule this worker implements: the job is marked `done` ONLY on
//! `Ok` — every message in the window landed or was a deterministic 422
//! skip. Any `Err` (including a `conversations.history` page-fetch failure,
//! which halts pagination outright) fails the job via `jobs::queue::fail`,
//! never `complete`, so the retry re-runs the FULL window from the top.
//! Safe because `atoms`' `UNIQUE (graph_id, source_type, source_id)` with
//! `ON CONFLICT DO NOTHING` makes every already-landed message idempotent —
//! a retried window costs one no-op write per message that already landed.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::history_backfill::{run_slack, BackfillTarget};
use crate::jobs::queue::{self, JobOutcome, JobRow};
use crate::routes::slack::AccountPlane;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// How many `HistoryBackfill` jobs to dequeue per poll cycle, per tenant.
/// One at a time, deliberately unlike `slack_ingest::JOB_BATCH_SIZE`'s 8: a
/// single unit can already take minutes on its own (up to
/// `connectors::history_backfill`'s own `BACKFILL_MAX_PAGES` pages, each
/// separated by its own inter-page sleep), so there is no batching upside —
/// only a bigger blast radius if this tenant's slot starves its neighbours.
const JOB_BATCH_SIZE: usize = 1;

/// Idle-poll interval — mirrors `workers::slack_ingest::EMPTY_POLL_SLEEP`.
pub const EMPTY_POLL_SLEEP: Duration = Duration::from_millis(500);

/// One `HistoryBackfill` pass on one tenant's `(pool, store)` pair: dequeue
/// up to [`JOB_BATCH_SIZE`] job(s) and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a queue worker
/// (`HarnessConfig::queue_worker`) — see that module's doc for the full
/// loop/backoff/scheduling contract.
///
/// `control_pool` is needed ONLY to resolve the bot token: `oauth_tokens` is
/// control-plane-only for every provider (Slack OAuth connect writes it
/// there regardless of the account's plane — see `routes::slack_events::
/// ingest_slack_message`'s own doc comment for the fuller history). `state`
/// is threaded through purely for `ingest_atom`'s plane-agnostic shared
/// clients (`http_client`, `r2`, `dek_cache`) — the same reason `workers::
/// slack_ingest::tick` takes a full `Arc<AppState>`.
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["HistoryBackfill"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        process_job(t, control_pool, state, job).await;
    }
    Ok(Tick::worked(n))
}

/// Fields `JobKind::HistoryBackfill` carries — deserialized straight from
/// `job.payload`. See that variant's own doc comment for why every field is
/// captured at enqueue time rather than re-derived here.
#[derive(serde::Deserialize)]
struct HistoryBackfillPayload {
    provider: String,
    account_id: Uuid,
    workspace_id: String,
    container_id: String,
    target_graph_id: Uuid,
    target_user_id: Uuid,
    clerk_org_id: Option<String>,
    window_days: Option<u32>,
}

async fn process_job(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>, job: JobRow) {
    let payload: HistoryBackfillPayload = match serde_json::from_value(job.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("malformed HistoryBackfill payload: {e}");
            tracing::error!(job_id = %job.id, error = %msg, "history backfill worker: malformed job payload");
            if let Err(qe) = queue::fail(&t.pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark malformed job as failed");
            }
            return;
        }
    };

    // Only Slack is implemented (this PR) — see `JobKind::HistoryBackfill`'s
    // own doc comment. An unrecognised provider is a STRUCTURAL failure:
    // retrying would hit the identical "no such arm" wall every time, so
    // fail it immediately rather than let it eat retry attempts before the
    // reaper's bounded backoff eventually converges it to `dead` anyway.
    //
    // `queue::could_not_do(.., FailureClass::Structural)`, NOT `queue::fail`
    // (CodeRabbit round 1, #1005, Data Integrity): an earlier version called
    // `fail`, which is the ORDINARY retry-ladder path — the doc comment
    // above already stated the correct intent while the code took the wrong
    // path, so this job was re-dequeued and re-rejected on every attempt
    // until the ladder's own attempt limit converged it to `dead` anyway,
    // burning every retry on a failure retrying could never fix.
    // `could_not_do(Structural)` skips straight to `dead_letter`.
    if payload.provider != "slack" {
        let msg = format!("history backfill: unsupported provider '{}'", payload.provider);
        tracing::error!(job_id = %job.id, error = %msg, "history backfill worker: {msg}");
        if let Err(qe) =
            queue::could_not_do(&t.pool, job.id, queue::FailureClass::Structural, &msg).await
        {
            tracing::error!(error = %qe, job_id = %job.id, "failed to mark unsupported-provider job as failed");
        }
        return;
    }

    let plane = if payload.clerk_org_id.is_some() { AccountPlane::Org } else { AccountPlane::Personal };

    // Bot token resolution: the ROW is control-plane-only, but for an org
    // account its DEK lives in the tenant DB's `account_keys` — same "two
    // pools, two questions" split `ingest_slack_message` documents (org-plane
    // DEK fix). `t.pool`/`t.tenant` are this job's own resolved plane, so the
    // bot token and every atom this run writes stay on that SAME plane.
    let dek_plane = crate::crypto::DekPlane::from_tenant(&t.tenant, t.pool.clone(), payload.account_id);
    let bot_token = match crate::workers::huddle::get_slack_bot_token_for_workspace(
        control_pool,
        &dek_plane,
        payload.account_id,
        &payload.workspace_id,
    )
    .await
    {
        Ok(Some(token)) => token,
        Ok(None) => {
            let msg = format!(
                "no oauth_tokens bot token for Slack workspace {} — cannot backfill",
                payload.workspace_id
            );
            tracing::warn!(job_id = %job.id, error = %msg, "history backfill worker: {msg}");
            if let Err(qe) = queue::fail(&t.pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job as failed after missing bot token");
            }
            return;
        }
        Err(e) => {
            let msg = format!("failed to resolve Slack bot token: {e}");
            tracing::warn!(job_id = %job.id, error = %msg, "history backfill worker: {msg}");
            if let Err(qe) = queue::fail(&t.pool, job.id, &msg).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job as failed after bot-token error");
            }
            return;
        }
    };

    let target = BackfillTarget {
        account_id: payload.account_id,
        workspace_id: payload.workspace_id,
        container_id: payload.container_id,
        target_graph_id: payload.target_graph_id,
        target_user_id: payload.target_user_id,
        plane,
        clerk_org_id: payload.clerk_org_id,
        window_days: payload.window_days,
    };

    match run_slack(state, control_pool, &t.pool, &state.http_client, &bot_token, &target).await {
        Ok(outcome) => {
            tracing::info!(
                job_id = %job.id,
                ingested = outcome.ingested,
                skipped_deterministic = outcome.skipped_deterministic,
                truncated = outcome.truncated,
                "history backfill worker: run completed"
            );
            if let Err(e) = queue::complete(&t.pool, job.id, JobOutcome::DidWork).await {
                tracing::error!(error = %e, job_id = %job.id, "failed to complete HistoryBackfill job");
            }
        }
        Err(e) => {
            // NOT scrubbed, NOT downgraded — `run_slack`'s `Err` means the
            // window did not fully land (see this module's doc comment), so
            // this must fail the job (never `complete`) so the WHOLE window
            // retries. `jobs.payload` here carries no message content or bot
            // token to begin with (`JobKind::HistoryBackfill`'s own doc
            // comment), so there is nothing to redact even on success.
            tracing::warn!(job_id = %job.id, error = %e, "history backfill worker: run failed — whole window will retry");
            if let Err(qe) = queue::fail(&t.pool, job.id, &e).await {
                tracing::error!(error = %qe, job_id = %job.id, "failed to mark job as failed after backfill error");
            }
        }
    }
}

#[cfg(test)]
mod unsupported_provider_pg_tests {
    //! CodeRabbit round 1 (#1005, Data Integrity): FAILS WITHOUT THE FIX.
    //! Proves an unrecognised `provider` dead-letters IMMEDIATELY
    //! (`status = 'dead'` on the first call) rather than entering the
    //! ordinary retry ladder (`status = 'pending'` with `attempts` bumped,
    //! `run_after` pushed out) — this branch runs BEFORE any bot-token/DEK
    //! resolution, so it needs no Slack mock, no real account keys.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib unsupported_provider_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::process_job;
    use crate::jobs::{queue, JobKind};
    use crate::store::PostgresStore;
    use crate::tenancy::TenantId;
    use crate::workers::tenant_harness::WorkerTenant;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin_conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
    async fn seed_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn unsupported_provider_dead_letters_immediately_not_via_the_retry_ladder_pg() {
        let db = "cs_history_backfill_unsupported_provider_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!(
                "skipping unsupported_provider_dead_letters_immediately_not_via_the_retry_ladder_pg \
                 — no ORGT_TEST_BASE_URL"
            );
            return;
        };
        let account_id = seed_account(&pool).await;
        let kind = JobKind::HistoryBackfill {
            provider: "not-a-real-provider".to_string(),
            account_id,
            workspace_id: "T1".to_string(),
            container_id: "C1".to_string(),
            target_graph_id: Uuid::new_v4(),
            target_user_id: Uuid::new_v4(),
            clerk_org_id: None,
            window_days: None,
        };
        queue::enqueue(&pool, &kind, Some(account_id), None, 0).await.unwrap();
        let jobs = queue::dequeue(&pool, &["HistoryBackfill"], 1).await.unwrap();
        assert_eq!(jobs.len(), 1);
        let job = jobs.into_iter().next().unwrap();
        let job_id = job.id;

        let state = std::sync::Arc::new(crate::test_app_state_owned(None));
        let store: std::sync::Arc<dyn crate::store::Store> =
            std::sync::Arc::new(PostgresStore::new(pool.clone()));
        let t = WorkerTenant { tenant: TenantId::Consumer, pool: pool.clone(), store };
        process_job(&t, &pool, &state, job).await;

        let (status, attempts, failure_class): (String, i32, Option<String>) = sqlx::query_as(
            "SELECT status::text, attempts, failure_class FROM jobs WHERE id = $1",
        )
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            status, "dead",
            "an unrecognised provider must dead-letter on the FIRST call, never enter the \
             ordinary retry ladder (which would leave status='pending')"
        );
        assert_eq!(attempts, 1, "dead_letter still increments attempts once, for observability parity");
        assert_eq!(
            failure_class.as_deref(),
            Some("structural"),
            "must be classified structural, not the default transient ladder"
        );

        teardown(admin_conn, pool, db).await;
    }
}
