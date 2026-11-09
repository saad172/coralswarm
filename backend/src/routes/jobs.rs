//! `GET /v1/jobs/{id}` — poll a background job's status/progress.
//!
//! # Why this exists
//! The async-promote PR moved `promote_node_scoped`'s coral copy/link tail
//! off the request path and into a `PromoteNode` background job (see
//! `workers::promote`'s module doc). A caller (the web app's promote UI)
//! that gets back a `job_id` instead of an immediate "done" needs SOME way
//! to ask "is it done yet, and how far along". Before this route, `jobs`
//! had no REST polling endpoint of any kind for any job kind — the only
//! existing async-status pattern in this codebase, `mcp_tasks`
//! (`mcp::tools::read::get_task_result`), is MCP-tool-specific (its own
//! table, its own RLS policies, polled by an MCP client, not the web app)
//! and was left untouched; this is a new, minimal, generic-over-`kind`
//! REST endpoint in the same spirit.
//!
//! Deliberately generic over every job `kind`, not `PromoteNode`-specific:
//! the `jobs` table and its `progress_current`/`progress_total` columns
//! (migration `0209_job_progress`/`0079_job_progress`) are shared by every
//! job kind, and a caller polling a `PromoteNode` job's id needs no special
//! handling here — this route just projects whatever the row already says.
//! A job kind that never calls `jobs::queue::update_progress` simply reports
//! `progress_current`/`progress_total` as `null` (see that column's own doc
//! comment: `NULL` means "no progress signal", never `0`).
//!
//! # Access control
//! `jobs` carries no RLS policies (it is not a content table under
//! `migrations_control/0083_consumer_rls` or `migrations_tenant/0009_tenant_rls`
//! — every existing worker reads/writes it via `queue::dequeue`/`complete`/
//! `fail` against the ambient pool, never a scoped transaction). So the ONE
//! enforcement this route has is its own explicit `WHERE id = $1 AND
//! account_id = $2` predicate against the caller's own `account_id` — same
//! app-layer-primary posture this codebase already uses in other places
//! that predate/sit outside RLS coverage (e.g. `tenancy::reachability`'s own
//! doc comment on `tenant_graph_reachable_by_user`'s explicit equality
//! check). A job whose `account_id` is `NULL` (the deliberately account-less
//! unscoped sweeps — `JobRow::account_id`'s own doc comment) is never
//! returned to any caller: there is no account it could belong to, so
//! nothing legitimately polls one by id here.

use axum::{extract::Path, http::StatusCode, Extension, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::tenancy::extractor::Tenant;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct JobStatusResponse {
    pub id: Uuid,
    pub kind: String,
    /// One of `pending`/`running`/`done`/`failed`/`dead` (`job_status`,
    /// migration `0004_jobs`).
    pub status: String,
    /// `NULL` (serialised as JSON `null`) means this job kind reports no
    /// granular progress — never assume `0` when this is absent. See
    /// `migrations_control/0209_job_progress.sql`'s header.
    pub progress_current: Option<i32>,
    pub progress_total: Option<i32>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// Poll a background job's status and progress.
///
/// Scoped to the caller's own `account_id` (see this module's doc comment
/// for why that check, not RLS, is what actually protects this route) — a
/// job id belonging to a different account, or that never existed, both
/// 404 identically (no existence-leak between "wrong account" and "no such
/// job").
#[utoipa_path(
    get,
    path = "/v1/jobs/{id}",
    tag = "jobs",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "Job identifier")),
    responses(
        (status = 200, description = "Job status/progress", body = Value),
        (status = 404, description = "No such job for this account"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_job(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(job_id): Path<Uuid>,
) -> (StatusCode, Json<Value>) {
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    let row: Option<JobStatusResponse> = match sqlx::query_as(
        r#"
        SELECT id, kind, status::text AS status, progress_current, progress_total,
               error_message, created_at, started_at, finished_at
        FROM jobs
        WHERE id = $1 AND account_id = $2
        "#,
    )
    .bind(job_id)
    .bind(user.account_id)
    .fetch_optional(&pool)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!(error = %e, job_id = %job_id, "get_job: query failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    match row {
        Some(job) => {
            let tracked = job.kind == "PromoteNode" || job.kind == "PublishSource";
            let mut value = serde_json::to_value(job).unwrap_or_else(|_| json!({}));
            if tracked {
                // `processing` is OPTIONAL ENRICHMENT, never a precondition for
                // the response (CodeRabbit round 1 on #871). This used to
                // return 503 whenever the lookup failed — but
                // `publication::status` runs a recursive CTE over `jobs` and
                // `job_dependencies`, so one lock wait or statement timeout hid
                // `status`, `error_message` and `finished_at` behind a 503:
                // exactly the fields the polling client needs most when the job
                // has ALREADY failed. A null `processing` degrades the progress
                // bar; a 503 hides the job.
                //
                // `Ok(None)` is "this account cannot see that publication root"
                // (see `publication::status`) and is reported the same way —
                // absent, never as a finished publication.
                match crate::jobs::publication::status(&pool, job_id, user.account_id).await {
                    Ok(Some(p)) => match serde_json::to_value(p) {
                        Ok(v) => value["processing"] = v,
                        Err(e) => {
                            tracing::error!(error=%e, %job_id, "publication progress failed to serialise");
                            value["processing"] = Value::Null;
                        }
                    },
                    Ok(None) => {
                        tracing::warn!(%job_id, "tracked job has no visible publication root");
                        value["processing"] = Value::Null;
                    }
                    Err(e) => {
                        tracing::error!(error=%e, %job_id, "publication progress unavailable");
                        value["processing"] = Value::Null;
                    }
                }
            }
            (StatusCode::OK, Json(value))
        },
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "job not found"})),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `JobStatusResponse` must serialise every field name verbatim (the
    /// frontend agent building the progress bar polls these exact keys) —
    /// pins the wire shape mechanically rather than relying on every
    /// reviewer to eyeball the `#[derive(Serialize)]` output.
    #[test]
    fn job_status_response_serialises_the_documented_keys() {
        let job = JobStatusResponse {
            id: Uuid::nil(),
            kind: "PromoteNode".to_string(),
            status: "running".to_string(),
            progress_current: Some(500),
            progress_total: Some(3129),
            error_message: None,
            created_at: Utc::now(),
            started_at: Some(Utc::now()),
            finished_at: None,
        };
        let value = serde_json::to_value(&job).unwrap();
        for key in [
            "id",
            "kind",
            "status",
            "progress_current",
            "progress_total",
            "error_message",
            "created_at",
            "started_at",
            "finished_at",
        ] {
            assert!(value.get(key).is_some(), "missing key: {key}");
        }
        assert_eq!(value["progress_current"], 500);
        assert_eq!(value["progress_total"], 3129);
        assert!(value["error_message"].is_null());
        assert!(value["finished_at"].is_null());
    }
}
