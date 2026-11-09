//! Authenticated source publication controls. Every route is owner- and graph-scoped.
use crate::{
    jobs::publication,
    middleware::auth::UserContext,
    services::source_publication::{self, Connection},
    tenancy::{extractor::Tenant, reachability::ReachabilityResolver},
};
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;
type Error = (StatusCode, Json<Value>);
fn bad(message: &str) -> Error {
    (StatusCode::BAD_REQUEST, Json(json!({"error":message})))
}
fn missing() -> Error {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error":"Source or workspace not available"})),
    )
}
fn db(e: impl std::fmt::Display) -> Error {
    tracing::error!(error=%e,"source publication request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":"Could not update source publication"})),
    )
}

async fn source_access(tenant: &Tenant, user: &UserContext, graph: Uuid) -> Result<(), Error> {
    let store = tenant.store.as_ref().ok_or_else(missing)?;
    if !ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store.as_ref(),
        graph,
        user.user_id,
    )
    .await
    .map_err(db)?
    {
        return Err(missing());
    }
    let mut tx = tenant
        .begin_member_scoped(user.account_id)
        .await
        .map_err(db)?;
    let allowed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id=$1 AND account_id=$2 AND archived_at IS NULL AND (($3 AND kind='member' AND owner_user_id=$4) OR (NOT $3 AND kind='personal')))")
        .bind(graph).bind(user.account_id).bind(tenant.org_context().is_some()).bind(user.user_id).fetch_one(&mut *tx).await.map_err(db)?;
    if !allowed {
        return Err(missing());
    }
    tx.commit().await.map_err(db)?;
    Ok(())
}
#[derive(Deserialize)]
pub struct Create {
    pub source_id: String,
    pub target_graph_id: Uuid,
    #[serde(default)]
    pub keep_connected: bool,
    #[serde(default)]
    pub channel: bool,
    #[serde(default = "yes")]
    pub include_history: bool,
}
fn yes() -> bool {
    true
}
pub async fn create(
    _tenant: Tenant,
    Extension(_user): Extension<UserContext>,
    Path(_graph): Path<Uuid>,
    Json(_req): Json<Create>,
) -> Result<(StatusCode, Json<Value>), Error> {
    return Err((StatusCode::CONFLICT,Json(json!({"error":"Select exact retained passages using /sources/publications","code":"selected_evidence_consent_required"}))));
}

#[derive(Default, Deserialize)]
pub struct ListQuery {
    pub before: Option<Uuid>,
}
pub async fn list(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph): Path<Uuid>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, Error> {
    source_access(&tenant, &user, graph).await?;
    let pool = tenant.pool.as_ref().ok_or_else(missing)?;
    let mut tx = tenant
        .begin_member_scoped(user.account_id)
        .await
        .map_err(db)?;
    let mut rows:Vec<Connection>=sqlx::query_as("SELECT * FROM source_connections WHERE source_graph_id=$1 AND account_id=$2 AND owner_id=$3 AND ($4::uuid IS NULL OR (created_at,id)<(SELECT created_at,id FROM source_connections WHERE id=$4 AND source_graph_id=$1 AND account_id=$2 AND owner_id=$3)) ORDER BY created_at DESC,id DESC LIMIT 26")
        .bind(graph).bind(user.account_id).bind(user.user_id).bind(query.before).fetch_all(&mut *tx).await.map_err(db)?;
    let more = rows.len() > 25;
    if more {
        rows.pop();
    }
    let next_cursor = if more {
        rows.last().map(|c| c.id)
    } else {
        None
    };
    // Everything that needs `tx` happens BEFORE it is released; the progress
    // lookups happen after (CodeRabbit round 1 on #871). This loop used to hold
    // the member-scoped transaction open across up to 25 separate recursive
    // `publication::status` queries, each of which acquires its OWN connection
    // from the same bounded pool (20 by default). Enough concurrent list
    // requests and every connection is held by a transaction waiting for a
    // connection, which resolves as the 10-second acquire timeout rather than
    // as a deadlock — an availability failure with no obvious cause.
    let mut result = Vec::new();
    for row in &rows {
        let pending = source_publication::has_pending(&mut tx, row)
            .await
            .map_err(db)?;
        let mut value = serde_json::to_value(row).map_err(db)?;
        value["processing"] = Value::Null;
        value["has_pending_evidence"] = json!(pending);
        result.push(value);
    }
    tx.commit().await.map_err(db)?;

    // One lookup per DISTINCT root: several connections can share a publication
    // root, and each `status` call is a recursive CTE.
    let mut seen: HashMap<Uuid, Value> = HashMap::new();
    for (value, row) in result.iter_mut().zip(rows.iter()) {
        let Some(id) = row.current_job_id else { continue };
        if let Some(cached) = seen.get(&id) {
            value["processing"] = cached.clone();
            continue;
        }
        // `None` is "this account cannot see that root" — reported as absent,
        // never as a finished publication. See `publication::status`.
        let processing = match publication::status(pool, id, user.account_id)
            .await
            .map_err(db)?
        {
            Some(p) => serde_json::to_value(p).map_err(db)?,
            None => Value::Null,
        };
        seen.insert(id, processing.clone());
        value["processing"] = processing;
    }
    Ok(Json(
        json!({"connections":result,"next_cursor":next_cursor}),
    ))
}
#[derive(Deserialize)]
pub struct Update {
    pub action: String,
}
pub async fn update(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((graph, id)): Path<(Uuid, Uuid)>,
    Json(req): Json<Update>,
) -> Result<Json<Value>, Error> {
    if tenant.pool.is_some() && req.action!="pause"{return Err((StatusCode::CONFLICT,Json(json!({"error":"Standing publication requires versioned selected evidence consent","code":"selected_evidence_consent_required"}))));}
    source_access(&tenant, &user, graph).await?;
    if !["pause", "resume", "retry"].contains(&req.action.as_str()) {
        return Err(bad("Choose pause, resume, or retry"));
    }
    let mut tx = tenant
        .begin_member_scoped(user.account_id)
        .await
        .map_err(db)?;
    let c:Option<Connection>=sqlx::query_as("SELECT * FROM source_connections WHERE id=$1 AND source_graph_id=$2 AND owner_id=$3 AND account_id=$4 FOR UPDATE")
        .bind(id).bind(graph).bind(user.user_id).bind(user.account_id).fetch_optional(&mut *tx).await.map_err(db)?;
    let c = c.ok_or_else(missing)?;
    if req.action == "retry" || req.action == "resume" {
        if let Some(root) = c.current_job_id {
            // Retry failed stages without duplicating successful copies. Remove their
            // debounce key so they cannot collide with newly queued graph-wide work.
            sqlx::query(
                r#"WITH RECURSIVE work(id) AS (
              SELECT id FROM jobs WHERE id=$1 AND account_id=$2 UNION
              SELECT d.child_id FROM job_dependencies d JOIN work w ON d.parent_id=w.id)
              UPDATE jobs SET status='pending', attempts=0,error_message=NULL,finished_at=NULL,
                 run_after=now(),idempotency_key=NULL WHERE id IN(SELECT id FROM work)
                 AND account_id=$2 AND (status IN('failed','dead')
                     OR (status='pending' AND error_message IS NOT NULL)
                     -- A job that did its own work but could not SCHEDULE the
                     -- work it causes lands `done` carrying
                     -- `publication::ENQUEUE_FAILURE_MESSAGE` rather than
                     -- burning the retry ladder (see `jobs::queue::complete`).
                     -- Retry is the action that repairs exactly those.
                     OR (status='done' AND error_message IS NOT NULL))"#,
            )
            .bind(root)
            .bind(user.account_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
    }
    sqlx::query("UPDATE source_connections SET paused=$2,next_check_at=now(),error_message=NULL WHERE id=$1")
        .bind(id).bind(req.action=="pause").execute(&mut *tx).await.map_err(db)?;
    tx.commit().await.map_err(db)?;
    Ok(Json(json!({"id":id,"paused":req.action=="pause"})))
}
