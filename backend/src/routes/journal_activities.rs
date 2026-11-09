//! `/v1/journal/activities`: the HTTP edge of the Today activity cards. The
//! read is `services::journal_activities` (module doc: grouping, kind
//! mapping, access); this file only authorizes, resolves owner names and maps
//! answers.
//!
//! Both routes answer 404 while `TODAY_ACTIVITIES_ENABLED` is unset.
//!
//! # Contract
//!
//! `GET /v1/journal/activities?from=&to=&who=mine|team&graph_id=&tz=` ->
//! 200 `{ activities: [...], day_activity: [{day, count}], truncated,
//! window: {from, to, tz, who} }`. `from`/`to` are RFC3339, at most 62 days
//! apart. 400 on a bad `from`/`to`/`who`/`tz`; 404 when `graph_id` names an
//! ocean the caller cannot read.
//!
//! `GET /v1/journal/activities/{key}?graph_id=&tz=` -> 200 one activity plus
//! `decisions`, `sources`, `reefs`; 404 for a malformed key, an unreadable
//! ocean or a group with nothing the caller may read.
//!
//! Full shapes: `docs/implementation/journal-activities-api.md`.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono_tz::Tz;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    services::journal_activities::{self as ja, GraphRef},
    tenancy::{extractor::Tenant, reachability::ReachabilityResolver, TenantId},
    workers::journal_sweep::resolve_user_tz,
    AppState,
};

fn err(status: StatusCode, msg: &str) -> Response {
    (
        status,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(json!({"error": msg})),
    )
        .into_response()
}

fn not_found() -> Response {
    err(StatusCode::NOT_FOUND, "not found")
}

fn internal() -> Response {
    err(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

fn ok(body: serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(body),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    pub from: Option<String>,
    pub to: Option<String>,
    pub who: Option<String>,
    pub graph_id: Option<Uuid>,
    pub tz: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DetailParams {
    pub graph_id: Uuid,
    pub tz: Option<String>,
}

/// The oceans the caller may read right now (ADR-0007 D9: its own private
/// ocean, shared and team oceans it belongs to; never another member's
/// private ocean), the same read `GET /v1/graphs` and `GET /v1/journal` use.
async fn readable_graphs(tenant: &Tenant, user: &UserContext) -> Result<Vec<GraphRef>, Response> {
    let Some(store) = tenant.store.as_ref() else {
        return Err(err(StatusCode::SERVICE_UNAVAILABLE, "service unavailable"));
    };
    match ReachabilityResolver::list_reachable_graphs(
        tenant.decision.as_ref(),
        store.as_ref(),
        user.user_id,
    )
    .await
    {
        Ok(graphs) => Ok(graphs
            .into_iter()
            .filter(|g| g.archived_at.is_none())
            .map(|g| GraphRef {
                id: g.id,
                name: g.name,
            })
            .collect()),
        Err(e) => {
            tracing::error!(error = %e, "journal activities: reachability failed");
            Err(internal())
        }
    }
}

/// `tz=` when given, else the same chain the journal sweep uses for this user.
async fn zone(tenant: &Tenant, user: &UserContext, raw: Option<&str>) -> Result<Tz, Response> {
    if let Some(tz) = ja::parse_tz(raw).map_err(|m| err(StatusCode::BAD_REQUEST, &m))? {
        return Ok(tz);
    }
    let Some(store) = tenant.store.as_ref() else {
        return Err(err(StatusCode::SERVICE_UNAVAILABLE, "service unavailable"));
    };
    let tenant_id = tenant
        .decision
        .as_ref()
        .map(TenantId::from_decision)
        .unwrap_or(TenantId::Consumer);
    Ok(resolve_user_tz(store.as_ref(), &tenant_id, user.user_id).await)
}

/// `GET /v1/journal/activities`
pub async fn list_activities(
    state: State<Arc<AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    query: Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    crate::routes::page_read::serve::<crate::read_models::today::JournalActivities>(
        state,
        tenant,
        user,
        original,
        Path(std::collections::BTreeMap::new()),
        query,
    )
    .await
    .into_response()
}

/// `GET /v1/journal/activities/{key}`
pub async fn get_activity(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(key): Path<String>,
    Query(params): Query<DetailParams>,
) -> Response {
    if !ja::enabled() {
        return not_found();
    }
    let tz = match zone(&tenant, &user, params.tz.as_deref()).await {
        Ok(tz) => tz,
        Err(r) => return r,
    };
    let graph = match readable_graphs(&tenant, &user).await {
        Ok(graphs) => graphs.into_iter().find(|g| g.id == params.graph_id),
        Err(r) => return r,
    };
    let Some(graph) = graph else {
        return not_found();
    };
    if tenant.pool.is_none() {
        return not_found();
    }

    let mut tx = match tenant.begin_member_scoped(user.account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "journal activity detail: begin_member_scoped failed");
            return internal();
        }
    };
    let in_account =
        match ja::narrow_to_account(&mut tx, vec![graph.clone()], user.account_id).await {
            Ok(g) => g,
            Err(e) => {
                tracing::error!(error = %e, "journal activity detail: account narrowing failed");
                return internal();
            }
        };
    if matches!(in_account.as_slice(), []) {
        return not_found();
    }
    let request = ja::DetailRequest {
        key: &key,
        tz,
        graph: &graph,
    };
    let detail = match ja::activity_detail(&mut tx, &request).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(error = %e, "journal activity detail failed");
            return internal();
        }
    };
    if let Err(e) = tx.commit().await {
        tracing::error!(error = %e, "journal activity detail: commit failed");
        return internal();
    }
    let Some(mut detail) = detail else {
        return not_found();
    };
    resolve_names(&state, std::iter::once(&mut detail.activity)).await;
    ok(serde_json::to_value(&detail).unwrap_or_else(|_| json!({})))
}

/// Fill `owner.name` from the control plane. Best-effort: on any failure the
/// names stay null and the cards still render.
async fn resolve_names<'a>(
    state: &AppState,
    activities: impl Iterator<Item = &'a mut ja::Activity>,
) {
    let mut activities: Vec<&mut ja::Activity> = activities.collect();
    // `users` is a CONTROL-plane table (names live nowhere else): `control_db()`.
    let Some(pool) = state.control_db() else {
        return;
    };
    let mut ids: Vec<Uuid> = activities.iter().filter_map(|a| a.owner.user_id).collect();
    ids.sort();
    ids.dedup();
    match ja::resolve_owner_names(pool, &ids).await {
        Ok(names) => activities.iter_mut().for_each(|a| a.apply_names(&names)),
        Err(e) => tracing::warn!(error = %e, "journal activities: owner names unavailable"),
    }
}
