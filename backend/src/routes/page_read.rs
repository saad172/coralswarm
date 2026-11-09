//! Router helpers for the `page_read` route class
//! (`docs/plans/page-load-standard/design.md` §4.2).
//!
//! A GET mounted through this module runs on the tenant's page pool lane
//! inside a `BEGIN READ ONLY` transaction with a 1 s statement timeout, and is
//! never shed by the admission cap. Only handlers that do indexed reads
//! belong here: a write inside the handler's own transaction fails with
//! SQLSTATE 25006, and a statement past a second is cancelled.
//!
//! `path` stays a string literal on purpose: `scripts/check_page_read_routes.sh`
//! (L5) greps `page_read::plain("…")` to learn the set of page_read routes
//! without a database or a compiler. `get`/`refresh` for stored read models
//! arrive with the `ReadModel` trait in L2.

use std::sync::Arc;

use axum::handler::Handler;
use axum::http::Method;
use axum::Router;

use crate::route_class::{classify, classify_absolute, RouteClass};
use crate::AppState;

/// A `page_read` GET for an already-cheap handler that is not a stored read
/// model (`GET /v1/me`, `GET /v1/graphs`, …). `path` is `/v1`-relative, like
/// every entry in `lib.rs`'s `v1_routes()`. Same class, lane and timeout as
/// a model route; the handler is an ordinary axum handler. `.merge` the
/// result into `v1_routes()`; a POST already mounted on the same path keeps
/// its own class.
pub fn plain<H, T>(path: &'static str, handler: H) -> Router<Arc<AppState>>
where
    H: Handler<T, Arc<AppState>>,
    T: 'static,
{
    classify(RouteClass::PageRead, Method::GET, path, handler)
}

/// [`plain`] for a GET on the open (unauthenticated, un-nested) router,
/// where `path` is the full template, e.g. `/v1/email/prefill`. Those
/// handlers use control read helpers, so capacity stays shared while the
/// read-only transaction and 1 s statement budget still apply.
pub fn plain_open<H, T>(path: &'static str, handler: H) -> Router<Arc<AppState>>
where
    H: Handler<T, Arc<AppState>>,
    T: 'static,
{
    classify_absolute(RouteClass::PageRead, Method::GET, path, handler)
}

use crate::{
    middleware::auth::UserContext,
    read_models::{self as rm, Params, ReadModel},
    tenancy::{extractor::Tenant, runtime::TenancyDecision},
};
use axum::response::{IntoResponse, Response};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use std::collections::BTreeMap;
type Failure = Response;
fn failure(status: StatusCode, message: &str) -> Failure {
    (status, Json(serde_json::json!({"error":message}))).into_response()
}
fn unavailable(error: impl std::fmt::Display) -> Failure {
    tracing::warn!(error=%error,"stored page request unavailable");
    failure(StatusCode::SERVICE_UNAVAILABLE, "read model unavailable")
}
fn parameter_error(error: rm::Error) -> Failure {
    if error.is::<rm::NotFound>() { failure(StatusCode::NOT_FOUND, "not found") }
    else { failure(StatusCode::BAD_REQUEST, "invalid model parameters") }
}
fn normalization_error(error:rm::Error)->Failure {
    if error.is::<rm::NotFound>() || error.is::<rm::BadRequest>() {parameter_error(error)} else {unavailable(error)}
}
fn denied_scope<M:ReadModel>() -> Failure {
    failure(if M::HIDE_DENIED_SCOPE { StatusCode::NOT_FOUND } else {StatusCode::FORBIDDEN}, "graph not found or access denied")
}

pub fn get<M: ReadModel>(path: &'static str) -> Router<Arc<AppState>> {
    classify(RouteClass::PageRead, Method::GET, path, serve::<M>)
        .layer(axum::middleware::from_fn(crate::library_read::no_store))
}
pub fn refresh<M: ReadModel>(path: &'static str) -> Router<Arc<AppState>> {
    // Axum owns the route's path string; registry PATH remains the only source.
    Router::new().route(
        &format!("{path}/refresh"),
        axum::routing::post(refresh_handler::<M>),
    )
}
async fn scope<M: ReadModel>(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    path: BTreeMap<String, String>,
    query: BTreeMap<String, String>,
) -> Result<rm::Scope<M::Params>, Failure> {
    if !M::enabled() { return Err(failure(StatusCode::NOT_FOUND, "not found")); }
    M::ACCESS.request(state, tenant, user).await?;
    let params = M::Params::parse(path, query)
        .map_err(parameter_error)?;
    if let Some(TenancyDecision::Org(ctx)) = &tenant.decision {
        if ctx.user_id != user.user_id || ctx.account_id != user.account_id {
            return Err(failure(StatusCode::FORBIDDEN, "actor mismatch"));
        }
    }
    let mut tx = tenant
        .begin_member_scoped(user.account_id)
        .await
        .map_err(unavailable)?;
    // Personal member opener historically sets only the account. Identity here
    // always comes from the verified UserContext on both planes.
    crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id)
        .await
        .map_err(unavailable)?;
    let account = if let Some(graph) = M::account_graph(&params) {
        let account: Option<uuid::Uuid> = sqlx::query_scalar("SELECT read_model_graph_account($1)")
            .bind(graph)
            .fetch_one(&mut *tx)
            .await
            .map_err(crate::store::StoreError::from)
            .map_err(unavailable)?;
        account.ok_or_else(denied_scope::<M>)?
    } else if let Some(anchor)=M::account_anchor(&params) {
        rm::anchored_account(&mut tx,anchor).await.map_err(unavailable)?
            .ok_or_else(||failure(StatusCode::FORBIDDEN,"object not found or access denied"))?
    } else {
        user.account_id
    };
    M::ACCESS.request_for_account(state, tenant, user, account).await?;
    sqlx::query("SELECT set_config('app.account_id',$1,true)")
        .bind(account.to_string())
        .execute(&mut *tx)
        .await
        .map_err(crate::store::StoreError::from)
        .map_err(unavailable)?;
    let params = params.normalize(&mut tx, user.user_id).await.map_err(normalization_error)?;
    let mut viewer = rm::Viewer::load(&mut tx, account, user.user_id)
        .await
        .map_err(unavailable)?;
    if M::CONTROL_FACTS {
        let control=state.control_db().ok_or_else(||unavailable("control unavailable"))?;
        let mut control_tx=crate::tenancy::scope::begin_request_account_read(control,account).await.map_err(unavailable)?;
        crate::tenancy::scope::set_tenant_user(&mut control_tx,user.user_id).await.map_err(unavailable)?;
        viewer.certify_control(&mut control_tx).await.map_err(unavailable)?;
        control_tx.rollback().await.map_err(crate::store::StoreError::from).map_err(unavailable)?;
    }
    let scope = viewer
        .scope::<M>(params)
        .map_err(|_| denied_scope::<M>())?;
    tx.rollback()
        .await
        .map_err(crate::store::StoreError::from)
        .map_err(unavailable)?;
    Ok(scope)
}
/// Generic handler also used by the documented legacy stats entry point.
pub async fn serve<M: ReadModel>(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    original: Option<Extension<TenancyDecision>>,
    Path(path): Path<BTreeMap<String, String>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Result<Json<rm::Stored<M::Payload>>, Failure> {
    if !M::enabled() { return Err(failure(StatusCode::NOT_FOUND, "not found")); }
    M::ACCESS.request(&state, &tenant, &user).await?;
    let params = M::Params::parse(path, query)
        .map_err(parameter_error)?;
    if let Some(TenancyDecision::Org(ctx)) = &tenant.decision {
        if ctx.user_id != user.user_id || ctx.account_id != user.account_id {
            return Err(failure(StatusCode::FORBIDDEN, "actor mismatch"));
        }
    }
    let mut tx = tenant
        .begin_member_scoped(user.account_id)
        .await
        .map_err(unavailable)?;
    crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id)
        .await
        .map_err(unavailable)?;
    let account = if let Some(graph) = M::account_graph(&params) {
        let account: Option<uuid::Uuid> = sqlx::query_scalar("SELECT read_model_graph_account($1)")
            .bind(graph)
            .fetch_one(&mut *tx)
            .await
            .map_err(crate::store::StoreError::from)
            .map_err(unavailable)?;
        account.ok_or_else(denied_scope::<M>)?
    } else if let Some(anchor)=M::account_anchor(&params) {
        rm::anchored_account(&mut tx,anchor).await.map_err(unavailable)?
            .ok_or_else(||failure(StatusCode::FORBIDDEN,"object not found or access denied"))?
    } else {
        user.account_id
    };
    let access=M::ACCESS.request_for_account(&state,&tenant,&user,account).await?;
    sqlx::query("SELECT set_config('app.account_id',$1,true)")
        .bind(account.to_string())
        .execute(&mut *tx)
        .await
        .map_err(crate::store::StoreError::from)
        .map_err(unavailable)?;
    let params = params.normalize(&mut tx, user.user_id).await.map_err(normalization_error)?;
    let mut viewer = rm::Viewer::load(&mut tx, account, user.user_id)
        .await
        .map_err(unavailable)?;
    if M::CONTROL_FACTS {
        let control=state.control_db().ok_or_else(||unavailable("control unavailable"))?;
        let mut control_tx=crate::tenancy::scope::begin_request_account_read(control,account).await.map_err(unavailable)?;
        crate::tenancy::scope::set_tenant_user(&mut control_tx,user.user_id).await.map_err(unavailable)?;
        viewer.certify_control(&mut control_tx).await.map_err(unavailable)?;
        control_tx.rollback().await.map_err(crate::store::StoreError::from).map_err(unavailable)?;
    }
    let scope = viewer
        .scope::<M>(params)
        .map_err(|_| denied_scope::<M>())?;
    let mut stored = rm::store::read::<M>(&mut tx, &scope)
        .await
        .map_err(unavailable)?;
    tx.rollback()
        .await
        .map_err(crate::store::StoreError::from)
        .map_err(unavailable)?;
    // Take the gate's ORIGINAL context, whose pool is the work lane. The Tenant
    // extractor's copy points at the page lane for this GET. Never enqueue on it.
    // `org` doubles as the wake target: `None` means the work is on the
    // consumer pool, whose harness entry is woken through `CONSUMER_WAKE`.
    let (pool, org) = match original.as_ref().map(|e| &e.0) {
        Some(TenancyDecision::Org(ctx)) => (Some(ctx.pool.clone()), Some(ctx.org_id.clone())),
        _ => (tenant.pool.clone(), None),
    };
    if stored.stale {
        match &org {
            Some(org) => state.worker_activity.note_enqueue(org),
            None => state.worker_activity.note_consumer_enqueue(),
        }
    }
    // Never computed, withheld, behind a reduction (served stale), or stale
    // with no ETA: enqueue now. A behind row's recompute would otherwise wait
    // for the dirty debounce while the caller polls for its own write.
    if stored.owe_refresh(chrono::Utc::now()) {
        if let Some(pool) = pool {
            let activity = state.worker_activity.clone();
            tokio::spawn(async move {
                match rm::store::enqueue::<M>(&pool, &scope).await {
                    Ok(()) => match &org {
                        Some(org) => activity.note_enqueue(org),
                        None => activity.note_consumer_enqueue(),
                    },
                    Err(_) => tracing::warn!(model = M::NAME, "read-model enqueue failed"),
                }
            });
        }
    }
    M::present(&mut stored.payload,access);
    Ok(Json(stored))
}
pub(crate) async fn refresh_handler<M: ReadModel>(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(path): Path<BTreeMap<String, String>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Result<(StatusCode, Json<serde_json::Value>), Failure> {
    let scope = scope::<M>(&state, &tenant, &user, path, query).await?;
    let pool = tenant.pool.as_ref().ok_or_else(|| unavailable("no pool"))?;
    rm::store::request_refresh::<M>(pool, &scope)
        .await
        .map_err(unavailable)?;
    match tenant.org_context() {
        Some(ctx) => state.worker_activity.note_enqueue(&ctx.org_id),
        None => state.worker_activity.note_consumer_enqueue(),
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"refresh_eta":chrono::Utc::now()+chrono::Duration::seconds(5)})),
    ))
}
