//! "Connect and done" capture for every recipe-registered Composio toolkit.
//! Connecting is the only member step: verification binds the workspace, runs
//! the recipe self-check and starts one workspace-wide capture whose watermark
//! is the moment of connect. The poller then ingests whatever changed since the
//! watermark. Auth credentials stay at Composio; content and checkpoints stay
//! in the member's own ocean. `{service}` is a toolkit slug; an unregistered
//! slug has no route behaviour at all.
use crate::source_origin::{
    CaptureConnectionId, CaptureConnectionKind, CaptureConnectionRef, CaptureSurfaceId,
    CaptureTransport, ContainerId, ContainerKind, ContentKind, ProviderNamespace, ServiceId,
    SourceContainer, SourceOrigin,
};
use crate::{
    connectors::composio::{self, recipe, Client, Error, Item, Recipe, Resume, SelfCheck, Window},
    middleware::auth::UserContext,
    tenancy::extractor::Tenant,
    AppState,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;

/// Pages one poll may ingest before yielding; an unfinished pass resumes on
/// the next tick from its saved cursor.
const MAX_PAGES_PER_POLL: usize = 20;
/// Shown when a recipe self-check fails. Plain words; the cause goes to ops.
pub const NOT_WORKING_YET: &str =
    "This tool isn't working with CoralSwarm yet. We've been told and are looking into it. Nothing you captured is lost.";

fn fail(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error":message}))).into_response()
}
fn internal() -> Response {
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Capture state could not be saved; try again",
    )
}
fn provider_error(e: Error) -> Response {
    let status = match e {
        Error::NotConfigured => StatusCode::SERVICE_UNAVAILABLE,
        Error::Reconnect | Error::Gone => StatusCode::UNAUTHORIZED,
        Error::PermissionDenied => StatusCode::FORBIDDEN,
        Error::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::BAD_GATEWAY,
    };
    fail(status, &e.to_string())
}
fn owner_key(user: &UserContext) -> String {
    format!("coralswarm:{}:{}", user.account_id, user.user_id)
}
fn parse_service(s: &str) -> Result<&'static Recipe, Response> {
    recipe::get(s).ok_or_else(|| fail(StatusCode::NOT_FOUND, "Unknown capture service"))
}
/// One connection's workspace-wide capture in the member's ocean.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Capture {
    pub id: Uuid,
    pub graph_id: Uuid,
    pub connection_id: Uuid,
    pub service: String,
    pub provider_namespace: String,
    #[serde(skip)]
    pub cursor: Option<String>,
    pub state: String,
    pub captured_items: i64,
    pub watermark: DateTime<Utc>,
    pub pass_started_at: Option<DateTime<Utc>>,
    pub next_poll_at: DateTime<Utc>,
    pub last_change_at: Option<DateTime<Utc>>,
    pub last_capture_at: Option<DateTime<Utc>>,
    pub retry_at: Option<DateTime<Utc>>,
    pub error_code: Option<String>,
    pub created_at: DateTime<Utc>,
}
const CAPTURE_COLUMNS: &str = "id,graph_id,connection_id,service,provider_namespace,cursor,state,captured_items,watermark,pass_started_at,next_poll_at,last_change_at,last_capture_at,retry_at,error_code,created_at";
#[derive(sqlx::FromRow)]
struct Connection {
    id: Uuid,
    connected_account_id: String,
    auth_config_id: String,
    status: String,
    provider_namespace: Option<String>,
    self_check: String,
    revoked_at: Option<DateTime<Utc>>,
    namespace_checked_at: Option<DateTime<Utc>>,
}
async fn connection(
    state: &AppState,
    user: &UserContext,
    service: &Recipe,
    id: Uuid,
) -> Result<Connection, Response> {
    let pool = state.control_db().ok_or_else(internal)?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await
        .map_err(|_| internal())?;
    let c=sqlx::query_as("SELECT id,connected_account_id,auth_config_id,status,provider_namespace,self_check,revoked_at,namespace_checked_at FROM composio_connections WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND service=$4")
 .bind(id).bind(user.account_id).bind(user.user_id).bind(&service.toolkit).fetch_optional(&mut *tx).await.map_err(|_|internal())?.ok_or_else(||fail(StatusCode::NOT_FOUND,"Connection not found"))?;
    tx.commit().await.map_err(|_| internal())?;
    Ok(c)
}
/// Control-plane connection state. `status` moves only between the five
/// values the migration allows; the caller owns the meaning.
async fn set_connection_status(
    state: &AppState,
    user: &UserContext,
    id: Uuid,
    status: &str,
) -> Result<(), ()> {
    let pool = state.control_db().ok_or(())?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await
        .map_err(|_| ())?;
    // Disconnect is final for a connection; only a new connect starts again.
    sqlx::query("UPDATE composio_connections SET status=$4,disconnected_at=CASE WHEN $4='disconnected' THEN COALESCE(disconnected_at,now()) ELSE disconnected_at END,updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND (status<>'disconnected' OR $4='disconnected')")
        .bind(id).bind(user.account_id).bind(user.user_id).bind(status).execute(&mut *tx).await.map_err(|_| ())?;
    tx.commit().await.map_err(|_| ())?;
    crate::read_models::dirty::connector_account_changed(state,user.account_id,user.user_id).await.map_err(|_| ())
}
/// How often an idle poll re-resolves the workspace live. Without it, a grant
/// routed to another workspace would keep advancing the watermark on empty
/// passes until that workspace changed. Stream calls surface auth failures on
/// their own, and a pass that returns items always re-resolves before ingesting.
const NAMESPACE_RECHECK: chrono::Duration = chrono::Duration::hours(24);
/// Stamps a live namespace check that matched (`Some`), or marks one due
/// (`None`). Losing the write only costs one extra check on the next poll.
async fn set_namespace_checked(
    state: &AppState,
    user: &UserContext,
    id: Uuid,
    matched: Option<&str>,
) -> Result<(), ()> {
    let pool = state.control_db().ok_or(())?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await
        .map_err(|_| ())?;
    sqlx::query("UPDATE composio_connections SET namespace_checked_at=CASE WHEN $4::text IS NULL THEN NULL ELSE now() END WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND ($4::text IS NULL OR provider_namespace=$4)")
        .bind(id).bind(user.account_id).bind(user.user_id).bind(matched).execute(&mut *tx).await.map_err(|_| ())?;
    tx.commit().await.map_err(|_| ())?;
    // This check timestamp is observational. Control0413 commits a durable
    // freshness notification with this write; keep the safe answer usable.
    // Actual namespace/status reductions retain their authority revision and
    // their existing reducing event paths.
    Ok(())
}
/// Persists a self-check verdict on the connection. A failure pauses the
/// connection and raises the ops alert; nothing else reports it.
async fn record_self_check(
    state: &AppState,
    user: &UserContext,
    service: &Recipe,
    id: Uuid,
    verdict: SelfCheck,
) -> Result<(), ()> {
    let pool = state.control_db().ok_or(())?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await
        .map_err(|_| ())?;
    let (sql, reason) = match verdict {
        SelfCheck::Passed => ("UPDATE composio_connections SET self_check='passed',self_check_reason=$4,self_checked_at=now(),status=CASE WHEN status='needs_attention' THEN 'active' ELSE status END,updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3", None),
        SelfCheck::Inconclusive => return Ok(()),
        SelfCheck::Failed(why) => ("UPDATE composio_connections SET self_check='failed',self_check_reason=$4,self_checked_at=now(),status=CASE WHEN status='disconnected' THEN status ELSE 'needs_attention' END,updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3", Some(why)),
    };
    sqlx::query(sql)
        .bind(id)
        .bind(user.account_id)
        .bind(user.user_id)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(|_| ())?;
    tx.commit().await.map_err(|_| ())?;
    crate::read_models::dirty::connector_account_changed(state,user.account_id,user.user_id).await.map_err(|_| ())?;
    if let SelfCheck::Failed(why) = verdict {
        alert_self_check_failed(service, id, user, why);
    }
    Ok(())
}
/// Ops alert: CoralSwarm pages on `alert = true` log events (see
/// `workers::tenant_harness`); `event` is the stable name to route on.
fn alert_self_check_failed(service: &Recipe, connection: Uuid, user: &UserContext, reason: &str) {
    tracing::error!(
        alert = true,
        event = "composio_self_check_failed",
        toolkit = %service.toolkit,
        toolkit_version = %service.version,
        connection_id = %connection,
        account_id = %user.account_id,
        reason,
        "Composio recipe failed on a real connection; the connection is paused as needs_attention"
    );
}
fn limits(service: &Recipe) -> Value {
    let mut limits = json!({
     "deletions":"never: items deleted or hidden in the tool stay in CoralSwarm",
     "updates":"every change after you connect becomes a new version of the same item",
    });
    for (topic, statement) in &service.limits {
        limits[topic] = json!(statement);
    }
    limits
}
/// Toolkits whose recipe has passed the self-check on at least one real
/// connection. Any failure reads as "none", which keeps every toolkit in beta.
async fn verified_toolkits(state: &AppState) -> BTreeSet<String> {
    let Some(pool) = state.control_db() else {
        return BTreeSet::new();
    };
    sqlx::query_scalar::<_, String>("SELECT * FROM composio_verified_toolkits()")
        .fetch_all(pool)
        .await
        .map(|v| v.into_iter().collect())
        .unwrap_or_default()
}
/// Registered toolkits, for the connectors index.
pub async fn toolkits(State(state): State<Arc<AppState>>) -> Response {
    let verified = verified_toolkits(&state).await;
    Json(json!({"toolkits":recipe::all().map(|r|json!({"toolkit":r.toolkit,"label":r.label,"kinds":r.kinds(),"beta":!verified.contains(&r.toolkit)})).collect::<Vec<_>>()}))
        .into_response()
}
/// A connection whose sign-in expired or was abandoned before it reached a workspace.
pub const SIGN_IN_EXPIRED: &str = "Signing in didn't finish. Connect again to start capturing.";
/// A connection that never bound a workspace gets sign-in copy, not revocation copy.
fn connection_message_for(status: &str, namespace: Option<&str>) -> Option<&'static str> {
    if status == "reconnect_needed" && namespace.is_none() {
        return Some(SIGN_IN_EXPIRED);
    }
    connection_message(status)
}
fn connection_message(status: &str) -> Option<&'static str> {
    Some(match status {
        "pending" => "Finish signing in to connect.",
        "needs_attention" => NOT_WORKING_YET,
        "reconnect_needed" => "Access was revoked or expired. Reconnect to keep capturing; everything already captured stays.",
        "disconnected" => "Disconnected. Everything already captured stays.",
        _ => return None,
    })
}
pub async fn status(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(service): Path<String>,
) -> Response {
    let service = match parse_service(&service) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let pool = match tenant.pool.as_ref() {
        Some(p) => p,
        None => return internal(),
    };
    let mut tx =
        match crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id).await
        {
            Ok(tx) => tx,
            Err(_) => return internal(),
        };
    let captures=match sqlx::query_as::<_,Capture>(&format!("SELECT {CAPTURE_COLUMNS} FROM composio_capture_scopes WHERE account_id=$1 AND owner_user_id=$2 AND service=$3 ORDER BY created_at"))
 .bind(user.account_id).bind(user.user_id).bind(&service.toolkit).fetch_all(&mut *tx).await {Ok(rows)=>rows,Err(_)=>return internal()};
    if tx.commit().await.is_err() {
        return internal();
    }
    let configured = std::env::var("COMPOSIO_API_KEY").is_ok_and(|s| !s.trim().is_empty())
        && std::env::var(service.auth_config_env()).is_ok_and(|s| !s.trim().is_empty());
    let connections = if let Some(pool) = state.control_db() {
        match crate::tenancy::scope::begin_account_scoped(pool, user.account_id).await {
            Ok(mut tx) => {
                let rows=sqlx::query_as::<_,(Uuid,String,Option<String>,String,DateTime<Utc>)>("SELECT id,status,provider_namespace,self_check,created_at FROM composio_connections WHERE account_id=$1 AND owner_user_id=$2 AND service=$3 ORDER BY created_at") .bind(user.account_id).bind(user.user_id).bind(&service.toolkit).fetch_all(&mut *tx).await;
                let _ = tx.commit().await;
                match rows {
                    Ok(rows) => rows
                        .into_iter()
                        .map(|(id, status, namespace, self_check, created_at)| {
                            let capture = captures.iter().find(|c| c.connection_id == id);
                            json!({"id":id,"status":status,"provider_namespace":namespace,"self_check":self_check,"created_at":created_at,"message":connection_message_for(&status,namespace.as_deref()),"capture":capture})
                        })
                        .collect::<Vec<_>>(),
                    Err(_) => return internal(),
                }
            }
            Err(_) => return internal(),
        }
    } else {
        vec![]
    };
    let beta = !verified_toolkits(&state).await.contains(&service.toolkit);
    Json(json!({"service":service.toolkit,"label":service.label,"toolkit_version":service.version,"kinds":service.kinds(),"configured":configured,"polling_enabled":std::env::var("COMPOSIO_CAPTURE_POLLING_ENABLED").as_deref()==Ok("true"),"beta":beta,"connections":connections,"limits":limits(service)})).into_response()
}
/// Auth link generated only for this authenticated account/member. Browser
/// input cannot attach a pre-existing Composio connection belonging to another user.
pub async fn connect(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(service): Path<String>,
) -> Response {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    let service = match parse_service(&service) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match start_connect(&state, &user, service).await {
        Ok((id, redirect_url)) => {
            Json(json!({"connection_id":id,"redirect_url":redirect_url})).into_response()
        }
        Err(r) => r,
    }
}
/// [`connect`] without the HTTP layer, shared with the generic connector API's
/// Composio adapters: checks the pinned read-only auth config, asks Composio for
/// a hosted sign-in link for this member, and records the pending connection.
/// Returns the new connection's id and the https link to send the member to.
pub(crate) async fn start_connect(
    state: &AppState,
    user: &UserContext,
    service: &'static Recipe,
) -> Result<(Uuid, String), Response> {
    let client = Client::configured().map_err(provider_error)?;
    let auth_config = std::env::var(service.auth_config_env())
        .ok()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| provider_error(Error::NotConfigured))?;
    // Fixed read-only auth configs are deployment-owned. Check the config before
    // initiating OAuth; never accept a config ID or scope list from the browser.
    let cfg = client
        .request(
            reqwest::Method::GET,
            &format!("/auth_configs/{auth_config}"),
            None,
        )
        .await
        .map_err(provider_error)?;
    if cfg.pointer("/toolkit/slug").and_then(Value::as_str) != Some(service.toolkit.as_str()) {
        return Err(provider_error(Error::Contract("auth-config toolkit")));
    }
    let scopes = cfg
        .pointer("/credentials/scopes")
        .or_else(|| cfg.pointer("/auth_config/scopes"))
        .or_else(|| cfg.get("scopes"));
    if !service.grants_read_only(scopes) {
        return Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Configure and verify a read-only Composio auth config before connecting",
        ));
    }
    let response = client
        .request(
            reqwest::Method::POST,
            "/connected_accounts/link",
            Some(json!({"auth_config_id":auth_config,"user_id":owner_key(user)})),
        )
        .await
        .map_err(provider_error)?;
    let remote = match response.get("connected_account_id").and_then(Value::as_str) {
        Some(v) if composio::account_id_ok(v) => v,
        _ => return Err(provider_error(Error::Contract("connected account id"))),
    };
    let redirect = match response
        .get("redirect_url")
        .and_then(Value::as_str)
        .and_then(|s| reqwest::Url::parse(s).ok())
    {
        Some(url) if url.scheme() == "https" => url,
        _ => return Err(provider_error(Error::Contract("auth redirect"))),
    };
    let pool = state.control_db().ok_or_else(internal)?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await
        .map_err(|_| internal())?;
    let id=sqlx::query_scalar::<_,Uuid>("INSERT INTO composio_connections(account_id,owner_user_id,service,connected_account_id,auth_config_id) VALUES($1,$2,$3,$4,$5) RETURNING id") .bind(user.account_id).bind(user.user_id).bind(&service.toolkit).bind(remote).bind(auth_config).fetch_one(&mut *tx).await.map_err(|_|internal())?;
    tx.commit().await.map_err(|_| internal())?;
    Ok((id, redirect.to_string()))
}
fn grant_matches(account: &Value, service: &Recipe, user: &UserContext, auth_config: &str) -> bool {
    account.get("user_id").and_then(Value::as_str) == Some(owner_key(user).as_str())
        && account.pointer("/toolkit/slug").and_then(Value::as_str) == Some(service.toolkit.as_str())
        && account.pointer("/auth_config/id").and_then(Value::as_str) == Some(auth_config)
        && service.grants_read_only(account.get("requested_scopes"))
}
/// Finishes a connect: checks the grant, binds the workspace, runs the recipe
/// self-check and starts capture. The web page calls it while the member signs
/// in; the capture worker runs the same logic for pending connections nobody
/// is watching. Repeating it on an active connection is harmless.
pub async fn verify(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((service, id)): Path<(String, Uuid)>,
) -> Response {
    let service = match parse_service(&service) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let client = match Client::configured() {
        Ok(c) => c,
        Err(e) => return provider_error(e),
    };
    verify_with(&state, &tenant, &user, service, id, &client).await
}
/// Shared body of [`verify`] for the route and the capture worker. Answers
/// 409 `sign_in_pending` while Composio still waits for the member, and 410
/// `sign_in_expired` (connection now `reconnect_needed`) once the Composio
/// account expired, failed, went inactive or was deleted.
#[doc(hidden)]
pub async fn verify_with(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    service: &Recipe,
    id: Uuid,
    client: &Client,
) -> Response {
    let c = match connection(state, user, service, id).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    if c.status == "disconnected" {
        return fail(
            StatusCode::CONFLICT,
            "This connection was disconnected; connect again to resume capture",
        );
    }
    if !composio::account_id_ok(&c.connected_account_id) {
        return provider_error(Error::Contract("connected account id"));
    }
    let account = match client
        .request(
            reqwest::Method::GET,
            &format!("/connected_accounts/{}", c.connected_account_id),
            None,
        )
        .await
    {
        Ok(v) => v,
        Err(Error::Gone) => return sign_in_ended(state, user, &c).await,
        Err(e) => return provider_error(e),
    };
    match account.get("status").and_then(Value::as_str) {
        Some("ACTIVE") => {}
        Some("EXPIRED" | "FAILED" | "INACTIVE") => return sign_in_ended(state, user, &c).await,
        // INITIALIZING / INITIATED: the member has not finished signing in.
        _ => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error":"Finish signing in, then CoralSwarm will start capturing","code":"sign_in_pending","status":"pending"})),
            )
                .into_response()
        }
    }
    if !grant_matches(&account, service, user, &c.auth_config_id) {
        return fail(
            StatusCode::FORBIDDEN,
            "Connected account does not match the pinned read-only grant",
        );
    }
    let namespace = match client
        .namespace(service, &c.connected_account_id, &owner_key(user))
        .await
    {
        Ok(v) => v,
        Err(e) => return provider_error(e),
    };
    if c.provider_namespace
        .as_ref()
        .is_some_and(|old| old != &namespace)
    {
        return fail(
            StatusCode::CONFLICT,
            "This grant now points to a different workspace; connect it separately",
        );
    }
    let pool = match state.control_db() {
        Some(p) => p,
        None => return internal(),
    };
    let mut tx = match crate::tenancy::scope::begin_account_scoped(pool, user.account_id).await {
        Ok(tx) => tx,
        Err(_) => return internal(),
    };
    match bind_connection_namespace_tx(&mut tx, user, c.id, &namespace).await {
        Ok(true) => {}
        Ok(false) => {
            return fail(
                StatusCode::CONFLICT,
                "Connection workspace changed; verify it separately",
            );
        }
        Err(_) => return internal(),
    }
    if tx.commit().await.is_err() {
        return internal();
    }
    if crate::read_models::dirty::connector_account_changed(state,user.account_id,user.user_id).await.is_err() { return internal(); }
    activate(state, tenant, user, service, c.id, client).await
}
/// The Composio account will never become usable: the connection stops
/// waiting and the member is asked to connect again.
async fn sign_in_ended(state: &AppState, user: &UserContext, c: &Connection) -> Response {
    if set_connection_status(state, user, c.id, "reconnect_needed").await.is_err() {
        return internal();
    }
    let message = connection_message_for("reconnect_needed", c.provider_namespace.as_deref());
    (
        StatusCode::GONE,
        Json(json!({"connection_id":c.id,"status":"reconnect_needed","code":"sign_in_expired","error":message,"message":message})),
    )
        .into_response()
}
/// Atomic first binding: simultaneous verification cannot overwrite a different
/// workspace even when both requests loaded an initially unbound connection.
#[doc(hidden)]
pub async fn bind_connection_namespace_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user: &UserContext,
    id: Uuid,
    namespace: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("UPDATE composio_connections SET status=CASE WHEN status IN ('pending','reconnect_needed') THEN 'active' ELSE status END,provider_namespace=$4,updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND status<>'disconnected' AND (provider_namespace IS NULL OR provider_namespace=$4)")
        .bind(id).bind(user.account_id).bind(user.user_id).bind(namespace).execute(&mut **tx).await?;
    Ok(result.rows_affected() == 1)
}
/// Self-check (unless already passed) and capture start for a connection whose
/// grant and workspace are bound. Capture starts at the watermark `now()`;
/// the probe items the self-check reads are never ingested.
#[doc(hidden)]
pub async fn activate(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    service: &Recipe,
    id: Uuid,
    client: &Client,
) -> Response {
    let c = match connection(state, user, service, id).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let Some(namespace) = c.provider_namespace.clone().filter(|_| matches!(c.status.as_str(), "active" | "needs_attention")) else {
        return fail(StatusCode::CONFLICT, "Verify this connection first");
    };
    let verdict = if c.self_check == "passed" {
        SelfCheck::Passed
    } else {
        match client
            .self_check(service, &c.connected_account_id, &owner_key(user), &namespace, Utc::now())
            .await
        {
            Ok(v) => v,
            Err(e) => return provider_error(e),
        }
    };
    // An empty probe cannot establish that a previously failed recipe was repaired.
    // Retain the failure and keep explicit recovery available until a probe passes.
    let verdict = if c.self_check == "failed" && matches!(verdict, SelfCheck::Inconclusive) {
        SelfCheck::Failed("recovery probe inconclusive")
    } else {
        verdict
    };
    if c.self_check != "passed" && record_self_check(state, user, service, id, verdict).await.is_err() {
        return internal();
    }
    let capture_state = if matches!(verdict, SelfCheck::Failed(_)) {
        "needs_attention"
    } else {
        "active"
    };
    let (capture, graph) = match ensure_capture(tenant, user, service, id, &namespace, capture_state).await {
        Ok(v) => v,
        Err(()) => return internal(),
    };
    let (status, self_check) = match verdict {
        SelfCheck::Failed(_) => ("needs_attention", "failed"),
        SelfCheck::Passed => ("active", "passed"),
        SelfCheck::Inconclusive => ("active", "pending"),
    };
    if crate::read_models::dirty::connector_changed(state,tenant,user.account_id,user.user_id).await.is_err() { return internal(); }
    Json(json!({"connection_id":id,"status":status,"provider_namespace":namespace,"self_check":self_check,"message":connection_message(status),"capture_id":capture,"graph_id":graph}))
        .into_response()
}
/// One capture per connection in the member's primary ocean. A connection that
/// replaces one needing reconnect for the same workspace inherits its
/// watermark, so the gap while access was broken is still captured.
async fn ensure_capture(
    tenant: &Tenant,
    user: &UserContext,
    service: &Recipe,
    connection: Uuid,
    namespace: &str,
    state: &str,
) -> Result<(Uuid, Uuid), ()> {
    let pool = tenant.pool.as_ref().ok_or(())?;
    let plane = if tenant.org_context().is_some() {
        crate::routes::slack::AccountPlane::Org
    } else {
        crate::routes::slack::AccountPlane::Personal
    };
    let graph = crate::routes::slack::resolve_primary_graph(
        pool,
        user.account_id,
        plane,
        crate::routes::slack::IngestCaller::Attributable(user.user_id),
    )
    .await
    .map_err(|_| ())?;
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id)
        .await
        .map_err(|_| ())?;
    let inherited = sqlx::query_scalar::<_, DateTime<Utc>>("SELECT watermark FROM composio_capture_scopes WHERE graph_id=$1 AND account_id=$2 AND owner_user_id=$3 AND service=$4 AND provider_namespace=$5 AND connection_id<>$6 AND state='reconnect_needed' ORDER BY watermark LIMIT 1")
        .bind(graph).bind(user.account_id).bind(user.user_id).bind(&service.toolkit).bind(namespace).bind(connection)
        .fetch_optional(&mut *tx).await.map_err(|_| ())?;
    let id = sqlx::query_scalar::<_, Uuid>("INSERT INTO composio_capture_scopes(graph_id,account_id,owner_user_id,connection_id,service,provider_namespace,state,watermark,next_poll_at) VALUES($1,$2,$3,$4,$5,$6,$7,COALESCE($8,now()),now()+interval '15 minutes') ON CONFLICT(graph_id,connection_id) DO UPDATE SET state=CASE WHEN composio_capture_scopes.state='disconnected' THEN 'disconnected' ELSE EXCLUDED.state END,error_code=NULL,updated_at=now() RETURNING id")
        .bind(graph).bind(user.account_id).bind(user.user_id).bind(connection).bind(&service.toolkit).bind(namespace).bind(state).bind(inherited)
        .fetch_one(&mut *tx).await.map_err(|_| ())?;
    if inherited.is_some() {
        sqlx::query("UPDATE composio_capture_scopes SET state='disconnected',updated_at=now() WHERE graph_id=$1 AND account_id=$2 AND owner_user_id=$3 AND service=$4 AND provider_namespace=$5 AND connection_id<>$6 AND state='reconnect_needed'")
            .bind(graph).bind(user.account_id).bind(user.user_id).bind(&service.toolkit).bind(namespace).bind(connection)
            .execute(&mut *tx).await.map_err(|_| ())?;
    }
    tx.commit().await.map_err(|_| ())?;
    Ok((id, graph))
}
async fn set_capture_state(
    tenant: &Tenant,
    user: &UserContext,
    connection: Uuid,
    state: &str,
) -> Result<(), ()> {
    let pool = tenant.pool.as_ref().ok_or(())?;
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id)
        .await
        .map_err(|_| ())?;
    sqlx::query("UPDATE composio_capture_scopes SET state=CASE WHEN state='disconnected' THEN state ELSE $4 END,updated_at=now() WHERE account_id=$1 AND owner_user_id=$2 AND connection_id=$3")
        .bind(user.account_id).bind(user.user_id).bind(connection).bind(state)
        .execute(&mut *tx).await.map_err(|_| ())?;
    tx.commit().await.map_err(|_| ())
}
/// Stops capture and revokes the Composio connected account (and its provider
/// credentials). Every captured atom stays. Safe to repeat: a failed
/// revocation is retried by calling again.
pub async fn disconnect(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((service, id)): Path<(String, Uuid)>,
) -> Response {
    let service = match parse_service(&service) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let client = match Client::configured() {
        Ok(c) => c,
        Err(e) => return provider_error(e),
    };
    disconnect_with(&state, &tenant, &user, service, id, &client).await
}
#[doc(hidden)]
pub async fn disconnect_with(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    service: &Recipe,
    id: Uuid,
    client: &Client,
) -> Response {
    let c = match connection(state, user, service, id).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    // Stop polling before touching Composio, so a failed revocation never
    // leaves capture running.
    if set_connection_status(state, user, id, "disconnected").await.is_err()
        || set_capture_state(tenant, user, id, "disconnected").await.is_err()
    {
        return internal();
    }
    if c.revoked_at.is_none() {
        use crate::workers::composio_capture::{revoke_connection, RevokeFailed};
        let Some(pool) = state.control_db() else {
            return internal();
        };
        match revoke_connection(pool, client, user.account_id, user.user_id, id, &c.connected_account_id).await {
            Ok(()) => {}
            Err(RevokeFailed::Provider(e)) => {
                let status = match e {
                    Error::NotConfigured => StatusCode::SERVICE_UNAVAILABLE,
                    _ => StatusCode::BAD_GATEWAY,
                };
                return (status, Json(json!({"connection_id":id,"status":"disconnected","revoked":false,"error":"Capture stopped, but revoking access at the provider failed. Try Disconnect again."}))).into_response();
            }
            Err(RevokeFailed::Store) => return internal(),
        }
    }
    Json(json!({"connection_id":id,"status":"disconnected","revoked":true,"captured_items":"kept"})).into_response()
}
/// Why a poll stopped without finishing.
enum Halt {
    Provider(Error),
    /// The connection is no longer active (disconnected, needs attention,
    /// reconnect needed); the capture mirrors it.
    Connection(String),
    SelfCheckFailed,
    Ingest,
}
impl From<Error> for Halt {
    fn from(e: Error) -> Self {
        Self::Provider(e)
    }
}
/// How long a paused toolkit's capture waits before the worker looks again.
const PAUSED_RECHECK_SECS: i64 = 300;

/// Push a paused capture's next poll out, so it stops being due. Best effort: if
/// this fails the capture is simply offered again next round, and gated again.
async fn defer_paused_capture(pool: &sqlx::PgPool, user: &UserContext, toolkit: &str, capture_id: Uuid) {
    let Ok(mut tx) = crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id).await else {
        return;
    };
    let saved = sqlx::query(
        "UPDATE composio_capture_scopes SET next_poll_at=now()+make_interval(secs=>$4),updated_at=now() \
         WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND service=$5",
    )
    .bind(capture_id).bind(user.account_id).bind(user.user_id).bind(PAUSED_RECHECK_SECS as f64).bind(toolkit)
    .execute(&mut *tx).await;
    if saved.is_ok() {
        let _ = tx.commit().await;
    }
}

/// One bounded poll of a capture: leases it, checks the connection and
/// workspace, finishes a pending self-check, then ingests change pages until
/// the pass ends or the page budget is spent. The cursor is saved after every
/// page; the watermark moves only when a pass completes.
#[doc(hidden)]
pub async fn poll(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user: &UserContext,
    service: &Recipe,
    capture_id: Uuid,
    client: &Client,
) -> Response {
    let Some(pool) = tenant.pool.as_ref() else {
        return internal();
    };
    // The member's pause or off switch for this toolkit, before the lease and any
    // provider call. The capture is deferred rather than left due, so a pile of
    // paused captures cannot crowd live ones out of the worker's due list; the
    // cursor and watermark stay, and a resumed toolkit carries on from them.
    if crate::connectors::catalog::pause::is_paused_toolkit_in(state, user.account_id, user.user_id, &service.toolkit).await {
        defer_paused_capture(pool, user, &service.toolkit, capture_id).await;
        return fail(StatusCode::CONFLICT, "Capture is paused");
    }
    let lease = Uuid::new_v4();
    let mut tx =
        match crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id).await
        {
            Ok(tx) => tx,
            Err(_) => return internal(),
        };
    let capture=match sqlx::query_as::<_,Capture>(&format!("UPDATE composio_capture_scopes SET lease_id=$5,lease_until=now()+interval '10 minutes',pass_started_at=COALESCE(pass_started_at,now()),updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND service=$4 AND state IN ('active','retry') AND (lease_until IS NULL OR lease_until<now()) RETURNING {CAPTURE_COLUMNS}")) .bind(capture_id).bind(user.account_id).bind(user.user_id).bind(&service.toolkit).bind(lease).fetch_optional(&mut *tx).await{Ok(Some(s))=>s,Ok(None)=>return fail(StatusCode::CONFLICT,"Capture is paused, disconnected or busy"),Err(_)=>return internal()};
    if tx.commit().await.is_err() {
        return internal();
    }
    let owner = owner_key(user);
    let result = tokio::time::timeout(std::time::Duration::from_secs(180), async {
        let c = connection(state, user, service, capture.connection_id)
            .await
            .map_err(|_| Halt::Ingest)?;
        if c.status != "active" {
            return Err(Halt::Connection(c.status));
        }
        if c.provider_namespace.as_deref() != Some(capture.provider_namespace.as_str()) {
            return Err(Halt::Provider(Error::Reconnect));
        }
        let namespace = capture.provider_namespace.as_str();
        let recheck = || async {
            let live = client
                .namespace(service, &c.connected_account_id, &owner)
                .await?;
            if live != namespace {
                return Err(Halt::Provider(Error::Reconnect));
            }
            let _ = set_namespace_checked(state, user, c.id, Some(namespace)).await;
            Ok::<(), Halt>(())
        };
        // A pending self-check reads a refused call as a recipe failure only
        // because the namespace call just succeeded, so it always re-resolves.
        let mut verified = c.self_check != "passed"
            || c.namespace_checked_at.is_none_or(|t| Utc::now() - t >= NAMESPACE_RECHECK);
        if verified {
            recheck().await?;
        }
        if c.self_check != "passed" {
            let verdict = client
                .self_check(service, &c.connected_account_id, &owner, namespace, Utc::now())
                .await?;
            if record_self_check(state, user, service, c.id, verdict).await.is_err() {
                return Err(Halt::Ingest);
            }
            if matches!(verdict, SelfCheck::Failed(_)) {
                return Err(Halt::SelfCheckFailed);
            }
        }
        // Never before the moment of connect, unless this capture inherited an
        // older watermark from the connection it replaced.
        let mut window = Window::from_watermark(capture.watermark, Utc::now());
        window.since = window.since.max(capture.created_at.min(capture.watermark));
        let mut resume: Resume = capture
            .cursor
            .as_deref()
            .and_then(|c| serde_json::from_str(c).ok())
            .unwrap_or_default();
        // Items ingested since the last saved cursor; saved pages count themselves.
        let (mut unsaved, mut changed) = (0usize, false);
        // The engine refuses a token handed straight back; a provider cycling
        // through several (a, b, a) is caught here before it pages forever.
        let mut visited = vec![resume.clone()];
        for _ in 0..MAX_PAGES_PER_POLL {
            let page = client
                .page(service, &c.connected_account_id, &owner, namespace, &window, &resume, false)
                .await?;
            let n = page.items.len();
            // Item identities carry the namespace, and a multi-site grant (Jira)
            // can be routed to another site, so nothing is ingested unchecked.
            if n > 0 && !verified {
                recheck().await?;
                verified = true;
            }
            // Overlap re-reads land before the watermark; only newer updates
            // count as activity for the poll interval.
            changed |= page.items.iter().any(|i| i.timestamp >= capture.watermark);
            for item in page.items {
                if !ingest_item(state, tenant, user, &capture, item).await {
                    return Err(Halt::Ingest);
                }
            }
            unsaved += n;
            let Some(next) = page.next else {
                return Ok((unsaved, true, changed));
            };
            if visited.contains(&next) {
                return Err(Halt::Provider(Error::Contract("non-advancing item cursor")));
            }
            visited.push(next.clone());
            let cursor = serde_json::to_string(&next).map_err(|_| Halt::Ingest)?;
            let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id)
                .await
                .map_err(|_| Halt::Ingest)?;
            let saved = sqlx::query("UPDATE composio_capture_scopes SET cursor=$3,captured_items=captured_items+$4,last_capture_at=now(),lease_until=now()+interval '10 minutes',updated_at=now() WHERE id=$1 AND lease_id=$2")
                .bind(capture.id).bind(lease).bind(cursor).bind(n as i64).execute(&mut *tx).await;
            if !saved.is_ok_and(|r| r.rows_affected() == 1) || tx.commit().await.is_err() {
                return Err(Halt::Ingest);
            }
            unsaved = 0;
            resume = next;
        }
        Ok((unsaved, false, changed))
    })
    .await
    .unwrap_or(Err(Halt::Provider(Error::Upstream)));
    // Marked before the capture state is saved, so a crash in between costs at
    // most one extra check.
    if let Err(Halt::Provider(Error::Reconnect | Error::PermissionDenied | Error::Gone | Error::Contract(_))) = &result {
        let _ = set_namespace_checked(state, user, capture.connection_id, None).await;
    }
    let mut tx =
        match crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id).await
        {
            Ok(tx) => tx,
            Err(_) => return internal(),
        };
    let now = Utc::now();
    match result {
        Ok((unsaved, complete, changed)) => {
            let last_change = if changed { Some(now) } else { capture.last_change_at };
            let next_poll = if complete {
                now + composio::next_poll_in(now, last_change, capture.created_at)
            } else {
                now
            };
            // A finished pass moves the watermark to when it started: anything
            // updated during the pass is re-read next time, never skipped.
            let saved: Result<Option<(String, DateTime<Utc>)>, _> = sqlx::query_as("UPDATE composio_capture_scopes SET watermark=CASE WHEN $4 THEN COALESCE(pass_started_at,watermark) ELSE watermark END,pass_started_at=CASE WHEN $4 THEN NULL ELSE pass_started_at END,cursor=CASE WHEN $4 THEN NULL ELSE cursor END,captured_items=captured_items+$5,last_capture_at=now(),last_change_at=$6,next_poll_at=$7,state=CASE WHEN state='disconnected' THEN state ELSE 'active' END,retry_at=NULL,error_code=NULL,lease_id=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND account_id=$2 AND lease_id=$3 RETURNING state,next_poll_at")
                .bind(capture.id).bind(user.account_id).bind(lease).bind(complete).bind(unsaved as i64).bind(last_change).bind(next_poll)
                .fetch_optional(&mut *tx).await;
            let Ok(Some((state_name, next_poll_at))) = saved else {
                return internal();
            };
            if tx.commit().await.is_err() {
                return internal();
            }
            Json(json!({"capture_id":capture.id,"state":state_name,"pass_complete":complete,"next_poll_at":next_poll_at})).into_response()
        }
        Err(halt) => {
            let (state_name, code, delay): (&str, Option<&str>, i64) = match &halt {
                Halt::Connection(s) if s == "disconnected" => ("disconnected", None, 0),
                Halt::Connection(s) if s == "needs_attention" => ("needs_attention", Some("self_check_failed"), 0),
                Halt::Connection(_) => ("reconnect_needed", Some("reconnect"), 0),
                Halt::SelfCheckFailed => ("needs_attention", Some("self_check_failed"), 0),
                Halt::Provider(Error::Reconnect | Error::PermissionDenied | Error::Gone) => {
                    ("reconnect_needed", Some("reconnect"), 0)
                }
                Halt::Provider(Error::RateLimited(d)) => ("retry", Some("rate_limited"), (*d).clamp(1, 86_400) as i64),
                // A second consecutive contract failure after the self-check
                // passed means the provider changed under the recipe.
                Halt::Provider(Error::Contract(_)) if capture.error_code.as_deref() == Some("contract") => {
                    ("needs_attention", Some("contract"), 0)
                }
                Halt::Provider(Error::Contract(_)) => ("retry", Some("contract"), 15 * 60),
                Halt::Provider(Error::NotConfigured) => ("retry", Some("not_configured"), 15 * 60),
                Halt::Provider(Error::Upstream) => ("retry", Some("upstream"), 5 * 60),
                Halt::Ingest => ("retry", Some("ingest"), 5 * 60),
            };
            let retry = now + chrono::Duration::seconds(delay);
            let saved=sqlx::query("UPDATE composio_capture_scopes SET state=CASE WHEN state='disconnected' THEN state ELSE $4 END,error_code=$5,retry_at=CASE WHEN $4='retry' THEN $6 ELSE NULL END,next_poll_at=$6,lease_id=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND account_id=$2 AND lease_id=$3")
                .bind(capture.id).bind(user.account_id).bind(lease).bind(state_name).bind(code).bind(retry).execute(&mut *tx).await;
            if !saved.is_ok_and(|r| r.rows_affected() == 1) || tx.commit().await.is_err() {
                return internal();
            }
            match state_name {
                "reconnect_needed" => {
                    // Provider-side revocation: ask the member to reconnect.
                    // Captured atoms stay exactly as they were.
                    if set_connection_status(state, user, capture.connection_id, "reconnect_needed").await.is_err() {
                        return internal();
                    }
                }
                "needs_attention" if code == Some("contract") => {
                    if set_connection_status(state, user, capture.connection_id, "needs_attention").await.is_err() {
                        return internal();
                    }
                    tracing::error!(
                        alert = true,
                        event = "composio_poll_contract_failed",
                        toolkit = %service.toolkit,
                        toolkit_version = %service.version,
                        connection_id = %capture.connection_id,
                        account_id = %user.account_id,
                        "Composio recipe stopped matching provider responses on a verified connection; capture paused"
                    );
                }
                _ => {}
            }
            let body = json!({"capture_id":capture.id,"state":state_name,"error_code":code,"message":connection_message(state_name)});
            let status = match state_name {
                "retry" => StatusCode::SERVICE_UNAVAILABLE,
                "disconnected" => StatusCode::CONFLICT,
                _ => StatusCode::UNPROCESSABLE_ENTITY,
            };
            (status, Json(body)).into_response()
        }
    }
}
pub(crate) async fn ingest_item(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user: &UserContext,
    capture: &Capture,
    item: Item,
) -> bool {
    let Ok(namespace) = ProviderNamespace::new(&capture.provider_namespace) else {
        return false;
    };
    let container = match &item.container {
        Some(c) => match (ContainerKind::new(&c.kind), ContainerId::new(&c.id)) {
            (Ok(kind), Ok(id)) => Some(SourceContainer {
                kind,
                id,
                provider_namespace: namespace.clone(),
                label: c.name.clone(),
            }),
            _ => return false,
        },
        None => None,
    };
    let (Ok(service_id), Ok(content_kind), Ok(surface), Ok(connection_ref)) = (
        ServiceId::new(&capture.service),
        ContentKind::new(&item.kind),
        CaptureSurfaceId::new("composio_poll"),
        CaptureConnectionId::new(format!("composio:{}", capture.connection_id)),
    ) else {
        return false;
    };
    let origin = SourceOrigin {
        service_id,
        transport: CaptureTransport::Composio,
        content_kind,
        capture_surface: Some(surface),
        connection: Some(CaptureConnectionRef {
            kind: CaptureConnectionKind::ConnectorInstall,
            id: connection_ref,
        }),
        container,
    };
    // Changed raw corals are replaced through the existing graph-confined cascade;
    // an unchanged re-read (overlap, retry) matches its content hash and is a no-op.
    let participants = item.author_name.clone().into_iter().collect::<Vec<_>>();
    let Some(pool) = tenant.pool.as_ref() else {
        return false;
    };
    let incoming = crate::services::source_reingest::Incoming {
        content: &item.content,
        timestamp: item.timestamp,
        participants: &participants,
    };
    let blobs = state
        .r2
        .as_deref()
        .map(|r| r as &dyn crate::services::blob_store::PromotionBlobStore);
    if crate::services::source_reingest::retract_if_changed(
        pool,
        blobs,
        user.account_id,
        capture.graph_id,
        &item.source_type,
        &item.source_id,
        &incoming,
    )
    .await
    .is_err()
    {
        return false;
    }
    // Deserialization centralizes the ingest DTO defaults, including future added
    // optional provenance fields, without exposing provider context on HTTP ingest.
    let source_type = item.source_type.clone();
    let source_id = item.source_id.clone();
    let (channel_id, channel_name) = item
        .container
        .as_ref()
        .map(|c| (Some(c.id.clone()), c.name.clone()))
        .unwrap_or_default();
    let req: crate::ingest::atom::IngestAtomRequest = match serde_json::from_value(
        json!({"graph_id":capture.graph_id,"source_type":item.source_type,"source_id":item.source_id,"content":item.content,"participants":participants,"timestamp":item.timestamp,"scope":"private","channel_id":channel_id,"channel_name":channel_name,"author_id":item.author_id,"author_name":item.author_name,"project":channel_name,"mcp_client_name":null,"fts_language":null}),
    ) {
        Ok(r) => r,
        Err(_) => return false,
    };
    // The recipe's capture hints (chunking, the item's own title) travel as
    // an in-process argument, never in the request body.
    let admitted = crate::ingest::submit(crate::ingest::IngestRequest {
        origin: Some(&origin),
        ..crate::ingest::IngestRequest::private(crate::ingest::Member {
            provider_namespace: Some(&capture.provider_namespace),
            hints: item.capture_hints(),
            ..crate::ingest::Member::new(state, tenant, user, req)
        })
    })
    .await
    .is_ok();
    if !admitted {
        return false;
    }
    // A successful authorized re-fetch restores this exact source if an earlier
    // capture had gated it.
    let Ok(mut tx) =
        crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id).await
    else {
        return false;
    };
    if crate::source_evidence::set_source_identity_access_tx(
        &mut tx,
        capture.graph_id,
        &source_type,
        &capture.provider_namespace,
        &source_id,
        "permitted",
    )
    .await
    .is_err()
    {
        return false;
    }
    tx.commit().await.is_ok()
}
#[cfg(test)]
mod tests {
    use super::*;
    fn linear() -> &'static Recipe {
        recipe::get("linear").unwrap()
    }
    #[test]
    fn only_registered_toolkits_route() {
        assert!(parse_service("linear").is_ok());
        assert!(parse_service("jira").is_ok());
        assert!(parse_service("linear_mcp").is_err());
        assert!(parse_service("LINEAR").is_err());
        assert!(parse_service("slack").is_err(), "native toolkits never get a Composio route");
    }
    #[test]
    fn limits_merge_recipe_statements_over_engine_defaults() {
        let l = limits(linear());
        assert!(l["deletions"].as_str().unwrap().starts_with("never"));
        assert_eq!(l["history"], "nothing from before you connect is imported");
        assert!(limits(recipe::get("jira").unwrap())["projects"]
            .as_str()
            .unwrap()
            .contains("not captured"));
    }
    #[test]
    fn every_paused_state_has_a_plain_message() {
        for status in ["pending", "needs_attention", "reconnect_needed", "disconnected"] {
            assert!(connection_message(status).is_some(), "{status}");
        }
        assert_eq!(connection_message("needs_attention"), Some(NOT_WORKING_YET));
        assert!(connection_message("active").is_none());
    }
    #[test]
    fn an_abandoned_sign_in_reads_as_sign_in_not_revocation() {
        assert_eq!(connection_message_for("reconnect_needed", None), Some(SIGN_IN_EXPIRED));
        assert_eq!(
            connection_message_for("reconnect_needed", Some("linear:workspace:w")),
            connection_message("reconnect_needed")
        );
        assert_eq!(connection_message_for("pending", None), connection_message("pending"));
    }
    #[test]
    fn verification_rejects_remote_owner_and_grant_mismatches() {
        let user = UserContext {
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            role: "member".into(),
            device_id: "d".into(),
        };
        let jira = recipe::get("jira").unwrap();
        let mut account = json!({"user_id":owner_key(&user), "toolkit":{"slug":"linear"}, "auth_config":{"id":"read-only"}, "requested_scopes":["read"]});
        assert!(grant_matches(&account, linear(), &user, "read-only"));
        account["user_id"] = json!("coralswarm:another-account:another-member");
        assert!(!grant_matches(&account, linear(), &user, "read-only"));
        account["user_id"] = json!(owner_key(&user));
        assert!(!grant_matches(&account, linear(), &user, "other-config"));
        assert!(!grant_matches(&account, jira, &user, "read-only"));
        account["requested_scopes"] = json!(["read", "write"]);
        assert!(!grant_matches(&account, linear(), &user, "read-only"));
    }
    #[test]
    fn owner_keys_are_member_and_account_specific() {
        let a = UserContext {
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            role: "member".into(),
            device_id: "d".into(),
        };
        let b = UserContext {
            account_id: a.account_id,
            user_id: Uuid::new_v4(),
            role: "member".into(),
            device_id: "d".into(),
        };
        assert_ne!(owner_key(&a), owner_key(&b));
    }
}

/// Owner-resolved recovery. The transaction lock is released even on cancellation.
/// Never hold a row lock across activate (which writes through its own transaction).
pub(crate) async fn recover_catalog(
    state: &AppState, tenant: &Tenant, user: &UserContext, service: &Recipe,
) -> Result<(), Response> {
    fn refusal(code: &'static str, status: StatusCode) -> Response {
        (status, Json(json!({"error": code, "code": code}))).into_response()
    }
    let pool = state.control_db().ok_or_else(internal)?;
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await.map_err(|_| internal())?;
    let key = format!("catalog-recovery:{}:{}:{}", user.account_id, user.user_id, service.toolkit);
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(key).fetch_one(&mut *tx).await.map_err(|_| internal())?;
    if !locked { return Err(refusal("recovery_in_progress", StatusCode::CONFLICT)); }
    let row: Option<(Uuid, String, String, Option<String>)> = sqlx::query_as(
        "SELECT id,status,self_check,provider_namespace FROM composio_connections WHERE account_id=$1 AND owner_user_id=$2 AND service=$3 AND status IN ('active','needs_attention','reconnect_needed') ORDER BY created_at DESC,id DESC LIMIT 1")
        .bind(user.account_id).bind(user.user_id).bind(&service.toolkit)
        .fetch_optional(&mut *tx).await.map_err(|_| internal())?;
    let Some((id,status,check,namespace)) = row else {
        return Err(refusal("not_connected", StatusCode::NOT_FOUND));
    };
    // A passed control-plane verdict can precede a failed tenant capture upsert.
    // Re-enter activation to reconcile it; passed verdicts skip the provider probe.
    let retry_failed = status == "needs_attention" && check == "failed";
    let reconcile_passed = status == "active" && check == "passed";
    if (!retry_failed && !reconcile_passed) || namespace.is_none() {
        return Err(refusal("recovery_not_applicable", StatusCode::CONFLICT));
    }
    let client = Client::configured().map_err(provider_error)?;
    let response = activate(state, tenant, user, service, id, &client).await;
    // Keep the lock alive until the primitive has finished its database writes.
    let result = if response.status().is_success() { Ok(()) } else { Err(response) };
    tx.rollback().await.map_err(|_| internal())?;
    result
}
