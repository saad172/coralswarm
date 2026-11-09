//! Cloud MCP delivery adapter. The in-process registry stores a durable UUID,
//! never answer text. Every delivery reopens fresh admission and full receipts.
use crate::{
    library_read,
    middleware::auth::UserContext,
    scoped_ask::{self, Answer, AskRequest},
    tenancy::extractor::Tenant,
    AppState,
};
use axum::response::Response;
use rmcp::model::ErrorData as McpError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;

fn unavailable() -> McpError {
    McpError::invalid_params("task or scope unavailable", None)
}
pub(crate) fn is_unavailable(error: &McpError) -> bool {
    let unavailable = unavailable();
    error.code == unavailable.code && error.message == unavailable.message && error.data.is_none()
}
fn db_error(_: impl std::fmt::Display) -> McpError {
    McpError::internal_error("scoped MCP task unavailable", None)
}
pub(crate) fn request_error(response: Response) -> McpError {
    match response.status().as_u16() {
        400|409=>McpError::invalid_params("invalid scope, style or changed conversation; provide an explicit LibraryScope and scope_action replace for changes",None),
        404=>unavailable(),
        429=>McpError::internal_error("Ask rate limit exceeded",None),
        _=>db_error("ask")
    }
}
async fn begin(
    tenant: &Tenant,
    user: &UserContext,
) -> Result<Transaction<'static, Postgres>, McpError> {
    let pool = tenant.pool.as_ref().ok_or_else(unavailable)?;
    crate::tenancy::scope::begin_tenant_scoped(pool, user.account_id, user.user_id)
        .await
        .map_err(db_error)
}
/// Stable after JSONB key reordering. The seal includes all actual output,
/// actor/requester/owner/graph/scope/receipt identities, not just cited IDs.
fn canonical(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            let mut m = serde_json::Map::new();
            for k in keys {
                m.insert(k.clone(), canonical(&map[k]));
            }
            Value::Object(m)
        }
        Value::Array(a) => Value::Array(a.iter().map(canonical).collect()),
        _ => v.clone(),
    }
}
fn hash(v: &Value) -> String {
    hex::encode(Sha256::digest(canonical(v).to_string().as_bytes()))
}
#[derive(sqlx::FromRow)]
struct Binding {
    graph_id: Uuid,
    scope_owner_account_id: Option<Uuid>,
    library_scope: Option<Value>,
    temporal_selection: Option<Value>,
    status: String,
    answer_receipt_id: Option<Uuid>,
    result_sha256: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}
fn seal(user: &UserContext, id: Uuid, binding: &Binding, payload: &Value) -> String {
    let mut data=json!({"task_id":id,"requester_account_id":user.account_id,"owner_user_id":user.user_id,"scope_owner_account_id":binding.scope_owner_account_id,"graph_id":binding.graph_id,"library_scope":binding.library_scope,"answer_receipt_id":binding.answer_receipt_id,"result":payload});
    // Previously sealed ordinary tasks have no temporal key. Keep their seal
    // domain unchanged; new bindings explicitly seal the normalized selector.
    if let Some(temporal)=&binding.temporal_selection {data["temporal_selection"]=temporal.clone();}
    hash(&data)
}
async fn admitted_binding(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    id: Uuid,
) -> Result<(Binding, library_read::LibraryScope, Uuid), McpError> {
    // Read only noncontent identity first, under requester RLS. No cached
    // result/error is touched until fresh owning-scope admission is established.
    let mut tx = begin(tenant, user).await?;
    let b:Binding=sqlx::query_as("SELECT graph_id,scope_owner_account_id,library_scope,temporal_selection,status,answer_receipt_id,result_sha256,created_at FROM mcp_tasks WHERE id=$1 AND owner_user_id=$2 AND account_id=$3 AND expires_at>now() AND tool_name='scoped_ask'")
        .bind(id).bind(user.user_id).bind(user.account_id).fetch_optional(&mut *tx).await.map_err(db_error)?.ok_or_else(unavailable)?;
    tx.commit().await.map_err(db_error)?;
    let owner = b.scope_owner_account_id.ok_or_else(unavailable)?;
    let scope: library_read::LibraryScope =
        serde_json::from_value(b.library_scope.clone().ok_or_else(unavailable)?)
            .map_err(db_error)?;
    let mut normalized = scope.clone();
    let temporal=b.temporal_selection.clone().map(serde_json::from_value::<crate::temporal::TemporalSelection>).transpose().map_err(db_error)?.unwrap_or_default();
    temporal.validate(&scope).map_err(|_|unavailable())?;
    if library_read::authorize_scope(tenant, user, &mut normalized)
        .await
        .map_err(request_error)?
        != owner
        || json!(normalized) != json!(scope)
    {
        return Err(unavailable());
    }
    for graph in scope
        .graph_ids
        .iter()
        .copied()
        .chain(std::iter::once(b.graph_id))
    {
        if !crate::source_evidence::still_admitted(state, tenant, user, graph, owner).await {
            return Err(unavailable());
        }
    }
    let mut tx = tenant.begin_member_scoped(owner).await.map_err(db_error)?;
    crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id)
        .await
        .map_err(db_error)?;
    if !library_read::validate_objects_tx(&mut tx, &scope)
        .await
        .map_err(db_error)?
    {
        return Err(unavailable());
    }
    tx.commit().await.map_err(db_error)?;
    Ok((b, scope, owner))
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum ClaimError {
    #[error("initial scoped request admission failed")]
    Request(Response),
    #[error("{0}")]
    Protocol(#[from] McpError),
}

pub(crate) struct Claim {
    pub id: Uuid,
    pub inserted: bool,
    pub request: AskRequest,
}
pub(crate) async fn claim(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    mut request: AskRequest,
    key: Option<String>,
) -> Result<Claim, ClaimError> {
    if request.text.trim().is_empty()
        || request.text.chars().count() > 4096
        || request
            .style
            .as_deref()
            .is_some_and(|s| !matches!(s, "concise" | "detailed" | "sources-only"))
        || request
            .candidate_limit
            .is_some_and(|n| !(1..=193).contains(&n))
        || key
            .as_ref()
            .is_some_and(|k| k.is_empty() || k.chars().count() > 200 || k.contains('\0'))
    {
        return Err(McpError::invalid_params(
            "invalid question, style, candidate limit or idempotency key",
            None,
        ).into());
    }
    request.text = request.text.trim().to_owned();
    let prepared = scoped_ask::prepare_scope(state, tenant, user, &mut request)
        .await
        .map_err(ClaimError::Request)?;
    request.library_scope = Some(prepared.scope.clone());
    request.admitted_recap_window=prepared.recap_window.clone();
    let mut identity=json!({"request":request,"scope_owner_account_id":prepared.account,"primary_graph":prepared.graph});
    if let Some(window)=&prepared.recap_window {identity["resolved_event_window"]=json!(window);}
    let fingerprint=hash(&identity);
    let mut tx = begin(tenant, user).await?;
    // Exact request variants remain independent; only exact canonical normalized
    // request retries reuse a row, following the existing unique key contract.
    let id = Uuid::new_v4();
    let inserted:Option<Uuid>=sqlx::query_scalar("INSERT INTO mcp_tasks(id,account_id,graph_id,owner_user_id,tool_name,status,idempotency_key,idempotency_request_hash,library_scope,scope_owner_account_id,temporal_selection) VALUES($1,$2,$3,$4,'scoped_ask','working',$5,$6,$7,$8,$9) ON CONFLICT(owner_user_id,account_id,tool_name,idempotency_key,idempotency_request_hash) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id")
        .bind(id).bind(user.account_id).bind(prepared.graph).bind(user.user_id).bind(&key).bind(&fingerprint).bind(json!(prepared.scope)).bind(prepared.account).bind(json!(prepared.temporal)).fetch_optional(&mut *tx).await.map_err(db_error)?;
    let claimed = if let Some(id) = inserted {
        id
    } else {
        sqlx::query_scalar("SELECT id FROM mcp_tasks WHERE account_id=$1 AND owner_user_id=$2 AND tool_name='scoped_ask' AND idempotency_key=$3 AND idempotency_request_hash=$4 AND created_at>now()-interval '5 minutes' AND expires_at>now() AND status IN('working','completed')")
            .bind(user.account_id).bind(user.user_id).bind(key).bind(fingerprint).fetch_optional(&mut *tx).await.map_err(db_error)?.ok_or_else(||McpError::invalid_params("expired or failed idempotency key; use a new key",None))?
    };
    tx.commit().await.map_err(db_error)?;
    admitted_binding(state, tenant, user, claimed).await?;
    Ok(Claim {
        id: claimed,
        inserted: inserted.is_some(),
        request,
    })
}
pub(crate) async fn run(
    state: Arc<AppState>,
    tenant: Tenant,
    user: UserContext,
    id: Uuid,
    request: AskRequest,
) -> Result<(), McpError> {
    #[cfg(feature = "test-support")]
    { run_inner(state, tenant, user, id, request, None).await }
    #[cfg(not(feature = "test-support"))]
    { run_inner(state, tenant, user, id, request).await }
}
#[cfg(feature = "test-support")]
pub(crate) async fn run_before_completion(
    state: Arc<AppState>, tenant: Tenant, user: UserContext, id: Uuid,
    request: AskRequest, ready: Arc<tokio::sync::Notify>, resume: Arc<tokio::sync::Notify>,
) -> Result<(), McpError> {
    run_inner(state, tenant, user, id, request, Some((ready, resume))).await
}
async fn run_inner(
    state: Arc<AppState>, tenant: Tenant, user: UserContext, id: Uuid, request: AskRequest,
    #[cfg(feature = "test-support")] completion_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
) -> Result<(), McpError> {
    let result=async {
        let (initial,scope,owner)=admitted_binding(&state,&tenant,&user,id).await?;
        let answer=scoped_ask::execute(&state,&tenant,&user,request).await.map_err(request_error)?;
        crate::mcp::tools::read::check_retained_output(&answer.payload)?;
        if answer.account!=owner || json!(answer.scope)!=json!(scope) || answer.payload["temporal"]!=initial.temporal_selection.unwrap_or_else(||json!(crate::temporal::TemporalSelection::default())) || !scoped_ask::emission_allowed(&state,&tenant,&user,&answer).await {return Err(unavailable());}
        let (mut b,_,_)=admitted_binding(&state,&tenant,&user,id).await?;
        if b.status!="working" {return Err(unavailable());}
        b.answer_receipt_id=Some(answer.receipt_id);
        let digest=seal(&user,id,&b,&answer.payload);
        // The fresh actor/receipt fence opens its own transactions. Complete it
        // before reserving the completion connection, so concurrent workers (or
        // a one-connection tenant pool) cannot deadlock while acquiring another.
        if !scoped_ask::emission_allowed(&state,&tenant,&user,&answer).await {return Err(unavailable());}
        #[cfg(feature = "test-support")]
        if let Some((ready, resume)) = completion_gate {
            ready.notify_one();
            resume.notified().await;
        }
        let mut tx=begin(&tenant,&user).await?;
        // Acquisition may have waited through a revocation. Recheck on this
        // executor instead of opening another content-pool transaction.
        if !scoped_ask::emission_allowed_tx(&state,&tenant,&user,&answer,&mut tx).await.map_err(db_error)? {return Err(unavailable());}
        let changed=sqlx::query("UPDATE mcp_tasks SET status='completed',result=$4,answer_receipt_id=$5,result_sha256=$6,updated_at=now() WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND status='working'")
            .bind(id).bind(user.account_id).bind(user.user_id).bind(&answer.payload).bind(answer.receipt_id).bind(digest).execute(&mut *tx).await.map_err(db_error)?.rows_affected();
        tx.commit().await.map_err(db_error)?;
        if changed!=1 {return Err(unavailable());} Ok(())
    }.await;
    if result.is_err() {
        // Never persist raw errors/prompts/model output on a failed path.
        if let Ok(mut tx) = begin(&tenant, &user).await {
            let _=sqlx::query("UPDATE mcp_tasks SET status='failed',error=jsonb_build_object('code','unavailable','message','Scoped answer unavailable'),updated_at=now() WHERE id=$1 AND owner_user_id=$2 AND account_id=$3 AND status='working'")
                .bind(id).bind(user.user_id).bind(user.account_id).execute(&mut *tx).await;
            let _ = tx.commit().await;
        }
    }
    result
}
/// Typed envelope shared by legacy poll and modern result expansion.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Delivery {
    pub task_id: Uuid,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub age_seconds: i64,
    pub result: Option<Value>,
}
pub(crate) async fn poll(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    id: Uuid,
) -> Result<Delivery, McpError> {
    let (mut b, scope, owner) = admitted_binding(state, tenant, user, id).await?;
    let mut tx = begin(tenant, user).await?;
    // Completion may race the metadata admission read. Load terminal metadata
    // and output atomically, bound to the immutable admitted scope identities;
    // no retry loop or stale working-status predicate can lose a valid result.
    let (status,receipt,digest,payload):(String,Option<Uuid>,Option<String>,Option<Value>)=sqlx::query_as("SELECT status,answer_receipt_id,result_sha256,result FROM mcp_tasks WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND graph_id=$4 AND library_scope=$5 AND scope_owner_account_id=$6 AND temporal_selection IS NOT DISTINCT FROM $7::jsonb AND tool_name='scoped_ask' AND expires_at>now()")
        .bind(id).bind(user.account_id).bind(user.user_id).bind(b.graph_id).bind(json!(scope)).bind(owner).bind(&b.temporal_selection).fetch_optional(&mut *tx).await.map_err(db_error)?.ok_or_else(unavailable)?;
    b.status=status;b.answer_receipt_id=receipt;b.result_sha256=digest;
    tx.commit().await.map_err(db_error)?;
    let result = if b.status == "completed" {
        let payload = payload.ok_or_else(unavailable)?;
        let receipt = b.answer_receipt_id.ok_or_else(unavailable)?;
        if b.result_sha256.as_deref() != Some(seal(user, id, &b, &payload).as_str())
            || payload["query_id"] != json!(receipt)
            || payload["library_scope"] != json!(scope)
            || payload.get("temporal").cloned().unwrap_or_else(||json!(crate::temporal::TemporalSelection::default())) != b.temporal_selection.clone().unwrap_or_else(||json!(crate::temporal::TemporalSelection::default()))
            || payload["support_status"] != "complete"
            || !matches!(
                payload["mode"].as_str(),
                Some("synthesized" | "retrieval_only")
            )
        {
            return Err(unavailable());
        }
        crate::mcp::tools::read::check_retained_output(&payload)?;
        let answer = Answer {
            generated: payload["mode"] == "synthesized",
            payload: payload.clone(),
            scope,
            account: owner,
            primary_graph: b.graph_id,
            receipt_id: receipt,
        };
        if !scoped_ask::emission_allowed(state, tenant, user, &answer).await {
            return Err(unavailable());
        }
        Some(payload)
    } else {
        // No raw cached error/result fields for pending/failed/cancelled rows.
        admitted_binding(state, tenant, user, id).await?;
        None
    };
    Ok(Delivery {
        task_id: id,
        status: b.status,
        created_at: b.created_at,
        age_seconds: (chrono::Utc::now() - b.created_at).num_seconds(),
        result,
    })
}

pub(crate) async fn admit_task(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    id: Uuid,
) -> Result<(), McpError> {
    admitted_binding(state, tenant, user, id).await.map(|_| ())
}

pub(crate) async fn fail_task(tenant: &Tenant, user: &UserContext, id: Uuid) {
    if let Ok(mut tx) = begin(tenant, user).await {
        let _=sqlx::query("UPDATE mcp_tasks SET status='failed',error=jsonb_build_object('code','unavailable','message','Scoped answer unavailable'),updated_at=now() WHERE id=$1 AND owner_user_id=$2 AND account_id=$3 AND status='working'")
            .bind(id).bind(user.user_id).bind(user.account_id).execute(&mut *tx).await;
        let _ = tx.commit().await;
    }
}
