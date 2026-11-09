//! One browser request, independently certified single-account stored sections.
//! Personal sharing fans in indexed rows; no counts/content are computed here.
use crate::{
    middleware::auth::UserContext,
    read_models::{
        self as rm,
        oceans_pages::{OceansIndex, OceansParams, OceansPayload},
        ReadModel,
    },
    tenancy::{extractor::Tenant, runtime::TenancyDecision},
    AppState,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{Postgres, Row, Transaction};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use uuid::Uuid;

#[derive(Serialize)]
pub struct Component {
    account_id: Uuid,
    anchor_graph_id: Option<Uuid>,
    refresh_path: String,
    stored: rm::Stored<OceansPayload>,
}
#[derive(Serialize)]
pub struct Index {
    components: Vec<Component>,
}
struct Context {
    anchor: Uuid,
    scope: rm::Scope<OceansParams>,
    access: rm::access::ReadAccess,
}
fn unavailable(e: impl std::fmt::Display) -> Response {
    tracing::warn!(error=%e,"OceansIndex stored fan-in unavailable");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"error":"ocean index unavailable"})),
    )
        .into_response()
}
fn sql_unavailable(error: sqlx::Error) -> Response {
    // Canonical scoped-SQL accounting records a PageRead timeout exactly once.
    unavailable(crate::store::StoreError::from(error))
}
fn empty_query(q: &BTreeMap<String, String>) -> Result<(), Response> {
    if !q.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"ocean index accepts no selectors"})),
        )
            .into_response());
    }
    Ok(())
}
const CONTEXTS_SQL: &str = r#"
WITH anchors AS MATERIALIZED (
 SELECT a.account_id,(array_agg(r.id ORDER BY r.id))[1] anchor
 FROM resolve_reachable_graphs($1) r
 CROSS JOIN LATERAL (SELECT read_model_graph_account(r.id) account_id OFFSET 0) a
 WHERE a.account_id IS NOT NULL GROUP BY a.account_id
)
SELECT c.* FROM anchors a CROSS JOIN LATERAL read_model_ocean_context(a.anchor) c ORDER BY c.account_id
"#;
// `read_model_ocean_stored_certified` (control 0422) returns the columns
// `rm::store::read` selects, including the `failed` and `identity` signals
// `decode` withholds on.
const STORED_SQL: &str = r#"
SELECT p.ordinality,s.* FROM unnest($1::uuid[],$2::text[]) WITH ORDINALITY p(anchor,scope_key,ordinality)
LEFT JOIN LATERAL read_model_ocean_stored_certified(p.anchor,p.scope_key,$3) s ON true ORDER BY p.ordinality
"#;
async fn personal_contexts(
    tenant: &Tenant,
    user: &UserContext,
) -> Result<(Transaction<'static, Postgres>, Vec<Context>), Response> {
    let mut tx = tenant
        .begin_member_repeatable_read(user.account_id)
        .await
        .map_err(unavailable)?;
    crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id)
        .await
        .map_err(unavailable)?;
    sqlx::query("SET LOCAL statement_timeout='1s'")
        .execute(&mut *tx)
        .await
        .map_err(sql_unavailable)?;
    let rows = sqlx::query(CONTEXTS_SQL)
        .bind(user.user_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(sql_unavailable)?;
    let mut contexts = Vec::with_capacity(rows.len());
    // The existing personal reachability function bounds the graph set to 500.
    // This loop constructs keys only: no per-component request transaction/query.
    for row in rows {
        let account = row.try_get("account_id").map_err(unavailable)?;
        let windows: BTreeMap<Uuid, Option<DateTime<Utc>>> =
            serde_json::from_value(row.try_get("windows").map_err(unavailable)?)
                .map_err(unavailable)?;
        let viewer = rm::Viewer {
            account_id: account,
            user_id: user.user_id,
            windows,
            control_revision: Some(row.try_get("revision").map_err(unavailable)?),
            control_freshness: row.try_get("freshness").map_err(unavailable)?,
        };
        let authority: bool = row.try_get("authority").map_err(unavailable)?;
        let authority = authority && crate::middleware::auth::authority_allowed();
        contexts.push(Context {
            anchor: row.try_get("anchor").map_err(unavailable)?,
            scope: viewer
                .scope::<OceansIndex>(OceansParams::default())
                .map_err(unavailable)?,
            access: rm::access::ReadAccess {
                admin_fields: authority,
                account_authority: authority,
            },
        });
    }
    Ok((tx, contexts))
}
fn refresh_path(anchor: Option<Uuid>) -> String {
    match anchor {
        Some(g) => format!("/v1/load/oceans/component/refresh?graph={g}"),
        None => "/v1/load/oceans/component/refresh".into(),
    }
}
pub async fn get(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    original: Option<Extension<TenancyDecision>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Result<Json<Index>, Response> {
    empty_query(&query)?;
    if tenant.org_context().is_some() {
        let stored = super::page_read::serve::<OceansIndex>(
            State(state),
            tenant,
            Extension(user.clone()),
            original,
            Path(BTreeMap::new()),
            Query(BTreeMap::new()),
        )
        .await?
        .0;
        return Ok(Json(Index {
            components: vec![Component {
                account_id: user.account_id,
                anchor_graph_id: None,
                refresh_path: refresh_path(None),
                stored,
            }],
        }));
    }
    let (mut tx, contexts) = personal_contexts(&tenant, &user).await?;
    let anchors: Vec<Uuid> = contexts.iter().map(|c| c.anchor).collect();
    let keys: Vec<&str> = contexts.iter().map(|c| c.scope.key()).collect();
    let rows = sqlx::query(STORED_SQL)
        .bind(&anchors)
        .bind(&keys)
        .bind(OceansIndex::VERSION)
        .fetch_all(&mut *tx)
        .await
        .map_err(sql_unavailable)?;
    let mut rows: HashMap<usize, _> = rows
        .into_iter()
        .map(|r| {
            let i = r.get::<i64, _>("ordinality") as usize - 1;
            (i, r)
        })
        .collect();
    let mut components = Vec::with_capacity(contexts.len());
    let mut refresh = vec![];
    let mut wake = false;
    for (i, c) in contexts.into_iter().enumerate() {
        let mut stored =
            rm::store::decode::<OceansIndex>(rows.remove(&i), &c.scope).map_err(unavailable)?;
        wake |= stored.stale;
        // Same contract as `page_read::serve`: never computed, withheld, behind
        // (served stale) or stale without an ETA is enqueued now, and the
        // enqueue owes the client an ETA, or the web treats the stale answer
        // as settled and never polls.
        if stored.owe_refresh(Utc::now()) {
            refresh.push(c.scope.clone());
        }
        OceansIndex::present(&mut stored.payload, c.access);
        components.push(Component {
            account_id: c.scope.account_id,
            anchor_graph_id: Some(c.anchor),
            refresh_path: refresh_path(Some(c.anchor)),
            stored,
        });
    }
    tx.rollback().await.map_err(sql_unavailable)?;
    if wake {
        state.worker_activity.note_consumer_enqueue();
    }
    if !refresh.is_empty() {
        let pool = tenant
            .pool
            .clone()
            .ok_or_else(|| unavailable("content pool unavailable"))?;
        let activity = state.worker_activity.clone();
        tokio::spawn(async move {
            use futures::{stream, StreamExt};
            stream::iter(refresh)
                .for_each_concurrent(4, |s| {
                    let pool = &pool;
                    let activity = &activity;
                    async move {
                        match rm::store::enqueue::<OceansIndex>(pool, &s).await {
                            Ok(()) => activity.note_consumer_enqueue(),
                            Err(e) => tracing::warn!(error=%e,"ocean component enqueue failed"),
                        }
                    }
                })
                .await;
        });
    }
    Ok(Json(Index { components }))
}
pub async fn refresh(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Result<(StatusCode, Json<serde_json::Value>), Response> {
    empty_query(&query)?;
    if tenant.org_context().is_some() {
        // The component helper retains current plane/actor/credential gates.
        let account = user.account_id;
        let (status, Json(mut body)) = super::page_read::refresh_handler::<OceansIndex>(
            State(state),
            tenant,
            Extension(user),
            Path(BTreeMap::new()),
            Query(BTreeMap::new()),
        )
        .await?;
        body["components"] = serde_json::json!([{"account_id":account,"anchor_graph_id":null,"refresh_path":refresh_path(None)}]);
        return Ok((status, Json(body)));
    }
    let (tx, contexts) = personal_contexts(&tenant, &user).await?;
    tx.rollback().await.map_err(sql_unavailable)?;
    let identities:Vec<_>=contexts.iter().map(|c|serde_json::json!({"account_id":c.scope.account_id,"anchor_graph_id":c.anchor,"refresh_path":refresh_path(Some(c.anchor))})).collect();
    let pool = tenant
        .pool
        .as_ref()
        .ok_or_else(|| unavailable("content pool unavailable"))?;
    use futures::{stream, StreamExt, TryStreamExt};
    stream::iter(contexts)
        .map(|c| async move {
            rm::store::request_refresh::<OceansIndex>(pool, &c.scope)
                .await
                .map_err(unavailable)
        })
        .buffer_unordered(4)
        .try_collect::<Vec<_>>()
        .await?;
    state.worker_activity.note_consumer_enqueue();
    Ok((
        StatusCode::ACCEPTED,
        Json(
            serde_json::json!({"components":identities,"refresh_eta":Utc::now()+chrono::Duration::seconds(5)}),
        ),
    ))
}
