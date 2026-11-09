//! `GET /v1/library` — typed topics across every reachable workspace.
//!
//! Numbered pages over one ordered set. Do not fan out `GET /v1/nodes` per
//! graph: merging page N from each ocean is only correct for page 1.
//! Consumer-plane reads are scoped per owning account (RLS is
//! `graphs.account_id = app.account_id`); a single `user.account_id`
//! transaction would hide reachable topics in other accounts. The org
//! tenant path stays one member-scoped transaction — tenant-local,
//! user-aware RLS.

use std::collections::HashMap;

use axum::{extract::Query, http::StatusCode, Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::store::library::{
    library_page_bounds, list_library_topics, parse_library_kind, LibraryTopic, LIBRARY_PAGE_SIZE,
    LIBRARY_Q_MAX,
};
use crate::store::Store;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::reachability::ReachabilityResolver;

macro_rules! require_store {
    ($tenant:expr) => {
        match &$tenant.store {
            Some(s) => s,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
            }
        }
    };
}

#[derive(Debug, Deserialize)]
pub struct ListLibraryQuery {
    pub graph_id: Option<Uuid>,
    pub entity_type: Option<String>,
    pub q: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

pub async fn list_library(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<ListLibraryQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    let entity_type = match params.entity_type.as_deref() {
        None => None,
        Some(t) => match parse_library_kind(t) {
            Some(k) => Some(k),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        json!({"error": "entity_type must be one of: person, org, topic, project"}),
                    ),
                )
            }
        },
    };

    let q = params.q.as_deref().map(str::trim).filter(|s| !s.is_empty());
    if let Some(raw) = q {
        if raw.chars().count() > LIBRARY_Q_MAX {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("q must be at most {LIBRARY_Q_MAX} characters")})),
            );
        }
    }

    let graphs = match ReachabilityResolver::list_reachable_graphs(
        tenant.decision.as_ref(),
        store.as_ref(),
        user.user_id,
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, "list_library reachability error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    let graph_ids: Vec<Uuid> = if let Some(want) = params.graph_id {
        if !graphs.iter().any(|g| g.id == want) {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "graph not found or access denied"})),
            );
        }
        vec![want]
    } else {
        graphs.iter().map(|g| g.id).collect()
    };

    let Some((offset, page_size)) = library_page_bounds(
        params.page.unwrap_or(1),
        params.page_size.unwrap_or(LIBRARY_PAGE_SIZE),
    ) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "page is too large"})),
        );
    };

    if tenant.pool.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    }

    let result = if tenant.org_context().is_some() {
        query_library(
            &tenant,
            user.account_id,
            &graph_ids,
            entity_type,
            q,
            offset,
            page_size,
        )
        .await
    } else {
        query_library_consumer(
            store.as_ref(),
            &tenant,
            &graph_ids,
            params.graph_id.is_some(),
            entity_type,
            q,
            offset,
            page_size,
        )
        .await
    };

    match result {
        Ok((topics, total)) => (
            StatusCode::OK,
            Json(json!({
                "topics": topics.iter().map(|t| t.to_json()).collect::<Vec<_>>(),
                "page": params.page.unwrap_or(1).max(1),
                "page_size": page_size,
                "total": total,
            })),
        ),
        Err(QueryErr::Forbidden) => (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "graph not found or access denied"})),
        ),
        Err(QueryErr::Other(e)) => {
            tracing::error!(error = %e, "list_library query error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
        }
    }
}

enum QueryErr {
    Forbidden,
    Other(String),
}

async fn query_library(
    tenant: &Tenant,
    account_id: Uuid,
    graph_ids: &[Uuid],
    entity_type: Option<&str>,
    q: Option<&str>,
    offset: i64,
    limit: i64,
) -> Result<(Vec<LibraryTopic>, i64), QueryErr> {
    let mut tx = tenant
        .begin_member_scoped(account_id)
        .await
        .map_err(|e| QueryErr::Other(format!("scope: {e}")))?;
    let result = list_library_topics(&mut tx, graph_ids, entity_type, q, offset, limit)
        .await
        .map_err(|e| QueryErr::Other(format!("query: {e}")))?;
    if let Err(e) = tx.commit().await {
        tracing::warn!(error = %e, "list_library read transaction commit failed");
    }
    Ok(result)
}

async fn query_library_consumer(
    store: &dyn Store,
    tenant: &Tenant,
    graph_ids: &[Uuid],
    selected_workspace: bool,
    entity_type: Option<&str>,
    q: Option<&str>,
    offset: i64,
    page_size: i64,
) -> Result<(Vec<LibraryTopic>, i64), QueryErr> {
    // ONE round trip resolving every reachable graph's owning account,
    // instead of one sequential round trip PER graph (PR D perf fix —
    // migration `0225_resolve_graph_account_ids`'s header has the production
    // evidence: each such round trip on this path was paying highly variable
    // network latency, up to ~80-160ms observed, against sub-millisecond
    // query execution).
    let accounts = store
        .graph_account_ids(graph_ids)
        .await
        .map_err(|e| QueryErr::Other(format!("graph_account_ids: {e}")))?;

    let mut by_account: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for gid in graph_ids {
        match accounts.get(gid) {
            Some(acct) => by_account.entry(*acct).or_default().push(*gid),
            None if selected_workspace => return Err(QueryErr::Forbidden),
            None => {
                tracing::warn!(graph_id = %gid, "list_library: reachable graph has no owning account");
            }
        }
    }
    if by_account.is_empty() {
        return Ok((Vec::new(), 0));
    }
    if by_account.len() == 1 {
        let (acct, gids) = by_account.into_iter().next().expect("len == 1");
        return query_library(tenant, acct, &gids, entity_type, q, offset, page_size).await;
    }

    // Each account's read is its own independent transaction on its own
    // pooled connection (`query_library` -> `begin_member_scoped` opens a
    // fresh one every call) — nothing here is shared mutable state, so
    // running them CONCURRENTLY rather than summing their round trips
    // sequentially is safe and, per PR D's evidence, the other half of the
    // round-trip-count fix (a founder with graphs split across N accounts
    // used to pay N times the per-account round-trip cost, serialized).
    let window = offset.saturating_add(page_size).max(page_size);
    let results = futures::future::try_join_all(by_account.into_iter().map(|(acct, gids)| {
        async move { query_library(tenant, acct, &gids, entity_type, q, 0, window).await }
    }))
    .await?;

    let mut merged = Vec::new();
    let mut total: i64 = 0;
    for (topics, t) in results {
        total = total.saturating_add(t);
        merged.extend(topics);
    }
    merged.sort_by(|a, b| {
        b.updated_at
            .cmp(&a.updated_at)
            .then_with(|| b.node_id.cmp(&a.node_id))
    });
    let page = merged
        .into_iter()
        .skip(offset as usize)
        .take(page_size as usize)
        .collect();
    Ok((page, total))
}
