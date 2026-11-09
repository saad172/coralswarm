//! Compatibility entry points for existing meeting links. Canonical evidence is
//! owned by source items; these wrappers resolve the meeting projection's item.
//! A native-ID collision never retargets that projection. Other identities use
//! the generic source-item UUID route even when their native IDs are identical.
use crate::{middleware::auth::UserContext, source_evidence, tenancy::extractor::Tenant, AppState};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;

pub use source_evidence::{
    atom_json, get_node_atoms, load_version_scoped, queue_extraction_tx, CaptureOutcome,
    SourceVersion,
};

#[allow(clippy::too_many_arguments)]
pub async fn capture_version_tx(
    tx: &mut Transaction<'_, Postgres>,
    state: &Arc<AppState>,
    graph_id: Uuid,
    account_id: Uuid,
    source_id: &str,
    raw: &str,
    duration_ms: Option<i64>,
    segments: Option<&Value>,
    org_id: Option<&str>,
    dek: &[u8; 32],
) -> Result<CaptureOutcome, String> {
    source_evidence::capture_source_version_tx(
        tx,
        state.r2.as_deref(),
        graph_id,
        account_id,
        &source_evidence::CaptureSource {
            source_type: "transcript",
            provider_namespace: "legacy:meeting",
            source_id,
            normalized_text: raw,
            representation: "normalized_text",
            metadata: &json!({}),
            occurred_at: None,
            participants: &[],
            ingested_by: None,
            scope: "private",
            duration_ms,
            segments,
        },
        org_id,
        dek,
    )
    .await
}

#[derive(Deserialize)]
pub struct EvidenceQuery {
    pub graph_id: Uuid,
    pub source_id: String,
    pub version_id: Option<Uuid>,
    pub before: Option<String>,
    pub atoms_after: Option<String>,
    pub atom_id: Option<Uuid>,
}

async fn resolve(
    tenant: &Tenant,
    user: &UserContext,
    graph: Uuid,
    source: &str,
) -> Result<Uuid, (StatusCode, Json<Value>)> {
    let account = source_evidence::authorize(tenant, user, graph).await?;
    let result=async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        let id:Option<Uuid>=sqlx::query_scalar("SELECT source_item_id FROM meeting_transcripts WHERE graph_id=$1 AND source_id=$2 AND source_item_id IS NOT NULL")
            .bind(graph).bind(source).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok::<_,crate::store::StoreError>(id)
    }.await;
    match result {
        Ok(Some(id)) => Ok(id),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error":"meeting not found or access denied"})),
        )),
        Err(error) => {
            tracing::error!(%error,"meeting evidence identity failed");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":"internal error"})),
            ))
        }
    }
}

pub async fn get_evidence(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(q): Query<EvidenceQuery>,
) -> (StatusCode, Json<Value>) {
    let source_item_id = match resolve(&tenant, &user, q.graph_id, &q.source_id).await {
        Ok(id) => id,
        Err(e) => return e,
    };
    let (status, Json(mut response)) = source_evidence::get_evidence(
        State(state),
        tenant,
        Extension(user),
        Query(source_evidence::EvidenceQuery {
            graph_id: q.graph_id,
            source_item_id,
            version_id: q.version_id,
            before: q.before,
            atoms_after: q.atoms_after,
            atom_id: q.atom_id,
        }),
    )
    .await;
    if let Some(obj) = response.as_object_mut() {
        obj.insert("source_id".into(), json!(q.source_id));
    }
    (status, Json(response))
}

#[derive(Deserialize)]
pub struct RetryRequest {
    pub graph_id: Uuid,
    pub source_id: String,
    pub version_id: Uuid,
}
pub async fn retry_evidence(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(q): Json<RetryRequest>,
) -> (StatusCode, Json<Value>) {
    let source_item_id = match resolve(&tenant, &user, q.graph_id, &q.source_id).await {
        Ok(id) => id,
        Err(e) => return e,
    };
    source_evidence::retry_evidence(
        tenant,
        Extension(user),
        Json(source_evidence::RetryRequest {
            graph_id: q.graph_id,
            source_item_id,
            version_id: q.version_id,
        }),
    )
    .await
}
