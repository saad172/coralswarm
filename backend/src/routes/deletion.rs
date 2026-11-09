//! Deletion routes (§7.3 of docs/plans/graph-temporal-memory.md) — the three
//! entry points over `services::deletion::delete_atom_cascade`.
//!
//! - `DELETE /v1/graphs/{graph_id}/corals/{atom_id}`
//!   — **coral**: delete one atom (+ any promoted copy elsewhere in the
//!   account).
//! - `DELETE /v1/graphs/{graph_id}/sources`
//!   (query: `source_type`, `source_id`)
//!   — **source**: delete every atom, anywhere in the account, matching one
//!   ingested source event.
//! - `DELETE /v1/graphs/{graph_id}/subjects/{node_id}`
//!   — **subject**: delete every coral currently linked to a person-reef.
//!   **Approximate** — see the response's own `caveat` field and
//!   `services::deletion::delete_subject`'s doc comment.
//!
//! # Why these are NOT `DELETE /v1/graphs/{graph_id}/atoms/{atom_id}`
//!
//! That path is already taken by `unpublish_atom_route` (§4.9) — a
//! DIFFERENT operation on a target-side promoted copy, gated on "are you the
//! original promoter". These routes delete from the CALLER'S OWN ocean(s),
//! gated differently (see below) — conflating the two paths would either
//! silently change unpublish's authorization or require this module to
//! smuggle in retraction's promoter check where it does not belong. See
//! `services::deletion`'s module doc, "What did NOT carry over from §4.9".
//!
//! # Authorization
//!
//! `coral`/`source`: `ReachabilityResolver::graph_reachable_by_user` — the
//! same bar every other ocean-mutating route in this router already clears.
//! `subject`: the SAME reachability precondition, PLUS `services::deletion::
//! delete_subject`'s own `owner`/`admin` role gate (enforced inside that
//! function, not only here) — see its doc comment for why this one entry
//! point is held to a stronger bar than the other two.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    services::{blob_store::PromotionBlobStore, deletion},
    tenancy::extractor::Tenant,
    tenancy::reachability::ReachabilityResolver,
    AppState,
};

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

/// Shared reachability precondition — identical shape to
/// `routes::nodes::unpublish_atom_route`'s own gate. Returns `Some(response)`
/// when the caller should be short-circuited (not reachable, or a resolver
/// error), `None` when the caller may proceed.
async fn require_reachable(
    tenant: &Tenant,
    store: &dyn crate::store::Store,
    graph_id: Uuid,
    user_id: Uuid,
) -> Option<(StatusCode, Json<Value>)> {
    match ReachabilityResolver::graph_reachable_by_user(
        tenant.decision.as_ref(),
        store,
        graph_id,
        user_id,
    )
    .await
    {
        Ok(true) => None,
        Ok(false) => Some((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "ocean not found"})),
        )),
        Err(e) => {
            tracing::error!(error = %e, "deletion route: reachability error");
            Some((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            ))
        }
    }
}

/// D6 (B2 unit 02): `surface_outcomes` here is the SAME data
/// `services::retraction_audit::record` persists to `retraction_audit_log`,
/// serialized synchronously in the response too — so the (separately built)
/// post-retract report can render "reached and unreached copies in one
/// list" immediately, without a second round trip, using exactly the
/// `removed`/`out_of_window`/`not_a_member` vocabulary
/// (`services::deletion::outcome`). Persistence (the audit row) and this
/// response are two views of the same call; neither is the other's source
/// of truth for what actually happened — `services::deletion` is.
fn cascade_json(outcome: &deletion::CascadeOutcome) -> Value {
    json!({
        // D6 UI (built separately from B2 unit 02/#718 — the caller-supplied
        // id every surface_outcome row below shares; also the key
        // `GET /v1/retractions/{retraction_id}` (routes::retractions) reads
        // back, so a caller that lost this response (e.g. a page reload) can
        // still re-hydrate the same report from the durable audit table.
        "retraction_id": outcome.retraction_id,
        "atoms_deleted": outcome.atoms_deleted.iter().map(|(g, a)| json!({
            "graph_id": g, "atom_id": a,
        })).collect::<Vec<_>>(),
        "deleted_reef_ids": outcome.deleted_reef_ids.iter().map(|(g, n)| json!({
            "graph_id": g, "node_id": n,
        })).collect::<Vec<_>>(),
        "propagated_graph_ids": outcome.propagated_graph_ids,
        "surface_outcomes": outcome.surface_outcomes.iter().map(|o| json!({
            "graph_id": o.graph_id,
            "is_personal_ocean": o.is_personal_ocean,
            "source_type": o.source_type,
            "source_id": o.source_id,
            "atom_id": o.atom_id,
            "surface": o.surface,
            "outcome": o.outcome,
            "survivors": o.survivors,
        })).collect::<Vec<_>>(),
    })
}

// ── Entry point 1: coral ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
pub struct DeleteQuery {
    /// Best-effort device id for the sync tombstone ops this write emits.
    /// Defaults to the fixed cloud-write marker, same convention as
    /// `unpublish_atom_route`.
    #[serde(default)]
    pub device_id: Option<String>,
}

#[utoipa_path(
    delete,
    path = "/v1/graphs/{graph_id}/corals/{atom_id}",
    tag = "deletion",
    security(("bearer_token" = [])),
    params(
        ("graph_id" = Uuid, Path, description = "Ocean the coral lives in"),
        ("atom_id" = Uuid, Path, description = "The coral (atom) to delete")
    ),
    responses(
        (status = 200, description = "Deleted (or already absent — idempotent)"),
        (status = 404, description = "Ocean not reachable by caller"),
        (status = 503, description = "Database or blob storage unavailable"),
    )
)]
pub async fn delete_coral_route(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((graph_id, atom_id)): Path<(Uuid, Uuid)>,
    Query(q): Query<DeleteQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    if let Some(resp) = require_reachable(&tenant, store.as_ref(), graph_id, user.user_id).await {
        return resp;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };
    let blobs = state.r2.as_deref().map(|r| r as &dyn PromotionBlobStore);
    let device_id = q
        .device_id
        .as_deref()
        .unwrap_or(crate::store::CLOUD_DEVICE_ID);

    match deletion::delete_coral(
        &pool,
        blobs,
        device_id,
        graph_id,
        atom_id,
        crate::services::connector_audit::Actor::User(user.user_id),
        state.control_db(), // FM-5 (B2 unit 02): CONTROL-plane retraction_audit_log write, not content.
    )
    .await
    {
        Ok(outcome) => (StatusCode::OK, Json(cascade_json(&outcome))),
        Err(e) => {
            tracing::error!(error = %e, graph_id = %graph_id, atom_id = %atom_id,
                "delete_coral failed — safe to retry unchanged");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "deletion failed — safe to retry"})),
            )
        }
    }
}

// ── Entry point 2: source ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct DeleteSourceQuery {
    pub source_type: String,
    pub source_id: String,
    #[serde(default)]
    pub device_id: Option<String>,
}

#[utoipa_path(
    delete,
    path = "/v1/graphs/{graph_id}/sources",
    tag = "deletion",
    security(("bearer_token" = [])),
    params(
        ("graph_id" = Uuid, Path, description = "Ocean to anchor the account scope from"),
        ("source_type" = String, Query, description = "e.g. 'slack', 'notion', 'zoom'"),
        ("source_id" = String, Query, description = "Provider-native message/doc id"),
    ),
    responses(
        (status = 200, description = "Deleted every matching atom across the account \
                                      (or none existed — idempotent)"),
        (status = 404, description = "Ocean not reachable by caller"),
        (status = 503, description = "Database or blob storage unavailable"),
    )
)]
pub async fn delete_source_route(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(graph_id): Path<Uuid>,
    Query(q): Query<DeleteSourceQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    if let Some(resp) = require_reachable(&tenant, store.as_ref(), graph_id, user.user_id).await {
        return resp;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };
    let blobs = state.r2.as_deref().map(|r| r as &dyn PromotionBlobStore);
    let device_id = q
        .device_id
        .as_deref()
        .unwrap_or(crate::store::CLOUD_DEVICE_ID);

    match deletion::delete_source_in_callers_scope(
        &pool,
        blobs,
        device_id,
        graph_id,
        &q.source_type,
        &q.source_id,
        crate::services::connector_audit::Actor::User(user.user_id),
        state.control_db(), // FM-5 (B2 unit 02): CONTROL-plane retraction_audit_log write, not content.
    )
    .await
    {
        Ok(outcome) => (StatusCode::OK, Json(cascade_json(&outcome))),
        Err(e) => {
            tracing::error!(error = %e, graph_id = %graph_id, source_type = %q.source_type,
                source_id = %q.source_id, "delete_source failed — safe to retry unchanged");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "deletion failed — safe to retry"})),
            )
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DeleteSourceItemQuery {
    pub graph_id: Uuid,
}

/// UUID identity selects one provider namespace, including source-only rows.
/// Uses the same graph write authorization as the existing source retraction.
pub async fn delete_source_item_route(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(source_item_id): Path<Uuid>,
    Query(q): Query<DeleteSourceItemQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    if let Some(response)=require_reachable(&tenant,store.as_ref(),q.graph_id,user.user_id).await {
        return response;
    }
    let Some(pool)=tenant.pool.as_ref() else {
        return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"service unavailable"})));
    };
    let blobs=state.r2.as_deref().map(|r|r as &dyn PromotionBlobStore);
    match deletion::delete_source_item_in_graph(pool,blobs,crate::store::CLOUD_DEVICE_ID,
        q.graph_id,source_item_id,crate::services::connector_audit::Actor::User(user.user_id),state.control_db()).await {
        Ok(outcome)=>(StatusCode::OK,Json(cascade_json(&outcome))),
        Err(error)=>{
            tracing::error!(%error,%source_item_id,"source item deletion failed");
            (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"deletion failed — safe to retry"})))
        }
    }
}

// ── Entry point 3: subject ───────────────────────────────────────────────────

/// Surfaced on EVERY successful (and every refused) response, per this
/// project's rule that the approximation caveat must reach the call site,
/// not just live in a spec doc. See `services::deletion::delete_subject`'s
/// doc comment for the full argument.
const SUBJECT_DELETION_CAVEAT: &str =
    "subject deletion is APPROXIMATE: reef membership is extraction-derived and lossy. \
     It can MISS corals that mention this person without extraction linking them, and can \
     INCLUDE corals only incidentally about them. Do not represent this as a complete erasure \
     of every mention of this person.";

#[utoipa_path(
    delete,
    path = "/v1/graphs/{graph_id}/subjects/{node_id}",
    tag = "deletion",
    security(("bearer_token" = [])),
    params(
        ("graph_id" = Uuid, Path, description = "Ocean the person-reef lives in"),
        ("node_id" = Uuid, Path, description = "The person-reef (an entity node) whose \
                                                linked corals should be deleted"),
    ),
    responses(
        (status = 200, description = "Deleted every coral linked to the subject reef \
                                      (approximate — see the `caveat` field)"),
        (status = 403, description = "Caller's account role is not owner/admin \
                                      (`insufficient_role`)"),
        (status = 404, description = "Ocean not reachable by caller, or the subject id is \
                                      not an entity reef in this graph"),
        (status = 503, description = "Database or blob storage unavailable"),
    )
)]
pub async fn delete_subject_route(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((graph_id, node_id)): Path<(Uuid, Uuid)>,
    Query(q): Query<DeleteQuery>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);
    if let Some(resp) = require_reachable(&tenant, store.as_ref(), graph_id, user.user_id).await {
        return resp;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };
    let blobs = state.r2.as_deref().map(|r| r as &dyn PromotionBlobStore);
    let device_id = q
        .device_id
        .as_deref()
        .unwrap_or(crate::store::CLOUD_DEVICE_ID);

    match deletion::delete_subject(
        &pool,
        blobs,
        device_id,
        graph_id,
        node_id,
        &user.role,
        user.user_id,
        state.control_db(), // FM-5 (B2 unit 02): CONTROL-plane retraction_audit_log write, not content.
    )
    .await
    {
        Ok(outcome) => {
            let mut body = cascade_json(&outcome);
            body["caveat"] = json!(SUBJECT_DELETION_CAVEAT);
            body["approximate"] = json!(true);
            (StatusCode::OK, Json(body))
        }
        Err(e @ deletion::DeletionError::InsufficientRole) => {
            tracing::warn!(error = %e, graph_id = %graph_id, node_id = %node_id,
                user_id = %user.user_id, role = %user.role,
                "delete_subject refused: caller's role does not permit subject deletion");
            (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "insufficient_role",
                    "message": "subject deletion requires an owner or admin account role",
                    "caveat": SUBJECT_DELETION_CAVEAT,
                })),
            )
        }
        Err(e @ deletion::DeletionError::SubjectNotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "subject_not_found",
                "message": e.to_string(),
            })),
        ),
        Err(e) => {
            tracing::error!(error = %e, graph_id = %graph_id, node_id = %node_id,
                "delete_subject failed — safe to retry unchanged");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "deletion failed — safe to retry"})),
            )
        }
    }
}
