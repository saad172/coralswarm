//! `GET /v1/activity` — paginated activity feed for the authenticated account.

use axum::{extract::Query, http::StatusCode, Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use utoipa::path as utoipa_path;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext, store::StoreError, tenancy::extractor::Tenant,
    tenancy::scope::begin_tenant_scoped,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

// `activity_events` is a tenant-tree table (org-tenancy migration-split
// roster) — this now reads through the `Tenant` extractor's resolved store
// (state.store on Personal / an org's tenant pool on Org), never ambient
// `state.store` directly (org-tenancy PR 1.4).
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

// ── Query params ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ActivityQueryParams {
    pub graph_id: Option<Uuid>,
    pub event_type: Option<String>,
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
}

fn default_page() -> u32 {
    1
}

fn default_page_size() -> u32 {
    20
}

// ── Handler ───────────────────────────────────────────────────────────────────

/// List recent activity events for the authenticated account.
///
/// All results are scoped to `account_id` from the caller's session — no
/// cross-account leakage is possible. Also scoped to `user_ctx.user_id`
/// (Group-B security fix, PR 0 of the member-private-oceans plan, item 7):
/// the feed carries per-member graph ids and event payloads, and before this
/// scoping existed any account/org member could read every other member's
/// activity. Founder decision: per-user everywhere, not merely a leak fix —
/// this is a deliberate behavior change from account-wide to per-user and
/// must be called out in the PR body.
#[utoipa_path(
    get,
    path = "/v1/activity",
    tag = "activity",
    security(("bearer_token" = [])),
    params(
        ("graph_id"   = Option<Uuid>, Query, description = "Filter by graph"),
        ("event_type" = Option<String>, Query, description = "Filter by event type"),
        ("page"       = Option<u32>,   Query, description = "Page number (default 1)"),
        ("page_size"  = Option<u32>,   Query, description = "Page size (default 20, max 50)"),
    ),
    responses(
        (status = 200, description = "Activity events", body = Value),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_activity(
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Query(params): Query<ActivityQueryParams>,
) -> (StatusCode, Json<Value>) {
    let store = require_store!(tenant);

    // Clamp page_size to [1, 50].
    let page_size = params.page_size.clamp(1, 50);
    let page = params.page.max(1);
    let offset = (page - 1) * page_size;

    // ── RLS scoping (org-tenancy B.1) ─────────────────────────────────────────
    // `activity_events` is RLS-covered (migration 0083): on any Postgres plane
    // the read runs inside a `begin_account_scoped` transaction so the
    // `app.account_id` policy predicate is satisfied once OPS-4 switches the
    // connection role to `coralswarm_app`. With no pool,
    // `begin_member_scoped` fails and this returns a 500.
    let events = {
        let mut scoped_tx = match tenant.begin_member_scoped(user_ctx.account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!(error = %e, "list_activity: begin_account_scoped failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        let events = match store
            .list_activity_scoped(
                &mut scoped_tx,
                user_ctx.account_id,
                user_ctx.user_id,
                params.graph_id,
                params.event_type.as_deref(),
                page_size,
                offset,
            )
            .await
        {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(error = %e, "list_activity store error");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        };
        if let Err(e) = scoped_tx.commit().await {
            tracing::error!(error = %e, "list_activity: scoped commit failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
        events
    };

    let events: Vec<Value> = events
        .iter()
        .map(|e| {
            json!({
                "id": e.id,
                "user_id": e.user_id,
                "graph_id": e.graph_id,
                "event_type": e.event_type,
                "payload": e.payload,
                "created_at": e.created_at,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!(events)))
}

// ── Fire-and-forget activity insert ──────────────────────────────────────────

/// Insert an activity event in the background.
///
/// Spawns a `tokio` task so the caller is never blocked and the request
/// never fails due to an activity-log write error.  Any DB error is logged
/// as a warning.
pub(crate) fn insert_activity_event(
    pool: PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Option<Uuid>,
    event_type: &'static str,
    payload: Value,
) {
    tokio::spawn(async move {
        // RLS scoping (org-tenancy B.1; extended by tenant RLS Phase 2):
        // `activity_events` is RLS-covered on both planes (migration 0083's
        // `account_id` policy on consumer; `0008_tenant_rls`'s nullable-
        // `graph_id` member-ocean policy on tenant), so the INSERT runs
        // inside a `begin_tenant_scoped` transaction — `user_id` is this
        // function's own required parameter (every call site already passes
        // its gate-verified acting user for the JSON `user_id` field below,
        // so there is nothing new to thread), never a value this function
        // invents. Still best-effort: any failure is logged, never surfaced
        // to the caller.
        let result: Result<(), StoreError> = async {
            let mut tx = begin_tenant_scoped(&pool, account_id, user_id).await?;
            sqlx::query(
                "INSERT INTO activity_events \
                 (account_id, user_id, graph_id, event_type, payload) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(account_id)
            .bind(user_id)
            .bind(graph_id)
            .bind(event_type)
            .bind(payload)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        }
        .await;
        if let Err(e) = result {
            tracing::warn!(
                error = %e,
                event_type,
                account_id = %account_id,
                "insert_activity_event failed (non-fatal)"
            );
        }
    });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::get,
        Router,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::AppState;

    fn activity_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/activity", get(list_activity))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let router = activity_router(crate::test_app_state());
        let req = HttpRequest::builder()
            .uri("/v1/activity")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_no_db_returns_503() {
        let router = activity_router(crate::test_app_state());
        let req = HttpRequest::builder()
            .uri("/v1/activity")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // require_auth middleware returns 503 when db=None.
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn page_size_clamped_to_50() {
        let p = ActivityQueryParams {
            graph_id: None,
            event_type: None,
            page: 1,
            page_size: 9999,
        };
        assert_eq!(p.page_size.clamp(1, 50), 50);
    }

    #[test]
    fn page_size_minimum_is_1() {
        let p = ActivityQueryParams {
            graph_id: None,
            event_type: None,
            page: 1,
            page_size: 0,
        };
        assert_eq!(p.page_size.clamp(1, 50), 1);
    }
}
